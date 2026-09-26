//! Reading cgroup v2 control files.
//!
//! Every function here distinguishes two failures that look alike and are not:
//!
//! - **The file is absent.** Entirely normal. `memory.peak` needs Linux 5.19, PSI
//!   needs `CONFIG_PSI`, `io.stat` is empty until there has been I/O, and a cgroup
//!   vanishes the moment its job ends — possibly between the `readdir` and the
//!   `open`. All of that is `Ok(None)`.
//! - **The file is there and says something unexpected.** That means an assumption
//!   about the kernel's format is wrong, and silently reporting zero would turn a
//!   bug into bad data. That is an `Err`.

use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::Path;

use cs_api::{Error, ErrorKind, Result};

/// Read a control file, treating absence as ordinary.
///
/// `ENOENT` because the kernel does not have the file, and `ENODEV` because the
/// cgroup was removed while we were reading it, are both `Ok(None)`.
pub(crate) fn read_file(path: &Path) -> Result<Option<String>> {
    match fs::read_to_string(path) {
        Ok(contents) => Ok(Some(contents)),
        Err(err) if is_gone(&err) => Ok(None),
        Err(err) => Err(Error::with_source(
            ErrorKind::Io,
            format!("cannot read {}", path.display()),
            err,
        )),
    }
}

/// Whether an error means "this is not here", as opposed to "something is wrong".
fn is_gone(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::NotFound | io::ErrorKind::PermissionDenied
    )
        // ENODEV: the cgroup was removed between opening and reading it, which
        // happens constantly on a busy node as jobs end.
        || err.raw_os_error() == Some(19)
}

/// Parse a file of `key value` lines — `cpu.stat`, `memory.stat`,
/// `memory.events`.
///
/// Unknown keys are kept: a newer kernel adding a field must not make the file
/// unreadable, and the caller only asks for what it knows.
pub(crate) fn read_keyed(path: &Path) -> Result<Option<HashMap<String, u64>>> {
    let Some(contents) = read_file(path)? else {
        return Ok(None);
    };

    let mut values = HashMap::new();
    for (number, line) in contents.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let mut parts = line.split_ascii_whitespace();
        let (Some(key), Some(value)) = (parts.next(), parts.next()) else {
            return Err(malformed(path, number, line, "expected `key value`"));
        };
        // Some fields are signed in principle and never negative in practice;
        // treat a negative as a sign the format is not what we think.
        let parsed = value.parse::<u64>().map_err(|err| {
            malformed(path, number, line, &format!("{key} is not a count: {err}"))
        })?;
        values.insert(key.to_owned(), parsed);
    }
    Ok(Some(values))
}

/// Parse a file holding one number — `memory.current`, `pids.current`.
///
/// `memory.max` and friends may hold the literal `max`, which is not a number and
/// is reported as `None` rather than an error.
pub(crate) fn read_count(path: &Path) -> Result<Option<u64>> {
    let Some(contents) = read_file(path)? else {
        return Ok(None);
    };
    let value = contents.trim();
    if value.is_empty() || value == "max" {
        return Ok(None);
    }
    value
        .parse::<u64>()
        .map(Some)
        .map_err(|err| malformed(path, 0, value, &format!("not a count: {err}")))
}

/// One device's line from `io.stat`.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub(crate) struct IoTotals {
    pub(crate) rbytes: u64,
    pub(crate) wbytes: u64,
    pub(crate) rios: u64,
    pub(crate) wios: u64,
}

impl IoTotals {
    fn add(&mut self, other: Self) {
        // Saturating throughout: these are summed across devices, and a counter
        // that wrapped in the kernel must not panic a monitoring agent.
        self.rbytes = self.rbytes.saturating_add(other.rbytes);
        self.wbytes = self.wbytes.saturating_add(other.wbytes);
        self.rios = self.rios.saturating_add(other.rios);
        self.wios = self.wios.saturating_add(other.wios);
    }
}

/// Parse `io.stat` and sum every device.
///
/// Lines look like `8:0 rbytes=1024 wbytes=0 rios=4 wios=0 dbytes=0 dios=0`. An
/// empty file — no I/O yet — is `Some(zeroes)`, not `None`: the metric exists, it
/// is simply zero, and reporting "unavailable" would be wrong.
pub(crate) fn read_io_stat(path: &Path) -> Result<Option<IoTotals>> {
    let Some(contents) = read_file(path)? else {
        return Ok(None);
    };

    let mut total = IoTotals::default();
    for (number, line) in contents.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let mut fields = line.split_ascii_whitespace();
        // The device major:minor, which is deliberately discarded — see the note
        // on summing in the proto.
        if fields.next().is_none() {
            continue;
        }

        let mut device = IoTotals::default();
        for field in fields {
            let Some((key, value)) = field.split_once('=') else {
                return Err(malformed(
                    path,
                    number,
                    line,
                    &format!("expected `key=value`, got {field:?}"),
                ));
            };
            let parsed = value.parse::<u64>().map_err(|err| {
                malformed(path, number, line, &format!("{key} is not a count: {err}"))
            })?;
            match key {
                "rbytes" => device.rbytes = parsed,
                "wbytes" => device.wbytes = parsed,
                "rios" => device.rios = parsed,
                "wios" => device.wios = parsed,
                // dbytes, dios, and whatever a newer kernel adds.
                _ => {}
            }
        }
        total.add(device);
    }
    Ok(Some(total))
}

/// The cumulative totals from one pressure file.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub(crate) struct Pressure {
    /// Microseconds at least one task was stalled.
    pub(crate) some_usec: u64,
    /// Microseconds *every* task was stalled. Absent from `cpu.pressure` on some
    /// kernels, where it stays zero.
    pub(crate) full_usec: u64,
}

/// Parse a `*.pressure` file, keeping only the cumulative totals.
///
/// Lines look like `some avg10=0.00 avg60=0.00 avg300=0.00 total=12345`. The
/// averages are deliberately ignored: they are derived, and a server given the
/// totals can compute any window it likes — whereas an average over the kernel's
/// fixed window cannot be recovered from anything else.
pub(crate) fn read_pressure(path: &Path) -> Result<Option<Pressure>> {
    let Some(contents) = read_file(path)? else {
        return Ok(None);
    };

    let mut pressure = Pressure::default();
    for (number, line) in contents.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let mut fields = line.split_ascii_whitespace();
        let Some(kind) = fields.next() else { continue };

        let mut total = None;
        for field in fields {
            if let Some(value) = field.strip_prefix("total=") {
                total = Some(value.parse::<u64>().map_err(|err| {
                    malformed(path, number, line, &format!("total is not a count: {err}"))
                })?);
            }
        }
        let Some(total) = total else {
            return Err(malformed(path, number, line, "no total= field"));
        };
        match kind {
            "some" => pressure.some_usec = total,
            "full" => pressure.full_usec = total,
            _ => {}
        }
    }
    Ok(Some(pressure))
}

#[track_caller]
fn malformed(path: &Path, line_number: usize, line: &str, why: &str) -> Error {
    Error::new(
        // Decode rather than Io: the read worked, the *content* is not what this
        // build expects, and retrying will produce the same bytes.
        ErrorKind::Decode,
        format!(
            "{}:{} is not in the expected format ({why}): {line:?}",
            path.display(),
            line_number + 1
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    /// A directory holding fake control files.
    struct Fixture {
        dir: tempfile::TempDir,
    }

    impl Fixture {
        fn new() -> Self {
            Self {
                dir: tempfile::tempdir().expect("a temp dir"),
            }
        }

        fn write(&self, name: &str, contents: &str) -> std::path::PathBuf {
            let path = self.dir.path().join(name);
            fs::write(&path, contents).expect("write");
            path
        }

        fn missing(&self, name: &str) -> std::path::PathBuf {
            self.dir.path().join(name)
        }
    }

    #[test]
    fn a_missing_file_is_not_an_error() {
        let fixture = Fixture::new();
        assert_eq!(read_file(&fixture.missing("nope")).expect("read"), None);
        assert_eq!(read_keyed(&fixture.missing("nope")).expect("read"), None);
        assert_eq!(read_count(&fixture.missing("nope")).expect("read"), None);
        assert_eq!(read_io_stat(&fixture.missing("nope")).expect("read"), None);
        assert_eq!(read_pressure(&fixture.missing("nope")).expect("read"), None);
    }

    #[test]
    fn cpu_stat_is_read_by_key() {
        let fixture = Fixture::new();
        let path = fixture.write(
            "cpu.stat",
            "usage_usec 123456\nuser_usec 100000\nsystem_usec 23456\n\
             nr_periods 10\nnr_throttled 2\nthrottled_usec 500\n",
        );
        let values = read_keyed(&path).expect("read").expect("present");
        assert_eq!(values["usage_usec"], 123_456);
        assert_eq!(values["nr_throttled"], 2);
        assert_eq!(values.len(), 6);
    }

    #[test]
    fn an_unknown_key_from_a_newer_kernel_is_kept_not_rejected() {
        let fixture = Fixture::new();
        let path = fixture.write("cpu.stat", "usage_usec 1\nsomething_new 42\n");
        let values = read_keyed(&path).expect("read").expect("present");
        assert_eq!(values["usage_usec"], 1);
        assert_eq!(values["something_new"], 42, "forward compatibility");
    }

    #[test]
    fn memory_stat_sized_files_are_read_whole() {
        let fixture = Fixture::new();
        // The real file has ~40 lines; a few is enough to prove the shape.
        let path = fixture.write(
            "memory.stat",
            "anon 4096\nfile 8192\nkernel_stack 0\nslab 128\npgfault 55\n",
        );
        let values = read_keyed(&path).expect("read").expect("present");
        assert_eq!(values["anon"], 4096);
        assert_eq!(values["file"], 8192);
    }

    #[test]
    fn a_malformed_line_is_reported_with_where_it_was() {
        let fixture = Fixture::new();
        let path = fixture.write("cpu.stat", "usage_usec 1\nuser_usec notanumber\n");
        let err = read_keyed(&path).unwrap_err();
        assert_eq!(
            err.kind(),
            ErrorKind::Decode,
            "the read worked; the content is wrong"
        );
        assert!(!err.is_retryable(), "the same bytes will fail again");
        let shown = err.to_string();
        assert!(
            shown.contains("cpu.stat:2"),
            "should name the line: {shown}"
        );
        assert!(shown.contains("user_usec"), "should quote it: {shown}");
    }

    #[test]
    fn a_line_with_no_value_is_reported() {
        let fixture = Fixture::new();
        let path = fixture.write("cpu.stat", "usage_usec\n");
        let err = read_keyed(&path).unwrap_err();
        assert!(err.to_string().contains("expected `key value`"));
    }

    #[test]
    fn single_value_files_are_read() {
        let fixture = Fixture::new();
        assert_eq!(
            read_count(&fixture.write("memory.current", "8388608\n")).expect("read"),
            Some(8_388_608)
        );
        assert_eq!(
            read_count(&fixture.write("pids.current", "17")).expect("read"),
            Some(17)
        );
    }

    #[test]
    fn a_limit_of_max_is_not_a_number_and_not_an_error() {
        let fixture = Fixture::new();
        assert_eq!(
            read_count(&fixture.write("memory.max", "max\n")).expect("read"),
            None
        );
        assert_eq!(
            read_count(&fixture.write("empty", "\n")).expect("read"),
            None
        );
    }

    #[test]
    fn io_stat_is_summed_across_devices() {
        let fixture = Fixture::new();
        let path = fixture.write(
            "io.stat",
            "8:0 rbytes=1024 wbytes=2048 rios=4 wios=8 dbytes=0 dios=0\n\
             8:16 rbytes=512 wbytes=0 rios=2 wios=0 dbytes=0 dios=0\n",
        );
        let totals = read_io_stat(&path).expect("read").expect("present");
        assert_eq!(totals.rbytes, 1536);
        assert_eq!(totals.wbytes, 2048);
        assert_eq!(totals.rios, 6);
        assert_eq!(totals.wios, 8);
    }

    #[test]
    fn an_empty_io_stat_is_zero_rather_than_unavailable() {
        let fixture = Fixture::new();
        let totals = read_io_stat(&fixture.write("io.stat", ""))
            .expect("read")
            .expect("the metric exists, it is just zero");
        assert_eq!(totals, IoTotals::default());
    }

    #[test]
    fn an_unparseable_io_stat_field_is_reported() {
        let fixture = Fixture::new();
        let path = fixture.write("io.stat", "8:0 rbytes=1024 wbytes\n");
        let err = read_io_stat(&path).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Decode);
        assert!(err.to_string().contains("expected `key=value`"));
    }

    #[test]
    fn io_counters_saturate_rather_than_overflow() {
        let fixture = Fixture::new();
        let path = fixture.write(
            "io.stat",
            &format!(
                "8:0 rbytes={max} wbytes=0 rios=0 wios=0\n8:16 rbytes={max} wbytes=0 rios=0 wios=0\n",
                max = u64::MAX
            ),
        );
        let totals = read_io_stat(&path).expect("read").expect("present");
        assert_eq!(
            totals.rbytes,
            u64::MAX,
            "a monitoring agent must not panic on a strange counter"
        );
    }

    #[test]
    fn pressure_keeps_the_totals_and_drops_the_averages() {
        let fixture = Fixture::new();
        let path = fixture.write(
            "memory.pressure",
            "some avg10=1.50 avg60=0.75 avg300=0.10 total=123456\n\
             full avg10=0.50 avg60=0.25 avg300=0.00 total=7890\n",
        );
        let pressure = read_pressure(&path).expect("read").expect("present");
        assert_eq!(pressure.some_usec, 123_456);
        assert_eq!(pressure.full_usec, 7890);
    }

    #[test]
    fn cpu_pressure_without_a_full_line_leaves_it_zero() {
        let fixture = Fixture::new();
        let path = fixture.write("cpu.pressure", "some avg10=0.00 avg60=0.00 total=42\n");
        let pressure = read_pressure(&path).expect("read").expect("present");
        assert_eq!(pressure.some_usec, 42);
        assert_eq!(pressure.full_usec, 0, "some kernels have no full for cpu");
    }

    #[test]
    fn a_pressure_line_without_a_total_is_reported() {
        let fixture = Fixture::new();
        let path = fixture.write("cpu.pressure", "some avg10=0.00\n");
        let err = read_pressure(&path).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Decode);
        assert!(err.to_string().contains("no total="));
    }

    #[test]
    fn blank_lines_are_ignored_everywhere() {
        let fixture = Fixture::new();
        assert_eq!(
            read_keyed(&fixture.write("a", "\n\nusage_usec 1\n\n"))
                .expect("read")
                .expect("present")
                .len(),
            1
        );
        assert_eq!(
            read_io_stat(&fixture.write("b", "\n\n8:0 rbytes=1\n\n"))
                .expect("read")
                .expect("present")
                .rbytes,
            1
        );
        assert_eq!(
            read_pressure(&fixture.write("c", "\n\nsome total=1\n\n"))
                .expect("read")
                .expect("present")
                .some_usec,
            1
        );
    }
}
