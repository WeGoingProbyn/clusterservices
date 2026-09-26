//! Reading `/proc/self`.
//!
//! Small, fiddly, and worth doing carefully: `/proc/<pid>/stat` is the one file in
//! Linux that cannot be parsed by splitting on whitespace, because field 2 is a
//! thread name in parentheses and a thread name may contain spaces *and*
//! parentheses.

use std::fs;
use std::io;
use std::path::Path;

use cs_api::{Error, ErrorKind, Result};

/// CPU time from one `stat` file, in clock ticks.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub(crate) struct CpuTicks {
    pub(crate) user: u64,
    pub(crate) system: u64,
}

impl CpuTicks {
    /// Total, converted with the kernel's tick rate.
    pub(crate) fn to_usec(self, ticks_per_second: u64) -> u64 {
        let ticks = self.user.saturating_add(self.system);
        // Microseconds: ticks * 1_000_000 / HZ. Multiply first, so a 100Hz tick
        // does not round to nothing, and saturate rather than wrap.
        ticks
            .saturating_mul(1_000_000)
            .checked_div(ticks_per_second.max(1))
            .unwrap_or(0)
    }
}

/// One thread's name and CPU.
#[derive(Clone, PartialEq, Eq, Debug)]
pub(crate) struct ThreadCpu {
    pub(crate) comm: String,
    pub(crate) ticks: CpuTicks,
}

/// Read a file, treating absence as ordinary.
///
/// A thread can exit between listing `/proc/self/task` and reading its `stat`,
/// which happens every time a sampler is rebuilt.
fn read_maybe(path: &Path) -> Result<Option<String>> {
    match fs::read_to_string(path) {
        Ok(contents) => Ok(Some(contents)),
        Err(err)
            if matches!(
                err.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::PermissionDenied
            ) =>
        {
            Ok(None)
        }
        // ESRCH: the thread went away mid-read.
        Err(err) if err.raw_os_error() == Some(3) => Ok(None),
        Err(err) => Err(Error::with_source(
            ErrorKind::Io,
            format!("cannot read {}", path.display()),
            err,
        )),
    }
}

/// Parse `utime` and `stime` out of a `/proc/<pid>/stat` line.
///
/// The parse starts at the **last** `)`, not the first, and not at a space. Field
/// 2 is `(comm)`, and a thread may be called `(weird) name` — splitting on
/// whitespace, or on the first `)`, silently shifts every later field and yields a
/// plausible-looking wrong number.
pub(crate) fn parse_stat(path: &Path, contents: &str) -> Result<CpuTicks> {
    let tail_start = contents.rfind(')').ok_or_else(|| {
        Error::new(
            ErrorKind::Decode,
            format!("{} has no comm field", path.display()),
        )
    })? + 1;

    // After `(comm)` the next field is `state`, which is field 3 — so field N is
    // at index N - 3. utime is 14, stime is 15.
    let fields: Vec<&str> = contents[tail_start..].split_ascii_whitespace().collect();
    const UTIME: usize = 14 - 3;
    const STIME: usize = 15 - 3;

    let pick = |index: usize, name: &str| -> Result<u64> {
        fields
            .get(index)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Decode,
                    format!(
                        "{} has {} fields after its comm, too few for {name}",
                        path.display(),
                        fields.len()
                    ),
                )
            })?
            .parse::<u64>()
            .map_err(|err| {
                Error::new(
                    ErrorKind::Decode,
                    format!("{} has a non-numeric {name}: {err}", path.display()),
                )
            })
    };

    Ok(CpuTicks {
        user: pick(UTIME, "utime")?,
        system: pick(STIME, "stime")?,
    })
}

/// This process's own CPU, from `<proc_self>/stat`.
pub(crate) fn read_process_cpu(proc_self: &Path) -> Result<Option<CpuTicks>> {
    let path = proc_self.join("stat");
    let Some(contents) = read_maybe(&path)? else {
        return Ok(None);
    };
    parse_stat(&path, &contents).map(Some)
}

/// Resident memory in bytes and the thread count, from `<proc_self>/status`.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub(crate) struct ProcStatus {
    pub(crate) rss_bytes: Option<u64>,
    pub(crate) threads: Option<u64>,
}

/// Parse the handful of `Key:\tvalue` lines worth having.
///
/// Lenient by design: `/proc/self/status` has forty-odd fields that vary by kernel
/// and architecture, and this wants two of them. An unparseable line is skipped
/// rather than failing the sample — unlike a cgroup file, the format here is a
/// loose convention rather than a contract.
pub(crate) fn read_status(proc_self: &Path) -> Result<ProcStatus> {
    let Some(contents) = read_maybe(&proc_self.join("status"))? else {
        return Ok(ProcStatus::default());
    };

    let mut status = ProcStatus::default();
    for line in contents.lines() {
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        let value = value.trim();
        match key {
            // "VmRSS:	 123456 kB"
            "VmRSS" => {
                if let Some(kb) = value
                    .split_ascii_whitespace()
                    .next()
                    .and_then(|number| number.parse::<u64>().ok())
                {
                    status.rss_bytes = Some(kb.saturating_mul(1024));
                }
            }
            "Threads" => status.threads = value.parse().ok(),
            _ => {}
        }
    }
    Ok(status)
}

/// Every thread's name and CPU, from `<proc_self>/task/*/`.
///
/// Threads that vanish mid-walk are skipped, not reported: a sampler being rebuilt
/// replaces its thread, and that must not fail a sample.
pub(crate) fn read_threads(proc_self: &Path) -> Result<Vec<ThreadCpu>> {
    let task_dir = proc_self.join("task");
    let entries = match fs::read_dir(&task_dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => {
            return Err(Error::with_source(
                ErrorKind::Io,
                format!("cannot list {}", task_dir.display()),
                err,
            ));
        }
    };

    let mut threads = Vec::new();
    for entry in entries {
        let Ok(entry) = entry else { continue };
        let dir = entry.path();

        let Some(comm) = read_maybe(&dir.join("comm"))? else {
            continue;
        };
        let stat_path = dir.join("stat");
        let Some(stat) = read_maybe(&stat_path)? else {
            continue;
        };

        threads.push(ThreadCpu {
            comm: comm.trim().to_owned(),
            ticks: parse_stat(&stat_path, &stat)?,
        });
    }
    // Stable order, so a batch's thread list does not churn with readdir.
    threads.sort_unstable_by(|left, right| left.comm.cmp(&right.comm));
    Ok(threads)
}

/// The service a thread belongs to, from the `<service>/<worker>` convention.
///
/// Empty for a thread with no `/` — the runtime's own, and the main thread. That is
/// wanted: it puts the framework's overhead beside the plugins' in the same report.
pub(crate) fn service_of(comm: &str) -> &str {
    comm.split_once('/').map_or("", |(service, _)| service)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::FakeProc;
    use std::path::PathBuf;

    fn path() -> PathBuf {
        PathBuf::from("/proc/self/stat")
    }

    #[test]
    fn a_stat_line_yields_utime_and_stime() {
        // Fields 1..15 of a real line, which is all this needs.
        let line = "42 (cs-agent) S 1 42 42 0 -1 4194304 1234 0 5 0 137 42 0 0 20 0 9 0 100\n";
        let ticks = parse_stat(&path(), line).expect("parse");
        assert_eq!(ticks.user, 137);
        assert_eq!(ticks.system, 42);
    }

    /// The reason this parser starts at the last `)`: a thread name is arbitrary.
    #[test]
    fn a_thread_name_containing_spaces_and_parens_does_not_shift_the_fields() {
        let line = "42 (we (are) evil) S 1 42 42 0 -1 4194304 1234 0 5 0 137 42 0 0\n";
        let ticks = parse_stat(&path(), line).expect("parse");
        assert_eq!(
            (ticks.user, ticks.system),
            (137, 42),
            "splitting on whitespace or the first paren would give a plausible wrong answer"
        );
    }

    #[test]
    fn a_stat_line_with_no_comm_is_reported() {
        let err = parse_stat(&path(), "42 cs-agent S 1\n").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Decode);
        assert!(err.to_string().contains("no comm"));
    }

    #[test]
    fn a_truncated_stat_line_is_reported_rather_than_read_as_zero() {
        let err = parse_stat(&path(), "42 (cs-agent) S 1 42\n").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Decode);
        assert!(err.to_string().contains("too few for utime"));
    }

    #[test]
    fn a_non_numeric_field_is_reported() {
        let line = "42 (cs-agent) S 1 42 42 0 -1 4194304 1234 0 5 0 lots 42 0 0\n";
        let err = parse_stat(&path(), line).unwrap_err();
        assert!(err.to_string().contains("non-numeric utime"));
    }

    #[test]
    fn ticks_become_microseconds_at_the_kernels_rate() {
        let ticks = CpuTicks {
            user: 100,
            system: 50,
        };
        // 150 ticks at 100Hz is 1.5 seconds.
        assert_eq!(ticks.to_usec(100), 1_500_000);
        // At 1000Hz the same count is a tenth of that.
        assert_eq!(ticks.to_usec(1000), 150_000);
        // A nonsense rate must not divide by zero.
        assert_eq!(ticks.to_usec(0), 150_000_000);
    }

    #[test]
    fn a_huge_tick_count_saturates_rather_than_wrapping() {
        let ticks = CpuTicks {
            user: u64::MAX,
            system: u64::MAX,
        };
        assert_eq!(ticks.to_usec(100), u64::MAX / 100);
    }

    #[test]
    fn status_gives_resident_memory_in_bytes_and_a_thread_count() {
        let proc = FakeProc::new();
        proc.write_status(123_456, 9);
        let status = read_status(proc.path()).expect("read");
        assert_eq!(
            status.rss_bytes,
            Some(123_456 * 1024),
            "the file is in kB and a server should not have to know that"
        );
        assert_eq!(status.threads, Some(9));
    }

    #[test]
    fn an_unfamiliar_status_file_yields_what_it_can() {
        let proc = FakeProc::new();
        // A kernel or architecture without VmRSS, plus a line this does not parse.
        proc.write(
            "status",
            "Name:\tcs-agent\nThreads:\t4\nSomething\nVmRSS:\tnonsense kB\n",
        );
        let status = read_status(proc.path()).expect("read");
        assert_eq!(status.threads, Some(4));
        assert_eq!(status.rss_bytes, None, "skipped, not fatal");
    }

    #[test]
    fn a_missing_proc_is_not_an_error() {
        let missing = PathBuf::from("/nonexistent/proc/self");
        assert_eq!(read_process_cpu(&missing).expect("read"), None);
        assert_eq!(read_status(&missing).expect("read"), ProcStatus::default());
        assert!(read_threads(&missing).expect("read").is_empty());
    }

    #[test]
    fn every_thread_is_read_and_ordered_by_name() {
        let proc = FakeProc::new();
        proc.add_thread(101, "cs-agent", 10, 1);
        proc.add_thread(102, "cgroup/sample", 300, 20);
        proc.add_thread(103, "tokio-runtime-w", 5, 0);

        let threads = read_threads(proc.path()).expect("read");
        let names: Vec<&str> = threads.iter().map(|t| t.comm.as_str()).collect();
        assert_eq!(names, ["cgroup/sample", "cs-agent", "tokio-runtime-w"]);
        assert_eq!(threads[0].ticks.user, 300);
        assert_eq!(threads[0].ticks.system, 20);
    }

    #[test]
    fn a_thread_that_exits_mid_walk_is_skipped() {
        let proc = FakeProc::new();
        proc.add_thread(101, "cgroup/sample", 10, 1);
        // A tid directory with no files: the thread went away between readdir and
        // open, which is what a sampler rebuild looks like.
        proc.add_empty_thread(102);

        let threads = read_threads(proc.path()).expect("read");
        assert_eq!(threads.len(), 1, "the live one, and no failure");
    }

    #[test]
    fn a_thread_name_maps_to_its_service() {
        assert_eq!(service_of("cgroup/sample"), "cgroup");
        assert_eq!(service_of("gpu/nvml"), "gpu");
        assert_eq!(
            service_of("tokio-runtime-w"),
            "",
            "the runtime's own threads belong to no service, and that is worth seeing"
        );
        assert_eq!(service_of("cs-agent"), "");
    }
}
