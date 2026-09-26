//! A fake `/proc/self` tree.
//!
//! Enabled by the `test-util` feature. Real `/proc` cannot be made to hold a
//! thread called `we (are) evil`, or to lose a thread at the moment a test wants —
//! so the parsers are pointed at a directory instead.

// Scoped to this module: the rest of the crate obeys the workspace ban.
#![allow(
    clippy::expect_used,
    reason = "a test fixture reports a broken assumption by panicking"
)]

use std::fs;
use std::path::{Path, PathBuf};

/// A throwaway `/proc/self`.
///
/// Cleaned up when dropped.
#[derive(Debug)]
pub struct FakeProc {
    dir: tempfile::TempDir,
}

impl FakeProc {
    /// An empty tree, with a `task` directory ready.
    ///
    /// # Panics
    ///
    /// If a temporary directory cannot be made.
    #[must_use]
    pub fn new() -> Self {
        let dir = tempfile::tempdir().expect("a temporary directory");
        fs::create_dir_all(dir.path().join("task")).expect("the task directory");
        Self { dir }
    }

    /// What to pass as the proc root.
    #[must_use]
    pub fn path(&self) -> &Path {
        self.dir.path()
    }

    /// Write a file directly, for a format this fixture does not model.
    ///
    /// # Panics
    ///
    /// If the file cannot be written.
    pub fn write(&self, name: &str, contents: &str) {
        fs::write(self.path().join(name), contents).expect("write");
    }

    /// Write `stat` for the process itself.
    ///
    /// # Panics
    ///
    /// If the file cannot be written.
    pub fn write_process_cpu(&self, user_ticks: u64, system_ticks: u64) {
        self.write("stat", &stat_line("cs-agent", user_ticks, system_ticks));
    }

    /// Write `status` with a resident size in kB and a thread count.
    ///
    /// # Panics
    ///
    /// If the file cannot be written.
    pub fn write_status(&self, rss_kb: u64, threads: u64) {
        self.write(
            "status",
            &format!(
                "Name:\tcs-agent\nState:\tS (sleeping)\nVmRSS:\t{rss_kb} kB\nThreads:\t{threads}\n"
            ),
        );
    }

    /// Add a thread with a name and a CPU total.
    ///
    /// # Panics
    ///
    /// If the files cannot be written.
    pub fn add_thread(&self, tid: u32, comm: &str, user_ticks: u64, system_ticks: u64) {
        let dir = self.thread_dir(tid);
        fs::write(dir.join("comm"), format!("{comm}\n")).expect("write comm");
        fs::write(dir.join("stat"), stat_line(comm, user_ticks, system_ticks)).expect("write stat");
    }

    /// Add a thread directory with nothing in it — a thread that exited between the
    /// `readdir` and the `open`.
    ///
    /// # Panics
    ///
    /// If the directory cannot be created.
    pub fn add_empty_thread(&self, tid: u32) {
        self.thread_dir(tid);
    }

    /// Remove a thread, as happens when a service is rebuilt.
    ///
    /// # Panics
    ///
    /// If the directory exists and cannot be removed.
    pub fn remove_thread(&self, tid: u32) {
        let dir = self.path().join("task").join(tid.to_string());
        if dir.exists() {
            fs::remove_dir_all(dir).expect("remove the thread directory");
        }
    }

    fn thread_dir(&self, tid: u32) -> PathBuf {
        let dir = self.path().join("task").join(tid.to_string());
        fs::create_dir_all(&dir).expect("the thread directory");
        dir
    }
}

impl Default for FakeProc {
    fn default() -> Self {
        Self::new()
    }
}

/// A `stat` line with the fields up to `stime` filled in.
fn stat_line(comm: &str, user_ticks: u64, system_ticks: u64) -> String {
    // 1 pid, 2 (comm), 3 state, then ppid pgrp session tty_nr tpgid flags minflt
    // cminflt majflt cmajflt, then utime stime.
    format!(
        "42 ({comm}) S 1 42 42 0 -1 4194304 1234 0 5 0 {user_ticks} {system_ticks} 0 0 20 0 4 0 100\n"
    )
}
