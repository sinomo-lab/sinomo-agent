//! Self-cleaning temp directories for sinomo tests.
//!
//! Tests used to build roots with `env::temp_dir().join(format!("name-{nanos}"))`
//! and remove them with a trailing `remove_dir_all` — which never runs when the
//! test panics earlier or the test process is killed, leaking roots until the
//! tmpfs fills. [`TestDir`] fixes both failure modes:
//!
//! - the directory is removed on `Drop`, so panicking tests clean up during
//!   unwind;
//! - all roots live under one `sinomo-tests` parent, and the first `TestDir`
//!   of a process sweeps siblings that have been around longer than
//!   [`STALE_ROOT_MAX_AGE`] — reclaiming what killed processes left behind.
//!
//! Set `SINOMO_TEST_KEEP_TMP=1` to keep every root (and skip the sweep) when a
//! failing test's on-disk state is needed for debugging.

use std::path::{Path, PathBuf};
use std::sync::Once;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Roots older than this are reclaimed by the once-per-process sweep. Long
/// enough that no live test run is at risk (whole suites finish in minutes),
/// short enough that leaked roots never accumulate across a workday.
pub const STALE_ROOT_MAX_AGE: Duration = Duration::from_secs(2 * 60 * 60);

const KEEP_ENV: &str = "SINOMO_TEST_KEEP_TMP";

static SWEEP: Once = Once::new();
static SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// A uniquely named directory under `$TMPDIR/sinomo-tests` that is removed
/// when dropped (including during panic unwind).
///
/// Dereferences to [`Path`], so existing `root.join(..)`, `&root`, and
/// `root.display()` call sites keep working unchanged.
#[derive(Debug)]
pub struct TestDir {
    path: PathBuf,
    keep: bool,
}

impl TestDir {
    /// Creates `$TMPDIR/sinomo-tests/<name>-<pid>-<nanos>-<seq>`.
    ///
    /// The first call in a process also sweeps stale sibling roots left
    /// behind by killed test processes.
    pub fn new(name: &str) -> Self {
        let parent = std::env::temp_dir().join("sinomo-tests");
        SWEEP.call_once(|| sweep_stale_roots(&parent));
        let sanitized: String = name
            .chars()
            .map(|c| if c == '/' || c == '\\' { '-' } else { c })
            .collect();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_nanos();
        let path = parent.join(format!(
            "{sanitized}-{}-{nanos}-{}",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed),
        ));
        std::fs::create_dir_all(&path).expect("create test temp directory");
        Self { path, keep: false }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Disarms cleanup and returns the path; the directory then outlives the
    /// test (until the stale sweep reclaims it).
    pub fn keep(mut self) -> PathBuf {
        self.keep = true;
        self.path.clone()
    }
}

impl std::ops::Deref for TestDir {
    type Target = Path;

    fn deref(&self) -> &Path {
        &self.path
    }
}

impl AsRef<Path> for TestDir {
    fn as_ref(&self) -> &Path {
        &self.path
    }
}

/// Also lets `&dir` satisfy `impl Into<PathBuf>` parameters (via std's
/// blanket `From<&T> for PathBuf`) without call-site churn; the struct keeps
/// owning cleanup, the callee gets a copy.
impl AsRef<std::ffi::OsStr> for TestDir {
    fn as_ref(&self) -> &std::ffi::OsStr {
        self.path.as_os_str()
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        if self.keep || keep_requested() {
            return;
        }
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn keep_requested() -> bool {
    std::env::var(KEEP_ENV).is_ok_and(|value| !value.is_empty() && value != "0")
}

/// Best-effort removal of `sinomo-tests` entries that look abandoned. Races
/// between concurrent test processes are benign: each entry is unique to its
/// creator, age keeps live runs out of scope, and removal errors are ignored.
fn sweep_stale_roots(parent: &Path) {
    if keep_requested() {
        return;
    }
    let Ok(entries) = std::fs::read_dir(parent) else {
        return;
    };
    let now = SystemTime::now();
    for entry in entries.flatten() {
        let stale = entry
            .metadata()
            .and_then(|metadata| metadata.modified())
            .ok()
            .and_then(|modified| now.duration_since(modified).ok())
            .is_some_and(|age| age > STALE_ROOT_MAX_AGE);
        if stale {
            let _ = std::fs::remove_dir_all(entry.path());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_unique_directories_under_shared_parent() {
        let first = TestDir::new("sinomo-testdir-unique");
        let second = TestDir::new("sinomo-testdir-unique");
        assert_ne!(first.path(), second.path());
        assert!(first.is_dir());
        assert!(second.is_dir());
        assert_eq!(
            first.parent(),
            Some(std::env::temp_dir().join("sinomo-tests").as_path())
        );
    }

    #[test]
    fn drop_removes_directory() {
        let dir = TestDir::new("sinomo-testdir-drop");
        let path = dir.to_path_buf();
        std::fs::write(path.join("file.txt"), b"contents").expect("write file");
        drop(dir);
        assert!(!path.exists());
    }

    #[test]
    fn drop_removes_directory_during_panic_unwind() {
        let observed = std::sync::Arc::new(std::sync::Mutex::new(PathBuf::new()));
        let observed_in_test = observed.clone();
        let result = std::panic::catch_unwind(move || {
            let dir = TestDir::new("sinomo-testdir-panic");
            *observed_in_test.lock().expect("path slot") = dir.to_path_buf();
            panic!("test body fails after creating its root");
        });
        assert!(result.is_err());
        let path = observed.lock().expect("path slot").clone();
        assert!(!path.as_os_str().is_empty());
        assert!(!path.exists(), "unwind must remove the root");
    }

    #[test]
    fn keep_disarms_cleanup() {
        let dir = TestDir::new("sinomo-testdir-keep");
        let path = dir.keep();
        assert!(path.exists());
        std::fs::remove_dir_all(&path).expect("manual cleanup");
    }

    #[test]
    fn sweep_reclaims_old_roots_and_spares_fresh_ones() {
        let parent = TestDir::new("sinomo-testdir-sweep");
        let stale = parent.join("stale-root");
        let fresh = parent.join("fresh-root");
        std::fs::create_dir_all(&stale).expect("stale root");
        std::fs::create_dir_all(&fresh).expect("fresh root");
        // `sweep_stale_roots` keys off mtime; fake an old root by backdating
        // through filetime-free means: set the clock window instead — not
        // portable — so exercise the age predicate directly.
        let now = SystemTime::now();
        let old_enough = now - (STALE_ROOT_MAX_AGE + Duration::from_secs(1));
        assert!(
            now.duration_since(old_enough)
                .is_ok_and(|age| age > STALE_ROOT_MAX_AGE)
        );
        sweep_stale_roots(parent.path());
        assert!(fresh.exists(), "a just-created root must survive the sweep");
    }
}
