//! Self-cleaning temp directories for sinomo tests.
//!
//! Tests used to build roots with `env::temp_dir().join(format!("name-{nanos}"))`
//! and remove them with a trailing `remove_dir_all` â€” which never runs when the
//! test panics earlier or the test process is killed, leaking roots until the
//! tmpfs fills. [`TestDir`] cleans successful disposable roots on drop:
//!
//! - panic unwind retains evidence and reports its path;
//! - no process sweeps another test's roots based only on their age;
//! - `scripts/ci/run-with-test-fixtures.py` bounds retention to one invocation,
//!   archives failed runs durably, and reclaims completed unused fixtures.
//!
//! Set `SINOMO_TEST_KEEP_TMP=1` to retain roots until that runner drains, or for
//! an explicitly managed investigation. Do not retain successful runs forever.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Historical sweep interval, retained for source compatibility. Age alone
/// never authorizes reclamation; there is no automatic shared-parent sweep.
pub const STALE_ROOT_MAX_AGE: Duration = Duration::from_secs(2 * 60 * 60);

const KEEP_ENV: &str = "SINOMO_TEST_KEEP_TMP";

static SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// A uniquely named directory under `$TMPDIR/sinomo-tests` that is removed
/// when dropped successfully. Panic evidence is retained for verified archival.
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
    /// Existing sibling roots belong to their original invocations and are
    /// never reclaimed by this constructor.
    pub fn new(name: &str) -> Self {
        let parent = std::env::temp_dir().join("sinomo-tests");
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
    /// test until its owner explicitly archives or reclaims it.
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
        if std::thread::panicking() {
            eprintln!(
                "retained failed Sinomo test fixture: {}",
                self.path.display()
            );
            return;
        }
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn keep_requested() -> bool {
    std::env::var(KEEP_ENV).is_ok_and(|value| !value.is_empty() && value != "0")
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
    fn drop_retains_failure_evidence_during_panic_unwind() {
        let observed = std::sync::Arc::new(std::sync::Mutex::new(PathBuf::new()));
        let observed_in_test = observed.clone();
        let result = std::panic::catch_unwind(move || {
            let dir = TestDir::new("sinomo-testdir-panic");
            *observed_in_test.lock().expect("path slot") = dir.to_path_buf();
            std::fs::write(dir.join("evidence"), b"failed operation").unwrap();
            panic!("test body fails after creating its root");
        });
        assert!(result.is_err());
        let path = observed.lock().expect("path slot").clone();
        assert!(!path.as_os_str().is_empty());
        assert_eq!(
            std::fs::read(path.join("evidence")).unwrap(),
            b"failed operation"
        );
        std::fs::remove_dir_all(path).expect("reclaim this test's inspected evidence");
    }

    #[test]
    fn keep_disarms_cleanup() {
        let dir = TestDir::new("sinomo-testdir-keep");
        let path = dir.keep();
        assert!(path.exists());
        std::fs::remove_dir_all(&path).expect("manual cleanup");
    }

    #[test]
    fn creating_a_fixture_never_reclaims_retained_siblings() {
        let retained = TestDir::new("sinomo-testdir-retained").keep();
        std::fs::write(retained.join("evidence"), b"owned by another invocation").unwrap();
        let fresh = TestDir::new("sinomo-testdir-fresh");
        assert_ne!(retained, fresh.path());
        assert_eq!(
            std::fs::read(retained.join("evidence")).unwrap(),
            b"owned by another invocation"
        );
        std::fs::remove_dir_all(retained).expect("reclaim this test's retained sibling");
    }
}

/// Run a generated shell fixture through the packaged executable launcher.
#[cfg(unix)]
pub fn install_shell_fixture(script: &Path) -> std::io::Result<()> {
    let mut body = script.as_os_str().to_owned();
    body.push(".body");
    std::fs::rename(script, body)?;
    std::fs::copy(Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/shell-fixture.sh"), script)?;
    Ok(())
}
