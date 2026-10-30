//! Cross-process turn drain used by planned daemon replacement. Pending commands stay durable
//! in their owning service; a rollout cannot stop the daemon while a leased turn is executing.
use fs2::FileExt;
use std::{
    fs::{self, File, OpenOptions},
    path::Path,
    time::{Duration, Instant},
};

/// Owns one logical lock scope. Explicit unlock releases an inherited/dup'ed
/// open-file description when this Rust owner ends, before File closes.
#[derive(Debug)]
pub struct RuntimeWorkLease {
    file: File,
}
impl Drop for RuntimeWorkLease {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

fn work_file(data_dir: &Path, name: &str) -> Result<RuntimeWorkLease, String> {
    fs::create_dir_all(data_dir.join("rollout")).map_err(|e| e.to_string())?;
    OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(data_dir.join("rollout").join(name))
        .map(|file| RuntimeWorkLease { file })
        .map_err(|e| e.to_string())
}

pub struct RuntimeDrainGuard {
    _work: RuntimeWorkLease,
    _admission: RuntimeWorkLease,
}

pub fn acquire_runtime_work(data_dir: &Path) -> Result<RuntimeWorkLease, String> {
    acquire_runtime_work_until(data_dir, || false)
}

pub fn acquire_runtime_work_until(
    data_dir: &Path,
    cancelled: impl Fn() -> bool,
) -> Result<RuntimeWorkLease, String> {
    acquire_work(data_dir, true, cancelled)
}

/// Controls of existing work (especially interruption) remain available while a drain waits.
/// Their durable receipts still participate in the final exclusive work barrier.
pub fn acquire_runtime_control(data_dir: &Path) -> Result<RuntimeWorkLease, String> {
    acquire_work(data_dir, false, || false)
}

fn acquire_work(
    data_dir: &Path,
    new_work: bool,
    cancelled: impl Fn() -> bool,
) -> Result<RuntimeWorkLease, String> {
    loop {
        sinomo_core::local::ensure_deployment_enabled(data_dir)?;
        if cancelled() {
            return Err("execution host stopped before command admission".into());
        }
        let admission = new_work
            .then(|| work_file(data_dir, "runtime-admission.lock"))
            .transpose()?;
        if let Some(admission) = &admission {
            match FileExt::try_lock_shared(&admission.file) {
                Ok(()) => {}
                Err(error) if lock_contended(&error) => {
                    std::thread::sleep(Duration::from_millis(50));
                    continue;
                }
                Err(error) => return Err(error.to_string()),
            }
        }
        let file = work_file(data_dir, "runtime-work.lock")?;
        match FileExt::try_lock_shared(&file.file) {
            Ok(()) => {}
            Err(error) if lock_contended(&error) => {
                drop(admission);
                std::thread::sleep(Duration::from_millis(50));
                continue;
            }
            Err(error) => return Err(error.to_string()),
        }
        let paths = crate::SelfRolloutPaths::new(data_dir);
        if !crate::rollout_main_activation_path(&paths).exists()
            && !crate::rollout_shadow_finalize_path(&paths).exists()
        {
            if cancelled() {
                return Err("execution host stopped before command admission".into());
            }
            return Ok(file);
        }
        drop(file);
        drop(admission);
        std::thread::sleep(Duration::from_millis(250));
    }
}

fn lock_contended(error: &std::io::Error) -> bool {
    error.kind() == std::io::ErrorKind::WouldBlock
        || error.raw_os_error() == fs2::lock_contended_error().raw_os_error()
}

pub fn drain_runtime_work(
    data_dir: &Path,
    timeout: Duration,
) -> Result<RuntimeDrainGuard, crate::RolloutError> {
    let failure = |detail| crate::RolloutError::InvalidTransition { detail };
    // Fence new turns first, so a continuously fed queue cannot starve a planned replacement.
    let admission = work_file(data_dir, "runtime-admission.lock").map_err(failure)?;
    let deadline = Instant::now() + timeout;
    loop {
        match FileExt::try_lock_exclusive(&admission.file) {
            Ok(()) => break,
            Err(error) if lock_contended(&error) && Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(5))
            }
            Err(error) => {
                return Err(failure(format!(
                    "runtime admission drain unavailable: {error}"
                )));
            }
        }
    }
    let file = work_file(data_dir, "runtime-work.lock").map_err(failure)?;
    loop {
        match FileExt::try_lock_exclusive(&file.file) {
            Ok(()) => {
                return Ok(RuntimeDrainGuard {
                    _work: file,
                    _admission: admission,
                });
            }
            Err(error) if lock_contended(&error) && Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(50))
            }
            Err(error) => {
                return Err(failure(format!(
                    "daemon activation deferred until ongoing work reaches a safe boundary: {error}"
                )));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rollout_cannot_replace_a_daemon_with_a_live_turn() {
        let root = sinomo_testdir::TestDir::new("rollout-work-drain");
        let work = acquire_runtime_work(&root).unwrap();
        assert!(drain_runtime_work(&root, Duration::ZERO).is_err());
        drop(work);
        assert!(drain_runtime_work(&root, Duration::ZERO).is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn runtime_work_release_is_not_retained_by_an_inherited_child_descriptor() {
        let root = sinomo_testdir::TestDir::new("rollout-work-inherited-descriptor");
        let work = acquire_runtime_work(&root).unwrap();
        let mut child = std::process::Command::new("/bin/cat")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::from(work.file.try_clone().unwrap()))
            .spawn()
            .unwrap();
        assert!(child.try_wait().unwrap().is_none());
        assert!(drain_runtime_work(&root, Duration::ZERO).is_err());
        drop(work);
        let drained = drain_runtime_work(&root, Duration::ZERO);
        // Close and reap the controlled holder before asserting the result.
        child.stdin.take();
        assert!(child.wait().unwrap().success());
        assert!(
            drained.is_ok(),
            "the Rust work scope ended while an unrelated child retained its descriptor: {}",
            drained
                .err()
                .map(|error| error.to_string())
                .unwrap_or_default()
        );
    }

    #[cfg(unix)]
    #[test]
    fn runtime_drain_release_unlocks_inherited_admission_and_work_descriptors() {
        let root = sinomo_testdir::TestDir::new("rollout-drain-inherited-descriptors");
        let guard = drain_runtime_work(&root, Duration::ZERO).unwrap();
        let mut child = std::process::Command::new("/bin/cat")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::from(
                guard._work.file.try_clone().unwrap(),
            ))
            .stderr(std::process::Stdio::from(
                guard._admission.file.try_clone().unwrap(),
            ))
            .spawn()
            .unwrap();
        assert!(child.try_wait().unwrap().is_none());
        assert!(drain_runtime_work(&root, Duration::ZERO).is_err());
        drop(guard);
        let reacquired = drain_runtime_work(&root, Duration::ZERO);
        child.stdin.take();
        assert!(child.wait().unwrap().success());
        assert!(
            reacquired.is_ok(),
            "both drain locks release before inherited descriptors close"
        );
    }

    #[test]
    fn drain_fences_new_turns_but_keeps_interruption_available() {
        let root = sinomo_testdir::TestDir::new("rollout-admission-fence");
        let first = acquire_runtime_work(&root).unwrap();
        let worker_root = root.path().to_path_buf();
        let (drained_tx, drained_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let drainer = std::thread::spawn(move || {
            let _guard = drain_runtime_work(&worker_root, Duration::from_secs(3)).unwrap();
            drained_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let admission = work_file(&root, "runtime-admission.lock").unwrap();
            if FileExt::try_lock_shared(&admission.file).is_err() {
                break;
            }
            drop(admission);
            assert!(Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(5));
        }
        drop(acquire_runtime_control(&root).unwrap());
        let queued_root = root.path().to_path_buf();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let queued = std::thread::spawn(move || {
            let _work = acquire_runtime_work(&queued_root).unwrap();
            started_tx.send(()).unwrap();
        });
        drop(first);
        drained_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(started_rx.recv_timeout(Duration::from_millis(50)).is_err());
        release_tx.send(()).unwrap();
        started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        drainer.join().unwrap();
        queued.join().unwrap();
    }
}
