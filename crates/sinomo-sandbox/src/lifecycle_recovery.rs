use crate::SandboxId;
use sinomo_core::storage::durable_file::{self, DurableAtomicFileOptions, DurableFileError};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const PENDING_DIR: &str = "pending";
#[cfg(test)]
const SAFETY_SWEEP_STAMP: &str = "last-safety-sweep";
const PENDING_MARKER_CONTENT: &[u8] = b"sinomo-sandbox-authoritative-recovery-pending-v1\n";
pub const SANDBOX_LIFECYCLE_SAFETY_SWEEP_INTERVAL: Duration = Duration::from_secs(15 * 60);

#[derive(Debug, thiserror::Error)]
pub enum SandboxLifecycleRecoveryError {
    #[error("sandbox lifecycle recovery I/O error at {path}: {detail}")]
    Io { path: String, detail: String },
}

#[derive(Clone, Debug)]
pub struct SandboxLifecycleRecoveryState {
    root: PathBuf,
    safety_sweep_interval: Duration,
    last_safety_sweep: Arc<Mutex<Option<u64>>>,
}

impl SandboxLifecycleRecoveryState {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            safety_sweep_interval: SANDBOX_LIFECYCLE_SAFETY_SWEEP_INTERVAL,
            last_safety_sweep: Arc::new(Mutex::new(None)),
        }
    }

    pub fn with_safety_sweep_interval(mut self, interval: Duration) -> Self {
        self.safety_sweep_interval = interval;
        self
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn mark_authoritative_recovery_pending(
        &self,
        sandbox_id: &SandboxId,
    ) -> Result<(), SandboxLifecycleRecoveryError> {
        self.ensure_dirs()?;
        self.write_state(
            &self.pending_marker_path(sandbox_id),
            PENDING_MARKER_CONTENT,
        )
    }

    pub fn clear_authoritative_recovery_pending(
        &self,
        sandbox_id: &SandboxId,
    ) -> Result<(), SandboxLifecycleRecoveryError> {
        durable_file::remove_file_if_exists_and_sync_parent(&self.pending_marker_path(sandbox_id))
            .map(|_| ())
            .map_err(durable_error)
    }

    pub fn clear_authoritative_recovery_pending_best_effort(
        &self,
        sandbox_id: &SandboxId,
        context: &'static str,
    ) {
        if let Err(error) = self.clear_authoritative_recovery_pending(sandbox_id) {
            tracing::warn!(
                sandbox_id = %sandbox_id,
                context,
                error = %error,
                "failed to clear settled sandbox lifecycle recovery marker"
            );
        }
    }

    pub fn pending_sandbox_ids(
        &self,
    ) -> Result<BTreeSet<SandboxId>, SandboxLifecycleRecoveryError> {
        let pending_dir = self.pending_dir();
        let entries = match std::fs::read_dir(&pending_dir) {
            Ok(entries) => entries,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                return Ok(BTreeSet::new());
            }
            Err(source) => return Err(io_error(&pending_dir, source)),
        };
        let mut sandbox_ids = BTreeSet::new();
        for entry in entries {
            let entry = entry.map_err(|source| io_error(&pending_dir, source))?;
            let path = entry.path();
            if !entry
                .file_type()
                .map_err(|source| io_error(&path, source))?
                .is_file()
            {
                return Err(SandboxLifecycleRecoveryError::Io {
                    path: path.display().to_string(),
                    detail: "sandbox lifecycle pending marker is not a regular file".to_string(),
                });
            }
            let name =
                entry
                    .file_name()
                    .into_string()
                    .map_err(|_| SandboxLifecycleRecoveryError::Io {
                        path: path.display().to_string(),
                        detail: "sandbox lifecycle pending marker name is not UTF-8".to_string(),
                    })?;
            let sandbox_id =
                SandboxId::new(name).map_err(|source| SandboxLifecycleRecoveryError::Io {
                    path: path.display().to_string(),
                    detail: format!("invalid sandbox lifecycle pending marker: {source}"),
                })?;
            sandbox_ids.insert(sandbox_id);
        }
        Ok(sandbox_ids)
    }

    pub fn record_safety_sweep(
        &self,
        now_unix_nanos: u64,
    ) -> Result<(), SandboxLifecycleRecoveryError> {
        // Cadence is owned by the live recovery worker, not durable work.
        // Pending recovery markers remain durable and independent of this hint.
        *self
            .last_safety_sweep
            .lock()
            .map_err(|_| self.cadence_lock_error())? = Some(now_unix_nanos);
        Ok(())
    }

    pub fn safety_sweep_due_or_initialize(
        &self,
        now_unix_nanos: u64,
    ) -> Result<bool, SandboxLifecycleRecoveryError> {
        let Some(last_sweep_unix_nanos) = *self
            .last_safety_sweep
            .lock()
            .map_err(|_| self.cadence_lock_error())?
        else {
            // A new component owner must inspect real pending state. An old
            // on-disk cadence stamp neither suppresses startup nor grants work.
            return Ok(true);
        };
        if last_sweep_unix_nanos > now_unix_nanos {
            return Ok(true);
        }
        let interval_nanos =
            u64::try_from(self.safety_sweep_interval.as_nanos()).unwrap_or(u64::MAX);
        Ok(now_unix_nanos.saturating_sub(last_sweep_unix_nanos) >= interval_nanos)
    }

    pub fn run_due_recovery<E, F>(&self, now_unix_nanos: u64, scan: F) -> Result<usize, E>
    where
        E: From<SandboxLifecycleRecoveryError>,
        F: FnOnce(BTreeSet<SandboxId>) -> Result<usize, E>,
    {
        let marked_sandbox_ids = self.pending_sandbox_ids()?;
        let safety_sweep_due = self.safety_sweep_due_or_initialize(now_unix_nanos)?;
        if marked_sandbox_ids.is_empty() && !safety_sweep_due {
            return Ok(0);
        }
        let recovered = scan(marked_sandbox_ids)?;
        self.record_safety_sweep(now_unix_nanos)?;
        Ok(recovered)
    }

    fn pending_dir(&self) -> PathBuf {
        self.root.join(PENDING_DIR)
    }

    fn pending_marker_path(&self, sandbox_id: &SandboxId) -> PathBuf {
        self.pending_dir().join(sandbox_id.as_str())
    }

    #[cfg(test)]
    fn sweep_stamp_path(&self) -> PathBuf {
        self.root.join(SAFETY_SWEEP_STAMP)
    }

    fn cadence_lock_error(&self) -> SandboxLifecycleRecoveryError {
        SandboxLifecycleRecoveryError::Io {
            path: self.root.display().to_string(),
            detail: "sandbox lifecycle sweep cadence lock is poisoned".into(),
        }
    }

    fn ensure_dirs(&self) -> Result<(), SandboxLifecycleRecoveryError> {
        durable_file::ensure_private_directory(&self.root).map_err(durable_error)?;
        durable_file::ensure_private_directory(&self.pending_dir()).map_err(durable_error)
    }

    fn write_state(&self, path: &Path, bytes: &[u8]) -> Result<(), SandboxLifecycleRecoveryError> {
        durable_file::write_file_atomic(
            path,
            bytes,
            DurableAtomicFileOptions::new("sandbox-lifecycle-state", "tmp").sync_all(),
        )
        .map_err(durable_error)
    }
}

fn durable_error(source: DurableFileError) -> SandboxLifecycleRecoveryError {
    SandboxLifecycleRecoveryError::Io {
        path: source.path().to_string(),
        detail: source.detail().to_string(),
    }
}

fn io_error(path: &Path, source: std::io::Error) -> SandboxLifecycleRecoveryError {
    SandboxLifecycleRecoveryError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_safety_sweeps_do_not_rewrite_durable_state() {
        let root = sinomo_testdir::TestDir::new("sandbox-empty-safety-sweep");
        let state = SandboxLifecycleRecoveryState::new(root.path());
        let mut scans = 0;
        let first = 1_000_000_000;
        state
            .run_due_recovery::<SandboxLifecycleRecoveryError, _>(first, |pending| {
                assert!(pending.is_empty());
                scans += 1;
                Ok(0)
            })
            .unwrap();
        let stamp = state.sweep_stamp_path();
        let before = std::fs::read(&stamp).ok();
        let before_mark = std::fs::metadata(&stamp)
            .ok()
            .map(|m| m.modified().unwrap());
        let next =
            first + u64::try_from(SANDBOX_LIFECYCLE_SAFETY_SWEEP_INTERVAL.as_nanos()).unwrap();
        state
            .run_due_recovery::<SandboxLifecycleRecoveryError, _>(next, |pending| {
                assert!(pending.is_empty());
                scans += 1;
                Ok(0)
            })
            .unwrap();
        assert_eq!(scans, 2, "the existing safety sweep still runs when due");
        assert_eq!(
            std::fs::read(&stamp).ok(),
            before,
            "elapsed time and an empty completed sweep do not renew durable state"
        );
        assert_eq!(
            std::fs::metadata(&stamp)
                .ok()
                .map(|m| m.modified().unwrap()),
            before_mark
        );
        state
            .clone()
            .run_due_recovery::<SandboxLifecycleRecoveryError, _>(next + 1, |_| {
                panic!("clones of the same owner must retain the completed sweep cadence")
            })
            .unwrap();
    }
    #[test]
    fn pending_markers_remain_durable_and_override_shared_cadence() {
        let root = sinomo_testdir::TestDir::new("sandbox-pending-cadence");
        let state = SandboxLifecycleRecoveryState::new(root.path());
        let id = SandboxId::generate();
        state.record_safety_sweep(100).unwrap();
        state.mark_authoritative_recovery_pending(&id).unwrap();
        let marker = state.pending_marker_path(&id);
        let before = std::fs::read(&marker).unwrap();
        for now in [101, 102] {
            state
                .clone()
                .run_due_recovery::<SandboxLifecycleRecoveryError, _>(now, |pending| {
                    assert_eq!(pending, BTreeSet::from([id.clone()]));
                    Ok(0)
                })
                .unwrap();
            assert_eq!(
                std::fs::read(&marker).unwrap(),
                before,
                "a successful scan cannot clear the actual recovery obligation"
            );
        }
        let restarted = SandboxLifecycleRecoveryState::new(root.path());
        assert_eq!(
            restarted.pending_sandbox_ids().unwrap(),
            BTreeSet::from([id.clone()])
        );
        restarted.clear_authoritative_recovery_pending(&id).unwrap();
        assert!(!marker.exists());
    }

    #[test]
    fn failed_scan_stays_due_and_clock_rollback_keeps_existing_behavior() {
        let root = sinomo_testdir::TestDir::new("sandbox-failed-cadence");
        let state = SandboxLifecycleRecoveryState::new(root.path());
        let error = state.run_due_recovery::<SandboxLifecycleRecoveryError, _>(100, |_| {
            Err(SandboxLifecycleRecoveryError::Io {
                path: "controlled".into(),
                detail: "scan failed".into(),
            })
        });
        assert!(error.is_err());
        assert!(state.safety_sweep_due_or_initialize(101).unwrap());
        state.record_safety_sweep(101).unwrap();
        assert!(!state.safety_sweep_due_or_initialize(102).unwrap());
        assert!(state.safety_sweep_due_or_initialize(100).unwrap());
        assert!(!state.sweep_stamp_path().exists());
    }

    #[test]
    fn new_owner_scans_without_rewriting_or_trusting_legacy_cadence() {
        let root = sinomo_testdir::TestDir::new("sandbox-new-owner-cadence");
        let state = SandboxLifecycleRecoveryState::new(root.path());
        let legacy = b"9000000000000000000";
        std::fs::write(state.sweep_stamp_path(), legacy).unwrap();
        state
            .run_due_recovery::<SandboxLifecycleRecoveryError, _>(100, |_| Ok(0))
            .unwrap();
        assert!(!state.clone().safety_sweep_due_or_initialize(101).unwrap());
        let restarted = SandboxLifecycleRecoveryState::new(root.path());
        assert!(restarted.safety_sweep_due_or_initialize(101).unwrap());
        assert_eq!(std::fs::read(state.sweep_stamp_path()).unwrap(), legacy);
    }
}
