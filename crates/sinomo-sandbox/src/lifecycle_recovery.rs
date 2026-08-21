use crate::SandboxId;
use sinomo_core::storage::durable_file::{self, DurableAtomicFileOptions, DurableFileError};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

const PENDING_DIR: &str = "pending";
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
}

impl SandboxLifecycleRecoveryState {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            safety_sweep_interval: SANDBOX_LIFECYCLE_SAFETY_SWEEP_INTERVAL,
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
        self.ensure_dirs()?;
        self.write_state(
            &self.sweep_stamp_path(),
            now_unix_nanos.to_string().as_bytes(),
        )
    }

    pub fn safety_sweep_due_or_initialize(
        &self,
        now_unix_nanos: u64,
    ) -> Result<bool, SandboxLifecycleRecoveryError> {
        let path = self.sweep_stamp_path();
        let stored = match std::fs::read_to_string(&path) {
            Ok(stored) => stored,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(true),
            Err(source) => return Err(io_error(&path, source)),
        };
        let Ok(last_sweep_unix_nanos) = stored.trim().parse::<u64>() else {
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

    fn sweep_stamp_path(&self) -> PathBuf {
        self.root.join(SAFETY_SWEEP_STAMP)
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
