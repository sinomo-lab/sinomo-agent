use crate::SandboxId;
use serde::{Deserialize, Serialize};
use sinomo_core::kv::{ClusterKvCommittedDelta, KvEventKind};
use sinomo_core::node::NodeId;
use sinomo_core::storage::durable_file::{self, DurableAtomicFileOptions, DurableFileError};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

const PENDING_DIR: &str = "pending";
const PENDING_MARKER_CONTENT: &[u8] = b"sinomo-sandbox-authoritative-recovery-pending-v1\n";

/// Cluster-KV prefix of the pending authoritative-fence index.
pub const SANDBOX_PENDING_FENCE_KV_PREFIX: &str = "/sandbox/pending/";

/// Committed membership records; see `sinomo_core::node::cluster_member_key`.
pub(crate) const CLUSTER_MEMBER_KV_PREFIX: &str = "/cluster/members/";

pub fn sandbox_pending_fence_key(sandbox_id: &SandboxId) -> String {
    format!("{SANDBOX_PENDING_FENCE_KV_PREFIX}{sandbox_id}")
}

/// Compact cluster-KV index entry for one pending authoritative sandbox
/// fence. It names the node and lifecycle lease session that published the
/// fence so another node can take over the obligation when membership drops
/// that node. The fence itself remains the authoritative manifest in SIFS.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxPendingFenceRecord {
    pub sandbox_id: SandboxId,
    pub node_id: NodeId,
    pub session_id: String,
}

/// Whether a committed cluster-KV delta removes a cluster member record.
pub fn committed_delta_removes_cluster_member(delta: &ClusterKvCommittedDelta) -> bool {
    delta.events.iter().any(|event| {
        matches!(event.kind, KvEventKind::Delete) && event.key.starts_with(CLUSTER_MEMBER_KV_PREFIX)
    })
}

#[derive(Debug, thiserror::Error)]
pub enum SandboxLifecycleRecoveryError {
    #[error("sandbox lifecycle recovery I/O error at {path}: {detail}")]
    Io { path: String, detail: String },
}

/// Node-local wake markers for authoritative sandbox transactions this node
/// started or took over. They are read once when the component host becomes
/// ready; there is no recurring discovery.
#[derive(Clone, Debug)]
pub struct SandboxLifecycleRecoveryState {
    root: PathBuf,
}

impl SandboxLifecycleRecoveryState {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
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

    pub fn is_authoritative_recovery_pending(&self, sandbox_id: &SandboxId) -> bool {
        self.pending_marker_path(sandbox_id).is_file()
    }

    fn pending_dir(&self) -> PathBuf {
        self.root.join(PENDING_DIR)
    }

    fn pending_marker_path(&self, sandbox_id: &SandboxId) -> PathBuf {
        self.pending_dir().join(sandbox_id.as_str())
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
    use sinomo_core::kv::{KvEvent, KvRevision};
    use std::sync::Arc;

    #[test]
    fn pending_markers_are_durable_obligations_until_cleared() {
        let root = sinomo_testdir::TestDir::new("sandbox-pending-markers");
        let state = SandboxLifecycleRecoveryState::new(root.path());
        assert!(state.pending_sandbox_ids().unwrap().is_empty());
        assert!(
            !root.path().join(PENDING_DIR).exists(),
            "reading an absent marker folder creates no state"
        );
        let id = SandboxId::generate();
        state.mark_authoritative_recovery_pending(&id).unwrap();
        let marker = state.pending_marker_path(&id);
        let before = std::fs::read(&marker).unwrap();
        assert!(state.is_authoritative_recovery_pending(&id));
        for _ in 0..2 {
            assert_eq!(
                state.clone().pending_sandbox_ids().unwrap(),
                BTreeSet::from([id.clone()])
            );
            assert_eq!(
                std::fs::read(&marker).unwrap(),
                before,
                "reading the marker folder cannot clear the actual recovery obligation"
            );
        }
        let restarted = SandboxLifecycleRecoveryState::new(root.path());
        assert_eq!(
            restarted.pending_sandbox_ids().unwrap(),
            BTreeSet::from([id.clone()])
        );
        restarted.clear_authoritative_recovery_pending(&id).unwrap();
        restarted.clear_authoritative_recovery_pending(&id).unwrap();
        assert!(!marker.exists());
        assert!(restarted.pending_sandbox_ids().unwrap().is_empty());
    }

    #[test]
    fn foreign_marker_folder_entries_fail_closed() {
        let root = sinomo_testdir::TestDir::new("sandbox-pending-markers-invalid");
        let state = SandboxLifecycleRecoveryState::new(root.path());
        state
            .mark_authoritative_recovery_pending(&SandboxId::generate())
            .unwrap();
        std::fs::create_dir(root.path().join(PENDING_DIR).join("nested")).unwrap();
        assert!(
            state.pending_sandbox_ids().is_err(),
            "an unrecognized marker entry is uncertainty, not an empty obligation set"
        );
    }

    #[test]
    fn only_member_record_deletes_signal_a_membership_drop() {
        let delta = |key: &str, kind: KvEventKind| ClusterKvCommittedDelta {
            revision: KvRevision(7),
            compacted_revision: KvRevision(0),
            events: Arc::new(vec![KvEvent {
                revision: KvRevision(7),
                key: key.to_string(),
                kind,
            }]),
            final_key_states: Arc::new(Vec::new()),
        };
        assert!(committed_delta_removes_cluster_member(&delta(
            "/cluster/members/node-gone",
            KvEventKind::Delete
        )));
        assert!(!committed_delta_removes_cluster_member(&delta(
            "/cluster/members/node-joined",
            KvEventKind::Put { value: Vec::new() }
        )));
        assert!(!committed_delta_removes_cluster_member(&delta(
            "/cluster/endpoints/node-gone/route",
            KvEventKind::Delete
        )));
        assert!(!committed_delta_removes_cluster_member(&delta(
            &sandbox_pending_fence_key(&SandboxId::generate()),
            KvEventKind::Delete
        )));
    }
}
