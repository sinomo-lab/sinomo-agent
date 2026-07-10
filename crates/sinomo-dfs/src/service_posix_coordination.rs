//! POSIX coordination operations served by a metadata-shard primary.
//!
//! These operations reuse the namespace RPC envelope because it already
//! authenticates the frame source, routes by the current shard assignment,
//! rejects stale primaries, and suppresses mismatched responses. The payload
//! and response remain separately typed so a lock outcome can never be
//! mistaken for a namespace mutation acknowledgement.

use crate::posix::locks::DEFAULT_DFS_ADVISORY_LOCK_RPC_TIMEOUT;
use crate::*;

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub(crate) fn execute_posix_coordination_operation_local(
        &mut self,
        operation: &DfsNamespaceOperation,
    ) -> posix_model::Result<Option<DfsPosixCoordinationResult>> {
        match operation {
            DfsNamespaceOperation::AdvisoryLock {
                path,
                object_id,
                command,
                timeout_millis,
            } => self
                .posix_apply_advisory_lock_local(
                    path,
                    object_id,
                    command,
                    std::time::Duration::from_millis(*timeout_millis)
                        .min(DEFAULT_DFS_ADVISORY_LOCK_RPC_TIMEOUT)
                        .max(std::time::Duration::from_millis(1)),
                )
                .map(DfsPosixCoordinationResult::AdvisoryLock)
                .map(Some),
            DfsNamespaceOperation::FileSync { target, .. } => self
                .posix_sync_file_local(target, DEFAULT_DFS_POSIX_SYNC_TIMEOUT)
                .map(DfsPosixCoordinationResult::FileSync)
                .map(Some),
            DfsNamespaceOperation::DirectorySync { target, .. } => self
                .posix_sync_directory_local(target, DEFAULT_DFS_POSIX_SYNC_TIMEOUT)
                .map(DfsPosixCoordinationResult::DirectorySync)
                .map(Some),
            DfsNamespaceOperation::AdvisoryLockMigrationPrepare {
                object_id,
                state_fingerprint,
                ..
            } => {
                if state_fingerprint.is_empty() {
                    return Err(DfsPosixError::Dfs(DfsError::Distributed {
                        message: "advisory-lock migration prepare omitted its state fingerprint"
                            .to_string(),
                    }));
                }
                Ok(Some(
                    DfsPosixCoordinationResult::AdvisoryLockMigrationPrepared {
                        object_id: object_id.clone(),
                        state_fingerprint: state_fingerprint.clone(),
                    },
                ))
            }
            DfsNamespaceOperation::AdoptObjectAtPath {
                object,
                advisory_lock_state: Some(source_lock_state),
                ..
            } => {
                self.execute_namespace_operation_local(operation)?;
                let (_expected_lock_state, state_fingerprint) =
                    crate::advisory_lock::prepare_advisory_lock_state_for_migration(
                        source_lock_state,
                    )
                    .map_err(|source| {
                        DfsPosixError::Dfs(DfsError::Serialization {
                            message: format!(
                                "encode migrated advisory-lock state for {}: {source}",
                                object.object_id
                            ),
                        })
                    })?;
                let installed_lock_state = self
                    .metadata_backend
                    .as_ref()
                    .ok_or_else(|| {
                        DfsPosixError::Dfs(DfsError::Metadata {
                            message: "lock-bearing cross-shard adoption requires a durable metadata backend"
                                .to_string(),
                        })
                    })?
                    .get_advisory_lock_state(&object.file_system_id, &object.object_id)?;
                if installed_lock_state.as_ref().is_none_or(|installed| {
                    !installed
                        .migration_source_fingerprints
                        .contains(&state_fingerprint)
                }) {
                    return Err(DfsPosixError::Dfs(DfsError::Metadata {
                        message: format!(
                            "cross-shard adoption did not install the exact advisory-lock aggregate for {}",
                            object.object_id
                        ),
                    }));
                }
                let metadata_target = self
                    .capture_metadata_quorum_sync_target(object)
                    .map_err(DfsPosixError::from)?;
                self.sync_metadata_shard_to_quorum(
                    &metadata_target,
                    DEFAULT_DFS_POSIX_SYNC_TIMEOUT,
                )
                .map_err(DfsPosixError::from)?;
                Ok(Some(DfsPosixCoordinationResult::AdvisoryLockMigration {
                    object_id: object.object_id.clone(),
                    state_fingerprint,
                    durable_through_sequence: metadata_target.journal_sequence,
                }))
            }
            _ => self
                .execute_namespace_operation_local(operation)
                .map(|()| None),
        }
    }
}
