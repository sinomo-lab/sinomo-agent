//! Routed namespace request and response lifecycle.
//!
//! This module owns metadata-primary namespace routing: serving routed
//! namespace commands, collecting response journal batches, applying synced
//! path objects on requesters, and settling pending namespace operations. The
//! generic data-plane dispatcher remains in `service_protocol`.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use sinomo_core::node::NodeId;

use crate::*;
use crate::{posix_model, posix_path};

fn dfs_error_from_posix_namespace_sync(error: posix_model::DfsPosixError) -> DfsError {
    match error {
        posix_model::DfsPosixError::Dfs(error) => error,
        other => DfsError::Distributed {
            message: other.to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durable_namespace_prepare_keeps_ambiguous_failure_pending() {
        let error = posix_model::DfsPosixError::Dfs(DfsError::Distributed {
            message: "downstream operation failed after durable prepare".to_string(),
        });

        assert!(
            !namespace_execution_must_remain_pending(&error, false),
            "an ordinary deterministic distributed rejection remains terminal"
        );
        assert!(
            namespace_execution_must_remain_pending(&error, true),
            "once a hidden request-owned prepare is durable, recovery—not a contradictory rejection—owns the terminal result"
        );
    }
}

fn namespace_authority_shards_match(
    stamped: &[DfsMetadataShardConfig],
    current: &[DfsMetadataShardConfig],
) -> bool {
    if stamped.len() != current.len() {
        return false;
    }
    let mut matched = vec![false; current.len()];
    for stamped_shard in stamped {
        let Some((index, _)) = current
            .iter()
            .enumerate()
            .find(|(index, current_shard)| !matched[*index] && *current_shard == stamped_shard)
        else {
            return false;
        };
        matched[index] = true;
    }
    true
}

fn metadata_path_is_direct_child_of_posix_dir(directory_path: &str, metadata_path: &str) -> bool {
    let child_path = if metadata_path.is_empty() {
        "/".to_string()
    } else {
        format!("/{metadata_path}")
    };
    if child_path == directory_path {
        return false;
    }
    let prefix = if directory_path == "/" {
        "/".to_string()
    } else {
        format!("{}/", directory_path.trim_end_matches('/'))
    };
    let Some(rest) = child_path.strip_prefix(&prefix) else {
        return false;
    };
    !rest.is_empty() && !rest.contains('/')
}

fn namespace_operation_should_replay_journal_batches(operation: &DfsNamespaceOperation) -> bool {
    !matches!(
        operation,
        DfsNamespaceOperation::LookupPath { .. }
            | DfsNamespaceOperation::LookupPaths { .. }
            | DfsNamespaceOperation::AdvisoryLock { .. }
            | DfsNamespaceOperation::FileSync { .. }
            | DfsNamespaceOperation::DirectorySync { .. }
            | DfsNamespaceOperation::AdvisoryLockMigrationPrepare { .. }
            | DfsNamespaceOperation::VerifyObjectFamilyAdoption { .. }
    )
}

fn namespace_operation_requires_fresh_authority(operation: &DfsNamespaceOperation) -> bool {
    matches!(
        operation,
        DfsNamespaceOperation::AdvisoryLock { .. }
            | DfsNamespaceOperation::FileSync { .. }
            | DfsNamespaceOperation::DirectorySync { .. }
            | DfsNamespaceOperation::AdvisoryLockMigrationPrepare { .. }
            | DfsNamespaceOperation::FinalizeObjectFamilyAdoption { .. }
            | DfsNamespaceOperation::VerifyObjectFamilyAdoption { .. }
            | DfsNamespaceOperation::AdoptObjectAtPath {
                pin_transfer_id: Some(_),
                ..
            }
            | DfsNamespaceOperation::AdoptObjectAtPath {
                advisory_lock_state: Some(_),
                ..
            }
    )
}

fn namespace_operation_uses_durable_request_admission(operation: &DfsNamespaceOperation) -> bool {
    !namespace_operation_requires_fresh_authority(operation)
        && !matches!(
            operation,
            DfsNamespaceOperation::LookupPath { .. }
                | DfsNamespaceOperation::LookupPaths { .. }
                | DfsNamespaceOperation::SyncPath { .. }
        )
}

/// `mkdir_all` publishes all missing components in one request-owned namespace
/// prepare. Its admission can therefore share the prepare's durable table
/// batch instead of forcing a standalone Pending journal write first.
///
/// The transaction planner still verifies that the exact operation is proven
/// complete. Symlink-resolved or otherwise non-proving transactions fall back
/// to a standalone Pending admission before their prepare.
fn namespace_operation_can_fuse_durable_request_admission(
    operation: &DfsNamespaceOperation,
) -> bool {
    matches!(operation, DfsNamespaceOperation::MkdirAll { .. })
}

pub(crate) fn namespace_operation_response_retry_safe(operation: &DfsNamespaceOperation) -> bool {
    namespace_operation_uses_durable_request_admission(operation)
        || matches!(
            operation,
            DfsNamespaceOperation::LookupPath { .. }
                | DfsNamespaceOperation::LookupPaths { .. }
                | DfsNamespaceOperation::SyncPath { .. }
                | DfsNamespaceOperation::AdvisoryLock { .. }
                | DfsNamespaceOperation::FileSync { .. }
                | DfsNamespaceOperation::DirectorySync { .. }
                | DfsNamespaceOperation::AdvisoryLockMigrationPrepare { .. }
                | DfsNamespaceOperation::FinalizeObjectFamilyAdoption { .. }
                | DfsNamespaceOperation::VerifyObjectFamilyAdoption { .. }
                | DfsNamespaceOperation::AdoptObjectAtPath {
                    pin_transfer_id: Some(_),
                    ..
                }
        )
}

fn namespace_command_digest(command: &DfsNamespaceCommand) -> Result<String, DfsError> {
    let encoded = serde_json::to_vec(&(
        &command.file_system_id,
        &command.requesting_node_id,
        &command.operation,
    ))
    .map_err(|source| DfsError::Distributed {
        message: format!("failed to encode namespace command digest: {source}"),
    })?;
    Ok(format!("blake3:{}", blake3::hash(&encoded).to_hex()))
}

fn namespace_request_operation_id(requester_node_id: &NodeId, request_id: &str) -> String {
    let identity = format!("{requester_node_id}\0{request_id}");
    format!(
        "req-{}",
        &blake3::hash(identity.as_bytes()).to_hex().as_str()[..24]
    )
}

enum DurableNamespaceRequestAdmission {
    Execute {
        shard_id: DfsMetadataShardId,
        record: DfsNamespaceRequestRecord,
        persisted: bool,
        replaying_pending: bool,
    },
    Replay {
        result: DfsNamespaceResult,
        posix_coordination_result: Option<DfsPosixCoordinationResult>,
    },
    Reject {
        reason: String,
    },
}

pub(crate) const DFS_NAMESPACE_REQUEST_RECOVERY_BATCH_LIMIT: usize = 64;
const DFS_NAMESPACE_REQUEST_RECOVERY_SCAN_PAGE_LIMIT: usize = 4;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct DfsNamespaceRequestRecoveryReport {
    pub(crate) scanned: usize,
    pub(crate) resolved: usize,
    pub(crate) remaining: usize,
    pub(crate) failures: Vec<String>,
}

fn transaction_has_object_at(
    transaction: &DfsNamespaceTransaction,
    path: &str,
    predicate: impl Fn(&DfsObjectMetadata) -> bool,
) -> bool {
    let Ok(path) = posix_path::normalize_posix_path(path) else {
        return false;
    };
    transaction.objects.iter().any(|object| {
        posix_path::normalize_posix_path(&object.path).is_ok_and(|candidate| candidate == path)
            && predicate(object)
    })
}

fn namespace_transaction_proves_operation_completed(
    operation: &DfsNamespaceOperation,
    transaction: &DfsNamespaceTransaction,
) -> bool {
    match operation {
        DfsNamespaceOperation::Mkdir { path }
        | DfsNamespaceOperation::MkdirWithAttrs { path, .. }
        | DfsNamespaceOperation::MkdirAtNoLinks { path, .. }
        | DfsNamespaceOperation::MkdirAll { path } => {
            transaction_has_object_at(transaction, path, |object| {
                object.kind == DfsObjectKind::Directory
            })
        }
        DfsNamespaceOperation::Rmdir { path } | DfsNamespaceOperation::Unlink { path } => {
            transaction_has_object_at(transaction, path, |object| {
                object.kind == DfsObjectKind::Tombstone
            })
        }
        DfsNamespaceOperation::Rename { from, to }
        | DfsNamespaceOperation::RenameSingleNamespaceAtomic { from, to, .. } => {
            transaction_has_object_at(transaction, from, |object| {
                object.kind == DfsObjectKind::Tombstone
            }) && transaction_has_object_at(transaction, to, |object| {
                object.kind != DfsObjectKind::Tombstone
            })
        }
        DfsNamespaceOperation::Hardlink { link_path, .. } => {
            transaction_has_object_at(transaction, link_path, |object| {
                object.kind == DfsObjectKind::Hardlink
            })
        }
        DfsNamespaceOperation::SetMode { .. }
        | DfsNamespaceOperation::SetOwner { .. }
        | DfsNamespaceOperation::SetSymlinkOwner { .. }
        | DfsNamespaceOperation::SetTimes { .. } => !transaction.objects.is_empty(),
        DfsNamespaceOperation::TouchDirectoryModified { path } => {
            transaction_has_object_at(transaction, path, |object| {
                object.kind == DfsObjectKind::Directory
            })
        }
        DfsNamespaceOperation::CommitLocalEventualBatch {
            transaction: expected,
            ..
        } => expected.transaction_id == transaction.transaction_id,
        DfsNamespaceOperation::AdoptObjectAtPath { object, .. } => {
            transaction.objects.iter().any(|candidate| {
                candidate.object_id == object.object_id && candidate.path == object.path
            })
        }
        DfsNamespaceOperation::AdoptHardlinkAlias { alias, .. } => {
            transaction.objects.iter().any(|candidate| {
                candidate.object_id == alias.object_id && candidate.path == alias.path
            })
        }
        DfsNamespaceOperation::RenameDirectoryBounded { .. }
        | DfsNamespaceOperation::ResumeNamespaceJobBounded { .. }
        | DfsNamespaceOperation::ClearDirRecursive { .. }
        | DfsNamespaceOperation::ReconcilePlacement { .. }
        | DfsNamespaceOperation::SyncPath { .. }
        | DfsNamespaceOperation::LookupPath { .. }
        | DfsNamespaceOperation::LookupPaths { .. }
        | DfsNamespaceOperation::AdvisoryLock { .. }
        | DfsNamespaceOperation::FileSync { .. }
        | DfsNamespaceOperation::DirectorySync { .. }
        | DfsNamespaceOperation::AdvisoryLockMigrationPrepare { .. }
        | DfsNamespaceOperation::FinalizeObjectFamilyAdoption { .. }
        | DfsNamespaceOperation::VerifyObjectFamilyAdoption { .. } => false,
    }
}

fn namespace_execution_error_is_retriable(error: &posix_model::DfsPosixError) -> bool {
    matches!(
        error,
        posix_model::DfsPosixError::PendingWrite { .. }
            | posix_model::DfsPosixError::PartialBulkCommit { .. }
            | posix_model::DfsPosixError::Metadata(
                MetadataBackendError::Io { .. }
                    | MetadataBackendError::MembershipHandoffFrozen { .. }
                    | MetadataBackendError::Conflict { .. }
            )
            | posix_model::DfsPosixError::Dfs(
                DfsError::Network { .. }
                    | DfsError::StaleOwnerEpoch { .. }
                    | DfsError::MetadataShardMembershipHandoffParked { .. }
                    | DfsError::CommitContentRepairPending { .. }
                    | DfsError::Metadata { .. }
                    | DfsError::ContentStore { .. }
                    | DfsError::Busy { .. }
                    | DfsError::Cancelled { .. }
            )
    )
}

fn namespace_execution_must_remain_pending(
    error: &posix_model::DfsPosixError,
    durable_prepare_recorded: bool,
) -> bool {
    durable_prepare_recorded || namespace_execution_error_is_retriable(error)
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub(crate) fn recover_pending_namespace_requests_bounded(
        &mut self,
        max_requests: usize,
    ) -> Result<DfsNamespaceRequestRecoveryReport, DfsError> {
        let mut report = DfsNamespaceRequestRecoveryReport::default();
        if max_requests == 0 || self.control_plane.is_none() || self.metadata_backend.is_none() {
            return Ok(report);
        }
        let mut locally_authoritative = Vec::new();
        let mut locally_authoritative_count = 0usize;
        let mut shard_snapshots = BTreeMap::<DfsFileSystemId, Vec<DfsMetadataShardConfig>>::new();
        let mut cursor = self.pending_namespace_request_recovery_cursor.take();
        let scan_started_after_cursor = cursor.is_some();
        let scan_page_entry_limit = max_requests
            .max(1)
            .min(DFS_NAMESPACE_REQUEST_RECOVERY_BATCH_LIMIT);
        let mut scanned_pages = 0usize;
        'scan: while scanned_pages < DFS_NAMESPACE_REQUEST_RECOVERY_SCAN_PAGE_LIMIT {
            let page = self
                .metadata_backend
                .as_ref()
                .expect("metadata backend checked above")
                .list_pending_namespace_requests_page(cursor.as_ref(), scan_page_entry_limit)?;
            scanned_pages = scanned_pages.saturating_add(1);
            let page_len = page.requests.len();
            let page_next_cursor = page.next_cursor;
            for (page_index, request) in page.requests.into_iter().enumerate() {
                let request_cursor = DfsPendingNamespaceRequestPageCursor {
                    updated_at: request.updated_at,
                    requester_node_id: request.requester_node_id.clone(),
                    request_id: request.request_id.clone(),
                };
                if !shard_snapshots.contains_key(&request.file_system_id) {
                    let shards = self
                        .metadata_shard_config_snapshot(&request.file_system_id)
                        .ok()
                        .flatten()
                        .unwrap_or_default();
                    shard_snapshots.insert(request.file_system_id.clone(), shards);
                }
                let authority_is_local =
                    shard_snapshots
                        .get(&request.file_system_id)
                        .is_some_and(|snapshot| {
                            Self::affected_namespace_shards_from_snapshot(
                                &request.file_system_id,
                                &request.operation,
                                snapshot,
                            )
                            .is_ok_and(|shards| {
                                shards.iter().any(|shard| {
                                    shard.shard_id == request.authority_shard_id.0
                                        && shard.primary_node_id == self.node_id
                                })
                            })
                        });
                if authority_is_local {
                    locally_authoritative_count = locally_authoritative_count.saturating_add(1);
                    locally_authoritative.push(request);
                    if locally_authoritative.len() >= max_requests {
                        let has_unscanned_records =
                            page_index.saturating_add(1) < page_len || page_next_cursor.is_some();
                        cursor = has_unscanned_records.then_some(request_cursor);
                        break 'scan;
                    }
                }
            }
            cursor = page_next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        let wrapped_at_end = scan_started_after_cursor && cursor.is_none();
        self.pending_namespace_request_recovery_cursor = cursor;
        report.remaining = locally_authoritative_count.saturating_add(usize::from(
            self.pending_namespace_request_recovery_cursor.is_some() || wrapped_at_end,
        ));

        if !locally_authoritative.is_empty() {
            let request_keys = locally_authoritative
                .iter()
                .map(|request| {
                    (
                        request.requester_node_id.clone(),
                        request.request_id.clone(),
                    )
                })
                .collect::<BTreeSet<_>>();
            let recovered =
                self.recover_durable_namespace_prepares_for_requests(&request_keys, max_requests)?;
            report.scanned = report.scanned.saturating_add(recovered);
            report.resolved = report.resolved.saturating_add(recovered);
            report.remaining = report.remaining.saturating_sub(recovered);
            if recovered > 0 {
                let mut unresolved = Vec::with_capacity(locally_authoritative.len());
                for request in locally_authoritative {
                    let backend = self
                        .metadata_backend
                        .as_ref()
                        .expect("metadata backend checked above");
                    if backend
                        .get_namespace_request_receipt(
                            &request.requester_node_id,
                            &request.request_id,
                        )?
                        .is_some()
                    {
                        continue;
                    }
                    let current = backend
                        .get_namespace_request(&request.requester_node_id, &request.request_id)?
                        .ok_or_else(|| DfsError::Metadata {
                            message: format!(
                                "recovered namespace request {}/{} disappeared from durable metadata",
                                request.requester_node_id, request.request_id
                            ),
                        })?;
                    if current.status != DfsNamespaceRequestStatus::Completed {
                        unresolved.push(current);
                    }
                }
                locally_authoritative = unresolved;
            }
        }

        for mut request in locally_authoritative
            .into_iter()
            .take(max_requests.saturating_sub(report.scanned))
        {
            report.scanned = report.scanned.saturating_add(1);

            let command = DfsNamespaceCommand {
                request_id: request.request_id.clone(),
                file_system_id: request.file_system_id.clone(),
                requesting_node_id: request.requester_node_id.clone(),
                include_metadata_journal_batches: false,
                metadata_journal_after_sequences: Vec::new(),
                authority_shards: Vec::new(),
                operation: request.operation.clone(),
            };
            let attempt = self.handle_namespace_request_internal(
                request.requester_node_id.clone(),
                command,
                true,
            );
            let current = self
                .metadata_backend
                .as_ref()
                .expect("metadata backend checked above")
                .get_namespace_request(&request.requester_node_id, &request.request_id)?;
            let compacted = self
                .metadata_backend
                .as_ref()
                .expect("metadata backend checked above")
                .get_namespace_request_receipt(&request.requester_node_id, &request.request_id)?
                .is_some();
            if compacted
                || current
                    .is_some_and(|current| current.status == DfsNamespaceRequestStatus::Completed)
            {
                report.resolved = report.resolved.saturating_add(1);
                report.remaining = report.remaining.saturating_sub(1);
                continue;
            }

            let detail = match attempt {
                Ok(NetworkedDfsServiceEvent::RemoteNamespaceServed {
                    result: DfsNamespaceResult::Rejected { reason, .. },
                    ..
                }) => reason,
                Ok(_) => "namespace request remained pending after recovery execution".to_string(),
                Err(error) => error.to_string(),
            };
            request.recovery_attempts = request.recovery_attempts.saturating_add(1);
            request.last_recovery_error = Some(detail.clone());
            request.updated_at = time::OffsetDateTime::now_utc();
            match self
                .metadata_backend
                .as_mut()
                .expect("metadata backend checked above")
                .put_namespace_request_on_shard(&request.authority_shard_id, &request)
            {
                Ok(_) => {}
                Err(error) => report.failures.push(format!(
                    "namespace request {}/{} recovery diagnostics failed: {error}",
                    request.requester_node_id, request.request_id
                )),
            }
            report.failures.push(format!(
                "namespace request {}/{} remains recoverable: {detail}",
                request.requester_node_id, request.request_id
            ));
        }
        Ok(report)
    }

    fn admit_durable_namespace_request(
        &mut self,
        requester_node_id: &NodeId,
        command: &DfsNamespaceCommand,
        shard_id: DfsMetadataShardId,
        recovering: bool,
    ) -> Result<DurableNamespaceRequestAdmission, DfsError> {
        let digest = namespace_command_digest(command)?;
        let backend = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| DfsError::Metadata {
                message: "routed namespace mutation requires durable request admission".to_string(),
            })?;
        if let Some(receipt) =
            backend.get_namespace_request_receipt(requester_node_id, &command.request_id)?
        {
            if receipt.file_system_id != command.file_system_id || receipt.command_digest != digest
            {
                return Ok(DurableNamespaceRequestAdmission::Reject {
                    reason: format!(
                        "namespace request id {}/{} was already used for a different command",
                        requester_node_id, command.request_id
                    ),
                });
            }
            return Ok(DurableNamespaceRequestAdmission::Replay {
                result: receipt.result,
                posix_coordination_result: receipt.posix_coordination_result,
            });
        }
        let existing = backend.get_namespace_request(requester_node_id, &command.request_id)?;
        if let Some(mut existing) = existing {
            if existing.file_system_id != command.file_system_id
                || existing.command_digest != digest
            {
                return Ok(DurableNamespaceRequestAdmission::Reject {
                    reason: format!(
                        "namespace request id {}/{} was already used for a different command",
                        requester_node_id, command.request_id
                    ),
                });
            }
            if existing.status == DfsNamespaceRequestStatus::Pending {
                let owns_prepare = self
                    .metadata_backend
                    .as_ref()
                    .expect("metadata backend checked above")
                    .get_prepared_namespace_prepare_for_request(
                        requester_node_id,
                        &command.request_id,
                    )?
                    .is_some();
                if owns_prepare {
                    if !recovering {
                        let request_key = BTreeSet::from([(
                            requester_node_id.clone(),
                            command.request_id.clone(),
                        )]);
                        self.recover_durable_namespace_prepares_for_requests(&request_key, 1)?;
                        let recovered = self
                            .metadata_backend
                            .as_ref()
                            .expect("metadata backend checked above")
                            .get_namespace_request(requester_node_id, &command.request_id)?;
                        if let Some(recovered) = recovered {
                            existing = recovered;
                        } else if let Some(receipt) = self
                            .metadata_backend
                            .as_ref()
                            .expect("metadata backend checked above")
                            .get_namespace_request_receipt(requester_node_id, &command.request_id)?
                        {
                            return Ok(DurableNamespaceRequestAdmission::Replay {
                                result: receipt.result,
                                posix_coordination_result: receipt.posix_coordination_result,
                            });
                        } else {
                            return Err(DfsError::Metadata {
                                message: format!(
                                    "request-owned namespace prepare lost admission {}/{} during recovery",
                                    requester_node_id, command.request_id
                                ),
                            });
                        }
                    }
                    if existing.status == DfsNamespaceRequestStatus::Pending {
                        return Ok(DurableNamespaceRequestAdmission::Reject {
                            reason: format!(
                                "namespace request {}/{} has a durable hidden prepare awaiting recovery",
                                requester_node_id, command.request_id
                            ),
                        });
                    }
                }
            }
            return match existing.status {
                DfsNamespaceRequestStatus::Completed => {
                    let Some(result) = existing.result else {
                        return Err(DfsError::Metadata {
                            message: format!(
                                "completed namespace request {}/{} has no durable result",
                                requester_node_id, command.request_id
                            ),
                        });
                    };
                    Ok(DurableNamespaceRequestAdmission::Replay {
                        result,
                        posix_coordination_result: existing.posix_coordination_result,
                    })
                }
                DfsNamespaceRequestStatus::Pending => {
                    Ok(DurableNamespaceRequestAdmission::Execute {
                        shard_id: existing.authority_shard_id.clone(),
                        record: existing,
                        persisted: true,
                        replaying_pending: true,
                    })
                }
            };
        }
        if backend.namespace_request_history_expired_on_shard(
            &shard_id,
            requester_node_id,
            &command.request_id,
        )? {
            return Ok(DurableNamespaceRequestAdmission::Reject {
                reason: format!(
                    "namespace request {}/{} idempotency history expired or is unknown",
                    requester_node_id, command.request_id
                ),
            });
        }

        let now = time::OffsetDateTime::now_utc();
        let record = DfsNamespaceRequestRecord {
            file_system_id: command.file_system_id.clone(),
            requester_node_id: requester_node_id.clone(),
            request_id: command.request_id.clone(),
            operation_id: namespace_request_operation_id(requester_node_id, &command.request_id),
            authority_shard_id: shard_id.clone(),
            operation: command.operation.clone(),
            command_digest: digest,
            status: DfsNamespaceRequestStatus::Pending,
            result: None,
            posix_coordination_result: None,
            recovery_attempts: 0,
            last_recovery_error: None,
            created_at: now,
            updated_at: now,
        };
        let persisted = !namespace_operation_can_fuse_durable_request_admission(&command.operation);
        if persisted {
            self.metadata_backend
                .as_mut()
                .expect("metadata backend checked above")
                .put_namespace_request_on_shard(&shard_id, &record)?;
        }
        Ok(DurableNamespaceRequestAdmission::Execute {
            shard_id,
            record,
            persisted,
            replaying_pending: false,
        })
    }

    fn complete_durable_namespace_request(
        &mut self,
        shard_id: &DfsMetadataShardId,
        mut record: DfsNamespaceRequestRecord,
        result: DfsNamespaceResult,
        posix_coordination_result: Option<DfsPosixCoordinationResult>,
    ) -> Result<(DfsNamespaceResult, Option<DfsPosixCoordinationResult>), DfsError> {
        if let Some(receipt) = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| DfsError::Metadata {
                message: "routed namespace mutation lost its durable request backend".to_string(),
            })?
            .get_namespace_request_receipt(&record.requester_node_id, &record.request_id)?
        {
            if receipt.command_digest != record.command_digest
                || receipt.file_system_id != record.file_system_id
            {
                return Err(DfsError::Metadata {
                    message: format!(
                        "namespace request receipt {}/{} conflicts with the active command",
                        record.requester_node_id, record.request_id
                    ),
                });
            }
            return Ok((receipt.result, receipt.posix_coordination_result));
        }
        if let Some(existing) = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| DfsError::Metadata {
                message: "routed namespace mutation lost its durable request backend".to_string(),
            })?
            .get_namespace_request(&record.requester_node_id, &record.request_id)?
            && existing.status == DfsNamespaceRequestStatus::Completed
        {
            let canonical_result = existing.result.ok_or_else(|| DfsError::Metadata {
                message: format!(
                    "completed namespace request {}/{} has no durable result",
                    record.requester_node_id, record.request_id
                ),
            })?;
            return Ok((canonical_result, existing.posix_coordination_result));
        }
        record.status = DfsNamespaceRequestStatus::Completed;
        record.result = Some(result.clone());
        record.posix_coordination_result = posix_coordination_result.clone();
        record.last_recovery_error = None;
        record.updated_at = time::OffsetDateTime::now_utc();
        self.metadata_backend
            .as_mut()
            .ok_or_else(|| DfsError::Metadata {
                message: "routed namespace mutation lost its durable request backend".to_string(),
            })?
            .put_namespace_request_on_shard(shard_id, &record)?;
        Ok((result, posix_coordination_result))
    }

    pub(crate) fn durable_namespace_request_completion_for_transaction(
        &self,
        transaction: &DfsNamespaceTransaction,
        shard_id: &DfsMetadataShardId,
    ) -> Option<DfsNamespaceRequestRecord> {
        let active = self.active_durable_namespace_request.as_ref()?;
        if &active.authority_shard_id != shard_id
            || !namespace_transaction_proves_operation_completed(&active.operation, transaction)
        {
            return None;
        }
        let mut completed = active.clone();
        completed.status = DfsNamespaceRequestStatus::Completed;
        completed.result = Some(DfsNamespaceResult::Applied);
        completed.posix_coordination_result = None;
        completed.last_recovery_error = None;
        completed.updated_at = time::OffsetDateTime::now_utc();
        Some(completed)
    }

    fn synced_namespace_object_for_operation(
        &self,
        file_system_id: &DfsFileSystemId,
        operation: &DfsNamespaceOperation,
    ) -> Result<Option<DfsObjectMetadata>, DfsError> {
        Ok(self
            .synced_namespace_objects_for_operation(file_system_id, operation)?
            .into_iter()
            .next())
    }

    fn synced_namespace_objects_for_sync_path(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Vec<DfsObjectMetadata>, DfsError> {
        let Some(normalized_path) = posix_path::normalized_posix_path_key(path) else {
            return Ok(Vec::new());
        };
        let mut objects = Vec::new();
        let mut include_direct_children = false;
        if let Some(object) =
            self.synced_namespace_object_for_path(file_system_id, &normalized_path)?
        {
            include_direct_children =
                object.kind == DfsObjectKind::Directory && file_system_id == &sifs_file_system_id();
            objects.push(object);
        }
        if include_direct_children {
            let children = self
                .child_objects_for_sifs_dir(&normalized_path)
                .map_err(dfs_error_from_posix_namespace_sync)?;
            let mut children_by_path = BTreeMap::<String, DfsObjectMetadata>::new();
            for object in children {
                let object = self.object_metadata_with_receipt_locations(object)?;
                children_by_path.insert(metadata_path(&object.path), object);
            }
            for tombstone in
                self.direct_child_tombstones_for_sifs_dir(file_system_id, &normalized_path)?
            {
                let path = metadata_path(&tombstone.path);
                children_by_path.insert(path, tombstone);
            }
            objects.extend(children_by_path.into_values());
        }
        Ok(objects)
    }

    fn direct_child_tombstones_for_sifs_dir(
        &self,
        file_system_id: &DfsFileSystemId,
        directory_path: &str,
    ) -> Result<Vec<DfsObjectMetadata>, DfsError> {
        let mut memory_by_path = BTreeMap::<String, DfsObjectMetadata>::new();
        for ((tombstone_file_system_id, metadata_path), object) in &self.tombstones_by_metadata_path
        {
            if tombstone_file_system_id == file_system_id
                && metadata_path_is_direct_child_of_posix_dir(directory_path, metadata_path)
            {
                memory_by_path.insert(metadata_path.clone(), object.clone());
            }
        }
        for object in self.metadata.objects.values() {
            if object.file_system_id != *file_system_id {
                continue;
            }
            let object_metadata_path = metadata_path(&object.path);
            if !metadata_path_is_direct_child_of_posix_dir(directory_path, &object_metadata_path) {
                continue;
            }
            let replace = memory_by_path
                .get(&object_metadata_path)
                .is_none_or(|existing| {
                    existing.version < object.version
                        || (existing.version == object.version
                            && existing.kind == DfsObjectKind::Tombstone
                            && object.kind != DfsObjectKind::Tombstone)
                });
            if replace {
                memory_by_path.insert(object_metadata_path, object.clone());
            }
        }
        let mut candidate_paths = memory_by_path.keys().cloned().collect::<BTreeSet<_>>();
        if let Some(backend) = self.metadata_backend.as_ref() {
            for object in backend.child_tombstones_for_directory(file_system_id, directory_path)? {
                let object_metadata_path = metadata_path(&object.path);
                if !metadata_path_is_direct_child_of_posix_dir(
                    directory_path,
                    &object_metadata_path,
                ) {
                    continue;
                }
                candidate_paths.insert(object_metadata_path);
            }
        }
        let mut tombstones = Vec::new();
        for path in candidate_paths {
            let memory_object = memory_by_path.get(&path);
            let backend_object = self
                .metadata_backend
                .as_ref()
                .map(|backend| backend.get_object_by_path(file_system_id, &path))
                .transpose()?
                .flatten();
            let authoritative = match (memory_object, backend_object.as_ref()) {
                (Some(memory_object), Some(backend_object))
                    if self.local_write_view_overrides_backend(
                        &memory_object.object_id,
                        memory_object,
                        Some(backend_object),
                    ) =>
                {
                    Some(memory_object.clone())
                }
                (_, Some(backend_object)) => Some(backend_object.clone()),
                (Some(memory_object), None) => Some(memory_object.clone()),
                (None, None) => None,
            };
            if let Some(object) = authoritative
                && object.kind == DfsObjectKind::Tombstone
            {
                tombstones.push(self.object_metadata_with_receipt_locations(object)?);
            }
        }
        Ok(tombstones)
    }

    fn synced_mkdir_all_paths(path: &str) -> Vec<String> {
        let Some(normalized) = posix_path::normalized_posix_path_key(path) else {
            return Vec::new();
        };
        if normalized == "/" {
            return vec![normalized];
        }
        let mut paths = Vec::new();
        let mut current = String::new();
        for part in normalized.trim_matches('/').split('/') {
            current.push('/');
            current.push_str(part);
            paths.push(current.clone());
        }
        paths
    }

    fn synced_namespace_objects_for_operation(
        &self,
        file_system_id: &DfsFileSystemId,
        operation: &DfsNamespaceOperation,
    ) -> Result<Vec<DfsObjectMetadata>, DfsError> {
        let paths: Vec<String> = match operation {
            DfsNamespaceOperation::SyncPath { path } => {
                return self.synced_namespace_objects_for_sync_path(file_system_id, path);
            }
            DfsNamespaceOperation::LookupPath { path }
            | DfsNamespaceOperation::Mkdir { path }
            | DfsNamespaceOperation::MkdirWithAttrs { path, .. }
            | DfsNamespaceOperation::MkdirAtNoLinks { path, .. } => {
                posix_path::normalized_posix_path_key(path)
                    .into_iter()
                    .collect()
            }
            DfsNamespaceOperation::MkdirAll { path } => Self::synced_mkdir_all_paths(path),
            DfsNamespaceOperation::LookupPaths { paths } => paths
                .iter()
                .filter_map(|path| posix_path::normalized_posix_path_key(path))
                .collect(),
            _ => return Ok(Vec::new()),
        };
        let mut objects = Vec::new();
        for path in &paths {
            if let Some(object) = self.synced_namespace_object_for_path(file_system_id, path)? {
                objects.push(object);
            }
        }
        Ok(objects)
    }

    fn synced_namespace_object_for_path(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Option<DfsObjectMetadata>, DfsError> {
        let normalized_path = match posix_path::normalized_posix_path_key(path) {
            Some(path) => path,
            None => return Ok(None),
        };
        self.authoritative_metadata_object_for_path(file_system_id, &normalized_path)?
            .map(|object| self.object_metadata_with_receipt_locations(object))
            .transpose()
    }

    /// Serves a namespace operation routed from a replica. The local node must
    /// be the metadata-shard primary for every shard the operation touches.
    /// Lock and explicit sync operations run their typed authority/quorum
    /// protocol; ordinary mutations retain their existing response-journal
    /// catch-up path so the requester reads its own write.
    pub(crate) fn handle_namespace_request(
        &mut self,
        requester_node_id: NodeId,
        command: DfsNamespaceCommand,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        self.handle_namespace_request_internal(requester_node_id, command, false)
    }

    fn handle_namespace_request_internal(
        &mut self,
        requester_node_id: NodeId,
        command: DfsNamespaceCommand,
        recovering: bool,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let request_id = command.request_id.clone();
        let trace_started = Instant::now();
        let trace_operation = dfs_namespace_operation_trace_label(&command.operation);
        let trace_involved_paths = command.operation.involved_paths().len();
        let mut trace_affected_ms = 0;
        let mut trace_affected_shards = 0;
        let mut trace_execute_ms = 0;
        let mut affected_shard_ids: Vec<DfsMetadataShardId> = Vec::new();
        for expected in &command.authority_shards {
            self.rebind_completed_local_metadata_shard_membership_handoff_if_frozen(expected)?;
        }
        // The authority the requester should retry against when this node is
        // not the shard primary (the requester routed on a stale shard view).
        let mut authority_redirect: Option<NodeId> = None;
        let mut durable_replay = false;
        let mut posix_coordination_result = None;
        let result = if requester_node_id != command.requesting_node_id {
            DfsNamespaceResult::Rejected {
                reason: format!(
                    "namespace request requester mismatch: frame source {}, payload requester {}",
                    requester_node_id, command.requesting_node_id
                ),
                posix_error: None,
                current_primary_node_id: None,
            }
        } else if let DfsNamespaceOperation::AdvisoryLock { command, .. } = &command.operation
            && !crate::posix::locks::advisory_lock_command_belongs_to_node(
                command,
                &requester_node_id,
            )
        {
            DfsNamespaceResult::Rejected {
                reason: format!(
                    "advisory-lock command is not owned by authenticated requester {requester_node_id}"
                ),
                posix_error: None,
                current_primary_node_id: None,
            }
        } else if let Some(reason) = self.adoption_source_authority_rejection(
            &command.file_system_id,
            &command.operation,
            &requester_node_id,
        )? {
            DfsNamespaceResult::Rejected {
                reason,
                posix_error: None,
                current_primary_node_id: None,
            }
        } else {
            let affected_started = Instant::now();
            let affected = self
                .metadata_shard_config_snapshot(&command.file_system_id)
                .and_then(|snapshot| {
                    let snapshot = snapshot.unwrap_or_default();
                    let affected = Self::affected_namespace_shards_from_snapshot(
                        &command.file_system_id,
                        &command.operation,
                        &snapshot,
                    )?;
                    Ok((snapshot, affected))
                });
            match affected {
                Err(error) => {
                    trace_affected_ms = affected_started.elapsed().as_millis();
                    DfsNamespaceResult::Rejected {
                        reason: error.to_string(),
                        posix_error: None,
                        current_primary_node_id: None,
                    }
                }
                Ok((mut shard_snapshot, mut shards)) => {
                    trace_affected_ms = affected_started.elapsed().as_millis();
                    trace_affected_shards = shards.len();
                    if !recovering
                        && !command.authority_shards.is_empty()
                        && !namespace_authority_shards_match(&command.authority_shards, &shards)
                        && self.refresh_control_plane_latest().is_ok()
                        && let Ok(Some(refreshed_snapshot)) =
                            self.metadata_shard_config_snapshot(&command.file_system_id)
                        && let Ok(refreshed) = Self::affected_namespace_shards_from_snapshot(
                            &command.file_system_id,
                            &command.operation,
                            &refreshed_snapshot,
                        )
                    {
                        // A long-lived lane can trail the sender's committed
                        // view. Refresh once before rejecting; matching stamps
                        // stay on the no-refresh hot path.
                        shard_snapshot = refreshed_snapshot;
                        shards = refreshed;
                        trace_affected_ms = affected_started.elapsed().as_millis();
                        trace_affected_shards = shards.len();
                    }
                    if !recovering
                        && !command.authority_shards.is_empty()
                        && !namespace_authority_shards_match(&command.authority_shards, &shards)
                    {
                        let current_primary_node_id = command
                            .operation
                            .routing_path()
                            .and_then(|routing_path| {
                                crate::control_plane::select_metadata_shard_config_for_path_strict_default_sifs_split(
                                    &command.file_system_id,
                                    &shards,
                                    routing_path,
                                )
                                .ok()
                                .flatten()
                                .map(|shard| shard.primary_node_id)
                            });
                        DfsNamespaceResult::Rejected {
                            reason: format!(
                                "namespace authority shard stamp does not match the receiver's current affected shard configurations: stamped {:?}, current {:?}",
                                command.authority_shards, shards
                            ),
                            posix_error: None,
                            current_primary_node_id,
                        }
                    } else {
                        // Response batches may only carry shards this node is
                        // primary for; foreign halves of coordinated operations
                        // are synced from their own primaries.
                        affected_shard_ids = shards
                            .iter()
                            .filter(|shard| shard.primary_node_id == self.node_id)
                            .map(|shard| DfsMetadataShardId::new(shard.shard_id.clone()))
                            .collect();
                        let foreign_shard = shards
                            .iter()
                            .find(|shard| shard.primary_node_id != self.node_id)
                            .cloned();
                        let durable_admission =
                            if namespace_operation_uses_durable_request_admission(
                                &command.operation,
                            ) && !affected_shard_ids.is_empty()
                            {
                                let shard_id = affected_shard_ids
                                    .iter()
                                    .min()
                                    .expect("non-empty affected shard ids")
                                    .clone();
                                Some(self.admit_durable_namespace_request(
                                    &requester_node_id,
                                    &command,
                                    shard_id,
                                    recovering,
                                )?)
                            } else {
                                None
                            };
                        match durable_admission {
                            Some(DurableNamespaceRequestAdmission::Replay {
                                result,
                                posix_coordination_result: replayed_coordination_result,
                            }) => {
                                durable_replay = true;
                                posix_coordination_result = replayed_coordination_result;
                                result
                            }
                            Some(DurableNamespaceRequestAdmission::Reject { reason }) => {
                                durable_replay = true;
                                DfsNamespaceResult::Rejected {
                                    reason,
                                    posix_error: None,
                                    current_primary_node_id: None,
                                }
                            }
                            durable_execute => {
                                let deferred_admission = durable_execute.as_ref().and_then(
                                    |admission| match admission {
                                        DurableNamespaceRequestAdmission::Execute {
                                            shard_id,
                                            record,
                                            persisted: false,
                                            ..
                                        } => Some((shard_id.clone(), record.clone())),
                                        DurableNamespaceRequestAdmission::Execute { .. }
                                        | DurableNamespaceRequestAdmission::Replay { .. }
                                        | DurableNamespaceRequestAdmission::Reject { .. } => None,
                                    },
                                );
                                self.active_durable_namespace_prepare_recorded = false;
                                let replaying_pending_request = durable_execute.as_ref().and_then(
                                    |admission| match admission {
                                        DurableNamespaceRequestAdmission::Execute {
                                            record,
                                            replaying_pending: true,
                                            ..
                                        } => Some(record.clone()),
                                        DurableNamespaceRequestAdmission::Execute { .. }
                                        | DurableNamespaceRequestAdmission::Replay { .. }
                                        | DurableNamespaceRequestAdmission::Reject { .. } => None,
                                    },
                                );
                                self.active_durable_namespace_request = durable_execute
                                    .as_ref()
                                    .and_then(|admission| match admission {
                                        DurableNamespaceRequestAdmission::Execute {
                                            record,
                                            ..
                                        } => Some(record.clone()),
                                        DurableNamespaceRequestAdmission::Replay { .. }
                                        | DurableNamespaceRequestAdmission::Reject { .. } => None,
                                    });
                                let execute_started = Instant::now();
                                let previous_operation_context = if foreign_shard.is_none() {
                                    self.replace_namespace_operation_context(
                                        command.file_system_id.clone(),
                                        shard_snapshot.clone(),
                                    )
                                } else {
                                    // Cross-shard coordinators explicitly
                                    // refresh authority and may pump nested
                                    // requests while settling. They must never
                                    // inherit an outer local planning snapshot.
                                    self.active_namespace_operation_context.take()
                                };
                                let outcome = match (&foreign_shard, &command.operation) {
                                    (
                                        None,
                                        DfsNamespaceOperation::CommitLocalEventualBatch {
                                            transaction,
                                            writes,
                                        },
                                    ) => {
                                        if self.control_plane.is_some() && shards.len() != 1 {
                                            let shard_ids = shards
                                                .iter()
                                                .map(|shard| shard.shard_id.as_str())
                                                .collect::<Vec<_>>()
                                                .join(", ");
                                            Err(posix_model::DfsPosixError::Dfs(
                                                DfsError::Distributed {
                                                    message: format!(
                                                        "clustered local-eventual batch {} must resolve to exactly one metadata shard, found [{}]",
                                                        transaction.transaction_id, shard_ids
                                                    ),
                                                },
                                            ))
                                        } else {
                                            self.apply_local_eventual_batch_commit(
                                                &requester_node_id,
                                                transaction.clone(),
                                                writes,
                                                shards.first().cloned(),
                                            )
                                            .map(|mutated| {
                                                durable_replay = !mutated;
                                                None
                                            })
                                            .map_err(posix_model::DfsPosixError::from)
                                        }
                                    }
                                    (None, _) => self.execute_posix_coordination_operation_local(
                                        &command.operation,
                                    ),
                                    (Some(_), DfsNamespaceOperation::Rename { from, to }) => self
                                        .namespace_coordinator_dispatch_rename_with_pending_replay(
                                            &command.file_system_id,
                                            from,
                                            to,
                                            replaying_pending_request.as_ref(),
                                        )
                                        .map(|()| None),
                                    (
                                        Some(_),
                                        DfsNamespaceOperation::Hardlink {
                                            existing_path,
                                            link_path,
                                        },
                                    ) => self
                                        .namespace_coordinator_dispatch_hardlink(
                                            &command.file_system_id,
                                            existing_path,
                                            link_path,
                                        )
                                        .map(|()| None),
                                    (Some(shard), _) => {
                                        authority_redirect = Some(shard.primary_node_id.clone());
                                        Err(posix_model::DfsPosixError::Dfs(
                                            DfsError::Distributed {
                                                message: format!(
                                                    "node {} is not the metadata primary for shard {} (primary {}, epoch {})",
                                                    self.node_id,
                                                    shard.shard_id,
                                                    shard.primary_node_id,
                                                    shard.epoch
                                                ),
                                            },
                                        ))
                                    }
                                };
                                self.restore_namespace_operation_context(
                                    previous_operation_context,
                                );
                                let durable_prepare_recorded =
                                    self.active_durable_namespace_prepare_recorded;
                                self.active_durable_namespace_request = None;
                                self.active_durable_namespace_prepare_recorded = false;
                                trace_execute_ms = execute_started.elapsed().as_millis();
                                let retryable_error = outcome.as_ref().err().is_some_and(|error| {
                                    namespace_execution_must_remain_pending(
                                        error,
                                        durable_prepare_recorded,
                                    )
                                });
                                if retryable_error
                                    && !durable_prepare_recorded
                                    && let Some((shard_id, record)) = deferred_admission
                                {
                                    self.metadata_backend
                                        .as_mut()
                                        .expect("durable namespace request backend")
                                        .put_namespace_request_on_shard(&shard_id, &record)?;
                                }
                                let mut operation_result = match outcome {
                                    Ok(coordination_result) => {
                                        posix_coordination_result = coordination_result;
                                        DfsNamespaceResult::Applied
                                    }
                                    Err(error) => DfsNamespaceResult::Rejected {
                                        current_primary_node_id: authority_redirect.or_else(|| {
                                            namespace_rejection_current_primary(&error)
                                        }),
                                        posix_error: posix_model::namespace_posix_error_for(&error),
                                        reason: error.to_string(),
                                    },
                                };
                                if let Some(DurableNamespaceRequestAdmission::Execute {
                                    shard_id,
                                    record,
                                    ..
                                }) = durable_execute
                                    && !retryable_error
                                {
                                    let (canonical_result, canonical_coordination_result) = self
                                        .complete_durable_namespace_request(
                                            &shard_id,
                                            record,
                                            operation_result.clone(),
                                            posix_coordination_result.clone(),
                                        )?;
                                    operation_result = canonical_result;
                                    posix_coordination_result = canonical_coordination_result;
                                }
                                operation_result
                            }
                        }
                    }
                }
            }
        };
        let mutates_metadata = !durable_replay
            && !matches!(
                &command.operation,
                DfsNamespaceOperation::LookupPath { .. }
                    | DfsNamespaceOperation::LookupPaths { .. }
                    | DfsNamespaceOperation::SyncPath { .. }
                    | DfsNamespaceOperation::AdvisoryLock { .. }
                    | DfsNamespaceOperation::FileSync { .. }
                    | DfsNamespaceOperation::DirectorySync { .. }
            );
        if mutates_metadata && matches!(result, DfsNamespaceResult::Applied) {
            self.record_message_committed_work(DfsMessageCommittedWork {
                publish_metadata: true,
                repair_maintenance: false,
                namespace_maintenance: false,
                metadata_replica_progress: false,
            });
        }
        let journal_started = Instant::now();
        let mut metadata_journal_has_more = false;
        let metadata_journal_batches = if command.include_metadata_journal_batches
            && matches!(result, DfsNamespaceResult::Applied)
            && namespace_operation_should_replay_journal_batches(&command.operation)
        {
            let mut batches = Vec::new();
            for shard_id in &affected_shard_ids {
                let after_sequence = command
                    .metadata_journal_after_sequences
                    .iter()
                    .find(|cursor| &cursor.shard_id == shard_id)
                    .map(|cursor| cursor.after_sequence)
                    .unwrap_or(0);
                let mut shard_batches = self.metadata_journal_batches_for_shard_since_limited(
                    shard_id,
                    after_sequence,
                    DFS_ROUTED_NAMESPACE_RESPONSE_JOURNAL_BATCH_LIMIT.saturating_add(1),
                    DFS_ROUTED_NAMESPACE_RESPONSE_JOURNAL_ENTRY_LIMIT,
                    DFS_ROUTED_NAMESPACE_RESPONSE_JOURNAL_BYTE_LIMIT,
                )?;
                if shard_batches.len() > DFS_ROUTED_NAMESPACE_RESPONSE_JOURNAL_BATCH_LIMIT {
                    metadata_journal_has_more = true;
                    shard_batches.truncate(DFS_ROUTED_NAMESPACE_RESPONSE_JOURNAL_BATCH_LIMIT);
                }
                batches.extend(shard_batches);
            }
            batches
        } else {
            Vec::new()
        };
        let trace_journal_ms = journal_started.elapsed().as_millis();
        let trace_journal_batches = metadata_journal_batches.len();
        let sync_started = Instant::now();
        let synced_path_objects = if matches!(result, DfsNamespaceResult::Applied) {
            self.synced_namespace_objects_for_operation(
                &command.file_system_id,
                &command.operation,
            )?
        } else {
            Vec::new()
        };
        let synced_path_object = match &command.operation {
            DfsNamespaceOperation::SyncPath { .. } | DfsNamespaceOperation::LookupPath { .. } => {
                synced_path_objects.first().cloned()
            }
            _ => None,
        };
        let trace_sync_ms = sync_started.elapsed().as_millis();
        let trace_synced_path_objects = synced_path_objects.len();
        let send_started = Instant::now();
        let response = DfsNamespaceResponse {
            request_id: request_id.clone(),
            result: result.clone(),
            synced_path_object,
            synced_path_objects,
            metadata_journal_batches,
            metadata_journal_has_more: Some(metadata_journal_has_more),
            posix_coordination_result,
        };
        if !recovering {
            self.send_dfs_message_if_routable(
                &requester_node_id,
                DfsNetworkMessage::NamespaceResponse(response),
            )?;
        }
        let trace_send_ms = send_started.elapsed().as_millis();
        dfs_perf_trace(format_args!(
            "namespace-request node={} requester={} request={} operation={} involved_paths={} affected_shards={} result={} affected_ms={} execute_ms={} journal_batches={} journal_ms={} synced_path_objects={} sync_ms={} response_send_ms={} total_ms={}",
            self.node_id,
            requester_node_id,
            request_id,
            trace_operation,
            trace_involved_paths,
            trace_affected_shards,
            match &result {
                DfsNamespaceResult::Applied => "applied",
                DfsNamespaceResult::Rejected { .. } => "rejected",
            },
            trace_affected_ms,
            trace_execute_ms,
            trace_journal_batches,
            trace_journal_ms,
            trace_synced_path_objects,
            trace_sync_ms,
            trace_send_ms,
            trace_started.elapsed().as_millis()
        ));
        Ok(NetworkedDfsServiceEvent::RemoteNamespaceServed {
            request_id,
            requester_node_id,
            result,
            mutates_metadata,
        })
    }

    pub(crate) fn handle_namespace_response(
        &mut self,
        primary_node_id: NodeId,
        response: DfsNamespaceResponse,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        // Late, duplicate, or mismatched responses are discarded; a stray
        // frame must never fail the whole event loop.
        let (
            replay_journal_batches,
            apply_synced_path_object,
            dispatch_authority_shard,
            authority_shards,
        ) = {
            let Some(pending) = self.pending_routed_namespace_ops.get(&response.request_id) else {
                return Ok(NetworkedDfsServiceEvent::RemoteResponseDiscarded {
                    request_id: response.request_id,
                    source_node_id: primary_node_id,
                    reason: "namespace response for an untracked request".to_string(),
                });
            };
            if pending.primary_node_id != primary_node_id {
                let reason = format!(
                    "namespace response did not come from primary {}",
                    pending.primary_node_id
                );
                return Ok(NetworkedDfsServiceEvent::RemoteResponseDiscarded {
                    request_id: response.request_id,
                    source_node_id: primary_node_id,
                    reason,
                });
            }
            (
                pending.replay_journal_batches,
                pending.apply_synced_path_object,
                pending.dispatch_authority_shard.clone(),
                pending.authority_shards.clone(),
            )
        };
        let response_applied = matches!(response.result, DfsNamespaceResult::Applied);
        if let DfsNamespaceResult::Rejected {
            current_primary_node_id: Some(redirected_primary),
            ..
        } = &response.result
            && let Err(error) = self.validate_routed_namespace_authority_redirect(
                &primary_node_id,
                dispatch_authority_shard.as_ref(),
                &authority_shards,
                redirected_primary,
            )
        {
            let reason = format!("routed namespace authority redirect is uncorroborated: {error}");
            if let Some(pending) = self
                .pending_routed_namespace_ops
                .get_mut(&response.request_id)
            {
                pending.result = Some(PendingRoutedNamespaceResult {
                    result: DfsNamespaceResult::Rejected {
                        reason: reason.clone(),
                        posix_error: None,
                        current_primary_node_id: None,
                    },
                    posix_coordination_result: None,
                    synced_path_object: None,
                    synced_path_objects: Vec::new(),
                    metadata_journal_has_more: false,
                });
            }
            return Ok(NetworkedDfsServiceEvent::RemoteResponseDiscarded {
                request_id: response.request_id,
                source_node_id: primary_node_id,
                reason,
            });
        }
        if response_applied
            && let Err(error) = self.validate_routed_namespace_response_authority(
                &primary_node_id,
                dispatch_authority_shard.as_ref(),
                &authority_shards,
            )
        {
            let reason = format!("routed namespace response authority is stale: {error}");
            if let Some(pending) = self
                .pending_routed_namespace_ops
                .get_mut(&response.request_id)
            {
                pending.result = Some(PendingRoutedNamespaceResult {
                    result: DfsNamespaceResult::Rejected {
                        reason: reason.clone(),
                        posix_error: None,
                        current_primary_node_id: None,
                    },
                    posix_coordination_result: None,
                    synced_path_object: None,
                    synced_path_objects: Vec::new(),
                    metadata_journal_has_more: false,
                });
            }
            return Ok(NetworkedDfsServiceEvent::RemoteResponseDiscarded {
                request_id: response.request_id,
                source_node_id: primary_node_id,
                reason,
            });
        }
        // A rejection carries no authoritative metadata. Ignore any attached
        // journal or path payload before the caller evaluates a corroborated
        // authority redirect.
        if response_applied && replay_journal_batches {
            for batch in &response.metadata_journal_batches {
                let batch_file_system_id = metadata_journal_batch_file_system_id(batch);
                let expected_authority = authority_shards.iter().find(|shard| {
                    batch_file_system_id.as_ref() == Some(&shard.file_system_id)
                        && batch.shard_id.0 == shard.shard_id
                });
                let apply_result = match expected_authority {
                    Some(expected_authority) => self.apply_routed_write_metadata_journal_batch(
                        &primary_node_id,
                        expected_authority,
                        batch,
                    ),
                    None => Err(DfsError::Distributed {
                        message: format!(
                            "routed namespace response journal for shard {} does not match any dispatch authority assignment",
                            batch.shard_id.0
                        ),
                    }),
                };
                if let Err(error) = apply_result {
                    let reason = format!("routed namespace journal replay failed: {error}");
                    if let Some(pending) = self
                        .pending_routed_namespace_ops
                        .get_mut(&response.request_id)
                    {
                        pending.result = Some(PendingRoutedNamespaceResult {
                            result: DfsNamespaceResult::Rejected {
                                reason: reason.clone(),
                                posix_error: None,
                                current_primary_node_id: None,
                            },
                            posix_coordination_result: None,
                            synced_path_object: None,
                            synced_path_objects: Vec::new(),
                            metadata_journal_has_more: false,
                        });
                    }
                    return Ok(NetworkedDfsServiceEvent::RemoteResponseDiscarded {
                        request_id: response.request_id,
                        source_node_id: primary_node_id,
                        reason,
                    });
                }
            }
        }
        if response_applied
            && apply_synced_path_object
            && let Some(object) = response.synced_path_object.as_ref()
            && let Err(error) = self.apply_metadata_object_to_service_state(object.clone())
        {
            let reason = format!("routed namespace path sync failed: {error}");
            if let Some(pending) = self
                .pending_routed_namespace_ops
                .get_mut(&response.request_id)
            {
                pending.result = Some(PendingRoutedNamespaceResult {
                    result: DfsNamespaceResult::Rejected {
                        reason: reason.clone(),
                        posix_error: None,
                        current_primary_node_id: None,
                    },
                    posix_coordination_result: None,
                    synced_path_object: None,
                    synced_path_objects: Vec::new(),
                    metadata_journal_has_more: false,
                });
            }
            return Ok(NetworkedDfsServiceEvent::RemoteResponseDiscarded {
                request_id: response.request_id,
                source_node_id: primary_node_id,
                reason,
            });
        }
        if response_applied && apply_synced_path_object {
            for object in &response.synced_path_objects {
                if let Err(error) = self.apply_metadata_object_to_service_state(object.clone()) {
                    let reason = format!("routed namespace path sync failed: {error}");
                    if let Some(pending) = self
                        .pending_routed_namespace_ops
                        .get_mut(&response.request_id)
                    {
                        pending.result = Some(PendingRoutedNamespaceResult {
                            result: DfsNamespaceResult::Rejected {
                                reason: reason.clone(),
                                posix_error: None,
                                current_primary_node_id: None,
                            },
                            posix_coordination_result: None,
                            synced_path_object: None,
                            synced_path_objects: Vec::new(),
                            metadata_journal_has_more: false,
                        });
                    }
                    return Ok(NetworkedDfsServiceEvent::RemoteResponseDiscarded {
                        request_id: response.request_id,
                        source_node_id: primary_node_id,
                        reason,
                    });
                }
            }
        }
        let pending = self
            .pending_routed_namespace_ops
            .get_mut(&response.request_id)
            .expect("pending routed namespace op was validated before metadata replay");
        let (
            posix_coordination_result,
            synced_path_object,
            synced_path_objects,
            metadata_journal_has_more,
        ) = if response_applied {
            (
                response.posix_coordination_result.clone(),
                response.synced_path_object.clone(),
                response.synced_path_objects.clone(),
                response.metadata_journal_has_more.unwrap_or_else(|| {
                    response.metadata_journal_batches.len()
                        >= DFS_ROUTED_NAMESPACE_RESPONSE_JOURNAL_BATCH_LIMIT
                }),
            )
        } else {
            (None, None, Vec::new(), false)
        };
        pending.result = Some(PendingRoutedNamespaceResult {
            result: response.result.clone(),
            posix_coordination_result,
            synced_path_object,
            synced_path_objects,
            metadata_journal_has_more,
        });
        Ok(NetworkedDfsServiceEvent::RemoteNamespaceCompleted {
            request_id: response.request_id,
            primary_node_id,
            result: response.result,
        })
    }

    /// Verify that a successful namespace response still comes from the
    /// exact committed authority snapshot used to dispatch it. Namespace
    /// reads and coordination responses can legitimately contain no journal
    /// batches, so authority validation cannot live only in batch replay.
    fn validate_routed_namespace_response_authority(
        &mut self,
        primary_node_id: &NodeId,
        dispatch_authority_shard: Option<&DfsMetadataShardConfig>,
        expected_authority_shards: &[DfsMetadataShardConfig],
    ) -> Result<(), DfsError> {
        // Standalone/local services have no cluster authority view. A
        // clustered dispatch without an affected assignment cannot prove who
        // may complete the request and therefore fails closed.
        if expected_authority_shards.is_empty() {
            return if self.control_plane.is_none() {
                Ok(())
            } else {
                Err(DfsError::Distributed {
                    message: "clustered namespace response has no dispatch shard assignment"
                        .to_string(),
                })
            };
        }
        let Some(dispatch_authority_shard) = dispatch_authority_shard else {
            return Err(DfsError::Distributed {
                message: "clustered namespace response has no exact dispatch authority shard"
                    .to_string(),
            });
        };
        if &dispatch_authority_shard.primary_node_id != primary_node_id {
            return Err(DfsError::Distributed {
                message: format!(
                    "response source {primary_node_id} was not dispatch authority {} for routing shard {}",
                    dispatch_authority_shard.primary_node_id, dispatch_authority_shard.shard_id
                ),
            });
        }
        let Some(control_plane) = self.control_plane.as_mut() else {
            return Err(DfsError::Distributed {
                message: "routed namespace response requires a committed control-plane shard view"
                    .to_string(),
            });
        };
        for expected in expected_authority_shards {
            let Some(current) = DfsControlPlaneStore::new(control_plane.as_mut())
                .get_metadata_shard(&expected.file_system_id, &expected.shard_id)?
            else {
                return Err(DfsError::Distributed {
                    message: format!(
                        "affected namespace shard {} has no current committed assignment",
                        expected.shard_id
                    ),
                });
            };
            if current != *expected {
                return Err(DfsError::Distributed {
                    message: format!(
                        "dispatch authority for namespace shard {} epoch {} is no longer current (current primary {}, epoch {})",
                        expected.shard_id, expected.epoch, current.primary_node_id, current.epoch
                    ),
                });
            }
        }
        Ok(())
    }

    /// Refresh and bind a rejected response's redirect claim to the exact
    /// shard authority that selected the dispatch target. Other shards may be
    /// involved in a coordinated operation and legitimately have different
    /// primaries; they must not be mistaken for successors of the dispatch
    /// authority. Doing this while settling the response protects every
    /// caller, including branches that execute locally when the corroborated
    /// new primary is this node.
    fn validate_routed_namespace_authority_redirect(
        &mut self,
        attempted_primary: &NodeId,
        dispatch_authority_shard: Option<&DfsMetadataShardConfig>,
        expected_authority_shards: &[DfsMetadataShardConfig],
        redirected_primary: &NodeId,
    ) -> Result<(), DfsError> {
        let Some(dispatch_authority_shard) = dispatch_authority_shard else {
            return Err(DfsError::Distributed {
                message: format!(
                    "redirect from {attempted_primary} to {redirected_primary} has no exact dispatch shard assignment to refresh"
                ),
            });
        };
        if &dispatch_authority_shard.primary_node_id != attempted_primary {
            return Err(DfsError::Distributed {
                message: format!(
                    "redirect source {attempted_primary} was not dispatch authority {} for routing shard {}",
                    dispatch_authority_shard.primary_node_id, dispatch_authority_shard.shard_id
                ),
            });
        }
        let expected_dispatch_authorities = expected_authority_shards
            .iter()
            .filter(|shard| &shard.primary_node_id == attempted_primary)
            .collect::<Vec<_>>();
        if !expected_dispatch_authorities
            .iter()
            .any(|expected| *expected == dispatch_authority_shard)
        {
            return Err(DfsError::Distributed {
                message: format!(
                    "routing shard {} is missing from the immutable affected-shard authority snapshot",
                    dispatch_authority_shard.shard_id
                ),
            });
        }
        self.refresh_control_plane_latest()?;
        let Some(control_plane) = self.control_plane.as_mut() else {
            return Err(DfsError::Distributed {
                message:
                    "namespace authority redirect requires a committed control-plane shard view"
                        .to_string(),
            });
        };
        for expected in expected_dispatch_authorities {
            let Some(current) = DfsControlPlaneStore::new(control_plane.as_mut())
                .get_metadata_shard(&expected.file_system_id, &expected.shard_id)?
            else {
                return Err(DfsError::Distributed {
                    message: format!(
                        "redirected dispatch namespace shard {} has no current committed assignment",
                        expected.shard_id
                    ),
                });
            };
            if &current.primary_node_id != redirected_primary {
                return Err(DfsError::Distributed {
                    message: format!(
                        "response named primary {redirected_primary}, but current dispatch namespace shard {} epoch {} names {}",
                        current.shard_id, current.epoch, current.primary_node_id
                    ),
                });
            }
        }
        Ok(())
    }

    /// The requester's last applied journal sequence per locally known shard,
    /// reported with routed namespace operations so the primary's response
    /// batches replay exactly the missing entries.
    pub(crate) fn local_metadata_shard_cursors(
        &self,
    ) -> Result<Vec<DfsNamespaceShardCursor>, DfsError> {
        Ok(self
            .local_metadata_journal_cursors
            .iter()
            .map(|(shard_id, after_sequence)| DfsNamespaceShardCursor {
                shard_id: shard_id.clone(),
                after_sequence: *after_sequence,
            })
            .collect())
    }

    pub(crate) fn route_namespace_operation_to_metadata_primary_with_timeout(
        &mut self,
        file_system_id: DfsFileSystemId,
        primary_node_id: NodeId,
        operation: DfsNamespaceOperation,
        timeout: Duration,
    ) -> Result<DfsNamespaceResult, DfsError> {
        Ok(self
            .route_namespace_operation_to_metadata_primary_with_timeout_and_options(
                file_system_id,
                primary_node_id,
                operation,
                timeout,
                true,
                true,
            )?
            .result)
    }

    pub(crate) fn route_namespace_lookup_path_to_metadata_primary(
        &mut self,
        file_system_id: DfsFileSystemId,
        primary_node_id: NodeId,
        path: String,
        timeout: Duration,
    ) -> Result<Option<DfsObjectMetadata>, DfsError> {
        let mut primary = primary_node_id;
        let operation = DfsNamespaceOperation::LookupPath { path };
        for _ in 0..DFS_ROUTED_WRITE_AUTHORITY_REDIRECT_LIMIT {
            let settled = self
                .route_namespace_operation_to_metadata_primary_with_timeout_and_options(
                    file_system_id.clone(),
                    primary.clone(),
                    operation.clone(),
                    timeout,
                    false,
                    false,
                )?;
            match settled.result {
                DfsNamespaceResult::Applied => return Ok(settled.synced_path_object),
                DfsNamespaceResult::Rejected {
                    reason: _,
                    current_primary_node_id: Some(current),
                    ..
                } if current == self.node_id => {
                    return self.synced_namespace_object_for_operation(&file_system_id, &operation);
                }
                DfsNamespaceResult::Rejected {
                    current_primary_node_id: Some(current),
                    ..
                } if current != primary => {
                    let next_primary = self.refreshed_primary_after_authority_redirect(
                        &file_system_id,
                        &operation,
                        &primary,
                        current,
                    )?;
                    if next_primary == self.node_id {
                        return self
                            .synced_namespace_object_for_operation(&file_system_id, &operation);
                    }
                    primary = next_primary;
                }
                DfsNamespaceResult::Rejected { reason, .. } => {
                    return Err(DfsError::Distributed { message: reason });
                }
            }
        }
        Err(DfsError::Distributed {
            message: format!(
                "routed namespace lookup exceeded {DFS_ROUTED_WRITE_AUTHORITY_REDIRECT_LIMIT} authority redirects"
            ),
        })
    }

    pub(crate) fn route_namespace_lookup_paths_to_metadata_primary(
        &mut self,
        file_system_id: DfsFileSystemId,
        primary_node_id: NodeId,
        paths: Vec<String>,
        timeout: Duration,
    ) -> Result<Vec<DfsObjectMetadata>, DfsError> {
        if paths.is_empty() {
            return Ok(Vec::new());
        }
        let mut primary = primary_node_id;
        let operation = DfsNamespaceOperation::LookupPaths { paths };
        for _ in 0..DFS_ROUTED_WRITE_AUTHORITY_REDIRECT_LIMIT {
            let settled = self
                .route_namespace_operation_to_metadata_primary_with_timeout_and_options(
                    file_system_id.clone(),
                    primary.clone(),
                    operation.clone(),
                    timeout,
                    false,
                    false,
                )?;
            match settled.result {
                DfsNamespaceResult::Applied => return Ok(settled.synced_path_objects),
                DfsNamespaceResult::Rejected {
                    reason: _,
                    current_primary_node_id: Some(current),
                    ..
                } if current == self.node_id => {
                    return self
                        .synced_namespace_objects_for_operation(&file_system_id, &operation);
                }
                DfsNamespaceResult::Rejected {
                    current_primary_node_id: Some(current),
                    ..
                } if current != primary => {
                    let next_primary = self.refreshed_primary_after_authority_redirect(
                        &file_system_id,
                        &operation,
                        &primary,
                        current,
                    )?;
                    if next_primary == self.node_id {
                        return self
                            .synced_namespace_objects_for_operation(&file_system_id, &operation);
                    }
                    primary = next_primary;
                }
                DfsNamespaceResult::Rejected { reason, .. } => {
                    return Err(DfsError::Distributed { message: reason });
                }
            }
        }
        Err(DfsError::Distributed {
            message: format!(
                "routed namespace lookup exceeded {DFS_ROUTED_WRITE_AUTHORITY_REDIRECT_LIMIT} authority redirects"
            ),
        })
    }

    pub(crate) fn route_namespace_operation_to_metadata_primary_without_local_replay(
        &mut self,
        file_system_id: DfsFileSystemId,
        primary_node_id: NodeId,
        operation: DfsNamespaceOperation,
    ) -> Result<DfsNamespaceResult, DfsError> {
        Ok(self
            .route_namespace_operation_to_metadata_primary_with_timeout_and_options(
                file_system_id,
                primary_node_id,
                operation,
                DFS_ROUTED_NAMESPACE_OP_TIMEOUT,
                false,
                false,
            )?
            .result)
    }

    pub(crate) fn refreshed_primary_after_authority_redirect(
        &mut self,
        file_system_id: &DfsFileSystemId,
        operation: &DfsNamespaceOperation,
        attempted_primary: &NodeId,
        redirected_primary: NodeId,
    ) -> Result<NodeId, DfsError> {
        self.refresh_control_plane_latest()?;
        let shards = self.affected_namespace_shards(file_system_id, operation)?;
        let routing_path = operation.routing_path().ok_or_else(|| DfsError::Distributed {
            message: format!(
                "namespace authority redirect from {attempted_primary} to {redirected_primary} has no routing path"
            ),
        })?;
        let Some(dispatch_shard) =
            crate::control_plane::select_metadata_shard_config_for_path_strict_default_sifs_split(
                file_system_id,
                &shards,
                routing_path,
            )
            .map_err(DfsError::from)?
        else {
            return Err(DfsError::Distributed {
                message: format!(
                    "namespace authority redirect from {attempted_primary} to {redirected_primary} has no current routing-shard assignment"
                ),
            });
        };
        if dispatch_shard.primary_node_id != redirected_primary {
            return Err(DfsError::Distributed {
                message: format!(
                    "namespace authority redirect from {attempted_primary} named {redirected_primary}, but current routing shard {} names {}",
                    dispatch_shard.shard_id, dispatch_shard.primary_node_id
                ),
            });
        }
        Ok(redirected_primary)
    }

    pub(crate) fn route_namespace_operation_to_metadata_primary_with_timeout_and_options(
        &mut self,
        file_system_id: DfsFileSystemId,
        primary_node_id: NodeId,
        operation: DfsNamespaceOperation,
        timeout: Duration,
        replay_journal_batches: bool,
        apply_synced_path_object: bool,
    ) -> Result<PendingRoutedNamespaceResult, DfsError> {
        let requested_deadline = Instant::now() + timeout;
        let previous_deadline = self
            .transport
            .set_operation_deadline(Some(requested_deadline));
        let effective_deadline = previous_deadline
            .filter(|existing| *existing < requested_deadline)
            .unwrap_or(requested_deadline);
        if effective_deadline != requested_deadline {
            self.transport
                .set_operation_deadline(Some(effective_deadline));
        }
        let result = self.route_namespace_operation_to_metadata_primary_with_deadline(
            file_system_id,
            primary_node_id,
            operation,
            effective_deadline,
            replay_journal_batches,
            apply_synced_path_object,
        );
        self.transport.set_operation_deadline(previous_deadline);
        result
    }

    fn route_namespace_operation_to_metadata_primary_with_deadline(
        &mut self,
        file_system_id: DfsFileSystemId,
        primary_node_id: NodeId,
        operation: DfsNamespaceOperation,
        deadline: Instant,
        replay_journal_batches: bool,
        apply_synced_path_object: bool,
    ) -> Result<PendingRoutedNamespaceResult, DfsError> {
        let request_id = self.next_request_id();
        let metadata_journal_after_sequences = if replay_journal_batches {
            self.local_metadata_shard_cursors()?
        } else {
            Vec::new()
        };
        let authority_shards = if self.control_plane.is_some() {
            self.affected_namespace_shards(&file_system_id, &operation)?
        } else {
            if replay_journal_batches {
                return Err(DfsError::Distributed {
                    message: "routed namespace journal replay requires a committed control-plane shard view"
                        .to_string(),
                });
            }
            Vec::new()
        };
        let dispatch_authority_shard = if self.control_plane.is_some() {
            let routing_path = operation
                .routing_path()
                .ok_or_else(|| DfsError::Distributed {
                    message: "clustered routed namespace operation has no routing path".to_string(),
                })?;
            let dispatch = crate::control_plane::select_metadata_shard_config_for_path_strict_default_sifs_split(
                &file_system_id,
                &authority_shards,
                routing_path,
            )
            .map_err(DfsError::from)?
            .ok_or_else(|| DfsError::Distributed {
                message: format!(
                    "clustered routed namespace path {routing_path} has no dispatch shard assignment"
                ),
            })?;
            if dispatch.primary_node_id != primary_node_id {
                return Err(DfsError::Distributed {
                    message: format!(
                        "routed namespace target {primary_node_id} is not current dispatch authority {} for routing shard {} epoch {}",
                        dispatch.primary_node_id, dispatch.shard_id, dispatch.epoch
                    ),
                });
            }
            Some(dispatch)
        } else {
            None
        };
        let sent = match self.send_dfs_message_if_routable(
            &primary_node_id,
            DfsNetworkMessage::NamespaceRequest(DfsNamespaceCommand {
                request_id: request_id.clone(),
                file_system_id: file_system_id.clone(),
                requesting_node_id: self.node_id.clone(),
                include_metadata_journal_batches: replay_journal_batches,
                metadata_journal_after_sequences,
                authority_shards: authority_shards.clone(),
                operation,
            }),
        ) {
            Ok(sent) => sent,
            Err(error @ DfsError::Network { .. }) => {
                return Err(error);
            }
            Err(error) => return Err(error),
        };
        if !sent {
            return Err(DfsError::Network {
                message: format!(
                    "metadata primary {primary_node_id} is not routable for namespace operation"
                ),
            });
        }
        let Some(remaining) = deadline
            .checked_duration_since(Instant::now())
            .filter(|duration| !duration.is_zero())
        else {
            return Err(DfsError::Network {
                message: format!(
                    "routed namespace operation {request_id} timed out before response settle"
                ),
            });
        };
        self.pending_routed_namespace_ops.insert(
            request_id.clone(),
            PendingRoutedNamespaceOp {
                primary_node_id,
                dispatch_authority_shard,
                authority_shards,
                replay_journal_batches,
                apply_synced_path_object,
                result: None,
            },
        );
        self.settle_routed_namespace_op(&request_id, remaining)
    }

    fn settle_routed_namespace_op(
        &mut self,
        request_id: &str,
        timeout: Duration,
    ) -> Result<PendingRoutedNamespaceResult, DfsError> {
        self.routed_namespace_settle_depth = self.routed_namespace_settle_depth.saturating_add(1);
        let result = self.settle_routed_namespace_op_inner(request_id, timeout);
        self.routed_namespace_settle_depth = self.routed_namespace_settle_depth.saturating_sub(1);
        result
    }

    fn settle_routed_namespace_op_inner(
        &mut self,
        request_id: &str,
        timeout: Duration,
    ) -> Result<PendingRoutedNamespaceResult, DfsError> {
        let started_at = Instant::now();
        loop {
            if let Err(cancelled) = self.check_cancelled("routed namespace settle") {
                self.pending_routed_namespace_ops.remove(request_id);
                return Err(cancelled);
            }
            let events = match self.process_incoming() {
                Ok(events) => events,
                Err(error) => {
                    self.pending_routed_namespace_ops.remove(request_id);
                    return Err(error);
                }
            };
            if self
                .pending_routed_namespace_ops
                .get(request_id)
                .is_some_and(|pending| pending.result.is_some())
            {
                let pending = self
                    .pending_routed_namespace_ops
                    .remove(request_id)
                    .expect("pending routed namespace op checked above");
                return Ok(pending.result.expect("settled result checked above"));
            }
            let elapsed = started_at.elapsed();
            if elapsed >= timeout {
                self.pending_routed_namespace_ops.remove(request_id);
                return Err(DfsError::Network {
                    message: format!(
                        "routed namespace operation {request_id} timed out waiting for the metadata primary"
                    ),
                });
            }
            if events.is_empty() {
                let wait = DFS_ROUTED_NAMESPACE_IDLE_WAIT.min(timeout.saturating_sub(elapsed));
                self.transport
                    .wait_for_incoming(wait)
                    .map_err(DfsError::from_network)?;
            }
        }
    }
}

#[cfg(test)]
mod admission_tests {
    use super::*;
    use sinomo_core::DfsObjectPolicy;

    fn adoption_object() -> DfsObjectMetadata {
        DfsObjectMetadata {
            file_system_id: DfsFileSystemId::from_legacy_named("sifs").expect("filesystem"),
            object_id: DfsObjectId::from_legacy_named("object_pin_adoption").expect("object"),
            path: "/services/pinned.txt".to_string(),
            kind: DfsObjectKind::File,
            tombstone_provenance: None,
            owner_node_id: NodeId::from_legacy_named("node_target").expect("node"),
            current_locations: Vec::new(),
            content_hash: Some("blake3:test".to_string()),
            content_size_bytes: Some(4),
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 1,
            modified_at_unix_nanos: 1,
            created_at_unix_nanos: 1,
            changed_at_unix_nanos: 1,
            version: DfsVersion::from_legacy_named("version_pin_adoption").expect("version"),
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        }
    }

    #[test]
    fn pin_aware_adoption_bypasses_durable_admission_that_cannot_preserve_typed_receipts() {
        let operation = DfsNamespaceOperation::AdoptObjectAtPath {
            object: adoption_object(),
            source_path: "/users/pinned.txt".to_string(),
            pin_transfer_id: Some("pin-transfer".to_string()),
            versions: Vec::new(),
            version_pins: Vec::new(),
            version_pin_set_fingerprint: Some("blake3:pins".to_string()),
            receipts: Vec::new(),
            intents: Vec::new(),
            intent_receipts: Vec::new(),
            intent_receipt_retention: DfsWriteIntentReceiptRetentionState::default(),
            conflicts: Vec::new(),
            advisory_lock_state: None,
        };

        assert!(namespace_operation_requires_fresh_authority(&operation));
        assert!(!namespace_operation_uses_durable_request_admission(
            &operation
        ));
        assert!(!namespace_operation_can_fuse_durable_request_admission(
            &operation
        ));
        assert!(namespace_operation_response_retry_safe(&operation));
    }

    #[test]
    fn pin_transfer_finalization_stamps_only_the_destination_authority() {
        let operation = DfsNamespaceOperation::FinalizeObjectFamilyAdoption {
            path: "/services/pinned.txt".to_string(),
            source_path: "/users/pinned.txt".to_string(),
            object_id: DfsObjectId::from_legacy_named("object_pin_finalize").expect("object"),
            target_version: DfsVersion::from_legacy_named("version_pin_finalize").expect("version"),
            pin_transfer_id: "pin-transfer".to_string(),
        };

        assert_eq!(operation.routing_path(), Some("/services/pinned.txt"));
        assert_eq!(operation.involved_paths(), vec!["/services/pinned.txt"]);
        assert!(namespace_operation_requires_fresh_authority(&operation));
        assert!(namespace_operation_response_retry_safe(&operation));
    }
}
