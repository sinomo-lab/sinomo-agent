//! In-memory materialized metadata tables.
//!
//! Durable table IO lives in `metadata_table_store`; this module owns the
//! table maps, secondary indexes, and rollback bookkeeping used while
//! materializing journal entries.

#[cfg(test)]
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Deref;
use std::sync::Arc;

use sinomo_core::{Id, node::NodeId};
use time::OffsetDateTime;

use crate::advisory_lock::DfsAdvisoryLockStateRecord;
use crate::metadata_error::{MetadataBackendError, Result};
use crate::metadata_keys::{
    dentry_key, intent_key, normalize_path, write_intent_scan_key, write_intent_target_key,
};
use crate::metadata_namespace_transaction::{
    materialize_namespace_transaction_table_state,
    preapply_namespace_transaction_table_state_with_rollback,
};
use crate::metadata_object_index::object_index_table_ops_with_previous;
use crate::metadata_records::{
    DfsConflictRecord, DfsCrossShardPinTransferRecord, DfsDirFragmentId, DfsDirFragmentRecord,
    DfsInodeId, DfsInodeRecord, DfsMetadataPrefixProof, DfsMetadataTableOp,
    DfsMetadataVersionPinRecord, DfsNamespaceJobReceipt, DfsNamespaceJobRecord,
    DfsNamespacePrepareReceipt, DfsNamespacePrepareRecord, DfsNamespacePrepareStatus,
    DfsNamespaceReceiptOrderKey, DfsNamespaceReceiptRetentionState, DfsNamespaceRequestReceipt,
    DfsNamespaceRequestRecord, DfsNamespaceRequestStatus, DfsNamespaceTransaction,
    DfsNamespaceTransactionReceipt, DfsObjectVersionRecord, DfsPathRecord, DfsReplicaReceiptRecord,
    DfsVisibleVersionCommitRecord, DfsWriteIntentReceipt, DfsWriteIntentReceiptRetentionState,
    DfsWriteIntentRecord, normalized_object_metadata,
};
use crate::{
    DfsFileSystem, DfsFileSystemId, DfsObjectId, DfsObjectKind, DfsObjectMetadata, DfsVersion,
};

#[cfg(test)]
thread_local! {
    static NAMESPACE_COMMIT_ROLLBACK_MATERIALIZATIONS: Cell<usize> = const { Cell::new(0) };
    static NAMESPACE_COMMIT_DIRECT_MATERIALIZATIONS: Cell<usize> = const { Cell::new(0) };
    static NAMESPACE_PREPARE_ROLLBACK_MATERIALIZATIONS: Cell<usize> = const { Cell::new(0) };
    static NAMESPACE_PREPARE_POST_APPEND_MATERIALIZATIONS: Cell<usize> = const { Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn reset_namespace_commit_materialization_counters() {
    NAMESPACE_COMMIT_ROLLBACK_MATERIALIZATIONS.set(0);
    NAMESPACE_COMMIT_DIRECT_MATERIALIZATIONS.set(0);
}

#[cfg(test)]
pub(crate) fn namespace_commit_materialization_counters() -> (usize, usize) {
    (
        NAMESPACE_COMMIT_ROLLBACK_MATERIALIZATIONS.get(),
        NAMESPACE_COMMIT_DIRECT_MATERIALIZATIONS.get(),
    )
}

#[cfg(test)]
pub(crate) fn reset_namespace_prepare_materialization_counters() {
    NAMESPACE_PREPARE_ROLLBACK_MATERIALIZATIONS.set(0);
    NAMESPACE_PREPARE_POST_APPEND_MATERIALIZATIONS.set(0);
}

#[cfg(test)]
pub(crate) fn namespace_prepare_materialization_counters() -> (usize, usize) {
    (
        NAMESPACE_PREPARE_ROLLBACK_MATERIALIZATIONS.get(),
        NAMESPACE_PREPARE_POST_APPEND_MATERIALIZATIONS.get(),
    )
}

fn completed_namespace_request_outcome_matches(
    left: &DfsNamespaceRequestRecord,
    right: &DfsNamespaceRequestRecord,
) -> bool {
    left.file_system_id == right.file_system_id
        && left.requester_node_id == right.requester_node_id
        && left.request_id == right.request_id
        && left.operation_id == right.operation_id
        && left.authority_shard_id == right.authority_shard_id
        && left.operation == right.operation
        && left.command_digest == right.command_digest
        && left.created_at == right.created_at
        && left.status == DfsNamespaceRequestStatus::Completed
        && right.status == DfsNamespaceRequestStatus::Completed
        && left.result == right.result
        && left.posix_coordination_result == right.posix_coordination_result
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct DfsMetadataTables {
    pub(crate) file_systems: BTreeMap<DfsFileSystemId, DfsFileSystem>,
    pub(crate) objects: BTreeMap<DfsObjectId, DfsObjectMetadata>,
    pub(crate) file_system_objects: BTreeMap<(DfsFileSystemId, DfsObjectId), DfsPathRecord>,
    pub(crate) paths: BTreeMap<(DfsFileSystemId, String), DfsPathRecord>,
    pub(crate) inodes: BTreeMap<DfsInodeId, DfsInodeRecord>,
    pub(crate) dirfrags: BTreeMap<(DfsInodeId, DfsDirFragmentId), DfsDirFragmentRecord>,
    pub(crate) dentries: BTreeMap<(DfsInodeId, DfsDirFragmentId, u64, String), DfsPathRecord>,
    pub(crate) hardlinks: BTreeMap<(DfsObjectId, DfsObjectId), DfsPathRecord>,
    pub(crate) hardlink_paths: BTreeMap<(DfsObjectId, String, DfsObjectId), DfsPathRecord>,
    pub(crate) compacted_tombstones: BTreeMap<DfsObjectId, DfsObjectMetadata>,
    pub(crate) compacted_tombstone_paths: BTreeMap<(DfsFileSystemId, String), DfsObjectId>,
    pub(crate) versions: BTreeMap<(DfsObjectId, DfsVersion), DfsObjectVersionRecord>,
    pub(crate) version_pins:
        BTreeMap<(DfsObjectId, DfsVersion, String), DfsMetadataVersionPinRecord>,
    pub(crate) cross_shard_pin_transfers:
        BTreeMap<(DfsObjectId, String), DfsCrossShardPinTransferRecord>,
    pub(crate) receipts: BTreeMap<(DfsObjectId, DfsVersion, NodeId), DfsReplicaReceiptRecord>,
    pub(crate) intents: BTreeMap<(DfsObjectId, String), DfsWriteIntentRecord>,
    pub(crate) intent_receipts: BTreeMap<(DfsObjectId, String), DfsWriteIntentReceipt>,
    pub(crate) intent_receipt_retention: DfsWriteIntentReceiptRetentionState,
    pub(crate) intent_order: BTreeMap<(OffsetDateTime, String, DfsObjectId), (DfsObjectId, String)>,
    pub(crate) intent_targets: BTreeMap<(DfsObjectId, DfsVersion, String), (DfsObjectId, String)>,
    pub(crate) conflicts: BTreeMap<(DfsObjectId, String), DfsConflictRecord>,
    pub(crate) advisory_lock_states: BTreeMap<DfsObjectId, DfsAdvisoryLockStateRecord>,
    pub(crate) visible_commits: BTreeMap<(DfsObjectId, DfsVersion), DfsVisibleVersionCommitRecord>,
    pub(crate) namespace_transactions: BTreeMap<String, DfsNamespaceTransaction>,
    pub(crate) namespace_transaction_receipts: BTreeMap<String, DfsNamespaceTransactionReceipt>,
    pub(crate) namespace_prepares: BTreeMap<String, DfsNamespacePrepareRecord>,
    pub(crate) namespace_prepare_receipts: BTreeMap<String, DfsNamespacePrepareReceipt>,
    namespace_prepare_object_owners:
        BTreeMap<DfsFileSystemId, BTreeMap<DfsObjectId, BTreeSet<String>>>,
    namespace_prepare_path_owners: BTreeMap<DfsFileSystemId, BTreeMap<String, BTreeSet<String>>>,
    /// Derived from active request-owned prepares and rebuilt on snapshot load.
    pub(crate) namespace_prepare_request_owners: BTreeMap<(NodeId, String), BTreeSet<String>>,
    pub(crate) namespace_jobs: BTreeMap<String, DfsNamespaceJobRecord>,
    pub(crate) namespace_job_receipts: BTreeMap<String, DfsNamespaceJobReceipt>,
    pub(crate) namespace_requests: BTreeMap<(NodeId, String), DfsNamespaceRequestRecord>,
    pub(crate) namespace_request_receipts: BTreeMap<(NodeId, String), DfsNamespaceRequestReceipt>,
    pub(crate) namespace_receipt_retention: DfsNamespaceReceiptRetentionState,
    /// Derived oldest-attempt-first recovery index. Terminal idempotency rows
    /// stay in `namespace_requests` without participating in maintenance scans.
    pub(crate) pending_namespace_request_order: BTreeSet<(OffsetDateTime, NodeId, String)>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct DfsMetadataTableState {
    pub(crate) tables: DfsMetadataTables,
    pub(crate) table_log_offset: u64,
    pub(crate) table_log_journal_reference_replay_cost_bytes: u64,
    pub(crate) table_log_unpriced_namespace_prepare_references: u64,
    pub(crate) materialized_through_sequence: Option<u64>,
    pub(crate) snapshot_format_version: Option<u16>,
    pub(crate) snapshot_fingerprint: Option<MetadataTableSnapshotFingerprint>,
    pub(crate) prefix_proof: Option<DfsMetadataPrefixProof>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MetadataTableSnapshotFingerprint {
    pub(crate) len: u64,
    pub(crate) modified_unix_nanos: Option<u128>,
}

#[derive(Clone, Debug)]
pub(crate) struct MetadataTableReadSnapshotCache {
    pub(crate) snapshot_fingerprint: Option<MetadataTableSnapshotFingerprint>,
    pub(crate) checkpoint_fingerprint: Option<MetadataTableSnapshotFingerprint>,
    pub(crate) table_log_offset: u64,
    pub(crate) tables: Arc<DfsMetadataTables>,
}

pub(crate) enum DfsMetadataTablesSnapshot<'a> {
    Borrowed(&'a DfsMetadataTables),
    Shared(Arc<DfsMetadataTables>),
}

impl Deref for DfsMetadataTablesSnapshot<'_> {
    type Target = DfsMetadataTables;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Borrowed(tables) => tables,
            Self::Shared(tables) => tables,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct MetadataTableReplay {
    pub(crate) offset: u64,
    pub(crate) journal_reference_replay_cost_bytes: u64,
    pub(crate) unpriced_namespace_prepare_references: u64,
    pub(crate) materialized_through_sequence: Option<u64>,
    pub(crate) prefix_proof: Option<DfsMetadataPrefixProof>,
}

pub(crate) fn max_optional_sequence(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(sequence), None) | (None, Some(sequence)) => Some(sequence),
        (None, None) => None,
    }
}

pub(crate) fn namespace_prepare_footprint(
    prepare: &DfsNamespacePrepareRecord,
) -> (BTreeSet<DfsObjectId>, BTreeSet<String>) {
    let mut object_ids = prepare
        .expected_object_versions
        .iter()
        .map(|expected| expected.object_id.clone())
        .collect::<BTreeSet<_>>();
    object_ids.extend(
        prepare
            .transaction
            .objects
            .iter()
            .map(|object| object.object_id.clone()),
    );
    let mut paths = prepare
        .expected_path_occupants
        .iter()
        .map(|expected| normalize_path(&expected.path))
        .collect::<BTreeSet<_>>();
    paths.extend(
        prepare
            .transaction
            .objects
            .iter()
            .map(|object| normalize_path(&object.path)),
    );
    (object_ids, paths)
}

fn namespace_transaction_order_key(
    transaction: &DfsNamespaceTransaction,
) -> DfsNamespaceReceiptOrderKey {
    DfsNamespaceReceiptOrderKey {
        created_at: transaction.created_at,
        identity: transaction.transaction_id.clone(),
    }
}

fn namespace_prepare_order_key(prepare: &DfsNamespacePrepareRecord) -> DfsNamespaceReceiptOrderKey {
    DfsNamespaceReceiptOrderKey {
        created_at: prepare.transaction.created_at,
        identity: prepare.operation_id.clone(),
    }
}

fn namespace_job_order_key(
    created_at: OffsetDateTime,
    job_id: &str,
) -> DfsNamespaceReceiptOrderKey {
    DfsNamespaceReceiptOrderKey {
        created_at,
        identity: job_id.to_string(),
    }
}

fn namespace_request_order_token(request_id: &str) -> Option<String> {
    let candidate = request_id.rsplit('_').next().unwrap_or(request_id);
    Id::new(candidate.to_string())
        .ok()
        .map(|id| id.as_str().to_ascii_lowercase())
}

pub(crate) fn namespace_transaction_receipt_eviction_delta(
    receipt: &DfsNamespaceTransactionReceipt,
) -> DfsNamespaceReceiptRetentionState {
    DfsNamespaceReceiptRetentionState {
        transaction_floor: Some(DfsNamespaceReceiptOrderKey {
            created_at: receipt.created_at,
            identity: receipt.transaction_id.clone(),
        }),
        ..DfsNamespaceReceiptRetentionState::default()
    }
}

pub(crate) fn namespace_prepare_receipt_eviction_delta(
    receipt: &DfsNamespacePrepareReceipt,
) -> DfsNamespaceReceiptRetentionState {
    DfsNamespaceReceiptRetentionState {
        prepare_floor: Some(DfsNamespaceReceiptOrderKey {
            created_at: receipt.created_at,
            identity: receipt.operation_id.clone(),
        }),
        ..DfsNamespaceReceiptRetentionState::default()
    }
}

pub(crate) fn namespace_job_receipt_eviction_delta(
    receipt: &DfsNamespaceJobReceipt,
) -> DfsNamespaceReceiptRetentionState {
    DfsNamespaceReceiptRetentionState {
        job_floor: Some(namespace_job_order_key(receipt.created_at, &receipt.job_id)),
        ..DfsNamespaceReceiptRetentionState::default()
    }
}

pub(crate) fn namespace_request_receipt_eviction_delta(
    receipt: &DfsNamespaceRequestReceipt,
) -> DfsNamespaceReceiptRetentionState {
    let mut retention = DfsNamespaceReceiptRetentionState::default();
    if let Some(token) = namespace_request_order_token(&receipt.request_id) {
        retention
            .max_evicted_request_order_token_by_requester
            .insert(receipt.requester_node_id.clone(), token);
    } else {
        retention
            .opaque_request_history_evicted_requester_node_ids
            .insert(receipt.requester_node_id.clone());
    }
    retention
}

impl DfsMetadataTables {
    /// Apply one table operation while preserving fail-closed validation for
    /// compact semantic transitions.
    ///
    /// Ordinary operations are self-contained and retain the historical
    /// infallible apply path. A committed-prepare marker intentionally refers
    /// to the full prepared body in the preceding table state, so durable-log
    /// replay uses this checked entry point and rejects a missing or mismatched
    /// dependency instead of silently publishing partial metadata.
    pub(crate) fn apply_checked(&mut self, op: DfsMetadataTableOp) -> Result<()> {
        match op {
            DfsMetadataTableOp::PutNamespacePrepareJournalReference {
                journal_sequence,
                operation_id,
                ..
            } => Err(MetadataBackendError::Serde(format!(
                "namespace prepare {operation_id} table reference at journal sequence {journal_sequence} was not resolved"
            ))),
            DfsMetadataTableOp::CommitNamespacePrepare {
                operation_id,
                operation_digest,
                owner_epoch,
            } => self.materialize_committed_namespace_prepare(
                &operation_id,
                &operation_digest,
                owner_epoch,
            ),
            op => {
                #[cfg(test)]
                if matches!(&op, DfsMetadataTableOp::PutNamespacePrepare { .. }) {
                    NAMESPACE_PREPARE_POST_APPEND_MATERIALIZATIONS.set(
                        NAMESPACE_PREPARE_POST_APPEND_MATERIALIZATIONS
                            .get()
                            .saturating_add(1),
                    );
                }
                self.apply(op);
                Ok(())
            }
        }
    }

    /// Validate any dependency referenced by a compact semantic operation
    /// without cloning or mutating its full prepared body.
    pub(crate) fn validate_checked(&self, op: &DfsMetadataTableOp) -> Result<()> {
        if let DfsMetadataTableOp::PutNamespacePrepareJournalReference {
            journal_sequence,
            operation_id,
            ..
        } = op
        {
            return Err(MetadataBackendError::Serde(format!(
                "namespace prepare {operation_id} table reference at journal sequence {journal_sequence} was not resolved"
            )));
        }
        if let DfsMetadataTableOp::CommitNamespacePrepare {
            operation_id,
            operation_digest,
            owner_epoch,
        } = op
        {
            self.validated_committed_namespace_prepare(
                operation_id,
                operation_digest,
                *owner_epoch,
            )?;
        }
        Ok(())
    }

    pub(crate) fn apply_checked_with_rollback(
        &mut self,
        op: DfsMetadataTableOp,
        rollback: &mut DfsMetadataTableRollback,
    ) -> Result<()> {
        if let DfsMetadataTableOp::PutNamespacePrepareJournalReference {
            journal_sequence,
            operation_id,
            ..
        } = &op
        {
            return Err(MetadataBackendError::Serde(format!(
                "namespace prepare {operation_id} table reference at journal sequence {journal_sequence} was not resolved"
            )));
        }
        let DfsMetadataTableOp::CommitNamespacePrepare {
            operation_id,
            operation_digest,
            owner_epoch,
        } = op
        else {
            self.apply_with_rollback(op, rollback);
            return Ok(());
        };
        #[cfg(test)]
        NAMESPACE_COMMIT_ROLLBACK_MATERIALIZATIONS.set(
            NAMESPACE_COMMIT_ROLLBACK_MATERIALIZATIONS
                .get()
                .saturating_add(1),
        );
        let mut prepare =
            self.committed_namespace_prepare(&operation_id, &operation_digest, owner_epoch)?;
        for receipt in &prepare.content_receipts {
            self.apply_with_rollback(
                DfsMetadataTableOp::PutReceipt {
                    receipt: receipt.clone(),
                },
                rollback,
            );
        }
        preapply_namespace_transaction_table_state_with_rollback(
            self,
            &prepare.transaction,
            rollback,
        );
        prepare.status = DfsNamespacePrepareStatus::Committed;
        self.apply_with_rollback(
            DfsMetadataTableOp::PutNamespacePrepare {
                prepare: prepare.clone(),
            },
            rollback,
        );
        if let Some(request) = prepare.completed_request {
            let key = (
                request.requester_node_id.clone(),
                request.request_id.clone(),
            );
            let materialized = self.namespace_requests.get(&key).is_some_and(|existing| {
                completed_namespace_request_outcome_matches(existing, &request)
            }) || self
                .namespace_request_receipts
                .get(&key)
                .is_some_and(|receipt| receipt.matches_completed_request(&request));
            if !materialized {
                self.apply_with_rollback(
                    DfsMetadataTableOp::PutNamespaceRequest { request },
                    rollback,
                );
            }
        } else if let Some(completion) = prepare.completed_request_completion {
            let key = completion.request_key();
            let materialized = self
                .namespace_requests
                .get(&key)
                .is_some_and(|request| completion.matches_completed_request(request))
                || self
                    .namespace_request_receipts
                    .get(&key)
                    .is_some_and(|receipt| receipt.matches_completion(&completion));
            if !materialized {
                self.apply_with_rollback(
                    DfsMetadataTableOp::PutNamespaceRequestReceipt {
                        receipt: DfsNamespaceRequestReceipt::from_completion(&completion),
                    },
                    rollback,
                );
            }
        }
        Ok(())
    }

    fn committed_namespace_prepare(
        &self,
        operation_id: &str,
        operation_digest: &str,
        owner_epoch: u64,
    ) -> Result<DfsNamespacePrepareRecord> {
        self.validated_committed_namespace_prepare(operation_id, operation_digest, owner_epoch)
            .cloned()
    }

    fn validated_committed_namespace_prepare(
        &self,
        operation_id: &str,
        operation_digest: &str,
        owner_epoch: u64,
    ) -> Result<&DfsNamespacePrepareRecord> {
        let prepare = self.namespace_prepares.get(operation_id).ok_or_else(|| {
            MetadataBackendError::Serde(format!(
                "compact namespace commit {operation_id} has no preceding durable prepare"
            ))
        })?;
        if prepare.operation_digest != operation_digest || prepare.owner_epoch != owner_epoch {
            return Err(MetadataBackendError::Serde(format!(
                "compact namespace commit {operation_id} digest/epoch fence mismatch"
            )));
        }
        if prepare.status == DfsNamespacePrepareStatus::Aborted {
            return Err(MetadataBackendError::Serde(format!(
                "compact namespace commit {operation_id} refers to an aborted prepare"
            )));
        }
        Ok(prepare)
    }

    fn materialize_committed_namespace_prepare(
        &mut self,
        operation_id: &str,
        operation_digest: &str,
        owner_epoch: u64,
    ) -> Result<()> {
        #[cfg(test)]
        NAMESPACE_COMMIT_DIRECT_MATERIALIZATIONS.set(
            NAMESPACE_COMMIT_DIRECT_MATERIALIZATIONS
                .get()
                .saturating_add(1),
        );
        let mut prepare =
            self.committed_namespace_prepare(operation_id, operation_digest, owner_epoch)?;
        for receipt in &prepare.content_receipts {
            self.apply(DfsMetadataTableOp::PutReceipt {
                receipt: receipt.clone(),
            });
        }
        materialize_namespace_transaction_table_state(self, &prepare.transaction);
        prepare.status = DfsNamespacePrepareStatus::Committed;
        self.apply(DfsMetadataTableOp::PutNamespacePrepare {
            prepare: prepare.clone(),
        });
        if let Some(request) = prepare.completed_request {
            let key = (
                request.requester_node_id.clone(),
                request.request_id.clone(),
            );
            let materialized = self.namespace_requests.get(&key).is_some_and(|existing| {
                completed_namespace_request_outcome_matches(existing, &request)
            }) || self
                .namespace_request_receipts
                .get(&key)
                .is_some_and(|receipt| receipt.matches_completed_request(&request));
            if !materialized {
                self.apply(DfsMetadataTableOp::PutNamespaceRequest { request });
            }
        } else if let Some(completion) = prepare.completed_request_completion {
            let key = completion.request_key();
            let materialized = self
                .namespace_requests
                .get(&key)
                .is_some_and(|request| completion.matches_completed_request(request))
                || self
                    .namespace_request_receipts
                    .get(&key)
                    .is_some_and(|receipt| receipt.matches_completion(&completion));
            if !materialized {
                self.apply(DfsMetadataTableOp::PutNamespaceRequestReceipt {
                    receipt: DfsNamespaceRequestReceipt::from_completion(&completion),
                });
            }
        }
        Ok(())
    }

    pub(crate) fn cross_shard_pin_transfer_owner(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        pin_id: &str,
    ) -> Option<&DfsCrossShardPinTransferRecord> {
        self.cross_shard_pin_transfers.values().find(|transfer| {
            transfer.copied_pins.iter().any(|pin| {
                &pin.object_id == object_id && &pin.version == version && pin.pin_id == pin_id
            })
        })
    }

    pub(crate) fn cross_shard_pin_transfer_fence(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        pin_id: &str,
    ) -> Option<&DfsCrossShardPinTransferRecord> {
        self.cross_shard_pin_transfers.values().find(|transfer| {
            transfer.source_pins.iter().any(|pin| {
                &pin.object_id == object_id && &pin.version == version && pin.pin_id == pin_id
            })
        })
    }

    pub(crate) fn has_materialized_state(&self) -> bool {
        !self.file_systems.is_empty()
            || !self.objects.is_empty()
            || !self.versions.is_empty()
            || !self.version_pins.is_empty()
            || !self.cross_shard_pin_transfers.is_empty()
            || !self.receipts.is_empty()
            || !self.intents.is_empty()
            || !self.intent_receipts.is_empty()
            || !self.intent_receipt_retention.is_empty()
            || !self.conflicts.is_empty()
            || !self.advisory_lock_states.is_empty()
            || !self.namespace_transactions.is_empty()
            || !self.namespace_transaction_receipts.is_empty()
            || !self.namespace_jobs.is_empty()
            || !self.namespace_job_receipts.is_empty()
            || !self.namespace_requests.is_empty()
            || !self.namespace_request_receipts.is_empty()
            || !self.compacted_tombstones.is_empty()
            || !self.visible_commits.is_empty()
            || !self.namespace_prepares.is_empty()
            || !self.namespace_prepare_receipts.is_empty()
            || self.namespace_receipt_retention != DfsNamespaceReceiptRetentionState::default()
    }

    pub(crate) fn namespace_transaction_history_expired(
        &self,
        transaction: &DfsNamespaceTransaction,
    ) -> bool {
        self.namespace_receipt_retention
            .transaction_floor
            .as_ref()
            .is_some_and(|floor| namespace_transaction_order_key(transaction) <= *floor)
    }

    pub(crate) fn namespace_prepare_history_expired(
        &self,
        prepare: &DfsNamespacePrepareRecord,
    ) -> bool {
        self.namespace_receipt_retention
            .prepare_floor
            .as_ref()
            .is_some_and(|floor| namespace_prepare_order_key(prepare) <= *floor)
    }

    pub(crate) fn namespace_prepare_history_has_evictions(&self) -> bool {
        self.namespace_receipt_retention.prepare_floor.is_some()
    }

    pub(crate) fn namespace_job_history_expired(
        &self,
        created_at: OffsetDateTime,
        job_id: &str,
    ) -> bool {
        self.namespace_receipt_retention
            .job_floor
            .as_ref()
            .is_some_and(|floor| namespace_job_order_key(created_at, job_id) <= *floor)
    }

    pub(crate) fn namespace_request_history_expired(
        &self,
        requester_node_id: &NodeId,
        request_id: &str,
    ) -> bool {
        match namespace_request_order_token(request_id) {
            Some(token) => self
                .namespace_receipt_retention
                .max_evicted_request_order_token_by_requester
                .get(requester_node_id)
                .is_some_and(|floor| token <= *floor),
            None => self
                .namespace_receipt_retention
                .opaque_request_history_evicted_requester_node_ids
                .contains(requester_node_id),
        }
    }

    fn put_intent_indexed(&mut self, intent: DfsWriteIntentRecord) {
        let key = (intent.object_id.clone(), intent.intent_id.clone());
        if let Some(previous) = self.intents.insert(key.clone(), intent.clone()) {
            self.intent_order.remove(&write_intent_scan_key(&previous));
            self.intent_targets
                .remove(&write_intent_target_key(&previous));
        }
        self.intent_order
            .insert(write_intent_scan_key(&intent), key);
        self.intent_targets
            .insert(write_intent_target_key(&intent), intent_key(&intent));
    }

    fn remove_intent_indexed(&mut self, object_id: &DfsObjectId, intent_id: &str) {
        let key = (object_id.clone(), intent_id.to_string());
        if let Some(previous) = self.intents.remove(&key) {
            self.intent_order.remove(&write_intent_scan_key(&previous));
            self.intent_targets
                .remove(&write_intent_target_key(&previous));
        }
    }

    fn rebuild_intent_indexes(&mut self) {
        self.intent_order.clear();
        self.intent_targets.clear();
        for intent in self.intents.values() {
            let key = intent_key(intent);
            self.intent_order
                .insert(write_intent_scan_key(intent), key.clone());
            self.intent_targets
                .insert(write_intent_target_key(intent), key);
        }
    }

    fn namespace_prepare_footprint_indexes_from_history(
        namespace_prepares: &BTreeMap<String, DfsNamespacePrepareRecord>,
    ) -> (
        BTreeMap<DfsFileSystemId, BTreeMap<DfsObjectId, BTreeSet<String>>>,
        BTreeMap<DfsFileSystemId, BTreeMap<String, BTreeSet<String>>>,
    ) {
        let mut object_owners =
            BTreeMap::<DfsFileSystemId, BTreeMap<DfsObjectId, BTreeSet<String>>>::new();
        let mut path_owners =
            BTreeMap::<DfsFileSystemId, BTreeMap<String, BTreeSet<String>>>::new();
        for prepare in namespace_prepares
            .values()
            .filter(|prepare| prepare.status == DfsNamespacePrepareStatus::Prepared)
        {
            let (object_ids, paths) = namespace_prepare_footprint(prepare);
            for object_id in object_ids {
                object_owners
                    .entry(prepare.file_system_id.clone())
                    .or_default()
                    .entry(object_id)
                    .or_default()
                    .insert(prepare.operation_id.clone());
            }
            for path in paths {
                path_owners
                    .entry(prepare.file_system_id.clone())
                    .or_default()
                    .entry(path)
                    .or_default()
                    .insert(prepare.operation_id.clone());
            }
        }
        (object_owners, path_owners)
    }

    fn rebuild_namespace_prepare_footprint_indexes(&mut self) {
        let (object_owners, path_owners) =
            Self::namespace_prepare_footprint_indexes_from_history(&self.namespace_prepares);
        self.namespace_prepare_object_owners = object_owners;
        self.namespace_prepare_path_owners = path_owners;
    }

    fn namespace_prepare_request_key(
        prepare: &DfsNamespacePrepareRecord,
    ) -> Option<(NodeId, String)> {
        prepare
            .completed_request
            .as_ref()
            .map(|request| {
                (
                    request.requester_node_id.clone(),
                    request.request_id.clone(),
                )
            })
            .or_else(|| {
                prepare
                    .completed_request_completion
                    .as_ref()
                    .map(|completion| completion.request_key())
            })
    }

    fn index_namespace_prepare_request(&mut self, prepare: &DfsNamespacePrepareRecord) {
        if prepare.status != DfsNamespacePrepareStatus::Prepared {
            return;
        }
        let Some(request_key) = Self::namespace_prepare_request_key(prepare) else {
            return;
        };
        self.namespace_prepare_request_owners
            .entry(request_key)
            .or_default()
            .insert(prepare.operation_id.clone());
    }

    fn namespace_prepare_request_index_from_history(
        namespace_prepares: &BTreeMap<String, DfsNamespacePrepareRecord>,
    ) -> BTreeMap<(NodeId, String), BTreeSet<String>> {
        let mut request_owners = BTreeMap::<(NodeId, String), BTreeSet<String>>::new();
        for prepare in namespace_prepares
            .values()
            .filter(|prepare| prepare.status == DfsNamespacePrepareStatus::Prepared)
        {
            let Some(request_key) = Self::namespace_prepare_request_key(prepare) else {
                continue;
            };
            request_owners
                .entry(request_key)
                .or_default()
                .insert(prepare.operation_id.clone());
        }
        request_owners
    }

    fn unindex_namespace_prepare_request(&mut self, prepare: &DfsNamespacePrepareRecord) {
        if prepare.status != DfsNamespacePrepareStatus::Prepared {
            return;
        }
        let Some(request_key) = Self::namespace_prepare_request_key(prepare) else {
            return;
        };
        let remove_request = self
            .namespace_prepare_request_owners
            .get_mut(&request_key)
            .is_some_and(|owners| {
                owners.remove(&prepare.operation_id);
                owners.is_empty()
            });
        if remove_request {
            self.namespace_prepare_request_owners.remove(&request_key);
        }
    }

    fn rebuild_namespace_prepare_request_index(&mut self) {
        self.namespace_prepare_request_owners =
            Self::namespace_prepare_request_index_from_history(&self.namespace_prepares);
    }

    fn index_namespace_prepare_footprint(&mut self, prepare: &DfsNamespacePrepareRecord) {
        if prepare.status != DfsNamespacePrepareStatus::Prepared {
            return;
        }
        let (object_ids, paths) = namespace_prepare_footprint(prepare);
        for object_id in object_ids {
            self.namespace_prepare_object_owners
                .entry(prepare.file_system_id.clone())
                .or_default()
                .entry(object_id)
                .or_default()
                .insert(prepare.operation_id.clone());
        }
        for path in paths {
            self.namespace_prepare_path_owners
                .entry(prepare.file_system_id.clone())
                .or_default()
                .entry(path)
                .or_default()
                .insert(prepare.operation_id.clone());
        }
    }

    fn unindex_namespace_prepare_footprint(&mut self, prepare: &DfsNamespacePrepareRecord) {
        if prepare.status != DfsNamespacePrepareStatus::Prepared {
            return;
        }
        let (object_ids, paths) = namespace_prepare_footprint(prepare);
        for object_id in object_ids {
            let remove_file_system = self
                .namespace_prepare_object_owners
                .get_mut(&prepare.file_system_id)
                .is_some_and(|objects| {
                    let remove_object = objects.get_mut(&object_id).is_some_and(|owners| {
                        owners.remove(&prepare.operation_id);
                        owners.is_empty()
                    });
                    if remove_object {
                        objects.remove(&object_id);
                    }
                    objects.is_empty()
                });
            if remove_file_system {
                self.namespace_prepare_object_owners
                    .remove(&prepare.file_system_id);
            }
        }
        for path in paths {
            let remove_file_system = self
                .namespace_prepare_path_owners
                .get_mut(&prepare.file_system_id)
                .is_some_and(|paths| {
                    let remove_path = paths.get_mut(&path).is_some_and(|owners| {
                        owners.remove(&prepare.operation_id);
                        owners.is_empty()
                    });
                    if remove_path {
                        paths.remove(&path);
                    }
                    paths.is_empty()
                });
            if remove_file_system {
                self.namespace_prepare_path_owners
                    .remove(&prepare.file_system_id);
            }
        }
    }

    fn put_namespace_prepare_indexed(&mut self, prepare: DfsNamespacePrepareRecord) {
        let operation_id = prepare.operation_id.clone();
        if let Some(previous) = self.namespace_prepares.remove(&operation_id) {
            self.unindex_namespace_prepare_footprint(&previous);
            self.unindex_namespace_prepare_request(&previous);
        }
        self.index_namespace_prepare_footprint(&prepare);
        self.index_namespace_prepare_request(&prepare);
        self.namespace_prepares.insert(operation_id, prepare);
    }

    fn namespace_request_order_key(
        request: &DfsNamespaceRequestRecord,
    ) -> (OffsetDateTime, NodeId, String) {
        (
            request.updated_at,
            request.requester_node_id.clone(),
            request.request_id.clone(),
        )
    }

    fn put_namespace_request_indexed(&mut self, request: DfsNamespaceRequestRecord) {
        let request_key = (
            request.requester_node_id.clone(),
            request.request_id.clone(),
        );
        if let Some(previous) = self.namespace_requests.remove(&request_key)
            && previous.status == DfsNamespaceRequestStatus::Pending
        {
            self.pending_namespace_request_order
                .remove(&Self::namespace_request_order_key(&previous));
        }
        if request.status == DfsNamespaceRequestStatus::Pending {
            self.pending_namespace_request_order
                .insert(Self::namespace_request_order_key(&request));
        }
        self.namespace_requests.insert(request_key, request);
    }

    fn rebuild_pending_namespace_request_index(&mut self) {
        self.pending_namespace_request_order = self
            .namespace_requests
            .values()
            .filter(|request| request.status == DfsNamespaceRequestStatus::Pending)
            .map(Self::namespace_request_order_key)
            .collect();
    }

    pub(crate) fn namespace_prepare_object_owner_other_than(
        &self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        operation_id: &str,
    ) -> Option<&str> {
        self.namespace_prepare_object_owners
            .get(file_system_id)
            .and_then(|objects| objects.get(object_id))
            .and_then(|owners| {
                owners
                    .iter()
                    .map(String::as_str)
                    .find(|owner| *owner != operation_id)
            })
    }

    pub(crate) fn namespace_prepare_normalized_path_owner_other_than(
        &self,
        file_system_id: &DfsFileSystemId,
        normalized_path: &str,
        operation_id: &str,
    ) -> Option<&str> {
        self.namespace_prepare_path_owners
            .get(file_system_id)
            .and_then(|paths| paths.get(normalized_path))
            .and_then(|owners| {
                owners
                    .iter()
                    .map(String::as_str)
                    .find(|owner| *owner != operation_id)
            })
    }

    #[cfg(test)]
    pub(crate) fn namespace_prepare_footprint_indexes_match_history(&self) -> bool {
        let (object_owners, path_owners) =
            Self::namespace_prepare_footprint_indexes_from_history(&self.namespace_prepares);
        self.namespace_prepare_object_owners == object_owners
            && self.namespace_prepare_path_owners == path_owners
    }

    pub(crate) fn apply(&mut self, op: DfsMetadataTableOp) {
        match op {
            DfsMetadataTableOp::PutFileSystem { file_system } => {
                self.file_systems
                    .insert(file_system.file_system_id.clone(), file_system);
            }
            DfsMetadataTableOp::PutObject { object } => {
                self.objects.insert(object.object_id.clone(), object);
            }
            DfsMetadataTableOp::RemoveObject { object_id } => {
                self.objects.remove(&object_id);
            }
            DfsMetadataTableOp::PutFileSystemObject { path_record } => {
                self.file_system_objects.insert(
                    (
                        path_record.file_system_id.clone(),
                        path_record.object_id.clone(),
                    ),
                    path_record,
                );
            }
            DfsMetadataTableOp::RemoveFileSystemObject {
                file_system_id,
                object_id,
            } => {
                self.file_system_objects
                    .remove(&(file_system_id, object_id));
            }
            DfsMetadataTableOp::PutPath { path_record } => {
                self.paths.insert(
                    (path_record.file_system_id.clone(), path_record.path.clone()),
                    path_record,
                );
            }
            DfsMetadataTableOp::RemovePath {
                file_system_id,
                path,
            } => {
                self.paths.remove(&(file_system_id, path));
            }
            DfsMetadataTableOp::PutInode { inode } => {
                self.inodes.insert(inode.inode_id.clone(), inode);
            }
            DfsMetadataTableOp::RemoveInode { inode_id } => {
                self.inodes.remove(&inode_id);
            }
            DfsMetadataTableOp::PutDirFragment { dirfrag } => {
                self.dirfrags.insert(
                    (dirfrag.dir_inode_id.clone(), dirfrag.dirfrag_id.clone()),
                    dirfrag,
                );
            }
            DfsMetadataTableOp::PutDentry { path_record } => {
                self.dentries.insert(dentry_key(&path_record), path_record);
            }
            DfsMetadataTableOp::RemoveDentry {
                parent_inode_id,
                dirfrag_id,
                name_hash,
                name,
            } => {
                self.dentries
                    .remove(&(parent_inode_id, dirfrag_id, name_hash, name));
            }
            DfsMetadataTableOp::PutHardlink {
                target_object_id,
                hardlink_object_id,
                path_record,
            } => {
                self.put_hardlink_index(target_object_id, hardlink_object_id, path_record);
            }
            DfsMetadataTableOp::RemoveHardlink {
                target_object_id,
                hardlink_object_id,
            } => {
                self.remove_hardlink_index(&target_object_id, &hardlink_object_id);
            }
            DfsMetadataTableOp::PutCompactedTombstone { object } => {
                self.put_compacted_tombstone(object);
            }
            DfsMetadataTableOp::RemoveCompactedTombstonePath {
                file_system_id,
                path,
            } => {
                self.remove_compacted_tombstone_path(&file_system_id, &path);
            }
            DfsMetadataTableOp::PutVersion { version } => {
                self.versions.insert(
                    (version.object_id.clone(), version.version.clone()),
                    version,
                );
            }
            DfsMetadataTableOp::RemoveVersion { object_id, version } => {
                self.versions.remove(&(object_id, version));
            }
            DfsMetadataTableOp::PutVersionPin { pin } => {
                self.version_pins.insert(
                    (
                        pin.object_id.clone(),
                        pin.version.clone(),
                        pin.pin_id.clone(),
                    ),
                    pin,
                );
            }
            DfsMetadataTableOp::RemoveVersionPin {
                object_id,
                version,
                pin_id,
            } => {
                self.version_pins.remove(&(object_id, version, pin_id));
            }
            DfsMetadataTableOp::ReconcileCrossShardPinTransfer { versions, transfer } => {
                for version in versions {
                    self.versions.insert(
                        (version.object_id.clone(), version.version.clone()),
                        version,
                    );
                }
                let key = (transfer.object_id.clone(), transfer.transfer_id.clone());
                if let Some(previous) = self.cross_shard_pin_transfers.get(&key) {
                    for pin in &previous.copied_pins {
                        if transfer.copied_pins.iter().any(|current| current == pin) {
                            continue;
                        }
                        let pin_key = (
                            pin.object_id.clone(),
                            pin.version.clone(),
                            pin.pin_id.clone(),
                        );
                        if self.version_pins.get(&pin_key) == Some(pin) {
                            self.version_pins.remove(&pin_key);
                        }
                    }
                }
                for pin in &transfer.source_pins {
                    self.version_pins.insert(
                        (
                            pin.object_id.clone(),
                            pin.version.clone(),
                            pin.pin_id.clone(),
                        ),
                        pin.clone(),
                    );
                }
                self.cross_shard_pin_transfers.insert(key, transfer);
            }
            DfsMetadataTableOp::RemoveCrossShardPinTransfer {
                object_id,
                transfer_id,
            } => {
                self.cross_shard_pin_transfers
                    .remove(&(object_id, transfer_id));
            }
            DfsMetadataTableOp::FinalizeCrossShardSourceEviction {
                tombstone,
                removed_pins,
                removed_pin_transfers,
            } => {
                let previous = self.objects.get(&tombstone.object_id).cloned();
                for pin in removed_pins {
                    let key = (
                        pin.object_id.clone(),
                        pin.version.clone(),
                        pin.pin_id.clone(),
                    );
                    if self.version_pins.get(&key) == Some(&pin) {
                        self.version_pins.remove(&key);
                    }
                }
                for transfer in removed_pin_transfers {
                    let key = (transfer.object_id.clone(), transfer.transfer_id.clone());
                    if self.cross_shard_pin_transfers.get(&key) == Some(&transfer) {
                        self.cross_shard_pin_transfers.remove(&key);
                    }
                }
                for op in object_index_table_ops_with_previous(self, &tombstone, previous.as_ref())
                {
                    self.apply(op);
                }
            }
            DfsMetadataTableOp::PutReceipt { receipt } => {
                self.receipts.insert(
                    (
                        receipt.receipt.object_id.clone(),
                        receipt.receipt.committed_version.clone(),
                        receipt.replica_node_id.clone(),
                    ),
                    receipt,
                );
            }
            DfsMetadataTableOp::RemoveReceipt {
                object_id,
                version,
                replica_node_id,
            } => {
                self.receipts.remove(&(
                    object_id.clone(),
                    version.clone(),
                    replica_node_id.clone(),
                ));
            }
            DfsMetadataTableOp::PutIntent { intent } => {
                self.intent_receipts
                    .remove(&(intent.object_id.clone(), intent.intent_id.clone()));
                self.put_intent_indexed(intent);
            }
            DfsMetadataTableOp::PutIntentReceipt { receipt } => {
                self.remove_intent_indexed(&receipt.object_id, &receipt.intent_id);
                self.intent_receipts.insert(
                    (receipt.object_id.clone(), receipt.intent_id.clone()),
                    receipt,
                );
            }
            DfsMetadataTableOp::RemoveIntentReceipt {
                object_id,
                intent_id,
                retention,
            } => {
                self.intent_receipts.remove(&(object_id, intent_id));
                self.intent_receipt_retention.merge_conservative(retention);
            }
            DfsMetadataTableOp::MergeIntentReceiptRetention { retention } => {
                self.intent_receipt_retention.merge_conservative(retention);
            }
            DfsMetadataTableOp::PutConflict { conflict } => {
                self.conflicts.insert(
                    (conflict.object_id.clone(), conflict.conflict_id.clone()),
                    conflict,
                );
            }
            DfsMetadataTableOp::PutAdvisoryLockState { state } => {
                self.advisory_lock_states
                    .insert(state.object_id.clone(), state);
            }
            DfsMetadataTableOp::RemoveConflict {
                object_id,
                conflict_id,
            } => {
                self.conflicts.remove(&(object_id, conflict_id));
            }
            DfsMetadataTableOp::PutVisibleCommit {
                object_id,
                version,
                commit,
            } => {
                self.visible_commits.insert((object_id, version), commit);
            }
            DfsMetadataTableOp::RemoveVisibleCommit { object_id, version } => {
                self.visible_commits.remove(&(object_id, version));
            }
            DfsMetadataTableOp::PutNamespaceTransaction { transaction } => {
                self.namespace_transaction_receipts
                    .remove(&transaction.transaction_id);
                self.namespace_transactions
                    .insert(transaction.transaction_id.clone(), transaction);
            }
            DfsMetadataTableOp::PutNamespaceTransactionReceipt { receipt } => {
                self.namespace_transactions.remove(&receipt.transaction_id);
                self.namespace_transaction_receipts
                    .insert(receipt.transaction_id.clone(), receipt);
            }
            DfsMetadataTableOp::RemoveNamespaceTransactionReceipt {
                transaction_id,
                retention,
            } => {
                self.namespace_transaction_receipts.remove(&transaction_id);
                self.namespace_receipt_retention
                    .merge_conservative(retention);
            }
            DfsMetadataTableOp::PutNamespacePrepare { prepare } => {
                self.namespace_prepare_receipts
                    .remove(&prepare.operation_id);
                self.put_namespace_prepare_indexed(prepare);
            }
            DfsMetadataTableOp::PutNamespacePrepareJournalReference {
                journal_sequence,
                operation_id,
                ..
            } => {
                panic!(
                    "namespace prepare {operation_id} table reference at journal sequence {journal_sequence} must be resolved before apply"
                );
            }
            DfsMetadataTableOp::CommitNamespacePrepare {
                operation_id,
                operation_digest,
                owner_epoch,
            } => {
                self.materialize_committed_namespace_prepare(
                    &operation_id,
                    &operation_digest,
                    owner_epoch,
                )
                .expect("compact namespace commit must reference its exact durable prepare");
            }
            DfsMetadataTableOp::PutNamespacePrepareReceipt { receipt } => {
                if let Some(previous) = self.namespace_prepares.remove(&receipt.operation_id) {
                    self.unindex_namespace_prepare_footprint(&previous);
                    self.unindex_namespace_prepare_request(&previous);
                }
                self.namespace_prepare_receipts
                    .insert(receipt.operation_id.clone(), receipt);
            }
            DfsMetadataTableOp::RemoveNamespacePrepareReceipt {
                operation_id,
                retention,
            } => {
                self.namespace_prepare_receipts.remove(&operation_id);
                self.namespace_receipt_retention
                    .merge_conservative(retention);
            }
            DfsMetadataTableOp::PutNamespaceJob { job } => {
                self.namespace_job_receipts.remove(&job.job_id);
                self.namespace_jobs.insert(job.job_id.clone(), job);
            }
            DfsMetadataTableOp::PutNamespaceJobReceipt { receipt } => {
                self.namespace_jobs.remove(&receipt.job_id);
                self.namespace_job_receipts
                    .insert(receipt.job_id.clone(), receipt);
            }
            DfsMetadataTableOp::RemoveNamespaceJobReceipt { job_id, retention } => {
                self.namespace_job_receipts.remove(&job_id);
                self.namespace_receipt_retention
                    .merge_conservative(retention);
            }
            DfsMetadataTableOp::PutNamespaceRequest { request } => {
                self.namespace_request_receipts.remove(&(
                    request.requester_node_id.clone(),
                    request.request_id.clone(),
                ));
                self.put_namespace_request_indexed(request);
            }
            DfsMetadataTableOp::PutNamespaceRequestReceipt { receipt } => {
                let key = (
                    receipt.requester_node_id.clone(),
                    receipt.request_id.clone(),
                );
                if let Some(previous) = self.namespace_requests.remove(&key)
                    && previous.status == DfsNamespaceRequestStatus::Pending
                {
                    self.pending_namespace_request_order
                        .remove(&Self::namespace_request_order_key(&previous));
                }
                self.namespace_request_receipts.insert(key, receipt);
            }
            DfsMetadataTableOp::RemoveNamespaceRequestReceipt {
                requester_node_id,
                request_id,
                retention,
            } => {
                self.namespace_request_receipts
                    .remove(&(requester_node_id, request_id));
                self.namespace_receipt_retention
                    .merge_conservative(retention);
            }
        }
    }

    pub(crate) fn apply_with_rollback(
        &mut self,
        op: DfsMetadataTableOp,
        rollback: &mut DfsMetadataTableRollback,
    ) {
        rollback.record(self, &op);
        self.apply(op);
    }

    fn put_compacted_tombstone(&mut self, object: DfsObjectMetadata) {
        let object = normalized_object_metadata(&object);
        let path_key = (object.file_system_id.clone(), normalize_path(&object.path));
        if let Some(previous_object_id) = self
            .compacted_tombstone_paths
            .insert(path_key, object.object_id.clone())
            && previous_object_id != object.object_id
        {
            self.compacted_tombstones.remove(&previous_object_id);
        }
        self.compacted_tombstones
            .insert(object.object_id.clone(), object);
    }

    fn remove_compacted_tombstone_path(&mut self, file_system_id: &DfsFileSystemId, path: &str) {
        let path_key = (file_system_id.clone(), normalize_path(path));
        if let Some(object_id) = self.compacted_tombstone_paths.remove(&path_key) {
            self.compacted_tombstones.remove(&object_id);
        }
    }

    pub(crate) fn compacted_tombstone_by_path(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Option<DfsObjectMetadata> {
        let path_key = (file_system_id.clone(), normalize_path(path));
        self.compacted_tombstone_paths
            .get(&path_key)
            .and_then(|object_id| self.compacted_tombstones.get(object_id))
            .filter(|object| {
                object.kind == DfsObjectKind::Tombstone
                    && object.file_system_id == *file_system_id
                    && normalize_path(&object.path) == path_key.1
            })
            .cloned()
    }

    pub(crate) fn compacted_tombstone_matches(&self, object: &DfsObjectMetadata) -> bool {
        if object.kind != DfsObjectKind::Tombstone {
            return false;
        }
        self.compacted_tombstones
            .get(&object.object_id)
            .is_some_and(|stored| {
                normalized_object_metadata(stored) == normalized_object_metadata(object)
            })
    }

    /// Return true when a compacted tombstone proves that replaying `object`
    /// during namespace-transaction recovery would restore stale state.
    ///
    /// An exact compacted tombstone is already fully materialized. A newer
    /// tombstone may also fence an older transaction, but only when the object
    /// and its namespace identity match; path reuse by another object must not
    /// suppress recovery.
    pub(crate) fn compacted_tombstone_fences_namespace_recovery(
        &self,
        object: &DfsObjectMetadata,
    ) -> bool {
        if self.compacted_tombstone_matches(object) {
            return true;
        }
        self.compacted_tombstones
            .get(&object.object_id)
            .is_some_and(|stored| {
                stored.kind == DfsObjectKind::Tombstone
                    && stored.file_system_id == object.file_system_id
                    && normalize_path(&stored.path) == normalize_path(&object.path)
                    && (stored.changed_at_unix_nanos, &stored.version)
                        > (object.changed_at_unix_nanos, &object.version)
            })
    }

    fn put_hardlink_index(
        &mut self,
        target_object_id: DfsObjectId,
        hardlink_object_id: DfsObjectId,
        path_record: DfsPathRecord,
    ) {
        self.remove_hardlink_index(&target_object_id, &hardlink_object_id);
        self.hardlink_paths.insert(
            (
                target_object_id.clone(),
                path_record.path.clone(),
                hardlink_object_id.clone(),
            ),
            path_record.clone(),
        );
        self.hardlinks
            .insert((target_object_id, hardlink_object_id), path_record);
    }

    fn remove_hardlink_index(
        &mut self,
        target_object_id: &DfsObjectId,
        hardlink_object_id: &DfsObjectId,
    ) {
        if let Some(path_record) = self
            .hardlinks
            .remove(&(target_object_id.clone(), hardlink_object_id.clone()))
        {
            self.hardlink_paths.remove(&(
                target_object_id.clone(),
                path_record.path,
                hardlink_object_id.clone(),
            ));
        }
    }

    fn rebuild_hardlink_path_index(&mut self) {
        self.hardlink_paths = self
            .hardlinks
            .iter()
            .map(|((target_object_id, hardlink_object_id), path_record)| {
                (
                    (
                        target_object_id.clone(),
                        path_record.path.clone(),
                        hardlink_object_id.clone(),
                    ),
                    path_record.clone(),
                )
            })
            .collect();
    }
}

#[derive(Debug, Default)]
pub(crate) struct DfsMetadataTableRollback {
    file_systems: BTreeMap<DfsFileSystemId, Option<DfsFileSystem>>,
    objects: BTreeMap<DfsObjectId, Option<DfsObjectMetadata>>,
    file_system_objects: BTreeMap<(DfsFileSystemId, DfsObjectId), Option<DfsPathRecord>>,
    paths: BTreeMap<(DfsFileSystemId, String), Option<DfsPathRecord>>,
    inodes: BTreeMap<DfsInodeId, Option<DfsInodeRecord>>,
    dirfrags: BTreeMap<(DfsInodeId, DfsDirFragmentId), Option<DfsDirFragmentRecord>>,
    dentries: BTreeMap<(DfsInodeId, DfsDirFragmentId, u64, String), Option<DfsPathRecord>>,
    hardlinks: BTreeMap<(DfsObjectId, DfsObjectId), Option<DfsPathRecord>>,
    compacted_tombstones: BTreeMap<DfsObjectId, Option<DfsObjectMetadata>>,
    compacted_tombstone_paths: BTreeMap<(DfsFileSystemId, String), Option<DfsObjectId>>,
    versions: BTreeMap<(DfsObjectId, DfsVersion), Option<DfsObjectVersionRecord>>,
    version_pins: BTreeMap<(DfsObjectId, DfsVersion, String), Option<DfsMetadataVersionPinRecord>>,
    cross_shard_pin_transfers:
        BTreeMap<(DfsObjectId, String), Option<DfsCrossShardPinTransferRecord>>,
    receipts: BTreeMap<(DfsObjectId, DfsVersion, NodeId), Option<DfsReplicaReceiptRecord>>,
    intents: BTreeMap<(DfsObjectId, String), Option<DfsWriteIntentRecord>>,
    intent_receipts: BTreeMap<(DfsObjectId, String), Option<DfsWriteIntentReceipt>>,
    intent_receipt_retention: Option<DfsWriteIntentReceiptRetentionState>,
    conflicts: BTreeMap<(DfsObjectId, String), Option<DfsConflictRecord>>,
    advisory_lock_states: BTreeMap<DfsObjectId, Option<DfsAdvisoryLockStateRecord>>,
    visible_commits: BTreeMap<(DfsObjectId, DfsVersion), Option<DfsVisibleVersionCommitRecord>>,
    namespace_transactions: BTreeMap<String, Option<DfsNamespaceTransaction>>,
    namespace_transaction_receipts: BTreeMap<String, Option<DfsNamespaceTransactionReceipt>>,
    namespace_prepares: BTreeMap<String, Option<DfsNamespacePrepareRecord>>,
    namespace_prepare_receipts: BTreeMap<String, Option<DfsNamespacePrepareReceipt>>,
    namespace_jobs: BTreeMap<String, Option<DfsNamespaceJobRecord>>,
    namespace_job_receipts: BTreeMap<String, Option<DfsNamespaceJobReceipt>>,
    namespace_requests: BTreeMap<(NodeId, String), Option<DfsNamespaceRequestRecord>>,
    namespace_request_receipts: BTreeMap<(NodeId, String), Option<DfsNamespaceRequestReceipt>>,
    namespace_receipt_retention: Option<DfsNamespaceReceiptRetentionState>,
}

impl DfsMetadataTableRollback {
    fn record(&mut self, tables: &DfsMetadataTables, op: &DfsMetadataTableOp) {
        match op {
            DfsMetadataTableOp::PutFileSystem { file_system } => record_rollback_value(
                &mut self.file_systems,
                &tables.file_systems,
                file_system.file_system_id.clone(),
            ),
            DfsMetadataTableOp::PutObject { object } => {
                record_rollback_value(&mut self.objects, &tables.objects, object.object_id.clone())
            }
            DfsMetadataTableOp::RemoveObject { object_id } => {
                record_rollback_value(&mut self.objects, &tables.objects, object_id.clone())
            }
            DfsMetadataTableOp::PutFileSystemObject { path_record } => record_rollback_value(
                &mut self.file_system_objects,
                &tables.file_system_objects,
                (
                    path_record.file_system_id.clone(),
                    path_record.object_id.clone(),
                ),
            ),
            DfsMetadataTableOp::RemoveFileSystemObject {
                file_system_id,
                object_id,
            } => record_rollback_value(
                &mut self.file_system_objects,
                &tables.file_system_objects,
                (file_system_id.clone(), object_id.clone()),
            ),
            DfsMetadataTableOp::PutPath { path_record } => record_rollback_value(
                &mut self.paths,
                &tables.paths,
                (path_record.file_system_id.clone(), path_record.path.clone()),
            ),
            DfsMetadataTableOp::RemovePath {
                file_system_id,
                path,
            } => record_rollback_value(
                &mut self.paths,
                &tables.paths,
                (file_system_id.clone(), path.clone()),
            ),
            DfsMetadataTableOp::PutInode { inode } => {
                record_rollback_value(&mut self.inodes, &tables.inodes, inode.inode_id.clone())
            }
            DfsMetadataTableOp::RemoveInode { inode_id } => {
                record_rollback_value(&mut self.inodes, &tables.inodes, inode_id.clone())
            }
            DfsMetadataTableOp::PutDirFragment { dirfrag } => record_rollback_value(
                &mut self.dirfrags,
                &tables.dirfrags,
                (dirfrag.dir_inode_id.clone(), dirfrag.dirfrag_id.clone()),
            ),
            DfsMetadataTableOp::PutDentry { path_record } => record_rollback_value(
                &mut self.dentries,
                &tables.dentries,
                dentry_key(path_record),
            ),
            DfsMetadataTableOp::RemoveDentry {
                parent_inode_id,
                dirfrag_id,
                name_hash,
                name,
            } => record_rollback_value(
                &mut self.dentries,
                &tables.dentries,
                (
                    parent_inode_id.clone(),
                    dirfrag_id.clone(),
                    *name_hash,
                    name.clone(),
                ),
            ),
            DfsMetadataTableOp::PutHardlink {
                target_object_id,
                hardlink_object_id,
                ..
            } => record_rollback_value(
                &mut self.hardlinks,
                &tables.hardlinks,
                (target_object_id.clone(), hardlink_object_id.clone()),
            ),
            DfsMetadataTableOp::RemoveHardlink {
                target_object_id,
                hardlink_object_id,
            } => record_rollback_value(
                &mut self.hardlinks,
                &tables.hardlinks,
                (target_object_id.clone(), hardlink_object_id.clone()),
            ),
            DfsMetadataTableOp::PutCompactedTombstone { object } => {
                record_compacted_tombstone_rollback(self, tables, object);
            }
            DfsMetadataTableOp::RemoveCompactedTombstonePath {
                file_system_id,
                path,
            } => {
                record_compacted_tombstone_path_rollback(self, tables, file_system_id, path);
            }
            DfsMetadataTableOp::PutVersion { version } => record_rollback_value(
                &mut self.versions,
                &tables.versions,
                (version.object_id.clone(), version.version.clone()),
            ),
            DfsMetadataTableOp::RemoveVersion { object_id, version } => record_rollback_value(
                &mut self.versions,
                &tables.versions,
                (object_id.clone(), version.clone()),
            ),
            DfsMetadataTableOp::PutVersionPin { pin } => record_rollback_value(
                &mut self.version_pins,
                &tables.version_pins,
                (
                    pin.object_id.clone(),
                    pin.version.clone(),
                    pin.pin_id.clone(),
                ),
            ),
            DfsMetadataTableOp::RemoveVersionPin {
                object_id,
                version,
                pin_id,
            } => record_rollback_value(
                &mut self.version_pins,
                &tables.version_pins,
                (object_id.clone(), version.clone(), pin_id.clone()),
            ),
            DfsMetadataTableOp::ReconcileCrossShardPinTransfer { versions, transfer } => {
                for version in versions {
                    record_rollback_value(
                        &mut self.versions,
                        &tables.versions,
                        (version.object_id.clone(), version.version.clone()),
                    );
                }
                record_rollback_value(
                    &mut self.cross_shard_pin_transfers,
                    &tables.cross_shard_pin_transfers,
                    (transfer.object_id.clone(), transfer.transfer_id.clone()),
                );
                if let Some(previous) = tables
                    .cross_shard_pin_transfers
                    .get(&(transfer.object_id.clone(), transfer.transfer_id.clone()))
                {
                    for pin in &previous.copied_pins {
                        record_rollback_value(
                            &mut self.version_pins,
                            &tables.version_pins,
                            (
                                pin.object_id.clone(),
                                pin.version.clone(),
                                pin.pin_id.clone(),
                            ),
                        );
                    }
                }
                for pin in &transfer.source_pins {
                    record_rollback_value(
                        &mut self.version_pins,
                        &tables.version_pins,
                        (
                            pin.object_id.clone(),
                            pin.version.clone(),
                            pin.pin_id.clone(),
                        ),
                    );
                }
            }
            DfsMetadataTableOp::RemoveCrossShardPinTransfer {
                object_id,
                transfer_id,
            } => record_rollback_value(
                &mut self.cross_shard_pin_transfers,
                &tables.cross_shard_pin_transfers,
                (object_id.clone(), transfer_id.clone()),
            ),
            DfsMetadataTableOp::FinalizeCrossShardSourceEviction {
                tombstone,
                removed_pins,
                removed_pin_transfers,
            } => {
                for pin in removed_pins {
                    record_rollback_value(
                        &mut self.version_pins,
                        &tables.version_pins,
                        (
                            pin.object_id.clone(),
                            pin.version.clone(),
                            pin.pin_id.clone(),
                        ),
                    );
                }
                for transfer in removed_pin_transfers {
                    record_rollback_value(
                        &mut self.cross_shard_pin_transfers,
                        &tables.cross_shard_pin_transfers,
                        (transfer.object_id.clone(), transfer.transfer_id.clone()),
                    );
                }
                let previous = tables.objects.get(&tombstone.object_id);
                for nested in object_index_table_ops_with_previous(tables, tombstone, previous) {
                    self.record(tables, &nested);
                }
            }
            DfsMetadataTableOp::PutReceipt { receipt } => record_rollback_value(
                &mut self.receipts,
                &tables.receipts,
                (
                    receipt.receipt.object_id.clone(),
                    receipt.receipt.committed_version.clone(),
                    receipt.replica_node_id.clone(),
                ),
            ),
            DfsMetadataTableOp::RemoveReceipt {
                object_id,
                version,
                replica_node_id,
            } => record_rollback_value(
                &mut self.receipts,
                &tables.receipts,
                (object_id.clone(), version.clone(), replica_node_id.clone()),
            ),
            DfsMetadataTableOp::PutIntent { intent } => {
                let key = (intent.object_id.clone(), intent.intent_id.clone());
                record_rollback_value(&mut self.intents, &tables.intents, key.clone());
                record_rollback_value(&mut self.intent_receipts, &tables.intent_receipts, key);
            }
            DfsMetadataTableOp::PutIntentReceipt { receipt } => {
                let key = (receipt.object_id.clone(), receipt.intent_id.clone());
                record_rollback_value(&mut self.intents, &tables.intents, key.clone());
                record_rollback_value(&mut self.intent_receipts, &tables.intent_receipts, key);
            }
            DfsMetadataTableOp::RemoveIntentReceipt {
                object_id,
                intent_id,
                ..
            } => {
                record_rollback_value(
                    &mut self.intent_receipts,
                    &tables.intent_receipts,
                    (object_id.clone(), intent_id.clone()),
                );
                if self.intent_receipt_retention.is_none() {
                    self.intent_receipt_retention = Some(tables.intent_receipt_retention.clone());
                }
            }
            DfsMetadataTableOp::MergeIntentReceiptRetention { .. } => {
                if self.intent_receipt_retention.is_none() {
                    self.intent_receipt_retention = Some(tables.intent_receipt_retention.clone());
                }
            }
            DfsMetadataTableOp::PutConflict { conflict } => record_rollback_value(
                &mut self.conflicts,
                &tables.conflicts,
                (conflict.object_id.clone(), conflict.conflict_id.clone()),
            ),
            DfsMetadataTableOp::PutAdvisoryLockState { state } => record_rollback_value(
                &mut self.advisory_lock_states,
                &tables.advisory_lock_states,
                state.object_id.clone(),
            ),
            DfsMetadataTableOp::RemoveConflict {
                object_id,
                conflict_id,
            } => record_rollback_value(
                &mut self.conflicts,
                &tables.conflicts,
                (object_id.clone(), conflict_id.clone()),
            ),
            DfsMetadataTableOp::PutVisibleCommit {
                object_id, version, ..
            } => record_rollback_value(
                &mut self.visible_commits,
                &tables.visible_commits,
                (object_id.clone(), version.clone()),
            ),
            DfsMetadataTableOp::RemoveVisibleCommit { object_id, version } => {
                record_rollback_value(
                    &mut self.visible_commits,
                    &tables.visible_commits,
                    (object_id.clone(), version.clone()),
                )
            }
            DfsMetadataTableOp::PutNamespaceTransaction { transaction } => {
                record_rollback_value(
                    &mut self.namespace_transactions,
                    &tables.namespace_transactions,
                    transaction.transaction_id.clone(),
                );
                record_rollback_value(
                    &mut self.namespace_transaction_receipts,
                    &tables.namespace_transaction_receipts,
                    transaction.transaction_id.clone(),
                );
            }
            DfsMetadataTableOp::PutNamespaceTransactionReceipt { receipt } => {
                record_rollback_value(
                    &mut self.namespace_transactions,
                    &tables.namespace_transactions,
                    receipt.transaction_id.clone(),
                );
                record_rollback_value(
                    &mut self.namespace_transaction_receipts,
                    &tables.namespace_transaction_receipts,
                    receipt.transaction_id.clone(),
                );
            }
            DfsMetadataTableOp::RemoveNamespaceTransactionReceipt { transaction_id, .. } => {
                record_rollback_value(
                    &mut self.namespace_transaction_receipts,
                    &tables.namespace_transaction_receipts,
                    transaction_id.clone(),
                );
                if self.namespace_receipt_retention.is_none() {
                    self.namespace_receipt_retention =
                        Some(tables.namespace_receipt_retention.clone());
                }
            }
            DfsMetadataTableOp::PutNamespacePrepare { prepare } => {
                #[cfg(test)]
                NAMESPACE_PREPARE_ROLLBACK_MATERIALIZATIONS.set(
                    NAMESPACE_PREPARE_ROLLBACK_MATERIALIZATIONS
                        .get()
                        .saturating_add(1),
                );
                record_rollback_value(
                    &mut self.namespace_prepares,
                    &tables.namespace_prepares,
                    prepare.operation_id.clone(),
                );
                record_rollback_value(
                    &mut self.namespace_prepare_receipts,
                    &tables.namespace_prepare_receipts,
                    prepare.operation_id.clone(),
                );
            }
            DfsMetadataTableOp::PutNamespacePrepareJournalReference {
                journal_sequence,
                operation_id,
                ..
            } => {
                panic!(
                    "namespace prepare {operation_id} table reference at journal sequence {journal_sequence} must be resolved before rollback planning"
                );
            }
            DfsMetadataTableOp::CommitNamespacePrepare { operation_id, .. } => {
                panic!(
                    "compact namespace commit {operation_id} must be applied after its durable table marker, never preapplied with rollback"
                );
            }
            DfsMetadataTableOp::PutNamespacePrepareReceipt { receipt } => {
                record_rollback_value(
                    &mut self.namespace_prepares,
                    &tables.namespace_prepares,
                    receipt.operation_id.clone(),
                );
                record_rollback_value(
                    &mut self.namespace_prepare_receipts,
                    &tables.namespace_prepare_receipts,
                    receipt.operation_id.clone(),
                );
            }
            DfsMetadataTableOp::RemoveNamespacePrepareReceipt { operation_id, .. } => {
                record_rollback_value(
                    &mut self.namespace_prepare_receipts,
                    &tables.namespace_prepare_receipts,
                    operation_id.clone(),
                );
                if self.namespace_receipt_retention.is_none() {
                    self.namespace_receipt_retention =
                        Some(tables.namespace_receipt_retention.clone());
                }
            }
            DfsMetadataTableOp::PutNamespaceJob { job } => {
                record_rollback_value(
                    &mut self.namespace_jobs,
                    &tables.namespace_jobs,
                    job.job_id.clone(),
                );
                record_rollback_value(
                    &mut self.namespace_job_receipts,
                    &tables.namespace_job_receipts,
                    job.job_id.clone(),
                );
            }
            DfsMetadataTableOp::PutNamespaceJobReceipt { receipt } => {
                record_rollback_value(
                    &mut self.namespace_jobs,
                    &tables.namespace_jobs,
                    receipt.job_id.clone(),
                );
                record_rollback_value(
                    &mut self.namespace_job_receipts,
                    &tables.namespace_job_receipts,
                    receipt.job_id.clone(),
                );
            }
            DfsMetadataTableOp::RemoveNamespaceJobReceipt { job_id, .. } => {
                record_rollback_value(
                    &mut self.namespace_job_receipts,
                    &tables.namespace_job_receipts,
                    job_id.clone(),
                );
                if self.namespace_receipt_retention.is_none() {
                    self.namespace_receipt_retention =
                        Some(tables.namespace_receipt_retention.clone());
                }
            }
            DfsMetadataTableOp::PutNamespaceRequest { request } => {
                let key = (
                    request.requester_node_id.clone(),
                    request.request_id.clone(),
                );
                record_rollback_value(
                    &mut self.namespace_requests,
                    &tables.namespace_requests,
                    key.clone(),
                );
                record_rollback_value(
                    &mut self.namespace_request_receipts,
                    &tables.namespace_request_receipts,
                    key,
                );
            }
            DfsMetadataTableOp::PutNamespaceRequestReceipt { receipt } => {
                let key = (
                    receipt.requester_node_id.clone(),
                    receipt.request_id.clone(),
                );
                record_rollback_value(
                    &mut self.namespace_requests,
                    &tables.namespace_requests,
                    key.clone(),
                );
                record_rollback_value(
                    &mut self.namespace_request_receipts,
                    &tables.namespace_request_receipts,
                    key,
                );
            }
            DfsMetadataTableOp::RemoveNamespaceRequestReceipt {
                requester_node_id,
                request_id,
                ..
            } => {
                record_rollback_value(
                    &mut self.namespace_request_receipts,
                    &tables.namespace_request_receipts,
                    (requester_node_id.clone(), request_id.clone()),
                );
                if self.namespace_receipt_retention.is_none() {
                    self.namespace_receipt_retention =
                        Some(tables.namespace_receipt_retention.clone());
                }
            }
        }
    }

    pub(crate) fn rollback(self, tables: &mut DfsMetadataTables) {
        rollback_map(&mut tables.file_systems, self.file_systems);
        rollback_map(&mut tables.objects, self.objects);
        rollback_map(&mut tables.file_system_objects, self.file_system_objects);
        rollback_map(&mut tables.paths, self.paths);
        rollback_map(&mut tables.inodes, self.inodes);
        rollback_map(&mut tables.dirfrags, self.dirfrags);
        rollback_map(&mut tables.dentries, self.dentries);
        rollback_map(&mut tables.hardlinks, self.hardlinks);
        tables.rebuild_hardlink_path_index();
        rollback_map(&mut tables.compacted_tombstones, self.compacted_tombstones);
        rollback_map(
            &mut tables.compacted_tombstone_paths,
            self.compacted_tombstone_paths,
        );
        rollback_map(&mut tables.versions, self.versions);
        rollback_map(&mut tables.version_pins, self.version_pins);
        rollback_map(
            &mut tables.cross_shard_pin_transfers,
            self.cross_shard_pin_transfers,
        );
        rollback_map(&mut tables.receipts, self.receipts);
        rollback_map(&mut tables.intents, self.intents);
        rollback_map(&mut tables.intent_receipts, self.intent_receipts);
        if let Some(retention) = self.intent_receipt_retention {
            tables.intent_receipt_retention = retention;
        }
        tables.rebuild_intent_indexes();
        rollback_map(&mut tables.conflicts, self.conflicts);
        rollback_map(&mut tables.advisory_lock_states, self.advisory_lock_states);
        rollback_map(&mut tables.visible_commits, self.visible_commits);
        rollback_map(
            &mut tables.namespace_transactions,
            self.namespace_transactions,
        );
        rollback_map(
            &mut tables.namespace_transaction_receipts,
            self.namespace_transaction_receipts,
        );
        rollback_map(&mut tables.namespace_prepares, self.namespace_prepares);
        rollback_map(
            &mut tables.namespace_prepare_receipts,
            self.namespace_prepare_receipts,
        );
        tables.rebuild_namespace_prepare_footprint_indexes();
        tables.rebuild_namespace_prepare_request_index();
        rollback_map(&mut tables.namespace_jobs, self.namespace_jobs);
        rollback_map(
            &mut tables.namespace_job_receipts,
            self.namespace_job_receipts,
        );
        rollback_map(&mut tables.namespace_requests, self.namespace_requests);
        rollback_map(
            &mut tables.namespace_request_receipts,
            self.namespace_request_receipts,
        );
        if let Some(retention) = self.namespace_receipt_retention {
            tables.namespace_receipt_retention = retention;
        }
        tables.rebuild_pending_namespace_request_index();
    }
}

fn record_rollback_value<K, V>(
    rollback: &mut BTreeMap<K, Option<V>>,
    table: &BTreeMap<K, V>,
    key: K,
) where
    K: Clone + Ord,
    V: Clone,
{
    if !rollback.contains_key(&key) {
        rollback.insert(key.clone(), table.get(&key).cloned());
    }
}

fn record_compacted_tombstone_rollback(
    rollback: &mut DfsMetadataTableRollback,
    tables: &DfsMetadataTables,
    object: &DfsObjectMetadata,
) {
    let object_id = object.object_id.clone();
    let path_key = (object.file_system_id.clone(), normalize_path(&object.path));
    record_rollback_value(
        &mut rollback.compacted_tombstones,
        &tables.compacted_tombstones,
        object_id,
    );
    record_rollback_value(
        &mut rollback.compacted_tombstone_paths,
        &tables.compacted_tombstone_paths,
        path_key.clone(),
    );
    if let Some(previous_object_id) = tables.compacted_tombstone_paths.get(&path_key) {
        record_rollback_value(
            &mut rollback.compacted_tombstones,
            &tables.compacted_tombstones,
            previous_object_id.clone(),
        );
    }
}

fn record_compacted_tombstone_path_rollback(
    rollback: &mut DfsMetadataTableRollback,
    tables: &DfsMetadataTables,
    file_system_id: &DfsFileSystemId,
    path: &str,
) {
    let path_key = (file_system_id.clone(), normalize_path(path));
    record_rollback_value(
        &mut rollback.compacted_tombstone_paths,
        &tables.compacted_tombstone_paths,
        path_key.clone(),
    );
    if let Some(object_id) = tables.compacted_tombstone_paths.get(&path_key) {
        record_rollback_value(
            &mut rollback.compacted_tombstones,
            &tables.compacted_tombstones,
            object_id.clone(),
        );
    }
}

fn rollback_map<K, V>(table: &mut BTreeMap<K, V>, rollback: BTreeMap<K, Option<V>>)
where
    K: Ord,
{
    for (key, value) in rollback {
        if let Some(value) = value {
            table.insert(key, value);
        } else {
            table.remove(&key);
        }
    }
}
