//! In-memory materialized metadata tables.
//!
//! Durable table IO lives in `metadata_table_store`; this module owns the
//! table maps, secondary indexes, and rollback bookkeeping used while
//! materializing journal entries.

mod move_destination_reservations;
mod parent_counts;
mod running_jobs;

use std::borrow::Cow;
#[cfg(test)]
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::Hash;
use std::ops::Bound::{Included, Unbounded};
use std::ops::Deref;
use std::sync::Arc;

use sinomo_core::{Id, node::NodeId};
use time::OffsetDateTime;

use crate::advisory_lock::DfsAdvisoryLockStateRecord;
use crate::intent::write_intent_status_is_active;
use crate::metadata_error::{MetadataBackendError, Result};
use crate::metadata_keys::{
    dentry_key, id_lower_bound, intent_key, normalize_path, write_intent_scan_key,
    write_intent_target_key,
};
use crate::metadata_object_index::object_index_table_ops_with_previous;
use crate::metadata_records::{
    DfsCrossShardPinTransferRecord, DfsDirFragmentId, DfsDirFragmentRecord, DfsInodeId,
    DfsInodeRecord, DfsMetadataPrefixProof, DfsMetadataSummary, DfsMetadataTableOp,
    DfsMetadataVersionPinRecord, DfsNamespaceJobReceipt, DfsNamespaceJobRecord,
    DfsNamespaceJobStatus, DfsNamespaceReceiptOrderKey, DfsNamespaceReceiptRetentionState,
    DfsNamespaceRequestCompletion, DfsNamespaceRequestReceipt, DfsNamespaceRequestRecord,
    DfsNamespaceRequestStatus, DfsNamespaceTransactionReceipt, DfsObjectVersionRecord,
    DfsPathRecord, DfsPreparedOperationEviction, DfsPreparedOperationRecord,
    DfsPreparedOperationStatus, DfsReplicaReceiptRecord, DfsReplicaReceiptWithdrawalRecord,
    DfsWriteIntentReceipt, DfsWriteIntentReceiptRetentionState, DfsWriteIntentRecord,
    normalized_object_metadata,
};

mod prepared_operation;
#[cfg(test)]
mod receipt_generation_tests;
mod repair_index;
#[cfg(test)]
mod repair_index_tests;
use crate::{
    DfsFileSystem, DfsFileSystemId, DfsObjectId, DfsObjectKind, DfsObjectMetadata, DfsVersion,
};
#[cfg(test)]
pub(crate) use repair_index::assert_repair_index_matches_rows;
pub(crate) use repair_index::{DfsRepairIndex, DfsRepairNeed, DfsRepairNeedKey};

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
    /// Generic grant candidates, derived incrementally from durable rows.
    /// Snapshot/journal materialization uses the same table operations; this
    /// index is never serialized and carries no module-specific semantics.
    pub(crate) retained_revision_index: DfsRetainedRevisionIndex,
    pub(crate) version_pins:
        BTreeMap<(DfsObjectId, DfsVersion, String), DfsMetadataVersionPinRecord>,
    pub(crate) cross_shard_pin_transfers:
        BTreeMap<(DfsObjectId, String), DfsCrossShardPinTransferRecord>,
    pub(crate) receipts: BTreeMap<(DfsObjectId, DfsVersion, NodeId), DfsReplicaReceiptRecord>,
    /// Withdrawn receipt slots of live versions. A slot is present in at most
    /// one of `receipts` and `receipt_withdrawals`; its generation is the row's
    /// generation, or zero when neither row exists.
    pub(crate) receipt_withdrawals:
        BTreeMap<(DfsObjectId, DfsVersion, NodeId), DfsReplicaReceiptWithdrawalRecord>,
    pub(crate) intents: BTreeMap<(DfsObjectId, String), DfsWriteIntentRecord>,
    pub(crate) intent_receipts: BTreeMap<(DfsObjectId, String), DfsWriteIntentReceipt>,
    pub(crate) intent_receipt_retention: DfsWriteIntentReceiptRetentionState,
    pub(crate) intent_order: BTreeMap<(OffsetDateTime, String, DfsObjectId), (DfsObjectId, String)>,
    pub(crate) intent_targets: BTreeMap<(DfsObjectId, DfsVersion, String), (DfsObjectId, String)>,
    pub(crate) advisory_lock_states: BTreeMap<DfsObjectId, DfsAdvisoryLockStateRecord>,
    pub(crate) namespace_transaction_receipts: BTreeMap<String, DfsNamespaceTransactionReceipt>,
    /// Derived object-local reference-outcome lookup; never serialized.
    content_reference_outcome_ids: BTreeMap<DfsObjectId, BTreeSet<String>>,
    content_reference_outcome_objects: BTreeMap<String, DfsObjectId>,
    /// Sole immutable prepared body and exact terminal outcome owner.
    pub(crate) prepared_operations_v7:
        BTreeMap<String, crate::metadata_records::DfsPreparedOperationRecord>,
    pub(crate) prepared_operation_evictions: BTreeMap<String, DfsPreparedOperationEviction>,
    namespace_prepare_object_owners:
        BTreeMap<DfsFileSystemId, BTreeMap<DfsObjectId, BTreeSet<String>>>,
    namespace_prepare_path_owners: BTreeMap<DfsFileSystemId, BTreeMap<String, BTreeSet<String>>>,
    /// Derived from active request-owned prepares and rebuilt on snapshot load.
    pub(crate) namespace_prepare_request_owners: BTreeMap<(NodeId, String), BTreeSet<String>>,
    pub(crate) namespace_jobs: BTreeMap<String, DfsNamespaceJobRecord>,
    /// Nonserialized active parent-write lookup, derived only from durable jobs.
    parent_count_reservations: BTreeMap<(DfsFileSystemId, DfsObjectId), BTreeSet<String>>,
    /// Nonserialized running directory-move destination reservations by
    /// reserved subtree root, derived only from durable jobs.
    move_destination_reservations: BTreeMap<(DfsFileSystemId, String), BTreeSet<String>>,
    /// Nonserialized ids of the Running namespace jobs, derived only from
    /// durable jobs.
    running_namespace_jobs: BTreeSet<String>,
    pub(crate) namespace_job_receipts: BTreeMap<String, DfsNamespaceJobReceipt>,
    pub(crate) namespace_requests: BTreeMap<(NodeId, String), DfsNamespaceRequestRecord>,
    pub(crate) namespace_request_receipts: BTreeMap<(NodeId, String), DfsNamespaceRequestReceipt>,
    pub(crate) namespace_receipt_retention: DfsNamespaceReceiptRetentionState,
    /// Derived oldest-attempt-first recovery index. Terminal idempotency rows
    /// stay in `namespace_requests` without participating in maintenance scans.
    pub(crate) pending_namespace_request_order: BTreeSet<(OffsetDateTime, NodeId, String)>,
    /// Incremental operator projection maintained at the same table-op
    /// boundary as the materialized indexes above. Status must never rediscover
    /// these counts by walking a production-sized metadata generation.
    status_index: DfsMetadataStatusIndex,
    /// Current repair needs, derived at the same per-object boundary as the
    /// status projection; never serialized.
    repair_index: DfsRepairIndex,
}

type RetainedPathKey = (DfsFileSystemId, Option<String>, String);
const MAX_RETAINED_PATH_PREFIXES: usize = 64;
const MAX_RETAINED_PATH_PREFIX_BYTES: usize = 16 * 1024;

#[cfg(test)]
thread_local! {
    pub(crate) static RETAINED_PATH_INDEX_UPDATES: Cell<usize> = const { Cell::new(0) };
    static INTENT_INDEX_KEY_VISITS: Cell<usize> = const { Cell::new(0) };
}

fn retained_object_path_is_unchanged(
    previous: &DfsObjectMetadata,
    next: &DfsObjectMetadata,
) -> bool {
    previous.file_system_id == next.file_system_id
        && previous.path == next.path
        && previous.tombstone_provenance.is_none() == next.tombstone_provenance.is_none()
}

fn retained_version_index_is_unchanged(
    previous: &DfsObjectVersionRecord,
    next: &DfsObjectVersionRecord,
) -> bool {
    previous.file_system_id == next.file_system_id && previous.retention == next.retention
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct DfsRetainedRevisionIndex {
    pub(crate) captured_paths: BTreeMap<RetainedPathKey, BTreeSet<(DfsObjectId, DfsVersion)>>,
    pub(crate) unindexed_captured_paths:
        BTreeMap<(DfsFileSystemId, Option<String>), BTreeSet<(DfsObjectId, DfsVersion)>>,
    pub(crate) current_paths: BTreeMap<RetainedPathKey, BTreeSet<DfsObjectId>>,
    /// Deep/long paths remain bounded filtered candidates, instead of
    /// allocating a quadratic ancestor index or losing their families.
    pub(crate) unindexed_current_paths:
        BTreeMap<(DfsFileSystemId, Option<String>), BTreeSet<DfsObjectId>>,
    /// Immutable generations may span filesystems after an object move.
    /// Keep each retained filesystem family independent while projecting its
    /// current path, which is allowed to belong to another filesystem.
    pub(crate) object_versions:
        BTreeMap<(DfsObjectId, DfsFileSystemId, Option<String>), BTreeSet<DfsVersion>>,
}

fn retained_path_prefixes(path: &str) -> Option<Vec<String>> {
    if path.len() > 4096 {
        return None;
    }
    let path = crate::metadata_keys::normalize_posix_path(path);
    let mut prefixes = vec!["/".to_string()];
    let mut bytes = 1;
    for (index, character) in path.char_indices().skip(1) {
        if character == '/' {
            bytes += index;
            if prefixes.len() >= MAX_RETAINED_PATH_PREFIXES
                || bytes > MAX_RETAINED_PATH_PREFIX_BYTES
            {
                return None;
            }
            prefixes.push(path[..index].to_string());
        }
    }
    if path != "/" {
        if prefixes.len() >= MAX_RETAINED_PATH_PREFIXES
            || bytes + path.len() > MAX_RETAINED_PATH_PREFIX_BYTES
        {
            return None;
        }
        prefixes.push(path);
    }
    Some(prefixes)
}

fn remove_retained_index_value<K: Ord, V: Ord>(
    index: &mut BTreeMap<K, BTreeSet<V>>,
    key: &K,
    value: &V,
) {
    if let Some(values) = index.get_mut(key) {
        values.remove(value);
        if values.is_empty() {
            index.remove(key);
        }
    }
}

impl DfsRetainedRevisionIndex {
    fn update_object(&mut self, object: &DfsObjectMetadata, insert: bool) {
        if object.tombstone_provenance.is_some() {
            return;
        }
        let families = self
            .object_versions
            .range((
                Included((object.object_id.clone(), id_lower_bound(), None)),
                Unbounded,
            ))
            .take_while(|((object_id, _, _), _)| object_id == &object.object_id)
            .map(|((_, file_system_id, namespace), _)| (file_system_id.clone(), namespace.clone()))
            .collect::<Vec<_>>();
        if families.is_empty() {
            return;
        }
        #[cfg(test)]
        RETAINED_PATH_INDEX_UPDATES.set(RETAINED_PATH_INDEX_UPDATES.get().saturating_add(1));
        let Some(prefixes) = retained_path_prefixes(&object.path) else {
            for (file_system_id, namespace) in families {
                let key = (file_system_id, namespace);
                if insert {
                    self.unindexed_current_paths
                        .entry(key)
                        .or_default()
                        .insert(object.object_id.clone());
                } else {
                    remove_retained_index_value(
                        &mut self.unindexed_current_paths,
                        &key,
                        &object.object_id,
                    );
                }
            }
            return;
        };
        for (file_system_id, namespace) in families {
            for prefix in &prefixes {
                let key = (file_system_id.clone(), namespace.clone(), prefix.clone());
                if insert {
                    self.current_paths
                        .entry(key)
                        .or_default()
                        .insert(object.object_id.clone());
                } else {
                    remove_retained_index_value(&mut self.current_paths, &key, &object.object_id);
                }
            }
        }
    }

    fn update_version(
        &mut self,
        version: &DfsObjectVersionRecord,
        current: Option<&DfsObjectMetadata>,
        insert: bool,
    ) {
        #[cfg(test)]
        if version.has_retention_grant() {
            RETAINED_PATH_INDEX_UPDATES.set(RETAINED_PATH_INDEX_UPDATES.get().saturating_add(1));
        }
        let mut namespaces = BTreeSet::new();
        let identity = (version.object_id.clone(), version.version.clone());
        for (namespace, state) in &version.retention {
            let Some(grant) = &state.grant else { continue };
            for owner in [None, Some(namespace.clone())] {
                namespaces.insert(owner.clone());
                let Some(prefixes) = retained_path_prefixes(&grant.object.path) else {
                    let key = (version.file_system_id.clone(), owner);
                    if insert {
                        self.unindexed_captured_paths
                            .entry(key)
                            .or_default()
                            .insert(identity.clone());
                    } else {
                        remove_retained_index_value(
                            &mut self.unindexed_captured_paths,
                            &key,
                            &identity,
                        );
                    }
                    continue;
                };
                for prefix in prefixes {
                    let key = (version.file_system_id.clone(), owner.clone(), prefix);
                    if insert {
                        self.captured_paths
                            .entry(key)
                            .or_default()
                            .insert(identity.clone());
                    } else {
                        remove_retained_index_value(&mut self.captured_paths, &key, &identity);
                    }
                }
            }
        }
        for namespace in namespaces {
            let key = (
                version.object_id.clone(),
                version.file_system_id.clone(),
                namespace.clone(),
            );
            let had_versions = self.object_versions.contains_key(&key);
            if insert {
                self.object_versions
                    .entry(key.clone())
                    .or_default()
                    .insert(version.version.clone());
            } else {
                remove_retained_index_value(&mut self.object_versions, &key, &version.version);
            }
            let has_versions = self.object_versions.contains_key(&key);
            if had_versions == has_versions {
                continue;
            }
            if let Some(object) = current.filter(|object| object.tombstone_provenance.is_none()) {
                let Some(prefixes) = retained_path_prefixes(&object.path) else {
                    let key = (version.file_system_id.clone(), namespace);
                    if has_versions {
                        self.unindexed_current_paths
                            .entry(key)
                            .or_default()
                            .insert(object.object_id.clone());
                    } else {
                        remove_retained_index_value(
                            &mut self.unindexed_current_paths,
                            &key,
                            &object.object_id,
                        );
                    }
                    continue;
                };
                for prefix in prefixes {
                    let path_key = (version.file_system_id.clone(), namespace.clone(), prefix);
                    if has_versions {
                        self.current_paths
                            .entry(path_key)
                            .or_default()
                            .insert(object.object_id.clone());
                    } else {
                        remove_retained_index_value(
                            &mut self.current_paths,
                            &path_key,
                            &object.object_id,
                        );
                    }
                }
            }
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct DfsMetadataStatusIndex {
    object_count_by_file_system: BTreeMap<DfsFileSystemId, usize>,
    pending_repair_count_by_file_system: BTreeMap<DfsFileSystemId, usize>,
    expiring_repairs_by_file_system:
        BTreeMap<DfsFileSystemId, crate::repair_deadlines::RepairDeadlines>,
    pending_write_count: usize,
    pending_namespace_job_count: usize,
    failed_namespace_job_count: usize,
    cross_shard_namespace_lock_ref_counts: BTreeMap<(DfsFileSystemId, String), usize>,
    cross_shard_namespace_lock_count: usize,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct DfsMetadataObjectStatusContribution {
    file_system_id: Option<DfsFileSystemId>,
    object_count: usize,
    pending_repair_count: usize,
    expiring_repairs: BTreeMap<u64, usize>,
    pending_write_count: usize,
}

#[derive(Clone, Debug)]
enum DfsMetadataStatusMutationEffect {
    None,
    Object(DfsObjectId),
    NamespaceJob(String),
}

#[derive(Clone, Debug, Default)]
pub(crate) struct DfsMetadataTableState {
    pub(crate) tables: DfsMetadataTables,
    pub(crate) generation: Option<crate::DfsMetadataGenerationIdentity>,
    pub(crate) table_log_offset: u64,

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
    pub(crate) storage_generation:
        Option<crate::metadata_table_store::MetadataTableStorageGeneration>,
    pub(crate) snapshot_fingerprint: Option<MetadataTableSnapshotFingerprint>,
    pub(crate) checkpoint_fingerprint: Option<MetadataTableSnapshotFingerprint>,
    pub(crate) table_log_offset: u64,

    pub(crate) materialized_through_sequence: Option<u64>,
    pub(crate) snapshot_format_version: Option<u16>,
    pub(crate) prefix_proof: Option<DfsMetadataPrefixProof>,
    pub(crate) tables: Arc<DfsMetadataTables>,
}

pub(crate) enum DfsMetadataTablesSnapshot<'a> {
    Borrowed(&'a DfsMetadataTables),
    Shared {
        tables: Arc<DfsMetadataTables>,
        prefix_proof: Option<DfsMetadataPrefixProof>,
        materialized_through_sequence: Option<u64>,
    },
}

impl Deref for DfsMetadataTablesSnapshot<'_> {
    type Target = DfsMetadataTables;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Borrowed(tables) => tables,
            Self::Shared { tables, .. } => tables,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct MetadataTableReplay {
    pub(crate) offset: u64,

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

/// Owned active-prepare footprint built while the prepare body is validated.
///
/// Foreground prepare admission passes this exact value through overlap
/// validation and post-journal table indexing. Replay and snapshot rebuilds
/// can still construct it from the self-contained prepare record at the safe
/// generic apply boundary.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct DfsNamespacePrepareFootprint {
    file_system_id: DfsFileSystemId,
    operation_id: String,
    operation_digest: String,
    owner_epoch: u64,
    transaction_id: String,
    expected_object_count: usize,
    expected_path_count: usize,
    transaction_object_count: usize,
    transaction_version_count: usize,
    content_receipt_count: usize,
    object_ids: BTreeSet<DfsObjectId>,
    normalized_paths: BTreeSet<String>,
}

impl DfsNamespacePrepareFootprint {
    pub(crate) fn from_validated_parts(
        prepare: &DfsPreparedOperationRecord,
        object_ids: BTreeSet<DfsObjectId>,
        normalized_paths: BTreeSet<String>,
    ) -> Self {
        Self {
            file_system_id: prepare.file_system_id.clone(),
            operation_id: prepare.operation_id.clone(),
            operation_digest: prepare.digest.payload_digest.clone(),
            owner_epoch: prepare.owner_epoch,
            transaction_id: prepare.transaction.transaction_id.clone(),
            expected_object_count: prepare.expected_object_versions.len(),
            expected_path_count: prepare.expected_path_occupants.len(),
            transaction_object_count: prepare.transaction.objects.len(),
            transaction_version_count: prepare.transaction.versions.len(),
            content_receipt_count: prepare.content_receipts.len(),
            object_ids,
            normalized_paths,
        }
    }

    pub(crate) fn object_ids(&self) -> &BTreeSet<DfsObjectId> {
        &self.object_ids
    }

    pub(crate) fn normalized_paths(&self) -> &BTreeSet<String> {
        &self.normalized_paths
    }

    fn into_parts(self) -> (BTreeSet<DfsObjectId>, BTreeSet<String>) {
        (self.object_ids, self.normalized_paths)
    }
}

pub(crate) fn prepared_payload_reservation_keys(
    payload: &crate::DfsPreparedOperationPayload,
) -> (BTreeSet<DfsObjectId>, BTreeSet<String>) {
    let mut object_ids = payload
        .expected_object_versions
        .iter()
        .map(|expected| expected.object_id.clone())
        .collect::<BTreeSet<_>>();
    object_ids.extend(
        payload
            .transaction
            .objects
            .iter()
            .map(|object| object.object_id.clone()),
    );
    object_ids.extend(
        payload
            .transaction
            .content_reference_identity
            .iter()
            .map(|identity| identity.object_id.clone()),
    );
    object_ids.extend(
        payload
            .metadata_effects
            .iter()
            .flat_map(|effect| effect.affected_object_ids()),
    );
    let mut paths = payload
        .expected_path_occupants
        .iter()
        .map(|expected| normalize_path(&expected.path))
        .collect::<BTreeSet<_>>();
    paths.extend(
        payload
            .transaction
            .objects
            .iter()
            .map(|object| normalize_path(&object.path)),
    );
    (object_ids, paths)
}

pub(crate) fn namespace_prepare_footprint(
    prepare: &DfsPreparedOperationRecord,
) -> DfsNamespacePrepareFootprint {
    let (object_ids, paths) = prepared_payload_reservation_keys(&prepare.payload);
    DfsNamespacePrepareFootprint::from_validated_parts(prepare, object_ids, paths)
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
    let mut retention = DfsNamespaceReceiptRetentionState {
        transaction_floor: Some(DfsNamespaceReceiptOrderKey {
            created_at: receipt.created_at,
            identity: receipt.transaction_id.clone(),
        }),
        ..DfsNamespaceReceiptRetentionState::default()
    };
    if let Some(identity) = &receipt.content_reference_identity {
        let operation_id = receipt
            .transaction_id
            .strip_prefix("file-history:")
            .unwrap_or(&receipt.transaction_id);
        let token = crate::intent::stable_operation_order_token(operation_id);
        retention.content_reference_outcome_retention.insert(
            identity.object_id.clone(),
            crate::DfsContentReferenceOutcomeRetention {
                file_system_id: identity.file_system_id.clone(),
                opaque_operation_history_evicted: token.is_none(),
                max_evicted_operation_order_token: token,
            },
        );
    }
    retention
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
    fn refresh_content_reference_outcome_index(&mut self, transaction_id: &str) {
        if let Some(object_id) = self
            .content_reference_outcome_objects
            .remove(transaction_id)
            && let Some(ids) = self.content_reference_outcome_ids.get_mut(&object_id)
        {
            ids.remove(transaction_id);
            if ids.is_empty() {
                self.content_reference_outcome_ids.remove(&object_id);
            }
        }
        let identity = self
            .namespace_transaction_receipts
            .get(transaction_id)
            .and_then(|receipt| receipt.content_reference_identity.as_ref());
        if let Some(identity) = identity {
            let object_id = identity.object_id.clone();
            self.content_reference_outcome_objects
                .insert(transaction_id.into(), object_id.clone());
            self.content_reference_outcome_ids
                .entry(object_id)
                .or_default()
                .insert(transaction_id.into());
        }
    }

    pub(crate) fn content_reference_outcome_ids_for_object(
        &self,
        object_id: &DfsObjectId,
    ) -> Option<&BTreeSet<String>> {
        self.content_reference_outcome_ids.get(object_id)
    }
    pub(crate) fn intent_record(
        &self,
        object_id: &DfsObjectId,
        intent_id: &str,
    ) -> Option<DfsWriteIntentRecord> {
        let key = (object_id.clone(), intent_id.to_string());
        self.intents.get(&key).cloned().or_else(|| {
            self.intent_receipts
                .get(&key)
                .map(DfsWriteIntentReceipt::to_intent_record)
        })
    }

    /// Return the operator counters maintained incrementally with table
    /// materialization. This is bounded by the filesystem catalog, not by the
    /// number of objects, versions, receipts, intents, or jobs.
    pub(crate) fn metadata_status_summary(&self) -> DfsMetadataSummary {
        self.metadata_status_summary_at(crate::dfs_now_unix_nanos())
    }

    pub(crate) fn metadata_status_summary_at(&self, now: u64) -> DfsMetadataSummary {
        let object_count = self
            .file_systems
            .keys()
            .map(|file_system_id| {
                self.status_index
                    .object_count_by_file_system
                    .get(file_system_id)
                    .copied()
                    .unwrap_or_default()
            })
            .fold(0usize, usize::saturating_add);
        let pending_repair_count = self
            .file_systems
            .keys()
            .map(|file_system_id| {
                self.status_index
                    .pending_repair_count_by_file_system
                    .get(file_system_id)
                    .copied()
                    .unwrap_or_default()
                    .saturating_add(
                        self.status_index
                            .expiring_repairs_by_file_system
                            .get(file_system_id)
                            .map_or(0, |deadlines| deadlines.count_after(now)),
                    )
            })
            .fold(0usize, usize::saturating_add);
        DfsMetadataSummary {
            file_system_count: self.file_systems.len(),
            object_count,
            version_count: self.versions.len(),
            receipt_count: self.receipts.len(),
            intent_count: self.intents.len(),
            pending_write_count: self.status_index.pending_write_count,
            pending_repair_count,
            pending_namespace_job_count: self.status_index.pending_namespace_job_count,
            failed_namespace_job_count: self.status_index.failed_namespace_job_count,
            cross_shard_namespace_lock_count: self.status_index.cross_shard_namespace_lock_count,
        }
    }

    fn status_mutation_effect(op: &DfsMetadataTableOp) -> DfsMetadataStatusMutationEffect {
        match op {
            DfsMetadataTableOp::PutObject { object } => {
                DfsMetadataStatusMutationEffect::Object(object.object_id.clone())
            }
            DfsMetadataTableOp::RemoveObject { object_id }
            | DfsMetadataTableOp::RemoveVersion { object_id, .. }
            | DfsMetadataTableOp::RemoveVersionPin { object_id, .. }
            | DfsMetadataTableOp::RemoveFileSystemObject { object_id, .. }
            | DfsMetadataTableOp::RemoveReceipt { object_id, .. }
            | DfsMetadataTableOp::RemoveRetainedRepairOwner { object_id, .. }
            | DfsMetadataTableOp::PutIntentReceipt {
                receipt: DfsWriteIntentReceipt { object_id, .. },
            } => DfsMetadataStatusMutationEffect::Object(object_id.clone()),
            DfsMetadataTableOp::PutFileSystemObject { path_record } => {
                DfsMetadataStatusMutationEffect::Object(path_record.object_id.clone())
            }
            DfsMetadataTableOp::PutVersion { version } => {
                DfsMetadataStatusMutationEffect::Object(version.object_id.clone())
            }
            DfsMetadataTableOp::PutVersionPin { pin } => {
                DfsMetadataStatusMutationEffect::Object(pin.object_id.clone())
            }
            DfsMetadataTableOp::ReconcileCrossShardPinTransfer { transfer, .. } => {
                DfsMetadataStatusMutationEffect::Object(transfer.object_id.clone())
            }
            DfsMetadataTableOp::PutCompactedTombstone { object } => {
                DfsMetadataStatusMutationEffect::Object(object.object_id.clone())
            }
            DfsMetadataTableOp::PutReceipt { receipt } => {
                DfsMetadataStatusMutationEffect::Object(receipt.receipt.object_id.clone())
            }
            DfsMetadataTableOp::WithdrawReceipt { withdrawal } => {
                DfsMetadataStatusMutationEffect::Object(withdrawal.object_id.clone())
            }
            DfsMetadataTableOp::PutIntent { intent } => {
                DfsMetadataStatusMutationEffect::Object(intent.object_id.clone())
            }

            DfsMetadataTableOp::PutNamespaceJob { job } => {
                DfsMetadataStatusMutationEffect::NamespaceJob(job.job_id.clone())
            }
            DfsMetadataTableOp::PutNamespaceJobReceipt { receipt } => {
                DfsMetadataStatusMutationEffect::NamespaceJob(receipt.job_id.clone())
            }
            _ => DfsMetadataStatusMutationEffect::None,
        }
    }

    fn object_status_contribution(
        &self,
        object_id: &DfsObjectId,
    ) -> DfsMetadataObjectStatusContribution {
        self.object_status_and_repair_needs(object_id).0
    }

    /// One per-object derivation feeds both the status projection and the
    /// repair index, so the two cannot disagree about an object's demand.
    fn object_status_and_repair_needs(
        &self,
        object_id: &DfsObjectId,
    ) -> (
        DfsMetadataObjectStatusContribution,
        BTreeMap<DfsRepairNeedKey, DfsRepairNeed>,
    ) {
        let intents = self
            .intents
            .range((Included((object_id.clone(), String::new())), Unbounded))
            .take_while(|((record_object_id, _), _)| record_object_id == object_id)
            .map(|(_, intent)| intent)
            .collect::<Vec<_>>();
        let pending_write_count = intents
            .iter()
            .filter(|intent| write_intent_status_is_active(&intent.status))
            .count();

        let Some(object) = self
            .objects
            .get(object_id)
            .or_else(|| self.compacted_tombstones.get(object_id))
        else {
            return (
                DfsMetadataObjectStatusContribution {
                    pending_write_count,
                    ..DfsMetadataObjectStatusContribution::default()
                },
                BTreeMap::new(),
            );
        };
        let path_record = self
            .file_system_objects
            .get(&(object.file_system_id.clone(), object.object_id.clone()));
        let object_countable = path_record.is_some_and(|path_record| {
            path_record.file_system_id == object.file_system_id
                && normalize_path(&object.path) == path_record.path
        });
        // A cross-shard source-eviction tombstone is retained as migration
        // evidence, not as a logical object. The exact sharded scanner rejects
        // it through its routing callback; the bounded projection must encode
        // the same decision while the affected object is already in hand.
        if object.kind == DfsObjectKind::Tombstone
            && matches!(
                object.tombstone_provenance.as_ref(),
                Some(crate::DfsTombstoneProvenance::SourceEviction { .. })
            )
        {
            return (
                DfsMetadataObjectStatusContribution {
                    pending_write_count,
                    ..DfsMetadataObjectStatusContribution::default()
                },
                BTreeMap::new(),
            );
        }

        let versions = self
            .versions
            .range((Included((object_id.clone(), id_lower_bound())), Unbounded))
            .take_while(|((id, _), _)| id == object_id)
            .map(|(_, record)| record);
        let (_, pins) = crate::metadata_summary::object_history_root_inputs(self, object_id);
        let receipts = self
            .receipts
            .range((
                Included((object_id.clone(), id_lower_bound(), id_lower_bound())),
                Unbounded,
            ))
            .take_while(|((id, _, _), _)| id == object_id)
            .map(|(_, record)| record);
        let mut pending_repair_count = 0usize;
        let mut expiring_repairs = BTreeMap::<u64, usize>::new();
        let mut needs = BTreeMap::new();
        for (key, demand) in crate::metadata_summary::repair_demands_for_object_observed_at(
            object,
            intents.iter().copied(),
            receipts,
            versions,
            &pins,
            None,
        ) {
            match demand.root {
                crate::metadata_summary::RepairRoot::Permanent => {
                    pending_repair_count = pending_repair_count.saturating_add(1);
                }
                crate::metadata_summary::RepairRoot::Until(deadline) => {
                    let count = expiring_repairs.entry(deadline).or_default();
                    *count = count.saturating_add(1);
                }
            }
            // A version names one content hash; a conflicting second hash for
            // the same missing replica keeps the first in key order.
            let need_key = (
                object_id.clone(),
                key.version.clone(),
                key.replica_node_id.clone(),
            );
            if needs.contains_key(&need_key) {
                continue;
            }
            let receipt_generation =
                self.receipt_slot_generation(object_id, &key.version, &key.replica_node_id);
            // The same unfinished owner that file-history enrollment defers to.
            let enrollment_blocked_by = demand
                .owner_intent_id
                .is_none()
                .then(|| {
                    intents
                        .iter()
                        .filter(|intent| {
                            intent.file_system_id == object.file_system_id
                                && intent.target_version == key.version
                                && write_intent_status_is_active(&intent.status)
                        })
                        .map(|intent| intent.intent_id.clone())
                        .min()
                })
                .flatten();
            needs.insert(
                need_key,
                DfsRepairNeed {
                    file_system_id: object.file_system_id.clone(),
                    content_hash: key.content_hash,
                    owner_root: demand.owner_root(),
                    owner_intent_id: demand.owner_intent_id,
                    root: demand.root,
                    receipt_generation,
                    enrollment_blocked_by,
                },
            );
        }

        (
            DfsMetadataObjectStatusContribution {
                file_system_id: Some(object.file_system_id.clone()),
                object_count: usize::from(object_countable),
                pending_repair_count,
                expiring_repairs,
                pending_write_count,
            },
            needs,
        )
    }

    fn adjust_status_map_count(
        counts: &mut BTreeMap<DfsFileSystemId, usize>,
        file_system_id: &DfsFileSystemId,
        before: usize,
        after: usize,
    ) {
        let current = counts.get(file_system_id).copied().unwrap_or_default();
        debug_assert!(current >= before);
        let updated = current.saturating_sub(before).saturating_add(after);
        if updated == 0 {
            counts.remove(file_system_id);
        } else {
            counts.insert(file_system_id.clone(), updated);
        }
    }

    fn replace_object_status_contribution(
        &mut self,
        before: DfsMetadataObjectStatusContribution,
        after: DfsMetadataObjectStatusContribution,
    ) {
        let mut file_system_ids = BTreeSet::new();
        file_system_ids.extend(before.file_system_id.iter().cloned());
        file_system_ids.extend(after.file_system_id.iter().cloned());
        for file_system_id in file_system_ids {
            let before_matches = before.file_system_id.as_ref() == Some(&file_system_id);
            let after_matches = after.file_system_id.as_ref() == Some(&file_system_id);
            let deadlines = before
                .expiring_repairs
                .keys()
                .chain(after.expiring_repairs.keys())
                .copied()
                .collect::<BTreeSet<_>>();
            if !deadlines.is_empty() {
                let index = self
                    .status_index
                    .expiring_repairs_by_file_system
                    .entry(file_system_id.clone())
                    .or_default();
                for deadline in deadlines {
                    index.replace(
                        deadline,
                        if before_matches {
                            before.expiring_repairs.get(&deadline).copied().unwrap_or(0)
                        } else {
                            0
                        },
                        if after_matches {
                            after.expiring_repairs.get(&deadline).copied().unwrap_or(0)
                        } else {
                            0
                        },
                    );
                }
                if index.is_empty() {
                    self.status_index
                        .expiring_repairs_by_file_system
                        .remove(&file_system_id);
                }
            }
            Self::adjust_status_map_count(
                &mut self.status_index.object_count_by_file_system,
                &file_system_id,
                if before_matches {
                    before.object_count
                } else {
                    Default::default()
                },
                if after_matches {
                    after.object_count
                } else {
                    Default::default()
                },
            );
            Self::adjust_status_map_count(
                &mut self.status_index.pending_repair_count_by_file_system,
                &file_system_id,
                if before_matches {
                    before.pending_repair_count
                } else {
                    Default::default()
                },
                if after_matches {
                    after.pending_repair_count
                } else {
                    Default::default()
                },
            );
        }
        debug_assert!(self.status_index.pending_write_count >= before.pending_write_count);
        self.status_index.pending_write_count = self
            .status_index
            .pending_write_count
            .saturating_sub(before.pending_write_count)
            .saturating_add(after.pending_write_count);
    }

    fn namespace_job_status(
        &self,
        job_id: &str,
    ) -> Option<(DfsNamespaceJobStatus, Option<(DfsFileSystemId, String)>)> {
        self.namespace_jobs.get(job_id).map(|job| {
            (
                job.status.clone(),
                (job.status == DfsNamespaceJobStatus::Running
                    && job.kind.holds_cross_shard_namespace_lock_while_running())
                .then(|| (job.file_system_id.clone(), job.root_path.clone())),
            )
        })
    }

    fn replace_namespace_job_status(
        &mut self,
        before: Option<(DfsNamespaceJobStatus, Option<(DfsFileSystemId, String)>)>,
        after: Option<(DfsNamespaceJobStatus, Option<(DfsFileSystemId, String)>)>,
    ) {
        if before == after {
            return;
        }
        match before.as_ref().map(|(status, _)| status) {
            Some(DfsNamespaceJobStatus::Running) => {
                debug_assert!(self.status_index.pending_namespace_job_count > 0);
                self.status_index.pending_namespace_job_count = self
                    .status_index
                    .pending_namespace_job_count
                    .saturating_sub(1);
            }
            Some(DfsNamespaceJobStatus::Failed) => {
                debug_assert!(self.status_index.failed_namespace_job_count > 0);
                self.status_index.failed_namespace_job_count = self
                    .status_index
                    .failed_namespace_job_count
                    .saturating_sub(1);
            }
            Some(DfsNamespaceJobStatus::Completed) | None => {}
        }
        match after.as_ref().map(|(status, _)| status) {
            Some(DfsNamespaceJobStatus::Running) => {
                self.status_index.pending_namespace_job_count = self
                    .status_index
                    .pending_namespace_job_count
                    .saturating_add(1);
            }
            Some(DfsNamespaceJobStatus::Failed) => {
                self.status_index.failed_namespace_job_count = self
                    .status_index
                    .failed_namespace_job_count
                    .saturating_add(1);
            }
            Some(DfsNamespaceJobStatus::Completed) | None => {}
        }
        if let Some(lock_key) = before.as_ref().and_then(|(_, lock_key)| lock_key.as_ref()) {
            let remove_effective_lock = self
                .status_index
                .cross_shard_namespace_lock_ref_counts
                .get_mut(lock_key)
                .is_some_and(|ref_count| {
                    debug_assert!(*ref_count > 0);
                    *ref_count = ref_count.saturating_sub(1);
                    *ref_count == 0
                });
            if remove_effective_lock {
                self.status_index
                    .cross_shard_namespace_lock_ref_counts
                    .remove(lock_key);
                debug_assert!(self.status_index.cross_shard_namespace_lock_count > 0);
                self.status_index.cross_shard_namespace_lock_count = self
                    .status_index
                    .cross_shard_namespace_lock_count
                    .saturating_sub(1);
            }
        }
        if let Some(lock_key) = after.as_ref().and_then(|(_, lock_key)| lock_key.as_ref()) {
            let ref_count = self
                .status_index
                .cross_shard_namespace_lock_ref_counts
                .entry(lock_key.clone())
                .or_default();
            if *ref_count == 0 {
                self.status_index.cross_shard_namespace_lock_count = self
                    .status_index
                    .cross_shard_namespace_lock_count
                    .saturating_add(1);
            }
            *ref_count = ref_count.saturating_add(1);
        }
    }

    /// Apply one table operation while preserving fail-closed validation for
    /// compact semantic transitions.
    ///
    /// Ordinary operations are self-contained and retain the historical
    /// infallible apply path. A committed-prepare marker intentionally refers
    /// to the full prepared body in the preceding table state, so durable-log
    /// replay uses this checked entry point and rejects a missing or mismatched
    /// dependency instead of silently publishing partial metadata.
    pub(crate) fn apply_checked(&mut self, op: DfsMetadataTableOp) -> Result<()> {
        let mut rollback = DfsMetadataTableRollback::default();
        let result = self.apply_checked_with_rollback(op, &mut rollback);
        if result.is_err() {
            rollback.rollback(self);
        }
        result
    }
    pub(crate) fn terminal_eviction_for_digest(
        &self, digest: &crate::DfsPreparedOperationDigest,
    ) -> Result<DfsPreparedOperationEviction> {
        if let Some(record) = self.prepared_operations_v7.get(&digest.operation_id) {
            if record.digest != *digest {
                return Err(MetadataBackendError::Serde("eviction changes its original operation digest".into()));
            }
            let terminal = DfsPreparedOperationEviction::from_terminal_record(record)?;
            if let Some(retained) = self.prepared_operation_evictions.get(&digest.operation_id) {
                retained.validate_against_record(record)?;
            }
            return Ok(terminal);
        }
        let retained = self.prepared_operation_evictions.get(&digest.operation_id)
            .ok_or_else(|| MetadataBackendError::Serde("eviction has neither original body nor exact terminal evidence".into()))?;
        retained.validate()?;
        if retained.digest != *digest {
            return Err(MetadataBackendError::Serde("eviction replay changes its retained terminal identity".into()));
        }
        Ok(retained.clone())
    }

    pub(crate) fn validate_checked(&self, op: &DfsMetadataTableOp) -> Result<()> {
        match op {
            DfsMetadataTableOp::PutNamespaceJob { job } if job.kind == crate::DfsNamespaceJobKind::ParentCountParticipant => {
                let (participant, state) = job.parent_count_participant().ok_or_else(|| MetadataBackendError::Serde("parent participant job lost its declaration".into()))?;
                participant.validate()?;
                if !participant.matches_job(job, state) { return Err(MetadataBackendError::Serde("parent participant job changes its declared identity".into())); }
                Ok(())
            }
            DfsMetadataTableOp::PutNamespaceJob { job }
                if job.kind == crate::DfsNamespaceJobKind::DirectoryMoveDestinationReservation =>
            {
                Self::validate_move_destination_reservation(job)
            }
            DfsMetadataTableOp::PutNamespaceJobReceipt { receipt } if receipt.kind == crate::DfsNamespaceJobKind::ParentCountParticipant => {
                if receipt.status != DfsNamespaceJobStatus::Completed || !receipt.parent_count.as_ref().is_some_and(|terminal| terminal.participant_id == receipt.job_id && terminal.state != crate::DfsParentCountState::Reserved) {
                    return Err(MetadataBackendError::Serde("parent participant receipt has no exact terminal decision".into()));
                }
                Ok(())
            }
            DfsMetadataTableOp::PutReceipt { receipt } => {
                self.validate_receipt_publication(receipt)
            }
            DfsMetadataTableOp::WithdrawReceipt { withdrawal } => {
                self.validate_receipt_withdrawal(withdrawal)
            }
            DfsMetadataTableOp::PutPreparedOperationV7 { record } => record.validate(),
            DfsMetadataTableOp::CommitPreparedOperationV7 {
                digest,
                committed_entry,
            } => self.validate_prepared_operation_v7_commit(digest, committed_entry),
            DfsMetadataTableOp::RemovePreparedOperation { digest } => {
                self.terminal_eviction_for_digest(digest).map(drop)
            }
            DfsMetadataTableOp::ResolvePreparedOperation { digest, status } => {
                let record = self
                    .prepared_operations_v7
                    .get(&digest.operation_id)
                    .ok_or_else(|| {
                        MetadataBackendError::Serde("resolution lacks current preparation".into())
                    })?;
                if record.digest != *digest
                    || record.status != DfsPreparedOperationStatus::Prepared
                    || !matches!(
                        status,
                        DfsPreparedOperationStatus::Aborted
                            | DfsPreparedOperationStatus::Superseded
                    )
                {
                    return Err(MetadataBackendError::Serde(
                        "resolution preparation compare changed".into(),
                    ));
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
    /// Current generation of one receipt slot: the present receipt's, the
    /// withdrawal tombstone's, or zero for a slot that was never withdrawn.
    pub(crate) fn receipt_slot_generation(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
    ) -> u64 {
        let key = (object_id.clone(), version.clone(), replica_node_id.clone());
        self.receipts
            .get(&key)
            .map(|receipt| receipt.generation)
            .or_else(|| {
                self.receipt_withdrawals
                    .get(&key)
                    .map(|withdrawal| withdrawal.generation)
            })
            .unwrap_or(0)
    }

    /// A version keeps its receipt-slot generations while it has a version row
    /// or is its object's head; whole-version collection forgets them.
    pub(crate) fn receipt_version_is_live(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
    ) -> bool {
        self.versions
            .contains_key(&(object_id.clone(), version.clone()))
            || self
                .objects
                .get(object_id)
                .is_some_and(|object| object.version == *version)
    }

    fn receipt_withdrawal_keys_for_version(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
    ) -> Vec<(DfsObjectId, DfsVersion, NodeId)> {
        self.receipt_withdrawals
            .range((
                Included((object_id.clone(), version.clone(), id_lower_bound())),
                Unbounded,
            ))
            .take_while(|((object, slot_version, _), _)| {
                object == object_id && slot_version == version
            })
            .map(|(key, _)| key.clone())
            .collect()
    }

    /// Publication compare-and-swap: the receipt names the slot generation its
    /// publisher planned against. A same-generation publication over a present
    /// receipt leaves the generation unchanged.
    pub(crate) fn validate_receipt_publication(
        &self,
        receipt: &DfsReplicaReceiptRecord,
    ) -> Result<()> {
        let current = self.receipt_slot_generation(
            &receipt.receipt.object_id,
            &receipt.receipt.committed_version,
            &receipt.replica_node_id,
        );
        if current != receipt.generation {
            return Err(MetadataBackendError::ReceiptGenerationSuperseded {
                object_id: receipt.receipt.object_id.clone(),
                version: receipt.receipt.committed_version.clone(),
                replica_node_id: receipt.replica_node_id.clone(),
                expected: receipt.generation,
                current,
            });
        }
        Ok(())
    }

    /// Withdrawal compare-and-swap: it advances exactly the observed slot
    /// generation of a live version, whether or not a receipt is present.
    fn validate_receipt_withdrawal(
        &self,
        withdrawal: &DfsReplicaReceiptWithdrawalRecord,
    ) -> Result<()> {
        let expected = withdrawal.generation.checked_sub(1).ok_or_else(|| {
            MetadataBackendError::Serde("receipt withdrawal must advance its slot".into())
        })?;
        if !self.receipt_version_is_live(&withdrawal.object_id, &withdrawal.version) {
            return Err(MetadataBackendError::Serde(
                "receipt withdrawal targets a collected version".into(),
            ));
        }
        let current = self.receipt_slot_generation(
            &withdrawal.object_id,
            &withdrawal.version,
            &withdrawal.replica_node_id,
        );
        if current != expected {
            return Err(MetadataBackendError::ReceiptGenerationSuperseded {
                object_id: withdrawal.object_id.clone(),
                version: withdrawal.version.clone(),
                replica_node_id: withdrawal.replica_node_id.clone(),
                expected,
                current,
            });
        }
        Ok(())
    }

    /// Install one withdrawn slot from a snapshot image. Snapshots carry table
    /// state, not publications, so no generation compare applies.
    pub(crate) fn load_receipt_withdrawal(
        &mut self,
        withdrawal: DfsReplicaReceiptWithdrawalRecord,
    ) -> Result<()> {
        let key = withdrawal.slot_key();
        if withdrawal.generation == 0 || self.receipts.contains_key(&key) {
            return Err(MetadataBackendError::Serde(
                "snapshot receipt withdrawal must be the slot's only advanced row".into(),
            ));
        }
        let object_id = key.0.clone();
        if self.receipt_withdrawals.insert(key, withdrawal).is_some() {
            return Err(MetadataBackendError::Serde(
                "snapshot repeats a receipt withdrawal".into(),
            ));
        }
        // The tombstone carries the slot generation the derived index records.
        self.refresh_repair_index_for_object(&object_id);
        Ok(())
    }

    pub(crate) fn apply_checked_with_rollback(
        &mut self,
        op: DfsMetadataTableOp,
        rollback: &mut DfsMetadataTableRollback,
    ) -> Result<()> {
        match op {
            DfsMetadataTableOp::PutPreparedOperationV7 { record } => {
                self.apply_prepared_operation_v7_with_rollback(*record, rollback)
            }
            DfsMetadataTableOp::CommitPreparedOperationV7 {
                digest,
                committed_entry,
            } => {
                self.commit_prepared_operation_v7_with_rollback(&digest, &committed_entry, rollback)
            }
            DfsMetadataTableOp::RemovePreparedOperation { digest } => {
                // Derive and compare the original witness before changing any
                // table. Exact replay keeps the retained terminal unchanged.
                let terminal = self.terminal_eviction_for_digest(&digest)?;
                if !self.prepared_operations_v7.contains_key(&digest.operation_id) { return Ok(()); }
                let op = DfsMetadataTableOp::RemovePreparedOperation { digest: digest.clone() };
                rollback.record(self, &op);
                let record = self.prepared_operations_v7.remove(&digest.operation_id).expect("validated eviction");
                self.unindex_namespace_prepare_footprint(&record);
                self.unindex_namespace_prepare_request(&record);
                self.prepared_operation_evictions.insert(digest.operation_id.clone(), terminal);
                Ok(())
            }
            DfsMetadataTableOp::ResolvePreparedOperation { digest, status } => {
                let op = DfsMetadataTableOp::ResolvePreparedOperation {
                    digest: digest.clone(),
                    status,
                };
                self.validate_checked(&op)?;
                rollback.record(self, &op);
                let mut record = self
                    .prepared_operations_v7
                    .get(&digest.operation_id)
                    .expect("validated resolution")
                    .clone();
                record.status = status;
                self.put_prepared_operation_indexed(record);
                Ok(())
            }
            op @ (DfsMetadataTableOp::PutReceipt { .. }
            | DfsMetadataTableOp::WithdrawReceipt { .. }
            | DfsMetadataTableOp::PutNamespaceJob { .. }
            | DfsMetadataTableOp::PutNamespaceJobReceipt { .. }) => {
                self.validate_checked(&op)?;
                self.apply_with_rollback(op, rollback);
                Ok(())
            }
            op => {
                self.apply_with_rollback(op, rollback);
                Ok(())
            }
        }
    }

    pub(crate) fn namespace_request_completion_is_materialized(
        &self,
        completion: &DfsNamespaceRequestRecord,
    ) -> bool {
        let key = completion.request_key();
        self.namespace_requests
            .get(&key)
            .is_some_and(|request| request.matches_completed_outcome(completion))
            || self
                .namespace_request_receipts
                .get(&key)
                .is_some_and(|receipt| receipt.matches_completed_request(completion))
    }

    pub(crate) fn compact_namespace_request_completion_is_materialized(
        &self,
        completion: &DfsNamespaceRequestCompletion,
    ) -> bool {
        let key = completion.request_key();
        self.namespace_requests
            .get(&key)
            .is_some_and(|request| completion.matches_completed_request(request))
            || self
                .namespace_request_receipts
                .get(&key)
                .is_some_and(|receipt| receipt.matches_completion(completion))
    }

    pub(crate) fn missing_namespace_request_completion_op(
        &self,
        completion: Cow<'_, DfsNamespaceRequestRecord>,
    ) -> Option<DfsMetadataTableOp> {
        (!self.namespace_request_completion_is_materialized(&completion))
            .then(|| DfsMetadataTableOp::put_namespace_request(completion.into_owned()))
    }

    pub(crate) fn missing_compact_namespace_request_completion_op(
        &self,
        completion: &DfsNamespaceRequestCompletion,
    ) -> Option<DfsMetadataTableOp> {
        (!self.compact_namespace_request_completion_is_materialized(completion)).then(|| {
            DfsMetadataTableOp::put_namespace_request_receipt(
                DfsNamespaceRequestReceipt::from_completion(completion),
            )
        })
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
            || !self.receipt_withdrawals.is_empty()
            || !self.intents.is_empty()
            || !self.intent_receipts.is_empty()
            || !self.intent_receipt_retention.is_empty()
            || !self.advisory_lock_states.is_empty()
            || !self.namespace_transaction_receipts.is_empty()
            || !self.namespace_jobs.is_empty()
            || !self.namespace_job_receipts.is_empty()
            || !self.namespace_requests.is_empty()
            || !self.namespace_request_receipts.is_empty()
            || !self.compacted_tombstones.is_empty()
            || !self.prepared_operations_v7.is_empty()
            || self.namespace_receipt_retention != DfsNamespaceReceiptRetentionState::default()
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
        #[cfg(test)]
        INTENT_INDEX_KEY_VISITS.with(|count| count.set(count.get() + 1));
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
        #[cfg(test)]
        INTENT_INDEX_KEY_VISITS.with(|count| count.set(count.get() + 1));
        let key = (object_id.clone(), intent_id.to_string());
        if let Some(previous) = self.intents.remove(&key) {
            self.intent_order.remove(&write_intent_scan_key(&previous));
            self.intent_targets
                .remove(&write_intent_target_key(&previous));
        }
    }

    fn namespace_prepare_footprint_indexes_from_history(
        prepared_operations_v7: &BTreeMap<String, DfsPreparedOperationRecord>,
    ) -> (
        BTreeMap<DfsFileSystemId, BTreeMap<DfsObjectId, BTreeSet<String>>>,
        BTreeMap<DfsFileSystemId, BTreeMap<String, BTreeSet<String>>>,
    ) {
        let mut object_owners =
            BTreeMap::<DfsFileSystemId, BTreeMap<DfsObjectId, BTreeSet<String>>>::new();
        let mut path_owners =
            BTreeMap::<DfsFileSystemId, BTreeMap<String, BTreeSet<String>>>::new();
        for prepare in prepared_operations_v7
            .values()
            .filter(|prepare| prepare.status == DfsPreparedOperationStatus::Prepared)
        {
            let (object_ids, paths) = namespace_prepare_footprint(prepare).into_parts();
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

    pub(crate) fn rebuild_prepared_operation_reservations(&mut self) -> Result<()> {
        for record in self.prepared_operations_v7.values() {
            record.validate()?;
        }
        for (operation_id, witness) in &self.prepared_operation_evictions {
            witness.validate()?;
            if operation_id != &witness.digest.operation_id
                || self.prepared_operations_v7.contains_key(operation_id)
            {
                return Err(MetadataBackendError::Serde(
                    "snapshot operation outcome overlaps a full body or changes indexed identity"
                        .into(),
                ));
            }
        }
        self.rebuild_namespace_prepare_footprint_indexes();
        self.rebuild_namespace_prepare_request_index();
        Ok(())
    }

    fn rebuild_namespace_prepare_footprint_indexes(&mut self) {
        let (object_owners, path_owners) =
            Self::namespace_prepare_footprint_indexes_from_history(&self.prepared_operations_v7);
        self.namespace_prepare_object_owners = object_owners;
        self.namespace_prepare_path_owners = path_owners;
    }

    /// Every request a prepare's effects admit or complete: its completed
    /// request attachment, and each request row it puts. A request's row
    /// prepares are its admission, so a retry finds them as it finds the
    /// prepare that completes it.
    fn namespace_prepare_request_keys(
        prepare: &DfsPreparedOperationRecord,
    ) -> BTreeSet<(NodeId, String)> {
        let mut keys = BTreeSet::new();
        if let Some(request) = prepare.completed_request.as_ref() {
            keys.insert((
                request.requester_node_id.clone(),
                request.request_id.clone(),
            ));
        } else if let Some(completion) = prepare.completed_request_completion.as_ref() {
            keys.insert(completion.request_key());
        }
        for effect in &prepare.payload.metadata_effects {
            if let crate::DfsPreparedMetadataEffect::PutNamespaceRequest { request } = effect {
                keys.insert((
                    request.requester_node_id.clone(),
                    request.request_id.clone(),
                ));
            }
        }
        keys
    }

    fn index_namespace_prepare_request(&mut self, prepare: &DfsPreparedOperationRecord) {
        if prepare.status != DfsPreparedOperationStatus::Prepared {
            return;
        }
        for request_key in Self::namespace_prepare_request_keys(prepare) {
            self.namespace_prepare_request_owners
                .entry(request_key)
                .or_default()
                .insert(prepare.operation_id.clone());
        }
    }

    fn namespace_prepare_request_index_from_history(
        prepared_operations_v7: &BTreeMap<String, DfsPreparedOperationRecord>,
    ) -> BTreeMap<(NodeId, String), BTreeSet<String>> {
        let mut request_owners = BTreeMap::<(NodeId, String), BTreeSet<String>>::new();
        for prepare in prepared_operations_v7
            .values()
            .filter(|prepare| prepare.status == DfsPreparedOperationStatus::Prepared)
        {
            for request_key in Self::namespace_prepare_request_keys(prepare) {
                request_owners
                    .entry(request_key)
                    .or_default()
                    .insert(prepare.operation_id.clone());
            }
        }
        request_owners
    }

    fn unindex_namespace_prepare_request(&mut self, prepare: &DfsPreparedOperationRecord) {
        if prepare.status != DfsPreparedOperationStatus::Prepared {
            return;
        }
        for request_key in Self::namespace_prepare_request_keys(prepare) {
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
    }

    /// The live prepares of one request, in journal order: its row
    /// prepares, then the prepare that completes it. A prepare that another
    /// of them supersedes is not live. Two live prepares that would each
    /// complete the request compete, which no admission produces.
    pub(crate) fn prepared_operations_for_request(
        &self,
        requester: &NodeId,
        request_id: &str,
    ) -> Result<Vec<&DfsPreparedOperationRecord>> {
        let key = (requester.clone(), request_id.to_owned());
        let Some(ids) = self.namespace_prepare_request_owners.get(&key) else {
            return Ok(Vec::new());
        };
        let records = ids
            .iter()
            .map(|id| {
                self.prepared_operations_v7.get(id).ok_or_else(|| {
                    MetadataBackendError::Serde(
                        "request reservation indexes a missing prepared body".into(),
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut live = records.iter().copied().filter(|record| !records.iter().any(|replacement| {
            replacement.payload.metadata_effects.iter().any(|effect| matches!(effect,
                crate::DfsPreparedMetadataEffect::ResolvePreparedOperation { digest, status: DfsPreparedOperationStatus::Superseded }
                if digest == &record.digest))
        })).collect::<Vec<_>>();
        if live.is_empty() {
            return Err(MetadataBackendError::Serde(
                "request has cyclic prepared reservations".into(),
            ));
        }
        if live
            .iter()
            .filter(|record| {
                record.completed_request.is_some() || record.completed_request_completion.is_some()
            })
            .count()
            > 1
        {
            return Err(MetadataBackendError::Serde(
                "request has competing prepared completions".into(),
            ));
        }
        live.sort_by_key(|record| record.digest.prepare_sequence);
        Ok(live)
    }

    fn rebuild_namespace_prepare_request_index(&mut self) {
        self.namespace_prepare_request_owners =
            Self::namespace_prepare_request_index_from_history(&self.prepared_operations_v7);
    }

    fn index_namespace_prepare_footprint(
        &mut self,
        prepare: &DfsPreparedOperationRecord,
        footprint: DfsNamespacePrepareFootprint,
    ) {
        if prepare.status != DfsPreparedOperationStatus::Prepared {
            return;
        }
        let (object_ids, paths) = footprint.into_parts();
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

    fn unindex_namespace_prepare_footprint(&mut self, prepare: &DfsPreparedOperationRecord) {
        if prepare.status != DfsPreparedOperationStatus::Prepared {
            return;
        }
        // The payload was shape-validated before this Prepared record entered
        // the table. Walk its borrowed keys directly when releasing ownership;
        // rebuilding an owned footprint here would clone every object ID and
        // retain another pair of large sets on the foreground commit path.
        let object_ids = prepare
            .expected_object_versions
            .iter()
            .map(|expected| expected.object_id.clone())
            .chain(
                prepare
                    .transaction
                    .objects
                    .iter()
                    .map(|object| object.object_id.clone()),
            )
            .chain(
                prepare
                    .transaction
                    .versions
                    .iter()
                    .map(|version| version.object_id.clone()),
            )
            .chain(
                prepare
                    .metadata_effects
                    .iter()
                    .flat_map(|effect| effect.affected_object_ids()),
            )
            .collect::<BTreeSet<_>>();
        for object_id in &object_ids {
            let remove_file_system = self
                .namespace_prepare_object_owners
                .get_mut(&prepare.file_system_id)
                .is_some_and(|objects| {
                    let remove_object = objects.get_mut(object_id).is_some_and(|owners| {
                        owners.remove(&prepare.operation_id);
                        owners.is_empty()
                    });
                    if remove_object {
                        objects.remove(object_id);
                    }
                    objects.is_empty()
                });
            if remove_file_system {
                self.namespace_prepare_object_owners
                    .remove(&prepare.file_system_id);
            }
        }
        for path in prepare
            .expected_path_occupants
            .iter()
            .map(|expected| normalize_path(&expected.path))
            .chain(
                prepare
                    .transaction
                    .objects
                    .iter()
                    .map(|object| normalize_path(&object.path)),
            )
        {
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

    fn put_prepared_operation_indexed_with_footprint(
        &mut self,
        prepare: DfsPreparedOperationRecord,
        footprint: DfsNamespacePrepareFootprint,
    ) {
        let operation_id = prepare.operation_id.clone();
        if let Some(previous) = self.prepared_operations_v7.remove(&operation_id) {
            self.unindex_namespace_prepare_footprint(&previous);
            self.unindex_namespace_prepare_request(&previous);
        }
        self.index_namespace_prepare_footprint(&prepare, footprint);
        self.index_namespace_prepare_request(&prepare);
        self.prepared_operations_v7.insert(operation_id, prepare);
    }

    pub(super) fn put_prepared_operation_indexed(&mut self, prepare: DfsPreparedOperationRecord) {
        if prepare.status != DfsPreparedOperationStatus::Prepared {
            let operation_id = prepare.operation_id.clone();
            if let Some(previous) = self.prepared_operations_v7.remove(&operation_id) {
                self.unindex_namespace_prepare_footprint(&previous);
                self.unindex_namespace_prepare_request(&previous);
            }
            self.prepared_operations_v7.insert(operation_id, prepare);
            return;
        }
        let footprint = namespace_prepare_footprint(&prepare);
        self.put_prepared_operation_indexed_with_footprint(prepare, footprint);
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

    pub(crate) fn prepared_operation_reservations(
        &self,
        payload: &crate::DfsPreparedOperationPayload,
    ) -> Vec<DfsPreparedOperationRecord> {
        let (objects, paths) = prepared_payload_reservation_keys(payload);
        let mut ids = BTreeSet::new();
        if let Some(index) = self
            .namespace_prepare_object_owners
            .get(&payload.file_system_id)
        {
            for object in objects {
                if let Some(owners) = index.get(&object) {
                    ids.extend(owners.iter().cloned());
                }
            }
        }
        if let Some(index) = self
            .namespace_prepare_path_owners
            .get(&payload.file_system_id)
        {
            for path in paths {
                if let Some(owners) = index.get(&path) {
                    ids.extend(owners.iter().cloned());
                }
            }
        }
        ids.into_iter()
            .filter_map(|id| self.prepared_operations_v7.get(&id))
            .filter(|record| {
                record.status == DfsPreparedOperationStatus::Prepared
                    && record.operation_id != payload.operation_id
            })
            .cloned()
            .collect()
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

    pub(crate) fn prepared_operations_for_object(
        &self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
    ) -> Result<Vec<DfsPreparedOperationRecord>> {
        let Some(owners) = self
            .namespace_prepare_object_owners
            .get(file_system_id)
            .and_then(|objects| objects.get(object_id))
        else {
            return Ok(Vec::new());
        };
        owners
            .iter()
            .map(|id| {
                let record = self.prepared_operations_v7.get(id).ok_or_else(|| {
                    MetadataBackendError::Serde(
                        "object reservation indexes a missing prepared body".into(),
                    )
                })?;
                if record.status != DfsPreparedOperationStatus::Prepared
                    || record.file_system_id != *file_system_id
                    || !namespace_prepare_footprint(record)
                        .object_ids()
                        .contains(object_id)
                {
                    return Err(MetadataBackendError::Serde(
                        "object reservation differs from its actual prepared body".into(),
                    ));
                }
                Ok(record.clone())
            })
            .collect()
    }

    pub(crate) fn prepared_content_reference_protection(
        &self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        max_prepares: usize,
    ) -> crate::metadata_error::Result<(bool, bool)> {
        let Some(owners) = self
            .namespace_prepare_object_owners
            .get(file_system_id)
            .and_then(|objects| objects.get(object_id))
        else {
            return Ok((false, true));
        };
        for operation_id in owners.iter().take(max_prepares) {
            let prepare = self
                .prepared_operations_v7
                .get(operation_id)
                .ok_or_else(|| {
                    crate::MetadataBackendError::Serde("prepared reference owner is missing".into())
                })?;
            if prepare.status != DfsPreparedOperationStatus::Prepared
                || prepare.file_system_id != *file_system_id
            {
                return Err(crate::MetadataBackendError::Serde(
                    "prepared reference index is inconsistent".into(),
                ));
            }
            if prepare
                .transaction
                .file_version_pins
                .as_ref()
                .is_some_and(|pins| {
                    pins.add.iter().any(|pin| {
                        pin.file_system_id == *file_system_id
                            && pin.object_id == *object_id
                            && prepare
                                .transaction
                                .versions
                                .iter()
                                .find(|version| {
                                    version.object_id == pin.object_id
                                        && version.version == pin.version
                                })
                                .or_else(|| {
                                    self.versions
                                        .get(&(pin.object_id.clone(), pin.version.clone()))
                                })
                                .is_some_and(|version| {
                                    version.file_system_id == *file_system_id
                                        && version.has_retention_grant()
                                })
                    })
                })
            {
                return Ok((true, true));
            }
        }
        Ok((false, owners.len() <= max_prepares))
    }

    pub(crate) fn prepared_content_reference_owner_count(
        &self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
    ) -> usize {
        self.namespace_prepare_object_owners
            .get(file_system_id)
            .and_then(|objects| objects.get(object_id))
            .map_or(0, BTreeSet::len)
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

    pub(crate) fn apply(&mut self, op: DfsMetadataTableOp) {
        let effect = Self::status_mutation_effect(&op);
        let object_before = match &effect {
            DfsMetadataStatusMutationEffect::Object(object_id) => {
                Some(self.object_status_contribution(object_id))
            }
            _ => None,
        };
        let namespace_job_before = match &effect {
            DfsMetadataStatusMutationEffect::NamespaceJob(job_id) => {
                Some(self.namespace_job_status(job_id))
            }
            _ => None,
        };

        self.apply_unindexed(op);

        match effect {
            DfsMetadataStatusMutationEffect::None => {}
            DfsMetadataStatusMutationEffect::Object(object_id) => {
                let (after, needs) = self.object_status_and_repair_needs(&object_id);
                self.replace_object_status_contribution(
                    object_before.expect("object status mutation captured prior contribution"),
                    after,
                );
                self.repair_index.replace_object(&object_id, needs);
            }

            DfsMetadataStatusMutationEffect::NamespaceJob(job_id) => {
                let before = namespace_job_before
                    .expect("namespace-job status mutation captured prior contribution");
                let after = self.namespace_job_status(&job_id);
                self.replace_namespace_job_status(before, after);
            }
        }
    }

    fn apply_unindexed(&mut self, op: DfsMetadataTableOp) {
        match op {
            DfsMetadataTableOp::PutFileSystem { file_system } => {
                self.file_systems
                    .insert(file_system.file_system_id.clone(), file_system);
            }
            DfsMetadataTableOp::PutObject { object } => {
                let previous = self.objects.get(&object.object_id);
                if previous
                    .is_none_or(|previous| !retained_object_path_is_unchanged(previous, &object))
                {
                    if let Some(previous) = previous {
                        self.retained_revision_index.update_object(previous, false);
                    }
                    self.retained_revision_index.update_object(&object, true);
                }
                self.objects.insert(object.object_id.clone(), object);
            }
            DfsMetadataTableOp::RemoveObject { object_id } => {
                if let Some(previous) = self.objects.get(&object_id) {
                    self.retained_revision_index.update_object(previous, false);
                }
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
            DfsMetadataTableOp::PutVersion { mut version } => {
                if let Some(existing) = self
                    .versions
                    .get(&(version.object_id.clone(), version.version.clone()))
                {
                    for (namespace, old) in &existing.retention {
                        let state = version.retention.entry(namespace.clone()).or_default();
                        state.order = state.order.max(old.order);
                        if state.grant.is_none() {
                            state.grant = old.grant.clone();
                        }
                    }
                    if version.content_durability.is_none() {
                        version.content_durability = existing.content_durability.clone();
                    }
                }
                let key = (version.object_id.clone(), version.version.clone());
                let current = self.objects.get(&version.object_id);
                let previous = self.versions.get(&key);
                if previous
                    .is_none_or(|previous| !retained_version_index_is_unchanged(previous, &version))
                {
                    if let Some(previous) = previous {
                        self.retained_revision_index
                            .update_version(previous, current, false);
                    }
                    self.retained_revision_index
                        .update_version(&version, current, true);
                }
                self.versions.insert(
                    (version.object_id.clone(), version.version.clone()),
                    version,
                );
            }
            DfsMetadataTableOp::RemoveVersion { object_id, version } => {
                for key in self.receipt_withdrawal_keys_for_version(&object_id, &version) {
                    self.receipt_withdrawals.remove(&key);
                }
                if let Some(previous) = self.versions.remove(&(object_id.clone(), version)) {
                    self.retained_revision_index.update_version(
                        &previous,
                        self.objects.get(&object_id),
                        false,
                    );
                }
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
                    let key = (version.object_id.clone(), version.version.clone());
                    let current = self.objects.get(&version.object_id);
                    let previous = self.versions.get(&key);
                    if previous.is_none_or(|previous| {
                        !retained_version_index_is_unchanged(previous, &version)
                    }) {
                        if let Some(previous) = previous {
                            self.retained_revision_index
                                .update_version(previous, current, false);
                        }
                        self.retained_revision_index
                            .update_version(&version, current, true);
                    }
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
                        self.apply(DfsMetadataTableOp::RemoveVersionPin {
                            object_id: pin.object_id,
                            version: pin.version,
                            pin_id: pin.pin_id,
                        });
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
                let key = (
                    receipt.receipt.object_id.clone(),
                    receipt.receipt.committed_version.clone(),
                    receipt.replica_node_id.clone(),
                );
                self.receipt_withdrawals.remove(&key);
                self.receipts.insert(key, receipt);
            }
            DfsMetadataTableOp::RemoveReceipt {
                object_id,
                version,
                replica_node_id,
            } => {
                let key = (object_id, version, replica_node_id);
                let removed = self.receipts.remove(&key);
                if !self.receipt_version_is_live(&key.0, &key.1) {
                    self.receipt_withdrawals.remove(&key);
                } else if let Some(receipt) = removed
                    && receipt.generation > 0
                {
                    // Collecting one row of a live version must not let the
                    // slot's generation go backwards under an older reply.
                    self.receipt_withdrawals.insert(
                        key.clone(),
                        DfsReplicaReceiptWithdrawalRecord {
                            file_system_id: receipt.receipt.file_system_id,
                            object_id: key.0,
                            version: key.1,
                            replica_node_id: key.2,
                            generation: receipt.generation,
                        },
                    );
                }
            }
            DfsMetadataTableOp::WithdrawReceipt { withdrawal } => {
                let key = withdrawal.slot_key();
                self.receipts.remove(&key);
                self.receipt_withdrawals.insert(key, withdrawal);
            }
            DfsMetadataTableOp::PutIntent { intent } => {
                self.intent_receipts
                    .remove(&(intent.object_id.clone(), intent.intent_id.clone()));
                self.put_intent_indexed(intent);
            }
            DfsMetadataTableOp::RemoveRetainedRepairOwner {
                object_id,
                intent_id,
            } => {
                self.remove_intent_indexed(&object_id, &intent_id);
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

            DfsMetadataTableOp::PutAdvisoryLockState { state } => {
                self.advisory_lock_states
                    .insert(state.object_id.clone(), state);
            }

            DfsMetadataTableOp::MergeContentReferenceOutcomeRetention {
                object_id,
                retention,
            } => {
                self.namespace_receipt_retention
                    .content_reference_outcome_retention
                    .entry(object_id)
                    .and_modify(|current| current.merge_conservative(&retention))
                    .or_insert(retention);
            }
            DfsMetadataTableOp::PutNamespaceTransactionReceipt { receipt } => {
                let transaction_id = receipt.transaction_id.clone();
                self.namespace_transaction_receipts
                    .insert(receipt.transaction_id.clone(), receipt);
                self.refresh_content_reference_outcome_index(&transaction_id);
            }
            DfsMetadataTableOp::RemoveNamespaceTransactionReceipt {
                transaction_id,
                retention,
            } => {
                self.namespace_transaction_receipts.remove(&transaction_id);
                self.refresh_content_reference_outcome_index(&transaction_id);
                self.namespace_receipt_retention
                    .merge_conservative(retention);
            }

            DfsMetadataTableOp::RemovePreparedOperation { digest } => {
                self.apply_checked(DfsMetadataTableOp::RemovePreparedOperation { digest })
                    .expect("checked operation eviction");
            }
            DfsMetadataTableOp::ResolvePreparedOperation { digest, status } => {
                self.apply_checked(DfsMetadataTableOp::ResolvePreparedOperation { digest, status })
                    .expect("validated current resolution");
            }
            DfsMetadataTableOp::PutPreparedOperationV7 { record } => {
                self.apply_checked(DfsMetadataTableOp::PutPreparedOperationV7 { record })
                    .expect("native prepared record must pass its exact checked projection");
            }
            DfsMetadataTableOp::CommitPreparedOperationV7 {
                digest,
                committed_entry,
            } => {
                self.apply_checked(DfsMetadataTableOp::CommitPreparedOperationV7 {
                    digest,
                    committed_entry,
                })
                .expect("native digest marker must reference its exact prepared body");
            }

            DfsMetadataTableOp::PutNamespaceJob { job } => {
                self.remove_parent_count_index(&job.job_id);
                self.remove_move_destination_reservation_index(&job.job_id);
                self.namespace_job_receipts.remove(&job.job_id);
                let job_id = job.job_id.clone();
                self.namespace_jobs.insert(job_id.clone(), job);
                self.insert_parent_count_index(&job_id);
                self.insert_move_destination_reservation_index(&job_id);
                self.index_running_namespace_job(&job_id);
            }
            DfsMetadataTableOp::PutNamespaceJobReceipt { receipt } => {
                self.remove_parent_count_index(&receipt.job_id);
                self.remove_move_destination_reservation_index(&receipt.job_id);
                self.namespace_jobs.remove(&receipt.job_id);
                self.index_running_namespace_job(&receipt.job_id);
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
                self.put_namespace_request_indexed(*request);
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
                self.namespace_request_receipts.insert(key, *receipt);
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
    // Rollback state is a membership lookup, never an ordered scan. Hash maps
    // keep a bulk transaction's short-lived undo keys in a few contiguous
    // allocations instead of interleaving thousands of B-tree nodes with the
    // live metadata B-trees. That distinction matters after large replica
    // prepares: dropping the undo log can then return whole allocator spans
    // instead of leaving the resident tables spread across half-empty arenas.
    file_systems: HashMap<DfsFileSystemId, Option<DfsFileSystem>>,
    objects: HashMap<DfsObjectId, Option<DfsObjectMetadata>>,
    file_system_objects: HashMap<(DfsFileSystemId, DfsObjectId), Option<DfsPathRecord>>,
    paths: HashMap<(DfsFileSystemId, String), Option<DfsPathRecord>>,
    inodes: HashMap<DfsInodeId, Option<DfsInodeRecord>>,
    dirfrags: HashMap<(DfsInodeId, DfsDirFragmentId), Option<DfsDirFragmentRecord>>,
    dentries: HashMap<(DfsInodeId, DfsDirFragmentId, u64, String), Option<DfsPathRecord>>,
    hardlinks: HashMap<(DfsObjectId, DfsObjectId), Option<DfsPathRecord>>,
    compacted_tombstones: HashMap<DfsObjectId, Option<DfsObjectMetadata>>,
    compacted_tombstone_paths: HashMap<(DfsFileSystemId, String), Option<DfsObjectId>>,
    versions: HashMap<(DfsObjectId, DfsVersion), Option<DfsObjectVersionRecord>>,
    version_pins: HashMap<(DfsObjectId, DfsVersion, String), Option<DfsMetadataVersionPinRecord>>,
    cross_shard_pin_transfers:
        HashMap<(DfsObjectId, String), Option<DfsCrossShardPinTransferRecord>>,
    receipts: HashMap<(DfsObjectId, DfsVersion, NodeId), Option<DfsReplicaReceiptRecord>>,
    receipt_withdrawals:
        HashMap<(DfsObjectId, DfsVersion, NodeId), Option<DfsReplicaReceiptWithdrawalRecord>>,
    intents: HashMap<(DfsObjectId, String), Option<DfsWriteIntentRecord>>,
    intent_receipts: HashMap<(DfsObjectId, String), Option<DfsWriteIntentReceipt>>,
    intent_receipt_retention: Option<DfsWriteIntentReceiptRetentionState>,
    advisory_lock_states: HashMap<DfsObjectId, Option<DfsAdvisoryLockStateRecord>>,
    namespace_transaction_receipts: HashMap<String, Option<DfsNamespaceTransactionReceipt>>,
    prepared_operations_v7:
        HashMap<String, Option<crate::metadata_records::DfsPreparedOperationRecord>>,
    prepared_operation_evictions: HashMap<String, Option<DfsPreparedOperationEviction>>,
    namespace_jobs: HashMap<String, Option<DfsNamespaceJobRecord>>,
    namespace_job_receipts: HashMap<String, Option<DfsNamespaceJobReceipt>>,
    namespace_requests: HashMap<(NodeId, String), Option<DfsNamespaceRequestRecord>>,
    namespace_request_receipts: HashMap<(NodeId, String), Option<DfsNamespaceRequestReceipt>>,
    namespace_receipt_retention: Option<DfsNamespaceReceiptRetentionState>,
    status_index: Option<DfsMetadataStatusIndex>,
    /// Each touched object's repair needs before its first recorded change.
    repair_index: HashMap<DfsObjectId, Vec<(DfsRepairNeedKey, DfsRepairNeed)>>,
}

impl DfsMetadataTableRollback {
    fn record(&mut self, tables: &DfsMetadataTables, op: &DfsMetadataTableOp) {
        if self.status_index.is_none() {
            self.status_index = Some(tables.status_index.clone());
        }
        if let DfsMetadataStatusMutationEffect::Object(object_id) =
            DfsMetadataTables::status_mutation_effect(op)
            && let std::collections::hash_map::Entry::Vacant(entry) =
                self.repair_index.entry(object_id)
        {
            let entries = tables.repair_index.object_entries(entry.key());
            entry.insert(entries);
        }
        match op {
            DfsMetadataTableOp::RemovePreparedOperation { digest } => {
                record_rollback_value(
                    &mut self.prepared_operation_evictions,
                    &tables.prepared_operation_evictions,
                    digest.operation_id.clone(),
                );
                record_rollback_value(
                    &mut self.prepared_operations_v7,
                    &tables.prepared_operations_v7,
                    digest.operation_id.clone(),
                );
            }
            DfsMetadataTableOp::PutPreparedOperationV7 { record } => record_rollback_value(
                &mut self.prepared_operations_v7,
                &tables.prepared_operations_v7,
                record.digest.operation_id.clone(),
            ),
            DfsMetadataTableOp::CommitPreparedOperationV7 { digest, .. }
            | DfsMetadataTableOp::ResolvePreparedOperation { digest, .. } => record_rollback_value(
                &mut self.prepared_operations_v7,
                &tables.prepared_operations_v7,
                digest.operation_id.clone(),
            ),
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
            DfsMetadataTableOp::RemoveVersion { object_id, version } => {
                record_rollback_value(
                    &mut self.versions,
                    &tables.versions,
                    (object_id.clone(), version.clone()),
                );
                for key in tables.receipt_withdrawal_keys_for_version(object_id, version) {
                    record_rollback_value(
                        &mut self.receipt_withdrawals,
                        &tables.receipt_withdrawals,
                        key,
                    );
                }
            }
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
            DfsMetadataTableOp::PutReceipt { receipt } => self.record_receipt_slot(
                tables,
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
            } => self.record_receipt_slot(
                tables,
                (object_id.clone(), version.clone(), replica_node_id.clone()),
            ),
            DfsMetadataTableOp::WithdrawReceipt { withdrawal } => {
                self.record_receipt_slot(tables, withdrawal.slot_key())
            }
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
            DfsMetadataTableOp::RemoveRetainedRepairOwner {
                object_id,
                intent_id,
            } => {
                record_rollback_value(
                    &mut self.intents,
                    &tables.intents,
                    (object_id.clone(), intent_id.clone()),
                );
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

            DfsMetadataTableOp::PutAdvisoryLockState { state } => record_rollback_value(
                &mut self.advisory_lock_states,
                &tables.advisory_lock_states,
                state.object_id.clone(),
            ),

            DfsMetadataTableOp::MergeContentReferenceOutcomeRetention { .. } => {
                if self.namespace_receipt_retention.is_none() {
                    self.namespace_receipt_retention =
                        Some(tables.namespace_receipt_retention.clone());
                }
            }
            DfsMetadataTableOp::PutNamespaceTransactionReceipt { receipt } => {
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

    fn record_receipt_slot(
        &mut self,
        tables: &DfsMetadataTables,
        key: (DfsObjectId, DfsVersion, NodeId),
    ) {
        record_rollback_value(&mut self.receipts, &tables.receipts, key.clone());
        record_rollback_value(
            &mut self.receipt_withdrawals,
            &tables.receipt_withdrawals,
            key,
        );
    }

    pub(crate) fn new_repair_owners(
        &self,
        tables: &DfsMetadataTables,
    ) -> Vec<DfsWriteIntentRecord> {
        self.intents
            .iter()
            .filter_map(|(key, original)| {
                if original.is_some() || !matches!(self.intent_receipts.get(key), Some(None)) {
                    return None;
                }
                tables
                    .intents
                    .get(key)
                    .filter(|owner| {
                        owner.publication.is_retained_repair()
                            || crate::placement::current_placement_repair_owner_matches_identity(
                                owner,
                            )
                    })
                    .cloned()
            })
            .collect()
    }

    pub(crate) fn rollback(self, tables: &mut DfsMetadataTables) {
        rollback_map(
            &mut tables.prepared_operation_evictions,
            self.prepared_operation_evictions,
        );
        for (operation_id, previous) in self.prepared_operations_v7 {
            match previous {
                Some(record) => {
                    tables.prepared_operations_v7.insert(operation_id, record);
                }
                None => {
                    tables.prepared_operations_v7.remove(&operation_id);
                }
            }
        }
        // Restore only affected candidate rows/families. A failed namespace
        // batch must not turn rollback into a scan of unrelated versions.
        let changed_objects = self.objects.keys().cloned().collect::<Vec<_>>();
        let changed_versions = self.versions.keys().cloned().collect::<Vec<_>>();
        for object_id in &changed_objects {
            if let Some(object) = tables.objects.get(object_id) {
                tables.retained_revision_index.update_object(object, false);
            }
        }
        for key in &changed_versions {
            if let Some(version) = tables.versions.get(key) {
                tables.retained_revision_index.update_version(
                    version,
                    tables.objects.get(&version.object_id),
                    false,
                );
            }
        }
        let changed_reference_outcomes = self
            .namespace_transaction_receipts
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
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
        for key in changed_versions {
            if let Some(version) = tables.versions.get(&key) {
                tables.retained_revision_index.update_version(
                    version,
                    tables.objects.get(&version.object_id),
                    true,
                );
            }
        }
        for object_id in changed_objects {
            if let Some(object) = tables.objects.get(&object_id) {
                tables.retained_revision_index.update_object(object, true);
            }
        }
        rollback_map(&mut tables.version_pins, self.version_pins);
        rollback_map(
            &mut tables.cross_shard_pin_transfers,
            self.cross_shard_pin_transfers,
        );
        rollback_map(&mut tables.receipts, self.receipts);
        rollback_map(&mut tables.receipt_withdrawals, self.receipt_withdrawals);
        // Validation temporarily changes only these recorded keys. Restore
        // their exact old rows and index keys without rescanning unrelated
        // owners for every prepared record replayed from the durable log.
        for ((object_id, intent_id), original) in self.intents {
            match original {
                Some(intent) => tables.put_intent_indexed(intent),
                None => tables.remove_intent_indexed(&object_id, &intent_id),
            }
        }
        rollback_map(&mut tables.intent_receipts, self.intent_receipts);
        if let Some(retention) = self.intent_receipt_retention {
            tables.intent_receipt_retention = retention;
        }
        rollback_map(&mut tables.advisory_lock_states, self.advisory_lock_states);
        rollback_map(
            &mut tables.namespace_transaction_receipts,
            self.namespace_transaction_receipts,
        );
        for transaction_id in changed_reference_outcomes {
            tables.refresh_content_reference_outcome_index(&transaction_id);
        }
        tables.rebuild_namespace_prepare_footprint_indexes();
        tables.rebuild_namespace_prepare_request_index();
        rollback_map(&mut tables.namespace_jobs, self.namespace_jobs);
        tables.rebuild_parent_count_index();
        tables.rebuild_move_destination_reservation_index();
        tables.rebuild_running_namespace_job_index();
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
        if let Some(status_index) = self.status_index {
            tables.status_index = status_index;
        }
        for (object_id, entries) in self.repair_index {
            tables
                .repair_index
                .replace_object(&object_id, entries.into_iter().collect());
        }
    }
}

fn record_rollback_value<K, V>(rollback: &mut HashMap<K, Option<V>>, table: &BTreeMap<K, V>, key: K)
where
    K: Eq + Hash + Ord,
    V: Clone,
{
    if let std::collections::hash_map::Entry::Vacant(entry) = rollback.entry(key) {
        let previous = table.get(entry.key()).cloned();
        entry.insert(previous);
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

fn rollback_map<K, V>(table: &mut BTreeMap<K, V>, rollback: HashMap<K, Option<V>>)
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

#[cfg(test)]
mod rollback_tests {
    use super::*;

    #[test]
    fn rollback_records_only_the_value_before_a_keys_first_mutation() {
        let key = "same-key".to_string();
        let mut table = BTreeMap::from([(key.clone(), 1_u64)]);
        let mut rollback = HashMap::new();

        record_rollback_value(&mut rollback, &table, key.clone());
        table.insert(key.clone(), 2);
        record_rollback_value(&mut rollback, &table, key.clone());
        table.insert(key.clone(), 3);

        rollback_map(&mut table, rollback);
        assert_eq!(table.get(&key), Some(&1));
    }
}

#[cfg(test)]
mod version_metadata_tests {
    use super::*;

    #[test]
    fn current_rows_allow_authoritative_namespace_clear() {
        let value = crate::DfsModuleMetadataValue {
            schema_version: 1,
            payload: serde_json::json!({"opaque": true}),
        };
        let original = crate::DfsObjectVersionRecord {
            file_system_id: crate::DfsFileSystemId::generate(),
            object_id: crate::DfsObjectId::generate(),
            version: crate::DfsVersion::generate(),
            content_hash: Some("blake3:metadata-state".into()),
            size_bytes: Some(1),
            locations: Vec::new(),
            created_by_event_id: None,
            created_at: time::OffsetDateTime::UNIX_EPOCH,
            visible: true,
            retention: BTreeMap::new(),
            module_metadata: BTreeMap::from([
                ("org.example.owner".into(), value.clone()),
                ("org.example.other".into(), value),
            ]),
            content_durability: None,
        };
        let key = (original.object_id.clone(), original.version.clone());
        let mut tables = DfsMetadataTables::default();
        tables.apply(DfsMetadataTableOp::PutVersion {
            version: original.clone(),
        });

        let mut clear = original;
        clear.module_metadata.remove("org.example.owner");
        let encoded = sinomo_core::object::serde_ext::to_bytes(
            &clear,
            7,
            sinomo_core::object::LengthMode::U32,
        )
        .unwrap();
        let decoded = sinomo_core::object::serde_ext::from_bytes(&encoded).unwrap();
        tables.apply(DfsMetadataTableOp::PutVersion { version: decoded });
        assert_eq!(tables.versions[&key].module_metadata, clear.module_metadata);
        assert!(
            tables.versions[&key]
                .module_metadata
                .contains_key("org.example.other")
        );

        clear.module_metadata.clear();
        let encoded = sinomo_core::object::serde_ext::to_bytes(
            &clear,
            7,
            sinomo_core::object::LengthMode::U32,
        )
        .unwrap();
        let decoded = sinomo_core::object::serde_ext::from_bytes(&encoded).unwrap();
        tables.apply(DfsMetadataTableOp::PutVersion { version: decoded });
        assert!(tables.versions[&key].module_metadata.is_empty());
    }
}

#[cfg(test)]
mod repair_birth_selection_tests {
    use super::*;

    // DATA-only selection inputs: these rows do not supply authority or receipts.
    fn placement_owner() -> DfsWriteIntentRecord {
        let node = sinomo_core::node::NodeId::from_legacy_named("birth-selection-owner").unwrap();
        let mut object = crate::posix_object::new_posix_metadata_object(
            &node,
            "/services/birth-selection",
            crate::DfsObjectKind::File,
        );
        object.content_hash = Some("blake3:birth-selection".to_string());
        let intent = crate::DistributedWriteIntent {
            publication: Default::default(),
            intent_id: format!(
                "placement:birth-selection:{}:{}",
                object.object_id, object.version
            ),
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            base_version: Some(object.version.clone()),
            target_version: object.version.clone(),
            requesting_node_id: node.clone(),
            operation_id: None,
            operation_order_token: None,
            owner_epoch: 7,
            desired_replica_nodes: vec![node],
            policy: crate::DfsReplicationPolicy::new(1, 1).unwrap(),
            touch_parent_on_create: false,
            create_predecessor_object_id: None,
            state: crate::DfsWriteIntentState::Committed,
        };
        crate::intent::write_intent_record(&intent, Some("blake3:birth-selection"), Some(object))
    }

    fn newly_touched_owner(
        owner: DfsWriteIntentRecord,
    ) -> (DfsMetadataTables, DfsMetadataTableRollback) {
        let key = (owner.object_id.clone(), owner.intent_id.clone());
        let mut tables = DfsMetadataTables::default();
        tables.intents.insert(key.clone(), owner);
        let mut rollback = DfsMetadataTableRollback::default();
        rollback.intents.insert(key.clone(), None);
        rollback.intent_receipts.insert(key, None);
        (tables, rollback)
    }

    #[test]
    fn current_placement_birth_requires_explicit_new_full_and_compact_owner() {
        let owner = placement_owner();
        let key = (owner.object_id.clone(), owner.intent_id.clone());
        let (tables, mut rollback) = newly_touched_owner(owner.clone());
        assert_eq!(rollback.new_repair_owners(&tables), vec![owner.clone()]);

        rollback.intents.insert(key.clone(), Some(owner.clone()));
        assert!(rollback.new_repair_owners(&tables).is_empty());
        rollback.intents.insert(key.clone(), None);
        rollback.intent_receipts.remove(&key);
        assert!(rollback.new_repair_owners(&tables).is_empty());
        let receipt =
            crate::DfsWriteIntentReceipt::from_intent(&owner, time::OffsetDateTime::UNIX_EPOCH)
                .unwrap();
        rollback.intent_receipts.insert(key.clone(), Some(receipt));
        assert!(rollback.new_repair_owners(&tables).is_empty());
        rollback.intent_receipts.insert(key.clone(), None);
        let mut removed = tables.clone();
        removed.intents.remove(&key);
        assert!(rollback.new_repair_owners(&removed).is_empty());
    }

    #[test]
    fn current_placement_birth_rejects_prepared_or_other_request_identities() {
        let owner = placement_owner();
        let mut rejected = Vec::new();
        let mut prepared = owner.clone();
        prepared.status = crate::DfsWriteIntentStatus::ReceiptsPending;
        rejected.push(prepared);
        let mut aborted = owner.clone();
        aborted.status = crate::DfsWriteIntentStatus::Aborted;
        rejected.push(aborted);
        let mut ordinary = owner.clone();
        ordinary.intent_id = "dfs_write_op_birth_selection".to_string();
        rejected.push(ordinary);
        let mut caller_owned = owner.clone();
        caller_owned.operation_id = Some("caller-operation".to_string());
        rejected.push(caller_owned);
        let mut wrong_base = owner.clone();
        wrong_base.base_version = None;
        rejected.push(wrong_base);
        let mut wrong_object = owner.clone();
        wrong_object.object_id = crate::DfsObjectId::generate();
        rejected.push(wrong_object);
        let mut wrong_version = owner.clone();
        wrong_version.target_version = crate::DfsVersion::generate();
        wrong_version.base_version = Some(wrong_version.target_version.clone());
        rejected.push(wrong_version);

        for changed in rejected {
            let (tables, rollback) = newly_touched_owner(changed);
            assert!(rollback.new_repair_owners(&tables).is_empty());
        }
    }
}

#[cfg(test)]
mod intent_index_rollback_tests {
    use super::*;
    use crate::{DfsReplicationPolicy, DfsWriteIntentStatus};

    // Table-kernel DATA only. These fixtures supply no native publication or
    // content authority; the native prepared-operation tests cover that boundary.
    fn intent(index: i64) -> DfsWriteIntentRecord {
        let owner = NodeId::from_legacy_named("rollback-index-owner").unwrap();
        DfsWriteIntentRecord {
            publication: Default::default(),
            intent_id: format!("rollback-index-intent-{index}"),
            file_system_id: crate::sifs_file_system_id(),
            object_id: DfsObjectId::from_legacy_named(format!("rollback-index-object-{index}"))
                .unwrap(),
            base_version: None,
            target_version: DfsVersion::from_legacy_named(format!(
                "rollback-index-version-{index}"
            ))
            .unwrap(),
            coordinator_node_id: owner.clone(),
            operation_id: Some(format!("rollback-index-operation-{index}")),
            operation_order_token: None,
            owner_epoch: 1,
            desired_replica_node_ids: vec![owner],
            policy: DfsReplicationPolicy::new(1, 1).unwrap(),
            status: DfsWriteIntentStatus::ReceiptsPending,
            content_hash: Some(crate::content_hash(b"rollback index fixture")),
            content_size_bytes: None,
            object_metadata: None,
            touch_parent_on_create: false,
            create_predecessor_object_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(index),
            updated_at: None,
        }
    }

    fn fixture() -> DfsMetadataTables {
        let mut tables = DfsMetadataTables::default();
        for index in 0..512 {
            tables.apply(DfsMetadataTableOp::PutIntent {
                intent: intent(index),
            });
        }
        tables
    }

    fn terminal_receipt(mut original: DfsWriteIntentRecord) -> DfsWriteIntentReceipt {
        original.status = DfsWriteIntentStatus::Committed;
        original.updated_at = Some(OffsetDateTime::UNIX_EPOCH + time::Duration::hours(1));
        DfsWriteIntentReceipt::from_intent(&original, original.updated_at.unwrap()).unwrap()
    }

    #[test]
    fn receipt_validation_rollback_does_not_reindex_unrelated_intents() {
        let mut tables = fixture();
        let before = tables.clone();
        let original = intent(0);
        let receipt = DfsReplicaReceiptRecord {
            receipt: sinomo_core::DfsWriteReceipt {
                file_system_id: original.file_system_id,
                object_id: original.object_id,
                committed_version: original.target_version,
                owner_node_id: original.coordinator_node_id.clone(),
                event_id: sinomo_core::events::EventId::generate(),
            },
            replica_node_id: original.coordinator_node_id,
            content_hash: original.content_hash,
            received_at: OffsetDateTime::UNIX_EPOCH,
            generation: 0,
        };
        for _ in 0..3 {
            let mut undo = DfsMetadataTableRollback::default();
            tables.apply_with_rollback(
                DfsMetadataTableOp::PutReceipt {
                    receipt: receipt.clone(),
                },
                &mut undo,
            );
            INTENT_INDEX_KEY_VISITS.with(|count| count.set(0));
            undo.rollback(&mut tables);
            let visits = INTENT_INDEX_KEY_VISITS.with(Cell::get);
            assert_eq!(
                tables, before,
                "exact rows and every derived index restored"
            );
            assert_eq!(
                visits, 0,
                "receipt-only validation must not scan512 unrelated intent keys"
            );
        }
    }

    #[test]
    fn mixed_intent_rollback_restores_exact_indexes_with_bounded_key_work() {
        let mut tables = fixture();
        tables.apply(DfsMetadataTableOp::PutIntentReceipt {
            receipt: terminal_receipt(intent(2)),
        });
        let before = tables.clone();
        for _ in 0..3 {
            let mut undo = DfsMetadataTableRollback::default();
            // Repeated updates must restore the FIRST prior value, including
            // both independent target and creation-order index keys.
            let mut changed = intent(0);
            for round in 1..=2 {
                changed.created_at += time::Duration::hours(round);
                changed.target_version = DfsVersion::generate();
                tables.apply_with_rollback(
                    DfsMetadataTableOp::PutIntent {
                        intent: changed.clone(),
                    },
                    &mut undo,
                );
            }
            tables.apply_with_rollback(
                DfsMetadataTableOp::PutIntentReceipt {
                    receipt: terminal_receipt(intent(1)),
                },
                &mut undo,
            );
            tables.apply_with_rollback(
                DfsMetadataTableOp::PutIntent {
                    intent: intent(512),
                },
                &mut undo,
            );
            // A rejected tentative reopening must restore its compact terminal
            // outcome, without leaving a full owner or target index behind.
            tables.apply_with_rollback(
                DfsMetadataTableOp::PutIntent { intent: intent(2) },
                &mut undo,
            );
            assert_eq!(undo.intents.len(), 4);
            assert_ne!(tables, before);
            INTENT_INDEX_KEY_VISITS.with(|count| count.set(0));
            undo.rollback(&mut tables);
            let visits = INTENT_INDEX_KEY_VISITS.with(Cell::get);
            assert_eq!(
                tables, before,
                "exact full/compact owners and all indexes restored"
            );
            assert_eq!(
                visits, 4,
                "rollback visits its4 touched keys, not511 resident intents"
            );
        }
    }
}
