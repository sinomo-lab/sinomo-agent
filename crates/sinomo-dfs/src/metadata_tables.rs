//! In-memory materialized metadata tables.
//!
//! Durable table IO lives in `metadata_table_store`; this module owns the
//! table maps, secondary indexes, and rollback bookkeeping used while
//! materializing journal entries.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Deref;
use std::sync::Arc;

use sinomo_core::{Id, node::NodeId};
use time::{Duration, OffsetDateTime};

use crate::advisory_lock::DfsAdvisoryLockStateRecord;
use crate::metadata_keys::{
    dentry_key, intent_key, normalize_path, write_intent_scan_key, write_intent_target_key,
};
use crate::metadata_records::{
    DfsConflictRecord, DfsDirFragmentId, DfsDirFragmentRecord, DfsInodeId, DfsInodeRecord,
    DfsMetadataPrefixProof, DfsMetadataTableOp, DfsNamespaceJobRecord, DfsNamespacePrepareReceipt,
    DfsNamespacePrepareRecord, DfsNamespacePrepareStatus, DfsNamespaceReceiptOrderKey,
    DfsNamespaceReceiptRetentionState, DfsNamespaceRequestReceipt, DfsNamespaceRequestRecord,
    DfsNamespaceRequestStatus, DfsNamespaceTransaction, DfsNamespaceTransactionReceipt,
    DfsObjectVersionRecord, DfsPathRecord, DfsReplicaReceiptRecord, DfsVisibleVersionCommitRecord,
    DfsWriteIntentRecord, normalized_object_metadata,
};
use crate::{
    DfsFileSystem, DfsFileSystemId, DfsObjectId, DfsObjectKind, DfsObjectMetadata, DfsVersion,
};

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
    pub(crate) receipts: BTreeMap<(DfsObjectId, DfsVersion, NodeId), DfsReplicaReceiptRecord>,
    pub(crate) intents: BTreeMap<(DfsObjectId, String), DfsWriteIntentRecord>,
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
    pub(crate) namespace_requests: BTreeMap<(NodeId, String), DfsNamespaceRequestRecord>,
    pub(crate) namespace_request_receipts: BTreeMap<(NodeId, String), DfsNamespaceRequestReceipt>,
    pub(crate) namespace_receipt_retention: DfsNamespaceReceiptRetentionState,
    /// Derived oldest-attempt-first recovery index. Terminal idempotency rows
    /// stay in `namespace_requests` without participating in maintenance scans.
    pub(crate) pending_namespace_request_order: BTreeSet<(OffsetDateTime, NodeId, String)>,
}

pub(crate) const DEFAULT_NAMESPACE_TRANSACTION_RECEIPT_LIMIT: usize = 4_096;
pub(crate) const DEFAULT_NAMESPACE_PREPARE_RECEIPT_LIMIT: usize = 4_096;
pub(crate) const DEFAULT_NAMESPACE_REQUEST_RECEIPT_LIMIT: usize = 4_096;
pub(crate) const DEFAULT_NAMESPACE_RECEIPT_RETENTION_DAYS: i64 = 7;

#[derive(Clone, Copy, Debug)]
pub(crate) struct DfsNamespaceReceiptRetentionLimits {
    pub(crate) max_transaction_receipts: usize,
    pub(crate) max_prepare_receipts: usize,
    pub(crate) max_request_receipts: usize,
    pub(crate) max_age: Duration,
}

impl Default for DfsNamespaceReceiptRetentionLimits {
    fn default() -> Self {
        Self {
            max_transaction_receipts: DEFAULT_NAMESPACE_TRANSACTION_RECEIPT_LIMIT,
            max_prepare_receipts: DEFAULT_NAMESPACE_PREPARE_RECEIPT_LIMIT,
            max_request_receipts: DEFAULT_NAMESPACE_REQUEST_RECEIPT_LIMIT,
            max_age: Duration::days(DEFAULT_NAMESPACE_RECEIPT_RETENTION_DAYS),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct DfsMetadataTableState {
    pub(crate) tables: DfsMetadataTables,
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
    pub(crate) snapshot_fingerprint: Option<MetadataTableSnapshotFingerprint>,
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

fn namespace_request_order_token(request_id: &str) -> Option<String> {
    let candidate = request_id.rsplit('_').next().unwrap_or(request_id);
    Id::new(candidate.to_string())
        .ok()
        .map(|id| id.as_str().to_ascii_lowercase())
}

fn update_order_floor(
    floor: &mut Option<DfsNamespaceReceiptOrderKey>,
    evicted: DfsNamespaceReceiptOrderKey,
) {
    if floor.as_ref().is_none_or(|current| evicted > *current) {
        *floor = Some(evicted);
    }
}

impl DfsMetadataTables {
    pub(crate) fn has_materialized_state(&self) -> bool {
        !self.file_systems.is_empty()
            || !self.objects.is_empty()
            || !self.versions.is_empty()
            || !self.receipts.is_empty()
            || !self.intents.is_empty()
            || !self.conflicts.is_empty()
            || !self.advisory_lock_states.is_empty()
            || !self.namespace_transactions.is_empty()
            || !self.namespace_transaction_receipts.is_empty()
            || !self.namespace_jobs.is_empty()
            || !self.namespace_requests.is_empty()
            || !self.namespace_request_receipts.is_empty()
            || !self.compacted_tombstones.is_empty()
            || !self.visible_commits.is_empty()
            || !self.namespace_prepares.is_empty()
            || !self.namespace_prepare_receipts.is_empty()
            || self.namespace_receipt_retention != DfsNamespaceReceiptRetentionState::default()
    }

    /// Fold terminal namespace history into fixed-size idempotency receipts.
    ///
    /// Callers only persist this image at an exact materialized journal
    /// checkpoint. Pending requests and prepared transactions remain complete
    /// because autonomous recovery still needs their semantic payload.
    pub(crate) fn compact_terminal_namespace_history(
        &mut self,
        checkpoint_time: OffsetDateTime,
        limits: DfsNamespaceReceiptRetentionLimits,
    ) -> Result<(), serde_json::Error> {
        // Build every fallible digest before changing the live tables. The
        // checkpoint can fail without losing the complete recovery records.
        let transaction_receipts = self
            .namespace_transactions
            .values()
            .map(|transaction| {
                DfsNamespaceTransactionReceipt::from_transaction(transaction, checkpoint_time)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let terminal_prepares = self
            .namespace_prepares
            .values()
            .filter_map(|prepare| {
                DfsNamespacePrepareReceipt::from_prepare(prepare, checkpoint_time)
            })
            .collect::<Vec<_>>();
        let completed_requests = self
            .namespace_requests
            .values()
            .filter_map(|request| {
                DfsNamespaceRequestReceipt::from_request(request, checkpoint_time)
            })
            .collect::<Vec<_>>();

        for receipt in transaction_receipts {
            self.namespace_transactions.remove(&receipt.transaction_id);
            self.namespace_transaction_receipts
                .insert(receipt.transaction_id.clone(), receipt);
        }
        for receipt in terminal_prepares {
            self.namespace_prepares.remove(&receipt.operation_id);
            self.namespace_prepare_receipts
                .insert(receipt.operation_id.clone(), receipt);
        }
        for receipt in completed_requests {
            let key = (
                receipt.requester_node_id.clone(),
                receipt.request_id.clone(),
            );
            self.namespace_requests.remove(&key);
            self.namespace_request_receipts.insert(key, receipt);
        }
        self.evict_terminal_namespace_receipts(checkpoint_time, limits);
        self.rebuild_namespace_prepare_footprint_indexes();
        self.rebuild_namespace_prepare_request_index();
        self.rebuild_pending_namespace_request_index();
        Ok(())
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

    pub(crate) fn namespace_request_history_expired(&self, request_id: &str) -> bool {
        match namespace_request_order_token(request_id) {
            Some(token) => self
                .namespace_receipt_retention
                .max_evicted_request_order_token
                .as_ref()
                .is_some_and(|floor| token <= *floor),
            None => {
                self.namespace_receipt_retention
                    .opaque_request_history_evicted
            }
        }
    }

    fn evict_terminal_namespace_receipts(
        &mut self,
        checkpoint_time: OffsetDateTime,
        limits: DfsNamespaceReceiptRetentionLimits,
    ) {
        let cutoff = checkpoint_time.checked_sub(limits.max_age);

        let mut transactions = self
            .namespace_transaction_receipts
            .values()
            .map(|receipt| {
                (
                    (
                        receipt.retained_at,
                        DfsNamespaceReceiptOrderKey {
                            created_at: receipt.created_at,
                            identity: receipt.transaction_id.clone(),
                        },
                    ),
                    receipt.transaction_id.clone(),
                )
            })
            .collect::<Vec<_>>();
        transactions.sort();
        let transaction_age_count = cutoff.map_or(0, |cutoff| {
            transactions.partition_point(|((retained_at, _), _)| *retained_at < cutoff)
        });
        let transaction_excess = transactions
            .len()
            .saturating_sub(limits.max_transaction_receipts);
        for ((_, order_key), transaction_id) in transactions
            .into_iter()
            .take(transaction_age_count.max(transaction_excess))
        {
            self.namespace_transaction_receipts.remove(&transaction_id);
            update_order_floor(
                &mut self.namespace_receipt_retention.transaction_floor,
                order_key,
            );
        }

        let mut prepares = self
            .namespace_prepare_receipts
            .values()
            .map(|receipt| {
                (
                    (
                        receipt.retained_at,
                        DfsNamespaceReceiptOrderKey {
                            created_at: receipt.created_at,
                            identity: receipt.operation_id.clone(),
                        },
                    ),
                    receipt.operation_id.clone(),
                )
            })
            .collect::<Vec<_>>();
        prepares.sort();
        let prepare_age_count = cutoff.map_or(0, |cutoff| {
            prepares.partition_point(|((retained_at, _), _)| *retained_at < cutoff)
        });
        let prepare_excess = prepares.len().saturating_sub(limits.max_prepare_receipts);
        for ((_, order_key), operation_id) in prepares
            .into_iter()
            .take(prepare_age_count.max(prepare_excess))
        {
            self.namespace_prepare_receipts.remove(&operation_id);
            update_order_floor(
                &mut self.namespace_receipt_retention.prepare_floor,
                order_key,
            );
        }

        let mut requests = self
            .namespace_request_receipts
            .iter()
            .map(|(key, receipt)| {
                (
                    (
                        receipt.retained_at,
                        receipt.requester_node_id.clone(),
                        receipt.request_id.clone(),
                    ),
                    key.clone(),
                )
            })
            .collect::<Vec<_>>();
        requests.sort();
        let request_age_count = cutoff.map_or(0, |cutoff| {
            requests.partition_point(|((created_at, _, _), _)| *created_at < cutoff)
        });
        let request_excess = requests.len().saturating_sub(limits.max_request_receipts);
        for (_, key) in requests
            .into_iter()
            .take(request_age_count.max(request_excess))
        {
            if self.namespace_request_receipts.remove(&key).is_some() {
                self.note_evicted_request_id(&key.1);
            }
        }
    }

    fn note_evicted_request_id(&mut self, request_id: &str) {
        let Some(token) = namespace_request_order_token(request_id) else {
            self.namespace_receipt_retention
                .opaque_request_history_evicted = true;
            return;
        };
        let floor = &mut self
            .namespace_receipt_retention
            .max_evicted_request_order_token;
        if floor.as_ref().is_none_or(|current| token > *current) {
            *floor = Some(token);
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

    fn index_namespace_prepare_request(&mut self, prepare: &DfsNamespacePrepareRecord) {
        if prepare.status != DfsNamespacePrepareStatus::Prepared {
            return;
        }
        let Some(request) = prepare.completed_request.as_ref() else {
            return;
        };
        self.namespace_prepare_request_owners
            .entry((
                request.requester_node_id.clone(),
                request.request_id.clone(),
            ))
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
            let Some(request) = prepare.completed_request.as_ref() else {
                continue;
            };
            request_owners
                .entry((
                    request.requester_node_id.clone(),
                    request.request_id.clone(),
                ))
                .or_default()
                .insert(prepare.operation_id.clone());
        }
        request_owners
    }

    fn unindex_namespace_prepare_request(&mut self, prepare: &DfsNamespacePrepareRecord) {
        if prepare.status != DfsNamespacePrepareStatus::Prepared {
            return;
        }
        let Some(request) = prepare.completed_request.as_ref() else {
            return;
        };
        let request_key = (
            request.requester_node_id.clone(),
            request.request_id.clone(),
        );
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
                self.put_intent_indexed(intent);
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
            DfsMetadataTableOp::PutNamespacePrepare { prepare } => {
                self.namespace_prepare_receipts
                    .remove(&prepare.operation_id);
                self.put_namespace_prepare_indexed(prepare);
            }
            DfsMetadataTableOp::PutNamespacePrepareReceipt { receipt } => {
                if let Some(previous) = self.namespace_prepares.remove(&receipt.operation_id) {
                    self.unindex_namespace_prepare_footprint(&previous);
                    self.unindex_namespace_prepare_request(&previous);
                }
                self.namespace_prepare_receipts
                    .insert(receipt.operation_id.clone(), receipt);
            }
            DfsMetadataTableOp::PutNamespaceJob { job } => {
                self.namespace_jobs.insert(job.job_id.clone(), job);
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
    receipts: BTreeMap<(DfsObjectId, DfsVersion, NodeId), Option<DfsReplicaReceiptRecord>>,
    intents: BTreeMap<(DfsObjectId, String), Option<DfsWriteIntentRecord>>,
    conflicts: BTreeMap<(DfsObjectId, String), Option<DfsConflictRecord>>,
    advisory_lock_states: BTreeMap<DfsObjectId, Option<DfsAdvisoryLockStateRecord>>,
    visible_commits: BTreeMap<(DfsObjectId, DfsVersion), Option<DfsVisibleVersionCommitRecord>>,
    namespace_transactions: BTreeMap<String, Option<DfsNamespaceTransaction>>,
    namespace_transaction_receipts: BTreeMap<String, Option<DfsNamespaceTransactionReceipt>>,
    namespace_prepares: BTreeMap<String, Option<DfsNamespacePrepareRecord>>,
    namespace_prepare_receipts: BTreeMap<String, Option<DfsNamespacePrepareReceipt>>,
    namespace_jobs: BTreeMap<String, Option<DfsNamespaceJobRecord>>,
    namespace_requests: BTreeMap<(NodeId, String), Option<DfsNamespaceRequestRecord>>,
    namespace_request_receipts: BTreeMap<(NodeId, String), Option<DfsNamespaceRequestReceipt>>,
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
            DfsMetadataTableOp::PutIntent { intent } => record_rollback_value(
                &mut self.intents,
                &tables.intents,
                (intent.object_id.clone(), intent.intent_id.clone()),
            ),
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
            DfsMetadataTableOp::PutNamespacePrepare { prepare } => {
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
            DfsMetadataTableOp::PutNamespaceJob { job } => record_rollback_value(
                &mut self.namespace_jobs,
                &tables.namespace_jobs,
                job.job_id.clone(),
            ),
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
        rollback_map(&mut tables.receipts, self.receipts);
        rollback_map(&mut tables.intents, self.intents);
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
        rollback_map(&mut tables.namespace_requests, self.namespace_requests);
        rollback_map(
            &mut tables.namespace_request_receipts,
            self.namespace_request_receipts,
        );
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
