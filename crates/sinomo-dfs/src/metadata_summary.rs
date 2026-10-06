//! Read-model summaries derived from materialized metadata tables.
//!
//! The functions here are deliberately pure over `DfsMetadataTables`. Backends
//! decide how to obtain a consistent table snapshot; this module turns that
//! snapshot into user-facing counts and repair status.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound::{Included, Unbounded};

use crate::intent::write_intent_status_is_active;
use crate::metadata_keys::{id_lower_bound, normalize_path};
use crate::metadata_records::{
    DfsConflictStatus, DfsMetadataSummary, DfsNamespaceJobStatus, DfsObjectVersionRecord,
    DfsReplicaReceiptRecord, DfsWriteIntentRecord, DfsWriteIntentStatus,
};
use crate::metadata_tables::DfsMetadataTables;
use crate::{DfsFileSystemId, DfsObjectId, DfsObjectMetadata, DfsVersion};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RepairRoot {
    Permanent,
    Until(u64),
}

impl RepairRoot {
    pub(crate) fn is_live(self, now: u64) -> bool {
        match self {
            Self::Permanent => true,
            Self::Until(deadline) => now < deadline,
        }
    }
}

pub(crate) fn retention_orders_for_versions<'a>(
    versions: impl IntoIterator<Item = &'a DfsObjectVersionRecord>,
) -> BTreeMap<String, u64> {
    let mut orders = BTreeMap::new();
    for version in versions {
        for (namespace, state) in &version.retention {
            let order = orders.entry(namespace.clone()).or_insert(0u64);
            *order = (*order).max(state.order);
        }
    }
    orders
}

pub(crate) fn object_history_root_inputs(
    tables: &DfsMetadataTables,
    object_id: &DfsObjectId,
) -> (BTreeMap<String, u64>, BTreeSet<DfsVersion>) {
    let high = retention_orders_for_versions(
        tables
            .versions
            .range((Included((object_id.clone(), id_lower_bound())), Unbounded))
            .take_while(|((id, _), _)| id == object_id)
            .map(|(_, version)| version),
    );
    let pins = tables
        .version_pins
        .range((
            Included((object_id.clone(), id_lower_bound(), String::new())),
            Unbounded,
        ))
        .take_while(|((id, _, _), _)| id == object_id)
        .map(|((_, version, _), _)| version.clone())
        .collect();
    (high, pins)
}

/// The same immutable bytes can remain current through metadata-only edits.
/// Other versions require an explicit pin or an applicable captured grant.
pub(crate) fn repair_root_for_intent(
    object: &DfsObjectMetadata,
    intent: &DfsWriteIntentRecord,
    version: Option<&DfsObjectVersionRecord>,
    pinned: bool,
    high_water: &BTreeMap<String, u64>,
) -> Option<RepairRoot> {
    if intent.status != DfsWriteIntentStatus::Committed
        || intent.file_system_id != object.file_system_id
        || intent.object_id != object.object_id
        || object.tombstone_provenance.is_some()
    {
        return None;
    }
    if intent.publication.is_current()
        && object.kind != crate::DfsObjectKind::Tombstone
        && (intent.target_version == object.version
            || (intent.content_hash.is_some() && intent.content_hash == object.content_hash))
    {
        return Some(RepairRoot::Permanent);
    }
    let version = version.filter(|version| {
        version.file_system_id == object.file_system_id
            && version.object_id == object.object_id
            && version.version == intent.target_version
            && version.content_hash.is_some()
            && version.content_hash == intent.content_hash
    })?;
    if !version.has_retention_grant() {
        return None;
    }
    if pinned {
        return Some(RepairRoot::Permanent);
    }
    let mut latest_deadline = None;
    for (namespace, state) in &version.retention {
        let Some(grant) = state
            .grant
            .as_ref()
            .and_then(|grant| grant.minimum.as_ref())
        else {
            continue;
        };
        let high = high_water.get(namespace).copied().unwrap_or(grant.order);
        if high.saturating_sub(grant.order) < u64::from(grant.minimum.keep_last) {
            return Some(RepairRoot::Permanent);
        }
        if grant.minimum.keep_for_seconds == 0 {
            continue;
        }
        let Some(deadline) = grant
            .granted_at_unix_nanos
            .checked_add(grant.minimum.keep_for_seconds.saturating_mul(1_000_000_000))
        else {
            return Some(RepairRoot::Permanent);
        };
        latest_deadline = Some(latest_deadline.unwrap_or(0u64).max(deadline));
    }
    latest_deadline.map(RepairRoot::Until)
}

#[cfg(test)]
thread_local! {
    static FULL_METADATA_SUMMARY_SCANS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn reset_full_metadata_summary_scans() {
    FULL_METADATA_SUMMARY_SCANS.with(|scans| scans.set(0));
}

#[cfg(test)]
pub(crate) fn full_metadata_summary_scans() -> usize {
    FULL_METADATA_SUMMARY_SCANS.with(std::cell::Cell::get)
}

pub(crate) fn metadata_summary_from_tables<F, E>(
    tables: &DfsMetadataTables,
    mut object_is_countable: F,
) -> Result<DfsMetadataSummary, E>
where
    F: FnMut(&DfsObjectMetadata) -> Result<bool, E>,
{
    metadata_summary_from_tables_scoped(tables, None, &mut object_is_countable)
}

pub(crate) fn metadata_summary_for_file_system_from_tables<F, E>(
    tables: &DfsMetadataTables,
    file_system_id: &DfsFileSystemId,
    mut object_is_countable: F,
) -> Result<Option<DfsMetadataSummary>, E>
where
    F: FnMut(&DfsObjectMetadata) -> Result<bool, E>,
{
    if !tables.file_systems.contains_key(file_system_id) {
        return Ok(None);
    }
    metadata_summary_from_tables_scoped(tables, Some(file_system_id), &mut object_is_countable)
        .map(Some)
}

fn metadata_summary_from_tables_scoped<F, E>(
    tables: &DfsMetadataTables,
    file_system_scope: Option<&DfsFileSystemId>,
    object_is_countable: &mut F,
) -> Result<DfsMetadataSummary, E>
where
    F: FnMut(&DfsObjectMetadata) -> Result<bool, E>,
{
    #[cfg(test)]
    FULL_METADATA_SUMMARY_SCANS.with(|scans| scans.set(scans.get().saturating_add(1)));
    let file_system_in_scope = |file_system_id: &DfsFileSystemId| {
        file_system_scope.is_none_or(|scope| file_system_id == scope)
    };
    let mut object_count = 0usize;
    for ((file_system_id, _), path_record) in &tables.file_system_objects {
        if !file_system_in_scope(file_system_id)
            || !tables.file_systems.contains_key(file_system_id)
            || path_record.file_system_id != *file_system_id
        {
            continue;
        }
        let Some(object) = tables.objects.get(&path_record.object_id) else {
            continue;
        };
        if object.file_system_id == *file_system_id
            && normalize_path(&object.path) == path_record.path
            && object_is_countable(object)?
        {
            object_count = object_count.saturating_add(1);
        }
    }

    let now = crate::dfs_now_unix_nanos();
    let mut history_inputs = BTreeMap::new();
    let mut pending_repair_count = 0usize;
    for intent in tables.intents.values() {
        if intent.status != DfsWriteIntentStatus::Committed {
            continue;
        }
        if !file_system_in_scope(&intent.file_system_id) {
            continue;
        }
        let Some(object) = tables
            .objects
            .get(&intent.object_id)
            .or_else(|| tables.compacted_tombstones.get(&intent.object_id))
        else {
            continue;
        };
        if !file_system_in_scope(&object.file_system_id)
            || !tables.file_systems.contains_key(&object.file_system_id)
            || !object_is_countable(object)?
        {
            continue;
        }
        let (high, pins) = history_inputs
            .entry(object.object_id.clone())
            .or_insert_with(|| object_history_root_inputs(tables, &object.object_id));
        if !repair_root_for_intent(
            object,
            intent,
            tables
                .versions
                .get(&(object.object_id.clone(), intent.target_version.clone())),
            pins.contains(&intent.target_version),
            high,
        )
        .is_some_and(|root| root.is_live(now))
        {
            continue;
        }
        let receipts = receipts_for_object_version_from_tables(
            tables,
            &object.object_id,
            &intent.target_version,
        );
        pending_repair_count =
            pending_repair_count.saturating_add(missing_replica_count(intent, &receipts));
    }

    Ok(DfsMetadataSummary {
        file_system_count: match file_system_scope {
            Some(file_system_id) if tables.file_systems.contains_key(file_system_id) => 1,
            Some(_) => 0,
            None => tables.file_systems.len(),
        },
        object_count,
        version_count: tables
            .versions
            .values()
            .filter(|version| file_system_in_scope(&version.file_system_id))
            .count(),
        receipt_count: tables
            .receipts
            .values()
            .filter(|receipt| file_system_in_scope(&receipt.receipt.file_system_id))
            .count(),
        intent_count: tables
            .intents
            .values()
            .filter(|intent| file_system_in_scope(&intent.file_system_id))
            .count(),
        pending_write_count: tables
            .intents
            .values()
            .filter(|intent| {
                file_system_in_scope(&intent.file_system_id)
                    && write_intent_status_is_active(&intent.status)
            })
            .count(),
        open_conflict_count: tables
            .conflicts
            .values()
            .filter(|conflict| {
                file_system_in_scope(&conflict.file_system_id)
                    && conflict.status == DfsConflictStatus::Open
            })
            .count(),
        pending_repair_count,
        pending_namespace_job_count: tables
            .namespace_jobs
            .values()
            .filter(|job| {
                file_system_in_scope(&job.file_system_id)
                    && job.status == DfsNamespaceJobStatus::Running
            })
            .count(),
        failed_namespace_job_count: tables
            .namespace_jobs
            .values()
            .filter(|job| {
                file_system_in_scope(&job.file_system_id)
                    && job.status == DfsNamespaceJobStatus::Failed
            })
            .count(),
        cross_shard_namespace_lock_count: tables
            .namespace_jobs
            .values()
            .filter(|job| {
                file_system_in_scope(&job.file_system_id)
                    && job.status == DfsNamespaceJobStatus::Running
                    && job.kind.holds_cross_shard_namespace_lock_while_running()
            })
            .map(|job| (job.file_system_id.clone(), job.root_path.clone()))
            .collect::<BTreeSet<_>>()
            .len(),
    })
}

fn receipts_for_object_version_from_tables(
    tables: &DfsMetadataTables,
    object_id: &DfsObjectId,
    version: &DfsVersion,
) -> Vec<DfsReplicaReceiptRecord> {
    tables
        .receipts
        .range((
            Included((object_id.clone(), version.clone(), id_lower_bound())),
            Unbounded,
        ))
        .take_while(|((record_object_id, record_version, _), _)| {
            record_object_id == object_id && record_version == version
        })
        .map(|(_, receipt)| receipt.clone())
        .collect()
}

pub(crate) fn count_pending_repairs_for_object(
    object: &DfsObjectMetadata,
    intents: &[DfsWriteIntentRecord],
    receipts_by_version: &BTreeMap<DfsVersion, Vec<DfsReplicaReceiptRecord>>,
    versions: &BTreeMap<DfsVersion, DfsObjectVersionRecord>,
    pinned_versions: &BTreeSet<DfsVersion>,
) -> usize {
    let high = retention_orders_for_versions(versions.values());
    let now = crate::dfs_now_unix_nanos();
    let mut count = 0usize;
    for intent in intents {
        if !repair_root_for_intent(
            object,
            intent,
            versions.get(&intent.target_version),
            pinned_versions.contains(&intent.target_version),
            &high,
        )
        .is_some_and(|root| root.is_live(now))
        {
            continue;
        }
        count = count.saturating_add(missing_replica_count(
            intent,
            receipts_by_version
                .get(&intent.target_version)
                .map(Vec::as_slice)
                .unwrap_or_default(),
        ));
    }
    count
}

fn missing_replica_count(
    intent: &DfsWriteIntentRecord,
    receipts: &[DfsReplicaReceiptRecord],
) -> usize {
    let nodes = receipts
        .iter()
        .filter(|receipt| {
            intent
                .content_hash
                .as_ref()
                .is_none_or(|hash| receipt.content_hash.as_ref() == Some(hash))
        })
        .map(|receipt| &receipt.replica_node_id)
        .collect::<BTreeSet<_>>();
    intent
        .desired_replica_node_ids
        .iter()
        .filter(|node| !nodes.contains(node))
        .count()
}

pub(crate) fn accumulate_metadata_summary_detail_counts(
    summary: &mut DfsMetadataSummary,
    detail: &DfsMetadataSummary,
) {
    summary.object_count = summary.object_count.saturating_add(detail.object_count);
    summary.version_count = summary.version_count.saturating_add(detail.version_count);
    summary.receipt_count = summary.receipt_count.saturating_add(detail.receipt_count);
    summary.intent_count = summary.intent_count.saturating_add(detail.intent_count);
    summary.pending_write_count = summary
        .pending_write_count
        .saturating_add(detail.pending_write_count);
    summary.open_conflict_count = summary
        .open_conflict_count
        .saturating_add(detail.open_conflict_count);
    summary.pending_repair_count = summary
        .pending_repair_count
        .saturating_add(detail.pending_repair_count);
    summary.pending_namespace_job_count = summary
        .pending_namespace_job_count
        .saturating_add(detail.pending_namespace_job_count);
    summary.failed_namespace_job_count = summary
        .failed_namespace_job_count
        .saturating_add(detail.failed_namespace_job_count);
    summary.cross_shard_namespace_lock_count = summary
        .cross_shard_namespace_lock_count
        .saturating_add(detail.cross_shard_namespace_lock_count);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_keys::path_record_for_object;
    use crate::metadata_records::{
        DfsConflictRecord, DfsConflictStatus, DfsInodeRecord, DfsMetadataTableOp,
        DfsMetadataTableSnapshot, DfsNamespaceJobKind, DfsNamespaceJobRecord,
        DfsNamespaceJobStatus, DfsObjectVersionRecord,
    };
    use crate::metadata_table_store::{
        LEGACY_METADATA_TABLE_SNAPSHOT_FORMAT_VERSION, METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
        metadata_tables_from_table_snapshot, metadata_tables_snapshot,
    };
    use crate::metadata_tables::DfsMetadataTableRollback;
    use crate::{DfsFileSystem, DfsObjectKind};
    use sinomo_core::events::EventId;
    use sinomo_core::node::NodeId;
    use sinomo_core::{DfsObjectPolicy, DfsWriteReceipt};
    use time::OffsetDateTime;

    fn node(value: &str) -> NodeId {
        NodeId::from_legacy_named(value).expect("valid node id")
    }

    fn file_system_id() -> DfsFileSystemId {
        DfsFileSystemId::from_legacy_named("fs_summary").expect("valid filesystem id")
    }

    fn object_id() -> DfsObjectId {
        DfsObjectId::from_legacy_named("obj_summary").expect("valid object id")
    }

    fn version(value: &str) -> DfsVersion {
        DfsVersion::from_legacy_named(value).expect("valid version")
    }

    fn file_system() -> DfsFileSystem {
        DfsFileSystem {
            metadata_format: crate::DfsRevisionMetadataFormat::NamespacedV6,
            retention_policies: Default::default(),
            module_metadata: Default::default(),
            file_system_id: file_system_id(),
            display_name: "summary".to_string(),
            owner_node_id: node("node_owner"),
            subtree_policies: Vec::new(),
        }
    }

    fn object() -> DfsObjectMetadata {
        DfsObjectMetadata {
            object_id: object_id(),
            file_system_id: file_system_id(),
            path: "/docs/readme.md".to_string(),
            kind: DfsObjectKind::File,
            tombstone_provenance: None,
            owner_node_id: node("node_owner"),
            current_locations: vec![node("node_owner")],
            content_hash: Some("blake3:summary".to_string()),
            content_size_bytes: Some(7),
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version: version("version_summary_1"),
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        }
    }

    fn version_record(object: &DfsObjectMetadata) -> DfsObjectVersionRecord {
        DfsObjectVersionRecord {
            metadata_format: crate::DfsRevisionMetadataFormat::NamespacedV6,
            content_durability: None,
            retention: Default::default(),
            module_metadata: Default::default(),
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            version: object.version.clone(),
            content_hash: object.content_hash.clone(),
            size_bytes: object.content_size_bytes,
            locations: object.current_locations.clone(),
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        }
    }

    fn intent(
        object: &DfsObjectMetadata,
        intent_id: &str,
        status: DfsWriteIntentStatus,
    ) -> DfsWriteIntentRecord {
        DfsWriteIntentRecord {
            publication: Default::default(),
            intent_id: intent_id.to_string(),
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            base_version: None,
            target_version: object.version.clone(),
            coordinator_node_id: node("node_owner"),
            operation_id: None,
            operation_order_token: None,
            owner_epoch: 0,
            desired_replica_node_ids: vec![node("node_owner"), node("node_replica")],
            policy: crate::DfsReplicationPolicy::confirmed(2).expect("valid policy"),
            status,
            content_hash: object.content_hash.clone(),
            object_metadata: Some(object.clone()),
            touch_parent_on_create: false,
            create_predecessor_object_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: Some(OffsetDateTime::UNIX_EPOCH),
        }
    }

    fn receipt(object: &DfsObjectMetadata, replica_node_id: NodeId) -> DfsReplicaReceiptRecord {
        DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: object.file_system_id.clone(),
                object_id: object.object_id.clone(),
                committed_version: object.version.clone(),
                owner_node_id: object.owner_node_id.clone(),
                event_id: EventId::from_legacy_named("evt_summary_receipt").expect("valid event"),
            },
            replica_node_id,
            content_hash: object.content_hash.clone(),
            received_at: OffsetDateTime::UNIX_EPOCH,
        }
    }

    fn assert_status_projection_matches_scan(tables: &DfsMetadataTables, context: &str) {
        let scanned =
            metadata_summary_from_tables::<_, std::convert::Infallible>(tables, |_| Ok(true))
                .expect("scanned summary");
        assert_eq!(
            tables.metadata_status_summary(),
            scanned,
            "incremental status projection diverged after {context}"
        );
    }

    #[test]
    fn retained_repair_status_expires_without_writes_and_survives_pin_rollback() {
        let historical = object();
        let mut current = historical.clone();
        current.version = version("history-status-replacement");
        current.content_hash = Some("blake3:replacement".into());
        let captured_at = crate::dfs_now_unix_nanos();
        let deadline = captured_at + 60_000_000_000;
        let mut captured = version_record(&historical);
        captured.retention.insert(
            "vendor.status".into(),
            crate::DfsRevisionRetentionState {
                order: 1,
                grant: Some(Box::new(crate::DfsRevisionRetentionGrant {
                    object: Box::new(historical.clone()),
                    granted_at_unix_nanos: captured_at,
                    origin: crate::DfsRetentionGrantOrigin::Rule,
                    minimum: Some(crate::DfsOrderedRetentionGrant {
                        order: 1,
                        granted_at_unix_nanos: captured_at,
                        minimum: crate::DfsRetentionMinimum {
                            keep_last: 0,
                            keep_for_seconds: 60,
                        },
                    }),
                })),
            },
        );
        let mut tables = DfsMetadataTables::default();
        for op in [
            DfsMetadataTableOp::PutFileSystem {
                file_system: file_system(),
            },
            DfsMetadataTableOp::PutObject {
                object: current.clone(),
            },
            DfsMetadataTableOp::PutFileSystemObject {
                path_record: path_record_for_object(&current),
            },
            DfsMetadataTableOp::PutVersion { version: captured },
            DfsMetadataTableOp::PutIntent {
                intent: intent(
                    &historical,
                    "history-status",
                    DfsWriteIntentStatus::Committed,
                ),
            },
            DfsMetadataTableOp::PutReceipt {
                receipt: receipt(&historical, node("node_owner")),
            },
        ] {
            tables.apply(op);
            assert_status_projection_matches_scan(&tables, "age-retained repair setup");
        }
        assert_eq!(
            tables
                .metadata_status_summary_at(deadline - 1)
                .pending_repair_count,
            1
        );
        assert_eq!(
            tables
                .metadata_status_summary_at(deadline)
                .pending_repair_count,
            0
        );

        let mut rollback = DfsMetadataTableRollback::default();
        tables.apply_with_rollback(
            DfsMetadataTableOp::PutVersionPin {
                pin: crate::DfsMetadataVersionPinRecord {
                    namespace: None,
                    file_system_id: historical.file_system_id.clone(),
                    object_id: historical.object_id.clone(),
                    version: historical.version.clone(),
                    pin_id: "checkpoint".into(),
                    created_at: OffsetDateTime::UNIX_EPOCH,
                },
            },
            &mut rollback,
        );
        assert_eq!(
            tables
                .metadata_status_summary_at(deadline)
                .pending_repair_count,
            1
        );
        rollback.rollback(&mut tables);
        assert_eq!(
            tables
                .metadata_status_summary_at(deadline - 1)
                .pending_repair_count,
            1
        );
        assert_eq!(
            tables
                .metadata_status_summary_at(deadline)
                .pending_repair_count,
            0
        );
        assert_status_projection_matches_scan(&tables, "pin rollback");

        let restored = metadata_tables_from_table_snapshot(
            DfsMetadataTableSnapshot {
                format_version: METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
                materialized_through_sequence: None,
                tables: Some(Box::new(metadata_tables_snapshot(&tables))),
                ops: Vec::new(),
                semantic_state_fingerprint: None,
                prefix_proof: None,
            },
            "retained repair expiry snapshot",
        )
        .expect("restore expiry index");
        assert_eq!(
            restored
                .metadata_status_summary_at(deadline - 1)
                .pending_repair_count,
            1
        );
        assert_eq!(
            restored
                .metadata_status_summary_at(deadline)
                .pending_repair_count,
            0
        );
        tables.apply(DfsMetadataTableOp::PutReceipt {
            receipt: receipt(&historical, node("node_replica")),
        });
        assert_eq!(
            tables
                .metadata_status_summary_at(deadline - 1)
                .pending_repair_count,
            0
        );
        assert_status_projection_matches_scan(&tables, "retained repair completion");
    }

    #[test]
    fn summary_counts_pending_writes_and_missing_repairs_from_tables() {
        let file_system = file_system();
        let object = object();
        let path_record = path_record_for_object(&object);
        let mut tables = DfsMetadataTables::default();
        for op in [
            DfsMetadataTableOp::PutFileSystem { file_system },
            DfsMetadataTableOp::PutObject {
                object: object.clone(),
            },
            DfsMetadataTableOp::PutFileSystemObject {
                path_record: path_record.clone(),
            },
            DfsMetadataTableOp::PutPath { path_record },
            DfsMetadataTableOp::PutInode {
                inode: DfsInodeRecord::from(&object),
            },
            DfsMetadataTableOp::PutVersion {
                version: version_record(&object),
            },
            DfsMetadataTableOp::PutIntent {
                intent: intent(&object, "intent_staging", DfsWriteIntentStatus::Staging),
            },
            DfsMetadataTableOp::PutIntent {
                intent: intent(&object, "intent_committed", DfsWriteIntentStatus::Committed),
            },
            DfsMetadataTableOp::PutReceipt {
                receipt: receipt(&object, node("node_owner")),
            },
        ] {
            tables.apply(op);
            assert_status_projection_matches_scan(&tables, "base table operation");
        }

        let summary =
            metadata_summary_from_tables::<_, std::convert::Infallible>(&tables, |_| Ok(true))
                .expect("summary");

        assert_eq!(summary.file_system_count, 1);
        assert_eq!(summary.object_count, 1);
        assert_eq!(summary.version_count, 1);
        assert_eq!(summary.receipt_count, 1);
        assert_eq!(summary.intent_count, 2);
        assert_eq!(summary.pending_write_count, 1);
        assert_eq!(summary.pending_repair_count, 1);
    }

    #[test]
    fn incremental_status_projection_survives_transitions_snapshots_replay_and_rollback() {
        let file_system = file_system();
        let object = object();
        let path_record = path_record_for_object(&object);
        let open_conflict = DfsConflictRecord {
            conflict_id: "conflict-summary-status".to_string(),
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            path: object.path.clone(),
            base_version: None,
            local_version: object.version.clone(),
            remote_version: version("version_summary_remote"),
            detected_by_node_id: node("node_owner"),
            reason: "projection test".to_string(),
            local_content_hash: object.content_hash.clone(),
            remote_content_hash: Some("blake3:remote".to_string()),
            created_by_intent_id: None,
            strategy: None,
            status: DfsConflictStatus::Open,
            detected_at: OffsetDateTime::UNIX_EPOCH,
        };
        let running_job = DfsNamespaceJobRecord {
            job_id: "job-summary-status".to_string(),
            file_system_id: object.file_system_id.clone(),
            origin: None,
            kind: DfsNamespaceJobKind::RenameObjectAcrossShardPrimaries,
            status: DfsNamespaceJobStatus::Running,
            root_path: "/docs".to_string(),
            target_path: Some("/archive/docs".to_string()),
            pending_frames: Vec::new(),
            processed_object_count: 0,
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: OffsetDateTime::UNIX_EPOCH,
            completed_at: None,
            failure_reason: None,
        };
        let base_ops = vec![
            DfsMetadataTableOp::PutFileSystem { file_system },
            DfsMetadataTableOp::PutObject {
                object: object.clone(),
            },
            DfsMetadataTableOp::PutFileSystemObject { path_record },
            DfsMetadataTableOp::PutVersion {
                version: version_record(&object),
            },
            DfsMetadataTableOp::PutIntent {
                intent: intent(
                    &object,
                    "intent_status_projection",
                    DfsWriteIntentStatus::Committed,
                ),
            },
            DfsMetadataTableOp::PutReceipt {
                receipt: receipt(&object, node("node_owner")),
            },
            DfsMetadataTableOp::PutConflict {
                conflict: open_conflict.clone(),
            },
            DfsMetadataTableOp::PutNamespaceJob {
                job: running_job.clone(),
            },
        ];
        let mut tables = DfsMetadataTables::default();
        for op in base_ops.iter().cloned() {
            tables.apply(op);
            assert_status_projection_matches_scan(&tables, "counter-bearing base replay");
        }
        assert_eq!(tables.metadata_status_summary().pending_repair_count, 1);
        assert_eq!(tables.metadata_status_summary().open_conflict_count, 1);
        assert_eq!(
            tables.metadata_status_summary().pending_namespace_job_count,
            1
        );
        assert_eq!(
            tables
                .metadata_status_summary()
                .cross_shard_namespace_lock_count,
            1
        );

        let structured = metadata_tables_from_table_snapshot(
            DfsMetadataTableSnapshot {
                format_version: METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
                materialized_through_sequence: None,
                tables: Some(Box::new(metadata_tables_snapshot(&tables))),
                ops: Vec::new(),
                semantic_state_fingerprint: None,
                prefix_proof: None,
            },
            "status projection structured test snapshot",
        )
        .expect("restore structured snapshot");
        assert_eq!(
            structured.metadata_status_summary(),
            tables.metadata_status_summary()
        );
        assert_status_projection_matches_scan(&structured, "structured snapshot restore");

        let replayed = metadata_tables_from_table_snapshot(
            DfsMetadataTableSnapshot {
                format_version: LEGACY_METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
                materialized_through_sequence: None,
                tables: None,
                ops: base_ops,
                semantic_state_fingerprint: None,
                prefix_proof: None,
            },
            "status projection replay test snapshot",
        )
        .expect("restore replay snapshot");
        assert_eq!(
            replayed.metadata_status_summary(),
            tables.metadata_status_summary()
        );
        assert_status_projection_matches_scan(&replayed, "legacy operation replay");

        tables.apply(DfsMetadataTableOp::PutIntent {
            intent: intent(
                &object,
                "intent_status_projection",
                DfsWriteIntentStatus::ReceiptsPending,
            ),
        });
        assert_status_projection_matches_scan(&tables, "active intent transition");
        assert_eq!(tables.metadata_status_summary().pending_write_count, 1);
        assert_eq!(tables.metadata_status_summary().pending_repair_count, 0);

        tables.apply(DfsMetadataTableOp::PutIntent {
            intent: intent(
                &object,
                "intent_status_projection",
                DfsWriteIntentStatus::Committed,
            ),
        });
        tables.apply(DfsMetadataTableOp::PutReceipt {
            receipt: receipt(&object, node("node_replica")),
        });
        assert_status_projection_matches_scan(&tables, "repair receipt completion");
        assert_eq!(tables.metadata_status_summary().pending_repair_count, 0);

        let mut resolved_conflict = open_conflict.clone();
        resolved_conflict.status = DfsConflictStatus::Resolved;
        tables.apply(DfsMetadataTableOp::PutConflict {
            conflict: resolved_conflict,
        });
        let mut failed_job = running_job.clone();
        failed_job.status = DfsNamespaceJobStatus::Failed;
        failed_job.failure_reason = Some("projection test failure".to_string());
        tables.apply(DfsMetadataTableOp::PutNamespaceJob {
            job: failed_job.clone(),
        });
        assert_status_projection_matches_scan(&tables, "conflict and job terminal transitions");
        assert_eq!(tables.metadata_status_summary().open_conflict_count, 0);
        assert_eq!(
            tables.metadata_status_summary().pending_namespace_job_count,
            0
        );
        assert_eq!(
            tables.metadata_status_summary().failed_namespace_job_count,
            1
        );
        assert_eq!(
            tables
                .metadata_status_summary()
                .cross_shard_namespace_lock_count,
            0
        );

        tables.apply(DfsMetadataTableOp::PutNamespaceJobReceipt {
            receipt: crate::DfsNamespaceJobReceipt::from_job(
                &failed_job,
                OffsetDateTime::UNIX_EPOCH,
            )
            .expect("terminal job receipt"),
        });
        assert_status_projection_matches_scan(&tables, "terminal job collection");
        assert_eq!(
            tables.metadata_status_summary().failed_namespace_job_count,
            0
        );

        let before_rollback = tables.metadata_status_summary();
        let mut rollback = DfsMetadataTableRollback::default();
        tables.apply_with_rollback(
            DfsMetadataTableOp::PutConflict {
                conflict: open_conflict,
            },
            &mut rollback,
        );
        tables.apply_with_rollback(
            DfsMetadataTableOp::PutNamespaceJob { job: running_job },
            &mut rollback,
        );
        assert_ne!(tables.metadata_status_summary(), before_rollback);
        rollback.rollback(&mut tables);
        assert_eq!(tables.metadata_status_summary(), before_rollback);
        assert_status_projection_matches_scan(&tables, "table rollback");
    }

    #[test]
    fn cross_shard_lock_projection_deduplicates_effective_namespace_lock() {
        let file_system = file_system();
        let file_system_id = file_system.file_system_id.clone();
        let running_job = DfsNamespaceJobRecord {
            job_id: "job-shared-lock-a".to_string(),
            file_system_id,
            origin: None,
            kind: DfsNamespaceJobKind::RenameObjectAcrossShardPrimaries,
            status: DfsNamespaceJobStatus::Running,
            root_path: "/shared-lock".to_string(),
            target_path: Some("/target/shared-lock".to_string()),
            pending_frames: Vec::new(),
            processed_object_count: 0,
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: OffsetDateTime::UNIX_EPOCH,
            completed_at: None,
            failure_reason: None,
        };
        let mut second_job = running_job.clone();
        second_job.job_id = "job-shared-lock-b".to_string();
        let mut tables = DfsMetadataTables::default();
        tables.apply(DfsMetadataTableOp::PutFileSystem { file_system });
        tables.apply(DfsMetadataTableOp::PutNamespaceJob {
            job: running_job.clone(),
        });
        tables.apply(DfsMetadataTableOp::PutNamespaceJob {
            job: second_job.clone(),
        });
        assert_status_projection_matches_scan(&tables, "duplicate effective namespace locks");
        assert_eq!(
            tables.metadata_status_summary().pending_namespace_job_count,
            2
        );
        assert_eq!(
            tables
                .metadata_status_summary()
                .cross_shard_namespace_lock_count,
            1,
            "two durable job rows for one filesystem/root lock represent one effective lock"
        );

        let mut restored = metadata_tables_from_table_snapshot(
            DfsMetadataTableSnapshot {
                format_version: METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
                materialized_through_sequence: None,
                tables: Some(Box::new(metadata_tables_snapshot(&tables))),
                ops: Vec::new(),
                semantic_state_fingerprint: None,
                prefix_proof: None,
            },
            "deduplicated lock projection snapshot",
        )
        .expect("restore deduplicated lock projection");
        assert_status_projection_matches_scan(&restored, "deduplicated lock snapshot restore");
        assert_eq!(
            restored
                .metadata_status_summary()
                .cross_shard_namespace_lock_count,
            1
        );

        let mut failed_job = running_job;
        failed_job.status = DfsNamespaceJobStatus::Failed;
        failed_job.failure_reason = Some("test terminal transition".to_string());
        restored.apply(DfsMetadataTableOp::PutNamespaceJob { job: failed_job });
        assert_status_projection_matches_scan(&restored, "first shared-lock job terminal");
        assert_eq!(
            restored
                .metadata_status_summary()
                .cross_shard_namespace_lock_count,
            1,
            "the second running job still retains the effective lock"
        );

        second_job.status = DfsNamespaceJobStatus::Completed;
        second_job.completed_at = Some(OffsetDateTime::UNIX_EPOCH);
        restored.apply(DfsMetadataTableOp::PutNamespaceJob { job: second_job });
        assert_status_projection_matches_scan(&restored, "last shared-lock job terminal");
        assert_eq!(
            restored
                .metadata_status_summary()
                .cross_shard_namespace_lock_count,
            0
        );
    }

    #[test]
    fn scoped_summary_honors_object_filter_for_visible_counts_and_repairs() {
        let file_system = file_system();
        let object = object();
        let path_record = path_record_for_object(&object);
        let mut tables = DfsMetadataTables::default();
        for op in [
            DfsMetadataTableOp::PutFileSystem { file_system },
            DfsMetadataTableOp::PutObject {
                object: object.clone(),
            },
            DfsMetadataTableOp::PutFileSystemObject { path_record },
            DfsMetadataTableOp::PutIntent {
                intent: intent(&object, "intent_committed", DfsWriteIntentStatus::Committed),
            },
        ] {
            tables.apply(op);
        }

        let summary = metadata_summary_for_file_system_from_tables::<_, std::convert::Infallible>(
            &tables,
            &file_system_id(),
            |_| Ok(false),
        )
        .expect("summary")
        .expect("filesystem summary");

        assert_eq!(summary.file_system_count, 1);
        assert_eq!(summary.object_count, 0);
        assert_eq!(summary.pending_repair_count, 0);
        assert_eq!(summary.intent_count, 1);
    }
}
