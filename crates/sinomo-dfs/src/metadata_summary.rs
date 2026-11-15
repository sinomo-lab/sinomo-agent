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
    DfsMetadataSummary, DfsNamespaceJobStatus, DfsObjectVersionRecord, DfsReplicaReceiptRecord,
    DfsWriteIntentRecord, DfsWriteIntentStatus,
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
    current_version: Option<&DfsObjectVersionRecord>,
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
    if intent.publication.is_current() {
        let matched = crate::content_durability::current_repair_content_matches(
            object,
            intent,
            version,
            current_version,
        );
        let superseded = crate::content_durability::current_repair_content_is_superseded(
            object,
            intent,
            version,
            current_version,
        );
        // Keep the validated full legacy owner's recorded N contract when a
        // missing lineage row cannot establish either role. Runtime execution
        // reports unavailable until it can prove its actual current root.
        return (matched || !superseded).then_some(RepairRoot::Permanent);
    }
    let Some(version) = version else {
        return Some(RepairRoot::Permanent);
    };
    if !crate::content_durability::retained_repair_content_matches(object, intent, version)
        && version.content_durability.is_some()
    {
        return None;
    }
    retained_version_repair_root(object, version, pinned, high_water)
}

/// Captured Version DATA already owns the named durability goal, even between
/// a head change and enrollment of its actual retained maintenance intent.
/// This projection never creates or impersonates such an intent.
pub(crate) fn retained_version_repair_root(
    object: &DfsObjectMetadata,
    version: &DfsObjectVersionRecord,
    pinned: bool,
    high_water: &BTreeMap<String, u64>,
) -> Option<RepairRoot> {
    if object.tombstone_provenance.is_some()
        || version.file_system_id != object.file_system_id
        || version.object_id != object.object_id
        || version.content_hash.is_none()
    {
        return None;
    }
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
    let mut pending_repair_count = 0usize;
    for object in tables.objects.values().chain(
        tables
            .compacted_tombstones
            .values()
            .filter(|object| !tables.objects.contains_key(&object.object_id)),
    ) {
        if !file_system_in_scope(&object.file_system_id)
            || !tables.file_systems.contains_key(&object.file_system_id)
            || !object_is_countable(object)?
        {
            continue;
        }
        let versions = tables
            .versions
            .range((
                Included((object.object_id.clone(), id_lower_bound())),
                Unbounded,
            ))
            .take_while(|((id, _), _)| id == &object.object_id)
            .map(|(_, record)| record);
        let (_, pins) = object_history_root_inputs(tables, &object.object_id);
        let intents = tables
            .intents
            .range((
                Included((object.object_id.clone(), String::new())),
                Unbounded,
            ))
            .take_while(|((id, _), _)| id == &object.object_id)
            .map(|(_, record)| record);
        let receipts = tables
            .receipts
            .range((
                Included((object.object_id.clone(), id_lower_bound(), id_lower_bound())),
                Unbounded,
            ))
            .take_while(|((id, _, _), _)| id == &object.object_id)
            .map(|(_, record)| record);
        pending_repair_count = pending_repair_count.saturating_add(
            repair_demands_for_object(object, intents, receipts, versions, &pins)
                .values()
                .filter(|demand| demand.root.is_live(now))
                .count(),
        );
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

/// A missing replica subject is unique independently of how many actual
/// maintenance records refer to it. The enclosing call fixes filesystem/object.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct RepairDemandKey {
    pub(crate) version: DfsVersion,
    pub(crate) content_hash: String,
    pub(crate) replica_node_id: sinomo_core::node::NodeId,
}

#[derive(Clone, Debug)]
pub(crate) struct RepairDemand {
    pub(crate) root: RepairRoot,
    /// None denotes real Version demand without an eligible full owner. It is
    /// never exposed as a fabricated durable intent identity.
    pub(crate) owner_intent_id: Option<String>,
    owner_root: Option<RepairRoot>,
}

impl RepairDemand {
    /// Root of the selected owner intent, which covers this demand's root.
    pub(crate) fn owner_root(&self) -> Option<RepairRoot> {
        self.owner_root
    }
}

fn repair_root_covers(owner: RepairRoot, demand: RepairRoot) -> bool {
    match (owner, demand) {
        (RepairRoot::Permanent, _) => true,
        (RepairRoot::Until(_), RepairRoot::Permanent) => false,
        (RepairRoot::Until(owner), RepairRoot::Until(demand)) => owner >= demand,
    }
}

fn merge_repair_root(left: RepairRoot, right: RepairRoot) -> RepairRoot {
    match (left, right) {
        (RepairRoot::Permanent, _) | (_, RepairRoot::Permanent) => RepairRoot::Permanent,
        (RepairRoot::Until(left), RepairRoot::Until(right)) => RepairRoot::Until(left.max(right)),
    }
}

/// One metadata cut supplies every input. This only projects missing named N
/// demand; existing W/source, current-primary, grant, and permit checks remain
/// mandatory before any actual owner enrollment or content transfer.
pub(crate) fn repair_demands_for_object<'a>(
    object: &DfsObjectMetadata,
    intents: impl IntoIterator<Item = &'a DfsWriteIntentRecord>,
    receipts: impl IntoIterator<Item = &'a DfsReplicaReceiptRecord>,
    versions: impl IntoIterator<Item = &'a DfsObjectVersionRecord>,
    pinned_versions: &BTreeSet<DfsVersion>,
) -> BTreeMap<RepairDemandKey, RepairDemand> {
    repair_demands_for_object_observed_at(
        object,
        intents,
        receipts,
        versions,
        pinned_versions,
        Some(crate::dfs_now_unix_nanos()),
    )
}

/// `observed_at: None` selects owners as if every root were live, so the
/// result depends only on metadata rows. The derived repair index uses this
/// form and leaves the liveness of a root to the executor at dispatch time.
pub(crate) fn repair_demands_for_object_observed_at<'a>(
    object: &DfsObjectMetadata,
    intents: impl IntoIterator<Item = &'a DfsWriteIntentRecord>,
    receipts: impl IntoIterator<Item = &'a DfsReplicaReceiptRecord>,
    versions: impl IntoIterator<Item = &'a DfsObjectVersionRecord>,
    pinned_versions: &BTreeSet<DfsVersion>,
    observed_at: Option<u64>,
) -> BTreeMap<RepairDemandKey, RepairDemand> {
    let versions = versions
        .into_iter()
        .map(|version| (version.version.clone(), version))
        .collect::<BTreeMap<_, _>>();
    let high = retention_orders_for_versions(versions.values().copied());
    let receipts = receipts
        .into_iter()
        .filter(|receipt| {
            receipt.receipt.file_system_id == object.file_system_id
                && receipt.receipt.object_id == object.object_id
        })
        .filter_map(|receipt| {
            Some(RepairDemandKey {
                version: receipt.receipt.committed_version.clone(),
                content_hash: receipt.content_hash.clone()?,
                replica_node_id: receipt.replica_node_id.clone(),
            })
        })
        .collect::<BTreeSet<_>>();
    let current = versions.get(&object.version).copied();
    let mut demands = BTreeMap::<RepairDemandKey, RepairDemand>::new();
    let mut add_plan = |version: &DfsVersion,
                        hash: &str,
                        nodes: &[sinomo_core::node::NodeId],
                        root: RepairRoot,
                        owner: Option<&str>| {
        for node in nodes {
            let key = RepairDemandKey {
                version: version.clone(),
                content_hash: hash.to_owned(),
                replica_node_id: node.clone(),
            };
            if receipts.contains(&key) {
                continue;
            }
            let entry = demands.entry(key).or_insert_with(|| RepairDemand {
                root,
                owner_intent_id: None,
                owner_root: None,
            });
            entry.root = merge_repair_root(entry.root, root);
            // A historical expired owner cannot win lexically over a live
            // Current root. Prefer the real owner with the strongest own root;
            // ties are deterministic. The merged lifetime is checked below.
            if let Some(owner) =
                owner.filter(|_| observed_at.is_none_or(|observed_at| root.is_live(observed_at)))
            {
                let replace = entry.owner_root.is_none_or(|old_root| {
                    !repair_root_covers(old_root, root)
                        || (repair_root_covers(root, old_root)
                            && entry
                                .owner_intent_id
                                .as_deref()
                                .is_none_or(|old| owner < old))
                });
                if replace {
                    entry.owner_intent_id = Some(owner.to_owned());
                    entry.owner_root = Some(root);
                }
            }
        }
    };
    for intent in intents {
        let target = versions.get(&intent.target_version).copied();
        let Some(root) = repair_root_for_intent(
            object,
            intent,
            target,
            current,
            pinned_versions.contains(&intent.target_version),
            &high,
        ) else {
            continue;
        };
        let Some(hash) = intent.content_hash.as_deref() else {
            continue;
        };
        add_plan(
            &intent.target_version,
            hash,
            &intent.desired_replica_node_ids,
            root,
            Some(&intent.intent_id),
        );
    }
    for version in versions.values() {
        let Some(root) = retained_version_repair_root(
            object,
            version,
            pinned_versions.contains(&version.version),
            &high,
        ) else {
            continue;
        };
        let Some(plan) = version.content_durability.as_deref() else {
            continue;
        };
        if crate::distributed::validate_replication_plan(
            &plan.desired_replica_node_ids,
            &plan.policy,
        )
        .is_err()
        {
            continue;
        }
        let Some(hash) = version.content_hash.as_deref() else {
            continue;
        };
        add_plan(
            &version.version,
            hash,
            &plan.desired_replica_node_ids,
            root,
            None,
        );
    }
    for demand in demands.values_mut() {
        if !demand
            .owner_root
            .is_some_and(|owner| repair_root_covers(owner, demand.root))
        {
            demand.owner_intent_id = None;
            demand.owner_root = None;
        }
    }
    demands
}

pub(crate) fn count_pending_repairs_for_object(
    object: &DfsObjectMetadata,
    intents: &[DfsWriteIntentRecord],
    receipts_by_version: &BTreeMap<DfsVersion, Vec<DfsReplicaReceiptRecord>>,
    versions: &BTreeMap<DfsVersion, DfsObjectVersionRecord>,
    pinned_versions: &BTreeSet<DfsVersion>,
) -> usize {
    let now = crate::dfs_now_unix_nanos();
    repair_demands_for_object(
        object,
        intents,
        receipts_by_version.values().flatten(),
        versions.values(),
        pinned_versions,
    )
    .values()
    .filter(|demand| demand.root.is_live(now))
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
        DfsInodeRecord, DfsMetadataTableOp, DfsMetadataTableSnapshot, DfsNamespaceJobKind,
        DfsNamespaceJobRecord, DfsNamespaceJobStatus, DfsObjectVersionRecord,
    };
    use crate::metadata_table_store::{
        METADATA_TABLE_SNAPSHOT_FORMAT_VERSION, metadata_tables_from_table_snapshot,
        metadata_tables_snapshot,
    };
    use crate::metadata_tables::DfsMetadataTableRollback;
    use crate::{DfsFileSystem, DfsObjectKind};
    use sinomo_core::events::EventId;
    use sinomo_core::node::NodeId;
    use sinomo_core::{DfsObjectPolicy, DfsWriteReceipt};
    use time::OffsetDateTime;

    fn current_snapshot(tables: &DfsMetadataTables) -> DfsMetadataTableSnapshot {
        let mut snapshot = DfsMetadataTableSnapshot {
            format_version: METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
            materialized_through_sequence: 0,
            tables: Box::new(metadata_tables_snapshot(tables)),
            semantic_state_fingerprint: String::new(),
            prefix_proof: crate::metadata_records::dfs_metadata_prefix_proof_genesis(
                &crate::DfsMetadataShardId::new("summary-test"),
            ),
        };
        snapshot.semantic_state_fingerprint =
            crate::metadata_records::dfs_metadata_table_snapshot_state_fingerprint(&snapshot)
                .unwrap();
        snapshot
    }

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
            catalog_generation: 0,
            metadata_storage: None,
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
            namespace_link_count: (DfsObjectKind::File).initial_namespace_link_count(),
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
            generation: 0,
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
        crate::metadata_tables::assert_repair_index_matches_rows(tables, context);
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
        captured.content_durability = Some(crate::content_durability::from_intent(
            &crate::intent::distributed_intent_from_record(&intent(
                &historical,
                "history-status-source",
                DfsWriteIntentStatus::Committed,
            )),
        ));
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
            DfsMetadataTableOp::PutReceipt {
                receipt: receipt(&historical, node("node_owner")),
            },
        ] {
            tables.apply(op);
            assert_status_projection_matches_scan(&tables, "age-retained repair setup");
        }
        // Projection DATA only: the captured version owns N demand before a
        // separate native executor is enrolled. No intent is synthesized here.
        assert!(tables.intents.is_empty());
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
                    namespace: "module.dfs.history".into(),
                    file_system_id: historical.file_system_id.clone(),
                    object_id: historical.object_id.clone(),
                    version: historical.version.clone(),
                    pin_id: crate::content_reference_storage_id("module.dfs.history", "checkpoint"),
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
            current_snapshot(&tables),
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
    fn full_legacy_named_demand_survives_missing_versions_and_durability() {
        // Table DATA only. A valid full legacy W1/N2 contract with one exact
        // receipt cannot disappear because optional lineage rows are absent.
        let original = object();
        let mut current = original.clone();
        current.version = version("legacy-metadata-successor");
        let mut owner = intent(
            &original,
            "legacy-full-owner",
            DfsWriteIntentStatus::Committed,
        );
        owner.policy = crate::DfsReplicationPolicy::new(1, 2).unwrap();
        let mut tables = DfsMetadataTables::default();
        for operation in [
            DfsMetadataTableOp::PutFileSystem {
                file_system: file_system(),
            },
            DfsMetadataTableOp::PutObject {
                object: current.clone(),
            },
            DfsMetadataTableOp::PutIntent {
                intent: owner.clone(),
            },
            DfsMetadataTableOp::PutReceipt {
                receipt: receipt(&original, node("node_owner")),
            },
        ] {
            tables.apply(operation);
        }
        assert!(!crate::content_durability::current_repair_content_matches(
            &current, &owner, None, None,
        ));
        assert!(
            !crate::content_durability::current_repair_content_is_superseded(
                &current, &owner, None, None,
            )
        );
        assert_eq!(tables.metadata_status_summary().pending_repair_count, 1);
        assert_status_projection_matches_scan(&tables, "missing legacy lineage rows");
        for object in [&original, &current] {
            let version = version_record(object);
            assert!(version.content_durability.is_none());
            tables.apply(DfsMetadataTableOp::PutVersion { version });
            assert_eq!(tables.metadata_status_summary().pending_repair_count, 1);
            assert_status_projection_matches_scan(&tables, "legacy version without durability");
        }
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

        let mut snapshot = DfsMetadataTableSnapshot {
            format_version: METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
            materialized_through_sequence: 0,
            tables: Box::new(metadata_tables_snapshot(&tables)),
            semantic_state_fingerprint: String::new(),
            prefix_proof: crate::metadata_records::dfs_metadata_prefix_proof_genesis(
                &crate::DfsMetadataShardId::new("summary-test"),
            ),
        };
        snapshot.semantic_state_fingerprint =
            crate::metadata_records::dfs_metadata_table_snapshot_state_fingerprint(&snapshot)
                .unwrap();
        let structured =
            metadata_tables_from_table_snapshot(snapshot, "status projection current snapshot")
                .expect("restore current snapshot");
        assert_eq!(
            structured.metadata_status_summary(),
            tables.metadata_status_summary()
        );
        assert_status_projection_matches_scan(&structured, "current snapshot restore");

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

        let mut failed_job = running_job.clone();
        failed_job.status = DfsNamespaceJobStatus::Failed;
        failed_job.failure_reason = Some("projection test failure".to_string());
        tables.apply(DfsMetadataTableOp::PutNamespaceJob {
            job: failed_job.clone(),
        });
        assert_status_projection_matches_scan(&tables, "conflict and job terminal transitions");
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
            current_snapshot(&tables),
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
