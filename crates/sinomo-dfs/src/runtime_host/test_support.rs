use super::*;

#[derive(Clone, Debug)]
#[cfg(test)]
pub(super) enum DaemonDfsMetadataLookup<T> {
    Found(T),
    Missing,
    Unavailable,
}

#[cfg(test)]
pub(super) trait DaemonDfsMetadataLookupSource {
    fn object_lookup(&self, object_id: &DfsObjectId) -> DaemonDfsMetadataLookup<DfsObjectMetadata>;

    fn version_lookup(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
    ) -> DaemonDfsMetadataLookup<DfsObjectVersionRecord>;
}

#[cfg(test)]
impl<T> DaemonDfsMetadataLookupSource for T
where
    T: DfsMetadataBackend + ?Sized,
{
    fn object_lookup(&self, object_id: &DfsObjectId) -> DaemonDfsMetadataLookup<DfsObjectMetadata> {
        match self.get_object(object_id) {
            Ok(Some(object)) => DaemonDfsMetadataLookup::Found(object),
            Ok(None) => DaemonDfsMetadataLookup::Missing,
            Err(_) => DaemonDfsMetadataLookup::Unavailable,
        }
    }

    fn version_lookup(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
    ) -> DaemonDfsMetadataLookup<DfsObjectVersionRecord> {
        match self.get_version(object_id, version) {
            Ok(Some(version)) => DaemonDfsMetadataLookup::Found(version),
            Ok(None) => DaemonDfsMetadataLookup::Missing,
            Err(_) => DaemonDfsMetadataLookup::Unavailable,
        }
    }
}

#[derive(Debug, Default)]
#[cfg(test)]
pub(super) struct DaemonDfsMetadataLookups {
    pub(super) objects: BTreeMap<DfsObjectId, DaemonDfsMetadataLookup<DfsObjectMetadata>>,
    pub(super) versions:
        BTreeMap<(DfsObjectId, DfsVersion), DaemonDfsMetadataLookup<DfsObjectVersionRecord>>,
}

#[cfg(test)]
impl DaemonDfsMetadataLookupSource for DaemonDfsMetadataLookups {
    fn object_lookup(&self, object_id: &DfsObjectId) -> DaemonDfsMetadataLookup<DfsObjectMetadata> {
        self.objects
            .get(object_id)
            .cloned()
            .unwrap_or(DaemonDfsMetadataLookup::Unavailable)
    }

    fn version_lookup(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
    ) -> DaemonDfsMetadataLookup<DfsObjectVersionRecord> {
        self.versions
            .get(&(object_id.clone(), version.clone()))
            .cloned()
            .unwrap_or(DaemonDfsMetadataLookup::Unavailable)
    }
}

#[cfg(test)]
pub(super) fn daemon_dfs_intents_without_recovery_source<M>(
    data_dir: &SinomoDataDir,
    metadata: &M,
    write_intents: &[DfsWriteIntentRecord],
    receipt_records: &[DfsReplicaReceiptRecord],
    offline_node_ids: &BTreeSet<NodeId>,
    local_node_id: &NodeId,
) -> BTreeSet<String>
where
    M: DaemonDfsMetadataLookupSource + ?Sized,
{
    if !write_intents.iter().any(|intent| {
        matches!(
            intent.status,
            DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
        )
    }) {
        return BTreeSet::new();
    }
    let content = LocalContentStore::open_for_inspection(data_dir.dfs_dir().join("content"));
    let mut receipts_by_version = BTreeMap::<
        (
            &DfsFileSystemId,
            &sinomo_dfs::DfsObjectId,
            &sinomo_dfs::DfsVersion,
        ),
        Vec<&DfsReplicaReceiptRecord>,
    >::new();
    for record in receipt_records {
        receipts_by_version
            .entry((
                &record.receipt.file_system_id,
                &record.receipt.object_id,
                &record.receipt.committed_version,
            ))
            .or_default()
            .push(record);
    }
    write_intents
        .iter()
        .filter(|intent| {
            matches!(
                intent.status,
                DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
            )
        })
        .filter(|intent| {
            let is_invisible_create = match metadata.object_lookup(&intent.object_id) {
                DaemonDfsMetadataLookup::Found(object)
                    if object.file_system_id == intent.file_system_id
                        && object.version == intent.target_version =>
                {
                    // The target version is already visible. Hydration either
                    // marks the stale intent committed or reconciles an exact
                    // hash disagreement; neither path needs a content source.
                    return false;
                }
                DaemonDfsMetadataLookup::Found(object)
                    if object.file_system_id == intent.file_system_id =>
                {
                    false
                }
                DaemonDfsMetadataLookup::Found(_) => return false,
                DaemonDfsMetadataLookup::Missing => true,
                // Storage inspection is advisory scheduling state. Fail open
                // rather than hiding work behind an ambiguous metadata error.
                DaemonDfsMetadataLookup::Unavailable => return false,
            };
            let Some(content_hash) = intent.content_hash.as_ref() else {
                // Hydration cannot reconstruct a pending write without either
                // an already-visible exact target (handled above) or a content
                // identity. Keep the durable intent for periodic reconciliation
                // without selecting the short maintenance cadence.
                return true;
            };
            let has_local_storage_candidate = match content.object_version_storage_candidate_exists(
                intent.object_id.as_str(),
                intent.target_version.as_str(),
                content_hash,
            ) {
                Ok(available) => available,
                // Invalid metadata or any local I/O ambiguity must keep the
                // normal fail-open maintenance cadence.
                Err(_) => return false,
            };
            let has_durable_size_hint =
                match metadata.version_lookup(&intent.object_id, &intent.target_version) {
                    DaemonDfsMetadataLookup::Found(version)
                        if version.content_hash.as_ref() == Some(content_hash)
                            && version.size_bytes.is_some() =>
                    {
                        true
                    }
                    DaemonDfsMetadataLookup::Found(_) | DaemonDfsMetadataLookup::Missing => false,
                    DaemonDfsMetadataLookup::Unavailable => return false,
                };
            let matching_receipt_node_ids = receipts_by_version
                .get(&(
                    &intent.file_system_id,
                    &intent.object_id,
                    &intent.target_version,
                ))
                .into_iter()
                .flatten()
                .filter(|record| {
                    record.content_hash.as_ref() == Some(content_hash)
                        && intent
                            .desired_replica_node_ids
                            .contains(&record.replica_node_id)
                })
                .map(|record| &record.replica_node_id)
                .collect::<BTreeSet<_>>();
            let commit_ready = matching_receipt_node_ids.len() >= intent.policy.min_commit_replicas;
            if is_invisible_create {
                let has_valid_object_metadata =
                    intent.object_metadata.as_ref().is_some_and(|object| {
                        object.object_id == intent.object_id
                            && object.file_system_id == intent.file_system_id
                    });
                let has_local_manifest_size_hint = match content
                    .read_manifest(intent.object_id.as_str(), intent.target_version.as_str())
                {
                    Ok(manifest)
                        if manifest.object_id == intent.object_id.as_str()
                            && manifest.version == intent.target_version.as_str()
                            && manifest.content_hash == *content_hash =>
                    {
                        true
                    }
                    Ok(_) => return false,
                    Err(ContentStoreError::Io { source, .. })
                        if source.kind() == std::io::ErrorKind::NotFound =>
                    {
                        false
                    }
                    Err(_) => return false,
                };
                // Hydration deliberately skips an invisible create until it
                // has all three ingredients needed by commit promotion: the
                // durable create metadata, receipt quorum, and an exact size
                // hint backed by a version record or matching manifest. A raw
                // whole-content blob without a manifest is not a size hint.
                return !(has_valid_object_metadata
                    && commit_ready
                    && (has_durable_size_hint || has_local_manifest_size_hint));
            }
            if has_local_storage_candidate || commit_ready {
                // A visible base object lets hydration reconstruct the pending
                // update with a zero size fallback. Local content can continue
                // replica staging below quorum; receipt quorum can commit even
                // when every receipt holder is currently offline.
                return false;
            }
            if matching_receipt_node_ids
                .iter()
                .any(|node_id| *node_id != local_node_id && !offline_node_ids.contains(*node_id))
            {
                // A visible base object lets hydration reconstruct the pending
                // update before repair selects this reachable exact-version
                // source. Invisible creates returned above because hydration
                // cannot build that pending write without a local size hint.
                return false;
            }
            true
        })
        .map(|intent| intent.intent_id.clone())
        .collect()
}

#[cfg(test)]
pub(super) fn daemon_dfs_repairs_without_recovery_source<M>(
    data_dir: &SinomoDataDir,
    metadata: &M,
    repair_views: &[DfsOperatorRepairView],
    receipt_records: &[DfsReplicaReceiptRecord],
    offline_node_ids: &BTreeSet<NodeId>,
    local_node_id: &NodeId,
) -> BTreeSet<(String, NodeId)>
where
    M: DaemonDfsMetadataLookupSource + ?Sized,
{
    if repair_views.is_empty() {
        return BTreeSet::new();
    }
    let content = LocalContentStore::open_for_inspection(data_dir.dfs_dir().join("content"));
    let mut receipts_by_version = BTreeMap::<
        (
            &DfsFileSystemId,
            &sinomo_dfs::DfsObjectId,
            &sinomo_dfs::DfsVersion,
        ),
        Vec<&DfsReplicaReceiptRecord>,
    >::new();
    for record in receipt_records {
        receipts_by_version
            .entry((
                &record.receipt.file_system_id,
                &record.receipt.object_id,
                &record.receipt.committed_version,
            ))
            .or_default()
            .push(record);
    }
    repair_views
        .iter()
        .filter(|repair| {
            let Some(content_hash) = repair.content_hash.as_ref() else {
                return false;
            };
            match content.object_version_storage_candidate_exists(
                repair.object_id.as_str(),
                repair.version.as_str(),
                content_hash,
            ) {
                Ok(true) => return false,
                Ok(false) => {}
                // Advisory source inspection must remain fail-open.
                Err(_) => return false,
            }
            let has_reachable_receipt = receipts_by_version
                .get(&(&repair.file_system_id, &repair.object_id, &repair.version))
                .into_iter()
                .flatten()
                .any(|record| {
                    record.content_hash.as_ref() == Some(content_hash)
                        && &record.replica_node_id != local_node_id
                        && record.replica_node_id != repair.missing_replica_node_id
                        && !offline_node_ids.contains(&record.replica_node_id)
                });
            if has_reachable_receipt {
                return false;
            }
            match metadata.object_lookup(&repair.object_id) {
                DaemonDfsMetadataLookup::Found(object)
                    if object.file_system_id == repair.file_system_id
                        && object.version == repair.version
                        && object.content_hash.as_ref() == Some(content_hash) =>
                {
                    !object.current_locations.iter().any(|node_id| {
                        node_id != local_node_id
                            && node_id != &repair.missing_replica_node_id
                            && !offline_node_ids.contains(node_id)
                    })
                }
                // The repair view may have become stale between the operator
                // scan and this lookup. Let maintenance reconcile/remove it
                // instead of classifying the stale record as source-less.
                DaemonDfsMetadataLookup::Found(_) | DaemonDfsMetadataLookup::Missing => false,
                DaemonDfsMetadataLookup::Unavailable => false,
            }
        })
        .map(|repair| {
            (
                repair.intent_id.clone(),
                repair.missing_replica_node_id.clone(),
            )
        })
        .collect()
}

#[cfg(test)]
pub(super) fn daemon_dfs_actionable_conflict_count(
    metadata: &dyn DfsMetadataBackend,
    conflicts: &[DfsConflictRecord],
) -> Result<usize, sinomo_dfs::MetadataBackendError> {
    let mut actionable = 0usize;
    for conflict in conflicts
        .iter()
        .filter(|conflict| conflict.status == DfsConflictStatus::Open)
    {
        let current = metadata.get_object(&conflict.object_id)?;
        let requires_maintenance =
            daemon_dfs_conflict_requires_maintenance(conflict, current.as_ref());
        actionable = actionable.saturating_add(usize::from(requires_maintenance));
    }
    Ok(actionable)
}

#[cfg(test)]
pub(super) fn daemon_dfs_conflict_requires_maintenance(
    conflict: &DfsConflictRecord,
    current: Option<&DfsObjectMetadata>,
) -> bool {
    match current {
        // These are the exact cases that
        // `reconcile_stale_conflicts_bounded` can advance: a deleted object
        // supersedes the conflict, either branch becoming visible resolves
        // it, and any version beyond the recorded base supersedes it. A
        // conflict whose base remains visible is an intentional
        // operator-facing conflict and must not select the short background
        // cadence forever.
        None => true,
        Some(object)
            if object.version == conflict.local_version
                || object.version == conflict.remote_version =>
        {
            true
        }
        Some(object)
            if conflict
                .base_version
                .as_ref()
                .is_some_and(|base_version| object.version == *base_version) =>
        {
            false
        }
        Some(_) => true,
    }
}

#[cfg(test)]
pub(super) fn daemon_dfs_actionable_placement_reconciliation_count(
    jobs: &[DfsPlacementReconciliationJob],
    pending_repair_count: usize,
) -> usize {
    sinomo_dfs::dfs_actionable_placement_reconciliation_count(jobs, pending_repair_count)
}

#[cfg(test)]
pub(super) fn daemon_dfs_pending_work_from_records(
    pending_write_count: usize,
    pending_repair_count: usize,
    pending_namespace_job_count: usize,
    actionable_conflict_count: usize,
    actionable_placement_reconciliation_count: usize,
    write_intents: &[DfsWriteIntentRecord],
    receipt_records: &[DfsReplicaReceiptRecord],
    metadata_shard_statuses: &[DfsMetadataShardStatus],
    repair_views: &[DfsOperatorRepairView],
    repair_attempt_records: &[DfsRepairAttemptRecord],
    intents_without_recovery_source: &BTreeSet<String>,
    repairs_without_recovery_source: &BTreeSet<(String, NodeId)>,
    offline_node_ids: &BTreeSet<NodeId>,
    local_node_id: &NodeId,
    pending_local_membership_handoff_count: usize,
    now_unix_nanos: u64,
) -> DaemonDfsPendingMaintenanceWork {
    // Keep offline repair state durable, but do not let it select the short
    // triggered-maintenance cadence. Reachability changes notify the scheduler,
    // while the deferred-work projection retains a bounded read-only re-probe.
    let mut receipts_by_version = BTreeMap::<
        (
            &DfsFileSystemId,
            &sinomo_dfs::DfsObjectId,
            &sinomo_dfs::DfsVersion,
        ),
        Vec<&DfsReplicaReceiptRecord>,
    >::new();
    for record in receipt_records {
        receipts_by_version
            .entry((
                &record.receipt.file_system_id,
                &record.receipt.object_id,
                &record.receipt.committed_version,
            ))
            .or_default()
            .push(record);
    }
    let active_write_intents = write_intents
        .iter()
        .filter(|intent| {
            matches!(
                &intent.status,
                DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
            )
        })
        .collect::<Vec<_>>();
    let stale_write_intent_min_age_nanos =
        daemon_dfs_duration_nanos(sinomo_dfs::DEFAULT_DFS_STALE_WRITE_INTENT_MIN_AGE);
    let stale_write_intent_deadlines = active_write_intents
        .iter()
        .filter(|intent| intents_without_recovery_source.contains(&intent.intent_id))
        .map(|intent| {
            intent
                .updated_at
                .unwrap_or(intent.created_at)
                .unix_timestamp_nanos()
                .try_into()
                .unwrap_or(0_u64)
                .saturating_add(stale_write_intent_min_age_nanos)
        })
        .collect::<Vec<_>>();
    let projected_stale_write_intent_count = stale_write_intent_deadlines.len();
    let due_stale_write_intent_count = stale_write_intent_deadlines
        .iter()
        .filter(|deadline| **deadline <= now_unix_nanos)
        .count();
    let next_stale_write_intent_deadline_unix_nanos = stale_write_intent_deadlines
        .into_iter()
        .filter(|deadline| *deadline > now_unix_nanos)
        .min();
    let eligible_pending_write_count = active_write_intents
        .iter()
        .filter(|intent| {
            if intents_without_recovery_source.contains(&intent.intent_id) {
                // A fresh abandoned intent has no executable short-cadence
                // action when its object, receipts, and readable hash blob are
                // all absent. Keep it durable until its exact stale-intent
                // deadline or a state-change notification makes it actionable.
                return false;
            }
            let desired_replica_node_ids = intent
                .desired_replica_node_ids
                .iter()
                .collect::<BTreeSet<_>>();
            if desired_replica_node_ids.is_empty() {
                return true;
            }
            let receipt_nodes = receipts_by_version
                .get(&(
                    &intent.file_system_id,
                    &intent.object_id,
                    &intent.target_version,
                ))
                .into_iter()
                .flatten()
                .filter(|record| {
                    intent
                        .content_hash
                        .as_ref()
                        .is_none_or(|hash| record.content_hash.as_ref() == Some(hash))
                })
                .map(|record| &record.replica_node_id)
                .collect::<BTreeSet<_>>();
            let received_replica_count = desired_replica_node_ids
                .iter()
                .filter(|node_id| receipt_nodes.contains(*node_id))
                .count();
            if received_replica_count >= intent.policy.min_commit_replicas {
                // The intent can make progress locally even if every remaining
                // desired replica is offline.
                return true;
            }
            let missing_replica_node_ids = desired_replica_node_ids
                .iter()
                .filter(|node_id| !receipt_nodes.contains(*node_id))
                .collect::<Vec<_>>();
            missing_replica_node_ids.is_empty()
                || missing_replica_node_ids
                    .iter()
                    .any(|node_id| !offline_node_ids.contains(**node_id))
        })
        .count()
        // If the summary and full scan ever disagree, retain the old fail-open
        // behavior rather than hiding work whose target cannot be classified.
        .saturating_add(pending_write_count.saturating_sub(active_write_intents.len()));

    let mut eligible_repair_keys = BTreeSet::new();
    for repair in repair_views {
        if !offline_node_ids.contains(&repair.missing_replica_node_id)
            && !repairs_without_recovery_source.contains(&(
                repair.intent_id.clone(),
                repair.missing_replica_node_id.clone(),
            ))
        {
            eligible_repair_keys
                .insert((repair.intent_id.as_str(), &repair.missing_replica_node_id));
        }
    }

    let eligible_repair_attempt_records = repair_attempt_records
        .iter()
        .filter(|record| {
            eligible_repair_keys
                .contains(&(record.intent_id.as_str(), &record.missing_replica_node_id))
        })
        .collect::<Vec<_>>();
    let attempted_eligible_repair_keys = eligible_repair_attempt_records
        .iter()
        .map(|record| (record.intent_id.as_str(), &record.missing_replica_node_id))
        .collect::<BTreeSet<_>>();
    let due_repair_attempt_count = eligible_repair_attempt_records
        .iter()
        .filter(|record| {
            record.next_retry_unix_nanos != u64::MAX
                && record.next_retry_unix_nanos <= now_unix_nanos
        })
        .count();
    let next_repair_retry_unix_nanos = eligible_repair_attempt_records
        .iter()
        .filter(|record| record.next_retry_unix_nanos != u64::MAX)
        .filter_map(|record| {
            (record.next_retry_unix_nanos > now_unix_nanos).then_some(record.next_retry_unix_nanos)
        })
        .min();
    let unattempted_repair_count = eligible_repair_keys
        .difference(&attempted_eligible_repair_keys)
        .count()
        // Preserve fail-open triggering if operator inspection returns fewer
        // repair records than the metadata summary counted.
        .saturating_add(pending_repair_count.saturating_sub(repair_views.len()));
    let metadata_replica_lag_count = metadata_shard_statuses
        .iter()
        .filter(|status| status.primary_node_id.as_ref() == Some(local_node_id))
        .flat_map(|status| status.replica_lag.iter())
        .filter(|lag| lag.lag_entries > 0 && !offline_node_ids.contains(&lag.target_node_id))
        .count();
    let all_metadata_replica_lag_count = metadata_shard_statuses
        .iter()
        .filter(|status| status.primary_node_id.as_ref() == Some(local_node_id))
        .flat_map(|status| status.replica_lag.iter())
        .filter(|lag| lag.lag_entries > 0)
        .count();
    let finite_future_repair_attempt_count = eligible_repair_attempt_records
        .iter()
        .filter(|record| {
            record.next_retry_unix_nanos != u64::MAX
                && record.next_retry_unix_nanos > now_unix_nanos
        })
        .count();
    let deferred_work_count = pending_write_count
        .saturating_sub(eligible_pending_write_count)
        .saturating_sub(projected_stale_write_intent_count)
        .saturating_add(
            pending_repair_count.saturating_sub(
                due_repair_attempt_count
                    .saturating_add(unattempted_repair_count)
                    .saturating_add(finite_future_repair_attempt_count),
            ),
        )
        .saturating_add(all_metadata_replica_lag_count.saturating_sub(metadata_replica_lag_count));

    DaemonDfsPendingMaintenanceWork {
        pending_write_count: eligible_pending_write_count,
        pending_repair_count,
        repair_attempt_count: repair_attempt_records.len(),
        due_repair_attempt_count,
        unattempted_repair_count,
        next_repair_retry_unix_nanos,
        due_time_based_maintenance_count: due_stale_write_intent_count,
        next_time_based_maintenance_unix_nanos: next_stale_write_intent_deadline_unix_nanos,
        deferred_work_count,
        pending_namespace_job_count,
        actionable_conflict_count,
        actionable_placement_reconciliation_count,
        metadata_replica_lag_count,
        pending_local_membership_handoff_count,
        completed_local_primary_handoff_rebind_count: 0,
        ..DaemonDfsPendingMaintenanceWork::default()
    }
}

#[cfg(test)]
#[derive(Debug, Default)]
pub(super) struct DaemonDfsContentGcProjection {
    pub(super) expected_shard_count: usize,
    pub(super) covered_shard_count: usize,
    pub(super) catalog_journal_lag: u64,
    pub(super) blocked_reasons: Vec<DfsContentRootCatalogIncompleteReason>,
    pub(super) scrub_blocked_issue_count: usize,
    pub(super) version_candidate_count: usize,
    pub(super) orphan_candidate_count: usize,
    pub(super) due_maintenance_count: usize,
    pub(super) reservation_due_maintenance_count: usize,
    pub(super) next_deadline_unix_nanos: Option<u64>,
}

#[cfg(test)]
pub(super) fn daemon_dfs_version_gc_candidate_count(
    report: &sinomo_dfs::ContentStoreVersionGcReport,
) -> usize {
    usize::try_from(
        report
            .newly_observed_absent_version_count
            .saturating_add(report.deletion_ready_version_count)
            .saturating_add(report.retained_same_or_stale_catalog_version_count),
    )
    .unwrap_or(usize::MAX)
}

#[cfg(test)]
pub(super) fn daemon_dfs_orphan_gc_candidate_count(
    report: &sinomo_dfs::ContentStoreGcReport,
) -> usize {
    usize::try_from(
        report
            .newly_observed_orphan_count
            .saturating_add(report.eligible_orphan_count)
            .saturating_add(report.stale_orphan_observation_count)
            .saturating_add(report.retained_same_or_stale_catalog_orphan_count)
            .saturating_add(report.eligible_packed_segment_count),
    )
    .unwrap_or(usize::MAX)
}

#[cfg(test)]
pub(super) fn daemon_dfs_content_gc_blocked_reason_requires_refresh(
    reason: &DfsContentRootCatalogIncompleteReason,
) -> bool {
    match reason {
        // These describe topology which cannot produce complete coverage yet.
        // Cluster/topology and handoff notifications re-probe them, while the
        // catalog deadline below remains a bounded safety wake.
        DfsContentRootCatalogIncompleteReason::NoFileSystemsConfigured
        | DfsContentRootCatalogIncompleteReason::NoMetadataShardsConfigured
        | DfsContentRootCatalogIncompleteReason::FileSystemWithoutMetadataShards { .. }
        | DfsContentRootCatalogIncompleteReason::MembershipHandoffFrozen { .. }
        | DfsContentRootCatalogIncompleteReason::MembershipHandoffInProgress { .. }
        | DfsContentRootCatalogIncompleteReason::PageBudgetExceeded { .. }
        | DfsContentRootCatalogIncompleteReason::UnsupportedCatalogFormat { .. } => false,
        DfsContentRootCatalogIncompleteReason::ShardIncomplete { reasons, .. } => reasons
            .iter()
            .any(daemon_dfs_content_gc_blocked_reason_requires_refresh),
        DfsContentRootCatalogIncompleteReason::NotAuthoritativePrimary { .. }
        | DfsContentRootCatalogIncompleteReason::UnknownShard
        | DfsContentRootCatalogIncompleteReason::AuthorityChanged { .. }
        | DfsContentRootCatalogIncompleteReason::TopologyChanged { .. }
        | DfsContentRootCatalogIncompleteReason::DurableCutUnavailable { .. }
        | DfsContentRootCatalogIncompleteReason::StateFingerprintChanged
        | DfsContentRootCatalogIncompleteReason::MetadataBackendUnavailable
        | DfsContentRootCatalogIncompleteReason::TransportUnavailable { .. }
        | DfsContentRootCatalogIncompleteReason::MissingFileSystem { .. }
        | DfsContentRootCatalogIncompleteReason::UnexpectedFileSystem { .. }
        | DfsContentRootCatalogIncompleteReason::CatalogRefreshFailed { .. }
        | DfsContentRootCatalogIncompleteReason::CatalogNotDurablyInstalled
        | DfsContentRootCatalogIncompleteReason::CatalogLocalScanCutInvalid
        | DfsContentRootCatalogIncompleteReason::MissingShard { .. }
        | DfsContentRootCatalogIncompleteReason::UnexpectedShard { .. }
        | DfsContentRootCatalogIncompleteReason::DuplicateShard { .. }
        | DfsContentRootCatalogIncompleteReason::CatalogExpired { .. }
        | DfsContentRootCatalogIncompleteReason::CatalogTimestampInFuture { .. }
        | DfsContentRootCatalogIncompleteReason::HeadWatermarkUnavailable { .. }
        | DfsContentRootCatalogIncompleteReason::HeadWatermarkIdentityMismatch { .. }
        | DfsContentRootCatalogIncompleteReason::CatalogBehindHeadWatermark { .. }
        | DfsContentRootCatalogIncompleteReason::HeadWatermarkFingerprintChanged { .. }
        | DfsContentRootCatalogIncompleteReason::MalformedCatalog { .. } => true,
    }
}

#[cfg(test)]
pub(super) fn daemon_dfs_content_gc_blocked_reason_is_ordinary_head_advance(
    reason: &DfsContentRootCatalogIncompleteReason,
) -> bool {
    match reason {
        DfsContentRootCatalogIncompleteReason::CatalogBehindHeadWatermark { .. } => true,
        DfsContentRootCatalogIncompleteReason::ShardIncomplete { reasons, .. } => {
            !reasons.is_empty()
                && reasons
                    .iter()
                    .all(daemon_dfs_content_gc_blocked_reason_is_ordinary_head_advance)
        }
        _ => false,
    }
}

#[cfg(test)]
pub(super) fn daemon_dfs_content_gc_projection(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    content: &LocalContentStore,
    reservations: &sinomo_dfs::ContentStoreReservationRoots,
    lifecycle_options: DfsMaintenanceRunOptions,
    now_unix_nanos: u64,
) -> Result<DaemonDfsContentGcProjection, DaemonError> {
    let expected_file_systems = cluster_dfs_all_file_system_configs(cluster_paths)?;
    let expected_shards = cluster_dfs_all_metadata_shards(cluster_paths)?;
    let pending_handoffs = cluster_dfs_all_metadata_shard_membership_handoffs(cluster_paths)?;
    let head_watermarks = cluster_dfs_all_metadata_shard_head_watermarks(cluster_paths)?;
    let catalog_work_expected = !expected_file_systems.is_empty()
        || !expected_shards.is_empty()
        || !pending_handoffs.is_empty();
    let catalog_store = default_dfs_content_root_catalog_store(data_dir.dfs_dir().join("content"));
    let catalog = match catalog_store.load() {
        Ok(catalog) => catalog,
        Err(error) => {
            return Ok(DaemonDfsContentGcProjection {
                expected_shard_count: expected_shards.len(),
                blocked_reasons: vec![
                    DfsContentRootCatalogIncompleteReason::CatalogRefreshFailed {
                        reason: format!("durable catalog is unreadable: {error}"),
                    },
                ],
                due_maintenance_count: usize::from(catalog_work_expected),
                ..DaemonDfsContentGcProjection::default()
            });
        }
    };
    let Some(catalog) = catalog else {
        return Ok(DaemonDfsContentGcProjection {
            expected_shard_count: expected_shards.len(),
            blocked_reasons: vec![if expected_file_systems.is_empty() {
                DfsContentRootCatalogIncompleteReason::NoFileSystemsConfigured
            } else if expected_shards.is_empty() {
                DfsContentRootCatalogIncompleteReason::NoMetadataShardsConfigured
            } else {
                DfsContentRootCatalogIncompleteReason::CatalogNotDurablyInstalled
            }],
            due_maintenance_count: usize::from(catalog_work_expected),
            ..DaemonDfsContentGcProjection::default()
        });
    };

    let validation = match catalog.validate_for_collection_with_head_watermark_policy(
        &expected_file_systems,
        &expected_shards,
        &pending_handoffs,
        &head_watermarks,
        false,
        now_unix_nanos,
        lifecycle_options.content_root_catalog_max_age,
    ) {
        Ok(validation) => validation,
        Err(error) => DfsContentRootCatalogCollectionValidation::incomplete_for_expected_shards(
            vec![DfsContentRootCatalogIncompleteReason::MalformedCatalog {
                reason: error.to_string(),
            }],
            expected_shards.len(),
        ),
    };
    let max_age_nanos = daemon_dfs_duration_nanos(lifecycle_options.content_root_catalog_max_age);
    let catalog_refresh_deadline = catalog
        .generated_at_unix_nanos()
        .saturating_add(max_age_nanos);
    let mut projection = DaemonDfsContentGcProjection {
        expected_shard_count: validation.expected_shard_count(),
        covered_shard_count: validation.covered_shard_count(),
        catalog_journal_lag: validation.max_journal_lag_entries(),
        blocked_reasons: validation.reasons().to_vec(),
        ..DaemonDfsContentGcProjection::default()
    };
    if !validation.is_collection_authorized() {
        let only_ordinary_head_advance = !validation.reasons().is_empty()
            && validation
                .reasons()
                .iter()
                .all(daemon_dfs_content_gc_blocked_reason_is_ordinary_head_advance);
        if only_ordinary_head_advance && catalog_refresh_deadline > now_unix_nanos {
            let catalog_versions = catalog
                .protected_version_pairs()
                .expect("a behind-head catalog with otherwise complete coverage has version roots");
            projection.reservation_due_maintenance_count = reservations
                .reservations
                .iter()
                .filter(|reservation| {
                    catalog_versions.contains(&(
                        reservation.identity.object_id.clone(),
                        reservation.identity.version.clone(),
                    ))
                })
                .count();
            // A newer quorum head is normal during sustained writes. The old
            // catalog cannot authorize deletion, but durable reservations
            // protect every post-catalog publication. Historical positive
            // coverage remains sufficient to release exact covered
            // reservations; it can never reject an uncovered reservation or
            // authorize deletion. Coalesce only the destructive refresh to
            // the existing bounded expiry deadline.
            projection.next_deadline_unix_nanos = Some(catalog_refresh_deadline);
            return Ok(projection);
        }
        let repairable_block = validation
            .reasons()
            .iter()
            .any(daemon_dfs_content_gc_blocked_reason_requires_refresh);
        if repairable_block || catalog_refresh_deadline <= now_unix_nanos {
            projection.due_maintenance_count = projection.due_maintenance_count.saturating_add(1);
        } else {
            // Structural coverage gaps and active handoffs remain parked on
            // their state-change notifications, with catalog expiry as a
            // bounded fallback wake.
            projection.next_deadline_unix_nanos = Some(catalog_refresh_deadline);
        }
        return Ok(projection);
    }
    let protection = validation
        .protection()
        .expect("authorized content-root catalog validation has protection");
    let catalog_generation = validation
        .catalog_generation()
        .expect("authorized content-root catalog validation has a generation");
    let catalog_installed_at_unix_nanos = validation
        .catalog_installed_at_unix_nanos()
        .expect("authorized content-root catalog validation has a durable install observation");
    let version_projection = content.collect_versions(
        ContentStoreVersionGcOptions::dry_run(
            lifecycle_options.content_version_gc_min_age,
            lifecycle_options.content_version_gc_keep_latest_versions,
        )
        .with_max_deletions(lifecycle_options.content_version_gc_max_records)
        .with_min_absence_age(lifecycle_options.content_version_gc_min_age)
        .with_catalog_projection_observation(catalog_generation, catalog_installed_at_unix_nanos)
        .with_protected_versions(
            protection
                .versions
                .iter()
                .map(|identity| (identity.object_id.as_str(), identity.version.as_str())),
        ),
    )?;
    let orphan_projection = content.collect_garbage(
        ContentStoreGcOptions::dry_run(lifecycle_options.content_orphan_gc_min_age)
            .with_max_deletions(lifecycle_options.content_orphan_gc_max_records)
            .with_catalog_projection_observation(
                catalog_generation,
                catalog_installed_at_unix_nanos,
            )
            .with_protected_content_hashes(protection.content_hashes.iter().map(String::as_str)),
    )?;
    projection.scrub_blocked_issue_count = usize::try_from(
        version_projection
            .blocked_by_scrub_issue_count
            .max(orphan_projection.blocked_by_scrub_issue_count),
    )
    .unwrap_or(usize::MAX);
    projection.version_candidate_count = daemon_dfs_version_gc_candidate_count(&version_projection);
    projection.orphan_candidate_count = daemon_dfs_orphan_gc_candidate_count(&orphan_projection);
    projection.due_maintenance_count = projection
        .due_maintenance_count
        .saturating_add(projection.version_candidate_count)
        .saturating_add(projection.orphan_candidate_count);
    projection.next_deadline_unix_nanos = projection
        .next_deadline_unix_nanos
        .into_iter()
        .chain(std::iter::once(catalog_refresh_deadline))
        .chain(version_projection.next_eligible_at_unix_nanos)
        .chain(orphan_projection.next_eligible_at_unix_nanos)
        .min();

    for reservation in &reservations.reservations {
        let covered = protection
            .versions
            .binary_search_by(|identity| {
                identity
                    .object_id
                    .as_str()
                    .cmp(reservation.identity.object_id.as_str())
                    .then_with(|| {
                        identity
                            .version
                            .as_str()
                            .cmp(reservation.identity.version.as_str())
                    })
            })
            .is_ok();
        let rejection_deadline =
            reservation
                .reserved_at_unix_nanos
                .saturating_add(daemon_dfs_duration_nanos(
                    lifecycle_options.content_reservation_rejection_grace,
                ));
        if covered {
            projection.reservation_due_maintenance_count = projection
                .reservation_due_maintenance_count
                .saturating_add(1);
        } else if catalog_installed_at_unix_nanos > rejection_deadline
            || now_unix_nanos >= rejection_deadline
        {
            projection.due_maintenance_count = projection.due_maintenance_count.saturating_add(1);
        } else {
            projection.next_deadline_unix_nanos = projection
                .next_deadline_unix_nanos
                .into_iter()
                .chain(std::iter::once(rejection_deadline))
                .min();
        }
    }

    Ok(projection)
}

#[derive(Debug)]
#[cfg(test)]
pub(super) struct DaemonDfsMaintenanceMetadataObservation {
    pub(super) summary: DfsMetadataSummary,
    pub(super) metadata_shard_statuses: Vec<DfsMetadataShardStatus>,
    pub(super) write_intents: Vec<DfsWriteIntentRecord>,
    pub(super) repair_views: Vec<DfsOperatorRepairView>,
    pub(super) actionable_conflict_count: usize,
    pub(super) receipt_records: Vec<DfsReplicaReceiptRecord>,
    pub(super) lookups: DaemonDfsMetadataLookups,
}

#[cfg(test)]
pub(super) fn daemon_dfs_maintenance_metadata_observation(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
) -> Result<DaemonDfsMaintenanceMetadataObservation, DaemonError> {
    sinomo_dfs::with_cached_cluster_aware_dfs_metadata_inspection(
        data_dir,
        cluster_paths,
        |metadata| {
            let summary = metadata.metadata_summary()?;
            let metadata_shard_statuses = metadata.metadata_shard_statuses()?;
            let write_intents = if summary.pending_write_count > 0 {
                metadata.all_intents()?
            } else {
                Vec::new()
            };
            let repair_views = if summary.pending_repair_count > 0 {
                DfsOperatorInspector::new(metadata).repairs(None, None)?
            } else {
                Vec::new()
            };
            let conflicts = if summary.open_conflict_count > 0 {
                metadata.all_conflicts()?
            } else {
                Vec::new()
            };
            let actionable_conflict_count =
                daemon_dfs_actionable_conflict_count(metadata, &conflicts)?;

            let mut receipt_records = Vec::new();
            let mut receipt_versions = BTreeSet::new();
            for intent in write_intents.iter().filter(|intent| {
                matches!(
                    &intent.status,
                    DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
                )
            }) {
                if receipt_versions
                    .insert((intent.object_id.clone(), intent.target_version.clone()))
                {
                    receipt_records.extend(
                        metadata.receipts_for_version(&intent.object_id, &intent.target_version)?,
                    );
                }
            }
            for repair in &repair_views {
                if receipt_versions.insert((repair.object_id.clone(), repair.version.clone())) {
                    receipt_records
                        .extend(metadata.receipts_for_version(&repair.object_id, &repair.version)?);
                }
            }

            let mut object_ids = BTreeSet::new();
            let mut version_ids = BTreeSet::new();
            for intent in write_intents.iter().filter(|intent| {
                matches!(
                    &intent.status,
                    DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
                )
            }) {
                object_ids.insert(intent.object_id.clone());
                version_ids.insert((intent.object_id.clone(), intent.target_version.clone()));
            }
            for repair in &repair_views {
                object_ids.insert(repair.object_id.clone());
            }
            let mut lookups = DaemonDfsMetadataLookups::default();
            for object_id in object_ids {
                let lookup = metadata.object_lookup(&object_id);
                lookups.objects.insert(object_id, lookup);
            }
            for (object_id, version) in version_ids {
                let lookup = metadata.version_lookup(&object_id, &version);
                lookups.versions.insert((object_id, version), lookup);
            }

            Ok(DaemonDfsMaintenanceMetadataObservation {
                summary,
                metadata_shard_statuses,
                write_intents,
                repair_views,
                actionable_conflict_count,
                receipt_records,
                lookups,
            })
        },
    )
    .map_err(Into::into)
}
