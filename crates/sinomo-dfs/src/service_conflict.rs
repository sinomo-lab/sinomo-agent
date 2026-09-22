//! Future split-recovery conflict records and reconnect reconciliation for DFS services.
//!
//! This module keeps future split-recovery conflict metadata, conflict lookup,
//! and reconnect-time local manifest reconciliation together. Normal
//! quorum-backed DFS operation must not mint conflict records; reconnect only
//! trusts authoritative metadata and ignores divergent local manifests.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use time::OffsetDateTime;

use crate::intent::write_intent_status_is_active;
use crate::metadata_pages::collect_metadata_pages;
use crate::transfer::PendingNetworkedWrite;
use crate::*;

/// Reject implausibly large durable placement records before a routine pass
/// uses their size as a point-read bound. Normal cluster replica plans are far
/// smaller; operators can inspect damaged records through explicit recovery.
const ROUTINE_CREATE_RECOVERY_MAX_REPLICAS: usize = 64;

enum DfsWriteIntentMaintenanceAction {
    CommitVisible,
    AbortMissingObject,
    AbortSuperseded,
    SkipFresh,
}

enum DfsInvisibleCreateRecovery {
    Recovered,
    Preserve,
    NotApplicable,
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    /// Returns every known conflict for an object, merging transient in-memory
    /// records with durable backend pages.
    pub fn conflicts_for_object(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<Vec<DfsConflictRecord>, DfsError> {
        self.collect_conflict_records_for_object(
            object_id,
            self.metadata.conflicts_for_object(object_id),
            |_| true,
        )
    }

    /// Returns unresolved conflicts for an object, merging transient in-memory
    /// records with durable backend pages.
    pub fn open_conflicts_for_object(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<Vec<DfsConflictRecord>, DfsError> {
        self.collect_conflict_records_for_object(
            object_id,
            self.metadata.open_conflicts_for_object(object_id),
            |conflict| conflict.status == DfsConflictStatus::Open,
        )
    }

    /// Updates a conflict status in the authoritative backend when configured,
    /// and mirrors the resulting record into the service's transient metadata.
    pub fn update_conflict_status(
        &mut self,
        object_id: &DfsObjectId,
        conflict_id: &str,
        status: DfsConflictStatus,
    ) -> Result<Option<DfsConflictRecord>, DfsError> {
        let updated = if let Some(backend) = self.metadata_backend.as_mut() {
            backend.update_conflict_status(object_id, conflict_id, status.clone())?
        } else {
            self.metadata
                .conflicts_for_object(object_id)
                .into_iter()
                .find(|conflict| conflict.conflict_id == conflict_id)
                .map(|mut conflict| {
                    conflict.status = status;
                    conflict
                })
        };
        if let Some(conflict) = updated.clone() {
            self.metadata.add_conflict(conflict);
        }
        Ok(updated)
    }

    pub(crate) fn reconcile_stale_conflicts_bounded(
        &mut self,
        max_records: usize,
    ) -> Result<DfsConflictMaintenanceReport, DfsError> {
        if max_records == 0 {
            return Ok(DfsConflictMaintenanceReport::default());
        }
        let conflicts = self.open_conflict_maintenance_candidates(max_records)?;
        let mut report = DfsConflictMaintenanceReport {
            scanned_open_conflict_count: conflicts.len(),
            ..DfsConflictMaintenanceReport::default()
        };
        for conflict in conflicts {
            let Some(status) = self.reconciled_conflict_status(&conflict)? else {
                continue;
            };
            let Some(updated) =
                self.update_conflict_status(&conflict.object_id, &conflict.conflict_id, status)?
            else {
                continue;
            };
            match updated.status {
                DfsConflictStatus::Resolved => {
                    report.resolved_conflict_count =
                        report.resolved_conflict_count.saturating_add(1);
                }
                DfsConflictStatus::Superseded => {
                    report.superseded_conflict_count =
                        report.superseded_conflict_count.saturating_add(1);
                }
                DfsConflictStatus::Open => {}
            }
        }
        Ok(report)
    }

    pub(crate) fn reconcile_stale_write_intents_bounded(
        &mut self,
        max_records: usize,
        stale_min_age: Duration,
    ) -> Result<DfsWriteIntentMaintenanceReport, DfsError> {
        if max_records == 0 {
            return Ok(DfsWriteIntentMaintenanceReport::default());
        }
        let intents = self.active_write_intent_maintenance_candidates(max_records)?;
        let mut report = DfsWriteIntentMaintenanceReport {
            scanned_active_intent_count: intents.len(),
            ..DfsWriteIntentMaintenanceReport::default()
        };
        for intent in intents {
            match self.recover_commit_ready_prepared_create(&intent)? {
                DfsInvisibleCreateRecovery::Recovered => {
                    report.committed_visible_intent_count =
                        report.committed_visible_intent_count.saturating_add(1);
                    continue;
                }
                DfsInvisibleCreateRecovery::Preserve => continue,
                DfsInvisibleCreateRecovery::NotApplicable => {}
            }
            let Some(action) = self.reconciled_write_intent_action(&intent, stale_min_age)? else {
                continue;
            };
            if matches!(action, DfsWriteIntentMaintenanceAction::SkipFresh) {
                report.skipped_fresh_stale_candidate_count += 1;
                continue;
            }
            let missing = matches!(action, DfsWriteIntentMaintenanceAction::AbortMissingObject);
            let Some(updated) = self.reconcile_observed_write_intent(&intent, missing)? else {
                continue;
            };
            match updated.status {
                DfsWriteIntentStatus::Committed => report.committed_visible_intent_count += 1,
                DfsWriteIntentStatus::Aborted if missing => {
                    report.aborted_missing_object_count += 1
                }
                DfsWriteIntentStatus::Aborted => report.aborted_superseded_intent_count += 1,
                _ => unreachable!("reconciliation returns only terminal outcomes"),
            }
        }
        Ok(report)
    }

    /// Finish one exact invisible create left after its intent and W receipts
    /// became durable but namespace admission or authority collection failed.
    ///
    /// This is deliberately narrower than full hydration. Routine maintenance
    /// never guesses content or adopts ambiguous replica history. It reuses an
    /// exact durable prepare when present; after an earlier overlapping prepare
    /// clears, the normal batched commit path may derive a fresh parent-bound
    /// prepare for a receipt-complete follower. Canonical authorization and the
    /// prepared transaction's publication CAS remain the visibility boundary.
    fn recover_commit_ready_prepared_create(
        &mut self,
        observed_intent: &DfsWriteIntentRecord,
    ) -> Result<DfsInvisibleCreateRecovery, DfsError> {
        if observed_intent.status != DfsWriteIntentStatus::ReceiptsPending
            || !observed_intent.touch_parent_on_create
        {
            return Ok(DfsInvisibleCreateRecovery::NotApplicable);
        }
        if observed_intent.desired_replica_node_ids.is_empty()
            || observed_intent.desired_replica_node_ids.len() > ROUTINE_CREATE_RECOVERY_MAX_REPLICAS
            || validate_replication_plan(
                &observed_intent.desired_replica_node_ids,
                &observed_intent.policy,
            )
            .is_err()
        {
            return Ok(DfsInvisibleCreateRecovery::Preserve);
        }
        let Some(content_hash) = observed_intent.content_hash.clone() else {
            return Ok(DfsInvisibleCreateRecovery::Preserve);
        };
        let Some(base_object) = observed_intent.object_metadata.clone() else {
            return Ok(DfsInvisibleCreateRecovery::Preserve);
        };
        if base_object.object_id != observed_intent.object_id
            || base_object.file_system_id != observed_intent.file_system_id
            || observed_intent.base_version.as_ref() != Some(&base_object.version)
        {
            return Ok(DfsInvisibleCreateRecovery::Preserve);
        }
        let Some(content_size_bytes) = self.local_object_version_manifest_size_if_matches(
            &observed_intent.object_id,
            &observed_intent.target_version,
            &content_hash,
        ) else {
            return Ok(DfsInvisibleCreateRecovery::Preserve);
        };

        // The merged global intent page deliberately hides historical shard
        // copies. Visibility recovery needs the stronger physical invariant:
        // exactly one configured local shard owns this exact durable key.
        let Some(durable_shard_id) = self.unique_exact_intent_shard(observed_intent)? else {
            return Ok(DfsInvisibleCreateRecovery::Preserve);
        };
        let shard = match self.certified_local_metadata_owner_shard_for_object(&base_object) {
            Ok(Some(shard)) => shard,
            Ok(None) | Err(DfsError::StaleOwnerEpoch { .. }) => {
                return Ok(DfsInvisibleCreateRecovery::Preserve);
            }
            Err(error) => return Err(error),
        };
        if shard.primary_node_id != self.node_id
            || shard.epoch != observed_intent.owner_epoch
            || DfsMetadataShardId::new(shard.shard_id.clone()) != durable_shard_id
        {
            return Ok(DfsInvisibleCreateRecovery::Preserve);
        }

        let operation_id = format!("namespace:create:{}", observed_intent.intent_id);
        let (prepare, receipt_node_ids) = {
            let Some(backend) = self.metadata_backend.as_ref() else {
                return Ok(DfsInvisibleCreateRecovery::Preserve);
            };
            if backend.get_object(&observed_intent.object_id)?.is_some() {
                return Ok(DfsInvisibleCreateRecovery::NotApplicable);
            }
            if backend.get_intent_on_shard(
                &durable_shard_id,
                &observed_intent.object_id,
                &observed_intent.intent_id,
            )? != Some(observed_intent.clone())
            {
                return Ok(DfsInvisibleCreateRecovery::Preserve);
            }
            let prepare = backend.get_namespace_prepare(&operation_id)?;
            let mut receipts = Vec::with_capacity(observed_intent.desired_replica_node_ids.len());
            for replica_node_id in &observed_intent.desired_replica_node_ids {
                if let Some(receipt) = backend.get_receipt_on_shard(
                    &durable_shard_id,
                    &observed_intent.object_id,
                    &observed_intent.target_version,
                    replica_node_id,
                )? {
                    receipts.push(receipt);
                }
            }
            if receipts.iter().any(|receipt| {
                receipt.receipt.file_system_id != observed_intent.file_system_id
                    || receipt.receipt.object_id != observed_intent.object_id
                    || receipt.receipt.committed_version != observed_intent.target_version
                    || receipt.content_hash.as_deref() != Some(content_hash.as_str())
                    || !observed_intent
                        .desired_replica_node_ids
                        .contains(&receipt.replica_node_id)
            }) {
                return Ok(DfsInvisibleCreateRecovery::Preserve);
            }
            let receipt_count = receipts.len();
            let receipt_node_ids = receipts
                .into_iter()
                .map(|receipt| receipt.replica_node_id)
                .collect::<BTreeSet<_>>();
            if receipt_node_ids.len() != receipt_count {
                return Ok(DfsInvisibleCreateRecovery::Preserve);
            }
            if receipt_node_ids.len() < observed_intent.policy.min_commit_replicas {
                return Ok(DfsInvisibleCreateRecovery::NotApplicable);
            }
            if let Some(prepare) = prepare.as_ref()
                && !prepared_create_matches_intent(
                    backend.as_ref(),
                    prepare,
                    &durable_shard_id,
                    observed_intent,
                    &base_object,
                    &content_hash,
                    content_size_bytes,
                    &receipt_node_ids,
                )?
            {
                return Ok(DfsInvisibleCreateRecovery::Preserve);
            }
            (prepare, receipt_node_ids)
        };

        // No epoch rebind is permitted in routine maintenance. Recheck the
        // exact physical record after the authority read before reconstructing
        // transient coordinator state.
        if self
            .metadata_backend
            .as_ref()
            .expect("metadata backend proved above")
            .get_intent_on_shard(
                &durable_shard_id,
                &observed_intent.object_id,
                &observed_intent.intent_id,
            )?
            != Some(observed_intent.clone())
        {
            return Ok(DfsInvisibleCreateRecovery::Preserve);
        }
        let expected_coordinator_intent = distributed_intent_from_record(observed_intent);
        if self
            .coordinator
            .intent(&observed_intent.intent_id)
            .is_some_and(|current| current != &expected_coordinator_intent)
        {
            return Ok(DfsInvisibleCreateRecovery::Preserve);
        }
        let existing_receipt_node_ids = self
            .coordinator
            .receipts_for_intent(&observed_intent.intent_id)
            .into_iter()
            .map(|receipt| receipt.replica_node_id.clone())
            .collect::<BTreeSet<_>>();
        if !existing_receipt_node_ids.is_subset(&receipt_node_ids) {
            return Ok(DfsInvisibleCreateRecovery::Preserve);
        }
        if self
            .coordinator
            .intent(&observed_intent.intent_id)
            .is_none()
            && self
                .coordinator
                .restore_intent(expected_coordinator_intent.clone())
                .is_err()
        {
            return Ok(DfsInvisibleCreateRecovery::Preserve);
        }
        for replica_node_id in &receipt_node_ids {
            self.restore_backend_durable_receipt(
                &observed_intent.intent_id,
                replica_node_id.clone(),
                false,
            )?;
        }
        let restored_receipt_node_ids = self
            .coordinator
            .receipts_for_intent(&observed_intent.intent_id)
            .into_iter()
            .map(|receipt| receipt.replica_node_id.clone())
            .collect::<BTreeSet<_>>();
        if self.coordinator.intent(&observed_intent.intent_id) != Some(&expected_coordinator_intent)
            || restored_receipt_node_ids != receipt_node_ids
        {
            return Ok(DfsInvisibleCreateRecovery::Preserve);
        }
        if !self
            .coordinator
            .is_commit_ready(&observed_intent.intent_id)
            .unwrap_or(false)
        {
            return Ok(DfsInvisibleCreateRecovery::Preserve);
        }
        self.add_transient_object_metadata(base_object)?;
        self.writes.pending_writes.insert(
            observed_intent.intent_id.clone(),
            PendingNetworkedWrite::new(
                observed_intent.object_id.clone(),
                observed_intent.target_version.clone(),
                content_hash,
                content_size_bytes,
            )
            .with_foreground_replica_repair(false),
        );
        self.apply_recovered_commit_ready_intent(&observed_intent.intent_id)?;
        self.cleanup_committed_pending_write_if_repaired(&observed_intent.intent_id);
        debug_assert!(prepare.is_none_or(|prepare| {
            prepare.operation_id == operation_id
                && prepare.status == DfsNamespacePrepareStatus::Prepared
        }));
        Ok(DfsInvisibleCreateRecovery::Recovered)
    }

    fn unique_exact_intent_shard(
        &self,
        intent: &DfsWriteIntentRecord,
    ) -> Result<Option<DfsMetadataShardId>, DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(None);
        };
        let mut found = None;
        for status in backend.metadata_shard_statuses()? {
            if status
                .file_system_id
                .as_ref()
                .is_some_and(|file_system_id| file_system_id != &intent.file_system_id)
            {
                continue;
            }
            let Some(physical_intent) = backend.get_intent_on_shard(
                &status.shard_id,
                &intent.object_id,
                &intent.intent_id,
            )?
            else {
                continue;
            };
            if physical_intent != *intent || found.is_some() {
                return Ok(None);
            }
            found = Some(status.shard_id);
        }
        Ok(found)
    }

    fn active_write_intent_maintenance_candidates(
        &self,
        max_records: usize,
    ) -> Result<Vec<DfsWriteIntentRecord>, DfsError> {
        let mut candidates = Vec::new();
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(candidates);
        };
        let mut cursor = None;
        while candidates.len() < max_records {
            let remaining = max_records.saturating_sub(candidates.len());
            let page = backend.write_intents_page(cursor.as_ref(), remaining)?;
            for intent in page.intents {
                if write_intent_status_is_active(&intent.status) {
                    candidates.push(intent);
                    if candidates.len() >= max_records {
                        break;
                    }
                }
            }
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        Ok(candidates)
    }

    fn reconciled_write_intent_action(
        &self,
        intent: &DfsWriteIntentRecord,
        stale_min_age: Duration,
    ) -> Result<Option<DfsWriteIntentMaintenanceAction>, DfsError> {
        if !write_intent_status_is_active(&intent.status) {
            return Ok(None);
        }
        let stale = write_intent_record_is_stale(intent, stale_min_age);
        let object = match self.current_read_object_metadata(&intent.object_id) {
            Ok(object) => object,
            Err(DfsError::UnknownObject { .. }) if stale => {
                return Ok(Some(DfsWriteIntentMaintenanceAction::AbortMissingObject));
            }
            Err(DfsError::UnknownObject { .. }) => {
                return Ok(Some(DfsWriteIntentMaintenanceAction::SkipFresh));
            }
            Err(error) => return Err(error),
        };

        if object.file_system_id == intent.file_system_id
            && object.version == intent.target_version
            && intent.content_hash.as_ref().is_none_or(|content_hash| {
                object.content_hash.as_deref() == Some(content_hash.as_str())
            })
        {
            return Ok(Some(DfsWriteIntentMaintenanceAction::CommitVisible));
        }

        let base_is_current = intent
            .base_version
            .as_ref()
            .is_some_and(|base_version| object.version == *base_version);
        if !base_is_current {
            return Ok(Some(DfsWriteIntentMaintenanceAction::AbortSuperseded));
        }
        Ok(None)
    }

    pub(crate) fn reconcile_observed_write_intent(
        &mut self,
        intent: &DfsWriteIntentRecord,
        allow_abort_missing: bool,
    ) -> Result<Option<DfsWriteIntentRecord>, DfsError> {
        self.reconcile_observed_write_intent_with_policy(intent, allow_abort_missing, false)
    }

    pub(crate) fn reconcile_observed_write_intent_with_policy(
        &mut self,
        intent: &DfsWriteIntentRecord,
        allow_abort_missing: bool,
        reject_if_unpublished: bool,
    ) -> Result<Option<DfsWriteIntentRecord>, DfsError> {
        let Some(shard_id) = self.unique_exact_intent_shard(intent)? else {
            return Ok(None);
        };
        let observed = self
            .metadata_backend
            .as_ref()
            .expect("physical intent backend")
            .get_object_on_shard(&shard_id, &intent.object_id)?;
        let authority_object = observed.as_ref().or(intent.object_metadata.as_ref());
        if self.control_plane.is_some() {
            let Some(object) = authority_object else {
                return Ok(None);
            };
            if self
                .certified_local_recovery_shard(object, &shard_id, intent.owner_epoch)?
                .is_none()
            {
                return Ok(None);
            }
        } else if authority_object.map_or(intent.coordinator_node_id != self.node_id, |object| {
            object.owner_node_id != self.node_id
        }) {
            return Ok(None);
        }
        if self.control_plane.is_none()
            && self
                .metadata_backend
                .as_ref()
                .expect("backend")
                .metadata_shard_authoritative_writes_frozen(&shard_id)?
        {
            return Ok(None);
        }
        let Some(updated) = self
            .metadata_backend
            .as_mut()
            .expect("physical intent backend")
            .reconcile_write_intent_if_unchanged(
                &shard_id,
                intent,
                observed.as_ref(),
                allow_abort_missing,
                reject_if_unpublished,
            )?
        else {
            return Ok(None);
        };
        self.record_message_committed_work(DfsMessageCommittedWork {
            publish_metadata: true,
            repair_maintenance: true,
            ..Default::default()
        });
        match updated.status {
            DfsWriteIntentStatus::Committed => {
                if self
                    .coordinator
                    .intent(&updated.intent_id)
                    .is_some_and(|restored| distributed_intent_matches_record(restored, intent))
                {
                    self.coordinator.commit_intent(&updated.intent_id)?;
                    self.restore_visible_committed_object_location_receipts(&updated)?;
                    self.restore_metadata_successor_head(&updated)?;
                }
            }
            DfsWriteIntentStatus::Aborted | DfsWriteIntentStatus::Rejected => {
                if self
                    .coordinator
                    .intent(&updated.intent_id)
                    .is_some_and(|restored| {
                        distributed_intent_matches_record(restored, intent)
                            && matches!(restored.state, DfsWriteIntentState::AwaitingReceipts)
                    })
                {
                    let _ = self.coordinator.abort_intent(&updated.intent_id);
                }
                self.remove_pending_write_state(&updated.intent_id);
            }
            DfsWriteIntentStatus::Staging
            | DfsWriteIntentStatus::ReceiptsPending
            | DfsWriteIntentStatus::Conflict => {}
        }
        Ok(Some(updated))
    }

    fn open_conflict_maintenance_candidates(
        &self,
        max_records: usize,
    ) -> Result<Vec<DfsConflictRecord>, DfsError> {
        let mut candidates = Vec::new();
        if let Some(backend) = self.metadata_backend.as_ref() {
            let mut cursor = None;
            while candidates.len() < max_records {
                let remaining = max_records.saturating_sub(candidates.len());
                let page = backend.conflict_records_page(cursor.as_ref(), remaining)?;
                for conflict in page.conflicts {
                    if conflict.status == DfsConflictStatus::Open {
                        candidates.push(conflict);
                        if candidates.len() >= max_records {
                            break;
                        }
                    }
                }
                let Some(next_cursor) = page.next_cursor else {
                    break;
                };
                cursor = Some(next_cursor);
            }
            return Ok(candidates);
        }

        for conflicts in self.metadata.conflicts.values() {
            for conflict in conflicts.values() {
                if conflict.status != DfsConflictStatus::Open {
                    continue;
                }
                candidates.push(conflict.clone());
                if candidates.len() >= max_records {
                    return Ok(candidates);
                }
            }
        }
        Ok(candidates)
    }

    fn reconciled_conflict_status(
        &self,
        conflict: &DfsConflictRecord,
    ) -> Result<Option<DfsConflictStatus>, DfsError> {
        if conflict.status != DfsConflictStatus::Open {
            return Ok(None);
        }
        let object = match self.current_read_object_metadata(&conflict.object_id) {
            Ok(object) => object,
            Err(DfsError::UnknownObject { .. }) => {
                return Ok(Some(DfsConflictStatus::Superseded));
            }
            Err(error) => return Err(error),
        };
        if object.version == conflict.local_version || object.version == conflict.remote_version {
            return Ok(Some(DfsConflictStatus::Resolved));
        }
        if conflict
            .base_version
            .as_ref()
            .is_some_and(|base_version| object.version == *base_version)
        {
            return Ok(None);
        }
        Ok(Some(DfsConflictStatus::Superseded))
    }

    /// Reconciles locally stored manifests for an object after reconnect.
    ///
    /// Matching current-version content refreshes local replica receipts.
    /// Local manifests that do not match the authoritative current or
    /// historical backend version are ignored as divergent local content. They
    /// do not overwrite the visible object and do not become DFS conflicts
    /// during normal quorum-backed operation.
    pub fn reconcile_local_manifests(
        &mut self,
        object_id: &DfsObjectId,
    ) -> Result<DfsReconnectReconciliation, DfsError> {
        let object = self.current_read_object_metadata(object_id)?;
        let manifests = self
            .content
            .manifests_for_object(object.object_id.as_str())?;
        let mut refreshed_receipts = 0;
        let mut ignored_divergent_manifests = 0usize;
        let mut has_current_manifest = false;
        let may_refresh_local_replica_receipt = self.local_manifest_receipt_refresh_is_standalone();

        for manifest in manifests {
            let manifest_version = DfsVersion::deserialize_migrating_legacy(
                manifest.version.clone(),
            )
            .map_err(|error| DfsError::ContentStore {
                message: error.to_string(),
            })?;
            if manifest_version == object.version {
                if object
                    .content_hash
                    .as_ref()
                    .is_some_and(|hash| hash == &manifest.content_hash)
                {
                    if self
                        .content
                        .has_blob(&manifest.content_hash)
                        .unwrap_or(false)
                    {
                        has_current_manifest = true;
                        if may_refresh_local_replica_receipt {
                            let local_node_id = self.node_id.clone();
                            let receipt_inserted = self.put_metadata_receipt(
                                &object.file_system_id,
                                &object.object_id,
                                &object.version,
                                &local_node_id,
                                &manifest.content_hash,
                            )?;
                            if receipt_inserted {
                                refreshed_receipts += 1;
                            }
                        }
                    }
                } else {
                    ignored_divergent_manifests = ignored_divergent_manifests.saturating_add(1);
                }
            } else if !self.local_manifest_is_known_historical_version(
                &object.object_id,
                &manifest_version,
                &manifest.content_hash,
            )? {
                ignored_divergent_manifests = ignored_divergent_manifests.saturating_add(1);
            }
        }

        Ok(DfsReconnectReconciliation {
            object_id: object.object_id.clone(),
            refreshed_receipts,
            ignored_divergent_manifests,
            conflicts: Vec::new(),
            missing_current: object.content_hash.is_some() && !has_current_manifest,
        })
    }

    /// Reconnect may rediscover bytes that placement reconciliation deliberately
    /// removed from the durable replica set. A config read cannot authorize a
    /// later receipt write because Apply may replace or create a rule between
    /// those operations. Clustered services therefore keep rediscovered bytes
    /// as cache only; placement reconciliation remains the sole writer of
    /// authoritative receipts/locations. Standalone services preserve the
    /// legacy reconnect behavior.
    fn local_manifest_receipt_refresh_is_standalone(&self) -> bool {
        self.control_plane.is_none()
    }

    #[cfg(test)]
    pub(crate) fn put_conflict_record(
        &mut self,
        conflict: &DfsConflictRecord,
    ) -> Result<(), DfsError> {
        if let Some(backend) = self.metadata_backend.as_mut() {
            if backend
                .get_conflict(&conflict.object_id, &conflict.conflict_id)?
                .as_ref()
                == Some(conflict)
            {
                self.metadata.add_conflict(conflict.clone());
                return Ok(());
            }
            backend.put_conflict(conflict)?;
        }
        self.metadata.add_conflict(conflict.clone());
        Ok(())
    }

    fn collect_conflict_records_for_object<I, F>(
        &self,
        object_id: &DfsObjectId,
        memory_conflicts: I,
        mut include_backend_conflict: F,
    ) -> Result<Vec<DfsConflictRecord>, DfsError>
    where
        I: IntoIterator<Item = DfsConflictRecord>,
        F: FnMut(&DfsConflictRecord) -> bool,
    {
        let mut conflicts: BTreeMap<String, DfsConflictRecord> = memory_conflicts
            .into_iter()
            .map(|conflict| (conflict.conflict_id.clone(), conflict))
            .collect();
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(conflicts.into_values().collect());
        };
        let backend_conflicts = collect_metadata_pages(|cursor| {
            let page = backend.conflicts_for_object_page(
                object_id,
                cursor,
                DFS_CONFLICT_SCAN_PAGE_SIZE,
            )?;
            Ok((page.conflicts, page.next_cursor))
        })?;
        for conflict in backend_conflicts {
            if include_backend_conflict(&conflict) {
                conflicts.insert(conflict.conflict_id.clone(), conflict);
            }
        }
        Ok(conflicts.into_values().collect())
    }

    fn local_manifest_is_known_historical_version(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        content_hash: &str,
    ) -> Result<bool, DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(false);
        };
        Ok(backend
            .get_version(object_id, version)?
            .is_some_and(|record| record.content_hash.as_deref() == Some(content_hash)))
    }
}

#[allow(clippy::too_many_arguments)]
fn prepared_create_matches_intent(
    backend: &dyn DfsMetadataBackend,
    prepare: &DfsNamespacePrepareRecord,
    durable_shard_id: &DfsMetadataShardId,
    intent: &DfsWriteIntentRecord,
    base_object: &DfsObjectMetadata,
    content_hash: &str,
    content_size_bytes: u64,
    receipt_node_ids: &BTreeSet<NodeId>,
) -> Result<bool, DfsError> {
    let expected_operation_id = format!("namespace:create:{}", intent.intent_id);
    if prepare.status != DfsNamespacePrepareStatus::Prepared
        || prepare.operation_id != expected_operation_id
        || prepare.file_system_id != intent.file_system_id
        || prepare.shard_id != *durable_shard_id
        || prepare.owner_epoch != intent.owner_epoch
        || prepare.transaction.file_system_id != intent.file_system_id
        || !prepare.content_receipts.is_empty()
        || prepare.completed_request.is_some()
        || prepare.completed_request_completion.is_some()
        || prepare.transaction.objects.len() != 2
        || prepare.transaction.versions.len() != 2
        || !prepare.transaction.advisory_lock_states.is_empty()
        || crate::service_replica::metadata_namespace_prepare_payload_digest(prepare)?
            != prepare.operation_digest
    {
        return Ok(false);
    }

    let child_objects = prepare
        .transaction
        .objects
        .iter()
        .filter(|object| object.object_id == intent.object_id)
        .collect::<Vec<_>>();
    let child_versions = prepare
        .transaction
        .versions
        .iter()
        .filter(|version| {
            version.object_id == intent.object_id && version.version == intent.target_version
        })
        .collect::<Vec<_>>();
    let ([child], [child_version]) = (child_objects.as_slice(), child_versions.as_slice()) else {
        return Ok(false);
    };
    if child.file_system_id != intent.file_system_id
        || child.version != intent.target_version
        || child.content_hash.as_deref() != Some(content_hash)
        || child.content_size_bytes != Some(content_size_bytes)
        || child.current_locations.len() < intent.policy.min_commit_replicas
        || child
            .current_locations
            .iter()
            .any(|node_id| !receipt_node_ids.contains(node_id))
        || child
            .current_locations
            .iter()
            .collect::<BTreeSet<_>>()
            .len()
            != child.current_locations.len()
        || child_version.file_system_id != intent.file_system_id
        || child_version.content_hash.as_deref() != Some(content_hash)
        || child_version.size_bytes != Some(content_size_bytes)
        || child_version.locations != child.current_locations
        || child_version.created_by_event_id != child.last_modified_by_event_id
        || !child_version.visible
    {
        return Ok(false);
    }

    // Only the fields intentionally advanced by the canonical content commit
    // may differ from the durable transient base recorded by the intent.
    let mut recovered_base = (*child).clone();
    recovered_base.version = base_object.version.clone();
    recovered_base.content_hash = base_object.content_hash.clone();
    recovered_base.content_size_bytes = base_object.content_size_bytes;
    recovered_base.current_locations = base_object.current_locations.clone();
    recovered_base.modified_at_unix_nanos = base_object.modified_at_unix_nanos;
    recovered_base.changed_at_unix_nanos = base_object.changed_at_unix_nanos;
    recovered_base.last_modified_by_event_id = base_object.last_modified_by_event_id.clone();
    if recovered_base != *base_object {
        return Ok(false);
    }

    let parent_path = crate::posix_path::parent_posix_path(&child.path);
    let parent_objects = prepare
        .transaction
        .objects
        .iter()
        .filter(|object| object.object_id != intent.object_id)
        .collect::<Vec<_>>();
    let [parent] = parent_objects.as_slice() else {
        return Ok(false);
    };
    if parent_path.as_deref() != Some(parent.path.as_str())
        || parent.kind != DfsObjectKind::Directory
        || !prepare.transaction.versions.iter().any(|version| {
            version.object_id == parent.object_id
                && version.version == parent.version
                && version.file_system_id == parent.file_system_id
                && version.content_hash == parent.content_hash
                && version.size_bytes == parent.content_size_bytes
                && version.locations == parent.current_locations
                && version.created_by_event_id == parent.last_modified_by_event_id
                && version.visible
        })
    {
        return Ok(false);
    }

    let (expected_object_versions, mut expected_path_occupants, _) =
        crate::service_metadata::namespace_prepare_expectations(
            backend,
            durable_shard_id,
            &prepare.transaction,
        )?;
    for expected in &mut expected_path_occupants {
        expected.path = crate::metadata_keys::normalize_path(&expected.path);
    }
    expected_path_occupants.sort();
    Ok(expected_object_versions == prepare.expected_object_versions
        && expected_path_occupants == prepare.expected_path_occupants)
}

fn write_intent_record_is_stale(intent: &DfsWriteIntentRecord, stale_min_age: Duration) -> bool {
    let Ok(stale_min_age) = time::Duration::try_from(stale_min_age) else {
        return false;
    };
    OffsetDateTime::now_utc() - intent.created_at >= stale_min_age
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::DfsObjectPolicy;
    use sinomo_core::kv::{InProcessKvStore, KvEntry, KvError, KvRevision, KvTxn, KvTxnResponse};
    use sinomo_core::network::InProcessNetwork;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[derive(Clone, Debug)]
    struct FailingOperationFloorKv {
        inner: SharedDfsControlPlaneKvClient<InProcessKvStore>,
        fail_operation_floor_reads: Arc<AtomicBool>,
    }

    impl FailingOperationFloorKv {
        fn new(inner: SharedDfsControlPlaneKvClient<InProcessKvStore>) -> Self {
            Self {
                inner,
                fail_operation_floor_reads: Arc::new(AtomicBool::new(false)),
            }
        }

        fn set_failing(&self, failing: bool) {
            self.fail_operation_floor_reads
                .store(failing, Ordering::SeqCst);
        }
    }

    impl DfsControlPlaneKvClient for FailingOperationFloorKv {
        fn refresh_latest(&mut self) -> Result<(), KvError> {
            self.inner.with_client(|kv| kv.refresh_latest())
        }

        fn get(&self, key: &str) -> Result<Option<KvEntry>, KvError> {
            if key.contains("/metadata-operation-floors/")
                && self.fail_operation_floor_reads.load(Ordering::SeqCst)
            {
                return Err(KvError::Io {
                    path: "routine-create-recovery-authoritative-view".to_string(),
                    detail: "resident Raft view changed after ReadIndex quorum; retry the read"
                        .to_string(),
                });
            }
            self.inner
                .with_client(|kv| DfsControlPlaneKvClient::get(kv, key))
        }

        fn range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError> {
            self.inner
                .with_client(|kv| DfsControlPlaneKvClient::range_prefix(kv, prefix))
        }

        fn put(&mut self, key: String, value: Vec<u8>) -> Result<KvRevision, KvError> {
            self.inner
                .with_client(|kv| DfsControlPlaneKvClient::put(kv, key, value))
        }

        fn txn(&mut self, txn: KvTxn) -> Result<KvTxnResponse, KvError> {
            self.inner
                .with_client(|kv| DfsControlPlaneKvClient::txn(kv, txn))
        }
    }

    #[derive(Default)]
    struct NoopMaintenanceHost;

    impl DfsMaintenanceHost for NoopMaintenanceHost {
        fn load_anti_entropy_cursor(
            &mut self,
            _file_system_id: &DfsFileSystemId,
        ) -> Result<Option<DfsAntiEntropyCursorRecord>, DfsError> {
            Ok(None)
        }

        fn store_anti_entropy_cursor(
            &mut self,
            _cursor: &DfsAntiEntropyCursorRecord,
        ) -> Result<(), DfsError> {
            Ok(())
        }

        fn clear_anti_entropy_cursor(
            &mut self,
            _file_system_id: &DfsFileSystemId,
        ) -> Result<(), DfsError> {
            Ok(())
        }
    }

    struct UnusedPublisher;

    impl DfsMetadataJournalPublisherHost for UnusedPublisher {
        fn open_primary_shard_backend(
            &mut self,
            _shard_id: &DfsMetadataShardId,
        ) -> Result<Box<dyn DfsMetadataBackend>, DfsError> {
            unreachable!("metadata publication is disabled in this test")
        }
    }

    struct UnusedReplicationTransport;

    impl DfsMetadataJournalReplicationTransport for UnusedReplicationTransport {
        fn has_metadata_replication_route(&self, _target_node_id: &NodeId) -> bool {
            false
        }

        fn send_metadata_journal_batch(
            &mut self,
            _target_node_id: &NodeId,
            _command: &DfsMetadataJournalBatchCommand,
            _timeout: Duration,
        ) -> Result<DfsMetadataJournalBatchResponse, DfsMetadataJournalReplicationTransportError>
        {
            unreachable!("metadata publication is disabled in this test")
        }

        fn send_metadata_snapshot_install(
            &mut self,
            _target_node_id: &NodeId,
            _command: &DfsMetadataSnapshotInstallCommand,
            _timeout: Duration,
        ) -> Result<DfsMetadataSnapshotInstallResponse, DfsMetadataJournalReplicationTransportError>
        {
            unreachable!("metadata publication is disabled in this test")
        }
    }

    fn root_shard(file_system: &DfsFileSystem, owner: &NodeId) -> DfsMetadataShardConfig {
        DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: owner.clone(),
            replica_node_ids: vec![
                NodeId::from_legacy_named("routine-create-metadata-replica")
                    .expect("metadata replica"),
            ],
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        }
    }

    fn control_plane(
        file_system: &DfsFileSystem,
        shards: &[DfsMetadataShardConfig],
    ) -> FailingOperationFloorKv {
        let shared = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
        shared.with_client(|kv| {
            let mut store = DfsControlPlaneStore::new(kv);
            store
                .put_file_system_config(&DfsFileSystemConfig::from(file_system))
                .expect("filesystem config");
            for shard in shards {
                store.put_metadata_shard(shard).expect("metadata shard");
            }
        });
        FailingOperationFloorKv::new(shared)
    }

    fn maintenance_tick<T: DfsMessageTransport>(
        service: &mut NetworkedDfsService<T>,
        owner: &NodeId,
        file_system: &DfsFileSystem,
        shards: &[DfsMetadataShardConfig],
        max_intents: usize,
    ) -> DfsMaintenanceTickReport {
        let mut host = NoopMaintenanceHost;
        let mut publisher = UnusedPublisher;
        let mut replication = UnusedReplicationTransport;
        run_dfs_maintenance_tick_with_host(
            service,
            &mut host,
            file_system.file_system_id.clone(),
            owner,
            shards.to_vec(),
            &mut publisher,
            &mut replication,
            DfsMaintenanceTickOptions {
                namespace_job_max_jobs: 0,
                namespace_job_max_steps_per_job: 0,
                conflict_max_records: 0,
                write_intent_max_records: max_intents,
                stale_write_intent_min_age: Duration::ZERO,
                placement_reconciliation_max_objects: 0,
                repair_max_jobs: 0,
                lifecycle: DfsMaintenanceRunOptions {
                    repair_metadata: false,
                    cleanup_stale_staging: false,
                    ..DfsMaintenanceRunOptions::default()
                },
                publish_metadata: false,
                ..DfsMaintenanceTickOptions::default()
            },
        )
        .expect("routine maintenance tick")
    }

    #[test]
    fn routine_ticks_recover_prepared_then_unprepared_zero_byte_creates_for_same_parent() {
        let root = sinomo_testdir::TestDir::new("dfs-routine-sequential-create-recovery");
        let owner = NodeId::from_legacy_named("routine-sequential-create-owner").expect("owner");
        let file_system = DfsFileSystem::sifs(owner.clone());
        let shard = root_shard(&file_system, &owner);
        let control_plane = control_plane(&file_system, std::slice::from_ref(&shard));
        let network = InProcessNetwork::new();
        let mut service = NetworkedDfsService::new(
            network.add_node(owner.clone()),
            LocalContentStore::new(root.path().join("content")).expect("content store"),
        )
        .with_control_plane(control_plane.clone())
        .with_metadata_backend(
            LocalSobjMetadataBackend::new(
                DfsMetadataShardId::new("root"),
                root.path().join("metadata"),
            )
            .expect("metadata backend"),
        )
        .expect("attach metadata backend");
        service
            .bootstrap_distributed_sifs()
            .expect("bootstrap SIFS");
        let parent_path = "/services/routine-sequential-create";
        service.posix_mkdir_all(parent_path).expect("create parent");

        control_plane.set_failing(true);
        for index in 1..=2 {
            let child_path = format!("{parent_path}/page-{index:02}.txt");
            service
                .posix_create_file_with_contents_and_attrs_and_operation_id(
                    &child_path,
                    &[],
                    DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
                    DfsPosixCreateAttrs::new(0o640, 6101, 6102),
                    &format!("routine-zero-create-{index}"),
                )
                .expect_err("injected authority/overlap failure must keep the create pending");
        }
        control_plane.set_failing(false);

        let backend = service.metadata_backend().expect("metadata backend");
        let pending = backend
            .write_intents_page(None, 8)
            .expect("pending intents")
            .intents
            .into_iter()
            .filter(|intent| intent.status == DfsWriteIntentStatus::ReceiptsPending)
            .collect::<Vec<_>>();
        assert_eq!(pending.len(), 2);
        assert!(
            backend
                .get_namespace_prepare(&format!("namespace:create:{}", pending[0].intent_id))
                .expect("first prepare")
                .is_some(),
            "the oldest durable intent must own the active parent prepare"
        );
        assert!(
            backend
                .get_namespace_prepare(&format!("namespace:create:{}", pending[1].intent_id))
                .expect("second prepare")
                .is_none(),
            "the later same-parent intent must be receipt-complete but unprepared"
        );

        let first_prepare = backend
            .get_namespace_prepare(&format!("namespace:create:{}", pending[0].intent_id))
            .expect("first prepare lookup")
            .expect("first prepare body");
        let first_receipt_nodes = backend
            .receipts_for_version(&pending[0].object_id, &pending[0].target_version)
            .expect("first receipts")
            .into_iter()
            .map(|receipt| receipt.replica_node_id)
            .collect::<BTreeSet<_>>();
        assert!(
            prepared_create_matches_intent(
                backend,
                &first_prepare,
                &DfsMetadataShardId::new("root"),
                &pending[0],
                pending[0].object_metadata.as_ref().expect("base object"),
                pending[0].content_hash.as_deref().expect("content hash"),
                0,
                &first_receipt_nodes,
            )
            .expect("validate first prepare"),
            "first prepare should pass routine validation: {first_prepare:#?}"
        );

        let first = maintenance_tick(
            &mut service,
            &owner,
            &file_system,
            std::slice::from_ref(&shard),
            1,
        );
        assert_eq!(first.write_intents.scanned_active_intent_count, 1);
        assert_eq!(first.write_intents.committed_visible_intent_count, 1);
        assert!(
            service
                .posix_stat(&format!("{parent_path}/page-01.txt"))
                .is_ok()
        );
        assert!(
            service
                .metadata_backend()
                .expect("metadata backend")
                .get_object_by_path(
                    &file_system.file_system_id,
                    &format!("{parent_path}/page-02.txt"),
                )
                .expect("second path")
                .is_none()
        );

        let second = maintenance_tick(
            &mut service,
            &owner,
            &file_system,
            std::slice::from_ref(&shard),
            1,
        );
        assert_eq!(second.write_intents.scanned_active_intent_count, 1);
        assert_eq!(second.write_intents.committed_visible_intent_count, 1);
        for index in 1..=2 {
            let child = service
                .posix_stat(&format!("{parent_path}/page-{index:02}.txt"))
                .expect("recovered child");
            assert_eq!(child.size_bytes, Some(0));
        }
        assert_eq!(
            service
                .metadata_backend()
                .expect("metadata backend")
                .metadata_summary()
                .expect("metadata summary")
                .pending_write_count,
            0
        );

        let settled = maintenance_tick(
            &mut service,
            &owner,
            &file_system,
            std::slice::from_ref(&shard),
            1,
        );
        assert_eq!(settled.write_intents.scanned_active_intent_count, 0);
        assert_eq!(settled.write_intents.committed_visible_intent_count, 0);
    }

    #[test]
    fn routine_tick_preserves_mismatched_coordinator_intent_and_extra_receipt() {
        let root = sinomo_testdir::TestDir::new("dfs-routine-coordinator-mismatch");
        let owner = NodeId::from_legacy_named("routine-coordinator-mismatch-owner").expect("owner");
        let extra_replica =
            NodeId::from_legacy_named("routine-coordinator-mismatch-extra").expect("extra");
        let file_system = DfsFileSystem::sifs(owner.clone());
        let shard = root_shard(&file_system, &owner);
        let control_plane = control_plane(&file_system, std::slice::from_ref(&shard));
        let metadata_root = root.path().join("metadata");
        let content_root = root.path().join("content");
        let network = InProcessNetwork::new();
        let mut original = NetworkedDfsService::new(
            network.add_node(owner.clone()),
            LocalContentStore::new(&content_root).expect("content store"),
        )
        .with_control_plane(control_plane.clone())
        .with_metadata_backend(
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &metadata_root)
                .expect("metadata backend"),
        )
        .expect("attach metadata backend");
        original
            .bootstrap_distributed_sifs()
            .expect("bootstrap SIFS");
        let parent_path = "/services/routine-coordinator-mismatch";
        let child_path = format!("{parent_path}/page.txt");
        original
            .posix_mkdir_all(parent_path)
            .expect("create parent");
        control_plane.set_failing(true);
        original
            .posix_create_file_with_contents_and_attrs_and_operation_id(
                &child_path,
                &[],
                DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
                DfsPosixCreateAttrs::new(0o640, 6101, 6102),
                "routine-coordinator-mismatch-create",
            )
            .expect_err("injected authority failure must leave a prepared intent");
        control_plane.set_failing(false);
        let intent = original
            .metadata_backend()
            .expect("metadata backend")
            .write_intents_page(None, 8)
            .expect("intent page")
            .intents
            .into_iter()
            .find(|intent| intent.status == DfsWriteIntentStatus::ReceiptsPending)
            .expect("pending intent");
        let prepare_operation_id = format!("namespace:create:{}", intent.intent_id);
        let prepare_before = original
            .metadata_backend()
            .expect("metadata backend")
            .get_namespace_prepare(&prepare_operation_id)
            .expect("prepare lookup")
            .expect("prepared create");
        drop(original);

        let network = InProcessNetwork::new();
        let mut recovered = NetworkedDfsService::new(
            network.add_node(owner.clone()),
            LocalContentStore::new(&content_root).expect("recovered content store"),
        )
        .with_control_plane(control_plane)
        .with_metadata_backend_lazy(
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &metadata_root)
                .expect("reopened metadata backend"),
        )
        .expect("lazy metadata attachment");
        let mut mismatched_intent = distributed_intent_from_record(&intent);
        mismatched_intent
            .desired_replica_nodes
            .push(extra_replica.clone());
        mismatched_intent.policy = DfsReplicationPolicy::new(1, 2).expect("mismatched policy");
        recovered
            .coordinator
            .restore_intent(mismatched_intent.clone())
            .expect("mismatched coordinator intent");
        recovered
            .coordinator
            .restore_durable_receipt(&intent.intent_id, owner.clone())
            .expect("matching in-memory receipt");
        recovered
            .coordinator
            .restore_durable_receipt(&intent.intent_id, extra_replica.clone())
            .expect("extra in-memory receipt");

        let report = maintenance_tick(
            &mut recovered,
            &owner,
            &file_system,
            std::slice::from_ref(&shard),
            1,
        );
        assert_eq!(report.write_intents.scanned_active_intent_count, 1);
        assert_eq!(report.write_intents.committed_visible_intent_count, 0);
        assert_eq!(report.write_intents.aborted_missing_object_count, 0);
        assert_eq!(report.write_intents.aborted_superseded_intent_count, 0);
        assert_eq!(
            recovered.coordinator.intent(&intent.intent_id),
            Some(&mismatched_intent),
            "routine recovery must not replace a non-identical in-memory intent"
        );
        assert_eq!(
            recovered
                .coordinator
                .receipts_for_intent(&intent.intent_id)
                .into_iter()
                .map(|receipt| receipt.replica_node_id.clone())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([owner, extra_replica]),
            "routine recovery must not consume or publish an extra in-memory receipt"
        );
        assert!(
            recovered
                .metadata_backend()
                .expect("metadata backend")
                .get_object_by_path(&file_system.file_system_id, &child_path)
                .expect("child lookup")
                .is_none(),
            "mismatched coordinator state must remain namespace-invisible"
        );
        assert_eq!(
            recovered
                .metadata_backend()
                .expect("metadata backend")
                .get_intent(&intent.object_id, &intent.intent_id)
                .expect("durable intent"),
            Some(intent)
        );
        assert_eq!(
            recovered
                .metadata_backend()
                .expect("metadata backend")
                .get_namespace_prepare(&prepare_operation_id)
                .expect("prepare lookup"),
            Some(prepare_before)
        );
    }

    #[test]
    fn routine_tick_preserves_exact_coordinator_intent_with_nondurable_extra_receipt() {
        let root = sinomo_testdir::TestDir::new("dfs-routine-extra-coordinator-receipt");
        let owner =
            NodeId::from_legacy_named("routine-extra-coordinator-receipt-owner").expect("owner");
        let extra_replica =
            NodeId::from_legacy_named("routine-extra-coordinator-receipt-node").expect("extra");
        let file_system = DfsFileSystem::sifs(owner.clone());
        let shard = root_shard(&file_system, &owner);
        let control_plane = control_plane(&file_system, std::slice::from_ref(&shard));
        let metadata_root = root.path().join("metadata");
        let content_root = root.path().join("content");
        let network = InProcessNetwork::new();
        let mut original = NetworkedDfsService::new(
            network.add_node(owner.clone()),
            LocalContentStore::new(&content_root).expect("content store"),
        )
        .with_control_plane(control_plane.clone())
        .with_metadata_backend(
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &metadata_root)
                .expect("metadata backend"),
        )
        .expect("attach metadata backend");
        original
            .bootstrap_distributed_sifs()
            .expect("bootstrap SIFS");
        let parent_path = "/services/routine-extra-coordinator-receipt";
        let child_path = format!("{parent_path}/page.txt");
        original
            .posix_mkdir_all(parent_path)
            .expect("create parent");
        control_plane.set_failing(true);
        original
            .posix_create_file_with_contents_and_attrs_and_operation_id(
                &child_path,
                &[],
                DfsWriteOptions::replicated_eventual(vec![owner.clone(), extra_replica.clone()]),
                DfsPosixCreateAttrs::new(0o640, 6101, 6102),
                "routine-extra-coordinator-receipt-create",
            )
            .expect_err("injected authority failure must leave a prepared intent");
        control_plane.set_failing(false);
        let intent = original
            .metadata_backend()
            .expect("metadata backend")
            .write_intents_page(None, 8)
            .expect("intent page")
            .intents
            .into_iter()
            .find(|intent| intent.status == DfsWriteIntentStatus::ReceiptsPending)
            .expect("pending intent");
        assert_eq!(
            intent.desired_replica_node_ids,
            vec![owner.clone(), extra_replica.clone()]
        );
        let prepare_operation_id = format!("namespace:create:{}", intent.intent_id);
        let prepare_before = original
            .metadata_backend()
            .expect("metadata backend")
            .get_namespace_prepare(&prepare_operation_id)
            .expect("prepare lookup")
            .expect("prepared create");
        assert!(
            original
                .metadata_backend()
                .expect("metadata backend")
                .get_receipt_on_shard(
                    &DfsMetadataShardId::new("root"),
                    &intent.object_id,
                    &intent.target_version,
                    &owner,
                )
                .expect("owner receipt")
                .is_some()
        );
        assert!(
            original
                .metadata_backend()
                .expect("metadata backend")
                .get_receipt_on_shard(
                    &DfsMetadataShardId::new("root"),
                    &intent.object_id,
                    &intent.target_version,
                    &extra_replica,
                )
                .expect("extra receipt")
                .is_none(),
            "the adversarial receipt must exist only in coordinator memory"
        );
        drop(original);

        let network = InProcessNetwork::new();
        let mut recovered = NetworkedDfsService::new(
            network.add_node(owner.clone()),
            LocalContentStore::new(&content_root).expect("recovered content store"),
        )
        .with_control_plane(control_plane)
        .with_metadata_backend_lazy(
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &metadata_root)
                .expect("reopened metadata backend"),
        )
        .expect("lazy metadata attachment");
        let expected_intent = distributed_intent_from_record(&intent);
        recovered
            .coordinator
            .restore_intent(expected_intent.clone())
            .expect("exact coordinator intent");
        recovered
            .coordinator
            .restore_durable_receipt(&intent.intent_id, owner.clone())
            .expect("durable in-memory receipt");
        recovered
            .coordinator
            .restore_durable_receipt(&intent.intent_id, extra_replica.clone())
            .expect("nondurable extra in-memory receipt");

        let report = maintenance_tick(
            &mut recovered,
            &owner,
            &file_system,
            std::slice::from_ref(&shard),
            1,
        );
        assert_eq!(report.write_intents.scanned_active_intent_count, 1);
        assert_eq!(report.write_intents.committed_visible_intent_count, 0);
        assert_eq!(report.write_intents.aborted_missing_object_count, 0);
        assert_eq!(report.write_intents.aborted_superseded_intent_count, 0);
        assert_eq!(
            recovered.coordinator.intent(&intent.intent_id),
            Some(&expected_intent)
        );
        assert_eq!(
            recovered
                .coordinator
                .receipts_for_intent(&intent.intent_id)
                .into_iter()
                .map(|receipt| receipt.replica_node_id.clone())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([owner, extra_replica.clone()]),
            "the subset guard must preserve the adversarial coordinator state"
        );
        assert!(
            recovered
                .metadata_backend()
                .expect("metadata backend")
                .get_object_by_path(&file_system.file_system_id, &child_path)
                .expect("child lookup")
                .is_none(),
            "an extra nondurable receipt must not become visible"
        );
        assert_eq!(
            recovered
                .metadata_backend()
                .expect("metadata backend")
                .get_intent(&intent.object_id, &intent.intent_id)
                .expect("durable intent"),
            Some(intent.clone())
        );
        assert_eq!(
            recovered
                .metadata_backend()
                .expect("metadata backend")
                .get_namespace_prepare(&prepare_operation_id)
                .expect("prepare lookup"),
            Some(prepare_before)
        );
        assert!(
            recovered
                .metadata_backend()
                .expect("metadata backend")
                .get_receipt_on_shard(
                    &DfsMetadataShardId::new("root"),
                    &intent.object_id,
                    &intent.target_version,
                    &extra_replica,
                )
                .expect("durable extra receipt")
                .is_none()
        );
    }

    #[test]
    fn routine_tick_keeps_duplicate_physical_intent_shards_invisible() {
        let root = sinomo_testdir::TestDir::new("dfs-routine-duplicate-intent-shards");
        let owner = NodeId::from_legacy_named("routine-duplicate-intent-owner").expect("owner");
        let file_system = DfsFileSystem::sifs(owner.clone());
        let root_shard = root_shard(&file_system, &owner);
        let legacy_shard = DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "legacy".to_string(),
            primary_node_id: owner.clone(),
            replica_node_ids: root_shard.replica_node_ids.clone(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::PathPrefix {
                prefix: "/legacy".to_string(),
            },
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let shards = vec![root_shard.clone(), legacy_shard.clone()];
        let control_plane = control_plane(&file_system, &shards);
        let metadata_root = root.path().join("metadata");
        let content_root = root.path().join("content");
        let network = InProcessNetwork::new();
        let mut original = NetworkedDfsService::new(
            network.add_node(owner.clone()),
            LocalContentStore::new(&content_root).expect("content store"),
        )
        .with_control_plane(control_plane.clone())
        .with_metadata_backend(
            LocalShardedMetadataBackend::new(&metadata_root, shards.clone())
                .expect("sharded metadata backend"),
        )
        .expect("attach metadata backend");
        original
            .bootstrap_distributed_sifs()
            .expect("bootstrap SIFS");
        let parent_path = "/services/routine-duplicate-intent";
        let child_path = format!("{parent_path}/page.txt");
        original
            .posix_mkdir_all(parent_path)
            .expect("create parent");

        control_plane.set_failing(true);
        original
            .posix_create_file_with_contents_and_attrs_and_operation_id(
                &child_path,
                &[],
                DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
                DfsPosixCreateAttrs::new(0o640, 6101, 6102),
                "routine-duplicate-zero-create",
            )
            .expect_err("injected authority failure must leave a prepared intent");
        control_plane.set_failing(false);
        let intent = original
            .metadata_backend()
            .expect("metadata backend")
            .write_intents_page(None, 8)
            .expect("intent page")
            .intents
            .into_iter()
            .find(|intent| intent.status == DfsWriteIntentStatus::ReceiptsPending)
            .expect("pending intent");
        let receipts = intent
            .desired_replica_node_ids
            .iter()
            .filter_map(|replica_node_id| {
                original
                    .metadata_backend()
                    .expect("metadata backend")
                    .get_receipt_on_shard(
                        &DfsMetadataShardId::new(root_shard.shard_id.clone()),
                        &intent.object_id,
                        &intent.target_version,
                        replica_node_id,
                    )
                    .expect("durable receipt")
            })
            .collect::<Vec<_>>();
        assert_eq!(receipts.len(), 1);
        drop(original);

        let legacy_root =
            metadata_root.join(crate::metadata_keys::key_segment(&legacy_shard.shard_id));
        let mut legacy_backend = LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new(legacy_shard.shard_id.clone()),
            legacy_root,
        )
        .expect("legacy physical shard");
        legacy_backend
            .put_file_system(&file_system)
            .expect("legacy filesystem");
        legacy_backend
            .put_intent_and_receipts_for_object(intent.object_metadata.as_ref(), &intent, &receipts)
            .expect("duplicate exact intent and receipt");
        drop(legacy_backend);

        let network = InProcessNetwork::new();
        let mut recovered = NetworkedDfsService::new(
            network.add_node(owner.clone()),
            LocalContentStore::new(&content_root).expect("recovered content store"),
        )
        .with_control_plane(control_plane)
        .with_metadata_backend_lazy(
            LocalShardedMetadataBackend::new(&metadata_root, shards.clone())
                .expect("reopened sharded backend"),
        )
        .expect("lazy metadata attachment");
        let report = maintenance_tick(&mut recovered, &owner, &file_system, &shards, 1);
        assert_eq!(report.write_intents.scanned_active_intent_count, 1);
        assert_eq!(report.write_intents.committed_visible_intent_count, 0);
        assert_eq!(report.write_intents.aborted_missing_object_count, 0);
        assert_eq!(report.write_intents.aborted_superseded_intent_count, 0);
        assert_eq!(report.write_intents.skipped_fresh_stale_candidate_count, 0);
        assert!(
            recovered
                .metadata_backend()
                .expect("metadata backend")
                .get_object_by_path(&file_system.file_system_id, &child_path)
                .expect("child lookup")
                .is_none(),
            "an ambiguous physical intent must remain namespace-invisible"
        );
        for shard in [&root_shard, &legacy_shard] {
            assert_eq!(
                recovered
                    .metadata_backend()
                    .expect("metadata backend")
                    .get_intent_on_shard(
                        &DfsMetadataShardId::new(shard.shard_id.clone()),
                        &intent.object_id,
                        &intent.intent_id,
                    )
                    .expect("exact shard intent"),
                Some(intent.clone()),
                "routine maintenance must preserve both ambiguous durable copies"
            );
        }
    }
}
