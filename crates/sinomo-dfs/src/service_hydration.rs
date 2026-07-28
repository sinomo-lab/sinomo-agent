//! Metadata backend hydration and journal replay for the networked DFS service.
//!
//! This module keeps durable metadata replay, missing-payload journal recovery,
//! and in-memory service-state restoration out of the live data-plane protocol
//! code.

use std::collections::{BTreeMap, BTreeSet};

use sinomo_core::DfsWriteReceipt;
use sinomo_core::events::EventId;
use time::OffsetDateTime;

use crate::metadata_pages::{try_walk_metadata_pages, walk_metadata_pages};
use crate::transfer::PendingNetworkedWrite;
use crate::*;

type HydrationReceiptsByVersion = BTreeMap<(DfsObjectId, DfsVersion), Vec<DfsReplicaReceiptRecord>>;

fn durable_path_object_fences_replayed_create(
    durable_path_object: &DfsObjectMetadata,
    intent_record: &DfsWriteIntentRecord,
) -> bool {
    durable_path_object.kind != DfsObjectKind::Tombstone
        || intent_record
            .create_predecessor_object_id
            .as_ref()
            .is_none_or(|predecessor_object_id| {
                predecessor_object_id == &intent_record.object_id
                    || predecessor_object_id != &durable_path_object.object_id
            })
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub(crate) fn seed_local_metadata_journal_cursors_from_backend(
        &mut self,
    ) -> Result<(), DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(());
        };
        for status in backend.metadata_shard_statuses()? {
            let cursor = self
                .local_metadata_journal_cursors
                .entry(status.shard_id)
                .or_insert(0);
            *cursor = (*cursor).max(status.journal_last_sequence);
        }
        Ok(())
    }

    fn rebuild_cross_shard_namespace_locks(&mut self) -> Result<(), DfsError> {
        self.cross_shard_namespace_locks.clear();
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(());
        };
        let mut locks = Vec::new();
        walk_metadata_pages(|cursor| {
            let page =
                backend.list_namespace_jobs_page(cursor, DFS_METADATA_HYDRATE_OBJECT_PAGE_SIZE)?;
            for job in page.jobs {
                if job.status == DfsNamespaceJobStatus::Running
                    && matches!(
                        job.kind,
                        DfsNamespaceJobKind::RenameObjectAcrossShardPrimaries
                            | DfsNamespaceJobKind::HardlinkAcrossShardPrimaries
                            | DfsNamespaceJobKind::RenameDirectoryAcrossShardPrimaries
                    )
                {
                    locks.push((job.file_system_id.clone(), job.root_path.clone()));
                }
            }
            Ok(page.next_cursor)
        })?;
        for (file_system_id, path) in locks {
            self.lock_cross_shard_namespace_path(&file_system_id, &path);
        }
        Ok(())
    }

    fn apply_cross_shard_namespace_job_guard(&mut self, job: &DfsNamespaceJobRecord) {
        if !matches!(
            job.kind,
            DfsNamespaceJobKind::RenameObjectAcrossShardPrimaries
                | DfsNamespaceJobKind::HardlinkAcrossShardPrimaries
                | DfsNamespaceJobKind::RenameDirectoryAcrossShardPrimaries
        ) {
            return;
        }
        if job.status == DfsNamespaceJobStatus::Running {
            self.lock_cross_shard_namespace_path(&job.file_system_id, &job.root_path);
        } else {
            self.unlock_cross_shard_namespace_path(&job.file_system_id, &job.root_path);
        }
    }

    pub fn hydrate_from_metadata_backend(&mut self) -> Result<(), DfsError> {
        self.hydrate_from_metadata_backend_with_promotion_recovery(false)
    }

    /// Hydrate durable service state and reconstruct post-response replica
    /// promotions in the same object/receipt/intent scan.
    ///
    /// This is deliberately separate from ordinary node-transport hydration:
    /// a maintenance service may share the served lanes' detached outbox
    /// without claiming that its own foreground writes can respond before
    /// replica repair. Keeping recovery in this one pass also avoids opening a
    /// normally hydrated service and then scanning the full backend again.
    pub(crate) fn hydrate_from_metadata_backend_for_commit_promotion_recovery(
        &mut self,
    ) -> Result<(), DfsError> {
        self.hydrate_from_metadata_backend_with_promotion_recovery(true)
    }

    fn hydrate_from_metadata_backend_with_promotion_recovery(
        &mut self,
        reconstruct_commit_promotions: bool,
    ) -> Result<(), DfsError> {
        if self.metadata_backend.is_none() {
            return Ok(());
        };

        let file_systems = self
            .metadata_backend
            .as_ref()
            .expect("metadata backend checked above")
            .list_file_systems()?;

        self.metadata = DfsMetadataStore::new();
        self.tombstones_by_metadata_path.clear();
        self.transient_metadata_object_ids.clear();
        self.transient_metadata_predecessor_object_ids.clear();
        self.coordinator = DistributedEventualCoordinator::new();
        self.pending_writes.clear();
        self.pending_reads.clear();
        self.rebuild_cross_shard_namespace_locks()?;

        for file_system in &file_systems {
            self.metadata.add_file_system(file_system.clone());
        }
        if !self
            .metadata
            .filesystems
            .contains_key(&sifs_file_system_id())
        {
            self.metadata
                .add_file_system(DfsFileSystem::sifs(self.node_id.clone()));
        }

        // Hidden namespace prepares are not part of the visible object scan.
        // A certified current primary may safely rebind and finish standalone
        // prepares now; write-coupled create prepares remain gated on their
        // durable write receipts below.
        self.recover_durable_namespace_prepares()?;

        for file_system in &file_systems {
            try_walk_metadata_pages(|cursor| {
                let page = self
                    .metadata_backend
                    .as_ref()
                    .expect("metadata backend checked above")
                    .objects_for_file_system_page(
                        &file_system.file_system_id,
                        cursor,
                        DFS_METADATA_HYDRATE_OBJECT_PAGE_SIZE,
                    )?;
                for object in page.objects {
                    self.coordinator.set_committed_version(
                        object.file_system_id.clone(),
                        object.object_id.clone(),
                        object.version.clone(),
                    );
                    self.track_hydrated_metadata_tombstone(&object);
                    self.metadata.add_object(object.clone())?;
                }
                Ok::<_, DfsError>(page.next_cursor)
            })?;
        }

        // Conflicts already have a global cursor-backed index. Scanning that
        // index once avoids one empty per-object lookup (and its table refresh
        // checks) for every hydrated object. On a mount with thousands of
        // objects and no conflicts, the old N+1 path dominated startup.
        try_walk_metadata_pages(|cursor| {
            let page = self
                .metadata_backend
                .as_ref()
                .expect("metadata backend checked above")
                .conflict_records_page(cursor, DFS_CONFLICT_SCAN_PAGE_SIZE)?;
            for conflict in page.conflicts {
                self.metadata.add_conflict(conflict);
            }
            Ok::<_, DfsError>(page.next_cursor)
        })?;

        // Receipts already have a global cursor-backed index. Materialize it
        // once for restart hydration instead of doing two object/version scans
        // for every restorable intent (coordinator receipt restore followed by
        // visible-location recovery). Besides being linear, this avoids one
        // object-shard-index lookup per intent scan on sharded backends.
        let mut receipts_by_version = HydrationReceiptsByVersion::new();
        try_walk_metadata_pages(|cursor| {
            let page = self
                .metadata_backend
                .as_ref()
                .expect("metadata backend checked above")
                .receipt_records_page(cursor, DFS_METADATA_HYDRATE_OBJECT_PAGE_SIZE)?;
            for receipt in page.receipts {
                receipts_by_version
                    .entry((
                        receipt.receipt.object_id.clone(),
                        receipt.receipt.committed_version.clone(),
                    ))
                    .or_default()
                    .push(receipt);
            }
            Ok::<_, DfsError>(page.next_cursor)
        })?;

        let durable_write_intent_shards = self.durable_write_intent_shards()?;
        let mut pending_write_records = Vec::new();
        let mut recovered_commit_promotion_records = Vec::new();
        try_walk_metadata_pages(|intent_scan_cursor| {
            let intent_page = self
                .metadata_backend
                .as_ref()
                .expect("metadata backend checked above")
                .write_intents_page(intent_scan_cursor, DFS_METADATA_OBJECT_INTENT_PAGE_SIZE)?;
            for mut intent_record in intent_page.intents {
                self.coordinator
                    .observe_restored_intent_id(&intent_record.intent_id);
                if !write_intent_status_requires_hydrate_restore(&intent_record.status) {
                    continue;
                }
                if self.metadata.object(&intent_record.object_id).is_none() {
                    if !matches!(
                        intent_record.status,
                        DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
                    ) {
                        continue;
                    }
                    let Some(object) = intent_record.object_metadata.clone() else {
                        continue;
                    };
                    if object.object_id != intent_record.object_id
                        || object.file_system_id != intent_record.file_system_id
                    {
                        continue;
                    }
                    let durable_path_object = self
                        .metadata_backend
                        .as_ref()
                        .expect("metadata backend checked above")
                        .get_object_by_path(&object.file_system_id, &object.path)?;
                    if durable_path_object.as_ref().is_some_and(|durable| {
                        durable_path_object_fences_replayed_create(durable, &intent_record)
                    }) {
                        // The create lost a later namespace race while its
                        // response was detached. A live object or compacted
                        // tombstone other than the exact predecessor recorded
                        // when the create began is authoritative, even if a
                        // damaged index happens to reuse this object ID.
                        self.mark_replayed_intent_as_rejected(&intent_record)?;
                        continue;
                    }
                }
                self.repair_visible_committed_pending_intent_record(&mut intent_record)?;
                let durable_shard_id = durable_write_intent_shards.get(&(
                    intent_record.object_id.clone(),
                    intent_record.intent_id.clone(),
                ));
                let locally_adopted_intent = self.adopt_prepared_write_intent_for_recovery(
                    &mut intent_record,
                    durable_shard_id,
                )?;
                let intent = distributed_intent_from_record(&intent_record);
                // Bulk restore: repair jobs are rebuilt once after the scan.
                // Rebuilding per restored intent/receipt made hydration
                // quadratic over an unsettled-write backlog and burned seconds
                // of CPU on every SIFS open of an affected data dir.
                match self
                    .coordinator
                    .restore_intent_deferring_repair_rebuild(intent)
                {
                    Ok(()) => {}
                    Err(DfsDistributedError::PendingIntentConflict { .. })
                        if matches!(
                            intent_record.status,
                            DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
                        ) =>
                    {
                        self.mark_replayed_intent_as_rejected(&intent_record)?;
                        continue;
                    }
                    Err(
                        DfsDistributedError::DuplicateIntentId { .. }
                        | DfsDistributedError::DuplicateIntent { .. },
                    ) => {
                        if matches!(intent_record.status, DfsWriteIntentStatus::Committed)
                            && self
                                .current_read_object_metadata(&intent_record.object_id)
                                .is_ok_and(|object| {
                                    object.version == intent_record.target_version
                                        && intent_record.content_hash.as_deref().is_none_or(
                                            |content_hash| {
                                                object.content_hash.as_deref() == Some(content_hash)
                                            },
                                        )
                                })
                        {
                            let receipts = receipts_by_version
                                .entry((
                                    intent_record.object_id.clone(),
                                    intent_record.target_version.clone(),
                                ))
                                .or_default();
                            self.restore_visible_committed_object_location_receipts_from_hydration_scan(
                                &intent_record,
                                receipts,
                                false,
                            )?;
                            recovered_commit_promotion_records.push(intent_record.clone());
                            continue;
                        }
                        self.mark_replayed_intent_as_rejected(&intent_record)?;
                        continue;
                    }
                    Err(error) => return Err(DfsError::from(error)),
                }
                let receipts = receipts_by_version
                    .entry((
                        intent_record.object_id.clone(),
                        intent_record.target_version.clone(),
                    ))
                    .or_default();
                self.restore_backend_receipts_for_intent_from_hydration_scan(
                    &intent_record,
                    receipts,
                    false,
                )?;
                self.restore_visible_committed_object_location_receipts_from_hydration_scan(
                    &intent_record,
                    receipts,
                    false,
                )?;
                if matches!(intent_record.status, DfsWriteIntentStatus::Committed) {
                    recovered_commit_promotion_records.push(intent_record.clone());
                }
                if matches!(
                    intent_record.status,
                    DfsWriteIntentStatus::Staging
                        | DfsWriteIntentStatus::ReceiptsPending
                        | DfsWriteIntentStatus::Committed
                ) {
                    pending_write_records.push((intent_record, locally_adopted_intent));
                }
            }
            Ok(intent_page.next_cursor)
        })?;
        self.coordinator.rebuild_repair_jobs_after_bulk_restore();
        if reconstruct_commit_promotions {
            for intent_record in &recovered_commit_promotion_records {
                self.queue_recovered_commit_promotions(intent_record)?;
            }
        }

        let repair_intent_ids: BTreeSet<String> = self
            .coordinator
            .repair_jobs()
            .iter()
            .map(|job| job.intent_id.clone())
            .collect();
        let mut recovered_commit_ready_intent_ids = Vec::new();
        for (intent_record, locally_adopted_intent) in pending_write_records {
            if !self
                .coordinator
                .intent(&intent_record.intent_id)
                .is_some_and(|restored_intent| {
                    distributed_intent_matches_record(restored_intent, &intent_record)
                })
            {
                continue;
            }
            let commit_ready = self
                .coordinator
                .is_commit_ready(&intent_record.intent_id)
                .unwrap_or(false);
            let missing_visible_object = self.metadata.object(&intent_record.object_id).is_none();
            if missing_visible_object
                && (!matches!(
                    intent_record.status,
                    DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
                ) || !commit_ready)
            {
                continue;
            }
            let authority_object = self
                .metadata
                .object(&intent_record.object_id)
                .cloned()
                .or_else(|| intent_record.object_metadata.clone());
            let locally_authoritative_intent = if matches!(
                intent_record.status,
                DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
            ) && self.control_plane.is_some()
            {
                locally_adopted_intent
            } else {
                intent_record.coordinator_node_id == self.node_id
            };
            let needs_pending_write = locally_authoritative_intent
                && (matches!(
                    intent_record.status,
                    DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
                ) || (matches!(intent_record.status, DfsWriteIntentStatus::Committed)
                    && repair_intent_ids.contains(&intent_record.intent_id)));
            if needs_pending_write && let Some(content_hash) = intent_record.content_hash.clone() {
                let size_hint = self.pending_write_size_hint(
                    &intent_record.object_id,
                    &intent_record.target_version,
                    &content_hash,
                )?;
                if missing_visible_object && size_hint.is_none() {
                    continue;
                }
                if missing_visible_object {
                    let Some(object) = authority_object else {
                        continue;
                    };
                    // A create intent is durable before its first visible object.
                    // Expose its base metadata only once durable receipts prove the
                    // write commit-ready, immediately before finalizing it.
                    self.add_transient_object_metadata(object)?;
                }
                self.pending_writes.insert(
                    intent_record.intent_id.clone(),
                    PendingNetworkedWrite::new(
                        intent_record.object_id.clone(),
                        intent_record.target_version.clone(),
                        content_hash,
                        size_hint.unwrap_or(0),
                    )
                    .with_foreground_replica_repair(false),
                );
                if matches!(
                    intent_record.status,
                    DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
                ) && commit_ready
                {
                    recovered_commit_ready_intent_ids.push(intent_record.intent_id.clone());
                }
            }
        }
        for intent_id in recovered_commit_ready_intent_ids {
            self.apply_recovered_commit_ready_intent(&intent_id)?;
            self.cleanup_committed_pending_write_if_repaired(&intent_id);
        }

        // Request-owned namespace prepares were completed before the object
        // scan above. Any admission which crashed before it could persist a
        // prepare is now safe to resume from its exact durable operation. The
        // bounded pass keeps service open finite; reactive maintenance repeats
        // it until no locally authoritative request remains pending.
        let _ = self.recover_pending_namespace_requests_bounded(
            crate::service_routed_namespace::DFS_NAMESPACE_REQUEST_RECOVERY_BATCH_LIMIT,
        )?;
        self.seed_local_metadata_journal_cursors_from_backend()?;
        Ok(())
    }

    /// Reconstruct post-response CommitVersion work from durable committed
    /// intents and receipts. The transport outbox deduplicates successful
    /// identities for this process; after a restart, replaying an already
    /// applied exact version/hash is safe and closes the crash window between
    /// visible metadata commit and the original detached notification.
    fn queue_recovered_commit_promotions(
        &mut self,
        intent_record: &DfsWriteIntentRecord,
    ) -> Result<(), DfsError> {
        if !matches!(intent_record.status, DfsWriteIntentStatus::Committed) {
            return Ok(());
        }
        // This runs only after the hydration pass has rebuilt `metadata` from
        // the authoritative object scan and merged the already-materialized
        // global receipt scan into its locations. Going back through
        // `current_read_object_metadata` here would repeat a per-version
        // receipt lookup for every recovered promotion, defeating the
        // one-pass recovery algorithm this path exists to provide.
        let object = match self.metadata.object(&intent_record.object_id).cloned() {
            Some(object)
                if object.file_system_id == intent_record.file_system_id
                    && object.version == intent_record.target_version =>
            {
                object
            }
            _ => return Ok(()),
        };
        // Replica promotion is an authoritative metadata command. An object
        // owner is often the original content source, but placement repair can
        // install a committed intent coordinated by another node. Replaying
        // that intent from the owner would be rejected by every target and can
        // leave a detached retry loop alive indefinitely. Only the durable
        // intent coordinator, or the current metadata primary after failover,
        // may reconstruct this work.
        if intent_record.coordinator_node_id != self.node_id
            && !self.is_local_metadata_primary_for_object(&object)?
        {
            return Ok(());
        }
        let Some(content_hash) = object
            .content_hash
            .as_deref()
            .or(intent_record.content_hash.as_deref())
            .map(str::to_string)
        else {
            return Ok(());
        };
        if intent_record
            .content_hash
            .as_deref()
            .is_some_and(|expected| object.content_hash.as_deref() != Some(expected))
        {
            return Ok(());
        }
        let targets = self
            .coordinator
            .receipts_for_intent(&intent_record.intent_id)
            .into_iter()
            .filter(|receipt| {
                receipt.replica_node_id != self.node_id
                    && receipt.replica_node_id != intent_record.coordinator_node_id
            })
            .map(|receipt| receipt.replica_node_id.clone())
            .collect::<BTreeSet<_>>();
        if targets.is_empty() {
            return Ok(());
        }
        let object_metadata = Some(object);
        let authority_shard = self.command_authority_shard_for_object(object_metadata.as_ref())?;
        for target_node_id in targets {
            let request_id = self.next_request_id();
            self.send_best_effort_message_if_routable_detached(
                &target_node_id,
                DfsNetworkMessage::CommitVersion(DfsCommitVersionCommand {
                    request_id,
                    intent_id: intent_record.intent_id.clone(),
                    file_system_id: intent_record.file_system_id.clone(),
                    object_id: intent_record.object_id.clone(),
                    expected_base_version: intent_record.base_version.clone(),
                    version: intent_record.target_version.clone(),
                    content_hash: content_hash.clone(),
                    object_metadata: object_metadata.clone(),
                    authority_shard: authority_shard.clone(),
                }),
            )?;
        }
        Ok(())
    }

    pub(crate) fn hydrate_file_systems_from_metadata_backend(&mut self) -> Result<(), DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(());
        };
        for file_system in backend.list_file_systems()? {
            self.metadata.add_file_system(file_system);
        }
        if !self
            .metadata
            .filesystems
            .contains_key(&sifs_file_system_id())
        {
            self.metadata
                .add_file_system(DfsFileSystem::sifs(self.node_id.clone()));
        }
        // Lazy services still process mutations. Restore the durable guards for
        // running cross-shard namespace jobs without paying for a full object,
        // version, intent, and receipt scan.
        self.rebuild_cross_shard_namespace_locks()?;
        self.seed_local_metadata_journal_cursors_from_backend()?;
        Ok(())
    }

    fn repair_visible_committed_pending_intent_record(
        &mut self,
        intent_record: &mut DfsWriteIntentRecord,
    ) -> Result<(), DfsError> {
        if !matches!(
            intent_record.status,
            DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
        ) {
            return Ok(());
        }
        let Some(object) = self.metadata.object(&intent_record.object_id) else {
            return Ok(());
        };
        if object.file_system_id != intent_record.file_system_id
            || object.version != intent_record.target_version
        {
            return Ok(());
        }
        if let Some(content_hash) = intent_record.content_hash.as_deref()
            && object.content_hash.as_deref() != Some(content_hash)
        {
            return Ok(());
        }

        intent_record.status = DfsWriteIntentStatus::Committed;
        intent_record.updated_at = Some(OffsetDateTime::now_utc());
        if let Some(backend) = self.metadata_backend.as_mut() {
            backend.put_intent(intent_record)?;
        }
        Ok(())
    }

    /// Whether the backend's current record for the intent is in a terminal
    /// state (the write finished one way or another), making a replayed
    /// snapshot of that intent historical rather than actionable.
    fn backend_intent_record_is_terminal(
        &self,
        object_id: &DfsObjectId,
        intent_id: &str,
    ) -> Result<bool, DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(false);
        };
        Ok(backend
            .get_intent(object_id, intent_id)?
            .is_some_and(|record| {
                matches!(
                    record.status,
                    DfsWriteIntentStatus::Committed
                        | DfsWriteIntentStatus::Rejected
                        | DfsWriteIntentStatus::Conflict
                        | DfsWriteIntentStatus::Aborted
                )
            }))
    }

    pub(crate) fn restore_visible_committed_object_location_receipts(
        &mut self,
        intent_record: &DfsWriteIntentRecord,
    ) -> Result<(), DfsError> {
        self.restore_visible_committed_object_location_receipts_with_repair_rebuild(
            intent_record,
            true,
        )
    }

    /// See [`restore_backend_durable_receipt_with_repair_rebuild`](Self::restore_backend_durable_receipt_with_repair_rebuild)
    /// for the `rebuild_repair_jobs` contract; bulk hydration passes `false`
    /// and rebuilds once afterwards.
    pub(crate) fn restore_visible_committed_object_location_receipts_with_repair_rebuild(
        &mut self,
        intent_record: &DfsWriteIntentRecord,
        rebuild_repair_jobs: bool,
    ) -> Result<(), DfsError> {
        let mut receipts = self.backend_receipts_for_version(
            &intent_record.object_id,
            &intent_record.target_version,
        )?;
        self.restore_visible_committed_object_location_receipts_from_hydration_scan(
            intent_record,
            &mut receipts,
            rebuild_repair_jobs,
        )
    }

    fn restore_backend_receipts_for_intent_from_hydration_scan(
        &mut self,
        intent_record: &DfsWriteIntentRecord,
        receipts: &[DfsReplicaReceiptRecord],
        rebuild_repair_jobs: bool,
    ) -> Result<(), DfsError> {
        for receipt in receipts {
            if let Some(expected_hash) = intent_record.content_hash.as_deref()
                && receipt.content_hash.as_deref() != Some(expected_hash)
            {
                continue;
            }
            self.restore_backend_durable_receipt_with_repair_rebuild(
                &intent_record.intent_id,
                receipt.replica_node_id.clone(),
                false,
                rebuild_repair_jobs,
            )?;
        }
        Ok(())
    }

    fn restore_visible_committed_object_location_receipts_from_hydration_scan(
        &mut self,
        intent_record: &DfsWriteIntentRecord,
        receipts: &mut Vec<DfsReplicaReceiptRecord>,
        rebuild_repair_jobs: bool,
    ) -> Result<(), DfsError> {
        if !matches!(intent_record.status, DfsWriteIntentStatus::Committed) {
            return Ok(());
        }
        let Some(object) = self.metadata.object(&intent_record.object_id).cloned() else {
            return Ok(());
        };
        if object.file_system_id != intent_record.file_system_id
            || object.version != intent_record.target_version
        {
            return Ok(());
        }
        if let Some(content_hash) = intent_record.content_hash.as_deref()
            && object.content_hash.as_deref() != Some(content_hash)
        {
            return Ok(());
        }
        let Some(content_hash) = object
            .content_hash
            .as_deref()
            .or(intent_record.content_hash.as_deref())
        else {
            return Ok(());
        };

        let mut existing_receipts = BTreeSet::new();
        for receipt in receipts.iter() {
            if receipt.content_hash.as_deref() != Some(content_hash) {
                continue;
            }
            existing_receipts.insert(receipt.replica_node_id.clone());
            self.restore_backend_durable_receipt_with_repair_rebuild(
                &intent_record.intent_id,
                receipt.replica_node_id.clone(),
                false,
                rebuild_repair_jobs,
            )?;
        }

        let owner_node_id = object.owner_node_id.clone();
        for replica_node_id in object.current_locations {
            if !intent_record
                .desired_replica_node_ids
                .contains(&replica_node_id)
            {
                continue;
            }
            let has_verified_local_blob = replica_node_id == self.node_id
                && self.content.has_blob(content_hash).unwrap_or(false);
            if !existing_receipts.contains(&replica_node_id)
                && has_verified_local_blob
                && let Some(backend) = self.metadata_backend.as_mut()
            {
                let recovered_receipt = DfsReplicaReceiptRecord {
                    receipt: DfsWriteReceipt {
                        file_system_id: intent_record.file_system_id.clone(),
                        object_id: intent_record.object_id.clone(),
                        committed_version: intent_record.target_version.clone(),
                        owner_node_id: owner_node_id.clone(),
                        event_id: EventId::generate(),
                    },
                    replica_node_id: replica_node_id.clone(),
                    content_hash: Some(content_hash.to_string()),
                    received_at: OffsetDateTime::now_utc(),
                };
                backend.put_receipt(&recovered_receipt)?;
                if let Some(existing_receipt) = receipts
                    .iter_mut()
                    .find(|receipt| receipt.replica_node_id == replica_node_id)
                {
                    *existing_receipt = recovered_receipt;
                } else {
                    receipts.push(recovered_receipt);
                }
                existing_receipts.insert(replica_node_id.clone());
            }
            if existing_receipts.contains(&replica_node_id) {
                self.restore_backend_durable_receipt_with_repair_rebuild(
                    &intent_record.intent_id,
                    replica_node_id,
                    false,
                    rebuild_repair_jobs,
                )?;
            }
        }
        Ok(())
    }

    pub(crate) fn apply_metadata_journal_batch_to_service_state(
        &mut self,
        batch: &DfsMetadataJournalBatch,
    ) -> Result<(), DfsError> {
        for entry in &batch.entries {
            self.apply_metadata_journal_entry_to_service_state(entry)?;
        }
        if self.metadata_backend.is_some() {
            let affected_object_ids = batch
                .entries
                .iter()
                .flat_map(|entry| entry.mutation.affected_object_ids())
                .collect::<BTreeSet<_>>();
            for object_id in affected_object_ids {
                // The raw payload order is shard-local, while the sharded
                // backend has just reconciled global object placement. End the
                // replay with that authoritative view so a late source-shard
                // payload cannot leave the service cache on the pre-move path.
                self.apply_backend_object_to_service_state(&object_id)?;
            }
        }
        let cursor = self
            .local_metadata_journal_cursors
            .entry(batch.shard_id.clone())
            .or_insert(0);
        *cursor = (*cursor).max(batch.next_after_sequence);
        Ok(())
    }

    fn apply_metadata_journal_entry_to_service_state(
        &mut self,
        entry: &DfsMetadataJournalEntry,
    ) -> Result<(), DfsError> {
        match &entry.mutation {
            DfsMetadataMutation::PutFileSystem { file_system, .. } => {
                let Some(file_system) = file_system.as_ref() else {
                    self.apply_missing_payload_metadata_journal_entry_to_service_state(entry)?;
                    return Ok(());
                };
                self.metadata.add_file_system(file_system.clone());
            }
            DfsMetadataMutation::PutObject { object, .. } => {
                let Some(object) = object.as_ref() else {
                    self.apply_missing_payload_metadata_journal_entry_to_service_state(entry)?;
                    return Ok(());
                };
                self.apply_metadata_object_to_service_state(object.clone())?;
            }
            DfsMetadataMutation::PutVersion { version_record, .. } => {
                let Some(version_record) = version_record.as_ref() else {
                    self.apply_missing_payload_metadata_journal_entry_to_service_state(entry)?;
                    return Ok(());
                };
                self.apply_metadata_version_to_service_state(version_record);
            }
            DfsMetadataMutation::PutReceipt {
                object_id,
                version,
                replica_node_id,
                receipt,
                ..
            } => {
                if receipt.is_none() {
                    self.apply_missing_payload_metadata_journal_entry_to_service_state(entry)?;
                    return Ok(());
                }
                self.apply_metadata_receipt_to_service_state(object_id, version, replica_node_id)?;
            }
            DfsMetadataMutation::RemoveReceipt {
                object_id,
                version,
                replica_node_id,
                ..
            } => {
                if let Some(object) = self.metadata.objects.get_mut(object_id)
                    && object.version == *version
                {
                    object
                        .current_locations
                        .retain(|node_id| node_id != replica_node_id);
                }
            }
            DfsMetadataMutation::PutIntent { intent, .. } => {
                let Some(intent) = intent.as_ref() else {
                    self.apply_missing_payload_metadata_journal_entry_to_service_state(entry)?;
                    return Ok(());
                };
                self.apply_metadata_intent_to_service_state(intent)?;
            }
            DfsMetadataMutation::CommitVisibleVersion {
                committed_object,
                visible_version,
                object_id,
                committed_version,
                ..
            } => {
                let Some(committed_object) = committed_object.as_ref() else {
                    self.apply_missing_payload_metadata_journal_entry_to_service_state(entry)?;
                    return Ok(());
                };
                let Some(visible_version) = visible_version.as_ref() else {
                    self.apply_missing_payload_metadata_journal_entry_to_service_state(entry)?;
                    return Ok(());
                };
                self.apply_metadata_object_to_service_state(committed_object.clone())?;
                self.apply_metadata_version_to_service_state(visible_version);
                self.restore_backend_receipts_for_visible_version(object_id, committed_version)?;
            }
            DfsMetadataMutation::PutConflict { conflict, .. } => {
                let Some(conflict) = conflict.as_ref() else {
                    self.apply_missing_payload_metadata_journal_entry_to_service_state(entry)?;
                    return Ok(());
                };
                self.metadata.add_conflict(conflict.clone());
            }
            // Advisory locks are consumed directly from the authoritative
            // metadata backend. Namespace jobs have no legacy object mirror,
            // but their running/terminal state owns the replica-local
            // failover guard and must update it on every replayed transition.
            DfsMetadataMutation::PutAdvisoryLockState { .. }
            | DfsMetadataMutation::PutNamespacePrepare { .. }
            | DfsMetadataMutation::PutNamespaceRequest { .. }
            | DfsMetadataMutation::PutNamespaceTransactionReceipt { .. }
            | DfsMetadataMutation::PutNamespacePrepareReceipt { .. }
            | DfsMetadataMutation::PutNamespaceRequestReceipt { .. } => {}
            DfsMetadataMutation::PutNamespaceJob { job, .. } => {
                let Some(job) = job.as_ref() else {
                    self.apply_missing_payload_metadata_journal_entry_to_service_state(entry)?;
                    return Ok(());
                };
                self.apply_cross_shard_namespace_job_guard(job);
            }
            DfsMetadataMutation::ApplyNamespaceTransaction { transaction, .. } => {
                let Some(transaction) = transaction.as_ref() else {
                    self.apply_missing_payload_metadata_journal_entry_to_service_state(entry)?;
                    return Ok(());
                };
                self.apply_metadata_namespace_transaction_to_service_state(transaction)?;
            }
        }
        Ok(())
    }

    fn apply_missing_payload_metadata_journal_entry_to_service_state(
        &mut self,
        entry: &DfsMetadataJournalEntry,
    ) -> Result<(), DfsError> {
        match &entry.mutation {
            DfsMetadataMutation::PutFileSystem { file_system_id, .. } => {
                let file_system = self
                    .metadata_backend
                    .as_ref()
                    .map(|backend| backend.get_file_system(file_system_id))
                    .transpose()?
                    .flatten();
                if let Some(file_system) = file_system {
                    self.metadata.add_file_system(file_system);
                }
            }
            DfsMetadataMutation::PutObject { object_id, .. } => {
                self.apply_backend_object_to_service_state(object_id)?;
            }
            DfsMetadataMutation::PutVersion {
                object_id, version, ..
            } => {
                let version_record = self
                    .metadata_backend
                    .as_ref()
                    .map(|backend| backend.get_version(object_id, version))
                    .transpose()?
                    .flatten();
                if let Some(version_record) = version_record {
                    self.apply_metadata_version_to_service_state(&version_record);
                }
            }
            DfsMetadataMutation::PutReceipt {
                object_id,
                version,
                replica_node_id,
                ..
            } => {
                if self
                    .backend_receipt_for_replica(object_id, version, replica_node_id)?
                    .is_some()
                {
                    self.apply_metadata_receipt_to_service_state(
                        object_id,
                        version,
                        replica_node_id,
                    )?;
                }
            }
            DfsMetadataMutation::RemoveReceipt {
                object_id,
                version,
                replica_node_id,
                ..
            } => {
                if let Some(object) = self.metadata.objects.get_mut(object_id)
                    && object.version == *version
                {
                    object
                        .current_locations
                        .retain(|node_id| node_id != replica_node_id);
                }
            }
            DfsMetadataMutation::PutIntent {
                object_id,
                intent_id,
                ..
            } => {
                let intent = self
                    .metadata_backend
                    .as_ref()
                    .map(|backend| backend.get_intent(object_id, intent_id))
                    .transpose()?
                    .flatten();
                if let Some(intent) = intent {
                    self.apply_metadata_intent_to_service_state(&intent)?;
                }
            }
            DfsMetadataMutation::CommitVisibleVersion {
                object_id,
                committed_version,
                ..
            } => {
                self.apply_backend_object_to_service_state(object_id)?;
                let version_record = self
                    .metadata_backend
                    .as_ref()
                    .map(|backend| backend.get_version(object_id, committed_version))
                    .transpose()?
                    .flatten();
                if let Some(version_record) = version_record {
                    self.apply_metadata_version_to_service_state(&version_record);
                }
                self.restore_backend_receipts_for_visible_version(object_id, committed_version)?;
            }
            DfsMetadataMutation::PutConflict {
                object_id,
                conflict_id,
                ..
            } => {
                let conflict = self
                    .metadata_backend
                    .as_ref()
                    .map(|backend| backend.get_conflict(object_id, conflict_id))
                    .transpose()?
                    .flatten();
                if let Some(conflict) = conflict {
                    self.metadata.add_conflict(conflict);
                } else {
                    self.metadata.remove_conflict(object_id, conflict_id);
                }
            }
            DfsMetadataMutation::PutAdvisoryLockState { .. }
            | DfsMetadataMutation::PutNamespacePrepare { .. }
            | DfsMetadataMutation::PutNamespaceRequest { .. }
            | DfsMetadataMutation::PutNamespaceTransactionReceipt { .. }
            | DfsMetadataMutation::PutNamespacePrepareReceipt { .. }
            | DfsMetadataMutation::PutNamespaceRequestReceipt { .. } => {}
            DfsMetadataMutation::PutNamespaceJob { job_id, .. } => {
                let job = self
                    .metadata_backend
                    .as_ref()
                    .map(|backend| backend.get_namespace_job(job_id))
                    .transpose()?
                    .flatten();
                if let Some(job) = job.as_ref() {
                    self.apply_cross_shard_namespace_job_guard(job);
                }
            }
            DfsMetadataMutation::ApplyNamespaceTransaction { transaction_id, .. } => {
                let transaction = self
                    .metadata_backend
                    .as_ref()
                    .map(|backend| backend.get_namespace_transaction(transaction_id))
                    .transpose()?
                    .flatten();
                if let Some(transaction) = transaction {
                    self.apply_metadata_namespace_transaction_to_service_state(&transaction)?;
                } else {
                    return Err(DfsError::Metadata {
                        message: format!(
                            "metadata journal entry {}:{} cannot apply namespace transaction {} without its transaction payload",
                            entry.shard_id.0, entry.sequence, transaction_id
                        ),
                    });
                }
            }
        }
        Ok(())
    }

    fn apply_metadata_namespace_transaction_to_service_state(
        &mut self,
        transaction: &DfsNamespaceTransaction,
    ) -> Result<(), DfsError> {
        let mut objects = transaction.objects.clone();
        objects.sort_by(|left, right| {
            let left_is_tombstone = left.kind == DfsObjectKind::Tombstone;
            let right_is_tombstone = right.kind == DfsObjectKind::Tombstone;
            right_is_tombstone
                .cmp(&left_is_tombstone)
                .then_with(|| left.path.cmp(&right.path))
                .then_with(|| left.object_id.cmp(&right.object_id))
        });
        for object in objects {
            self.apply_metadata_object_to_service_state(object)?;
        }
        for version in &transaction.versions {
            self.apply_metadata_version_to_service_state(version);
        }
        Ok(())
    }

    pub(crate) fn apply_backend_object_to_service_state(
        &mut self,
        object_id: &DfsObjectId,
    ) -> Result<(), DfsError> {
        let object = self
            .metadata_backend
            .as_ref()
            .map(|backend| backend.get_object(object_id))
            .transpose()?
            .flatten();
        if let Some(object) = object {
            self.apply_metadata_object_to_service_state(object)?;
        }
        Ok(())
    }

    fn restore_backend_receipts_for_visible_version(
        &mut self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
    ) -> Result<(), DfsError> {
        for receipt in self.backend_receipts_for_version(object_id, version)? {
            self.apply_metadata_receipt_to_service_state(
                object_id,
                version,
                &receipt.replica_node_id,
            )?;
        }
        Ok(())
    }

    pub(crate) fn apply_metadata_object_to_service_state(
        &mut self,
        mut object: DfsObjectMetadata,
    ) -> Result<(), DfsError> {
        normalize_current_locations(&mut object.current_locations);
        self.transient_metadata_object_ids.remove(&object.object_id);
        self.transient_metadata_predecessor_object_ids
            .remove(&object.object_id);
        if !self
            .metadata
            .filesystems
            .contains_key(&object.file_system_id)
        {
            self.metadata.add_file_system(DfsFileSystem {
                file_system_id: object.file_system_id.clone(),
                display_name: object.file_system_id.to_string(),
                owner_node_id: object.owner_node_id.clone(),
                subtree_policies: Vec::new(),
            });
        }
        self.coordinator.set_committed_version(
            object.file_system_id.clone(),
            object.object_id.clone(),
            object.version.clone(),
        );
        self.track_metadata_tombstone(&object);
        self.metadata.add_object(object)?;
        Ok(())
    }

    pub(crate) fn apply_metadata_version_to_service_state(
        &mut self,
        version: &DfsObjectVersionRecord,
    ) {
        if !version.visible {
            return;
        }
        let Some(object) = self.metadata.objects.get_mut(&version.object_id) else {
            return;
        };
        if object.version != version.version {
            return;
        }
        if object.content_hash.is_none() {
            object.content_hash = version.content_hash.clone();
        }
        if version.size_bytes.is_some() {
            object.content_size_bytes = version.size_bytes;
        }
        if !version.locations.is_empty() {
            object.current_locations = version.locations.clone();
            normalize_current_locations(&mut object.current_locations);
        }
        self.coordinator.set_committed_version(
            object.file_system_id.clone(),
            object.object_id.clone(),
            object.version.clone(),
        );
    }

    fn apply_metadata_intent_to_service_state(
        &mut self,
        intent_record: &DfsWriteIntentRecord,
    ) -> Result<(), DfsError> {
        let intent = distributed_intent_from_record(intent_record);
        match self.coordinator.restore_intent(intent) {
            Ok(()) => {}
            Err(DfsDistributedError::PendingIntentConflict { .. }) => {
                self.mark_replayed_intent_as_rejected(intent_record)?;
                return Ok(());
            }
            Err(
                DfsDistributedError::DuplicateIntentId { .. }
                | DfsDistributedError::DuplicateIntent { .. },
            ) => {
                if self
                    .coordinator
                    .intent(&intent_record.intent_id)
                    .is_some_and(|intent| distributed_intent_matches_record(intent, intent_record))
                {
                    // Same intent replayed after local state had already advanced.
                } else if matches!(intent_record.status, DfsWriteIntentStatus::Committed)
                    && self
                        .current_read_object_metadata(&intent_record.object_id)
                        .is_ok_and(|object| {
                            object.version == intent_record.target_version
                                && intent_record.content_hash.as_deref().is_none_or(
                                    |content_hash| {
                                        object.content_hash.as_deref() == Some(content_hash)
                                    },
                                )
                        })
                {
                    self.restore_visible_committed_object_location_receipts(intent_record)?;
                } else {
                    self.mark_replayed_intent_as_rejected(intent_record)?;
                    return Ok(());
                }
            }
            Err(DfsDistributedError::BaseVersionConflict { .. }) => {
                if self
                    .current_read_object_metadata(&intent_record.object_id)
                    .is_ok_and(|object| {
                        object.version == intent_record.target_version
                            && intent_record
                                .content_hash
                                .as_deref()
                                .is_none_or(|content_hash| {
                                    object.content_hash.as_deref() == Some(content_hash)
                                })
                    })
                {
                    if matches!(intent_record.status, DfsWriteIntentStatus::Committed) {
                        self.restore_visible_committed_object_location_receipts(intent_record)?;
                    }
                    return Ok(());
                }
                // Journal replay can revisit an intent that later history in
                // the same stream already superseded — either because the
                // backend applied the whole batch before service-state replay
                // or because a republished batch duplicates entries this node
                // replayed earlier. The backend's current intent record is
                // authoritative: a terminal record makes the replayed entry
                // historical rather than inconsistent.
                if self.backend_intent_record_is_terminal(
                    &intent_record.object_id,
                    &intent_record.intent_id,
                )? {
                    return Ok(());
                }
                self.mark_replayed_intent_as_rejected(intent_record)?;
                return Ok(());
            }
            Err(error) => return Err(error.into()),
        }

        if matches!(
            intent_record.status,
            DfsWriteIntentStatus::Staging
                | DfsWriteIntentStatus::ReceiptsPending
                | DfsWriteIntentStatus::Committed
        ) && let Some(content_hash) = intent_record.content_hash.clone()
            && !self.pending_writes.contains_key(&intent_record.intent_id)
        {
            let size_bytes = self
                .pending_write_size_hint(
                    &intent_record.object_id,
                    &intent_record.target_version,
                    &content_hash,
                )?
                .unwrap_or(0);
            self.pending_writes.insert(
                intent_record.intent_id.clone(),
                PendingNetworkedWrite::new(
                    intent_record.object_id.clone(),
                    intent_record.target_version.clone(),
                    content_hash,
                    size_bytes,
                )
                .with_foreground_replica_repair(false),
            );
        }
        Ok(())
    }

    fn mark_replayed_intent_as_rejected(
        &mut self,
        intent_record: &DfsWriteIntentRecord,
    ) -> Result<(), DfsError> {
        let mut rejected_record = intent_record.clone();
        rejected_record.status = DfsWriteIntentStatus::Rejected;
        rejected_record.updated_at = Some(OffsetDateTime::now_utc());
        if let Some(backend) = self.metadata_backend.as_mut() {
            backend.put_intent(&rejected_record)?;
        }
        self.remove_pending_write_state(&rejected_record.intent_id);
        Ok(())
    }

    fn apply_metadata_receipt_to_service_state(
        &mut self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
    ) -> Result<(), DfsError> {
        let intent_ids = self.backend_intent_ids_for_version(object_id, version)?;
        for intent_id in intent_ids {
            self.restore_backend_durable_receipt(&intent_id, replica_node_id.clone(), true)?;
        }
        Ok(())
    }

    pub(crate) fn pending_write_size_hint(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        content_hash: &str,
    ) -> Result<Option<u64>, DfsError> {
        if let Some(backend) = self.metadata_backend.as_ref()
            && let Some(version_record) = backend.get_version(object_id, version)?
            && version_record.content_hash.as_deref() == Some(content_hash)
            && version_record.size_bytes.is_some()
        {
            return Ok(version_record.size_bytes);
        }
        match self
            .content
            .read_manifest(object_id.as_str(), version.as_str())
        {
            Ok(manifest) if manifest.content_hash == content_hash => Ok(Some(manifest.size_bytes)),
            Ok(_) => Ok(None),
            Err(ContentStoreError::Io { source, .. })
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                Ok(None)
            }
            Err(error) => Err(error.into()),
        }
    }
}
