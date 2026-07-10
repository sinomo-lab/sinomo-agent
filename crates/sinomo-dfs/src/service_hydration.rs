//! Metadata backend hydration and journal replay for the networked DFS service.
//!
//! This module keeps durable metadata replay, missing-payload journal recovery,
//! and in-memory service-state restoration out of the live data-plane protocol
//! code.

use std::collections::BTreeSet;

use sinomo_core::DfsWriteReceipt;
use sinomo_core::events::EventId;
use time::OffsetDateTime;

use crate::metadata_pages::{try_walk_metadata_pages, walk_metadata_pages};
use crate::transfer::PendingNetworkedWrite;
use crate::*;

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

    pub fn hydrate_from_metadata_backend(&mut self) -> Result<(), DfsError> {
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
        self.coordinator = DistributedEventualCoordinator::new();
        self.pending_writes.clear();
        self.pending_reads.clear();
        self.incoming_read_transfers.clear();
        self.incoming_read_memory_transfers.clear();
        self.incoming_fetch_transfers.clear();
        self.incoming_blob_transfers.clear();
        self.incoming_patch_transfers.clear();
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

                    self.add_backend_conflict_pages_to_memory(&object.object_id)?;
                }
                Ok::<_, DfsError>(page.next_cursor)
            })?;
        }

        let mut pending_write_records = Vec::new();
        try_walk_metadata_pages(|intent_scan_cursor| {
            let intent_page = self
                .metadata_backend
                .as_ref()
                .expect("metadata backend checked above")
                .write_intents_page(intent_scan_cursor, DFS_METADATA_OBJECT_INTENT_PAGE_SIZE)?;
            for mut intent_record in intent_page.intents {
                if self.metadata.object(&intent_record.object_id).is_none() {
                    continue;
                }
                self.coordinator
                    .observe_restored_intent_id(&intent_record.intent_id);
                if !write_intent_status_requires_hydrate_restore(&intent_record.status) {
                    continue;
                }
                self.repair_visible_committed_pending_intent_record(&mut intent_record)?;
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
                            self.restore_visible_committed_object_location_receipts_with_repair_rebuild(
                                &intent_record,
                                false,
                            )?;
                            continue;
                        }
                        self.mark_replayed_intent_as_rejected(&intent_record)?;
                        continue;
                    }
                    Err(error) => return Err(DfsError::from(error)),
                }
                self.restore_backend_receipt_pages_for_intent(&intent_record, false)?;
                self.restore_visible_committed_object_location_receipts_with_repair_rebuild(
                    &intent_record,
                    false,
                )?;
                if matches!(
                    intent_record.status,
                    DfsWriteIntentStatus::Staging
                        | DfsWriteIntentStatus::ReceiptsPending
                        | DfsWriteIntentStatus::Committed
                ) {
                    pending_write_records.push(intent_record);
                }
            }
            Ok(intent_page.next_cursor)
        })?;
        self.coordinator.rebuild_repair_jobs_after_bulk_restore();

        let repair_intent_ids: BTreeSet<String> = self
            .coordinator
            .repair_jobs()
            .iter()
            .map(|job| job.intent_id.clone())
            .collect();
        let mut recovered_commit_ready_intent_ids = Vec::new();
        for intent_record in pending_write_records {
            let Some(restored_intent) = self.coordinator.intent(&intent_record.intent_id) else {
                continue;
            };
            if !distributed_intent_matches_record(restored_intent, &intent_record) {
                continue;
            }
            let locally_coordinated_intent = intent_record.coordinator_node_id == self.node_id;
            let needs_pending_write = locally_coordinated_intent
                && (matches!(
                    intent_record.status,
                    DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
                ) || (matches!(intent_record.status, DfsWriteIntentStatus::Committed)
                    && repair_intent_ids.contains(&intent_record.intent_id)));
            if needs_pending_write && let Some(content_hash) = intent_record.content_hash.clone() {
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
                    ),
                );
                if matches!(
                    intent_record.status,
                    DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
                ) && self
                    .coordinator
                    .is_commit_ready(&intent_record.intent_id)
                    .unwrap_or(false)
                {
                    recovered_commit_ready_intent_ids.push(intent_record.intent_id.clone());
                }
            }
        }
        for intent_id in recovered_commit_ready_intent_ids {
            self.apply_committed_intent(&intent_id)?;
            self.cleanup_committed_pending_write_if_repaired(&intent_id);
        }

        self.seed_local_metadata_journal_cursors_from_backend()?;
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
        for receipt in self.backend_receipts_for_version(&object.object_id, &object.version)? {
            if receipt.content_hash.as_deref() != Some(content_hash) {
                continue;
            }
            existing_receipts.insert(receipt.replica_node_id.clone());
            self.restore_backend_durable_receipt_with_repair_rebuild(
                &intent_record.intent_id,
                receipt.replica_node_id,
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
                backend.put_receipt(&DfsReplicaReceiptRecord {
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
                })?;
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
            // metadata backend; namespace jobs likewise have no legacy
            // in-memory mirror.
            DfsMetadataMutation::PutAdvisoryLockState { .. }
            | DfsMetadataMutation::PutNamespaceJob { .. } => {}
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
            | DfsMetadataMutation::PutNamespaceJob { .. } => {}
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
                ),
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
