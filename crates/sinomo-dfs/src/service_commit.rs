//! Committed write lifecycle and durable metadata persistence helpers.
//!
//! This module owns the service-level transition from staged DFS write
//! intents into visible versions. It also centralizes metadata-backend
//! receipt persistence, local metadata updates, command content storage,
//! and write-outcome calculation used by write, transfer, protocol, and
//! POSIX layers.

use std::collections::BTreeSet;
use std::io::Read;
use std::time::Instant;

use sinomo_core::{DfsObjectPolicy, DfsWriteReceipt, DfsWriteRequest};
use sinomo_events::EventId;
use sinomo_node::NodeId;
use time::OffsetDateTime;

use crate::*;

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub(crate) fn apply_committed_intent(&mut self, intent_id: &str) -> Result<(), DfsError> {
        self.apply_committed_intent_with_repair_mode(intent_id, true)
    }

    fn apply_committed_intent_with_repair_mode(
        &mut self,
        intent_id: &str,
        foreground_replica_repair: bool,
    ) -> Result<(), DfsError> {
        let Some(intent) = self.coordinator.intent(intent_id).cloned() else {
            return Err(DfsError::Distributed {
                message: format!("unknown committed intent: {intent_id}"),
            });
        };
        if matches!(intent.state, DfsWriteIntentState::Committed) {
            self.clear_active_replica_stage(&intent.object_id, &intent.target_version);
            return Ok(());
        }
        if !self.coordinator.is_commit_ready(intent_id)? {
            return Err(DfsError::Distributed {
                message: format!("intent {intent_id} is not ready to commit"),
            });
        }
        self.ensure_intent_epoch_current(&intent)?;
        let pending =
            self.pending_writes
                .get(intent_id)
                .cloned()
                .ok_or_else(|| DfsError::Distributed {
                    message: format!("missing pending write for committed intent: {intent_id}"),
                })?;
        if intent.object_id != pending.object_id || intent.target_version != pending.version {
            return Err(DfsError::Distributed {
                message: format!(
                    "committed intent {intent_id} state does not match pending write: intent {} {}, pending {} {}",
                    intent.object_id, intent.target_version, pending.object_id, pending.version
                ),
            });
        }
        let durable_receipts: Vec<DistributedReplicaReceipt> = self
            .coordinator
            .receipts_for_intent(intent_id)
            .into_iter()
            .cloned()
            .collect();
        for receipt in durable_receipts {
            self.put_metadata_receipt(
                &intent.file_system_id,
                &receipt.object_id,
                &receipt.version,
                &receipt.replica_node_id,
                &pending.content_hash,
            )?;
        }
        let write_time = dfs_now_unix_nanos();
        self.commit_visible_version_in_metadata_backend(&intent, &pending, write_time)?;
        self.coordinator
            .commit_intent(intent_id)
            .map_err(DfsError::from)?;
        let committed_intent =
            self.coordinator
                .intent(intent_id)
                .cloned()
                .ok_or_else(|| DfsError::Distributed {
                    message: format!("missing committed intent after finalize: {intent_id}"),
                })?;
        self.put_metadata_intent(&committed_intent, Some(&pending.content_hash))?;
        self.commit_local_metadata(
            &pending.object_id,
            &pending.version,
            &pending.content_hash,
            write_time,
        )?;
        self.clear_active_replica_stage(&pending.object_id, &pending.version);

        let durable_nodes: BTreeSet<NodeId> = self
            .coordinator
            .receipts_for_intent(intent_id)
            .into_iter()
            .map(|receipt| receipt.replica_node_id.clone())
            .collect();
        if let Some(object) = self.metadata.objects.get_mut(&pending.object_id) {
            object.current_locations = durable_nodes.iter().cloned().collect();
        }

        let commit_targets: Vec<NodeId> = durable_nodes
            .iter()
            .filter(|node_id| *node_id != &self.node_id && *node_id != &intent.requesting_node_id)
            .cloned()
            .collect();
        let committed_metadata = Some(self.current_read_object_metadata(&pending.object_id)?);
        let authority_shard =
            self.command_authority_shard_for_object(committed_metadata.as_ref())?;
        for replica_node_id in commit_targets {
            let request_id = self.next_request_id();
            self.send_best_effort_message_if_routable(
                &replica_node_id,
                DfsNetworkMessage::CommitVersion(DfsCommitVersionCommand {
                    request_id,
                    intent_id: intent_id.to_string(),
                    file_system_id: intent.file_system_id.clone(),
                    object_id: pending.object_id.clone(),
                    expected_base_version: intent.base_version.clone(),
                    version: pending.version.clone(),
                    content_hash: pending.content_hash.clone(),
                    object_metadata: committed_metadata.clone(),
                    authority_shard: authority_shard.clone(),
                }),
            )?;
        }

        if !foreground_replica_repair {
            return Ok(());
        }

        let repair_jobs: Vec<DistributedRepairJob> = self
            .coordinator
            .repair_jobs()
            .iter()
            .filter(|job| job.intent_id == intent_id)
            .cloned()
            .collect();
        if !repair_jobs.is_empty() && !self.pending_repair_is_locally_authoritative(&pending)? {
            self.suppress_repair_retries_for_lost_authority(intent_id, None, true);
            return Ok(());
        }
        for job in repair_jobs {
            let replica_node_id = job.missing_replica_node_id.clone();
            if !self.repair_retry_is_due(intent_id, &replica_node_id) {
                continue;
            }
            if replica_node_id == self.node_id {
                if self
                    .local_object_version_manifest_size_if_matches(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                    )
                    .is_some()
                {
                    self.record_local_pending_write_receipt_if_desired(
                        intent_id,
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                    )?;
                    continue;
                }
                let fetch_requested = self.request_repair_blob_from_alternate(&job, &pending)?;
                if !fetch_requested {
                    self.record_repair_retry_scheduled(intent_id, &replica_node_id);
                }
                continue;
            }
            if self.open_pending_write_blob_stream(&pending).is_err() {
                let fetch_requested = self.request_repair_blob_from_alternate(&job, &pending)?;
                if !fetch_requested {
                    self.record_repair_retry_scheduled(intent_id, &replica_node_id);
                }
                continue;
            }
            let start_chunk_index = pending
                .repair_next_chunk_indexes
                .get(&replica_node_id)
                .copied()
                .unwrap_or(0);
            let request_id = pending
                .repair_inflight_request_ids
                .get(&replica_node_id)
                .cloned()
                .unwrap_or_else(|| self.next_request_id());
            let max_chunks = self.adaptive_stage_chunk_window(&replica_node_id, pending.size_bytes);
            let outcome = self.send_repair_replica_window_if_routable(
                &replica_node_id,
                request_id.clone(),
                intent_id,
                &intent.file_system_id,
                &pending.object_id,
                intent.base_version.clone(),
                &pending.version,
                &pending.content_hash,
                pending.size_bytes,
                start_chunk_index,
                max_chunks,
                committed_metadata.clone(),
            )?;
            if outcome.sent {
                self.record_repair_window_progress(
                    intent_id,
                    &replica_node_id,
                    request_id,
                    outcome,
                );
            } else {
                self.record_repair_retry_scheduled(intent_id, &replica_node_id);
            }
        }
        Ok(())
    }

    pub(crate) fn try_apply_committed_intent_if_ready(
        &mut self,
        intent_id: &str,
    ) -> Result<(), DfsError> {
        self.try_apply_committed_intent_if_ready_with_repair_mode(intent_id, true)
    }

    pub(crate) fn try_apply_committed_intent_if_ready_with_repair_mode(
        &mut self,
        intent_id: &str,
        foreground_replica_repair: bool,
    ) -> Result<(), DfsError> {
        if self.coordinator.is_commit_ready(intent_id)? {
            self.apply_committed_intent_with_repair_mode(intent_id, foreground_replica_repair)?;
        }
        Ok(())
    }

    pub(crate) fn normalize_desired_replica_nodes(
        &self,
        object: &DfsObjectMetadata,
        desired_replica_nodes: Vec<NodeId>,
    ) -> Vec<NodeId> {
        let mut normalized = desired_replica_nodes;
        if matches!(
            object.policy,
            DfsObjectPolicy::ReplicatedEventual | DfsObjectPolicy::ReplicatedConfirmed
        ) && !normalized.contains(&object.owner_node_id)
        {
            normalized.insert(0, object.owner_node_id.clone());
        }
        let mut deduped = Vec::new();
        for node_id in normalized {
            if !deduped.contains(&node_id) {
                deduped.push(node_id);
            }
        }
        deduped
    }

    pub(crate) fn ensure_no_backend_pending_write_intent(
        &self,
        object_id: &DfsObjectId,
        target_version: &DfsVersion,
        excluding_intent_id: Option<&str>,
    ) -> Result<(), DfsDistributedError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(());
        };
        let mut intent_cursor = None;
        loop {
            let page = backend
                .intents_for_object_page(
                    object_id,
                    intent_cursor.as_ref(),
                    DFS_METADATA_OBJECT_INTENT_PAGE_SIZE,
                )
                .map_err(|error| DfsDistributedError::InvalidPolicy {
                    reason: error.to_string(),
                })?;
            for intent in page.intents {
                if excluding_intent_id == Some(intent.intent_id.as_str()) {
                    continue;
                }
                if matches!(
                    intent.status,
                    DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
                ) {
                    return Err(DfsDistributedError::PendingIntentConflict {
                        object_id: object_id.clone(),
                        pending_intent_id: intent.intent_id.clone(),
                        pending_target_version: intent.target_version.clone(),
                        requested_target_version: target_version.clone(),
                    });
                }
            }
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            intent_cursor = Some(next_cursor);
        }
        Ok(())
    }

    pub(crate) fn record_active_replica_stage(
        &mut self,
        object_id: &DfsObjectId,
        intent_id: &str,
        version: &DfsVersion,
        content_hash: &str,
        requester_node_id: &NodeId,
    ) {
        self.active_replica_stages.insert(
            object_id.clone(),
            ActiveReplicaStage {
                intent_id: intent_id.to_string(),
                version: version.clone(),
                content_hash: content_hash.to_string(),
                requester_node_id: requester_node_id.clone(),
            },
        );
    }

    pub(crate) fn clear_active_replica_stage(
        &mut self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
    ) {
        if self
            .active_replica_stages
            .get(object_id)
            .is_some_and(|stage| &stage.version == version)
        {
            self.active_replica_stages.remove(object_id);
        }
    }

    pub(crate) fn restore_pending_write_for_intent_if_missing(
        &mut self,
        intent_id: &str,
        object_id: &DfsObjectId,
    ) -> Result<(), DfsError> {
        if self.pending_writes.contains_key(intent_id) {
            return Ok(());
        }
        let Some(record) = (match self.metadata_backend.as_ref() {
            Some(backend) => backend.get_intent(object_id, intent_id)?,
            None => None,
        }) else {
            let _ = self
                .coordinator
                .intent(intent_id)
                .ok_or_else(|| DfsError::Distributed {
                    message: format!("unknown pending write intent: {intent_id}"),
                })?;
            return Ok(());
        };
        if self.coordinator.intent(intent_id).is_none() {
            self.coordinator
                .restore_intent(distributed_intent_from_record(&record))?;
            if let Some(backend) = self.metadata_backend.as_ref() {
                let mut receipt_cursor = None;
                loop {
                    let page = backend.receipts_for_version_page(
                        &record.object_id,
                        &record.target_version,
                        receipt_cursor.as_ref(),
                        DFS_METADATA_OBJECT_RECEIPT_PAGE_SIZE,
                    )?;
                    for receipt in page.receipts {
                        if let Some(expected_hash) = record.content_hash.as_deref()
                            && receipt.content_hash.as_deref() != Some(expected_hash)
                        {
                            continue;
                        }
                        match self
                            .coordinator
                            .restore_durable_receipt(intent_id, receipt.replica_node_id)
                        {
                            Ok(()) => {}
                            Err(
                                DfsDistributedError::UnexpectedReplica { .. }
                                | DfsDistributedError::IntentClosed { .. },
                            ) => continue,
                            Err(error) => return Err(error.into()),
                        }
                    }
                    let Some(next_cursor) = page.next_cursor else {
                        break;
                    };
                    receipt_cursor = Some(next_cursor);
                }
            }
        }
        let intent =
            self.coordinator
                .intent(intent_id)
                .cloned()
                .ok_or_else(|| DfsError::Distributed {
                    message: format!("unknown pending write intent: {intent_id}"),
                })?;
        if !distributed_intent_matches_record(&intent, &record) {
            return Err(DfsError::Distributed {
                message: format!(
                    "stored intent record does not match coordinator state for {intent_id}"
                ),
            });
        }
        if self
            .current_read_object_metadata(&record.object_id)
            .is_err()
            && let Some(object) = record.object_metadata.clone()
        {
            self.add_transient_object_metadata(object)?;
        }
        let Some(content_hash) = record.content_hash.clone() else {
            return Ok(());
        };
        let is_local_coordinator = record.coordinator_node_id == self.node_id;
        let local_manifest_size = self.local_object_version_manifest_size_if_matches(
            &record.object_id,
            &record.target_version,
            &content_hash,
        );
        let is_local_desired_replica_with_content = record
            .desired_replica_node_ids
            .iter()
            .any(|node_id| node_id == &self.node_id)
            && local_manifest_size.is_some();
        let has_repair_job = self
            .coordinator
            .repair_jobs()
            .iter()
            .any(|job| job.intent_id == intent_id);
        let is_local_authoritative_repair = if has_repair_job {
            let repair_object = self.current_read_object_metadata(&record.object_id)?;
            self.is_local_metadata_primary_for_object(&repair_object)?
        } else {
            false
        };
        if !is_local_coordinator
            && !is_local_desired_replica_with_content
            && !is_local_authoritative_repair
        {
            return Ok(());
        }
        let size_bytes = self
            .pending_write_size_hint(&record.object_id, &record.target_version, &content_hash)?
            .or(local_manifest_size)
            .unwrap_or(0);
        self.pending_writes.insert(
            intent_id.to_string(),
            PendingNetworkedWrite::new(
                record.object_id,
                record.target_version,
                content_hash,
                size_bytes,
            ),
        );
        Ok(())
    }

    pub(crate) fn restore_pending_writes_for_repair_jobs(&mut self) -> Result<(), DfsError> {
        let repair_jobs = self.coordinator.repair_jobs().to_vec();
        for job in repair_jobs {
            let object = self.current_read_object_metadata(&job.object_id)?;
            if self.is_local_metadata_primary_for_object(&object)? {
                self.restore_pending_write_for_intent_if_missing(&job.intent_id, &job.object_id)?;
            }
        }
        Ok(())
    }

    pub(crate) fn commit_local_metadata(
        &mut self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        content_hash: &str,
        write_time_unix_nanos: u64,
    ) -> Result<DfsObjectMetadata, DfsError> {
        let content_size_bytes = self
            .content
            .read_manifest(object_id.as_str(), version.as_str())
            .ok()
            .filter(|manifest| manifest.content_hash == content_hash)
            .map(|manifest| manifest.size_bytes);
        if let Some(backend) = self.metadata_backend.as_ref()
            && let Some(backend_object) = backend.get_object(object_id)?
        {
            self.metadata.add_object(backend_object)?;
        }
        let object =
            self.metadata
                .objects
                .get_mut(object_id)
                .ok_or_else(|| DfsError::UnknownObject {
                    object_id: object_id.clone(),
                })?;
        let content_unchanged = object
            .content_hash
            .as_ref()
            .is_some_and(|existing| existing == content_hash);
        let already_current = object.version == *version && content_unchanged;
        object.version = version.clone();
        object.content_hash = Some(content_hash.to_string());
        if content_size_bytes.is_some() {
            object.content_size_bytes = content_size_bytes;
        }
        if !content_unchanged {
            object.modified_at_unix_nanos = write_time_unix_nanos;
            object.changed_at_unix_nanos = write_time_unix_nanos;
        }
        if !already_current {
            object.last_modified_by_event_id = Some(EventId::generate());
        }
        if !object.current_locations.contains(&self.node_id) {
            object.current_locations.push(self.node_id.clone());
        }
        self.coordinator.set_committed_version(
            object.file_system_id.clone(),
            object.object_id.clone(),
            object.version.clone(),
        );
        let object = object.clone();
        self.track_metadata_tombstone(&object);
        Ok(object.clone())
    }

    pub(crate) fn store_conflicting_write_content_hash(
        &self,
        request: &DfsWriteRequest,
        bytes: &[u8],
    ) -> Result<Option<String>, DfsError> {
        match self.content.put_object_version(
            request.object_id.as_str(),
            request.target_version.as_str(),
            bytes,
        ) {
            Ok(manifest) => Ok(Some(manifest.content_hash)),
            Err(ContentStoreError::VersionHashConflict { new_hash, .. }) => Ok(Some(new_hash)),
            Err(error) => Err(error.into()),
        }
    }

    pub(crate) fn store_conflicting_write_reader_content_hash<R: Read>(
        &self,
        request: &DfsWriteRequest,
        reader: R,
    ) -> Result<Option<String>, DfsError> {
        match self.content.put_object_version_from_reader(
            request.object_id.as_str(),
            request.target_version.as_str(),
            reader,
        ) {
            Ok(manifest) => Ok(Some(manifest.content_hash)),
            Err(ContentStoreError::VersionHashConflict { new_hash, .. }) => Ok(Some(new_hash)),
            Err(error) => Err(error.into()),
        }
    }

    pub(crate) fn ensure_replicated_confirmed_object(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<(), DfsError> {
        let object = self.current_read_object_metadata(object_id)?;
        if object.policy != DfsObjectPolicy::ReplicatedConfirmed {
            return Err(DfsError::UnsupportedPolicy {
                policy: object.policy.clone(),
            });
        }
        Ok(())
    }

    pub(crate) fn confirmed_locations_for_object(
        &self,
        object: &DfsObjectMetadata,
        required_replica_node_ids: &[NodeId],
    ) -> Result<Vec<NodeId>, DfsError> {
        let mut confirmed_locations: BTreeSet<NodeId> = object
            .current_locations
            .iter()
            .filter(|node_id| required_replica_node_ids.contains(node_id))
            .cloned()
            .collect();

        if let Some(backend) = self.metadata_backend.as_ref() {
            confirmed_locations.clear();
            let mut receipt_cursor = None;
            loop {
                let page = backend.receipts_for_version_page(
                    &object.object_id,
                    &object.version,
                    receipt_cursor.as_ref(),
                    DFS_METADATA_OBJECT_RECEIPT_PAGE_SIZE,
                )?;
                for receipt in page.receipts {
                    if !required_replica_node_ids.contains(&receipt.replica_node_id) {
                        continue;
                    }
                    if let Some(expected_hash) = object.content_hash.as_deref() {
                        if receipt.content_hash.as_deref() != Some(expected_hash) {
                            continue;
                        }
                    }
                    confirmed_locations.insert(receipt.replica_node_id);
                }
                let Some(next_cursor) = page.next_cursor else {
                    break;
                };
                receipt_cursor = Some(next_cursor);
            }
        }

        Ok(confirmed_locations.into_iter().collect())
    }

    pub(crate) fn mark_repaired_location(
        &mut self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
        content_hash: &str,
    ) -> Result<(), DfsError> {
        let mut object = self.current_read_object_metadata(object_id)?;
        if &object.version != version {
            return Ok(());
        }
        if let Some(expected_hash) = object.content_hash.as_deref() {
            if expected_hash != content_hash {
                return Err(DfsError::ContentHashMismatch {
                    expected: expected_hash.to_string(),
                    actual: content_hash.to_string(),
                });
            }
        }
        if object.current_locations.contains(replica_node_id) {
            self.metadata.add_object(object.clone())?;
            self.put_visible_object_in_metadata_backend(&object)?;
            return Ok(());
        }
        object.current_locations.push(replica_node_id.clone());
        self.metadata.add_object(object.clone())?;
        self.put_visible_object_in_metadata_backend(&object)?;
        Ok(())
    }

    pub(crate) fn put_metadata_receipt(
        &mut self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
        content_hash: &str,
    ) -> Result<bool, DfsError> {
        self.ensure_metadata_receipt_accepts(object_id, version, replica_node_id, content_hash)?;
        let object = self.current_read_object_metadata(object_id).ok();
        let inserted = {
            let Some(backend) = self.metadata_backend.as_mut() else {
                return Ok(false);
            };
            let mut existing_matching_receipt = false;
            let mut receipt_cursor = None;
            loop {
                let page = backend.receipts_for_version_page(
                    object_id,
                    version,
                    receipt_cursor.as_ref(),
                    DFS_METADATA_OBJECT_RECEIPT_PAGE_SIZE,
                )?;
                for receipt in page.receipts {
                    if receipt.replica_node_id != *replica_node_id {
                        continue;
                    }
                    if receipt.content_hash.as_deref() == Some(content_hash) {
                        existing_matching_receipt = true;
                    }
                    break;
                }
                if existing_matching_receipt {
                    break;
                }
                let Some(next_cursor) = page.next_cursor else {
                    break;
                };
                receipt_cursor = Some(next_cursor);
            }
            if existing_matching_receipt {
                false
            } else {
                let receipt = DfsReplicaReceiptRecord {
                    receipt: DfsWriteReceipt {
                        file_system_id: file_system_id.clone(),
                        object_id: object_id.clone(),
                        committed_version: version.clone(),
                        owner_node_id: self.node_id.clone(),
                        event_id: EventId::generate(),
                    },
                    replica_node_id: replica_node_id.clone(),
                    content_hash: Some(content_hash.to_string()),
                    received_at: OffsetDateTime::now_utc(),
                };
                backend.put_receipt_for_object(object.as_ref(), &receipt)?;
                true
            }
        };
        match self.mark_repaired_location(object_id, version, replica_node_id, content_hash) {
            Ok(()) | Err(DfsError::UnknownObject { .. }) => {}
            Err(error) => return Err(error),
        }
        Ok(inserted)
    }

    pub(crate) fn ensure_metadata_receipt_accepts(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
        content_hash: &str,
    ) -> Result<(), DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(());
        };
        let mut receipt_cursor = None;
        loop {
            let page = backend.receipts_for_version_page(
                object_id,
                version,
                receipt_cursor.as_ref(),
                DFS_METADATA_OBJECT_RECEIPT_PAGE_SIZE,
            )?;
            for receipt in page.receipts {
                if receipt.replica_node_id != *replica_node_id {
                    continue;
                }
                if receipt.content_hash.as_deref() == Some(content_hash) {
                    return Ok(());
                }
                return Err(DfsError::Distributed {
                    message: format!(
                        "conflicting DFS replica receipt for {object_id} {version} from {replica_node_id}: existing {:?}, new {content_hash}",
                        receipt.content_hash
                    ),
                });
            }
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            receipt_cursor = Some(next_cursor);
        }
        Ok(())
    }

    pub(crate) fn put_metadata_intent(
        &mut self,
        intent: &DistributedWriteIntent,
        content_hash: Option<&str>,
    ) -> Result<(), DfsError> {
        let object = self.current_object_for_write(&intent.object_id).ok();
        let Some(backend) = self.metadata_backend.as_mut() else {
            return Ok(());
        };
        let mut record = write_intent_record(intent, content_hash, object.clone());
        if let Some(existing) = backend.get_intent(&intent.object_id, &intent.intent_id)? {
            record.created_at = existing.created_at;
            let mut comparable_record = record.clone();
            comparable_record.updated_at = existing.updated_at;
            if comparable_record == existing {
                return Ok(());
            }
        }
        backend.put_intent_for_object(object.as_ref(), &record)?;
        Ok(())
    }

    pub(crate) fn put_metadata_intent_and_receipts(
        &mut self,
        intent: &DistributedWriteIntent,
        content_hash: &str,
        replica_node_ids: &[NodeId],
    ) -> Result<(), DfsError> {
        let object = self.current_object_for_write(&intent.object_id).ok();
        for replica_node_id in replica_node_ids {
            self.ensure_metadata_receipt_accepts(
                &intent.object_id,
                &intent.target_version,
                replica_node_id,
                content_hash,
            )?;
        }

        let Some(backend) = self.metadata_backend.as_mut() else {
            return Ok(());
        };
        let owner_node_id = self.node_id.clone();
        let mut intent_record = write_intent_record(intent, Some(content_hash), object.clone());
        if let Some(existing) = backend.get_intent(&intent.object_id, &intent.intent_id)? {
            intent_record.created_at = existing.created_at;
            let mut comparable_record = intent_record.clone();
            comparable_record.updated_at = existing.updated_at;
            if comparable_record == existing && replica_node_ids.is_empty() {
                return Ok(());
            }
        }
        let receipts = replica_node_ids
            .iter()
            .map(|replica_node_id| DfsReplicaReceiptRecord {
                receipt: DfsWriteReceipt {
                    file_system_id: intent.file_system_id.clone(),
                    object_id: intent.object_id.clone(),
                    committed_version: intent.target_version.clone(),
                    owner_node_id: owner_node_id.clone(),
                    event_id: EventId::generate(),
                },
                replica_node_id: replica_node_id.clone(),
                content_hash: Some(content_hash.to_string()),
                received_at: OffsetDateTime::now_utc(),
            })
            .collect::<Vec<_>>();
        backend.put_intent_and_receipts_for_object(object.as_ref(), &intent_record, &receipts)?;
        for replica_node_id in replica_node_ids {
            match self.mark_repaired_location(
                &intent.object_id,
                &intent.target_version,
                replica_node_id,
                content_hash,
            ) {
                Ok(()) | Err(DfsError::UnknownObject { .. }) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    pub(crate) fn put_visible_object_in_metadata_backend(
        &mut self,
        object: &DfsObjectMetadata,
    ) -> Result<(), DfsError> {
        let mut object = object.clone();
        normalize_current_locations(&mut object.current_locations);
        let size_bytes = self.object_content_size_hint(&object)?;
        let Some(backend) = self.metadata_backend.as_mut() else {
            return Ok(());
        };
        let file_system = backend
            .get_file_system(&object.file_system_id)?
            .unwrap_or_else(|| DfsFileSystem {
                file_system_id: object.file_system_id.clone(),
                display_name: object.file_system_id.to_string(),
                owner_node_id: object.owner_node_id.clone(),
                subtree_policies: Vec::new(),
            });
        backend.ensure_file_system(&file_system)?;
        if backend.get_object(&object.object_id)?.as_ref() != Some(&object) {
            backend.put_object(&object)?;
        }
        put_visible_version_record_if_changed(
            backend.as_mut(),
            object_version_record(&object, size_bytes),
        )?;
        Ok(())
    }

    pub(crate) fn put_visible_object_and_version_in_metadata_backend(
        &mut self,
        object: &DfsObjectMetadata,
        size_bytes: Option<u64>,
    ) -> Result<(), DfsError> {
        let mut object = object.clone();
        normalize_current_locations(&mut object.current_locations);
        let size_bytes = match size_bytes {
            Some(size_bytes) => Some(size_bytes),
            None => self.object_content_size_hint(&object)?,
        };
        let Some(backend) = self.metadata_backend.as_mut() else {
            return Ok(());
        };
        let file_system = backend
            .get_file_system(&object.file_system_id)?
            .unwrap_or_else(|| DfsFileSystem {
                file_system_id: object.file_system_id.clone(),
                display_name: object.file_system_id.to_string(),
                owner_node_id: object.owner_node_id.clone(),
                subtree_policies: Vec::new(),
            });
        backend.ensure_file_system(&file_system)?;

        let object_changed = backend.get_object(&object.object_id)?.as_ref() != Some(&object);

        let mut version = object_version_record(&object, size_bytes);
        let version_changed =
            if let Some(existing) = backend.get_version(&version.object_id, &version.version)? {
                version.created_at = existing.created_at;
                existing != version
            } else {
                true
            };

        match (object_changed, version_changed) {
            (true, true) => {
                backend.put_object_and_version(&object, &version)?;
            }
            (true, false) => {
                backend.put_object(&object)?;
            }
            (false, true) => {
                backend.put_version(&version)?;
            }
            (false, false) => {}
        }
        Ok(())
    }

    pub(crate) fn put_object_version_size_in_metadata_backend(
        &mut self,
        object: &DfsObjectMetadata,
        size_bytes: Option<u64>,
    ) -> Result<(), DfsError> {
        let Some(backend) = self.metadata_backend.as_mut() else {
            return Ok(());
        };
        put_visible_version_record_if_changed(
            backend.as_mut(),
            object_version_record(object, size_bytes),
        )?;
        Ok(())
    }

    fn commit_visible_version_in_metadata_backend(
        &mut self,
        intent: &DistributedWriteIntent,
        pending: &PendingNetworkedWrite,
        write_time_unix_nanos: u64,
    ) -> Result<(), DfsError> {
        let expected_object = self.current_object_for_write(&pending.object_id)?;
        let durable_nodes: BTreeSet<NodeId> = self
            .coordinator
            .receipts_for_intent(&intent.intent_id)
            .into_iter()
            .map(|receipt| receipt.replica_node_id.clone())
            .collect();
        let durable_locations: Vec<NodeId> = durable_nodes.iter().cloned().collect();
        if expected_object.version == pending.version
            && expected_object.content_hash.as_deref() == Some(pending.content_hash.as_str())
        {
            let mut current_locations = expected_object.current_locations.clone();
            normalize_current_locations(&mut current_locations);
            let size_is_current = pending.size_bytes == 0
                || expected_object.content_size_bytes == Some(pending.size_bytes);
            if current_locations == durable_locations && size_is_current {
                return Ok(());
            }
            let mut repaired_object = expected_object.clone();
            if pending.size_bytes != 0 {
                repaired_object.content_size_bytes = Some(pending.size_bytes);
            }
            repaired_object.current_locations = durable_locations;
            self.put_visible_object_in_metadata_backend(&repaired_object)?;
            self.metadata.add_object(repaired_object)?;
            return Ok(());
        }
        let mut committed_object = expected_object.clone();
        committed_object.version = pending.version.clone();
        committed_object.content_hash = Some(pending.content_hash.clone());
        committed_object.content_size_bytes = Some(pending.size_bytes);
        committed_object.modified_at_unix_nanos = write_time_unix_nanos;
        committed_object.changed_at_unix_nanos = write_time_unix_nanos;
        committed_object.last_modified_by_event_id = Some(EventId::generate());
        committed_object.current_locations = durable_locations;
        let visible_version = object_version_record(&committed_object, Some(pending.size_bytes));
        let required_replica_node_ids: Vec<NodeId> = durable_nodes.into_iter().collect();
        let Some(backend) = self.metadata_backend.as_mut() else {
            return Ok(());
        };
        if backend.get_object(&pending.object_id)?.is_none() {
            backend.apply_namespace_transaction(&DfsNamespaceTransaction {
                transaction_id: namespace_transaction_id_for_object(
                    "create-visible",
                    &pending.object_id,
                    &pending.version,
                ),
                file_system_id: committed_object.file_system_id.clone(),
                objects: vec![committed_object.clone()],
                versions: vec![visible_version],
                created_at: OffsetDateTime::now_utc(),
            })?;
            return Ok(());
        }
        backend.commit_visible_version(
            &expected_object.version,
            &committed_object,
            &visible_version,
            &required_replica_node_ids,
        )?;
        Ok(())
    }

    pub(crate) fn commit_remote_manifest_ready_intent_batched(
        &mut self,
        intent: &DistributedWriteIntent,
        base_object: &DfsObjectMetadata,
        content_hash: &str,
        content_size_bytes: u64,
    ) -> Result<(), DfsError> {
        let trace_started = Instant::now();
        let trace_intent_id = intent.intent_id.clone();
        let trace_object_id = intent.object_id.clone();
        let trace_version = intent.target_version.clone();
        if !self.coordinator.is_commit_ready(&intent.intent_id)? {
            return Err(DfsError::Distributed {
                message: format!("intent {} is not ready to commit", intent.intent_id),
            });
        }
        let prepare_started = Instant::now();
        self.ensure_intent_epoch_current(intent)?;
        let durable_nodes: BTreeSet<NodeId> = self
            .coordinator
            .receipts_for_intent(&intent.intent_id)
            .into_iter()
            .map(|receipt| receipt.replica_node_id.clone())
            .collect();
        let write_time = dfs_now_unix_nanos();
        let mut committed_object = base_object.clone();
        committed_object.version = intent.target_version.clone();
        committed_object.content_hash = Some(content_hash.to_string());
        committed_object.content_size_bytes = Some(content_size_bytes);
        committed_object.modified_at_unix_nanos = write_time;
        committed_object.changed_at_unix_nanos = write_time;
        committed_object.last_modified_by_event_id = Some(EventId::generate());
        committed_object.current_locations = durable_nodes.iter().cloned().collect();
        let visible_version = object_version_record(&committed_object, Some(content_size_bytes));

        let mut committed_intent = intent.clone();
        committed_intent.state = DfsWriteIntentState::Committed;
        let intent_record = write_intent_record(
            &committed_intent,
            Some(content_hash),
            Some(committed_object.clone()),
        );
        let receipts = durable_nodes
            .iter()
            .map(|replica_node_id| DfsReplicaReceiptRecord {
                receipt: DfsWriteReceipt {
                    file_system_id: intent.file_system_id.clone(),
                    object_id: intent.object_id.clone(),
                    committed_version: intent.target_version.clone(),
                    owner_node_id: self.node_id.clone(),
                    event_id: EventId::generate(),
                },
                replica_node_id: replica_node_id.clone(),
                content_hash: Some(content_hash.to_string()),
                received_at: OffsetDateTime::now_utc(),
            })
            .collect::<Vec<_>>();
        let prepare_ms = prepare_started.elapsed().as_millis();
        let backend_started = Instant::now();
        if let Some(backend) = self.metadata_backend.as_mut() {
            backend.put_object_version_intent_and_receipts(
                &committed_object,
                &visible_version,
                &intent_record,
                &receipts,
            )?;
        }
        let backend_ms = backend_started.elapsed().as_millis();
        let coordinator_started = Instant::now();
        self.coordinator
            .commit_intent(&intent.intent_id)
            .map_err(DfsError::from)?;
        let coordinator_ms = coordinator_started.elapsed().as_millis();
        let memory_started = Instant::now();
        self.track_metadata_tombstone(&committed_object);
        self.metadata.add_object(committed_object)?;
        self.clear_active_replica_stage(&intent.object_id, &intent.target_version);
        let memory_ms = memory_started.elapsed().as_millis();
        dfs_perf_trace(format_args!(
            "remote-manifest-commit-batched intent={} object={} version={} receipts={} prepare_ms={} backend_ms={} coordinator_ms={} memory_ms={} total_ms={}",
            trace_intent_id,
            trace_object_id,
            trace_version,
            receipts.len(),
            prepare_ms,
            backend_ms,
            coordinator_ms,
            memory_ms,
            trace_started.elapsed().as_millis()
        ));
        Ok(())
    }

    pub(crate) fn store_command_bytes(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        expected_hash: &str,
        bytes: &[u8],
    ) -> Result<(), DfsError> {
        let actual_hash = content_hash(bytes);
        if actual_hash != expected_hash {
            return Err(DfsError::ContentHashMismatch {
                expected: expected_hash.to_string(),
                actual: actual_hash,
            });
        }
        let manifest =
            self.content
                .put_object_version(object_id.as_str(), version.as_str(), bytes)?;
        if manifest.content_hash != expected_hash {
            return Err(DfsError::ContentHashMismatch {
                expected: expected_hash.to_string(),
                actual: manifest.content_hash,
            });
        }
        Ok(())
    }

    pub(crate) fn store_command_patch(
        &self,
        object_id: &DfsObjectId,
        base_version: &DfsVersion,
        target_version: &DfsVersion,
        expected_hash: &str,
        offset: u64,
        expected_patch_hash: &str,
        bytes: &[u8],
        base_identity: Option<&ContentVersionIdentity>,
    ) -> Result<(), DfsError> {
        let actual_patch_hash = content_hash(bytes);
        if actual_patch_hash != expected_patch_hash {
            return Err(DfsError::ContentHashMismatch {
                expected: expected_patch_hash.to_string(),
                actual: actual_patch_hash,
            });
        }
        let manifest = self
            .content
            .put_object_version_patch_with_info_and_base_identity(
                object_id.as_str(),
                base_version.as_str(),
                target_version.as_str(),
                offset,
                bytes,
                base_identity,
            )?
            .manifest;
        if manifest.content_hash != expected_hash {
            return Err(DfsError::ContentHashMismatch {
                expected: expected_hash.to_string(),
                actual: manifest.content_hash,
            });
        }
        Ok(())
    }

    pub(crate) fn write_outcome(
        &self,
        intent_id: &str,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        if let Some(intent) = self.coordinator.intent(intent_id)
            && matches!(intent.state, DfsWriteIntentState::Committed)
        {
            return self.local_write_outcome(intent_id, intent);
        }
        if let Some(pending) = self.pending_routed_writes.get(intent_id) {
            match &pending.result {
                Some(Ok(outcome)) => return Ok(outcome.clone()),
                Some(Err(error)) => return Err(error.clone()),
                None => {}
            }
            if pending.command.request_id != intent_id
                && let Some(canonical) = self.pending_routed_writes.get(&pending.command.request_id)
            {
                match &canonical.result {
                    Some(Ok(outcome)) => return Ok(outcome.clone()),
                    Some(Err(error)) => return Err(error.clone()),
                    None => {
                        return Ok(NetworkedDfsWriteOutcome::Pending {
                            intent_id: canonical
                                .remote_intent_id
                                .clone()
                                .unwrap_or_else(|| intent_id.to_string()),
                            durable_receipts: canonical.durable_receipts,
                            required_receipts: canonical.required_receipts,
                        });
                    }
                }
            }
            return match &pending.result {
                Some(Ok(_)) | Some(Err(_)) => unreachable!("pending result handled above"),
                None => Ok(NetworkedDfsWriteOutcome::Pending {
                    intent_id: pending
                        .remote_intent_id
                        .clone()
                        .unwrap_or_else(|| intent_id.to_string()),
                    durable_receipts: pending.durable_receipts,
                    required_receipts: pending.required_receipts,
                }),
            };
        }
        if let Some(intent) = self.coordinator.intent(intent_id) {
            return self.local_write_outcome(intent_id, intent);
        }
        Err(DfsError::Distributed {
            message: format!("unknown intent: {intent_id}"),
        })
    }

    fn local_write_outcome(
        &self,
        intent_id: &str,
        intent: &DistributedWriteIntent,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        if matches!(intent.state, DfsWriteIntentState::Committed) {
            let pending = self.pending_writes.get(intent_id);
            let content_hash = pending
                .map(|pending| pending.content_hash.clone())
                .or_else(|| {
                    self.metadata_backend
                        .as_ref()
                        .and_then(|backend| {
                            backend
                                .get_intent(&intent.object_id, intent_id)
                                .ok()
                                .flatten()
                        })
                        .and_then(|intent_record| intent_record.content_hash)
                })
                .or_else(|| {
                    self.current_read_object_metadata(&intent.object_id)
                        .ok()
                        .filter(|object| object.version == intent.target_version)
                        .and_then(|object| object.content_hash)
                })
                .ok_or_else(|| DfsError::Distributed {
                    message: format!("missing content hash for committed intent: {intent_id}"),
                })?;
            Ok(NetworkedDfsWriteOutcome::Committed {
                intent_id: intent_id.to_string(),
                version: pending
                    .map(|pending| pending.version.clone())
                    .unwrap_or_else(|| intent.target_version.clone()),
                content_hash,
                repair_jobs: self
                    .coordinator
                    .repair_jobs()
                    .iter()
                    .filter(|job| job.intent_id == intent_id)
                    .cloned()
                    .collect(),
            })
        } else {
            let _pending =
                self.pending_writes
                    .get(intent_id)
                    .ok_or_else(|| DfsError::Distributed {
                        message: format!("missing pending write: {intent_id}"),
                    })?;
            Ok(NetworkedDfsWriteOutcome::Pending {
                intent_id: intent_id.to_string(),
                durable_receipts: self.coordinator.durable_receipt_count(intent_id),
                required_receipts: intent.policy.min_commit_replicas,
            })
        }
    }
}
