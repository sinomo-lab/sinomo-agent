//! Committed write lifecycle and durable metadata persistence helpers.
//!
//! This module owns the service-level transition from staged DFS write
//! intents into visible versions. It also centralizes metadata-backend
//! receipt persistence, local metadata updates, command content storage,
//! and write-outcome calculation used by write, transfer, protocol, and
//! POSIX layers.

use std::collections::BTreeSet;
use std::time::Instant;

use sinomo_core::events::EventId;
use sinomo_core::node::NodeId;
use sinomo_core::{DfsObjectPolicy, DfsWriteReceipt};
use time::OffsetDateTime;

use crate::metadata_pages::walk_metadata_pages;
use crate::posix_path::parent_posix_path;
use crate::transfer::PendingNetworkedWrite;
use crate::*;

#[cfg(test)]
type CreatePrepareTestHook = Box<dyn FnOnce(&DfsPreparedOperationPayload)>;

#[cfg(test)]
thread_local! {
    pub(crate) static BEFORE_CREATE_PREPARE: std::cell::RefCell<Option<CreatePrepareTestHook>> = const { std::cell::RefCell::new(None) };
}

pub(crate) fn ensure_atomic_create_parent_touch_shard(
    file_system_id: &DfsFileSystemId,
    local_node_id: &NodeId,
    child_path: &str,
    parent_path: &str,
    shards: &[DfsMetadataShardConfig],
) -> Result<(), DfsError> {
    // A service without a control plane is the local, unsharded case. Once
    // shard placement exists, both records must be owned by the same shard:
    // ShardedSobjMetadataBackend partitions a multi-shard namespace batch into
    // independent commits and therefore cannot provide this POSIX atomicity.
    if shards.is_empty() {
        return Ok(());
    }
    if shards.len() != 1 {
        let shard_ids = shards
            .iter()
            .map(|shard| shard.shard_id.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        return Err(DfsError::Distributed {
            message: format!(
                "cannot atomically publish created object {child_path} and parent {parent_path} across metadata shards [{shard_ids}]"
            ),
        });
    }
    let shard = &shards[0];
    if shard.primary_node_id != *local_node_id {
        return Err(DfsError::StaleOwnerEpoch {
            file_system_id: file_system_id.clone(),
            local_node_id: local_node_id.clone(),
            primary_node_id: shard.primary_node_id.clone(),
            epoch: shard.epoch,
        });
    }
    Ok(())
}

fn create_visible_path_is_available(
    intent: &DistributedWriteIntent,
    durable_path_object: Option<&DfsObjectMetadata>,
) -> bool {
    match durable_path_object {
        None => true,
        Some(object) => {
            object.kind == DfsObjectKind::Tombstone
                && object.object_id != intent.object_id
                && intent.create_predecessor_object_id.as_ref() == Some(&object.object_id)
        }
    }
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    fn content_durability_for_write(
        &mut self,
        intent: &DistributedWriteIntent,
        _object: &DfsObjectMetadata,
    ) -> Result<Option<Box<DfsContentDurability>>, DfsError> {
        Ok(Some(crate::content_durability::from_intent(intent)))
    }

    pub(crate) fn apply_committed_intent(&mut self, intent_id: &str) -> Result<(), DfsError> {
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
        let pending = self
            .writes
            .pending_writes
            .get(intent_id)
            .cloned()
            .ok_or_else(|| DfsError::Distributed {
                message: format!("missing pending write for committed intent: {intent_id}"),
            })?;
        // PendingNetworkedWrite retains the effective primary execution mode.
        if intent.object_id != pending.object_id || intent.target_version != pending.version {
            return Err(DfsError::Distributed {
                message: format!(
                    "committed intent {intent_id} state does not match pending write: intent {} {}, pending {} {}",
                    intent.object_id, intent.target_version, pending.object_id, pending.version
                ),
            });
        }
        if !intent.publication.is_current() {
            return self.commit_retained_file_intent(&intent, &pending);
        }
        if intent.publication.current_precondition().is_some()
            && self.metadata_backend.as_ref().ok_or_else(|| DfsError::Metadata {
                message: "conditional publication requires its durable original owner".into(),
            })?.prepared_operation_record(&format!("write:{}", intent.intent_id))?.is_none()
        {
            let original_path = intent.publication.current_precondition()
                .expect("conditional publication checked above").path().to_string();
            self.check_current_write_precondition(&intent.publication, &intent.file_system_id,
                &intent.object_id, &original_path, intent.base_version.as_ref())?;
        }
        let transient_create = self
            .transient_metadata_object_ids
            .contains(&intent.object_id);
        let first_visible_create = match self.metadata_backend.as_ref() {
            Some(backend) => {
                if self.control_plane.is_some() {
                    transient_create || backend.get_object(&intent.object_id)?.is_none()
                } else {
                    // Current standalone creates preserve the same atomic
                    // child-plus-parent payload as clustered publication.
                    intent.touch_parent_on_create
                }
            }
            None if self.control_plane.is_some() => {
                return Err(DfsError::Metadata {
                    message: "clustered canonical write requires a durable metadata backend"
                        .to_string(),
                });
            }
            None => false,
        };
        let durable_object = self
            .metadata_backend
            .as_ref()
            .map(|backend| backend.get_object(&intent.object_id))
            .transpose()?
            .flatten();
        let already_visible = durable_object.as_ref().is_some_and(|object| {
            object.version == intent.target_version
                && object.content_hash.as_deref() == Some(pending.content_hash.as_str())
        });
        // Every receipt completion uses the same original-base-bound body.
        // Only an exact retained outcome enters the read-only replay branch.
        let batched_publication = first_visible_create
            || (durable_object.is_some() && !already_visible)
            || (!already_visible
                && self.metadata_backend.is_some()
                && self.control_plane.is_none());
        if batched_publication {
            let base_object = match self.current_read_object_metadata(&intent.object_id) {
                Ok(object) => object,
                Err(DfsError::UnknownObject { object_id }) if object_id == intent.object_id => {
                    self.current_object_for_write(&object_id)?
                }
                Err(error) => return Err(error),
            };
            self.commit_ready_intent_batched(
                &intent,
                &base_object,
                &pending.content_hash,
                pending.size_bytes,
            )?;
        }
        if !batched_publication {
            let digest = self
                .recorded_write_operation_for_retry(
                    &intent,
                    &pending.content_hash,
                    pending.size_bytes,
                )?
                .ok_or_else(|| DfsError::Metadata {
                    message: "write has no recorded prepared-operation outcome".into(),
                })?;
            self.commit_stored_prepared_operation(&digest)?;
            self.coordinator
                .restore_recorded_committed_intent(intent_id)?;
            self.clear_active_replica_stage(&pending.object_id, &pending.version);
        }

        if let Some(base) = &intent.base_version {
            self.activate_file_history_repair_roots([(intent.object_id.clone(), base.clone())])?;
        }

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
            self.send_best_effort_message_if_routable_detached(
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

        // Replicas the commit left without a receipt are needs in the shard's
        // repair index; the primary's executor sends them.
        Ok(())
    }

    pub(crate) fn try_apply_committed_intent_if_ready(
        &mut self,
        intent_id: &str,
    ) -> Result<(), DfsError> {
        if self.coordinator.intent(intent_id).is_some_and(|intent|
            intent.publication.current_precondition().is_some()) {
            self.refresh_committed_write_projection(intent_id)?;
        }
        if self.recover_current_write_precondition_failure(intent_id)?.is_some() {
            return Ok(());
        }
        if self.coordinator.is_commit_ready(intent_id)? {
            if let (Some(intent), Some(pending)) = (
                self.coordinator.intent(intent_id).cloned(),
                self.writes.pending_writes.get(intent_id),
            ) {
                let hash = pending.content_hash.clone();
                let size = pending.size_bytes;
                // Retried receipt completion owns the same input as an
                // explicit retry. Resume its sealed body before any fresh
                // planning can change timestamps, read sets or other effects.
                self.recover_prepared_write_for_retry(&intent, &hash, size)?;
            }
            if let Err(error) = self.apply_committed_intent(intent_id) {
                if let DfsError::WritePreconditionFailed { failure } = &error {
                    self.fail_current_write_precondition(intent_id, failure.clone())?;
                    // The exact intent now owns a durable negative outcome.
                    // Continue the callback's ordinary response flush; its
                    // original caller observes the typed cause via write_outcome.
                    return Ok(());
                }
                // A failed author attempt may have found a different immutable
                // prepare with the same ID. It cannot authorize publication of
                // that body. Recognize only an already committed outcome here;
                // pending work retains its ordinary maintenance/exact-retry
                // recovery path and is never automatically aborted.
                let record = self.coordinator.intent(intent_id).and_then(|intent| {
                    self.metadata_backend.as_ref().and_then(|backend| {
                        backend
                            .get_intent(&intent.object_id, intent_id)
                            .ok()
                            .flatten()
                    })
                });
                if let Some(record) = record {
                    match self.reconcile_observed_write_intent_with_policy(&record, false, true) {
                        Ok(Some(updated)) if updated.status == DfsWriteIntentStatus::Committed => {
                            return Ok(());
                        }
                        Ok(_) => {}
                        Err(recovery) => {
                            tracing::warn!(%intent_id, %recovery, "write outcome reconciliation will retry")
                        }
                    }
                }
                return Err(error);
            }
        }
        Ok(())
    }

    /// A foreground W<N write can commit while another pre-commit StageBlob is
    /// still in flight. When that exact sibling later becomes durable, publish
    /// its location and promote the staged replica without re-running the
    /// already-completed visible commit.
    /// Called only from the stage-response closure while its placement
    /// authority permit is still held.
    pub(crate) fn finalize_late_committed_stage_receipt_under_authority(
        &mut self,
        intent_id: &str,
        replica_node_id: &NodeId,
    ) -> Result<(), DfsError> {
        let Some(intent) = self.coordinator.intent(intent_id).cloned() else {
            return Ok(());
        };
        if !matches!(intent.state, DfsWriteIntentState::Committed)
            || !intent.publication.is_current()
        {
            return Ok(());
        }
        let Some(pending) = self.writes.pending_writes.get(intent_id).cloned() else {
            return Ok(());
        };
        if self.retained_content_transfer_required(
            intent_id,
            &pending.object_id,
            &pending.version,
        )? {
            return Ok(());
        }
        if replica_node_id == &self.node_id || replica_node_id == &intent.requesting_node_id {
            return Ok(());
        }
        let committed_metadata = Some(self.current_read_object_metadata(&pending.object_id)?);
        let authority_shard =
            self.command_authority_shard_for_object(committed_metadata.as_ref())?;
        let request_id = self.next_request_id();
        self.send_best_effort_message_if_routable_detached(
            replica_node_id,
            DfsNetworkMessage::CommitVersion(DfsCommitVersionCommand {
                request_id,
                intent_id: intent_id.to_string(),
                file_system_id: intent.file_system_id,
                object_id: pending.object_id,
                expected_base_version: intent.base_version,
                version: pending.version,
                content_hash: pending.content_hash,
                object_metadata: committed_metadata,
                authority_shard,
            }),
        )?;
        Ok(())
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
        let mut pending_intent = None;
        walk_metadata_pages(|cursor| {
            let page = backend.intents_for_object_page(
                object_id,
                cursor,
                DFS_METADATA_OBJECT_INTENT_PAGE_SIZE,
            )?;
            for intent in page.intents {
                if !intent.publication.is_current() {
                    continue;
                }
                if excluding_intent_id == Some(intent.intent_id.as_str()) {
                    continue;
                }
                if matches!(
                    intent.status,
                    DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
                ) {
                    pending_intent = Some(intent);
                    return Ok(None);
                }
            }
            Ok(page.next_cursor)
        })
        .map_err(|error| DfsDistributedError::InvalidPolicy {
            reason: error.to_string(),
        })?;
        if let Some(intent) = pending_intent {
            return Err(DfsDistributedError::PendingIntentConflict {
                object_id: object_id.clone(),
                pending_intent_id: intent.intent_id,
                pending_target_version: intent.target_version,
                requested_target_version: target_version.clone(),
            });
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
        self.writes.active_replica_stages.insert(
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
            .writes
            .active_replica_stages
            .get(object_id)
            .is_some_and(|stage| &stage.version == version)
        {
            self.writes.active_replica_stages.remove(object_id);
        }
    }

    pub(crate) fn pending_write_record_to_restore(
        &mut self,
        intent_id: &str,
        object_id: &DfsObjectId,
    ) -> Result<Option<DfsWriteIntentRecord>, DfsError> {
        // A sealed current write terminalizes at W; its compact outcome still
        // owns the exact N-W repair plan. get_intent restores that recorded
        // plan, while the repair executor checks the current head and authority
        // before effects. Committed visibility alone never retires missing N.
        if self
            .writes
            .pending_writes
            .get(intent_id)
            .is_some_and(|pending| &pending.object_id == object_id)
            && self
                .coordinator
                .intent(intent_id)
                .is_some_and(|intent| &intent.object_id == object_id)
        {
            return Ok(None);
        }
        // Missing transfer state is reconstructed from its original intent.
        // An unpublished create's descriptor remains outside the namespace view.
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
            return Ok(None);
        };
        Ok(Some(record))
    }

    pub(crate) fn restore_pending_write_for_intent_if_missing(
        &mut self,
        intent_id: &str,
        object_id: &DfsObjectId,
    ) -> Result<(), DfsError> {
        let Some(record) = self.pending_write_record_to_restore(intent_id, object_id)? else {
            return Ok(());
        };
        self.restore_pending_write_from_record(record)
    }

    /// Pending transfer ownership belongs to the current metadata primary,
    /// independently of the historical requesting node. Reconstruct it from
    /// the exact durable intent after a service-lane change or process restart;
    /// neither a peer-provided stamp nor receipt count grants this role.
    pub(crate) fn local_pending_write_authority_shard(
        &mut self,
        record: &DfsWriteIntentRecord,
    ) -> Result<Option<DfsMetadataShardConfig>, DfsError> {
        if !matches!(
            record.status,
            DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
        ) || self.control_plane.is_none()
        {
            return Ok(None);
        }
        let object = match self.current_read_object_metadata(&record.object_id) {
            Ok(object) => Some(object),
            Err(DfsError::UnknownObject { .. }) => record.object_metadata.clone(),
            Err(error) => return Err(error),
        };
        let Some(object) = object else {
            return Ok(None);
        };
        if object.object_id != record.object_id
            || object.file_system_id != record.file_system_id
            || object.kind == DfsObjectKind::Tombstone
        {
            return Ok(None);
        }
        Ok(self
            .select_metadata_shard_for_object(&object)?
            .filter(|shard| {
                shard.primary_node_id == self.node_id
                    && shard.file_system_id == record.file_system_id
                    && shard.epoch == record.owner_epoch
            }))
    }

    pub(crate) fn restore_pending_write_from_record(
        &mut self,
        record: DfsWriteIntentRecord,
    ) -> Result<(), DfsError> {
        let intent_id = record.intent_id.as_str();
        if matches!(record.status, DfsWriteIntentStatus::PreconditionFailed { .. }) {
            self.coordinator.restore_intent(distributed_intent_from_record(&record))?;
            self.remove_pending_write_state(intent_id);
            return Ok(());
        }
        if !record.publication.is_current() {
            return self.restore_retained_content_intent(&record);
        }
        if self.coordinator.intent(intent_id).is_none() {
            self.coordinator
                .restore_intent(distributed_intent_from_record(&record))?;
            self.restore_metadata_successor_head(&record)?;
            self.restore_backend_receipt_pages_for_intent(&record)?;
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
        let hidden_create = self.control_plane.is_some()
            && record.base_version.is_none()
            && matches!(
                record.status,
                DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
            )
            && !self.coordinator.is_commit_ready(intent_id)?;
        if !hidden_create
            && self
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
        let has_repair_job = !self.coordinator.missing_replica_jobs(intent_id).is_empty();
        let is_local_authoritative_repair = if has_repair_job {
            let repair_object = self.current_read_object_metadata(&record.object_id)?;
            self.is_local_metadata_primary_for_object(&repair_object)?
        } else {
            false
        };
        let is_local_authoritative_pending = if !is_local_coordinator
            && !is_local_desired_replica_with_content
            && !is_local_authoritative_repair
        {
            self.local_pending_write_authority_shard(&record)?.is_some()
        } else {
            false
        };
        if !is_local_coordinator
            && !is_local_desired_replica_with_content
            && !is_local_authoritative_repair
            && !is_local_authoritative_pending
        {
            return Ok(());
        }
        let size_bytes = self
            .pending_write_size_hint(&record.object_id, &record.target_version, &content_hash)?
            .or(local_manifest_size)
            .unwrap_or(0);
        if let Some(pending) = self.writes.pending_writes.get(intent_id) {
            if pending.object_id != record.object_id
                || pending.version != record.target_version
                || pending.content_hash != content_hash
            {
                return Err(DfsError::Metadata {
                    message: "restored intent differs from its live pending write".into(),
                });
            }
            // Rebuilding a coordinator projection cannot replace the actual
            // request, delivery classification or foreground/background mode.
        } else {
            self.writes.pending_writes.insert(
                intent_id.to_string(),
                PendingNetworkedWrite::new(
                    record.object_id,
                    record.target_version,
                    content_hash,
                    size_bytes,
                )
                .with_foreground_replica_repair(false),
            );
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
        let transient_create = self.transient_metadata_object_ids.contains(object_id);
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
        if !(already_current || transient_create && object.last_modified_by_event_id.is_some()) {
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
        self.transient_metadata_object_ids.remove(object_id);
        self.transient_metadata_predecessor_object_ids
            .remove(object_id);
        self.track_metadata_tombstone(&object);
        self.clear_metadata_lookup_cache();
        self.namespace_execution
            .publication_completed(std::slice::from_ref(&object));
        Ok(object)
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

        if self.metadata_backend.is_some() {
            confirmed_locations.clear();
            for receipt in self.backend_receipts_for_version(&object.object_id, &object.version)? {
                if !required_replica_node_ids.contains(&receipt.replica_node_id) {
                    continue;
                }
                if let Some(expected_hash) = object.content_hash.as_deref()
                    && receipt.content_hash.as_deref() != Some(expected_hash)
                {
                    continue;
                }
                confirmed_locations.insert(receipt.replica_node_id);
            }
        }

        Ok(confirmed_locations.into_iter().collect())
    }

    /// The object a receipt belongs to, after the admission checks every
    /// receipt publication starts with: no conflicting receipt, the object's
    /// actual filesystem, and a slot generation unchanged since the receipt's
    /// evidence was planned.
    pub(crate) fn metadata_receipt_object_at_generation(
        &mut self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
        content_hash: &str,
        generation: u64,
    ) -> Result<DfsObjectMetadata, DfsError> {
        self.ensure_metadata_receipt_accepts(object_id, version, replica_node_id, content_hash)?;
        let object = self.current_object_for_write(object_id)?;
        if object.file_system_id != *file_system_id {
            return Err(DfsError::Metadata {
                message: "receipt filesystem differs from its actual object".into(),
            });
        }
        let current = self.backend_receipt_slot_generation(object_id, version, replica_node_id)?;
        if current != generation {
            return Err(DfsError::ReceiptGenerationSuperseded {
                object_id: object_id.clone(),
                version: version.clone(),
                replica_node_id: replica_node_id.clone(),
                expected: generation,
                current,
            });
        }
        Ok(object)
    }

    /// Whether the commit this receipt makes ready publishes the receipt
    /// itself. The commit publication seals every durable receipt at its
    /// planned slot generation together with the visible head, so a receipt
    /// that completes W for an open default publication needs no publication
    /// of its own before that commit.
    pub(crate) fn commit_publishes_completing_receipt(
        &self,
        intent_id: &str,
        replica_node_id: &NodeId,
    ) -> Result<bool, DfsError> {
        let Some(intent) = self.coordinator.intent(intent_id) else {
            return Ok(false);
        };
        if intent.state != DfsWriteIntentState::AwaitingReceipts
            || !intent.publication.is_current()
            || !self.writes.pending_writes.contains_key(intent_id)
        {
            return Ok(false);
        }
        let receipts = self.coordinator.receipts_for_intent(intent_id);
        let already_counted = receipts
            .iter()
            .any(|receipt| receipt.replica_node_id == *replica_node_id);
        if already_counted
            || receipts.len() + 1 < intent.policy.min_commit_replicas
            || !intent.desired_replica_nodes.contains(replica_node_id)
        {
            return Ok(false);
        }
        self.can_commit_ready_intent_batched(intent)
    }

    /// Publish one receipt at the slot generation its evidence was planned
    /// against. A later withdrawal makes this a typed
    /// [`DfsError::ReceiptGenerationSuperseded`]; the metadata writer enforces
    /// the same compare when the publication is prepared and committed. An
    /// already present receipt at that generation publishes nothing new.
    pub(crate) fn put_metadata_receipt(
        &mut self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
        content_hash: &str,
        generation: u64,
    ) -> Result<bool, DfsError> {
        let object = self.metadata_receipt_object_at_generation(
            file_system_id,
            object_id,
            version,
            replica_node_id,
            content_hash,
            generation,
        )?;
        let existing = self.backend_receipt_for_replica(object_id, version, replica_node_id)?;
        let mut effects = Vec::new();
        let inserted = existing.is_none();
        if inserted {
            effects.push(DfsPreparedMetadataEffect::PutReceipt {
                receipt: DfsReplicaReceiptRecord {
                    receipt: DfsWriteReceipt {
                        file_system_id: file_system_id.clone(),
                        object_id: object_id.clone(),
                        committed_version: version.clone(),
                        owner_node_id: self.node_id.clone(),
                        event_id: EventId::generate(),
                    },
                    replica_node_id: replica_node_id.clone(),
                    content_hash: Some(content_hash.into()),
                    received_at: OffsetDateTime::now_utc(),
                    generation,
                },
            });
        }
        let visible_target = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| DfsError::Metadata {
                message: "content receipt persistence requires a current backend".into(),
            })?
            .get_object(object_id)?
            .is_some_and(|row| row.version == *version && row.kind != DfsObjectKind::Tombstone);
        if visible_target
            && object.kind != DfsObjectKind::Tombstone
            && !object.current_locations.contains(replica_node_id)
        {
            let mut updated = object.clone();
            updated.current_locations.push(replica_node_id.clone());
            effects.push(DfsPreparedMetadataEffect::PutObject { object: updated });
        }
        if !effects.is_empty() {
            self.publish_object_metadata_effects(&object, effects)?;
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
        let Some(receipt) =
            self.backend_receipt_for_replica(object_id, version, replica_node_id)?
        else {
            return Ok(());
        };
        if receipt.content_hash.as_deref() == Some(content_hash) {
            return Ok(());
        }
        Err(DfsError::Distributed {
            message: format!(
                "conflicting DFS replica receipt for {object_id} {version} from {replica_node_id}: existing {:?}, new {content_hash}",
                receipt.content_hash
            ),
        })
    }

    pub(crate) fn put_metadata_intent(
        &mut self,
        intent: &DistributedWriteIntent,
        content_hash: Option<&str>,
    ) -> Result<(), DfsError> {
        let mut effects = Vec::new();
        if intent.state == DfsWriteIntentState::Aborted {
            let prepared = self
                .metadata_backend
                .as_ref()
                .map(|backend| {
                    backend.prepared_operation_record(&format!("write:{}", intent.intent_id))
                })
                .transpose()?
                .flatten();
            if let Some(prepared) = prepared {
                match prepared.status {
                    DfsPreparedOperationStatus::Prepared => {
                        effects.push(DfsPreparedMetadataEffect::ResolvePreparedOperation {
                            digest: prepared.digest,
                            status: DfsPreparedOperationStatus::Aborted,
                        })
                    }
                    DfsPreparedOperationStatus::Aborted => {}
                    _ => {
                        return Err(DfsError::Metadata {
                            message: "closed content preparation cannot be aborted".into(),
                        });
                    }
                }
            }
        }
        let object = self.current_object_for_write(&intent.object_id)?;
        let mut record = write_intent_record(intent, content_hash, Some(object.clone()));
        let mut intent_changed = true;
        let mut existing = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| DfsError::Metadata {
                message: "intent persistence requires a current backend".into(),
            })?
            .get_intent(&intent.object_id, &intent.intent_id)?;
        if existing.is_none() && intent.state == DfsWriteIntentState::Aborted {
            existing = self.unpublished_write_intent_for_abort(&object, intent, content_hash)?;
        }
        if let Some(existing) = existing {
            record.created_at = existing.created_at;
            if intent.state == DfsWriteIntentState::Aborted {
                if !crate::intent::write_intent_status_is_active(&existing.status)
                    && existing.status != DfsWriteIntentStatus::Aborted
                {
                    return Err(DfsError::Metadata {
                        message: "closed durable write intent cannot be aborted".into(),
                    });
                }
                let mut expected = crate::intent::distributed_intent_from_record(&existing);
                expected.state = DfsWriteIntentState::Aborted;
                if &expected != intent {
                    return Err(DfsError::Metadata {
                        message: "abort candidate differs from its actual durable intent".into(),
                    });
                }
                // Preserve every original intent field and content descriptor.
                // Only the terminal status and its actual observation time change.
                record = existing.clone();
                record.status = DfsWriteIntentStatus::Aborted;
                record.updated_at = Some(OffsetDateTime::now_utc());
            }
            let mut comparable = record.clone();
            comparable.updated_at = existing.updated_at;
            intent_changed = comparable != existing;
        }
        if intent_changed {
            effects.push(DfsPreparedMetadataEffect::PutIntent { intent: record });
        }
        if !effects.is_empty() {
            // One existing kernel commit closes the hidden body and its intent.
            self.publish_object_metadata_effects(&object, effects)?;
        }
        Ok(())
    }

    fn unpublished_write_intent_for_abort(
        &mut self,
        object: &DfsObjectMetadata,
        intent: &DistributedWriteIntent,
        content_hash: Option<&str>,
    ) -> Result<Option<DfsWriteIntentRecord>, DfsError> {
        let scope = self.metadata_effect_scope_for_object(
            object,
            intent
                .operation_id
                .clone()
                .unwrap_or_else(|| intent.intent_id.clone()),
        )?;
        let observed = self
            .metadata_backend
            .as_ref()
            .expect("abort backend already checked")
            .write_operation_admission_snapshot(
                &scope.shard_id,
                &intent.file_system_id,
                &intent.object_id,
                &intent.intent_id,
            )?;
        if observed.intent.is_some() {
            return Ok(observed.intent);
        }
        let uncertain = |reason: &str| DfsError::WriteReconciliationRequired {
            operation_id: intent
                .operation_id
                .clone()
                .unwrap_or_else(|| intent.intent_id.clone()),
            object_id: intent.object_id.clone(),
            reason: reason.into(),
        };
        let mut original = None;
        for prepared in observed.prepared {
            let Some(record) = crate::service_routed_write::prepared_intent_for_write(
                &prepared,
                &intent.object_id,
                &intent.intent_id,
            ) else {
                continue;
            };
            let mut expected = intent.clone();
            expected.state = crate::intent::distributed_intent_from_record(record).state;
            let hash = content_hash
                .or(record.content_hash.as_deref())
                .ok_or_else(|| {
                    uncertain("unpublished write opening has no recorded content hash")
                })?;
            crate::service_routed_write::validate_retained_write_opening(
                &prepared, record, &expected, hash,
            )?;
            if original.replace(record.clone()).is_some() {
                return Err(uncertain("write abort has competing prepared openings"));
            }
        }
        // This is original DATA, not an applied outcome or publication permit.
        // The existing kernel still recovers/certifies it before terminalization.
        // The terminal update must retain these birth fields even before the
        // opening becomes visible in the materialized table.
        Ok(original)
    }

    pub(crate) fn put_metadata_intent_and_receipts(
        &mut self,
        intent: &DistributedWriteIntent,
        content_hash: &str,
        replica_node_ids: &[NodeId],
    ) -> Result<(), DfsError> {
        self.put_metadata_intent_and_receipts_with_planning_shard(
            intent,
            content_hash,
            replica_node_ids,
            None,
        )
    }

    pub(crate) fn put_metadata_intent_and_receipts_with_planning_shard(
        &mut self,
        intent: &DistributedWriteIntent,
        content_hash: &str,
        replica_node_ids: &[NodeId],
        planning_shard: Option<&DfsMetadataShardConfig>,
    ) -> Result<(), DfsError> {
        let object = self.current_object_for_write(&intent.object_id)?;
        let mut record = write_intent_record(intent, Some(content_hash), Some(object.clone()));
        let existing = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| DfsError::Metadata {
                message: "intent persistence requires a current backend".into(),
            })?
            .get_intent(&intent.object_id, &intent.intent_id)?;
        let mut effects = Vec::new();
        if let Some(existing) = existing.as_ref() {
            record.created_at = existing.created_at;
            if existing.status == DfsWriteIntentStatus::Committed
                && crate::intent::distributed_intent_from_record(existing) == *intent
                && existing.content_hash.as_deref() == Some(content_hash)
            {
                // Replica repair may change the current object's locations.
                // Replaying the same terminal intent preserves its original
                // captured object and timestamps; new receipts remain effects.
                record = existing.clone();
            }
        }
        let mut comparable = record.clone();
        if let Some(existing) = existing.as_ref() {
            comparable.updated_at = existing.updated_at;
        }
        if existing.as_ref() != Some(&comparable) {
            effects.push(DfsPreparedMetadataEffect::PutIntent { intent: record });
        }
        for replica in replica_node_ids {
            self.ensure_metadata_receipt_accepts(
                &intent.object_id,
                &intent.target_version,
                replica,
                content_hash,
            )?;
            if self
                .backend_receipt_for_replica(&intent.object_id, &intent.target_version, replica)?
                .is_none()
            {
                // A delayed opening must use each stage's original generation.
                // The native PutReceipt validation rejects a withdrawn slot.
                let generation = match self
                    .writes
                    .pending_writes
                    .get(&intent.intent_id)
                    .and_then(|p| p.planned_receipt_generations.get(replica))
                    .copied()
                {
                    Some(planned) => planned,
                    None => self.backend_receipt_slot_generation(
                        &intent.object_id,
                        &intent.target_version,
                        replica,
                    )?,
                };
                effects.push(DfsPreparedMetadataEffect::PutReceipt {
                    receipt: DfsReplicaReceiptRecord {
                        receipt: DfsWriteReceipt {
                            file_system_id: intent.file_system_id.clone(),
                            object_id: intent.object_id.clone(),
                            committed_version: intent.target_version.clone(),
                            owner_node_id: self.node_id.clone(),
                            event_id: EventId::generate(),
                        },
                        replica_node_id: replica.clone(),
                        content_hash: Some(content_hash.into()),
                        received_at: OffsetDateTime::now_utc(),
                        generation,
                    },
                });
            }
        }
        let visible_target = self
            .metadata_backend
            .as_ref()
            .expect("intent backend")
            .get_object(&intent.object_id)?
            .is_some_and(|row| {
                row.version == intent.target_version && row.kind != DfsObjectKind::Tombstone
            });
        if intent.state == DfsWriteIntentState::Committed && visible_target {
            let mut updated = object.clone();
            for replica in replica_node_ids {
                if !updated.current_locations.contains(replica) {
                    updated.current_locations.push(replica.clone());
                }
            }
            if updated != object {
                effects.push(DfsPreparedMetadataEffect::PutObject { object: updated });
            }
        }
        if !effects.is_empty() {
            if let Some(shard) = planning_shard {
                if shard.file_system_id != object.file_system_id
                    || shard.primary_node_id != self.node_id
                {
                    return Err(DfsError::Metadata {
                        message: "placement planning shard differs from the local object author".into(),
                    });
                }
                // This is an input scope, not writer permission. The existing
                // captured publisher acquires real execution and validates the
                // native current assignment, epoch, freeze and prepared body.
                self.publish_captured_object_metadata_effects(
                    &object,
                    DfsPreparedOperationScope {
                        file_system_id: object.file_system_id.clone(),
                        shard_id: DfsMetadataShardId::new(shard.shard_id.clone()),
                        owner_epoch: shard.epoch,
                        operation_id: format!("metadata:{}", sinomo_core::Id::generate()),
                    },
                    effects,
                )?;
            } else {
                self.publish_object_metadata_effects(&object, effects)?;
            }
        }
        Ok(())
    }

    pub(crate) fn put_visible_object_in_metadata_backend(
        &mut self,
        object: &DfsObjectMetadata,
    ) -> Result<(), DfsError> {
        self.put_visible_object_and_version_in_metadata_backend(
            object,
            self.object_content_size_hint(object)?,
        )
    }

    /// Complete a bootstrap cohort without recounting or overwriting live
    /// parents. New parents already carry the cohort's full initial count.
    pub(crate) fn publish_missing_bootstrap_objects(
        &mut self,
        objects: Vec<DfsObjectMetadata>,
    ) -> Result<(), DfsError> {
        let mut still_missing = Vec::new();
        for object in objects {
            let backend = self.metadata_backend.as_ref().ok_or_else(|| DfsError::Metadata {
                message: "bootstrap cohort requires its actual metadata owner".into(),
            })?;
            let occupant = backend.get_object_by_path(&object.file_system_id, &object.path)?;
            if occupant.is_some() { continue; }
            let current = backend.get_object(&object.object_id)?;
            if current.is_some() {
                return Err(DfsError::Metadata { message: format!("bootstrap identity {} requires qualified repair rather than pathname recreation", object.object_id) });
            }
            still_missing.push(object);
        }
        let mut groups = std::collections::BTreeMap::<DfsMetadataShardId, Vec<DfsObjectMetadata>>::new();
        for object in still_missing {
            let scope = self.metadata_effect_scope_for_object(&object, "bootstrap-cohort-scope".into())?;
            groups.entry(scope.shard_id).or_default().push(object);
        }
        let mut authored_parents = BTreeSet::new();
        for (shard_id, candidates) in groups {
            let _namespace_plan = self.namespace_execution.begin_independent_planning();
            let mut objects = Vec::new();
            for object in candidates {
                let backend = self.metadata_backend.as_ref().expect("validated bootstrap owner");
                if backend.get_object_by_path(&object.file_system_id, &object.path)?.is_some() { continue; }
                if backend.get_object(&object.object_id)?.is_some() {
                    return Err(DfsError::Metadata { message: "bootstrap identity changed before its owning transaction".into() });
                }
                self.namespace_execution.observe_path(&object.file_system_id, &object.path, None);
                self.namespace_execution.observe_object(&object.file_system_id, &object.object_id, None);
                objects.push(object);
            }
            let fresh_paths = objects.iter().map(|object| object.path.clone()).collect::<BTreeSet<_>>();
            let mut parents = std::collections::BTreeMap::<String, DfsObjectMetadata>::new();
            for object in &objects {
                if object.kind != DfsObjectKind::Directory { continue; }
                let Some(parent_path) = parent_posix_path(&object.path) else { continue; };
                if fresh_paths.contains(&parent_path) || authored_parents.contains(&parent_path) { continue; }
                if let Some(parent) = parents.get_mut(&parent_path) {
                    parent.namespace_link_count = Some(crate::posix::updated_namespace_link_count(parent, 1)?);
                    continue;
                }
                let parent_shards = self.namespace_shards_for_exact_paths(&object.file_system_id, [parent_path.as_str()])?;
                if parent_shards.iter().any(|parent| parent.shard_id != shard_id.0 || parent.primary_node_id != self.node_id) {
                    return Err(DfsError::Metadata { message: format!("retained bootstrap anchor {} requires qualified original-genesis repair across its parent shard", object.path) });
                }
                let parent = self.parent_directory_touch_metadata_for_create(object)?.ok_or_else(|| DfsError::Metadata {
                    message: "bootstrap directory has no parent successor".into(),
                })?;
                parents.insert(parent_path, parent);
            }
            objects.extend(parents.into_values());
            if objects.is_empty() { continue; }
            let file_system_id = objects[0].file_system_id.clone();
            let versions = objects.iter().map(|object| object_version_record(object, object.content_size_bytes)).collect::<Vec<_>>();
            self.apply_namespace_transaction(DfsNamespaceTransaction {
                content_reference_identity: None, contribution_digest: None,
                extension_expectations: Vec::new(), file_version_pins: None,
                transaction_id: namespace_transaction_id_for_batch("bootstrap-missing-cohort", &file_system_id, &objects, &versions),
                file_system_id, objects, versions, advisory_lock_states: Vec::new(),
                created_at: OffsetDateTime::now_utc(),
            })?;
            authored_parents.extend(fresh_paths);
        }
        Ok(())
    }

    fn publish_namespace_membership_change(
        &mut self,
        object: &DfsObjectMetadata,
    ) -> Result<bool, DfsError> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let Some(backend) = self.metadata_backend.as_ref() else { return Ok(false); };
        let current = backend.get_object(&object.object_id)?;
        let occupant = backend.get_object_by_path(&object.file_system_id, &object.path)?;
        let old_occupant = match current.as_ref() {
            Some(current) if current.path != object.path => backend.get_object_by_path(&current.file_system_id, &current.path)?,
            _ => occupant.clone(),
        };
        self.namespace_execution.observe_object(&object.file_system_id, &object.object_id, current.as_ref());
        self.namespace_execution.observe_path(&object.file_system_id, &object.path, occupant.as_ref());
        if let Some(current) = current.as_ref().filter(|current| current.path != object.path) {
            self.namespace_execution.observe_path(&current.file_system_id, &current.path, old_occupant.as_ref());
        }
        let mut deltas = std::collections::BTreeMap::<String, i64>::new();
        let mut change = |row: &DfsObjectMetadata, delta| {
            if row.kind == DfsObjectKind::Directory && row.tombstone_provenance.is_none()
                && let Some(parent) = parent_posix_path(&row.path)
            { *deltas.entry(parent).or_default() += delta; }
        };
        if let Some(current) = current.as_ref().filter(|current| old_occupant.as_ref().is_some_and(|old| old.object_id == current.object_id)) { change(current, -1); }
        if let Some(occupant) = occupant.as_ref().filter(|old| old.object_id != object.object_id) { change(occupant, -1); }
        change(object, 1);
        deltas.retain(|_, delta| *delta != 0);
        let canonical_unlink = object.kind == DfsObjectKind::Tombstone
            && object.tombstone_provenance.is_none()
            && current.as_ref().is_some_and(|current| !matches!(current.kind,
                DfsObjectKind::Directory | DfsObjectKind::Hardlink | DfsObjectKind::Tombstone)
                && current.path == object.path
                && old_occupant.as_ref().is_some_and(|occupant| occupant.object_id == current.object_id));
        if canonical_unlink {
            let current = current.as_ref().expect("validated original canonical row");
            if crate::posix::updated_namespace_link_count(current, 0)? != 1 {
                return Err(DfsError::Metadata { message: "canonical deletion with surviving links requires the owning unlink/promotion operation".into() });
            }
            if let Some(parent) = parent_posix_path(&object.path) { deltas.entry(parent).or_insert(0); }
        }
        if deltas.is_empty() { return Ok(false); }
        let mut paths = vec![object.path.as_str()];
        paths.extend(deltas.keys().map(String::as_str));
        if let Some(current) = current.as_ref() { paths.push(&current.path); }
        let shards = self.namespace_shards_for_exact_paths(&object.file_system_id, paths)?;
        if shards.len() > 1 || shards.first().is_some_and(|shard| shard.primary_node_id != self.node_id) {
            return Err(DfsError::Metadata { message: "directory membership change requires its owning namespace transaction or existing cross-shard adoption job".into() });
        }
        let mut objects = vec![object.clone()];
        for (path, delta) in deltas {
            let mut parent = self.metadata_backend.as_ref().unwrap()
                .get_object_by_path(&object.file_system_id, &path)?
                .ok_or_else(|| DfsError::Metadata { message: format!("directory membership parent {path} is absent") })?;
            if parent.kind != DfsObjectKind::Directory {
                return Err(DfsError::Metadata { message: format!("directory membership parent {path} is not a directory") });
            }
            self.namespace_execution.observe_object(&parent.file_system_id, &parent.object_id, Some(&parent));
            self.namespace_execution.observe_path(&parent.file_system_id, &parent.path, Some(&parent));
            parent.namespace_link_count = Some(crate::posix::updated_namespace_link_count(&parent, delta)?);
            let touch = dfs_now_unix_nanos().max(parent.modified_at_unix_nanos.saturating_add(1)).max(parent.changed_at_unix_nanos.saturating_add(1));
            parent.modified_at_unix_nanos = touch;
            parent.changed_at_unix_nanos = touch;
            parent.version = namespace::next_version(&parent.version);
            objects.push(parent);
        }
        let versions = objects.iter().map(|object| object_version_record(object, object.content_size_bytes)).collect::<Vec<_>>();
        self.apply_namespace_transaction(DfsNamespaceTransaction {
            content_reference_identity: None, contribution_digest: None,
            extension_expectations: Vec::new(), file_version_pins: None,
            transaction_id: namespace_transaction_id_for_batch("directory-membership", &object.file_system_id, &objects, &versions),
            file_system_id: object.file_system_id.clone(), objects, versions,
            advisory_lock_states: Vec::new(), created_at: OffsetDateTime::now_utc(),
        })?;
        Ok(true)
    }

    pub(crate) fn put_visible_object_and_version_in_metadata_backend(
        &mut self,
        object: &DfsObjectMetadata,
        size_bytes: Option<u64>,
    ) -> Result<(), DfsError> {
        let mut object = object.clone();
        normalize_current_locations(&mut object.current_locations);
        if self.publish_namespace_membership_change(&object)? { return Ok(()); }
        let mut version = object_version_record(
            &object,
            size_bytes.or(self.object_content_size_hint(&object)?),
        );
        let scope = self.metadata_effect_scope_for_object(
            &object,
            format!("object-metadata:{}", sinomo_core::Id::generate()),
        )?;
        let _execution = self.acquire_prepared_execution(&scope)?;
        let backend = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| DfsError::Metadata {
                message: "visible publication requires a current backend".into(),
            })?;
        let current = backend.get_object_on_shard(&scope.shard_id, &object.object_id)?;
        let occupant = backend.get_object_by_path_on_shard(
            &scope.shard_id,
            &object.file_system_id,
            &object.path,
        )?;
        let mut effects = Vec::new();
        if current.as_ref() != Some(&object) {
            effects.push(DfsPreparedMetadataEffect::PutObject {
                object: object.clone(),
            });
        }
        let existing = backend.get_version(&version.object_id, &version.version)?;
        if let Some(existing) = existing.as_ref() {
            version.created_at = existing.created_at;
            version.retention = existing.retention.clone();
            version.module_metadata = existing.module_metadata.clone();
            version.content_durability = existing.content_durability.clone();
        } else if let Some(current) = current.as_ref()
            && object.content_hash.is_some()
            && current.file_system_id == object.file_system_id
            && current.content_hash == object.content_hash
            && current.content_size_bytes == version.size_bytes
        {
            let original = backend
                .get_version(&current.object_id, &current.version)?
                .ok_or_else(|| DfsError::Metadata {
                    message: "metadata-only successor has no actual original version".into(),
                })?;
            if !original.visible
                || original.file_system_id != object.file_system_id
                || original.object_id != object.object_id
                || original.content_hash != version.content_hash
                || original.size_bytes != version.size_bytes
                || original.content_durability.is_none()
            {
                return Err(DfsError::Metadata {
                    message: "metadata-only successor has no unchanged recorded data generation"
                        .into(),
                });
            }
            // A new metadata revision still names the same verified byte
            // generation and its recorded grant. It creates no fresh receipt.
            version.content_durability = original.content_durability;
        }
        if existing.as_ref() != Some(&version) {
            effects.push(DfsPreparedMetadataEffect::PutVersion { version });
        }
        if !effects.is_empty() {
            self.publish_metadata_effects_on_shard(
                scope,
                vec![DfsNamespaceObjectVersionExpectation {
                    object_id: object.object_id.clone(),
                    expected_version: current.map(|row| row.version),
                }],
                vec![DfsNamespacePathOccupantExpectation {
                    path: object.path.clone(),
                    expected_object_id: occupant.map(|row| row.object_id),
                }],
                effects,
            )?;
        }
        Ok(())
    }

    pub(crate) fn put_object_version_size_in_metadata_backend(
        &mut self,
        object: &DfsObjectMetadata,
        size_bytes: Option<u64>,
    ) -> Result<(), DfsError> {
        self.put_visible_object_and_version_in_metadata_backend(object, size_bytes)
    }

    /// Builds the parent-directory half of a POSIX create commit. The caller
    /// persists it in the same namespace transaction that first publishes the
    /// child, so a restart can observe neither mutation or both and can never
    /// advance the parent twice while replaying a commit-ready intent.
    pub(crate) fn parent_directory_touch_metadata_for_create(
        &mut self,
        child: &DfsObjectMetadata,
    ) -> Result<Option<DfsObjectMetadata>, DfsError> {
        let Some(parent_path) = parent_posix_path(&child.path) else {
            return Ok(None);
        };
        let shards = self.namespace_shards_for_exact_paths(
            &child.file_system_id,
            [child.path.as_str(), parent_path.as_str()],
        )?;
        ensure_atomic_create_parent_touch_shard(
            &child.file_system_id,
            &self.node_id,
            &child.path,
            &parent_path,
            &shards,
        )?;
        let parent = if let Some(backend) = self.metadata_backend.as_ref() {
            backend.get_object_by_path(&child.file_system_id, &parent_path)?
        } else {
            self.metadata
                .object_by_normalized_posix_path(&child.file_system_id, &parent_path)
                .cloned()
        }
        .ok_or_else(|| DfsError::Metadata {
            message: format!(
                "cannot publish created object {} without parent directory {parent_path}",
                child.path
            ),
        })?;
        if parent.kind != DfsObjectKind::Directory {
            return Err(DfsError::Metadata {
                message: format!(
                    "cannot publish created object {} because parent {parent_path} is {:?}",
                    child.path, parent.kind
                ),
            });
        }
        self.namespace_execution.observe_object(&parent.file_system_id, &parent.object_id, Some(&parent));
        self.namespace_execution.observe_path(&parent.file_system_id, &parent.path, Some(&parent));
        let mut touched_parent = parent;
        let touch_time = dfs_now_unix_nanos()
            .max(touched_parent.modified_at_unix_nanos.saturating_add(1))
            .max(touched_parent.changed_at_unix_nanos.saturating_add(1));
        touched_parent.modified_at_unix_nanos = touch_time;
        touched_parent.changed_at_unix_nanos = touch_time;
        touched_parent.version = namespace::next_version(&touched_parent.version);
        touched_parent.last_modified_by_event_id = Some(EventId::generate());
        if child.kind == DfsObjectKind::Directory {
            touched_parent.namespace_link_count = Some(crate::posix::updated_namespace_link_count(&touched_parent, 1)?);
        }
        Ok(Some(touched_parent))
    }

    #[tracing::instrument(
        target = "sinomo.perf",
        level = "trace",
        name = "dfs.write.commit",
        skip_all,
        fields(stage = "dfs.write.commit")
    )]
    /// Commit a ready intent. A sealed receipt whose slot was withdrawn after
    /// its stage send planned it is superseded: that ends the replica's
    /// attempt, so it is no longer durable for this commit and stages again.
    /// The commit then completes from the other replicas if they still reach
    /// W, and otherwise reports the supersession while the write waits.
    pub(crate) fn commit_ready_intent_batched(
        &mut self,
        intent: &DistributedWriteIntent,
        base_object: &DfsObjectMetadata,
        content_hash: &str,
        content_size_bytes: u64,
    ) -> Result<(), DfsError> {
        loop {
            let error = match self.commit_ready_intent_batched_once(
                intent,
                base_object,
                content_hash,
                content_size_bytes,
            ) {
                Ok(()) => return Ok(()),
                Err(error) => error,
            };
            let DfsError::ReceiptGenerationSuperseded {
                replica_node_id, ..
            } = &error
            else {
                return Err(error);
            };
            let replica = replica_node_id.clone();
            if !self
                .coordinator
                .receipts_for_intent(&intent.intent_id)
                .iter()
                .any(|receipt| receipt.replica_node_id == replica)
            {
                return Err(error);
            }
            self.coordinator
                .withdraw_durable_receipt(&intent.intent_id, &replica)?;
            if let Some(pending) = self.writes.pending_writes.get_mut(&intent.intent_id) {
                pending.planned_receipt_generations.remove(&replica);
            }
            self.record_stage_retry_scheduled(&intent.intent_id, &replica);
            if !self.coordinator.is_commit_ready(&intent.intent_id)? {
                return Err(error);
            }
        }
    }

    fn commit_ready_intent_batched_once(
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
        let mut expectations_ms = 0;
        let readiness_span = tracing::trace_span!(target: "sinomo.perf", "dfs.commit.phase",
            stage = "dfs.commit.phase", phase = "readiness")
        .entered();
        if !self.coordinator.is_commit_ready(&intent.intent_id)? {
            return Err(DfsError::Distributed {
                message: format!("intent {} is not ready to commit", intent.intent_id),
            });
        }
        drop(readiness_span);
        if let Some(digest) =
            self.recorded_write_operation_for_retry(intent, content_hash, content_size_bytes)?
        {
            let replay = self.commit_stored_prepared_operation(&digest)?;
            debug_assert!(!replay.published);
            self.coordinator
                .restore_recorded_committed_intent(&intent.intent_id)?;
            self.clear_active_replica_stage(&intent.object_id, &intent.target_version);
            return Ok(());
        }
        let history_span = tracing::trace_span!(target: "sinomo.perf", "dfs.commit.phase",
            stage = "dfs.commit.phase", phase = "history_refresh")
        .entered();
        let configured_file_system = self.refresh_file_history_for_publication(base_object)?;
        drop(history_span);
        let mut planning_span = Some(
            tracing::trace_span!(target: "sinomo.perf", "dfs.commit.phase",
            stage = "dfs.commit.phase", phase = "plan")
            .entered(),
        );
        let prepare_started = Instant::now();
        let durable_nodes: BTreeSet<NodeId> = self
            .coordinator
            .receipts_for_intent(&intent.intent_id)
            .into_iter()
            .map(|receipt| receipt.replica_node_id.clone())
            .collect();
        let durable_object_exists = match self.metadata_backend.as_ref() {
            Some(backend) => backend.get_object(&intent.object_id)?.is_some(),
            None => intent.base_version.is_some() && !intent.touch_parent_on_create,
        };
        let first_visible_create = intent.touch_parent_on_create || !durable_object_exists;
        // The combined backend operation validates receipt identity and hash
        // under the same write lock as visible publication. A separate lookup
        // here would replay/clone a large externally advanced metadata table
        // only to repeat the check after taking that lock.
        let write_time = dfs_now_unix_nanos();
        let mut committed_object = base_object.clone();
        committed_object.version = intent.target_version.clone();
        committed_object.content_hash = Some(content_hash.to_string());
        committed_object.content_size_bytes = Some(content_size_bytes);
        committed_object.modified_at_unix_nanos = write_time;
        committed_object.changed_at_unix_nanos = write_time;
        if !(intent.touch_parent_on_create && committed_object.last_modified_by_event_id.is_some())
        {
            committed_object.last_modified_by_event_id = Some(EventId::generate());
        }
        committed_object.current_locations = durable_nodes.iter().cloned().collect();
        self.establish_rule_capture_before_object(&committed_object)?;
        let mut visible_version =
            object_version_record(&committed_object, Some(content_size_bytes));
        visible_version.content_durability =
            self.content_durability_for_write(intent, &committed_object)?;

        let mut committed_intent = intent.clone();
        committed_intent.state = DfsWriteIntentState::Committed;
        let mut pending_intent = intent.clone();
        pending_intent.state = DfsWriteIntentState::AwaitingReceipts;
        let mut pending_intent_record = write_intent_record(
            &pending_intent,
            Some(content_hash),
            Some(base_object.clone()),
        );
        if let Some(existing) = self
            .metadata_backend
            .as_ref()
            .map(|backend| backend.get_intent(&intent.object_id, &intent.intent_id))
            .transpose()?
            .flatten()
        {
            // Reuse the original staged body; a repair can update the current
            // base's replica locations before this write crosses W. The current
            // base still supplies visible publication and its version/path CAS.
            let captured_object =
                existing
                    .object_metadata
                    .as_ref()
                    .ok_or_else(|| DfsError::Metadata {
                        message: "staged write lost its captured object metadata".into(),
                    })?;
            let mut comparable_base = base_object.clone();
            comparable_base
                .current_locations
                .clone_from(&captured_object.current_locations);
            if &comparable_base != captured_object
                || captured_object.file_system_id != intent.file_system_id
                || captured_object.object_id != intent.object_id
                || intent
                    .base_version
                    .as_ref()
                    .is_some_and(|base| base != &captured_object.version)
            {
                return Err(DfsError::Metadata {
                    message: "staged write changed its captured base identity".into(),
                });
            }
            if crate::intent::DfsWriteIdentity::of_record(&existing)
                != crate::intent::DfsWriteIdentity::of_record(&pending_intent_record)
                || existing.content_hash != pending_intent_record.content_hash
            {
                return Err(DfsError::Metadata {
                    message: "staged write differs from its captured nonterminal intent".into(),
                });
            }
            pending_intent_record.object_metadata = existing.object_metadata.clone();
            pending_intent_record.created_at = existing.created_at;
            pending_intent_record.updated_at = existing.updated_at;
        }
        let mut intent_record = write_intent_record(
            &committed_intent,
            Some(content_hash),
            Some(committed_object.clone()),
        );
        intent_record.created_at = pending_intent_record.created_at;
        // Each sealed W receipt publishes at the slot generation its stage
        // send planned against, so a withdrawal after that plan fails the
        // prepared publication's generation compare. A copy staged without a
        // send, such as this node's own, plans against the current tables.
        let planned_generations = self
            .writes
            .pending_writes
            .get(&intent.intent_id)
            .map(|pending| pending.planned_receipt_generations.clone())
            .unwrap_or_default();
        let receipts = durable_nodes
            .iter()
            .map(|replica_node_id| {
                Ok(DfsReplicaReceiptRecord {
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
                    generation: match planned_generations.get(replica_node_id) {
                        Some(generation) => *generation,
                        None => self.backend_receipt_slot_generation(
                            &intent.object_id,
                            &intent.target_version,
                            replica_node_id,
                        )?,
                    },
                })
            })
            .collect::<Result<Vec<_>, DfsError>>()?;
        let mut touched_parent = None;
        let create_transaction = if first_visible_create {
            if durable_object_exists {
                self.abort_pending_write(&intent.intent_id)?;
                return Err(DfsError::AlreadyExists {
                    path: committed_object.path.clone(),
                });
            }
            if let Some(backend) = self.metadata_backend.as_ref() {
                let durable_path_object = backend
                    .get_object_by_path(&committed_object.file_system_id, &committed_object.path)?;
                if !create_visible_path_is_available(intent, durable_path_object.as_ref()) {
                    let occupant = durable_path_object
                        .as_ref()
                        .map(|object| format!("{} {:?}", object.object_id, object.kind))
                        .unwrap_or_else(|| "unknown object".to_string());
                    self.abort_pending_write(&intent.intent_id)?;
                    return Err(DfsError::Distributed {
                        message: format!(
                            "cannot publish created object {} because its path is owned by {occupant}",
                            committed_object.path
                        ),
                    });
                }
            }
            touched_parent = if intent.touch_parent_on_create {
                self.parent_directory_touch_metadata_for_create(&committed_object)?
            } else {
                None
            };
            let mut objects = vec![committed_object.clone()];
            let mut versions = vec![visible_version.clone()];
            if let Some(parent) = touched_parent.as_ref() {
                objects.push(parent.clone());
                versions.push(object_version_record(parent, parent.content_size_bytes));
            }
            Some(DfsNamespaceTransaction {
                content_reference_identity: None,
                extension_expectations: Vec::new(),
                contribution_digest: None,
                file_version_pins: None,
                transaction_id: namespace_transaction_id_for_object(
                    "create-visible-batched",
                    &intent.object_id,
                    &intent.target_version,
                ),
                file_system_id: committed_object.file_system_id.clone(),
                objects,
                versions,
                advisory_lock_states: Vec::new(),
                created_at: OffsetDateTime::now_utc(),
            })
        } else {
            None
        };
        // Recovery may have replaced the synthesized child metadata with the
        // exact durable prepare payload. Terminal intent metadata must point
        // at that same object, not at a newly timestamped reconstruction.
        intent_record.object_metadata = Some(committed_object.clone());
        // In clustered mode, durably journal the pending intent and exact
        // receipt set before collecting authority. The healthy fast path then
        // replicates this hidden prepare to every configured metadata replica.
        // If the shard is incomplete, the DFS recovery quorum remains
        // authoritative only after cluster Raft fences this exact prepare, so
        // a promoted replica cannot omit a committed base/target reservation.
        let selected_shard = if self.control_plane.is_some() {
            Some(
                self.select_metadata_shard_for_object(base_object)?
                    .ok_or_else(|| DfsError::Distributed {
                        message: format!(
                            "clustered DFS intent {} has no metadata-shard assignment",
                            intent.intent_id
                        ),
                    })?,
            )
        } else {
            None
        };
        if self.metadata_backend.is_none() {
            return Err(DfsError::Metadata {
                message: "publication requires a durable prepared-operation backend".into(),
            });
        }
        let _prepared =
            {
                let operation_id = format!("write:{}", intent.intent_id);
                let existing = self
                    .metadata_backend
                    .as_ref()
                    .expect("durable backend")
                    .prepared_operation_digest(&operation_id)?;
                let published = if let Some(digest) = existing {
                    let payload = self
                        .metadata_backend
                        .as_ref()
                        .expect("durable backend")
                        .prepared_operation_payload(&operation_id)?
                        .ok_or_else(|| DfsError::Metadata {
                            message: "exact write retry lost its captured prepared body".into(),
                        })?;
                    let recorded =
                        payload
                            .prepared_intent
                            .as_ref()
                            .ok_or_else(|| DfsError::Metadata {
                                message: "write retry names a namespace-only preparation".into(),
                            })?;
                    let target = payload
                        .transaction
                        .objects
                        .iter()
                        .find(|object| object.object_id == intent.object_id)
                        .ok_or_else(|| DfsError::Metadata {
                            message: "write preparation has no exact target".into(),
                        })?;
                    if crate::intent::DfsWriteIdentity::of_record(recorded)
                        != crate::intent::DfsWriteIdentity::of_intent(intent)
                        || recorded.content_hash.as_deref() != Some(content_hash)
                        || target.path != base_object.path
                        || target.kind != base_object.kind
                        || target.mode != base_object.mode
                        || target.uid != base_object.uid
                        || target.gid != base_object.gid
                        || target.content_hash.as_deref() != Some(content_hash)
                        || target.content_size_bytes != Some(content_size_bytes)
                        || payload
                            .required_replica_node_ids
                            .iter()
                            .any(|node| !durable_nodes.contains(node))
                    {
                        return Err(DfsError::Metadata {
                            message: "write retry differs from its immutable preparation".into(),
                        });
                    }
                    self.commit_stored_prepared_operation(&digest)?
                } else {
                    let transaction =
                        create_transaction
                            .clone()
                            .unwrap_or_else(|| DfsNamespaceTransaction {
                                content_reference_identity: None,
                                extension_expectations: Vec::new(),
                                contribution_digest: None,
                                file_version_pins: None,
                                transaction_id: namespace_transaction_id_for_object(
                                    "write-visible",
                                    &intent.object_id,
                                    &intent.target_version,
                                ),
                                file_system_id: intent.file_system_id.clone(),
                                objects: vec![committed_object.clone()],
                                versions: vec![visible_version.clone()],
                                advisory_lock_states: Vec::new(),
                                created_at: pending_intent_record.created_at,
                            });
                    let shard_id = selected_shard
                        .as_ref()
                        .map(|shard| DfsMetadataShardId::new(shard.shard_id.clone()))
                        .map(Ok)
                        .unwrap_or_else(|| self.local_prepared_operation_shard())?;
                    let (mut expected_object_versions, mut expected_path_occupants, _) =
                        crate::service_metadata::namespace_prepare_expectations(
                            self.metadata_backend
                                .as_ref()
                                .expect("durable backend")
                                .as_ref(),
                            &shard_id,
                            &transaction,
                        )?;
                    if durable_object_exists && !first_visible_create {
                        let expected =
                            intent
                                .base_version
                                .as_ref()
                                .ok_or_else(|| DfsError::Metadata {
                                    message: "existing-object write has no original base version"
                                        .into(),
                                })?;
                        let target = expected_object_versions
                            .iter_mut()
                            .find(|row| row.object_id == intent.object_id)
                            .ok_or_else(|| DfsError::Metadata {
                                message: "write preparation omitted its original head expectation"
                                    .into(),
                            })?;
                        target.expected_version = Some(expected.clone());
                    }
                    if let Some(condition) = intent.publication.current_precondition() {
                        crate::write_precondition::bind_guard(condition, &mut expected_object_versions,
                            &mut expected_path_occupants).map_err(|message| DfsError::Metadata { message })?;
                    }
                    let completion = crate::posix::owner_write::completion(
                        self.namespace_execution.request(),
                        &intent_record,
                        &committed_object,
                        &transaction,
                    );
                    self.publish_prepared_operation_with_file_system(
                        DfsPreparedOperationPayload::for_namespace_transaction(
                            DfsPreparedOperationScope {
                                file_system_id: intent.file_system_id.clone(),
                                shard_id,
                                owner_epoch: selected_shard.as_ref().map_or(0, |shard| shard.epoch),
                                operation_id,
                            },
                            expected_object_versions,
                            expected_path_occupants,
                            transaction,
                            receipts.clone(),
                            Some(pending_intent_record.clone()),
                            Some(intent_record.clone()),
                            durable_nodes.iter().cloned().collect(),
                            None,
                            completion,
                        )?,
                        configured_file_system.as_ref(),
                    )?
                };
                if !published.published {
                    self.coordinator
                        .restore_recorded_committed_intent(&intent.intent_id)?;
                    self.clear_active_replica_stage(&intent.object_id, &intent.target_version);
                    return Ok(());
                }
                committed_object = published
                    .transaction
                    .objects
                    .iter()
                    .find(|object| object.object_id == intent.object_id)
                    .cloned()
                    .ok_or_else(|| DfsError::Metadata {
                        message: "committed preparation has no target".into(),
                    })?;
                intent_record = published.terminal_intent.as_ref().cloned().ok_or_else(|| {
                    DfsError::Metadata {
                        message: "committed write preparation has no terminal intent".into(),
                    }
                })?;
                touched_parent = published
                    .transaction
                    .objects
                    .iter()
                    .find(|object| object.object_id != intent.object_id)
                    .cloned();
                published
            };
        drop(planning_span);
        let prepare_ms = prepare_started.elapsed().as_millis();
        let backend_started = Instant::now();
        self.record_message_committed_work(DfsMessageCommittedWork {
            publish_metadata: true,
            semantic_changes: true,
            ..Default::default()
        });
        let backend_ms = backend_started.elapsed().as_millis();
        let notify_span = tracing::trace_span!(target: "sinomo.perf", "dfs.commit.phase",
            stage = "dfs.commit.phase", phase = "notify_publication")
        .entered();
        self.namespace_execution
            .publication_completed(std::slice::from_ref(&committed_object));
        if let Some(parent) = touched_parent.as_ref() {
            self.namespace_execution
                .publication_completed(std::slice::from_ref(parent));
        }
        drop(notify_span);
        let coordinator_started = Instant::now();
        let coordinator_span = tracing::trace_span!(target: "sinomo.perf", "dfs.commit.phase",
            stage = "dfs.commit.phase", phase = "coordinator_commit")
        .entered();
        self.coordinator
            .commit_intent(&intent.intent_id)
            .map_err(DfsError::from)?;
        drop(coordinator_span);
        let coordinator_ms = coordinator_started.elapsed().as_millis();
        let memory_started = Instant::now();
        let memory_span = tracing::trace_span!(target: "sinomo.perf", "dfs.commit.phase",
            stage = "dfs.commit.phase", phase = "update_caches_and_repair_roots")
        .entered();
        self.track_metadata_tombstone(&committed_object);
        self.metadata.add_object(committed_object.clone())?;
        if let Some(parent) = touched_parent {
            self.metadata.add_object(parent)?;
        }
        self.coordinator.set_committed_version(
            committed_object.file_system_id.clone(),
            committed_object.object_id.clone(),
            committed_object.version.clone(),
        );
        self.transient_metadata_object_ids.remove(&intent.object_id);
        self.transient_metadata_predecessor_object_ids
            .remove(&intent.object_id);
        self.clear_metadata_lookup_cache();
        self.clear_active_replica_stage(&intent.object_id, &intent.target_version);
        if let Some(base) = &intent.base_version {
            self.activate_file_history_repair_roots([(intent.object_id.clone(), base.clone())])?;
        }
        drop(memory_span);
        let memory_ms = memory_started.elapsed().as_millis();
        dfs_perf_trace(format_args!(
            "ready-intent-commit-batched intent={} object={} version={} create={} receipts={} expectations_ms={} prepare_ms={} backend_ms={} coordinator_ms={} memory_ms={} total_ms={}",
            trace_intent_id,
            trace_object_id,
            trace_version,
            intent.touch_parent_on_create,
            receipts.len(),
            expectations_ms,
            prepare_ms,
            backend_ms,
            coordinator_ms,
            memory_ms,
            trace_started.elapsed().as_millis()
        ));
        Ok(())
    }

    /// Before exposing a pending synchronous W=2 write, retain its original
    /// opening only if no final transaction already owns those receipts.
    pub(crate) fn retain_unfinished_stage_opening(
        &mut self,
        intent: &DistributedWriteIntent,
        hash: &str,
        size: u64,
    ) -> Result<(), DfsError> {
        if !self
            .coordinator
            .intent(&intent.intent_id)
            .is_some_and(|i| i.state == DfsWriteIntentState::AwaitingReceipts)
        {
            return Ok(());
        }
        self.refresh_committed_write_projection(&intent.intent_id)?;
        if !self
            .coordinator
            .intent(&intent.intent_id)
            .is_some_and(|i| i.state == DfsWriteIntentState::AwaitingReceipts)
        {
            return Ok(());
        }
        if let Some(backend) = self.metadata_backend.as_ref()
            && let Some(final_record) =
                backend.prepared_operation_record(&format!("write:{}", intent.intent_id))?
        {
            Self::validate_recorded_write_retry(&final_record.payload, intent, hash, size)?;
            return Ok(());
        }
        let receipts: Vec<_> = self
            .coordinator
            .receipts_for_intent(&intent.intent_id)
            .into_iter()
            .filter(|receipt| receipt.state == DfsReplicaReceiptState::Durable)
            .map(|receipt| receipt.replica_node_id.clone())
            .collect();
        self.put_metadata_intent_and_receipts(intent, hash, &receipts)
    }

    fn recorded_write_operation_for_retry(
        &self,
        intent: &DistributedWriteIntent,
        content_hash: &str,
        content_size_bytes: u64,
    ) -> Result<Option<DfsPreparedOperationDigest>, DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(None);
        };
        let Some(digest) =
            backend.prepared_operation_digest(&format!("write:{}", intent.intent_id))?
        else {
            return Ok(None);
        };
        if backend.committed_prepared_operation(&digest)?.is_none() {
            return Ok(None);
        }
        let payload = backend
            .prepared_operation_payload(&digest.operation_id)?
            .ok_or_else(|| DfsError::Metadata {
                message: "recorded write outcome lost its immutable body".into(),
            })?;
        Self::validate_recorded_write_retry(&payload, intent, content_hash, content_size_bytes)?;
        Ok(Some(digest))
    }

    pub(crate) fn validate_recorded_write_retry(
        payload: &DfsPreparedOperationPayload,
        intent: &DistributedWriteIntent,
        content_hash: &str,
        content_size_bytes: u64,
    ) -> Result<(), DfsError> {
        let recorded = payload
            .prepared_intent
            .as_ref()
            .ok_or_else(|| DfsError::Metadata {
                message: "write replay names a namespace-only outcome".into(),
            })?;
        let target = payload
            .transaction
            .objects
            .iter()
            .find(|object| object.object_id == intent.object_id)
            .ok_or_else(|| DfsError::Metadata {
                message: "recorded write has no exact target".into(),
            })?;
        if crate::intent::DfsWriteIdentity::of_record(recorded)
            != crate::intent::DfsWriteIdentity::of_intent(intent)
            || recorded.content_hash.as_deref() != Some(content_hash)
            || target.version != intent.target_version
            || target.content_hash.as_deref() != Some(content_hash)
            || target.content_size_bytes != Some(content_size_bytes)
        {
            return Err(DfsError::Metadata {
                message: "recorded write retry differs from its original input".into(),
            });
        }
        Ok(())
    }

    pub(crate) fn recover_prepared_write_for_retry(
        &mut self,
        intent: &DistributedWriteIntent,
        content_hash: &str,
        content_size_bytes: u64,
    ) -> Result<(), DfsError> {
        if !self
            .coordinator
            .intent(&intent.intent_id)
            .is_some_and(|resident| resident.state == DfsWriteIntentState::AwaitingReceipts)
        {
            return Ok(());
        }
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(());
        };
        let Some(record) = backend
            .prepared_operation_record(&format!("write:{}", intent.intent_id))?
            .filter(|record| record.status == DfsPreparedOperationStatus::Prepared)
        else {
            return Ok(());
        };
        Self::validate_recorded_write_retry(
            &record.payload,
            intent,
            content_hash,
            content_size_bytes,
        )?;
        // An explicit replay resumes only this complete original body. The
        // ordinary recovery guard still requires the current certified owner
        // and fresh acknowledgements from every assigned metadata participant.
        self.recover_one_prepared_operation(record)?;
        Ok(())
    }

    fn recover_prepared_intent_opening_publication(
        &mut self,
        intent: &DistributedWriteIntent,
        content_hash: &str,
    ) -> Result<Option<DfsObjectMetadata>, DfsError> {
        if self.metadata_backend.is_none()
            || self.transport.delegates_local_namespace_authority()
            || !self
                .coordinator
                .intent(&intent.intent_id)
                .is_some_and(|resident| resident.state == DfsWriteIntentState::AwaitingReceipts)
        {
            return Ok(None);
        }
        let object = self.current_object_for_write(&intent.object_id)?;
        let scope = self.metadata_effect_scope_for_object(
            &object,
            intent
                .operation_id
                .clone()
                .unwrap_or_else(|| intent.intent_id.clone()),
        )?;
        let observed = self
            .metadata_backend
            .as_ref()
            .expect("retry backend")
            .write_operation_admission_snapshot(
                &scope.shard_id,
                &intent.file_system_id,
                &intent.object_id,
                &intent.intent_id,
            )?;
        let mut opening = None;
        for record in observed.prepared {
            let Some(original) = crate::service_routed_write::prepared_intent_for_write(
                &record,
                &intent.object_id,
                &intent.intent_id,
            ) else {
                continue;
            };
            crate::service_routed_write::validate_retained_write_opening(
                &record,
                original,
                intent,
                content_hash,
            )?;
            if opening.replace(record).is_some() {
                return Err(DfsError::WriteReconciliationRequired {
                    operation_id: intent
                        .operation_id
                        .clone()
                        .unwrap_or_else(|| intent.intent_id.clone()),
                    object_id: intent.object_id.clone(),
                    reason: "write opening has competing prepared effects".into(),
                });
            }
        }
        let recovered = if let Some(record) = opening {
            if !self.recover_one_prepared_operation(record)? {
                return Err(DfsError::Metadata {
                    message: "original opening still requires its native recovery owner".into(),
                });
            }
            true
        } else {
            false
        };
        let Some(stored) = self
            .metadata_backend
            .as_ref()
            .expect("retry backend")
            .get_intent_on_shard(&scope.shard_id, &intent.object_id, &intent.intent_id)?
        else {
            // No successful placeholder or staging authority is invented.
            return Ok(None);
        };
        let actual = crate::intent::distributed_intent_from_record(&stored);
        if crate::intent::DfsWriteIdentity::of_intent(intent)
            != crate::intent::DfsWriteIdentity::of_record(&stored)
            || stored.content_hash.as_deref() != Some(content_hash)
        {
            return Err(DfsError::WriteOperationIdConflict {
                operation_id: intent
                    .operation_id
                    .clone()
                    .unwrap_or_else(|| intent.intent_id.clone()),
                object_id: intent.object_id.clone(),
            });
        }
        self.refresh_committed_write_projection(&intent.intent_id)?;
        if actual.state != DfsWriteIntentState::AwaitingReceipts {
            return Ok(None);
        }
        self.restore_pending_write_for_intent_if_missing(&intent.intent_id, &intent.object_id)?;
        self.restore_backend_receipt_pages_for_intent(&stored)?;
        if recovered {
            self.record_durable_write_intent_work(&actual, &[]);
        }
        Ok(Some(object))
    }

    /// Resume only existing publication checkpoints before consuming a real
    /// receipt. This phase never re-enters staging or commit readiness: those
    /// remain the caller's normal next steps after its receipt is recorded.
    pub(crate) fn recover_write_publications_before_receipt(
        &mut self,
        intent_id: &str,
        content_hash: &str,
    ) -> Result<(), DfsError> {
        let Some(intent) = self
            .coordinator
            .intent(intent_id)
            .cloned()
            .filter(|intent| intent.state == DfsWriteIntentState::AwaitingReceipts)
        else {
            return Ok(());
        };
        let pending = self.writes.pending_writes.get(intent_id).ok_or_else(|| {
            DfsError::WriteReconciliationRequired {
                operation_id: intent
                    .operation_id
                    .clone()
                    .unwrap_or_else(|| intent_id.to_owned()),
                object_id: intent.object_id.clone(),
                reason: "receipt completion has no original pending transfer descriptor".into(),
            }
        })?;
        if pending.content_hash != content_hash {
            return Err(DfsError::WriteOperationIdConflict {
                operation_id: intent
                    .operation_id
                    .clone()
                    .unwrap_or_else(|| intent_id.to_owned()),
                object_id: intent.object_id.clone(),
            });
        }
        let size = pending.size_bytes;
        self.refresh_committed_write_projection(intent_id)?;
        self.recover_prepared_write_for_retry(&intent, content_hash, size)?;
        self.recover_prepared_intent_opening_publication(&intent, content_hash)?;
        Ok(())
    }

    pub(crate) fn recover_prepared_intent_opening_for_retry(
        &mut self,
        intent: &DistributedWriteIntent,
        content_hash: &str,
        content_size_bytes: u64,
    ) -> Result<(), DfsError> {
        let Some(object) =
            self.recover_prepared_intent_opening_publication(intent, content_hash)?
        else {
            return Ok(());
        };
        let Some(pending) = self.writes.pending_writes.get(&intent.intent_id) else {
            return Err(DfsError::WriteReconciliationRequired {
                operation_id: intent
                    .operation_id
                    .clone()
                    .unwrap_or_else(|| intent.intent_id.clone()),
                object_id: intent.object_id.clone(),
                reason: "original opening has no retained transfer source".into(),
            });
        };
        if pending.content_hash != content_hash || pending.size_bytes != content_size_bytes {
            return Err(DfsError::WriteReconciliationRequired {
                operation_id: intent
                    .operation_id
                    .clone()
                    .unwrap_or_else(|| intent.intent_id.clone()),
                object_id: intent.object_id.clone(),
                reason: "original transfer descriptor is unavailable or differs from the retry"
                    .into(),
            });
        }
        self.try_apply_committed_intent_if_ready(&intent.intent_id)?;
        if self.should_stage_missing_replicas_after_write(&intent.intent_id) {
            self.stage_missing_desired_replicas_for_intent(&intent.intent_id, object)?;
        }
        Ok(())
    }

    /// A base version can belong to transient, not-yet-published metadata.
    /// Existing-object batching therefore keys off durable backend state, while
    /// first publication uses an exact namespace prepare (with the parent touch
    /// included when POSIX create semantics require one).
    pub(crate) fn can_commit_ready_intent_batched(
        &self,
        intent: &DistributedWriteIntent,
    ) -> Result<bool, DfsError> {
        if intent.touch_parent_on_create {
            return Ok(true);
        }
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(true);
        };
        if backend
            .prepared_operation_record(&format!("write:{}", intent.intent_id))?
            .is_some()
        {
            return Ok(true);
        }
        match backend.get_object(&intent.object_id)? {
            Some(_) => Ok(intent.base_version.is_some()),
            None => Ok(true),
        }
    }

    pub(crate) fn store_command_bytes(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        expected_hash: &str,
        bytes: &[u8],
    ) -> Result<(), DfsError> {
        if let Err(error) = self.content.put_object_version_with_expected_hash(
            object_id.as_str(),
            version.as_str(),
            expected_hash,
            bytes,
        ) {
            return match error {
                ContentStoreError::BlobHashMismatch { expected, actual } => {
                    Err(DfsError::ContentHashMismatch { expected, actual })
                }
                error => Err(error.into()),
            };
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
            .put_object_version_patch_with_trusted_content_hash_and_base_identity(
                object_id.as_str(),
                base_version.as_str(),
                target_version.as_str(),
                offset,
                bytes,
                expected_hash,
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

    /// Adopt only an exact durable terminal into this resident projection.
    /// This is response/recovery bookkeeping, never publication authority.
    pub(crate) fn refresh_committed_write_projection(
        &mut self,
        intent_id: &str,
    ) -> Result<(), DfsError> {
        let Some(intent) = self
            .coordinator
            .intent(intent_id)
            .filter(|intent| intent.state == DfsWriteIntentState::AwaitingReceipts)
            .cloned()
        else {
            return Ok(());
        };
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(());
        };
        let record = match backend.get_intent(&intent.object_id, intent_id)? {
            Some(record) => Some(record),
            None => backend
                .get_intent_receipt(&intent.object_id, intent_id)?
                .map(|receipt| receipt.to_intent_record()),
        };
        let Some(record) = record.filter(|record| matches!(record.status,
            DfsWriteIntentStatus::Committed | DfsWriteIntentStatus::PreconditionFailed { .. }))
        else { return Ok(()); };
        if let DfsWriteIntentStatus::PreconditionFailed { failure } = &record.status {
            let mut recorded = crate::intent::distributed_intent_from_record(&record);
            let epoch_advanced = recorded.owner_epoch >= intent.owner_epoch;
            recorded.owner_epoch = intent.owner_epoch;
            recorded.state = intent.state.clone();
            if !epoch_advanced || recorded != intent
                || self.writes.pending_writes.get(intent_id).is_some_and(|pending|
                    record.content_hash.as_deref() != Some(pending.content_hash.as_str()))
            {
                return Err(DfsError::Metadata { message: "negative terminal differs from its original resident intent".into() });
            }
            self.coordinator.fail_intent_precondition(intent_id, failure.clone())?;
            self.remove_pending_write_state(intent_id);
            self.clear_active_replica_stage(&record.object_id, &record.target_version);
            return Ok(());
        }
        let content_size = backend
            .get_version(&record.object_id, &record.target_version)?
            .and_then(|version| version.size_bytes);
        let mut recorded = crate::intent::distributed_intent_from_record(&record);
        // Guarded recovery can advance the owner epoch; it cannot change the
        // caller, original base, target, policy, or operation identity.
        let epoch_advanced = recorded.owner_epoch >= intent.owner_epoch;
        recorded.owner_epoch = intent.owner_epoch;
        recorded.state = intent.state.clone();
        if !epoch_advanced
            || recorded != intent
            || self
                .writes
                .pending_writes
                .get(intent_id)
                .is_some_and(|pending| {
                    record.content_hash.as_deref() != Some(pending.content_hash.as_str())
                        || content_size != Some(pending.size_bytes)
                })
        {
            return Err(DfsError::Metadata {
                message: "committed write record differs from the original resident intent".into(),
            });
        }
        self.restore_recorded_content_receipts(&record)?;
        self.coordinator
            .restore_recorded_committed_intent(intent_id)?;
        self.apply_backend_object_to_service_state(&record.object_id)?;
        self.clear_active_replica_stage(&record.object_id, &record.target_version);
        Ok(())
    }

    pub(crate) fn write_outcome(
        &self,
        intent_id: &str,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        if let Some(intent) = self.coordinator.intent(intent_id)
            && matches!(intent.state, DfsWriteIntentState::Committed
                | DfsWriteIntentState::PreconditionFailed { .. })
        {
            return self.local_write_outcome(intent_id, intent);
        }
        if let Some(pending) = self.writes.pending_routed_writes.get(intent_id) {
            // Aliases are links to the current canonical attempt, not a second
            // outcome owner. Explicit retries and their rollback update that
            // canonical state without letting an old alias result win.
            let pending = if pending.canonical_request_id != intent_id {
                self.writes
                    .pending_routed_writes
                    .get(&pending.canonical_request_id)
                    .ok_or_else(|| DfsError::Distributed {
                        message: format!("orphaned routed write alias: {intent_id}"),
                    })?
            } else {
                pending
            };
            return match &pending.result {
                Some(Ok(outcome)) => Ok(outcome.clone()),
                Some(Err(error)) => Err(error.clone()),
                // A held write reports its evidence until it is sent again.
                None if pending.follow.is_some() => Err(pending
                    .follow
                    .as_ref()
                    .expect("held write checked above")
                    .evidence
                    .clone()),
                None => Ok(NetworkedDfsWriteOutcome::Pending {
                    intent_id: pending
                        .remote_intent_id
                        .clone()
                        .unwrap_or_else(|| pending.canonical_request_id.clone()),
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
        if let DfsWriteIntentState::PreconditionFailed { failure } = &intent.state {
            return Err(DfsError::WritePreconditionFailed { failure: failure.clone() });
        }
        if matches!(intent.state, DfsWriteIntentState::Committed) {
            let pending = self.writes.pending_writes.get(intent_id);
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
                repair_jobs: self.coordinator.missing_replica_jobs(intent_id),
            })
        } else {
            let _pending =
                self.writes
                    .pending_writes
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

#[cfg(test)]
mod atomic_create_parent_touch_tests {
    use super::*;

    fn node(name: &str) -> NodeId {
        NodeId::from_legacy_named(name).expect("node id")
    }

    fn shard(shard_id: &str, primary_node_id: NodeId, epoch: u64) -> DfsMetadataShardConfig {
        DfsMetadataShardConfig {
            file_system_id: sifs_file_system_id(),
            shard_id: shard_id.to_string(),
            primary_node_id,
            replica_node_ids: Vec::new(),
            epoch,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        }
    }

    #[test]
    fn atomic_parent_touch_accepts_one_locally_owned_shard() {
        let local = node("node_atomic_parent_local");
        ensure_atomic_create_parent_touch_shard(
            &sifs_file_system_id(),
            &local,
            "/services/child.txt",
            "/services",
            &[shard("services", local.clone(), 7)],
        )
        .expect("same-shard parent touch");
    }

    #[test]
    fn atomic_parent_touch_rejects_split_shards() {
        let local = node("node_atomic_parent_split_local");
        let error = ensure_atomic_create_parent_touch_shard(
            &sifs_file_system_id(),
            &local,
            "/services/anchor/child.txt",
            "/services/anchor",
            &[
                shard("anchor", local.clone(), 3),
                shard("services", local.clone(), 3),
            ],
        )
        .expect_err("split-shard parent touch must fail closed");

        assert!(error.to_string().contains("across metadata shards"));
    }

    #[test]
    fn atomic_parent_touch_rejects_foreign_primary() {
        let local = node("node_atomic_parent_foreign_local");
        let remote = node("node_atomic_parent_foreign_remote");
        let error = ensure_atomic_create_parent_touch_shard(
            &sifs_file_system_id(),
            &local,
            "/services/child.txt",
            "/services",
            &[shard("services", remote.clone(), 11)],
        )
        .expect_err("foreign-primary parent touch must fail closed");

        assert!(matches!(
            error,
            DfsError::StaleOwnerEpoch {
                primary_node_id,
                epoch: 11,
                ..
            } if primary_node_id == remote
        ));
    }

    #[test]
    fn create_visible_path_accepts_absent_path() {
        let intent = intent_with_predecessor(None);
        assert!(create_visible_path_is_available(&intent, None));
    }

    #[test]
    fn create_visible_path_accepts_exact_predecessor_tombstone() {
        let predecessor_id = DfsObjectId::from_legacy_named("obj_create_predecessor")
            .expect("predecessor object id");
        let intent = intent_with_predecessor(Some(predecessor_id.clone()));
        let predecessor = path_object(predecessor_id, DfsObjectKind::Tombstone);

        assert!(create_visible_path_is_available(
            &intent,
            Some(&predecessor)
        ));
    }

    #[test]
    fn create_visible_path_rejects_live_or_different_tombstone() {
        let predecessor_id = DfsObjectId::from_legacy_named("obj_create_expected_predecessor")
            .expect("predecessor object id");
        let intent = intent_with_predecessor(Some(predecessor_id));
        let live = path_object(
            DfsObjectId::from_legacy_named("obj_create_live_winner").expect("live object id"),
            DfsObjectKind::File,
        );
        let different_tombstone = path_object(
            DfsObjectId::from_legacy_named("obj_create_later_tombstone")
                .expect("tombstone object id"),
            DfsObjectKind::Tombstone,
        );

        assert!(!create_visible_path_is_available(&intent, Some(&live)));
        assert!(!create_visible_path_is_available(
            &intent,
            Some(&different_tombstone)
        ));
    }

    fn intent_with_predecessor(
        create_predecessor_object_id: Option<DfsObjectId>,
    ) -> DistributedWriteIntent {
        DistributedWriteIntent {
            publication: Default::default(),
            intent_id: "intent_create_path_fence".to_string(),
            file_system_id: sifs_file_system_id(),
            object_id: DfsObjectId::from_legacy_named("obj_create_candidate")
                .expect("candidate object id"),
            base_version: None,
            target_version: DfsVersion::from_legacy_named("version_2").expect("version"),
            requesting_node_id: node("node_create_path_fence"),
            operation_id: None,
            operation_order_token: None,
            owner_epoch: 1,
            desired_replica_nodes: vec![node("node_create_path_fence")],
            policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
            touch_parent_on_create: true,
            create_predecessor_object_id,
            state: DfsWriteIntentState::AwaitingReceipts,
        }
    }

    fn path_object(object_id: DfsObjectId, kind: DfsObjectKind) -> DfsObjectMetadata {
        DfsObjectMetadata {
            namespace_link_count: kind.initial_namespace_link_count(),
            object_id,
            file_system_id: sifs_file_system_id(),
            path: "/services/fenced-create.txt".to_string(),
            kind,
            tombstone_provenance: None,
            owner_node_id: node("node_create_path_fence"),
            current_locations: Vec::new(),
            content_hash: None,
            content_size_bytes: None,
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 1,
            modified_at_unix_nanos: 1,
            created_at_unix_nanos: 1,
            changed_at_unix_nanos: 1,
            version: DfsVersion::from_legacy_named("version_1").expect("version"),
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        }
    }
}
