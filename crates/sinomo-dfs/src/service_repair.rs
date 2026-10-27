//! Repair, stage, and pending-transfer lifecycle for the networked DFS service.
//!
//! This module owns retry scheduling, stale inflight cleanup, alternate-source
//! fetch requests, inbound repair transfer handling, and repair-attempt
//! persistence. Unrelated fetch/read response handling stays in
//! `service_fetch_transfer` and `service_read_transfer`; outbound session
//! request construction stays in `service_outbound`.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use sinomo_core::node::NodeId;

use crate::transfer::{
    DFS_REPAIR_RETRY_SUPPRESSED_TICK, PendingNetworkedWrite, fetch_inflight_timeout_ticks,
    repair_retry_delay_nanos_for_ticks, repair_retry_delay_ticks, repair_retry_state_is_suppressed,
    repair_transfer_inflight_timeout_ticks, stage_inflight_retry_delay_ticks,
    wake_repair_retry_state,
};
use crate::*;

#[cfg(test)]
thread_local! {
    // Negative cuts on this explicit caller only. Neither supplies a reply or
    // retained history, and no worker/timer is installed.
    pub(crate) static HISTORY_QUERY_AFTER_ATTEMPT: std::cell::RefCell<Option<Box<dyn FnOnce() -> Result<(), DfsError>>>> = const { std::cell::RefCell::new(None) };
    pub(crate) static HISTORY_QUERY_AFTER_OBSERVATION: std::cell::RefCell<Option<Box<dyn FnOnce() -> Result<(), DfsError>>>> = const { std::cell::RefCell::new(None) };
}

#[cfg(any(test, feature = "test-support"))]
impl NetworkedDfsService<crate::DfsNodeTransport> {
    /// Explicit one-item selected history query. No executor association or B
    /// bytes are required; historical values never enter Repair completion.
    pub(crate) fn query_installed_original_history(
        &mut self, assignment: &DfsMetadataShardConfig, producer: &NodeId, request_id: &str,
    ) -> Result<DfsOriginalRepairHistoryResponse, DfsError> {
        use std::time::Instant;
        let captured = Instant::now();
        let deadline = self.transport.operation_deadline().map_or(captured + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            |caller| caller.min(captured + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT));
        // This API owns two distinct phases. It cannot release an admission
        // borrowed from an enclosing Main mutation/callback.
        if self.publication_execution.current().is_some() {
            return Err(DfsError::Metadata { message: "history query cannot borrow an enclosing mutation phase".into() });
        }
        let query = self.with_imported_history_phase(assignment, producer, request_id, deadline, |service, queue, imported| {
            queue.begin_imported_history_query(imported, assignment, deadline, || {
                history_query_deadline(deadline)?;
                let carrier = DfsOriginalRepairHistoryCarrier { source: service.node_id.clone(),
                    target: imported.material.birth.original.target_node_id.clone(), frame_id: service.next_frame_id, message_id: service.next_message_id };
                service.next_frame_id = service.next_frame_id.checked_add(1).ok_or_else(|| DfsError::Metadata { message: "history frame identity exhausted".into() })?;
                service.next_message_id = service.next_message_id.checked_add(1).ok_or_else(|| DfsError::Metadata { message: "history message identity exhausted".into() })?;
                Ok(carrier)
            })
        })?;
        history_query_deadline(deadline)?;
        if let Some(response) = query.terminal() { return Ok(response.clone()); }
        #[cfg(test)]
        HISTORY_QUERY_AFTER_ATTEMPT.with(|slot| {
            let cut = slot.borrow_mut().take();
            if let Some(cut) = cut { cut()?; }
            Ok::<(), DfsError>(())
        })?;
        // All B mutation, queue writer and shard-fence owners from the first
        // phase have been dropped before this actual one-call RPC.
        let request = query.request();
        let returned = self.transport.request_original_repair_history_direct(
            &request.carrier.target, &request.routed(), deadline,
        );
        match returned {
            Err(error) => {
                let delivery = error.delivery_state();
                // Failure facts concern only this query. If current B is gone
                // the already-synced attempt remains conservatively unknown.
                let _ = self.with_imported_history_phase(assignment, producer, request_id, deadline, |_, queue, _| {
                    queue.observe_imported_history_delivery(&query, delivery, deadline)
                });
                Err(DfsError::from_network(error))
            }
            Ok(returned) => {
                history_query_deadline(deadline)?;
                let routed = returned.into_routed();
                let observed = self.with_imported_history_phase(assignment, producer, request_id, deadline, |_, queue, _| {
                    history_query_deadline(deadline)?;
                    queue.observe_imported_history_result(&query, routed, deadline)
                })?;
                #[cfg(test)]
                HISTORY_QUERY_AFTER_OBSERVATION.with(|slot| {
                    let cut = slot.borrow_mut().take();
            if let Some(cut) = cut { cut()?; }
                    Ok::<(), DfsError>(())
                })?;
                history_query_deadline(deadline)?;
                let result = observed.response().ok_or_else(|| DfsError::Metadata { message: "history observation has no exact result".into() })?;
                Ok(result.clone())
            }
        }
    }

    fn with_imported_history_phase<R>(
        &mut self, assignment: &DfsMetadataShardConfig, producer: &NodeId, request_id: &str,
        deadline: std::time::Instant,
        operation: impl FnOnce(&mut Self, &crate::LocalDfsRepairQueueStore, &crate::repair_queue_store::ImportedRepairOriginalData) -> Result<R, DfsError>,
    ) -> Result<R, DfsError> {
        history_query_deadline(deadline)?;
        let invalid = || DfsError::Metadata { message: "history query has no exact current native B owner/import".into() };
        if assignment.primary_node_id != self.node_id || producer == &self.node_id || self.publication_execution.current().is_some() { return Err(invalid()); }
        let shard = DfsMetadataShardId::new(&assignment.shard_id);
        let backend = self.metadata_backend.as_ref().ok_or_else(invalid)?;
        let layout = backend.selected_original_data_layout(&assignment.file_system_id, &shard)?.ok_or_else(invalid)?;
        let scope = backend.publication_owner_scope(&assignment.file_system_id, &shard)?;
        let owner = crate::runtime_host::publication_runtime_owner(scope.root()).map_err(|error| DfsError::Metadata { message: error.to_string() })?.ok_or_else(invalid)?;
        layout.check_owner(&owner)?;
        let queue = layout.queue()?;
        let imported = queue.installed_original_handoff_data(producer, request_id)?.ok_or_else(invalid)?;
        if imported.material.cut.handoff.desired_shard != *assignment { return Err(invalid()); }
        let _admission = self.acquire_prepared_execution(&DfsPreparedOperationScope { file_system_id: assignment.file_system_id.clone(),
            shard_id: shard.clone(), owner_epoch: assignment.epoch, operation_id: request_id.to_owned() })?.ok_or_else(invalid)?;
        history_query_deadline(deadline)?;
        let execution = self.publication_execution.current().ok_or_else(invalid)?;
        let provider = self.publication_provider.current().ok_or_else(invalid)?;
        let material = imported.material.birth.original_wire_material()?;
        provider.validate_original_history_installer(&execution, &scope, &material, assignment,
            &self.node_id, &imported.material.birth.original.target_node_id)?;
        let _fence = crate::publication_guard::shard_fence_guard(&scope);
        let backend = self.metadata_backend.as_ref().ok_or_else(invalid)?;
        let freeze = backend.metadata_shard_write_freeze(&shard)?.ok_or_else(invalid)?;
        if freeze.activated_epoch != Some(assignment.epoch) || freeze.repair_custody.as_ref() != Some(&imported.material.cut)
            || backend.metadata_shard_authoritative_writes_frozen(&shard)? { return Err(invalid()); }
        history_query_deadline(deadline)?;
        let result = operation(self, &queue, &imported)?;
        provider.validate_original_history_installer(&execution, &scope, &material, assignment,
            &self.node_id, &imported.material.birth.original.target_node_id)?;
        layout.check_owner(&owner)?;
        history_query_deadline(deadline)?;
        Ok(result)
    }
}

#[cfg(any(test, feature = "test-support"))]
fn history_query_deadline(deadline: std::time::Instant) -> Result<(), DfsError> {
    if std::time::Instant::now() >= deadline { return Err(DfsError::Metadata { message: "original history query caller deadline expired".into() }); }
    Ok(())
}

/// Only the actual selected native receiver can construct this outcome after
/// its fixed content/cache branch succeeds. It is not a serialized capability.
pub(crate) struct NativeOriginalRepairOutcome {
    admission: crate::publication_guard::NativeContentOnlyRepairAdmission,
    response: DfsRepairReplicaResponse,
    prepared_seal: String,
    bound_id: u64,
}
impl NativeOriginalRepairOutcome {
    pub(crate) fn revalidate(&self, material: &DfsOriginalRepairMaterial, prepared_seal: &str, bound_id: u64) -> Result<(), DfsError> {
        self.admission.revalidate(material)?;
        if self.prepared_seal != prepared_seal || self.bound_id != bound_id {
            return Err(DfsError::Metadata { message: "native Repair outcome changed its actual bound-content phase".into() });
        }
        Ok(())
    }
    pub(crate) fn response(&self) -> &DfsRepairReplicaResponse { &self.response }
    pub(crate) fn provenance(&self) -> crate::publication_guard::NativeContentOnlyRepairProvenance { self.admission.provenance() }
    pub(crate) fn check_content_owner(&self, store: &LocalContentStore, incarnation: &str) -> Result<(), crate::ContentStoreError> { self.admission.check_content_owner(store, incarnation) }
    pub(crate) fn check_content_store(&self, store: &LocalContentStore) -> Result<(), crate::ContentStoreError> { self.admission.check_content_store(store) }
}

/// The ordinary base validator deliberately permits retained same-hash bytes.
/// This prospective current-content-only slice cannot roll a later metadata
/// generation back while applying its receiver-local cache substep.
fn ensure_selected_original_exact_current_content(current_version: &DfsVersion, current_hash: Option<&str>, command: &DfsRepairReplicaCommand) -> Result<(), DfsError> {
    if current_version != &command.version || current_hash != Some(command.content_hash.as_str()) {
        return Err(DfsError::Metadata { message: "selected original Repair target is not the exact current version/hash".into() });
    }
    Ok(())
}

/// Minted only after admitted native receipt application, or an admitted
/// exact persisted application read. The queue seals settlement atomically.
/// An observed response alone cannot construct this application witness.
pub(crate) struct AppliedOriginalRepair {
    observed: crate::repair_queue_store::RepairObservedResult,
}
impl AppliedOriginalRepair {
    pub(crate) fn observed(&self) -> &crate::repair_queue_store::RepairObservedResult {
        &self.observed
    }
}

/// Prohibition-only preflight for the disabled DATA join. Inputs are not an
/// authority certificate, a complete residency proof, W/N admission or a
/// serialization boundary between a policy change and a content-root write.
pub(crate) fn ensure_repair_content_not_forbidden(
    executor: &NodeId,
    immutable_policy: &DfsReplicationPolicy,
    actor: &'static str,
) -> Result<(), DfsError> {
    if immutable_policy.forbidden_node_ids.contains(executor) {
        return Err(DfsError::InvalidReplicationPolicy {
            message: format!("immutable Repair intent forbids {actor} retention on this node"),
        });
    }
    Ok(())
}

/// Pure hard prohibitions shared by disabled custody and the selected native
/// receiver. A desired/required set is not an exclusive retainer allowlist.
/// `actor` changes diagnostics only, never policy or authority.
pub(crate) fn ensure_repair_content_hard_prohibitions(
    executor: &NodeId,
    object: &DfsObjectMetadata,
    immutable_policy: &DfsReplicationPolicy,
    config: &DfsFileSystemConfig,
    actor: &'static str,
) -> Result<(), DfsError> {
    ensure_repair_content_not_forbidden(executor, immutable_policy, actor)?;
    if config.file_system_id != object.file_system_id {
        return Err(DfsError::Metadata {
            message: format!("{actor} retention policy names another filesystem"),
        });
    }
    let resolved = crate::placement::resolve_placement_policy(
        config, &object.path, object.policy.clone(), immutable_policy.desired_replicas,
    )?;
    if resolved.policy.forbidden_node_ids.contains(executor) {
        return Err(DfsError::InvalidReplicationPolicy {
            message: format!("current path policy forbids {actor} retention on this node"),
        });
    }
    if (object.policy == sinomo_core::DfsObjectPolicy::LocalOwned
        || resolved.policy.consistency == sinomo_core::DfsObjectPolicy::LocalOwned)
        && &object.owner_node_id != executor
    {
        return Err(DfsError::InvalidReplicationPolicy {
            message: format!("local-owned content cannot be retained by a different {actor}"),
        });
    }
    Ok(())
}

/// Exact current background Repair intent. The returned record is DATA from
/// this caller's existing indexed backend read, never an authority capability.
pub(crate) fn checked_selected_original_intent<'a>(
    material: &DfsOriginalRepairMaterial,
    receiver: &NodeId,
    intent: Option<&'a DfsWriteIntentRecord>,
) -> Result<&'a DfsWriteIntentRecord, DfsError> {
    let intent = intent.ok_or_else(|| DfsError::Metadata {
        message: "selected original Repair has no exact current native intent".into(),
    })?;
    let command = &material.original.command;
    if intent.intent_id != command.intent_id
        || intent.file_system_id != command.file_system_id
        || intent.object_id != command.object_id
        || intent.base_version != command.expected_base_version
        || intent.target_version != command.version
        || intent.content_hash.as_deref() != Some(command.content_hash.as_str())
        || !intent.publication.is_current()
        || intent.status != DfsWriteIntentStatus::Committed
        || !intent.desired_replica_node_ids.contains(receiver)
    {
        return Err(DfsError::Metadata {
            message: "selected original Repair current intent tuple, publication or target differs".into(),
        });
    }
    // Current canonical metadata or a path override cannot erase an immutable
    // LocalOwned restriction recorded in either original provenance body.
    for (source, metadata) in [
        ("original command", command.object_metadata.as_ref()),
        ("actual intent", intent.object_metadata.as_ref()),
    ] {
        if let Some(metadata) = metadata {
            if metadata.file_system_id != command.file_system_id || metadata.object_id != command.object_id {
                return Err(DfsError::Metadata { message: format!("selected original Repair {source} metadata scope differs") });
            }
            if metadata.policy == sinomo_core::DfsObjectPolicy::LocalOwned && &metadata.owner_node_id != receiver {
                return Err(DfsError::InvalidReplicationPolicy { message: format!("immutable {source} local-owned content cannot be retained by another selected receiver") });
            }
        }
    }
    Ok(intent)
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) fn associate_disabled_imported_executor_with_hard_prohibitions(
    executor: &NodeId,
    object: &DfsObjectMetadata,
    immutable_policy: &DfsReplicationPolicy,
    retained_configuration: impl FnOnce() -> Result<Option<DfsFileSystemConfig>, DfsError>,
    retain: impl FnOnce() -> Result<
        crate::repair_queue_store::ImportedRepairExecutorAssociation,
        DfsError,
    >,
) -> Result<crate::repair_queue_store::ImportedRepairExecutorAssociation, DfsError> {
    // A later path override cannot erase an immutable intent prohibition. Do
    // this before even borrowing current configuration or opening B content.
    ensure_repair_content_not_forbidden(executor, immutable_policy, "disabled executor")?;
    let config = retained_configuration()?.ok_or_else(|| DfsError::Metadata {
        message: "disabled executor retention has no retained exact-filesystem policy".into(),
    })?;
    // The exact-key accessor decodes the value; it does not validate this
    // embedded identity. Never resolve another filesystem's rule over this path.
    ensure_repair_content_hard_prohibitions(executor, object, immutable_policy, &config, "disabled executor")?;
    // Required nodes, counts and zones are deliberately not evaluated as an
    // allowlist or an N-target plan. This is an extra effect-body hold only.
    retain()
}

/// Observe the actual producer maps without changing transfer custody.
fn pending_repair_transfer_observation<'a>(
    pending: &'a PendingNetworkedWrite,
    target: &NodeId,
) -> (
    Option<&'a crate::transfer::RepairRetryState>,
    Option<&'a String>,
    sinomo_api::dfs::maintenance::DfsRepairTransferKind,
) {
    use sinomo_api::dfs::maintenance::DfsRepairTransferKind;
    if let Some(id) = pending.repair_inflight_request_ids.get(target) {
        (
            pending.repair_retries.get(target),
            Some(id),
            DfsRepairTransferKind::Repair,
        )
    } else if let Some(id) = pending.stage_inflight_request_ids.get(target) {
        (
            pending.stage_retries.get(target),
            Some(id),
            DfsRepairTransferKind::Stage,
        )
    } else {
        (
            pending.repair_retries.get(target),
            None,
            DfsRepairTransferKind::Repair,
        )
    }
}

/// Logical DFS write/repair records can outlive individual network sessions.
/// This is a stale-state horizon, not a socket or stream idle timeout; active
/// network I/O uses `sinomo-network`'s five-second policy.
pub(crate) const DFS_PENDING_TRANSFER_STALE_AFTER: Duration = Duration::from_secs(60);
/// How long a primary keeps an unanswered routed-write response obligation
/// before assuming the requester gave up.
const DFS_REMOTE_WRITE_RESPONSE_TTL: Duration = Duration::from_secs(60);
const DFS_TRANSIENT_ROUTE_REPAIR_RETRY_MAX_TICKS: u64 = 6;

pub(crate) fn repair_retry_delay_ticks_for_error(attempts: u32, last_error: Option<&str>) -> u64 {
    let delay_ticks = repair_retry_delay_ticks(attempts);
    if last_error.is_some_and(repair_error_indicates_transient_route_failure) {
        delay_ticks.min(DFS_TRANSIENT_ROUTE_REPAIR_RETRY_MAX_TICKS)
    } else {
        delay_ticks
    }
}

// One policy calculation for both ordinary retry scheduling and its exact
// terminal checkpoint. Prospective state is copied DATA, never a live claim.
fn schedule_repair_retry_state(
    state: &mut crate::transfer::RepairRetryState,
    tick: u64,
    last_error: Option<&str>,
) -> bool {
    if repair_retry_state_is_suppressed(state) {
        state.inflight = false;
        return false;
    }
    if !state.inflight {
        state.attempts = state.attempts.saturating_add(1);
    }
    state.inflight = false;
    state.next_retry_tick = tick.saturating_add(repair_retry_delay_ticks_for_error(
        state.attempts,
        last_error,
    ));
    true
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct DfsRepairBlobRequestOutcome {
    pub(crate) requested: bool,
    pub(crate) last_error: Option<String>,
}

/// Shared work budget for one repair-maintenance pass.
///
/// Stage retries and committed repairs use the same target budget. A failed or
/// unroutable send still consumes one slot: the bound limits attempted work and
/// wall-clock exposure, not only successful schedules.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DfsRepairMaintenanceBudget {
    remaining_targets: usize,
}

impl DfsRepairMaintenanceBudget {
    const fn new(max_targets: usize) -> Self {
        Self {
            remaining_targets: max_targets,
        }
    }

    const fn is_exhausted(self) -> bool {
        self.remaining_targets == 0
    }

    fn reserve(&mut self, requested: usize) -> usize {
        let reserved = requested.min(self.remaining_targets);
        self.remaining_targets -= reserved;
        reserved
    }

    fn reserve_one(&mut self) -> bool {
        self.reserve(1) == 1
    }
}

pub(crate) const DFS_REPAIR_EXAMINED_RECORDS_PER_PAGE: usize = 16;

#[derive(Default)]
pub(crate) struct DfsRepairPageProgress {
    /// No source snapshot was acquired; keep the same durable page pending.
    pub(crate) source_read_pending: bool,
    pub(crate) scheduled: Vec<DistributedRepairJob>,
    pub(crate) error: Option<DfsError>,
    /// Physical bounded input visits; distinct from checkpointable keys.
    pub(crate) examined_record_count: usize,
    pub(crate) examined: BTreeSet<(String, NodeId)>,
    /// Actual unavailable target observations stay unchanged at checkpoint.
    pub(crate) parked: BTreeSet<(String, NodeId)>,
    pub(crate) retired_intents: Vec<DfsRepairAttemptRecord>,
}

enum ManagedRepairPreparation {
    Prepared(PreparedManagedRepairBatch),
    /// Current metadata changed inside acquisition; re-run the existing scalar
    /// current/retained predicate before any effect. This grants no authority.
    RecheckCurrent,
}

struct PreparedManagedRepairBatch {
    error: Option<DfsError>,
    processed: BTreeSet<(String, NodeId)>,
    dispatches: Vec<(
        crate::service_outbound::PreparedReplicaSends,
        Vec<(DistributedRepairJob, String)>,
    )>,
}

impl PreparedManagedRepairBatch {
    fn target_count(&self) -> usize {
        self.dispatches
            .iter()
            .map(|(_, targets)| targets.len())
            .sum()
    }
}

// Queue retirement consumes only this exact stable identity. Retry fields
// are never written back from a retirement record.
pub(crate) fn retired_repair_identity(
    job: &DistributedRepairJob,
    pending: &PendingNetworkedWrite,
) -> DfsRepairAttemptRecord {
    DfsRepairAttemptRecord {
        transfer_delivery_state: None,
        completed_transfer_request_id: None,
        transfer_kind: None,
        intent_id: job.intent_id.clone(),
        object_id: pending.object_id.clone(),
        version: pending.version.clone(),
        content_hash: pending.content_hash.clone(),
        missing_replica_node_id: job.missing_replica_node_id.clone(),
        attempts: 0,
        in_flight: false,
        last_attempt_unix_nanos: 0,
        next_retry_unix_nanos: 0,
        transfer_request_id: None,
        last_error: None,
    }
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub(crate) fn publish_superseded_placement_intent_receipts(
        &mut self,
        object: &DfsObjectMetadata,
        replacement_id: &str,
        after: Option<&DfsWriteIntentPageCursor>,
        limit: usize,
    ) -> Result<(Vec<String>, Option<DfsWriteIntentPageCursor>), DfsError> {
        let backend = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| DfsError::Metadata {
                message: "placement retirement requires its current metadata owner".into(),
            })?;
        let replacement = backend
            .get_intent(&object.object_id, replacement_id)?
            .filter(|intent| {
                intent.publication.is_current()
                    && intent.status == DfsWriteIntentStatus::Committed
                    && intent.file_system_id == object.file_system_id
                    && intent.target_version == object.version
                    && intent.content_hash == object.content_hash
            })
            .ok_or_else(|| DfsError::Metadata {
                message: "placement replacement is not durably installed".into(),
            })?;
        let page = backend.intents_for_object_version_page(
            &object.object_id,
            &object.version,
            after,
            limit,
        )?;
        let retired = page
            .intents
            .into_iter()
            .filter(|intent| {
                intent.intent_id != replacement.intent_id
                    && intent.publication.is_current()
                    && intent.status == DfsWriteIntentStatus::Committed
                    && intent.file_system_id == object.file_system_id
                    && intent.content_hash == object.content_hash
            })
            .collect::<Vec<_>>();
        if retired.is_empty() {
            return Ok((Vec::new(), page.next_cursor));
        }
        let mut effects = Vec::new();
        if retired
            .iter()
            .any(|intent| !intent.intent_id.starts_with("placement:"))
        {
            if let Some(version) = backend
                .get_version(&object.object_id, &object.version)?
                .filter(|version| version.has_retention_grant())
            {
                let owner = self.recorded_retained_repair_owner(&version)?;
                match backend.get_intent(&object.object_id, &owner.intent_id)? {
                    Some(existing)
                        if crate::file_history::retained_repair_plan_matches(&existing, &owner) => {
                    }
                    Some(_) => {
                        return Err(DfsError::Metadata {
                            message: "retained repair owner changes its recorded durability".into(),
                        });
                    }
                    None => effects.push(DfsPreparedMetadataEffect::PutIntent { intent: owner }),
                }
            }
        }
        let at = time::OffsetDateTime::now_utc();
        for intent in &retired {
            // The bounded history page also reconstructs compacted receipts.
            // Replaying a settled retirement must preserve its original evidence
            // and retention time, rather than author another metadata operation.
            if backend
                .get_intent_receipt(&object.object_id, &intent.intent_id)?
                .is_some_and(|receipt| receipt.matches_intent(intent))
            {
                continue;
            }
            let receipt = DfsWriteIntentReceipt::from_intent(intent, at).ok_or_else(|| {
                DfsError::Metadata {
                    message: "placement retirement lacks exact terminal intent evidence".into(),
                }
            })?;
            effects.push(DfsPreparedMetadataEffect::PutIntentReceipt { receipt });
        }
        let ids = retired.into_iter().map(|intent| intent.intent_id).collect();
        // One guarded commit installs the retained owner before compacting
        // executable full records into their exact terminal retry receipts.
        if !effects.is_empty() {
            self.publish_object_metadata_effects(object, effects)?;
        }
        Ok((ids, page.next_cursor))
    }
    pub(crate) fn recorded_retained_repair_owner(
        &self,
        version: &DfsObjectVersionRecord,
    ) -> Result<DfsWriteIntentRecord, DfsError> {
        let owner = crate::file_history::retained_repair_intent(version, None)?;
        let durability = version
            .content_durability
            .as_ref()
            .ok_or_else(|| DfsError::Metadata {
                message: "retained repair requires its recorded version durability".into(),
            })?;
        let receipts = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| DfsError::Metadata {
                message: "retained repair requires its actual metadata owner".into(),
            })?
            .receipts_for_version(&version.object_id, &durability.source_version)?;
        let copies = receipts
            .iter()
            .filter(|receipt| {
                crate::metadata_receipt::recorded_receipt_matches_version(receipt, version)
                    && durability
                        .desired_replica_node_ids
                        .contains(&receipt.replica_node_id)
            })
            .map(|receipt| &receipt.replica_node_id)
            .collect::<BTreeSet<_>>();
        if copies.len() < durability.policy.min_commit_replicas {
            return Err(DfsError::Metadata {
                message: "retained repair owner lacks recorded distinct W receipts".into(),
            });
        }
        Ok(owner)
    }
    fn repair_fanout_companion(
        &self,
        job: &DistributedRepairJob,
        repairs: &[DistributedRepairJob],
        exclusions: &DfsMaintenanceExecutionExclusions,
        consumed: &BTreeSet<(String, NodeId)>,
    ) -> Option<DistributedRepairJob> {
        repairs
            .iter()
            .find(|candidate| {
                candidate.intent_id == job.intent_id
                    && candidate.missing_replica_node_id != job.missing_replica_node_id
                    && candidate.missing_replica_node_id != self.node_id
                    && !exclusions.contains(&candidate.missing_replica_node_id)
                    && !consumed.contains(&(
                        candidate.intent_id.clone(),
                        candidate.missing_replica_node_id.clone(),
                    ))
                    && self.repair_retry_is_due(
                        &candidate.intent_id,
                        &candidate.missing_replica_node_id,
                    )
                    && !self.stage_request_is_active(
                        &candidate.intent_id,
                        &candidate.missing_replica_node_id,
                    )
            })
            .cloned()
    }

    fn original_repair_execution_admits(
        &self,
        birth: &crate::repair_queue_store::RepairOriginalBirth,
    ) -> Result<bool, DfsError> {
        let DfsNetworkMessage::RepairReplica(command) = &birth.original.message else {
            return Ok(false);
        };
        if birth.original.source_node_id != self.node_id {
            return Ok(false);
        }
        let Some(shard) = command.authority_shard.as_ref() else {
            return Ok(false);
        };
        let (Some(backend), Some(execution)) = (
            self.metadata_backend.as_ref(),
            self.publication_execution.current(),
        ) else {
            return Ok(false);
        };
        let shard_id = DfsMetadataShardId::new(&shard.shard_id);
        let owner = backend.publication_owner_scope(&command.file_system_id, &shard_id)?;
        Ok(execution.admits(&owner, &command.file_system_id, &shard_id))
    }

    /// Explicit selected-only DATA join. The current native B owner supplies
    /// admission; neither the imported A stamp nor the resulting association
    /// grants dispatch, receipt application, closure or root retirement.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn associate_installed_repair_executor_disabled(
        &mut self,
        assignment: &DfsMetadataShardConfig,
        producer: &NodeId,
        request_id: &str,
    ) -> Result<Option<crate::repair_queue_store::ImportedRepairExecutorAssociation>, DfsError>
    {
        let invalid = || DfsError::Metadata {
            message: "disabled imported Repair association has no exact current native executor"
                .into(),
        };
        if assignment.primary_node_id != self.node_id || producer == &self.node_id {
            return Err(invalid());
        }
        let shard_id = DfsMetadataShardId::new(&assignment.shard_id);
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Err(invalid());
        };
        let Some(layout) =
            backend.selected_original_data_layout(&assignment.file_system_id, &shard_id)?
        else {
            return Ok(None);
        };
        let scope = backend.publication_owner_scope(&assignment.file_system_id, &shard_id)?;
        let owner = crate::runtime_host::publication_runtime_owner(scope.root())
            .map_err(|error| DfsError::Metadata {
                message: error.to_string(),
            })?
            .ok_or_else(invalid)?;
        layout.check_owner(&owner)?;
        let queue = layout.queue()?;
        let Some(imported) = queue.installed_original_handoff_data(producer, request_id)? else {
            return Ok(None);
        };
        if imported.material.cut.handoff.desired_shard != *assignment {
            return Err(invalid());
        }
        // This uses the real provider's existing configuration/storage checks
        // and exact native admission. No prepared operation is created here.
        let _execution = self.acquire_prepared_execution(&DfsPreparedOperationScope {
            file_system_id: assignment.file_system_id.clone(),
            shard_id: shard_id.clone(),
            owner_epoch: assignment.epoch,
            operation_id: request_id.to_owned(),
        })?;
        let execution = self.publication_execution.current().ok_or_else(invalid)?;
        if !execution.admits(&scope, &assignment.file_system_id, &shard_id) {
            return Err(invalid());
        }
        let _fence = crate::publication_guard::shard_fence_guard(&scope);
        let backend = self.metadata_backend.as_ref().ok_or_else(invalid)?;
        let freeze = backend
            .metadata_shard_write_freeze(&shard_id)?
            .ok_or_else(invalid)?;
        if freeze.activated_epoch != Some(assignment.epoch)
            || freeze.repair_custody.as_ref() != Some(&imported.material.cut)
            || backend.metadata_shard_authoritative_writes_frozen(&shard_id)?
        {
            return Err(invalid());
        }
        let DfsNetworkMessage::RepairReplica(command) = &imported.material.birth.original.message
        else {
            return Err(invalid());
        };
        let object = backend
            .get_object_on_shard(&shard_id, &command.object_id)?
            .ok_or_else(invalid)?;
        let intent = backend
            .get_intent_on_shard(&shard_id, &command.object_id, &command.intent_id)?
            .ok_or_else(invalid)?;
        if object.file_system_id != command.file_system_id
            || object.version != command.version
            || object.content_hash.as_deref() != Some(command.content_hash.as_str())
            || object.content_size_bytes
                != Some(imported.material.birth.attachment.total_size_bytes)
            || intent.file_system_id != command.file_system_id
            || intent.object_id != command.object_id
            || intent.target_version != command.version
            || intent.content_hash.as_deref() != Some(command.content_hash.as_str())
            || intent.status != DfsWriteIntentStatus::Committed
            || !intent
                .desired_replica_node_ids
                .contains(&imported.material.birth.original.target_node_id)
            || self
                .certified_local_metadata_owner_shard_for_object(&object)?
                .as_ref()
                != Some(assignment)
        {
            return Err(invalid());
        }
        let association = associate_disabled_imported_executor_with_hard_prohibitions(
            &self.node_id,
            &object,
            &intent.policy,
            || {
                // Retained-only: no get/range fallback, KV read or hydration.
                let Some(control) = self.control_plane.as_deref() else {
                    return Ok(None);
                };
                let Some(configuration) = control.write_configuration_projection()? else {
                    return Ok(None);
                };
                configuration
                    .file_system_config(&object.file_system_id)
                    .map_err(DfsError::from)
            },
            || {
                // Resolve B content from the actual retained DATA owner only
                // after both immutable and current explicit hard checks.
                let data_dir = sinomo_core::local::SinomoDataDir::new(owner.physical_data_root());
                let content = LocalContentStore::new(data_dir.dfs_dir().join("content"))?;
                queue.associate_imported_executor_disabled(
                    &imported,
                    &self.node_id,
                    assignment,
                    &content,
                )
            },
        )?;
        layout.check_owner(&owner)?;
        if self
            .certified_local_metadata_owner_shard_for_object(&object)?
            .as_ref()
            != Some(assignment)
            || !execution.admits(&scope, &assignment.file_system_id, &shard_id)
        {
            return Err(invalid());
        }
        Ok(Some(association))
    }

    /// Read exact persisted application material under the ACTUAL native
    /// execution admission. A retained provider/empty job list is insufficient.
    fn already_applied_original_repair(
        &self,
        birth: &crate::repair_queue_store::RepairOriginalBirth,
        observed: crate::repair_queue_store::RepairObservedResult,
    ) -> Result<Option<AppliedOriginalRepair>, DfsError> {
        let DfsNetworkMessage::RepairReplica(command) = &birth.original.message else {
            return Ok(None);
        };
        let Some(shard) = command.authority_shard.as_ref() else {
            return Ok(None);
        };
        let (Some(backend), Some(execution)) = (
            self.metadata_backend.as_ref(),
            self.publication_execution.current(),
        ) else {
            return Ok(None);
        };
        let shard_id = DfsMetadataShardId::new(&shard.shard_id);
        let owner = backend.publication_owner_scope(&command.file_system_id, &shard_id)?;
        if !execution.admits(&owner, &command.file_system_id, &shard_id) {
            return Ok(None);
        }
        let Some(receipt) = backend.get_receipt_on_shard(
            &shard_id,
            &command.object_id,
            &command.version,
            &birth.original.target_node_id,
        )?
        else {
            return Ok(None);
        };
        if receipt.receipt.file_system_id != command.file_system_id
            || receipt.receipt.object_id != command.object_id
            || receipt.receipt.committed_version != command.version
            || receipt.replica_node_id != birth.original.target_node_id
            || receipt.content_hash.as_deref() != Some(command.content_hash.as_str())
        {
            return Ok(None);
        }
        // put_metadata_receipt also maintains visible-head locations. Both
        // effects must already exist; never rewrite a newer head to settle old work.
        if let Some(object) = backend.get_object(&command.object_id)?
            && object.version == command.version
            && object.kind != DfsObjectKind::Tombstone
            && (object.file_system_id != command.file_system_id
                || object.content_hash.as_deref() != Some(command.content_hash.as_str())
                || !object
                    .current_locations
                    .contains(&birth.original.target_node_id))
        {
            return Ok(None);
        }
        Ok(Some(AppliedOriginalRepair { observed }))
    }

    /// Resolve at most this selected target. The stored original supplies
    /// correlation; only exact already-applied evidence can settle it here.
    /// Missing evidence stays pending. No transport/new metadata effect/ID occurs.
    fn recover_observed_original_repair(
        &mut self,
        birth: &crate::repair_queue_store::RepairOriginalBirth,
    ) -> Result<bool, DfsError> {
        let Some(response) = birth
            .observed
            .iter()
            .find(|response| matches!(response.result, DfsBlobTransferResult::Committed { .. }))
            .cloned()
        else {
            return Ok(false);
        };
        if !self.original_repair_execution_admits(birth)? {
            return Ok(false);
        }
        let Some(store) = self.owned_repair_queue_store()? else {
            return Err(DfsError::Distributed {
                message: "stored result recovery requires the actual native owner".into(),
            });
        };
        // This revalidates every response field against the complete synced
        // head and persists/deduplicates the exact observation, not a Boolean.
        let observed = store
            .observe_original_repair_result(&self.node_id, &response)?
            .ok_or_else(|| DfsError::Distributed {
                message: "stored result lost original custody".into(),
            })?;
        if let Some(applied) = self.already_applied_original_repair(birth, observed.clone())? {
            let closed = store.close_applied_original_repair(&applied)?;
            self.content.release_repair_payload(&closed)?;
            // This branch authors no metadata or network effect, including
            // when the intent was compacted and no repair job remains.
            return Ok(true);
        }
        // A missing receipt may be a LATER authoritative withdrawal, not a
        // crash before first application. Stable request/intent/version/hash
        // and current authority do not order this old positive against that
        // negative observation. Reauthoring metadata here would erase a valid
        // deficit without contacting the holder. Retain original custody until
        // the existing metadata contract provides causal application evidence.
        Ok(false)
    }

    fn recover_observed_original_repair_page(
        &mut self,
        limit: usize,
        intents: Option<&BTreeSet<String>>,
        exclusions: Option<&DfsMaintenanceExecutionExclusions>,
    ) -> Result<usize, DfsError> {
        let Some(store) = self.owned_repair_queue_store()? else {
            return Ok(0);
        };
        let page = store.observed_original_repair_page(limit)?;
        let mut recovered = 0;
        for birth in page {
            let DfsNetworkMessage::RepairReplica(command) = &birth.original.message else {
                return Err(DfsError::Distributed {
                    message: "original recovery page contains another effect".into(),
                });
            };
            if intents.is_some_and(|intents| !intents.contains(&command.intent_id))
                || exclusions
                    .is_some_and(|excluded| excluded.contains(&birth.original.target_node_id))
            {
                continue;
            }
            // At most `limit` visits even when rows/jobs disappeared. Pending
            // visits do not consume send capacity and cannot starve unrelated
            // ordinary repair behind the same permanently missing evidence.
            recovered += usize::from(self.recover_observed_original_repair(&birth)?);
        }
        Ok(recovered)
    }

    fn original_repair_request_for_target(
        &mut self,
        intent: &str,
        target: &NodeId,
        pending: &PendingNetworkedWrite,
    ) -> Result<Option<String>, DfsError> {
        if let Some(store) = self.owned_repair_queue_store()?
            && let Some(birth) = store.original_repair_for_slot(intent, target)?
        {
            #[cfg(any(test, feature = "test-support"))]
            if store.native_original_completion_pending(&birth.original)?
                || self.native_publication_owner_for_observation()
                    .map(|(_, owner)| owner.native_repair_custody_return_pending(&birth.original))
                    .transpose()?.unwrap_or(false)
            {
                // Known returned DATA is pending custody/settlement. Do not
                // claim a new carrier or turn this hold into receipt evidence.
                return Ok(None);
            }
            if self.recover_observed_original_repair(&birth)? {
                return Ok(None);
            }
            let DfsNetworkMessage::RepairReplica(command) = &birth.original.message else {
                return Err(DfsError::Distributed {
                    message: "original custody is not Repair".into(),
                });
            };
            if !birth.observed.is_empty() {
                return Ok(None);
            }
            if birth.original.source_node_id != self.node_id
                || pending
                    .repair_inflight_request_ids
                    .get(target)
                    .is_some_and(|id| id != &command.request_id)
                || pending.stage_inflight_request_ids.contains_key(target)
            {
                return Err(DfsError::Distributed {
                    message: "original Repair awaits exact same-producer recovery".into(),
                });
            }
            return Ok(Some(command.request_id.clone()));
        }
        Ok(Some(
            pending
                .repair_inflight_request_ids
                .get(target)
                .cloned()
                .unwrap_or_else(|| self.next_request_id()),
        ))
    }

    /// Native returned DATA holds one exact original pending settlement. The
    /// existing page parking path must retain its captured scheduling row too:
    /// projecting an already-due retry from wall time would otherwise emit a
    /// new Set on every maintenance pass despite performing no send.
    #[cfg(any(test, feature = "test-support"))]
    fn native_completion_held_repair_targets(
        &self,
        records: &[DfsRepairAttemptRecord],
    ) -> Result<BTreeSet<(String, NodeId)>, DfsError> {
        let mut held = BTreeSet::new();
        if records.is_empty() {
            return Ok(held);
        }
        let Some((_, owner)) = self.native_publication_owner_for_observation() else {
            return Ok(held);
        };
        let Some(layout) =
            owner
                .selected_original_data_layout()
                .map_err(|error| DfsError::Metadata {
                    message: error.to_string(),
                })?
        else {
            return Ok(held);
        };
        let store = layout.queue()?;
        for record in records {
            if record.transfer_kind
                != Some(sinomo_api::dfs::maintenance::DfsRepairTransferKind::Repair)
            {
                continue;
            }
            let Some(birth) = store
                .original_repair_for_slot(&record.intent_id, &record.missing_replica_node_id)?
            else {
                continue;
            };
            let DfsNetworkMessage::RepairReplica(command) = &birth.original.message else {
                continue;
            };
            if record.transfer_request_id.as_ref() == Some(&command.request_id)
                && record.intent_id == command.intent_id
                && record.object_id == command.object_id
                && record.version == command.version
                && record.content_hash == command.content_hash
                && record.missing_replica_node_id == birth.original.target_node_id
                && store.native_original_completion_pending(&birth.original)?
            {
                held.insert((
                    record.intent_id.clone(),
                    record.missing_replica_node_id.clone(),
                ));
            }
        }
        Ok(held)
    }

    fn prepare_repair_fanout_jobs(
        &mut self,
        job: &DistributedRepairJob,
        pending: &PendingNetworkedWrite,
        repairs: &[DistributedRepairJob],
        remaining: usize,
        exclusions: &DfsMaintenanceExecutionExclusions,
        consumed: &mut BTreeSet<(String, NodeId)>,
    ) -> Result<Vec<(DistributedRepairJob, String)>, DfsError> {
        let Some(request_id) = self.original_repair_request_for_target(
            &job.intent_id,
            &job.missing_replica_node_id,
            pending,
        )?
        else {
            return Ok(Vec::new());
        };
        let mut fanout = vec![(job.clone(), request_id)];
        if remaining > 1
            && let Some(companion) =
                self.repair_fanout_companion(job, repairs, exclusions, consumed)
            && self.restore_pending_write_for_repair_job(&companion)?
                == crate::service_commit::RepairProducerCustodyReadiness::Ready
            && self.prepare_repair_target(&companion.intent_id, &companion.missing_replica_node_id)
        {
            let companion_pending = self
                .writes
                .pending_writes
                .get(&companion.intent_id)
                .cloned()
                .expect("same-intent pending repair validated");
            if self.pending_repair_targets_current_content(&companion, &companion_pending)? {
                let Some(request_id) = self.original_repair_request_for_target(
                    &companion.intent_id,
                    &companion.missing_replica_node_id,
                    &companion_pending,
                )?
                else {
                    return Ok(fanout);
                };
                consumed.insert((
                    companion.intent_id.clone(),
                    companion.missing_replica_node_id.clone(),
                ));
                fanout.push((companion, request_id));
            }
        }
        Ok(fanout)
    }

    pub(crate) fn replica_queue_matches_current_transfer(
        &self,
        intent_id: &str,
        target: &NodeId,
        request_id: &str,
        stage: bool,
    ) -> bool {
        use sinomo_api::dfs::maintenance::DfsRepairTransferKind;
        let Some(pending) = self.writes.pending_writes.get(intent_id) else {
            return false;
        };
        let kind = if stage {
            DfsRepairTransferKind::Stage
        } else {
            DfsRepairTransferKind::Repair
        };
        self.repair_attempt_records
            .get(&(intent_id.to_string(), target.clone()))
            .is_some_and(|row| {
                row.transfer_kind == Some(kind)
                    && row.transfer_request_id.as_deref() == Some(request_id)
                    && row.intent_id == intent_id
                    && row.object_id == pending.object_id
                    && row.version == pending.version
                    && row.content_hash == pending.content_hash
                    && row.missing_replica_node_id == *target
            })
    }

    pub(crate) fn begin_replica_transfer_attempt(
        &mut self,
        intent_id: &str,
        target: &NodeId,
        request_id: &str,
        stage: bool,
        _background_repair: bool,
    ) -> Result<(), DfsError> {
        use sinomo_api::dfs::maintenance::DfsRepairTransferKind;
        let kind = if stage {
            DfsRepairTransferKind::Stage
        } else {
            DfsRepairTransferKind::Repair
        };
        #[cfg(any(test, feature = "test-support"))]
        if let Some((root, _owner)) = self.native_publication_owner_for_observation()
            && let Some(hook) = crate::runtime_host::daemon_dfs_pre_dispatch_test_hook(
                &sinomo_core::local::SinomoDataDir::new(root),
            )
        {
            let prior_transfer = self
                .repair_attempt_records
                .get(&(intent_id.to_string(), target.clone()))
                .cloned();
            hook(
                crate::runtime_host::DaemonDfsPreDispatchTestPoint::BeforeReplicaTransferClaim {
                    kind,
                    background_repair: _background_repair,
                    intent_id_digest: *blake3::hash(intent_id.as_bytes()).as_bytes(),
                    target_id_digest: *blake3::hash(target.as_str().as_bytes()).as_bytes(),
                    request_id_digest: *blake3::hash(request_id.as_bytes()).as_bytes(),
                    prior_transfer: prior_transfer.as_ref(),
                },
                None,
                None,
            )?;
        }
        let key = (intent_id.to_string(), target.clone());
        if self.repair_attempt_records.get(&key).is_some_and(|record| {
            record
                .transfer_request_id
                .as_deref()
                .is_some_and(|existing| {
                    record.transfer_kind != Some(kind) || existing != request_id
                })
        }) {
            return Err(DfsError::Metadata {
                message: "replica transfer cannot replace unresolved durable request custody"
                    .into(),
            });
        }
        if self.repair_attempt_records.get(&key).is_some_and(|row| {
            row.transfer_kind.is_none()
                || (row.transfer_request_id.is_none()
                    && row.transfer_delivery_state
                        != Some(sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent)
                    && row.completed_transfer_request_id.is_none())
        }) {
            return Err(DfsError::Metadata {
                message: "replica transfer cannot replace unidentified UNKNOWN durable custody"
                    .into(),
            });
        }
        let Some(pending) = self.writes.pending_writes.get_mut(intent_id) else {
            return Ok(());
        };
        if self.repair_attempt_records.get(&key).is_some_and(|row| {
            row.object_id != pending.object_id
                || row.version != pending.version
                || row.content_hash != pending.content_hash
                || row.missing_replica_node_id != *target
        }) {
            return Err(DfsError::Metadata {
                message: "replica transfer differs from its exact durable immutable identity"
                    .into(),
            });
        }
        if (stage && pending.repair_inflight_request_ids.contains_key(target))
            || (!stage && pending.stage_inflight_request_ids.contains_key(target))
        {
            return Err(DfsError::Metadata {
                message: "replica transfer retains unresolved custody for its other operation kind"
                    .into(),
            });
        }
        let (requests, retries) = if stage {
            (
                &mut pending.stage_inflight_request_ids,
                &mut pending.stage_retries,
            )
        } else {
            (
                &mut pending.repair_inflight_request_ids,
                &mut pending.repair_retries,
            )
        };
        if requests
            .get(target)
            .is_some_and(|existing| existing != request_id)
        {
            return Err(DfsError::Metadata {
                message: "replica transfer cannot replace an unresolved request identity".into(),
            });
        }
        let state = retries.entry(target.clone()).or_default();
        if state.unresolved_custody_without_request {
            return Err(DfsError::Metadata {
                message: "replica transfer retains imported unknown custody without an exact request identity".into(),
            });
        }
        state.retrying_unresolved_transfer =
            requests.contains_key(target) || state.unresolved_custody_without_request;
        state.previous_transfer_delivery_state = state.transfer_delivery_state;
        state.completed_transfer_request_id = None;
        // Before polling the carrier, its current outcome is UNKNOWN. A prior
        // unresolved attempt remains independent evidence for this same ID.
        state.transfer_delivery_state = None;
        requests.insert(target.clone(), request_id.to_string());
        state.inflight = true;
        if !stage || self.repair_attempt_records.contains_key(&key) {
            let record =
                self.repair_attempt_records
                    .entry(key)
                    .or_insert_with(|| DfsRepairAttemptRecord {
                        intent_id: intent_id.to_string(),
                        object_id: pending.object_id.clone(),
                        version: pending.version.clone(),
                        content_hash: pending.content_hash.clone(),
                        missing_replica_node_id: target.clone(),
                        attempts: state.attempts,
                        in_flight: true,
                        last_attempt_unix_nanos: dfs_now_unix_nanos(),
                        next_retry_unix_nanos: dfs_now_unix_nanos(),
                        transfer_request_id: Some(request_id.to_string()),
                        last_error: None,
                        transfer_delivery_state: None,
                        completed_transfer_request_id: None,
                        transfer_kind: Some(kind),
                    });
            record.in_flight = true;
            record.transfer_request_id = Some(request_id.to_string());
            record.transfer_kind = Some(kind);
            record.transfer_delivery_state = None;
            record.completed_transfer_request_id = None;
        }
        Ok(())
    }

    pub(crate) fn observe_replica_transfer_delivery(
        &mut self,
        intent_id: &str,
        target: &NodeId,
        request_id: &str,
        stage: bool,
        outcome: &StageBlobSendOutcome,
    ) -> Result<(), DfsError> {
        use sinomo_core::network::NetworkDeliveryState;
        let Some(pending) = self.writes.pending_writes.get_mut(intent_id) else {
            return Ok(());
        };
        let (requests, retries) = if stage {
            (
                &mut pending.stage_inflight_request_ids,
                &mut pending.stage_retries,
            )
        } else {
            (
                &mut pending.repair_inflight_request_ids,
                &mut pending.repair_retries,
            )
        };
        if requests.get(target).map(String::as_str) != Some(request_id) {
            return Err(DfsError::Metadata {
                message: "replica send result differs from its actual pending request".into(),
            });
        }
        let state = retries.entry(target.clone()).or_default();
        state.transfer_delivery_state = if outcome.sent {
            Some(NetworkDeliveryState::PossiblyApplied)
        } else {
            if state.retrying_unresolved_transfer {
                join_transfer_delivery(
                    state.previous_transfer_delivery_state,
                    outcome.delivery_state,
                )
            } else {
                outcome.delivery_state
            }
        };
        state.inflight = outcome.sent;
        if state.transfer_delivery_state == Some(NetworkDeliveryState::DefinitelyNotSent) {
            requests.remove(target);
        }
        let kind = if stage {
            sinomo_api::dfs::maintenance::DfsRepairTransferKind::Stage
        } else {
            sinomo_api::dfs::maintenance::DfsRepairTransferKind::Repair
        };
        if let Some(record) = self
            .repair_attempt_records
            .get_mut(&(intent_id.to_string(), target.clone()))
        {
            if record.transfer_kind == Some(kind)
                && record.transfer_request_id.as_deref() == Some(request_id)
                && record.object_id == pending.object_id
                && record.version == pending.version
                && record.content_hash == pending.content_hash
            {
                record.in_flight = state.inflight;
                record.transfer_request_id = requests.get(target).cloned();
                record.transfer_delivery_state = state.transfer_delivery_state;
                record.completed_transfer_request_id = None;
            }
        }

        Ok(())
    }

    pub(crate) fn record_replica_transfer_completed(
        &mut self,
        intent_id: &str,
        target: &NodeId,
        request_id: &str,
        stage: bool,
    ) -> Result<(), DfsError> {
        let key = (intent_id.to_string(), target.clone());
        let exact_queue =
            self.replica_queue_matches_current_transfer(intent_id, target, request_id, stage);
        let queue_requires_exact = !stage || self.repair_attempt_records.contains_key(&key);
        if queue_requires_exact && !exact_queue {
            return Err(DfsError::Distributed {
                message: "terminal replica response has no exact current same-kind queue custody"
                    .into(),
            });
        }
        let pending = self
            .writes
            .pending_writes
            .get_mut(intent_id)
            .ok_or_else(|| DfsError::Distributed {
                message: "terminal replica response has no current pending transfer".into(),
            })?;
        let (requests, retries) = if stage {
            (
                &mut pending.stage_inflight_request_ids,
                &mut pending.stage_retries,
            )
        } else {
            (
                &mut pending.repair_inflight_request_ids,
                &mut pending.repair_retries,
            )
        };
        if requests.get(target).map(String::as_str) != Some(request_id) {
            return Err(DfsError::Distributed {
                message: "terminal replica response differs from its exact current request".into(),
            });
        }
        requests.remove(target);
        let state = retries.entry(target.clone()).or_default();
        state.inflight = false;
        state.completed_transfer_request_id = Some(request_id.to_string());
        if exact_queue {
            let record = self
                .repair_attempt_records
                .get_mut(&key)
                .expect("exact current same-kind row checked");
            record.in_flight = false;
            record.transfer_request_id = None;
            record.completed_transfer_request_id = Some(request_id.to_string());
            record.transfer_delivery_state = state.transfer_delivery_state;
        }
        Ok(())
    }

    fn record_repair_fanout_outcomes(
        &mut self,
        fanout: Vec<(DistributedRepairJob, String)>,
        outcomes: Vec<StageBlobSendOutcome>,
        scheduled: &mut Vec<DistributedRepairJob>,
    ) {
        for ((job, request_id), outcome) in fanout.into_iter().zip(outcomes) {
            if outcome.sent {
                self.record_repair_inflight(
                    &job.intent_id,
                    &job.missing_replica_node_id,
                    request_id,
                );
                scheduled.push(job);
            } else {
                self.record_repair_retry_scheduled_with_error(
                    &job.intent_id,
                    &job.missing_replica_node_id,
                    outcome.last_error,
                );
            }
        }
    }

    fn try_prepare_managed_background_repair_batch(
        &mut self,
        repairs: &[DistributedRepairJob],
        first_index: usize,
        limit: usize,
        exclusions: &DfsMaintenanceExecutionExclusions,
        consumed: &BTreeSet<(String, NodeId)>,
        placement_plans: &[DfsPlacementReconciliationJob],
    ) -> Result<Option<ManagedRepairPreparation>, DfsError> {
        let Some(first) = repairs.get(first_index) else {
            return Ok(None);
        };
        let Some(job_id) =
            crate::placement::placement_reconciliation_job_id_from_intent_id(&first.intent_id)
                .map(str::to_owned)
        else {
            return Ok(None);
        };
        if limit == 0 || !self.transport.supports_background_replica_dispatch() {
            return Ok(None);
        }
        let mut candidates = Vec::new();
        let mut seen = BTreeSet::new();
        for repair in repairs
            .iter()
            .skip(first_index)
            .take(DFS_REPAIR_EXAMINED_RECORDS_PER_PAGE)
        {
            if exclusions.contains(&repair.missing_replica_node_id)
                || (repair != first
                    && consumed.contains(&(
                        repair.intent_id.clone(),
                        repair.missing_replica_node_id.clone(),
                    )))
            {
                continue;
            }
            if seen.contains(&repair.intent_id) {
                continue;
            }
            if repair.file_system_id != first.file_system_id
                || crate::placement::placement_reconciliation_job_id_from_intent_id(
                    &repair.intent_id,
                ) != Some(job_id.as_str())
                || repair.missing_replica_node_id == self.node_id
                || !self.repair_retry_is_due(&repair.intent_id, &repair.missing_replica_node_id)
                || self.stage_request_is_active(&repair.intent_id, &repair.missing_replica_node_id)
                || self
                    .writes
                    .pending_remote_write_responses
                    .contains_key(&repair.intent_id)
            {
                break;
            }
            if self.restore_pending_write_for_repair_job(repair)?
                == crate::service_commit::RepairProducerCustodyReadiness::Parked
            {
                continue;
            }
            let Some(intent) = self.coordinator.intent(&repair.intent_id).cloned() else {
                break;
            };
            let Some(pending) = self.writes.pending_writes.get(&repair.intent_id).cloned() else {
                break;
            };
            let Ok(object) = self.current_read_object_metadata(&pending.object_id) else {
                break;
            };
            if !intent.publication.is_current()
                || intent.state != DfsWriteIntentState::Committed
                || object.version != pending.version
                || candidates.iter().any(
                    |(_, candidate, _, _): &(
                        DistributedRepairJob,
                        DistributedWriteIntent,
                        PendingNetworkedWrite,
                        DfsObjectMetadata,
                    )| candidate.object_id == intent.object_id,
                )
                || !self
                    .pending_repair_is_locally_authoritative(&pending)
                    .unwrap_or(false)
                || !self
                    .pending_repair_targets_current_content(repair, &pending)
                    .unwrap_or(false)
                || self
                    .retained_content_transfer_required(
                        &repair.intent_id,
                        &pending.object_id,
                        &pending.version,
                    )
                    .unwrap_or(true)
                || !matches!(
                    self.superseding_repair_plan(repair, placement_plans),
                    Ok(None)
                )
                || self.open_pending_write_blob_stream(&pending).is_err()
            {
                break;
            }
            seen.insert(repair.intent_id.clone());
            candidates.push((repair.clone(), intent, pending, object));
        }
        if candidates.is_empty() {
            return Ok(None);
        }
        let obsolete = candidates
            .iter()
            .map(|(_, intent, _, _)| intent.clone())
            .collect::<Vec<_>>();
        let result = self.with_authoritative_placement_reconciliation_completion(
            &first.file_system_id,
            &job_id,
            None,
            candidates.len(),
            |service, job, config| {
                let mut stale = Vec::new();
                let mut error = None;
                let mut recheck_current = false;
                let mut dispatches = Vec::new();
                let mut processed = BTreeSet::new();
                let mut selected = consumed.clone();
                let mut remaining = limit;
                for (repair, intent, pending, _) in candidates {
                    let object = match service.current_read_object_metadata(&pending.object_id) {
                        Ok(object) => object,
                        Err(failed) => {
                            error = Some(failed);
                            break;
                        }
                    };
                    if object.version != pending.version
                        || object.content_hash.as_deref() != Some(pending.content_hash.as_str())
                        || !service.pending_repair_targets_current_content(&repair, &pending)?
                    {
                        recheck_current = true;
                        break;
                    }
                    let valid = match service
                        .managed_placement_intent_matches_job(&intent, &object, job, config)
                    {
                        Ok(valid) => valid,
                        Err(failed) => {
                            error = Some(failed);
                            break;
                        }
                    };
                    if !valid {
                        for target in repairs
                            .iter()
                            .filter(|target| target.intent_id == intent.intent_id)
                        {
                            processed.insert((
                                target.intent_id.clone(),
                                target.missing_replica_node_id.clone(),
                            ));
                        }
                        stale.push(intent);
                        continue;
                    }
                    if remaining == 0 {
                        break;
                    }
                    if !service
                        .prepare_repair_target(&repair.intent_id, &repair.missing_replica_node_id)
                    {
                        break;
                    }
                    selected.insert((
                        repair.intent_id.clone(),
                        repair.missing_replica_node_id.clone(),
                    ));
                    let prepared_result = (|| {
                        let fanout = service.prepare_repair_fanout_jobs(
                            &repair,
                            &pending,
                            repairs,
                            remaining,
                            exclusions,
                            &mut selected,
                        )?;
                        remaining -= fanout.len();
                        let targets = fanout
                            .iter()
                            .map(|(target, id)| {
                                (target.missing_replica_node_id.clone(), id.clone())
                            })
                            .collect::<Vec<_>>();
                        let prepared = service.prepare_current_background_repair_replica_sends(
                            &targets, &intent, &pending, object,
                        )?;
                        Ok::<_, DfsError>((prepared, fanout))
                    })();
                    let (prepared, fanout) = match prepared_result {
                        Ok(prepared) => prepared,
                        Err(failed) => {
                            error = Some(failed);
                            break;
                        }
                    };
                    for (target, _) in &fanout {
                        processed.insert((
                            target.intent_id.clone(),
                            target.missing_replica_node_id.clone(),
                        ));
                    }
                    dispatches.push((prepared, fanout));
                }
                service.retire_stale_placement_intents(&stale)?;
                Ok((
                    PreparedManagedRepairBatch {
                        error,
                        dispatches,
                        processed,
                    },
                    recheck_current,
                ))
            },
        )?;
        if let Some(((mut batch, recheck_current), release)) = result {
            if let Err(error) = release {
                batch.dispatches.clear();
                batch.processed.clear();
                batch.error = Some(error);
            }
            if batch.processed.is_empty() && batch.error.is_none() {
                return Ok(recheck_current.then_some(ManagedRepairPreparation::RecheckCurrent));
            }
            return Ok(Some(ManagedRepairPreparation::Prepared(batch)));
        }
        // None here is JOB-level stale evidence: this acquisition deliberately
        // did not use the first object's path. It can retire execution ownership
        // but cannot grant dispatch, metadata replacement, or receipt authority.
        self.retire_stale_placement_intents(&obsolete)?;
        let processed = repairs
            .iter()
            .filter(|repair| {
                obsolete
                    .iter()
                    .any(|intent| intent.intent_id == repair.intent_id)
            })
            .map(|repair| {
                (
                    repair.intent_id.clone(),
                    repair.missing_replica_node_id.clone(),
                )
            })
            .collect();
        Ok(Some(ManagedRepairPreparation::Prepared(
            PreparedManagedRepairBatch {
                error: None,
                dispatches: Vec::new(),
                processed,
            },
        )))
    }

    /// Drain only the repair jobs created while serving the current message.
    ///
    /// A committed repair job retains its pending write until it is repaired
    /// or loses authority, so the pending record is the exact source for the
    /// content identity persisted in the daemon's local durable queue. Jobs
    /// without a pending write are intentionally omitted, matching
    /// `durable_repair_queue_records`: they are not executable by this lane.
    pub(crate) fn take_message_committed_repair_queue_records(
        &mut self,
    ) -> Vec<DfsRepairAttemptRecord> {
        let now = dfs_now_unix_nanos();
        let mut records = BTreeMap::new();
        for job in self.coordinator.take_message_repair_jobs() {
            let Some(pending) = self.writes.pending_writes.get_mut(&job.intent_id) else {
                continue;
            };
            let key = (job.intent_id.clone(), job.missing_replica_node_id.clone());
            if let Some(existing) = self.repair_attempt_records.get(&key) {
                records.insert(key, existing.clone());
                continue;
            }
            let (state, transfer_id, kind) =
                pending_repair_transfer_observation(pending, &job.missing_replica_node_id);
            let record = DfsRepairAttemptRecord {
                intent_id: job.intent_id,
                object_id: pending.object_id.clone(),
                version: pending.version.clone(),
                content_hash: pending.content_hash.clone(),
                missing_replica_node_id: job.missing_replica_node_id,
                attempts: state.map_or(0, |state| state.attempts),
                in_flight: state.is_some_and(|state| state.inflight),
                last_attempt_unix_nanos: 0,
                next_retry_unix_nanos: now,
                transfer_request_id: transfer_id.cloned(),
                last_error: None,
                transfer_delivery_state: state.and_then(|state| state.transfer_delivery_state),
                completed_transfer_request_id: state
                    .and_then(|state| state.completed_transfer_request_id.clone()),
                transfer_kind: Some(kind),
            };
            // Capturing a missing target describes scheduling work, not an
            // observed producer. Do not install this derived snapshot as live
            // custody: another lane may already own a named durable request.
            // Actual producer/queue records are retained by their existing paths.
            records.insert(key, record);
        }
        records.into_values().collect()
    }

    /// Number of new stage sends that may be reserved for this intent now.
    /// Background writes reserve only the exact W deficit; foreground writes
    /// may reserve every missing desired replica. A timed-out inflight request
    /// is treated as a freed reservation so its exact request can be retried.
    fn pending_stage_send_budget(
        &self,
        intent: &DistributedWriteIntent,
        pending: &PendingNetworkedWrite,
        durable_nodes: &BTreeSet<NodeId>,
        execution_exclusions: Option<&DfsMaintenanceExecutionExclusions>,
    ) -> usize {
        let durable_desired = intent
            .desired_replica_nodes
            .iter()
            .filter(|node_id| durable_nodes.contains(*node_id))
            .count();
        let active_stage_reservations = pending
            .stage_inflight_request_ids
            .keys()
            .filter(|node_id| {
                intent.desired_replica_nodes.contains(*node_id)
                    && !durable_nodes.contains(*node_id)
                    && !execution_exclusions.is_some_and(|excluded| excluded.contains(node_id))
                    && !self.stage_retry_is_due(&intent.intent_id, node_id)
            })
            .cloned();
        let active_repair_reservations = pending
            .repair_inflight_request_ids
            .keys()
            .filter(|node_id| {
                intent.desired_replica_nodes.contains(*node_id)
                    && !durable_nodes.contains(*node_id)
                    && !execution_exclusions.is_some_and(|excluded| excluded.contains(node_id))
                    && !self.repair_retry_is_due(&intent.intent_id, node_id)
            })
            .cloned();
        let active_reservations = active_stage_reservations
            .chain(active_repair_reservations)
            .collect::<BTreeSet<_>>()
            .len();
        let target_receipts = if pending.foreground_replica_repair {
            intent.desired_replica_nodes.len()
        } else {
            intent.policy.min_commit_replicas
        };
        target_receipts.saturating_sub(durable_desired.saturating_add(active_reservations))
    }

    fn pending_stage_targets(
        &self,
        intent: &DistributedWriteIntent,
        pending: &PendingNetworkedWrite,
        durable_nodes: &BTreeSet<NodeId>,
        execution_exclusions: Option<&DfsMaintenanceExecutionExclusions>,
    ) -> Vec<NodeId> {
        let budget =
            self.pending_stage_send_budget(intent, pending, durable_nodes, execution_exclusions);
        intent
            .desired_replica_nodes
            .iter()
            .filter(|node_id| *node_id != &self.node_id && !durable_nodes.contains(*node_id))
            .filter(|node_id| {
                !execution_exclusions.is_some_and(|excluded| excluded.contains(node_id))
            })
            .filter(|node_id| self.stage_retry_is_due(&intent.intent_id, node_id))
            .filter(|node_id| !self.repair_request_is_active(&intent.intent_id, node_id))
            .take(budget)
            .cloned()
            .collect()
    }

    pub(crate) fn stage_request_is_active(
        &self,
        intent_id: &str,
        replica_node_id: &NodeId,
    ) -> bool {
        self.writes
            .pending_writes
            .get(intent_id)
            .is_some_and(|pending| {
                pending
                    .stage_inflight_request_ids
                    .contains_key(replica_node_id)
                    && !self.stage_retry_is_due(intent_id, replica_node_id)
            })
    }

    fn repair_request_is_active(&self, intent_id: &str, replica_node_id: &NodeId) -> bool {
        self.writes
            .pending_writes
            .get(intent_id)
            .is_some_and(|pending| {
                pending
                    .repair_inflight_request_ids
                    .contains_key(replica_node_id)
                    && !self.repair_retry_is_due(intent_id, replica_node_id)
            })
    }

    /// A timer cannot settle the prior Stage. Repair can begin only once its
    /// exact Stage request has a terminal reply or typed NotSent observation.
    fn prepare_repair_target(&mut self, intent_id: &str, replica_node_id: &NodeId) -> bool {
        if self
            .repair_attempt_records
            .get(&(intent_id.to_string(), replica_node_id.clone()))
            .is_some_and(|record| {
                record.transfer_request_id.is_some() && record.transfer_kind.is_none()
            })
        {
            return false;
        }
        let Some(pending) = self.writes.pending_writes.get(intent_id) else {
            return true;
        };
        !pending
            .stage_inflight_request_ids
            .contains_key(replica_node_id)
    }

    pub(crate) fn handle_repair_replica(
        &mut self,
        requester_node_id: NodeId,
        command: DfsRepairReplicaCommand,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let result = match self
            .ensure_current_replica_publication(&command.intent_id, &command.object_id)
            .and_then(|()| {
                self.ensure_replica_command_authorized_with_metadata(
                    &requester_node_id,
                    &command.intent_id,
                    &command.object_id,
                    command.object_metadata.as_ref(),
                    command.authority_shard.as_ref(),
                )
            })
            .and_then(|()| self.ensure_replica_object_metadata(command.object_metadata.as_ref()))
            .and_then(|()| {
                self.ensure_replica_command_hash_matches_intent(
                    &command.intent_id,
                    &command.object_id,
                    &command.content_hash,
                )
            })
            .and_then(|()| {
                self.ensure_no_conflicting_pending_replica_write(
                    "repair",
                    &requester_node_id,
                    &command.intent_id,
                    &command.object_id,
                    &command.version,
                    &command.content_hash,
                )
            })
            .and_then(|()| {
                self.ensure_command_base_matches_current(
                    "repair",
                    &command.object_id,
                    command.expected_base_version.as_ref(),
                    &command.version,
                    &command.content_hash,
                )
            })
            .and_then(|()| {
                let local_node_id = self.node_id.clone();
                self.ensure_metadata_receipt_accepts(
                    &command.object_id,
                    &command.version,
                    &local_node_id,
                    &command.content_hash,
                )
            })
            .and_then(|()| self.store_received_repair_content(&requester_node_id, &command))
        {
            Ok(()) => {
                let local_node_id = self.node_id.clone();
                match self.commit_replica_metadata_cache(
                    &command.file_system_id,
                    &command.object_id,
                    &command.version,
                    &local_node_id,
                    &command.content_hash,
                    command.object_metadata.as_ref(),
                ) {
                    Ok(()) => DfsBlobTransferResult::Committed {
                        content_hash: command.content_hash.clone(),
                    },
                    Err(error) => DfsBlobTransferResult::Rejected {
                        reason: error.to_string(),
                    },
                }
            }
            Err(error) => DfsBlobTransferResult::Rejected {
                reason: error.to_string(),
            },
        };
        self.send_repair_replica_response(
            &requester_node_id,
            command.request_id,
            command.intent_id.clone(),
            command.file_system_id,
            command.object_id.clone(),
            command.version.clone(),
            result.clone(),
        )?;
        Ok(self.repair_replica_result_event(
            command.intent_id,
            command.object_id,
            command.version,
            result,
        ))
    }

    /// Prospective A-only receiver foundation. Selected errors propagate with
    /// no domain reply: an uncertain durable phase is never a Rejected result.
    pub(crate) fn handle_selected_original_repair(&mut self, requester: NodeId, selected: DfsSelectedOriginalRepairCommand) -> Result<NetworkedDfsServiceEvent, DfsError> {
        self.content.requires_original_repair_fixture()?;
        selected.material.validate()?;
        let material = &selected.material;
        let command = &material.original.command;
        if selected.version != 1 || requester != material.original.source_node_id || self.node_id != material.original.target_node_id {
            return Err(DfsError::Metadata { message: "selected Repair authenticated original/executor/receiver differs".into() });
        }
        let backend = self.metadata_backend.as_ref().ok_or_else(|| DfsError::Metadata { message: "selected Repair requires its actual native backend".into() })?;
        let object = backend.get_object(&command.object_id)?.ok_or_else(|| DfsError::UnknownObject { object_id: command.object_id.clone() })?;
        ensure_selected_original_exact_current_content(&object.version, object.content_hash.as_deref(), command)?;
        let intent = backend.get_intent(&command.object_id, &command.intent_id)?;
        let intent = checked_selected_original_intent(material, &self.node_id, intent.as_ref())?;
        let stamp = command.authority_shard.as_ref().ok_or_else(|| DfsError::Metadata { message: "selected Repair original has no authority stamp".into() })?;
        let owner = backend.publication_owner_scope(&command.file_system_id, &DfsMetadataShardId::new(stamp.shard_id.clone()))?;
        let execution = self.publication_execution.current().ok_or_else(|| DfsError::Metadata { message: "selected Repair has no actual native mutation admission".into() })?;
        let provider = self.publication_provider.current().ok_or_else(|| DfsError::Metadata { message: "selected Repair has no current native publication provider".into() })?;
        let admission = provider.admit_content_only_original_repair(execution, owner, material, &object, &self.node_id, intent)?;
        // These existing checks are read-only. In particular, the ordinary
        // potentially authoring object/helper and metadata-cache path are NOT
        // called anywhere in this selected branch.
        self.ensure_current_replica_publication(&command.intent_id, &command.object_id)?;
        self.ensure_replica_command_authorized_with_metadata(&requester, &command.intent_id, &command.object_id, command.object_metadata.as_ref(), command.authority_shard.as_ref())?;
        self.ensure_replica_command_hash_matches_intent(&command.intent_id, &command.object_id, &command.content_hash)?;
        self.ensure_no_conflicting_pending_replica_write("selected repair", &requester, &command.intent_id, &command.object_id, &command.version, &command.content_hash)?;
        self.ensure_command_base_matches_current("selected repair", &command.object_id, command.expected_base_version.as_ref(), &command.version, &command.content_hash)?;
        self.ensure_metadata_receipt_accepts(&command.object_id, &command.version, &self.node_id, &command.content_hash)?;
        let prepared = self.content.prepare_original_repair(material, &selected.bytes, &admission)?;
        #[cfg(test)]
        self.content.received_repair_after_prepared_test_cut()?;
        let bound = self.content.bind_original_repair_content(&prepared, &selected.bytes)?;
        admission.revalidate(material)?;
        let object = self.metadata_backend.as_ref().ok_or_else(|| DfsError::Metadata { message: "selected Repair lost its actual native backend".into() })?
            .get_object(&command.object_id)?.ok_or_else(|| DfsError::UnknownObject { object_id: command.object_id.clone() })?;
        ensure_selected_original_exact_current_content(&object.version, object.content_hash.as_deref(), command)?;
        let current_intent = self.metadata_backend.as_ref().ok_or_else(|| DfsError::Metadata { message: "selected Repair lost its actual native backend".into() })?
            .get_intent(&command.object_id, &command.intent_id)?;
        admission.check_intent(current_intent.as_ref())?;
        // Explicitly local cache only. Never call add_file_system,
        // add_object_metadata, put_metadata_receipt, or a V7 publication helper.
        if !self.metadata.filesystems.contains_key(&object.file_system_id) {
            self.metadata.add_file_system(DfsFileSystem { file_system_id: object.file_system_id.clone(), display_name: object.file_system_id.to_string(), owner_node_id: object.owner_node_id.clone(), retention_policies: Default::default(), module_metadata: Default::default(), subtree_policies: Vec::new() });
        }
        self.metadata.add_object(object)?;
        self.commit_local_metadata(&command.object_id, &command.version, &command.content_hash, dfs_now_unix_nanos())?;
        let response = DfsRepairReplicaResponse { request_id: command.request_id.clone(), intent_id: command.intent_id.clone(), file_system_id: command.file_system_id.clone(), object_id: command.object_id.clone(), version: command.version.clone(), replica_node_id: self.node_id.clone(), result: DfsBlobTransferResult::Committed { content_hash: command.content_hash.clone() } };
        let outcome = NativeOriginalRepairOutcome { admission, response, prepared_seal: bound.prepared_seal().into(), bound_id: bound.bound_id() };
        let response = self.content.finish_original_repair(&bound, &outcome)?;
        self.send_dfs_message_if_routable(&requester, DfsNetworkMessage::RepairReplicaResponse(response.clone()))?;
        Ok(self.repair_replica_result_event(response.intent_id, response.object_id, response.version, response.result))
    }

    // This local fixture-gated content proof does not replace any preflight,
    // later metadata publication or reply handling. Default stores return None
    // without I/O and take precisely the existing V1 content path.
    fn store_received_repair_content(
        &self,
        requester: &NodeId,
        command: &DfsRepairReplicaCommand,
    ) -> Result<(), DfsError> {
        fn content_error(error: crate::ContentStoreError) -> DfsError {
            match error {
                crate::ContentStoreError::BlobHashMismatch { expected, actual } => {
                    DfsError::ContentHashMismatch { expected, actual }
                }
                error => error.into(),
            }
        }
        let Some(prepared) = self
            .content
            .prepare_received_repair(requester, &self.node_id, command)
            .map_err(content_error)?
        else {
            return self.store_command_bytes(
                &command.object_id,
                &command.version,
                &command.content_hash,
                &command.bytes,
            );
        };
        #[cfg(test)]
        self.content
            .received_repair_after_prepared_test_cut()
            .map_err(content_error)?;
        let effect = self
            .content
            .store_prepared_received_repair(&prepared, &command.bytes)
            .map_err(content_error)?;
        if effect.captured_request_id() != command.request_id
            || effect.manifest().object_id != command.object_id.as_str()
            || effect.manifest().version != command.version.as_str()
            || effect.manifest().content_hash != command.content_hash
        {
            return Err(DfsError::Distributed {
                message: "received Repair content witness changed its invocation".into(),
            });
        }
        Ok(())
    }

    /// A served reply can run in a different service lane from its sender.
    /// Recover only the actual named queue custody under this execution owner;
    /// the metadata owner and the strict response matcher remain required.
    fn restore_repair_response_custody(
        &mut self,
        response: &DfsRepairReplicaResponse,
    ) -> Result<(), DfsError> {
        use sinomo_api::dfs::maintenance::DfsRepairTransferKind;
        use sinomo_core::network::NetworkDeliveryState;
        if self
            .ensure_repair_response_request_matches_pending(response)
            .is_ok()
        {
            return Ok(());
        }
        let Some(_execution) = self.publication_execution.current() else {
            return Ok(());
        };
        let Some(store) = self.owned_repair_queue_store()? else {
            return Ok(());
        };
        let records = store.records_for_targets(
            &response.intent_id,
            std::slice::from_ref(&response.replica_node_id),
        )?;
        let invalid = || DfsError::Distributed {
            message: "DFS repair response has no exact durable same-kind request custody".into(),
        };
        let mut record = records.into_iter().next().ok_or_else(invalid)?;
        let pending = self
            .writes
            .pending_writes
            .get(&response.intent_id)
            .ok_or_else(invalid)?;
        if record.transfer_kind != Some(DfsRepairTransferKind::Repair)
            || record.transfer_request_id.as_ref() != Some(&response.request_id)
            || record.completed_transfer_request_id.is_some()
            || record.intent_id != response.intent_id
            || record.object_id != response.object_id
            || record.version != response.version
            || record.missing_replica_node_id != response.replica_node_id
            || record.object_id != pending.object_id
            || record.version != pending.version
            || record.content_hash != pending.content_hash
            || pending
                .repair_inflight_request_ids
                .get(&response.replica_node_id)
                .is_some_and(|current| current != &response.request_id)
            || pending
                .stage_inflight_request_ids
                .contains_key(&response.replica_node_id)
            || pending
                .repair_retries
                .get(&response.replica_node_id)
                .is_some_and(|state| {
                    state.unresolved_custody_without_request
                        || state.completed_transfer_request_id.as_ref()
                            == Some(&response.request_id)
                        || (!pending
                            .repair_inflight_request_ids
                            .contains_key(&response.replica_node_id)
                            && (state.inflight
                                || (state.transfer_delivery_state
                                    != Some(NetworkDeliveryState::DefinitelyNotSent)
                                    && state.completed_transfer_request_id.is_none())))
                })
            || pending
                .stage_retries
                .get(&response.replica_node_id)
                .is_some_and(|state| {
                    state.unresolved_custody_without_request
                        || state.inflight
                        || (state.transfer_delivery_state
                            != Some(NetworkDeliveryState::DefinitelyNotSent)
                            && state.completed_transfer_request_id.is_none())
                })
        {
            return Err(invalid());
        }
        if let Some(prior) = self
            .repair_attempt_records
            .get(&(response.intent_id.clone(), response.replica_node_id.clone()))
        {
            if prior.completed_transfer_request_id.as_ref() == Some(&response.request_id)
                || prior.object_id != record.object_id
                || prior.version != record.version
                || prior.content_hash != record.content_hash
                || (repair_transfer_custody_unresolved(prior)
                    && (prior.transfer_kind != record.transfer_kind
                        || prior.transfer_request_id != record.transfer_request_id))
            {
                return Err(invalid());
            }
            if repair_transfer_custody_unresolved(prior) {
                record.transfer_delivery_state = join_transfer_delivery(
                    prior.transfer_delivery_state,
                    record.transfer_delivery_state,
                );
            }
        }
        self.restore_repair_attempt_records_merging(&[record]);
        Ok(())
    }

    /// Persist an actual correlated terminal carrier response before releasing
    /// its logical claim. The caller retains its current authority checks and,
    /// for durable success, stores the actual metadata receipt first. Queue CAS
    /// rejection leaves the pending request intact for the SAME reply retry.
    fn record_owned_repair_terminal_response(
        &mut self,
        response: &DfsRepairReplicaResponse,
    ) -> Result<(), DfsError> {
        let mut completed_retry_due = None;
        if let Some(store) = self.owned_repair_queue_store()? {
            let (records, observation) = store.observe_transfer_targets(
                &response.intent_id,
                std::slice::from_ref(&response.replica_node_id),
            )?;
            if let Some(before) = records.first() {
                use sinomo_api::dfs::maintenance::DfsRepairTransferKind;
                use sinomo_core::network::NetworkDeliveryState;
                let pending = self
                    .writes
                    .pending_writes
                    .get(&response.intent_id)
                    .expect("strict actual reply matcher retained pending write");
                let invalid = || DfsError::Metadata {
                    message: "repair completion differs from its actual current durable claim"
                        .into(),
                };
                if before.transfer_kind != Some(DfsRepairTransferKind::Repair)
                    || before.intent_id != response.intent_id
                    || before.object_id != response.object_id
                    || before.version != response.version
                    || before.missing_replica_node_id != response.replica_node_id
                    || before.object_id != pending.object_id
                    || before.version != pending.version
                    || before.content_hash != pending.content_hash
                {
                    return Err(invalid());
                }
                let private_birth = before.transfer_request_id.is_none()
                    && !before.in_flight
                    && before.completed_transfer_request_id.is_none()
                    && before.transfer_delivery_state
                        == Some(NetworkDeliveryState::DefinitelyNotSent)
                    && pending
                        .repair_inflight_request_ids
                        .get(&response.replica_node_id)
                        == Some(&response.request_id);
                match before.transfer_request_id.as_deref() {
                    current
                        if before.completed_transfer_request_id.is_none()
                            && (current == Some(response.request_id.as_str()) || private_birth) =>
                    {
                        // A private producer can finish while the durable row
                        // still holds its unclaimed birth. Keep the real named
                        // producer's history and checkpoint this accepted reply
                        // before a later producer snapshot can republish it.
                        let mut desired = if private_birth {
                            self.repair_attempt_records
                                .get(&(
                                    response.intent_id.clone(),
                                    response.replica_node_id.clone(),
                                ))
                                .filter(|actual| {
                                    actual.intent_id == before.intent_id
                                        && actual.object_id == before.object_id
                                        && actual.version == before.version
                                        && actual.content_hash == before.content_hash
                                        && actual.missing_replica_node_id
                                            == before.missing_replica_node_id
                                        && actual.transfer_kind
                                            == Some(DfsRepairTransferKind::Repair)
                                        && actual.transfer_request_id.as_ref()
                                            == Some(&response.request_id)
                                        && actual.completed_transfer_request_id.is_none()
                                })
                                .cloned()
                                .ok_or_else(invalid)?
                        } else {
                            before.clone()
                        };
                        desired.in_flight = false;
                        desired.transfer_request_id = None;
                        desired.completed_transfer_request_id = Some(response.request_id.clone());
                        match &response.result {
                            DfsBlobTransferResult::Committed { .. }
                            | DfsBlobTransferResult::Durable { .. } => {
                                desired.next_retry_unix_nanos = 0;
                            }
                            DfsBlobTransferResult::Rejected { reason }
                                if !repair_rejection_indicates_lost_authority(reason) =>
                            {
                                let mut retry = pending
                                    .repair_retries
                                    .get(&response.replica_node_id)
                                    .cloned()
                                    .ok_or_else(invalid)?;
                                // The ordinary caller schedules after the exact
                                // terminal releases its logical request. Preview
                                // that SAME state transition before durable CAS.
                                retry.inflight = false;
                                if schedule_repair_retry_state(
                                    &mut retry,
                                    self.repair_retry_tick,
                                    None,
                                ) {
                                    let now = dfs_now_unix_nanos();
                                    desired.attempts = retry.attempts;
                                    desired.last_attempt_unix_nanos = now;
                                    desired.next_retry_unix_nanos =
                                        now.saturating_add(repair_retry_delay_nanos_for_ticks(
                                            retry
                                                .next_retry_tick
                                                .saturating_sub(self.repair_retry_tick),
                                        ));
                                }
                            }
                            // Existing lost-authority suppression is unchanged.
                            DfsBlobTransferResult::Rejected { .. } => {}
                        }
                        let checkpointed = if private_birth {
                            // The generic transfer checkpoint requires a
                            // durable named claim. This branch instead owns
                            // the exact private producer checked above and an
                            // actual accepted reply. CAS only its observed
                            // unclaimed birth through the existing page log.
                            matches!(
                                store.reconcile_maintenance_page(
                                    &observation,
                                    0,
                                    std::slice::from_ref(before),
                                    std::slice::from_ref(&desired),
                                    &[],
                                    &[],
                                )?,
                                crate::repair_queue_store::DfsRepairQueuePageReconcileOutcome::Applied { .. }
                            )
                        } else {
                            store.checkpoint_transfer_record(before, &desired)?
                        };
                        if !checkpointed {
                            return Err(DfsError::Metadata {
                                message:
                                    "repair completion queue changed before its owned checkpoint"
                                        .into(),
                            });
                        }
                        completed_retry_due = Some(desired.next_retry_unix_nanos);
                    }
                    None if !before.in_flight
                        && before.completed_transfer_request_id.as_deref()
                            == Some(response.request_id.as_str()) =>
                    {
                        completed_retry_due = Some(before.next_retry_unix_nanos);
                    }
                    _ => return Err(invalid()),
                }
            }
        }
        self.record_replica_transfer_completed(
            &response.intent_id,
            &response.replica_node_id,
            &response.request_id,
            false,
        )?;
        if let Some(due) = completed_retry_due {
            self.repair_attempt_records
                .get_mut(&(response.intent_id.clone(), response.replica_node_id.clone()))
                .expect("strict current row retained through terminal checkpoint")
                .next_retry_unix_nanos = due;
        }
        Ok(())
    }

    fn ensure_repair_response_matches_pending(
        &self,
        response: &DfsRepairReplicaResponse,
        content_hash: &str,
    ) -> Result<(), DfsError> {
        self.ensure_repair_response_request_matches_pending(response)?;
        let pending = self
            .writes
            .pending_writes
            .get(&response.intent_id)
            .expect("pending write validated");
        self.ensure_response_matches_pending_write(
            "repair",
            &response.intent_id,
            &response.file_system_id,
            &response.object_id,
            &response.version,
            &response.replica_node_id,
            content_hash,
            pending,
        )
    }

    fn ensure_repair_response_request_matches_pending(
        &self,
        response: &DfsRepairReplicaResponse,
    ) -> Result<(), DfsError> {
        let exact_pending = self
            .writes
            .pending_writes
            .get(&response.intent_id)
            .is_some_and(|pending| {
                pending
                    .repair_inflight_request_ids
                    .get(&response.replica_node_id)
                    == Some(&response.request_id)
            });
        if exact_pending
            && self.replica_queue_matches_current_transfer(
                &response.intent_id,
                &response.replica_node_id,
                &response.request_id,
                false,
            )
        {
            return Ok(());
        }
        Err(DfsError::Distributed {
            message: format!(
                "DFS repair response has no exact current pending and queue request: intent {}, replica {}, request {}",
                response.intent_id, response.replica_node_id, response.request_id
            ),
        })
    }

    pub(crate) fn handle_repair_replica_response(
        &mut self,
        frame_source_node_id: NodeId,
        response: DfsRepairReplicaResponse,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        if frame_source_node_id != response.replica_node_id {
            let reason = format!(
                "DFS repair response source mismatch: frame source {}, payload replica {}",
                frame_source_node_id, response.replica_node_id
            );
            return Ok(NetworkedDfsServiceEvent::RepairRejected {
                intent_id: response.intent_id,
                object_id: response.object_id,
                replica_node_id: frame_source_node_id,
                reason,
            });
        }
        // Persist the complete exactly correlated response BEFORE any metadata
        // restoration, authority lookup or application can fail. Legacy has no
        // original body and stays explicitly outside this stronger closure.
        let original_store = self.owned_repair_queue_store()?;
        let observed_original = if let Some(store) = &original_store {
            store.observe_original_repair_result(&self.node_id, &response)?
        } else {
            None
        };
        if matches!(response.result, DfsBlobTransferResult::Committed { .. })
            && let (Some(store), Some(observed)) = (&original_store, &observed_original)
            && let Some(birth) = store.original_repair_for_result(&self.node_id, &response)?
            && let Some(applied) = self.already_applied_original_repair(&birth, observed.clone())?
        {
            let closed = store.close_applied_original_repair(&applied)?;
            self.content.release_repair_payload(&closed)?;
            let DfsBlobTransferResult::Committed { content_hash } = response.result else {
                unreachable!()
            };
            return Ok(NetworkedDfsServiceEvent::ReplicaRepaired {
                intent_id: response.intent_id,
                object_id: response.object_id,
                version: response.version,
                replica_node_id: response.replica_node_id,
                content_hash,
            });
        }
        self.restore_pending_write_for_intent_if_missing(&response.intent_id, &response.object_id)?;
        if let Err(error) = self.restore_repair_response_custody(&response) {
            return Ok(NetworkedDfsServiceEvent::RepairRejected {
                intent_id: response.intent_id,
                object_id: response.object_id,
                replica_node_id: response.replica_node_id,
                reason: error.to_string(),
            });
        }
        match response.result {
            DfsBlobTransferResult::Committed { ref content_hash }
            | DfsBlobTransferResult::Durable { ref content_hash } => {
                if let Err(error) =
                    self.ensure_repair_response_matches_pending(&response, content_hash)
                {
                    return Ok(NetworkedDfsServiceEvent::RepairRejected {
                        intent_id: response.intent_id,
                        object_id: response.object_id,
                        replica_node_id: response.replica_node_id,
                        reason: error.to_string(),
                    });
                }
                let event =
                    self.with_placement_intent_authority(&response.intent_id, |service| {
                        service.put_metadata_receipt(
                            &response.file_system_id,
                            &response.object_id,
                            &response.version,
                            &response.replica_node_id,
                            content_hash,
                        )?;
                        service.record_owned_repair_terminal_response(&response)?;
                        if matches!(response.result, DfsBlobTransferResult::Committed { .. })
                            && let (Some(store), Some(observed)) =
                                (&original_store, &observed_original)
                        {
                            if let Some(birth) =
                                store.original_repair_for_result(&service.node_id, &response)?
                                && let Some(applied) = service
                                    .already_applied_original_repair(&birth, observed.clone())?
                            {
                                let closed = store.close_applied_original_repair(&applied)?;
                                // Queue unlock precedes content mutation. Missing
                                // exact application evidence retains custody.
                                service.content.release_repair_payload(&closed)?;
                            }
                        }

                        service.coordinator.record_durable_receipt(
                            &response.intent_id,
                            response.replica_node_id.clone(),
                        )?;
                        service.record_repair_metadata_settle(
                            &response.intent_id,
                            &response.replica_node_id,
                        );
                        service.cleanup_committed_pending_write_if_repaired(&response.intent_id);
                        Ok(NetworkedDfsServiceEvent::ReplicaRepaired {
                            intent_id: response.intent_id.clone(),
                            object_id: response.object_id.clone(),
                            version: response.version.clone(),
                            replica_node_id: response.replica_node_id.clone(),
                            content_hash: content_hash.clone(),
                        })
                    })?;
                Ok(
                    event.unwrap_or_else(|| NetworkedDfsServiceEvent::RepairRejected {
                        intent_id: response.intent_id,
                        object_id: response.object_id,
                        replica_node_id: response.replica_node_id,
                        reason: "write intent is closed or its placement policy was superseded"
                            .to_string(),
                    }),
                )
            }
            DfsBlobTransferResult::Rejected { ref reason } => {
                if !self.placement_intent_remains_authoritative(&response.intent_id)? {
                    return Ok(NetworkedDfsServiceEvent::RepairRejected {
                        intent_id: response.intent_id,
                        object_id: response.object_id,
                        replica_node_id: response.replica_node_id,
                        reason: "write intent is closed or its placement policy was superseded"
                            .to_string(),
                    });
                }
                let pending_hash = self
                    .writes
                    .pending_writes
                    .get(&response.intent_id)
                    .map(|pending| pending.content_hash.clone());
                let correlation = pending_hash
                    .as_deref()
                    .ok_or_else(|| DfsError::Distributed {
                        message: "missing exact pending repair for rejected response".into(),
                    })
                    .and_then(|hash| self.ensure_repair_response_matches_pending(&response, hash));
                match correlation {
                    Ok(()) => {
                        self.record_owned_repair_terminal_response(&response)?;
                        if repair_rejection_indicates_lost_authority(reason) {
                            self.suppress_repair_retries_for_lost_authority(
                                &response.intent_id,
                                Some(&response.replica_node_id),
                                false,
                            );
                        } else {
                            self.record_repair_retry_scheduled(
                                &response.intent_id,
                                &response.replica_node_id,
                            );
                        }
                        Ok(NetworkedDfsServiceEvent::RepairRejected {
                            intent_id: response.intent_id,
                            object_id: response.object_id,
                            replica_node_id: response.replica_node_id,
                            reason: reason.clone(),
                        })
                    }
                    Err(error) => Ok(NetworkedDfsServiceEvent::RepairRejected {
                        intent_id: response.intent_id,
                        object_id: response.object_id,
                        replica_node_id: response.replica_node_id,
                        reason: error.to_string(),
                    }),
                }
            }
        }
    }

    pub(crate) fn send_repair_replica_response(
        &mut self,
        requester_node_id: &NodeId,
        request_id: String,
        intent_id: String,
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        version: DfsVersion,
        result: DfsBlobTransferResult,
    ) -> Result<(), DfsError> {
        self.send_dfs_message_if_routable(
            requester_node_id,
            DfsNetworkMessage::RepairReplicaResponse(DfsRepairReplicaResponse {
                request_id,
                intent_id,
                file_system_id,
                object_id,
                version,
                replica_node_id: self.node_id.clone(),
                result,
            }),
        )?;
        Ok(())
    }

    pub(crate) fn repair_replica_result_event(
        &self,
        intent_id: String,
        object_id: DfsObjectId,
        version: DfsVersion,
        result: DfsBlobTransferResult,
    ) -> NetworkedDfsServiceEvent {
        match result {
            DfsBlobTransferResult::Committed { content_hash }
            | DfsBlobTransferResult::Durable { content_hash } => {
                NetworkedDfsServiceEvent::ReplicaRepaired {
                    intent_id,
                    object_id,
                    version,
                    replica_node_id: self.node_id.clone(),
                    content_hash,
                }
            }
            DfsBlobTransferResult::Rejected { reason } => {
                NetworkedDfsServiceEvent::RepairRejected {
                    intent_id,
                    object_id,
                    replica_node_id: self.node_id.clone(),
                    reason,
                }
            }
        }
    }
}

impl<T> NetworkedDfsService<T>
where
    T: AsyncDfsMessageTransport + DfsTransportIdentity + DfsMessageTransport,
{
    /// Wake scheduling before the maintenance owner captures its execution page.
    /// This never consumes retry budget or changes transfer custody/scan cursor.
    pub(crate) fn wake_connectivity_blocked_repair_records(
        &mut self,
        records: &[DfsRepairAttemptRecord],
        exclusions: &DfsMaintenanceExecutionExclusions,
    ) -> Result<Option<BTreeSet<(String, NodeId)>>, DfsError> {
        let candidates = records
            .iter()
            .filter(|row| {
                !exclusions.contains(&row.missing_replica_node_id)
                    && crate::repair::connectivity_repair_schedule_can_wake(row)
            })
            .cloned()
            .collect::<Vec<_>>();
        if candidates.is_empty() {
            return Ok(None);
        }
        let candidates = self.restore_pending_writes_for_repair_attempt_records(&candidates)?;
        let mut wake = Vec::new();
        for row in candidates {
            let Some(job) = self
                .coordinator
                .repair_job_for_target(&row.intent_id, &row.missing_replica_node_id)
            else {
                continue;
            };
            let Some(pending) = self.writes.pending_writes.get(&row.intent_id).cloned() else {
                continue;
            };
            if pending.object_id == row.object_id
                && pending.version == row.version
                && pending.content_hash == row.content_hash
                && job.object_id == row.object_id
                && job.version == row.version
                && self.pending_repair_is_locally_authoritative(&pending)?
            {
                wake.push(row);
            }
        }
        if wake.is_empty() {
            return Ok(None);
        }
        let Some(store) = self.owned_repair_queue_store()? else {
            return Ok(None);
        };
        let observed = wake
            .iter()
            .map(|row| {
                (
                    (row.intent_id.clone(), row.missing_replica_node_id.clone()),
                    row,
                )
            })
            .collect::<BTreeMap<_, _>>();
        let current = store.wake_observed_scheduling_records_if(
            &wake,
            dfs_now_unix_nanos(),
            &crate::repair::connectivity_repair_schedule_can_wake,
        )?;
        let mut present = BTreeSet::new();
        for row in current {
            let key = (row.intent_id.clone(), row.missing_replica_node_id.clone());
            if let Some(before) = observed.get(&key)
                && (row.object_id != before.object_id
                    || row.version != before.version
                    || row.content_hash != before.content_hash)
            {
                return Err(DfsError::Metadata {
                    message: "connectivity wake queue identity changed after owner validation"
                        .into(),
                });
            }
            present.insert(key);
        }
        Ok(Some(
            observed
                .keys()
                .filter(|key| !present.contains(*key))
                .cloned()
                .collect(),
        ))
    }

    pub(crate) async fn retry_pending_repair_page(
        &mut self,
        max_scheduled: usize,
        repair_attempts: &[DfsRepairAttemptRecord],
        execution_exclusions: &DfsMaintenanceExecutionExclusions,
        scoped_targets: Option<&BTreeSet<(String, NodeId)>>,
        skipped_targets: Option<&BTreeSet<(String, NodeId)>>,
        advance_retry_tick: bool,
    ) -> Result<DfsRepairPageProgress, DfsError> {
        self.retry_pending_repair_page_with_resident_yield(
            max_scheduled,
            repair_attempts,
            execution_exclusions,
            scoped_targets,
            skipped_targets,
            advance_retry_tick,
            false,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn retry_pending_repair_page_with_resident_yield(
        &mut self,
        max_scheduled: usize,
        repair_attempts: &[DfsRepairAttemptRecord],
        execution_exclusions: &DfsMaintenanceExecutionExclusions,
        scoped_targets: Option<&BTreeSet<(String, NodeId)>>,
        skipped_targets: Option<&BTreeSet<(String, NodeId)>>,
        advance_retry_tick: bool,
        yield_on_source_fence: bool,
    ) -> Result<DfsRepairPageProgress, DfsError> {
        if max_scheduled == 0 {
            return Ok(DfsRepairPageProgress::default());
        }
        #[cfg(any(test, feature = "test-support"))]
        let native_held_targets = self.native_completion_held_repair_targets(repair_attempts)?;
        #[cfg(not(any(test, feature = "test-support")))]
        let native_held_targets = BTreeSet::new();
        self.prune_idle_transfer_state();
        let Some(restored) = self
            .restore_pending_writes_with_source_read_mode(repair_attempts, yield_on_source_fence)?
        else {
            return Ok(DfsRepairPageProgress {
                source_read_pending: true,
                ..Default::default()
            });
        };
        let repair_attempts = restored.as_slice();
        self.restore_repair_attempt_records_merging(repair_attempts);
        let recovered_original_count = self.recover_observed_original_repair_page(
            max_scheduled,
            None,
            Some(execution_exclusions),
        )?;
        let mut settled_keys = repair_attempts
            .iter()
            .filter(|record| {
                self.coordinator
                    .repair_job_for_target(&record.intent_id, &record.missing_replica_node_id)
                    .is_none()
            })
            .map(|record| {
                (
                    record.intent_id.clone(),
                    record.missing_replica_node_id.clone(),
                )
            })
            .collect::<BTreeSet<_>>();
        settled_keys.extend(native_held_targets.iter().cloned());
        if (self.writes.pending_writes.is_empty() && self.coordinator.repair_jobs().is_empty())
            || max_scheduled == 0
        {
            return Ok(DfsRepairPageProgress {
                examined_record_count: repair_attempts.len(),
                examined: settled_keys,
                parked: native_held_targets,
                ..Default::default()
            });
        }
        let mut budget =
            DfsRepairMaintenanceBudget::new(max_scheduled.saturating_sub(recovered_original_count));
        if advance_retry_tick {
            self.repair_retry_tick = self.repair_retry_tick.saturating_add(1);
            self.expire_stale_fetch_requests();
            self.retry_pending_stages_async_bounded(&mut budget, execution_exclusions)
                .await?;
        }
        let repair_jobs = if let Some(targets) = scoped_targets {
            targets
                .iter()
                .take(DFS_REPAIR_EXAMINED_RECORDS_PER_PAGE)
                .filter_map(|(id, target)| self.coordinator.repair_job_for_target(id, target))
                .collect::<Vec<_>>()
        } else {
            self.coordinator
                .repair_jobs()
                .iter()
                .take(DFS_REPAIR_EXAMINED_RECORDS_PER_PAGE)
                .cloned()
                .collect()
        };
        let examined_record_count =
            repair_attempts
                .len()
                .max(scoped_targets.map_or(repair_jobs.len(), |targets| {
                    targets.len().min(DFS_REPAIR_EXAMINED_RECORDS_PER_PAGE)
                }));
        let mut retired_intents = Vec::new();
        let mut examined = settled_keys;
        let mut parked = native_held_targets;
        let mut scheduled = Vec::new();
        let mut error = None;
        let mut consumed_repair_targets = skipped_targets.cloned().unwrap_or_default();
        // These keys are examined/preserved, not scheduled. They consume no
        // send capacity and cannot exclude unrelated originals to the same C.
        consumed_repair_targets.extend(parked.iter().cloned());
        let mut placement_plans = None;
        let observed_placement_config = self
            .metadata
            .filesystems
            .get(&crate::sifs_file_system_id())
            .map(crate::DfsFileSystemConfig::from);

        for (repair_index, job) in repair_jobs.iter().cloned().enumerate() {
            if budget.is_exhausted() {
                break;
            }
            examined.insert((job.intent_id.clone(), job.missing_replica_node_id.clone()));
            if execution_exclusions.contains(&job.missing_replica_node_id) {
                continue;
            }
            if consumed_repair_targets
                .contains(&(job.intent_id.clone(), job.missing_replica_node_id.clone()))
            {
                continue;
            }
            consumed_repair_targets
                .insert((job.intent_id.clone(), job.missing_replica_node_id.clone()));
            if self.restore_pending_write_for_repair_job(&job)?
                == crate::service_commit::RepairProducerCustodyReadiness::Parked
            {
                parked.insert((job.intent_id.clone(), job.missing_replica_node_id.clone()));
                continue;
            }
            if !self.prepare_repair_target(&job.intent_id, &job.missing_replica_node_id) {
                continue;
            }
            let Some(pending) = self.writes.pending_writes.get(&job.intent_id).cloned() else {
                continue;
            };
            if !self.pending_repair_is_locally_authoritative(&pending)? {
                self.suppress_repair_retries_for_lost_authority(&job.intent_id, None, true);
                continue;
            }
            if !self.pending_repair_targets_current_content(&job, &pending)? {
                self.retire_committed_repair(&job.intent_id);
                retired_intents.push(retired_repair_identity(&job, &pending));
                break;
            }
            if !self.repair_retry_is_due(&job.intent_id, &job.missing_replica_node_id) {
                continue;
            }
            // Capture plan inputs once for this bounded pass. Supersession is
            // a durable operation under the placement permit, not suppression
            // based on a remembered queue or an equal metadata snapshot.
            let needs_placement_recovery =
                self.repair_may_need_placement_recovery(&job, observed_placement_config.as_ref())?;
            if needs_placement_recovery && placement_plans.is_none() {
                placement_plans = Some(self.placement_plans_for_repair()?);
            }
            let mut replacement_retirements = Vec::new();
            let replacement = if needs_placement_recovery {
                self.replace_superseded_repair_obligations_recording(
                    &repair_jobs[repair_index..],
                    placement_plans.as_ref().unwrap(),
                    budget.remaining_targets,
                    observed_placement_config.as_ref(),
                    |candidate| !execution_exclusions.contains(&candidate.missing_replica_node_id),
                    &mut replacement_retirements,
                )
            } else {
                Ok(0)
            };
            for record in &replacement_retirements {
                examined.extend(
                    repair_jobs
                        .iter()
                        .filter(|job| job.intent_id == record.intent_id)
                        .map(|job| (job.intent_id.clone(), job.missing_replica_node_id.clone())),
                );
            }
            retired_intents.extend(replacement_retirements);
            match replacement {
                Ok(replaced) if replaced > 0 => {
                    budget.reserve(replaced);
                    break;
                }
                Ok(_) => {}
                Err(failed) => {
                    error = Some(failed);
                    break;
                }
            }
            let preparation = self.try_prepare_managed_background_repair_batch(
                &repair_jobs,
                repair_index,
                budget.remaining_targets,
                execution_exclusions,
                &consumed_repair_targets,
                placement_plans.as_deref().unwrap_or(&[]),
            )?;
            if matches!(&preparation, Some(ManagedRepairPreparation::RecheckCurrent)) {
                if !self.pending_repair_is_locally_authoritative(&pending)? {
                    self.suppress_repair_retries_for_lost_authority(&job.intent_id, None, true);
                    break;
                }
                if !self.pending_repair_targets_current_content(&job, &pending)? {
                    self.retire_committed_repair(&job.intent_id);
                    retired_intents.push(retired_repair_identity(&job, &pending));
                    break;
                }
            }
            if let Some(ManagedRepairPreparation::Prepared(batch)) = preparation {
                if batch.error.is_some()
                    && !batch
                        .processed
                        .contains(&(job.intent_id.clone(), job.missing_replica_node_id.clone()))
                {
                    examined.remove(&(job.intent_id.clone(), job.missing_replica_node_id.clone()));
                }
                examined.extend(batch.processed.iter().cloned());
                consumed_repair_targets.extend(batch.processed.iter().cloned());
                let reserved = budget.reserve(batch.target_count());
                debug_assert_eq!(reserved, batch.target_count());
                error = batch.error;
                let mut dispatches = batch.dispatches.into_iter();
                while let Some((prepared, fanout_jobs)) = dispatches.next() {
                    for (candidate, _) in &fanout_jobs {
                        consumed_repair_targets.insert((
                            candidate.intent_id.clone(),
                            candidate.missing_replica_node_id.clone(),
                        ));
                    }
                    // Actual job custody was released by the synchronous
                    // preparation boundary before any awaited dispatch.
                    let outcomes = match self.queue_current_background_repair_sends(prepared) {
                        Ok(outcomes) => outcomes,
                        Err(failed) => {
                            for (target, _) in fanout_jobs
                                .into_iter()
                                .chain(dispatches.flat_map(|(_, jobs)| jobs))
                            {
                                examined
                                    .remove(&(target.intent_id, target.missing_replica_node_id));
                            }
                            error = Some(failed);
                            break;
                        }
                    };
                    self.record_repair_fanout_outcomes(fanout_jobs, outcomes, &mut scheduled);
                }
                break;
            }
            // A parked or non-local target performs no effect and needs no
            // distributed placement permit. Validate only eligible retries;
            // receipts independently retain their effect-time authority check.
            match self.placement_intent_remains_authoritative(&job.intent_id) {
                Ok(true) => {}
                Ok(false) => break,
                Err(failed) => {
                    examined.remove(&(job.intent_id.clone(), job.missing_replica_node_id.clone()));
                    error = Some(failed);
                    break;
                }
            }

            if job.missing_replica_node_id == self.node_id {
                if !budget.reserve_one() {
                    break;
                }
                if self
                    .local_object_version_manifest_size_if_matches(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                    )
                    .is_some()
                {
                    self.record_local_pending_write_receipt_if_desired(
                        &job.intent_id,
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                    )?;
                    break;
                }
                let fetch_outcome = self
                    .request_repair_blob_from_alternate_async(&job, &pending, execution_exclusions)
                    .await?;
                if !fetch_outcome.requested {
                    self.record_repair_retry_scheduled_with_error(
                        &job.intent_id,
                        &job.missing_replica_node_id,
                        fetch_outcome.last_error,
                    );
                }
                break;
            }

            if self.open_pending_write_blob_stream(&pending).is_err() {
                if !budget.reserve_one() {
                    break;
                }
                let fetch_outcome = self
                    .request_repair_blob_from_alternate_async(&job, &pending, execution_exclusions)
                    .await?;
                if !fetch_outcome.requested {
                    self.record_repair_retry_scheduled_with_error(
                        &job.intent_id,
                        &job.missing_replica_node_id,
                        fetch_outcome.last_error,
                    );
                }
                break;
            }
            let remaining = budget.remaining_targets;
            if remaining == 0 {
                break;
            }
            let fanout_jobs = self.prepare_repair_fanout_jobs(
                &job,
                &pending,
                &repair_jobs,
                remaining,
                execution_exclusions,
                &mut consumed_repair_targets,
            )?;
            let reserved = budget.reserve(fanout_jobs.len());
            debug_assert_eq!(reserved, fanout_jobs.len());
            let object_metadata = Some(self.current_object_for_write(&pending.object_id)?);
            let fanout_targets = fanout_jobs
                .iter()
                .map(|(job, request_id)| (job.missing_replica_node_id.clone(), request_id.clone()))
                .collect::<Vec<_>>();
            let outcomes = self
                .send_repair_replica_attachments_if_routable_async(
                    &fanout_targets,
                    &job.intent_id,
                    &job.file_system_id,
                    &pending.object_id,
                    self.coordinator
                        .intent(&job.intent_id)
                        .and_then(|intent| intent.base_version.clone()),
                    &pending.version,
                    &pending.content_hash,
                    pending.size_bytes,
                    object_metadata,
                )
                .await?;
            // Every actual companion claim must be included in the SAME page
            // checkpoint before its owned payload can enter the detached worker.
            examined.extend(fanout_jobs.iter().map(|(target, _)| {
                (
                    target.intent_id.clone(),
                    target.missing_replica_node_id.clone(),
                )
            }));
            self.record_repair_fanout_outcomes(fanout_jobs, outcomes, &mut scheduled);
            break;
        }

        Ok(DfsRepairPageProgress {
            source_read_pending: false,
            scheduled,
            error,
            examined_record_count,
            examined,
            parked,
            retired_intents,
        })
    }

    async fn retry_pending_stages_async_bounded(
        &mut self,
        budget: &mut DfsRepairMaintenanceBudget,
        execution_exclusions: &DfsMaintenanceExecutionExclusions,
    ) -> Result<(), DfsError> {
        let pending_intent_ids = self
            .writes
            .pending_writes
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for intent_id in pending_intent_ids {
            if budget.is_exhausted() {
                break;
            }
            let Some(intent) = self.coordinator.intent(&intent_id).cloned() else {
                continue;
            };
            if !matches!(intent.state, DfsWriteIntentState::AwaitingReceipts) {
                continue;
            }
            while let Some(pending) = self.writes.pending_writes.get(&intent_id).cloned() {
                let durable_nodes = self
                    .coordinator
                    .receipts_for_intent(&intent_id)
                    .into_iter()
                    .map(|receipt| receipt.replica_node_id.clone())
                    .collect::<BTreeSet<_>>();
                let mut stage_targets = self.pending_stage_targets(
                    &intent,
                    &pending,
                    &durable_nodes,
                    Some(execution_exclusions),
                );
                if stage_targets.is_empty() {
                    // Local staging is not a StageBlob RPC target. After a
                    // restart it may nevertheless be the only receipt missing
                    // from W. Recover its bytes through the ordinary correlated
                    // fetch, without inventing a receipt or transfer outcome.
                    if intent.desired_replica_nodes.contains(&self.node_id)
                        && !durable_nodes.contains(&self.node_id)
                        && !execution_exclusions.contains(&self.node_id)
                        && self.pending_stage_send_budget(
                            &intent,
                            &pending,
                            &durable_nodes,
                            Some(execution_exclusions),
                        ) > 0
                        && budget.reserve(1) == 1
                    {
                        self.request_stage_blob_from_alternate_async(
                            &intent,
                            &pending,
                            execution_exclusions,
                        )
                        .await?;
                    }
                    break;
                }
                let reserved = budget.reserve(stage_targets.len());
                stage_targets.truncate(reserved);
                if stage_targets.is_empty() {
                    return Ok(());
                }
                let object_metadata = Some(self.current_object_for_write(&pending.object_id)?);
                let mut full_stage_targets = Vec::new();
                for replica_node_id in stage_targets {
                    let request_id = pending
                        .stage_inflight_request_ids
                        .get(&replica_node_id)
                        .cloned()
                        .unwrap_or_else(|| self.next_request_id());
                    match self
                        .send_pending_stage_patch_if_routable_async(
                            &replica_node_id,
                            request_id.clone(),
                            &intent,
                            &pending,
                            object_metadata.clone(),
                        )
                        .await?
                    {
                        Some(outcome) if outcome.sent => {
                            self.record_stage_inflight(&intent_id, &replica_node_id, request_id);
                        }
                        Some(_) => {
                            self.record_stage_retry_scheduled(&intent_id, &replica_node_id);
                        }
                        None => full_stage_targets.push((replica_node_id, request_id)),
                    }
                }
                if full_stage_targets.is_empty() {
                    continue;
                }
                if self.open_pending_write_blob_stream(&pending).is_err() {
                    let fetch_requested = self
                        .request_stage_blob_from_alternate_async(
                            &intent,
                            &pending,
                            execution_exclusions,
                        )
                        .await?;
                    if !fetch_requested {
                        for (replica_node_id, _) in full_stage_targets {
                            self.record_stage_retry_scheduled(&intent_id, &replica_node_id);
                        }
                    }
                    break;
                }
                let outcomes = self
                    .send_stage_blob_attachments_if_routable_async(
                        &full_stage_targets,
                        &intent.intent_id,
                        &intent.file_system_id,
                        &intent.object_id,
                        intent.base_version.clone(),
                        &intent.target_version,
                        &pending.content_hash,
                        pending.size_bytes,
                        object_metadata,
                    )
                    .await?;
                for ((replica_node_id, request_id), outcome) in
                    full_stage_targets.into_iter().zip(outcomes)
                {
                    if outcome.sent {
                        self.record_stage_inflight(&intent_id, &replica_node_id, request_id);
                    } else {
                        self.record_stage_retry_scheduled(&intent_id, &replica_node_id);
                    }
                }
                if budget.is_exhausted() {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    async fn send_pending_stage_patch_if_routable_async(
        &mut self,
        replica_node_id: &NodeId,
        request_id: String,
        intent: &DistributedWriteIntent,
        pending: &PendingNetworkedWrite,
        object_metadata: Option<DfsObjectMetadata>,
    ) -> Result<Option<StageBlobSendOutcome>, DfsError> {
        let Some(patch) = pending.patch.as_ref() else {
            return Ok(None);
        };
        if patch.full_stage_fallback_nodes.contains(replica_node_id) {
            return Ok(None);
        }
        let Some(base_version) = intent.base_version.as_ref() else {
            return Ok(None);
        };
        if base_version != &patch.base_version {
            return Ok(None);
        }
        let outcome = self
            .send_stage_patch_attachment_if_routable_async(
                replica_node_id,
                request_id,
                &intent.intent_id,
                &intent.file_system_id,
                &intent.object_id,
                base_version,
                &intent.target_version,
                &pending.content_hash,
                patch.offset,
                &patch.patch_hash,
                patch.patch_size_bytes,
                object_metadata,
            )
            .await?;
        Ok(Some(outcome))
    }

    async fn request_stage_blob_from_alternate_async(
        &mut self,
        intent: &DistributedWriteIntent,
        pending: &PendingNetworkedWrite,
        execution_exclusions: &DfsMaintenanceExecutionExclusions,
    ) -> Result<bool, DfsError> {
        let mut skipped_failed_source = false;
        let authority_object = self.current_read_object_metadata(&pending.object_id).ok();
        let authority_shard = self.command_authority_shard_for_object(authority_object.as_ref())?;
        for source_node_id in self.stage_blob_sources(&intent.intent_id) {
            if execution_exclusions.contains(&source_node_id) {
                continue;
            }
            if pending.fetch_inflight_from.contains(&source_node_id) {
                continue;
            }
            if pending.fetch_failed_from.contains(&source_node_id) {
                skipped_failed_source = true;
                continue;
            }
            let request_id = pending
                .fetch_inflight_request_ids
                .get(&source_node_id)
                .cloned()
                .unwrap_or_else(|| self.next_request_id());
            let sent = self
                .send_best_effort_message_if_routable_async(
                    &source_node_id,
                    DfsNetworkMessage::FetchBlob(DfsFetchBlobCommand {
                        request_id: request_id.clone(),
                        intent_id: intent.intent_id.clone(),
                        object_id: pending.object_id.clone(),
                        version: pending.version.clone(),
                        content_hash: pending.content_hash.clone(),
                        authority_shard: authority_shard.clone(),
                    }),
                )
                .await?;
            if sent {
                self.record_fetch_inflight(&intent.intent_id, source_node_id, request_id);
                return Ok(true);
            }
        }
        if skipped_failed_source
            && let Some(pending) = self.writes.pending_writes.get_mut(&intent.intent_id)
        {
            pending.fetch_failed_from.clear();
        }
        Ok(false)
    }

    async fn request_repair_blob_from_alternate_async(
        &mut self,
        job: &DistributedRepairJob,
        pending: &PendingNetworkedWrite,
        execution_exclusions: &DfsMaintenanceExecutionExclusions,
    ) -> Result<DfsRepairBlobRequestOutcome, DfsError> {
        let mut skipped_failed_source = false;
        let mut last_error = None;
        let authority_object = self.current_read_object_metadata(&pending.object_id).ok();
        let authority_shard = self.command_authority_shard_for_object(authority_object.as_ref())?;
        for source_node_id in self.repair_blob_sources(job, pending)? {
            if execution_exclusions.contains(&source_node_id) {
                continue;
            }
            if pending.fetch_inflight_from.contains(&source_node_id) {
                continue;
            }
            if pending.fetch_failed_from.contains(&source_node_id) {
                skipped_failed_source = true;
                continue;
            }
            let request_id = pending
                .fetch_inflight_request_ids
                .get(&source_node_id)
                .cloned()
                .unwrap_or_else(|| self.next_request_id());
            let send_outcome = self
                .send_best_effort_message_if_routable_outcome_async(
                    &source_node_id,
                    DfsNetworkMessage::FetchBlob(DfsFetchBlobCommand {
                        request_id: request_id.clone(),
                        intent_id: job.intent_id.clone(),
                        object_id: pending.object_id.clone(),
                        version: pending.version.clone(),
                        content_hash: pending.content_hash.clone(),
                        authority_shard: authority_shard.clone(),
                    }),
                )
                .await?;
            if send_outcome.sent {
                tracing::debug!(
                    target: "sinomo.dfs.repair",
                    local_node_id = %self.node_id,
                    source_node_id = %source_node_id,
                    missing_replica_node_id = %job.missing_replica_node_id,
                    intent_id = %job.intent_id,
                    object_id = %pending.object_id,
                    version = %pending.version,
                    content_hash = %pending.content_hash,
                    request_id = %request_id,
                    "requested DFS repair blob from alternate source"
                );
                self.record_fetch_inflight(&job.intent_id, source_node_id, request_id);
                self.record_repair_fetch_inflight(&job.intent_id, &job.missing_replica_node_id);
                return Ok(DfsRepairBlobRequestOutcome {
                    requested: true,
                    last_error: None,
                });
            }
            if send_outcome.last_error.is_some() {
                last_error = send_outcome.last_error;
            }
        }
        if skipped_failed_source
            && let Some(pending) = self.writes.pending_writes.get_mut(&job.intent_id)
        {
            pending.fetch_failed_from.clear();
        }
        tracing::debug!(
            target: "sinomo.dfs.repair",
            local_node_id = %self.node_id,
            missing_replica_node_id = %job.missing_replica_node_id,
            intent_id = %job.intent_id,
            object_id = %pending.object_id,
            version = %pending.version,
            content_hash = %pending.content_hash,
            skipped_failed_source,
            "no routable DFS repair blob alternate source"
        );
        Ok(DfsRepairBlobRequestOutcome {
            requested: false,
            last_error,
        })
    }
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub(crate) fn prune_idle_transfer_state(&mut self) {
        self.prune_retained_routed_writes();
        // Requesters give up on routed writes after their settle timeout;
        // response obligations they will never consume must not accumulate.
        if !self.writes.pending_remote_write_responses.is_empty() {
            self.writes
                .pending_remote_write_responses
                .retain(|_, pending| pending.created_at.elapsed() <= DFS_REMOTE_WRITE_RESPONSE_TTL);
        }
    }

    pub fn retry_pending_repairs(&mut self) -> Result<Vec<DistributedRepairJob>, DfsError> {
        self.retry_pending_repairs_bounded(usize::MAX)
    }

    pub fn retry_pending_repairs_bounded(
        &mut self,
        max_scheduled: usize,
    ) -> Result<Vec<DistributedRepairJob>, DfsError> {
        self.retry_pending_repairs_bounded_with_attempts(max_scheduled, &[])
    }

    /// Retry a bounded durable repair queue after restoring only its exact
    /// intent identities from the metadata backend.
    ///
    /// This restart path is O(active repair work), rather than O(total DFS
    /// metadata), and therefore does not require a full service hydrate.
    pub fn retry_pending_repairs_bounded_with_attempts(
        &mut self,
        max_scheduled: usize,
        repair_attempts: &[DfsRepairAttemptRecord],
    ) -> Result<Vec<DistributedRepairJob>, DfsError> {
        self.retry_pending_repairs_scoped_bounded_with_attempts(
            max_scheduled,
            repair_attempts,
            None,
        )
    }

    pub(crate) fn retry_pending_repairs_for_intents_bounded(
        &mut self,
        intent_ids: &BTreeSet<String>,
        max_scheduled: usize,
    ) -> Result<Vec<DistributedRepairJob>, DfsError> {
        self.retry_pending_repairs_scoped_bounded_with_attempts(
            max_scheduled,
            &[],
            Some(intent_ids),
        )
    }

    fn retry_pending_repairs_scoped_bounded_with_attempts(
        &mut self,
        max_scheduled: usize,
        repair_attempts: &[DfsRepairAttemptRecord],
        intent_filter: Option<&BTreeSet<String>>,
    ) -> Result<Vec<DistributedRepairJob>, DfsError> {
        self.prune_idle_transfer_state();
        let restored = self.restore_pending_writes_for_repair_attempt_records(repair_attempts)?;
        let repair_attempts = restored.as_slice();
        self.restore_repair_attempt_records(repair_attempts);
        let recovered_original_count =
            self.recover_observed_original_repair_page(max_scheduled, intent_filter, None)?;
        if (self.writes.pending_writes.is_empty() && self.coordinator.repair_jobs().is_empty())
            || max_scheduled == 0
            || intent_filter.is_some_and(BTreeSet::is_empty)
        {
            return Ok(Vec::new());
        }
        self.repair_retry_tick = self.repair_retry_tick.saturating_add(1);
        self.expire_stale_fetch_requests();
        let mut budget =
            DfsRepairMaintenanceBudget::new(max_scheduled.saturating_sub(recovered_original_count));
        self.retry_pending_stages_scoped_bounded(intent_filter, &mut budget)?;
        let repair_jobs = self.coordinator.repair_jobs().to_vec();
        let mut scheduled = Vec::new();
        let mut consumed_repair_targets = BTreeSet::new();
        let mut placement_plans = None;
        let observed_placement_config = self
            .metadata
            .filesystems
            .get(&crate::sifs_file_system_id())
            .map(crate::DfsFileSystemConfig::from);

        for (repair_index, job) in repair_jobs.iter().cloned().enumerate() {
            if budget.is_exhausted() {
                break;
            }
            if intent_filter.is_some_and(|intent_ids| !intent_ids.contains(&job.intent_id)) {
                continue;
            }
            if consumed_repair_targets
                .contains(&(job.intent_id.clone(), job.missing_replica_node_id.clone()))
            {
                continue;
            }
            consumed_repair_targets
                .insert((job.intent_id.clone(), job.missing_replica_node_id.clone()));
            if self.restore_pending_write_for_repair_job(&job)?
                == crate::service_commit::RepairProducerCustodyReadiness::Parked
            {
                continue;
            }
            if !self.prepare_repair_target(&job.intent_id, &job.missing_replica_node_id) {
                continue;
            }
            let Some(pending) = self.writes.pending_writes.get(&job.intent_id).cloned() else {
                continue;
            };
            if !self.pending_repair_is_locally_authoritative(&pending)? {
                self.suppress_repair_retries_for_lost_authority(&job.intent_id, None, true);
                continue;
            }
            if !self.pending_repair_targets_current_content(&job, &pending)? {
                self.retire_committed_repair(&job.intent_id);
                continue;
            }
            if !self.repair_retry_is_due(&job.intent_id, &job.missing_replica_node_id) {
                continue;
            }
            let needs_placement_recovery =
                self.repair_may_need_placement_recovery(&job, observed_placement_config.as_ref())?;
            if needs_placement_recovery && placement_plans.is_none() {
                placement_plans = Some(self.placement_plans_for_repair()?);
            }
            let replaced = if needs_placement_recovery {
                self.replace_superseded_repair_obligations(
                    &repair_jobs[repair_index..],
                    placement_plans.as_ref().unwrap(),
                    budget.remaining_targets,
                    observed_placement_config.as_ref(),
                    |candidate| intent_filter.is_none_or(|ids| ids.contains(&candidate.intent_id)),
                )?
            } else {
                0
            };
            if replaced > 0 {
                budget.reserve(replaced);
                continue;
            }
            // A parked or non-local target performs no effect and needs no
            // distributed placement permit. Validate only eligible retries;
            // receipts independently retain their effect-time authority check.
            if !self.placement_intent_remains_authoritative(&job.intent_id)? {
                continue;
            }

            if job.missing_replica_node_id == self.node_id {
                if !budget.reserve_one() {
                    break;
                }
                if self
                    .local_object_version_manifest_size_if_matches(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                    )
                    .is_some()
                {
                    self.record_local_pending_write_receipt_if_desired(
                        &job.intent_id,
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                    )?;
                    continue;
                }
                let fetch_outcome = self.request_repair_blob_from_alternate(&job, &pending)?;
                if !fetch_outcome.requested {
                    self.record_repair_retry_scheduled_with_error(
                        &job.intent_id,
                        &job.missing_replica_node_id,
                        fetch_outcome.last_error,
                    );
                }
                continue;
            }

            if self.open_pending_write_blob_stream(&pending).is_err() {
                if !budget.reserve_one() {
                    break;
                }
                let fetch_outcome = self.request_repair_blob_from_alternate(&job, &pending)?;
                if !fetch_outcome.requested {
                    self.record_repair_retry_scheduled_with_error(
                        &job.intent_id,
                        &job.missing_replica_node_id,
                        fetch_outcome.last_error,
                    );
                }
                continue;
            }
            let request_id = pending
                .repair_inflight_request_ids
                .get(&job.missing_replica_node_id)
                .cloned()
                .unwrap_or_else(|| self.next_request_id());
            let remaining = budget.remaining_targets;
            if remaining == 0 {
                break;
            }
            let mut fanout_jobs = vec![(job.clone(), request_id)];
            if remaining > 1
                && let Some(companion) = repair_jobs
                    .iter()
                    .find(|candidate| {
                        candidate.intent_id == job.intent_id
                            && candidate.missing_replica_node_id != job.missing_replica_node_id
                            && candidate.missing_replica_node_id != self.node_id
                            && !consumed_repair_targets.contains(&(
                                candidate.intent_id.clone(),
                                candidate.missing_replica_node_id.clone(),
                            ))
                            && self.repair_retry_is_due(
                                &candidate.intent_id,
                                &candidate.missing_replica_node_id,
                            )
                            && !self.stage_request_is_active(
                                &candidate.intent_id,
                                &candidate.missing_replica_node_id,
                            )
                    })
                    .cloned()
                && self
                    .prepare_repair_target(&companion.intent_id, &companion.missing_replica_node_id)
            {
                let companion_pending = self
                    .writes
                    .pending_writes
                    .get(&companion.intent_id)
                    .cloned()
                    .expect("same-intent pending repair validated");
                if self.pending_repair_targets_current_content(&companion, &companion_pending)? {
                    let companion_request_id = companion_pending
                        .repair_inflight_request_ids
                        .get(&companion.missing_replica_node_id)
                        .cloned()
                        .unwrap_or_else(|| self.next_request_id());
                    consumed_repair_targets.insert((
                        companion.intent_id.clone(),
                        companion.missing_replica_node_id.clone(),
                    ));
                    fanout_jobs.push((companion, companion_request_id));
                }
            }
            let reserved = budget.reserve(fanout_jobs.len());
            debug_assert_eq!(reserved, fanout_jobs.len());
            let object_metadata = Some(self.current_object_for_write(&pending.object_id)?);
            let fanout_targets = fanout_jobs
                .iter()
                .map(|(job, request_id)| (job.missing_replica_node_id.clone(), request_id.clone()))
                .collect::<Vec<_>>();
            let outcomes = self.send_repair_replica_attachments_if_routable(
                &fanout_targets,
                &job.intent_id,
                &job.file_system_id,
                &pending.object_id,
                self.coordinator
                    .intent(&job.intent_id)
                    .and_then(|intent| intent.base_version.clone()),
                &pending.version,
                &pending.content_hash,
                pending.size_bytes,
                object_metadata,
            )?;
            for ((fanout_job, request_id), outcome) in fanout_jobs.into_iter().zip(outcomes) {
                if outcome.sent {
                    self.record_repair_inflight(
                        &fanout_job.intent_id,
                        &fanout_job.missing_replica_node_id,
                        request_id,
                    );
                    scheduled.push(fanout_job);
                } else {
                    self.record_repair_retry_scheduled_with_error(
                        &fanout_job.intent_id,
                        &fanout_job.missing_replica_node_id,
                        outcome.last_error,
                    );
                }
            }
            if budget.is_exhausted() {
                break;
            }
        }

        Ok(scheduled)
    }

    pub(crate) fn remove_pending_write_state(&mut self, intent_id: &str) {
        self.writes.pending_writes.remove(intent_id);
    }

    pub(crate) fn retire_committed_repair(&mut self, intent_id: &str) {
        self.retire_committed_repairs(&BTreeSet::from([intent_id.to_string()]));
    }

    pub(crate) fn retire_committed_repairs(&mut self, intent_ids: &BTreeSet<String>) {
        let retired = intent_ids
            .iter()
            .filter(|id| {
                self.coordinator
                    .intent(id)
                    .is_some_and(|intent| intent.state == DfsWriteIntentState::Committed)
            })
            .cloned()
            .collect::<BTreeSet<_>>();
        self.coordinator.retire_committed_repairs(&retired);
        for id in &retired {
            self.writes.pending_writes.remove(id);
        }
        self.repair_attempt_records.retain(|(id, _), record| {
            if !retired.contains(id) {
                return true;
            }
            // Withdrawing an executor cannot settle a carrier's exact claim.
            if repair_transfer_custody_unresolved(record) {
                record.next_retry_unix_nanos = u64::MAX;
                return true;
            }
            false
        });
    }

    /// Losing a placement plan's execution eligibility does not settle the
    /// committed content obligation. Keep its actual producer DATA parked in
    /// the existing queue until authoritative replacement or completion.
    pub(crate) fn park_committed_repairs(&mut self, intent_ids: &BTreeSet<String>) {
        let preserved = self
            .repair_attempt_records
            .iter()
            .filter(|((id, _), _)| intent_ids.contains(id))
            .map(|(key, record)| (key.clone(), record.clone()))
            .collect::<Vec<_>>();
        self.retire_committed_repairs(intent_ids);
        for (key, mut record) in preserved {
            record.next_retry_unix_nanos = u64::MAX;
            self.repair_attempt_records.insert(key, record);
        }
    }

    /// Resolve only the outgoing identities affected by this publication.
    /// Disabled policies do not add object/history lookups to ordinary writes.
    pub(crate) fn file_history_repair_roots_before_objects<'a>(
        &self,
        objects: impl IntoIterator<Item = &'a DfsObjectMetadata>,
        file_system: Option<&DfsFileSystem>,
    ) -> Result<BTreeSet<(DfsObjectId, DfsVersion)>, DfsError> {
        let mut roots = BTreeSet::new();
        let Some(file_system) = file_system.filter(|fs| !fs.retention_policies.is_empty()) else {
            return Ok(roots);
        };
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(roots);
        };
        for object in objects {
            if object.file_system_id != file_system.file_system_id {
                continue;
            }
            let own = backend.get_object(&object.object_id)?;
            let replaced = backend.get_object_by_path(&object.file_system_id, &object.path)?;
            for previous in own.iter().chain(replaced.iter()) {
                if !crate::file_history::capture_retention_for_file_system(
                    Some(file_system),
                    previous,
                    object,
                )
                .is_empty()
                {
                    roots.insert((previous.object_id.clone(), previous.version.clone()));
                }
            }
        }
        Ok(roots)
    }

    pub(crate) fn activate_file_history_repair_roots(
        &mut self,
        roots: impl IntoIterator<Item = (DfsObjectId, DfsVersion)>,
    ) -> Result<(), DfsError> {
        for (object, version) in roots {
            if self.reconcile_file_history_repair_root(&object, &version)? {
                self.record_message_committed_work(DfsMessageCommittedWork {
                    repair_maintenance: true,
                    ..Default::default()
                });
            }
        }
        Ok(())
    }

    /// Resume only repair work required by an immutable revision's current
    /// live roots. This does not recreate a reference or replay its mutation.
    /// A remote owner receives the same wake from exact-outcome reconciliation.
    /// sinomo:dfs-operation
    pub fn reconcile_content_reference_maintenance(
        &mut self,
        identity: &crate::DfsRevisionReference,
    ) -> Result<(), DfsError> {
        if self
            .metadata_backend
            .as_ref()
            .map(|backend| backend.get_version(&identity.object_id, &identity.version))
            .transpose()?
            .flatten()
            .is_some_and(|record| record.file_system_id != identity.file_system_id)
        {
            return Err(DfsError::Metadata {
                message: "reference maintenance names a foreign filesystem".into(),
            });
        }
        self.activate_file_history_repair_roots([(
            identity.object_id.clone(),
            identity.version.clone(),
        )])
    }

    /// A committed pin change owns the exact repair delta for that immutable
    /// version. This cold path scans only its object/version family and never
    /// turns a compact terminal retry receipt back into executable work.
    pub(crate) fn reconcile_file_history_repair_root(
        &mut self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
    ) -> Result<bool, DfsError> {
        let Some(version_record) = self
            .metadata_backend
            .as_ref()
            .map(|backend| backend.get_version(object_id, version))
            .transpose()?
            .flatten()
            .filter(|record| record.has_retention_grant())
        else {
            return Ok(false);
        };
        let Some(object) = self
            .metadata_backend
            .as_ref()
            .map(|b| b.get_object(object_id))
            .transpose()?
            .flatten()
        else {
            return Ok(false);
        };
        if !self.is_local_metadata_primary_for_object(&object)? {
            return Ok(false);
        }
        let mut cursor = None;
        let mut activated = false;
        let mut full_owner = false;
        let mut unfinished_owner = false;
        loop {
            let page = self
                .metadata_backend
                .as_ref()
                .expect("history metadata")
                .intents_for_object_version_page(object_id, version, cursor.as_ref(), 128)?;
            for record in page.intents {
                if record.file_system_id != object.file_system_id {
                    continue;
                }
                if crate::intent::write_intent_status_is_active(&record.status) {
                    // Capture metadata may commit before its original data
                    // intent terminalizes. That durable write still owns the
                    // operation and must not race a second maintenance owner.
                    unfinished_owner = true;
                    continue;
                }
                if record.status != DfsWriteIntentStatus::Committed {
                    continue;
                }
                if self
                    .metadata_backend
                    .as_ref()
                    .expect("history metadata")
                    .get_intent_receipt(object_id, &record.intent_id)?
                    .is_some()
                {
                    continue;
                }
                full_owner = true;
                let Some(hash) = &record.content_hash else {
                    continue;
                };
                let active = (record.publication.is_current()
                    && object.kind != DfsObjectKind::Tombstone
                    && object.content_hash.as_ref() == Some(hash))
                    || self.retained_file_content_is_live(object_id, version, hash)?;
                if !active {
                    self.park_inactive_retained_repair(&record)?;
                    continue;
                }
                activated |= self.activate_file_history_repair_record(&object, &record)?;
            }
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        if !full_owner && !unfinished_owner {
            let scope = self.metadata_effect_scope_for_object(
                &object,
                format!("metadata:{}", sinomo_core::Id::generate()),
            )?;
            let _execution = self.acquire_prepared_execution(&scope)?;
            // Admission may have queued: revalidate the real immutable version
            // and its recorded W under this existing owner before enrollment.
            let Some(version_record) = self
                .metadata_backend
                .as_ref()
                .expect("history metadata")
                .get_version(object_id, version)?
                .filter(|record| record.has_retention_grant())
            else {
                return Ok(activated);
            };
            let owner = self.recorded_retained_repair_owner(&version_record)?;
            let history = self
                .metadata_backend
                .as_ref()
                .expect("history metadata")
                .object_write_intent_history(object_id)?;
            if let Some(stored) = history
                .intents
                .iter()
                .find(|record| record.intent_id == owner.intent_id)
            {
                if !crate::file_history::retained_repair_plan_matches(stored, &owner) {
                    return Err(DfsError::Metadata {
                        message: "retained owner already has a different stored identity".into(),
                    });
                }
                activated |= self.activate_file_history_repair_record(&object, stored)?;
                return Ok(activated);
            }
            if history
                .receipts
                .iter()
                .any(|receipt| receipt.intent_id == owner.intent_id)
            {
                return Err(DfsError::Metadata {
                    message: "new retained owner identity already has a terminal receipt".into(),
                });
            }
            // The shared guarded commit enrolls this actual owner before
            // publication; recovery cannot execute its dormant queue row early.
            self.publish_captured_object_metadata_effects(
                &object,
                scope,
                vec![DfsPreparedMetadataEffect::PutIntent {
                    intent: owner.clone(),
                }],
            )?;
            if self.retained_file_content_is_live(
                object_id,
                version,
                owner
                    .content_hash
                    .as_deref()
                    .expect("validated repair hash"),
            )? {
                activated |= self.activate_file_history_repair_record(&object, &owner)?;
            }
        }
        Ok(activated)
    }

    /// Withdraw execution while retaining this actual stored owner's transfer
    /// facts. A released root can be pinned again before physical history GC.
    pub(crate) fn park_inactive_retained_repair(
        &mut self,
        record: &DfsWriteIntentRecord,
    ) -> Result<(), DfsError> {
        self.retire_committed_repair(&record.intent_id);
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(());
        };
        let stored = backend.object_write_intent_history(&record.object_id)?;
        let Some(owner) = stored.intents.iter().find(|owner| {
            owner.intent_id == record.intent_id
                && !owner.publication.is_current()
                && owner.status == DfsWriteIntentStatus::Committed
                && owner.file_system_id == record.file_system_id
                && owner.object_id == record.object_id
                && owner.target_version == record.target_version
                && owner.content_hash == record.content_hash
                && owner.desired_replica_node_ids == record.desired_replica_node_ids
        }) else {
            return Ok(());
        };
        let Some(version) = backend.get_version(&owner.object_id, &owner.target_version)? else {
            return Ok(());
        };
        if version.file_system_id != owner.file_system_id
            || version.content_hash != owner.content_hash
        {
            return Err(DfsError::Metadata {
                message: "inactive retained owner differs from its actual immutable version".into(),
            });
        }
        let receipts = backend.receipts_for_version(&owner.object_id, &owner.target_version)?;
        let Some(store) = self.owned_repair_queue_store()? else {
            return Ok(());
        };
        for mut row in
            store.records_for_targets(&owner.intent_id, &owner.desired_replica_node_ids)?
        {
            if row.object_id != owner.object_id
                || row.version != owner.target_version
                || Some(row.content_hash.as_str()) != owner.content_hash.as_deref()
            {
                return Err(DfsError::Metadata {
                    message: "inactive retained queue differs from its actual full owner".into(),
                });
            }
            if !repair_transfer_custody_unresolved(&row)
                && receipts.iter().any(|receipt| {
                    receipt.replica_node_id == row.missing_replica_node_id
                        && receipt.receipt.file_system_id == owner.file_system_id
                        && receipt.receipt.object_id == owner.object_id
                        && receipt.receipt.committed_version == owner.target_version
                        && receipt.content_hash == owner.content_hash
                })
            {
                continue;
            }
            // MAX is the existing parked deadline. No kind, claim, completion,
            // typed delivery evidence, attempt count or diagnostic is changed.
            row.next_retry_unix_nanos = u64::MAX;
            self.repair_attempt_records.insert(
                (row.intent_id.clone(), row.missing_replica_node_id.clone()),
                row,
            );
        }
        Ok(())
    }

    fn activate_file_history_repair_record(
        &mut self,
        object: &DfsObjectMetadata,
        record: &DfsWriteIntentRecord,
    ) -> Result<bool, DfsError> {
        self.coordinator.restore_intent_deferring_repair_rebuild(
            crate::intent::distributed_intent_from_record(record),
        )?;
        // Restoring an old canonical intent cannot rewind the current cache.
        self.coordinator.set_committed_version(
            object.file_system_id.clone(),
            object.object_id.clone(),
            object.version.clone(),
        );
        self.restore_backend_receipt_pages_for_intent(record, false)?;
        self.coordinator
            .set_committed_repair_active(&record.intent_id, true);
        // Hydration can already own the volatile jobs. That does not prove a
        // previous activation durably woke their parked restart cursor.
        if !self
            .coordinator
            .capture_active_repair_jobs(&record.intent_id)
        {
            return Ok(false);
        }
        self.restore_pending_write_for_intent_if_missing(&record.intent_id, &record.object_id)?;
        let activated = self.writes.pending_writes.contains_key(&record.intent_id);
        if activated && let Some(store) = self.owned_repair_queue_store()? {
            let mut current =
                store.records_for_targets(&record.intent_id, &record.desired_replica_node_ids)?;
            for row in &current {
                if row.object_id != record.object_id
                    || row.version != record.target_version
                    || Some(row.content_hash.as_str()) != record.content_hash.as_deref()
                {
                    return Err(DfsError::Metadata {
                        message: "retained activation queue differs from its actual full owner"
                            .into(),
                    });
                }
            }
            let eligible = current
                .iter()
                .filter(|row| crate::repair::retained_repair_schedule_can_wake(row))
                .cloned()
                .collect::<Vec<_>>();
            let eligible_keys = eligible
                .iter()
                .map(|row| (row.intent_id.clone(), row.missing_replica_node_id.clone()))
                .collect::<BTreeSet<_>>();
            // EnqueueIfMissing deliberately preserves existing backoff. The
            // actual live Retain must instead durably wake these exact settled
            // observations, without overwriting a concurrent claim or removal.
            let mut observed = store
                .wake_observed_settled_records(&eligible, dfs_now_unix_nanos())?
                .into_iter()
                .map(|row| {
                    (
                        (row.intent_id.clone(), row.missing_replica_node_id.clone()),
                        row,
                    )
                })
                .collect::<BTreeMap<_, _>>();
            current.retain_mut(|row| {
                let key = (row.intent_id.clone(), row.missing_replica_node_id.clone());
                if !eligible_keys.contains(&key) {
                    return true;
                }
                if let Some(actual) = observed.remove(&key) {
                    *row = actual;
                    true
                } else {
                    false
                }
            });
            self.restore_repair_attempt_records_merging(&current);
        }
        // Activation is scheduling, not settlement. Existing and re-pinned
        // owners retain every retry fact, UNKNOWN claim and exact request map.
        Ok(activated)
    }

    pub(crate) fn cleanup_committed_pending_write_if_repaired(&mut self, intent_id: &str) {
        let is_committed = self
            .coordinator
            .intent(intent_id)
            .is_some_and(|intent| matches!(intent.state, DfsWriteIntentState::Committed));
        if !is_committed {
            return;
        }
        let has_repair_jobs = self
            .coordinator
            .repair_jobs()
            .iter()
            .any(|job| job.intent_id == intent_id);
        if has_repair_jobs {
            return;
        }
        if self
            .writes
            .pending_writes
            .get(intent_id)
            .is_some_and(|pending| {
                pending
                    .repair_retries
                    .values()
                    .any(|state| state.inflight || state.next_retry_tick > self.repair_retry_tick)
            })
        {
            return;
        }
        self.remove_pending_write_state(intent_id);
    }

    fn retry_pending_stages_scoped_bounded(
        &mut self,
        intent_filter: Option<&BTreeSet<String>>,
        budget: &mut DfsRepairMaintenanceBudget,
    ) -> Result<(), DfsError> {
        let pending_intent_ids = self
            .writes
            .pending_writes
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for intent_id in pending_intent_ids {
            if budget.is_exhausted() {
                break;
            }
            if intent_filter.is_some_and(|intent_ids| !intent_ids.contains(&intent_id)) {
                continue;
            }
            let Some(intent) = self.coordinator.intent(&intent_id).cloned() else {
                continue;
            };
            if !matches!(intent.state, DfsWriteIntentState::AwaitingReceipts) {
                continue;
            }
            let Some(object_id) = self
                .writes
                .pending_writes
                .get(&intent_id)
                .map(|pending| pending.object_id.clone())
            else {
                continue;
            };
            let object_metadata = self.current_object_for_write(&object_id)?;
            self.stage_missing_desired_replicas_for_intent_bounded(
                &intent_id,
                object_metadata,
                budget,
            )?;
        }
        Ok(())
    }

    pub(crate) fn should_stage_missing_replicas_after_write(&self, intent_id: &str) -> bool {
        matches!(
            self.coordinator
                .intent(intent_id)
                .map(|intent| &intent.state),
            Some(DfsWriteIntentState::AwaitingReceipts)
        )
    }

    pub(crate) fn stage_missing_desired_replicas_for_intent(
        &mut self,
        intent_id: &str,
        object_metadata: DfsObjectMetadata,
    ) -> Result<(), DfsError> {
        let mut budget = DfsRepairMaintenanceBudget::new(usize::MAX);
        self.stage_missing_desired_replicas_for_intent_bounded(
            intent_id,
            object_metadata,
            &mut budget,
        )
    }

    fn stage_missing_desired_replicas_for_intent_bounded(
        &mut self,
        intent_id: &str,
        object_metadata: DfsObjectMetadata,
        budget: &mut DfsRepairMaintenanceBudget,
    ) -> Result<(), DfsError> {
        let Some(intent) = self.coordinator.intent(intent_id).cloned() else {
            return Ok(());
        };
        // StageBlob is a pre-commit protocol. Once W publishes the version,
        // missing desired replicas converge through RepairReplica so a target
        // can never own both transfer kinds for the same intent.
        if !matches!(intent.state, DfsWriteIntentState::AwaitingReceipts) {
            return Ok(());
        }
        loop {
            if budget.is_exhausted() {
                break;
            }
            let Some(pending) = self.writes.pending_writes.get(intent_id).cloned() else {
                break;
            };
            let durable_nodes = self
                .coordinator
                .receipts_for_intent(intent_id)
                .into_iter()
                .map(|receipt| receipt.replica_node_id.clone())
                .collect::<BTreeSet<_>>();
            let mut stage_targets =
                self.pending_stage_targets(&intent, &pending, &durable_nodes, None);
            if stage_targets.is_empty() {
                if intent.desired_replica_nodes.contains(&self.node_id)
                    && !durable_nodes.contains(&self.node_id)
                    && self.pending_stage_send_budget(&intent, &pending, &durable_nodes, None) > 0
                    && budget.reserve(1) == 1
                {
                    self.request_stage_blob_from_alternate(&intent, &pending)?;
                }
                break;
            }
            let reserved = budget.reserve(stage_targets.len());
            stage_targets.truncate(reserved);
            if stage_targets.is_empty() {
                break;
            }
            let mut full_stage_targets = Vec::new();

            for replica_node_id in stage_targets {
                let request_id = pending
                    .stage_inflight_request_ids
                    .get(&replica_node_id)
                    .cloned()
                    .unwrap_or_else(|| self.next_request_id());
                match self.send_pending_stage_patch_if_routable(
                    &replica_node_id,
                    request_id.clone(),
                    &intent,
                    &pending,
                    Some(object_metadata.clone()),
                )? {
                    Some(outcome) if outcome.sent => {
                        self.record_stage_inflight(intent_id, &replica_node_id, request_id);
                    }
                    Some(_) => {
                        self.record_stage_retry_scheduled(intent_id, &replica_node_id);
                    }
                    None => full_stage_targets.push((replica_node_id, request_id)),
                }
            }

            if full_stage_targets.is_empty() {
                continue;
            }
            if self.open_pending_write_blob_stream(&pending).is_err() {
                let fetch_requested = self.request_stage_blob_from_alternate(&intent, &pending)?;
                if !fetch_requested {
                    for (replica_node_id, _) in full_stage_targets {
                        self.record_stage_retry_scheduled(intent_id, &replica_node_id);
                    }
                }
                break;
            }
            let outcomes = self.send_stage_blob_attachments_if_routable(
                &full_stage_targets,
                intent_id,
                &intent.file_system_id,
                &intent.object_id,
                intent.base_version.clone(),
                &intent.target_version,
                &pending.content_hash,
                pending.size_bytes,
                Some(object_metadata.clone()),
            )?;
            for ((replica_node_id, request_id), outcome) in
                full_stage_targets.into_iter().zip(outcomes)
            {
                if outcome.sent {
                    self.record_stage_inflight(intent_id, &replica_node_id, request_id);
                } else {
                    self.record_stage_retry_scheduled(intent_id, &replica_node_id);
                }
            }
        }
        Ok(())
    }

    fn send_pending_stage_patch_if_routable(
        &mut self,
        replica_node_id: &NodeId,
        request_id: String,
        intent: &DistributedWriteIntent,
        pending: &PendingNetworkedWrite,
        object_metadata: Option<DfsObjectMetadata>,
    ) -> Result<Option<StageBlobSendOutcome>, DfsError> {
        let Some(patch) = pending.patch.as_ref() else {
            return Ok(None);
        };
        if patch.full_stage_fallback_nodes.contains(replica_node_id) {
            return Ok(None);
        }
        let Some(base_version) = intent.base_version.as_ref() else {
            return Ok(None);
        };
        if base_version != &patch.base_version {
            return Ok(None);
        }
        let outcome = self.send_stage_patch_attachment_if_routable(
            replica_node_id,
            request_id,
            &intent.intent_id,
            &intent.file_system_id,
            &intent.object_id,
            base_version,
            &intent.target_version,
            &pending.content_hash,
            patch.offset,
            &patch.patch_hash,
            patch.patch_size_bytes,
            object_metadata,
        )?;
        Ok(Some(outcome))
    }

    pub(crate) fn request_stage_blob_from_alternate(
        &mut self,
        intent: &DistributedWriteIntent,
        pending: &PendingNetworkedWrite,
    ) -> Result<bool, DfsError> {
        let mut skipped_failed_source = false;
        let authority_object = self.current_read_object_metadata(&pending.object_id).ok();
        let authority_shard = self.command_authority_shard_for_object(authority_object.as_ref())?;
        for source_node_id in self.stage_blob_sources(&intent.intent_id) {
            if pending.fetch_inflight_from.contains(&source_node_id) {
                continue;
            }
            if pending.fetch_failed_from.contains(&source_node_id) {
                skipped_failed_source = true;
                continue;
            }
            let request_id = pending
                .fetch_inflight_request_ids
                .get(&source_node_id)
                .cloned()
                .unwrap_or_else(|| self.next_request_id());
            let sent = self.send_best_effort_message_if_routable(
                &source_node_id,
                DfsNetworkMessage::FetchBlob(DfsFetchBlobCommand {
                    request_id: request_id.clone(),
                    intent_id: intent.intent_id.clone(),
                    object_id: pending.object_id.clone(),
                    version: pending.version.clone(),
                    content_hash: pending.content_hash.clone(),
                    authority_shard: authority_shard.clone(),
                }),
            )?;
            if sent {
                self.record_fetch_inflight(&intent.intent_id, source_node_id, request_id);
                return Ok(true);
            }
        }
        if skipped_failed_source
            && let Some(pending) = self.writes.pending_writes.get_mut(&intent.intent_id)
        {
            pending.fetch_failed_from.clear();
        }
        Ok(false)
    }

    pub(crate) fn stage_blob_sources(&self, intent_id: &str) -> Vec<NodeId> {
        let mut sources = self
            .coordinator
            .receipts_for_intent(intent_id)
            .into_iter()
            .filter_map(|receipt| {
                (receipt.replica_node_id != self.node_id).then_some(receipt.replica_node_id.clone())
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<BTreeSet<_>>();
        if let Some(intent) = self.coordinator.intent(intent_id)
            && intent.requesting_node_id != self.node_id
        {
            // A requester whose pre-staged blob was excluded by snapshot
            // replanning is a transfer source, not a durable replica.
            // Content-hash verification still gates every fetch response.
            sources.insert(intent.requesting_node_id.clone());
        }
        self.ranked_transfer_sources(sources)
    }

    fn record_fetch_inflight(
        &mut self,
        intent_id: &str,
        source_node_id: NodeId,
        request_id: String,
    ) {
        let tick = self.repair_retry_tick;
        self.passive_fetch_started_at.insert(
            (intent_id.to_string(), source_node_id.clone()),
            std::time::Instant::now(),
        );
        if let Some(pending) = self.writes.pending_writes.get_mut(intent_id) {
            pending.fetch_inflight_from.insert(source_node_id.clone());
            pending
                .fetch_inflight_request_ids
                .insert(source_node_id.clone(), request_id);
            pending
                .fetch_inflight_started_ticks
                .insert(source_node_id.clone(), tick);
            let retry_until_tick = tick.saturating_add(fetch_inflight_timeout_ticks());
            for state in pending.repair_retries.values_mut() {
                if state.inflight && !repair_retry_state_is_suppressed(state) {
                    state.next_retry_tick = state.next_retry_tick.max(retry_until_tick);
                }
            }
        }
    }

    pub(crate) fn expire_stale_fetch_requests(&mut self) {
        let tick = self.repair_retry_tick;
        for (intent_id, pending) in &mut self.writes.pending_writes {
            let expired_sources = pending
                .fetch_inflight_started_ticks
                .iter()
                .filter_map(|(source_node_id, started_tick)| {
                    let expires_at = started_tick.saturating_add(fetch_inflight_timeout_ticks());
                    (expires_at <= tick).then_some(source_node_id.clone())
                })
                .collect::<Vec<_>>();
            if expired_sources.is_empty() {
                continue;
            }
            for source_node_id in expired_sources {
                pending.fetch_inflight_from.remove(&source_node_id);
                pending.fetch_inflight_started_ticks.remove(&source_node_id);
                self.passive_fetch_started_at
                    .remove(&(intent_id.clone(), source_node_id.clone()));
                pending.fetch_failed_from.insert(source_node_id);
            }
            for state in pending.stage_retries.values_mut() {
                state.next_retry_tick = 0;
                state.inflight = false;
            }
            for state in pending.repair_retries.values_mut() {
                wake_repair_retry_state(state);
                state.inflight = false;
            }
        }
    }

    fn stage_retry_is_due(&self, intent_id: &str, replica_node_id: &NodeId) -> bool {
        self.writes
            .pending_writes
            .get(intent_id)
            .and_then(|pending| pending.stage_retries.get(replica_node_id))
            .is_none_or(|state| state.next_retry_tick <= self.repair_retry_tick)
    }

    fn record_stage_inflight(
        &mut self,
        intent_id: &str,
        replica_node_id: &NodeId,
        request_id: String,
    ) {
        let tick = self.repair_retry_tick;
        if let Some(pending) = self.writes.pending_writes.get_mut(intent_id) {
            let state = pending
                .stage_retries
                .entry(replica_node_id.clone())
                .or_default();
            state.attempts = state.attempts.saturating_add(1);
            state.inflight = true;
            state.transfer_delivery_state =
                Some(sinomo_core::network::NetworkDeliveryState::PossiblyApplied);
            state.completed_transfer_request_id = None;
            state.next_retry_tick =
                tick.saturating_add(stage_inflight_retry_delay_ticks(state.attempts));
            pending
                .stage_inflight_request_ids
                .insert(replica_node_id.clone(), request_id);
        }
    }

    pub(crate) fn record_stage_retry_scheduled(
        &mut self,
        intent_id: &str,
        replica_node_id: &NodeId,
    ) {
        let tick = self.repair_retry_tick;
        if let Some(pending) = self.writes.pending_writes.get_mut(intent_id) {
            let state = pending
                .stage_retries
                .entry(replica_node_id.clone())
                .or_default();
            if !state.inflight {
                state.attempts = state.attempts.saturating_add(1);
            }
            state.inflight = false;
            state.next_retry_tick = tick.saturating_add(repair_retry_delay_ticks(state.attempts));
        }
    }

    pub(crate) fn record_stage_patch_fallback(
        &mut self,
        intent_id: &str,
        replica_node_id: &NodeId,
    ) {
        if let Some(pending) = self.writes.pending_writes.get_mut(intent_id)
            && let Some(patch) = pending.patch.as_mut()
        {
            patch
                .full_stage_fallback_nodes
                .insert(replica_node_id.clone());
        }
    }

    pub(crate) fn repair_retry_is_due(&self, intent_id: &str, replica_node_id: &NodeId) -> bool {
        if self
            .writes
            .pending_remote_write_responses
            .get(intent_id)
            .is_some_and(|pending| pending.foreground_replica_repair)
        {
            let Some(pending_write) = self.writes.pending_writes.get(intent_id) else {
                return false;
            };
            return pending_write
                .repair_retries
                .get(replica_node_id)
                .is_none_or(|state| !repair_retry_state_is_suppressed(state) && !state.inflight);
        }
        if self
            .repair_attempt_records
            .get(&(intent_id.to_string(), replica_node_id.clone()))
            .is_some_and(|record| record.next_retry_unix_nanos > dfs_now_unix_nanos())
        {
            return false;
        }
        self.writes
            .pending_writes
            .get(intent_id)
            .and_then(|pending| pending.repair_retries.get(replica_node_id))
            .is_none_or(|state| {
                !repair_retry_state_is_suppressed(state)
                    && state.next_retry_tick <= self.repair_retry_tick
            })
    }

    fn restore_repair_attempt_records(&mut self, records: &[DfsRepairAttemptRecord]) {
        self.repair_attempt_records
            .retain(|_, record| repair_transfer_custody_unresolved(record));
        self.restore_repair_attempt_records_merging(records);
    }

    pub(crate) fn restore_repair_attempt_records_merging(
        &mut self,
        records: &[DfsRepairAttemptRecord],
    ) {
        for record in records {
            if self
                .coordinator
                .repair_job_for_target(&record.intent_id, &record.missing_replica_node_id)
                .is_none()
            {
                let key = (
                    record.intent_id.clone(),
                    record.missing_replica_node_id.clone(),
                );
                if repair_transfer_custody_unresolved(record) {
                    let mut current = record.clone();
                    if self
                        .repair_attempt_records
                        .get(&key)
                        .is_some_and(|parked| parked.next_retry_unix_nanos == u64::MAX)
                    {
                        current.next_retry_unix_nanos = u64::MAX;
                    }
                    self.repair_attempt_records.insert(key, current);
                } else if !record.in_flight
                    && record.transfer_request_id.is_none()
                    && record.transfer_kind.is_some()
                    && let Some(completed) = record.completed_transfer_request_id.as_ref()
                    && self.repair_attempt_records.get(&key).is_some_and(|cached| {
                        cached.object_id == record.object_id
                            && cached.version == record.version
                            && cached.content_hash == record.content_hash
                            && cached.transfer_kind == record.transfer_kind
                            && cached.transfer_request_id.as_ref() == Some(completed)
                            && cached.completed_transfer_request_id.is_none()
                    })
                {
                    // Another admitted lane consumed this exact native reply.
                    // Its durable terminal settles our older carrier DATA even
                    // after the canonical receipt removed the repair job. Do
                    // not export that stale in-flight row over the terminal.
                    // A different or unknown request/kind remains unresolved.
                    self.repair_attempt_records.remove(&key);
                }
                // The durable queue is a restart cursor for an authoritative
                // repair job, not an independent source of repair authority.
                // A receipt can settle that job before this node consumes its
                // queued retry. Do not resurrect the already-cleared target
                // from stale retry/backoff bookkeeping.
                continue;
            }
            let Some(pending) = self.writes.pending_writes.get_mut(&record.intent_id) else {
                continue;
            };
            if pending.object_id != record.object_id
                || pending.version != record.version
                || pending.content_hash != record.content_hash
            {
                continue;
            }
            let mut restored_record = record.clone();
            let now = dfs_now_unix_nanos();
            if repair_attempt_record_indicates_transient_route_failure(&restored_record)
                && !restored_record.in_flight
                && restored_record.transfer_request_id.is_none()
                && restored_record.next_retry_unix_nanos > now
            {
                restored_record.in_flight = false;
                restored_record.next_retry_unix_nanos = now;
                restored_record.transfer_request_id = None;
            }
            self.repair_attempt_records.insert(
                (
                    restored_record.intent_id.clone(),
                    restored_record.missing_replica_node_id.clone(),
                ),
                restored_record.clone(),
            );
            use sinomo_api::dfs::maintenance::DfsRepairTransferKind;
            let (requests, retries) = match restored_record.transfer_kind {
                Some(DfsRepairTransferKind::Stage) => (
                    &mut pending.stage_inflight_request_ids,
                    &mut pending.stage_retries,
                ),
                _ => (
                    &mut pending.repair_inflight_request_ids,
                    &mut pending.repair_retries,
                ),
            };
            let state = retries
                .entry(restored_record.missing_replica_node_id.clone())
                .or_default();
            state.attempts = state.attempts.max(restored_record.attempts);
            state.unresolved_custody_without_request =
                restored_record.transfer_request_id.is_none()
                    && (restored_record.transfer_kind.is_none()
                        || (restored_record.transfer_delivery_state.is_none()
                            && restored_record.completed_transfer_request_id.is_none()));
            state.transfer_delivery_state = restored_record.transfer_delivery_state;
            state.completed_transfer_request_id = if restored_record.transfer_kind.is_some() {
                restored_record.completed_transfer_request_id.clone()
            } else {
                None
            };
            state.inflight =
                restored_record.in_flight && restored_record.next_retry_unix_nanos > now;
            if repair_attempt_record_indicates_lost_authority(&restored_record)
                && !restored_record.in_flight
                && restored_record.transfer_request_id.is_none()
            {
                state.next_retry_tick = DFS_REPAIR_RETRY_SUPPRESSED_TICK;
            }
            if restored_record.transfer_kind.is_some() {
                if let Some(request_id) = restored_record.transfer_request_id.as_ref() {
                    if requests
                        .get(&restored_record.missing_replica_node_id)
                        .is_none_or(|current| current == request_id)
                    {
                        requests.insert(
                            restored_record.missing_replica_node_id.clone(),
                            request_id.clone(),
                        );
                    }
                } else if !state.inflight {
                    requests.remove(&restored_record.missing_replica_node_id);
                }
            }
        }
    }

    fn repair_queue_records(&self, include_unattempted: bool) -> Vec<DfsRepairAttemptRecord> {
        self.repair_queue_records_for_targets(include_unattempted, None)
    }

    pub(crate) fn repair_queue_records_for_targets(
        &self,
        include_unattempted: bool,
        targets: Option<&BTreeSet<(String, NodeId)>>,
    ) -> Vec<DfsRepairAttemptRecord> {
        let now = dfs_now_unix_nanos();
        let mut records = Vec::new();
        let pending: Vec<_> = if let Some(targets) = targets {
            targets
                .iter()
                .map(|(id, _)| id)
                .collect::<BTreeSet<_>>()
                .into_iter()
                .filter_map(|id| self.writes.pending_writes.get_key_value(id))
                .collect()
        } else {
            self.writes.pending_writes.iter().collect()
        };
        for (intent_id, pending) in pending {
            for replica_node_id in pending.repair_retries.keys() {
                if self
                    .coordinator
                    .repair_job_for_target(intent_id, replica_node_id)
                    .is_none()
                    && !self
                        .repair_attempt_records
                        .get(&(intent_id.clone(), replica_node_id.clone()))
                        .is_some_and(repair_transfer_custody_unresolved)
                {
                    continue;
                }

                if targets.is_some_and(|targets| {
                    !targets.contains(&(intent_id.clone(), replica_node_id.clone()))
                }) {
                    continue;
                }
                let key = (intent_id.clone(), replica_node_id.clone());
                if let Some(existing) = self
                    .repair_attempt_records
                    .get(&(intent_id.clone(), replica_node_id.clone()))
                    && existing.next_retry_unix_nanos == u64::MAX
                    && self
                        .coordinator
                        .repair_job_for_target(intent_id, replica_node_id)
                        .is_none()
                {
                    records.push(existing.clone());
                    continue;
                }
                if let Some(existing) = self
                    .repair_attempt_records
                    .get(&(intent_id.clone(), replica_node_id.clone()))
                {
                    if existing.transfer_kind
                        == Some(sinomo_api::dfs::maintenance::DfsRepairTransferKind::Stage)
                        || existing.transfer_kind.is_none()
                    {
                        records.push(existing.clone());
                        continue;
                    }
                }
                let (observed_state, observed_request_id, transfer_kind) =
                    pending_repair_transfer_observation(pending, replica_node_id);
                // Missing producer state remains unknown; the actual named ID
                // still retains custody and cannot become NotSent.
                let unknown_state = crate::transfer::RepairRetryState::default();
                let state = observed_state.unwrap_or(&unknown_state);
                if repair_retry_state_is_suppressed(state) {
                    if let Some(existing) = self.repair_attempt_records.get(&key)
                        && repair_attempt_record_indicates_lost_authority(existing)
                    {
                        let mut record = existing.clone();
                        record.attempts = record.attempts.max(state.attempts);
                        record.in_flight = state.inflight;
                        record.next_retry_unix_nanos = u64::MAX;
                        record.transfer_request_id = observed_request_id.cloned();
                        record.transfer_kind = Some(transfer_kind);
                        record.transfer_delivery_state = state.transfer_delivery_state;
                        record.completed_transfer_request_id =
                            state.completed_transfer_request_id.clone();
                        records.push(record);
                    }
                    continue;
                }
                if state.attempts == 0 && !include_unattempted {
                    continue;
                }
                let transfer_request_id = observed_request_id.cloned();
                if let Some(existing) = self.repair_attempt_records.get(&key)
                    && existing.next_retry_unix_nanos > now
                    && existing.attempts >= state.attempts
                    && existing.in_flight == state.inflight
                    && existing.transfer_request_id == transfer_request_id
                    && existing.transfer_delivery_state == state.transfer_delivery_state
                    && existing.completed_transfer_request_id == state.completed_transfer_request_id
                {
                    records.push(existing.clone());
                    continue;
                }
                let remaining_ticks = state.next_retry_tick.saturating_sub(self.repair_retry_tick);
                let next_retry_unix_nanos =
                    now.saturating_add(repair_retry_delay_nanos_for_ticks(remaining_ticks));
                let last_error = self
                    .repair_attempt_records
                    .get(&key)
                    .and_then(|record| record.last_error.clone());
                // Exporting a due restored row is not a new physical attempt.
                // Preserve its native attempt time when the producer's exact
                // attempt and custody facts have not changed; only the retry
                // deadline below is reconstructed from the scheduler ticks.
                let unchanged_attempt = self.repair_attempt_records.get(&key).filter(|record| {
                    record.attempts == state.attempts
                        && record.in_flight == state.inflight
                        && record.transfer_request_id == transfer_request_id
                        && record.transfer_delivery_state == state.transfer_delivery_state
                        && record.completed_transfer_request_id
                            == state.completed_transfer_request_id
                        && record.transfer_kind == Some(transfer_kind)
                });
                records.push(DfsRepairAttemptRecord {
                    intent_id: intent_id.clone(),
                    object_id: pending.object_id.clone(),
                    version: pending.version.clone(),
                    content_hash: pending.content_hash.clone(),
                    missing_replica_node_id: replica_node_id.clone(),
                    attempts: state.attempts,
                    in_flight: state.inflight,
                    last_attempt_unix_nanos: unchanged_attempt.map_or_else(
                        || if state.attempts == 0 { 0 } else { now },
                        |record| record.last_attempt_unix_nanos,
                    ),
                    next_retry_unix_nanos: if state.attempts == 0 {
                        now
                    } else {
                        next_retry_unix_nanos
                    },
                    transfer_request_id,
                    last_error,
                    transfer_delivery_state: state.transfer_delivery_state,
                    completed_transfer_request_id: state.completed_transfer_request_id.clone(),
                    transfer_kind: Some(transfer_kind),
                });
            }
        }
        if include_unattempted {
            let mut recorded_targets = records
                .iter()
                .map(|record| {
                    (
                        record.intent_id.clone(),
                        record.missing_replica_node_id.clone(),
                    )
                })
                .collect::<BTreeSet<_>>();
            let jobs: Vec<_> = if let Some(targets) = targets {
                targets
                    .iter()
                    .filter_map(|(id, target)| self.coordinator.repair_job_for_target(id, target))
                    .collect()
            } else {
                self.coordinator.repair_jobs().to_vec()
            };
            for job in &jobs {
                let key = (job.intent_id.clone(), job.missing_replica_node_id.clone());
                if !recorded_targets.insert(key) {
                    continue;
                }
                let Some(pending) = self.writes.pending_writes.get(&job.intent_id) else {
                    continue;
                };
                let key = (job.intent_id.clone(), job.missing_replica_node_id.clone());
                if let Some(existing) = self.repair_attempt_records.get(&key) {
                    records.push(existing.clone());
                    continue;
                }
                let producer = pending.repair_retries.get(&job.missing_replica_node_id);
                records.push(DfsRepairAttemptRecord {
                    transfer_delivery_state: producer
                        .and_then(|state| state.transfer_delivery_state),
                    completed_transfer_request_id: producer
                        .and_then(|state| state.completed_transfer_request_id.clone()),
                    transfer_kind: Some(
                        sinomo_api::dfs::maintenance::DfsRepairTransferKind::Repair,
                    ),
                    intent_id: job.intent_id.clone(),
                    object_id: pending.object_id.clone(),
                    version: pending.version.clone(),
                    content_hash: pending.content_hash.clone(),
                    missing_replica_node_id: job.missing_replica_node_id.clone(),
                    attempts: 0,
                    in_flight: false,
                    last_attempt_unix_nanos: 0,
                    next_retry_unix_nanos: now,
                    transfer_request_id: None,
                    last_error: None,
                });
            }
        }
        let represented = records
            .iter()
            .map(|record| {
                (
                    record.intent_id.clone(),
                    record.missing_replica_node_id.clone(),
                )
            })
            .collect::<BTreeSet<_>>();
        for (key, record) in &self.repair_attempt_records {
            if !represented.contains(key)
                && (repair_transfer_custody_unresolved(record)
                    || record.next_retry_unix_nanos == u64::MAX)
                && targets.is_none_or(|targets| targets.contains(key))
            {
                records.push(record.clone());
            }
        }
        records.sort_by(|left, right| {
            left.intent_id.cmp(&right.intent_id).then_with(|| {
                left.missing_replica_node_id
                    .cmp(&right.missing_replica_node_id)
            })
        });
        records
    }

    pub(crate) fn active_repair_attempt_records(&self) -> Vec<DfsRepairAttemptRecord> {
        self.repair_queue_records(false)
    }

    /// Return the exact bounded repair work owned by this live service lane.
    ///
    /// Unlike [`Self::active_repair_attempt_records`], this includes jobs which
    /// have not made their first network attempt. The daemon persists these
    /// zero-attempt records at the same committed edge that wakes reactive
    /// maintenance, so a restart never needs a full metadata inventory to
    /// rediscover ordinary repair work.
    pub fn durable_repair_queue_records(&self) -> Vec<DfsRepairAttemptRecord> {
        self.repair_queue_records(true)
    }

    /// The queue is derived restart DATA. Native history GC can replace a
    /// terminal canonical receipt with an already-stored retained repair owner.
    /// Only that actual matching owner can reconstruct work after compaction;
    /// absence of the old identity cannot create an intent or authorize a send.
    fn stored_repair_record_for_queue(
        &self,
        records: &[DfsRepairAttemptRecord],
        orphaned_custody: &mut BTreeSet<String>,
    ) -> Result<Option<(DfsWriteIntentRecord, bool)>, DfsError> {
        let Some(first) = records.first() else {
            return Ok(None);
        };
        let unknown = || DfsError::Distributed {
            message: format!("unknown pending write intent: {}", first.intent_id),
        };
        let Some(backend) = self.metadata_backend.as_ref() else {
            return if self.coordinator.intent(&first.intent_id).is_some() {
                Ok(None)
            } else {
                Err(unknown())
            };
        };
        let history = backend.object_write_intent_history(&first.object_id)?;
        if let Some(record) = history
            .intents
            .iter()
            .find(|record| record.intent_id == first.intent_id)
        {
            return Ok(Some((record.clone(), false)));
        }
        let Some(receipt) = history
            .receipts
            .iter()
            .find(|receipt| receipt.intent_id == first.intent_id)
        else {
            // A removed owner's unresolved carrier evidence remains inert.
            // MAX cannot grant a new owner or a transfer. Fresh prepublication
            // NotSent enrollment still requires its actual stored owner below.
            if records.iter().all(|row| {
                row.next_retry_unix_nanos == u64::MAX && repair_transfer_custody_unresolved(row)
            }) {
                return Ok(None);
            }
            // History GC may remove the executable owner before a cold lane
            // observes the carrier's parked deadline. Preserve only the SAME
            // unresolved records actually held by this owner's durable queue.
            // An incoming tuple or fresh NotSent row cannot establish custody.
            if records.iter().all(|row| {
                repair_transfer_custody_unresolved(row)
                    && row.transfer_kind.is_some()
                    && row.transfer_request_id.is_some()
                    && row.completed_transfer_request_id.is_none()
                    && row.attempts > 0
                    && row.last_attempt_unix_nanos > 0
            }) && self
                .coordinator
                .intent(&first.intent_id)
                .is_none_or(|intent| intent.state == DfsWriteIntentState::Committed)
            {
                if let Some(store) = self.owned_repair_queue_store()? {
                    let targets = records
                        .iter()
                        .map(|row| row.missing_replica_node_id.clone())
                        .collect::<Vec<_>>();
                    let actual = store.records_for_targets(&first.intent_id, &targets)?;
                    if records
                        .iter()
                        .all(|row| actual.iter().any(|stored| stored == row))
                    {
                        orphaned_custody.insert(first.intent_id.clone());
                        return Ok(None);
                    }
                }
            }
            return Err(unknown());
        };
        if receipt.publication.is_current()
            && receipt.status == DfsWriteIntentStatus::Committed
            && records.iter().all(|queued| {
                queued.intent_id == receipt.intent_id
                    && queued.object_id == receipt.object_id
                    && queued.version == receipt.target_version
                    && receipt.content_hash.as_deref() == Some(queued.content_hash.as_str())
                    && receipt
                        .desired_replica_node_ids
                        .contains(&queued.missing_replica_node_id)
            })
        {
            // Preserve the existing canonical Current W-to-N receipt contract.
            // This is an exact Current projection, not a stored Retained owner.
            return Ok(Some((receipt.to_intent_record(), false)));
        }
        if receipt.status != DfsWriteIntentStatus::Committed
            || receipt.publication.is_current()
            || receipt.publication.is_retained_repair()
            || receipt.object_id != first.object_id
            || receipt.target_version != first.version
            || receipt.content_hash.as_deref() != Some(first.content_hash.as_str())
        {
            return Err(unknown());
        }
        let Some(version) = backend.get_version(&first.object_id, &first.version)? else {
            return Err(unknown());
        };
        if !version.has_retention_grant()
            || records.iter().any(|queued| {
                queued.intent_id != first.intent_id
                    || queued.object_id != version.object_id
                    || queued.version != version.version
                    || version.content_hash.as_deref() != Some(queued.content_hash.as_str())
            })
        {
            return Err(unknown());
        }
        // The ordinary factory validates the version's exact recorded W/N and
        // real W receipts. Its DATA is used solely to compare the stored owner.
        let expected = self.recorded_retained_repair_owner(&version)?;
        let actual = history
            .intents
            .iter()
            .find(|stored| stored.intent_id == expected.intent_id)
            .cloned();
        let actual = matching_stored_compacted_repair_owner(&version, &expected, actual, records)?;
        Ok(Some((actual, true)))
    }

    /// Refresh only a queued current intent's derived receipt projection.
    /// Another actual lane can finish its receipts while this coordinator stays
    /// resident. Read the raw location row and exact byte receipts together;
    /// receipt durability alone cannot finish an unpublished location effect.
    fn refresh_current_repair_receipt_projection(
        &mut self,
        record: &DfsWriteIntentRecord,
        scheduling: &[DfsRepairAttemptRecord],
        yield_on_source_fence: bool,
    ) -> Result<Option<bool>, DfsError> {
        if !record.publication.is_current()
            || record.status != DfsWriteIntentStatus::Committed
            || self.coordinator.intent(&record.intent_id)
                != Some(&crate::intent::distributed_intent_from_record(record))
        {
            return Ok(Some(false));
        }
        let Some(backend) = self.metadata_backend.as_mut() else {
            return Ok(Some(false));
        };
        let Some(first) = backend.get_object(&record.object_id)? else {
            return Ok(Some(false));
        };
        if yield_on_source_fence {
            if !backend
                .try_begin_resident_read_snapshot_for_path(&first.file_system_id, &first.path)?
            {
                return Ok(None);
            }
        } else {
            backend.begin_consistent_read_snapshot_for_path(&first.file_system_id, &first.path)?;
        }
        let cut = (|| -> Result<_, DfsError> {
            Ok((
                backend.get_object(&record.object_id)?,
                backend.get_version(&record.object_id, &record.target_version)?,
                backend.receipts_for_version(&record.object_id, &record.target_version)?,
            ))
        })();
        backend.end_consistent_read_snapshot_for_path(&first.file_system_id, &first.path);
        let (Some(object), Some(version), receipts) = cut? else {
            return Ok(Some(false));
        };
        if object.file_system_id != record.file_system_id
            || object.version != record.target_version
            || object.content_hash != record.content_hash
            || !version.visible
            || version.file_system_id != record.file_system_id
            || version.object_id != record.object_id
            || version.version != record.target_version
            || version.content_hash != record.content_hash
            || object.content_size_bytes != version.size_bytes
        {
            return Ok(Some(false));
        }
        let receipts = receipts
            .into_iter()
            .filter(|receipt| {
                record
                    .desired_replica_node_ids
                    .contains(&receipt.replica_node_id)
                    && crate::content_durability::receipt_matches_target_version(
                        receipt,
                        &version,
                        &receipt.replica_node_id,
                    )
            })
            .collect::<Vec<_>>();
        let cached = self
            .coordinator
            .receipts_for_intent(&record.intent_id)
            .into_iter()
            .filter(|receipt| receipt.state == DfsReplicaReceiptState::Durable)
            .map(|receipt| receipt.replica_node_id.clone())
            .collect::<BTreeSet<_>>();
        if !receipts.iter().any(|receipt| {
            !cached.contains(&receipt.replica_node_id)
                || (object.current_locations.contains(&receipt.replica_node_id)
                    && self
                        .coordinator
                        .repair_job_for_target(&record.intent_id, &receipt.replica_node_id)
                        .is_some())
        }) {
            return Ok(Some(false));
        }
        if scheduling.iter().any(|row| {
            row.intent_id != record.intent_id
                || row.object_id != record.object_id
                || row.version != record.target_version
                || Some(row.content_hash.as_str()) != record.content_hash.as_deref()
                || !record
                    .desired_replica_node_ids
                    .contains(&row.missing_replica_node_id)
        }) {
            return Err(DfsError::Metadata {
                message: "current repair receipt projection differs from its queued identity"
                    .into(),
            });
        }
        // Join the existing carrier DATA while its logical target is still
        // present. Settling a replica cannot erase UNKNOWN or named in-flight
        // custody; the normal queue projection below retains those records.
        self.restore_repair_attempt_records_merging(scheduling);
        for receipt in receipts {
            self.coordinator.restore_receipt_location_settlement(
                &record.intent_id,
                &receipt.replica_node_id,
                !object.current_locations.contains(&receipt.replica_node_id),
            );
            self.coordinator
                .restore_durable_receipt_deferring_repair_rebuild(
                    &record.intent_id,
                    receipt.replica_node_id,
                )?;
        }
        Ok(Some(true))
    }

    fn restore_pending_writes_for_repair_attempt_records(
        &mut self,
        records: &[DfsRepairAttemptRecord],
    ) -> Result<Vec<DfsRepairAttemptRecord>, DfsError> {
        self.restore_pending_writes_with_source_read_mode(records, false)?
            .ok_or_else(|| DfsError::Metadata {
                message: "blocking repair source restoration unexpectedly deferred".into(),
            })
    }

    #[tracing::instrument(target = "sinomo.perf", level = "trace", name = "dfs.repair.restore",
        skip_all, fields(stage = "dfs.repair.restore", node = %self.node_id,
            attempt_records = records.len(), distinct_intents = tracing::field::Empty,
            checked_intents = tracing::field::Empty, candidate_records = tracing::field::Empty,
            restored_records = tracing::field::Empty, outcome = "error"))]
    fn restore_pending_writes_with_source_read_mode(
        &mut self,
        records: &[DfsRepairAttemptRecord],
        yield_on_source_fence: bool,
    ) -> Result<Option<Vec<DfsRepairAttemptRecord>>, DfsError> {
        let span = tracing::Span::current();
        let mut distinct_intents = 0u64;
        let mut checked_intents = 0u64;
        let mut candidate_records = 0u64;
        let mut restored_records = 0u64;
        let result = (|| {
            let mut intents = BTreeMap::<String, Vec<DfsRepairAttemptRecord>>::new();
            for record in records {
                intents
                    .entry(record.intent_id.clone())
                    .or_default()
                    .push(record.clone());
            }
            distinct_intents = intents.len() as u64;
            let mut pending_records = Vec::with_capacity(intents.len());
            let mut replaced_owners = BTreeSet::new();
            let mut compacted_identities = BTreeSet::new();
            let mut orphaned_custody = BTreeSet::new();
            for queued in intents.values() {
                checked_intents += 1;
                if let Some((record, replaces_compacted_identity)) =
                    self.stored_repair_record_for_queue(queued, &mut orphaned_custody)?
                {
                    if replaces_compacted_identity {
                        replaced_owners.insert(record.intent_id.clone());
                        compacted_identities.insert(queued[0].intent_id.clone());
                    }
                    pending_records.push(record);
                }
            }
            // Withdraw obsolete full executors only after the exact current
            // terminal receipt and stored replacement owner were checked.
            // Their unresolved transfer DATA remains in the existing queue.
            self.coordinator
                .retire_committed_repairs(&compacted_identities);
            for id in &compacted_identities {
                self.writes.pending_writes.remove(id);
            }
            // Only derived execution is withdrawn. Existing retirement retains
            // uncertain request/delivery evidence and its real transfer owner.
            self.retire_committed_repairs(&orphaned_custody);
            candidate_records = pending_records.len() as u64;
            // Use the same deferred restoration primitives as startup hydration.
            // Rebuilding the complete repair list after each queued intent and
            // receipt made every bounded retry pass quadratic in its backlog.
            let mut restored = false;
            let mut source_read_pending = false;
            let restore_result = (|| {
                for record in &pending_records {
                    if record.publication.is_current()
                        && self.coordinator.intent(&record.intent_id).is_none()
                    {
                        self.coordinator.restore_intent_deferring_repair_rebuild(
                            crate::intent::distributed_intent_from_record(record),
                        )?;
                        restored = true;
                        self.restore_metadata_successor_head(record)?;
                        self.restore_backend_receipt_pages_for_intent(record, false)?;
                    } else {
                        match self.refresh_current_repair_receipt_projection(
                            record,
                            intents
                                .get(&record.intent_id)
                                .map(Vec::as_slice)
                                .unwrap_or(&[]),
                            yield_on_source_fence,
                        )? {
                            Some(changed) => restored |= changed,
                            None => {
                                source_read_pending = true;
                                break;
                            }
                        }
                    }
                }
                Ok::<_, DfsError>(())
            })();
            // Even a partial restore error must leave the coordinator's derived
            // repair list consistent. Preserve its existing inactive-intent set.
            if restored {
                self.coordinator.rebuild_repair_jobs();
            }
            restore_result?;
            if source_read_pending {
                // No page cursor or transfer identity advanced. Keep restored
                // DATA in this same lane and retry the unchanged durable page.
                return Ok(None);
            }
            let mut scheduling = records.to_vec();
            for row in &mut scheduling {
                if orphaned_custody.contains(&row.intent_id) {
                    // Preserve kind, request, completion, attempts and delivery.
                    // The existing observed queue checkpoint performs the CAS.
                    row.next_retry_unix_nanos = u64::MAX;
                }
            }
            for record in &pending_records {
                if !record.publication.is_current()
                    || record.status != DfsWriteIntentStatus::Committed
                    || !scheduling.iter().any(|row| {
                        row.intent_id == record.intent_id
                            && row.transfer_kind
                                == Some(sinomo_api::dfs::maintenance::DfsRepairTransferKind::Repair)
                            && row.in_flight
                            && row.transfer_request_id.is_some()
                            && row.completed_transfer_request_id.is_none()
                    })
                {
                    continue;
                }
                let expected_intent = distributed_intent_from_record(record);
                if self.coordinator.intent(&record.intent_id) != Some(&expected_intent) {
                    continue;
                }
                crate::file_history::validate_intent_publication(record)?;
                record.policy.validate()?;
                let object = self.current_read_object_metadata(&record.object_id)?;
                if object.file_system_id != record.file_system_id
                    || object.object_id != record.object_id
                    || object.content_hash != record.content_hash
                {
                    continue;
                }
                let Some(shard) = self.local_metadata_owner_shard_hint_for_object(&object)? else {
                    continue;
                };
                if shard.file_system_id != record.file_system_id {
                    continue;
                }
                let backend = self
                    .metadata_backend
                    .as_ref()
                    .expect("stored repair backend");
                let Some(version) =
                    backend.get_version(&record.object_id, &record.target_version)?
                else {
                    continue;
                };
                let Some(size) = version.size_bytes else {
                    continue;
                };
                if version.file_system_id != record.file_system_id
                    || version.object_id != record.object_id
                    || version.version != record.target_version
                    || version.content_hash != record.content_hash
                    || version.content_durability.as_deref()
                        != Some(crate::content_durability::from_intent(&expected_intent).as_ref())
                {
                    continue;
                }
                if object.content_size_bytes != version.size_bytes {
                    continue;
                }
                if object.version != record.target_version {
                    let Some(current) = backend.get_version(&record.object_id, &object.version)?
                    else {
                        continue;
                    };
                    if current.version != object.version
                        || !crate::content_durability::same_content_lineage(&current, &version)
                        || current.content_durability != version.content_durability
                    {
                        continue;
                    }
                }
                let receipts =
                    backend.receipts_for_version(&record.object_id, &record.target_version)?;
                let received = |node: &NodeId| {
                    receipts.iter().any(|receipt| {
                        crate::content_durability::receipt_matches_target_version(
                            receipt, &version, node,
                        )
                    })
                };
                let fully_satisfied = record
                    .desired_replica_node_ids
                    .iter()
                    .collect::<BTreeSet<_>>()
                    .len()
                    >= record.policy.desired_replicas
                    && record
                        .desired_replica_node_ids
                        .iter()
                        .all(|node| received(node))
                    && !self
                        .coordinator
                        .repair_jobs()
                        .iter()
                        .any(|job| job.intent_id == record.intent_id);
                for row in scheduling
                    .iter_mut()
                    .filter(|row| row.intent_id == record.intent_id)
                {
                    if row.object_id != record.object_id
                        || row.version != record.target_version
                        || Some(row.content_hash.as_str()) != record.content_hash.as_deref()
                        || !record
                            .desired_replica_node_ids
                            .contains(&row.missing_replica_node_id)
                    {
                        return Err(DfsError::Metadata {
                            message: "Current repair scheduling differs from its actual full owner"
                                .into(),
                        });
                    }
                    if row.transfer_kind
                        != Some(sinomo_api::dfs::maintenance::DfsRepairTransferKind::Repair)
                        || !row.in_flight
                        || row.transfer_request_id.is_none()
                        || row.completed_transfer_request_id.is_some()
                    {
                        continue;
                    }
                    let park = fully_satisfied && row.next_retry_unix_nanos != u64::MAX;
                    let rearm = row.next_retry_unix_nanos == u64::MAX
                        && row.attempts > 0
                        && row.last_attempt_unix_nanos > 0
                        && !received(&row.missing_replica_node_id)
                        && self
                            .coordinator
                            .repair_job_for_target(&row.intent_id, &row.missing_replica_node_id)
                            .is_some();
                    // Ordinary active retries retain their existing restore.
                    // Only a real deadline change needs this additional guard.
                    if !park && !rearm {
                        continue;
                    }
                    if let Some(prior) = self
                        .repair_attempt_records
                        .get(&(row.intent_id.clone(), row.missing_replica_node_id.clone()))
                    {
                        let mut same_carrier = prior.clone();
                        same_carrier.next_retry_unix_nanos = row.next_retry_unix_nanos;
                        if &same_carrier != row {
                            tracing::warn!(
                                target: "sinomo.dfs.repair",
                                intent = %record.intent_id,
                                file_system = %record.file_system_id,
                                object = %row.object_id,
                                version = %row.version,
                                replica = %row.missing_replica_node_id,
                                fully_satisfied,
                                immutable_tuple_equal = prior.intent_id == row.intent_id
                                    && prior.object_id == row.object_id
                                    && prior.version == row.version
                                    && prior.content_hash == row.content_hash
                                    && prior.missing_replica_node_id == row.missing_replica_node_id,
                                kind_equal = prior.transfer_kind == row.transfer_kind,
                                request_equal = prior.transfer_request_id == row.transfer_request_id,
                                completed_equal = prior.completed_transfer_request_id == row.completed_transfer_request_id,
                                delivery_equal = prior.transfer_delivery_state == row.transfer_delivery_state,
                                in_flight_equal = prior.in_flight == row.in_flight,
                                attempts_equal = prior.attempts == row.attempts,
                                last_attempt_equal = prior.last_attempt_unix_nanos == row.last_attempt_unix_nanos,
                                error_equal = prior.last_error == row.last_error,
                                input_deadline_is_max = row.next_retry_unix_nanos == u64::MAX,
                                prior_deadline_is_max = prior.next_retry_unix_nanos == u64::MAX,
                                input_kind = ?row.transfer_kind,
                                prior_kind = ?prior.transfer_kind,
                                input_request_id = ?row.transfer_request_id.as_deref().map(|value| value.chars().take(192).collect::<String>()),
                                prior_request_id = ?prior.transfer_request_id.as_deref().map(|value| value.chars().take(192).collect::<String>()),
                                input_completed_id = ?row.completed_transfer_request_id.as_deref().map(|value| value.chars().take(192).collect::<String>()),
                                prior_completed_id = ?prior.completed_transfer_request_id.as_deref().map(|value| value.chars().take(192).collect::<String>()),
                                input_delivery = ?row.transfer_delivery_state,
                                prior_delivery = ?prior.transfer_delivery_state,
                                input_in_flight = row.in_flight,
                                prior_in_flight = prior.in_flight,
                                input_attempts = row.attempts,
                                prior_attempts = prior.attempts,
                                input_last_attempt = row.last_attempt_unix_nanos,
                                prior_last_attempt = prior.last_attempt_unix_nanos,
                                input_deadline = row.next_retry_unix_nanos,
                                prior_deadline = prior.next_retry_unix_nanos,
                                input_error = ?row.last_error.as_deref().map(|value| value.chars().take(192).collect::<String>()),
                                prior_error = ?prior.last_error.as_deref().map(|value| value.chars().take(192).collect::<String>()),
                                "Current repair scheduling carrier comparison refused"
                            );
                            return Err(DfsError::Metadata {
                                message: "Current repair scheduling cannot replace changed live carrier custody".into(),
                            });
                        }
                    }
                    // Receipts retire missing-N execution, never this carrier.
                    // A real later same-target receipt withdrawal re-arms the
                    // SAME ID at its normal original producer horizon.
                    if park {
                        row.next_retry_unix_nanos = u64::MAX;
                    } else if rearm {
                        row.next_retry_unix_nanos = row.last_attempt_unix_nanos.saturating_add(
                            repair_retry_delay_nanos_for_ticks(
                                repair_retry_delay_ticks(row.attempts)
                                    .max(repair_transfer_inflight_timeout_ticks(size)),
                            ),
                        );
                    }
                }
            }
            let mut retained_restored = false;
            let restore_result = (|| {
                for record in pending_records {
                    retained_restored |= !record.publication.is_current();
                    if record.publication.is_current()
                        && let Some(pending) = self.writes.pending_writes.get(&record.intent_id)
                    {
                        if pending.object_id != record.object_id
                            || pending.version != record.target_version
                            || Some(pending.content_hash.as_str()) != record.content_hash.as_deref()
                            || self.coordinator.intent(&record.intent_id)
                                != Some(&distributed_intent_from_record(&record))
                        {
                            return Err(DfsError::Metadata {
                                message: "Current repair restore differs from its live owner"
                                    .into(),
                            });
                        }
                    } else {
                        self.restore_pending_write_from_record(record)?;
                    }
                    restored_records += 1;
                }
                Ok::<_, DfsError>(())
            })();
            // Every retained restore defers derived jobs, including the SAME
            // full owner after restart. Scheduling absence is not settlement.
            // Complete a partial restore before propagating its error too.
            if retained_restored {
                self.coordinator.rebuild_repair_jobs();
            }
            restore_result?;
            if !replaced_owners.is_empty() {
                for intent_id in replaced_owners {
                    // Persist the SAME stored owner's actual missing-N jobs in
                    // the existing observed queue checkpoint before old keys
                    // can retire. This captures scheduling DATA, not a mutation.
                    self.coordinator.capture_active_repair_jobs(&intent_id);
                }
            }
            Ok(Some(scheduling))
        })();
        span.record("distinct_intents", distinct_intents);
        span.record("checked_intents", checked_intents);
        span.record("candidate_records", candidate_records);
        span.record("restored_records", restored_records);
        span.record(
            "outcome",
            match &result {
                Ok(Some(_)) => "ok",
                Ok(None) => "pending",
                Err(_) => "error",
            },
        );
        result
    }

    pub(crate) fn observe_repair_maintenance_events(
        &mut self,
        events: &[NetworkedDfsServiceEvent],
    ) {
        for event in events {
            match event {
                NetworkedDfsServiceEvent::ReplicaRepaired {
                    intent_id,
                    replica_node_id,
                    ..
                } => {
                    self.repair_attempt_records
                        .remove(&(intent_id.clone(), replica_node_id.clone()));
                }
                NetworkedDfsServiceEvent::RepairRejected {
                    intent_id,
                    replica_node_id,
                    reason,
                    ..
                } => {
                    let key = (intent_id.clone(), replica_node_id.clone());
                    if let Some(record) = self.repair_attempt_records.get_mut(&key) {
                        if record.completed_transfer_request_id.is_some() {
                            record.in_flight = false;
                        }
                        record.last_error = Some(reason.clone());
                    } else if let Some(pending) = self.writes.pending_writes.get(intent_id) {
                        let state = pending.repair_retries.get(replica_node_id);
                        let attempts = state.map(|state| state.attempts).unwrap_or(1).max(1);
                        let next_retry_unix_nanos = if state
                            .is_some_and(repair_retry_state_is_suppressed)
                            && repair_rejection_indicates_lost_authority(reason)
                        {
                            u64::MAX
                        } else {
                            let remaining_ticks = state
                                .map(|state| {
                                    state.next_retry_tick.saturating_sub(self.repair_retry_tick)
                                })
                                .unwrap_or(0);
                            dfs_now_unix_nanos()
                                .saturating_add(repair_retry_delay_nanos_for_ticks(remaining_ticks))
                        };
                        self.repair_attempt_records.insert(
                            key,
                            DfsRepairAttemptRecord {
                                transfer_delivery_state: state
                                    .and_then(|state| state.transfer_delivery_state),
                                completed_transfer_request_id: state
                                    .and_then(|state| state.completed_transfer_request_id.clone()),
                                transfer_kind: Some(
                                    sinomo_api::dfs::maintenance::DfsRepairTransferKind::Repair,
                                ),
                                intent_id: intent_id.clone(),
                                object_id: pending.object_id.clone(),
                                version: pending.version.clone(),
                                content_hash: pending.content_hash.clone(),
                                missing_replica_node_id: replica_node_id.clone(),
                                attempts,
                                in_flight: false,
                                last_attempt_unix_nanos: dfs_now_unix_nanos(),
                                next_retry_unix_nanos,
                                transfer_request_id: pending
                                    .repair_inflight_request_ids
                                    .get(replica_node_id)
                                    .cloned(),
                                last_error: Some(reason.clone()),
                            },
                        );
                    }
                }
                _ => {}
            }
        }
    }

    pub(crate) fn pending_repair_is_locally_authoritative(
        &mut self,
        pending: &PendingNetworkedWrite,
    ) -> Result<bool, DfsError> {
        let object = self.current_read_object_metadata(&pending.object_id)?;
        self.is_local_metadata_primary_for_object(&object)
    }

    pub(crate) fn pending_repair_targets_current_content(
        &mut self,
        job: &DistributedRepairJob,
        pending: &PendingNetworkedWrite,
    ) -> Result<bool, DfsError> {
        if job.object_id != pending.object_id || job.version != pending.version {
            return Ok(false);
        }
        if self
            .coordinator
            .intent(&job.intent_id)
            .is_some_and(|intent| !intent.publication.is_current())
        {
            return self.retained_file_content_is_live(
                &pending.object_id,
                &pending.version,
                &pending.content_hash,
            );
        }
        let object = match self.current_read_object_metadata(&pending.object_id) {
            Ok(object) => object,
            Err(DfsError::UnknownObject { .. }) => return Ok(true),
            Err(error) => return Err(error),
        };
        if object.content_hash.as_deref() == Some(pending.content_hash.as_str()) {
            return Ok(true);
        }
        if object.version == pending.version {
            return Ok(false);
        }
        if self.retained_file_content_is_live(
            &pending.object_id,
            &pending.version,
            &pending.content_hash,
        )? {
            return Ok(true);
        }
        let base_version = self
            .coordinator
            .intent(&job.intent_id)
            .and_then(|intent| intent.base_version.as_ref());
        Ok(base_version.is_some_and(|base_version| object.version == *base_version))
    }

    pub(crate) fn record_repair_inflight(
        &mut self,
        intent_id: &str,
        replica_node_id: &NodeId,
        request_id: String,
    ) {
        let tick = self.repair_retry_tick;
        if let Some(pending) = self.writes.pending_writes.get_mut(intent_id) {
            let state = pending
                .repair_retries
                .entry(replica_node_id.clone())
                .or_default();
            if repair_retry_state_is_suppressed(state) {
                state.inflight = false;
                return;
            }
            state.attempts = state.attempts.saturating_add(1);
            state.inflight = true;
            state.transfer_delivery_state =
                Some(sinomo_core::network::NetworkDeliveryState::PossiblyApplied);
            state.completed_transfer_request_id = None;
            state.next_retry_tick = tick.saturating_add(
                repair_retry_delay_ticks(state.attempts)
                    .max(repair_transfer_inflight_timeout_ticks(pending.size_bytes)),
            );
            // Record the actual started attempt in the same carrier row that
            // begin_replica_transfer_attempt claimed. Export and replay must
            // retain one counter and timestamp history for that exact request.
            if let Some(record) = self
                .repair_attempt_records
                .get_mut(&(intent_id.to_string(), replica_node_id.clone()))
                && record.object_id == pending.object_id
                && record.version == pending.version
                && record.content_hash == pending.content_hash
                && record.missing_replica_node_id == *replica_node_id
                && record.transfer_kind
                    == Some(sinomo_api::dfs::maintenance::DfsRepairTransferKind::Repair)
                && record.transfer_request_id.as_deref() == Some(request_id.as_str())
                && record.in_flight
                && record.completed_transfer_request_id.is_none()
            {
                let now = dfs_now_unix_nanos();
                record.attempts = state.attempts;
                record.last_attempt_unix_nanos = now;
                record.transfer_delivery_state = state.transfer_delivery_state;
                if record.next_retry_unix_nanos != u64::MAX {
                    record.next_retry_unix_nanos =
                        now.saturating_add(repair_retry_delay_nanos_for_ticks(
                            state.next_retry_tick.saturating_sub(tick),
                        ));
                }
            }
            pending
                .repair_inflight_request_ids
                .insert(replica_node_id.clone(), request_id);
        }
    }

    fn record_repair_fetch_inflight(&mut self, intent_id: &str, replica_node_id: &NodeId) {
        let tick = self.repair_retry_tick;
        if let Some(pending) = self.writes.pending_writes.get_mut(intent_id) {
            let state = pending
                .repair_retries
                .entry(replica_node_id.clone())
                .or_default();
            if repair_retry_state_is_suppressed(state) {
                state.inflight = false;
                return;
            }
            state.attempts = state.attempts.saturating_add(1);
            state.inflight = true;
            state.next_retry_tick = tick.saturating_add(fetch_inflight_timeout_ticks());
            // Fetch progress cannot settle a separate repair request.
        }
    }

    pub(crate) fn record_repair_retry_scheduled(
        &mut self,
        intent_id: &str,
        replica_node_id: &NodeId,
    ) {
        self.record_repair_retry_scheduled_with_error(intent_id, replica_node_id, None);
    }

    pub(crate) fn record_repair_retry_scheduled_with_error(
        &mut self,
        intent_id: &str,
        replica_node_id: &NodeId,
        last_error: Option<String>,
    ) {
        let tick = self.repair_retry_tick;
        let previous_error = self
            .repair_attempt_records
            .get(&(intent_id.to_string(), replica_node_id.clone()))
            .and_then(|record| record.last_error.clone());
        let mut record_update = None;
        if let Some(pending) = self.writes.pending_writes.get_mut(intent_id) {
            let state = pending
                .repair_retries
                .entry(replica_node_id.clone())
                .or_default();
            if !schedule_repair_retry_state(state, tick, last_error.as_deref()) {
                return;
            }
            let remaining_ticks = state.next_retry_tick.saturating_sub(tick);
            record_update = Some(DfsRepairAttemptRecord {
                transfer_delivery_state: state.transfer_delivery_state,
                completed_transfer_request_id: state.completed_transfer_request_id.clone(),
                transfer_kind: Some(sinomo_api::dfs::maintenance::DfsRepairTransferKind::Repair),
                intent_id: intent_id.to_string(),
                object_id: pending.object_id.clone(),
                version: pending.version.clone(),
                content_hash: pending.content_hash.clone(),
                missing_replica_node_id: replica_node_id.clone(),
                attempts: state.attempts,
                in_flight: false,
                last_attempt_unix_nanos: dfs_now_unix_nanos(),
                next_retry_unix_nanos: dfs_now_unix_nanos()
                    .saturating_add(repair_retry_delay_nanos_for_ticks(remaining_ticks)),
                transfer_request_id: pending
                    .repair_inflight_request_ids
                    .get(replica_node_id)
                    .cloned(),
                last_error: last_error.or(previous_error),
            });
        }
        if let Some(record) = record_update {
            self.repair_attempt_records.insert(
                (
                    record.intent_id.clone(),
                    record.missing_replica_node_id.clone(),
                ),
                record,
            );
        }
    }

    pub(crate) fn suppress_repair_retries_for_lost_authority(
        &mut self,
        intent_id: &str,
        response_replica_node_id: Option<&NodeId>,
        drop_committed_pending_write: bool,
    ) {
        let remove_committed_pending_write = drop_committed_pending_write
            && self
                .coordinator
                .intent(intent_id)
                .is_some_and(|intent| matches!(intent.state, DfsWriteIntentState::Committed));
        let mut replica_node_ids = self
            .coordinator
            .repair_jobs()
            .iter()
            .filter(|job| job.intent_id == intent_id)
            .map(|job| job.missing_replica_node_id.clone())
            .collect::<BTreeSet<_>>();
        if let Some(replica_node_id) = response_replica_node_id {
            replica_node_ids.insert(replica_node_id.clone());
        }
        if let Some(pending) = self.writes.pending_writes.get(intent_id) {
            replica_node_ids.extend(pending.repair_retries.keys().cloned());
        }
        if let Some(pending) = self.writes.pending_writes.get_mut(intent_id) {
            for replica_node_id in replica_node_ids {
                let state = pending
                    .repair_retries
                    .entry(replica_node_id.clone())
                    .or_default();
                state.inflight = false;
                state.next_retry_tick = DFS_REPAIR_RETRY_SUPPRESSED_TICK;
            }
            pending.fetch_inflight_from.clear();
            pending.fetch_inflight_request_ids.clear();
            pending.fetch_inflight_started_ticks.clear();
            pending.fetch_failed_from.clear();
        }
        if remove_committed_pending_write
            && self
                .writes
                .pending_writes
                .get(intent_id)
                .is_some_and(|pending| {
                    pending.repair_inflight_request_ids.is_empty()
                        && pending.stage_inflight_request_ids.is_empty()
                })
        {
            self.writes.pending_writes.remove(intent_id);
        }
    }

    pub(crate) fn record_repair_metadata_settle(
        &mut self,
        intent_id: &str,
        replica_node_id: &NodeId,
    ) {
        if let Some(pending) = self.writes.pending_writes.get_mut(intent_id) {
            // Receipt persistence, coordinator convergence, and the visible
            // location update all completed before this point. Keeping a
            // timeout-only retry record would retain the whole pending blob
            // after N without protecting an unfinished metadata operation.
            pending.repair_retries.remove(replica_node_id);
            pending.repair_inflight_request_ids.remove(replica_node_id);
        }
        self.repair_attempt_records
            .remove(&(intent_id.to_string(), replica_node_id.clone()));
    }

    pub(crate) fn request_repair_blob_from_alternate(
        &mut self,
        job: &DistributedRepairJob,
        pending: &PendingNetworkedWrite,
    ) -> Result<DfsRepairBlobRequestOutcome, DfsError> {
        let mut skipped_failed_source = false;
        let mut last_error = None;
        let authority_object = self.current_read_object_metadata(&pending.object_id).ok();
        let authority_shard = self.command_authority_shard_for_object(authority_object.as_ref())?;
        for source_node_id in self.repair_blob_sources(job, pending)? {
            if pending.fetch_inflight_from.contains(&source_node_id) {
                continue;
            }
            if pending.fetch_failed_from.contains(&source_node_id) {
                skipped_failed_source = true;
                continue;
            }
            let request_id = pending
                .fetch_inflight_request_ids
                .get(&source_node_id)
                .cloned()
                .unwrap_or_else(|| self.next_request_id());
            let send_outcome = self.send_best_effort_message_if_routable_outcome(
                &source_node_id,
                DfsNetworkMessage::FetchBlob(DfsFetchBlobCommand {
                    request_id: request_id.clone(),
                    intent_id: job.intent_id.clone(),
                    object_id: pending.object_id.clone(),
                    version: pending.version.clone(),
                    content_hash: pending.content_hash.clone(),
                    authority_shard: authority_shard.clone(),
                }),
            )?;
            if send_outcome.sent {
                tracing::debug!(
                    target: "sinomo.dfs.repair",
                    local_node_id = %self.node_id,
                    source_node_id = %source_node_id,
                    missing_replica_node_id = %job.missing_replica_node_id,
                    intent_id = %job.intent_id,
                    object_id = %pending.object_id,
                    version = %pending.version,
                    content_hash = %pending.content_hash,
                    request_id = %request_id,
                    "requested DFS repair blob from alternate source"
                );
                self.record_fetch_inflight(&job.intent_id, source_node_id, request_id);
                self.record_repair_fetch_inflight(&job.intent_id, &job.missing_replica_node_id);
                return Ok(DfsRepairBlobRequestOutcome {
                    requested: true,
                    last_error: None,
                });
            }
            if send_outcome.last_error.is_some() {
                last_error = send_outcome.last_error;
            }
        }
        if skipped_failed_source
            && let Some(pending) = self.writes.pending_writes.get_mut(&job.intent_id)
        {
            pending.fetch_failed_from.clear();
        }
        tracing::debug!(
            target: "sinomo.dfs.repair",
            local_node_id = %self.node_id,
            missing_replica_node_id = %job.missing_replica_node_id,
            intent_id = %job.intent_id,
            object_id = %pending.object_id,
            version = %pending.version,
            content_hash = %pending.content_hash,
            skipped_failed_source,
            "no routable DFS repair blob alternate source"
        );
        Ok(DfsRepairBlobRequestOutcome {
            requested: false,
            last_error,
        })
    }

    pub(crate) fn repair_blob_sources(
        &self,
        job: &DistributedRepairJob,
        pending: &PendingNetworkedWrite,
    ) -> Result<Vec<NodeId>, DfsError> {
        let mut candidates = BTreeSet::new();
        for receipt in self.coordinator.receipts_for_intent(&job.intent_id) {
            if receipt.replica_node_id != self.node_id
                && receipt.replica_node_id != job.missing_replica_node_id
            {
                candidates.insert(receipt.replica_node_id.clone());
            }
        }
        match self.current_read_object_metadata(&pending.object_id) {
            Ok(object) => {
                for node_id in &object.current_locations {
                    if node_id != &self.node_id && node_id != &job.missing_replica_node_id {
                        candidates.insert(node_id.clone());
                    }
                }
            }
            Err(DfsError::UnknownObject { .. }) => {}
            Err(error) => return Err(error),
        }
        Ok(self.ranked_transfer_sources(candidates))
    }
}

pub(crate) fn repair_transfer_custody_unresolved(record: &DfsRepairAttemptRecord) -> bool {
    record.transfer_kind.is_none()
        || record.in_flight
        || record.transfer_request_id.is_some()
        || (record.transfer_delivery_state
            != Some(sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent)
            && record.completed_transfer_request_id.is_none())
}

/// Negative custody DATA only; this predicate grants no request or dispatch.
pub(crate) fn repair_record_has_unidentified_producer(record: &DfsRepairAttemptRecord) -> bool {
    // `in_flight` also retains read-only Fetch backoff. That row still owns
    // scheduling DATA, but typed NotSent is not an unidentified mutation.
    // Unknown kind/delivery and PossiblyApplied remain parked without an ID.
    record.transfer_kind.is_none()
        || (record.transfer_request_id.is_none()
            && record.completed_transfer_request_id.is_none()
            && record.transfer_delivery_state
                != Some(sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent))
}

pub(crate) fn join_transfer_delivery(
    previous: Option<sinomo_core::network::NetworkDeliveryState>,
    incoming: Option<sinomo_core::network::NetworkDeliveryState>,
) -> Option<sinomo_core::network::NetworkDeliveryState> {
    use sinomo_core::network::NetworkDeliveryState;
    match (previous, incoming) {
        (Some(NetworkDeliveryState::PossiblyApplied), _)
        | (_, Some(NetworkDeliveryState::PossiblyApplied)) => {
            Some(NetworkDeliveryState::PossiblyApplied)
        }
        (
            Some(NetworkDeliveryState::DefinitelyNotSent),
            Some(NetworkDeliveryState::DefinitelyNotSent),
        ) => Some(NetworkDeliveryState::DefinitelyNotSent),
        _ => None,
    }
}

/// Compare already-read DATA only. This never constructs or publishes an owner.
fn matching_stored_compacted_repair_owner(
    version: &DfsObjectVersionRecord,
    expected: &DfsWriteIntentRecord,
    actual: Option<DfsWriteIntentRecord>,
    records: &[DfsRepairAttemptRecord],
) -> Result<DfsWriteIntentRecord, DfsError> {
    let unknown = || DfsError::Distributed {
        message: format!(
            "unknown pending write intent: {}",
            records
                .first()
                .map_or("", |record| record.intent_id.as_str())
        ),
    };
    let first = records.first().ok_or_else(unknown)?;
    let actual = actual.ok_or_else(unknown)?;
    if !version.has_retention_grant()
        || records.iter().any(|queued| {
            queued.intent_id != first.intent_id
                || queued.object_id != version.object_id
                || queued.version != version.version
                || version.content_hash.as_deref() != Some(queued.content_hash.as_str())
        })
        || actual.intent_id == first.intent_id
        || actual.intent_id != expected.intent_id
        || actual.status != DfsWriteIntentStatus::Committed
        || actual.coordinator_node_id != expected.coordinator_node_id
        || actual.owner_epoch != expected.owner_epoch
        || !crate::file_history::retained_repair_plan_matches(&actual, expected)
        || records.iter().any(|queued| {
            !actual
                .desired_replica_node_ids
                .contains(&queued.missing_replica_node_id)
        })
    {
        return Err(unknown());
    }
    Ok(actual)
}

#[cfg(test)]
mod compacted_repair_owner_data_tests {
    use super::*;
    #[test]
    fn actual_captured_gc_owner_data_rejects_identity_and_plan_mismatches() {
        // These are exact captured native journal/queue VALUES. No metadata
        // backend, coordinator, owner, guard, permit or healthy state is installed.
        let data: serde_json::Value = serde_json::from_str(include_str!(
            "test_support/fixtures/native-retained-repair-gc-r219-current-unknown.json"
        ))
        .unwrap();
        let version: DfsObjectVersionRecord =
            serde_json::from_value(data["version"].clone()).unwrap();
        let expected: DfsWriteIntentRecord = serde_json::from_value(data["owner"].clone()).unwrap();
        let records: Vec<DfsRepairAttemptRecord> =
            serde_json::from_value(data["queue"].clone()).unwrap();
        assert_eq!(
            matching_stored_compacted_repair_owner(
                &version,
                &expected,
                Some(expected.clone()),
                &records
            )
            .unwrap(),
            expected
        );
        let reject = |actual: Option<DfsWriteIntentRecord>, queued: &[DfsRepairAttemptRecord]| {
            let error = matching_stored_compacted_repair_owner(&version, &expected, actual, queued)
                .unwrap_err();
            assert!(
                matches!(error, DfsError::Distributed { message } if message == format!("unknown pending write intent: {}", queued[0].intent_id))
            );
        };
        reject(None, &records);
        let mut changed = expected.clone();
        changed.owner_epoch += 1;
        reject(Some(changed), &records);
        let mut changed = expected.clone();
        changed.coordinator_node_id = changed
            .desired_replica_node_ids
            .iter()
            .find(|node| **node != changed.coordinator_node_id)
            .unwrap()
            .clone();
        reject(Some(changed), &records);
        let mut changed = expected.clone();
        changed.content_hash = Some("negative-data-hash".into());
        reject(Some(changed), &records);
        let mut changed = expected.clone();
        changed.target_version = version.object_id.clone();
        reject(Some(changed), &records);
        let mut changed = expected.clone();
        changed.policy.desired_replicas += 1;
        reject(Some(changed), &records);
        let mut changed = records.clone();
        changed[0].content_hash = "negative-data-hash".into();
        reject(Some(expected.clone()), &changed);
        let mut changed = records.clone();
        changed[0].version = version.object_id.clone();
        reject(Some(expected.clone()), &changed);
    }
}

pub(crate) fn enroll_prepared_repair_targets(
    data_dir: &sinomo_core::local::SinomoDataDir,
    current: &BTreeMap<(String, NodeId), DfsRepairAttemptRecord>,
    pending: &BTreeMap<String, PendingNetworkedWrite>,
    births: &[DfsRepairAttemptRecord],
) -> crate::metadata_error::Result<()> {
    if births.is_empty() {
        return Ok(());
    }
    let store = LocalDfsRepairQueueStore::new(default_dfs_repair_attempt_snapshot_path(data_dir));
    enroll_prepared_repair_targets_in_store(data_dir, &store, current, pending, births)
}

pub(crate) fn enroll_prepared_repair_targets_in_store(
    data_dir: &sinomo_core::local::SinomoDataDir,
    store: &LocalDfsRepairQueueStore,
    current: &BTreeMap<(String, NodeId), DfsRepairAttemptRecord>,
    pending: &BTreeMap<String, PendingNetworkedWrite>,
    births: &[DfsRepairAttemptRecord],
) -> crate::metadata_error::Result<()> {
    if births.is_empty() { return Ok(()); }
    let invalid = |message: &str| crate::MetadataBackendError::Serde(message.into());
    let (view, observed) = store
        .view_for_maintenance()
        .map_err(|error| invalid(&error.to_string()))?;
    let mut queued = view.records;
    for birth in births {
        let key = (
            birth.intent_id.clone(),
            birth.missing_replica_node_id.clone(),
        );
        let prior = queued.iter_mut().find(|row| {
            row.intent_id == birth.intent_id
                && row.missing_replica_node_id == birth.missing_replica_node_id
        });
        if let Some(runtime) = current.get(&key) {
            if runtime.object_id != birth.object_id
                || runtime.version != birth.version
                || runtime.content_hash != birth.content_hash
            {
                return Err(invalid(
                    "new repair owner changed prior runtime custody identity",
                ));
            }
            if let Some(prior) = prior {
                if prior.object_id != runtime.object_id
                    || prior.version != runtime.version
                    || prior.content_hash != runtime.content_hash
                {
                    return Err(invalid(
                        "new repair owner differs from prior exact queue custody",
                    ));
                }
                if repair_transfer_custody_unresolved(prior) {
                    if repair_transfer_custody_unresolved(runtime)
                        && (prior.transfer_request_id != runtime.transfer_request_id
                            || prior.transfer_kind != runtime.transfer_kind)
                    {
                        return Err(invalid(
                            "new repair owner has conflicting unresolved custody",
                        ));
                    }
                } else if repair_transfer_custody_unresolved(runtime) {
                    *prior = runtime.clone();
                }
            } else {
                queued.push(runtime.clone());
            }
            continue;
        }
        if let Some(prior) = prior {
            if prior.object_id != birth.object_id
                || prior.version != birth.version
                || prior.content_hash != birth.content_hash
            {
                return Err(invalid(
                    "new repair owner differs from prior exact queue custody",
                ));
            }
            continue;
        }
        if pending.get(&birth.intent_id).is_some_and(|pending| {
            pending.stage_inflight_request_ids.contains_key(&birth.missing_replica_node_id)
                || pending.repair_inflight_request_ids.contains_key(&birth.missing_replica_node_id)
                || [&pending.stage_retries, &pending.repair_retries].iter().any(|states|
                    states.get(&birth.missing_replica_node_id).is_some_and(|state|
                        state.unresolved_custody_without_request
                        || (state.transfer_delivery_state != Some(sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent)
                            && state.completed_transfer_request_id.is_none())))
        }) {
            return Err(invalid("new repair owner retains runtime uncertainty without a queue observation"));
        }
        queued.push(birth.clone());
    }
    #[cfg(any(test, feature = "test-support"))]
    let ids = births
        .iter()
        .map(|row| row.intent_id.clone())
        .collect::<BTreeSet<_>>();
    #[cfg(any(test, feature = "test-support"))]
    if let Some(hook) = crate::runtime_host::daemon_dfs_pre_dispatch_test_hook(data_dir) {
        for id in &ids {
            hook(crate::runtime_host::DaemonDfsPreDispatchTestPoint::BeforeRetainedRepairQueueEnrollment {
                owner_id_digest: *blake3::hash(id.as_bytes()).as_bytes(),
            }, None, None).map_err(|error| invalid(&error.to_string()))?;
        }
    }
    match store
        .reconcile_observed(&observed, &queued)
        .map_err(|error| invalid(&error.to_string()))?
    {
        crate::repair_queue_store::DfsRepairQueueObservedReconcileOutcome::Applied(_) => {}
        crate::repair_queue_store::DfsRepairQueueObservedReconcileOutcome::GenerationChanged {
            ..
        } => {
            return Err(invalid(
                "repair queue changed before actual owner publication",
            ));
        }
    }
    #[cfg(any(test, feature = "test-support"))]
    if let Some(hook) = crate::runtime_host::daemon_dfs_pre_dispatch_test_hook(data_dir) {
        for id in &ids {
            hook(crate::runtime_host::DaemonDfsPreDispatchTestPoint::AfterRetainedRepairQueueEnrollment {
                owner_id_digest: *blake3::hash(id.as_bytes()).as_bytes(),
            }, None, None).map_err(|error| invalid(&error.to_string()))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod imported_executor_admission_tests {
    use super::*;

    #[test]
    fn imported_executor_association_refuses_content_only_service_without_native_backend() {
        let root = sinomo_testdir::TestDir::new("imported-executor-no-native-owner");
        let content = LocalContentStore::new(root.join("content")).unwrap();
        let object = DfsObjectId::from_legacy_named("disabled-import-object").unwrap();
        let version = DfsVersion::from_legacy_named("disabled-import-version").unwrap();
        let bytes = b"content existence does not supply native authority";
        let manifest = content
            .put_object_version(object.as_str(), version.as_str(), bytes)
            .unwrap();
        let local = NodeId::from_legacy_named("disabled-import-local").unwrap();
        let producer = NodeId::from_legacy_named("disabled-import-original").unwrap();
        let assignment = DfsMetadataShardConfig {
            file_system_id: crate::sifs_file_system_id(),
            shard_id: "root".into(),
            primary_node_id: local.clone(),
            replica_node_ids: vec![producer.clone()],
            epoch: 8,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
        };
        let mut service = NetworkedDfsService::new(
            crate::test_runtime::DfsTestRuntime::local_transport(local),
            content,
        );
        assert!(
            service
                .associate_installed_repair_executor_disabled(
                    &assignment,
                    &producer,
                    "unproven-original",
                )
                .is_err()
        );
        assert_eq!(
            service
                .content
                .read_object_version(object.as_str(), version.as_str())
                .unwrap()
                .0,
            manifest
        );
        assert!(!root.join("repair.sobj").exists());
    }
}

#[cfg(test)]
mod received_repair_content_tests {
    use super::*;
    use crate::runtime_host::{DfsDataOwnerLock, DfsRuntimePaths};
    use sinomo_core::DfsObjectPolicy;
    use sinomo_core::kv::InProcessKvStore;
    use sinomo_core::local::SinomoDataDir;
    use sinomo_core::node::{
        DFS_METADATA_PREPARED_OPERATION_V7_CAPABILITY, cluster_capability_policy_key,
        cluster_member_key,
    };

    // Real file-backed original/body inputs; policy and intent VALUES below
    // exercise pure validation, not a native committed authority certificate.
    fn selected_hard_policy_inputs(root: &std::path::Path) -> (
        crate::repair_queue_store::LocalDfsRepairQueueStore,
        DfsOriginalRepairMaterial, DfsObjectMetadata, DfsWriteIntentRecord, DfsFileSystemConfig,
    ) {
        selected_hard_policy_inputs_with_original_policy(root, DfsObjectPolicy::ReplicatedEventual)
    }

    fn selected_hard_policy_inputs_with_original_policy(root: &std::path::Path, original_policy: DfsObjectPolicy) -> (
        crate::repair_queue_store::LocalDfsRepairQueueStore,
        DfsOriginalRepairMaterial, DfsObjectMetadata, DfsWriteIntentRecord, DfsFileSystemConfig,
    ) {
        let source_root = root.join("source");
        let (queue, _layout) = crate::repair_queue_store::LocalDfsRepairQueueStore::new_selected_original_fixture(&source_root).unwrap();
        let incarnation = queue.custody_incarnation().unwrap();
        let content = LocalContentStore::new(source_root.join("content")).unwrap();
        let producer = NodeId::from_legacy_named("custody-producer").unwrap();
        let target = NodeId::from_legacy_named("custody-receiver").unwrap();
        let object_id = DfsObjectId::from_legacy_named("custody-object").unwrap();
        let version = DfsVersion::from_legacy_named("custody-version").unwrap();
        let manifest = content.put_object_version(object_id.as_str(), version.as_str(), b"complete original attachment").unwrap();
        let object = DfsObjectMetadata {
            object_id: object_id.clone(), file_system_id: crate::sifs_file_system_id(),
            path: "/hard-policy/object.bin".into(), kind: DfsObjectKind::File, tombstone_provenance: None,
            owner_node_id: producer.clone(), current_locations: vec![producer.clone()],
            content_hash: Some(manifest.content_hash.clone()), content_size_bytes: Some(manifest.size_bytes),
            link_target_object_id: None, mode: 0o644, uid: 0, gid: 0,
            accessed_at_unix_nanos: 0, modified_at_unix_nanos: 0, created_at_unix_nanos: 0, changed_at_unix_nanos: 0,
            version: version.clone(), policy: original_policy,
            created_by_event_id: None, last_modified_by_event_id: None,
        };
        // Fix the complete head BEFORE the first content root/enrollment.
        // No existing original body/digest or uncertain effect is rewritten.
        let message = DfsRoutedMessage { source_node_id: producer.clone(), target_node_id: target.clone(), frame_id: 11, message_id: 12,
            message: DfsNetworkMessage::RepairReplica(DfsRepairReplicaCommand {
                request_id: "original-request".into(), background_repair: true, intent_id: "original-intent".into(),
                file_system_id: object.file_system_id.clone(), object_id: object_id.clone(), expected_base_version: None,
                version: version.clone(), content_hash: manifest.content_hash.clone(), bytes: Vec::new(), object_metadata: Some(object.clone()),
                authority_shard: Some(DfsMetadataShardConfig { file_system_id: object.file_system_id.clone(), shard_id: "root".into(), primary_node_id: producer,
                    replica_node_ids: vec![target], epoch: 7, assignment: DfsMetadataShardAssignment::Root, default_policy: DfsObjectPolicy::ReplicatedEventual }),
            }) };
        let descriptor = DfsRpcAttachmentDescriptor { total_size_bytes: manifest.size_bytes, content_hash: Some(manifest.content_hash.clone()) };
        let retained = content.capture_repair_payload(
            crate::repair_queue_store::repair_subject(&incarnation, &message).unwrap(),
            crate::repair_queue_store::original_repair_digest(&message, &descriptor).unwrap(),
            object_id.as_str(), version.as_str(), &manifest.content_hash, manifest.size_bytes,
        ).unwrap();
        queue.enroll_original_repair(&message, &descriptor, &retained).unwrap();
        let material = queue.original_repair(&message).unwrap().unwrap().original_wire_material().unwrap();
        let command = &material.original.command;
        let policy = DfsReplicationPolicy::new(1, 2).unwrap().with_placement_constraints(
            vec![material.original.source_node_id.clone(), material.original.target_node_id.clone()], Vec::new(), 0, BTreeMap::new(),
        ).unwrap();
        let intent = DfsWriteIntentRecord {
            publication: DfsWritePublication::Current, intent_id: command.intent_id.clone(), file_system_id: command.file_system_id.clone(),
            object_id: command.object_id.clone(), base_version: command.expected_base_version.clone(), target_version: command.version.clone(),
            coordinator_node_id: material.original.source_node_id.clone(), operation_id: None, operation_order_token: None,
            owner_epoch: command.authority_shard.as_ref().unwrap().epoch,
            desired_replica_node_ids: vec![material.original.source_node_id.clone(), material.original.target_node_id.clone()],
            policy, status: DfsWriteIntentStatus::Committed, content_hash: Some(command.content_hash.clone()), object_metadata: Some(object.clone()),
            touch_parent_on_create: false, create_predecessor_object_id: None,
            created_at: time::OffsetDateTime::UNIX_EPOCH, updated_at: None,
        };
        let config = DfsFileSystemConfig::from(&DfsFileSystem::sifs(material.original.source_node_id.clone()));
        (queue, material, object, intent, config)
    }

    #[test]
    fn repair_hard_policy_required_peers_and_self_only_rule_are_not_exclusive() {
        let root = sinomo_testdir::TestDir::new("repair-hard-policy-allowed-retainer");
        let (queue, material, object, intent, mut config) = selected_hard_policy_inputs(root.path());
        let other = NodeId::from_legacy_named("another-policy-retainer").unwrap();
        let mut unrelated = crate::DfsSubtreePolicy::new("/hard-policy", DfsObjectPolicy::LocalOwned, 1, 1);
        unrelated.scope = crate::DfsPlacementScope::SelfOnly;
        unrelated.forbidden_node_ids.push(other.clone());
        config.subtree_policies.push(unrelated);
        let before = (queue.view().unwrap(), intent.clone(), config.clone(), material.clone());
        ensure_repair_content_hard_prohibitions(&other, &object, &intent.policy, &config, "selected receiver").unwrap();
        ensure_repair_content_hard_prohibitions(&material.original.target_node_id, &object, &intent.policy, &config, "selected receiver").unwrap();
        assert_eq!(queue.view().unwrap(), before.0);
        assert_eq!(intent, before.1);
        assert_eq!(config, before.2);
        assert_eq!(material, before.3);
    }

    #[test]
    fn repair_hard_policy_rejects_original_current_and_local_owned_prohibitions() {
        let root = sinomo_testdir::TestDir::new("repair-hard-policy-denied-retainer");
        let (queue, material, object, intent, config) = selected_hard_policy_inputs(root.path());
        let receiver = &material.original.target_node_id;
        for arm in ["immutable", "current", "object-local", "path-local", "foreign-fs"] {
            let mut object = object.clone(); let mut policy = intent.policy.clone(); let mut config = config.clone();
            let reason = match arm {
                "immutable" => {
                    policy = DfsReplicationPolicy::new(1, 2).unwrap().with_placement_constraints(
                        vec![material.original.source_node_id.clone()], vec![receiver.clone()], 0, BTreeMap::new(),
                    ).unwrap();
                    "immutable Repair intent forbids"
                }
                "current" => {
                    let mut rule = crate::DfsSubtreePolicy::new("/hard-policy", DfsObjectPolicy::ReplicatedEventual, 1, 2);
                    rule.forbidden_node_ids.push(receiver.clone()); config.subtree_policies.push(rule);
                    "current path policy forbids"
                }
                "object-local" => { object.policy = DfsObjectPolicy::LocalOwned; "local-owned content" }
                "path-local" => {
                    config.subtree_policies.push(crate::DfsSubtreePolicy::new("/hard-policy", DfsObjectPolicy::LocalOwned, 1, 1));
                    "local-owned content"
                }
                "foreign-fs" => { config.file_system_id = DfsFileSystemId::from_legacy_named("other-policy-filesystem").unwrap(); "another filesystem" }
                _ => unreachable!(),
            };
            let before = queue.view().unwrap();
            let error = ensure_repair_content_hard_prohibitions(receiver, &object, &policy, &config, "selected receiver").unwrap_err();
            assert!(error.to_string().contains(reason), "{arm}: {error}");
            assert_eq!(queue.view().unwrap(), before);
            assert_eq!(queue.original_repair(&material.original.routed()).unwrap().unwrap().original_wire_material().unwrap(), material);
        }
        let mut owned = object.clone(); owned.policy = DfsObjectPolicy::LocalOwned; owned.owner_node_id = receiver.clone();
        ensure_repair_content_hard_prohibitions(receiver, &owned, &intent.policy, &config, "selected receiver").unwrap();
    }

    #[test]
    fn selected_original_intent_requires_exact_current_committed_target_tuple() {
        let root = sinomo_testdir::TestDir::new("selected-repair-exact-intent");
        let (queue, material, _object, intent, _config) = selected_hard_policy_inputs(root.path());
        let receiver = &material.original.target_node_id;
        assert_eq!(checked_selected_original_intent(&material, receiver, Some(&intent)).unwrap(), &intent);
        assert!(checked_selected_original_intent(&material, receiver, None).is_err());
        for arm in ["intent", "filesystem", "object", "base", "version", "hash", "missing-hash", "retained", "staging", "target", "metadata-fs", "metadata-object"] {
            let mut changed = intent.clone();
            match arm {
                "intent" => changed.intent_id.push_str("-other"),
                "filesystem" => changed.file_system_id = DfsFileSystemId::from_legacy_named("other-intent-fs").unwrap(),
                "object" => changed.object_id = DfsObjectId::from_legacy_named("other-intent-object").unwrap(),
                "base" => changed.base_version = Some(DfsVersion::from_legacy_named("other-intent-base").unwrap()),
                "version" => changed.target_version = DfsVersion::from_legacy_named("other-intent-version").unwrap(),
                "hash" => changed.content_hash = Some(crate::content_hash(b"other intent bytes")),
                "missing-hash" => changed.content_hash = None,
                "retained" => changed.publication = DfsWritePublication::RetainedRepair,
                "staging" => changed.status = DfsWriteIntentStatus::Staging,
                "target" => changed.desired_replica_node_ids.retain(|node| node != receiver),
                "metadata-fs" => changed.object_metadata.as_mut().unwrap().file_system_id = DfsFileSystemId::from_legacy_named("foreign-provenance-fs").unwrap(),
                "metadata-object" => changed.object_metadata.as_mut().unwrap().object_id = DfsObjectId::from_legacy_named("foreign-provenance-object").unwrap(),
                _ => unreachable!(),
            }
            assert!(checked_selected_original_intent(&material, receiver, Some(&changed)).is_err(), "{arm}");
        }
        assert_eq!(queue.original_repair(&material.original.routed()).unwrap().unwrap().original_wire_material().unwrap(), material);
    }

    #[test]
    fn selected_original_preserves_local_owned_provenance_despite_current_replication_policy() {
        let root = sinomo_testdir::TestDir::new("selected-repair-immutable-local-owned");
        for arm in ["original-command", "actual-intent"] {
            let case = root.join(arm); std::fs::create_dir_all(&case).unwrap();
            let (queue, material, mut current, mut intent, config) = selected_hard_policy_inputs_with_original_policy(&case,
                if arm == "original-command" { DfsObjectPolicy::LocalOwned } else { DfsObjectPolicy::ReplicatedEventual });
            let receiver = &material.original.target_node_id;
            let original = queue.original_repair(&material.original.routed()).unwrap().unwrap();
            material.validate().unwrap();
            current.policy = DfsObjectPolicy::ReplicatedEventual;
            if arm == "original-command" { intent.object_metadata = Some(current.clone()); }
            else { intent.object_metadata.as_mut().unwrap().policy = DfsObjectPolicy::LocalOwned; }
            // Current policy alone would admit C. Neither immutable body may
            // lose its LocalOwned owner restriction through that later view.
            ensure_repair_content_hard_prohibitions(receiver, &current, &intent.policy, &config, "selected receiver").unwrap();
            let error = checked_selected_original_intent(&material, receiver, Some(&intent)).unwrap_err();
            assert!(error.to_string().contains(if arm == "original-command" { "immutable original command local-owned" } else { "immutable actual intent local-owned" }), "{error}");
            assert_eq!(queue.original_repair(&material.original.routed()).unwrap(), Some(original));
        }
        let allowed_case = root.join("local-owner-is-receiver"); std::fs::create_dir_all(&allowed_case).unwrap();
        let (_queue, material, _current, mut intent, _config) = selected_hard_policy_inputs(&allowed_case);
        let receiver = &material.original.target_node_id;
        intent.object_metadata.as_mut().unwrap().policy = DfsObjectPolicy::LocalOwned;
        intent.object_metadata.as_mut().unwrap().owner_node_id = receiver.clone();
        checked_selected_original_intent(&material, receiver, Some(&intent)).unwrap();
    }

    #[test]
    fn selected_original_intent_keeps_write_coordinator_and_epoch_distinct_from_repair_authority() {
        let root = sinomo_testdir::TestDir::new("selected-repair-intent-prior-coordinator");
        let (_queue, material, _object, mut intent, _config) = selected_hard_policy_inputs(root.path());
        intent.coordinator_node_id = NodeId::from_legacy_named("prior-write-coordinator").unwrap();
        intent.owner_epoch = intent.owner_epoch.checked_sub(1).unwrap();
        checked_selected_original_intent(&material, &material.original.target_node_id, Some(&intent)).unwrap();
        // Native admission still separately requires the actual current A
        // primary/epoch and C owner. These values grant no such permission.
    }

    #[test]
    fn selected_original_rejects_older_target_even_when_current_hash_matches() {
        let root = sinomo_testdir::TestDir::new("selected-original-no-cache-head-rollback");
        let (queue, _source, message, descriptor, retained, _layout) = crate::repair_queue_store::tests::selected_original_fixture(&root.join("source"));
        queue.enroll_original_repair(&message, &descriptor, &retained).unwrap();
        let original = queue.original_repair(&message).unwrap().unwrap();
        let material = original.original_wire_material().unwrap();
        let command = &material.original.command;
        let before = queue.view().unwrap();
        ensure_selected_original_exact_current_content(&command.version, Some(&command.content_hash), command).unwrap();
        let later = DfsVersion::from_legacy_named("later-metadata-only-head").unwrap();
        assert_ne!(later, command.version);
        let error = ensure_selected_original_exact_current_content(&later, Some(&command.content_hash), command).unwrap_err();
        assert!(error.to_string().contains("exact current version/hash"));
        assert!(ensure_selected_original_exact_current_content(&command.version, None, command).is_err());
        assert!(ensure_selected_original_exact_current_content(&command.version, Some("different-content"), command).is_err());
        assert_eq!(queue.view().unwrap(), before);
        assert_eq!(queue.original_repair(&message).unwrap(), Some(original));
        // This tests the exact readonly guard used before Prepared and again
        // before local cache application. It is not a native epoch/race proof.
    }

    #[test]
    fn selected_original_default_old_fixture_and_missing_native_owner_never_apply() {
        let root = sinomo_testdir::TestDir::new("selected-original-reject-unadmitted-native");
        let (queue, _source, message, descriptor, retained, _layout) = crate::repair_queue_store::tests::selected_original_fixture(&root.join("source"));
        queue.enroll_original_repair(&message, &descriptor, &retained).unwrap();
        let material = queue.original_repair(&message).unwrap().unwrap().original_wire_material().unwrap();
        for format in [2,3,4] {
            let case = root.join(format!("format-{format}"));
            std::fs::create_dir_all(&case).unwrap();
            let content_root = case.join("content");
            let content = match format {
                2 => LocalContentStore::new(&content_root).unwrap(),
                3 => LocalContentStore::new_received_repair_fixture(&content_root).unwrap().0,
                _ => LocalContentStore::new_original_repair_fixture(&content_root).unwrap().0,
            };
            let catalog = content_root.join("inline-bindings/current.sobj");
            let before = std::fs::read(&catalog).unwrap();
            let mut service = NetworkedDfsService::new(crate::test_runtime::DfsTestRuntime::local_transport(material.original.target_node_id.clone()), content);
            // Construction intentionally installs SIFS. Rejection must preserve
            // that actual baseline, not require an empty filesystem cache.
            let metadata_before = service.metadata.clone();
            let error = service.handle_selected_original_repair(material.original.source_node_id.clone(), DfsSelectedOriginalRepairCommand { version: 1, material: material.clone(), bytes: b"complete original attachment".to_vec() }).unwrap_err();
            assert!(error.to_string().contains(if format == 4 { "actual native backend" } else { "explicit native fixture" }), "{error}");
            assert_eq!(service.metadata.objects, metadata_before.objects);
            assert_eq!(service.metadata.filesystems, metadata_before.filesystems);
            assert_eq!(service.metadata.objects_by_path, metadata_before.objects_by_path);
            assert_eq!(std::fs::read(&catalog).unwrap(), before);
            assert!(service.content.read_object_version(material.original.command.object_id.as_str(), material.original.command.version.as_str()).is_err());
            if format == 4 { assert!(service.content.original_repair_test_snapshot(&material).unwrap().is_none()); }
        }
    }

    // Capture member/capability DATA verbatim from ordinary native startup,
    // following the maintained control_plane unit-fixture pattern. It grants
    // no live transaction, publication proof or backend authority to this copy.
    fn receiver_control_plane(
        requester: &NodeId,
        local: &NodeId,
    ) -> (
        SharedDfsControlPlaneKvClient<InProcessKvStore>,
        DfsMetadataShardConfig,
    ) {
        let requester = requester.clone();
        let local = local.clone();
        std::thread::spawn(move || {
            let mut runtime = crate::test_runtime::DfsTestRuntime::new(
                "received-repair-captured-cp-data",
                requester.clone(),
            )
            .unwrap();
            runtime.define_node(local.clone()).unwrap();
            runtime.start_nodes().unwrap();
            let shard = runtime.root_shard().unwrap();
            assert_eq!(shard.primary_node_id, requester);
            assert!(shard.replica_node_ids.contains(&local));
            let paths = sinomo_core::network::ClusterPaths::new(runtime.data_dir(&requester));
            let mut source = crate::runtime_host::DaemonDfsControlPlaneKvClient::new_on_runtime(
                &paths,
                &runtime.runtime_handle(),
            );
            let nodes = [&requester, &local];
            let keys = nodes
                .iter()
                .flat_map(|node| {
                    [
                        cluster_member_key(node),
                        cluster_capability_policy_key(node),
                    ]
                })
                .collect::<Vec<_>>();
            let entries = source.get_authoritative_consistent(&keys).unwrap();
            assert_eq!(entries.len(), keys.len());
            let mut data = InProcessKvStore::new();
            for (key, entry) in keys.into_iter().zip(entries) {
                if let Some(entry) = entry {
                    assert_eq!(entry.key, key);
                    data.put(entry.key, entry.value, None).unwrap();
                }
            }
            for node in nodes {
                let member = data
                    .get(&cluster_member_key(node))
                    .unwrap()
                    .decode::<sinomo_core::ClusterMemberRecord>()
                    .unwrap();
                assert_eq!(&member.node_id, node);
                assert!(member.state.is_authorized());
                assert!(
                    member
                        .capabilities
                        .iter()
                        .any(|c| c.as_str() == DFS_METADATA_PREPARED_OPERATION_V7_CAPABILITY)
                );
            }
            DfsControlPlaneStore::new(&mut data)
                .put_metadata_shard(&shard)
                .unwrap();
            drop(source);
            runtime.close().unwrap();
            (SharedDfsControlPlaneKvClient::new(data), shard)
        })
        .join()
        .expect("ordinary native configuration DATA producer")
    }

    // No backend is fabricated. These local receiver controls exercise owner,
    // base, content and normal cache/reply handling, not backend intent/receipt
    // or current-publication authority. Those need native metadata fixtures.
    fn receiver(
        content: LocalContentStore,
    ) -> (
        NetworkedDfsService<DfsNodeTransport>,
        NodeId,
        DfsRepairReplicaCommand,
    ) {
        let requester = NodeId::from_legacy_named("received-repair-owner").unwrap();
        let local = NodeId::from_legacy_named("received-repair-local").unwrap();
        let (control_plane, shard) = receiver_control_plane(&requester, &local);
        let mut service = NetworkedDfsService::new(
            crate::test_runtime::DfsTestRuntime::local_transport(local),
            content,
        )
        .with_control_plane(control_plane);
        let object = DfsObjectMetadata {
            object_id: DfsObjectId::from_legacy_named("received-repair-object").unwrap(),
            file_system_id: crate::sifs_file_system_id(),
            path: "/received-repair.bin".into(),
            kind: DfsObjectKind::File,
            tombstone_provenance: None,
            owner_node_id: requester.clone(),
            current_locations: vec![requester.clone()],
            content_hash: None,
            content_size_bytes: None,
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version: DfsVersion::from_legacy_named("base").unwrap(),
            policy: DfsObjectPolicy::LocalOwned,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        };
        service.metadata.add_object(object.clone()).unwrap();
        let selected = service
            .select_metadata_shard_for_object(&object)
            .unwrap()
            .expect("fixture shard");
        assert_eq!(selected, shard);
        assert_ne!(
            selected.primary_node_id, service.node_id,
            "exercise the existing nonauthoritative replica cache branch"
        );
        let bytes = b"actual receiver captured owned body".to_vec();
        let command = DfsRepairReplicaCommand {
            request_id: "actual-received-repair".into(),
            background_repair: true,
            intent_id: "actual-received-intent".into(),
            file_system_id: object.file_system_id,
            object_id: object.object_id,
            expected_base_version: Some(object.version),
            version: DfsVersion::from_legacy_named("target").unwrap(),
            content_hash: crate::content_hash(&bytes),
            bytes,
            object_metadata: None,
            authority_shard: None,
        };
        (service, requester, command)
    }

    #[test]
    fn received_repair_default_v1_receiver_keeps_normal_content_and_metadata_result() {
        let root = sinomo_testdir::TestDir::new("received-repair-default-receiver");
        let data = SinomoDataDir::new(root.path());
        let _owner = DfsDataOwnerLock::acquire(&DfsRuntimePaths::new(&data)).unwrap();
        let content = LocalContentStore::new(data.dfs_dir().join("content")).unwrap();
        let (mut service, requester, command) = receiver(content);
        let event = service
            .handle_repair_replica(requester.clone(), command.clone())
            .unwrap();
        assert!(
            matches!(event, NetworkedDfsServiceEvent::ReplicaRepaired { .. }),
            "{event:?}"
        );
        assert_eq!(
            service
                .content
                .read_object_version(command.object_id.as_str(), command.version.as_str())
                .unwrap()
                .1,
            command.bytes
        );
        assert_eq!(
            service.metadata.object(&command.object_id).unwrap().version,
            command.version
        );
        assert!(
            service
                .content
                .received_repair_test_snapshot(&requester, &command.request_id)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn received_repair_fixture_receiver_binds_exact_capture_and_keeps_metadata_path() {
        let root = sinomo_testdir::TestDir::new("received-repair-fixture-receiver");
        let (content, identity) =
            LocalContentStore::new_received_repair_fixture(root.join("content")).unwrap();
        let (mut service, requester, command) = receiver(content);
        let event = service
            .handle_repair_replica(requester.clone(), command.clone())
            .unwrap();
        assert!(
            matches!(event, NetworkedDfsServiceEvent::ReplicaRepaired { .. }),
            "{event:?}"
        );
        let object = service.metadata.object(&command.object_id).unwrap();
        assert_eq!(object.version, command.version);
        assert_eq!(object.content_hash.as_ref(), Some(&command.content_hash));
        assert!(object.current_locations.contains(&service.node_id));
        let before = service
            .content
            .received_repair_test_snapshot(&requester, &command.request_id)
            .unwrap()
            .unwrap();
        assert_eq!(before.command, command);
        assert!(before.bound_id.is_some());
        drop(service);
        let store = LocalContentStore::reopen_received_repair_fixture(&identity).unwrap();
        assert_eq!(
            store
                .received_repair_test_snapshot(&requester, &command.request_id)
                .unwrap()
                .unwrap(),
            before
        );
        assert_eq!(
            store
                .read_object_version(command.object_id.as_str(), command.version.as_str())
                .unwrap()
                .1,
            command.bytes
        );
    }

    #[test]
    fn received_repair_receiver_preflight_rejections_create_no_capture_or_binding() {
        for fault in ["requester", "base", "body"] {
            let root = sinomo_testdir::TestDir::new(&format!("received-repair-preflight-{fault}"));
            let (content, _) =
                LocalContentStore::new_received_repair_fixture(root.join("content")).unwrap();
            let (mut service, requester, command) = receiver(content);
            let mut incoming = command.clone();
            let sender = match fault {
                "requester" => NodeId::from_legacy_named("unauthorized-node").unwrap(),
                "base" => {
                    incoming.expected_base_version =
                        Some(DfsVersion::from_legacy_named("wrong-base").unwrap());
                    requester.clone()
                }
                "body" => {
                    incoming.bytes = b"body does not match declared hash".to_vec();
                    requester.clone()
                }
                _ => unreachable!(),
            };
            let event = service
                .handle_repair_replica(sender.clone(), incoming)
                .unwrap();
            assert!(
                matches!(event, NetworkedDfsServiceEvent::RepairRejected { .. }),
                "{fault}: {event:?}"
            );
            assert!(
                service
                    .content
                    .received_repair_test_snapshot(&sender, &command.request_id)
                    .unwrap()
                    .is_none()
            );
            assert!(
                service
                    .content
                    .read_manifest(command.object_id.as_str(), command.version.as_str())
                    .is_err()
            );
        }
    }

    #[test]
    fn received_repair_receiver_unsupported_new_body_uses_ordinary_durable_path() {
        for large_body in [true, false] {
            let root = sinomo_testdir::TestDir::new(&format!(
                "received-repair-receiver-large-fallback-{large_body}"
            ));
            let (content, identity) =
                LocalContentStore::new_received_repair_fixture(root.join("content")).unwrap();
            let (mut service, requester, mut command) = receiver(content);
            if large_body {
                command.bytes = vec![17; 32 * 1024];
            } else {
                command.intent_id = "x".repeat(65 * 1024);
            }
            command.content_hash = crate::content_hash(&command.bytes);
            let event = service
                .handle_repair_replica(requester.clone(), command.clone())
                .unwrap();
            assert!(
                matches!(event, NetworkedDfsServiceEvent::ReplicaRepaired { .. }),
                "{event:?}"
            );
            assert!(
                service
                    .content
                    .received_repair_test_snapshot(&requester, &command.request_id)
                    .unwrap()
                    .is_none()
            );
            drop(service);
            let store = LocalContentStore::reopen_received_repair_fixture(&identity).unwrap();
            assert_eq!(
                store
                    .read_object_version(command.object_id.as_str(), command.version.as_str())
                    .unwrap()
                    .1,
                command.bytes
            );
            let roots = store.content_reservation_roots().unwrap();
            assert!(
                roots
                    .reservations
                    .iter()
                    .any(|r| r.identity.object_id == command.object_id.as_str()
                        && r.identity.version == command.version.as_str()
                        && r.identity.content_hash == command.content_hash)
            );
        }
    }

    #[test]
    fn received_repair_receiver_cut_reopens_prepared_only_then_binds_same_identity() {
        let root = sinomo_testdir::TestDir::new("received-repair-receiver-cut");
        let (content, identity) =
            LocalContentStore::new_received_repair_fixture(root.join("content")).unwrap();
        content.arm_received_repair_after_prepared_test_cut();
        let (mut service, requester, command) = receiver(content);
        let event = service
            .handle_repair_replica(requester.clone(), command.clone())
            .unwrap();
        assert!(
            matches!(&event, NetworkedDfsServiceEvent::RepairRejected { reason, .. } if reason.contains("after Prepared")),
            "{event:?}"
        );
        let before = service
            .content
            .received_repair_test_snapshot(&requester, &command.request_id)
            .unwrap()
            .unwrap();
        assert_eq!(before.command, command);
        assert_eq!(before.bound_id, None);
        assert!(
            service
                .content
                .read_manifest(command.object_id.as_str(), command.version.as_str())
                .is_err()
        );
        drop(service); // No service/store handle survives the actual file-backed reopen.
        let content = LocalContentStore::reopen_received_repair_fixture(&identity).unwrap();
        assert_eq!(
            content
                .received_repair_test_snapshot(&requester, &command.request_id)
                .unwrap()
                .unwrap(),
            before
        );
        let (mut service, requester, retry) = receiver(content);
        assert_eq!(retry, command);
        let event = service
            .handle_repair_replica(requester.clone(), retry)
            .unwrap();
        assert!(
            matches!(event, NetworkedDfsServiceEvent::ReplicaRepaired { .. }),
            "{event:?}"
        );
        let after = service
            .content
            .received_repair_test_snapshot(&requester, &command.request_id)
            .unwrap()
            .unwrap();
        assert_eq!(after.prepared_id, before.prepared_id);
        assert_eq!(after.prepared_seal, before.prepared_seal);
        assert_eq!(after.command, before.command);
        assert!(after.bound_id.is_some());
    }
}

#[cfg(test)]
mod receipt_projection_tests {
    use super::*;

    /// Test-only use of the EXISTING caller-deadline contract. Registered Main
    /// delegates namespace work, so routed sync clamps its internal requested
    /// timeout to this earlier deadline; no production timeout is changed.
    fn registered_seed_sync_with_deadline(
        writer: &mut NetworkedDfsService<DfsNodeTransport>,
        path: &str,
        deadline: std::time::Instant,
    ) -> std::result::Result<DfsPosixFileSyncCertificate, DfsPosixError> {
        assert!(writer.transport.delegates_local_namespace_authority());
        assert_eq!(writer.transport.operation_deadline(), Some(deadline));
        let result = writer.posix_sync_file(path, DfsPosixSyncKind::File);
        assert_eq!(
            writer.transport.operation_deadline(),
            Some(deadline),
            "nested sync must preserve the stricter caller deadline"
        );
        result
    }

    /// Both arms use fresh native roots with the same deterministic node IDs,
    /// configuration, policy, payload and one binary. Only the seed preparation
    /// contract differs. The 5s budget starts BEFORE write in both arms; R41
    /// started its 5s clock after write, so this comparison is stricter.
    fn controlled_seed_arm(
        registered_main: bool,
    ) -> std::result::Result<serde_json::Value, String> {
        let name = "native-original-repair-seed-pair";
        let path = "/services/original-repair-seed-pair.bin";
        let bytes = b"native original Repair body";
        let (runtime, browser, config) =
            crate::test_support::current_native_metadata_service(name, 2, path)?;
        drop(browser);
        let owner = config.primary_node_id.clone();
        let peer = config.replica_node_ids[0].clone();
        runtime.apply_placement_policy(
            &owner,
            path,
            DfsPlacementScope::Subtree,
            DfsPlacementPolicy::new(sinomo_core::DfsObjectPolicy::ReplicatedEventual, 1, 2)
                .unwrap(),
        )?;
        let mut policy_reader = runtime.metadata_primary_service(path)?;
        let (_, _, actual_policy) = policy_reader
            .managed_replication_policy_for_path(
                path,
                sinomo_core::DfsObjectPolicy::ReplicatedEventual,
                2,
            )
            .map_err(|error| error.to_string())?;
        if actual_policy.min_commit_replicas != 1 || actual_policy.desired_replicas != 2 {
            return Err(format!(
                "unexpected effective seed policy {actual_policy:?}"
            ));
        }
        drop(policy_reader);
        let mut writer = if registered_main {
            runtime.client(&owner)?
        } else {
            runtime.metadata_primary_service(path)?
        };
        if writer.transport.delegates_local_namespace_authority() != registered_main {
            return Err("seed opener has wrong actual namespace owner".into());
        }
        let namespace_capture = runtime.capture_namespace_requests(&owner, 32)?;
        let started = std::time::Instant::now();
        let budget = sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        let deadline = started + budget;
        let previous_deadline = writer.set_transport_operation_deadline(Some(deadline));
        let observation_cursor = runtime.rpc_observations().len();
        let mut report = serde_json::json!({
            "arm":if registered_main {"registered-main-write-sync"} else {"owning-core-write-maintenance"},
            "configuration":config, "effective_policy":format!("{actual_policy:?}"),
            "path":path, "payload_bytes":bytes.len(), "caller_budget_seconds":budget.as_secs_f64(),
            "clock_start":"before-write", "phase":"write", "prepared":false,
            "root":runtime.data_dir(&owner).root(), "maintenance_calls":[],
            "local_fallback_exclusion":"not claimed; typed Main ingress and strict returned elapsed budget are checked"
        });
        let written = writer.posix_write_file(
            path,
            bytes,
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(1, 2).unwrap(),
                replica_candidates: vec![owner.clone(), peer.clone()],
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        );
        report["write_seconds"] = serde_json::json!(started.elapsed().as_secs_f64());
        let stat = match written {
            Ok(stat) => Some(stat),
            Err(error) => {
                report["write_error"] = serde_json::json!(error.to_string());
                None
            }
        };
        let barrier_started = std::time::Instant::now();
        let mut maintenance_calls = Vec::new();
        // Errors belong to this arm's measured outcome, never an early return
        // that skips the common explicit runtime drain/report tail below.
        let barrier: std::result::Result<(), String> = (|| {
            let stat = stat.as_ref().ok_or("write failed; barrier not attempted")?;
            report["phase"] = serde_json::json!("barrier");
            if registered_main {
                registered_seed_sync_with_deadline(&mut writer, path, deadline)
                    .map_err(|error| error.to_string())
                    .and_then(|certificate| {
                        if certificate.target.version == stat.version
                            && certificate.content_evidence.required_receipts == 2
                        {
                            Ok(())
                        } else {
                            Err("sync returned a different immutable target or durability".into())
                        }
                    })
            } else {
                loop {
                    let receipts = writer
                        .metadata_backend()
                        .ok_or("seed Core has no backend")?
                        .receipts_for_version(&stat.object_id, &stat.version)
                        .map_err(|error| error.to_string())?;
                    if receipts.len() == 2 {
                        break Ok(());
                    }
                    let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                    if remaining.is_zero() {
                        break Err("same 5s seed caller deadline expired".into());
                    }
                    let tick = runtime.maintenance_tick(&owner, remaining)?;
                    maintenance_calls.push(format!("{:?}", tick.outcome));
                    if let Err(error) = tick.outcome {
                        break Err(error);
                    }
                }
            }
        })();
        let within_budget = std::time::Instant::now() <= deadline;
        let deadline_preserved = writer.transport.operation_deadline() == Some(deadline);
        report["barrier_seconds"] = serde_json::json!(barrier_started.elapsed().as_secs_f64());
        report["seed_seconds"] = serde_json::json!(started.elapsed().as_secs_f64());
        report["within_caller_budget"] = serde_json::json!(within_budget);
        report["caller_deadline_preserved"] = serde_json::json!(deadline_preserved);
        report["barrier_result"] = serde_json::json!(format!("{barrier:?}"));
        report["maintenance_calls"] = serde_json::json!(maintenance_calls);
        writer.set_transport_operation_deadline(previous_deadline);
        let events = runtime.rpc_observations();
        let observations_complete =
            events.len() >= observation_cursor && runtime.observation_dropped_count() == 0;
        let main_namespace_completions = events
            .get(observation_cursor..)
            .unwrap_or_default()
            .iter()
            .filter(|event| {
                event.route == DFS_DATA_NAMESPACE_ROUTE
                    && event.origin == crate::test_runtime::RpcOrigin::RegisteredHost
                    && event.phase == crate::test_runtime::RpcPhase::Completed
            })
            .count();
        let raw_requests = namespace_capture.records();
        let typed_sync_requests = raw_requests.iter().filter(|message| stat.as_ref().is_some_and(|stat| matches!(&message.message,
            DfsNetworkMessage::NamespaceRequest(request) if matches!(&request.operation,
                DfsNamespaceOperation::FileSync { target, .. } if target.file_system_id == stat.file_system_id && target.object_id == stat.object_id && target.version == stat.version && Some(target.content_hash.clone()) == stat.content_hash)
        ))).collect::<Vec<_>>();
        report["observations_complete"] = serde_json::json!(observations_complete);
        report["registered_main_namespace_completions"] =
            serde_json::json!(main_namespace_completions);
        report["actual_typed_main_file_sync_requests"] = serde_json::json!(typed_sync_requests);
        report["captured_namespace_requests"] = serde_json::json!(raw_requests);
        drop(namespace_capture);
        drop(writer);
        // Common validation follows the measured barrier in BOTH arms. Keep its
        // cost and any partial result/error, then close the actual owner even
        // when write, maintenance, receipt inspection or binding reads failed.
        let check_started = std::time::Instant::now();
        let validation: std::result::Result<bool, String> = (|| {
            let stat = stat
                .as_ref()
                .ok_or("write failed; immutable target unavailable")?;
            report["stat"] = serde_json::json!({
                "file_system_id":stat.file_system_id,
                "object_id":stat.object_id,
                "version":stat.version,
                "content_hash":stat.content_hash
            });
            let inspector = runtime.metadata_primary_service(path)?;
            let receipts = inspector
                .metadata_backend()
                .ok_or("seed inspector has no backend")?
                .receipts_for_version(&stat.object_id, &stat.version)
                .map_err(|error| error.to_string())?;
            let exact_receipts = receipts.len() == 2
                && receipts
                    .iter()
                    .map(|receipt| receipt.replica_node_id.clone())
                    .collect::<BTreeSet<_>>()
                    == BTreeSet::from([owner.clone(), peer.clone()])
                && receipts.iter().all(|receipt| {
                    receipt.receipt.file_system_id == stat.file_system_id
                        && receipt.receipt.object_id == stat.object_id
                        && receipt.receipt.committed_version == stat.version
                        && receipt.content_hash == stat.content_hash
                });
            report["exact_receipts"] = serde_json::json!(exact_receipts);
            report["receipts"] = serde_json::json!(receipts);
            report["bindings"] = serde_json::json!([]);
            let mut exact_bindings = true;
            for node in [&owner, &peer] {
                let actual = runtime.client(node)?;
                let binding = match actual
                    .content()
                    .read_object_version(stat.object_id.as_str(), stat.version.as_str())
                {
                    Ok((manifest, contents)) => {
                        let exact =
                            Some(manifest.content_hash) == stat.content_hash && contents == bytes;
                        exact_bindings &= exact;
                        serde_json::json!({"node":node,"exact":exact,"bytes":contents.len()})
                    }
                    Err(error) => {
                        exact_bindings = false;
                        serde_json::json!({"node":node,"exact":false,"error":error.to_string()})
                    }
                };
                report["bindings"].as_array_mut().unwrap().push(binding);
            }
            report["exact_live_bindings"] = serde_json::json!(exact_bindings);
            Ok(exact_receipts && exact_bindings)
        })();
        report["post_barrier_validation_seconds"] =
            serde_json::json!(check_started.elapsed().as_secs_f64());
        report["total_seed_and_validation_seconds"] =
            serde_json::json!(started.elapsed().as_secs_f64());
        report["prepared"] = serde_json::json!(
            barrier.is_ok()
                && within_budget
                && deadline_preserved
                && observations_complete
                && validation == Ok(true)
                && (!registered_main
                    || (main_namespace_completions > 0 && !typed_sync_requests.is_empty()))
        );
        if let Err(error) = validation {
            report["validation_error"] = serde_json::json!(error);
        }
        let report_path = runtime
            .data_dir(&owner)
            .root()
            .join("controlled-seed-arm.json");
        if let Err(error) =
            std::fs::write(&report_path, serde_json::to_vec_pretty(&report).unwrap())
        {
            report["artifact_write_error"] = serde_json::json!(error.to_string());
        }
        let drain_started = std::time::Instant::now();
        let drained = runtime.close();
        report["drain_seconds"] = serde_json::json!(drain_started.elapsed().as_secs_f64());
        report["drain_result"] = serde_json::json!(format!("{drained:?}"));
        report["drain_ok"] = serde_json::json!(drained.is_ok());
        // Retention-all preserves the root until the maintained outer helper
        // archives and verifies it. Outside that helper normal cleanup may
        // already have removed the path; stdout still retains the final report.
        if report_path.exists() {
            if let Err(error) =
                std::fs::write(&report_path, serde_json::to_vec_pretty(&report).unwrap())
            {
                report["artifact_write_error"] = serde_json::json!(error.to_string());
            }
        }
        Ok(report)
    }

    #[test]
    fn original_repair_seed_preparation_same_budget_controlled_comparison() {
        let core = controlled_seed_arm(false).unwrap_or_else(|error| serde_json::json!({"arm":"owning-core-write-maintenance","setup_error":error,"prepared":false}));
        let main = controlled_seed_arm(true).unwrap_or_else(|error| serde_json::json!({"arm":"registered-main-write-sync","setup_error":error,"prepared":false}));
        let report = serde_json::json!({"order":["owning-core-write-maintenance","registered-main-write-sync"],"arms":[core,main],"production":"same current source-qualified binary for both arms","controls":"same deterministic node IDs/configuration, explicit W1/N2, payload, options and 5s caller deadline before write","limits":"one ordered fresh-root pair; cluster/root/request/version identities and cache state differ; current production/config differs from R41; no sole historical attribution or SLO claim"});
        eprintln!(
            "CONTROLLED_SEED_COMPARISON {}",
            serde_json::to_string(&report).unwrap()
        );
        let path = std::env::temp_dir().join(format!(
            "controlled-seed-comparison-{}.json",
            std::process::id()
        ));
        std::fs::write(&path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
        let core = &report["arms"][0];
        let main = &report["arms"][1];
        assert!(
            core.get("setup_error").is_none() && main.get("setup_error").is_none(),
            "incomplete controlled setup: {report}"
        );
        assert_eq!(
            core["configuration"], main["configuration"],
            "the pair must share exact shard configuration"
        );
        assert!(
            core.get("artifact_write_error").is_none()
                && main.get("artifact_write_error").is_none(),
            "paired evidence could not be preserved: {report}"
        );
        assert_eq!(core["effective_policy"], main["effective_policy"]);
        assert_eq!(core["caller_budget_seconds"], main["caller_budget_seconds"]);
        assert_eq!(core["clock_start"], main["clock_start"]);
        assert_eq!(
            core["drain_ok"], true,
            "old-arm owner must drain normally; seed failure is not cleanup success"
        );
        assert_eq!(main["drain_ok"], true, "Main owner must drain normally");
        assert_eq!(
            main["prepared"], true,
            "registered Main seed must pass unchanged/stricter budget and exact baseline checks; old-arm result is evidence, not a required failure: {report}"
        );
    }

    #[test]
    fn original_repair_native_completion_preserves_result_and_closes_exact_root() {
        let name = "native-original-repair-custody";
        let path = "/services/original-repair-custody.bin";
        let bytes = b"native original Repair body";
        let (runtime, browser, config) =
            crate::test_support::current_native_metadata_service(name, 2, path).unwrap();
        drop(browser);
        let owner = config.primary_node_id;
        let peer = config.replica_node_ids[0].clone();
        runtime
            .apply_placement_policy(
                &owner,
                path,
                DfsPlacementScope::Subtree,
                DfsPlacementPolicy::new(sinomo_core::DfsObjectPolicy::ReplicatedEventual, 1, 2)
                    .unwrap(),
            )
            .unwrap();
        // Registered Main owns caller binding. Its normal sync barrier has a
        // distinct operation contract from one bounded maintenance invocation.
        let mut writer = runtime.client(&owner).unwrap();
        let seed_capture = runtime.capture_namespace_requests(&owner, 32).unwrap();
        let seed_started = std::time::Instant::now();
        let seed_deadline = seed_started + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        let previous_deadline = writer.set_transport_operation_deadline(Some(seed_deadline));
        let stat = writer
            .posix_write_file(
                path,
                bytes,
                crate::DfsWriteOptions {
                    policy: crate::DfsReplicationPolicy::new(1, 2).unwrap(),
                    replica_candidates: vec![owner.clone(), peer.clone()],
                    wait_for_commit: true,
                    foreground_replica_repair: true,
                },
            )
            .unwrap();
        let synced = registered_seed_sync_with_deadline(&mut writer, path, seed_deadline);
        let seed_seconds = seed_started.elapsed().as_secs_f64();
        writer.set_transport_operation_deadline(previous_deadline);
        eprintln!(
            "CONTROLLED_NATIVE_SEED seconds={seed_seconds} caller_budget=5 clock_start=before-write result={synced:?}"
        );
        assert!(
            std::time::Instant::now() <= seed_deadline,
            "registered Main seed exceeded the unchanged/stricter 5s caller budget"
        );
        let synced = synced.unwrap();
        assert_eq!(synced.target.version, stat.version);
        assert_eq!(synced.content_evidence.required_receipts, 2);
        let seed_requests = seed_capture.records();
        assert!(seed_requests.iter().any(|message| matches!(&message.message,
            DfsNetworkMessage::NamespaceRequest(request) if matches!(&request.operation,
                DfsNamespaceOperation::FileSync { target, .. } if target.file_system_id == stat.file_system_id && target.object_id == stat.object_id && target.version == stat.version && Some(target.content_hash.clone()) == stat.content_hash)
        )), "seed must enter actual typed Main FileSync");
        drop(seed_capture);
        drop(writer);
        let mut writer = runtime.metadata_primary_service(path).unwrap();
        let receipts = writer
            .metadata_backend()
            .unwrap()
            .receipts_for_version(&stat.object_id, &stat.version)
            .unwrap();
        assert_eq!(receipts.len(), 2);
        assert_eq!(
            receipts
                .iter()
                .map(|receipt| receipt.replica_node_id.clone())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([owner.clone(), peer.clone()])
        );
        assert!(receipts.iter().all(|receipt| receipt.receipt.file_system_id
            == stat.file_system_id
            && receipt.receipt.object_id == stat.object_id
            && receipt.receipt.committed_version == stat.version
            && receipt.content_hash == stat.content_hash));
        for node in [&owner, &peer] {
            let actual = runtime.client(node).unwrap();
            let (manifest, contents) = actual
                .content()
                .read_object_version(stat.object_id.as_str(), stat.version.as_str())
                .unwrap();
            assert_eq!(Some(manifest.content_hash), stat.content_hash);
            assert_eq!(contents, bytes);
        }
        let capture = runtime
            .capture_checkpointed_background_repairs(&owner, 8)
            .unwrap();
        writer
            .mark_remote_read_location_missing(
                &stat.object_id,
                &stat.version,
                &stat.content_hash,
                &peer,
            )
            .unwrap();
        drop(writer);
        let store = crate::repair_queue_store::LocalDfsRepairQueueStore::new(
            crate::default_dfs_repair_attempt_snapshot_path(runtime.data_dir(&owner)),
        );
        let deadline = std::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        let mut recent_ticks = std::collections::VecDeque::with_capacity(8);
        let birth = loop {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            assert!(!remaining.is_zero(), "normal Main repair budget");
            let tick = runtime.maintenance_tick(&owner, remaining).unwrap();
            assert!(
                tick.outcome.is_ok(),
                "normal Main custody completion: {:?}",
                tick.outcome
            );
            recent_ticks.push_back(tick);
            if recent_ticks.len() > 8 {
                recent_ticks.pop_front();
            }
            if let Some(birth) = capture.records().iter().find_map(|captured| {
                store
                    .original_repair(&captured.message)
                    .unwrap()
                    .filter(|birth| birth.closed_at_sequence.is_some())
            }) {
                break birth;
            }
            let within_budget = std::time::Instant::now() < deadline;
            if !within_budget {
                // Failure-only observations of existing owners. Never extend
                // the caller budget, publish work or infer a reply/authority.
                let captured = capture.records();
                eprintln!(
                    "ORIGINAL_REPAIR_TIMEOUT capture_count={} omitted={} recent_ticks={recent_ticks:#?}",
                    captured.len(),
                    capture.omitted_count()
                );
                let mut ids = BTreeSet::new();
                for call in &captured {
                    if let DfsNetworkMessage::RepairReplica(command) = &call.message.message {
                        ids.insert(command.request_id.clone());
                    }
                    eprintln!(
                        "ORIGINAL_REPAIR_CAPTURE head={:?} descriptor={:?} birth={:?}",
                        call.message,
                        call.descriptor,
                        store.original_repair(&call.message)
                    );
                }
                eprintln!(
                    "ORIGINAL_REPAIR_QUEUE {:?}",
                    store.view().map(|view| view.records)
                );
                let events = runtime.rpc_observations();
                let matching = events
                    .iter()
                    .filter(|event| {
                        event
                            .dfs_request_id
                            .as_ref()
                            .is_some_and(|id| ids.contains(id))
                    })
                    .collect::<Vec<_>>();
                eprintln!(
                    "ORIGINAL_REPAIR_RPC exact_request_events={matching:#?} dropped={}",
                    runtime.observation_dropped_count()
                );
            }
            assert!(
                within_budget,
                "actual original Repair completion within normal existing budget"
            );
        };
        assert_eq!(capture.omitted_count(), 0);
        assert!(!birth.attempts.is_empty());
        assert!(
            birth
                .observed
                .iter()
                .any(|response| matches!(response.result, DfsBlobTransferResult::Committed { .. }))
        );
        let closed = store
            .closed_repair_birth(&birth.root.subject)
            .unwrap()
            .expect("durable closure");
        let content = crate::LocalContentStore::open_for_inspection(
            runtime.data_dir(&owner).dfs_dir().join("content"),
        );
        assert!(
            content.inspect_repair_payload_root(&birth.root).is_err(),
            "closed root is released, not a live retention promise"
        );
        let actual = runtime.client(&peer).unwrap();
        assert_eq!(
            actual
                .content()
                .read_blob(stat.content_hash.as_deref().unwrap())
                .unwrap(),
            bytes
        );
        drop(actual);
        assert_eq!(closed.root(), &birth.root);
    }

    #[test]
    fn bounded_repair_restore_observes_receipt_published_by_another_actual_owner_lane() {
        let name = "native-repair-receipt-projection-refresh";
        let path = "/services/receipt-projection-refresh.bin";
        let bytes = b"actual two-holder receipt projection";
        let (runtime, browser, config) =
            crate::test_support::current_native_metadata_service(name, 2, path).unwrap();
        drop(browser);
        let owner = config.primary_node_id;
        let peer = config.replica_node_ids[0].clone();
        let mut writer = runtime.metadata_primary_service(path).unwrap();
        let stat = writer
            .posix_write_file(
                path,
                bytes,
                crate::DfsWriteOptions {
                    policy: crate::DfsReplicationPolicy::new(1, 2).unwrap(),
                    replica_candidates: vec![owner.clone(), peer.clone()],
                    wait_for_commit: true,
                    foreground_replica_repair: true,
                },
            )
            .unwrap();
        let deadline = std::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        loop {
            if writer
                .metadata_backend()
                .unwrap()
                .receipts_for_version(&stat.object_id, &stat.version)
                .unwrap()
                .len()
                == 2
            {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "actual second receipt must arrive within the existing budget"
            );
            writer.retry_pending_repairs_bounded(1).unwrap();
            writer.process_incoming().unwrap();
        }
        let background = runtime
            .deny_background_replica_before_claim(&owner)
            .unwrap();
        let physical = runtime.client(&peer).unwrap();
        assert_eq!(
            physical
                .content()
                .read_blob(stat.content_hash.as_deref().unwrap())
                .unwrap(),
            bytes
        );
        drop(physical);
        writer
            .mark_remote_read_location_missing(
                &stat.object_id,
                &stat.version,
                &stat.content_hash,
                &peer,
            )
            .unwrap();
        drop(writer);

        let mut retained = runtime.metadata_primary_service(path).unwrap();
        retained.hydrate_from_metadata_backend().unwrap();
        let scheduling = retained.durable_repair_queue_records();
        assert_eq!(scheduling.len(), 1);
        assert_eq!(scheduling[0].missing_replica_node_id, peer);
        assert!(
            repair_transfer_custody_unresolved(&scheduling[0]),
            "hydration provides no invented NotSent or completion witness"
        );
        let intent_id = scheduling[0].intent_id.clone();
        assert_eq!(
            retained.coordinator.receipts_for_intent(&intent_id).len(),
            1
        );
        assert!(
            retained
                .coordinator
                .repair_job_for_target(&intent_id, &peer)
                .is_some()
        );
        let mut publisher = runtime.metadata_primary_service(path).unwrap();
        assert!(
            publisher
                .put_metadata_receipt(
                    &stat.file_system_id,
                    &stat.object_id,
                    &stat.version,
                    &peer,
                    stat.content_hash.as_deref().unwrap()
                )
                .unwrap()
        );
        let before = publisher
            .metadata_backend()
            .unwrap()
            .export_shard_table_snapshot(&DfsMetadataShardId::new("services"))
            .unwrap()
            .unwrap();
        assert_eq!(
            publisher
                .metadata_backend()
                .unwrap()
                .receipts_for_version(&stat.object_id, &stat.version)
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            retained.coordinator.receipts_for_intent(&intent_id).len(),
            1,
            "the retained coordinator is genuinely behind the other real lane"
        );
        let rebuilds = retained.coordinator.repair_job_rebuild_count;
        let restored = retained
            .restore_pending_writes_for_repair_attempt_records(&scheduling)
            .unwrap();
        assert_eq!(
            restored, scheduling,
            "logical receipt refresh preserves every actual custody field"
        );
        assert_eq!(
            retained
                .repair_attempt_records
                .get(&(intent_id.clone(), peer.clone())),
            Some(&scheduling[0]),
            "unresolved producer evidence survives logical settlement"
        );
        assert!(
            retained
                .coordinator
                .repair_job_for_target(&intent_id, &peer)
                .is_none(),
            "a verified durable receipt and current location must settle the stale logical job before another dispatch"
        );
        assert_eq!(
            retained.coordinator.receipts_for_intent(&intent_id).len(),
            2
        );
        assert_eq!(retained.coordinator.repair_job_rebuild_count, rebuilds + 1);
        retained
            .restore_pending_writes_for_repair_attempt_records(&scheduling)
            .unwrap();
        assert_eq!(
            retained.coordinator.repair_job_rebuild_count,
            rebuilds + 1,
            "an unchanged cut does not rebuild the complete repair list again"
        );
        assert_eq!(
            publisher
                .metadata_backend()
                .unwrap()
                .export_shard_table_snapshot(&DfsMetadataShardId::new("services"))
                .unwrap()
                .unwrap(),
            before,
            "repair projection refresh cannot publish or alter durable metadata"
        );
        drop(publisher);
        drop(retained);
        drop(background);
        runtime.close().unwrap();
    }
}

#[cfg(test)]
mod retained_receipt_publisher_tests {
    use super::*;

    #[test]
    fn native_retained_receipts_allow_primary_publisher_distinct_from_write_coordinator() {
        let path = "/services/retained-publisher-role.bin";
        let (runtime, browser, config) = crate::test_support::current_native_metadata_service(
            "native-retained-publisher-role",
            3,
            path,
        )
        .unwrap();
        drop(browser);
        let primary = config.primary_node_id.clone();
        let caller = config.replica_node_ids[0].clone();
        assert_ne!(caller, primary);
        let mut writer = runtime.client(&caller).unwrap();
        let stat = writer
            .posix_write_file(
                path,
                b"actual routed W2 retained content",
                crate::DfsWriteOptions {
                    policy: crate::DfsReplicationPolicy::new(2, 2).unwrap(),
                    replica_candidates: vec![primary.clone(), caller.clone()],
                    wait_for_commit: true,
                    foreground_replica_repair: true,
                },
            )
            .unwrap();
        let primary_service = runtime.metadata_primary_service(path).unwrap();
        let original = primary_service
            .metadata_backend()
            .unwrap()
            .get_version(&stat.object_id, &stat.version)
            .unwrap()
            .unwrap();
        let plan = original.content_durability.as_ref().unwrap();
        assert_eq!(plan.coordinator_node_id, caller);
        assert_eq!(plan.policy.min_commit_replicas, 2);
        assert_eq!(plan.policy.desired_replicas, 2);
        let receipts = primary_service
            .metadata_backend()
            .unwrap()
            .receipts_for_version(&stat.object_id, &plan.source_version)
            .unwrap();
        assert_eq!(
            receipts
                .iter()
                .map(|r| &r.replica_node_id)
                .collect::<BTreeSet<_>>()
                .len(),
            2
        );
        assert!(receipts.iter().all(|r| r.receipt.owner_node_id == primary));
        drop(primary_service);

        let capture_request = crate::DfsContentReferenceRequest {
            namespace: "vendor.semantic".into(),
            path: path.into(),
            identity: crate::DfsRevisionReference {
                file_system_id: stat.file_system_id.clone(),
                object_id: stat.object_id.clone(),
                version: stat.version.clone(),
            },
            operation: crate::DfsContentReferenceOperation::CaptureCurrent {
                reference_id: "primary-publisher".into(),
                metadata: None,
            },
            operation_id: sinomo_core::Id::generate().to_string(),
        };
        let capture = writer.retain_content_revision(capture_request.clone());
        let reader = runtime.metadata_primary_service(path).unwrap();
        let retained = reader
            .metadata_backend()
            .unwrap()
            .get_version(&stat.object_id, &stat.version)
            .unwrap()
            .unwrap();
        assert!(
            retained.has_retention_grant(),
            "normal capture must publish actual retained provenance"
        );
        let owner = reader.recorded_retained_repair_owner(&retained);
        eprintln!(
            "RETAINED_RECEIPT_ROLE {}",
            serde_json::json!({
                "primary": primary, "coordinator": caller,
                "object": stat.object_id, "version": stat.version,
                "capture_operation": capture_request.operation_id,
                "capture_result": format!("{capture:?}"),
                "owner_result": format!("{owner:?}"),
                "receipts": receipts,
            })
        );
        if owner.is_ok() {
            // These are invalid requested views of real recorded receipts, not
            // supplied acknowledgements or mutations of the actual metadata.
            let mut wrong = retained.clone();
            wrong.content_hash = Some("blake3:wrong".into());
            assert!(reader.recorded_retained_repair_owner(&wrong).is_err());
            let mut wrong = retained.clone();
            wrong.content_durability.as_mut().unwrap().source_version = DfsVersion::generate();
            assert!(reader.recorded_retained_repair_owner(&wrong).is_err());
            let mut wrong = retained.clone();
            wrong.object_id = DfsObjectId::generate();
            assert!(reader.recorded_retained_repair_owner(&wrong).is_err());
            let mut wrong = retained.clone();
            wrong.file_system_id = DfsFileSystemId::generate();
            assert!(reader.recorded_retained_repair_owner(&wrong).is_err());
            let mut wrong = retained.clone();
            wrong
                .content_durability
                .as_mut()
                .unwrap()
                .desired_replica_node_ids = vec![primary.clone(), primary.clone()];
            assert!(reader.recorded_retained_repair_owner(&wrong).is_err());
        }
        drop(reader);
        drop(writer);
        runtime.close().unwrap();
        capture.expect(
            "normal retained capture keeps actual W2 evidence across publisher/coordinator roles",
        );
        let owner = owner.expect("recorded actual primary receipts satisfy retained W2");
        assert_eq!(owner.coordinator_node_id, caller);
        assert_eq!(owner.policy.min_commit_replicas, 2);
    }
}
