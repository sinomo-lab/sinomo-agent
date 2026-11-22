//! A requester follows its shard's record.
//!
//! A request reaches the shard primary this requester's configuration
//! projection names. Three answers say that record is changing: an unknown
//! outcome, a refusal that names another primary, and a request that could
//! not be delivered. None of them routes the request. Each only says what to
//! wait for: the requester waits on its own projection until the record is
//! strictly newer than the one it dispatched with, then sends the identical
//! request, under the same operation id, to the primary that record names.
//!
//! The wait ends on the record change, on the caller's operation deadline or
//! on cancellation. A timer that doubles to the network's liveness interval
//! re-sends the request to the current primary while the record has not
//! changed; it never ends the wait.
//!
//! A mutation keeps one operation id for its whole call, so every attempt is
//! the same request to the primary that answers it. A read changes nothing:
//! each of its attempts has an id of its own.

use crate::{
    DfsControlPlaneStore, DfsError, DfsFileSystemId, DfsMessageTransport, DfsMetadataShardConfig,
    DfsNamespaceOperation, DfsNamespacePosixError, DfsNamespaceResult, NetworkedDfsService,
    PendingRoutedNamespaceResult,
};
use sinomo_core::network::NetworkDeliveryState;
use sinomo_core::node::NodeId;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// First interval of a following requester's re-send timer. It doubles to
/// [`DFS_RECORD_FOLLOW_RESEND_MAX`]. The timer only re-sends.
pub(crate) const DFS_RECORD_FOLLOW_RESEND_FIRST: Duration = Duration::from_millis(250);
pub(crate) const DFS_RECORD_FOLLOW_RESEND_MAX: Duration =
    sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;

/// The re-send timer of one call.
#[derive(Clone, Copy, Debug)]
pub(crate) struct DfsRecordFollowResend {
    interval: Duration,
    due: Instant,
}

impl DfsRecordFollowResend {
    pub(crate) fn new() -> Self {
        Self {
            interval: DFS_RECORD_FOLLOW_RESEND_FIRST,
            due: Instant::now() + DFS_RECORD_FOLLOW_RESEND_FIRST,
        }
    }

    /// Arm the next interval after a re-send.
    fn rearm(&mut self) {
        self.interval = self
            .interval
            .saturating_mul(2)
            .min(DFS_RECORD_FOLLOW_RESEND_MAX);
        self.due = Instant::now() + self.interval;
    }

    /// Start over after the record changed: the new primary has not been
    /// tried yet.
    fn restart(&mut self) {
        self.interval = DFS_RECORD_FOLLOW_RESEND_FIRST;
        self.due = Instant::now() + DFS_RECORD_FOLLOW_RESEND_FIRST;
    }
}

/// What a requester's evidence of a record change asks it to wait for.
#[derive(Clone, Debug)]
pub(crate) struct DfsRecordChangeEvidence {
    /// The record the request was dispatched with.
    pub(crate) dispatched: DfsMetadataShardConfig,
    /// The lowest epoch the evidence says the record reaches.
    pub(crate) floor_epoch: Option<u64>,
}

impl DfsRecordChangeEvidence {
    /// Whether `current` is the change this evidence waits for: a higher
    /// epoch, at least the evidence's, or another primary or shard.
    pub(crate) fn satisfied_by(&self, current: &DfsMetadataShardConfig) -> bool {
        current.shard_id != self.dispatched.shard_id
            || current.primary_node_id != self.dispatched.primary_node_id
            || current.epoch
                >= self
                    .floor_epoch
                    .unwrap_or(0)
                    .max(self.dispatched.epoch.saturating_add(1))
    }
}

/// How one wait for a record change ended.
#[derive(Clone, Debug)]
pub(crate) enum DfsRecordFollowed {
    /// The projection shows the newer record.
    Newer(DfsMetadataShardConfig),
    /// The record has not changed and the re-send timer is due.
    ResendDue(DfsMetadataShardConfig),
    /// The wait's owner asked to be woken, and a message arrived for it or
    /// its own timer is due.
    Woken,
    /// This requester does not wait, or its caller's budget ended.
    Ended,
}

/// What, besides the record's change, ends one wait on it for its owner.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct DfsRecordWaitWake {
    /// A message that arrives for this requester ends the wait, so its owner
    /// reads the message before it waits again.
    pub(crate) on_arrival: bool,
    /// The owner has work of its own due then.
    pub(crate) at: Option<Instant>,
}

/// How a followed namespace request ended.
#[derive(Debug)]
pub(crate) enum DfsFollowedNamespaceRoute {
    /// The primary answered: applied, or a definite rejection.
    Settled(PendingRoutedNamespaceResult),
    /// The record names this node, which executes the operation itself.
    LocalAuthority,
}

/// The evidence one namespace attempt left, with what the caller gets when
/// the wait ends without a newer record.
#[derive(Clone)]
enum NamespaceAttemptEvidence {
    Rejected(PendingRoutedNamespaceResult),
    Failed(DfsError),
}

fn post_primary_unreachable(local: &NodeId, shard: &DfsMetadataShardConfig) {
    crate::write_set_keeping_signals::post_commit_demand(
        local,
        &shard.file_system_id,
        &crate::DfsMetadataShardId::new(shard.shard_id.clone()),
        crate::write_set_keeping_signals::DfsWriteSetCommitDemand::PrimaryUnreachable,
    );
}

/// Hear a requester's hint that it could not deliver to a shard's primary.
/// Only `local`'s own requesters are heard, and only about the record
/// `local` still projects. The hint leaves one flag for the keeper and
/// changes no DFS state, so hearing it needs no admission.
pub(crate) fn hear_primary_verification_hint(
    local: &NodeId,
    source_node_id: &NodeId,
    request: &crate::DfsPrimaryVerificationRequest,
    projected: Option<&DfsMetadataShardConfig>,
) -> bool {
    if source_node_id != local {
        return false;
    }
    let Some(current) = projected.filter(|current| {
        current.file_system_id == request.file_system_id
            && current.shard_id == request.shard_id
            && current.epoch == request.epoch
            && current.primary_node_id == request.primary_node_id
    }) else {
        return false;
    };
    post_primary_unreachable(local, current);
    true
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    /// Whether this service waits for record changes: it is a requester that
    /// delegates to its node's Main, as every mount, CLI and hosted client
    /// is. Work Main does for itself (a served request, work under a shard's
    /// admission, maintenance and recovery) reads the projection once: a
    /// wait there would hold a lane or stall the pass that runs it.
    pub(crate) fn follows_record_changes(&self) -> bool {
        #[cfg(any(test, feature = "test-support"))]
        if self.writes.single_attempt_for_test {
            return false;
        }
        self.control_plane.is_some()
            && self.transport.delegates_local_namespace_authority()
            && !self.transport.serves_routed_request()
            && self.routed_namespace_settle_depth == 0
            && self.publication_execution.current().is_none()
    }

    /// sinomo:kv-configuration: Read one shard record from this requester's existing configuration projection.
    pub(crate) fn projected_shard_record(
        &mut self,
        file_system_id: &DfsFileSystemId,
        shard_id: &str,
    ) -> Result<Option<DfsMetadataShardConfig>, DfsError> {
        let Some(control_plane) = self.control_plane.as_mut() else {
            return Ok(None);
        };
        DfsControlPlaneStore::new(control_plane.as_mut())
            .get_metadata_shard(file_system_id, shard_id)
            .map_err(DfsError::from)
    }

    /// The current projection of the dispatch shard, including same-epoch
    /// membership updates that change the exact stamp required for a re-send.
    fn current_dispatch_record(
        &mut self,
        evidence: &DfsRecordChangeEvidence,
    ) -> Option<DfsMetadataShardConfig> {
        // A read that fails while the projection is being replaced is not an
        // answer; the wait continues on the next publication.
        self.projected_shard_record(
            &evidence.dispatched.file_system_id,
            &evidence.dispatched.shard_id,
        )
        .ok()
        .flatten()
    }

    /// Wait on this requester's own projection until it shows the record
    /// `evidence` waits for, the re-send timer is due, the caller's operation
    /// deadline passes, or the caller is cancelled. A requester that does not
    /// wait reads its projection once.
    pub(crate) fn await_newer_shard_record(
        &mut self,
        evidence: &DfsRecordChangeEvidence,
        resend: &mut DfsRecordFollowResend,
    ) -> Result<DfsRecordFollowed, DfsError> {
        loop {
            match self.await_shard_record_change(
                evidence,
                Some(&mut *resend),
                DfsRecordWaitWake::default(),
            )? {
                // Nothing asked to be woken.
                DfsRecordFollowed::Woken => {}
                followed => return Ok(followed),
            }
        }
    }

    /// One wait on this requester's projection of the shard `evidence` names.
    ///
    /// It ends when the projection shows the record the evidence waits for,
    /// when `resend`'s timer is due, when `wake` asks for it, at the caller's
    /// operation deadline, or on cancellation. Without `resend` only a newer
    /// record sends the request again. A requester that does not wait reads
    /// its projection once.
    pub(crate) fn await_shard_record_change(
        &mut self,
        evidence: &DfsRecordChangeEvidence,
        resend: Option<&mut DfsRecordFollowResend>,
        wake: DfsRecordWaitWake,
    ) -> Result<DfsRecordFollowed, DfsError> {
        let deadline = self.transport.operation_deadline();
        if deadline.is_some_and(|at| Instant::now() >= at) {
            return Ok(DfsRecordFollowed::Ended);
        }
        let watch = self
            .follows_record_changes()
            .then(|| {
                self.control_plane
                    .as_ref()
                    .and_then(|control_plane| control_plane.watch_configuration_changes())
            })
            .flatten();
        let Some(mut watch) = watch else {
            // No publication events reach this requester: its projection is
            // read through, so one read is all it can observe.
            self.refresh_control_plane_latest()?;
            return Ok(
                match self
                    .current_dispatch_record(evidence)
                    .filter(|current| evidence.satisfied_by(current))
                {
                    Some(record) => DfsRecordFollowed::Newer(record),
                    None => DfsRecordFollowed::Ended,
                },
            );
        };
        let wake_watch = watch.waker();
        let waker: crate::service_state::DfsCancellationWaker = Arc::new(move || wake_watch());
        let _cancellation_wake = self.register_cancellation_waker(&waker);
        if wake.on_arrival {
            self.transport
                .set_incoming_arrival_waker(Some(Arc::clone(&waker)));
        }
        let followed =
            self.await_shard_record_change_on(watch.as_mut(), evidence, resend, wake, deadline);
        if wake.on_arrival {
            self.transport.set_incoming_arrival_waker(None);
        }
        followed
    }

    fn await_shard_record_change_on(
        &mut self,
        watch: &mut dyn crate::DfsConfigurationChangeWatch,
        evidence: &DfsRecordChangeEvidence,
        mut resend: Option<&mut DfsRecordFollowResend>,
        wake: DfsRecordWaitWake,
        deadline: Option<Instant>,
    ) -> Result<DfsRecordFollowed, DfsError> {
        loop {
            self.check_cancelled("shard record wait")?;
            if deadline.is_some_and(|at| Instant::now() >= at) {
                return Ok(DfsRecordFollowed::Ended);
            }
            // An answer that already arrived is read before the record: it
            // may settle the request that would otherwise be sent again.
            if wake.on_arrival
                && self
                    .transport
                    .pending_incoming_count()
                    .map_err(DfsError::from_network)?
                    .is_some_and(|pending| pending > 0)
            {
                return Ok(DfsRecordFollowed::Woken);
            }
            let current = self.current_dispatch_record(evidence);
            if let Some(record) = current
                .as_ref()
                .filter(|record| evidence.satisfied_by(record))
            {
                if let Some(resend) = resend.as_deref_mut() {
                    resend.restart();
                }
                return Ok(DfsRecordFollowed::Newer(record.clone()));
            }
            let now = Instant::now();
            if deadline.is_some_and(|deadline| now >= deadline) {
                return Ok(DfsRecordFollowed::Ended);
            }
            if let Some(resend) = resend.as_deref_mut()
                && now >= resend.due
            {
                resend.rearm();
                // A membership change can keep both epoch and primary. Reuse
                // the operation body, but take the stamp from our current
                // projection. An older projection cannot downgrade a write.
                let record = current
                    .filter(|record| record.epoch == evidence.dispatched.epoch)
                    .unwrap_or_else(|| evidence.dispatched.clone());
                return Ok(DfsRecordFollowed::ResendDue(record));
            }
            if wake.at.is_some_and(|at| now >= at) {
                return Ok(DfsRecordFollowed::Woken);
            }
            // With nothing due, the wait still wakes at the network's liveness
            // interval and reads the projection again. Nothing is sent then.
            let mut wait = resend
                .as_deref()
                .map_or(DFS_RECORD_FOLLOW_RESEND_MAX, |resend| {
                    resend.due.saturating_duration_since(now)
                });
            for at in [wake.at, deadline].into_iter().flatten() {
                wait = wait.min(at.saturating_duration_since(now));
            }
            watch.wait_for_change(wait).map_err(DfsError::from)?;
        }
    }

    /// A request from this node could not be delivered to `shard`'s primary:
    /// ask this node's candidate role to have the network verify it. The
    /// demand is a hint, kept in memory by the Main that runs the keeper.
    /// Each undelivered attempt posts it once: the first attempt against a
    /// record, and each attempt the re-send timer makes while that record
    /// stands. A wait posts nothing.
    pub(crate) fn request_primary_verification(&mut self, shard: &DfsMetadataShardConfig) {
        if shard.primary_node_id == self.node_id {
            return;
        }
        if self.transport.delegates_local_namespace_authority() {
            // The keeper runs in this node's Main, which may be another
            // process: the hint travels as a DFS message to this node. It is
            // posted without waiting for Main's answer, and a lost one is
            // posted again with the next attempt.
            let node = self.node_id.clone();
            let message = self.next_dfs_routed_message(
                &node,
                crate::DfsNetworkMessage::PrimaryVerificationRequest(
                    crate::DfsPrimaryVerificationRequest {
                        file_system_id: shard.file_system_id.clone(),
                        shard_id: shard.shard_id.clone(),
                        epoch: shard.epoch,
                        primary_node_id: shard.primary_node_id.clone(),
                    },
                ),
            );
            let _ = self.transport.post_hint_to_local_main(message);
            return;
        }
        post_primary_unreachable(&self.node_id, shard);
    }

    /// A read-only probe could not be delivered to `shard`'s primary. Ask for
    /// the primary's verification; a requester that follows record changes
    /// then waits for the record to change or for the re-send timer. Returns
    /// whether the probe is sent again, to the primary the projection names
    /// then.
    pub(crate) fn follow_undelivered_probe(
        &mut self,
        shard: &DfsMetadataShardConfig,
        resend: &mut DfsRecordFollowResend,
    ) -> Result<bool, DfsError> {
        self.request_primary_verification(shard);
        if !self.follows_record_changes() {
            return Ok(false);
        }
        let evidence = DfsRecordChangeEvidence {
            dispatched: shard.clone(),
            floor_epoch: None,
        };
        Ok(!matches!(
            self.await_newer_shard_record(&evidence, resend)?,
            DfsRecordFollowed::Ended
        ))
    }

    /// Serve a requester's hint that it could not deliver to a shard's
    /// primary, for a service that receives one itself. A node's Main hears
    /// the hint at its ingress, before any admission.
    pub(crate) fn handle_primary_verification_request(
        &mut self,
        source_node_id: &NodeId,
        request: &crate::DfsPrimaryVerificationRequest,
    ) {
        let projected = self
            .projected_shard_record(&request.file_system_id, &request.shard_id)
            .ok()
            .flatten();
        hear_primary_verification_hint(&self.node_id, source_node_id, request, projected.as_ref());
    }

    /// The primary this requester's projection names for `operation` now,
    /// when it is not `attempted`. A caller that does not wait goes on only
    /// when the record already moved.
    pub(crate) fn primary_after_record_change(
        &mut self,
        file_system_id: &DfsFileSystemId,
        operation: &DfsNamespaceOperation,
        attempted: &NodeId,
        undelivered: bool,
    ) -> Option<NodeId> {
        let shards = self
            .namespace_operation_effect_authority_shards(file_system_id, operation)
            .ok()?;
        let current = Self::namespace_operation_dispatch_shard(file_system_id, operation, &shards)
            .ok()
            .flatten()?;
        if &current.primary_node_id != attempted {
            return Some(current.primary_node_id);
        }
        if undelivered {
            self.request_primary_verification(&current);
        }
        None
    }

    /// Route one namespace mutation and follow its shard's record until the
    /// primary answers it definitely.
    ///
    /// The same `request_id` and body are sent on every attempt, so an
    /// attempt that may have applied is sent again: the primary answers a
    /// repeat with what the first one did.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn route_namespace_following_record(
        &mut self,
        request_id: &str,
        file_system_id: &DfsFileSystemId,
        primary_node_id: NodeId,
        operation: &DfsNamespaceOperation,
        timeout: Option<Duration>,
        replay_journal_batches: bool,
        apply_synced_path_object: bool,
    ) -> Result<DfsFollowedNamespaceRoute, DfsError> {
        self.route_namespace_following_record_with(
            Some(request_id),
            file_system_id,
            primary_node_id,
            operation,
            timeout,
            replay_journal_batches,
            apply_synced_path_object,
        )
    }

    /// Route one read. Each attempt has an id of its own; a read changes
    /// nothing, so every attempt may be sent again. Its budget is the read's
    /// own `timeout`.
    pub(crate) fn route_namespace_read_following_record(
        &mut self,
        file_system_id: &DfsFileSystemId,
        primary_node_id: NodeId,
        operation: &DfsNamespaceOperation,
        timeout: Option<Duration>,
    ) -> Result<DfsFollowedNamespaceRoute, DfsError> {
        self.route_namespace_following_record_with(
            None,
            file_system_id,
            primary_node_id,
            operation,
            timeout,
            false,
            false,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn route_namespace_following_record_with(
        &mut self,
        request_id: Option<&str>,
        file_system_id: &DfsFileSystemId,
        primary_node_id: NodeId,
        operation: &DfsNamespaceOperation,
        timeout: Option<Duration>,
        replay_journal_batches: bool,
        apply_synced_path_object: bool,
    ) -> Result<DfsFollowedNamespaceRoute, DfsError> {
        // One budget covers every attempt and every wait between them.
        let previous_deadline = self.transport.operation_deadline();
        let deadline = match (
            timeout.and_then(|timeout| Instant::now().checked_add(timeout)),
            previous_deadline,
        ) {
            (Some(own), Some(existing)) => Some(own.min(existing)),
            (deadline, None) | (None, deadline) => deadline,
        };
        self.transport.set_operation_deadline(deadline);
        let result = self.route_namespace_following_record_within_deadline(
            request_id,
            file_system_id,
            primary_node_id,
            operation,
            replay_journal_batches,
            apply_synced_path_object,
        );
        self.transport.set_operation_deadline(previous_deadline);
        result
    }

    fn route_namespace_following_record_within_deadline(
        &mut self,
        request_id: Option<&str>,
        file_system_id: &DfsFileSystemId,
        primary_node_id: NodeId,
        operation: &DfsNamespaceOperation,
        replay_journal_batches: bool,
        apply_synced_path_object: bool,
    ) -> Result<DfsFollowedNamespaceRoute, DfsError> {
        if let Some(id) = request_id {
            crate::service_routed_namespace::validate_namespace_operation_id(id)?;
        }
        let mut primary = primary_node_id;
        let mut resend = DfsRecordFollowResend::new();
        let mut unresolved = None;
        let ended = |left| match left {
            NamespaceAttemptEvidence::Rejected(routed) => {
                Ok(DfsFollowedNamespaceRoute::Settled(routed))
            }
            NamespaceAttemptEvidence::Failed(error) => Err(error),
        };
        loop {
            let attempt_id = request_id.map_or_else(|| self.next_request_id(), str::to_owned);
            let (command, pending) = match self.prepare_projected_namespace_request(
                &attempt_id,
                file_system_id.clone(),
                &primary,
                operation.clone(),
                replay_journal_batches,
                apply_synced_path_object,
            ) {
                Ok(prepared) => prepared,
                Err(error) => {
                    return ended(unresolved.unwrap_or(NamespaceAttemptEvidence::Failed(error)));
                }
            };
            if pending.primary_node_id != primary
                && pending.primary_node_id == self.node_id
                && !self.transport.delegates_local_namespace_authority()
            {
                return Ok(DfsFollowedNamespaceRoute::LocalAuthority);
            }
            primary.clone_from(&pending.primary_node_id);
            let dispatched = pending.dispatch_authority_shard.clone();
            let deadline = self.transport.operation_deadline();
            let attempt = self.dispatch_prepared_namespace_request(command, pending, deadline);
            // Each arm is what the attempt left, the lowest epoch it names
            // and whether it was undelivered. A typed definite refusal that
            // is neither an unknown outcome nor a refusal for the request's
            // authority is the caller's answer: nothing waits on it.
            let (left, floor_epoch, undelivered) = match attempt {
                Ok(routed) => match &routed.result {
                    DfsNamespaceResult::Applied => {
                        return Ok(DfsFollowedNamespaceRoute::Settled(routed));
                    }
                    DfsNamespaceResult::Rejected {
                        posix_error:
                            Some(DfsNamespacePosixError::CommitOutcomeUnknown { cause, .. }),
                        ..
                    } => {
                        let floor = match cause {
                            crate::DfsCommitOutcomeUnknownCause::RecordChanged { epoch } => {
                                Some(*epoch)
                            }
                            _ => None,
                        };
                        (NamespaceAttemptEvidence::Rejected(routed), floor, false)
                    }
                    DfsNamespaceResult::Rejected {
                        current_primary_node_id: Some(_),
                        ..
                    } => {
                        // A refusal for the request's authority names the
                        // primary of the refuser's record, which may be the
                        // refuser itself at another epoch. It ran nothing.
                        (NamespaceAttemptEvidence::Rejected(routed), None, false)
                    }
                    DfsNamespaceResult::Rejected { .. } => {
                        return Ok(DfsFollowedNamespaceRoute::Settled(routed));
                    }
                },
                Err(
                    error @ DfsError::NetworkDelivery {
                        delivery_state: NetworkDeliveryState::DefinitelyNotSent,
                        ..
                    },
                ) => (NamespaceAttemptEvidence::Failed(error), None, true),
                Err(error @ DfsError::Network { .. }) if self.remote_namespace_phase.is_some() => {
                    return Err(error);
                }
                // A shard whose storage is closed refused before any effect.
                // The refusal crosses a node boundary as the text of a failed
                // call; it is the request's answer, not a failed delivery.
                Err(error) if crate::error::dfs_error_is_closed_shard_storage(&error) => {
                    return ended(unresolved.unwrap_or(NamespaceAttemptEvidence::Failed(error)));
                }
                Err(DfsError::Network { message }) => (
                    NamespaceAttemptEvidence::Failed(DfsError::NetworkDelivery {
                        message,
                        delivery_state: NetworkDeliveryState::PossiblyApplied,
                    }),
                    None,
                    true,
                ),
                Err(error @ DfsError::NetworkDelivery { .. }) => {
                    (NamespaceAttemptEvidence::Failed(error), None, true)
                }
                Err(error) => {
                    return ended(unresolved.unwrap_or(NamespaceAttemptEvidence::Failed(error)));
                }
            };
            // An unsent retry or an authority refusal cannot settle an earlier dispatch.
            if request_id.is_some()
                && matches!(
                    &left,
                    NamespaceAttemptEvidence::Failed(DfsError::NetworkDelivery {
                        delivery_state: NetworkDeliveryState::PossiblyApplied,
                        ..
                    }) | NamespaceAttemptEvidence::Rejected(PendingRoutedNamespaceResult {
                        result: DfsNamespaceResult::Rejected {
                            posix_error: Some(DfsNamespacePosixError::CommitOutcomeUnknown { .. }),
                            ..
                        },
                        ..
                    })
                )
            {
                unresolved = Some(left.clone());
            }
            let Some(dispatched) = dispatched else {
                // The request went to a primary this requester's projection
                // does not name for the operation: there is no record of its
                // own to follow.
                if undelivered
                    && let Some(path) = operation.routing_path()
                    && let Ok(Some(shard)) = self.projected_namespace_shard_for_path(path)
                {
                    self.request_primary_verification(&shard);
                }
                return ended(unresolved.unwrap_or(left));
            };
            let evidence = DfsRecordChangeEvidence {
                dispatched,
                floor_epoch,
            };
            if undelivered {
                self.request_primary_verification(&evidence.dispatched);
            }
            let followed = match self.await_newer_shard_record(&evidence, &mut resend) {
                Ok(followed) => followed,
                Err(error) if error.is_cancellation() => return Err(error),
                Err(error) => {
                    return ended(unresolved.unwrap_or(NamespaceAttemptEvidence::Failed(error)));
                }
            };
            match followed {
                DfsRecordFollowed::Newer(record) => {
                    if record.primary_node_id == self.node_id
                        && !self.transport.delegates_local_namespace_authority()
                    {
                        return Ok(DfsFollowedNamespaceRoute::LocalAuthority);
                    }
                    primary = record.primary_node_id;
                }
                DfsRecordFollowed::ResendDue(record) => primary = record.primary_node_id,
                DfsRecordFollowed::Woken | DfsRecordFollowed::Ended => {
                    return ended(unresolved.unwrap_or(left));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DfsControlPlaneKvClient, DfsRoutedCall, DfsRoutedMessage, LocalContentStore};
    use sinomo_core::kv::{InProcessKvStore, KvEntry, KvError, KvRevision, KvTxn, KvTxnResponse};
    use sinomo_core::network::NetworkError;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Condvar, Mutex};

    /// A delegating requester's transport that sends nothing: it counts the
    /// hints posted to its node's Main and holds what a test makes arrive.
    struct RequesterTransport {
        node: NodeId,
        hints: Arc<AtomicUsize>,
        arrived: Arc<Mutex<Vec<DfsRoutedMessage>>>,
        arrival: Arc<Mutex<Option<crate::service_state::DfsCancellationWaker>>>,
        deadline: Option<Instant>,
    }

    impl crate::DfsTransportIdentity for RequesterTransport {
        fn local_node_id(&self) -> &NodeId {
            &self.node
        }
    }

    impl DfsMessageTransport for RequesterTransport {
        fn delegates_local_namespace_authority(&self) -> bool {
            true
        }
        fn send_routed_call(
            &mut self,
            target_node_id: &NodeId,
            _call: DfsRoutedCall,
        ) -> Result<(), NetworkError> {
            Err(NetworkError::UnknownNode {
                node_id: target_node_id.clone(),
            })
        }
        fn post_hint_to_local_main(
            &mut self,
            _message: DfsRoutedMessage,
        ) -> Result<(), NetworkError> {
            self.hints.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        fn drain_incoming_messages(&mut self) -> Result<Vec<DfsRoutedMessage>, NetworkError> {
            Ok(std::mem::take(&mut *self.arrived.lock().unwrap()))
        }
        fn pending_incoming_count(&self) -> Result<Option<usize>, NetworkError> {
            Ok(Some(self.arrived.lock().unwrap().len()))
        }
        fn set_incoming_arrival_waker(
            &mut self,
            waker: Option<crate::service_state::DfsCancellationWaker>,
        ) {
            *self.arrival.lock().unwrap() = waker;
        }
        fn set_operation_deadline(&mut self, deadline: Option<Instant>) -> Option<Instant> {
            std::mem::replace(&mut self.deadline, deadline)
        }
        fn operation_deadline(&self) -> Option<Instant> {
            self.deadline
        }
    }

    /// The publication a watch waits on: how many times it was replaced, and
    /// one pending wake.
    #[derive(Debug, Default)]
    struct Publication {
        state: Mutex<(u64, bool)>,
        changed: Condvar,
    }

    impl Publication {
        fn replace(&self) {
            self.state.lock().unwrap().0 += 1;
            self.changed.notify_all();
        }
    }

    struct PublicationWatch {
        publication: Arc<Publication>,
        seen: u64,
    }

    impl crate::DfsConfigurationChangeWatch for PublicationWatch {
        fn wait_for_change(&mut self, wait: Duration) -> Result<bool, KvError> {
            let state = self.publication.state.lock().unwrap();
            let seen = self.seen;
            let (mut state, _) = self
                .publication
                .changed
                .wait_timeout_while(state, wait, |state| state.0 == seen && !state.1)
                .unwrap();
            state.1 = false;
            let changed = state.0 != self.seen;
            self.seen = state.0;
            Ok(changed)
        }

        fn waker(&self) -> Arc<dyn Fn() + Send + Sync> {
            let publication = Arc::clone(&self.publication);
            Arc::new(move || {
                publication.state.lock().unwrap().1 = true;
                publication.changed.notify_all();
            })
        }
    }

    /// An in-process configuration whose replacements a requester can watch.
    #[derive(Debug)]
    struct WatchedConfiguration {
        kv: Arc<Mutex<InProcessKvStore>>,
        publication: Arc<Publication>,
    }

    impl DfsControlPlaneKvClient for WatchedConfiguration {
        fn watch_configuration_changes(
            &self,
        ) -> Option<Box<dyn crate::DfsConfigurationChangeWatch>> {
            Some(Box::new(PublicationWatch {
                seen: self.publication.state.lock().unwrap().0,
                publication: Arc::clone(&self.publication),
            }))
        }
        fn get(&self, key: &str) -> Result<Option<KvEntry>, KvError> {
            DfsControlPlaneKvClient::get(&*self.kv.lock().unwrap(), key)
        }
        fn range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError> {
            DfsControlPlaneKvClient::range_prefix(&*self.kv.lock().unwrap(), prefix)
        }
        fn put(&mut self, key: String, value: Vec<u8>) -> Result<KvRevision, KvError> {
            DfsControlPlaneKvClient::put(&mut *self.kv.lock().unwrap(), key, value)
        }
        fn txn(&mut self, txn: KvTxn) -> Result<KvTxnResponse, KvError> {
            DfsControlPlaneKvClient::txn(&mut *self.kv.lock().unwrap(), txn)
        }
    }

    struct Requester {
        service: NetworkedDfsService<RequesterTransport>,
        kv: Arc<Mutex<InProcessKvStore>>,
        publication: Arc<Publication>,
        hints: Arc<AtomicUsize>,
        arrived: Arc<Mutex<Vec<DfsRoutedMessage>>>,
        arrival: Arc<Mutex<Option<crate::service_state::DfsCancellationWaker>>>,
        shard: DfsMetadataShardConfig,
        _root: sinomo_testdir::TestDir,
    }

    impl Requester {
        /// A delegating requester on one node whose shard's primary is another.
        fn new(name: &str) -> Self {
            let root = sinomo_testdir::TestDir::new(&format!("sinomo-dfs-record-follow-{name}"));
            let node = NodeId::from_legacy_named("follow-requester").unwrap();
            let shard = DfsMetadataShardConfig {
                file_system_id: crate::sifs_file_system_id(),
                shard_id: "root".into(),
                primary_node_id: NodeId::from_legacy_named("follow-primary").unwrap(),
                replica_node_ids: Vec::new(),
                epoch: 4,
                assignment: crate::DfsMetadataShardAssignment::Root,
                default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
            };
            let kv = Arc::new(Mutex::new(InProcessKvStore::default()));
            let publication = Arc::new(Publication::default());
            let hints = Arc::new(AtomicUsize::new(0));
            let arrived = Arc::new(Mutex::new(Vec::new()));
            let arrival = Arc::new(Mutex::new(None));
            let service = NetworkedDfsService::new(
                RequesterTransport {
                    node,
                    hints: Arc::clone(&hints),
                    arrived: Arc::clone(&arrived),
                    arrival: Arc::clone(&arrival),
                    deadline: None,
                },
                LocalContentStore::new(root.path().join("content")).unwrap(),
            )
            .with_control_plane(WatchedConfiguration {
                kv: Arc::clone(&kv),
                publication: Arc::clone(&publication),
            });
            let requester = Self {
                service,
                kv,
                publication,
                hints,
                arrived,
                arrival,
                shard,
                _root: root,
            };
            requester.publish(&requester.shard);
            requester
        }

        /// Replace the published configuration with one holding `record`.
        fn publish(&self, record: &DfsMetadataShardConfig) {
            self.kv
                .lock()
                .unwrap()
                .put(
                    crate::metadata_shard_key(&record.file_system_id, &record.shard_id),
                    sinomo_core::kv::encode_kv_value(record).unwrap(),
                    None,
                )
                .unwrap();
            self.publication.replace();
        }

        fn evidence(&self) -> DfsRecordChangeEvidence {
            DfsRecordChangeEvidence {
                dispatched: self.shard.clone(),
                floor_epoch: None,
            }
        }
    }

    /// A wait posts no hint, however often its watch wakes. Only an attempt
    /// that could not be delivered posts one.
    #[test]
    fn a_woken_record_wait_posts_no_primary_verification_hint() {
        let mut requester = Requester::new("wakes");
        assert!(requester.service.follows_record_changes());
        let shard = requester.shard.clone();
        requester.service.request_primary_verification(&shard);
        assert_eq!(requester.hints.load(Ordering::SeqCst), 1);

        // Replacements that leave the record as it is wake the watch.
        let publication = Arc::clone(&requester.publication);
        let waking = std::thread::spawn(move || {
            for _ in 0..40 {
                publication.replace();
                std::thread::sleep(Duration::from_millis(2));
            }
        });
        let mut resend = DfsRecordFollowResend::new();
        let evidence = requester.evidence();
        let followed = requester
            .service
            .await_newer_shard_record(&evidence, &mut resend)
            .unwrap();
        waking.join().unwrap();
        assert!(
            matches!(&followed, DfsRecordFollowed::ResendDue(record) if record == &shard),
            "an unchanged record ends the wait only at the re-send timer: {followed:?}"
        );
        assert_eq!(
            requester.hints.load(Ordering::SeqCst),
            1,
            "the wait itself posted nothing"
        );
        // The attempt the timer makes posts the next one.
        requester.service.request_primary_verification(&shard);
        assert_eq!(requester.hints.load(Ordering::SeqCst), 2);
    }

    /// The projection showing a newer record ends the wait with that record,
    /// well before the re-send timer.
    #[test]
    fn a_newer_record_ends_the_wait_with_the_record_it_names() {
        let mut requester = Requester::new("newer");
        let mut newer = requester.shard.clone();
        newer.epoch += 1;
        newer.primary_node_id = NodeId::from_legacy_named("follow-successor").unwrap();
        let (kv, publication, record) = (
            Arc::clone(&requester.kv),
            Arc::clone(&requester.publication),
            newer.clone(),
        );
        let changing = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(30));
            kv.lock()
                .unwrap()
                .put(
                    crate::metadata_shard_key(&record.file_system_id, &record.shard_id),
                    sinomo_core::kv::encode_kv_value(&record).unwrap(),
                    None,
                )
                .unwrap();
            publication.replace();
        });
        let evidence = requester.evidence();
        let followed = requester
            .service
            .await_shard_record_change(&evidence, None, DfsRecordWaitWake::default())
            .unwrap();
        changing.join().unwrap();
        assert!(
            matches!(&followed, DfsRecordFollowed::Newer(record) if record == &newer),
            "{followed:?}"
        );
    }

    /// A message that arrives for the requester ends a wait whose owner asked
    /// for it, at once and not at the next timer: the owner reads the message
    /// before anything is sent again. A wait that did not ask stays.
    #[test]
    fn an_arriving_message_ends_the_wait_its_owner_asked_to_be_woken_from() {
        let mut requester = Requester::new("arrival");
        let message = crate::build_dfs_routed_message(
            &requester.shard.primary_node_id,
            &NodeId::from_legacy_named("follow-requester").unwrap(),
            1,
            1,
            crate::DfsNetworkMessage::PrimaryVerificationRequest(
                crate::DfsPrimaryVerificationRequest {
                    file_system_id: requester.shard.file_system_id.clone(),
                    shard_id: requester.shard.shard_id.clone(),
                    epoch: requester.shard.epoch,
                    primary_node_id: requester.shard.primary_node_id.clone(),
                },
            ),
        );
        let (arrived, arrival) = (
            Arc::clone(&requester.arrived),
            Arc::clone(&requester.arrival),
        );
        let delivering = std::thread::spawn(move || {
            // Deliver once the wait has installed its arrival waker.
            let started = Instant::now();
            loop {
                if let Some(waker) = arrival.lock().unwrap().clone() {
                    arrived.lock().unwrap().push(message);
                    waker();
                    return;
                }
                assert!(started.elapsed() < Duration::from_secs(30));
                std::thread::sleep(Duration::from_millis(1));
            }
        });
        let evidence = requester.evidence();
        let started = Instant::now();
        let followed = requester
            .service
            .await_shard_record_change(
                &evidence,
                None,
                DfsRecordWaitWake {
                    on_arrival: true,
                    at: None,
                },
            )
            .unwrap();
        delivering.join().unwrap();
        assert!(matches!(followed, DfsRecordFollowed::Woken), "{followed:?}");
        assert!(
            started.elapsed() < DFS_RECORD_FOLLOW_RESEND_MAX,
            "the arrival, not a timer, ended the wait: {:?}",
            started.elapsed()
        );
        assert!(
            requester.arrival.lock().unwrap().is_none(),
            "the wait took its arrival waker with it"
        );
        // The message is still the owner's to read.
        assert_eq!(requester.arrived.lock().unwrap().len(), 1);
    }

    /// The owner's own timer ends its wait; the caller's deadline ends it for
    /// good.
    #[test]
    fn the_owners_timer_and_the_callers_deadline_end_the_wait() {
        let mut requester = Requester::new("timers");
        let evidence = requester.evidence();
        let woken = requester
            .service
            .await_shard_record_change(
                &evidence,
                None,
                DfsRecordWaitWake {
                    on_arrival: true,
                    at: Some(Instant::now() + Duration::from_millis(40)),
                },
            )
            .unwrap();
        assert!(matches!(woken, DfsRecordFollowed::Woken), "{woken:?}");
        requester
            .service
            .transport
            .set_operation_deadline(Some(Instant::now() + Duration::from_millis(40)));
        let ended = requester
            .service
            .await_shard_record_change(&evidence, None, DfsRecordWaitWake::default())
            .unwrap();
        assert!(matches!(ended, DfsRecordFollowed::Ended), "{ended:?}");
    }

    /// A hint is heard only from this node's own requesters and only about
    /// the record this node still projects.
    #[test]
    fn a_hint_is_heard_only_from_this_node_about_the_record_it_projects() {
        let local = NodeId::from_legacy_named("hint-local").unwrap();
        let other = NodeId::from_legacy_named("hint-other").unwrap();
        let shard = DfsMetadataShardConfig {
            file_system_id: crate::sifs_file_system_id(),
            shard_id: "hint-shard".into(),
            primary_node_id: NodeId::from_legacy_named("hint-primary").unwrap(),
            replica_node_ids: Vec::new(),
            epoch: 7,
            assignment: crate::DfsMetadataShardAssignment::Root,
            default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
        };
        let request = crate::DfsPrimaryVerificationRequest {
            file_system_id: shard.file_system_id.clone(),
            shard_id: shard.shard_id.clone(),
            epoch: shard.epoch,
            primary_node_id: shard.primary_node_id.clone(),
        };
        let demanded = || {
            crate::write_set_keeping_signals::take_commit_demands(&local)
                .into_values()
                .any(|demands| demands.primary_unreachable)
        };
        assert!(!hear_primary_verification_hint(
            &local,
            &other,
            &request,
            Some(&shard)
        ));
        assert!(!hear_primary_verification_hint(
            &local, &local, &request, None
        ));
        let mut moved = shard.clone();
        moved.epoch += 1;
        assert!(!hear_primary_verification_hint(
            &local,
            &local,
            &request,
            Some(&moved)
        ));
        assert!(!demanded(), "a refused hint leaves no demand");
        assert!(hear_primary_verification_hint(
            &local,
            &local,
            &request,
            Some(&shard)
        ));
        assert!(demanded(), "a heard hint leaves the keeper its demand");
    }
}
