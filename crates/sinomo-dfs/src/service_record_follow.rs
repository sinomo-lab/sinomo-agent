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
    /// The request did not reach the primary: this node's candidate role is
    /// asked to have the network verify it.
    pub(crate) undelivered: bool,
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
    /// This requester does not wait, or its caller's budget ended.
    Ended,
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
        let wake = watch.waker();
        let cancellation_waker: crate::service_state::DfsCancellationWaker =
            Arc::new(move || wake());
        let _cancellation_wake = self.register_cancellation_waker(&cancellation_waker);
        loop {
            self.check_cancelled("shard record wait")?;
            if deadline.is_some_and(|at| Instant::now() >= at) {
                return Ok(DfsRecordFollowed::Ended);
            }
            let current = self.current_dispatch_record(evidence);
            if let Some(record) = current
                .as_ref()
                .filter(|record| evidence.satisfied_by(record))
            {
                resend.restart();
                return Ok(DfsRecordFollowed::Newer(record.clone()));
            }
            let now = Instant::now();
            if deadline.is_some_and(|deadline| now >= deadline) {
                return Ok(DfsRecordFollowed::Ended);
            }
            if now >= resend.due {
                resend.rearm();
                // A membership change can keep both epoch and primary. Reuse
                // the operation body, but take the stamp from our current
                // projection. An older projection cannot downgrade a write.
                let record = current
                    .filter(|record| record.epoch == evidence.dispatched.epoch)
                    .unwrap_or_else(|| evidence.dispatched.clone());
                return Ok(DfsRecordFollowed::ResendDue(record));
            }
            if evidence.undelivered {
                self.request_primary_verification(&evidence.dispatched);
            }
            let mut wait = resend.due.saturating_duration_since(now);
            if let Some(deadline) = deadline {
                wait = wait.min(deadline.saturating_duration_since(now));
            }
            watch.wait_for_change(wait).map_err(DfsError::from)?;
        }
    }

    /// A request from this node could not be delivered to `shard`'s primary:
    /// ask this node's candidate role to have the network verify it. The
    /// demand is a hint, kept in memory by the Main that runs the keeper;
    /// every later attempt posts it again.
    pub(crate) fn request_primary_verification(&mut self, shard: &DfsMetadataShardConfig) {
        if shard.primary_node_id == self.node_id {
            return;
        }
        if self.transport.delegates_local_namespace_authority() {
            // The keeper runs in this node's Main, which may be another
            // process: the hint travels as a DFS message to this node. It
            // needs no reply, and a lost one is sent again with the next
            // attempt.
            let node = self.node_id.clone();
            let _ = self.send_dfs_message_if_routable(
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
            return;
        }
        Self::post_primary_unreachable(&self.node_id, shard);
    }

    fn post_primary_unreachable(local: &NodeId, shard: &DfsMetadataShardConfig) {
        crate::write_set_keeping_signals::post_commit_demand(
            local,
            &shard.file_system_id,
            &crate::DfsMetadataShardId::new(shard.shard_id.clone()),
            crate::write_set_keeping_signals::DfsWriteSetCommitDemand::PrimaryUnreachable,
        );
    }

    /// Serve a requester's hint that it could not deliver to a shard's
    /// primary. Only this node's own requesters are heard, and only about the
    /// record this node still projects.
    pub(crate) fn handle_primary_verification_request(
        &mut self,
        source_node_id: &NodeId,
        request: &crate::DfsPrimaryVerificationRequest,
    ) {
        if source_node_id != &self.node_id {
            return;
        }
        let current = self
            .projected_shard_record(&request.file_system_id, &request.shard_id)
            .ok()
            .flatten();
        if let Some(current) = current.filter(|current| {
            current.epoch == request.epoch && current.primary_node_id == request.primary_node_id
        }) {
            Self::post_primary_unreachable(&self.node_id, &current);
        }
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

    /// Route one namespace request and follow its shard's record until the
    /// primary answers it definitely.
    ///
    /// With `request_id` the same id and body are sent on every attempt, so
    /// an attempt that may have applied is sent again. Without one each
    /// attempt has a new id, and only an attempt that was not delivered, or
    /// was refused for its authority, is sent again.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn route_namespace_following_record(
        &mut self,
        request_id: Option<&str>,
        file_system_id: &DfsFileSystemId,
        primary_node_id: NodeId,
        operation: &DfsNamespaceOperation,
        timeout: Option<Duration>,
        replay_journal_batches: bool,
        apply_synced_path_object: bool,
    ) -> Result<DfsFollowedNamespaceRoute, DfsError> {
        self.route_namespace_following_record_with(
            request_id,
            file_system_id,
            primary_node_id,
            operation,
            timeout,
            replay_journal_batches,
            apply_synced_path_object,
            request_id.is_some(),
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
            true,
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
        exact_resend: bool,
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
            exact_resend,
        );
        self.transport.set_operation_deadline(previous_deadline);
        result
    }

    #[allow(clippy::too_many_arguments)]
    fn route_namespace_following_record_within_deadline(
        &mut self,
        request_id: Option<&str>,
        file_system_id: &DfsFileSystemId,
        primary_node_id: NodeId,
        operation: &DfsNamespaceOperation,
        replay_journal_batches: bool,
        apply_synced_path_object: bool,
        exact_resend: bool,
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
            // Each arm is what the attempt left, the lowest epoch it names,
            // whether it was undelivered, and whether a re-send is exact.
            let (left, floor_epoch, undelivered, resendable) = match attempt {
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
                        (
                            NamespaceAttemptEvidence::Rejected(routed),
                            floor,
                            false,
                            exact_resend,
                        )
                    }
                    DfsNamespaceResult::Rejected {
                        current_primary_node_id: Some(_),
                        ..
                    } => {
                        // A refusal for the request's authority names the
                        // primary of the refuser's record, which may be the
                        // refuser itself at another epoch. It ran nothing,
                        // so sending again is exact.
                        (
                            NamespaceAttemptEvidence::Rejected(routed),
                            None,
                            false,
                            true,
                        )
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
                ) => (NamespaceAttemptEvidence::Failed(error), None, true, true),
                Err(error @ DfsError::Network { .. }) if self.remote_namespace_phase.is_some() => {
                    return Err(error);
                }
                Err(DfsError::Network { message }) => (
                    NamespaceAttemptEvidence::Failed(DfsError::NetworkDelivery {
                        message,
                        delivery_state: NetworkDeliveryState::PossiblyApplied,
                    }),
                    None,
                    true,
                    exact_resend,
                ),
                Err(error @ DfsError::NetworkDelivery { .. }) => (
                    NamespaceAttemptEvidence::Failed(error),
                    None,
                    true,
                    exact_resend,
                ),
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
                undelivered,
            };
            if undelivered {
                self.request_primary_verification(&evidence.dispatched);
            }
            if !resendable {
                // An attempt under an id of its own that may have applied is
                // never repeated under another.
                return ended(unresolved.unwrap_or(left));
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
                DfsRecordFollowed::Ended => return ended(unresolved.unwrap_or(left)),
            }
        }
    }
}
