//! Native local-fence RPC observations; no Promise/Chosen/serving inference.
use super::*;
use crate::runtime_host::{
    OwnedSelectedLocalRecoveryFenceReturn, call_native_selected_local_recovery_fence,
};
use crate::test_runtime::{FaultAction, FaultRule, RpcOrigin, RpcPhase};
use crate::{
    DfsError, DfsMessageCommand, DfsMessageResponse, DfsNetworkMessage,
    DfsSelectedLocalRecoveryFenceRpcOutcome as Outcome,
    DfsSelectedLocalRecoveryFenceRpcRequest as RpcRequest,
};
use sinomo_core::node::NodeId;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

// Test carrier correlation only, never a logical reservation/effect identity.
static NEXT_RAW_CARRIER: AtomicU64 = AtomicU64::new(1_u64 << 60);

fn with_rpc(
    name: &str,
    selected: bool,
    check: impl FnOnce(&mut NativeFenceFixture, &RpcRequest, &mut Service),
) {
    with_native(name, selected, |fixture| {
        fixture.no_cooperative_marker();
        let target = fixture.shard.replica_node_ids[1].clone();
        assert_ne!(fixture.acceptor, target);
        let layout = if selected {
            Some(
                fixture
                    .runtime
                    .selected_original_data_layout(&target)
                    .unwrap(),
            )
        } else {
            None
        };
        assert!(
            crate::local_state::read_local_dfs_metadata_shard_write_freeze_with_layout(
                fixture.runtime.data_dir(&target),
                &fixture.shard_id(),
                layout.as_deref(),
            )
            .unwrap()
            .is_none()
        );
        let mut receiver = fixture.runtime.client(&target).unwrap();
        // Open/retain this actual local scope BEFORE it becomes fenced. Tests
        // never follow a remote_reported path or assert cold Main readiness.
        receiver
            .metadata_backend()
            .unwrap()
            .publication_owner_scope(&fixture.shard.file_system_id, &fixture.shard_id())
            .unwrap();
        let reservation = fixture.request();
        fixture.reserve(&reservation);
        let request = RpcRequest {
            reservation,
            expected_target: target,
        };
        check(fixture, &request, &mut receiver);
    });
}

fn call(
    fixture: &NativeFenceFixture,
    request: &RpcRequest,
    cut: Instant,
) -> Result<OwnedSelectedLocalRecoveryFenceReturn, DfsError> {
    call_native_selected_local_recovery_fence(
        fixture
            .runtime
            .data_dir(&request.reservation.ballot.candidate_node_id),
        &fixture.runtime.runtime_handle(),
        request,
        cut,
    )
}

fn error<T>(result: Result<T, DfsError>) -> DfsError {
    match result {
        Err(error) => error,
        Ok(_) => panic!("unexpected owned native return"),
    }
}

fn local_data(
    receiver: &Service,
    fixture: &NativeFenceFixture,
) -> Option<DfsSelectedLocalRecoveryFenceData> {
    receiver
        .metadata_backend()
        .unwrap()
        .selected_local_recovery_fence(&fixture.shard_id())
        .unwrap()
}

fn absent_marker(fixture: &NativeFenceFixture, target: &NodeId) {
    let path =
        crate::local_dfs_metadata_shard_path(fixture.runtime.data_dir(target), &fixture.shard_id())
            .join("journal/membership-handoff-write-freeze.sobj");
    assert_eq!(
        std::fs::symlink_metadata(path).unwrap_err().kind(),
        std::io::ErrorKind::NotFound
    );
}

fn assert_reported(
    actual: &OwnedSelectedLocalRecoveryFenceReturn,
    request: &RpcRequest,
    local: &DfsSelectedLocalRecoveryFenceData,
) {
    assert_eq!(actual.request(), request);
    assert_eq!(
        actual.actual_carrier().source_node_id,
        request.reservation.ballot.candidate_node_id
    );
    assert_eq!(
        actual.actual_carrier().target_node_id,
        request.expected_target
    );
    assert!(!actual.outer_request_id().is_empty());
    assert_eq!(
        actual.actual_carrier().message,
        DfsNetworkMessage::SelectedLocalRecoveryFenceRequest(request.clone())
    );
    assert_eq!(actual.remote_reported_fence(), &local.fence);
    assert_eq!(
        actual.remote_reported_marker_digest(),
        blake3::hash(&local.marker_bytes).to_hex().as_str()
    );
    assert_eq!(
        actual.remote_reported_marker_bytes(),
        local.marker_bytes.len() as u64
    );
}

fn raw_command(source: &NodeId, target: &NodeId, message: DfsNetworkMessage) -> DfsMessageCommand {
    let frame = NEXT_RAW_CARRIER.fetch_add(1, Ordering::Relaxed);
    DfsMessageCommand {
        request_id: format!("native-local-fence-probe-{frame}"),
        routed_message: crate::build_dfs_routed_message(source, target, frame, frame, message),
    }
}

fn raw_native(
    fixture: &NativeFenceFixture,
    source: &NodeId,
    command: &DfsMessageCommand,
    lose: bool,
    cut: Instant,
) -> Result<DfsMessageResponse, String> {
    assert_eq!(source, &command.routed_message.source_node_id);
    let target = &command.routed_message.target_node_id;
    let host = crate::runtime_host::registered_dfs_runtime_host(fixture.runtime.data_dir(source))
        .unwrap()
        .unwrap();
    let previous = fixture
        .runtime
        .rpc_observations()
        .into_iter()
        .map(|item| item.call_id)
        .collect::<BTreeSet<_>>();
    let rpc = crate::dfs_message_rpc_call(command).unwrap();
    let route = rpc.request.route.clone();
    let pending = host.object_rpc.call_async(
        target,
        rpc,
        crate::DfsObjectRpcSendOptions::default()
            .with_overall_timeout(cut.saturating_duration_since(Instant::now()))
            .cluster_rpc_options(),
        true,
    );
    if lose {
        let constructed = fixture
            .runtime
            .rpc_observations()
            .into_iter()
            .filter(|item| {
                item.source == *source
                    && item.target == *target
                    && item.route == route
                    && item.origin == RpcOrigin::RegisteredHost
                    && item.phase == RpcPhase::Constructed
                    && !previous.contains(&item.call_id)
            })
            .collect::<Vec<_>>();
        assert_eq!(constructed.len(), 1);
        fixture.runtime.install_fault(FaultRule {
            source: Some(source.clone()),
            target: Some(target.clone()),
            route: Some(route),
            origin: Some(RpcOrigin::RegisteredHost),
            call_id: Some(constructed[0].call_id),
            remaining_hits: 1,
            action: FaultAction::LoseReplyAfterDispatch,
        });
    }
    let result = fixture.runtime.runtime_handle().block_on(async {
        tokio::time::timeout_at(tokio::time::Instant::from_std(cut), async {
            let reply = pending.await.map_err(|error| error.to_string())?;
            let value = reply
                .into_materialized_value_response(2 * 1024 * 1024)
                .await
                .map_err(|error| error.to_string())?;
            crate::decode_dfs_message_rpc_response(&value).map_err(|error| error.to_string())
        })
        .await
        .map_err(|_| "original five-second raw native RPC deadline expired".to_string())?
    });
    if lose {
        fixture.runtime.clear_faults();
    }
    if Instant::now() > cut {
        return Err(
            "original five-second native RPC deadline expired before accepting return".into(),
        );
    }
    result
}

fn response<'a>(
    command: &DfsMessageCommand,
    result: &'a DfsMessageResponse,
) -> &'a crate::DfsSelectedLocalRecoveryFenceRpcResponse {
    assert_eq!(result.request_id, command.request_id);
    assert_eq!(result.outbound_messages.len(), 1);
    let frame = &result.outbound_messages[0];
    assert_eq!(frame.source_node_id, command.routed_message.target_node_id);
    assert_eq!(frame.target_node_id, command.routed_message.source_node_id);
    assert_eq!(frame.frame_id, command.routed_message.frame_id);
    assert_eq!(frame.message_id, command.routed_message.message_id);
    let DfsNetworkMessage::SelectedLocalRecoveryFenceResponse(reply) = &frame.message else {
        panic!("wrong native response family")
    };
    reply
}

#[test]
fn native_local_fence_rpc_candidate_to_eligible_replica_and_duplicate_are_reported_data() {
    with_rpc(
        "native-local-fence-rpc-positive",
        true,
        |fixture, request, receiver| {
            let serving = fixture.serving_cells();
            let before = exact_frames(&fixture.runtime, &request.expected_target, &fixture.shard);
            let actual = call(fixture, request, deadline()).unwrap();
            let local = local_data(receiver, fixture).unwrap();
            assert_reported(&actual, request, &local);
            let duplicate = call(fixture, request, deadline()).unwrap();
            assert_reported(&duplicate, request, &local);
            same_data(&local_data(receiver, fixture).unwrap(), &local);
            fixture.no_cooperative_marker();
            assert_eq!(
                exact_frames(&fixture.runtime, &request.expected_target, &fixture.shard),
                before
            );
            assert_eq!(fixture.serving_cells(), serving);
            assert!(
                fixture
                    .runtime
                    .rpc_observations()
                    .iter()
                    .any(
                        |item| item.source == request.reservation.ballot.candidate_node_id
                            && item.target == request.expected_target
                            && item.origin == RpcOrigin::RegisteredHost
                            && item.phase == RpcPhase::Completed
                    )
            );
            assert_eq!(fixture.runtime.observation_dropped_count(), 0);
            // Explicit duplicate re-certification is activity, not a quiet window.
            drop(duplicate);
            drop(actual);
        },
    );
}

#[test]
fn native_local_fence_rpc_refuses_actual_wrong_source_target_and_request_before_marker() {
    with_rpc(
        "native-local-fence-rpc-refusals",
        true,
        |fixture, request, receiver| {
            let primary = fixture.shard.primary_node_id.clone();
            let candidate = request.reservation.ballot.candidate_node_id.clone();
            let before = exact_frames(&fixture.runtime, &request.expected_target, &fixture.shard);
            let serving = fixture.serving_cells();
            let mut wrong_target = request.clone();
            wrong_target.expected_target = candidate.clone();
            let mut wrong_request = request.clone();
            wrong_request
                .reservation
                .reservation_id
                .push_str("-changed");
            for (source, wire, reason) in [
                (&primary, request, "candidate/carrier/expected target"),
                (
                    &candidate,
                    &wrong_target,
                    "candidate/carrier/expected target",
                ),
                (&candidate, &wrong_request, "reservation"),
            ] {
                let command = raw_command(
                    source,
                    &request.expected_target,
                    DfsNetworkMessage::SelectedLocalRecoveryFenceRequest(wire.clone()),
                );
                let returned = raw_native(fixture, source, &command, false, deadline()).unwrap();
                let reply = response(&command, &returned);
                assert_eq!(reply.reservation, wire.reservation);
                let Outcome::Refused { reason: actual } = &reply.outcome else {
                    panic!("actual authenticated refusal required: {:?}", reply.outcome)
                };
                assert!(actual.contains(reason), "{actual}");
                assert!(local_data(receiver, fixture).is_none());
                assert_eq!(
                    exact_frames(&fixture.runtime, &request.expected_target, &fixture.shard),
                    before
                );
            }
            let expired = error(call(fixture, request, Instant::now()));
            assert!(expired.to_string().contains("deadline"));
            let actual_wrong_owner = error(call_native_selected_local_recovery_fence(
                fixture.runtime.data_dir(&primary),
                &fixture.runtime.runtime_handle(),
                request,
                deadline(),
            ));
            assert!(actual_wrong_owner.to_string().contains("candidate"));
            assert!(local_data(receiver, fixture).is_none());
            fixture.no_cooperative_marker();
            assert_eq!(fixture.serving_cells(), serving);
        },
    );
}

#[test]
fn native_local_fence_rpc_lost_real_reply_recall_keeps_logical_subject_and_marker() {
    with_rpc(
        "native-local-fence-rpc-lost-reply",
        true,
        |fixture, request, receiver| {
            let candidate = &request.reservation.ballot.candidate_node_id;
            let command = raw_command(
                candidate,
                &request.expected_target,
                DfsNetworkMessage::SelectedLocalRecoveryFenceRequest(request.clone()),
            );
            let lost = raw_native(fixture, candidate, &command, true, deadline()).unwrap_err();
            assert!(
                lost.contains("PossiblyApplied"),
                "actual dispatch/reply loss: {lost}"
            );
            let local = local_data(receiver, fixture).unwrap();
            assert_eq!(local.fence.reservation, request.reservation);
            assert_eq!(local.fence.acceptor_node_id, request.expected_target);
            let actual = call(fixture, request, deadline()).unwrap();
            assert_reported(&actual, request, &local);
            same_data(&local_data(receiver, fixture).unwrap(), &local);
            assert_eq!(fixture.runtime.observation_dropped_count(), 0);
            drop(actual);
        },
    );
}

struct ReleaseGate(Option<Arc<tokio::sync::Semaphore>>);
impl ReleaseGate {
    fn release(&mut self) {
        if let Some(gate) = self.0.take() {
            gate.add_permits(1);
        }
    }
}
impl Drop for ReleaseGate {
    fn drop(&mut self) {
        self.release();
    }
}

fn held_return(name: &str, change_reservation: bool) {
    with_rpc(name, true, |fixture, request, receiver| {
        let candidate = request.reservation.ballot.candidate_node_id.clone();
        let prior = fixture
            .runtime
            .rpc_observations()
            .into_iter()
            .map(|item| item.call_id)
            .collect::<BTreeSet<_>>();
        let route = crate::dfs_message_rpc_call(&raw_command(
            &candidate,
            &request.expected_target,
            DfsNetworkMessage::SelectedLocalRecoveryFenceRequest(request.clone()),
        ))
        .unwrap()
        .request
        .route;
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        fixture.runtime.install_fault(FaultRule {
            source: Some(candidate.clone()),
            target: Some(request.expected_target.clone()),
            route: Some(route.clone()),
            origin: Some(RpcOrigin::RegisteredHost),
            call_id: None,
            remaining_hits: 1,
            action: FaultAction::PauseAfterDispatch(Arc::clone(&gate)),
        });
        let cut = deadline();
        let data_dir = fixture.runtime.data_dir(&candidate).clone();
        let runtime = fixture.runtime.runtime_handle();
        let wire = request.clone();
        let (actual_error, stored) = std::thread::scope(|scope| {
            // Drops before scope joins on every panic; no native owner lock
            // or namespace admission is held by this test across the RPC.
            let mut release = ReleaseGate(Some(gate));
            let child = scope.spawn(move || {
                call_native_selected_local_recovery_fence(&data_dir, &runtime, &wire, cut)
            });
            let call_id = loop {
                let matches = fixture
                    .runtime
                    .rpc_observations()
                    .into_iter()
                    .filter(|item| {
                        item.source == candidate
                            && item.target == request.expected_target
                            && item.route == route
                            && item.origin == RpcOrigin::RegisteredHost
                            && item.phase == RpcPhase::AfterDispatch
                            && !prior.contains(&item.call_id)
                    })
                    .collect::<Vec<_>>();
                if !matches.is_empty() {
                    assert_eq!(matches.len(), 1);
                    break matches[0].call_id;
                }
                assert!(
                    Instant::now() < cut,
                    "receiver must actually complete within the original call"
                );
                assert!(
                    !child.is_finished(),
                    "no return before selected receiver-completed cut"
                );
                std::thread::sleep(Duration::from_millis(1));
            };
            assert!(!child.is_finished());
            let stored = local_data(receiver, fixture).unwrap();
            assert_eq!(stored.fence.reservation, request.reservation);
            if change_reservation {
                let mut store =
                    DfsControlPlaneStore::new(fixture.control.as_mut().unwrap().as_mut());
                assert!(
                    store
                        .cancel_metadata_shard_recovery_ballot_reservation(
                            &request.reservation,
                            cut
                        )
                        .unwrap()
                );
                assert!(
                    Instant::now() < cut,
                    "actual reservation change does not renew this RPC"
                );
            } else {
                while Instant::now() <= cut {
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
            release.release();
            let error = error(child.join().unwrap());
            assert!(
                matches!(
                    error,
                    DfsError::NetworkDelivery {
                        delivery_state: sinomo_core::network::NetworkDeliveryState::PossiblyApplied,
                        ..
                    }
                ),
                "{error}"
            );
            let observations = fixture.runtime.rpc_observations();
            assert!(
                observations
                    .iter()
                    .any(|item| item.call_id == call_id && item.phase == RpcPhase::AfterDispatch)
            );
            eprintln!(
                "LOCAL_FENCE_HELD_RETURN reservation_changed={change_reservation} call={call_id} error={error}; receiver DATA remains historical, no owned return"
            );
            (error, stored)
        });
        fixture.runtime.clear_faults();
        if change_reservation {
            assert!(
                actual_error.to_string().contains("pending")
                    || actual_error.to_string().contains("reservation")
            );
        } else {
            assert!(
                actual_error.to_string().contains("deadline")
                    || actual_error.to_string().contains("timeout")
            );
        }
        same_data(&local_data(receiver, fixture).unwrap(), &stored);
        fixture.no_cooperative_marker();
        assert_eq!(fixture.runtime.observation_dropped_count(), 0);
    });
}

#[test]
fn native_local_fence_rpc_held_return_after_expiry_or_reservation_change_has_no_owned_observation()
{
    held_return("native-local-fence-rpc-expired-return", false);
    held_return("native-local-fence-rpc-changed-return", true);
}

#[test]
fn native_local_fence_rpc_generic_direct_and_reply_as_request_refuse_without_capability() {
    with_rpc(
        "native-local-fence-rpc-generic",
        true,
        |fixture, request, receiver| {
            let before_source = exact_frames(&fixture.runtime, &fixture.acceptor, &fixture.shard);
            let candidate = &request.reservation.ballot.candidate_node_id;
            let command = raw_command(
                candidate,
                &request.expected_target,
                DfsNetworkMessage::SelectedLocalRecoveryFenceRequest(request.clone()),
            );
            let mut events = Vec::new();
            let error = receiver
                .process_routed_message_with_options(
                    command.routed_message.clone(),
                    false,
                    &mut events,
                )
                .unwrap_err();
            assert!(error.to_string().contains("selected"));
            assert!(events.is_empty());
            assert!(local_data(receiver, fixture).is_none());
            let handle = fixture.runtime.runtime_handle();
            let direct_paths = crate::runtime_host::DfsRuntimePaths::new(
                fixture.runtime.data_dir(&request.expected_target),
            );
            let direct = handle.block_on(crate::runtime_host::handle_daemon_dfs_message_with_encoded_size_async(
            &direct_paths, command.clone(), None, None, None, &handle));
            assert!(
                direct.is_err(),
                "a supplied generic command is not authenticated selected ingress"
            );
            assert!(local_data(receiver, fixture).is_none());

            // This reply genuinely comes from an authenticated receiver. Pure
            // validation of it still yields DATA only, never the opaque return.
            let returned = raw_native(fixture, candidate, &command, false, deadline()).unwrap();
            assert!(matches!(
                response(&command, &returned).outcome,
                Outcome::Fenced { .. }
            ));
            let local = local_data(receiver, fixture).unwrap();
            let data = crate::node_transport::validate_selected_local_recovery_fence_rpc_return(
                &command,
                returned.clone(),
            )
            .unwrap();
            assert_eq!(data.fence, local.fence);
            let mut wrong_outer = returned.clone();
            wrong_outer.request_id.push_str("-wrong");
            let mut wrong_target = returned.clone();
            wrong_target.outbound_messages[0].target_node_id = request.expected_target.clone();
            let mut changed_subject = returned.clone();
            if let DfsNetworkMessage::SelectedLocalRecoveryFenceResponse(reply) =
                &mut changed_subject.outbound_messages[0].message
            {
                reply.reservation.reservation_id.push_str("-changed");
            }
            let mut extra = returned.clone();
            extra
                .outbound_messages
                .push(returned.outbound_messages[0].clone());
            for altered in [wrong_outer, wrong_target, changed_subject, extra] {
                assert!(
                    crate::node_transport::validate_selected_local_recovery_fence_rpc_return(
                        &command, altered
                    )
                    .is_err()
                );
            }
            let actual_reply = returned.outbound_messages[0].clone();
            let mut source = fixture.runtime.client(candidate).unwrap();
            assert!(
                source
                    .process_routed_message_with_options(
                        actual_reply.clone(),
                        false,
                        &mut Vec::new()
                    )
                    .is_err()
            );
            let reply_command =
                raw_command(&request.expected_target, candidate, actual_reply.message);
            let reply_paths =
                crate::runtime_host::DfsRuntimePaths::new(fixture.runtime.data_dir(candidate));
            let direct_reply = handle.block_on(crate::runtime_host::handle_daemon_dfs_message_with_encoded_size_async(
            &reply_paths, reply_command.clone(), None, None, None, &handle));
            assert!(direct_reply.is_err());
            assert!(
                raw_native(
                    fixture,
                    &request.expected_target,
                    &reply_command,
                    false,
                    deadline()
                )
                .is_err(),
                "an actual reply presented as a new RPC request has no owned-return waiter"
            );
            same_data(&local_data(receiver, fixture).unwrap(), &local);
            fixture.no_cooperative_marker();
            assert_eq!(
                exact_frames(&fixture.runtime, &fixture.acceptor, &fixture.shard),
                before_source
            );
            drop(source);
        },
    );
}

#[test]
fn native_local_fence_rpc_default_and_cooperative_refusals_preserve_ordinary_continuation() {
    with_rpc(
        "native-local-fence-rpc-default",
        false,
        |fixture, request, receiver| {
            fixture.seed("/services/native-local-recovery-fence.bin");
            let error = error(call(fixture, request, deadline()));
            assert!(error.to_string().contains("default"));
            let candidate = &request.reservation.ballot.candidate_node_id;
            let command = raw_command(
                candidate,
                &request.expected_target,
                DfsNetworkMessage::SelectedLocalRecoveryFenceRequest(request.clone()),
            );
            let returned = raw_native(fixture, candidate, &command, false, deadline()).unwrap();
            let Outcome::Refused { reason } = &response(&command, &returned).outcome else {
                panic!("actual default receiver refusal")
            };
            assert!(reason.contains("default"));
            assert!(
                receiver
                    .metadata_backend()
                    .unwrap()
                    .selected_local_recovery_fence(&fixture.shard_id())
                    .is_err()
            );
            absent_marker(fixture, &request.expected_target);
            fixture.seed("/services/native-local-recovery-fence.bin");
            absent_marker(fixture, &request.expected_target);
        },
    );
    with_rpc(
        "native-local-fence-rpc-cooperative",
        true,
        |fixture, request, receiver| {
            fixture.seed("/services/native-local-recovery-fence.bin");
            let mut wire = request.clone();
            wire.expected_target = fixture.shard.primary_node_id.clone();
            let path = crate::local_dfs_metadata_shard_path(
                fixture.runtime.data_dir(&wire.expected_target),
                &fixture.shard_id(),
            )
            .join("journal/membership-handoff-write-freeze.sobj");
            let before = std::fs::read(&path).unwrap();
            let primary_layout = fixture
                .runtime
                .selected_original_data_layout(&wire.expected_target)
                .unwrap();
            let original_marker =
                crate::local_state::read_local_dfs_metadata_shard_write_freeze_with_layout(
                    fixture.runtime.data_dir(&wire.expected_target),
                    &fixture.shard_id(),
                    Some(&primary_layout),
                )
                .unwrap()
                .unwrap();
            let serving = fixture.serving_cells();
            let command = raw_command(
                &wire.reservation.ballot.candidate_node_id,
                &wire.expected_target,
                DfsNetworkMessage::SelectedLocalRecoveryFenceRequest(wire.clone()),
            );
            let returned = raw_native(
                fixture,
                &wire.reservation.ballot.candidate_node_id,
                &command,
                false,
                deadline(),
            )
            .unwrap();
            let reason = match &response(&command, &returned).outcome {
                Outcome::Refused { reason } | Outcome::Uncertain { reason } => reason,
                other => panic!("cooperative marker must remain unsuperseded: {other:?}"),
            };
            assert!(reason.contains("cooperative"), "{reason}");
            assert_eq!(std::fs::read(&path).unwrap(), before);
            assert_eq!(fixture.serving_cells(), serving);
            assert!(local_data(receiver, fixture).is_none());
            fixture.seed("/services/native-local-recovery-fence.bin");
            // Ordinary writes may advance the existing rebind checkpoint.
            // They must retain the original cooperative cut and custody.
            let mut after =
                crate::local_state::read_local_dfs_metadata_shard_write_freeze_with_layout(
                    fixture.runtime.data_dir(&wire.expected_target),
                    &fixture.shard_id(),
                    Some(&primary_layout),
                )
                .unwrap()
                .unwrap();
            assert!(
                after.rebind_prefix_proof.journal_sequence
                    >= original_marker.rebind_prefix_proof.journal_sequence
            );
            after.rebind_prefix_proof = original_marker.rebind_prefix_proof.clone();
            assert_eq!(after, original_marker);
            assert_eq!(fixture.serving_cells(), serving);
        },
    );
}
