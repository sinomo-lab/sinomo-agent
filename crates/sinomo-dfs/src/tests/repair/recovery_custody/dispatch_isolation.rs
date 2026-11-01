//! Actual native late replies exercise worker isolation, not fabricated outcomes.
use super::*;

fn accept_empty_native_repair_policy(
    runtime: &crate::test_runtime::DfsTestRuntime,
    owner: &NodeId,
    replica: &NodeId,
    path: &str,
) {
    let mut placement = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 2).unwrap();
    placement.required_node_ids = vec![owner.clone(), replica.clone()];
    let accepted = runtime
        .apply_placement_policy(owner, path, DfsPlacementScope::SelfOnly, placement)
        .unwrap();
    let control = runtime.shared_control_plane().unwrap();
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    for _ in 0..2 {
        let job = control
            .with_client(|kv| {
                DfsControlPlaneStore::new(kv.as_mut())
                    .placement_reconciliation(&accepted.file_system_id, &accepted.job_id)
            })
            .unwrap()
            .unwrap();
        assert_eq!(job.accepted_config, accepted.accepted_config);
        assert_eq!(job.scanned_object_count, 0);
        assert_eq!(job.scheduled_repair_count, 0);
        assert!(job.last_error.is_none());
        if job.state == DfsPlacementReconciliationState::Completed {
            break;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(!remaining.is_zero());
        let tick = runtime.maintenance_tick(owner, remaining).unwrap();
        assert!(
            tick.outcome.is_ok(),
            "empty native policy job: {:?}",
            tick.outcome
        );
        assert_eq!(tick.omitted_kv_event_count, 0);
    }
    let completed = control
        .with_client(|kv| {
            DfsControlPlaneStore::new(kv.as_mut())
                .placement_reconciliation(&accepted.file_system_id, &accepted.job_id)
        })
        .unwrap()
        .unwrap();
    assert_eq!(completed.state, DfsPlacementReconciliationState::Completed);
    assert_eq!(completed.scheduled_repair_count, 0);
    assert_eq!(completed.scanned_object_count, 0);
    assert_eq!(completed.matched_object_count, 0);
    assert_eq!(completed.pending_repair_count, 0);
    assert!(completed.last_error.is_none());
    assert!(Instant::now() < deadline);
}

fn write_native_backlog(
    runtime: &crate::test_runtime::DfsTestRuntime,
    owner: &NodeId,
    replica: &NodeId,
    path: &str,
    bytes: &[u8],
) -> DfsStat {
    let mut caller = runtime.client(owner).unwrap();
    let mut policy = DfsReplicationPolicy::new(1, 2).unwrap();
    policy.required_node_ids = vec![owner.clone(), replica.clone()];
    caller
        .posix_write_file(
            path,
            bytes,
            DfsWriteOptions {
                replica_candidates: vec![owner.clone(), replica.clone()],
                policy,
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap()
}

#[derive(Clone, Copy, Debug)]
enum GuardNegative {
    ClosedSizeMismatch,
    ClosedMissingHash,
    ClosedMissingAttachment,
    OpenUnknownAlias,
    OpenUnknownSubject,
}

impl GuardNegative {
    fn name(self) -> &'static str {
        match self {
            Self::ClosedSizeMismatch => "native-closed-repair-size-mismatch",
            Self::ClosedMissingHash => "native-closed-repair-missing-hash",
            Self::ClosedMissingAttachment => "native-closed-repair-missing-attachment",
            Self::OpenUnknownAlias => "native-open-repair-unknown-alias",
            Self::OpenUnknownSubject => "native-open-repair-unknown-subject",
        }
    }
    fn keeps_original_open(self) -> bool {
        matches!(self, Self::OpenUnknownAlias | Self::OpenUnknownSubject)
    }
}

struct ActualSourceLifetime(Arc<AtomicBool>);
impl Drop for ActualSourceLifetime {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

fn native_late_repair_isolates_ready_original(observed_unclosed: bool) {
    native_repair_dispatch_case(observed_unclosed, None);
}

fn native_repair_dispatch_case(observed_unclosed: bool, negative: Option<GuardNegative>) {
    let name = if let Some(negative) = negative {
        negative.name()
    } else if observed_unclosed {
        "native-observed-repair-dispatch-isolation"
    } else {
        "native-closed-repair-dispatch-isolation"
    };
    // Both actual normal policy jobs finish while their paths are empty and
    // no background-claim hold or content repair debt exists. A later tick
    // cannot be used to finish B after the real A repair has been held.
    let (mut runtime, owner, replica, initial) = native_missing_content_repair_with_nodes(name, 2);
    drop(initial);
    let path_a = format!("/services/{name}.bin");
    let path_b = format!("/services/{name}-independent.bin");
    accept_empty_native_repair_policy(&runtime, &owner, &replica, &path_a);
    accept_empty_native_repair_policy(&runtime, &owner, &replica, &path_b);
    let background = runtime
        .deny_background_replica_before_claim(&owner)
        .unwrap();
    let bytes_a = (0..256 * 1024 + 31)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let a = write_native_backlog(&runtime, &owner, &replica, &path_a, &bytes_a);
    let b = write_native_backlog(
        &runtime,
        &owner,
        &replica,
        &path_b,
        b"independent native repair must not be batch blocked",
    );
    let mut core = runtime.metadata_primary_service(&a.path).unwrap();
    let (execution, hold) = native_private_repair_owner(&runtime, &core, &a.path, background);
    let queue = core.owned_repair_queue_store().unwrap().unwrap();
    let records = queue.view().unwrap().records;
    let a_intent = records
        .iter()
        .find(|row| {
            row.object_id == a.object_id
                && row.version == a.version
                && row.missing_replica_node_id == replica
        })
        .expect("first Main write has its real durable missing target")
        .intent_id
        .clone();
    let b_intent = records
        .iter()
        .find(|row| {
            row.object_id == b.object_id
                && row.version == b.version
                && row.missing_replica_node_id == replica
        })
        .expect("second Main write has its real durable missing target")
        .intent_id
        .clone();
    assert_ne!(a_intent, b_intent);
    core.retry_pending_repairs_bounded_with_attempts(0, &records)
        .unwrap();
    for (stat, id) in [(&a, &a_intent), (&b, &b_intent)] {
        let intent = core.coordinator().intent(id).unwrap();
        assert_eq!(intent.policy.min_commit_replicas, 1);
        assert_eq!(intent.policy.desired_replicas, 2);
        assert!(
            core.coordinator()
                .repair_jobs()
                .iter()
                .any(|job| job.intent_id == *id
                    && job.object_id == stat.object_id
                    && job.missing_replica_node_id == replica)
        );
        assert!(
            core.backend_receipt_for_replica(&stat.object_id, &stat.version, &replica)
                .unwrap()
                .is_none()
        );
    }
    runtime.clear_faults();
    let pending_a = core.writes.pending_writes[&a_intent].clone();
    let request_a = core
        .original_repair_request_for_target(&a_intent, &replica, &pending_a)
        .unwrap()
        .expect("open A Original or its first normal producer");
    let intent_a = core.coordinator().intent(&a_intent).unwrap().clone();
    let object_a = core.current_read_object_metadata(&a.object_id).unwrap();
    let first_a = core
        .prepare_current_background_repair_replica_sends(
            &[(replica.clone(), request_a.clone())],
            &intent_a,
            &pending_a,
            object_a.clone(),
        )
        .unwrap();
    let lost = Arc::new(Mutex::new(Vec::<DfsRoutedMessage>::new()));
    runtime.install_fault(crate::test_runtime::FaultRule {
        source: Some(owner.clone()),
        target: Some(replica.clone()),
        route: Some(DFS_DATA_REPAIR_REPLICA_ROUTE.into()),
        call_id: None,
        origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
        remaining_hits: 1,
        action: crate::test_runtime::FaultAction::CaptureTransferReplyAndLoseAfterDispatch {
            intent_id: Some(a_intent.clone()),
            captured: Arc::clone(&lost),
        },
    });
    let first_outcome = crate::test_runtime::executor()
        .block_on(core.dispatch_prepared_repair_replica_sends(first_a, false))
        .unwrap();
    assert_eq!(first_outcome.len(), 1);
    assert_eq!(
        first_outcome[0].delivery_state,
        Some(sinomo_core::network::NetworkDeliveryState::PossiblyApplied)
    );
    assert!(!first_outcome[0].sent);
    // Low-level dispatch already observed the actual lost return's delivery.
    // Consume its returned failure as record_repair_fanout_outcomes does,
    // before the ordinary observed checkpoint captures this scheduling owner.
    core.record_repair_retry_scheduled_with_error(
        &a_intent,
        &replica,
        first_outcome[0].last_error.clone(),
    );
    let first_outcome_custody = core.durable_repair_queue_records();
    assert!(core.transport.drain_incoming_messages().unwrap().is_empty());
    let actual_reply = lost.lock().unwrap().clone();
    assert_eq!(actual_reply.len(), 1);
    let routed_reply = actual_reply.into_iter().next().unwrap();
    assert_eq!(routed_reply.source_node_id, replica);
    assert_eq!(routed_reply.target_node_id, owner);
    let DfsNetworkMessage::RepairReplicaResponse(reply_a) = routed_reply.message else {
        panic!("actual native repair reply required")
    };
    assert_eq!(reply_a.intent_id, a_intent);
    assert_eq!(reply_a.request_id, request_a);
    assert!(
        matches!(&reply_a.result, DfsBlobTransferResult::Committed { content_hash }
        if Some(content_hash) == a.content_hash.as_ref())
    );
    runtime.clear_faults();
    let birth_a = queue
        .original_repair_for_slot(&a_intent, &replica)
        .unwrap()
        .unwrap();
    assert!(birth_a.observed.is_empty() && birth_a.closed_at_sequence.is_none());
    checkpoint_actual_native_custody(&runtime, &owner, &core.durable_repair_queue_records());

    // Prepare a second owned A carrier WHILE its actual Original remains open.
    // It reuses the full command/root and records a new real carrier attempt;
    // no command is prepared or sent after a native Close.
    let retry_a = core
        .prepare_current_background_repair_replica_sends(
            &[(replica.clone(), request_a.clone())],
            &intent_a,
            &core.writes.pending_writes[&a_intent].clone(),
            object_a,
        )
        .unwrap();
    let pending_b = core.writes.pending_writes[&b_intent].clone();
    let request_b = core
        .original_repair_request_for_target(&b_intent, &replica, &pending_b)
        .unwrap()
        .expect("independent B has its genuine first/open Original");
    let intent_b = core.coordinator().intent(&b_intent).unwrap().clone();
    let object_b = core.current_read_object_metadata(&b.object_id).unwrap();
    let first_b = core
        .prepare_current_background_repair_replica_sends(
            &[(replica.clone(), request_b.clone())],
            &intent_b,
            &pending_b,
            object_b,
        )
        .unwrap();
    let (peer, async_peer) = runtime.peer_transports(&owner).unwrap();
    let served = DfsNodeTransport::new_with_peer_transports_for_nodes(
        owner.clone(),
        vec![replica.clone()],
        peer,
        async_peer,
        sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
    );
    let old_worker = core.prepare_message_deferred_work();
    let old_transport = std::mem::replace(&mut core.transport, served);
    assert!(
        !old_worker.has_pending().unwrap(),
        "old transport retains no hidden completion"
    );
    core.set_background_replica_dispatch(true);
    core.queue_current_background_repair_sends(retry_a).unwrap();
    core.queue_current_background_repair_sends(first_b).unwrap();
    checkpoint_actual_native_custody(&runtime, &owner, &core.durable_repair_queue_records());
    core.transport
        .checkpoint_background_replica_dispatch()
        .unwrap();
    let mut worker = core.prepare_message_deferred_work();
    let (queued, omitted) = worker.checkpointed_replica_messages(8);
    assert_eq!(omitted, 0);
    assert_eq!(queued.len(), 2);
    let queued_a = queued
        .iter()
        .find(|(message, _)| {
            matches!(&message.message,
        DfsNetworkMessage::RepairReplica(command) if command.intent_id == a_intent)
        })
        .unwrap()
        .0
        .clone();
    let queued_b = queued
        .iter()
        .find(|(message, _)| {
            matches!(&message.message,
        DfsNetworkMessage::RepairReplica(command) if command.intent_id == b_intent)
        })
        .unwrap()
        .0
        .clone();
    assert!(queue.validate_original_repair_dispatch(&queued_a).is_ok());
    assert!(queue.validate_original_repair_dispatch(&queued_b).is_ok());
    let birth_b = queue
        .original_repair_for_slot(&b_intent, &replica)
        .unwrap()
        .unwrap();
    let before_reply = queue.view().unwrap();
    let keep_a_open = negative.is_some_and(GuardNegative::keeps_original_open);
    let a_result = if keep_a_open {
        Ok(None)
    } else if observed_unclosed {
        runtime.stop_node(&replica).unwrap();
        let result = core.handle_repair_replica_response(replica.clone(), reply_a.clone());
        assert!(result.is_err());
        assert!(
            receipt_prepare(&core, &reply_a).is_some(),
            "actual failed ACK retains its hidden body"
        );
        runtime.reopen_node(replica.clone()).unwrap();
        result.map(Some)
    } else {
        core.handle_repair_replica_response(replica.clone(), reply_a.clone())
            .map(Some)
    };
    let late_a = queue.original_repair(&birth_a.original).unwrap().unwrap();
    if keep_a_open {
        assert!(late_a.observed.is_empty() && late_a.closed_at_sequence.is_none());
        assert!(queue.validate_original_repair_dispatch(&queued_a).is_ok());
    } else {
        assert_eq!(late_a.observed, vec![reply_a.clone()]);
        assert_eq!(late_a.closed_at_sequence.is_none(), observed_unclosed);
        assert!(queue.validate_original_repair_dispatch(&queued_a).is_err());
    }
    let source_dropped = Arc::new(AtomicBool::new(false));
    let mut removed_attachment = None;
    let negative_call = negative.map(|negative| {
        worker.alter_checkpointed_replica_call_for_test(&queued_a, |call| {
            call.attachment
                .as_mut()
                .unwrap()
                .source
                .hold_lifetime_guard(ActualSourceLifetime(Arc::clone(&source_dropped)))
                .unwrap();
            match negative {
                GuardNegative::ClosedSizeMismatch => {
                    let body = call.attachment.as_mut().unwrap();
                    body.descriptor.total_size_bytes =
                        body.descriptor.total_size_bytes.checked_add(1).unwrap();
                }
                GuardNegative::ClosedMissingHash => {
                    call.attachment.as_mut().unwrap().descriptor.content_hash = None;
                }
                GuardNegative::ClosedMissingAttachment => {
                    // This malformed input contains no worker-owned attachment.
                    // Keep its actual removed source alive in fixture custody;
                    // do not pretend that an absent input reader is in the Vec.
                    removed_attachment = call.attachment.take();
                }
                GuardNegative::OpenUnknownAlias => {
                    call.message.frame_id ^= u64::MAX;
                }
                GuardNegative::OpenUnknownSubject => {
                    let DfsNetworkMessage::RepairReplica(command) = &mut call.message.message
                    else {
                        unreachable!()
                    };
                    command.request_id = format!("untracked-negative-input:{}", command.request_id);
                }
            }
            (
                call.message.clone(),
                call.attachment.as_ref().map(|body| body.descriptor.clone()),
            )
        })
    });
    let expected_guard_error = negative_call.as_ref().map(|(message, _)| {
        queue
            .validate_original_repair_dispatch(message)
            .unwrap_err()
            .to_string()
    });
    let custody_before_dispatch = queue.view().unwrap().records;

    assert!(queue.validate_original_repair_dispatch(&queued_b).is_ok());
    let owner_dir = sinomo_core::local::SinomoDataDir::new(runtime.data_dir(&owner).root());
    worker.set_background_delivery_handler(move |event| {
        crate::runtime_message::checkpoint_detached_replica_delivery(&owner_dir, event)
    });
    let core = Arc::new(Mutex::new(core));
    let callbacks = Arc::new(Mutex::new(Vec::new()));
    let response_core = Arc::clone(&core);
    let response_callbacks = Arc::clone(&callbacks);
    worker.set_background_response_handler(move |message, original| {
        response_callbacks
            .lock()
            .unwrap()
            .push((message.clone(), original.cloned()));
        let DfsNetworkMessage::RepairReplicaResponse(reply) = message.message else {
            return Err(DfsError::Metadata {
                message: "unexpected native repair callback family".into(),
            });
        };
        response_core
            .lock()
            .unwrap()
            .handle_repair_replica_response(message.source_node_id, reply)
            .map(|_| ())
    });
    let rpc_start = runtime.rpc_observations().len();
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let first_drain = worker.drain_exclusive_with_retry_observation();
    let first_cut = queue.view().unwrap();
    let a_after_first = queue.original_repair(&birth_a.original).unwrap().unwrap();
    let first_calls = runtime.rpc_observations()[rpc_start..].to_vec();
    let b_crossed = first_calls.iter().any(|event| {
        event.source == owner
            && event.target == replica
            && event.route == DFS_DATA_REPAIR_REPLICA_ROUTE
            && event.phase == crate::test_runtime::RpcPhase::Completed
            && event.dfs_intent_id.as_deref() == Some(b_intent.as_str())
            && event.dfs_request_id.as_deref() == Some(request_b.as_str())
    });
    let a_resent = first_calls.iter().any(|event| {
        event.source == owner
            && event.target == replica
            && event.route == DFS_DATA_REPAIR_REPLICA_ROUTE
            && event.phase == crate::test_runtime::RpcPhase::BeforeDispatch
            && event.dfs_intent_id.as_deref() == Some(a_intent.as_str())
    });
    let b_callback = callbacks.lock().unwrap().iter().any(|(message, original)| {
        matches!(&message.message, DfsNetworkMessage::RepairReplicaResponse(reply)
            if reply.intent_id == b_intent && reply.request_id == request_b)
            && original.as_ref() == Some(&queued_b)
    });
    let parked_first = worker.checkpointed_replica_call_data_for_test(8);
    if negative.is_none()
        && observed_unclosed
        && queue
            .original_repair(&birth_a.original)
            .unwrap()
            .unwrap()
            .closed_at_sequence
            .is_none()
    {
        let mut service = core.lock().unwrap();
        let pending = service.writes.pending_writes[&a_intent].clone();
        assert!(
            service
                .original_repair_request_for_target(&a_intent, &replica, &pending)
                .unwrap()
                .is_none(),
            "the existing owner confirms only A's stored receipt body"
        );
    }
    let last_drain = worker.drain_exclusive_with_retry_observation();
    let final_queue = queue.view().unwrap();
    let final_a = queue.original_repair(&birth_a.original).unwrap().unwrap();
    let final_b = queue.original_repair(&birth_b.original).unwrap().unwrap();
    let final_pending = worker.has_pending().unwrap();
    let receipt_b = core
        .lock()
        .unwrap()
        .backend_receipt_for_replica(&b.object_id, &b.version, &replica)
        .unwrap();
    let physical_b = LocalContentStore::open_for_inspection(
        runtime.data_dir(&replica).dfs_dir().join("content"),
    )
    .read_object_version(b.object_id.as_str(), b.version.as_str());
    let diagnostics = serde_json::json!({
        "qualification": "actual native late reply plus two already-owned readers; private component isolation, not a new background actor or fabricated authority",
        "observed_unclosed_case": observed_unclosed,
        "negative_data": format!("{negative:?}"), "negative_call": negative_call,
        "expected_guard_error": expected_guard_error,
        "actual_source_dropped": source_dropped.load(Ordering::SeqCst),
        "fixture_holds_removed_attachment": removed_attachment.is_some(),
        "custody_before_dispatch": custody_before_dispatch,
        "actual_late_reply_result": format!("{a_result:?}"),
        "actual_first_send_outcomes": format!("{first_outcome:#?}"),
        "first_outcome_caller_custody": first_outcome_custody,
        "first_drain": format!("{first_drain:?}"), "last_drain": format!("{last_drain:?}"),
        "queued": queued, "before_reply_records": before_reply.records,
        "late_original": late_a, "first_records": first_cut.records,
        "original_after_first": a_after_first, "parked_after_first": parked_first,
        "first_calls": format!("{first_calls:#?}"), "callbacks": *callbacks.lock().unwrap(),
        "b_crossed": b_crossed, "b_callback": b_callback, "a_resent": a_resent,
        "original_a": birth_a, "original_b": birth_b, "final_a": final_a,
        "final_b": final_b, "final_records": final_queue.records,
        "final_pending": final_pending, "receipt_b": receipt_b,
        "physical_b": format!("{physical_b:?}"), "normal_deadline_met": Instant::now() < deadline,
        "omitted_rpc_observations": runtime.observation_dropped_count(),
    });
    std::fs::write(
        runtime
            .data_dir(&owner)
            .root()
            .parent()
            .unwrap()
            .join("dispatch-isolation-state.json"),
        serde_json::to_vec_pretty(&diagnostics).unwrap(),
    )
    .unwrap();
    runtime.checkpoint_rpc_observation_window().unwrap();
    assert!(
        b_crossed && b_callback,
        "B's actual carrier and admitted callback must progress despite A"
    );
    assert!(!a_resent, "observed/closed A cannot cross another carrier");
    if negative.is_some() {
        let actual_negative = negative_call.as_ref().unwrap();
        assert_eq!(parked_first.1, 0);
        assert_eq!(
            parked_first.0,
            vec![actual_negative.clone()],
            "only the exact malformed owned input remains parked"
        );
        assert_eq!(
            first_drain.0.as_ref().unwrap_err().to_string(),
            *expected_guard_error.as_ref().unwrap()
        );
        assert_eq!(
            last_drain.0.as_ref().unwrap_err().to_string(),
            *expected_guard_error.as_ref().unwrap()
        );
        assert_eq!(
            final_a, late_a,
            "negative worker input cannot alter any native A birth fact"
        );
        let before_a = custody_before_dispatch
            .iter()
            .find(|row| row.intent_id == a_intent && row.missing_replica_node_id == replica);
        let after_a = final_queue
            .records
            .iter()
            .find(|row| row.intent_id == a_intent && row.missing_replica_node_id == replica);
        assert_eq!(
            after_a, before_a,
            "A's delivery/attempt/request/expiry evidence is unchanged"
        );
        if keep_a_open {
            let actual_a = before_a.expect("open A retains its actual named scheduling cursor");
            assert!(actual_a.in_flight && actual_a.attempts > 0);
            assert_eq!(
                actual_a.transfer_request_id.as_deref(),
                Some(request_a.as_str())
            );
            assert!(actual_a.completed_transfer_request_id.is_none());
            assert_eq!(
                actual_a.transfer_delivery_state,
                Some(sinomo_core::network::NetworkDeliveryState::PossiblyApplied)
            );
        }
        assert!(
            !source_dropped.load(Ordering::SeqCst),
            "the actual parked or fixture-retained source has not been dropped"
        );
        assert!(final_pending);
    } else if observed_unclosed {
        assert!(
            first_drain.0.is_err(),
            "A's actual blocked ownership remains visible"
        );
        assert!(
            parked_first
                .0
                .iter()
                .any(|(message, _)| *message == queued_a)
        );
    } else {
        assert!(
            !parked_first
                .0
                .iter()
                .any(|(message, _)| *message == queued_a)
        );
    }
    assert!(final_b.closed_at_sequence.is_some());
    if negative.is_none() {
        assert!(final_a.closed_at_sequence.is_some());
    }
    if !observed_unclosed && !keep_a_open {
        assert_eq!(
            final_a, late_a,
            "retiring the stale reader cannot append to or rewrite the already-closed birth"
        );
    }
    assert_eq!(
        final_a.attempts, late_a.attempts,
        "no new A carrier attempt is permitted after its real outcome"
    );
    assert_eq!(final_a.original, birth_a.original);
    assert_eq!(final_a.root, birth_a.root);
    assert_eq!(final_a.attachment, birth_a.attachment);
    assert_eq!(final_b.original, birth_b.original);
    assert_eq!(final_b.root, birth_b.root);
    assert_eq!(final_b.attachment, birth_b.attachment);
    assert!(receipt_b.is_some());
    if negative.is_none() {
        assert!(!final_pending);
    }
    assert_eq!(
        physical_b.unwrap().1,
        b"independent native repair must not be batch blocked"
    );
    assert!(
        Instant::now() < deadline,
        "unchanged normal five-second action window"
    );
    assert_eq!(runtime.observation_dropped_count(), 0);
    drop(worker);
    drop(core);
    drop(removed_attachment);
    if negative.is_some() {
        assert!(
            source_dropped.load(Ordering::SeqCst),
            "the retained actual source is released only by its explicit fixture/worker owner drop"
        );
    }
    drop(old_worker);
    drop(old_transport);
    drop(execution);
    drop(hold);
    runtime.close().unwrap();
}

#[test]
fn native_observed_original_keeps_its_reader_without_blocking_an_independent_repair() {
    native_late_repair_isolates_ready_original(true);
}

#[test]
fn native_closed_original_retires_only_its_stale_reader_and_sends_the_independent_repair() {
    native_late_repair_isolates_ready_original(false);
}

#[test]
fn native_closed_original_wrong_size_keeps_its_actual_source_and_other_call_progresses() {
    native_repair_dispatch_case(false, Some(GuardNegative::ClosedSizeMismatch));
}

#[test]
fn native_closed_original_missing_hash_keeps_its_actual_source_and_other_call_progresses() {
    native_repair_dispatch_case(false, Some(GuardNegative::ClosedMissingHash));
}

#[test]
fn native_closed_original_missing_attachment_retains_fixture_source_and_rejects_only_that_call() {
    native_repair_dispatch_case(false, Some(GuardNegative::ClosedMissingAttachment));
}

#[test]
fn native_open_original_unknown_alias_remains_owned_while_other_call_progresses() {
    native_repair_dispatch_case(false, Some(GuardNegative::OpenUnknownAlias));
}

#[test]
fn native_open_original_unknown_subject_remains_owned_while_other_call_progresses() {
    native_repair_dispatch_case(false, Some(GuardNegative::OpenUnknownSubject));
}
