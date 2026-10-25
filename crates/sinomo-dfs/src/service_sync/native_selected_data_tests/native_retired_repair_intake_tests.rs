//! Normal admitted Main returns -> custody DATA only, before serving authority.
use super::*;
use crate::repair_queue_store::{
    DfsRepairQueueCompactionOptions, LocalDfsRepairQueueStore, NativeOwnedRepairCompletionRef,
    RetainedNativeOwnedRepairCompletion,
};
use crate::runtime_host::{NativeRepairCustodySelection, OwnedOriginalRepairCompletion};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const PATH: &str = "/services/selected-original-data.bin";

fn with_case(
    name: &str,
    check: impl FnOnce(&mut GenuineNativeOriginal, &mut Option<NativeRepairCustodySelection>),
) {
    let mut fixture = genuine_native_original(name);
    let material = fixture.birth.original_wire_material().unwrap();
    let mut selection = Some(
        crate::runtime_host::select_native_original_repair_custody(
            fixture.runtime.data_dir(&fixture.shard.primary_node_id),
            &material,
        )
        .unwrap(),
    );
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        check(&mut fixture, &mut selection)
    }));
    if outcome.is_err() {
        let diagnostic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            fixture.runtime.checkpoint_rpc_observation_window()
        }));
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match diagnostic {
            Ok(result) => eprintln!("NATIVE_REPAIR_INTAKE_FAILURE_RPC {result:?}"),
            Err(_) => {
                eprintln!("NATIVE_REPAIR_INTAKE_FAILURE_RPC unavailable: checkpoint panicked")
            }
        }));
    }
    // Every test-local cloned capability is dropped by check's return/unwind.
    // The probe owns only Weak capability references and is gone before close.
    drop(selection.take());
    fixture.runtime.clear_faults();
    drop(fixture.held.take());
    drop(fixture.held_promoted.take());
    let closed = fixture.runtime.close();
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
    closed.unwrap();
}

fn queue(fixture: &GenuineNativeOriginal) -> LocalDfsRepairQueueStore {
    fixture
        .runtime
        .selected_original_data_layout(&fixture.shard.primary_node_id)
        .unwrap()
        .queue()
        .unwrap()
}

fn remaining(deadline: Instant) -> Duration {
    deadline
        .checked_duration_since(Instant::now())
        .expect("same existing 45s scenario window")
}

fn tick(fixture: &GenuineNativeOriginal, deadline: Instant) {
    let result = fixture
        .runtime
        .maintenance_tick(
            &fixture.shard.primary_node_id,
            remaining(deadline).min(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT),
        )
        .unwrap();
    eprintln!("NATIVE_REPAIR_CUSTODY_TICK {:?}", result.outcome);
    std::thread::sleep(
        deadline
            .saturating_duration_since(Instant::now())
            .min(Duration::from_millis(10)),
    );
}

fn metadata_facts(fixture: &GenuineNativeOriginal) -> serde_json::Value {
    let material = fixture.birth.original_wire_material().unwrap();
    let command = &material.original.command;
    let service = fixture.runtime.metadata_primary_service(PATH).unwrap();
    let backend = service.metadata_backend().unwrap();
    let object = backend.get_object(&command.object_id).unwrap().unwrap();
    let receipts = backend
        .receipts_for_version(&command.object_id, &command.version)
        .unwrap();
    assert!(!object.current_locations.contains(&fixture.holder));
    assert!(
        !receipts
            .iter()
            .any(|receipt| receipt.replica_node_id == fixture.holder)
    );
    serde_json::json!({
        "object": object,
        "version": backend.get_version(&command.object_id, &command.version).unwrap(),
        "receipts": receipts,
        "intents": backend.intents_for_object(&command.object_id).unwrap(),
    })
}

fn assert_no_effects(fixture: &GenuineNativeOriginal, before: &serde_json::Value) {
    let source = queue(fixture);
    let birth = source
        .original_repair(&fixture.birth.original)
        .unwrap()
        .unwrap();
    assert_eq!(birth.original, fixture.birth.original);
    assert_eq!(birth.attachment, fixture.birth.attachment);
    assert_eq!(birth.root, fixture.birth.root);
    assert!(birth.observed.is_empty() && birth.closed_at_sequence.is_none());
    assert!(
        source
            .closed_repair_birth(&birth.root.subject)
            .unwrap()
            .is_none()
    );
    fixture
        .runtime
        .client(&fixture.shard.primary_node_id)
        .unwrap()
        .content()
        .inspect_repair_payload_root(&birth.root)
        .unwrap();
    assert_eq!(
        &metadata_facts(fixture),
        before,
        "custody DATA cannot publish receipts/W, change the visible object/version/intent or apply a historical result"
    );
    let material = birth.original_wire_material().unwrap();
    let command = &material.original.command;
    for node in [&fixture.shard.primary_node_id, &fixture.holder] {
        assert_eq!(
            fixture
                .runtime
                .client(node)
                .unwrap()
                .content()
                .read_object_version(command.object_id.as_str(), command.version.as_str())
                .unwrap()
                .1,
            vec![0x5a; 4096],
            "custody intake/replay preserves the original exact bytes"
        );
    }
    assert!(
        fixture
            .runtime
            .client(&fixture.promoted)
            .unwrap()
            .content()
            .read_object_version(command.object_id.as_str(), command.version.as_str())
            .is_err(),
        "metadata DATA transfer supplies no B bytes or B replica receipt"
    );
    assert_eq!(fixture.runtime.observation_dropped_count(), 0);
}

fn repair_wire_calls(fixture: &GenuineNativeOriginal) -> Vec<u64> {
    let material = fixture.birth.original_wire_material().unwrap();
    let command = &material.original.command;
    fixture
        .runtime
        .rpc_observations()
        .into_iter()
        .filter(|call| {
            call.source == fixture.shard.primary_node_id
                && call.target == fixture.holder
                && call.route == DFS_DATA_REPAIR_REPLICA_ROUTE
                && call.origin == RpcOrigin::RegisteredHost
                && call.phase == RpcPhase::BeforeDispatch
                && call.dfs_request_id.as_deref() == Some(command.request_id.as_str())
                && call.dfs_intent_id.as_deref() == Some(command.intent_id.as_str())
                && call.dfs_object_id.as_ref() == Some(&command.object_id)
                && call.dfs_version.as_ref() == Some(&command.version)
        })
        .map(|call| call.call_id)
        .collect()
}

fn held_normal_return(
    fixture: &GenuineNativeOriginal,
    selection: &NativeRepairCustodySelection,
    deadline: Instant,
) -> Arc<OwnedOriginalRepairCompletion> {
    let previous = repair_wire_calls(fixture);
    assert!(selection.captured().is_none() && selection.intake_attempts() == 0);
    // This removes only the fixture's negative claim boundary once. Existing
    // Main admission, queue checkpoint, original body and retry clock own work.
    fixture
        .allow_one_same_original_retry
        .store(true, Ordering::Release);
    fixture.claim_window.store(true, Ordering::Release);
    loop {
        if selection.captured().is_some()
            && selection
                .last_intake_error()
                .as_deref()
                .is_some_and(|error| error.contains("native original Repair custody intake held"))
        {
            break;
        }
        tick(fixture, deadline);
    }
    assert!(
        !fixture
            .allow_one_same_original_retry
            .load(Ordering::Acquire)
    );
    assert!(!fixture.claim_window.load(Ordering::Acquire));
    let observed = selection.captured().unwrap();
    let completion = selection
        .captured_completion()
        .expect("the actual typed return remains in the existing outbox");
    let material = fixture.birth.original_wire_material().unwrap();
    assert!(material.matches_carrier(&observed.carrier));
    assert_eq!(completion.material(), &material);
    assert_eq!(completion.carrier(), &observed.carrier);
    assert_eq!(completion.returned_frame(), &observed.returned_frame);
    assert_eq!(completion.payload_root(), &fixture.birth.root);
    assert_eq!(completion.attempt_sequence(), observed.attempt_sequence);
    assert_eq!(completion.rpc_request_id(), observed.rpc_request_id);
    assert!(
        observed.attempt_sequence
            > fixture
                .birth
                .attempts
                .iter()
                .map(|attempt| attempt.sequence)
                .max()
                .unwrap()
    );
    let command = &material.original.command;
    assert_eq!(observed.returned_frame.source_node_id, fixture.holder);
    assert_eq!(
        observed.returned_frame.target_node_id,
        fixture.shard.primary_node_id
    );
    let DfsNetworkMessage::RepairReplicaResponse(response) = &observed.returned_frame.message
    else {
        panic!("actual C return");
    };
    assert_eq!(
        response,
        &DfsRepairReplicaResponse {
            request_id: command.request_id.clone(),
            intent_id: command.intent_id.clone(),
            file_system_id: command.file_system_id.clone(),
            object_id: command.object_id.clone(),
            version: command.version.clone(),
            replica_node_id: fixture.holder.clone(),
            result: DfsBlobTransferResult::Committed {
                content_hash: command.content_hash.clone()
            },
        }
    );
    let now = repair_wire_calls(fixture);
    assert_eq!(
        now.len(),
        previous.len() + 1,
        "one new actual Main call, no replacement ID or hidden wire retry"
    );
    let call_id = *now.last().unwrap();
    let completed: Vec<_> = fixture
        .runtime
        .rpc_observations()
        .into_iter()
        .filter(|call| call.call_id == call_id && call.phase == RpcPhase::Completed)
        .collect();
    assert_eq!(completed.len(), 1);
    assert!(completed[0].outcome_error.is_none());
    assert_eq!(completed[0].dfs_frame_id, Some(observed.carrier.frame_id));
    assert!(
        matches!(completed[0].signer.as_ref(), Some(sinomo_core::service::ServiceSigner::Remote { node_id, .. })
        if node_id == fixture.shard.primary_node_id.as_str())
    );
    assert!(selection.retained_reference().is_none());
    assert!(selection.intake_attempts() > 0);
    assert_eq!(fixture.runtime.observation_dropped_count(), 0);
    assert!(Instant::now() <= deadline);
    completion
}

fn wait_retained(
    fixture: &GenuineNativeOriginal,
    selection: &NativeRepairCustodySelection,
    deadline: Instant,
) -> NativeOwnedRepairCompletionRef {
    loop {
        if let Some(reference) = selection.retained_reference() {
            return reference;
        }
        tick(fixture, deadline);
    }
}

fn assert_record(
    record: &RetainedNativeOwnedRepairCompletion,
    completion: &OwnedOriginalRepairCompletion,
) {
    assert_eq!(record.carrier(), completion.carrier());
    assert_eq!(record.material(), completion.material());
    assert_eq!(record.payload_root(), completion.payload_root());
    assert_eq!(record.returned_frame(), completion.returned_frame());
    assert_eq!(record.attempt_sequence(), completion.attempt_sequence());
    assert_eq!(record.rpc_request_id(), completion.rpc_request_id());
    assert!(record.retained_at_sequence() > record.attempt_sequence());
}

fn raw_reply_refuses(
    fixture: &GenuineNativeOriginal,
    returned: &DfsRoutedMessage,
    expected: &str,
    deadline: Instant,
) {
    let before = queue(fixture).view().unwrap();
    let last = fixture
        .runtime
        .rpc_observations()
        .iter()
        .map(|call| call.call_id)
        .max()
        .unwrap_or(0);
    let command = DfsMessageCommand {
        request_id: format!("dfs_message_{}", returned.frame_id),
        routed_message: returned.clone(),
    };
    let call = crate::dfs_message_rpc_call(&command).unwrap();
    let error = match fixture.runtime.rpc_result(
        &fixture.holder,
        &fixture.shard.primary_node_id,
        call,
        remaining(deadline).min(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT),
    ) {
        Ok(_) => {
            panic!("an unowned generic response cannot acknowledge this selected native result")
        }
        Err(error) => error,
    };
    assert!(error.contains(expected), "{error}");
    assert_eq!(queue(fixture).view().unwrap(), before);
    let refused: Vec<_> = fixture
        .runtime
        .rpc_observations()
        .into_iter()
        .filter(|call| {
            call.call_id > last
                && call.source == fixture.holder
                && call.target == fixture.shard.primary_node_id
                && call.route == DFS_DATA_REPAIR_REPLICA_RESPONSE_ROUTE
                && call.origin == RpcOrigin::ExplicitFixtureRpc
                && call.phase == RpcPhase::EndedWithoutResponse
        })
        .collect();
    assert_eq!(refused.len(), 1);
    assert!(
        refused[0]
            .outcome_error
            .as_deref()
            .is_some_and(|error| error.contains(expected))
    );
    assert!(
        matches!(refused[0].signer.as_ref(), Some(sinomo_core::service::ServiceSigner::Remote { node_id, .. })
        if node_id == fixture.holder.as_str())
    );
    assert_eq!(fixture.runtime.observation_dropped_count(), 0);
}

fn compact_cold_reopen(
    fixture: &mut GenuineNativeOriginal,
    selection: &mut Option<NativeRepairCustodySelection>,
    reference: &NativeOwnedRepairCompletionRef,
    expected: &RetainedNativeOwnedRepairCompletion,
) {
    let layout = fixture
        .runtime
        .selected_original_data_layout(&fixture.shard.primary_node_id)
        .unwrap();
    let identity = layout.queue_layout().clone();
    let source = layout.queue().unwrap();
    source
        .compact_if_needed(DfsRepairQueueCompactionOptions::force())
        .unwrap();
    assert_eq!(
        &source
            .retained_native_owned_repair_completion(reference)
            .unwrap(),
        expected
    );
    // The caller has dropped every cloned opaque capability. Only historical
    // DATA/reference survives. Retained fixture layout is representation DATA.
    drop(selection.take());
    drop(source);
    drop(layout);
    fixture
        .runtime
        .stop_node(&fixture.shard.primary_node_id)
        .unwrap();
    LocalDfsRepairQueueStore::evict_for_test(identity.snapshot_path());
    let cold = LocalDfsRepairQueueStore::reopen_selected_original_fixture(&identity).unwrap();
    assert_eq!(
        &cold
            .retained_native_owned_repair_completion(reference)
            .unwrap(),
        expected
    );
    assert!(
        cold.native_original_completion_pending(expected.carrier())
            .unwrap()
    );
    drop(cold);
    fixture
        .runtime
        .reopen_node(fixture.shard.primary_node_id.clone())
        .unwrap();
    assert_eq!(
        &queue(fixture)
            .retained_native_owned_repair_completion(reference)
            .unwrap(),
        expected
    );
}

#[test]
fn native_current_a_custody_intake_recovers_namespace_failure_and_holds_retry() {
    with_case("native-current-a-custody", |fixture, selected| {
        let before = metadata_facts(fixture);
        let deadline = Instant::now() + DEFAULT_DFS_POSIX_SYNC_TIMEOUT;
        let selection = selected.as_ref().unwrap();
        let completion = held_normal_return(fixture, selection, deadline);
        assert_no_effects(fixture, &before);
        let wires = repair_wire_calls(fixture);
        let held_attempts = selection.intake_attempts();
        selection.fail_next_namespace_once();
        selection.allow_intake();
        loop {
            if selection
                .last_intake_error()
                .as_deref()
                .is_some_and(|error| error.contains("injected selected namespace barrier failure"))
            {
                break;
            }
            tick(fixture, deadline);
        }
        assert!(selection.intake_attempts() > held_attempts);
        let after_failed_barrier = queue(fixture).view().unwrap().head_sequence;
        assert_no_effects(fixture, &before);
        let reference = wait_retained(fixture, selection, deadline);
        let source = queue(fixture);
        let record = source
            .retained_native_owned_repair_completion(&reference)
            .unwrap();
        assert_record(&record, &completion);
        assert!(
            record.retained_at_sequence() <= after_failed_barrier,
            "the real failed namespace barrier already appended this record; retry cannot allocate a replacement"
        );
        let stable = source.view().unwrap();
        let raw = native_repair_receiver_content_files(
            fixture
                .runtime
                .selected_original_data_layout(&fixture.shard.primary_node_id)
                .unwrap()
                .queue_layout()
                .root(),
        );
        let duplicate = source
            .retain_native_owned_repair_completion(&completion)
            .unwrap();
        assert_eq!(duplicate.reference(), &reference);
        assert_eq!(source.view().unwrap(), stable);
        assert_eq!(
            native_repair_receiver_content_files(
                fixture
                    .runtime
                    .selected_original_data_layout(&fixture.shard.primary_node_id)
                    .unwrap()
                    .queue_layout()
                    .root()
            ),
            raw
        );
        assert_eq!(repair_wire_calls(fixture), wires);
        assert!(
            source
                .native_original_completion_pending(completion.carrier())
                .unwrap()
        );
        let error = source
            .validate_original_repair_dispatch(completion.carrier())
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("original Repair awaits native completion custody settlement")
        );
        let returned = completion.returned_frame().clone();
        drop(completion);
        drop(source);
        compact_cold_reopen(fixture, selected, &reference, &record);
        let source = queue(fixture);
        let current = fixture.runtime.metadata_primary_service(PATH).unwrap();
        assert_eq!(current.node_id(), &fixture.shard.primary_node_id);
        let scope = DfsPreparedOperationScope {
            file_system_id: fixture.shard.file_system_id.clone(),
            shard_id: DfsMetadataShardId::new(&fixture.shard.shard_id),
            owner_epoch: fixture.shard.epoch,
            operation_id: fixture.birth.root.subject.request_id.clone(),
        };
        let admission = current.acquire_prepared_execution(&scope).unwrap();
        assert!(
            admission.is_some(),
            "A still has actual current native admission; loss of authority cannot explain this hold"
        );
        drop(admission);
        drop(current);
        // Make the existing negative fixture claim gate permissive once: the
        // durable native-fact guard must prevent reaching it or emitting wire.
        fixture
            .allow_one_same_original_retry
            .store(true, Ordering::Release);
        fixture.claim_window.store(true, Ordering::Release);
        let due = source
            .view()
            .unwrap()
            .records
            .iter()
            .map(|row| row.next_retry_unix_nanos)
            .max()
            .unwrap_or(0);
        while SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            < u128::from(due)
        {
            std::thread::sleep(remaining(deadline).min(Duration::from_millis(10)));
        }
        let stable = source.view().unwrap();
        for _ in 0..2 {
            tick(fixture, deadline);
        }
        assert_eq!(
            source.view().unwrap(),
            stable,
            "holding a native custody fact cannot churn queue rows/head or backoff checkpoints"
        );
        assert_eq!(repair_wire_calls(fixture), wires);
        assert_eq!(
            source
                .retained_native_owned_repair_completion(&reference)
                .unwrap(),
            record
        );
        raw_reply_refuses(
            fixture,
            &returned,
            "original Repair awaits native completion custody settlement",
            deadline,
        );
        assert_no_effects(fixture, &before);
    });
}

#[test]
fn native_retired_a_intakes_same_normal_return_before_serving_refusal() {
    with_case("native-retired-a-custody", |fixture, selected| {
        let before = metadata_facts(fixture);
        let deadline = Instant::now() + DEFAULT_DFS_POSIX_SYNC_TIMEOUT;
        let selection = selected.as_ref().unwrap();
        let completion = held_normal_return(fixture, selection, deadline);
        let wires = repair_wire_calls(fixture);
        let request = fixture.birth.root.subject.request_id.clone();
        let layout_b = fixture
            .runtime
            .selected_original_data_layout(&fixture.promoted)
            .unwrap();
        let mut retiring = fixture.runtime.metadata_primary_service(PATH).unwrap();
        let fence = retiring
            .metadata_shard_fence_guard(&fixture.shard.file_system_id, &fixture.shard.shard_id)
            .unwrap();
        let control = fixture.runtime.shared_control_plane().unwrap();
        let proposed = control
            .with_client(|kv| {
                DfsControlPlaneStore::new(kv.as_mut())
                    .request_metadata_shard_primary_handoff(&fixture.shard, &fixture.promoted)
            })
            .unwrap()
            .unwrap();
        fixture
            .runtime
            .select_genuine_original_before_handoff(
                &fixture.shard.primary_node_id,
                &request,
                &proposed,
            )
            .unwrap();
        drop(fence);
        let result = retiring.reconcile_pending_metadata_shard_membership_handoffs(
            &fixture.runtime.metadata_shards().unwrap(),
            remaining(deadline),
        );
        if let Err(error) = result {
            assert_eq!(
                fixture
                    .runtime
                    .observe_completed_native_handoff(&control, &proposed, deadline)
                    .unwrap(),
                Some(proposed.desired_shard.clone()),
                "actual handoff error: {error}"
            );
        }
        let shard_id = DfsMetadataShardId::new(&fixture.shard.shard_id);
        let marker = loop {
            let marker =
                crate::local_state::read_local_dfs_metadata_shard_write_freeze_with_layout(
                    fixture.runtime.data_dir(&fixture.promoted),
                    &shard_id,
                    Some(&layout_b),
                )
                .unwrap();
            if let Some(marker) =
                marker.filter(|marker| marker.activated_epoch == Some(proposed.desired_shard.epoch))
            {
                break marker;
            }
            fixture
                .runtime
                .maintenance_tick(
                    &fixture.promoted,
                    remaining(deadline).min(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT),
                )
                .unwrap();
        };
        let cut = marker.repair_custody.as_ref().unwrap();
        assert_eq!(cut.version, 2);
        let source_directory = crate::local_dfs_metadata_shard_path(
            fixture.runtime.data_dir(&fixture.shard.primary_node_id),
            &shard_id,
        )
        .join("journal/repair-handoffs-v1")
        .join(format!(
            "{}-source",
            blake3::hash(proposed.handoff_id.as_bytes()).to_hex()
        ));
        let source_cut: DfsRepairHandoffCut =
            crate::metadata_io::read_sobj_optional(&source_directory.join("cut.sobj"))
                .unwrap()
                .expect("actual source published its immutable cut before native installation");
        source_cut.validate_selected_original_data().unwrap();
        assert_eq!(
            &source_cut, cut,
            "B must retain A's exact actual fenced cut"
        );
        let actual_fence = source_cut
            .handoff
            .retiring_primary_fence
            .as_ref()
            .expect("actual source cut contains the retiring primary's durable fence");
        let layout_a = fixture
            .runtime
            .selected_original_data_layout(&fixture.shard.primary_node_id)
            .unwrap();
        let source_freeze =
            crate::local_state::read_local_dfs_metadata_shard_write_freeze_with_layout(
                fixture.runtime.data_dir(&fixture.shard.primary_node_id),
                &shard_id,
                Some(&layout_a),
            )
            .unwrap()
            .expect("actual A freeze remains durable after role retirement");
        assert_eq!(source_freeze.handoff_id, proposed.handoff_id);
        assert_eq!(source_freeze.retiring_epoch, proposed.retiring_shard.epoch);
        assert_eq!(actual_fence.handoff_id, source_freeze.handoff_id);
        assert_eq!(
            actual_fence.primary_node_id,
            proposed.retiring_shard.primary_node_id
        );
        assert_eq!(
            actual_fence.journal_sequence,
            source_freeze.journal_sequence
        );
        assert_eq!(
            actual_fence.state_fingerprint,
            source_freeze.state_fingerprint
        );
        assert!(actual_fence.journal_sequence >= proposed.required_prepared_sequence);
        if let Some(head) = &proposed.required_head_watermark {
            assert!(actual_fence.journal_sequence >= head.journal_sequence);
        }
        // The initial proposal precedes A's acknowledgement. Preserve every
        // proposal identity/configuration/minimum-prefix field exactly; only
        // its absent fence is completed from the independently retained source.
        let mut fenced_proposal = proposed.clone();
        if let Some(initial_fence) = &proposed.retiring_primary_fence {
            assert_eq!(initial_fence, actual_fence);
        } else {
            fenced_proposal.retiring_primary_fence = Some(actual_fence.clone());
        }
        assert_eq!(source_cut.handoff, fenced_proposal);
        drop(layout_a);
        assert_eq!(
            fixture
                .runtime
                .metadata_primary_service(PATH)
                .unwrap()
                .node_id(),
            &fixture.promoted
        );
        assert_no_effects(fixture, &before);
        let old_scope = DfsPreparedOperationScope {
            file_system_id: fixture.shard.file_system_id.clone(),
            shard_id: shard_id.clone(),
            owner_epoch: fixture.shard.epoch,
            operation_id: request.clone(),
        };
        assert!(retiring.publication_provider.current().is_some());
        assert!(retiring.publication_execution.current().is_none());
        assert!(
            retiring.acquire_prepared_execution(&old_scope).is_err(),
            "the actual old A provider must refuse serving authority before local custody intake"
        );
        raw_reply_refuses(
            fixture,
            completion.returned_frame(),
            "selected original Repair response lacks native owned-return provenance",
            deadline,
        );
        assert!(
            Arc::ptr_eq(&selection.captured_completion().unwrap(), &completion),
            "a generic frame cannot replace the actual owned return; legitimate local retries retain this same Arc"
        );
        assert!(selection.retained_reference().is_none());
        selection.allow_intake();
        let reference = wait_retained(fixture, selection, deadline);
        let source = queue(fixture);
        let record = source
            .retained_native_owned_repair_completion(&reference)
            .unwrap();
        assert_record(&record, &completion);
        let stable = source.view().unwrap();
        let duplicate = source
            .retain_native_owned_repair_completion(&completion)
            .unwrap();
        assert_eq!(duplicate.reference(), &reference);
        assert_eq!(source.view().unwrap(), stable);
        assert_eq!(
            repair_wire_calls(fixture),
            wires,
            "retired intake retries only local custody, never C wire or B execution"
        );
        assert!(
            retiring.acquire_prepared_execution(&old_scope).is_err(),
            "durable intake must not restore the old A provider"
        );
        assert_no_effects(fixture, &before);
        let installed = layout_b
            .queue()
            .unwrap()
            .installed_original_handoff_data(&fixture.shard.primary_node_id, &request)
            .unwrap()
            .unwrap();
        assert_eq!(installed.material.birth.original, fixture.birth.original);
        assert!(
            layout_b
                .queue()
                .unwrap()
                .original_repair(&fixture.birth.original)
                .unwrap()
                .is_none()
        );
        drop(retiring);
        drop(source);
        drop(control);
        drop(layout_b);
        drop(completion);
        compact_cold_reopen(fixture, selected, &reference, &record);
        assert_eq!(repair_wire_calls(fixture), wires);
        assert_eq!(
            fixture
                .runtime
                .metadata_primary_service(PATH)
                .unwrap()
                .node_id(),
            &fixture.promoted
        );
        assert_no_effects(fixture, &before);
    });
}

struct QueueSwap {
    root: PathBuf,
    parked: PathBuf,
    rejected: PathBuf,
}
impl QueueSwap {
    fn install(root: &Path) -> Self {
        let parent = root.parent().unwrap();
        let parked = parent.join("native-intake-original-queue");
        let rejected = parent.join("native-intake-rejected-queue");
        let copy = parent.join("native-intake-copy-queue");
        for path in [&parked, &rejected, &copy] {
            assert!(!path.exists());
        }
        std::fs::create_dir(&copy).unwrap();
        let files = native_repair_receiver_content_files(root);
        for (relative, bytes) in &files {
            let path = copy.join(relative);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, bytes).unwrap();
        }
        assert_eq!(native_repair_receiver_content_files(&copy), files);
        let guard = Self {
            root: root.to_owned(),
            parked,
            rejected,
        };
        std::fs::rename(root, &guard.parked).unwrap();
        std::fs::rename(copy, root).unwrap();
        guard
    }
}
impl Drop for QueueSwap {
    fn drop(&mut self) {
        if self.parked.exists() {
            if self.root.exists() {
                std::fs::rename(&self.root, &self.rejected)
                    .expect("retain unadmitted copied queue");
            }
            std::fs::rename(&self.parked, &self.root)
                .expect("restore original queue before native close");
        }
    }
}

#[test]
fn native_intake_refuses_weak_cap_foreign_queue_physical_copy_and_ambiguous_attempt() {
    with_case("native-intake-refusals", |fixture, selected| {
        let before = metadata_facts(fixture);
        let deadline = Instant::now() + DEFAULT_DFS_POSIX_SYNC_TIMEOUT;
        let source = queue(fixture);
        let material = fixture.birth.original_wire_material().unwrap();
        let witness = source
            .capture_original_repair_attempt_witness(&fixture.birth.original, &material)
            .unwrap();
        let weak = crate::runtime_host::call_native_original_repair_owned(
            fixture.runtime.data_dir(&fixture.shard.primary_node_id),
            witness,
            DfsObjectRpcSendOptions::new(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
                .with_overall_timeout(
                    remaining(deadline).min(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT),
                ),
        )
        .unwrap();
        let stable = source.view().unwrap();
        let root = fixture
            .runtime
            .selected_original_data_layout(&fixture.shard.primary_node_id)
            .unwrap()
            .queue_layout()
            .root()
            .to_owned();
        let raw = native_repair_receiver_content_files(&root);
        let error = source
            .retain_native_owned_repair_completion(&weak)
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("native completion lacks admitted producer custody provenance"),
            "{error}"
        );
        assert_eq!(source.view().unwrap(), stable);
        assert_eq!(native_repair_receiver_content_files(&root), raw);
        assert_no_effects(fixture, &before);
        drop(weak);
        let selection = selected.as_ref().unwrap();
        let completion = held_normal_return(fixture, selection, deadline);
        let stable = source.view().unwrap();
        let raw = native_repair_receiver_content_files(&root);
        let other = fixture
            .runtime
            .selected_original_data_layout(&fixture.promoted)
            .unwrap()
            .queue()
            .unwrap();
        let other_before = other.view().unwrap();
        let error = other
            .retain_native_owned_repair_completion(&completion)
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("original attempt witness belongs to another resident queue owner"),
            "{error}"
        );
        assert_eq!(other.view().unwrap(), other_before);
        let swapped = QueueSwap::install(&root);
        let copied = native_repair_receiver_content_files(&root);
        let error = source
            .retain_native_owned_repair_completion(&completion)
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("selected original fixture was physically replaced"),
            "{error}"
        );
        assert_eq!(native_repair_receiver_content_files(&root), copied);
        drop(swapped);
        assert_eq!(source.view().unwrap(), stable);
        assert_eq!(native_repair_receiver_content_files(&root), raw);
        raw_reply_refuses(
            fixture,
            completion.returned_frame(),
            "selected original Repair response lacks native owned-return provenance",
            deadline,
        );
        assert!(
            Arc::ptr_eq(&selection.captured_completion().unwrap(), &completion),
            "the actual typed return survives an unproven generic reply"
        );
        assert!(selection.retained_reference().is_none());
        source
            .record_original_repair_attempt(completion.carrier())
            .unwrap();
        let stable = source.view().unwrap();
        let raw = native_repair_receiver_content_files(&root);
        let error = source
            .retain_native_owned_repair_completion(&completion)
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("native completion custody has a changed or ambiguous persisted attempt"),
            "{error}"
        );
        assert_eq!(source.view().unwrap(), stable);
        assert_eq!(native_repair_receiver_content_files(&root), raw);
        assert!(selection.retained_reference().is_none());
        assert_no_effects(fixture, &before);
        drop(completion);
    });
}
