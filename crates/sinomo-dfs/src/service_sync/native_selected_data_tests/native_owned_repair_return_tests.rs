//! Actual owned native returns. No candidate promotion, application or closure.
use super::*;
use crate::test_runtime::RpcObservation;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

fn with_original(name: &str, check: impl FnOnce(&mut GenuineNativeOriginal)) {
    // Ordinary receiver/wire deliberately avoids claiming selected Terminal
    // replay or dedup activation. The real original's first reply was lost.
    let mut fixture = genuine_native_original(name);
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| check(&mut fixture)));
    if outcome.is_err() {
        let diagnostic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            fixture.runtime.checkpoint_rpc_observation_window()
        }));
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match diagnostic {
            Ok(result) => eprintln!("NATIVE_OWNED_REPAIR_RETURN_FAILURE_RPC {result:?}"),
            Err(_) => {
                eprintln!("NATIVE_OWNED_REPAIR_RETURN_FAILURE_RPC unavailable: checkpoint panicked")
            }
        }));
    }
    drop(fixture.held.take());
    drop(fixture.held_promoted.take());
    let closed = fixture.runtime.close();
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
    closed.unwrap();
}

fn source_facts(fixture: &GenuineNativeOriginal) -> serde_json::Value {
    let original = &fixture.birth.original;
    let DfsNetworkMessage::RepairReplica(command) = &original.message else {
        panic!("actual original Repair");
    };
    let queue = fixture
        .runtime
        .selected_original_data_layout(&original.source_node_id)
        .unwrap()
        .queue()
        .unwrap();
    let view = queue.view().unwrap();
    let birth = queue.original_repair(original).unwrap().unwrap();
    assert!(birth.observed.is_empty() && birth.closed_at_sequence.is_none());
    assert!(
        queue
            .closed_repair_birth(&birth.root.subject)
            .unwrap()
            .is_none()
    );
    fixture
        .runtime
        .client(&original.source_node_id)
        .unwrap()
        .content()
        .inspect_repair_payload_root(&birth.root)
        .unwrap();
    let primary = fixture
        .runtime
        .metadata_primary_service("/services/selected-original-data.bin")
        .unwrap();
    let backend = primary.metadata_backend().unwrap();
    let object = backend.get_object(&command.object_id).unwrap().unwrap();
    let receipts = backend
        .receipts_for_version(&command.object_id, &command.version)
        .unwrap();
    assert!(
        !object.current_locations.contains(&fixture.holder)
            && !receipts
                .iter()
                .any(|receipt| receipt.replica_node_id == fixture.holder),
        "the lost original reply has not already published C at A"
    );
    serde_json::json!({
        "head": view.head_sequence, "rows": view.records, "birth": birth,
        "object": object,
        "version": backend.get_version(&command.object_id, &command.version).unwrap(),
        "receipts": receipts,
        "intents": backend.intents_for_object(&command.object_id).unwrap(),
    })
}

fn last_call(runtime: &DfsTestRuntime) -> u64 {
    assert_eq!(runtime.observation_dropped_count(), 0);
    runtime
        .rpc_observations()
        .iter()
        .map(|call| call.call_id)
        .max()
        .unwrap_or(0)
}

fn same_call(call: &RpcObservation, original: &DfsRoutedMessage, after: u64) -> bool {
    let DfsNetworkMessage::RepairReplica(command) = &original.message else {
        panic!("actual original Repair");
    };
    call.call_id > after
        && call.source == original.source_node_id
        && call.target == original.target_node_id
        && call.route == DFS_DATA_REPAIR_REPLICA_ROUTE
        && call.origin == RpcOrigin::RegisteredHost
        && call.dfs_frame_id == Some(original.frame_id)
        && call.dfs_request_id.as_deref() == Some(command.request_id.as_str())
        && call.dfs_intent_id.as_deref() == Some(command.intent_id.as_str())
        && call.dfs_file_system_id.as_ref() == Some(&command.file_system_id)
        && call.dfs_object_id.as_ref() == Some(&command.object_id)
        && call.dfs_version.as_ref() == Some(&command.version)
}

fn assert_actual_completed(
    runtime: &DfsTestRuntime,
    original: &DfsRoutedMessage,
    after: u64,
) -> u64 {
    let calls = runtime.rpc_observations();
    let completed: Vec<_> = calls
        .iter()
        .filter(|call| same_call(call, original, after) && call.phase == RpcPhase::Completed)
        .collect();
    assert_eq!(
        completed.len(),
        1,
        "one actual owned native call, no hidden retry: {completed:?}"
    );
    let call_id = completed[0].call_id;
    assert!(completed[0].outcome_error.is_none());
    assert!(matches!(completed[0].signer.as_ref(),
        Some(sinomo_core::service::ServiceSigner::Remote { node_id, .. })
        if node_id == original.source_node_id.as_str()));
    assert!(calls.iter().any(|call| same_call(call, original, after)
        && call.call_id == call_id
        && call.phase == RpcPhase::AfterDispatch));
    assert!(!calls.iter().any(|call| same_call(call, original, after)
        && call.call_id == call_id
        && call.phase == RpcPhase::EndedWithoutResponse));
    assert_eq!(runtime.observation_dropped_count(), 0);
    call_id
}

fn options(deadline: Instant) -> DfsObjectRpcSendOptions {
    DfsObjectRpcSendOptions::new(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
        .with_overall_timeout(
            deadline
                .checked_duration_since(Instant::now())
                .expect("same original 5s call budget"),
        )
}

#[test]
fn native_owned_original_repair_return_keeps_exact_call_and_source_state() {
    with_original("native-owned-original-return", |fixture| {
        let original = fixture.birth.original.clone();
        let material = fixture.birth.original_wire_material().unwrap();
        let queue = fixture
            .runtime
            .selected_original_data_layout(&original.source_node_id)
            .unwrap()
            .queue()
            .unwrap();
        let before = source_facts(fixture);
        let witness = queue
            .capture_original_repair_attempt_witness(&original, &material)
            .unwrap();
        let sequence = witness.attempt_sequence();
        let incarnation = witness.queue_incarnation().to_owned();
        assert_eq!(witness.carrier(), &original);
        assert_eq!(witness.material(), &material);
        assert_eq!(witness.payload_root(), &fixture.birth.root);
        let first = last_call(&fixture.runtime);
        let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        let completion = crate::runtime_host::call_native_original_repair_owned(
            fixture.runtime.data_dir(&original.source_node_id),
            witness,
            options(deadline),
        )
        .unwrap();
        assert!(
            Instant::now() <= deadline,
            "late return cannot prove this 5s call"
        );
        assert_eq!(completion.carrier(), &original);
        assert_eq!(completion.material(), &material);
        assert_eq!(completion.attempt_sequence(), sequence);
        assert_eq!(completion.queue_incarnation(), incarnation);
        assert_eq!(
            completion.rpc_request_id(),
            format!("dfs_message_{}", original.frame_id)
        );
        let returned = completion.returned_frame();
        assert_eq!(returned.source_node_id, fixture.holder);
        assert_eq!(returned.target_node_id, original.source_node_id);
        let DfsNetworkMessage::RepairReplicaResponse(response) = &returned.message else {
            panic!("actual C Repair return");
        };
        let command = &material.original.command;
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
                receipt_generation: command.receipt_generation,
            }
        );
        // C's routed frame/message counters are preserved, never equated with
        // A's carrier counters. The actual outer RPC binds A's Attempt above.
        assert_actual_completed(&fixture.runtime, &original, first);
        assert_eq!(
            fixture
                .runtime
                .client(&fixture.holder)
                .unwrap()
                .content()
                .read_object_version(command.object_id.as_str(), command.version.as_str())
                .unwrap()
                .1,
            vec![0x5a; 4096]
        );
        assert_eq!(
            source_facts(fixture),
            before,
            "a return capability cannot observe/apply/Close, create an A receipt, rewrite the original or release its root"
        );
        assert!(
            Instant::now() <= deadline,
            "all exact-return checks retain the original call budget"
        );
    });
}

#[test]
fn native_owned_original_repair_return_refuses_another_native_producer() {
    with_original("native-owned-original-wrong-producer", |fixture| {
        let original = fixture.birth.original.clone();
        let material = fixture.birth.original_wire_material().unwrap();
        let queue = fixture
            .runtime
            .selected_original_data_layout(&original.source_node_id)
            .unwrap()
            .queue()
            .unwrap();
        let witness = queue
            .capture_original_repair_attempt_witness(&original, &material)
            .unwrap();
        let before = source_facts(fixture);
        let other_queue = fixture
            .runtime
            .selected_original_data_layout(&fixture.promoted)
            .unwrap()
            .queue()
            .unwrap();
        let other_before = other_queue.view().unwrap();
        let first = last_call(&fixture.runtime);
        let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        let error = match crate::runtime_host::call_native_original_repair_owned(
            fixture.runtime.data_dir(&fixture.promoted),
            witness,
            options(deadline),
        ) {
            Ok(_) => panic!("B cannot use A's opaque original attempt"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("native original Repair completion original carrier does not belong to this native producer"), "{error}");
        assert!(Instant::now() <= deadline);
        assert!(
            !fixture
                .runtime
                .rpc_observations()
                .iter()
                .any(|call| call.call_id > first && call.route == DFS_DATA_REPAIR_REPLICA_ROUTE),
            "wrong native producer must fail before RPC"
        );
        assert_eq!(source_facts(fixture), before);
        assert_eq!(other_queue.view().unwrap(), other_before);
        assert_eq!(fixture.runtime.observation_dropped_count(), 0);
    });
}

#[test]
fn native_owned_original_repair_return_refuses_ambiguous_persisted_attempt() {
    with_original("native-owned-original-wrong-attempt", |fixture| {
        let original = fixture.birth.original.clone();
        let material = fixture.birth.original_wire_material().unwrap();
        let queue = fixture
            .runtime
            .selected_original_data_layout(&original.source_node_id)
            .unwrap()
            .queue()
            .unwrap();
        let witness = queue
            .capture_original_repair_attempt_witness(&original, &material)
            .unwrap();
        let before_attempts = queue
            .original_repair(&original)
            .unwrap()
            .unwrap()
            .attempts
            .len();
        // The ordinary writer really persists a second sequence for these same
        // carrier IDs. No token or authenticated return is fabricated by this cut.
        queue.record_original_repair_attempt(&original).unwrap();
        let current = queue.original_repair(&original).unwrap().unwrap();
        assert_eq!(current.attempts.len(), before_attempts + 1);
        assert_eq!(current.original_wire_material().unwrap(), material);
        let before = source_facts(fixture);
        let first = last_call(&fixture.runtime);
        let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        let error = match crate::runtime_host::call_native_original_repair_owned(
            fixture.runtime.data_dir(&original.source_node_id),
            witness,
            options(deadline),
        ) {
            Ok(_) => panic!("ambiguous persisted carrier cannot supply a native completion"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("original attempt witness has ambiguous persisted carrier attempts"),
            "{error}"
        );
        assert!(Instant::now() <= deadline);
        assert!(
            !fixture
                .runtime
                .rpc_observations()
                .iter()
                .any(|call| call.call_id > first && call.route == DFS_DATA_REPAIR_REPLICA_ROUTE),
            "invalid Attempt must fail before RPC"
        );
        assert_eq!(source_facts(fixture), before);
        assert_eq!(fixture.runtime.observation_dropped_count(), 0);
    });
}

struct ReleaseReturn(Arc<tokio::sync::Semaphore>);
impl Drop for ReleaseReturn {
    fn drop(&mut self) {
        self.0.add_permits(1);
    }
}

/// Swap only the selected queue directory, keeping every original byte under
/// the fixture's ownership. The byte-equal copy is never admitted as an owner.
struct QueueReplacement {
    root: PathBuf,
    parked: PathBuf,
    rejected: PathBuf,
}
impl QueueReplacement {
    fn install(root: &Path) -> Self {
        let parent = root.parent().unwrap();
        let parked = parent.join("native-owned-return-original-queue");
        let rejected = parent.join("native-owned-return-rejected-copy");
        let copy = parent.join("native-owned-return-copy");
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
        // Arm restoration BEFORE the first rename. Any later assertion/unwind
        // restores the original before the runtime's normal close.
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
impl Drop for QueueReplacement {
    fn drop(&mut self) {
        if self.parked.exists() {
            if self.root.exists() {
                std::fs::rename(&self.root, &self.rejected)
                    .expect("retain unadmitted replacement queue");
            }
            std::fs::rename(&self.parked, &self.root)
                .expect("restore exact original queue before runtime close");
        }
    }
}

#[test]
fn native_owned_original_repair_return_refuses_queue_replacement_after_real_return() {
    with_original("native-owned-original-replaced-queue", |fixture| {
        let original = fixture.birth.original.clone();
        let material = fixture.birth.original_wire_material().unwrap();
        let layout = fixture
            .runtime
            .selected_original_data_layout(&original.source_node_id)
            .unwrap();
        let queue = layout.queue().unwrap();
        let root = layout.queue_layout().root().to_owned();
        let before = source_facts(fixture);
        let before_files = native_repair_receiver_content_files(&root);
        let first = last_call(&fixture.runtime);
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        fixture.runtime.install_fault(FaultRule {
            source: Some(original.source_node_id.clone()),
            target: Some(fixture.holder.clone()),
            route: Some(DFS_DATA_REPAIR_REPLICA_ROUTE.into()),
            call_id: None,
            origin: Some(RpcOrigin::RegisteredHost),
            remaining_hits: 1,
            action: FaultAction::PauseAfterDispatch(Arc::clone(&gate)),
        });
        let data_dir = fixture.runtime.data_dir(&original.source_node_id).clone();
        let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        std::thread::scope(|scope| {
            let worker_original = original.clone();
            let worker = scope.spawn(move || {
                let witness = queue
                    .capture_original_repair_attempt_witness(&worker_original, &material)
                    .unwrap();
                crate::runtime_host::call_native_original_repair_owned(
                    &data_dir,
                    witness,
                    options(deadline),
                )
                .map(|_| ())
                .map_err(|error| {
                    let possibly_applied = matches!(
                        &error,
                        DfsError::NetworkDelivery {
                            delivery_state:
                                sinomo_core::network::NetworkDeliveryState::PossiblyApplied,
                            ..
                        }
                    );
                    (possibly_applied, error.to_string())
                })
            });
            // This guard drops before scope's implicit join on every panic.
            let release = ReleaseReturn(Arc::clone(&gate));
            let paused_call = loop {
                let after_dispatch = fixture.runtime.rpc_observations().into_iter().find(|call| {
                    same_call(call, &original, first) && call.phase == RpcPhase::AfterDispatch
                });
                if let Some(call) = after_dispatch {
                    break call.call_id;
                }
                assert!(
                    !worker.is_finished(),
                    "native call ended before its actual C-return pause"
                );
                let remaining = deadline
                    .checked_duration_since(Instant::now())
                    .expect("same 5s call budget reaches actual response pause");
                std::thread::sleep(remaining.min(Duration::from_millis(5)));
            };
            assert!(
                !fixture
                    .runtime
                    .rpc_observations()
                    .iter()
                    .any(|call| call.call_id == paused_call && call.phase == RpcPhase::Completed)
            );
            let replacement = QueueReplacement::install(&root);
            assert!(
                layout.check().is_err(),
                "byte-equal replacement is not the original physical queue"
            );
            drop(release);
            let result = worker.join().expect("owned native call worker joins");
            let (possibly_applied, error) =
                result.expect_err("no capability may cross physical queue replacement");
            assert!(
                possibly_applied,
                "a real completed RPC must remain PossiblyApplied: {error}"
            );
            assert!(
                error.contains("selected original fixture was physically replaced"),
                "{error}"
            );
            assert_eq!(
                assert_actual_completed(&fixture.runtime, &original, first),
                paused_call
            );
            assert!(
                Instant::now() <= deadline,
                "late response cannot satisfy the physical-owner control"
            );
            drop(replacement);
        });
        fixture.runtime.clear_faults();
        assert_eq!(native_repair_receiver_content_files(&root), before_files);
        assert_eq!(
            source_facts(fixture),
            before,
            "the completed but refused return cannot alter A's observation, receipt, original or retained root"
        );
    });
}
