//! A local committed V7 frame and its later dissemination are distinct facts.
use super::*;
use crate::test_runtime::{DfsTestRuntime, FaultAction, FaultRule, RpcOrigin, RpcPhase};

#[derive(Clone, Debug, Eq, PartialEq)]
struct Evidence {
    record: DfsPreparedOperationRecord,
    prepare_frames: Vec<Vec<u8>>,
    commit_frames: Vec<Vec<u8>>,
    base: Option<DfsVersion>,
    content_hash: Option<String>,
}

fn inspect(
    runtime: &DfsTestRuntime,
    node: &NodeId,
    shard: &DfsMetadataShardId,
    operation: &str,
    object: &DfsObjectId,
    intent: &str,
) -> Option<Evidence> {
    let physical_root = crate::local_dfs_metadata_shard_path(runtime.data_dir(node), shard);
    crate::local_state::with_current_owned_dfs_shard_inspection(
        runtime.data_dir(node),
        shard,
        None,
        |backend| {
            let Some(record) = backend.prepared_operation_record(operation)? else {
                return Ok(None);
            };
            // The owning inspection already holds this exact root's physical
            // fence/path registration. The backend journal wrapper takes the
            // non-reentrant writer lock again; use its existing read-only
            // durable readers ONLY inside this already-fenced closure.
            let entries = crate::metadata_journal::durable_journal_entries_since_limited(
                &physical_root,
                0,
                DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            )?;
            let batch = crate::metadata_journal::bounded_metadata_journal_batch(
                shard.clone(),
                0,
                entries,
                DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
                DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
            )?
            .expect("actual retained native shard journal");
            assert_eq!(&batch.shard_id, shard);
            assert!(
                batch.entries.len() < DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
                "small fixture must expose its complete actual frame prefix"
            );
            let frames = crate::metadata_journal::read_segmented_journal_frames_at_sequences(
                &physical_root,
                &batch
                    .entries
                    .iter()
                    .map(|entry| entry.sequence)
                    .collect::<Vec<_>>(),
            )?;
            assert_eq!(frames.len(), batch.entries.len());
            let mut prepare_frames = Vec::new();
            let mut commit_frames = Vec::new();
            for (entry, frame) in batch.entries.iter().zip(frames) {
                let frame =
                    frame.expect("selected actual journal entry retains its immutable exact frame");
                assert_eq!(frame.entry(), entry);
                assert_eq!(&entry.shard_id, shard);
                match &entry.mutation {
                    DfsMetadataMutation::PrepareOperationV7 { operation_id, .. }
                        if operation_id == operation =>
                    {
                        prepare_frames.push(frame.exact_bytes().to_vec());
                    }
                    DfsMetadataMutation::CommitPreparedOperationV7 { operation_id, .. }
                        if operation_id == operation =>
                    {
                        commit_frames.push(frame.exact_bytes().to_vec());
                    }
                    _ => {}
                }
            }
            let actual = backend
                .intents_for_object(object)?
                .into_iter()
                .find(|record| record.intent_id == intent)
                .or_else(|| record.payload.terminal_intent.clone());
            Ok(Some(Evidence {
                record,
                prepare_frames,
                commit_frames,
                base: actual
                    .as_ref()
                    .and_then(|record| record.base_version.clone()),
                content_hash: actual.and_then(|record| record.content_hash),
            }))
        },
    )
    .unwrap()
    .flatten()
}

struct ReleaseCommitGates {
    gates: Vec<Arc<tokio::sync::Semaphore>>,
    released: bool,
}
impl ReleaseCommitGates {
    fn release(&mut self) {
        if !self.released {
            for gate in &self.gates {
                gate.add_permits(1);
            }
            self.released = true;
        }
    }
}
impl Drop for ReleaseCommitGates {
    fn drop(&mut self) {
        self.release();
    }
}

#[test]
fn native_local_commit_reply_loss_and_delayed_peer_commit_replay_same_operation() {
    let name = "native-local-commit-delivery";
    let (mut runtime, _) = DfsTestRuntime::start_cluster(name, 3).unwrap();
    let path = "/services/native-commit-delivery.bin";
    let config = crate::select_metadata_shard_config_for_path(
        &runtime.file_system().file_system_id,
        &runtime.metadata_shards().unwrap(),
        path,
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        config.replica_node_ids.len(),
        2,
        "three real assigned metadata participants"
    );
    let owner = config.primary_node_id.clone();
    let shard = DfsMetadataShardId::new(&config.shard_id);
    let mut policy = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 1).unwrap();
    policy.required_node_ids = vec![owner.clone()];
    runtime
        .apply_placement_policy(&owner, path, DfsPlacementScope::SelfOnly, policy)
        .unwrap();
    let options = DfsWriteOptions {
        policy: DfsReplicationPolicy::new(1, 1).unwrap(),
        replica_candidates: vec![owner.clone()],
        wait_for_commit: true,
        foreground_replica_repair: true,
    };
    let mut caller = runtime.client(&owner).unwrap();
    assert!(
        caller.transport.delegates_local_namespace_authority(),
        "use admitted Main, not Core"
    );
    let seed = caller
        .posix_write_file(path, &[0x31; 4096], options.clone())
        .unwrap();
    caller
        .posix_sync_file(path, DfsPosixSyncKind::File)
        .unwrap();
    let operation = format!("commit-delivery-{}", sinomo_core::Id::generate());
    let intent = crate::intent::stable_write_intent_id(&owner, &seed.object_id, &operation);
    let metadata_operation = format!("write:{intent}");
    let body = vec![0x72; 4096];
    let gates: Vec<_> = config
        .replica_node_ids
        .iter()
        .map(|_| Arc::new(tokio::sync::Semaphore::new(0)))
        .collect();
    let held: Vec<_> = config
        .replica_node_ids
        .iter()
        .map(|_| Arc::new(Mutex::new(Vec::<DfsMetadataJournalBatchCommand>::new())))
        .collect();
    let mut release = ReleaseCommitGates {
        gates: gates.clone(),
        released: false,
    };
    for ((peer, gate), captured) in config.replica_node_ids.iter().zip(&gates).zip(&held) {
        runtime.install_fault(FaultRule {
            source: Some(owner.clone()),
            target: Some(peer.clone()),
            route: Some(DFS_METADATA_JOURNAL_BATCH_ROUTE.into()),
            origin: Some(RpcOrigin::RegisteredHost),
            call_id: None,
            remaining_hits: 1,
            action: FaultAction::PauseExactWriteCommitPublication {
                operation_id: metadata_operation.clone(),
                gate: Arc::clone(gate),
                captured: Arc::clone(captured),
            },
        });
    }
    let replies = Arc::new(Mutex::new(Vec::<DfsRoutedMessage>::new()));
    runtime.install_fault(FaultRule {
        source: Some(owner.clone()),
        target: Some(owner.clone()),
        route: Some(DFS_DATA_DISTRIBUTED_WRITE_ROUTE.into()),
        origin: None,
        call_id: None,
        remaining_hits: 1,
        action: FaultAction::CaptureWriteReplyAndLoseAfterDispatch(Arc::clone(&replies)),
    });
    let deadline = Instant::now() + DEFAULT_DFS_POSIX_SYNC_TIMEOUT;
    let previous = caller.set_transport_operation_deadline(Some(deadline));
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // Every retry supplies the SAME explicit original object/base, body,
        // options and semantic ID. Existing generation may already equal target.
        let first = caller.posix_replace_file_if_version_from_reader_with_operation_id(
            path,
            &seed.object_id,
            &seed.version,
            std::io::Cursor::new(body.clone()),
            options.clone(),
            &operation,
        );
        eprintln!("COMMIT_DELIVERY_FIRST_CALL {first:?}");
        let local = loop {
            if let Some(actual) = inspect(
                &runtime,
                &owner,
                &shard,
                &metadata_operation,
                &seed.object_id,
                &intent,
            )
            .filter(|actual| actual.record.status == DfsPreparedOperationStatus::Committed)
            {
                break actual;
            }
            assert!(
                Instant::now() < deadline,
                "same45s window includes actual local commit inspection"
            );
            std::thread::sleep(Duration::from_millis(5));
        };
        assert_eq!(local.prepare_frames.len(), 1);
        assert_eq!(local.commit_frames.len(), 1);
        assert_eq!(local.record.digest.operation_id, metadata_operation);
        assert_eq!(local.record.digest.file_system_id, config.file_system_id);
        assert_eq!(local.record.digest.shard_id, shard);
        assert_eq!(local.record.digest.owner_epoch, config.epoch);
        assert_eq!(
            local.record.digest.payload_digest,
            local.record.payload.canonical_digest().unwrap()
        );
        assert!(local.record.committed_entry.is_some());
        assert_eq!(local.base, Some(seed.version.clone()));
        let captured = replies.lock().unwrap().clone();
        assert_eq!(
            captured.len(),
            1,
            "one REAL completed domain reply is captured before loss"
        );
        assert_eq!(captured[0].source_node_id, owner);
        assert_eq!(captured[0].target_node_id, owner);
        let target_version = match &captured[0].message {
            DfsNetworkMessage::DistributedWriteResponse(response) => {
                assert_eq!(response.request_id, operation);
                assert_eq!(response.object_id, seed.object_id);
                let DfsDistributedWriteResult::Outcome {
                    outcome:
                        NetworkedDfsWriteOutcome::Committed {
                            intent_id,
                            version,
                            content_hash,
                            ..
                        },
                } = &response.result
                else {
                    panic!(
                        "capture must be actual Committed domain ACK: {:?}",
                        response.result
                    )
                };
                assert_eq!(intent_id, &intent);
                assert_eq!(Some(content_hash.clone()), local.content_hash);
                version.clone()
            }
            DfsNetworkMessage::WriteResponse(response) => {
                assert_eq!(response.request_id, operation);
                assert_eq!(response.object_id, seed.object_id);
                let DfsWriteResult::Committed { receipt, .. } = &response.result else {
                    panic!("actual Committed domain ACK")
                };
                receipt.committed_version.clone()
            }
            _ => panic!("only actual Write/DistributedWrite domain reply qualifies"),
        };
        match &first {
            Ok(stat) => {
                assert_eq!(stat.object_id, seed.object_id);
                assert_eq!(stat.version, target_version);
            }
            Err(error) => assert!(
                matches!(
                    error,
                    DfsPosixError::PendingWrite { .. }
                        | DfsPosixError::Dfs(DfsError::NetworkDelivery {
                            delivery_state:
                                sinomo_core::network::NetworkDeliveryState::PossiblyApplied,
                            ..
                        })
                ),
                "captured real committed reply may auto-reconcile or remain explicitly uncertain: {error}"
            ),
        }
        let observations = runtime.rpc_observations();
        assert!(
            observations.iter().any(|call| call.source == owner
                && call.target == owner
                && call.route == DFS_DATA_DISTRIBUTED_WRITE_ROUTE
                && call.phase == RpcPhase::EndedWithoutResponse
                && call
                    .outcome_error
                    .as_deref()
                    .is_some_and(|error| error.contains("PossiblyApplied"))),
            "transport loss is separate from the captured actual domain ACK and any automatic client reconciliation"
        );
        for (index, peer) in config.replica_node_ids.iter().enumerate() {
            loop {
                if !held[index].lock().unwrap().is_empty() {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "same45s window reaches both actual authenticated commit holds"
                );
                std::thread::sleep(Duration::from_millis(5));
            }
            let commands = held[index].lock().unwrap().clone();
            assert_eq!(commands.len(), 1);
            let command = &commands[0];
            assert_eq!(command.source_node_id.as_ref(), Some(&owner));
            assert_eq!(
                command.file_system_id.as_ref(),
                Some(&config.file_system_id)
            );
            assert_eq!(command.batch.shard_id, shard);
            let held_batch = command.batch.decoded().unwrap();
            let held_commit = held_batch
                .entries
                .iter()
                .zip(&command.batch.journal_frames)
                .find(|(entry, _)| {
                    matches!(&entry.mutation,
                    DfsMetadataMutation::CommitPreparedOperationV7 {operation_id,..}
                    if operation_id==&metadata_operation)
                })
                .unwrap()
                .1;
            assert_eq!(
                held_commit.exact_bytes.as_ref(),
                local.commit_frames[0].as_slice(),
                "the held authenticated RPC carries the actual immutable A commit frame"
            );
            assert!(
                held_batch
                    .entries
                    .iter()
                    .any(|entry| matches!(&entry.mutation,
                DfsMetadataMutation::CommitPreparedOperationV7 {operation_id,digest,..}
                if operation_id==&metadata_operation&&digest==&local.record.digest))
            );
            let actual = loop {
                if let Some(actual) = inspect(
                    &runtime,
                    peer,
                    &shard,
                    &metadata_operation,
                    &seed.object_id,
                    &intent,
                ) {
                    break actual;
                }
                assert!(Instant::now() < deadline);
                std::thread::sleep(Duration::from_millis(5));
            };
            assert_eq!(actual.record.status, DfsPreparedOperationStatus::Prepared);
            assert_eq!(actual.record.digest, local.record.digest);
            assert_eq!(actual.record.payload, local.record.payload);
            assert!(actual.record.committed_entry.is_none());
            assert_eq!(actual.prepare_frames, local.prepare_frames);
            assert!(actual.commit_frames.is_empty());
        }
        assert_eq!(runtime.observation_dropped_count(), 0);
        runtime.clear_faults();
        // Release these ORIGINAL held commit RPCs, never construct catch-up ACKs.
        release.release();
        let replay = caller
            .posix_replace_file_if_version_from_reader_with_operation_id(
                path,
                &seed.object_id,
                &seed.version,
                std::io::Cursor::new(body.clone()),
                options.clone(),
                &operation,
            )
            .expect("same exact operation replays its acknowledged outcome");
        assert_eq!(replay.object_id, seed.object_id);
        assert_eq!(replay.version, target_version);
        if let Ok(first) = first {
            assert_eq!(replay, first);
        }
        let after = inspect(
            &runtime,
            &owner,
            &shard,
            &metadata_operation,
            &seed.object_id,
            &intent,
        )
        .unwrap();
        assert_eq!(
            after, local,
            "retry cannot make a second target prepare/commit, timestamp, body or digest"
        );
        for peer in &config.replica_node_ids {
            loop {
                let actual = inspect(
                    &runtime,
                    peer,
                    &shard,
                    &metadata_operation,
                    &seed.object_id,
                    &intent,
                );
                if actual.as_ref().is_some_and(|actual| actual == &local) {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "actual peer frames must converge under same45s window: {actual:?}"
                );
                std::thread::sleep(Duration::from_millis(5));
            }
        }
        let restarted = config.replica_node_ids[0].clone();
        runtime.stop_node(&restarted).unwrap();
        runtime.reopen_node(restarted.clone()).unwrap();
        loop {
            let actual = inspect(
                &runtime,
                &restarted,
                &shard,
                &metadata_operation,
                &seed.object_id,
                &intent,
            );
            if actual.as_ref().is_some_and(|actual| actual == &local) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "same committed peer frames survive normal component reopen: {actual:?}"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(
            inspect(
                &runtime,
                &owner,
                &shard,
                &metadata_operation,
                &seed.object_id,
                &intent
            )
            .unwrap(),
            local
        );
        assert_eq!(caller.posix_read_file_distributed(path).unwrap(), body);
        assert_eq!(runtime.observation_dropped_count(), 0);
        eprintln!(
            "COMMIT_DELIVERY exactlocal={:?} firstCaller={:?} peerFullPrepareThenActualHeldCommit=true replay={replay:?}; this is no full-dissemination ACK requirement",
            local.record.digest, captured[0]
        );
    }));
    caller.set_transport_operation_deadline(previous);
    runtime.clear_faults();
    drop(release);
    drop(caller);
    if outcome.is_err() {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            eprintln!(
                "COMMIT_DELIVERY_FAILURE_RPC {:?}",
                runtime.checkpoint_rpc_observation_window()
            )
        }));
    }
    let closed = runtime.close();
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
    closed.unwrap();
}
