//! A real carrier constructed before the fence, dispatched only after it.
use super::*;
use crate::test_runtime::{FaultAction, FaultRule, RpcOrigin, RpcPhase};
use std::collections::BTreeSet;
use std::sync::Arc;

#[test]
fn native_local_recovery_fence_refuses_original_inflight_carrier_after_install() {
    with_native("native-local-fence-inflight", true, |fixture| {
        let path = "/services/native-local-recovery-fence.bin";
        let record = fixture.seed(path);
        fixture.no_cooperative_marker();
        let shard_id = fixture.shard_id();
        let primary = fixture.shard.primary_node_id.clone();
        let target = fixture.acceptor.clone();
        let mut source = fixture.runtime.metadata_primary_service(path).unwrap();
        let backend = source.metadata_backend_mut().unwrap();
        backend
            .begin_consistent_read_snapshot_on_shard(&shard_id)
            .unwrap();
        let current = backend.prepared_operation_record(&record.digest.operation_id);
        let prefix = backend.metadata_prefix_proof(&shard_id);
        backend.end_consistent_read_snapshot_on_shard(&shard_id);
        assert_eq!(current.unwrap(), Some(record.clone()));
        let prefix = prefix.unwrap().unwrap();
        let backend = source.metadata_backend().unwrap();
        let batch = backend
            .journal_batch_for_shard_since_limited(
                &shard_id,
                0,
                crate::DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
                crate::DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
            )
            .unwrap()
            .unwrap();
        assert!(!batch.entries.is_empty());
        assert!(batch.entries.len() < crate::DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT);
        assert_eq!(batch.next_after_sequence, prefix.journal_sequence);
        let frames =
            crate::metadata_replication::prepare_dfs_metadata_journal_frame_batch(backend, &batch)
                .unwrap();
        drop(source);
        let digest = record.digest;
        let probe = DfsMetadataAuthorityProbe {
            probe_id: "native-inflight-local-fence-authority".into(),
            expected_shard: fixture.shard.clone(),
            operation: DfsMetadataAuthorityOperation::PreparedOperation {
                operation_id: digest.operation_id,
                file_system_id: digest.file_system_id,
                shard_id: digest.shard_id,
                owner_epoch: digest.owner_epoch,
                prepare_sequence: digest.prepare_sequence,
                payload_digest: digest.payload_digest,
            },
            prepared_prefix: prefix.clone(),
        };
        let command = DfsMetadataJournalBatchCommand {
            request_id: "native-inflight-local-fence-request".into(),
            source_node_id: Some(primary.clone()),
            file_system_id: Some(fixture.shard.file_system_id.clone()),
            authority_epoch: Some(fixture.shard.epoch),
            verify_prefix_only: false,
            prefix_probe: None,
            authority_probe: Some(probe.clone()),
            durable_replication: None,
            batch: frames,
        };
        let before = send_native(&fixture.runtime, &primary, &target, &command).unwrap();
        assert_eq!(before.result, DfsMetadataJournalBatchResult::Applied);
        assert_eq!(
            before.verified_authority_probe_id,
            crate::service_replica::metadata_authority_probe_confirmation_id(&probe)
        );

        let mut receiver = fixture
            .runtime
            .local_shard_copy_service(&target, &fixture.shard)
            .unwrap();
        assert!(
            receiver
                .native_publication_owner_for_observation()
                .is_some()
        );
        let target_store = fixture.node_store(&target);
        let backend: &dyn DfsMetadataBackend = &target_store;
        assert!(
            backend
                .metadata_shard_write_freeze(&shard_id)
                .unwrap()
                .is_none()
        );
        let prefix_floor = backend
            .metadata_prefix_ack_floor(&shard_id)
            .unwrap()
            .unwrap();
        let authority_floor = backend
            .metadata_authority_ack_floor(&shard_id)
            .unwrap()
            .unwrap();
        drop(target_store);
        assert_eq!(prefix_floor.durable_prefix_proof, prefix);
        assert_eq!(authority_floor.durable_prefix_proof, prefix);
        let retained_frames = exact_frames(&fixture.runtime, &target, &fixture.shard);
        let request = fixture.request();
        fixture.reserve(&request);
        let serving = fixture.serving_cells();

        // Construction on the actual registered host records this exact carrier
        // synchronously. It is not polled until its one-call pause is installed.
        let host =
            crate::runtime_host::registered_dfs_runtime_host(fixture.runtime.data_dir(&primary))
                .unwrap()
                .unwrap();
        let old_calls = fixture
            .runtime
            .rpc_observations()
            .into_iter()
            .map(|item| item.call_id)
            .collect::<BTreeSet<_>>();
        let cut = deadline();
        let call = crate::dfs_metadata_journal_batch_rpc_call(&command).unwrap();
        let pending = host.object_rpc.call_async(
            &target,
            call,
            crate::DfsObjectRpcSendOptions::default()
                .with_overall_timeout(cut.saturating_duration_since(Instant::now()))
                .cluster_rpc_options(),
            true,
        );
        let constructed = fixture
            .runtime
            .rpc_observations()
            .into_iter()
            .filter(|item| {
                item.source == primary
                    && item.target == target
                    && item.route == crate::DFS_METADATA_JOURNAL_BATCH_ROUTE
                    && item.origin == RpcOrigin::RegisteredHost
                    && item.phase == RpcPhase::Constructed
                    && !old_calls.contains(&item.call_id)
            })
            .collect::<Vec<_>>();
        assert_eq!(
            constructed.len(),
            1,
            "identify the actual unpolled native carrier"
        );
        let call_id = constructed[0].call_id;
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        fixture.runtime.install_fault(FaultRule {
            source: Some(primary.clone()),
            target: Some(target.clone()),
            route: Some(crate::DFS_METADATA_JOURNAL_BATCH_ROUTE.into()),
            call_id: Some(call_id),
            origin: Some(RpcOrigin::RegisteredHost),
            remaining_hits: 1,
            action: FaultAction::PauseBeforeDispatch(Arc::clone(&gate)),
        });
        struct ReleaseOriginalGate(Option<Arc<tokio::sync::Semaphore>>);
        impl ReleaseOriginalGate {
            fn release(&mut self) {
                if let Some(gate) = self.0.take() {
                    gate.add_permits(1);
                }
            }
        }
        impl Drop for ReleaseOriginalGate {
            fn drop(&mut self) {
                self.release();
            }
        }
        let runtime = fixture.runtime.runtime_handle();
        let (installed, result) = std::thread::scope(|scope| {
            // This guard drops on panic BEFORE scope's automatic child join.
            let mut release = ReleaseOriginalGate(Some(gate));
            let child = scope.spawn(move || {
                runtime.block_on(async move {
                    tokio::time::timeout_at(tokio::time::Instant::from_std(cut), async move {
                        let result = pending.await.map_err(|error| error.to_string())?;
                        let response = result
                            .into_materialized_value_response(64 * 1024)
                            .await
                            .map_err(|error| error.to_string())?;
                        crate::decode_dfs_metadata_journal_batch_rpc_response(&response)
                            .map_err(|error| error.to_string())
                    })
                    .await
                    .map_err(|_| "original five-second native RPC deadline expired".to_string())?
                })
            });
            loop {
                let observations = fixture.runtime.rpc_observations();
                if observations
                    .iter()
                    .any(|item| item.call_id == call_id && item.phase == RpcPhase::BeforeDispatch)
                {
                    assert!(!child.is_finished(), "original carrier remains held");
                    assert!(
                        !observations.iter().any(|item| item.call_id == call_id
                            && matches!(
                                item.phase,
                                RpcPhase::AfterDispatch | RpcPhase::Decoded | RpcPhase::Completed
                            )),
                        "no receiver effect or ACK precedes the fence"
                    );
                    break;
                }
                assert!(
                    Instant::now() < cut,
                    "observe the same original carrier within 5s"
                );
                assert!(
                    !child.is_finished(),
                    "native call ended before its selected cut"
                );
                std::thread::sleep(Duration::from_millis(1));
            }
            let installed = receiver
                .install_selected_local_recovery_fence(&request, cut)
                .unwrap();
            assert!(installed.reservation_current);
            assert!(installed.post_verification_error.is_none());
            assert_eq!(installed.data.fence.acceptor_node_id, target);
            assert_eq!(installed.data.fence.prefix, prefix);
            release.release();
            let result = child.join().unwrap();
            assert!(Instant::now() <= cut, "no renewed RPC/install budget");
            (installed, result)
        });
        fixture.runtime.clear_faults();
        let error = match result {
            Ok(response) => {
                assert_eq!(response.request_id, command.request_id);
                assert_eq!(response.shard_id, command.batch.shard_id);
                assert_eq!(
                    response.next_after_sequence,
                    command.batch.next_after_sequence
                );
                assert!(!response.verified_prefix_only);
                assert!(response.verified_authority_probe_id.is_none());
                match response.result {
                    DfsMetadataJournalBatchResult::Rejected { reason, .. } => reason,
                    DfsMetadataJournalBatchResult::Applied => {
                        panic!("held native call received ACK after fence")
                    }
                }
            }
            Err(error) => error,
        };
        assert!(
            error.contains("recovery fence"),
            "exact native refusal required: {error}"
        );
        let backend = receiver.metadata_backend().unwrap();
        assert_eq!(
            backend.metadata_prefix_proof(&shard_id).unwrap(),
            Some(prefix)
        );
        assert_eq!(
            backend.metadata_prefix_ack_floor(&shard_id).unwrap(),
            Some(prefix_floor)
        );
        assert_eq!(
            backend.metadata_authority_ack_floor(&shard_id).unwrap(),
            Some(authority_floor)
        );
        same_data(
            &backend
                .selected_local_recovery_fence(&shard_id)
                .unwrap()
                .unwrap(),
            &installed.data,
        );
        assert_eq!(
            exact_frames(&fixture.runtime, &target, &fixture.shard),
            retained_frames
        );
        drop(receiver);
        assert_eq!(fixture.serving_cells(), serving);
        assert_eq!(fixture.runtime.observation_dropped_count(), 0);
        eprintln!(
            "native pre-dispatch cut call={call_id} source={primary} target={target}: {error}; no prior ACK or delayed primary decision claim"
        );
    });
}
