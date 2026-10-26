//! Complete observation sets only; no serving, vote or durable attestation.
use super::*;
use crate::node_transport::{DfsNativePrefixRoundPeerStatus, OwnedNativeMetadataPrefixRound};

fn target(fixture: &Fixture) -> crate::DfsMetadataQuorumSyncTarget {
    let probe = fixture.command.prefix_probe.as_ref().unwrap();
    crate::DfsMetadataQuorumSyncTarget {
        file_system_id: fixture.shard.file_system_id.clone(),
        shard_id: probe.shard_id.clone(),
        primary_node_id: fixture.shard.primary_node_id.clone(),
        authority_epoch: Some(fixture.shard.epoch),
        replica_node_ids: fixture.shard.replica_node_ids.clone(),
        expected_shard: Some(fixture.shard.clone()),
        journal_sequence: probe.journal_sequence,
        barrier_id: probe.barrier_id.clone(),
        state_fingerprint: probe.state_fingerprint.clone(),
        checkpoint_snapshot: None,
    }
}
fn observed(round: &OwnedNativeMetadataPrefixRound) -> serde_json::Value {
    serde_json::json!({"round_id":round.round_id(),"target":round.target(),
        "complete_capture":round.is_complete(),"finalization_error":round.finalization_error().map(ToString::to_string),
        "source_observation":round.common_source_observation().map(|s|{
            let p=s.physical_owner();serde_json::json!({"source":s.source_node_id(),"assignment":s.assignment(),
                "prefix":s.prefix(),"physical":{"data_root":p.data_root,"data_identity":p.data_identity,
                    "owner_identity":p.owner_identity,"shard_root":p.shard_root,"shard_identity":p.shard_identity},
                "qualification":"actual common source observation, not a self RPC ACK or current authority"})}),
        "peers":round.peer_outcomes().iter().map(|p|serde_json::json!({"peer":p.peer(),"command":p.command(),
            "status":format!("{:?}",p.status()),"attempted":p.attempted(),"response":p.response(),
            "native":p.native_return().map(projection),"error":p.error().map(ToString::to_string),
            "delivery_state":p.delivery_state().map(|s|format!("{s:?}")),"unattempted_reason":p.unattempted_reason()})).collect::<Vec<_>>(),
        "authority":"capture DATA only; no serving/genesis/C1/commit/quorum permission"})
}
fn run_round(
    label: &str,
    case: impl FnOnce(
        &mut Fixture,
        &mut serde_json::Value,
    ) -> TestResult<Vec<OwnedNativeMetadataPrefixRound>>,
) {
    let artifact = sinomo_testdir::TestDir::new(label);
    let mut fixture = Fixture::new(label).unwrap();
    let root = fixture.source_root();
    let owner = require_daemon_dfs_data_owner_lock(&DfsRuntimePaths::new(
        fixture.native.data_dir(&fixture.shard.primary_node_id),
    ))
    .unwrap();
    let weak = Arc::downgrade(&owner);
    drop(owner);
    let mut report = serde_json::json!({"scope":"one complete observation round on initialized native full3",
        "limits":"no new authority, durable state, serving/genesis/C1 or quietness proof","target":target(&fixture)});
    let checked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        case(&mut fixture, &mut report)
    }));
    report["all_rpc"] = wire(&fixture.native.rpc_observations());
    let closed = fixture.native.close();
    report["normal_close"] = serde_json::json!(closed);
    report["source_owner_released"] = serde_json::json!(weak.upgrade().is_none());
    if let Ok(Ok(values)) = &checked {
        report["historical_after_close"]=serde_json::json!(values.iter().map(|r|serde_json::json!({
            "round":observed(r),"live_bindings":r.peer_outcomes().iter().filter_map(|p|p.native_return())
                .map(|cap|cap.validate_binding(cap.peer(),cap.command(),&root).map_err(|e|e.to_string())).collect::<Vec<_>>()
        })).collect::<Vec<_>>());
    }
    let saved = (|| -> TestResult<()> {
        let bytes = serde_json::to_vec_pretty(&report)?;
        if bytes.len() > 4 * 1024 * 1024 {
            return Err("bounded native round evidence overflow".into());
        }
        let path = artifact.join("native-prefix-round-report.json");
        let mut f = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        f.write_all(&bytes)?;
        f.sync_all()?;
        eprintln!("NATIVE_PREFIX_ROUND_REPORT {}", path.display());
        Ok(())
    })();
    if let Err(panic) = checked {
        std::panic::resume_unwind(panic);
    }
    let values = checked.unwrap().unwrap();
    saved.unwrap();
    closed.unwrap();
    assert!(
        weak.upgrade().is_none(),
        "complete/incomplete observations may not pin the owner"
    );
    for round in values {
        for peer in round.peer_outcomes() {
            if let Some(cap) = peer.native_return() {
                assert!(
                    cap.validate_binding(cap.peer(), cap.command(), &root)
                        .is_err()
                );
            }
        }
    }
}
fn assert_complete(
    round: &OwnedNativeMetadataPrefixRound,
    expected: &crate::DfsMetadataQuorumSyncTarget,
    fixture: &Fixture,
) {
    assert!(round.is_complete());
    assert_eq!(round.target(), expected);
    assert!(round.finalization_error().is_none());
    let remotes = fixture
        .shard
        .replica_node_ids
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    assert_eq!(remotes.len(), 2);
    assert_eq!(
        round
            .peer_outcomes()
            .iter()
            .map(|p| p.peer().clone())
            .collect::<BTreeSet<_>>(),
        remotes
    );
    let source = round
        .common_source_observation()
        .expect("complete observation needs its actual common source");
    assert_eq!(source.source_node_id(), &fixture.shard.primary_node_id);
    assert_eq!(source.assignment(), &fixture.shard);
    assert_eq!(source.prefix().journal_sequence, expected.journal_sequence);
    assert_eq!(source.prefix().digest, expected.state_fingerprint);
    for slot in round.peer_outcomes() {
        assert_eq!(
            slot.status(),
            DfsNativePrefixRoundPeerStatus::NativeCaptured
        );
        assert!(slot.attempted());
        assert!(slot.error().is_none());
        assert!(slot.unattempted_reason().is_none());
        let cap = slot
            .native_return()
            .expect("raw response cannot replace native return");
        assert_eq!(cap.peer(), slot.peer());
        assert_eq!(cap.command(), slot.command());
        assert_eq!(Some(cap.response()), slot.response());
        assert_eq!(cap.source_assignment(), &fixture.shard);
        assert_eq!(cap.source_prefix(), source.prefix());
        assert_eq!(cap.local_physical_owner(), source.physical_owner());
        cap.validate_binding(slot.peer(), slot.command(), &fixture.source_root())
            .unwrap();
    }
}
#[test]
fn native_prefix_round_requires_exact_native_remote_set_and_releases_owner() {
    run_round("native-prefix-round-complete", |fixture, report| {
        let target = target(fixture);
        let marks = fixture.markers()?;
        let cursor = fixture.native.rpc_observations().len();
        let mut producer = fixture.transport();
        let deadline = Instant::now() + CAP;
        producer.set_operation_deadline(Some(deadline));
        let round = producer.capture_metadata_prefix_full_assignment_round(&target)?;
        report["actual_round"] = observed(&round);
        report["rpc"] = wire(&fixture.native.rpc_observations()[cursor..]);
        assert_complete(&round, &target, fixture);
        assert_eq!(round.deadline(), deadline);
        let events = fixture.native.rpc_observations();
        let events = &events[cursor..];
        for peer in &target.replica_node_ids {
            assert_eq!(own_calls(events, &target.primary_node_id, peer).len(), 1);
        }
        assert_eq!(
            round
                .peer_outcomes()
                .iter()
                .map(|p| p.command().request_id.clone())
                .collect::<BTreeSet<_>>()
                .len(),
            2
        );
        assert_eq!(fixture.markers()?, marks);
        assert_eq!(fixture.native.observation_dropped_count(), 0);
        drop(producer);
        Ok(vec![round])
    });
}

struct MatchingUnprovenPeer {
    calls: AtomicUsize,
}
impl DfsPeerTransport for MatchingUnprovenPeer {
    fn call_object_rpc(
        &self,
        _: &NodeId,
        call: ObjectRpcCall,
        _: crate::DfsObjectRpcSendOptions,
    ) -> Result<ObjectRpcResult, DfsPeerTransportError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let request = crate::node_transport::materialize_dfs_value_call_sync(call, 64 * 1024)?;
        let command: crate::DfsMetadataJournalBatchCommand = request
            .decode_body()
            .map_err(|e| DfsPeerTransportError::new(e.to_string()))?;
        ObjectRpcResponse::encode_body(
            &request.route,
            &crate::DfsMetadataJournalBatchResponse {
                request_id: command.request_id,
                shard_id: command.batch.shard_id,
                next_after_sequence: command.batch.next_after_sequence,
                applied_entries: 0,
                verified_prefix_only: true,
                verified_authority_probe_id: None,
                result: crate::DfsMetadataJournalBatchResult::Applied,
            },
        )
        .map(ObjectRpcResult::value)
        .map_err(|e| DfsPeerTransportError::new(e.to_string()))
    }
}
impl AsyncDfsPeerTransport for MatchingUnprovenPeer {
    fn call_object_rpc_async<'a>(
        &'a self,
        p: &'a NodeId,
        c: ObjectRpcCall,
        o: crate::DfsObjectRpcSendOptions,
    ) -> crate::DfsPeerTransportFuture<'a, Result<ObjectRpcResult, DfsPeerTransportError>> {
        Box::pin(async move { DfsPeerTransport::call_object_rpc(self, p, c, o) })
    }
}
#[test]
fn native_prefix_round_refuses_invalid_membership_and_keeps_generic_data_unproven() {
    run_round("native-prefix-round-unproven", |fixture, report| {
        let target = target(fixture);
        let cursor = fixture.native.rpc_observations().len();
        let mut producer = fixture.transport();
        let mut duplicate = target.clone();
        duplicate.replica_node_ids[1] = duplicate.replica_node_ids[0].clone();
        duplicate.expected_shard.as_mut().unwrap().replica_node_ids =
            duplicate.replica_node_ids.clone();
        producer.set_operation_deadline(Some(Instant::now() + CAP));
        let dup = producer.capture_metadata_prefix_full_assignment_round(&duplicate);
        report["duplicate_member"] =
            serde_json::json!(dup.as_ref().map(observed).map_err(ToString::to_string));
        assert!(dup.is_err());
        let mut subset = target.clone();
        subset.replica_node_ids.pop();
        subset.expected_shard.as_mut().unwrap().replica_node_ids = subset.replica_node_ids.clone();
        producer.set_operation_deadline(Some(Instant::now() + CAP));
        let sub = producer.capture_metadata_prefix_full_assignment_round(&subset);
        report["subset"] =
            serde_json::json!(sub.as_ref().map(observed).map_err(ToString::to_string));
        assert!(sub.is_err());
        assert!(
            own_calls(
                &fixture.native.rpc_observations()[cursor..],
                &target.primary_node_id,
                &fixture.peer()
            )
            .is_empty()
        );
        let fake = Arc::new(MatchingUnprovenPeer {
            calls: AtomicUsize::new(0),
        });
        let mut generic = crate::DfsNodeTransport::with_peer_transports_for_nodes(
            target.primary_node_id.clone(),
            fixture.nodes.clone(),
            CAP,
            fake.clone(),
            fake.clone(),
        );
        generic.set_operation_deadline(Some(Instant::now() + CAP));
        let round = generic.capture_metadata_prefix_full_assignment_round(&target)?;
        report["matching_generic_round"] = observed(&round);
        assert!(!round.is_complete());
        assert_eq!(round.target(), &target);
        assert!(round.common_source_observation().is_none());
        assert!(
            round
                .peer_outcomes()
                .iter()
                .all(|p| p.native_return().is_none())
        );
        assert!(round.peer_outcomes().iter().any(|p| p.status()
            == DfsNativePrefixRoundPeerStatus::NoNativeProof
            && p.response().is_some()));
        assert_eq!(
            fake.calls.load(Ordering::SeqCst),
            round
                .peer_outcomes()
                .iter()
                .filter(|p| p.attempted())
                .count()
        );
        assert!(
            !round
                .peer_outcomes()
                .iter()
                .any(|p| p.command().request_id.is_empty())
        );
        report["rpc_window"] = wire(&fixture.native.rpc_observations()[cursor..]);
        for peer in &target.replica_node_ids {
            assert!(
                own_calls(
                    &fixture.native.rpc_observations()[cursor..],
                    &target.primary_node_id,
                    peer
                )
                .is_empty()
            );
        }
        drop(generic);
        drop(producer);
        Ok(vec![round])
    });
}
#[test]
fn native_prefix_round_preserves_partial_return_and_requires_fresh_complete_retry() {
    run_round("native-prefix-round-partial", |fixture, report| {
        let target = target(fixture);
        let peer = target.replica_node_ids.iter().max().unwrap().clone();
        let mut producer = fixture.transport();
        let guard = hold(fixture, &peer);
        let cursor = fixture.native.rpc_observations().len();
        let deadline = Instant::now() + CAP;
        producer.set_operation_deadline(Some(deadline));
        let partial = producer.capture_metadata_prefix_full_assignment_round(&target)?;
        let events = fixture.native.rpc_observations();
        let window = &events[cursor..];
        report["partial"] = observed(&partial);
        report["held_rpc"] = wire(window);
        assert!(
            !partial.is_complete(),
            "source plus one remote cannot complete the full assignment"
        );
        assert_eq!(partial.deadline(), deadline);
        assert_eq!(partial.target(), &target);
        assert!(partial.common_source_observation().is_none());
        let b = partial
            .peer_outcomes()
            .iter()
            .find(|p| p.peer() != &peer)
            .unwrap();
        assert_eq!(b.status(), DfsNativePrefixRoundPeerStatus::NativeCaptured);
        assert!(b.native_return().is_some());
        let c = partial
            .peer_outcomes()
            .iter()
            .find(|p| p.peer() == &peer)
            .unwrap();
        assert!(c.attempted());
        assert!(c.native_return().is_none());
        assert!(c.error().is_some());
        assert_ne!(
            c.delivery_state(),
            Some(sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent),
            "the actual completed receiver must not be reclassified as unsent"
        );
        let calls = own_calls(window, &target.primary_node_id, &peer);
        assert_eq!(calls.len(), 1);
        assert!(
            window
                .iter()
                .any(|e| calls.contains(&e.call_id) && e.phase == RpcPhase::AfterDispatch)
        );
        drop(guard);
        drain_calls(&fixture.native, &calls)?;
        fixture.native.assert_fault_budgets()?;
        let retry_cursor = fixture.native.rpc_observations().len();
        producer.set_operation_deadline(Some(Instant::now() + CAP));
        let complete = producer.capture_metadata_prefix_full_assignment_round(&target)?;
        report["fresh_complete"] = observed(&complete);
        report["fresh_rpc"] = wire(&fixture.native.rpc_observations()[retry_cursor..]);
        assert_complete(&complete, &target, fixture);
        assert_ne!(partial.round_id(), complete.round_id());
        for old in partial.peer_outcomes() {
            let new = complete
                .peer_outcomes()
                .iter()
                .find(|p| p.peer() == old.peer())
                .unwrap();
            assert_ne!(old.command().request_id, new.command().request_id);
            if let Some(cap) = old.native_return() {
                assert_ne!(cap.attempt_id(), new.native_return().unwrap().attempt_id());
            }
        }
        for peer in &target.replica_node_ids {
            assert_eq!(
                own_calls(
                    &fixture.native.rpc_observations()[retry_cursor..],
                    &target.primary_node_id,
                    peer
                )
                .len(),
                1
            );
        }
        assert_eq!(fixture.native.observation_dropped_count(), 0);
        drop(producer);
        Ok(vec![partial, complete])
    });
}
