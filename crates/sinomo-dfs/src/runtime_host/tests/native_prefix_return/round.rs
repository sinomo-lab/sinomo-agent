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

#[test]
fn native_prefix_round_deadline_keeps_next_peer_unattempted() {
    run_round("native-prefix-round-before-next", |fixture, report| {
        let target = target(fixture);
        let first = target.replica_node_ids.iter().min().unwrap().clone();
        let next = target.replica_node_ids.iter().max().unwrap().clone();
        let guard = hold(fixture, &first);
        let cursor = fixture.native.rpc_observations().len();
        let mut producer = fixture.transport();
        let deadline = Instant::now() + CAP;
        producer.set_operation_deadline(Some(deadline));
        let round = producer.capture_metadata_prefix_full_assignment_round(&target)?;
        let events = fixture.native.rpc_observations();
        let window = &events[cursor..];
        report["deadline_round"] = observed(&round);
        report["rpc_window"] = wire(window);
        let calls = own_calls(window, &target.primary_node_id, &first);
        // Release/drain before assertions so a failed control cannot strand a call.
        drop(guard);
        drain_calls(&fixture.native, &calls)?;
        fixture.native.assert_fault_budgets()?;
        assert!(!round.is_complete());
        assert_eq!(round.deadline(), deadline);
        assert_eq!(round.target(), &target);
        assert_eq!(calls.len(), 1);
        assert!(
            window
                .iter()
                .any(|e| calls.contains(&e.call_id) && e.phase == RpcPhase::AfterDispatch)
        );
        let failed = round
            .peer_outcomes()
            .iter()
            .find(|p| p.peer() == &first)
            .unwrap();
        assert!(failed.attempted());
        assert_eq!(failed.status(), DfsNativePrefixRoundPeerStatus::Failed);
        assert!(failed.error().is_some());
        assert_ne!(
            failed.delivery_state(),
            Some(sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent)
        );
        let skipped = round
            .peer_outcomes()
            .iter()
            .find(|p| p.peer() == &next)
            .unwrap();
        assert_eq!(
            skipped.status(),
            DfsNativePrefixRoundPeerStatus::Unattempted
        );
        assert!(!skipped.attempted());
        assert!(skipped.response().is_none() && skipped.native_return().is_none());
        assert!(skipped.error().is_none() && skipped.delivery_state().is_none());
        assert!(skipped.unattempted_reason().is_some());
        assert!(!skipped.command().request_id.is_empty());
        assert_eq!(
            skipped
                .command()
                .prefix_probe
                .as_ref()
                .unwrap()
                .journal_sequence,
            target.journal_sequence
        );
        assert!(own_calls(window, &target.primary_node_id, &next).is_empty());
        drop(producer);
        Ok(vec![round])
    });
}

#[derive(Default)]
struct OwnerTransitionState {
    first_returned: bool,
    release: bool,
    actual_first: Option<serde_json::Value>,
}
struct TransitionPeer {
    data: SinomoDataDir,
    runtime: tokio::runtime::Handle,
    first: NodeId,
    pause: Arc<(Mutex<OwnerTransitionState>, std::sync::Condvar)>,
}
impl DfsPeerTransport for TransitionPeer {
    fn call_object_rpc(
        &self,
        _: &NodeId,
        _: ObjectRpcCall,
        _: crate::DfsObjectRpcSendOptions,
    ) -> Result<ObjectRpcResult, DfsPeerTransportError> {
        Err(DfsPeerTransportError::new(
            "only the typed real native delegation is selected",
        ))
    }
    fn call_metadata_prefix_probe(
        &self,
        attempt: &DfsMetadataPrefixProbeAttempt,
        options: crate::DfsObjectRpcSendOptions,
    ) -> Result<DfsMetadataPrefixProbeReturn, DfsPeerTransportError> {
        // Each call resolves the ACTUAL currently registered native adapter.
        // No native value is constructed, modified or deserialized here.
        let (native, asynchronous) =
            daemon_dfs_peer_transports_on_runtime(&self.data, &self.runtime);
        let result = native.call_metadata_prefix_probe(attempt, options);
        drop(native);
        drop(asynchronous); // Do not pin the retired host through stop.
        if attempt.peer() == &self.first {
            let mut state = self.pause.0.lock().unwrap();
            state.actual_first = Some(match &result {
                Ok(value) => match &value.evidence {
                    DfsMetadataPrefixEvidence::Native(cap) => projection(cap),
                    DfsMetadataPrefixEvidence::NoNativeProof => {
                        serde_json::json!({"no_native_proof":true})
                    }
                },
                Err(error) => serde_json::json!({"error":error.to_string()}),
            });
            state.first_returned = true;
            self.pause.1.notify_all();
            while !state.release {
                let remaining = attempt.deadline().saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }
                let waited = self.pause.1.wait_timeout(state, remaining).unwrap();
                state = waited.0;
                if waited.1.timed_out() {
                    break;
                }
            }
        }
        result // Original native value/response; producer still enforces its deadline.
    }
}
impl AsyncDfsPeerTransport for TransitionPeer {
    fn call_object_rpc_async<'a>(
        &'a self,
        _: &'a NodeId,
        _: ObjectRpcCall,
        _: crate::DfsObjectRpcSendOptions,
    ) -> crate::DfsPeerTransportFuture<'a, Result<ObjectRpcResult, DfsPeerTransportError>> {
        Box::pin(async {
            Err(DfsPeerTransportError::new(
                "no alternate async path selected",
            ))
        })
    }
}
struct TransitionWorker {
    task: Option<std::thread::JoinHandle<Result<OwnedNativeMetadataPrefixRound, NetworkError>>>,
    pause: Arc<(Mutex<OwnerTransitionState>, std::sync::Condvar)>,
}
impl TransitionWorker {
    fn release(&self) {
        self.pause.0.lock().unwrap().release = true;
        self.pause.1.notify_all();
    }
    fn join(&mut self) -> TestResult<OwnedNativeMetadataPrefixRound> {
        self.release();
        self.task
            .take()
            .ok_or("owned round worker missing")?
            .join()
            .map_err(|_| "owned native round worker panicked")?
            .map_err(Into::into)
    }
}
impl Drop for TransitionWorker {
    fn drop(&mut self) {
        self.release();
        if let Some(task) = self.task.take() {
            let _ = task.join();
        }
    }
}

#[test]
fn native_prefix_round_refuses_genuine_returns_across_source_owner_replacement() {
    // A separate wrapper checks BOTH sequential native owners after close.
    let artifact = sinomo_testdir::TestDir::new("native-prefix-round-mixed-owner");
    let mut fixture = Fixture::new("native-prefix-round-mixed-owner").unwrap();
    let root = fixture.source_root();
    let primary = fixture.shard.primary_node_id.clone();
    let old_owner = require_daemon_dfs_data_owner_lock(&DfsRuntimePaths::new(
        fixture.native.data_dir(&primary),
    ))
    .unwrap();
    let old_weak = Arc::downgrade(&old_owner);
    let old_identity = old_owner.storage_owner_file_identity().unwrap();
    drop(old_owner);
    let mut new_weak = None;
    let mut report = serde_json::json!({"scope":"sequential actual source owners in an owned native fixture",
        "limits":"no competing owner or fabricated native value; finalization refusal is not serving authority"});
    let checked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
        || -> TestResult<OwnedNativeMetadataPrefixRound> {
            let target = target(&fixture);
            let first = target.replica_node_ids.iter().min().unwrap().clone();
            let pause = Arc::new((
                Mutex::new(OwnerTransitionState::default()),
                std::sync::Condvar::new(),
            ));
            let peer = Arc::new(TransitionPeer {
                data: fixture.native.data_dir(&primary).clone(),
                runtime: fixture.native.runtime_handle(),
                first,
                pause: Arc::clone(&pause),
            });
            let mut producer = crate::DfsNodeTransport::with_peer_transports_for_nodes(
                primary.clone(),
                fixture.nodes.clone(),
                CAP,
                peer.clone(),
                peer,
            );
            let deadline = Instant::now() + CAP;
            producer.set_operation_deadline(Some(deadline));
            let worker_target = target.clone();
            let task = std::thread::spawn(move || {
                producer.capture_metadata_prefix_full_assignment_round(&worker_target)
            });
            let mut worker = TransitionWorker {
                task: Some(task),
                pause: Arc::clone(&pause),
            };
            {
                let mut state = pause.0.lock().unwrap();
                while !state.first_returned {
                    let left = deadline.saturating_duration_since(Instant::now());
                    if left.is_zero() {
                        return Err(
                            "first actual native return unavailable within original round budget"
                                .into(),
                        );
                    }
                    let waited = pause.1.wait_timeout(state, left).unwrap();
                    state = waited.0;
                    if waited.1.timed_out() && !state.first_returned {
                        return Err("first actual native return timed out".into());
                    }
                }
                report["first_actual_before_transition"] = state.actual_first.clone().unwrap();
                if report["first_actual_before_transition"]["attempt"]
                    .as_str()
                    .is_none()
                {
                    return Err("the first delegate produced no actual private native value".into());
                }
            }
            let stopped = fixture.native.stop_node(&primary);
            report["old_stop"] = serde_json::json!(stopped);
            stopped?;
            report["old_owner_released_before_new"] =
                serde_json::json!(old_weak.upgrade().is_none());
            assert!(
                old_weak.upgrade().is_none(),
                "no competing source owners are allowed"
            );
            let path = root.join("daemon/data-owner.lock");
            let retained = root.join("daemon/round-retired-owner.lock");
            assert!(!retained.exists());
            assert_eq!(identity(&path)?, old_identity);
            fs::rename(&path, &retained)?;
            let file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)?;
            file.sync_all()?;
            let replacement = storage_file_identity(&file)?;
            drop(file);
            assert_ne!(replacement, old_identity);
            report["physical_transition"] = serde_json::json!({"old":old_identity,"new":replacement,
            "retained_original":retained,"same_metadata_root":root});
            let reopened = fixture.native.reopen_node(primary.clone());
            report["reopen"] = serde_json::json!(reopened);
            reopened?;
            let current = require_daemon_dfs_data_owner_lock(&DfsRuntimePaths::new(
                fixture.native.data_dir(&primary),
            ))?;
            assert_eq!(current.storage_owner_file_identity()?, replacement);
            new_weak = Some(Arc::downgrade(&current));
            drop(current);
            assert!(
                Instant::now() < deadline,
                "normal transition must not extend the original5s round"
            );
            let round = worker.join()?;
            report["mixed_round"] = observed(&round);
            assert!(!round.is_complete());
            assert_eq!(round.target(), &target);
            assert_eq!(round.deadline(), deadline);
            assert!(round.common_source_observation().is_none());
            assert!(round.finalization_error().is_some());
            assert_eq!(round.peer_outcomes().len(), 2);
            let mut values = round.peer_outcomes().iter().map(|p| {
                assert_eq!(p.status(), DfsNativePrefixRoundPeerStatus::NativeCaptured);
                assert!(p.error().is_none());
                p.native_return()
                    .expect("both values must come from actual native RPCs")
            });
            let a = values.next().unwrap();
            let b = values.next().unwrap();
            assert_eq!(a.source_assignment(), b.source_assignment());
            assert_eq!(a.source_prefix(), b.source_prefix());
            assert_ne!(a.local_physical_owner(), b.local_physical_owner());
            assert_eq!(a.local_physical_owner().owner_identity, old_identity);
            assert_eq!(b.local_physical_owner().owner_identity, replacement);
            report["refusal_qualification"] = serde_json::json!(
                "mixed actual physical observations retained; existing live binding check may refuse before the literal cross-source comparison"
            );
            Ok(round)
        },
    ));
    report["all_rpc"] = wire(&fixture.native.rpc_observations());
    let closed = fixture.native.close();
    report["normal_close"] = serde_json::json!(closed);
    report["old_owner_released"] = serde_json::json!(old_weak.upgrade().is_none());
    report["new_owner_released"] =
        serde_json::json!(new_weak.as_ref().map(|w| w.upgrade().is_none()));
    if let Ok(Ok(round)) = &checked {
        report["historical_after_close"] = observed(round);
    }
    let saved = (|| -> TestResult<()> {
        let bytes = serde_json::to_vec_pretty(&report)?;
        if bytes.len() > 4 * 1024 * 1024 {
            return Err("mixed-owner evidence overflow".into());
        }
        let path = artifact.join("native-prefix-round-mixed-owner-report.json");
        let mut f = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        f.write_all(&bytes)?;
        f.sync_all()?;
        eprintln!("NATIVE_PREFIX_ROUND_MIXED_REPORT {}", path.display());
        Ok(())
    })();
    if let Err(panic) = checked {
        std::panic::resume_unwind(panic);
    }
    checked.unwrap().unwrap();
    saved.unwrap();
    closed.unwrap();
    assert!(old_weak.upgrade().is_none());
    assert!(new_weak.unwrap().upgrade().is_none());
}
