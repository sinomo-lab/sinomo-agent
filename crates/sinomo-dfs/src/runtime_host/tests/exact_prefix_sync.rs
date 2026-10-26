//! Native exact-prefix barrier DATA; no new serving/initialization authority.
use super::*;
use crate::test_runtime::{
    DfsTestRuntime, FaultAction, FaultRule, RpcObservation, RpcOrigin, RpcPhase,
};
use crate::{DfsMetadataQuorumSyncCertificate, DfsMetadataQuorumSyncTarget};
use serde::Serialize;
use std::io::Write;
use std::path::{Path, PathBuf};
use tokio::sync::Semaphore;

type TestResult<T> = std::result::Result<T, Box<dyn std::error::Error>>;
const CAP: Duration = sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;

#[derive(Debug, Eq, PartialEq, Serialize)]
struct Frame {
    entry: crate::DfsMetadataJournalEntry,
    bytes: Vec<u8>,
}
fn frames(root: &Path) -> TestResult<Vec<Frame>> {
    let deadline = Instant::now() + CAP;
    loop {
        if let Some(_guard) = crate::metadata_lock::try_metadata_inspection_lock(
            root.to_owned(),
            crate::metadata_cursor::journal_write_lock_path(root),
        )? {
            let entries = crate::metadata_journal::durable_journal_entries_since_limited(
                root,
                0,
                DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            )?;
            if entries.len() >= DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT {
                return Err("bounded full frame inventory unavailable".into());
            }
            let actual = crate::metadata_journal::read_segmented_journal_frames_at_sequences(
                root,
                &entries
                    .iter()
                    .map(|entry| entry.sequence)
                    .collect::<Vec<_>>(),
            )?;
            let result = entries
                .into_iter()
                .zip(actual)
                .map(|(entry, frame)| {
                    let frame = frame.ok_or("exact physical frame unavailable")?;
                    if frame.entry() != &entry {
                        return Err("actual frame differs from durable entry".into());
                    }
                    Ok(Frame {
                        entry,
                        bytes: frame.exact_bytes().to_vec(),
                    })
                })
                .collect::<TestResult<Vec<_>>>()?;
            if Instant::now() > deadline {
                return Err("actual frame inspection exceeded original5s".into());
            }
            return Ok(result);
        }
        if Instant::now() >= deadline {
            return Err("actual frame inspection unavailable at original5s".into());
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}
fn markers(
    native: &DfsTestRuntime,
    nodes: &[NodeId],
    shard: &DfsMetadataShardConfig,
) -> TestResult<BTreeMap<PathBuf, Option<Vec<u8>>>> {
    nodes
        .iter()
        .map(|node| {
            let path = crate::local_dfs_metadata_shard_path(
                native.data_dir(node),
                &DfsMetadataShardId::new(&shard.shard_id),
            )
            .join("journal/membership-handoff-write-freeze.sobj");
            let bytes = match fs::read(&path) {
                Ok(bytes) => Some(bytes),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => return Err(error.into()),
            };
            Ok((path, bytes))
        })
        .collect()
}
fn raw(
    trace: &crate::native_shard_trace::NativeShardTraceCapture,
) -> TestResult<serde_json::Value> {
    let mut bytes = Vec::new();
    trace.export_raw(&mut bytes)?;
    if bytes.len() > 64 * 1024 * 1024 {
        return Err("bounded native trace overflow".into());
    }
    Ok(serde_json::from_slice(&bytes)?)
}
fn facts(trace: &serde_json::Value) -> Vec<&serde_json::Value> {
    trace["boundary_packets"]
        .as_array()
        .unwrap()
        .iter()
        .map(|packet| &packet["facts"])
        .collect()
}
fn probe_ids(trace: &serde_json::Value, target: &DfsMetadataQuorumSyncTarget) -> BTreeSet<String> {
    let prefix = format!("{}-", target.barrier_id);
    facts(trace)
        .into_iter()
        .filter(|fact| {
            fact["phase"] == "direct-journal-rpc-attempt-started"
                && fact["command"]["prefix_probe"]["barrier_id"]
                    .as_str()
                    .is_some_and(|id| id.starts_with(&prefix))
        })
        .map(|fact| fact["command"]["request_id"].as_str().unwrap().to_owned())
        .collect()
}
fn certificate(
    cert: &DfsMetadataQuorumSyncCertificate,
    target: &DfsMetadataQuorumSyncTarget,
    shard: &DfsMetadataShardConfig,
) {
    assert_eq!(&cert.target, target);
    let members = std::iter::once(shard.primary_node_id.clone())
        .chain(shard.replica_node_ids.iter().cloned())
        .collect::<BTreeSet<_>>();
    assert_eq!(members.len(), 3);
    // This existing field describes majority size. Full-assignment success
    // separately requires every configured native participant below.
    assert_eq!(cert.quorum_size, 2);
    assert_eq!(cert.acknowledged_node_ids.len(), 3);
    assert_eq!(
        cert.acknowledged_node_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        members
    );
}
fn fresh_proofs(
    source: &serde_json::Value,
    receivers: &BTreeMap<NodeId, serde_json::Value>,
    target: &DfsMetadataQuorumSyncTarget,
    allowed: &BTreeSet<String>,
) {
    let source_facts = facts(source);
    for node in &target.replica_node_ids {
        let start = source_facts
            .iter()
            .copied()
            .find(|fact| {
                fact["phase"] == "direct-journal-rpc-attempt-started"
                    && fact["target_node_id"] == node.as_str()
                    && fact["route"] == crate::DFS_METADATA_JOURNAL_BATCH_ROUTE
                    && fact["command"]["request_id"]
                        .as_str()
                        .is_some_and(|id| allowed.contains(id))
                    && source_facts.iter().any(|returned| {
                        returned["phase"] == "direct-journal-rpc-attempt-returned"
                            && returned["attempt_id"] == fact["attempt_id"]
                            && returned["error_detail"].is_null()
                            && returned["response"]["request_id"] == fact["command"]["request_id"]
                    })
            })
            .expect("fresh actual native direct prefix call returned for every configured replica");
        let command: crate::DfsMetadataJournalBatchCommand =
            serde_json::from_value(start["command"].clone()).unwrap();
        assert!(command.verify_prefix_only && command.batch.journal_frames.is_empty());
        assert_eq!(
            command.source_node_id.as_ref(),
            Some(&target.primary_node_id)
        );
        assert_eq!(
            command.file_system_id.as_ref(),
            Some(&target.file_system_id)
        );
        assert_eq!(command.authority_epoch, target.authority_epoch);
        let prefix = command.prefix_probe.as_ref().unwrap();
        assert_eq!(prefix.shard_id, target.shard_id);
        assert_eq!(prefix.journal_sequence, target.journal_sequence);
        assert_eq!(prefix.state_fingerprint, target.state_fingerprint);
        assert!(
            prefix
                .barrier_id
                .starts_with(&format!("{}-", target.barrier_id))
        );
        assert!(prefix.primary_handoff_id.is_none() && prefix.repair_custody.is_none());
        let returned = source_facts
            .iter()
            .copied()
            .find(|fact| {
                fact["phase"] == "direct-journal-rpc-attempt-returned"
                    && fact["attempt_id"] == start["attempt_id"]
            })
            .unwrap();
        let response: crate::DfsMetadataJournalBatchResponse =
            serde_json::from_value(returned["response"].clone()).unwrap();
        assert_eq!(response.request_id, command.request_id);
        assert_eq!(response.shard_id, target.shard_id);
        assert_eq!(response.next_after_sequence, target.journal_sequence);
        assert_eq!(response.applied_entries, 0);
        assert!(response.verified_prefix_only);
        assert_eq!(
            response.result,
            crate::DfsMetadataJournalBatchResult::Applied
        );
        assert!(source_facts.iter().any(|fact| fact["phase"]
            == "source-journal-response-accepted"
            && fact["authenticated_source_node_id"] == node.as_str()
            && fact["expected_target_node_id"] == node.as_str()
            && fact["response"] == returned["response"]
            && fact["expected_prefix_only"] == true));
        assert!(
            facts(&receivers[node]).iter().any(|fact| fact["phase"]
                == "replica-prefix-probe-accepted"
                && fact["local_node_id"] == node.as_str()
                && fact["command"] == start["command"]
                && fact["response"] == returned["response"]
                && serde_json::from_value::<DfsMetadataShardConfig>(
                    fact["actual_source_config"].clone()
                )
                .is_ok_and(|config| Some(&config) == target.expected_shard.as_ref())),
            "actual native receiver supplied this SAME exact prefix/assignment response"
        );
    }
}
fn wire(observations: &[RpcObservation]) -> serde_json::Value {
    serde_json::json!(observations.iter().map(|event|serde_json::json!({"call_id":event.call_id,"source":event.source,"target":event.target,
        "route":event.route,"phase":format!("{:?}",event.phase),"origin":format!("{:?}",event.origin),"error":event.outcome_error})).collect::<Vec<_>>())
}
fn no_snapshot(observations: &[RpcObservation]) {
    assert!(
        observations.iter().all(
            |event| event.route != crate::DFS_METADATA_SNAPSHOT_INSTALL_ROUTE
                && event.route != crate::DFS_DATA_METADATA_SNAPSHOT_INSTALL_ROUTE
        ),
        "strict exact mode cannot invoke snapshot repair"
    );
}

struct Fixture {
    native: DfsTestRuntime,
    nodes: Vec<NodeId>,
    source: NetworkedDfsService<crate::DfsNodeTransport>,
    shard: DfsMetadataShardConfig,
    target: DfsMetadataQuorumSyncTarget,
    traces: BTreeMap<NodeId, crate::native_shard_trace::NativeShardTraceCapture>,
}
impl Fixture {
    fn new(name: &str) -> TestResult<Self> {
        let (native, nodes) = DfsTestRuntime::start_cluster(name, 3)?;
        let shard = native.root_shard()?;
        assert_eq!(shard.replica_node_ids.len(), 2);
        let path = "/exact-prefix-native-seed";
        let mut main = native.client(&shard.primary_node_id)?;
        let deadline = Instant::now() + CAP;
        let previous = main.set_transport_operation_deadline(Some(deadline));
        let seeded = main.posix_mkdir(path);
        main.set_transport_operation_deadline(previous);
        seeded?;
        assert!(Instant::now() <= deadline);
        drop(main);
        let mut source = native.metadata_primary_service(path)?;
        assert!(source.native_publication_owner_for_observation().is_some());
        let object = source
            .metadata_backend()
            .unwrap()
            .get_object_by_path(&shard.file_system_id, path)?
            .ok_or("actual Main seed unavailable")?;
        let warm = source.capture_metadata_quorum_sync_target(&object)?;
        let warmed = source.sync_metadata_shard_to_quorum(&warm, CAP)?;
        assert_eq!(warmed.acknowledged_node_ids.len(), 3);
        let target = source.capture_metadata_quorum_sync_target(&object)?;
        assert_eq!(target.expected_shard.as_ref(), Some(&shard));
        assert!(!target.barrier_id.is_empty() && !target.state_fingerprint.is_empty());
        assert!(target.checkpoint_snapshot.is_none());
        let traces = nodes
            .iter()
            .map(|node| {
                crate::native_shard_trace::capture_native_shard_trace(
                    &native.data_dir(node).root(),
                    target.shard_id.clone(),
                )
                .map(|trace| (node.clone(), trace))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        assert!(
            markers(&native, &nodes, &shard)?
                .values()
                .any(Option::is_some),
            "existing cooperative history is preserved, not fresh full-three genesis"
        );
        Ok(Self {
            native,
            nodes,
            source,
            shard,
            target,
            traces,
        })
    }
    fn root(&self) -> PathBuf {
        crate::local_dfs_metadata_shard_path(
            self.native.data_dir(&self.shard.primary_node_id),
            &self.target.shard_id,
        )
    }
    fn source_raw(&self) -> TestResult<serde_json::Value> {
        raw(&self.traces[&self.shard.primary_node_id])
    }
    fn receivers(&self) -> TestResult<BTreeMap<NodeId, serde_json::Value>> {
        self.traces
            .iter()
            .filter(|(node, _)| **node != self.shard.primary_node_id)
            .map(|(node, trace)| raw(trace).map(|value| (node.clone(), value)))
            .collect()
    }
}
fn run(name: &str, case: impl FnOnce(&mut Fixture, &mut serde_json::Value) -> TestResult<()>) {
    let artifact = sinomo_testdir::TestDir::new(name);
    let mut fixture = Fixture::new(name).unwrap();
    let mut report = serde_json::json!({"scope":"already initialized native full3 metadata assignment with existing cooperative history",
        "limits":"live barrier result DATA only; no genesis/serving/C1/durable attestation or quietness claim","target":fixture.target});
    let checked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        case(&mut fixture, &mut report)
    }));
    for trace in fixture.traces.values() {
        trace.close();
    }
    let Fixture {
        native,
        source,
        traces,
        ..
    } = fixture;
    drop(source);
    let closed = native.close();
    report["normal_close"] = serde_json::json!(closed);
    let saved = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> TestResult<()> {
        for (node, trace) in traces {
            let value = raw(&trace)?;
            let path = artifact.join(format!("{node}.native-trace.json"));
            let bytes = serde_json::to_vec(&value)?;
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            assert_eq!(value["actual_owner_stopped"], true);
            for counter in ["dropped", "incomplete", "in_flight"] {
                assert_eq!(value[counter], "0");
            }
        }
        let bytes = serde_json::to_vec_pretty(&report)?;
        if bytes.len() > 4 * 1024 * 1024 {
            return Err("bounded exact-prefix report overflow".into());
        }
        let path = artifact.join("exact-prefix-native-report.json");
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        eprintln!("EXACT_PREFIX_NATIVE_REPORT {}", path.display());
        Ok(())
    }));
    if let Err(panic) = checked {
        std::panic::resume_unwind(panic);
    }
    checked.unwrap().unwrap();
    saved.expect("native trace export panicked").unwrap();
    closed.unwrap();
}

#[test]
fn native_exact_prefix_sync_returns_unchanged_full_assignment_target() {
    run("native-exact-prefix-full-assignment", |fixture, report| {
        let target = fixture.target.clone();
        let before = frames(&fixture.root())?;
        let marker = markers(&fixture.native, &fixture.nodes, &fixture.shard)?;
        let cursor = fixture.native.rpc_observations().len();
        let started = Instant::now();
        let outcome = fixture
            .source
            .sync_metadata_shard_exact_prefix_full_assignment(&target, CAP);
        report["strict_result"] = serde_json::json!(outcome.as_ref().map_err(ToString::to_string));
        report["elapsed"] = serde_json::json!(format!("{:?}", started.elapsed()));
        let outcome = outcome?;
        assert!(started.elapsed() <= CAP);
        certificate(&outcome, &target, &fixture.shard);
        assert_eq!(target, fixture.target);
        assert_eq!(frames(&fixture.root())?, before);
        assert_eq!(
            markers(&fixture.native, &fixture.nodes, &fixture.shard)?,
            marker
        );
        let source = fixture.source_raw()?;
        let ids = probe_ids(&source, &target);
        assert_eq!(ids.len(), 2);
        fresh_proofs(&source, &fixture.receivers()?, &target, &ids);
        let all = fixture.native.rpc_observations();
        let window = &all[cursor..];
        no_snapshot(window);
        for node in &target.replica_node_ids {
            assert!(
                window
                    .iter()
                    .any(|event| event.source == target.primary_node_id
                        && &event.target == node
                        && event.origin == RpcOrigin::RegisteredHost
                        && event.route == crate::DFS_METADATA_JOURNAL_BATCH_ROUTE
                        && event.phase == RpcPhase::Completed)
            );
        }
        report["rpc"] = wire(window);
        report["actual_probe_ids"] = serde_json::json!(ids);
        assert_eq!(fixture.native.observation_dropped_count(), 0);
        Ok(())
    });
}

#[test]
fn native_exact_prefix_sync_refuses_real_source_append_while_ordinary_sync_advances() {
    run("native-exact-prefix-source-advance", |fixture, report| {
        let target = fixture.target.clone();
        let before = frames(&fixture.root())?;
        let mut main = fixture.native.client(&fixture.shard.primary_node_id)?;
        let deadline = Instant::now() + CAP;
        let previous = main.set_transport_operation_deadline(Some(deadline));
        let appended = main.posix_mkdir("/exact-prefix-real-later-append");
        main.set_transport_operation_deadline(previous);
        appended?;
        assert!(Instant::now() <= deadline);
        drop(main);
        let advanced = frames(&fixture.root())?;
        assert!(advanced.len() > before.len());
        assert!(
            advanced
                .iter()
                .any(|frame| frame.entry.sequence > target.journal_sequence
                    && matches!(
                        frame.entry.mutation,
                        crate::DfsMetadataMutation::CommitPreparedOperationV7 { .. }
                    )),
            "real Main semantic append must commit after capture"
        );
        let cursor = fixture.native.rpc_observations().len();
        let started = Instant::now();
        let strict = fixture
            .source
            .sync_metadata_shard_exact_prefix_full_assignment(&target, CAP);
        report["strict_after_actual_append"] = serde_json::json!(strict.as_ref().map_err(ToString::to_string));
        let strict_error = strict.expect_err("strict call cannot silently adopt a stronger head");
        assert!(
            strict_error
                .to_string()
                .contains("exact-prefix sync source changed")
        );
        assert!(started.elapsed() <= CAP);
        assert_eq!(target, fixture.target);
        assert_eq!(frames(&fixture.root())?, advanced);
        let source = fixture.source_raw()?;
        assert!(
            probe_ids(&source, &target).is_empty(),
            "no own fixed-target metadata probe after source movement"
        );
        let all = fixture.native.rpc_observations();
        no_snapshot(&all[cursor..]);
        report["strict_rpc"] = wire(&all[cursor..]);
        let ordinary = fixture.source.sync_metadata_shard_to_quorum(&target, CAP);
        report["same_target_ordinary_result"] = serde_json::json!(ordinary.as_ref().map_err(ToString::to_string));
        let ordinary = ordinary?;
        assert!(ordinary.target.journal_sequence > target.journal_sequence);
        assert!(ordinary.target.journal_sequence >= advanced.last().unwrap().entry.sequence);
        assert_ne!(ordinary.target.state_fingerprint, target.state_fingerprint);
        assert_eq!(ordinary.target.barrier_id, target.barrier_id);
        assert_eq!(ordinary.target.expected_shard, target.expected_shard);
        assert_eq!(ordinary.target.replica_node_ids, target.replica_node_ids);
        assert_eq!(ordinary.quorum_size, 2);
        assert_eq!(
            ordinary
                .acknowledged_node_ids
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>(),
            fixture.nodes.iter().cloned().collect()
        );
        assert_eq!(target, fixture.target);
        report["actual_frames_before"] = serde_json::json!(before);
        report["actual_frames_after_append"] = serde_json::json!(advanced);
        assert_eq!(fixture.native.observation_dropped_count(), 0);
        Ok(())
    });
}

struct ResponseHold<'a> {
    native: &'a DfsTestRuntime,
    gate: Arc<Semaphore>,
}
impl Drop for ResponseHold<'_> {
    fn drop(&mut self) {
        self.native.remove_faults_matching(|rule|matches!(&rule.action,FaultAction::PauseAfterDispatch(gate) if Arc::ptr_eq(gate,&self.gate)));
        self.gate.add_permits(32);
    }
}
#[test]
fn native_exact_prefix_sync_requires_fresh_c_reply_after_owned_response_loss() {
    run("native-exact-prefix-response-loss", |fixture, report| {
        let target = fixture.target.clone();
        let c = target.replica_node_ids.last().unwrap().clone();
        let gate = Arc::new(Semaphore::new(0));
        let fault_routes = [
            crate::DFS_METADATA_JOURNAL_BATCH_ROUTE,
            crate::DFS_DATA_METADATA_JOURNAL_BATCH_ROUTE,
        ];
        // Cover the real direct-to-routed fallback without broadening to KV
        // or increasing the original aggregate32 eligible-hit budget.
        for route in fault_routes {
            fixture.native.install_fault(FaultRule {
                source: Some(target.primary_node_id.clone()),
                target: Some(c.clone()),
                route: Some(route.into()),
                call_id: None,
                origin: Some(RpcOrigin::RegisteredHost),
                remaining_hits: 16,
                action: FaultAction::PauseAfterDispatch(Arc::clone(&gate)),
            });
        }
        let hold = ResponseHold {
            native: &fixture.native,
            gate,
        };
        let before = frames(&fixture.root())?;
        let cursor = fixture.native.rpc_observations().len();
        let started = Instant::now();
        let failed = fixture
            .source
            .sync_metadata_shard_exact_prefix_full_assignment(&target, CAP);
        report["held_actual_c_response_result"] =
            serde_json::json!(failed.as_ref().map_err(ToString::to_string));
        // Preserve the actual route/origin/receiver and source-return facts
        // even when an unexpected successful result fails the next assertion.
        let first = fixture.source_raw()?;
        let first_ids = probe_ids(&first, &target);
        let received = raw(&fixture.traces[&c])?;
        let all = fixture.native.rpc_observations();
        let held = &all[cursor..];
        report["held_rpc"] = wire(held);
        report["held_elapsed"] = serde_json::json!(format!("{:?}", started.elapsed()));
        report["source_boundary_facts_before_negative_assert"] = first["boundary_packets"].clone();
        report["c_boundary_facts_before_negative_assert"] = received["boundary_packets"].clone();
        let matches_by_route = fault_routes.map(|route| {
            let ids = |phase| {
                held.iter()
                    .filter(|event| {
                        event.source == target.primary_node_id
                            && event.target == c
                            && event.route == route
                            && event.origin == RpcOrigin::RegisteredHost
                            && event.phase == phase
                    })
                    .map(|event| event.call_id)
                    .collect::<BTreeSet<_>>()
            };
            (
                route,
                ids(RpcPhase::BeforeDispatch),
                ids(RpcPhase::AfterDispatch),
            )
        });
        let total_matches = matches_by_route
            .iter()
            .map(|(_, ids, _)| ids.len())
            .sum::<usize>();
        report["fault_matches_before_negative_assert"] = serde_json::json!({
            "per_route":matches_by_route,"total_eligible_calls":total_matches,
            "per_route_budget":16,"aggregate_budget":32,
            "budget_status":fixture.native.assert_fault_budgets(),
            "observation":"BeforeDispatch matches exact rule eligibility; AfterDispatch is actual receiver completion immediately before the existing gate wait; waiter count is not exposed",
        });
        assert!(
            failed.is_err(),
            "A+B cannot create a full3 result when actual C reply is held"
        );
        assert!(
            matches!(
                failed,
                Err(crate::DfsError::Network { .. } | crate::DfsError::NetworkDelivery { .. })
            ),
            "the genuine held-response failure must retain its network/deadline outcome"
        );
        assert!(!first_ids.is_empty());
        assert!(matches_by_route.iter().all(|(_, ids, _)| ids.len() <= 16));
        assert!(total_matches <= 32);
        assert!(
            facts(&received)
                .iter()
                .any(|fact| fact["phase"] == "replica-prefix-probe-accepted"
                    && fact["local_node_id"] == c.as_str()
                    && fact["command"]["request_id"]
                        .as_str()
                        .is_some_and(|id| first_ids.contains(id))),
            "C really executed this prefix probe before its actual response was held/lost"
        );
        no_snapshot(held);
        let calls = held
            .iter()
            .filter(|event| {
                event.source == target.primary_node_id
                    && event.target == c
                    && fault_routes.contains(&event.route.as_str())
                    && event.origin == RpcOrigin::RegisteredHost
                    && event.phase == RpcPhase::AfterDispatch
            })
            .map(|event| event.call_id)
            .collect::<BTreeSet<_>>();
        assert!(!calls.is_empty());
        drop(hold); // Release before drain/retry; RAII also releases on earlier error/panic.
        let drain_deadline = Instant::now() + CAP;
        loop {
            let all = fixture.native.rpc_observations();
            if calls.iter().all(|id| {
                all.iter().any(|event| {
                    event.call_id == *id
                        && matches!(
                            event.phase,
                            RpcPhase::Completed | RpcPhase::EndedWithoutResponse
                        )
                })
            }) {
                break;
            }
            if Instant::now() >= drain_deadline {
                return Err(
                    "owned held metadata calls lack terminal observations within original5s drain"
                        .into(),
                );
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        fixture.native.assert_fault_budgets()?;
        assert_eq!(frames(&fixture.root())?, before);
        assert_eq!(target, fixture.target);
        let fresh = fixture
            .source
            .sync_metadata_shard_exact_prefix_full_assignment(&target, CAP);
        report["same_target_fresh_result"] =
            serde_json::json!(fresh.as_ref().map_err(ToString::to_string));
        let fresh = fresh?;
        certificate(&fresh, &target, &fixture.shard);
        let after = fixture.source_raw()?;
        let all_ids = probe_ids(&after, &target);
        let fresh_ids = all_ids
            .difference(&first_ids)
            .cloned()
            .collect::<BTreeSet<_>>();
        assert_eq!(
            fresh_ids.len(),
            2,
            "each same-target retry must actually issue fresh per-invocation probes"
        );
        assert!(fresh_ids.is_disjoint(&first_ids));
        fresh_proofs(&after, &fixture.receivers()?, &target, &fresh_ids);
        assert_eq!(target, fixture.target);
        no_snapshot(&fixture.native.rpc_observations()[cursor..]);
        report["first_probe_ids"] = serde_json::json!(first_ids);
        report["fresh_probe_ids"] = serde_json::json!(fresh_ids);
        report["fault_scope"] = serde_json::json!(
            "only registered A-to-C canonical and routed metadata RPC completed replies; shared gate with16+16 aggregate32 budget; actual cancelled held response may be lost, no raw late-response injection or node outage claim"
        );
        assert_eq!(fixture.native.observation_dropped_count(), 0);
        Ok(())
    });
}
