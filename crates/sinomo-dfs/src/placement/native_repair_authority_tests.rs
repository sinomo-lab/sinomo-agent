use super::*;
use crate::DfsMetadataBackend;
use crate::test_runtime::{DfsTestRuntime, FaultAction, FaultRule};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const NATIVE_GROUP_SCAN_GATE: &str =
    "negative native repair-group gate until its real scan completes";

fn repair_group_scan_is_incomplete(
    request: &sinomo_core::kv::KvTxnRequest,
    observed: &crate::test_runtime::NativePlacementAuthorityReader,
) -> bool {
    let job = request.txn.success.iter().find_map(|operation| {
        let sinomo_core::kv::KvOp::Put { key, value, .. } = operation else {
            return None;
        };
        if !key.contains("/placement-authority-permit-v1") {
            return None;
        }
        let permit: serde_json::Value = sinomo_core::kv::decode_kv_value(value).unwrap();
        permit["job_id"].as_str().map(str::to_owned)
    });
    let Some(job) = job.filter(|job| observed.group_acquisition_is_open(job)) else {
        return false;
    };
    request.txn.compares.iter().any(|compare| {
        let sinomo_core::kv::KvCompare::ValueEquals { key, value } = compare else {
            return false;
        };
        if !key.contains("/placement-reconciliations-v1/") {
            return false;
        }
        let actual: serde_json::Value = sinomo_core::kv::decode_kv_value(value).unwrap();
        actual["job_id"] == job && matches!(actual["state"].as_str(), Some("pending" | "scanning"))
    })
}

fn native_group_scan_gate_error() -> sinomo_core::service::ServiceError {
    sinomo_core::service::ServiceError::Kv(sinomo_core::kv::KvError::Serialization {
        detail: NATIVE_GROUP_SCAN_GATE.into(),
    })
}

pub(super) fn native_managed_setup(
    name: &str,
    scopes: &[(&str, DfsPlacementScope)],
) -> (
    DfsTestRuntime,
    Vec<NodeId>,
    Vec<String>,
    Arc<tokio::sync::Semaphore>,
    Arc<Mutex<Vec<sinomo_core::kv::KvEvent>>>,
    sinomo_core::cluster::ClusterKvCommittedDeltaSubscription,
    crate::runtime_admission::DfsMutationAdmission,
    crate::test_runtime::NativePlacementAuthorityCapture,
    crate::SharedDfsControlPlaneKvClient<Box<dyn crate::DfsControlPlaneKvClient>>,
) {
    let nodes = DfsTestRuntime::ordered_node_ids(name, 3);
    let mut runtime = DfsTestRuntime::new(name, nodes[0].clone()).unwrap();
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).unwrap();
    }
    runtime.start_nodes().unwrap();
    let mut client = runtime.client(&nodes[0]).unwrap();
    let mut initial_policy = crate::DfsReplicationPolicy::new(1, 1).unwrap();
    initial_policy.required_node_ids = vec![nodes[0].clone()];
    for index in 0..4 {
        let stat = client
            .posix_write_file(
                &format!("/native-managed-{index}"),
                &[index as u8; 64],
                crate::DfsWriteOptions {
                    policy: initial_policy.clone(),
                    replica_candidates: vec![nodes[0].clone()],
                    wait_for_commit: true,
                    foreground_replica_repair: false,
                },
            )
            .unwrap();
        assert_eq!(
            stat.current_locations,
            vec![nodes[0].clone()],
            "the later eight-repair assertion requires four real sole local replicas"
        );
        assert_eq!(
            client.posix_read_file(&stat.path).unwrap(),
            vec![index as u8; 64]
        );
    }
    let candidates = client.placement_content_candidates().unwrap();
    assert_eq!(candidates.len(), nodes.len());
    drop(client);
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    for target in &nodes[1..] {
        runtime.install_fault(FaultRule {
            source: Some(nodes[0].clone()),
            target: Some(target.clone()),
            route: Some(crate::routes::DFS_DATA_REPAIR_REPLICA_ROUTE.into()),
            call_id: None,
            origin: None,
            remaining_hits: 32,
            action: FaultAction::PauseBeforeDispatch(Arc::clone(&gate)),
        });
    }
    let authority = runtime
        .capture_placement_authority_calls(&nodes[0])
        .unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&events);
    // Keep the actual subscription alive through policy publication and all
    // ordinary owner ticks. Each accepted job carries the genuine candidate cut.
    let subscription = sinomo_core::cluster::subscribe_cluster_kv_committed_deltas(
        &sinomo_core::network::ClusterPaths::new(runtime.data_dir(&nodes[0])),
        move |delta| {
            let mut captured = observed.lock().unwrap();
            assert!(
                captured.len() + delta.events.len() <= 4096,
                "native case observation bound"
            );
            captured.extend(delta.events.iter().cloned());
        },
    );
    let control = runtime.shared_control_plane().unwrap();
    let scheduler =
        crate::runtime_host::daemon_dfs_mutation_scheduler(runtime.data_dir(&nodes[0])).unwrap();
    let admission = crate::test_runtime::executor()
        .block_on(scheduler.admit(&crate::DfsMessageExecutionScope::GlobalExclusive))
        .unwrap();
    let mut jobs = Vec::new();
    for (path, scope) in scopes {
        control.with_client(|kv| {
            let mut store = DfsControlPlaneStore::new(kv.as_mut());
            let expected = store
                .get_file_system_config(&runtime.file_system().file_system_id)
                .unwrap()
                .unwrap();
            let mut desired = expected.clone();
            upsert_placement_rule(
                &mut desired.subtree_policies,
                normalize_placement_rule(
                    path,
                    *scope,
                    DfsPlacementPolicy::new(sinomo_core::DfsObjectPolicy::ReplicatedEventual, 1, 3)
                        .unwrap(),
                )
                .unwrap(),
            );
            let job_id = sinomo_core::Id::generate().to_string();
            let now = dfs_now_unix_nanos();
            let job = crate::control_plane::build_placement_reconciliation_job(
                job_id.clone(),
                &desired,
                (*path).into(),
                *scope,
                Default::default(),
                candidates.clone(),
                None,
                now,
            )
            .unwrap();
            assert!(
                store
                    .compare_and_put_placement_update(&expected, &desired, &job)
                    .unwrap()
            );
            jobs.push(job_id);
        });
    }
    (
        runtime,
        nodes,
        jobs,
        gate,
        events,
        subscription,
        admission,
        authority,
        control,
    )
}

pub(super) fn native_managed_work(
    name: &str,
    scopes: &[(&str, DfsPlacementScope)],
) -> (
    DfsTestRuntime,
    Vec<NodeId>,
    Vec<String>,
    Arc<tokio::sync::Semaphore>,
    Arc<Mutex<Vec<sinomo_core::kv::KvEvent>>>,
    crate::test_runtime::NativePlacementAuthorityReader,
) {
    let (runtime, nodes, jobs, gate, events, subscription, admission, authority, control) =
        native_managed_setup(name, scopes);
    let observed = authority.reader();
    let scan_gate = runtime
        .native_kv_request_hook(&nodes[0], move |request| {
            if repair_group_scan_is_incomplete(request, &observed) {
                return Err(native_group_scan_gate_error());
            }
            Ok(())
        })
        .unwrap();
    drop(admission);

    for _ in 0..8 {
        let tick_deadline =
            std::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        let result = runtime
            .maintenance_tick(&nodes[0], sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
            .unwrap();
        assert!(
            result.outcome.is_ok()
                || result
                    .outcome
                    .as_ref()
                    .err()
                    .is_some_and(|error| error.contains(NATIVE_GROUP_SCAN_GATE)),
            "ordinary served setup owner: {:?}",
            result.outcome
        );
        assert_eq!(result.omitted_kv_event_count, 0);
        let all_repairing = control.with_client(|kv| {
            let store = DfsControlPlaneStore::new(kv.as_mut());
            jobs.iter().all(|id| {
                store
                    .placement_reconciliation(&runtime.file_system().file_system_id, id)
                    .unwrap()
                    .is_some_and(|job| {
                        job.state == DfsPlacementReconciliationState::Completed
                            || (job.state == DfsPlacementReconciliationState::Repairing
                                && job.pending_repair_count > 0)
                    })
            })
        });
        if jobs.len() > 1 {
            // Drain actual receiver/receipt work from each scanned object before
            // the next manual tick. The SAME tick's network budget covers this
            // wait; neither queue rows nor retry eligibility are rewritten.
            gate.add_permits(64);
            let ready_paths = control.with_client(|kv| {
                let store = DfsControlPlaneStore::new(kv.as_mut());
                jobs.iter()
                    .filter_map(|id| {
                        let job = store
                            .placement_reconciliation(&runtime.file_system().file_system_id, id)
                            .unwrap()
                            .unwrap();
                        assert_eq!(job.scope, DfsPlacementScope::SelfOnly);
                        matches!(
                            job.state,
                            DfsPlacementReconciliationState::Repairing
                                | DfsPlacementReconciliationState::Completed
                        )
                        .then_some(job.path)
                    })
                    .collect::<Vec<_>>()
            });
            loop {
                let backend = crate::LocalSobjMetadataBackend::open_for_inspection(
                    crate::DfsMetadataShardId::new("root"),
                    crate::default_dfs_metadata_shards_root(runtime.data_dir(&nodes[0]))
                        .join(crate::metadata_keys::key_segment(&"root")),
                )
                .unwrap();
                let received = ready_paths.iter().all(|path| {
                    backend
                        .get_object_by_path(&crate::sifs_file_system_id(), path)
                        .unwrap()
                        .is_some_and(|object| {
                            nodes
                                .iter()
                                .all(|node| object.current_locations.contains(node))
                        })
                });
                if received {
                    break;
                }
                assert!(
                    std::time::Instant::now() < tick_deadline,
                    "actual receipts for scanned objects must settle within the original tick budget"
                );
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        if all_repairing {
            break;
        }
    }
    drop(scan_gate);
    drop(subscription);
    let calls = authority.reader();
    drop(authority);
    (runtime, nodes, jobs, gate, events, calls)
}

fn permit_groups_after_scan(
    events: &[sinomo_core::kv::KvEvent],
    calls: &crate::test_runtime::NativePlacementAuthorityReader,
    job_id: &str,
) -> usize {
    let scan_finished = events
        .iter()
        .filter_map(|event| {
            let sinomo_core::kv::KvEventKind::Put { value } = &event.kind else {
                return None;
            };
            if !event.key.contains("/placement-reconciliations-v1/") {
                return None;
            }
            let job: serde_json::Value = sinomo_core::kv::decode_kv_value(value).unwrap();
            (job["job_id"] == job_id && job["state"] == "repairing").then_some(event.revision)
        })
        .min()
        .expect("actual durable scanning-to-repairing transition");
    let committed = events
        .iter()
        .filter_map(|event| {
            let sinomo_core::kv::KvEventKind::Put { value } = &event.kind else {
                return None;
            };
            if !event.key.contains("/placement-authority-permit-v1") {
                return None;
            }
            let permit: serde_json::Value = sinomo_core::kv::decode_kv_value(value).unwrap();
            (permit["job_id"] == job_id).then(|| {
                (
                    permit["holder_id"].as_str().unwrap().to_string(),
                    event.revision,
                )
            })
        })
        .collect::<BTreeMap<_, _>>();
    let observed = calls
        .records()
        .into_iter()
        .filter(|call| call.job_id == job_id)
        .collect::<Vec<_>>();
    let all_holders = observed
        .iter()
        .filter_map(|call| call.holder_id.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        all_holders,
        committed.keys().cloned().collect(),
        "every real permit acquisition must correspond to its committed KV receipt: {observed:?}"
    );
    let groups = observed
        .iter()
        .filter(|call| call.path.is_none() && call.holder_id.is_some())
        .collect::<Vec<_>>();
    for group in &groups {
        assert!(
            group.finished,
            "the actual repair authority scope has returned"
        );
        assert_eq!(
            group.released,
            Some(true),
            "the real permit was released before dispatch"
        );
        assert_eq!(
            group.job_state,
            Some(DfsPlacementReconciliationState::Repairing)
        );
        assert!(committed[group.holder_id.as_ref().unwrap()] > scan_finished);
    }
    eprintln!(
        "native placement permit evidence job={job_id} all_acquisitions={} repair_groups={} per_object_acquisitions={}",
        all_holders.len(),
        groups.len(),
        observed
            .iter()
            .filter(|call| call.path.is_some() && call.holder_id.is_some())
            .count()
    );
    groups.len()
}

#[test]
fn native_managed_policy_change_before_permit_retires_old_plan_without_dispatch() {
    let (runtime, nodes, jobs, gate, _events, subscription, admission, authority, control) =
        native_managed_setup(
            "native-managed-current-config",
            &[("/", DfsPlacementScope::Subtree)],
        );

    let changing = control.clone();
    let fs = runtime.file_system().file_system_id;
    let observed = authority.reader();
    let job = jobs[0].clone();
    let once = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let changed = Arc::clone(&once);
    let hook=runtime.native_kv_request_hook(&nodes[0],move |request| {
        if repair_group_scan_is_incomplete(request, &observed) { return Err(native_group_scan_gate_error()); }
        let acquire=request.txn.success.iter().any(|op|matches!(op,sinomo_core::kv::KvOp::Put{key,..} if key.contains("/placement-authority-permit-v1")));
        if acquire && observed.group_acquisition_is_open(&job) && !changed.swap(true,std::sync::atomic::Ordering::SeqCst) {
            changing.with_client(|kv| {
                let mut store=DfsControlPlaneStore::new(kv.as_mut());
                let current=store.get_file_system_config(&fs)?.ok_or_else(||sinomo_core::kv::KvError::Serialization{detail:"actual config disappeared".into()})?;
                let mut desired=current.clone();
                upsert_placement_rule(&mut desired.subtree_policies,normalize_placement_rule("/",DfsPlacementScope::Subtree,DfsPlacementPolicy::new(sinomo_core::DfsObjectPolicy::ReplicatedEventual,1,1).unwrap()).unwrap());
                if !store.compare_and_put_file_system_config(Some(&current),&desired)? {
                    return Err(sinomo_core::kv::KvError::Serialization{detail:"normal config change raced".into()});
                }
                Ok::<_,sinomo_core::kv::KvError>(())
            }).map_err(sinomo_core::service::ServiceError::Kv)?;
        }
        Ok(())
    }).unwrap();
    drop(admission);
    for _ in 0..8 {
        let result = runtime
            .maintenance_tick(&nodes[0], sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
            .unwrap();
        assert_eq!(result.omitted_kv_event_count, 0);
        if once.load(std::sync::atomic::Ordering::SeqCst) {
            break;
        }
    }
    assert!(once.load(std::sync::atomic::Ordering::SeqCst));
    assert!(!runtime.rpc_observations().iter().any(|event| event.route
        == crate::routes::DFS_DATA_REPAIR_REPLICA_ROUTE
        && event.phase == crate::test_runtime::RpcPhase::BeforeDispatch));
    let cfg = control.with_client(|kv| {
        DfsControlPlaneStore::new(kv.as_mut())
            .get_file_system_config(&runtime.file_system().file_system_id)
            .unwrap()
            .unwrap()
    });
    let root = cfg
        .subtree_policies
        .iter()
        .find(|rule| rule.path_prefix == "/")
        .unwrap();
    assert_eq!((root.min_commit_replicas, root.desired_replicas), (1, 1));
    drop(hook);
    gate.add_permits(64);
    runtime.clear_faults();
    drop(subscription);
    drop(authority);
    runtime.close().unwrap();
}

#[test]
fn native_old_dispatched_repair_cannot_replace_a_real_newer_write() {
    let (runtime, nodes, _, gate, _, _) = native_managed_work(
        "native-managed-late-old-content",
        &[("/", DfsPlacementScope::Subtree)],
    );
    let mut client = runtime.client(&nodes[0]).unwrap();
    let before = client.posix_stat("/native-managed-0").unwrap();
    let newer = client
        .posix_write_file(
            "/native-managed-0",
            b"actual newer durable content",
            crate::DfsWriteOptions {
                policy: crate::DfsReplicationPolicy::new(1, 3).unwrap(),
                replica_candidates: nodes.clone(),
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    assert_ne!(newer.version, before.version);
    gate.add_permits(64);
    runtime.clear_faults();
    client.flush_detached_transport_sends().unwrap();
    assert_eq!(
        client.posix_stat("/native-managed-0").unwrap().version,
        newer.version
    );
    assert_eq!(
        client.posix_read_file("/native-managed-0").unwrap(),
        b"actual newer durable content"
    );
    drop(client);
    runtime.close().unwrap();
}

#[test]
fn native_managed_release_failure_checkpoints_work_without_receiver_dispatch() {
    let (runtime, nodes, jobs, gate, _events, subscription, admission, authority, control) =
        native_managed_setup(
            "native-managed-release-failure",
            &[("/", DfsPlacementScope::Subtree)],
        );
    let observed = authority.reader();
    let job = jobs[0].clone();
    let denied_release = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let actual_denial = Arc::clone(&denied_release);
    let hook=runtime.native_kv_request_hook(&nodes[0],move |request| {
        if repair_group_scan_is_incomplete(request, &observed) { return Err(native_group_scan_gate_error()); }
        let release=request.txn.success.iter().any(|op|matches!(op,sinomo_core::kv::KvOp::Delete{key} if key.contains("/placement-authority-permit-v1")));
        let holders = observed.group_holder_ids(&job);
        let exact_group = request.txn.compares.iter().any(|compare| {
            let sinomo_core::kv::KvCompare::ValueEquals { key, value } = compare else { return false; };
            if !key.contains("/placement-authority-permit-v1") { return false; }
            let permit: serde_json::Value = sinomo_core::kv::decode_kv_value(value).unwrap();
            permit["holder_id"].as_str().is_some_and(|id| holders.iter().any(|holder| holder == id))
        });
        if release && exact_group {
            actual_denial.store(true, std::sync::atomic::Ordering::SeqCst);
            return Err(sinomo_core::service::ServiceError::Kv(sinomo_core::kv::KvError::Serialization{detail:"shared native release boundary fault".into()}));
        }
        Ok(())
    }).unwrap();
    drop(admission);
    let mut failed = false;
    for _ in 0..8 {
        let result = runtime
            .maintenance_tick(&nodes[0], sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
            .unwrap();
        assert_eq!(result.omitted_kv_event_count, 0);
        if result.outcome.is_err() && denied_release.load(std::sync::atomic::Ordering::SeqCst) {
            failed = true;
            break;
        }
    }
    assert!(
        failed,
        "actual native release failure reached the served owner"
    );

    assert!(
        control
            .with_client(|kv| DfsControlPlaneStore::new(kv.as_mut())
                .placement_authority_permit_status(&runtime.file_system().file_system_id)
                .unwrap())
            .is_some()
    );
    assert!(!runtime.rpc_observations().iter().any(|event| event.route
        == crate::routes::DFS_DATA_REPAIR_REPLICA_ROUTE
        && event.phase == crate::test_runtime::RpcPhase::BeforeDispatch));
    let store = crate::LocalDfsRepairQueueStore::new(
        crate::default_dfs_repair_attempt_snapshot_path(runtime.data_dir(&nodes[0])),
    );
    let crate::DfsRepairQueueBoundedStatus::Hydrated(before) =
        store.bounded_status_view(16).unwrap()
    else {
        panic!("failed owner retained real durable work")
    };
    assert_eq!(before.record_count, 8);
    assert!(before.records.iter().all(|record| !record.in_flight));
    drop(hook);
    let resumed = runtime
        .maintenance_tick(&nodes[0], sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
        .unwrap();
    assert!(
        resumed.outcome.is_ok(),
        "same normal owner resolves exact pending release: {:?}",
        resumed.outcome
    );
    assert!(
        control
            .with_client(|kv| DfsControlPlaneStore::new(kv.as_mut())
                .placement_authority_permit_status(&runtime.file_system().file_system_id)
                .unwrap())
            .is_none()
    );
    gate.add_permits(64);
    runtime.clear_faults();
    drop(subscription);
    drop(authority);
    runtime.close().unwrap();
}

#[test]
fn native_managed_dispatch_failure_preserves_checkpointed_ids_across_service_reopen() {
    let (mut runtime, nodes, jobs, gate, events, calls) = native_managed_work(
        "native-managed-reopen",
        &[("/", DfsPlacementScope::Subtree)],
    );
    let store = crate::LocalDfsRepairQueueStore::new(
        crate::default_dfs_repair_attempt_snapshot_path(runtime.data_dir(&nodes[0])),
    );
    let before = store.view().unwrap();
    assert_eq!(before.records.len(), 8);
    assert!(
        before
            .records
            .iter()
            .all(|record| record.in_flight && record.transfer_request_id.is_some())
    );
    let ids = before
        .records
        .iter()
        .map(|record| {
            (
                (
                    record.intent_id.clone(),
                    record.missing_replica_node_id.clone(),
                ),
                record.transfer_request_id.clone(),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(
        permit_groups_after_scan(&events.lock().unwrap(), &calls, &jobs[0]),
        1
    );
    // Closing the real node closes the existing dispatch gates; no receiver
    // success is injected for the cancelled calls.
    runtime.stop_node(&nodes[0]).unwrap();
    let stopped = store.view().unwrap();
    assert_eq!(stopped.records.len(), before.records.len());
    let stop_observations = runtime.rpc_observations();
    for record in &stopped.records {
        let key = (
            record.intent_id.clone(),
            record.missing_replica_node_id.clone(),
        );
        let expected = ids.get(&key).unwrap().as_ref().unwrap();
        let original = before
            .records
            .iter()
            .find(|prior| {
                prior.intent_id == record.intent_id
                    && prior.missing_replica_node_id == record.missing_replica_node_id
            })
            .unwrap();
        assert_eq!(
            (
                &record.object_id,
                &record.version,
                &record.content_hash,
                record.transfer_kind
            ),
            (
                &original.object_id,
                &original.version,
                &original.content_hash,
                original.transfer_kind
            )
        );
        assert!(
            record.completed_transfer_request_id.is_none(),
            "no receiver completed before dispatch gates closed"
        );
        if let Some(active) = &record.transfer_request_id {
            assert_eq!(
                active, expected,
                "unsettled custody preserves the original named request"
            );
        } else {
            assert!(!record.in_flight);
            assert_eq!(
                record.transfer_delivery_state,
                Some(sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent)
            );
            let physical_calls = stop_observations
                .iter()
                .filter(|event| {
                    event.source == nodes[0]
                        && event.target == record.missing_replica_node_id
                        && event.route == crate::DFS_DATA_REPAIR_REPLICA_ROUTE
                        && event.dfs_request_id.as_ref() == Some(expected)
                        && event.phase == crate::test_runtime::RpcPhase::BeforeDispatch
                })
                .map(|event| event.call_id)
                .collect::<BTreeSet<_>>();
            assert!(
                !physical_calls.is_empty(),
                "a cleared ID needs its exact real pre-dispatch call"
            );
            for call_id in physical_calls {
                assert!(
                    stop_observations
                        .iter()
                        .any(|event| event.call_id == call_id
                            && event.phase == crate::test_runtime::RpcPhase::EndedWithoutResponse)
                );
                assert!(
                    !stop_observations
                        .iter()
                        .any(|event| event.call_id == call_id
                            && matches!(
                                event.phase,
                                crate::test_runtime::RpcPhase::AfterDispatch
                                    | crate::test_runtime::RpcPhase::Completed
                            )),
                    "the actual stopped call never entered a receiver"
                );
            }
        }
    }
    let observation_cut = stop_observations.len();
    runtime.clear_faults();
    runtime.reopen_node(nodes[0].clone()).unwrap();
    let result = runtime
        .maintenance_tick(&nodes[0], sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
        .unwrap();
    assert!(
        result.outcome.is_ok(),
        "ordinary reopened owner: {:?}",
        result.outcome
    );
    let after = store.view().unwrap();
    for original in stopped
        .records
        .iter()
        .filter(|record| record.transfer_request_id.is_some())
    {
        let expected = original.transfer_request_id.as_ref().unwrap();
        if let Some(current) = after.records.iter().find(|record| {
            record.intent_id == original.intent_id
                && record.missing_replica_node_id == original.missing_replica_node_id
        }) {
            assert_eq!(
                current.transfer_request_id.as_ref(),
                Some(expected),
                "a new service must not invent a fresh ID for unsettled work"
            );
        }
        assert!(
            runtime.rpc_observations()[observation_cut..]
                .iter()
                .all(|event| {
                    event.route != crate::DFS_DATA_REPAIR_REPLICA_ROUTE
                        || event.target != original.missing_replica_node_id
                        || event.dfs_intent_id.as_deref() != Some(original.intent_id.as_str())
                        || event.dfs_request_id.as_ref() == Some(expected)
                }),
            "any actual replay of unsettled work retains the original request identity"
        );
    }

    assert!(after.records.iter().all(|record| ids.contains_key(&(
        record.intent_id.clone(),
        record.missing_replica_node_id.clone()
    ))));
    gate.add_permits(64);
    runtime.close().unwrap();
}

#[test]
fn managed_native_same_job_batches_after_real_scanning_and_releases_before_dispatch() {
    let (runtime, nodes, jobs, gate, events, calls) = native_managed_work(
        "native-managed-one-job",
        &[("/", DfsPlacementScope::Subtree)],
    );
    assert_eq!(
        permit_groups_after_scan(&events.lock().unwrap(), &calls, &jobs[0]),
        1
    );
    let queue = crate::LocalDfsRepairQueueStore::new(
        crate::default_dfs_repair_attempt_snapshot_path(runtime.data_dir(&nodes[0])),
    );
    // This assertion inspects the exact durable queue. The nonblocking status
    // peek may legitimately report an overlapping resident update.
    let durable = queue.view().unwrap();
    assert_eq!(
        durable.records.len(),
        8,
        "four actual objects retain both missing receiver targets"
    );
    assert!(
        durable
            .records
            .iter()
            .all(|record| record.in_flight && record.transfer_request_id.is_some())
    );
    let scheduler =
        crate::runtime_host::daemon_dfs_mutation_scheduler(runtime.data_dir(&nodes[0])).unwrap();
    let expected_requests = durable
        .records
        .iter()
        .map(|record| record.transfer_request_id.clone().unwrap())
        .collect::<BTreeSet<_>>();
    assert_eq!(expected_requests.len(), 8);
    let admission = crate::test_runtime::executor().block_on(async {
        tokio::time::timeout(
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            scheduler.admit(&crate::DfsMessageExecutionScope::GlobalExclusive),
        )
        .await
        .expect("paused receiver work cannot block normal global admission")
        .unwrap()
    });
    let observed = runtime.rpc_observations();
    let actual_paused = observed
        .iter()
        .filter(|event| {
            event.route == crate::DFS_DATA_REPAIR_REPLICA_ROUTE
                && event.phase == crate::test_runtime::RpcPhase::BeforeDispatch
        })
        .filter_map(|event| event.dfs_request_id.clone())
        .collect::<BTreeSet<_>>();
    assert!(!actual_paused.is_empty());
    assert!(
        actual_paused.is_subset(&expected_requests),
        "every actual paused call belongs to the same eight checkpointed transfers"
    );
    assert_eq!(
        queue.view().unwrap().records,
        durable.records,
        "normal bounded fanout may queue the remaining calls; admission must preserve all eight exact owners"
    );
    assert!(
        !observed.iter().any(|event| {
            event
                .dfs_request_id
                .as_ref()
                .is_some_and(|id| expected_requests.contains(id))
                && matches!(
                    event.phase,
                    crate::test_runtime::RpcPhase::Completed
                        | crate::test_runtime::RpcPhase::EndedWithoutResponse
                )
        }),
        "admission must be granted while the actual calls remain paused and all eight transfers are unfinished, never after their timeout"
    );
    drop(admission);
    let control = runtime.shared_control_plane().unwrap();
    assert!(
        control
            .with_client(|kv| DfsControlPlaneStore::new(kv.as_mut())
                .placement_authority_permit_status(&runtime.file_system().file_system_id)
                .unwrap())
            .is_none()
    );
    assert!(runtime.rpc_observations().iter().any(|event| event.route
        == crate::routes::DFS_DATA_REPAIR_REPLICA_ROUTE
        && event.phase == crate::test_runtime::RpcPhase::BeforeDispatch));
    gate.add_permits(64);
    runtime.clear_faults();
    runtime.close().unwrap();
}

#[test]
fn managed_native_distinct_jobs_keep_distinct_real_permit_groups() {
    let (runtime, nodes, jobs, gate, events, calls) = native_managed_work(
        "native-managed-distinct-jobs",
        &[
            ("/native-managed-0", DfsPlacementScope::SelfOnly),
            ("/native-managed-1", DfsPlacementScope::SelfOnly),
        ],
    );
    let captured = events.lock().unwrap();
    assert_eq!(permit_groups_after_scan(&captured, &calls, &jobs[0]), 1);
    assert_eq!(permit_groups_after_scan(&captured, &calls, &jobs[1]), 1);
    drop(captured);
    let control = runtime.shared_control_plane().unwrap();
    assert!(
        control
            .with_client(|kv| DfsControlPlaneStore::new(kv.as_mut())
                .placement_authority_permit_status(&runtime.file_system().file_system_id)
                .unwrap())
            .is_none()
    );
    assert!(matches!(
        runtime
            .maintenance_tick(&nodes[0], Duration::from_secs(5))
            .unwrap()
            .outcome,
        Ok(crate::DfsMaintenanceTickOutcome::Completed { .. })
            | Ok(crate::DfsMaintenanceTickOutcome::Deferred)
    ));
    gate.add_permits(64);
    runtime.clear_faults();
    runtime.close().unwrap();
}

fn native_pending_ordinary_repair(
    name: &str,
) -> (
    DfsTestRuntime,
    Vec<NodeId>,
    NetworkedDfsService<crate::DfsNodeTransport>,
    crate::DfsStat,
) {
    let nodes = DfsTestRuntime::ordered_node_ids(name, 3);
    let mut runtime = DfsTestRuntime::new(name, nodes[0].clone()).unwrap();
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).unwrap();
    }
    runtime.start_nodes().unwrap();
    // Fail only real content dispatch. Native metadata replication and authority
    // still use the ordinary hosted receivers.
    for target in &nodes[1..] {
        for route in [
            crate::routes::DFS_DATA_STAGE_BLOB_ROUTE,
            crate::routes::DFS_DATA_REPAIR_REPLICA_ROUTE,
        ] {
            runtime.install_fault(FaultRule {
                source: Some(nodes[0].clone()),
                target: Some(target.clone()),
                route: Some(route.into()),
                call_id: None,
                origin: None,
                remaining_hits: 32,
                action: FaultAction::BudgetExhausted,
            });
        }
    }
    // The ordinary producer owns coordinator/queue state; a browser facade
    // delegates it to Main and cannot be inspected as that producer.
    let mut service = runtime.metadata_primary_service("/native-repair").unwrap();
    assert_eq!(service.node_id(), &nodes[0]);
    let stat = service
        .posix_write_file(
            "/native-repair",
            b"original durable generation",
            crate::DfsWriteOptions {
                policy: crate::DfsReplicationPolicy::new(1, 3).unwrap(),
                replica_candidates: nodes.clone(),
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    assert_eq!(service.coordinator.repair_jobs().len(), 2);
    assert_eq!(service.durable_repair_queue_records().len(), 2);
    (runtime, nodes, service, stat)
}

#[test]
fn ordinary_native_served_repair_performs_no_placement_permit_commit() {
    let (runtime, nodes, service, _) =
        native_pending_ordinary_repair("native-ordinary-served-repair");
    let actual = service.durable_repair_queue_records();
    let store = crate::LocalDfsRepairQueueStore::new(
        crate::default_dfs_repair_attempt_snapshot_path(runtime.data_dir(&nodes[0])),
    );
    // Enqueue only identities produced by the actual committed native write.
    // The normal owner independently revalidates their metadata before effect.
    store.append_enqueues(actual.clone()).unwrap();
    drop(service);
    runtime.clear_faults();
    let deadline = std::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let observed = runtime
        .maintenance_tick(&nodes[0], sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
        .unwrap();
    assert!(
        observed.outcome.is_ok(),
        "ordinary served repair: {:?}",
        observed.outcome
    );
    assert_eq!(observed.omitted_kv_event_count, 0);
    assert!(
        !observed
            .committed_kv_events
            .iter()
            .any(|event| event.key.contains("/placement-authority-permit-v1"))
    );
    while !runtime.rpc_observations().iter().any(|event| {
        event.route == crate::routes::DFS_DATA_REPAIR_REPLICA_ROUTE
            && event.phase == crate::test_runtime::RpcPhase::BeforeDispatch
    }) {
        assert!(
            std::time::Instant::now() < deadline,
            "the real detached repair must dispatch within the original tick budget"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    runtime.close().unwrap();
}

#[test]
fn native_served_repair_revalidates_the_actual_newer_content_generation() {
    let (runtime, nodes, mut service, original) =
        native_pending_ordinary_repair("native-served-current-content");
    let actual = service.durable_repair_queue_records();
    let old_id = actual[0].intent_id.clone();
    let old_record = service
        .metadata_backend()
        .unwrap()
        .get_intent(&original.object_id, &old_id)
        .unwrap()
        .unwrap();
    let store = crate::LocalDfsRepairQueueStore::new(
        crate::default_dfs_repair_attempt_snapshot_path(runtime.data_dir(&nodes[0])),
    );
    store.append_enqueues(actual).unwrap();
    let newer = service
        .posix_write_file(
            "/native-repair",
            b"real newer content",
            crate::DfsWriteOptions {
                policy: crate::DfsReplicationPolicy::new(1, 3).unwrap(),
                replica_candidates: nodes.clone(),
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    assert_ne!(newer.version, original.version);
    drop(service);
    let observed = runtime
        .maintenance_tick(&nodes[0], sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
        .unwrap();
    assert!(
        observed.outcome.is_ok(),
        "actual served retirement: {:?}",
        observed.outcome
    );
    let mut current = runtime.client(&nodes[0]).unwrap();
    assert_eq!(
        current.posix_stat("/native-repair").unwrap().version,
        newer.version
    );
    assert_eq!(
        current
            .metadata_backend()
            .unwrap()
            .get_intent(&original.object_id, &old_id)
            .unwrap(),
        Some(old_record)
    );
    let remaining = store.view().unwrap().records;
    assert!(
        remaining.iter().all(|record| record.intent_id != old_id),
        "old repair is retired from the real durable queue"
    );
    runtime.clear_faults();
    drop(current);
    runtime.close().unwrap();
}
