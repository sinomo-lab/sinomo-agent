//! A directory move's parent-count participant whose shard's primary is
//! lost, on the native test runtime: the root shard is on the write-set
//! protocol, the move's coordinator is another node, and a member takes the
//! root shard over while the participant's phase is prepared there but not
//! committed. The member that took over re-issues the phase under the
//! participant's own id at its epoch, and the phase is answered once.

use super::commit_native_tests::{cut, durable_prefix, hold_replies, members, release, submit};
use super::takeover_driver_native_tests::{event_log, keep_with_paths, lose};
use super::takeover_loss_native_tests::{record_from, reelect, shard_events, wait_until};
use super::*;
use crate::test_runtime::{FaultAction, FaultRule, Partition};
use crate::test_support::DfsTestRuntime;
use crate::{DfsParentCountAction, DfsParentCountParticipant, DfsPreparedOperationStatus};
use std::sync::mpsc;
use std::time::Duration;

/// The bound on every wait a test makes for an event.
const EVENT_BOUND: Duration = Duration::from_secs(90);

/// The table snapshot of `node`'s copy of `shard`.
fn snapshot(
    runtime: &DfsTestRuntime,
    node: &NodeId,
    shard: &str,
) -> crate::DfsMetadataTableSnapshot {
    let shard = crate::DfsMetadataShardId::new(shard);
    let root = crate::local_dfs_metadata_shard_path(runtime.data_dir(node), &shard);
    crate::LocalSobjMetadataBackend::open_for_inspection(shard, root)
        .unwrap()
        .export_table_snapshot()
        .unwrap()
}

/// The directory at `path` in `node`'s copy of `shard`.
fn directory_at(
    runtime: &DfsTestRuntime,
    node: &NodeId,
    shard: &str,
    path: &str,
) -> crate::DfsObjectMetadata {
    snapshot(runtime, node, shard)
        .tables
        .objects
        .into_iter()
        .find(|object| object.path == path && object.kind == crate::DfsObjectKind::Directory)
        .unwrap_or_else(|| panic!("{path} is a directory on {shard}"))
}

/// `node`'s copy of the root shard.
fn root_at(runtime: &DfsTestRuntime, node: &NodeId) -> crate::LocalSobjMetadataBackend {
    let shard = crate::DfsMetadataShardId::new("root");
    let root = crate::local_dfs_metadata_shard_path(runtime.data_dir(node), &shard);
    crate::LocalSobjMetadataBackend::open_for_inspection(shard, root).unwrap()
}

/// Every prepared body of `participant`'s phases in `node`'s copy of the
/// root shard: its id and status.
fn family(
    runtime: &DfsTestRuntime,
    node: &NodeId,
    participant: &DfsParentCountParticipant,
) -> Vec<(String, DfsPreparedOperationStatus)> {
    use crate::DfsMetadataBackend as _;
    let backend = root_at(runtime, node);
    let prefix = format!("{}:", participant.participant_id);
    let mut family = Vec::new();
    let mut after = None;
    loop {
        let (records, next) = backend
            .prepared_operation_family_page(
                &crate::DfsMetadataShardId::new("root"),
                &prefix,
                after.as_deref(),
                16,
            )
            .unwrap();
        family.extend(
            records
                .into_iter()
                .map(|record| (record.digest.operation_id, record.status)),
        );
        let Some(next) = next else {
            break;
        };
        after = Some(next);
    }
    family
}

/// The status of `operation` in `node`'s copy of the root shard.
fn status_at(
    runtime: &DfsTestRuntime,
    node: &NodeId,
    operation: &str,
) -> Option<DfsPreparedOperationStatus> {
    use crate::DfsMetadataBackend as _;
    root_at(runtime, node)
        .prepared_operation_record(operation)
        .unwrap()
        .map(|record| record.status)
}

/// A three-node cluster whose root shard has all three nodes as write-set
/// members, its primary R; the users and services shards are U's alone, so
/// a move of `/users` into `/services` is coordinated on U and counts `/` on
/// the root shard. Returns the runtime, the root shard, U and the third node
/// B.
fn parent_on_a_switched_root(
    name: &str,
) -> (DfsTestRuntime, DfsMetadataShardConfig, NodeId, NodeId) {
    let (runtime, service, root) =
        crate::test_support::current_native_metadata_service_with_kv_voters(name, 3, "/").unwrap();
    drop(service);
    assert_eq!(root.shard_id, "root");
    let r = root.primary_node_id.clone();
    let nodes = DfsTestRuntime::ordered_node_ids(name, 3);
    let others = nodes
        .iter()
        .filter(|node| **node != r)
        .cloned()
        .collect::<Vec<_>>();
    let (u, b) = (others[0].clone(), others[1].clone());
    for shard in runtime.metadata_shards().unwrap() {
        if shard.shard_id != "root" {
            runtime
                .reassign_metadata_shard(
                    &shard.file_system_id,
                    &shard.shard_id,
                    u.clone(),
                    Vec::new(),
                )
                .unwrap();
        }
    }
    let mut root = runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|shard| shard.shard_id == "root")
        .unwrap();
    if members(&root).len() != 3 || root.primary_node_id != r {
        root = runtime
            .reassign_metadata_shard(
                &root.file_system_id,
                "root",
                r.clone(),
                vec![u.clone(), b.clone()],
            )
            .unwrap();
    }
    for node in members(&root) {
        runtime
            .advertise_metadata_write_set_capability(&node)
            .unwrap();
    }
    for member in &root.replica_node_ids {
        catch_up_root(&runtime, &root, member);
    }
    let all = members(&root).into_iter().collect();
    let switch = build_metadata_write_set_switch(&cut(&runtime, &root), &all).unwrap();
    submit(&runtime, &root, &switch);
    (runtime, root, u, b)
}

/// Replicate the root primary's whole journal to `member` through the
/// ordinary durability barrier.
fn catch_up_root(runtime: &DfsTestRuntime, root: &DfsMetadataShardConfig, member: &NodeId) {
    let mut service = runtime.metadata_primary_service("/").unwrap();
    let shard_id = crate::DfsMetadataShardId::new(root.shard_id.clone());
    let head = service
        .metadata_backend()
        .unwrap()
        .metadata_prefix_proof(&shard_id)
        .unwrap()
        .expect("the primary has a prefix");
    let current = cut(runtime, root).shard;
    let mut replica_node_ids = current.replica_node_ids.clone();
    replica_node_ids.sort();
    let target = crate::DfsMetadataQuorumSyncTarget {
        file_system_id: current.file_system_id.clone(),
        shard_id,
        primary_node_id: current.primary_node_id.clone(),
        authority_epoch: Some(current.epoch),
        replica_node_ids,
        expected_shard: Some(current.clone()),
        journal_sequence: head.journal_sequence,
        barrier_id: sinomo_core::Id::generate().to_string(),
        state_fingerprint: head.digest.clone(),
        checkpoint_snapshot: None,
    };
    service
        .sync_metadata_shard_to_quorum_intersecting(
            &target,
            std::slice::from_ref(member),
            Duration::from_secs(30),
        )
        .expect("the member catches up");
    drop(service);
    assert_eq!(durable_prefix(runtime, member, root).as_ref(), Some(&head));
}

/// The move of `/users` to `/services/moved-users`, called from B's client
/// on U, on its own thread. Returns the call's result channel.
fn start_move(
    runtime: &DfsTestRuntime,
    u: &NodeId,
    b: &NodeId,
) -> mpsc::Receiver<Result<(), String>> {
    let mut caller = runtime.client(b).unwrap();
    let coordinator = u.clone();
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        let moved = caller
            .route_namespace_operation_with_request_id(
                sinomo_core::Id::generate().to_string(),
                coordinator,
                crate::DfsNamespaceOperation::Rename {
                    from: "/users".into(),
                    to: "/services/moved-users".into(),
                },
            )
            .map(drop)
            .map_err(|error| error.to_string());
        let _ = sender.send(moved);
    });
    receiver
}

/// Stop `node` before it prepares the first participant phase that records
/// `state`. Returns the stopped participant's channel, the sender that lets
/// the phase go on, and the stop's guard.
fn stop_phase_at(
    runtime: &DfsTestRuntime,
    node: &NodeId,
    state: crate::DfsParentCountState,
) -> (
    mpsc::Receiver<DfsParentCountParticipant>,
    mpsc::SyncSender<()>,
    Box<dyn std::any::Any + Send>,
) {
    let (stopped_tx, stopped_rx) = mpsc::sync_channel(1);
    let (go_on_tx, go_on_rx) = mpsc::sync_channel::<()>(1);
    let stop = std::sync::Mutex::new(Some((stopped_tx, go_on_rx)));
    let guard = crate::runtime_host::install_daemon_dfs_pre_dispatch_test_hook(
        runtime.data_dir(node),
        std::sync::Arc::new(move |point, _, _| {
            let crate::runtime_host::DaemonDfsPreDispatchTestPoint::BeforeMetadataEffectPrepare {
                payload,
            } = point
            else {
                return Ok(());
            };
            let participant = payload
                .metadata_effects
                .iter()
                .find_map(|effect| match effect {
                    crate::DfsPreparedMetadataEffect::PutNamespaceJob { job } => job
                        .parent_count_participant()
                        .filter(|(_, recorded)| **recorded == state)
                        .map(|(participant, _)| participant.clone()),
                    _ => None,
                });
            if let Some(participant) = participant
                && let Some((stopped, go_on)) = stop.lock().unwrap().take()
            {
                stopped.send(participant).unwrap();
                go_on.recv().unwrap();
            }
            Ok(())
        }),
    );
    (stopped_rx, go_on_tx, Box::new(guard))
}

/// The root shard's primary R is cut off and lost, and B takes over, as only
/// B's keeper runs, and activates.
fn b_takes_over_root(
    runtime: &DfsTestRuntime,
    root: &DfsMetadataShardConfig,
    u: &NodeId,
    b: &NodeId,
) {
    let r = root.primary_node_id.clone();
    runtime.partition_node(
        &r,
        Partition {
            kv: true,
            dfs: true,
        },
    );
    lose(runtime, &[u, b], &r);
    wait_until(
        "B takes the root shard over and activates",
        runtime,
        &[b],
        || !shard_events(runtime, b, root, DfsMetadataTakeoverEvent::ACTIVATED).is_empty(),
    );
    assert_eq!(&record_from(runtime, root, b).shard.primary_node_id, b);
}

/// The move completed once: `/` counts one link fewer and `/services` one
/// more, the participant's phases each committed once, and nothing of it is
/// left prepared or reserved.
fn moved_once(
    runtime: &DfsTestRuntime,
    b: &NodeId,
    u: &NodeId,
    participant: &DfsParentCountParticipant,
    services_before: &crate::DfsObjectMetadata,
) {
    let root_after = directory_at(runtime, b, "root", "/");
    assert_eq!(root_after.object_id, participant.expected.object_id);
    assert_eq!(
        root_after.namespace_link_count, participant.successor.namespace_link_count,
        "`/` counts the moved directory out once"
    );
    assert_eq!(
        directory_at(runtime, u, "services", "/services").namespace_link_count,
        services_before.namespace_link_count.map(|count| count + 1)
    );
    let family = family(runtime, b, participant);
    for phase in ["reserve", "apply"] {
        let committed = family
            .iter()
            .filter(|(id, status)| {
                id.contains(&format!(":{phase}:"))
                    && *status == DfsPreparedOperationStatus::Committed
            })
            .count();
        assert_eq!(committed, 1, "{phase} committed once: {family:?}");
    }
    assert!(
        family
            .iter()
            .all(|(_, status)| *status != DfsPreparedOperationStatus::Prepared),
        "nothing is left prepared: {family:?}"
    );
    use crate::DfsMetadataBackend as _;
    let backend = root_at(runtime, b);
    assert!(
        backend
            .parent_count_reservations(
                &crate::DfsMetadataShardId::new("root"),
                &participant.expected.file_system_id,
                &participant.expected.object_id,
            )
            .unwrap()
            .is_empty(),
        "no reservation is left"
    );
}

/// Let the cut-off old primary back in and settle cluster KV.
fn heal_root(runtime: &DfsTestRuntime, root: &DfsMetadataShardConfig, u: &NodeId, b: &NodeId) {
    let r = &root.primary_node_id;
    runtime.partition_node(r, Partition::default());
    for observer in [u, b] {
        runtime.set_peer_verdict(
            observer,
            r,
            sinomo_core::network::PeerPathVerdict::Path,
            false,
        );
    }
    for node in [r, u, b] {
        reelect(runtime, node);
    }
}

/// The root shard's primary prepared the participant's reservation, both
/// members hold it, and the primary was lost before it committed it. The
/// member that took over re-issues the reservation under the participant's
/// id at its epoch; the coordinator's phase is answered once, the move
/// completes, and the counts are right.
#[test]
fn native_participant_reservation_prepared_at_a_lost_primary_is_answered_once() {
    let (runtime, root, u, b) = parent_on_a_switched_root("native-participant-reserve");
    let r = root.primary_node_id.clone();
    keep_with_paths(&runtime, &root, &[&b]);
    let services_before = directory_at(&runtime, &u, "services", "/services");
    let (stopped, go_on, stop) = stop_phase_at(&runtime, &r, crate::DfsParentCountState::Reserved);
    let moved = start_move(&runtime, &u, &b);
    let participant = stopped
        .recv_timeout(EVENT_BOUND)
        .expect("R prepares the reservation");
    let reserve = participant.operation_id(&DfsParentCountAction::Reserve);
    // The reservation reaches both members; neither answer reaches R.
    let gates = [&u, &b].map(|member| hold_replies(&runtime, &root, member));
    go_on.send(()).unwrap();
    wait_until("both members hold the reservation", &runtime, &[&b], || {
        [&u, &b].into_iter().all(|member| {
            status_at(&runtime, member, &reserve) == Some(DfsPreparedOperationStatus::Prepared)
        })
    });
    b_takes_over_root(&runtime, &root, &u, &b);
    for gate in &gates {
        release(gate);
    }
    runtime.clear_faults();
    drop(stop);
    let outcome = moved.recv_timeout(EVENT_BOUND * 2).unwrap_or_else(|_| {
        panic!(
            "the move is answered once B has taken over; family {:?}\n{}",
            family(&runtime, &b, &participant),
            event_log(&runtime, &[&u, &b])
        )
    });
    assert_eq!(
        outcome,
        Ok(()),
        "family {:?}",
        family(&runtime, &b, &participant)
    );
    assert_eq!(
        status_at(&runtime, &b, &reserve),
        Some(DfsPreparedOperationStatus::Superseded),
        "R's reservation is superseded by its re-issue"
    );
    moved_once(&runtime, &b, &u, &participant, &services_before);
    heal_root(&runtime, &root, &u, &b);
    runtime.close().unwrap();
}

/// The participant's reservation committed, and the root shard's primary
/// prepared its apply, which both members hold, but was lost before it
/// committed it. The member that took over re-issues the apply under the
/// participant's id at its epoch; the coordinator's phase is answered once,
/// the move completes, and the counts are right.
#[test]
fn native_participant_apply_prepared_at_a_lost_primary_is_answered_once() {
    let (runtime, root, u, b) = parent_on_a_switched_root("native-participant-apply");
    let r = root.primary_node_id.clone();
    keep_with_paths(&runtime, &root, &[&b]);
    let services_before = directory_at(&runtime, &u, "services", "/services");
    let (stopped, go_on, stop) = stop_phase_at(&runtime, &r, crate::DfsParentCountState::Applied);
    let moved = start_move(&runtime, &u, &b);
    let participant = stopped
        .recv_timeout(EVENT_BOUND)
        .expect("R prepares the apply");
    let apply = participant.operation_id(&DfsParentCountAction::Apply);
    let gates = [&u, &b].map(|member| hold_replies(&runtime, &root, member));
    go_on.send(()).unwrap();
    wait_until("both members hold the apply", &runtime, &[&b], || {
        [&u, &b].into_iter().all(|member| {
            status_at(&runtime, member, &apply) == Some(DfsPreparedOperationStatus::Prepared)
        })
    });
    // The apply follows the reservation's commit in R's journal.
    assert_eq!(
        status_at(
            &runtime,
            &b,
            &participant.operation_id(&DfsParentCountAction::Reserve)
        ),
        Some(DfsPreparedOperationStatus::Committed),
        "the reservation committed before the apply"
    );
    b_takes_over_root(&runtime, &root, &u, &b);
    for gate in &gates {
        release(gate);
    }
    runtime.clear_faults();
    drop(stop);
    let outcome = moved.recv_timeout(EVENT_BOUND * 2).unwrap_or_else(|_| {
        panic!(
            "the move is answered once B has taken over; family {:?}\n{}",
            family(&runtime, &b, &participant),
            event_log(&runtime, &[&u, &b])
        )
    });
    assert_eq!(
        outcome,
        Ok(()),
        "family {:?}",
        family(&runtime, &b, &participant)
    );
    assert_eq!(
        status_at(&runtime, &b, &apply),
        Some(DfsPreparedOperationStatus::Superseded),
        "R's apply is superseded by its re-issue"
    );
    moved_once(&runtime, &b, &u, &participant, &services_before);
    heal_root(&runtime, &root, &u, &b);
    runtime.close().unwrap();
}

/// [`native_participant_reservation_prepared_at_a_lost_primary_is_answered_once`]
/// with the coordinator's phases to B held back: B's activation re-issues
/// and commits the reservation on its own. Once the coordinator's phase
/// reaches B, it is answered from that commit and the move completes.
#[test]
fn native_participant_reservation_is_recovered_at_activation_without_a_resend() {
    let (runtime, root, u, b) = parent_on_a_switched_root("native-participant-activation");
    let r = root.primary_node_id.clone();
    keep_with_paths(&runtime, &root, &[&b]);
    let services_before = directory_at(&runtime, &u, "services", "/services");
    let (stopped, go_on, stop) = stop_phase_at(&runtime, &r, crate::DfsParentCountState::Reserved);
    let moved = start_move(&runtime, &u, &b);
    let participant = stopped
        .recv_timeout(EVENT_BOUND)
        .expect("R prepares the reservation");
    let reserve = participant.operation_id(&DfsParentCountAction::Reserve);
    let gates = [&u, &b].map(|member| hold_replies(&runtime, &root, member));
    go_on.send(()).unwrap();
    wait_until("both members hold the reservation", &runtime, &[&b], || {
        [&u, &b].into_iter().all(|member| {
            status_at(&runtime, member, &reserve) == Some(DfsPreparedOperationStatus::Prepared)
        })
    });
    // Nothing the coordinator sends reaches B until B has recovered.
    let resend = std::sync::Arc::new(tokio::sync::Semaphore::new(0));
    runtime.install_fault(FaultRule {
        source: Some(u.clone()),
        target: Some(b.clone()),
        route: Some(crate::DFS_DATA_NAMESPACE_ROUTE.into()),
        call_id: None,
        origin: None,
        remaining_hits: 32,
        action: FaultAction::PauseBeforeDispatch(std::sync::Arc::clone(&resend)),
    });
    runtime.checkpoint_rpc_observation_window().unwrap();
    b_takes_over_root(&runtime, &root, &u, &b);
    for gate in &gates {
        release(gate);
    }
    let epoch = record_from(&runtime, &root, &b).shard.epoch;
    let reissued = participant.operation_id_at_epoch(&DfsParentCountAction::Reserve, epoch);
    wait_until(
        "B's activation commits the reservation",
        &runtime,
        &[&b],
        || status_at(&runtime, &b, &reissued) == Some(DfsPreparedOperationStatus::Committed),
    );
    assert_eq!(
        status_at(&runtime, &b, &reserve),
        Some(DfsPreparedOperationStatus::Superseded)
    );
    assert!(
        !runtime
            .rpc_observations()
            .iter()
            .any(|call| call.source == u
                && call.target == b
                && call.route == crate::DFS_DATA_NAMESPACE_ROUTE
                && call.phase == crate::test_runtime::RpcPhase::AfterDispatch),
        "no phase from the coordinator reached B"
    );
    release(&resend);
    runtime.clear_faults();
    drop(stop);
    let outcome = moved.recv_timeout(EVENT_BOUND * 2).unwrap_or_else(|_| {
        panic!(
            "the move is answered; family {:?}\n{}",
            family(&runtime, &b, &participant),
            event_log(&runtime, &[&u, &b])
        )
    });
    assert_eq!(
        outcome,
        Ok(()),
        "family {:?}",
        family(&runtime, &b, &participant)
    );
    moved_once(&runtime, &b, &u, &participant, &services_before);
    heal_root(&runtime, &root, &u, &b);
    runtime.close().unwrap();
}
