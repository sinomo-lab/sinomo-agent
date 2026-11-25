//! A content write whose primary is lost, on the native test runtime: the
//! write's row, and for some writes its final prepare, reached the members,
//! and a member took over. The member that took over owns the write: it
//! adopts an open write at its own epoch, commits a final prepare it holds,
//! and answers the caller's same operation id, a write or its abort, once.

use super::commit_native_tests::{
    catch_up, directory, members, primary, raise_n, shard_root, switched_with_kv_voters,
};
use super::takeover_driver_native_tests::{event_log, keep_with_paths, lose};
use super::takeover_loss_native_tests::{
    record_from, reelect, shard_events, try_record_from, wait_until,
};
use super::*;
use crate::test_runtime::{FaultAction, FaultRule, Partition, executor};
use crate::test_support::DfsTestRuntime;
use sinomo_core::network::PeerPathVerdict;
use std::sync::mpsc;
use std::time::{Duration, Instant};

/// The bound on every wait a test makes for an event.
const EVENT_BOUND: Duration = Duration::from_secs(90);

type Call = Box<
    dyn FnOnce(
            &mut crate::NetworkedDfsService<crate::DfsNodeTransport>,
        ) -> Result<crate::DfsStat, crate::DfsPosixError>
        + Send,
>;

/// W of 2 over the shard's three members, waiting for the commit.
fn options(config: &DfsMetadataShardConfig) -> crate::DfsWriteOptions {
    crate::DfsWriteOptions {
        policy: crate::DfsReplicationPolicy::new(2, 3).unwrap(),
        replica_candidates: members(config),
        wait_for_commit: true,
        foreground_replica_repair: true,
    }
}

/// A three-member W=2 shard whose members hold `/services/owned`. A content
/// write above the W the shard accepts waits for the primary's keeper to
/// raise the shard's N, and the native runtime mounts every keeper
/// suspended: the shard's N is raised to 2 here.
fn owned_shard(name: &str) -> (DfsTestRuntime, DfsMetadataShardConfig) {
    let (runtime, config) = switched_with_kv_voters(name, Some((2, 3)));
    raise_n(&runtime, &config, 2);
    {
        let mut service = primary(&runtime);
        directory(&mut service, "/services/owned");
    }
    for member in &config.replica_node_ids {
        catch_up(&runtime, &config, member);
    }
    (runtime, config)
}

/// Each call `from` made with `operation` as its request: its target,
/// route and last phase, and its error.
fn write_calls_from(
    runtime: &DfsTestRuntime,
    from: &NodeId,
    operation: &str,
) -> Vec<(
    NodeId,
    String,
    crate::test_runtime::RpcPhase,
    Option<String>,
)> {
    let mut latest = std::collections::BTreeMap::new();
    for call in runtime.rpc_observations() {
        if &call.source == from && call.dfs_request_id.as_deref() == Some(operation) {
            latest.insert(call.call_id, call);
        }
    }
    latest
        .into_values()
        .map(|call| (call.target, call.route, call.phase, call.outcome_error))
        .collect()
}

/// The primary cannot stage content to either member.
fn cannot_stage(runtime: &DfsTestRuntime, config: &DfsMetadataShardConfig) {
    for target in &config.replica_node_ids {
        for route in [
            crate::DFS_DATA_STAGE_BLOB_ROUTE,
            crate::DFS_DATA_STAGE_PATCH_ROUTE,
            crate::DFS_DATA_REPAIR_REPLICA_ROUTE,
        ] {
            runtime.install_fault(FaultRule {
                source: Some(config.primary_node_id.clone()),
                target: Some(target.clone()),
                route: Some(route.into()),
                call_id: None,
                origin: None,
                remaining_hits: 32,
                action: FaultAction::BudgetExhausted,
            });
        }
    }
}

/// Run `call` on C's client while the primary cannot stage to either
/// member: the primary keeps the write's only copy, answers it `Pending`,
/// and is then cut off and lost; a member takes over. Returns what the call
/// returned and the shard's new record.
fn pending_past_a_lost_primary(
    runtime: &DfsTestRuntime,
    config: &DfsMetadataShardConfig,
    operation: &'static str,
    budget: Option<Duration>,
    call: Call,
) -> (
    Result<crate::DfsStat, crate::DfsPosixError>,
    DfsMetadataShardConfig,
) {
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    keep_with_paths(runtime, config, &[&b, &c]);
    cannot_stage(runtime, config);
    runtime.checkpoint_rpc_observation_window().unwrap();
    let data_dir = runtime.data_dir(&c).clone();
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        let mut client =
            crate::runtime_host::open_dfs_runtime_service(&data_dir, Some(executor().handle()))
                .expect("the requester opens its client");
        if let Some(budget) = budget {
            client.set_transport_operation_deadline(Some(Instant::now() + budget));
        }
        let _ = sender.send(call(&mut client));
    });
    let started = Instant::now();
    while !{
        let calls = runtime.rpc_observations();
        calls.iter().any(|call| {
            call.source == c
                && call.target == p
                && call.dfs_request_id.as_deref() == Some(operation)
                && calls.iter().any(|answer| {
                    answer.call_id == call.call_id
                        && answer.phase == crate::test_runtime::RpcPhase::Completed
                })
        })
    } {
        assert!(
            started.elapsed() < EVENT_BOUND,
            "the primary answers {operation}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    runtime.partition_node(
        &p,
        Partition {
            kv: true,
            dfs: true,
        },
    );
    lose(runtime, &[&b, &c], &p);
    let outcome = receiver.recv_timeout(EVENT_BOUND * 2).unwrap_or_else(|_| {
        panic!(
            "{operation} is answered once a member has taken over; C's calls {:?}\n{}",
            write_calls_from(runtime, &c, operation),
            event_log(runtime, &[&b, &c])
        )
    });
    runtime.clear_faults();
    let started = Instant::now();
    let successor = loop {
        let record = record_from(runtime, config, &b).shard;
        if record.primary_node_id != p {
            break record;
        }
        assert!(started.elapsed() < EVENT_BOUND, "a member takes over");
        std::thread::sleep(Duration::from_millis(100));
    };
    (outcome, successor)
}

/// `path` read through `reader`'s client: its stat and bytes.
fn read(runtime: &DfsTestRuntime, reader: &NodeId, path: &str) -> (crate::DfsStat, Vec<u8>) {
    let mut client = runtime.client(reader).unwrap();
    let started = Instant::now();
    loop {
        match (
            client.posix_stat_distributed(path),
            client.posix_read_file_distributed(path),
        ) {
            (Ok(stat), Ok(bytes)) => return (stat, bytes),
            (stat, bytes) => assert!(
                started.elapsed() < EVENT_BOUND,
                "{path} is readable: {stat:?} {bytes:?}"
            ),
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// What a write that crossed the takeover returned: its result, or `None`
/// when it committed and only the stat that follows the commit failed, as
/// it does while the cut-off node is still primary of the shards that did
/// not switch.
fn committed(
    outcome: Result<crate::DfsStat, crate::DfsPosixError>,
    what: &str,
) -> Option<crate::DfsStat> {
    match outcome {
        Ok(stat) => Some(stat),
        Err(crate::DfsPosixError::Dfs(crate::DfsError::NetworkDelivery { message, .. }))
            if message.contains("the write committed; reading its result failed") =>
        {
            None
        }
        Err(error) => panic!("{what}: {error:?}"),
    }
}

/// Let the cut-off old primary back in: its cluster KV first, so it reads
/// the record that deposed it, then the rest; wait until the new primary has
/// it in its write set again.
fn heal(runtime: &DfsTestRuntime, config: &DfsMetadataShardConfig) {
    let old = &config.primary_node_id;
    let others = [&config.replica_node_ids[0], &config.replica_node_ids[1]];
    runtime.partition_node(
        old,
        Partition {
            kv: false,
            dfs: true,
        },
    );
    wait_until(
        "the old primary reads the new record",
        runtime,
        &others,
        || {
            try_record_from(runtime, config, old)
                .is_some_and(|record| &record.shard.primary_node_id != old)
        },
    );
    runtime.partition_node(old, Partition::default());
    for observer in others {
        runtime.set_peer_verdict(observer, old, PeerPathVerdict::Path, false);
    }
    wait_until(
        "the old primary rejoins the write set",
        runtime,
        &others,
        || {
            try_record_from(runtime, config, others[0]).is_some_and(|record| {
                record
                    .write_set_state()
                    .switched()
                    .is_some_and(|write_set| write_set.contains(old))
            })
        },
    );
    for node in [old, others[0], others[1]] {
        reelect(runtime, node);
    }
}

/// `path` committed on every member before the test's write.
fn committed_before(
    runtime: &DfsTestRuntime,
    config: &DfsMetadataShardConfig,
    path: &str,
) -> crate::DfsStat {
    let mut service = primary(runtime);
    let before = service
        .posix_write_file(path, b"before", options(config))
        .unwrap();
    drop(service);
    for member in &config.replica_node_ids {
        catch_up(runtime, config, member);
    }
    before
}

/// An exclusive create the lost primary answered `Pending`: the member
/// that took over creates the file once under the call's operation id.
#[test]
fn native_exclusive_create_completes_once_past_a_lost_primary() {
    let (runtime, config) = owned_shard("native-successor-create");
    let c = config.replica_node_ids[1].clone();
    let options = options(&config);
    let path = "/services/owned/created.txt";
    let operation = "successor-create";
    let call_options = options.clone();
    let (outcome, _) = pending_past_a_lost_primary(
        &runtime,
        &config,
        operation,
        None,
        Box::new(move |client| {
            client.posix_create_file_with_operation_id(
                path,
                operation.as_bytes(),
                call_options,
                operation,
            )
        }),
    );
    let created = committed(outcome, "the member that took over creates the file");
    heal(&runtime, &config);
    let (stat, bytes) = read(&runtime, &c, path);
    if let Some(created) = created.as_ref() {
        assert_eq!(stat.object_id, created.object_id);
    }
    assert_eq!(
        stat.version,
        crate::intent::stable_write_target_version(&c, &stat.object_id, operation)
    );
    assert_eq!(bytes, operation.as_bytes());
    let again = runtime
        .client(&c)
        .unwrap()
        .posix_create_file_with_operation_id(path, operation.as_bytes(), options, operation)
        .expect("the same operation id is answered from its commit");
    assert_eq!(again.version, stat.version);
    runtime.close().unwrap();
}

/// A replace conditional on the file's version, answered `Pending` by the
/// lost primary: the member that took over commits it once over that
/// version.
#[test]
fn native_conditional_replace_completes_once_past_a_lost_primary() {
    let (runtime, config) = owned_shard("native-successor-replace");
    let c = config.replica_node_ids[1].clone();
    let options = options(&config);
    let path = "/services/owned/replaced.txt";
    let operation = "successor-replace";
    let before = committed_before(&runtime, &config, path);
    let call_options = options.clone();
    let (object_id, version) = (before.object_id.clone(), before.version.clone());
    let (outcome, _) = pending_past_a_lost_primary(
        &runtime,
        &config,
        operation,
        None,
        Box::new(move |client| {
            client.posix_replace_file_if_version_with_operation_id(
                path,
                &object_id,
                &version,
                operation.as_bytes(),
                call_options,
                operation,
            )
        }),
    );
    let replaced = committed(outcome, "the member that took over commits the replace");
    heal(&runtime, &config);
    let (stat, bytes) = read(&runtime, &c, path);
    assert_eq!(stat.object_id, before.object_id);
    if let Some(replaced) = replaced {
        assert_eq!(stat.version, replaced.version);
    }
    assert_eq!(
        stat.version,
        crate::intent::stable_write_target_version(&c, &stat.object_id, operation)
    );
    assert_eq!(bytes, operation.as_bytes());
    runtime.close().unwrap();
}

/// `node`'s durable row of the write `intent_id` to `object_id`.
fn intent_at(
    runtime: &DfsTestRuntime,
    node: &NodeId,
    config: &DfsMetadataShardConfig,
    object_id: &crate::DfsObjectId,
    intent_id: &str,
) -> Option<crate::DfsWriteIntentRecord> {
    use crate::DfsMetadataBackend as _;
    crate::LocalSobjMetadataBackend::open_for_inspection(
        crate::DfsMetadataShardId::new(config.shard_id.clone()),
        shard_root(runtime, node, config),
    )
    .ok()?
    .get_intent(object_id, intent_id)
    .ok()
    .flatten()
}

/// The status of `node`'s durable prepare `operation`.
fn prepared_at(
    runtime: &DfsTestRuntime,
    node: &NodeId,
    config: &DfsMetadataShardConfig,
    operation: &str,
) -> Option<crate::DfsPreparedOperationStatus> {
    use crate::DfsMetadataBackend as _;
    crate::LocalSobjMetadataBackend::open_for_inspection(
        crate::DfsMetadataShardId::new(config.shard_id.clone()),
        shard_root(runtime, node, config),
    )
    .ok()?
    .prepared_operation_record(operation)
    .ok()
    .flatten()
    .map(|record| record.status)
}

/// A write of `bytes` over `base` from `requester`'s own client under
/// `operation`.
fn write_over(
    writer: &mut crate::NetworkedDfsService<crate::DfsNodeTransport>,
    config: &DfsMetadataShardConfig,
    requester: &NodeId,
    base: &crate::DfsStat,
    operation: &str,
    bytes: &[u8],
) -> Result<crate::NetworkedDfsWriteOutcome, crate::DfsError> {
    writer.write_distributed_reader_with_candidates(
        sinomo_core::DfsWriteRequest {
            file_system_id: base.file_system_id.clone(),
            object_id: base.object_id.clone(),
            target_version: crate::intent::stable_write_target_version(
                requester,
                &base.object_id,
                operation,
            ),
            requesting_node_id: requester.clone(),
        },
        std::io::Cursor::new(bytes),
        members(config),
        crate::DfsReplicationPolicy::new(2, 3).unwrap(),
        crate::service_write::DfsDistributedWriteOptions {
            operation_id: Some(operation.into()),
            ..Default::default()
        },
    )
}

/// Cut the primary off and lose it; `successor` takes over, as only its
/// keeper runs, and activates.
fn takes_over(runtime: &DfsTestRuntime, config: &DfsMetadataShardConfig, successor: &NodeId) {
    take_over(runtime, config, successor, false);
}

/// [`takes_over`] with the maintenance a successor may run early: one tick
/// once the record names it primary but before it activates, and one once
/// it activates.
fn takes_over_with_early_ticks(
    runtime: &DfsTestRuntime,
    config: &DfsMetadataShardConfig,
    successor: &NodeId,
) {
    take_over(runtime, config, successor, true);
}

/// One maintenance tick at `node`.
fn tick(runtime: &DfsTestRuntime, node: &NodeId) {
    runtime
        .maintenance_tick(node, sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
        .unwrap()
        .outcome
        .unwrap();
}

fn take_over(
    runtime: &DfsTestRuntime,
    config: &DfsMetadataShardConfig,
    successor: &NodeId,
    early_ticks: bool,
) {
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    if early_ticks {
        runtime.hold_takeover_activation(successor, true);
    }
    runtime.partition_node(
        &p,
        Partition {
            kv: true,
            dfs: true,
        },
    );
    lose(runtime, &[&b, &c], &p);
    if early_ticks {
        wait_until(
            "the record names the member primary",
            runtime,
            &[successor],
            || {
                try_record_from(runtime, config, successor)
                    .is_some_and(|record| &record.shard.primary_node_id == successor)
            },
        );
        tick(runtime, successor);
        runtime.hold_takeover_activation(successor, false);
    }
    wait_until(
        "the member takes over and activates",
        runtime,
        &[successor],
        || {
            !shard_events(
                runtime,
                successor,
                config,
                DfsMetadataTakeoverEvent::ACTIVATED,
            )
            .is_empty()
        },
    );
    assert_eq!(
        &record_from(runtime, config, successor)
            .shard
            .primary_node_id,
        successor
    );
    if early_ticks {
        tick(runtime, successor);
    }
}

/// `requester`'s own client writes over `base` under `operation` while the
/// primary cannot stage to either member: the primary keeps the write's
/// only copy and answers it `Pending`, and the client keeps the write's
/// request. Both members hold the write's row when the primary is lost and
/// `successor` takes over, with its early ticks when `early_ticks`.
/// Returns the client and the write's intent id.
fn open_write_past_a_lost_primary(
    runtime: &DfsTestRuntime,
    config: &DfsMetadataShardConfig,
    base: &crate::DfsStat,
    operation: &str,
    requester: &NodeId,
    successor: &NodeId,
    early_ticks: bool,
) -> (crate::NetworkedDfsService<crate::DfsNodeTransport>, String) {
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    keep_with_paths(runtime, config, &[successor]);
    cannot_stage(runtime, config);
    let mut writer = runtime.client(requester).unwrap();
    let outcome = write_over(
        &mut writer,
        config,
        requester,
        base,
        operation,
        operation.as_bytes(),
    )
    .expect("the primary answers the write");
    assert!(
        matches!(outcome, crate::NetworkedDfsWriteOutcome::Pending { .. }),
        "the primary holds the write's only copy: {outcome:?}"
    );
    let intent_id = crate::intent::stable_write_intent_id(requester, &base.object_id, operation);
    wait_until("both members hold the write's row", runtime, &[&b], || {
        [&b, &c]
            .into_iter()
            .all(|member| intent_at(runtime, member, config, &base.object_id, &intent_id).is_some())
    });
    take_over(runtime, config, successor, early_ticks);
    runtime.clear_faults();
    (writer, intent_id)
}

/// How the member `owner` that took over answered the requester's abort
/// `answer` of the open write `intent_id` over `base`, once: the write is
/// aborted and never commits, or `owner` completed it first from a live copy
/// of its bytes and the abort is answered with that commit. Returns the
/// version and bytes the file then holds.
fn assert_abort_answered_once(
    runtime: &DfsTestRuntime,
    config: &DfsMetadataShardConfig,
    owner: &NodeId,
    base: &crate::DfsStat,
    intent_id: &str,
    answer: &crate::DistributedWriteIntent,
    operation: &str,
) -> (crate::DfsVersion, Vec<u8>) {
    let row = intent_at(runtime, owner, config, &base.object_id, intent_id).unwrap();
    assert_eq!(*answer, crate::intent::distributed_intent_from_record(&row));
    match row.status {
        crate::DfsWriteIntentStatus::Aborted => {
            assert_eq!(answer.state, crate::DfsWriteIntentState::Aborted);
            (base.version.clone(), b"before".to_vec())
        }
        crate::DfsWriteIntentStatus::Committed => {
            assert_eq!(answer.state, crate::DfsWriteIntentState::Committed);
            (answer.target_version.clone(), operation.as_bytes().to_vec())
        }
        other => panic!("the abort is answered with the write's end: {other:?}"),
    }
}

/// An open write the lost primary answered `Pending` is answered once at
/// the member that took over, from the write's row, when the requester
/// aborts it: aborted, or answered with the commit if the member completed
/// the write first from the requester's copy. A repeat of an abort under its
/// id, as a requester whose answer was lost sends it, is answered from the
/// row as that same abort.
fn abort_of_an_open_write_at_a_successor(name: &str, early_ticks: bool) {
    let (runtime, config) = owned_shard(name);
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    let path = "/services/owned/aborted.txt";
    let operation = "successor-abort";
    let before = committed_before(&runtime, &config, path);
    let (mut writer, intent_id) =
        open_write_past_a_lost_primary(&runtime, &config, &before, operation, &c, &b, early_ticks);
    writer.set_transport_operation_deadline(Some(Instant::now() + EVENT_BOUND));
    let answer = writer
        .abort_pending_write(&intent_id)
        .expect("the member that took over answers the abort");
    let (version, content) = assert_abort_answered_once(
        &runtime, &config, &b, &before, &intent_id, &answer, operation,
    );
    if answer.state == crate::DfsWriteIntentState::Aborted {
        let row = intent_at(&runtime, &b, &config, &before.object_id, &intent_id);
        let repeat = writer
            .route_namespace_operation_to_metadata_primary_with_request_id_and_options(
                crate::service_ownership::pending_write_abort_request_id(&intent_id),
                before.file_system_id.clone(),
                b.clone(),
                crate::DfsNamespaceOperation::AbortPendingWrite {
                    path: path.into(),
                    object_id: before.object_id.clone(),
                    target_version: answer.target_version.clone(),
                    intent_id: intent_id.clone(),
                    write_operation_id: operation.into(),
                },
                EVENT_BOUND,
                false,
                false,
            )
            .expect("the repeat is answered");
        assert_eq!(repeat.result, crate::DfsNamespaceResult::Applied);
        assert!(
            matches!(
                &repeat.posix_coordination_result,
                Some(crate::DfsPosixCoordinationResult::AbortedWriteIntent(intent))
                    if **intent == answer
            ),
            "the repeat is answered as the same abort: {:?}",
            repeat.posix_coordination_result
        );
        assert_eq!(
            intent_at(&runtime, &b, &config, &before.object_id, &intent_id),
            row,
            "the repeat changes nothing"
        );
    }
    drop(writer);
    heal(&runtime, &config);
    let (stat, bytes) = read(&runtime, &c, path);
    assert_eq!(stat.version, version, "the write ended once");
    assert_eq!(bytes, content);
    runtime.close().unwrap();
}

#[test]
fn native_abort_of_an_open_write_at_a_successor_is_answered_once() {
    abort_of_an_open_write_at_a_successor("native-successor-abort", false);
}

/// [`native_abort_of_an_open_write_at_a_successor_is_answered_once`] after the
/// successor's early maintenance ticks, which can complete the write from the
/// requester's copy before the abort arrives.
#[test]
fn native_abort_of_an_open_write_at_a_successor_after_early_ticks_is_answered_once() {
    abort_of_an_open_write_at_a_successor("native-successor-abort-early", true);
}

/// C's own client writes over `base` under `operation`; the primary staged
/// it at W and its final prepare reached both members, but the primary's
/// commit is held. The primary is lost and `successor` takes over, and its
/// activation commits the prepare it holds. Returns the client and the
/// write's intent id once the successor's row records the commit.
fn final_prepare_held_past_a_lost_primary(
    runtime: &DfsTestRuntime,
    config: &DfsMetadataShardConfig,
    base: &crate::DfsStat,
    operation: &str,
    successor: &NodeId,
) -> (crate::NetworkedDfsService<crate::DfsNodeTransport>, String) {
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    keep_with_paths(runtime, config, &[successor]);
    let intent_id = crate::intent::stable_write_intent_id(&c, &base.object_id, operation);
    let prepare = format!("write:{intent_id}");
    let hold = crate::metadata_backend::prepared_operation::hold_prepared_commit(
        &[shard_root(runtime, &p, config)],
        &prepare,
    );
    let mut writer = runtime.client(&c).unwrap();
    let outcome = write_over(
        &mut writer,
        config,
        &c,
        base,
        operation,
        operation.as_bytes(),
    );
    assert!(
        !matches!(
            outcome,
            Ok(crate::NetworkedDfsWriteOutcome::Committed { .. })
        ),
        "the primary cannot commit the write: {outcome:?}"
    );
    wait_until(
        "both members hold the final prepare",
        runtime,
        &[&b],
        || {
            [&b, &c].into_iter().all(|member| {
                prepared_at(runtime, member, config, &prepare)
                    == Some(crate::DfsPreparedOperationStatus::Prepared)
            })
        },
    );
    takes_over(runtime, config, successor);
    drop(hold);
    wait_until(
        "the member that took over commits the final prepare",
        runtime,
        &[successor],
        || {
            intent_at(runtime, successor, config, &base.object_id, &intent_id)
                .is_some_and(|row| row.status == crate::DfsWriteIntentStatus::Committed)
        },
    );
    (writer, intent_id)
}

/// A write whose final prepare the members hold, its primary lost before
/// committing it: the member that took over commits the prepare it holds,
/// and the caller's one call is answered with that commit, once. The same
/// operation id is then answered from the commit.
#[test]
fn native_final_prepare_held_by_members_commits_once_at_the_successor() {
    let (runtime, config) = owned_shard("native-successor-final-prepare");
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    let path = "/services/owned/prepared.txt";
    let operation = "successor-final-prepare";
    let before = committed_before(&runtime, &config, path);
    keep_with_paths(&runtime, &config, &[&b]);
    let intent_id = crate::intent::stable_write_intent_id(&c, &before.object_id, operation);
    let prepare = format!("write:{intent_id}");
    let hold = crate::metadata_backend::prepared_operation::hold_prepared_commit(
        &[shard_root(&runtime, &p, &config)],
        &prepare,
    );
    let data_dir = runtime.data_dir(&c).clone();
    let call_options = options(&config);
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        let mut client =
            crate::runtime_host::open_dfs_runtime_service(&data_dir, Some(executor().handle()))
                .expect("the requester opens its client");
        let _ = sender.send(client.posix_write_file_with_operation_id(
            path,
            operation.as_bytes(),
            call_options,
            operation,
        ));
    });
    wait_until(
        "both members hold the final prepare",
        &runtime,
        &[&b],
        || {
            [&b, &c].into_iter().all(|member| {
                prepared_at(&runtime, member, &config, &prepare)
                    == Some(crate::DfsPreparedOperationStatus::Prepared)
            })
        },
    );
    takes_over(&runtime, &config, &b);
    drop(hold);
    let outcome = receiver.recv_timeout(EVENT_BOUND * 2).unwrap_or_else(|_| {
        panic!(
            "the call is answered once B has taken over\n{}",
            event_log(&runtime, &[&b, &c])
        )
    });
    let answered = committed(outcome, "the member that took over commits the write");
    let target = crate::intent::stable_write_target_version(&c, &before.object_id, operation);
    if let Some(answered) = answered {
        assert_eq!(answered.version, target);
    }
    let row = intent_at(&runtime, &b, &config, &before.object_id, &intent_id).unwrap();
    assert_eq!(row.status, crate::DfsWriteIntentStatus::Committed);
    heal(&runtime, &config);
    let (stat, bytes) = read(&runtime, &c, path);
    assert_eq!(stat.object_id, before.object_id);
    assert_eq!(
        stat.version, target,
        "the write committed once, at its own version"
    );
    assert_eq!(bytes, operation.as_bytes());
    let again = runtime
        .client(&c)
        .unwrap()
        .posix_write_file_with_operation_id(path, operation.as_bytes(), options(&config), operation)
        .expect("the same operation id is answered from the commit");
    assert_eq!(again.version, target);
    runtime.close().unwrap();
}

/// The requester aborts a write whose final prepare the member that took
/// over has already committed: the abort is answered with the commit, and
/// the write stays committed.
#[test]
fn native_abort_of_a_write_its_successor_committed_is_answered_with_the_commit() {
    let (runtime, config) = owned_shard("native-successor-abort-committed");
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    let path = "/services/owned/committed.txt";
    let operation = "successor-abort-committed";
    let before = committed_before(&runtime, &config, path);
    let (mut writer, intent_id) =
        final_prepare_held_past_a_lost_primary(&runtime, &config, &before, operation, &b);
    let row = intent_at(&runtime, &b, &config, &before.object_id, &intent_id).unwrap();
    writer.set_transport_operation_deadline(Some(Instant::now() + EVENT_BOUND));
    let answer = writer
        .abort_pending_write(&intent_id)
        .expect("the abort is answered with the write's commit");
    assert_eq!(answer.state, crate::DfsWriteIntentState::Committed);
    assert_eq!(answer, crate::intent::distributed_intent_from_record(&row));
    assert_eq!(
        intent_at(&runtime, &b, &config, &before.object_id, &intent_id),
        Some(row),
        "the abort changes nothing"
    );
    drop(writer);
    heal(&runtime, &config);
    let (stat, bytes) = read(&runtime, &c, path);
    assert_eq!(
        stat.version,
        crate::intent::stable_write_target_version(&c, &before.object_id, operation)
    );
    assert_eq!(bytes, operation.as_bytes());
    runtime.close().unwrap();
}

/// [`native_abort_of_an_open_write_at_a_successor_is_answered_once`] where the
/// requester's own node took over: its abort is answered from the row, as an
/// abort or with the commit it completed first from its own copy. The
/// requester is B, the member the takeover ranks first, so it takes over
/// without handing the takeover to a member whose keeper does not run.
fn abort_of_an_open_write_its_requester_took_over(name: &str, early_ticks: bool) {
    let (runtime, config) = owned_shard(name);
    let b = config.replica_node_ids[0].clone();
    let path = "/services/owned/aborted-local.txt";
    let operation = "successor-abort-local";
    let before = committed_before(&runtime, &config, path);
    let (mut writer, intent_id) =
        open_write_past_a_lost_primary(&runtime, &config, &before, operation, &b, &b, early_ticks);
    writer.set_transport_operation_deadline(Some(Instant::now() + EVENT_BOUND));
    let answer = writer
        .abort_pending_write(&intent_id)
        .expect("the requester's node answers the abort of the write it took over");
    let (version, content) = assert_abort_answered_once(
        &runtime, &config, &b, &before, &intent_id, &answer, operation,
    );
    drop(writer);
    heal(&runtime, &config);
    let (stat, bytes) = read(&runtime, &b, path);
    assert_eq!(stat.version, version, "the write ended once");
    assert_eq!(bytes, content);
    runtime.close().unwrap();
}

#[test]
fn native_abort_of_an_open_write_its_requester_took_over_is_answered_once() {
    abort_of_an_open_write_its_requester_took_over("native-successor-abort-local", false);
}

/// [`native_abort_of_an_open_write_its_requester_took_over_is_answered_once`]
/// after the successor's early maintenance ticks, which can complete the
/// write from its own copy before the abort arrives.
#[test]
fn native_abort_of_an_open_write_its_requester_took_over_after_early_ticks_is_answered_once() {
    abort_of_an_open_write_its_requester_took_over("native-successor-abort-local-early", true);
}

/// An open write whose only copy is on the primary that wrote it: the
/// primary's own client wrote it, the primary could stage it to neither
/// member and answered it `Pending`, and the primary is lost. The member that
/// took over holds the write's row and none of its bytes, and no live node
/// holds them. Its early maintenance ticks, before and after it activates,
/// and the reconciliation its activation requests leave the write open: a
/// write commits only with W durable receipts of its bytes.
#[test]
fn native_open_write_whose_only_copy_is_on_the_lost_primary_stays_open() {
    let (runtime, config) = owned_shard("native-successor-only-copy-lost");
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let path = "/services/owned/only-copy.txt";
    let operation = "successor-only-copy-lost";
    let before = committed_before(&runtime, &config, path);
    let (writer, intent_id) =
        open_write_past_a_lost_primary(&runtime, &config, &before, operation, &p, &b, true);
    let paths = crate::runtime_host::DfsRuntimePaths::new(runtime.data_dir(&b));
    let mut rounds = 0;
    while crate::runtime_host::daemon_dfs_maintenance_reconciliation_generation(&paths).is_some() {
        rounds += 1;
        assert!(rounds <= 16, "the activation's reconciliation completes");
        tick(&runtime, &b);
    }
    let row = intent_at(&runtime, &b, &config, &before.object_id, &intent_id).unwrap();
    assert_eq!(row.status, crate::DfsWriteIntentStatus::ReceiptsPending);
    assert_eq!(
        prepared_at(&runtime, &b, &config, &format!("write:{intent_id}")),
        None,
        "no commit was prepared"
    );
    let copy = crate::LocalSobjMetadataBackend::open_for_inspection(
        crate::DfsMetadataShardId::new(config.shard_id.clone()),
        shard_root(&runtime, &b, &config),
    )
    .unwrap()
    .export_table_snapshot()
    .unwrap();
    let receipts = copy
        .tables
        .receipts
        .iter()
        .filter(|receipt| {
            receipt.receipt.object_id == before.object_id
                && receipt.receipt.committed_version == row.target_version
        })
        .count();
    assert!(
        receipts < row.policy.min_commit_replicas,
        "the write holds fewer than W receipts: {receipts}"
    );
    assert_eq!(
        copy.tables
            .objects
            .iter()
            .find(|object| object.object_id == before.object_id)
            .map(|object| object.version.clone()),
        Some(before.version.clone()),
        "the file keeps its committed version"
    );
    drop(writer);
    runtime.close().unwrap();
}

/// An opening only the members hold, its primary lost before it committed
/// it: the member that took over re-issues the opening at its epoch, and the
/// re-issue's commit is held until the caller's same-id retry arrives. The
/// retry meets the opening and its re-issue both prepared, recovers the
/// opening through its re-issue, and the write commits once.
#[test]
fn native_retry_meeting_its_openings_recovery_commits_once() {
    use crate::DfsMetadataBackend as _;
    let (runtime, config) = owned_shard("native-successor-opening-reissue");
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    let path = "/services/owned/reissued.txt";
    let operation = "successor-opening-reissue";
    let before = committed_before(&runtime, &config, path);
    keep_with_paths(&runtime, &config, &[&b]);
    let intent_id = crate::intent::stable_write_intent_id(&c, &before.object_id, operation);
    // Neither member's answer reaches the primary: it never commits the
    // opening it prepared.
    let gates =
        [&b, &c].map(|member| super::commit_native_tests::hold_replies(&runtime, &config, member));
    let data_dir = runtime.data_dir(&c).clone();
    let call_options = options(&config);
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        let mut client =
            crate::runtime_host::open_dfs_runtime_service(&data_dir, Some(executor().handle()))
                .expect("the requester opens its client");
        let _ = sender.send(client.posix_write_file_with_operation_id(
            path,
            operation.as_bytes(),
            call_options,
            operation,
        ));
    });
    let opening_at = |member: &NodeId| {
        crate::LocalSobjMetadataBackend::open_for_inspection(
            crate::DfsMetadataShardId::new(config.shard_id.clone()),
            shard_root(&runtime, member, &config),
        )
        .ok()?
        .list_prepared_operations()
        .ok()?
        .into_iter()
        .find(|prepared| {
            prepared.payload.prepared_intent.is_none()
                && crate::service_routed_write::prepared_intent_for_write(
                    prepared,
                    &before.object_id,
                    &intent_id,
                )
                .is_some()
        })
        .map(|prepared| prepared.digest.operation_id)
    };
    wait_until("both members hold the opening", &runtime, &[&b], || {
        opening_at(&b).is_some() && opening_at(&c).is_some()
    });
    let opening = opening_at(&b).unwrap();
    let reissue = crate::metadata_records::recovery_operation_id(&opening, config.epoch + 1);
    let hold = std::sync::Mutex::new(Some(
        crate::metadata_backend::prepared_operation::hold_prepared_commit(
            &[shard_root(&runtime, &b, &config)],
            &reissue,
        ),
    ));
    // The hold ends when the caller's retry starts at B.
    let _release = crate::runtime_host::install_daemon_dfs_pre_dispatch_test_hook(
        runtime.data_dir(&b),
        std::sync::Arc::new(move |point, command, _| {
            if matches!(
                point,
                crate::runtime_host::DaemonDfsPreDispatchTestPoint::BeforeExecute { .. }
            ) && command.is_some_and(|command| {
                matches!(&command.routed_message.message,
                    crate::DfsNetworkMessage::DistributedWriteRequest(write)
                        if write.request_id == operation)
            }) {
                hold.lock().unwrap().take();
            }
            Ok(())
        }),
    );
    // Nothing the caller sends reaches B until B has re-issued the opening.
    let resend = std::sync::Arc::new(tokio::sync::Semaphore::new(0));
    runtime.install_fault(FaultRule {
        source: Some(c.clone()),
        target: Some(b.clone()),
        route: Some(crate::DFS_DATA_DISTRIBUTED_WRITE_ROUTE.into()),
        call_id: None,
        origin: None,
        remaining_hits: 32,
        action: FaultAction::PauseBeforeDispatch(std::sync::Arc::clone(&resend)),
    });
    takes_over(&runtime, &config, &b);
    for gate in &gates {
        super::commit_native_tests::release(gate);
    }
    wait_until("B re-issues the opening", &runtime, &[&b], || {
        prepared_at(&runtime, &b, &config, &reissue)
            == Some(crate::DfsPreparedOperationStatus::Prepared)
    });
    assert_eq!(
        prepared_at(&runtime, &b, &config, &opening),
        Some(crate::DfsPreparedOperationStatus::Prepared),
        "the opening stays prepared until its re-issue commits"
    );
    super::commit_native_tests::release(&resend);
    let outcome = receiver.recv_timeout(EVENT_BOUND * 2).unwrap_or_else(|_| {
        panic!(
            "{operation} is answered once B has taken over; C's calls {:?}\n{}",
            write_calls_from(&runtime, &c, operation),
            event_log(&runtime, &[&b, &c])
        )
    });
    runtime.clear_faults();
    let target = crate::intent::stable_write_target_version(&c, &before.object_id, operation);
    if let Some(stat) = committed(outcome, "the retry recovers its opening and commits") {
        assert_eq!(stat.version, target);
    }
    assert_eq!(
        prepared_at(&runtime, &b, &config, &reissue),
        Some(crate::DfsPreparedOperationStatus::Committed)
    );
    assert_eq!(
        prepared_at(&runtime, &b, &config, &opening),
        Some(crate::DfsPreparedOperationStatus::Superseded)
    );
    let _ = p;
    heal(&runtime, &config);
    let (stat, bytes) = read(&runtime, &c, path);
    assert_eq!(stat.version, target, "the write committed once");
    assert_eq!(bytes, operation.as_bytes());
    runtime.close().unwrap();
}

/// C's call writes over a file while the primary cannot stage to either
/// member, and the primary answers it `Pending`. The primary is lost and B
/// takes over, and B adopts the open write at its own epoch before the
/// call's same-id retry reaches it. Without `held`, the primary holds the
/// write's one copy, and B's copy from the retry completes W. With `held`,
/// the write's copies belong to B and C only, so B's copy from the retry is
/// one of two: B answers the retry `Pending` while it cannot stage to C,
/// and commits once its copy to C lands. Ends with the call answered and the
/// write committed once.
fn retry_after_its_successor_adopted_the_write(name: &str, held: bool, early_ticks: bool) {
    let (runtime, config) = owned_shard(name);
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    let path = "/services/owned/adopted.txt";
    let operation = "successor-adopted-retry";
    let before = committed_before(&runtime, &config, path);
    keep_with_paths(&runtime, &config, &[&b]);
    cannot_stage(&runtime, &config);
    runtime.checkpoint_rpc_observation_window().unwrap();
    let data_dir = runtime.data_dir(&c).clone();
    let mut call_options = options(&config);
    if held {
        call_options.policy = crate::DfsReplicationPolicy::new(2, 2).unwrap();
        call_options.policy.forbidden_node_ids = vec![p.clone()];
    }
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        let mut client =
            crate::runtime_host::open_dfs_runtime_service(&data_dir, Some(executor().handle()))
                .expect("the requester opens its client");
        let _ = sender.send(client.posix_write_file_with_operation_id(
            path,
            operation.as_bytes(),
            call_options,
            operation,
        ));
    });
    let intent_id = crate::intent::stable_write_intent_id(&c, &before.object_id, operation);
    let answered_by = |target: &NodeId| {
        let calls = runtime.rpc_observations();
        calls.iter().any(|call| {
            call.source == c
                && &call.target == target
                && call.dfs_request_id.as_deref() == Some(operation)
                && calls.iter().any(|answer| {
                    answer.call_id == call.call_id
                        && answer.phase == crate::test_runtime::RpcPhase::Completed
                })
        })
    };
    wait_until("the primary answers the write", &runtime, &[&b], || {
        answered_by(&p)
    });
    // Nothing the caller sends reaches B until B has adopted the write.
    let resend = std::sync::Arc::new(tokio::sync::Semaphore::new(0));
    runtime.install_fault(FaultRule {
        source: Some(c.clone()),
        target: Some(b.clone()),
        route: Some(crate::DFS_DATA_DISTRIBUTED_WRITE_ROUTE.into()),
        call_id: None,
        origin: None,
        remaining_hits: 32,
        action: FaultAction::PauseBeforeDispatch(std::sync::Arc::clone(&resend)),
    });
    if held {
        for route in [
            crate::DFS_DATA_STAGE_BLOB_ROUTE,
            crate::DFS_DATA_STAGE_PATCH_ROUTE,
            crate::DFS_DATA_REPAIR_REPLICA_ROUTE,
        ] {
            runtime.install_fault(FaultRule {
                source: Some(b.clone()),
                target: Some(c.clone()),
                route: Some(route.into()),
                call_id: None,
                origin: None,
                remaining_hits: 32,
                action: FaultAction::BudgetExhausted,
            });
        }
    }
    if early_ticks {
        takes_over_with_early_ticks(&runtime, &config, &b);
        // Every tick the takeover asks of B runs before the retry reaches
        // it, as an early tick can leave it.
        let paths = crate::runtime_host::DfsRuntimePaths::new(runtime.data_dir(&b));
        let mut rounds = 0;
        while crate::runtime_host::daemon_dfs_maintenance_reconciliation_generation(&paths)
            .is_some()
        {
            rounds += 1;
            assert!(rounds <= 16, "B's reconciliation completes");
            tick(&runtime, &b);
        }
    } else {
        takes_over(&runtime, &config, &b);
    }
    let epoch = record_from(&runtime, &config, &b).shard.epoch;
    wait_until("B adopts the write at its epoch", &runtime, &[&b], || {
        intent_at(&runtime, &b, &config, &before.object_id, &intent_id)
            .is_some_and(|row| row.owner_epoch == epoch)
    });
    let b_root = runtime.data_dir(&b).root();
    let refused_before = crate::maintenance::stage_wakes::refused_stages_noted(&b_root);
    super::commit_native_tests::release(&resend);
    if held {
        wait_until("B answers the retry", &runtime, &[&b], || answered_by(&b));
        let early = receiver.recv_timeout(Duration::from_secs(1));
        assert!(
            early.is_err(),
            "B holds the write below W: {early:?}; C's calls {:?}",
            write_calls_from(&runtime, &c, operation)
        );
        // B's stage to C was refused: its re-send timer, or C's path coming
        // back, sends it again, with no tick to run it.
        assert!(
            crate::maintenance::stage_wakes::refused_stages_noted(&b_root) > refused_before,
            "B records its refused stage to C"
        );
        if early_ticks {
            // A tick under the refusal sends the stage again from the write B
            // hydrated before the retry, with the size its row recorded.
            tick(&runtime, &b);
        }
        // B's copy to C lands: the write commits, and its commit answers
        // the call.
        runtime.clear_faults();
    }
    let outcome = receiver.recv_timeout(EVENT_BOUND * 2).unwrap_or_else(|_| {
        panic!(
            "{operation} is answered once B has adopted it; C's calls {:?}\n{}",
            write_calls_from(&runtime, &c, operation),
            event_log(&runtime, &[&b, &c])
        )
    });
    runtime.clear_faults();
    let target = crate::intent::stable_write_target_version(&c, &before.object_id, operation);
    if let Some(stat) = committed(outcome, "the member that took over commits the write") {
        assert_eq!(stat.version, target);
    }
    let to_b = write_calls_from(&runtime, &c, operation)
        .into_iter()
        .filter(|(target, route, _, _)| {
            target == &b && route == crate::DFS_DATA_DISTRIBUTED_WRITE_ROUTE
        })
        .count();
    assert_eq!(
        to_b, 1,
        "the call sent the write to B once, and B answered it"
    );
    heal(&runtime, &config);
    let (stat, bytes) = read(&runtime, &c, path);
    assert_eq!(stat.version, target, "the write committed once");
    assert_eq!(bytes, operation.as_bytes());
    runtime.close().unwrap();
}

/// The member that took over adopted the open write at its own epoch
/// before the caller's same-id retry reached it: the retry's content still
/// binds to the write there, and the write commits once.
#[test]
fn native_retry_after_its_successor_adopted_the_write_commits_once() {
    retry_after_its_successor_adopted_the_write("native-successor-adopted-retry", false, false);
}

/// As [`native_retry_after_its_successor_adopted_the_write_commits_once`],
/// with the write's copies on B and C only: the member that took over holds
/// one of two while it serves the retry, answers it `Pending` and holds that
/// answer, and the write's commit answers the caller's one call.
#[test]
fn native_retry_held_at_its_successor_is_answered_by_its_commit() {
    retry_after_its_successor_adopted_the_write("native-successor-held-retry", true, false);
}

/// [`native_retry_held_at_its_successor_is_answered_by_its_commit`] where B
/// runs every tick its takeover asks for before the retry reaches it: before
/// it activates, once it activates and its reconciliation. No tick follows
/// the refusal of B's stage to C; the stage's own wake sends it again once C
/// takes it, and the commit answers the call.
#[test]
fn native_retry_held_at_its_successor_after_early_ticks_is_answered_by_its_commit() {
    retry_after_its_successor_adopted_the_write("native-successor-held-retry-early", true, true);
}
