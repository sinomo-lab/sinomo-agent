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
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    runtime.partition_node(
        &p,
        Partition {
            kv: true,
            dfs: true,
        },
    );
    lose(runtime, &[&b, &c], &p);
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
}

/// `requester`'s own client writes over `base` under `operation` while the
/// primary cannot stage to either member: the primary keeps the write's
/// only copy and answers it `Pending`, and the client keeps the write's
/// request. Both members hold the write's row when the primary is lost and
/// `successor` takes over. Returns the client and the write's intent id.
fn open_write_past_a_lost_primary(
    runtime: &DfsTestRuntime,
    config: &DfsMetadataShardConfig,
    base: &crate::DfsStat,
    operation: &str,
    requester: &NodeId,
    successor: &NodeId,
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
    takes_over(runtime, config, successor);
    runtime.clear_faults();
    (writer, intent_id)
}

/// An open write the lost primary answered `Pending` is aborted at the
/// member that took over, from the write's row: the requester's abort
/// closes it once. A repeat of the abort under its id, as a requester whose
/// answer was lost sends it, is answered from the row as that same abort.
#[test]
fn native_abort_of_an_open_write_at_a_successor_aborts_it_once() {
    let (runtime, config) = owned_shard("native-successor-abort");
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    let path = "/services/owned/aborted.txt";
    let operation = "successor-abort";
    let before = committed_before(&runtime, &config, path);
    let (mut writer, intent_id) =
        open_write_past_a_lost_primary(&runtime, &config, &before, operation, &c, &b);
    writer.set_transport_operation_deadline(Some(Instant::now() + EVENT_BOUND));
    let aborted = writer
        .abort_pending_write(&intent_id)
        .expect("the member that took over aborts the write");
    let row = intent_at(&runtime, &b, &config, &before.object_id, &intent_id).unwrap();
    assert_eq!(row.status, crate::DfsWriteIntentStatus::Aborted);
    assert_eq!(aborted.state, crate::DfsWriteIntentState::Aborted);
    assert_eq!(aborted, crate::intent::distributed_intent_from_record(&row));
    let repeat = writer
        .route_namespace_operation_to_metadata_primary_with_request_id_and_options(
            crate::service_ownership::pending_write_abort_request_id(&intent_id),
            before.file_system_id.clone(),
            b.clone(),
            crate::DfsNamespaceOperation::AbortPendingWrite {
                path: path.into(),
                object_id: before.object_id.clone(),
                target_version: aborted.target_version.clone(),
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
                if **intent == aborted
        ),
        "the repeat is answered as the same abort: {:?}",
        repeat.posix_coordination_result
    );
    assert_eq!(
        intent_at(&runtime, &b, &config, &before.object_id, &intent_id),
        Some(row),
        "the repeat changes nothing"
    );
    drop(writer);
    heal(&runtime, &config);
    let (stat, bytes) = read(&runtime, &c, path);
    assert_eq!(
        stat.version, before.version,
        "the aborted write never committed"
    );
    assert_eq!(bytes, b"before");
    runtime.close().unwrap();
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

/// [`native_abort_of_an_open_write_at_a_successor_aborts_it_once`] where the
/// requester's own node took over: its abort closes the write from the row.
/// The requester is B, the member the takeover ranks first, so it takes over
/// without handing the takeover to a member whose keeper does not run.
#[test]
fn native_abort_of_an_open_write_its_requester_took_over_aborts_it_once() {
    let (runtime, config) = owned_shard("native-successor-abort-local");
    let b = config.replica_node_ids[0].clone();
    let path = "/services/owned/aborted-local.txt";
    let operation = "successor-abort-local";
    let before = committed_before(&runtime, &config, path);
    let (mut writer, intent_id) =
        open_write_past_a_lost_primary(&runtime, &config, &before, operation, &b, &b);
    writer.set_transport_operation_deadline(Some(Instant::now() + EVENT_BOUND));
    let aborted = writer
        .abort_pending_write(&intent_id)
        .expect("the requester's node aborts the write it took over");
    let row = intent_at(&runtime, &b, &config, &before.object_id, &intent_id).unwrap();
    assert_eq!(row.status, crate::DfsWriteIntentStatus::Aborted);
    assert_eq!(aborted, crate::intent::distributed_intent_from_record(&row));
    drop(writer);
    heal(&runtime, &config);
    let (stat, bytes) = read(&runtime, &b, path);
    assert_eq!(
        stat.version, before.version,
        "the aborted write never committed"
    );
    assert_eq!(bytes, b"before");
    runtime.close().unwrap();
}
