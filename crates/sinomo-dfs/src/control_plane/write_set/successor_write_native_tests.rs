//! An open content write whose primary is lost, on the native test runtime:
//! the primary held the write's only copy and answered it `Pending`, the
//! write's row reached the members, and a member took over. The member that
//! took over owns the open write: it adopts it at its own epoch and the
//! caller's same operation id finishes it there once.

use super::commit_native_tests::{catch_up, directory, members, primary, switched_with_kv_voters};
use super::takeover_driver_native_tests::{event_log, keep_with_paths, lose};
use super::takeover_loss_native_tests::{record_from, reelect, try_record_from, wait_until};
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

/// A three-member W=2 shard whose members hold `/services/owned`.
fn owned_shard(name: &str) -> (DfsTestRuntime, DfsMetadataShardConfig) {
    let (runtime, config) = switched_with_kv_voters(name, Some((2, 3)));
    {
        let mut service = primary(&runtime);
        directory(&mut service, "/services/owned");
    }
    for member in &config.replica_node_ids {
        catch_up(&runtime, &config, member);
    }
    (runtime, config)
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
    for target in [&b, &c] {
        for route in [
            crate::DFS_DATA_STAGE_BLOB_ROUTE,
            crate::DFS_DATA_STAGE_PATCH_ROUTE,
            crate::DFS_DATA_REPAIR_REPLICA_ROUTE,
        ] {
            runtime.install_fault(FaultRule {
                source: Some(p.clone()),
                target: Some(target.clone()),
                route: Some(route.into()),
                call_id: None,
                origin: None,
                remaining_hits: 32,
                action: FaultAction::BudgetExhausted,
            });
        }
    }
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
            "{operation} is answered once a member has taken over\n{}",
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

/// `path` committed with one copy, on the primary, before the test's write.
fn committed_before(
    runtime: &DfsTestRuntime,
    config: &DfsMetadataShardConfig,
    path: &str,
) -> crate::DfsStat {
    let mut service = primary(runtime);
    let before = service
        .posix_write_file(
            path,
            b"before",
            crate::DfsWriteOptions {
                policy: crate::DfsReplicationPolicy::new(1, 1).unwrap(),
                replica_candidates: vec![config.primary_node_id.clone()],
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
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
