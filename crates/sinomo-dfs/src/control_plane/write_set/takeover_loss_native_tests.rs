//! Takeover with real primary loss on the native test runtime: every node is
//! a cluster-KV voter, so stopping the shard primary, which also leads
//! cluster KV here, or cutting a node off with a partition, leaves a voter
//! quorum that re-elects and commits.

use super::commit_native_tests::{
    catch_up, directory, fail_journal_calls, mode_change, primary, shard_root, shrink,
    switched_with_kv_voters, write_set_of,
};
use super::takeover_driver_native_tests::{
    event_log, keep_with_paths, lose, object_mode, takeover_events,
};
use super::takeover_native_tests::{
    member_state_path, prepare_unknown, shard_id_of, store_archives,
};
use super::*;
use crate::DfsMetadataTakeoverEvent;
use crate::test_runtime::{FaultAction, FaultRule, Partition, executor};
use crate::test_support::DfsTestRuntime;
use sinomo_core::network::{NetworkDeliveryState, NetworkError, PeerPathVerdict};
use sinomo_core::service::ServiceError;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// The cluster-KV leader `node`'s own Raft state names.
fn kv_leader_seen_by(runtime: &DfsTestRuntime, node: &NodeId) -> Option<NodeId> {
    use sinomo_core::cluster::ClusterKvPathsExt;
    let paths = sinomo_core::network::ClusterPaths::new(runtime.data_dir(node));
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&paths).unwrap();
    paths.kv_paths().known_raft_voter_leader_id(&state).unwrap()
}

/// Try one mode change to `path` through `node`'s own service on the shard
/// as `shard` configures it, within `deadline` when one is given. No other
/// shard is consulted.
fn try_mode_at(
    runtime: &DfsTestRuntime,
    shard: &DfsMetadataShardConfig,
    node: &NodeId,
    path: &str,
    mode: u32,
    operation: &str,
    deadline: Option<Duration>,
) -> Result<crate::DfsObjectId, String> {
    let mut service = crate::runtime_host::open_dfs_runtime_service_on_shard(
        runtime.data_dir(node),
        Some(executor().handle()),
        shard,
    )
    .map_err(|error| error.to_string())?;
    let current = service
        .lookup_sifs_path(path)
        .map_err(|error| error.to_string())?
        .ok_or("the object does not exist")?;
    service
        .transport
        .set_operation_deadline(deadline.map(|deadline| Instant::now() + deadline));
    let committed =
        service.publish_prepared_operation(mode_change(shard, &current, mode, operation));
    service.transport.set_operation_deadline(None);
    match committed {
        Ok(committed) if committed.published => Ok(current.object_id),
        Ok(_) => Err("the change was not published".into()),
        Err(error) => Err(format!("{error:?}")),
    }
}

/// Commit one mode change to `path` through `primary`'s own service, retried
/// under fresh lookups: activation's reconciliation can still be changing the
/// object, and an election after a loss can fail a commit transiently.
fn commit_mode_at(
    runtime: &DfsTestRuntime,
    shard: &DfsMetadataShardConfig,
    primary: &NodeId,
    path: &str,
    mode: u32,
    operation: &str,
) -> crate::DfsObjectId {
    commit_mode_eventually(runtime, shard, primary, path, mode, operation)
}

/// Commit one KV write from `node`, retried until the voters have elected a
/// leader it reaches. Tests that cut voters off from cluster KV use it before
/// closing, so the election a returning voter's higher term forces has
/// settled and no KV call outlives the close.
pub(super) fn reelect(runtime: &DfsTestRuntime, survivor: &NodeId) {
    let cluster = runtime
        .component_context(survivor)
        .unwrap()
        .cluster_access()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let request = sinomo_core::kv::KvTxnRequest::generated(sinomo_core::kv::KvTxn {
            compares: Vec::new(),
            success: vec![sinomo_core::kv::KvOp::Put {
                key: format!("/test/takeover-loss/reelect/{survivor}"),
                value: b"elected".to_vec(),
                lease_id: None,
            }],
            failure: Vec::new(),
        });
        match executor().block_on(cluster.kv_commit_compact_async(request)) {
            Ok(_) => return,
            Err(error) => {
                assert!(
                    Instant::now() < deadline,
                    "the survivors elect no KV leader: {error}"
                );
                std::thread::sleep(Duration::from_millis(200));
            }
        }
    }
}

/// The shard record as `reader` reads it. A stopped, cut-off or returning
/// voter makes the survivors elect again, and an authoritative read during
/// that election fails transiently, so a failed read is retried.
pub(super) fn record_from(
    runtime: &DfsTestRuntime,
    config: &DfsMetadataShardConfig,
    reader: &NodeId,
) -> DfsMetadataShardRecordCut {
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        if let Some(record) = try_record_from(runtime, config, reader) {
            return record;
        }
        assert!(
            Instant::now() < deadline,
            "{reader} cannot read the shard record"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// The shard record as `reader` reads it, or `None` while its read fails.
pub(super) fn try_record_from(
    runtime: &DfsTestRuntime,
    config: &DfsMetadataShardConfig,
    reader: &NodeId,
) -> Option<DfsMetadataShardRecordCut> {
    let mut client = runtime.configuration_publisher(reader);
    DfsControlPlaneStore::new(client.as_mut())
        .read_metadata_shard_record_cut(
            &config.file_system_id,
            &config.shard_id,
            &super::commit_native_tests::members(config),
        )
        .ok()
        .flatten()
}

/// Replicate `primary`'s whole journal to `members` through the ordinary
/// durability barrier, from the shard as `shard` configures it. A change
/// committed through a bounded effect service reaches the followers' stores
/// with the next replication; this is that replication.
fn catch_up_from(
    runtime: &DfsTestRuntime,
    shard: &DfsMetadataShardConfig,
    primary: &NodeId,
    members: &[&NodeId],
) {
    let mut service = crate::runtime_host::open_dfs_runtime_service_on_shard(
        runtime.data_dir(primary),
        Some(executor().handle()),
        shard,
    )
    .unwrap();
    let shard_id = crate::DfsMetadataShardId::new(shard.shard_id.clone());
    let head = service
        .metadata_backend()
        .unwrap()
        .metadata_prefix_proof(&shard_id)
        .unwrap()
        .expect("the primary has a prefix");
    // The barrier certifies against the replicas it names: the reachable
    // write-set members, not every configured replica.
    let mut replica_node_ids = shard
        .replica_node_ids
        .iter()
        .filter(|node| members.contains(node))
        .cloned()
        .collect::<Vec<_>>();
    replica_node_ids.sort();
    let target = crate::DfsMetadataQuorumSyncTarget {
        file_system_id: shard.file_system_id.clone(),
        shard_id,
        primary_node_id: shard.primary_node_id.clone(),
        authority_epoch: Some(shard.epoch),
        replica_node_ids,
        expected_shard: Some(shard.clone()),
        journal_sequence: head.journal_sequence,
        barrier_id: sinomo_core::Id::generate().to_string(),
        state_fingerprint: head.digest.clone(),
        checkpoint_snapshot: None,
    };
    let members = members
        .iter()
        .map(|member| (*member).clone())
        .collect::<Vec<_>>();
    service
        .sync_metadata_shard_to_quorum_intersecting(&target, &members, Duration::from_secs(30))
        .expect("the members catch up");
}

/// [`commit_mode_at`], retried under fresh operation ids while the write set
/// cannot acknowledge yet.
fn commit_mode_eventually(
    runtime: &DfsTestRuntime,
    shard: &DfsMetadataShardConfig,
    primary: &NodeId,
    path: &str,
    mode: u32,
    operation: &str,
) -> crate::DfsObjectId {
    let deadline = Instant::now() + Duration::from_secs(90);
    for attempt in 0.. {
        match try_mode_at(
            runtime,
            shard,
            primary,
            path,
            mode,
            &format!("{operation}-{attempt}"),
            Some(Duration::from_secs(5)),
        ) {
            Ok(object) => return object,
            Err(error) => assert!(
                Instant::now() < deadline,
                "{operation} never commits: {error}"
            ),
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    unreachable!()
}

/// `node`'s durable journal head sequence for the shard.
fn journal_head(runtime: &DfsTestRuntime, node: &NodeId, config: &DfsMetadataShardConfig) -> u64 {
    super::commit_native_tests::durable_prefix(runtime, node, config)
        .unwrap()
        .journal_sequence
}

/// `operation`'s prepared-operation status in `node`'s copy of the shard.
fn operation_status(
    runtime: &DfsTestRuntime,
    node: &NodeId,
    config: &DfsMetadataShardConfig,
    operation: &str,
) -> Option<crate::DfsPreparedOperationStatus> {
    use crate::DfsMetadataBackend as _;
    crate::LocalSobjMetadataBackend::open_for_inspection(
        shard_id_of(config),
        shard_root(runtime, node, config),
    )
    .ok()?
    .prepared_operation_record(operation)
    .ok()
    .flatten()
    .map(|record| record.status)
}

/// `node`'s durable write-set member state for the shard, if it keeps one.
fn member_state(
    runtime: &DfsTestRuntime,
    node: &NodeId,
    config: &DfsMetadataShardConfig,
) -> Option<crate::DfsMetadataWriteSetMemberState> {
    crate::metadata_io::read_sobj_optional(&member_state_path(runtime, node, config))
        .ok()
        .flatten()
}

fn promise_of(
    runtime: &DfsTestRuntime,
    node: &NodeId,
    config: &DfsMetadataShardConfig,
) -> Option<crate::DfsMetadataTakeoverPromise> {
    member_state(runtime, node, config).and_then(|state| state.promise)
}

/// Whether `request` writes the shard's write-set record: on a member that is
/// not the primary, only a takeover swap does.
fn writes_the_write_set(
    config: &DfsMetadataShardConfig,
    request: &sinomo_core::kv::KvTxnRequest,
) -> bool {
    let key = crate::metadata_shard_write_set_key(&config.file_system_id, &config.shard_id);
    request
        .txn
        .success
        .iter()
        .any(|op| matches!(op, sinomo_core::kv::KvOp::Put { key: put, .. } if *put == key))
}

/// Refuse `node`'s takeover swaps before they reach cluster KV while the
/// registration lives; its attempt keeps the promise and re-sends.
fn hold_swaps(
    runtime: &DfsTestRuntime,
    config: &DfsMetadataShardConfig,
    node: &NodeId,
) -> crate::test_support::DfsTestNativeKvHookRegistration {
    let config = config.clone();
    runtime
        .native_kv_request_hook(node, move |request| {
            if writes_the_write_set(&config, request) {
                Err(ServiceError::Network(NetworkError::delivery_failed(
                    "native fixture held swap",
                    NetworkDeliveryState::DefinitelyNotSent,
                    "the test holds this takeover swap",
                )))
            } else {
                Ok(())
            }
        })
        .unwrap()
}

/// `node`'s takeover events named `name` for this shard only: a keeper takes
/// over every switched shard whose primary it judges lost, and the default
/// root and users shards switch too once every member advertises the
/// capability.
pub(super) fn shard_events(
    runtime: &DfsTestRuntime,
    node: &NodeId,
    config: &DfsMetadataShardConfig,
    name: &str,
) -> Vec<DfsMetadataTakeoverEvent> {
    takeover_events(runtime, node, name)
        .into_iter()
        .filter(|event| event.shard_id == config.shard_id)
        .collect()
}

pub(super) fn the_other(one: &NodeId, b: &NodeId, c: &NodeId) -> NodeId {
    if one == b { c.clone() } else { b.clone() }
}

fn sorted(mut nodes: Vec<NodeId>) -> Vec<NodeId> {
    nodes.sort();
    nodes
}

pub(super) fn wait_until(
    what: &str,
    runtime: &DfsTestRuntime,
    nodes: &[&NodeId],
    mut done: impl FnMut() -> bool,
) {
    let deadline = Instant::now() + Duration::from_secs(90);
    while !done() {
        assert!(
            Instant::now() < deadline,
            "{what} did not happen:\n{}\nkeeping {:?}",
            event_log(runtime, nodes),
            nodes
                .iter()
                .map(|node| runtime.write_set_keeping_events(node))
                .collect::<Vec<_>>()
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Each node's mode for `object`, for a failure message.
fn modes(
    runtime: &DfsTestRuntime,
    config: &DfsMetadataShardConfig,
    nodes: &[&NodeId],
    object: &crate::DfsObjectId,
) -> Vec<(NodeId, Option<u32>)> {
    nodes
        .iter()
        .map(|node| ((*node).clone(), object_mode(runtime, node, config, object)))
        .collect()
}

/// TK1: the primary, which also leads cluster KV, stops abruptly while a
/// change it prepared is durable on one member only. That member has the
/// longer journal and takes over once the surviving KV voters re-elect; the
/// reconciliation commits the prepared change at the new epoch, a new write
/// commits on the remaining write set, and the stopped primary stays
/// configured outside it.
#[test]
fn native_takeover_after_the_primary_and_kv_leader_stops() {
    let (mut runtime, config) = switched_with_kv_voters("native-loss-stop", None);
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    assert_eq!(
        kv_leader_seen_by(&runtime, &b).as_ref(),
        Some(&p),
        "P leads cluster KV"
    );
    let current = {
        let mut service = primary(&runtime);
        directory(&mut service, "/services/loss")
    };
    for member in [&b, &c] {
        catch_up(&runtime, &config, member);
    }
    // X is prepared on P and C; B never sees it.
    fail_journal_calls(&runtime, &config, &b);
    prepare_unknown(&runtime, &config, &current, "loss-x");
    runtime.clear_faults();
    keep_with_paths(&runtime, &config, &[&b, &c]);

    runtime.stop_node(&p).unwrap();
    lose(&runtime, &[&b, &c], &p);
    wait_until("C takes over", &runtime, &[&b, &c], || {
        record_from(&runtime, &config, &b).shard.primary_node_id == c
    });
    let record = record_from(&runtime, &config, &b);
    assert_eq!(record.shard.epoch, config.epoch + 1);
    assert_eq!(
        record.write_set_state().switched().unwrap().write_set,
        sorted(vec![b.clone(), c.clone()]),
        "P stays configured, outside the write set"
    );
    assert!(record.shard.replica_node_ids.contains(&p));
    wait_until(
        "the prepared change commits at the new epoch",
        &runtime,
        &[&b, &c],
        || object_mode(&runtime, &c, &config, &current.object_id) == Some(0o640),
    );
    wait_until("C activates", &runtime, &[&b, &c], || {
        !shard_events(&runtime, &c, &config, DfsMetadataTakeoverEvent::ACTIVATED).is_empty()
    });
    assert!(shard_events(&runtime, &b, &config, DfsMetadataTakeoverEvent::SWAPPED).is_empty());
    assert!(
        shard_root(&runtime, &p, &config).exists(),
        "P's copy is left intact"
    );
    // Every default shard has P as its primary here and only this one is
    // switched, so a POSIX path, which resolves from the root shard, has no
    // authority left. The new primary's own service commits on this shard.
    let after = commit_mode_at(
        &runtime,
        &record.shard,
        &c,
        "/services/loss",
        0o600,
        "loss-after",
    );
    catch_up_from(&runtime, &record.shard, &c, &[&b]);
    wait_until(
        "a write commits on the remaining write set",
        &runtime,
        &[&b, &c],
        || {
            [&b, &c]
                .into_iter()
                .all(|member| object_mode(&runtime, member, &config, &after) == Some(0o600))
        },
    );
    wait_until(
        "the new primary's fence is retired",
        &runtime,
        &[&b, &c],
        || !member_state_path(&runtime, &c, &config).exists(),
    );
    runtime.close().unwrap();
}

/// TK2: the first successor stops too, and the member left takes over again
/// at the next epoch, asking nobody: the write set is the two of them. The
/// old primary's return keeps a KV voter quorum without the first successor;
/// it stays outside the write set until the new primary reaches it, which
/// re-adds it as an ordinary member, and a change then commits on both.
#[test]
fn native_second_takeover_after_the_first_successor_stops() {
    let (mut runtime, config) = switched_with_kv_voters("native-loss-second", Some((1, 3)));
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    {
        let mut service = primary(&runtime);
        directory(&mut service, "/services/second");
    }
    for member in [&b, &c] {
        catch_up(&runtime, &config, member);
    }
    keep_with_paths(&runtime, &config, &[&b, &c]);
    runtime.stop_node(&p).unwrap();
    lose(&runtime, &[&b, &c], &p);
    wait_until("a member takes over", &runtime, &[&b, &c], || {
        record_from(&runtime, &config, &b).shard.primary_node_id != p
    });
    let first = record_from(&runtime, &config, &b).shard.primary_node_id;
    let second = the_other(&first, &b, &c);
    wait_until("the first successor activates", &runtime, &[&b, &c], || {
        !shard_events(
            &runtime,
            &first,
            &config,
            DfsMetadataTakeoverEvent::ACTIVATED,
        )
        .is_empty()
    });

    // The members still see P lost, so nobody re-adds it yet.
    runtime.reopen_node(p.clone()).unwrap();
    runtime.stop_node(&first).unwrap();
    lose(&runtime, &[&second], &first);
    wait_until("the other member takes over", &runtime, &[&second], || {
        record_from(&runtime, &config, &second)
            .shard
            .primary_node_id
            == second
    });
    let record = record_from(&runtime, &config, &second);
    assert_eq!(record.shard.epoch, config.epoch + 2);
    assert_eq!(
        record.write_set_state().switched().unwrap().write_set,
        vec![second.clone()]
    );
    let started = shard_events(
        &runtime,
        &second,
        &config,
        DfsMetadataTakeoverEvent::UNCORROBORATED_STARTED,
    );
    let last = started
        .last()
        .expect("the second takeover started an attempt");
    assert_eq!(last.epoch, config.epoch + 1, "{started:?}");
    assert!(
        last.node_ids.is_empty(),
        "a two-member write set asks nobody: {last:?}"
    );
    wait_until(
        "the second successor activates",
        &runtime,
        &[&second],
        || {
            !shard_events(
                &runtime,
                &second,
                &config,
                DfsMetadataTakeoverEvent::ACTIVATED,
            )
            .is_empty()
        },
    );

    runtime.set_peer_verdict(&second, &p, PeerPathVerdict::Path, false);
    wait_until("P rejoins the write set", &runtime, &[&second], || {
        record_from(&runtime, &config, &second)
            .write_set_state()
            .switched()
            .is_some_and(|write_set| write_set.contains(&p))
    });
    let record = record_from(&runtime, &config, &second);
    assert_eq!(
        record.shard.primary_node_id, second,
        "P is not made primary again"
    );
    let after = commit_mode_at(
        &runtime,
        &record.shard,
        &second,
        "/services/second",
        0o600,
        "second-after",
    );
    catch_up_from(&runtime, &record.shard, &second, &[&p]);
    wait_until("the change commits on both", &runtime, &[&second], || {
        [&second, &p]
            .into_iter()
            .all(|member| object_mode(&runtime, member, &config, &after) == Some(0o600))
    });
    runtime.close().unwrap();
}

/// TK5: the primary stays up, but a partition cuts it off from the members on
/// every DFS route and from cluster KV, and a member takes over. A commit the
/// old primary attempts from then on commits nothing, at it or anywhere.
/// Once its KV returns it reads the record that deposed it and refuses as a
/// deposed primary. Once the network heals, the new primary aligns it, which
/// drops that unacknowledged tail, catches it up and re-adds it; it is never
/// made primary again.
#[test]
fn native_takeover_of_a_partitioned_live_primary() {
    let (runtime, config) = switched_with_kv_voters("native-loss-partitioned", Some((2, 3)));
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    {
        let mut service = primary(&runtime);
        directory(&mut service, "/services/partitioned");
    }
    for member in [&b, &c] {
        catch_up(&runtime, &config, member);
    }
    keep_with_paths(&runtime, &config, &[&b, &c]);
    runtime.partition_node(
        &p,
        Partition {
            kv: true,
            dfs: true,
        },
    );
    lose(&runtime, &[&b, &c], &p);
    wait_until("a member takes over", &runtime, &[&b, &c], || {
        record_from(&runtime, &config, &b).shard.primary_node_id != p
    });
    let winner = record_from(&runtime, &config, &b).shard.primary_node_id;
    let other = the_other(&winner, &b, &c);
    wait_until("the new primary activates", &runtime, &[&b, &c], || {
        !shard_events(
            &runtime,
            &winner,
            &config,
            DfsMetadataTakeoverEvent::ACTIVATED,
        )
        .is_empty()
    });

    // The old primary still serves and still holds its old record.
    let late = try_mode_at(
        &runtime,
        &config,
        &p,
        "/services/partitioned",
        0o640,
        "partitioned-late",
        Some(Duration::from_secs(5)),
    );
    assert!(
        late.is_err(),
        "the cut-off primary commits nothing: {late:?}"
    );
    assert_ne!(
        operation_status(&runtime, &p, &config, "partitioned-late"),
        Some(crate::DfsPreparedOperationStatus::Committed)
    );
    for member in [&winner, &other] {
        assert_eq!(
            operation_status(&runtime, member, &config, "partitioned-late"),
            None
        );
    }

    // Its KV returns: it reads the record that deposed it, and refuses.
    runtime.partition_node(
        &p,
        Partition {
            kv: false,
            dfs: true,
        },
    );
    // Cut off, P kept campaigning; its higher term makes the voters elect
    // again before its reads succeed.
    wait_until("P reads the new record", &runtime, &[&b, &c], || {
        try_record_from(&runtime, &config, &p)
            .is_some_and(|record| record.shard.primary_node_id == winner)
    });
    let deposed = try_mode_at(
        &runtime,
        &config,
        &p,
        "/services/partitioned",
        0o644,
        "partitioned-deposed",
        Some(Duration::from_secs(5)),
    );
    assert!(
        deposed.is_err(),
        "the deposed primary commits nothing: {deposed:?}"
    );
    assert_ne!(
        operation_status(&runtime, &p, &config, "partitioned-deposed"),
        Some(crate::DfsPreparedOperationStatus::Committed)
    );

    // The network heals and the new primary reaches it.
    runtime.partition_node(&p, Partition::default());
    runtime.set_peer_verdict(&winner, &p, PeerPathVerdict::Path, false);
    wait_until("P rejoins the write set", &runtime, &[&b, &c], || {
        write_set_of(&runtime, &config).contains(&p)
    });
    let record = record_from(&runtime, &config, &b);
    assert_eq!(
        record.shard.primary_node_id, winner,
        "P is not made primary again"
    );
    assert_ne!(
        operation_status(&runtime, &p, &config, "partitioned-late"),
        Some(crate::DfsPreparedOperationStatus::Committed),
        "the old primary's own tail did not survive"
    );
    let observed = runtime.rpc_observations().len();
    let after = commit_mode_at(
        &runtime,
        &record.shard,
        &winner,
        "/services/partitioned",
        0o600,
        "partitioned-after",
    );
    catch_up_from(&runtime, &record.shard, &winner, &[&other, &p]);
    let deadline = Instant::now() + Duration::from_secs(90);
    while ![&winner, &other, &p]
        .into_iter()
        .all(|member| object_mode(&runtime, member, &config, &after) == Some(0o600))
    {
        assert!(
            Instant::now() < deadline,
            "the change commits on every member: {:?} {:?}, record {:?}\n{}\nkeeping {:?}\nafter the commit {:?}",
            modes(&runtime, &config, &[&winner, &other, &p], &after),
            [&winner, &other, &p].map(|member| operation_status(
                &runtime,
                member,
                &config,
                "partitioned-after"
            )),
            record_from(&runtime, &config, &b).write_set_state(),
            event_log(&runtime, &[&b, &c]),
            runtime.write_set_keeping_events(&winner),
            {
                let mut routes = std::collections::BTreeMap::<String, usize>::new();
                for event in runtime.rpc_observations().into_iter().skip(observed) {
                    *routes
                        .entry(format!(
                            "{} {}->{} {:?} {:?}",
                            event.route,
                            event.source,
                            event.target,
                            event.phase,
                            event.outcome_error
                        ))
                        .or_default() += 1;
                }
                routes
            }
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    for node in [&p, &b, &c] {
        reelect(&runtime, node);
    }
    runtime.close().unwrap();
}

/// TK11: a primary every member still reaches is never deposed, however
/// slowly it answers: its replies to one member are held 7 s once, past that
/// member's network deadline, then 400 ms on each of 20 writes. Every write
/// commits, and no member starts an attempt or promises.
#[test]
fn native_takeover_never_deposes_a_slow_primary() {
    let (runtime, config) = switched_with_kv_voters("native-loss-slow", None);
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    keep_with_paths(&runtime, &config, &[&p, &b, &c]);
    // P's replies to C on its namespace and write routes wait for the gate.
    let hold = |gate: &Arc<tokio::sync::Semaphore>, hits: usize| {
        for route in [
            crate::DFS_DATA_NAMESPACE_ROUTE,
            crate::DFS_DATA_DISTRIBUTED_WRITE_ROUTE,
        ] {
            runtime.install_fault(FaultRule {
                source: Some(c.clone()),
                target: Some(p.clone()),
                route: Some(route.into()),
                call_id: None,
                origin: None,
                remaining_hits: hits,
                action: FaultAction::PauseAfterDispatch(Arc::clone(gate)),
            });
        }
    };
    let release_every = |gate: &Arc<tokio::sync::Semaphore>, period: Duration| {
        let (gate, stop) = (
            Arc::clone(gate),
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
        );
        let (stopped, opened) = (Arc::clone(&stop), Arc::clone(&gate));
        let thread = std::thread::spawn(move || {
            while !stopped.load(Ordering::Acquire) {
                std::thread::sleep(period);
                gate.add_permits(1);
            }
        });
        move || {
            stop.store(true, Ordering::Release);
            thread.join().unwrap();
            // No reply stays held once the writes are done.
            opened.add_permits(tokio::sync::Semaphore::MAX_PERMITS / 2);
        }
    };

    // A write through C's client to P, retried while it fails. A spent hold
    // rule refuses further calls, so a failed attempt clears the holds.
    let write = |path: &str| {
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            let written = runtime.client(&c).unwrap().posix_write_file(
                path,
                path.as_bytes(),
                crate::DfsWriteOptions::replicated_eventual(vec![p.clone()]),
            );
            match written {
                Ok(_) => return,
                Err(error) => {
                    assert!(Instant::now() < deadline, "{path} was not written: {error}");
                    runtime.clear_faults();
                    std::thread::sleep(Duration::from_millis(200));
                }
            }
        }
    };

    // One reply held past C's network deadline: the write fails once and its
    // retry commits.
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    hold(&gate, 32);
    let stop = release_every(&gate, Duration::from_secs(7));
    write("/services/slow-held.txt");
    stop();
    runtime.clear_faults();
    for index in 0..20 {
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        hold(&gate, 32);
        let stop = release_every(&gate, Duration::from_millis(400));
        write(&format!("/services/slow-{index}.txt"));
        stop();
        runtime.clear_faults();
    }
    for node in [&b, &c] {
        assert!(
            runtime
                .write_set_takeover_events(node)
                .iter()
                .all(|(_, event)| event.shard_id != config.shard_id),
            "{node} started nothing:\n{}",
            event_log(&runtime, &[&b, &c])
        );
        assert!(
            promise_of(&runtime, node, &config).is_none(),
            "{node} promised nothing"
        );
    }
    let record = record_from(&runtime, &config, &p);
    assert_eq!(
        (record.shard.primary_node_id, record.shard.epoch),
        (p, config.epoch)
    );
    runtime.close().unwrap();
}

/// TK13 (b): with the primary stopped, two members that judge each other
/// unreachable both judge it lost, each ranks alone, and both promise from
/// the same cut before either swap reaches cluster KV. They reach each other
/// again before the swaps are submitted. Exactly one swap applies; the other
/// settles rejected with the epoch advanced and its candidate follows the
/// winner. Neither keeps a promise, and the winner commits with the other.
#[test]
fn native_concurrent_candidates_promise_from_one_cut_and_one_swap_applies() {
    let (mut runtime, config) = switched_with_kv_voters("native-loss-race", None);
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    {
        let mut service = primary(&runtime);
        directory(&mut service, "/services/race");
    }
    for member in [&b, &c] {
        catch_up(&runtime, &config, member);
    }
    keep_with_paths(&runtime, &config, &[&b, &c]);
    let held = [
        hold_swaps(&runtime, &config, &b),
        hold_swaps(&runtime, &config, &c),
    ];
    runtime.stop_node(&p).unwrap();
    runtime.set_peer_verdict(&b, &c, PeerPathVerdict::NoPath, false);
    runtime.set_peer_verdict(&c, &b, PeerPathVerdict::NoPath, false);
    lose(&runtime, &[&b, &c], &p);
    wait_until("both promise", &runtime, &[&b, &c], || {
        promise_of(&runtime, &b, &config).is_some() && promise_of(&runtime, &c, &config).is_some()
    });
    let (at_b, at_c) = (
        promise_of(&runtime, &b, &config).unwrap(),
        promise_of(&runtime, &c, &config).unwrap(),
    );
    assert_eq!(at_b.cut, at_c.cut, "both promised from the same cut");
    assert_ne!(at_b.swap_request_id, at_c.swap_request_id);
    // Judging each other unreachable only made both rank alone. They reach
    // each other again before either swap is submitted, so the one that is
    // superseded has no reason to judge the winner lost.
    runtime.set_peer_verdict(&b, &c, PeerPathVerdict::Path, false);
    runtime.set_peer_verdict(&c, &b, PeerPathVerdict::Path, false);
    drop(held);

    wait_until(
        "one swap applies and the other is superseded",
        &runtime,
        &[&b, &c],
        || {
            let swapped = [&b, &c]
                .into_iter()
                .filter(|node| {
                    !shard_events(&runtime, node, &config, DfsMetadataTakeoverEvent::SWAPPED)
                        .is_empty()
                })
                .count();
            let superseded = [&b, &c]
                .into_iter()
                .filter(|node| {
                    !shard_events(
                        &runtime,
                        node,
                        &config,
                        DfsMetadataTakeoverEvent::SWAP_SUPERSEDED,
                    )
                    .is_empty()
                })
                .count();
            swapped == 1 && superseded == 1
        },
    );
    let record = record_from(&runtime, &config, &b);
    assert_eq!(
        record.shard.epoch,
        config.epoch + 1,
        "exactly one epoch advance"
    );
    let winner = record.shard.primary_node_id.clone();
    let loser = the_other(&winner, &b, &c);
    assert_eq!(
        shard_events(
            &runtime,
            &winner,
            &config,
            DfsMetadataTakeoverEvent::SWAPPED
        )
        .len(),
        1
    );
    assert!(shard_events(&runtime, &loser, &config, DfsMetadataTakeoverEvent::SWAPPED).is_empty());
    wait_until("no promise is left", &runtime, &[&b, &c], || {
        promise_of(&runtime, &b, &config).is_none() && promise_of(&runtime, &c, &config).is_none()
    });
    assert!(
        attempts_started_at(&runtime, &loser, &config, config.epoch + 1).is_empty(),
        "the superseded candidate follows the winner"
    );
    let after = commit_mode_eventually(
        &runtime,
        &record.shard,
        &winner,
        "/services/race",
        0o600,
        "race-after",
    );
    catch_up_from(&runtime, &record.shard, &winner, &[&loser]);
    wait_until(
        "the winner commits with the other",
        &runtime,
        &[&b, &c],
        || {
            [&b, &c]
                .into_iter()
                .all(|member| object_mode(&runtime, member, &config, &after) == Some(0o600))
        },
    );
    runtime.close().unwrap();
}

/// Refuse `node`'s writes of the shard's write-set record before they reach
/// cluster KV unless `admit` passes the request.
fn gate_write_set_writes(
    runtime: &DfsTestRuntime,
    config: &DfsMetadataShardConfig,
    node: &NodeId,
    admit: impl Fn(&sinomo_core::kv::KvTxnRequest) -> bool + Send + Sync + 'static,
) -> crate::test_support::DfsTestNativeKvHookRegistration {
    let config = config.clone();
    runtime
        .native_kv_request_hook(node, move |request| {
            if writes_the_write_set(&config, request) && !admit(request) {
                Err(ServiceError::Network(NetworkError::delivery_failed(
                    "native fixture held swap",
                    NetworkDeliveryState::DefinitelyNotSent,
                    "the test holds this write-set write",
                )))
            } else {
                Ok(())
            }
        })
        .unwrap()
}

/// `node`'s attempts for this shard that started at `epoch`, asked or not.
fn attempts_started_at(
    runtime: &DfsTestRuntime,
    node: &NodeId,
    config: &DfsMetadataShardConfig,
    epoch: u64,
) -> Vec<DfsMetadataTakeoverEvent> {
    [
        DfsMetadataTakeoverEvent::CORROBORATION_STARTED,
        DfsMetadataTakeoverEvent::UNCORROBORATED_STARTED,
    ]
    .into_iter()
    .flat_map(|name| shard_events(runtime, node, config, name))
    .filter(|event| event.epoch == epoch)
    .collect()
}

/// Two survivors of a stopped primary, one superseded by the other in a
/// fixed order.
struct Superseded {
    runtime: DfsTestRuntime,
    config: DfsMetadataShardConfig,
    /// The candidate whose swap is still held: it settles superseded once
    /// `b_open` is set.
    b: NodeId,
    /// The candidate whose swap applied: primary at the next epoch.
    c: NodeId,
    b_open: Arc<std::sync::atomic::AtomicBool>,
    /// C writes the record again (a shrink, an add) only once this is set.
    c_open: Arc<std::sync::atomic::AtomicBool>,
    _gates: [crate::test_support::DfsTestNativeKvHookRegistration; 2],
}

impl Superseded {
    fn release_b(&self) {
        self.b_open.store(true, Ordering::SeqCst);
        wait_until(
            "B is superseded",
            &self.runtime,
            &[&self.b, &self.c],
            || {
                !shard_events(
                    &self.runtime,
                    &self.b,
                    &self.config,
                    DfsMetadataTakeoverEvent::SWAP_SUPERSEDED,
                )
                .is_empty()
            },
        );
    }

    fn heal(&self) {
        self.runtime.partition_node(&self.b, Partition::default());
        self.runtime
            .set_peer_verdict(&self.b, &self.c, PeerPathVerdict::Path, false);
        self.runtime
            .set_peer_verdict(&self.c, &self.b, PeerPathVerdict::Path, false);
    }

    fn record(&self) -> DfsMetadataShardRecordCut {
        record_from(&self.runtime, &self.config, &self.b)
    }

    fn write_set(&self) -> Vec<NodeId> {
        self.record()
            .write_set_state()
            .switched()
            .unwrap()
            .write_set
            .clone()
    }

    /// Wait until `observer`'s keeper has asked the network to verify `peer`
    /// once more than `seen` times: it evaluated again and is still waiting.
    fn asks_again(&self, observer: &NodeId, peer: &NodeId, seen: usize) -> usize {
        let asked = || {
            self.runtime
                .peer_verification_requests(observer)
                .iter()
                .filter(|asked| *asked == peer)
                .count()
        };
        wait_until(
            "the keeper asks the network to verify the peer",
            &self.runtime,
            &[&self.b, &self.c],
            || asked() > seen,
        );
        asked()
    }

    /// The winner commits a change that the other acknowledges.
    fn commits_on_both(&self, operation: &str) {
        let record = self.record();
        let primary = record.shard.primary_node_id.clone();
        let other = the_other(&primary, &self.b, &self.c);
        let after = commit_mode_eventually(
            &self.runtime,
            &record.shard,
            &primary,
            "/services/race",
            0o600,
            operation,
        );
        catch_up_from(&self.runtime, &record.shard, &primary, &[&other]);
        wait_until(
            "the primary commits with the other",
            &self.runtime,
            &[&self.b, &self.c],
            || {
                [&self.b, &self.c].into_iter().all(|member| {
                    object_mode(&self.runtime, member, &self.config, &after) == Some(0o600)
                })
            },
        );
    }

    fn close(self) {
        self.c_open.store(true, Ordering::SeqCst);
        self.b_open.store(true, Ordering::SeqCst);
        self.runtime.close().unwrap();
    }
}

/// Both survivors promise from one cut with every write of the record held.
/// C's own swap alone is then admitted and applies; B's stays held. With
/// `probe_owners` each survivor's network accepts verification requests and
/// the test settles them; without a probe owner, a standing verdict cannot start a new campaign.
/// With `healed_first` the two reach each other again before C's swap is
/// admitted, so B first reads C's epoch with a path to it.
fn one_candidate_superseded_by_the_other(
    name: &str,
    probe_owners: bool,
    healed_first: bool,
) -> Superseded {
    use std::sync::atomic::AtomicBool;
    let (mut runtime, config) = switched_with_kv_voters(name, None);
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    {
        let mut service = primary(&runtime);
        directory(&mut service, "/services/race");
    }
    for member in [&b, &c] {
        catch_up(&runtime, &config, member);
    }
    keep_with_paths(&runtime, &config, &[&b, &c]);
    for node in [&b, &c] {
        runtime.set_probe_owner(node, probe_owners);
    }
    let b_open = Arc::new(AtomicBool::new(false));
    let c_open = Arc::new(AtomicBool::new(false));
    let c_swap = Arc::new(std::sync::Mutex::new(None::<sinomo_core::kv::KvRequestId>));
    let gates = [
        gate_write_set_writes(&runtime, &config, &b, {
            let b_open = Arc::clone(&b_open);
            move |_| b_open.load(Ordering::SeqCst)
        }),
        // C: only its own promised swap, or everything once opened.
        gate_write_set_writes(&runtime, &config, &c, {
            let (c_open, c_swap) = (Arc::clone(&c_open), Arc::clone(&c_swap));
            move |request| {
                c_open.load(Ordering::SeqCst)
                    || c_swap.lock().unwrap().as_ref() == Some(&request.request_id)
            }
        }),
    ];
    runtime.stop_node(&p).unwrap();
    runtime.set_peer_verdict(&b, &c, PeerPathVerdict::NoPath, false);
    runtime.set_peer_verdict(&c, &b, PeerPathVerdict::NoPath, false);
    lose(&runtime, &[&b, &c], &p);
    wait_until("both promise", &runtime, &[&b, &c], || {
        promise_of(&runtime, &b, &config).is_some() && promise_of(&runtime, &c, &config).is_some()
    });
    let (at_b, at_c) = (
        promise_of(&runtime, &b, &config).unwrap(),
        promise_of(&runtime, &c, &config).unwrap(),
    );
    assert_eq!(at_b.cut, at_c.cut, "both promised from the same cut");
    assert!(
        !runtime.peer_verification_requests(&b).contains(&c)
            && !runtime.peer_verification_requests(&b).contains(&p),
        "a verdict that turned no-path under the record needs no examination"
    );
    if healed_first {
        runtime.set_peer_verdict(&b, &c, PeerPathVerdict::Path, false);
        runtime.set_peer_verdict(&c, &b, PeerPathVerdict::Path, false);
    } else {
        runtime.partition_node(
            &b,
            Partition {
                kv: false,
                dfs: true,
            },
        );
    }
    // C's swap alone reaches cluster KV: C wins.
    *c_swap.lock().unwrap() = Some(at_c.swap_request_id.clone());
    wait_until("C's swap applies", &runtime, &[&b, &c], || {
        !shard_events(&runtime, &c, &config, DfsMetadataTakeoverEvent::SWAPPED).is_empty()
    });
    let record = record_from(&runtime, &config, &c);
    assert_eq!(
        (record.shard.primary_node_id.clone(), record.shard.epoch),
        (c.clone(), config.epoch + 1)
    );
    assert_eq!(
        record.write_set_state().switched().unwrap().write_set,
        sorted(vec![b.clone(), c.clone()]),
        "the stopped primary left the write set; B stays in it"
    );
    assert!(
        shard_events(
            &runtime,
            &b,
            &config,
            DfsMetadataTakeoverEvent::SWAP_SUPERSEDED
        )
        .is_empty(),
        "B's swap is still held"
    );
    Superseded {
        runtime,
        config,
        b,
        c,
        b_open,
        c_open,
        _gates: gates,
    }
}

/// TK13 (c): two survivors that cannot reach each other and both reach
/// cluster KV. Each network owner confirms the standing verdict under
/// the new epoch. The elected one removes the other, the other takes
/// the elected one over, and cluster KV orders the two guarded transactions:
/// whichever applies first leaves its node primary and sole write-set
/// member, and the other's is refused. Once they reach each other the
/// primary adds the other back.
#[test]
fn native_partitioned_survivors_settle_on_the_first_guarded_transaction() {
    let (mut runtime, config) = switched_with_kv_voters("native-loss-partitioned", None);
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    {
        let mut service = primary(&runtime);
        directory(&mut service, "/services/race");
    }
    for member in [&b, &c] {
        catch_up(&runtime, &config, member);
    }
    keep_with_paths(&runtime, &config, &[&b, &c]);
    let held = [
        hold_swaps(&runtime, &config, &b),
        hold_swaps(&runtime, &config, &c),
    ];
    runtime.stop_node(&p).unwrap();
    runtime.set_peer_verdict(&b, &c, PeerPathVerdict::NoPath, false);
    runtime.set_peer_verdict(&c, &b, PeerPathVerdict::NoPath, false);
    lose(&runtime, &[&b, &c], &p);
    wait_until("both promise", &runtime, &[&b, &c], || {
        promise_of(&runtime, &b, &config).is_some() && promise_of(&runtime, &c, &config).is_some()
    });
    runtime.partition_node(
        &b,
        Partition {
            kv: false,
            dfs: true,
        },
    );
    for member in [&b, &c] {
        runtime.set_probe_owner(member, true);
    }
    drop(held);
    // The two roles can race only after the new epoch is visible locally.
    // Answer each actual request for examination under that epoch.
    let mut answered = std::collections::BTreeSet::new();
    let alone = || {
        try_record_from(&runtime, &config, &b).filter(|record| {
            record
                .write_set_state()
                .switched()
                .is_some_and(|write_set| {
                    write_set.write_set == vec![record.shard.primary_node_id.clone()]
                })
        })
    };
    wait_until(
        "one survivor is primary and alone in the write set",
        &runtime,
        &[&b, &c],
        || {
            for (observer, peer) in [(&b, &c), (&c, &b)] {
                if !answered.contains(observer)
                    && runtime.peer_verification_requests(observer).contains(peer)
                {
                    runtime.settle_peer_verification(observer, peer, PeerPathVerdict::NoPath);
                    answered.insert(observer.clone());
                }
            }
            alone().is_some()
        },
    );
    wait_until("no promise is left", &runtime, &[&b, &c], || {
        promise_of(&runtime, &b, &config).is_none() && promise_of(&runtime, &c, &config).is_none()
    });
    let record = alone().expect("the record stays settled while they are partitioned");
    let primary_node = record.shard.primary_node_id.clone();
    let other = the_other(&primary_node, &b, &c);
    assert!(primary_node == b || primary_node == c);
    let swaps = [&b, &c]
        .into_iter()
        .map(|node| shard_events(&runtime, node, &config, DfsMetadataTakeoverEvent::SWAPPED).len())
        .sum::<usize>();
    assert_eq!(
        record.shard.epoch,
        config.epoch + swaps as u64,
        "one epoch per applied swap"
    );
    assert!(
        (1..=2).contains(&swaps),
        "the first election, and at most the superseded candidate's own:\n{}",
        event_log(&runtime, &[&b, &c])
    );

    runtime.partition_node(&b, Partition::default());
    runtime.set_peer_verdict(&b, &c, PeerPathVerdict::Path, false);
    runtime.set_peer_verdict(&c, &b, PeerPathVerdict::Path, false);
    // The primary's writes bring the other back; a join that fails waits for
    // the next change, so the write is repeated until it is added.
    let deadline = Instant::now() + Duration::from_secs(90);
    for round in 0.. {
        let record = record_from(&runtime, &config, &primary_node);
        commit_mode_eventually(
            &runtime,
            &record.shard,
            &primary_node,
            "/services/race",
            0o600,
            &format!("partitioned-after-{round}"),
        );
        let settle = Instant::now() + Duration::from_secs(10);
        while Instant::now() < settle
            && !record_from(&runtime, &config, &primary_node)
                .write_set_state()
                .switched()
                .unwrap()
                .contains(&other)
        {
            std::thread::sleep(Duration::from_millis(100));
        }
        if record_from(&runtime, &config, &primary_node)
            .write_set_state()
            .switched()
            .unwrap()
            .contains(&other)
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the primary adds the other back:\n{}\nkeeping {:?}",
            event_log(&runtime, &[&b, &c]),
            runtime.write_set_keeping_events(&primary_node)
        );
    }
    let record = record_from(&runtime, &config, &b);
    assert_eq!(
        (record.shard.primary_node_id.clone(), record.shard.epoch),
        (primary_node, config.epoch + swaps as u64),
        "healing changes no primary"
    );
    runtime.close().unwrap();
}

/// The partition of TK13 (c) with the superseded candidate's transaction
/// first: B's verdict for C answers as it stands, the write set has two
/// members so B asks nobody, and B's swap applies. C leaves the write set.
#[test]
fn native_partitioned_superseded_candidate_takes_over_when_its_swap_applies_first() {
    let race = one_candidate_superseded_by_the_other("native-loss-recampaign", true, false);
    let (runtime, config, b, c) = (&race.runtime, &race.config, &race.b, &race.c);
    race.release_b();
    race.asks_again(b, c, 0);
    runtime.settle_peer_verification(b, c, PeerPathVerdict::NoPath);
    wait_until("B takes over C", runtime, &[b, c], || {
        !shard_events(runtime, b, config, DfsMetadataTakeoverEvent::SWAPPED).is_empty()
    });
    let record = race.record();
    assert_eq!(
        (record.shard.primary_node_id.clone(), record.shard.epoch),
        (b.clone(), config.epoch + 2),
        "a second epoch advance: B deposed C"
    );
    assert_eq!(race.write_set(), vec![b.clone()], "C left the write set");
    let started = attempts_started_at(runtime, b, config, config.epoch + 1);
    assert_eq!(started.len(), 1, "{started:?}");
    assert_eq!(started[0].previous_primary_node_id, *c);
    assert_eq!(started[0].cause, "primary-no-path");
    assert!(
        shard_events(
            runtime,
            b,
            config,
            DfsMetadataTakeoverEvent::CORROBORATION_STARTED
        )
        .is_empty(),
        "B asked nobody in either attempt, and no event says it did"
    );
    assert_eq!(
        shard_events(
            runtime,
            b,
            config,
            DfsMetadataTakeoverEvent::UNCORROBORATED_STARTED
        )
        .len(),
        2
    );
    race.heal();
    race.close();
}

/// The partition of TK13 (c) with the elected primary's transaction first:
/// C removes B on its own verdict before B's swap settles. B is then no
/// write-set member and starts nothing.
#[test]
fn native_partitioned_elected_primary_removes_the_other_when_its_shrink_applies_first() {
    let race = one_candidate_superseded_by_the_other("native-loss-recampaign-shrink", true, false);
    let (runtime, config, b, c) = (&race.runtime, &race.config, &race.b, &race.c);
    race.c_open.store(true, Ordering::SeqCst);
    race.asks_again(c, b, 0);
    runtime.settle_peer_verification(c, b, PeerPathVerdict::NoPath);
    wait_until("C removes B", runtime, &[b, c], || {
        race.write_set() == vec![c.clone()]
    });
    race.release_b();
    wait_until("no promise is left", runtime, &[b, c], || {
        promise_of(runtime, b, config).is_none() && promise_of(runtime, c, config).is_none()
    });
    let record = race.record();
    assert_eq!(
        (record.shard.primary_node_id.clone(), record.shard.epoch),
        (c.clone(), config.epoch + 1)
    );
    assert_eq!(race.write_set(), vec![c.clone()]);
    assert!(shard_events(runtime, b, config, DfsMetadataTakeoverEvent::SWAPPED).is_empty());
    assert!(attempts_started_at(runtime, b, config, config.epoch + 1).is_empty());
    race.heal();
    race.close();
}

/// A superseded candidate that reaches the winner again before its swap
/// settles follows it and starts nothing.
#[test]
fn native_superseded_candidate_that_reaches_the_winner_follows_it() {
    let race = one_candidate_superseded_by_the_other("native-loss-follows", false, false);
    let (runtime, config, b, c) = (&race.runtime, &race.config, &race.b, &race.c);
    race.heal();
    race.release_b();
    wait_until("no promise is left", runtime, &[b, c], || {
        promise_of(runtime, b, config).is_none() && promise_of(runtime, c, config).is_none()
    });
    race.c_open.store(true, Ordering::SeqCst);
    // C commits with B: B acknowledges at C's epoch, so B never left it.
    race.commits_on_both("follows-after");
    let record = race.record();
    assert_eq!(
        (record.shard.primary_node_id.clone(), record.shard.epoch),
        (c.clone(), config.epoch + 1),
        "exactly one epoch advance"
    );
    assert!(shard_events(runtime, b, config, DfsMetadataTakeoverEvent::SWAPPED).is_empty());
    assert!(attempts_started_at(runtime, b, config, config.epoch + 1).is_empty());
    race.close();
}

/// TK13 (d): B's no-path verdict for C already stood when C was elected, so
/// only the record changed. On a node whose network can examine C, B asks it
/// to and starts nothing while that episode is open: not when another peer's
/// verdict changes, not when the configuration is published again, not
/// however long the episode takes. C, elected with the same standing verdict
/// for B, removes nobody either. The episodes settle with a path, and B
/// follows C.
#[test]
fn native_superseded_candidate_waits_for_the_network_to_examine_the_winner() {
    let race = one_candidate_superseded_by_the_other("native-loss-examine-path", true, false);
    let (runtime, config, b, c) = (&race.runtime, &race.config, &race.b, &race.c);
    let p = config.primary_node_id.clone();
    // C may write the record: it removes B only on evidence.
    race.c_open.store(true, Ordering::SeqCst);
    race.release_b();
    let nothing_started = |when: &str| {
        assert!(
            attempts_started_at(runtime, b, config, config.epoch + 1).is_empty()
                && promise_of(runtime, b, config).is_none(),
            "B starts nothing {when}:\n{}",
            event_log(runtime, &[b, c])
        );
        assert_eq!(
            race.write_set(),
            sorted(vec![b.clone(), c.clone()]),
            "C removes nobody {when}:\n{:?}",
            runtime.write_set_keeping_events(c)
        );
    };
    wait_until("B's promise ends", runtime, &[b, c], || {
        promise_of(runtime, b, config).is_none()
    });
    let asked_by_b = race.asks_again(b, c, 0);
    let asked_by_c = race.asks_again(c, b, 0);
    nothing_started("while the network has examined nothing");
    // Another peer's verdict changes: the snapshot's generation moves.
    runtime.set_peer_verdict(b, &p, PeerPathVerdict::Unknown, false);
    runtime.set_peer_verdict(c, &p, PeerPathVerdict::Unknown, false);
    let asked_by_b = race.asks_again(b, c, asked_by_b);
    let asked_by_c = race.asks_again(c, b, asked_by_c);
    nothing_started("when another peer's verdict changes");
    // The configuration is published again.
    runtime
        .apply_placement_policy(
            c,
            "/unrelated",
            crate::DfsPlacementScope::Subtree,
            crate::DfsPlacementPolicy::new(sinomo_core::DfsObjectPolicy::ReplicatedEventual, 1, 3)
                .unwrap(),
        )
        .unwrap();
    let asked_by_b = race.asks_again(b, c, asked_by_b);
    race.asks_again(c, b, asked_by_c);
    nothing_started("when an unrelated configuration revision is published");
    // An episode that is still running settles nothing.
    runtime.set_peer_verdict(b, c, PeerPathVerdict::NoPath, true);
    runtime.set_peer_verdict(b, &p, PeerPathVerdict::NoPath, false);
    race.asks_again(b, c, asked_by_b);
    nothing_started("while the episode runs");

    // Both episodes settle: the two reach each other.
    runtime.partition_node(b, Partition::default());
    runtime.settle_peer_verification(b, c, PeerPathVerdict::Path);
    runtime.settle_peer_verification(c, b, PeerPathVerdict::Path);
    race.commits_on_both("examined-path-after");
    let record = race.record();
    assert_eq!(
        (record.shard.primary_node_id.clone(), record.shard.epoch),
        (c.clone(), config.epoch + 1),
        "exactly one epoch advance"
    );
    assert!(shard_events(runtime, b, config, DfsMetadataTakeoverEvent::SWAPPED).is_empty());
    assert!(attempts_started_at(runtime, b, config, config.epoch + 1).is_empty());
    race.close();
}

/// TK13 (e): the same wait, and the network confirms the loss: its episode
/// for C settles with no path. B then takes C over, asking nobody.
#[test]
fn native_superseded_candidate_takes_over_a_winner_the_network_confirms_lost() {
    let race = one_candidate_superseded_by_the_other("native-loss-examine-lost", true, false);
    let (runtime, config, b, c) = (&race.runtime, &race.config, &race.b, &race.c);
    race.release_b();
    wait_until("B's promise ends", runtime, &[b, c], || {
        promise_of(runtime, b, config).is_none()
    });
    race.asks_again(b, c, 0);
    assert!(
        attempts_started_at(runtime, b, config, config.epoch + 1).is_empty(),
        "B starts nothing before the network examined C:\n{}",
        event_log(runtime, &[b, c])
    );
    runtime.settle_peer_verification(b, c, PeerPathVerdict::NoPath);
    wait_until("B takes over C", runtime, &[b, c], || {
        !shard_events(runtime, b, config, DfsMetadataTakeoverEvent::SWAPPED).is_empty()
    });
    let record = race.record();
    assert_eq!(
        (record.shard.primary_node_id.clone(), record.shard.epoch),
        (b.clone(), config.epoch + 2)
    );
    assert_eq!(race.write_set(), vec![b.clone()]);
    race.heal();
    race.close();
}

/// A successor B reached when it first read its epoch, and lost afterwards:
/// the verdict turned no-path under the record, so B takes over at once and
/// asks the network nothing.
#[test]
fn native_successor_lost_after_it_was_reached_is_taken_over_without_an_examination() {
    let race = one_candidate_superseded_by_the_other("native-loss-later", true, true);
    let (runtime, config, b, c) = (&race.runtime, &race.config, &race.b, &race.c);
    race.release_b();
    wait_until("no promise is left", runtime, &[b, c], || {
        promise_of(runtime, b, config).is_none() && promise_of(runtime, c, config).is_none()
    });
    assert!(attempts_started_at(runtime, b, config, config.epoch + 1).is_empty());
    let previous_requests = runtime
        .peer_verification_requests(b)
        .into_iter()
        .filter(|peer| peer == c)
        .count();
    runtime.partition_node(
        b,
        Partition {
            kv: false,
            dfs: true,
        },
    );
    runtime.set_peer_verdict(b, c, PeerPathVerdict::NoPath, false);
    wait_until("B takes over C", runtime, &[b, c], || {
        !shard_events(runtime, b, config, DfsMetadataTakeoverEvent::SWAPPED).is_empty()
    });
    assert_eq!(
        runtime
            .peer_verification_requests(b)
            .into_iter()
            .filter(|peer| peer == c)
            .count(),
        previous_requests,
        "the loss after the election requires no second examination"
    );
    let record = race.record();
    assert_eq!(
        (record.shard.primary_node_id.clone(), record.shard.epoch),
        (b.clone(), config.epoch + 2)
    );
    race.heal();
    race.close();
}

/// The elected primary's side of TK13 (d) and (e): C's no-path verdict for B
/// already stood when C became primary. C asks its network to examine B and
/// removes nobody until that episode settles with no path.
#[test]
fn native_elected_primary_has_a_standing_no_path_member_examined_before_removing_it() {
    let race = one_candidate_superseded_by_the_other("native-loss-examine-shrink", true, false);
    let (runtime, config, b, c) = (&race.runtime, &race.config, &race.b, &race.c);
    let p = config.primary_node_id.clone();
    race.c_open.store(true, Ordering::SeqCst);
    let asked = race.asks_again(c, b, 0);
    runtime.set_peer_verdict(c, &p, PeerPathVerdict::Unknown, false);
    race.asks_again(c, b, asked);
    assert_eq!(
        race.write_set(),
        sorted(vec![b.clone(), c.clone()]),
        "C removes nobody before the network examined B: {:?}",
        runtime.write_set_keeping_events(c)
    );
    runtime.settle_peer_verification(c, b, PeerPathVerdict::NoPath);
    wait_until("C removes B", runtime, &[b, c], || {
        race.write_set() == vec![c.clone()]
    });
    race.release_b();
    wait_until("no promise is left", runtime, &[b, c], || {
        promise_of(runtime, b, config).is_none() && promise_of(runtime, c, config).is_none()
    });
    assert!(shard_events(runtime, b, config, DfsMetadataTakeoverEvent::SWAPPED).is_empty());
    let record = race.record();
    assert_eq!(
        (record.shard.primary_node_id.clone(), record.shard.epoch),
        (c.clone(), config.epoch + 1)
    );
    race.heal();
    race.close();
}

/// A keeper that starts with the primary's no-path verdict already standing,
/// as after a restart, first reads the record with it: the network examines
/// the primary once, and the takeover follows the episode's own result.
#[test]
fn native_keeper_that_starts_with_the_primary_unreachable_has_it_examined_first() {
    let (mut runtime, config) = switched_with_kv_voters("native-loss-examine-start", None);
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    {
        let mut service = primary(&runtime);
        directory(&mut service, "/services/start");
    }
    for member in [&b, &c] {
        catch_up(&runtime, &config, member);
    }
    keep_with_paths(&runtime, &config, &[]);
    for node in [&b, &c] {
        runtime.set_probe_owner(node, true);
    }
    runtime.stop_node(&p).unwrap();
    lose(&runtime, &[&b, &c], &p);
    for keeper in [&b, &c] {
        runtime.enable_write_set_keeping(keeper);
    }
    wait_until(
        "both ask the network to verify P",
        &runtime,
        &[&b, &c],
        || {
            [&b, &c]
                .into_iter()
                .all(|node| runtime.peer_verification_requests(node).contains(&p))
        },
    );
    for node in [&b, &c] {
        assert!(
            attempts_started_at(&runtime, node, &config, config.epoch).is_empty()
                && promise_of(&runtime, node, &config).is_none(),
            "{node} starts nothing before the network examined P:\n{}",
            event_log(&runtime, &[&b, &c])
        );
    }
    for node in [&b, &c] {
        runtime.settle_peer_verification(node, &p, PeerPathVerdict::NoPath);
    }
    wait_until("a survivor takes over", &runtime, &[&b, &c], || {
        try_record_from(&runtime, &config, &b)
            .is_some_and(|record| record.shard.primary_node_id != p)
    });
    let record = record_from(&runtime, &config, &b);
    assert_eq!(record.shard.epoch, config.epoch + 1);
    runtime.close().unwrap();
}

/// How often `observer`'s keeper asked the network to verify `peer`.
fn verification_requests(runtime: &DfsTestRuntime, observer: &NodeId, peer: &NodeId) -> usize {
    runtime
        .peer_verification_requests(observer)
        .iter()
        .filter(|asked| *asked == peer)
        .count()
}

/// `observer`'s episode for `peer` ends without a conclusion, as a cancelled
/// or inconclusive one does, or one with nothing to dial. Returns once its
/// keeper has asked the network again: the request it re-sends on its timer.
fn ends_unconcluded_and_is_asked_for_again(
    runtime: &DfsTestRuntime,
    nodes: &[&NodeId],
    observer: &NodeId,
    peer: &NodeId,
) {
    runtime.end_peer_verification_unconcluded(observer, peer);
    // Shorter than the first re-send: the keeper has seen the episode end
    // and asks nothing more until its timer.
    std::thread::sleep(Duration::from_millis(100));
    let asked = verification_requests(runtime, observer, peer);
    wait_until(
        "the keeper asks the network to verify the peer again",
        runtime,
        nodes,
        || verification_requests(runtime, observer, peer) > asked,
    );
}

/// A keeper that starts with the primary's no-path verdict already standing
/// asks the network to examine it. Episodes that end without a conclusion
/// (cancelled, nothing to dial, no verdict from their rounds) certify
/// nothing, however many of them end: nobody starts or promises anything, and
/// each survivor asks again. One episode whose own probe finds no path is
/// then enough, and a survivor takes over.
#[test]
fn native_standing_verdict_is_certified_only_by_an_examination_that_concludes() {
    let (mut runtime, config) = switched_with_kv_voters("native-loss-examine-unconcluded", None);
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    {
        let mut service = primary(&runtime);
        directory(&mut service, "/services/unconcluded");
    }
    for member in [&b, &c] {
        catch_up(&runtime, &config, member);
    }
    keep_with_paths(&runtime, &config, &[]);
    for node in [&b, &c] {
        runtime.set_probe_owner(node, true);
    }
    runtime.stop_node(&p).unwrap();
    lose(&runtime, &[&b, &c], &p);
    for keeper in [&b, &c] {
        runtime.enable_write_set_keeping(keeper);
    }
    wait_until(
        "both ask the network to verify P",
        &runtime,
        &[&b, &c],
        || {
            [&b, &c]
                .into_iter()
                .all(|node| runtime.peer_verification_requests(node).contains(&p))
        },
    );
    let nothing_started = |when: &str| {
        for node in [&b, &c] {
            assert!(
                attempts_started_at(&runtime, node, &config, config.epoch).is_empty()
                    && promise_of(&runtime, node, &config).is_none(),
                "{node} starts nothing {when}:\n{}",
                event_log(&runtime, &[&b, &c])
            );
        }
        let record = record_from(&runtime, &config, &b);
        assert_eq!(
            (record.shard.primary_node_id.clone(), record.shard.epoch),
            (p.clone(), config.epoch),
            "the record is unchanged {when}"
        );
    };
    nothing_started("before the network examined P");
    for node in [&b, &c] {
        ends_unconcluded_and_is_asked_for_again(&runtime, &[&b, &c], node, &p);
    }
    nothing_started("after an episode that concluded nothing");
    // The next episode is seen running before it ends the same way.
    for node in [&b, &c] {
        runtime.set_peer_verdict(node, &p, PeerPathVerdict::NoPath, true);
    }
    for node in [&b, &c] {
        ends_unconcluded_and_is_asked_for_again(&runtime, &[&b, &c], node, &p);
    }
    nothing_started("after a second episode that concluded nothing");

    for node in [&b, &c] {
        runtime.settle_peer_verification(node, &p, PeerPathVerdict::NoPath);
    }
    wait_until("a survivor takes over", &runtime, &[&b, &c], || {
        try_record_from(&runtime, &config, &b)
            .is_some_and(|record| record.shard.primary_node_id != p)
    });
    let record = record_from(&runtime, &config, &b);
    assert_eq!(record.shard.epoch, config.epoch + 1);
    runtime.close().unwrap();
}

/// The elected primary's side: C's no-path verdict for B already stood when
/// C became primary. An episode for B that ends without a conclusion removes
/// nobody, and C asks again; the episode whose probe finds no path does.
#[test]
fn native_elected_primary_keeps_a_member_whose_examination_concludes_nothing() {
    let race = one_candidate_superseded_by_the_other("native-loss-shrink-unconcluded", true, false);
    let (runtime, config, b, c) = (&race.runtime, &race.config, &race.b, &race.c);
    race.c_open.store(true, Ordering::SeqCst);
    race.asks_again(c, b, 0);
    for round in ["first", "second"] {
        ends_unconcluded_and_is_asked_for_again(runtime, &[b, c], c, b);
        assert_eq!(
            race.write_set(),
            sorted(vec![b.clone(), c.clone()]),
            "C removes nobody after the {round} episode that concluded nothing: {:?}",
            runtime.write_set_keeping_events(c)
        );
    }
    runtime.settle_peer_verification(c, b, PeerPathVerdict::NoPath);
    wait_until("C removes B", runtime, &[b, c], || {
        race.write_set() == vec![c.clone()]
    });
    race.release_b();
    wait_until("no promise is left", runtime, &[b, c], || {
        promise_of(runtime, b, config).is_none() && promise_of(runtime, c, config).is_none()
    });
    assert!(shard_events(runtime, b, config, DfsMetadataTakeoverEvent::SWAPPED).is_empty());
    let record = race.record();
    assert_eq!(
        (record.shard.primary_node_id.clone(), record.shard.epoch),
        (c.clone(), config.epoch + 1)
    );
    race.heal();
    race.close();
}

/// A candidate whose own verdict qualifies and the member it ranks first,
/// whose no-path verdict for the primary already stood when it first read the
/// epoch and whose network holds the examination open.
struct HandOff {
    runtime: DfsTestRuntime,
    config: DfsMetadataShardConfig,
    /// The candidate: its verdict turned no-path under the epoch.
    asker: NodeId,
    /// The member ranked first: it is handed the attempt.
    target: NodeId,
}

impl HandOff {
    fn nodes(&self) -> [&NodeId; 2] {
        [&self.asker, &self.target]
    }

    fn events(&self, node: &NodeId, name: &str) -> Vec<DfsMetadataTakeoverEvent> {
        shard_events(&self.runtime, node, &self.config, name)
    }

    fn primary(&self) -> (NodeId, u64) {
        let record = record_from(&self.runtime, &self.config, &self.asker);
        (record.shard.primary_node_id.clone(), record.shard.epoch)
    }

    /// The target holds the hand-off: it promised and swapped nothing, the
    /// asker waits for it, and the record is unchanged.
    fn target_waits(&self, when: &str) {
        let log = || event_log(&self.runtime, &self.nodes());
        assert!(
            promise_of(&self.runtime, &self.target, &self.config).is_none()
                && self
                    .events(&self.target, DfsMetadataTakeoverEvent::PROMISED)
                    .is_empty(),
            "the target promises nothing {when}:\n{}",
            log()
        );
        assert!(
            self.events(&self.asker, DfsMetadataTakeoverEvent::PROMISED)
                .is_empty(),
            "the asker waits for the target {when}:\n{}",
            log()
        );
        assert_eq!(
            self.primary(),
            (self.config.primary_node_id.clone(), self.config.epoch),
            "the record is unchanged {when}"
        );
    }
}

/// The primary stays up, and every member's keeper but its own runs. The
/// target's keeper starts with its no-path verdict for the primary standing,
/// so it asks its network to examine the primary and that episode stays
/// open. The asker reaches the primary when it first reads the epoch and
/// loses it afterwards, so its own verdict qualifies: it asks the target,
/// which answers that it lost the primary too, ranks it first and hands the
/// attempt to it.
fn handed_to_a_member_with_a_standing_verdict(name: &str) -> HandOff {
    let (runtime, config) = switched_with_kv_voters(name, None);
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    {
        let mut service = primary(&runtime);
        directory(&mut service, "/services/hand-off");
    }
    for member in [&b, &c] {
        catch_up(&runtime, &config, member);
    }
    // The member ranked first by journal head, then node id, is asked.
    let mut ranking = [&b, &c].map(|node| (journal_head(&runtime, node, &config), node.clone()));
    ranking.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));
    let (target, asker) = (ranking[0].1.clone(), ranking[1].1.clone());

    keep_with_paths(&runtime, &config, &[]);
    runtime.set_probe_owner(&target, true);
    lose(&runtime, &[&target], &p);
    runtime.enable_write_set_keeping(&target);
    wait_until(
        "the target asks its network to examine the primary",
        &runtime,
        &[&asker, &target],
        || runtime.peer_verification_requests(&target).contains(&p),
    );
    assert!(
        attempts_started_at(&runtime, &target, &config, config.epoch).is_empty(),
        "the target starts nothing on a verdict that only stood"
    );

    runtime.enable_write_set_keeping(&asker);
    lose(&runtime, &[&asker], &p);
    let hand_off = HandOff {
        runtime,
        config,
        asker,
        target,
    };
    wait_until(
        "the asker hands the attempt to the target",
        &hand_off.runtime,
        &hand_off.nodes(),
        || {
            hand_off
                .events(
                    &hand_off.asker,
                    DfsMetadataTakeoverEvent::CORROBORATION_HANDED_OFF,
                )
                .iter()
                .any(|event| event.node_ids == vec![hand_off.target.clone()])
        },
    );
    hand_off
}

/// A hand-off target swaps on its own verdict, so that verdict must count
/// under the epoch as for an attempt of its own. With its examination of the
/// primary open the target holds the hand-off: it promises nothing however
/// often its keeper evaluates. Once the episode's own probe finds no path it
/// runs the attempt and its swap applies.
#[test]
fn native_hand_off_target_waits_for_the_network_to_examine_the_primary() {
    let hand_off = handed_to_a_member_with_a_standing_verdict("native-loss-hand-off-waits");
    let (runtime, config) = (&hand_off.runtime, &hand_off.config);
    let (asker, target) = (&hand_off.asker, &hand_off.target);
    let p = config.primary_node_id.clone();
    std::thread::sleep(Duration::from_millis(500));
    hand_off.target_waits("while its network has examined nothing");
    // The target's keeper evaluates again: another peer's entry moves.
    let asked = verification_requests(runtime, target, &p);
    runtime.set_peer_verdict(target, asker, PeerPathVerdict::Path, false);
    wait_until(
        "the target's keeper evaluates again",
        runtime,
        &hand_off.nodes(),
        || verification_requests(runtime, target, &p) > asked,
    );
    hand_off.target_waits("when its keeper evaluates again");

    runtime.settle_peer_verification(target, &p, PeerPathVerdict::NoPath);
    wait_until(
        "the target's swap applies",
        runtime,
        &hand_off.nodes(),
        || {
            !hand_off
                .events(target, DfsMetadataTakeoverEvent::SWAPPED)
                .is_empty()
        },
    );
    assert_eq!(hand_off.primary(), (target.clone(), config.epoch + 1));
    assert!(
        hand_off
            .events(asker, DfsMetadataTakeoverEvent::SWAPPED)
            .is_empty(),
        "the asker follows the target"
    );
    hand_off.runtime.close().unwrap();
}

/// The target's examination of the primary ends without a conclusion: it
/// declines the hand-off and promises nothing. The asker, whose own verdict
/// qualifies, ranks again without it and takes over itself, so the takeover
/// does not wait on a member that cannot examine the primary.
#[test]
fn native_hand_off_target_declines_when_its_examination_concludes_nothing() {
    let hand_off = handed_to_a_member_with_a_standing_verdict("native-loss-hand-off-declines");
    let (runtime, config) = (&hand_off.runtime, &hand_off.config);
    let (asker, target) = (&hand_off.asker, &hand_off.target);
    let p = config.primary_node_id.clone();
    hand_off.target_waits("while its network has examined nothing");

    runtime.end_peer_verification_unconcluded(target, &p);
    wait_until(
        "the asker's own swap applies",
        runtime,
        &hand_off.nodes(),
        || {
            !hand_off
                .events(asker, DfsMetadataTakeoverEvent::SWAPPED)
                .is_empty()
        },
    );
    assert_eq!(hand_off.primary(), (asker.clone(), config.epoch + 1));
    assert!(
        hand_off
            .events(target, DfsMetadataTakeoverEvent::PROMISED)
            .is_empty()
            && hand_off
                .events(target, DfsMetadataTakeoverEvent::SWAPPED)
                .is_empty(),
        "the target promised and swapped nothing:\n{}",
        event_log(runtime, &hand_off.nodes())
    );
    assert!(
        runtime
            .write_set_takeover_trace(asker)
            .iter()
            .any(|line| line.contains("primary-unexamined")),
        "the target declined for its unexamined verdict: {:?}",
        runtime.write_set_takeover_trace(asker)
    );
    hand_off.runtime.close().unwrap();
}

/// The target's examination of the primary finds a path: the target reaches
/// the primary, so its decline vetoes the asker's attempt, as a `reaches`
/// answer in the round does. The asker does not rank again without it, so
/// nobody promises or swaps and the record is unchanged. The veto ends that
/// attempt only: once the primary is really lost, the next verdict change
/// starts a new one and the takeover proceeds.
#[test]
fn native_hand_off_target_that_finds_a_path_vetoes_until_the_primary_is_really_lost() {
    let mut hand_off = handed_to_a_member_with_a_standing_verdict("native-loss-hand-off-vetoes");
    let config = hand_off.config.clone();
    let (asker, target) = (hand_off.asker.clone(), hand_off.target.clone());
    let p = config.primary_node_id.clone();
    hand_off.target_waits("while its network has examined nothing");

    hand_off
        .runtime
        .settle_peer_verification(&target, &p, PeerPathVerdict::Path);
    wait_until(
        "the target vetoes the asker's attempt",
        &hand_off.runtime,
        &hand_off.nodes(),
        || {
            hand_off
                .events(&asker, DfsMetadataTakeoverEvent::CORROBORATION_VETOED)
                .iter()
                .any(|event| event.node_ids == vec![target.clone()])
        },
    );
    assert!(
        hand_off
            .runtime
            .write_set_takeover_trace(&asker)
            .iter()
            .any(|line| line.contains("primary-reached")),
        "the target declined as reaching the primary: {:?}",
        hand_off.runtime.write_set_takeover_trace(&asker)
    );
    // Nothing follows the veto while nothing changes.
    std::thread::sleep(Duration::from_millis(1500));
    for node in hand_off.nodes() {
        for name in [
            DfsMetadataTakeoverEvent::PROMISED,
            DfsMetadataTakeoverEvent::SWAPPED,
        ] {
            assert!(
                hand_off.events(node, name).is_empty(),
                "{node}: no {name} after the veto:\n{}",
                event_log(&hand_off.runtime, &hand_off.nodes())
            );
        }
        assert!(promise_of(&hand_off.runtime, node, &config).is_none());
    }
    assert_eq!(
        hand_off.primary(),
        (p.clone(), config.epoch),
        "the record is unchanged"
    );

    // The primary is really lost: it stops, and both members' networks
    // examine it and find no path. For the target that is a path lost under
    // the epoch; for the asker it is the verdict change a new attempt needs.
    hand_off.runtime.stop_node(&p).unwrap();
    for node in [&target, &asker] {
        hand_off
            .runtime
            .settle_peer_verification(node, &p, PeerPathVerdict::NoPath);
    }
    wait_until(
        "a survivor takes over",
        &hand_off.runtime,
        &hand_off.nodes(),
        || {
            try_record_from(&hand_off.runtime, &config, &asker)
                .is_some_and(|record| record.shard.primary_node_id != p)
        },
    );
    let (primary, epoch) = hand_off.primary();
    assert!(primary == asker || primary == target);
    assert_eq!(epoch, config.epoch + 1);
    hand_off.runtime.close().unwrap();
}

/// A hand-off to a member whose network cannot examine the primary at all
/// (it has no probe owner) is declined at once, and one whose examination
/// finds a path is declined as reaching the primary, which vetoes. Neither
/// promises anything.
#[test]
fn native_hand_off_target_without_an_examination_or_with_a_path_declines() {
    use crate::takeover_corroboration::{DfsTakeoverCorroborationKind, DfsTakeoverHandOffOutcome};
    let (runtime, config) = switched_with_kv_voters("native-loss-hand-off-refused", None);
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    let mut ranking = [&b, &c]
        .map(|node| crate::takeover_corroboration::DfsTakeoverRank {
            node_id: node.clone(),
            journal_sequence: journal_head(&runtime, node, &config),
            mobile: false,
        })
        .to_vec();
    ranking.sort_by(|left, right| {
        right
            .journal_sequence
            .cmp(&left.journal_sequence)
            .then_with(|| left.node_id.cmp(&right.node_id))
    });
    let target = ranking[0].node_id.clone();
    let asker = ranking[1].node_id.clone();
    keep_with_paths(&runtime, &config, &[]);
    // The target's network accepts no request: nothing can examine P.
    runtime.set_probe_owner(&target, false);
    lose(&runtime, &[&target], &p);
    runtime.enable_write_set_keeping(&target);
    wait_until(
        "the target asks its network to examine the primary",
        &runtime,
        &[&asker, &target],
        || runtime.peer_verification_requests(&target).contains(&p),
    );
    let record = record_from(&runtime, &config, &asker);
    let hand_off = || {
        let mut request =
            super::takeover_native_tests::corroboration_request(&config, &record, &asker);
        request.kind = DfsTakeoverCorroborationKind::HandOff {
            ranking: ranking.clone(),
        };
        super::takeover_native_tests::ask(&runtime, &asker, &target, &request)
    };
    let declined_for = |hand_off: Option<DfsTakeoverHandOffOutcome>| match hand_off {
        Some(DfsTakeoverHandOffOutcome::Declined { reason, .. }) => reason,
        other => panic!("the target declines: {other:?}"),
    };
    assert_eq!(declined_for(hand_off().hand_off), "primary-unexamined");

    // Its network returns and examines P: the episode's probe finds a path.
    runtime.set_probe_owner(&target, true);
    let data_dir = runtime.data_dir(&asker).clone();
    let handle = runtime.runtime_handle();
    let mut request = super::takeover_native_tests::corroboration_request(&config, &record, &asker);
    request.kind = DfsTakeoverCorroborationKind::HandOff {
        ranking: ranking.clone(),
    };
    let reply = std::thread::scope(|scope| {
        let asked = scope.spawn(|| {
            crate::runtime_host::ask_dfs_takeover_corroboration_for_test(
                &data_dir, &handle, &target, &request,
            )
            .expect("the target answers")
        });
        std::thread::sleep(Duration::from_millis(500));
        assert!(
            !asked.is_finished(),
            "the hand-off waits for the open examination"
        );
        runtime.settle_peer_verification(&target, &p, PeerPathVerdict::Path);
        asked.join().unwrap()
    });
    assert!(reply.reaches_primary());
    assert_eq!(declined_for(reply.hand_off), "primary-reached");
    assert!(
        promise_of(&runtime, &target, &config).is_none()
            && shard_events(
                &runtime,
                &target,
                &config,
                DfsMetadataTakeoverEvent::PROMISED
            )
            .is_empty(),
        "the target promised nothing:\n{}",
        event_log(&runtime, &[&asker, &target])
    );
    let record = record_from(&runtime, &config, &asker);
    assert_eq!(
        (record.shard.primary_node_id, record.shard.epoch),
        (p, config.epoch)
    );
    runtime.close().unwrap();
}

/// A two-member write set {P, B} with P alive and keeping: C leaves the
/// write set first, and B, which alone judges P lost, asks nobody.
///
/// P's keeper sees no path to C, so the write set stays {P, B} by the
/// keeper's own inputs: a member it cannot reach never rejoins and ranks
/// below B, so a shrunk B is the one re-added. N stays 3; N=2 alone would not
/// hold it, because once B is shrunk B and C tie and a per-shard hash picks
/// which one P re-adds.
fn two_member_write_set(name: &str) -> (DfsTestRuntime, DfsMetadataShardConfig) {
    let (runtime, config) = switched_with_kv_voters(name, Some((1, 3)));
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    {
        let mut service = primary(&runtime);
        directory(&mut service, &format!("/services/{name}"));
    }
    catch_up(&runtime, &config, &b);
    shrink(&runtime, &config, &c, 1);
    // Every verdict is set before either keeper starts.
    keep_with_paths(&runtime, &config, &[]);
    runtime.set_peer_verdict(&p, &c, PeerPathVerdict::NoPath, false);
    for keeper in [&p, &b] {
        runtime.enable_write_set_keeping(keeper);
    }
    (runtime, config)
}

/// Once B's attempt has promised with its swap held, P, alive, writes: B
/// refuses P's epoch and P's keeper shrinks it, leaving the epoch unchanged.
/// P then judges B unreachable, so its keeper cannot re-add B before
/// `released_and_re_added` lets it.
fn shrunk_while_promised(runtime: &DfsTestRuntime, config: &DfsMetadataShardConfig, name: &str) {
    let p = &config.primary_node_id;
    let b = &config.replica_node_ids[0];
    let fenced = try_mode_at(
        runtime,
        config,
        p,
        &format!("/services/{name}"),
        0o640,
        &format!("{name}-fenced"),
        None,
    );
    assert!(
        fenced.is_err(),
        "a promised member cannot acknowledge: {fenced:?}"
    );
    // The keeper reports a shrink after it commits, so the committed write set
    // can show it first: wait for both, then check why B was shrunk.
    let shrunk = || {
        runtime
            .write_set_keeping_events(p)
            .into_iter()
            .filter(|(name, event)| {
                name == crate::DfsMetadataWriteSetEvent::SHRUNK
                    && event.shard_id == config.shard_id
                    && event.node_ids.contains(b)
            })
            .map(|(_, event)| event)
            .collect::<Vec<_>>()
    };
    wait_until(
        "P's keeper shrinks B and reports it",
        runtime,
        &[p, b],
        || !write_set_of(runtime, config).contains(b) && !shrunk().is_empty(),
    );
    let shrunk = shrunk();
    assert!(
        shrunk.iter().all(|event| event.cause == "member-fenced"),
        "{shrunk:?}"
    );
    assert_eq!(record_from(runtime, config, p).shard.epoch, config.epoch);
    // B refuses every join while it holds its promise, so no join of B can
    // have succeeded before this verdict.
    runtime.set_peer_verdict(p, b, PeerPathVerdict::NoPath, false);
}

/// B's swap settles rejected with the epoch unchanged, its promise is
/// released and its fence is back at the epoch it promised at; P's next
/// write catches it up and P's keeper re-adds it.
///
/// P's keeper sees no path to B until B judges P reachable again: re-added
/// into the two-member write set while it still judged P lost, B would take
/// over again, asking nobody.
fn released_and_re_added(runtime: &DfsTestRuntime, config: &DfsMetadataShardConfig, name: &str) {
    let p = &config.primary_node_id;
    let b = &config.replica_node_ids[0];
    wait_until("B releases its promise", runtime, &[p, b], || {
        !shard_events(
            runtime,
            b,
            config,
            DfsMetadataTakeoverEvent::PROMISE_RELEASED,
        )
        .is_empty()
    });
    assert!(
        promise_of(runtime, b, config).is_none(),
        "{:?}\n{}\n{:?}",
        member_state(runtime, b, config),
        event_log(runtime, &[p, b]),
        shard_events(
            runtime,
            b,
            config,
            DfsMetadataTakeoverEvent::PROMISE_RELEASED
        )
    );
    assert!(
        member_state(runtime, b, config)
            .is_none_or(|state| state.accept_from_epoch <= config.epoch),
        "B's fence is back at the epoch: {:?}",
        member_state(runtime, b, config)
    );
    assert!(
        shard_events(runtime, b, config, DfsMetadataTakeoverEvent::SWAPPED).is_empty(),
        "B never swaps:\n{}",
        event_log(runtime, &[p, b])
    );
    runtime.set_peer_verdict(b, p, PeerPathVerdict::Path, false);
    runtime.set_peer_verdict(p, b, PeerPathVerdict::Path, false);
    // P's writes bring B back. The fenced change may still commit by the
    // recovery the shrink requests, so each write retries under a fresh
    // lookup; and a join that fails waits for the next change, so the
    // write is repeated until B is re-added.
    let deadline = Instant::now() + Duration::from_secs(90);
    for round in 0.. {
        commit_mode_eventually(
            runtime,
            config,
            p,
            &format!("/services/{name}"),
            0o600,
            &format!("{name}-after-{round}"),
        );
        let settle = Instant::now() + Duration::from_secs(10);
        while Instant::now() < settle && !write_set_of(runtime, config).contains(b) {
            std::thread::sleep(Duration::from_millis(100));
        }
        if write_set_of(runtime, config).contains(b) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "P's keeper re-adds B:\n{}\nkeeping {:?}",
            event_log(runtime, &[p, b]),
            runtime.write_set_keeping_events(p)
        );
    }
    let record = record_from(runtime, config, p);
    assert_eq!(
        (record.shard.primary_node_id.clone(), record.shard.epoch),
        (p.clone(), config.epoch)
    );
}

/// TK14: B promises while its swap is held; P, alive, shrinks B; B's swap is
/// then rejected with the epoch unchanged, B releases, catches up and is
/// re-added.
#[test]
fn native_takeover_shrink_race_releases_the_promise() {
    let name = "native-loss-shrink-race";
    let (runtime, config) = two_member_write_set(name);
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let held = hold_swaps(&runtime, &config, &b);
    lose(&runtime, &[&b], &p);
    wait_until("B promises", &runtime, &[&p, &b], || {
        promise_of(&runtime, &b, &config).is_some()
    });
    shrunk_while_promised(&runtime, &config, name);
    drop(held);
    released_and_re_added(&runtime, &config, name);
    runtime.close().unwrap();
}

/// TK20: B promises, and its swap is stuck unknown because a partition cuts
/// B off from cluster KV while P still reaches it over DFS. B answers P's
/// epoch with its fence and P's keeper shrinks it at the floor of one. Once
/// B's KV returns, its re-sent swap settles rejected with the epoch
/// unchanged; B releases and is re-added.
#[test]
fn native_takeover_stuck_swap_of_a_kv_isolated_candidate_releases() {
    let name = "native-loss-stuck-swap";
    let (runtime, config) = two_member_write_set(name);
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let held = hold_swaps(&runtime, &config, &b);
    lose(&runtime, &[&b], &p);
    wait_until("B promises", &runtime, &[&p, &b], || {
        promise_of(&runtime, &b, &config).is_some()
    });
    runtime.partition_node(
        &b,
        Partition {
            kv: true,
            dfs: false,
        },
    );
    drop(held);
    shrunk_while_promised(&runtime, &config, name);
    assert!(
        promise_of(&runtime, &b, &config).is_some(),
        "the cut-off candidate's outcome stays unknown"
    );
    runtime.partition_node(&b, Partition::default());
    released_and_re_added(&runtime, &config, name);
    // The returned voter's higher term makes the voters elect again; let
    // that settle before the nodes stop, so no KV call outlives the close.
    for node in [&p, &b] {
        reelect(&runtime, node);
    }
    runtime.close().unwrap();
}

/// TK15: the reply of the candidate's committed swap is lost. Its re-send of
/// the identical request returns the committed outcome: exactly one epoch
/// advance, one swapped event, and the record's last change is the promise.
#[test]
fn native_takeover_lost_swap_reply_settles_from_its_own_outcome() {
    let (mut runtime, config) = switched_with_kv_voters("native-loss-lost-reply", None);
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    keep_with_paths(&runtime, &config, &[&b, &c]);
    let lost = Arc::new(AtomicUsize::new(0));
    let hooks = [&b, &c].map(|node| {
        let (config, lost) = (config.clone(), Arc::clone(&lost));
        runtime
            .native_kv_reply_hook(node, move |request| {
                if writes_the_write_set(&config, request)
                    && lost.fetch_add(1, Ordering::AcqRel) == 0
                {
                    Err(ServiceError::Network(NetworkError::delivery_failed(
                        "native fixture lost swap reply",
                        NetworkDeliveryState::PossiblyApplied,
                        "the committed reply was lost",
                    )))
                } else {
                    Ok(())
                }
            })
            .unwrap()
    });
    runtime.stop_node(&p).unwrap();
    lose(&runtime, &[&b, &c], &p);
    wait_until("a member takes over", &runtime, &[&b, &c], || {
        record_from(&runtime, &config, &b).shard.primary_node_id != p
    });
    let winner = record_from(&runtime, &config, &b).shard.primary_node_id;
    wait_until("the winner reports its swap", &runtime, &[&b, &c], || {
        !shard_events(
            &runtime,
            &winner,
            &config,
            DfsMetadataTakeoverEvent::SWAPPED,
        )
        .is_empty()
    });
    assert!(
        lost.load(Ordering::Acquire) >= 2,
        "the reply was lost and the swap re-sent"
    );
    drop(hooks);
    let swapped = shard_events(
        &runtime,
        &winner,
        &config,
        DfsMetadataTakeoverEvent::SWAPPED,
    );
    assert_eq!(swapped.len(), 1, "{}", event_log(&runtime, &[&b, &c]));
    let other = the_other(&winner, &b, &c);
    assert!(shard_events(&runtime, &other, &config, DfsMetadataTakeoverEvent::SWAPPED).is_empty());
    let record = record_from(&runtime, &config, &b);
    assert_eq!(
        record.shard.epoch,
        config.epoch + 1,
        "exactly one epoch advance"
    );
    let last = record
        .write_set_state()
        .switched()
        .unwrap()
        .last_change
        .clone();
    if matches!(
        last.kind,
        crate::DfsMetadataShardWriteSetChangeKind::Takeover { .. }
    ) {
        assert_eq!(Some(last.change_id), swapped[0].swap_request_id);
    }
    wait_until("the winner's promise ends", &runtime, &[&b, &c], || {
        promise_of(&runtime, &winner, &config).is_none()
    });
    runtime.close().unwrap();
}

/// TK17: the members are on the minority side of a cluster-KV partition. One
/// member judges the live primary lost and the other, which still reaches it,
/// vetoes. Then the other judges it lost too and is asked to take over as the
/// first-ranked member: it declines, because it cannot confirm the record
/// without a KV quorum, or its keeper cannot evaluate the shard at all.
/// Neither promises, and the record is unchanged once KV returns.
#[test]
fn native_takeover_on_the_kv_minority_side_promises_nothing() {
    let (runtime, config) = switched_with_kv_voters("native-loss-minority", None);
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    // The member ranked first by journal head, then node id, is asked.
    let mut ranking = [&b, &c]
        .map(|node| crate::takeover_corroboration::DfsTakeoverRank {
            node_id: node.clone(),
            journal_sequence: journal_head(&runtime, node, &config),
            mobile: false,
        })
        .to_vec();
    ranking.sort_by(|left, right| {
        right
            .journal_sequence
            .cmp(&left.journal_sequence)
            .then_with(|| left.node_id.cmp(&right.node_id))
    });
    let first = ranking[0].node_id.clone();
    let asker = ranking[1].node_id.clone();
    keep_with_paths(&runtime, &config, &[&b, &c]);
    lose(&runtime, &[&asker], &p);
    wait_until("the other member vetoes", &runtime, &[&b, &c], || {
        shard_events(
            &runtime,
            &asker,
            &config,
            DfsMetadataTakeoverEvent::CORROBORATION_VETOED,
        )
        .iter()
        .any(|event| event.node_ids == vec![first.clone()])
    });
    let record = record_from(&runtime, &config, &asker);

    for member in [&b, &c] {
        runtime.partition_node(
            member,
            Partition {
                kv: true,
                dfs: false,
            },
        );
    }
    lose(&runtime, &[&first], &p);
    let mut request = super::takeover_native_tests::corroboration_request(&config, &record, &asker);
    request.kind = crate::takeover_corroboration::DfsTakeoverCorroborationKind::HandOff { ranking };
    let reply = super::takeover_native_tests::ask(&runtime, &asker, &first, &request);
    match reply.hand_off {
        Some(crate::takeover_corroboration::DfsTakeoverHandOffOutcome::Declined {
            reason, ..
        }) => {
            assert!(reason == "no-quorum" || reason == "unavailable", "{reason}")
        }
        other => panic!("the first-ranked member cannot take over on the minority side: {other:?}"),
    }
    for node in [&b, &c] {
        assert!(
            promise_of(&runtime, node, &config).is_none(),
            "{node} promised nothing:\n{}",
            event_log(&runtime, &[&b, &c])
        );
        assert!(
            shard_events(&runtime, node, &config, DfsMetadataTakeoverEvent::PROMISED).is_empty()
        );
    }
    // The members reach P again before KV returns, so nothing follows.
    for member in [&b, &c] {
        runtime.set_peer_verdict(member, &p, PeerPathVerdict::Path, false);
    }
    for member in [&b, &c] {
        runtime.partition_node(member, Partition::default());
    }
    let record = record_from(&runtime, &config, &b);
    assert_eq!(
        (record.shard.primary_node_id, record.shard.epoch),
        (p, config.epoch)
    );
    for node in [&b, &c] {
        reelect(&runtime, node);
    }
    runtime.close().unwrap();
}

/// TK16 with the candidate shrunk while it is down: B stops between its
/// durable promise and its swap, and P, alive, shrinks it out of the write
/// set meanwhile. Restarted outside the write set, B still settles its
/// promise by re-sending the identical swap, which is rejected with the
/// epoch unchanged; B releases the promise and is re-added.
#[test]
fn native_takeover_promise_of_a_candidate_shrunk_while_down_is_released() {
    let name = "native-loss-shrunk-down";
    let (mut runtime, config) = two_member_write_set(name);
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    runtime.stop_takeover_after_promise(&b, true);
    lose(&runtime, &[&b], &p);
    wait_until("B promises", &runtime, &[&p, &b], || {
        promise_of(&runtime, &b, &config).is_some()
    });
    runtime.stop_node(&b).unwrap();
    runtime.stop_takeover_after_promise(&b, false);
    runtime.set_peer_verdict(&p, &b, PeerPathVerdict::NoPath, false);
    wait_until("P's keeper shrinks B", &runtime, &[&p], || {
        !write_set_of(&runtime, &config).contains(&b)
    });
    runtime.reopen_node(b.clone()).unwrap();
    released_and_re_added(&runtime, &config, name);
    runtime.close().unwrap();
}

/// TK7: a member that is offline across two takeovers returns with a
/// prefix that diverges below the current epoch start, and is repaired by
/// snapshot. C acknowledged X, which the old primary never committed and the
/// first successor B never received; B commits Y in X's place. P, re-added
/// by B, takes over from B in turn and commits Z. When C returns, its journal
/// differs from the current history where it ends, so the new primary
/// installs a snapshot on it before re-adding it: Y and Z are intact on C
/// and X is not committed there.
#[test]
fn native_takeover_member_with_a_divergent_prefix_is_repaired_by_snapshot() {
    let (mut runtime, config) = switched_with_kv_voters("native-loss-divergent", Some((1, 3)));
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    let current = {
        let mut service = primary(&runtime);
        directory(&mut service, "/services/divergent")
    };
    for member in [&b, &c] {
        catch_up(&runtime, &config, member);
    }
    keep_with_paths(&runtime, &config, &[&p, &b]);
    // X is prepared on P and C while B is cut off from DFS.
    runtime.partition_node(
        &b,
        Partition {
            kv: false,
            dfs: true,
        },
    );
    prepare_unknown(&runtime, &config, &current, "divergent-x");
    // C's acknowledgement of X can land after P's attempt ended.
    wait_until("C holds X past B's head", &runtime, &[&b, &c], || {
        journal_head(&runtime, &c, &config) > journal_head(&runtime, &b, &config)
    });

    // C goes offline; P's keeper shrinks it.
    runtime.stop_node(&c).unwrap();
    for observer in [&p, &b] {
        runtime.set_peer_verdict(observer, &c, PeerPathVerdict::NoPath, false);
    }
    wait_until("P's keeper shrinks C", &runtime, &[&p, &b], || {
        !write_set_of(&runtime, &config).contains(&c)
    });

    // First takeover: P is cut off from DFS before B reaches anyone again,
    // so B never receives X, and B takes over without it.
    runtime.partition_node(
        &p,
        Partition {
            kv: false,
            dfs: true,
        },
    );
    runtime.partition_node(&b, Partition::default());
    lose(&runtime, &[&b], &p);
    wait_until("B takes over", &runtime, &[&b], || {
        record_from(&runtime, &config, &b).shard.primary_node_id == b
    });
    let first = record_from(&runtime, &config, &b);
    assert_eq!(first.shard.epoch, config.epoch + 1);
    let first_start = first
        .write_set_state()
        .switched()
        .and_then(|write_set| write_set.epoch_start.clone())
        .expect("the takeover epoch names its start");
    assert!(
        first_start.journal_sequence < journal_head(&runtime, &c, &config),
        "B's epoch starts below X"
    );
    wait_until("B activates", &runtime, &[&b], || {
        !shard_events(&runtime, &b, &config, DfsMetadataTakeoverEvent::ACTIVATED).is_empty()
    });
    commit_mode_eventually(
        &runtime,
        &first.shard,
        &b,
        "/services/divergent",
        0o600,
        "divergent-y",
    );

    // B reaches P again and re-adds it; P drops X on alignment.
    runtime.partition_node(&p, Partition::default());
    runtime.set_peer_verdict(&b, &p, PeerPathVerdict::Path, false);
    wait_until("B re-adds P", &runtime, &[&b], || {
        write_set_of(&runtime, &config).contains(&p)
    });

    // Second takeover: B is cut off from DFS, and P takes over.
    runtime.partition_node(
        &b,
        Partition {
            kv: false,
            dfs: true,
        },
    );
    lose(&runtime, &[&p], &b);
    wait_until("P takes over", &runtime, &[&p], || {
        record_from(&runtime, &config, &p).shard.primary_node_id == p
    });
    let second = record_from(&runtime, &config, &p);
    assert_eq!(second.shard.epoch, config.epoch + 2);
    wait_until("P activates", &runtime, &[&p], || {
        shard_events(&runtime, &p, &config, DfsMetadataTakeoverEvent::ACTIVATED)
            .iter()
            .any(|event| event.epoch == config.epoch + 1)
    });
    commit_mode_eventually(
        &runtime,
        &second.shard,
        &p,
        "/services/divergent",
        0o644,
        "divergent-z",
    );
    let start = second
        .write_set_state()
        .switched()
        .and_then(|write_set| write_set.epoch_start.clone())
        .expect("the takeover epoch names its start");
    let behind = super::commit_native_tests::durable_prefix(&runtime, &c, &config)
        .expect("C holds a journal");
    assert!(
        behind.journal_sequence < start.journal_sequence,
        "C's journal ends below the epoch start: {behind:?} {start:?}"
    );

    // C returns: P's keeper repairs it by snapshot and re-adds it.
    let archived = store_archives(&runtime, &c, &config);
    runtime.reopen_node(c.clone()).unwrap();
    runtime.set_peer_verdict(&p, &c, PeerPathVerdict::Path, false);
    wait_until("P re-adds C", &runtime, &[&p], || {
        write_set_of(&runtime, &config).contains(&c)
    });
    assert!(
        store_archives(&runtime, &c, &config)
            .difference(&archived)
            .next()
            .is_some(),
        "a snapshot install archived C's divergent store"
    );
    catch_up_from(
        &runtime,
        &record_from(&runtime, &config, &p).shard,
        &p,
        &[&c],
    );
    assert_eq!(
        object_mode(&runtime, &c, &config, &current.object_id),
        Some(0o644),
        "the committed changes are intact on C"
    );
    assert_eq!(
        super::commit_native_tests::durable_prefix(&runtime, &c, &config),
        super::commit_native_tests::durable_prefix(&runtime, &p, &config),
        "C holds the new primary's history"
    );
    assert_ne!(
        operation_status(&runtime, &c, &config, "divergent-x"),
        Some(crate::DfsPreparedOperationStatus::Committed)
    );
    runtime.close().unwrap();
}

/// What `node` holds for a recovered change, in a form two nodes can be
/// compared by: its durable head, the settled status of the original and of
/// its recovery body, and digests of the object and of the whole shard table
/// state.
#[derive(Debug, PartialEq, Eq)]
struct SettledView {
    /// The durable head: its digest chains every record, so equal heads are
    /// one history even where a snapshot install replaced a member's
    /// retained journal.
    head: Option<crate::DfsMetadataPrefixProof>,
    original: Option<crate::DfsPreparedOperationStatus>,
    recovery: Option<crate::DfsPreparedOperationStatus>,
    object: Option<crate::DfsObjectMetadata>,
    tables: String,
}

fn settled_view(
    runtime: &DfsTestRuntime,
    node: &NodeId,
    config: &DfsMetadataShardConfig,
    original: &str,
    recovery: &str,
    object: &crate::DfsObjectId,
) -> SettledView {
    use crate::DfsMetadataBackend as _;
    let backend = crate::LocalSobjMetadataBackend::open_for_inspection(
        shard_id_of(config),
        shard_root(runtime, node, config),
    )
    .unwrap();
    let status = |operation: &str| {
        backend
            .prepared_operation_record(operation)
            .ok()
            .flatten()
            .map(|record| record.status)
    };
    let digest = |text: String| blake3::hash(text.as_bytes()).to_hex()[..16].to_string();
    SettledView {
        head: backend.metadata_prefix_proof().ok(),
        original: status(original),
        recovery: status(recovery),
        object: backend.get_object(object).unwrap(),
        tables: digest(format!(
            "{:?}",
            backend
                .export_shard_table_snapshot(&shard_id_of(config))
                .unwrap()
        )),
    }
}

/// Every node in `nodes` comes to hold the same history, statuses and state:
/// a follower materializes what it acknowledged a moment later, so the views
/// are compared until they agree.
fn assert_one_history(
    runtime: &DfsTestRuntime,
    config: &DfsMetadataShardConfig,
    nodes: &[&NodeId],
    original: &str,
    recovery: &str,
    object: &crate::DfsObjectId,
) -> SettledView {
    let deadline = Instant::now() + Duration::from_secs(90);
    let views = loop {
        let views = nodes
            .iter()
            .map(|node| settled_view(runtime, node, config, original, recovery, object))
            .collect::<Vec<_>>();
        if views.iter().all(|view| view == &views[0]) {
            break views;
        }
        assert!(
            Instant::now() < deadline,
            "the nodes do not converge: {:#?}",
            nodes.iter().zip(&views).collect::<Vec<_>>()
        );
        std::thread::sleep(Duration::from_millis(100));
    };
    assert_eq!(
        views[0].original,
        Some(crate::DfsPreparedOperationStatus::Superseded),
        "the original is superseded by its recovery"
    );
    assert_eq!(
        views[0].recovery,
        Some(crate::DfsPreparedOperationStatus::Committed)
    );
    views.into_iter().next().unwrap()
}

/// A change every member stored but whose outcome is unknown when its
/// primary stops is recovered by the successor as `recover:<op>:<epoch>`:
/// the same body, a new operation id and owner epoch, and an effect that
/// marks the original superseded. A member cut off during the recovery and
/// the old primary both catch up to that same history: every node holds the
/// same journal, statuses, object and table state, also after a restart.
#[test]
fn native_takeover_recovered_change_reaches_one_history_everywhere() {
    let (mut runtime, config) = switched_with_kv_voters("native-loss-recovered", Some((1, 3)));
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    let current = {
        let mut service = primary(&runtime);
        directory(&mut service, "/services/recovered")
    };
    for member in [&b, &c] {
        catch_up(&runtime, &config, member);
    }
    // Both members store X, but P never hears their acknowledgements.
    let gates =
        [&b, &c].map(|member| super::commit_native_tests::hold_replies(&runtime, &config, member));
    prepare_unknown(&runtime, &config, &current, "recovered-x");
    wait_until("both members hold X", &runtime, &[&b, &c], || {
        [&b, &c]
            .into_iter()
            .all(|member| operation_status(&runtime, member, &config, "recovered-x").is_some())
    });
    keep_with_paths(&runtime, &config, &[&b, &c]);
    runtime.stop_node(&p).unwrap();
    for gate in &gates {
        super::commit_native_tests::release(gate);
    }
    runtime.partition_node(
        &c,
        Partition {
            kv: false,
            dfs: true,
        },
    );
    runtime.set_peer_verdict(&b, &c, PeerPathVerdict::NoPath, false);
    lose(&runtime, &[&b], &p);
    wait_until("B takes over", &runtime, &[&b], || {
        record_from(&runtime, &config, &b).shard.primary_node_id == b
    });
    let recovery = format!("recover:recovered-x:{}", config.epoch + 1);
    wait_until("B recovers X", &runtime, &[&b], || {
        operation_status(&runtime, &b, &config, &recovery)
            == Some(crate::DfsPreparedOperationStatus::Committed)
    });
    runtime.partition_node(&c, Partition::default());
    runtime.set_peer_verdict(&b, &c, PeerPathVerdict::Path, false);
    catch_up_from(
        &runtime,
        &record_from(&runtime, &config, &b).shard,
        &b,
        &[&c],
    );
    assert_one_history(
        &runtime,
        &config,
        &[&b, &c],
        "recovered-x",
        &recovery,
        &current.object_id,
    );

    runtime.reopen_node(p.clone()).unwrap();
    runtime.set_peer_verdict(&b, &p, PeerPathVerdict::Path, false);
    wait_until("B re-adds P", &runtime, &[&b], || {
        write_set_of(&runtime, &config).contains(&p)
    });
    catch_up_from(
        &runtime,
        &record_from(&runtime, &config, &b).shard,
        &b,
        &[&c, &p],
    );
    let view = assert_one_history(
        &runtime,
        &config,
        &[&b, &c, &p],
        "recovered-x",
        &recovery,
        &current.object_id,
    );
    assert_eq!(view.object.as_ref().map(|object| object.mode), Some(0o640));
    runtime.stop_node(&c).unwrap();
    runtime.reopen_node(c.clone()).unwrap();
    assert_one_history(
        &runtime,
        &config,
        &[&b, &c, &p],
        "recovered-x",
        &recovery,
        &current.object_id,
    );
    runtime.close().unwrap();
}

/// P commits a write, but its commit record never reaches B or C before P
/// stops: the record sits past the successor's epoch start on P alone. The
/// successor recovers the write from its prepared body, with identical
/// effects, so even while P is away every node shows the same object, and
/// only P's own status reads committed. When P rejoins, alignment discards
/// that record and P converges on the new history; nothing is applied twice.
#[test]
fn native_takeover_old_primary_own_commit_record_is_discarded_on_rejoin() {
    let (mut runtime, config) = switched_with_kv_voters("native-loss-own-commit", Some((1, 3)));
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    {
        let mut service = primary(&runtime);
        directory(&mut service, "/services/own-commit");
    }
    for member in [&b, &c] {
        catch_up(&runtime, &config, member);
    }
    keep_with_paths(&runtime, &config, &[&b, &c]);
    // The members hold the write's prepare before P commits it. Every later
    // batch P sends them starts with that commit record: its background
    // publication, and the prepare of the change P makes next to record the
    // write's eventual copies. Holding all of them keeps the commit on P.
    // Compare the original write on the stopped primary with that same
    // recovered write. A later N-W repair legitimately adds a location to
    // the live copies and cannot update the stopped primary until rejoin.
    let repairs = [&p, &b, &c].map(|node| runtime.hold_repair_executor(node).unwrap());
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let captured = Arc::new(std::sync::Mutex::new(Vec::new()));
    for member in [&b, &c] {
        runtime.install_fault(FaultRule {
            source: Some(p.clone()),
            target: Some(member.clone()),
            route: None, // Both authenticated journal carriers can deliver the commit.
            call_id: None,
            origin: None,
            remaining_hits: 8,
            action: FaultAction::PauseWriteCommitCarriage {
                gate: Arc::clone(&gate),
                captured: Arc::clone(&captured),
            },
        });
    }
    let operation = sinomo_core::Id::generate().to_string();
    let written = runtime
        .client(&p)
        .unwrap()
        .posix_write_file_with_operation_id(
            "/services/own-commit/x.txt",
            b"own commit bytes",
            crate::DfsWriteOptions::replicated_eventual(vec![p.clone(), b.clone(), c.clone()]),
            &operation,
        )
        .expect("P commits the write");
    let object = written.object_id.clone();
    for member in [&b, &c] {
        assert!(
            journal_head(&runtime, member, &config) < journal_head(&runtime, &p, &config),
            "{member} lacks P's commit record"
        );
    }
    runtime.stop_node(&p).unwrap();
    runtime.clear_faults();
    lose(&runtime, &[&b, &c], &p);
    wait_until("a member takes over", &runtime, &[&b, &c], || {
        record_from(&runtime, &config, &b).shard.primary_node_id != p
    });
    let successor = record_from(&runtime, &config, &b).shard;
    let winner = successor.primary_node_id;
    let intent_id = crate::intent::stable_write_intent_id(&p, &object, &operation);
    let original = format!("write:{intent_id}");
    let recovery = format!("recover:{original}:{}", successor.epoch);
    wait_until(
        "the successor commits the recovery",
        &runtime,
        &[&b, &c],
        || {
            [&b, &c].into_iter().all(|member| {
                operation_status(&runtime, member, &config, &recovery)
                    == Some(crate::DfsPreparedOperationStatus::Committed)
            })
        },
    );
    let members = assert_one_history(&runtime, &config, &[&b, &c], &original, &recovery, &object);
    // P is away: only its own status for the original differs.
    let away = settled_view(&runtime, &p, &config, &original, &recovery, &object);
    assert_eq!(
        away.original,
        Some(crate::DfsPreparedOperationStatus::Committed)
    );
    assert_eq!(away.recovery, None);
    assert_eq!(
        away.object, members.object,
        "the same object even before P rejoins"
    );

    drop(repairs);
    runtime.reopen_node(p.clone()).unwrap();
    runtime.set_peer_verdict(&winner, &p, PeerPathVerdict::Path, false);
    wait_until("P rejoins", &runtime, &[&b, &c], || {
        write_set_of(&runtime, &config).contains(&p)
    });
    let others = [&p, &b, &c]
        .into_iter()
        .filter(|node| **node != winner)
        .collect::<Vec<_>>();
    catch_up_from(
        &runtime,
        &record_from(&runtime, &config, &winner).shard,
        &winner,
        &others,
    );
    assert_one_history(
        &runtime,
        &config,
        &[&winner, &p, others[1]],
        &original,
        &recovery,
        &object,
    );
    runtime.stop_node(&p).unwrap();
    runtime.reopen_node(p.clone()).unwrap();
    assert_one_history(
        &runtime,
        &config,
        &[&winner, &p, others[1]],
        &original,
        &recovery,
        &object,
    );
    runtime.close().unwrap();
}

/// TK1 in a quiet cluster with no write to prompt it: the primary, which
/// leads cluster KV, stops while the members are cut off from cluster KV, so
/// the first confirm reads fail. Nothing else changes once KV returns; the
/// re-sent confirm read alone completes the takeover, and the member with the
/// longer journal still takes over.
#[test]
fn native_takeover_resends_a_failed_confirm_read_in_a_quiet_cluster() {
    let (mut runtime, config) = switched_with_kv_voters("native-loss-reconfirm", None);
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    let current = {
        let mut service = primary(&runtime);
        directory(&mut service, "/services/reconfirm")
    };
    for member in [&b, &c] {
        catch_up(&runtime, &config, member);
    }
    // X is prepared on P and C; B never sees it, so C ranks first.
    fail_journal_calls(&runtime, &config, &b);
    prepare_unknown(&runtime, &config, &current, "reconfirm-x");
    runtime.clear_faults();
    keep_with_paths(&runtime, &config, &[&b, &c]);
    for member in [&b, &c] {
        runtime.partition_node(
            member,
            Partition {
                kv: true,
                dfs: false,
            },
        );
    }
    runtime.stop_node(&p).unwrap();
    lose(&runtime, &[&b, &c], &p);
    let unconfirmed = |node: &NodeId| {
        runtime
            .write_set_takeover_trace(node)
            .iter()
            .any(|line| line.contains("unconfirmed"))
    };
    wait_until("a confirm read fails", &runtime, &[&b, &c], || {
        unconfirmed(&b) || unconfirmed(&c)
    });
    for node in [&b, &c] {
        assert!(
            promise_of(&runtime, node, &config).is_none(),
            "{node} promised nothing"
        );
    }
    for member in [&b, &c] {
        runtime.partition_node(member, Partition::default());
    }
    wait_until("C takes over", &runtime, &[&b, &c], || {
        record_from(&runtime, &config, &b).shard.primary_node_id == c
    });
    assert_eq!(
        record_from(&runtime, &config, &b).shard.epoch,
        config.epoch + 1
    );
    wait_until(
        "the prepared change commits at the new epoch",
        &runtime,
        &[&b, &c],
        || object_mode(&runtime, &c, &config, &current.object_id) == Some(0o640),
    );
    assert!(shard_events(&runtime, &b, &config, DfsMetadataTakeoverEvent::SWAPPED).is_empty());
    runtime.close().unwrap();
}

/// The old primary returns while one of its own maintenance passes holds its
/// global admission and waits on the new primary, as its reconciliation does
/// when it routes a namespace request there. The new primary's join holds the
/// shard's exact admission, which that request waits behind, while it aligns
/// and probes the member, and the member answers no probe while its pass
/// holds its global admission. The two would wait on each other until the
/// member's pass gave up; the join's probes preempt that pass instead, and
/// the member rejoins at once.
#[test]
fn native_rejoin_preempts_a_maintenance_pass_on_the_returning_member() {
    let (mut runtime, config) =
        switched_with_kv_voters("native-loss-rejoin-preempts", Some((1, 3)));
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    keep_with_paths(&runtime, &config, &[&b, &c]);
    runtime.stop_node(&p).unwrap();
    lose(&runtime, &[&b, &c], &p);
    let write_set = |runtime: &DfsTestRuntime| {
        record_from(runtime, &config, &b)
            .write_set_state()
            .switched()
            .map(|state| state.write_set.clone())
            .unwrap_or_default()
    };
    wait_until("a member takes over", &runtime, &[&b, &c], || {
        record_from(&runtime, &config, &b).shard.primary_node_id != p
    });
    assert!(!write_set(&runtime).contains(&p));

    runtime.reopen_node(p.clone()).unwrap();
    // A maintenance pass on P that cannot finish before the new primary
    // answers: it holds P's global admission until foreground work preempts it.
    let scheduler = crate::runtime_host::daemon_dfs_mutation_scheduler(runtime.data_dir(&p))
        .expect("P's admission scheduler");
    let (held, holding) = std::sync::mpsc::channel();
    let pass = executor().spawn(async move {
        let interest = scheduler.begin_maintenance_interest();
        let admission = scheduler
            .admit(&crate::DfsMessageExecutionScope::GlobalExclusive)
            .await
            .expect("P's maintenance admission");
        held.send(()).unwrap();
        interest.requested().await;
        drop(admission);
    });
    holding
        .recv_timeout(Duration::from_secs(30))
        .expect("the pass holds P's admission");

    let started = Instant::now();
    for observer in [&b, &c] {
        runtime.set_peer_verdict(observer, &p, PeerPathVerdict::Path, false);
    }
    // A rejoin takes well under a second here; the bound only has to sit
    // clearly below the member pass's own request budget.
    let bound = Duration::from_secs(20);
    while !write_set(&runtime).contains(&p) {
        assert!(
            started.elapsed() < bound,
            "P was not re-added within {bound:?}:\n{}",
            event_log(&runtime, &[&b, &c])
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    let deadline = Instant::now() + Duration::from_secs(30);
    while !pass.is_finished() {
        assert!(Instant::now() < deadline, "the pass was preempted");
        std::thread::sleep(Duration::from_millis(20));
    }
    runtime.close().unwrap();
}
