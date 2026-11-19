//! Keeping W on the native test runtime: real Raft-backed cluster KV, real
//! shard stores and the real member acknowledgement path.

use super::commit_native_tests::{
    catch_up, cut, fail_journal_calls, primary, shrink, switched, write_set_of,
};
use super::*;
use crate::DfsMetadataShardId;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn member_probe(
    config: &DfsMetadataShardConfig,
    target: &NodeId,
    prefix: &crate::DfsMetadataPrefixProof,
    generation: u64,
) -> sinomo_core::network::ObjectRpcRequest {
    let command = crate::metadata_replication::dfs_metadata_write_set_member_probe_command(
        &config.primary_node_id,
        target,
        &config.file_system_id,
        &DfsMetadataShardId::new(config.shard_id.clone()),
        config.epoch,
        prefix,
        generation,
    );
    crate::dfs_metadata_journal_batch_rpc_request(&command).unwrap()
}

/// A catch-up probe naming the record's current generation is answered at
/// once, and its view names that generation. A probe naming a generation the
/// member has not observed is answered only once the member's own view
/// reaches it: the member waits on its publication, not on a timer.
#[test]
fn native_write_set_probe_waits_for_the_named_generation() {
    let (runtime, config) = switched("native-write-set-probe-generation", None);
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    let head = catch_up(&runtime, &config, &b);
    let current = write_set_of(&runtime, &config);

    let reply = runtime
        .rpc_request(
            &config.primary_node_id,
            &b,
            member_probe(&config, &b, &head, current.generation),
            Duration::from_secs(30),
        )
        .unwrap();
    let response = crate::decode_dfs_metadata_journal_batch_rpc_response(&reply).unwrap();
    assert_eq!(response.result, crate::DfsMetadataJournalBatchResult::Applied);
    assert!(response.verified_prefix_only);
    assert_eq!(
        response.member_view,
        Some(crate::DfsMetadataMemberAckView {
            epoch: config.epoch,
            write_set_generation: current.generation,
            durable_prefix: head.clone(),
        })
    );

    let next = current.generation + 1;
    // Moving the record to the next generation is what ends the wait. It is
    // submitted from another thread while this one waits on the probe.
    let change = build_metadata_write_set_shrink(
        &cut(&runtime, &config),
        &c,
        &DfsMetadataShardBounds { w_floor: 1, n: 3 },
    )
    .unwrap();
    let mut client = runtime.configuration_publisher(&config.primary_node_id);
    let submitter = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(1500));
        DfsControlPlaneStore::new(client.as_mut()).submit_metadata_write_set_change(&change)
    });
    let started = Instant::now();
    let reply = runtime
        .rpc_request(
            &config.primary_node_id,
            &b,
            member_probe(&config, &b, &head, next),
            Duration::from_secs(60),
        )
        .unwrap();
    assert!(
        started.elapsed() >= Duration::from_millis(1500),
        "the member answers only once its view reaches the named generation"
    );
    assert_eq!(
        submitter.join().unwrap(),
        DfsMetadataWriteSetTxnOutcome::Applied
    );
    let response = crate::decode_dfs_metadata_journal_batch_rpc_response(&reply).unwrap();
    assert_eq!(response.result, crate::DfsMetadataJournalBatchResult::Applied);
    assert_eq!(
        response.member_view,
        Some(crate::DfsMetadataMemberAckView {
            epoch: config.epoch,
            write_set_generation: next,
            durable_prefix: head.clone(),
        })
    );
    assert_eq!(write_set_of(&runtime, &config).generation, next);
}

use crate::test_support::{DfsTestRuntime, current_native_metadata_service};
use sinomo_core::network::PeerPathVerdict;

/// The keys one KV request compares and writes, for assertion messages.
fn kv_request_keys(request: &sinomo_core::kv::KvTxnRequest) -> String {
    use sinomo_core::kv::{KvCompare, KvOp};
    let compares = request.txn.compares.iter().map(|compare| match compare {
        KvCompare::Exists { key }
        | KvCompare::Missing { key }
        | KvCompare::ValueEquals { key, .. }
        | KvCompare::VersionEquals { key, .. }
        | KvCompare::CreateRevisionEquals { key, .. }
        | KvCompare::ModifyRevisionEquals { key, .. }
        | KvCompare::LeaseEquals { key, .. } => key.clone(),
    });
    let writes = request.txn.success.iter().filter_map(|op| match op {
        KvOp::Put { key, .. } | KvOp::Delete { key } => Some(key.clone()),
        KvOp::DeletePrefix { prefix } => Some(prefix.clone()),
        _ => None,
    });
    format!(
        "compares {:?} writes {:?}",
        compares.collect::<Vec<_>>(),
        writes.collect::<Vec<_>>()
    )
}

fn configured(config: &DfsMetadataShardConfig) -> Vec<NodeId> {
    metadata_shard_configured_node_ids(config)
        .into_iter()
        .collect()
}

/// A shard of `nodes` members on the write-set protocol under a subtree rule
/// W, N, switched by hand, with every peer reported reachable to the primary.
/// The primary's keeper is still suspended.
fn kept(
    name: &str,
    nodes: usize,
    rule: (usize, usize),
    mobile: &[usize],
) -> (DfsTestRuntime, DfsMetadataShardConfig) {
    let (runtime, service, config) = current_native_metadata_service(name, nodes, "/services").unwrap();
    drop(service);
    let policy = crate::DfsPlacementPolicy::new(
        sinomo_core::DfsObjectPolicy::ReplicatedEventual,
        rule.0,
        rule.1,
    )
    .unwrap();
    runtime
        .apply_placement_policy(
            &config.primary_node_id,
            "/services",
            crate::DfsPlacementScope::Subtree,
            policy,
        )
        .unwrap();
    let members = configured(&config);
    for (index, node) in config.replica_node_ids.iter().enumerate() {
        if mobile.contains(&index) {
            runtime
                .set_member_node_role(node, sinomo_core::ClusterNodeRole::Mobile)
                .unwrap();
        }
    }
    for node in &members {
        runtime.advertise_metadata_write_set_capability(node).unwrap();
    }
    let all = members.iter().cloned().collect();
    let switch = build_metadata_write_set_switch(&cut(&runtime, &config), &all).unwrap();
    let mut client = runtime.configuration_publisher(&config.primary_node_id);
    assert_eq!(
        DfsControlPlaneStore::new(client.as_mut()).submit_metadata_write_set_change(&switch),
        DfsMetadataWriteSetTxnOutcome::Applied
    );
    for node in &members {
        runtime.wait_published_configuration_current(node).unwrap();
        if node != &config.primary_node_id {
            runtime.set_peer_verdict(&config.primary_node_id, node, PeerPathVerdict::Path, false);
        }
    }
    (runtime, config)
}

fn record(runtime: &DfsTestRuntime, config: &DfsMetadataShardConfig) -> (DfsMetadataShardConfig, DfsMetadataShardWriteSet) {
    let cut = cut(runtime, config);
    let write_set = cut.write_set_state().switched().cloned().expect("switched");
    (cut.shard, write_set)
}

fn sorted(nodes: &[&NodeId]) -> Vec<NodeId> {
    let mut nodes = nodes.iter().map(|node| (*node).clone()).collect::<Vec<_>>();
    nodes.sort();
    nodes
}

fn wait_for(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(90);
    while !done() {
        assert!(Instant::now() < deadline, "{what} did not happen");
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn wait_in(
    runtime: &DfsTestRuntime,
    config: &DfsMetadataShardConfig,
    what: &str,
    mut done: impl FnMut() -> bool,
) {
    let deadline = Instant::now() + Duration::from_secs(90);
    while !done() {
        assert!(
            Instant::now() < deadline,
            "{what} did not happen: {}",
            describe(runtime, config)
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn describe(runtime: &DfsTestRuntime, config: &DfsMetadataShardConfig) -> String {
    let (shard, write_set) = record(runtime, config);
    format!(
        "replicas {:?} write set {:?} substitutes {:?} events {:?} failures {:?}",
        shard.replica_node_ids,
        write_set.write_set,
        write_set.substitutes,
        runtime
            .write_set_keeping_events(&config.primary_node_id)
            .into_iter()
            .map(|(name, event)| format!(
                "{name} {} g{} {:?} {}",
                event.shard_id, event.generation, event.node_ids, event.cause
            ))
            .collect::<Vec<_>>(),
        runtime.write_set_keeping_failures(&config.primary_node_id)
    )
}

fn event_names(runtime: &DfsTestRuntime, node: &NodeId) -> Vec<String> {
    runtime
        .write_set_keeping_events(node)
        .into_iter()
        .map(|(name, _)| name)
        .collect()
}

fn holds_object(
    runtime: &DfsTestRuntime,
    node: &NodeId,
    config: &DfsMetadataShardConfig,
    object_id: &crate::DfsObjectId,
) -> bool {
    use crate::DfsMetadataBackend as _;
    let root = crate::default_dfs_metadata_shards_root(runtime.data_dir(node))
        .join(crate::metadata_keys::key_segment(&config.shard_id));
    crate::LocalSobjMetadataBackend::open_for_inspection(
        DfsMetadataShardId::new(config.shard_id.clone()),
        root,
    )
    .ok()
    .and_then(|backend| backend.get_object(object_id).ok().flatten())
    .is_some()
}

/// A removed member deletes its copy when it next opens its stores, here at
/// its next maintenance turn.
fn copy_deleted(
    runtime: &DfsTestRuntime,
    node: &NodeId,
    config: &DfsMetadataShardConfig,
    object_id: &crate::DfsObjectId,
) -> bool {
    let _ = runtime.maintenance_tick(node, sinomo_core::network::DEFAULT_NETWORK_TIMEOUT);
    !holds_object(runtime, node, config, object_id)
}

/// Run `write` again while it is refused because the publication advanced
/// under it. The keeper's own changes to the filesystem's other shards
/// advance the publication; a write admitted across one, or a service opened
/// before one, is refused before it prepares, and a caller retries it on a
/// fresh service.
fn retry_configuration_churn<T>(
    mut write: impl FnMut() -> Result<T, crate::DfsPosixError>,
) -> Result<T, crate::DfsPosixError> {
    let mut result = write();
    let mut retries = 0;
    while let Err(crate::DfsPosixError::Dfs(crate::DfsError::Metadata { message })) = &result
        && message.contains("configuration advanced")
        && retries < 50
    {
        retries += 1;
        result = write();
    }
    result
}

fn write_file(
    runtime: &DfsTestRuntime,
    config: &DfsMetadataShardConfig,
    path: &str,
) -> crate::DfsObjectId {
    let mut service = runtime.metadata_primary_service("/services").unwrap();
    service
        .posix_write_file(
            path,
            path.as_bytes(),
            crate::DfsWriteOptions::replicated_eventual(vec![config.primary_node_id.clone()]),
        )
        .unwrap()
        .object_id
}

/// The switch-time reduction keeps N members; a member with no path leaves
/// the write set; with no reachable out-of-sync original a recruit takes its
/// place, catches up by snapshot and joins; when the original returns it
/// catches up incrementally and rejoins, and the most recent recruit is
/// retired and deletes its copy.
#[test]
fn native_keeper_reduces_shrinks_recruits_and_retires() {
    let (runtime, config) = kept("native-keeper-lifecycle", 4, (2, 3), &[]);
    let primary = config.primary_node_id.clone();
    let seed = write_file(&runtime, &config, "/services/keeper-seed.txt");
    // The commit record follows the acknowledged prepare to the members
    // through ordinary replication; drive it there instead of waiting.
    for member in &config.replica_node_ids {
        catch_up(&runtime, &config, member);
    }
    wait_for("the seed reaches every member", || {
        configured(&config)
            .iter()
            .all(|node| holds_object(&runtime, node, &config, &seed))
    });
    runtime.enable_write_set_keeping(&primary);
    // Reduction: four configured members, N = 3.
    // The keeper reports each change after it commits, so a committed record
    // can show a change before its event: every wait below names both.
    wait_in(
        &runtime,
        &config,
        "the switch-time reduction and its event",
        || {
            let (shard, write_set) = record(&runtime, &config);
            shard.replica_node_ids.len() == 2
                && write_set.write_set.len() == 3
                && event_names(&runtime, &primary)
                    .contains(&DfsMetadataWriteSetEvent::RETIRED.to_string())
        },
    );
    let (shard, write_set) = record(&runtime, &config);
    let spare = configured(&config)
        .into_iter()
        .find(|node| !metadata_shard_configured_node_ids(&shard).contains(node))
        .unwrap();
    let _ = write_set;
    wait_for("the retired copy is deleted", || copy_deleted(&runtime, &spare, &config, &seed));
    let b = shard.replica_node_ids[0].clone();
    let c = shard.replica_node_ids[1].clone();

    // C has no path: it leaves the write set, the floor 2 allows it.
    runtime.set_peer_verdict(&primary, &c, PeerPathVerdict::NoPath, false);
    // The spare becomes the only reachable candidate: it is recruited.
    wait_in(&runtime, &config, "the recruit joins", || {
        let (_, write_set) = record(&runtime, &config);
        write_set.write_set == sorted(&[&primary, &b, &spare])
    });
    let (shard, write_set) = record(&runtime, &config);
    assert!(metadata_shard_configured_node_ids(&shard).contains(&c), "C keeps its membership");
    assert!(holds_object(&runtime, &c, &config, &seed), "C keeps its copy");
    assert!(holds_object(&runtime, &spare, &config, &seed), "the recruit holds the snapshot");
    assert_eq!(write_set.substitutes.len(), 1);
    assert_eq!(write_set.substitutes[0].node_id, spare);
    write_file(&runtime, &config, "/services/keeper-after-recruit.txt");

    // C returns: it rejoins and the substitute is retired.
    runtime.set_peer_verdict(&primary, &c, PeerPathVerdict::Path, false);
    wait_in(&runtime, &config, "the original rejoins and the substitute is retired", || {
        let (shard, write_set) = record(&runtime, &config);
        write_set.write_set == sorted(&[&primary, &b, &c])
            && !metadata_shard_configured_node_ids(&shard).contains(&spare)
    });
    wait_for("the substitute's copy is deleted", || copy_deleted(&runtime, &spare, &config, &seed));
    wait_in(&runtime, &config, "every change's event", || {
        let names = event_names(&runtime, &primary);
        [
            DfsMetadataWriteSetEvent::SHRUNK,
            DfsMetadataWriteSetEvent::RECRUITED,
            DfsMetadataWriteSetEvent::ADD_SUBMITTED,
            DfsMetadataWriteSetEvent::ADDED,
            DfsMetadataWriteSetEvent::RETIRED,
        ]
        .into_iter()
        .all(|name| names.contains(&name.to_string()))
    });
    write_file(&runtime, &config, "/services/keeper-after-return.txt");
}

/// A dead member held at a W floor equal to N is replaced by one recruit
/// beyond N, which every configured member being in the write set admits;
/// once the recruit has joined, the dead member leaves and writes resume.
#[test]
fn native_keeper_replaces_a_dead_member_at_a_floor_equal_to_n() {
    let (runtime, config) = kept("native-keeper-replacement", 3, (2, 2), &[]);
    let primary = config.primary_node_id.clone();
    runtime.enable_write_set_keeping(&primary);
    wait_in(&runtime, &config, "the reduction to N = 2", || {
        let (shard, write_set) = record(&runtime, &config);
        shard.replica_node_ids.len() == 1 && write_set.write_set.len() == 2
    });
    let (shard, _) = record(&runtime, &config);
    let member = shard.replica_node_ids[0].clone();
    let spare = configured(&config)
        .into_iter()
        .find(|node| !metadata_shard_configured_node_ids(&shard).contains(node))
        .unwrap();
    // The member has no path; at the floor it stays, and the spare is
    // recruited as a replacement beyond N.
    runtime.set_peer_verdict(&primary, &member, PeerPathVerdict::NoPath, false);
    wait_in(&runtime, &config, "the replacement joins and the dead member leaves", || {
        let (_, write_set) = record(&runtime, &config);
        write_set.write_set == sorted(&[&primary, &spare])
    });
    let (shard, write_set) = record(&runtime, &config);
    assert!(metadata_shard_configured_node_ids(&shard).contains(&member));
    assert_eq!(write_set.substitutes.len(), 1);
    write_file(&runtime, &config, "/services/keeper-replacement.txt");
}

/// Copies stay on non-mobile members: the switch-time reduction keeps the
/// non-mobile ones, and when a member becomes mobile while a non-mobile one is
/// eligible, the non-mobile one replaces it and the mobile one is removed.
#[test]
fn native_keeper_keeps_copies_on_non_mobile_members() {
    let (runtime, config) = kept("native-keeper-mobile", 3, (1, 2), &[1]);
    let primary = config.primary_node_id.clone();
    let mobile = config.replica_node_ids[1].clone();
    let other = config.replica_node_ids[0].clone();
    runtime.enable_write_set_keeping(&primary);
    wait_in(&runtime, &config, "the reduction keeps the non-mobile member", || {
        let (shard, write_set) = record(&runtime, &config);
        shard.replica_node_ids == vec![other.clone()]
            && write_set.write_set == sorted(&[&primary, &other])
    });
    // The roles swap: the non-mobile one replaces the now mobile member.
    runtime
        .set_member_node_role(&mobile, sinomo_core::ClusterNodeRole::Workstation)
        .unwrap();
    runtime
        .set_member_node_role(&other, sinomo_core::ClusterNodeRole::Mobile)
        .unwrap();
    wait_in(&runtime, &config, "the non-mobile member replaces the mobile one", || {
        let (shard, write_set) = record(&runtime, &config);
        shard.replica_node_ids == vec![mobile.clone()]
            && write_set.write_set == sorted(&[&primary, &mobile])
    });
}

/// A shard of `nodes` members under the subtree rule W, N that the primary's
/// keeper will switch itself, once every member advertises the protocol.
/// Every peer is reported reachable to the primary; the keeper is suspended.
fn unswitched(
    name: &str,
    nodes: usize,
    rule: (usize, usize),
) -> (DfsTestRuntime, DfsMetadataShardConfig) {
    let (runtime, service, config) = current_native_metadata_service(name, nodes, "/services").unwrap();
    drop(service);
    let policy = crate::DfsPlacementPolicy::new(
        sinomo_core::DfsObjectPolicy::ReplicatedEventual,
        rule.0,
        rule.1,
    )
    .unwrap();
    runtime
        .apply_placement_policy(
            &config.primary_node_id,
            "/services",
            crate::DfsPlacementScope::Subtree,
            policy,
        )
        .unwrap();
    for node in &config.replica_node_ids {
        runtime.set_peer_verdict(&config.primary_node_id, node, PeerPathVerdict::Path, false);
    }
    (runtime, config)
}

fn advertise_all(runtime: &DfsTestRuntime, config: &DfsMetadataShardConfig) {
    let members = configured(config);
    for node in &members {
        runtime.advertise_metadata_write_set_capability(node).unwrap();
    }
    for node in &members {
        runtime.wait_published_configuration_current(node).unwrap();
    }
}

fn switched_record(
    runtime: &DfsTestRuntime,
    config: &DfsMetadataShardConfig,
) -> Option<(DfsMetadataShardConfig, DfsMetadataShardWriteSet)> {
    let cut = cut(runtime, config);
    let write_set = cut.write_set_state().switched().cloned()?;
    Some((cut.shard, write_set))
}

fn switch_events(runtime: &DfsTestRuntime, config: &DfsMetadataShardConfig) -> Vec<(String, String)> {
    runtime
        .write_set_keeping_events(&config.primary_node_id)
        .into_iter()
        .filter(|(_, event)| event.shard_id == config.shard_id)
        .map(|(name, event)| (name, event.cause))
        .collect()
}

/// The switch reads the content the shard already holds: a W = 3 object
/// committed under the all-member rule starts the record's raised N at 3, so
/// the rule's N = 2 does not reduce the shard below that content's W.
#[test]
fn native_keeper_switch_starts_raised_n_from_the_content_w_it_finds() {
    let (runtime, config) = unswitched("native-keeper-switch-content-w", 3, (1, 2));
    let primary = config.primary_node_id.clone();
    let mut service = runtime.metadata_primary_service("/services").unwrap();
    service
        .posix_write_file_with_operation_id(
            "/services/keeper-w3-before-switch.bin",
            b"committed at W = 3 before the switch",
            crate::DfsWriteOptions {
                policy: crate::DfsReplicationPolicy::new(3, 3).unwrap(),
                replica_candidates: configured(&config),
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
            "keeper-w3-before-switch",
        )
        .unwrap();
    drop(service);
    advertise_all(&runtime, &config);
    runtime.enable_write_set_keeping(&primary);
    // The keeper reports the switch after it commits: wait for both, then
    // check the switch's cause.
    wait_for("the keeper switches the shard and reports it", || {
        switched_record(&runtime, &config).is_some()
            && switch_events(&runtime, &config)
                .iter()
                .any(|(name, _)| name == DfsMetadataWriteSetEvent::SWITCHED)
    });
    let (shard, write_set) = switched_record(&runtime, &config).unwrap();
    assert_eq!(write_set.raised_n, Some(3), "{write_set:?}");
    assert_eq!(
        switch_events(&runtime, &config).first(),
        Some(&(
            DfsMetadataWriteSetEvent::SWITCHED.to_string(),
            "capability-gate".to_string()
        ))
    );
    // Effective N is 3: the reduction to the rule's N = 2 does not happen.
    std::thread::sleep(Duration::from_secs(2));
    let (after, kept) = switched_record(&runtime, &config).unwrap();
    assert_eq!(after.replica_node_ids, shard.replica_node_ids);
    assert_eq!(kept.write_set.len(), 3, "{:?}", switch_events(&runtime, &config));
}

/// A content version that records no replication policy, as content written
/// before versions recorded it, has an unknown W: the switch keeps every
/// configured member, says so in its event, and nothing is reduced. Current
/// code records every content version's policy, so the test names an object
/// whose version the switch evidence reads as unrecorded.
#[test]
fn native_keeper_switch_keeps_every_member_for_content_of_unrecorded_w() {
    let (runtime, config) = unswitched("native-keeper-switch-unrecorded", 3, (1, 2));
    let primary = config.primary_node_id.clone();
    let mut service = runtime.metadata_primary_service("/services").unwrap();
    let stat = service
        .posix_write_file(
            "/services/keeper-unrecorded.txt",
            b"written before versions recorded their policy",
            crate::DfsWriteOptions::replicated_eventual(vec![primary.clone()]),
        )
        .unwrap();
    drop(service);
    crate::service_write_set_keeping::treat_content_as_unrecorded_for_test(stat.object_id);
    advertise_all(&runtime, &config);
    runtime.enable_write_set_keeping(&primary);
    // The keeper reports the switch after it commits: wait for both, then
    // check the switch's cause.
    wait_for("the keeper switches the shard and reports it", || {
        switched_record(&runtime, &config).is_some()
            && switch_events(&runtime, &config)
                .iter()
                .any(|(name, _)| name == DfsMetadataWriteSetEvent::SWITCHED)
    });
    let (shard, write_set) = switched_record(&runtime, &config).unwrap();
    assert_eq!(write_set.raised_n, Some(3), "{write_set:?}");
    assert_eq!(
        switch_events(&runtime, &config).first(),
        Some(&(
            DfsMetadataWriteSetEvent::SWITCHED.to_string(),
            "capability-gate-unrecorded-content-w".to_string()
        ))
    );
    std::thread::sleep(Duration::from_secs(2));
    let (after, _) = switched_record(&runtime, &config).unwrap();
    assert_eq!(after.replica_node_ids, shard.replica_node_ids);
    assert_eq!(after.replica_node_ids.len(), 2);
    assert!(
        !switch_events(&runtime, &config)
            .iter()
            .any(|(name, _)| name == DfsMetadataWriteSetEvent::RETIRED),
        "{:?}",
        switch_events(&runtime, &config)
    );
}

/// Quiet means no work: a Main daemon whose shards are all unswitched holds
/// no network reachability subscription; it takes one once it is primary of
/// a switched shard.
#[test]
fn native_keeper_holds_no_reachability_subscription_without_a_switched_shard() {
    let (runtime, config) = unswitched("native-keeper-no-subscription", 3, (1, 3));
    for member in configured(&config) {
        runtime
            .withhold_metadata_write_set_capability(&member)
            .unwrap();
    }
    let primary = config.primary_node_id.clone();
    runtime.enable_write_set_keeping(&primary);
    wait_for("the keeper evaluates", || {
        runtime.write_set_keeping_holds_reachability(&primary).is_some()
    });
    std::thread::sleep(Duration::from_secs(1));
    assert_eq!(runtime.write_set_keeping_holds_reachability(&primary), Some(false));
    assert!(switched_record(&runtime, &config).is_none());
    advertise_all(&runtime, &config);
    wait_for("the keeper switches the shard", || switched_record(&runtime, &config).is_some());
    wait_for("the keeper subscribes", || {
        runtime.write_set_keeping_holds_reachability(&primary) == Some(true)
    });
}

/// A commit whose content W is above the W the record accepts stays pending
/// until the keeper raises the shard's N: then it recruits up to it, the
/// recovery pass commits the body and the same-id retry returns it.
#[test]
fn native_keeper_raises_n_for_a_commit_above_the_accepted_w() {
    let (runtime, config) = kept("native-keeper-raise-n", 3, (1, 2), &[]);
    let primary = config.primary_node_id.clone();
    runtime.enable_write_set_keeping(&primary);
    wait_in(&runtime, &config, "the reduction to N = 2", || {
        let (shard, write_set) = record(&runtime, &config);
        shard.replica_node_ids.len() == 1 && write_set.write_set.len() == 2
    });
    let write = |runtime: &DfsTestRuntime| {
        let mut service = runtime.metadata_primary_service("/services").unwrap();
        service.posix_write_file_with_operation_id(
            "/services/keeper-w3.bin",
            b"content at W = 3",
            crate::DfsWriteOptions {
                policy: crate::DfsReplicationPolicy::new(3, 3).unwrap(),
                replica_candidates: configured(&config),
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
            "keeper-w3",
        )
    };
    match retry_configuration_churn(|| write(&runtime)) {
        Err(crate::DfsPosixError::Dfs(crate::DfsError::MetadataCommitOutcomeUnknown {
            cause: crate::DfsCommitOutcomeUnknownCause::ReplicationBelowIntent { .. }
                | crate::DfsCommitOutcomeUnknownCause::BelowWriteSetFloor { .. },
            ..
        })) => {}
        other => panic!("a write above the accepted W stays pending first: {other:?}"),
    }
    // The keeper reports the raise after it commits: wait for both.
    wait_in(
        &runtime,
        &config,
        "N rises to 3, a recruit joins and the raise is reported",
        || {
            let (_, write_set) = record(&runtime, &config);
            write_set.raised_n == Some(3)
                && write_set.write_set.len() == 3
                && event_names(&runtime, &primary)
                    .contains(&DfsMetadataWriteSetEvent::N_RAISED.to_string())
        },
    );
    let mut committed = None;
    wait_in(&runtime, &config, "the same operation commits", || {
        committed = write(&runtime).ok();
        committed.is_some()
    });
    // The recruit made for the raise is not retired below it.
    std::thread::sleep(Duration::from_secs(2));
    assert_eq!(record(&runtime, &config).1.write_set.len(), 3);
}

/// A dead member at the floor stays and nothing is submitted while it waits;
/// an unsettled verdict starts nothing; an idle stable shard sends nothing.
#[test]
fn native_keeper_holds_the_floor_and_does_nothing_without_a_settled_verdict() {
    let (runtime, config) = kept("native-keeper-floor", 3, (2, 3), &[]);
    let primary = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    // Every write-set transaction on this shard compares its record. The
    // keeper also switches and reduces the filesystem's other shards when it
    // starts; those are not this shard's work.
    let record_key = metadata_shard_write_set_key(&config.file_system_id, &config.shard_id);
    let sent = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let logged = Arc::clone(&sent);
    let _hook = runtime
        .native_kv_request_hook(&primary, move |request| {
            let keys = kv_request_keys(request);
            if keys.contains(&format!("{record_key:?}")) {
                logged.lock().unwrap().push(keys);
            }
            Ok(())
        })
        .unwrap();
    let sent_count = || sent.lock().unwrap().len();
    runtime.enable_write_set_keeping(&primary);
    // Stable: three members, N = 3. Nothing is sent for this shard.
    std::thread::sleep(Duration::from_secs(3));
    assert_eq!(
        sent_count(),
        0,
        "an idle shard sends nothing: {:#?} {}",
        sent.lock().unwrap(),
        describe(&runtime, &config)
    );
    // A verdict the network is still verifying starts nothing.
    runtime.set_peer_verdict(&primary, &c, PeerPathVerdict::NoPath, true);
    std::thread::sleep(Duration::from_secs(2));
    assert_eq!(record(&runtime, &config).1.write_set.len(), 3);
    assert_eq!(sent_count(), 0, "an unsettled verdict starts nothing");
    // Settled: C leaves the write set.
    runtime.set_peer_verdict(&primary, &c, PeerPathVerdict::NoPath, false);
    wait_in(&runtime, &config, "C leaves", || {
        record(&runtime, &config).1.write_set == sorted(&[&primary, &b])
    });
    // B too has no path: at the floor 2 it stays, and with no candidate to
    // recruit nothing is submitted.
    let before = sent_count();
    runtime.set_peer_verdict(&primary, &b, PeerPathVerdict::NoPath, false);
    std::thread::sleep(Duration::from_secs(3));
    assert_eq!(record(&runtime, &config).1.write_set, sorted(&[&primary, &b]));
    assert_eq!(
        sent_count(),
        before,
        "a dead member held at the floor costs no requests: {:#?}",
        sent.lock().unwrap()
    );
}

/// A commit that could not reach a member asks the network to verify it; once
/// the verdict settles with no path the member leaves and the prepared change
/// commits through recovery.
#[test]
fn native_keeper_acts_on_a_commit_that_could_not_reach_a_member() {
    let (mut runtime, config) = kept("native-keeper-commit-hint", 3, (1, 3), &[]);
    let primary = config.primary_node_id.clone();
    let c = config.replica_node_ids[1].clone();
    runtime.enable_write_set_keeping(&primary);
    // C is gone: every call to it fails at the network layer, while its
    // verdict still reports a path until the network settles it.
    runtime.stop_node(&c).unwrap();
    let write = |runtime: &DfsTestRuntime| {
        let mut service = runtime.metadata_primary_service("/services").unwrap();
        service.posix_write_file_with_operation_id(
            "/services/keeper-hint.txt",
            b"hint",
            crate::DfsWriteOptions::replicated_eventual(vec![primary.clone()]),
            "keeper-hint",
        )
    };
    let result = retry_configuration_churn(|| write(&runtime));
    match &result {
        Err(crate::DfsPosixError::Dfs(crate::DfsError::MetadataCommitOutcomeUnknown {
            cause: crate::DfsCommitOutcomeUnknownCause::AcknowledgementsMissing { missing, .. },
            ..
        })) if missing.contains(&c) => {}
        other => panic!(
            "the commit cannot reach C: {other:?} {:?}",
            runtime.write_set_keeping_failures(&primary)
        ),
    }
    wait_in(&runtime, &config, "the keeper asks the network about C", || {
        runtime.peer_verification_requests(&primary).contains(&c)
    });
    runtime.set_peer_verdict(&primary, &c, PeerPathVerdict::NoPath, false);
    wait_in(&runtime, &config, "C leaves", || !record(&runtime, &config).1.contains(&c));
    let mut committed = None;
    wait_in(&runtime, &config, "the prepared change commits", || {
        let mut service = runtime.metadata_primary_service("/services").unwrap();
        committed = service
            .posix_write_file_with_operation_id(
                "/services/keeper-hint.txt",
                b"hint",
                crate::DfsWriteOptions::replicated_eventual(vec![primary.clone()]),
                "keeper-hint",
            )
            .ok();
        committed.is_some()
    });
}

/// K16: file and directory sync on a write-set shard certify against the
/// write set, so out-of-set members B and C, both unreachable, are not needed
/// although a configured majority would need one of them. Below the W floor
/// nothing is certified.
#[test]
fn native_write_set_sync_certifies_against_the_write_set() {
    let (runtime, config) = switched("native-write-set-sync", Some((1, 3)));
    let local = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    shrink(&runtime, &config, &c, 1);
    shrink(&runtime, &config, &b, 1);
    fail_journal_calls(&runtime, &config, &b);
    fail_journal_calls(&runtime, &config, &c);
    let mut service = primary(&runtime);
    service
        .posix_write_file(
            "/services/write-set-sync.txt",
            b"synced",
            crate::DfsWriteOptions::replicated_eventual(vec![local.clone()]),
        )
        .unwrap();
    let file = service
        .posix_capture_file_sync_target("/services/write-set-sync.txt", crate::DfsPosixSyncKind::File)
        .unwrap();
    let certificate = service
        .posix_sync_file_local(&file, Duration::from_secs(30))
        .expect("the write set {P} certifies the file");
    assert_eq!(certificate.metadata_certificate.acknowledged_node_ids, vec![local.clone()]);
    let directory = service.posix_capture_directory_sync_target("/services").unwrap();
    let certificate = service
        .posix_sync_directory_local(&directory, Duration::from_secs(30))
        .expect("the write set {P} certifies the directory");
    assert_eq!(certificate.metadata_certificate.acknowledged_node_ids, vec![local.clone()]);
    drop(service);

    // The rule's floor rises to 2 above the write set of 1.
    let policy =
        crate::DfsPlacementPolicy::new(sinomo_core::DfsObjectPolicy::ReplicatedEventual, 2, 3)
            .unwrap();
    runtime
        .apply_placement_policy(&local, "/services", crate::DfsPlacementScope::Subtree, policy)
        .unwrap();
    runtime.wait_published_configuration_current(&local).unwrap();
    let mut service = primary(&runtime);
    match service.posix_sync_file_local(&file, Duration::from_secs(30)) {
        Err(crate::DfsPosixError::PendingWrite {
            durable_receipts: 1,
            required_receipts: 2,
            ..
        }) => {}
        other => panic!("file sync below the floor is a pending write: {other:?}"),
    }
    // Below the floor a directory sync is retriable (EAGAIN, NFS JUKEBOX),
    // like the file, never a definite failure.
    match service.posix_sync_directory_local(&directory, Duration::from_secs(30)) {
        Err(error @ crate::DfsPosixError::PendingWrite {
            durable_receipts: 1,
            required_receipts: 2,
            ..
        }) => assert!(crate::posix_error_outcome_is_unknown(&error)),
        other => panic!("directory sync below the floor is pending: {other:?}"),
    }
}

/// Two primaries each re-add the other. A is primary of the services shard;
/// B took the users shard over from A, so A is out of its write set; and B is
/// out of the services write set. Both return at once: A re-adds B to
/// services while B re-adds A to users, and both joins hold their admission
/// before either probes its member. A join holds only its own shard's
/// admission, so each node answers the other primary's probe beside its own
/// join, and both re-adds complete.
#[test]
fn native_cross_joins_of_two_primaries_both_complete() {
    let (runtime, service, users) =
        current_native_metadata_service("native-cross-join", 3, "/users").unwrap();
    drop(service);
    let services = runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|shard| shard.shard_id == "services")
        .unwrap();
    let a = users.primary_node_id.clone();
    assert_eq!(services.primary_node_id, a);
    assert_eq!(configured(&services), configured(&users));
    let policy =
        crate::DfsPlacementPolicy::new(sinomo_core::DfsObjectPolicy::ReplicatedEventual, 1, 3)
            .unwrap();
    for path in ["/users", "/services"] {
        runtime
            .apply_placement_policy(&a, path, crate::DfsPlacementScope::Subtree, policy.clone())
            .unwrap();
    }
    for node in configured(&users) {
        runtime.advertise_metadata_write_set_capability(&node).unwrap();
    }
    let switch_to = |config: &DfsMetadataShardConfig, write_set: &BTreeSet<NodeId>| {
        let switch = build_metadata_write_set_switch(&cut(&runtime, config), write_set).unwrap();
        let mut client = runtime.configuration_publisher(&config.primary_node_id);
        assert_eq!(
            DfsControlPlaneStore::new(client.as_mut()).submit_metadata_write_set_change(&switch),
            DfsMetadataWriteSetTxnOutcome::Applied
        );
    };

    // Only users is switched: the other members judge A lost and one of them
    // takes users over. A is left configured outside its write set.
    switch_to(&users, &configured(&users).into_iter().collect());
    for node in configured(&users) {
        runtime.wait_published_configuration_current(&node).unwrap();
    }
    let members = configured(&users)
        .into_iter()
        .filter(|node| node != &a)
        .collect::<Vec<_>>();
    super::takeover_driver_native_tests::keep_with_paths(
        &runtime,
        &users,
        &members.iter().collect::<Vec<_>>(),
    );
    super::takeover_driver_native_tests::lose(
        &runtime,
        &members.iter().collect::<Vec<_>>(),
        &a,
    );
    let users_record = |reader: &NodeId| {
        super::takeover_driver_native_tests::record_from(&runtime, &users, reader)
    };
    wait_for("a member takes users over", || {
        users_record(&members[0]).shard.primary_node_id != a
    });
    let b = users_record(&members[0]).shard.primary_node_id.clone();
    let c = members.iter().find(|node| **node != b).unwrap().clone();
    assert!(
        !users_record(&b)
            .write_set_state()
            .switched()
            .unwrap()
            .write_set
            .contains(&a)
    );

    // From here each primary's first join is held, once admitted, until the
    // other's is admitted too, so each probes the other while both hold.
    let overlap = runtime.hold_write_set_joins_until_all_admitted(&[&a, &b]);
    let started = Instant::now();
    // A returns to users: B's join re-adding A is admitted and held.
    for member in [&b, &c] {
        runtime.set_peer_verdict(member, &a, PeerPathVerdict::Path, false);
    }
    // Services is switched without B, so A re-adds B: A's join is admitted,
    // and both proceed.
    switch_to(&services, &[a.clone(), c.clone()].into_iter().collect());
    for peer in [&b, &c] {
        runtime.set_peer_verdict(&a, peer, PeerPathVerdict::Path, false);
    }
    runtime.enable_write_set_keeping(&a);
    // A re-add takes well under a second here; the bound only has to sit
    // clearly below anything that could end a wait on its own.
    let bound = Duration::from_secs(30);
    let in_write_set = |record: DfsMetadataShardRecordCut, member: &NodeId| {
        record
            .write_set_state()
            .switched()
            .is_some_and(|state| state.write_set.contains(member))
    };
    while !(in_write_set(users_record(&b), &a)
        && in_write_set(super::takeover_driver_native_tests::record_from(&runtime, &services, &a), &b))
    {
        assert!(
            started.elapsed() < bound,
            "both re-adds within {bound:?}: users {:?}\nservices {:?}\nA {:?}\nB {:?}",
            users_record(&b).write_set_state(),
            super::takeover_driver_native_tests::record_from(&runtime, &services, &a).write_set_state(),
            runtime.write_set_keeping_failures(&a),
            runtime.write_set_keeping_failures(&b),
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(overlap.all_admitted(), "both joins were held together");
    drop(overlap);
    runtime.close().unwrap();
}

/// The users shard switched with every member, with default setup's own
/// shard-reconciliation calls captured on every node.
fn switched_users_with_setup_captures(
    name: &str,
) -> (
    DfsTestRuntime,
    DfsMetadataShardConfig,
    Vec<crate::default_cluster::bootstrap_observation::CaptureGuard>,
) {
    let (runtime, service, users) = current_native_metadata_service(name, 3, "/users").unwrap();
    drop(service);
    for node in configured(&users) {
        runtime.advertise_metadata_write_set_capability(&node).unwrap();
    }
    let all = configured(&users).into_iter().collect();
    let switch = build_metadata_write_set_switch(&cut(&runtime, &users), &all).unwrap();
    let mut client = runtime.configuration_publisher(&users.primary_node_id);
    assert_eq!(
        DfsControlPlaneStore::new(client.as_mut()).submit_metadata_write_set_change(&switch),
        DfsMetadataWriteSetTxnOutcome::Applied
    );
    drop(client);
    for node in configured(&users) {
        runtime.wait_published_configuration_current(&node).unwrap();
    }
    let captures = configured(&users)
        .into_iter()
        .map(|node| {
            crate::default_cluster::bootstrap_observation::CaptureGuard::start(
                runtime.data_dir(&node).root(),
                node,
            )
        })
        .collect();
    (runtime, users, captures)
}

/// Whether `capture` holds a default-setup pass that read `shard` as the
/// current assignment.
fn setup_saw(
    capture: &crate::default_cluster::bootstrap_observation::CaptureGuard,
    shard: &DfsMetadataShardConfig,
) -> bool {
    let snapshot = capture.snapshot();
    let expected = serde_json::to_value(shard).unwrap();
    snapshot["events"].as_array().is_some_and(|events| {
        events.iter().any(|event| {
            event["stage"] == "shard-current" && event["value"]["current"] == expected
        })
    })
}

fn assert_no_pending_handoff(runtime: &DfsTestRuntime, shard: &DfsMetadataShardConfig) {
    let control = runtime.shared_control_plane().unwrap();
    let pending = control.with_client(|kv| {
        DfsControlPlaneStore::new(kv.as_mut())
            .get_metadata_shard_membership_handoff(&shard.file_system_id, &shard.shard_id)
            .unwrap()
    });
    assert!(pending.is_none(), "default setup began a handoff: {pending:?}");
}

/// A cooperative primary rotation of a switched shard leaves its write-set
/// record bound to the new primary, epoch and epoch start. Default setup
/// then leaves the rotated shard alone: the pass the rotation itself prompts,
/// and a pass that asks for the former primary back.
#[test]
fn native_switched_shard_rotation_stays_with_its_new_primary() {
    let (runtime, users, captures) = switched_users_with_setup_captures("native-rotation-kept");
    let a = users.primary_node_id.clone();
    let b = users.replica_node_ids[0].clone();
    let c = users.replica_node_ids[1].clone();
    let rotated = runtime
        .reassign_metadata_shard(&users.file_system_id, "users", b.clone(), vec![a.clone(), c.clone()])
        .unwrap();
    assert_eq!(rotated.primary_node_id, b);
    assert_eq!(rotated.epoch, users.epoch + 1);
    let assert_bound = |runtime: &DfsTestRuntime| {
        for reader in [&a, &b, &c] {
            let cut = super::takeover_driver_native_tests::record_from(runtime, &users, reader);
            assert_eq!(cut.shard, rotated, "{reader} reads the rotated shard");
            let state = cut
                .write_set_state()
                .switched()
                .cloned()
                .unwrap_or_else(|| panic!("the record stays bound: {:?}", cut.write_set_state()));
            assert_eq!(state.primary_node_id, b);
            assert_eq!(state.epoch, rotated.epoch);
            assert!(state.epoch_start.is_some());
        }
    };
    assert_bound(&runtime);
    // The rotation is an input of default setup: a pass reads the rotated
    // shard and keeps it.
    wait_for("default setup reads the rotated shard", || {
        captures.iter().any(|capture| setup_saw(capture, &rotated))
    });
    assert_bound(&runtime);
    // A pass asking for the former primary back changes nothing either.
    let mut former = rotated.clone();
    former.primary_node_id = a.clone();
    former.replica_node_ids = sorted(&[&b, &c]);
    former.epoch = rotated.epoch + 1;
    runtime.reconcile_default_metadata_shard(&a, former).unwrap();
    for node in configured(&users) {
        runtime.wait_published_configuration_current(&node).unwrap();
    }
    assert_no_pending_handoff(&runtime, &users);
    assert_bound(&runtime);
    runtime.close().unwrap();
}

/// A write-set record no longer bound to its shard never lets default setup
/// change the shard's primary: the binding broke, so setup leaves the shard
/// on the all-member rule for a re-switch to rebind, rather than handing it
/// to another layout.
#[test]
fn native_default_setup_never_moves_a_shard_with_a_stale_record() {
    let (runtime, users, _captures) = switched_users_with_setup_captures("native-stale-record-kept");
    let a = users.primary_node_id.clone();
    let b = users.replica_node_ids[0].clone();
    let c = users.replica_node_ids[1].clone();
    let mut stale = cut(&runtime, &users)
        .write_set_state()
        .switched()
        .cloned()
        .unwrap();
    stale.epoch = users.epoch - 1;
    let mut client = runtime.configuration_publisher(&a);
    client
        .put(
            crate::metadata_shard_write_set_key(&users.file_system_id, &users.shard_id),
            sinomo_core::kv::encode_kv_value(&stale).unwrap(),
        )
        .unwrap();
    drop(client);
    for node in configured(&users) {
        runtime.wait_published_configuration_current(&node).unwrap();
    }
    assert!(matches!(
        cut(&runtime, &users).write_set_state(),
        DfsMetadataShardWriteSetState::Stale { .. }
    ));
    // Setup asks for the shard's primary to move to C.
    let mut moved = users.clone();
    moved.primary_node_id = c.clone();
    moved.replica_node_ids = sorted(&[&a, &b]);
    moved.epoch = users.epoch + 1;
    assert_eq!(runtime.reconcile_default_metadata_shard(&a, moved), Ok(()));
    for node in configured(&users) {
        runtime.wait_published_configuration_current(&node).unwrap();
    }
    assert_no_pending_handoff(&runtime, &users);
    let current = cut(&runtime, &users);
    assert_eq!(current.shard, users, "the primary and epoch stay");
    assert!(matches!(
        current.write_set_state(),
        DfsMetadataShardWriteSetState::Stale { .. }
    ));
    runtime.close().unwrap();
}

#[test]
fn write_set_file_sync_fans_out_one_frozen_barrier() {
    use super::commit_native_tests::{hold_replies, release};
    let (runtime, config) = switched("native-one-write-set-barrier", None);
    let mut service = primary(&runtime);
    let path = "/services/one-barrier";
    let opts = crate::DfsWriteOptions::replicated_eventual(
        std::iter::once(config.primary_node_id.clone())
            .chain(config.replica_node_ids.clone())
            .collect(),
    );
    service.posix_write_file(path, b"frozen", opts).unwrap();
    let frozen = service
        .posix_capture_file_sync_target(path, crate::DfsPosixSyncKind::File)
        .unwrap();
    service
        .posix_sync_file_local(&frozen, Duration::from_secs(5))
        .unwrap();
    let mut second = primary(&runtime);
    let owner = service
        .publication_provider
        .current()
        .expect("native publication owner");
    let flights = owner.metadata_sync_flights();
    let mut peers = config.replica_node_ids.clone();
    peers.sort();
    let mark = runtime
        .rpc_observations()
        .iter()
        .map(|e| e.call_id)
        .max()
        .unwrap_or(0);
    let gate = hold_replies(&runtime, &config, &peers[0]);
    std::thread::scope(|scope| {
        let task = scope.spawn(|| service.posix_sync_file_local(&frozen, Duration::from_secs(5)));
        let until = Instant::now() + Duration::from_secs(3);
        let reached = loop {
            let seen: std::collections::BTreeSet<_> = runtime
                .rpc_observations()
                .into_iter()
                .filter(|e| {
                    e.call_id > mark
                        && e.source == config.primary_node_id
                        && e.route == crate::DFS_METADATA_JOURNAL_BATCH_ROUTE
                        && e.dfs_request_id
                            .as_deref()
                            .is_some_and(|id| id.starts_with("dfs_sync_probe_"))
                })
                .map(|e| e.target)
                .collect();
            if peers.iter().all(|node| seen.contains(node)) {
                break true;
            }
            if Instant::now() >= until || task.is_finished() {
                break false;
            }
            std::thread::yield_now();
        };
        let joined = scope.spawn(|| second.posix_sync_file_local(&frozen, Duration::from_secs(5)));
        while flights.live_waiters() == 0 && Instant::now() < until && !joined.is_finished() {
            std::thread::yield_now();
        }
        let waiters = flights.live_waiters();
        release(&gate);
        let cert = task.join().unwrap().unwrap();
        let also = joined.join().unwrap().unwrap();
        assert_eq!(
            waiters, 1,
            "same prefix and membership join the outstanding proof"
        );
        assert_eq!(also.metadata_certificate, cert.metadata_certificate);
        assert_eq!(cert.target, frozen);
        assert_eq!(cert.metadata_certificate.acknowledged_node_ids.len(), 3);
        assert!(
            reached,
            "both members must receive a probe while the first reply is held"
        );
    });
    runtime.clear_faults();
    drop(second);
    drop(service);
    runtime.close().unwrap();
}
