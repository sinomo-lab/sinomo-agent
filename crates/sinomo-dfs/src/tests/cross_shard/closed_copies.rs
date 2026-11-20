//! A node that holds a closed copy of one shard keeps serving every node-wide
//! path on its other shards.

use super::namespace_counts::{close_shard_for_conversion, replace_shard_storage};
use super::*;

/// The repair index of `shard` in `node`'s own store.
fn repair_needs(
    runtime: &crate::test_runtime::DfsTestRuntime,
    node: &NodeId,
    shard: &str,
) -> Vec<(DfsObjectId, DfsVersion, NodeId)> {
    runtime
        .node_metadata(node)
        .expect("node metadata store")
        .repair_index_page(&DfsMetadataShardId::new(shard), None, 64)
        .expect("node repair index")
        .needs
        .into_iter()
        .map(|(key, _)| key)
        .collect()
}

/// `owner` is the primary of root and of users, and a replica of services.
/// Closing users leaves `owner` with a copy it cannot open. Every node-wide
/// path the node runs for its open shards then serves: hardlink scans, writes
/// with their intent and receipt scans, replica transfer in both directions,
/// full maintenance passes with a repair of root, reconciliation and status.
/// Once users reopens, the node serves it again.
#[test]
fn a_node_holding_a_closed_shard_copy_serves_every_node_wide_path() {
    let mut cluster = sifs_two_primary_cluster("native-closed-copy-node-wide");
    let owner = cluster.users_primary_node.clone();
    let services_node = cluster.services_primary_node.clone();
    let replica_node = cluster.replica_node.clone();
    let nodes = [owner.clone(), services_node.clone(), replica_node.clone()];
    let shards = cluster._runtime.metadata_shards().unwrap();
    let config = |shard: &str| {
        shards
            .iter()
            .find(|config| config.shard_id == shard)
            .unwrap()
            .clone()
    };
    assert_eq!(config("root").primary_node_id, owner);
    assert_eq!(config("users").primary_node_id, owner);
    assert_eq!(config("services").primary_node_id, services_node);
    assert!(config("services").replica_node_ids.contains(&owner));
    assert!(config("root").replica_node_ids.contains(&replica_node));
    let options = || DfsWriteOptions::replicated_eventual(vec![replica_node.clone()]);
    let held = "/users/closed-copy-held";
    let held_renamed = "/users/closed-copy-held-renamed";
    cluster
        .replica
        .posix_write_file(held, b"users bytes", options())
        .unwrap();

    let mut kv = cluster._runtime.configuration_publisher(&owner);
    let (active, converting) =
        close_shard_for_conversion(&cluster._runtime, kv.as_mut(), &nodes, "users");
    cluster.replica.single_attempt_for_test(true);
    let refused = cluster.replica.posix_rename(held, held_renamed);
    cluster.replica.single_attempt_for_test(false);
    assert!(
        matches!(&refused, Err(error)
            if error.to_string().contains("metadata shard storage is converting")),
        "the closed shard itself still refuses: {refused:?}"
    );

    // Hardlink scans within root: link, unlink the canonical so its alias is
    // found and promoted, then unlink the alias.
    let canonical = "/closed-copy-canonical";
    let alias = "/closed-copy-alias";
    cluster
        .replica
        .posix_write_file(canonical, b"linked bytes", options())
        .unwrap();
    cluster.replica.posix_hardlink(canonical, alias).unwrap();
    assert_eq!(
        cluster
            .replica
            .posix_stat_distributed(canonical)
            .unwrap()
            .link_count,
        2
    );
    cluster.replica.posix_unlink(canonical).unwrap();
    assert_eq!(
        cluster.replica.posix_read_file_distributed(alias).unwrap(),
        b"linked bytes"
    );
    cluster.replica.posix_unlink(alias).unwrap();
    assert!(node_store_object_at(&cluster._runtime, &owner, alias).is_none());

    // Writes whose intents and receipts the root primary scans, replicated
    // from `owner` to its root replicas and to `owner` from services.
    let written = "/closed-copy-written";
    let replicated = "/services/closed-copy-replicated";
    cluster
        .replica
        .posix_write_file(written, b"root bytes", options())
        .unwrap();
    cluster
        .replica
        .posix_write_file(replicated, b"services bytes", options())
        .unwrap();
    sifs_two_primary_publish_until(
        &mut cluster,
        "replica transfer beside a closed copy",
        |cluster| {
            node_store_object_at(&cluster._runtime, &cluster.replica_node, written).is_some()
                && node_store_object_at(&cluster._runtime, &cluster.services_primary_node, written)
                    .is_some()
                && node_store_object_at(&cluster._runtime, &cluster.users_primary_node, replicated)
                    .is_some()
        },
    );

    // A repair of root: every copy for services_node is refused until the
    // faults clear, so the write commits below its desired copies with a
    // repair need that the owner's executor serves once the routes work
    // again. The repair route is refused as well, so the need is still
    // there when it is read. A rule refuses at most 32 calls, so each route
    // gets several.
    for route in [
        crate::DFS_DATA_STAGE_BLOB_ROUTE,
        crate::DFS_DATA_REPAIR_REPLICA_ROUTE,
    ] {
        for _ in 0..4 {
            cluster
                ._runtime
                .install_fault(crate::test_support::DfsTestFaultRule {
                    source: None,
                    target: Some(services_node.clone()),
                    route: Some(route.into()),
                    call_id: None,
                    origin: None,
                    remaining_hits: 32,
                    action: crate::test_support::DfsTestFaultAction::BudgetExhausted,
                });
        }
    }
    let below_n = cluster
        .replica
        .posix_write_file(
            "/closed-copy-repaired",
            &(0..48 * 1024 + 5)
                .map(|index| (index % 251) as u8)
                .collect::<Vec<_>>(),
            // Every node is a candidate and N counts all three, so with
            // services_node refused at most two copies exist wherever the
            // write places them.
            DfsWriteOptions {
                replica_candidates: vec![
                    replica_node.clone(),
                    services_node.clone(),
                    owner.clone(),
                ],
                policy: DfsReplicationPolicy::new(1, 3).unwrap(),
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    let need = repair_needs(&cluster._runtime, &owner, "root")
        .into_iter()
        .find(|(object_id, version, _)| {
            *object_id == below_n.object_id && *version == below_n.version
        })
        .expect("the write left a root repair need");
    cluster._runtime.clear_faults();
    sifs_two_primary_publish_until(&mut cluster, "the root need is repaired", |cluster| {
        !repair_needs(&cluster._runtime, &cluster.users_primary_node, "root").contains(&need)
    });

    // Reconciliation, then another full maintenance pass.
    cluster
        ._runtime
        .request_repair_reconciliation(&owner)
        .unwrap();
    let tick = cluster
        ._runtime
        .maintenance_tick(&owner, sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
        .unwrap();
    assert!(tick.outcome.is_ok(), "{:?}", tick.outcome);

    // Status of the node, as an operator reads it.
    let status = cluster
        ._runtime
        .rpc_request(
            &replica_node,
            &owner,
            crate::dfs_status_rpc_request().unwrap(),
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
        )
        .unwrap();
    let status = crate::decode_dfs_status_rpc_response(&status).unwrap();
    for shard in ["root", "services"] {
        assert!(
            status
                .metadata_shards
                .iter()
                .any(|status| status.shard_id.0 == shard),
            "{shard} reports status beside the closed copy: {status:?}"
        );
    }

    // Reopened, the shard serves again, and so does the node's maintenance.
    replace_shard_storage(
        &cluster._runtime,
        kv.as_mut(),
        &nodes,
        &converting,
        active.value.clone(),
    );
    cluster.replica.posix_rename(held, held_renamed).unwrap();
    assert!(node_store_object_at(&cluster._runtime, &owner, held_renamed).is_some());
    let tick = cluster
        ._runtime
        .maintenance_tick(&owner, sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
        .unwrap();
    assert!(tick.outcome.is_ok(), "{:?}", tick.outcome);
    drop(kv);
    cluster.close();
}

/// services is closed on its primary, a node other than the writer's. A
/// write committed from the writer's node into users still completes: its
/// post-commit object lookup fans out to every shard's primary, and the
/// closed shard's refusal means only that it cannot answer now, while users
/// answers with the live record. A lookup only the closed shard could answer
/// is retryable `Busy`, never absence, and answers once services reopens.
#[test]
fn a_shard_closed_on_another_node_leaves_authority_lookups_serving() {
    let mut cluster = sifs_two_primary_cluster("native-closed-copy-remote-fan-out");
    let services_node = cluster.services_primary_node.clone();
    let writer = cluster.replica_node.clone();
    let nodes = [
        cluster.users_primary_node.clone(),
        services_node.clone(),
        writer.clone(),
    ];
    let shards = cluster._runtime.metadata_shards().unwrap();
    let primary = |shard: &str| {
        shards
            .iter()
            .find(|config| config.shard_id == shard)
            .unwrap()
            .primary_node_id
            .clone()
    };
    assert_eq!(primary("services"), services_node);
    assert_ne!(primary("users"), services_node);
    assert_ne!(writer, services_node);
    let options = || DfsWriteOptions::replicated_eventual(vec![writer.clone()]);
    let fs = sifs_file_system_id();
    let held = "/services/remote-fan-out-held";
    cluster
        .replica
        .posix_write_file(held, b"services bytes", options())
        .unwrap();
    let held = cluster.replica.posix_stat_distributed(held).unwrap();

    let mut kv = cluster._runtime.configuration_publisher(&writer);
    let (active, converting) =
        close_shard_for_conversion(&cluster._runtime, kv.as_mut(), &nodes, "services");
    let written = "/users/remote-fan-out-written";
    let stat = cluster
        .replica
        .posix_write_file(written, b"users bytes", options())
        .unwrap();
    assert_eq!(
        cluster
            .replica
            .posix_stat_distributed(written)
            .unwrap()
            .object_id,
        stat.object_id
    );
    assert_eq!(
        cluster
            .replica
            .posix_read_file_distributed(written)
            .unwrap(),
        b"users bytes"
    );
    let current = cluster
        .replica
        .current_object_from_authority(&fs, &stat.object_id)
        .unwrap()
        .expect("users answers for its live object");
    assert_eq!(current.path, written);
    let undecided = cluster
        .replica
        .current_object_from_authority(&fs, &held.object_id);
    assert!(
        matches!(&undecided, Err(DfsError::Busy { .. })),
        "only the closed shard could hold this object: {undecided:?}"
    );

    replace_shard_storage(
        &cluster._runtime,
        kv.as_mut(),
        &nodes,
        &converting,
        active.value.clone(),
    );
    let reopened = cluster
        .replica
        .current_object_from_authority(&fs, &held.object_id)
        .unwrap()
        .expect("the reopened shard answers for its object");
    assert_eq!(reopened.object_id, held.object_id);
    drop(kv);
    cluster.close();
}
