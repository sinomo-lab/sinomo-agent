//! Ordinary routed overwrite keeps the configured W/N and exact target set.
//! Fresh native identities only; retained Docker operations are never replayed.
#![cfg(feature = "test-support")]

use sinomo_core::DfsObjectPolicy;
use sinomo_dfs::test_support::DfsTestRuntime;
use sinomo_dfs::{DfsPlacementPolicy, DfsPlacementScope, DfsWriteOptions};
use std::collections::BTreeSet;
use std::time::{Duration, Instant};

#[test]
fn routed_overwrite_does_not_widen_replication_before_main_planning() {
    let (runtime, nodes) =
        DfsTestRuntime::start_cluster("ordinary-alpha-beta-placement", 6).unwrap();
    let path = "/services/ordinary-alpha-beta/file.txt";
    let primary = runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|shard| shard.shard_id == "services")
        .unwrap()
        .primary_node_id;
    let historical = nodes.iter().find(|node| **node != primary).unwrap().clone();
    let selected = nodes
        .iter()
        .filter(|node| **node != historical && **node != primary)
        .take(2)
        .cloned()
        .collect::<Vec<_>>();
    let requester = selected[1].clone();
    let mut alpha_writer = runtime.client(&historical).unwrap();
    alpha_writer
        .posix_mkdir_all("/services/ordinary-alpha-beta")
        .unwrap();
    let (_, _, policy) = alpha_writer
        .managed_replication_policy_for_path(path, DfsObjectPolicy::ReplicatedEventual, nodes.len())
        .unwrap();
    let alpha = alpha_writer
        .posix_write_file(
            path,
            b"alpha",
            DfsWriteOptions {
                policy,
                replica_candidates: nodes.clone(),
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    drop(alpha_writer);
    let mut placement = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 2).unwrap();
    placement.required_node_ids = selected.clone();
    // The old owner remains eligible, but is not one of the two required nodes.
    // Ordinary Apply supplies actual policy authority; no synthetic snapshot.
    runtime
        .apply_placement_policy(&primary, path, DfsPlacementScope::SelfOnly, placement)
        .unwrap();
    // The requester reads policy from its own node's published configuration;
    // let that publication reach the rule committed on the primary first.
    runtime
        .wait_published_configuration_current(&requester)
        .unwrap();
    let mut writer = runtime.client(&requester).unwrap();
    let (_, _, policy) = writer
        .managed_replication_policy_for_path(path, DfsObjectPolicy::ReplicatedEventual, nodes.len())
        .unwrap();
    assert_eq!(policy.min_commit_replicas, 1);
    assert_eq!(policy.desired_replicas, 2);
    assert_eq!(policy.required_node_ids, selected);
    let client_has_snapshot = writer.placement_snapshot_cache().snapshot().is_some();
    let write_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    writer.set_transport_operation_deadline(Some(write_deadline));
    let beta_result = writer.posix_write_file(
        path,
        b"beta",
        DfsWriteOptions {
            policy: policy.clone(),
            replica_candidates: nodes.clone(),
            wait_for_commit: true,
            foreground_replica_repair: false,
        },
    );
    drop(writer);
    let beta = beta_result.as_ref().ok();
    let owner = runtime.metadata_primary_service(path).unwrap();
    let backend = owner.metadata_backend().unwrap();
    let object = backend.get_object(&alpha.object_id).unwrap().unwrap();
    let version = backend
        .get_version(&object.object_id, &object.version)
        .unwrap()
        .unwrap();
    let history = backend
        .object_write_intent_history(&object.object_id)
        .unwrap();
    let durability = version.content_durability.clone();
    drop(owner);
    let expected = selected.iter().cloned().collect::<BTreeSet<_>>();
    let plan_matches = durability.as_ref().is_some_and(|d| {
        d.policy.min_commit_replicas == 1
            && d.policy.desired_replicas == 2
            && d.desired_replica_node_ids
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>()
                == expected
    });
    let mut ticks = Vec::new();
    let mut receipts = Vec::new();
    let repair_deadline = Instant::now() + Duration::from_secs(60);
    let repair = (|| -> Result<(), String> {
        if !plan_matches {
            return Err("published write plan differs from configured exact targets".into());
        }
        loop {
            let owner = runtime.metadata_primary_service(path)?;
            receipts = owner
                .metadata_backend()
                .unwrap()
                .receipts_for_version(&object.object_id, &object.version)
                .map_err(|error| error.to_string())?;
            drop(owner);
            if selected.iter().all(|node| {
                receipts.iter().any(|r| {
                    &r.replica_node_id == node
                        && r.receipt.object_id == object.object_id
                        && r.receipt.committed_version == object.version
                        && r.content_hash == object.content_hash
                })
            }) {
                return Ok(());
            }
            let remaining = repair_deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err("exact desired membership did not converge within 60 seconds".into());
            }
            let tick = runtime.maintenance_tick(
                &primary,
                remaining.min(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT),
            );
            if ticks.len() == 8 {
                ticks.remove(0);
            }
            ticks.push(format!("{tick:?}"));
            tick?;
        }
    })();
    let replica_bytes = selected
        .iter()
        .map(|node| {
            let bytes = runtime.client(node).and_then(|replica| {
                replica
                    .content()
                    .read_object_version(object.object_id.as_str(), object.version.as_str())
                    .map_err(|error| error.to_string())
            });
            (node.clone(), bytes)
        })
        .collect::<Vec<_>>();
    std::fs::write(
        runtime.data_dir(&primary).root().join("ordinary-alpha-beta-placement.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "historical_owner":historical,"requester":requester,"primary":primary,
            "selected":selected,"requested_policy":policy,"client_has_snapshot":client_has_snapshot,
            "alpha_object":alpha.object_id,"alpha_version":alpha.version,
            "beta_result":format!("{beta_result:?}"),"object":object,"version":version,
            "history":history,"receipts":receipts,"repair_result":repair,"last_ticks":ticks,
            "replica_bytes":replica_bytes,"rpc":format!("{:#?}",runtime.rpc_observations()),
            "qualification":"Fresh alpha/beta via real registered Main, no historical replay, supplied receipt or authority; exact plan plus actual receipt/content checks."
        })).unwrap(),
    ).unwrap();
    let same_object =
        beta.is_some_and(|b| b.object_id == alpha.object_id && b.version != alpha.version);
    runtime.close().unwrap();
    beta_result.expect("ordinary beta write commits under its original caller deadline");
    assert!(
        same_object,
        "ordinary overwrite keeps object and advances version"
    );
    assert_eq!(object.owner_node_id, historical);
    assert_eq!(object.content_size_bytes, Some(4));
    assert_eq!(
        object.content_hash,
        Some(format!("blake3:{}", blake3::hash(b"beta")))
    );
    let durability = durability.expect("published version carries its actual write plan");
    assert_eq!(durability.policy.min_commit_replicas, 1);
    assert_eq!(
        durability.policy.desired_replicas, 2,
        "Main must preserve the configured N through normal maintenance"
    );
    assert_eq!(
        durability
            .desired_replica_node_ids
            .into_iter()
            .collect::<BTreeSet<_>>(),
        expected
    );
    repair.expect("actual exact desired replicas converge under unchanged 60-second bound");
    for (node, bytes) in replica_bytes {
        let (manifest, bytes) =
            bytes.unwrap_or_else(|error| panic!("actual target {node}: {error}"));
        assert_eq!(manifest.size_bytes, 4);
        assert_eq!(Some(manifest.content_hash), object.content_hash);
        assert_eq!(bytes, b"beta");
    }
}
