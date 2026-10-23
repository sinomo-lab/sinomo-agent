//! Content accounting only: these tests do not grant metadata takeover authority
//! or establish real-process/transport/disk-loss availability.

use sinomo_core::node::NodeId;
use sinomo_dfs::{
    DfsDistributedError, DfsFileSystemId, DfsObjectId, DfsReplicationPolicy, DfsVersion,
    DfsWriteIntentState, DistributedEventualCoordinator, DistributedWriteIntent, LocalContentStore,
    validate_replication_plan,
};

fn node(name: &str) -> NodeId {
    NodeId::from_legacy_named(name).unwrap()
}

fn intent(
    coordinator: &mut DistributedEventualCoordinator,
    object: &str,
    policy: DfsReplicationPolicy,
) -> DistributedWriteIntent {
    coordinator
        .create_intent_at_epoch_with_parent_touch_and_id(
            DfsFileSystemId::from_legacy_named("availability-content-fs").unwrap(),
            DfsObjectId::from_legacy_named(object).unwrap(),
            None,
            DfsVersion::from_legacy_named("availability-content-version").unwrap(),
            node("a"),
            7,
            vec![node("a"), node("b"), node("c")],
            policy,
            false,
            None,
            Some(format!("original-{object}")),
            Some(format!("caller-{object}")),
            None,
        )
        .unwrap()
}

#[test]
fn exact_a_c_version_keeps_c_as_candidate_when_b_has_no_content() {
    let root = sinomo_testdir::TestDir::new("availability-exact-surviving-copy");
    let store_a = LocalContentStore::new(root.join("a")).unwrap();
    let store_b = LocalContentStore::new(root.join("b")).unwrap();
    let store_c = LocalContentStore::new(root.join("c")).unwrap();
    let mut coordinator = DistributedEventualCoordinator::new();
    let original = intent(
        &mut coordinator,
        "a-c-exact-version",
        DfsReplicationPolicy::new(2, 3).unwrap(),
    );
    let bytes = b"committed exact version held by A and C";
    let manifest = store_a
        .put_object_version(
            original.object_id.as_str(),
            original.target_version.as_str(),
            bytes,
        )
        .unwrap();
    let c_manifest = store_c
        .put_object_version(
            original.object_id.as_str(),
            original.target_version.as_str(),
            bytes,
        )
        .unwrap();
    assert_eq!(c_manifest.content_hash, manifest.content_hash);
    assert!(!store_b.has_blob(&manifest.content_hash).unwrap());
    assert!(
        store_b
            .read_manifest(
                original.object_id.as_str(),
                original.target_version.as_str()
            )
            .is_err()
    );
    for target in [node("a"), node("c")] {
        coordinator
            .record_durable_receipt(&original.intent_id, target)
            .unwrap();
    }
    assert!(
        coordinator
            .finalize_commit_if_ready(&original.intent_id)
            .unwrap()
    );
    let jobs = coordinator.repair_jobs().to_vec();
    let projected = coordinator
        .replica_availability(
            &original.intent_id,
            &[node("b"), node("c")],
            &[node("b"), node("c")],
        )
        .unwrap();
    assert_eq!(
        projected.durable_replica_node_ids,
        vec![node("a"), node("c")]
    );
    assert_eq!(
        projected.reachable_durable_replica_node_ids,
        vec![node("c")]
    );
    assert_eq!(projected.missing_receipt_node_ids, vec![node("b")]);
    let (opened, read) = store_c
        .read_object_version(
            original.object_id.as_str(),
            original.target_version.as_str(),
        )
        .unwrap();
    assert_eq!(opened, c_manifest);
    assert_eq!(read, bytes);
    assert_eq!(coordinator.repair_jobs(), jobs);
}

#[test]
fn survivor_receipts_retain_n_and_exact_deficit_across_coordinator_restore() {
    let root = sinomo_testdir::TestDir::new("availability-content-accounting");
    let store_b = LocalContentStore::new(root.join("b")).unwrap();
    let store_c = LocalContentStore::new(root.join("c")).unwrap();
    let mut coordinator = DistributedEventualCoordinator::new();
    let original = intent(
        &mut coordinator,
        "survivor-object",
        DfsReplicationPolicy::new(2, 3).unwrap(),
    );
    let bytes = b"same exact immutable bytes on survivors B and C";
    let manifest_b = store_b
        .put_object_version(
            original.object_id.as_str(),
            original.target_version.as_str(),
            bytes,
        )
        .unwrap();
    let manifest_c = store_c
        .put_object_version(
            original.object_id.as_str(),
            original.target_version.as_str(),
            bytes,
        )
        .unwrap();
    assert_eq!(manifest_b.content_hash, manifest_c.content_hash);

    // Reachability and writable-target observations alone supply no receipts.
    let initial = coordinator
        .replica_availability(
            &original.intent_id,
            &[node("b"), node("c")],
            &[node("b"), node("c")],
        )
        .unwrap();
    assert_eq!(initial.missing_commit_receipts(), 2);
    assert!(initial.durable_replica_node_ids.is_empty());
    assert_eq!(
        initial.missing_receipt_node_ids,
        original.desired_replica_nodes
    );
    assert!(
        !coordinator
            .finalize_commit_if_ready(&original.intent_id)
            .unwrap()
    );

    coordinator
        .record_durable_receipt(&original.intent_id, node("b"))
        .unwrap();
    assert!(
        !coordinator
            .finalize_commit_if_ready(&original.intent_id)
            .unwrap()
    );
    coordinator
        .record_durable_receipt(&original.intent_id, node("c"))
        .unwrap();
    coordinator
        .record_durable_receipt(&original.intent_id, node("c"))
        .unwrap();
    assert!(
        coordinator
            .finalize_commit_if_ready(&original.intent_id)
            .unwrap()
    );
    let committed = coordinator.intent(&original.intent_id).unwrap().clone();
    assert_eq!(committed.state, DfsWriteIntentState::Committed);
    assert_eq!(committed.policy, original.policy);
    assert_eq!(
        committed.desired_replica_nodes,
        original.desired_replica_nodes
    );
    assert_eq!(coordinator.durable_receipt_count(&original.intent_id), 2);
    assert_eq!(coordinator.repair_jobs().len(), 1);
    assert_eq!(
        coordinator.repair_jobs()[0].missing_replica_node_id,
        node("a")
    );

    // Rebuild from the same recorded intent/receipt identities. This is not a
    // new request, authority election or a simulation of losing A's process.
    let mut restored = DistributedEventualCoordinator::new();
    restored.restore_intent(committed.clone()).unwrap();
    for receipt in coordinator.receipts_for_intent(&original.intent_id) {
        restored
            .restore_durable_receipt(&receipt.intent_id, receipt.replica_node_id.clone())
            .unwrap();
    }
    assert_eq!(restored.intent(&original.intent_id), Some(&committed));
    assert_eq!(restored.repair_jobs(), coordinator.repair_jobs());

    // Only C is reachable and it accepts no new content. Reading this exact
    // committed version still has one candidate despite W=2.
    let observed = restored
        .replica_availability(&original.intent_id, &[node("b")], &[node("c")])
        .unwrap();
    assert_eq!(observed.missing_commit_receipts(), 0);
    assert_eq!(observed.observed_writable_replica_node_ids, vec![node("b")]);
    assert_eq!(observed.reachable_durable_replica_node_ids, vec![node("c")]);
    assert_eq!(observed.missing_receipt_node_ids, vec![node("a")]);
    assert_eq!(store_c.read_blob(&manifest_c.content_hash).unwrap(), bytes);
    assert_eq!(restored.repair_jobs(), coordinator.repair_jobs());

    // Reconnect is an observation, never a receipt or implicit deficit clear.
    let rejoined = restored
        .replica_availability(
            &original.intent_id,
            &[node("a"), node("b"), node("c")],
            &[node("a"), node("b"), node("c")],
        )
        .unwrap();
    assert_eq!(rejoined.missing_receipt_node_ids, vec![node("a")]);
    assert_eq!(restored.repair_jobs().len(), 1);
    let store_a = LocalContentStore::new(root.join("a")).unwrap();
    let repaired = store_a
        .put_object_version(
            original.object_id.as_str(),
            original.target_version.as_str(),
            bytes,
        )
        .unwrap();
    assert_eq!(repaired.content_hash, manifest_b.content_hash);
    restored
        .record_durable_receipt(&original.intent_id, node("a"))
        .unwrap();
    assert!(restored.repair_jobs().is_empty());
}

#[test]
fn terminal_projection_and_observation_order_never_reopen_or_reorder_an_intent() {
    let mut coordinator = DistributedEventualCoordinator::new();
    let desired = vec![node("c"), node("a"), node("b")];
    let original = coordinator
        .create_intent(
            DfsFileSystemId::from_legacy_named("availability-order-fs").unwrap(),
            DfsObjectId::from_legacy_named("availability-order-object").unwrap(),
            None,
            DfsVersion::from_legacy_named("availability-order-version").unwrap(),
            node("b"),
            desired.clone(),
            DfsReplicationPolicy::new(1, 3).unwrap(),
        )
        .unwrap();
    coordinator
        .record_durable_receipt(&original.intent_id, node("a"))
        .unwrap();
    let aborted = coordinator.abort_intent(&original.intent_id).unwrap();
    let observed = coordinator
        .replica_availability(
            &original.intent_id,
            &[node("b"), node("b"), node("outside"), node("c")],
            &[node("outside"), node("a"), node("a")],
        )
        .unwrap();
    assert_eq!(observed.intent, aborted);
    assert_eq!(observed.intent.desired_replica_nodes, desired);
    assert_eq!(observed.intent.state, DfsWriteIntentState::Aborted);
    // A complete content threshold is descriptive even for an aborted intent.
    assert_eq!(observed.missing_commit_receipts(), 0);
    assert_eq!(observed.durable_replica_node_ids, vec![node("a")]);
    assert_eq!(observed.reachable_durable_replica_node_ids, vec![node("a")]);
    assert_eq!(
        observed.observed_writable_replica_node_ids,
        vec![node("c"), node("b")]
    );
    assert_eq!(
        observed.missing_receipt_node_ids,
        vec![node("c"), node("b")]
    );
    assert_eq!(coordinator.intent(&original.intent_id), Some(&aborted));
    assert!(coordinator.repair_jobs().is_empty());
    assert!(matches!(
        coordinator.record_durable_receipt(&original.intent_id, node("b")),
        Err(DfsDistributedError::IntentClosed { .. })
    ));
}

#[test]
fn observations_preserve_confirmed_policy_constraints_and_membership() {
    let mut coordinator = DistributedEventualCoordinator::new();
    let policy = DfsReplicationPolicy::confirmed(3)
        .unwrap()
        .with_placement_constraints(
            vec![node("a")],
            vec![node("outside")],
            2,
            [
                (node("a"), "zone-a".into()),
                (node("b"), "zone-b".into()),
                (node("c"), "zone-b".into()),
            ]
            .into_iter()
            .collect(),
        )
        .unwrap();
    let original = intent(&mut coordinator, "confirmed-object", policy);
    assert!(
        validate_replication_plan(&[node("b"), node("c"), node("outside")], &original.policy,)
            .is_err()
    );
    for target in [node("b"), node("c")] {
        coordinator
            .record_durable_receipt(&original.intent_id, target)
            .unwrap();
    }
    let observed = coordinator
        .replica_availability(
            &original.intent_id,
            &[node("b"), node("b"), node("c"), node("outside")],
            &[node("b"), node("c"), node("outside")],
        )
        .unwrap();
    assert_eq!(observed.intent, original);
    assert_eq!(observed.missing_commit_receipts(), 1);
    assert_eq!(
        observed.observed_writable_replica_node_ids,
        vec![node("b"), node("c")]
    );
    assert_eq!(
        observed.reachable_durable_replica_node_ids,
        vec![node("b"), node("c")]
    );
    assert_eq!(observed.missing_receipt_node_ids, vec![node("a")]);
    assert!(
        !coordinator
            .finalize_commit_if_ready(&original.intent_id)
            .unwrap()
    );
    assert!(coordinator.repair_jobs().is_empty());
    assert!(matches!(
        coordinator.record_durable_receipt(&original.intent_id, node("outside")),
        Err(DfsDistributedError::UnexpectedReplica { .. })
    ));
    assert!(matches!(
        coordinator.replica_availability("unknown", &[], &[]),
        Err(DfsDistributedError::UnknownIntent { .. })
    ));
}

#[test]
fn a_only_w1_content_has_no_reachable_holder_without_blocking_other_content() {
    let mut coordinator = DistributedEventualCoordinator::new();
    let a_only = intent(
        &mut coordinator,
        "a-only",
        DfsReplicationPolicy::new(1, 3).unwrap(),
    );
    coordinator
        .record_durable_receipt(&a_only.intent_id, node("a"))
        .unwrap();
    assert!(
        coordinator
            .finalize_commit_if_ready(&a_only.intent_id)
            .unwrap()
    );
    let unrelated = intent(
        &mut coordinator,
        "unrelated",
        DfsReplicationPolicy::new(2, 3).unwrap(),
    );
    for target in [node("b"), node("c")] {
        coordinator
            .record_durable_receipt(&unrelated.intent_id, target)
            .unwrap();
    }
    assert!(
        coordinator
            .finalize_commit_if_ready(&unrelated.intent_id)
            .unwrap()
    );
    let reachable = [node("b"), node("c")];
    let unavailable = coordinator
        .replica_availability(&a_only.intent_id, &reachable, &reachable)
        .unwrap();
    assert_eq!(unavailable.durable_replica_node_ids, vec![node("a")]);
    assert_eq!(unavailable.missing_commit_receipts(), 0);
    assert!(unavailable.reachable_durable_replica_node_ids.is_empty());
    let available = coordinator
        .replica_availability(&unrelated.intent_id, &reachable, &reachable)
        .unwrap();
    assert_eq!(available.reachable_durable_replica_node_ids, reachable);
    assert_eq!(available.missing_receipt_node_ids, vec![node("a")]);
    // Route flaps do not erase A's recorded copy or change either durable plan.
    for reachable in [&[][..], &[node("a")][..], &reachable[..]] {
        let projected = coordinator
            .replica_availability(&a_only.intent_id, &[], reachable)
            .unwrap();
        assert_eq!(projected.durable_replica_node_ids, vec![node("a")]);
        assert_eq!(projected.intent.policy.desired_replicas, 3);
        assert_eq!(
            projected.missing_receipt_node_ids,
            vec![node("b"), node("c")]
        );
    }
}
