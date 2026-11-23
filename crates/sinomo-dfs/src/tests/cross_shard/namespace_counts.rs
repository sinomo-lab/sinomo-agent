use super::*;

#[test]
fn cross_shard_hardlink_count_successor_survives_commit_reply_loss_and_replay() {
    let mut cluster = sifs_two_primary_cluster("native-counted-hardlink-commit-gap");
    let canonical = "/users/counted-canonical";
    let alias = "/services/counted-alias";
    cluster
        .replica
        .posix_write_file(
            canonical,
            b"counted bytes",
            DfsWriteOptions::replicated_eventual(vec![cluster.replica_node.clone()]),
        )
        .unwrap();
    assert_eq!(
        cluster
            .replica
            .posix_stat_distributed(canonical)
            .unwrap()
            .link_count,
        1
    );
    let operation_id = sinomo_core::Id::generate().to_string();
    let operation = DfsNamespaceOperation::Hardlink {
        existing_path: canonical.into(),
        link_path: alias.into(),
    };
    let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
    let hook = crate::posix::install_hardlink_count_after_commit_test_hook(canonical, move || {
        entered_tx.send(()).unwrap();
        release_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("bounded actual count interception");
        Err(DfsPosixError::Dfs(DfsError::Network {
            message: "count successor committed before source progress; reply lost".into(),
        }))
    });
    let mut original_successor = None;
    std::thread::scope(|scope| {
        let requester = &mut cluster.replica;
        let id = operation_id.clone();
        let op = operation.clone();
        let primary = cluster.users_primary_node.clone();
        let call = scope
            .spawn(move || requester.route_namespace_operation_with_request_id(id, primary, op));
        let entered = entered_rx.recv_timeout(Duration::from_secs(5));
        let observation = (|| -> std::result::Result<_, String> {
            entered.as_ref().map_err(|error| error.to_string())?;
            let cut =
                hardlink_read_set_snapshot(&cluster._runtime, &cluster.users_primary_node, "users")
                    .map_err(|error| error.to_string())?;
            let row = cut
                .tables
                .objects
                .iter()
                .find(|object| object.path == canonical)
                .ok_or("canonical object missing")?
                .clone();
            let job = cut
                .tables
                .namespace_jobs
                .iter()
                .find(|job| {
                    job.kind == DfsNamespaceJobKind::HardlinkAcrossShardPrimaries
                        && job
                            .origin
                            .as_ref()
                            .is_some_and(|origin| origin.request_id == operation_id)
                })
                .ok_or("original counted creation job missing")?;
            assert_eq!(job.status, DfsNamespaceJobStatus::Running);
            let sealed = job
                .pending_frames
                .iter()
                .find_map(|frame| match frame {
                    DfsNamespaceJobFrame::CrossShardHardlinkTargetFenceCommit {
                        committed_target,
                        ..
                    } => Some(committed_target),
                    _ => None,
                })
                .ok_or("count completion must remain durably pending")?;
            assert_eq!(&row, sealed);
            assert_eq!(row.namespace_link_count, Some(2));
            Ok(row)
        })();
        release_tx.send(()).ok();
        let _outcome = call.join().unwrap();
        entered.expect("real target count commit reached");
        original_successor = Some(observation.unwrap());
    });
    drop(hook);
    // Restart both real authorities with their original durable job/receipts.
    let users = cluster.users_primary_node.clone();
    let services = cluster.services_primary_node.clone();
    drop(cluster.users_primary);
    drop(cluster.services_primary);
    cluster._runtime.stop_node(&services).unwrap();
    cluster._runtime.stop_node(&users).unwrap();
    cluster._runtime.reopen_node(users.clone()).unwrap();
    cluster._runtime.reopen_node(services.clone()).unwrap();
    cluster.users_primary = cluster._runtime.client(&users).unwrap();
    cluster.services_primary = cluster._runtime.client(&services).unwrap();
    cluster
        .replica
        .route_namespace_operation_with_request_id(
            operation_id.clone(),
            users.clone(),
            operation.clone(),
        )
        .unwrap();
    let after = cluster.replica.posix_stat_distributed(canonical).unwrap();
    assert_eq!(after.link_count, 2);
    assert_eq!(after.version, original_successor.unwrap().version);
    assert_eq!(
        cluster
            .replica
            .posix_stat_distributed(alias)
            .unwrap()
            .object_id,
        after.object_id
    );
    cluster
        .replica
        .posix_hardlink(canonical, "/services/counted-later-alias")
        .unwrap();
    assert_eq!(
        cluster
            .replica
            .posix_stat_distributed(canonical)
            .unwrap()
            .link_count,
        3
    );
    cluster
        .replica
        .route_namespace_operation_with_request_id(operation_id, users, operation)
        .unwrap();
    assert_eq!(
        cluster
            .replica
            .posix_stat_distributed(canonical)
            .unwrap()
            .link_count,
        3,
        "old exact replay cannot reapply a delta or restore its older count"
    );
    cluster.close();
}

#[test]
fn legacy_hardlink_count_frame_is_retained_without_adopting_an_alias() {
    let mut cluster = sifs_two_primary_cluster("native-legacy-hardlink-count-refusal");
    let canonical = "/users/legacy-count-canonical";
    let alias_path = "/services/legacy-count-alias";
    cluster
        .replica
        .posix_write_file(
            canonical,
            b"legacy original",
            DfsWriteOptions::replicated_eventual(vec![cluster.replica_node.clone()]),
        )
        .unwrap();
    let current =
        node_store_object_at(&cluster._runtime, &cluster.users_primary_node, canonical).unwrap();
    let mut alias = current.clone();
    alias.object_id = DfsObjectId::generate();
    alias.path = alias_path.into();
    alias.kind = DfsObjectKind::Hardlink;
    alias.namespace_link_count = None;
    alias.link_target_object_id = Some(current.object_id.clone());
    alias.content_hash = None;
    alias.content_size_bytes = None;
    alias.current_locations.clear();
    let now = time::OffsetDateTime::now_utc();
    let job = DfsNamespaceJobRecord {
        job_id: "original-legacy-hardlink-without-count-successor".into(),
        file_system_id: current.file_system_id.clone(),
        origin: None,
        kind: DfsNamespaceJobKind::HardlinkAcrossShardPrimaries,
        status: DfsNamespaceJobStatus::Running,
        root_path: canonical.into(),
        target_path: Some(alias_path.into()),
        pending_frames: vec![
            DfsNamespaceJobFrame::CrossShardHardlinkComplete {
                target_object_id: current.object_id.clone(),
            },
            DfsNamespaceJobFrame::CrossShardHardlinkAdopt {
                alias,
                target_object_id: current.object_id.clone(),
                destination_primary_node_id: cluster.services_primary_node.clone(),
            },
        ],
        processed_object_count: 0,
        created_at: now,
        updated_at: now,
        completed_at: None,
        failure_reason: None,
    };
    let mut owner = cluster
        ._runtime
        .metadata_primary_service(canonical)
        .unwrap();
    owner.persist_namespace_job(&job).unwrap();
    let before = owner
        .metadata_backend()
        .unwrap()
        .metadata_prefix_proof(&DfsMetadataShardId::new("users"))
        .unwrap();
    assert!(
        owner
            .run_cross_shard_namespace_job(job.clone(), 16)
            .is_err()
    );
    assert_eq!(owner.namespace_job(&job.job_id).unwrap(), Some(job));
    assert_eq!(
        owner
            .metadata_backend()
            .unwrap()
            .metadata_prefix_proof(&DfsMetadataShardId::new("users"))
            .unwrap(),
        before
    );
    assert!(
        node_store_object_at(
            &cluster._runtime,
            &cluster.services_primary_node,
            alias_path
        )
        .is_none()
    );
    assert_eq!(
        node_store_object_at(&cluster._runtime, &cluster.users_primary_node, canonical).unwrap(),
        current
    );
    drop(owner);
    cluster.close();
}

#[test]
fn cross_shard_directory_move_updates_coowned_parents_once_and_carries_subtree_counts() {
    let mut cluster = sifs_two_primary_cluster("native-counted-directory-move");
    let old_parent = "/users/count-source-parent";
    let new_parent = "/services/count-target-parent";
    let from = "/users/count-source-parent/tree";
    let to = "/services/count-target-parent/tree";
    cluster
        .replica
        .posix_mkdir_all(&format!("{from}/child"))
        .unwrap();
    cluster.replica.posix_mkdir_all(new_parent).unwrap();
    assert_eq!(
        cluster
            .replica
            .posix_stat_distributed(old_parent)
            .unwrap()
            .link_count,
        3
    );
    assert_eq!(
        cluster
            .replica
            .posix_stat_distributed(new_parent)
            .unwrap()
            .link_count,
        2
    );
    assert_eq!(
        cluster
            .replica
            .posix_stat_distributed(from)
            .unwrap()
            .link_count,
        3
    );
    let operation = sinomo_core::Id::generate().to_string();
    cluster
        .replica
        .posix_rename_with_operation_id(from, to, &operation)
        .unwrap();
    assert_eq!(
        cluster
            .replica
            .posix_stat_distributed(old_parent)
            .unwrap()
            .link_count,
        2
    );
    assert_eq!(
        cluster
            .replica
            .posix_stat_distributed(new_parent)
            .unwrap()
            .link_count,
        3
    );
    assert_eq!(
        cluster
            .replica
            .posix_stat_distributed(to)
            .unwrap()
            .link_count,
        3,
        "descendant physical adoption must not increment transported membership again"
    );
    assert_eq!(
        cluster
            .replica
            .posix_stat_distributed(&format!("{to}/child"))
            .unwrap()
            .link_count,
        2
    );
    cluster
        .replica
        .posix_rename_with_operation_id(from, to, &operation)
        .unwrap();
    assert_eq!(
        cluster
            .replica
            .posix_stat_distributed(old_parent)
            .unwrap()
            .link_count,
        2
    );
    assert_eq!(
        cluster
            .replica
            .posix_stat_distributed(new_parent)
            .unwrap()
            .link_count,
        3
    );
    cluster.close();
}

#[test]
fn cross_shard_canonical_unlink_promotes_exact_alias_and_decrements_once() {
    let mut cluster = sifs_two_primary_cluster("native-counted-unlink-promotion");
    let canonical = "/users/promotion-source";
    let alias = "/services/promotion-alias";
    cluster
        .replica
        .posix_write_file(
            canonical,
            b"promotion bytes",
            DfsWriteOptions::replicated_eventual(vec![cluster.replica_node.clone()]),
        )
        .unwrap();
    cluster.replica.posix_hardlink(canonical, alias).unwrap();
    let before = cluster.replica.posix_stat_distributed(canonical).unwrap();
    assert_eq!(before.link_count, 2);
    let operation = sinomo_core::Id::generate().to_string();
    let expected = DfsNamespaceObjectPrecondition {
        object_id: before.object_id.clone(),
        version: before.version,
    };
    cluster
        .replica
        .posix_unlink_if_generation(canonical, operation.clone(), expected.clone())
        .unwrap();
    let promoted = cluster.replica.posix_stat_distributed(alias).unwrap();
    assert_eq!(promoted.object_id, before.object_id);
    assert_eq!(promoted.link_count, 1);
    cluster
        .replica
        .posix_unlink_if_generation(canonical, operation, expected)
        .unwrap();
    assert_eq!(
        cluster
            .replica
            .posix_stat_distributed(alias)
            .unwrap()
            .link_count,
        1
    );
    assert_eq!(
        cluster.replica.posix_read_file_distributed(alias).unwrap(),
        b"promotion bytes"
    );
    cluster.close();
}

#[test]
fn cross_shard_directory_replacement_preserves_target_parent_membership() {
    let mut cluster = sifs_two_primary_cluster("native-counted-directory-replacement");
    let old_parent = "/users/replacement-source-parent";
    let new_parent = "/services/replacement-target-parent";
    let from = "/users/replacement-source-parent/tree";
    let to = "/services/replacement-target-parent/tree";
    cluster
        .replica
        .posix_mkdir_all(&format!("{from}/child"))
        .unwrap();
    cluster.replica.posix_mkdir_all(to).unwrap();
    let old_target = cluster.replica.posix_stat_distributed(to).unwrap();
    assert_eq!(old_target.link_count, 2);
    assert_eq!(
        cluster
            .replica
            .posix_stat_distributed(old_parent)
            .unwrap()
            .link_count,
        3
    );
    assert_eq!(
        cluster
            .replica
            .posix_stat_distributed(new_parent)
            .unwrap()
            .link_count,
        3
    );
    let operation = sinomo_core::Id::generate().to_string();
    cluster
        .replica
        .posix_rename_with_operation_id(from, to, &operation)
        .unwrap();
    assert_eq!(
        cluster
            .replica
            .posix_stat_distributed(old_parent)
            .unwrap()
            .link_count,
        2
    );
    assert_eq!(
        cluster
            .replica
            .posix_stat_distributed(new_parent)
            .unwrap()
            .link_count,
        3,
        "replacing one empty directory with another changes no target-parent membership"
    );
    let moved = cluster.replica.posix_stat_distributed(to).unwrap();
    assert_ne!(moved.object_id, old_target.object_id);
    assert_eq!(moved.link_count, 3);
    cluster
        .replica
        .posix_rename_with_operation_id(from, to, &operation)
        .unwrap();
    assert_eq!(
        cluster
            .replica
            .posix_stat_distributed(new_parent)
            .unwrap()
            .link_count,
        3
    );
    cluster.close();
}

#[test]
fn concurrent_alias_owners_cannot_reserve_the_same_canonical_count_generation() {
    let mut cluster = sifs_two_primary_cluster("native-count-fence-exclusivity");
    let root = cluster
        ._runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|config| config.shard_id == "root")
        .unwrap();
    let second_owner = cluster.replica_node.clone();
    let replicas = std::iter::once(root.primary_node_id.clone())
        .chain(root.replica_node_ids.iter().cloned())
        .filter(|node| *node != second_owner)
        .collect();
    cluster
        ._runtime
        .reassign_metadata_shard(&root.file_system_id, "root", second_owner.clone(), replicas)
        .unwrap();
    let canonical = "/users/exclusive-count-canonical";
    let first_alias = "/services/exclusive-count-first";
    let second_alias = "/exclusive-count-second";
    cluster
        .replica
        .posix_write_file(
            canonical,
            b"count fence",
            DfsWriteOptions::replicated_eventual(vec![cluster.replica_node.clone()]),
        )
        .unwrap();
    cluster
        .replica
        .posix_hardlink(canonical, first_alias)
        .unwrap();
    cluster
        .replica
        .posix_hardlink(canonical, second_alias)
        .unwrap();
    let before =
        hardlink_read_set_snapshot(&cluster._runtime, &cluster.users_primary_node, "users")
            .unwrap();
    let canonical_before = before
        .tables
        .objects
        .iter()
        .find(|row| row.path == canonical)
        .unwrap()
        .clone();
    assert_eq!(canonical_before.namespace_link_count, Some(3));
    let expected = DfsNamespaceObjectPrecondition {
        object_id: canonical_before.object_id.clone(),
        version: canonical_before.version.clone(),
    };
    let read_aliases = || {
        [
            (
                cluster.services_primary_node.clone(),
                "services",
                first_alias,
            ),
            (second_owner.clone(), "root", second_alias),
        ]
        .into_iter()
        .map(|(owner, shard, path)| {
            hardlink_read_set_snapshot(&cluster._runtime, &owner, shard)
                .unwrap()
                .tables
                .objects
                .into_iter()
                .find(|row| row.path == path)
                .unwrap()
        })
        .collect::<Vec<_>>()
    };
    let aliases_before = read_aliases();
    let make_fence = |source_path: &str| {
        let now = time::OffsetDateTime::now_utc();
        let mut successor = canonical_before.clone();
        successor.namespace_link_count = Some(2);
        successor.version = crate::namespace::next_version(&successor.version);
        successor.changed_at_unix_nanos += 1;
        DfsNamespaceJobRecord {
            job_id: format!("namespace_job_count_fence_{}", sinomo_core::Id::generate()),
            file_system_id: canonical_before.file_system_id.clone(),
            origin: None,
            kind: DfsNamespaceJobKind::HardlinkTargetFenceAcrossShardPrimary,
            status: DfsNamespaceJobStatus::Running,
            root_path: canonical.into(),
            target_path: None,
            pending_frames: vec![DfsNamespaceJobFrame::HardlinkTargetFenceReservation {
                source_path: source_path.into(),
                expected_target: expected.clone(),
                committed_target: successor,
            }],
            processed_object_count: 0,
            created_at: now,
            updated_at: now,
            completed_at: None,
            failure_reason: None,
        }
    };
    // Both owners capture the same real canonical generation before either
    // reservation is installed. Exercise the actual target phase admission,
    // stopping before either source owner is allowed to change its alias.
    let first_fence = make_fence(first_alias);
    let second_fence = make_fence(second_alias);
    let mut first_owner = cluster
        ._runtime
        .client(&cluster.services_primary_node)
        .unwrap();
    let mut second_owner = cluster._runtime.client(&cluster.replica_node).unwrap();
    first_owner
        .route_namespace_operation_with_request_id(
            sinomo_core::Id::generate().to_string(),
            cluster.users_primary_node.clone(),
            DfsNamespaceOperation::InstallHardlinkTargetFence {
                fence: Box::new(first_fence.clone()),
                source_path: first_alias.into(),
            },
        )
        .unwrap();
    let first_cut =
        hardlink_read_set_snapshot(&cluster._runtime, &cluster.users_primary_node, "users")
            .unwrap();
    assert!(
        first_cut
            .tables
            .namespace_jobs
            .iter()
            .any(|job| job == &first_fence)
    );
    let second_result = second_owner.route_namespace_operation_with_request_id(
        sinomo_core::Id::generate().to_string(),
        cluster.users_primary_node.clone(),
        DfsNamespaceOperation::InstallHardlinkTargetFence {
            fence: Box::new(second_fence.clone()),
            source_path: second_alias.into(),
        },
    );
    let after = hardlink_read_set_snapshot(&cluster._runtime, &cluster.users_primary_node, "users")
        .unwrap();
    let canonical_after = after
        .tables
        .objects
        .iter()
        .find(|row| row.path == canonical)
        .unwrap();
    let second_installed = after
        .tables
        .namespace_jobs
        .iter()
        .any(|job| job == &second_fence);
    assert!(
        matches!(
            second_result,
            Err(DfsPosixError::Dfs(DfsError::Busy { .. }))
        ),
        "second owner must stop before alias effects: {second_result:?}; second fence installed={second_installed}; canonical before={canonical_before:?} after={canonical_after:?}"
    );
    assert_eq!(
        read_aliases(),
        aliases_before,
        "rejected reservation changes neither alias"
    );
    assert!(!second_installed);
    assert_eq!(canonical_after, &canonical_before);
    // The original owner may replay its already admitted fence while retaining
    // the original exact successor; it must not block on its own reservation.
    first_owner
        .route_namespace_operation_with_request_id(
            sinomo_core::Id::generate().to_string(),
            cluster.users_primary_node.clone(),
            DfsNamespaceOperation::InstallHardlinkTargetFence {
                fence: Box::new(first_fence),
                source_path: first_alias.into(),
            },
        )
        .unwrap();
}

/// `shard`'s storage record as the control plane holds it now.
pub(super) fn shard_storage_entry(kv: &mut dyn DfsControlPlaneKvClient, shard: &str) -> sinomo_core::kv::KvEntry {
    let key = crate::metadata_shard_storage_key(&sifs_file_system_id(), shard);
    kv.get_authoritative_consistent(&[key])
        .unwrap()
        .pop()
        .unwrap()
        .expect("a born shard carries a storage record")
}

/// Test setup only: replace the exact record `expected` with `value` and wait
/// until every node in `nodes` publishes it. No conversion runs; a Converting
/// record only closes the shard until the original record is restored. `kv`
/// is a node's native settings producer, so a node whose configuration is
/// withheld does not stall the setup.
pub(super) fn replace_shard_storage(
    runtime: &crate::test_runtime::DfsTestRuntime,
    kv: &mut dyn DfsControlPlaneKvClient,
    nodes: &[NodeId],
    expected: &sinomo_core::kv::KvEntry,
    value: Vec<u8>,
) -> sinomo_core::kv::KvEntry {
    let replaced = kv
        .txn(KvTxn {
            compares: vec![sinomo_core::kv::KvCompare::ValueEquals {
                key: expected.key.clone(),
                value: expected.value.clone(),
            }],
            success: vec![sinomo_core::kv::KvOp::Put {
                key: expected.key.clone(),
                value,
                lease_id: None,
            }],
            failure: Vec::new(),
        })
        .unwrap()
        .succeeded;
    assert!(replaced, "the storage record changed under the test");
    for node in nodes {
        runtime.wait_published_configuration_current(node).unwrap();
    }
    let shard = expected.key.rsplit('/').next().unwrap();
    shard_storage_entry(kv, shard)
}

/// Close `shard` as a conversion does: its Active record turns Converting.
/// Returns the Active and the Converting record.
pub(super) fn close_shard_for_conversion(
    runtime: &crate::test_runtime::DfsTestRuntime,
    kv: &mut dyn DfsControlPlaneKvClient,
    nodes: &[NodeId],
    shard: &str,
) -> (sinomo_core::kv::KvEntry, sinomo_core::kv::KvEntry) {
    let config = runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|config| config.shard_id == shard)
        .unwrap();
    let active = shard_storage_entry(kv, shard);
    assert!(
        active
            .decode::<crate::DfsMetadataShardStorage>()
            .unwrap()
            .is_active()
    );
    let converting = replace_shard_storage(
        runtime,
        kv,
        nodes,
        &active,
        sinomo_core::kv::encode_kv_value(&crate::DfsMetadataShardStorage::converting_for_test(
            &config, 1,
        ))
        .unwrap(),
    );
    (active, converting)
}

/// A cluster whose root, services and users shards each have one member, so
/// the root primary that coordinates a move holds no copy of users: users
/// can be closed for conversion while root and services keep serving.
pub(super) fn disjoint_shard_cluster(name: &str) -> (SifsTwoPrimaryCluster, [NodeId; 3]) {
    let cluster = super::parent_counts::foreign_parent_cluster(name);
    let members = |config: &DfsMetadataShardConfig| {
        std::iter::once(config.primary_node_id.clone())
            .chain(config.replica_node_ids.iter().cloned())
            .collect::<Vec<_>>()
    };
    let shards = cluster._runtime.metadata_shards().unwrap();
    let member_of = |shard: &str| {
        members(shards.iter().find(|config| config.shard_id == shard).unwrap())
    };
    assert_eq!(member_of("root"), vec![cluster.replica_node.clone()]);
    assert_eq!(member_of("services"), vec![cluster.services_primary_node.clone()]);
    assert_eq!(member_of("users"), vec![cluster.users_primary_node.clone()]);
    let nodes = [
        cluster.users_primary_node.clone(),
        cluster.services_primary_node.clone(),
        cluster.replica_node.clone(),
    ];
    (cluster, nodes)
}

#[test]
fn cross_shard_alias_rename_waits_while_its_target_shard_converts() {
    let (mut cluster, nodes) = disjoint_shard_cluster("native-alias-rename-target-converting");
    let root_node = cluster.replica_node.clone();
    let services_node = cluster.services_primary_node.clone();
    let canonical = "/users/converting-target";
    let alias = "/converting-alias";
    let moved = "/services/converting-alias";
    cluster
        .replica
        .posix_write_file(
            canonical,
            b"converting target bytes",
            DfsWriteOptions::replicated_eventual(vec![cluster.replica_node.clone()]),
        )
        .unwrap();
    cluster.replica.posix_hardlink(canonical, alias).unwrap();
    let linked = cluster.replica.posix_namespace_lstat_distributed(alias).unwrap();
    assert_eq!(linked.kind, DfsObjectKind::Hardlink);

    // The alias moves from root to services; neither owns its target.
    let mut kv = cluster._runtime.configuration_publisher(&root_node);
    let (active, converting) =
        close_shard_for_conversion(&cluster._runtime, kv.as_mut(), &nodes, "users");
    let refused = cluster.replica.posix_rename(alias, moved);
    assert!(
        matches!(&refused, Err(DfsPosixError::Dfs(DfsError::Busy { .. }))),
        "a move of an alias whose target shard converts is retriable: {refused:?}"
    );
    assert_eq!(
        node_store_object_at(&cluster._runtime, &root_node, alias).map(|object| object.object_id),
        Some(linked.object_id.clone())
    );
    assert!(node_store_object_at(&cluster._runtime, &services_node, moved).is_none());
    let store = cluster._runtime.node_metadata(&root_node).unwrap();
    assert!(
        !store
            .list_namespace_jobs()
            .unwrap()
            .iter()
            .any(|job| job.root_path == alias),
        "the refusal precedes the move's job"
    );
    let pending = store.list_pending_namespace_requests_page(None, 128).unwrap();
    assert!(pending.next_cursor.is_none(), "bounded native fixture request inventory");
    assert!(
        !pending.requests.iter().any(|request| matches!(&request.operation,
            DfsNamespaceOperation::Rename { from, .. } if from == alias)),
        "a fresh refused rename is not recorded for later execution: {:?}",
        pending.requests
    );
    drop(store);

    replace_shard_storage(&cluster._runtime, kv.as_mut(), &nodes, &converting, active.value.clone());
    let renamed = cluster.replica.posix_rename(alias, moved).unwrap();
    assert_eq!(renamed.link_count, 2);
    assert_eq!(
        cluster.replica.posix_namespace_lstat_distributed(moved).unwrap().object_id,
        linked.object_id
    );
    assert!(node_store_object_at(&cluster._runtime, &root_node, alias).is_none());
    drop(kv);
    cluster.close();
}

#[test]
fn cross_shard_alias_rename_reads_the_target_record_while_the_target_still_serves() {
    let (mut cluster, nodes) = disjoint_shard_cluster("native-alias-rename-target-record");
    let users_node = cluster.users_primary_node.clone();
    let root_node = cluster.replica_node.clone();
    let services_node = cluster.services_primary_node.clone();
    let canonical = "/users/record-converting-target";
    let alias = "/record-converting-alias";
    let moved = "/services/record-converting-alias";
    cluster
        .replica
        .posix_write_file(
            canonical,
            b"record converting target bytes",
            DfsWriteOptions::replicated_eventual(vec![cluster.replica_node.clone()]),
        )
        .unwrap();
    cluster.replica.posix_hardlink(canonical, alias).unwrap();
    let linked = cluster.replica.posix_namespace_lstat_distributed(alias).unwrap();

    // The target's own node has not yet observed its record turning
    // Converting and still answers for the target; the coordinator has.
    let key = crate::metadata_shard_storage_key(&sifs_file_system_id(), "users");
    let gap = cluster
        ._runtime
        .lose_configuration_delta_and_pause_rebuild(&users_node, key.clone())
        .unwrap();
    let observers = [root_node.clone(), services_node.clone()];
    let mut kv = cluster._runtime.configuration_publisher(&root_node);
    let (active, converting) =
        close_shard_for_conversion(&cluster._runtime, kv.as_mut(), &observers, "users");
    assert!(
        gap.dropped_delta()
            .unwrap()
            .events
            .iter()
            .any(|event| event.key == key),
        "the target's node withheld the record change"
    );
    let refused = cluster.replica.posix_rename(alias, moved);
    assert!(
        matches!(&refused, Err(DfsPosixError::Dfs(DfsError::Busy { .. }))),
        "the coordinator reads the target's record, not only its reachability: {refused:?}"
    );
    assert_eq!(
        node_store_object_at(&cluster._runtime, &root_node, alias).map(|object| object.object_id),
        Some(linked.object_id.clone())
    );
    assert!(node_store_object_at(&cluster._runtime, &services_node, moved).is_none());

    // Restoring the record is the next delta the target's node observes; it
    // detects the withheld one and rebuilds once released.
    replace_shard_storage(&cluster._runtime, kv.as_mut(), &observers, &converting, active.value.clone());
    gap.wait_rebuild(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
        .unwrap();
    gap.release();
    drop(gap);
    for node in &nodes {
        cluster._runtime.wait_published_configuration_current(node).unwrap();
    }
    cluster.replica.posix_rename(alias, moved).unwrap();
    assert_eq!(
        cluster.replica.posix_namespace_lstat_distributed(moved).unwrap().object_id,
        linked.object_id
    );
    drop(kv);
    cluster.close();
}

/// A shard with no storage record holds storage that migration has yet to
/// convert, so it is closed as a converting shard is: the coordinator reads
/// the target's record, finds none, and refuses the move even while the
/// target's own node still answers for the target.
#[test]
fn cross_shard_alias_rename_waits_while_its_target_shard_has_no_storage_record() {
    let (mut cluster, nodes) = disjoint_shard_cluster("native-alias-rename-target-unrecorded");
    let users_node = cluster.users_primary_node.clone();
    let root_node = cluster.replica_node.clone();
    let services_node = cluster.services_primary_node.clone();
    let canonical = "/users/unrecorded-target";
    let alias = "/unrecorded-alias";
    let moved = "/services/unrecorded-alias";
    cluster
        .replica
        .posix_write_file(
            canonical,
            b"unrecorded target bytes",
            DfsWriteOptions::replicated_eventual(vec![cluster.replica_node.clone()]),
        )
        .unwrap();
    cluster.replica.posix_hardlink(canonical, alias).unwrap();
    let linked = cluster.replica.posix_namespace_lstat_distributed(alias).unwrap();

    // The target's own node has not yet observed its record's removal and
    // still answers for the target; the coordinator has.
    let key = crate::metadata_shard_storage_key(&sifs_file_system_id(), "users");
    let gap = cluster
        ._runtime
        .lose_configuration_delta_and_pause_rebuild(&users_node, key.clone())
        .unwrap();
    let observers = [root_node.clone(), services_node.clone()];
    let mut kv = cluster._runtime.configuration_publisher(&root_node);
    let active = shard_storage_entry(kv.as_mut(), "users");
    // Test setup only: the record is removed, as for old storage that
    // migration has yet to convert, and restored below; no conversion runs.
    let set_record = |kv: &mut dyn DfsControlPlaneKvClient, value: Option<Vec<u8>>| {
        let (compare, op) = match value {
            Some(value) => (
                sinomo_core::kv::KvCompare::Missing { key: key.clone() },
                sinomo_core::kv::KvOp::Put { key: key.clone(), value, lease_id: None },
            ),
            None => (
                sinomo_core::kv::KvCompare::ValueEquals { key: key.clone(), value: active.value.clone() },
                sinomo_core::kv::KvOp::Delete { key: key.clone() },
            ),
        };
        assert!(kv
            .txn(KvTxn { compares: vec![compare], success: vec![op], failure: Vec::new() })
            .unwrap()
            .succeeded);
        for node in &observers {
            cluster._runtime.wait_published_configuration_current(node).unwrap();
        }
    };
    set_record(kv.as_mut(), None);
    assert!(
        gap.dropped_delta()
            .unwrap()
            .events
            .iter()
            .any(|event| event.key == key),
        "the target's node withheld the record change"
    );
    let refused = cluster.replica.posix_rename(alias, moved);
    assert!(
        matches!(&refused, Err(DfsPosixError::Dfs(DfsError::Busy { .. }))),
        "a target shard with no storage record is closed: {refused:?}"
    );
    assert_eq!(
        node_store_object_at(&cluster._runtime, &root_node, alias).map(|object| object.object_id),
        Some(linked.object_id.clone())
    );
    assert!(node_store_object_at(&cluster._runtime, &services_node, moved).is_none());

    set_record(kv.as_mut(), Some(active.value.clone()));
    gap.wait_rebuild(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
        .unwrap();
    gap.release();
    drop(gap);
    for node in &nodes {
        cluster._runtime.wait_published_configuration_current(node).unwrap();
    }
    cluster.replica.posix_rename(alias, moved).unwrap();
    assert_eq!(
        cluster.replica.posix_namespace_lstat_distributed(moved).unwrap().object_id,
        linked.object_id
    );
    drop(kv);
    cluster.close();
}

pub(super) fn directory_move_job(
    runtime: &crate::test_runtime::DfsTestRuntime,
    node: &NodeId,
    from: &str,
) -> DfsNamespaceJobRecord {
    runtime
        .node_metadata(node)
        .unwrap()
        .list_namespace_jobs()
        .unwrap()
        .into_iter()
        .find(|job| {
            job.kind == DfsNamespaceJobKind::RenameDirectoryAcrossShardPrimaries
                && job.root_path == from
        })
        .expect("the directory move persisted its job")
}

#[test]
fn cross_shard_directory_move_keeps_a_page_whose_alias_target_shard_converts() {
    let (mut cluster, nodes) = disjoint_shard_cluster("native-tree-move-target-converting");
    let root_node = cluster.replica_node.clone();
    let services_node = cluster.services_primary_node.clone();
    let canonical = "/users/tree-converting-target";
    let from = "/converting-tree";
    let to = "/services/converting-tree";
    let alias = format!("{from}/alias");
    let plain = format!("{from}/plain");
    let options = || DfsWriteOptions::replicated_eventual(vec![cluster.replica_node.clone()]);
    cluster.replica.posix_mkdir_all(from).unwrap();
    cluster
        .replica
        .posix_write_file(canonical, b"tree target bytes", options())
        .unwrap();
    cluster
        .replica
        .posix_write_file(&plain, b"plain bytes", options())
        .unwrap();
    cluster.replica.posix_hardlink(canonical, &alias).unwrap();
    let linked = cluster.replica.posix_namespace_lstat_distributed(&alias).unwrap();
    assert_eq!(linked.kind, DfsObjectKind::Hardlink);

    let mut kv = cluster._runtime.configuration_publisher(&root_node);
    let (active, converting) =
        close_shard_for_conversion(&cluster._runtime, kv.as_mut(), &nodes, "users");
    let parked = cluster.replica.posix_rename(from, to);
    assert!(
        matches!(&parked, Err(DfsPosixError::Dfs(DfsError::Busy { .. }))),
        "the move waits on its carried alias's target shard: {parked:?}"
    );
    let job = directory_move_job(&cluster._runtime, &root_node, from);
    assert_eq!(job.status, DfsNamespaceJobStatus::Running);
    assert!(
        job.pending_frames.iter().any(|frame| matches!(frame,
            DfsNamespaceJobFrame::CrossShardDirScan { source_path, cursor: None, .. }
                if source_path == from)),
        "the job keeps the page: {:?}",
        job.pending_frames
    );
    for child in [&alias, &plain] {
        assert!(
            node_store_object_at(&cluster._runtime, &root_node, child).is_some(),
            "{child} keeps its source row"
        );
        let target = child.replacen(from, to, 1);
        assert!(
            node_store_object_at(&cluster._runtime, &services_node, &target).is_none(),
            "no frame of the page ran: {target}"
        );
    }

    replace_shard_storage(&cluster._runtime, kv.as_mut(), &nodes, &converting, active.value.clone());
    // The existing driver resumes the Running job: the coordinator's
    // maintenance runs its pending request and namespace jobs.
    let deadline = Instant::now() + DEFAULT_DFS_POSIX_SYNC_TIMEOUT;
    // A pass can meet unrelated transient work, such as a membership handoff
    // the fixture's default setup still runs; the next pass retries it.
    let mut last_tick = None;
    while directory_move_job(&cluster._runtime, &root_node, from).status
        == DfsNamespaceJobStatus::Running
    {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "the parked move did not resume: {:?}, last maintenance pass {last_tick:?}",
            directory_move_job(&cluster._runtime, &root_node, from)
        );
        let tick = cluster
            ._runtime
            .maintenance_tick(
                &root_node,
                remaining.min(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT),
            )
            .unwrap()
            .outcome;
        last_tick = Some(tick);
    }
    assert_eq!(
        directory_move_job(&cluster._runtime, &root_node, from).status,
        DfsNamespaceJobStatus::Completed
    );
    assert_eq!(
        cluster
            .replica
            .posix_namespace_lstat_distributed(&format!("{to}/alias"))
            .unwrap()
            .object_id,
        linked.object_id
    );
    assert_eq!(
        cluster.replica.posix_stat_distributed(canonical).unwrap().link_count,
        2
    );
    assert!(node_store_object_at(&cluster._runtime, &root_node, &alias).is_none());
    assert!(
        node_store_object_at(&cluster._runtime, &services_node, &format!("{to}/plain")).is_some()
    );
    drop(kv);
    cluster.close();
}

#[test]
fn cross_shard_global_operations_serve_beside_a_closed_shard_copy() {
    // Every node holds a copy of every shard, and root and users share their
    // primary: that node's global checkout covers both slots.
    let mut cluster = sifs_two_primary_cluster("native-global-beside-closed-shard");
    let owner = cluster.users_primary_node.clone();
    let shards = cluster._runtime.metadata_shards().unwrap();
    for shard in ["root", "users"] {
        let config = shards
            .iter()
            .find(|config| config.shard_id == shard)
            .unwrap();
        assert_eq!(config.primary_node_id, owner, "{shard}");
        assert_eq!(config.replica_node_ids.len(), 2, "{shard}");
    }
    let nodes = [
        cluster.users_primary_node.clone(),
        cluster.services_primary_node.clone(),
        cluster.replica_node.clone(),
    ];
    let options = || DfsWriteOptions::replicated_eventual(vec![cluster.replica_node.clone()]);
    let beside = "/beside-closed";
    let beside_renamed = "/beside-closed-renamed";
    let closed = "/users/closed-file";
    let closed_renamed = "/users/closed-file-renamed";
    cluster
        .replica
        .posix_write_file(beside, b"root bytes", options())
        .unwrap();
    cluster
        .replica
        .posix_write_file(closed, b"users bytes", options())
        .unwrap();

    let mut kv = cluster._runtime.configuration_publisher(&owner);
    let (active, converting) =
        close_shard_for_conversion(&cluster._runtime, kv.as_mut(), &nodes, "users");
    // A rename is globally admitted. This one addresses only root, which the
    // closed users slot on the same node does not stop.
    cluster
        .replica
        .posix_rename(beside, beside_renamed)
        .unwrap();
    assert!(node_store_object_at(&cluster._runtime, &owner, beside_renamed).is_some());
    assert!(node_store_object_at(&cluster._runtime, &owner, beside).is_none());

    // A globally admitted operation that addresses the closed shard still
    // meets its storage refusal.
    let refused = cluster.replica.posix_rename(closed, closed_renamed);
    assert!(
        matches!(
            &refused,
            Err(crate::DfsPosixError::Dfs(
                crate::DfsError::ShardStorageClosed {
                    cause: crate::DfsShardStorageClosedCause::Conversion,
                    ..
                }
            ))
        ),
        "a rename inside the closed shard refuses: {refused:?}"
    );
    // A write to a file of the closed shard is refused the same way, before
    // any effect.
    let refused = cluster
        .replica
        .posix_write_file(closed, b"rewritten bytes", options());
    assert!(
        matches!(
            &refused,
            Err(crate::DfsPosixError::Dfs(
                crate::DfsError::ShardStorageClosed {
                    cause: crate::DfsShardStorageClosedCause::Conversion,
                    ..
                }
            ))
        ),
        "a write inside the closed shard refuses: {refused:?}"
    );

    replace_shard_storage(
        &cluster._runtime,
        kv.as_mut(),
        &nodes,
        &converting,
        active.value.clone(),
    );
    cluster
        .replica
        .posix_rename(closed, closed_renamed)
        .unwrap();
    assert!(node_store_object_at(&cluster._runtime, &owner, closed_renamed).is_some());
    assert!(node_store_object_at(&cluster._runtime, &owner, closed).is_none());
    drop(kv);
    cluster.close();
}

#[test]
fn cross_shard_directory_move_parked_on_a_converting_target_resumes_when_it_activates() {
    let (mut cluster, nodes) = disjoint_shard_cluster("native-tree-move-target-activation-wake");
    let root_node = cluster.replica_node.clone();
    let services_node = cluster.services_primary_node.clone();
    let canonical = "/users/tree-activation-target";
    let from = "/activation-tree";
    let to = "/services/activation-tree";
    let alias = format!("{from}/alias");
    let options = || DfsWriteOptions::replicated_eventual(vec![cluster.replica_node.clone()]);
    cluster.replica.posix_mkdir_all(from).unwrap();
    cluster
        .replica
        .posix_write_file(canonical, b"tree target bytes", options())
        .unwrap();
    cluster.replica.posix_hardlink(canonical, &alias).unwrap();
    let linked = cluster
        .replica
        .posix_namespace_lstat_distributed(&alias)
        .unwrap();

    let mut kv = cluster._runtime.configuration_publisher(&root_node);
    let (active, converting) =
        close_shard_for_conversion(&cluster._runtime, kv.as_mut(), &nodes, "users");
    let parked = cluster.replica.posix_rename(from, to);
    assert!(
        matches!(&parked, Err(DfsPosixError::Dfs(DfsError::Busy { .. }))),
        "the move waits on its carried alias's target shard: {parked:?}"
    );
    // Every wake the move emitted precedes its reply. Once the coordinator's
    // maintenance has finished its last pass and launched no other, the
    // parked job has no earlier wake left: its next routine retry is at least
    // 5 s out (30 s for a pass that still has work).
    let root = cluster._runtime.data_dir(&root_node).root();
    let passes = || {
        use crate::maintenance::phase_runs::{FULL_TICK_COMPLETION, FULL_TICK_LAUNCH, count};
        (
            count(&root, FULL_TICK_LAUNCH),
            count(&root, FULL_TICK_COMPLETION),
        )
    };
    let settle_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let mut previous = passes();
    let mut settled_polls = 0;
    while settled_polls < 5 {
        assert!(
            Instant::now() < settle_deadline,
            "the coordinator's maintenance never settled"
        );
        std::thread::sleep(Duration::from_millis(20));
        let current = passes();
        settled_polls = if current == previous && current.0 == current.1 {
            settled_polls + 1
        } else {
            0
        };
        previous = current;
    }
    assert_eq!(
        directory_move_job(&cluster._runtime, &root_node, from).status,
        DfsNamespaceJobStatus::Running
    );

    replace_shard_storage(
        &cluster._runtime,
        kv.as_mut(),
        &nodes,
        &converting,
        active.value.clone(),
    );
    // No maintenance pass is invoked here. The coordinator observing the
    // record's activation wakes its maintenance, which resumes the job well
    // before that routine retry.
    let deadline = Instant::now() + Duration::from_secs(4);
    while directory_move_job(&cluster._runtime, &root_node, from).status
        == DfsNamespaceJobStatus::Running
    {
        assert!(
            Instant::now() < deadline,
            "the parked move did not resume on the activation: {:?}",
            directory_move_job(&cluster._runtime, &root_node, from)
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(
        directory_move_job(&cluster._runtime, &root_node, from).status,
        DfsNamespaceJobStatus::Completed
    );
    assert_eq!(
        cluster
            .replica
            .posix_namespace_lstat_distributed(&format!("{to}/alias"))
            .unwrap()
            .object_id,
        linked.object_id
    );
    assert!(node_store_object_at(&cluster._runtime, &root_node, &alias).is_none());
    assert!(
        node_store_object_at(&cluster._runtime, &services_node, &format!("{to}/alias")).is_some()
    );
    drop(kv);
    cluster.close();
}

/// Each node's full maintenance launches and completions.
fn maintenance_passes(
    runtime: &crate::test_runtime::DfsTestRuntime,
    nodes: &[NodeId],
) -> Vec<(u64, u64)> {
    use crate::maintenance::phase_runs::{FULL_TICK_COMPLETION, FULL_TICK_LAUNCH, count};
    nodes
        .iter()
        .map(|node| {
            let root = runtime.data_dir(node).root();
            (
                count(&root, FULL_TICK_LAUNCH),
                count(&root, FULL_TICK_COMPLETION),
            )
        })
        .collect()
}

/// Wait until no node has a maintenance pass running and none launches for a
/// few consecutive observations. Returns the settled counts.
fn settled_maintenance_passes(
    runtime: &crate::test_runtime::DfsTestRuntime,
    nodes: &[NodeId],
) -> Vec<(u64, u64)> {
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let mut previous = maintenance_passes(runtime, nodes);
    let mut settled_polls = 0;
    while settled_polls < 5 {
        assert!(Instant::now() < deadline, "maintenance never settled");
        std::thread::sleep(Duration::from_millis(20));
        let current = maintenance_passes(runtime, nodes);
        let idle = current.iter().all(|(launched, completed)| launched == completed);
        settled_polls = if current == previous && idle {
            settled_polls + 1
        } else {
            0
        };
        previous = current;
    }
    previous
}

#[test]
fn a_shard_activation_launches_no_maintenance_on_a_node_with_no_parked_work() {
    let (cluster, nodes) = disjoint_shard_cluster("native-activation-idle-nodes");
    let root_node = cluster.replica_node.clone();
    let mut kv = cluster._runtime.configuration_publisher(&root_node);
    let (active, converting) =
        close_shard_for_conversion(&cluster._runtime, kv.as_mut(), &nodes, "users");
    let before = settled_maintenance_passes(&cluster._runtime, &nodes);

    // No node holds work parked on users: its activation is no work anywhere.
    replace_shard_storage(
        &cluster._runtime,
        kv.as_mut(),
        &nodes,
        &converting,
        active.value.clone(),
    );
    let window = Instant::now() + Duration::from_secs(2);
    while Instant::now() < window {
        assert_eq!(
            maintenance_passes(&cluster._runtime, &nodes),
            before,
            "an activation launched maintenance on a node with no parked work"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    drop(kv);
    cluster.close();
}

/// Test setup only: apply one compare-and-write of `shard`'s storage record and
/// wait until every node in `nodes` publishes it.
fn write_shard_storage(
    runtime: &crate::test_runtime::DfsTestRuntime,
    kv: &mut dyn DfsControlPlaneKvClient,
    nodes: &[NodeId],
    compare: sinomo_core::kv::KvCompare,
    write: sinomo_core::kv::KvOp,
) {
    let written = kv
        .txn(KvTxn {
            compares: vec![compare],
            success: vec![write],
            failure: Vec::new(),
        })
        .unwrap()
        .succeeded;
    assert!(written, "the storage record changed under the test");
    for node in nodes {
        runtime.wait_published_configuration_current(node).unwrap();
    }
}

/// A target shard with no storage record whose copy refuses to open, as an
/// unprepared old-format root does, is not Active: a move carrying an alias
/// that targets it is parked with `Busy` rather than failed, and its storage
/// activation resumes it.
#[test]
fn cross_shard_directory_move_parks_on_an_unprepared_target_without_a_record() {
    let (mut cluster, nodes) = disjoint_shard_cluster("native-tree-move-unprepared-target");
    let root_node = cluster.replica_node.clone();
    let users_node = cluster.users_primary_node.clone();
    let services_node = cluster.services_primary_node.clone();
    let canonical = "/users/unprepared-target";
    let from = "/unprepared-tree";
    let to = "/services/unprepared-tree";
    let alias = format!("{from}/alias");
    let options = || DfsWriteOptions::replicated_eventual(vec![cluster.replica_node.clone()]);
    cluster.replica.posix_mkdir_all(from).unwrap();
    cluster
        .replica
        .posix_write_file(canonical, b"unprepared target bytes", options())
        .unwrap();
    cluster.replica.posix_hardlink(canonical, &alias).unwrap();
    let linked = cluster
        .replica
        .posix_namespace_lstat_distributed(&alias)
        .unwrap();

    // The users copy awaits storage preparation, and users has no record.
    let users_root = crate::local_dfs_metadata_shard_path(
        cluster._runtime.data_dir(&users_node),
        &DfsMetadataShardId::new("users"),
    );
    let preparation = crate::register_dfs_storage_preparation_provider(
        users_root,
        Arc::new(|_, scope, _| {
            Ok(match scope {
                crate::DfsStoragePreparationScope::Metadata(shard) if shard.0 == "users" => {
                    crate::DfsStoragePreparation::Pending {
                        processed_records: 0,
                    }
                }
                _ => crate::DfsStoragePreparation::Ready,
            })
        }),
        None,
    )
    .unwrap();
    let mut kv = cluster._runtime.configuration_publisher(&root_node);
    let active = shard_storage_entry(kv.as_mut(), "users");
    write_shard_storage(
        &cluster._runtime,
        kv.as_mut(),
        &nodes,
        sinomo_core::kv::KvCompare::ValueEquals {
            key: active.key.clone(),
            value: active.value.clone(),
        },
        sinomo_core::kv::KvOp::Delete {
            key: active.key.clone(),
        },
    );

    let parked = cluster.replica.posix_rename(from, to);
    assert!(
        matches!(&parked, Err(DfsPosixError::Dfs(DfsError::Busy { .. }))),
        "the move waits on its carried alias's unprepared target shard: {parked:?}"
    );
    let root = cluster._runtime.data_dir(&root_node).root();
    let passes = || {
        use crate::maintenance::phase_runs::{FULL_TICK_COMPLETION, FULL_TICK_LAUNCH, count};
        (
            count(&root, FULL_TICK_LAUNCH),
            count(&root, FULL_TICK_COMPLETION),
        )
    };
    let settle_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let mut previous = passes();
    let mut settled_polls = 0;
    while settled_polls < 5 {
        assert!(
            Instant::now() < settle_deadline,
            "the coordinator's maintenance never settled"
        );
        std::thread::sleep(Duration::from_millis(20));
        let current = passes();
        settled_polls = if current == previous && current.0 == current.1 {
            settled_polls + 1
        } else {
            0
        };
        previous = current;
    }
    assert_eq!(
        directory_move_job(&cluster._runtime, &root_node, from).status,
        DfsNamespaceJobStatus::Running
    );

    // Its conversion closes the shard with a record and activates it; the
    // prepared copy then opens. Only the activation wakes the coordinator.
    drop(preparation);
    let users = cluster
        ._runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|config| config.shard_id == "users")
        .unwrap();
    let converting = sinomo_core::kv::encode_kv_value(
        &crate::DfsMetadataShardStorage::converting_for_test(&users, 1),
    )
    .unwrap();
    write_shard_storage(
        &cluster._runtime,
        kv.as_mut(),
        &nodes,
        sinomo_core::kv::KvCompare::Missing {
            key: active.key.clone(),
        },
        sinomo_core::kv::KvOp::Put {
            key: active.key.clone(),
            value: converting.clone(),
            lease_id: None,
        },
    );
    write_shard_storage(
        &cluster._runtime,
        kv.as_mut(),
        &nodes,
        sinomo_core::kv::KvCompare::ValueEquals {
            key: active.key.clone(),
            value: converting,
        },
        sinomo_core::kv::KvOp::Put {
            key: active.key.clone(),
            value: active.value.clone(),
            lease_id: None,
        },
    );
    let deadline = Instant::now() + Duration::from_secs(4);
    while directory_move_job(&cluster._runtime, &root_node, from).status
        == DfsNamespaceJobStatus::Running
    {
        assert!(
            Instant::now() < deadline,
            "the parked move did not resume on the activation: {:?}",
            directory_move_job(&cluster._runtime, &root_node, from)
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(
        directory_move_job(&cluster._runtime, &root_node, from).status,
        DfsNamespaceJobStatus::Completed
    );
    assert_eq!(
        cluster
            .replica
            .posix_namespace_lstat_distributed(&format!("{to}/alias"))
            .unwrap()
            .object_id,
        linked.object_id
    );
    assert!(
        node_store_object_at(&cluster._runtime, &services_node, &format!("{to}/alias")).is_some()
    );
    drop(kv);
    cluster.close();
}
