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
