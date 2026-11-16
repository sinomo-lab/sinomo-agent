use super::*;
use crate::test_support::DfsTestRuntime;
use crate::{DfsMetadataBackend, DfsMetadataShardId, DfsPreparedOperationStatus, DfsWriteOptions};
use sinomo_core::node::NodeId;
use std::time::Instant;

fn fixture(
    name: &str,
) -> (
    DfsTestRuntime,
    NodeId,
    NetworkedDfsService<crate::DfsNodeTransport>,
) {
    let (runtime, nodes) = DfsTestRuntime::start_cluster(name, 1).unwrap();
    let node = nodes[0].clone();
    let client = runtime.client(&node).unwrap();
    (runtime, node, client)
}
fn count(client: &mut NetworkedDfsService<crate::DfsNodeTransport>, path: &str) -> u64 {
    client.posix_stat_distributed(path).unwrap().link_count
}
#[test]
fn unknown_and_invalid_counts_never_bootstrap_during_mutation() {
    let node = NodeId::generate();
    let mut file =
        crate::posix_object::new_posix_metadata_object(&node, "/file", DfsObjectKind::File);
    file.namespace_link_count = None;
    assert!(crate::posix::updated_namespace_link_count(&file, 1).is_err());
    file.namespace_link_count = Some(1);
    assert!(crate::posix::updated_namespace_link_count(&file, -1).is_err());
    file.namespace_link_count = Some(u64::MAX);
    assert!(crate::posix::updated_namespace_link_count(&file, 1).is_err());
    let directory =
        crate::posix_object::new_posix_metadata_object(&node, "/dir", DfsObjectKind::Directory);
    assert!(crate::posix::updated_namespace_link_count(&directory, -1).is_err());
}
#[test]
fn native_local_alias_count_shares_publication_and_exact_retry() {
    let (runtime, node, mut client) = fixture("native-local-count-alias");
    let root = "/services/local-count-alias";
    let path = format!("{root}/file");
    let alias = format!("{root}/alias");
    client.posix_mkdir_all(root).unwrap();
    let original = client
        .posix_write_file(
            &path,
            b"original",
            DfsWriteOptions::replicated_eventual(vec![node.clone()]),
        )
        .unwrap();
    assert_eq!(count(&mut client, &path), 1);
    for _ in 0..2 {
        client
            .route_namespace_operation_with_request_id(
                "native-count-hardlink-exact".into(),
                node.clone(),
                DfsNamespaceOperation::Hardlink {
                    existing_path: path.clone(),
                    link_path: alias.clone(),
                },
            )
            .unwrap();
        assert_eq!(count(&mut client, &path), 2);
        assert_eq!(count(&mut client, &alias), 2);
    }
    let snapshot = runtime
        .node_metadata(&node)
        .unwrap()
        .export_shard_table_snapshot(&DfsMetadataShardId::new("services"))
        .unwrap()
        .unwrap();
    let publications =
        snapshot
            .tables
            .prepared_operations_v7
            .iter()
            .filter(|record| {
                record.status == DfsPreparedOperationStatus::Committed
                    && record.transaction.objects.iter().any(|object| {
                        object.path == alias && object.kind == DfsObjectKind::Hardlink
                    })
            })
            .collect::<Vec<_>>();
    assert_eq!(
        publications.len(),
        1,
        "exact retry has one native alias publication"
    );
    let published = &publications[0].transaction.objects;
    assert!(
        published
            .iter()
            .any(|object| object.object_id == original.object_id
                && object.namespace_link_count == Some(2))
    );
    assert!(
        published
            .iter()
            .any(|object| object.path == root && object.kind == DfsObjectKind::Directory)
    );
    client
        .posix_write_file(
            &path,
            b"overwrite keeps aliases",
            DfsWriteOptions::replicated_eventual(vec![node.clone()]),
        )
        .unwrap();
    assert_eq!(count(&mut client, &alias), 2);
    client.posix_unlink(&alias).unwrap();
    assert_eq!(count(&mut client, &path), 1);
    client.posix_hardlink(&path, &alias).unwrap();
    client.posix_unlink(&path).unwrap();
    assert_eq!(count(&mut client, &alias), 1);
    assert_eq!(
        client.posix_stat_distributed(&alias).unwrap().object_id,
        original.object_id
    );
    assert!(matches!(
        client.posix_stat_distributed(&path),
        Err(DfsPosixError::NotFound { .. })
    ));
    drop(client);
    runtime.close().unwrap();
}
#[test]
fn native_directory_counts_cover_batched_create_rename_replace_and_retry() {
    let (runtime, _, mut client) = fixture("native-local-count-directories");
    let root = "/services/local-count-directories";
    let a = format!("{root}/a");
    let b = format!("{root}/b");
    client.posix_mkdir_all(&format!("{a}/nested")).unwrap();
    assert_eq!(count(&mut client, root), 3);
    assert_eq!(count(&mut client, &a), 3);
    client.posix_mkdir(&b).unwrap();
    for _ in 0..2 {
        client
            .posix_mkdir_with_operation_id(&format!("{a}/x"), "native-count-mkdir-retry")
            .unwrap();
        assert_eq!(count(&mut client, &a), 4);
    }
    for _ in 0..2 {
        client
            .posix_rename_with_operation_id(
                &format!("{a}/x"),
                &format!("{b}/x"),
                "native-count-rename-retry",
            )
            .unwrap();
        assert_eq!(count(&mut client, &a), 3);
        assert_eq!(count(&mut client, &b), 3);
    }
    client
        .posix_rename(&format!("{b}/x"), &format!("{b}/y"))
        .unwrap();
    assert_eq!(count(&mut client, &b), 3);
    client.posix_mkdir(&format!("{b}/z")).unwrap();
    assert_eq!(count(&mut client, &b), 4);
    client
        .posix_rename(&format!("{b}/y"), &format!("{b}/z"))
        .unwrap();
    assert_eq!(count(&mut client, &b), 3);
    client.posix_rmdir(&format!("{b}/z")).unwrap();
    assert_eq!(count(&mut client, &b), 2);
    client.posix_rename(&a, &format!("{b}/moved")).unwrap();
    assert_eq!(count(&mut client, root), 3);
    assert_eq!(count(&mut client, &b), 3);
    assert_eq!(count(&mut client, &format!("{b}/moved")), 3);
    assert_eq!(count(&mut client, &format!("{b}/moved/nested")), 2);
    drop(client);
    runtime.close().unwrap();
}
#[test]
fn native_recursive_clear_counts_survive_internal_promotions_and_resume() {
    let (mut runtime, node, mut client) = fixture("native-local-count-recursive-clear");
    let root = "/services/local-count-clear";
    let outside = "/services/local-count-clear-outside";
    client.posix_mkdir_all(&format!("{root}/sub")).unwrap();
    let first = format!("{root}/first");
    client
        .posix_write_file(
            &first,
            b"surviving bytes",
            DfsWriteOptions::replicated_eventual(vec![node.clone()]),
        )
        .unwrap();
    for alias in [
        format!("{root}/alias"),
        format!("{root}/sub/alias"),
        outside.into(),
    ] {
        client.posix_hardlink(&first, &alias).unwrap();
    }
    let internal = format!("{root}/internal");
    client
        .posix_write_file(
            &internal,
            b"internal bytes",
            DfsWriteOptions::replicated_eventual(vec![node.clone()]),
        )
        .unwrap();
    client
        .posix_hardlink(&internal, &format!("{root}/internal-alias"))
        .unwrap();
    assert_eq!(count(&mut client, outside), 4);
    drop(client);
    let mut owner = runtime.metadata_primary_service(root).unwrap();
    let mut job = owner.posix_clear_dir_recursive_bounded(root, 1).unwrap();
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    while job.status != DfsNamespaceJobStatus::Completed {
        assert!(
            Instant::now() < deadline,
            "finite subtree must finish under original native deadline"
        );
        let immediate_directories = owner
            .posix_list_dir(root)
            .unwrap()
            .iter()
            .filter(|entry| entry.kind == DfsObjectKind::Directory)
            .count() as u64;
        assert_eq!(
            owner.posix_stat(root).unwrap().link_count,
            2 + immediate_directories
        );
        job = owner
            .posix_resume_namespace_job_bounded(&job.job_id, 1)
            .unwrap();
    }
    assert_eq!(owner.posix_stat(root).unwrap().link_count, 2);
    assert!(owner.posix_list_dir(root).unwrap().is_empty());
    assert_eq!(owner.posix_stat(outside).unwrap().link_count, 1);
    owner
        .posix_resume_namespace_job_bounded(&job.job_id, 1)
        .unwrap();
    assert_eq!(owner.posix_stat(outside).unwrap().link_count, 1);
    drop(owner);
    runtime.stop_node(&node).unwrap();
    runtime.reopen_node(node.clone()).unwrap();
    let mut client = runtime.client(&node).unwrap();
    assert_eq!(count(&mut client, outside), 1);
    assert_eq!(
        client.posix_read_file_distributed(outside).unwrap(),
        b"surviving bytes"
    );
    drop(client);
    runtime.close().unwrap();
}

#[test]
fn native_generic_directory_membership_copublishes_parent_and_ignores_metadata_touches() {
    let (runtime, node, mut client) = fixture("native-generic-directory-count");
    let parent = "/services/generic-directory-count";
    let path = format!("{parent}/child");
    client.posix_mkdir(parent).unwrap();
    let mut owner = runtime.metadata_primary_service(parent).unwrap();
    let mut child =
        crate::posix_object::new_posix_metadata_object(&node, &path, DfsObjectKind::Directory);
    owner.add_object_metadata(child.clone()).unwrap();
    assert_eq!(count(&mut client, parent), 3);
    owner.add_object_metadata(child.clone()).unwrap();
    assert_eq!(count(&mut client, parent), 3);
    child.version = crate::namespace::next_version(&child.version);
    child.uid = 42;
    owner.add_object_metadata(child.clone()).unwrap();
    assert_eq!(count(&mut client, parent), 3);
    let snapshot = runtime
        .node_metadata(&node)
        .unwrap()
        .export_shard_table_snapshot(&DfsMetadataShardId::new("services"))
        .unwrap()
        .unwrap();
    let births = snapshot
        .tables
        .prepared_operations_v7
        .iter()
        .filter(|record| {
            record.status == DfsPreparedOperationStatus::Committed
                && record.transaction.objects.iter().any(|row| {
                    row.object_id == child.object_id && row.kind == DfsObjectKind::Directory
                })
                && record
                    .transaction
                    .objects
                    .iter()
                    .any(|row| row.path == parent && row.namespace_link_count == Some(3))
        })
        .count();
    assert_eq!(
        births, 1,
        "child and parent count share one original native publication"
    );
    let mut invalid = crate::posix_object::new_posix_metadata_object(
        &node,
        &format!("{parent}/unknown-count"),
        DfsObjectKind::Directory,
    );
    invalid.namespace_link_count = None;
    assert!(owner.add_object_metadata(invalid.clone()).is_err());
    assert_eq!(
        count(&mut client, parent),
        3,
        "rejected child cannot increment its parent"
    );
    assert!(matches!(
        client.posix_stat_distributed(&invalid.path),
        Err(DfsPosixError::NotFound { .. })
    ));
    let tombstone = owner.tombstone_path_object(child);
    owner.add_object_metadata(tombstone.clone()).unwrap();
    assert_eq!(count(&mut client, parent), 2);
    owner.add_object_metadata(tombstone).unwrap();
    assert_eq!(count(&mut client, parent), 2);
    let file_path = format!("{parent}/last-file");
    let file = client
        .posix_write_file(
            &file_path,
            b"last name",
            DfsWriteOptions::replicated_eventual(vec![node.clone()]),
        )
        .unwrap();
    let file = runtime
        .node_metadata(&node)
        .unwrap()
        .get_object(&file.object_id)
        .unwrap()
        .unwrap();
    owner
        .add_object_metadata(owner.tombstone_path_object(file))
        .unwrap();
    assert_eq!(
        count(&mut client, parent),
        2,
        "file deletion does not change directory child count"
    );
    assert!(matches!(
        client.posix_stat_distributed(&file_path),
        Err(DfsPosixError::NotFound { .. })
    ));
    drop(owner);
    drop(client);
    runtime.close().unwrap();
}

#[test]
fn native_missing_node_bootstrap_cohort_preserves_parent_and_repeated_input() {
    let (runtime, node, mut client) = fixture("native-bootstrap-node-count");
    let parent = crate::namespace::SIFS_NODES_PATH;
    client
        .posix_mkdir(&format!("{parent}/user-owned-directory"))
        .unwrap();
    let before = client.posix_stat_distributed(parent).unwrap();
    let extra = NodeId::generate();
    // This supplies the production bootstrap helper's desired namespace DATA
    // to the real Root author. It does not simulate membership or node joins.
    let cohort = crate::namespace::distributed_sifs_bootstrap_objects_for_nodes(
        [node.clone(), extra.clone()],
        node.clone(),
    );
    let new_path = crate::namespace::sifs_node_path(&extra);
    for _ in 0..2 {
        runtime
            .with_main_maintenance_service(&node, |owner| {
                owner.publish_missing_bootstrap_objects(cohort.clone())
            })
            .unwrap()
            .unwrap();
        assert_eq!(count(&mut client, parent), before.link_count + 1);
        assert_eq!(count(&mut client, &new_path), 3);
        assert_eq!(
            count(&mut client, &crate::namespace::sifs_node_files_path(&extra)),
            2
        );
    }
    runtime
        .with_main_maintenance_service(&node, |owner| owner.bootstrap_distributed_sifs())
        .unwrap()
        .unwrap();
    assert_eq!(
        count(&mut client, parent),
        before.link_count + 1,
        "ordinary bootstrap cannot reset a live parent's user-created sibling count"
    );
    let snapshot = runtime
        .node_metadata(&node)
        .unwrap()
        .export_shard_table_snapshot(&DfsMetadataShardId::new("root"))
        .unwrap()
        .unwrap();
    let additions = snapshot
        .tables
        .prepared_operations_v7
        .iter()
        .filter(|record| {
            record.status == DfsPreparedOperationStatus::Committed
                && record
                    .transaction
                    .objects
                    .iter()
                    .any(|row| row.path == new_path)
                && record.transaction.objects.iter().any(|row| {
                    row.path == parent && row.namespace_link_count == Some(before.link_count + 1)
                })
        })
        .count();
    assert_eq!(
        additions, 1,
        "missing node and existing parent successor publish exactly once"
    );
    drop(client);
    runtime.close().unwrap();
}

#[test]
fn native_canonical_unlink_discovers_alias_outside_its_metadata_replica_set() {
    let name = "native-unreplicated-alias-promotion";
    let (runtime, nodes) = DfsTestRuntime::start_cluster(name, 3).unwrap();
    let fs = runtime.file_system().file_system_id;
    let users = runtime
        .reassign_metadata_shard(&fs, "users", nodes[0].clone(), Vec::new())
        .unwrap();
    let services = runtime
        .reassign_metadata_shard(&fs, "services", nodes[1].clone(), Vec::new())
        .unwrap();
    assert!(users.replica_node_ids.is_empty());
    assert!(services.replica_node_ids.is_empty());
    let canonical = "/users/unreplicated-alias-canonical";
    let alias = "/services/unreplicated-alias-entry";
    let mut client = runtime.client(&nodes[2]).unwrap();
    let original = client
        .posix_write_file(
            canonical,
            b"remote alias survives",
            DfsWriteOptions::replicated_eventual(nodes.clone()),
        )
        .unwrap();
    client.posix_hardlink(canonical, alias).unwrap();
    assert_eq!(count(&mut client, canonical), 2);
    let local = runtime.node_metadata(&users.primary_node_id).unwrap();
    assert!(
        local
            .first_hardlink_to_object(&fs, &original.object_id)
            .unwrap()
            .is_none(),
        "the actual canonical node has no replica of the only alias shard"
    );
    assert_eq!(
        local
            .get_object(&original.object_id)
            .unwrap()
            .unwrap()
            .namespace_link_count,
        Some(2)
    );
    drop(local);
    let remote_alias = runtime
        .node_metadata(&services.primary_node_id)
        .unwrap()
        .get_object_by_path(&fs, alias)
        .unwrap()
        .unwrap();
    assert_eq!(remote_alias.kind, DfsObjectKind::Hardlink);
    assert_eq!(
        remote_alias.link_target_object_id.as_ref(),
        Some(&original.object_id)
    );
    client.posix_unlink(canonical).unwrap_or_else(|error| {
        panic!("stored count requires authoritative foreign alias discovery: canonical={} alias={} error={error:?}", original.object_id, remote_alias.object_id)
    });
    assert_eq!(count(&mut client, alias), 1);
    assert_eq!(
        client.posix_stat_distributed(alias).unwrap().object_id,
        original.object_id
    );
    assert_eq!(
        client.posix_read_file_distributed(alias).unwrap(),
        b"remote alias survives"
    );
    assert!(matches!(
        client.posix_stat_distributed(canonical),
        Err(DfsPosixError::NotFound { .. })
    ));
    drop(client);
    runtime.close().unwrap();
}
#[test]
fn native_alias_unlink_resolves_canonical_outside_its_metadata_replica_set() {
    let (runtime, nodes) =
        DfsTestRuntime::start_cluster("native-unreplicated-alias-unlink", 3).unwrap();
    let fs = runtime.file_system().file_system_id;
    let users = runtime
        .reassign_metadata_shard(&fs, "users", nodes[0].clone(), Vec::new())
        .unwrap();
    let services = runtime
        .reassign_metadata_shard(&fs, "services", nodes[1].clone(), Vec::new())
        .unwrap();
    assert!(users.replica_node_ids.is_empty() && services.replica_node_ids.is_empty());
    let canonical = "/users/unreplicated-unlink-canonical";
    let alias = "/services/unreplicated-unlink-alias";
    let mut client = runtime.client(&nodes[2]).unwrap();
    let original = client
        .posix_write_file(
            canonical,
            b"canonical remains",
            DfsWriteOptions::replicated_eventual(nodes.clone()),
        )
        .unwrap();
    client.posix_hardlink(canonical, alias).unwrap();
    assert_eq!(count(&mut client, canonical), 2);
    let backend = runtime.node_metadata(&services.primary_node_id).unwrap();
    assert!(
        backend.get_object(&original.object_id).unwrap().is_none(),
        "the actual alias authority does not replicate the canonical shard"
    );
    let marker = backend.get_object_by_path(&fs, alias).unwrap().unwrap();
    assert_eq!(marker.kind, DfsObjectKind::Hardlink);
    assert_eq!(
        marker.link_target_object_id.as_ref(),
        Some(&original.object_id)
    );
    drop(backend);
    for _ in 0..2 {
        client
            .route_namespace_operation_with_request_id(
                "native-unreplicated-alias-unlink-exact".into(),
                services.primary_node_id.clone(),
                DfsNamespaceOperation::ConditionalUnlink {
                    path: alias.into(),
                    expected: DfsNamespaceObjectPrecondition {
                        object_id: marker.object_id.clone(),
                        version: marker.version.clone(),
                    },
                },
            )
            .unwrap();
        let current = client.posix_stat_distributed(canonical).unwrap();
        assert_eq!(current.object_id, original.object_id);
        assert_eq!(current.link_count, 1);
        assert_eq!(
            client.posix_read_file_distributed(canonical).unwrap(),
            b"canonical remains"
        );
        assert!(matches!(
            client.posix_stat_distributed(alias),
            Err(DfsPosixError::NotFound { .. })
        ));
    }
    assert_eq!(
        runtime
            .node_metadata(&users.primary_node_id)
            .unwrap()
            .get_object(&original.object_id)
            .unwrap()
            .unwrap()
            .namespace_link_count,
        Some(1)
    );
    drop(client);
    runtime.close().unwrap();
}
