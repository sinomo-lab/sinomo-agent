//! Cross shard regression tests.

use super::*;
#[test]
fn returning_adoption_checks_the_physical_eviction_head_before_publication() {
    let node = NodeId::generate();
    let (mut runtime, mut service) = native_sifs_service("native-returning-adoption", node.clone());
    let original = service
        .posix_write_file(
            "/services/old-location",
            b"returning object",
            DfsWriteOptions::replicated_eventual(vec![node.clone()]),
        )
        .unwrap();
    service
        .posix_rename("/services/old-location", "/users/returning-source")
        .unwrap();
    let eviction = service
        .metadata_backend()
        .unwrap()
        .get_object_on_shard(&DfsMetadataShardId::new("services"), &original.object_id)
        .unwrap()
        .unwrap();
    assert_eq!(eviction.kind, DfsObjectKind::Tombstone);
    assert!(matches!(
        eviction.tombstone_provenance,
        Some(DfsTombstoneProvenance::SourceEviction { .. })
    ));
    let returned = service
        .posix_rename("/users/returning-source", "/services/returned-location")
        .unwrap();
    assert_eq!(returned.object_id, original.object_id);
    assert_ne!(returned.version, eviction.version);
    assert_eq!(
        service
            .posix_read_file("/services/returned-location")
            .unwrap(),
        b"returning object"
    );
    assert!(matches!(
        service.posix_stat("/services/old-location"),
        Err(DfsPosixError::NotFound { .. })
    ));
    drop(service);
    runtime.stop_node(&node).unwrap();
    runtime.reopen_node(node.clone()).unwrap();
    let mut reopened = runtime.client(&node).unwrap();
    assert_eq!(
        reopened
            .posix_stat("/services/returned-location")
            .unwrap()
            .object_id,
        original.object_id
    );
    assert_eq!(
        reopened
            .posix_read_file("/services/returned-location")
            .unwrap(),
        b"returning object"
    );
    drop(reopened);
    runtime.close().unwrap();
}

#[test]
fn content_reference_release_replay_survives_two_moves_between_disjoint_authorities() {
    reference_release_replay_after_two_moves(false);
}

#[test]
fn content_reference_noop_release_replay_survives_two_moves_between_disjoint_authorities() {
    reference_release_replay_after_two_moves(true);
}

fn reference_release_replay_after_two_moves(no_op_release: bool) {
    let label = if no_op_release {
        "native-reference-noop-two-moves"
    } else {
        "native-reference-release-two-moves"
    };
    let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(label, 4);
    let mut runtime = crate::test_runtime::DfsTestRuntime::new(label, nodes[0].clone()).unwrap();
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).unwrap();
    }
    runtime
        .start_nodes()
        .expect("normal native membership and provider publication");
    for (shard, target) in [
        ("users", &nodes[0]),
        ("services", &nodes[1]),
        ("root", &nodes[2]),
    ] {
        let config = runtime
            .metadata_shards()
            .unwrap()
            .into_iter()
            .find(|config| config.shard_id == shard)
            .unwrap();
        if config.primary_node_id != *target {
            let replicas =
                crate::control_plane::topology_preserving_metadata_shard_replicas(&config, target)
                    .unwrap();
            runtime
                .reassign_metadata_shard(&sifs_file_system_id(), shard, target.clone(), replicas)
                .unwrap();
        }
    }
    let mut requester = runtime.client(&nodes[3]).unwrap();
    let bytes = b"original reference data";
    let stat = requester
        .posix_write_file(
            "/users/reference-a",
            bytes,
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(1, 1).unwrap(),
                replica_candidates: vec![nodes[0].clone()],
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    let identity = DfsRevisionReference {
        file_system_id: stat.file_system_id.clone(),
        object_id: stat.object_id.clone(),
        version: stat.version.clone(),
    };
    let capture = DfsContentReferenceRequest {
        namespace: "org.example.reference-moves".into(),
        path: stat.path.clone(),
        identity: identity.clone(),
        operation: DfsContentReferenceOperation::CaptureCurrent {
            reference_id: "capture-root".into(),
            metadata: None,
        },
        operation_id: sinomo_core::Id::generate().to_string(),
    };
    requester.retain_content_revision(capture.clone()).unwrap();
    if !no_op_release {
        requester
            .retain_content_revision(DfsContentReferenceRequest {
                operation: DfsContentReferenceOperation::Retain {
                    reference_id: "reused-leaf".into(),
                },
                operation_id: sinomo_core::Id::generate().to_string(),
                ..capture.clone()
            })
            .unwrap();
    }
    requester
        .posix_rename("/users/reference-a", "/services/reference-b")
        .unwrap();
    let release = DfsContentReferenceRequest {
        operation: DfsContentReferenceOperation::Release {
            reference_id: "reused-leaf".into(),
        },
        operation_id: sinomo_core::Id::generate().to_string(),
        ..capture.clone()
    };
    let before = no_op_release.then(|| requester.revision_metadata(&identity).unwrap().unwrap());
    requester.retain_content_revision(release.clone()).unwrap();
    if let Some(before) = before {
        let after = requester.revision_metadata(&identity).unwrap().unwrap();
        assert_eq!(after.current_object, before.current_object);
        assert_eq!(after.version, before.version);
    }
    let retain = DfsContentReferenceRequest {
        operation: DfsContentReferenceOperation::Retain {
            reference_id: "reused-leaf".into(),
        },
        operation_id: sinomo_core::Id::generate().to_string(),
        ..capture
    };
    requester.retain_content_revision(retain.clone()).unwrap();
    requester.retain_content_revision(release.clone()).unwrap();
    assert!(requester.content_reference_present(&retain).unwrap());
    let transaction_id = format!("file-history:{}", release.operation_id);
    let middle = runtime.client(&nodes[1]).unwrap();
    let admitted = middle
        .metadata_backend()
        .unwrap()
        .get_namespace_transaction_receipt(&transaction_id)
        .unwrap()
        .unwrap();
    drop(middle);
    requester
        .posix_rename("/services/reference-b", "/reference-c")
        .unwrap();
    requester.retain_content_revision(release.clone()).unwrap();
    assert!(requester.content_reference_present(&retain).unwrap());
    let mut changed = release.clone();
    changed.operation = DfsContentReferenceOperation::Release {
        reference_id: "different-leaf".into(),
    };
    assert!(requester.retain_content_revision(changed).is_err());
    assert!(requester.content_reference_present(&retain).unwrap());
    assert_eq!(
        requester
            .read_file_version_content(
                &identity,
                stat.content_hash.as_deref().unwrap(),
                stat.size_bytes
            )
            .unwrap(),
        bytes
    );
    let final_owner = runtime.client(&nodes[2]).unwrap();
    let transferred = final_owner
        .metadata_backend()
        .unwrap()
        .get_namespace_transaction_receipt(&transaction_id)
        .unwrap()
        .unwrap();
    assert!(admitted.same_outcome_identity(&transferred));
    assert_eq!(transferred.content_reference_identity, Some(identity));
    assert_eq!(
        transferred.contribution_digest,
        Some(
            blake3::hash(&serde_json::to_vec(&release).unwrap())
                .to_hex()
                .to_string()
        )
    );
    drop((final_owner, requester));
    runtime.close().expect("normal component owners stopped");
}

#[test]
fn mkdir_all_cross_shard_error_names_the_actual_child_parent_pair() {
    let (runtime, service, current) = crate::test_support::current_native_metadata_service(
        "native-create-pair-validation",
        1,
        "/services",
    )
    .unwrap();
    let child_path = "/services/batched/one/two";
    let parent_path = "/services/batched/one";
    let hash = child_path
        .as_bytes()
        .iter()
        .fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
        });
    let mut child = current.clone();
    child.shard_id = "hash-selected-leaf".into();
    child.assignment = DfsMetadataShardAssignment::NameHashRange {
        start_inclusive: hash,
        end_exclusive: hash.wrapping_add(1),
    };
    let before = service
        .metadata_backend()
        .unwrap()
        .metadata_prefix_proof(&DfsMetadataShardId::new(current.shard_id.clone()))
        .unwrap();
    let error = crate::service_commit::ensure_atomic_create_parent_touch_shard(
        &current.file_system_id,
        &current.primary_node_id,
        child_path,
        parent_path,
        &[current.clone(), child],
    )
    .unwrap_err();
    assert!(error.to_string().contains(&format!(
        "created object {child_path} and parent {parent_path}"
    )));
    assert_eq!(
        service
            .metadata_backend()
            .unwrap()
            .metadata_prefix_proof(&DfsMetadataShardId::new(current.shard_id))
            .unwrap(),
        before
    );
    drop(service);
    runtime.close().unwrap();
}

#[test]
fn streamed_routed_write_binds_quarantine_after_hardlink_rebase() {
    let name = "native-streamed-hardlink-storage-binding";
    let (runtime, service, config) =
        crate::test_support::current_native_metadata_service(name, 2, "/services").unwrap();
    let primary = config.primary_node_id;
    drop(service);
    let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 2);
    let requester = nodes.iter().find(|node| **node != primary).unwrap().clone();
    let mut policy = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 2, 2).unwrap();
    policy.required_node_ids = nodes.clone();
    runtime
        .apply_placement_policy(&primary, "/services", DfsPlacementScope::Subtree, policy)
        .unwrap();
    let mut writer = runtime.client(&requester).unwrap();
    let target_path = "/services/streamed-target.bin";
    let alias_path = "/services/streamed-alias.bin";
    let options = DfsWriteOptions {
        policy: DfsReplicationPolicy::new(2, 2).unwrap(),
        replica_candidates: nodes.clone(),
        wait_for_commit: true,
        foreground_replica_repair: true,
    };
    let original = writer
        .posix_write_file(target_path, b"actual original target", options.clone())
        .unwrap();
    writer.posix_hardlink(target_path, alias_path).unwrap();
    let alias = writer
        .metadata_backend()
        .unwrap()
        .get_object_by_path(&sifs_file_system_id(), alias_path)
        .unwrap()
        .unwrap();
    assert_eq!(alias.kind, DfsObjectKind::Hardlink);
    assert_eq!(
        alias.link_target_object_id,
        Some(original.object_id.clone())
    );
    assert_ne!(alias.object_id, original.object_id);
    let bytes = vec![0x5a; 2 * 1024 * 1024 + 8192];
    let before = runtime.rpc_observations().len();
    let committed = writer
        .posix_write_file_from_reader(alias_path, std::io::Cursor::new(&bytes), options)
        .unwrap();
    assert_eq!(committed.object_id, original.object_id);
    assert_eq!(writer.posix_read_file(target_path).unwrap(), bytes);
    assert_eq!(writer.posix_read_file(alias_path).unwrap(), bytes);
    assert!(
        runtime.rpc_observations()[before..]
            .iter()
            .any(|event| event.source == requester
                && event.target == primary
                && event.route == DFS_DATA_DISTRIBUTED_WRITE_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::Completed)
    );
    for node in &nodes {
        let actual = runtime.client(node).unwrap();
        assert!(
            actual
                .content()
                .read_manifest(alias.object_id.as_str(), committed.version.as_str())
                .is_err(),
            "the streamed write cannot reserve an alias object version"
        );
        let stored = actual
            .content()
            .read_blob(committed.content_hash.as_ref().unwrap())
            .unwrap();
        assert_eq!(stored, bytes);
        assert_eq!(
            content_hash(&stored),
            committed.content_hash.clone().unwrap()
        );
    }
    drop(writer);
    runtime.close().unwrap();
}

#[test]
fn sifs_recursive_clear_preserves_hardlinks_outside_cleared_tree() {
    let node_id = NodeId::from_legacy_named("node_sifs_clear_hardlink").expect("node");
    let clear_root = "/services/sandbox/instances/sandbox_clear_hardlink/tmp/0";
    let file_path = "/services/sandbox/instances/sandbox_clear_hardlink/tmp/0/cache.txt";
    let outside_alias = "/services/sandbox/instances/sandbox_clear_hardlink/work/cache-alias.txt";

    let (mut runtime, mut dfs) = native_sifs_service(
        "sifs_recursive_clear_preserves_hardlinks_outside_cleared_tree",
        node_id.clone(),
    );
    dfs.posix_mkdir_all(clear_root).expect("mkdir clear root");
    dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_clear_hardlink/work")
        .expect("mkdir work");
    dfs.posix_write_file(
        file_path,
        b"cache",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write cache");
    dfs.posix_set_times(file_path, Some(12_345), Some(67_890))
        .expect("old file times");
    let before = dfs.posix_stat(file_path).expect("file stat");
    let object_id = before.object_id.clone();
    dfs.posix_hardlink(file_path, outside_alias)
        .expect("hardlink outside");

    dfs.posix_clear_dir_recursive(clear_root)
        .expect("clear recursive");

    assert!(matches!(
        dfs.posix_stat(file_path),
        Err(DfsPosixError::NotFound { .. })
    ));
    let alias_stat = dfs.posix_stat(outside_alias).expect("alias survives");
    assert_eq!(alias_stat.object_id, object_id);
    assert_eq!(alias_stat.link_count, 1);
    assert_eq!(
        alias_stat.modified_at_unix_nanos, before.modified_at_unix_nanos,
        "clearing a hardlinked path must not change file content mtime"
    );
    assert!(
        alias_stat.changed_at_unix_nanos > before.changed_at_unix_nanos,
        "promoting a surviving hardlink alias must advance ctime because the canonical path changed"
    );
    assert_ne!(
        alias_stat.version, before.version,
        "promoting a surviving hardlink alias must advance the metadata version"
    );
    assert_eq!(
        dfs.posix_read_file(outside_alias).expect("read alias"),
        b"cache"
    );

    let reloaded = runtime.client(&node_id).unwrap();
    let alias_stat = reloaded
        .posix_stat(outside_alias)
        .expect("reloaded alias survives");
    assert_eq!(alias_stat.object_id, object_id);
    assert_eq!(alias_stat.link_count, 1);
    assert_eq!(
        reloaded
            .posix_read_file(outside_alias)
            .expect("reloaded read alias"),
        b"cache"
    );
}

#[test]
fn sifs_posix_hardlink_shares_content_and_link_count() {
    let root = temp_root("sifs-posix-hardlink-shared-content");
    let node_id = NodeId::from_legacy_named("node_sifs_hardlink").expect("node");
    let original = "/services/sandbox/instances/sandbox_hardlink/work/file.txt";
    let link = "/services/sandbox/instances/sandbox_hardlink/work/alias.txt";

    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_posix_hardlink_shares_content_and_link_count",
        node_id.clone(),
    );
    dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_hardlink/work")
        .expect("mkdir");
    dfs.posix_write_file(
        original,
        b"original",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write original");
    dfs.posix_set_times(original, Some(12_345), Some(67_890))
        .expect("set old times");
    dfs.posix_hardlink(original, link).expect("hardlink");

    let linked_ctime = dfs
        .posix_stat(original)
        .expect("original linked stat")
        .changed_at_unix_nanos;
    assert!(linked_ctime > 67_890);
    assert_eq!(
        dfs.posix_stat(original).expect("original stat").link_count,
        2
    );
    assert_eq!(dfs.posix_stat(link).expect("link stat").link_count, 2);
    dfs.posix_write_file(
        link,
        b"updated",
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("write link");
    assert_eq!(
        dfs.posix_read_file(original).expect("read original"),
        b"updated"
    );
    assert_eq!(dfs.posix_read_file(link).expect("read link"), b"updated");

    dfs.posix_unlink(link).expect("unlink alias");
    let remaining_stat = dfs.posix_stat(original).expect("remaining stat");
    assert_eq!(remaining_stat.link_count, 1);
    assert!(remaining_stat.changed_at_unix_nanos >= linked_ctime);
    assert!(matches!(
        dfs.posix_read_file(link),
        Err(DfsPosixError::NotFound { .. })
    ));

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_hardlink_after_extent_append_preserves_opened_range_reads_after_restart() {
    let node_id = NodeId::from_legacy_named("node_sifs_hardlink_extent").expect("node");
    let work = "/services/sandbox/instances/sandbox_hardlink_extent/work";
    let original = "/services/sandbox/instances/sandbox_hardlink_extent/work/file.txt";
    let link = "/services/sandbox/instances/sandbox_hardlink_extent/work/alias.txt";
    let expected = b"alpha-beta";

    let (mut runtime, mut dfs) = native_sifs_service(
        "sifs_posix_hardlink_after_extent_append_preserves_opened_range_reads_after_restart",
        node_id.clone(),
    );
    dfs.posix_mkdir_all(work).expect("mkdir");
    dfs.posix_write_file(
        original,
        b"alpha",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write base");
    let appended = dfs
        .posix_write_file_at(
            original,
            5,
            b"-beta",
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("append extent patch");
    let appended_hash = appended.content_hash.clone().expect("appended hash");
    assert!(
        !dfs.content
            .has_blob(&appended_hash)
            .expect("aggregate blob lookup"),
        "the regression requires an extent manifest without a monolithic aggregate blob"
    );

    dfs.posix_hardlink(original, link).expect("hardlink");
    let object_id = dfs.posix_stat(original).expect("linked stat").object_id;
    for path in [original, link] {
        assert_eq!(
            dfs.posix_read_file_range_for_opened_object(
                path,
                &object_id,
                0,
                expected.len() + 16,
                false,
            )
            .expect("opened range after metadata-only hardlink bump"),
            expected
        );
    }

    drop(dfs);
    runtime.stop_node(&node_id).unwrap();
    runtime.reopen_node(node_id.clone()).unwrap();
    let mut reopened = runtime.client(&node_id).unwrap();
    for path in [original, link] {
        assert_eq!(
            reopened
                .posix_read_file_range_for_opened_object(
                    path,
                    &object_id,
                    0,
                    expected.len() + 16,
                    false,
                )
                .expect("opened range after content-store restart"),
            expected
        );
    }
}

#[test]
fn sifs_posix_hardlink_preserves_symlink_object() {
    let root = temp_root("sifs-posix-hardlink-symlink");
    let node_id = NodeId::from_legacy_named("node_sifs_hardlink_symlink").expect("node");
    let work = "/services/sandbox/instances/sandbox_hardlink_symlink/work";
    let target = "/services/sandbox/instances/sandbox_hardlink_symlink/work/target.txt";
    let link = "/services/sandbox/instances/sandbox_hardlink_symlink/work/link.txt";
    let alias = "/services/sandbox/instances/sandbox_hardlink_symlink/work/link-alias.txt";

    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_posix_hardlink_preserves_symlink_object",
        node_id.clone(),
    );
    dfs.posix_mkdir_all(work).expect("mkdir");
    dfs.posix_write_file(
        target,
        b"target",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write target");
    dfs.posix_symlink(
        "target.txt",
        link,
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("symlink");
    dfs.posix_hardlink(link, alias).expect("hardlink symlink");

    let link_stat = dfs.posix_lstat(link).expect("link lstat");
    let alias_stat = dfs.posix_lstat(alias).expect("alias lstat");
    assert_eq!(link_stat.kind, DfsObjectKind::Symlink);
    assert_eq!(alias_stat.kind, DfsObjectKind::Symlink);
    assert_eq!(link_stat.object_id, alias_stat.object_id);
    assert_eq!(link_stat.link_count, 2);
    assert_eq!(alias_stat.link_count, 2);
    assert_eq!(
        dfs.posix_readlink(alias).expect("alias readlink"),
        "target.txt"
    );

    dfs.posix_unlink(link).expect("unlink original symlink");
    assert!(matches!(
        dfs.posix_readlink(link),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert_eq!(
        dfs.posix_readlink(alias).expect("remaining alias readlink"),
        "target.txt"
    );
    assert_eq!(
        dfs.posix_lstat(alias).expect("remaining alias").link_count,
        1
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_unlink_promotes_hardlink_when_canonical_removed() {
    let root = temp_root("sifs-posix-hardlink-promote");
    let node_id = NodeId::from_legacy_named("node_sifs_hardlink_promote").expect("node");
    let original = "/services/sandbox/instances/sandbox_hardlink_promote/work/file.txt";
    let link = "/services/sandbox/instances/sandbox_hardlink_promote/work/alias.txt";

    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_posix_unlink_promotes_hardlink_when_canonical_removed",
        node_id.clone(),
    );
    dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_hardlink_promote/work")
        .expect("mkdir");
    dfs.posix_write_file(
        original,
        b"content",
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("write original");
    let original_object_id = dfs.posix_stat(original).expect("original stat").object_id;
    dfs.posix_hardlink(original, link).expect("hardlink");

    dfs.posix_unlink(original).expect("unlink canonical");
    assert!(matches!(
        dfs.posix_read_file(original),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert_eq!(
        dfs.posix_read_file(link).expect("read promoted"),
        b"content"
    );
    let promoted = dfs.posix_stat(link).expect("promoted stat");
    assert_eq!(promoted.object_id, original_object_id);
    assert_eq!(promoted.link_count, 1);

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_rename_hardlink_over_canonical_preserves_target() {
    let node_id = NodeId::from_legacy_named("node_sifs_rename_hardlink").expect("node");
    let canonical = "/services/sandbox/instances/sandbox_rename_hardlink/work/file.txt";
    let alias = "/services/sandbox/instances/sandbox_rename_hardlink/work/alias.txt";

    let (mut runtime, mut dfs) = native_sifs_service(
        "sifs_posix_rename_hardlink_over_canonical_preserves_target",
        node_id.clone(),
    );
    dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_rename_hardlink/work")
        .expect("mkdir");
    dfs.posix_write_file(
        canonical,
        b"content",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write canonical");
    let canonical_object_id = dfs.posix_stat(canonical).expect("canonical stat").object_id;
    dfs.posix_hardlink(canonical, alias).expect("hardlink");
    assert_eq!(dfs.posix_stat(alias).expect("alias stat").link_count, 2);

    dfs.posix_rename(alias, canonical)
        .expect("rename alias over canonical");

    let alias_stat = dfs.posix_stat(alias).expect("alias remains");
    let canonical_stat = dfs.posix_stat(canonical).expect("canonical remains");
    assert_eq!(canonical_stat.object_id, canonical_object_id);
    assert_eq!(alias_stat.object_id, canonical_object_id);
    assert_eq!(canonical_stat.link_count, 2);
    assert_eq!(alias_stat.link_count, 2);
    assert_eq!(
        dfs.posix_read_file(canonical).expect("read canonical"),
        b"content"
    );
    assert_eq!(dfs.posix_read_file(alias).expect("read alias"), b"content");

    let reloaded = runtime.client(&node_id).unwrap();
    assert_eq!(
        reloaded
            .posix_stat(alias)
            .expect("reloaded alias")
            .link_count,
        2
    );
    assert_eq!(
        reloaded.posix_read_file(canonical).expect("reloaded read"),
        b"content"
    );
}

#[test]
fn sifs_posix_rename_over_canonical_preserves_other_hardlinks() {
    let node_id =
        NodeId::from_legacy_named("node_sifs_rename_over_canonical_hardlinks").expect("node");
    let work = "/services/sandbox/instances/sandbox_rename_over_hardlinks/work";
    let source = "/services/sandbox/instances/sandbox_rename_over_hardlinks/work/source.txt";
    let canonical = "/services/sandbox/instances/sandbox_rename_over_hardlinks/work/canonical.txt";
    let alias = "/services/sandbox/instances/sandbox_rename_over_hardlinks/work/alias.txt";

    let (mut runtime, mut dfs) = native_sifs_service(
        "sifs_posix_rename_over_canonical_preserves_other_hardlinks",
        node_id.clone(),
    );
    dfs.posix_mkdir_all(work).expect("mkdir");
    dfs.posix_write_file(
        canonical,
        b"old",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write canonical");
    let old_object_id = dfs.posix_stat(canonical).expect("canonical stat").object_id;
    dfs.posix_hardlink(canonical, alias).expect("hardlink");
    dfs.posix_write_file(
        source,
        b"new",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write source");
    let source_object_id = dfs.posix_stat(source).expect("source stat").object_id;

    dfs.posix_rename(source, canonical)
        .expect("rename source over canonical");

    assert!(matches!(
        dfs.posix_stat(source),
        Err(DfsPosixError::NotFound { .. })
    ));
    let canonical_stat = dfs
        .posix_stat(canonical)
        .expect("canonical stat after rename");
    let alias_stat = dfs.posix_stat(alias).expect("alias stat after rename");
    assert_eq!(canonical_stat.object_id, source_object_id);
    assert_eq!(canonical_stat.link_count, 1);
    assert_eq!(alias_stat.object_id, old_object_id);
    assert_eq!(alias_stat.link_count, 1);
    assert_eq!(
        dfs.posix_read_file(canonical).expect("read canonical"),
        b"new"
    );
    assert_eq!(dfs.posix_read_file(alias).expect("read alias"), b"old");

    let reloaded = runtime.client(&node_id).unwrap();
    assert_eq!(
        reloaded
            .posix_stat(alias)
            .expect("reloaded alias")
            .object_id,
        old_object_id
    );
    assert_eq!(
        reloaded
            .posix_read_file(canonical)
            .expect("reloaded canonical"),
        b"new"
    );
    assert_eq!(
        reloaded.posix_read_file(alias).expect("reloaded alias"),
        b"old"
    );
}

#[test]
fn sifs_posix_rename_hardlink_over_canonical_preserves_both_targets() {
    let node_id =
        NodeId::from_legacy_named("node_sifs_rename_hardlink_over_other_canonical").expect("node");
    let work = "/services/sandbox/instances/sandbox_rename_hardlink_over_other/work";
    let source_canonical =
        "/services/sandbox/instances/sandbox_rename_hardlink_over_other/work/source.txt";
    let source_alias =
        "/services/sandbox/instances/sandbox_rename_hardlink_over_other/work/source-alias.txt";
    let dest_canonical =
        "/services/sandbox/instances/sandbox_rename_hardlink_over_other/work/dest.txt";
    let dest_alias =
        "/services/sandbox/instances/sandbox_rename_hardlink_over_other/work/dest-alias.txt";

    let (mut runtime, mut dfs) = native_sifs_service(
        "sifs_posix_rename_hardlink_over_canonical_preserves_both_targets",
        node_id.clone(),
    );
    dfs.posix_mkdir_all(work).expect("mkdir");
    dfs.posix_write_file(
        source_canonical,
        b"source",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write source");
    dfs.posix_hardlink(source_canonical, source_alias)
        .expect("source hardlink");
    let source_object_id = dfs
        .posix_stat(source_canonical)
        .expect("source stat")
        .object_id;
    dfs.posix_write_file(
        dest_canonical,
        b"dest",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write dest");
    dfs.posix_hardlink(dest_canonical, dest_alias)
        .expect("dest hardlink");
    let dest_object_id = dfs.posix_stat(dest_canonical).expect("dest stat").object_id;

    dfs.posix_rename(source_alias, dest_canonical)
        .expect("rename source alias over dest canonical");

    let source_stat = dfs
        .posix_stat(source_canonical)
        .expect("source canonical remains");
    let renamed_stat = dfs.posix_stat(dest_canonical).expect("renamed source");
    let dest_alias_stat = dfs.posix_stat(dest_alias).expect("dest alias remains");
    assert_eq!(source_stat.object_id, source_object_id);
    assert_eq!(renamed_stat.object_id, source_object_id);
    assert_eq!(source_stat.link_count, 2);
    assert_eq!(renamed_stat.link_count, 2);
    assert_eq!(dest_alias_stat.object_id, dest_object_id);
    assert_eq!(dest_alias_stat.link_count, 1);
    assert_eq!(
        dfs.posix_read_file(source_canonical)
            .expect("read source canonical"),
        b"source"
    );
    assert_eq!(
        dfs.posix_read_file(dest_canonical)
            .expect("read renamed source"),
        b"source"
    );
    assert_eq!(
        dfs.posix_read_file(dest_alias).expect("read dest alias"),
        b"dest"
    );
    assert!(matches!(
        dfs.posix_stat(source_alias),
        Err(DfsPosixError::NotFound { .. })
    ));

    let reloaded = runtime.client(&node_id).unwrap();
    assert_eq!(
        reloaded
            .posix_stat(dest_canonical)
            .expect("reloaded renamed source")
            .object_id,
        source_object_id
    );
    assert_eq!(
        reloaded
            .posix_stat(dest_alias)
            .expect("reloaded dest alias")
            .object_id,
        dest_object_id
    );
}

#[test]
fn sifs_posix_rename_hardlink_alias_updates_target_ctime() {
    let root = temp_root("sifs-posix-rename-hardlink-alias-ctime");
    let node_id = NodeId::from_legacy_named("node_sifs_rename_hardlink_alias_ctime").expect("node");
    let work = "/services/sandbox/instances/sandbox_rename_alias_ctime/work";
    let canonical = "/services/sandbox/instances/sandbox_rename_alias_ctime/work/canonical.txt";
    let alias = "/services/sandbox/instances/sandbox_rename_alias_ctime/work/alias.txt";
    let renamed = "/services/sandbox/instances/sandbox_rename_alias_ctime/work/renamed-alias.txt";

    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_posix_rename_hardlink_alias_updates_target_ctime",
        node_id.clone(),
    );
    dfs.posix_mkdir_all(work).expect("mkdir");
    dfs.posix_write_file(
        canonical,
        b"content",
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("write canonical");
    dfs.posix_hardlink(canonical, alias).expect("hardlink");
    let before = dfs.posix_stat(canonical).expect("before stat");
    std::thread::sleep(std::time::Duration::from_millis(1));

    dfs.posix_rename(alias, renamed)
        .expect("rename hardlink alias");

    let canonical_after = dfs.posix_stat(canonical).expect("canonical remains");
    let renamed_after = dfs.posix_stat(renamed).expect("renamed alias");
    assert_eq!(canonical_after.object_id, before.object_id);
    assert_eq!(renamed_after.object_id, before.object_id);
    assert_eq!(canonical_after.link_count, 2);
    assert_eq!(renamed_after.link_count, 2);
    assert!(
        canonical_after.changed_at_unix_nanos > before.changed_at_unix_nanos,
        "renaming a hardlink alias must advance target inode ctime"
    );
    assert_eq!(
        canonical_after.changed_at_unix_nanos,
        renamed_after.changed_at_unix_nanos
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_rename_over_hardlink_alias_updates_replaced_target_ctime() {
    let root = temp_root("sifs-posix-rename-over-hardlink-alias-ctime");
    let node_id =
        NodeId::from_legacy_named("node_sifs_rename_over_hardlink_alias_ctime").expect("node");
    let work = "/services/sandbox/instances/sandbox_rename_over_alias_ctime/work";
    let source = "/services/sandbox/instances/sandbox_rename_over_alias_ctime/work/source.txt";
    let canonical =
        "/services/sandbox/instances/sandbox_rename_over_alias_ctime/work/canonical.txt";
    let alias = "/services/sandbox/instances/sandbox_rename_over_alias_ctime/work/alias.txt";

    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_posix_rename_over_hardlink_alias_updates_replaced_target_ctime",
        node_id.clone(),
    );
    dfs.posix_mkdir_all(work).expect("mkdir");
    dfs.posix_write_file(
        canonical,
        b"old",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write canonical");
    dfs.posix_hardlink(canonical, alias).expect("hardlink");
    let old_before = dfs.posix_stat(canonical).expect("old before");
    dfs.posix_write_file(
        source,
        b"new",
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("write source");
    let source_object_id = dfs.posix_stat(source).expect("source stat").object_id;
    std::thread::sleep(std::time::Duration::from_millis(1));

    dfs.posix_rename(source, alias)
        .expect("rename source over hardlink alias");

    let canonical_after = dfs.posix_stat(canonical).expect("canonical remains");
    let alias_after = dfs.posix_stat(alias).expect("alias replaced");
    assert_eq!(canonical_after.object_id, old_before.object_id);
    assert_eq!(canonical_after.link_count, 1);
    assert!(
        canonical_after.changed_at_unix_nanos > old_before.changed_at_unix_nanos,
        "replacing a hardlink alias must advance the replaced target inode ctime"
    );
    assert_eq!(alias_after.object_id, source_object_id);
    assert_eq!(alias_after.link_count, 1);
    assert_eq!(dfs.posix_read_file(canonical).expect("read old"), b"old");
    assert_eq!(dfs.posix_read_file(alias).expect("read new"), b"new");

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_set_times_updates_hardlink_target_metadata() {
    let root = temp_root("sifs-posix-set-times-hardlink");
    let node_id = NodeId::from_legacy_named("node_sifs_set_times").expect("node");
    let original = "/services/sandbox/instances/sandbox_set_times/work/file.txt";
    let link = "/services/sandbox/instances/sandbox_set_times/work/alias.txt";

    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_posix_set_times_updates_hardlink_target_metadata",
        node_id.clone(),
    );
    dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_set_times/work")
        .expect("mkdir");
    dfs.posix_write_file(
        original,
        b"content",
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("write original");
    dfs.posix_hardlink(original, link).expect("hardlink");

    dfs.posix_set_times(link, Some(12_345), Some(67_890))
        .expect("set times through link");
    let original_stat = dfs.posix_stat(original).expect("original stat");
    let link_stat = dfs.posix_stat(link).expect("link stat");
    assert_eq!(original_stat.accessed_at_unix_nanos, 12_345);
    assert_eq!(original_stat.modified_at_unix_nanos, 67_890);
    assert_eq!(link_stat.accessed_at_unix_nanos, 12_345);
    assert_eq!(link_stat.modified_at_unix_nanos, 67_890);
    assert!(original_stat.changed_at_unix_nanos > 67_890);
    assert_eq!(
        original_stat.changed_at_unix_nanos,
        link_stat.changed_at_unix_nanos
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_set_owner_updates_hardlink_target_metadata() {
    let root = temp_root("sifs-posix-set-owner-hardlink");
    let node_id = NodeId::from_legacy_named("node_sifs_set_owner").expect("node");
    let original = "/services/sandbox/instances/sandbox_set_owner/work/file.txt";
    let link = "/services/sandbox/instances/sandbox_set_owner/work/alias.txt";

    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_posix_set_owner_updates_hardlink_target_metadata",
        node_id.clone(),
    );
    dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_set_owner/work")
        .expect("mkdir");
    dfs.posix_write_file(
        original,
        b"content",
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("write original");
    dfs.posix_hardlink(original, link).expect("hardlink");

    dfs.posix_set_owner(link, 1001, 1002)
        .expect("set owner through link");
    let original_stat = dfs.posix_stat(original).expect("original stat");
    let link_stat = dfs.posix_stat(link).expect("link stat");
    assert_eq!(original_stat.uid, 1001);
    assert_eq!(original_stat.gid, 1002);
    assert_eq!(link_stat.uid, 1001);
    assert_eq!(link_stat.gid, 1002);
    assert_eq!(
        original_stat.changed_at_unix_nanos,
        link_stat.changed_at_unix_nanos
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_bulk_overwrite_preserves_hardlink_and_symlink_fallback_semantics() {
    let root = temp_root("sifs-posix-bulk-overwrite-links");
    let node_id = NodeId::from_legacy_named("node_sifs_bulk_overwrite_links").expect("node");
    let work = "/services/sandbox/instances/sandbox_bulk_overwrite_links/work";
    let target = "/services/sandbox/instances/sandbox_bulk_overwrite_links/work/target.txt";
    let hardlink = "/services/sandbox/instances/sandbox_bulk_overwrite_links/work/hard.txt";
    let symlink = "/services/sandbox/instances/sandbox_bulk_overwrite_links/work/sym.txt";
    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_posix_bulk_overwrite_preserves_hardlink_and_symlink_fallback_semantics",
        node_id.clone(),
    );
    dfs.posix_mkdir_all(work).expect("mkdir");
    dfs.posix_write_file(
        target,
        b"initial",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write target");
    dfs.posix_hardlink(target, hardlink).expect("hardlink");
    dfs.posix_symlink(
        "target.txt",
        symlink,
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("symlink");

    crate::metadata_read_snapshot::reset_metadata_durable_table_probes();
    crate::posix::reset_posix_bulk_prep_durable_table_probes();
    dfs.posix_write_files_local_eventual_with_known_parents([(
        hardlink,
        b"through hardlink".as_slice(),
    )])
    .expect("bulk write through hardlink");
    assert_eq!(
        crate::posix::posix_bulk_prep_durable_table_probes(),
        0,
        "hardlink target discovery must remain inside the pinned metadata generation"
    );
    assert_eq!(
        dfs.posix_read_file(target).expect("read hardlink target"),
        b"through hardlink"
    );

    crate::posix::reset_posix_bulk_prep_durable_table_probes();
    dfs.posix_write_files_local_eventual_with_known_parents([(
        symlink,
        b"through symlink".as_slice(),
    )])
    .expect("bulk write through symlink fallback");
    assert_eq!(
        crate::posix::posix_bulk_prep_durable_table_probes(),
        0,
        "symlink classification must not resume per-record durable table polling"
    );
    assert_eq!(
        dfs.posix_read_file(target).expect("read symlink target"),
        b"through symlink"
    );

    crate::metadata_read_snapshot::reset_metadata_durable_table_probes();
    dfs.posix_stat(target).expect("stat after symlink fallback");
    assert!(
        crate::metadata_read_snapshot::metadata_durable_table_probes() > 0,
        "symlink fallback must run after and leave behind no pinned metadata generation"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_write_at_merges_bytes_and_preserves_hardlink_identity() {
    let root = temp_root("sifs-posix-write-at-hardlink");
    let node_id = NodeId::from_legacy_named("node_sifs_write_at").expect("node");
    let work = "/services/sandbox/instances/sandbox_write_at/work";
    let original = "/services/sandbox/instances/sandbox_write_at/work/file.txt";
    let alias = "/services/sandbox/instances/sandbox_write_at/work/alias.txt";

    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_posix_write_at_merges_bytes_and_preserves_hardlink_identity",
        node_id.clone(),
    );
    dfs.posix_mkdir_all(work).expect("mkdir");
    dfs.posix_write_file(
        original,
        b"abcdef",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write original");
    dfs.posix_hardlink(original, alias).expect("hardlink");

    let stat = dfs
        .posix_write_file_at(
            alias,
            2,
            b"XYZ",
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("write at hardlink");

    assert_eq!(stat.size_bytes, Some(6));
    assert_eq!(
        dfs.posix_read_file(original).expect("read original"),
        b"abXYZf"
    );
    assert_eq!(dfs.posix_read_file(alias).expect("read alias"), b"abXYZf");
    let original_stat = dfs.posix_stat(original).expect("original stat");
    let alias_stat = dfs.posix_stat(alias).expect("alias stat");
    assert_eq!(original_stat.object_id, alias_stat.object_id);
    assert_eq!(original_stat.link_count, 2);
    assert_eq!(alias_stat.link_count, 2);

    let stat = dfs
        .posix_write_file_at(
            original,
            8,
            b"!",
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("sparse write");

    assert_eq!(stat.size_bytes, Some(9));
    assert_eq!(
        dfs.posix_read_file(alias).expect("read sparse alias"),
        b"abXYZf\0\0!"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_truncate_preserves_hardlink_identity() {
    let root = temp_root("sifs-posix-truncate-hardlink");
    let node_id = NodeId::from_legacy_named("node_sifs_truncate").expect("node");
    let work = "/services/sandbox/instances/sandbox_truncate/work";
    let original = "/services/sandbox/instances/sandbox_truncate/work/file.txt";
    let alias = "/services/sandbox/instances/sandbox_truncate/work/alias.txt";

    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_posix_truncate_preserves_hardlink_identity",
        node_id.clone(),
    );
    dfs.posix_mkdir_all(work).expect("mkdir");
    dfs.posix_write_file(
        original,
        b"abcdef",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write original");
    dfs.posix_hardlink(original, alias).expect("hardlink");

    let stat = dfs
        .posix_truncate_file(
            alias,
            3,
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("truncate through hardlink");
    assert_eq!(stat.size_bytes, Some(3));
    assert_eq!(dfs.posix_read_file(original).expect("read"), b"abc");
    assert_eq!(dfs.posix_read_file(alias).expect("read alias"), b"abc");
    assert_eq!(
        dfs.posix_stat(original).expect("original stat").object_id,
        dfs.posix_stat(alias).expect("alias stat").object_id
    );

    let stat = dfs
        .posix_truncate_file(
            original,
            5,
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("extend sparse through original");
    assert_eq!(stat.size_bytes, Some(5));
    assert_eq!(
        dfs.posix_read_file(alias).expect("read sparse alias"),
        b"abc\0\0"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_relink_after_canonical_unlink_does_not_create_hardlink_cycle() {
    let root = temp_root("sifs-posix-relink-after-canonical-unlink");
    let node_id = NodeId::from_legacy_named("node_sifs_relink_after_unlink").expect("node");
    let work = "/services/sandbox/instances/sandbox_relink_after_unlink/work";
    let original = "/services/sandbox/instances/sandbox_relink_after_unlink/work/file.txt";
    let alias = "/services/sandbox/instances/sandbox_relink_after_unlink/work/alias.txt";

    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_posix_relink_after_canonical_unlink_does_not_create_hardlink_cycle",
        node_id.clone(),
    );
    dfs.posix_mkdir_all(work).expect("mkdir");
    dfs.posix_write_file(
        original,
        b"content",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write original");
    dfs.posix_hardlink(original, alias).expect("hardlink alias");
    dfs.posix_write_file(
        alias,
        b"updated",
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("write alias");
    dfs.posix_unlink(original).expect("unlink canonical path");

    dfs.posix_hardlink(alias, original)
        .expect("relink original path");

    let original_stat = dfs.posix_stat(original).expect("original stat");
    let alias_stat = dfs.posix_stat(alias).expect("alias stat");
    assert_eq!(original_stat.object_id, alias_stat.object_id);
    assert_eq!(original_stat.link_count, 2);
    assert_eq!(alias_stat.link_count, 2);
    assert_eq!(
        dfs.posix_read_file(original).expect("read original"),
        b"updated"
    );
    assert_eq!(dfs.posix_read_file(alias).expect("read alias"), b"updated");

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_cluster_posix_contract_hardlink_link_count_races() {
    let mut cluster = sifs_posix_cluster("sifs-contract-hardlink-races", "/users");
    let scenario = "hardlink-link-count-races";
    let file = "/users/contract-hardlink.txt";
    let alias = "/users/contract-hardlink-alias.txt";
    let replacement = "/users/contract-hardlink-replacement.txt";
    let bytes = b"hardlink identity".to_vec();
    let updated = b"updated through alias".to_vec();
    let mut observations = Vec::new();

    sifs_cluster_primary_posix_write(&mut cluster, file, &bytes);
    let link = cluster
        .primary
        .posix_hardlink(file, alias)
        .expect("hardlink");
    record_posix_observation(
        &mut observations,
        scenario,
        "hardlink",
        &cluster.primary_node,
        alias,
        Instant::now(),
        "committed",
        Some(&link),
    );
    assert_eq!(
        cluster.primary.posix_stat(file).expect("file").link_count,
        2
    );
    let alias_write = cluster.writer.posix_write_file(
        alias,
        &updated,
        sifs_cluster_write_options(&cluster.writer_node.clone()),
    );
    let alias_write_request = match &alias_write {
        Ok(stat) => {
            record_posix_observation(
                &mut observations,
                scenario,
                "alias-write",
                &cluster.writer_node,
                alias,
                Instant::now(),
                "committed",
                Some(stat),
            );
            None
        }
        Err(DfsPosixError::PendingWrite { intent_id, .. }) => Some(intent_id.clone()),
        Err(error) => panic!("alias write failed unexpectedly: {error}"),
    };
    sifs_posix_cluster_pump_until(&mut cluster, "alias write", |cluster| {
        alias_write_request.as_ref().is_none_or(|intent| {
            !matches!(
                cluster.writer.write_outcome(intent),
                Ok(NetworkedDfsWriteOutcome::Pending { .. })
            )
        }) && cluster
            .writer
            .posix_stat(alias)
            .is_ok_and(|stat| stat.content_hash.as_deref() == Some(content_hash(&updated).as_str()))
    });
    sifs_cluster_primary_posix_write(&mut cluster, replacement, b"replacement");
    cluster
        .primary
        .posix_rename(replacement, file)
        .expect("rename over canonical");
    let alias_stat = cluster.primary.posix_stat(alias).expect("alias stat");
    assert_eq!(alias_stat.link_count, 1);
    assert_eq!(
        cluster
            .primary
            .posix_read_file(alias)
            .expect("alias content"),
        updated,
        "alias must retain original object content after canonical replacement"
    );
    assert_ne!(
        cluster
            .primary
            .posix_stat(file)
            .expect("new canonical")
            .object_id,
        alias_stat.object_id,
        "rename-over-canonical must not collapse distinct hardlink targets"
    );
    assert_sifs_cluster_no_posix_leaks(&cluster, scenario);
    finish_posix_observations(&observations);
    drop(cluster);
}

#[test]
fn native_owner_restart_recovers_its_actual_running_cross_shard_job() {
    native_cross_shard_move_recovers_from_real_carrier_failure(
        "native-restarted-job-owner",
        true,
        true,
    );
}

#[test]
fn request_shells_share_live_cross_shard_namespace_guards() {
    let node = NodeId::from_legacy_named("node_shared_cross_shard_guards").expect("node");
    let (runtime, global_shell) =
        native_sifs_service("native-shared-cross-shard-guards", node.clone());
    let file_system = runtime.file_system();
    let registry = global_shell.cross_shard_namespace_lock_registry();
    let exact_shell = runtime
        .client(&node)
        .unwrap()
        .with_cross_shard_namespace_lock_registry(registry.clone());

    registry.upsert_job(
        None,
        "shared-guard-job".to_string(),
        file_system.file_system_id.clone(),
        "/services/moving-tree".to_string(),
    );
    assert!(matches!(
        exact_shell.ensure_path_not_in_cross_shard_flight(
            &file_system.file_system_id,
            "/services/moving-tree/child.txt",
        ),
        Err(DfsError::Busy { .. })
    ));

    registry.remove_job("shared-guard-job");
    exact_shell
        .ensure_path_not_in_cross_shard_flight(
            &file_system.file_system_id,
            "/services/moving-tree/child.txt",
        )
        .expect("unlock is visible to every request shell");
    assert!(
        global_shell.cross_shard_namespace_locks.is_empty(),
        "the global shell observes the same job removal"
    );
    drop(exact_shell);
    drop(global_shell);
    runtime.close().unwrap();
}

#[test]
fn cross_shard_namespace_guards_are_job_owned_and_shard_replaceable() {
    let registry = DfsCrossShardNamespaceLockRegistry::default();
    let file_system_id = sifs_file_system_id();
    let root_shard = DfsMetadataShardId::new("root");
    let services_shard = DfsMetadataShardId::new("services");
    let path = "/services/reused-path";
    registry.replace_jobs([
        (
            Some(root_shard.clone()),
            "older-job".to_string(),
            file_system_id.clone(),
            path.to_string(),
        ),
        (
            Some(services_shard.clone()),
            "newer-job".to_string(),
            file_system_id.clone(),
            path.to_string(),
        ),
    ]);

    registry.remove_job("older-job");
    assert!(
        registry.blocks(&file_system_id, path),
        "a delayed terminal transition must not remove another job's same-path guard"
    );

    registry.replace_shard_jobs(
        &services_shard,
        std::iter::empty::<(String, DfsFileSystemId, Vec<String>)>(),
    );
    assert!(
        !registry.blocks(&file_system_id, path),
        "an exact shard snapshot with no running jobs removes that shard's obsolete guards"
    );
    assert!(root_shard != services_shard);
}

#[test]
fn sifs_cluster_metadata_less_bulk_overwrite_follows_revalidated_hardlink_target() {
    let mut cluster = sifs_two_primary_cluster("sifs-bulk-metadata-less-hardlink");
    let remote_dir = "/services/bulk-metadata-less-hardlink";
    let target = "/services/bulk-metadata-less-hardlink/target.txt";
    let link = "/services/bulk-metadata-less-hardlink/link.txt";
    cluster
        .services_primary
        .posix_mkdir_all(remote_dir)
        .expect("remote hardlink parent");
    sifs_two_primary_publish_until(&mut cluster, "metadata-less hardlink parent", |cluster| {
        cluster.replica.posix_stat(remote_dir).is_ok()
    });

    let SifsTwoPrimaryCluster {
        shared_kv,
        services_primary_node,
        users_primary_node,
        root,
        _runtime,
        services_primary,
        replica_node,
        mut replica,
        ..
    } = cluster;
    let stop = Arc::new(AtomicBool::new(false));
    let services_stop = Arc::clone(&stop);
    let services_thread =
        std::thread::spawn(move || pump_sifs_service_until_stop(services_primary, services_stop));
    let options = DfsWriteOptions::replicated_eventual(vec![replica_node]);
    replica
        .posix_write_file(target, b"before", options)
        .expect("write hardlink target");
    replica
        .posix_hardlink(target, link)
        .expect("create hardlink");
    let target_before = replica.posix_stat(target).expect("stat hardlink target");
    let link_before = replica
        .posix_namespace_lstat_distributed(link)
        .expect("lstat hardlink before metadata eviction");
    assert_eq!(link_before.kind, DfsObjectKind::Hardlink);
    let parent_id = replica
        .posix_stat(remote_dir)
        .expect("stat hardlink parent")
        .object_id;

    for object_id in [&parent_id, &target_before.object_id, &link_before.object_id] {
        replica.metadata.remove_object(object_id);
    }
    replica.metadata_backend = None;

    let stat = replica
        .posix_write_files_local_eventual_with_known_parents([(
            link,
            b"through authoritative hardlink".as_slice(),
        )])
        .expect("metadata-less write through authoritative hardlink")
        .pop()
        .expect("hardlink write stat");
    assert_eq!(stat.path, link);
    assert_eq!(stat.object_id, target_before.object_id);
    assert_eq!(
        replica
            .posix_read_file_distributed(target)
            .expect("read hardlink target"),
        b"through authoritative hardlink"
    );
    stop.store(true, Ordering::SeqCst);
    let services_primary = services_thread.join().expect("services primary thread");
    let link_after = services_primary
        .posix_namespace_lstat(link)
        .expect("authoritative hardlink after overwrite");
    assert_eq!(link_after.kind, DfsObjectKind::Hardlink);
    assert_eq!(link_after.object_id, link_before.object_id);
    assert_eq!(
        services_primary
            .posix_stat(target)
            .expect("authoritative hardlink target stat")
            .content_hash
            .as_deref(),
        Some(content_hash(b"through authoritative hardlink").as_str())
    );
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn sifs_cluster_posix_rename_across_shard_primaries_moves_object_transactionally() {
    let cluster = sifs_two_primary_cluster("sifs-xshard-rename");
    let SifsTwoPrimaryCluster {
        root,
        _runtime,
        users_primary_node,
        services_primary_node,
        replica_node,
        shared_kv,
        users_primary,
        services_primary,
        mut replica,
        rejections: _,
    } = cluster;
    let bytes = b"bytes that move between shard primaries".to_vec();
    let moved_bytes = b"rewritten at the new shard authority".to_vec();

    let stop = Arc::new(AtomicBool::new(false));
    let users_stop = Arc::clone(&stop);
    let users_thread = std::thread::spawn(move || {
        let mut users_primary = users_primary;
        while !users_stop.load(Ordering::SeqCst) {
            users_primary
                .process_incoming()
                .expect("users primary pump");
            std::thread::sleep(Duration::from_millis(1));
        }
        users_primary
    });
    let services_stop = Arc::clone(&stop);
    let services_thread =
        std::thread::spawn(move || pump_sifs_service_until_stop(services_primary, services_stop));

    // Create under /users (routed to the users primary).
    replica
        .posix_write_file(
            "/users/move-me.txt",
            &bytes,
            DfsWriteOptions::replicated_eventual(vec![replica_node.clone()]),
        )
        .expect("replica routed create");
    assert_eq!(
        replica.posix_read_file("/users/move-me.txt").expect("read"),
        bytes
    );

    // Rename across shard primaries; the source primary coordinates the
    // adoption on the services primary.
    let stat = replica
        .posix_rename("/users/move-me.txt", "/services/moved.txt")
        .expect("cross-primary rename");
    assert_eq!(
        stat.content_hash.as_deref(),
        Some(content_hash(&bytes).as_str())
    );
    assert_eq!(stat.path, "/services/moved.txt");

    // Read-your-write on the requester for both halves.
    assert!(matches!(
        replica.posix_stat("/users/move-me.txt"),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert_eq!(
        replica
            .posix_read_file("/services/moved.txt")
            .expect("read moved"),
        bytes
    );

    // The object's write authority moved with it: a write to the new path
    // routes to the services primary and commits there.
    replica
        .posix_write_file(
            "/services/moved.txt",
            &moved_bytes,
            DfsWriteOptions::replicated_eventual(vec![replica_node.clone()]),
        )
        .expect("write at new shard authority");
    assert_eq!(
        replica
            .posix_read_file("/services/moved.txt")
            .expect("read rewritten"),
        moved_bytes
    );

    stop.store(true, Ordering::SeqCst);
    let users_primary = users_thread.join().expect("users thread");
    let services_primary = services_thread.join().expect("services thread");
    assert!(matches!(
        users_primary.posix_stat("/users/move-me.txt"),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert!(users_primary.posix_stat("/services/moved.txt").is_ok());
    assert_eq!(
        services_primary
            .posix_stat("/services/moved.txt")
            .expect("services primary stat")
            .content_hash
            .as_deref(),
        Some(content_hash(&moved_bytes).as_str())
    );

    // Convergence: both primaries publish and every node agrees, with no
    // journal rejections anywhere.
    let mut cluster = SifsTwoPrimaryCluster {
        _runtime,
        root,
        users_primary_node,
        services_primary_node,
        replica_node,
        shared_kv,
        users_primary,
        services_primary,
        replica,
        rejections: Vec::new(),
    };
    let expected_hash = content_hash(&moved_bytes);
    sifs_two_primary_publish_until(&mut cluster, "post-rename convergence", |cluster| {
        let services_sees_users_half = matches!(
            cluster.services_primary.posix_stat("/users/move-me.txt"),
            Err(DfsPosixError::NotFound { .. })
        );
        let replica_current = cluster
            .replica
            .posix_stat("/services/moved.txt")
            .is_ok_and(|stat| stat.content_hash.as_deref() == Some(expected_hash.as_str()));
        services_sees_users_half && replica_current
    });
    assert!(
        cluster.rejections.is_empty(),
        "cross-primary rename must keep every replica journal aligned: {:?}",
        cluster.rejections
    );
    let users_entries = cluster
        .users_primary
        .posix_list_dir("/users")
        .expect("users list");
    assert!(
        !users_entries
            .iter()
            .any(|entry| entry.path == "/users/move-me.txt"),
        "old path must not be listed after the move: {users_entries:?}"
    );
    let services_entries = cluster
        .replica
        .posix_list_dir("/services")
        .expect("replica services list");
    assert!(
        services_entries
            .iter()
            .any(|entry| entry.path == "/services/moved.txt"),
        "new path must be listed on replicas: {services_entries:?}"
    );

    let SifsTwoPrimaryCluster {
        root,
        _runtime,
        users_primary_node,
        services_primary_node,
        replica_node,
        shared_kv,
        mut users_primary,
        services_primary,
        replica,
        rejections,
    } = cluster;
    let unlink_stop = Arc::new(AtomicBool::new(false));
    let services_stop = Arc::clone(&unlink_stop);
    let services_thread =
        std::thread::spawn(move || pump_sifs_service_until_stop(services_primary, services_stop));
    let unlinked = users_primary
        .posix_unlink("/services/moved.txt")
        .expect("users primary routes unlink to services primary after cross-shard move");
    assert_eq!(unlinked.kind, DfsObjectKind::Tombstone);
    unlink_stop.store(true, Ordering::SeqCst);
    let services_primary = services_thread.join().expect("services thread");
    let mut cluster = SifsTwoPrimaryCluster {
        _runtime,
        root,
        users_primary_node,
        services_primary_node,
        replica_node,
        shared_kv,
        users_primary,
        services_primary,
        replica,
        rejections,
    };
    sifs_two_primary_publish_until(&mut cluster, "post-move unlink convergence", |cluster| {
        matches!(
            cluster.replica.posix_stat("/services/moved.txt"),
            Err(DfsPosixError::NotFound { .. })
        ) && matches!(
            cluster.users_primary.posix_stat("/services/moved.txt"),
            Err(DfsPosixError::NotFound { .. })
        )
    });

    cluster.close();
}

#[test]
fn sifs_cluster_cross_shard_rename_replays_caller_owned_operation_id() {
    let cluster = sifs_two_primary_cluster("sifs-xshard-stable-rename-replay");
    let SifsTwoPrimaryCluster {
        shared_kv,
        services_primary_node,
        users_primary_node,
        root,
        _runtime,
        replica_node,
        users_primary,
        services_primary,
        mut replica,
        ..
    } = cluster;
    let source_path = "/users/stable-replay-source.txt";
    let target_path = "/services/stable-replay-target.txt";
    let operation_id = "dfs_test_cross_shard_stable_rename";

    let stop = Arc::new(AtomicBool::new(false));
    let users_stop = Arc::clone(&stop);
    let users_thread =
        std::thread::spawn(move || pump_sifs_service_until_stop(users_primary, users_stop));
    let services_stop = Arc::clone(&stop);
    let services_thread =
        std::thread::spawn(move || pump_sifs_service_until_stop(services_primary, services_stop));

    replica
        .posix_write_file(
            source_path,
            b"stable cross-shard rename replay",
            DfsWriteOptions::replicated_eventual(vec![replica_node]),
        )
        .expect("create stable-rename source");
    let first = replica
        .posix_rename_with_operation_id(source_path, target_path, operation_id)
        .expect("first stable cross-shard rename");
    let replay = replica
        .posix_rename_with_operation_id(source_path, target_path, operation_id)
        .expect("replay stable cross-shard rename");

    assert_eq!(replay.object_id, first.object_id);
    assert_eq!(replay.version, first.version);
    assert_eq!(
        replica
            .posix_read_file(target_path)
            .expect("target remains readable after exact replay"),
        b"stable cross-shard rename replay"
    );
    assert!(matches!(
        replica.posix_stat(source_path),
        Err(DfsPosixError::NotFound { .. })
    ));

    stop.store(true, Ordering::SeqCst);
    users_thread.join().expect("users service thread");
    services_thread.join().expect("services service thread");
    _runtime
        .close()
        .expect("normal embedded DFS fixture shutdown");
}

#[test]
fn sifs_cluster_posix_cross_shard_rename_preserves_durable_version_pin() {
    cross_shard_rename_preserves_pin_and_durability(false);
}

#[test]
fn file_history_cross_shard_metadata_generation_preserves_durability() {
    cross_shard_rename_preserves_pin_and_durability(true);
}

fn cross_shard_rename_preserves_pin_and_durability(with_durability: bool) {
    let cluster = if with_durability {
        sifs_two_primary_cluster("sifs-xshard-content-durability")
    } else {
        sifs_two_primary_cluster("sifs-xshard-pinned-rename")
    };
    let SifsTwoPrimaryCluster {
        shared_kv,
        services_primary_node,
        users_primary_node,
        root,
        _runtime,
        replica_node,
        mut users_primary,
        services_primary,
        mut replica,
        ..
    } = cluster;
    let source_path = "/users/pinned-move.txt";
    let target_path = "/services/pinned-moved.txt";
    let bytes = b"pinned metadata version crosses shard primaries".to_vec();

    let create_stop = Arc::new(AtomicBool::new(false));
    let users_stop = Arc::clone(&create_stop);
    let users_thread =
        std::thread::spawn(move || pump_sifs_service_until_stop(users_primary, users_stop));
    let services_stop = Arc::clone(&create_stop);
    let services_thread =
        std::thread::spawn(move || pump_sifs_service_until_stop(services_primary, services_stop));
    replica
        .posix_write_file(
            source_path,
            &bytes,
            DfsWriteOptions::replicated_eventual(vec![replica_node.clone()]),
        )
        .expect("create pinned source");
    create_stop.store(true, Ordering::SeqCst);
    users_primary = users_thread.join().expect("users create pump");
    let services_primary = services_thread.join().expect("services create pump");

    let source = users_primary
        .posix_stat(source_path)
        .expect("source metadata");
    let source_durability = users_primary
        .metadata_backend()
        .unwrap()
        .get_version(&source.object_id, &source.version)
        .unwrap()
        .unwrap()
        .content_durability;
    assert!(
        source_durability.is_some(),
        "current writes always retain their actual data-generation binding"
    );
    users_primary
        .retain_content_revision(DfsContentReferenceRequest {
            namespace: "module.dfs.history".into(),
            path: source.path.clone(),
            identity: DfsRevisionReference {
                file_system_id: source.file_system_id.clone(),
                object_id: source.object_id.clone(),
                version: source.version.clone(),
            },
            operation: DfsContentReferenceOperation::CaptureCurrent {
                reference_id: "checkpoint-cross-primary-rename".into(),
                metadata: None,
            },
            operation_id: sinomo_core::Id::generate().to_string(),
        })
        .expect("actual guarded source capture");
    let pin = users_primary
        .metadata_backend()
        .unwrap()
        .version_pins_for_object(&source.object_id)
        .unwrap()
        .into_iter()
        .find(|pin| {
            pin.pin_id
                == crate::content_reference_storage_id(
                    "module.dfs.history",
                    "checkpoint-cross-primary-rename",
                )
        })
        .expect("the source capture persisted its genuine pin");

    let rename_stop = Arc::new(AtomicBool::new(false));
    let users_stop = Arc::clone(&rename_stop);
    let users_thread =
        std::thread::spawn(move || pump_sifs_service_until_stop(users_primary, users_stop));
    let services_stop = Arc::clone(&rename_stop);
    let services_thread =
        std::thread::spawn(move || pump_sifs_service_until_stop(services_primary, services_stop));
    replica
        .posix_rename(source_path, target_path)
        .expect("pinned cross-primary rename");
    rename_stop.store(true, Ordering::SeqCst);
    let mut users_primary = users_thread.join().expect("users rename pump");
    let mut services_primary = services_thread.join().expect("services rename pump");

    let adopted_target = services_primary
        .posix_stat(target_path)
        .expect("adopted target metadata");
    assert_eq!(
        services_primary
            .metadata_backend()
            .unwrap()
            .get_version(&adopted_target.object_id, &adopted_target.version)
            .unwrap()
            .unwrap()
            .content_durability,
        source_durability,
        "metadata-only cross-shard adoption must carry the data durability plan"
    );
    assert!(
        services_primary
            .metadata_backend()
            .expect("services metadata backend")
            .version_pins_for_object(&source.object_id)
            .expect("target pins")
            .iter()
            .any(|installed| installed == &pin),
        "the destination authority must retain the exact durable source pin"
    );
    let retired_source = users_primary
        .metadata_backend
        .as_mut()
        .expect("users metadata backend")
        .capture_cross_shard_source_family(&source.object_id, source_path)
        .expect("retired physical source family");
    assert!(
        retired_source.version_pins.is_empty(),
        "source pins must be removed atomically with source eviction"
    );
    assert!(matches!(
        retired_source.source_object.tombstone_provenance,
        Some(DfsTombstoneProvenance::SourceEviction { .. })
    ));
    assert_eq!(
        retired_source.source_object.version, adopted_target.version,
        "source-eviction witness and installed target must name the durable job-frame generation"
    );
    assert!(
        services_primary
            .metadata_backend()
            .expect("services metadata backend")
            .cross_shard_pin_transfers_for_object(&source.object_id)
            .expect("target transfer provenance")
            .is_empty(),
        "quorum-durable finalization must retire temporary pin-transfer provenance"
    );

    if with_durability {
        let stop = Arc::new(AtomicBool::new(false));
        let users_stop = stop.clone();
        let users =
            std::thread::spawn(move || pump_sifs_service_until_stop(users_primary, users_stop));
        let replica_stop = stop.clone();
        let replica =
            std::thread::spawn(move || pump_sifs_service_until_stop(replica, replica_stop));
        let result = services_primary.retain_content_revision(DfsContentReferenceRequest {
            namespace: "org.example.cross-shard".into(),
            path: target_path.into(),
            identity: DfsFileVersionIdentity {
                file_system_id: source.file_system_id.clone(),
                object_id: source.object_id.clone(),
                version: adopted_target.version.clone(),
            },
            operation: DfsContentReferenceOperation::CaptureCurrent {
                reference_id: "after-cross-shard-move".into(),
                metadata: None,
            },
            operation_id: sinomo_core::Id::generate().to_string(),
        });
        stop.store(true, Ordering::SeqCst);
        users.join().unwrap();
        replica.join().unwrap();
        result.unwrap();
        let captured = services_primary
            .metadata_backend()
            .unwrap()
            .get_version(&adopted_target.object_id, &adopted_target.version)
            .unwrap()
            .unwrap();
        assert!(
            captured
                .retention_grant("org.example.cross-shard")
                .is_some()
        );
        assert_eq!(captured.content_durability, source_durability);
        assert_eq!(
            services_primary.posix_stat(target_path).unwrap().version,
            adopted_target.version
        );
    }

    _runtime
        .close()
        .expect("normal embedded DFS fixture shutdown");
}

#[test]
fn sifs_cluster_posix_rename_across_shard_primaries_rejects_invalid_destinations() {
    let cluster = sifs_two_primary_cluster("sifs-xshard-errno");
    let SifsTwoPrimaryCluster {
        shared_kv,
        services_primary_node,
        users_primary_node,
        root,
        _runtime,
        replica_node,
        users_primary,
        services_primary,
        mut replica,
        ..
    } = cluster;

    let stop = Arc::new(AtomicBool::new(false));
    let users_stop = Arc::clone(&stop);
    let users_thread = std::thread::spawn(move || {
        let mut users_primary = users_primary;
        while !users_stop.load(Ordering::SeqCst) {
            users_primary
                .process_incoming()
                .expect("users primary pump");
            std::thread::sleep(Duration::from_millis(1));
        }
        users_primary
    });
    let services_stop = Arc::clone(&stop);
    let services_thread =
        std::thread::spawn(move || pump_sifs_service_until_stop(services_primary, services_stop));

    let bytes = b"survives every failed rename".to_vec();
    replica
        .posix_write_file(
            "/users/keep-me.txt",
            &bytes,
            DfsWriteOptions::replicated_eventual(vec![replica_node.clone()]),
        )
        .expect("replica create");

    // Source missing: validated by the source primary.
    assert!(matches!(
        replica.posix_rename("/users/absent.txt", "/services/x.txt"),
        Err(DfsPosixError::NotFound { .. })
    ));
    // Directory sources move through the cross-shard subtree saga; renaming
    // a directory into its own subtree stays rejected.
    replica.posix_mkdir("/users/srcdir").expect("mkdir srcdir");
    let dir_stat = replica
        .posix_rename("/users/srcdir", "/services/srcdir")
        .expect("cross-primary directory rename");
    assert_eq!(dir_stat.kind, DfsObjectKind::Directory);
    assert!(matches!(
        replica.posix_rename("/services/srcdir", "/services/srcdir/inner"),
        Err(DfsPosixError::InvalidPath { .. })
    ));
    // Missing destination parent: validated by the destination primary and
    // round-tripped through two hops.
    assert!(matches!(
        replica.posix_rename("/users/keep-me.txt", "/services/no-parent/keep.txt"),
        Err(DfsPosixError::MissingParent { .. })
    ));
    // Directory destination: also a destination-primary validation.
    replica
        .posix_mkdir("/services/destdir")
        .expect("mkdir destdir");
    assert!(matches!(
        replica.posix_rename("/users/keep-me.txt", "/services/destdir"),
        Err(DfsPosixError::IsDirectory { .. })
    ));

    // Aborted sagas must release their locks and leave the source intact:
    // the source is still readable and writable.
    assert_eq!(
        replica.posix_read_file("/users/keep-me.txt").expect("read"),
        bytes
    );
    let updated = b"still writable after aborted moves".to_vec();
    replica
        .posix_write_file(
            "/users/keep-me.txt",
            &updated,
            DfsWriteOptions::replicated_eventual(vec![replica_node]),
        )
        .expect("source writable after aborts");
    assert_eq!(
        replica
            .posix_read_file("/users/keep-me.txt")
            .expect("read updated"),
        updated
    );

    stop.store(true, Ordering::SeqCst);
    let mut users_primary = users_thread.join().expect("users thread");
    services_thread.join().expect("services thread");

    // No saga may be left running on the coordinator.
    let report = users_primary
        .posix_resume_namespace_jobs_bounded(8, 512)
        .expect("maintenance resume");
    assert_eq!(
        report.remaining_running_job_count, 0,
        "aborted cross-shard renames must not leave running jobs: {report:?}"
    );

    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn sifs_cluster_posix_hardlink_across_shard_primaries_links_and_promotes() {
    let cluster = sifs_two_primary_cluster("sifs-xshard-hardlink");
    let SifsTwoPrimaryCluster {
        shared_kv,
        services_primary_node,
        users_primary_node,
        root,
        _runtime,
        replica_node,
        users_primary,
        services_primary,
        mut replica,
        ..
    } = cluster;
    let bytes = b"content shared across shard primaries".to_vec();

    let stop = Arc::new(AtomicBool::new(false));
    let users_stop = Arc::clone(&stop);
    let users_thread = std::thread::spawn(move || {
        let mut users_primary = users_primary;
        while !users_stop.load(Ordering::SeqCst) {
            users_primary
                .process_incoming()
                .expect("users primary pump");
            std::thread::sleep(Duration::from_millis(1));
        }
        users_primary
    });
    let services_stop = Arc::clone(&stop);
    let services_thread =
        std::thread::spawn(move || pump_sifs_service_until_stop(services_primary, services_stop));

    replica
        .posix_write_file(
            "/users/canonical.txt",
            &bytes,
            DfsWriteOptions::replicated_eventual(vec![replica_node.clone()]),
        )
        .expect("replica create canonical");

    // Hardlink across primaries: the target's primary coordinates the alias
    // adoption on the services primary.
    let link_stat = replica
        .posix_hardlink("/users/canonical.txt", "/services/alias")
        .expect("cross-primary hardlink");
    assert_eq!(link_stat.size_bytes, Some(bytes.len() as u64));
    assert_eq!(link_stat.link_count, 2);
    assert_eq!(
        replica
            .posix_read_file("/services/alias")
            .expect("read through alias"),
        bytes
    );
    assert_eq!(
        replica
            .posix_stat("/users/canonical.txt")
            .expect("canonical stat")
            .link_count,
        2
    );

    // The alias path is owned by the services primary while its canonical
    // inode is owned by the users primary. Alias unlink must coordinate the
    // canonical ctime transition on that authority before tombstoning the
    // alias generation locally.
    let canonical_before_alias_unlink = replica
        .posix_stat("/users/canonical.txt")
        .expect("canonical before alias unlink");
    let alias_unlinked = replica
        .posix_unlink("/services/alias")
        .expect("unlink alias through alias primary");
    assert_eq!(alias_unlinked.kind, DfsObjectKind::Tombstone);
    let canonical_after_alias_unlink = replica
        .posix_lstat_distributed("/users/canonical.txt")
        .expect("canonical after alias unlink");
    assert_eq!(canonical_after_alias_unlink.link_count, 1);
    assert_ne!(
        canonical_after_alias_unlink.version, canonical_before_alias_unlink.version,
        "alias-primary unlink must advance the canonical target generation",
    );
    assert!(
        canonical_after_alias_unlink.changed_at_unix_nanos
            > canonical_before_alias_unlink.changed_at_unix_nanos,
        "alias-primary unlink must advance canonical ctime",
    );

    let recreated_alias = replica
        .posix_hardlink("/users/canonical.txt", "/services/alias-for-rename")
        .expect("recreate cross-primary alias");
    let canonical_before_alias_rename = replica
        .posix_stat("/users/canonical.txt")
        .expect("canonical before alias rename");
    let renamed_alias = replica
        .posix_rename("/services/alias-for-rename", "/services/renamed-alias")
        .expect("rename alias through alias primary");
    assert_eq!(renamed_alias.object_id, recreated_alias.object_id);
    assert_eq!(renamed_alias.link_count, 2);
    let canonical_after_alias_rename = replica
        .posix_lstat_distributed("/users/canonical.txt")
        .expect("canonical after alias rename");
    assert_eq!(canonical_after_alias_rename.link_count, 2);
    assert_ne!(
        canonical_after_alias_rename.version, canonical_before_alias_rename.version,
        "alias-primary rename must advance the canonical target generation",
    );
    assert!(
        canonical_after_alias_rename.changed_at_unix_nanos
            > canonical_before_alias_rename.changed_at_unix_nanos,
        "alias-primary rename must advance canonical ctime",
    );

    // Unlinking the canonical promotes the foreign alias through the
    // cross-shard move saga.
    let unlinked = replica
        .posix_unlink("/users/canonical.txt")
        .expect("unlink canonical with foreign alias");
    assert_eq!(unlinked.kind, DfsObjectKind::Tombstone);
    assert!(matches!(
        replica.posix_stat("/users/canonical.txt"),
        Err(DfsPosixError::NotFound { .. })
    ));

    stop.store(true, Ordering::SeqCst);
    let users_primary = users_thread.join().expect("users thread");
    let services_primary = services_thread.join().expect("services thread");

    // Convergence: the alias becomes the canonical record everywhere with
    // the content intact and a link count of one.
    let mut cluster = SifsTwoPrimaryCluster {
        _runtime,
        root,
        users_primary_node,
        services_primary_node,
        replica_node,
        shared_kv,
        users_primary,
        services_primary,
        replica,
        rejections: Vec::new(),
    };
    let expected_hash = content_hash(&bytes);
    sifs_two_primary_publish_until(&mut cluster, "post-promotion convergence", |cluster| {
        let replica_alias = cluster
            .replica
            .posix_stat("/services/renamed-alias")
            .is_ok_and(|stat| {
                stat.content_hash.as_deref() == Some(expected_hash.as_str())
                    && stat.kind == DfsObjectKind::File
                    && stat.link_count == 1
            });
        let canonical_gone = matches!(
            cluster.replica.posix_stat("/users/canonical.txt"),
            Err(DfsPosixError::NotFound { .. })
        );
        replica_alias && canonical_gone
    });
    assert_eq!(
        cluster
            .replica
            .posix_read_file("/services/renamed-alias")
            .expect("read promoted alias"),
        bytes
    );
    assert!(
        cluster.rejections.is_empty(),
        "cross-primary hardlink and promotion must keep journals aligned: {:?}",
        cluster.rejections
    );

    cluster.close();
}

#[test]
fn cross_primary_hardlink_alias_unlink_fences_the_crash_gap_and_recovers_exactly_once() {
    let mut cluster = sifs_two_primary_cluster("native-hardlink-alias-crash-gap");
    let canonical_path = "/users/fenced-canonical";
    let alias_path = "/services/fenced-alias";
    cluster
        .replica
        .posix_write_file(
            canonical_path,
            b"fenced hardlink payload",
            DfsWriteOptions::replicated_eventual(vec![cluster.replica_node.clone()]),
        )
        .unwrap();
    cluster
        .replica
        .posix_hardlink(canonical_path, alias_path)
        .unwrap();
    let alias = cluster
        .replica
        .posix_namespace_lstat_distributed(alias_path)
        .unwrap();
    let expected = DfsNamespaceObjectPrecondition {
        object_id: alias.object_id,
        version: alias.version,
    };
    let before = cluster
        .users_primary
        .posix_lstat_distributed(canonical_path)
        .unwrap();
    let operation_id = sinomo_core::Id::generate().to_string();
    let operation = DfsNamespaceOperation::ConditionalUnlink {
        path: alias_path.into(),
        expected: expected.clone(),
    };
    let (observed_tx, observed_rx) = std::sync::mpsc::sync_channel(1);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
    let hook = crate::posix::install_hardlink_alias_after_commit_test_hook(alias_path, move || {
        observed_tx.send(()).unwrap();
        release_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("bounded actual after-commit interception");
        Err(DfsPosixError::Dfs(DfsError::Network {
            message: "actual alias committed; requester completion lost".into(),
        }))
    });
    std::thread::scope(|scope| {
        let requester = &mut cluster.replica;
        let pending_operation_id = operation_id.clone();
        let expected = expected.clone();
        let pending = scope.spawn(move || {
            requester.posix_unlink_if_generation(alias_path, pending_operation_id, expected)
        });
        let observed = observed_rx.recv_timeout(Duration::from_secs(5));
        // Always release the actual native request before propagating a failed assertion.
        let state = observed.as_ref().ok().map(|_| {
            let alias = cluster
                .services_primary
                .metadata_backend()
                .unwrap()
                .get_object_by_path(&sifs_file_system_id(), alias_path)
                .unwrap()
                .unwrap();
            let canonical = cluster
                .users_primary
                .metadata_backend()
                .unwrap()
                .get_object_by_path(&sifs_file_system_id(), canonical_path)
                .unwrap()
                .unwrap();
            let request = cluster
                .services_primary
                .metadata_backend()
                .unwrap()
                .get_namespace_request(&cluster.replica_node, &operation_id)
                .unwrap()
                .unwrap();
            (alias, canonical, request)
        });
        release_tx.send(()).ok();
        let outcome = pending.join().unwrap();
        observed.expect("real guarded alias commit reached");
        let (alias, canonical, request) = state.unwrap();
        assert_eq!(alias.kind, DfsObjectKind::Tombstone);
        assert_eq!(canonical.version, before.version);
        assert_eq!(request.status, DfsNamespaceRequestStatus::Pending);
        assert!(
            outcome.is_err(),
            "lost completion does not fabricate an outer terminal response"
        );
    });
    drop(hook);
    // Restart both actual authorities, retaining their volumes and native frames.
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
    let replay = cluster
        .replica
        .posix_unlink_if_generation(alias_path, operation_id.clone(), expected.clone())
        .unwrap();
    assert_eq!(replay.kind, DfsObjectKind::Tombstone);
    assert!(matches!(
        cluster
            .replica
            .reconcile_stable_namespace_operation(&operation_id, &operation)
            .unwrap(),
        DfsStableNamespaceOperationReconciliation::Applied { .. }
    ));
    let mut changed = expected;
    changed.version = DfsVersion::generate();
    assert!(matches!(
        cluster
            .replica
            .posix_unlink_if_generation(alias_path, operation_id, changed),
        Err(DfsPosixError::Conflict { .. })
    ));
    let canonical = cluster.users_primary.posix_stat(canonical_path).unwrap();
    assert_eq!(canonical.link_count, 1);
    assert_ne!(canonical.version, before.version);
    assert_eq!(
        cluster.replica.posix_read_file(canonical_path).unwrap(),
        b"fenced hardlink payload"
    );
    assert!(matches!(
        cluster.replica.posix_stat(alias_path),
        Err(DfsPosixError::NotFound { .. })
    ));
    cluster.close();
}

#[test]
fn sifs_cluster_posix_cross_shard_rename_recovers_after_destination_outage() {
    native_cross_shard_move_recovers_from_real_carrier_failure(
        "sifs_cluster_posix_cross_shard_rename_recovers_after_destination_outage",
        true,
        false,
    );
}

#[test]
fn sifs_cluster_posix_cross_shard_rename_retry_resumes_existing_job() {
    native_cross_shard_move_recovers_from_real_carrier_failure(
        "sifs_cluster_posix_cross_shard_rename_retry_resumes_existing_job",
        false,
        false,
    );
}

#[test]
fn sifs_cluster_posix_cross_shard_rename_pending_replay_reconciles_completed_job() {
    native_cross_shard_move_recovers_from_real_carrier_failure(
        "sifs_cluster_posix_cross_shard_rename_pending_replay_reconciles_completed_job",
        false,
        true,
    );
}

#[test]
fn sifs_cluster_posix_directory_rename_across_shard_primaries_moves_subtree() {
    let cluster = sifs_two_primary_cluster("sifs-xshard-dir-rename");
    let SifsTwoPrimaryCluster {
        shared_kv,
        services_primary_node,
        users_primary_node,
        root,
        _runtime,
        replica_node,
        users_primary,
        services_primary,
        mut replica,
        ..
    } = cluster;
    let bytes_a = b"alpha child crossing shard authorities".to_vec();
    let bytes_b = b"nested child content".to_vec();
    let bytes_a2 = b"rewritten under the new shard authority".to_vec();

    let stop = Arc::new(AtomicBool::new(false));
    let users_stop = Arc::clone(&stop);
    let users_thread = std::thread::spawn(move || {
        let mut users_primary = users_primary;
        while !users_stop.load(Ordering::SeqCst) {
            users_primary
                .process_incoming()
                .expect("users primary pump");
            std::thread::sleep(Duration::from_millis(1));
        }
        users_primary
    });
    let services_stop = Arc::clone(&stop);
    let services_thread =
        std::thread::spawn(move || pump_sifs_service_until_stop(services_primary, services_stop));

    // Build a small subtree under /users from the replica.
    replica.posix_mkdir("/users/proj").expect("mkdir proj");
    replica
        .posix_write_file(
            "/users/proj/a.txt",
            &bytes_a,
            DfsWriteOptions::replicated_eventual(vec![replica_node.clone()]),
        )
        .expect("create a.txt");
    replica.posix_mkdir("/users/proj/sub").expect("mkdir sub");
    replica
        .posix_write_file(
            "/users/proj/sub/b.txt",
            &bytes_b,
            DfsWriteOptions::replicated_eventual(vec![replica_node.clone()]),
        )
        .expect("create b.txt");

    // The whole subtree moves between shard primaries.
    let stat = replica
        .posix_rename("/users/proj", "/services/proj")
        .expect("cross-primary directory rename");
    assert_eq!(stat.kind, DfsObjectKind::Directory);

    // Read-your-write on the requester for the full subtree, both halves.
    assert!(matches!(
        replica.posix_stat("/users/proj"),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert!(matches!(
        replica.posix_stat("/users/proj/a.txt"),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert_eq!(
        replica
            .posix_read_file("/services/proj/a.txt")
            .expect("read a"),
        bytes_a
    );
    assert_eq!(
        replica
            .posix_read_file("/services/proj/sub/b.txt")
            .expect("read b"),
        bytes_b
    );
    let names: Vec<String> = replica
        .posix_list_dir("/services/proj")
        .expect("list moved dir")
        .into_iter()
        .map(|entry| entry.name)
        .collect();
    assert_eq!(names, vec!["a.txt".to_string(), "sub".to_string()]);

    // Write authority moved with every child: a write through the new path
    // commits at the services primary.
    replica
        .posix_write_file(
            "/services/proj/a.txt",
            &bytes_a2,
            DfsWriteOptions::replicated_eventual(vec![replica_node.clone()]),
        )
        .expect("write at new authority");
    assert_eq!(
        replica
            .posix_read_file("/services/proj/a.txt")
            .expect("read rewritten"),
        bytes_a2
    );

    stop.store(true, Ordering::SeqCst);
    let mut users_primary = users_thread.join().expect("users thread");
    let services_primary = services_thread.join().expect("services thread");

    // No saga may be left running on the coordinator.
    let report = users_primary
        .posix_resume_namespace_jobs_bounded(8, 512)
        .expect("maintenance resume");
    assert_eq!(
        (report.remaining_running_job_count, report.failed_job_count),
        (0, 0),
        "directory move must leave no running or failed jobs: {report:?}"
    );

    // Convergence: both primaries publish; every node agrees on the moved
    // subtree with no journal rejections.
    let mut cluster = SifsTwoPrimaryCluster {
        _runtime,
        root,
        users_primary_node,
        services_primary_node,
        replica_node,
        shared_kv,
        users_primary,
        services_primary,
        replica,
        rejections: Vec::new(),
    };
    let expected_b = content_hash(&bytes_b);
    sifs_two_primary_publish_until(&mut cluster, "post-dir-move convergence", |cluster| {
        let services_sees_old_gone = matches!(
            cluster.services_primary.posix_stat("/users/proj"),
            Err(DfsPosixError::NotFound { .. })
        );
        let users_sees_nested = cluster
            .users_primary
            .posix_stat("/services/proj/sub/b.txt")
            .is_ok_and(|stat| stat.content_hash.as_deref() == Some(expected_b.as_str()));
        services_sees_old_gone && users_sees_nested
    });
    assert!(
        cluster.rejections.is_empty(),
        "cross-primary directory rename must keep journals aligned: {:?}",
        cluster.rejections
    );
    let listing = |service: &NetworkedDfsService<DfsNodeTransport>| {
        service
            .posix_list_dir("/services/proj")
            .expect("list")
            .into_iter()
            .map(|entry| (entry.name, entry.kind, entry.object_id))
            .collect::<Vec<_>>()
    };
    let users_view = listing(&cluster.users_primary);
    assert_eq!(users_view, listing(&cluster.services_primary));
    assert_eq!(users_view, listing(&cluster.replica));

    cluster.close();
}

#[test]
fn sifs_cluster_posix_directory_rename_across_shard_primaries_recovers_with_subtree_lock() {
    let mut cluster = sifs_two_primary_cluster("native-subtree-carrier-recovery");
    let from = "/users/recovery-tree";
    let to = "/services/recovery-tree";
    cluster.users_primary.posix_mkdir_all(from).unwrap();
    let options = DfsWriteOptions::replicated_eventual(vec![cluster.users_primary_node.clone()]);
    cluster
        .users_primary
        .posix_write_file(&format!("{from}/child"), b"frozen child", options.clone())
        .unwrap();
    cluster
        ._runtime
        .install_fault(crate::test_runtime::FaultRule {
            source: Some(cluster.users_primary_node.clone()),
            target: Some(cluster.services_primary_node.clone()),
            route: Some(DFS_DATA_NAMESPACE_ROUTE.into()),
            call_id: None,
            origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
            remaining_hits: 32,
            action: crate::test_runtime::FaultAction::BudgetExhausted,
        });
    let operation = sinomo_core::Id::generate().to_string();
    assert!(
        cluster
            .users_primary
            .posix_rename_with_operation_id(from, to, &operation)
            .is_err()
    );
    assert!(matches!(
        cluster
            .users_primary
            .posix_write_file(&format!("{from}/child"), b"blocked", options),
        Err(DfsPosixError::Dfs(DfsError::Busy { .. }))
    ));
    assert!(matches!(
        cluster
            .users_primary
            .posix_mkdir(&format!("{from}/new-dir")),
        Err(DfsPosixError::Dfs(DfsError::Busy { .. }))
    ));
    cluster._runtime.clear_faults();
    cluster
        .users_primary
        .posix_rename_with_operation_id(from, to, &operation)
        .unwrap();
    assert_eq!(
        cluster
            .replica
            .posix_read_file(&format!("{to}/child"))
            .unwrap(),
        b"frozen child"
    );
    assert!(matches!(
        cluster.replica.posix_stat(from),
        Err(DfsPosixError::NotFound { .. })
    ));
    cluster
        .users_primary
        .posix_mkdir(from)
        .expect("completed real job releases the source subtree reservation");
    cluster._runtime.assert_fault_budgets().unwrap();
    cluster.close();
}

#[test]
fn sifs_cluster_posix_cross_shard_move_fences_open_write_intents() {
    let mut cluster = sifs_two_primary_cluster("native-cross-shard-open-write-fence");
    let from = "/users/write-fenced";
    let to = "/services/write-fenced";
    let original = cluster
        .users_primary
        .posix_write_file(
            from,
            b"original",
            DfsWriteOptions::replicated_eventual(vec![cluster.users_primary_node.clone()]),
        )
        .unwrap();
    cluster
        ._runtime
        .install_fault(crate::test_runtime::FaultRule {
            source: None,
            target: Some(cluster.services_primary_node.clone()),
            route: Some(DFS_DATA_STAGE_BLOB_ROUTE.into()),
            call_id: None,
            origin: None,
            remaining_hits: 32,
            action: crate::test_runtime::FaultAction::BudgetExhausted,
        });
    let target = DfsVersion::generate();
    let outcome = cluster
        .users_primary
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: original.file_system_id.clone(),
                object_id: original.object_id.clone(),
                target_version: target.clone(),
                requesting_node_id: cluster.users_primary_node.clone(),
            },
            b"pending actual W2 replacement".to_vec(),
            vec![
                cluster.users_primary_node.clone(),
                cluster.services_primary_node.clone(),
            ],
            DfsReplicationPolicy::new(2, 2).unwrap(),
        )
        .unwrap();
    assert!(matches!(outcome, NetworkedDfsWriteOutcome::Pending { .. }));
    assert!(matches!(
        cluster.users_primary.posix_rename(from, to),
        Err(DfsPosixError::Dfs(DfsError::Busy { .. }))
    ));
    assert!(
        !cluster
            .users_primary
            .metadata_backend()
            .unwrap()
            .list_namespace_jobs()
            .unwrap()
            .iter()
            .any(|job| job.root_path == from && job.status == DfsNamespaceJobStatus::Running)
    );
    cluster._runtime.clear_faults();
    let deadline = Instant::now() + DEFAULT_DFS_POSIX_SYNC_TIMEOUT;
    while cluster.users_primary.posix_stat(from).unwrap().version != target {
        cluster
            .users_primary
            .retry_pending_repairs_bounded(8)
            .unwrap();
        cluster.users_primary.process_incoming().unwrap();
        assert!(
            Instant::now() < deadline,
            "actual stage completion must settle within its normal deadline"
        );
        std::thread::sleep(SIFS_CLUSTER_PUMP_IDLE_SLEEP);
    }
    cluster.users_primary.posix_rename(from, to).unwrap();
    assert_eq!(
        cluster.replica.posix_read_file(to).unwrap(),
        b"pending actual W2 replacement"
    );
    cluster._runtime.assert_fault_budgets().unwrap();
    cluster.close();
}

#[test]
fn sifs_cluster_posix_cross_shard_move_allows_committed_pending_repair() {
    let mut cluster = sifs_two_primary_cluster("native-move-with-real-repair");
    let from = "/users/repair-pending";
    let to = "/services/repair-pending";
    cluster
        ._runtime
        .install_fault(crate::test_runtime::FaultRule {
            source: None,
            target: Some(cluster.services_primary_node.clone()),
            route: Some(DFS_DATA_REPAIR_REPLICA_ROUTE.into()),
            call_id: None,
            origin: None,
            remaining_hits: 32,
            action: crate::test_runtime::FaultAction::BudgetExhausted,
        });
    let source = cluster
        .users_primary
        .posix_write_file(
            from,
            b"committed repair-pending bytes",
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(1, 2).unwrap(),
                replica_candidates: vec![
                    cluster.users_primary_node.clone(),
                    cluster.services_primary_node.clone(),
                ],
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    assert!(
        cluster
            .users_primary
            .coordinator
            .repair_jobs()
            .iter()
            .any(|job| job.object_id == source.object_id)
    );
    let moved = cluster.users_primary.posix_rename(from, to).unwrap();
    assert_eq!(moved.object_id, source.object_id);
    assert_eq!(moved.content_hash, source.content_hash);
    cluster._runtime.clear_faults();
    assert_eq!(
        cluster.replica.posix_read_file(to).unwrap(),
        b"committed repair-pending bytes"
    );
    assert!(matches!(
        cluster.replica.posix_stat(from),
        Err(DfsPosixError::NotFound { .. })
    ));
    cluster._runtime.assert_fault_budgets().unwrap();
    cluster.close();
}

#[test]
fn sifs_cluster_posix_rejects_adoption_from_non_source_primary() {
    let mut cluster = sifs_two_primary_cluster("native-rogue-source-adoption");
    let from = "/users/rogue-source";
    let to = "/services/rogue-target";
    let source = cluster
        .users_primary
        .posix_write_file(
            from,
            b"actual source bytes",
            DfsWriteOptions::replicated_eventual(vec![cluster.users_primary_node.clone()]),
        )
        .unwrap();
    let pause = cluster
        ._runtime
        .pause_remote_namespace_phase(&cluster.users_primary_node)
        .unwrap();
    let mut requester = cluster._runtime.client(&cluster.replica_node).unwrap();
    let shard = DfsMetadataShardId::new("services");
    let before = cluster
        .services_primary
        .metadata_backend()
        .unwrap()
        .metadata_prefix_proof(&shard)
        .unwrap();
    std::thread::scope(|scope| {
        let original = scope.spawn(move || requester.posix_rename(from, to));
        let captured = pause.wait_captured(Duration::from_secs(5));
        let outcome = captured.as_ref().ok().map(|captured| {
            let mut tampered = captured.clone();
            tampered.routed_message.source_node_id = cluster.replica_node.clone();
            let DfsNetworkMessage::NamespaceRequest(command) = &mut tampered.routed_message.message
            else {
                panic!("actual namespace source phase");
            };
            assert!(matches!(
                command.operation,
                DfsNamespaceOperation::AdoptObjectAtPath { .. }
            ));
            command.requesting_node_id = cluster.replica_node.clone();
            let request = ObjectRpcRequest::encode_body(
                DFS_DATA_NAMESPACE_ROUTE,
                DFS_RPC_CATEGORY,
                &tampered,
            )
            .unwrap();
            cluster._runtime.rpc_request(
                &cluster.replica_node,
                &cluster.services_primary_node,
                request,
                Duration::from_secs(5),
            )
        });
        let unchanged = cluster
            .services_primary
            .metadata_backend()
            .unwrap()
            .metadata_prefix_proof(&shard)
            .unwrap();
        pause.release();
        let completed = original.join().unwrap();
        captured.expect("original native job/source offer captured");
        let response: DfsMessageResponse = outcome
            .unwrap()
            .expect("actual rogue-signer route reply")
            .decode_body()
            .unwrap();
        assert!(response.outbound_messages.iter().any(|message| matches!(&message.message,
            DfsNetworkMessage::NamespaceResponse(response) if matches!(&response.result,
                DfsNamespaceResult::Rejected { reason, .. } if reason.contains("primary of source shard")))));
        assert_eq!(
            unchanged, before,
            "rejected signer cannot consume the real source/job offer or publish a destination"
        );
        assert_eq!(completed.unwrap().object_id, source.object_id);
    });
    assert_eq!(
        cluster.replica.posix_read_file(to).unwrap(),
        b"actual source bytes"
    );
    assert_eq!(cluster._runtime.observation_dropped_count(), 0);
    cluster.close();
}

#[test]
fn native_namespace_job_resume_honors_step_budget() {
    let node = NodeId::generate();
    let (runtime, mut service) =
        native_sifs_service("native-namespace-job-step-budget", node.clone());
    let from = "/services/budget-source";
    let to = "/services/budget-target";
    service.posix_mkdir_all(from).unwrap();
    for index in 0..3 {
        service
            .posix_write_file(
                &format!("{from}/file-{index}"),
                format!("budget {index}").as_bytes(),
                DfsWriteOptions::replicated_eventual(vec![node.clone()]),
            )
            .unwrap();
    }
    let mut job = service.posix_rename_directory_bounded(from, to, 1).unwrap();
    assert_eq!(job.status, DfsNamespaceJobStatus::Running);
    let mut rounds = 1;
    while job.status == DfsNamespaceJobStatus::Running {
        let previous = job.processed_object_count;
        job = service
            .posix_resume_namespace_job_bounded(&job.job_id, 1)
            .unwrap();
        assert!(
            job.processed_object_count.saturating_sub(previous) <= 1,
            "one execution step may process at most one actual namespace object"
        );
        rounds += 1;
        assert!(rounds < 64, "the bounded current job must converge");
    }
    assert_eq!(job.status, DfsNamespaceJobStatus::Completed);
    assert!(rounds >= 3);
    for index in 0..3 {
        assert_eq!(
            service
                .posix_read_file(&format!("{to}/file-{index}"))
                .unwrap(),
            format!("budget {index}").as_bytes()
        );
    }
    assert!(matches!(
        service.posix_stat(from),
        Err(DfsPosixError::NotFound { .. })
    ));
    drop(service);
    runtime.close().unwrap();
}

#[test]
fn same_node_cross_shard_pin_family_survives_exact_replay_and_owner_restart() {
    let node = NodeId::generate();
    let (mut runtime, mut service) =
        native_sifs_service("native-same-node-pinned-move", node.clone());
    let source_path = "/users/pinned-source";
    let target_path = "/services/pinned-target";
    let source = service
        .posix_write_file(
            source_path,
            b"pinned source bytes",
            DfsWriteOptions::replicated_eventual(vec![node.clone()]),
        )
        .unwrap();
    let identity = DfsRevisionReference {
        file_system_id: source.file_system_id.clone(),
        object_id: source.object_id.clone(),
        version: source.version.clone(),
    };
    service
        .retain_content_revision(DfsContentReferenceRequest {
            namespace: "org.example.native-pin".into(),
            path: source_path.into(),
            identity: identity.clone(),
            operation: DfsContentReferenceOperation::CaptureCurrent {
                reference_id: "current-pin".into(),
                metadata: None,
            },
            operation_id: sinomo_core::Id::generate().to_string(),
        })
        .unwrap();
    let pins = service
        .metadata_backend()
        .unwrap()
        .version_pins_for_object(&source.object_id)
        .unwrap();
    assert_eq!(pins.len(), 1);
    let operation = sinomo_core::Id::generate().to_string();
    let moved = service
        .posix_rename_with_operation_id(source_path, target_path, &operation)
        .unwrap();
    assert_eq!(moved.object_id, source.object_id);
    assert_eq!(
        service
            .posix_rename_with_operation_id(source_path, target_path, &operation)
            .unwrap(),
        moved
    );
    assert_eq!(
        service
            .metadata_backend()
            .unwrap()
            .version_pins_for_object(&source.object_id)
            .unwrap(),
        pins
    );
    assert!(
        service
            .metadata_backend()
            .unwrap()
            .cross_shard_pin_transfers_for_object(&source.object_id)
            .unwrap()
            .is_empty()
    );
    drop(service);
    runtime.stop_node(&node).unwrap();
    runtime.reopen_node(node.clone()).unwrap();
    let mut service = runtime.client(&node).unwrap();
    assert_eq!(
        service.posix_read_file(target_path).unwrap(),
        b"pinned source bytes"
    );
    assert_eq!(
        service
            .metadata_backend()
            .unwrap()
            .version_pins_for_object(&source.object_id)
            .unwrap(),
        pins
    );
    assert_eq!(
        service
            .posix_rename_with_operation_id(source_path, target_path, &operation)
            .unwrap()
            .object_id,
        source.object_id
    );
    assert_eq!(
        service
            .read_file_version_content(
                &identity,
                source.content_hash.as_deref().unwrap(),
                source.size_bytes
            )
            .unwrap(),
        b"pinned source bytes"
    );
    drop(service);
    runtime.close().unwrap();
}

// Shared fault choreography uses existing native transports, jobs and owners.
fn native_cross_shard_move_recovers_from_real_carrier_failure(
    name: &str,
    restart_source: bool,
    explicit_resume: bool,
) {
    let mut cluster = sifs_two_primary_cluster(name);
    let from = "/users/native-carrier-source";
    let to = "/services/native-carrier-target";
    let original = cluster
        .users_primary
        .posix_write_file(
            from,
            b"real carrier fault data",
            DfsWriteOptions::replicated_eventual(vec![cluster.users_primary_node.clone()]),
        )
        .unwrap();
    let operation = sinomo_core::Id::generate().to_string();
    let baseline = cluster._runtime.rpc_observations().len();
    cluster
        ._runtime
        .install_fault(crate::test_runtime::FaultRule {
            source: Some(cluster.users_primary_node.clone()),
            target: Some(cluster.services_primary_node.clone()),
            route: Some(DFS_DATA_NAMESPACE_ROUTE.into()),
            call_id: None,
            origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
            remaining_hits: 32,
            action: crate::test_runtime::FaultAction::BudgetExhausted,
        });
    let started = Instant::now();
    assert!(
        cluster
            .users_primary
            .posix_rename_with_operation_id(from, to, &operation)
            .is_err()
    );
    assert!(
        started.elapsed() < DEFAULT_DFS_POSIX_SYNC_TIMEOUT,
        "failed real carrier is bounded by the normal operation deadline"
    );
    assert!(
        cluster
            ._runtime
            .rpc_observations()
            .iter()
            .skip(baseline)
            .any(|event| event.source == cluster.users_primary_node
                && event.target == cluster.services_primary_node
                && event.route == DFS_DATA_NAMESPACE_ROUTE
                && event.phase == crate::test_runtime::RpcPhase::EndedWithoutResponse)
    );
    let jobs = cluster
        .users_primary
        .metadata_backend()
        .unwrap()
        .list_namespace_jobs()
        .unwrap();
    let parked = jobs
        .into_iter()
        .find(|job| job.root_path == from && job.target_path.as_deref() == Some(to))
        .expect("actual durable coordinator after transport failure");
    assert_eq!(parked.status, DfsNamespaceJobStatus::Running);
    assert!(matches!(
        cluster.users_primary.posix_write_file(
            from,
            b"must not race parked job",
            DfsWriteOptions::replicated_eventual(vec![cluster.users_primary_node.clone()])
        ),
        Err(DfsPosixError::Dfs(DfsError::Busy { .. }))
    ));
    cluster._runtime.clear_faults();
    if restart_source {
        let node = cluster.users_primary_node.clone();
        drop(cluster.users_primary);
        cluster
            ._runtime
            .stop_node(&node)
            .expect("real source component shutdown");
        cluster
            ._runtime
            .reopen_node(node.clone())
            .expect("real source owner restart");
        cluster.users_primary = cluster._runtime.client(&node).unwrap();
    }
    if explicit_resume {
        let terminal = cluster
            .users_primary
            .posix_resume_namespace_job_bounded(&parked.job_id, 512)
            .unwrap();
        assert_eq!(terminal.status, DfsNamespaceJobStatus::Completed);
    }
    let completed = cluster
        .users_primary
        .posix_rename_with_operation_id(from, to, &operation)
        .unwrap();
    assert_eq!(completed.object_id, original.object_id);
    assert_eq!(
        cluster
            .users_primary
            .posix_rename_with_operation_id(from, to, &operation)
            .unwrap()
            .object_id,
        original.object_id
    );
    assert_eq!(
        cluster.replica.posix_read_file(to).unwrap(),
        b"real carrier fault data"
    );
    assert!(matches!(
        cluster.replica.posix_stat(from),
        Err(DfsPosixError::NotFound { .. })
    ));
    let terminal = cluster
        .users_primary
        .metadata_backend()
        .unwrap()
        .get_namespace_job(&parked.job_id)
        .unwrap();
    if let Some(terminal) = terminal {
        assert_eq!(terminal.status, DfsNamespaceJobStatus::Completed);
    }
    cluster._runtime.assert_fault_budgets().unwrap();
    assert_eq!(cluster._runtime.observation_dropped_count(), 0);
    cluster.close();
}
