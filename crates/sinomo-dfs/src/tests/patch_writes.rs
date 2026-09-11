//! Patch writes regression tests.

use super::*;

#[test]
fn routed_namespace_redirect_corroborates_only_the_dispatch_shard_successor() {
    let root = temp_root("routed-namespace-cross-shard-redirect-corroboration");
    let requester =
        NodeId::from_legacy_named("node_namespace_redirect_requester").expect("requester");
    let retired_dispatch =
        NodeId::from_legacy_named("node_namespace_redirect_retired").expect("retired dispatch");
    let current_dispatch =
        NodeId::from_legacy_named("node_namespace_redirect_current").expect("current dispatch");
    let foreign_primary =
        NodeId::from_legacy_named("node_namespace_redirect_foreign").expect("foreign primary");

    let mut dispatch_authority = test_routed_authority_shard(&retired_dispatch, 3);
    dispatch_authority.shard_id = "users".to_string();
    dispatch_authority.assignment = DfsMetadataShardAssignment::PathPrefix {
        prefix: SIFS_USERS_PATH.to_string(),
    };
    let mut current_dispatch_authority = dispatch_authority.clone();
    current_dispatch_authority.primary_node_id = current_dispatch.clone();
    current_dispatch_authority.epoch = 4;
    let foreign_authority = test_routed_authority_shard(&foreign_primary, 9);

    let control_plane = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    control_plane.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .put_metadata_shard(&current_dispatch_authority)
            .expect("current dispatch authority");
        control
            .put_metadata_shard(&foreign_authority)
            .expect("unchanged foreign authority");
    });
    let mut dfs = NetworkedDfsService::new(
        DroppingSendTransport {
            local_node_id: requester,
        },
        LocalContentStore::new(root.join("content")).expect("content"),
    )
    .with_control_plane(control_plane);
    dfs.pending_routed_namespace_ops.insert(
        "dfs_net_req_cross_shard_redirect".to_string(),
        PendingRoutedNamespaceOp {
            primary_node_id: retired_dispatch.clone(),
            dispatch_authority_shard: Some(dispatch_authority.clone()),
            authority_shards: vec![dispatch_authority, foreign_authority],
            replay_journal_batches: false,
            apply_synced_path_object: false,
            result: None,
        },
    );

    let event = dfs
        .handle_namespace_response(
            retired_dispatch,
            DfsNamespaceResponse {
                request_id: "dfs_net_req_cross_shard_redirect".to_string(),
                result: DfsNamespaceResult::Rejected {
                    reason: "dispatch authority moved".to_string(),
                    posix_error: None,
                    current_primary_node_id: Some(current_dispatch.clone()),
                },
                synced_path_object: None,
                synced_path_objects: Vec::new(),
                metadata_journal_batches: Vec::new(),
                metadata_journal_has_more: None,
                posix_coordination_result: None,
            },
        )
        .expect("corroborated redirect response");
    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::RemoteNamespaceCompleted {
            result: DfsNamespaceResult::Rejected {
                current_primary_node_id: Some(ref primary),
                ..
            },
            ..
        } if primary == &current_dispatch
    ));
    assert!(matches!(
        dfs.pending_routed_namespace_ops
            .get("dfs_net_req_cross_shard_redirect")
            .and_then(|pending| pending.result.as_ref())
            .map(|settled| &settled.result),
        Some(DfsNamespaceResult::Rejected {
            current_primary_node_id: Some(primary),
            ..
        }) if primary == &current_dispatch
    ));

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_write_at_self_only_uses_extent_patch_manifest() {
    let root = temp_root("sifs-posix-write-at-extent-patch");
    let node_id = NodeId::from_legacy_named("node_sifs_write_at_extent").expect("node");
    let work = "/services/sandbox/instances/sandbox_write_at_extent/work";
    let file = "/services/sandbox/instances/sandbox_write_at_extent/work/file.bin";

    let mut initial = vec![0_u8; 256 * 1024 + 19];
    for (index, byte) in initial.iter_mut().enumerate() {
        *byte = (index % 251) as u8;
    }
    let initial_hash = content_hash(&initial);
    let patch = b"small-random-write";
    let patch_offset = 128 * 1024 + 11;
    let mut expected = initial.clone();
    expected[patch_offset..patch_offset + patch.len()].copy_from_slice(patch);
    let expected_hash = content_hash(&expected);

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all(work).expect("mkdir");
    dfs.posix_write_file(
        file,
        &initial,
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write initial");

    let stat = dfs
        .posix_write_file_at(
            file,
            patch_offset as u64,
            patch,
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("write extent patch");

    assert_eq!(stat.size_bytes, Some(expected.len() as u64));
    assert_eq!(stat.content_hash.as_deref(), Some(expected_hash.as_str()));
    let manifest = dfs
        .content()
        .read_manifest(stat.object_id.as_str(), stat.version.as_str())
        .expect("patched manifest");
    assert_eq!(manifest.content_hash, expected_hash);
    assert!(
        !manifest.extents.is_empty(),
        "local self-only write_at should preserve unchanged bytes as extents"
    );
    assert!(
        manifest
            .extents
            .iter()
            .any(|extent| extent.content_hash == initial_hash)
    );
    assert!(
        !dfs.content()
            .has_blob(&manifest.content_hash)
            .expect("logical blob check"),
        "patched write_at should not materialize a full replacement blob"
    );
    assert_eq!(dfs.posix_read_file(file).expect("read patched"), expected);
    assert_eq!(
        dfs.posix_read_file_range(file, patch_offset as u64 - 8, patch.len() + 16)
            .expect("read patched range"),
        expected[patch_offset - 8..patch_offset + patch.len() + 8]
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_write_at_from_reader_uses_extent_patch_manifest() {
    let root = temp_root("sifs-posix-write-at-reader-extent-patch");
    let node_id = NodeId::from_legacy_named("node_sifs_write_at_reader_extent").expect("node");
    let work = "/services/sandbox/instances/sandbox_write_at_reader_extent/work";
    let file = "/services/sandbox/instances/sandbox_write_at_reader_extent/work/file.bin";

    let mut initial = vec![0_u8; 320 * 1024 + 29];
    for (index, byte) in initial.iter_mut().enumerate() {
        *byte = (index % 251) as u8;
    }
    let initial_hash = content_hash(&initial);
    let patch = (0..80 * 1024 + 23)
        .map(|index| (index % 241) as u8)
        .collect::<Vec<_>>();
    let patch_hash = content_hash(&patch);
    let patch_offset = 160 * 1024 + 7;
    let mut expected = initial.clone();
    expected[patch_offset..patch_offset + patch.len()].copy_from_slice(&patch);
    let expected_hash = content_hash(&expected);

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all(work).expect("mkdir");
    dfs.posix_write_file(
        file,
        &initial,
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write initial");

    let stat = dfs
        .posix_write_file_at_from_reader(
            file,
            patch_offset as u64,
            std::io::Cursor::new(patch.clone()),
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("streaming write_at extent patch");

    assert_eq!(stat.size_bytes, Some(expected.len() as u64));
    assert_eq!(stat.content_hash.as_deref(), Some(expected_hash.as_str()));
    let manifest = dfs
        .content()
        .read_manifest(stat.object_id.as_str(), stat.version.as_str())
        .expect("patched manifest");
    assert_eq!(manifest.content_hash, expected_hash);
    assert!(
        manifest
            .extents
            .iter()
            .any(|extent| extent.content_hash == initial_hash)
    );
    assert!(
        manifest
            .extents
            .iter()
            .any(|extent| extent.content_hash == patch_hash)
    );
    assert!(
        !dfs.content()
            .has_blob(&manifest.content_hash)
            .expect("logical blob check"),
        "streamed write_at should not materialize a full replacement blob"
    );
    assert_eq!(
        dfs.posix_read_file_range(file, patch_offset as u64 - 8, patch.len() + 16)
            .expect("read patched range"),
        expected[patch_offset - 8..patch_offset + patch.len() + 8]
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_write_at_from_reader_streams_sparse_extension_as_extent_patch() {
    let root = temp_root("sifs-posix-write-at-reader-sparse-extent-patch");
    let node_id = NodeId::from_legacy_named("node_sifs_write_at_reader_sparse").expect("node");
    let work = "/services/sandbox/instances/sandbox_write_at_reader_sparse/work";
    let file = "/services/sandbox/instances/sandbox_write_at_reader_sparse/work/file.bin";

    let initial = (0..192 * 1024 + 29)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let initial_hash = content_hash(&initial);
    let patch = (0..96 * 1024 + 23)
        .map(|index| (index % 241) as u8)
        .collect::<Vec<_>>();
    let gap_size = 80 * 1024 + 11;
    let patch_offset = initial.len() + gap_size;
    let mut sparse_patch = vec![0_u8; gap_size];
    sparse_patch.extend_from_slice(&patch);
    let sparse_patch_hash = content_hash(&sparse_patch);
    let mut expected = initial.clone();
    expected.extend_from_slice(&sparse_patch);
    let expected_hash = content_hash(&expected);

    struct StreamingReader {
        bytes: Vec<u8>,
        offset: usize,
        bulk_reads: std::rc::Rc<std::cell::Cell<usize>>,
    }

    impl std::io::Read for StreamingReader {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            if self.offset == 0 {
                assert_eq!(buffer.len(), 1, "POSIX write_at should only peek one byte");
            } else {
                assert_eq!(
                    buffer.len(),
                    64 * 1024,
                    "content ingest should retain its bounded streaming buffer"
                );
                self.bulk_reads.set(self.bulk_reads.get() + 1);
            }
            if self.offset >= self.bytes.len() {
                return Ok(0);
            }
            let len = buffer.len().min(self.bytes.len() - self.offset);
            buffer[..len].copy_from_slice(&self.bytes[self.offset..self.offset + len]);
            self.offset += len;
            Ok(len)
        }
    }

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all(work).expect("mkdir");
    dfs.posix_write_file(
        file,
        &initial,
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write initial");

    let bulk_reads = std::rc::Rc::new(std::cell::Cell::new(0));
    let stat = dfs
        .posix_write_file_at_from_reader(
            file,
            patch_offset as u64,
            StreamingReader {
                bytes: patch,
                offset: 0,
                bulk_reads: bulk_reads.clone(),
            },
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("stream sparse write_at");

    assert!(bulk_reads.get() > 1, "patch input should be streamed");
    assert_eq!(stat.size_bytes, Some(expected.len() as u64));
    assert_eq!(stat.content_hash.as_deref(), Some(expected_hash.as_str()));
    let manifest = dfs
        .content()
        .read_manifest(stat.object_id.as_str(), stat.version.as_str())
        .expect("sparse patch manifest");
    assert!(
        manifest
            .extents
            .iter()
            .any(|extent| extent.content_hash == initial_hash),
        "the unchanged base should remain an extent"
    );
    assert!(
        manifest
            .extents
            .iter()
            .any(|extent| extent.content_hash == sparse_patch_hash),
        "the generated zero gap and caller stream should share one patch extent"
    );
    assert!(
        !dfs.content()
            .has_blob(&manifest.content_hash)
            .expect("logical blob check"),
        "sparse write_at should not materialize the full logical object"
    );
    assert_eq!(
        dfs.posix_read_file(file).expect("read sparse file"),
        expected
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_write_at_replicates_extent_patch_without_full_blob() {
    let root = temp_root("sifs-posix-write-at-replicated-extent-patch");
    let owner = NodeId::from_legacy_named("node_sifs_write_at_patch_owner").expect("owner");
    let replica = NodeId::from_legacy_named("node_sifs_write_at_patch_replica").expect("replica");
    let object_id =
        DfsObjectId::from_legacy_named("obj_sifs_write_at_patch_replicated").expect("object");
    let base_version =
        DfsVersion::from_legacy_named("version_sifs_write_at_patch_base").expect("base");
    let path = "/services/replicated-write-at-patch.bin";
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), root.join("owner-meta"))
            .expect("owner metadata"),
    )
    .expect("owner backend");
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        LocalContentStore::new(root.join("replica-content")).expect("replica content"),
    )
    .with_metadata_backend(
        LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new("replica"),
            root.join("replica-meta"),
        )
        .expect("replica metadata"),
    )
    .expect("replica backend");
    owner_dfs.bootstrap_distributed_sifs().expect("owner sifs");
    replica_dfs
        .bootstrap_distributed_sifs()
        .expect("replica sifs");

    let mut initial = vec![0_u8; 384 * 1024 + 37];
    for (index, byte) in initial.iter_mut().enumerate() {
        *byte = (index % 239) as u8;
    }
    let owner_base = owner_dfs
        .content()
        .put_object_version(object_id.as_str(), base_version.as_str(), &initial)
        .expect("owner base content");
    let replica_base = replica_dfs
        .content()
        .put_object_version(object_id.as_str(), base_version.as_str(), &initial)
        .expect("replica base content");
    assert_eq!(owner_base.content_hash, replica_base.content_hash);
    let mut object = file_metadata(owner.clone());
    object.object_id = object_id.clone();
    object.path = path.to_string();
    object.version = base_version.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![owner.clone(), replica.clone()];
    object.content_hash = Some(owner_base.content_hash.clone());
    object.content_size_bytes = Some(owner_base.size_bytes);
    owner_dfs
        .add_object_metadata(object.clone())
        .expect("owner object metadata");
    replica_dfs
        .add_object_metadata(object)
        .expect("replica object metadata");

    let patch = b"remote extent patch";
    let patch_offset = 192 * 1024 + 13;
    let mut expected = initial.clone();
    expected[patch_offset..patch_offset + patch.len()].copy_from_slice(patch);
    let expected_hash = content_hash(&expected);
    let pending_stat = owner_dfs
        .posix_write_file_at(
            path,
            patch_offset as u64,
            patch,
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(2, 2).expect("strict policy"),
                replica_candidates: vec![owner.clone(), replica.clone()],
                wait_for_commit: false,
                foreground_replica_repair: true,
            },
        )
        .expect("owner patch write");
    assert_eq!(pending_stat.size_bytes, Some(expected.len() as u64));

    let mut committed = false;
    for _ in 0..16 {
        replica_dfs.process_incoming().expect("replica incoming");
        owner_dfs.process_incoming().expect("owner incoming");
        replica_dfs
            .process_incoming()
            .expect("replica commit incoming");
        let owner_object = owner_dfs
            .current_read_object_metadata(&object_id)
            .expect("owner object");
        let replica_object = replica_dfs
            .current_read_object_metadata(&object_id)
            .expect("replica object");
        if owner_object.content_hash.as_deref() == Some(expected_hash.as_str())
            && replica_object.content_hash.as_deref() == Some(expected_hash.as_str())
        {
            committed = true;
            break;
        }
    }
    assert!(committed, "replicated patch write did not commit");

    let committed_owner = owner_dfs
        .current_read_object_metadata(&object_id)
        .expect("committed owner metadata");
    assert_eq!(
        committed_owner.content_hash.as_deref(),
        Some(expected_hash.as_str())
    );
    assert_eq!(
        owner_dfs.posix_read_file(path).expect("owner read"),
        expected
    );
    assert_eq!(
        replica_dfs
            .current_read_object_metadata(&object_id)
            .expect("replica metadata")
            .current_locations,
        vec![owner.clone(), replica.clone()]
    );
    let replica_manifest = replica_dfs
        .content()
        .read_manifest(object_id.as_str(), committed_owner.version.as_str())
        .expect("replica patched manifest");
    assert_eq!(replica_manifest.content_hash, expected_hash);
    assert!(
        !replica_manifest.extents.is_empty(),
        "remote patch stage should preserve unchanged bytes as extents"
    );
    assert!(
        replica_manifest
            .extents
            .iter()
            .any(|extent| extent.content_hash == owner_base.content_hash)
    );
    assert!(
        replica_manifest
            .extents
            .iter()
            .any(|extent| extent.content_hash == content_hash(patch))
    );
    assert!(
        !replica_dfs
            .content()
            .has_blob(&replica_manifest.content_hash)
            .expect("logical blob check"),
        "replicated patch write should not materialize a full replacement blob on the replica"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_write_at_replicates_large_extent_patch_in_chunks_without_full_blob() {
    let root = temp_root("sifs-posix-write-at-large-patch-chunks");
    let owner = NodeId::from_legacy_named("node_sifs_write_at_large_patch_owner").expect("owner");
    let replica =
        NodeId::from_legacy_named("node_sifs_write_at_large_patch_replica").expect("replica");
    let object_id =
        DfsObjectId::from_legacy_named("obj_sifs_write_at_large_patch").expect("object");
    let base_version =
        DfsVersion::from_legacy_named("version_sifs_write_at_large_patch_base").expect("base");
    let path = "/services/replicated-write-at-large-patch.bin";
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), root.join("owner-meta"))
            .expect("owner metadata"),
    )
    .expect("owner backend");
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        LocalContentStore::new(root.join("replica-content")).expect("replica content"),
    )
    .with_metadata_backend(
        LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new("replica"),
            root.join("replica-meta"),
        )
        .expect("replica metadata"),
    )
    .expect("replica backend");
    owner_dfs.bootstrap_distributed_sifs().expect("owner sifs");
    replica_dfs
        .bootstrap_distributed_sifs()
        .expect("replica sifs");

    let mut initial = vec![0_u8; 768 * 1024 + 19];
    for (index, byte) in initial.iter_mut().enumerate() {
        *byte = (index % 241) as u8;
    }
    let owner_base = owner_dfs
        .content()
        .put_object_version(object_id.as_str(), base_version.as_str(), &initial)
        .expect("owner base content");
    let replica_base = replica_dfs
        .content()
        .put_object_version(object_id.as_str(), base_version.as_str(), &initial)
        .expect("replica base content");
    assert_eq!(owner_base.content_hash, replica_base.content_hash);
    let mut object = file_metadata(owner.clone());
    object.object_id = object_id.clone();
    object.path = path.to_string();
    object.version = base_version.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![owner.clone(), replica.clone()];
    object.content_hash = Some(owner_base.content_hash.clone());
    object.content_size_bytes = Some(owner_base.size_bytes);
    owner_dfs
        .add_object_metadata(object.clone())
        .expect("owner object metadata");
    replica_dfs
        .add_object_metadata(object)
        .expect("replica object metadata");

    let mut patch = vec![0_u8; 160 * 1024 + 17];
    for (index, byte) in patch.iter_mut().enumerate() {
        *byte = 255_u8.wrapping_sub((index % 251) as u8);
    }
    let patch_hash = content_hash(&patch);
    let patch_offset = 257 * 1024 + 3;
    let mut expected = initial.clone();
    expected[patch_offset..patch_offset + patch.len()].copy_from_slice(&patch);
    let expected_hash = content_hash(&expected);
    owner_dfs
        .posix_write_file_at(
            path,
            patch_offset as u64,
            &patch,
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(2, 2).expect("strict policy"),
                replica_candidates: vec![owner.clone(), replica.clone()],
                wait_for_commit: false,
                foreground_replica_repair: true,
            },
        )
        .expect("owner large patch write");

    let mut committed = false;
    for _ in 0..16 {
        replica_dfs.process_incoming().expect("replica incoming");
        owner_dfs.process_incoming().expect("owner incoming");
        replica_dfs
            .process_incoming()
            .expect("replica commit incoming");
        let owner_object = owner_dfs
            .current_read_object_metadata(&object_id)
            .expect("owner object");
        let replica_object = replica_dfs
            .current_read_object_metadata(&object_id)
            .expect("replica object");
        if owner_object.content_hash.as_deref() == Some(expected_hash.as_str())
            && replica_object.content_hash.as_deref() == Some(expected_hash.as_str())
        {
            committed = true;
            break;
        }
    }
    assert!(committed, "chunked replicated patch write did not commit");

    let committed_owner = owner_dfs
        .current_read_object_metadata(&object_id)
        .expect("committed owner metadata");
    assert_eq!(
        owner_dfs
            .posix_read_file_range(path, patch_offset as u64, patch.len())
            .expect("owner patched range"),
        patch
    );
    let replica_manifest = replica_dfs
        .content()
        .read_manifest(object_id.as_str(), committed_owner.version.as_str())
        .expect("replica patched manifest");
    assert_eq!(replica_manifest.content_hash, expected_hash);
    assert!(
        replica_manifest
            .extents
            .iter()
            .any(|extent| extent.content_hash == owner_base.content_hash)
    );
    assert!(
        replica_manifest
            .extents
            .iter()
            .any(|extent| extent.content_hash == patch_hash)
    );
    assert!(
        !replica_dfs
            .content()
            .has_blob(&replica_manifest.content_hash)
            .expect("logical blob check"),
        "chunked patch stage should not materialize a full replacement blob"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_write_at_retries_lost_patch_stage_as_patch() {
    let root = temp_root("sifs-posix-write-at-patch-retry");
    let owner = NodeId::from_legacy_named("node_sifs_write_at_patch_retry_owner").expect("owner");
    let replica =
        NodeId::from_legacy_named("node_sifs_write_at_patch_retry_replica").expect("replica");
    let path = "/services/retry-write-at-patch.bin";
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), root.join("owner-meta"))
            .expect("owner metadata"),
    )
    .expect("owner backend");
    let mut replica_mailbox = network.add_node(replica.clone());
    owner_dfs.bootstrap_distributed_sifs().expect("owner sifs");

    let mut initial = vec![0_u8; 256 * 1024 + 31];
    for (index, byte) in initial.iter_mut().enumerate() {
        *byte = (index % 251) as u8;
    }
    owner_dfs
        .posix_write_file(
            path,
            &initial,
            DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .expect("initial self write");

    let patch = b"lost patch stage should retry as patch";
    let patch_offset = 96 * 1024 + 5;
    owner_dfs
        .posix_write_file_at(
            path,
            patch_offset as u64,
            patch,
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(2, 2).expect("strict policy"),
                replica_candidates: vec![owner.clone(), replica.clone()],
                wait_for_commit: false,
                foreground_replica_repair: true,
            },
        )
        .expect("patch write");

    let first_messages = drain_dfs_messages(&mut replica_mailbox);
    assert!(
        first_messages
            .iter()
            .any(|message| matches!(message, DfsNetworkMessage::StagePatch(_))),
        "first stage should send a patch"
    );
    assert!(
        first_messages
            .iter()
            .all(|message| !matches!(message, DfsNetworkMessage::StageBlob(_))),
        "patch stage should not send a full-object body"
    );

    owner_dfs.repair_retry_tick = 16;
    owner_dfs
        .retry_pending_repairs()
        .expect("retry pending patch stage");

    let retry_messages = drain_dfs_messages(&mut replica_mailbox);
    assert!(
        retry_messages
            .iter()
            .any(|message| matches!(message, DfsNetworkMessage::StagePatch(_))),
        "lost patch stage should be retried as a patch"
    );
    assert!(
        retry_messages
            .iter()
            .all(|message| !matches!(message, DfsNetworkMessage::StageBlob(_))),
        "patch retry should not fall back to a full-object body before an explicit patch rejection"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_write_at_retries_lost_patch_attachment() {
    let root = temp_root("sifs-posix-write-at-patch-attachment-retry");
    let owner = NodeId::from_legacy_named("node_sifs_write_at_patch_retry_owner").expect("owner");
    let replica =
        NodeId::from_legacy_named("node_sifs_write_at_patch_retry_replica").expect("replica");
    let path = "/services/retry-write-at-patch.bin";
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), root.join("owner-meta"))
            .expect("owner metadata"),
    )
    .expect("owner backend");
    let mut replica_mailbox = network.add_node(replica.clone());
    owner_dfs.bootstrap_distributed_sifs().expect("owner sifs");

    let mut initial = vec![0_u8; 512 * 1024 + 11];
    for (index, byte) in initial.iter_mut().enumerate() {
        *byte = (index % 251) as u8;
    }
    owner_dfs
        .posix_write_file(
            path,
            &initial,
            DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .expect("initial self write");

    let mut patch = vec![0_u8; 4 * 1024 * 1024 + 160 * 1024 + 5];
    for (index, byte) in patch.iter_mut().enumerate() {
        *byte = (index % 239) as u8;
    }
    let patch_hash = content_hash(&patch);
    struct StreamingPatchReader {
        bytes: Vec<u8>,
        offset: usize,
        bulk_reads: std::rc::Rc<std::cell::Cell<usize>>,
    }
    impl std::io::Read for StreamingPatchReader {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            if self.offset == 0 {
                if buffer.len() != 1 {
                    return Err(std::io::Error::other(format!(
                        "expected initial POSIX peek buffer, got {}",
                        buffer.len()
                    )));
                }
            } else if buffer.len() != 64 * 1024 {
                return Err(std::io::Error::other(format!(
                    "expected content-store streaming buffer, got {}",
                    buffer.len()
                )));
            } else {
                self.bulk_reads.set(self.bulk_reads.get() + 1);
            }
            if self.offset >= self.bytes.len() {
                return Ok(0);
            }
            let len = buffer.len().min(self.bytes.len() - self.offset);
            buffer[..len].copy_from_slice(&self.bytes[self.offset..self.offset + len]);
            self.offset += len;
            Ok(len)
        }
    }
    let bulk_reads = std::rc::Rc::new(std::cell::Cell::new(0));
    owner_dfs
        .posix_write_file_at_from_reader(
            path,
            96 * 1024 + 5,
            StreamingPatchReader {
                bytes: patch.clone(),
                offset: 0,
                bulk_reads: bulk_reads.clone(),
            },
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(2, 2).expect("strict policy"),
                replica_candidates: vec![owner.clone(), replica.clone()],
                wait_for_commit: false,
                foreground_replica_repair: true,
            },
        )
        .expect("patch write");
    assert!(
        bulk_reads.get() > 1,
        "replicated write_at reader should stream the patch through the content store"
    );
    let pending = owner_dfs
        .writes
        .pending_writes
        .values()
        .next()
        .expect("pending patch write");
    let pending_patch = pending.patch.as_ref().expect("pending patch metadata");
    assert_eq!(pending_patch.patch_hash, patch_hash);
    assert_eq!(pending_patch.patch_size_bytes, patch.len() as u64);
    assert!(
        owner_dfs
            .content()
            .has_blob(&pending_patch.patch_hash)
            .expect("pending patch blob exists"),
        "pending patch retry should stream from the content-addressed patch blob"
    );

    let first_messages = drain_dfs_messages(&mut replica_mailbox);
    assert!(
        first_messages
            .iter()
            .any(|message| matches!(message, DfsNetworkMessage::StagePatch(_))),
        "first patch stage should send a patch attachment"
    );
    assert!(
        first_messages
            .iter()
            .all(|message| !matches!(message, DfsNetworkMessage::StageBlob(_))),
        "patch stage should not send a full-object attachment"
    );

    owner_dfs.repair_retry_tick = 16;
    owner_dfs
        .retry_pending_repairs()
        .expect("retry pending patch stage");

    let retry_messages = drain_dfs_messages(&mut replica_mailbox);
    assert!(
        retry_messages
            .iter()
            .any(|message| matches!(message, DfsNetworkMessage::StagePatch(_))),
        "lost patch stage should be retried with a patch attachment"
    );
    assert!(
        retry_messages
            .iter()
            .all(|message| !matches!(message, DfsNetworkMessage::StageBlob(_))),
        "patch retry should not fall back to a full-object attachment before an explicit patch rejection"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_cluster_posix_contract_concurrent_write_at_truncate_races() {
    let mut cluster = sifs_posix_cluster("sifs-contract-write-at-truncate");
    let scenario = "concurrent-write-at-truncate-races";
    let path = "/users/contract-ranged.bin";
    let base = b"abcdefghij".to_vec();
    let mut observations = Vec::new();

    sifs_cluster_routed_posix_write(&mut cluster, path, &base);
    sifs_cluster_publish_metadata_until(&mut cluster, "range race replica catch-up", |cluster| {
        cluster.observer.posix_stat(path).is_ok()
    });
    let base_stat = cluster.writer.posix_stat(path).expect("base stat");
    record_posix_observation(
        &mut observations,
        scenario,
        "seed",
        &cluster.writer_node,
        path,
        Instant::now(),
        "committed",
        Some(&base_stat),
    );

    let patch = b"XYZ".to_vec();
    let write_at = cluster.writer.posix_write_file_at(
        path,
        4,
        &patch,
        sifs_cluster_write_options(&cluster.writer_node.clone()),
    );
    let shrink = cluster.observer.posix_truncate_file(
        path,
        6,
        sifs_cluster_write_options(&cluster.observer_node.clone()),
    );
    let overwrite = cluster.primary.posix_write_file(
        path,
        b"whole-file",
        sifs_cluster_write_options(&cluster.primary_node.clone()),
    );
    for (op, node, result) in [
        ("write-at", cluster.writer_node.clone(), write_at),
        ("truncate-shrink", cluster.observer_node.clone(), shrink),
        ("whole-overwrite", cluster.primary_node.clone(), overwrite),
    ] {
        match result {
            Ok(stat) => record_posix_observation(
                &mut observations,
                scenario,
                op,
                &node,
                path,
                Instant::now(),
                "committed",
                Some(&stat),
            ),
            Err(DfsPosixError::PendingWrite { intent_id, .. }) => record_posix_observation(
                &mut observations,
                scenario,
                op,
                &node,
                path,
                Instant::now(),
                format!("pending:{intent_id}"),
                None,
            ),
            Err(DfsPosixError::Conflict { .. }) => {
                panic!("{op} created a DFS conflict during normal operation")
            }
            Err(DfsPosixError::Dfs(DfsError::Busy { .. })) => record_posix_observation(
                &mut observations,
                scenario,
                op,
                &node,
                path,
                Instant::now(),
                "retry-busy",
                None,
            ),
            Err(error) => panic!("{op} returned unexpected error: {error}"),
        }
    }
    sifs_posix_cluster_pump_until(&mut cluster, "write-at/truncate race settle", |cluster| {
        cluster.primary.posix_stat(path).is_ok()
            && sifs_routed_writes_settled(&cluster.writer)
            && sifs_routed_writes_settled(&cluster.observer)
    });

    let final_bytes = sifs_cluster_fetch_for_local_read(&mut cluster, |c| &mut c.primary, path);
    let mut patched = base.clone();
    patched.splice(4..7, patch);
    let shrunk = base[..6].to_vec();
    let mut extended_after_shrink = shrunk.clone();
    extended_after_shrink.resize(12, 0);
    let valid = [
        base.as_slice(),
        patched.as_slice(),
        shrunk.as_slice(),
        extended_after_shrink.as_slice(),
        b"whole-file".as_slice(),
    ];
    assert!(
        valid.contains(&final_bytes.as_slice()),
        "final bytes must match a serialized write_at/truncate/overwrite order: {final_bytes:?}"
    );

    cluster
        .writer
        .posix_truncate_file(
            path,
            12,
            sifs_cluster_write_options(&cluster.writer_node.clone()),
        )
        .unwrap_or_else(|error| panic!("truncate extension after race failed: {error}"));
    sifs_posix_cluster_pump_until(&mut cluster, "post-race truncate extension", |cluster| {
        cluster
            .writer
            .posix_stat(path)
            .is_ok_and(|stat| stat.size_bytes == Some(12))
    });
    let extended = cluster.writer.posix_read_file(path).expect("extended read");
    assert_eq!(
        &extended[final_bytes.len().min(12)..],
        vec![0; 12usize.saturating_sub(final_bytes.len())].as_slice(),
        "truncate extension must zero-fill holes"
    );
    assert_sifs_cluster_no_posix_leaks(&cluster, scenario);
    finish_posix_observations(&observations);
    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn sifs_cluster_posix_write_at_beyond_eof_reads_back_zero_filled_hole() {
    let mut cluster = sifs_posix_cluster("sifs-cluster-hole");
    let path = "/users/sparse.bin";
    let base = b"0123456789".to_vec();
    let patch = b"PATCH".to_vec();
    let hole_offset = 16u64;

    sifs_cluster_routed_posix_write(&mut cluster, path, &base);

    let mut expected = base.clone();
    expected.resize(hole_offset as usize, 0);
    expected.extend_from_slice(&patch);
    let expected_hash = content_hash(&expected);

    let options = sifs_cluster_write_options(&cluster.writer_node.clone());
    match cluster
        .writer
        .posix_write_file_at(path, hole_offset, &patch, options)
    {
        Ok(_) | Err(DfsPosixError::PendingWrite { .. }) => {}
        Err(error) => panic!("routed write_at failed: {error}"),
    }
    sifs_posix_cluster_pump_until(&mut cluster, "write_at hole commit", |cluster| {
        cluster
            .writer
            .posix_stat(path)
            .is_ok_and(|stat| stat.content_hash.as_deref() == Some(expected_hash.as_str()))
    });

    // POSIX hole semantics: bytes between the old EOF and the write offset
    // must read back as zeros.
    let read = cluster.writer.posix_read_file(path).expect("writer read");
    assert_eq!(read, expected);
    let stat = cluster.writer.posix_stat(path).expect("writer stat");
    assert_eq!(stat.size_bytes, Some(expected.len() as u64));

    // Range reads observe the same bytes (hole inclusive).
    let range = cluster
        .writer
        .posix_read_file_range(path, base.len() as u64, expected.len())
        .expect("writer range read");
    assert_eq!(range, expected[base.len()..].to_vec());

    // The primary agrees on the patched content.
    let read = sifs_cluster_fetch_for_local_read(&mut cluster, |c| &mut c.primary, path);
    assert_eq!(read, expected);

    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn sifs_cluster_routed_posix_write_at_from_reader_streams_fallback_overlay() {
    let mut cluster = sifs_posix_cluster("sifs-cluster-routed-reader-overlay");
    let path = "/users/routed-reader-overlay.bin";
    let base = (0..256 * 1024 + 17)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let patch = (0..2 * 1024 * 1024 + 192 * 1024 + 31)
        .map(|index| (index % 239) as u8)
        .collect::<Vec<_>>();
    let patch_offset = base.len() + 64 * 1024 + 9;
    let mut expected = base.clone();
    expected.resize(patch_offset, 0);
    expected.extend_from_slice(&patch);
    let expected_hash = content_hash(&expected);

    sifs_cluster_routed_posix_write(&mut cluster, path, &base);

    struct RoutedStreamingReader {
        bytes: Vec<u8>,
        offset: usize,
        bulk_reads: Arc<AtomicUsize>,
    }

    impl std::io::Read for RoutedStreamingReader {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            if self.offset == 0 {
                assert_eq!(buffer.len(), 1, "POSIX write_at should only peek one byte");
            } else {
                assert!(
                    buffer.len() <= 1024 * 1024,
                    "the routed fallback read buffer must remain bounded, got {} bytes",
                    buffer.len()
                );
                self.bulk_reads.fetch_add(1, Ordering::SeqCst);
            }
            if self.offset >= self.bytes.len() {
                return Ok(0);
            }
            let len = buffer.len().min(self.bytes.len() - self.offset);
            buffer[..len].copy_from_slice(&self.bytes[self.offset..self.offset + len]);
            self.offset += len;
            Ok(len)
        }
    }

    let bulk_reads = Arc::new(AtomicUsize::new(0));
    let options = sifs_cluster_write_options(&cluster.writer_node.clone());
    let write =
        sifs_cluster_writer_call_with_pump(&mut cluster, "routed reader overlay write_at", {
            let bulk_reads = Arc::clone(&bulk_reads);
            move |writer| {
                writer.posix_write_file_at_from_reader(
                    path,
                    patch_offset as u64,
                    RoutedStreamingReader {
                        bytes: patch,
                        offset: 0,
                        bulk_reads,
                    },
                    options,
                )
            }
        });
    match write {
        Ok(_) | Err(DfsPosixError::PendingWrite { .. }) => {}
        Err(error) => panic!("routed reader overlay write_at failed: {error}"),
    }
    sifs_posix_cluster_pump_until(&mut cluster, "routed reader overlay commit", |cluster| {
        sifs_routed_writes_settled(&cluster.writer)
            && cluster
                .writer
                .posix_stat(path)
                .is_ok_and(|stat| stat.content_hash.as_deref() == Some(expected_hash.as_str()))
    });

    assert!(
        bulk_reads.load(Ordering::SeqCst) > 2,
        "the caller's ranged-write input should be consumed incrementally"
    );
    assert_eq!(
        cluster.writer.posix_read_file(path).expect("writer read"),
        expected
    );
    let primary = sifs_cluster_fetch_for_local_read(&mut cluster, |c| &mut c.primary, path);
    assert_eq!(primary, expected);

    std::fs::remove_dir_all(&cluster.root).ok();
}
