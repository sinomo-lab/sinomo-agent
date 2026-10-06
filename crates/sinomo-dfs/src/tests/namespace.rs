//! Namespace regression tests.

use super::*;

#[test]
fn namespace_set_owner_preserves_legacy_wire_shape() {
    #[derive(Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
    #[serde(rename_all = "kebab-case")]
    enum LegacyNamespaceOwnerOperation {
        SetOwner { path: String, uid: u32, gid: u32 },
    }

    let legacy = LegacyNamespaceOwnerOperation::SetOwner {
        path: "/users/legacy-owner".to_string(),
        uid: 1001,
        gid: 1002,
    };
    let current = DfsNamespaceOperation::SetOwner {
        path: "/users/legacy-owner".to_string(),
        uid: 1001,
        gid: 1002,
    };
    let codec = sinomo_core::network::SinomoObjectCodec::network_frame();
    let legacy_bytes = codec.encode(&legacy).expect("encode legacy SetOwner");
    let current_bytes = codec.encode(&current).expect("encode current SetOwner");
    assert_eq!(
        current_bytes, legacy_bytes,
        "SetOwner must retain its original path/uid/gid wire shape"
    );
    assert_eq!(
        codec
            .decode::<LegacyNamespaceOwnerOperation>(&current_bytes)
            .expect("decode current SetOwner as legacy"),
        legacy
    );
    assert_eq!(
        codec
            .decode::<DfsNamespaceOperation>(&legacy_bytes)
            .expect("decode legacy SetOwner as current"),
        current
    );

    for operation in [
        DfsNamespaceOperation::SetSymlinkOwner {
            path: "/users/link-owner".to_string(),
            uid: 1001,
            gid: 1002,
        },
        DfsNamespaceOperation::MkdirWithAttrs {
            path: "/users/private".to_string(),
            attrs: DfsPosixCreateAttrs::new(0o700, 1001, 1002),
        },
    ] {
        let bytes = codec.encode(&operation).expect("encode new operation");
        assert_ne!(bytes, legacy_bytes);
        assert!(
            codec
                .decode::<LegacyNamespaceOwnerOperation>(&bytes)
                .is_err(),
            "new operation must not masquerade as legacy SetOwner"
        );
    }
}

#[tokio::test]
async fn posix_remote_stream_returns_before_body_collection() {
    let local = NodeId::from_legacy_named("node_posix_stream_source").expect("local");
    let remote = NodeId::from_legacy_named("node_posix_stream_target").expect("remote");
    let object_id = DfsObjectId::from_legacy_named("object_posix_stream").expect("object");
    let version = DfsVersion::from_legacy_named("version_posix_stream").expect("version");
    let bytes = vec![0x5a; 48 * 1024];
    let expected_hash = content_hash(&bytes);
    let (body_sender, body) = ObjectRpcIncomingBody::test_channel(Some(bytes.len() as u64), 1);
    let transport =
        node_transport_with_streamed_direct_read_body(local.clone(), remote.clone(), body);
    let root = temp_root("posix-remote-stream-head");
    let mut service = NetworkedDfsService::new(
        transport,
        LocalContentStore::new(root.join("content")).expect("content"),
    );
    service
        .add_file_system(DfsFileSystem::sifs(local))
        .expect("SIFS metadata");
    service
        .add_object_metadata(DfsObjectMetadata {
            object_id: object_id.clone(),
            file_system_id: sifs_file_system_id(),
            path: "/stream-native-read.bin".to_string(),
            kind: DfsObjectKind::File,
            tombstone_provenance: None,
            owner_node_id: remote.clone(),
            current_locations: vec![remote],
            content_hash: Some(expected_hash.clone()),
            content_size_bytes: Some(bytes.len() as u64),
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version: version.clone(),
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        })
        .expect("remote object metadata");

    // No body chunk has been produced. Any hidden compatibility collector in
    // the POSIX or service layer would block this future until timeout.
    let opened = tokio::time::timeout(
        Duration::from_millis(100),
        service.posix_open_remote_file_stream("/stream-native-read.bin"),
    )
    .await
    .expect("POSIX stream head must not collect its body")
    .expect("open POSIX remote stream");
    assert_eq!(opened.object_id, object_id);
    assert_eq!(opened.version, version);
    assert_eq!(opened.content_hash, expected_hash);
    assert_eq!(opened.stream.observed_size_bytes(), 0);

    body_sender
        .send(bytes)
        .await
        .expect("produce POSIX stream body");
    drop(body_sender);
    let completion = opened.stream.finish().await.expect("finalize POSIX stream");
    assert_eq!(completion.content_size_bytes, 48 * 1024);
}

#[test]
fn conditional_unlink_keeps_its_operation_id_after_response_loss_and_primary_movement() {
    let root = temp_root("conditional-unlink-response-loss-primary-move");
    let old_primary_node =
        NodeId::from_legacy_named("node_conditional_unlink_old_primary").expect("old primary");
    let new_primary_node =
        NodeId::from_legacy_named("node_conditional_unlink_new_primary").expect("new primary");
    let requester_node =
        NodeId::from_legacy_named("node_conditional_unlink_requester").expect("requester");
    let network = InProcessNetwork::new();
    let shared_kv = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    let shard = |primary: &NodeId, replicas: Vec<NodeId>, epoch| DfsMetadataShardConfig {
        file_system_id: sifs_file_system_id(),
        shard_id: "root".to_string(),
        primary_node_id: primary.clone(),
        replica_node_ids: replicas,
        epoch,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let promoted_shard = shard(&new_primary_node, vec![old_primary_node.clone()], 2);
    shared_kv.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(&distributed_sifs(
                old_primary_node.clone(),
            )))
            .expect("filesystem config");
        control
            .put_metadata_shard(&shard(&old_primary_node, vec![new_primary_node.clone()], 1))
            .expect("initial shard");
    });
    let mut old_primary = sifs_posix_cluster_node(
        &network,
        &shared_kv,
        &root,
        "old-primary",
        old_primary_node.clone(),
    );
    let mut new_primary = sifs_posix_cluster_node(
        &network,
        &shared_kv,
        &root,
        "new-primary",
        new_primary_node.clone(),
    );
    let requester_root = root.join("requester");
    let durable_promotion_ready = Arc::new(AtomicBool::new(false));
    let requester_transport = PromoteAfterConditionalNamespaceDeliveryTransport {
        inner: network.add_node(requester_node.clone()),
        promotion: Some((shared_kv.clone(), promoted_shard)),
        durable_promotion_ready: Arc::clone(&durable_promotion_ready),
    };
    let mut requester = NetworkedDfsService::new(
        requester_transport,
        LocalContentStore::new(requester_root.join("content")).expect("requester content"),
    )
    .with_control_plane(shared_kv.clone())
    .with_metadata_backend(
        open_root_local_metadata_backend(requester_root.join("metadata"))
            .expect("requester metadata"),
    )
    .expect("attach requester metadata");

    new_primary = std::thread::scope(|scope| {
        let stop = Arc::new(AtomicBool::new(false));
        let pump_stop = Arc::clone(&stop);
        let pump = scope.spawn(move || pump_sifs_service_until_stop(new_primary, pump_stop));
        old_primary
            .bootstrap_distributed_sifs()
            .expect("initial primary bootstrap");
        stop.store(true, Ordering::SeqCst);
        pump.join().expect("bootstrap replica pump")
    });

    let path = "/users/conditional-unlink-after-loss.txt";
    let (old_after_seed, new_after_seed, seeded) = std::thread::scope(|scope| {
        let stop = Arc::new(AtomicBool::new(false));
        let old_stop = Arc::clone(&stop);
        let new_stop = Arc::clone(&stop);
        let old_pump = scope.spawn(move || pump_sifs_service_until_stop(old_primary, old_stop));
        let new_pump = scope.spawn(move || pump_sifs_service_until_stop(new_primary, new_stop));
        let seeded = requester.posix_mkdir_all("/users").and_then(|_| {
            requester.posix_write_file(
                path,
                b"unlink exactly once",
                DfsWriteOptions::replicated_eventual(vec![requester_node.clone()]),
            )
        });
        stop.store(true, Ordering::SeqCst);
        (
            old_pump.join().expect("old primary seed pump"),
            new_pump.join().expect("new primary seed pump"),
            seeded,
        )
    });
    let seeded = seeded.expect("seed routed file");
    old_primary = old_after_seed;
    new_primary = new_after_seed;
    let expected = DfsNamespaceObjectPrecondition {
        object_id: seeded.object_id,
        version: seeded.version,
    };
    let operation_id = format!("cf_{}", sinomo_core::Id::generate());
    let operation = DfsNamespaceOperation::ConditionalUnlink {
        path: path.to_string(),
        expected: expected.clone(),
    };

    let (old_after_unlink, new_after_unlink, unlinked) = std::thread::scope(|scope| {
        let stop = Arc::new(AtomicBool::new(false));
        let old_stop = Arc::clone(&stop);
        let new_stop = Arc::clone(&stop);
        let promotion_ready = Arc::clone(&durable_promotion_ready);
        let retiring_promotion_ready = Arc::clone(&durable_promotion_ready);
        let receipt_requester = requester_node.clone();
        let receipt_operation_id = operation_id.clone();
        let old_pump = scope.spawn(move || {
            while !old_stop.load(Ordering::SeqCst) {
                old_primary
                    .process_incoming()
                    .expect("old primary conditional unlink pump");
                match old_primary.replicate_configured_metadata_shards(&sifs_file_system_id()) {
                    Ok(_) => {}
                    // The test deliberately promotes the successor as soon as
                    // this receipt is durable. A retiring pump can observe the
                    // new epoch before its stop flag; fencing is the expected
                    // result of that race, not a failed unlink publication.
                    Err(DfsError::StaleOwnerEpoch { .. })
                        if retiring_promotion_ready.load(Ordering::SeqCst) => {}
                    Err(error) => panic!("publish terminal conditional unlink receipt: {error}"),
                }
                if !old_stop.load(Ordering::SeqCst) {
                    let _ = old_primary
                        .transport
                        .wait_for_incoming(Duration::from_millis(2));
                }
            }
            old_primary
        });
        let new_pump = scope.spawn(move || {
            while !new_stop.load(Ordering::SeqCst) {
                new_primary
                    .process_incoming()
                    .expect("new primary conditional unlink replica pump");
                let admission = new_primary
                    .metadata_backend_mut()
                    .expect("future primary metadata")
                    .namespace_request_admission_snapshot(
                        &DfsMetadataShardId::new("root"),
                        &receipt_requester,
                        &receipt_operation_id,
                    )
                    .expect("future primary admission snapshot");
                let receipt_is_durable = admission.receipt.is_some()
                    || admission.request.is_some_and(|request| {
                        request.status == DfsNamespaceRequestStatus::Completed
                            && matches!(request.result, Some(DfsNamespaceResult::Applied))
                    });
                if receipt_is_durable {
                    promotion_ready.store(true, Ordering::SeqCst);
                }
                if !new_stop.load(Ordering::SeqCst) {
                    let _ = new_primary
                        .transport
                        .wait_for_incoming(Duration::from_millis(2));
                }
            }
            new_primary
        });
        let unlinked = requester.posix_unlink_if_generation(path, operation_id.clone(), expected);
        stop.store(true, Ordering::SeqCst);
        (
            old_pump.join().expect("old primary unlink pump"),
            new_pump.join().expect("new primary unlink pump"),
            unlinked,
        )
    });
    let tombstone = unlinked.expect("retry exact conditional unlink after response loss");
    assert_eq!(tombstone.kind, DfsObjectKind::Tombstone);

    // Reopen both participants after the injected movement. Reconciliation
    // must find the one receipt under the exact caller id on the promoted
    // authority, rather than treating the retry as a second unlink.
    drop(old_after_unlink);
    drop(new_after_unlink);
    drop(requester);
    let restarted_primary =
        sifs_posix_cluster_node(&network, &shared_kv, &root, "new-primary", new_primary_node);
    let mut restarted_requester = sifs_posix_cluster_node(
        &network,
        &shared_kv,
        &root,
        "requester-restarted",
        requester_node.clone(),
    );
    let (reconciliation, restarted_primary) = std::thread::scope(|scope| {
        let stop = Arc::new(AtomicBool::new(false));
        let pump_stop = Arc::clone(&stop);
        let pump = scope.spawn(move || pump_sifs_service_until_stop(restarted_primary, pump_stop));
        let reconciliation =
            restarted_requester.reconcile_stable_namespace_operation(&operation_id, &operation);
        stop.store(true, Ordering::SeqCst);
        (
            reconciliation,
            pump.join().expect("restarted promoted authority pump"),
        )
    });
    let reconciliation = reconciliation.expect("routed namespace reconciliation");
    let DfsStableNamespaceOperationReconciliation::Applied { receipt } = reconciliation else {
        panic!("exact conditional unlink receipt must be Applied: {reconciliation:?}");
    };
    assert_eq!(receipt.operation_id, operation_id);
    assert_eq!(receipt.requesting_node_id, requester_node);
    assert!(matches!(receipt.result, DfsNamespaceResult::Applied));

    drop(restarted_primary);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_recursive_clear_follows_directory_symlink_argument() {
    let root = temp_root("sifs-recursive-clear-directory-symlink");
    let node_id = NodeId::from_legacy_named("node_sifs_clear_symlink").expect("node");
    let base = "/services/sandbox/instances/sandbox_clear_symlink";
    let real_dir = format!("{base}/tmp/0");
    let link_dir = format!("{base}/work/current-tmp");
    let file_path = format!("{real_dir}/session-cache.txt");

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all(&real_dir).expect("mkdir real tmp");
    dfs.posix_mkdir_all(&format!("{base}/work"))
        .expect("mkdir work");
    dfs.posix_write_file(
        &file_path,
        b"cache",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write cache");
    dfs.posix_symlink(
        "../tmp/0",
        &link_dir,
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("symlink directory");

    dfs.posix_clear_dir_recursive(&link_dir)
        .expect("clear via directory symlink");

    assert!(matches!(
        dfs.posix_read_file(&file_path),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert_eq!(
        dfs.posix_lstat(&link_dir)
            .expect("link survives clear")
            .kind,
        DfsObjectKind::Symlink
    );
    assert!(
        dfs.posix_list_dir(&real_dir)
            .expect("real dir survives")
            .is_empty()
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_recursive_clear_is_scoped_to_target_subtree() {
    let root = temp_root("sifs-recursive-clear-scoped-subtree");
    let node_id = NodeId::from_legacy_named("node_sifs_clear_scoped").expect("node");
    let target_root = "/services/sandbox/instances/sandbox_clear_scoped/tmp/0";
    let target_file = "/services/sandbox/instances/sandbox_clear_scoped/tmp/0/nested/cache.txt";
    let sibling_file = "/services/sandbox/instances/sandbox_clear_scoped/tmp/1/nested/cache.txt";
    let unrelated_file =
        "/services/sandbox/instances/sandbox_clear_scoped/work/unrelated/cache.txt";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all(&format!("{target_root}/nested"))
        .expect("mkdir target");
    dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_clear_scoped/tmp/1/nested")
        .expect("mkdir sibling");
    dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_clear_scoped/work/unrelated")
        .expect("mkdir unrelated");
    for path in [target_file, sibling_file, unrelated_file] {
        dfs.posix_write_file(
            path,
            path.as_bytes(),
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("write file");
    }

    dfs.posix_clear_dir_recursive(target_root)
        .expect("clear target subtree");

    assert!(matches!(
        dfs.posix_read_file(target_file),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert_eq!(
        dfs.posix_read_file(sibling_file).expect("sibling survives"),
        sibling_file.as_bytes()
    );
    assert_eq!(
        dfs.posix_read_file(unrelated_file)
            .expect("unrelated survives"),
        unrelated_file.as_bytes()
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_nonexclusive_create_follows_a_dangling_symlink() {
    let root = temp_root("sifs-posix-create-dangling-symlink");
    let node_id = NodeId::from_legacy_named("node_sifs_create_dangling_symlink").expect("node");
    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all("/services/dangling-create")
        .expect("mkdir parent");
    let options = DfsWriteOptions::replicated_eventual(vec![node_id]);
    let link = "/services/dangling-create/link";
    let target = "/services/dangling-create/target";
    dfs.posix_symlink("target", link, options.clone())
        .expect("create dangling symlink");

    assert!(matches!(
        dfs.posix_create_file_with_attrs(
            link,
            options.clone(),
            DfsPosixCreateAttrs::new(0o600, 7001, 7002),
        ),
        Err(DfsPosixError::AlreadyExists { .. })
    ));
    let created = dfs
        .posix_create_file_following_symlink_with_attrs(
            link,
            options,
            DfsPosixCreateAttrs::new(0o640, 7001, 7002),
        )
        .expect("create dangling symlink target");
    assert_eq!(created.path, target);
    assert_eq!(
        (created.mode, created.uid, created.gid),
        (0o640, 7001, 7002)
    );
    assert_eq!(
        dfs.posix_lstat(link).expect("link remains").kind,
        DfsObjectKind::Symlink
    );
    assert_eq!(dfs.posix_readlink(link).expect("read link"), "target");

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_create_attrs_are_installed_atomically_and_only_on_create() {
    let root = temp_root("sifs-posix-create-attrs");
    let node_id = NodeId::from_legacy_named("node_sifs_create_attrs").expect("node");
    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all("/services/create-attrs")
        .expect("mkdir parent");

    let dir_path = "/services/create-attrs/private";
    let dir = dfs
        .posix_mkdir_with_create_attrs(dir_path, DfsPosixCreateAttrs::new(0o2750, 1001, 2001))
        .expect("mkdir with attrs");
    assert_eq!((dir.mode, dir.uid, dir.gid), (0o2750, 1001, 2001));

    let options = DfsWriteOptions::replicated_eventual(vec![node_id.clone()]);
    let file_path = "/services/create-attrs/private/file";
    let file = dfs
        .posix_write_file_with_create_attrs(
            file_path,
            b"one",
            options.clone(),
            DfsPosixCreateAttrs::new(0o640, 1002, 2002),
        )
        .expect("file with attrs");
    assert_eq!((file.mode, file.uid, file.gid), (0o640, 1002, 2001));
    let overwritten = dfs
        .posix_write_file_with_create_attrs(
            file_path,
            b"two",
            options.clone(),
            DfsPosixCreateAttrs::new(0o600, 3002, 4002),
        )
        .expect("overwrite existing file");
    assert_eq!(
        (overwritten.mode, overwritten.uid, overwritten.gid),
        (0o640, 1002, 2001),
        "create attributes must not mutate an existing inode"
    );

    dfs.posix_unlink(file_path).expect("unlink file");
    let recreated = dfs
        .posix_write_file_with_create_attrs(
            file_path,
            b"three",
            options.clone(),
            DfsPosixCreateAttrs::new(0o600, 3002, 4002),
        )
        .expect("recreate tombstoned file");
    assert_eq!(
        (recreated.mode, recreated.uid, recreated.gid),
        (0o600, 3002, 2001)
    );

    let streamed = dfs
        .posix_write_file_from_reader_with_create_attrs(
            "/services/create-attrs/private/streamed",
            std::io::Cursor::new(b"streamed"),
            options.clone(),
            DfsPosixCreateAttrs::new(0o620, 3003, 4003),
        )
        .expect("reader-backed file with attrs");
    assert_eq!(
        (streamed.mode, streamed.uid, streamed.gid),
        (0o620, 3003, 2001)
    );

    let link_path = "/services/create-attrs/private/link";
    let link = dfs
        .posix_symlink_with_create_attrs(
            "file",
            link_path,
            options,
            DfsPosixCreateAttrs::new(0o777, 1003, 2003),
        )
        .expect("symlink with attrs");
    assert_eq!((link.mode, link.uid, link.gid), (0o777, 1003, 2001));
    assert_eq!(
        (
            dfs.posix_lstat(link_path).expect("lstat link").mode,
            dfs.posix_lstat(link_path).expect("lstat link").uid,
            dfs.posix_lstat(link_path).expect("lstat link").gid,
        ),
        (0o777, 1003, 2001)
    );

    dfs.posix_rmdir(dir_path).expect_err("non-empty dir");
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_symlink_preserves_target_as_object_content() {
    let root = temp_root("sifs-posix-symlink");
    let node_id = NodeId::from_legacy_named("node_sifs_symlink").expect("node");
    let link_path = "/services/sandbox/instances/sandbox_link/work/lib";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_link/work")
        .expect("mkdir");
    let stat = dfs
        .posix_symlink(
            "../shared/lib",
            link_path,
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("symlink");

    assert_eq!(stat.kind, DfsObjectKind::Symlink);
    assert_eq!(stat.size_bytes, Some("../shared/lib".len() as u64));
    assert_eq!(
        dfs.posix_readlink(link_path).expect("readlink"),
        "../shared/lib"
    );
    let owned_link = dfs
        .posix_set_symlink_owner(link_path, 1001, 1002)
        .expect("set symlink owner without following missing target");
    assert_eq!((owned_link.uid, owned_link.gid), (1001, 1002));
    assert_eq!(
        (
            dfs.posix_lstat(link_path).expect("owned link lstat").uid,
            dfs.posix_lstat(link_path).expect("owned link lstat").gid,
        ),
        (1001, 1002)
    );
    dfs.posix_set_symlink_times(link_path, Some(12_345), Some(67_890))
        .expect("old symlink times");
    let before_readlink = dfs.posix_lstat(link_path).expect("before readlink lstat");
    assert_eq!(
        dfs.posix_readlink_update_access(link_path)
            .expect("readlink update access"),
        "../shared/lib"
    );
    let stat = dfs.posix_lstat(link_path).expect("symlink lstat");
    assert!(stat.accessed_at_unix_nanos > 12_345);
    assert_eq!(stat.modified_at_unix_nanos, 67_890);
    assert_eq!(
        stat.changed_at_unix_nanos, before_readlink.changed_at_unix_nanos,
        "readlink access update must not advance symlink ctime"
    );
    assert_ne!(
        stat.version, before_readlink.version,
        "readlink access update must advance the DFS metadata version so it persists across distributed reconciliation"
    );
    assert!(matches!(
        dfs.posix_symlink(
            "other",
            link_path,
            DfsWriteOptions::replicated_eventual(vec![dfs.node_id().clone()])
        ),
        Err(DfsPosixError::AlreadyExists { .. })
    ));

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_trailing_slash_requires_directory() {
    let root = temp_root("sifs-posix-trailing-slash-directory");
    let node_id = NodeId::from_legacy_named("node_sifs_trailing_slash").expect("node");
    let work = "/services/sandbox/instances/sandbox_trailing/work";
    let real_dir = "/services/sandbox/instances/sandbox_trailing/work/real-dir";
    let file_path = "/services/sandbox/instances/sandbox_trailing/work/file.txt";
    let link_dir = "/services/sandbox/instances/sandbox_trailing/work/link-dir";
    let link_file = "/services/sandbox/instances/sandbox_trailing/work/link-file";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all(real_dir).expect("mkdir real dir");
    dfs.posix_write_file(
        file_path,
        b"content",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write file");
    dfs.posix_symlink(
        "real-dir",
        link_dir,
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("symlink dir");
    dfs.posix_symlink(
        "file.txt",
        link_file,
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("symlink file");

    assert_eq!(
        dfs.posix_lstat(link_dir).expect("plain lstat").kind,
        DfsObjectKind::Symlink
    );
    assert_eq!(
        dfs.posix_lstat(&format!("{link_dir}/"))
            .expect("trailing slash lstat follows directory symlink")
            .kind,
        DfsObjectKind::Directory
    );
    assert!(matches!(
        dfs.posix_read_file(&format!("{file_path}/")),
        Err(DfsPosixError::NotDirectory { .. })
    ));
    assert!(matches!(
        dfs.posix_read_file(&format!("{link_file}/")),
        Err(DfsPosixError::NotDirectory { .. })
    ));
    assert!(matches!(
        dfs.posix_readlink(&format!("{link_dir}/")),
        Err(DfsPosixError::NotSymlink { .. })
    ));
    assert!(matches!(
        dfs.posix_rename(&format!("{file_path}/"), &format!("{work}/renamed.txt")),
        Err(DfsPosixError::NotDirectory { .. })
    ));
    assert!(matches!(
        dfs.posix_rename(file_path, &format!("{work}/missing-destination/")),
        Err(DfsPosixError::NotDirectory { .. })
    ));
    assert!(matches!(
        dfs.posix_hardlink(&format!("{file_path}/"), &format!("{work}/file-alias.txt")),
        Err(DfsPosixError::NotDirectory { .. })
    ));
    assert!(matches!(
        dfs.posix_hardlink(file_path, &format!("{work}/missing-alias/")),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert!(matches!(
        dfs.posix_symlink(
            "file.txt",
            &format!("{work}/missing-link/"),
            DfsWriteOptions::replicated_eventual(vec![dfs.node_id().clone()])
        ),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert!(matches!(
        dfs.posix_symlink(
            "file.txt",
            &format!("{real_dir}/"),
            DfsWriteOptions::replicated_eventual(vec![dfs.node_id().clone()])
        ),
        Err(DfsPosixError::AlreadyExists { .. })
    ));
    assert!(matches!(
        dfs.posix_unlink(&format!("{link_dir}/")),
        Err(DfsPosixError::IsDirectory { .. })
    ));
    assert_eq!(
        dfs.posix_lstat(link_dir)
            .expect("symlink survives failed unlink")
            .kind,
        DfsObjectKind::Symlink
    );
    assert_eq!(
        dfs.posix_stat(real_dir).expect("target survives").kind,
        DfsObjectKind::Directory
    );
    assert_eq!(
        dfs.posix_list_dir(&format!("{work}/"))
            .expect("list with trailing slash")
            .len(),
        4
    );
    dfs.posix_mkdir_all(&format!("{work}/rename-source-dir"))
        .expect("mkdir rename source dir");
    dfs.posix_rename(
        &format!("{work}/rename-source-dir"),
        &format!("{work}/renamed-dir/"),
    )
    .expect("rename directory to trailing slash missing path");
    assert_eq!(
        dfs.posix_stat(&format!("{work}/renamed-dir"))
            .expect("renamed directory")
            .kind,
        DfsObjectKind::Directory
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_rmdir_reports_directory_not_empty() {
    let root = temp_root("sifs-posix-rmdir-not-empty");
    let node_id = NodeId::from_legacy_named("node_sifs_rmdir").expect("node");
    let dir_path = "/services/sandbox/instances/sandbox_rmdir/work";
    let file_path = "/services/sandbox/instances/sandbox_rmdir/work/file.txt";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all(dir_path).expect("mkdir");
    dfs.posix_write_file(
        file_path,
        b"content",
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("write");

    assert!(matches!(
        dfs.posix_rmdir(dir_path),
        Err(DfsPosixError::DirectoryNotEmpty { .. })
    ));

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_empty_directory_checks_are_bounded_for_large_directories() {
    let root = temp_root("sifs-posix-bounded-empty-dir-check");
    let node_id = NodeId::from_legacy_named("node_sifs_bounded_empty_dir").expect("node");
    let source = "/services/sandbox/instances/sandbox_bounded_empty_dir/work/source";
    let nonempty = "/services/sandbox/instances/sandbox_bounded_empty_dir/work/nonempty";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all(source).expect("mkdir source");
    for index in 0..160 {
        dfs.posix_mkdir_all(&format!("{nonempty}/child-{index:04}"))
            .expect("mkdir child");
    }
    let mut transient = file_metadata(node_id.clone());
    transient.file_system_id = sifs_file_system_id();
    transient.object_id =
        DfsObjectId::from_legacy_named("obj_sifs_bounded_empty_dir_transient").expect("object id");
    transient.path = format!("{nonempty}/transient.txt");
    transient.version = DfsVersion::generate();
    transient.content_hash = None;
    transient.content_size_bytes = Some(0);
    dfs.add_transient_object_metadata(transient)
        .expect("transient child overlay");

    crate::posix::reset_posix_metadata_full_directory_collect_count();
    assert!(matches!(
        dfs.posix_rmdir(nonempty),
        Err(DfsPosixError::DirectoryNotEmpty { .. })
    ));
    assert_eq!(
        crate::posix::posix_metadata_full_directory_collect_count(),
        0,
        "rmdir emptiness checks must stay on the bounded dentry page path with an overlay"
    );

    crate::posix::reset_posix_metadata_full_directory_collect_count();
    assert!(matches!(
        dfs.posix_rename(source, nonempty),
        Err(DfsPosixError::DirectoryNotEmpty { .. })
    ));
    assert_eq!(
        crate::posix::posix_metadata_full_directory_collect_count(),
        0,
        "rename destination checks must stay on the bounded dentry page path with an overlay"
    );
    assert!(dfs.posix_stat(source).is_ok());

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_stat_reports_directory_link_counts() {
    let root = temp_root("sifs-posix-link-count");
    let node_id = NodeId::from_legacy_named("node_sifs_link_count").expect("node");
    let parent = "/services/sandbox/instances/sandbox_links/work";
    let child_dir = "/services/sandbox/instances/sandbox_links/work/child";
    let nested_file = "/services/sandbox/instances/sandbox_links/work/file.txt";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all(child_dir).expect("mkdir child");
    dfs.posix_write_file(
        nested_file,
        b"content",
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("write");

    assert_eq!(dfs.posix_stat(parent).expect("parent stat").link_count, 3);
    assert_eq!(dfs.posix_stat(child_dir).expect("child stat").link_count, 2);
    assert_eq!(
        dfs.posix_stat(nested_file).expect("file stat").link_count,
        1
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_rename_directory_batches_child_moves_per_job_batch() {
    let root = temp_root("sifs-posix-rename-dir-batched-children");
    let node_id = NodeId::from_legacy_named("node_sifs_rename_dir_batch").expect("node");
    let old_dir = "/services/sandbox/instances/sandbox_rename_batch/work/old";
    let new_dir = "/services/sandbox/instances/sandbox_rename_batch/work/new";
    let max_steps = 4;
    let child_count = 10usize;

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all(old_dir).expect("mkdir old dir");
    let mut old_paths = Vec::new();
    let mut new_paths = Vec::new();
    let mut child_object_ids = BTreeSet::new();
    for index in 0..child_count {
        let old_path = format!("{old_dir}/file-{index:02}.txt");
        let new_path = format!("{new_dir}/file-{index:02}.txt");
        dfs.posix_write_file(
            &old_path,
            old_path.as_bytes(),
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("write child");
        child_object_ids.insert(dfs.posix_stat(&old_path).expect("child stat").object_id);
        old_paths.push(old_path);
        new_paths.push(new_path);
    }
    let journal_after_sequence = dfs
        .metadata_backend()
        .expect("metadata backend")
        .journal_since(0)
        .expect("journal before rename")
        .last()
        .map(|entry| entry.sequence)
        .unwrap_or(0);

    let mut job = dfs
        .posix_rename_directory_bounded(old_dir, new_dir, max_steps)
        .expect("first bounded rename batch");
    while job.status != DfsNamespaceJobStatus::Completed {
        job = dfs
            .posix_resume_namespace_job_bounded(&job.job_id, max_steps)
            .expect("resume bounded rename batch");
    }

    let journal = dfs
        .metadata_backend()
        .expect("metadata backend")
        .journal_since(journal_after_sequence)
        .expect("rename journal");
    let child_move_transactions = journal
        .iter()
        .filter_map(|entry| match &entry.mutation {
            DfsMetadataMutation::ApplyNamespaceTransaction {
                transaction_id,
                transaction: Some(transaction),
                ..
            } if transaction_id.starts_with("namespace_rename-child-batch_") => Some(transaction),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(
        child_move_transactions.len() < child_count,
        "bounded rename should journal child moves per job batch, not per child"
    );
    assert!(
        child_move_transactions
            .iter()
            .any(|transaction| transaction.objects.len() > 1),
        "at least one bounded batch should contain multiple child moves"
    );
    assert!(
        child_move_transactions
            .iter()
            .all(|transaction| transaction.objects.len() <= max_steps),
        "a child-move transaction must remain bounded by the job step budget"
    );
    let journaled_child_object_ids = child_move_transactions
        .iter()
        .flat_map(|transaction| transaction.objects.iter())
        .map(|object| object.object_id.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(journaled_child_object_ids, child_object_ids);
    assert_eq!(
        child_move_transactions
            .iter()
            .map(|transaction| transaction.objects.len())
            .sum::<usize>(),
        child_count,
        "each child should be journaled exactly once"
    );
    assert!(job.processed_object_count >= child_count as u64);
    for (old_path, new_path) in old_paths.iter().zip(new_paths.iter()) {
        assert!(matches!(
            dfs.posix_stat(old_path),
            Err(DfsPosixError::NotFound { .. })
        ));
        assert_eq!(
            dfs.posix_read_file(new_path).expect("moved child"),
            old_path.as_bytes()
        );
    }

    drop(dfs);
    let reopened = networked_sifs_with_backend(&root, node_id);
    for new_path in &new_paths {
        assert_eq!(
            reopened.posix_stat(new_path).expect("reopened child").kind,
            DfsObjectKind::File
        );
    }

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_rename_directory_caps_child_move_transactions_above_step_budget() {
    let root = temp_root("sifs-posix-rename-dir-capped-children");
    let node_id = NodeId::from_legacy_named("node_sifs_rename_dir_cap").expect("node");
    let old_dir = "/services/sandbox/instances/sandbox_rename_cap/work/old";
    let new_dir = "/services/sandbox/instances/sandbox_rename_cap/work/new";
    let transaction_cap = crate::posix::POSIX_RENAME_MOVE_TRANSACTION_MAX_OBJECTS;
    let child_count = transaction_cap + 1;
    let max_steps = transaction_cap * 2;

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all(old_dir).expect("mkdir old dir");
    let children = (0..child_count)
        .map(|index| {
            let mut object = file_metadata(node_id.clone());
            object.object_id = DfsObjectId::from_legacy_named(format!("obj_rename_cap_{index:04}"))
                .expect("object id");
            object.path = format!("{old_dir}/file-{index:04}.txt");
            object
        })
        .collect::<Vec<_>>();
    let child_object_ids = children
        .iter()
        .map(|object| object.object_id.clone())
        .collect::<BTreeSet<_>>();
    dfs.add_object_metadata_batch(children)
        .expect("populate child metadata");
    let journal_after_sequence = dfs
        .metadata_backend()
        .expect("metadata backend")
        .journal_since(0)
        .expect("journal before rename")
        .last()
        .map(|entry| entry.sequence)
        .unwrap_or(0);

    let job = dfs
        .posix_rename_directory_bounded(old_dir, new_dir, max_steps)
        .expect("oversized bounded rename batch");
    assert_eq!(job.status, DfsNamespaceJobStatus::Completed);

    let journal = dfs
        .metadata_backend()
        .expect("metadata backend")
        .journal_since(journal_after_sequence)
        .expect("rename journal");
    let child_move_transactions = journal
        .iter()
        .filter_map(|entry| match &entry.mutation {
            DfsMetadataMutation::ApplyNamespaceTransaction {
                transaction_id,
                transaction: Some(transaction),
                ..
            } if transaction_id.starts_with("namespace_rename-child-batch_") => Some(transaction),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        child_move_transactions.len(),
        2,
        "one child over the cap should require exactly two transactions"
    );
    assert_eq!(
        child_move_transactions
            .iter()
            .map(|transaction| transaction.objects.len())
            .max(),
        Some(transaction_cap)
    );
    assert!(
        child_move_transactions
            .iter()
            .all(|transaction| transaction.objects.len() <= transaction_cap),
        "the caller's step budget must not enlarge durable transactions"
    );
    let journaled_child_object_ids = child_move_transactions
        .iter()
        .flat_map(|transaction| transaction.objects.iter())
        .map(|object| object.object_id.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(journaled_child_object_ids, child_object_ids);
    assert_eq!(
        child_move_transactions
            .iter()
            .map(|transaction| transaction.objects.len())
            .sum::<usize>(),
        child_count
    );
    assert_eq!(
        dfs.posix_stat(&format!("{new_dir}/file-0000.txt"))
            .expect("first moved child")
            .kind,
        DfsObjectKind::File
    );
    assert_eq!(
        dfs.posix_stat(&format!("{new_dir}/file-{:04}.txt", child_count - 1))
            .expect("last moved child")
            .kind,
        DfsObjectKind::File
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_rename_directory_does_not_resurrect_deleted_children() {
    let root = temp_root("sifs-posix-rename-dir-deleted-child");
    let node_id = NodeId::from_legacy_named("node_sifs_rename_dir_deleted_child").expect("node");
    let old_dir = "/services/sandbox/instances/sandbox_rename_deleted/work/old";
    let new_dir = "/services/sandbox/instances/sandbox_rename_deleted/work/new";
    let old_child = format!("{old_dir}/deleted.txt");
    let new_child = format!("{new_dir}/deleted.txt");

    {
        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        dfs.posix_mkdir_all(old_dir).expect("mkdir old dir");
        dfs.posix_write_file(
            &old_child,
            b"deleted",
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("write child");
        dfs.posix_unlink(&old_child).expect("unlink child");
        dfs.posix_rename(old_dir, new_dir).expect("rename dir");

        assert!(matches!(
            dfs.posix_read_file(&new_child),
            Err(DfsPosixError::NotFound { .. })
        ));
        assert!(
            dfs.posix_list_dir(new_dir)
                .expect("new dir listing")
                .is_empty()
        );
    }

    let reloaded = networked_sifs_with_backend(&root, node_id);
    assert!(matches!(
        reloaded.posix_read_file(&new_child),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert!(
        reloaded
            .posix_list_dir(new_dir)
            .expect("reloaded new dir listing")
            .is_empty()
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_rename_over_existing_file_survives_reload() {
    let root = temp_root("sifs-posix-rename-over-existing-file-reload");
    let node_id = NodeId::from_legacy_named("node_sifs_rename_over_existing_reload").expect("node");
    let work = "/services/sandbox/instances/sandbox_rename_over_existing/work";
    let source = "/services/sandbox/instances/sandbox_rename_over_existing/work/source.txt";
    let dest = "/services/sandbox/instances/sandbox_rename_over_existing/work/dest.txt";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all(work).expect("mkdir");
    dfs.posix_write_file(
        source,
        b"source",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write source");
    let source_object_id = dfs.posix_stat(source).expect("source stat").object_id;
    dfs.posix_write_file(
        dest,
        b"dest",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write dest");

    dfs.posix_rename(source, dest).expect("rename over dest");

    assert!(matches!(
        dfs.posix_stat(source),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert_eq!(dfs.posix_read_file(dest).expect("read dest"), b"source");
    assert_eq!(
        dfs.posix_stat(dest).expect("dest stat").object_id,
        source_object_id
    );

    let reloaded = networked_sifs_with_backend(&root, node_id);
    assert!(matches!(
        reloaded.posix_stat(source),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert_eq!(
        reloaded.posix_read_file(dest).expect("reloaded read dest"),
        b"source"
    );
    assert_eq!(
        reloaded
            .posix_stat(dest)
            .expect("reloaded dest stat")
            .object_id,
        source_object_id
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_set_times_without_fields_is_noop() {
    let root = temp_root("sifs-posix-set-times-noop");
    let node_id = NodeId::from_legacy_named("node_sifs_set_times_noop").expect("node");
    let file = "/services/sandbox/instances/sandbox_set_times_noop/work/file.txt";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_set_times_noop/work")
        .expect("mkdir");
    dfs.posix_write_file(
        file,
        b"content",
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("write file");
    dfs.posix_set_times(file, Some(12_345), Some(67_890))
        .expect("set baseline times");
    let before = dfs.posix_stat(file).expect("before stat");

    let after = dfs
        .posix_set_times(file, None, None)
        .expect("set no timestamp fields");

    assert_eq!(after.accessed_at_unix_nanos, before.accessed_at_unix_nanos);
    assert_eq!(after.modified_at_unix_nanos, before.modified_at_unix_nanos);
    assert_eq!(after.changed_at_unix_nanos, before.changed_at_unix_nanos);
    assert_eq!(
        after.version, before.version,
        "omitting both timestamps must not churn the distributed metadata version"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_set_mode_zero_is_reported_as_zero() {
    let root = temp_root("sifs-posix-set-mode-zero");
    let node_id = NodeId::from_legacy_named("node_sifs_set_mode_zero").expect("node");
    let file = "/services/sandbox/instances/sandbox_set_mode_zero/work/file.txt";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_set_mode_zero/work")
        .expect("mkdir");
    dfs.posix_write_file(
        file,
        b"content",
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("write file");

    let changed = dfs.posix_set_mode(file, 0).expect("chmod 0000");
    assert_eq!(changed.mode, 0);
    assert_eq!(dfs.posix_stat(file).expect("stat").mode, 0);
    assert!(
        dfs.posix_stat(file).expect("stat").changed_at_unix_nanos >= changed.changed_at_unix_nanos
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_metadata_only_versions_preserve_content_size() {
    let root = temp_root("sifs-posix-metadata-only-size");
    let node_id = NodeId::from_legacy_named("node_sifs_metadata_only_size").expect("node");
    let work = "/services/sandbox/instances/sandbox_metadata_size/work";
    let file = "/services/sandbox/instances/sandbox_metadata_size/work/file.txt";
    let renamed = "/services/sandbox/instances/sandbox_metadata_size/work/renamed.txt";
    let link = "/services/sandbox/instances/sandbox_metadata_size/work/link.txt";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all(work).expect("mkdir");
    dfs.posix_write_file(
        file,
        b"content",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write file");
    assert_eq!(
        dfs.posix_set_mode(file, 0o600).expect("chmod").size_bytes,
        Some(7)
    );
    assert_eq!(
        dfs.posix_rename(file, renamed).expect("rename").size_bytes,
        Some(7)
    );

    dfs.posix_symlink(
        "renamed.txt",
        link,
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("symlink");
    assert_eq!(
        dfs.posix_set_symlink_times(link, Some(1), Some(2))
            .expect("set symlink times")
            .size_bytes,
        Some("renamed.txt".len() as u64)
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_recreate_file_refreshes_creation_metadata() {
    let root = temp_root("sifs-posix-recreate-file-times");
    let node_id = NodeId::from_legacy_named("node_sifs_recreate_file").expect("node");
    let file_path = "/services/sandbox/instances/sandbox_recreate/work/file.txt";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_recreate/work")
        .expect("mkdir");
    dfs.posix_write_file(
        file_path,
        b"old",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write old");
    dfs.posix_set_times(file_path, Some(12_345), Some(67_890))
        .expect("old times");
    dfs.posix_unlink(file_path).expect("unlink");

    let recreated = dfs
        .posix_write_file(
            file_path,
            b"new",
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("recreate");

    assert_eq!(recreated.size_bytes, Some(3));
    assert!(recreated.accessed_at_unix_nanos > 12_345);
    assert!(recreated.modified_at_unix_nanos > 67_890);
    assert!(recreated.created_at_unix_nanos > 67_890);
    assert_eq!(dfs.posix_read_file(file_path).expect("read"), b"new");

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_recreate_directory_refreshes_creation_metadata() {
    let root = temp_root("sifs-posix-recreate-dir-times");
    let node_id = NodeId::from_legacy_named("node_sifs_recreate_dir").expect("node");
    let dir_path = "/services/sandbox/instances/sandbox_recreate_dir/work/dir";

    let mut dfs = networked_sifs_with_backend(&root, node_id);
    dfs.posix_mkdir_all(dir_path).expect("mkdir");
    dfs.posix_set_times(dir_path, Some(12_345), Some(67_890))
        .expect("old times");
    dfs.posix_rmdir(dir_path).expect("rmdir");
    let recreated = dfs.posix_mkdir(dir_path).expect("recreate dir");

    assert!(recreated.accessed_at_unix_nanos > 12_345);
    assert!(recreated.modified_at_unix_nanos > 67_890);
    assert!(recreated.created_at_unix_nanos > 67_890);

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_create_bulk_local_eventual_rejects_existing_paths() {
    let root = temp_root("sifs-posix-create-bulk-local-eventual");
    let node_id = NodeId::from_legacy_named("node_sifs_create_bulk_local").expect("node");
    let work = "/services/sandbox/instances/sandbox_create_bulk_local/work";
    let path_a = "/services/sandbox/instances/sandbox_create_bulk_local/work/a.txt";
    let path_b = "/services/sandbox/instances/sandbox_create_bulk_local/work/b.txt";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all(work).expect("mkdir");
    let parent_before = dfs.posix_stat(work).expect("parent before bulk create");
    let stats = dfs
        .posix_create_files_local_eventual_with_known_parents(vec![
            (path_a, b"bulk create alpha".as_slice()),
            (path_b, b"bulk create beta".as_slice()),
        ])
        .expect("bulk create");

    assert_eq!(stats.len(), 2);
    assert_eq!(stats[0].path, path_a);
    assert_eq!(stats[0].current_locations, vec![node_id.clone()]);
    assert_eq!(stats[1].path, path_b);
    assert_eq!(stats[1].current_locations, vec![node_id]);
    assert_eq!(
        dfs.posix_read_file(path_a).expect("read a"),
        b"bulk create alpha"
    );
    assert_eq!(
        dfs.posix_read_file(path_b).expect("read b"),
        b"bulk create beta"
    );
    let parent_after = dfs.posix_stat(work).expect("parent after bulk create");
    assert_ne!(
        parent_after.version, parent_before.version,
        "bulk create should advance the parent directory metadata version"
    );
    assert!(
        parent_after.modified_at_unix_nanos >= parent_before.modified_at_unix_nanos,
        "bulk create should refresh the parent directory mtime"
    );

    assert!(
        matches!(
            dfs.posix_create_files_local_eventual_with_known_parents(vec![(
                path_a,
                b"must not overwrite".as_slice()
            )]),
            Err(DfsPosixError::AlreadyExists { path }) if path == path_a
        ),
        "create-only batch must reject existing paths"
    );
    assert_eq!(
        dfs.posix_read_file(path_a).expect("read unchanged a"),
        b"bulk create alpha"
    );

    dfs.posix_unlink(path_a).expect("tombstone existing path");
    let recreated = dfs
        .posix_create_files_local_eventual_with_known_parents(vec![(
            path_a,
            b"bulk recreate alpha".as_slice(),
        )])
        .expect("bulk recreate tombstoned path");
    assert_eq!(recreated.len(), 1);
    assert_eq!(
        dfs.posix_read_file(path_a).expect("read recreated a"),
        b"bulk recreate alpha"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_list_updates_directory_atime_without_changing_mtime() {
    let root = temp_root("sifs-posix-list-atime");
    let node_id = NodeId::from_legacy_named("node_sifs_list_atime").expect("node");
    let dir_path = "/services/sandbox/instances/sandbox_list_atime/work";
    let file_path = "/services/sandbox/instances/sandbox_list_atime/work/file.txt";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all(dir_path).expect("mkdir");
    dfs.posix_write_file(
        file_path,
        b"content",
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("write");
    dfs.posix_set_times(dir_path, Some(12_345), Some(67_890))
        .expect("old times");

    let entries = dfs
        .posix_list_dir_update_access(dir_path)
        .expect("list update access");
    assert_eq!(
        entries
            .iter()
            .map(|entry| entry.name.as_str())
            .collect::<Vec<_>>(),
        vec!["file.txt"]
    );

    let stat = dfs.posix_stat(dir_path).expect("stat");
    assert!(stat.accessed_at_unix_nanos > 12_345);
    assert_eq!(stat.modified_at_unix_nanos, 67_890);

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_namespace_mutations_update_parent_directory_mtime() {
    let root = temp_root("sifs-posix-parent-mtime");
    let node_id = NodeId::from_legacy_named("node_sifs_parent_mtime").expect("node");
    let work = "/services/sandbox/instances/sandbox_parent_mtime/work";
    let other = "/services/sandbox/instances/sandbox_parent_mtime/other";
    let file = "/services/sandbox/instances/sandbox_parent_mtime/work/file.txt";
    let alias = "/services/sandbox/instances/sandbox_parent_mtime/work/alias.txt";
    let link = "/services/sandbox/instances/sandbox_parent_mtime/work/link.txt";
    let renamed = "/services/sandbox/instances/sandbox_parent_mtime/other/renamed.txt";
    let dir = "/services/sandbox/instances/sandbox_parent_mtime/work/dir";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all(work).expect("mkdir work");
    dfs.posix_mkdir_all(other).expect("mkdir other");

    dfs.posix_set_times(work, Some(12_345), Some(67_890))
        .expect("old work times");
    let before_create = dfs.posix_stat(work).expect("work before create");
    std::thread::sleep(std::time::Duration::from_millis(1));
    dfs.posix_write_file(
        file,
        b"content",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("create file");
    let after_create = dfs.posix_stat(work).expect("work after create");
    assert!(after_create.modified_at_unix_nanos > 67_890);
    assert!(
        after_create.changed_at_unix_nanos > before_create.changed_at_unix_nanos,
        "creating a directory entry must advance parent directory ctime"
    );

    dfs.posix_set_times(work, Some(12_345), Some(67_890))
        .expect("reset work times");
    dfs.posix_write_file(
        file,
        b"updated",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("overwrite file");
    assert_eq!(
        dfs.posix_stat(work)
            .expect("work after overwrite")
            .modified_at_unix_nanos,
        67_890,
        "overwriting existing file content must not change parent directory mtime"
    );

    dfs.posix_hardlink(file, alias).expect("hardlink");
    assert!(
        dfs.posix_stat(work)
            .expect("work after hardlink")
            .modified_at_unix_nanos
            > 67_890
    );

    dfs.posix_set_times(work, Some(12_345), Some(67_890))
        .expect("reset work times");
    dfs.posix_symlink(
        "file.txt",
        link,
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("symlink");
    assert!(
        dfs.posix_stat(work)
            .expect("work after symlink")
            .modified_at_unix_nanos
            > 67_890
    );

    dfs.posix_set_times(work, Some(12_345), Some(67_890))
        .expect("reset work times");
    dfs.posix_mkdir(dir).expect("mkdir child");
    assert!(
        dfs.posix_stat(work)
            .expect("work after mkdir")
            .modified_at_unix_nanos
            > 67_890
    );

    dfs.posix_set_times(work, Some(12_345), Some(67_890))
        .expect("reset work times");
    dfs.posix_rmdir(dir).expect("rmdir child");
    assert!(
        dfs.posix_stat(work)
            .expect("work after rmdir")
            .modified_at_unix_nanos
            > 67_890
    );

    dfs.posix_set_times(work, Some(12_345), Some(67_890))
        .expect("reset work times");
    dfs.posix_unlink(alias).expect("unlink alias");
    assert!(
        dfs.posix_stat(work)
            .expect("work after unlink")
            .modified_at_unix_nanos
            > 67_890
    );

    dfs.posix_set_times(work, Some(12_345), Some(67_890))
        .expect("reset work times");
    dfs.posix_set_times(other, Some(22_222), Some(88_888))
        .expect("old other times");
    dfs.posix_rename(file, renamed).expect("rename across dirs");
    assert!(
        dfs.posix_stat(work)
            .expect("work after rename")
            .modified_at_unix_nanos
            > 67_890
    );
    assert!(
        dfs.posix_stat(other)
            .expect("other after rename")
            .modified_at_unix_nanos
            > 88_888
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_rename_updates_moved_object_ctime_without_changing_mtime() {
    let root = temp_root("sifs-posix-rename-ctime");
    let node_id = NodeId::from_legacy_named("node_sifs_rename_ctime").expect("node");
    let work = "/services/sandbox/instances/sandbox_rename_ctime/work";
    let source = "/services/sandbox/instances/sandbox_rename_ctime/work/source.txt";
    let renamed = "/services/sandbox/instances/sandbox_rename_ctime/work/renamed.txt";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all(work).expect("mkdir work");
    dfs.posix_write_file(
        source,
        b"content",
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("write source");
    dfs.posix_set_times(source, Some(12_345), Some(67_890))
        .expect("old source times");

    let before = dfs.posix_stat(source).expect("before stat");
    dfs.posix_rename(source, renamed).expect("rename");
    let after = dfs.posix_stat(renamed).expect("after stat");

    assert_eq!(after.object_id, before.object_id);
    assert_eq!(after.created_at_unix_nanos, before.created_at_unix_nanos);
    assert_eq!(
        after.modified_at_unix_nanos, before.modified_at_unix_nanos,
        "rename must not change file content mtime"
    );
    assert!(
        after.changed_at_unix_nanos > before.changed_at_unix_nanos,
        "rename must advance ctime because the path metadata changed"
    );
    assert_ne!(
        after.version, before.version,
        "rename metadata update must advance the object metadata version"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_cluster_routes_file_and_directory_sync_certificates() {
    let mut cluster = sifs_posix_cluster("sifs-cluster-routed-posix-sync");
    let path = "/users/synced-file.txt";
    sifs_cluster_routed_posix_write(&mut cluster, path, b"durable bytes");

    let path_for_call = path.to_string();
    let file_certificate = sifs_cluster_writer_call_with_pump(
        &mut cluster,
        "routed fdatasync certificate",
        move |writer| writer.posix_sync_file(&path_for_call, DfsPosixSyncKind::FileData),
    )
    .expect("routed file sync");
    assert_eq!(file_certificate.target.kind, DfsPosixSyncKind::FileData);
    assert!(
        file_certificate
            .metadata_certificate
            .acknowledged_node_ids
            .len()
            >= file_certificate.metadata_certificate.quorum_size
    );
    assert!(file_certificate.content_evidence.required_receipts >= 1);

    let directory_certificate = sifs_cluster_writer_call_with_pump(
        &mut cluster,
        "routed directory fsync certificate",
        move |writer| writer.posix_sync_directory(SIFS_USERS_PATH),
    )
    .expect("routed directory sync");
    assert_eq!(directory_certificate.target.path, SIFS_USERS_PATH);
    assert!(
        directory_certificate
            .metadata_certificate
            .acknowledged_node_ids
            .len()
            >= directory_certificate.metadata_certificate.quorum_size
    );

    assert!(cluster.rejections.is_empty(), "{:?}", cluster.rejections);
    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn sifs_cluster_posix_contract_create_unlink_same_path_race() {
    let mut cluster = sifs_posix_cluster("sifs-contract-create-unlink");
    let scenario = "create-unlink-same-path-race";
    let path = "/users/contract-create-unlink.txt";
    let base = b"base before unlink".to_vec();
    let recreated = b"recreated bytes".to_vec();
    let mut observations = Vec::new();

    sifs_cluster_primary_posix_write(&mut cluster, path, &base);
    let write = cluster.writer.posix_write_file(
        path,
        &recreated,
        sifs_cluster_write_options(&cluster.writer_node.clone()),
    );
    let write_request = match &write {
        Err(DfsPosixError::PendingWrite { intent_id, .. }) => Some(intent_id.clone()),
        _ => None,
    };
    record_posix_observation(
        &mut observations,
        scenario,
        "create-or-overwrite",
        &cluster.writer_node,
        path,
        Instant::now(),
        format!("{write:?}"),
        write.as_ref().ok(),
    );
    let unlink = cluster.primary.posix_unlink(path);
    record_posix_observation(
        &mut observations,
        scenario,
        "unlink",
        &cluster.primary_node,
        path,
        Instant::now(),
        format!("{unlink:?}"),
        unlink.as_ref().ok(),
    );
    sifs_posix_cluster_pump_until(&mut cluster, "create/unlink race settle", |cluster| {
        write_request.as_ref().is_none_or(|intent| {
            !matches!(
                cluster.writer.write_outcome(intent),
                Ok(NetworkedDfsWriteOutcome::Pending { .. })
            )
        })
    });
    let final_stat = cluster.primary.posix_stat(path);
    match final_stat {
        Ok(stat) => assert_eq!(
            stat.content_hash.as_deref(),
            Some(content_hash(&recreated).as_str()),
            "present outcome must be the recreating writer, not a tombstone resurrection"
        ),
        Err(DfsPosixError::NotFound { .. }) => {}
        Err(error) => panic!("unexpected final stat error: {error}"),
    }
    sifs_cluster_publish_metadata_until(
        &mut cluster,
        "create/unlink replica convergence",
        |cluster| {
            matches!(
                cluster.primary.posix_stat(path),
                Err(DfsPosixError::NotFound { .. })
            ) == matches!(
                cluster.observer.posix_stat(path),
                Err(DfsPosixError::NotFound { .. })
            )
        },
    );
    assert_sifs_cluster_no_posix_leaks(&cluster, scenario);
    finish_posix_observations(&observations);
    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn sifs_cluster_posix_contract_same_directory_concurrent_creates() {
    let mut cluster = sifs_posix_cluster("sifs-contract-same-dir-creates");
    let scenario = "same-directory-concurrent-creates";
    let dir = "/users/contract-create-dir";
    let mut observations = Vec::new();
    cluster.primary.posix_mkdir(dir).expect("mkdir parent");
    sifs_cluster_publish_metadata_until(
        &mut cluster,
        "same-directory parent catch-up",
        |cluster| {
            cluster.writer.posix_stat(dir).is_ok() && cluster.observer.posix_stat(dir).is_ok()
        },
    );
    let before = cluster.primary.posix_stat(dir).expect("parent before");

    let mut requests = Vec::new();
    for (index, node, service) in [
        (0, cluster.writer_node.clone(), &mut cluster.writer),
        (1, cluster.observer_node.clone(), &mut cluster.observer),
    ] {
        let path = format!("{dir}/child-{index}.txt");
        let bytes = format!("child payload {index}").into_bytes();
        let result = service.posix_write_file(&path, &bytes, sifs_cluster_write_options(&node));
        if let Err(DfsPosixError::PendingWrite { intent_id, .. }) = &result {
            requests.push((node.clone(), intent_id.clone()));
        }
        record_posix_observation(
            &mut observations,
            scenario,
            format!("create-{index}"),
            &node,
            path,
            Instant::now(),
            format!("{result:?}"),
            result.as_ref().ok(),
        );
    }
    sifs_posix_cluster_pump_until(&mut cluster, "same-directory creates settle", |cluster| {
        let requests_settled = requests.iter().all(|(node, intent)| {
            let service = if node == &cluster.writer_node {
                &cluster.writer
            } else {
                &cluster.observer
            };
            !matches!(
                service.write_outcome(intent),
                Ok(NetworkedDfsWriteOutcome::Pending { .. })
            )
        });
        (0..2).all(|index| {
            cluster
                .primary
                .posix_stat(&format!("{dir}/child-{index}.txt"))
                .is_ok()
        }) && requests_settled
    });
    let after = cluster.primary.posix_stat(dir).expect("parent after");
    assert!(
        after.modified_at_unix_nanos >= before.modified_at_unix_nanos
            && after.changed_at_unix_nanos >= before.changed_at_unix_nanos
            && after.version != before.version,
        "parent directory metadata must be monotonic across concurrent creates; before={before:?}; after={after:?}"
    );
    sifs_cluster_publish_metadata_until(
        &mut cluster,
        "same-directory creates publish",
        |cluster| {
            (0..2).all(|index| {
                cluster
                    .observer
                    .posix_stat(&format!("{dir}/child-{index}.txt"))
                    .is_ok()
            })
        },
    );
    assert_sifs_cluster_no_posix_leaks(&cluster, scenario);
    finish_posix_observations(&observations);
    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn sifs_cluster_posix_contract_same_path_concurrent_creates() {
    let mut cluster = sifs_posix_cluster("sifs-contract-same-path-creates");
    let scenario = "same-path-concurrent-creates";
    let path = "/users/contract-same-path.txt";
    let writer_bytes = b"writer contender".to_vec();
    let observer_bytes = b"observer contender".to_vec();
    let mut observations = Vec::new();
    let writer = cluster.writer.posix_write_file(
        path,
        &writer_bytes,
        sifs_cluster_write_options(&cluster.writer_node.clone()),
    );
    let observer = cluster.observer.posix_write_file(
        path,
        &observer_bytes,
        sifs_cluster_write_options(&cluster.observer_node.clone()),
    );
    let writer_request = match &writer {
        Err(DfsPosixError::PendingWrite { intent_id, .. }) => Some(intent_id.clone()),
        _ => None,
    };
    let observer_request = match &observer {
        Err(DfsPosixError::PendingWrite { intent_id, .. }) => Some(intent_id.clone()),
        _ => None,
    };
    record_posix_observation(
        &mut observations,
        scenario,
        "writer-create",
        &cluster.writer_node,
        path,
        Instant::now(),
        format!("{writer:?}"),
        writer.as_ref().ok(),
    );
    record_posix_observation(
        &mut observations,
        scenario,
        "observer-create",
        &cluster.observer_node,
        path,
        Instant::now(),
        format!("{observer:?}"),
        observer.as_ref().ok(),
    );
    sifs_posix_cluster_pump_until(&mut cluster, "same-path creates settle", |cluster| {
        let writer_settled = writer_request.as_ref().is_none_or(|intent| {
            !matches!(
                cluster.writer.write_outcome(intent),
                Ok(NetworkedDfsWriteOutcome::Pending { .. })
            )
        });
        let observer_settled = observer_request.as_ref().is_none_or(|intent| {
            !matches!(
                cluster.observer.write_outcome(intent),
                Ok(NetworkedDfsWriteOutcome::Pending { .. })
            )
        });
        writer_settled && observer_settled && cluster.primary.posix_stat(path).is_ok()
    });
    let entries = cluster
        .primary
        .posix_list_dir("/users")
        .expect("primary list");
    assert_eq!(
        entries.iter().filter(|entry| entry.path == path).count(),
        1,
        "same-path create race must leave one dentry: {entries:?}"
    );
    let stat = cluster.primary.posix_stat(path).expect("winner stat");
    assert!(
        [content_hash(&writer_bytes), content_hash(&observer_bytes)]
            .contains(stat.content_hash.as_ref().expect("winner hash")),
        "winner content must be one candidate"
    );
    assert_sifs_cluster_no_posix_leaks(&cluster, scenario);
    finish_posix_observations(&observations);
    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn sifs_cluster_posix_contract_rmdir_vs_child_create() {
    let mut cluster = sifs_posix_cluster("sifs-contract-rmdir-create");
    let scenario = "rmdir-vs-child-create";
    let dir = "/users/contract-rmdir";
    let child = "/users/contract-rmdir/child.txt";
    let mut observations = Vec::new();
    cluster.primary.posix_mkdir(dir).expect("mkdir parent");

    let create = cluster.writer.posix_write_file(
        child,
        b"child",
        sifs_cluster_write_options(&cluster.writer_node.clone()),
    );
    record_posix_observation(
        &mut observations,
        scenario,
        "child-create",
        &cluster.writer_node,
        child,
        Instant::now(),
        format!("{create:?}"),
        create.as_ref().ok(),
    );
    let rmdir = cluster.primary.posix_rmdir(dir);
    record_posix_observation(
        &mut observations,
        scenario,
        "rmdir",
        &cluster.primary_node,
        dir,
        Instant::now(),
        format!("{rmdir:?}"),
        rmdir.as_ref().ok(),
    );
    sifs_posix_cluster_pump_until(&mut cluster, "rmdir/create settle", |cluster| {
        cluster.writer.writes.pending_routed_writes.is_empty()
    });
    let dir_present = cluster.primary.posix_stat(dir).is_ok();
    let child_present = cluster.primary.posix_stat(child).is_ok();
    assert!(
        (dir_present && child_present) || (!dir_present && !child_present),
        "must not leave an orphan child under a removed directory"
    );
    if dir_present {
        assert!(matches!(
            cluster.primary.posix_rmdir(dir),
            Err(DfsPosixError::DirectoryNotEmpty { .. })
        ));
    }
    assert_sifs_cluster_no_posix_leaks(&cluster, scenario);
    finish_posix_observations(&observations);
    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn sifs_posix_rmdir_rejects_child_from_fresh_shared_metadata_handle() {
    let root = temp_root("sifs-rmdir-shared-metadata-race");
    let metadata_root = root.join("metadata");
    let node = NodeId::from_legacy_named("node_sifs_rmdir_shared_metadata").expect("node");
    let dir = "/services/shared-rmdir-race";
    let child = "/services/shared-rmdir-race/child.txt";

    let mut seed =
        open_shared_metadata_posix_service(&root, &metadata_root, node.clone(), "content-seed");
    seed.bootstrap_distributed_sifs().expect("bootstrap sifs");
    seed.posix_mkdir(dir).expect("mkdir race dir");

    let mut child_writer =
        open_shared_metadata_posix_service(&root, &metadata_root, node.clone(), "content-child");
    let mut remover =
        open_shared_metadata_posix_service(&root, &metadata_root, node.clone(), "content-rmdir");

    child_writer
        .posix_write_file(
            child,
            b"child",
            DfsWriteOptions::replicated_eventual(vec![node.clone()]),
        )
        .expect("child create commits first");
    assert!(matches!(
        remover.posix_rmdir(dir),
        Err(DfsPosixError::DirectoryNotEmpty { .. })
    ));

    let verifier =
        open_shared_metadata_posix_service(&root, &metadata_root, node, "content-verify");
    assert!(verifier.posix_stat(dir).is_ok());
    assert!(verifier.posix_stat(child).is_ok());
}

#[test]
fn sifs_posix_child_create_rejects_parent_removed_by_fresh_shared_metadata_handle() {
    let root = temp_root("sifs-child-create-shared-metadata-race");
    let metadata_root = root.join("metadata");
    let node = NodeId::from_legacy_named("node_sifs_child_shared_metadata").expect("node");
    let dir = "/services/shared-child-race";
    let child = "/services/shared-child-race/child.txt";

    let mut seed =
        open_shared_metadata_posix_service(&root, &metadata_root, node.clone(), "content-seed");
    seed.bootstrap_distributed_sifs().expect("bootstrap sifs");
    seed.posix_mkdir(dir).expect("mkdir race dir");

    let mut child_writer =
        open_shared_metadata_posix_service(&root, &metadata_root, node.clone(), "content-child");
    let mut remover =
        open_shared_metadata_posix_service(&root, &metadata_root, node.clone(), "content-rmdir");

    let live_changed_at = remover
        .posix_lstat(dir)
        .expect("live directory stat")
        .changed_at_unix_nanos;
    let removed = remover.posix_rmdir(dir).expect("rmdir commits first");
    assert!(
        removed.changed_at_unix_nanos > live_changed_at,
        "rmdir tombstone ctime must fence historical namespace transactions"
    );
    assert!(matches!(
        child_writer.posix_write_file(
            child,
            b"child",
            DfsWriteOptions::replicated_eventual(vec![node.clone()]),
        ),
        Err(DfsPosixError::MissingParent { .. })
            | Err(DfsPosixError::NotFound { .. })
            | Err(DfsPosixError::Dfs(_))
    ));

    let verifier =
        open_shared_metadata_posix_service(&root, &metadata_root, node, "content-verify");
    let verifier_dir = verifier.posix_stat(dir);
    assert!(
        matches!(verifier_dir, Err(DfsPosixError::NotFound { .. })),
        "removed parent resurrected after rejected child create: {verifier_dir:?}"
    );
    assert!(matches!(
        verifier.posix_stat(child),
        Err(DfsPosixError::NotFound { .. })
    ));
}

#[test]
fn sifs_cluster_posix_contract_directory_rename_while_child_mutations_run() {
    let cluster = sifs_two_primary_cluster("sifs-contract-dir-rename-child");
    let scenario = "directory-rename-while-child-mutations-run";
    let SifsTwoPrimaryCluster {
        root,
        replica_node,
        users_primary,
        services_primary,
        mut replica,
        ..
    } = cluster;
    let mut observations = Vec::new();
    let stop = Arc::new(AtomicBool::new(false));
    let users_stop = Arc::clone(&stop);
    let users_thread =
        std::thread::spawn(move || pump_sifs_service_until_stop(users_primary, users_stop));
    let services_stop = Arc::clone(&stop);
    let services_thread =
        std::thread::spawn(move || pump_sifs_service_until_stop(services_primary, services_stop));

    replica.posix_mkdir("/users/contract-proj").expect("mkdir");
    replica
        .posix_write_file(
            "/users/contract-proj/before.txt",
            b"before",
            DfsWriteOptions::replicated_eventual(vec![replica_node.clone()]),
        )
        .expect("before child");
    let child_create = replica.posix_write_file(
        "/users/contract-proj/during.txt",
        b"during",
        DfsWriteOptions::replicated_eventual(vec![replica_node.clone()]),
    );
    record_posix_observation(
        &mut observations,
        scenario,
        "child-create",
        &replica_node,
        "/users/contract-proj/during.txt",
        Instant::now(),
        format!("{child_create:?}"),
        child_create.as_ref().ok(),
    );
    let rename = replica.posix_rename("/users/contract-proj", "/services/contract-proj");
    record_posix_observation(
        &mut observations,
        scenario,
        "dir-rename",
        &replica_node,
        "/services/contract-proj",
        Instant::now(),
        format!("{rename:?}"),
        rename.as_ref().ok(),
    );
    assert!(
        rename.is_ok(),
        "directory rename should serialize with child mutation: {rename:?}"
    );
    assert!(matches!(
        replica.posix_stat("/users/contract-proj"),
        Err(DfsPosixError::NotFound { .. })
    ));
    let old_child = replica.posix_stat("/users/contract-proj/during.txt");
    let new_child = replica.posix_stat("/services/contract-proj/during.txt");
    assert!(
        old_child.is_ok() ^ new_child.is_ok(),
        "child mutation must resolve under exactly one subtree"
    );

    stop.store(true, Ordering::SeqCst);
    let users_primary = users_thread.join().expect("users thread");
    let services_primary = services_thread.join().expect("services thread");
    let mut cluster = SifsTwoPrimaryCluster {
        root,
        users_primary_node: NodeId::from_legacy_named("node_contract_dir_users").expect("node"),
        services_primary_node: NodeId::from_legacy_named("node_contract_dir_services")
            .expect("node"),
        replica_node,
        shared_kv: SharedDfsControlPlaneKvClient::new(InProcessKvStore::new()),
        users_primary,
        services_primary,
        replica,
        rejections: Vec::new(),
    };
    sifs_two_primary_publish_until(&mut cluster, "dir rename child convergence", |cluster| {
        cluster
            .replica
            .posix_stat("/services/contract-proj")
            .is_ok()
            && matches!(
                cluster.replica.posix_stat("/users/contract-proj"),
                Err(DfsPosixError::NotFound { .. })
            )
    });
    assert_two_primary_no_posix_leaks(&cluster, scenario);
    finish_posix_observations(&observations);
    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn sifs_cluster_posix_contract_paged_list_while_directory_mutates() {
    let mut cluster = sifs_posix_cluster("sifs-contract-paged-list");
    let scenario = "paged-list-while-directory-mutates";
    let dir = "/users/contract-paged";
    let mut observations = Vec::new();
    cluster.primary.posix_mkdir(dir).expect("mkdir");
    for index in 0..20 {
        sifs_cluster_primary_posix_write(
            &mut cluster,
            &format!("{dir}/file-{index:02}.txt"),
            format!("payload {index}").as_bytes(),
        );
    }
    let first = cluster
        .primary
        .posix_list_dir_page(dir, None, 5)
        .expect("first page");
    record_posix_observation(
        &mut observations,
        scenario,
        "list-page-0",
        &cluster.primary_node,
        dir,
        Instant::now(),
        format!("entries={}", first.entries.len()),
        None,
    );
    cluster
        .primary
        .posix_unlink(&format!("{dir}/file-10.txt"))
        .expect("unlink during page walk");
    sifs_cluster_primary_posix_write(&mut cluster, &format!("{dir}/file-20.txt"), b"new");

    let mut cursor = first.next_cursor.clone();
    let mut seen = BTreeSet::new();
    for entry in first.entries {
        assert!(seen.insert(entry.path), "duplicate entry in first page");
    }
    while let Some(after) = cursor {
        let page = cluster
            .primary
            .posix_list_dir_page(dir, Some(&after), 5)
            .expect("next page");
        for entry in &page.entries {
            assert!(
                seen.insert(entry.path.clone()),
                "paged list returned duplicate entry: {entry:?}"
            );
            assert_ne!(
                entry.path,
                format!("{dir}/file-10.txt"),
                "paged list must not return tombstoned children as live"
            );
        }
        cursor = page.next_cursor;
    }
    let final_entries = cluster.primary.posix_list_dir(dir).expect("final list");
    assert!(
        final_entries
            .iter()
            .any(|entry| entry.path == format!("{dir}/file-20.txt"))
            && !final_entries
                .iter()
                .any(|entry| entry.path == format!("{dir}/file-10.txt")),
        "final list must include committed creates and exclude tombstones: {final_entries:?}"
    );
    assert_sifs_cluster_no_posix_leaks(&cluster, scenario);
    finish_posix_observations(&observations);
    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn lazy_service_rebuild_preserves_shared_namespace_guard_registry() {
    let root = temp_root("lazy-service-shared-namespace-guards");
    let node = NodeId::from_legacy_named("node_lazy_shared_namespace_guards").expect("node");
    let file_system_id = sifs_file_system_id();
    let registry = DfsCrossShardNamespaceLockRegistry::default();
    let mut service = NetworkedDfsService::new(
        InProcessNetwork::new().add_node(node),
        LocalContentStore::new(root.join("content")).expect("content"),
    )
    .with_cross_shard_namespace_lock_registry(registry.clone());
    service.next_frame_id = 41;
    service.next_message_id = 42;
    service.next_request_id = 43;
    service.repair_retry_tick = 44;
    let replica = NodeId::from_legacy_named("node_lazy_shared_replica").expect("replica");
    service
        .replication
        .metadata_replication_cursors
        .insert((replica.clone(), DfsMetadataShardId::new("services")), 45);
    let committed_work_tracker = DfsCommittedWorkTracker::new();
    service.set_committed_work_tracker(committed_work_tracker.clone());
    let cancellation = DfsCancellationFlag::new();
    service.set_cancellation(Some(cancellation.clone()));

    let mut rebuilt = service
        .into_lazy_metadata_backend_service()
        .expect("lazy service rebuild");
    assert!(
        rebuilt
            .cross_shard_namespace_locks
            .shares_state_with(&registry),
        "disaster-recovery collapse must retain the generation's shared registry"
    );
    assert_eq!(rebuilt.next_frame_id, 41);
    assert_eq!(rebuilt.next_message_id, 42);
    assert_eq!(rebuilt.next_request_id, 43);
    assert_eq!(rebuilt.repair_retry_tick, 44);
    assert_eq!(
        rebuilt
            .replication
            .metadata_replication_cursors
            .get(&(replica, DfsMetadataShardId::new("services"),)),
        Some(&45)
    );
    rebuilt.record_message_committed_work(DfsMessageCommittedWork {
        metadata_replica_progress: true,
        ..DfsMessageCommittedWork::default()
    });
    assert!(
        committed_work_tracker.take().metadata_replica_progress,
        "a rebuilt service must retain the caller-owned committed-work edge"
    );
    cancellation.cancel();
    assert!(matches!(
        rebuilt.check_cancelled("lazy rebuild test"),
        Err(DfsError::Cancelled { .. })
    ));
    registry.upsert_job(
        None,
        "post-rebuild-job".to_string(),
        file_system_id.clone(),
        "/services/post-rebuild".to_string(),
    );
    assert!(matches!(
        rebuilt
            .ensure_path_not_in_cross_shard_flight(&file_system_id, "/services/post-rebuild/child"),
        Err(DfsError::Busy { .. })
    ));
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn exact_namespace_projection_collapse_keeps_only_shard_runtime_state() {
    let root = temp_root("exact-namespace-projection-collapse");
    let node = NodeId::from_legacy_named("node_exact_namespace_collapse").expect("node");
    let file_system_id = sifs_file_system_id();
    let shard_id = DfsMetadataShardId::new("root");
    let resident = LocalShardedMetadataBackend::new(
        root.join("metadata"),
        vec![DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: shard_id.0.clone(),
            primary_node_id: node.clone(),
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        }],
    )
    .expect("sharded metadata")
    .into_resident();
    let registry = DfsCrossShardNamespaceLockRegistry::default();
    let mut service = NetworkedDfsService::new(
        InProcessNetwork::new().add_node(node.clone()),
        LocalContentStore::new(root.join("content")).expect("content"),
    )
    .with_cross_shard_namespace_lock_registry(registry.clone())
    .with_resident_metadata_shard_backend_lazy(
        resident
            .scope_shard(&file_system_id, &shard_id)
            .expect("exact shard")
            .checkout()
            .expect("exact checkout"),
    )
    .expect("lazy exact service");
    registry.upsert_job(
        Some(shard_id.clone()),
        "collapse-running-job".to_string(),
        file_system_id.clone(),
        "/services/collapse-running".to_string(),
    );
    let mut object = file_metadata(node);
    object.file_system_id = file_system_id.clone();
    object.path = "/services/collapse-object".to_string();
    service
        .metadata
        .add_object(object.clone())
        .expect("cached object");
    service.coordinator.set_committed_version(
        file_system_id.clone(),
        object.object_id.clone(),
        object.version.clone(),
    );
    service
        .replication
        .local_metadata_journal_cursors
        .insert(shard_id.clone(), 91);
    service.next_request_id = 92;

    service
        .collapse_exact_namespace_request_projection()
        .expect("collapse exact namespace projection");

    assert!(service.metadata().object(&object.object_id).is_none());
    assert_eq!(service.coordinator().committed_version_count(), 0);
    assert!(service.metadata.filesystems.contains_key(&file_system_id));
    assert_eq!(
        service
            .replication
            .local_metadata_journal_cursors
            .get(&shard_id),
        Some(&91)
    );
    assert_eq!(service.next_request_id, 92);
    assert!(registry.blocks(&file_system_id, "/services/collapse-running/child"));
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_cluster_posix_concurrent_creates_serialize_with_explicit_conflict() {
    let mut cluster = sifs_posix_cluster("sifs-cluster-race");
    let path = "/users/raced.txt";
    let writer_bytes = b"writer candidate content".to_vec();
    let observer_bytes = b"observer candidate bytes!".to_vec();
    assert_ne!(content_hash(&writer_bytes), content_hash(&observer_bytes));

    // Both replicas race to create the same path before any frames are pumped.
    let writer_options = sifs_cluster_write_options(&cluster.writer_node.clone());
    let writer_request = match cluster
        .writer
        .posix_write_file(path, &writer_bytes, writer_options)
    {
        Err(DfsPosixError::PendingWrite { intent_id, .. }) => intent_id,
        other => panic!("expected pending routed write from writer, got {other:?}"),
    };
    let observer_options = sifs_cluster_write_options(&cluster.observer_node.clone());
    let observer_request =
        match cluster
            .observer
            .posix_write_file(path, &observer_bytes, observer_options)
        {
            Err(DfsPosixError::PendingWrite { intent_id, .. }) => intent_id,
            other => panic!("expected pending routed write from observer, got {other:?}"),
        };

    sifs_posix_cluster_pump_until(&mut cluster, "racing writes settle", |cluster| {
        let writer_settled = !matches!(
            cluster.writer.write_outcome(&writer_request),
            Ok(NetworkedDfsWriteOutcome::Pending { .. })
        );
        let observer_settled = !matches!(
            cluster.observer.write_outcome(&observer_request),
            Ok(NetworkedDfsWriteOutcome::Pending { .. })
        );
        writer_settled && observer_settled
    });

    let writer_outcome = cluster.writer.write_outcome(&writer_request);
    let observer_outcome = cluster.observer.write_outcome(&observer_request);
    let mut committed_hashes = Vec::new();
    let mut surfaced_failures = 0usize;
    for outcome in [&writer_outcome, &observer_outcome] {
        match outcome {
            Ok(NetworkedDfsWriteOutcome::Committed { content_hash, .. }) => {
                committed_hashes.push(content_hash.clone());
            }
            Err(_) => {
                surfaced_failures += 1;
            }
            Ok(NetworkedDfsWriteOutcome::Conflict { .. }) => {
                panic!("racing create created a DFS conflict during normal operation")
            }
            Ok(NetworkedDfsWriteOutcome::Pending { .. }) => {
                panic!("racing write left pending after settle")
            }
        }
    }
    assert_eq!(
        (committed_hashes.len(), surfaced_failures),
        (1, 1),
        "exactly one racing create must commit and the other must reject without a conflict; \
         writer: {writer_outcome:?}, observer: {observer_outcome:?}"
    );
    let winner_hash = committed_hashes.remove(0);
    assert!(
        winner_hash == content_hash(&writer_bytes) || winner_hash == content_hash(&observer_bytes),
        "winning content must be exactly one candidate, never interleaved"
    );

    // The primary's visible metadata must reference the winner's bytes.
    let primary_stat = cluster.primary.posix_stat(path).expect("primary stat");
    assert_eq!(
        primary_stat.content_hash.as_deref(),
        Some(winner_hash.as_str())
    );
    let winner_len = if winner_hash == content_hash(&writer_bytes) {
        writer_bytes.len()
    } else {
        observer_bytes.len()
    };
    assert_eq!(primary_stat.size_bytes, Some(winner_len as u64));

    // Reads after a rejected racing write return the winner or report missing
    // content while repair catches up; they must never surface a normal DFS
    // conflict or return the losing candidate.
    match cluster.primary.posix_read_file_distributed(path) {
        Ok(bytes) => {
            let read_hash = content_hash(&bytes);
            assert_eq!(
                read_hash, winner_hash,
                "read after race must observe the winning content only"
            );
        }
        Err(DfsPosixError::Conflict { .. }) => {
            panic!("read after racing create surfaced a DFS conflict during normal operation")
        }
        Err(DfsPosixError::NotFound { .. }) => {
            // Content may still live only on the winning replica; metadata
            // remains authoritative for the assertion above.
        }
        Err(error) => panic!("unexpected read failure after race: {error}"),
    }

    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn sifs_cluster_posix_truncate_shrinks_and_extends_with_zeros() {
    let mut cluster = sifs_posix_cluster("sifs-cluster-truncate");
    let path = "/users/truncate.bin";
    let base = b"truncate source content".to_vec();

    sifs_cluster_routed_posix_write(&mut cluster, path, &base);

    // Shrink.
    let shrunk = base[..8].to_vec();
    let shrunk_hash = content_hash(&shrunk);
    let options = sifs_cluster_write_options(&cluster.writer_node.clone());
    match cluster.writer.posix_truncate_file(path, 8, options) {
        Ok(_) | Err(DfsPosixError::PendingWrite { .. }) => {}
        Err(error) => panic!("routed truncate failed: {error}"),
    }
    sifs_posix_cluster_pump_until(&mut cluster, "truncate shrink commit", |cluster| {
        cluster
            .writer
            .posix_stat(path)
            .is_ok_and(|stat| stat.content_hash.as_deref() == Some(shrunk_hash.as_str()))
    });
    assert_eq!(
        cluster.writer.posix_read_file(path).expect("shrunk read"),
        shrunk
    );
    assert_eq!(
        cluster
            .writer
            .posix_stat(path)
            .expect("shrunk stat")
            .size_bytes,
        Some(8)
    );

    // Extend: the new tail must be zero filled.
    let mut extended = shrunk.clone();
    extended.resize(12, 0);
    let extended_hash = content_hash(&extended);
    let options = sifs_cluster_write_options(&cluster.writer_node.clone());
    match cluster.writer.posix_truncate_file(path, 12, options) {
        Ok(_) | Err(DfsPosixError::PendingWrite { .. }) => {}
        Err(error) => panic!("routed truncate extension failed: {error}"),
    }
    sifs_posix_cluster_pump_until(&mut cluster, "truncate extend commit", |cluster| {
        cluster
            .writer
            .posix_stat(path)
            .is_ok_and(|stat| stat.content_hash.as_deref() == Some(extended_hash.as_str()))
    });
    assert_eq!(
        cluster.writer.posix_read_file(path).expect("extended read"),
        extended
    );

    // Primary sees the final truncated content.
    let read = sifs_cluster_fetch_for_local_read(&mut cluster, |c| &mut c.primary, path);
    assert_eq!(read, extended);

    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn sifs_cluster_posix_rename_replicates_atomically_to_observers() {
    let mut cluster = sifs_posix_cluster("sifs-cluster-rename");
    let source = "/users/rename-source.txt";
    let destination = "/users/rename-destination.txt";
    let source_bytes = b"bytes that travel with the rename".to_vec();
    let destination_bytes = b"bytes that are replaced".to_vec();
    let source_hash = content_hash(&source_bytes);

    // Namespace mutations are authoritative on the shard primary.
    sifs_cluster_primary_posix_write(&mut cluster, source, &source_bytes);
    sifs_cluster_primary_posix_write(&mut cluster, destination, &destination_bytes);

    // Ship the pre-rename namespace to the observer first so the rename
    // replay has to move/replace existing replica records rather than start
    // from an empty namespace.
    sifs_cluster_publish_metadata_until(&mut cluster, "pre-rename catch-up", |cluster| {
        cluster.observer.posix_stat(source).is_ok()
            && cluster.observer.posix_stat(destination).is_ok()
    });

    let renamed_stat = cluster
        .primary
        .posix_rename(source, destination)
        .expect("primary rename over existing destination");
    assert_eq!(
        renamed_stat.content_hash.as_deref(),
        Some(source_hash.as_str())
    );

    // Rename must be atomic on the primary: destination has the source
    // content, the source path is gone.
    assert_eq!(
        cluster
            .primary
            .posix_stat(destination)
            .expect("primary destination stat")
            .content_hash
            .as_deref(),
        Some(source_hash.as_str())
    );
    assert!(matches!(
        cluster.primary.posix_stat(source),
        Err(DfsPosixError::NotFound { .. })
    ));

    // Replicas converge to the same post-rename namespace.
    sifs_cluster_publish_metadata_until(&mut cluster, "post-rename catch-up", |cluster| {
        let destination_current = cluster
            .observer
            .posix_stat(destination)
            .is_ok_and(|stat| stat.content_hash.as_deref() == Some(source_hash.as_str()));
        let source_gone = matches!(
            cluster.observer.posix_stat(source),
            Err(DfsPosixError::NotFound { .. })
        );
        destination_current && source_gone
    });
    let entries = cluster
        .observer
        .posix_list_dir("/users")
        .expect("observer list after rename");
    assert!(
        entries.iter().any(|entry| entry.path == destination),
        "observer must list the rename destination: {entries:?}"
    );
    assert!(
        !entries.iter().any(|entry| entry.path == source),
        "observer must not list the rename source after replay: {entries:?}"
    );

    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn sifs_cluster_posix_directory_rename_moves_children_on_observers() {
    let mut cluster = sifs_posix_cluster("sifs-cluster-dir-rename");
    let child_bytes = b"child file inside renamed directory".to_vec();
    let child_hash = content_hash(&child_bytes);

    cluster
        .primary
        .posix_mkdir("/users/project-a")
        .expect("primary mkdir");
    sifs_cluster_primary_posix_write(&mut cluster, "/users/project-a/notes.txt", &child_bytes);

    sifs_cluster_publish_metadata_until(&mut cluster, "pre-dir-rename catch-up", |cluster| {
        cluster
            .observer
            .posix_stat("/users/project-a/notes.txt")
            .is_ok()
    });

    cluster
        .primary
        .posix_rename("/users/project-a", "/users/project-b")
        .expect("primary directory rename");
    assert_eq!(
        cluster
            .primary
            .posix_stat("/users/project-b/notes.txt")
            .expect("primary child stat")
            .content_hash
            .as_deref(),
        Some(child_hash.as_str())
    );
    assert!(matches!(
        cluster.primary.posix_stat("/users/project-a"),
        Err(DfsPosixError::NotFound { .. })
    ));

    sifs_cluster_publish_metadata_until(&mut cluster, "post-dir-rename catch-up", |cluster| {
        let child_moved = cluster
            .observer
            .posix_stat("/users/project-b/notes.txt")
            .is_ok_and(|stat| stat.content_hash.as_deref() == Some(child_hash.as_str()));
        let old_dir_gone = matches!(
            cluster.observer.posix_stat("/users/project-a"),
            Err(DfsPosixError::NotFound { .. })
        );
        child_moved && old_dir_gone
    });
    assert!(matches!(
        cluster.observer.posix_stat("/users/project-a/notes.txt"),
        Err(DfsPosixError::NotFound { .. })
    ));
    let stat = cluster
        .observer
        .posix_stat("/users/project-b")
        .expect("observer renamed dir stat");
    assert_eq!(stat.kind, DfsObjectKind::Directory);

    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn sifs_cluster_posix_namespace_and_stats_agree_across_nodes() {
    let mut cluster = sifs_posix_cluster("sifs-cluster-agree");
    let dir = "/users/team";
    let file_a = "/users/team/a.txt";
    let file_b = "/users/team/b.txt";
    let link = "/users/team/link";
    let a_bytes = b"alpha file content".to_vec();
    let b_bytes = b"bravo".to_vec();

    cluster.primary.posix_mkdir(dir).expect("primary mkdir");
    sifs_cluster_primary_posix_write(&mut cluster, file_a, &a_bytes);
    sifs_cluster_primary_posix_write(&mut cluster, file_b, &b_bytes);
    cluster
        .primary
        .posix_symlink(
            "a.txt",
            link,
            sifs_cluster_write_options(&cluster.primary_node.clone()),
        )
        .expect("primary symlink");

    sifs_cluster_publish_metadata_until(&mut cluster, "namespace agreement", |cluster| {
        cluster.observer.posix_lstat(link).is_ok()
            && cluster.writer.posix_lstat(link).is_ok()
            && cluster.observer.posix_stat(file_b).is_ok()
            && cluster.writer.posix_stat(file_b).is_ok()
    });

    let primary_entries = cluster.primary.posix_list_dir(dir).expect("primary list");
    let writer_entries = cluster.writer.posix_list_dir(dir).expect("writer list");
    let observer_entries = cluster.observer.posix_list_dir(dir).expect("observer list");
    let names = |entries: &[DfsDirEntry]| {
        entries
            .iter()
            .map(|entry| {
                (
                    entry.name.clone(),
                    entry.kind.clone(),
                    entry.object_id.clone(),
                )
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(names(&primary_entries), names(&writer_entries));
    assert_eq!(names(&primary_entries), names(&observer_entries));

    for path in [file_a, file_b] {
        let primary_stat = cluster.primary.posix_stat(path).expect("primary stat");
        let listed = primary_entries
            .iter()
            .find(|entry| entry.path == path)
            .expect("file appears in directory page");
        assert_eq!(
            listed.version, primary_stat.version,
            "listed version {path}"
        );
        assert_eq!(
            listed.size_bytes, primary_stat.size_bytes,
            "listed size {path}"
        );
        assert_eq!(
            listed.modified_at_unix_nanos, primary_stat.modified_at_unix_nanos,
            "listed modified time {path}"
        );
        assert_eq!(listed.policy, primary_stat.policy, "listed policy {path}");
        assert_eq!(
            listed.current_locations, primary_stat.current_locations,
            "listed placement {path}"
        );
        for (label, stat) in [
            (
                "writer",
                cluster.writer.posix_stat(path).expect("writer stat"),
            ),
            (
                "observer",
                cluster.observer.posix_stat(path).expect("observer stat"),
            ),
        ] {
            assert_eq!(stat.object_id, primary_stat.object_id, "{label} {path}");
            assert_eq!(stat.kind, primary_stat.kind, "{label} {path}");
            assert_eq!(stat.size_bytes, primary_stat.size_bytes, "{label} {path}");
            assert_eq!(
                stat.content_hash, primary_stat.content_hash,
                "{label} {path}"
            );
            assert_eq!(stat.version, primary_stat.version, "{label} {path}");
        }
    }

    // lstat reports the symlink itself without needing its content.
    let observer_lstat = cluster.observer.posix_lstat(link).expect("observer lstat");
    assert_eq!(observer_lstat.kind, DfsObjectKind::Symlink);

    // Following the symlink requires its content (the target string); fetch
    // it to the observer before follow-the-link assertions.
    cluster
        .observer
        .posix_request_remote_read_no_follow(link)
        .expect("observer symlink remote read");
    sifs_posix_cluster_pump_until(&mut cluster, "symlink content fetch", |cluster| {
        cluster.observer.posix_readlink(link).is_ok()
    });
    assert_eq!(
        cluster.observer.posix_readlink(link).expect("readlink"),
        "a.txt"
    );

    // With the link content local, stat follows it to the target file.
    let observer_stat = cluster.observer.posix_stat(link).expect("observer stat");
    assert_eq!(observer_stat.kind, DfsObjectKind::File);
    assert_eq!(observer_stat.size_bytes, Some(a_bytes.len() as u64));

    // Following the symlink resolves to the same bytes on every node.
    let through_link = sifs_cluster_fetch_for_local_read(&mut cluster, |c| &mut c.observer, link);
    assert_eq!(through_link, a_bytes);

    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn sifs_cluster_posix_error_mapping_matches_posix_contract() {
    let mut cluster = sifs_posix_cluster("sifs-cluster-errno");

    // EEXIST: mkdir over an existing directory.
    cluster
        .primary
        .posix_mkdir("/users/errno-dir")
        .expect("primary mkdir");
    assert!(matches!(
        cluster.primary.posix_mkdir("/users/errno-dir"),
        Err(DfsPosixError::AlreadyExists { .. })
    ));

    // EISDIR: writing file bytes over a directory, unlinking a directory.
    assert!(matches!(
        cluster.primary.posix_write_file(
            "/users/errno-dir",
            b"nope",
            sifs_cluster_write_options(&cluster.primary_node.clone()),
        ),
        Err(DfsPosixError::IsDirectory { .. })
    ));
    assert!(matches!(
        cluster.primary.posix_unlink("/users/errno-dir"),
        Err(DfsPosixError::IsDirectory { .. })
    ));

    // ENOTDIR: rmdir on a file; trailing slash on a file path.
    sifs_cluster_primary_posix_write(&mut cluster, "/users/errno-file.txt", b"errno bytes");
    assert!(matches!(
        cluster.primary.posix_rmdir("/users/errno-file.txt"),
        Err(DfsPosixError::NotDirectory { .. })
    ));
    assert!(matches!(
        cluster.primary.posix_stat("/users/errno-file.txt/"),
        Err(DfsPosixError::NotDirectory { .. })
    ));

    // ENOTEMPTY: rmdir on a non-empty directory.
    sifs_cluster_primary_posix_write(&mut cluster, "/users/errno-dir/child.txt", b"child");
    assert!(matches!(
        cluster.primary.posix_rmdir("/users/errno-dir"),
        Err(DfsPosixError::DirectoryNotEmpty { .. })
    ));

    // ENOENT: reading/statting a missing path; writing under a missing parent.
    assert!(matches!(
        cluster.primary.posix_read_file("/users/errno-missing.txt"),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert!(matches!(
        cluster.primary.posix_write_file(
            "/users/errno-no-parent/file.txt",
            b"nope",
            sifs_cluster_write_options(&cluster.primary_node.clone()),
        ),
        Err(DfsPosixError::MissingParent { .. })
    ));

    // EINVAL: renaming a directory into its own subtree; relative paths.
    assert!(matches!(
        cluster
            .primary
            .posix_rename("/users/errno-dir", "/users/errno-dir/inner"),
        Err(DfsPosixError::InvalidPath { .. })
    ));
    assert!(matches!(
        cluster.primary.posix_stat("relative/path.txt"),
        Err(DfsPosixError::InvalidPath { .. })
    ));

    // The same contract holds on a non-primary node: failed validation must
    // not leave routed-write state behind.
    assert!(matches!(
        cluster.writer.posix_write_file(
            "/users/errno-no-parent/file.txt",
            b"nope",
            sifs_cluster_write_options(&cluster.writer_node.clone()),
        ),
        Err(DfsPosixError::MissingParent { .. })
    ));
    assert!(
        cluster.writer.writes.pending_routed_writes.is_empty(),
        "failed POSIX validation on a replica must not leak routed-write state"
    );

    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn clustered_mkdir_all_fuses_request_admission_with_prepare() {
    let cluster = sifs_two_primary_cluster("sifs-mkdir-all-fused-admission");
    let SifsTwoPrimaryCluster {
        root,
        replica_node,
        mut services_primary,
        replica,
        ..
    } = cluster;
    let path = "/services/fused-admission/child";
    let request_id = "dfs_net_req_mkdir_all_fused_admission".to_string();
    let command = DfsNamespaceCommand {
        request_id: request_id.clone(),
        file_system_id: sifs_file_system_id(),
        requesting_node_id: replica_node.clone(),
        include_metadata_journal_batches: false,
        metadata_journal_after_sequences: Vec::new(),
        authority_shards: Vec::new(),
        operation: DfsNamespaceOperation::MkdirAll {
            path: path.to_string(),
        },
    };

    crate::metadata_backend::reset_metadata_journal_write_count();
    let event = services_primary
        .handle_namespace_request(replica_node.clone(), command)
        .expect("serve fused-admission mkdir-all");
    assert!(
        matches!(
            &event,
            NetworkedDfsServiceEvent::RemoteNamespaceServed {
                result: DfsNamespaceResult::Applied,
                ..
            }
        ),
        "unexpected fused-admission event: {event:?}"
    );
    assert_eq!(
        crate::metadata_backend::metadata_journal_write_count(),
        2,
        "the common clustered mkdir-all path should write prepare + commit, not Pending + prepare + commit"
    );
    let completed = services_primary
        .metadata_backend
        .as_ref()
        .expect("metadata backend")
        .get_namespace_request(&replica_node, &request_id)
        .expect("request lookup")
        .expect("completed request");
    assert_eq!(completed.status, DfsNamespaceRequestStatus::Completed);
    assert_eq!(completed.result, Some(DfsNamespaceResult::Applied));
    assert_eq!(
        services_primary
            .posix_stat(path)
            .expect("created directory")
            .kind,
        DfsObjectKind::Directory
    );

    let no_op_request_id = "dfs_net_req_mkdir_all_fused_admission_noop".to_string();
    crate::metadata_backend::reset_metadata_journal_write_count();
    let no_op = services_primary
        .handle_namespace_request(
            replica_node.clone(),
            DfsNamespaceCommand {
                request_id: no_op_request_id.clone(),
                file_system_id: sifs_file_system_id(),
                requesting_node_id: replica_node.clone(),
                include_metadata_journal_batches: false,
                metadata_journal_after_sequences: Vec::new(),
                authority_shards: Vec::new(),
                operation: DfsNamespaceOperation::MkdirAll {
                    path: path.to_string(),
                },
            },
        )
        .expect("serve already-satisfied mkdir-all");
    assert!(
        matches!(
            &no_op,
            NetworkedDfsServiceEvent::RemoteNamespaceServed {
                result: DfsNamespaceResult::Applied,
                ..
            }
        ),
        "unexpected fused-admission no-op event: {no_op:?}"
    );
    assert_eq!(
        crate::metadata_backend::metadata_journal_write_count(),
        1,
        "an already-satisfied mkdir-all should record Completed directly"
    );
    assert_eq!(
        services_primary
            .metadata_backend
            .as_ref()
            .expect("metadata backend")
            .get_namespace_request(&replica_node, &no_op_request_id)
            .expect("no-op request lookup")
            .expect("completed no-op request")
            .status,
        DfsNamespaceRequestStatus::Completed
    );

    drop(replica);
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn compound_posix_routing_refresh_invalidates_prior_config_revision() {
    let root = temp_root("compound-posix-routing-refreshes-config-revision");
    let users_primary =
        NodeId::from_legacy_named("node_posix_snapshot_users").expect("users primary");
    let old_services_primary =
        NodeId::from_legacy_named("node_posix_snapshot_services_old").expect("old primary");
    let new_services_primary =
        NodeId::from_legacy_named("node_posix_snapshot_services_new").expect("new primary");
    let requester = NodeId::from_legacy_named("node_posix_snapshot_requester").expect("requester");
    let shards = sifs_two_primary_shard_configs(
        &users_primary,
        &old_services_primary,
        1,
        vec![requester.clone()],
        vec![requester.clone()],
    );
    let shared_kv = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    shared_kv.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        for shard in &shards {
            control.put_metadata_shard(shard).expect("metadata shard");
        }
    });

    let range_prefix_calls = Arc::new(AtomicUsize::new(0));
    let mut dfs = NetworkedDfsService::new(
        crate::test_runtime::DfsTestRuntime::local_transport(requester),
        LocalContentStore::new(root.join("content")).expect("content"),
    )
    .with_control_plane(CountingSharedDfsControlPlaneKvClient::new(
        shared_kv.clone(),
        Arc::clone(&range_prefix_calls),
    ));

    let initial = dfs
        .routed_namespace_primaries_for_paths_fresh([
            "/users/source.txt",
            "/services/destination.txt",
        ])
        .expect("initial compound routing");
    assert_eq!(
        initial,
        vec![
            Some(users_primary.clone()),
            Some(old_services_primary.clone())
        ]
    );
    assert_eq!(range_prefix_calls.load(Ordering::SeqCst), 1);

    let mut updated_services = shards
        .iter()
        .find(|shard| shard.shard_id == "services")
        .expect("services shard")
        .clone();
    updated_services.primary_node_id = new_services_primary.clone();
    updated_services.epoch = 2;
    shared_kv.with_client(|kv| {
        DfsControlPlaneStore::new(kv)
            .put_metadata_shard(&updated_services)
            .expect("advance services shard config revision");
    });

    dfs.refresh_control_plane_latest()
        .expect("refresh control-plane revision");
    let refreshed = dfs
        .routed_namespace_primaries_for_paths_fresh([
            "/users/source.txt",
            "/services/destination.txt",
        ])
        .expect("refreshed compound routing");
    assert_eq!(
        refreshed,
        vec![Some(users_primary), Some(new_services_primary)]
    );
    assert_eq!(
        range_prefix_calls.load(Ordering::SeqCst),
        2,
        "each compound decision may load one snapshot, including after explicit revision refresh"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_non_replica_directory_pull_captures_durable_replica_progress() {
    let cluster = sifs_outsider_cluster("sifs-outsider-directory-pull-capture");
    let SifsPosixCluster {
        root,
        primary_node,
        mut primary,
        mut observer,
        ..
    } = cluster;

    let dir = "/users/pulled-directory";
    let path = "/users/pulled-directory/remote.txt";
    primary.posix_mkdir_all(dir).expect("primary directory");
    primary
        .posix_write_file(
            path,
            b"remote directory entry",
            DfsWriteOptions::replicated_eventual(vec![primary_node]),
        )
        .expect("primary file");

    let stop_primary = Arc::new(AtomicBool::new(false));
    let primary_stop = Arc::clone(&stop_primary);
    let primary_thread = std::thread::spawn(move || {
        while !primary_stop.load(Ordering::SeqCst) {
            primary.process_incoming().expect("primary pump");
            std::thread::sleep(Duration::from_millis(1));
        }
        primary
    });

    observer.disable_implicit_read_replica_ingest();
    let listed =
        observer.capture_mounted_mutation(|service| service.posix_list_dir_distributed(dir));
    let entries = listed.outcome.expect("outsider pulls directory journal");
    assert!(entries.iter().any(|entry| entry.path == path));
    assert!(
        listed.committed_work.metadata_replica_progress,
        "a non-replica SyncPath pull must retain its durable replica-progress edge"
    );
    let settled =
        observer.capture_mounted_mutation(|service| service.posix_list_dir_distributed(dir));
    let settled_entries = settled.outcome.expect("settled outsider directory pull");
    assert!(settled_entries.iter().any(|entry| entry.path == path));
    assert!(
        !settled.committed_work.metadata_replica_progress,
        "an explicit directory pull with no newly applied entries must not report replica progress"
    );

    stop_primary.store(true, Ordering::SeqCst);
    primary_thread.join().expect("primary thread");
    std::fs::remove_dir_all(&root).ok();
}
