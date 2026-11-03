//! Namespace regression tests.

use super::*;

#[test]
fn current_namespace_owner_operations_roundtrip_distinct_semantics() {
    let codec = sinomo_core::network::SinomoObjectCodec::network_frame();
    let operations = [
        DfsNamespaceOperation::SetOwner {
            path: "/users/owner".into(),
            uid: 1001,
            gid: 1002,
        },
        DfsNamespaceOperation::SetSymlinkOwner {
            path: "/users/link-owner".into(),
            uid: 1001,
            gid: 1002,
        },
        DfsNamespaceOperation::MkdirWithAttrs {
            path: "/users/private".into(),
            attrs: DfsPosixCreateAttrs::new(0o700, 1001, 1002),
        },
    ];
    let encoded = operations
        .iter()
        .map(|operation| codec.encode(operation).unwrap())
        .collect::<Vec<_>>();
    for (operation, bytes) in operations.iter().zip(&encoded) {
        assert_eq!(
            &codec.decode::<DfsNamespaceOperation>(bytes).unwrap(),
            operation
        );
    }
    assert_ne!(encoded[0], encoded[1]);
    assert_ne!(encoded[0], encoded[2]);
}

#[test]
fn posix_remote_stream_returns_before_body_collection() {
    let bytes = vec![0x5a; 48 * 1024];
    let (runtime, owner, _, reader_node, stat, bytes) =
        native_read_fixture("native-posix-stream-head", 1, false, bytes);
    let mut service = runtime.client(&reader_node).unwrap();
    // Warm the ordinary metadata-only lookup before measuring the body-open
    // boundary; this changes no read deadline or producer behavior.
    assert_eq!(
        service
            .posix_stat_distributed(&stat.path)
            .unwrap()
            .object_id,
        stat.object_id
    );
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let started = Arc::new(AtomicBool::new(false));
    let forwarded = Arc::new(AtomicUsize::new(0));
    runtime.install_fault(crate::test_support::DfsTestFaultRule {
        source: Some(reader_node.clone()),
        target: Some(owner.clone()),
        route: Some(DFS_DIRECT_READ_ROUTE.into()),
        call_id: None,
        origin: Some(crate::test_support::DfsTestRpcOrigin::RegisteredHost),
        remaining_hits: 1,
        action: crate::test_runtime::FaultAction::PauseNativeReadBody {
            gate: Arc::clone(&gate),
            started: Arc::clone(&started),
            forwarded_bytes: Arc::clone(&forwarded),
            relay_hops: 0,
        },
    });
    crate::test_runtime::executor().block_on(async {
        let mut opened = tokio::time::timeout(
            Duration::from_millis(100),
            service.posix_open_remote_file_stream(&stat.path),
        )
        .await
        .expect("normal POSIX stream returns the actual head before body production")
        .unwrap();
        assert_eq!(opened.object_id, stat.object_id);
        assert_eq!(opened.version, stat.version);
        assert_eq!(opened.content_hash, stat.content_hash.clone().unwrap());
        assert_eq!(opened.stream.observed_size_bytes(), 0);
        assert!(!started.load(Ordering::Acquire));
        assert_eq!(forwarded.load(Ordering::Acquire), 0);
        gate.add_permits(32);
        let mut actual = Vec::new();
        while let Some(chunk) = opened.stream.next_chunk().await.unwrap() {
            actual.extend_from_slice(chunk.as_ref());
        }
        assert_eq!(actual, bytes);
        assert_eq!(
            opened.stream.finish().await.unwrap().content_size_bytes,
            48 * 1024
        );
    });
    drop(service);
    runtime.clear_faults();
    runtime.close().unwrap();
}

#[test]
fn sifs_recursive_clear_follows_directory_symlink_argument() {
    let root = temp_root("sifs-recursive-clear-directory-symlink");
    let node_id = NodeId::from_legacy_named("node_sifs_clear_symlink").expect("node");
    let base = "/services/sandbox/instances/sandbox_clear_symlink";
    let real_dir = format!("{base}/tmp/0");
    let link_dir = format!("{base}/work/current-tmp");
    let file_path = format!("{real_dir}/session-cache.txt");

    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_recursive_clear_follows_directory_symlink_argument",
        node_id.clone(),
    );
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
        dfs.posix_read_file_distributed(&file_path),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert_eq!(
        dfs.posix_lstat_distributed(&link_dir)
            .expect("link survives clear")
            .kind,
        DfsObjectKind::Symlink
    );
    assert!(
        dfs.posix_list_dir_distributed(&real_dir)
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

    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_recursive_clear_is_scoped_to_target_subtree",
        node_id.clone(),
    );
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
        dfs.posix_read_file_distributed(target_file),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert_eq!(
        dfs.posix_read_file_distributed(sibling_file)
            .expect("sibling survives"),
        sibling_file.as_bytes()
    );
    assert_eq!(
        dfs.posix_read_file_distributed(unrelated_file)
            .expect("unrelated survives"),
        unrelated_file.as_bytes()
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_nonexclusive_create_follows_a_dangling_symlink() {
    let root = temp_root("sifs-posix-create-dangling-symlink");
    let node_id = NodeId::from_legacy_named("node_sifs_create_dangling_symlink").expect("node");
    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_posix_nonexclusive_create_follows_a_dangling_symlink",
        node_id.clone(),
    );
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
        dfs.posix_lstat_distributed(link)
            .expect("link remains")
            .kind,
        DfsObjectKind::Symlink
    );
    assert_eq!(
        dfs.posix_readlink_distributed(link).expect("read link"),
        "target"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn native_warm_remote_client_recreates_unlinked_dangling_symlink() {
    let SifsPosixCluster {
        _runtime: runtime,
        primary_node,
        writer_node,
        mut writer,
        primary,
        observer,
        ..
    } = sifs_posix_cluster("native-warm-symlink-recreate", "/");
    drop(primary);
    drop(observer);
    // Use the actual mounted opener, not the native owning service shell. The
    // adapters and configuration still belong to this registered Main runtime.
    let control_plane = writer.control_plane.take();
    drop(writer);
    let (sync_transport, async_transport) = runtime.peer_transports(&writer_node).unwrap();
    let mut writer = crate::open_mounted_sifs_service_with_peer_transports(
        runtime.data_dir(&writer_node).clone(),
        sync_transport,
        async_transport,
        control_plane,
    )
    .expect("ordinary mounted service over native Main adapters")
    .service;
    assert_ne!(primary_node, writer_node);
    assert!(writer.transport.delegates_local_namespace_authority());
    let parent = "/native-warm-symlink-recreate";
    let link = "/native-warm-symlink-recreate/link";
    let target = "/native-warm-symlink-recreate/absent-target";
    writer.posix_mkdir(parent).expect("native parent creation");
    let options =
        DfsWriteOptions::replicated_eventual(vec![primary_node.clone(), writer_node.clone()]);
    writer
        .posix_symlink("absent-target", link, options.clone())
        .expect("native initial dangling symlink");
    let mut previous = writer
        .posix_lstat_distributed(link)
        .expect("actual initial namespace entry");
    let mut recreate_failure = None;
    for round in 0..2 {
        assert_eq!(previous.kind, DfsObjectKind::Symlink);
        assert!(matches!(
            writer.posix_stat_distributed(target),
            Err(DfsPosixError::NotFound { .. })
        ));
        assert!(matches!(
            writer.posix_symlink("other-target", link, options.clone()),
            Err(DfsPosixError::AlreadyExists { .. })
        ));
        assert_eq!(
            writer.posix_readlink_distributed(link).unwrap(),
            "absent-target",
            "a rejected duplicate must preserve the real dangling link"
        );
        let capture = runtime
            .capture_namespace_requests(&primary_node, 32)
            .expect("capture actual unlink request");
        let observations_before = runtime.rpc_observations().len();
        let removed = writer.posix_unlink(link).expect("native unlink");
        // Recreate immediately on the SAME mounted client. Even an observational
        // owner open or lstat here could refresh a shared view and hide the bug.
        let recreated = writer.posix_symlink("absent-target", link, options.clone());
        let calls = runtime.rpc_observations();
        let request = capture
            .records()
            .into_iter()
            .find_map(|message| match message.message {
                DfsNetworkMessage::NamespaceRequest(command)
                    if matches!(&command.operation,
                        DfsNamespaceOperation::ConditionalUnlink { path, .. } if path == link) =>
                {
                    Some(command)
                }
                _ => None,
            })
            .expect("unlink entered the real remote Main");
        drop(capture);
        assert_eq!(removed.kind, DfsObjectKind::Tombstone);
        assert_eq!(removed.object_id, previous.object_id);
        let local_after = writer.lookup_sifs_path(link).unwrap();
        let old_transient_after = writer
            .transient_metadata_object_ids
            .contains(&previous.object_id);
        let mut owner = runtime
            .metadata_primary_service(link)
            .expect("actual root metadata owner");
        let shard = runtime.root_shard().expect("actual root assignment");
        assert_eq!(shard.primary_node_id, primary_node);
        let shard_id = DfsMetadataShardId::new(shard.shard_id);
        let backend = owner.metadata_backend_mut().unwrap();
        let tombstone = backend
            .get_object_on_shard(&shard_id, &previous.object_id)
            .unwrap()
            .expect("old object's durable unlink tombstone survives recreation");
        assert_eq!(tombstone.kind, DfsObjectKind::Tombstone);
        assert_eq!(tombstone.object_id, previous.object_id);
        assert_eq!(tombstone.version, removed.version);
        let admission = backend
            .namespace_request_admission_snapshot(
                &shard_id,
                &request.requesting_node_id,
                &request.request_id,
            )
            .expect("original native unlink outcome");
        let digest = namespace_command_digest(&request).unwrap();
        if let Some(receipt) = admission.receipt {
            assert_eq!(receipt.command_digest, digest);
            assert_eq!(receipt.result, DfsNamespaceResult::Applied);
        } else {
            let recorded = admission.request.expect("original unlink retained outcome");
            assert_eq!(recorded.command_digest, digest);
            assert_eq!(recorded.status, DfsNamespaceRequestStatus::Completed);
            assert_eq!(recorded.result, Some(DfsNamespaceResult::Applied));
        }
        drop(owner);
        let write_calls = calls[observations_before..]
            .iter()
            .filter(|call| {
                call.source == writer_node
                    && matches!(
                        call.route.as_str(),
                        DFS_DATA_WRITE_ROUTE | DFS_DATA_DISTRIBUTED_WRITE_ROUTE
                    )
                    && call.phase == crate::test_runtime::RpcPhase::BeforeDispatch
            })
            .collect::<Vec<_>>();
        eprintln!(
            "MOUNTED_SYMLINK_RECREATE round={round} writer={writer_node} primary={primary_node} unlink_request={} tombstone={tombstone:?} local_after={local_after:?} old_transient_after={old_transient_after} recreate={recreated:?} write_calls={write_calls:?} omitted={}",
            request.request_id,
            runtime.observation_dropped_count()
        );
        match recreated {
            Ok(_) => {
                let current = writer.posix_lstat_distributed(link).unwrap();
                assert_eq!(current.kind, DfsObjectKind::Symlink);
                assert_ne!(current.object_id, previous.object_id);
                assert_eq!(
                    writer.posix_readlink_distributed(link).unwrap(),
                    "absent-target"
                );
                previous = current;
            }
            Err(error) => {
                recreate_failure = Some(format!("round {round}: {error}"));
                break;
            }
        }
    }
    assert_eq!(runtime.observation_dropped_count(), 0);
    drop(writer);
    runtime.close().expect("native owners drain");
    assert!(
        recreate_failure.is_none(),
        "the same warm native non-primary writer must recreate its authoritatively unlinked dangling symlink: {recreate_failure:?}"
    );
}

#[test]
fn native_mounted_symlink_recreate_with_held_replica_materialization() {
    let SifsPosixCluster {
        _runtime: runtime,
        primary_node,
        writer_node,
        mut writer,
        primary,
        observer,
        ..
    } = sifs_posix_cluster("native-held-symlink-recreate", "/");
    drop(primary);
    drop(observer);
    let control_plane = writer.control_plane.take();
    drop(writer);
    let (sync_transport, async_transport) = runtime.peer_transports(&writer_node).unwrap();
    let mut writer = crate::open_mounted_sifs_service_with_peer_transports(
        runtime.data_dir(&writer_node).clone(),
        sync_transport,
        async_transport,
        control_plane,
    )
    .expect("actual mounted opener over registered native Main")
    .service;
    let shard = runtime.root_shard().unwrap();
    let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
    assert_ne!(primary_node, writer_node);
    assert_eq!(shard.primary_node_id, primary_node);
    assert!(shard.replica_node_ids.contains(&writer_node));
    assert!(writer.transport.delegates_local_namespace_authority());
    let link = "/native-held-symlink-recreate/link";
    writer.posix_mkdir("/native-held-symlink-recreate").unwrap();
    let options =
        DfsWriteOptions::replicated_eventual(vec![primary_node.clone(), writer_node.clone()]);
    writer
        .posix_symlink("absent-target", link, options.clone())
        .expect("initial native dangling symlink");
    let original = writer.posix_lstat_distributed(link).unwrap();
    let initial_sequence = {
        let owner = runtime.metadata_primary_service(link).unwrap();
        let backend = owner.metadata_backend().unwrap();
        let current = backend.get_object(&original.object_id).unwrap().unwrap();
        assert_eq!(current.version, original.version);
        let snapshot = backend.export_table_snapshot().unwrap();
        assert!(snapshot.prefix_proof.is_well_formed());
        assert_eq!(
            snapshot.materialized_through_sequence,
            snapshot.prefix_proof.journal_sequence
        );
        snapshot.materialized_through_sequence
    };
    crate::runtime_host::materialization_test::wait_for_existing_materialization(
        runtime.data_dir(&writer_node),
        &sifs_file_system_id(),
        &shard_id,
        &runtime.runtime_handle(),
        initial_sequence,
        Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
        |_| {},
    )
    .expect("initial real symlink is materialized before the hold");
    assert_eq!(
        node_store_object(&runtime, &writer_node, link)
            .expect("the writer node's store holds the initial symlink")
            .version,
        original.version
    );
    let pause = Arc::new(
        runtime
            .pause_replica_materialization(&writer_node, &shard_id)
            .unwrap(),
    );
    let unlink_capture = runtime
        .capture_namespace_requests(&primary_node, 32)
        .unwrap();
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let previous_deadline = writer.set_transport_operation_deadline(Some(deadline));
    let removed = writer.posix_unlink(link).expect("ordinary primary unlink");
    let captured = runtime
        .wait_paused_materialization(&pause, deadline.saturating_duration_since(Instant::now()))
        .expect("the actual replica materializer reached its pause");
    let pending = crate::runtime_host::materialization_test::wait_for_existing_materialization(
        runtime.data_dir(&writer_node),
        &sifs_file_system_id(),
        &shard_id,
        &runtime.runtime_handle(),
        initial_sequence,
        deadline,
        |_| {},
    )
    .expect("observe the existing replica after its actual ingress checkout returns");
    eprintln!("HELD_MOUNTED_SYMLINK_BOUNDARY captured={captured:?} pending={pending:?}");
    assert!(captured.unavailable.is_none(), "{captured:?}");
    assert!(pending.unavailable.is_none(), "{pending:?}");
    assert!(
        pending.durable_sequence.unwrap() > pending.materialized_sequence.unwrap(),
        "the real unlink must leave accepted journal work pending: {pending:?}"
    );
    let request = unlink_capture
        .records()
        .into_iter()
        .find_map(|message| match message.message {
            DfsNetworkMessage::NamespaceRequest(command)
                if matches!(&command.operation,
                    DfsNamespaceOperation::ConditionalUnlink { path, .. } if path == link) =>
            {
                Some(command)
            }
            _ => None,
        })
        .expect("actual unlink request identity");
    drop(unlink_capture);
    // Release only when the real recreate crosses the decision under test and
    // reaches Main. The hold must not prevent its later publication completion.
    let dispatched = Arc::new(Mutex::new(None));
    let observed = Arc::clone(&dispatched);
    let release_on_dispatch = Arc::clone(&pause);
    let expected_source = writer_node.clone();
    let expected_target = primary_node.clone();
    let dispatch_hook = crate::runtime_host::install_daemon_dfs_pre_dispatch_test_hook(
        runtime.data_dir(&primary_node),
        Arc::new(move |point, command, _| {
            if !matches!(
                point,
                crate::runtime_host::DaemonDfsPreDispatchTestPoint::BeforeExecute { .. }
            ) {
                return Ok(());
            }
            if let Some(command) = command
                && command.routed_message.source_node_id == expected_source
                && command.routed_message.target_node_id == expected_target
                && let DfsNetworkMessage::DistributedWriteRequest(write)
                | DfsNetworkMessage::ExclusiveCreateRequest(write) =
                    &command.routed_message.message
                && write.object_metadata.as_ref().is_some_and(|object| {
                    object.path == link && object.kind == DfsObjectKind::Symlink
                })
            {
                *observed.lock().unwrap() = Some((
                    write.request_id.clone(),
                    write.request.object_id.clone(),
                    write.request.target_version.clone(),
                ));
                release_on_dispatch.release();
            }
            Ok(())
        }),
    );
    let recreated = writer.posix_symlink("absent-target", link, options);
    let actual_dispatch = dispatched.lock().unwrap().clone();
    let local_after = writer.lookup_sifs_path(link).unwrap();
    pause.release();
    drop(dispatch_hook);
    let published_sequence = {
        let mut owner = runtime.metadata_primary_service(link).unwrap();
        let backend = owner.metadata_backend_mut().unwrap();
        let tombstone = backend
            .get_object_on_shard(&shard_id, &original.object_id)
            .unwrap()
            .unwrap();
        assert_eq!(removed.kind, DfsObjectKind::Tombstone);
        assert_eq!(tombstone.kind, DfsObjectKind::Tombstone);
        assert_eq!(tombstone.version, removed.version);
        let admission = backend
            .namespace_request_admission_snapshot(
                &shard_id,
                &request.requesting_node_id,
                &request.request_id,
            )
            .unwrap();
        let digest = namespace_command_digest(&request).unwrap();
        if let Some(receipt) = admission.receipt {
            assert_eq!(receipt.command_digest, digest);
            assert_eq!(receipt.result, DfsNamespaceResult::Applied);
        } else {
            let recorded = admission.request.expect("exact original unlink outcome");
            assert_eq!(recorded.command_digest, digest);
            assert_eq!(recorded.status, DfsNamespaceRequestStatus::Completed);
            assert_eq!(recorded.result, Some(DfsNamespaceResult::Applied));
        }
        backend
            .export_table_snapshot()
            .unwrap()
            .materialized_through_sequence
    };
    let materialized = crate::runtime_host::materialization_test::wait_for_existing_materialization(
        runtime.data_dir(&writer_node),
        &sifs_file_system_id(),
        &shard_id,
        &runtime.runtime_handle(),
        published_sequence,
        deadline,
        |_| {},
    );
    eprintln!(
        "HELD_MOUNTED_SYMLINK writer={writer_node} primary={primary_node} unlink_request={} initial_sequence={initial_sequence} captured={captured:?} pending={pending:?} recreate={recreated:?} dispatch={actual_dispatch:?} local_after={local_after:?} publication_sequence={published_sequence} materialized={materialized:?} omitted={}",
        request.request_id,
        runtime.observation_dropped_count()
    );
    materialized.expect("publication materializes within the original caller deadline");
    let recreated =
        recreated.expect("warm mounted recreate must use the actual final-entry authority");
    assert!(
        actual_dispatch.is_some(),
        "real recreate must reach native Main"
    );
    let current = writer.posix_lstat_distributed(link).unwrap();
    assert_eq!(current.kind, DfsObjectKind::Symlink);
    assert_eq!(current.object_id, recreated.object_id);
    assert_ne!(current.object_id, original.object_id);
    assert_eq!(
        writer.posix_readlink_distributed(link).unwrap(),
        "absent-target"
    );
    assert!(Instant::now() < deadline, "original caller deadline");
    writer.set_transport_operation_deadline(previous_deadline);
    assert_eq!(runtime.observation_dropped_count(), 0);
    drop(pause);
    drop(writer);
    runtime.close().expect("actual native owners drain");
}

#[test]
fn sifs_posix_create_attrs_are_installed_atomically_and_only_on_create() {
    let root = temp_root("sifs-posix-create-attrs");
    let node_id = NodeId::from_legacy_named("node_sifs_create_attrs").expect("node");
    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_posix_create_attrs_are_installed_atomically_and_only_on_create",
        node_id.clone(),
    );
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
            dfs.posix_lstat_distributed(link_path)
                .expect("lstat link")
                .mode,
            dfs.posix_lstat_distributed(link_path)
                .expect("lstat link")
                .uid,
            dfs.posix_lstat_distributed(link_path)
                .expect("lstat link")
                .gid,
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

    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_posix_symlink_preserves_target_as_object_content",
        node_id.clone(),
    );
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
        dfs.posix_readlink_distributed(link_path).expect("readlink"),
        "../shared/lib"
    );
    let owned_link = dfs
        .posix_set_symlink_owner(link_path, 1001, 1002)
        .expect("set symlink owner without following missing target");
    assert_eq!((owned_link.uid, owned_link.gid), (1001, 1002));
    assert_eq!(
        (
            dfs.posix_lstat_distributed(link_path)
                .expect("owned link lstat")
                .uid,
            dfs.posix_lstat_distributed(link_path)
                .expect("owned link lstat")
                .gid,
        ),
        (1001, 1002)
    );
    dfs.posix_set_symlink_times(link_path, Some(12_345), Some(67_890))
        .expect("old symlink times");
    let before_readlink = dfs
        .posix_lstat_distributed(link_path)
        .expect("before readlink lstat");
    assert_eq!(
        dfs.posix_readlink_update_access(link_path)
            .expect("readlink update access"),
        "../shared/lib"
    );
    let stat = dfs
        .posix_lstat_distributed(link_path)
        .expect("symlink lstat");
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

    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_posix_trailing_slash_requires_directory",
        node_id.clone(),
    );
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
        dfs.posix_lstat_distributed(link_dir)
            .expect("plain lstat")
            .kind,
        DfsObjectKind::Symlink
    );
    assert_eq!(
        dfs.posix_lstat_distributed(&format!("{link_dir}/"))
            .expect("trailing slash lstat follows directory symlink")
            .kind,
        DfsObjectKind::Directory
    );
    assert!(matches!(
        dfs.posix_read_file_distributed(&format!("{file_path}/")),
        Err(DfsPosixError::NotDirectory { .. })
    ));
    assert!(matches!(
        dfs.posix_read_file_distributed(&format!("{link_file}/")),
        Err(DfsPosixError::NotDirectory { .. })
    ));
    assert!(matches!(
        dfs.posix_readlink_distributed(&format!("{link_dir}/")),
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
        dfs.posix_lstat_distributed(link_dir)
            .expect("symlink survives failed unlink")
            .kind,
        DfsObjectKind::Symlink
    );
    assert_eq!(
        dfs.posix_stat_distributed(real_dir)
            .expect("target survives")
            .kind,
        DfsObjectKind::Directory
    );
    assert_eq!(
        dfs.posix_list_dir_distributed(&format!("{work}/"))
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
        dfs.posix_stat_distributed(&format!("{work}/renamed-dir"))
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

    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_posix_rmdir_reports_directory_not_empty",
        node_id.clone(),
    );
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

    let (runtime, mut dfs) = native_sifs_service(
        "sifs_posix_empty_directory_checks_are_bounded_for_large_directories",
        node_id.clone(),
    );
    dfs.posix_mkdir_all(source).expect("mkdir source");
    let mut checkpointed = 0;
    for index in 0..160 {
        dfs.posix_mkdir_all(&format!("{nonempty}/child-{index:04}"))
            .expect("mkdir child");
        if (index + 1) % 32 == 0 {
            checkpointed += runtime.checkpoint_rpc_observation_window().unwrap();
        }
    }
    assert!(checkpointed > 0);
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
    assert!(dfs.posix_stat_distributed(source).is_ok());

    assert_eq!(runtime.observation_dropped_count(), 0);
    drop(dfs);
    runtime
        .close()
        .expect("all native observation windows and owners drain");
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_stat_reports_directory_link_counts() {
    let root = temp_root("sifs-posix-link-count");
    let node_id = NodeId::from_legacy_named("node_sifs_link_count").expect("node");
    let parent = "/services/sandbox/instances/sandbox_links/work";
    let child_dir = "/services/sandbox/instances/sandbox_links/work/child";
    let nested_file = "/services/sandbox/instances/sandbox_links/work/file.txt";

    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_posix_stat_reports_directory_link_counts",
        node_id.clone(),
    );
    dfs.posix_mkdir_all(child_dir).expect("mkdir child");
    dfs.posix_write_file(
        nested_file,
        b"content",
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("write");

    assert_eq!(
        dfs.posix_stat_distributed(parent)
            .expect("parent stat")
            .link_count,
        3
    );
    assert_eq!(
        dfs.posix_stat_distributed(child_dir)
            .expect("child stat")
            .link_count,
        2
    );
    assert_eq!(
        dfs.posix_stat_distributed(nested_file)
            .expect("file stat")
            .link_count,
        1
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_rename_directory_batches_child_moves_per_job_batch() {
    let node_id = NodeId::from_legacy_named("node_sifs_rename_dir_batch").expect("node");
    let old_dir = "/services/sandbox/instances/sandbox_rename_batch/work/old";
    let new_dir = "/services/sandbox/instances/sandbox_rename_batch/work/new";
    let max_steps = 4;
    let child_count = 10usize;

    let (mut runtime, mut dfs) = native_sifs_service(
        "sifs_posix_rename_directory_batches_child_moves_per_job_batch",
        node_id.clone(),
    );
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
        child_object_ids.insert(
            dfs.posix_stat_distributed(&old_path)
                .expect("child stat")
                .object_id,
        );
        old_paths.push(old_path);
        new_paths.push(new_path);
    }
    // The bounded producer runs inside its actual owning Core. Fixture setup
    // above used normal Main routing; that delegating shell cannot run the
    // private directory-job algorithm or supply its current journal baseline.
    drop(dfs);
    let mut dfs = runtime
        .metadata_primary_service(old_dir)
        .expect("actual owning Core for the bounded directory rename");
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
            DfsMetadataMutation::PrepareOperationV7 { record, .. }
                if record
                    .transaction
                    .transaction_id
                    .starts_with("namespace_rename-child-batch_") =>
            {
                Some(&record.transaction)
            }
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
    runtime.stop_node(&node_id).unwrap();
    runtime.reopen_node(node_id.clone()).unwrap();
    let mut reopened = runtime.client(&node_id).unwrap();
    for new_path in &new_paths {
        assert_eq!(
            reopened
                .posix_stat_distributed(new_path)
                .expect("reopened child")
                .kind,
            DfsObjectKind::File
        );
    }
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

    let (runtime, mut dfs) = native_sifs_service(
        "sifs_posix_rename_directory_caps_child_move_transactions_above_step_budget",
        node_id.clone(),
    );
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
    // The bounded producer runs inside its actual owning Core. Fixture setup
    // above used normal Main routing; that delegating shell keeps no store,
    // so it can neither publish the child rows directly, run the private
    // directory-job algorithm nor supply its current journal baseline.
    drop(dfs);
    let mut dfs = runtime
        .metadata_primary_service(old_dir)
        .expect("actual owning Core for the bounded directory rename");
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
            DfsMetadataMutation::PrepareOperationV7 { record, .. }
                if record
                    .transaction
                    .transaction_id
                    .starts_with("namespace_rename-child-batch_") =>
            {
                Some(&record.transaction)
            }
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
    let node_id = NodeId::from_legacy_named("node_sifs_rename_dir_deleted_child").expect("node");
    let old_dir = "/services/sandbox/instances/sandbox_rename_deleted/work/old";
    let new_dir = "/services/sandbox/instances/sandbox_rename_deleted/work/new";
    let old_child = format!("{old_dir}/deleted.txt");
    let new_child = format!("{new_dir}/deleted.txt");

    let (mut runtime, setup) =
        native_sifs_service("native-directory-no-resurrection", node_id.clone());
    drop(setup);
    {
        let mut dfs = runtime.client(&node_id).unwrap();
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
            dfs.posix_read_file_distributed(&new_child),
            Err(DfsPosixError::NotFound { .. })
        ));
        assert!(
            dfs.posix_list_dir_distributed(new_dir)
                .expect("new dir listing")
                .is_empty()
        );
    }

    runtime.stop_node(&node_id).unwrap();
    runtime.reopen_node(node_id.clone()).unwrap();
    let mut reloaded = runtime.client(&node_id).unwrap();
    assert!(matches!(
        reloaded.posix_read_file_distributed(&new_child),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert!(
        reloaded
            .posix_list_dir_distributed(new_dir)
            .expect("reloaded new dir listing")
            .is_empty()
    );
}

#[test]
fn sifs_posix_rename_over_existing_file_survives_reload() {
    let node_id = NodeId::from_legacy_named("node_sifs_rename_over_existing_reload").expect("node");
    let work = "/services/sandbox/instances/sandbox_rename_over_existing/work";
    let source = "/services/sandbox/instances/sandbox_rename_over_existing/work/source.txt";
    let dest = "/services/sandbox/instances/sandbox_rename_over_existing/work/dest.txt";

    let (mut runtime, mut dfs) = native_sifs_service(
        "sifs_posix_rename_over_existing_file_survives_reload",
        node_id.clone(),
    );
    dfs.posix_mkdir_all(work).expect("mkdir");
    dfs.posix_write_file(
        source,
        b"source",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write source");
    let source_object_id = dfs
        .posix_stat_distributed(source)
        .expect("source stat")
        .object_id;
    dfs.posix_write_file(
        dest,
        b"dest",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write dest");

    dfs.posix_rename(source, dest).expect("rename over dest");

    assert!(matches!(
        dfs.posix_stat_distributed(source),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert_eq!(
        dfs.posix_read_file_distributed(dest).expect("read dest"),
        b"source"
    );
    assert_eq!(
        dfs.posix_stat_distributed(dest)
            .expect("dest stat")
            .object_id,
        source_object_id
    );

    let mut reloaded = runtime.client(&node_id).unwrap();
    assert!(matches!(
        reloaded.posix_stat_distributed(source),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert_eq!(
        reloaded
            .posix_read_file_distributed(dest)
            .expect("reloaded read dest"),
        b"source"
    );
    assert_eq!(
        reloaded
            .posix_stat_distributed(dest)
            .expect("reloaded dest stat")
            .object_id,
        source_object_id
    );
}

#[test]
fn sifs_posix_set_times_without_fields_is_noop() {
    let root = temp_root("sifs-posix-set-times-noop");
    let node_id = NodeId::from_legacy_named("node_sifs_set_times_noop").expect("node");
    let file = "/services/sandbox/instances/sandbox_set_times_noop/work/file.txt";

    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_posix_set_times_without_fields_is_noop",
        node_id.clone(),
    );
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
    let before = dfs.posix_stat_distributed(file).expect("before stat");

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

    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_posix_set_mode_zero_is_reported_as_zero",
        node_id.clone(),
    );
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
    assert_eq!(dfs.posix_stat_distributed(file).expect("stat").mode, 0);
    assert!(
        dfs.posix_stat_distributed(file)
            .expect("stat")
            .changed_at_unix_nanos
            >= changed.changed_at_unix_nanos
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

    let (runtime, mut dfs) = native_sifs_service(
        "sifs_posix_metadata_only_versions_preserve_content_size",
        node_id.clone(),
    );
    dfs.posix_mkdir_all(work).expect("mkdir");
    dfs.posix_write_file(
        file,
        b"content",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write file");
    // Main owns setup; immutable-body retries run in the actual owning Core,
    // rather than a delegating public client's optional cached backend.
    drop(dfs);
    let mut dfs = runtime
        .metadata_primary_service(file)
        .expect("actual metadata owner for metadata successors");
    let original = dfs.posix_stat(file).expect("original head");
    let original_version = dfs
        .metadata_backend()
        .unwrap()
        .get_version(&original.object_id, &original.version)
        .unwrap()
        .unwrap();
    let durability = original_version
        .content_durability
        .expect("actual write grant");
    let original_receipts = dfs
        .metadata_backend()
        .unwrap()
        .receipts_for_version(&original.object_id, &durability.source_version)
        .unwrap();
    assert!(original_receipts.len() >= durability.policy.min_commit_replicas);
    let chmod = dfs.posix_set_mode(file, 0o600).expect("chmod");
    assert_eq!(chmod.size_bytes, Some(7));
    assert_eq!(
        dfs.metadata_backend()
            .unwrap()
            .get_version(&chmod.object_id, &chmod.version)
            .unwrap()
            .unwrap()
            .content_durability,
        Some(durability.clone())
    );
    let moved = dfs.posix_rename(file, renamed).expect("rename");
    assert_eq!(moved.size_bytes, Some(7));
    assert_ne!(moved.version, durability.source_version);
    assert_eq!(
        dfs.metadata_backend()
            .unwrap()
            .get_version(&moved.object_id, &moved.version)
            .unwrap()
            .unwrap()
            .content_durability,
        Some(durability.clone())
    );
    assert_eq!(
        dfs.metadata_backend()
            .unwrap()
            .receipts_for_version(&original.object_id, &durability.source_version)
            .unwrap(),
        original_receipts
    );
    assert!(
        dfs.metadata_backend()
            .unwrap()
            .receipts_for_version(&moved.object_id, &moved.version)
            .unwrap()
            .is_empty(),
        "metadata succession must not restamp its original W receipts"
    );

    let transaction_id = crate::namespace::namespace_transaction_id_for_versioned_path(
        "rename",
        &moved.object_id,
        &moved.version,
        renamed,
    );
    let rename_snapshot = dfs
        .metadata_backend()
        .unwrap()
        .export_table_snapshot()
        .expect("actual immutable rename snapshot");
    let rename_records = rename_snapshot
        .tables
        .prepared_operations_v7
        .iter()
        .filter(|record| {
            record.transaction.transaction_id == transaction_id
                && record.transaction.objects.iter().any(|object| {
                    object.object_id == moved.object_id && object.version == moved.version
                })
        })
        .collect::<Vec<_>>();
    assert_eq!(rename_records.len(), 1, "one actual immutable rename body");
    let rename_record = (*rename_records[0]).clone();
    rename_record
        .validate()
        .expect("complete original rename digest");
    assert_eq!(
        rename_record.status,
        crate::DfsPreparedOperationStatus::Committed
    );
    assert!(rename_record.completed_request.is_none());
    assert!(rename_record.completed_request_completion.is_none());
    assert_eq!(
        dfs.metadata_backend()
            .unwrap()
            .committed_prepared_operation(&rename_record.digest)
            .unwrap(),
        rename_record
            .committed_entry
            .as_ref()
            .map(|entry| entry.journal_entry())
    );
    let mut original_transaction = rename_record.transaction.clone();
    original_transaction
        .versions
        .retain(|version| version.object_id != moved.object_id);
    let later = dfs
        .posix_set_mode(renamed, 0o640)
        .expect("later metadata head");
    dfs.apply_namespace_transaction(original_transaction.clone())
        .expect("exact recorded retry");
    assert_eq!(dfs.posix_stat(renamed).unwrap().version, later.version);
    original_transaction
        .objects
        .iter_mut()
        .find(|object| object.object_id == moved.object_id)
        .unwrap()
        .mode ^= 0o100;
    assert!(
        dfs.apply_namespace_transaction(original_transaction)
            .is_err(),
        "the recorded version completion cannot hide a changed retry body"
    );
    assert_eq!(dfs.posix_stat(renamed).unwrap().version, later.version);
    let after_retry = dfs
        .metadata_backend()
        .unwrap()
        .export_table_snapshot()
        .expect("original rename body after exact and changed retries");
    assert_eq!(
        after_retry
            .tables
            .prepared_operations_v7
            .iter()
            .find(|record| { record.operation_id == rename_record.operation_id }),
        Some(&rename_record),
        "retries retain the entire original digest, body, and terminal receipt"
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

    drop(dfs);
    runtime
        .close()
        .expect("normal metadata-size runtime shutdown");
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_recreate_file_refreshes_creation_metadata() {
    let root = temp_root("sifs-posix-recreate-file-times");
    let node_id = NodeId::from_legacy_named("node_sifs_recreate_file").expect("node");
    let file_path = "/services/sandbox/instances/sandbox_recreate/work/file.txt";

    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_posix_recreate_file_refreshes_creation_metadata",
        node_id.clone(),
    );
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
    assert_eq!(
        dfs.posix_read_file_distributed(file_path).expect("read"),
        b"new"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_recreate_directory_refreshes_creation_metadata() {
    let root = temp_root("sifs-posix-recreate-dir-times");
    let node_id = NodeId::from_legacy_named("node_sifs_recreate_dir").expect("node");
    let dir_path = "/services/sandbox/instances/sandbox_recreate_dir/work/dir";

    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_posix_recreate_directory_refreshes_creation_metadata",
        node_id,
    );
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

    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_posix_create_bulk_local_eventual_rejects_existing_paths",
        node_id.clone(),
    );
    dfs.posix_mkdir_all(work).expect("mkdir");
    let parent_before = dfs
        .posix_stat_distributed(work)
        .expect("parent before bulk create");
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
        dfs.posix_read_file_distributed(path_a).expect("read a"),
        b"bulk create alpha"
    );
    assert_eq!(
        dfs.posix_read_file_distributed(path_b).expect("read b"),
        b"bulk create beta"
    );
    let parent_after = dfs
        .posix_stat_distributed(work)
        .expect("parent after bulk create");
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
        dfs.posix_read_file_distributed(path_a)
            .expect("read unchanged a"),
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
        dfs.posix_read_file_distributed(path_a)
            .expect("read recreated a"),
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

    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_posix_list_updates_directory_atime_without_changing_mtime",
        node_id.clone(),
    );
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

    let stat = dfs.posix_stat_distributed(dir_path).expect("stat");
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

    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_posix_namespace_mutations_update_parent_directory_mtime",
        node_id.clone(),
    );
    dfs.posix_mkdir_all(work).expect("mkdir work");
    dfs.posix_mkdir_all(other).expect("mkdir other");

    dfs.posix_set_times(work, Some(12_345), Some(67_890))
        .expect("old work times");
    let before_create = dfs
        .posix_stat_distributed(work)
        .expect("work before create");
    std::thread::sleep(std::time::Duration::from_millis(1));
    dfs.posix_write_file(
        file,
        b"content",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("create file");
    let after_create = dfs.posix_stat_distributed(work).expect("work after create");
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
        dfs.posix_stat_distributed(work)
            .expect("work after overwrite")
            .modified_at_unix_nanos,
        67_890,
        "overwriting existing file content must not change parent directory mtime"
    );

    dfs.posix_hardlink(file, alias).expect("hardlink");
    assert!(
        dfs.posix_stat_distributed(work)
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
        dfs.posix_stat_distributed(work)
            .expect("work after symlink")
            .modified_at_unix_nanos
            > 67_890
    );

    dfs.posix_set_times(work, Some(12_345), Some(67_890))
        .expect("reset work times");
    dfs.posix_mkdir(dir).expect("mkdir child");
    assert!(
        dfs.posix_stat_distributed(work)
            .expect("work after mkdir")
            .modified_at_unix_nanos
            > 67_890
    );

    dfs.posix_set_times(work, Some(12_345), Some(67_890))
        .expect("reset work times");
    dfs.posix_rmdir(dir).expect("rmdir child");
    assert!(
        dfs.posix_stat_distributed(work)
            .expect("work after rmdir")
            .modified_at_unix_nanos
            > 67_890
    );

    dfs.posix_set_times(work, Some(12_345), Some(67_890))
        .expect("reset work times");
    dfs.posix_unlink(alias).expect("unlink alias");
    assert!(
        dfs.posix_stat_distributed(work)
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
        dfs.posix_stat_distributed(work)
            .expect("work after rename")
            .modified_at_unix_nanos
            > 67_890
    );
    assert!(
        dfs.posix_stat_distributed(other)
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

    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_posix_rename_updates_moved_object_ctime_without_changing_mtime",
        node_id.clone(),
    );
    dfs.posix_mkdir_all(work).expect("mkdir work");
    dfs.posix_write_file(
        source,
        b"content",
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("write source");
    dfs.posix_set_times(source, Some(12_345), Some(67_890))
        .expect("old source times");

    let before = dfs.posix_stat_distributed(source).expect("before stat");
    dfs.posix_rename(source, renamed).expect("rename");
    let after = dfs.posix_stat_distributed(renamed).expect("after stat");

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
    let mut cluster = sifs_posix_cluster("sifs-cluster-routed-posix-sync", "/users");
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
    drop(cluster);
}

#[test]
fn sifs_cluster_posix_contract_create_unlink_same_path_race() {
    let mut cluster = sifs_posix_cluster("sifs-contract-create-unlink", "/users");
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
    let final_stat = cluster.primary.posix_stat_distributed(path);
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
            // The observer node's store converges to the primary's outcome.
            let runtime = &cluster._runtime;
            node_store_object(runtime, &cluster.primary_node, path).is_none()
                == node_store_object(runtime, &cluster.observer_node, path).is_none()
        },
    );
    assert_sifs_cluster_no_posix_leaks(&cluster, scenario);
    finish_posix_observations(&observations);
    drop(cluster);
}

#[test]
fn sifs_cluster_posix_contract_same_directory_concurrent_creates() {
    let mut cluster = sifs_posix_cluster("sifs-contract-same-dir-creates", "/users");
    let scenario = "same-directory-concurrent-creates";
    let dir = "/users/contract-create-dir";
    let mut observations = Vec::new();
    cluster.primary.posix_mkdir(dir).expect("mkdir parent");
    sifs_cluster_publish_metadata_until(
        &mut cluster,
        "same-directory parent catch-up",
        |cluster| {
            let runtime = &cluster._runtime;
            node_store_object(runtime, &cluster.writer_node, dir).is_some()
                && node_store_object(runtime, &cluster.observer_node, dir).is_some()
        },
    );
    let before = cluster
        .primary
        .posix_stat_distributed(dir)
        .expect("parent before");

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
                .posix_stat_distributed(&format!("{dir}/child-{index}.txt"))
                .is_ok()
        }) && requests_settled
    });
    let after = cluster
        .primary
        .posix_stat_distributed(dir)
        .expect("parent after");
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
                node_store_object(
                    &cluster._runtime,
                    &cluster.observer_node,
                    &format!("{dir}/child-{index}.txt"),
                )
                .is_some()
            })
        },
    );
    assert_sifs_cluster_no_posix_leaks(&cluster, scenario);
    finish_posix_observations(&observations);
    drop(cluster);
}

#[test]
fn sifs_cluster_posix_contract_same_path_concurrent_creates() {
    let mut cluster = sifs_posix_cluster("sifs-contract-same-path-creates", "/users");
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
        writer_settled && observer_settled && cluster.primary.posix_stat_distributed(path).is_ok()
    });
    // The primary's own store holds exactly one live dentry for the path.
    let entries = node_store_children(&cluster._runtime, &cluster.primary_node, "/users");
    assert_eq!(
        entries
            .iter()
            .filter(|(name, _, _)| format!("/users/{name}") == path)
            .count(),
        1,
        "same-path create race must leave one dentry: {entries:?}"
    );
    let stat = cluster
        .primary
        .posix_stat_distributed(path)
        .expect("winner stat");
    assert!(
        [content_hash(&writer_bytes), content_hash(&observer_bytes)]
            .contains(stat.content_hash.as_ref().expect("winner hash")),
        "winner content must be one candidate"
    );
    assert_sifs_cluster_no_posix_leaks(&cluster, scenario);
    finish_posix_observations(&observations);
    drop(cluster);
}

#[test]
fn sifs_cluster_posix_contract_rmdir_vs_child_create() {
    let mut cluster = sifs_posix_cluster("sifs-contract-rmdir-create", "/users");
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
        sifs_routed_writes_settled(&cluster.writer)
    });
    let dir_present = cluster.primary.posix_stat_distributed(dir).is_ok();
    let child_present = cluster.primary.posix_stat_distributed(child).is_ok();
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
    drop(cluster);
}

#[test]
fn sifs_posix_rmdir_rejects_child_from_fresh_shared_metadata_handle() {
    let root = temp_root("sifs-rmdir-shared-metadata-race");
    let metadata_root = root.join("metadata");
    let node = NodeId::from_legacy_named("node_sifs_rmdir_shared_metadata").expect("node");
    let dir = "/services/shared-rmdir-race";
    let child = "/services/shared-rmdir-race/child.txt";

    let mut runtime = crate::test_runtime::DfsTestRuntime::new(
        "sifs_posix_rmdir_rejects_child_from_fresh_shared_metadata_handle",
        node.clone(),
    )
    .unwrap();
    runtime.start_nodes().unwrap();
    let mut seed = runtime.client(&node).unwrap();
    seed.bootstrap_distributed_sifs().expect("bootstrap sifs");
    seed.posix_mkdir(dir).expect("mkdir race dir");

    let mut child_writer = runtime.client(&node).unwrap();
    let mut remover = runtime.client(&node).unwrap();

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

    let mut verifier = runtime.client(&node).unwrap();
    assert!(verifier.posix_stat_distributed(dir).is_ok());
    assert!(verifier.posix_stat_distributed(child).is_ok());
}

#[test]
fn sifs_posix_child_create_rejects_parent_removed_by_fresh_shared_metadata_handle() {
    let root = temp_root("sifs-child-create-shared-metadata-race");
    let metadata_root = root.join("metadata");
    let node = NodeId::from_legacy_named("node_sifs_child_shared_metadata").expect("node");
    let dir = "/services/shared-child-race";
    let child = "/services/shared-child-race/child.txt";

    let mut runtime = crate::test_runtime::DfsTestRuntime::new(
        "sifs_posix_child_create_rejects_parent_removed_by_fresh_shared_metadata_handle",
        node.clone(),
    )
    .unwrap();
    runtime.start_nodes().unwrap();
    let mut seed = runtime.client(&node).unwrap();
    seed.bootstrap_distributed_sifs().expect("bootstrap sifs");
    seed.posix_mkdir(dir).expect("mkdir race dir");

    let mut child_writer = runtime.client(&node).unwrap();
    let mut remover = runtime.client(&node).unwrap();

    let live_changed_at = remover
        .posix_lstat_distributed(dir)
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

    let mut verifier = runtime.client(&node).unwrap();
    let verifier_dir = verifier.posix_stat_distributed(dir);
    assert!(
        matches!(verifier_dir, Err(DfsPosixError::NotFound { .. })),
        "removed parent resurrected after rejected child create: {verifier_dir:?}"
    );
    assert!(matches!(
        verifier.posix_stat_distributed(child),
        Err(DfsPosixError::NotFound { .. })
    ));
}

#[test]
fn sifs_cluster_posix_contract_directory_rename_while_child_mutations_run() {
    let cluster = sifs_two_primary_cluster("sifs-contract-dir-rename-child");
    let scenario = "directory-rename-while-child-mutations-run";
    let SifsTwoPrimaryCluster {
        shared_kv,
        services_primary_node,
        users_primary_node,
        _runtime,
        root,
        replica_node,
        users_primary,
        services_primary,
        mut replica,
        ..
    } = cluster;
    let mut observations = Vec::new();

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
        replica.posix_stat_distributed("/users/contract-proj"),
        Err(DfsPosixError::NotFound { .. })
    ));
    let old_child = replica.posix_stat_distributed("/users/contract-proj/during.txt");
    let new_child = replica.posix_stat_distributed("/services/contract-proj/during.txt");
    assert!(
        old_child.is_ok() ^ new_child.is_ok(),
        "child mutation must resolve under exactly one subtree"
    );

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
    sifs_two_primary_publish_until(&mut cluster, "dir rename child convergence", |cluster| {
        let runtime = &cluster._runtime;
        node_store_object(runtime, &cluster.replica_node, "/services/contract-proj").is_some()
            && node_store_object(runtime, &cluster.replica_node, "/users/contract-proj").is_none()
    });
    assert_two_primary_no_posix_leaks(&cluster, scenario);
    finish_posix_observations(&observations);
    cluster.close();
}

#[test]
fn sifs_cluster_posix_contract_paged_list_while_directory_mutates() {
    let mut cluster = sifs_posix_cluster("sifs-contract-paged-list", "/users");
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
    // Paged listing reads the primary's own store; its delegating client
    // keeps no local view to page through.
    let pager = cluster
        ._runtime
        .metadata_primary_service(dir)
        .expect("store-backed primary for the paged walk");
    assert_eq!(pager.node_id(), &cluster.primary_node);
    let first = pager.posix_list_dir_page(dir, None, 5).expect("first page");
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
        let page = pager
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
    drop(pager);
    let final_entries = cluster
        .primary
        .posix_list_dir_distributed(dir)
        .expect("final list");
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
    drop(cluster);
}

#[test]
fn lazy_service_rebuild_preserves_shared_namespace_guard_registry() {
    let node = NodeId::from_legacy_named("node_lazy_shared_namespace_guards").expect("node");
    let file_system_id = sifs_file_system_id();
    let (_runtime, mut service) = native_sifs_service(
        "lazy_service_rebuild_preserves_shared_namespace_guard_registry",
        node,
    );
    let registry = service.cross_shard_namespace_lock_registry();
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
}

#[test]
fn exact_namespace_projection_collapse_keeps_only_shard_runtime_state() {
    let node = NodeId::from_legacy_named("node_exact_namespace_collapse").expect("node");
    let file_system_id = sifs_file_system_id();
    let (_runtime, mut service) = native_sifs_service(
        "exact_namespace_projection_collapse_keeps_only_shard_runtime_state",
        node.clone(),
    );
    let config = _runtime
        .metadata_shards()
        .expect("actual committed metadata assignments")
        .into_iter()
        .find(|config| config.shard_id == "services")
        .expect("services assignment");
    let shard_id = DfsMetadataShardId::new(config.shard_id);
    let stat = service
        .posix_write_file(
            "/services/collapse-object",
            b"normal committed namespace projection",
            DfsWriteOptions::replicated_eventual(vec![node]),
        )
        .expect("publish through the normal service");
    let object = service
        .current_read_object_metadata_distributed(&stat.object_id)
        .expect("actual committed object");
    let resident = {
        let cache_slot = crate::runtime_message::dfs_runtime_message_cache()
            .root(&_runtime.data_dir(service.node_id()).root())
            .expect("actual root cache slot");
        let cache = cache_slot.lock().expect("actual runtime message cache");
        cache
            .as_ref()
            .expect("mounted native cache entry")
            .bounded_status_resident_current()
            .expect("actual current resident generation")
    };
    service.metadata_backend = None;
    service = service
        .with_resident_metadata_shard_backend_lazy(
            resident
                .scope_shard(&file_system_id, &shard_id)
                .expect("actual exact shard")
                .checkout()
                .expect("actual exact checkout"),
        )
        .expect("same normal exact request shell");
    let registry = service.cross_shard_namespace_lock_registry();
    registry.upsert_job(
        Some(shard_id.clone()),
        "collapse-running-job".to_string(),
        file_system_id.clone(),
        "/services/collapse-running".to_string(),
    );
    service
        .metadata
        .add_object(object.clone())
        .expect("reconstructable cache of the committed object");
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
}

#[test]
fn sifs_cluster_posix_concurrent_creates_serialize_with_explicit_conflict() {
    let mut cluster = sifs_posix_cluster("sifs-cluster-race", "/users");
    let path = "/users/raced.txt";
    let writer_bytes = b"writer candidate content".to_vec();
    let observer_bytes = b"observer candidate bytes!".to_vec();
    assert_ne!(content_hash(&writer_bytes), content_hash(&observer_bytes));

    // Native Main processes real frames while callers run. Race two exclusive
    // creates with independent durable identities, rather than assuming the
    // old manual pump prevents the first blind overwrite from becoming visible.
    let writer_operation = sinomo_core::Id::generate().to_string();
    let observer_operation = sinomo_core::Id::generate().to_string();
    assert_ne!(writer_operation, observer_operation);
    let writer_options = sifs_cluster_write_options(&cluster.writer_node.clone());
    let observer_options = sifs_cluster_write_options(&cluster.observer_node.clone());
    let start = std::sync::Arc::new(std::sync::Barrier::new(2));
    let (writer_initial, observer_initial) = std::thread::scope(|scope| {
        let writer = &mut cluster.writer;
        let observer = &mut cluster.observer;
        let writer_start = std::sync::Arc::clone(&start);
        let observer_start = std::sync::Arc::clone(&start);
        let writer_operation = &writer_operation;
        let observer_operation = &observer_operation;
        let writer_bytes = &writer_bytes;
        let observer_bytes = &observer_bytes;
        let writer = scope.spawn(move || {
            writer_start.wait();
            writer.posix_create_file_with_operation_id(
                path,
                writer_bytes,
                writer_options,
                writer_operation,
            )
        });
        let observer = scope.spawn(move || {
            observer_start.wait();
            observer.posix_create_file_with_operation_id(
                path,
                observer_bytes,
                observer_options,
                observer_operation,
            )
        });
        (writer.join().unwrap(), observer.join().unwrap())
    });
    let pending_request = |result: &Result<DfsStat, DfsPosixError>| match result {
        Err(DfsPosixError::PendingWrite { intent_id, .. }) => Some(intent_id.clone()),
        // A native exclusive-create loser may be rejected before its first
        // effect. This is an actual typed result, not a fabricated write intent.
        Err(DfsPosixError::AlreadyExists { path: rejected }) if rejected == path => None,
        other => {
            panic!("exclusive create must be pending or explicitly already exists, got {other:?}")
        }
    };
    let writer_request = pending_request(&writer_initial);
    let observer_request = pending_request(&observer_initial);

    sifs_posix_cluster_pump_until(&mut cluster, "racing writes settle", |cluster| {
        let writer_settled = writer_request.as_ref().is_none_or(|request| {
            !matches!(
                cluster.writer.write_outcome(request),
                Ok(NetworkedDfsWriteOutcome::Pending { .. })
            )
        });
        let observer_settled = observer_request.as_ref().is_none_or(|request| {
            !matches!(
                cluster.observer.write_outcome(request),
                Ok(NetworkedDfsWriteOutcome::Pending { .. })
            )
        });
        writer_settled && observer_settled
    });

    let writer_outcome = writer_request
        .as_ref()
        .map(|request| cluster.writer.write_outcome(request));
    let observer_outcome = observer_request
        .as_ref()
        .map(|request| cluster.observer.write_outcome(request));
    let mut committed_hashes = Vec::new();
    let mut surfaced_failures = 0usize;
    for (initial, outcome) in [
        (&writer_initial, &writer_outcome),
        (&observer_initial, &observer_outcome),
    ] {
        match outcome {
            Some(Ok(NetworkedDfsWriteOutcome::Committed { content_hash, .. })) => {
                committed_hashes.push(content_hash.clone());
            }
            Some(Err(DfsError::AlreadyExists { path: rejected })) if rejected == path => {
                surfaced_failures += 1;
            }
            Some(Err(error)) => {
                panic!("racing exclusive create returned a non-conflict failure: {error:?}")
            }
            None => {
                assert!(matches!(initial,
                    Err(DfsPosixError::AlreadyExists { path: rejected }) if rejected == path));
                surfaced_failures += 1;
            }
            Some(Ok(NetworkedDfsWriteOutcome::Pending { .. })) => {
                panic!("racing write left pending after settle")
            }
        }
    }
    assert_eq!(
        (committed_hashes.len(), surfaced_failures),
        (1, 1),
        "exactly one racing create must commit and the other must reject without a conflict; \
         writer operation {writer_operation}: initial={writer_initial:?}, outcome={writer_outcome:?}; \
         observer operation {observer_operation}: initial={observer_initial:?}, outcome={observer_outcome:?}"
    );
    let winner_hash = committed_hashes.remove(0);
    assert!(
        winner_hash == content_hash(&writer_bytes) || winner_hash == content_hash(&observer_bytes),
        "winning content must be exactly one candidate, never interleaved"
    );

    // The primary's visible metadata must reference the winner's bytes.
    let primary_stat = cluster
        .primary
        .posix_stat_distributed(path)
        .expect("primary stat");
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

    drop(cluster);
}

#[test]
fn sifs_cluster_posix_truncate_shrinks_and_extends_with_zeros() {
    let mut cluster = sifs_posix_cluster("sifs-cluster-truncate", "/users");
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
            .posix_stat_distributed(path)
            .is_ok_and(|stat| stat.content_hash.as_deref() == Some(shrunk_hash.as_str()))
    });
    assert_eq!(
        cluster
            .writer
            .posix_read_file_distributed(path)
            .expect("shrunk read"),
        shrunk
    );
    assert_eq!(
        cluster
            .writer
            .posix_stat_distributed(path)
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
            .posix_stat_distributed(path)
            .is_ok_and(|stat| stat.content_hash.as_deref() == Some(extended_hash.as_str()))
    });
    assert_eq!(
        cluster
            .writer
            .posix_read_file_distributed(path)
            .expect("extended read"),
        extended
    );

    // Primary sees the final truncated content.
    let read = sifs_cluster_fetch_for_local_read(&mut cluster, |c| &mut c.primary, path);
    assert_eq!(read, extended);

    drop(cluster);
}

#[test]
fn sifs_cluster_posix_rename_replicates_atomically_to_observers() {
    let mut cluster = sifs_posix_cluster("sifs-cluster-rename", "/users");
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
        let runtime = &cluster._runtime;
        node_store_object(runtime, &cluster.observer_node, source).is_some()
            && node_store_object(runtime, &cluster.observer_node, destination).is_some()
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
            .posix_stat_distributed(destination)
            .expect("primary destination stat")
            .content_hash
            .as_deref(),
        Some(source_hash.as_str())
    );
    assert!(matches!(
        cluster.primary.posix_stat_distributed(source),
        Err(DfsPosixError::NotFound { .. })
    ));

    // Replica stores converge to the same post-rename namespace.
    sifs_cluster_publish_metadata_until(&mut cluster, "post-rename catch-up", |cluster| {
        let runtime = &cluster._runtime;
        let destination_current =
            node_store_content_hash(runtime, &cluster.observer_node, destination)
                .is_some_and(|hash| hash == source_hash);
        let source_gone = node_store_object(runtime, &cluster.observer_node, source).is_none();
        destination_current && source_gone
    });
    let entries = node_store_children(&cluster._runtime, &cluster.observer_node, "/users")
        .into_iter()
        .map(|(name, _, _)| format!("/users/{name}"))
        .collect::<Vec<_>>();
    assert!(
        entries.iter().any(|entry| entry == destination),
        "observer must list the rename destination: {entries:?}"
    );
    assert!(
        !entries.iter().any(|entry| entry == source),
        "observer must not list the rename source after replay: {entries:?}"
    );

    drop(cluster);
}

#[test]
fn sifs_cluster_posix_directory_rename_moves_children_on_observers() {
    let mut cluster = sifs_posix_cluster("sifs-cluster-dir-rename", "/users");
    let child_bytes = b"child file inside renamed directory".to_vec();
    let child_hash = content_hash(&child_bytes);

    cluster
        .primary
        .posix_mkdir("/users/project-a")
        .expect("primary mkdir");
    sifs_cluster_primary_posix_write(&mut cluster, "/users/project-a/notes.txt", &child_bytes);

    sifs_cluster_publish_metadata_until(&mut cluster, "pre-dir-rename catch-up", |cluster| {
        node_store_object(
            &cluster._runtime,
            &cluster.observer_node,
            "/users/project-a/notes.txt",
        )
        .is_some()
    });

    cluster
        .primary
        .posix_rename("/users/project-a", "/users/project-b")
        .expect("primary directory rename");
    assert_eq!(
        cluster
            .primary
            .posix_stat_distributed("/users/project-b/notes.txt")
            .expect("primary child stat")
            .content_hash
            .as_deref(),
        Some(child_hash.as_str())
    );
    assert!(matches!(
        cluster.primary.posix_stat_distributed("/users/project-a"),
        Err(DfsPosixError::NotFound { .. })
    ));

    sifs_cluster_publish_metadata_until(&mut cluster, "post-dir-rename catch-up", |cluster| {
        let runtime = &cluster._runtime;
        let child_moved = node_store_content_hash(
            runtime,
            &cluster.observer_node,
            "/users/project-b/notes.txt",
        )
        .is_some_and(|hash| hash == child_hash);
        let old_dir_gone =
            node_store_object(runtime, &cluster.observer_node, "/users/project-a").is_none();
        child_moved && old_dir_gone
    });
    assert!(
        node_store_object(
            &cluster._runtime,
            &cluster.observer_node,
            "/users/project-a/notes.txt"
        )
        .is_none()
    );
    let stat = node_store_object(
        &cluster._runtime,
        &cluster.observer_node,
        "/users/project-b",
    )
    .expect("observer renamed dir stat");
    assert_eq!(stat.kind, DfsObjectKind::Directory);

    drop(cluster);
}

#[test]
fn sifs_cluster_posix_namespace_and_stats_agree_across_nodes() {
    let mut cluster = sifs_posix_cluster("sifs-cluster-agree", "/users");
    let dir = "/users/team";
    let file_a = "/users/team/a.txt";
    let file_b = "/users/team/b.txt";
    let link = "/users/team/link";
    let a_bytes = b"alpha file content".to_vec();
    let b_bytes = b"bravo".to_vec();

    cluster.primary.posix_mkdir(dir).expect("primary mkdir");
    sifs_cluster_primary_posix_write(&mut cluster, file_a, &a_bytes);
    sifs_cluster_primary_posix_write(&mut cluster, file_b, &b_bytes);
    let mut link_options = sifs_cluster_write_options(&cluster.primary_node);
    link_options.wait_for_commit = true;
    cluster
        .primary
        .posix_symlink("a.txt", link, link_options)
        .expect("primary symlink");

    sifs_cluster_publish_metadata_until(&mut cluster, "namespace agreement", |cluster| {
        let runtime = &cluster._runtime;
        node_store_object(runtime, &cluster.observer_node, link).is_some()
            && node_store_object(runtime, &cluster.writer_node, link).is_some()
            && node_store_object(runtime, &cluster.observer_node, file_b).is_some()
            && node_store_object(runtime, &cluster.writer_node, file_b).is_some()
    });

    // Every node's store holds the same namespace identity as the primary's,
    // and the primary caller's listing projects that same identity.
    let primary_entries = cluster
        .primary
        .posix_list_dir_distributed(dir)
        .expect("primary list");
    let mut names = primary_entries
        .iter()
        .map(|entry| {
            (
                entry.name.clone(),
                entry.kind.clone(),
                entry.object_id.clone(),
            )
        })
        .collect::<Vec<_>>();
    names.sort_by(|left, right| left.0.cmp(&right.0));
    let primary_store = node_store_children(&cluster._runtime, &cluster.primary_node, dir);
    assert_eq!(names, primary_store);
    assert_eq!(
        primary_store,
        node_store_children(&cluster._runtime, &cluster.writer_node, dir)
    );
    assert_eq!(
        primary_store,
        node_store_children(&cluster._runtime, &cluster.observer_node, dir)
    );

    for path in [file_a, file_b] {
        let primary_stat = cluster
            .primary
            .posix_stat_distributed(path)
            .expect("primary stat");
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
        for (label, node) in [
            ("writer", &cluster.writer_node),
            ("observer", &cluster.observer_node),
        ] {
            let stat = node_store_object(&cluster._runtime, node, path)
                .unwrap_or_else(|| panic!("{label} stat"));
            assert_eq!(stat.object_id, primary_stat.object_id, "{label} {path}");
            assert_eq!(stat.kind, primary_stat.kind, "{label} {path}");
            assert_eq!(
                stat.content_size_bytes, primary_stat.size_bytes,
                "{label} {path}"
            );
            assert_eq!(
                stat.content_hash, primary_stat.content_hash,
                "{label} {path}"
            );
            assert_eq!(stat.version, primary_stat.version, "{label} {path}");
        }
    }

    // lstat reports the symlink itself without needing its content.
    let observer_lstat = cluster
        .observer
        .posix_lstat_distributed(link)
        .expect("observer lstat");
    assert_eq!(observer_lstat.kind, DfsObjectKind::Symlink);

    // The observer's caller keeps no local view; its distributed reads fetch
    // the link content to read the target and to follow the link.
    assert_eq!(
        cluster
            .observer
            .posix_readlink_distributed(link)
            .expect("readlink"),
        "a.txt"
    );
    let observer_stat = cluster
        .observer
        .posix_stat_distributed(link)
        .expect("observer stat");
    assert_eq!(observer_stat.kind, DfsObjectKind::File);
    assert_eq!(observer_stat.size_bytes, Some(a_bytes.len() as u64));

    // Following the symlink resolves to the same bytes on every node.
    let through_link = sifs_cluster_fetch_for_local_read(&mut cluster, |c| &mut c.observer, link);
    assert_eq!(through_link, a_bytes);

    drop(cluster);
}

#[test]
fn sifs_cluster_posix_error_mapping_matches_posix_contract() {
    let mut cluster = sifs_posix_cluster("sifs-cluster-errno", "/users");

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
        cluster
            .primary
            .posix_stat_distributed("/users/errno-file.txt/"),
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
        cluster
            .primary
            .posix_read_file_distributed("/users/errno-missing.txt"),
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
        cluster.primary.posix_stat_distributed("relative/path.txt"),
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

    drop(cluster);
}

#[test]
fn clustered_mkdir_all_fuses_request_admission_with_prepare() {
    let mut cluster = sifs_two_primary_cluster("sifs-mkdir-all-fused-admission");
    let path = "/services/fused-admission/child";
    let operation = DfsNamespaceOperation::MkdirAll {
        path: path.to_string(),
    };
    let requester = cluster.replica_node.clone();
    let primary = cluster.services_primary_node.clone();
    for (no_op, request_id) in [
        (false, "dfs_net_req_mkdir_all_fused_admission"),
        (true, "dfs_net_req_mkdir_all_fused_admission_noop"),
    ] {
        let owner_before = cluster
            ._runtime
            .metadata_primary_service(path)
            .expect("actual owner before the namespace RPC");
        assert_eq!(owner_before.node_id, primary);
        let before_sequence = owner_before
            .metadata_backend()
            .expect("actual owning backend")
            .journal_last_sequence()
            .expect("actual journal baseline");
        drop(owner_before);

        // The normal stable-ID route stamps the coherent configuration, and the
        // registered receiver supplies its real CurrentExecutionContext.
        let response = cluster
            .replica
            .route_namespace_operation_to_metadata_primary_with_request_id_and_options(
                request_id.to_string(),
                sifs_file_system_id(),
                primary.clone(),
                operation.clone(),
                crate::DFS_ROUTED_NAMESPACE_OP_TIMEOUT,
                false,
                false,
            )
            .expect("normal stamped mkdir-all RPC");
        assert_eq!(response.result, DfsNamespaceResult::Applied);

        let mut owner = cluster
            ._runtime
            .metadata_primary_service(path)
            .expect("actual owner after the namespace RPC");
        assert_eq!(owner.node_id, primary);
        let backend = owner.metadata_backend().expect("actual owning backend");
        let completed = backend
            .get_namespace_request(&requester, request_id)
            .expect("actual owner request lookup")
            .expect("completed request");
        assert_eq!(completed.status, DfsNamespaceRequestStatus::Completed);
        assert_eq!(completed.result, Some(DfsNamespaceResult::Applied));
        assert_eq!(completed.file_system_id, sifs_file_system_id());
        assert_eq!(completed.requester_node_id, requester);
        assert_eq!(completed.request_id, request_id);
        assert_eq!(completed.operation, operation);
        let matches_request = |request: &crate::DfsNamespaceRequestRecord| {
            request.file_system_id == completed.file_system_id
                && request.requester_node_id == requester
                && request.request_id == request_id
        };
        let journal = backend
            .journal_since(before_sequence)
            .expect("actual owner journal for this RPC");
        let prepares = journal
            .iter()
            .filter_map(|entry| match &entry.mutation {
                DfsMetadataMutation::PrepareOperationV7 { record, .. }
                    if record
                        .completed_request
                        .as_ref()
                        .is_some_and(matches_request)
                        || record.completed_request_completion.as_ref().is_some_and(
                            |completion| {
                                completion.file_system_id == completed.file_system_id
                                    && completion.requester_node_id == requester
                                    && completion.request_id == request_id
                            },
                        )
                        || record.metadata_effects.iter().any(|effect| {
                            matches!(effect,
                        crate::DfsPreparedMetadataEffect::PutNamespaceRequest { request }
                        if matches_request(request))
                        }) =>
                {
                    Some(record.as_ref())
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            prepares.len(),
            1,
            "one request-owned publication, without a separate Pending admission"
        );
        let prepare = prepares[0];
        prepare
            .validate()
            .expect("complete original request prepare");
        assert_eq!(prepare.file_system_id, completed.file_system_id);
        assert_eq!(prepare.shard_id, completed.authority_shard_id);
        assert!(
            backend
                .prepared_operation_for_request(&requester, request_id)
                .unwrap()
                .is_none()
        );
        if no_op {
            // Every current v7 semantic effect is sealed in prepare + commit.
            // The no-op has one direct Completed effect and no namespace body.
            assert!(prepare.transaction.objects.is_empty());
            assert!(prepare.transaction.versions.is_empty());
            assert!(prepare.completed_request.is_none());
            assert!(prepare.completed_request_completion.is_none());
            assert!(matches!(prepare.metadata_effects.as_slice(),
                [crate::DfsPreparedMetadataEffect::PutNamespaceRequest { request }]
                if request.as_ref() == &completed));
        } else {
            assert_eq!(prepare.completed_request.as_ref(), Some(&completed));
            assert!(prepare.completed_request_completion.is_none());
            assert!(prepare.metadata_effects.iter().all(|effect| !matches!(
                effect,
                crate::DfsPreparedMetadataEffect::PutNamespaceRequest { .. }
            )));
            let object = backend
                .get_object_by_path(&sifs_file_system_id(), &crate::metadata_path(path))
                .unwrap()
                .expect("actually created directory");
            assert!(prepare.transaction.objects.contains(&object));
        }
        let request_entries = journal
            .iter()
            .filter(|entry| match &entry.mutation {
                DfsMetadataMutation::PrepareOperationV7 { record, .. } => {
                    record.digest == prepare.digest
                }
                DfsMetadataMutation::CommitPreparedOperationV7 { digest, .. } => {
                    digest == &prepare.digest
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(
            request_entries.len(),
            2,
            "exact prepare + commit for this request"
        );
        assert_eq!(request_entries[0].sequence, prepare.digest.prepare_sequence);
        let terminal = backend
            .committed_prepared_operation(&prepare.digest)
            .unwrap()
            .expect("actual committed request digest");
        assert_eq!(&terminal, request_entries[1]);
        assert!(matches!(&terminal.mutation,
            DfsMetadataMutation::CommitPreparedOperationV7 { digest, .. }
            if digest == &prepare.digest));
        assert_eq!(
            owner.posix_stat(path).expect("created directory").kind,
            DfsObjectKind::Directory
        );
        drop(owner);
    }
    cluster.close();
}

#[test]
fn compound_posix_routing_refresh_invalidates_prior_config_revision() {
    let (runtime, nodes) =
        crate::test_runtime::DfsTestRuntime::start_cluster("native-compound-routing-refresh", 3)
            .expect("ordinary native startup");
    let shards = runtime.metadata_shards().unwrap();
    let users = shards.iter().find(|s| s.shard_id == "users").unwrap();
    let services = shards.iter().find(|s| s.shard_id == "services").unwrap();
    let users_primary = users.primary_node_id.clone();
    let old_services_primary = services.primary_node_id.clone();
    let requester = nodes
        .iter()
        .find(|n| **n != users_primary && **n != old_services_primary)
        .unwrap()
        .clone();
    let new_services_primary = nodes
        .iter()
        .find(|n| **n != requester && **n != old_services_primary)
        .unwrap()
        .clone();
    let mut dfs = runtime.client(&requester).unwrap();
    let counter = Arc::new(AtomicUsize::new(0));
    let actual = dfs.control_plane.take().unwrap();
    dfs = dfs.with_control_plane(CountingDfsControlPlaneKvClient::new(
        actual,
        Arc::clone(&counter),
    ));
    let range_prefix_calls = counter;

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

    let mut replicas = services
        .replica_node_ids
        .iter()
        .filter(|n| **n != new_services_primary)
        .cloned()
        .collect::<Vec<_>>();
    if !replicas.contains(&old_services_primary) {
        replicas.push(old_services_primary.clone());
    }
    let installed = runtime
        .reassign_metadata_shard(
            &services.file_system_id,
            "services",
            new_services_primary.clone(),
            replicas,
        )
        .expect("normal exact-prefix native role handoff");
    assert_eq!(installed.primary_node_id, new_services_primary);
    assert!(installed.epoch > services.epoch);

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

    drop(dfs);
    runtime.close().expect("ordinary native close");
}

#[test]
fn sifs_non_replica_directory_listing_reads_the_owner_without_ingest() {
    let cluster = sifs_outsider_cluster("sifs-outsider-directory-pull-capture");
    let SifsPosixCluster {
        primary_node,
        observer_node,
        mut primary,
        mut observer,
        _runtime,
        ..
    } = cluster;

    let dir = "/users/pulled-directory";
    let path = "/users/pulled-directory/remote.txt";
    primary.posix_mkdir_all(dir).expect("primary directory");
    primary
        .posix_write_file(
            path,
            b"remote directory entry",
            DfsWriteOptions::replicated_eventual(vec![primary_node.clone()]),
        )
        .expect("primary file");

    let configs = _runtime
        .metadata_shards()
        .expect("actual current assignment");
    let config = select_metadata_shard_config_for_path(&sifs_file_system_id(), &configs, dir)
        .unwrap()
        .unwrap();
    assert_eq!(config.primary_node_id, primary_node);
    assert!(!config.replica_node_ids.contains(&observer_node));
    assert_ne!(observer_node, primary_node);
    // The outsider's ordinary caller delegates to its Main and keeps no
    // store; the node's own store is inspected directly.
    assert!(observer.transport.delegates_local_namespace_authority());
    assert!(observer.metadata_backend().is_none());
    let shard = DfsMetadataShardId::new(config.shard_id.clone());
    let node_prefix = || {
        _runtime
            .node_metadata(&observer_node)
            .unwrap()
            .metadata_prefix_proof(&shard)
            .unwrap()
    };
    let before = node_prefix();
    let listed =
        observer.capture_mounted_mutation(|service| service.posix_list_dir_distributed(dir));
    let entries = listed
        .outcome
        .expect("outsider lists the directory from its owner");
    assert!(entries.iter().any(|entry| entry.path == path));
    assert!(
        !listed.committed_work.metadata_replica_progress,
        "a non-replica listing reads its owner and applies no journal"
    );
    assert_eq!(
        node_prefix(),
        before,
        "a non-replica listing leaves the node's copy of the shard untouched"
    );
    drop(observer);
    drop(primary);
    _runtime.close().expect("normal native owner drain");
}
