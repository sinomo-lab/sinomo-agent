//! Bootstrap regression tests.

use super::*;

#[test]
fn delegated_bootstrap_seeds_only_configuration_without_a_client_metadata_backend() {
    let node = crate::test_runtime::DfsTestRuntime::ordered_node_ids(
        "native-client-bootstrap-projection",
        1,
    )
    .remove(0);
    let (runtime, mut client) =
        native_sifs_service("native-client-bootstrap-projection", node.clone());
    assert!(client.transport.delegates_local_namespace_authority());
    let expected_file_system = client.metadata.filesystems[&sifs_file_system_id()].clone();
    let shards = runtime.metadata_shards().unwrap();
    let private_backend = client.metadata_backend.take().unwrap();
    let before = shards
        .iter()
        .map(|shard| {
            private_backend
                .export_shard_table_snapshot(&DfsMetadataShardId::new(shard.shard_id.clone()))
                .unwrap()
                .unwrap()
                .prefix_proof
        })
        .collect::<Vec<_>>();
    // Removing a client's optional private mirror cannot transfer Main's
    // bootstrap ownership into this unadmitted service handle.
    client.metadata = DfsMetadataStore::new();
    client
        .bootstrap_distributed_sifs()
        .expect("configuration-only client bootstrap");
    assert!(crate::placement::dfs_file_system_semantically_eq(
        &client.metadata.filesystems[&sifs_file_system_id()],
        &expected_file_system,
    ));
    assert!(
        client.metadata.objects.is_empty(),
        "client bootstrap must not synthesize namespace anchors"
    );
    assert!(
        client.metadata_backend.is_none(),
        "client bootstrap must not open an authority backend"
    );
    let mut owner_client = runtime.client(&node).unwrap();
    let after = shards
        .iter()
        .map(|shard| {
            owner_client
                .metadata_backend()
                .unwrap()
                .export_shard_table_snapshot(&DfsMetadataShardId::new(shard.shard_id.clone()))
                .unwrap()
                .unwrap()
                .prefix_proof
        })
        .collect::<Vec<_>>();
    assert_eq!(after, before, "only Main may author bootstrap metadata");
    let path = "/users/client-bootstrap-owner.bin";
    owner_client
        .posix_write_file(
            path,
            b"resident owner bytes",
            DfsWriteOptions::replicated_eventual(vec![node]),
        )
        .expect("normal Main still owns admission and publication");
    assert_eq!(
        owner_client.posix_read_file(path).unwrap(),
        b"resident owner bytes"
    );
    drop(owner_client);
    drop(client);
    drop(private_backend);
    runtime.close().unwrap();
}
#[test]
fn clustered_service_rejects_public_local_metadata_publication_apis() {
    let root = temp_root("clustered-local-publication-rejected");
    let local = NodeId::from_legacy_named("node_clustered_local_publication").expect("local");
    let object = file_metadata(local.clone());
    let object_id = object.object_id.clone();
    let version = object.version.clone();
    let mut dfs = NetworkedDfsService::new(
        crate::test_runtime::DfsTestRuntime::local_transport(local),
        LocalContentStore::new(root.join("content")).expect("content"),
    )
    .with_control_plane(SharedDfsControlPlaneKvClient::new(InProcessKvStore::new()));

    for error in [
        dfs.put_local_object_version(object.clone(), b"local bytes")
            .expect_err("clustered byte publication must use canonical write"),
        dfs.put_local_object_version_from_reader(
            object,
            std::io::Cursor::new(b"reader bytes".to_vec()),
        )
        .expect_err("clustered reader publication must use canonical write"),
    ] {
        assert!(
            matches!(error, DfsError::Distributed { ref message } if message.contains("standalone-only")),
            "unexpected clustered local-publication error: {error}"
        );
    }
    assert!(
        dfs.content()
            .read_manifest(object_id.as_str(), version.as_str())
            .is_err(),
        "rejected clustered publication must not stage content before authorization"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn cluster_dfs_message_command_processor_returns_none_without_cluster_state() {
    let root = temp_root("cluster_dfs_message_command_orphan");
    let data_dir = SinomoDataDir::new(&root);
    data_dir.ensure_root_layout().expect("root layout");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let local =
        NodeId::from_legacy_named("node_cluster_message_command_orphan_local").expect("local");
    let command = DfsMessageCommand {
        request_id: "dfs_message_command_orphan".to_string(),
        routed_message: DfsRoutedMessage {
            source_node_id: local.clone(),
            target_node_id: local.clone(),
            frame_id: 1,
            message_id: 1,
            message: DfsNetworkMessage::ReadRequest(DfsReadRequest {
                request_id: "orphan-read".to_string(),
                object_id: DfsObjectId::generate(),
                version: None,
                content_hash: None,
                content_size_bytes: None,
                range_offset: None,
                range_max_bytes: None,
            }),
        },
    };

    let result = process_cluster_dfs_message_command(&data_dir, &cluster_paths, command, None)
        .expect("cluster DFS message command");

    assert!(result.is_none());
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn cluster_dfs_message_command_processor_uses_cluster_local_node_id() {
    let root = temp_root("cluster_message_command_wrong_target");
    let data_dir = SinomoDataDir::new(&root);
    data_dir.ensure_root_layout().expect("root layout");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let now = OffsetDateTime::now_utc();
    let cluster_id = sinomo_core::node::ClusterId::from_legacy_named("cluster_dfs_message_command")
        .expect("cluster");
    let local = NodeId::from_legacy_named("node_cluster_message_local").expect("local");
    let remote = NodeId::from_legacy_named("node_cluster_message_remote").expect("remote");
    let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
        cluster_id,
        "dfs message command",
        local.clone(),
        "local",
        "pk-local",
        now,
    );
    state.members[0] = sinomo_core::cluster::local_cluster_state_authority_member(
        state.cluster_id.clone(),
        local.clone(),
        "local",
        "pk-local",
        now,
    );
    sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");
    sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
        .expect("cluster kv state");
    let command = DfsMessageCommand {
        request_id: "message-command-cluster-wrong-target".to_string(),
        routed_message: DfsRoutedMessage {
            source_node_id: remote.clone(),
            target_node_id: remote.clone(),
            frame_id: 1,
            message_id: 1,
            message: DfsNetworkMessage::ReadRequest(DfsReadRequest {
                request_id: "cluster-wrong-target-read".to_string(),
                object_id: DfsObjectId::generate(),
                version: None,
                content_hash: None,
                content_size_bytes: None,
                range_offset: None,
                range_max_bytes: None,
            }),
        },
    };

    let error = process_cluster_dfs_message_command(&data_dir, &cluster_paths, command, None)
        .expect_err("wrong target rejected");

    let error = error.to_string();
    assert!(
        error.contains(local.as_str()),
        "local id missing from: {error}"
    );
    assert!(
        error.contains(remote.as_str()),
        "remote id missing from: {error}"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn default_cluster_smoke_bootstraps_dfs_before_running_smoke() {
    let node = crate::test_runtime::DfsTestRuntime::ordered_node_ids("native-default-bootstrap", 1)
        .remove(0);
    let (runtime, mut service) = native_sifs_service("native-default-bootstrap", node.clone());
    for path in ["/", "/services", "/users"] {
        assert_eq!(
            service.posix_stat(path).unwrap().kind,
            DfsObjectKind::Directory
        );
    }
    let written = service
        .posix_write_file(
            "/users/native-bootstrap-smoke.bin",
            b"normal startup bytes",
            DfsWriteOptions::replicated_eventual(vec![node]),
        )
        .unwrap();
    assert_eq!(
        service.posix_read_file(&written.path).unwrap(),
        b"normal startup bytes"
    );
    drop(service);
    runtime.close().unwrap();
}

#[test]
fn cluster_dfs_metadata_publish_after_events_uses_default_cluster_publish() {
    let root = temp_root("cluster-publish-after-events");
    let data_dir = SinomoDataDir::new(&root);
    data_dir.ensure_root_layout().expect("layout");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let events = [NetworkedDfsServiceEvent::RemoteNamespaceServed {
        request_id: "namespace_cluster_publish_after_events".to_string(),
        requester_node_id: NodeId::from_legacy_named("node_cluster_publish_after_events").unwrap(),
        result: DfsNamespaceResult::Applied,
        mutates_metadata: true,
    }];
    let metadata_sender: DfsMetadataJournalBatchSender =
        Arc::new(|_, _, _| panic!("orphan cluster publish should not send metadata batches"));
    let snapshot_sender: DfsMetadataSnapshotInstallSender =
        Arc::new(|_, _, _| panic!("orphan cluster publish should not install metadata snapshots"));
    let mut reports = Vec::new();
    let mut errors = Vec::new();

    assert!(run_cluster_dfs_metadata_publish_after_events(
        &data_dir,
        &cluster_paths,
        metadata_sender,
        snapshot_sender,
        &events,
        DfsMetadataJournalReplicationOptions::default(),
        |report| reports.push(report),
        |error| errors.push(error),
    ));

    assert_eq!(
        reports,
        vec![DfsMetadataJournalReplicationReport {
            outcomes: Vec::new(),
        }]
    );
    assert!(errors.is_empty());
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn cold_sifs_scale_fixture_stays_preemptible() {
    let node =
        crate::test_runtime::DfsTestRuntime::ordered_node_ids("native-cold-scale", 1).remove(0);
    let (mut runtime, browser) = native_sifs_service("native-cold-scale", node.clone());
    drop(browser);
    // Populate the real one-node corpus through its actual Core author. The
    // boundary under test is cold hydration, not thousands of loopback RPCs.
    let mut writer = runtime.metadata_primary_service("/services").unwrap();
    let before_objects = writer
        .metadata_backend()
        .unwrap()
        .metadata_summary()
        .unwrap()
        .object_count;
    for index in 0..2048 {
        writer
            .posix_mkdir(&format!("/services/scale-fixture-{index:04}"))
            .unwrap();
    }
    assert_eq!(
        writer
            .metadata_backend()
            .unwrap()
            .metadata_summary()
            .unwrap()
            .object_count,
        before_objects + 2048
    );
    assert_eq!(runtime.observation_dropped_count(), 0);
    let configs = runtime.metadata_shards().unwrap();
    let fs = runtime.file_system().file_system_id;
    let shard = select_metadata_shard_config_for_path(&fs, &configs, "/services")
        .unwrap()
        .unwrap();
    drop(writer);
    runtime.stop_node(&node).unwrap();
    let backend = LocalSobjMetadataBackend::open_for_inspection(
        DfsMetadataShardId::new(shard.shard_id.clone()),
        default_dfs_metadata_shards_root(runtime.data_dir(&node)).join(&shard.shard_id),
    )
    .unwrap();
    let content =
        LocalContentStore::open_for_inspection(runtime.data_dir(&node).dfs_dir().join("content"));
    let mut service = NetworkedDfsService::new(
        crate::test_runtime::DfsTestRuntime::local_transport(node),
        content,
    );
    service.set_metadata_backend_unhydrated(backend);
    let cancellation_checks = AtomicUsize::new(0);
    let completed = service
        .hydrate_from_metadata_backend_for_commit_promotion_recovery_until_cancelled(&|| {
            cancellation_checks.fetch_add(1, Ordering::SeqCst) >= 1
        })
        .unwrap();
    assert!(!completed);
    assert!(cancellation_checks.load(Ordering::SeqCst) >= 2);
    drop(service);
    runtime.close().unwrap();
}

#[test]
fn sifs_opened_object_ranges_allow_one_atime_update_for_many_chunks() {
    let root = temp_root("sifs-opened-range-atime");
    let node_id = NodeId::from_legacy_named("node_sifs_opened_range_atime").expect("node");
    let file_path = "/services/sandbox/instances/sandbox_range_atime/work/file.txt";
    let bytes = b"content split across several FUSE reads";

    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_opened_object_ranges_allow_one_atime_update_for_many_chunks",
        node_id.clone(),
    );
    dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_range_atime/work")
        .expect("mkdir");
    let written = dfs
        .posix_write_file(
            file_path,
            bytes,
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("write");
    dfs.posix_set_times(file_path, Some(12_345), Some(67_890))
        .expect("old times");
    let before = dfs.posix_stat(file_path).expect("before stat");

    assert_eq!(
        dfs.posix_read_file_range_for_object(&written.object_id, 0, 7)
            .expect("first no-atime range"),
        &bytes[..7]
    );
    let after_no_atime = dfs.posix_stat(file_path).expect("no-atime stat");
    assert_eq!(after_no_atime.version, before.version);
    assert_eq!(after_no_atime.accessed_at_unix_nanos, 12_345);

    assert_eq!(
        dfs.posix_read_file_range_for_object_update_access(&written.object_id, 7, 9)
            .expect("first handle range updates atime"),
        &bytes[7..16]
    );
    let after_access = dfs.posix_stat(file_path).expect("updated atime stat");
    assert_ne!(after_access.version, before.version);
    assert!(after_access.accessed_at_unix_nanos > 12_345);
    assert_eq!(after_access.modified_at_unix_nanos, 67_890);

    assert_eq!(
        dfs.posix_read_file_range_for_object(&written.object_id, 16, 8)
            .expect("subsequent no-atime range"),
        &bytes[16..24]
    );
    let after_next_chunk = dfs.posix_stat(file_path).expect("next chunk stat");
    assert_eq!(
        after_next_chunk.version, after_access.version,
        "subsequent chunks must not append authoritative atime metadata"
    );

    let renamed_path = "/services/sandbox/instances/sandbox_range_atime/work/file-renamed.txt";
    dfs.posix_rename(file_path, renamed_path)
        .expect("rename opened file");
    assert_eq!(
        dfs.posix_read_file_range_for_opened_object(renamed_path, &written.object_id, 5, 6, false,)
            .expect("renamed opened-object range"),
        &bytes[5..11]
    );

    let local_node_id = dfs.node_id().clone();
    dfs.posix_write_file(
        file_path,
        b"replacement",
        DfsWriteOptions::replicated_eventual(vec![local_node_id]),
    )
    .expect("pathname replacement");
    assert!(matches!(
        dfs.posix_read_file_range_for_opened_object(file_path, &written.object_id, 0, 4, false,),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert_eq!(
        dfs.posix_read_file_range_for_opened_object(renamed_path, &written.object_id, 0, 4, false,)
            .expect("old handle keeps renamed identity"),
        &bytes[..4]
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_cluster_bulk_local_eventual_splits_mixed_primary_batch() {
    let mut cluster = sifs_two_primary_cluster("sifs-bulk-mixed-primary");
    let local_dir = "/users/bulk-mixed";
    let remote_dir = "/services/bulk-mixed";
    let local_path = "/users/bulk-mixed/local.txt";
    let remote_path = "/services/bulk-mixed/remote.txt";
    let local_bytes = b"bulk bytes owned by the local users shard".as_slice();
    let remote_bytes = b"bulk bytes owned by the remote services shard".as_slice();

    cluster
        .users_primary
        .posix_mkdir_all(local_dir)
        .expect("local bulk parent");
    cluster
        .services_primary
        .posix_mkdir_all(remote_dir)
        .expect("remote bulk parent");
    sifs_two_primary_publish_until(&mut cluster, "mixed bulk parents", |cluster| {
        cluster.users_primary.posix_stat(remote_dir).is_ok()
            && cluster.services_primary.posix_stat(local_dir).is_ok()
            && cluster.replica.posix_stat(local_dir).is_ok()
            && cluster.replica.posix_stat(remote_dir).is_ok()
    });

    let SifsTwoPrimaryCluster {
        _runtime,
        root,
        users_primary_node,
        services_primary_node,
        replica_node,
        shared_kv,
        mut users_primary,
        services_primary,
        replica,
        rejections,
    } = cluster;

    let stop = Arc::new(AtomicBool::new(false));
    let services_stop = Arc::clone(&stop);
    let services_thread =
        std::thread::spawn(move || pump_sifs_service_until_stop(services_primary, services_stop));

    let admitted_batches = Arc::new(Mutex::new(Vec::new()));
    let mut batch_hooks = Vec::new();
    for owner in [&users_primary_node, &services_primary_node] {
        let observed = Arc::clone(&admitted_batches);
        batch_hooks.push(crate::runtime_host::install_daemon_dfs_pre_dispatch_test_hook(
            _runtime.data_dir(owner),
            Arc::new(move |point, command, _| {
                if let crate::runtime_host::DaemonDfsPreDispatchTestPoint::BeforeExecute { admission_held } = point
                    && let Some(command) = command
                    && matches!(&command.routed_message.message,
                        DfsNetworkMessage::NamespaceRequest(request)
                        if matches!(&request.operation, DfsNamespaceOperation::CommitLocalEventualBatch { .. }))
                {
                    observed.lock().unwrap().push((
                        command.routed_message.source_node_id.clone(),
                        command.routed_message.target_node_id.clone(), admission_held,
                    ));
                }
                Ok(())
            }),
        ));
    }
    let stats = users_primary
        .posix_write_files_local_eventual_with_known_parents(vec![
            (local_path, local_bytes),
            (remote_path, remote_bytes),
        ])
        .expect("mixed-primary bulk write");
    drop(batch_hooks);
    let admitted = admitted_batches.lock().unwrap();
    assert_eq!(
        admitted.len(),
        2,
        "both local and remote groups must execute on their actual Main owner: {admitted:?}"
    );
    assert!(
        admitted
            .iter()
            .all(|(source, _, owned)| source == &users_primary_node && *owned)
    );
    assert_eq!(
        admitted
            .iter()
            .map(|(_, target, _)| target.clone())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([users_primary_node.clone(), services_primary_node.clone()])
    );
    drop(admitted);
    assert_eq!(stats.len(), 2);
    assert_eq!(stats[0].path, local_path);
    assert_eq!(
        stats[0].content_hash.as_deref(),
        Some(content_hash(local_bytes).as_str())
    );
    assert_eq!(stats[1].path, remote_path);
    assert_eq!(
        stats[1].content_hash.as_deref(),
        Some(content_hash(remote_bytes).as_str())
    );
    assert_eq!(
        users_primary
            .posix_read_file(local_path)
            .expect("read local bulk file"),
        local_bytes
    );
    assert_eq!(
        users_primary
            .posix_stat_distributed(remote_path)
            .expect("remote stat through primary")
            .content_hash
            .as_deref(),
        Some(content_hash(remote_bytes).as_str())
    );

    stop.store(true, Ordering::SeqCst);
    let services_primary = services_thread.join().expect("services thread");
    assert_eq!(
        services_primary
            .posix_stat(remote_path)
            .expect("remote primary stat")
            .content_hash
            .as_deref(),
        Some(content_hash(remote_bytes).as_str())
    );
    assert_eq!(
        users_primary
            .posix_stat(local_path)
            .expect("local primary stat")
            .content_hash
            .as_deref(),
        Some(content_hash(local_bytes).as_str())
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
        rejections,
    };
    sifs_two_primary_publish_until(&mut cluster, "mixed bulk convergence", |cluster| {
        cluster.replica.posix_stat(local_path).is_ok_and(|stat| {
            stat.content_hash.as_deref() == Some(content_hash(local_bytes).as_str())
        }) && cluster.replica.posix_stat(remote_path).is_ok_and(|stat| {
            stat.content_hash.as_deref() == Some(content_hash(remote_bytes).as_str())
        })
    });
    assert!(
        cluster.rejections.is_empty(),
        "mixed-primary bulk write must not fork metadata journals: {:?}",
        cluster.rejections
    );

    cluster.close();
}

#[test]
fn clustered_bootstrap_ensures_filesystem_only_on_authoritative_shard_and_probe_prefix() {
    let mut cluster = sifs_two_primary_cluster("filesystem-ensure-exact-authoritative-shard");
    let root_id = DfsMetadataShardId::new("root");
    let services_id = DfsMetadataShardId::new("services");
    let before_root = cluster
        .services_primary
        .metadata_backend()
        .unwrap()
        .export_shard_table_snapshot(&root_id)
        .unwrap()
        .unwrap()
        .prefix_proof;
    cluster
        .services_primary
        .bootstrap_distributed_sifs()
        .expect("bootstrap only locally authoritative namespace");
    let backend = cluster.services_primary.metadata_backend().unwrap();
    let after_root = backend
        .export_shard_table_snapshot(&root_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        after_root.prefix_proof, before_root,
        "local bootstrap must not author the non-primary root shard"
    );
    let services = backend
        .export_shard_table_snapshot(&services_id)
        .unwrap()
        .unwrap();
    assert!(
        services
            .tables
            .file_systems
            .iter()
            .any(|file_system| { file_system.file_system_id == sifs_file_system_id() }),
        "the actual services owner must retain its filesystem definition"
    );
    let batch = backend
        .journal_batch_for_shard_since_limited(
            &services_id,
            0,
            DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
        )
        .unwrap()
        .expect("actual bootstrap journal");
    let (prepare_entry, prepared) = batch
        .entries
        .iter()
        .find_map(|entry| match &entry.mutation {
            DfsMetadataMutation::PrepareOperationV7 { record, .. }
                if record.payload.metadata_effects.iter().any(|effect| {
                    matches!(effect,
                DfsPreparedMetadataEffect::PutFileSystem { file_system }
                    if file_system.file_system_id == sifs_file_system_id())
                }) =>
            {
                Some((entry, record))
            }
            _ => None,
        })
        .expect("filesystem is sealed in one complete services preparation");
    assert_eq!(prepared.payload.file_system_id, sifs_file_system_id());
    assert_eq!(prepared.payload.shard_id, services_id);
    assert_eq!(prepared.digest.prepare_sequence, prepare_entry.sequence);
    assert_eq!(
        prepared.digest.payload_digest,
        prepared.payload.canonical_digest().unwrap()
    );
    assert!(
        batch.entries.iter().any(|entry| matches!(&entry.mutation,
        DfsMetadataMutation::CommitPreparedOperationV7 { digest, .. }
            if digest == &prepared.digest && entry.sequence > prepare_entry.sequence)),
        "the complete filesystem preparation precedes its digest-only commit"
    );
    let sealed_body = prepared.payload.clone();
    let operation_id = prepared.digest.operation_id.clone();
    let expected_prefix = services.prefix_proof;
    sifs_two_primary_publish_until(&mut cluster, "complete bootstrap prefix", |cluster| {
        cluster
            .replica
            .metadata_backend()
            .unwrap()
            .export_shard_table_snapshot(&services_id)
            .unwrap()
            .is_some_and(|snapshot| snapshot.prefix_proof == expected_prefix)
    });
    let replicated = cluster
        .replica
        .metadata_backend()
        .unwrap()
        .prepared_operation_record(&operation_id)
        .unwrap()
        .expect("replicated sealed preparation");
    assert_eq!(
        replicated.payload, sealed_body,
        "the actual receiver prefix includes the full filesystem body"
    );
    assert_eq!(replicated.status, DfsPreparedOperationStatus::Committed);
    cluster.close();
}

#[test]
fn default_reconciliation_preserves_activated_metadata_layout_after_native_reopen() {
    let name = "native-persistent-metadata-layout";
    let (mut runtime, service, initial) =
        crate::test_support::current_native_metadata_service(name, 3, "/services").unwrap();
    drop(service);
    // These are the real initial members after native genesis and expansion.
    // No current assignment or acknowledgement is inserted by this fixture.
    let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 3);
    let members = std::iter::once(initial.primary_node_id.clone())
        .chain(initial.replica_node_ids.iter().cloned())
        .collect::<BTreeSet<_>>();
    assert_eq!(members, nodes.iter().cloned().collect());
    let new_primary = initial.replica_node_ids[0].clone();
    let activated = runtime
        .reassign_metadata_shard(
            &initial.file_system_id,
            &initial.shard_id,
            new_primary.clone(),
            vec![initial.primary_node_id.clone()],
        )
        .unwrap();
    assert_eq!(activated.primary_node_id, new_primary);
    assert_eq!(
        activated.replica_node_ids,
        vec![initial.primary_node_id.clone()]
    );
    assert!(activated.epoch > initial.epoch);
    let control = runtime.shared_control_plane().unwrap();
    let proof = control
        .with_client(|kv| {
            DfsControlPlaneStore::new(kv.as_mut()).get_metadata_shard_membership_handoff_proof(
                &activated.file_system_id,
                &activated.shard_id,
            )
        })
        .unwrap()
        .unwrap();
    assert_eq!(proof.desired_shard, activated);
    assert!(
        proof
            .acknowledged_node_ids
            .contains(&initial.primary_node_id)
    );
    assert!(proof.acknowledged_node_ids.contains(&new_primary));
    drop(control);
    // Normal stopped-owner startup necessarily runs the recurring default
    // reconciler; preserving a transient service view is insufficient.
    for node in nodes.iter().rev() {
        runtime.stop_node(node).unwrap();
    }
    runtime.start_nodes().unwrap();
    let restored = runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|config| config.shard_id == activated.shard_id)
        .unwrap();
    assert_eq!(restored, activated);
    let control = runtime.shared_control_plane().unwrap();
    let retained = control
        .with_client(|kv| {
            DfsControlPlaneStore::new(kv.as_mut()).get_metadata_shard_membership_handoff_proof(
                &activated.file_system_id,
                &activated.shard_id,
            )
        })
        .unwrap()
        .unwrap();
    assert_eq!(retained, proof);
    drop(control);
    let path = "/services/persistent-layout.bin";
    let mut policy = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 1).unwrap();
    policy.required_node_ids = vec![new_primary.clone()];
    policy.forbidden_node_ids = nodes
        .iter()
        .filter(|node| **node != new_primary)
        .cloned()
        .collect();
    runtime
        .apply_placement_policy(&new_primary, path, DfsPlacementScope::Subtree, policy)
        .unwrap();
    let mut client = runtime.client(&new_primary).unwrap();
    let (resolved, _, replication) = client
        .managed_replication_policy_for_path(path, DfsObjectPolicy::ReplicatedEventual, 1)
        .unwrap();
    assert_eq!(
        resolved.policy.consistency,
        DfsObjectPolicy::ReplicatedEventual
    );
    assert_eq!(replication.required_node_ids, vec![new_primary.clone()]);
    let written = client
        .posix_write_file(
            path,
            b"actual activated owner bytes",
            DfsWriteOptions {
                replica_candidates: vec![new_primary.clone()],
                policy: replication,
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    assert_eq!(
        client.posix_read_file_distributed(&written.path).unwrap(),
        b"actual activated owner bytes"
    );
    drop(client);
    runtime.close().unwrap();
}
