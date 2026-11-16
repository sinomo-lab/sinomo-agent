//! Replication regression tests.

use super::*;

#[test]
fn dfs_metadata_replication_retry_gate_tracks_incomplete_outcomes() {
    let node = NodeId::from_legacy_named("node_dfs_retry_gate").expect("node");
    let shard_id = DfsMetadataShardId::new("services");

    assert!(!dfs_metadata_replication_report_needs_retry(
        &DfsMetadataJournalReplicationReport {
            outcomes: vec![
                DfsMetadataJournalReplicationOutcome::Applied {
                    target_node_id: node.clone(),
                    shard_id: shard_id.clone(),
                    entries: 1,
                    next_after_sequence: 8,
                },
                DfsMetadataJournalReplicationOutcome::NoChanges {
                    target_node_id: node.clone(),
                    shard_id: shard_id.clone(),
                    after_sequence: 8,
                },
            ],
        }
    ));
    assert!(
        dfs_metadata_replication_report_needs_retry(&DfsMetadataJournalReplicationReport {
            outcomes: vec![DfsMetadataJournalReplicationOutcome::Failed {
                target_node_id: node.clone(),
                shard_id: shard_id.clone(),
                reason: "target timed out after applying the batch".to_string(),
            }],
        }),
        "an ambiguous post-send timeout must retain the exact lane under failure backoff"
    );
    assert!(dfs_metadata_replication_report_needs_retry(
        &DfsMetadataJournalReplicationReport {
            outcomes: vec![DfsMetadataJournalReplicationOutcome::BacklogRemaining {
                target_node_id: node,
                shard_id,
                after_sequence: 64,
                max_batches: 64,
            }],
        }
    ));
}

#[test]
fn missing_current_manifest_blob_returns_pending_replication() {
    let bytes = vec![b'm'; 17 * 1024];
    // The shard primary's executor would restore the withdrawn location from
    // its still-present bytes; hold it so the read meets exactly one location.
    let (runtime, dfs, stat, withdrawn, _executor_hold) =
        super::repair::native_withdrawn_local_replica_fixture_held(
            "native-read-missing-current-blob",
            &bytes,
        );
    let remaining = dfs
        .metadata_backend()
        .unwrap()
        .receipts_for_version(&stat.object_id, &stat.version)
        .unwrap();
    assert_eq!(remaining.len(), 1);
    let replica = remaining[0].replica_node_id.clone();
    assert_ne!(replica, withdrawn.replica_node_id);
    let hash = stat.content_hash.as_deref().unwrap();
    dfs.content()
        .debug_remove_loose_blob(hash)
        .expect("damage actual native cached bytes");
    assert!(dfs.content().read_blob(hash).is_err());
    let holder = runtime.client(&replica).unwrap();
    assert_eq!(holder.content().read_blob(hash).unwrap(), bytes);
    drop(holder);
    let outcome = dfs
        .read_local_object_with_state(&stat.object_id, false)
        .expect("missing blob is treated as cache miss");
    assert!(
        matches!(outcome, NetworkedDfsReadOutcome::PendingReplication {
        object_id, version, content_hash, current_locations,
    } if object_id == stat.object_id && version == stat.version
        && content_hash == stat.content_hash && current_locations == vec![replica])
    );
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn posix_conditional_replace_rejects_a_stale_editor_snapshot() {
    let root = temp_root("posix-conditional-replace-stale-editor");
    let node_id = NodeId::from_legacy_named("node_conditional_replace").expect("node");
    let path = "/documents/notes.txt";
    let (_runtime, mut dfs) = native_sifs_service(
        "posix_conditional_replace_rejects_a_stale_editor_snapshot",
        node_id.clone(),
    );
    dfs.posix_mkdir_all("/documents").expect("mkdir");
    let options = DfsWriteOptions::replicated_eventual(vec![node_id]);
    let opened = dfs
        .posix_write_file(path, b"opened", options.clone())
        .expect("create file");

    dfs.posix_write_file(path, b"newer", options.clone())
        .expect("concurrent write");
    let error = dfs
        .posix_replace_file_if_version(
            path,
            &opened.object_id,
            &opened.version,
            b"stale editor contents",
            options.clone(),
        )
        .expect_err("stale editor save must fail");
    assert!(matches!(error, DfsPosixError::Conflict { .. }));
    assert_eq!(
        dfs.posix_read_file_distributed(path)
            .expect("read newer version"),
        b"newer"
    );

    let current = dfs.posix_stat_distributed(path).expect("current stat");
    let saved = dfs
        .posix_replace_file_if_version(
            path,
            &current.object_id,
            &current.version,
            b"saved safely",
            options,
        )
        .expect("matching conditional save");
    assert_ne!(saved.version, current.version);
    assert_eq!(
        dfs.posix_read_file_distributed(path)
            .expect("read saved version"),
        b"saved safely"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sharded_point_lookup_snapshot_refreshes_only_the_routed_path_shard() {
    let root = temp_root("sifs-sharded-point-lookup-snapshot");
    let node = NodeId::from_legacy_named("node_sifs_sharded_point_snapshot").expect("node");
    let shards = sifs_two_primary_shard_configs(&node, &node, 1, Vec::new(), Vec::new());
    for shard in &shards {
        let physical = root.join("metadata").join(&shard.shard_id);
        std::fs::create_dir_all(&physical).unwrap();
        crate::test_runtime::copy_native_genesis_data(&physical, &shard.shard_id);
    }
    let mut backend = open_local_sharded_metadata_backend(root.join("metadata"), shards.clone())
        .expect("metadata");
    let file_system_id = sifs_file_system_id();

    crate::metadata_sharded_backend::reset_sharded_consistent_read_snapshot_shards();
    backend
        .begin_consistent_read_snapshot_for_path(&file_system_id, "/services/point-lookup-parent")
        .expect("path-scoped snapshot");
    backend.end_consistent_read_snapshot_for_path(&file_system_id, "/services/point-lookup-parent");

    assert_eq!(
        crate::metadata_sharded_backend::sharded_consistent_read_snapshot_shards(),
        vec![DfsMetadataShardId::new("services")],
        "a fresh routing generation has no historical Root fallback"
    );
    drop(backend);
    let historical_root = root.join("historical-metadata");
    for shard in &shards {
        let physical = historical_root.join(&shard.shard_id);
        std::fs::create_dir_all(&physical).unwrap();
        crate::test_runtime::copy_native_genesis_data(&physical, &shard.shard_id);
    }
    let old = vec![
        shards
            .iter()
            .find(|shard| shard.shard_id == "root")
            .unwrap()
            .clone(),
    ];
    drop(open_local_sharded_metadata_backend(&historical_root, old).unwrap());
    let mut backend = open_local_sharded_metadata_backend(&historical_root, shards).unwrap();
    crate::metadata_sharded_backend::reset_sharded_consistent_read_snapshot_shards();
    backend
        .begin_consistent_read_snapshot_for_path(&file_system_id, "/services/point-lookup-parent")
        .unwrap();
    backend.end_consistent_read_snapshot_for_path(&file_system_id, "/services/point-lookup-parent");
    assert_eq!(
        crate::metadata_sharded_backend::sharded_consistent_read_snapshot_shards(),
        vec![
            DfsMetadataShardId::new("services"),
            DfsMetadataShardId::new("root")
        ],
        "actual retained Root routing must be included without refreshing unrelated users"
    );
    drop(backend);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn stat_projection_snapshot_pins_only_exact_shard_with_retained_ancestor() {
    let root = temp_root("stat-exact-shard-snapshot");
    let node = NodeId::from_legacy_named("stat_exact_shard_snapshot").unwrap();
    let shards = sifs_two_primary_shard_configs(&node, &node, 1, Vec::new(), Vec::new());
    let original = shards
        .iter()
        .find(|shard| shard.shard_id == "root")
        .unwrap()
        .clone();
    for shard in &shards {
        let physical = root.join(&shard.shard_id);
        std::fs::create_dir_all(&physical).unwrap();
        crate::test_runtime::copy_native_genesis_data(&physical, &shard.shard_id);
    }
    drop(open_local_sharded_metadata_backend(&root, vec![original]).unwrap());
    let mut backend = open_local_sharded_metadata_backend(&root, shards).unwrap();
    let selected = DfsMetadataShardId::new("services");
    crate::metadata_sharded_backend::reset_sharded_consistent_read_snapshot_shards();
    backend
        .begin_consistent_read_snapshot_on_shard(&selected)
        .unwrap();
    assert!(
        backend
            .metadata_prefix_proof(&selected)
            .unwrap()
            .unwrap()
            .is_well_formed()
    );
    backend.end_consistent_read_snapshot_on_shard(&selected);
    assert_eq!(
        crate::metadata_sharded_backend::sharded_consistent_read_snapshot_shards(),
        vec![selected]
    );
    crate::metadata_sharded_backend::reset_sharded_consistent_read_snapshot_shards();
    assert!(
        backend
            .begin_consistent_read_snapshot_on_shard(&DfsMetadataShardId::new(
                "missing-stat-authority"
            ))
            .is_err()
    );
    assert!(crate::metadata_sharded_backend::sharded_consistent_read_snapshot_shards().is_empty());
}

#[test]
fn sifs_cluster_posix_replica_catches_up_after_journal_publish() {
    let mut cluster = sifs_posix_cluster("sifs-cluster-catchup", "/users");
    let path = "/users/replicated.txt";
    let bytes = b"observer should converge to this content";
    let original = cluster
        ._runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|shard| shard.shard_id == "users")
        .unwrap();
    // A configured participant must ACK every prepare. Establish the intended
    // initially uninvolved observer through a real membership transition.
    let initial = cluster
        ._runtime
        .reassign_metadata_shard(
            &original.file_system_id,
            &original.shard_id,
            cluster.primary_node.clone(),
            vec![cluster.writer_node.clone()],
        )
        .unwrap();
    assert!(!initial.replica_node_ids.contains(&cluster.observer_node));
    let mut policy = DfsReplicationPolicy::new(1, 1).unwrap();
    policy.required_node_ids = vec![cluster.primary_node.clone()];
    policy.forbidden_node_ids = vec![cluster.observer_node.clone()];
    cluster
        .writer
        .posix_write_file(
            path,
            bytes,
            DfsWriteOptions {
                policy,
                replica_candidates: vec![cluster.primary_node.clone()],
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .unwrap();
    // The unassigned observer's own store must not hold the path; its
    // delegating client keeps no local view to ask.
    let observer_root =
        crate::default_dfs_metadata_shards_root(cluster._runtime.data_dir(&cluster.observer_node))
            .join(crate::metadata_keys::key_segment(&original.shard_id));
    let before = LocalSobjMetadataBackend::open_for_inspection(
        DfsMetadataShardId::new(&original.shard_id),
        &observer_root,
    )
    .unwrap();
    assert!(
        before
            .get_object_by_path(&original.file_system_id, path)
            .unwrap()
            .is_none(),
        "the actual unassigned observer must not have the unpublished-to-it path"
    );
    drop(before);
    assert!(
        !cluster
            .observer
            .content()
            .has_blob(&content_hash(bytes))
            .unwrap()
    );

    // The existing native catch-up protocol publishes the real prefix before
    // joint activation adds this observer to the metadata participant set.
    let joined = cluster
        ._runtime
        .reassign_metadata_shard(
            &initial.file_system_id,
            &initial.shard_id,
            cluster.primary_node.clone(),
            vec![cluster.writer_node.clone(), cluster.observer_node.clone()],
        )
        .unwrap();
    assert_eq!(joined.epoch, initial.epoch + 1);
    assert!(joined.replica_node_ids.contains(&cluster.observer_node));
    drop(cluster.observer);
    cluster.observer = cluster._runtime.client(&cluster.observer_node).unwrap();
    let expected_hash = content_hash(bytes);
    // The joined observer node's own store caught up.
    let stat = node_store_object(&cluster._runtime, &cluster.observer_node, path)
        .expect("actual caught-up replica stat");
    assert_eq!(stat.kind, DfsObjectKind::File);
    assert_eq!(stat.content_hash.as_deref(), Some(expected_hash.as_str()));
    assert_eq!(stat.content_size_bytes, Some(bytes.len() as u64));
    let entries = node_store_children(&cluster._runtime, &cluster.observer_node, "/users");
    assert!(entries.iter().any(
        |(name, _, object_id)| *object_id == stat.object_id && format!("/users/{name}") == path
    ));
    assert!(
        !cluster.observer.content().has_blob(&expected_hash).unwrap(),
        "metadata catch-up must not invent a content replica"
    );
    let read = sifs_cluster_fetch_for_local_read(&mut cluster, |c| &mut c.observer, path);
    assert_eq!(read, bytes.to_vec());
    let publication = cluster
        ._runtime
        .maintenance_tick(
            &cluster.primary_node,
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
        )
        .unwrap();
    assert!(publication.outcome.is_ok(), "{:?}", publication.outcome);
    assert_eq!(publication.omitted_kv_event_count, 0);
    sifs_posix_cluster_pump_round(&mut cluster);
    assert_eq!(
        node_store_content_hash(&cluster._runtime, &cluster.observer_node, path),
        Some(expected_hash.clone()),
        "idempotent publication must not regress the actual replica"
    );
    assert_eq!(
        cluster.writer.posix_read_file_distributed(path).unwrap(),
        bytes
    );
    assert!(cluster.rejections.is_empty(), "{:?}", cluster.rejections);
    let SifsPosixCluster {
        primary,
        writer,
        observer,
        _runtime,
        ..
    } = cluster;
    drop((primary, writer, observer));
    _runtime.close().unwrap();
}

#[test]
fn routed_namespace_without_local_replay_omits_response_journal_batches() {
    let mut cluster = sifs_two_primary_cluster("native-namespace-no-response-replay");
    let cfg = cluster
        ._runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|cfg| cfg.shard_id == "services")
        .unwrap();
    let command = DfsMessageCommand {
        request_id: "native-no-replay".into(),
        routed_message: DfsRoutedMessage {
            source_node_id: cluster.replica_node.clone(),
            target_node_id: cluster.services_primary_node.clone(),
            frame_id: 1,
            message_id: 1,
            message: DfsNetworkMessage::NamespaceRequest(DfsNamespaceCommand {
                request_id: "native-no-replay".into(),
                file_system_id: sifs_file_system_id(),
                requesting_node_id: cluster.replica_node.clone(),
                include_metadata_journal_batches: false,
                metadata_journal_after_sequences: Vec::new(),
                authority_shards: vec![cfg],
                operation: DfsNamespaceOperation::MkdirAll {
                    path: "/services/no-replay-batches".into(),
                },
            }),
        },
    };
    let request =
        ObjectRpcRequest::encode_body(DFS_DATA_NAMESPACE_ROUTE, DFS_RPC_CATEGORY, &command)
            .unwrap();
    let reply: DfsMessageResponse = cluster
        ._runtime
        .rpc_request(
            &cluster.replica_node,
            &cluster.services_primary_node,
            request,
            DEFAULT_DFS_POSIX_SYNC_TIMEOUT,
        )
        .unwrap()
        .decode_body()
        .unwrap();
    let responses = reply
        .outbound_messages
        .iter()
        .filter_map(|message| match &message.message {
            DfsNetworkMessage::NamespaceResponse(response) => Some(response),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(responses.len(), 1);
    assert!(matches!(responses[0].result, DfsNamespaceResult::Applied));
    assert!(responses[0].metadata_journal_batches.is_empty());
    assert!(
        responses[0].synced_path_object.is_none() && responses[0].synced_path_objects.is_empty()
    );
    assert_eq!(
        cluster
            .replica
            .posix_stat_distributed("/services/no-replay-batches")
            .unwrap()
            .kind,
        DfsObjectKind::Directory
    );
    cluster.close();
}

#[test]
fn durable_namespace_exact_retry_replays_completed_rename_and_rejects_id_reuse() {
    let mut cluster = sifs_two_primary_cluster("native-namespace-rename-replay");
    let from = "/services/replay-source";
    let to = "/services/replay-destination";
    let other = "/services/replay-changed-destination";
    let original = cluster
        .replica
        .posix_write_file(
            from,
            b"namespace replay payload",
            DfsWriteOptions::replicated_eventual(vec![cluster.replica_node.clone()]),
        )
        .unwrap();
    let id = sinomo_core::Id::generate().to_string();
    let first = cluster
        .replica
        .posix_rename_with_operation_id(from, to, &id)
        .unwrap();
    let owner = cluster.services_primary_node.clone();
    drop(cluster.services_primary);
    cluster._runtime.stop_node(&owner).unwrap();
    cluster._runtime.reopen_node(owner.clone()).unwrap();
    cluster.services_primary = cluster._runtime.client(&owner).unwrap();
    let replay = cluster
        .replica
        .posix_rename_with_operation_id(from, to, &id)
        .unwrap();
    assert_eq!(replay.object_id, original.object_id);
    assert_eq!(replay.version, first.version);
    assert_eq!(
        cluster.replica.posix_read_file_distributed(to).unwrap(),
        b"namespace replay payload"
    );
    assert!(
        cluster
            .replica
            .posix_rename_with_operation_id(to, other, &id)
            .is_err()
    );
    assert_eq!(
        cluster.replica.posix_stat_distributed(to).unwrap().version,
        first.version
    );
    assert!(matches!(
        cluster.replica.posix_stat_distributed(other),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert!(matches!(
        cluster.replica.posix_stat_distributed(from),
        Err(DfsPosixError::NotFound { .. })
    ));
    cluster.close();
}

#[test]
fn delegating_replica_cursor_snapshot_advertises_nothing_without_backend_status() {
    // Only Main ingests a shard's journal into the node's store, so a caller
    // delegating to it advertises no routed cursor even after genuine progress
    // on a shard its node replicates, and never scans backend shard status.
    let mut cluster = sifs_two_primary_cluster("sifs-namespace-cached-cursors");
    let shard = DfsMetadataShardId::new("services");
    assert!(
        cluster
            .replica
            .transport
            .delegates_local_namespace_authority()
    );
    crate::metadata_sharded_backend::take_sharded_metadata_status_query_count();
    assert!(
        cluster
            .replica
            .local_metadata_shard_cursors()
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        crate::metadata_sharded_backend::take_sharded_metadata_status_query_count(),
        0
    );
    let mut author = cluster
        ._runtime
        .metadata_primary_service("/services")
        .unwrap();
    author
        .posix_mkdir_all("/services/actual-cursor-progress")
        .unwrap();
    let synced = author.posix_sync_directory("/services").unwrap();
    let actual = synced.metadata_certificate.target.journal_sequence;
    drop(author);
    let cfg = cluster
        ._runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|cfg| cfg.shard_id == shard.0)
        .unwrap();
    assert!(cfg.replica_node_ids.contains(&cluster.replica_node));
    crate::metadata_sharded_backend::take_sharded_metadata_status_query_count();
    assert!(
        cluster
            .replica
            .local_metadata_shard_cursors_for(Some(std::slice::from_ref(&cfg)))
            .unwrap()
            .is_empty(),
        "a delegating caller advertises no journal position"
    );
    assert_eq!(
        cluster
            .replica
            .local_metadata_journal_after_sequence_for_shard(&cfg)
            .unwrap(),
        None
    );
    assert!(
        cluster
            .replica
            .local_metadata_shard_cursors()
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        crate::metadata_sharded_backend::take_sharded_metadata_status_query_count(),
        0,
        "quiet routed cursor reads must not scan backend shard status"
    );
    // The progress itself reached the replica node's store through its Main.
    sifs_two_primary_publish_until(&mut cluster, "replica store progress", |cluster| {
        cluster
            ._runtime
            .node_metadata(&cluster.replica_node)
            .unwrap()
            .metadata_prefix_proof(&shard)
            .unwrap()
            .is_some_and(|proof| proof.journal_sequence >= actual)
    });
    cluster.close();
}

#[test]
fn namespace_rename_shard_selection_uses_one_config_snapshot_for_both_paths() {
    let (runtime, nodes) =
        crate::test_runtime::DfsTestRuntime::start_cluster("native-rename-one-snapshot", 3)
            .expect("ordinary native startup");
    let mut dfs = runtime.client(&nodes[0]).unwrap();
    let range_prefix_calls = Arc::new(AtomicUsize::new(0));
    let actual = dfs.control_plane.take().unwrap();
    dfs = dfs.with_control_plane(CountingDfsControlPlaneKvClient::new(
        actual,
        Arc::clone(&range_prefix_calls),
    ));

    let affected = dfs
        .affected_namespace_shards(
            &sifs_file_system_id(),
            &DfsNamespaceOperation::Rename {
                from: "/users/source.txt".to_string(),
                to: "/services/destination.txt".to_string(),
            },
        )
        .expect("rename shard selection");
    let affected_ids = affected
        .iter()
        .map(|shard| shard.shard_id.as_str())
        .collect::<BTreeSet<_>>();

    assert_eq!(affected_ids, BTreeSet::from(["services", "users"]));
    assert_eq!(
        range_prefix_calls.load(Ordering::SeqCst),
        1,
        "all rename paths and parents must be selected from one immutable config snapshot"
    );

    drop(dfs);
    runtime.close().expect("ordinary native close");
}

#[test]
fn delegating_caller_on_a_replica_advertises_no_journal_position() {
    let name = "native-delegating-caller-no-journal-position";
    let (runtime, primary_service, config) =
        crate::test_support::current_native_metadata_service(name, 3, "/services").unwrap();
    drop(primary_service);
    let replica = config.replica_node_ids[0].clone();
    let mut delegating = runtime.client(&replica).unwrap();
    assert!(delegating.transport.delegates_local_namespace_authority());
    assert_eq!(
        delegating
            .local_metadata_journal_after_sequence_for_shard(&config)
            .unwrap(),
        None,
        "only Main ingests response batches into the node's store"
    );
    drop(delegating);
    runtime.close().unwrap();
}

#[test]
fn delegating_client_truncates_right_after_its_own_write_at() {
    let name = "native-delegating-write-at-then-truncate";
    let (runtime, mut primary_service, config) =
        crate::test_support::current_native_metadata_service(name, 3, "/services").unwrap();
    let primary = config.primary_node_id.clone();
    let replica = config.replica_node_ids[0].clone();
    let path = "/services/patch-then-truncate.txt";
    let created = primary_service
        .posix_write_file(
            path,
            b"0123456789",
            DfsWriteOptions::replicated_eventual(vec![primary.clone()]),
        )
        .unwrap();
    drop(primary_service);
    let mut client = runtime.client(&replica).unwrap();
    assert!(client.transport.delegates_local_namespace_authority());
    let options = DfsWriteOptions::replicated_eventual(vec![primary.clone()]);
    client
        .posix_write_file_at(path, 2, b"ab", options.clone())
        .unwrap();
    let truncated = client
        .posix_truncate_file_for_object(&created.object_id, 4, options)
        .expect("the patched version is readable for the rewrite");
    assert_eq!(truncated.size_bytes, Some(4));
    assert_eq!(client.posix_read_file_distributed(path).unwrap(), b"01ab");
    drop(client);
    runtime.close().unwrap();
}

#[test]
fn outsider_patches_symlinks_and_stats_through_the_primary() {
    let name = "native-outsider-patch-through-primary";
    let (runtime, mut primary_service, config) =
        crate::test_support::current_native_metadata_service(name, 3, "/services").unwrap();
    let primary = config.primary_node_id.clone();
    let outsider = config.replica_node_ids[0].clone();
    let path = "/services/outsider-patch.txt";
    let created = primary_service
        .posix_write_file(
            path,
            b"0123456789",
            DfsWriteOptions::replicated_eventual(vec![primary.clone()]),
        )
        .unwrap();
    drop(primary_service);
    let remaining = config
        .replica_node_ids
        .iter()
        .filter(|node| **node != outsider)
        .cloned()
        .collect();
    runtime
        .reassign_metadata_shard(
            &config.file_system_id,
            &config.shard_id,
            primary.clone(),
            remaining,
        )
        .unwrap();
    let outsider_row = || {
        LocalSobjMetadataBackend::open_for_inspection(
            DfsMetadataShardId::new(config.shard_id.clone()),
            crate::default_dfs_metadata_shards_root(runtime.data_dir(&outsider))
                .join(crate::metadata_keys::key_segment(&config.shard_id)),
        )
        .unwrap()
        .get_object(&created.object_id)
        .unwrap()
    };
    let deadline = Instant::now() + Duration::from_secs(30);
    while outsider_row().is_some() {
        assert!(Instant::now() < deadline, "the outsider kept its copy");
        runtime
            .maintenance_tick(&outsider, sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
            .unwrap();
    }

    let mut client = runtime.client(&outsider).unwrap();
    assert!(client.transport.delegates_local_namespace_authority());
    let options = DfsWriteOptions::replicated_eventual(vec![primary.clone()]);
    let truncated = client
        .posix_truncate_file_for_object(&created.object_id, 4, options.clone())
        .expect("truncate reads its base from the primary");
    assert_eq!(truncated.size_bytes, Some(4));
    assert_eq!(truncated.link_count, 1);
    let patched = client
        .posix_write_file_at(path, 2, b"ab", options.clone())
        .expect("write-at reads its base from the primary");
    assert_eq!(patched.object_id, created.object_id);
    assert_eq!(patched.size_bytes, Some(4));
    let link = client
        .posix_symlink("outsider-patch.txt", "/services/outsider-link", options)
        .expect("symlink checks its parent at the primary");
    assert_eq!(link.kind, DfsObjectKind::Symlink);
    let by_id = client
        .posix_stat_object(&created.object_id)
        .unwrap()
        .expect("stat by object id reads the primary");
    assert_eq!(by_id.size_bytes, Some(4));
    assert_eq!(by_id.link_count, 1);
    assert!(outsider_row().is_none(), "the outsider keeps no copy");
    drop(client);
    runtime.close().unwrap();
}

#[test]
fn removed_replica_discards_its_shard_copy_and_recovers_when_added_back() {
    let name = "native-removed-replica-discards-copy";
    let (runtime, mut primary_service, config) =
        crate::test_support::current_native_metadata_service(name, 3, "/services").unwrap();
    let primary = config.primary_node_id.clone();
    let removed = config.replica_node_ids[0].clone();
    let path = "/services/discarded-copy.txt";
    let stat = primary_service
        .posix_write_file(
            path,
            b"replicated row",
            DfsWriteOptions::replicated_eventual(vec![primary.clone()]),
        )
        .unwrap();
    drop(primary_service);
    let removed_row = || {
        let shard_root = crate::default_dfs_metadata_shards_root(runtime.data_dir(&removed))
            .join(crate::metadata_keys::key_segment(&config.shard_id));
        LocalSobjMetadataBackend::open_for_inspection(
            DfsMetadataShardId::new(config.shard_id.clone()),
            shard_root,
        )
        .unwrap()
        .get_object(&stat.object_id)
        .unwrap()
    };
    let settle = |nodes: &[&NodeId], done: &dyn Fn() -> bool, what: &str| {
        let deadline = Instant::now() + Duration::from_secs(30);
        while !done() {
            assert!(Instant::now() < deadline, "{what} did not settle");
            for node in nodes {
                runtime
                    .maintenance_tick(node, sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
                    .unwrap();
            }
        }
    };
    settle(
        &[&primary, &removed],
        &|| removed_row().is_some(),
        "replication",
    );

    let remaining = config
        .replica_node_ids
        .iter()
        .filter(|node| **node != removed)
        .cloned()
        .collect();
    runtime
        .reassign_metadata_shard(
            &config.file_system_id,
            &config.shard_id,
            primary.clone(),
            remaining,
        )
        .unwrap();
    settle(
        &[&removed],
        &|| removed_row().is_none(),
        "discarding the removed copy",
    );

    runtime
        .reassign_metadata_shard(
            &config.file_system_id,
            &config.shard_id,
            primary.clone(),
            config.replica_node_ids.clone(),
        )
        .unwrap();
    settle(
        &[&primary, &removed],
        &|| removed_row().is_some_and(|row| row.version == stat.version),
        "re-adding the replica",
    );
    runtime.close().unwrap();
}

#[test]
fn lookup_response_object_updates_outsider_read_view_without_journal_batches() {
    let name = "native-sync-path-outsider";
    let (runtime, initial, config) =
        crate::test_support::current_native_metadata_service(name, 3, "/services").unwrap();
    drop(initial);
    let owner = config.primary_node_id;
    let peers = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 3)
        .into_iter()
        .filter(|node| *node != owner)
        .collect::<Vec<_>>();
    let outsider = peers[1].clone();
    let current = runtime
        .reassign_metadata_shard(
            &config.file_system_id,
            &config.shard_id,
            owner.clone(),
            vec![peers[0].clone()],
        )
        .unwrap();
    assert!(!current.replica_node_ids.contains(&outsider));
    let path = "/services/native-outsider-current.bin";
    let mut policy = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 1, 1).unwrap();
    policy.required_node_ids = vec![owner.clone()];
    runtime
        .apply_placement_policy(&owner, path, DfsPlacementScope::Subtree, policy)
        .unwrap();
    let mut reader = runtime.client(&outsider).unwrap();
    assert!(
        reader
            .metadata()
            .object_by_normalized_posix_path(&config.file_system_id, path)
            .is_none()
    );
    let mut writer = runtime.client(&owner).unwrap();
    let stat = writer
        .posix_write_file(
            path,
            b"actual current synced path",
            DfsWriteOptions {
                replica_candidates: vec![owner.clone()],
                policy: DfsReplicationPolicy::new(1, 1).unwrap(),
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .unwrap();
    drop(writer);
    let local_snapshot = || {
        // An outsider may have no certified local prefix. Compare the actual
        // observational rows and optional cut; never manufacture an exportable
        // current snapshot merely to inspect an absent replica.
        let root = crate::default_dfs_metadata_shards_root(runtime.data_dir(&outsider))
            .join(crate::metadata_keys::key_segment(&"services"));
        let state = crate::metadata_table_store::read_metadata_table_state_lenient(&root).unwrap();
        (crate::metadata_table_store::metadata_tables_snapshot(&state.tables), state.prefix_proof)
    };
    let unchanged = local_snapshot();
    let before = runtime.rpc_observations().len();
    let response = reader
        .route_namespace_operation_to_metadata_primary_with_timeout_and_options(
            config.file_system_id.clone(),
            owner.clone(),
            DfsNamespaceOperation::LookupPath {
                path: path.into(),
                inline_range: None,
            },
            SIFS_CLUSTER_PUMP_DEADLINE,
            false,
            true,
        )
        .unwrap();
    assert_eq!(response.result, DfsNamespaceResult::Applied);
    assert!(!response.metadata_journal_has_more);
    let actual = response.synced_path_object.unwrap();
    assert_eq!(actual.object_id, stat.object_id);
    assert_eq!(actual.version, stat.version);
    assert_eq!(actual.content_hash, stat.content_hash);
    assert_eq!(
        local_snapshot(),
        unchanged,
        "a reply object does not grant outsider journal ingestion"
    );
    let public = reader.posix_stat_distributed(path).unwrap();
    assert_eq!(public.object_id, actual.object_id);
    assert_eq!(public.version, actual.version);
    assert_eq!(public.content_hash, actual.content_hash);
    assert!(
        reader
            .metadata()
            .object_by_normalized_posix_path(&config.file_system_id, path)
            .is_none(),
        "Main owns persistence; public current reads do not invent a private replica"
    );
    assert_eq!(local_snapshot(), unchanged);
    assert!(
        runtime.rpc_observations()[before..]
            .iter()
            .any(|event| event.source == outsider
                && event.target == owner
                && event.route == crate::DFS_DATA_NAMESPACE_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::Completed)
    );
    drop(reader);
    runtime.close().unwrap();
}

// Capture the actual equal native prefix before producing any corpus entry.
// Native startup may already have checkpointed its genesis frames; those are
// not the 700 writes whose complete byte lineage this fixture must retain.
fn native_backlog_anchor(
    runtime: &crate::test_runtime::DfsTestRuntime,
    primary: &NetworkedDfsService<DfsNodeTransport>,
    replica: &NodeId,
) -> crate::DfsMetadataPrefixProof {
    let shard = DfsMetadataShardId::new("users");
    let anchor = primary
        .metadata_backend()
        .unwrap()
        .metadata_prefix_proof(&shard)
        .unwrap()
        .unwrap();
    let receiver = LocalSobjMetadataBackend::open_for_inspection(
        shard.clone(),
        crate::default_dfs_metadata_shards_root(runtime.data_dir(replica))
            .join(crate::metadata_keys::key_segment(&shard.0)),
    )
    .unwrap();
    assert_eq!(
        receiver.metadata_prefix_proof().unwrap(),
        anchor,
        "all corpus replay starts at the actual common native prefix"
    );
    assert!(anchor.is_well_formed());
    anchor
}

// Verify the actual replica's durable history, independently of a browser's
// private read projection. This replays byte-prefix proofs only; it grants no
// ingest permission and never writes metadata or manufactures acknowledgements.
fn assert_native_backlog_cut(
    runtime: &crate::test_runtime::DfsTestRuntime,
    replica: &NodeId,
    anchor: &crate::DfsMetadataPrefixProof,
    cut: &crate::DfsMetadataPrefixProof,
    files: &[DfsStat],
) {
    assert_eq!(files.len(), 700);
    let shard = DfsMetadataShardId::new("users");
    let backend = LocalSobjMetadataBackend::open_for_inspection(
        shard.clone(),
        crate::default_dfs_metadata_shards_root(runtime.data_dir(replica))
            .join(crate::metadata_keys::key_segment(&shard.0)),
    )
    .unwrap();
    let mut proof = anchor.clone();
    let mut batches = 0;
    while proof.journal_sequence < cut.journal_sequence {
        let batch = backend
            .journal_batch_for_shard_since_limited(
                &shard,
                proof.journal_sequence,
                DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
                DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
            )
            .unwrap()
            .expect("the actual replica retains every captured corpus frame");
        assert!(!batch.entries.is_empty());
        assert!(batch.entries.len() <= DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT);
        let frames = backend.journal_frames_for_batch(&batch).unwrap();
        assert_eq!(frames.len(), batch.entries.len());
        for (entry, frame) in batch.entries.iter().zip(&frames) {
            if entry.sequence > cut.journal_sequence {
                break;
            }
            assert_eq!(entry.sequence, proof.journal_sequence + 1);
            assert_eq!(frame.sequence, entry.sequence);
            proof = crate::metadata_journal::metadata_prefix_proof_after_exact_frame(
                &proof,
                entry,
                &shard,
                &frame.exact_bytes,
            )
            .unwrap();
        }
        batches += 1;
    }
    assert!(
        batches > 1,
        "the complete corpus must cross the unchanged journal batch limit"
    );
    assert_eq!(
        &proof, cut,
        "all exact replica bytes must reach the captured author prefix"
    );
    for file in files {
        let actual = backend.get_object(&file.object_id).unwrap().unwrap();
        assert_eq!(actual.path, file.path);
        assert_eq!(actual.kind, DfsObjectKind::File);
        assert_eq!(actual.version, file.version);
        assert_eq!(actual.content_hash, file.content_hash);
        assert_eq!(actual.content_size_bytes, file.size_bytes);
    }
}

#[test]
fn sifs_cluster_posix_routed_op_replays_deep_catch_up_across_batches() {
    // A Main browser reads its routed result through current authority. Its
    // private cache is not the replica owner. Preserve immediate public reads
    // and independently verify all 700 files in the actual durable replica.
    let cluster = sifs_two_primary_cluster("sifs-xshard-deepsync");
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
    let diagnostics = [&users_primary_node, &services_primary_node, &replica_node]
        .into_iter()
        .map(|node| {
            crate::native_boundary_diagnostics::capture_all_operations(
                _runtime.data_dir(node).root(),
            )
        })
        .collect::<Vec<_>>();
    drop(users_primary);
    let mut users_primary = _runtime.metadata_primary_service("/users").unwrap();
    let anchor = native_backlog_anchor(&_runtime, &users_primary, &replica_node);

    // Build the full native corpus through its actual owning Core.
    let local_options = DfsWriteOptions {
        policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
        replica_candidates: vec![users_primary.node_id().clone()],
        wait_for_commit: true,
        foreground_replica_repair: true,
    };
    let mut checkpointed = 0;
    let mut files = Vec::with_capacity(700);
    for index in 0..700 {
        let created = users_primary.posix_write_file(
            &format!("/users/bulk-{index:03}.txt"),
            format!("bulk payload {index}").as_bytes(),
            local_options.clone(),
        );
        if created.is_err() {
            for diagnostic in &diagnostics {
                diagnostic.dump(
                    "deep-catchup-bulk-failure",
                    serde_json::json!({
                        "index": index, "result": format!("{created:?}"),
                        "rpc": format!("{:?}", _runtime.rpc_observations()),
                    }),
                );
            }
        }
        let stat = created.expect("bulk create");
        files.push(stat);
        if (index + 1) % 100 == 0 {
            checkpointed += _runtime.checkpoint_rpc_observation_window().unwrap();
            for diagnostic in &diagnostics {
                diagnostic.checkpoint(&format!("deep-catchup-window-{}", index + 1));
            }
        }
    }
    assert!(
        checkpointed > 4096,
        "full corpus observations exceed one buffer"
    );

    let shard = DfsMetadataShardId::new("users");
    let cut = users_primary
        .metadata_backend()
        .unwrap()
        .metadata_prefix_proof(&shard)
        .unwrap()
        .unwrap();
    let setup = users_primary.posix_sync_directory("/users").unwrap();
    assert_eq!(
        setup.metadata_certificate.target.journal_sequence,
        cut.journal_sequence
    );
    assert_eq!(
        setup
            .metadata_certificate
            .acknowledged_node_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        [
            users_primary_node.clone(),
            services_primary_node.clone(),
            replica_node.clone()
        ]
        .into_iter()
        .collect()
    );
    assert!(
        files
            .iter()
            .all(|file| replica.metadata().object(&file.object_id).is_none()),
        "the browser starts without private copies of the corpus"
    );
    drop(users_primary);

    replica
        .posix_mkdir("/users/deep-sync-marker")
        .expect("routed mkdir with deep catch-up");
    assert!(
        replica
            .posix_stat_distributed("/users/bulk-000.txt")
            .is_ok(),
        "earliest backlog entry must be visible after the routed op"
    );
    assert!(
        replica
            .posix_stat_distributed("/users/bulk-699.txt")
            .is_ok(),
        "latest backlog entry must be visible after the routed op"
    );
    assert!(
        replica
            .posix_stat_distributed("/users/deep-sync-marker")
            .is_ok()
    );

    assert_native_backlog_cut(&_runtime, &replica_node, &anchor, &cut, &files);
    assert_eq!(_runtime.observation_dropped_count(), 0);
    drop((services_primary, replica, shared_kv));
    _runtime
        .close()
        .expect("normal Main and native owner drain");
}

#[test]
fn sifs_cluster_posix_mkdir_all_replays_deep_catch_up_across_batches() {
    // A Main browser reads its routed result through current authority. Its
    // private cache is not the replica owner. Preserve immediate public reads
    // and independently verify all 700 files in the actual durable replica.
    let cluster = sifs_two_primary_cluster("sifs-xshard-mkdir-all-deepsync");
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
    drop(users_primary);
    let mut users_primary = _runtime.metadata_primary_service("/users").unwrap();
    let anchor = native_backlog_anchor(&_runtime, &users_primary, &replica_node);

    let local_options = DfsWriteOptions {
        policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
        replica_candidates: vec![users_primary.node_id().clone()],
        wait_for_commit: true,
        foreground_replica_repair: true,
    };
    let mut checkpointed = 0;
    let mut files = Vec::with_capacity(700);
    for index in 0..700 {
        let path = format!("/users/mkdir-all-backlog-{index:03}.txt");
        let payload = format!("bulk payload {index}");
        let result =
            users_primary.posix_write_file(&path, payload.as_bytes(), local_options.clone());
        if let Err(error) = &result {
            let checkpoint = _runtime.checkpoint_rpc_observation_window();
            let artifact = root.join("bulk-create-failure.json");
            std::fs::write(
                &artifact,
                serde_json::to_vec_pretty(&serde_json::json!({
                    "index": index,
                    "path": path,
                    "payload": payload,
                    "error": format!("{error:?}"),
                    "checkpoint": format!("{checkpoint:?}"),
                    "observation_omissions": _runtime.observation_dropped_count(),
                }))
                .unwrap(),
            )
            .unwrap();
            eprintln!("NATIVE_BULK_CREATE_FAILURE {}", artifact.display());
        }
        let stat = result.expect("bulk create");
        files.push(stat);
        if (index + 1) % 100 == 0 {
            checkpointed += _runtime.checkpoint_rpc_observation_window().unwrap();
        }
    }
    assert!(
        checkpointed > 4096,
        "full corpus observations exceed one buffer"
    );

    let shard = DfsMetadataShardId::new("users");
    let cut = users_primary
        .metadata_backend()
        .unwrap()
        .metadata_prefix_proof(&shard)
        .unwrap()
        .unwrap();
    let setup = users_primary.posix_sync_directory("/users").unwrap();
    assert_eq!(
        setup.metadata_certificate.target.journal_sequence,
        cut.journal_sequence
    );
    assert_eq!(
        setup
            .metadata_certificate
            .acknowledged_node_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        [
            users_primary_node.clone(),
            services_primary_node.clone(),
            replica_node.clone()
        ]
        .into_iter()
        .collect()
    );
    assert!(
        files
            .iter()
            .all(|file| replica.metadata().object(&file.object_id).is_none()),
        "the browser starts without private copies of the corpus"
    );
    drop(users_primary);

    replica
        .posix_mkdir_all("/users/deep-mkdir-all/leaf")
        .expect("routed mkdir_all with deep catch-up");
    assert!(
        replica
            .posix_stat_distributed("/users/mkdir-all-backlog-000.txt")
            .is_ok(),
        "earliest backlog entry must be visible after routed mkdir_all"
    );
    assert!(
        replica
            .posix_stat_distributed("/users/mkdir-all-backlog-699.txt")
            .is_ok(),
        "latest backlog entry must be visible after routed mkdir_all"
    );
    assert!(
        replica
            .posix_stat_distributed("/users/deep-mkdir-all")
            .is_ok()
    );
    assert!(
        replica
            .posix_stat_distributed("/users/deep-mkdir-all/leaf")
            .is_ok()
    );

    assert_native_backlog_cut(&_runtime, &replica_node, &anchor, &cut, &files);
    assert_eq!(_runtime.observation_dropped_count(), 0);
    drop((services_primary, replica, shared_kv));
    _runtime
        .close()
        .expect("normal Main and native owner drain");
}

#[test]
fn sifs_cluster_posix_mkdir_all_under_services_replays_created_chain() {
    let cluster = sifs_two_primary_cluster("sifs-xshard-mkdir-all-services-chain");
    let SifsTwoPrimaryCluster {
        shared_kv,
        services_primary_node,
        users_primary_node,
        _runtime,
        root,
        services_primary,
        mut replica,
        ..
    } = cluster;

    let stop = Arc::new(AtomicBool::new(false));
    let services_stop = Arc::clone(&stop);
    let services_thread =
        std::thread::spawn(move || pump_sifs_service_until_stop(services_primary, services_stop));

    replica
        .posix_mkdir_all("/services/cluster-tests/latency-regression")
        .expect("routed services mkdir_all");
    assert!(
        replica
            .posix_stat_distributed("/services/cluster-tests")
            .is_ok(),
        "mkdir_all must sync the intermediate directory"
    );
    assert!(
        replica
            .posix_stat_distributed("/services/cluster-tests/latency-regression")
            .is_ok(),
        "mkdir_all must sync the created leaf directory"
    );

    stop.store(true, Ordering::SeqCst);
    services_thread.join().expect("services thread");
    std::fs::remove_dir_all(&root).ok();
}
