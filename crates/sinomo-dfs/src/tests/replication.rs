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
    let (runtime, mut dfs, stat, withdrawn) = super::repair::native_withdrawn_local_replica_fixture(
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
        dfs.posix_read_file(path).expect("read newer version"),
        b"newer"
    );

    let current = dfs.posix_stat(path).expect("current stat");
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
        dfs.posix_read_file(path).expect("read saved version"),
        b"saved safely"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sharded_point_lookup_snapshot_refreshes_only_the_routed_path_shard() {
    let root = temp_root("sifs-sharded-point-lookup-snapshot");
    let node = NodeId::from_legacy_named("node_sifs_sharded_point_snapshot").expect("node");
    let shards = sifs_two_primary_shard_configs(&node, &node, 1, Vec::new(), Vec::new());
    let mut backend =
        open_local_sharded_metadata_backend(root.join("metadata"), shards).expect("metadata");
    let file_system_id = sifs_file_system_id();

    crate::metadata_sharded_backend::reset_sharded_consistent_read_snapshot_shards();
    backend
        .begin_consistent_read_snapshot_for_path(&file_system_id, "/services/point-lookup-parent")
        .expect("path-scoped snapshot");
    backend.end_consistent_read_snapshot_for_path(&file_system_id, "/services/point-lookup-parent");

    assert_eq!(
        crate::metadata_sharded_backend::sharded_consistent_read_snapshot_shards(),
        vec![
            DfsMetadataShardId::new("services"),
            DfsMetadataShardId::new("root"),
        ],
        "one-path response hydration must retain the routing fallback without refreshing the unrelated users shard"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_cluster_posix_replica_catches_up_after_journal_publish() {
    let mut cluster = sifs_posix_cluster("sifs-cluster-catchup", "/users");
    let path = "/users/replicated.txt";
    let bytes = b"observer should converge to this content";

    sifs_cluster_routed_posix_write(&mut cluster, path, bytes);

    // The observer never participated in the write; it learns the namespace
    // through the primary's metadata journal publication.
    assert!(
        matches!(
            cluster.observer.posix_stat(path),
            Err(DfsPosixError::NotFound { .. })
        ),
        "observer must not see the path before metadata publication"
    );

    let expected_hash = content_hash(bytes);
    sifs_cluster_publish_metadata_until(&mut cluster, path, |cluster| {
        cluster
            .observer
            .posix_stat(path)
            .is_ok_and(|stat| stat.content_hash.as_deref() == Some(expected_hash.as_str()))
    });

    let stat = cluster.observer.posix_stat(path).expect("observer stat");
    assert_eq!(stat.kind, DfsObjectKind::File);
    assert_eq!(stat.size_bytes, Some(bytes.len() as u64));
    let entries = cluster
        .observer
        .posix_list_dir("/users")
        .expect("observer list /users");
    assert!(
        entries.iter().any(|entry| entry.path == path),
        "observer directory listing must contain the replicated path: {entries:?}"
    );

    // Content follows metadata on demand.
    let read = sifs_cluster_fetch_for_local_read(&mut cluster, |c| &mut c.observer, path);
    assert_eq!(read, bytes.to_vec());

    // Publication is idempotent: republishing must not error or regress state.
    let publication = cluster
        ._runtime
        .maintenance_tick(
            &cluster.primary_node,
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
        )
        .expect("ordinary idempotent Main metadata publication");
    assert!(publication.outcome.is_ok(), "{:?}", publication.outcome);
    assert_eq!(publication.omitted_kv_event_count, 0);
    sifs_posix_cluster_pump_round(&mut cluster);
    let stat = cluster
        .observer
        .posix_stat(path)
        .expect("observer stat after republish");
    assert_eq!(
        stat.content_hash.as_deref(),
        Some(expected_hash.as_str()),
        "republishing the journal must not regress replicated metadata"
    );

    // The writer is also a configured replica. Normal Main replication must
    // preserve its current namespace and readable body as well.
    assert_eq!(
        cluster.writer.posix_read_file(path).expect("writer read"),
        bytes.to_vec()
    );

    assert!(
        cluster.rejections.is_empty(),
        "no journal batch may be rejected during routed writes and catch-up: {:?}",
        cluster.rejections
    );

    drop(cluster);
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
            .posix_stat("/services/no-replay-batches")
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
fn routed_namespace_cursor_snapshot_uses_cached_state_not_backend_status() {
    let mut cluster = sifs_two_primary_cluster("sifs-namespace-cached-cursors");
    let services_shard = DfsMetadataShardId::new("services");
    cluster
        .replica
        .replication
        .local_metadata_journal_cursors
        .clear();
    assert!(
        cluster
            .replica
            .local_metadata_shard_cursors()
            .expect("cached cursors")
            .is_empty(),
        "routed namespace sends must not scan backend shard status when the cache is empty"
    );

    cluster
        .replica
        .replication
        .local_metadata_journal_cursors
        .insert(services_shard.clone(), 42);
    let cursors = cluster
        .replica
        .local_metadata_shard_cursors()
        .expect("cached cursor snapshot");
    assert_eq!(cursors.len(), 1);
    assert_eq!(cursors[0].shard_id, services_shard);
    assert_eq!(cursors[0].after_sequence, 42);

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
fn sync_path_response_object_updates_outsider_read_view_without_journal_batches() {
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
    let before = runtime.rpc_observations().len();
    let response = reader
        .route_namespace_operation_to_metadata_primary_with_timeout_and_options(
            config.file_system_id.clone(),
            owner.clone(),
            DfsNamespaceOperation::SyncPath { path: path.into() },
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
        reader
            .metadata()
            .object_by_normalized_posix_path(&config.file_system_id, path),
        Some(&actual)
    );
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

// Verify the actual replica's durable history, independently of a browser's
// private read projection. This replays byte-prefix proofs only; it grants no
// ingest permission and never writes metadata or manufactures acknowledgements.
fn assert_native_backlog_cut(
    runtime: &crate::test_runtime::DfsTestRuntime,
    replica: &NodeId,
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
    let mut proof = crate::metadata_records::dfs_metadata_prefix_proof_genesis(&shard);
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
    drop(users_primary);
    let mut users_primary = _runtime.metadata_primary_service("/users").unwrap();

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
        let stat = users_primary
            .posix_write_file(
                &format!("/users/bulk-{index:03}.txt"),
                format!("bulk payload {index}").as_bytes(),
                local_options.clone(),
            )
            .expect("bulk create");
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

    assert_native_backlog_cut(&_runtime, &replica_node, &cut, &files);
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

    let local_options = DfsWriteOptions {
        policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
        replica_candidates: vec![users_primary.node_id().clone()],
        wait_for_commit: true,
        foreground_replica_repair: true,
    };
    let mut checkpointed = 0;
    let mut files = Vec::with_capacity(700);
    for index in 0..700 {
        let stat = users_primary
            .posix_write_file(
                &format!("/users/mkdir-all-backlog-{index:03}.txt"),
                format!("bulk payload {index}").as_bytes(),
                local_options.clone(),
            )
            .expect("bulk create");
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

    assert_native_backlog_cut(&_runtime, &replica_node, &cut, &files);
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
        replica.posix_stat("/services/cluster-tests").is_ok(),
        "mkdir_all must sync the intermediate directory"
    );
    assert!(
        replica
            .posix_stat("/services/cluster-tests/latency-regression")
            .is_ok(),
        "mkdir_all must sync the created leaf directory"
    );

    stop.store(true, Ordering::SeqCst);
    services_thread.join().expect("services thread");
    std::fs::remove_dir_all(&root).ok();
}
