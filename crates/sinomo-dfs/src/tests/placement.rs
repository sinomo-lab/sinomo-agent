//! Placement regression tests.

use super::*;

#[test]
fn subtree_policy_default_encoding_remains_legacy_compatible() {
    let policy = DfsSubtreePolicy::legacy("/models", DfsObjectPolicy::ReplicatedEventual);
    let encoded = serde_json::to_value(&policy).expect("serialize subtree policy");

    assert_eq!(
        encoded,
        serde_json::json!({
            "path_prefix": "/models",
            "policy": "replicated-eventual"
        }),
        "default placement fields must not change metadata snapshot semantic checksums"
    );
    assert_eq!(
        serde_json::from_value::<DfsSubtreePolicy>(encoded).expect("deserialize legacy policy"),
        policy,
    );
}

#[test]
fn pending_write_stream_falls_back_to_exact_blob_after_manifest_replacement() {
    let root = temp_root("pending-write-exact-blob-fallback");
    let node = NodeId::from_legacy_named("pending-write-fallback-node").expect("node");
    let object_id =
        DfsObjectId::from_legacy_named("pending-write-fallback-object").expect("object id");
    let version = DfsVersion::from_legacy_named("pending-write-fallback-version").expect("version");
    let first = b"first concurrent body";
    let second = b"second concurrent body";
    let content = LocalContentStore::new(root.join("content")).expect("content store");
    let first_manifest = content
        .put_object_version_blob_only(object_id.as_str(), version.as_str(), first)
        .expect("retain first content-addressed blob");
    content
        .put_object_version(object_id.as_str(), version.as_str(), second)
        .expect("publish replacement manifest");
    let service = NetworkedDfsService::new(ReceiveOnlyTransport::new(node), content);
    let pending = PendingNetworkedWrite::new(
        object_id,
        version,
        first_manifest.content_hash,
        first_manifest.size_bytes,
    );

    let mut reader = service
        .open_pending_write_blob_stream(&pending)
        .expect("open exact pending blob despite replacement manifest");
    let mut actual = Vec::new();
    reader.read_to_end(&mut actual).expect("read pending blob");
    assert_eq!(actual, first);

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn mkdir_all_over_tombstone_uses_new_directory_attributes_and_policy() {
    let root = temp_root("mkdir-all-tombstone-create-metadata");
    let node =
        NodeId::from_legacy_named("node_mkdir_all_tombstone_create_metadata").expect("node id");
    let mut dfs = networked_sifs_with_backend(&root, node.clone());
    let path = "/unmanaged/deleted-file";
    dfs.posix_mkdir_all("/unmanaged").expect("create parent");
    let deleted = dfs
        .posix_write_file_with_create_attrs(
            path,
            b"deleted",
            DfsWriteOptions::replicated_eventual(vec![node]),
            DfsPosixCreateAttrs::new(0o640, 1001, 1002),
        )
        .expect("create file");
    let mut deleted_metadata = dfs
        .metadata()
        .object(&deleted.object_id)
        .expect("deleted file metadata")
        .clone();
    deleted_metadata.policy = DfsObjectPolicy::LocalOwned;
    dfs.add_object_metadata(deleted_metadata)
        .expect("install distinct deleted policy");
    dfs.posix_unlink(path).expect("tombstone file");

    let tombstone = dfs
        .metadata()
        .object(&deleted.object_id)
        .expect("tombstone metadata");
    assert_eq!(tombstone.kind, DfsObjectKind::Tombstone);
    assert_eq!(
        (tombstone.mode, tombstone.uid, tombstone.gid),
        (0o640, 1001, 1002),
        "the test must exercise a tombstone retaining the deleted file attributes"
    );
    assert_eq!(tombstone.policy, DfsObjectPolicy::LocalOwned);

    dfs.posix_mkdir_all(path)
        .expect("recreate tombstone as directory");
    let recreated = dfs.posix_stat(path).expect("recreated directory");
    assert_eq!(recreated.kind, DfsObjectKind::Directory);
    assert_eq!(
        (recreated.mode, recreated.uid, recreated.gid),
        (0o755, 0, 0),
        "mkdir-all must apply new-directory attributes instead of the deleted file attributes"
    );
    assert_eq!(
        recreated.policy,
        DfsObjectPolicy::ReplicatedEventual,
        "an unmanaged mkdir-all must use the fresh-directory policy fallback"
    );
}

#[test]
fn mkdir_over_tombstone_applies_current_managed_directory_policy() {
    let root = temp_root("mkdir-tombstone-managed-policy");
    let node = NodeId::from_legacy_named("node_mkdir_tombstone_managed_policy").expect("node id");
    let mut dfs = networked_sifs_with_backend(&root, node.clone());
    let path = "/services/deleted-file";
    let deleted = dfs
        .posix_write_file_with_create_attrs(
            path,
            b"deleted",
            DfsWriteOptions::replicated_eventual(vec![node]),
            DfsPosixCreateAttrs::new(0o600, 2001, 2002),
        )
        .expect("create file");
    let mut deleted_metadata = dfs
        .metadata()
        .object(&deleted.object_id)
        .expect("deleted file metadata")
        .clone();
    deleted_metadata.policy = DfsObjectPolicy::LocalOwned;
    dfs.add_object_metadata(deleted_metadata)
        .expect("install distinct deleted policy");
    dfs.posix_unlink(path).expect("tombstone file");

    let mut file_system = dfs
        .metadata()
        .filesystems
        .get(&sifs_file_system_id())
        .expect("SIFS metadata")
        .clone();
    file_system.subtree_policies.push(DfsSubtreePolicy::legacy(
        path,
        DfsObjectPolicy::ReplicatedConfirmed,
    ));
    dfs.add_file_system(file_system)
        .expect("install current managed policy");

    let recreated = dfs.posix_mkdir(path).expect("recreate managed directory");
    assert_eq!(recreated.kind, DfsObjectKind::Directory);
    assert_eq!(
        (recreated.mode, recreated.uid, recreated.gid),
        (0o755, 0, 0)
    );
    assert_eq!(
        recreated.policy,
        DfsObjectPolicy::ReplicatedConfirmed,
        "mkdir must resolve the current managed policy instead of retaining the dead object policy"
    );
}

#[test]
fn dfs_smoke_replica_candidates_preserve_routable_order() {
    let local = NodeId::from_legacy_named("node_dfs_smoke_candidates_local").expect("local node");
    let stale = NodeId::from_legacy_named("node_dfs_smoke_candidates_stale").expect("stale node");
    let reachable =
        NodeId::from_legacy_named("node_dfs_smoke_candidates_reachable").expect("reachable node");

    let candidates = dfs_smoke_replica_candidate_sets(
        &local,
        vec![local.clone(), stale.clone(), reachable.clone()],
        vec![reachable.clone(), stale.clone(), reachable.clone()],
    );

    assert_eq!(
        candidates,
        vec![vec![local.clone(), reachable], vec![local.clone(), stale],],
        "DFS smoke should try routable nodes in selector order instead of sorted node-id order"
    );
}

#[test]
fn reconnect_does_not_restore_local_replica_removed_by_managed_placement() {
    let root = temp_root("reconnect-respects-managed-placement");
    let local = NodeId::from_legacy_named("node_reconnect_placement_local").expect("local");
    let remote = NodeId::from_legacy_named("node_reconnect_placement_remote").expect("remote");
    let object_id =
        DfsObjectId::from_legacy_named("obj_reconnect_placement_removed").expect("object");
    let version = DfsVersion::from_legacy_named("version_reconnect_placement").expect("version");
    let path = "/services/reconnect-placement.txt";
    let bytes = b"local bytes remain after replica removal";
    let control_plane = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    control_plane.with_client(|kv| {
        let mut config = DfsFileSystemConfig::from(&file_system(local.clone()));
        let mut policy = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 1)
            .expect("placement policy");
        policy.forbidden_node_ids = vec![local.clone()];
        config.subtree_policies.push(
            normalize_placement_rule(path, DfsPlacementScope::SelfOnly, policy)
                .expect("placement rule"),
        );
        let mut store = DfsControlPlaneStore::new(kv);
        store
            .put_file_system_config(&config)
            .expect("filesystem config");
    });

    let mut dfs = networked_sifs_with_backend(&root, local.clone());
    let manifest = dfs
        .content
        .put_object_version(object_id.as_str(), version.as_str(), bytes)
        .expect("retain local manifest and blob");
    let mut metadata = file_metadata(local.clone());
    metadata.object_id = object_id.clone();
    metadata.path = path.to_string();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.version = version.clone();
    metadata.current_locations = vec![remote.clone()];
    metadata.content_hash = Some(manifest.content_hash.clone());
    metadata.content_size_bytes = Some(bytes.len() as u64);
    dfs.add_object_metadata(metadata).expect("metadata");
    // Once a control plane is attached, a config check cannot authorize a
    // later receipt write: Apply could create or replace a rule in between.
    // Reconnect must leave all clustered replica metadata to the permitted job.
    let mut dfs = dfs.with_control_plane(control_plane);
    assert!(
        dfs.resolved_placement_policy_for_path(path, DfsObjectPolicy::ReplicatedEventual, 2)
            .expect("resolved placement")
            .policy
            .forbidden_node_ids
            .contains(&local),
        "test setup must explicitly forbid the reconnecting local node"
    );

    let reconciliation = dfs
        .reconcile_local_manifests(&object_id)
        .expect("reconcile local manifests");

    assert_eq!(reconciliation.refreshed_receipts, 0);
    assert!(
        !reconciliation.missing_current,
        "the local bytes still exist"
    );
    assert!(
        dfs.metadata_backend()
            .expect("backend")
            .receipts_for_version(&object_id, &version)
            .expect("receipts")
            .iter()
            .all(|receipt| receipt.replica_node_id != local),
        "reconnect must not recreate the removed local receipt"
    );
    assert_eq!(
        dfs.metadata()
            .object(&object_id)
            .expect("object")
            .current_locations,
        vec![remote],
        "reconnect must not recreate the removed local location"
    );
    assert!(
        dfs.content()
            .has_blob(&manifest.content_hash)
            .expect("local blob lookup"),
        "placement removal leaves the bytes available as a local cache"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn dfs_policy_violation_event_is_structured_for_supervision() {
    let owner = NodeId::from_legacy_named("node_desktop").expect("node");
    let remote = NodeId::from_legacy_named("node_mobile").expect("node");
    let event = DfsPolicyViolation {
        requesting_node_id: remote,
        executing_node_id: owner.clone(),
        file_system_id: Some(DfsFileSystemId::from_legacy_named("sifs").expect("filesystem")),
        object_id: Some(DfsObjectId::from_legacy_named("obj_file").expect("object")),
        reason: "write attempted against non-owner".to_string(),
    }
    .event();

    assert_eq!(event.kind.as_str(), "dfs.policy.violation");
    assert_eq!(event.target_node_id, Some(owner));
    assert_eq!(
        event.payload["file_system_id"],
        DfsFileSystemId::from_legacy_named("sifs")
            .expect("filesystem")
            .as_str()
    );
}

#[test]
fn sifs_cluster_posix_routed_namespace_ops_converge_across_replicas() {
    // Namespace mutations issued from different replicas serialize through
    // the shard primary, and every node converges to one identical namespace.
    let cluster = sifs_posix_cluster("sifs-cluster-ns-converge");
    let SifsPosixCluster {
        root,
        primary_node: _,
        writer_node,
        observer_node: _,
        primary,
        mut writer,
        mut observer,
        rejections: _,
    } = cluster;

    let stop_primary = Arc::new(AtomicBool::new(false));
    let primary_stop = Arc::clone(&stop_primary);
    let primary_thread = std::thread::spawn(move || {
        let mut primary = primary;
        while !primary_stop.load(Ordering::SeqCst) {
            primary.process_incoming().expect("primary pump");
            std::thread::sleep(Duration::from_millis(1));
        }
        primary
    });

    // The writer builds a subtree and a file.
    writer
        .posix_mkdir("/users/shared")
        .expect("writer routed mkdir");
    let bytes = b"replica-converged bytes".to_vec();
    writer
        .posix_write_file(
            "/users/shared/from-writer.txt",
            &bytes,
            DfsWriteOptions::replicated_eventual(vec![writer_node.clone()]),
        )
        .expect("writer routed write");

    // The observer mutates the same subtree it has never replayed: the
    // primary resolves authoritatively, so the routed ops still apply.
    observer
        .posix_mkdir("/users/shared/from-observer")
        .expect("observer routed mkdir into unseen subtree");
    observer
        .posix_rename(
            "/users/shared/from-writer.txt",
            "/users/shared/renamed-by-observer.txt",
        )
        .expect("observer routed rename of writer file");
    assert!(matches!(
        observer.posix_stat("/users/shared/from-writer.txt"),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert_eq!(
        observer
            .posix_stat("/users/shared/renamed-by-observer.txt")
            .expect("observer stat renamed")
            .content_hash
            .as_deref(),
        Some(content_hash(&bytes).as_str())
    );

    // The writer learns the observer's mutations through publication.
    stop_primary.store(true, Ordering::SeqCst);
    let mut primary = primary_thread.join().expect("primary thread");
    let mut rejections = Vec::new();
    let deadline = Instant::now() + SIFS_CLUSTER_PUMP_DEADLINE;
    loop {
        primary
            .replicate_configured_metadata_shards(&sifs_file_system_id())
            .expect("publish after replica namespace ops");
        pump_dfs_services_collect_rejections(
            &mut [
                ("primary", &mut primary),
                ("writer", &mut writer),
                ("observer", &mut observer),
            ],
            &mut rejections,
        );
        let writer_sees_rename = writer
            .posix_stat("/users/shared/renamed-by-observer.txt")
            .is_ok()
            && matches!(
                writer.posix_stat("/users/shared/from-writer.txt"),
                Err(DfsPosixError::NotFound { .. })
            );
        let writer_sees_observer_dir = writer.posix_stat("/users/shared/from-observer").is_ok();
        if writer_sees_rename && writer_sees_observer_dir {
            break;
        }
        if Instant::now() >= deadline {
            panic!("writer did not converge to observer mutations; rejections: {rejections:?}");
        }
        std::thread::sleep(SIFS_CLUSTER_PUMP_IDLE_SLEEP);
    }
    assert!(
        rejections.is_empty(),
        "cross-replica namespace ops must keep journals aligned: {rejections:?}"
    );

    // All three nodes list the same namespace identity.
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
    let primary_entries = primary
        .posix_list_dir("/users/shared")
        .expect("primary list shared");
    assert_eq!(
        names(&primary_entries),
        names(&writer.posix_list_dir("/users/shared").expect("writer list"))
    );
    assert_eq!(
        names(&primary_entries),
        names(
            &observer
                .posix_list_dir("/users/shared")
                .expect("observer list")
        )
    );

    // The renamed content reads identically from its staging replica.
    assert_eq!(
        writer
            .posix_read_file("/users/shared/renamed-by-observer.txt")
            .expect("writer read renamed"),
        bytes
    );

    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn sifs_cluster_posix_reverse_direction_move_resolves_by_id_on_replicas() {
    // Replicas hold no object→shard index: after a services → users move the
    // eviction tombstone lives in the "services" shard, which sorts *before*
    // "users". By-id resolution must prefer the live record regardless of
    // shard naming.
    let cluster = sifs_two_primary_cluster("sifs-xshard-reverse");
    let SifsTwoPrimaryCluster {
        root,
        replica_node,
        users_primary,
        services_primary,
        mut replica,
        ..
    } = cluster;
    let bytes = b"reverse-direction move payload".to_vec();

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
            "/services/origin.txt",
            &bytes,
            DfsWriteOptions::replicated_eventual(vec![replica_node.clone()]),
        )
        .expect("create under services");
    replica
        .posix_rename("/services/origin.txt", "/users/relocated.txt")
        .expect("reverse-direction cross-primary rename");
    assert_eq!(
        replica
            .posix_read_file("/users/relocated.txt")
            .expect("read after reverse move"),
        bytes
    );

    stop.store(true, Ordering::SeqCst);
    let users_primary = users_thread.join().expect("users thread");
    let services_primary = services_thread.join().expect("services thread");

    // Converge the replica through publication, then prove by-id resolution:
    // the local read path resolves the object id through the backend, which
    // must return the live "users" record, not the "services" tombstone.
    let mut cluster = SifsTwoPrimaryCluster {
        root,
        users_primary_node: NodeId::from_legacy_named("node_unused_users4").expect("node"),
        services_primary_node: NodeId::from_legacy_named("node_unused_services4").expect("node"),
        replica_node,
        shared_kv: SharedDfsControlPlaneKvClient::new(InProcessKvStore::new()),
        users_primary,
        services_primary,
        replica,
        rejections: Vec::new(),
    };
    let expected_hash = content_hash(&bytes);
    sifs_two_primary_publish_until(&mut cluster, "reverse move convergence", |cluster| {
        cluster
            .replica
            .posix_stat("/users/relocated.txt")
            .is_ok_and(|stat| stat.content_hash.as_deref() == Some(expected_hash.as_str()))
            && matches!(
                cluster.replica.posix_stat("/services/origin.txt"),
                Err(DfsPosixError::NotFound { .. })
            )
    });
    assert_eq!(
        cluster
            .replica
            .posix_read_file("/users/relocated.txt")
            .expect("replica by-id read after convergence"),
        bytes,
        "replica object resolution must prefer the live record over the eviction tombstone"
    );
    assert!(
        cluster.rejections.is_empty(),
        "reverse-direction move must keep journals aligned: {:?}",
        cluster.rejections
    );

    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn mounted_read_policy_does_not_escalate_lookup_failure_to_sync_path() {
    fn read_service(
        root: &Path,
        name: &str,
        local_node: &NodeId,
        primary_node: &NodeId,
        control_plane: SharedDfsControlPlaneKvClient<InProcessKvStore>,
    ) -> (
        NetworkedDfsService<DfsNodeTransport>,
        Arc<Mutex<Vec<&'static str>>>,
    ) {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let calls_for_sender = Arc::clone(&calls);
        let local_for_sender = local_node.clone();
        let primary_for_sender = primary_node.clone();
        let sender: DfsMessageSender = Arc::new(move |target, command, _timeout| {
            assert_eq!(target, &primary_for_sender);
            let DfsNetworkMessage::NamespaceRequest(namespace) = &command.routed_message.message
            else {
                return Err(DfsPeerTransportError::new(format!(
                    "unexpected mounted-read fallback command: {:?}",
                    command.routed_message.message
                )));
            };
            let label = match &namespace.operation {
                DfsNamespaceOperation::LookupPath { .. } => "lookup-path",
                DfsNamespaceOperation::SyncPath { .. } => "sync-path",
                operation => {
                    return Err(DfsPeerTransportError::new(format!(
                        "unexpected mounted-read namespace operation: {operation:?}"
                    )));
                }
            };
            calls_for_sender
                .lock()
                .expect("mounted read calls")
                .push(label);
            Ok(DfsMessageResponse {
                request_id: command.request_id.clone(),
                outbound_messages: vec![build_dfs_routed_message(
                    &primary_for_sender,
                    &local_for_sender,
                    command.routed_message.frame_id.saturating_add(1),
                    command.routed_message.message_id.saturating_add(1),
                    DfsNetworkMessage::NamespaceResponse(DfsNamespaceResponse {
                        request_id: namespace.request_id.clone(),
                        result: DfsNamespaceResult::Rejected {
                            reason: "injected authoritative lookup failure".to_string(),
                            posix_error: None,
                            current_primary_node_id: None,
                        },
                        synced_path_object: None,
                        synced_path_objects: Vec::new(),
                        metadata_journal_batches: Vec::new(),
                        metadata_journal_has_more: None,
                        posix_coordination_result: None,
                    }),
                )],
            })
        });
        let transport = DfsNodeTransport::with_message_sender_for_nodes(
            local_node.clone(),
            vec![primary_node.clone()],
            Duration::from_millis(100),
            sender,
        );
        let service = NetworkedDfsService::new(
            transport,
            LocalContentStore::new(root.join(name).join("content"))
                .expect("mounted-read content store"),
        )
        .with_control_plane(control_plane);
        (service, calls)
    }

    let root = temp_root("mounted-read-no-implicit-sync-path");
    let local_node =
        NodeId::from_legacy_named("node_mounted_read_no_implicit_sync").expect("local node");
    let primary_node = NodeId::from_legacy_named("node_mounted_read_no_implicit_sync_primary")
        .expect("primary node");
    let authority_shard = test_routed_authority_shard(&primary_node, 1);
    let control_plane = test_routed_authority_control_plane(&authority_shard);

    let (mut ordinary, ordinary_calls) = read_service(
        &root,
        "ordinary",
        &local_node,
        &primary_node,
        control_plane.clone(),
    );
    assert!(
        ordinary
            .posix_stat_distributed("/services/missing.txt")
            .is_err()
    );
    assert_eq!(
        ordinary_calls
            .lock()
            .expect("ordinary read calls")
            .as_slice(),
        ["lookup-path", "sync-path"],
        "the ordinary compatibility policy should exercise the fallback this test fences"
    );

    let (mut mounted, mounted_calls) =
        read_service(&root, "mounted", &local_node, &primary_node, control_plane);
    mounted.disable_implicit_read_replica_ingest();
    assert!(
        mounted
            .posix_stat_distributed("/services/missing.txt")
            .is_err()
    );
    assert_eq!(
        mounted_calls.lock().expect("mounted read calls").as_slice(),
        ["lookup-path"],
        "a mounted read must not turn an authority lookup failure into an unowned durable SyncPath apply"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn placement_reconciliation_resumes_durable_phase_and_updates_existing_file() {
    let root = temp_root("placement-reconciliation-durable-cursor");
    let local = NodeId::from_legacy_named("node_placement_reconciliation_local").expect("node");
    let file_system = file_system(local.clone());
    let control_plane = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    control_plane.with_client(|kv| {
        let mut store = DfsControlPlaneStore::new(kv);
        store
            .put_file_system_config(&DfsFileSystemConfig::from(&file_system))
            .expect("filesystem config");
        store
            .put_metadata_shard(&DfsMetadataShardConfig {
                file_system_id: file_system.file_system_id.clone(),
                shard_id: "root".to_string(),
                primary_node_id: local.clone(),
                replica_node_ids: Vec::new(),
                epoch: 1,
                assignment: DfsMetadataShardAssignment::Root,
                namespace_prepare_codec: Default::default(),
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            })
            .expect("root metadata shard");
    });
    let metadata_root = root.join("metadata");
    let content_root = root.join("content");
    let mut metadata = open_root_local_metadata_backend(&metadata_root).expect("metadata backend");
    metadata
        .put_file_system(&file_system)
        .expect("seed filesystem");
    for object in distributed_sifs_bootstrap_objects(file_system.owner_node_id.clone()) {
        metadata.put_object(&object).expect("seed SIFS namespace");
    }
    let mut service = NetworkedDfsService::new(
        ReceiveOnlyTransport::new(local.clone()),
        LocalContentStore::new(&content_root).expect("content"),
    )
    .with_control_plane(control_plane.clone())
    .with_metadata_backend(metadata)
    .expect("attach metadata backend");
    let path = "/services/placement-existing.txt";
    service.posix_mkdir_all("/services").expect("services");
    service
        .posix_write_file(
            path,
            b"existing placement content",
            DfsWriteOptions::replicated_eventual(vec![local.clone()]),
        )
        .expect("existing file");
    assert_eq!(
        service.posix_stat(path).expect("initial stat").policy,
        DfsObjectPolicy::ReplicatedEventual
    );

    let now = dfs_now_unix_nanos();
    let job = control_plane.with_client(|kv| {
        let mut store = DfsControlPlaneStore::new(kv);
        let expected = store
            .get_file_system_config(&file_system.file_system_id)
            .expect("read config")
            .expect("config");
        let mut desired = expected.clone();
        upsert_placement_rule(
            &mut desired.subtree_policies,
            normalize_placement_rule(
                path,
                DfsPlacementScope::SelfOnly,
                DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 1, 1)
                    .expect("confirmed policy"),
            )
            .expect("placement rule"),
        );
        let job = DfsPlacementReconciliationJob {
            schema_version: crate::DFS_PLACEMENT_RECONCILIATION_SCHEMA_VERSION,
            job_id: "placement-durable-job".to_string(),
            file_system_id: file_system.file_system_id.clone(),
            path: path.to_string(),
            scope: DfsPlacementScope::SelfOnly,
            unavailable_node_ids: BTreeSet::new(),
            candidate_snapshot: vec![DfsNodeAdvertisement::content(local.clone())],
            placement_snapshot: None,
            adaptive: false,
            adaptive_budget: None,
            processed_content_bytes: 0,
            accepted_config: Some(desired.clone()),
            state: DfsPlacementReconciliationState::Pending,
            cursor: None,
            total_object_count: 0,
            scanned_object_count: 0,
            matched_object_count: 0,
            updated_object_count: 0,
            scheduled_repair_count: 0,
            completed_repair_count: 0,
            pending_repair_count: 0,
            revision: 0,
            created_at_unix_nanos: now,
            updated_at_unix_nanos: now,
            last_error: None,
        };
        assert!(
            store
                .compare_and_put_placement_update(&expected, &desired, &job)
                .expect("publish placement and job")
        );
        job
    });

    let non_primary = NodeId::from_legacy_named("node_placement_reconciliation_replica")
        .expect("non-primary node");
    let mut replica_service = NetworkedDfsService::new(
        ReceiveOnlyTransport::new(non_primary),
        LocalContentStore::new(root.join("replica-content")).expect("replica content"),
    )
    .with_control_plane(control_plane.clone());
    assert!(
        replica_service
            .run_placement_reconciliation_bounded(1)
            .expect("non-primary reconciliation tick")
            .is_none(),
        "only the filesystem root metadata primary may advance the durable job"
    );
    control_plane.with_client(|kv| {
        assert_eq!(
            DfsControlPlaneStore::new(kv)
                .placement_reconciliation(&file_system.file_system_id, &job.job_id)
                .expect("unclaimed durable job")
                .expect("job remains pending")
                .revision,
            0,
            "a non-primary tick must not claim progress or perform side effects"
        );
    });

    let first_page = service
        .run_placement_reconciliation_bounded(1)
        .expect("first reconciliation page")
        .expect("active job");
    assert_eq!(first_page.state, DfsPlacementReconciliationState::Repairing);
    assert!(
        first_page.cursor.is_none(),
        "a self-only job resolves its one indexed object"
    );
    assert_eq!(first_page.scanned_object_count, 1);
    assert_eq!(first_page.total_object_count, 1);
    drop(service);

    let mut reopened = NetworkedDfsService::new(
        ReceiveOnlyTransport::new(local),
        LocalContentStore::new(&content_root).expect("reopen content"),
    )
    .with_control_plane(control_plane.clone())
    .with_metadata_backend(
        open_root_local_metadata_backend(&metadata_root).expect("reopen metadata"),
    )
    .expect("attach reopened metadata");
    let mut completed = None;
    for _ in 0..32 {
        let Some(next) = reopened
            .run_placement_reconciliation_bounded(2)
            .expect("resume reconciliation")
        else {
            break;
        };
        if next.state.is_terminal() {
            completed = Some(next);
            break;
        }
    }
    let completed = completed.expect("durable reconciliation completes after restart");
    assert_eq!(completed.state, DfsPlacementReconciliationState::Completed);
    assert_eq!(completed.matched_object_count, 1);
    assert_eq!(
        reopened.posix_stat(path).expect("reconciled stat").policy,
        DfsObjectPolicy::ReplicatedConfirmed
    );
    control_plane.with_client(|kv| {
        assert_eq!(
            DfsControlPlaneStore::new(kv)
                .placement_reconciliation(&file_system.file_system_id, &job.job_id)
                .expect("durable job"),
            Some(completed)
        );
    });
}
