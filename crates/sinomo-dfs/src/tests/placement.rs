//! Placement regression tests.

use super::*;

#[test]
fn current_subtree_policy_encoding_requires_explicit_replication_counts() {
    let policy = DfsSubtreePolicy::new("/models", DfsObjectPolicy::ReplicatedEventual, 1, 2);
    let encoded = serde_json::to_value(&policy).unwrap();
    assert_eq!(encoded["min_commit_replicas"], 1);
    assert_eq!(encoded["desired_replicas"], 2);
    assert_eq!(
        serde_json::from_value::<DfsSubtreePolicy>(encoded).unwrap(),
        policy
    );
    assert!(
        serde_json::from_value::<DfsSubtreePolicy>(
            serde_json::json!({"path_prefix":"/models","policy":"replicated-eventual"})
        )
        .is_err(),
        "old implicit count records must be converted before current DFS use"
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
    let service = NetworkedDfsService::new(
        crate::test_runtime::DfsTestRuntime::local_transport(node),
        content,
    );
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
    let node =
        NodeId::from_legacy_named("node_mkdir_all_tombstone_create_metadata").expect("node id");
    let (_runtime, dfs) = native_sifs_service(
        "mkdir_all_over_tombstone_uses_new_directory_attributes_and_policy",
        node.clone(),
    );
    let path = "/unmanaged/deleted-file";
    drop(dfs);
    let mut dfs = _runtime.metadata_primary_service(path).unwrap();
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
        .metadata_backend()
        .unwrap()
        .get_object(&deleted.object_id)
        .unwrap()
        .expect("actual committed file metadata");
    deleted_metadata.policy = DfsObjectPolicy::LocalOwned;
    dfs.add_object_metadata(deleted_metadata)
        .expect("install distinct deleted policy");
    dfs.posix_unlink(path).expect("tombstone file");

    let tombstone = dfs
        .metadata_backend()
        .unwrap()
        .get_object(&deleted.object_id)
        .unwrap()
        .expect("actual committed tombstone metadata");
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
    drop(dfs);
    _runtime.close().unwrap();
}

#[test]
fn mkdir_over_tombstone_applies_current_managed_directory_policy() {
    let node = NodeId::from_legacy_named("node_mkdir_tombstone_managed_policy").expect("node id");
    let (_runtime, dfs) = native_sifs_service(
        "mkdir_over_tombstone_applies_current_managed_directory_policy",
        node.clone(),
    );
    let path = "/services/deleted-file";
    drop(dfs);
    let mut dfs = _runtime.metadata_primary_service(path).unwrap();
    let deleted = dfs
        .posix_write_file_with_create_attrs(
            path,
            b"deleted",
            DfsWriteOptions::replicated_eventual(vec![node]),
            DfsPosixCreateAttrs::new(0o600, 2001, 2002),
        )
        .expect("create file");
    let mut deleted_metadata = dfs
        .metadata_backend()
        .unwrap()
        .get_object(&deleted.object_id)
        .unwrap()
        .expect("actual committed file metadata");
    deleted_metadata.policy = DfsObjectPolicy::LocalOwned;
    dfs.add_object_metadata(deleted_metadata)
        .expect("install distinct deleted policy");
    dfs.posix_unlink(path).expect("tombstone file");

    // A metadata filesystem row cannot replace the canonical managed rule.
    // Apply the new rule through the actual native configuration producer.
    let owner = dfs.node_id().clone();
    drop(dfs);
    let accepted = _runtime
        .apply_placement_policy(
            &owner,
            path,
            DfsPlacementScope::Subtree,
            DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 2, 2).unwrap(),
        )
        .unwrap();
    let control = _runtime.shared_control_plane().unwrap();
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    loop {
        let job = control
            .with_client(|kv| {
                DfsControlPlaneStore::new(kv.as_mut())
                    .placement_reconciliation(&accepted.file_system_id, &accepted.job_id)
            })
            .unwrap()
            .unwrap();
        assert_eq!(job.accepted_config, accepted.accepted_config);
        assert!(job.last_error.is_none());
        if job.state == DfsPlacementReconciliationState::Completed {
            break;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "empty native placement Apply must finish"
        );
        _runtime
            .maintenance_tick(&owner, remaining)
            .unwrap()
            .outcome
            .unwrap();
    }
    let mut dfs = _runtime.metadata_primary_service(path).unwrap();
    let current = dfs
        .resolved_placement_policy_for_path(path, DfsObjectPolicy::ReplicatedEventual, 1)
        .unwrap();
    assert_eq!(
        current.policy.consistency,
        DfsObjectPolicy::ReplicatedConfirmed
    );
    assert_eq!(current.policy.min_commit_replicas, 2);
    assert_eq!(current.policy.desired_replicas, 2);

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
    drop(dfs);
    _runtime.close().unwrap();
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
    let bytes = b"local bytes remain after replica removal";
    let (runtime, author, stat, removed) = super::repair::native_withdrawn_local_replica_fixture(
        "reconnect-respects-managed-placement",
        bytes,
    );
    let local = removed.replica_node_id.clone();
    let current = author
        .metadata_backend()
        .unwrap()
        .get_object(&stat.object_id)
        .unwrap()
        .unwrap();
    assert_eq!(current.current_locations.len(), 1);
    let remote = current.current_locations[0].clone();
    assert_ne!(local, remote);
    assert_eq!(
        author
            .content()
            .read_blob(stat.content_hash.as_deref().unwrap())
            .unwrap(),
        bytes
    );
    drop(author);

    let mut policy = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 1).unwrap();
    policy.forbidden_node_ids = vec![local.clone()];
    policy.required_node_ids = vec![remote.clone()];
    let accepted = runtime
        .apply_placement_policy(&local, &stat.path, DfsPlacementScope::SelfOnly, policy)
        .unwrap();
    let control = runtime.shared_control_plane().unwrap();
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    loop {
        let job = control
            .with_client(|kv| {
                DfsControlPlaneStore::new(kv.as_mut())
                    .placement_reconciliation(&accepted.file_system_id, &accepted.job_id)
            })
            .unwrap()
            .unwrap();
        assert_eq!(job.accepted_config, accepted.accepted_config);
        assert!(
            job.last_error.is_none(),
            "actual managed placement: {job:?}"
        );
        if job.state == DfsPlacementReconciliationState::Completed {
            break;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(!remaining.is_zero(), "normal managed placement must finish");
        runtime
            .maintenance_tick(&local, remaining)
            .unwrap()
            .outcome
            .unwrap();
    }
    let mut dfs = runtime.metadata_primary_service(&stat.path).unwrap();
    assert!(
        dfs.resolved_placement_policy_for_path(&stat.path, DfsObjectPolicy::ReplicatedEventual, 2)
            .unwrap()
            .policy
            .forbidden_node_ids
            .contains(&local),
        "the actual native rule must forbid this reconnecting holder"
    );
    assert_eq!(
        dfs.content()
            .read_blob(stat.content_hash.as_deref().unwrap())
            .unwrap(),
        bytes
    );
    // Re-establish an ordinary read cache from the genuine remaining holder.
    // This is DATA only: neither the cache fill nor reconnect may enroll this
    // forbidden node as a replica or restore its withdrawn durable receipt.
    assert_eq!(
        dfs.request_remote_object_read(&stat.object_id).unwrap(),
        vec![remote.clone()]
    );
    let cache_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    while !dfs.reads.pending_reads.is_empty() {
        dfs.process_incoming().unwrap();
        assert!(
            Instant::now() < cache_deadline,
            "actual cache read must finish"
        );
    }
    let reconciliation = dfs.reconcile_local_manifests(&stat.object_id).unwrap();
    assert_eq!(reconciliation.refreshed_receipts, 0);
    assert!(
        !reconciliation.missing_current,
        "the actual local bytes still exist"
    );
    assert!(
        dfs.metadata_backend()
            .unwrap()
            .receipts_for_version(&stat.object_id, &stat.version)
            .unwrap()
            .iter()
            .all(|receipt| receipt.replica_node_id != local),
        "reconnect must not recreate the removed local receipt"
    );
    let current = dfs
        .metadata_backend()
        .unwrap()
        .get_object(&stat.object_id)
        .unwrap()
        .unwrap();
    assert_eq!(current.version, stat.version);
    assert_eq!(
        current.current_locations,
        vec![remote],
        "reconnect must not recreate the removed local location"
    );
    assert_eq!(
        dfs.content()
            .read_blob(stat.content_hash.as_deref().unwrap())
            .unwrap(),
        bytes,
        "placement removal leaves the original bytes available as a local cache"
    );
    drop(dfs);
    runtime.close().unwrap();
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
    let cluster = sifs_posix_cluster("sifs-cluster-ns-converge", "/users");
    let SifsPosixCluster {
        root,
        primary_node,
        writer_node,
        observer_node: _,
        primary,
        mut writer,
        mut observer,
        rejections: _,
        _runtime,
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
        let publication = _runtime
            .maintenance_tick(
                &primary_node,
                deadline
                    .saturating_duration_since(Instant::now())
                    .min(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT),
            )
            .expect("ordinary Main publication after replica namespace ops");
        assert!(publication.outcome.is_ok(), "{:?}", publication.outcome);
        assert_eq!(publication.omitted_kv_event_count, 0);
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

    drop(_runtime);
}

#[test]
fn sifs_cluster_posix_reverse_direction_move_resolves_by_id_on_replicas() {
    // Replicas hold no object→shard index: after a services → users move the
    // eviction tombstone lives in the "services" shard, which sorts *before*
    // "users". By-id resolution must prefer the live record regardless of
    // shard naming.
    let cluster = sifs_two_primary_cluster("sifs-xshard-reverse");
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

    // The local-read assertion needs a real content replica. A caller's
    // candidate hint does not pin the native placement decision.
    let mut placement = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 1).unwrap();
    placement.required_node_ids = vec![replica_node.clone()];
    _runtime
        .apply_placement_policy(
            &services_primary_node,
            "/services/origin.txt",
            DfsPlacementScope::Subtree,
            placement,
        )
        .unwrap();
    // Open the caller after fixture configuration so its retained write
    // projection contains this policy, then pass those exact constraints.
    drop(replica);
    let mut replica = _runtime.client(&replica_node).unwrap();
    let (_, _, effective_policy) = replica
        .managed_replication_policy_for_path(
            "/services/origin.txt",
            DfsObjectPolicy::ReplicatedEventual,
            1,
        )
        .unwrap();
    assert_eq!(
        effective_policy.required_node_ids,
        vec![replica_node.clone()]
    );
    let original = replica
        .posix_write_file(
            "/services/origin.txt",
            &bytes,
            DfsWriteOptions {
                replica_candidates: vec![replica_node.clone()],
                policy: effective_policy,
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .expect("create under services");
    assert_eq!(original.current_locations, vec![replica_node.clone()]);
    assert!(
        replica
            .content()
            .has_blob(original.content_hash.as_ref().unwrap())
            .unwrap()
    );
    let source_intents = _runtime
        .metadata_primary_service("/services")
        .unwrap()
        .metadata_backend()
        .unwrap()
        .intents_for_object(&original.object_id)
        .unwrap();
    let target_epoch = _runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|shard| shard.shard_id == "users")
        .unwrap()
        .epoch;
    assert!(
        source_intents
            .iter()
            .any(|intent| intent.status == DfsWriteIntentStatus::Committed
                && intent.owner_epoch > target_epoch),
        "this real move must carry an original receipt from a higher independent source epoch"
    );
    replica
        .posix_rename("/services/origin.txt", "/users/relocated.txt")
        .expect("reverse-direction cross-primary rename");
    let target = _runtime.metadata_primary_service("/users").unwrap();
    for original in &source_intents {
        assert_eq!(
            target
                .metadata_backend()
                .unwrap()
                .get_intent(&original.object_id, &original.intent_id)
                .unwrap(),
            Some(original.clone()),
            "adoption preserves the exact original terminal outcome, including its source epoch"
        );
    }
    drop(target);
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

    cluster.close();
}

#[test]
fn mounted_read_policy_does_not_escalate_lookup_failure_to_sync_path() {
    fn read_service(
        root: &Path,
        name: &str,
        local_node: &NodeId,
        primary_node: &NodeId,
        control_plane: Box<dyn DfsControlPlaneKvClient>,
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
                        committed_work: Default::default(),
                        result: DfsNamespaceResult::Rejected {
                            reason: "injected authoritative lookup failure".to_string(),
                            posix_error: None,
                            current_primary_node_id: None,
                        },
                        synced_path_object: None,
                        synced_path_objects: Vec::new(),
                        metadata_journal_batches: Vec::new(),
                        metadata_journal_has_more: false,
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
        .with_boxed_control_plane(control_plane);
        (service, calls)
    }

    let root = temp_root("mounted-read-no-implicit-sync-path");
    // Only the transport's rejection is synthetic. All authority and member
    // capability records come from an actual mounted native configuration.
    let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(
        "mounted-read-no-implicit-sync-path",
        2,
    );
    let mut runtime = crate::test_runtime::DfsTestRuntime::new(
        "mounted-read-no-implicit-sync-path",
        nodes[0].clone(),
    )
    .unwrap();
    runtime.define_node(nodes[1].clone()).unwrap();
    runtime.start_nodes().unwrap();
    let services = runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|shard| shard.shard_id == "services")
        .unwrap();
    let primary_node = services.primary_node_id.clone();
    let local_node = nodes
        .into_iter()
        .find(|node| node != &primary_node)
        .unwrap();
    // The compatibility fallback is specifically an unassigned observer path;
    // an assigned replica correctly refuses this uncorroborated lookup failure.
    let assignment = runtime
        .reassign_metadata_shard(
            &services.file_system_id,
            &services.shard_id,
            primary_node.clone(),
            Vec::new(),
        )
        .unwrap();
    assert!(!assignment.replica_node_ids.contains(&local_node));
    let mut observed_client = runtime.client(&local_node).unwrap();
    let control_plane = observed_client.control_plane.take().unwrap();
    drop(observed_client);

    let (mut ordinary, ordinary_calls) =
        read_service(&root, "ordinary", &local_node, &primary_node, control_plane);
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

    let mut observed_client = runtime.client(&local_node).unwrap();
    let control_plane = observed_client.control_plane.take().unwrap();
    drop(observed_client);
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

    drop((ordinary, mounted));
    runtime.close().unwrap();
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn placement_reconciliation_resumes_durable_phase_and_updates_existing_file() {
    let local =
        crate::test_runtime::DfsTestRuntime::ordered_node_ids("native-placement-restart", 1)
            .remove(0);
    let mut runtime =
        crate::test_runtime::DfsTestRuntime::new("native-placement-restart", local.clone())
            .unwrap();
    runtime.start_nodes().unwrap();
    let file_system = runtime.file_system();
    let control_plane = runtime.shared_control_plane().unwrap();
    let mut service = runtime.client(&local).unwrap();
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
        let mut store = DfsControlPlaneStore::new(kv.as_mut());
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
            accepted_config: desired.clone(),
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

    runtime.stop_node(&local).unwrap();
    runtime.reopen_node(local.clone()).unwrap();
    let mut reopened = runtime.client(&local).unwrap();
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
            DfsControlPlaneStore::new(kv.as_mut())
                .placement_reconciliation(&file_system.file_system_id, &job.job_id)
                .expect("durable job"),
            Some(completed)
        );
    });
    drop(reopened);
    drop(control_plane);
    runtime.close().unwrap();
}
