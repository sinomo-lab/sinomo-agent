use super::*;

#[test]
fn file_history_policy_survives_visible_metadata_refresh_across_shards() {
    let root = temp_root("history-policy-visible-refresh-shards");
    let owner = NodeId::generate();
    let mut dfs = networked_sifs_with_backend(&root.join("initial"), owner.clone());
    let stat = dfs
        .posix_write_file(
            "/services/history-policy.txt",
            b"current",
            DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .unwrap();
    let backend = dfs.metadata_backend().unwrap();
    let file_system = backend
        .get_file_system(&sifs_file_system_id())
        .unwrap()
        .unwrap();
    let object = backend.get_object(&stat.object_id).unwrap().unwrap();
    let version = backend
        .get_version(&stat.object_id, &stat.version)
        .unwrap()
        .unwrap();
    let configs = vec![
        DfsMetadataShardConfig {
            file_system_id: sifs_file_system_id(),
            shard_id: "root".into(),
            primary_node_id: owner.clone(),
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: DfsMetadataNamespacePrepareCodec::PackedV3,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        },
        DfsMetadataShardConfig {
            file_system_id: sifs_file_system_id(),
            shard_id: "services".into(),
            primary_node_id: owner,
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::PathPrefix {
                prefix: "/services".into(),
            },
            namespace_prepare_codec: DfsMetadataNamespacePrepareCodec::PackedV3,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        },
    ];
    let metadata_root = root.join("sharded");
    let mut sharded = open_local_sharded_metadata_backend(&metadata_root, configs).unwrap();
    sharded.put_file_system(&file_system).unwrap();
    sharded.put_object_and_version(&object, &version).unwrap();
    let mut enabled = file_system.clone();
    enabled.file_history = DfsFileHistoryPolicy {
        rules: vec![DfsFileHistoryRule {
            path: "/services".into(),
            retention: DfsFileHistoryRetention {
                keep_last: 1,
                keep_for_seconds: 0,
            },
        }],
    };
    sharded
        .ensure_file_system_on_shard(&DfsMetadataShardId::new("services"), &enabled)
        .unwrap();
    // A policy is refreshed on the writing shard. Sibling shards may still
    // contain the earlier definition, which must not become update authority.
    assert_eq!(
        sharded.get_file_system(&sifs_file_system_id()).unwrap(),
        Some(file_system.clone())
    );
    dfs = dfs.with_metadata_backend(sharded).unwrap();
    let mut observed = object;
    for combined in [false, true] {
        observed.current_locations.push(NodeId::generate());
        if combined {
            dfs.put_visible_object_and_version_in_metadata_backend(
                &observed,
                observed.content_size_bytes,
            )
            .unwrap();
        } else {
            dfs.put_visible_object_in_metadata_backend(&observed)
                .unwrap();
        }
        for (shard, expected) in [("root", &file_system), ("services", &enabled)] {
            let inspection = LocalSobjMetadataBackend::open_for_inspection(
                DfsMetadataShardId::new(shard),
                metadata_root.join(shard),
            )
            .unwrap();
            assert_eq!(
                inspection.get_file_system(&sifs_file_system_id()).unwrap(),
                Some(expected.clone()),
                "visible metadata refresh must preserve each shard's policy"
            );
        }
    }
    std::fs::remove_dir_all(root).ok();
}

fn history_control_plane(
    mut kv: InProcessKvStore,
) -> SharedDfsControlPlaneKvClient<InProcessKvStore> {
    // These fixtures author a capability-validated V3 shard. Model the same
    // authoritative membership records required by the production boundary.
    let shards = DfsControlPlaneStore::new(&mut kv)
        .list_metadata_shards(&sifs_file_system_id())
        .unwrap();
    let nodes = shards
        .iter()
        .flat_map(|shard| {
            std::iter::once(shard.primary_node_id.clone())
                .chain(shard.replica_node_ids.iter().cloned())
        })
        .collect::<BTreeSet<_>>();
    let cluster = nodes
        .iter()
        .find_map(|node| {
            kv.get(&sinomo_core::node::cluster_member_key(node))
                .and_then(|entry| entry.decode::<sinomo_core::ClusterMemberRecord>().ok())
                .map(|member| member.cluster_id)
        })
        .unwrap_or_else(sinomo_core::Id::generate);
    for node in nodes {
        let key = sinomo_core::node::cluster_member_key(&node);
        if kv.get(&key).is_some() {
            continue;
        }
        let mut member = sinomo_core::ClusterMemberRecord::local_non_voting(
            cluster.clone(),
            node.clone(),
            "history fixture",
            "history-key",
            OffsetDateTime::UNIX_EPOCH,
        );
        member.state = sinomo_core::ClusterMemberState::Active;
        member.capabilities = sinomo_core::node::main_daemon_capabilities(&member.role);
        member.allowed_capabilities = member.capabilities.clone();
        kv.put_serde(key, &member, None).unwrap();
    }
    SharedDfsControlPlaneKvClient::new(kv)
}

#[test]
fn file_history_incomplete_repairs_resume_on_a_new_metadata_authority() {
    let root = temp_root("history-repair-new-authority");
    let original = NodeId::generate();
    let promoted = NodeId::generate();
    let replica = NodeId::generate();
    let network = InProcessNetwork::new();
    let mut source = NetworkedDfsService::new(
        network.add_node(original.clone()),
        LocalContentStore::new(root.join("source-content")).unwrap(),
    )
    .with_metadata_backend(open_root_local_metadata_backend(root.join("source-metadata")).unwrap())
    .unwrap();
    source.bootstrap_distributed_sifs().unwrap();
    let fs = source
        .metadata_backend()
        .unwrap()
        .get_file_system(&sifs_file_system_id())
        .unwrap()
        .unwrap();
    let mut kv = InProcessKvStore::new();
    for node in [&original, &promoted, &replica] {
        let mut member = sinomo_core::ClusterMemberRecord::local_non_voting(
            sinomo_core::Id::generate(),
            node.clone(),
            "repair authority fixture",
            "repair-key",
            OffsetDateTime::UNIX_EPOCH,
        );
        member.state = sinomo_core::ClusterMemberState::Active;
        member.capabilities = sinomo_core::node::main_daemon_capabilities(&member.role);
        member.allowed_capabilities = member.capabilities.clone();
        kv.put_serde(sinomo_core::node::cluster_member_key(node), &member, None)
            .unwrap();
        kv.put_serde(
            sinomo_core::node::cluster_capability_policy_key(node),
            &sinomo_core::node::ClusterCapabilityPolicyRecord::from(&member),
            None,
        )
        .unwrap();
    }
    let mut shard = DfsMetadataShardConfig {
        file_system_id: fs.file_system_id.clone(),
        shard_id: "root".into(),
        primary_node_id: original.clone(),
        replica_node_ids: Vec::new(),
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: DfsMetadataNamespacePrepareCodec::PackedV3,
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let mut store = DfsControlPlaneStore::new(&mut kv);
    store
        .put_file_system_config(&DfsFileSystemConfig::from(&fs))
        .unwrap();
    store.put_metadata_shard(&shard).unwrap();
    let shared = history_control_plane(kv);
    source = source.with_control_plane(shared.clone());
    let path = "/services/repair-after-authority-change";
    let saved = source
        .posix_write_file(
            path,
            b"retained",
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(1, 2).unwrap(),
                replica_candidates: vec![original.clone(), replica.clone()],
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    capture_history(&mut source, &saved);
    source
        .posix_write_file(
            path,
            b"current",
            DfsWriteOptions::replicated_eventual(vec![original.clone()]),
        )
        .unwrap();
    let current = source
        .metadata_backend()
        .unwrap()
        .get_object(&saved.object_id)
        .unwrap()
        .unwrap();
    let batch = source
        .metadata_backend()
        .unwrap()
        .journal_batch_since_limited(0, 1024, usize::MAX)
        .unwrap()
        .unwrap();
    let mut transferred = open_root_local_metadata_backend(root.join("promoted-metadata")).unwrap();
    transferred.apply_journal_batch(&batch).unwrap();
    // Model the image after a certified handoff. The separate protocol/live
    // cases prove activation; this case proves recovery of incomplete content
    // work from the transferred metadata, with a different executor identity.
    shard.primary_node_id = promoted.clone();
    shard.epoch = 2;
    shared
        .with_client(|kv| {
            kv.put_serde(
                crate::control_plane::metadata_shard_key(&shard.file_system_id, &shard.shard_id),
                &shard,
                None,
            )
        })
        .unwrap();
    let mut successor = NetworkedDfsService::new(
        network.add_node(promoted),
        LocalContentStore::new(root.join("promoted-content")).unwrap(),
    )
    .with_control_plane(shared.clone())
    .with_metadata_backend(transferred)
    .unwrap();
    successor
        .activate_file_history_repair_roots([(saved.object_id.clone(), saved.version.clone())])
        .unwrap();
    assert!(
        successor
            .coordinator
            .repair_jobs()
            .iter()
            .any(|job| job.version == saved.version && job.missing_replica_node_id == replica)
    );
    let mut destination = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        LocalContentStore::new(root.join("replica-content")).unwrap(),
    )
    .with_control_plane(shared);
    destination.metadata.add_object(current.clone()).unwrap();
    destination
        .content
        .put_object_version(
            current.object_id.as_str(),
            current.version.as_str(),
            b"current",
        )
        .unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let source_stop = Arc::clone(&stop);
    let source_thread = std::thread::spawn(move || {
        while !source_stop.load(Ordering::SeqCst) {
            source.process_incoming().unwrap();
            std::thread::sleep(Duration::from_millis(1));
        }
        source
    });
    let replica_stop = Arc::clone(&stop);
    let replica_thread = std::thread::spawn(move || {
        while !replica_stop.load(Ordering::SeqCst) {
            destination.process_incoming().unwrap();
            std::thread::sleep(Duration::from_millis(1));
        }
        destination
    });
    let result = (|| -> Result<(), DfsError> {
        let deadline = Instant::now() + Duration::from_secs(5);
        while successor
            .coordinator
            .repair_jobs()
            .iter()
            .any(|job| job.version == saved.version)
        {
            successor.retry_pending_repairs()?;
            successor.process_incoming()?;
            if Instant::now() >= deadline {
                return Err(DfsError::Metadata {
                    message: "retained repair did not finish".into(),
                });
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        Ok(())
    })();
    stop.store(true, Ordering::SeqCst);
    let source = source_thread.join().unwrap();
    let destination = replica_thread.join().unwrap();
    result.unwrap();
    assert_eq!(
        destination
            .content
            .read_object_version(saved.object_id.as_str(), saved.version.as_str())
            .unwrap()
            .1,
        b"retained"
    );
    assert_eq!(
        destination
            .metadata
            .object(&current.object_id)
            .unwrap()
            .version,
        current.version
    );
    assert_eq!(
        successor
            .metadata_backend()
            .unwrap()
            .get_object(&current.object_id)
            .unwrap()
            .unwrap()
            .version,
        current.version
    );
    assert_eq!(
        successor
            .coordinator
            .committed_version(&current.file_system_id, &current.object_id),
        Some(&current.version)
    );
    assert_eq!(source.posix_read_file(path).unwrap(), b"current");
    let plan = successor
        .metadata_backend()
        .unwrap()
        .get_version(&saved.object_id, &saved.version)
        .unwrap()
        .unwrap()
        .content_durability
        .unwrap();
    assert_eq!(plan.policy, DfsReplicationPolicy::new(1, 2).unwrap());
    assert_eq!(plan.coordinator_node_id, original);
    assert_eq!(
        plan.owner_epoch, 1,
        "the original grant stays immutable under a new executor"
    );
}

#[test]
fn file_history_committed_repair_cannot_override_a_later_lower_sorting_head() {
    let root = temp_root("history-repair-read-view");
    let owner = NodeId::generate();
    let remote = NodeId::generate();
    let mut writer = networked_sifs_with_backend(&root, owner.clone());
    let path = "/services/repair-read-view";
    let initial = writer
        .posix_write_file(
            path,
            b"seed",
            DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .unwrap();
    // Stable restore identities are opaque and may sort after later writes.
    let restored_version = DfsVersion::new("7zzzzzzzzzzzzzzzzzzzzzzzzz").unwrap();
    let outcome = writer
        .write_distributed_bytes_with_base(
            DfsWriteRequest {
                file_system_id: initial.file_system_id.clone(),
                object_id: initial.object_id.clone(),
                target_version: restored_version.clone(),
                requesting_node_id: owner.clone(),
            },
            Some(initial.version),
            b"restored".to_vec(),
            vec![owner.clone(), remote],
            DfsReplicationPolicy::new(1, 2).unwrap(),
            crate::service_write::DfsDistributedWriteOptions {
                repair: crate::service_write::DfsWriteRepair::Background,
                ..Default::default()
            },
        )
        .unwrap();
    assert!(matches!(
        outcome,
        NetworkedDfsWriteOutcome::Committed { .. }
    ));
    let mut reader = networked_sifs_with_backend(&root, owner.clone());
    assert!(
        reader
            .writes
            .pending_writes
            .values()
            .any(|pending| pending.version == restored_version)
    );
    assert_eq!(reader.posix_read_file(path).unwrap(), b"restored");
    let later = writer
        .posix_write_file(
            path,
            b"after restore",
            DfsWriteOptions::replicated_eventual(vec![owner]),
        )
        .unwrap();
    assert!(later.version < restored_version);
    assert_eq!(
        reader
            .metadata_backend()
            .unwrap()
            .get_object(&later.object_id)
            .unwrap()
            .unwrap()
            .version,
        later.version
    );
    assert_eq!(reader.posix_read_file(path).unwrap(), b"after restore");
    let mut streamed = Vec::new();
    std::io::Read::read_to_end(
        &mut reader.posix_open_current_file_stream(path).unwrap().reader,
        &mut streamed,
    )
    .unwrap();
    assert_eq!(streamed, b"after restore");
}

/// Deliver transfers to a real receiver/content store before returning its
/// response. Corruption affects only the response, after durable ingestion.
struct HistoryGrantTransport {
    inner: sinomo_core::network::InProcessTransport,
    replica: NetworkedDfsService<sinomo_core::network::InProcessTransport>,
    corrupt_receipt: bool,
}

impl DfsTransportIdentity for HistoryGrantTransport {
    fn local_node_id(&self) -> &NodeId {
        self.inner.local_node_id()
    }
}

impl DfsMessageTransport for HistoryGrantTransport {
    fn send_routed_call(
        &mut self,
        target: &NodeId,
        call: DfsRoutedCall,
    ) -> Result<(), NetworkError> {
        self.inner.send_routed_call(target, call)?;
        self.replica
            .process_incoming()
            .expect("real replica handles transfer");
        Ok(())
    }

    fn drain_incoming_messages(&mut self) -> Result<Vec<DfsRoutedMessage>, NetworkError> {
        let mut messages = self.inner.drain_incoming_messages()?;
        if self.corrupt_receipt {
            for message in &mut messages {
                if let DfsNetworkMessage::StageBlobResponse(response) = &mut message.message {
                    response.result = DfsBlobTransferResult::Durable {
                        content_hash: "wrong-hash".into(),
                    };
                }
            }
        }
        Ok(messages)
    }
}

fn legacy_grant_fixture(
    root: &Path,
    w: usize,
    corrupt_receipt: bool,
) -> (
    NetworkedDfsService<HistoryGrantTransport>,
    DfsObjectMetadata,
    NodeId,
) {
    let owner = NodeId::generate();
    let remote = NodeId::generate();
    let network = InProcessNetwork::new();
    let transport = network.add_node(owner.clone());
    let remote_transport = network.add_node(remote.clone());
    let mut kv = InProcessKvStore::new();
    for node in [&owner, &remote] {
        let mut member = sinomo_core::ClusterMemberRecord::local_non_voting(
            sinomo_core::Id::generate(),
            node.clone(),
            "grant fixture",
            "grant-key",
            OffsetDateTime::UNIX_EPOCH,
        );
        member.state = sinomo_core::ClusterMemberState::Active;
        member.capabilities = sinomo_core::node::main_daemon_capabilities(&member.role);
        member.allowed_capabilities = member.capabilities.clone();
        kv.put_serde(sinomo_core::node::cluster_member_key(node), &member, None)
            .unwrap();
        kv.put_serde(
            sinomo_core::node::cluster_capability_policy_key(node),
            &sinomo_core::node::ClusterCapabilityPolicyRecord::from(&member),
            None,
        )
        .unwrap();
        DfsControlPlaneStore::new(&mut kv)
            .advertise_content_service(&DfsNodeAdvertisement::content(node.clone()))
            .unwrap();
    }
    let mut fs = file_system(owner.clone());
    fs.subtree_policies.push(
        DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, w, 2)
            .unwrap()
            .into_rule("/services".into(), DfsPlacementScope::Subtree),
    );
    let mut control = DfsControlPlaneStore::new(&mut kv);
    control
        .put_file_system_config(&DfsFileSystemConfig::from(&fs))
        .unwrap();
    control
        .put_metadata_shard(&DfsMetadataShardConfig {
            file_system_id: fs.file_system_id.clone(),
            shard_id: "root".into(),
            primary_node_id: owner.clone(),
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: DfsMetadataNamespacePrepareCodec::PackedV3,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        })
        .unwrap();
    let shared = history_control_plane(kv);
    let replica = NetworkedDfsService::new(
        remote_transport,
        LocalContentStore::new(root.join("replica-content")).unwrap(),
    )
    .with_control_plane(shared.clone());
    let mut dfs = NetworkedDfsService::new(
        HistoryGrantTransport {
            inner: transport,
            replica,
            corrupt_receipt,
        },
        LocalContentStore::new(root.join("content")).unwrap(),
    )
    .with_metadata_backend(open_root_local_metadata_backend(root.join("metadata")).unwrap())
    .unwrap();
    dfs.bootstrap_distributed_sifs().unwrap();
    dfs.add_file_system(fs).unwrap();
    let mut object = file_metadata(owner);
    object.path = "/services/legacy-grant".into();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    let manifest = dfs
        .content
        .put_object_version(
            object.object_id.as_str(),
            object.version.as_str(),
            b"legacy bytes",
        )
        .unwrap();
    object.content_hash = Some(manifest.content_hash);
    object.content_size_bytes = Some(12);
    dfs.metadata_backend
        .as_mut()
        .unwrap()
        .put_object_and_version(&object, &crate::object_version_record(&object, Some(12)))
        .unwrap();
    dfs.metadata.add_object(object.clone()).unwrap();
    (dfs.with_control_plane(shared), object, remote)
}

#[test]
fn file_history_legacy_fresh_grant_requires_current_policy_receipts_and_replays() {
    for corrupt in [false, true] {
        let root = temp_root("history-fresh-grant");
        let (mut dfs, object, remote) = legacy_grant_fixture(&root, 2, corrupt);
        let request = DfsFileHistoryPinRequest {
            path: object.path.clone(),
            identity: DfsFileVersionIdentity {
                file_system_id: object.file_system_id.clone(),
                object_id: object.object_id.clone(),
                version: object.version.clone(),
            },
            operation: DfsFileHistoryPinOperation::Capture {
                pin_id: "fresh-grant".into(),
            },
            operation_id: sinomo_core::Id::generate().to_string(),
        };
        let outcome = dfs.file_history_pin(request.clone());
        let backend = dfs.metadata_backend().unwrap();
        let captured = backend
            .get_version(&object.object_id, &object.version)
            .unwrap()
            .unwrap();
        assert_eq!(
            backend.get_object(&object.object_id).unwrap(),
            Some(object.clone())
        );
        if corrupt {
            assert!(
                outcome
                    .unwrap_err()
                    .to_string()
                    .contains("requires 2 receipts")
            );
            assert!(captured.retained.is_none());
            assert!(captured.content_durability.is_none());
            assert!(
                backend
                    .version_pins_for_object(&object.object_id)
                    .unwrap()
                    .is_empty()
            );
            continue;
        }
        outcome.unwrap();
        let grant = captured.content_durability.as_ref().unwrap();
        assert!(grant.current_policy_granted_at_unix_nanos.is_some());
        assert_eq!(grant.policy.min_commit_replicas, 2);
        assert_eq!(grant.policy.desired_replicas, 2);
        assert!(grant.desired_replica_node_ids.contains(&remote));
        assert_eq!(
            dfs.transport
                .replica
                .content
                .read_object_version(object.object_id.as_str(), object.version.as_str())
                .unwrap()
                .1,
            b"legacy bytes"
        );
        dfs.file_history_pin(request).unwrap();
        assert_eq!(
            dfs.metadata_backend()
                .unwrap()
                .get_version(&object.object_id, &object.version)
                .unwrap(),
            Some(captured.clone())
        );
        drop(dfs);
        let reopened = open_root_local_metadata_backend(root.join("metadata")).unwrap();
        assert_eq!(
            reopened
                .get_version(&object.object_id, &object.version)
                .unwrap(),
            Some(captured)
        );
        assert_eq!(
            reopened.get_object(&object.object_id).unwrap(),
            Some(object)
        );
    }
}

#[test]
fn file_history_legacy_fresh_grant_keeps_missing_n_repair_ownership() {
    let root = temp_root("history-fresh-grant-repair");
    let (mut dfs, object, remote) = legacy_grant_fixture(&root, 1, false);
    // A placement job has its own lifetime and cannot suppress the new
    // capture's repair owner, even when it names this exact generation.
    dfs.metadata_backend
        .as_mut()
        .unwrap()
        .put_intent(&DfsWriteIntentRecord {
            publication: DfsWritePublication::Current,
            intent_id: format!(
                "placement:retired-job:{}:{}",
                object.object_id, object.version
            ),
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            base_version: Some(object.version.clone()),
            target_version: object.version.clone(),
            coordinator_node_id: object.owner_node_id.clone(),
            operation_id: None,
            operation_order_token: None,
            owner_epoch: 1,
            desired_replica_node_ids: vec![object.owner_node_id.clone()],
            policy: DfsReplicationPolicy::new(1, 1).unwrap(),
            status: DfsWriteIntentStatus::Committed,
            content_hash: object.content_hash.clone(),
            object_metadata: Some(object.clone()),
            touch_parent_on_create: false,
            create_predecessor_object_id: None,
            created_at: OffsetDateTime::now_utc(),
            updated_at: None,
        })
        .unwrap();
    dfs.file_history_pin(DfsFileHistoryPinRequest {
        path: object.path.clone(),
        identity: DfsFileVersionIdentity {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            version: object.version.clone(),
        },
        operation: DfsFileHistoryPinOperation::Capture {
            pin_id: "fresh-grant".into(),
        },
        operation_id: sinomo_core::Id::generate().to_string(),
    })
    .unwrap();
    assert!(
        dfs.coordinator
            .repair_jobs()
            .iter()
            .any(|job| job.version == object.version && job.missing_replica_node_id == remote)
    );
    dfs.retry_pending_repairs().unwrap();
    dfs.process_incoming().unwrap();
    assert_eq!(
        dfs.transport
            .replica
            .content
            .read_object_version(object.object_id.as_str(), object.version.as_str())
            .unwrap()
            .1,
        b"legacy bytes"
    );
    let mut current = dfs
        .metadata_backend()
        .unwrap()
        .get_object(&object.object_id)
        .unwrap()
        .unwrap();
    assert!(current.current_locations.contains(&remote));
    current.current_locations = object.current_locations.clone();
    assert_eq!(
        current, object,
        "repair may enrich locations but cannot change the file head"
    );
}

#[test]
fn file_history_legacy_rule_fresh_grant_fences_publication_on_durability() {
    for mode in [
        LegacyCaptureMode::Delete,
        LegacyCaptureMode::Replace,
        LegacyCaptureMode::Bulk,
    ] {
        for corrupt in [false, true] {
            let root = temp_root("history-fresh-rule-grant");
            let (mut dfs, object, _) = legacy_grant_fixture(&root, 2, corrupt);
            dfs.update_file_history_policy(DfsFileHistoryPolicyUpdateRequest {
                file_system_id: object.file_system_id.clone(),
                expected: Default::default(),
                policy: DfsFileHistoryPolicy {
                    rules: vec![DfsFileHistoryRule {
                        path: object.path.clone(),
                        retention: DfsFileHistoryRetention {
                            keep_last: 1,
                            keep_for_seconds: 0,
                        },
                    }],
                },
            })
            .unwrap();
            let result = match mode {
                LegacyCaptureMode::Delete => dfs.posix_unlink(&object.path).map(|_| ()),
                LegacyCaptureMode::Replace => dfs
                    .posix_write_file(
                        &object.path,
                        b"replacement",
                        DfsWriteOptions {
                            foreground_replica_repair: false,
                            ..DfsWriteOptions::replicated_eventual(vec![
                                object.owner_node_id.clone(),
                            ])
                        },
                    )
                    .map(|_| ()),
                LegacyCaptureMode::Bulk => dfs
                    .posix_write_files_local_eventual_with_known_parents(vec![(
                        object.path.as_str(),
                        b"replacement".as_slice(),
                    )])
                    .map(|_| ()),
                LegacyCaptureMode::Explicit => unreachable!(),
            };
            let backend = dfs.metadata_backend().unwrap();
            let version = backend
                .get_version(&object.object_id, &object.version)
                .unwrap()
                .unwrap();
            if corrupt {
                assert!(
                    result
                        .unwrap_err()
                        .to_string()
                        .contains("requires 2 receipts")
                );
                assert_eq!(backend.get_object(&object.object_id).unwrap(), Some(object));
                assert!(version.content_durability.is_none());
                assert!(version.retained.is_none());
            } else {
                result.unwrap();
                let current = backend.get_object(&object.object_id).unwrap().unwrap();
                assert_ne!(current.version, object.version);
                assert_eq!(
                    current.kind == DfsObjectKind::Tombstone,
                    matches!(mode, LegacyCaptureMode::Delete)
                );
                assert!(
                    version
                        .content_durability
                        .unwrap()
                        .current_policy_granted_at_unix_nanos
                        .is_some()
                );
                assert!(version.retained.unwrap().policy_capture.is_some());
            }
        }
    }
}

#[test]
fn file_history_policy_update_is_honored_by_fast_existing_file_writes() {
    let root = temp_root("history-policy-fast-commit");
    let owner = NodeId::generate();
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let options = DfsWriteOptions {
        foreground_replica_repair: false,
        ..DfsWriteOptions::replicated_eventual(vec![owner.clone()])
    };
    let path = "/services/policy-after-create";
    let before = dfs
        .posix_write_file(path, b"before-policy", options.clone())
        .unwrap();
    let fs = dfs
        .metadata_backend()
        .unwrap()
        .get_file_system(&before.file_system_id)
        .unwrap()
        .unwrap();
    let mut kv = InProcessKvStore::new();
    let mut member = sinomo_core::ClusterMemberRecord::local_non_voting(
        sinomo_core::Id::generate(),
        owner.clone(),
        "policy owner",
        "policy-owner-key",
        OffsetDateTime::UNIX_EPOCH,
    );
    member.state = sinomo_core::ClusterMemberState::Active;
    member.capabilities = sinomo_core::node::main_daemon_capabilities(&member.role);
    member.allowed_capabilities = member.capabilities.clone();
    kv.put_serde(sinomo_core::node::cluster_member_key(&owner), &member, None)
        .unwrap();
    kv.put_serde(
        sinomo_core::node::cluster_capability_policy_key(&owner),
        &sinomo_core::node::ClusterCapabilityPolicyRecord::from(&member),
        None,
    )
    .unwrap();
    let mut control = DfsControlPlaneStore::new(&mut kv);
    control
        .put_file_system_config(&DfsFileSystemConfig::from(&fs))
        .unwrap();
    control
        .put_metadata_shard(&DfsMetadataShardConfig {
            file_system_id: fs.file_system_id.clone(),
            shard_id: "root".into(),
            primary_node_id: owner,
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: DfsMetadataNamespacePrepareCodec::PackedV3,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        })
        .unwrap();
    let shared = history_control_plane(kv);
    dfs = dfs.with_control_plane(shared.clone());
    let admin_network = InProcessNetwork::new();
    let mut admin = NetworkedDfsService::new(
        admin_network.add_node(NodeId::generate()),
        LocalContentStore::new(root.join("policy-admin")).unwrap(),
    )
    .with_control_plane(shared);
    let policy = DfsFileHistoryPolicy {
        rules: vec![DfsFileHistoryRule {
            path: path.into(),
            retention: DfsFileHistoryRetention {
                keep_last: 1,
                keep_for_seconds: 0,
            },
        }],
    };
    admin
        .update_file_history_policy(DfsFileHistoryPolicyUpdateRequest {
            file_system_id: fs.file_system_id.clone(),
            expected: Default::default(),
            policy: policy.clone(),
        })
        .unwrap();
    dfs.posix_write_file(path, b"after-policy", options.clone())
        .unwrap();
    assert_eq!(
        history_entries(&mut dfs, path),
        vec![history_identity(&before)]
    );
    admin
        .update_file_history_policy(DfsFileHistoryPolicyUpdateRequest {
            file_system_id: fs.file_system_id,
            expected: policy,
            policy: Default::default(),
        })
        .unwrap();
    dfs.posix_write_file(path, b"after-disable", options)
        .unwrap();
    assert_eq!(
        history_entries(&mut dfs, path),
        vec![history_identity(&before)],
        "disabling future captures must retain the previously granted minimum without capturing another version"
    );
}

#[test]
fn file_history_count_retention_filters_reads_and_pages_before_gc() {
    let root = temp_root("history-live-grants");
    let owner = NodeId::generate();
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let path = "/services/live-grants";
    let mut fs = dfs
        .metadata_backend()
        .unwrap()
        .get_file_system(&sifs_file_system_id())
        .unwrap()
        .unwrap();
    fs.file_history.rules.push(DfsFileHistoryRule {
        path: path.into(),
        retention: DfsFileHistoryRetention {
            keep_last: 1,
            keep_for_seconds: 0,
        },
    });
    dfs.add_file_system(fs).unwrap();
    let options = DfsWriteOptions::replicated_eventual(vec![owner]);
    let first = dfs
        .posix_write_file(path, b"first", options.clone())
        .unwrap();
    let second = dfs
        .posix_write_file(path, b"second", options.clone())
        .unwrap();
    dfs.posix_write_file(path, b"current", options).unwrap();
    assert_eq!(
        history_entries(&mut dfs, path),
        vec![history_identity(&second)]
    );
    assert!(matches!(
        dfs.read_file_history(DfsFileHistoryReadRequest {
            identity: history_identity(&first),
            offset: 0,
            max_bytes: 64,
        })
        .unwrap(),
        DfsFileHistoryReadOutcome::NotRetained { .. }
    ));
    assert!(matches!(dfs.read_file_history(DfsFileHistoryReadRequest {
        identity: history_identity(&second), offset: 0, max_bytes: 64,
    }).unwrap(), DfsFileHistoryReadOutcome::Found { data } if data.bytes == b"second"));
    dfs.file_history_pin(DfsFileHistoryPinRequest {
        path: path.into(),
        identity: history_identity(&first),
        operation: DfsFileHistoryPinOperation::Pin {
            pin_id: "independent".into(),
        },
        operation_id: sinomo_core::Id::generate().to_string(),
    })
    .unwrap();
    assert_eq!(
        history_entries(&mut dfs, path).len(),
        2,
        "an independent pin still retains the older capture"
    );
    assert_eq!(dfs.posix_read_file(path).unwrap(), b"current");
}

fn history_entries<T: DfsMessageTransport>(
    dfs: &mut NetworkedDfsService<T>,
    path: &str,
) -> Vec<DfsFileVersionIdentity> {
    let mut request = DfsFileHistoryListRequest {
        file_system_id: sifs_file_system_id(),
        path: Some(path.into()),
        object_id: None,
        after: None,
        limit: 1,
    };
    let mut entries = Vec::new();
    loop {
        let page = dfs.list_file_history(request.clone()).unwrap();
        entries.extend(page.entries.into_iter().map(|entry| entry.identity));
        let Some(after) = page.next_cursor else { break };
        assert_ne!(request.after.as_ref(), Some(&after));
        request.after = Some(after);
    }
    entries
}

#[test]
fn file_history_legacy_metadata_capture_recovers_full_and_compact_write_plans() {
    legacy_capture_recovers_plans(LegacyCaptureMode::Explicit);
}

#[test]
fn file_history_legacy_rule_capture_journals_durability_before_publication() {
    for mode in [
        LegacyCaptureMode::Replace,
        LegacyCaptureMode::Delete,
        LegacyCaptureMode::Bulk,
    ] {
        legacy_capture_recovers_plans(mode);
    }
}

#[derive(Clone, Copy, Debug)]
enum LegacyCaptureMode {
    Explicit,
    Replace,
    Delete,
    Bulk,
}

fn apply_legacy_capture<T: DfsMessageTransport>(
    dfs: &mut NetworkedDfsService<T>,
    metadata: &DfsStat,
    owner: &NodeId,
    mode: LegacyCaptureMode,
) -> Result<(), String> {
    if matches!(mode, LegacyCaptureMode::Explicit) {
        dfs.file_history_pin(DfsFileHistoryPinRequest {
            path: metadata.path.clone(),
            identity: history_identity(metadata),
            operation: DfsFileHistoryPinOperation::Capture {
                pin_id: "checkpoint".into(),
            },
            operation_id: sinomo_core::Id::generate().to_string(),
        })
        .map_err(|error| error.to_string())?;
    }
    match mode {
        LegacyCaptureMode::Delete => dfs
            .posix_unlink(&metadata.path)
            .map(|_| ())
            .map_err(|error| error.to_string()),
        LegacyCaptureMode::Bulk => dfs
            .posix_write_files_local_eventual_with_known_parents(vec![(
                metadata.path.as_str(),
                b"replacement".as_slice(),
            )])
            .map(|_| ())
            .map_err(|error| error.to_string()),
        LegacyCaptureMode::Explicit | LegacyCaptureMode::Replace => dfs
            .posix_write_file(
                &metadata.path,
                b"replacement",
                DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
            )
            .map(|_| ())
            .map_err(|error| error.to_string()),
    }
}

fn legacy_capture_recovers_plans(mode: LegacyCaptureMode) {
    for ambiguous in [false, true] {
        for compact in [false, true] {
            let root = temp_root("legacy-history-durability");
            let owner = NodeId::generate();
            let mut dfs = networked_sifs_with_backend(&root, owner.clone());
            let mut shard = DfsMetadataShardConfig {
                file_system_id: sifs_file_system_id(),
                shard_id: "root".into(),
                primary_node_id: owner.clone(),
                replica_node_ids: Vec::new(),
                epoch: 1,
                assignment: DfsMetadataShardAssignment::Root,
                namespace_prepare_codec: DfsMetadataNamespacePrepareCodec::PackedV2,
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            };
            let mut kv = InProcessKvStore::new();
            let mut member = sinomo_core::ClusterMemberRecord::local_non_voting(
                sinomo_core::Id::generate(),
                owner.clone(),
                "legacy upgrade fixture",
                "legacy-upgrade-key",
                OffsetDateTime::UNIX_EPOCH,
            );
            member.state = sinomo_core::ClusterMemberState::Active;
            member.capabilities = sinomo_core::node::main_daemon_capabilities(&member.role);
            member.allowed_capabilities = member.capabilities.clone();
            kv.put_serde(sinomo_core::node::cluster_member_key(&owner), &member, None)
                .unwrap();
            kv.put_serde(
                sinomo_core::node::cluster_capability_policy_key(&owner),
                &sinomo_core::node::ClusterCapabilityPolicyRecord::from(&member),
                None,
            )
            .unwrap();
            kv.put_serde(
                crate::control_plane::metadata_shard_key(&shard.file_system_id, "root"),
                &shard,
                None,
            )
            .unwrap();
            let control = history_control_plane(kv);
            dfs = dfs.with_control_plane(control.clone());
            let data = dfs
                .posix_write_file(
                    "/services/legacy-history",
                    b"legacy bytes",
                    DfsWriteOptions {
                        replica_candidates: vec![
                            owner.clone(),
                            NodeId::generate(),
                            NodeId::generate(),
                        ],
                        policy: DfsReplicationPolicy::new(1, 3).unwrap(),
                        wait_for_commit: true,
                        foreground_replica_repair: false,
                    },
                )
                .unwrap();
            let metadata = dfs.posix_set_mode(&data.path, 0o640).unwrap();
            let backend = dfs.metadata_backend().unwrap();
            let legacy = backend
                .get_version(&metadata.object_id, &metadata.version)
                .unwrap()
                .unwrap();
            assert!(
                legacy.content_durability.is_none(),
                "write the fixture through the old codec"
            );
            let source = backend
                .intents_for_object(&data.object_id)
                .unwrap()
                .into_iter()
                .find(|intent| {
                    intent.target_version == data.version
                        && intent.status == DfsWriteIntentStatus::Committed
                })
                .unwrap();
            let mut unrelated = legacy.clone();
            unrelated.created_by_event_id = Some(EventId::generate());
            assert!(
                crate::content_durability::recover_from_backend(&unrelated, backend)
                    .unwrap()
                    .is_none(),
                "same bytes from unrelated content events cannot establish durability"
            );
            unrelated.created_by_event_id = None;
            assert!(
                crate::content_durability::recover_from_backend(&unrelated, backend)
                    .unwrap()
                    .is_none(),
                "missing provenance cannot be inferred from replica locations"
            );
            if compact {
                let backend = dfs.metadata_backend.as_mut().unwrap();
                let mut entry = backend.put_intent(&source).unwrap();
                entry.sequence += 1;
                entry.mutation = DfsMetadataMutation::PutIntentReceipt {
                    file_system_id: source.file_system_id.clone(),
                    object_id: source.object_id.clone(),
                    intent_id: source.intent_id.clone(),
                    receipt: DfsWriteIntentReceipt::from_intent(&source, OffsetDateTime::now_utc())
                        .unwrap(),
                };
                backend
                    .apply_journal_batch(&DfsMetadataJournalBatch {
                        shard_id: DfsMetadataShardId::new("root"),
                        next_after_sequence: entry.sequence,
                        entries: vec![entry],
                    })
                    .unwrap();
            }
            // The fixture changes only the local codec contract. Cross-node
            // capability admission and handoff are covered by the shard tests.
            shard.namespace_prepare_codec = DfsMetadataNamespacePrepareCodec::PackedV3;
            control
                .with_client(|kv| {
                    kv.put_serde(
                        crate::control_plane::metadata_shard_key(&shard.file_system_id, "root"),
                        &shard,
                        None,
                    )
                })
                .unwrap();
            if !matches!(mode, LegacyCaptureMode::Explicit) {
                let mut fs = dfs
                    .metadata_backend()
                    .unwrap()
                    .get_file_system(&metadata.file_system_id)
                    .unwrap()
                    .unwrap();
                fs.file_history.rules.push(DfsFileHistoryRule {
                    path: metadata.path.clone(),
                    retention: DfsFileHistoryRetention {
                        keep_last: 1,
                        keep_for_seconds: 0,
                    },
                });
                dfs.add_file_system(fs).unwrap();
            }
            if ambiguous {
                let mut other = source.clone();
                other.intent_id = "ambiguous-legacy-policy".into();
                other.operation_id = None;
                other.operation_order_token = None;
                other.policy = DfsReplicationPolicy::new(1, 2).unwrap();
                other.desired_replica_node_ids.truncate(2);
                dfs.metadata_backend
                    .as_mut()
                    .unwrap()
                    .put_intent(&other)
                    .unwrap();
                let error = apply_legacy_capture(&mut dfs, &metadata, &owner, mode).unwrap_err();
                assert!(
                    error.contains("conflicting recorded durability"),
                    "{mode:?}: {error}"
                );
                let backend = dfs.metadata_backend().unwrap();
                // Request admission may persist its failure evidence; the
                // rejected operation must not publish a capture or a new head.
                assert_eq!(
                    backend
                        .get_object(&metadata.object_id)
                        .unwrap()
                        .unwrap()
                        .version,
                    metadata.version
                );
                assert_eq!(
                    backend
                        .get_version(&metadata.object_id, &metadata.version)
                        .unwrap(),
                    Some(legacy.clone())
                );
                assert!(
                    backend
                        .version_pins_for_object(&metadata.object_id)
                        .unwrap()
                        .is_empty()
                );
                assert!(
                    backend
                        .get_version(&metadata.object_id, &metadata.version)
                        .unwrap()
                        .unwrap()
                        .retained
                        .is_none()
                );
                continue;
            }
            let prefix = dfs
                .metadata_backend()
                .unwrap()
                .journal_batch_since_limited(0, 1024, usize::MAX)
                .unwrap()
                .unwrap();
            let mut replica = LocalSobjMetadataBackend::new(
                prefix.shard_id.clone(),
                root.join("replayed-history"),
            )
            .unwrap();
            replica.apply_journal_batch(&prefix).unwrap();
            assert!(
                replica
                    .get_version(&metadata.object_id, &metadata.version)
                    .unwrap()
                    .unwrap()
                    .content_durability
                    .is_none(),
                "old journal replay must not invent new durability metadata"
            );
            apply_legacy_capture(&mut dfs, &metadata, &owner, mode).unwrap();
            let backend = dfs.metadata_backend().unwrap();
            let captured = backend
                .get_version(&metadata.object_id, &metadata.version)
                .unwrap()
                .unwrap();
            assert!(captured.retained.is_some());
            let plan = captured
                .content_durability
                .as_ref()
                .expect("recover recorded legacy data plan");
            assert_eq!(plan.source_version, data.version);
            assert_eq!(plan.policy, source.policy);
            assert_eq!(
                plan.desired_replica_node_ids
                    .iter()
                    .collect::<BTreeSet<_>>(),
                source.desired_replica_node_ids.iter().collect()
            );
            let owner = backend
                .get_intent(
                    &metadata.object_id,
                    &crate::file_history::retained_repair_intent_id(&captured),
                )
                .unwrap()
                .unwrap();
            assert_eq!(owner.policy, DfsReplicationPolicy::new(1, 3).unwrap());
            let tail = backend
                .journal_batch_since_limited(prefix.next_after_sequence, 1024, usize::MAX)
                .unwrap()
                .unwrap();
            let mut saw_enrichment = false;
            for entry in &tail.entries {
                replica
                    .apply_journal_batch(&DfsMetadataJournalBatch {
                        shard_id: tail.shard_id.clone(),
                        entries: vec![entry.clone()],
                        next_after_sequence: entry.sequence,
                    })
                    .unwrap();
                let version = replica
                    .get_version(&metadata.object_id, &metadata.version)
                    .unwrap()
                    .unwrap();
                if let DfsMetadataMutation::ApplyNamespaceTransaction {
                    transaction: Some(transaction),
                    ..
                } = &entry.mutation
                    && transaction
                        .transaction_id
                        .starts_with("history-durability_")
                {
                    assert!(transaction.objects.is_empty());
                    assert!(version.content_durability.is_some());
                    assert!(version.retained.is_none(), "enrichment is not a capture");
                    assert_eq!(
                        replica
                            .get_object(&metadata.object_id)
                            .unwrap()
                            .unwrap()
                            .version,
                        metadata.version
                    );
                    saw_enrichment = true;
                }
                if version.retained.is_some() {
                    assert!(version.content_durability.is_some());
                    assert!(
                        replica
                            .get_intent(
                                &metadata.object_id,
                                &crate::file_history::retained_repair_intent_id(&version)
                            )
                            .unwrap()
                            .is_some(),
                        "every replay prefix exposing the capture must also own its repair plan"
                    );
                }
            }
            assert_eq!(saw_enrichment, !matches!(mode, LegacyCaptureMode::Explicit));
            assert_eq!(
                replica
                    .get_version(&metadata.object_id, &metadata.version)
                    .unwrap()
                    .unwrap(),
                captured
            );
            assert_history_repair_inventory(&dfs, &metadata.object_id);
            if matches!(mode, LegacyCaptureMode::Delete) {
                assert!(dfs.posix_stat(&metadata.path).is_err());
            } else {
                assert_eq!(dfs.posix_read_file(&metadata.path).unwrap(), b"replacement");
            }
        }
    }
}

#[test]
fn file_history_repair_status_includes_metadata_successors_and_pinned_history() {
    let root = temp_root("history-repair-status-roots");
    let owner = NodeId::from_legacy_named("history-status-owner").unwrap();
    let missing = NodeId::from_legacy_named("history-status-missing").unwrap();
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let original = dfs
        .posix_write_file(
            "/services/status-history",
            b"old",
            DfsWriteOptions {
                replica_candidates: vec![owner.clone(), missing],
                policy: DfsReplicationPolicy::new(1, 2).unwrap(),
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    let successor = dfs.posix_set_mode(&original.path, 0o640).unwrap();
    assert_history_repair_inventory(&dfs, &successor.object_id);
    assert_eq!(
        dfs.metadata_backend()
            .unwrap()
            .metadata_summary()
            .unwrap()
            .pending_repair_count,
        1,
        "the current metadata successor still owns the unfinished data write"
    );
    capture_history(&mut dfs, &successor);
    dfs.posix_write_file(
        &original.path,
        b"current",
        DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
    )
    .unwrap();
    dfs.retry_pending_repairs_bounded(8).unwrap();
    assert_history_repair_inventory(&dfs, &successor.object_id);
    assert!(
        dfs.metadata_backend()
            .unwrap()
            .metadata_summary()
            .unwrap()
            .pending_repair_count
            > 0,
        "retained history repairs must remain visible after the head changes"
    );
    dfs.posix_unlink(&original.path).unwrap();
    assert_history_repair_inventory(&dfs, &successor.object_id);
    dfs.posix_write_file(
        &original.path,
        b"recreated",
        DfsWriteOptions::replicated_eventual(vec![owner]),
    )
    .unwrap();
    assert_history_repair_inventory(&dfs, &successor.object_id);
    dfs.file_history_pin(DfsFileHistoryPinRequest {
        path: successor.path.clone(),
        identity: history_identity(&successor),
        operation: DfsFileHistoryPinOperation::Unpin {
            pin_id: "checkpoint".into(),
        },
        operation_id: sinomo_core::Id::generate().to_string(),
    })
    .unwrap();
    assert_history_repair_inventory(&dfs, &successor.object_id);
    assert_eq!(
        dfs.metadata_backend()
            .unwrap()
            .metadata_summary()
            .unwrap()
            .pending_repair_count,
        0
    );
}

fn assert_history_repair_inventory<T: DfsMessageTransport>(
    dfs: &NetworkedDfsService<T>,
    object_id: &DfsObjectId,
) {
    let backend = dfs.metadata_backend().unwrap();
    let inspector = DfsOperatorInspector::new(backend);
    let count = backend.metadata_summary().unwrap().pending_repair_count;
    let all = inspector.repairs(None, None).unwrap();
    assert_eq!(all.len(), count, "repair inventory must match summary");
    let exact = inspector.repairs(None, Some(object_id)).unwrap();
    assert_eq!(exact, all);
    assert_eq!(
        inspector.object(object_id).unwrap().unwrap().repairs.len(),
        count
    );
    for filter in [None, Some(object_id)] {
        let mut collected = Vec::new();
        let mut cursor = None;
        loop {
            let page = inspector
                .repairs_page(None, filter, Some(1), cursor.as_ref())
                .unwrap();
            collected.extend(page.repairs);
            let Some(next) = page.next_cursor else { break };
            assert_ne!(cursor.as_ref(), Some(&next), "repair cursor must advance");
            cursor = Some(next);
        }
        assert_eq!(collected, all);
    }
}

#[test]
fn file_history_rule_delete_and_bulk_replace_schedule_the_captured_generation() {
    for delete in [true, false] {
        let root = temp_root("history-rule-namespace-demand");
        let owner = NodeId::from_legacy_named("history-rule-namespace-owner").unwrap();
        let mut dfs = networked_sifs_with_backend(&root, owner.clone());
        let first = dfs
            .posix_write_file(
                "/services/rule-namespace",
                b"old",
                DfsWriteOptions {
                    replica_candidates: vec![
                        owner,
                        NodeId::from_legacy_named("history-rule-namespace-missing").unwrap(),
                    ],
                    policy: DfsReplicationPolicy::new(1, 2).unwrap(),
                    wait_for_commit: true,
                    foreground_replica_repair: false,
                },
            )
            .unwrap();
        let captured = dfs.posix_set_mode(&first.path, 0o640).unwrap();
        let mut fs = dfs
            .metadata_backend()
            .unwrap()
            .get_file_system(&first.file_system_id)
            .unwrap()
            .unwrap();
        fs.file_history.rules.push(DfsFileHistoryRule {
            path: first.path.clone(),
            retention: DfsFileHistoryRetention {
                keep_last: 1,
                keep_for_seconds: 0,
            },
        });
        dfs.add_file_system(fs).unwrap();
        dfs.begin_message_committed_work();
        if delete {
            dfs.posix_unlink(&first.path).unwrap();
            assert!(dfs.posix_stat(&first.path).is_err());
        } else {
            dfs.posix_write_files_local_eventual_with_known_parents(vec![(
                first.path.as_str(),
                b"new".as_slice(),
            )])
            .unwrap();
            assert_eq!(dfs.posix_read_file(&first.path).unwrap(), b"new");
        }
        assert!(
            dfs.take_message_committed_repair_queue_records()
                .iter()
                .any(|record| record.version == captured.version),
            "delete={delete}"
        );
        let backend = dfs.metadata_backend().unwrap();
        let version = backend
            .get_version(&captured.object_id, &captured.version)
            .unwrap()
            .unwrap();
        let repair = backend
            .get_intent(
                &captured.object_id,
                &crate::file_history::retained_repair_intent_id(&version),
            )
            .unwrap()
            .unwrap();
        assert_eq!(repair.policy, DfsReplicationPolicy::new(1, 2).unwrap());
        let current = backend.get_object(&captured.object_id).unwrap().unwrap();
        assert_eq!(
            dfs.coordinator
                .committed_version(&current.file_system_id, &current.object_id),
            Some(&current.version)
        );
    }
}

#[test]
fn file_history_rule_capture_of_metadata_successor_owns_repairs_without_a_pin_request() {
    let root = temp_root("history-rule-repair-demand");
    let owner = NodeId::from_legacy_named("history-rule-demand-owner").unwrap();
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let first = dfs
        .posix_write_file(
            "/services/rule-demand",
            b"outgoing bytes",
            DfsWriteOptions {
                replica_candidates: vec![
                    owner.clone(),
                    NodeId::from_legacy_named("history-rule-a").unwrap(),
                    NodeId::from_legacy_named("history-rule-b").unwrap(),
                ],
                policy: DfsReplicationPolicy::new(1, 3).unwrap(),
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    let captured = dfs.posix_set_mode(&first.path, 0o640).unwrap();
    let mut fs = dfs
        .metadata_backend()
        .unwrap()
        .get_file_system(&first.file_system_id)
        .unwrap()
        .unwrap();
    fs.file_history.rules.push(DfsFileHistoryRule {
        path: first.path.clone(),
        retention: DfsFileHistoryRetention {
            keep_last: 2,
            keep_for_seconds: 0,
        },
    });
    dfs.add_file_system(fs).unwrap();
    dfs.begin_message_committed_work();
    let current = dfs
        .posix_write_file(
            &first.path,
            b"current bytes",
            DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .unwrap();
    dfs.retry_pending_repairs_bounded(8).unwrap();
    assert!(
        dfs.metadata_backend()
            .unwrap()
            .get_version(&captured.object_id, &captured.version)
            .unwrap()
            .unwrap()
            .retained
            .unwrap()
            .policy_capture
            .is_some()
    );
    assert!(
        dfs.coordinator
            .repair_jobs()
            .iter()
            .any(|job| job.version == captured.version),
        "the explicit rule must own repairs for its captured metadata generation"
    );
    assert!(
        dfs.take_message_committed_repair_queue_records()
            .iter()
            .any(|record| record.version == captured.version)
    );
    assert_eq!(
        dfs.posix_read_file(&current.path).unwrap(),
        b"current bytes"
    );
    assert_eq!(
        dfs.posix_stat(&current.path).unwrap().version,
        current.version
    );
    let backend = dfs.metadata_backend().unwrap();
    let version = backend
        .get_version(&captured.object_id, &captured.version)
        .unwrap()
        .unwrap();
    let owner_id = crate::file_history::retained_repair_intent_id(&version);
    assert!(
        backend
            .get_intent(&captured.object_id, &owner_id)
            .unwrap()
            .is_some()
    );
    let batch = backend
        .journal_batch_since_limited(0, 1024, usize::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(
        batch.next_after_sequence,
        backend.journal_last_sequence().unwrap()
    );
    let mut replica =
        LocalSobjMetadataBackend::new(batch.shard_id.clone(), root.join("capture-replay")).unwrap();
    for entry in batch.entries {
        replica
            .apply_journal_batch(&DfsMetadataJournalBatch {
                shard_id: batch.shard_id.clone(),
                next_after_sequence: entry.sequence,
                entries: vec![entry],
            })
            .unwrap();
        if replica
            .get_version(&captured.object_id, &captured.version)
            .unwrap()
            .is_some_and(|record| record.retained.is_some())
        {
            assert!(
                replica
                    .get_intent(&captured.object_id, &owner_id)
                    .unwrap()
                    .is_some(),
                "a replay prefix may not expose a capture without its durable repair owner"
            );
        }
    }
    // Lose process-local work before the host has persisted any queue delta.
    drop(dfs);
    let network = InProcessNetwork::new();
    let mut reopened = NetworkedDfsService::new(
        network.add_node(owner),
        LocalContentStore::new(root.join("content")).unwrap(),
    )
    .with_metadata_backend_lazy(open_root_local_metadata_backend(root.join("metadata")).unwrap())
    .unwrap();
    assert!(reopened.coordinator.repair_jobs().is_empty());
    assert!(
        reopened
            .hydrate_from_metadata_backend_for_commit_promotion_recovery_until_cancelled(&|| false)
            .unwrap()
    );
    let recovered = reopened.durable_repair_queue_records();
    assert!(
        recovered
            .iter()
            .any(|record| record.intent_id == owner_id && record.version == captured.version)
    );
    let data_dir = sinomo_core::local::SinomoDataDir::new(root.join("queue-owner"));
    crate::persist_default_dfs_repair_queue_records(&data_dir, recovered).unwrap();
    assert_eq!(
        reopened.posix_stat(&current.path).unwrap().version,
        current.version
    );
    assert_eq!(
        reopened.posix_read_file(&current.path).unwrap(),
        b"current bytes"
    );
}

#[test]
fn file_history_local_batch_plan_survives_metadata_changes_and_gc() {
    let root = temp_root("history-local-batch-durability");
    let owner = NodeId::from_legacy_named("history-local-batch-owner").unwrap();
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let written = dfs
        .posix_write_files_local_eventual_with_known_parents(vec![(
            "/services/batch-history",
            b"batch content".as_slice(),
        )])
        .unwrap()
        .remove(0);
    let metadata = dfs.posix_set_mode(&written.path, 0o640).unwrap();
    for _ in 0..8 {
        dfs.metadata_backend
            .as_mut()
            .unwrap()
            .gc_metadata_history(DfsMetadataHistoryGcOptions::delete_eligible(
                Duration::ZERO,
                0,
            ))
            .unwrap();
    }
    drop(dfs);
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let version = dfs
        .metadata_backend()
        .unwrap()
        .get_version(&metadata.object_id, &metadata.version)
        .unwrap()
        .unwrap();
    assert!(version.retained.is_none());
    let plan = version.content_durability.unwrap();
    assert_eq!(plan.source_version, written.version);
    assert_eq!(plan.policy, DfsReplicationPolicy::new(1, 1).unwrap());
    assert_eq!(plan.desired_replica_node_ids, vec![owner]);
    capture_history(&mut dfs, &metadata);
    assert!(matches!(dfs.read_file_history(DfsFileHistoryReadRequest {
        identity: history_identity(&metadata), offset: 0, max_bytes: 64,
    }).unwrap(), DfsFileHistoryReadOutcome::Found { data } if data.bytes == b"batch content"));
    assert_eq!(
        dfs.posix_stat(&metadata.path).unwrap().version,
        metadata.version
    );
}

#[test]
fn file_history_capture_after_chmod_preserves_the_content_replication_plan() {
    capture_after_metadata_change_preserves_durability(false);
}

#[test]
fn file_history_capture_after_rename_and_write_history_gc_preserves_durability() {
    capture_after_metadata_change_preserves_durability(true);
}

fn capture_after_metadata_change_preserves_durability(collect_before_capture: bool) {
    let root = temp_root("history-metadata-generation-policy");
    let owner = NodeId::from_legacy_named("history-metadata-owner").unwrap();
    let remote_a = NodeId::from_legacy_named("history-metadata-a").unwrap();
    let remote_b = NodeId::from_legacy_named("history-metadata-b").unwrap();
    let network = InProcessNetwork::new();
    let mut dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("content")).unwrap(),
    )
    .with_metadata_backend(open_root_local_metadata_backend(root.join("metadata")).unwrap())
    .unwrap();
    dfs.bootstrap_distributed_sifs().unwrap();
    let original = dfs
        .posix_write_file(
            "/services/history-metadata-generation",
            b"retained content",
            DfsWriteOptions {
                replica_candidates: vec![owner.clone(), remote_a.clone(), remote_b.clone()],
                policy: DfsReplicationPolicy::new(1, 3).unwrap(),
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    if collect_before_capture {
        let mut peers = [remote_a, remote_b]
            .into_iter()
            .enumerate()
            .map(|(index, node)| {
                NetworkedDfsService::new(
                    network.add_node(node),
                    LocalContentStore::new(root.join(format!("peer-{index}"))).unwrap(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(dfs.retry_pending_repairs_bounded(8).unwrap().len(), 2);
        for peer in &mut peers {
            peer.process_incoming().unwrap();
        }
        dfs.process_incoming().unwrap();
        assert!(
            dfs.coordinator.repair_jobs().is_empty(),
            "the original write must first reach N"
        );
    }
    let mut metadata = dfs.posix_set_mode(&original.path, 0o640).unwrap();
    if collect_before_capture {
        dfs.posix_rename(&original.path, "/services/renamed-history-source")
            .unwrap();
        metadata = dfs.posix_stat("/services/renamed-history-source").unwrap();
    }
    for _ in 0..8 {
        dfs.metadata_backend
            .as_mut()
            .unwrap()
            .gc_metadata_history(DfsMetadataHistoryGcOptions::delete_eligible(
                Duration::ZERO,
                0,
            ))
            .unwrap();
    }
    drop(dfs);
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    if collect_before_capture {
        assert!(
            dfs.metadata_backend()
                .unwrap()
                .get_version(&original.object_id, &original.version)
                .unwrap()
                .is_none(),
            "the test must remove the original generation and its operation evidence"
        );
    } else {
        assert_eq!(
            dfs.coordinator.repair_jobs().len(),
            2,
            "current metadata-only generations must keep their pending N work after GC and restart"
        );
        assert_eq!(
            dfs.coordinator
                .committed_version(&metadata.file_system_id, &metadata.object_id),
            Some(&metadata.version)
        );
    }
    assert_ne!(metadata.version, original.version);
    assert_eq!(metadata.content_hash, original.content_hash);
    let record = dfs
        .metadata_backend()
        .unwrap()
        .get_version(&metadata.object_id, &metadata.version)
        .unwrap()
        .unwrap();
    assert!(
        record.retained.is_none(),
        "metadata-only changes do not capture history"
    );
    assert_eq!(
        record.content_durability.unwrap().policy,
        DfsReplicationPolicy::new(1, 3).unwrap()
    );
    capture_history(&mut dfs, &metadata);
    let current = dfs
        .posix_write_file(
            &metadata.path,
            b"current content",
            DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .unwrap();
    dfs.retry_pending_repairs_bounded(8).unwrap();
    for _ in 0..8 {
        dfs.metadata_backend
            .as_mut()
            .unwrap()
            .gc_metadata_history(DfsMetadataHistoryGcOptions::delete_eligible(
                Duration::ZERO,
                0,
            ))
            .unwrap();
    }
    drop(dfs);
    let mut dfs = networked_sifs_with_backend(&root, owner);
    dfs.file_history_pin(DfsFileHistoryPinRequest {
        path: current.path.clone(),
        identity: history_identity(&metadata),
        operation: DfsFileHistoryPinOperation::Pin {
            pin_id: "after-restart".into(),
        },
        operation_id: sinomo_core::Id::generate().to_string(),
    })
    .unwrap();
    let jobs = dfs.coordinator.repair_jobs();
    assert!(
        jobs.iter().any(|job| {
            job.version == metadata.version
                && dfs.coordinator.intent(&job.intent_id).unwrap().policy
                    == DfsReplicationPolicy::new(1, 3).unwrap()
        }),
        "a metadata-only generation must retain the original W=1/N=3 repair obligation: {jobs:?}"
    );
    assert_eq!(
        dfs.posix_read_file(&current.path).unwrap(),
        b"current content"
    );
    assert_eq!(
        dfs.posix_stat(&current.path).unwrap().version,
        current.version
    );
}

#[test]
fn file_history_repin_after_compaction_has_fresh_repair_ownership() {
    repin_after_gc_has_fresh_repair_ownership(false, false);
}

#[test]
fn file_history_repin_after_retry_receipt_eviction_keeps_replication_policy() {
    repin_after_gc_has_fresh_repair_ownership(true, false);
}

#[test]
fn file_history_clustered_pin_operations_remain_usable_after_gc() {
    repin_after_gc_has_fresh_repair_ownership(true, true);
}

fn repin_after_gc_has_fresh_repair_ownership(evict_receipt: bool, clustered: bool) {
    let root = temp_root("history-compacted-repair-owner");
    let owner = NodeId::from_legacy_named("history-compacted-owner").unwrap();
    let a = NodeId::from_legacy_named("history-compacted-a").unwrap();
    let b = NodeId::from_legacy_named("history-compacted-b").unwrap();
    let network = InProcessNetwork::new();
    let open_primary = || {
        let mut service = NetworkedDfsService::new(
            network.add_node(owner.clone()),
            LocalContentStore::new(root.join("content")).unwrap(),
        )
        .with_metadata_backend(open_root_local_metadata_backend(root.join("metadata")).unwrap())
        .unwrap();
        service.bootstrap_distributed_sifs().unwrap();
        service
    };
    let mut dfs = open_primary();
    let current = dfs
        .posix_write_file(
            "/services/compacted-history",
            b"canonical",
            DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .unwrap();
    let request = preserve_request(&current, "preserve-before-compaction");
    let DfsFileHistoryPreserveOutcome::Retained { identity } = dfs
        .preserve_local_file_version(
            request.clone(),
            &b"retained"[..],
            DfsWriteOptions {
                replica_candidates: vec![owner.clone(), a.clone(), b.clone()],
                policy: DfsReplicationPolicy::new(1, 3).unwrap(),
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap()
    else {
        panic!("W reached");
    };
    let control_plane = if clustered {
        let mut kv = InProcessKvStore::new();
        kv.put_serde(
            crate::control_plane::metadata_shard_key(&sifs_file_system_id(), "root"),
            &DfsMetadataShardConfig {
                file_system_id: sifs_file_system_id(),
                shard_id: "root".into(),
                primary_node_id: owner.clone(),
                replica_node_ids: Vec::new(),
                epoch: 1,
                assignment: DfsMetadataShardAssignment::Root,
                namespace_prepare_codec: DfsMetadataNamespacePrepareCodec::PackedV3,
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            },
            None,
        )
        .unwrap();
        let client = history_control_plane(kv);
        dfs = dfs.with_control_plane(client.clone());
        Some(client)
    } else {
        None
    };
    let original_intent = dfs.coordinator.repair_jobs()[0].intent_id.clone();
    dfs.file_history_pin(DfsFileHistoryPinRequest {
        path: current.path.clone(),
        identity: identity.clone(),
        operation: DfsFileHistoryPinOperation::Unpin {
            pin_id: request.pin_id.clone(),
        },
        operation_id: sinomo_core::Id::generate().to_string(),
    })
    .unwrap();
    dfs.metadata_backend
        .as_mut()
        .unwrap()
        .gc_metadata_history(
            DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 0)
                .with_now_unix_nanos(crate::metadata_records::dfs_now_unix_nanos()),
        )
        .unwrap();
    assert!(
        dfs.metadata_backend()
            .unwrap()
            .get_intent_receipt(&current.object_id, &original_intent)
            .unwrap()
            .is_some()
    );
    if evict_receipt {
        dfs.metadata_backend
            .as_mut()
            .unwrap()
            .gc_metadata_history(
                DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 0)
                    .with_now_unix_nanos(crate::metadata_records::dfs_now_unix_nanos()),
            )
            .unwrap();
        assert!(
            dfs.metadata_backend()
                .unwrap()
                .get_intent_receipt(&current.object_id, &original_intent)
                .unwrap()
                .is_none()
        );
        drop(dfs);
        dfs = open_primary();
        if let Some(client) = &control_plane {
            dfs = dfs.with_control_plane(client.clone());
        }
    }
    assert!(
        dfs.metadata_backend()
            .unwrap()
            .get_version(&identity.object_id, &identity.version)
            .unwrap()
            .is_some()
    );
    dfs.begin_message_committed_work();
    let repin = DfsFileHistoryPinRequest {
        path: current.path.clone(),
        identity: identity.clone(),
        operation: DfsFileHistoryPinOperation::Pin {
            pin_id: request.pin_id.clone(),
        },
        operation_id: sinomo_core::Id::generate().to_string(),
    };
    dfs.file_history_pin(repin.clone()).unwrap();
    let jobs = dfs.coordinator.repair_jobs();
    assert_eq!(
        jobs.len(),
        2,
        "the retained W=1/N=3 obligation must survive intent compaction"
    );
    assert!(
        jobs.iter()
            .all(|job| job.intent_id != original_intent && job.version == identity.version)
    );
    assert_eq!(
        dfs.coordinator.intent(&jobs[0].intent_id).unwrap().policy,
        DfsReplicationPolicy::new(1, 3).unwrap()
    );
    let repair_intent = jobs[0].intent_id.clone();
    assert_eq!(dfs.take_message_committed_repair_queue_records().len(), 2);
    assert_eq!(
        dfs.posix_stat(&current.path).unwrap().version,
        current.version
    );
    assert_eq!(dfs.posix_read_file(&current.path).unwrap(), b"canonical");
    if evict_receipt {
        let gc = || {
            DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 0)
                .with_now_unix_nanos(crate::metadata_records::dfs_now_unix_nanos())
        };
        for _ in 0..8 {
            dfs.metadata_backend
                .as_mut()
                .unwrap()
                .gc_metadata_history(gc())
                .unwrap();
        }
        assert!(
            dfs.metadata_backend()
                .unwrap()
                .get_intent(&current.object_id, &repair_intent)
                .unwrap()
                .is_some()
        );
        let before = dfs
            .metadata_backend()
            .unwrap()
            .journal_last_sequence()
            .unwrap();
        assert!(
            dfs.file_history_pin(repin)
                .unwrap_err()
                .to_string()
                .contains("expired")
        );
        assert_eq!(
            dfs.metadata_backend()
                .unwrap()
                .journal_last_sequence()
                .unwrap(),
            before
        );
        let mut replicas = [a, b]
            .into_iter()
            .enumerate()
            .map(|(index, node)| {
                let service = NetworkedDfsService::new(
                    network.add_node(node),
                    LocalContentStore::new(root.join(format!("replica-{index}"))).unwrap(),
                );
                if let Some(client) = &control_plane {
                    service.with_control_plane(client.clone())
                } else {
                    service
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(dfs.retry_pending_repairs_bounded(8).unwrap().len(), 2);
        for replica in &mut replicas {
            replica.process_incoming().unwrap();
        }
        dfs.process_incoming().unwrap();
        assert!(dfs.coordinator.repair_jobs().is_empty());
        for replica in &replicas {
            assert_eq!(
                replica
                    .content
                    .read_object_version(identity.object_id.as_str(), identity.version.as_str())
                    .unwrap()
                    .1,
                b"retained"
            );
            assert!(replica.metadata.object(&current.object_id).is_none());
        }
        assert_eq!(dfs.posix_read_file(&current.path).unwrap(), b"canonical");
        dfs.file_history_pin(DfsFileHistoryPinRequest {
            path: current.path.clone(),
            identity: identity.clone(),
            operation: DfsFileHistoryPinOperation::Unpin {
                pin_id: request.pin_id,
            },
            operation_id: sinomo_core::Id::generate().to_string(),
        })
        .unwrap();
        for _ in 0..8 {
            dfs.metadata_backend
                .as_mut()
                .unwrap()
                .gc_metadata_history(gc())
                .unwrap();
        }
        assert!(
            dfs.metadata_backend()
                .unwrap()
                .get_version(&identity.object_id, &identity.version)
                .unwrap()
                .is_none(),
            "maintenance ownership must not become a retention root"
        );
        assert!(
            dfs.metadata_backend()
                .unwrap()
                .get_intent(&current.object_id, &repair_intent)
                .unwrap()
                .is_none()
        );
        assert!(
            dfs.metadata_backend()
                .unwrap()
                .get_intent_receipt(&current.object_id, &repair_intent)
                .unwrap()
                .is_none()
        );
    }
}

#[test]
fn file_history_remote_retained_reads_and_recovery_ignore_legacy_conflict_selection() {
    let root = temp_root("history-legacy-remote-source");
    let owner = NodeId::from_legacy_named("history-legacy-owner").unwrap();
    let holder = NodeId::from_legacy_named("history-legacy-holder").unwrap();
    let network = InProcessNetwork::new();
    let mut primary = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("original-content")).unwrap(),
    )
    .with_metadata_backend(open_root_local_metadata_backend(root.join("metadata")).unwrap())
    .unwrap();
    primary.bootstrap_distributed_sifs().unwrap();
    let options = DfsWriteOptions::replicated_eventual(vec![owner.clone()]);
    let current = primary
        .posix_write_file("/services/legacy-source", b"canonical", options.clone())
        .unwrap();
    let bytes = b"explicitly preserved local operation";
    let DfsFileHistoryPreserveOutcome::Retained { identity } = primary
        .preserve_local_file_version(
            preserve_request(&current, "legacy-explicit-preserve"),
            &bytes[..],
            options.clone(),
        )
        .unwrap()
    else {
        panic!("capture reached W");
    };
    let hash = content_hash(bytes);
    let replica = NetworkedDfsService::new(
        network.add_node(holder.clone()),
        LocalContentStore::new(root.join("replica-content")).unwrap(),
    );
    replica
        .store_command_bytes(&identity.object_id, &identity.version, &hash, bytes)
        .unwrap();
    replica
        .store_command_bytes(
            &current.object_id,
            &current.version,
            current.content_hash.as_deref().unwrap(),
            b"canonical",
        )
        .unwrap();
    primary
        .put_metadata_receipt(
            &current.file_system_id,
            &current.object_id,
            &current.version,
            &holder,
            current.content_hash.as_deref().unwrap(),
        )
        .unwrap();
    primary
        .put_metadata_receipt(
            &identity.file_system_id,
            &identity.object_id,
            &identity.version,
            &holder,
            &hash,
        )
        .unwrap();
    let conflict = DfsConflictRecord {
        conflict_id: "legacy-conflict".into(),
        file_system_id: current.file_system_id.clone(),
        object_id: current.object_id.clone(),
        path: current.path.clone(),
        base_version: None,
        local_version: current.version.clone(),
        remote_version: identity.version.clone(),
        detected_by_node_id: owner.clone(),
        reason: "imported legacy conflict".into(),
        local_content_hash: current.content_hash.clone(),
        remote_content_hash: Some(hash.clone()),
        created_by_intent_id: None,
        strategy: Some("legacy".into()),
        status: DfsConflictStatus::Open,
        detected_at: time::OffsetDateTime::now_utc(),
    };
    primary
        .metadata_backend
        .as_mut()
        .unwrap()
        .put_conflict(&conflict)
        .unwrap();
    drop(primary);
    // The authoritative node has metadata and legacy conflict markers, but
    // its only readable copy of the retained bytes is on the data-only peer.
    let mut primary = NetworkedDfsService::new(
        network.add_node(owner),
        LocalContentStore::new(root.join("empty-primary-content")).unwrap(),
    )
    .with_metadata_backend(open_root_local_metadata_backend(root.join("metadata")).unwrap())
    .unwrap();
    assert!(
        primary
            .open_content_hash_stream(
                &identity.object_id,
                &identity.version,
                &hash,
                Some(bytes.len() as u64)
            )
            .is_err()
    );
    assert!(matches!(
        primary
            .read_local_object_with_state(&current.object_id, false)
            .unwrap(),
        NetworkedDfsReadOutcome::Conflicted { .. }
    ));
    let stop = Arc::new(AtomicBool::new(false));
    let replica_stop = stop.clone();
    let pump = std::thread::spawn(move || pump_sifs_service_until_stop(replica, replica_stop));
    let result = (|| {
        let range = primary.read_file_history(DfsFileHistoryReadRequest {
            identity: identity.clone(),
            offset: 0,
            max_bytes: 64,
        })?;
        let DfsFileHistoryReadOutcome::Found { data } = range else {
            return Err(DfsError::Metadata {
                message: format!("explicit retained read was blocked: {range:?}"),
            });
        };
        let exact = primary.read_object_version_content(
            &identity.object_id,
            &identity.version,
            &hash,
            Some(bytes.len() as u64),
        )?;
        let exact_current = primary.read_object_version_content(
            &current.object_id,
            &current.version,
            current.content_hash.as_deref().unwrap(),
            current.size_bytes,
        )?;
        let restored = primary.restore_file_history(
            DfsFileHistoryRestoreRequest {
                source: identity.clone(),
                destination_path: "/services/recovered-legacy-content".into(),
                expected_destination: None,
                operation_id: "legacy-retained-recovery".into(),
            },
            options.clone(),
        )?;
        let recovered =
            primary
                .posix_read_file(&restored.path)
                .map_err(|error| DfsError::Metadata {
                    message: error.to_string(),
                })?;
        let source_before_replace = primary
            .current_read_object_metadata(&current.object_id)?
            .version;
        let replaced = primary.restore_file_history(
            DfsFileHistoryRestoreRequest {
                source: identity.clone(),
                destination_path: current.path.clone(),
                expected_destination: Some(history_identity(&current)),
                operation_id: "legacy-retained-replace".into(),
            },
            options,
        )?;
        let marker_before_maintenance = primary
            .metadata_backend()
            .unwrap()
            .get_conflict(&current.object_id, &conflict.conflict_id)?;
        let maintenance = primary.reconcile_stale_conflicts_bounded(8)?;
        let replaced_bytes =
            primary
                .posix_read_file(&current.path)
                .map_err(|error| DfsError::Metadata {
                    message: error.to_string(),
                })?;
        Ok::<_, DfsError>((
            data.bytes,
            exact,
            exact_current,
            restored,
            recovered,
            source_before_replace,
            replaced,
            marker_before_maintenance,
            maintenance,
            replaced_bytes,
        ))
    })();
    stop.store(true, Ordering::SeqCst);
    let replica = pump.join().unwrap();
    let (
        range,
        exact,
        exact_current,
        restored,
        recovered,
        source_before_replace,
        replaced,
        marker_before_maintenance,
        maintenance,
        replaced_bytes,
    ) = result.unwrap();
    assert_eq!(range, bytes);
    assert_eq!(exact, bytes);
    assert_eq!(exact_current, b"canonical");
    assert_eq!(recovered, bytes);
    assert_ne!(restored.committed.object_id, current.object_id);
    assert_eq!(
        source_before_replace, current.version,
        "recovery elsewhere never rewrites its source"
    );
    assert_eq!(replaced.committed.object_id, current.object_id);
    assert_ne!(replaced.committed.version, current.version);
    assert_ne!(replaced.committed.version, identity.version);
    assert_eq!(marker_before_maintenance, Some(conflict));
    assert_eq!(maintenance.superseded_conflict_count, 1);
    assert_eq!(replaced_bytes, bytes);
    assert_eq!(
        primary.posix_stat(&current.path).unwrap().version,
        replaced.committed.version
    );
    assert!(replica.metadata.object(&current.object_id).is_none());
}

#[test]
fn file_history_current_artifact_is_readable_without_becoming_a_capture() {
    let root = temp_root("history-current-artifact");
    let owner = NodeId::from_legacy_named("history-artifact-owner").unwrap();
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let stat = dfs
        .posix_write_file(
            "/services/artifact",
            b"artifact bytes",
            DfsWriteOptions::replicated_eventual(vec![owner]),
        )
        .unwrap();
    let mut artifact = dfs.current_read_object_metadata(&stat.object_id).unwrap();
    artifact.kind = DfsObjectKind::Artifact;
    dfs.add_object_metadata(artifact).unwrap();
    assert_eq!(
        dfs.read_object_version_content(
            &stat.object_id,
            &stat.version,
            stat.content_hash.as_deref().unwrap(),
            stat.size_bytes,
        )
        .unwrap(),
        b"artifact bytes"
    );
    assert!(
        dfs.metadata_backend()
            .unwrap()
            .get_version(&stat.object_id, &stat.version)
            .unwrap()
            .unwrap()
            .retained
            .is_none()
    );
}

#[test]
fn file_history_repin_reactivates_only_the_retained_versions_repair() {
    let root = temp_root("history-repin-repair");
    let owner = NodeId::from_legacy_named("history-repin-owner").unwrap();
    let remote = NodeId::from_legacy_named("history-repin-offline").unwrap();
    let network = InProcessNetwork::new();
    let mut dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("content")).unwrap(),
    )
    .with_metadata_backend(open_root_local_metadata_backend(root.join("metadata")).unwrap())
    .unwrap();
    dfs.bootstrap_distributed_sifs().unwrap();
    let stat = dfs
        .posix_write_file(
            "/services/repin",
            b"canonical",
            DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .unwrap();
    let request = preserve_request(&stat, "repin-preserve");
    let DfsFileHistoryPreserveOutcome::Retained { identity } = dfs
        .preserve_local_file_version(
            request.clone(),
            &b"retained alternative"[..],
            DfsWriteOptions {
                replica_candidates: vec![owner.clone(), remote.clone()],
                policy: DfsReplicationPolicy::new(1, 2).unwrap(),
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap()
    else {
        panic!("local receipt reaches W");
    };
    let jobs = dfs.coordinator.repair_jobs().to_vec();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].version, identity.version);
    let intent_id = jobs[0].intent_id.clone();
    dfs.file_history_pin(DfsFileHistoryPinRequest {
        path: stat.path.clone(),
        identity: identity.clone(),
        operation: DfsFileHistoryPinOperation::Pin {
            pin_id: "independent-root".into(),
        },
        operation_id: "repin-second-root".into(),
    })
    .unwrap();
    dfs.file_history_pin(DfsFileHistoryPinRequest {
        path: stat.path.clone(),
        identity: identity.clone(),
        operation: DfsFileHistoryPinOperation::Unpin {
            pin_id: request.pin_id.clone(),
        },
        operation_id: "repin-release".into(),
    })
    .unwrap();
    assert_eq!(
        dfs.coordinator.repair_jobs(),
        jobs.as_slice(),
        "another pin still owns the work"
    );
    dfs.file_history_pin(DfsFileHistoryPinRequest {
        path: stat.path.clone(),
        identity: identity.clone(),
        operation: DfsFileHistoryPinOperation::Unpin {
            pin_id: "independent-root".into(),
        },
        operation_id: "repin-release-last-root".into(),
    })
    .unwrap();
    dfs.retry_pending_repairs_bounded(8).unwrap();
    assert!(
        dfs.coordinator.repair_jobs().is_empty(),
        "released history cannot retain executable repair jobs"
    );
    assert!(dfs.durable_repair_queue_records().is_empty());
    assert!(!dfs.writes.pending_writes.contains_key(&intent_id));
    // A cold lane can encounter a stale queue row after the pin was removed.
    // Later generic receipt restoration must not make that row executable.
    dfs.coordinator = DistributedEventualCoordinator::new();
    dfs.restore_pending_write_for_intent_if_missing(&intent_id, &stat.object_id)
        .unwrap();
    dfs.coordinator
        .restore_durable_receipt(&intent_id, owner.clone())
        .unwrap();
    assert!(dfs.coordinator.repair_jobs().is_empty());
    dfs.coordinator.begin_message_repair_job_capture();
    dfs.file_history_pin(DfsFileHistoryPinRequest {
        path: stat.path.clone(),
        identity: identity.clone(),
        operation: DfsFileHistoryPinOperation::Pin {
            pin_id: request.pin_id,
        },
        operation_id: "repin-acquire".into(),
    })
    .unwrap();
    assert_eq!(
        dfs.coordinator.repair_jobs(),
        jobs.as_slice(),
        "an explicit root reactivates its missing N replica"
    );
    assert!(dfs.writes.pending_writes.contains_key(&intent_id));
    assert_eq!(
        dfs.take_message_committed_repair_queue_records().len(),
        1,
        "the pin publication must durably enqueue newly actionable work"
    );
    let mut replica = NetworkedDfsService::new(
        network.add_node(remote.clone()),
        LocalContentStore::new(root.join("replica-content")).unwrap(),
    );
    let scheduled = dfs.retry_pending_repairs_bounded(8).unwrap();
    assert_eq!(scheduled.len(), 1);
    let replica_events = replica.process_incoming().unwrap();
    let primary_events = dfs.process_incoming().unwrap();
    assert!(
        dfs.coordinator.repair_jobs().is_empty(),
        "replica: {replica_events:?}; primary: {primary_events:?}"
    );
    assert!(!dfs.writes.pending_writes.contains_key(&intent_id));
    assert_eq!(
        replica
            .content
            .read_object_version(identity.object_id.as_str(), identity.version.as_str())
            .unwrap()
            .1,
        b"retained alternative"
    );
    assert!(
        replica.metadata.object(&stat.object_id).is_none(),
        "retained repair never publishes a second current view"
    );
    assert_eq!(dfs.posix_read_file(&stat.path).unwrap(), b"canonical");
}

#[test]
fn file_history_repin_after_restart_does_not_rewind_a_canonical_write_intent() {
    let root = temp_root("history-repin-canonical-intent");
    let owner = NodeId::from_legacy_named("history-repin-canonical-owner").unwrap();
    let remote = NodeId::from_legacy_named("history-repin-canonical-remote").unwrap();
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let first = dfs
        .posix_write_file(
            "/services/canonical-repin",
            b"old",
            DfsWriteOptions {
                replica_candidates: vec![owner.clone(), remote],
                policy: DfsReplicationPolicy::new(1, 2).unwrap(),
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    capture_history(&mut dfs, &first);
    let current = dfs
        .posix_write_file(
            &first.path,
            b"current",
            DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .unwrap();
    dfs.file_history_pin(DfsFileHistoryPinRequest {
        path: first.path.clone(),
        identity: history_identity(&first),
        operation: DfsFileHistoryPinOperation::Unpin {
            pin_id: "checkpoint".into(),
        },
        operation_id: "canonical-release".into(),
    })
    .unwrap();
    assert!(dfs.coordinator.repair_jobs().is_empty());
    drop(dfs);
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    assert!(dfs.coordinator.repair_jobs().is_empty());
    dfs.file_history_pin(DfsFileHistoryPinRequest {
        path: first.path.clone(),
        identity: history_identity(&first),
        operation: DfsFileHistoryPinOperation::Pin {
            pin_id: "checkpoint".into(),
        },
        operation_id: "canonical-repin".into(),
    })
    .unwrap();
    assert_eq!(dfs.coordinator.repair_jobs().len(), 1);
    assert_eq!(dfs.coordinator.repair_jobs()[0].version, first.version);
    assert_eq!(
        dfs.coordinator
            .committed_version(&current.file_system_id, &current.object_id),
        Some(&current.version)
    );
    dfs.posix_write_file(
        &current.path,
        b"next",
        DfsWriteOptions::replicated_eventual(vec![owner]),
    )
    .unwrap();
    assert_eq!(dfs.posix_read_file(&current.path).unwrap(), b"next");
}

#[test]
fn file_history_expired_rule_retires_repairs_even_during_retry_backoff() {
    let root = temp_root("history-expired-repair");
    let owner = NodeId::from_legacy_named("history-expired-owner").unwrap();
    let remote = NodeId::from_legacy_named("history-expired-offline").unwrap();
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let first = dfs
        .posix_write_file(
            "/services/expiring-history",
            b"old",
            DfsWriteOptions {
                replica_candidates: vec![owner.clone(), remote],
                policy: DfsReplicationPolicy::new(1, 2).unwrap(),
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    let mut fs = dfs
        .metadata_backend()
        .unwrap()
        .get_file_system(&first.file_system_id)
        .unwrap()
        .unwrap();
    fs.file_history.rules.push(DfsFileHistoryRule {
        path: first.path.clone(),
        retention: DfsFileHistoryRetention {
            keep_last: 0,
            keep_for_seconds: 1,
        },
    });
    dfs.add_file_system(fs).unwrap();
    dfs.posix_write_file(
        &first.path,
        b"current",
        DfsWriteOptions::replicated_eventual(vec![owner]),
    )
    .unwrap();
    let retained = dfs
        .metadata_backend()
        .unwrap()
        .get_version(&first.object_id, &first.version)
        .unwrap()
        .unwrap();
    assert!(retained.retained.unwrap().policy_capture.is_some());
    let mut records = dfs.durable_repair_queue_records();
    assert_eq!(records.len(), 1);
    records[0].attempts = 1;
    records[0].next_retry_unix_nanos =
        crate::metadata_records::dfs_now_unix_nanos().saturating_add(60_000_000_000);
    // Exercise the production wall-clock expiry boundary while the transport
    // retry remains far in the future; no network work should run here.
    std::thread::sleep(Duration::from_secs(2));
    assert!(history_entries(&mut dfs, &first.path).is_empty());
    assert!(matches!(
        dfs.read_file_history(DfsFileHistoryReadRequest {
            identity: history_identity(&first),
            offset: 0,
            max_bytes: 64,
        })
        .unwrap(),
        DfsFileHistoryReadOutcome::NotRetained { .. }
    ));
    assert_history_repair_inventory(&dfs, &first.object_id);
    assert_eq!(
        dfs.metadata_backend()
            .unwrap()
            .metadata_summary()
            .unwrap()
            .pending_repair_count,
        0
    );
    assert!(
        dfs.retry_pending_repairs_bounded_with_attempts(8, &records)
            .unwrap()
            .is_empty()
    );
    assert!(dfs.coordinator.repair_jobs().is_empty());
    assert!(dfs.durable_repair_queue_records().is_empty());
    assert_eq!(dfs.posix_read_file(&first.path).unwrap(), b"current");
}

#[test]
fn file_history_pin_receipt_replay_recovers_its_repair_queue_handoff() {
    let root = temp_root("history-pin-replay-repair");
    let owner = NodeId::from_legacy_named("history-pin-replay-owner").unwrap();
    let remote = NodeId::from_legacy_named("history-pin-replay-offline").unwrap();
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let stat = dfs
        .posix_write_file(
            "/services/replay-pin",
            b"canonical",
            DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .unwrap();
    let DfsFileHistoryPreserveOutcome::Retained { identity } = dfs
        .preserve_local_file_version(
            preserve_request(&stat, "replay-preserve"),
            &b"retained"[..],
            DfsWriteOptions {
                replica_candidates: vec![owner.clone(), remote],
                policy: DfsReplicationPolicy::new(1, 2).unwrap(),
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap()
    else {
        panic!("W reached");
    };
    let mut kv = InProcessKvStore::new();
    kv.put_serde(
        crate::control_plane::metadata_shard_key(&sifs_file_system_id(), "root"),
        &DfsMetadataShardConfig {
            file_system_id: sifs_file_system_id(),
            shard_id: "root".into(),
            primary_node_id: owner.clone(),
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: DfsMetadataNamespacePrepareCodec::PackedV3,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        },
        None,
    )
    .unwrap();
    let control_plane = history_control_plane(kv);
    let mut dfs = dfs.with_control_plane(control_plane.clone());
    let request = DfsFileHistoryPinRequest {
        path: stat.path.clone(),
        identity,
        operation: DfsFileHistoryPinOperation::Pin {
            pin_id: "second-root".into(),
        },
        operation_id: "publish-root-before-queue-handoff".into(),
    };
    let first = dfs.file_history_pin(request.clone()).unwrap();
    let before = dfs
        .metadata_backend()
        .unwrap()
        .journal_last_sequence()
        .unwrap();
    drop(dfs);
    // Reopen exactly like a lazy request lane: no repair inventory or local
    // queue survived, but the completed namespace request is durable.
    let network = InProcessNetwork::new();
    let mut dfs = NetworkedDfsService::new(
        network.add_node(owner),
        LocalContentStore::new(root.join("content")).unwrap(),
    )
    .with_metadata_backend_lazy(open_root_local_metadata_backend(root.join("metadata")).unwrap())
    .unwrap()
    .with_control_plane(control_plane);
    assert!(dfs.coordinator.repair_jobs().is_empty());
    dfs.begin_message_committed_work();
    assert_eq!(dfs.file_history_pin(request).unwrap(), first);
    assert_eq!(
        dfs.metadata_backend()
            .unwrap()
            .journal_last_sequence()
            .unwrap(),
        before
    );
    assert_eq!(dfs.coordinator.repair_jobs().len(), 1);
    assert_eq!(dfs.take_message_committed_repair_queue_records().len(), 1);
    assert!(dfs.take_message_committed_work().repair_maintenance);
    assert_eq!(
        dfs.current_read_object_metadata(&stat.object_id)
            .unwrap()
            .version,
        stat.version
    );
}

#[test]
fn file_history_exact_version_read_rejects_unretained_and_unanchored_bytes() {
    let root = temp_root("history-exact-read-admission");
    let owner = NodeId::from_legacy_named("history-exact-reader").unwrap();
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let options = DfsWriteOptions::replicated_eventual(vec![owner]);
    let first = dfs
        .posix_write_file("/services/exact-read", b"first", options.clone())
        .unwrap();
    let second = dfs
        .posix_write_file("/services/exact-read", b"second", options)
        .unwrap();
    assert_eq!(
        dfs.read_object_version_content(
            &second.object_id,
            &second.version,
            second.content_hash.as_deref().unwrap(),
            second.size_bytes,
        )
        .unwrap(),
        b"second"
    );
    assert!(
        dfs.read_object_version_content(
            &first.object_id,
            &first.version,
            first.content_hash.as_deref().unwrap(),
            first.size_bytes,
        )
        .is_err(),
        "an internal superseded generation is not user-readable history"
    );
    assert!(
        dfs.read_object_version_content(
            &DfsObjectId::from_legacy_named("unanchored-object").unwrap(),
            &first.version,
            first.content_hash.as_deref().unwrap(),
            first.size_bytes,
        )
        .is_err(),
        "a known blob hash is not authority to read arbitrary content"
    );
}

#[test]
fn file_history_exact_version_reads_use_primary_and_remote_content() {
    for codec in [
        DfsMetadataNamespacePrepareCodec::PackedV2,
        DfsMetadataNamespacePrepareCodec::PackedV3,
    ] {
        let root = temp_root(&format!("history-exact-remote-{codec:?}"));
        let owner = NodeId::from_legacy_named("history-exact-primary").unwrap();
        let reader = NodeId::from_legacy_named("history-exact-client").unwrap();
        let network = InProcessNetwork::new();
        let mut primary = NetworkedDfsService::new(
            network.add_node(owner.clone()),
            LocalContentStore::new(root.join("primary-content")).unwrap(),
        )
        .with_metadata_backend(open_root_local_metadata_backend(root.join("metadata")).unwrap())
        .unwrap();
        primary.bootstrap_distributed_sifs().unwrap();
        let options = DfsWriteOptions::replicated_eventual(vec![owner.clone()]);
        let bytes = vec![42; crate::file_history_query::DFS_FILE_HISTORY_RANGE_LIMIT as usize + 31];
        let first = primary
            .posix_write_file("/services/exact-remote", &bytes, options.clone())
            .unwrap();
        let stale = primary
            .current_read_object_metadata(&first.object_id)
            .unwrap();
        if codec.supports_file_history() {
            capture_history(&mut primary, &first);
        }
        let current = primary
            .posix_write_file(&first.path, b"current bytes", options.clone())
            .unwrap();
        let empty = primary
            .posix_write_file("/services/exact-empty", b"", options)
            .unwrap();
        let mut kv = InProcessKvStore::new();
        kv.put_serde(
            crate::control_plane::metadata_shard_key(&sifs_file_system_id(), "root"),
            &DfsMetadataShardConfig {
                file_system_id: sifs_file_system_id(),
                shard_id: "root".into(),
                primary_node_id: owner,
                replica_node_ids: Vec::new(),
                epoch: 1,
                assignment: DfsMetadataShardAssignment::Root,
                namespace_prepare_codec: codec,
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            },
            None,
        )
        .unwrap();
        let shared = history_control_plane(kv);
        let primary = primary.with_control_plane(shared.clone());
        let mut client = NetworkedDfsService::new(
            network.add_node(reader),
            LocalContentStore::new(root.join("client-content")).unwrap(),
        )
        .with_control_plane(shared);
        // A cached old head and exact old bytes must not grant read access.
        client.add_transient_object_metadata(stale).unwrap();
        if codec == DfsMetadataNamespacePrepareCodec::PackedV2 {
            client
                .store_command_bytes(
                    &first.object_id,
                    &first.version,
                    first.content_hash.as_deref().unwrap(),
                    &bytes,
                )
                .unwrap();
        }
        let stop = Arc::new(AtomicBool::new(false));
        let primary_stop = stop.clone();
        let pump = std::thread::spawn(move || pump_sifs_service_until_stop(primary, primary_stop));
        let result = (|| {
            let read_current = client.read_object_version_content(
                &current.object_id,
                &current.version,
                current.content_hash.as_deref().unwrap(),
                current.size_bytes,
            )?;
            assert_eq!(read_current, b"current bytes");
            assert!(
                client
                    .read_object_version_content(
                        &current.object_id,
                        &current.version,
                        first.content_hash.as_deref().unwrap(),
                        current.size_bytes,
                    )
                    .is_err()
            );
            assert!(
                client
                    .read_object_version_content(
                        &current.object_id,
                        &current.version,
                        current.content_hash.as_deref().unwrap(),
                        Some(1),
                    )
                    .is_err()
            );
            assert!(
                client
                    .read_object_version_content(
                        &empty.object_id,
                        &empty.version,
                        empty.content_hash.as_deref().unwrap(),
                        Some(0),
                    )?
                    .is_empty()
            );
            let retained = client.read_object_version_content(
                &first.object_id,
                &first.version,
                first.content_hash.as_deref().unwrap(),
                first.size_bytes,
            );
            if codec.supports_file_history() {
                assert_eq!(retained?, bytes);
                // Keep a full local copy after a successful remote read, then
                // release its root. Availability must not imply admission.
                client.store_command_bytes(
                    &first.object_id,
                    &first.version,
                    first.content_hash.as_deref().unwrap(),
                    &bytes,
                )?;
                client.file_history_pin(DfsFileHistoryPinRequest {
                    path: first.path.clone(),
                    identity: history_identity(&first),
                    operation: DfsFileHistoryPinOperation::Unpin {
                        pin_id: "checkpoint".into(),
                    },
                    operation_id: "exact-read-release-checkpoint".into(),
                })?;
                assert!(
                    client
                        .read_object_version_content(
                            &first.object_id,
                            &first.version,
                            first.content_hash.as_deref().unwrap(),
                            first.size_bytes,
                        )
                        .is_err(),
                    "cached historical bytes do not survive release of their explicit root"
                );
            } else {
                assert!(
                    retained.is_err(),
                    "older shard codecs still admit only the exact current version"
                );
            }
            Ok::<_, DfsError>(())
        })();
        stop.store(true, Ordering::SeqCst);
        pump.join().unwrap();
        result.unwrap();
    }
}

fn retained_local_publication() -> DfsWritePublication {
    DfsWritePublication::Retained {
        capture: Box::new(DfsRetainedWritePublication {
            pin_id: "preserved-local-operation".into(),
            source: DfsFileVersionSource::LocalOperation {
                operation_id: "explicit-preserve-local".into(),
                base_version: Some(DfsVersion::from_legacy_named("disconnected-base").unwrap()),
            },
        }),
    }
}

#[test]
fn file_history_retained_content_publication_and_restart_never_replace_current_file() {
    let root = temp_root("retained-content-commit");
    let owner = NodeId::from_legacy_named("retained-content-owner").unwrap();
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let path = "/services/preserved-local-file";
    let stat = dfs
        .posix_write_file(
            path,
            b"canonical",
            DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .unwrap();
    let original = dfs.current_read_object_metadata(&stat.object_id).unwrap();
    let target = DfsVersion::from_legacy_named("preserved-local-content").unwrap();
    let bytes = b"disconnected local operation bytes";
    let hash = content_hash(bytes);
    let intent = dfs
        .coordinator
        .create_intent_for_publication(
            stat.file_system_id.clone(),
            stat.object_id.clone(),
            None,
            target.clone(),
            owner.clone(),
            0,
            vec![owner.clone()],
            DfsReplicationPolicy::new(1, 1).unwrap(),
            false,
            None,
            Some("preserve-local-content".into()),
            Some("explicit-preserve-local".into()),
            None,
            retained_local_publication(),
        )
        .unwrap();
    let mut descriptor = original.clone();
    descriptor.version = target.clone();
    descriptor.content_hash = Some(hash.clone());
    descriptor.content_size_bytes = Some(bytes.len() as u64);
    let record = crate::intent::write_intent_record(&intent, Some(&hash), Some(descriptor));
    dfs.metadata_backend
        .as_mut()
        .unwrap()
        .put_intent(&record)
        .unwrap();
    dfs.store_command_bytes(&stat.object_id, &target, &hash, bytes)
        .unwrap();
    dfs.writes.pending_writes.insert(
        intent.intent_id.clone(),
        crate::transfer::PendingNetworkedWrite::new(
            stat.object_id.clone(),
            target.clone(),
            hash.clone(),
            bytes.len() as u64,
        ),
    );
    assert!(
        dfs.apply_committed_intent(&intent.intent_id).is_err(),
        "content capture still requires W receipts"
    );
    dfs.put_metadata_receipt(
        &stat.file_system_id,
        &stat.object_id,
        &target,
        &owner,
        &hash,
    )
    .unwrap();
    dfs.coordinator
        .record_durable_receipt(&intent.intent_id, owner.clone())
        .unwrap();
    dfs.apply_committed_intent(&intent.intent_id).unwrap();
    assert!(
        dfs.retained_file_content_is_live(&stat.object_id, &target, &hash)
            .unwrap()
    );
    assert_eq!(
        dfs.current_read_object_metadata(&stat.object_id).unwrap(),
        original
    );
    assert_eq!(dfs.posix_read_file(path).unwrap(), b"canonical");
    let captured = DfsFileVersionIdentity {
        file_system_id: stat.file_system_id.clone(),
        object_id: stat.object_id.clone(),
        version: target.clone(),
    };
    assert!(
        matches!(dfs.read_file_history(DfsFileHistoryReadRequest { identity: captured.clone(), offset: 0, max_bytes: 64 }).unwrap(), DfsFileHistoryReadOutcome::Found { data } if data.bytes == bytes)
    );
    // Replaying the pre-bookkeeping state after a crash must find the existing
    // capture. An independently released pin must not be recreated by replay.
    dfs.file_history_pin(DfsFileHistoryPinRequest {
        path: path.into(),
        identity: captured,
        operation: DfsFileHistoryPinOperation::Unpin {
            pin_id: "preserved-local-operation".into(),
        },
        operation_id: "release-preserved-local".into(),
    })
    .unwrap();
    assert!(
        !dfs.retained_file_content_is_live(&stat.object_id, &target, &hash)
            .unwrap()
    );
    dfs.metadata_backend
        .as_mut()
        .unwrap()
        .put_intent(&record)
        .unwrap();
    drop(dfs);
    let dfs = networked_sifs_with_backend(&root, owner);
    assert_eq!(dfs.posix_stat(path).unwrap().version, stat.version);
    assert_eq!(dfs.posix_read_file(path).unwrap(), b"canonical");
    assert!(
        dfs.metadata_backend()
            .unwrap()
            .version_pins_for_object(&stat.object_id)
            .unwrap()
            .is_empty()
    );
    assert!(
        dfs.conflicts_for_object(&stat.object_id)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn file_history_retained_replica_staging_does_not_publish_or_block_current_content() {
    let root = temp_root("retained-content-stage");
    let owner = NodeId::from_legacy_named("retained-stage-owner").unwrap();
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let path = "/services/stage-local-file";
    let options = DfsWriteOptions::replicated_eventual(vec![owner.clone()]);
    let stat = dfs
        .posix_write_file(path, b"canonical", options.clone())
        .unwrap();
    let object = dfs.current_read_object_metadata(&stat.object_id).unwrap();
    let command = DfsStageBlobCommand {
        request_id: "retained-stage".into(),
        intent_id: "explicit-retained-stage".into(),
        file_system_id: stat.file_system_id.clone(),
        object_id: stat.object_id.clone(),
        expected_base_version: Some(DfsVersion::from_legacy_named("old-local-base").unwrap()),
        version: DfsVersion::from_legacy_named("retained-stage-target").unwrap(),
        content_hash: content_hash(b"local candidate"),
        bytes: b"local candidate".to_vec(),
        object_metadata: Some(object.clone()),
        authority_shard: None,
    };
    let mut foreign = command.clone();
    foreign.file_system_id =
        DfsFileSystemId::from_legacy_named("foreign-retained-filesystem").unwrap();
    foreign.object_metadata.as_mut().unwrap().file_system_id = foreign.file_system_id.clone();
    assert!(matches!(
        dfs.handle_stage_retained_blob(owner.clone(), foreign, None)
            .unwrap(),
        NetworkedDfsServiceEvent::BlobStageRejected { .. }
    ));
    assert!(matches!(
        dfs.handle_stage_retained_blob(owner.clone(), command.clone(), None)
            .unwrap(),
        NetworkedDfsServiceEvent::BlobStaged { .. }
    ));
    assert_eq!(
        dfs.current_read_object_metadata(&stat.object_id).unwrap(),
        object
    );
    assert!(
        !dfs.writes
            .active_replica_stages
            .contains_key(&stat.object_id)
    );
    let mut conflicting = command;
    conflicting.content_hash = content_hash(b"other candidate");
    conflicting.bytes = b"other candidate".to_vec();
    assert!(matches!(
        dfs.handle_stage_retained_blob(owner, conflicting, None)
            .unwrap(),
        NetworkedDfsServiceEvent::BlobStageRejected { .. }
    ));
    dfs.posix_write_file(path, b"next canonical", options)
        .unwrap();
    assert_eq!(dfs.posix_read_file(path).unwrap(), b"next canonical");
    assert!(
        dfs.list_file_history(DfsFileHistoryListRequest {
            file_system_id: stat.file_system_id,
            path: Some(path.into()),
            object_id: None,
            after: None,
            limit: 64,
        })
        .unwrap()
        .entries
        .is_empty(),
        "staging is not explicit metadata publication"
    );
}

fn history_identity(stat: &DfsStat) -> DfsFileVersionIdentity {
    DfsFileVersionIdentity {
        file_system_id: stat.file_system_id.clone(),
        object_id: stat.object_id.clone(),
        version: stat.version.clone(),
    }
}

fn preserve_request(stat: &DfsStat, operation_id: &str) -> DfsFileHistoryPreserveRequest {
    DfsFileHistoryPreserveRequest {
        path: stat.path.clone(),
        file_system_id: stat.file_system_id.clone(),
        object_id: stat.object_id.clone(),
        local_operation_id: "original-local-operation".into(),
        base_version: Some(stat.version.clone()),
        pin_id: "explicit-local-preservation".into(),
        operation_id: operation_id.into(),
    }
}

#[test]
fn file_history_preserve_checks_capability_before_consuming_a_local_reader() {
    for codec in [
        DfsMetadataNamespacePrepareCodec::Legacy,
        DfsMetadataNamespacePrepareCodec::PackedV1,
        DfsMetadataNamespacePrepareCodec::PackedV2,
    ] {
        preserve_checks_capability_before_consuming_reader(codec);
    }
}

fn preserve_checks_capability_before_consuming_reader(codec: DfsMetadataNamespacePrepareCodec) {
    let root = temp_root("preserve-capability-gate");
    let owner = NodeId::from_legacy_named("preserve-capability-owner").unwrap();
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let options = DfsWriteOptions::replicated_eventual(vec![owner.clone()]);
    let stat = dfs
        .posix_write_file(
            "/services/preserve-capability",
            b"canonical",
            options.clone(),
        )
        .unwrap();
    let mut kv = InProcessKvStore::new();
    kv.put_serde(
        crate::control_plane::metadata_shard_key(&sifs_file_system_id(), "root"),
        &DfsMetadataShardConfig {
            file_system_id: sifs_file_system_id(),
            shard_id: "root".into(),
            primary_node_id: owner,
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: codec,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        },
        None,
    )
    .unwrap();
    let mut dfs = dfs.with_control_plane(history_control_plane(kv));
    let before = dfs
        .metadata_backend()
        .unwrap()
        .journal_last_sequence()
        .unwrap();
    let mut reader = std::io::Cursor::new(b"candidate");
    assert!(
        dfs.preserve_local_file_version(
            preserve_request(&stat, "unsupported-preserve"),
            &mut reader,
            options
        )
        .unwrap_err()
        .to_string()
        .contains("history-capable")
    );
    assert_eq!(reader.position(), 0);
    assert_eq!(
        dfs.metadata_backend()
            .unwrap()
            .journal_last_sequence()
            .unwrap(),
        before
    );
}

#[test]
fn file_history_preserve_keeps_local_owned_bytes_on_the_owner() {
    let root = temp_root("preserve-local-owned");
    let owner = NodeId::from_legacy_named("preserve-local-owned-owner").unwrap();
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let options = DfsWriteOptions::replicated_eventual(vec![owner]);
    let stat = dfs
        .posix_write_file(
            "/services/preserve-local-owned",
            b"private",
            options.clone(),
        )
        .unwrap();
    let mut object = dfs.current_read_object_metadata(&stat.object_id).unwrap();
    object.policy = DfsObjectPolicy::LocalOwned;
    dfs.add_object_metadata(object.clone()).unwrap();
    let outcome = dfs
        .preserve_local_file_version(
            preserve_request(&stat, "preserve-private"),
            &b"private candidate"[..],
            options,
        )
        .unwrap();
    let DfsFileHistoryPreserveOutcome::Retained { identity } = outcome else {
        panic!("local owner is durable");
    };
    let version = dfs
        .metadata_backend()
        .unwrap()
        .get_version(&identity.object_id, &identity.version)
        .unwrap()
        .unwrap();
    assert_eq!(version.locations, vec![object.owner_node_id.clone()]);
    assert_eq!(
        dfs.current_read_object_metadata(&stat.object_id).unwrap(),
        object
    );
    assert_eq!(dfs.posix_read_file(&stat.path).unwrap(), b"private");
}

#[test]
fn file_history_preserve_routes_to_authority_and_keeps_both_current_views() {
    let root = temp_root("preserve-local-routed-api");
    let owner = NodeId::from_legacy_named("preserve-routed-owner").unwrap();
    let requester = NodeId::from_legacy_named("preserve-routed-requester").unwrap();
    let network = InProcessNetwork::new();
    let mut primary = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("primary-content")).unwrap(),
    )
    .with_metadata_backend(open_root_local_metadata_backend(root.join("primary-metadata")).unwrap())
    .unwrap();
    primary.bootstrap_distributed_sifs().unwrap();
    let stat = primary
        .posix_write_file(
            "/services/retained-routed",
            b"canonical",
            DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .unwrap();
    let mut kv = InProcessKvStore::new();
    kv.put_serde(
        crate::control_plane::metadata_shard_key(&sifs_file_system_id(), "root"),
        &DfsMetadataShardConfig {
            file_system_id: sifs_file_system_id(),
            shard_id: "root".into(),
            primary_node_id: owner.clone(),
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: DfsMetadataNamespacePrepareCodec::PackedV3,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        },
        None,
    )
    .unwrap();
    let shared = history_control_plane(kv);
    let primary = primary.with_control_plane(shared.clone());
    let mut client = NetworkedDfsService::new(
        network.add_node(requester.clone()),
        LocalContentStore::new(root.join("requester-content")).unwrap(),
    )
    .with_control_plane(shared);
    let stop = Arc::new(AtomicBool::new(false));
    let primary_stop = stop.clone();
    let pump = std::thread::spawn(move || pump_sifs_service_until_stop(primary, primary_stop));
    let result = (|| {
        let request = preserve_request(&stat, "preserve-routed-once");
        let options = DfsWriteOptions {
            replica_candidates: vec![requester],
            policy: DfsReplicationPolicy::new(1, 1).unwrap(),
            wait_for_commit: true,
            foreground_replica_repair: false,
        };
        let outcome = client.preserve_local_file_version(
            request.clone(),
            &b"local staged alternative"[..],
            options.clone(),
        )?;
        let DfsFileHistoryPreserveOutcome::Retained { identity } = outcome else {
            panic!("preservation must reach W");
        };
        assert_eq!(
            client
                .current_read_object_metadata(&stat.object_id)?
                .version,
            stat.version
        );
        let retry = client.preserve_local_file_version(
            request,
            &b"local staged alternative"[..],
            options,
        )?;
        assert_eq!(
            retry,
            DfsFileHistoryPreserveOutcome::Retained {
                identity: identity.clone()
            }
        );
        client.read_file_history(DfsFileHistoryReadRequest {
            identity,
            offset: 0,
            max_bytes: 64,
        })
    })();
    stop.store(true, Ordering::SeqCst);
    let primary = pump.join().unwrap();
    assert!(
        matches!(result.unwrap(), DfsFileHistoryReadOutcome::Found { data } if data.bytes == b"local staged alternative")
    );
    assert_eq!(
        primary.posix_stat(&stat.path).unwrap().version,
        stat.version
    );
    assert_eq!(primary.posix_read_file(&stat.path).unwrap(), b"canonical");
}

#[test]
fn file_history_preserve_api_replays_without_repinning_or_replacing_current_content() {
    let root = temp_root("preserve-local-reader-api");
    let owner = NodeId::from_legacy_named("preserve-reader-owner").unwrap();
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let options = DfsWriteOptions::replicated_eventual(vec![owner.clone()]);
    let stat = dfs
        .posix_write_file("/services/preserve-reader", b"base", options.clone())
        .unwrap();
    let request = preserve_request(&stat, "preserve-reader-once");
    let current = dfs
        .posix_write_file(&stat.path, b"canonical moved on", options.clone())
        .unwrap();
    let outcome = dfs
        .preserve_local_file_version(request.clone(), &b"local candidate"[..], options.clone())
        .unwrap();
    let DfsFileHistoryPreserveOutcome::Retained { identity } = outcome else {
        panic!("local W reached");
    };
    assert_ne!(identity.version, current.version);
    assert_eq!(dfs.posix_stat(&stat.path).unwrap().version, current.version);
    assert!(
        matches!(dfs.read_file_history(DfsFileHistoryReadRequest { identity: identity.clone(), offset: 0, max_bytes: 64 }).unwrap(), DfsFileHistoryReadOutcome::Found { data } if data.bytes == b"local candidate")
    );
    dfs.file_history_pin(DfsFileHistoryPinRequest {
        path: stat.path.clone(),
        identity: identity.clone(),
        operation: DfsFileHistoryPinOperation::Unpin {
            pin_id: request.pin_id.clone(),
        },
        operation_id: "release-local-reader".into(),
    })
    .unwrap();
    drop(dfs);
    let mut dfs = networked_sifs_with_backend(&root, owner);
    assert_eq!(
        dfs.preserve_local_file_version(request.clone(), &b"local candidate"[..], options.clone())
            .unwrap(),
        DfsFileHistoryPreserveOutcome::Retained {
            identity: identity.clone()
        }
    );
    assert!(
        dfs.metadata_backend()
            .unwrap()
            .version_pins_for_object(&stat.object_id)
            .unwrap()
            .is_empty()
    );
    assert!(
        dfs.preserve_local_file_version(request, &b"changed candidate"[..], options)
            .is_err()
    );
    assert_eq!(
        dfs.posix_read_file(&stat.path).unwrap(),
        b"canonical moved on"
    );
    assert!(
        dfs.conflicts_for_object(&stat.object_id)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn file_history_pending_preservation_does_not_block_current_writes_and_publishes_after_w() {
    let root = temp_root("preserve-local-receipt-threshold");
    let owner = NodeId::from_legacy_named("preserve-threshold-owner").unwrap();
    let remote = NodeId::from_legacy_named("preserve-threshold-remote").unwrap();
    let network = InProcessNetwork::new();
    let mut primary = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner-content")).unwrap(),
    )
    .with_metadata_backend(open_root_local_metadata_backend(root.join("owner-metadata")).unwrap())
    .unwrap();
    primary.bootstrap_distributed_sifs().unwrap();
    let ordinary = DfsWriteOptions::replicated_eventual(vec![owner.clone()]);
    let stat = primary
        .posix_write_file("/services/preserve-threshold", b"base", ordinary.clone())
        .unwrap();
    let request = preserve_request(&stat, "preserve-needs-two-replicas");
    let pending = primary
        .preserve_local_file_version(
            request,
            &b"candidate awaiting W"[..],
            DfsWriteOptions {
                replica_candidates: vec![owner.clone(), remote.clone()],
                policy: DfsReplicationPolicy::new(2, 2).unwrap(),
                wait_for_commit: false,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    let DfsFileHistoryPreserveOutcome::Pending {
        identity,
        intent_id,
    } = pending
    else {
        panic!("offline replica prevents W");
    };
    assert!(
        primary
            .metadata_backend()
            .unwrap()
            .get_version(&identity.object_id, &identity.version)
            .unwrap()
            .is_none()
    );
    primary
        .posix_write_file(&stat.path, b"next current content", ordinary)
        .unwrap();
    let current = primary
        .current_read_object_metadata(&stat.object_id)
        .unwrap();
    let mut replica = NetworkedDfsService::new(
        network.add_node(remote.clone()),
        LocalContentStore::new(root.join("replica-content")).unwrap(),
    );
    primary.repair_retry_tick = primary.repair_retry_tick.saturating_add(100);
    primary
        .stage_missing_desired_replicas_for_intent(&intent_id, current.clone())
        .unwrap();
    let replica_events = replica.process_incoming().unwrap();
    let primary_events = primary.process_incoming().unwrap();
    assert!(
        matches!(
            primary.write_outcome(&intent_id).unwrap(),
            NetworkedDfsWriteOutcome::Committed { .. }
        ),
        "replica events: {replica_events:?}; primary events: {primary_events:?}"
    );
    assert_eq!(
        primary
            .current_read_object_metadata(&stat.object_id)
            .unwrap(),
        current
    );
    assert_eq!(
        primary.posix_read_file(&stat.path).unwrap(),
        b"next current content"
    );
    assert!(
        matches!(primary.read_file_history(DfsFileHistoryReadRequest { identity, offset: 0, max_bytes: 64 }).unwrap(), DfsFileHistoryReadOutcome::Found { data } if data.bytes == b"candidate awaiting W")
    );
    assert!(
        replica.metadata.object(&stat.object_id).is_none(),
        "staging historical bytes must not synthesize a current replica object"
    );
}

#[test]
fn file_history_repairs_retained_bytes_without_rewinding_the_replica() {
    let root = temp_root("history-storage-only-repair");
    let owner = NodeId::from_legacy_named("history-repair-owner").unwrap();
    let replica = NodeId::from_legacy_named("history-repair-replica").unwrap();
    let network = InProcessNetwork::new();
    let mut replica_transport = network.add_node(replica.clone());
    let mut primary = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("primary-content")).unwrap(),
    )
    .with_metadata_backend(open_root_local_metadata_backend(root.join("primary-metadata")).unwrap())
    .unwrap();
    primary.bootstrap_distributed_sifs().unwrap();
    let options = DfsWriteOptions::replicated_eventual(vec![owner.clone()]);
    let path = "/services/repair-retained";
    let first = primary
        .posix_write_file(path, b"retained bytes", options.clone())
        .unwrap();
    capture_history(&mut primary, &first);
    primary
        .posix_write_file(path, b"new current bytes", options)
        .unwrap();
    let current = primary
        .current_read_object_metadata(&first.object_id)
        .unwrap();
    let outcomes = primary
        .send_repair_replica_attachments_if_routable(
            &[(replica.clone(), "repair-explicit-version".into())],
            "repair-history",
            &first.file_system_id,
            &first.object_id,
            Some(first.version.clone()),
            &first.version,
            &content_hash(b"retained bytes"),
            b"retained bytes".len() as u64,
            Some(current.clone()),
        )
        .unwrap();
    assert!(outcomes[0].sent);
    let mut commands = drain_dfs_messages(&mut replica_transport);
    assert_eq!(commands.len(), 1);
    let DfsNetworkMessage::RepairRetainedBlob(command) = commands.remove(0) else {
        panic!("retained content must use storage-only repair");
    };
    assert_eq!(command.bytes, b"retained bytes");
    let mut replica_service = NetworkedDfsService::new(
        replica_transport,
        LocalContentStore::new(root.join("replica-content")).unwrap(),
    )
    .with_metadata_backend(open_root_local_metadata_backend(root.join("replica-metadata")).unwrap())
    .unwrap();
    replica_service
        .add_file_system(DfsFileSystem::sifs(owner.clone()))
        .unwrap();
    replica_service
        .add_object_metadata(current.clone())
        .unwrap();
    assert!(matches!(
        replica_service
            .handle_repair_retained_blob(owner, command, None)
            .unwrap(),
        NetworkedDfsServiceEvent::ReplicaRepaired { .. }
    ));
    assert_eq!(
        replica_service
            .current_read_object_metadata(&first.object_id)
            .unwrap(),
        current
    );
    assert_eq!(
        replica_service
            .content
            .read_object_version(first.object_id.as_str(), first.version.as_str())
            .unwrap()
            .1,
        b"retained bytes"
    );
    assert!(
        !replica_service
            .writes
            .active_replica_stages
            .contains_key(&first.object_id)
    );
}

fn capture_history<T: DfsMessageTransport>(dfs: &mut NetworkedDfsService<T>, stat: &DfsStat) {
    dfs.file_history_pin(DfsFileHistoryPinRequest {
        path: stat.path.clone(),
        identity: history_identity(stat),
        operation: DfsFileHistoryPinOperation::Capture {
            pin_id: "checkpoint".into(),
        },
        operation_id: format!("capture-{}", stat.version),
    })
    .unwrap();
}

#[test]
fn file_history_restore_rejects_wrong_input_before_publishing_a_destination() {
    let root = temp_root("restore-source-identity");
    let owner = NodeId::from_legacy_named("restore-source-identity-owner").unwrap();
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let options = DfsWriteOptions::replicated_eventual(vec![owner]);
    let source = dfs
        .posix_write_file(
            "/services/restore-identity-source",
            b"saved",
            options.clone(),
        )
        .unwrap();
    capture_history(&mut dfs, &source);
    let path = "/services/restore-identity-destination";
    let request = DfsFileHistoryRestoreRequest {
        source: history_identity(&source),
        destination_path: path.into(),
        expected_destination: None,
        operation_id: "restore-source-identity-once".into(),
    };
    let job = dfs
        .begin_file_restore_job(path, &request, &options)
        .unwrap();
    let job = dfs
        .posix_resume_namespace_job_bounded(&job.job_id, 2)
        .unwrap();
    let state = crate::service_restore_jobs::restore_job_state(&job)
        .unwrap()
        .clone();
    let parent_before = dfs.posix_stat("/services").unwrap();
    let before = dfs
        .metadata_backend()
        .unwrap()
        .journal_last_sequence()
        .unwrap();
    let input = crate::service_file_restore::FileRestoreInput::Reader {
        reader: Box::new(&b"wrong"[..]),
        content_hash: content_hash(b"saved"),
        size_bytes: 5,
    };
    assert!(
        dfs.posix_restore_file_content_for_requester(
            path,
            &request,
            input,
            options.clone(),
            &state.requester_node_id,
            &state.write_operation_id
        )
        .is_err()
    );
    assert!(dfs.posix_stat(path).is_err());
    assert_eq!(
        dfs.posix_stat("/services").unwrap().version,
        parent_before.version
    );
    assert_eq!(
        dfs.metadata_backend()
            .unwrap()
            .journal_last_sequence()
            .unwrap(),
        before
    );
    assert!(
        dfs.content
            .read_manifest(
                state.planned.object_id.as_str(),
                state.planned.version.as_str()
            )
            .is_err(),
        "rejected input must remain unbound quarantine"
    );
    let response = dfs.restore_file_history(request, options).unwrap();
    assert_eq!(response.committed, state.planned);
    assert_eq!(dfs.posix_read_file(path).unwrap(), b"saved");
}

#[test]
fn file_history_restore_runs_on_the_primary_with_the_original_requester_identity() {
    let root = temp_root("restore-routed-job");
    let owner = NodeId::from_legacy_named("restore-routed-primary").unwrap();
    let requester = NodeId::from_legacy_named("restore-routed-requester").unwrap();
    let network = InProcessNetwork::new();
    let mut primary = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("primary-content")).unwrap(),
    )
    .with_metadata_backend(open_root_local_metadata_backend(root.join("primary-metadata")).unwrap())
    .unwrap();
    primary.bootstrap_distributed_sifs().unwrap();
    let options = DfsWriteOptions::replicated_eventual(vec![owner.clone()]);
    let source = primary
        .posix_write_file(
            "/services/routed-restore-source",
            b"saved remote content",
            options.clone(),
        )
        .unwrap();
    let destination = primary
        .posix_write_file(
            "/services/routed-restore-destination",
            b"before",
            options.clone(),
        )
        .unwrap();
    let mut kv = InProcessKvStore::new();
    kv.put_serde(
        crate::control_plane::metadata_shard_key(&sifs_file_system_id(), "root"),
        &DfsMetadataShardConfig {
            file_system_id: sifs_file_system_id(),
            shard_id: "root".into(),
            primary_node_id: owner.clone(),
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: DfsMetadataNamespacePrepareCodec::PackedV3,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        },
        None,
    )
    .unwrap();
    let shared = history_control_plane(kv);
    let primary = primary.with_control_plane(shared.clone());
    let mut client = NetworkedDfsService::new(
        network.add_node(requester.clone()),
        LocalContentStore::new(root.join("client-content")).unwrap(),
    )
    .with_control_plane(shared);
    let request = DfsFileHistoryRestoreRequest {
        source: history_identity(&source),
        destination_path: destination.path.clone(),
        expected_destination: Some(history_identity(&destination)),
        operation_id: "routed-restore-once".into(),
    };
    let stop = Arc::new(AtomicBool::new(false));
    let primary_stop = stop.clone();
    let pump = std::thread::spawn(move || pump_sifs_service_until_stop(primary, primary_stop));
    let result = (|| {
        client.file_history_pin(DfsFileHistoryPinRequest {
            path: source.path.clone(),
            identity: history_identity(&source),
            operation: DfsFileHistoryPinOperation::Capture {
                pin_id: "checkpoint".into(),
            },
            operation_id: "routed-checkpoint-before-restore".into(),
        })?;
        let response = client.restore_file_history(request.clone(), options.clone())?;
        assert_eq!(
            client
                .posix_read_file_distributed(&destination.path)
                .map_err(|source| DfsError::Metadata {
                    message: source.to_string()
                })?,
            b"saved remote content"
        );
        assert_eq!(
            client
                .restore_file_history(request.clone(), options)
                .unwrap(),
            response
        );
        Ok::<_, DfsError>(response)
    })();
    stop.store(true, Ordering::SeqCst);
    let primary = pump.join().unwrap();
    let response = result.unwrap();
    let job_id =
        crate::service_restore_jobs::file_restore_job_id(&requester, &request.operation_id);
    let job = primary
        .metadata_backend()
        .unwrap()
        .get_namespace_job(&job_id)
        .unwrap()
        .unwrap();
    let state = crate::service_restore_jobs::restore_job_state(&job).unwrap();
    assert_eq!(state.requester_node_id, requester);
    assert_eq!(state.planned, response.committed);
    assert!(state.cleanup_complete);
    let receipts = primary
        .metadata_backend()
        .unwrap()
        .receipts_for_version(&response.committed.object_id, &response.committed.version)
        .unwrap();
    assert_eq!(
        receipts
            .iter()
            .map(|receipt| receipt.replica_node_id.clone())
            .collect::<Vec<_>>(),
        vec![owner],
        "the executing primary owns the ingested bytes; requester identity is not a storage receipt"
    );
    assert_eq!(
        primary
            .metadata_backend()
            .unwrap()
            .version_pins_for_object(&source.object_id)
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn file_history_restore_recovers_a_commit_after_later_edit_and_replays_compacted_result() {
    let root = temp_root("restore-durable-result");
    let owner = NodeId::from_legacy_named("restore-durable-result-owner").unwrap();
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let options = DfsWriteOptions::replicated_eventual(vec![owner.clone()]);
    let path = "/services/durable-restore";
    let source = dfs
        .posix_write_file(path, b"saved", options.clone())
        .unwrap();
    capture_history(&mut dfs, &source);
    let edited = dfs
        .posix_write_file(path, b"before restore", options.clone())
        .unwrap();
    let request = DfsFileHistoryRestoreRequest {
        source: history_identity(&source),
        destination_path: path.into(),
        expected_destination: Some(history_identity(&edited)),
        operation_id: "restore-durable-result-once".into(),
    };
    let job = dfs
        .begin_file_restore_job(path, &request, &options)
        .unwrap();
    let job = dfs
        .posix_resume_namespace_job_bounded(&job.job_id, 2)
        .unwrap();
    let state = crate::service_restore_jobs::restore_job_state(&job)
        .unwrap()
        .clone();
    assert!(state.source_pin_applied && state.result.is_none());
    // The ordinary write commits, then the coordinator crashes before it can
    // checkpoint the restore result. A subsequent writer advances the head.
    let descriptor = dfs
        .resolve_file_history_version(&request.source)
        .unwrap()
        .unwrap();
    let input = dfs.open_file_restore_reader(&descriptor).unwrap();
    let committed = dfs
        .posix_restore_file_content_for_requester(
            path,
            &request,
            input,
            options.clone(),
            &state.requester_node_id,
            &state.write_operation_id,
        )
        .unwrap();
    dfs.posix_write_file(path, b"later edit", options.clone())
        .unwrap();
    dfs.metadata_backend
        .as_mut()
        .unwrap()
        .gc_metadata_history(
            DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 0)
                .with_now_unix_nanos(u64::MAX),
        )
        .unwrap();
    let write_intent_id = crate::intent::stable_write_intent_id(
        &owner,
        &state.planned.object_id,
        &state.write_operation_id,
    );
    assert!(
        dfs.metadata_backend()
            .unwrap()
            .get_intent_receipt(&state.planned.object_id, &write_intent_id)
            .unwrap()
            .is_none(),
        "the unfinished job must retain its full commit witness"
    );
    drop(dfs);
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let response = dfs
        .restore_file_history(request.clone(), options.clone())
        .unwrap();
    assert_eq!(response.committed, history_identity(&committed));
    assert_eq!(dfs.posix_read_file(path).unwrap(), b"later edit");
    dfs.file_history_pin(DfsFileHistoryPinRequest {
        path: path.into(),
        identity: history_identity(&source),
        operation: DfsFileHistoryPinOperation::Unpin {
            pin_id: "checkpoint".into(),
        },
        operation_id: "release-source-after-restore".into(),
    })
    .unwrap();
    assert!(matches!(
        dfs.read_file_history(DfsFileHistoryReadRequest {
            identity: history_identity(&source),
            offset: 0,
            max_bytes: 64
        })
        .unwrap(),
        DfsFileHistoryReadOutcome::NotRetained { .. }
    ));
    dfs.metadata_backend
        .as_mut()
        .unwrap()
        .gc_metadata_history(
            DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 0)
                .with_now_unix_nanos(u64::MAX),
        )
        .unwrap();
    assert!(
        dfs.metadata_backend()
            .unwrap()
            .get_namespace_job(&job.job_id)
            .unwrap()
            .is_none()
    );
    assert!(
        dfs.metadata_backend()
            .unwrap()
            .get_namespace_job_receipt(&job.job_id)
            .unwrap()
            .unwrap()
            .file_restore
            .is_some()
    );
    drop(dfs);
    let mut dfs = networked_sifs_with_backend(&root, owner);
    assert_eq!(
        dfs.restore_file_history(request, options).unwrap(),
        response
    );
    assert_eq!(dfs.posix_read_file(path).unwrap(), b"later edit");
}

#[test]
fn file_history_cancelled_restore_keeps_its_source_until_recovery() {
    let root = temp_root("restore-cancelled-source-owner");
    let owner = NodeId::generate();
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let options = DfsWriteOptions::replicated_eventual(vec![owner.clone()]);
    let path = "/services/cancelled-restore";
    let saved = dfs
        .posix_write_file(path, b"saved", options.clone())
        .unwrap();
    capture_history(&mut dfs, &saved);
    let edited = dfs
        .posix_write_file(path, b"edited", options.clone())
        .unwrap();
    let request = DfsFileHistoryRestoreRequest {
        source: history_identity(&saved),
        destination_path: path.into(),
        expected_destination: Some(history_identity(&edited)),
        operation_id: sinomo_core::Id::generate().to_string(),
    };
    let job = dfs
        .begin_file_restore_job(path, &request, &options)
        .unwrap();
    let job = dfs
        .posix_resume_namespace_job_bounded(&job.job_id, 2)
        .unwrap();
    let state = crate::service_restore_jobs::restore_job_state(&job)
        .unwrap()
        .clone();
    assert!(state.source_pin_applied && state.result.is_none());
    let DfsFileHistoryPinOperation::Pin { pin_id: job_pin } = state.source_pin.unwrap().operation
    else {
        panic!("source pin")
    };
    let pins = dfs
        .metadata_backend()
        .unwrap()
        .version_pins_for_object(&saved.object_id)
        .unwrap();
    for pin in pins.into_iter().filter(|pin| pin.pin_id != job_pin) {
        dfs.file_history_pin(DfsFileHistoryPinRequest {
            path: path.into(),
            identity: history_identity(&saved),
            operation: DfsFileHistoryPinOperation::Unpin { pin_id: pin.pin_id },
            operation_id: sinomo_core::Id::generate().to_string(),
        })
        .unwrap();
    }
    let cancellation = DfsCancellationFlag::new();
    cancellation.cancel();
    dfs.set_cancellation(Some(cancellation));
    assert!(
        dfs.posix_resume_namespace_job_bounded(&job.job_id, 4)
            .is_err()
    );
    let persisted = dfs
        .metadata_backend()
        .unwrap()
        .get_namespace_job(&job.job_id)
        .unwrap()
        .unwrap();
    assert_eq!(persisted.status, DfsNamespaceJobStatus::Running);
    assert!(
        crate::service_restore_jobs::restore_job_state(&persisted)
            .unwrap()
            .result
            .is_none()
    );
    assert_eq!(
        dfs.metadata_backend()
            .unwrap()
            .get_object(&edited.object_id)
            .unwrap()
            .unwrap()
            .version,
        edited.version
    );
    dfs.metadata_backend
        .as_mut()
        .unwrap()
        .gc_metadata_history(
            DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 0)
                .with_now_unix_nanos(u64::MAX),
        )
        .unwrap();
    assert!(
        dfs.metadata_backend()
            .unwrap()
            .get_version(&saved.object_id, &saved.version)
            .unwrap()
            .is_some()
    );
    drop(dfs);
    let mut dfs = networked_sifs_with_backend(&root, owner);
    let resumed = dfs
        .posix_resume_namespace_job_bounded(&job.job_id, 4)
        .unwrap();
    assert_eq!(resumed.status, DfsNamespaceJobStatus::Completed);
    assert_eq!(dfs.posix_read_file(path).unwrap(), b"saved");
    assert!(
        !dfs.metadata_backend()
            .unwrap()
            .version_pins_for_object(&saved.object_id)
            .unwrap()
            .iter()
            .any(|pin| pin.pin_id == job_pin)
    );
    assert_eq!(
        dfs.restore_file_history(request, options)
            .unwrap()
            .committed,
        state.planned
    );
}

#[test]
fn file_history_restore_cleanup_survives_restart_and_lost_unpin_reply() {
    let root = temp_root("restore-durable-cleanup");
    let owner = NodeId::from_legacy_named("restore-durable-cleanup-owner").unwrap();
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let options = DfsWriteOptions::replicated_eventual(vec![owner.clone()]);
    let path = "/services/restore-cleanup";
    let source = dfs
        .posix_write_file(path, b"saved", options.clone())
        .unwrap();
    capture_history(&mut dfs, &source);
    let edited = dfs
        .posix_write_file(path, b"edited", options.clone())
        .unwrap();
    let request = DfsFileHistoryRestoreRequest {
        source: history_identity(&source),
        destination_path: path.into(),
        expected_destination: Some(history_identity(&edited)),
        operation_id: "restore-cleanup-once".into(),
    };
    let initial = dfs
        .begin_file_restore_job(path, &request, &options)
        .unwrap();
    let job = dfs
        .posix_resume_namespace_job_bounded(&initial.job_id, 3)
        .unwrap();
    let state = crate::service_restore_jobs::restore_job_state(&job)
        .unwrap()
        .clone();
    assert!(state.result.is_some() && !state.cleanup_complete);
    let before = dfs
        .metadata_backend()
        .unwrap()
        .journal_last_sequence()
        .unwrap();
    assert!(
        dfs.metadata_backend
            .as_mut()
            .unwrap()
            .put_namespace_job(&initial)
            .is_err()
    );
    assert_eq!(
        dfs.metadata_backend()
            .unwrap()
            .journal_last_sequence()
            .unwrap(),
        before
    );
    let pin = state.source_pin.unwrap();
    let DfsFileHistoryPinOperation::Pin { pin_id } = pin.operation.clone() else {
        panic!("restore pin");
    };
    dfs.file_history_pin(DfsFileHistoryPinRequest {
        operation: DfsFileHistoryPinOperation::Unpin {
            pin_id: pin_id.clone(),
        },
        operation_id: format!("restore-unpin:{}", job.job_id),
        ..pin
    })
    .unwrap();
    dfs.posix_write_file(path, b"after successful restore", options.clone())
        .unwrap();
    drop(dfs);
    let mut dfs = networked_sifs_with_backend(&root, owner);
    let report = dfs.posix_resume_namespace_jobs_bounded(8, 4).unwrap();
    assert_eq!(report.completed_job_count, 1);
    assert!(
        !dfs.metadata_backend()
            .unwrap()
            .version_pins_for_object(&source.object_id)
            .unwrap()
            .iter()
            .any(|pin| pin.pin_id == pin_id)
    );
    let response = dfs.restore_file_history(request, options).unwrap();
    assert_eq!(response.committed, state.planned);
    assert_eq!(
        dfs.posix_read_file(path).unwrap(),
        b"after successful restore"
    );
}

#[test]
fn file_history_reads_restore_and_deleted_recovery_keep_one_current_view() {
    let root = temp_root("history-restore-and-recovery");
    let owner = NodeId::from_legacy_named("history-restore-owner").unwrap();
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let options = DfsWriteOptions::replicated_eventual(vec![owner]);
    let path = "/services/history-document";
    let original = dfs
        .posix_write_file(path, b"original\n", options.clone())
        .unwrap();
    let identity = history_identity(&original);
    assert!(matches!(
        dfs.read_file_history(DfsFileHistoryReadRequest {
            identity: identity.clone(),
            offset: 0,
            max_bytes: 64
        })
        .unwrap(),
        DfsFileHistoryReadOutcome::NotRetained { .. }
    ));
    capture_history(&mut dfs, &original);
    let edited = dfs
        .posix_write_file(path, b"edited\n", options.clone())
        .unwrap();
    let page = dfs
        .list_file_history(DfsFileHistoryListRequest {
            file_system_id: identity.file_system_id.clone(),
            path: Some(path.into()),
            object_id: None,
            after: None,
            limit: 128,
        })
        .unwrap();
    assert_eq!(page.entries.len(), 1);
    assert!(!page.entries[0].is_current);
    let read = dfs
        .read_file_history(DfsFileHistoryReadRequest {
            identity: identity.clone(),
            offset: 2,
            max_bytes: 3,
        })
        .unwrap();
    let DfsFileHistoryReadOutcome::Found { data } = read else {
        panic!("retained bytes must be readable");
    };
    assert_eq!(data.bytes, b"igi");
    assert_eq!(dfs.posix_stat(path).unwrap().version, edited.version);
    let restored = dfs
        .restore_file_history(
            DfsFileHistoryRestoreRequest {
                source: identity.clone(),
                destination_path: path.into(),
                expected_destination: Some(history_identity(&edited)),
                operation_id: "restore-history-document".into(),
            },
            options.clone(),
        )
        .unwrap();
    assert_ne!(restored.committed.version, identity.version);
    assert_ne!(restored.committed.version, edited.version);
    assert_eq!(dfs.posix_read_file(path).unwrap(), b"original\n");
    assert!(
        dfs.restore_file_history(
            DfsFileHistoryRestoreRequest {
                source: identity.clone(),
                destination_path: path.into(),
                expected_destination: Some(history_identity(&edited)),
                operation_id: "stale-restore".into()
            },
            options.clone()
        )
        .is_err()
    );
    assert_eq!(
        dfs.posix_stat(path).unwrap().version,
        restored.committed.version
    );
    dfs.posix_unlink(path).unwrap();
    let recovered = dfs
        .restore_file_history(
            DfsFileHistoryRestoreRequest {
                source: identity.clone(),
                destination_path: path.into(),
                expected_destination: None,
                operation_id: "recover-history-document".into(),
            },
            options,
        )
        .unwrap();
    assert_ne!(recovered.committed.object_id, identity.object_id);
    assert_eq!(
        dfs.posix_stat(path).unwrap().object_id,
        recovered.committed.object_id,
        "releasing an old version pin must not re-publish its deleted object's tombstone"
    );
    assert_eq!(dfs.posix_read_file(path).unwrap(), b"original\n");
    assert!(
        dfs.conflicts_for_object(&identity.object_id)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn file_history_query_routes_to_primary_and_reads_an_old_replica_range() {
    let root = temp_root("history-remote-query");
    let owner = NodeId::from_legacy_named("history-primary").unwrap();
    let requester = NodeId::from_legacy_named("history-requester").unwrap();
    let network = InProcessNetwork::new();
    let backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root.join("metadata"))
            .unwrap();
    let mut primary = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("primary-content")).unwrap(),
    )
    .with_metadata_backend(backend)
    .unwrap();
    primary.bootstrap_distributed_sifs().unwrap();
    let options = DfsWriteOptions::replicated_eventual(vec![owner.clone()]);
    let original = primary
        .posix_write_file(
            "/services/remote-history",
            b"remote-original",
            options.clone(),
        )
        .unwrap();
    capture_history(&mut primary, &original);
    let edited = primary
        .posix_write_file("/services/remote-history", b"current", options)
        .unwrap();
    let journal_before_reads = primary
        .metadata_backend()
        .unwrap()
        .journal_last_sequence()
        .unwrap();
    let mut kv = InProcessKvStore::new();
    let shard = DfsMetadataShardConfig {
        file_system_id: sifs_file_system_id(),
        shard_id: "root".into(),
        primary_node_id: owner,
        replica_node_ids: Vec::new(),
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: DfsMetadataNamespacePrepareCodec::PackedV3,
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    kv.put_serde(
        crate::control_plane::metadata_shard_key(&sifs_file_system_id(), "root"),
        &shard,
        None,
    )
    .unwrap();
    let shared = history_control_plane(kv);
    let primary = primary.with_control_plane(shared.clone());
    let mut client = NetworkedDfsService::new(
        network.add_node(requester),
        LocalContentStore::new(root.join("client-content")).unwrap(),
    )
    .with_control_plane(shared);
    let stop = Arc::new(AtomicBool::new(false));
    let primary_stop = stop.clone();
    let pump = std::thread::spawn(move || pump_sifs_service_until_stop(primary, primary_stop));
    let result = (|| {
        let page = client.list_file_history(DfsFileHistoryListRequest {
            file_system_id: sifs_file_system_id(),
            path: Some("/services".into()),
            object_id: None,
            after: None,
            limit: 128,
        })?;
        assert_eq!(page.entries.len(), 1);
        client.read_file_history(DfsFileHistoryReadRequest {
            identity: history_identity(&original),
            offset: 7,
            max_bytes: 8,
        })
    })();
    stop.store(true, Ordering::SeqCst);
    let primary = pump.join().unwrap();
    let DfsFileHistoryReadOutcome::Found { data } = result.unwrap() else {
        panic!("historical replica range unavailable");
    };
    assert_eq!(data.bytes, b"original");
    assert_eq!(
        primary
            .metadata_backend()
            .unwrap()
            .journal_last_sequence()
            .unwrap(),
        journal_before_reads,
        "history queries must not publish metadata or mint another version"
    );
    assert_eq!(
        primary
            .posix_stat("/services/remote-history")
            .unwrap()
            .version,
        edited.version
    );
}

#[test]
fn file_history_cursor_cannot_be_reused_for_another_query() {
    let root = temp_root("history-query-cursor");
    let owner = NodeId::from_legacy_named("history-cursor-owner").unwrap();
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let options = DfsWriteOptions::replicated_eventual(vec![owner]);
    for path in ["/services/history-a", "/services/history-b"] {
        let stat = dfs
            .posix_write_file(path, b"content", options.clone())
            .unwrap();
        capture_history(&mut dfs, &stat);
    }
    let mut request = DfsFileHistoryListRequest {
        file_system_id: sifs_file_system_id(),
        path: Some("/services".into()),
        object_id: None,
        after: None,
        limit: 1,
    };
    let page = dfs.list_file_history(request.clone()).unwrap();
    request.after = Some(page.next_cursor.expect("bounded scan has another page"));
    request.path = Some("/users".into());
    assert!(
        dfs.list_file_history(request)
            .unwrap_err()
            .to_string()
            .contains("another query")
    );
}

#[test]
fn file_history_hardlink_capture_uses_canonical_identity_and_follows_promotion() {
    let root = temp_root("history-hardlink-capture");
    let owner = NodeId::from_legacy_named("history-hardlink-owner").unwrap();
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let options = DfsWriteOptions::replicated_eventual(vec![owner]);
    let original = "/services/history-original";
    let alias = "/services/history-alias";
    dfs.posix_write_file(original, b"shared inode", options)
        .unwrap();
    dfs.posix_hardlink(original, alias).unwrap();
    let through_alias = dfs.posix_stat(alias).unwrap();
    capture_history(&mut dfs, &through_alias);
    let identity = history_identity(&through_alias);
    let entry = dfs
        .list_file_history(DfsFileHistoryListRequest {
            file_system_id: sifs_file_system_id(),
            object_id: Some(identity.object_id.clone()),
            path: None,
            after: None,
            limit: 128,
        })
        .unwrap();
    assert_eq!(entry.entries.len(), 1);
    assert_eq!(entry.entries[0].captured.object.path, original);
    dfs.posix_unlink(original).unwrap();
    assert_eq!(dfs.posix_stat(alias).unwrap().object_id, identity.object_id);
    dfs.file_history_pin(DfsFileHistoryPinRequest {
        path: original.into(),
        identity: identity.clone(),
        operation: DfsFileHistoryPinOperation::Unpin {
            pin_id: "checkpoint".into(),
        },
        operation_id: "unpin-after-promotion".into(),
    })
    .unwrap();
    assert_eq!(dfs.posix_read_file(alias).unwrap(), b"shared inode");
    assert!(
        dfs.list_file_history(DfsFileHistoryListRequest {
            file_system_id: sifs_file_system_id(),
            object_id: Some(identity.object_id),
            path: None,
            after: None,
            limit: 128
        })
        .unwrap()
        .entries
        .is_empty()
    );
}

#[test]
fn file_history_policy_applies_to_normal_writes_without_creating_conflicts() {
    let root = temp_root("history-service-retention");
    let owner = NodeId::from_legacy_named("history-policy-owner").unwrap();
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let options = DfsWriteOptions::replicated_eventual(vec![owner.clone()]);
    let mut fs = crate::namespace::distributed_sifs(owner);
    fs.file_history.rules.push(DfsFileHistoryRule {
        path: "/services".into(),
        retention: DfsFileHistoryRetention {
            keep_last: 1,
            keep_for_seconds: 0,
        },
    });
    dfs.add_file_system(fs).unwrap();
    let path = "/services/history-policy-file";
    let first = dfs
        .posix_write_file(path, b"first", options.clone())
        .unwrap();
    let second = dfs
        .posix_write_file(path, b"second", options.clone())
        .unwrap();
    let current = dfs.posix_write_file(path, b"current", options).unwrap();
    assert_eq!(dfs.posix_read_file(path).unwrap(), b"current");
    assert!(
        dfs.conflicts_for_object(&first.object_id)
            .unwrap()
            .is_empty()
    );
    let backend = dfs.metadata_backend.as_mut().unwrap();
    for _ in 0..10 {
        backend
            .gc_metadata_history(
                DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 0)
                    .with_now_unix_nanos(u64::MAX),
            )
            .unwrap();
    }
    assert!(
        backend
            .get_version(&first.object_id, &first.version)
            .unwrap()
            .is_none()
    );
    assert!(
        backend
            .get_version(&second.object_id, &second.version)
            .unwrap()
            .is_some()
    );
    assert_eq!(
        backend
            .get_version(&current.object_id, &current.version)
            .unwrap()
            .unwrap()
            .history_order,
        2
    );
}

#[test]
fn file_history_service_capture_and_unpin_preserve_the_current_namespace() {
    let root = temp_root("file-history-service");
    let owner = NodeId::from_legacy_named("history-owner").unwrap();
    let content = LocalContentStore::new(root.join("content")).unwrap();
    let mut object = file_metadata(owner.clone());
    object.path = "/src/lib.rs".into();
    let manifest = content
        .put_object_version(object.object_id.as_str(), object.version.as_str(), b"first")
        .unwrap();
    object.content_hash = Some(manifest.content_hash);
    object.content_size_bytes = Some(5);
    let mut backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root.join("metadata"))
            .unwrap();
    backend
        .put_file_system(&file_system(owner.clone()))
        .unwrap();
    backend
        .put_object_and_version(
            &object,
            &crate::metadata_records::object_version_record(&object, Some(5)),
        )
        .unwrap();
    let network = InProcessNetwork::new();
    let mut dfs = NetworkedDfsService::new(network.add_node(owner), content)
        .with_metadata_backend(backend)
        .unwrap();
    let mut request = DfsFileHistoryPinRequest {
        path: object.path.clone(),
        identity: DfsFileVersionIdentity {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            version: object.version.clone(),
        },
        operation: DfsFileHistoryPinOperation::Capture {
            pin_id: "before-edit".into(),
        },
        operation_id: "capture-before-edit".into(),
    };
    dfs.file_history_pin(request.clone()).unwrap();
    let backend = dfs.metadata_backend().unwrap();
    assert_eq!(
        backend.get_object(&object.object_id).unwrap(),
        Some(object.clone())
    );
    assert!(
        backend
            .get_version(&object.object_id, &object.version)
            .unwrap()
            .unwrap()
            .retained
            .is_some()
    );
    assert_eq!(
        backend
            .version_pins_for_object(&object.object_id)
            .unwrap()
            .len(),
        1
    );
    assert!(
        backend
            .conflicts_for_object(&object.object_id)
            .unwrap()
            .is_empty()
    );
    request.operation = DfsFileHistoryPinOperation::Unpin {
        pin_id: "before-edit".into(),
    };
    request.operation_id = "release-before-edit".into();
    dfs.file_history_pin(request).unwrap();
    assert!(
        dfs.metadata_backend()
            .unwrap()
            .version_pins_for_object(&object.object_id)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        dfs.metadata_backend()
            .unwrap()
            .get_object(&object.object_id)
            .unwrap(),
        Some(object)
    );
}

#[test]
fn file_history_cli_capture_defaults_to_observing_current_identity() {
    let args = [
        "history",
        "capture",
        "/users/document",
        "--pin",
        "before-edit",
    ]
    .into_iter()
    .map(str::to_string)
    .collect::<Vec<_>>();
    let command = crate::cli::parse_dfs_command(&args).unwrap();
    let crate::cli::DfsCliCommand::HistoryPin(options) = command else {
        panic!("history command");
    };
    assert_eq!(options.path, "/users/document");
    assert!(options.object_id.is_none());
    assert!(options.version.is_none());
    assert_eq!(
        options.operation,
        DfsFileHistoryPinOperation::Capture {
            pin_id: "before-edit".into()
        }
    );
    let args = [
        "history",
        "unpin",
        "/users/document",
        "--pin",
        "before-edit",
    ]
    .into_iter()
    .map(str::to_string)
    .collect::<Vec<_>>();
    assert!(crate::cli::parse_dfs_command(&args).is_err());
}
