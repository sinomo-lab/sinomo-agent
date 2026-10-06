//! Cross shard regression tests.

use super::*;
#[test]
fn returning_adoption_checks_the_physical_eviction_head_before_publication() {
    for change_marker in [false, true] {
        let label = if change_marker {
            "returning-adoption-marker-race"
        } else {
            "returning-adoption-marker"
        };
        let root = temp_root(label);
        let node = NodeId::from_legacy_named(label).unwrap();
        let mut shards = sifs_two_primary_shard_configs(&node, &node, 1, Vec::new(), Vec::new());
        for shard in &mut shards {
            shard.namespace_prepare_codec = DfsMetadataNamespacePrepareCodec::PackedV6;
        }
        let mut kv = InProcessKvStore::new();
        let mut member = sinomo_core::ClusterMemberRecord::local_non_voting(
            sinomo_core::Id::from_legacy_named(label).unwrap(),
            node.clone(),
            label,
            format!("{label}-key"),
            OffsetDateTime::UNIX_EPOCH,
        );
        member.state = sinomo_core::ClusterMemberState::Active;
        member.capabilities = sinomo_core::node::main_daemon_capabilities(&member.role);
        member.allowed_capabilities = member.capabilities.clone();
        kv.put_serde(sinomo_core::node::cluster_member_key(&node), &member, None)
            .unwrap();
        kv.put_serde(
            sinomo_core::node::cluster_capability_policy_key(&node),
            &sinomo_core::node::ClusterCapabilityPolicyRecord::from(&member),
            None,
        )
        .unwrap();
        {
            let mut control = DfsControlPlaneStore::new(&mut kv);
            control
                .put_file_system_config(&DfsFileSystemConfig::from(&distributed_sifs(node.clone())))
                .unwrap();
            for shard in &shards {
                control.put_metadata_shard(shard).unwrap();
            }
        }
        let mut dfs = NetworkedDfsService::new(
            crate::test_runtime::DfsTestRuntime::local_transport(node.clone()),
            LocalContentStore::new(root.join("content")).unwrap(),
        )
        .with_control_plane(kv)
        .with_metadata_backend(
            open_local_sharded_metadata_backend(root.join("metadata"), shards).unwrap(),
        )
        .unwrap();
        dfs.bootstrap_distributed_sifs().unwrap();
        let stat = dfs
            .posix_write_file(
                "/services/old-location",
                b"returning object",
                DfsWriteOptions::replicated_eventual(vec![node]),
            )
            .unwrap();
        let source = dfs
            .metadata_backend()
            .unwrap()
            .get_object(&stat.object_id)
            .unwrap()
            .unwrap();
        let mut moved = source.clone();
        moved.path = "/users/returning-source".into();
        moved.version = crate::namespace::next_version(&source.version);
        dfs.metadata_backend
            .as_mut()
            .unwrap()
            .evict_object_record_for_foreign_shard_move(&moved, &source.path)
            .unwrap();
        let eviction = dfs
            .metadata_backend()
            .unwrap()
            .get_object_on_shard(&DfsMetadataShardId::new("services"), &source.object_id)
            .unwrap()
            .unwrap();
        assert_eq!(eviction.kind, DfsObjectKind::Tombstone);
        assert!(matches!(
            eviction.tombstone_provenance,
            Some(DfsTombstoneProvenance::SourceEviction { .. })
        ));
        assert_eq!(
            dfs.metadata_backend()
                .unwrap()
                .get_object_by_path_on_shard(
                    &DfsMetadataShardId::new("services"),
                    &source.file_system_id,
                    &source.path,
                )
                .unwrap(),
            Some(eviction.clone()),
            "source eviction keeps its old physical path occupant"
        );
        // The fixture evicts through the backend directly. Refresh the
        // service's normal projection before asserting the visible namespace.
        dfs.hydrate_from_metadata_backend().unwrap();
        assert!(dfs.posix_stat(&source.path).is_err());
        let mut incoming = source;
        incoming.path = "/services/returned-location".into();
        incoming.version = crate::namespace::next_version(&eviction.version);
        incoming.changed_at_unix_nanos = eviction.changed_at_unix_nanos.saturating_add(1);
        let mut successor = eviction.clone();
        successor.version = crate::namespace::next_version(&incoming.version);
        successor.changed_at_unix_nanos = incoming.changed_at_unix_nanos.saturating_add(1);
        let expected_head = if change_marker {
            successor.clone()
        } else {
            crate::metadata_records::normalized_object_metadata(&incoming)
        };
        let _hook = change_marker.then(|| {
            crate::service_metadata::install_namespace_prepare_after_expectations_test_hook(
                move |backend| {
                    backend.put_object(&successor).unwrap();
                },
            )
        });
        let result = dfs.adopt_object_at_path_local(
            &incoming,
            "/users/returning-source",
            None,
            &[],
            &[],
            None,
            &[],
            &[],
            &[],
            &DfsWriteIntentReceiptRetentionState::default(),
            &[],
            None,
            &[],
            None,
        );
        if change_marker {
            assert!(
                result.is_err(),
                "a changed physical eviction head must reject adoption"
            );
            assert!(dfs.posix_stat(&incoming.path).is_err());
            assert!(
                dfs.metadata_backend()
                    .unwrap()
                    .get_version(&incoming.object_id, &incoming.version)
                    .unwrap()
                    .is_none()
            );
        } else {
            result.expect("the exact physical eviction can be replaced by the returning object");
            assert_eq!(
                dfs.posix_stat(&incoming.path).unwrap().object_id,
                incoming.object_id
            );
        }
        assert_eq!(
            dfs.metadata_backend()
                .unwrap()
                .get_object_on_shard(&DfsMetadataShardId::new("services"), &incoming.object_id)
                .unwrap(),
            Some(expected_head),
            "adoption must preserve an intervening physical head and publish only its own exact input"
        );
    }
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
        "reference-noop-two-moves"
    } else {
        "reference-release-two-moves"
    };
    let root = temp_root(label);
    let nodes = ["a", "b", "c", "requester"]
        .map(|suffix| NodeId::from_legacy_named(format!("{label}-{suffix}")).unwrap());
    let network = InProcessNetwork::new();
    let mut genesis = networked_sifs_with_backend(&root.join("genesis"), nodes[0].clone());
    genesis.posix_mkdir_all("/services/third").unwrap();
    let file_system = genesis
        .metadata_backend()
        .unwrap()
        .get_file_system(&sifs_file_system_id())
        .unwrap()
        .unwrap();
    let page = genesis
        .metadata_backend()
        .unwrap()
        .objects_for_file_system_page(&sifs_file_system_id(), None, 128)
        .unwrap();
    assert!(page.next_cursor.is_none());
    let bootstrap_directories = page.objects;
    assert!(
        bootstrap_directories
            .iter()
            .all(|object| object.kind == DfsObjectKind::Directory)
    );
    let mut shards =
        sifs_two_primary_shard_configs(&nodes[0], &nodes[1], 1, Vec::new(), Vec::new());
    shards.push(DfsMetadataShardConfig {
        file_system_id: sifs_file_system_id(),
        shard_id: "third".into(),
        primary_node_id: nodes[2].clone(),
        replica_node_ids: Vec::new(),
        epoch: 1,
        assignment: DfsMetadataShardAssignment::PathPrefix {
            prefix: "/services/third".into(),
        },
        namespace_prepare_codec: DfsMetadataNamespacePrepareCodec::PackedV6,
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    });
    for shard in &mut shards {
        shard.namespace_prepare_codec = DfsMetadataNamespacePrepareCodec::PackedV6;
    }
    assert!(shards.iter().all(|shard| shard.replica_node_ids.is_empty()));
    let mut kv = InProcessKvStore::new();
    let cluster_id = sinomo_core::Id::from_legacy_named(label).unwrap();
    for node_id in &nodes {
        let mut member = sinomo_core::ClusterMemberRecord::local_non_voting(
            cluster_id.clone(),
            node_id.clone(),
            "reference move fixture",
            format!("{node_id}-key"),
            OffsetDateTime::UNIX_EPOCH,
        );
        member.state = sinomo_core::ClusterMemberState::Active;
        member.capabilities = sinomo_core::node::main_daemon_capabilities(&member.role);
        member.allowed_capabilities = member.capabilities.clone();
        kv.put_serde(
            sinomo_core::node::cluster_member_key(node_id),
            &member,
            None,
        )
        .unwrap();
        kv.put_serde(
            sinomo_core::node::cluster_capability_policy_key(node_id),
            &sinomo_core::node::ClusterCapabilityPolicyRecord::from(&member),
            None,
        )
        .unwrap();
    }
    let control_plane = SharedDfsControlPlaneKvClient::new(kv);
    control_plane.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(&file_system))
            .unwrap();
        for shard in &shards {
            control.put_metadata_shard(shard).unwrap();
        }
    });
    let mut services = nodes
        .iter()
        .enumerate()
        .map(|(index, node_id)| {
            let node_root = root.join(format!("node-{index}"));
            let mut backend =
                open_local_sharded_metadata_backend(node_root.join("metadata"), shards.clone())
                    .unwrap();
            backend.put_file_system(&file_system).unwrap();
            // Only bootstrap directory state is seeded. Every tested file,
            // reference, move and retry goes through the typed owner routes.
            for directory in &bootstrap_directories {
                backend.put_object(directory).unwrap();
            }
            NetworkedDfsService::new(
                network.add_node(node_id.clone()),
                LocalContentStore::new(node_root.join("content")).unwrap(),
            )
            .with_control_plane(control_plane.clone())
            .with_metadata_backend(backend)
            .unwrap()
        })
        .collect::<Vec<_>>();
    let mut requester = services.pop().unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let (settlement_sender, settlement_receiver) =
        std::sync::mpsc::sync_channel::<(DfsObjectId, std::sync::mpsc::SyncSender<()>)>(1);
    let mut settlement_receiver = Some(settlement_receiver);
    let pumps = services
        .into_iter()
        .enumerate()
        .map(|(index, mut service)| {
            let stop = stop.clone();
            let settlement_receiver = (index == 1).then(|| {
                settlement_receiver
                    .take()
                    .expect("B owns the repair barrier")
            });
            std::thread::spawn(move || {
                let repair_interval =
                    Duration::from_nanos(crate::transfer::repair_retry_delay_nanos_for_ticks(1));
                let mut next_repair = Instant::now();
                let mut pending_settlement = None;
                while !stop.load(Ordering::SeqCst) {
                    service
                        .process_incoming()
                        .expect("reference fixture message pump");
                    if let Some(receiver) = settlement_receiver.as_ref()
                        && let Ok(request) = receiver.try_recv()
                    {
                        assert!(pending_settlement.is_none());
                        pending_settlement = Some(request);
                    }
                    let committed = service.take_message_committed_work();
                    if committed.repair_maintenance || Instant::now() >= next_repair {
                        service
                            .retry_pending_repairs_bounded(8)
                            .expect("reference fixture ordinary repair worker");
                        next_repair = Instant::now() + repair_interval;
                    }
                    if let Some((object_id, reply)) = pending_settlement.as_ref() {
                        let backend = service.metadata_backend().expect("source-owner metadata");
                        let object = backend
                            .get_object(object_id)
                            .expect("source-owner current head")
                            .expect("source object remains present before its second move");
                        let intents = backend
                            .intents_for_object_page(object_id, None, 8)
                            .expect("bounded source-owner intent query");
                        assert!(
                            intents.next_cursor.is_none(),
                            "the fixture must not hide additional source repair obligations"
                        );
                        let pending_writes_settled =
                            service
                                .writes
                                .pending_writes
                                .iter()
                                .all(|(intent_id, pending)| {
                                    pending.object_id != *object_id
                                        || service.coordinator.intent(intent_id).is_some_and(
                                            |intent| {
                                                intent.state == DfsWriteIntentState::Committed
                                                    && intent.target_version == object.version
                                            },
                                        )
                                });
                        let durable_intents_settled = intents.intents.iter().all(|intent| {
                            intent.status != DfsWriteIntentStatus::Staging
                                && (intent.status != DfsWriteIntentStatus::ReceiptsPending
                                    || intent.target_version == object.version)
                        });
                        if pending_writes_settled
                            && durable_intents_settled
                            && !service.coordinator.has_open_intent_for_object(object_id)
                        {
                            reply
                                .send(())
                                .expect("requester still owns the repair barrier");
                            pending_settlement = None;
                        }
                    }
                    if !stop.load(Ordering::SeqCst) {
                        let _ = service
                            .transport
                            .wait_for_incoming(SIFS_CLUSTER_PUMP_IDLE_SLEEP);
                    }
                }
                service
            })
        })
        .collect::<Vec<_>>();
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        requester.bootstrap_distributed_sifs().unwrap();
        let bytes = b"retained bytes across disjoint authorities";
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
        let source = DfsRevisionReference {
            file_system_id: stat.file_system_id.clone(),
            object_id: stat.object_id.clone(),
            version: stat.version.clone(),
        };
        let capture = DfsContentReferenceRequest {
            wire_format: DfsContentReferenceFormat::NamespacedV6,
            namespace: "org.example.reference-moves".into(),
            path: stat.path.clone(),
            identity: source.clone(),
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
        // The requested locator is still A, but this first effect is admitted
        // at B. A's read-only reconciliation proves it has no prior outcome.
        let before_noop =
            no_op_release.then(|| requester.revision_metadata(&source).unwrap().unwrap());
        requester.retain_content_revision(release.clone()).unwrap();
        if let Some(before) = before_noop {
            let after = requester.revision_metadata(&source).unwrap().unwrap();
            assert_eq!(after.current_object, before.current_object);
            assert_eq!(after.version, before.version);
        }
        let retain = DfsContentReferenceRequest {
            operation: DfsContentReferenceOperation::Retain {
                reference_id: "reused-leaf".into(),
            },
            operation_id: sinomo_core::Id::generate().to_string(),
            ..capture.clone()
        };
        requester.retain_content_revision(retain.clone()).unwrap();
        requester.retain_content_revision(release.clone()).unwrap();
        assert!(requester.content_reference_present(&retain).unwrap());
        requester
            .retain_content_revision(DfsContentReferenceRequest {
                operation: DfsContentReferenceOperation::Release {
                    reference_id: "capture-root".into(),
                },
                operation_id: sinomo_core::Id::generate().to_string(),
                ..capture
            })
            .unwrap();
        // Replaying the reference outcome wakes real retained-repair work at
        // B. Let the source owner's normal worker settle that exact object's
        // obligations before asking it to move authority again.
        let (settled_sender, settled_receiver) = std::sync::mpsc::sync_channel(1);
        settlement_sender
            .send((source.object_id.clone(), settled_sender))
            .unwrap();
        let settlement_deadline =
            Instant::now() + crate::service_state::DFS_ROUTED_NAMESPACE_OP_TIMEOUT;
        loop {
            match settled_receiver.try_recv() {
                Ok(()) => break,
                Err(std::sync::mpsc::TryRecvError::Empty) => {}
                Err(error) => panic!("source-owner repair worker ended before settlement: {error}"),
            }
            assert!(
                Instant::now() < settlement_deadline,
                "B's retained repair must settle within the existing network deadline"
            );
            requester.process_incoming().unwrap();
            let _ = requester
                .transport
                .wait_for_incoming(SIFS_CLUSTER_PUMP_IDLE_SLEEP);
        }
        requester
            .posix_rename("/services/reference-b", "/services/third/reference-c")
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
                    &source,
                    stat.content_hash.as_deref().unwrap(),
                    stat.size_bytes
                )
                .unwrap(),
            bytes
        );
        (release, source)
    }));
    stop.store(true, Ordering::SeqCst);
    let primaries = pumps
        .into_iter()
        .map(|pump| pump.join().unwrap())
        .collect::<Vec<_>>();
    let (release, source) = match outcome {
        Ok(outcome) => outcome,
        Err(error) => std::panic::resume_unwind(error),
    };
    let transaction_id = format!("file-history:{}", release.operation_id);
    let a = primaries[0].metadata_backend().unwrap();
    assert!(
        a.get_namespace_transaction(&transaction_id)
            .unwrap()
            .is_none()
    );
    assert!(
        a.get_namespace_transaction_receipt(&transaction_id)
            .unwrap()
            .is_none()
    );
    let b = primaries[1].metadata_backend().unwrap();
    // A normal commit can immediately retain only its compact receipt. Both
    // representations must prove the original effect, independent of repairs.
    let admitted = if let Some(transaction) = b.get_namespace_transaction(&transaction_id).unwrap()
    {
        crate::DfsNamespaceTransactionReceipt::from_transaction(
            &transaction,
            transaction.created_at,
        )
        .unwrap()
    } else {
        b.get_namespace_transaction_receipt(&transaction_id)
            .unwrap()
            .expect("B retains the exact admitted outcome in full or compact form")
    };
    assert_eq!(admitted.transaction_id, transaction_id);
    assert_eq!(admitted.file_system_id, source.file_system_id);
    assert_eq!(admitted.content_reference_identity, Some(source.clone()));
    assert_eq!(
        admitted.contribution_digest,
        Some(
            blake3::hash(&serde_json::to_vec(&release).unwrap())
                .to_hex()
                .to_string()
        )
    );
    let c = primaries[2].metadata_backend().unwrap();
    let transferred = c
        .get_namespace_transaction_receipt(&transaction_id)
        .unwrap()
        .unwrap();
    assert!(
        admitted.same_outcome_identity(&transferred),
        "C must retain B's original tuple, logical input, transaction digest and creation time"
    );
    assert!(transferred.retained_at <= admitted.retained_at);
    assert_eq!(transferred.content_reference_identity, Some(source));
    assert_eq!(
        transferred.contribution_digest,
        Some(
            blake3::hash(&serde_json::to_vec(&release).unwrap())
                .to_hex()
                .to_string()
        )
    );
}

#[test]
fn mkdir_all_cross_shard_error_names_the_actual_child_parent_pair() {
    let root = temp_root("mkdir-all-cross-shard-error-paths");
    let node =
        NodeId::from_legacy_named("node_mkdir_all_cross_shard_error_paths").expect("node id");
    let path = "/services/batched/one/two";
    let parent_path = "/services/batched/one";
    let path_hash = path
        .as_bytes()
        .iter()
        .fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
        });
    let shards = vec![
        DfsMetadataShardConfig {
            file_system_id: sifs_file_system_id(),
            shard_id: "root".to_string(),
            primary_node_id: node.clone(),
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        },
        DfsMetadataShardConfig {
            file_system_id: sifs_file_system_id(),
            shard_id: "hash-selected-leaf".to_string(),
            primary_node_id: node.clone(),
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::NameHashRange {
                start_inclusive: path_hash,
                end_exclusive: path_hash.wrapping_add(1),
            },
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        },
    ];
    let control_plane = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    control_plane.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        for shard in &shards {
            control.put_metadata_shard(shard).expect("metadata shard");
        }
    });
    let metadata =
        open_local_sharded_metadata_backend(root.join("metadata"), shards).expect("metadata");
    let mut dfs = NetworkedDfsService::new(
        crate::test_runtime::DfsTestRuntime::local_transport(node),
        LocalContentStore::new(root.join("content")).expect("content store"),
    )
    .with_control_plane(control_plane)
    .with_metadata_backend(metadata)
    .expect("metadata backend");
    dfs.bootstrap_distributed_sifs().expect("bootstrap SIFS");

    let error = dfs
        .posix_mkdir_all(path)
        .expect_err("cross-shard child-parent pair must be rejected");
    let detail = error.to_string();
    assert!(
        detail.contains(&format!("created object {path} and parent {parent_path}")),
        "cross-shard error must name the actual failing pair, got: {detail}"
    );
    assert!(
        dfs.posix_stat("/services/batched").is_err(),
        "pair validation must finish before publishing the batched chain"
    );
}

#[test]
fn streamed_routed_write_binds_quarantine_after_hardlink_rebase() {
    let root = temp_root("streamed-routed-write-binds-after-rebase");
    let requester = NodeId::from_legacy_named("node_streamed_rebase_requester").expect("node");
    let primary = NodeId::from_legacy_named("node_streamed_rebase_primary").expect("node");
    let target_object_id =
        DfsObjectId::from_legacy_named("obj_streamed_rebase_target").expect("object");
    let alias_object_id =
        DfsObjectId::from_legacy_named("obj_streamed_rebase_alias").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("base version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("target version");
    let network = InProcessNetwork::new();
    let _requester_mailbox = network.add_node(requester.clone());
    let content = LocalContentStore::new(root.join("content")).expect("content store");
    let mut dfs = NetworkedDfsService::new(network.add_node(primary.clone()), content);

    let mut target = file_metadata(primary.clone());
    target.object_id = target_object_id.clone();
    target.path = "/services/streamed-target.bin".to_string();
    target.version = version_1.clone();
    target.policy = DfsObjectPolicy::ReplicatedEventual;
    target.current_locations = vec![primary.clone()];
    dfs.add_object_metadata(target).expect("target metadata");
    let mut alias = file_metadata(primary.clone());
    alias.object_id = alias_object_id.clone();
    alias.path = "/services/streamed-alias.bin".to_string();
    alias.kind = DfsObjectKind::Hardlink;
    alias.link_target_object_id = Some(target_object_id.clone());
    alias.version = version_1.clone();
    alias.policy = DfsObjectPolicy::ReplicatedEventual;
    alias.current_locations = vec![primary.clone()];
    dfs.add_object_metadata(alias.clone())
        .expect("hardlink metadata");

    let bytes = b"streamed content must bind to the canonical hardlink target";
    let expected_hash = content_hash(bytes);
    let mut ingest = dfs
        .content
        .begin_quarantined_blob_ingest(bytes.len() as u64, Some(expected_hash.as_str()))
        .expect("begin quarantine");
    ingest.write_chunk(bytes).expect("write quarantine");
    let blob = ingest.finish_quarantined().expect("finish quarantine");
    let event = dfs
        .handle_preingested_distributed_write_request(
            requester.clone(),
            DfsDistributedWriteCommand {
                publication: Default::default(),
                request_id: "dfs_streamed_rebase_write".to_string(),
                request: DfsWriteRequest {
                    file_system_id: sifs_file_system_id(),
                    object_id: alias_object_id.clone(),
                    target_version: version_2.clone(),
                    requesting_node_id: requester,
                },
                expected_base_version: Some(version_1),
                metadata_journal_after_sequence: None,
                bytes: Vec::new(),
                content_hash: Some(expected_hash.clone()),
                content_size_bytes: Some(bytes.len() as u64),
                requester_blob_pre_staged: false,
                desired_replica_nodes: vec![primary],
                policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
                object_metadata: Some(alias),
                authority_shard: None,
                touch_parent_on_create: false,
                foreground_replica_repair: false,
                exclusive_create: false,
            },
            blob,
        )
        .expect("serve streamed routed write");
    let committed_version = match &event {
        NetworkedDfsServiceEvent::RemoteWriteServed {
            result:
                DfsDistributedWriteResult::Outcome {
                    outcome: NetworkedDfsWriteOutcome::Committed { version, .. },
                },
            ..
        } => version.clone(),
        other => panic!("expected committed routed write, got {other:?}"),
    };
    assert!(
        dfs.content
            .read_manifest(alias_object_id.as_str(), version_2.as_str())
            .is_err(),
        "the pre-rebase alias version must never be reserved"
    );
    let (manifest, stored) = dfs
        .content
        .read_object_version(target_object_id.as_str(), committed_version.as_str())
        .expect("canonical target version");
    assert_eq!(manifest.content_hash, expected_hash);
    assert_eq!(stored, bytes);

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_recursive_clear_preserves_hardlinks_outside_cleared_tree() {
    let root = temp_root("sifs-recursive-clear-hardlink-outside");
    let node_id = NodeId::from_legacy_named("node_sifs_clear_hardlink").expect("node");
    let clear_root = "/services/sandbox/instances/sandbox_clear_hardlink/tmp/0";
    let file_path = "/services/sandbox/instances/sandbox_clear_hardlink/tmp/0/cache.txt";
    let outside_alias = "/services/sandbox/instances/sandbox_clear_hardlink/work/cache-alias.txt";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
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

    let reloaded = networked_sifs_with_backend(&root, node_id);
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

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_hardlink_shares_content_and_link_count() {
    let root = temp_root("sifs-posix-hardlink-shared-content");
    let node_id = NodeId::from_legacy_named("node_sifs_hardlink").expect("node");
    let original = "/services/sandbox/instances/sandbox_hardlink/work/file.txt";
    let link = "/services/sandbox/instances/sandbox_hardlink/work/alias.txt";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
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
    let root = temp_root("sifs-posix-hardlink-extent-range");
    let node_id = NodeId::from_legacy_named("node_sifs_hardlink_extent").expect("node");
    let work = "/services/sandbox/instances/sandbox_hardlink_extent/work";
    let original = "/services/sandbox/instances/sandbox_hardlink_extent/work/file.txt";
    let link = "/services/sandbox/instances/sandbox_hardlink_extent/work/alias.txt";
    let expected = b"alpha-beta";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
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
    let mut reopened = networked_sifs_with_backend(&root, node_id);
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

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_hardlink_preserves_symlink_object() {
    let root = temp_root("sifs-posix-hardlink-symlink");
    let node_id = NodeId::from_legacy_named("node_sifs_hardlink_symlink").expect("node");
    let work = "/services/sandbox/instances/sandbox_hardlink_symlink/work";
    let target = "/services/sandbox/instances/sandbox_hardlink_symlink/work/target.txt";
    let link = "/services/sandbox/instances/sandbox_hardlink_symlink/work/link.txt";
    let alias = "/services/sandbox/instances/sandbox_hardlink_symlink/work/link-alias.txt";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
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

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
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
    let root = temp_root("sifs-posix-rename-hardlink-over-canonical");
    let node_id = NodeId::from_legacy_named("node_sifs_rename_hardlink").expect("node");
    let canonical = "/services/sandbox/instances/sandbox_rename_hardlink/work/file.txt";
    let alias = "/services/sandbox/instances/sandbox_rename_hardlink/work/alias.txt";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
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

    let reloaded = networked_sifs_with_backend(&root, node_id);
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

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_rename_over_canonical_preserves_other_hardlinks() {
    let root = temp_root("sifs-posix-rename-over-canonical-hardlinks");
    let node_id =
        NodeId::from_legacy_named("node_sifs_rename_over_canonical_hardlinks").expect("node");
    let work = "/services/sandbox/instances/sandbox_rename_over_hardlinks/work";
    let source = "/services/sandbox/instances/sandbox_rename_over_hardlinks/work/source.txt";
    let canonical = "/services/sandbox/instances/sandbox_rename_over_hardlinks/work/canonical.txt";
    let alias = "/services/sandbox/instances/sandbox_rename_over_hardlinks/work/alias.txt";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
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

    let reloaded = networked_sifs_with_backend(&root, node_id);
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

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_rename_hardlink_over_canonical_preserves_both_targets() {
    let root = temp_root("sifs-posix-rename-hardlink-over-other-canonical");
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

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
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

    let reloaded = networked_sifs_with_backend(&root, node_id);
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

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_rename_hardlink_alias_updates_target_ctime() {
    let root = temp_root("sifs-posix-rename-hardlink-alias-ctime");
    let node_id = NodeId::from_legacy_named("node_sifs_rename_hardlink_alias_ctime").expect("node");
    let work = "/services/sandbox/instances/sandbox_rename_alias_ctime/work";
    let canonical = "/services/sandbox/instances/sandbox_rename_alias_ctime/work/canonical.txt";
    let alias = "/services/sandbox/instances/sandbox_rename_alias_ctime/work/alias.txt";
    let renamed = "/services/sandbox/instances/sandbox_rename_alias_ctime/work/renamed-alias.txt";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
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

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
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

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
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

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
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
    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
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

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
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

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
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

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
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
    let mut cluster = sifs_posix_cluster("sifs-contract-hardlink-races");
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
    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn lazy_metadata_hydration_restores_running_cross_shard_mutation_guards() {
    let root = temp_root("lazy-metadata-cross-shard-mutation-guards");
    let node = NodeId::from_legacy_named("node_lazy_cross_shard_guards").expect("node");
    let file_system = distributed_sifs(node.clone());
    let guarded_root = "/services/guarded-tree";
    let now = OffsetDateTime::now_utc();
    let job = DfsNamespaceJobRecord {
        job_id: "running-cross-shard-rename".to_string(),
        file_system_id: file_system.file_system_id.clone(),
        origin: None,
        kind: DfsNamespaceJobKind::RenameDirectoryAcrossShardPrimaries,
        status: DfsNamespaceJobStatus::Running,
        root_path: guarded_root.to_string(),
        target_path: Some("/users/guarded-tree".to_string()),
        pending_frames: Vec::new(),
        processed_object_count: 0,
        created_at: now,
        updated_at: now,
        completed_at: None,
        failure_reason: None,
    };
    let metadata_root = root.join("metadata");
    let mut backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &metadata_root)
            .expect("metadata backend");
    backend.put_file_system(&file_system).expect("filesystem");
    backend.put_namespace_job(&job).expect("namespace job");
    drop(backend);

    let service = NetworkedDfsService::new(
        InProcessNetwork::new().add_node(node),
        LocalContentStore::new(root.join("content")).expect("content"),
    )
    .with_metadata_backend_lazy(
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), metadata_root)
            .expect("reopened metadata backend"),
    )
    .expect("lazy metadata service");

    assert!(matches!(
        service.ensure_path_not_in_cross_shard_flight(
            &file_system.file_system_id,
            "/services/guarded-tree/child.txt",
        ),
        Err(DfsError::Busy { .. })
    ));
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn request_shells_share_live_cross_shard_namespace_guards() {
    let root = temp_root("shared-cross-shard-namespace-guards");
    let node = NodeId::from_legacy_named("node_shared_cross_shard_guards").expect("node");
    let file_system = distributed_sifs(node.clone());
    let registry = DfsCrossShardNamespaceLockRegistry::default();
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let global_shell = NetworkedDfsService::new(
        InProcessNetwork::new().add_node(node.clone()),
        content.clone(),
    )
    .with_cross_shard_namespace_lock_registry(registry.clone());
    let exact_shell = NetworkedDfsService::new(InProcessNetwork::new().add_node(node), content)
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
    std::fs::remove_dir_all(root).ok();
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
        root,
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
        users_primary_node: _,
        services_primary_node: _,
        replica_node,
        shared_kv: _,
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
        root,
        users_primary_node: NodeId::from_legacy_named("node_unused_users").expect("node"),
        services_primary_node: NodeId::from_legacy_named("node_unused_services").expect("node"),
        replica_node,
        shared_kv: SharedDfsControlPlaneKvClient::new(InProcessKvStore::new()),
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

    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn sifs_cluster_cross_shard_rename_replays_caller_owned_operation_id() {
    let cluster = sifs_two_primary_cluster("sifs-xshard-stable-rename-replay");
    let SifsTwoPrimaryCluster {
        root,
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
    std::fs::remove_dir_all(root).ok();
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
        sifs_two_primary_cluster_with_codec(
            "sifs-xshard-content-durability",
            DfsMetadataNamespacePrepareCodec::PackedV6,
        )
    } else {
        sifs_two_primary_cluster("sifs-xshard-pinned-rename")
    };
    let SifsTwoPrimaryCluster {
        root,
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
    assert_eq!(source_durability.is_some(), with_durability);
    let pin = DfsMetadataVersionPinRecord {
        namespace: None,
        file_system_id: source.file_system_id.clone(),
        object_id: source.object_id.clone(),
        version: source.version.clone(),
        pin_id: "checkpoint-cross-primary-rename".to_string(),
        created_at: time::OffsetDateTime::now_utc(),
    };
    users_primary
        .metadata_backend
        .as_mut()
        .expect("users metadata backend")
        .put_version_pin(&pin)
        .expect("durable source pin")
        .expect("new source pin");

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
            wire_format: DfsContentReferenceFormat::NamespacedV6,
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

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_same_node_cross_shard_adoption_recovers_pinned_source_after_crash_window() {
    let root = temp_root("sifs-same-node-pinned-adoption-crash");
    let node = NodeId::from_legacy_named("node_same_pinned_adoption").expect("node");
    let shards = sifs_two_primary_shard_configs(&node, &node, 1, Vec::new(), Vec::new());
    let mut kv = InProcessKvStore::new();
    {
        let mut control = DfsControlPlaneStore::new(&mut kv);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(&distributed_sifs(node.clone())))
            .expect("sifs filesystem config");
        for shard in &shards {
            control.put_metadata_shard(shard).expect("metadata shard");
        }
    }
    let mut dfs = NetworkedDfsService::new(
        crate::test_runtime::DfsTestRuntime::local_transport(node.clone()),
        LocalContentStore::new(root.join("content")).expect("content"),
    )
    .with_control_plane(kv)
    .with_metadata_backend(
        open_local_sharded_metadata_backend(root.join("metadata"), shards).expect("metadata"),
    )
    .expect("metadata backend");
    dfs.bootstrap_distributed_sifs().expect("bootstrap SIFS");

    let source_path = "/users/crash-pinned.txt";
    let target_path = "/services/crash-pinned.txt";
    dfs.posix_write_file(
        source_path,
        b"same-node crash-window pin",
        DfsWriteOptions::replicated_eventual(vec![node]),
    )
    .expect("create source");
    let source = dfs.posix_stat(source_path).expect("source metadata");
    let pin = DfsMetadataVersionPinRecord {
        namespace: None,
        file_system_id: source.file_system_id.clone(),
        object_id: source.object_id.clone(),
        version: source.version.clone(),
        pin_id: "checkpoint-same-node-crash".to_string(),
        created_at: time::OffsetDateTime::now_utc(),
    };
    dfs.metadata_backend
        .as_mut()
        .expect("metadata backend")
        .put_version_pin(&pin)
        .expect("source pin")
        .expect("new source pin");

    let capture = dfs
        .metadata_backend
        .as_mut()
        .expect("metadata backend")
        .capture_cross_shard_source_family(&source.object_id, source_path)
        .expect("source family");
    let mut moved = capture.source_object.clone();
    moved.path = target_path.to_string();
    moved.version = crate::namespace::next_version(&source.version);
    moved.changed_at_unix_nanos = moved.changed_at_unix_nanos.saturating_add(1);
    let mut versions = capture.versions.clone();
    versions.push(crate::posix::cross_shard_object_version_record(&moved));
    let transfer_id =
        crate::protocol::cross_shard_pin_transfer_id(&moved, source_path).expect("transfer id");
    let pin_fingerprint =
        crate::protocol::cross_shard_version_pin_set_fingerprint(&moved, &capture.version_pins)
            .expect("pin fingerprint");

    assert!(
        dfs.adopt_object_at_path_local(
            &moved,
            source_path,
            Some(&transfer_id),
            &versions,
            &capture.version_pins,
            Some(&pin_fingerprint),
            &capture.receipts,
            &capture.intents,
            &capture.intent_receipts,
            &capture.intent_receipt_retention,
            &capture.content_reference_outcomes,
            capture.content_reference_outcome_retention.as_ref(),
            &capture.conflicts,
            capture.advisory_lock_state.as_ref(),
        )
        .expect("destination adoption"),
        "a copied pin must create active transfer provenance"
    );
    let adopted_target = dfs.posix_stat(target_path).expect("adopted target");
    assert_eq!(adopted_target.object_id, source.object_id);
    assert_eq!(
        adopted_target.version, moved.version,
        "destination publication must preserve the durable frame target generation"
    );
    assert!(
        dfs.metadata_backend()
            .expect("metadata backend")
            .get_version(&source.object_id, &moved.version)
            .expect("target version lookup")
            .is_some(),
        "the typed adoption receipt generation must exist in destination history"
    );
    assert_eq!(
        dfs.metadata_backend
            .as_mut()
            .expect("metadata backend")
            .capture_cross_shard_source_family(&source.object_id, source_path)
            .expect("pre-recovery source family")
            .version_pins,
        vec![pin.clone()],
        "the crash window retains the physical source pin before fenced cleanup"
    );

    // Model restart/resume after target publication but before source
    // eviction. The object index now resolves target, so recovery must use the
    // explicit old-path shard rather than an authority-routed pin scan.
    dfs.run_cross_shard_adopt(&moved, source_path)
        .expect("recover pinned source cleanup");
    let retired = dfs
        .metadata_backend
        .as_mut()
        .expect("metadata backend")
        .capture_cross_shard_source_family(&source.object_id, source_path)
        .expect("retired source family");
    assert!(retired.version_pins.is_empty());
    assert!(matches!(
        retired.source_object.tombstone_provenance,
        Some(DfsTombstoneProvenance::SourceEviction {
            pin_transfer_id: Some(ref recorded),
            ..
        }) if recorded == &transfer_id
    ));
    assert_eq!(
        dfs.metadata_backend()
            .expect("metadata backend")
            .version_pins_for_object(&source.object_id)
            .expect("authoritative target pins"),
        vec![pin],
        "recovery must preserve the copied pin on the target authority"
    );
    assert!(
        dfs.metadata_backend()
            .expect("metadata backend")
            .cross_shard_pin_transfers_for_object(&source.object_id)
            .expect("finalized transfer state")
            .is_empty()
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_stale_cross_shard_runner_without_source_cannot_bypass_active_pin_transfer() {
    let root = temp_root("sifs-stale-runner-active-pin-transfer");
    let node = NodeId::from_legacy_named("node_stale_runner_active_transfer").expect("node");
    let shards = sifs_two_primary_shard_configs(&node, &node, 1, Vec::new(), Vec::new());
    let mut kv = InProcessKvStore::new();
    {
        let mut control = DfsControlPlaneStore::new(&mut kv);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(&distributed_sifs(node.clone())))
            .expect("sifs filesystem config");
        for shard in &shards {
            control.put_metadata_shard(shard).expect("metadata shard");
        }
    }
    let mut dfs = NetworkedDfsService::new(
        crate::test_runtime::DfsTestRuntime::local_transport(node.clone()),
        LocalContentStore::new(root.join("content")).expect("content"),
    )
    .with_control_plane(kv)
    .with_metadata_backend(
        open_local_sharded_metadata_backend(root.join("metadata"), shards).expect("metadata"),
    )
    .expect("metadata backend");
    dfs.bootstrap_distributed_sifs().expect("bootstrap SIFS");

    let source_path = "/users/stale-runner-source.txt";
    let target_path = "/services/stale-runner-target.txt";
    dfs.posix_write_file(
        target_path,
        b"target exists without a local physical source family",
        DfsWriteOptions::replicated_eventual(vec![node]),
    )
    .expect("create target");
    let target_stat = dfs.posix_stat(target_path).expect("target metadata");
    let target = dfs
        .metadata_backend()
        .expect("metadata backend")
        .get_object(&target_stat.object_id)
        .expect("target object lookup")
        .expect("target object");
    let pin = DfsMetadataVersionPinRecord {
        namespace: None,
        file_system_id: target.file_system_id.clone(),
        object_id: target.object_id.clone(),
        version: target.version.clone(),
        pin_id: "checkpoint-stale-runner-active-transfer".to_string(),
        created_at: time::OffsetDateTime::now_utc(),
    };
    let transfer_id =
        crate::protocol::cross_shard_pin_transfer_id(&target, source_path).expect("transfer id");
    let pin_fingerprint = crate::protocol::cross_shard_version_pin_set_fingerprint(
        &target,
        std::slice::from_ref(&pin),
    )
    .expect("pin fingerprint");
    assert!(
        dfs.adopt_object_at_path_local(
            &target,
            source_path,
            Some(&transfer_id),
            &[],
            std::slice::from_ref(&pin),
            Some(&pin_fingerprint),
            &[],
            &[],
            &[],
            &DfsWriteIntentReceiptRetentionState::default(),
            &[],
            None,
            &[],
            None,
        )
        .expect("install modeled target transfer"),
        "copying a previously absent target pin must retain active transfer provenance"
    );
    assert_eq!(
        dfs.metadata_backend()
            .expect("metadata backend")
            .cross_shard_pin_transfers_for_object(&target.object_id)
            .expect("active transfer")
            .len(),
        1
    );

    let error = dfs.run_cross_shard_adopt(&target, source_path).expect_err(
        "a stale runner without the physical source cannot finalize an active transfer",
    );
    assert!(
        error.to_string().contains("active pin transfer"),
        "unexpected stale-runner rejection: {error}"
    );
    assert_eq!(
        dfs.metadata_backend()
            .expect("metadata backend")
            .cross_shard_pin_transfers_for_object(&target.object_id)
            .expect("retained active transfer")
            .len(),
        1,
        "the read-only proof path must not remove transfer provenance"
    );
    assert_eq!(
        dfs.metadata_backend()
            .expect("metadata backend")
            .version_pins_for_object(&target.object_id)
            .expect("retained copied pin"),
        vec![pin],
        "the stale runner must not reconcile the authoritative copied pin away"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_same_node_cross_shard_adoption_finalizes_pin_transfer_after_source_eviction_crash() {
    let root = temp_root("sifs-same-node-pinned-finalize-crash");
    let node = NodeId::from_legacy_named("node_same_pinned_finalize").expect("node");
    let shards = sifs_two_primary_shard_configs(&node, &node, 1, Vec::new(), Vec::new());
    let mut kv = InProcessKvStore::new();
    {
        let mut control = DfsControlPlaneStore::new(&mut kv);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(&distributed_sifs(node.clone())))
            .expect("sifs filesystem config");
        for shard in &shards {
            control.put_metadata_shard(shard).expect("metadata shard");
        }
    }
    let mut dfs = NetworkedDfsService::new(
        crate::test_runtime::DfsTestRuntime::local_transport(node.clone()),
        LocalContentStore::new(root.join("content")).expect("content"),
    )
    .with_control_plane(kv)
    .with_metadata_backend(
        open_local_sharded_metadata_backend(root.join("metadata"), shards).expect("metadata"),
    )
    .expect("metadata backend");
    dfs.bootstrap_distributed_sifs().expect("bootstrap SIFS");

    let source_path = "/users/finalize-crash-pinned.txt";
    let target_path = "/services/finalize-crash-pinned.txt";
    dfs.posix_write_file(
        source_path,
        b"same-node finalize crash pin",
        DfsWriteOptions::replicated_eventual(vec![node]),
    )
    .expect("create source");
    let source = dfs.posix_stat(source_path).expect("source metadata");
    let pin = DfsMetadataVersionPinRecord {
        namespace: None,
        file_system_id: source.file_system_id.clone(),
        object_id: source.object_id.clone(),
        version: source.version.clone(),
        pin_id: "checkpoint-same-node-finalize-crash".to_string(),
        created_at: time::OffsetDateTime::now_utc(),
    };
    dfs.metadata_backend
        .as_mut()
        .expect("metadata backend")
        .put_version_pin(&pin)
        .expect("source pin")
        .expect("new source pin");

    let capture = dfs
        .metadata_backend
        .as_mut()
        .expect("metadata backend")
        .capture_cross_shard_source_family(&source.object_id, source_path)
        .expect("source family");
    let mut moved = capture.source_object.clone();
    moved.path = target_path.to_string();
    moved.version = crate::namespace::next_version(&capture.source_object.version);
    moved.changed_at_unix_nanos = moved.changed_at_unix_nanos.saturating_add(1);
    let mut versions = capture.versions.clone();
    versions.push(crate::posix::cross_shard_object_version_record(&moved));
    let transfer_id =
        crate::protocol::cross_shard_pin_transfer_id(&moved, source_path).expect("transfer id");
    let pin_fingerprint =
        crate::protocol::cross_shard_version_pin_set_fingerprint(&moved, &capture.version_pins)
            .expect("pin fingerprint");

    assert!(
        dfs.adopt_object_at_path_local(
            &moved,
            source_path,
            Some(&transfer_id),
            &versions,
            &capture.version_pins,
            Some(&pin_fingerprint),
            &capture.receipts,
            &capture.intents,
            &capture.intent_receipts,
            &capture.intent_receipt_retention,
            &capture.content_reference_outcomes,
            capture.content_reference_outcome_retention.as_ref(),
            &capture.conflicts,
            capture.advisory_lock_state.as_ref(),
        )
        .expect("destination adoption"),
        "a copied pin must create active transfer provenance"
    );
    dfs.metadata_backend
        .as_mut()
        .expect("metadata backend")
        .evict_object_record_for_foreign_shard_move_with_pin_fence(
            &moved,
            source_path,
            &capture.source_object,
            capture.source_journal_sequence,
            &capture.version_pins,
            Some(&transfer_id),
        )
        .expect("fenced source eviction");

    let retired_before_recovery = dfs
        .metadata_backend
        .as_mut()
        .expect("metadata backend")
        .capture_cross_shard_source_family(&source.object_id, source_path)
        .expect("retired source family");
    assert!(retired_before_recovery.version_pins.is_empty());
    assert!(matches!(
        retired_before_recovery.source_object.tombstone_provenance,
        Some(DfsTombstoneProvenance::SourceEviction {
            pin_transfer_id: Some(ref recorded),
            ..
        }) if recorded == &transfer_id
    ));
    assert_eq!(
        dfs.metadata_backend()
            .expect("metadata backend")
            .cross_shard_pin_transfers_for_object(&source.object_id)
            .expect("active target transfer")
            .len(),
        1,
        "the modeled crash leaves target transfer provenance active"
    );

    // Resume from the durable source-eviction witness. Recovery must finalize
    // only the matching target transfer and must not attempt to recopy pins
    // from the now-empty source shard.
    dfs.run_cross_shard_adopt(&moved, source_path)
        .expect("recover target finalization");
    assert_eq!(
        dfs.metadata_backend()
            .expect("metadata backend")
            .version_pins_for_object(&source.object_id)
            .expect("target pins"),
        vec![pin],
        "finalization recovery must retain the adopted durable pin"
    );
    assert_eq!(
        dfs.posix_stat(target_path).expect("adopted target").version,
        moved.version,
        "finalization recovery must retain the durable frame target generation"
    );
    assert!(
        dfs.metadata_backend()
            .expect("metadata backend")
            .cross_shard_pin_transfers_for_object(&source.object_id)
            .expect("finalized transfer")
            .is_empty(),
        "recovery must retire only temporary transfer provenance"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_cluster_posix_rename_across_shard_primaries_rejects_invalid_destinations() {
    let cluster = sifs_two_primary_cluster("sifs-xshard-errno");
    let SifsTwoPrimaryCluster {
        root,
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
        root,
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
        root,
        users_primary_node: NodeId::from_legacy_named("node_unused_users2").expect("node"),
        services_primary_node: NodeId::from_legacy_named("node_unused_services2").expect("node"),
        replica_node,
        shared_kv: SharedDfsControlPlaneKvClient::new(InProcessKvStore::new()),
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

    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn cross_primary_hardlink_alias_unlink_fences_the_crash_gap_and_recovers_exactly_once() {
    let cluster = sifs_two_primary_cluster("sifs-hardlink-alias-crash-gap");
    let SifsTwoPrimaryCluster {
        root,
        users_primary_node,
        services_primary_node,
        replica_node,
        shared_kv,
        users_primary,
        services_primary,
        mut replica,
        ..
    } = cluster;
    let canonical_path = "/users/fenced-canonical.txt";
    let alias_path = "/services/fenced-alias";
    let stop = Arc::new(AtomicBool::new(false));
    let users_stop = Arc::clone(&stop);
    let services_stop = Arc::clone(&stop);
    let users_thread =
        std::thread::spawn(move || pump_sifs_service_until_stop(users_primary, users_stop));
    let services_thread =
        std::thread::spawn(move || pump_sifs_service_until_stop(services_primary, services_stop));

    replica
        .posix_write_file(
            canonical_path,
            b"fenced hardlink payload",
            DfsWriteOptions::replicated_eventual(vec![replica_node.clone()]),
        )
        .expect("seed canonical");
    replica
        .posix_hardlink(canonical_path, alias_path)
        .expect("seed cross-primary alias");
    let alias_generation = replica
        .posix_namespace_lstat_distributed(alias_path)
        .expect("capture exact alias generation");
    let expected = DfsNamespaceObjectPrecondition {
        object_id: alias_generation.object_id,
        version: alias_generation.version,
    };
    let canonical_before = replica
        .posix_lstat_distributed(canonical_path)
        .expect("canonical before parked unlink");
    let operation_id = format!("cf_{}", sinomo_core::Id::generate());
    let operation = DfsNamespaceOperation::ConditionalUnlink {
        path: alias_path.to_string(),
        expected: expected.clone(),
    };
    let _hook = crate::posix::install_hardlink_alias_after_commit_test_hook(alias_path, || {
        Err(DfsPosixError::Dfs(DfsError::Network {
            message: "injected crash after durable alias commit".to_string(),
        }))
    });
    let error = replica
        .posix_unlink_if_generation(alias_path, operation_id.clone(), expected.clone())
        .expect_err("post-alias crash hook must park the exact operation");
    assert!(
        matches!(&error, DfsPosixError::Dfs(DfsError::Network { .. }))
            || matches!(
                &error,
                DfsPosixError::Dfs(DfsError::Distributed { message })
                    if message.contains("injected crash after durable alias commit")
            ),
        "unexpected injected crash result: {error:?}"
    );

    stop.store(true, Ordering::SeqCst);
    let mut users_primary = users_thread.join().expect("users primary after crash gap");
    let mut services_primary = services_thread
        .join()
        .expect("services primary after crash gap");

    // The alias transaction is durable and the canonical generation has not
    // moved yet, but neither generation is observable through the public
    // path contract while their durable reservations are Running.
    let parked_request = services_primary
        .metadata_backend()
        .expect("services metadata")
        .get_namespace_request(&replica_node, &operation_id)
        .expect("parked outer request")
        .expect("durable outer request remains available");
    assert_eq!(parked_request.request_id, operation_id);
    assert_eq!(
        parked_request.status,
        DfsNamespaceRequestStatus::Pending,
        "the alias subtransaction must not publish a terminal outer receipt before the canonical generation commits"
    );
    let raw_alias = services_primary
        .metadata_backend()
        .expect("services metadata")
        .get_object_by_path(&sifs_file_system_id(), alias_path)
        .expect("raw alias lookup")
        .expect("raw alias tombstone");
    assert_eq!(raw_alias.kind, DfsObjectKind::Tombstone);
    let raw_canonical = users_primary
        .metadata_backend()
        .expect("users metadata")
        .get_object_by_path(&sifs_file_system_id(), canonical_path)
        .expect("raw canonical lookup")
        .expect("raw canonical object");
    assert_eq!(raw_canonical.version, canonical_before.version);
    for error in [
        services_primary
            .posix_stat(alias_path)
            .expect_err("alias read must be fenced"),
        users_primary
            .posix_stat(canonical_path)
            .expect_err("canonical read must be fenced"),
        services_primary
            .posix_unlink(alias_path)
            .expect_err("conflicting alias mutation must be fenced"),
        users_primary
            .posix_set_mode(canonical_path, 0o600)
            .expect_err("conflicting canonical mutation must be fenced"),
    ] {
        assert!(
            matches!(error, DfsPosixError::Dfs(DfsError::Busy { .. })),
            "parked hardlink generation must fail Busy, got {error:?}"
        );
    }

    // Simulate the process crash by dropping both authority shells, then
    // reopen their durable shard backends. The target fence hydrates first;
    // source maintenance can then finish the exact pre-minted generation.
    let users_transport = users_primary.transport.clone();
    let services_transport = services_primary.transport.clone();
    drop(users_primary);
    drop(services_primary);
    let shards = sifs_two_primary_shard_configs(
        &users_primary_node,
        &services_primary_node,
        2,
        vec![services_primary_node.clone(), replica_node.clone()],
        vec![users_primary_node.clone(), replica_node.clone()],
    );
    let users_primary = NetworkedDfsService::new(
        users_transport,
        LocalContentStore::new(root.join("users-primary/content")).expect("users content reopen"),
    )
    .with_control_plane(shared_kv.clone())
    .with_metadata_backend(
        open_local_sharded_metadata_backend(root.join("users-primary/metadata"), shards.clone())
            .expect("users metadata reopen"),
    )
    .expect("attach reopened users authority");
    let users_stop = Arc::new(AtomicBool::new(false));
    let users_pump_stop = Arc::clone(&users_stop);
    let users_thread =
        std::thread::spawn(move || pump_sifs_service_until_stop(users_primary, users_pump_stop));
    let mut services_primary = NetworkedDfsService::new(
        services_transport,
        LocalContentStore::new(root.join("services-primary/content"))
            .expect("services content reopen"),
    )
    .with_control_plane(shared_kv.clone())
    .with_metadata_backend(
        open_local_sharded_metadata_backend(root.join("services-primary/metadata"), shards)
            .expect("services metadata reopen"),
    )
    .expect("attach reopened services authority");
    services_primary
        .posix_resume_namespace_jobs_bounded(16, 512)
        .expect("resume parked alias job after restart");
    users_stop.store(true, Ordering::SeqCst);
    let users_primary = users_thread.join().expect("reopened users authority");

    let (users_primary, services_primary, replay, reconciliation, conflict) =
        std::thread::scope(|scope| {
            let stop = Arc::new(AtomicBool::new(false));
            let users_stop = Arc::clone(&stop);
            let services_stop = Arc::clone(&stop);
            let users_thread =
                scope.spawn(move || pump_sifs_service_until_stop(users_primary, users_stop));
            let services_thread =
                scope.spawn(move || pump_sifs_service_until_stop(services_primary, services_stop));
            let replay = replica.posix_unlink_if_generation(
                alias_path,
                operation_id.clone(),
                expected.clone(),
            );
            let reconciliation =
                replica.reconcile_stable_namespace_operation(&operation_id, &operation);
            let mut different_expected = expected;
            different_expected.version = DfsVersion::generate();
            let conflict = replica.posix_unlink_if_generation(
                alias_path,
                operation_id.clone(),
                different_expected,
            );
            stop.store(true, Ordering::SeqCst);
            (
                users_thread.join().expect("users replay pump"),
                services_thread.join().expect("services replay pump"),
                replay,
                reconciliation,
                conflict,
            )
        });
    assert_eq!(
        replay.expect("same-id unlink replay").kind,
        DfsObjectKind::Tombstone
    );
    assert!(matches!(
        reconciliation.expect("exact terminal reconciliation"),
        DfsStableNamespaceOperationReconciliation::Applied { .. }
    ));
    assert!(matches!(conflict, Err(DfsPosixError::Conflict { .. })));
    let canonical_after = users_primary
        .posix_stat(canonical_path)
        .expect("canonical after recovery");
    assert_eq!(canonical_after.link_count, 1);
    assert_ne!(canonical_after.version, canonical_before.version);
    assert!(matches!(
        services_primary.posix_stat(alias_path),
        Err(DfsPosixError::NotFound { .. })
    ));

    drop(replica);
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn sifs_cluster_posix_cross_shard_rename_recovers_after_destination_outage() {
    let mut cluster = sifs_two_primary_cluster("sifs-xshard-recovery");
    let ghost = NodeId::from_legacy_named("node_sifs_xshard_recovery_ghost").expect("ghost node");
    let reactive_route_budget = Duration::from_secs(5);

    // Create the source file directly on the users primary (local commit).
    let bytes = b"survives a destination outage".to_vec();
    cluster
        .users_primary
        .posix_write_file(
            "/users/stuck.txt",
            &bytes,
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
                replica_candidates: vec![cluster.users_primary_node.clone()],
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .expect("primary local create");

    // Point the services shard at an unreachable primary: the adoption
    // cannot be sent, so the saga parks as a running recoverable job.
    let to_ghost = sifs_two_primary_shard_configs(
        &cluster.users_primary_node,
        &ghost,
        3,
        vec![
            cluster.services_primary_node.clone(),
            cluster.replica_node.clone(),
        ],
        vec![
            cluster.users_primary_node.clone(),
            cluster.replica_node.clone(),
        ],
    );
    cluster.shared_kv.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        for shard in &to_ghost {
            control.put_metadata_shard(shard).expect("ghost shard");
        }
    });
    // Keep every still-online replica reactive while only the configured
    // destination primary is absent. Otherwise this fixture also models two
    // unrelated replica outages and spends the metadata durability boundary
    // before it reaches the immediately-failing ghost route.
    let services_primary = std::mem::replace(
        &mut cluster.services_primary,
        NetworkedDfsService::new(
            sinomo_core::network::InProcessNetwork::new()
                .add_node(NodeId::from_legacy_named("node_unused_services_pump").expect("node")),
            LocalContentStore::new(cluster.root.join("unused-services-content")).expect("content"),
        ),
    );
    let replica = std::mem::replace(
        &mut cluster.replica,
        NetworkedDfsService::new(
            sinomo_core::network::InProcessNetwork::new()
                .add_node(NodeId::from_legacy_named("node_unused_replica_pump").expect("node")),
            LocalContentStore::new(cluster.root.join("unused-replica-content")).expect("content"),
        ),
    );
    let stop = Arc::new(AtomicBool::new(false));
    let services_stop = Arc::clone(&stop);
    let replica_stop = Arc::clone(&stop);
    let services_thread =
        std::thread::spawn(move || pump_sifs_service_until_stop(services_primary, services_stop));
    let replica_thread =
        std::thread::spawn(move || pump_sifs_service_until_stop(replica, replica_stop));
    cluster.users_primary.begin_message_committed_work();
    let initial_rename_started = Instant::now();
    let error = cluster
        .users_primary
        .posix_rename("/users/stuck.txt", "/services/stuck-moved.txt")
        .expect_err("rename to unreachable destination primary must not block forever");
    assert!(
        initial_rename_started.elapsed() < reactive_route_budget,
        "a known-unreachable destination route must fail reactively instead of consuming a durability timeout: {:?}",
        initial_rename_started.elapsed()
    );
    assert!(
        matches!(error, DfsPosixError::Dfs(DfsError::Network { .. })),
        "expected a transport error, got {error:?}"
    );
    let committed_work = cluster.users_primary.take_message_committed_work();
    assert!(committed_work.publish_metadata);
    assert!(
        committed_work.namespace_maintenance,
        "a running cross-shard job must retain one reactive maintenance owner after transport failure"
    );

    // The in-flight lock protects the half-moved object from concurrent
    // mutation while the saga awaits recovery.
    let write_error = cluster
        .users_primary
        .posix_write_file(
            "/users/stuck.txt",
            b"concurrent write",
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
                replica_candidates: vec![cluster.users_primary_node.clone()],
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .expect_err("writes to an in-flight cross-shard path must be rejected");
    assert!(
        write_error
            .to_string()
            .contains("moving between metadata shards"),
        "unexpected lock error: {write_error}"
    );
    let chmod_error = cluster
        .users_primary
        .posix_set_mode("/users/stuck.txt", 0o600)
        .expect_err("namespace mutations on an in-flight path must be rejected");
    assert!(
        chmod_error
            .to_string()
            .contains("moving between metadata shards"),
        "unexpected namespace lock error: {chmod_error}"
    );
    let parked_resume_started = Instant::now();
    let parked_report = cluster
        .users_primary
        .posix_resume_namespace_jobs_bounded(8, 512)
        .expect("maintenance scan while destination is still unreachable");
    assert!(
        parked_resume_started.elapsed() < reactive_route_budget,
        "a parked job retry must rediscover the still-unreachable destination reactively: {:?}",
        parked_resume_started.elapsed()
    );
    assert_eq!(
        (
            parked_report.resumed_job_count,
            parked_report.completed_job_count,
            parked_report.failed_job_count,
            parked_report.remaining_running_job_count,
        ),
        (1, 0, 0, 1),
        "a transient resume failure must leave the cross-shard saga running: {parked_report:?}"
    );

    // Heal the topology: transfer the services shard to its real primary and
    // resume the parked saga through the maintenance path.
    let healed = sifs_two_primary_shard_configs(
        &cluster.users_primary_node,
        &cluster.services_primary_node,
        4,
        vec![
            cluster.services_primary_node.clone(),
            cluster.replica_node.clone(),
        ],
        vec![
            cluster.users_primary_node.clone(),
            cluster.replica_node.clone(),
        ],
    );
    cluster.shared_kv.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        for shard in &healed {
            control.put_metadata_shard(shard).expect("healed shard");
        }
    });
    let healed_resume_started = Instant::now();
    let report = cluster
        .users_primary
        .posix_resume_namespace_jobs_bounded(8, 512)
        .expect("maintenance resume");
    assert!(
        healed_resume_started.elapsed() < reactive_route_budget,
        "healed live replicas must complete the parked job without an idle-replica timeout: {:?}",
        healed_resume_started.elapsed()
    );
    assert_eq!(
        (report.resumed_job_count, report.completed_job_count),
        (1, 1),
        "the parked cross-shard rename must resume and complete: {report:?}"
    );
    stop.store(true, Ordering::SeqCst);
    cluster.services_primary = services_thread.join().expect("services thread");
    cluster.replica = replica_thread.join().expect("replica thread");

    // The move completed: old path gone, new path adopted, lock released.
    assert!(matches!(
        cluster.users_primary.posix_stat("/users/stuck.txt"),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert_eq!(
        cluster
            .users_primary
            .posix_stat("/services/stuck-moved.txt")
            .expect("moved stat")
            .content_hash
            .as_deref(),
        Some(content_hash(&bytes).as_str())
    );
    cluster
        .users_primary
        .posix_write_file(
            "/users/stuck.txt",
            b"fresh file at the freed path",
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
                replica_candidates: vec![cluster.users_primary_node.clone()],
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .expect("the freed source path accepts new files after recovery");

    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn sifs_cluster_posix_cross_shard_rename_retry_resumes_existing_job() {
    let mut cluster = sifs_two_primary_cluster("sifs-xshard-retry");
    let ghost = NodeId::from_legacy_named("node_sifs_xshard_retry_ghost").expect("ghost node");
    let bytes = b"rename retry resumes the same durable saga".to_vec();
    cluster
        .users_primary
        .posix_write_file(
            "/users/retry.txt",
            &bytes,
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
                replica_candidates: vec![cluster.users_primary_node.clone()],
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .expect("primary local create");

    let to_ghost = sifs_two_primary_shard_configs(
        &cluster.users_primary_node,
        &ghost,
        3,
        vec![
            cluster.services_primary_node.clone(),
            cluster.replica_node.clone(),
        ],
        vec![
            cluster.users_primary_node.clone(),
            cluster.replica_node.clone(),
        ],
    );
    cluster.shared_kv.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        for shard in &to_ghost {
            control.put_metadata_shard(shard).expect("ghost shard");
        }
    });
    let error = cluster
        .users_primary
        .posix_rename("/users/retry.txt", "/services/retry-moved.txt")
        .expect_err("first rename parks while destination primary is unreachable");
    assert!(
        matches!(error, DfsPosixError::Dfs(DfsError::Network { .. })),
        "expected a transport error, got {error:?}"
    );

    let healed = sifs_two_primary_shard_configs(
        &cluster.users_primary_node,
        &cluster.services_primary_node,
        4,
        vec![
            cluster.services_primary_node.clone(),
            cluster.replica_node.clone(),
        ],
        vec![
            cluster.users_primary_node.clone(),
            cluster.replica_node.clone(),
        ],
    );
    cluster.shared_kv.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        for shard in &healed {
            control.put_metadata_shard(shard).expect("healed shard");
        }
    });
    let services_primary = std::mem::replace(
        &mut cluster.services_primary,
        NetworkedDfsService::new(
            sinomo_core::network::InProcessNetwork::new()
                .add_node(NodeId::from_legacy_named("node_unused_pump_retry").expect("node")),
            LocalContentStore::new(cluster.root.join("unused-content")).expect("content"),
        ),
    );
    let stop = Arc::new(AtomicBool::new(false));
    let services_stop = Arc::clone(&stop);
    let services_thread =
        std::thread::spawn(move || pump_sifs_service_until_stop(services_primary, services_stop));

    cluster
        .users_primary
        .execute_namespace_operation_local(&DfsNamespaceOperation::Rename {
            from: "/users/retry.txt".to_string(),
            to: "/services/retry-moved.txt".to_string(),
        })
        .expect("routed retry resumes existing cross-shard rename job");
    assert!(matches!(
        cluster.users_primary.posix_stat("/users/retry.txt"),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert_eq!(
        cluster
            .users_primary
            .posix_read_file("/services/retry-moved.txt")
            .expect("read moved"),
        bytes
    );

    stop.store(true, Ordering::SeqCst);
    cluster.services_primary = services_thread.join().expect("services thread");
    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn sifs_cluster_posix_cross_shard_rename_pending_replay_reconciles_completed_job() {
    let mut cluster = sifs_two_primary_cluster("sifs-xshard-pending-replay");
    let ghost =
        NodeId::from_legacy_named("node_sifs_xshard_pending_replay_ghost").expect("ghost node");
    let source = "/users/pending-replay.txt";
    let destination = "/services/pending-replay-moved.txt";
    let bytes = b"pending durable rename reconciles its completed saga".to_vec();
    cluster
        .users_primary
        .posix_write_file(
            source,
            &bytes,
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
                replica_candidates: vec![cluster.users_primary_node.clone()],
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .expect("primary local create");

    let to_ghost = sifs_two_primary_shard_configs(
        &cluster.users_primary_node,
        &ghost,
        3,
        vec![
            cluster.services_primary_node.clone(),
            cluster.replica_node.clone(),
        ],
        vec![
            cluster.users_primary_node.clone(),
            cluster.replica_node.clone(),
        ],
    );
    cluster.shared_kv.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        for shard in &to_ghost {
            control.put_metadata_shard(shard).expect("ghost shard");
        }
    });

    let request_id = "dfs_net_req_xshard_pending_replay".to_string();
    let command = DfsNamespaceCommand {
        request_id: request_id.clone(),
        file_system_id: sifs_file_system_id(),
        requesting_node_id: cluster.replica_node.clone(),
        include_metadata_journal_batches: false,
        metadata_journal_after_sequences: Vec::new(),
        authority_shards: Vec::new(),
        operation: DfsNamespaceOperation::Rename {
            from: source.to_string(),
            to: destination.to_string(),
        },
    };
    let first = cluster
        .users_primary
        .handle_namespace_request(cluster.replica_node.clone(), command.clone())
        .expect("serve rename while destination primary is unavailable");
    assert!(matches!(
        first,
        NetworkedDfsServiceEvent::RemoteNamespaceServed {
            result: DfsNamespaceResult::Rejected {
                posix_error: None,
                ref reason,
                ..
            },
            ..
        } if reason.contains("network")
    ));
    let pending_request = cluster
        .users_primary
        .metadata_backend
        .as_ref()
        .expect("metadata backend")
        .get_namespace_request(&cluster.replica_node, &request_id)
        .expect("durable namespace request lookup")
        .expect("pending durable namespace request");
    assert_eq!(
        pending_request.status,
        DfsNamespaceRequestStatus::Pending,
        "a retryable cross-shard transport failure must preserve the durable admission"
    );
    let parked_job = cluster
        .users_primary
        .metadata_backend
        .as_ref()
        .expect("metadata backend")
        .list_namespace_jobs_page(None, 16)
        .expect("namespace jobs")
        .jobs
        .into_iter()
        .find(|job| {
            job.kind == DfsNamespaceJobKind::RenameObjectAcrossShardPrimaries
                && job.status == DfsNamespaceJobStatus::Running
                && job.root_path == source
                && job.target_path.as_deref() == Some(destination)
        })
        .expect("parked cross-shard rename job");
    assert!(
        parked_job.created_at >= pending_request.created_at,
        "the saga must belong to this durable admission"
    );
    let (moved_object, old_path) = parked_job
        .pending_frames
        .iter()
        .find_map(|frame| match frame {
            DfsNamespaceJobFrame::CrossShardAdopt {
                moved_object,
                old_path,
                ..
            } => Some((moved_object.clone(), old_path.clone())),
            _ => None,
        })
        .expect("cross-shard adoption frame");
    let healed = sifs_two_primary_shard_configs(
        &cluster.users_primary_node,
        &cluster.services_primary_node,
        4,
        vec![
            cluster.services_primary_node.clone(),
            cluster.replica_node.clone(),
        ],
        vec![
            cluster.users_primary_node.clone(),
            cluster.replica_node.clone(),
        ],
    );
    cluster.shared_kv.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        for shard in &healed {
            control.put_metadata_shard(shard).expect("healed shard");
        }
    });
    let services_primary = std::mem::replace(
        &mut cluster.services_primary,
        NetworkedDfsService::new(
            sinomo_core::network::InProcessNetwork::new().add_node(
                NodeId::from_legacy_named("node_unused_pump_pending_replay").expect("node"),
            ),
            LocalContentStore::new(cluster.root.join("unused-content")).expect("content"),
        ),
    );
    let stop = Arc::new(AtomicBool::new(false));
    let services_stop = Arc::clone(&stop);
    let services_thread =
        std::thread::spawn(move || pump_sifs_service_until_stop(services_primary, services_stop));

    let report = cluster
        .users_primary
        .posix_resume_namespace_jobs_bounded(8, 512)
        .expect("maintenance completes parked rename");
    assert_eq!(
        (report.resumed_job_count, report.completed_job_count),
        (1, 1),
        "the parked cross-shard rename must complete independently of the outer response: {report:?}"
    );
    let completed_job = cluster
        .users_primary
        .metadata_backend
        .as_ref()
        .expect("metadata backend")
        .get_namespace_job(&parked_job.job_id)
        .expect("completed namespace job lookup")
        .expect("completed namespace job");
    assert_eq!(
        completed_job.origin,
        Some(DfsNamespaceJobOrigin::from_request(&pending_request)),
        "new cross-shard sagas must retain the exact durable request identity"
    );
    let mut stale_runner_job = parked_job.clone();
    for frame in &mut stale_runner_job.pending_frames {
        if let DfsNamespaceJobFrame::CrossShardAdopt { moved_object, .. } = frame {
            moved_object.object_id =
                DfsObjectId::from_legacy_named("object_stale_completed_saga_runner")
                    .expect("stale object id");
        }
    }
    let terminal_replay = cluster
        .users_primary
        .run_cross_shard_namespace_job(stale_runner_job, 512)
        .expect("stale runner reloads terminal job before executing old frames");
    assert_eq!(terminal_replay, completed_job);
    let gc_report = cluster
        .users_primary
        .metadata_backend
        .as_mut()
        .expect("metadata backend")
        .gc_metadata_history(
            DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 1).with_max_removals(256),
        )
        .expect("run metadata history GC while outer request is pending");
    assert_eq!(
        gc_report.compacted_namespace_job_count, 0,
        "GC must retain a completed job while its owning request is still pending"
    );
    assert!(
        cluster
            .users_primary
            .metadata_backend
            .as_ref()
            .expect("metadata backend")
            .get_namespace_job(&parked_job.job_id)
            .expect("post-GC completed namespace job lookup")
            .is_some(),
        "the completed job remains the durable success witness until the outer request is terminal"
    );

    // A second runner can retain the original frames after the first runner
    // adopted and evicted the source object. It must refresh the destination
    // authority and treat the adoption as the same idempotent saga.
    let mut duplicate_runner = NetworkedDfsService::new(
        cluster.users_primary.transport.clone(),
        LocalContentStore::new(cluster.root.join("duplicate-runner-content")).expect("content"),
    )
    .with_control_plane(cluster.shared_kv.clone())
    .with_metadata_backend(
        open_local_sharded_metadata_backend(
            cluster.root.join("duplicate-runner-metadata"),
            healed.clone(),
        )
        .expect("duplicate runner metadata"),
    )
    .expect("attach duplicate runner metadata");
    duplicate_runner
        .run_cross_shard_adopt(&moved_object, &old_path)
        .expect("stale saga runner reconciles adopted destination object");

    // The outer request is still Pending because maintenance completed the
    // saga without sending its namespace response. An exact replay must use
    // the completed job as the success witness instead of attempting a fresh
    // rename from the now-absent source path.
    let replay = cluster
        .users_primary
        .handle_namespace_request(cluster.replica_node.clone(), command)
        .expect("replay pending durable rename");
    assert!(matches!(
        replay,
        NetworkedDfsServiceEvent::RemoteNamespaceServed {
            result: DfsNamespaceResult::Applied,
            ..
        }
    ));
    let completed_request = cluster
        .users_primary
        .metadata_backend
        .as_ref()
        .expect("metadata backend")
        .get_namespace_request(&cluster.replica_node, &request_id)
        .expect("durable namespace request lookup")
        .expect("completed durable namespace request");
    assert_eq!(
        completed_request.status,
        DfsNamespaceRequestStatus::Completed
    );
    assert_eq!(completed_request.result, Some(DfsNamespaceResult::Applied));
    assert!(matches!(
        cluster.users_primary.posix_stat(source),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert_eq!(
        cluster
            .users_primary
            .posix_read_file(destination)
            .expect("read moved file after exact replay"),
        bytes
    );

    stop.store(true, Ordering::SeqCst);
    cluster.services_primary = services_thread.join().expect("services thread");
    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn sifs_cluster_posix_directory_rename_across_shard_primaries_moves_subtree() {
    let cluster = sifs_two_primary_cluster("sifs-xshard-dir-rename");
    let SifsTwoPrimaryCluster {
        root,
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
        root,
        users_primary_node: NodeId::from_legacy_named("node_unused_users3").expect("node"),
        services_primary_node: NodeId::from_legacy_named("node_unused_services3").expect("node"),
        replica_node,
        shared_kv: SharedDfsControlPlaneKvClient::new(InProcessKvStore::new()),
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
    let listing = |service: &NetworkedDfsService<sinomo_core::network::InProcessTransport>| {
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

    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn sifs_cluster_posix_directory_rename_across_shard_primaries_recovers_with_subtree_lock() {
    let mut cluster = sifs_two_primary_cluster("sifs-xshard-dir-recovery");
    let ghost =
        NodeId::from_legacy_named("node_sifs_xshard_dir_recovery_ghost").expect("ghost node");
    let bytes = b"child frozen during the parked subtree move".to_vec();
    let local_write_options = DfsWriteOptions {
        policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
        replica_candidates: vec![cluster.users_primary_node.clone()],
        wait_for_commit: true,
        foreground_replica_repair: true,
    };

    cluster
        .users_primary
        .posix_mkdir("/users/proj2")
        .expect("mkdir proj2");
    cluster
        .users_primary
        .posix_write_file("/users/proj2/file.txt", &bytes, local_write_options.clone())
        .expect("create child");

    // Park the saga: the services shard points at an unreachable primary.
    let to_ghost = sifs_two_primary_shard_configs(
        &cluster.users_primary_node,
        &ghost,
        3,
        vec![
            cluster.services_primary_node.clone(),
            cluster.replica_node.clone(),
        ],
        vec![
            cluster.users_primary_node.clone(),
            cluster.replica_node.clone(),
        ],
    );
    cluster.shared_kv.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        for shard in &to_ghost {
            control.put_metadata_shard(shard).expect("ghost shard");
        }
    });
    let error = cluster
        .users_primary
        .posix_rename("/users/proj2", "/services/proj2")
        .expect_err("directory rename to unreachable primary must park, not block");
    assert!(
        matches!(error, DfsPosixError::Dfs(DfsError::Network { .. })),
        "expected a transport error, got {error:?}"
    );

    // The subtree is frozen, not just the root: child writes and child
    // namespace mutations are rejected while the saga awaits recovery.
    let child_write = cluster
        .users_primary
        .posix_write_file(
            "/users/proj2/file.txt",
            b"concurrent",
            local_write_options.clone(),
        )
        .expect_err("child writes must be frozen during the subtree move");
    assert!(
        child_write
            .to_string()
            .contains("moving between metadata shards"),
        "unexpected child write error: {child_write}"
    );
    let child_mkdir = cluster
        .users_primary
        .posix_mkdir("/users/proj2/newdir")
        .expect_err("child namespace mutations must be frozen during the subtree move");
    assert!(
        child_mkdir
            .to_string()
            .contains("moving between metadata shards"),
        "unexpected child mkdir error: {child_mkdir}"
    );

    // Heal the topology and resume through maintenance: the whole subtree
    // completes its move.
    let healed = sifs_two_primary_shard_configs(
        &cluster.users_primary_node,
        &cluster.services_primary_node,
        4,
        vec![
            cluster.services_primary_node.clone(),
            cluster.replica_node.clone(),
        ],
        vec![
            cluster.users_primary_node.clone(),
            cluster.replica_node.clone(),
        ],
    );
    cluster.shared_kv.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        for shard in &healed {
            control.put_metadata_shard(shard).expect("healed shard");
        }
    });
    let services_primary = std::mem::replace(
        &mut cluster.services_primary,
        NetworkedDfsService::new(
            sinomo_core::network::InProcessNetwork::new()
                .add_node(NodeId::from_legacy_named("node_unused_pump2").expect("node")),
            LocalContentStore::new(cluster.root.join("unused-content")).expect("content"),
        ),
    );
    let stop = Arc::new(AtomicBool::new(false));
    let services_stop = Arc::clone(&stop);
    let services_thread =
        std::thread::spawn(move || pump_sifs_service_until_stop(services_primary, services_stop));
    let report = cluster
        .users_primary
        .posix_resume_namespace_jobs_bounded(8, 512)
        .expect("maintenance resume");
    assert_eq!(
        (report.resumed_job_count, report.completed_job_count),
        (1, 1),
        "the parked directory move must resume and complete: {report:?}"
    );
    stop.store(true, Ordering::SeqCst);
    cluster.services_primary = services_thread.join().expect("services thread");

    // The whole subtree moved and the lock is gone.
    assert!(matches!(
        cluster.users_primary.posix_stat("/users/proj2"),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert_eq!(
        cluster
            .users_primary
            .posix_read_file("/services/proj2/file.txt")
            .expect("read moved child"),
        bytes
    );
    cluster
        .users_primary
        .posix_mkdir("/users/proj2")
        .expect("the freed source path accepts a fresh directory after recovery");

    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn sifs_cluster_posix_cross_shard_move_fences_open_write_intents() {
    let mut cluster = sifs_two_primary_cluster("sifs-xshard-fence");
    let bytes = b"fenced object content".to_vec();
    let staged = b"write that must settle before the move".to_vec();
    let local_options = DfsWriteOptions {
        policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
        replica_candidates: vec![cluster.users_primary_node.clone()],
        wait_for_commit: true,
        foreground_replica_repair: true,
    };

    cluster
        .users_primary
        .posix_write_file("/users/fenced.txt", &bytes, local_options.clone())
        .expect("create fenced file");

    // Open a write intent that cannot settle yet: a (2,2) write staging to
    // the services primary, which is not pumping.
    cluster
        .users_primary
        .posix_write_file(
            "/users/fenced.txt",
            &staged,
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(2, 2).expect("strict policy"),
                replica_candidates: vec![
                    cluster.users_primary_node.clone(),
                    cluster.services_primary_node.clone(),
                ],
                wait_for_commit: false,
                foreground_replica_repair: true,
            },
        )
        .expect("open staged write");

    // The move must reject up front with the retriable Busy error and leave
    // no parked saga behind.
    let error = cluster
        .users_primary
        .posix_rename("/users/fenced.txt", "/services/fenced-moved.txt")
        .expect_err("move must fence on the open write intent");
    assert!(
        matches!(error, DfsPosixError::Dfs(DfsError::Busy { .. })),
        "expected Busy, got {error:?}"
    );
    let report = cluster
        .users_primary
        .posix_resume_namespace_jobs_bounded(8, 512)
        .expect("maintenance scan");
    assert_eq!(
        (report.resumed_job_count, report.remaining_running_job_count),
        (0, 0),
        "a fenced move must not create a parked saga: {report:?}"
    );

    // Let the staged write settle, then the move succeeds.
    let services_primary = std::mem::replace(
        &mut cluster.services_primary,
        NetworkedDfsService::new(
            sinomo_core::network::InProcessNetwork::new()
                .add_node(NodeId::from_legacy_named("node_unused_pump3").expect("node")),
            LocalContentStore::new(cluster.root.join("unused-content")).expect("content"),
        ),
    );
    let stop = Arc::new(AtomicBool::new(false));
    let services_stop = Arc::clone(&stop);
    let services_thread =
        std::thread::spawn(move || pump_sifs_service_until_stop(services_primary, services_stop));
    let staged_hash = content_hash(&staged);
    let deadline = Instant::now() + SIFS_CLUSTER_PUMP_DEADLINE;
    loop {
        cluster
            .users_primary
            .process_incoming()
            .expect("users primary pump");
        if cluster
            .users_primary
            .posix_stat("/users/fenced.txt")
            .is_ok_and(|stat| stat.content_hash.as_deref() == Some(staged_hash.as_str()))
        {
            break;
        }
        if Instant::now() >= deadline {
            panic!("staged write did not settle");
        }
        std::thread::sleep(SIFS_CLUSTER_PUMP_IDLE_SLEEP);
    }
    cluster
        .users_primary
        .posix_rename("/users/fenced.txt", "/services/fenced-moved.txt")
        .expect("move succeeds once the write settled");
    assert_eq!(
        cluster
            .users_primary
            .posix_read_file("/services/fenced-moved.txt")
            .expect("read moved"),
        staged
    );
    stop.store(true, Ordering::SeqCst);
    cluster.services_primary = services_thread.join().expect("services thread");

    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn sifs_cluster_posix_cross_shard_move_allows_committed_pending_repair() {
    let mut cluster = sifs_two_primary_cluster("sifs-xshard-repair-pending");
    let bytes = b"committed while desired replica repair is pending".to_vec();

    // Commit with min=1, desired=2 while the destination primary is not
    // pumping. The write is visible and safe to move, but the second replica
    // remains a background repair target.
    let source_stat = cluster
        .users_primary
        .posix_write_file(
            "/users/repair-pending.txt",
            &bytes,
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(1, 2).expect("policy"),
                replica_candidates: vec![
                    cluster.users_primary_node.clone(),
                    cluster.services_primary_node.clone(),
                ],
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .expect("min-replica write commits");
    assert!(
        cluster
            .users_primary
            .coordinator
            .repair_jobs()
            .iter()
            .any(|job| job.object_id == source_stat.object_id),
        "test setup must leave committed repair work pending"
    );

    let services_primary = std::mem::replace(
        &mut cluster.services_primary,
        NetworkedDfsService::new(
            sinomo_core::network::InProcessNetwork::new()
                .add_node(NodeId::from_legacy_named("node_unused_repair_pending").expect("node")),
            LocalContentStore::new(cluster.root.join("unused-content")).expect("content"),
        ),
    );
    let stop = Arc::new(AtomicBool::new(false));
    let services_stop = Arc::clone(&stop);
    let services_thread =
        std::thread::spawn(move || pump_sifs_service_until_stop(services_primary, services_stop));

    let moved = cluster
        .users_primary
        .posix_rename(
            "/users/repair-pending.txt",
            "/services/repair-pending-moved.txt",
        )
        .expect("committed repair-pending object moves across shards");
    assert_eq!(moved.path, "/services/repair-pending-moved.txt");
    assert_eq!(
        moved.content_hash.as_deref(),
        Some(content_hash(&bytes).as_str())
    );
    cluster
        .users_primary
        .mark_repaired_location(
            &source_stat.object_id,
            &source_stat.version,
            &cluster.services_primary_node,
            content_hash(&bytes).as_str(),
        )
        .expect("late pre-move repair completion is safely ignored");
    assert!(matches!(
        cluster
            .users_primary
            .posix_stat("/users/repair-pending.txt"),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert_eq!(
        cluster
            .users_primary
            .posix_read_file("/services/repair-pending-moved.txt")
            .expect("read moved object"),
        bytes
    );

    stop.store(true, Ordering::SeqCst);
    cluster.services_primary = services_thread.join().expect("services thread");
    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn sifs_cluster_posix_rejects_adoption_from_non_source_primary() {
    let mut cluster = sifs_two_primary_cluster("sifs-xshard-rogue");
    let mut rogue_object = file_metadata(cluster.replica_node.clone());
    rogue_object.object_id = DfsObjectId::from_legacy_named("obj_rogue_adopt").expect("object");
    rogue_object.file_system_id = sifs_file_system_id();
    rogue_object.path = "/services/rogue.txt".to_string();
    rogue_object.policy = DfsObjectPolicy::ReplicatedEventual;

    // The replica is not the primary of the claimed source shard (/users is
    // owned by the users primary), so the services primary must reject the
    // injected adoption outright.
    let event = cluster
        .services_primary
        .handle_namespace_request(
            cluster.replica_node.clone(),
            DfsNamespaceCommand {
                request_id: "rogue-adopt-1".to_string(),
                file_system_id: sifs_file_system_id(),
                requesting_node_id: cluster.replica_node.clone(),
                include_metadata_journal_batches: true,
                metadata_journal_after_sequences: Vec::new(),
                authority_shards: Vec::new(),
                operation: DfsNamespaceOperation::AdoptObjectAtPath {
                    content_reference_outcomes: Vec::new(),
                    content_reference_outcome_retention: None,
                    object: rogue_object,
                    source_path: "/users/rogue-src.txt".to_string(),
                    pin_transfer_id: None,
                    versions: Vec::new(),
                    version_pins: Vec::new(),
                    version_pin_set_fingerprint: None,
                    receipts: Vec::new(),
                    intents: Vec::new(),
                    intent_receipts: Vec::new(),
                    intent_receipt_retention: DfsWriteIntentReceiptRetentionState::default(),
                    conflicts: Vec::new(),
                    advisory_lock_state: None,
                },
            },
        )
        .expect("handler must answer");
    let NetworkedDfsServiceEvent::RemoteNamespaceServed { result, .. } = event else {
        panic!("expected a served event, got {event:?}");
    };
    let DfsNamespaceResult::Rejected { reason, .. } = result else {
        panic!("rogue adoption must be rejected, got {result:?}");
    };
    assert!(
        reason.contains("is not the primary of source shard"),
        "unexpected rejection reason: {reason}"
    );
    assert!(matches!(
        cluster.services_primary.posix_stat("/services/rogue.txt"),
        Err(DfsPosixError::NotFound { .. })
    ));

    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn sifs_cluster_posix_cross_shard_resume_honors_step_budget() {
    let mut cluster = sifs_two_primary_cluster("sifs-xshard-budget");
    let ghost = NodeId::from_legacy_named("node_sifs_xshard_budget_ghost").expect("ghost");
    let local_options = DfsWriteOptions {
        policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
        replica_candidates: vec![cluster.users_primary_node.clone()],
        wait_for_commit: true,
        foreground_replica_repair: true,
    };
    cluster
        .users_primary
        .posix_mkdir("/users/budget-dir")
        .expect("mkdir");
    for index in 0..3 {
        cluster
            .users_primary
            .posix_write_file(
                &format!("/users/budget-dir/file-{index}.txt"),
                format!("budget {index}").as_bytes(),
                local_options.clone(),
            )
            .expect("create child");
    }

    // Park the subtree move against an unreachable destination primary.
    let to_ghost = sifs_two_primary_shard_configs(
        &cluster.users_primary_node,
        &ghost,
        5,
        vec![
            cluster.services_primary_node.clone(),
            cluster.replica_node.clone(),
        ],
        vec![
            cluster.users_primary_node.clone(),
            cluster.replica_node.clone(),
        ],
    );
    cluster.shared_kv.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        for shard in &to_ghost {
            control.put_metadata_shard(shard).expect("ghost shard");
        }
    });
    cluster
        .users_primary
        .posix_rename("/users/budget-dir", "/services/budget-dir")
        .expect_err("park against the ghost primary");

    // Heal and resume with a one-step budget: completion must take multiple
    // bounded rounds instead of one unbounded run.
    let healed = sifs_two_primary_shard_configs(
        &cluster.users_primary_node,
        &cluster.services_primary_node,
        6,
        vec![
            cluster.services_primary_node.clone(),
            cluster.replica_node.clone(),
        ],
        vec![
            cluster.users_primary_node.clone(),
            cluster.replica_node.clone(),
        ],
    );
    cluster.shared_kv.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        for shard in &healed {
            control.put_metadata_shard(shard).expect("healed shard");
        }
    });
    let services_primary = std::mem::replace(
        &mut cluster.services_primary,
        NetworkedDfsService::new(
            sinomo_core::network::InProcessNetwork::new()
                .add_node(NodeId::from_legacy_named("node_unused_pump4").expect("node")),
            LocalContentStore::new(cluster.root.join("unused-content")).expect("content"),
        ),
    );
    let stop = Arc::new(AtomicBool::new(false));
    let services_stop = Arc::clone(&stop);
    let services_thread =
        std::thread::spawn(move || pump_sifs_service_until_stop(services_primary, services_stop));

    let first = cluster
        .users_primary
        .posix_resume_namespace_jobs_bounded(8, 1)
        .expect("budgeted resume");
    assert_eq!(
        (first.resumed_job_count, first.remaining_running_job_count),
        (1, 1),
        "a one-step budget must leave the saga running: {first:?}"
    );
    let mut rounds = 1usize;
    loop {
        let report = cluster
            .users_primary
            .posix_resume_namespace_jobs_bounded(8, 1)
            .expect("budgeted resume round");
        rounds += 1;
        if report.completed_job_count == 1 {
            break;
        }
        assert!(rounds < 64, "budgeted resume did not converge");
    }
    assert!(
        rounds >= 3,
        "three children plus root must take several one-step rounds"
    );
    stop.store(true, Ordering::SeqCst);
    cluster.services_primary = services_thread.join().expect("services thread");

    assert!(matches!(
        cluster.users_primary.posix_stat("/users/budget-dir"),
        Err(DfsPosixError::NotFound { .. })
    ));
    for index in 0..3 {
        assert!(
            cluster
                .users_primary
                .posix_stat(&format!("/services/budget-dir/file-{index}.txt"))
                .is_ok(),
            "child {index} must have moved"
        );
    }

    std::fs::remove_dir_all(&cluster.root).ok();
}
