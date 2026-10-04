//! Requester receipts must name the canonical durable object/version binding.

use super::*;

mod active_inline;
mod fetch_reconstruction;

type PrestageService = NetworkedDfsService<sinomo_core::network::InProcessTransport>;

struct PrestageRecordingTransport {
    inner: sinomo_core::network::InProcessTransport,
    sent: Arc<Mutex<Vec<DfsNetworkMessage>>>,
}

impl DfsTransportIdentity for PrestageRecordingTransport {
    fn local_node_id(&self) -> &NodeId {
        self.inner.local_node_id()
    }
}

impl DfsMessageTransport for PrestageRecordingTransport {
    fn send_routed_call(
        &mut self,
        target: &NodeId,
        call: DfsRoutedCall,
    ) -> Result<(), NetworkError> {
        self.sent.lock().unwrap().push(call.message.message.clone());
        self.inner.send_routed_call(target, call)
    }
    fn drain_incoming_messages(&mut self) -> Result<Vec<DfsRoutedMessage>, NetworkError> {
        self.inner.drain_incoming_messages()
    }
    fn can_send_synchronously(&self) -> bool {
        self.inner.can_send_synchronously()
    }
}

struct PrestageFixture {
    network: InProcessNetwork,
    primary: NetworkedDfsService<PrestageRecordingTransport>,
    primary_sent: Arc<Mutex<Vec<DfsNetworkMessage>>>,
    requester: PrestageService,
    primary_id: NodeId,
    requester_id: NodeId,
    current: DfsObjectMetadata,
    requester_content: PathBuf,
}

fn prestage_fixture(root: &Path) -> PrestageFixture {
    prestage_fixture_with_requester_content(root, root.join("requester-content"))
}

fn prestage_fixture_with_requester_content(
    root: &Path,
    requester_content: PathBuf,
) -> PrestageFixture {
    let primary_id = NodeId::from_legacy_named("prestage_primary").unwrap();
    let requester_id = NodeId::from_legacy_named("prestage_requester").unwrap();
    let file_system = file_system(primary_id.clone());
    let shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "root".to_string(),
        primary_node_id: primary_id.clone(),
        // Isolate the content receipt boundary: the metadata shard has one
        // primary, while the desired content replica is the requester.
        replica_node_ids: Vec::new(),
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let control_plane = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    control_plane.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(&file_system))
            .unwrap();
        control.put_metadata_shard(&shard).unwrap();
    });
    let mut current = file_metadata(primary_id.clone());
    current.path = "/canonical.bin".to_string();
    current.version = DfsVersion::from_legacy_named("prestage_current_version").unwrap();
    current.policy = DfsObjectPolicy::ReplicatedEventual;
    current.current_locations = vec![requester_id.clone()];
    current.content_hash = Some(content_hash(b"current canonical content"));
    current.content_size_bytes = Some(b"current canonical content".len() as u64);
    let network = InProcessNetwork::new();
    let primary_metadata = seeded_durable_metadata_replica(
        root.join("primary-metadata"),
        vec![shard.clone()],
        &file_system,
        &current,
    );
    let mut requester_metadata =
        open_local_sharded_metadata_backend(root.join("requester-metadata"), vec![shard])
            .expect("requester metadata replica");
    let root_shard = DfsMetadataShardId::new("root");
    let snapshot = primary_metadata
        .export_shard_table_snapshot(&root_shard)
        .expect("primary initial snapshot")
        .expect("primary root shard");
    requester_metadata
        .replace_with_replicated_snapshot(
            root_shard,
            snapshot
                .materialized_through_sequence
                .expect("exact initial prefix"),
            snapshot,
        )
        .expect("seed requester with the identical authoritative journal prefix");
    let content = LocalContentStore::new(&requester_content).unwrap();
    content
        .put_object_version(
            current.object_id.as_str(),
            current.version.as_str(),
            b"current canonical content",
        )
        .unwrap();
    let primary_sent = Arc::new(Mutex::new(Vec::new()));
    let primary = NetworkedDfsService::new(
        PrestageRecordingTransport {
            inner: network.add_node(primary_id.clone()),
            sent: Arc::clone(&primary_sent),
        },
        LocalContentStore::new(root.join("primary-content")).unwrap(),
    )
    .with_control_plane(control_plane.clone())
    .with_metadata_backend(primary_metadata)
    .unwrap();
    let requester = NetworkedDfsService::new(network.add_node(requester_id.clone()), content)
        .with_control_plane(control_plane)
        .with_metadata_backend(requester_metadata)
        .unwrap();
    PrestageFixture {
        network,
        primary,
        primary_sent,
        requester,
        primary_id,
        requester_id,
        current,
        requester_content,
    }
}

fn assert_durable_binding(root: &Path, object: &DfsObjectId, version: &DfsVersion, bytes: &[u8]) {
    // A fresh store discards all service-local publication state. This is
    // deliberately checked before delivering the committed routed response.
    let reopened = LocalContentStore::new(root).expect("reopen requester content");
    let (manifest, stored) = reopened
        .read_object_version(object.as_str(), version.as_str())
        .expect("canonical tuple must already have a durable manifest");
    assert_eq!(stored, bytes);
    assert_eq!(manifest.content_hash, content_hash(bytes));
    assert!(
        reopened
            .content_reservation_roots()
            .expect("durable content reservations")
            .reservations
            .iter()
            .any(|reservation| {
                reservation.identity.object_id == object.as_str()
                    && reservation.identity.version == version.as_str()
                    && reservation.identity.content_hash == manifest.content_hash
            }),
        "the canonical binding must remain rooted before catalog/reply reconciliation"
    );
}

fn rebased_requester_prestage_requires_canonical_stage(hardlink: bool) {
    let root = temp_root(if hardlink {
        "requester-prestage-hardlink"
    } else {
        "requester-prestage-version"
    });
    let mut fixture = prestage_fixture(root.path());
    let bytes = b"new bytes staged before the authority selected its final tuple";
    let mut proposed = fixture.current.clone();
    proposed.version = DfsVersion::from_legacy_named("prestage_stale_base").unwrap();
    if hardlink {
        let mut alias = fixture.current.clone();
        alias.object_id = DfsObjectId::from_legacy_named("prestage_alias").unwrap();
        alias.kind = DfsObjectKind::Hardlink;
        alias.path = "/alias.bin".to_string();
        alias.link_target_object_id = Some(fixture.current.object_id.clone());
        fixture.primary.add_object_metadata(alias).unwrap();
        // The requester has not observed this alias yet. Its canonical target
        // metadata is already seeded, which authorizes the later fetch/stage;
        // replica setup must not author a namespace mutation on this node.
        proposed.object_id =
            DfsObjectId::from_legacy_named("prestage_blind_provisional_inode").unwrap();
        proposed.path = "/alias.bin".to_string();
        proposed.content_hash = None;
        proposed.content_size_bytes = None;
    }
    let staged_version = DfsVersion::from_legacy_named("prestage_proposed_target").unwrap();
    fixture
        .requester
        .content
        .put_object_version(proposed.object_id.as_str(), staged_version.as_str(), bytes)
        .expect("durable original requester tuple");
    let mut policy = DfsReplicationPolicy::new(1, 1).unwrap();
    policy.required_node_ids.push(fixture.requester_id.clone());
    // Normalization otherwise adds the content owner (the primary), allowing
    // its canonical fetched copy to satisfy W before requester repair. This
    // fixture deliberately requires the requester's canonical durable copy.
    policy.forbidden_node_ids.push(fixture.primary_id.clone());
    let command = DfsDistributedWriteCommand {
        publication: Default::default(),
        request_id: if hardlink {
            "prestage-hardlink-rebase"
        } else {
            "prestage-version-rebase"
        }
        .to_string(),
        request: DfsWriteRequest {
            file_system_id: proposed.file_system_id.clone(),
            object_id: proposed.object_id.clone(),
            target_version: staged_version.clone(),
            requesting_node_id: fixture.requester_id.clone(),
        },
        expected_base_version: Some(proposed.version.clone()),
        metadata_journal_after_sequence: None,
        bytes: Vec::new(),
        content_hash: Some(content_hash(bytes)),
        content_size_bytes: Some(bytes.len() as u64),
        requester_blob_pre_staged: true,
        desired_replica_nodes: vec![fixture.requester_id.clone()],
        policy,
        object_metadata: Some(proposed),
        authority_shard: None,
        touch_parent_on_create: false,
        foreground_replica_repair: false,
        exclusive_create: false,
    };
    let event = fixture
        .primary
        .handle_distributed_write_request(fixture.requester_id.clone(), command)
        .expect("authority handles stale captured requester command");
    let NetworkedDfsServiceEvent::RemoteWriteServed {
        result:
            DfsDistributedWriteResult::Outcome {
                outcome:
                    NetworkedDfsWriteOutcome::Pending {
                        intent_id,
                        durable_receipts,
                        required_receipts,
                    },
            },
        ..
    } = event
    else {
        panic!("rebased requester tuple must not satisfy W before canonical staging: {event:?}");
    };
    assert_eq!(durable_receipts, 0);
    assert_eq!(required_receipts, 1);
    let intent = fixture
        .primary
        .coordinator
        .intent(&intent_id)
        .expect("pending canonical intent")
        .clone();
    assert_eq!(intent.object_id, fixture.current.object_id);
    assert_eq!(
        intent.desired_replica_nodes,
        vec![fixture.requester_id.clone()]
    );
    assert_ne!(intent.target_version, staged_version);
    assert!(
        fixture
            .requester
            .content
            .read_manifest(intent.object_id.as_str(), intent.target_version.as_str())
            .is_err()
    );
    assert_eq!(
        fixture
            .primary
            .metadata
            .object(&fixture.current.object_id)
            .unwrap()
            .version,
        fixture.current.version,
        "W=0 must not publish the canonical new version"
    );

    let mut canonical_bindings = 0;
    let mut fetches = 0;
    let mut committed = false;
    let mut dropped_committed_replies = 0;
    for _ in 0..16 {
        let messages = fixture
            .requester
            .transport
            .drain_incoming_messages()
            .unwrap();
        for message in messages {
            match &message.message {
                DfsNetworkMessage::DistributedWriteResponse(response) => {
                    if matches!(
                        response.result,
                        DfsDistributedWriteResult::Outcome {
                            outcome: NetworkedDfsWriteOutcome::Committed { .. }
                        }
                    ) {
                        dropped_committed_replies += 1;
                    }
                    continue; // Simulate lost replies; never run requester post-reply publication.
                }
                DfsNetworkMessage::FetchBlob(_) => fetches += 1,
                DfsNetworkMessage::StageBlob(_) | DfsNetworkMessage::RepairReplica(_) => {
                    canonical_bindings += 1;
                    let (object_id, version) = match &message.message {
                        DfsNetworkMessage::StageBlob(stage) => (&stage.object_id, &stage.version),
                        DfsNetworkMessage::RepairReplica(repair) => {
                            (&repair.object_id, &repair.version)
                        }
                        _ => unreachable!("canonical binding message"),
                    };
                    assert_eq!(object_id, &intent.object_id);
                    assert_eq!(version, &intent.target_version);
                    assert!(
                        !matches!(
                            fixture.primary.write_outcome(&intent_id).unwrap(),
                            NetworkedDfsWriteOutcome::Committed { .. }
                        ),
                        "canonical staging must precede W=1 visibility"
                    );
                }
                _ => {}
            }
            let binds_canonical = matches!(
                message.message,
                DfsNetworkMessage::StageBlob(_) | DfsNetworkMessage::RepairReplica(_)
            );
            fixture
                .requester
                .process_routed_messages_with_options(vec![message], false)
                .unwrap();
            if binds_canonical {
                assert_durable_binding(
                    &fixture.requester_content,
                    &intent.object_id,
                    &intent.target_version,
                    bytes,
                );
            }
        }
        fixture
            .primary
            .process_incoming_without_deferred_parent_touches()
            .unwrap();
        // The daemon turns BlobFetched into retained repair work. Drive that
        // follow-up explicitly rather than expecting the inbound pump itself
        // to stage a requester that is still missing its canonical binding.
        fixture.primary.retry_pending_repairs().unwrap();
        if matches!(
            fixture.primary.write_outcome(&intent_id).unwrap(),
            NetworkedDfsWriteOutcome::Committed { .. }
        ) {
            committed = true;
            fixture
                .primary
                .flush_completed_pending_remote_write_responses(&mut Vec::new())
                .unwrap();
            for message in fixture
                .requester
                .transport
                .drain_incoming_messages()
                .unwrap()
            {
                if matches!(
                    message.message,
                    DfsNetworkMessage::DistributedWriteResponse(DfsDistributedWriteResponse {
                        result: DfsDistributedWriteResult::Outcome {
                            outcome: NetworkedDfsWriteOutcome::Committed { .. }
                        },
                        ..
                    })
                ) {
                    dropped_committed_replies += 1;
                }
            }
            break;
        }
    }
    assert!(committed, "canonical stage receipt must complete the write");
    assert!(
        fetches > 0,
        "rebased binding fetches the durable requester hash"
    );
    assert!(
        canonical_bindings > 0,
        "rebased binding needs a canonical replica stage or repair"
    );
    let receipts = fixture.primary.coordinator.receipts_for_intent(&intent_id);
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].replica_node_id, fixture.requester_id);
    assert!(
        dropped_committed_replies > 0,
        "test must exercise a lost committed reply"
    );
    drop(fixture.requester); // Requester crash before its committed-reply publisher.
    assert_durable_binding(
        &fixture.requester_content,
        &intent.object_id,
        &intent.target_version,
        bytes,
    );
}

#[test]
fn requester_prestage_version_rebase_waits_for_canonical_binding_before_lost_reply() {
    rebased_requester_prestage_requires_canonical_stage(false);
}

#[test]
fn requester_prestage_hardlink_retarget_waits_for_canonical_binding_before_lost_reply() {
    rebased_requester_prestage_requires_canonical_stage(true);
}

#[test]
fn requester_prestage_byte_create_uses_manifest_without_primary_binding_or_stage_back() {
    let root = temp_root("requester-prestage-byte-create");
    let mut fixture = prestage_fixture(root.path());
    let payload = vec![0x52; 4096];
    let bytes = payload.as_slice();
    let observed_prepare = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let observed_from_hook = Arc::clone(&observed_prepare);
    // The primary is processed on this test thread; requester replay runs on
    // its separate thread and cannot contribute to this thread-local counter.
    crate::service_commit::BEFORE_CREATE_PREPARE.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move |prepare| {
            assert_eq!(prepare.transaction.objects.len(), 2);
            assert_eq!(prepare.transaction.versions.len(), 2);
            crate::metadata_journal::reset_journal_append_sync_calls_for_test();
            observed_from_hook.store(true, std::sync::atomic::Ordering::Release);
        }));
    });
    let mut policy = DfsReplicationPolicy::new(1, 1).unwrap();
    policy.required_node_ids.push(fixture.requester_id.clone());
    let candidates = vec![fixture.requester_id.clone(), fixture.primary_id.clone()];
    let mut changed_retry = std::thread::scope(|scope| {
        let requester = &mut fixture.requester;
        let writer = scope.spawn(move || {
            requester.posix_write_file(
                "/new-prestaged.bin",
                bytes,
                DfsWriteOptions {
                    policy,
                    replica_candidates: candidates,
                    wait_for_commit: true,
                    foreground_replica_repair: false,
                },
            )
        });
        let mut captured = Vec::new();
        let mut committed_response = false;
        let deadline = Instant::now() + Duration::from_secs(5);
        while !writer.is_finished() {
            let messages = fixture.primary.transport.drain_incoming_messages().unwrap();
            for message in &messages {
                if let DfsNetworkMessage::DistributedWriteRequest(command) = &message.message {
                    assert!(command.requester_blob_pre_staged);
                    assert!(
                        command.bytes.is_empty(),
                        "wait-for-commit create is manifest-only"
                    );
                    assert_eq!(
                        command.content_hash.as_deref(),
                        Some(content_hash(bytes).as_str())
                    );
                    assert_eq!(command.content_size_bytes, Some(bytes.len() as u64));
                    assert!(
                        command
                            .desired_replica_nodes
                            .contains(&fixture.requester_id)
                    );
                    assert_durable_binding(
                        &fixture.requester_content,
                        &command.request.object_id,
                        &command.request.target_version,
                        bytes,
                    );
                    captured.push(command.clone());
                }
            }
            let events = fixture
                .primary
                .process_routed_messages_with_options(messages, false)
                .unwrap();
            committed_response |= events.iter().any(|event| {
                matches!(
                    event,
                    NetworkedDfsServiceEvent::RemoteWriteServed {
                        result: DfsDistributedWriteResult::Outcome {
                            outcome: NetworkedDfsWriteOutcome::Committed { .. }
                        },
                        ..
                    }
                )
            });
            assert!(
                Instant::now() < deadline,
                "waiting create must reach its committed response"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        let stat = writer
            .join()
            .expect("requester write thread")
            .expect("wait-for-commit create completes");
        assert_eq!(
            stat.content_hash.as_deref(),
            Some(content_hash(bytes).as_str())
        );
        assert!(
            committed_response,
            "the exact requester binding must satisfy W=1 directly"
        );
        assert_eq!(captured.len(), 1, "healthy create sends one routed write");
        captured.pop().unwrap()
    });
    assert!(observed_prepare.load(std::sync::atomic::Ordering::Acquire));
    assert_eq!(
        crate::metadata_journal::journal_append_sync_calls_for_test(),
        2,
        "AwaitCommit requester-prestaged4KiB create must use grouped I/R/P and C/T"
    );
    assert!(
        !fixture
            .primary
            .content
            .has_blob(&content_hash(bytes))
            .unwrap(),
        "metadata primary must not acquire an unnecessary content binding"
    );
    changed_retry.content_hash = Some(content_hash(b"different stable retry payload"));
    changed_retry.content_size_bytes = Some(b"different stable retry payload".len() as u64);
    let changed = fixture
        .primary
        .handle_distributed_write_request(fixture.requester_id.clone(), changed_retry)
        .expect("changed payload retry produces an explicit rejection");
    assert!(
        matches!(
            changed,
            NetworkedDfsServiceEvent::RemoteWriteServed {
                result: DfsDistributedWriteResult::Rejected { .. },
                ..
            }
        ),
        "stable operation identity must not accept a changed prestaged payload: {changed:?}"
    );
    for message in fixture.primary_sent.lock().unwrap().iter() {
        assert!(
            !matches!(
                message,
                DfsNetworkMessage::StageBlob(_) | DfsNetworkMessage::FetchBlob(_)
            ),
            "an unchanged durable requester tuple needs no content round trip"
        );
    }
}

#[test]
fn requester_prestage_exclusive_create_cannot_replace_existing_path() {
    let root = temp_root("requester-prestage-exclusive-collision");
    let mut fixture = prestage_fixture(root.path());
    let bytes = b"losing exclusive create payload";
    let mut proposed = fixture.current.clone();
    proposed.object_id = DfsObjectId::from_legacy_named("prestage_exclusive_candidate").unwrap();
    proposed.content_hash = None;
    proposed.content_size_bytes = None;
    let target = DfsVersion::from_legacy_named("prestage_exclusive_target").unwrap();
    fixture
        .requester
        .content
        .put_object_version(proposed.object_id.as_str(), target.as_str(), bytes)
        .expect("stage losing exclusive-create tuple");
    let operation = "prestage-exclusive-collision";
    let intent_id = crate::intent::stable_write_intent_id(
        &fixture.requester_id,
        &proposed.object_id,
        operation,
    );
    let command = DfsDistributedWriteCommand {
        publication: Default::default(),
        request_id: operation.to_string(),
        request: DfsWriteRequest {
            file_system_id: proposed.file_system_id.clone(),
            object_id: proposed.object_id.clone(),
            target_version: target,
            requesting_node_id: fixture.requester_id.clone(),
        },
        expected_base_version: Some(proposed.version.clone()),
        metadata_journal_after_sequence: None,
        bytes: Vec::new(),
        content_hash: Some(content_hash(bytes)),
        content_size_bytes: Some(bytes.len() as u64),
        requester_blob_pre_staged: true,
        desired_replica_nodes: vec![fixture.requester_id.clone()],
        policy: DfsReplicationPolicy::new(1, 1).unwrap(),
        object_metadata: Some(proposed),
        authority_shard: None,
        touch_parent_on_create: true,
        foreground_replica_repair: false,
        exclusive_create: true,
    };
    let message = build_dfs_routed_message(
        &fixture.requester_id,
        &fixture.primary_id,
        1,
        1,
        DfsNetworkMessage::ExclusiveCreateRequest(command),
    );
    let events = fixture
        .primary
        .process_routed_messages_with_options(vec![message], false)
        .expect("exclusive collision is a typed reply");
    assert!(
        events.iter().any(|event| matches!(
            event,
            NetworkedDfsServiceEvent::RemoteWriteServed {
                result: DfsDistributedWriteResult::Rejected { .. },
                ..
            }
        )),
        "existing canonical path must reject the prestaged create: {events:?}"
    );
    assert!(fixture.primary.coordinator.intent(&intent_id).is_none());
    assert_eq!(
        fixture
            .primary
            .coordinator
            .durable_receipt_count(&intent_id),
        0
    );
    assert_eq!(
        fixture
            .primary
            .current_object_for_write(&fixture.current.object_id)
            .unwrap(),
        fixture.current,
        "a losing create cannot replace or advance the existing canonical object"
    );
}

#[test]
fn requester_prestage_excluded_from_final_plan_is_only_a_transfer_source() {
    let root = temp_root("requester-prestage-final-plan-excludes-requester");
    let mut fixture = prestage_fixture(root.path());
    let bytes = b"durable requester bytes excluded from the final placement";
    let target = DfsVersion::from_legacy_named("prestage_excluded_requester_target").unwrap();
    fixture
        .requester
        .content
        .put_object_version(fixture.current.object_id.as_str(), target.as_str(), bytes)
        .expect("stage requester tuple before placement");
    let mut policy = DfsReplicationPolicy::new(1, 1).unwrap();
    policy.required_node_ids.push(fixture.primary_id.clone());
    policy.forbidden_node_ids.push(fixture.requester_id.clone());
    // Exercise the production final-plan receipt filter, after a placement
    // decision replaces the original requester with the primary.
    let outcome = fixture
        .primary
        .write_stable_remote_distributed_manifest(
            DfsWriteRequest {
                file_system_id: fixture.current.file_system_id.clone(),
                object_id: fixture.current.object_id.clone(),
                target_version: target.clone(),
                requesting_node_id: fixture.requester_id.clone(),
            },
            Some(fixture.current.version.clone()),
            content_hash(bytes),
            bytes.len() as u64,
            true,
            vec![fixture.primary_id.clone()],
            policy,
            "prestage-final-plan-excludes-requester".to_string(),
            DfsDistributedWriteOptions {
                repair: DfsWriteRepair::Background,
                ..DfsDistributedWriteOptions::default()
            },
        )
        .expect("plan excludes the staged requester");
    let NetworkedDfsWriteOutcome::Pending {
        intent_id,
        durable_receipts,
        required_receipts,
    } = outcome
    else {
        panic!("an excluded requester must not satisfy W: {outcome:?}");
    };
    assert_eq!((durable_receipts, required_receipts), (0, 1));
    assert_eq!(
        fixture
            .primary
            .coordinator
            .intent(&intent_id)
            .unwrap()
            .desired_replica_nodes,
        vec![fixture.primary_id.clone()]
    );
    let mut committed = false;
    for _ in 0..16 {
        fixture
            .requester
            .process_incoming_without_deferred_parent_touches()
            .unwrap();
        fixture
            .primary
            .process_incoming_without_deferred_parent_touches()
            .unwrap();
        fixture.primary.retry_pending_repairs().unwrap();
        if matches!(
            fixture.primary.write_outcome(&intent_id).unwrap(),
            NetworkedDfsWriteOutcome::Committed { .. }
        ) {
            committed = true;
            break;
        }
    }
    assert!(
        committed,
        "the selected primary must fetch and durably stage before commit"
    );
    let receipts = fixture.primary.coordinator.receipts_for_intent(&intent_id);
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].replica_node_id, fixture.primary_id);
    assert_durable_binding(
        &root.join("primary-content"),
        &fixture.current.object_id,
        &target,
        bytes,
    );
}

fn assert_requester_fetch_rejected(
    fixture: &mut PrestageFixture,
    requester: &NodeId,
    command: &DfsFetchBlobCommand,
) {
    let event = fixture
        .requester
        .handle_fetch_blob(requester.clone(), command.clone())
        .expect("invalid fetch has a typed rejection response");
    assert!(
        matches!(event, NetworkedDfsServiceEvent::BlobFetchRejected { .. }),
        "unexpected fetch admission: {event:?}"
    );
}

#[test]
fn excluded_requester_with_replicated_pending_intent_fetches_and_commits_standard_w_n() {
    for (w, n) in [(1, 2), (2, 3)] {
        let root = temp_root(&format!("excluded-requester-pending-intent-w{w}-n{n}"));
        let mut fixture = prestage_fixture(root.path());
        let primary_id = fixture.primary_id.clone();
        let file_system = file_system(fixture.primary_id.clone());
        let shard = fixture
            .primary
            .select_metadata_shard_for_object(&fixture.current)
            .unwrap()
            .unwrap();
        let control_plane = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
        control_plane.with_client(|kv| {
            let mut control = DfsControlPlaneStore::new(kv);
            control
                .put_file_system_config(&DfsFileSystemConfig::from(&file_system))
                .unwrap();
            control.put_metadata_shard(&shard).unwrap();
        });
        let mut replicas = Vec::new();
        let mut desired = vec![fixture.primary_id.clone()];
        for index in 1..n {
            let node =
                NodeId::from_legacy_named(format!("excluded-requester-replica-{index}")).unwrap();
            desired.push(node.clone());
            let mut metadata =
                open_root_local_metadata_backend(root.join(format!("replica-{index}-metadata")))
                    .unwrap();
            metadata.put_file_system(&file_system).unwrap();
            metadata.put_object(&fixture.current).unwrap();
            replicas.push(
                NetworkedDfsService::new(
                    fixture.network.add_node(node),
                    LocalContentStore::new(root.join(format!("replica-{index}-content"))).unwrap(),
                )
                .with_control_plane(control_plane.clone())
                .with_metadata_backend(metadata)
                .unwrap(),
            );
        }
        let bytes = b"pre-staged requester source survives canonical placement exclusion";
        let target = DfsVersion::from_legacy_named("excluded-requester-pending-target").unwrap();
        fixture
            .requester
            .content
            .put_object_version(fixture.current.object_id.as_str(), target.as_str(), bytes)
            .unwrap();
        let mut policy = DfsReplicationPolicy::new(w, n).unwrap();
        policy.required_node_ids = desired.clone();
        policy.forbidden_node_ids.push(fixture.requester_id.clone());
        let outcome = fixture
            .primary
            .write_stable_remote_distributed_manifest(
                DfsWriteRequest {
                    file_system_id: fixture.current.file_system_id.clone(),
                    object_id: fixture.current.object_id.clone(),
                    target_version: target.clone(),
                    requesting_node_id: fixture.requester_id.clone(),
                },
                Some(fixture.current.version.clone()),
                content_hash(bytes),
                bytes.len() as u64,
                true,
                desired.clone(),
                policy,
                "excluded-requester-pending-operation".to_string(),
                DfsDistributedWriteOptions {
                    repair: DfsWriteRepair::Background,
                    ..DfsDistributedWriteOptions::default()
                },
            )
            .unwrap();
        let NetworkedDfsWriteOutcome::Pending {
            intent_id,
            durable_receipts,
            required_receipts,
        } = outcome
        else {
            panic!("excluded requester cannot satisfy W={w}: {outcome:?}");
        };
        assert_eq!((durable_receipts, required_receipts), (0, w));
        let mut pending_intent = fixture
            .primary
            .metadata_backend
            .as_ref()
            .unwrap()
            .get_intent(&fixture.current.object_id, &intent_id)
            .unwrap()
            .unwrap();
        assert_eq!(pending_intent.coordinator_node_id, fixture.requester_id);
        assert_eq!(pending_intent.status, DfsWriteIntentStatus::ReceiptsPending);
        assert_eq!(pending_intent.policy.desired_replicas, n);
        assert_eq!(
            pending_intent
                .desired_replica_node_ids
                .iter()
                .collect::<BTreeSet<_>>(),
            desired.iter().collect::<BTreeSet<_>>()
        );
        assert!(
            !pending_intent
                .desired_replica_node_ids
                .contains(&fixture.requester_id)
        );

        // The source has already received the primary's durable pending intent.
        // Without this prefix, absent-intent compatibility admission masks the
        // source/replica distinction and the older test never exercises it.
        let mut source_metadata =
            open_root_local_metadata_backend(root.join("source-pending-metadata")).unwrap();
        source_metadata.put_file_system(&file_system).unwrap();
        source_metadata.put_object(&fixture.current).unwrap();
        source_metadata.put_intent(&pending_intent).unwrap();
        fixture.requester.metadata_backend = Some(
            crate::service_state::DfsMetadataBackendOwner::Owned(Box::new(source_metadata)),
        );

        let command = DfsFetchBlobCommand {
            request_id: "excluded-requester-admission-check".to_string(),
            intent_id: intent_id.clone(),
            object_id: fixture.current.object_id.clone(),
            version: target.clone(),
            content_hash: content_hash(bytes),
            authority_shard: Some(shard),
        };
        let outsider = NodeId::from_legacy_named("excluded-requester-outsider").unwrap();
        let _outsider_mailbox = fixture.network.add_node(outsider.clone());
        assert_requester_fetch_rejected(&mut fixture, &outsider, &command);
        for epoch in [
            command.authority_shard.as_ref().unwrap().epoch,
            command.authority_shard.as_ref().unwrap().epoch + 1,
        ] {
            let mut forged = command.clone();
            let authority = forged.authority_shard.as_mut().unwrap();
            authority.primary_node_id = outsider.clone();
            authority.epoch = epoch;
            assert_requester_fetch_rejected(&mut fixture, &outsider, &forged);
        }
        let projected_authority = fixture.requester.control_plane.take();
        assert_requester_fetch_rejected(&mut fixture, &primary_id, &command);
        fixture.requester.control_plane = projected_authority;
        // A desired content target is not the metadata primary. It cannot use
        // the extra requester-source permission merely because it can stage.
        assert_requester_fetch_rejected(&mut fixture, &desired[1], &command);
        // This negative RPC was invoked directly, without installing a fetch
        // in the target's write coordinator. Consume its actual rejection at
        // the caller's reply boundary before pumping unrelated stage work.
        let rejected_reply = replicas[0].transport.drain_incoming_messages().unwrap();
        assert_eq!(rejected_reply.len(), 1);
        assert!(matches!(
            &rejected_reply[0].message,
            DfsNetworkMessage::FetchBlobResponse(response)
                if response.request_id == command.request_id
                    && response.intent_id == intent_id
                    && matches!(response.result, DfsFetchBlobResult::Rejected { .. })
        ));
        for invalid in [
            DfsFetchBlobCommand {
                version: fixture.current.version.clone(),
                ..command.clone()
            },
            DfsFetchBlobCommand {
                content_hash: content_hash(b"different payload"),
                ..command.clone()
            },
        ] {
            assert_requester_fetch_rejected(&mut fixture, &primary_id, &invalid);
        }

        let source_events = fixture
            .requester
            .process_incoming_without_deferred_parent_touches()
            .unwrap();
        assert!(
            source_events.iter().any(|event| matches!(
                event,
                NetworkedDfsServiceEvent::BlobFetched { intent_id: fetched, .. }
                    if fetched == &intent_id
            )),
            "the replicated pending intent must allow its requester source: {source_events:?}"
        );
        assert_eq!(
            fixture
                .primary
                .coordinator
                .durable_receipt_count(&intent_id),
            0
        );
        assert_eq!(
            fixture
                .primary
                .current_object_for_write(&fixture.current.object_id)
                .unwrap()
                .version,
            fixture.current.version
        );

        let mut committed = false;
        for _ in 0..16 {
            fixture
                .primary
                .process_incoming_without_deferred_parent_touches()
                .unwrap();
            if w == 2
                && fixture
                    .primary
                    .coordinator
                    .durable_receipt_count(&intent_id)
                    < w
            {
                assert_eq!(
                    fixture
                        .primary
                        .current_object_for_write(&fixture.current.object_id)
                        .unwrap()
                        .version,
                    fixture.current.version
                );
            }
            for replica in &mut replicas {
                replica
                    .process_incoming_without_deferred_parent_touches()
                    .unwrap();
            }
            fixture.primary.retry_pending_repairs().unwrap();
            if matches!(
                fixture.primary.write_outcome(&intent_id).unwrap(),
                NetworkedDfsWriteOutcome::Committed { .. }
            ) {
                committed = true;
                break;
            }
        }
        assert!(
            committed,
            "excluded requester fetch/stage must reach W={w}/N={n}"
        );
        let receipts = fixture
            .primary
            .coordinator
            .receipts_for_intent(&intent_id)
            .into_iter()
            .cloned()
            .collect::<Vec<_>>();
        assert!(receipts.len() >= w);
        assert!(
            receipts
                .iter()
                .all(|receipt| desired.contains(&receipt.replica_node_id))
        );
        assert_eq!(
            fixture
                .primary
                .current_object_for_write(&fixture.current.object_id)
                .unwrap()
                .version,
            target
        );
        for receipt in receipts {
            let index = desired
                .iter()
                .position(|node| node == &receipt.replica_node_id)
                .unwrap();
            let content_root = if index == 0 {
                root.join("primary-content")
            } else {
                root.join(format!("replica-{index}-content"))
            };
            assert_durable_binding(&content_root, &fixture.current.object_id, &target, bytes);
        }

        // The transfer-source exception expires with the pending write. A
        // terminal coordinator without a replica receipt has no extra right.
        for status in [
            DfsWriteIntentStatus::Committed,
            DfsWriteIntentStatus::Aborted,
        ] {
            // Each terminal outcome is a separate durable history. Never
            // rewrite a committed intent as aborted to exercise admission.
            let mut terminal_metadata = open_root_local_metadata_backend(
                root.join(format!("source-terminal-{status:?}-metadata")),
            )
            .unwrap();
            terminal_metadata.put_file_system(&file_system).unwrap();
            terminal_metadata.put_object(&fixture.current).unwrap();
            pending_intent.status = DfsWriteIntentStatus::ReceiptsPending;
            terminal_metadata.put_intent(&pending_intent).unwrap();
            pending_intent.status = status;
            terminal_metadata.put_intent(&pending_intent).unwrap();
            fixture.requester.metadata_backend = Some(
                crate::service_state::DfsMetadataBackendOwner::Owned(Box::new(terminal_metadata)),
            );
            assert_requester_fetch_rejected(&mut fixture, &primary_id, &command);
        }
    }
}
