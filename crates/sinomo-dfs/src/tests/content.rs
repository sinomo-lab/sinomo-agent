//! Content regression tests.

use super::*;

#[test]
fn content_hash_range_fallback_validates_returned_window_not_full_blob_size() {
    let root = temp_root("content_hash_range_fallback_validates_window");
    let node = NodeId::from_legacy_named("node_content_hash_range_window").expect("node");
    let object_id =
        DfsObjectId::from_legacy_named("obj_content_hash_range_window").expect("object");
    let version =
        DfsVersion::from_legacy_named("version_content_hash_range_window").expect("version");
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let dfs = NetworkedDfsService::new(
        crate::test_runtime::DfsTestRuntime::local_transport(node),
        content,
    );
    let bytes = (0..64).collect::<Vec<u8>>();
    let manifest = dfs
        .content
        .put_object_version_blob_only(object_id.as_str(), version.as_str(), &bytes)
        .expect("blob-only content");

    let mut reader = dfs
        .open_content_hash_range_stream(
            &object_id,
            &version,
            &manifest.content_hash,
            Some(bytes.len() as u64),
            16,
            bytes.len(),
        )
        .expect("range inside expected object size");
    let mut actual = Vec::new();
    reader.read_to_end(&mut actual).expect("read range");

    assert_eq!(actual, bytes[16..]);
}

#[test]
fn content_root_catalog_refresh_failure_durably_invalidates_previous_catalog() {
    let root = temp_root("content-root-catalog-refresh-failure");
    let node_id = NodeId::from_legacy_named("node_catalog_refresh_failure").expect("node");
    let (_runtime, mut dfs) = native_sifs_service(
        "content_root_catalog_refresh_failure_durably_invalidates_previous_catalog",
        node_id.clone(),
    );
    let shard = DfsMetadataShardConfig {
        file_system_id: sifs_file_system_id(),
        shard_id: "root".to_string(),
        primary_node_id: node_id.clone(),
        replica_node_ids: Vec::new(),
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,

        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let file_system = DfsFileSystemConfig {
        catalog_generation: 0,
        retention_policies: Default::default(),
        module_metadata: Default::default(),
        file_system_id: shard.file_system_id.clone(),
        display_name: "Catalog refresh failure filesystem".to_string(),
        owner_node_id: node_id,
        subtree_policies: Vec::new(),
    };
    let topology_digest = dfs_content_root_catalog_topology_digest_with_file_systems(
        std::slice::from_ref(&file_system),
        std::slice::from_ref(&shard),
    )
    .expect("topology digest");
    let identity = DfsContentRootCatalogShardIdentity::from_config(&shard);
    let catalog = DfsContentRootCatalog::from_topology_fragments(
        topology_digest.clone(),
        vec![file_system.file_system_id.clone()],
        vec![identity.clone()],
        1,
        Vec::new(),
        vec![DfsContentRootCatalogFragment {
            identity,
            topology_digest,
            generated_at_unix_nanos: 1,
            coverage: DfsContentRootCatalogCoverage::Complete {
                prefix_proof_version: DFS_CONTENT_ROOT_CATALOG_PREFIX_PROOF_VERSION,
                journal_sequence: 1,
                state_fingerprint: format!(
                    "dfs-metadata-prefix-v2:blake3:{}",
                    blake3::hash(b"catalog-before-refresh-failure").to_hex()
                ),
            },
            entries: Vec::new(),
        }],
    )
    .expect("complete catalog");
    let store = default_dfs_content_root_catalog_store(root.join("content"));
    store
        .install_test_catalog_at(catalog, 1)
        .expect("install previous complete catalog");

    let error = dfs
        .refresh_content_root_catalog(
            std::slice::from_ref(&file_system),
            std::slice::from_ref(&shard),
            &[],
            root.join("content"),
            &[],
            0,
        )
        .expect_err("zero page budget must fail refresh");
    assert!(error.to_string().contains("page budget"));

    let installed = store
        .load()
        .expect("load failure catalog")
        .expect("failure catalog installed");
    assert!(
        !installed.is_complete(),
        "a stale complete catalog must not survive a hard refresh failure"
    );
    assert!(installed.incomplete_reasons().iter().any(|reason| matches!(
        reason,
        DfsContentRootCatalogIncompleteReason::CatalogRefreshFailed { .. }
    )));

    let duplicate_error = dfs
        .refresh_content_root_catalog(
            &[file_system.clone(), file_system],
            std::slice::from_ref(&shard),
            &[],
            root.join("content"),
            &[],
            1,
        )
        .expect_err("duplicate topology must fail refresh");
    assert!(duplicate_error.to_string().contains("duplicate identities"));
    let fallback = store
        .load()
        .expect("load fallback failure catalog")
        .expect("fallback failure catalog installed");
    assert!(!fallback.is_complete());
    assert!(fallback.incomplete_reasons().iter().any(|reason| {
        matches!(
            reason,
            DfsContentRootCatalogIncompleteReason::CatalogRefreshFailed { reason }
                if reason.contains("duplicate identities")
        )
    }));

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn native_content_root_catalog_pages_share_one_recorded_cut() {
    let node =
        crate::test_runtime::DfsTestRuntime::ordered_node_ids("native-catalog-pages", 1).remove(0);
    let (runtime, browser) = native_sifs_service("native-catalog-pages", node.clone());
    drop(browser);
    let mut writer = runtime.metadata_primary_service("/").unwrap();
    for index in 0..300_u64 {
        writer
            .posix_write_file(
                &format!("/catalog-{index:04}.bin"),
                &index.to_le_bytes(),
                DfsWriteOptions::replicated_eventual(vec![node.clone()]),
            )
            .unwrap();
    }
    let shard = runtime.root_shard().unwrap();
    drop(writer);
    let mut owner = runtime.metadata_primary_service("/").unwrap();
    let backend = owner.metadata_backend_mut().unwrap();
    backend.begin_consistent_read_snapshot().unwrap();
    let request = DfsContentRootCatalogPageRequest {
        identity: DfsContentRootCatalogShardIdentity::from_config(&shard),
        expected_topology_digest: dfs_content_root_catalog_topology_digest_with_file_systems(
            &[DfsFileSystemConfig::from(&runtime.file_system())],
            &[shard],
        )
        .unwrap(),
        minimum_journal_sequence: 0,
        minimum_state_fingerprint: None,
        after: None,
        max_entries: 16,
    };
    let mut cursor = None;
    let mut entries = Vec::new();
    let mut pages = 0;
    let mut cut = None;
    loop {
        let mut next = request.clone();
        next.after = cursor;
        if let Some(after) = &next.after {
            next.minimum_journal_sequence = after.journal_sequence;
            next.minimum_state_fingerprint = Some(after.state_fingerprint.clone());
        }
        let page = backend.content_root_catalog_page(&next).unwrap();
        page.validate_for_request(&next).unwrap();
        pages += 1;
        let identity = (page.coverage.clone(), page.generated_at_unix_nanos);
        if let Some(cut) = &cut {
            assert_eq!(&identity, cut);
        } else {
            cut = Some(identity);
        }
        assert!(page.entries.len() <= 16);
        entries.extend(page.entries);
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    assert!(pages > 1);
    assert!(entries.len() > 300);
    backend.end_consistent_read_snapshot();
    drop(owner);
    assert_eq!(runtime.observation_dropped_count(), 0);
    runtime.close().unwrap();
}

#[test]
fn dfs_smoke_content_nodes_prefer_fresh_remote_advertisements() {
    let local = NodeId::from_legacy_named("node_dfs_smoke_fresh_ads_local").expect("local node");
    let stale = NodeId::from_legacy_named("node_dfs_smoke_fresh_ads_stale").expect("stale node");
    let fresh =
        NodeId::from_legacy_named("node_dfs_smoke_fresh_ads_reachable").expect("fresh node");
    let now = OffsetDateTime::UNIX_EPOCH + time::Duration::hours(2);

    let mut local_advertisement = DfsNodeAdvertisement::content(local.clone());
    local_advertisement.updated_at = now - time::Duration::hours(2);
    let mut stale_advertisement = DfsNodeAdvertisement::content(stale.clone());
    stale_advertisement.updated_at = now - time::Duration::hours(2);
    let mut fresh_advertisement = DfsNodeAdvertisement::content(fresh.clone());
    fresh_advertisement.updated_at = now - time::Duration::minutes(5);

    let content_nodes = dfs_smoke_content_node_ids(
        &local,
        vec![
            local_advertisement,
            stale_advertisement,
            fresh_advertisement,
        ],
        now,
    );
    assert_eq!(
        content_nodes,
        vec![fresh.clone()],
        "DFS smoke should avoid stale content advertisements when a fresh remote is available"
    );

    let candidates =
        dfs_smoke_replica_candidate_sets(&local, content_nodes, vec![stale.clone(), fresh.clone()]);
    assert_eq!(candidates, vec![vec![local, fresh]]);
}

#[test]
fn add_object_metadata_batch_rejects_unrecorded_content_generation() {
    let owner = NodeId::from_legacy_named("node_batch_size_owner").expect("node");
    let (runtime, client) =
        native_sifs_service("native-metadata-batch-trusted-size", owner.clone());
    drop(client);
    // Backend publication is Main-side work on the owning shard's store.
    let mut dfs = runtime
        .metadata_primary_service("/services/batch-visible/trusted-size.txt")
        .unwrap();
    let object_id = DfsObjectId::generate();
    let version = DfsVersion::generate();
    let manifest = dfs
        .content()
        .put_object_version(object_id.as_str(), version.as_str(), b"local")
        .expect("actual current local content manifest");
    let mut object = file_metadata(owner);
    object.object_id = object_id;
    object.path = "/services/batch-visible/trusted-size.txt".to_string();
    object.content_hash = Some(manifest.content_hash);
    object.content_size_bytes = Some(128);
    object.version = version;
    let error = dfs
        .add_object_metadata_batch([object.clone()])
        .expect_err("local bytes alone cannot authorize an unrecorded visible generation");
    assert!(
        error
            .to_string()
            .contains("no sealed or recorded same-object head")
    );
    let backend = dfs.metadata_backend().expect("actual metadata backend");
    assert!(backend.get_object(&object.object_id).unwrap().is_none());
    assert!(
        backend
            .get_version(&object.object_id, &object.version)
            .unwrap()
            .is_none()
    );
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn complete_remote_range_content_mismatch_prunes_stale_location() {
    let (runtime, source, _, reader_node, stat, bytes) = native_read_fixture(
        "native-range-no-storage-rehash",
        1,
        true,
        b"actual complete ranged read payload".to_vec(),
    );
    // The authoritative reader runs the read engine over its node's store;
    // a delegating client has no local object view to plan from.
    let mut reader = runtime.metadata_primary_service(&stat.path).unwrap();
    assert_eq!(reader.node_id(), &reader_node);
    assert_eq!(
        reader
            .request_remote_object_range_read(&stat.object_id, 0, bytes.len())
            .unwrap(),
        vec![source.clone()]
    );
    let request = reader.reads.pending_reads.keys().next().unwrap().clone();
    let original = reader
        .transport
        .drain_incoming_messages()
        .unwrap()
        .into_iter()
        .find(|routed| {
            matches!(&routed.message,
        DfsNetworkMessage::ReadResponse(response) if response.request_id == request)
        })
        .unwrap();
    assert_eq!(original.source_node_id, source);
    assert_eq!(original.target_node_id, reader_node);
    let DfsNetworkMessage::ReadResponse(mut response) = original.message else {
        unreachable!()
    };
    assert_eq!(response.object_id, stat.object_id);
    let DfsReadResult::RangeFound(data) = &mut response.result else {
        panic!("actual native RangeFound required")
    };
    assert_eq!(data.object_id, stat.object_id);
    assert_eq!(data.version, stat.version);
    assert_eq!(data.content_hash, stat.content_hash);
    assert_eq!(data.offset, 0);
    assert_eq!(data.total_size_bytes, bytes.len() as u64);
    assert_eq!(data.bytes, bytes);
    data.bytes[0] ^= 0xff;
    let event = reader
        .handle_networked_read_response(source.clone(), response)
        .unwrap();
    assert!(
        matches!(
            event,
            NetworkedDfsServiceEvent::RemoteReadCompleted {
                result: DfsReadResult::RangeFound(_),
                ..
            }
        ),
        "normal matching identity/length acceptance does not rehash stored range bytes"
    );
    assert!(!reader.reads.pending_reads.contains_key(&request));
    assert!(
        reader
            .current_read_object_metadata(&stat.object_id)
            .unwrap()
            .current_locations
            .contains(&source)
    );
    assert_eq!(
        reader
            .metadata_backend()
            .unwrap()
            .get_object(&stat.object_id)
            .unwrap()
            .unwrap()
            .content_hash,
        stat.content_hash
    );
    drop(reader);
    runtime.close().unwrap();
}

#[test]
fn fetch_response_reuses_existing_local_blob_without_rehashing_payload() {
    let name = "native-existing-blob-duplicate-fetch";
    let (runtime, initial, config) =
        crate::test_support::current_native_metadata_service(name, 3, "/services").unwrap();
    let owner = config.primary_node_id;
    drop(initial);
    let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 3);
    let peers = nodes
        .iter()
        .filter(|node| **node != owner)
        .cloned()
        .collect::<Vec<_>>();
    let source = peers[0].clone();
    let target = peers[1].clone();
    let path = "/services/native-duplicate-fetch.bin";
    let mut base_policy =
        DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 1).unwrap();
    base_policy.required_node_ids = vec![owner.clone()];
    runtime
        .apply_placement_policy(&owner, path, DfsPlacementScope::Subtree, base_policy)
        .unwrap();
    let mut first = runtime.client(&owner).unwrap();
    let base = first
        .posix_write_file(
            path,
            b"actual original fetch head",
            DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .unwrap();
    drop(first);
    let mut current_policy =
        DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 2, 3).unwrap();
    current_policy.required_node_ids = nodes.clone();
    runtime
        .apply_placement_policy(&owner, path, DfsPlacementScope::Subtree, current_policy)
        .unwrap();
    deny_native_read_repair(&runtime, &owner, &target);
    let mut first = runtime.metadata_primary_service(path).unwrap();
    // The test drives the node's repair executor; routine maintenance stays
    // out of the way.
    let (execution, hold) = super::repair::native_private_repair_owner(&runtime, &first, path, ());
    let bytes = vec![0x76; 2 * 1024 * 1024 + 4096];
    let version = crate::namespace::next_version(&base.version);
    let request = DfsWriteRequest {
        file_system_id: base.file_system_id.clone(),
        object_id: base.object_id.clone(),
        target_version: version.clone(),
        requesting_node_id: owner.clone(),
    };
    let mut policy = DfsReplicationPolicy::new(2, 3).unwrap();
    policy.required_node_ids = nodes.clone();
    let outcome = first
        .write_distributed_reader_with_candidates(
            request.clone(),
            std::io::Cursor::new(&bytes),
            nodes,
            policy,
            DfsDistributedWriteOptions::default(),
        )
        .unwrap();
    first.settle_ready_write_outcome(outcome, true).unwrap();
    assert!(
        super::repair::indexed_object_repair_needs(
            &runtime,
            &first,
            &base.file_system_id,
            path,
            &request.object_id,
            &version,
        )
        .iter()
        .any(|(missing, owner_intent)| *missing == target && owner_intent.is_some()),
        "the committed W2 write leaves its third replica as repair work"
    );
    let hash = content_hash(&bytes);
    first.content().debug_remove_loose_blob(&hash).unwrap();
    let captured = Arc::new(Mutex::new(Vec::new()));
    runtime.install_fault(crate::test_support::DfsTestFaultRule {
        source: Some(owner.clone()),
        target: Some(source.clone()),
        route: Some(crate::DFS_DATA_FETCH_BLOB_ROUTE.into()),
        call_id: None,
        origin: Some(crate::test_support::DfsTestRpcOrigin::RegisteredHost),
        remaining_hits: 1,
        action: crate::test_runtime::FaultAction::AlterFetchReplyAfterDispatch {
            change: crate::test_runtime::FetchReplyFault::CorruptPayloadAndLength,
            captured: Arc::clone(&captured),
        },
    });
    // The executor refills this node's own copy from the source before it
    // can send the replica; the source's reply is corrupted in flight and
    // held in this lane's queue.
    let data_root = runtime.data_dir(&owner).root();
    let shards = runtime.metadata_shards().unwrap();
    let none = crate::DfsMaintenanceExecutionExclusions::default();
    let pass = first
        .run_repair_executor_pass(&data_root, &shards, &none, 1)
        .unwrap();
    assert_eq!(pass.scheduled.len(), 1);
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    while captured.lock().unwrap().is_empty() {
        assert!(Instant::now() < deadline, "the source's fetch reply");
        std::thread::sleep(Duration::from_millis(5));
    }
    // Another lane of the same node binds the verified content into this
    // same content store before the queued duplicate reply is served.
    runtime.clear_faults();
    deny_native_read_repair(&runtime, &owner, &target);
    let mut second = runtime.metadata_primary_service(path).unwrap();
    second
        .store_command_bytes(&request.object_id, &version, &hash, &bytes)
        .unwrap();
    drop(second);
    let events = first.process_incoming().unwrap();
    assert!(
        events.iter().any(
            |event| matches!(event, NetworkedDfsServiceEvent::BlobFetched {
        object_id, source_node_id, ..
    } if object_id == &request.object_id && source_node_id == &source)
        ),
        "actual duplicate payload corruption and length change are ignored only after a genuine prior verified binding: {events:?}"
    );
    assert!(
        crate::repair_executor::repair_executor_work(&data_root).due,
        "the fetch reply ends the attempt and wakes the executor"
    );
    let stored = first
        .content()
        .read_object_version(request.object_id.as_str(), version.as_str())
        .unwrap();
    assert_eq!(stored.0.content_hash, hash);
    assert_eq!(stored.0.size_bytes, bytes.len() as u64);
    assert_eq!(stored.1, bytes);
    drop(execution);
    drop(first);
    drop(hold);
    runtime.clear_faults();
    runtime.close().unwrap();
}

#[test]
fn rejected_streamed_stage_does_not_reserve_version_for_later_valid_content() {
    use crate::runtime_host::{
        DaemonDfsPreDispatchTestPoint, install_daemon_dfs_pre_dispatch_test_hook,
    };
    use std::sync::{Arc, Mutex};
    let (runtime, nodes) = crate::test_runtime::DfsTestRuntime::start_cluster(
        "rejected-streamed-stage-native-base",
        2,
    )
    .unwrap();
    let owner = runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|shard| shard.shard_id == "root")
        .unwrap()
        .primary_node_id;
    let replica = nodes.iter().find(|node| **node != owner).unwrap().clone();
    let path = "/streamed-stage-base.bin";
    let mut client = runtime.client(&owner).unwrap();
    let base = client
        .posix_write_file(
            path,
            b"actual native base",
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(1, 1).unwrap(),
                replica_candidates: vec![owner.clone()],
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    client.posix_sync_directory("/").unwrap();
    drop(client);
    let captured = Arc::new(Mutex::new(None::<DfsStageBlobCommand>));
    let capture = Arc::clone(&captured);
    let receiver_hook = install_daemon_dfs_pre_dispatch_test_hook(
        runtime.data_dir(&replica),
        Arc::new(move |point, command, _| {
            if matches!(point, DaemonDfsPreDispatchTestPoint::BeforeExecute { .. })
                && let Some(command) = command
                && let DfsNetworkMessage::StageBlob(stage) = &command.routed_message.message
            {
                let mut saved = capture.lock().unwrap();
                if saved.is_none() {
                    *saved = Some(stage.clone());
                }
                return Err(DfsError::Cancelled {
                    operation: "capture actual native Stage before receiver effects",
                });
            }
            Ok(())
        }),
    );
    let background = runtime
        .deny_background_replica_before_claim(&owner)
        .unwrap();
    let valid_bytes = b"later valid streamed stage bytes";
    let target_version = crate::namespace::next_version(&base.version);
    let mut policy = DfsReplicationPolicy::new(2, 2).unwrap();
    policy.required_node_ids = nodes.clone();
    let mut writer = runtime.metadata_primary_service(path).unwrap();
    let outcome = writer
        .write_distributed_reader_with_candidates(
            DfsWriteRequest {
                file_system_id: base.file_system_id.clone(),
                object_id: base.object_id.clone(),
                target_version: target_version.clone(),
                requesting_node_id: owner.clone(),
            },
            std::io::Cursor::new(valid_bytes),
            nodes,
            policy,
            DfsDistributedWriteOptions::default(),
        )
        .unwrap();
    let NetworkedDfsWriteOutcome::Pending { intent_id, .. } = outcome else {
        panic!("actual denied Stage keeps W2 pending")
    };
    let stage = captured
        .lock()
        .unwrap()
        .clone()
        .expect("actual native Stage command");
    assert_eq!(stage.intent_id, intent_id);
    assert_eq!(stage.object_id, base.object_id);
    assert_eq!(stage.version, target_version);
    assert_eq!(stage.expected_base_version.as_ref(), Some(&base.version));
    drop(receiver_hook);
    let mut dfs = runtime.client(&replica).unwrap();
    assert!(
        dfs.content
            .read_manifest(base.object_id.as_str(), target_version.as_str())
            .is_err()
    );

    let rejected_bytes = b"stale streamed stage bytes";
    let rejected_hash = content_hash(rejected_bytes);
    let mut rejected_ingest = dfs
        .content
        .begin_quarantined_blob_ingest(rejected_bytes.len() as u64, Some(&rejected_hash))
        .unwrap();
    rejected_ingest.write_chunk(rejected_bytes).unwrap();
    let rejected_blob = rejected_ingest.finish_quarantined().unwrap();
    let mut stale = stage.clone();
    stale.request_id.push_str("-negative-stale-base");
    stale.expected_base_version =
        Some(DfsVersion::from_legacy_named("version_streamed_stage_stale").unwrap());
    assert_ne!(stale.expected_base_version, stage.expected_base_version);
    stale.content_hash = rejected_hash;
    let rejected = dfs
        .handle_preingested_stage_blob(owner.clone(), stale, rejected_blob)
        .expect("rejected stage response event");
    assert!(
        matches!(
            &rejected,
            NetworkedDfsServiceEvent::BlobStageRejected { reason, .. }
                if reason.contains("stale DFS stage base")
        ),
        "the original stale-base check must reject before content persistence: {rejected:?}"
    );
    assert!(
        dfs.content
            .read_manifest(base.object_id.as_str(), target_version.as_str())
            .is_err(),
        "rejected streamed stage must not reserve its requested version"
    );

    let valid_hash = content_hash(valid_bytes);
    assert_eq!(stage.content_hash, valid_hash);
    let mut valid_ingest = dfs
        .content
        .begin_quarantined_blob_ingest(valid_bytes.len() as u64, Some(&valid_hash))
        .unwrap();
    valid_ingest.write_chunk(valid_bytes).unwrap();
    let valid_blob = valid_ingest.finish_quarantined().unwrap();
    let valid = dfs
        .handle_preingested_stage_blob(owner, stage, valid_blob)
        .expect("valid stage response event");
    assert!(matches!(valid, NetworkedDfsServiceEvent::BlobStaged { .. }));
    let manifest = dfs
        .content
        .read_manifest(base.object_id.as_str(), target_version.as_str())
        .expect("valid stage manifest");
    assert_eq!(manifest.content_hash, valid_hash);
    assert_eq!(
        dfs.content
            .read_blob(&manifest.content_hash)
            .expect("valid stage blob"),
        valid_bytes
    );
    drop(dfs);
    drop(writer);
    drop(background);
    runtime.close().unwrap();
}

#[test]
fn stage_blob_persists_when_response_target_is_unroutable() {
    // Reuse the normal native producer/receiver and its captured Stage reply.
    // The receiver completed persistence; only actual reply delivery was lost.
    let (runtime, owner, replica, writer, stat, _intent, exchange, background) =
        super::repair::native_committed_stage_backlog("stage-response-target-unroutable");
    assert_eq!(exchange.source_node_id, owner);
    assert_eq!(exchange.target_node_id, replica);
    let DfsNetworkMessage::StageBlob(command) = &exchange.command.routed_message.message else {
        panic!("actual native Stage command required");
    };
    let DfsNetworkMessage::StageBlobResponse(response) = &exchange.response.message else {
        panic!("actual completed native Stage reply required");
    };
    assert_eq!(exchange.response.source_node_id, replica);
    assert_eq!(exchange.response.target_node_id, owner);
    assert_eq!(response.request_id, command.request_id);
    assert_eq!(response.intent_id, command.intent_id);
    assert_eq!(response.file_system_id, stat.file_system_id);
    assert_eq!(response.object_id, stat.object_id);
    assert_eq!(response.version, stat.version);
    assert_eq!(response.replica_node_id, replica);
    let bytes = writer
        .content
        .read_blob(
            stat.content_hash
                .as_deref()
                .expect("original producer hash"),
        )
        .expect("original hash-addressed producer bytes");
    let expected_hash = content_hash(&bytes);
    assert!(
        matches!(&response.result, DfsBlobTransferResult::Durable { content_hash }
        if content_hash == &expected_hash)
    );
    let receiver = runtime.client(&replica).expect("actual native receiver");
    let manifest = receiver
        .content
        .read_manifest(stat.object_id.as_str(), stat.version.as_str())
        .expect("stage manifest");
    assert_eq!(manifest.content_hash, expected_hash);
    assert_eq!(manifest.size_bytes, bytes.len() as u64);
    assert_eq!(
        receiver
            .content
            .read_blob(&manifest.content_hash)
            .expect("blob"),
        bytes
    );
    drop(receiver);
    drop(writer);
    drop(background);
    runtime.clear_faults();
    runtime.close().expect("ordinary native close");
}

#[test]
fn reconcile_local_manifests_requires_current_blob_bytes() {
    let name = "reconcile-requires-current-blob";
    let (runtime, client, config) =
        crate::test_support::current_native_metadata_service(name, 2, "/services").unwrap();
    let node_id = config.primary_node_id;
    drop(client);
    let remote = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 2)
        .into_iter()
        .find(|node| *node != node_id)
        .unwrap();
    let path = "/services/reconcile-requires-current-blob.bin";
    let mut policy = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 1).unwrap();
    policy.required_node_ids = vec![remote.clone()];
    runtime
        .apply_placement_policy(&node_id, path, DfsPlacementScope::Subtree, policy)
        .unwrap();
    let bytes = vec![b'r'; 17 * 1024];
    let mut dfs = runtime
        .metadata_primary_service(path)
        .expect("actual current metadata owner");
    let mut replication = DfsReplicationPolicy::new(1, 1).unwrap();
    replication.required_node_ids = vec![remote.clone()];
    replication.forbidden_node_ids = vec![node_id.clone()];
    let stat = dfs
        .posix_write_file(
            path,
            &bytes,
            DfsWriteOptions {
                policy: replication,
                replica_candidates: vec![remote.clone()],
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .expect("actual remote content publication");
    let before = dfs
        .metadata_backend()
        .unwrap()
        .receipts_for_version(&stat.object_id, &stat.version)
        .unwrap();
    assert!(!before.is_empty());
    assert!(
        before
            .iter()
            .all(|receipt| receipt.replica_node_id == remote)
    );
    assert!(
        !before
            .iter()
            .any(|receipt| receipt.replica_node_id == node_id)
    );
    let object_before = dfs
        .current_read_object_metadata(&stat.object_id)
        .expect("recorded head");
    assert_eq!(object_before.current_locations, vec![remote.clone()]);
    // An actual verified local manifest does not create a metadata receipt.
    let manifest = dfs
        .content
        .put_object_version(stat.object_id.as_str(), stat.version.as_str(), &bytes)
        .expect("seed real local manifest only");
    assert_eq!(manifest.content_hash, stat.content_hash.unwrap());
    dfs.content
        .debug_remove_loose_blob(&manifest.content_hash)
        .expect("remove actual local blob");
    let reconciliation = dfs
        .reconcile_local_manifests(&stat.object_id)
        .expect("reconcile");
    assert_eq!(reconciliation.refreshed_receipts, 0);
    assert!(reconciliation.missing_current);
    assert_eq!(
        dfs.metadata_backend()
            .unwrap()
            .receipts_for_version(&stat.object_id, &stat.version)
            .unwrap(),
        before,
        "missing local bytes cannot alter the genuine remote receipt or add a local receipt"
    );
    assert_eq!(
        dfs.current_read_object_metadata(&stat.object_id).unwrap(),
        object_before
    );
    assert_eq!(
        dfs.metadata()
            .object(&stat.object_id)
            .expect("object")
            .current_locations,
        vec![remote]
    );
    drop(dfs);
    runtime.close().expect("ordinary native close");
}
