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
    let dfs = NetworkedDfsService::new(ReceiveOnlyTransport::new(node), content);
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
    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    let shard = DfsMetadataShardConfig {
        file_system_id: sifs_file_system_id(),
        shard_id: "root".to_string(),
        primary_node_id: node_id.clone(),
        replica_node_ids: Vec::new(),
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let file_system = DfsFileSystemConfig {
        file_history: Default::default(),
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
fn content_root_catalog_continuations_do_not_repeat_control_plane_refresh() {
    let root = temp_root("content-root-catalog-control-plane-once");
    let primary = NodeId::from_legacy_named("node_catalog_primary").expect("primary");
    let file_system = file_system(primary.clone());
    let file_system_config = DfsFileSystemConfig::from(&file_system);
    let shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "root".to_string(),
        primary_node_id: primary.clone(),
        replica_node_ids: Vec::new(),
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let current_kv = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    current_kv.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .put_file_system_config(&file_system_config)
            .expect("filesystem config");
        control.put_metadata_shard(&shard).expect("metadata shard");
    });

    let mut metadata =
        open_root_local_metadata_backend(root.join("metadata")).expect("metadata backend");
    metadata
        .put_file_system(&file_system)
        .expect("metadata filesystem");
    let mut object = file_metadata(primary.clone());
    object.file_system_id = file_system.file_system_id.clone();
    metadata.put_object(&object).expect("metadata object");
    for index in 0..1_100_u64 {
        metadata
            .put_version(&DfsObjectVersionRecord {
                content_durability: None,
                history_order: 0,
                retained: None,
                file_system_id: file_system.file_system_id.clone(),
                object_id: object.object_id.clone(),
                version: DfsVersion::from_legacy_named(format!(
                    "version_catalog_control_plane_{index:04}"
                ))
                .expect("catalog version"),
                content_hash: Some(format!("blake3:{index:064x}")),
                size_bytes: Some(index),
                locations: vec![primary.clone()],
                created_by_event_id: None,
                created_at: OffsetDateTime::UNIX_EPOCH
                    + time::Duration::seconds(index.try_into().expect("timestamp")),
                visible: true,
            })
            .expect("metadata version");
    }

    let refreshes = Arc::new(AtomicUsize::new(0));
    let control_plane = RefreshingDfsControlPlaneKvClient::new(
        InProcessKvStore::new(),
        current_kv,
        Arc::clone(&refreshes),
    );
    let network = InProcessNetwork::new();
    let content_root = root.join("content");
    let content = LocalContentStore::new(&content_root).expect("content");
    let mut service = NetworkedDfsService::new(network.add_node(primary), content)
        .with_control_plane(control_plane)
        .with_metadata_backend(metadata)
        .expect("metadata service");
    refreshes.store(0, Ordering::SeqCst);

    let catalog = service
        .refresh_content_root_catalog(
            std::slice::from_ref(&file_system_config),
            std::slice::from_ref(&shard),
            &[],
            &content_root,
            &[],
            16,
        )
        .expect("multi-page catalog refresh");
    assert!(catalog.is_complete());
    assert!(
        catalog.fragments()[0].entries.len() > DFS_CONTENT_ROOT_CATALOG_PAGE_ENTRY_LIMIT,
        "fixture must require a continuation page"
    );
    assert_eq!(
        refreshes.load(Ordering::SeqCst),
        3,
        "refresh should read topology before and after the drain and prove authority once, not once per page"
    );

    std::fs::remove_dir_all(root).ok();
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
fn local_content_anti_entropy_accepts_hash_addressed_blob_without_version_manifest() {
    let root = temp_root("local-content-anti-entropy-hash-addressed");
    let node_id = NodeId::from_legacy_named("node_anti_entropy_hash").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("fs_anti_entropy_hash").expect("fs");
    let network = InProcessNetwork::new();
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let metadata =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root.join("metadata"))
            .expect("metadata");
    let mut dfs = NetworkedDfsService::new(network.add_node(node_id.clone()), content)
        .with_metadata_backend(metadata)
        .expect("metadata backend");
    let file_system = DfsFileSystem {
        file_history: Default::default(),
        file_system_id: file_system_id.clone(),
        display_name: "anti entropy hash".to_string(),
        owner_node_id: node_id.clone(),
        subtree_policies: Vec::new(),
    };
    dfs.metadata_backend
        .as_mut()
        .expect("metadata backend")
        .put_file_system(&file_system)
        .expect("filesystem");

    let bytes = b"hash-addressed bytes without matching manifest";
    let seed_manifest = dfs
        .content
        .put_object_version("obj_seed_for_hash_addressed_blob", "version_seed", bytes)
        .expect("seed blob");
    let mut object = file_metadata(node_id.clone());
    object.file_system_id = file_system_id.clone();
    object.object_id =
        DfsObjectId::from_legacy_named("obj_anti_entropy_hash_blob").expect("object");
    object.version = DfsVersion::from_legacy_named("version_hash_addressed").expect("ver");
    object.path = "/anti/hash-addressed.bin".to_string();
    object.current_locations = vec![node_id.clone()];
    object.content_hash = Some(seed_manifest.content_hash.clone());
    object.content_size_bytes = Some(seed_manifest.size_bytes);
    dfs.metadata_backend
        .as_mut()
        .expect("metadata backend")
        .put_object(&object)
        .expect("hash-addressed object metadata");

    let report = dfs
        .run_local_content_anti_entropy(DfsAntiEntropyOptions {
            file_system_id: file_system_id.clone(),
            after: None,
            max_objects: 8,
            max_issues: 8,
            repair_metadata: true,
        })
        .expect("anti entropy");

    assert_eq!(report.scanned_object_count, 1);
    assert_eq!(report.checked_local_location_count, 1);
    assert_eq!(report.stale_local_location_count, 0);
    assert_eq!(report.repaired_location_count, 0);
    assert_eq!(report.issue_count, 0);
    let retained = dfs
        .metadata_backend()
        .expect("metadata backend")
        .get_object(&object.object_id)
        .expect("object")
        .expect("retained object");
    assert!(retained.current_locations.contains(&node_id));

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn add_object_metadata_batch_trusts_supplied_content_size() {
    let root = temp_root("add-object-metadata-batch-trusts-size");
    let owner = NodeId::from_legacy_named("node_batch_size_owner").expect("node");
    let network = InProcessNetwork::new();
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let manifest = content
        .put_object_version(
            "obj_batch_trusted_size",
            "version_batch_trusted_size",
            b"local",
        )
        .expect("local content manifest");
    let metadata =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root.join("metadata"))
            .expect("metadata");
    let mut dfs = NetworkedDfsService::new(network.add_node(owner.clone()), content)
        .with_metadata_backend(metadata)
        .expect("metadata backend");

    let mut object = file_metadata(owner);
    object.object_id = DfsObjectId::from_legacy_named("obj_batch_trusted_size").expect("object");
    object.path = "/services/batch-visible/trusted-size.txt".to_string();
    object.content_hash = Some(manifest.content_hash);
    object.content_size_bytes = Some(128);
    object.version = DfsVersion::from_legacy_named("version_batch_trusted_size").expect("version");

    dfs.add_object_metadata_batch([object.clone()])
        .expect("metadata batch");

    let version = dfs
        .metadata_backend()
        .expect("metadata backend")
        .get_version(&object.object_id, &object.version)
        .expect("version lookup")
        .expect("version");
    assert_eq!(
        version.size_bytes, object.content_size_bytes,
        "metadata-only batch imports should not stat local content first"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn complete_remote_range_content_mismatch_prunes_stale_location() {
    let root = temp_root("remote-range-read-mismatch-prunes-location");
    let reader = NodeId::from_legacy_named("node_range_mismatch_reader").expect("node");
    let source = NodeId::from_legacy_named("node_range_mismatch_source").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_range_mismatch").expect("object");
    let version = DfsVersion::from_legacy_named("version_range_mismatch").expect("version");
    let expected_bytes = b"expected remote range bytes".to_vec();
    let expected_hash = content_hash(&expected_bytes);
    let mut corrupt_bytes = expected_bytes.clone();
    corrupt_bytes[0] ^= 0xff;
    let request_id = "dfs_net_req_range_mismatch".to_string();
    let mut dfs = networked_sifs_with_backend(&root, reader);
    let mut object = file_metadata(source.clone());
    object.object_id = object_id.clone();
    object.version = version.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![source.clone()];
    object.content_hash = Some(expected_hash.clone());
    object.content_size_bytes = Some(expected_bytes.len() as u64);
    dfs.add_object_metadata(object).expect("object metadata");
    dfs.reads.pending_reads.insert(
        request_id.clone(),
        PendingNetworkedRead {
            object_id: object_id.clone(),
            version: version.clone(),
            content_hash: Some(expected_hash.clone()),
            content_size_bytes: Some(expected_bytes.len() as u64),
            range_offset: Some(0),
            range_max_bytes: Some(expected_bytes.len()),
            target_node_id: source.clone(),
            next_retry_at: Instant::now() + DFS_REMOTE_READ_RETRY_DELAY,
            cache_response: false,
        },
    );

    let event = dfs
        .handle_networked_read_response(
            source.clone(),
            DfsReadResponse {
                request_id: request_id.clone(),
                object_id: object_id.clone(),
                result: DfsReadResult::RangeFound(DfsReadRangeData {
                    object_id: object_id.clone(),
                    version,
                    content_hash: Some(expected_hash),
                    offset: 0,
                    total_size_bytes: expected_bytes.len() as u64,
                    bytes: corrupt_bytes,
                }),
            },
        )
        .expect("matching identity and length are accepted without rehashing");

    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::RemoteReadCompleted {
            result: DfsReadResult::RangeFound(_),
            ..
        }
    ));
    assert!(!dfs.reads.pending_reads.contains_key(&request_id));
    assert!(
        dfs.current_read_object_metadata(&object_id)
            .expect("current metadata")
            .current_locations
            .contains(&source)
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn fetch_response_reuses_existing_local_blob_without_rehashing_payload() {
    let root = temp_root("fetch-response-reuses-existing-local-blob");
    let owner = NodeId::from_legacy_named("node_fetch_reuse_owner").expect("node");
    let source = NodeId::from_legacy_named("node_fetch_reuse_source").expect("node");
    let target = NodeId::from_legacy_named("node_fetch_reuse_target").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_fetch_reuse").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let intent_id = "dfs_intent_fetch_reuse".to_string();
    let request_id = "dfs_net_req_fetch_reuse".to_string();
    let bytes = b"already fetched bytes";
    let mut stale_payload = bytes.to_vec();
    stale_payload[0] ^= 0xff;
    stale_payload.extend_from_slice(b" with a different duplicate payload length");
    let mut dfs = NetworkedDfsService::new(
        InProcessNetwork::new().add_node(owner.clone()),
        LocalContentStore::new(root.join("content")).expect("content"),
    );
    let manifest = dfs
        .content
        .put_object_version(object_id.as_str(), version.as_str(), bytes)
        .expect("seed existing local blob");
    dfs.writes.pending_writes.insert(
        intent_id.clone(),
        PendingNetworkedWrite {
            object_id: object_id.clone(),
            version: version.clone(),
            content_hash: manifest.content_hash.clone(),
            size_bytes: bytes.len() as u64,
            foreground_replica_repair: true,
            patch: None,
            stage_retries: BTreeMap::from([(
                target.clone(),
                RepairRetryState {
                    attempts: 2,
                    next_retry_tick: 100,
                    inflight: true,
                },
            )]),
            repair_retries: BTreeMap::new(),
            stage_inflight_request_ids: BTreeMap::new(),
            repair_inflight_request_ids: BTreeMap::new(),
            fetch_inflight_from: BTreeSet::from([source.clone()]),
            fetch_inflight_request_ids: BTreeMap::from([(source.clone(), request_id.clone())]),
            fetch_inflight_started_ticks: BTreeMap::from([(source.clone(), 0)]),
            fetch_failed_from: BTreeSet::new(),
        },
    );

    let event = dfs
        .handle_fetch_blob_response(
            source.clone(),
            DfsFetchBlobResponse {
                request_id,
                intent_id: intent_id.clone(),
                object_id: object_id.clone(),
                version: version.clone(),
                source_node_id: source.clone(),
                result: DfsFetchBlobResult::Found {
                    content_hash: manifest.content_hash.clone(),
                    bytes: stale_payload,
                },
            },
        )
        .expect("existing local blob should satisfy fetch");

    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::BlobFetched {
            source_node_id,
            ..
        } if source_node_id == source
    ));
    let pending = dfs
        .writes
        .pending_writes
        .get(&intent_id)
        .expect("pending write");
    assert!(!pending.fetch_inflight_from.contains(&source));
    assert!(!pending.fetch_inflight_request_ids.contains_key(&source));
    assert!(pending.fetch_failed_from.is_empty());
    assert_eq!(
        pending
            .stage_retries
            .get(&target)
            .expect("stage retry")
            .next_retry_tick,
        0
    );
    assert_eq!(
        dfs.content
            .read_blob(&manifest.content_hash)
            .expect("existing blob"),
        bytes
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn rejected_streamed_stage_does_not_reserve_version_for_later_valid_content() {
    let root = temp_root("rejected-streamed-stage-does-not-reserve-version");
    let owner = NodeId::from_legacy_named("node_streamed_stage_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_streamed_stage_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_streamed_stage").expect("object");
    let stale_version =
        DfsVersion::from_legacy_named("version_streamed_stage_stale").expect("stale version");
    let version_1 =
        DfsVersion::from_legacy_named("version_streamed_stage_1").expect("base version");
    let version_2 =
        DfsVersion::from_legacy_named("version_streamed_stage_2").expect("target version");
    let mut dfs = networked_sifs_with_backend(&root, replica.clone());
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.current_locations = vec![owner.clone()];
    dfs.add_object_metadata(metadata).expect("metadata");

    let rejected_bytes = b"stale streamed stage bytes";
    let rejected_hash = content_hash(rejected_bytes);
    let mut rejected_ingest = dfs
        .content
        .begin_quarantined_blob_ingest(rejected_bytes.len() as u64, Some(rejected_hash.as_str()))
        .expect("begin rejected quarantine");
    rejected_ingest
        .write_chunk(rejected_bytes)
        .expect("write rejected quarantine");
    let rejected_blob = rejected_ingest
        .finish_quarantined()
        .expect("finish rejected quarantine");
    let rejected = dfs
        .handle_preingested_stage_blob(
            owner.clone(),
            DfsStageBlobCommand {
                request_id: "dfs_streamed_stage_rejected".to_string(),
                intent_id: "dfs_streamed_stage_rejected_intent".to_string(),
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                expected_base_version: Some(stale_version),
                version: version_2.clone(),
                content_hash: rejected_hash,
                bytes: Vec::new(),
                object_metadata: None,
                authority_shard: None,
            },
            rejected_blob,
        )
        .expect("rejected stage response event");
    assert!(matches!(
        rejected,
        NetworkedDfsServiceEvent::BlobStageRejected { .. }
    ));
    assert!(
        dfs.content
            .read_manifest(object_id.as_str(), version_2.as_str())
            .is_err(),
        "rejected streamed stage must not reserve its requested version"
    );

    let valid_bytes = b"later valid streamed stage bytes";
    let valid_hash = content_hash(valid_bytes);
    let mut valid_ingest = dfs
        .content
        .begin_quarantined_blob_ingest(valid_bytes.len() as u64, Some(valid_hash.as_str()))
        .expect("begin valid quarantine");
    valid_ingest
        .write_chunk(valid_bytes)
        .expect("write valid quarantine");
    let valid_blob = valid_ingest
        .finish_quarantined()
        .expect("finish valid quarantine");
    let valid = dfs
        .handle_preingested_stage_blob(
            owner,
            DfsStageBlobCommand {
                request_id: "dfs_streamed_stage_valid".to_string(),
                intent_id: "dfs_streamed_stage_valid_intent".to_string(),
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                expected_base_version: Some(version_1),
                version: version_2.clone(),
                content_hash: valid_hash.clone(),
                bytes: Vec::new(),
                object_metadata: None,
                authority_shard: None,
            },
            valid_blob,
        )
        .expect("valid stage response event");
    assert!(matches!(valid, NetworkedDfsServiceEvent::BlobStaged { .. }));
    let manifest = dfs
        .content
        .read_manifest(object_id.as_str(), version_2.as_str())
        .expect("valid stage manifest");
    assert_eq!(manifest.content_hash, valid_hash);
    assert_eq!(
        dfs.content
            .read_blob(&manifest.content_hash)
            .expect("valid stage blob"),
        valid_bytes
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn stage_blob_persists_when_response_target_is_unroutable() {
    let root = temp_root("stage-response-target-unroutable");
    let owner = NodeId::from_legacy_named("node_stage_notify_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_stage_notify_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_stage_notify").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"stage durable despite lost requester";
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.current_locations = vec![owner.clone()];
    let mut dfs = NetworkedDfsService::new(
        ReceiveOnlyTransport::new(replica.clone()),
        LocalContentStore::new(root.join("content")).expect("content"),
    );

    let event = dfs
        .handle_stage_blob(
            owner,
            DfsStageBlobCommand {
                request_id: "dfs_net_req_stage_unroutable".to_string(),
                intent_id: "dfs_intent_stage_unroutable".to_string(),
                file_system_id: metadata.file_system_id.clone(),
                object_id: object_id.clone(),
                expected_base_version: Some(metadata.version.clone()),
                version: version.clone(),
                content_hash: content_hash(bytes),
                bytes: bytes.to_vec(),
                object_metadata: Some(metadata),
                authority_shard: None,
            },
        )
        .expect("unroutable response target should not fail durable stage");

    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::BlobStaged {
            object_id: staged_object_id,
            version: staged_version,
            replica_node_id,
            ..
        } if staged_object_id == object_id
            && staged_version == version
            && replica_node_id == replica
    ));
    let manifest = dfs
        .content
        .read_manifest(object_id.as_str(), version.as_str())
        .expect("stage manifest");
    assert_eq!(
        dfs.content.read_blob(&manifest.content_hash).expect("blob"),
        bytes
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn reconcile_local_manifests_requires_current_blob_bytes() {
    let root = temp_root("reconcile-requires-current-blob");
    let node_id = NodeId::from_legacy_named("node_reconcile_missing_blob").expect("node");
    let remote = NodeId::from_legacy_named("node_reconcile_remote").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_reconcile_missing_blob").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = vec![b'r'; 17 * 1024];
    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    let manifest = dfs
        .content
        .put_object_version(object_id.as_str(), version.as_str(), &bytes)
        .expect("seed manifest");
    let hex = manifest
        .content_hash
        .strip_prefix("blake3:")
        .expect("blake3 hash");
    std::fs::remove_file(root.join("content").join("blobs").join(&hex[..2]).join(hex))
        .expect("remove blob");
    let mut metadata = file_metadata(node_id);
    metadata.object_id = object_id.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.version = version.clone();
    metadata.current_locations = vec![remote.clone()];
    metadata.content_hash = Some(manifest.content_hash.clone());
    metadata.content_size_bytes = Some(bytes.len() as u64);
    dfs.add_object_metadata(metadata).expect("metadata");

    let reconciliation = dfs
        .reconcile_local_manifests(&object_id)
        .expect("reconcile");

    assert_eq!(reconciliation.refreshed_receipts, 0);
    assert!(reconciliation.conflicts.is_empty());
    assert!(reconciliation.missing_current);
    assert!(
        dfs.metadata_backend()
            .expect("backend")
            .receipts_for_version(&object_id, &version)
            .expect("receipts")
            .is_empty()
    );
    assert_eq!(
        dfs.metadata()
            .object(&object_id)
            .expect("object")
            .current_locations,
        vec![remote]
    );
    std::fs::remove_dir_all(root).ok();
}
