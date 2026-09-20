//! Retention regression tests.

use super::*;

#[test]
fn lifecycle_maintenance_runs_version_blob_and_tombstone_collection() {
    let root = temp_root("lifecycle-maintenance-runs-collectors");
    let node_id = NodeId::from_legacy_named("node_lifecycle_maintenance").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_lifecycle_tombstone").expect("object");
    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    let version_1 = dfs
        .content
        .put_object_version("object-lifecycle", "version_1", b"history bytes v1")
        .expect("put version 1");
    let version_2 = dfs
        .content
        .put_object_version("object-lifecycle", "version_2", b"history bytes v2")
        .expect("put version 2");

    let mut tombstone = file_metadata(node_id.clone());
    tombstone.object_id = object_id.clone();
    tombstone.kind = DfsObjectKind::Tombstone;
    tombstone.path = "/services/lifecycle/deleted.txt".to_string();
    tombstone.content_hash = None;
    tombstone.content_size_bytes = None;
    tombstone.changed_at_unix_nanos = 10;
    dfs.metadata_backend
        .as_mut()
        .expect("metadata backend")
        .put_object(&tombstone)
        .expect("tombstone metadata");

    let catalog_shard = DfsMetadataShardConfig {
        file_system_id: sifs_file_system_id(),
        shard_id: "root".to_string(),
        primary_node_id: node_id,
        replica_node_ids: Vec::new(),
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let catalog_file_system = DfsFileSystemConfig {
        file_history: Default::default(),
        file_system_id: catalog_shard.file_system_id.clone(),
        display_name: "Lifecycle catalog filesystem".to_string(),
        owner_node_id: catalog_shard.primary_node_id.clone(),
        subtree_policies: Vec::new(),
    };
    let catalog_validation = |generation: &str, generated_at_unix_nanos: u64| {
        let topology_digest = dfs_content_root_catalog_topology_digest_with_file_systems(
            std::slice::from_ref(&catalog_file_system),
            std::slice::from_ref(&catalog_shard),
        )
        .expect("catalog topology");
        let state_fingerprint = format!(
            "dfs-metadata-prefix-v2:blake3:{}",
            blake3::hash(generation.as_bytes()).to_hex()
        );
        let identity = DfsContentRootCatalogShardIdentity::from_config(&catalog_shard);
        let catalog = DfsContentRootCatalog::from_fragments(
            topology_digest.clone(),
            vec![identity.clone()],
            generated_at_unix_nanos,
            vec![DfsContentRootCatalogFragment {
                identity,
                topology_digest,
                generated_at_unix_nanos,
                coverage: DfsContentRootCatalogCoverage::Complete {
                    prefix_proof_version: DFS_CONTENT_ROOT_CATALOG_PREFIX_PROOF_VERSION,
                    journal_sequence: 1,
                    state_fingerprint,
                },
                entries: Vec::new(),
            }],
        )
        .expect("content-root catalog");
        let installed = default_dfs_content_root_catalog_store(root.join("content"))
            .install_test_catalog_at(catalog, generated_at_unix_nanos)
            .expect("install content-root catalog");
        installed
            .validate_for_collection_with_head_watermark_policy(
                std::slice::from_ref(&catalog_file_system),
                std::slice::from_ref(&catalog_shard),
                &[],
                &[],
                false,
                u64::MAX,
                Duration::MAX,
            )
            .expect("validate content-root catalog")
    };
    let serialized_validation = serde_json::to_value(catalog_validation(
        "serialized-lifecycle-catalog",
        u64::MAX - 3,
    ))
    .expect("serialize lifecycle catalog validation");
    let replayed_validation: DfsContentRootCatalogCollectionValidation =
        serde_json::from_value(serialized_validation)
            .expect("deserialize lifecycle catalog validation");
    assert!(
        !replayed_validation.is_collection_authorized(),
        "a serialized validation report must not recreate destructive-GC authority"
    );
    let replayed_report = dfs
        .run_lifecycle_maintenance(DfsLifecycleMaintenanceOptions {
            content_root_catalog_validation: Some(replayed_validation),
            version_gc: Some(ContentStoreVersionGcOptions::delete_eligible(
                Duration::ZERO,
                1,
            )),
            orphan_gc: Some(ContentStoreGcOptions::delete_eligible(Duration::ZERO)),
            ..DfsLifecycleMaintenanceOptions::default()
        })
        .expect("lifecycle maintenance with replayed validation report");
    assert!(replayed_report.content_gc_metadata_protection_unavailable);
    assert!(replayed_report.version_gc.is_none());
    assert!(replayed_report.orphan_gc.is_none());

    let report = dfs
        .run_lifecycle_maintenance(DfsLifecycleMaintenanceOptions {
            scrub_content: true,
            stale_staging_gc: None,
            anti_entropy: None,
            metadata_history_gc: None,
            content_root_catalog_validation: Some(catalog_validation(
                "lifecycle-catalog-1",
                u64::MAX - 2,
            )),
            content_reservation_reconciliation: Some(
                ContentStoreReservationReconciliationOptions::bounded(0, Duration::ZERO, u64::MAX)
                    .with_catalog_generation("caller-catalog-generation-is-overridden"),
            ),
            version_gc: Some(ContentStoreVersionGcOptions::delete_eligible(
                std::time::Duration::from_nanos(0),
                1,
            )),
            orphan_gc: Some(ContentStoreGcOptions::delete_eligible(
                std::time::Duration::from_nanos(0),
            )),
            tombstone_compaction: Some(
                DfsTombstoneCompactionOptions::compact_eligible(std::time::Duration::from_nanos(
                    50,
                ))
                .with_now_unix_nanos(100),
            ),
        })
        .expect("lifecycle maintenance");

    assert!(
        report
            .content_scrub
            .as_ref()
            .expect("content scrub")
            .is_healthy()
    );
    let version_gc = report.version_gc.expect("version gc");
    assert_eq!(version_gc.deleted_version_count, 0);
    assert_eq!(version_gc.newly_observed_absent_version_count, 1);
    assert_eq!(version_gc.retained_latest_version_count, 1);
    let orphan_gc = report.orphan_gc.expect("orphan gc");
    assert_eq!(orphan_gc.deleted_orphan_count, 0);
    assert_eq!(orphan_gc.newly_observed_orphan_count, 0);
    assert!(
        dfs.content
            .has_blob(&version_1.content_hash)
            .expect("v1 retained for durable orphan grace")
    );
    assert!(
        dfs.content
            .has_blob(&version_2.content_hash)
            .expect("v2 retained")
    );

    assert!(report.metadata_backend_configured);
    assert!(report.tombstone_compaction_requested);
    let tombstone_report = report.tombstone_compaction.expect("tombstone compaction");
    assert_eq!(tombstone_report.compacted_tombstone_count, 1);

    let second_observation = dfs
        .run_lifecycle_maintenance(DfsLifecycleMaintenanceOptions {
            content_root_catalog_validation: Some(catalog_validation(
                "lifecycle-catalog-2",
                u64::MAX - 1,
            )),
            version_gc: Some(ContentStoreVersionGcOptions::delete_eligible(
                Duration::ZERO,
                1,
            )),
            orphan_gc: Some(ContentStoreGcOptions::delete_eligible(Duration::ZERO)),
            ..DfsLifecycleMaintenanceOptions::default()
        })
        .expect("second catalog lifecycle maintenance");
    assert_eq!(
        second_observation
            .version_gc
            .expect("second-observation version gc")
            .deleted_version_count,
        1
    );
    assert_eq!(
        second_observation
            .orphan_gc
            .expect("first orphan observation")
            .newly_observed_orphan_count,
        1
    );
    let follow_up = dfs
        .run_lifecycle_maintenance(DfsLifecycleMaintenanceOptions {
            content_root_catalog_validation: Some(catalog_validation(
                "lifecycle-catalog-3",
                u64::MAX,
            )),
            orphan_gc: Some(ContentStoreGcOptions::delete_eligible(
                std::time::Duration::ZERO,
            )),
            ..DfsLifecycleMaintenanceOptions::default()
        })
        .expect("follow-up lifecycle maintenance");
    assert_eq!(
        follow_up
            .orphan_gc
            .expect("follow-up orphan gc")
            .deleted_orphan_count,
        1
    );
    assert!(
        !dfs.content
            .has_blob(&version_1.content_hash)
            .expect("v1 reclaimed after durable orphan observation")
    );
    let backend = dfs.metadata_backend().expect("metadata backend");
    assert_eq!(
        backend.get_object(&object_id).expect("compacted object"),
        None
    );
    assert_eq!(
        backend
            .get_object_by_path(&sifs_file_system_id(), "/services/lifecycle/deleted.txt")
            .expect("path marker")
            .expect("compacted tombstone marker")
            .kind,
        DfsObjectKind::Tombstone
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn lifecycle_maintenance_reports_skipped_tombstones_without_backend() {
    let root = temp_root("lifecycle-maintenance-no-backend");
    let node_id = NodeId::from_legacy_named("node_lifecycle_no_backend").expect("node");
    let network = InProcessNetwork::new();
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(root.join("content")).expect("content"),
    );

    let report = dfs
        .run_lifecycle_maintenance(DfsLifecycleMaintenanceOptions {
            tombstone_compaction: Some(DfsTombstoneCompactionOptions::compact_eligible(
                std::time::Duration::from_nanos(0),
            )),
            ..DfsLifecycleMaintenanceOptions::default()
        })
        .expect("lifecycle maintenance without backend");

    assert!(!report.metadata_backend_configured);
    assert!(report.tombstone_compaction_requested);
    assert!(report.tombstone_compaction.is_none());
    assert!(report.content_scrub.is_none());
    assert!(report.version_gc.is_none());
    assert!(report.orphan_gc.is_none());

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn lifecycle_shared_gc_runs_once_without_file_system_records() {
    let root = temp_root("lifecycle-shared-gc-without-file-systems");
    let node_id = NodeId::from_legacy_named("node_lifecycle_empty_catalog").expect("node");
    let metadata =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root.join("metadata"))
            .expect("metadata backend");
    assert!(
        metadata
            .list_file_systems()
            .expect("empty filesystem catalog")
            .is_empty()
    );
    let mut dfs = NetworkedDfsService::new(
        InProcessNetwork::new().add_node(node_id),
        LocalContentStore::new(root.join("content")).expect("content store"),
    )
    .with_metadata_backend(metadata)
    .expect("attach metadata backend");
    let mut host = RecordingMaintenanceHost::default();

    let reports = run_dfs_lifecycle_maintenance_with_host(
        &mut dfs,
        &mut host,
        DfsMaintenanceRunOptions {
            cleanup_stale_staging: true,
            stale_staging_min_age: Duration::ZERO,
            compact_tombstones: true,
            tombstone_compaction_min_age: Duration::ZERO,
            collect_metadata_history: true,
            metadata_history_gc_min_age: Duration::ZERO,
            collect_content_versions: true,
            content_version_gc_min_age: Duration::ZERO,
            collect_orphan_content: true,
            content_orphan_gc_min_age: Duration::ZERO,
            ..DfsMaintenanceRunOptions::default()
        },
    )
    .expect("shared lifecycle maintenance");

    assert_eq!(
        reports.len(),
        1,
        "shared maintenance must execute exactly once without filesystem-scoped anti-entropy work"
    );
    let report = &reports[0];
    assert!(report.stale_staging_gc.is_some());
    assert!(report.metadata_history_gc.is_some());
    assert!(report.version_gc.is_none());
    assert!(report.orphan_gc.is_none());
    assert!(report.content_gc_metadata_protection_unavailable);
    assert!(report.tombstone_compaction.is_some());
    assert!(report.anti_entropy.is_none());
    assert!(host.stored.is_empty());
    assert!(host.cleared.is_empty());

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn deferred_write_drain_reports_the_primary_rejection() {
    let root = temp_root("deferred-write-rejection");
    let requester = NodeId::generate();
    let primary = NodeId::generate();
    let object_id = DfsObjectId::generate();
    let shard = test_routed_authority_shard(&primary, 1);
    let mut dfs = NetworkedDfsService::new(
        FailingSendTransport {
            local_node_id: requester.clone(),
        },
        LocalContentStore::new(root.join("content")).unwrap(),
    )
    .with_control_plane(test_routed_authority_control_plane(&shard));
    dfs.writes.pending_routed_writes.insert(
        "deferred-rejected".into(),
        test_pending_routed_write(
            "deferred-rejected",
            &requester,
            &object_id,
            &primary,
            shard.clone(),
        ),
    );
    dfs.writes.pending_routed_writes.insert(
        "still-in-flight".into(),
        test_pending_routed_write(
            "still-in-flight",
            &requester,
            &DfsObjectId::generate(),
            &primary,
            shard,
        ),
    );
    dfs.handle_distributed_write_response(
        primary,
        DfsDistributedWriteResponse {
            request_id: "deferred-rejected".into(),
            object_id,
            result: DfsDistributedWriteResult::Rejected {
                reason: "authoritative write precondition failed".into(),
                current_primary_node_id: None,
            },
            metadata_journal_batches: Vec::new(),
        },
    )
    .unwrap();
    let error = dfs
        .process_incoming_until_pending_routed_writes_ready(Duration::from_millis(10))
        .expect_err("a terminal rejection cannot be reported as a successful drain");
    assert!(
        error
            .to_string()
            .contains("authoritative write precondition failed"),
        "{error}"
    );
    assert_eq!(dfs.writes.pending_routed_writes.len(), 1);
    assert!(
        dfs.writes.pending_routed_writes["still-in-flight"]
            .command
            .is_none(),
        "keep the other operation's late-response identity while releasing its retry body"
    );
}

#[test]
fn pending_routed_write_timeout_compacts_request_and_publishes_late_commit_manifest() {
    let requester =
        NodeId::from_legacy_named("node_routed_write_ready_requester").expect("requester");
    let primary = NodeId::from_legacy_named("node_routed_write_ready_primary").expect("primary");
    let alias_object_id =
        DfsObjectId::from_legacy_named("obj_routed_write_ready_alias").expect("alias object");
    let target_object_id =
        DfsObjectId::from_legacy_named("obj_routed_write_ready_target").expect("target object");
    let version = DfsVersion::from_legacy_named("version_routed_write_ready").expect("version");
    let root = temp_root("routed-write-ready-final-result");
    let authority_shard = test_routed_authority_shard(&primary, 1);
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let local_bytes = b"late routed write manifest publication bytes";
    let staged = content
        .put_object_version_blob_only(
            "obj_routed_write_ready_staging",
            "version_routed_write_ready_staging",
            local_bytes,
        )
        .expect("pre-stage requester blob");
    let mut dfs = NetworkedDfsService::new(
        FailingSendTransport {
            local_node_id: requester.clone(),
        },
        content,
    )
    .with_control_plane(test_routed_authority_control_plane(&authority_shard));
    let mut target = file_metadata(requester.clone());
    target.object_id = target_object_id.clone();
    target.path = "/services/routed-write-ready-target".to_string();
    dfs.add_transient_object_metadata(target)
        .expect("target metadata");
    let mut alias = file_metadata(requester.clone());
    alias.object_id = alias_object_id.clone();
    alias.path = "/services/routed-write-ready-alias".to_string();
    alias.kind = DfsObjectKind::Hardlink;
    alias.link_target_object_id = Some(target_object_id.clone());
    dfs.add_transient_object_metadata(alias)
        .expect("hardlink metadata");
    let mut pending = test_pending_routed_write(
        "dfs_net_req_1",
        &requester,
        &alias_object_id,
        &primary,
        authority_shard,
    );
    let command = pending.command.as_mut().expect("full retry command");
    command.bytes = vec![0x5a; 1024 * 1024];
    command.content_hash = Some(staged.content_hash.clone());
    command.content_size_bytes = Some(local_bytes.len() as u64);
    command.requester_blob_pre_staged = true;
    dfs.writes
        .pending_routed_writes
        .insert("dfs_net_req_1".to_string(), pending);

    dfs.handle_distributed_write_response(
        primary.clone(),
        DfsDistributedWriteResponse {
            request_id: "dfs_net_req_1".to_string(),
            object_id: alias_object_id.clone(),
            result: DfsDistributedWriteResult::Outcome {
                outcome: NetworkedDfsWriteOutcome::Pending {
                    intent_id: "intent_remote_alias".to_string(),
                    durable_receipts: 1,
                    required_receipts: 2,
                },
            },
            metadata_journal_batches: Vec::new(),
        },
    )
    .expect("pending alias response");

    let error = dfs
        .process_incoming_until_pending_routed_writes_ready(Duration::from_millis(1))
        .expect_err("remote intent alias alone must not mark a routed write ready");
    assert!(
        matches!(error, DfsError::Network { ref message } if message.contains("routed write responses did not arrive")),
        "unexpected readiness error: {error}"
    );
    assert!(
        !dfs.writes.pending_routed_writes.is_empty()
            && dfs
                .writes
                .pending_routed_writes
                .values()
                .all(|pending| pending.command.is_none()),
        "a timed-out routed write must release every body while retaining late-response identity"
    );

    let event = dfs
        .handle_distributed_write_response(
            primary,
            DfsDistributedWriteResponse {
                request_id: "dfs_net_req_1".to_string(),
                object_id: alias_object_id.clone(),
                result: DfsDistributedWriteResult::Outcome {
                    outcome: NetworkedDfsWriteOutcome::Committed {
                        intent_id: "intent_remote_alias".to_string(),
                        version: version.clone(),
                        content_hash: staged.content_hash.clone(),
                        repair_jobs: Vec::new(),
                    },
                },
                metadata_journal_batches: Vec::new(),
            },
        )
        .expect("late canonical final response");
    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::RemoteWriteCompleted { .. }
    ));
    let manifest = dfs
        .content
        .read_manifest(target_object_id.as_str(), version.as_str())
        .expect("late commit publishes canonical hardlink-target manifest");
    assert_eq!(manifest.content_hash, staged.content_hash);
    assert_eq!(manifest.size_bytes, local_bytes.len() as u64);
    assert!(
        dfs.content
            .read_manifest(alias_object_id.as_str(), version.as_str())
            .is_err(),
        "manifest publication must follow the compact receipt through the current hardlink identity"
    );
    assert!(matches!(
        dfs.take_write_outcome("intent_remote_alias")
            .expect("late completion outcome"),
        NetworkedDfsWriteOutcome::Committed { .. }
    ));
    assert!(dfs.writes.pending_routed_writes.is_empty());

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn compact_routed_write_late_response_ledger_is_bounded_and_expires_aliases() {
    let requester =
        NodeId::from_legacy_named("node_routed_write_bound_requester").expect("requester");
    let primary = NodeId::from_legacy_named("node_routed_write_bound_primary").expect("primary");
    let object_id = DfsObjectId::from_legacy_named("obj_routed_write_bound").expect("object");
    let root = temp_root("routed-write-late-response-bound");
    let authority_shard = test_routed_authority_shard(&primary, 1);
    let mut dfs = NetworkedDfsService::new(
        FailingSendTransport {
            local_node_id: requester.clone(),
        },
        LocalContentStore::new(root.join("content")).expect("content"),
    )
    .with_control_plane(test_routed_authority_control_plane(&authority_shard));
    let retained_at = Instant::now();
    let request_count =
        crate::service_routed_write::DFS_ROUTED_WRITE_LATE_RESPONSE_MAX_REQUESTS.saturating_add(1);
    for index in 0..request_count {
        let request_id = format!("dfs_net_req_bound_{index:04}");
        let mut pending = test_pending_routed_write(
            &request_id,
            &requester,
            &object_id,
            &primary,
            authority_shard.clone(),
        );
        pending.command = None;
        pending.compact_retained_at = Some(retained_at + Duration::from_nanos(index as u64));
        dfs.writes.pending_routed_writes.insert(request_id, pending);
    }

    dfs.prune_retained_routed_writes();
    assert_eq!(
        dfs.writes.pending_routed_writes.len(),
        crate::service_routed_write::DFS_ROUTED_WRITE_LATE_RESPONSE_MAX_REQUESTS
    );
    assert!(
        !dfs.writes
            .pending_routed_writes
            .contains_key("dfs_net_req_bound_0000")
    );

    let canonical_request_id = "dfs_net_req_bound_0001";
    let mut alias = dfs.writes.pending_routed_writes[canonical_request_id].clone();
    alias.command = None;
    alias.canonical_request_id = canonical_request_id.to_string();
    dfs.writes
        .pending_routed_writes
        .insert("intent_routed_write_bound_alias".to_string(), alias);
    let expired_at = Instant::now()
        - crate::service_routed_write::DFS_ROUTED_WRITE_LATE_RESPONSE_TTL
        - Duration::from_millis(1);
    for pending in dfs.writes.pending_routed_writes.values_mut() {
        pending.compact_retained_at = Some(expired_at);
    }

    dfs.prune_retained_routed_writes();
    assert!(
        dfs.writes.pending_routed_writes.is_empty(),
        "expired canonical requests and every alias must be released together"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn stat_prefers_backend_tombstone_over_stale_memory_file() {
    let root = temp_root("stat-prefers-backend-tombstone");
    let owner = NodeId::from_legacy_named("node_stat_tombstone_owner").expect("owner");
    let object_id = DfsObjectId::from_legacy_named("obj_stat_tombstone").expect("object");
    let path = "/stat-tombstone.txt";
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let mut tombstone = file_metadata(owner.clone());
    tombstone.object_id = object_id.clone();
    tombstone.path = path.to_string();
    tombstone.kind = DfsObjectKind::Tombstone;
    tombstone.version = DfsVersion::from_legacy_named("version_2").expect("version");
    tombstone.content_hash = None;
    tombstone.content_size_bytes = None;
    tombstone.current_locations = vec![owner.clone()];
    dfs.put_visible_object_in_metadata_backend(&tombstone)
        .expect("backend tombstone");

    let mut stale_memory = tombstone.clone();
    stale_memory.kind = DfsObjectKind::File;
    stale_memory.version = DfsVersion::from_legacy_named("version_3").expect("version");
    stale_memory.content_hash = Some("blake3:stale-memory-file".to_string());
    stale_memory.content_size_bytes = Some(12);
    dfs.metadata
        .add_object(stale_memory)
        .expect("stale memory file");

    let error = dfs
        .posix_stat_distributed(path)
        .expect_err("backend tombstone must hide stale memory file");

    assert!(matches!(error, DfsPosixError::NotFound { .. }));
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn tombstone_tracking_ignores_stale_live_path_metadata() {
    let root = temp_root("tombstone-ignores-stale-live");
    let owner = NodeId::from_legacy_named("node_tombstone_stale_live").expect("owner");
    let path = "/tombstone-stale-live.txt";
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    dfs.tombstones_by_metadata_path.clear();
    let stale_version = DfsVersion::new("00000000000000000000000001").expect("version");
    let tombstone_version = DfsVersion::new("00000000000000000000000002").expect("version");
    let fresh_version = DfsVersion::new("00000000000000000000000003").expect("version");

    let mut tombstone = file_metadata(owner.clone());
    tombstone.object_id =
        DfsObjectId::from_legacy_named("obj_tombstone_stale_live_delete").expect("object");
    tombstone.path = path.to_string();
    tombstone.kind = DfsObjectKind::Tombstone;
    tombstone.version = tombstone_version;
    dfs.track_metadata_tombstone(&tombstone);

    let key = (sifs_file_system_id(), metadata_path(path));
    assert!(
        dfs.tombstones_by_metadata_path.contains_key(&key),
        "newer tombstone should be tracked for the deleted path"
    );

    let mut stale_live = file_metadata(owner.clone());
    stale_live.object_id =
        DfsObjectId::from_legacy_named("obj_tombstone_stale_live_old").expect("object");
    stale_live.path = path.to_string();
    stale_live.version = stale_version;
    dfs.track_hydrated_metadata_tombstone(&stale_live);
    assert!(
        dfs.tombstones_by_metadata_path.contains_key(&key),
        "older live metadata must not clear a newer tombstone"
    );

    let mut fresh_live = stale_live;
    fresh_live.object_id =
        DfsObjectId::from_legacy_named("obj_tombstone_stale_live_fresh").expect("object");
    fresh_live.version = fresh_version;
    dfs.track_hydrated_metadata_tombstone(&fresh_live);
    assert!(
        !dfs.tombstones_by_metadata_path.contains_key(&key),
        "newer live metadata should clear the older tombstone"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn hydrated_tombstone_tracking_is_incremental_and_live_path_aware() {
    let root = temp_root("hydrate-incremental-tombstones");
    let node_id = NodeId::from_legacy_named("node_hydrate_tombstones").expect("node");
    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.metadata = DfsMetadataStore::new();
    dfs.metadata.add_file_system(file_system(node_id.clone()));
    dfs.tombstones_by_metadata_path.clear();

    let mut live_first = file_metadata(node_id.clone());
    live_first.object_id =
        DfsObjectId::from_legacy_named("obj_hydrate_live_first").expect("object");
    live_first.path = "/users/hydrate/live-first.txt".to_string();
    dfs.metadata
        .add_object(live_first.clone())
        .expect("live object");

    let mut late_tombstone = file_metadata(node_id.clone());
    late_tombstone.object_id =
        DfsObjectId::from_legacy_named("obj_hydrate_late_tombstone").expect("object");
    late_tombstone.kind = DfsObjectKind::Tombstone;
    late_tombstone.path = live_first.path.clone();
    dfs.track_hydrated_metadata_tombstone(&late_tombstone);
    assert!(
        !dfs.tombstones_by_metadata_path
            .contains_key(&(sifs_file_system_id(), metadata_path(&late_tombstone.path))),
        "hydration must not retain a tombstone for a path already owned by a live object"
    );

    let mut early_tombstone = file_metadata(node_id.clone());
    early_tombstone.object_id =
        DfsObjectId::from_legacy_named("obj_hydrate_early_tombstone").expect("object");
    early_tombstone.kind = DfsObjectKind::Tombstone;
    early_tombstone.path = "/users/hydrate/live-after-tombstone.txt".to_string();
    dfs.track_hydrated_metadata_tombstone(&early_tombstone);
    assert!(
        dfs.tombstones_by_metadata_path
            .contains_key(&(sifs_file_system_id(), metadata_path(&early_tombstone.path))),
        "a tombstone should be tracked until hydration sees a live object for the path"
    );

    let mut live_later = file_metadata(node_id);
    live_later.object_id =
        DfsObjectId::from_legacy_named("obj_hydrate_live_later").expect("object");
    live_later.path = early_tombstone.path.clone();
    dfs.track_hydrated_metadata_tombstone(&live_later);
    assert!(
        !dfs.tombstones_by_metadata_path
            .contains_key(&(sifs_file_system_id(), metadata_path(&early_tombstone.path))),
        "a later live object should clear a previously tracked hydrated tombstone"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn hydrate_from_metadata_backend_reuses_older_compacted_tombstone_path() {
    let root = temp_root("hydrate-reuses-older-compacted-tombstone-path");
    let owner = NodeId::from_legacy_named("node_hydrate_ready_new_primary").expect("node");
    let requester = NodeId::from_legacy_named("node_hydrate_ready_new_requester").expect("node");
    let replica = NodeId::from_legacy_named("node_hydrate_ready_new_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_hydrate_ready_new").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"commit ready new object after restart";
    let network = InProcessNetwork::new();
    let metadata_root = root.join("metadata");
    let file_system = file_system(owner.clone());
    let control_plane = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    control_plane.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(&file_system))
            .expect("filesystem config");
        control
            .put_metadata_shard(&DfsMetadataShardConfig {
                file_system_id: file_system.file_system_id.clone(),
                shard_id: "owner".to_string(),
                primary_node_id: owner.clone(),
                replica_node_ids: vec![requester.clone(), replica.clone()],
                epoch: 1,
                assignment: DfsMetadataShardAssignment::Root,
                namespace_prepare_codec: Default::default(),
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            })
            .expect("metadata shard");
    });
    let mut owner_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
            .expect("owner backend");
    let path = "/services/hydrate-ready-new.txt";
    let mut old_tombstone = file_metadata(owner.clone());
    let old_tombstone_object_id =
        DfsObjectId::from_legacy_named("obj_hydrate_ready_old_tombstone").expect("object");
    old_tombstone.object_id = old_tombstone_object_id.clone();
    old_tombstone.path = path.to_string();
    old_tombstone.kind = DfsObjectKind::Tombstone;
    old_tombstone.content_hash = None;
    old_tombstone.content_size_bytes = None;
    old_tombstone.created_at_unix_nanos = 1;
    old_tombstone.changed_at_unix_nanos = 2;
    owner_backend
        .put_file_system(&file_system)
        .expect("filesystem");
    owner_backend
        .put_object(&old_tombstone)
        .expect("old tombstone");
    owner_backend
        .compact_tombstones(
            DfsTombstoneCompactionOptions::compact_eligible(Duration::ZERO)
                .with_now_unix_nanos(100),
        )
        .expect("compact old tombstone");
    assert_eq!(
        owner_backend
            .get_object_by_path(&sifs_file_system_id(), path)
            .expect("compacted path lookup"),
        Some(old_tombstone),
        "test setup must retain only an older compacted tombstone path marker"
    );
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_control_plane(control_plane.clone())
    .with_metadata_backend(owner_backend)
    .expect("metadata backend");
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.path = path.to_string();
    metadata.version = version_1;
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    owner_dfs.add_file_system(file_system).expect("owner fs");
    owner_dfs
        .add_transient_object_metadata(metadata)
        .expect("transient new object");

    let write = owner_dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: requester.clone(),
            },
            bytes.to_vec(),
            vec![owner.clone(), requester.clone(), replica.clone()],
            DfsReplicationPolicy::new(3, 3).expect("policy"),
        )
        .expect("pending write");
    let intent_id = match write {
        NetworkedDfsWriteOutcome::Pending { intent_id, .. } => intent_id,
        other => panic!("expected pending write, got {other:?}"),
    };
    let content_hash = content_hash(bytes);
    let persisted_intent = owner_dfs
        .metadata_backend()
        .expect("backend")
        .get_intent(&object_id, &intent_id)
        .expect("intent")
        .expect("intent");
    assert_eq!(
        persisted_intent.coordinator_node_id, requester,
        "test setup must model a routed write whose requester is not the metadata primary"
    );
    assert_eq!(
        persisted_intent.create_predecessor_object_id,
        Some(old_tombstone_object_id),
        "the intent must durably identify the exact tombstone it replaces"
    );
    owner_dfs
        .metadata_backend
        .as_mut()
        .expect("metadata backend")
        .put_receipt(&DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                committed_version: version_2.clone(),
                owner_node_id: owner.clone(),
                event_id: EventId::generate(),
            },
            replica_node_id: replica.clone(),
            content_hash: Some(content_hash.clone()),
            received_at: OffsetDateTime::now_utc(),
        })
        .expect("persist replica receipt without visible commit");
    assert!(
        owner_dfs
            .metadata_backend()
            .expect("backend")
            .get_object(&object_id)
            .expect("object lookup")
            .is_none(),
        "test setup must leave the new object absent from the visible namespace"
    );
    drop(owner_dfs);

    let recovered_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
            .expect("recovered backend");
    let recovered_owner = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner-content")).expect("recovered content"),
    )
    .with_control_plane(control_plane)
    .with_metadata_backend(recovered_backend)
    .expect("commit-ready new-object intent should hydrate");

    let object = recovered_owner
        .metadata_backend()
        .expect("backend")
        .get_object(&object_id)
        .expect("object")
        .expect("new object should become visible during recovery");
    assert_eq!(object.version, version_2);
    assert_eq!(object.content_hash.as_deref(), Some(content_hash.as_str()));
    assert_eq!(
        object
            .current_locations
            .into_iter()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([owner, requester, replica])
    );
    assert!(
        !recovered_owner
            .writes
            .pending_writes
            .contains_key(&intent_id),
        "fully receipted new-object intent should be cleaned after recovery"
    );
    assert_eq!(
        recovered_owner
            .metadata_backend()
            .expect("backend")
            .get_intent(&object_id, &intent_id)
            .expect("intent")
            .expect("intent")
            .status,
        DfsWriteIntentStatus::Committed
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn hydrate_from_metadata_backend_does_not_resurrect_after_later_compacted_tombstone() {
    let root = temp_root("hydrate-does-not-resurrect-after-later-compacted-tombstone");
    let owner = NodeId::from_legacy_named("node_hydrate_replaced_primary").expect("node");
    let requester = NodeId::from_legacy_named("node_hydrate_replaced_requester").expect("node");
    let replica = NodeId::from_legacy_named("node_hydrate_replaced_replica").expect("node");
    let stale_object_id =
        DfsObjectId::from_legacy_named("obj_hydrate_replaced_stale").expect("object");
    let replacement_object_id =
        DfsObjectId::from_legacy_named("obj_hydrate_replaced_current").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let replacement_version =
        DfsVersion::from_legacy_named("version_replacement").expect("version");
    let path = "/services/hydrate-replaced-path.txt";
    let bytes = b"stale commit-ready create bytes";
    let network = InProcessNetwork::new();
    let metadata_root = root.join("metadata");
    let backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
        .expect("metadata backend");
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(backend)
    .expect("metadata backend");
    let mut stale = file_metadata(owner.clone());
    stale.object_id = stale_object_id.clone();
    stale.path = path.to_string();
    stale.version = version_1;
    stale.policy = DfsObjectPolicy::ReplicatedEventual;
    owner_dfs
        .add_file_system(file_system(owner.clone()))
        .expect("owner fs");
    owner_dfs
        .add_transient_object_metadata(stale)
        .expect("transient stale object");

    let write = owner_dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id: stale_object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: requester.clone(),
            },
            bytes.to_vec(),
            vec![owner.clone(), requester.clone(), replica.clone()],
            DfsReplicationPolicy::new(3, 3).expect("policy"),
        )
        .expect("pending write");
    let intent_id = match write {
        NetworkedDfsWriteOutcome::Pending { intent_id, .. } => intent_id,
        other => panic!("expected pending write, got {other:?}"),
    };
    assert!(
        owner_dfs
            .metadata_backend()
            .expect("backend")
            .get_intent(&stale_object_id, &intent_id)
            .expect("intent lookup")
            .expect("intent")
            .create_predecessor_object_id
            .is_none(),
        "a create that began on an empty path must not claim a predecessor"
    );
    let content_hash = content_hash(bytes);
    owner_dfs
        .metadata_backend
        .as_mut()
        .expect("metadata backend")
        .put_receipt(&DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: sifs_file_system_id(),
                object_id: stale_object_id.clone(),
                committed_version: version_2,
                owner_node_id: owner.clone(),
                event_id: EventId::generate(),
            },
            replica_node_id: replica,
            content_hash: Some(content_hash),
            received_at: OffsetDateTime::now_utc(),
        })
        .expect("persist final durable receipt");
    let mut later_tombstone = file_metadata(owner.clone());
    later_tombstone.object_id = replacement_object_id.clone();
    later_tombstone.path = path.to_string();
    later_tombstone.version = replacement_version;
    later_tombstone.kind = DfsObjectKind::Tombstone;
    later_tombstone.content_hash = None;
    later_tombstone.content_size_bytes = None;
    later_tombstone.changed_at_unix_nanos = 1;
    owner_dfs
        .metadata_backend
        .as_mut()
        .expect("backend")
        .put_object(&later_tombstone)
        .expect("later tombstone");
    owner_dfs
        .metadata_backend
        .as_mut()
        .expect("backend")
        .compact_tombstones(
            DfsTombstoneCompactionOptions::compact_eligible(Duration::ZERO)
                .with_now_unix_nanos(100),
        )
        .expect("compact later tombstone");
    drop(owner_dfs);

    let recovered_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
            .expect("recovered backend");
    let recovered = NetworkedDfsService::new(
        network.add_node(owner),
        LocalContentStore::new(root.join("owner-content")).expect("recovered content"),
    )
    .with_metadata_backend(recovered_backend)
    .expect("later tombstone must fence stale intent recovery");

    let visible = recovered
        .metadata_backend()
        .expect("backend")
        .get_object_by_path(&sifs_file_system_id(), path)
        .expect("path lookup")
        .expect("later tombstone remains authoritative");
    assert_eq!(visible.object_id, replacement_object_id);
    assert_eq!(visible.kind, DfsObjectKind::Tombstone);
    assert!(
        recovered
            .metadata_backend()
            .expect("backend")
            .get_object(&stale_object_id)
            .expect("stale object lookup")
            .is_none(),
        "recovery must not republish the stale create over a later deletion"
    );
    assert!(
        recovered.coordinator.intent(&intent_id).is_none(),
        "a displaced create intent must not re-enter active recovery state"
    );
    assert_eq!(
        recovered
            .metadata_backend()
            .expect("backend")
            .get_intent(&stale_object_id, &intent_id)
            .expect("intent lookup")
            .expect("durable intent")
            .status,
        DfsWriteIntentStatus::Rejected,
        "the displaced create must become terminal instead of retrying on every restart"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_rename_records_old_path_tombstone() {
    let root = temp_root("sifs-posix-rename-tombstone");
    let node_id = NodeId::from_legacy_named("node_sifs_rename_tombstone").expect("node");
    let old_path = "/services/sandbox/instances/sandbox_rename/work/old.txt";
    let new_path = "/services/sandbox/instances/sandbox_rename/work/new.txt";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_rename/work")
        .expect("mkdir");
    dfs.posix_write_file(
        old_path,
        b"content",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write old");
    let old_version = dfs.posix_stat(old_path).expect("old stat").version;
    dfs.posix_rename(old_path, new_path).expect("rename");

    assert!(matches!(
        dfs.posix_stat(old_path),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert_eq!(dfs.posix_read_file(new_path).expect("read new"), b"content");
    let tombstone = dfs
        .metadata
        .objects
        .values()
        .find(|object| object.path == old_path && object.kind == DfsObjectKind::Tombstone)
        .expect("old path tombstone");
    assert_ne!(tombstone.version, old_version);

    let reloaded = networked_sifs_with_backend(&root, node_id);
    assert!(matches!(
        reloaded.posix_stat(old_path),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert_eq!(
        reloaded
            .posix_read_file(new_path)
            .expect("reloaded read new"),
        b"content"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_bulk_overwrite_rejects_tombstoned_parent() {
    let root = temp_root("sifs-posix-bulk-overwrite-tombstoned-parent");
    let node_id =
        NodeId::from_legacy_named("node_sifs_bulk_overwrite_tombstoned_parent").expect("node");
    let work = "/services/sandbox/instances/sandbox_bulk_overwrite_tombstoned_parent/work";
    let path = "/services/sandbox/instances/sandbox_bulk_overwrite_tombstoned_parent/work/file.txt";
    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all(work).expect("mkdir");
    dfs.posix_write_file(
        path,
        b"initial",
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("write initial");
    dfs.posix_unlink(path).expect("unlink child");
    dfs.posix_rmdir(work).expect("tombstone parent");

    assert!(
        matches!(
            dfs.posix_write_files_local_eventual_with_known_parents([(
                path,
                b"must not recreate below tombstone".as_slice(),
            )]),
            Err(DfsPosixError::MissingParent { path: missing }) if missing == work
        ),
        "bulk overwrite must not create a child below a tombstoned parent"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_list_dir_page_prefers_newer_durable_child_over_stale_memory_tombstone() {
    let root = temp_root("sifs-posix-list-page-stale-memory-tombstone");
    let node_id = NodeId::from_legacy_named("node_sifs_list_page_stale_tombstone").expect("node");
    let dir_path = "/services/sandbox/instances/sandbox_list_page_stale_tombstone/work";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all(dir_path).expect("mkdir");
    let mut persisted = Vec::new();
    for index in 0..3 {
        persisted.push(
            dfs.posix_write_file(
                &format!("{dir_path}/file-{index}.txt"),
                b"content",
                DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
            )
            .expect("write"),
        );
    }

    let mut stale_tombstone = dfs
        .metadata()
        .object(&persisted[1].object_id)
        .expect("persisted object metadata")
        .clone();
    stale_tombstone.kind = DfsObjectKind::Tombstone;
    stale_tombstone.version = DfsVersion::generate();
    stale_tombstone.content_hash = None;
    stale_tombstone.content_size_bytes = None;
    dfs.track_metadata_tombstone(&stale_tombstone);

    let mut durable_live = stale_tombstone.clone();
    durable_live.kind = DfsObjectKind::File;
    durable_live.version = DfsVersion::generate();
    durable_live.content_hash = Some(content_hash(b"newer durable live child"));
    durable_live.content_size_bytes = Some(b"newer durable live child".len() as u64);
    LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root.join("metadata"))
        .expect("durable live metadata backend")
        .put_object(&durable_live)
        .expect("newer durable live child");

    assert!(
        dfs.transient_metadata_object_ids.is_empty(),
        "a stale observed tombstone is not an active transient namespace overlay"
    );
    let mut cursor = None;
    let mut entries = Vec::new();
    loop {
        let page = dfs
            .posix_list_dir_page(dir_path, cursor.as_ref(), 1)
            .expect("list page");
        assert!(page.entries.len() <= 1);
        if let Some(next_cursor) = page.next_cursor.as_ref() {
            assert!(
                next_cursor.dentry.is_some(),
                "stale non-transient tombstones must retain bounded backend pagination"
            );
        }
        entries.extend(page.entries);
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        cursor = Some(next_cursor);
    }

    entries.sort_by(|left, right| left.name.cmp(&right.name));
    assert_eq!(entries.len(), 3);
    let durable_entry = entries
        .iter()
        .find(|entry| entry.path == durable_live.path)
        .expect("newer durable child must suppress the stale in-memory tombstone");
    assert_eq!(durable_entry.kind, DfsObjectKind::File);
    assert_eq!(durable_entry.object_id, durable_live.object_id);

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn cold_lazy_sync_path_includes_durable_direct_child_tombstones() {
    let root = temp_root("cold-lazy-sync-path-tombstones");
    let node = NodeId::from_legacy_named("node_cold_lazy_sync_primary").expect("node");
    let requester = NodeId::from_legacy_named("node_cold_lazy_sync_requester").expect("requester");
    let directory = "/services/cold-sync-dir";
    let deleted_path = "/services/cold-sync-dir/deleted.txt";
    let sender: DfsMessageSender = Arc::new(|_, command, _| {
        Ok(DfsMessageResponse {
            request_id: command.request_id.clone(),
            outbound_messages: Vec::new(),
        })
    });
    {
        let mut writer = NetworkedDfsService::new(
            DfsNodeServedMessageTransport::new_with_message_sender(
                node.clone(),
                Vec::new(),
                Arc::clone(&sender),
                Duration::from_secs(1),
            ),
            LocalContentStore::new(root.join("content")).expect("writer content"),
        )
        .with_metadata_backend(
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root.join("metadata"))
                .expect("writer metadata backend"),
        )
        .expect("writer metadata service");
        writer.bootstrap_distributed_sifs().expect("bootstrap sifs");
        writer.posix_mkdir_all(directory).expect("directory");
        writer
            .posix_write_file(
                deleted_path,
                b"deleted payload",
                DfsWriteOptions::replicated_eventual(vec![node.clone()]),
            )
            .expect("write deleted child");
        writer.posix_unlink(deleted_path).expect("unlink child");
    }

    let mut cold = NetworkedDfsService::new(
        DfsNodeServedMessageTransport::new_with_message_sender(
            node.clone(),
            Vec::new(),
            sender,
            Duration::from_secs(1),
        ),
        LocalContentStore::new(root.join("content")).expect("cold content"),
    )
    .with_metadata_backend_lazy(
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root.join("metadata"))
            .expect("cold metadata backend"),
    )
    .expect("cold lazy service");
    let routed_message = build_dfs_routed_message(
        &requester,
        &node,
        1,
        1,
        DfsNetworkMessage::NamespaceRequest(DfsNamespaceCommand {
            request_id: "cold-lazy-sync-path".to_string(),
            file_system_id: sifs_file_system_id(),
            requesting_node_id: requester.clone(),
            include_metadata_journal_batches: false,
            metadata_journal_after_sequences: Vec::new(),
            authority_shards: Vec::new(),
            operation: DfsNamespaceOperation::SyncPath {
                path: directory.to_string(),
            },
        }),
    );
    let result = cold
        .process_message_command(test_dfs_message_command(
            "cold_lazy_sync_path_rpc",
            routed_message,
        ))
        .expect("cold SyncPath");

    assert!(result.response.outbound_messages.iter().any(|message| {
        matches!(
            &message.message,
            DfsNetworkMessage::NamespaceResponse(DfsNamespaceResponse {
                synced_path_objects,
                ..
            }) if synced_path_objects.iter().any(|object| {
                object.path == deleted_path && object.kind == DfsObjectKind::Tombstone
            })
        )
    }));

    let mut live_overlay =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root.join("metadata"))
            .expect("overlay metadata backend")
            .get_object_by_path(&sifs_file_system_id(), deleted_path)
            .expect("read durable tombstone")
            .expect("durable tombstone");
    assert_eq!(live_overlay.kind, DfsObjectKind::Tombstone);
    let stale_tombstone = live_overlay.clone();
    live_overlay.kind = DfsObjectKind::File;
    live_overlay.version = DfsVersion::generate();
    live_overlay.content_hash = Some(content_hash(b"newer transient live child"));
    live_overlay.content_size_bytes = Some(b"newer transient live child".len() as u64);
    cold.add_transient_object_metadata(live_overlay.clone())
        .expect("newer live overlay");
    let routed_message = build_dfs_routed_message(
        &requester,
        &node,
        2,
        2,
        DfsNetworkMessage::NamespaceRequest(DfsNamespaceCommand {
            request_id: "cold-lazy-sync-path-live-overlay".to_string(),
            file_system_id: sifs_file_system_id(),
            requesting_node_id: requester.clone(),
            include_metadata_journal_batches: false,
            metadata_journal_after_sequences: Vec::new(),
            authority_shards: Vec::new(),
            operation: DfsNamespaceOperation::SyncPath {
                path: directory.to_string(),
            },
        }),
    );
    let result = cold
        .process_message_command(test_dfs_message_command(
            "cold_lazy_sync_path_live_overlay_rpc",
            routed_message,
        ))
        .expect("cold SyncPath with live overlay");
    let synced_children = result
        .response
        .outbound_messages
        .iter()
        .filter_map(|message| match &message.message {
            DfsNetworkMessage::NamespaceResponse(response) => Some(&response.synced_path_objects),
            _ => None,
        })
        .flatten()
        .filter(|object| object.path == deleted_path)
        .collect::<Vec<_>>();
    assert_eq!(
        synced_children.len(),
        1,
        "SyncPath must return exactly one authoritative child per path"
    );
    assert_eq!(synced_children[0].kind, DfsObjectKind::Tombstone);
    assert_eq!(synced_children[0].version, stale_tombstone.version);

    cold.remove_transient_object_metadata(&live_overlay.object_id);
    cold.track_metadata_tombstone(&stale_tombstone);
    let mut durable_live = live_overlay;
    durable_live.version = DfsVersion::generate();
    durable_live.content_hash = Some(content_hash(b"newer durable live child"));
    durable_live.content_size_bytes = Some(b"newer durable live child".len() as u64);
    LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root.join("metadata"))
        .expect("durable live metadata backend")
        .put_object(&durable_live)
        .expect("newer durable live child");
    let routed_message = build_dfs_routed_message(
        &requester,
        &node,
        3,
        3,
        DfsNetworkMessage::NamespaceRequest(DfsNamespaceCommand {
            request_id: "cold-lazy-sync-path-durable-live".to_string(),
            file_system_id: sifs_file_system_id(),
            requesting_node_id: requester.clone(),
            include_metadata_journal_batches: false,
            metadata_journal_after_sequences: Vec::new(),
            authority_shards: Vec::new(),
            operation: DfsNamespaceOperation::SyncPath {
                path: directory.to_string(),
            },
        }),
    );
    let result = cold
        .process_message_command(test_dfs_message_command(
            "cold_lazy_sync_path_durable_live_rpc",
            routed_message,
        ))
        .expect("cold SyncPath with durable live child");
    let synced_children = result
        .response
        .outbound_messages
        .iter()
        .filter_map(|message| match &message.message {
            DfsNetworkMessage::NamespaceResponse(response) => Some(&response.synced_path_objects),
            _ => None,
        })
        .flatten()
        .filter(|object| object.path == deleted_path)
        .collect::<Vec<_>>();
    assert_eq!(
        synced_children.len(),
        1,
        "a durable live child must suppress an older in-memory tombstone"
    );
    assert_eq!(synced_children[0].kind, DfsObjectKind::File);
    assert_eq!(synced_children[0].version, durable_live.version);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_cluster_posix_unlink_replicates_tombstone_to_observers() {
    let mut cluster = sifs_posix_cluster("sifs-cluster-unlink");
    let path = "/users/unlink-me.txt";
    let bytes = b"bytes to be unlinked".to_vec();

    sifs_cluster_primary_posix_write(&mut cluster, path, &bytes);
    sifs_cluster_publish_metadata_until(&mut cluster, "pre-unlink catch-up", |cluster| {
        cluster.observer.posix_stat(path).is_ok()
    });

    cluster.primary.posix_unlink(path).expect("primary unlink");
    assert!(matches!(
        cluster.primary.posix_stat(path),
        Err(DfsPosixError::NotFound { .. })
    ));

    sifs_cluster_publish_metadata_until(&mut cluster, "post-unlink catch-up", |cluster| {
        matches!(
            cluster.observer.posix_stat(path),
            Err(DfsPosixError::NotFound { .. })
        )
    });
    assert!(matches!(
        cluster.observer.posix_read_file(path),
        Err(DfsPosixError::NotFound { .. })
    ));
    let entries = cluster
        .observer
        .posix_list_dir("/users")
        .expect("observer list after unlink");
    assert!(
        !entries.iter().any(|entry| entry.path == path),
        "observer must not list an unlinked path: {entries:?}"
    );

    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn routed_mkdir_all_prefers_live_path_shard_over_late_source_tombstone() {
    let root = temp_root("sifs-mkdir-all-stale-source-tombstone");
    let metadata_root = root.join("metadata");
    let root_primary =
        NodeId::from_legacy_named("node_mkdir_stale_root_primary").expect("root primary");
    let services_primary =
        NodeId::from_legacy_named("node_mkdir_stale_services_primary").expect("services primary");
    let requester = NodeId::from_legacy_named("node_mkdir_stale_requester").expect("requester");
    let shards = sifs_two_primary_shard_configs(
        &root_primary,
        &services_primary,
        4,
        vec![services_primary.clone(), requester.clone()],
        vec![root_primary.clone(), requester.clone()],
    );
    let file_system = distributed_sifs(root_primary.clone());
    let mut services_anchor = file_metadata(root_primary.clone());
    services_anchor.object_id =
        DfsObjectId::from_legacy_named("obj_mkdir_stale_services_anchor").expect("object");
    services_anchor.file_system_id = file_system.file_system_id.clone();
    services_anchor.path = SIFS_SERVICES_PATH.to_string();
    services_anchor.kind = DfsObjectKind::Directory;
    services_anchor.mode = 0o755;
    services_anchor.version =
        DfsVersion::from_legacy_named("version_mkdir_stale_services_anchor").expect("version");

    {
        let mut root_only =
            open_local_sharded_metadata_backend(metadata_root.clone(), vec![shards[0].clone()])
                .expect("root-only backend");
        root_only.put_file_system(&file_system).expect("filesystem");
        root_only
            .put_object(&services_anchor)
            .expect("root services anchor");
    }
    let mut backend = open_local_sharded_metadata_backend(metadata_root.clone(), shards.clone())
        .expect("sharded backend");
    backend
        .migrate_objects_to_configured_shards(&file_system.file_system_id, 8)
        .expect("migrate services anchor");
    let root_shard_id = DfsMetadataShardId::new("root");
    let services_shard_id = DfsMetadataShardId::new("services");
    let source_tombstone = backend
        .get_object_on_shard(&root_shard_id, &services_anchor.object_id)
        .expect("root object lookup")
        .expect("source tombstone");
    assert_eq!(source_tombstone.kind, DfsObjectKind::Tombstone);
    backend
        .write_object_shard_index(&services_anchor, &root_shard_id)
        .expect("stale replica index");

    let mut kv = InProcessKvStore::new();
    {
        let mut control = DfsControlPlaneStore::new(&mut kv);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(&file_system))
            .expect("filesystem config");
        for shard in &shards {
            control.put_metadata_shard(shard).expect("metadata shard");
        }
    }
    let network = InProcessNetwork::new();
    let _requester_transport = network.add_node(requester.clone());
    let mut dfs = NetworkedDfsService::new(
        network.add_node(services_primary),
        LocalContentStore::new(root.join("content")).expect("content"),
    )
    .with_control_plane(kv)
    .with_metadata_backend(backend)
    .expect("service backend");
    assert_eq!(
        dfs.posix_stat(SIFS_SERVICES_PATH)
            .expect("hydrated services anchor")
            .object_id,
        services_anchor.object_id,
        "hydration must ignore a stale derived index and the retired source tombstone"
    );
    assert!(
        !dfs.tombstones_by_metadata_path.contains_key(&(
            file_system.file_system_id.clone(),
            metadata_path(SIFS_SERVICES_PATH),
        )),
        "the retired source tombstone must not enter the hydrated logical path view"
    );

    // Model a late replay of the retired root-shard eviction row after the
    // service has already hydrated the live services-shard anchor.
    dfs.apply_metadata_object_to_service_state(source_tombstone)
        .expect("late source tombstone overlay");
    crate::metadata_shard_index::write_object_shard_index(
        &metadata_root,
        &services_anchor,
        &root_shard_id,
    )
    .expect("restore stale replica index after hydrate");
    assert_eq!(
        dfs.tombstones_by_metadata_path
            .get(&(
                file_system.file_system_id.clone(),
                metadata_path(SIFS_SERVICES_PATH),
            ))
            .map(|object| object.kind.clone()),
        Some(DfsObjectKind::Tombstone)
    );

    let before = dfs
        .metadata_backend()
        .expect("metadata backend")
        .metadata_shard_statuses()
        .expect("statuses before")
        .into_iter()
        .map(|status| (status.shard_id, status.journal_last_sequence))
        .collect::<BTreeMap<_, _>>();
    dfs.posix_mkdir_all_local(SIFS_SERVICES_PATH)
        .expect("existing services anchor mkdir-all");
    let after_anchor_noop = dfs
        .metadata_backend()
        .expect("metadata backend")
        .metadata_shard_statuses()
        .expect("statuses after anchor no-op")
        .into_iter()
        .map(|status| (status.shard_id, status.journal_last_sequence))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        after_anchor_noop, before,
        "the existing migrated anchor must be a zero-mutation mkdir-all"
    );

    let child_path = "/services/stale-index-probe";
    let event = dfs
        .handle_namespace_request(
            requester.clone(),
            DfsNamespaceCommand {
                request_id: "mkdir-stale-services-anchor".to_string(),
                file_system_id: file_system.file_system_id.clone(),
                requesting_node_id: requester,
                include_metadata_journal_batches: false,
                metadata_journal_after_sequences: Vec::new(),
                authority_shards: Vec::new(),
                operation: DfsNamespaceOperation::MkdirAll {
                    path: child_path.to_string(),
                },
            },
        )
        .expect("routed mkdir-all");
    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::RemoteNamespaceServed {
            result: DfsNamespaceResult::Applied,
            ..
        }
    ));
    let after = dfs
        .metadata_backend()
        .expect("metadata backend")
        .metadata_shard_statuses()
        .expect("statuses after")
        .into_iter()
        .map(|status| (status.shard_id, status.journal_last_sequence))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        after.get(&root_shard_id),
        before.get(&root_shard_id),
        "creating beneath the migrated anchor must not touch the root shard"
    );
    assert!(
        after
            .get(&services_shard_id)
            .zip(before.get(&services_shard_id))
            .is_some_and(|(after, before)| after > before),
        "the routed child create must mutate only the services shard"
    );
    let current_services_anchor = dfs
        .metadata_backend()
        .expect("metadata backend")
        .get_object_by_path(&file_system.file_system_id, SIFS_SERVICES_PATH)
        .expect("services path")
        .expect("services anchor");
    assert_eq!(current_services_anchor.object_id, services_anchor.object_id);
    assert_eq!(current_services_anchor.kind, DfsObjectKind::Directory);
    assert_eq!(
        dfs.metadata_backend()
            .expect("metadata backend")
            .get_object_by_path(&file_system.file_system_id, child_path)
            .expect("created child")
            .map(|object| object.kind),
        Some(DfsObjectKind::Directory)
    );

    dfs.metadata_backend
        .as_mut()
        .expect("metadata backend")
        .refresh_metadata_shard_durable_view(&services_shard_id)
        .expect("refresh services durable view");
    assert_eq!(
        crate::metadata_shard_index::read_object_shard_index(
            &metadata_root,
            &services_anchor.object_id,
        )
        .expect("repaired index")
        .map(|index| index.shard_id),
        Some(services_shard_id),
        "derived index repair follows visibility and never authorizes it"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_replica_distributed_stat_prefers_primary_tombstone_over_stale_local_hit() {
    let mut cluster = sifs_posix_cluster("sifs-replica-stat-primary-tombstone");
    let path = "/users/stat-stale-live.txt";
    let bytes = b"writer-local stale entry should lose to primary tombstone".to_vec();

    sifs_cluster_routed_posix_write(&mut cluster, path, &bytes);
    assert_eq!(
        cluster.writer.posix_read_file(path).expect("writer read"),
        bytes
    );
    assert!(cluster.writer.posix_stat(path).is_ok());

    cluster.primary.posix_unlink(path).expect("primary unlink");
    assert!(matches!(
        cluster.primary.posix_stat(path),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert!(
        cluster.writer.posix_stat(path).is_ok(),
        "test setup must leave a stale local live entry on the writer"
    );

    let SifsPosixCluster {
        root,
        mut primary,
        mut writer,
        ..
    } = cluster;
    let stop_primary = Arc::new(AtomicBool::new(false));
    let primary_stop = Arc::clone(&stop_primary);
    let primary_thread = std::thread::spawn(move || {
        while !primary_stop.load(Ordering::SeqCst) {
            primary.process_incoming().expect("primary pump");
            std::thread::sleep(Duration::from_millis(1));
        }
        primary
    });

    let distributed_stat = writer.posix_stat_distributed(path);
    stop_primary.store(true, Ordering::SeqCst);
    primary_thread.join().expect("primary thread");
    assert!(
        matches!(distributed_stat, Err(DfsPosixError::NotFound { .. })),
        "distributed stat must prefer the shard primary tombstone over a stale replica-local hit, got {distributed_stat:?}"
    );

    std::fs::remove_dir_all(&root).ok();
}
