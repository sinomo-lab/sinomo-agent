//! Content regression tests.

use super::*;

#[test]
fn content_root_catalog_continuation_reuses_exact_cut_after_live_head_advances() {
    let root = temp_root("content-root-catalog-continuation-cache");
    let shard_id = DfsMetadataShardId::new("root");
    let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
    backend.put_file_system(&file_system()).expect("filesystem");
    let file = object(version("version_catalog_current"));
    backend.put_object(&file).expect("object");
    for index in 0..96 {
        let mut record = object_version_record(
            &file,
            version(&format!("version_catalog_{index:03}")),
            OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(index),
        );
        record.content_hash = Some(format!("blake3:catalog-{index:03}"));
        backend.put_version(&record).expect("catalog version");
    }

    let shard = DfsMetadataShardConfig {
        file_system_id: file.file_system_id.clone(),
        shard_id: shard_id.0.clone(),
        primary_node_id: node("node_owner"),
        replica_node_ids: Vec::new(),
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let request = DfsContentRootCatalogPageRequest {
        identity: DfsContentRootCatalogShardIdentity::from_config(&shard),
        expected_topology_digest: dfs_content_root_catalog_topology_digest(&[shard])
            .expect("topology digest"),
        minimum_journal_sequence: 0,
        minimum_state_fingerprint: None,
        after: None,
        max_entries: 16,
    };
    let first = backend
        .content_root_catalog_page(&request)
        .expect("first content-root page");
    let first_cut = first.coverage.durable_cut().expect("complete first cut").0;
    let next = first.next_cursor.expect("fixture continuation");

    let mut later = object_version_record(
        &file,
        version("version_catalog_after_first_page"),
        OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(97),
    );
    later.content_hash = Some("blake3:catalog-after-first-page".to_string());
    backend
        .put_version(&later)
        .expect("advance live metadata head");

    let mut continuation_request = request.clone();
    continuation_request.minimum_journal_sequence = next.journal_sequence;
    continuation_request.minimum_state_fingerprint = Some(next.state_fingerprint.clone());
    continuation_request.after = Some(next);

    backend
        .content_root_catalog_page(&request)
        .expect("new observation at advanced live head");
    assert_eq!(
        backend
            .content_root_catalog_snapshot_builds
            .load(Ordering::Relaxed),
        2,
        "a new first page must re-observe the advanced durable head"
    );

    let continuation = backend
        .content_root_catalog_page(&continuation_request)
        .expect("interleaved cached exact-cut continuation");
    continuation
        .validate_for_request(&continuation_request)
        .expect("valid cached continuation");
    assert_eq!(
        continuation
            .coverage
            .durable_cut()
            .expect("complete continuation")
            .0,
        first_cut,
        "pagination must remain on the first page's immutable durable cut"
    );
    assert_eq!(
        backend
            .content_root_catalog_snapshot_builds
            .load(Ordering::Relaxed),
        2,
        "the prior exact cut must survive one interleaved first-page observation"
    );

    let mut latest = object_version_record(
        &file,
        version("version_catalog_after_interleaved_page"),
        OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(98),
    );
    latest.content_hash = Some("blake3:catalog-after-interleaved-page".to_string());
    backend
        .put_version(&latest)
        .expect("advance live metadata head again");
    let third = backend
        .content_root_catalog_page(&request)
        .expect("third live-head observation");
    assert_eq!(
        backend
            .content_root_catalog_snapshot_builds
            .load(Ordering::Relaxed),
        3,
        "the bounded cache must retain only the two newest durable cuts"
    );
    let evicted = backend
        .content_root_catalog_page(&continuation_request)
        .expect("evicted continuation fails closed");
    assert!(matches!(
        evicted.coverage,
        DfsContentRootCatalogCoverage::Incomplete { ref reasons }
            if reasons == &[DfsContentRootCatalogIncompleteReason::StateFingerprintChanged]
    ));
    assert_eq!(
        backend
            .content_root_catalog_snapshot_builds
            .load(Ordering::Relaxed),
        3,
        "an evicted stale cursor must fail before deriving another full root snapshot"
    );

    let third_next = third.next_cursor.expect("third-cut continuation");
    backend
        .freeze_metadata_shard_for_membership_handoff(&shard_id, "catalog-continuation-handoff", 1)
        .expect("freeze metadata shard")
        .expect("matching shard freeze");
    let mut frozen_request = request;
    frozen_request.minimum_journal_sequence = third_next.journal_sequence;
    frozen_request.minimum_state_fingerprint = Some(third_next.state_fingerprint.clone());
    frozen_request.after = Some(third_next);
    let frozen = backend
        .content_root_catalog_page(&frozen_request)
        .expect("frozen continuation response");
    assert!(matches!(
        frozen.coverage,
        DfsContentRootCatalogCoverage::Incomplete { ref reasons }
            if matches!(
                reasons.as_slice(),
                [DfsContentRootCatalogIncompleteReason::MembershipHandoffFrozen {
                    handoff_id: Some(handoff_id)
                }] if handoff_id == "catalog-continuation-handoff"
            )
    ));
    cleanup(&root);
}

#[test]
fn content_root_catalog_page_adapts_to_encoded_byte_budget() {
    let root = temp_root("content-root-catalog-byte-budget");
    let shard_id = DfsMetadataShardId::new("root");
    let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
    backend.put_file_system(&file_system()).expect("filesystem");
    let file = object(version("version_catalog_budget_current"));
    backend.put_object(&file).expect("object");
    for index in 0..700 {
        let version_suffix = format!("{index:03}{}", "v".repeat(180));
        let mut record = object_version_record(
            &file,
            version(&format!("version_catalog_budget_{version_suffix}")),
            OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(index),
        );
        record.content_hash = Some(format!("blake3:{index:03}{}", "a".repeat(220)));
        backend.put_version(&record).expect("catalog version");
    }

    let shard = DfsMetadataShardConfig {
        file_system_id: file.file_system_id.clone(),
        shard_id: shard_id.0.clone(),
        primary_node_id: node("node_owner"),
        replica_node_ids: Vec::new(),
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let request = DfsContentRootCatalogPageRequest {
        identity: DfsContentRootCatalogShardIdentity::from_config(&shard),
        expected_topology_digest: dfs_content_root_catalog_topology_digest(&[shard])
            .expect("topology digest"),
        minimum_journal_sequence: 0,
        minimum_state_fingerprint: None,
        after: None,
        max_entries: crate::DEFAULT_DFS_CONTENT_ROOT_CATALOG_PAGE_ENTRIES,
    };
    let page = backend
        .content_root_catalog_page(&request)
        .expect("byte-bounded content-root page");
    page.validate_for_request(&request)
        .expect("valid byte-bounded page");
    assert!(
        page.next_cursor.is_some(),
        "a byte-bounded first page must expose a continuation"
    );
    assert!(
        page.entries.len() < request.max_entries,
        "the encoded byte budget, not only the entry ceiling, must bound this fixture"
    );
    assert!(
        page.encoded_size_bytes().expect("encoded page size")
            <= DFS_CONTENT_ROOT_CATALOG_TARGET_PAGE_BYTES,
        "ordinary multi-entry pages should stay inside the target byte budget"
    );
    cleanup(&root);
}

#[test]
fn sharded_content_root_catalog_rejects_same_shard_id_from_different_filesystem() {
    let root = temp_root("content-root-catalog-cross-filesystem-shard");
    let shard_id = DfsMetadataShardId::new("root");
    let configured_shard = shard_config(&shard_id.0, DfsMetadataShardAssignment::Root);
    let mut backend = LocalShardedMetadataBackend::new(&root, vec![configured_shard.clone()])
        .expect("sharded backend");
    backend
        .put_file_system(&file_system())
        .expect("configured filesystem");
    backend
        .put_object(&object(version("version_catalog_configured")))
        .expect("configured filesystem object");

    let mut foreign_shard = configured_shard.clone();
    foreign_shard.file_system_id =
        DfsFileSystemId::from_legacy_named("foreign-filesystem").expect("foreign filesystem");
    let request = DfsContentRootCatalogPageRequest {
        identity: DfsContentRootCatalogShardIdentity::from_config(&foreign_shard),
        expected_topology_digest: dfs_content_root_catalog_topology_digest(std::slice::from_ref(
            &foreign_shard,
        ))
        .expect("foreign topology digest"),
        minimum_journal_sequence: 0,
        minimum_state_fingerprint: None,
        after: None,
        max_entries: DFS_CONTENT_ROOT_CATALOG_MAX_PAGE_ENTRIES,
    };

    let page = backend
        .content_root_catalog_page(&request)
        .expect("rootless cross-filesystem response");
    page.validate_for_request(&request)
        .expect("valid incomplete response");
    assert!(page.entries.is_empty());
    assert!(page.next_cursor.is_none());
    assert!(matches!(
        page.coverage,
        DfsContentRootCatalogCoverage::Incomplete { ref reasons }
            if matches!(
                reasons.as_slice(),
                [DfsContentRootCatalogIncompleteReason::AuthorityChanged {
                    observed: Some(observed),
                }] if observed
                    == &DfsContentRootCatalogShardIdentity::from_config(&configured_shard)
            )
    ));

    cleanup(&root);
}
