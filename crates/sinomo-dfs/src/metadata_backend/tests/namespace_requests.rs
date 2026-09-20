//! Namespace requests regression tests.

use super::*;

#[test]
fn sharded_backend_pages_namespace_jobs() {
    let root = temp_root("sharded_backend_pages_namespace_jobs");
    let mut backend = LocalShardedMetadataBackend::new(
        &root,
        vec![
            shard_config("root", DfsMetadataShardAssignment::Root),
            shard_config(
                "services",
                DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services".to_string(),
                },
            ),
        ],
    )
    .expect("sharded backend");

    for index in 0..6 {
        let root_path = if index % 2 == 0 {
            format!("/jobs/{index}")
        } else {
            format!("/services/jobs/{index}")
        };
        backend
            .put_namespace_job(&namespace_job(index, &root_path))
            .expect("namespace job");
    }

    let mut cursor = None;
    let mut job_ids = Vec::new();
    let mut pages = 0usize;
    loop {
        let page = backend
            .list_namespace_jobs_page(cursor.as_ref(), 2)
            .expect("job page");
        assert!(page.jobs.len() <= 2);
        job_ids.extend(page.jobs.iter().map(|job| job.job_id.clone()));
        pages = pages.saturating_add(1);
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        cursor = Some(next_cursor);
    }
    assert_eq!(
        job_ids,
        (0..6)
            .map(|index| format!("job_namespace_page_{index:02}"))
            .collect::<Vec<_>>()
    );
    assert_eq!(pages, 3);
    assert_eq!(
        backend
            .get_namespace_job("job_namespace_page_05")
            .expect("get job")
            .expect("job")
            .root_path,
        "/services/jobs/5"
    );

    cleanup(&root);
}

#[test]
fn sharded_backend_merges_pending_namespace_request_pages() {
    let root = temp_root("sharded_backend_pages_pending_namespace_requests");
    let mut backend = LocalShardedMetadataBackend::new(
        &root,
        vec![
            shard_config("root", DfsMetadataShardAssignment::Root),
            shard_config(
                "services",
                DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services".to_string(),
                },
            ),
        ],
    )
    .expect("sharded backend");

    for index in 0..6 {
        let status = if index == 3 {
            DfsNamespaceRequestStatus::Completed
        } else {
            DfsNamespaceRequestStatus::Pending
        };
        let mut request = namespace_request(index, status);
        let shard_id = if index % 2 == 0 {
            DfsMetadataShardId::new("root")
        } else {
            DfsMetadataShardId::new("services")
        };
        request.authority_shard_id = shard_id.clone();
        backend
            .put_namespace_request_on_shard(&shard_id, &request)
            .expect("namespace request");
    }

    let mut cursor = None;
    let mut request_ids = Vec::new();
    loop {
        let page = backend
            .list_pending_namespace_requests_page(cursor.as_ref(), 2)
            .expect("pending request page");
        request_ids.extend(
            page.requests
                .iter()
                .map(|request| request.request_id.clone()),
        );
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        cursor = Some(next_cursor);
    }
    assert_eq!(
        request_ids,
        [0, 1, 2, 4, 5]
            .into_iter()
            .map(|index| format!("namespace-request-{index:02}"))
            .collect::<Vec<_>>()
    );

    cleanup(&root);
}

#[test]
fn sharded_namespace_request_floor_survives_authority_prefix_change() {
    let root = temp_root("sharded_namespace_request_floor_authority_change");
    let root_shard = DfsMetadataShardId::new("root");
    let services_shard = DfsMetadataShardId::new("services");
    let mut backend = LocalShardedMetadataBackend::new(
        &root,
        vec![
            shard_config(&root_shard.0, DfsMetadataShardAssignment::Root),
            shard_config(
                &services_shard.0,
                DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services".to_string(),
                },
            ),
        ],
    )
    .expect("sharded backend");

    let mut completed = namespace_request(70, DfsNamespaceRequestStatus::Completed);
    completed.authority_shard_id = root_shard.clone();
    backend
        .put_namespace_request_on_shard(&root_shard, &completed)
        .expect("completed request on former authority");
    let compacted = backend
        .gc_metadata_history(
            DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 1)
                .with_now_unix_nanos(u64::MAX),
        )
        .expect("compact request body");
    assert_eq!(compacted.compacted_namespace_request_count, 1);
    let evicted = backend
        .gc_metadata_history(
            DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 1)
                .with_now_unix_nanos(u64::MAX),
        )
        .expect("evict compact request receipt");
    assert_eq!(evicted.removed_namespace_request_receipt_count, 1);

    assert!(
        backend
            .namespace_request_history_expired_on_shard(
                &services_shard,
                &completed.requester_node_id,
                &completed.request_id,
            )
            .expect("new-authority history lookup"),
        "the selected shard must consult an evicted receipt floor retained by the former authority"
    );
    let mut rerouted_retry = completed;
    rerouted_retry.authority_shard_id = services_shard.clone();
    assert!(
        backend
            .put_namespace_request_on_shard(&services_shard, &rerouted_retry)
            .expect_err("old request cannot become fresh on a new prefix authority")
            .to_string()
            .contains("idempotency history expired")
    );

    cleanup(&root);
}

#[test]
fn sharded_backend_gets_namespace_transaction_by_shard_transaction_id() {
    let root = temp_root("sharded_backend_get_namespace_transaction");
    let mut backend = LocalShardedMetadataBackend::new(
        &root,
        vec![
            shard_config("root", DfsMetadataShardAssignment::Root),
            shard_config(
                "services",
                DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services".to_string(),
                },
            ),
        ],
    )
    .expect("sharded backend");
    let file_system = file_system();
    backend.put_file_system(&file_system).expect("filesystem");
    let mut object = object(version("version_1"));
    object.path = "/services/namespace-transaction.txt".to_string();

    let entry = backend
        .apply_namespace_transaction(&DfsNamespaceTransaction {
            file_version_pins: None,
            transaction_id: "move-to-services".to_string(),
            file_system_id: file_system.file_system_id.clone(),
            objects: vec![object.clone()],
            versions: Vec::new(),
            advisory_lock_states: Vec::new(),
            created_at: OffsetDateTime::UNIX_EPOCH,
        })
        .expect("namespace transaction");
    let transaction_id = match &entry.mutation {
        DfsMetadataMutation::ApplyNamespaceTransaction { transaction_id, .. } => {
            transaction_id.clone()
        }
        other => panic!("expected namespace transaction journal entry, got {other:?}"),
    };

    let recovered = backend
        .get_namespace_transaction(&transaction_id)
        .expect("lookup transaction")
        .expect("transaction");
    assert_eq!(recovered.transaction_id, transaction_id);
    assert_eq!(
        recovered.objects.first().map(|object| &object.object_id),
        Some(&object.object_id)
    );

    cleanup(&root);
}

#[test]
fn namespace_request_admission_snapshot_polls_one_durable_generation() {
    let root = temp_root("namespace_request_admission_snapshot");
    let mut backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("metadata backend");
    let requester = NodeId::from_legacy_named("namespace-admission-requester").expect("requester");

    crate::metadata_read_snapshot::reset_metadata_durable_table_probes();
    let admission = backend
        .namespace_request_admission_snapshot(
            &DfsMetadataShardId::new("shard-root"),
            &requester,
            "request-fresh",
        )
        .expect("namespace admission snapshot");

    assert_eq!(
        admission,
        crate::DfsNamespaceRequestAdmissionSnapshot::default()
    );
    assert_eq!(
        crate::metadata_read_snapshot::metadata_durable_table_probes(),
        1,
        "request, receipt, prepare, and retention-floor evidence must share one durable generation probe"
    );

    crate::metadata_read_snapshot::reset_metadata_durable_table_probes();
    let _ = backend
        .get_namespace_request_receipt(&requester, "request-fresh")
        .expect("standalone receipt lookup");
    let _ = backend
        .get_namespace_request(&requester, "request-fresh")
        .expect("standalone request lookup");
    let _ = backend
        .get_prepared_namespace_prepare_for_request(&requester, "request-fresh")
        .expect("standalone prepare lookup");
    let _ = backend
        .namespace_request_history_expired_on_shard(
            &DfsMetadataShardId::new("shard-root"),
            &requester,
            "request-fresh",
        )
        .expect("standalone retention-floor lookup");
    assert_eq!(
        crate::metadata_read_snapshot::metadata_durable_table_probes(),
        4,
        "the regression fixture must exercise the four formerly independent durable probes"
    );

    cleanup(&root);
}

#[test]
fn namespace_request_admission_borrows_rotated_checkpoint_without_decode() {
    let root = temp_root("namespace_request_admission_rotated_checkpoint");
    let shard_id = DfsMetadataShardId::new("shard-root");
    let mut backend =
        LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("metadata backend");
    let mut stored_file_system = file_system();
    backend
        .put_file_system(&stored_file_system)
        .expect("materialize active table log");
    stored_file_system.display_name = "updated before rotation".to_string();
    backend
        .put_file_system(&stored_file_system)
        .expect("advance active table generation");
    {
        let _lock = backend.metadata_write_lock().expect("metadata writer lock");
        assert!(
            crate::metadata_table_store::rotate_metadata_table_log_for_checkpoint(&root, 0)
                .expect("rotate active table generation"),
            "the non-empty active table log must rotate"
        );
    }
    let requester = NodeId::from_legacy_named("rotated-admission-requester").expect("requester");

    crate::metadata_read_snapshot::reset_metadata_checkpoint_snapshot_decodes();
    crate::metadata_read_snapshot::reset_metadata_durable_table_probes();
    let admission = backend
        .namespace_request_admission_snapshot(&shard_id, &requester, "request-after-rotation")
        .expect("namespace admission after checkpoint rotation");

    assert_eq!(
        admission,
        crate::DfsNamespaceRequestAdmissionSnapshot::default()
    );
    assert_eq!(
        crate::metadata_read_snapshot::metadata_durable_table_probes(),
        1
    );
    assert_eq!(
        crate::metadata_read_snapshot::metadata_checkpoint_snapshot_decodes(),
        0,
        "a representation-only active-log rotation must not decode the accumulated checkpoint on admission"
    );

    cleanup(&root);
}

#[test]
fn sharded_namespace_request_admission_pins_each_shard_once() {
    let root = temp_root("sharded_namespace_request_admission_snapshot");
    let mut backend = LocalShardedMetadataBackend::new(
        &root,
        vec![
            shard_config("root", DfsMetadataShardAssignment::Root),
            shard_config(
                "services",
                DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services".to_string(),
                },
            ),
            shard_config(
                "users",
                DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/users".to_string(),
                },
            ),
        ],
    )
    .expect("sharded metadata backend");
    let requester = NodeId::from_legacy_named("sharded-admission-requester").expect("requester");

    crate::metadata_read_snapshot::reset_metadata_durable_table_probes();
    let admission = backend
        .namespace_request_admission_snapshot(
            &DfsMetadataShardId::new("services"),
            &requester,
            "request-fresh",
        )
        .expect("sharded namespace admission snapshot");
    assert_eq!(
        admission,
        crate::DfsNamespaceRequestAdmissionSnapshot::default()
    );
    assert_eq!(
        crate::metadata_read_snapshot::metadata_durable_table_probes(),
        3,
        "all admission indexes must share one durable probe per retained shard"
    );

    crate::metadata_read_snapshot::reset_metadata_durable_table_probes();
    let _ = backend
        .get_namespace_request_receipt(&requester, "request-fresh")
        .expect("standalone sharded receipt lookup");
    let _ = backend
        .get_namespace_request(&requester, "request-fresh")
        .expect("standalone sharded request lookup");
    let _ = backend
        .get_prepared_namespace_prepare_for_request(&requester, "request-fresh")
        .expect("standalone sharded prepare lookup");
    let _ = backend
        .namespace_request_history_expired_on_shard(
            &DfsMetadataShardId::new("services"),
            &requester,
            "request-fresh",
        )
        .expect("standalone sharded retention-floor lookup");
    assert_eq!(
        crate::metadata_read_snapshot::metadata_durable_table_probes(),
        12,
        "the regression fixture must cover four independent probes across three shards"
    );

    let pending = namespace_request(90, DfsNamespaceRequestStatus::Pending);
    crate::metadata_read_snapshot::reset_metadata_durable_table_probes();
    backend
        .put_namespace_request_on_shard(&DfsMetadataShardId::new("root"), &pending)
        .expect("persist request through combined admission");
    assert_eq!(
        crate::metadata_read_snapshot::metadata_durable_table_probes(),
        3,
        "request persistence must reuse the combined one-probe-per-shard admission"
    );

    cleanup(&root);
}

#[test]
fn local_backend_pages_namespace_jobs() {
    let root = temp_root("local_backend_pages_namespace_jobs");
    let mut backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &root).expect("backend");
    for index in 0..5 {
        backend
            .put_namespace_job(&namespace_job(index, &format!("/jobs/{index}")))
            .expect("namespace job");
    }

    let empty = backend
        .list_namespace_jobs_page(None, 0)
        .expect("empty page");
    assert!(empty.jobs.is_empty());
    assert!(empty.next_cursor.is_none());

    let mut cursor = None;
    let mut job_ids = Vec::new();
    let mut pages = 0usize;
    loop {
        let page = backend
            .list_namespace_jobs_page(cursor.as_ref(), 2)
            .expect("job page");
        assert!(page.jobs.len() <= 2);
        job_ids.extend(page.jobs.iter().map(|job| job.job_id.clone()));
        pages = pages.saturating_add(1);
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        cursor = Some(next_cursor);
    }
    assert_eq!(
        job_ids,
        (0..5)
            .map(|index| format!("job_namespace_page_{index:02}"))
            .collect::<Vec<_>>()
    );
    assert_eq!(pages, 3);
    assert_eq!(backend.list_namespace_jobs().expect("legacy list").len(), 5);

    cleanup(&root);
}

#[test]
fn local_backend_pages_only_pending_namespace_requests_and_rebuilds_index() {
    let root = temp_root("local_backend_pages_pending_namespace_requests");
    let shard_id = DfsMetadataShardId::new("root");
    let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
    let pending_later = namespace_request(2, DfsNamespaceRequestStatus::Pending);
    let pending_earlier = namespace_request(1, DfsNamespaceRequestStatus::Pending);
    let completed = namespace_request(0, DfsNamespaceRequestStatus::Completed);
    for request in [&pending_later, &completed, &pending_earlier] {
        backend
            .put_namespace_request_on_shard(&shard_id, request)
            .expect("namespace request");
    }

    let first = backend
        .list_pending_namespace_requests_page(None, 1)
        .expect("first pending request page");
    assert_eq!(
        first
            .requests
            .iter()
            .map(|request| request.request_id.as_str())
            .collect::<Vec<_>>(),
        vec![pending_earlier.request_id.as_str()]
    );
    let second = backend
        .list_pending_namespace_requests_page(first.next_cursor.as_ref(), 1)
        .expect("second pending request page");
    assert_eq!(
        second
            .requests
            .iter()
            .map(|request| request.request_id.as_str())
            .collect::<Vec<_>>(),
        vec![pending_later.request_id.as_str()]
    );
    assert!(second.next_cursor.is_none());

    let mut retried = pending_earlier.clone();
    retried.recovery_attempts = 1;
    retried.updated_at = OffsetDateTime::from_unix_timestamp(3).expect("retry timestamp");
    backend
        .put_namespace_request_on_shard(&shard_id, &retried)
        .expect("retry diagnostics");
    let reordered = backend
        .list_pending_namespace_requests_page(None, 8)
        .expect("reordered pending requests");
    assert_eq!(
        reordered
            .requests
            .iter()
            .map(|request| request.request_id.as_str())
            .collect::<Vec<_>>(),
        vec![
            pending_later.request_id.as_str(),
            pending_earlier.request_id.as_str()
        ],
        "updating recovery diagnostics must replace the old index key"
    );

    let mut completed_later = pending_later.clone();
    completed_later.status = DfsNamespaceRequestStatus::Completed;
    completed_later.result = Some(DfsNamespaceResult::Applied);
    completed_later.updated_at =
        OffsetDateTime::from_unix_timestamp(4).expect("completion timestamp");
    backend
        .put_namespace_request_on_shard(&shard_id, &completed_later)
        .expect("completed request");
    drop(backend);

    let reopened = LocalSobjMetadataBackend::new(shard_id, &root).expect("reopen backend");
    let pending = reopened
        .list_pending_namespace_requests_page(None, 8)
        .expect("pending requests after reopen");
    assert_eq!(pending.requests, vec![retried]);
    assert!(pending.next_cursor.is_none());
    assert_eq!(
        reopened
            .list_namespace_requests()
            .expect("retained idempotency requests")
            .len(),
        3,
        "terminal requests remain available for exact retry replay"
    );

    cleanup(&root);
}

#[test]
fn local_backend_recovers_interrupted_namespace_transaction() {
    let root = temp_root("local_backend_recovers_interrupted_namespace_transaction");
    let mut backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("backend");
    let file_system = file_system();
    let mut source = object(version("version_1"));
    source.path = "/services/web/index.html".to_string();
    let mut destination = object(version("version_1"));
    destination.object_id = DfsObjectId::from_legacy_named("obj_current").expect("object");
    destination.path = "/services/web/current.html".to_string();
    backend.put_file_system(&file_system).expect("filesystem");
    backend.put_object(&source).expect("source");
    backend.put_object(&destination).expect("destination");

    source.path = "/services/web/current.html".to_string();
    destination.kind = DfsObjectKind::Tombstone;
    destination.version = version("version_2");
    let transaction = DfsNamespaceTransaction {
        file_version_pins: None,
        transaction_id: "namespace_test_rename".to_string(),
        file_system_id: file_system_id(),
        objects: vec![source.clone(), destination.clone()],
        versions: Vec::new(),
        advisory_lock_states: Vec::new(),
        created_at: OffsetDateTime::UNIX_EPOCH,
    };
    backend
        .apply_namespace_transaction(&transaction)
        .expect("transaction");
    assert_eq!(
        metadata_table_batch_count(&root),
        4,
        "namespace transactions should materialize all table mutations for one journal entry in one table batch"
    );

    force_table_replay_from_journal(&backend, &root, 0);
    drop(backend);

    let recovered = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("recovered backend");
    assert_eq!(
        recovered
            .get_object_by_path(&file_system_id(), "/services/web/current.html")
            .expect("lookup current")
            .expect("current object")
            .object_id,
        source.object_id
    );
    assert!(
        recovered
            .get_object_by_path(&file_system_id(), "/services/web/index.html")
            .expect("lookup old")
            .is_none()
    );

    cleanup(&root);
}

#[test]
fn local_backend_reopen_does_not_rejournal_healthy_namespace_transactions() {
    let root = temp_root("local_backend_reopen_does_not_rejournal_namespace_transactions");
    let mut backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("backend");
    let file_system = file_system();
    let mut source = object(version("version_1"));
    source.path = "/services/web/index.html".to_string();
    let mut destination = object(version("version_1"));
    destination.object_id = DfsObjectId::from_legacy_named("obj_current").expect("object");
    destination.path = "/services/web/current.html".to_string();
    backend.put_file_system(&file_system).expect("filesystem");
    backend.put_object(&source).expect("source");
    backend.put_object(&destination).expect("destination");

    source.path = "/services/web/current.html".to_string();
    destination.kind = DfsObjectKind::Tombstone;
    destination.version = version("version_2");
    let transaction = DfsNamespaceTransaction {
        file_version_pins: None,
        transaction_id: "namespace_test_rename".to_string(),
        file_system_id: file_system_id(),
        objects: vec![source, destination],
        versions: Vec::new(),
        advisory_lock_states: Vec::new(),
        created_at: OffsetDateTime::UNIX_EPOCH,
    };
    backend
        .apply_namespace_transaction(&transaction)
        .expect("transaction");
    let journal_len = backend.journal_since(0).expect("journal").len();
    drop(backend);

    reset_namespace_recovery_planner_calls();
    let reopened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("reopened backend");

    assert_eq!(
        namespace_recovery_planner_calls(),
        0,
        "healthy transaction history must not clone tables through the recovery planner"
    );
    assert_eq!(
        reopened.journal_since(0).expect("journal").len(),
        journal_len,
        "opening a healthy backend must not append duplicate namespace transaction journal entries"
    );
    cleanup(&root);
}

#[test]
fn local_backend_put_object_and_version_uses_one_table_batch_without_namespace_transaction() {
    let root = temp_root("local_backend_object_version_grouped_table_batch");
    let mut backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("backend");
    let file_system = file_system();
    backend.put_file_system(&file_system).expect("filesystem");
    let mut object = object(version("version_2"));
    object.path = "/services/web/grouped.bin".to_string();
    object.content_hash = Some("blake3:grouped-object-version".to_string());
    object.content_size_bytes = Some(4096);
    let version_record = DfsObjectVersionRecord {
        content_durability: None,
        history_order: 0,
        retained: None,
        file_system_id: object.file_system_id.clone(),
        object_id: object.object_id.clone(),
        version: object.version.clone(),
        content_hash: object.content_hash.clone(),
        size_bytes: object.content_size_bytes,
        locations: object.current_locations.clone(),
        created_by_event_id: None,
        created_at: OffsetDateTime::UNIX_EPOCH,
        visible: true,
    };
    let table_batches_before = metadata_table_batch_count(&root);

    let entries = backend
        .put_object_and_version(&object, &version_record)
        .expect("grouped object version");

    assert_eq!(entries.len(), 2);
    assert!(matches!(
        entries[0].mutation,
        DfsMetadataMutation::PutObject { .. }
    ));
    assert!(matches!(
        entries[1].mutation,
        DfsMetadataMutation::PutVersion { .. }
    ));
    assert_eq!(
        metadata_table_batch_count(&root),
        table_batches_before + 1,
        "object and visible version should materialize in one table batch"
    );
    assert_eq!(
        backend
            .get_object(&object.object_id)
            .expect("object lookup"),
        Some(object.clone())
    );
    assert_eq!(
        backend
            .get_version(&object.object_id, &object.version)
            .expect("version lookup"),
        Some(normalized_version_record(&version_record))
    );
    assert!(backend.tables.namespace_transactions.is_empty());
    let journal_len = backend.journal_since(0).expect("journal").len();
    drop(backend);

    let reopened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("reopened backend");
    assert_eq!(
        reopened.journal_since(0).expect("journal").len(),
        journal_len,
        "grouped object/version write must not create a recoverable namespace transaction"
    );
    cleanup(&root);
}

#[test]
fn namespace_transaction_publishes_object_and_version_in_one_table_batch() {
    let root = temp_root("namespace_transaction_object_version_single_batch");
    let mut backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("backend");
    let file_system = file_system();
    backend.put_file_system(&file_system).expect("filesystem");
    let mut object = object(version("version_2"));
    object.path = "/services/web/app.bin".to_string();
    object.content_hash = Some("blake3:namespace-version".to_string());
    object.content_size_bytes = Some(16 * 1024 * 1024 * 1024);
    let version_record = DfsObjectVersionRecord {
        content_durability: None,
        history_order: 0,
        retained: None,
        file_system_id: object.file_system_id.clone(),
        object_id: object.object_id.clone(),
        version: object.version.clone(),
        content_hash: object.content_hash.clone(),
        size_bytes: object.content_size_bytes,
        locations: object.current_locations.clone(),
        created_by_event_id: None,
        created_at: OffsetDateTime::UNIX_EPOCH,
        visible: true,
    };
    let table_batches_before = metadata_table_batch_count(&root);

    backend
        .apply_namespace_transaction(&DfsNamespaceTransaction {
            file_version_pins: None,
            transaction_id: "namespace_object_version_single_batch".to_string(),
            file_system_id: object.file_system_id.clone(),
            objects: vec![object.clone()],
            versions: vec![version_record.clone()],
            advisory_lock_states: Vec::new(),
            created_at: OffsetDateTime::UNIX_EPOCH,
        })
        .expect("transaction");

    assert_eq!(
        metadata_table_batch_count(&root),
        table_batches_before + 1,
        "creating an object and its visible version should materialize in one table batch"
    );
    assert_eq!(
        backend
            .get_object(&object.object_id)
            .expect("object lookup"),
        Some(object.clone())
    );
    assert_eq!(
        backend
            .get_version(&object.object_id, &object.version)
            .expect("version lookup"),
        Some(normalized_version_record(&version_record))
    );
    let table_batches_after_transaction = metadata_table_batch_count(&root);
    drop(backend);

    let reopened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("reopen backend");

    assert_eq!(
        reopened
            .get_version(&object.object_id, &object.version)
            .expect("reopened version lookup"),
        Some(normalized_version_record(&version_record))
    );
    assert_eq!(
        metadata_table_batch_count(&root),
        table_batches_after_transaction,
        "replaying a deferred local cursor must not duplicate namespace transaction table batches"
    );

    cleanup(&root);
}

#[test]
fn sharded_namespace_transaction_moves_lock_aggregate_with_the_stable_object() {
    let root = temp_root("sharded_namespace_transaction_moves_lock_aggregate");
    let configs = vec![
        shard_config("root", DfsMetadataShardAssignment::Root),
        shard_config(
            "projects",
            DfsMetadataShardAssignment::PathPrefix {
                prefix: "/projects".to_string(),
            },
        ),
    ];
    let mut backend = LocalShardedMetadataBackend::new(&root, configs.clone()).expect("sharded");
    backend.put_file_system(&file_system()).expect("filesystem");
    let mut project = object(version("project-lock-move-v1"));
    project.object_id =
        DfsObjectId::from_legacy_named("project_lock_move_object").expect("project object id");
    project.path = "/projects/repo/readme.md".to_string();
    backend.put_object(&project).expect("project object");
    backend
        .apply_advisory_lock_command(
            &project.file_system_id,
            &project.object_id,
            DfsAdvisoryLockTransitionContext {
                authority_epoch: 1,
                now_unix_nanos: 1,
                lease_duration_nanos: 10,
                max_clock_skew_nanos: 0,
            },
            &DfsAdvisoryLockCommand::Acquire {
                request_id: DfsAdvisoryLockRequestId::new("project-move-acquire"),
                session_id: DfsAdvisoryLockSessionId::new("project-move-session"),
                mode: DfsAdvisoryLockMode::Exclusive,
                blocking: false,
                process_id: None,
            },
        )
        .expect("project lock");
    let source_state = backend
        .get_advisory_lock_state(&project.file_system_id, &project.object_id)
        .expect("source lock state")
        .expect("source lock aggregate");
    let (expected_state, _) = prepare_advisory_lock_state_for_migration(&source_state)
        .expect("prepare expected migration state");

    let mut moved_project = project.clone();
    moved_project.path = "/archive/readme.md".to_string();
    backend
        .apply_namespace_transaction(&DfsNamespaceTransaction {
            file_version_pins: None,
            transaction_id: "txn_project_lock_move".to_string(),
            file_system_id: project.file_system_id.clone(),
            objects: vec![moved_project.clone()],
            versions: Vec::new(),
            advisory_lock_states: Vec::new(),
            created_at: OffsetDateTime::UNIX_EPOCH,
        })
        .expect("move namespace transaction");

    assert_eq!(
        backend
            .get_object(&project.object_id)
            .expect("moved object")
            .as_ref()
            .map(|object| object.path.as_str()),
        Some(moved_project.path.as_str())
    );
    assert_eq!(
        backend
            .get_advisory_lock_state(&project.file_system_id, &project.object_id)
            .expect("moved lock state"),
        Some(expected_state.clone()),
        "a local cross-shard namespace transaction must not strand the lock aggregate on the source shard"
    );

    drop(backend);
    let reopened = LocalShardedMetadataBackend::new(&root, configs).expect("reopen sharded");
    assert_eq!(
        reopened
            .get_advisory_lock_state(&project.file_system_id, &project.object_id)
            .expect("reopened moved lock state"),
        Some(expected_state)
    );

    cleanup(&root);
}

#[test]
fn namespace_job_terminal_state_rejects_stale_runner_regression() {
    let root = temp_root("namespace-job-terminal-monotonic");
    let shard_id = DfsMetadataShardId::new("root");
    let running = namespace_job(91, "/users/terminal-monotonic");
    let mut completed = running.clone();
    completed.status = DfsNamespaceJobStatus::Completed;
    completed.processed_object_count = 1;
    completed.updated_at += time::Duration::seconds(1);
    completed.completed_at = Some(completed.updated_at);

    let mut backend = LocalSobjMetadataBackend::new(shard_id, &root).expect("metadata backend");
    backend.put_file_system(&file_system()).expect("filesystem");
    backend.put_namespace_job(&running).expect("running job");
    backend.put_namespace_job(&completed).expect("complete job");
    backend
        .put_namespace_job(&running)
        .expect("stale running retry is an idempotent no-op");
    assert_eq!(
        backend
            .get_namespace_job(&completed.job_id)
            .expect("terminal job lookup"),
        Some(completed.clone())
    );

    let mut conflicting_terminal = completed;
    conflicting_terminal.status = DfsNamespaceJobStatus::Failed;
    conflicting_terminal.failure_reason = Some("stale runner failure".to_string());
    assert!(
        backend
            .put_namespace_job(&conflicting_terminal)
            .expect_err("one terminal outcome cannot replace another")
            .to_string()
            .contains("cannot replace terminal state")
    );

    cleanup(&root);
}

#[test]
fn namespace_job_receipt_survives_checkpoint_and_exact_terminal_retry() {
    let root = temp_root("namespace-job-receipt-checkpoint");
    let shard_id = DfsMetadataShardId::new("root");
    let mut request = namespace_request(92, DfsNamespaceRequestStatus::Pending);
    request.created_at = OffsetDateTime::UNIX_EPOCH;
    request.updated_at = request.created_at;
    let origin = DfsNamespaceJobOrigin::from_request(&request);
    let mut job = namespace_job(92, "/users/source");
    job.origin = Some(origin.clone());
    job.kind = DfsNamespaceJobKind::RenameObjectAcrossShardPrimaries;
    job.status = DfsNamespaceJobStatus::Completed;
    job.target_path = Some("/services/target".to_string());
    job.completed_at = Some(job.updated_at);
    let receipt = DfsNamespaceJobReceipt::from_job(&job, job.updated_at)
        .expect("completed namespace job receipt");

    let mut backend = LocalSobjMetadataBackend::new(shard_id, &root).expect("metadata backend");
    backend.put_file_system(&file_system()).expect("filesystem");
    backend.put_namespace_job(&job).expect("namespace job");
    backend
        .put_namespace_job_receipt(&receipt)
        .expect("namespace job receipt");
    assert_eq!(
        backend
            .completed_namespace_job_receipt_for_paths_since(
                &job.file_system_id,
                &job.root_path,
                job.target_path.as_deref().expect("target path"),
                &[job.kind.clone()],
                request.created_at,
                &origin,
            )
            .expect("completed receipt lookup"),
        Some(receipt.clone())
    );
    backend
        .compact_metadata_table_log_if_needed(0)
        .expect("checkpoint receipt");
    drop(backend);

    let mut reopened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &root)
        .expect("reopen checkpoint");
    assert_eq!(
        reopened
            .get_namespace_job_receipt(&job.job_id)
            .expect("reopened receipt"),
        Some(receipt.clone())
    );
    reopened
        .put_namespace_job(&job)
        .expect("exact terminal retry uses compact receipt");
    let mut conflicting = job;
    conflicting.root_path = "/users/different-source".to_string();
    assert!(
        reopened
            .put_namespace_job(&conflicting)
            .expect_err("conflicting job retry must remain fenced")
            .to_string()
            .contains("different payload")
    );

    cleanup(&root);
}
