//! Namespace requests regression tests.

use super::*;

#[test]
fn mkdir_all_commits_a_missing_chain_in_one_namespace_transaction() {
    let node = NodeId::from_legacy_named("node_mkdir_all_single_transaction").expect("node id");
    let mut runtime = crate::test_runtime::DfsTestRuntime::new(
        "mkdir_all_commits_a_missing_chain_in_one_namespace_transaction",
        node.clone(),
    )
    .unwrap();
    runtime.start_nodes().unwrap();
    let mut dfs = runtime.client(&node).unwrap();
    dfs.bootstrap_distributed_sifs().expect("bootstrap SIFS");

    let shard = DfsMetadataShardId::new("services");
    let physical_root = crate::local_dfs_metadata_shard_path(runtime.data_dir(&node), &shard);
    let anchor = {
        let inspector = runtime
            .metadata_primary_service("/services")
            .expect("normal current Main/Core prefix inspector");
        inspector
            .metadata_backend()
            .unwrap()
            .metadata_prefix_proof(&shard)
            .expect("actual installed current prefix")
            .expect("actual services shard prefix")
    };
    let before = crate::inspect_metadata_prefix_descendant(&physical_root, &shard, &anchor)
        .expect("actual current source inspection")
        .expect("complete actual source prefix");
    dfs.posix_mkdir_all("/services/batched/one/two")
        .expect("create directory chain");
    let after = crate::inspect_metadata_prefix_descendant(&physical_root, &shard, &before)
        .expect("actual Main publication source inspection")
        .expect("the exact same source extends its original prefix");
    assert_eq!(
        after.journal_sequence - before.journal_sequence,
        2,
        "one mkdir-all operation must seal its complete chain in one prepare and one digest commit"
    );
    let chain_before_repeat = {
        let inspector = runtime
            .metadata_primary_service("/services/batched/one/two")
            .expect("normal current Main/Core inspector");
        let batch = inspector
            .metadata_backend()
            .unwrap()
            .journal_batch_for_shard_since_limited(&shard, before.journal_sequence, 2, usize::MAX)
            .unwrap()
            .expect("the actual same-shard durable journal");
        assert_eq!(batch.entries.len(), 2);
        let DfsMetadataMutation::PrepareOperationV7 { record, .. } = &batch.entries[0].mutation
        else {
            panic!("actual mkdir-all immutable preparation");
        };
        let DfsMetadataMutation::CommitPreparedOperationV7 { digest, .. } =
            &batch.entries[1].mutation
        else {
            panic!("actual mkdir-all digest commitment");
        };
        assert_eq!(record.digest, *digest);
        for path in [
            "/services/batched",
            "/services/batched/one",
            "/services/batched/one/two",
        ] {
            assert!(
                record
                    .payload
                    .transaction
                    .objects
                    .iter()
                    .any(|object| object.path == path && object.kind == DfsObjectKind::Directory)
            );
        }
        record
            .payload
            .transaction
            .objects
            .iter()
            .filter(|object| {
                matches!(
                    object.path.as_str(),
                    "/services/batched" | "/services/batched/one" | "/services/batched/one/two"
                )
            })
            .map(|object| {
                inspector
                    .metadata_backend()
                    .unwrap()
                    .get_object_on_shard(&shard, &object.object_id)
                    .unwrap()
                    .expect("actual committed chain object")
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(chain_before_repeat.len(), 3);
    for path in [
        "/services/batched",
        "/services/batched/one",
        "/services/batched/one/two",
    ] {
        assert_eq!(
            dfs.posix_stat(path).expect("created directory").kind,
            DfsObjectKind::Directory
        );
    }

    dfs.posix_mkdir_all("/services/batched/one/two")
        .expect("repeat existing directory chain");
    let repeat_after = crate::inspect_metadata_prefix_descendant(&physical_root, &shard, &after)
        .expect("actual repeated Main operation inspection")
        .expect("the same actual source extends its known prefix");
    assert_eq!(
        repeat_after.journal_sequence - after.journal_sequence,
        2,
        "a fresh routed retry records only its canonical completed request"
    );
    {
        let inspector = runtime
            .metadata_primary_service("/services/batched/one/two")
            .expect("normal current Main/Core repeat inspector");
        let backend = inspector.metadata_backend().unwrap();
        let batch = backend
            .journal_batch_for_shard_since_limited(&shard, after.journal_sequence, 2, usize::MAX)
            .unwrap()
            .expect("actual repeat completion kernel pair");
        assert_eq!(batch.entries.len(), 2);
        assert_eq!(batch.entries[0].sequence, after.journal_sequence + 1);
        assert_eq!(batch.entries[1].sequence, repeat_after.journal_sequence);
        let DfsMetadataMutation::PrepareOperationV7 { record, .. } = &batch.entries[0].mutation
        else {
            panic!("actual completed-request preparation");
        };
        let DfsMetadataMutation::CommitPreparedOperationV7 { digest, .. } =
            &batch.entries[1].mutation
        else {
            panic!("actual completed-request digest commitment");
        };
        assert_eq!(record.digest, *digest);
        let payload = &record.payload;
        assert!(payload.expected_object_versions.is_empty());
        assert!(payload.expected_path_occupants.is_empty());
        assert!(payload.transaction.objects.is_empty());
        assert!(payload.transaction.versions.is_empty());
        assert!(payload.transaction.advisory_lock_states.is_empty());
        assert!(payload.transaction.extension_expectations.is_empty());
        assert!(payload.transaction.file_version_pins.is_none());
        assert!(payload.transaction.content_reference_identity.is_none());
        assert!(payload.transaction.contribution_digest.is_none());
        assert!(payload.content_receipts.is_empty());
        assert!(payload.required_replica_node_ids.is_empty());
        assert!(payload.prepared_intent.is_none() && payload.terminal_intent.is_none());
        assert!(
            payload.completed_request.is_none() && payload.completed_request_completion.is_none()
        );
        let [DfsPreparedMetadataEffect::PutNamespaceRequest { request }] =
            payload.metadata_effects.as_slice()
        else {
            panic!("repeat publishes exactly one completed namespace request row");
        };
        assert_eq!(request.file_system_id, payload.file_system_id);
        assert_eq!(request.requester_node_id, node);
        assert_eq!(request.authority_shard_id, shard);
        assert_eq!(request.status, DfsNamespaceRequestStatus::Completed);
        assert_eq!(request.result, Some(DfsNamespaceResult::Applied));
        assert!(
            matches!(&request.operation, DfsNamespaceOperation::MkdirAll { path }
            if path == "/services/batched/one/two")
        );
        for object in &chain_before_repeat {
            assert_eq!(
                backend
                    .get_object_on_shard(&shard, &object.object_id)
                    .unwrap(),
                Some(object.clone()),
                "repeat must preserve the exact existing directory and version"
            );
        }
    }
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn reader_backed_stable_create_replays_after_reopen_and_rejects_a_new_digest() {
    let node_id = NodeId::from_legacy_named("node_reader_stable_create").expect("node");
    let path = "/documents/reader-create.txt";
    let operation_id = format!("cf_{}", sinomo_core::Id::generate());
    let options = DfsWriteOptions::replicated_eventual(vec![node_id.clone()]);

    let mut runtime = crate::test_runtime::DfsTestRuntime::new(
        "reader_backed_stable_create_replays_after_reopen_and_rejects_a_new_digest",
        node_id.clone(),
    )
    .unwrap();
    let trace_before = crate::test_support::NativeShardTraceArtifactSession::from_environment(
        "native-reader-digest-before-reopen",
        [(
            node_id.clone(),
            runtime.data_dir(&node_id).root().to_owned(),
        )],
        DfsMetadataShardId::new("root"),
    )
    .expect("actual first owner capture before mount");
    runtime.start_nodes().unwrap();
    let created = {
        let mut dfs = runtime.client(&node_id).unwrap();
        dfs.posix_mkdir_all("/documents").expect("mkdir");
        dfs.posix_create_file_from_reader_with_operation_id(
            path,
            std::io::Cursor::new(b"captured once".to_vec()),
            options.clone(),
            &operation_id,
        )
        .expect("stable reader create")
    };

    if let Some(trace) = &trace_before {
        trace.close().unwrap();
    }
    runtime.stop_node(&node_id).unwrap();
    if let Some(trace) = trace_before {
        trace.export().unwrap();
    }
    // The prior request is consumed before this new actual mount is admitted.
    // No capture identity is relabeled across the real owner reincarnation.
    let trace_after = crate::test_support::NativeShardTraceArtifactSession::from_environment(
        "native-reader-digest-after-reopen",
        [(
            node_id.clone(),
            runtime.data_dir(&node_id).root().to_owned(),
        )],
        DfsMetadataShardId::new("root"),
    )
    .expect("actual reopened owner capture before its new mount");
    runtime.reopen_node(node_id.clone()).unwrap();
    let mut restarted = runtime.client(&node_id).unwrap();
    assert_eq!(
        created.version,
        crate::intent::stable_write_target_version(&node_id, &created.object_id, &operation_id,),
        "stable create must publish its deterministic target version",
    );
    let reopened = restarted.posix_stat(path).expect("reopen created file");
    assert_eq!(reopened.object_id, created.object_id);
    assert_eq!(reopened.version, created.version);
    let durable_object = restarted
        .metadata_backend()
        .expect("backend")
        .get_object(&created.object_id)
        .expect("get durable object")
        .expect("durable object");
    let expected_event = EventId::derived(
        "sinomo-dfs.routed-exclusive-create.v1",
        format!(
            "{}\0{}\0{}\0{:?}\0{}\0{}\0{}",
            operation_id,
            durable_object.object_id,
            durable_object.path,
            durable_object.kind,
            durable_object.mode,
            durable_object.uid,
            durable_object.gid,
        )
        .as_bytes(),
    );
    assert_eq!(
        durable_object.last_modified_by_event_id.as_ref(),
        Some(&expected_event),
        "stable exclusive create must retain deterministic replay provenance",
    );
    let replayed = restarted
        .posix_create_file_from_reader_with_operation_id(
            path,
            std::io::Cursor::new(b"captured once".to_vec()),
            options.clone(),
            &operation_id,
        )
        .expect("reconcile dropped create response after restart");
    assert_eq!(replayed.object_id, created.object_id);
    assert_eq!(replayed.version, created.version);
    assert_eq!(
        restarted.posix_read_file(path).expect("replayed bytes"),
        b"captured once"
    );

    assert!(matches!(
        restarted.posix_create_file_from_reader_with_operation_id(
            path,
            std::io::Cursor::new(b"different content".to_vec()),
            options,
            &operation_id,
        ),
        Err(DfsPosixError::Conflict { .. }) | Err(DfsPosixError::Dfs(_))
    ));
    assert_eq!(
        restarted
            .posix_read_file(path)
            .expect("original bytes remain"),
        b"captured once"
    );

    drop(restarted);
    if let Some(trace) = &trace_after {
        trace.close().unwrap();
    }
    runtime.close().unwrap();
    if let Some(trace) = trace_after {
        trace.export().unwrap();
    }
}

#[test]
fn reader_backed_stable_cas_replays_but_rejects_a_recreated_path() {
    let node_id = NodeId::from_legacy_named("node_reader_stable_cas").expect("node");
    let path = "/documents/reader-cas.txt";
    let operation_id = format!("cf_{}", sinomo_core::Id::generate());
    let options = DfsWriteOptions::replicated_eventual(vec![node_id.clone()]);
    let (mut runtime, setup) = native_sifs_service(
        "reader_backed_stable_cas_replays_but_rejects_a_recreated_path",
        node_id.clone(),
    );
    drop(setup);
    let (opened, saved) = {
        let mut dfs = runtime.client(&node_id).unwrap();
        dfs.posix_mkdir_all("/documents").expect("mkdir");
        let opened = dfs
            .posix_write_file(path, b"opened", options.clone())
            .expect("seed file");
        let saved = dfs
            .posix_replace_file_if_version_from_reader_with_operation_id(
                path,
                &opened.object_id,
                &opened.version,
                std::io::Cursor::new(b"saved by reader".to_vec()),
                options.clone(),
                &operation_id,
            )
            .expect("reader CAS");
        (opened, saved)
    };

    runtime.stop_node(&node_id).unwrap();
    runtime.reopen_node(node_id.clone()).unwrap();
    let mut restarted = runtime.client(&node_id).unwrap();
    let replayed = restarted
        .posix_replace_file_if_version_from_reader_with_operation_id(
            path,
            &opened.object_id,
            &opened.version,
            std::io::Cursor::new(b"saved by reader".to_vec()),
            options.clone(),
            &operation_id,
        )
        .expect("reconcile dropped CAS response after restart");
    assert_eq!(replayed.version, saved.version);

    restarted.posix_unlink(path).expect("unlink old identity");
    let recreated = restarted
        .posix_write_file(path, b"replacement identity", options.clone())
        .expect("recreate path");
    assert_ne!(recreated.object_id, opened.object_id);
    assert!(matches!(
        restarted.posix_replace_file_if_version_from_reader_with_operation_id(
            path,
            &opened.object_id,
            &opened.version,
            std::io::Cursor::new(b"must not overwrite replacement".to_vec()),
            options,
            &operation_id,
        ),
        Err(DfsPosixError::Conflict { .. })
    ));
    assert_eq!(
        restarted
            .posix_read_file(path)
            .expect("replacement remains"),
        b"replacement identity"
    );

    drop(restarted);
    runtime.close().unwrap();
}

#[test]
fn sifs_posix_recreate_directory_uses_a_new_namespace_transaction_generation() {
    let root = temp_root("sifs-posix-recreate-dir-transaction-generation");
    let node_id = NodeId::from_legacy_named("node_sifs_recreate_dir_transaction").expect("node");
    let parent_path = "/services/sandbox/instances/sandbox_recreate_dir_transaction/work";
    let dir_path = "/services/sandbox/instances/sandbox_recreate_dir_transaction/work/recreated";

    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_posix_recreate_directory_uses_a_new_namespace_transaction_generation",
        node_id,
    );
    dfs.posix_mkdir_all(parent_path).expect("mkdir parent");
    let original = dfs.posix_mkdir(dir_path).expect("initial mkdir");
    dfs.posix_rmdir(dir_path).expect("rmdir");
    let recreated = dfs.posix_mkdir(dir_path).expect("recreate dir");

    assert_eq!(recreated.object_id, original.object_id);
    assert_ne!(recreated.version, original.version);
    assert_eq!(dfs.posix_stat(dir_path).expect("recreated stat"), recreated);

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn native_namespace_request_replay_survives_actual_primary_restart() {
    let nodes =
        crate::test_runtime::DfsTestRuntime::ordered_node_ids("native-namespace-replay-restart", 3);
    let mut runtime = crate::test_runtime::DfsTestRuntime::new(
        "native-namespace-replay-restart",
        nodes[0].clone(),
    )
    .unwrap();
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).unwrap();
    }
    runtime.start_nodes().unwrap();
    let primary = runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|shard| shard.shard_id == "services")
        .unwrap()
        .primary_node_id;
    let requester = nodes.iter().find(|node| **node != primary).unwrap().clone();
    let mut writer = runtime.client(&requester).unwrap();
    let from = "/services/native-replay-source";
    let to = "/services/native-replay-target";
    let source = writer
        .posix_write_file(
            from,
            b"native request bytes",
            DfsWriteOptions::replicated_eventual(nodes.clone()),
        )
        .unwrap();
    let operation = sinomo_core::Id::generate().to_string();
    let moved = writer
        .posix_rename_with_operation_id(from, to, &operation)
        .unwrap();
    assert_eq!(moved.object_id, source.object_id);
    drop(writer);
    runtime.stop_node(&primary).unwrap();
    runtime.reopen_node(primary).unwrap();
    let mut reader = runtime.client(&requester).unwrap();
    let replay = reader
        .posix_rename_with_operation_id(from, to, &operation)
        .unwrap();
    assert_eq!(replay, moved);
    assert_eq!(
        reader.posix_read_file_distributed(to).unwrap(),
        b"native request bytes"
    );
    assert!(matches!(
        reader.posix_stat_distributed(from),
        Err(DfsPosixError::NotFound { .. })
    ));
    drop(reader);
    runtime.close().unwrap();
}
