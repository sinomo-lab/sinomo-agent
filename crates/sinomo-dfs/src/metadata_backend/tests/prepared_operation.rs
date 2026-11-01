//! Native PreparedV7 kernel regressions; author guards come from real hosted services.
mod immutable_payload;
mod conditional_current;
mod checkpoint_cache;
mod owned_namespace_plan;
mod quiet_open;
mod local_cursor;
use super::*;
use crate::DfsControlPlaneStore;
use crate::test_support::{DfsTestRuntime, current_native_metadata_service};
type NativeService = crate::NetworkedDfsService<crate::DfsNodeTransport>;

fn native_case(
    name: &str,
    count: usize,
) -> (DfsTestRuntime, NativeService, DfsMetadataShardConfig) {
    let (runtime, delegated, _) = current_native_metadata_service(name, count, "/services")
        .expect("normal owned native metadata fixture");
    drop(delegated);
    let config = runtime
        .metadata_shards()
        .expect("actual current native metadata projection")
        .into_iter()
        .find(|shard| shard.shard_id == "services")
        .expect("actual owning services shard");
    let service = runtime
        .metadata_primary_service("/services")
        .expect("the normal Main/Core effect opener uses its current physical generation");
    (runtime, service, config)
}
fn directory(service: &mut NativeService, path: &str) -> DfsObjectMetadata {
    service
        .posix_mkdir_all(path)
        .expect("ordinary guarded directory initialization");
    service
        .lookup_sifs_path(path)
        .unwrap()
        .expect("actual initialized directory")
}
fn namespace_payload(
    config: &DfsMetadataShardConfig,
    current: &DfsObjectMetadata,
    committed: DfsObjectMetadata,
    operation: &str,
) -> crate::DfsPreparedOperationPayload {
    crate::DfsPreparedOperationPayload::for_namespace_transaction(
        crate::DfsPreparedOperationScope {
            file_system_id: current.file_system_id.clone(),
            shard_id: DfsMetadataShardId::new(config.shard_id.clone()),
            owner_epoch: config.epoch,
            operation_id: operation.into(),
        },
        vec![DfsNamespaceObjectVersionExpectation {
            object_id: current.object_id.clone(),
            expected_version: Some(current.version.clone()),
        }],
        vec![DfsNamespacePathOccupantExpectation {
            path: current.path.clone(),
            expected_object_id: Some(current.object_id.clone()),
        }],
        DfsNamespaceTransaction {
            content_reference_identity: None,
            contribution_digest: None,
            extension_expectations: Vec::new(),
            file_version_pins: None,
            transaction_id: format!("transaction:{operation}"),
            file_system_id: current.file_system_id.clone(),
            objects: vec![committed],
            versions: Vec::new(),
            advisory_lock_states: Vec::new(),
            created_at: OffsetDateTime::now_utc(),
        },
        Vec::new(),
        None,
        None,
        Vec::new(),
        None,
        None,
    )
    .unwrap()
}
fn entries(service: &NativeService, shard: &DfsMetadataShardId) -> Vec<DfsMetadataJournalEntry> {
    service
        .metadata_backend()
        .unwrap()
        .journal_batch_for_shard_since_limited(shard, 0, usize::MAX, usize::MAX)
        .unwrap()
        .expect("actual native shard journal")
        .entries
}

#[cfg(feature = "storage-conversion")]
#[test]
fn candidate_journal_recovers_head_hints_but_requires_the_actual_prepare() {
    let (mut runtime, mut service, config) = native_case("candidate-head-hint-recovery", 1);
    let shard = DfsMetadataShardId::new(config.shard_id.clone());
    let current = directory(&mut service, "/services/candidate-head-hint-recovery");
    let mut changed = current.clone();
    changed.version = DfsVersion::generate();
    changed.mode = 0o600;
    let payload = namespace_payload(&config, &current, changed, "candidate-hint-body");
    let base = service
        .metadata_backend()
        .unwrap()
        .metadata_prefix_proof(&shard)
        .unwrap()
        .unwrap();
    // These are hidden candidates populated from an actual native producer.
    // CandidateWriter grants no author guard and does not publish this body.
    let temporary = sinomo_testdir::TestDir::new("candidate-head-hint-images");
    for variant in ["missing", "corrupt", "stale"] {
        let root = temporary.join(variant);
        let writer = crate::DfsMetadataCandidateWriter::open(&root).unwrap();
        let (prepared, after_prepare) = writer.prepare(payload.clone(), &base).unwrap();
        let head = root.join("journal/head.sobj");
        fs::rename(&head, root.join("retained-head-after-prepare.sobj")).unwrap();
        match variant {
            "corrupt" => fs::write(&head, b"torn hint").unwrap(),
            "stale" => crate::metadata_cursor::write_journal_head_hint(&root, 1).unwrap(),
            "missing" => {}
            _ => unreachable!(),
        }
        drop(writer);
        assert_eq!(
            crate::metadata_journal::recover_next_journal_sequence(&root).unwrap(),
            after_prepare.journal_sequence + 1
        );
        let actual = crate::metadata_journal::segmented_journal_entry_at_sequence(
            &root,
            prepared.digest.prepare_sequence,
        )
        .unwrap()
        .unwrap();
        assert!(
            matches!(&actual.mutation, DfsMetadataMutation::PrepareOperationV7 { record, .. }
            if record.digest == prepared.digest && record.payload == prepared.payload)
        );
        let writer = crate::DfsMetadataCandidateWriter::open(&root).unwrap();
        let (committed, after_commit) = writer
            .commit(prepared, &after_prepare, OffsetDateTime::now_utc())
            .unwrap();
        assert_eq!(
            committed.status,
            crate::DfsPreparedOperationStatus::Committed
        );
        fs::rename(&head, root.join("retained-head-after-commit.sobj")).unwrap();
        drop(writer);
        assert_eq!(
            crate::metadata_journal::recover_next_journal_sequence(&root).unwrap(),
            after_commit.journal_sequence + 1
        );
        assert_eq!(
            crate::metadata_journal::segmented_journal_entry_at_sequence(&root, actual.sequence)
                .unwrap()
                .unwrap(),
            actual
        );
    }
    let root = temporary.join("missing-prepare");
    let writer = crate::DfsMetadataCandidateWriter::open(&root).unwrap();
    let (prepared, after_prepare) = writer.prepare(payload, &base).unwrap();
    fs::rename(
        root.join("journal/segments"),
        root.join("retained-original-segments"),
    )
    .unwrap();
    assert!(
        writer
            .commit(prepared, &after_prepare, OffsetDateTime::now_utc())
            .is_err(),
        "a head hint cannot stand in for the missing durable prepare"
    );
    assert!(!root.join("journal/segments").exists());
    drop(writer);
    drop(service);
    runtime.close().unwrap();
}
fn commit_kernel(
    service: &mut NativeService,
    digest: &crate::DfsPreparedOperationDigest,
) -> Result<crate::DfsPreparedOperationCommitResult> {
    let _execution = service
        .acquire_prepared_execution(&crate::DfsPreparedOperationScope {
            file_system_id: digest.file_system_id.clone(),
            shard_id: digest.shard_id.clone(),
            owner_epoch: digest.owner_epoch,
            operation_id: digest.operation_id.clone(),
        })
        .unwrap();
    let guard = service.authorize_prepared_publication(digest).unwrap();
    service
        .metadata_backend_mut()
        .unwrap()
        .commit_prepared_operation(digest, guard, None)
}

#[test]
fn superseding_prepare_preserves_original_body_despite_changed_local_filesystem_view() {
    let (mut runtime, mut service, config) = native_case("v7-native-immutable-supersession", 3);
    let shard = DfsMetadataShardId::new(config.shard_id.clone());
    let current = directory(&mut service, "/services/v7-native-immutable-supersession");
    let mut changed = current.clone();
    changed.version = DfsVersion::generate();
    changed.mode = 0o600;
    let original = namespace_payload(&config, &current, changed.clone(), "immutable-original");
    let stored_filesystem = service
        .metadata_backend()
        .unwrap()
        .get_file_system_on_shard(&shard, &config.file_system_id)
        .unwrap()
        .unwrap();
    let digest = service
        .metadata_backend_mut()
        .unwrap()
        .prepare_operation(original.clone(), None)
        .unwrap();
    // A local projection is DATA, not permission to change a retained body.
    // Keep real native authority/assignment and the actual stored definition.
    let mut local_view = stored_filesystem.clone();
    local_view.display_name.push_str(" refreshed local view");
    service
        .metadata
        .filesystems
        .insert(config.file_system_id.clone(), local_view.clone());
    let mut replacement = original.clone();
    replacement.operation_id = "immutable-replacement".into();
    replacement
        .metadata_effects
        .push(crate::DfsPreparedMetadataEffect::ResolvePreparedOperation {
            digest: digest.clone(),
            status: crate::DfsPreparedOperationStatus::Superseded,
        });
    let mut wrong = replacement.clone();
    wrong.operation_id = "immutable-wrong-readset".into();
    wrong.expected_object_versions[0].expected_version = None;
    assert!(service.publish_prepared_operation(wrong).is_err());
    let retained = service
        .metadata_backend()
        .unwrap()
        .prepared_operation_record(&digest.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(retained.payload.as_ref(), &original);
    assert_eq!(retained.status, crate::DfsPreparedOperationStatus::Prepared);
    let published = service
        .publish_prepared_operation(replacement.clone())
        .unwrap();
    assert!(published.published);
    assert_eq!(published.payload.as_ref(), &replacement);
    let retained = service
        .metadata_backend()
        .unwrap()
        .prepared_operation_record(&digest.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(retained.payload.as_ref(), &original);
    assert_eq!(
        retained.status,
        crate::DfsPreparedOperationStatus::Superseded
    );
    assert_eq!(
        service
            .metadata_backend()
            .unwrap()
            .get_file_system_on_shard(&shard, &config.file_system_id)
            .unwrap(),
        Some(stored_filesystem)
    );
    assert_eq!(
        service
            .metadata_backend()
            .unwrap()
            .get_object(&current.object_id)
            .unwrap(),
        Some(changed.clone())
    );
    // Fresh operations still carry a changed definition through the ordinary
    // complete-body path; only immutable supersession excludes enrichment.
    let mut next = changed.clone();
    next.version = DfsVersion::generate();
    let fresh = namespace_payload(&config, &changed, next, "immutable-next-fresh");
    let published = service.publish_prepared_operation(fresh).unwrap();
    assert!(published.payload.metadata_effects.iter().any(|effect| matches!(effect,
        crate::DfsPreparedMetadataEffect::PutFileSystem { file_system } if file_system == &local_view)));
    drop(service);
    runtime.close().unwrap();
}

#[test]
fn prepared_v7_namespace_commit_and_exact_retry_use_one_digest_marker() {
    let (mut runtime, mut service, config) = native_case("v7-native-namespace", 3);
    let shard = DfsMetadataShardId::new(config.shard_id.clone());
    let current = directory(&mut service, "/services/v7-native-namespace");
    let mut committed = current.clone();
    committed.version = DfsVersion::generate();
    committed.mode = 0o600;
    let payload = namespace_payload(&config, &current, committed.clone(), "v7-namespace");
    let before = entries(&service, &shard).len();
    let digest = service
        .metadata_backend_mut()
        .unwrap()
        .prepare_operation(payload.clone(), None)
        .unwrap();
    assert_eq!(
        service
            .metadata_backend()
            .unwrap()
            .get_object(&current.object_id)
            .unwrap(),
        Some(current.clone())
    );
    let prepared_count = entries(&service, &shard).len();
    assert_eq!(prepared_count, before + 1);
    assert_eq!(
        service
            .metadata_backend_mut()
            .unwrap()
            .prepare_operation(payload, None)
            .unwrap(),
        digest
    );
    assert_eq!(entries(&service, &shard).len(), prepared_count);
    let fresh = commit_kernel(&mut service, &digest).unwrap();
    assert!(fresh.published);
    let entry = service
        .metadata_backend()
        .unwrap()
        .committed_prepared_operation(&digest)
        .unwrap()
        .unwrap();
    assert_eq!(
        fresh.entry, entry,
        "the actual kernel return and retained commitment must be identical"
    );
    let physical_root =
        crate::local_dfs_metadata_shard_path(runtime.data_dir(&config.primary_node_id), &shard);
    let anchor = service
        .metadata_backend()
        .unwrap()
        .metadata_prefix_proof(&shard)
        .unwrap()
        .expect("actual current owned backend prefix");
    let committed_prefix =
        crate::inspect_metadata_prefix_descendant(&physical_root, &shard, &anchor)
            .unwrap()
            .expect("actual committed current prefix");
    let commit_batch = service
        .metadata_backend()
        .unwrap()
        .journal_batch_for_shard_since_limited(
            &shard,
            entry.sequence - 1,
            1,
            crate::DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
        )
        .unwrap()
        .unwrap();
    assert_eq!(commit_batch.entries, vec![entry.clone()]);
    let export_commit = |owner: &NativeService| {
        owner
            .metadata_backend()
            .unwrap()
            .journal_frames_for_batch(&commit_batch)
            .unwrap()
    };
    let original_frames = export_commit(&service);
    assert_eq!(original_frames.len(), 1);
    assert_eq!(
        crate::metadata_journal::decode_metadata_journal_frame_record(&original_frames[0]).unwrap(),
        entry
    );
    let original_reference = service
        .metadata_backend()
        .unwrap()
        .prepared_operation_record(&digest.operation_id)
        .unwrap()
        .unwrap()
        .committed_entry
        .unwrap();
    assert_eq!(original_reference.created_at, entry.created_at);
    assert_eq!(original_reference.journal_entry(), entry);
    let raw_frame_path = runtime
        .data_dir(&config.primary_node_id)
        .root()
        .join("boundary-diagnostic-v7-original-commit-frame.sobj");
    std::fs::write(&raw_frame_path, original_frames[0].exact_bytes.as_ref()).unwrap();
    let negative_file_evidence = {
        // Existing publication exclusion keeps canonical writers out of these
        // short negative probes. No commit/retry/stop occurs while it is held.
        let _fence = service
            .metadata_shard_fence_guard(&config.file_system_id, &config.shard_id)
            .unwrap();
        fn file_evidence(root: &std::path::Path) -> Vec<(std::path::PathBuf, Vec<u8>)> {
            fn visit(
                root: &std::path::Path,
                path: &std::path::Path,
                files: &mut Vec<(std::path::PathBuf, Vec<u8>)>,
            ) {
                let metadata = match std::fs::symlink_metadata(path) {
                    Ok(metadata) => metadata,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
                    Err(error) => {
                        panic!("inspect exact-frame evidence {}: {error}", path.display())
                    }
                };
                assert!(!metadata.file_type().is_symlink());
                if metadata.is_dir() {
                    for entry in std::fs::read_dir(path).unwrap() {
                        visit(root, &entry.unwrap().path(), files);
                    }
                } else {
                    assert!(metadata.is_file());
                    files.push((
                        path.strip_prefix(root).unwrap().to_owned(),
                        std::fs::read(path).unwrap(),
                    ));
                }
            }
            let mut files = Vec::new();
            for path in [
                root.join("journal"),
                crate::metadata_table_store::metadata_table_snapshot_path(root),
                crate::metadata_table_store::metadata_table_checkpoint_log_path(root),
                crate::metadata_table_store::metadata_table_path(root),
            ] {
                visit(root, &path, &mut files);
            }
            files.sort_by(|left, right| left.0.cmp(&right.0));
            files
        }
        let files_before = file_evidence(&physical_root);
        let mut restamped = commit_batch.clone();
        restamped.entries[0].created_at += time::Duration::nanoseconds(1);
        assert_eq!(
            restamped.entries[0].mutation, entry.mutation,
            "same actual digest/mutation, different timestamp only"
        );
        let error = service
            .metadata_backend()
            .unwrap()
            .journal_frames_for_batch(&restamped)
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("differs from selected batch entry"),
            "actual stored-frame mismatch required: {error}"
        );
        let stored =
            crate::metadata_table_store::read_metadata_table_state_lenient(&physical_root).unwrap();
        assert_eq!(
            stored.tables.prepared_operations_v7[&digest.operation_id]
                .committed_entry
                .as_ref(),
            Some(&original_reference)
        );
        let mut replay = stored.tables.clone();
        replay
            .apply_checked(DfsMetadataTableOp::CommitPreparedOperationV7 {
                digest: digest.clone(),
                committed_entry: original_reference.clone(),
            })
            .unwrap();
        assert_eq!(
            replay, stored.tables,
            "actual exact retained commit replay remains idempotent"
        );
        let mut changed_reference = original_reference.clone();
        changed_reference.created_at += time::Duration::nanoseconds(1);
        changed_reference.validate(&digest).unwrap(); // Shape alone is not exact stored commitment.
        let error = replay
            .apply_checked(DfsMetadataTableOp::CommitPreparedOperationV7 {
                digest: digest.clone(),
                committed_entry: changed_reference,
            })
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("changes its retained journal slot"),
            "actual retained-reference mismatch required: {error}"
        );
        assert_eq!(
            replay, stored.tables,
            "rejected timestamp replay changes no rows or indexes"
        );
        assert_eq!(
            file_evidence(&physical_root),
            files_before,
            "bounded fixture journal/table bytes must remain unchanged during negative probes; background checkpoint progress is a fixture-cut failure, not proof that timestamp rejection authored data"
        );
        assert_eq!(export_commit(&service), original_frames);
        files_before.into_iter().map(|(path, bytes)| serde_json::json!({
            "path": path, "bytes": bytes.len(), "blake3": blake3::hash(&bytes).to_hex().to_string(),
        })).collect::<Vec<_>>()
    };
    let recorded = service.commit_stored_prepared_operation(&digest).unwrap();
    assert!(!recorded.published);
    assert_eq!(
        export_commit(&service),
        original_frames,
        "exact retry must export the same stored frame bytes and timestamp"
    );
    assert_eq!(
        service
            .metadata_backend()
            .unwrap()
            .committed_prepared_operation(&digest)
            .unwrap(),
        Some(entry.clone()),
        "recorded retry returns the same actual kernel commitment"
    );
    assert_eq!(
        crate::inspect_metadata_prefix_descendant(&physical_root, &shard, &committed_prefix)
            .unwrap()
            .unwrap(),
        committed_prefix,
        "recorded publication preserves the entire actual physical prefix"
    );
    let committed_count = entries(&service, &shard).len();
    assert_eq!(committed_count, prepared_count + 1);
    assert!(matches!(
        entry.mutation,
        DfsMetadataMutation::CommitPreparedOperationV7 { .. }
    ));
    for peer in config
        .replica_node_ids
        .iter()
        .filter(|peer| **peer != config.primary_node_id)
    {
        runtime.stop_node(peer).unwrap();
    }
    let retry = service.commit_stored_prepared_operation(&digest).unwrap();
    assert!(!retry.published);
    assert_eq!(
        export_commit(&service),
        original_frames,
        "recorded retry with peers stopped still authors no replacement frame"
    );
    assert_eq!(entries(&service, &shard).len(), committed_count);
    assert_eq!(
        service
            .metadata_backend()
            .unwrap()
            .committed_prepared_operation(&digest)
            .unwrap(),
        Some(entry.clone())
    );
    let mut wrong = digest.clone();
    wrong.payload_digest.push('0');
    assert!(service.commit_stored_prepared_operation(&wrong).is_err());
    drop(service);
    // The exact recorded retry above ran with every metadata peer stopped.
    // Cold runtime admission is separate: restore the real participants,
    // including the fixture's KV voter, before asking it for fresh readiness.
    let mut stopped_peers = config
        .replica_node_ids
        .iter()
        .filter(|peer| **peer != config.primary_node_id)
        .map(|peer| {
            let state = sinomo_core::cluster::read_cluster_state_without_kv(
                &sinomo_core::network::ClusterPaths::new(runtime.data_dir(peer)),
            )
            .unwrap();
            (
                sinomo_core::kv::cluster_kv_raft_local_is_voter(&state),
                peer.clone(),
            )
        })
        .collect::<Vec<_>>();
    stopped_peers.sort_by_key(|(voter, _)| !*voter);
    for (_, peer) in stopped_peers {
        runtime.reopen_node(peer).unwrap();
    }
    runtime.stop_node(&config.primary_node_id).unwrap();
    runtime.reopen_node(config.primary_node_id.clone()).unwrap();
    let reopened = runtime.client(&config.primary_node_id).unwrap();
    assert_eq!(
        reopened
            .metadata_backend()
            .unwrap()
            .committed_prepared_operation(&digest)
            .unwrap(),
        Some(entry.clone())
    );
    assert_eq!(
        export_commit(&reopened),
        original_frames,
        "actual owner stop/reopen preserves the same stored frame bytes"
    );
    let reopened_reference = reopened
        .metadata_backend()
        .unwrap()
        .prepared_operation_record(&digest.operation_id)
        .unwrap()
        .unwrap()
        .committed_entry
        .unwrap();
    assert_eq!(reopened_reference, original_reference);
    assert_eq!(reopened_reference.created_at, entry.created_at);
    assert_eq!(
        reopened
            .metadata_backend()
            .unwrap()
            .get_object(&current.object_id)
            .unwrap(),
        Some(committed)
    );
    std::fs::write(runtime.data_dir(&config.primary_node_id).root()
        .join("boundary-diagnostic-v7-exact-commit-frame.json"), serde_json::to_vec_pretty(&serde_json::json!({
            "digest": digest, "actual_commit_reference": original_reference,
            "stored_frame_bytes": original_frames[0].exact_bytes.len(),
            "stored_frame_blake3": blake3::hash(original_frames[0].exact_bytes.as_ref()).to_hex().to_string(),
            "raw_original_frame": raw_frame_path, "unchanged_negative_file_evidence": negative_file_evidence,
            "qualification": "Actual native writer/export/exact retry and same-owner component stop/reopen. Same-digest timestamp mutations reject at stored-frame export and checked replay. No OS-crash proof or reconstruction of an unavailable A-only commit from B/C preparations."
        })).unwrap()).unwrap();
    drop(reopened);
    runtime.close().unwrap();
}
#[test]
fn prepared_v7_rejects_changed_payload_and_wrong_scope_before_effect() {
    let (runtime, mut service, config) = native_case("v7-native-immutable", 1);
    let shard = DfsMetadataShardId::new(config.shard_id.clone());
    let current = directory(&mut service, "/services/v7-native-immutable");
    let mut next = current.clone();
    next.version = DfsVersion::generate();
    let payload = namespace_payload(&config, &current, next, "v7-immutable");
    let digest = service
        .metadata_backend_mut()
        .unwrap()
        .prepare_operation(payload.clone(), None)
        .unwrap();
    let count = entries(&service, &shard).len();
    let mut changed = payload.clone();
    changed.transaction.objects[0].mode = 0o600;
    assert!(
        service
            .metadata_backend_mut()
            .unwrap()
            .prepare_operation(changed, None)
            .is_err()
    );
    let mut foreign = payload.clone();
    foreign.shard_id = DfsMetadataShardId::new("foreign");
    assert!(
        service
            .metadata_backend_mut()
            .unwrap()
            .prepare_operation(foreign, None)
            .is_err()
    );
    let mut retired = payload;
    retired.owner_epoch = config.epoch + 1;
    assert!(
        service
            .metadata_backend_mut()
            .unwrap()
            .prepare_operation(retired, None)
            .is_err()
    );
    assert_eq!(
        service
            .metadata_backend()
            .unwrap()
            .prepared_operation_digest(&digest.operation_id)
            .unwrap(),
        Some(digest)
    );
    assert_eq!(entries(&service, &shard).len(), count);
    assert_eq!(
        service
            .metadata_backend()
            .unwrap()
            .get_object(&current.object_id)
            .unwrap(),
        Some(current)
    );
    drop(service);
    runtime.close().unwrap();
}
#[test]
fn prepared_v7_recorded_reply_keeps_newer_object_and_parent_cache_without_publication() {
    let (runtime, mut service, config) = native_case("v7-native-recorded-cache", 1);
    let shard = DfsMetadataShardId::new(config.shard_id.clone());
    directory(&mut service, "/services/v7-native-recorded-cache");
    let child = directory(&mut service, "/services/v7-native-recorded-cache/child");
    let parent = service
        .lookup_sifs_path("/services/v7-native-recorded-cache")
        .unwrap()
        .unwrap();
    let mut first_child = child.clone();
    first_child.version = DfsVersion::generate();
    let mut first_parent = parent.clone();
    first_parent.version = DfsVersion::generate();
    let mut first = namespace_payload(&config, &child, first_child.clone(), "v7-first-cache");
    first.transaction.objects.push(first_parent.clone());
    first
        .expected_object_versions
        .push(DfsNamespaceObjectVersionExpectation {
            object_id: parent.object_id.clone(),
            expected_version: Some(parent.version.clone()),
        });
    first
        .expected_path_occupants
        .push(DfsNamespacePathOccupantExpectation {
            path: parent.path.clone(),
            expected_object_id: Some(parent.object_id.clone()),
        });
    let recorded = service.publish_prepared_operation(first).unwrap();
    assert!(recorded.published);
    let digest = service
        .metadata_backend()
        .unwrap()
        .prepared_operation_digest("v7-first-cache")
        .unwrap()
        .unwrap();
    let original_entry = service
        .metadata_backend()
        .unwrap()
        .committed_prepared_operation(&digest)
        .unwrap()
        .unwrap();
    let mut newer_child = first_child.clone();
    newer_child.version = DfsVersion::generate();
    newer_child.mode = 0o600;
    let mut newer_parent = first_parent.clone();
    newer_parent.version = DfsVersion::generate();
    newer_parent.modified_at_unix_nanos += 100;
    let mut next = namespace_payload(&config, &first_child, newer_child.clone(), "v7-later-cache");
    next.transaction.objects.push(newer_parent.clone());
    next.expected_object_versions
        .push(DfsNamespaceObjectVersionExpectation {
            object_id: parent.object_id.clone(),
            expected_version: Some(first_parent.version.clone()),
        });
    next.expected_path_occupants
        .push(DfsNamespacePathOccupantExpectation {
            path: parent.path.clone(),
            expected_object_id: Some(parent.object_id.clone()),
        });
    assert!(service.publish_prepared_operation(next).unwrap().published);
    service.metadata.add_object(first_child).unwrap();
    service.metadata.add_object(first_parent).unwrap();
    let tracker = crate::DfsCommittedWorkTracker::new();
    service.set_committed_work_tracker(tracker.clone());
    let count = entries(&service, &shard).len();
    let reply = service.commit_stored_prepared_operation(&digest).unwrap();
    assert!(!reply.published);
    assert_eq!(reply.payload, recorded.payload);
    assert_eq!(
        service.metadata.object(&child.object_id),
        Some(&newer_child)
    );
    assert_eq!(
        service.metadata.object(&parent.object_id),
        Some(&newer_parent)
    );
    assert_eq!(
        service
            .metadata_backend()
            .unwrap()
            .get_object(&child.object_id)
            .unwrap(),
        Some(newer_child)
    );
    assert_eq!(
        service
            .metadata_backend()
            .unwrap()
            .get_object(&parent.object_id)
            .unwrap(),
        Some(newer_parent)
    );
    assert_eq!(entries(&service, &shard).len(), count);
    assert_eq!(
        service
            .metadata_backend()
            .unwrap()
            .committed_prepared_operation(&digest)
            .unwrap(),
        Some(original_entry)
    );
    assert_eq!(tracker.take(), crate::DfsMessageCommittedWork::default());
    drop(service);
    runtime.close().unwrap();
}
#[test]
fn prepared_v7_content_seals_terminal_intent_and_enforces_receipts_and_head() {
    let (runtime, mut service, config) = native_case("v7-native-sealed-content", 3);
    let shard = DfsMetadataShardId::new(config.shard_id.clone());
    let trace = crate::test_support::capture_native_shard_trace(
        &runtime.data_dir(&config.primary_node_id).root(),
        shard.clone(),
    )
    .unwrap();
    let captured = Arc::new(std::sync::Mutex::new(None));
    let hook_capture = Arc::clone(&captured);
    // The shared normal before-prepare observer only copies this actual body.
    // It owns no backend or author capability and performs no preparation.
    crate::service_commit::BEFORE_CREATE_PREPARE.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move |payload| {
            *hook_capture.lock().unwrap() = Some(payload.clone());
        }));
    });
    let candidates = std::iter::once(config.primary_node_id.clone())
        .chain(config.replica_node_ids.iter().cloned())
        .collect();
    let stat = service
        .posix_write_file(
            "/services/v7-native-sealed-content.txt",
            &[0x57; 4096],
            crate::DfsWriteOptions {
                policy: DfsReplicationPolicy::new(2, 2).unwrap(),
                replica_candidates: candidates,
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .unwrap();
    let mut payload = captured
        .lock()
        .unwrap()
        .take()
        .expect("passively copied normal complete create body");
    payload.normalize();
    let digest = service
        .metadata_backend()
        .unwrap()
        .prepared_operation_digest(&payload.operation_id)
        .unwrap()
        .unwrap();
    trace.close();
    let mut bytes = Vec::new();
    trace.export_raw(&mut bytes).unwrap();
    let raw: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let preparations = raw["packets"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|packet| {
            packet["phase"] == "prepare-durable"
                && packet["operation"]["operation_id"] == payload.operation_id
        })
        .collect::<Vec<_>>();
    assert_eq!(
        preparations.len(),
        1,
        "the same native owner prepared this exact body once"
    );
    assert_eq!(
        serde_json::from_value::<crate::DfsPreparedOperationDigest>(
            preparations[0]["operation"].clone()
        )
        .unwrap(),
        digest
    );
    let materializations = raw["packets"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|packet| {
            packet["phase"] == "prepare-materialized"
                && packet["operation"]["operation_id"] == payload.operation_id
        })
        .collect::<Vec<_>>();
    assert_eq!(materializations.len(), 1);
    let physical_root =
        crate::local_dfs_metadata_shard_path(runtime.data_dir(&config.primary_node_id), &shard);
    let held = |packet: &serde_json::Value, side: &str| {
        assert_eq!(packet["owner_label"], raw["actual_mount_label"]);
        let encoded: Vec<u8> = serde_json::from_value(packet[side].clone()).unwrap();
        let held: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(held["shard_id"], shard.0);
        assert_eq!(
            held["backend_root"],
            serde_json::to_value(&physical_root).unwrap()
        );
        held
    };
    let before_held = held(preparations[0], "before_held_json");
    let after_held = held(materializations[0], "after_held_json");
    assert!(
        preparations[0]["owner_ordinal"]
            .as_str()
            .unwrap()
            .parse::<u64>()
            .unwrap()
            < materializations[0]["owner_ordinal"]
                .as_str()
                .unwrap()
                .parse::<u64>()
                .unwrap()
    );
    let prepared_records: Vec<crate::DfsPreparedOperationRecord> =
        serde_json::from_value(after_held["tables"]["prepared_operations_v7"].clone()).unwrap();
    let prepared_record = prepared_records
        .iter()
        .find(|record| record.digest == digest)
        .expect("actual materialized hidden preparation");
    assert_eq!(
        prepared_record.status,
        crate::DfsPreparedOperationStatus::Prepared
    );
    assert_eq!(prepared_record.payload.as_ref(), &payload);
    let target = payload.prepared_intent.as_ref().unwrap().object_id.clone();
    for held in [&before_held, &after_held] {
        // The current DTO omits an empty objects vector.
        let objects: Vec<DfsObjectMetadata> = match held["tables"].get("objects") {
            Some(value) => serde_json::from_value(value.clone()).unwrap(),
            None => Vec::new(),
        };
        assert!(
            objects.iter().all(|object| object.object_id != target),
            "the actual native preparation keeps its create target hidden"
        );
    }
    let actual_batch = service
        .metadata_backend()
        .unwrap()
        .journal_batch_for_shard_since_limited(&shard, 0, usize::MAX, usize::MAX)
        .unwrap()
        .expect("the actual immutable current shard journal");
    let actual_frames = service
        .metadata_backend()
        .unwrap()
        .journal_frames_for_batch(&actual_batch)
        .unwrap();
    assert_eq!(actual_frames.len(), actual_batch.entries.len());
    let before = actual_batch
        .entries
        .iter()
        .position(|entry| {
            matches!(&entry.mutation,
            DfsMetadataMutation::PrepareOperationV7 { record, .. } if record.digest == digest)
        })
        .expect("actual prepared body position in the current physical kernel journal");
    // Replay the SAME complete immutable prefix through the normal checked
    // table operations. All serialized fields must equal the actual held cut;
    // no partial DTO reader or later table state supplies this before image.
    let mut replay = DfsMetadataTables::default();
    let mut exact_prefix = crate::metadata_records::dfs_metadata_prefix_proof_genesis(&shard);
    let mut before_tables = None;
    for (entry, frame) in actual_batch.entries.iter().zip(&actual_frames) {
        assert_eq!(frame.sequence, entry.sequence);
        require_journal_entry_replay_payloads(entry).unwrap();
        if entry.sequence == digest.prepare_sequence {
            assert_eq!(
                preparations[0]["exact_entry"],
                serde_json::to_value(entry).unwrap()
            );
            let observed_frame: Vec<u8> =
                serde_json::from_value(preparations[0]["exact_frame"].clone()).unwrap();
            assert_eq!(observed_frame.as_slice(), frame.exact_bytes.as_ref());
            assert_eq!(before_held["journal_next_sequence"], entry.sequence);
            assert_eq!(
                before_held["materialized_through_sequence"],
                exact_prefix.journal_sequence
            );
            assert_eq!(
                before_held["certified_prefix"],
                serde_json::to_value(&exact_prefix).unwrap()
            );
            let snapshot = crate::metadata_table_store::metadata_tables_snapshot(&replay);
            assert_eq!(
                serde_json::to_value(&snapshot).unwrap(),
                before_held["tables"]
            );
            before_tables = Some(snapshot);
        }
        let op = match &entry.mutation {
            DfsMetadataMutation::PrepareOperationV7 { record, .. } => {
                DfsMetadataTableOp::PutPreparedOperationV7 {
                    record: record.clone(),
                }
            }
            DfsMetadataMutation::CommitPreparedOperationV7 { digest, .. } => {
                DfsMetadataTableOp::CommitPreparedOperationV7 {
                    digest: digest.clone(),
                    committed_entry: Box::new(
                        crate::DfsPreparedOperationCommitReference::from_journal_entry(entry)
                            .unwrap(),
                    ),
                }
            }
        };
        replay.apply_checked(op).unwrap();
        exact_prefix = crate::metadata_journal::metadata_prefix_proof_after_exact_frame(
            &exact_prefix,
            entry,
            &shard,
            frame.exact_bytes.as_ref(),
        )
        .unwrap();
        if entry.sequence == digest.prepare_sequence {
            assert_eq!(after_held["journal_next_sequence"], entry.sequence + 1);
            assert_eq!(after_held["materialized_through_sequence"], entry.sequence);
            assert_eq!(
                after_held["certified_prefix"],
                serde_json::to_value(&exact_prefix).unwrap()
            );
        }
    }
    let before_tables =
        before_tables.expect("the exact captured preparation occurs in this prefix");
    let prepared = |mut body: crate::DfsPreparedOperationPayload| {
        body.normalize();
        crate::DfsPreparedOperationRecord {
            digest: crate::DfsPreparedOperationDigest::at_sequence(&body, digest.prepare_sequence)
                .unwrap(),
            payload: Arc::new(body),
            status: crate::DfsPreparedOperationStatus::Prepared,
            committed_entry: None,
        }
    };
    // These are negative DATA controls over the exact captured before image.
    // They invoke the same production kernel predicates, without a writer,
    // storage effect, authority claim or an existing-ID replay shortcut.
    let validate = |record: &crate::DfsPreparedOperationRecord| {
        let mut tables =
            crate::metadata_table_store::metadata_tables_from_snapshot(before_tables.clone())
                .unwrap();
        super::super::prepared_operation::validate_preparation_against_tables(
            &mut tables,
            &shard,
            record,
        )
    };
    validate(&prepared(payload.clone())).unwrap();
    let mut missing_w = payload.clone();
    let missing = missing_w
        .content_receipts
        .pop()
        .expect("actual sealed W receipt");
    assert!(
        missing_w
            .required_replica_node_ids
            .contains(&missing.replica_node_id)
    );
    let missing_w = prepared(missing_w);
    missing_w.validate().unwrap();
    let mut missing_tables =
        crate::metadata_table_store::metadata_tables_from_snapshot(before_tables.clone()).unwrap();
    missing_tables.receipts.remove(&(
        missing.receipt.object_id.clone(),
        missing.receipt.committed_version.clone(),
        missing.replica_node_id.clone(),
    ));
    assert!(
        matches!(super::super::prepared_operation::validate_preparation_against_tables(
        &mut missing_tables, &shard, &missing_w),
        Err(MetadataBackendError::Serde(message))
            if message == "prepared operation lacks exact required durable content receipt")
    );
    let mut stale = payload.clone();
    stale
        .expected_object_versions
        .iter_mut()
        .find(|read| read.object_id == target)
        .unwrap()
        .expected_version = Some(DfsVersion::generate());
    let stale = prepared(stale);
    stale.validate().unwrap();
    let stale_tables =
        crate::metadata_table_store::metadata_tables_from_snapshot(before_tables.clone()).unwrap();
    let expected = super::super::namespace_prepare::validate_namespace_prepare_expectations(
        &stale_tables,
        &stale,
    )
    .expect_err("actual captured head does not match the changed negative read set");
    let MetadataBackendError::Rejected {
        code: expected_code,
        message: expected_message,
    } = expected
    else {
        panic!("changed absent target version must reach the kernel's exact read-set denial");
    };
    assert!(
        matches!(validate(&stale), Err(MetadataBackendError::Rejected { code, message })
        if code == expected_code && message == expected_message)
    );
    let mut changed = payload.clone();
    changed
        .terminal_intent
        .as_mut()
        .unwrap()
        .object_metadata
        .as_mut()
        .unwrap()
        .mode ^= 0o100;
    let changed = prepared(changed);
    changed.validate().unwrap();
    assert!(
        matches!(validate(&changed), Err(MetadataBackendError::Serde(message))
        if message == "current content publication does not seal its original head and visible effects")
    );
    let terminal = payload.terminal_intent.clone().unwrap();
    assert_eq!(
        terminal.policy.min_commit_replicas, 2,
        "the normal create actually requires W2"
    );
    assert_eq!(payload.required_replica_node_ids.len(), 2);
    assert_eq!(payload.content_receipts.len(), 2);
    assert_eq!(
        terminal.object_metadata.as_ref().unwrap().object_id,
        stat.object_id
    );
    let stored = service
        .metadata_backend()
        .unwrap()
        .prepared_operation_payload(&digest.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        stored.terminal_intent,
        Some(terminal.clone()),
        "entire sealed terminal body retained exactly"
    );
    assert_eq!(
        service
            .metadata_backend()
            .unwrap()
            .get_intent(&stat.object_id, &terminal.intent_id)
            .unwrap(),
        Some(terminal.clone()),
        "the sole current commit retains its full terminal owner until safe GC"
    );
    assert!(
        service
            .metadata_backend()
            .unwrap()
            .get_intent_receipt(&stat.object_id, &terminal.intent_id)
            .unwrap()
            .is_none(),
        "current publication cannot compact an owner before GC checks its lifetime"
    );
    let journal = entries(&service, &shard);
    assert_eq!(journal.len(), before + 2);
    assert!(matches!(
        journal[before].mutation,
        DfsMetadataMutation::PrepareOperationV7 { .. }
    ));
    assert!(matches!(
        journal[before + 1].mutation,
        DfsMetadataMutation::CommitPreparedOperationV7 { .. }
    ));
    let current = service
        .metadata_backend()
        .unwrap()
        .get_object(&stat.object_id)
        .unwrap()
        .unwrap();
    assert_eq!(current, terminal.object_metadata.unwrap());
    let mut wrong = digest.clone();
    wrong.shard_id = DfsMetadataShardId::new("wrong");
    assert!(
        service.commit_stored_prepared_operation(&wrong).is_err(),
        "recorded-digest lookup rejects a different shard without publishing"
    );
    assert_eq!(entries(&service, &shard), journal);
    assert_eq!(journal, actual_batch.entries);
    assert_eq!(replay.objects.get(&stat.object_id), Some(&current));
    assert_eq!(
        replay
            .prepared_operations_v7
            .get(&digest.operation_id)
            .unwrap()
            .digest,
        digest
    );
    let primary = config.primary_node_id.clone();
    drop(service);
    let mut runtime = runtime;
    runtime.stop_node(&primary).unwrap();
    runtime.reopen_node(primary.clone()).unwrap();
    let reopened = runtime.client(&primary).unwrap();
    assert_eq!(
        reopened
            .metadata_backend()
            .unwrap()
            .get_object(&stat.object_id)
            .unwrap(),
        Some(current)
    );
    assert!(
        reopened
            .metadata_backend()
            .unwrap()
            .committed_prepared_operation(&digest)
            .unwrap()
            .is_some()
    );
    drop(reopened);
    runtime.close().unwrap();
    let mut completed_bytes = Vec::new();
    trace.export_raw(&mut completed_bytes).unwrap();
    let completed: serde_json::Value = serde_json::from_slice(&completed_bytes).unwrap();
    assert_eq!(completed["closed"], true);
    assert_eq!(completed["actual_owner_stopped"], true);
    for counter in ["dropped", "incomplete", "in_flight"] {
        assert_eq!(
            completed[counter], "0",
            "same actual capture drained completely"
        );
    }
}
#[test]
fn collected_operation_keeps_exact_outcome_and_rejects_changed_newer_body() {
    let (mut runtime, mut service, config) = native_case("v7-native-eviction", 1);
    let shard = DfsMetadataShardId::new(config.shard_id.clone());
    let initial = directory(&mut service, "/services/v7-native-eviction");
    let mut updated = initial.clone();
    updated.version = DfsVersion::generate();
    let original = namespace_payload(&config, &initial, updated.clone(), "v7-evicted");
    service
        .publish_prepared_operation(original.clone())
        .unwrap();
    let digest = service
        .metadata_backend()
        .unwrap()
        .prepared_operation_digest(&original.operation_id)
        .unwrap()
        .unwrap();
    let entry = service
        .metadata_backend()
        .unwrap()
        .committed_prepared_operation(&digest)
        .unwrap()
        .unwrap();
    let eviction = crate::DfsPreparedOperationPayload::for_metadata_effects(
        crate::DfsPreparedOperationScope {
            file_system_id: initial.file_system_id.clone(),
            shard_id: shard.clone(),
            owner_epoch: config.epoch,
            operation_id: "gc:exact-terminal".into(),
        },
        Vec::new(),
        Vec::new(),
        vec![crate::DfsPreparedMetadataEffect::RemovePreparedOperation {
            digest: digest.clone(),
        }],
        OffsetDateTime::now_utc(),
    )
    .unwrap();
    service.publish_prepared_operation(eviction).unwrap();
    let count = entries(&service, &shard).len();
    assert_eq!(
        service
            .metadata_backend()
            .unwrap()
            .committed_prepared_operation(&digest)
            .unwrap(),
        Some(entry.clone())
    );
    let mut changed = original;
    changed.transaction.created_at += time::Duration::days(100);
    changed.transaction.objects[0].mode = 0o700;
    assert!(
        service
            .metadata_backend_mut()
            .unwrap()
            .prepare_operation(changed, None)
            .is_err()
    );
    assert_eq!(entries(&service, &shard).len(), count);
    assert_eq!(
        service
            .metadata_backend()
            .unwrap()
            .get_object(&initial.object_id)
            .unwrap(),
        Some(updated)
    );
    drop(service);
    runtime.stop_node(&config.primary_node_id).unwrap();
    runtime.reopen_node(config.primary_node_id.clone()).unwrap();
    let reopened = runtime.client(&config.primary_node_id).unwrap();
    assert_eq!(
        reopened
            .metadata_backend()
            .unwrap()
            .committed_prepared_operation(&digest)
            .unwrap(),
        Some(entry)
    );
    drop(reopened);
    runtime.close().unwrap();
}
#[test]
fn journal_cut_precondition_rejects_intervening_semantic_write_before_commit() {
    let (runtime, mut service, config) = native_case("v7-native-cut", 1);
    let shard = DfsMetadataShardId::new(config.shard_id.clone());
    let original = directory(&mut service, "/services/v7-native-cut");
    let journal = entries(&service, &shard);
    let cut = journal.last().map_or(0, |entry| entry.sequence);
    let mut updated = original.clone();
    updated.version = DfsVersion::generate();
    let mut payload = namespace_payload(&config, &original, updated, "v7-cut");
    payload.expected_journal_sequence = Some(cut);
    // Retain the ordinary exact-shard execution frame while exercising the
    // prepare -> intervening publication -> rejected commit order. The real
    // Main recovery worker otherwise may legitimately commit this preparation
    // before the unrelated write reaches its writer.
    let execution = service
        .acquire_prepared_execution(&crate::DfsPreparedOperationScope {
            file_system_id: config.file_system_id.clone(),
            shard_id: shard.clone(),
            owner_epoch: config.epoch,
            operation_id: payload.operation_id.clone(),
        })
        .unwrap();
    assert!(execution.is_some(), "actual exact-shard runtime admission");
    let mut encoded = serde_json::to_value(&payload).unwrap();
    encoded
        .as_object_mut()
        .unwrap()
        .remove("expected_journal_sequence");
    assert!(serde_json::from_value::<crate::DfsPreparedOperationPayload>(encoded).is_err());
    let digest = service
        .metadata_backend_mut()
        .unwrap()
        .prepare_operation(payload, None)
        .unwrap();
    assert_eq!(digest.prepare_sequence, cut + 1);
    let mut updated_fs = service
        .metadata_backend()
        .unwrap()
        .get_file_system(&config.file_system_id)
        .unwrap()
        .unwrap();
    updated_fs.display_name.push_str("-intervening");
    let unrelated = crate::DfsPreparedOperationPayload::for_metadata_effects(
        crate::DfsPreparedOperationScope {
            file_system_id: config.file_system_id.clone(),
            shard_id: shard.clone(),
            owner_epoch: config.epoch,
            operation_id: "journal-cut-unrelated".into(),
        },
        Vec::new(),
        Vec::new(),
        vec![crate::DfsPreparedMetadataEffect::PutFileSystem {
            file_system: updated_fs,
        }],
        OffsetDateTime::now_utc(),
    )
    .unwrap();
    service.publish_prepared_operation(unrelated).unwrap();
    let actual = service
        .metadata_backend()
        .unwrap()
        .prepared_operation_record(&digest.operation_id)
        .unwrap()
        .expect("original preparation remains durable");
    assert_eq!(actual.digest, digest);
    assert_eq!(actual.status, crate::DfsPreparedOperationStatus::Prepared);
    assert!(actual.committed_entry.is_none());
    assert_eq!(actual.payload.expected_journal_sequence, Some(cut));
    let journal = entries(&service, &shard);
    assert!(journal.last().unwrap().sequence > digest.prepare_sequence);
    assert!(journal.iter().any(|entry| matches!(&entry.mutation,
        DfsMetadataMutation::CommitPreparedOperationV7 { operation_id, .. }
            if operation_id == "journal-cut-unrelated")));
    assert!(!journal.iter().any(|entry| matches!(&entry.mutation,
        DfsMetadataMutation::CommitPreparedOperationV7 { operation_id, .. }
            if operation_id == &digest.operation_id)));
    let count = journal.len();
    assert!(commit_kernel(&mut service, &digest).is_err());
    assert_eq!(entries(&service, &shard).len(), count);
    assert_eq!(
        service
            .metadata_backend()
            .unwrap()
            .get_object(&original.object_id)
            .unwrap(),
        Some(original)
    );
    drop(execution);
    drop(service);
    runtime.close().unwrap();
}

#[test]
fn transferred_reference_expiry_merges_monotonically_and_rolls_back_exactly() {
    let mut tables = DfsMetadataTables::default();
    let id = object_id();
    let original = crate::DfsContentReferenceOutcomeRetention {
        file_system_id: file_system_id(),
        max_evicted_operation_order_token: None,
        opaque_operation_history_evicted: false,
    };
    tables
        .namespace_receipt_retention
        .content_reference_outcome_retention
        .insert(id.clone(), original.clone());
    let expired = crate::DfsContentReferenceOutcomeRetention {
        opaque_operation_history_evicted: true,
        ..original.clone()
    };
    let effect = crate::DfsPreparedMetadataEffect::MergeContentReferenceOutcomeRetention {
        object_id: id.clone(),
        retention: expired,
    };
    assert_eq!(effect.affected_object_ids(), BTreeSet::from([id.clone()]));
    let mut rollback = DfsMetadataTableRollback::default();
    crate::metadata_backend::prepared_operation::apply_metadata_effect_with_rollback(
        &mut tables,
        &effect,
        &mut rollback,
    )
    .unwrap();
    assert!(
        tables
            .namespace_receipt_retention
            .content_reference_outcome_retention[&id]
            .opaque_operation_history_evicted
    );
    rollback.rollback(&mut tables);
    assert_eq!(
        tables
            .namespace_receipt_retention
            .content_reference_outcome_retention[&id],
        original
    );
    tables.apply_checked(effect.into_table_op()).unwrap();
    tables
        .apply_checked(DfsMetadataTableOp::MergeContentReferenceOutcomeRetention {
            object_id: id.clone(),
            retention: original,
        })
        .unwrap();
    assert!(
        tables
            .namespace_receipt_retention
            .content_reference_outcome_retention[&id]
            .opaque_operation_history_evicted,
        "a later empty transfer cannot authorize unknown old retries"
    );
}

// Artifact-only additions for the existing native prepared_operation tests.
fn actual_w2_object(
    service: &mut NativeService,
    config: &DfsMetadataShardConfig,
    path: &str,
) -> DfsObjectMetadata {
    let nodes = std::iter::once(config.primary_node_id.clone())
        .chain(config.replica_node_ids.iter().cloned())
        .collect();
    let stat = service
        .posix_write_file(
            path,
            &[0x4d; 4096],
            crate::DfsWriteOptions {
                policy: DfsReplicationPolicy::new(2, 2).unwrap(),
                replica_candidates: nodes,
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .unwrap();
    service
        .metadata_backend()
        .unwrap()
        .get_object(&stat.object_id)
        .unwrap()
        .unwrap()
}
fn object_effects(
    config: &DfsMetadataShardConfig,
    object: &DfsObjectMetadata,
    id: &str,
    effects: Vec<crate::DfsPreparedMetadataEffect>,
) -> crate::DfsPreparedOperationPayload {
    crate::DfsPreparedOperationPayload::for_metadata_effects(
        crate::DfsPreparedOperationScope {
            file_system_id: object.file_system_id.clone(),
            shard_id: DfsMetadataShardId::new(config.shard_id.clone()),
            owner_epoch: config.epoch,
            operation_id: id.into(),
        },
        vec![DfsNamespaceObjectVersionExpectation {
            object_id: object.object_id.clone(),
            expected_version: Some(object.version.clone()),
        }],
        vec![DfsNamespacePathOccupantExpectation {
            path: object.path.clone(),
            expected_object_id: Some(object.object_id.clone()),
        }],
        effects,
        OffsetDateTime::now_utc(),
    )
    .unwrap()
}
#[test]
fn visible_metadata_effect_cannot_borrow_same_hash_from_a_different_object() {
    let (runtime, mut service, config) = native_case("v7-native-foreign-content", 3);
    let shard = DfsMetadataShardId::new(config.shard_id.clone());
    let first = actual_w2_object(&mut service, &config, "/services/v7-first-same-bytes");
    let second = actual_w2_object(&mut service, &config, "/services/v7-second-same-bytes");
    assert_eq!(first.content_hash, second.content_hash);
    assert_ne!(first.object_id, second.object_id);
    let mut borrowed = service
        .metadata_backend()
        .unwrap()
        .get_version(&second.object_id, &second.version)
        .unwrap()
        .unwrap();
    borrowed.object_id = first.object_id.clone();
    let before = entries(&service, &shard).len();
    let payload = object_effects(
        &config,
        &first,
        "v7-borrowed-content",
        vec![crate::DfsPreparedMetadataEffect::PutVersion {
            version: borrowed.clone(),
        }],
    );
    let mut transaction_body = payload.clone();
    transaction_body.operation_id = "v7-borrowed-namespace-content".into();
    transaction_body.metadata_effects.clear();
    transaction_body.transaction.versions.push(borrowed);
    assert!(
        service
            .metadata_backend_mut()
            .unwrap()
            .prepare_operation(payload, None)
            .is_err()
    );
    assert!(
        service
            .metadata_backend_mut()
            .unwrap()
            .prepare_operation(transaction_body, None)
            .is_err(),
        "namespace transaction rows cannot bypass the same content binding check"
    );
    assert_eq!(entries(&service, &shard).len(), before);
    assert_eq!(
        service
            .metadata_backend()
            .unwrap()
            .get_object(&first.object_id)
            .unwrap(),
        Some(first)
    );
    assert_eq!(
        service
            .metadata_backend()
            .unwrap()
            .get_object(&second.object_id)
            .unwrap(),
        Some(second)
    );
    drop(service);
    runtime.close().unwrap();
}
#[test]
fn first_committed_intent_effect_cannot_invent_an_unrecorded_data_generation() {
    let (runtime, mut service, config) = native_case("v7-native-unsealed-intent", 3);
    let shard = DfsMetadataShardId::new(config.shard_id.clone());
    let object = actual_w2_object(&mut service, &config, "/services/v7-real-terminal");
    let actual = service
        .metadata_backend()
        .unwrap()
        .intents_for_object(&object.object_id)
        .unwrap()
        .into_iter()
        .find(|intent| intent.status == DfsWriteIntentStatus::Committed)
        .expect("genuine native terminal record");
    let mut forged = actual;
    forged.intent_id = "v7-first-fabricated-terminal".into();
    forged.target_version = DfsVersion::generate();
    forged.object_metadata = None;
    let before = entries(&service, &shard).len();
    let id = forged.intent_id.clone();
    let payload = object_effects(
        &config,
        &object,
        "v7-unsealed-terminal",
        vec![crate::DfsPreparedMetadataEffect::PutIntent { intent: forged }],
    );
    assert!(
        service
            .metadata_backend_mut()
            .unwrap()
            .prepare_operation(payload, None)
            .is_err()
    );
    assert!(
        service
            .metadata_backend()
            .unwrap()
            .get_intent(&object.object_id, &id)
            .unwrap()
            .is_none()
    );
    assert_eq!(entries(&service, &shard).len(), before);
    assert_eq!(
        service
            .metadata_backend()
            .unwrap()
            .get_object(&object.object_id)
            .unwrap(),
        Some(object)
    );
    drop(service);
    runtime.close().unwrap();
}
#[test]
fn committed_intent_effect_cannot_invent_future_epoch_from_existing_content() {
    let (runtime, mut service, config) = native_case("v7-native-future-terminal", 3);
    let shard = DfsMetadataShardId::new(config.shard_id.clone());
    let object = actual_w2_object(&mut service, &config, "/services/v7-future-terminal");
    let actual = service
        .metadata_backend()
        .unwrap()
        .intents_for_object(&object.object_id)
        .unwrap()
        .into_iter()
        .find(|intent| intent.status == DfsWriteIntentStatus::Committed)
        .expect("genuine native terminal record");
    let before = entries(&service, &shard);
    for (index, change_identity) in [false, true].into_iter().enumerate() {
        let mut forged = actual.clone();
        forged.owner_epoch = config.epoch + 1;
        if change_identity {
            forged.intent_id.push_str("-invented");
        }
        let payload = object_effects(
            &config,
            &object,
            &format!("v7-future-terminal-{index}"),
            vec![crate::DfsPreparedMetadataEffect::PutIntent { intent: forged }],
        );
        assert!(
            service
                .metadata_backend_mut()
                .unwrap()
                .prepare_operation(payload, None)
                .is_err(),
            "neither an altered recorded outcome nor matching content can authorize a future intent"
        );
        assert_eq!(entries(&service, &shard), before);
        assert_eq!(
            service
                .metadata_backend()
                .unwrap()
                .get_intent(&object.object_id, &actual.intent_id)
                .unwrap(),
            Some(actual.clone())
        );
    }
    drop(service);
    runtime.close().unwrap();
}

#[test]
fn metadata_only_effect_preserves_actual_recorded_data_generation_and_w() {
    let (runtime, mut service, config) = native_case("v7-native-metadata-generation", 3);
    let shard = DfsMetadataShardId::new(config.shard_id.clone());
    let current = actual_w2_object(
        &mut service,
        &config,
        "/services/v7-native-metadata-generation",
    );
    let old = service
        .metadata_backend()
        .unwrap()
        .get_version(&current.object_id, &current.version)
        .unwrap()
        .unwrap();
    let plan = old
        .content_durability
        .clone()
        .expect("real data-generation binding");
    let mut next = current.clone();
    next.version = DfsVersion::generate();
    next.mode ^= 0o100;
    let mut version = old.clone();
    version.version = next.version.clone();
    let before = entries(&service, &shard).len();
    let incomplete = object_effects(
        &config,
        &current,
        "v7-incomplete-successor",
        vec![crate::DfsPreparedMetadataEffect::PutObject {
            object: next.clone(),
        }],
    );
    assert!(
        service
            .metadata_backend_mut()
            .unwrap()
            .prepare_operation(incomplete, None)
            .is_err()
    );
    let mut changed_plan = version.clone();
    changed_plan
        .content_durability
        .as_mut()
        .unwrap()
        .source_version = DfsVersion::generate();
    let wrong = object_effects(
        &config,
        &current,
        "v7-restamped-successor",
        vec![
            crate::DfsPreparedMetadataEffect::PutObject {
                object: next.clone(),
            },
            crate::DfsPreparedMetadataEffect::PutVersion {
                version: changed_plan,
            },
        ],
    );
    assert!(
        service
            .metadata_backend_mut()
            .unwrap()
            .prepare_operation(wrong, None)
            .is_err()
    );
    assert_eq!(entries(&service, &shard).len(), before);
    let payload = object_effects(
        &config,
        &current,
        "v7-complete-successor",
        vec![
            crate::DfsPreparedMetadataEffect::PutObject {
                object: next.clone(),
            },
            crate::DfsPreparedMetadataEffect::PutVersion {
                version: version.clone(),
            },
        ],
    );
    assert!(
        service
            .publish_prepared_operation(payload)
            .unwrap()
            .published
    );
    let committed = service
        .metadata_backend()
        .unwrap()
        .get_version(&next.object_id, &next.version)
        .unwrap()
        .unwrap();
    assert_eq!(committed.content_durability, Some(plan.clone()));
    assert_ne!(next.version, plan.source_version);
    assert_eq!(
        service
            .metadata_backend()
            .unwrap()
            .get_object(&next.object_id)
            .unwrap(),
        Some(next)
    );
    let receipts = service
        .metadata_backend()
        .unwrap()
        .receipts_for_version(&current.object_id, &plan.source_version)
        .unwrap();
    assert!(receipts.len() >= plan.policy.min_commit_replicas);
    drop(service);
    runtime.close().unwrap();
}

#[test]
fn metadata_only_successor_repair_uses_recorded_content_generation() {
    let (runtime, mut service, config) = native_case("v7-metadata-successor-repair", 3);
    let shard = DfsMetadataShardId::new(config.shard_id.clone());
    let original = actual_w2_object(&mut service, &config, "/services/repair-before-rename");
    let original_version = service
        .metadata_backend()
        .unwrap()
        .get_version(&original.object_id, &original.version)
        .unwrap()
        .unwrap();
    let plan = original_version.content_durability.clone().unwrap();
    let original_intent = service
        .metadata_backend()
        .unwrap()
        .intents_for_object(&original.object_id)
        .unwrap()
        .into_iter()
        .find(|intent| {
            intent.status == DfsWriteIntentStatus::Committed
                && intent.target_version == original.version
        })
        .expect("actual committed native write");
    let renamed = service
        .posix_rename(&original.path, "/services/repair-after-rename")
        .unwrap();
    let current = service
        .metadata_backend()
        .unwrap()
        .get_object(&renamed.object_id)
        .unwrap()
        .unwrap();
    assert_ne!(current.version, original.version);
    assert_eq!(current.content_hash, original.content_hash);
    let version = service
        .metadata_backend()
        .unwrap()
        .get_version(&current.object_id, &current.version)
        .unwrap()
        .unwrap();
    assert_eq!(version.content_durability, Some(plan.clone()));
    assert_ne!(current.version, plan.source_version);

    // This is the actual persistence path used by placement repair after a
    // metadata-only rename. It creates no new byte generation or supplied ACK.
    let mut repair = crate::intent::distributed_intent_from_record(&original_intent);
    repair.intent_id = format!(
        "placement:metadata-successor:{}:{}",
        current.object_id, current.version
    );
    repair.base_version = Some(current.version.clone());
    repair.target_version = current.version.clone();
    repair.operation_id = None;
    repair.operation_order_token = None;
    service
        .put_metadata_intent_and_receipts(&repair, current.content_hash.as_ref().unwrap(), &[])
        .expect(
            "repair of the actual metadata head retains the original data-generation W evidence",
        );
    let recorded = service
        .metadata_backend()
        .unwrap()
        .get_intent(&current.object_id, &repair.intent_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        crate::intent::distributed_intent_from_record(&recorded),
        repair
    );
    assert_eq!(recorded.object_metadata.as_ref(), Some(&current));
    let after = entries(&service, &shard);
    service
        .put_metadata_intent_and_receipts(&repair, current.content_hash.as_ref().unwrap(), &[])
        .unwrap();
    assert_eq!(
        entries(&service, &shard),
        after,
        "exact repair replay has no new journal effect"
    );

    for wrong_target in [true, false] {
        let mut forged = repair.clone();
        forged.intent_id.push_str(if wrong_target {
            ":wrong-target"
        } else {
            ":wrong-base"
        });
        if wrong_target {
            forged.target_version = DfsVersion::generate();
        } else {
            forged.base_version = Some(DfsVersion::generate());
        }
        assert!(
            service
                .put_metadata_intent_and_receipts(
                    &forged,
                    current.content_hash.as_ref().unwrap(),
                    &[]
                )
                .is_err()
        );
        assert!(
            service
                .metadata_backend()
                .unwrap()
                .get_intent(&current.object_id, &forged.intent_id)
                .unwrap()
                .is_none()
        );
        assert_eq!(entries(&service, &shard), after);
    }
    assert_eq!(
        service
            .metadata_backend()
            .unwrap()
            .get_version(&current.object_id, &current.version)
            .unwrap()
            .unwrap()
            .content_durability,
        Some(plan.clone())
    );
    assert!(
        service
            .metadata_backend()
            .unwrap()
            .receipts_for_version(&current.object_id, &plan.source_version)
            .unwrap()
            .len()
            >= plan.policy.min_commit_replicas
    );
    assert_eq!(
        service.posix_read_file(&current.path).unwrap(),
        vec![0x4d; 4096]
    );
    drop(service);
    runtime.close().unwrap();
}

#[test]
fn native_kernel_trace_observes_actual_hidden_prepare_and_visible_commit() {
    let (runtime, mut service, config) = native_case("v7-native-raw-trace", 3);
    let original = directory(&mut service, "/services/v7-native-trace-directory");
    let shard = DfsMetadataShardId::new(config.shard_id.clone());
    let capture = crate::test_support::capture_native_shard_trace(
        &runtime.data_dir(&config.primary_node_id).root(),
        shard.clone(),
    )
    .expect("actual normal mounted owner");
    let mut next = original.clone();
    next.version = DfsVersion::generate();
    next.mode ^= 0o100;
    let payload = namespace_payload(
        &config,
        &original,
        next.clone(),
        "v7-native-trace-publication",
    );
    let operation_id = payload.operation_id.clone();
    assert!(
        service
            .publish_prepared_operation(payload)
            .unwrap()
            .published
    );
    // Public visibility is observed through the normal Main read only after
    // the complete publication returned and released its writer admission.
    let mut reader = runtime.client(&config.primary_node_id).unwrap();
    let visible = reader.posix_stat_distributed(&next.path).unwrap();
    assert_eq!(visible.file_system_id, next.file_system_id);
    assert_eq!(visible.object_id, next.object_id);
    assert_eq!(visible.version, next.version);
    assert_eq!(visible.mode, next.mode);
    assert_eq!(visible.content_hash, next.content_hash);
    assert_eq!(visible.size_bytes, next.content_size_bytes);
    drop(reader);
    capture.close();
    drop(service);
    runtime.close().expect("normal component drain");
    let mut raw = Vec::new();
    capture
        .export_raw(&mut raw)
        .expect("actual raw facts export");
    let exported: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    assert_eq!(exported["classification"], "raw-native-kernel-observation");
    assert_eq!(exported["actual_owner_stopped"], true);
    assert_eq!(exported["in_flight"], "0");
    assert_eq!(exported["dropped"], "0");
    assert_eq!(exported["incomplete"], "0");
    let packets: Vec<_> = exported["packets"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|packet| packet["operation"]["operation_id"] == operation_id)
        .collect();
    assert_eq!(packets.len(), 4);
    let phases: Vec<_> = packets
        .iter()
        .map(|packet| packet["phase"].as_str().unwrap())
        .collect();
    assert_eq!(
        phases,
        [
            "prepare-durable",
            "prepare-materialized",
            "commit-durable",
            "commit-materialized"
        ]
    );
    let bytes = |value: &serde_json::Value| -> Vec<u8> {
        value
            .as_array()
            .unwrap()
            .iter()
            .map(|byte| u8::try_from(byte.as_u64().unwrap()).unwrap())
            .collect()
    };
    for packet in &packets {
        if !packet["exact_frame"].is_null() {
            let entry: DfsMetadataJournalEntry =
                sinomo_core::object::serde_ext::from_bytes(&bytes(&packet["exact_frame"])).unwrap();
            assert_eq!(serde_json::to_value(entry).unwrap(), packet["exact_entry"]);
        }
        let held: serde_json::Value =
            serde_json::from_slice(&bytes(&packet["after_held_json"])).unwrap();
        assert!(
            matches!(
                held["write_freeze"]["observation"].as_str(),
                Some("absent" | "present")
            ),
            "the normal gate must record its actual read, not manufacture absence"
        );
        if held["write_freeze"]["observation"] == "present" {
            assert_eq!(
                held["write_freeze"]["value"]["activated_epoch"].as_u64(),
                Some(config.epoch)
            );
        }
        assert!(
            held["certified_prefix"]["journal_sequence"]
                .as_u64()
                .is_some()
        );
        let object = held["tables"]["objects"]
            .as_array()
            .unwrap()
            .iter()
            .find(|object| object["object_id"] == original.object_id.as_str())
            .unwrap();
        // These are private held tables. Commit preapplication is protected
        // by the same writer and rollback; it does not prove public visibility.
        if matches!(
            packet["phase"].as_str(),
            Some("commit-durable" | "commit-materialized")
        ) {
            assert_eq!(
                serde_json::from_value::<DfsObjectMetadata>(object.clone()).unwrap(),
                next
            );
        } else {
            assert_eq!(
                serde_json::from_value::<DfsObjectMetadata>(object.clone()).unwrap(),
                original
            );
        }
    }
    // This artifact contains facts and explicitly missing authority/initial-cut
    // fields. Only the complete adapter plus real Quint replay can claim conformance.
    assert!(
        !exported["required_unavailable_facts"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    if let Some(path) = std::env::var_os("SINOMO_DFS_NATIVE_TRACE_OUTPUT") {
        std::fs::write(path, raw).expect("requested native trace artifact");
    }
}

#[test]
fn native_local_eventual_batch_seals_each_actual_write_and_rejects_mixed_bindings() {
    let (runtime, mut service, config) = native_case("v7-native-local-batch-proof", 1);
    let shard = DfsMetadataShardId::new(config.shard_id.clone());
    directory(&mut service, "/services/v7-native-batch");
    let written = service
        .posix_create_files_local_eventual_with_known_parents(vec![
            ("/services/v7-native-batch/a", b"native alpha".as_slice()),
            ("/services/v7-native-batch/b", b"native beta".as_slice()),
        ])
        .expect("normal guarded bulk publication");
    let journal = entries(&service, &shard);
    let prepared_entry = journal
        .iter()
        .find(|entry| {
            matches!(&entry.mutation,
            DfsMetadataMutation::PrepareOperationV7 { record, .. }
                if written.iter().all(|stat| record.transaction.objects.iter().any(|object|
                    object.object_id == stat.object_id && object.version == stat.version)))
        })
        .expect("actual immutable native bulk prepare body");
    let DfsMetadataMutation::PrepareOperationV7 { record, .. } = &prepared_entry.mutation else {
        unreachable!("selected actual prepare entry");
    };
    let record = record.as_ref();
    assert_eq!(record.content_receipts.len(), written.len());
    assert!(record.terminal_intent.is_none());
    for stat in &written {
        let version = service
            .metadata_backend()
            .unwrap()
            .get_version(&stat.object_id, &stat.version)
            .unwrap()
            .unwrap();
        let receipt = service
            .metadata_backend()
            .unwrap()
            .receipts_for_version(&stat.object_id, &stat.version)
            .unwrap();
        assert_eq!(receipt.len(), 1);
        assert_eq!(
            version.content_durability.as_ref(),
            Some(&crate::content_durability::from_local_receipt(&receipt[0]))
        );
    }
    // Reconstruct the genuine pre-prepare DATA from the complete actual native
    // journal. This pure validation fixture grants no runtime publication right.
    let mut tables = DfsMetadataTables::default();
    for entry in journal
        .iter()
        .take_while(|entry| entry.sequence < prepared_entry.sequence)
    {
        require_journal_entry_replay_payloads(entry).unwrap();
        let operation = match &entry.mutation {
            DfsMetadataMutation::PrepareOperationV7 { record, .. } => {
                DfsMetadataTableOp::PutPreparedOperationV7 {
                    record: record.clone(),
                }
            }
            DfsMetadataMutation::CommitPreparedOperationV7 { digest, .. } => {
                DfsMetadataTableOp::CommitPreparedOperationV7 {
                    digest: digest.clone(),
                    committed_entry: Box::new(
                        crate::DfsPreparedOperationCommitReference::from_journal_entry(entry)
                            .unwrap(),
                    ),
                }
            }
        };
        tables.apply_checked(operation).unwrap();
    }
    let mut check = tables.clone();
    crate::metadata_backend::prepared_operation::validate_metadata_effect_content_bindings(
        &check, &record, None,
    )
    .unwrap();
    crate::metadata_backend::prepared_operation::validate_preparation_against_tables(
        &mut check, &shard, &record,
    )
    .unwrap();
    assert_eq!(check, tables);
    for change in 0..6 {
        let mut bad = (*record).clone();
        let mut body = bad.payload.as_ref().clone();
        let first_id = written[0].object_id.clone();
        match change {
            0 => {
                body.content_receipts.pop();
            }
            1 => {
                body.content_receipts[0].receipt.object_id = written[1].object_id.clone();
            }
            2 => {
                body.transaction
                    .versions
                    .iter_mut()
                    .find(|version| version.object_id == first_id)
                    .unwrap()
                    .content_durability
                    .as_mut()
                    .unwrap()
                    .source_version = DfsVersion::generate();
            }
            3 => {
                body.transaction
                    .versions
                    .iter_mut()
                    .find(|version| version.object_id == first_id)
                    .unwrap()
                    .content_hash = written[1].content_hash.clone();
            }
            4 => {
                body.expected_object_versions
                    .iter_mut()
                    .find(|expected| expected.object_id == first_id)
                    .unwrap()
                    .expected_version = Some(DfsVersion::generate());
            }
            _ => {
                let mut foreign_effect = body
                    .transaction
                    .objects
                    .iter()
                    .find(|object| object.object_id == first_id)
                    .unwrap()
                    .clone();
                foreign_effect.content_hash = written[1].content_hash.clone();
                body.metadata_effects
                    .push(crate::DfsPreparedMetadataEffect::PutObject {
                        object: foreign_effect,
                    });
            }
        }
        bad.digest =
            crate::DfsPreparedOperationDigest::at_sequence(&body, record.digest.prepare_sequence)
                .unwrap();
        bad.payload = std::sync::Arc::new(body);
        let mut check = tables.clone();
        assert!(
            crate::metadata_backend::prepared_operation::validate_metadata_effect_content_bindings(
                &check, &bad, None
            )
            .and_then(|_| {
                crate::metadata_backend::prepared_operation::validate_preparation_against_tables(
                    &mut check, &shard, &bad,
                )
            })
            .is_err(),
            "altered native batch binding {change}"
        );
        assert_eq!(
            check, tables,
            "rejected proof cannot leave a receipt or visible effect"
        );
    }
    drop(service);
    runtime.close().unwrap();
}

#[test]
fn native_owned_recovery_starts_at_exact_tables_despite_a_lagging_cursor_hint() {
    let (runtime, mut service, config) = native_case("v7-native-lagging-cold-cursor", 1);
    let node = config.primary_node_id.clone();
    let shard = DfsMetadataShardId::new(config.shard_id.clone());
    let initial = service
        .metadata_backend()
        .unwrap()
        .native_held_snapshot_for_scope(&config.file_system_id, &shard)
        .expect("actual initial native scope");
    let mut repair_snapshot =
        LocalSobjMetadataBackend::open_for_replica_repair(shard.clone(), &initial.backend_root)
            .expect("normal snapshot-only handle before another writer advances");
    let prior = repair_snapshot.metadata_prefix_proof().unwrap();
    assert!(repair_snapshot.replica_repair_only);
    let path = "/services/v7-native-cold-cursor";
    let written = service
        .posix_write_file(
            path,
            b"actual native bytes",
            crate::DfsWriteOptions::replicated_eventual(vec![node.clone()]),
        )
        .unwrap();
    let held = service
        .metadata_backend()
        .unwrap()
        .native_held_snapshot_for_scope(&config.file_system_id, &shard)
        .expect("already-held actual native scope");
    let mut backend = crate::LocalSobjMetadataBackend::new(shard.clone(), &held.backend_root)
        .expect("actual current physical backend");
    let _writer = backend
        .metadata_write_lock()
        .expect("actual existing shard writer FD");
    let exact = backend
        .metadata_prefix_proof()
        .expect("fresh actual durable table cut");
    assert!(exact.journal_sequence >= 2);
    assert_eq!(
        backend
            .tables
            .objects
            .get(&written.object_id)
            .unwrap()
            .version,
        written.version
    );
    // A negative disk scheduling hint is not an ACK, writer, or prefix proof.
    // Keep all real journal frames, tables and authority state unchanged.
    crate::metadata_cursor::write_local_materialization_cursor(
        &held.backend_root,
        &crate::metadata_journal::DfsMetadataReplayCursor {
            shard_id: shard.clone(),
            last_sequence: exact.journal_sequence - 2,
        },
    )
    .unwrap();
    backend
        .recover_unmaterialized_local_journal_entries()
        .expect("existing recovery must start at the actual table cut");
    assert_eq!(backend.metadata_prefix_proof().unwrap(), exact);
    drop(_writer);
    drop(backend);
    assert!(exact.journal_sequence > prior.journal_sequence);
    let table_path = metadata_table_path(&held.backend_root);
    let table_bytes = fs::read(&table_path).unwrap();
    let table_generation = metadata_table_storage_generation(&held.backend_root).unwrap();
    for _ in 0..2 {
        repair_snapshot
            .begin_consistent_read_snapshot()
            .expect("current tables must precede recovery from an old repair-only snapshot");
        assert_eq!(repair_snapshot.metadata_prefix_proof().unwrap(), exact);
        assert_eq!(
            repair_snapshot
                .tables
                .objects
                .get(&written.object_id)
                .unwrap()
                .version,
            written.version
        );
        assert!(repair_snapshot.replica_repair_only);
        repair_snapshot.end_consistent_read_snapshot();
        assert_eq!(fs::read(&table_path).unwrap(), table_bytes);
        assert_eq!(
            metadata_table_storage_generation(&held.backend_root).unwrap(),
            table_generation
        );
    }
    // Repeating an actual complete record is corruption, never an accepted
    // materialization retry. Keep the same real writer across this negative
    // and restore only the previously observed original bytes afterward.
    let negative_writer = repair_snapshot.metadata_write_lock().unwrap();
    let first_bytes = 4 + u32::from_le_bytes(table_bytes[..4].try_into().unwrap()) as usize;
    fs::OpenOptions::new()
        .append(true)
        .open(&table_path)
        .unwrap()
        .write_all(&table_bytes[..first_bytes])
        .unwrap();
    let corrupted = fs::read(&table_path).unwrap();
    let corrupted_generation = metadata_table_storage_generation(&held.backend_root).unwrap();
    let error = repair_snapshot
        .refresh_after_metadata_write_lock()
        .expect_err("backward or equal complete prefix must remain rejected");
    assert!(matches!(error, MetadataBackendError::Rejected {
        code: crate::DfsMetadataRejectionCode::ReplicaDiverged,
        ref message,
    } if message == "table batch prefix/sequence is inconsistent"));
    assert_eq!(fs::read(&table_path).unwrap(), corrupted);
    assert_eq!(
        metadata_table_storage_generation(&held.backend_root).unwrap(),
        corrupted_generation
    );
    fs::write(&table_path, &table_bytes).unwrap();
    drop(negative_writer);
    drop(repair_snapshot);
    drop(service);
    let mut reopened = runtime
        .client(&node)
        .expect("ordinary current native reopen");
    assert_eq!(
        reopened
            .metadata_backend()
            .unwrap()
            .metadata_prefix_proof(&shard)
            .unwrap(),
        Some(exact)
    );
    let current = reopened
        .metadata_backend()
        .unwrap()
        .get_object(&written.object_id)
        .unwrap()
        .unwrap();
    assert_eq!(current.version, written.version);
    assert_eq!(
        reopened.posix_read_file(path).unwrap(),
        b"actual native bytes"
    );
    drop(reopened);
    runtime.close().unwrap();
}

#[test]
fn initial_epoch_binding_accepts_a_coherent_cut_before_an_unrelated_refresh() {
    let (runtime, mut service, config) = native_case("initial-binding-current-cut", 1);
    let current = directory(&mut service, "/services/initial-binding-current-cut");
    let mut changed = current.clone();
    changed.version = DfsVersion::generate();
    changed.mode = 0o600;
    let payload = namespace_payload(&config, &current, changed.clone(), "initial-binding-current-cut");
    let execution_guard = service.acquire_prepared_execution(&crate::DfsPreparedOperationScope {
        file_system_id: payload.file_system_id.clone(), shard_id: payload.shard_id.clone(),
        owner_epoch: payload.owner_epoch, operation_id: payload.operation_id.clone(),
    }).unwrap();
    let digest = service.metadata_backend_mut().unwrap().prepare_operation(payload, None).unwrap();
    let scope = service.metadata_backend().unwrap()
        .publication_owner_scope(&digest.file_system_id, &digest.shard_id).unwrap();
    let execution = service.publication_execution.current().expect("actual native effect admission");
    let authority = crate::runtime_host::daemon_dfs_metadata_authority_generation(
        &sinomo_core::network::ClusterPaths::new(runtime.data_dir(&config.primary_node_id)),
        &runtime.runtime_handle(),
    ).unwrap();
    let control = runtime.shared_control_plane().unwrap();
    let update = std::sync::Arc::clone(&authority);
    let observed = std::sync::Arc::new(std::sync::Mutex::new(None));
    let recorded = std::sync::Arc::clone(&observed);
    let fs = config.file_system_id.clone();
    crate::publication_guard::INITIAL_BINDING_AFTER_CUT.with(|slot| {
        assert!(slot.borrow_mut().replace(Box::new(move || {
            let before = update.projection_generation();
            let storage = update.storage_generation();
            control.with_client(|kv| {
                let mut store = DfsControlPlaneStore::new(kv.as_mut());
                let mut config = store.get_file_system_config(&fs).unwrap().unwrap();
                config.display_name.push_str("-real-binding-refresh");
                store.put_file_system_config(&config).unwrap();
            });
            crate::test_runtime::executor().block_on(async {
                tokio::time::timeout(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
                    update.ensure_write_configuration_ready_async()).await.unwrap().unwrap();
            });
            let after = update.projection_generation();
            assert!(after > before);
            assert_eq!(update.storage_generation(), storage);
            *recorded.lock().unwrap() = Some((before, after, storage));
        })).is_none());
    });
    let binding = service.publication_provider.validate_initial_binding_from_retained_provider(
        &scope, &digest, &execution, &config,
    );
    crate::publication_guard::INITIAL_BINDING_AFTER_CUT.with(|slot| slot.borrow_mut().take());
    let advanced = *observed.lock().unwrap();
    // Finish only the same genuine prepared body through the normal kernel.
    // This fixture drain does not turn a rejected read qualification into PASS.
    let completion = commit_kernel(&mut service, &digest);
    std::fs::write(
        runtime.data_dir(&config.primary_node_id).root().join("boundary-diagnostic-initial-binding-cut.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "digest": digest, "expected_shard": config, "actual_generation_advance": advanced,
            "binding_result": format!("{binding:?}"), "same_digest_completion": format!("{completion:?}"),
            "qualification": "Actual retained provider and execution owner. Real configuration transaction and shared native refresh; no injected cache value, fabricated authority or replacement operation identity."
        })).unwrap(),
    ).unwrap();
    let after = service.metadata_backend().unwrap().get_object(&current.object_id).unwrap();
    drop(execution);
    drop(execution_guard);
    drop(service);
    runtime.close().unwrap();
    assert!(advanced.is_some(), "the actual update boundary must be exercised");
    assert!(binding.is_ok(), "coherent initial binding cut: {binding:?}");
    assert!(completion.unwrap().published);
    assert_eq!(after, Some(changed));
}

#[test]
fn prepared_v7_initial_epoch_requires_the_real_host_provider() {
    let (mut runtime, mut service, config) = native_case("v7-initial-host-bind", 1);
    let shard = DfsMetadataShardId::new(config.shard_id.clone());
    let diagnostic = crate::native_boundary_diagnostics::capture(
        runtime.data_dir(&config.primary_node_id).root(),
    );
    let current = directory(&mut service, "/services/v7-initial-host-bind");
    let mut committed = current.clone();
    committed.version = DfsVersion::generate();
    committed.mode = 0o600;
    let payload = namespace_payload(&config, &current, committed.clone(), "v7-host-bind");
    // Like production publish_prepared_operation, own the real execution
    // window before exposing the durable prepare. Otherwise normal Main
    // recovery may correctly publish it before this test's explicit kernel.
    let execution = service
        .acquire_prepared_execution(&crate::DfsPreparedOperationScope {
            file_system_id: payload.file_system_id.clone(),
            shard_id: payload.shard_id.clone(),
            owner_epoch: payload.owner_epoch,
            operation_id: payload.operation_id.clone(),
        })
        .unwrap();
    let digest = service
        .metadata_backend_mut()
        .unwrap()
        .prepare_operation(payload, None)
        .unwrap();
    let scope = service
        .metadata_backend()
        .unwrap()
        .publication_owner_scope(&config.file_system_id, &shard)
        .unwrap();
    let mut unbound = LocalSobjMetadataBackend::new(shard.clone(), scope.root()).unwrap();
    assert_eq!(unbound.instance_authority_epoch, None);
    assert!(
        unbound
            .publication_writer_session(
                &config.file_system_id,
                &shard,
                &digest,
                None,
                None,
                None,
                Some(&config)
            )
            .is_err()
    );
    assert_eq!(unbound.instance_authority_epoch, None);
    assert!(
        unbound
            .committed_prepared_operation(&digest)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        unbound.get_object(&current.object_id).unwrap(),
        Some(current)
    );
    drop(unbound);
    // Controlled scheduling diagnostic: ask the SAME mounted Main owner to
    // reconcile its actual durable work. The actual native admission gate
    // decides whether it can run here; no proof, outcome or journal is supplied.
    let reconciliation = runtime
        .request_repair_reconciliation(&config.primary_node_id)
        .unwrap();
    let scheduler = crate::runtime_host::daemon_dfs_mutation_scheduler(
        runtime.data_dir(&config.primary_node_id),
    )
    .unwrap();
    let eligible = scheduler.try_admit_global();
    let maintenance_admitted = eligible.is_some();
    drop(eligible);
    if maintenance_admitted {
        let tick = runtime
            .maintenance_tick(
                &config.primary_node_id,
                sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            )
            .unwrap();
        diagnostic.dump(
            "competing-main",
            serde_json::json!({
                "digest": digest, "requested_reconciliation": reconciliation,
                "actual_global_admission_available": maintenance_admitted,
                "outcome": format!("{:?}", tick.outcome),
            }),
        );
    }
    let committed_result = commit_kernel(&mut service, &digest);
    diagnostic.dump(
        "initial-epoch-kernel",
        serde_json::json!({
            "digest": digest, "result": format!("{committed_result:?}"),
            "rpc": format!("{:?}", runtime.rpc_observations()),
        }),
    );
    let committed_result = committed_result.unwrap();
    assert!(committed_result.published);
    assert!(
        !maintenance_admitted,
        "the actual provider's retained admission excludes competing Main publication"
    );
    drop(execution);
    runtime
        .maintenance_tick(
            &config.primary_node_id,
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
        )
        .unwrap()
        .outcome
        .unwrap();
    assert_eq!(
        service
            .metadata_backend()
            .unwrap()
            .committed_prepared_operation(&digest)
            .unwrap(),
        Some(committed_result.entry),
        "Main recovery after release preserves the exact original commit"
    );
    assert_eq!(
        service
            .metadata_backend()
            .unwrap()
            .get_object(&committed.object_id)
            .unwrap(),
        Some(committed)
    );
    drop(service);
    runtime.close().unwrap();
}

#[test]
fn native_table_replay_validates_each_unchanged_owned_prepared_body_once() {
    let (mut runtime, mut service, config) = native_case("native-replay-owned-body-validation", 1);
    let expected = directory(&mut service, "/services/owned-body-validation");
    let source = runtime
        .data_dir(&config.primary_node_id)
        .dfs_dir()
        .join("metadata")
        .join(&config.shard_id);
    drop(service);
    runtime.stop_node(&config.primary_node_id).unwrap();
    let copy = sinomo_testdir::TestDir::new("native-replay-owned-body-copy");
    let source_snapshot = crate::metadata_table_store::metadata_table_snapshot_path(&source);
    let source_log = crate::metadata_table_store::metadata_table_path(&source);
    let copied_snapshot = crate::metadata_table_store::metadata_table_snapshot_path(&copy);
    let copied_log = crate::metadata_table_store::metadata_table_path(&copy);
    std::fs::create_dir_all(copied_log.parent().unwrap()).unwrap();
    let mut originals = Vec::new();
    for (from, to) in [(source_snapshot, copied_snapshot), (source_log, copied_log)] {
        let before = std::fs::read(&from).unwrap();
        assert!(
            before.len() <= 8 * 1024 * 1024,
            "bounded native fixture source"
        );
        std::fs::copy(&from, &to).unwrap();
        assert_eq!(std::fs::read(&to).unwrap(), before);
        originals.push((from, to, before));
    }
    // The ordinary reader replays copies of ACTUAL native table frames. No
    // synthetic authority, expected records, successful marker or altered body
    // is supplied to the projector. The stopped original remains untouched.
    crate::metadata_records::reset_prepared_payload_digest_count();
    let replayed = crate::metadata_table_store::read_metadata_table_state_lenient(&copy).unwrap();
    let prepared_count = replayed.tables.prepared_operations_v7.len();
    assert!(prepared_count > 0);
    assert_eq!(
        crate::metadata_records::prepared_payload_digest_count(),
        prepared_count,
        "prepare and commit replay retain the same immutable body and must not serialize/hash it repeatedly"
    );
    assert_eq!(
        replayed.tables.objects.get(&expected.object_id),
        Some(&expected)
    );
    for record in replayed.tables.prepared_operations_v7.values() {
        assert_eq!(record.status, crate::DfsPreparedOperationStatus::Committed);
        record
            .committed_entry
            .as_ref()
            .unwrap()
            .validate(&record.digest)
            .unwrap();
    }
    for (from, to, before) in originals {
        assert_eq!(std::fs::read(from).unwrap(), before);
        assert_eq!(std::fs::read(to).unwrap(), before);
    }
    runtime.close().unwrap();
}

#[test]
fn native_replay_validation_keeps_mutable_record_checks_and_releases_body_custody() {
    let (runtime, mut service, config) = native_case("native-replay-body-custody", 1);
    directory(&mut service, "/services/replay-body-custody");
    let shard = DfsMetadataShardId::new(config.shard_id.clone());
    let operation = entries(&service, &shard)
        .into_iter()
        .find_map(|entry| {
            if let DfsMetadataMutation::PrepareOperationV7 { record, .. } = entry.mutation {
                Some(record.digest.operation_id)
            } else {
                None
            }
        })
        .unwrap();
    let actual = service
        .metadata_backend()
        .unwrap()
        .prepared_operation_record(&operation)
        .unwrap()
        .unwrap();
    assert_eq!(actual.status, crate::DfsPreparedOperationStatus::Committed);
    drop(service);
    runtime.close().unwrap();
    use crate::metadata_records::{
        prepared_payload_digest_count, reset_prepared_payload_digest_count,
        with_prepared_payload_validation,
    };
    with_prepared_payload_validation(|| {
        reset_prepared_payload_digest_count();
        actual.validate().unwrap();
        actual.validate().unwrap();
        assert_eq!(prepared_payload_digest_count(), 1);
        let mut changed_scope = actual.clone();
        changed_scope.digest.owner_epoch += 1;
        assert!(changed_scope.validate().is_err());
        let mut changed_slot = actual.clone();
        changed_slot.digest.prepare_sequence = 0;
        assert!(changed_slot.validate().is_err());
        let mut changed_terminal = actual.clone();
        changed_terminal.status = crate::DfsPreparedOperationStatus::Prepared;
        assert!(changed_terminal.validate().is_err());
        let mut changed_reference = actual.clone();
        changed_reference.committed_entry.as_mut().unwrap().sequence =
            actual.digest.prepare_sequence;
        assert!(changed_reference.validate().is_err());
        assert_eq!(
            prepared_payload_digest_count(),
            1,
            "only immutable-body work is reused; record checks still reject changes"
        );
        let mut changed_body = actual.clone();
        Arc::make_mut(&mut changed_body.payload)
            .transaction
            .created_at += time::Duration::nanoseconds(1);
        assert!(changed_body.validate().is_err());
        assert_eq!(
            prepared_payload_digest_count(),
            2,
            "a changed allocation is validated anew"
        );
    });
    let mut unique = actual.clone();
    unique.payload = Arc::new(actual.payload.as_ref().clone());
    with_prepared_payload_validation(|| {
        unique.validate().unwrap();
        assert_eq!(
            Arc::strong_count(&unique.payload),
            1,
            "validation must not retain the body"
        );
        Arc::make_mut(&mut unique.payload).transaction.created_at += time::Duration::nanoseconds(1);
        assert!(
            unique.validate().is_err(),
            "unique-owner mutation dissociates the old weak generation"
        );
    });
    let mut outside = actual.clone();
    outside.payload = Arc::new(actual.payload.as_ref().clone());
    reset_prepared_payload_digest_count();
    outside.validate().unwrap();
    outside.validate().unwrap();
    assert_eq!(
        prepared_payload_digest_count(),
        2,
        "ordinary public validation retains no replay observation"
    );
    assert!(Arc::get_mut(&mut outside.payload).is_some());
    let aborted = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        with_prepared_payload_validation(|| {
            outside.validate().unwrap();
            panic!("injected owned replay unwind");
        });
    }));
    assert!(aborted.is_err());
    assert!(
        Arc::get_mut(&mut outside.payload).is_some(),
        "unwind releases the replay's weak custody before handback"
    );
    with_prepared_payload_validation(|| {
        outside.validate().unwrap();
        let witness = Arc::downgrade(&outside.payload);
        drop(outside);
        assert!(
            witness.upgrade().is_none(),
            "a decoded-body observation cannot keep an evicted body alive"
        );
    });
}
