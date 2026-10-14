//! Native PreparedV7 kernel regressions; author guards come from real hosted services.
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
    let recorded = service.commit_stored_prepared_operation(&digest).unwrap();
    assert!(!recorded.published);
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
        Some(entry)
    );
    assert_eq!(
        reopened
            .metadata_backend()
            .unwrap()
            .get_object(&current.object_id)
            .unwrap(),
        Some(committed)
    );
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
fn prepared_v7_initial_epoch_requires_the_real_host_provider() {
    let (mut runtime, mut service, config) = native_case("v7-initial-host-bind", 1);
    let shard = DfsMetadataShardId::new(config.shard_id.clone());
    let current = directory(&mut service, "/services/v7-initial-host-bind");
    let mut committed = current.clone();
    committed.version = DfsVersion::generate();
    committed.mode = 0o600;
    let payload = namespace_payload(&config, &current, committed.clone(), "v7-host-bind");
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
    assert!(commit_kernel(&mut service, &digest).unwrap().published);
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
