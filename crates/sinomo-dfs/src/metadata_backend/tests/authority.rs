//! Authority regression tests.

use super::*;

#[test]
fn sharded_backend_compaction_skips_activated_retiring_shard() {
    let root = temp_root("sharded_backend_compaction_skips_retiring_shard");
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
    let mut root_tombstone = object(version("version_compaction_active_root"));
    root_tombstone.path = "/deleted.md".to_string();
    root_tombstone.kind = DfsObjectKind::Tombstone;
    root_tombstone.content_hash = None;
    root_tombstone.changed_at_unix_nanos = 1;
    backend.put_file_system(&file_system).expect("filesystem");
    backend.put_object(&root_tombstone).expect("root tombstone");

    let services = DfsMetadataShardId::new("services");
    backend
        .freeze_metadata_shard_for_membership_handoff(&services, "handoff-retired-services", 1)
        .expect("freeze services shard")
        .expect("configured services shard");
    backend
        .mark_metadata_shard_membership_handoff_activated(&services, "handoff-retired-services", 2)
        .expect("mark successor activation");

    let report = backend
        .compact_tombstones(
            DfsTombstoneCompactionOptions::compact_eligible(Duration::ZERO)
                .with_now_unix_nanos(100),
        )
        .expect("an unrelated retired shard must not abort compaction");
    assert_eq!(report.compacted_tombstone_count, 1);
    assert_eq!(
        backend
            .get_object(&root_tombstone.object_id)
            .expect("compacted root tombstone"),
        None
    );
    assert!(matches!(
        backend
            .shard_backend_mut(&services)
            .expect("services backend")
            .put_object(&object(version("version_retired_shard_stays_frozen"))),
        Err(MetadataBackendError::MembershipHandoffFrozen { .. })
    ));

    cleanup(&root);
}

#[test]
fn replicated_snapshot_matching_exact_authority_floor_repairs_same_sequence() {
    let leader_root = temp_root("snapshot_matching_exact_authority_floor_leader");
    let replica_root = temp_root("snapshot_matching_exact_authority_floor_replica");
    let shard_id = DfsMetadataShardId::new("root");
    let mut leader = LocalSobjMetadataBackend::new(shard_id.clone(), &leader_root).expect("leader");
    leader
        .put_file_system(&file_system())
        .expect("leader filesystem");
    let sequence = leader.journal_last_sequence();
    let snapshot = leader.export_table_snapshot();
    let prefix = snapshot
        .prefix_proof
        .clone()
        .expect("authoritative snapshot prefix");

    let mut replica =
        LocalSobjMetadataBackend::new(shard_id.clone(), &replica_root).expect("replica");
    replica
        .put_metadata_authority_ack_floor(&DfsMetadataAuthorityAckFloor {
            shard_id: shard_id.clone(),
            journal_sequence: sequence,
            durable_prefix_proof: Some(prefix.clone()),
        })
        .expect("exact authority floor");

    replica
        .replace_with_replicated_snapshot(shard_id, sequence, snapshot)
        .expect("matching exact snapshot repair");
    assert_eq!(
        replica.metadata_prefix_proof().expect("installed prefix"),
        prefix
    );

    cleanup(&leader_root);
    cleanup(&replica_root);
}

#[test]
fn replicated_snapshot_rejects_divergent_or_legacy_same_sequence_authority_floor() {
    let leader_root = temp_root("snapshot_divergent_authority_floor_leader");
    let divergent_root = temp_root("snapshot_divergent_authority_floor_replica");
    let legacy_root = temp_root("snapshot_legacy_authority_floor_replica");
    let shard_id = DfsMetadataShardId::new("root");
    let mut leader = LocalSobjMetadataBackend::new(shard_id.clone(), &leader_root).expect("leader");
    leader
        .put_file_system(&file_system())
        .expect("leader filesystem");
    let sequence = leader.journal_last_sequence();
    let snapshot = leader.export_table_snapshot();
    let mut divergent_prefix = snapshot
        .prefix_proof
        .clone()
        .expect("authoritative snapshot prefix");
    let final_digit = divergent_prefix.digest.pop().expect("digest digit");
    divergent_prefix
        .digest
        .push(if final_digit == '0' { '1' } else { '0' });
    assert!(divergent_prefix.is_well_formed());

    let mut divergent =
        LocalSobjMetadataBackend::new(shard_id.clone(), &divergent_root).expect("replica");
    divergent
        .put_metadata_authority_ack_floor(&DfsMetadataAuthorityAckFloor {
            shard_id: shard_id.clone(),
            journal_sequence: sequence,
            durable_prefix_proof: Some(divergent_prefix),
        })
        .expect("divergent exact authority floor");
    assert!(
        divergent
            .replace_with_replicated_snapshot(shard_id.clone(), sequence, snapshot.clone(),)
            .expect_err("divergent exact floor must reject same-sequence repair")
            .to_string()
            .contains("prefix conflicts with locally acknowledged authority operation")
    );

    let mut legacy =
        LocalSobjMetadataBackend::new(shard_id.clone(), &legacy_root).expect("replica");
    legacy
        .put_metadata_authority_ack_floor(&DfsMetadataAuthorityAckFloor {
            shard_id: shard_id.clone(),
            journal_sequence: sequence,
            durable_prefix_proof: None,
        })
        .expect("legacy authority floor");
    assert!(
        legacy
            .replace_with_replicated_snapshot(shard_id, sequence, snapshot)
            .expect_err("legacy floor must fail closed at the same sequence")
            .to_string()
            .contains("prefix conflicts with locally acknowledged authority operation")
    );

    cleanup(&leader_root);
    cleanup(&divergent_root);
    cleanup(&legacy_root);
}

#[test]
fn metadata_authority_ack_floor_put_skips_rotated_table_generation_refresh() {
    let root = temp_root("local_backend_metadata_authority_floor_rotated");
    let shard_id = DfsMetadataShardId::new("root");
    let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
    backend
        .put_file_system(&file_system())
        .expect("materialize table log before rotation");
    {
        let _lock = backend.metadata_write_lock().expect("metadata writer lock");
        assert!(
            crate::metadata_table_store::rotate_metadata_table_log_for_checkpoint(&root, 0)
                .expect("rotate table generation")
        );
    }
    assert!(metadata_table_checkpoint_log_path(&root).is_file());
    let durable_generation_before =
        metadata_table_storage_generation(&root).expect("durable table generation");
    let cached_generation_before = backend.table_storage_generation.clone();
    let tables_before = backend.tables.clone();
    let table_log_offset_before = backend.table_log_offset;
    let materialized_sequence_before = backend.materialized_through_sequence;
    let prefix_proof_before = backend.prefix_proof.clone();
    let floor = DfsMetadataAuthorityAckFloor {
        shard_id: shard_id.clone(),
        journal_sequence: materialized_sequence_before,
        durable_prefix_proof: None,
    };

    backend
        .put_metadata_authority_ack_floor(&floor)
        .expect("advance authority acknowledgement floor without table refresh");

    assert_eq!(
        metadata_table_storage_generation(&root).expect("unchanged durable generation"),
        durable_generation_before,
        "authority acknowledgement sidecars must not replace table files"
    );
    assert_eq!(
        backend.table_storage_generation, cached_generation_before,
        "authority acknowledgement sidecars must not adopt or replay the rotated table generation"
    );
    assert_eq!(backend.tables, tables_before);
    assert_eq!(backend.table_log_offset, table_log_offset_before);
    assert_eq!(
        backend.materialized_through_sequence,
        materialized_sequence_before
    );
    assert_eq!(backend.prefix_proof, prefix_proof_before);
    assert_eq!(
        backend
            .metadata_authority_ack_floor(&shard_id)
            .expect("persisted authority acknowledgement floor"),
        Some(floor)
    );

    cleanup(&root);
}

#[test]
fn membership_handoff_freeze_survives_reopen_and_blocks_new_journal_entries() {
    let root = temp_root("membership-handoff-freeze-reopen");
    let metadata_root = root.join("metadata");
    let shard_id = DfsMetadataShardId::new("root");
    let mut backend =
        LocalSobjMetadataBackend::new(shard_id.clone(), &metadata_root).expect("metadata backend");
    backend.put_file_system(&file_system()).expect("filesystem");
    let frozen_head = backend.journal_last_sequence();
    let freeze = backend
        .freeze_metadata_shard_for_membership_handoff(&shard_id, "handoff-freeze-reopen", 7)
        .expect("install membership freeze")
        .expect("matching local shard");
    assert_eq!(freeze.journal_sequence, frozen_head);
    assert_eq!(freeze.activated_epoch, None);
    assert!(!freeze.state_fingerprint.is_empty());
    assert_eq!(
        freeze
            .rebind_prefix_proof
            .as_ref()
            .map(|proof| (proof.journal_sequence, proof.digest.as_str())),
        Some((freeze.journal_sequence, freeze.state_fingerprint.as_str()))
    );

    assert!(matches!(
        backend.put_object(&object(version("version_frozen_write"))),
        Err(MetadataBackendError::MembershipHandoffFrozen {
            ref handoff_id,
            ..
        }) if handoff_id == "handoff-freeze-reopen"
    ));
    assert_eq!(backend.journal_last_sequence(), frozen_head);

    drop(backend);
    let mut reopened = LocalSobjMetadataBackend::new(shard_id.clone(), &metadata_root)
        .expect("a frozen backend must reopen for handoff reconciliation");
    assert_eq!(
        reopened
            .metadata_shard_write_freeze(&shard_id)
            .expect("read durable freeze"),
        Some(freeze)
    );
    assert_eq!(reopened.journal_last_sequence(), frozen_head);
    assert!(matches!(
        reopened.put_object(&object(version("version_frozen_reopened_write"))),
        Err(MetadataBackendError::MembershipHandoffFrozen { .. })
    ));
    assert_eq!(reopened.journal_last_sequence(), frozen_head);

    assert!(
        reopened
            .clear_metadata_shard_membership_handoff_freeze(
                &shard_id,
                "different-handoff",
                &root_shard_config_at_epoch(&shard_id.0, 8),
            )
            .is_err(),
        "a completion for another handoff must not release this marker"
    );
    assert!(
        reopened
            .clear_metadata_shard_membership_handoff_freeze(
                &shard_id,
                "handoff-freeze-reopen",
                &root_shard_config_at_epoch(&shard_id.0, 9),
            )
            .is_err(),
        "a completion must bind the exact successor authority epoch"
    );
    assert!(matches!(
        reopened.put_object(&object(version("version_after_wrong_completion"))),
        Err(MetadataBackendError::MembershipHandoffFrozen { .. })
    ));

    assert!(
        reopened
            .clear_metadata_shard_membership_handoff_freeze(
                &shard_id,
                "handoff-freeze-reopen",
                &root_shard_config_at_epoch(&shard_id.0, 8),
            )
            .expect("advance handoff marker to activated generation")
    );
    assert_eq!(
        reopened
            .metadata_shard_write_freeze(&shard_id)
            .expect("read durable active generation")
            .and_then(|fence| fence.activated_epoch),
        Some(8)
    );
    reopened
        .put_object(&object(version("version_after_handoff_activation")))
        .expect("normal metadata append resumes after activation");
    assert!(reopened.journal_last_sequence() > frozen_head);
    let checkpoint_proof = reopened
        .metadata_prefix_proof()
        .expect("post-activation prefix proof");
    reopened
        .compact_metadata_table_log_if_needed(0)
        .expect("checkpoint activated metadata generation");
    assert_eq!(
        reopened
            .metadata_shard_write_freeze(&shard_id)
            .expect("read checkpointed active generation")
            .and_then(|marker| marker.rebind_prefix_proof),
        Some(checkpoint_proof),
        "checkpoint publication must advance the restart proof before pruning journal history"
    );

    cleanup(&root);
}

#[test]
fn activated_membership_marker_does_not_bind_retiring_backend() {
    let root = temp_root("membership-handoff-nonbinding-activation");
    let metadata_root = root.join("metadata");
    let shard_id = DfsMetadataShardId::new("root");
    let mut retiring = LocalSobjMetadataBackend::new(shard_id.clone(), &metadata_root)
        .expect("retiring metadata backend");
    retiring
        .put_file_system(&file_system())
        .expect("filesystem");
    let frozen_head = retiring.journal_last_sequence();

    retiring
        .freeze_metadata_shard_for_membership_handoff(
            &shard_id,
            "handoff-nonbinding-activation",
            17,
        )
        .expect("freeze retiring generation")
        .expect("local shard");
    assert!(
        retiring
            .mark_metadata_shard_membership_handoff_activated(
                &shard_id,
                "handoff-nonbinding-activation",
                18,
            )
            .expect("durably mark activated generation")
    );
    assert_eq!(retiring.instance_authority_epoch, None);
    assert_eq!(
        retiring
            .metadata_shard_write_freeze(&shard_id)
            .expect("read activated marker")
            .and_then(|marker| marker.activated_epoch),
        Some(18)
    );
    assert!(matches!(
        retiring.put_object(&object(version("version_retiring_stays_frozen"))),
        Err(MetadataBackendError::MembershipHandoffFrozen {
            ref handoff_id,
            ..
        }) if handoff_id == "handoff-nonbinding-activation"
    ));
    assert_eq!(retiring.journal_last_sequence(), frozen_head);

    assert!(
        retiring
            .clear_metadata_shard_membership_handoff_freeze(
                &shard_id,
                "handoff-nonbinding-activation",
                &root_shard_config_at_epoch(&shard_id.0, 18),
            )
            .expect("fresh authority observer binds exact activated generation")
    );
    assert_eq!(retiring.instance_authority_epoch, Some(18));
    retiring
        .put_object(&object(version("version_after_explicit_bind")))
        .expect("explicitly rebound generation may append");

    cleanup(&root);
}

#[test]
fn activated_retiring_backend_accepts_replica_ingest_without_binding_authority() {
    let root = temp_root("membership-handoff-retiring-replica-ingest");
    let shard_id = DfsMetadataShardId::new("root");
    let active_source_node_id = node("active-successor");
    let mut source = LocalSobjMetadataBackend::new(shard_id.clone(), root.join("source"))
        .expect("source metadata backend");
    let mut retiring = LocalSobjMetadataBackend::new(shard_id.clone(), root.join("retiring"))
        .expect("retiring metadata backend");

    source
        .put_file_system(&file_system())
        .expect("source filesystem");
    let initial = source
        .journal_batch_since_limited(0, 16, usize::MAX)
        .expect("initial source batch")
        .expect("initial source entries");
    retiring
        .apply_journal_batch(&initial)
        .expect("seed retiring replica");
    assert_eq!(
        retiring.metadata_prefix_proof().expect("retiring proof"),
        source.metadata_prefix_proof().expect("source proof")
    );

    retiring
        .freeze_metadata_shard_for_membership_handoff(
            &shard_id,
            "handoff-retiring-replica-ingest",
            17,
        )
        .expect("freeze retiring generation")
        .expect("local shard");

    let replicated_object = object(version("version_retiring_replica_batch"));
    source
        .put_object(&replicated_object)
        .expect("successor object");
    let batch = source
        .journal_batch_since_limited(initial.next_after_sequence, 16, usize::MAX)
        .expect("successor journal batch")
        .expect("successor journal entries");
    let exact_permit = DfsMetadataReplicaIngestPermit::new_validated(
        file_system_id(),
        shard_id.clone(),
        active_source_node_id.clone(),
        18,
    );
    let later_current_permit = DfsMetadataReplicaIngestPermit::new_validated(
        file_system_id(),
        shard_id.clone(),
        active_source_node_id.clone(),
        20,
    );
    assert!(matches!(
        retiring.apply_journal_batch(&batch),
        Err(MetadataBackendError::MembershipHandoffFrozen { .. })
    ));
    assert!(matches!(
        retiring.apply_journal_batch_with_replica_permit(&exact_permit, &batch),
        Err(MetadataBackendError::MembershipHandoffFrozen { .. })
    ));

    assert!(
        retiring
            .mark_metadata_shard_membership_handoff_activated(
                &shard_id,
                "handoff-retiring-replica-ingest",
                18,
            )
            .expect("mark successor generation activated")
    );
    assert_eq!(retiring.instance_authority_epoch, None);
    assert!(
        retiring
            .metadata_shard_authoritative_writes_frozen(&shard_id)
            .expect("author fence")
    );
    assert!(matches!(
        retiring.apply_journal_batch(&batch),
        Err(MetadataBackendError::MembershipHandoffFrozen { .. })
    ));
    let wrong_shard_permit = DfsMetadataReplicaIngestPermit::new_validated(
        file_system_id(),
        DfsMetadataShardId::new("services"),
        active_source_node_id.clone(),
        18,
    );
    assert!(matches!(
        retiring.apply_journal_batch_with_replica_permit(&wrong_shard_permit, &batch),
        Err(MetadataBackendError::Serde(_))
    ));
    let stale_epoch_permit = DfsMetadataReplicaIngestPermit::new_validated(
        file_system_id(),
        shard_id.clone(),
        active_source_node_id.clone(),
        17,
    );
    assert!(matches!(
        retiring.apply_journal_batch_with_replica_permit(&stale_epoch_permit, &batch),
        Err(MetadataBackendError::MembershipHandoffFrozen { .. })
    ));

    let foreign_file_system_id = DfsFileSystemId::from_legacy_named("fs_foreign_replica_ingest")
        .expect("foreign filesystem");
    let wrong_file_system_permit = DfsMetadataReplicaIngestPermit::new_validated(
        foreign_file_system_id.clone(),
        shard_id.clone(),
        active_source_node_id,
        18,
    );
    let before_rejected_batches = retiring.export_table_snapshot();
    let before_rejected_batch_head = retiring.journal_last_sequence();
    reset_metadata_begin_write_calls();

    let wrong_file_system_error = retiring
        .apply_journal_batch_with_replica_permit(&wrong_file_system_permit, &batch)
        .expect_err("foreign-filesystem permit must reject a target batch");
    assert!(
        wrong_file_system_error
            .to_string()
            .contains("permit filesystem")
    );

    let mut embedded_mismatch_batch = batch.clone();
    let DfsMetadataMutation::PutObject {
        object: Some(embedded_object),
        ..
    } = &mut embedded_mismatch_batch.entries[0].mutation
    else {
        panic!("expected replicated object mutation");
    };
    embedded_object.file_system_id = foreign_file_system_id.clone();
    let embedded_mismatch_error = retiring
        .apply_journal_batch_with_replica_permit(&exact_permit, &embedded_mismatch_batch)
        .expect_err("outer and embedded filesystem mismatch must reject");
    assert!(
        embedded_mismatch_error
            .to_string()
            .contains("object record filesystem")
    );

    let mut mixed_file_system_batch = batch.clone();
    let mut foreign_entry = mixed_file_system_batch.entries[0].clone();
    foreign_entry.sequence = foreign_entry.sequence.saturating_add(1);
    let DfsMetadataMutation::PutObject {
        file_system_id,
        object_id,
        path,
        object: Some(foreign_object),
    } = &mut foreign_entry.mutation
    else {
        panic!("expected replicated object mutation");
    };
    *file_system_id = foreign_file_system_id.clone();
    *object_id =
        DfsObjectId::from_legacy_named("obj_foreign_replica_ingest").expect("foreign object");
    *path = "docs/foreign-replica-ingest.md".to_string();
    foreign_object.file_system_id = foreign_file_system_id.clone();
    foreign_object.object_id = object_id.clone();
    foreign_object.path = path.clone();
    mixed_file_system_batch.next_after_sequence = foreign_entry.sequence;
    mixed_file_system_batch.entries.push(foreign_entry);
    let mixed_file_system_error = retiring
        .apply_journal_batch_with_replica_permit(&exact_permit, &mixed_file_system_batch)
        .expect_err("mixed-filesystem batch must reject atomically");
    assert!(
        mixed_file_system_error
            .to_string()
            .contains("outer mutation filesystem")
    );

    assert_eq!(
        metadata_begin_write_calls(),
        0,
        "filesystem validation must reject before writer-lock refresh"
    );
    assert_eq!(retiring.export_table_snapshot(), before_rejected_batches);
    assert_eq!(retiring.journal_last_sequence(), before_rejected_batch_head);
    assert_eq!(
        retiring
            .get_object(&replicated_object.object_id)
            .expect("rejected object lookup"),
        None,
        "no valid prefix of a mixed batch may materialize"
    );
    assert_eq!(
        retiring
            .apply_journal_batch_with_replica_permit(&later_current_permit, &batch)
            .expect("later current successor batch crosses older activated author fence"),
        1
    );
    assert_eq!(
        retiring.metadata_prefix_proof().expect("retiring proof"),
        source.metadata_prefix_proof().expect("successor proof")
    );
    assert_eq!(retiring.instance_authority_epoch, None);
    assert!(matches!(
        retiring.put_object(&object(version(
            "version_retiring_author_rejected_after_batch"
        ))),
        Err(MetadataBackendError::MembershipHandoffFrozen { .. })
    ));

    let snapshotted_object = {
        let mut object = object(version("version_retiring_replica_snapshot"));
        object.object_id =
            DfsObjectId::from_legacy_named("obj_retiring_replica_snapshot").expect("object");
        object.path = "docs/replica-snapshot.md".to_string();
        object
    };
    source
        .put_object(&snapshotted_object)
        .expect("successor snapshot object");
    let snapshot_sequence = source.journal_last_sequence();
    let snapshot = source.export_table_snapshot();
    assert!(matches!(
        retiring.replace_with_replicated_snapshot(
            shard_id.clone(),
            snapshot_sequence,
            snapshot.clone(),
        ),
        Err(MetadataBackendError::MembershipHandoffFrozen { .. })
    ));
    let before_rejected_snapshots = retiring.export_table_snapshot();
    let before_rejected_snapshot_head = retiring.journal_last_sequence();
    reset_metadata_begin_write_calls();

    let wrong_file_system_snapshot_error = retiring
        .replace_with_replicated_snapshot_with_replica_permit(
            &wrong_file_system_permit,
            shard_id.clone(),
            snapshot_sequence,
            snapshot.clone(),
        )
        .expect_err("foreign-filesystem permit must reject a target snapshot");
    assert!(
        wrong_file_system_snapshot_error
            .to_string()
            .contains("permit filesystem")
    );

    let mut mixed_file_system_snapshot = snapshot.clone();
    let mut foreign_snapshot_object = snapshotted_object.clone();
    foreign_snapshot_object.file_system_id = foreign_file_system_id;
    foreign_snapshot_object.object_id =
        DfsObjectId::from_legacy_named("obj_foreign_replica_snapshot")
            .expect("foreign snapshot object");
    foreign_snapshot_object.path = "docs/foreign-replica-snapshot.md".to_string();
    mixed_file_system_snapshot
        .tables
        .as_mut()
        .expect("structured snapshot tables")
        .objects
        .push(foreign_snapshot_object);
    let mixed_snapshot_fingerprint =
        dfs_metadata_table_snapshot_state_fingerprint(&mixed_file_system_snapshot)
            .expect("mixed snapshot semantic fingerprint");
    mixed_file_system_snapshot.semantic_state_fingerprint = Some(mixed_snapshot_fingerprint);
    let mixed_file_system_snapshot_error = retiring
        .replace_with_replicated_snapshot_with_replica_permit(
            &exact_permit,
            shard_id.clone(),
            snapshot_sequence,
            mixed_file_system_snapshot,
        )
        .expect_err("mixed-filesystem snapshot must reject atomically");
    assert!(
        mixed_file_system_snapshot_error
            .to_string()
            .contains("object record filesystem")
    );

    assert_eq!(
        metadata_begin_write_calls(),
        0,
        "snapshot filesystem validation must reject before writer-lock refresh"
    );
    assert_eq!(retiring.export_table_snapshot(), before_rejected_snapshots);
    assert_eq!(
        retiring.journal_last_sequence(),
        before_rejected_snapshot_head
    );
    assert_eq!(
        retiring
            .get_object(&snapshotted_object.object_id)
            .expect("rejected snapshot object lookup"),
        None,
        "a rejected mixed snapshot must not replace any tables"
    );
    retiring
        .replace_with_replicated_snapshot_with_replica_permit(
            &later_current_permit,
            shard_id.clone(),
            snapshot_sequence,
            snapshot,
        )
        .expect("later current successor snapshot crosses older activated author fence");
    assert_eq!(
        retiring.metadata_prefix_proof().expect("retiring proof"),
        source.metadata_prefix_proof().expect("snapshot proof")
    );
    assert_eq!(
        retiring
            .get_object(&snapshotted_object.object_id)
            .expect("snapshotted object"),
        Some(snapshotted_object)
    );
    assert_eq!(retiring.instance_authority_epoch, None);
    assert_eq!(
        retiring
            .metadata_shard_write_freeze(&shard_id)
            .expect("preserved activated marker")
            .and_then(|marker| marker.activated_epoch),
        Some(18),
        "snapshot repair must preserve the durable author fence"
    );
    assert!(
        retiring
            .metadata_shard_authoritative_writes_frozen(&shard_id)
            .expect("author fence after snapshot")
    );
    assert!(matches!(
        retiring.put_object(&object(version(
            "version_retiring_author_rejected_after_snapshot"
        ))),
        Err(MetadataBackendError::MembershipHandoffFrozen { .. })
    ));

    cleanup(&root);
}

#[test]
fn activated_membership_generation_keeps_independently_opened_stale_backend_fenced() {
    let root = temp_root("membership-handoff-stale-open-instance");
    let metadata_root = root.join("metadata");
    let shard_id = DfsMetadataShardId::new("root");
    let mut completing = LocalSobjMetadataBackend::new(shard_id.clone(), &metadata_root)
        .expect("completing metadata backend");
    completing
        .put_file_system(&file_system())
        .expect("filesystem");
    let mut stale = LocalSobjMetadataBackend::new(shard_id.clone(), &metadata_root)
        .expect("independently opened stale backend");

    completing
        .freeze_metadata_shard_for_membership_handoff(
            &shard_id,
            "handoff-cross-process-generation",
            11,
        )
        .expect("freeze retiring generation")
        .expect("local shard");
    assert!(
        completing
            .clear_metadata_shard_membership_handoff_freeze(
                &shard_id,
                "handoff-cross-process-generation",
                &root_shard_config_at_epoch(&shard_id.0, 12),
            )
            .expect("activate generation")
    );
    completing
        .put_object(&object(version("version_generation_12")))
        .expect("completing instance is rebound to generation 12");

    assert!(matches!(
        stale.put_object(&object(version("version_stale_generation_11"))),
        Err(MetadataBackendError::MembershipHandoffFrozen {
            ref handoff_id,
            ..
        }) if handoff_id == "handoff-cross-process-generation"
    ));
    stale
        .clear_metadata_shard_membership_handoff_freeze(
            &shard_id,
            "handoff-cross-process-generation",
            &root_shard_config_at_epoch(&shard_id.0, 12),
        )
        .expect("fresh control-plane observer rebinds stale instance");
    stale
        .put_object(&object(version("version_rebound_generation_12")))
        .expect("rebound instance may write the active generation");

    // The same active marker can become the next pending generation only
    // on an instance already rebound to its exact retiring epoch.
    let next = stale
        .freeze_metadata_shard_for_membership_handoff(&shard_id, "handoff-next-generation", 12)
        .expect("install next generation freeze")
        .expect("local shard");
    assert_eq!(next.activated_epoch, None);
    assert_eq!(next.retiring_epoch, 12);

    cleanup(&root);
}

#[test]
fn metadata_history_gc_is_fenced_during_membership_handoff() {
    let root = temp_root("metadata-history-gc-handoff-freeze");
    let shard_id = DfsMetadataShardId::new("root");
    let current_version = version("version_gc_frozen_current");
    let current = object(current_version.clone());
    let old_version = version("version_gc_frozen_old");
    let old_record =
        object_version_record(&current, old_version.clone(), OffsetDateTime::UNIX_EPOCH);
    let current_record = object_version_record(
        &current,
        current_version,
        OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1),
    );
    let mut backend =
        LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("metadata backend");
    backend.put_file_system(&file_system()).expect("filesystem");
    backend.put_object(&current).expect("current object");
    backend.put_version(&old_record).expect("old version");
    backend
        .put_version(&current_record)
        .expect("current version");
    backend
        .freeze_metadata_shard_for_membership_handoff(&shard_id, "metadata-history-gc-freeze", 1)
        .expect("freeze shard")
        .expect("local freeze");
    let frozen_head = backend.journal_last_sequence();

    assert!(matches!(
        backend.gc_metadata_history_authoritative(
            DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 1)
                .with_now_unix_nanos(2_000_000_000),
        ),
        Err(MetadataBackendError::MembershipHandoffFrozen {
            ref handoff_id,
            ..
        }) if handoff_id == "metadata-history-gc-freeze"
    ));
    assert_eq!(backend.journal_last_sequence(), frozen_head);
    assert_eq!(
        backend
            .get_version(&current.object_id, &old_version)
            .expect("frozen old version"),
        Some(old_record)
    );

    cleanup(&root);
}
