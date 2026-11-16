//! Durable member state and epoch-scoped acknowledgement floors of a
//! write-set shard, on real shard directories.

use super::*;
use crate::{
    DfsMetadataMemberAckView, DfsMetadataShardRecordCut, DfsMetadataShardWriteSet,
    DfsMetadataShardWriteSetChange, DfsMetadataShardWriteSetChangeKind,
    DfsMetadataTakeoverPromiseEnd,
};
use sinomo_core::kv::KvRequestId;

#[path = "write_set_member_state/native_snapshots.rs"]
mod native_snapshots;

fn node(name: &str) -> NodeId {
    NodeId::from_legacy_named(name).unwrap()
}

fn shard_id() -> DfsMetadataShardId {
    DfsMetadataShardId::new("root")
}

/// A switched root shard at `epoch` with primary A and replicas B and C, at
/// write-set `generation`.
fn cut(epoch: u64, generation: u64) -> DfsMetadataShardRecordCut {
    let members = ["node_member_a", "node_member_b", "node_member_c"].map(node);
    let shard = crate::DfsMetadataShardConfig {
        file_system_id: file_system_id(),
        shard_id: "root".into(),
        primary_node_id: members[0].clone(),
        replica_node_ids: members[1..].to_vec(),
        epoch,
        assignment: crate::DfsMetadataShardAssignment::Root,
        default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
    };
    let mut write_set = members.to_vec();
    write_set.sort();
    let record = DfsMetadataShardWriteSet {
        format: crate::DFS_METADATA_SHARD_WRITE_SET_FORMAT,
        file_system_id: file_system_id(),
        shard_id: "root".into(),
        epoch,
        primary_node_id: members[0].clone(),
        generation,
        write_set,
        epoch_start: None,
        switched_at_epoch: epoch,
        substitutes: Vec::new(),
        raised_n: None,
        last_change: DfsMetadataShardWriteSetChange {
            kind: DfsMetadataShardWriteSetChangeKind::Switch,
            change_id: "switch".into(),
        },
    };
    DfsMetadataShardRecordCut {
        shard_value: sinomo_core::kv::encode_kv_value(&shard).unwrap(),
        shard,
        write_set_value: Some(sinomo_core::kv::encode_kv_value(&record).unwrap()),
        handoff_value: None,
        watermark_value: None,
        operation_floor_value: None,
        file_system_config_value: None,
        storage_value: None,
        members: Vec::new(),
    }
}

fn permit(epoch: u64, scope: Option<u64>) -> DfsMetadataReplicaIngestPermit {
    DfsMetadataReplicaIngestPermit::new_validated(
        file_system_id(),
        shard_id(),
        node("node_member_a"),
        epoch,
    )
    .with_floor_epoch_scope(scope)
}

fn backend(root: &sinomo_testdir::TestDir) -> LocalSobjMetadataBackend {
    crate::test_runtime::copy_native_genesis_data(root.path(), &shard_id().0);
    LocalSobjMetadataBackend::new(shard_id(), root.path()).unwrap()
}

fn fenced(result: Result<MetadataWriteLock>) -> bool {
    matches!(result, Err(MetadataBackendError::SourceEpochFenced { .. }))
}

fn proof(sequence: u64, digit: char) -> DfsMetadataPrefixProof {
    DfsMetadataPrefixProof::from_digest(
        sequence,
        format!(
            "dfs-metadata-prefix-v2:blake3:{}",
            digit.to_string().repeat(64)
        ),
    )
    .unwrap()
}

#[test]
fn a_promise_fences_the_read_epoch_on_every_ingest_and_acknowledgement_path_and_survives_restart() {
    let root = sinomo_testdir::TestDir::new("write-set-promise-fence");
    let mut backend = backend(&root);
    let id = KvRequestId::generate();
    let epoch_start = backend
        .promise_metadata_takeover_owned(&cut(3, 1), &id)
        .unwrap();
    assert_eq!(
        epoch_start.journal_sequence,
        backend.journal_last_sequence()
    );
    assert_eq!(
        backend.metadata_prefix_proof().unwrap(),
        epoch_start,
        "epoch start is the exact head"
    );
    // Repeating the same promise returns the same start; a different one is refused.
    assert_eq!(
        backend
            .promise_metadata_takeover_owned(&cut(3, 1), &id)
            .unwrap(),
        epoch_start
    );
    assert!(
        backend
            .promise_metadata_takeover_owned(&cut(3, 1), &KvRequestId::generate())
            .is_err()
    );
    // Journal ingest, authority probes and snapshot install share these gates.
    assert!(fenced(backend.begin_metadata_replica_ingest(
        &permit(3, Some(3)),
        &shard_id()
    )));
    assert!(fenced(backend.begin_metadata_replica_durable_ingest(
        &permit(3, Some(3)),
        &shard_id()
    )));
    drop(
        backend
            .begin_metadata_replica_ingest(&permit(4, Some(3)), &shard_id())
            .unwrap(),
    );
    // Acknowledgement floors from the read epoch, or without an epoch, are refused.
    let floor = |epoch| DfsMetadataPrefixAckFloor {
        shard_id: shard_id(),
        journal_sequence: epoch_start.journal_sequence,
        state_fingerprint: String::new(),
        durable_prefix_proof: epoch_start.clone(),
        authority_epoch: epoch,
    };
    for epoch in [Some(3), None] {
        assert!(matches!(
            backend.put_metadata_prefix_ack_floor(&floor(epoch)),
            Err(MetadataBackendError::SourceEpochFenced { .. })
        ));
    }
    backend
        .put_metadata_prefix_ack_floor(&floor(Some(4)))
        .unwrap();
    // The promise is durable: a restarted process keeps refusing.
    drop(backend);
    let mut reopened = self::backend(&root);
    assert!(fenced(reopened.begin_metadata_replica_ingest(
        &permit(3, Some(3)),
        &shard_id()
    )));
    let state = reopened.read_write_set_member_state().unwrap().unwrap();
    assert_eq!(state.accept_from_epoch, 4);
    assert_eq!(state.promise.unwrap().swap_request_id, id);
}

#[test]
fn a_rejected_swap_at_an_unchanged_epoch_releases_the_promise() {
    let root = sinomo_testdir::TestDir::new("write-set-promise-release");
    let mut backend = backend(&root);
    let id = KvRequestId::generate();
    backend
        .promise_metadata_takeover_owned(&cut(3, 1), &id)
        .unwrap();
    // Settling another request's promise is refused; repeating a settlement is a no-op.
    assert!(
        backend
            .settle_metadata_takeover_promise_owned(
                &KvRequestId::generate(),
                DfsMetadataTakeoverPromiseEnd::Released
            )
            .is_err()
    );
    backend
        .settle_metadata_takeover_promise_owned(&id, DfsMetadataTakeoverPromiseEnd::Released)
        .unwrap();
    backend
        .settle_metadata_takeover_promise_owned(&id, DfsMetadataTakeoverPromiseEnd::Released)
        .unwrap();
    drop(
        backend
            .begin_metadata_replica_ingest(&permit(3, Some(3)), &shard_id())
            .unwrap(),
    );
    // The released state carries a fence at the read epoch and no promise.
    let state = backend.read_write_set_member_state().unwrap().unwrap();
    assert_eq!(state.accept_from_epoch, 3);
    assert!(state.promise.is_none());
    backend.retire_write_set_member_fence_owned(3).unwrap();
    assert!(
        backend.read_write_set_member_state().unwrap().is_none(),
        "nothing left to keep"
    );
}

#[test]
fn an_applied_swap_or_a_later_epoch_keeps_the_fence_until_the_record_reaches_it() {
    let root = sinomo_testdir::TestDir::new("write-set-promise-kept");
    let mut backend = backend(&root);
    let id = KvRequestId::generate();
    backend
        .promise_metadata_takeover_owned(&cut(3, 1), &id)
        .unwrap();
    backend
        .settle_metadata_takeover_promise_owned(&id, DfsMetadataTakeoverPromiseEnd::Kept)
        .unwrap();
    assert!(fenced(backend.begin_metadata_replica_ingest(
        &permit(3, Some(3)),
        &shard_id()
    )));
    // The fence stays while the member's view is behind it.
    backend.retire_write_set_member_fence_owned(3).unwrap();
    assert!(fenced(backend.begin_metadata_replica_ingest(
        &permit(3, Some(3)),
        &shard_id()
    )));
    backend.retire_write_set_member_fence_owned(4).unwrap();
    assert!(backend.read_write_set_member_state().unwrap().is_none());
}

#[test]
fn the_member_state_survives_snapshot_install_and_fences_an_old_epoch_snapshot() {
    let native = native_snapshots::NativeSnapshots::new("native-member-state-snapshot");
    let root = sinomo_testdir::TestDir::new("write-set-promise-snapshot");
    let mut backend = backend(&root);
    // Backend/owner validation with real native captured epochs, original cut,
    // source image and complete catalog; not a general takeover availability test.
    backend
        .promise_metadata_takeover_owned(&native.before_cut, &KvRequestId::generate())
        .unwrap();
    let old_permit = native.permit(&native.before_context, &native.before_image);
    assert!(matches!(
        backend.replace_with_replicated_snapshot_with_replica_permit(
            &old_permit,
            shard_id(),
            native.before_image.materialized_through_sequence,
            native.before_image.clone()
        ),
        Err(MetadataBackendError::SourceEpochFenced { .. })
    ));
    let current_permit = native.permit(&native.current_context, &native.current_image);
    backend
        .replace_with_replicated_snapshot_with_replica_permit(
            &current_permit,
            shard_id(),
            native.current_image.materialized_through_sequence,
            native.current_image.clone(),
        )
        .unwrap();
    assert!(backend.write_set_member_state_path().exists());
    assert!(fenced(
        backend.begin_metadata_replica_ingest(&old_permit, &shard_id())
    ));
    drop(backend);
    native.runtime.close().unwrap();
}

#[test]
fn interrupted_snapshot_install_reopens_for_authenticated_repair_with_its_member_fence() {
    let native = native_snapshots::NativeSnapshots::new("native-snapshot-install-interruption");
    let current = native.permit(&native.current_context, &native.current_image);
    let old = native.permit(&native.before_context, &native.before_image);
    for point in ["after-archive", "after-snapshot"] {
        let root = sinomo_testdir::TestDir::new("snapshot-install-interrupted-root");
        let mut replica = backend(&root);
        replica.promise_metadata_takeover_owned(&native.before_cut, &KvRequestId::generate()).unwrap();
        METADATA_SNAPSHOT_INSTALL_FAULT.set(Some(point));
        assert!(matches!(replica.replace_with_replicated_snapshot_with_replica_permit(
            &current, shard_id(), native.current_image.materialized_through_sequence, native.current_image.clone()),
            Err(MetadataBackendError::Io { source, .. }) if source.kind() == std::io::ErrorKind::Interrupted));
        drop(replica);
        let open_error = LocalSobjMetadataBackend::new(shard_id(), root.path()).unwrap_err();
        assert!(crate::metadata_replication::metadata_backend_open_error_allows_snapshot_repair(&open_error),
            "interrupted snapshot at {point} must be repairable, not mistaken for unfinished migration: {open_error}");
        assert!(matches!(LocalSobjMetadataBackend::replace_unreadable_with_replicated_snapshot(
            &old, shard_id(), root.path(), shard_id(), native.before_image.materialized_through_sequence, native.before_image.clone()),
            Err(MetadataBackendError::SourceEpochFenced { .. })));
        let (repaired, _) = LocalSobjMetadataBackend::replace_unreadable_with_replicated_snapshot(
            &current, shard_id(), root.path(), shard_id(), native.current_image.materialized_through_sequence, native.current_image.clone()).unwrap();
        assert_eq!(repaired.metadata_prefix_proof().unwrap(), native.current_image.prefix_proof);
        drop(repaired);
        let reopened = LocalSobjMetadataBackend::new(shard_id(), root.path()).unwrap();
        assert_eq!(reopened.metadata_prefix_proof().unwrap(), native.current_image.prefix_proof);
        drop(reopened);
    }
    native.runtime.close().unwrap();
}

#[test]
fn unsealed_snapshot_repair_requires_its_exact_native_generation() {
    let native = native_snapshots::NativeSnapshots::new("native-unsealed-snapshot-repair");
    let root = sinomo_testdir::TestDir::new("retained-unsealed-snapshot");
    let mut replica = backend(&root);
    let permit = native.permit(&native.current_context, &native.current_image);
    replica.replace_with_replicated_snapshot_with_replica_permit(
        &permit, shard_id(), native.current_image.materialized_through_sequence, native.current_image.clone()).unwrap();
    drop(replica);
    let active = crate::storage_generation::read_generation(root.path()).unwrap();
    // The earlier snapshot producer wrote exactly this unsealed header before
    // activation. It is unreadable DATA, never a fixture-created serving permit.
    let mut pending = active.clone();
    pending.state = crate::storage_generation::GenerationState::Staged { seal: None };
    crate::storage_generation::write_generation(root.path(), &pending).unwrap();
    let open_error = LocalSobjMetadataBackend::new(shard_id(), root.path()).unwrap_err();
    assert!(!crate::metadata_replication::metadata_backend_open_error_allows_snapshot_repair(&open_error));
    assert!(crate::metadata_replication::metadata_snapshot_open_error_allows_repair(
        &open_error, root.path(), &permit, &native.current_image).unwrap());
    pending.identity.generation.push_str("-foreign");
    crate::storage_generation::write_generation(root.path(), &pending).unwrap();
    assert!(!crate::metadata_replication::metadata_snapshot_open_error_allows_repair(
        &open_error, root.path(), &permit, &native.current_image).unwrap());
    pending.identity = active.identity;
    pending.state = crate::storage_generation::GenerationState::Staged {
        seal: Some(crate::DfsMetadataGenerationSeal {
            prefix_proof: native.current_image.prefix_proof.clone(),
            state_fingerprint: native.current_image.semantic_state_fingerprint.clone(),
            file_fingerprint: native.current_image.stream_digest.clone(),
        }),
    };
    crate::storage_generation::write_generation(root.path(), &pending).unwrap();
    assert!(!crate::metadata_replication::metadata_snapshot_open_error_allows_repair(
        &open_error, root.path(), &permit, &native.current_image).unwrap());
    native.runtime.close().unwrap();
}

#[test]
fn a_pending_add_needs_whole_journal_evidence_and_ends_when_the_record_moves() {
    let root = sinomo_testdir::TestDir::new("write-set-pending-add");
    let mut backend = backend(&root);
    let head = backend.metadata_prefix_proof().unwrap();
    let candidate = node("node_member_d");
    let evidence =
        |epoch, generation, durable_prefix: &DfsMetadataPrefixProof| DfsMetadataMemberAckView {
            epoch,
            write_set_generation: generation,
            durable_prefix: durable_prefix.clone(),
        };
    // Evidence from another epoch, another generation or a different prefix
    // describes some other copy than the member's current one.
    for wrong in [
        evidence(2, 1, &head),
        evidence(3, 2, &head),
        evidence(3, 1, &proof(9, 'f')),
    ] {
        assert!(
            backend
                .record_pending_write_set_add_owned(&cut(3, 1), &candidate, &wrong)
                .is_err()
        );
    }
    backend
        .record_pending_write_set_add_owned(&cut(3, 1), &candidate, &evidence(3, 1, &head))
        .unwrap();
    assert!(
        backend
            .record_pending_write_set_add_owned(&cut(3, 1), &candidate, &evidence(3, 1, &head))
            .is_err(),
        "one pending add per member"
    );
    // The requirement is durable across a restart.
    drop(backend);
    let mut backend = self::backend(&root);
    let state = backend.read_write_set_member_state().unwrap().unwrap();
    assert_eq!(state.pending_adds.len(), 1);
    assert_eq!(
        state.accept_from_epoch, 0,
        "a primary's pending add fences nothing"
    );
    // It stays while the record equals the add's cut, and ends once it moved.
    assert!(
        backend
            .release_pending_write_set_adds_owned(&cut(3, 1))
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        backend
            .release_pending_write_set_adds_owned(&cut(3, 2))
            .unwrap(),
        vec![candidate]
    );
    assert!(backend.read_write_set_member_state().unwrap().is_none());
}

#[test]
fn floors_bind_only_at_their_own_epoch_on_a_write_set_shard() {
    use crate::metadata_records::{
        DfsMetadataAckFloorRelation as Relation, dfs_metadata_ack_floor_relation as relation,
    };
    // Under the all-member rule every floor binds, as before.
    assert_eq!(relation(None, Some(9), 1), Relation::Binds);
    // A floor without an epoch belongs to the switch epoch.
    assert_eq!(relation(Some(3), None, 3), Relation::Binds);
    assert_eq!(relation(Some(3), None, 4), Relation::EarlierEpoch);
    assert_eq!(relation(Some(3), Some(5), 4), Relation::LaterEpoch);
    let root = sinomo_testdir::TestDir::new("write-set-floor-merge");
    let floor = |sequence, epoch| DfsMetadataPrefixAckFloor {
        shard_id: shard_id(),
        journal_sequence: sequence,
        state_fingerprint: String::new(),
        durable_prefix_proof: proof(sequence, 'a'),
        authority_epoch: epoch,
    };
    let put = |value| metadata_cursor::put_metadata_prefix_ack_floor(root.path(), &value).unwrap();
    let current = || {
        metadata_cursor::metadata_prefix_ack_floor(root.path(), &shard_id())
            .unwrap()
            .unwrap()
    };
    put(floor(5, None));
    // A later epoch replaces a higher sequence; an earlier epoch never does.
    put(floor(2, Some(4)));
    assert_eq!(current(), floor(2, Some(4)));
    put(floor(9, Some(3)));
    assert_eq!(current(), floor(2, Some(4)));
    put(floor(1, Some(4)));
    assert_eq!(current(), floor(2, Some(4)));
}

#[test]
fn an_earlier_epoch_floor_does_not_block_a_new_epochs_snapshot_and_is_replaced_before_it() {
    let native = native_snapshots::NativeSnapshots::new("native-member-floor-snapshot");
    let sequence = native.current_image.materialized_through_sequence;
    // Deliberately high stale floor DATA is the negative component input. Its
    // epoch is captured from a real earlier assignment; current snapshot
    // permission and image come from the real later native owner.
    let stale_floor = DfsMetadataPrefixAckFloor {
        shard_id: shard_id(),
        journal_sequence: sequence + 5,
        state_fingerprint: String::new(),
        durable_prefix_proof: proof(sequence + 5, 'b'),
        authority_epoch: Some(native.before_context.shard.epoch),
    };
    let legacy_root = sinomo_testdir::TestDir::new("write-set-floor-snapshot-legacy");
    let mut legacy = backend(&legacy_root);
    metadata_cursor::put_metadata_prefix_ack_floor(legacy_root.path(), &stale_floor).unwrap();
    let legacy_permit = native.permit(&native.legacy_context, &native.before_image);
    assert!(
        legacy
            .replace_with_replicated_snapshot_with_replica_permit(
                &legacy_permit,
                shard_id(),
                native.before_image.materialized_through_sequence,
                native.before_image.clone()
            )
            .is_err()
    );
    let root = sinomo_testdir::TestDir::new("write-set-floor-snapshot");
    let mut backend = backend(&root);
    metadata_cursor::put_metadata_prefix_ack_floor(root.path(), &stale_floor).unwrap();
    let oldest_permit = native.permit(&native.oldest_context, &native.before_image);
    assert!(matches!(
        backend.replace_with_replicated_snapshot_with_replica_permit(
            &oldest_permit,
            shard_id(),
            native.before_image.materialized_through_sequence,
            native.before_image.clone()
        ),
        Err(MetadataBackendError::SourceEpochFenced { .. })
    ));
    let current_permit = native.permit(&native.current_context, &native.current_image);
    backend
        .replace_with_replicated_snapshot_with_replica_permit(
            &current_permit,
            shard_id(),
            sequence,
            native.current_image.clone(),
        )
        .unwrap();
    let floor = metadata_cursor::metadata_prefix_ack_floor(root.path(), &shard_id())
        .unwrap()
        .unwrap();
    assert_eq!(
        floor.authority_epoch,
        Some(native.current_context.shard.epoch)
    );
    assert_eq!(floor.journal_sequence, sequence);
    drop(backend);
    self::backend(&root);
    drop(legacy);
    native.runtime.close().unwrap();
}

#[test]
fn shards_on_the_all_member_rule_keep_their_floor_and_reply_encoding() {
    let floor = DfsMetadataPrefixAckFloor {
        shard_id: shard_id(),
        journal_sequence: 1,
        state_fingerprint: String::new(),
        durable_prefix_proof: proof(1, 'c'),
        authority_epoch: None,
    };
    let encoded =
        String::from_utf8_lossy(&sinomo_core::kv::encode_kv_value(&floor).unwrap()).to_string();
    assert!(
        !encoded.contains("authority_epoch"),
        "an unscoped floor keeps its exact encoding"
    );
    let reply = crate::DfsMetadataJournalBatchResponse {
        request_id: "reply".into(),
        shard_id: shard_id(),
        next_after_sequence: 1,
        applied_entries: 0,
        verified_prefix_only: false,
        verified_authority_probe_id: None,
        result: crate::DfsMetadataJournalBatchResult::Applied,
        member_view: None,
        member_record: None,
    };
    let encoded =
        String::from_utf8_lossy(&sinomo_core::kv::encode_kv_value(&reply).unwrap()).to_string();
    assert!(!encoded.contains("member_view"));
}

fn handoff_marker(retiring_epoch: u64, activated_epoch: Option<u64>) -> crate::DfsMetadataShardWriteFreeze {
    crate::DfsMetadataShardWriteFreeze {
        handoff_id: "handoff-a".into(),
        retiring_epoch,
        activated_epoch,
        journal_sequence: 1,
        state_fingerprint: proof(1, 'a').digest,
        rebind_prefix_proof: proof(1, 'a'),
    }
}

#[test]
fn a_freeze_marker_the_write_set_record_moved_past_is_obsolete() {
    // An activated marker below the record's epoch fences nothing.
    assert!(handoff_marker(3, Some(4)).is_obsolete_for_write_set_record(5, None));
    assert!(!handoff_marker(3, Some(4)).is_obsolete_for_write_set_record(4, None));
    // A pending marker is obsolete once the record moved past its retiring
    // epoch without completing its handoff.
    assert!(handoff_marker(3, None).is_obsolete_for_write_set_record(4, None));
    assert!(handoff_marker(3, None).is_obsolete_for_write_set_record(4, Some("handoff-b")));
    assert!(!handoff_marker(3, None).is_obsolete_for_write_set_record(4, Some("handoff-a")));
    assert!(!handoff_marker(3, None).is_obsolete_for_write_set_record(3, None));
}

#[test]
fn an_obsolete_freeze_marker_is_removed_and_a_current_one_is_kept() {
    let root = sinomo_testdir::TestDir::new("write-set-obsolete-marker");
    let mut backend = backend(&root);
    backend.freeze_for_membership_handoff("handoff-a", 3).unwrap();
    assert!(matches!(
        backend.begin_metadata_write(),
        Err(MetadataBackendError::MembershipHandoffFrozen { .. })
    ));
    // Still at the retiring epoch, or completed: kept.
    assert!(!backend.clear_obsolete_membership_handoff_freeze(3, None).unwrap());
    assert!(
        !backend
            .clear_obsolete_membership_handoff_freeze(4, Some("handoff-a"))
            .unwrap()
    );
    assert!(backend.membership_handoff_write_freeze().unwrap().is_some());
    // The record moved past it without completing it: removed, and the
    // writer is no longer fenced.
    assert!(backend.clear_obsolete_membership_handoff_freeze(4, None).unwrap());
    assert!(backend.membership_handoff_write_freeze().unwrap().is_none());
    assert!(backend.begin_metadata_write().is_ok());
    // An activated marker at the record's epoch is current; below it, it is
    // obsolete, and the instance is unbound from the earlier epoch.
    backend.freeze_for_membership_handoff("handoff-b", 4).unwrap();
    assert!(
        backend
            .mark_membership_handoff_write_freeze_activated("handoff-b", 5)
            .unwrap()
    );
    backend.instance_authority_epoch = Some(5);
    assert!(!backend.clear_obsolete_membership_handoff_freeze(5, None).unwrap());
    assert_eq!(backend.instance_authority_epoch, Some(5));
    assert!(backend.clear_obsolete_membership_handoff_freeze(6, None).unwrap());
    assert_eq!(backend.instance_authority_epoch, None);
    assert!(backend.membership_handoff_write_freeze().unwrap().is_none());
    // A restart finds no marker.
    drop(backend);
    assert!(
        self::backend(&root)
            .membership_handoff_write_freeze()
            .unwrap()
            .is_none()
    );
}

#[test]
fn an_epoch_tagged_authority_floor_may_name_sequence_zero() {
    let root = sinomo_testdir::TestDir::new("write-set-zero-authority-floor");
    let floor = |sequence, epoch| crate::DfsMetadataAuthorityAckFloor {
        shard_id: shard_id(),
        journal_sequence: sequence,
        durable_prefix_proof: proof(sequence, 'd'),
        authority_epoch: epoch,
    };
    let put = |value| metadata_cursor::put_metadata_authority_ack_floor(root.path(), &value);
    put(floor(3, Some(3))).unwrap();
    // Without an epoch, sequence zero names nothing acknowledged.
    assert!(put(floor(0, None)).is_err());
    // With one it starts a later epoch and displaces the earlier floor.
    put(floor(0, Some(4))).unwrap();
    assert_eq!(
        metadata_cursor::metadata_authority_ack_floor(root.path(), &shard_id()).unwrap(),
        Some(floor(0, Some(4)))
    );
}

#[test]
fn a_later_epoch_snapshot_below_an_earlier_authority_floor_replaces_it() {
    // A native store is never empty, so the snapshot is the current epoch's
    // actual image; the earlier epoch's floor lies above its sequence, as an
    // empty epoch start lies below any earlier floor.
    let native = native_snapshots::NativeSnapshots::new("native-later-epoch-floor");
    let image = native.current_image.clone();
    let sequence = image.descriptor().materialized_through_sequence;
    let earlier = native.before_context.shard.epoch;
    let current = native.current_context.shard.epoch;
    assert!(earlier < current);
    let root = sinomo_testdir::TestDir::new("write-set-later-epoch-snapshot");
    let mut backend = backend(&root);
    metadata_cursor::put_metadata_authority_ack_floor(
        root.path(),
        &crate::DfsMetadataAuthorityAckFloor {
            shard_id: shard_id(),
            journal_sequence: sequence + 2,
            durable_prefix_proof: proof(sequence + 2, 'b'),
            authority_epoch: Some(earlier),
        },
    )
    .unwrap();
    backend
        .replace_with_replicated_snapshot_with_replica_permit(
            &native.permit(&native.current_context, &image),
            shard_id(),
            sequence,
            image,
        )
        .unwrap();
    let floor = metadata_cursor::metadata_authority_ack_floor(root.path(), &shard_id())
        .unwrap()
        .unwrap();
    assert_eq!(floor.authority_epoch, Some(current));
    assert_eq!(floor.journal_sequence, sequence);
    // The cold-open check compares the installed head with the current
    // epoch's floor, not the earlier one above it.
    drop(backend);
    LocalSobjMetadataBackend::new(shard_id(), root.path()).unwrap();
    native.runtime.close().unwrap();
}

#[test]
fn a_later_epochs_ingest_removes_a_pending_marker_only_on_a_write_set_shard() {
    let root = sinomo_testdir::TestDir::new("write-set-ingest-clears-marker");
    let mut backend = backend(&root);
    backend.freeze_for_membership_handoff("abandoned", 3).unwrap();
    let frozen = |result: Result<MetadataWriteLock>| {
        matches!(result, Err(MetadataBackendError::MembershipHandoffFrozen { .. }))
    };
    // The all-member rule keeps refusing, and so does the retiring epoch.
    assert!(frozen(backend.begin_metadata_replica_ingest(&permit(4, None), &shard_id())));
    assert!(frozen(backend.begin_metadata_replica_ingest(&permit(3, Some(3)), &shard_id())));
    assert!(backend.membership_handoff_write_freeze().unwrap().is_some());
    // A later epoch's primary on a write-set shard: the handoff can no longer
    // complete, so the marker goes and the ingest proceeds.
    assert!(
        backend
            .begin_metadata_replica_ingest(&permit(4, Some(3)), &shard_id())
            .is_ok()
    );
    assert!(backend.membership_handoff_write_freeze().unwrap().is_none());
}

#[test]
fn alignment_confirms_a_matching_epoch_start_and_refuses_a_different_one() {
    let root = sinomo_testdir::TestDir::new("write-set-align-decision");
    let mut backend = backend(&root);
    // A native store starts at its genesis head, not at sequence zero.
    let start = backend.metadata_prefix_proof().unwrap();
    let at = start.journal_sequence;
    let permit = permit(4, Some(3));
    // A different digest at this member's head is a divergence.
    assert!(matches!(
        backend.align_replica_to_prefix_owned(&permit, &proof(at, 'e'), &proof(at, 'e')),
        Err(MetadataBackendError::Rejected {
            code: crate::DfsMetadataRejectionCode::ReplicaDiverged,
            ..
        })
    ));
    // An epoch start beyond this member's journal is for the primary to check.
    assert!(matches!(
        backend
            .align_replica_to_prefix_owned(&permit, &proof(at + 3, 'f'), &proof(at + 3, 'f'))
            .unwrap(),
        crate::DfsMetadataReplicaAlignment::Behind { .. }
    ));
    // A matching epoch start at this member's head writes both floors there,
    // and aligning again is a no-op.
    for _ in 0..2 {
        assert_eq!(
            backend
                .align_replica_to_prefix_owned(&permit, &start, &start)
                .unwrap(),
            crate::DfsMetadataReplicaAlignment::Aligned {
                head: start.clone(),
                dropped: 0
            }
        );
    }
    let authority = metadata_cursor::metadata_authority_ack_floor(root.path(), &shard_id())
        .unwrap()
        .unwrap();
    assert_eq!(
        (authority.journal_sequence, authority.authority_epoch),
        (at, Some(4))
    );
    // An unswitched permit never aligns.
    assert!(
        backend
            .align_replica_to_prefix_owned(&self::permit(4, None), &start, &start)
            .is_err()
    );
    drop(backend);
    self::backend(&root);
}
