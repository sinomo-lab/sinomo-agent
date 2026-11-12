//! Durable member state and epoch-scoped acknowledgement floors of a
//! write-set shard, on real shard directories.

use super::*;
use crate::{
    DfsMetadataMemberAckView, DfsMetadataShardRecordCut, DfsMetadataShardWriteSet,
    DfsMetadataShardWriteSetChange, DfsMetadataShardWriteSetChangeKind,
    DfsMetadataTakeoverPromiseEnd,
};
use sinomo_core::kv::KvRequestId;

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
    let root = sinomo_testdir::TestDir::new("write-set-promise-snapshot");
    let source_root = sinomo_testdir::TestDir::new("write-set-promise-snapshot-source");
    let image = || {
        LocalSobjMetadataBackend::new(shard_id(), source_root.path())
            .unwrap()
            .export_snapshot_image()
            .unwrap()
    };
    let mut backend = backend(&root);
    backend
        .promise_metadata_takeover_owned(&cut(3, 1), &KvRequestId::generate())
        .unwrap();
    let image_for_old_epoch = image();
    let sequence = image_for_old_epoch
        .descriptor()
        .materialized_through_sequence;
    assert!(matches!(
        backend.replace_with_replicated_snapshot_with_replica_permit(
            &permit(3, Some(3)),
            shard_id(),
            sequence,
            image_for_old_epoch,
        ),
        Err(MetadataBackendError::SourceEpochFenced { .. })
    ));
    backend
        .replace_with_replicated_snapshot_with_replica_permit(
            &permit(4, Some(3)),
            shard_id(),
            sequence,
            image(),
        )
        .unwrap();
    assert!(backend.write_set_member_state_path().exists());
    assert!(fenced(backend.begin_metadata_replica_ingest(
        &permit(3, Some(3)),
        &shard_id()
    )));
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
    let source_root = sinomo_testdir::TestDir::new("write-set-floor-snapshot-source");
    let image = || {
        LocalSobjMetadataBackend::new(shard_id(), source_root.path())
            .unwrap()
            .export_snapshot_image()
            .unwrap()
    };
    let sequence = image().descriptor().materialized_through_sequence;
    let stale_floor = DfsMetadataPrefixAckFloor {
        shard_id: shard_id(),
        journal_sequence: sequence + 5,
        state_fingerprint: String::new(),
        durable_prefix_proof: proof(sequence + 5, 'b'),
        authority_epoch: Some(3),
    };
    // Under the all-member rule the floor still binds.
    let legacy_root = sinomo_testdir::TestDir::new("write-set-floor-snapshot-legacy");
    let mut legacy = backend(&legacy_root);
    metadata_cursor::put_metadata_prefix_ack_floor(legacy_root.path(), &stale_floor).unwrap();
    assert!(
        legacy
            .replace_with_replicated_snapshot_with_replica_permit(
                &permit(4, None),
                shard_id(),
                sequence,
                image()
            )
            .is_err()
    );
    // On a write-set shard an epoch-3 floor does not bind an epoch-4 source,
    // and an epoch-2 source is refused as stale.
    let root = sinomo_testdir::TestDir::new("write-set-floor-snapshot");
    let mut backend = backend(&root);
    metadata_cursor::put_metadata_prefix_ack_floor(root.path(), &stale_floor).unwrap();
    assert!(matches!(
        backend.replace_with_replicated_snapshot_with_replica_permit(
            &permit(2, Some(2)),
            shard_id(),
            sequence,
            image()
        ),
        Err(MetadataBackendError::SourceEpochFenced { .. })
    ));
    backend
        .replace_with_replicated_snapshot_with_replica_permit(
            &permit(4, Some(3)),
            shard_id(),
            sequence,
            image(),
        )
        .unwrap();
    let floor = metadata_cursor::metadata_prefix_ack_floor(root.path(), &shard_id())
        .unwrap()
        .unwrap();
    assert_eq!(floor.authority_epoch, Some(4));
    assert_eq!(floor.journal_sequence, sequence);
    // A restart finds floors consistent with the installed state.
    drop(backend);
    self::backend(&root);
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
    };
    let encoded =
        String::from_utf8_lossy(&sinomo_core::kv::encode_kv_value(&reply).unwrap()).to_string();
    assert!(!encoded.contains("member_view"));
}
