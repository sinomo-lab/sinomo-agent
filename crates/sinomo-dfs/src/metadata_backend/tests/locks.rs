//! Locks regression tests.

use super::*;

#[test]
fn advisory_lock_state_is_journaled_idempotent_and_replica_replayable() {
    let leader_root = temp_root("advisory_lock_state_leader");
    let follower_root = temp_root("advisory_lock_state_follower");
    let snapshot_root = temp_root("advisory_lock_state_snapshot");
    let shard_id = DfsMetadataShardId::new("root");
    let mut leader = LocalSobjMetadataBackend::new(shard_id.clone(), &leader_root).expect("leader");
    leader.put_file_system(&file_system()).expect("filesystem");
    let object = object(version("lock-v1"));
    leader.put_object(&object).expect("object");
    let command = DfsAdvisoryLockCommand::Acquire {
        request_id: DfsAdvisoryLockRequestId::new("acquire-a"),
        session_id: DfsAdvisoryLockSessionId::new("session-a"),
        mode: DfsAdvisoryLockMode::Exclusive,
        blocking: false,
        process_id: Some(42),
    };
    let context = DfsAdvisoryLockTransitionContext {
        authority_epoch: 3,
        now_unix_nanos: 100,
        lease_duration_nanos: 1_000,
        max_clock_skew_nanos: 0,
    };
    let applied = leader
        .apply_advisory_lock_command(&object.file_system_id, &object.object_id, context, &command)
        .expect("acquire");
    assert_eq!(applied.shard_id, shard_id);
    assert_eq!(applied.durable_through_sequence, 3);
    assert!(applied.journal_entry.is_some());
    assert!(matches!(
        &applied.outcome,
        DfsAdvisoryLockOutcome::Acquired { .. }
    ));

    let retry = leader
        .apply_advisory_lock_command(&object.file_system_id, &object.object_id, context, &command)
        .expect("idempotent retry");
    assert_eq!(retry.outcome, applied.outcome);
    assert!(retry.journal_entry.is_none());
    assert_eq!(retry.durable_through_sequence, 3);

    let expected_state = leader
        .get_advisory_lock_state(&object.file_system_id, &object.object_id)
        .expect("state")
        .expect("lock aggregate");
    drop(leader);
    let leader =
        LocalSobjMetadataBackend::new(shard_id.clone(), &leader_root).expect("reopen leader");
    assert_eq!(
        leader
            .get_advisory_lock_state(&object.file_system_id, &object.object_id)
            .expect("reopened state"),
        Some(expected_state.clone())
    );

    let mut follower =
        LocalSobjMetadataBackend::new(shard_id.clone(), &follower_root).expect("follower");
    let batch = leader
        .journal_batch_since_limited(0, 16, usize::MAX)
        .expect("journal batch")
        .expect("non-empty journal batch");
    follower.apply_journal_batch(&batch).expect("replay");
    assert_eq!(
        follower
            .get_advisory_lock_state(&object.file_system_id, &object.object_id)
            .expect("follower state"),
        Some(expected_state)
    );

    let mut snapshot_replica =
        LocalSobjMetadataBackend::new(shard_id.clone(), &snapshot_root).expect("snapshot");
    snapshot_replica
        .replace_with_replicated_snapshot(
            shard_id,
            leader.journal_last_sequence(),
            leader.export_table_snapshot(),
        )
        .expect("install snapshot");
    assert_eq!(
        snapshot_replica
            .get_advisory_lock_state(&object.file_system_id, &object.object_id)
            .expect("snapshot state"),
        follower
            .get_advisory_lock_state(&object.file_system_id, &object.object_id)
            .expect("follower state")
    );

    cleanup(&leader_root);
    cleanup(&follower_root);
    cleanup(&snapshot_root);
}
