use sinomo_kv::*;
use sinomo_node::NodeId;

fn id(name: &str) -> NodeId {
    NodeId::from_legacy_named(name).unwrap()
}

fn put(key: &str) -> KvCommand {
    KvCommand::Op(KvOp::Put {
        key: key.into(),
        value: b"value".to_vec(),
        lease_id: None,
    })
}

fn append(
    term: u64,
    leader: &str,
    prev: u64,
    prev_term: u64,
    entries: Vec<KvLogEntry>,
    commit: u64,
) -> RaftAppendEntriesRequest {
    RaftAppendEntriesRequest {
        term,
        leader_id: id(leader),
        prev_log_index: prev,
        prev_log_term: prev_term,
        entries,
        leader_commit: commit,
    }
}

fn entry(index: u64, term: u64, key: &str) -> KvLogEntry {
    KvLogEntry {
        index,
        term,
        request_id: KvRequestId::generate(),
        command: put(key),
        revision: KvRevision::ZERO,
    }
}

fn committed_snapshot(term: u64, entries: Vec<KvLogEntry>) -> RaftSnapshot {
    let root = sinomo_testdir::TestDir::new("sinomo-kv-snapshot-donor");
    let mut donor = DurableRaftKvNode::open(id("donor"), KvWalPaths::new(root.path())).unwrap();
    let commit = entries.last().unwrap().index;
    donor
        .append_entries(append(term, "leader", 0, 0, entries, commit))
        .unwrap();
    donor.save_snapshot().unwrap()
}

#[test]
fn snapshot_with_conflicting_boundary_must_discard_suffix() {
    let root = sinomo_testdir::TestDir::new("sinomo-kv-conflicting-snapshot");
    let paths = KvWalPaths::new(root.path());
    let mut node = DurableRaftKvNode::open(id("follower"), paths).unwrap();
    node.append_entries(append(
        1,
        "old-leader",
        0,
        0,
        vec![
            entry(1, 1, "old/1"),
            entry(2, 1, "old/2"),
            entry(3, 1, "old/3"),
        ],
        0,
    ))
    .unwrap();
    let snapshot = committed_snapshot(2, vec![entry(1, 2, "new/1"), entry(2, 2, "new/2")]);
    let outcome = node
        .apply_append_entries_with_optional_snapshot(
            Some(snapshot),
            append(2, "new-leader", 2, 2, vec![], 2),
        )
        .unwrap();
    assert!(outcome.response.success);
    assert_eq!(
        node.last_log_term(),
        2,
        "snapshot term must not be superseded by an incompatible old suffix"
    );
    assert_eq!(node.last_log_index(), 2);
}

#[test]
fn installed_snapshot_must_reject_stale_candidate() {
    let root = sinomo_testdir::TestDir::new("sinomo-kv-snapshot-vote");
    let mut node = DurableRaftKvNode::open(id("follower"), KvWalPaths::new(root.path())).unwrap();
    node.append_entries(append(
        1,
        "old-leader",
        0,
        0,
        vec![
            entry(1, 1, "old/1"),
            entry(2, 1, "old/2"),
            entry(3, 1, "old/3"),
        ],
        0,
    ))
    .unwrap();
    let snapshot = committed_snapshot(2, vec![entry(1, 2, "new/1"), entry(2, 2, "new/2")]);
    node.apply_append_entries_with_optional_snapshot(
        Some(snapshot),
        append(2, "new-leader", 2, 2, vec![], 2),
    )
    .unwrap();
    let vote = node
        .request_vote(RaftRequestVoteRequest {
            term: 3,
            candidate_id: id("stale-candidate"),
            last_log_index: 4,
            last_log_term: 1,
        })
        .unwrap();
    assert!(
        !vote.vote_granted,
        "candidate missing the committed term-2 snapshot must not receive a vote"
    );
}

#[test]
fn crash_after_snapshot_write_must_preserve_log_floor() {
    let root = sinomo_testdir::TestDir::new("sinomo-kv-snapshot-crash");
    let paths = KvWalPaths::new(root.path());
    let mut node = DurableRaftKvNode::open(id("follower"), paths.clone()).unwrap();
    let shared = entry(1, 2, "shared/1");
    node.append_entries(append(2, "leader", 0, 0, vec![shared.clone()], 1))
        .unwrap();
    drop(node);
    let snapshot = committed_snapshot(
        2,
        vec![
            shared,
            entry(2, 2, "new/2"),
            entry(3, 2, "new/3"),
            entry(4, 2, "new/4"),
            entry(5, 2, "new/5"),
        ],
    );
    // Exact crash boundary: restore_snapshot has persisted the new snapshot,
    // but has not yet rewritten the old WAL or hard state.
    std::fs::write(&paths.snapshot, encode_kv_value(&snapshot).unwrap()).unwrap();
    let mut reopened = DurableRaftKvNode::open(id("follower"), paths).unwrap();
    assert_eq!(reopened.applied_index(), 5);
    assert_eq!(reopened.hard_state().commit_index, 5);
    assert_eq!(reopened.last_log_index(), 5);
    assert_eq!(reopened.last_log_term(), 2);
    let response = reopened.append_entries(append(2, "leader", 5, 2, vec![entry(6, 2, "next")], 6));
    assert!(
        response.is_ok(),
        "follower must resume at snapshot index + 1: {response:?}"
    );
}

#[test]
fn rejected_snapshot_must_not_replace_recoverable_snapshot() {
    let root = sinomo_testdir::TestDir::new("sinomo-kv-invalid-snapshot");
    let paths = KvWalPaths::new(root.path());
    let mut node = DurableRaftKvNode::open(id("follower"), paths.clone()).unwrap();
    node.append_entries(append(1, "leader", 0, 0, vec![entry(1, 1, "committed")], 1))
        .unwrap();
    let mut invalid = node.save_snapshot().unwrap();
    let original_snapshot = std::fs::read(&paths.snapshot).unwrap();
    let original_wal = std::fs::read(&paths.wal).unwrap();
    let original_hard_state = std::fs::read(&paths.hard_state).unwrap();
    invalid.last_included_index = 2;
    let mut duplicate = invalid.kv.committed_requests[0].clone();
    duplicate.log_index += 1;
    std::sync::Arc::make_mut(&mut invalid.kv)
        .committed_requests
        .push(duplicate);
    assert!(
        node.apply_append_entries_with_optional_snapshot(
            Some(invalid),
            append(2, "leader", 2, 1, vec![], 2)
        )
        .is_err()
    );
    drop(node);
    assert_eq!(std::fs::read(&paths.snapshot).unwrap(), original_snapshot);
    assert_eq!(std::fs::read(&paths.wal).unwrap(), original_wal);
    assert_eq!(
        std::fs::read(&paths.hard_state).unwrap(),
        original_hard_state
    );
    let reopened = DurableRaftKvNode::open(id("follower"), paths);
    assert!(
        reopened.is_ok(),
        "rejected snapshot must leave a recoverable node: {:?}",
        reopened.err()
    );
}

#[test]
fn heartbeat_must_not_commit_unmatched_suffix() {
    let root = sinomo_testdir::TestDir::new("sinomo-kv-heartbeat");
    let mut node = DurableRaftKvNode::open(id("follower"), KvWalPaths::new(root.path())).unwrap();
    node.append_entries(append(
        1,
        "old-leader",
        0,
        0,
        vec![entry(1, 1, "shared"), entry(2, 1, "uncommitted")],
        1,
    ))
    .unwrap();
    let response = node
        .append_entries(append(2, "new-leader", 1, 1, vec![], 2))
        .unwrap();
    assert_eq!(response.match_index, 1);
    assert_eq!(
        node.applied_index(),
        1,
        "heartbeat must only commit through its matching prefix"
    );
}

#[test]
fn crash_after_snapshot_write_discards_conflicting_wal_suffix() {
    let root = sinomo_testdir::TestDir::new("sinomo-kv-snapshot-conflicting-wal");
    let paths = KvWalPaths::new(root.path());
    let mut node = DurableRaftKvNode::open(id("follower"), paths.clone()).unwrap();
    node.append_entries(append(
        1,
        "old-leader",
        0,
        0,
        vec![
            entry(1, 1, "old/1"),
            entry(2, 1, "old/2"),
            entry(3, 1, "old/3"),
        ],
        0,
    ))
    .unwrap();
    drop(node);
    let snapshot = committed_snapshot(2, vec![entry(1, 2, "new/1"), entry(2, 2, "new/2")]);
    std::fs::write(&paths.snapshot, encode_kv_value(&snapshot).unwrap()).unwrap();

    for _ in 0..2 {
        let mut reopened = DurableRaftKvNode::open(id("follower"), paths.clone()).unwrap();
        assert_eq!(reopened.last_log_index(), 2);
        assert_eq!(reopened.last_log_term(), 2);
        assert_eq!(reopened.hard_state().commit_index, 2);
        assert!(reopened.log().is_empty());
        let vote = reopened
            .request_vote(RaftRequestVoteRequest {
                term: 3,
                candidate_id: id("stale-candidate"),
                last_log_index: 4,
                last_log_term: 1,
            })
            .unwrap();
        assert!(!vote.vote_granted);
    }
}

#[test]
fn matching_snapshot_boundary_preserves_pending_suffix_across_restart() {
    for crash_before_wal_rewrite in [false, true] {
        let root = sinomo_testdir::TestDir::new("sinomo-kv-snapshot-matching-suffix");
        let paths = KvWalPaths::new(root.path());
        let first = entry(1, 1, "shared/1");
        let boundary = entry(2, 2, "shared/2");
        let pending = entry(3, 2, "pending/3");
        let snapshot = committed_snapshot(2, vec![first.clone(), boundary.clone()]);
        let mut node = DurableRaftKvNode::open(id("follower"), paths.clone()).unwrap();
        node.append_entries(append(
            2,
            "leader",
            0,
            0,
            vec![first, boundary, pending.clone()],
            1,
        ))
        .unwrap();
        if crash_before_wal_rewrite {
            drop(node);
            std::fs::write(&paths.snapshot, encode_kv_value(&snapshot).unwrap()).unwrap();
        } else {
            let outcome = node
                .apply_append_entries_with_optional_snapshot(
                    Some(snapshot),
                    append(2, "leader", 2, 2, vec![], 2),
                )
                .unwrap();
            assert!(outcome.response.success);
            assert_eq!(outcome.response.match_index, 2);
            drop(node);
        }
        let mut reopened = DurableRaftKvNode::open(id("follower"), paths.clone()).unwrap();
        assert_eq!(reopened.applied_index(), 2);
        assert_eq!(reopened.last_log_index(), 3);
        assert_eq!(
            reopened.log_entry_at(3).unwrap().request_id,
            pending.request_id
        );
        assert!(reopened.store().get("pending/3").is_none());
        reopened
            .append_entries(append(2, "leader", 2, 2, vec![pending.clone()], 3))
            .unwrap();
        assert_eq!(reopened.store().get("pending/3").unwrap().value, b"value");
        drop(reopened);
        let recovered = DurableRaftKvNode::open(id("follower"), paths).unwrap();
        assert_eq!(recovered.applied_index(), 3);
        assert_eq!(
            recovered
                .store()
                .committed_request(&pending.request_id)
                .unwrap()
                .log_index,
            3
        );
    }
}

#[test]
fn snapshot_persistence_failure_fences_handle_until_recovery() {
    let root = sinomo_testdir::TestDir::new("sinomo-kv-snapshot-persist-failure");
    let paths = KvWalPaths::new(root.path());
    let common = entry(1, 1, "common");
    let mut node = DurableRaftKvNode::open(id("follower"), paths.clone()).unwrap();
    node.append_entries(append(2, "leader", 0, 0, vec![common.clone()], 1))
        .unwrap();
    let snapshot = committed_snapshot(2, vec![common, entry(2, 2, "new")]);
    let saved_wal = root.join("saved-wal");
    std::fs::rename(&paths.wal, &saved_wal).unwrap();
    std::fs::create_dir(&paths.wal).unwrap();
    assert!(node.restore_snapshot(snapshot).is_err());
    assert!(node.is_poisoned());
    assert!(matches!(
        node.append_local_command(2, put("must-not-append")),
        Err(KvError::RaftSessionPoisoned)
    ));
    drop(node);
    std::fs::remove_dir(&paths.wal).unwrap();
    std::fs::rename(saved_wal, &paths.wal).unwrap();

    let mut reopened = DurableRaftKvNode::open(id("follower"), paths).unwrap();
    assert_eq!(reopened.applied_index(), 2);
    assert_eq!(reopened.hard_state().commit_index, 2);
    assert_eq!(reopened.last_log_index(), 2);
    assert!(reopened.store().get("new").is_some());
    reopened
        .append_entries(append(2, "leader", 2, 2, vec![entry(3, 2, "next")], 3))
        .unwrap();
    assert!(reopened.store().get("next").is_some());
}

#[test]
fn heartbeat_fast_path_acknowledges_only_its_matching_prefix() {
    let root = sinomo_testdir::TestDir::new("sinomo-kv-heartbeat-match-index");
    let mut node = DurableRaftKvNode::open(id("follower"), KvWalPaths::new(root.path())).unwrap();
    node.append_entries(append(
        1,
        "leader",
        0,
        0,
        vec![entry(1, 1, "shared"), entry(2, 1, "uncommitted")],
        1,
    ))
    .unwrap();
    let response = node
        .append_entries(append(1, "leader", 1, 1, vec![], 1))
        .unwrap();
    assert!(response.success);
    assert_eq!(response.match_index, 1);
    assert_eq!(node.last_log_index(), 2);
    assert_eq!(node.applied_index(), 1);
}

#[test]
fn partial_append_acknowledges_only_entries_present_in_the_request() {
    let root = sinomo_testdir::TestDir::new("sinomo-kv-partial-append-match-index");
    let mut node = DurableRaftKvNode::open(id("follower"), KvWalPaths::new(root.path())).unwrap();
    let second = entry(2, 1, "shared/2");
    node.append_entries(append(
        1,
        "old-leader",
        0,
        0,
        vec![
            entry(1, 1, "shared/1"),
            second.clone(),
            entry(3, 1, "uncommitted"),
        ],
        1,
    ))
    .unwrap();
    let response = node
        .append_entries(append(2, "new-leader", 1, 1, vec![second], 3))
        .unwrap();
    assert!(response.success);
    assert_eq!(response.match_index, 2);
    assert_eq!(node.applied_index(), 2);
    assert!(node.store().get("uncommitted").is_none());
}

#[test]
fn anchored_snapshot_starts_a_fresh_node_where_replication_continues() {
    let root = sinomo_testdir::TestDir::new("sinomo-kv-anchored-snapshot");
    let leader_paths = ClusterKvPaths::new(root.path().join("leader"));
    let leader = id("anchor_leader");
    let mut node = leader_paths.open_raft_node(&leader).unwrap();
    node.become_leader(3).unwrap();
    for key in ["/a", "/b"] {
        let appended = node.append_local_command(3, put(key)).unwrap();
        node.commit_to(appended.index).unwrap();
    }
    let applied_index = node.applied_index();
    drop(node);
    let anchored = read_cluster_kv_raft_anchored_snapshot_before(
        &leader_paths.raft_wal_paths(&leader),
        &leader,
        std::time::Instant::now() + std::time::Duration::from_secs(5),
    )
    .expect("resident leader")
    .unwrap();
    assert_eq!(
        (anchored.last_included_index, anchored.last_included_term),
        (applied_index, 3)
    );

    let joiner_paths = ClusterKvPaths::new(root.path().join("joiner"));
    let joiner = id("anchor_joiner");
    let store = joiner_paths.raft_store();
    assert!(
        store
            .install_anchored_snapshot(&joiner, anchored.clone())
            .unwrap()
    );
    assert!(
        !store
            .install_anchored_snapshot(&joiner, anchored.clone())
            .unwrap(),
        "a node already at the anchor keeps its state"
    );

    let mut node = leader_paths.open_raft_node(&leader).unwrap();
    let appended = node.append_local_command(3, put("/c")).unwrap();
    node.commit_to(appended.index).unwrap();
    let entries = node.log().to_vec();
    let leader_revision = node.store().revision();
    drop(node);

    let mut joined =
        DurableRaftKvNode::open(joiner.clone(), joiner_paths.raft_wal_paths(&joiner)).unwrap();
    assert_eq!(joined.applied_index(), anchored.last_included_index);
    assert!(joined.store().get("/b").is_some());
    // Replication resumes after the anchor. Resending the covered prefix does
    // not apply it twice.
    let response = joined
        .append_entries(append(3, "anchor_leader", 0, 0, entries, appended.index))
        .unwrap();
    assert!(response.success);
    assert_eq!(joined.applied_index(), appended.index);
    assert_eq!(joined.store().revision(), leader_revision);
    assert!(joined.store().get("/c").is_some());
}
