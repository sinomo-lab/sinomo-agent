use sinomo_base::{ClusterBootstrapState, Id};
use sinomo_kv::*;
use sinomo_node::NodeId;
use std::time::{Duration, Instant};
use time::OffsetDateTime;

fn fixture(
    name: &str,
) -> (
    sinomo_testdir::TestDir,
    ClusterKvPaths,
    ClusterBootstrapState,
) {
    let root = sinomo_testdir::TestDir::new(name);
    let paths = ClusterKvPaths::new(root.path());
    let local = NodeId::from_legacy_named(name).unwrap();
    let mut state = ClusterBootstrapState::initialized_locally(
        Id::from_legacy_named("group_cluster").unwrap(),
        "group",
        local.clone(),
        "local",
        "pk",
        OffsetDateTime::now_utc(),
    );
    state.term = 7;
    state.members[0].capabilities =
        vec![sinomo_base::CapabilityName::new(sinomo_node::DAEMON_MAIN_RUN_CAPABILITY).unwrap()];
    state.members[0].allowed_capabilities = state.members[0].capabilities.clone();
    let mut node = paths.raft_store().open_node(&local).unwrap();
    node.become_leader(7).unwrap();
    drop(node);
    (root, paths, state)
}
fn put(key: &str, value: &[u8]) -> KvCommandRequest {
    KvCommandRequest::generated(KvCommand::Op(KvOp::Put {
        key: key.into(),
        value: value.into(),
        lease_id: None,
    }))
}
fn proposal(state: &ClusterBootstrapState, request: KvCommandRequest) -> ClusterKvRaftProposal {
    ClusterKvRaftProposal::new(
        state.clone(),
        request,
        Instant::now() + Duration::from_secs(10),
    )
}
fn appended(result: Result<ClusterKvRaftPreparedProposal, KvError>) -> KvLogEntry {
    match result.unwrap() {
        ClusterKvRaftPreparedProposal::Appended(e) => e,
        _ => panic!("expected append"),
    }
}

#[test]
fn grouped_requests_keep_independent_identity_and_survive_cold_restart() {
    let (_root, paths, state) = fixture("group_restart");
    let requests: Vec<_> = (0..32)
        .map(|i| proposal(&state, put(&format!("/group/{i}"), b"value")))
        .collect();
    let entries: Vec<_> =
        prepare_cluster_kv_raft_proposals(&paths, &state.local_node_id, &requests)
            .into_iter()
            .map(appended)
            .collect();
    assert_eq!(entries.len(), 32);
    assert!(entries.windows(2).all(|e| e[1].index == e[0].index + 1));
    for (entry, p) in entries.iter().zip(&requests) {
        assert_eq!(entry.request_id, p.request.request_id);
    }
    // A separate nonresident open forces disk replay without relying on the
    // preparation owner's in-memory node. No commit was acknowledged yet.
    let wal = paths.raft_wal_paths(&state.local_node_id);
    let copy = KvWalPaths::new(_root.path().join("cold-replay"));
    std::fs::create_dir_all(&copy.root).unwrap();
    for file in std::fs::read_dir(&wal.root).unwrap() {
        let file = file.unwrap();
        if file.file_type().unwrap().is_file() {
            std::fs::copy(file.path(), copy.root.join(file.file_name())).unwrap();
        }
    }
    let mut cold = DurableRaftKvNode::open(state.local_node_id.clone(), copy).unwrap();
    assert_eq!(cold.last_log_index(), 32);
    assert_eq!(cold.hard_state().commit_index, 0);
    for entry in &entries {
        assert_eq!(cold.log_entry_at(entry.index), Some(entry));
    }
    cold.commit_to(32).unwrap();
    for p in &requests {
        assert!(
            cold.store()
                .committed_request(&p.request.request_id)
                .is_some()
        );
    }
}

#[test]
fn grouped_compares_follow_log_order_and_each_request_replays_its_result() {
    let (_root, paths, state) = fixture("group_compares");
    let first = put("/compare", b"first");
    let second = KvCommandRequest::generated(KvCommand::Txn(KvTxn {
        compares: vec![KvCompare::ValueEquals {
            key: "/compare".into(),
            value: b"first".to_vec(),
        }],
        success: vec![KvOp::Put {
            key: "/result".into(),
            value: b"yes".to_vec(),
            lease_id: None,
        }],
        failure: vec![KvOp::Put {
            key: "/result".into(),
            value: b"no".to_vec(),
            lease_id: None,
        }],
    }));
    let p = vec![proposal(&state, first), proposal(&state, second.clone())];
    let entries: Vec<_> = prepare_cluster_kv_raft_proposals(&paths, &state.local_node_id, &p)
        .into_iter()
        .map(appended)
        .collect();
    let mut node = paths.raft_store().open_node(&state.local_node_id).unwrap();
    node.commit_to(entries.last().unwrap().index).unwrap();
    assert_eq!(node.store().get("/result").unwrap().value, b"yes");
    drop(node);
    let replay = prepare_cluster_kv_raft_proposals(
        &paths,
        &state.local_node_id,
        &[proposal(&state, second)],
    )
    .pop()
    .unwrap()
    .unwrap();
    assert!(matches!(replay, ClusterKvRaftPreparedProposal::Replayed(_)));
}

#[test]
fn expired_and_stale_proposals_cannot_append_or_poison_valid_neighbor() {
    let (_root, paths, state) = fixture("group_fences");
    let mut expired = proposal(&state, put("/expired", b"no"));
    expired.deadline = Instant::now() - Duration::from_millis(1);
    let mut stale = proposal(&state, put("/stale", b"no"));
    stale.state.term = 6;
    let good = proposal(&state, put("/good", b"yes"));
    let results =
        prepare_cluster_kv_raft_proposals(&paths, &state.local_node_id, &[expired, stale, good]);
    assert!(results[0].is_err());
    assert!(results[1].is_err());
    assert_eq!(appended(results[2].clone()).index, 1);
}

#[test]
fn conflicting_request_id_does_not_change_history_or_reject_other_requests() {
    let (_root, paths, state) = fixture("group_duplicates");
    let first = put("/first", b"a");
    let mut conflict = put("/first", b"b");
    conflict.request_id = first.request_id.clone();
    let p = vec![
        proposal(&state, first),
        proposal(&state, conflict),
        proposal(&state, put("/other", b"c")),
    ];
    let r = prepare_cluster_kv_raft_proposals(&paths, &state.local_node_id, &p);
    assert_eq!(appended(r[0].clone()).index, 1);
    assert!(matches!(&r[1], Err(KvError::RequestIdConflict { .. })));
    assert_eq!(appended(r[2].clone()).index, 2);
}

#[test]
fn group_limits_and_membership_changes_fail_before_wal_mutation() {
    let (_root, paths, state) = fixture("group_bounds");
    let p: Vec<_> = (0..65)
        .map(|i| proposal(&state, put(&format!("/{i}"), b"v")))
        .collect();
    assert!(
        prepare_cluster_kv_raft_proposals(&paths, &state.local_node_id, &p)
            .iter()
            .all(Result::is_err)
    );
    for command in [
        KvCommand::Barrier,
        KvCommand::Op(KvOp::Delete {
            key: cluster_kv_raft_config_key(),
        }),
    ] {
        let request = KvCommandRequest::generated(command);
        assert!(!cluster_kv_raft_request_can_group(&request));
        assert!(
            prepare_cluster_kv_raft_proposals(
                &paths,
                &state.local_node_id,
                &[proposal(&state, request)]
            )[0]
            .is_err()
        );
    }
    assert_eq!(
        paths
            .raft_store()
            .open_node(&state.local_node_id)
            .unwrap()
            .last_log_index(),
        0
    );
}

#[test]
fn unreadable_wal_rejects_before_submission_and_recovers_after_repair() {
    let (_root, paths, state) = fixture("group_failed_wal");
    let wal = paths.raft_wal_paths(&state.local_node_id);
    std::fs::create_dir_all(&wal.wal).unwrap();
    let requests = vec![
        proposal(&state, put("/failed/a", b"a")),
        proposal(&state, put("/failed/b", b"b")),
    ];
    let results = prepare_cluster_kv_raft_proposals(&paths, &state.local_node_id, &requests);
    assert!(results.iter().all(Result::is_err));
    for request in &requests {
        let outcome = read_cluster_kv_request_attempt(&paths, &request.request.request_id).unwrap();
        assert!(
            matches!(outcome, Some(KvRequestOutcome::RejectedBeforeCommit { .. })),
            "results={results:?}, outcome={outcome:?}"
        );
    }
    std::fs::remove_dir(&wal.wal).unwrap();
    let next = prepare_cluster_kv_raft_proposals(
        &paths,
        &state.local_node_id,
        &[proposal(&state, put("/next", b"ok"))],
    );
    assert_eq!(
        appended(next[0].clone()).index,
        1,
        "failed unpublished staging cannot remain in the resident log"
    );
}

#[test]
fn group_revalidates_committed_voter_set_after_queue_admission() {
    let (_root, paths, state) = fixture("group_membership_fence");
    let config = ClusterKvRaftConfigRecord {
        cluster_id: state.cluster_id.clone(),
        term: 7,
        kv_revision: 1,
        voters: vec![
            state.local_node_id.clone(),
            NodeId::from_legacy_named("new_voter").unwrap(),
        ],
        learners: vec![],
        non_voting: vec![],
        updated_at: OffsetDateTime::now_utc(),
    };
    let mut node = paths.raft_store().open_node(&state.local_node_id).unwrap();
    let entry = node
        .append_local_command(
            7,
            KvCommand::Op(KvOp::Put {
                key: cluster_kv_raft_config_key(),
                value: encode_kv_value(&config).unwrap(),
                lease_id: None,
            }),
        )
        .unwrap();
    node.commit_to(entry.index).unwrap();
    drop(node);
    let result = prepare_cluster_kv_raft_proposals(
        &paths,
        &state.local_node_id,
        &[proposal(&state, put("/stale-voters", b"no"))],
    );
    assert!(
        matches!(&result[0], Err(KvError::AuthorityUnavailable { detail }) if detail.contains("membership fence"))
    );
}

#[test]
fn cancelled_admission_does_not_write_a_submission_or_wal_entry() {
    let (_root, paths, state) = fixture("group_cancelled");
    let request = put("/cancelled", b"no");
    let cancelled = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    let cancelled_proposal = proposal(&state, request.clone()).with_cancellation(cancelled);
    let results = prepare_cluster_kv_raft_proposals(
        &paths,
        &state.local_node_id,
        &[cancelled_proposal, proposal(&state, put("/valid", b"yes"))],
    );
    assert!(results[0].is_err());
    assert_eq!(appended(results[1].clone()).index, 1);
    assert!(
        read_cluster_kv_request_attempt(&paths, &request.request_id)
            .unwrap()
            .is_none()
    );
}

#[test]
fn acknowledged_prefix_applies_together_but_returns_the_owning_request_result() {
    let (_root, paths, state) = fixture("group_prefix_apply");
    let requests: Vec<_> = (0..3)
        .map(|i| {
            proposal(
                &state,
                KvCommandRequest::generated(KvCommand::Txn(KvTxn {
                    compares: vec![],
                    success: vec![KvOp::Put {
                        key: format!("/prefix/{i}"),
                        value: vec![i],
                        lease_id: None,
                    }],
                    failure: vec![],
                })),
            )
        })
        .collect();
    let entries: Vec<_> =
        prepare_cluster_kv_raft_proposals(&paths, &state.local_node_id, &requests)
            .into_iter()
            .map(appended)
            .collect();
    let mut storage = paths.local_raft_command_storage(
        state.local_node_id.clone(),
        cluster_kv_key_can_defer_materialized_snapshot as fn(&str) -> bool,
    );
    storage.adopt_group_prepared_entry(entries[0].clone());
    let commit = storage
        .commit_acknowledged_prefix_compact(entries[0].index, entries[2].index)
        .unwrap();
    assert_eq!(
        commit.committed_request.request_id,
        requests[0].request.request_id
    );
    assert_eq!(commit.committed_request.revision, KvRevision(1));
    assert_eq!(commit.txn_response.unwrap().revision, KvRevision(1));
    assert_eq!(commit.read_state.commit_index, 3);
    assert_eq!(commit.events.len(), 3);
    storage.release_session_after_commit();
    assert!(
        storage
            .replay_record_if_committed(&requests[2].request)
            .unwrap()
            .is_some()
    );
}

#[test]
fn grouped_finalization_respects_ack_bound_and_control_entry_boundaries() {
    for control in [
        KvCommand::Barrier,
        KvCommand::Op(KvOp::Delete {
            key: cluster_kv_raft_config_key(),
        }),
    ] {
        let (_root, paths, state) = fixture("group_prefix_control");
        let request = proposal(&state, put("/before-control", b"v"));
        let first = appended(
            prepare_cluster_kv_raft_proposals(&paths, &state.local_node_id, &[request]).remove(0),
        );
        let mut node = paths.raft_store().open_node(&state.local_node_id).unwrap();
        node.append_local_command(7, control).unwrap();
        node.append_local_command(7, put("/after-control", b"v").command)
            .unwrap();
        drop(node);
        let mut storage = paths.local_raft_command_storage(
            state.local_node_id.clone(),
            cluster_kv_key_can_defer_materialized_snapshot as fn(&str) -> bool,
        );
        storage.adopt_group_prepared_entry(first);
        let commit = storage.commit_acknowledged_prefix_compact(1, 3).unwrap();
        assert_eq!(commit.read_state.commit_index, 1);
        storage.release_session_after_commit();
    }
    let (_root, paths, state) = fixture("group_prefix_limit");
    let requests: Vec<_> = (0..65)
        .map(|i| proposal(&state, put(&format!("/limit/{i}"), b"v")))
        .collect();
    let mut entries = Vec::new();
    for batch in requests.chunks(64) {
        entries.extend(
            prepare_cluster_kv_raft_proposals(&paths, &state.local_node_id, batch)
                .into_iter()
                .map(appended),
        );
    }
    let mut storage = paths.local_raft_command_storage(
        state.local_node_id.clone(),
        cluster_kv_key_can_defer_materialized_snapshot as fn(&str) -> bool,
    );
    storage.adopt_group_prepared_entry(entries[0].clone());
    assert!(storage.commit_acknowledged_prefix_compact(1, 0).is_err());
    assert_eq!(
        storage
            .commit_acknowledged_prefix_compact(1, 1000)
            .unwrap()
            .read_state
            .commit_index,
        64
    );
}

#[test]
fn prefix_finalization_keeps_every_event_inside_retained_history() {
    let (_root, paths, state) = fixture("group_prefix_events");
    let requests: Vec<_> = (0..2)
        .map(|group| {
            proposal(
                &state,
                KvCommandRequest::generated(KvCommand::Txn(KvTxn {
                    compares: vec![],
                    success: (0..3000)
                        .map(|i| KvOp::Put {
                            key: format!("/events/{group}/{i}"),
                            value: vec![1],
                            lease_id: None,
                        })
                        .collect(),
                    failure: vec![],
                })),
            )
        })
        .collect();
    let entries: Vec<_> =
        prepare_cluster_kv_raft_proposals(&paths, &state.local_node_id, &requests)
            .into_iter()
            .map(appended)
            .collect();
    let mut storage = paths.local_raft_command_storage(
        state.local_node_id.clone(),
        cluster_kv_key_can_defer_materialized_snapshot as fn(&str) -> bool,
    );
    storage.adopt_group_prepared_entry(entries[0].clone());
    let commit = storage.commit_acknowledged_prefix_compact(1, 2).unwrap();
    assert_eq!(commit.read_state.commit_index, 1);
    assert_eq!(commit.events.len(), 3000);
}

#[test]
fn cold_restored_tail_cannot_supply_optimistic_local_acknowledgements() {
    let (root, paths, state) = fixture("group_prefix_cold");
    let requests: Vec<_> = (0..3)
        .map(|i| proposal(&state, put(&format!("/cold/{i}"), b"v")))
        .collect();
    let entries: Vec<_> =
        prepare_cluster_kv_raft_proposals(&paths, &state.local_node_id, &requests)
            .into_iter()
            .map(appended)
            .collect();
    let copy_paths = ClusterKvPaths::new(root.path().join("copy"));
    let source = paths.raft_wal_paths(&state.local_node_id);
    let dest = copy_paths.raft_wal_paths(&state.local_node_id);
    std::fs::create_dir_all(&dest.root).unwrap();
    for file in std::fs::read_dir(source.root).unwrap() {
        let file = file.unwrap();
        if file.file_type().unwrap().is_file() {
            std::fs::copy(file.path(), dest.root.join(file.file_name())).unwrap();
        }
    }
    let mut storage = copy_paths.local_raft_command_storage(
        state.local_node_id.clone(),
        cluster_kv_key_can_defer_materialized_snapshot as fn(&str) -> bool,
    );
    storage.adopt_group_prepared_entry(entries[0].clone());
    assert_eq!(
        storage
            .commit_acknowledged_prefix_compact(1, 3)
            .unwrap()
            .read_state
            .commit_index,
        1
    );
    storage.release_session_after_commit();
    // Fresh WAL durability covers the restored tail as well.
    let fresh = appended(
        prepare_cluster_kv_raft_proposals(
            &copy_paths,
            &state.local_node_id,
            &[proposal(&state, put("/fresh", b"v"))],
        )
        .remove(0),
    );
    storage.adopt_group_prepared_entry(entries[1].clone());
    assert_eq!(
        storage
            .commit_acknowledged_prefix_compact(2, fresh.index)
            .unwrap()
            .read_state
            .commit_index,
        4
    );
}
