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

#[test]
fn request_reads_and_appends_preserve_inherited_pending_log() {
    for operation in [
        "replay",
        "command",
        "request",
        "request_or_replay",
        "commit",
    ] {
        let root = sinomo_testdir::TestDir::new(operation);
        let paths = ClusterKvPaths::new(root.path());
        let store = paths.raft_store();
        let local = id("pending_log_owner");
        let pending = {
            let mut node = store.open_node(&local).unwrap();
            node.become_leader(7).unwrap();
            node.append_local_command(7, put("/pending")).unwrap()
        };
        let request = KvCommandRequest::generated(put("/next"));
        match operation {
            "replay" => assert!(
                store
                    .replay_request_if_committed(&local, &request)
                    .unwrap()
                    .is_none()
            ),
            "command" => assert_eq!(
                store
                    .append_command_uncommitted(&local, request.command)
                    .unwrap()
                    .index,
                pending.index + 1
            ),
            "request" => assert_eq!(
                store
                    .append_request_uncommitted(&local, request)
                    .unwrap()
                    .index,
                pending.index + 1
            ),
            "request_or_replay" => match store
                .append_request_uncommitted_or_replay(&local, request)
                .unwrap()
            {
                ClusterKvRaftUncommittedAppend::Appended(entry) => {
                    assert_eq!(entry.index, pending.index + 1)
                }
                _ => panic!("fresh request unexpectedly replayed"),
            },
            "commit" => assert_eq!(
                store
                    .append_request_with_response(&local, request)
                    .unwrap()
                    .committed_request
                    .log_index,
                pending.index + 1
            ),
            _ => unreachable!(),
        }
        let node = store.open_node(&local).unwrap();
        let retained = node
            .log_entry_at(pending.index)
            .expect("pending log entry retained");
        assert_eq!(retained.request_id, pending.request_id);
        assert_eq!(retained.command, pending.command);
    }
}

#[test]
fn identical_lease_entries_and_cold_replay_have_identical_state() {
    let root = sinomo_testdir::TestDir::new("raft-lease-determinism");
    let leader = id("lease_leader");
    let mut nodes: Vec<_> = ["lease_a", "lease_b"]
        .into_iter()
        .map(|name| {
            let paths = KvWalPaths::new(root.path().join(name));
            (
                id(name),
                paths.clone(),
                DurableRaftKvNode::open(id(name), paths).unwrap(),
            )
        })
        .collect();
    for (index, command) in [
        KvCommand::Op(KvOp::LeaseGrant {
            lease_id: LeaseId(42),
            ttl_ticks: 10,
        }),
        KvCommand::Op(KvOp::LeaseGrant {
            lease_id: LeaseId(42),
            ttl_ticks: 20,
        }),
        KvCommand::Op(KvOp::LeaseKeepAlive {
            lease_id: LeaseId(42),
        }),
    ]
    .into_iter()
    .enumerate()
    {
        let index = index as u64 + 1;
        let entry = KvLogEntry {
            index,
            term: 7,
            request_id: KvRequestId::generate(),
            command,
            revision: KvRevision::ZERO,
        };
        for (_, _, node) in &mut nodes {
            assert!(
                node.append_entries(RaftAppendEntriesRequest {
                    term: 7,
                    leader_id: leader.clone(),
                    prev_log_index: index - 1,
                    prev_log_term: if index == 1 { 0 } else { 7 },
                    entries: vec![entry.clone()],
                    leader_commit: index,
                })
                .unwrap()
                .success
            );
        }
        assert_eq!(
            nodes[0].2.store().try_snapshot().unwrap(),
            nodes[1].2.store().try_snapshot().unwrap()
        );
    }
    let expected = nodes[0].2.store().try_snapshot().unwrap();
    for (node_id, paths, node) in nodes {
        drop(node);
        let reopened = DurableRaftKvNode::open(node_id, paths).unwrap();
        assert_eq!(reopened.store().try_snapshot().unwrap(), expected);
    }
}

#[test]
fn explicit_local_truncation_cannot_erase_pending_consensus_history() {
    let root = sinomo_testdir::TestDir::new("raft-truncation-guard");
    let paths = KvWalPaths::new(root.path());
    let local = id("guarded_pending_owner");
    let pending = {
        let mut node = DurableRaftKvNode::open(local.clone(), paths.clone()).unwrap();
        node.become_leader(7).unwrap();
        node.append_local_command(7, put("/pending")).unwrap()
    };
    let original_wal = std::fs::read(&paths.wal).unwrap();
    assert!(
        DurableRaftKvNode::open_discarding_uncommitted_tail(local.clone(), paths.clone()).is_err()
    );
    let mut node = DurableRaftKvNode::open(local, paths.clone()).unwrap();
    assert!(node.discard_uncommitted_tail().is_err());
    assert_eq!(node.log_entry_at(pending.index), Some(&pending));
    assert_eq!(std::fs::read(paths.wal).unwrap(), original_wal);
}

#[test]
fn recovery_refuses_to_move_a_durable_commit_index_backwards() {
    let root = sinomo_testdir::TestDir::new("raft-missing-committed-prefix");
    let paths = KvWalPaths::new(root.path());
    let local = id("missing_committed_owner");
    let mut node = DurableRaftKvNode::open(local.clone(), paths.clone()).unwrap();
    let entry = node.append_local_command(7, put("/committed")).unwrap();
    node.commit_to(entry.index).unwrap();
    drop(node);
    let mut hard = paths.read_persisted_hard_state().unwrap();
    hard.commit_index += 1;
    paths.write_persisted_hard_state(&hard).unwrap();
    assert!(DurableRaftKvNode::open(local, paths.clone()).is_err());
    assert_eq!(paths.read_persisted_hard_state().unwrap(), hard);
}

#[test]
fn renewing_a_partially_elapsed_lease_is_deterministic() {
    let mut original = InProcessKvStore::new();
    original
        .apply_command(KvCommand::Op(KvOp::LeaseGrant {
            lease_id: LeaseId(77),
            ttl_ticks: 10,
        }))
        .unwrap();
    original.tick_leases(3).unwrap();
    let snapshot = original.try_snapshot().unwrap();
    let mut left = InProcessKvStore::restore(snapshot.clone()).unwrap();
    let mut right = InProcessKvStore::restore(snapshot).unwrap();
    let renew = KvCommand::Op(KvOp::LeaseKeepAlive {
        lease_id: LeaseId(77),
    });
    left.apply_command(renew.clone()).unwrap();
    right.apply_command(renew).unwrap();
    let renewed = left.try_snapshot().unwrap();
    assert_eq!(renewed, right.try_snapshot().unwrap());
    assert_eq!(renewed.leases[0].remaining_ticks, 10);
    assert_eq!(
        renewed.leases[0].granted_at,
        time::OffsetDateTime::UNIX_EPOCH
    );
}

#[test]
fn proposal_cleanup_preserves_an_entry_another_wave_may_have_replicated() {
    let root = sinomo_testdir::TestDir::new("raft-cleanup-preserves-history");
    let paths = ClusterKvPaths::new(root.path());
    let local = id("cleanup_owner");
    let store = paths.raft_store();
    store.set_leader_compact(&local, 7).unwrap();
    let mut storage = paths.local_raft_command_storage(local.clone(), |_| false);
    let pending = storage
        .append_uncommitted(KvCommandRequest::generated(put("/pending")))
        .unwrap();
    storage.release_session_for_quorum();
    storage
        .discard_uncommitted_tail_and_clear_leader_compact()
        .unwrap();
    let node = store.open_node(&local).unwrap();
    assert_eq!(node.log_entry_at(pending.index), Some(&pending));
    assert_eq!(node.hard_state().leader_id, None);
}

#[test]
#[ignore = "subprocess entry point for restart_preserves_a_peer_committed_prefix"]
fn initialize_resident_in_child_process() {
    let root =
        std::env::var_os("SINOMO_TEST_RAFT_RESTART_ROOT").expect("parent supplies isolated root");
    ClusterKvPaths::new(std::path::PathBuf::from(root))
        .locked_initialize_raft_resident(&id("restart_owner"))
        .unwrap();
}

#[test]
fn restart_preserves_a_peer_committed_prefix() {
    let root = sinomo_testdir::TestDir::new("raft-process-restart");
    let paths = ClusterKvPaths::new(root.path().join("leader"));
    let leader = id("restart_owner");
    let mut node = DurableRaftKvNode::open(leader.clone(), paths.raft_wal_paths(&leader)).unwrap();
    node.become_leader(7).unwrap();
    let seed = node.append_local_command(7, put("/seed")).unwrap();
    node.commit_to(seed.index).unwrap();
    let snapshot = node.save_snapshot().unwrap();
    let pending = node
        .append_local_command(7, put("/peer-committed"))
        .unwrap();
    let hard_before = node.hard_state().clone();
    let mut peers = Vec::new();
    for name in ["restart_peer_one", "restart_peer_two"] {
        let mut peer =
            DurableRaftKvNode::open(id(name), KvWalPaths::new(root.path().join(name))).unwrap();
        peer.restore_snapshot(snapshot.clone()).unwrap();
        assert!(
            peer.append_entries(RaftAppendEntriesRequest {
                term: pending.term,
                leader_id: leader.clone(),
                prev_log_index: seed.index,
                prev_log_term: seed.term,
                entries: vec![pending.clone()],
                leader_commit: pending.index,
            })
            .unwrap()
            .success
        );
        peers.push(peer);
    }
    // The old leader's durable commit knowledge lags the other two replicas.
    drop(node);
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "initialize_resident_in_child_process",
            "--ignored",
            "--nocapture",
        ])
        .env("SINOMO_TEST_RAFT_RESTART_ROOT", &paths.root)
        .output()
        .unwrap();
    assert!(
        child.status.success(),
        "child failed: {}",
        String::from_utf8_lossy(&child.stderr)
    );
    let mut node = DurableRaftKvNode::open(leader.clone(), paths.raft_wal_paths(&leader)).unwrap();
    assert_eq!(node.hard_state(), &hard_before);
    assert_eq!(node.log_entry_at(pending.index), Some(&pending));
    let next = node.append_local_command(7, put("/after-restart")).unwrap();
    assert_eq!(next.index, pending.index + 1);
    node.commit_to(next.index).unwrap();
    for mut peer in peers {
        assert!(
            peer.append_entries(RaftAppendEntriesRequest {
                term: next.term,
                leader_id: leader.clone(),
                prev_log_index: pending.index,
                prev_log_term: pending.term,
                entries: vec![next.clone()],
                leader_commit: next.index,
            })
            .unwrap()
            .success
        );
        assert_eq!(
            node.store().try_snapshot().unwrap(),
            peer.store().try_snapshot().unwrap()
        );
    }
}
