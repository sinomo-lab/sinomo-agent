use crate::internal::*;
use sinomo_core::{ClusterBootstrapState, ClusterMemberRole, ClusterMemberState, Id};
use std::collections::BTreeMap;
use time::OffsetDateTime;

fn node_id(name: &str) -> NodeId {
    NodeId::from_legacy_named(name).expect("node id")
}

fn cluster_with_nodes(names: &[&str]) -> (Vec<NodeId>, InProcessRaftKvCluster) {
    let nodes = names.iter().map(|name| node_id(name)).collect::<Vec<_>>();
    let cluster = InProcessRaftKvCluster::new(nodes.clone()).expect("cluster");
    (nodes, cluster)
}

fn assert_node_value(
    cluster: &InProcessRaftKvCluster,
    node_id: &NodeId,
    key: &str,
    expected: &[u8],
) {
    assert_eq!(
        cluster
            .store(node_id)
            .expect("node store")
            .get(key)
            .unwrap_or_else(|| panic!("missing key {key} on {node_id}"))
            .value,
        expected
    );
}

fn assert_node_missing(cluster: &InProcessRaftKvCluster, node_id: &NodeId, key: &str) {
    assert!(
        cluster
            .store(node_id)
            .expect("node store")
            .get(key)
            .is_none(),
        "{key} should be absent on {node_id}"
    );
}

fn assert_all_nodes_value(
    cluster: &InProcessRaftKvCluster,
    nodes: &[NodeId],
    key: &str,
    expected: &[u8],
) {
    for node_id in nodes {
        assert_node_value(cluster, node_id, key, expected);
    }
}

fn assert_all_nodes_revision(
    cluster: &InProcessRaftKvCluster,
    nodes: &[NodeId],
    expected: KvRevision,
) {
    for node_id in nodes {
        assert_eq!(
            cluster.store(node_id).expect("node store").revision(),
            expected,
            "{node_id} revision"
        );
    }
}

fn put_command(key: &str, value: &[u8]) -> KvCommand {
    KvCommand::Op(KvOp::Put {
        key: key.to_string(),
        value: value.to_vec(),
        lease_id: None,
    })
}

fn temp_root(prefix: &str) -> sinomo_testdir::TestDir {
    sinomo_testdir::TestDir::new(prefix)
}

struct ClusterSim {
    nodes: Vec<NodeId>,
    cluster: InProcessRaftKvCluster,
    online: Vec<bool>,
}

impl ClusterSim {
    fn new(names: &[&str]) -> Self {
        let (nodes, cluster) = cluster_with_nodes(names);
        let online = vec![true; nodes.len()];
        Self {
            nodes,
            cluster,
            online,
        }
    }

    fn partition(&mut self, node_index: usize) {
        self.cluster.set_node_online(&self.nodes[node_index], false);
        self.online[node_index] = false;
    }

    fn heal(&mut self, node_index: usize) {
        self.cluster.set_node_online(&self.nodes[node_index], true);
        self.online[node_index] = true;
    }

    fn elect(&mut self, node_index: usize) {
        if !self.online[node_index] {
            return;
        }
        match self.cluster.elect_leader(self.nodes[node_index].clone()) {
            Ok(_) => {}
            Err(KvError::LeaderOffline { .. } | KvError::QuorumUnavailable { .. }) => {}
            Err(error) => panic!("unexpected election error: {error}"),
        }
    }

    fn put_model(&mut self, key: String, value: Vec<u8>, model: &mut BTreeMap<String, Vec<u8>>) {
        match self.cluster.apply_as_leader(put_command(&key, &value)) {
            Ok(_) => {
                model.insert(key, value);
            }
            Err(KvError::LeaderOffline { .. } | KvError::QuorumUnavailable { .. }) => {}
            Err(error) => panic!("unexpected put error: {error}"),
        }
    }

    fn delete_model(&mut self, key: String, model: &mut BTreeMap<String, Vec<u8>>) {
        match self
            .cluster
            .apply_as_leader(KvCommand::Op(KvOp::Delete { key: key.clone() }))
        {
            Ok(_) => {
                model.remove(&key);
            }
            Err(KvError::LeaderOffline { .. } | KvError::QuorumUnavailable { .. }) => {}
            Err(error) => panic!("unexpected delete error: {error}"),
        }
    }

    fn assert_online_matches_model(&self, model: &BTreeMap<String, Vec<u8>>) {
        assert_online_nodes_match_model(&self.cluster, &self.nodes, &self.online, model);
    }
}

fn assert_online_nodes_match_model(
    cluster: &InProcessRaftKvCluster,
    nodes: &[NodeId],
    online: &[bool],
    model: &BTreeMap<String, Vec<u8>>,
) {
    for (index, node_id) in nodes.iter().enumerate() {
        if !online[index] {
            continue;
        }
        let store = cluster.store(node_id).expect("node store");
        for (key, expected) in model {
            assert_eq!(
                store
                    .get(key)
                    .unwrap_or_else(|| panic!("missing model key {key} on {node_id}"))
                    .value,
                *expected,
                "{node_id} model value for {key}"
            );
        }
        for entry in store.range_prefix("/cluster/model/") {
            assert!(
                model.contains_key(&entry.key),
                "{node_id} has unexpected model key {}",
                entry.key
            );
        }
    }
}

#[test]
fn cluster_quorum_commit_respects_single_three_and_five_voter_thresholds() {
    let (single_nodes, mut single) = cluster_with_nodes(&["kv_single_a"]);
    single
        .put("/cluster/quorum/single", b"ok".to_vec())
        .expect("single voter commit");
    assert_all_nodes_value(&single, &single_nodes, "/cluster/quorum/single", b"ok");

    let (nodes, mut cluster) = cluster_with_nodes(&[
        "kv_five_a",
        "kv_five_b",
        "kv_five_c",
        "kv_five_d",
        "kv_five_e",
    ]);
    cluster.set_node_online(&nodes[1], false);
    cluster.set_node_online(&nodes[2], false);
    cluster
        .put("/cluster/quorum/majority", b"committed".to_vec())
        .expect("three of five commit");
    assert_node_value(
        &cluster,
        &nodes[0],
        "/cluster/quorum/majority",
        b"committed",
    );
    assert_node_value(
        &cluster,
        &nodes[3],
        "/cluster/quorum/majority",
        b"committed",
    );
    assert_node_value(
        &cluster,
        &nodes[4],
        "/cluster/quorum/majority",
        b"committed",
    );
    assert_node_missing(&cluster, &nodes[1], "/cluster/quorum/majority");
    assert_node_missing(&cluster, &nodes[2], "/cluster/quorum/majority");

    cluster.set_node_online(&nodes[1], true);
    cluster.set_node_online(&nodes[2], true);
    assert_all_nodes_value(&cluster, &nodes, "/cluster/quorum/majority", b"committed");

    cluster.set_node_online(&nodes[1], false);
    cluster.set_node_online(&nodes[2], false);
    cluster.set_node_online(&nodes[3], false);
    let before_commit_index = cluster.commit_index();
    let error = cluster
        .put("/cluster/quorum/minority", b"blocked".to_vec())
        .expect_err("two of five cannot commit");
    assert!(matches!(
        error,
        KvError::QuorumUnavailable { quorum: 3, .. }
    ));
    assert_eq!(cluster.commit_index(), before_commit_index);
    for node_id in &nodes {
        assert_node_missing(&cluster, node_id, "/cluster/quorum/minority");
    }
}

#[test]
fn cluster_partition_election_keeps_majority_history_after_old_leader_heals() {
    let (nodes, mut cluster) =
        cluster_with_nodes(&["kv_partition_a", "kv_partition_b", "kv_partition_c"]);
    cluster
        .put("/cluster/partition/base", b"base".to_vec())
        .expect("base write");

    cluster.set_node_online(&nodes[0], false);
    let error = cluster
        .put("/cluster/partition/minority", b"blocked".to_vec())
        .expect_err("isolated leader cannot write");
    assert!(matches!(error, KvError::LeaderOffline { ref leader_id } if leader_id == &nodes[0]));

    assert!(cluster.elect_leader(nodes[1].clone()).expect("elect b"));
    cluster
        .put("/cluster/partition/majority", b"majority".to_vec())
        .expect("majority write");
    assert_node_missing(&cluster, &nodes[0], "/cluster/partition/majority");

    cluster.set_node_online(&nodes[0], true);
    assert_all_nodes_value(&cluster, &nodes, "/cluster/partition/base", b"base");
    assert_all_nodes_value(&cluster, &nodes, "/cluster/partition/majority", b"majority");
    assert_node_missing(&cluster, &nodes[0], "/cluster/partition/minority");
    assert_all_nodes_revision(&cluster, &nodes, KvRevision(2));
}

#[test]
fn cluster_replication_catchup_replays_gap_free_history_to_healed_voter() {
    let (nodes, mut cluster) =
        cluster_with_nodes(&["kv_catchup_a", "kv_catchup_b", "kv_catchup_c"]);
    cluster.set_node_online(&nodes[2], false);
    for index in 0..6 {
        cluster
            .put(
                format!("/cluster/catchup/{index}"),
                format!("value-{index}").into_bytes(),
            )
            .expect("majority write");
    }
    assert_eq!(
        cluster.store(&nodes[0]).expect("leader").revision(),
        KvRevision(6)
    );
    assert_eq!(
        cluster.store(&nodes[2]).expect("offline voter").revision(),
        KvRevision::ZERO
    );

    cluster.set_node_online(&nodes[2], true);
    assert_all_nodes_revision(&cluster, &nodes, KvRevision(6));
    assert_eq!(
        cluster
            .hard_state(&nodes[2])
            .expect("follower hard state")
            .commit_index,
        cluster.commit_index()
    );
    for index in 0..6 {
        assert_node_value(
            &cluster,
            &nodes[2],
            &format!("/cluster/catchup/{index}"),
            format!("value-{index}").as_bytes(),
        );
    }
}

#[test]
fn cluster_replication_catchup_replicates_tail_to_snapshot_joined_learner() {
    let (nodes, mut cluster) = cluster_with_nodes(&["kv_learner_a", "kv_learner_b"]);
    cluster
        .put("/cluster/learner/before", b"before".to_vec())
        .expect("initial write");
    let snapshot = cluster.store(&nodes[0]).expect("leader store").snapshot();
    let learner = node_id("kv_learner_c");
    cluster
        .add_learner_from_snapshot(learner.clone(), snapshot)
        .expect("add learner");
    cluster
        .put("/cluster/learner/after", b"after".to_vec())
        .expect("replicated learner tail");

    assert_node_value(&cluster, &learner, "/cluster/learner/before", b"before");
    assert_node_value(&cluster, &learner, "/cluster/learner/after", b"after");
    assert_eq!(
        cluster.store(&learner).expect("learner store").revision(),
        KvRevision(2)
    );
}

#[test]
fn cluster_durable_restart_keeps_committed_state_and_drops_uncommitted_tail() {
    let root = temp_root("sinomo-kv-cluster-durable-restart");
    let wal_paths = KvWalPaths::new(root.join("raft"));
    let local = node_id("kv_durable_restart");

    {
        let mut node =
            DurableRaftKvNode::open(local.clone(), wal_paths.clone()).expect("open durable node");
        let committed = node
            .append_local_command(1, put_command("/cluster/restart/committed", b"committed"))
            .expect("append committed");
        node.commit_to(committed.index).expect("commit");
        let uncommitted = node
            .append_local_command(1, put_command("/cluster/restart/uncommitted", b"dangling"))
            .expect("append uncommitted");
        assert!(uncommitted.index > committed.index);
    }

    let node = DurableRaftKvNode::open_discarding_uncommitted_tail(local, wal_paths)
        .expect("reopen durable node");
    assert_eq!(node.hard_state().commit_index, 1);
    assert_eq!(node.last_log_index(), 1);
    assert_eq!(
        node.store()
            .get("/cluster/restart/committed")
            .expect("committed entry")
            .value,
        b"committed"
    );
    assert!(node.store().get("/cluster/restart/uncommitted").is_none());
}

#[test]
fn cluster_watch_fanout_delivers_backlog_and_future_events_on_followers() {
    let (nodes, mut cluster) = cluster_with_nodes(&["kv_watch_a", "kv_watch_b", "kv_watch_c"]);
    cluster
        .put("/cluster/watch/backlog", b"backlog".to_vec())
        .expect("backlog write");
    let watch_id = cluster
        .watch_prefix(&nodes[1], "/cluster/watch/", KvRevision(1))
        .expect("watch follower");
    let backlog = cluster
        .drain_watch(&nodes[1], watch_id)
        .expect("drain backlog");
    assert_eq!(backlog.len(), 1);
    assert_eq!(backlog[0].key, "/cluster/watch/backlog");

    cluster.set_node_online(&nodes[1], false);
    cluster
        .put("/cluster/watch/missed", b"missed".to_vec())
        .expect("write while follower offline");
    assert!(
        cluster
            .drain_watch(&nodes[1], watch_id)
            .expect("offline local queue unchanged")
            .is_empty()
    );

    cluster.set_node_online(&nodes[1], true);
    let caught_up = cluster
        .drain_watch(&nodes[1], watch_id)
        .expect("drain caught up event");
    assert_eq!(caught_up.len(), 1);
    assert_eq!(caught_up[0].key, "/cluster/watch/missed");
    assert_eq!(caught_up[0].revision, KvRevision(2));
}

#[test]
fn cluster_membership_roles_exclude_pending_suspect_offline_and_removed_members() {
    let now = OffsetDateTime::now_utc();
    let local = node_id("kv_roles_local");
    let mut state = ClusterBootstrapState::initialized_locally(
        Id::from_legacy_named("cluster_kv_roles").expect("cluster"),
        "kv roles",
        local.clone(),
        "local",
        "pk-local",
        now,
    );
    state.members[0].role = ClusterMemberRole::Voter;
    state.members[0].state = ClusterMemberState::Voter;

    for (name, role, member_state) in [
        (
            "kv_roles_learner",
            ClusterMemberRole::Learner,
            ClusterMemberState::Learner,
        ),
        (
            "kv_roles_non_voting",
            ClusterMemberRole::NonVoting,
            ClusterMemberState::NonVoting,
        ),
        (
            "kv_roles_pending",
            ClusterMemberRole::Voter,
            ClusterMemberState::Pending,
        ),
        (
            "kv_roles_suspect",
            ClusterMemberRole::Voter,
            ClusterMemberState::Suspect,
        ),
        (
            "kv_roles_offline",
            ClusterMemberRole::Voter,
            ClusterMemberState::Offline,
        ),
        (
            "kv_roles_removed",
            ClusterMemberRole::Voter,
            ClusterMemberState::Removed,
        ),
    ] {
        let mut member = state.members[0].clone();
        member.node_id = node_id(name);
        member.display_name = name.to_string();
        member.node_public_key = format!("pk-{name}");
        member.role = role;
        member.state = member_state;
        member.joined_at = now;
        member.updated_at = now;
        state.members.push(member);
    }

    let config = ClusterKvRaftConfigRecord::from(&state);
    assert_eq!(config.voters, vec![local]);
    assert_eq!(config.learners, vec![node_id("kv_roles_learner")]);
    assert_eq!(config.non_voting, vec![node_id("kv_roles_non_voting")]);
    assert_eq!(cluster_kv_raft_quorum_size_for_state(&state), 1);
}

#[test]
fn cluster_txn_semantics_apply_one_branch_cluster_wide_without_partial_apply() {
    let (nodes, mut cluster) = cluster_with_nodes(&["kv_txn_a", "kv_txn_b", "kv_txn_c"]);
    let first = cluster
        .txn_response(KvTxn {
            compares: vec![KvCompare::Missing {
                key: "/cluster/txn/value".to_string(),
            }],
            success: vec![KvOp::Put {
                key: "/cluster/txn/value".to_string(),
                value: b"initial".to_vec(),
                lease_id: None,
            }],
            failure: vec![KvOp::Put {
                key: "/cluster/txn/unexpected".to_string(),
                value: b"failure".to_vec(),
                lease_id: None,
            }],
        })
        .expect("initial txn");
    assert!(first.succeeded);
    assert_eq!(first.revision, KvRevision(1));

    let second = cluster
        .txn_response(KvTxn {
            compares: vec![KvCompare::ValueEquals {
                key: "/cluster/txn/value".to_string(),
                value: b"wrong".to_vec(),
            }],
            success: vec![KvOp::Put {
                key: "/cluster/txn/unexpected".to_string(),
                value: b"success".to_vec(),
                lease_id: None,
            }],
            failure: vec![KvOp::Put {
                key: "/cluster/txn/value".to_string(),
                value: b"fallback".to_vec(),
                lease_id: None,
            }],
        })
        .expect("fallback txn");
    assert!(!second.succeeded);
    assert_eq!(second.revision, KvRevision(2));

    assert_all_nodes_value(&cluster, &nodes, "/cluster/txn/value", b"fallback");
    for node_id in &nodes {
        assert_node_missing(&cluster, node_id, "/cluster/txn/unexpected");
    }
}

#[test]
fn cluster_lease_semantics_revoke_removes_ephemeral_key_on_every_node() {
    let (nodes, mut cluster) = cluster_with_nodes(&["kv_lease_a", "kv_lease_b", "kv_lease_c"]);
    let lease_id = LeaseId(42);
    let created = cluster
        .txn_response(KvTxn {
            compares: Vec::new(),
            success: vec![
                KvOp::LeaseGrant {
                    lease_id,
                    ttl_ticks: 5,
                },
                KvOp::Put {
                    key: "/services/leased/member".to_string(),
                    value: b"online".to_vec(),
                    lease_id: Some(lease_id),
                },
            ],
            failure: Vec::new(),
        })
        .expect("create leased key");
    assert!(created.succeeded);
    assert_eq!(created.revision, KvRevision(1));
    assert_all_nodes_value(&cluster, &nodes, "/services/leased/member", b"online");

    let revoked = cluster
        .txn_response(KvTxn {
            compares: vec![KvCompare::LeaseEquals {
                key: "/services/leased/member".to_string(),
                lease_id: Some(lease_id),
            }],
            success: vec![KvOp::LeaseRevoke { lease_id }],
            failure: Vec::new(),
        })
        .expect("revoke lease");
    assert!(revoked.succeeded);
    assert_eq!(revoked.revision, KvRevision(2));
    for node_id in &nodes {
        assert_node_missing(&cluster, node_id, "/services/leased/member");
    }
}

#[test]
fn cluster_read_consistency_keeps_partitioned_follower_stale_until_healed() {
    let (nodes, mut cluster) = cluster_with_nodes(&["kv_read_a", "kv_read_b", "kv_read_c"]);
    cluster.set_node_online(&nodes[2], false);
    cluster
        .put("/cluster/read/a", b"a".to_vec())
        .expect("first write");
    cluster
        .put("/cluster/read/b", b"b".to_vec())
        .expect("second write");

    assert_eq!(
        cluster
            .store(&nodes[0])
            .expect("leader")
            .range_prefix("/cluster/read/")
            .len(),
        2
    );
    assert!(
        cluster
            .store(&nodes[2])
            .expect("offline follower")
            .range_prefix("/cluster/read/")
            .is_empty()
    );

    cluster.set_node_online(&nodes[2], true);
    assert_eq!(
        cluster
            .store(&nodes[2])
            .expect("healed follower")
            .range_prefix("/cluster/read/")
            .len(),
        2
    );
    assert_all_nodes_revision(&cluster, &nodes, KvRevision(2));
}

#[test]
fn cluster_snapshot_compaction_preserves_retained_tail_across_reopen() {
    let root = temp_root("sinomo-kv-cluster-snapshot-tail");
    let wal_paths = KvWalPaths::new(root.join("raft"));
    let local = node_id("kv_snapshot_tail");
    let snapshot_index;

    {
        let mut node =
            DurableRaftKvNode::open(local.clone(), wal_paths.clone()).expect("open durable node");
        for index in 0..300 {
            let entry = node
                .append_local_command(
                    1,
                    put_command(
                        &format!("/cluster/snapshot/before/{index:03}"),
                        format!("before-{index}").as_bytes(),
                    ),
                )
                .expect("append before snapshot");
            node.commit_to(entry.index).expect("commit before snapshot");
        }
        let snapshot = node.save_snapshot().expect("save snapshot");
        snapshot_index = snapshot.last_included_index;
        assert_eq!(snapshot_index, 300);
        let entry = node
            .append_local_command(1, put_command("/cluster/snapshot/after", b"after"))
            .expect("append after snapshot");
        node.commit_to(entry.index).expect("commit after snapshot");
    }

    let node = DurableRaftKvNode::open(local, wal_paths).expect("reopen durable node");
    assert_eq!(node.snapshot_index(), snapshot_index);
    assert_eq!(node.hard_state().commit_index, snapshot_index + 1);
    assert!(
        node.log()
            .iter()
            .any(|entry| entry.index == snapshot_index + 1),
        "post-snapshot committed tail should remain available for incremental catch-up"
    );
    assert_eq!(
        node.store()
            .get("/cluster/snapshot/after")
            .expect("post snapshot entry")
            .value,
        b"after"
    );
}

#[test]
fn cluster_rpc_contract_rejects_malformed_bodies_and_ignores_unknown_routes() {
    assert!(decode_cluster_kv_txn_rpc_request_body(b"not a sinomo object").is_err());
    let malformed_append = handle_cluster_raft_rpc_payload::<_, _, KvError>(
        CLUSTER_RAFT_APPEND_ROUTE,
        b"not a raft append payload",
        |_| panic!("malformed append must not reach handler"),
        |_| panic!("append route must not reach vote handler"),
    );
    assert!(malformed_append.is_err());

    let unknown = handle_cluster_raft_rpc_payload::<_, _, KvError>(
        "/cluster/raft/unknown",
        b"ignored",
        |_| panic!("unknown route must not call append handler"),
        |_| panic!("unknown route must not call vote handler"),
    )
    .expect("unknown route");
    assert!(unknown.is_none());
}

#[test]
fn cluster_model_randomized_deterministic_faults_match_committed_model() {
    let mut sim = ClusterSim::new(&["kv_model_a", "kv_model_b", "kv_model_c"]);
    let mut model = BTreeMap::<String, Vec<u8>>::new();
    let mut rng = 0x5eed_5eed_u64;

    for step in 0..96_u64 {
        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
        let action = (rng % 6) as usize;
        let node_index = ((rng >> 8) as usize) % sim.nodes.len();
        let key = format!("/cluster/model/key{}", (rng >> 16) % 5);

        match action {
            0 | 1 => {
                let value = format!("value-{step}-{rng}").into_bytes();
                sim.put_model(key, value, &mut model);
            }
            2 => sim.delete_model(key, &mut model),
            3 => sim.partition(node_index),
            4 => sim.heal(node_index),
            5 => sim.elect(node_index),
            _ => unreachable!("bounded action"),
        }

        sim.assert_online_matches_model(&model);
    }

    for index in 0..sim.nodes.len() {
        sim.heal(index);
    }
    sim.assert_online_matches_model(&model);
}
