//! Cluster-KV topology of the native runtime: the single-voter default, and
//! the opt-in voter set whose survivors re-elect when the KV leader stops.

use super::*;
use sinomo_core::kv::{KvAuthorityBasis, KvOp, KvTxn, KvTxnRequest};
use sinomo_core::service::ServiceError;

fn put(key: &str) -> KvTxnRequest {
    KvTxnRequest::generated(KvTxn {
        compares: Vec::new(),
        success: vec![KvOp::Put {
            key: key.into(),
            value: key.as_bytes().to_vec(),
            lease_id: None,
        }],
        failure: Vec::new(),
    })
}

/// The committed Raft voters as `node` itself sees them.
fn voters_seen_by(native: &DfsTestRuntime, node: &NodeId) -> Vec<NodeId> {
    let state = sinomo_core::cluster::read_cluster_consensus_state(&ClusterPaths::new(
        native.data_dir(node),
    ))
    .unwrap();
    let mut voters = sinomo_core::kv::cluster_kv_raft_voter_ids(&state).unwrap();
    voters.sort();
    voters
}

/// The KV leader `node`'s own Raft state names.
fn leader_seen_by(native: &DfsTestRuntime, node: &NodeId) -> Option<NodeId> {
    let paths = ClusterPaths::new(native.data_dir(node));
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&paths).unwrap();
    paths.kv_paths().known_raft_voter_leader_id(&state).unwrap()
}

/// `node`'s durable Raft hard state.
fn hard_state_of(native: &DfsTestRuntime, node: &NodeId) -> sinomo_core::kv::RaftHardState {
    let paths = ClusterPaths::new(native.data_dir(node));
    let _operation =
        sinomo_core::kv::lock_cluster_kv_raft_operation_for_paths(&paths.kv_paths()).unwrap();
    sinomo_core::cluster::open_cluster_raft_kv_node(&paths, node)
        .unwrap()
        .hard_state()
        .clone()
}

fn commit(
    native: &DfsTestRuntime,
    node: &NodeId,
    key: &str,
) -> Result<sinomo_core::kv::ClusterKvTxnCompactCommitResult, ServiceError> {
    let cluster = native
        .component_context(node)
        .unwrap()
        .cluster_access()
        .unwrap();
    executor().block_on(cluster.kv_commit_compact_async(put(key)))
}

/// The KV leader, the primary here, stops for good. A survivor's write finds
/// it unreachable, campaigns and commits with the other survivor; later writes
/// and reads from both survivors run at the new term, never through the
/// stopped node, and each survivor's DFS configuration publication follows.
#[test]
fn native_kv_voters_reelect_and_commit_after_the_leader_stops() {
    let (mut native, nodes) =
        DfsTestRuntime::start_cluster_with_voters("native-kv-voter-failover", 3).unwrap();
    assert!(native.kv_voters());
    let leader = native.primary.clone();
    let mut everyone = nodes.clone();
    everyone.sort();
    for node in &nodes {
        assert_eq!(voters_seen_by(&native, node), everyone, "{node}'s voters");
        assert_eq!(leader_seen_by(&native, node), Some(leader.clone()));
    }
    // The write-set fixtures change member records the same way.
    for node in &nodes {
        native
            .advertise_metadata_write_set_capability(node)
            .unwrap();
    }
    let survivors = nodes
        .iter()
        .filter(|node| **node != leader)
        .cloned()
        .collect::<Vec<_>>();
    let initial_term = commit(&native, &survivors[0], "/test/kv-voters/before")
        .unwrap()
        .authority
        .term;

    native.stop_node(&leader).unwrap();
    let after = commit(&native, &survivors[0], "/test/kv-voters/after")
        .expect("the first survivor write after the stop re-elects and commits");
    assert!(after.txn_response.as_ref().unwrap().succeeded);
    assert!(after.authority.term > initial_term, "{after:?}");
    let term = after.authority.term;
    let successor = leader_seen_by(&native, &survivors[0]).expect("a survivor leads");
    assert!(survivors.contains(&successor), "{successor} leads");
    // With the leader gone, winning needed the other survivor's vote: it was
    // delivered, granted for the successor and counted in the same term.
    let voter = survivors.iter().find(|node| **node != successor).unwrap();
    let granted = hard_state_of(&native, voter);
    assert_eq!(
        (granted.current_term, granted.voted_for.as_ref()),
        (term, Some(&successor)),
        "{granted:?}"
    );
    // The stopped node's own Raft state still names itself at the old term.
    assert_eq!(leader_seen_by(&native, &leader), Some(leader.clone()));

    for node in &survivors {
        assert_eq!(leader_seen_by(&native, node), Some(successor.clone()));
        let later = commit(&native, node, &format!("/test/kv-voters/later/{node}")).unwrap();
        assert_eq!(later.authority.term, term);
        let cluster = native
            .component_context(node)
            .unwrap()
            .cluster_access()
            .unwrap();
        let read = executor()
            .block_on(cluster.kv_get_authoritative_async("/test/kv-voters/after"))
            .unwrap();
        assert!(read.value.is_some());
        assert_eq!(
            read.authority.term, term,
            "{node} read through the successor"
        );
        assert!(
            matches!(
                read.authority.basis,
                KvAuthorityBasis::CurrentTermQuorum {
                    required_quorum: 2,
                    ..
                }
            ),
            "{:?}",
            read.authority.basis
        );
        native.wait_published_configuration_current(node).unwrap();
    }
    native.close().unwrap();
}

/// Without opting in, the bootstrap node stays the only voter. Once it stops,
/// a learner's write finds the only voter unreachable and fails as lost
/// quorum; nobody campaigns.
#[test]
fn native_single_voter_default_keeps_one_voter_and_never_elects() {
    let (mut native, nodes) = DfsTestRuntime::start_cluster("native-kv-single-voter", 3).unwrap();
    assert!(!native.kv_voters());
    let voter = native.primary.clone();
    for node in &nodes {
        assert_eq!(voters_seen_by(&native, node), vec![voter.clone()]);
        assert_eq!(leader_seen_by(&native, node), Some(voter.clone()));
    }
    let learner = nodes.iter().find(|node| **node != voter).unwrap().clone();
    commit(&native, &learner, "/test/kv-single-voter/before").unwrap();

    native.stop_node(&voter).unwrap();
    let error = commit(&native, &learner, "/test/kv-single-voter/after")
        .expect_err("a learner cannot commit without the only voter");
    assert!(
        matches!(
            &error,
            ServiceError::Network(NetworkError::ClusterReadOnly { detail })
                if detail.contains("could not forward")
        ),
        "{error:?}"
    );
    for node in &nodes {
        assert_eq!(leader_seen_by(&native, node), Some(voter.clone()));
    }
    native.close().unwrap();
}

/// A configured member that is not running is unreachable, as a dead peer is
/// through routing; a node outside the membership stays unknown.
#[test]
fn an_unavailable_kv_peer_is_unreachable_only_when_configured() {
    let nodes = DfsTestRuntime::ordered_node_ids("native-kv-unavailable-peer", 2);
    let state = sinomo_core::ClusterBootstrapState::initialized_locally(
        ClusterId::from_legacy_named("native-kv-unavailable-peer").unwrap(),
        "native-kv-unavailable-peer",
        nodes[0].clone(),
        "native-kv-unavailable-peer",
        format!("pk-{}", nodes[0]),
        OffsetDateTime::now_utc(),
    );
    assert!(matches!(
        crate::runtime_host::unavailable_test_kv_peer(&state, &nodes[0]),
        NetworkError::RetryExhausted {
            delivery_state: NetworkDeliveryState::DefinitelyNotSent,
            ..
        }
    ));
    assert!(matches!(
        crate::runtime_host::unavailable_test_kv_peer(&state, &nodes[1]),
        NetworkError::UnknownNode { node_id } if node_id == nodes[1]
    ));
}
