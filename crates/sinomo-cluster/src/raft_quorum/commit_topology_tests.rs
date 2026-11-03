use super::*;
use sinomo_kv::{ClusterKvRaftConfigRecord, KvCommand, KvOp};
use time::OffsetDateTime;

fn admitted_learner_fixture(
    name: &str,
) -> (
    sinomo_testdir::TestDir,
    ClusterPaths,
    ClusterBootstrapState,
    ClusterMemberRecord,
) {
    let root = sinomo_testdir::TestDir::new(name);
    let paths = ClusterPaths::new(&sinomo_local::SinomoDataDir::new(&root));
    paths.ensure().unwrap();
    let now = OffsetDateTime::now_utc();
    let mut stale = ClusterBootstrapState::initialized_locally(
        ClusterId::from_legacy_named(name).unwrap(),
        name,
        NodeId::from_legacy_named(format!("{name}_leader")).unwrap(),
        "leader",
        "leader-key",
        now,
    );
    stale.term = 7;
    let capability =
        sinomo_base::CapabilityName::new(sinomo_node::DAEMON_MAIN_RUN_CAPABILITY).unwrap();
    stale.members[0].capabilities = vec![capability.clone()];
    stale.members[0].allowed_capabilities = vec![capability.clone()];
    let mut learner = ClusterMemberRecord::pending_learner(
        stale.cluster_id.clone(),
        NodeId::from_legacy_named(format!("{name}_learner")).unwrap(),
        "learner",
        "learner-key",
        now,
    );
    learner.state = ClusterMemberState::Learner;
    learner.capabilities = vec![capability.clone()];
    learner.allowed_capabilities = vec![capability];
    let config = ClusterKvRaftConfigRecord {
        cluster_id: stale.cluster_id.clone(),
        term: 7,
        kv_revision: 1,
        voters: vec![stale.local_node_id.clone()],
        learners: vec![learner.node_id.clone()],
        non_voting: vec![],
        updated_at: now,
    };
    let mut node = paths
        .kv_paths()
        .raft_store()
        .open_node(&stale.local_node_id)
        .unwrap();
    node.become_leader(7).unwrap();
    for op in [
        KvOp::put_serde(
            crate::keyspace::cluster_member_key(&stale.local_node_id),
            &stale.members[0],
            None,
        )
        .unwrap(),
        KvOp::put_serde(
            crate::keyspace::cluster_member_key(&learner.node_id),
            &learner,
            None,
        )
        .unwrap(),
        KvOp::put_serde(sinomo_kv::cluster_kv_raft_config_key(), &config, None).unwrap(),
    ] {
        let entry = node.append_local_command(7, KvCommand::Op(op)).unwrap();
        node.commit_to(entry.index).unwrap();
    }
    assert!(node.store().revision().0 > stale.kv_revision);
    drop(node);
    crate::write_cluster_state(&paths, &stale).unwrap();
    crate::raft_election::locked_set_cluster_raft_leader(&paths, &stale.local_node_id, 7).unwrap();
    (root, paths, stale, learner)
}

#[test]
fn sync_commit_projection_keeps_committed_learner_with_revision() {
    let (_root, paths, mut stale, learner) =
        admitted_learner_fixture("sync_commit_learner_projection");
    let before = std::fs::read(&paths.kv_paths().raft_wal_paths(&stale.local_node_id).wal).unwrap();
    project_committed_cluster_raft_authority_before_sync_commit(
        &paths,
        &mut stale,
        Instant::now() + Duration::from_secs(5),
    )
    .unwrap();
    assert_eq!(
        std::fs::read(&paths.kv_paths().raft_wal_paths(&stale.local_node_id).wal).unwrap(),
        before
    );
    assert_eq!(
        stale.members.iter().find(|m| m.node_id == learner.node_id),
        Some(&learner),
        "advancing the native revision must not omit the learner named by that committed configuration"
    );
}

#[derive(Default)]
struct CaptureFanout {
    states: Vec<ClusterBootstrapState>,
}
impl ClusterRaftQuorumAsyncHost for CaptureFanout {
    fn send_raft_append_to_node_async<'a>(
        &'a mut self,
        _paths: &'a ClusterPaths,
        _state: &'a ClusterBootstrapState,
        _target: &'a NodeId,
        _request: &'a ClusterRaftAppendRequest,
        _timeout: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterRaftAppendResponse> {
        Box::pin(async { panic!("one voter needs no remote quorum acknowledgement") })
    }
    fn fan_out_committed_cluster_raft_command_async<'a>(
        &'a mut self,
        _paths: ClusterPaths,
        state: ClusterBootstrapState,
        _events: Arc<Vec<KvEvent>>,
        _targets: Vec<NodeId>,
    ) -> ClusterMembershipRpcFuture<'a, ()> {
        self.states.push(state);
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_commit_fanout_keeps_committed_learner_with_revision() {
    let (_root, paths, mut stale, learner) =
        admitted_learner_fixture("async_commit_learner_projection");
    let request = KvCommandRequest::generated(KvCommand::Op(KvOp::Put {
        key: "/test/real-activity".into(),
        value: b"original-body".to_vec(),
        lease_id: None,
    }));
    let mut host = CaptureFanout::default();
    let result = commit_cluster_raft_command_compact_with_quorum_async_with_host(
        &tokio::runtime::Handle::current(),
        &paths,
        &mut stale,
        request,
        ClusterKvRaftQuorumCommitOptions {
            timeout: Duration::from_secs(5),
        },
        ClusterControlPlaneTimeouts::default_daemon_transport()
            .raft_append_replication_options(Duration::from_secs(5)),
        &mut host,
    )
    .await
    .unwrap();
    assert!(result.authority.commit_index > 3);
    assert_eq!(host.states.len(), 1);
    assert_eq!(
        host.states[0]
            .members
            .iter()
            .find(|m| m.node_id == learner.node_id),
        Some(&learner),
        "committed fanout must retain the admitted learner even when the caller bootstrap view is stale"
    );
    assert_eq!(
        stale.members.iter().find(|m| m.node_id == learner.node_id),
        Some(&learner),
        "the returned revision and membership must describe the same observed configuration"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_committed_learner_identity_fails_before_append_or_fanout() {
    let (_root, paths, mut stale, learner) =
        admitted_learner_fixture("missing_commit_learner_identity");
    crate::append_cluster_raft_command(
        &paths,
        &stale.local_node_id,
        KvCommand::Op(KvOp::Delete {
            key: crate::keyspace::cluster_member_key(&learner.node_id),
        }),
    )
    .unwrap();
    let wal = paths.kv_paths().raft_wal_paths(&stale.local_node_id).wal;
    let before = std::fs::read(&wal).unwrap();
    let caller = stale.clone();
    let mut host = CaptureFanout::default();
    let error = commit_cluster_raft_command_compact_with_quorum_async_with_host(
        &tokio::runtime::Handle::current(),
        &paths,
        &mut stale,
        KvCommandRequest::generated(KvCommand::Op(KvOp::Put {
            key: "/test/must-not-append".into(),
            value: b"unadmitted".to_vec(),
            lease_id: None,
        })),
        ClusterKvRaftQuorumCommitOptions {
            timeout: Duration::from_secs(5),
        },
        ClusterControlPlaneTimeouts::default_daemon_transport()
            .raft_append_replication_options(Duration::from_secs(5)),
        &mut host,
    )
    .await
    .unwrap_err();
    assert!(matches!(error, NetworkError::ClusterReadOnly { .. }));
    assert!(host.states.is_empty());
    assert_eq!(std::fs::read(wal).unwrap(), before);
    assert_eq!(stale, caller);
}

#[test]
fn missing_member_projection_uses_native_identities_and_excludes_unadmitted_rows() {
    let (_root, paths, mut stale, learner) =
        admitted_learner_fixture("native_commit_learner_identity");
    let native_local = stale.members[0].clone();
    stale.members[0].node_public_key = "stale-caller-key".into();
    let mut unadmitted = learner.clone();
    unadmitted.node_id = NodeId::from_legacy_named("unadmitted_commit_learner").unwrap();
    stale.members.push(unadmitted.clone());
    let wal = paths.kv_paths().raft_wal_paths(&stale.local_node_id).wal;
    let before = std::fs::read(&wal).unwrap();
    project_committed_cluster_raft_authority_before_sync_commit(
        &paths,
        &mut stale,
        Instant::now() + Duration::from_secs(5),
    )
    .unwrap();
    assert_eq!(stale.members.len(), 2);
    assert_eq!(
        stale
            .members
            .iter()
            .find(|m| m.node_id == native_local.node_id),
        Some(&native_local)
    );
    assert_eq!(
        stale.members.iter().find(|m| m.node_id == learner.node_id),
        Some(&learner)
    );
    assert!(
        !stale
            .members
            .iter()
            .any(|m| m.node_id == unadmitted.node_id)
    );
    assert_eq!(std::fs::read(wal).unwrap(), before);
}

#[test]
fn read_index_adoption_does_not_resurrect_removed_learner_from_bootstrap() {
    let (_root, paths, mut stale, learner) = admitted_learner_fixture("read_index_removed_learner");
    stale.members.push(learner.clone());
    let mut confirmed = stale.clone();
    confirmed
        .members
        .retain(|member| member.node_id != learner.node_id);
    let config = ClusterKvRaftConfigRecord::from(&confirmed);
    let mut node = paths
        .kv_paths()
        .raft_store()
        .open_node(&stale.local_node_id)
        .unwrap();
    let entry = node
        .append_local_command(
            7,
            KvCommand::Op(
                KvOp::put_serde(sinomo_kv::cluster_kv_raft_config_key(), &config, None).unwrap(),
            ),
        )
        .unwrap();
    node.commit_to(entry.index).unwrap();
    drop(node);
    let mut storage = LocalClusterRaftCommandStorage::new(&paths, stale.local_node_id.clone());
    let read_state = storage.read_index_state().unwrap();
    confirmed.kv_revision = read_state.revision.0;
    let fence = ClusterRaftReadIndexAuthorityFence {
        read_state,
        config_fence: storage
            .read_index_entry(&sinomo_kv::cluster_kv_raft_config_key())
            .unwrap(),
        pending_current_term_barrier: None,
        confirmed_voters: BTreeSet::from([stale.local_node_id.clone()]),
        confirmed_state: confirmed.clone(),
        unresolved_local_node_id: None,
    };
    adopt_cluster_raft_read_index_state(&mut stale, &confirmed, &fence).unwrap();
    assert_eq!(
        ReadIndexConfigurationFloor::from_state(&stale),
        ReadIndexConfigurationFloor::from_config(&config)
    );
    assert!(
        !stale
            .members
            .iter()
            .any(|member| member.node_id == learner.node_id)
    );
}
