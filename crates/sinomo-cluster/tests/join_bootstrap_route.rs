//! Successful join routes are transport data, never replacement membership authority.
use sinomo_base::{
    ClusterBootstrapState, ClusterInvitationChannel, ClusterJoinInvite, ClusterMemberRecord,
    ClusterMemberRole, ClusterMemberState, Id, TrustDomain,
};
use sinomo_cluster::{
    ClusterJoinRegistrationResponse, apply_cluster_raft_role, read_cluster_state_without_kv,
    reconcile_accepted_cluster_join_response, write_cluster_state,
};
use sinomo_local::SinomoDataDir;
use sinomo_network::{
    self as network, AuthenticatedDirectStream, BuiltinTcpLinkProvider, ClusterPaths,
    ConnectOptions, ConnectivityGeneration, ConnectivitySnapshot, ConnectivityTracker,
    DaemonRoutedSessionIdentity, DirectorySnapshot, DirectoryVersion, MultiHopRoutedConnector,
    NetworkHealthPolicy, RoutedConnector, RoutedSessionIdentity,
};
use sinomo_node::{ClusterId, ClusterInviteId, NodeId};
use std::{sync::Arc, time::Duration};
use time::OffsetDateTime;

struct Fixture {
    _root: sinomo_testdir::TestDir,
    local_data: SinomoDataDir,
    seed_data: SinomoDataDir,
    local: NodeId,
    seed: NodeId,
    pending: ClusterBootstrapState,
    accepted: ClusterBootstrapState,
    invite: ClusterJoinInvite,
    endpoint: sinomo_base::ClusterEndpointRecord,
}
impl Fixture {
    fn new(address: String) -> Self {
        let root = sinomo_testdir::TestDir::new("accepted-join-route");
        let local_data = SinomoDataDir::new(root.path().join("local"));
        let seed_data = SinomoDataDir::new(root.path().join("seed"));
        local_data.ensure_root_layout().unwrap();
        seed_data.ensure_root_layout().unwrap();
        let local_key = sinomo_node::load_or_create_main_node_identity(&local_data.root()).unwrap();
        let seed_key = sinomo_node::load_or_create_main_node_identity(&seed_data.root()).unwrap();
        let local = NodeId::generate();
        let seed = NodeId::generate();
        let cluster = ClusterId::generate();
        let now = OffsetDateTime::now_utc();
        let pending = ClusterBootstrapState::pending_join(
            cluster.clone(),
            "route",
            local.clone(),
            "local",
            local_key.public_key.clone(),
            now,
        );
        let mut accepted = ClusterBootstrapState::initialized_locally(
            cluster.clone(),
            "route",
            seed.clone(),
            "seed",
            seed_key.public_key.clone(),
            now,
        );
        let mut member = ClusterMemberRecord::local_voter(
            cluster.clone(),
            local.clone(),
            "local",
            local_key.public_key,
            now,
        );
        apply_cluster_raft_role(&mut member, ClusterMemberRole::Learner, now);
        accepted.members.push(member);
        // The accepting seed's listener need not have reached its published KV projection yet.
        assert!(accepted.members[0].endpoints.is_empty());
        let endpoint = network::daemon_lan_endpoint(&seed, address, 0);
        let invite = ClusterJoinInvite {
            invite_id: ClusterInviteId::generate(),
            cluster_id: cluster,
            cluster_name: Some("route".into()),
            cluster_public_key: seed_key.public_key.clone(),
            invitation_secret: "isolated-fixture".into(),
            communication_channels: vec![ClusterInvitationChannel {
                endpoint: endpoint.clone(),
                node_public_key: seed_key.public_key.clone(),
            }],
            introducers: vec![],
            token_id: "isolated-fixture".into(),
            expires_at: now + time::Duration::hours(1),
            trust_domain: TrustDomain::new("trust.test").unwrap(),
            allowed_member_roles: vec![ClusterMemberRole::Learner],
            issuer_node_id: seed.clone(),
            issuer_public_key: seed_key.public_key,
            signature: String::new(),
        };
        Self {
            _root: root,
            local_data,
            seed_data,
            local,
            seed,
            pending,
            accepted,
            invite,
            endpoint,
        }
    }
    fn response(&self) -> ClusterJoinRegistrationResponse {
        ClusterJoinRegistrationResponse {
            accepted: true,
            detail: "accepted fixture boundary".into(),
            state: self.accepted.clone(),
            kv_snapshot: None,
            responder_node_id: Some(self.seed.clone()),
        }
    }
    fn reconcile(
        &mut self,
        response: ClusterJoinRegistrationResponse,
    ) -> Option<sinomo_cluster::ClusterJoinReconciliation> {
        reconcile_accepted_cluster_join_response(
            &mut self.pending,
            &self.invite,
            &self.endpoint,
            response,
        )
        .unwrap()
    }
    fn retained_route(&self) -> bool {
        self.pending
            .members
            .iter()
            .find(|m| m.node_id == self.seed)
            .is_some_and(|m| m.endpoints.iter().any(|e| e == &self.endpoint))
    }
}

#[test]
fn accepted_numeric_ipv4_seed_route_survives_identity_restart() {
    let mut f = Fixture::new("192.0.2.8:21353".into());
    let response = f.response();
    assert!(f.reconcile(response).is_some());
    assert!(
        f.retained_route(),
        "accepted numeric introducer was lost before the replacement can dial its seed"
    );
    let paths = ClusterPaths::new(&f.local_data);
    paths.ensure().unwrap();
    write_cluster_state(&paths, &f.pending).unwrap();
    let reopened = read_cluster_state_without_kv(&paths).unwrap();
    assert_eq!(reopened.local_node_id, f.local);
    assert!(
        reopened
            .members
            .iter()
            .find(|m| m.node_id == f.seed)
            .unwrap()
            .endpoints
            .contains(&f.endpoint)
    );
}
#[test]
fn accepted_numeric_ipv6_seed_route_is_retained() {
    let mut f = Fixture::new("[2001:db8::8]:21353".into());
    let response = f.response();
    assert!(f.reconcile(response).is_some());
    assert!(f.retained_route());
}
#[test]
fn accepted_dns_seed_route_remains_supported() {
    let mut f = Fixture::new("seed.test:21353".into());
    let response = f.response();
    assert!(f.reconcile(response).is_some());
    assert!(f.retained_route());
}
#[test]
fn foreign_acceptance_cannot_install_a_numeric_seed_route() {
    let mut f = Fixture::new("192.0.2.8:21353".into());
    let before = f.pending.clone();
    let mut r = f.response();
    r.state.cluster_id = ClusterId::generate();
    assert!(f.reconcile(r).is_none());
    assert_eq!(f.pending, before);
}
#[test]
fn changed_local_identity_cannot_install_a_numeric_seed_route() {
    let mut f = Fixture::new("192.0.2.8:21353".into());
    let before = f.pending.clone();
    let mut r = f.response();
    r.state
        .members
        .iter_mut()
        .find(|m| m.node_id == f.local)
        .unwrap()
        .node_public_key = "different".into();
    assert!(f.reconcile(r).is_none());
    assert_eq!(f.pending, before);
}
#[test]
fn unaccepted_local_member_cannot_install_a_numeric_seed_route() {
    let mut f = Fixture::new("192.0.2.8:21353".into());
    let before = f.pending.clone();
    let mut r = f.response();
    r.state
        .members
        .iter_mut()
        .find(|m| m.node_id == f.local)
        .unwrap()
        .state = ClusterMemberState::Pending;
    assert!(f.reconcile(r).is_none());
    assert_eq!(f.pending, before);
}

#[derive(Clone, Copy)]
enum ReceiverControl {
    Authorized,
    RevokedCaller,
    StaleCallerKey,
    ForeignListener,
}
async fn native_tcp_route(control: ReceiverControl) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut f = Fixture::new(listener.local_addr().unwrap().to_string());
    let response = f.response();
    assert!(f.reconcile(response).is_some());
    assert!(
        f.retained_route(),
        "the native dial must use the accepted numeric route, not a fixture-only endpoint injection"
    );
    let local_paths = ClusterPaths::new(&f.local_data);
    local_paths.ensure().unwrap();
    write_cluster_state(&local_paths, &f.pending).unwrap();
    let mut server_state = f.accepted.clone();
    server_state.members[0].endpoints.push(f.endpoint.clone());
    let observed_directory =
        DirectorySnapshot::try_from_members(DirectoryVersion(1), server_state.members.clone())
            .unwrap();
    let observed_identity =
        DaemonRoutedSessionIdentity::new(f.seed.clone(), f.seed_data.clone()).unwrap();
    let observed_seed = f.seed.clone();
    let mut server_id = f.seed.clone();
    match control {
        ReceiverControl::RevokedCaller => server_state.members.retain(|m| m.node_id != f.local),
        ReceiverControl::StaleCallerKey => {
            let stale = SinomoDataDir::new(f._root.path().join("stale-key"));
            stale.ensure_root_layout().unwrap();
            server_state
                .members
                .iter_mut()
                .find(|m| m.node_id == f.local)
                .unwrap()
                .node_public_key = sinomo_node::load_or_create_main_node_identity(&stale.root())
                .unwrap()
                .public_key;
        }
        ReceiverControl::ForeignListener => {
            server_id = NodeId::generate();
            server_state.local_node_id = server_id.clone();
        }
        ReceiverControl::Authorized => {}
    }
    let server_paths = ClusterPaths::new(&f.seed_data);
    server_paths.ensure().unwrap();
    write_cluster_state(&server_paths, &server_state).unwrap();
    let local_identity: Arc<dyn RoutedSessionIdentity> =
        Arc::new(DaemonRoutedSessionIdentity::new(f.local.clone(), f.local_data.clone()).unwrap());
    let server_identity =
        DaemonRoutedSessionIdentity::new(server_id.clone(), f.seed_data.clone()).unwrap();
    let server_directory =
        DirectorySnapshot::try_from_members(DirectoryVersion(1), server_state.members).unwrap();
    let (observed, observation) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        // Calibrate the route observation with a real authenticated TCP
        // connection. The later controls change receiver authority after
        // this success; a healthy route must not preserve that old authority.
        let (mut initial, _) = listener.accept().await.map_err(|e| e.to_string())?;
        let metadata = network::read_authenticated_transport_prelude(&mut initial)
            .await
            .map_err(|e| e.to_string())?;
        network::verify_builtin_authenticated_transport_metadata(
            &metadata,
            &observed_seed,
            &observed_directory,
            &observed_identity,
        )
        .map_err(|e| e.to_string())?;
        observed.send(metadata.capabilities).unwrap();
        drop(initial);
        let (mut socket, _) = listener.accept().await.map_err(|e| e.to_string())?;
        let metadata = network::read_authenticated_transport_prelude(&mut socket)
            .await
            .map_err(|e| e.to_string())?;
        let peer = network::verify_builtin_authenticated_transport_metadata(
            &metadata,
            &server_id,
            &server_directory,
            &server_identity,
        )
        .map_err(|e| e.to_string())?;
        let stream = AuthenticatedDirectStream::accepted(
            Box::new(socket),
            server_id,
            peer,
            Id::generate(),
            metadata.channel_binding,
            metadata.capabilities,
        )
        .map_err(|e| e.to_string())?;
        let mut routed =
            network::accept_direct_routed_session(stream, &server_directory, &server_identity)
                .await
                .map_err(|e| e.to_string())?;
        let mut request = [0; 4];
        routed
            .read_exact(&mut request)
            .await
            .map_err(|e| e.to_string())?;
        if &request != b"ping" {
            return Err("unexpected application bytes".into());
        }
        routed.write_all(b"pong").await.map_err(|e| e.to_string())?;
        Ok::<_, String>(())
    });
    let reopened = read_cluster_state_without_kv(&local_paths).unwrap();
    let directory =
        DirectorySnapshot::try_from_members(DirectoryVersion(1), reopened.members).unwrap();
    let provider = Arc::new(
        BuiltinTcpLinkProvider::new(
            f.local.clone(),
            local_identity.clone(),
            NetworkHealthPolicy::new(Duration::from_secs(2), Duration::from_secs(2)),
        )
        .unwrap(),
    );
    use network::LinkProvider;
    let observed_link = provider.connect(&f.endpoint).await.unwrap();
    let observed_stream = observed_link.open_stream().await.unwrap();
    let capabilities = tokio::time::timeout(Duration::from_secs(2), observation)
        .await
        .expect("authenticated observation deadline")
        .expect("authenticated TCP accepted");
    drop(observed_stream);
    drop(observed_link);
    let now = OffsetDateTime::now_utc();
    let tracker = Arc::new(ConnectivityTracker::new(
        ConnectivitySnapshot::try_new(
            ConnectivityGeneration(1),
            1,
            now,
            [network::DirectConnectivityObservation {
                observer_node_id: f.local.clone(),
                source_node_id: f.local.clone(),
                target_node_id: f.seed.clone(),
                endpoint: f.endpoint.clone(),
                capabilities,
                state: network::DirectConnectivityState::Reachable,
                failure_reason: None,
                latency_micros: None,
                delivered_bytes_per_second: None,
                provenance: "native authenticated TCP observation".into(),
                source_generation: 1,
                directory_generation: 1,
                observed_at: now,
                expires_at: now + time::Duration::seconds(30),
            }],
        )
        .unwrap(),
    ));
    let connector = MultiHopRoutedConnector::from_snapshot(
        f.local.clone(),
        directory,
        tracker.clone(),
        tracker,
        provider,
        local_identity,
    )
    .unwrap();
    let client = async {
        let mut peer = connector
            .connect(
                f.seed.clone(),
                ConnectOptions {
                    timeout: Duration::from_secs(2),
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| e.to_string())?;
        assert_eq!(peer.peer_node_id, f.seed);
        assert_eq!(
            peer.route_trace.nodes,
            vec![f.local.clone(), f.seed.clone()]
        );
        peer.write_all(b"ping").await.map_err(|e| e.to_string())?;
        let mut response = [0; 4];
        peer.read_exact(&mut response)
            .await
            .map_err(|e| e.to_string())?;
        if &response != b"pong" {
            return Err("wrong response".into());
        }
        Ok::<_, String>(())
    };
    let (client, server) = tokio::time::timeout(Duration::from_secs(5), async {
        (client.await, server.await.unwrap())
    })
    .await
    .expect("bounded native fixture");
    match control {
        ReceiverControl::Authorized => {
            assert!(client.is_ok(), "{client:?}");
            assert!(server.is_ok(), "{server:?}");
        }
        _ => {
            assert!(client.is_err(), "unauthorized native stream accepted");
            assert!(server.is_err(), "unauthorized application bytes accepted");
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accepted_numeric_route_opens_authenticated_native_tcp() {
    native_tcp_route(ReceiverControl::Authorized).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_route_rejects_revoked_caller() {
    native_tcp_route(ReceiverControl::RevokedCaller).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_route_rejects_stale_caller_key() {
    native_tcp_route(ReceiverControl::StaleCallerKey).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_route_rejects_foreign_listener() {
    native_tcp_route(ReceiverControl::ForeignListener).await;
}
