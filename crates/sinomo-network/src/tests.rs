use crate::prelude::*;
use crate::*;

use sinomo_core::{
    CapabilityName, ClusterCertificate, ClusterEndpointPrivacy, ClusterEndpointRecord,
    ClusterEndpointSource, ClusterMemberRecord, ClusterMemberRole, ClusterMemberState,
    ClusterNodeAvailability, ClusterNodeRolloutStatus, ClusterNodeStatusRecord,
    ClusterTransportKind, Confidence, EventEnvelope, EventKind, LatencyClass, PrivacyClass,
    RoutingMetadata, RoutingScope, Salience, Source,
};
use sinomo_events::{EventId, EventStore, InMemoryEventStore, ReplayQuery, SessionId};
use sinomo_kv::{
    InProcessKvStore, KvCommand, KvCompare, KvEntry, KvEventKind, KvOp, KvRevision, KvSnapshot,
    KvTxn, LeaseId, cluster_member_key,
};
use sinomo_node::NodeId;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Instant;

#[test]
fn object_rpc_router_exposes_binary_routes_and_catalog_metadata() {
    use tower_service::Service;

    let category = object_rpc_category!(
        "cluster.kv",
        "Cluster KV control routes",
        [
            route(
                "/cluster/kv/increment",
                "Increment a test counter",
                u64 => u64,
                observe("counter" => "sinomo_network_rpc_requests_total")
            )
        ]
    );
    let increment_route = category.routes[0].clone();
    let mut router = ObjectRpcRouter::new()
        .register_category_definition(category)
        .register_route(increment_route, |request| async move {
            let value: u64 = request.decode_body()?;
            ObjectRpcResponse::encode_body(request.route, &(value + 1))
        });

    assert!(router.catalog().categories.contains_key("cluster.kv"));
    assert!(
        router
            .catalog()
            .routes
            .contains_key("/cluster/kv/increment")
    );
    let descriptor = router
        .catalog()
        .routes
        .get("/cluster/kv/increment")
        .expect("route descriptor");
    assert_eq!(descriptor.input_type, "u64");
    assert_eq!(descriptor.output_type, "u64");

    let request = ObjectRpcRequest::encode_body("/cluster/kv/increment", "cluster.kv", &41_u64)
        .expect("request");
    assert!(request.body.starts_with(sinomo_object::MAGIC));

    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let response = runtime
        .block_on(router.call(request))
        .expect("route response");
    assert!(response.body.starts_with(sinomo_object::MAGIC));
    assert_eq!(response.decode_body::<u64>().expect("decode"), 42);
}

#[test]
fn object_rpc_frame_transport_serves_binary_request_response() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let (mut client, mut server) = tokio::io::duplex(4096);
        let mut router = ObjectRpcRouter::new().register_route(
            ObjectRouteDescriptor::for_types::<u64, u64>(
                "/cluster/kv/increment",
                "cluster.kv",
                "Increment a test counter",
            ),
            |request| async move {
                let value: u64 = request.decode_body()?;
                ObjectRpcResponse::encode_body(request.route, &(value + 1))
            },
        );
        let server_task = tokio::spawn(async move {
            serve_object_rpc_frame_async(&mut server, &mut router, 4096)
                .await
                .expect("serve frame");
        });
        let request = ObjectRpcRequest::encode_body("/cluster/kv/increment", "cluster.kv", &41_u64)
            .expect("request");
        let response = call_object_rpc_frame_async(&mut client, &request, 4096)
            .await
            .expect("response");

        server_task.await.expect("server task");
        assert_eq!(response.decode_body::<u64>().expect("decode"), 42);
    });
}

#[test]
fn async_object_frame_helpers_round_trip_binary_payloads() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let (mut client, mut server) = tokio::io::duplex(4096);
        let writer = tokio::spawn(async move {
            write_object_frame_async(&mut client, &DaemonControlRequest::new("RPC /test/status"))
                .await
                .expect("write frame");
        });
        let request: DaemonControlRequest = read_object_frame_async(&mut server, 4096)
            .await
            .expect("read frame");
        writer.await.expect("writer task");
        assert_eq!(request.command, "RPC /test/status");
    });
}

#[test]
fn async_daemon_control_client_round_trips_object_frames() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let address = listener.local_addr().expect("address").to_string();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let request: DaemonControlRequest = read_object_frame_async(&mut stream, 4096)
                .await
                .expect("read request");
            assert_eq!(request.command, "RPC /test/status");
            assert_eq!(request.rpc.as_ref().expect("rpc").route, "/test/status");
            let rpc_response = ObjectRpcResponse::encode_body("/test/status", &"OK".to_string())
                .expect("rpc response");
            write_object_frame_async(
                &mut stream,
                &DaemonControlResponse::rpc(String::new(), rpc_response),
            )
            .await
            .expect("write response");
        });
        let request =
            ObjectRpcRequest::encode_body("/test/status", "test", &()).expect("rpc request");
        let response = send_daemon_rpc_raw_async(
            &address,
            "RPC /test/status",
            None,
            &request,
            std::time::Duration::from_secs(1),
        )
        .await
        .expect("response");
        server.await.expect("server");
        assert_eq!(response.decode_body::<String>().expect("decode"), "OK");
    });
}

#[test]
fn async_daemon_rpc_client_rejects_non_rpc_response() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let address = listener.local_addr().expect("address").to_string();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let request: DaemonControlRequest = read_object_frame_async(&mut stream, 4096)
                .await
                .expect("read request");
            assert_eq!(request.command, "RPC /test/status");
            write_object_frame_async(&mut stream, &DaemonControlResponse::new("OK"))
                .await
                .expect("write response");
        });
        let request =
            ObjectRpcRequest::encode_body("/test/status", "test", &()).expect("rpc request");
        let error = send_daemon_rpc_raw_async(
            &address,
            "RPC /test/status",
            None,
            &request,
            std::time::Duration::from_secs(1),
        )
        .await
        .expect_err("non-rpc response");
        server.await.expect("server");
        assert!(error.to_string().contains("non-rpc response"));
    });
}

#[test]
fn lan_ip_filter_rejects_non_invitable_addresses() {
    assert!(!is_invitable_ip("127.0.0.1".parse().expect("loopback")));
    assert!(!is_invitable_ip("0.0.0.0".parse().expect("unspecified")));
    assert!(!is_invitable_ip("224.0.0.1".parse().expect("multicast")));
    assert!(is_invitable_ip("192.168.1.10".parse().expect("lan")));
}

#[test]
fn invite_channel_uri_uses_compact_iroh_endpoint() {
    let cluster_id = ClusterId::from_legacy_named("cluster_invite_full_iroh").expect("cluster");
    let node_id = NodeId::from_legacy_named("node_invite_full_iroh").expect("node");
    let endpoint = ClusterEndpointRecord {
        endpoint_id: stable_iroh_endpoint_id(&node_id, "iroh_public_key_test"),
        node_id: node_id.clone(),
        transport: ClusterTransportKind::Iroh,
        address: "iroh://iroh_public_key_test?relay=https://relay.example./".to_string(),
        source: ClusterEndpointSource::Rendezvous,
        privacy: ClusterEndpointPrivacy::Relayed,
        priority: 90,
        last_observed_latency_ms: Some(42),
    };
    let invite = ClusterJoinInvite {
        invite_id: ClusterInviteId::from_legacy_named("invite_full_iroh").expect("invite"),
        cluster_id,
        cluster_name: Some("full iroh".to_string()),
        cluster_public_key: "cluster-pk".to_string(),
        invitation_secret: "secret".to_string(),
        communication_channels: vec![ClusterInvitationChannel {
            endpoint: endpoint.clone(),
            node_public_key: "node-pk".to_string(),
        }],
        introducers: Vec::new(),
        token_id: "token".to_string(),
        expires_at: OffsetDateTime::UNIX_EPOCH,
        trust_domain: TrustDomain::new("trust.local").expect("trust domain"),
        allowed_member_roles: vec![ClusterMemberRole::Learner],
        issuer_node_id: node_id.clone(),
        issuer_public_key: "issuer-pk".to_string(),
        signature: String::new(),
    };

    let uri = encode_cluster_join_invite(&invite).expect("encode invite");
    let decoded = decode_cluster_join_invite(&uri).expect("decode invite");

    assert!(!uri.contains(node_id.as_str()));
    assert!(!uri.contains(endpoint.endpoint_id.as_str()));
    assert_eq!(decoded.communication_channels.len(), 1);
    assert_eq!(
        decoded.communication_channels[0].endpoint.transport,
        endpoint.transport
    );
    assert_eq!(
        decoded.communication_channels[0].endpoint.address,
        endpoint.address
    );
    assert_eq!(decoded.communication_channels[0].node_public_key, "node-pk");
}

#[test]
fn explicit_iroh_invite_introducer_keeps_iroh_transport() {
    let node_id = NodeId::from_legacy_named("node_explicit_iroh_intro").expect("node");
    let endpoint = invite_introducer_endpoint(
        &node_id,
        "iroh://iroh_public_key_intro?relay=https://relay.example./".to_string(),
    );

    assert_eq!(endpoint.transport, ClusterTransportKind::Iroh);
    assert_eq!(endpoint.node_id, node_id);
    assert_eq!(endpoint.privacy, ClusterEndpointPrivacy::Relayed);
    assert_eq!(
        endpoint.endpoint_id,
        stable_iroh_endpoint_id(&endpoint.node_id, "iroh_public_key_intro")
    );
}

#[test]
fn explicit_iroh_invite_introducer_only_treats_top_level_direct_query_as_direct() {
    let node_id = NodeId::from_legacy_named("node_iroh_intro_direct_query").expect("node");
    let relayed = invite_introducer_endpoint(
        &node_id,
        "iroh://iroh_public_key_intro_direct?relay=https://relay.example./?direct=1".to_string(),
    );
    let direct = invite_introducer_endpoint(
        &node_id,
        "iroh://iroh_public_key_intro_direct?direct=203.0.113.10:21355&relay=https://relay.example./".to_string(),
    );

    assert_eq!(relayed.privacy, ClusterEndpointPrivacy::Relayed);
    assert_eq!(direct.privacy, ClusterEndpointPrivacy::InternetDirect);
    assert!(direct.priority < relayed.priority);
}

#[test]
fn default_invite_compaction_keeps_fresh_iroh_before_later_peers() {
    let local = NodeId::from_legacy_named("node_invite_compact_local").expect("local");
    let peer = NodeId::from_legacy_named("node_invite_compact_peer").expect("peer");
    let mut introducers = vec![
        invite_introducer_endpoint(&local, "10.0.0.41:21353".to_string()),
        invite_introducer_endpoint(&local, "203.0.113.41:21353".to_string()),
        invite_introducer_endpoint(
            &local,
            "iroh://iroh_public_key_local?direct=203.0.113.41:21355&relay=https://relay.example./"
                .to_string(),
        ),
        invite_introducer_endpoint(
            &peer,
            "iroh://iroh_public_key_peer?direct=198.51.100.10:21355&relay=https://relay.example./"
                .to_string(),
        ),
    ];

    sort_default_invite_introducers(&mut introducers);
    compact_default_invite_introducers(&mut introducers);

    assert_eq!(introducers.len(), 3);
    assert_eq!(introducers[0].address, "203.0.113.41:21353");
    assert_eq!(introducers[2].node_id, local);
    assert!(
        introducers[2]
            .address
            .starts_with("iroh://iroh_public_key_local")
    );
}

#[test]
fn default_invite_sort_prefers_public_direct_over_private_direct() {
    let node = NodeId::from_legacy_named("node_invite_public_direct").expect("node");
    let private = invite_introducer_endpoint(&node, "10.0.0.41:21353".to_string());
    let public = invite_introducer_endpoint(&node, "203.0.113.41:21353".to_string());
    let mut introducers = vec![private, public.clone()];

    sort_default_invite_introducers(&mut introducers);

    assert_eq!(introducers.first(), Some(&public));
}

#[test]
fn join_registration_channels_prioritize_iroh_before_direct_lan() {
    let cluster_id = ClusterId::from_legacy_named("cluster_invite_iroh_first").expect("cluster");
    let node_id = NodeId::from_legacy_named("node_invite_iroh_first").expect("node");
    let lan_endpoint = ClusterEndpointRecord {
        endpoint_id: ClusterEndpointId::from_legacy_named("endpoint_invite_iroh_first_lan")
            .expect("endpoint"),
        node_id: node_id.clone(),
        transport: ClusterTransportKind::LanTcp,
        address: "192.0.2.10:21353".to_string(),
        source: ClusterEndpointSource::Static,
        privacy: ClusterEndpointPrivacy::Lan,
        priority: 20,
        last_observed_latency_ms: None,
    };
    let iroh_endpoint = ClusterEndpointRecord {
        endpoint_id: ClusterEndpointId::from_legacy_named("endpoint_invite_iroh_first_iroh")
            .expect("endpoint"),
        node_id: node_id.clone(),
        transport: ClusterTransportKind::Iroh,
        address: "iroh://iroh_public_key_first?relay=https://relay.example./".to_string(),
        source: ClusterEndpointSource::Rendezvous,
        privacy: ClusterEndpointPrivacy::Relayed,
        priority: 90,
        last_observed_latency_ms: None,
    };
    let invite = ClusterJoinInvite {
        invite_id: ClusterInviteId::from_legacy_named("invite_iroh_first").expect("invite"),
        cluster_id,
        cluster_name: Some("iroh first".to_string()),
        cluster_public_key: "cluster-pk".to_string(),
        invitation_secret: "secret".to_string(),
        communication_channels: vec![
            ClusterInvitationChannel {
                endpoint: lan_endpoint,
                node_public_key: "node-pk".to_string(),
            },
            ClusterInvitationChannel {
                endpoint: iroh_endpoint,
                node_public_key: "node-pk".to_string(),
            },
        ],
        introducers: Vec::new(),
        token_id: "token".to_string(),
        expires_at: OffsetDateTime::UNIX_EPOCH,
        trust_domain: TrustDomain::new("trust.local").expect("trust domain"),
        allowed_member_roles: vec![ClusterMemberRole::Learner],
        issuer_node_id: node_id,
        issuer_public_key: "issuer-pk".to_string(),
        signature: String::new(),
    };

    let channels = invite_channels_for_join_registration(&invite);

    assert_eq!(channels.len(), 2);
    assert_eq!(channels[0].endpoint.transport, ClusterTransportKind::Iroh);
    assert_eq!(channels[1].endpoint.transport, ClusterTransportKind::LanTcp);
}

#[test]
fn command_target_selection_orders_bounded_transport_before_iroh_fallback() {
    let node_id = NodeId::from_legacy_named("node_target_iroh_order").expect("node");
    let direct = daemon_lan_endpoint(&node_id, "192.0.2.45:21353".to_string(), 20);
    let iroh = ClusterEndpointRecord {
        endpoint_id: stable_iroh_endpoint_id(&node_id, "iroh_public_key_order"),
        node_id: node_id.clone(),
        transport: ClusterTransportKind::Iroh,
        address: "iroh://iroh_public_key_order?relay=https://relay.example./".to_string(),
        source: ClusterEndpointSource::Rendezvous,
        privacy: ClusterEndpointPrivacy::Relayed,
        priority: 90,
        last_observed_latency_ms: None,
    };

    let selected = prefer_bounded_daemon_command_targets(vec![direct.clone(), iroh.clone()]);

    assert_eq!(selected.first(), Some(&direct));
    assert!(selected.contains(&iroh));
}

#[test]
fn command_target_selection_preserves_connectivity_order_for_primary_endpoint() {
    let node_id = NodeId::from_legacy_named("node_target_connectivity_order").expect("node");
    let direct = daemon_lan_endpoint(&node_id, "192.0.2.46:21353".to_string(), 20);
    let iroh = ClusterEndpointRecord {
        endpoint_id: stable_iroh_endpoint_id(&node_id, "iroh_public_key_connectivity_order"),
        node_id: node_id.clone(),
        transport: ClusterTransportKind::Iroh,
        address: "iroh://iroh_public_key_connectivity_order?relay=https://relay.example./"
            .to_string(),
        source: ClusterEndpointSource::Rendezvous,
        privacy: ClusterEndpointPrivacy::Relayed,
        priority: 90,
        last_observed_latency_ms: None,
    };

    let selected = prefer_bounded_daemon_command_targets(vec![iroh.clone(), direct.clone()]);

    assert_eq!(selected.first(), Some(&iroh));
    assert!(selected.contains(&direct));
}

#[test]
fn command_target_selection_keeps_iroh_fallback_after_direct_endpoint() {
    let node_id = NodeId::from_legacy_named("node_target_iroh_fallback").expect("node");
    let direct = daemon_lan_endpoint(&node_id, "192.0.2.44:21353".to_string(), 20);
    let iroh = ClusterEndpointRecord {
        endpoint_id: stable_iroh_endpoint_id(&node_id, "iroh_public_key_fallback"),
        node_id: node_id.clone(),
        transport: ClusterTransportKind::Iroh,
        address: "iroh://iroh_public_key_fallback?relay=https://relay.example./".to_string(),
        source: ClusterEndpointSource::Rendezvous,
        privacy: ClusterEndpointPrivacy::Relayed,
        priority: 90,
        last_observed_latency_ms: None,
    };

    let selected = prefer_bounded_daemon_command_targets(vec![direct.clone(), iroh.clone()]);

    assert!(selected.contains(&direct));
    assert!(selected.contains(&iroh));
}

#[test]
fn command_target_selection_tries_iroh_before_extra_tcp_fallbacks() {
    let node_id = NodeId::from_legacy_named("node_target_iroh_before_tcp").expect("node");
    let primary = daemon_lan_endpoint(&node_id, "203.0.113.44:21353".to_string(), 20);
    let extra_tcp = daemon_lan_endpoint(&node_id, "10.0.0.44:21353".to_string(), 30);
    let iroh = ClusterEndpointRecord {
        endpoint_id: stable_iroh_endpoint_id(&node_id, "iroh_public_key_before_tcp"),
        node_id: node_id.clone(),
        transport: ClusterTransportKind::Iroh,
        address: "iroh://iroh_public_key_before_tcp?relay=https://relay.example./".to_string(),
        source: ClusterEndpointSource::Rendezvous,
        privacy: ClusterEndpointPrivacy::Relayed,
        priority: 90,
        last_observed_latency_ms: None,
    };

    let selected = prefer_bounded_daemon_command_targets(vec![
        primary.clone(),
        extra_tcp.clone(),
        iroh.clone(),
    ]);

    assert_eq!(selected[0], primary);
    assert_eq!(selected[1], iroh);
    assert_eq!(selected[2], extra_tcp);
}

#[test]
fn daemon_member_endpoint_publish_replaces_stale_tcp_listener_advertisements() {
    let now = OffsetDateTime::now_utc();
    let cluster_id = ClusterId::from_legacy_named("cluster_replace_stale_tcp").expect("cluster");
    let node_id = NodeId::from_legacy_named("node_replace_stale_tcp").expect("node");
    let mut state = ClusterBootstrapState::initialized_locally(
        cluster_id,
        "replace stale tcp",
        node_id.clone(),
        "node",
        "pk",
        now,
    );
    let stale_lan = daemon_lan_endpoint(&node_id, "10.0.0.41:21353".to_string(), 20);
    let current_loopback = daemon_loopback_endpoint(&node_id, "127.0.0.1:21353".to_string(), 0);
    let iroh = ClusterEndpointRecord {
        endpoint_id: stable_iroh_endpoint_id(&node_id, "iroh_public_key_replace_tcp"),
        node_id: node_id.clone(),
        transport: ClusterTransportKind::Iroh,
        address: "iroh://iroh_public_key_replace_tcp?relay=https://relay.example./".to_string(),
        source: ClusterEndpointSource::Rendezvous,
        privacy: ClusterEndpointPrivacy::Relayed,
        priority: 90,
        last_observed_latency_ms: None,
    };

    assert!(ensure_main_daemon_cluster_member(
        &mut state,
        vec![stale_lan.clone(), iroh.clone()],
        now,
    ));
    assert!(ensure_main_daemon_cluster_member(
        &mut state,
        vec![current_loopback.clone()],
        now + time::Duration::seconds(1),
    ));

    let member = state
        .members
        .iter()
        .find(|member| member.node_id == node_id)
        .expect("local member");
    assert!(member.endpoints.contains(&current_loopback));
    assert!(member.endpoints.contains(&iroh));
    assert!(
        !member.endpoints.contains(&stale_lan),
        "old TCP listener advertisements must be removed when the daemon bind changes"
    );
}

#[test]
fn iroh_daemon_proxy_uses_long_timeout_for_rollout_build() {
    let request =
        ObjectRpcRequest::encode_body(ROLLOUT_BUILD_ROUTE, ROLLOUT_RPC_CATEGORY, &"build request")
            .expect("request");
    let control = DaemonControlRequest::rpc(format!("RPC {ROLLOUT_BUILD_ROUTE}"), request);
    let mut payload = Vec::new();
    write_object_frame_blocking(&mut payload, &control).expect("frame");

    assert!(iroh_daemon_command_forward_timeout(&payload) >= std::time::Duration::from_secs(600));
}

#[test]
fn iroh_daemon_proxy_keeps_short_timeout_for_regular_commands() {
    let mut payload = Vec::new();
    write_object_frame_blocking(&mut payload, &DaemonControlRequest::new("status")).expect("frame");

    assert_eq!(
        iroh_daemon_command_forward_timeout(&payload),
        std::time::Duration::from_secs(10)
    );
}

#[test]
fn connectivity_sort_prefers_healthy_iroh_over_unreachable_tcp() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-connectivity-sort-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("paths");
    let now = OffsetDateTime::now_utc();
    let cluster_id = ClusterId::from_legacy_named("cluster_connectivity_sort").expect("cluster");
    let node_a = NodeId::from_legacy_named("node_connectivity_sort_a").expect("node a");
    let node_b = NodeId::from_legacy_named("node_connectivity_sort_b").expect("node b");
    let mut state = ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "connectivity sort",
        node_a.clone(),
        "node a",
        "pk-a",
        now,
    );
    let tcp = daemon_lan_endpoint(&node_b, "192.0.2.55:21353".to_string(), 20);
    let iroh = ClusterEndpointRecord {
        endpoint_id: stable_iroh_endpoint_id(&node_b, "iroh_public_key_sort"),
        node_id: node_b.clone(),
        transport: ClusterTransportKind::Iroh,
        address: "iroh://iroh_public_key_sort?relay=https://relay.example./".to_string(),
        source: ClusterEndpointSource::Rendezvous,
        privacy: ClusterEndpointPrivacy::Relayed,
        priority: 90,
        last_observed_latency_ms: None,
    };
    let mut member_b =
        ClusterMemberRecord::local_voter(cluster_id.clone(), node_b.clone(), "node b", "pk-b", now);
    member_b.capabilities = main_daemon_capabilities(&member_b.role);
    member_b.allowed_capabilities = member_b.capabilities.clone();
    member_b.endpoints = vec![tcp.clone(), iroh.clone()];
    state.members.push(member_b);
    write_cluster_state(&paths, &state).expect("write state");
    sync_cluster_state_to_local_raft_unchecked(&paths, &state).expect("sync state");
    sync_cluster_connectivity_records_to_kv(
        &paths,
        &node_a,
        vec![
            cluster_connectivity_record(
                &state,
                &tcp,
                ClusterConnectivityState::Unreachable,
                None,
                3,
                None,
                Some("failed".to_string()),
                now,
                60,
            ),
            cluster_connectivity_record(
                &state,
                &iroh,
                ClusterConnectivityState::Healthy,
                Some(50),
                0,
                Some("ok".to_string()),
                None,
                now,
                60,
            ),
        ],
    )
    .expect("connectivity records");

    let targets = cluster_sync_targets_with_connectivity(&paths, &state);

    assert_eq!(
        targets.first().map(|endpoint| &endpoint.endpoint_id),
        Some(&iroh.endpoint_id)
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn connectivity_sort_prefers_private_tcp_over_unknown_public_tcp() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-public-tcp-sort-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("paths");
    let now = OffsetDateTime::now_utc();
    let cluster_id = ClusterId::from_legacy_named("cluster_public_tcp_sort").expect("cluster");
    let node_a = NodeId::from_legacy_named("node_public_tcp_sort_a").expect("node a");
    let node_b = NodeId::from_legacy_named("node_public_tcp_sort_b").expect("node b");
    let mut state = ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "public tcp sort",
        node_a,
        "node a",
        "pk-a",
        now,
    );
    let private = daemon_lan_endpoint(&node_b, "10.0.0.41:21353".to_string(), 10);
    let public = daemon_lan_endpoint(&node_b, "203.0.113.41:21353".to_string(), 20);
    let mut member_b = ClusterMemberRecord::local_voter(cluster_id, node_b, "node b", "pk-b", now);
    member_b.capabilities = main_daemon_capabilities(&member_b.role);
    member_b.allowed_capabilities = member_b.capabilities.clone();
    member_b.endpoints = vec![private.clone(), public.clone()];
    state.members.push(member_b);

    let targets = cluster_sync_targets_with_connectivity(&paths, &state);

    assert_eq!(
        targets.first().map(|endpoint| &endpoint.endpoint_id),
        Some(&private.endpoint_id)
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn public_ip_endpoint_is_marked_internet_direct() {
    let node_id = NodeId::from_legacy_named("node_public_endpoint").expect("node");
    let endpoints = daemon_advertised_endpoints_from_addresses(
        &node_id,
        "127.0.0.1:21353".to_string(),
        "0.0.0.0",
        21353,
        0,
        &["10.0.0.41".parse().expect("private")],
        &["129.146.106.76".parse().expect("public")],
    );

    assert!(endpoints.iter().any(|endpoint| {
        endpoint.address == "129.146.106.76:21353"
            && endpoint.privacy == ClusterEndpointPrivacy::InternetDirect
    }));
}

#[test]
fn public_ip_discovery_can_be_disabled_for_shadow_endpoint_advertisements() {
    let node_id = NodeId::from_legacy_named("node_shadow_public_endpoint").expect("node");
    let endpoints = daemon_advertised_endpoints_from_addresses_with_public_discovery(
        &node_id,
        "127.0.0.1:21354".to_string(),
        "0.0.0.0",
        21354,
        10,
        &["10.0.0.41".parse().expect("private")],
        &["129.146.106.76".parse().expect("public")],
        false,
    );

    assert!(
        endpoints
            .iter()
            .any(|endpoint| endpoint.address == "10.0.0.41:21354")
    );
    assert!(!endpoints.iter().any(|endpoint| {
        endpoint.address == "129.146.106.76:21354"
            && endpoint.privacy == ClusterEndpointPrivacy::InternetDirect
    }));
}

#[test]
fn default_iroh_relay_urls_match_iroh_n0_preset_hosts() {
    for relay_url in default_sinomo_iroh_relay_urls() {
        let host = relay_url
            .strip_prefix("https://")
            .and_then(|rest| rest.split('/').next())
            .expect("https relay host");
        assert!(
            host.ends_with(".relay.n0.iroh-canary.iroh.link."),
            "default relay should follow the Iroh N0 preset relay map: {relay_url}"
        );
    }
}

#[cfg(feature = "iroh-transport")]
#[test]
fn iroh_daemon_proxy_round_trips_rpc_request() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind daemon fixture");
    let daemon_address = listener
        .local_addr()
        .expect("daemon fixture address")
        .to_string();
    let fixture = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept daemon fixture");
        let request: crate::DaemonControlRequest =
            crate::read_object_frame_blocking(&mut stream, crate::IROH_DAEMON_COMMAND_MAX_BYTES)
                .expect("read daemon fixture request");
        assert_eq!(request.command, "RPC /test/ping");
        assert_eq!(request.rpc.as_ref().expect("rpc").route, "/test/ping");
        let rpc_response = crate::ObjectRpcResponse::encode_body("/test/ping", &"PONG".to_string())
            .expect("rpc response");
        crate::write_object_frame_blocking(
            &mut stream,
            &crate::DaemonControlResponse::rpc(String::new(), rpc_response),
        )
        .expect("write daemon fixture response");
    });

    let root = std::env::temp_dir().join(format!(
        "sinomo-network-iroh-proxy-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
    paths.ensure().expect("cluster paths");
    let record = start_iroh_daemon_proxy(
        paths.clone(),
        ClusterId::from_legacy_named("cluster_iroh_proxy").expect("cluster"),
        NodeId::from_legacy_named("node_iroh_proxy").expect("node"),
        daemon_address,
    )
    .expect("start iroh proxy");

    let request = ObjectRpcRequest::encode_body("/test/ping", "test", &()).expect("rpc request");
    let response = send_daemon_rpc_to_cluster_endpoint(
        &record.endpoint(),
        "RPC /test/ping",
        None,
        &request,
        std::time::Duration::from_secs(5),
    )
    .expect("send iroh daemon rpc");

    assert_eq!(response.decode_body::<String>().expect("decode"), "PONG");
    fixture.join().expect("daemon fixture");
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sinomo_default_iroh_relays_use_stable_network_hosts() {
    let urls = crate::roaming::sinomo_iroh_relay_url_strings_from_env(None);

    assert_eq!(
        urls,
        vec![
            "https://use1-1.relay.n0.iroh-canary.iroh.link./".to_string(),
            "https://usw1-1.relay.n0.iroh-canary.iroh.link./".to_string(),
            "https://euc1-1.relay.n0.iroh-canary.iroh.link./".to_string(),
            "https://aps1-1.relay.n0.iroh-canary.iroh.link./".to_string(),
        ]
    );
    assert!(urls.iter().all(|url| url.contains("iroh-canary")));
}

#[test]
fn sinomo_iroh_relay_env_ignores_empty_entries() {
    let urls = crate::roaming::sinomo_iroh_relay_url_strings_from_env(Some(
        " https://relay-a.example./, ,https://relay-b.example./ ".to_string(),
    ));

    assert_eq!(
        urls,
        vec![
            "https://relay-a.example./".to_string(),
            "https://relay-b.example./".to_string(),
        ]
    );
}

#[test]
fn uncommitted_raft_append_does_not_rewrite_materialized_snapshot() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-uncommitted-raft-snapshot-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");
    let node_id = NodeId::from_legacy_named("node_uncommitted_snapshot").expect("node");

    append_cluster_raft_command(
        &paths,
        &node_id,
        KvCommand::Op(KvOp::Put {
            key: "/committed".to_string(),
            value: b"committed".to_vec(),
            lease_id: None,
        }),
    )
    .expect("seed committed snapshot");
    std::fs::write(&paths.kv_snapshot, b"sentinel snapshot").expect("sentinel snapshot");

    append_cluster_raft_command_uncommitted(
        &paths,
        &node_id,
        KvCommand::Op(KvOp::Put {
            key: "/uncommitted".to_string(),
            value: b"uncommitted".to_vec(),
            lease_id: None,
        }),
    )
    .expect("append uncommitted");

    assert_eq!(
        std::fs::read(&paths.kv_snapshot).expect("read snapshot"),
        b"sentinel snapshot"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn cluster_kv_store_reads_materialized_snapshot_without_opening_raft() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-kv-store-snapshot-first-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");
    let node_id = NodeId::from_legacy_named("node_snapshot_first").expect("node");

    append_cluster_raft_command(
        &paths,
        &node_id,
        KvCommand::Op(KvOp::Put {
            key: "/raft-only".to_string(),
            value: b"raft".to_vec(),
            lease_id: None,
        }),
    )
    .expect("seed raft");

    let mut snapshot_store = InProcessKvStore::new();
    snapshot_store
        .put("/snapshot-only", b"snapshot".to_vec(), None)
        .expect("seed snapshot store");
    write_cluster_kv_snapshot(&paths, &snapshot_store.snapshot()).expect("write snapshot");

    let store = read_cluster_kv_store(&paths).expect("read cluster kv store");
    assert!(store.get("/snapshot-only").is_some());
    assert!(store.get("/raft-only").is_none());
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn stale_cluster_kv_restore_refreshes_materialized_cache_from_raft() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-kv-stale-restore-refresh-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");
    let node_id = NodeId::from_legacy_named("node_stale_restore_refresh").expect("node");

    append_cluster_raft_command(
        &paths,
        &node_id,
        KvCommand::Op(KvOp::Put {
            key: "/current".to_string(),
            value: b"current".to_vec(),
            lease_id: None,
        }),
    )
    .expect("seed raft");
    let current = read_cluster_kv_snapshot(&paths).expect("current snapshot");

    let stale = InProcessKvStore::new().snapshot();
    write_cluster_kv_snapshot(&paths, &stale).expect("write stale cache");
    assert_eq!(
        read_cluster_kv_snapshot(&paths)
            .expect("stale cache")
            .revision,
        stale.revision
    );

    assert!(
        !restore_cluster_raft_from_snapshot_if_newer(&paths, &node_id, stale)
            .expect("restore stale snapshot")
    );
    let refreshed = read_cluster_kv_snapshot(&paths).expect("refreshed cache");
    assert_eq!(refreshed.revision, current.revision);
    assert!(
        InProcessKvStore::restore(refreshed)
            .get("/current")
            .is_some()
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn raft_append_request_omits_snapshot_for_up_to_date_compacted_follower() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-raft-append-compacted-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");
    let cluster_id =
        ClusterId::from_legacy_named("cluster_raft_append_compacted").expect("cluster");
    let node_id = NodeId::from_legacy_named("node_raft_append_compacted").expect("node");
    let state = ClusterBootstrapState::initialized_locally(
        cluster_id,
        "raft append compacted",
        node_id.clone(),
        "raft node",
        "public-key",
        OffsetDateTime::now_utc(),
    );

    for index in 0..130 {
        append_cluster_raft_command(
            &paths,
            &node_id,
            KvCommand::Op(KvOp::Put {
                key: format!("/raft/compacted/{index:03}"),
                value: index.to_string().into_bytes(),
                lease_id: None,
            }),
        )
        .expect("append command");
    }
    let mut node = open_cluster_raft_kv_node(&paths, &node_id).expect("open raft node");
    node.save_snapshot().expect("compact raft node");
    assert!(node.snapshot_index() > 0);
    assert!(node.log().is_empty());
    let snapshot_index = node.snapshot_index();
    let last_log_index = node.last_log_index();
    drop(node);

    let heartbeat = build_cluster_raft_append_request_from_next_index(
        &paths,
        &state,
        last_log_index.saturating_add(1),
    )
    .expect("build heartbeat");
    let catchup = build_cluster_raft_append_request_from_next_index(&paths, &state, snapshot_index)
        .expect("build catchup");

    assert!(heartbeat.snapshot.is_none());
    assert!(heartbeat.append.entries.is_empty());
    assert_eq!(heartbeat.append.prev_log_index, snapshot_index);
    assert!(catchup.snapshot.is_some());
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn connectivity_sort_prefers_unknown_iroh_over_unknown_private_tcp() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-unknown-iroh-sort-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("paths");
    let now = OffsetDateTime::now_utc();
    let cluster_id = ClusterId::from_legacy_named("cluster_unknown_iroh_sort").expect("cluster");
    let node_a = NodeId::from_legacy_named("node_unknown_iroh_sort_a").expect("node a");
    let node_b = NodeId::from_legacy_named("node_unknown_iroh_sort_b").expect("node b");
    let mut state = ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "unknown iroh sort",
        node_a,
        "node a",
        "pk-a",
        now,
    );
    let private = daemon_lan_endpoint(&node_b, "10.0.0.41:21353".to_string(), 10);
    let iroh = ClusterEndpointRecord {
        endpoint_id: stable_iroh_endpoint_id(&node_b, "iroh_public_key_unknown_sort"),
        node_id: node_b.clone(),
        transport: ClusterTransportKind::Iroh,
        address: "iroh://iroh_public_key_unknown_sort?relay=https://relay.example./".to_string(),
        source: ClusterEndpointSource::Rendezvous,
        privacy: ClusterEndpointPrivacy::Relayed,
        priority: 90,
        last_observed_latency_ms: None,
    };
    let mut member_b = ClusterMemberRecord::local_voter(cluster_id, node_b, "node b", "pk-b", now);
    member_b.capabilities = main_daemon_capabilities(&member_b.role);
    member_b.allowed_capabilities = member_b.capabilities.clone();
    member_b.endpoints = vec![private.clone(), iroh.clone()];
    state.members.push(member_b);

    let targets = cluster_sync_targets_with_connectivity(&paths, &state);

    assert_eq!(
        targets.first().map(|endpoint| &endpoint.endpoint_id),
        Some(&iroh.endpoint_id)
    );
    assert!(
        targets
            .iter()
            .any(|endpoint| endpoint.endpoint_id == private.endpoint_id)
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn cluster_storage_syncs_members_and_node_status_to_kv() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-cluster-storage-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");

    let now = OffsetDateTime::now_utc();
    let cluster_id = ClusterId::from_legacy_named("cluster_network_storage").expect("cluster");
    let node_id = NodeId::from_legacy_named("node_network_storage").expect("node");
    let mut state = ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "network storage",
        node_id.clone(),
        "network node",
        "public-key",
        now,
    );
    let member = state
        .members
        .iter_mut()
        .find(|member| member.node_id == node_id)
        .expect("local member");
    member.capabilities = vec![CapabilityName::new("daemon.main.run").expect("capability")];
    member.allowed_capabilities = member.capabilities.clone();
    member.endpoints = vec![ClusterEndpointRecord {
        endpoint_id: ClusterEndpointId::from_legacy_named("endpoint_network_storage_daemon_lan")
            .expect("endpoint id"),
        node_id: node_id.clone(),
        transport: ClusterTransportKind::LanTcp,
        address: "127.0.0.1:21353".to_string(),
        source: ClusterEndpointSource::Static,
        privacy: ClusterEndpointPrivacy::Lan,
        priority: 0,
        last_observed_latency_ms: None,
    }];

    write_cluster_state(&paths, &state).expect("write state");
    assert_eq!(
        read_cluster_state(&paths).expect("read state").cluster_id,
        cluster_id
    );

    sync_cluster_state_to_local_raft_unchecked(&paths, &state).expect("sync members");
    let members = read_cluster_members_from_kv(&paths).expect("members");
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].node_id, node_id);
    let store = read_cluster_kv_store(&paths).expect("cluster kv store");
    let identity: ClusterIdentityRecord = store
        .get(&cluster_identity_key())
        .expect("identity record")
        .decode()
        .expect("identity record");
    assert_eq!(identity.cluster_name, "network storage");
    let raft_config: ClusterRaftConfigRecord = store
        .get(&cluster_raft_config_key())
        .expect("raft config record")
        .decode()
        .expect("raft config record");
    assert_eq!(raft_config.voters, vec![node_id.clone()]);
    assert!(
        store
            .get(&cluster_endpoint_key(
                &node_id,
                &state.members[0].endpoints[0]
            ))
            .is_some()
    );
    let policy: ClusterCapabilityPolicyRecord = store
        .get(&cluster_capability_policy_key(&node_id))
        .expect("capability policy record")
        .decode()
        .expect("capability policy record");
    assert_eq!(policy.capabilities, state.members[0].capabilities);
    assert!(store.get(&cluster_rollout_desired_key()).is_some());

    let status = ClusterNodeStatusRecord {
        cluster_id: state.cluster_id.clone(),
        node_id: state.local_node_id.clone(),
        process: "daemon-main".to_string(),
        availability: ClusterNodeAvailability::Online,
        daemon_version: "0.1.0-test".to_string(),
        target_os: Some("test-os".to_string()),
        target_arch: Some("test-arch".to_string()),
        pid: Some(42),
        address: Some("127.0.0.1:21353".to_string()),
        bind_address: Some("0.0.0.0:21353".to_string()),
        executable: Some("sinomo".to_string()),
        rollout: Some(ClusterNodeRolloutStatus {
            status: "healthy".to_string(),
            mode: None,
            detail: None,
            source: None,
            requested_at: None,
            staged_at: None,
            updated_at: Some("2026-05-04T00:00:00Z".to_string()),
            failed_at: None,
            diagnostics: None,
            stdout_log: None,
            stderr_log: None,
            stdout_tail: None,
            stderr_tail: None,
        }),
        daemon_started_at: Some("2026-05-04T00:00:00Z".to_string()),
        observed_at: now,
    };
    sync_cluster_node_status_to_kv(&paths, status).expect("sync status");
    let statuses = read_cluster_node_statuses_from_kv(&paths).expect("statuses");
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].availability, ClusterNodeAvailability::Online);
    assert!(
        read_cluster_kv_store(&paths)
            .expect("cluster kv store")
            .get(&cluster_rollout_node_key(&state.local_node_id))
            .is_some()
    );
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime");
    let async_node = runtime
        .block_on(open_cluster_raft_kv_node_async(
            &paths,
            &state.local_node_id,
        ))
        .expect("async open");
    assert_eq!(async_node.node_id(), &state.local_node_id);

    std::fs::remove_dir_all(root).ok();
}

fn sobj_reported_len(bytes: &[u8]) -> usize {
    assert!(bytes.len() >= 12);
    assert_eq!(&bytes[..4], b"SOBJ");
    assert_eq!(bytes[6], 4, "network objects use u32 lengths");
    u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]) as usize
}

#[test]
fn cluster_state_read_rejects_trailing_file_bytes() {
    use std::io::Write;

    let root = std::env::temp_dir().join(format!(
        "sinomo-network-cluster-state-trailing-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");

    let cluster_id = ClusterId::from_legacy_named("cluster_trailing_state").expect("cluster");
    let node_id = NodeId::from_legacy_named("node_trailing_state").expect("node");
    let state = ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "trailing state",
        node_id,
        "node",
        "public-key",
        OffsetDateTime::now_utc(),
    );
    write_cluster_state(&paths, &state).expect("write state");
    std::fs::OpenOptions::new()
        .append(true)
        .open(&paths.state)
        .expect("open state")
        .write_all(b"stale-trailing-bytes")
        .expect("append trailing bytes");
    let bytes = std::fs::read(&paths.state).expect("read raw state");

    assert_eq!(sobj_reported_len(&bytes), bytes.len() - 20);
    assert!(
        read_cluster_state(&paths).is_err(),
        "stored SOBJ files with trailing bytes violate the object length invariant"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn cluster_state_rewrite_removes_trailing_file_bytes() {
    use std::io::Write;

    let root = std::env::temp_dir().join(format!(
        "sinomo-network-cluster-state-rewrite-trailing-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");

    let cluster_id =
        ClusterId::from_legacy_named("cluster_rewrite_trailing_state").expect("cluster");
    let node_id = NodeId::from_legacy_named("node_rewrite_trailing_state").expect("node");
    let mut state = ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "rewrite trailing state",
        node_id,
        "node",
        "public-key",
        OffsetDateTime::now_utc(),
    );
    write_cluster_state(&paths, &state).expect("write state");
    let initial = std::fs::read(&paths.state).expect("read initial raw state");
    assert_eq!(sobj_reported_len(&initial), initial.len());
    std::fs::OpenOptions::new()
        .append(true)
        .open(&paths.state)
        .expect("open state")
        .write_all(b"stale-trailing-bytes")
        .expect("append trailing bytes");

    state.cluster_name = "rewritten trailing state".to_string();
    write_cluster_state(&paths, &state).expect("rewrite state");

    let bytes = std::fs::read(&paths.state).expect("read raw state");
    assert_eq!(sobj_reported_len(&bytes), bytes.len());
    let strict: ClusterBootstrapState = SinomoObjectCodec::default()
        .decode(&bytes)
        .expect("strict state decode");
    assert_eq!(strict.cluster_id, cluster_id);
    assert_eq!(strict.cluster_name, "rewritten trailing state");
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn cluster_state_read_rejects_prefixed_or_truncated_file_bytes() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-cluster-state-invalid-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");

    let cluster_id = ClusterId::from_legacy_named("cluster_invalid_state").expect("cluster");
    let node_id = NodeId::from_legacy_named("node_invalid_state").expect("node");
    let state = ClusterBootstrapState::initialized_locally(
        cluster_id,
        "invalid state",
        node_id,
        "node",
        "public-key",
        OffsetDateTime::now_utc(),
    );
    write_cluster_state(&paths, &state).expect("write state");
    let valid = std::fs::read(&paths.state).expect("read state");
    assert_eq!(sobj_reported_len(&valid), valid.len());

    let mut prefixed = b"junk".to_vec();
    prefixed.extend_from_slice(&valid);
    std::fs::write(&paths.state, prefixed).expect("write prefixed state");
    assert!(
        read_cluster_state(&paths).is_err(),
        "prefixed bytes must not be treated as a recoverable trailing write"
    );

    std::fs::write(&paths.state, &valid[..valid.len() - 1]).expect("write truncated state");
    assert!(
        read_cluster_state(&paths).is_err(),
        "truncated objects must not be accepted"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn cluster_state_load_preserves_newer_local_member_records() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-cluster-state-merge-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");

    let earlier = OffsetDateTime::now_utc() - time::Duration::minutes(5);
    let later = OffsetDateTime::now_utc();
    let cluster_id = ClusterId::from_legacy_named("cluster_state_merge").expect("cluster");
    let node_id = NodeId::from_legacy_named("node_state_merge").expect("node");
    let mut stale = ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "state merge",
        node_id.clone(),
        "merge node",
        "public-key",
        earlier,
    );
    stale.members[0].role = ClusterMemberRole::Learner;
    stale.members[0].state = ClusterMemberState::Learner;
    stale.members[0].updated_at = earlier;
    sync_cluster_state_to_local_raft_unchecked(&paths, &stale).expect("sync stale kv");

    let mut local = stale.clone();
    local.members[0].role = ClusterMemberRole::Voter;
    local.members[0].state = ClusterMemberState::Voter;
    local.members[0].updated_at = later;
    local.updated_at = later;
    write_cluster_state(&paths, &local).expect("write local state");

    let loaded = read_cluster_state(&paths).expect("read merged state");
    assert_eq!(loaded.members[0].node_id, node_id);
    assert_eq!(loaded.members[0].role, ClusterMemberRole::Voter);
    assert_eq!(loaded.members[0].state, ClusterMemberState::Voter);

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn cluster_state_load_does_not_downgrade_local_member_from_kv() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-cluster-local-member-merge-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");

    let earlier = OffsetDateTime::now_utc() - time::Duration::minutes(5);
    let later = OffsetDateTime::now_utc();
    let cluster_id = ClusterId::from_legacy_named("cluster_local_member_merge").expect("cluster");
    let node_id = NodeId::from_legacy_named("node_local_member_merge").expect("node");
    let mut stale = ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "local member merge",
        node_id.clone(),
        "merge node",
        "public-key",
        later,
    );
    stale.members[0].role = ClusterMemberRole::Learner;
    stale.members[0].state = ClusterMemberState::Learner;
    stale.members[0].updated_at = later;
    sync_cluster_state_to_local_raft_unchecked(&paths, &stale).expect("sync stale kv");

    let mut local = stale.clone();
    local.members[0].role = ClusterMemberRole::Voter;
    local.members[0].state = ClusterMemberState::Voter;
    local.members[0].updated_at = earlier;
    local.updated_at = earlier;
    write_cluster_state(&paths, &local).expect("write local state");

    let loaded = read_cluster_state(&paths).expect("read merged state");
    assert_eq!(loaded.members[0].node_id, node_id);
    assert_eq!(loaded.members[0].role, ClusterMemberRole::Voter);
    assert_eq!(loaded.members[0].state, ClusterMemberState::Voter);

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn cluster_state_load_drops_remote_members_removed_from_kv() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-cluster-removed-member-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");

    let now = OffsetDateTime::now_utc();
    let cluster_id = ClusterId::from_legacy_named("cluster_removed_member").expect("cluster");
    let local_node_id = NodeId::from_legacy_named("node_removed_member_local").expect("local");
    let remote_node_id = NodeId::from_legacy_named("node_removed_member_remote").expect("remote");
    let mut state = ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "removed member",
        local_node_id.clone(),
        "local",
        "local-public-key",
        now,
    );
    state.members.push(ClusterMemberRecord::local_voter(
        cluster_id,
        remote_node_id.clone(),
        "remote",
        "remote-public-key",
        now,
    ));
    write_cluster_state(&paths, &state).expect("write local state");
    sync_cluster_state_to_local_raft_unchecked(&paths, &state).expect("sync members");
    append_cluster_raft_command(
        &paths,
        &local_node_id,
        KvCommand::Txn(KvTxn {
            compares: Vec::new(),
            success: vec![KvOp::Delete {
                key: cluster_member_key(&remote_node_id),
            }],
            failure: Vec::new(),
        }),
    )
    .expect("remove remote member");

    let loaded = read_cluster_state(&paths).expect("read state");
    assert_eq!(
        loaded
            .members
            .iter()
            .map(|member| member.node_id.clone())
            .collect::<Vec<_>>(),
        vec![local_node_id]
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn cluster_read_view_falls_back_to_snapshot_when_local_raft_state_is_corrupt() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-cluster-corrupt-read-view-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");

    let now = OffsetDateTime::now_utc();
    let cluster_id = ClusterId::from_legacy_named("cluster_corrupt_read_view").expect("cluster");
    let node_id = NodeId::from_legacy_named("node_corrupt_read_view").expect("node");
    let state = ClusterBootstrapState::initialized_locally(
        cluster_id,
        "corrupt read view",
        node_id.clone(),
        "read view node",
        "public-key",
        now,
    );
    write_cluster_state(&paths, &state).expect("write state");
    sync_cluster_state_to_local_raft_unchecked(&paths, &state).expect("sync members");

    let wal_paths = cluster_raft_wal_paths(&paths, &node_id);
    std::fs::write(&wal_paths.hard_state, b"\0\0\0").expect("corrupt hard state");
    let members = read_cluster_members_from_kv(&paths).expect("members from snapshot fallback");

    assert_eq!(members.len(), 1);
    assert_eq!(members[0].node_id, node_id);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn cluster_network_records_publish_overwrite_and_expire() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-graph-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");

    let now = OffsetDateTime::now_utc();
    let cluster_id = ClusterId::from_legacy_named("cluster_network_graph").expect("cluster");
    let node_a = NodeId::from_legacy_named("node_network_graph_a").expect("node a");
    let node_b = NodeId::from_legacy_named("node_network_graph_b").expect("node b");
    let mut state = ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "network graph",
        node_a.clone(),
        "node a",
        "public-key-a",
        now,
    );
    let mut member_b =
        ClusterMemberRecord::local_voter(cluster_id.clone(), node_b.clone(), "node b", "pk-b", now);
    member_b.endpoints = vec![ClusterEndpointRecord {
        endpoint_id: ClusterEndpointId::from_legacy_named("endpoint_network_graph_b")
            .expect("endpoint"),
        node_id: node_b.clone(),
        transport: ClusterTransportKind::LanTcp,
        address: "192.0.2.20:21353".to_string(),
        source: ClusterEndpointSource::Observed,
        privacy: ClusterEndpointPrivacy::Lan,
        priority: 10,
        last_observed_latency_ms: None,
    }];
    state.members.push(member_b.clone());
    write_cluster_state(&paths, &state).expect("write state");
    sync_cluster_state_to_local_raft_unchecked(&paths, &state).expect("sync state");

    sync_cluster_network_info_to_kv(
        &paths,
        ClusterNetworkInfoRecord {
            cluster_id: cluster_id.clone(),
            node_id: node_a.clone(),
            environment_id: "env_lan_a".to_string(),
            observed_addresses: vec!["192.0.2.10".to_string()],
            endpoints: Vec::new(),
            observed_at: now,
            expires_at: now + time::Duration::seconds(30),
        },
    )
    .expect("sync network info");
    sync_cluster_network_info_to_kv(
        &paths,
        ClusterNetworkInfoRecord {
            cluster_id: cluster_id.clone(),
            node_id: node_a.clone(),
            environment_id: "env_lan_b".to_string(),
            observed_addresses: vec!["198.51.100.10".to_string()],
            endpoints: Vec::new(),
            observed_at: now,
            expires_at: now + time::Duration::seconds(30),
        },
    )
    .expect("overwrite network info");

    let infos = read_cluster_network_info_from_kv(&paths).expect("read infos");
    assert_eq!(infos.len(), 1);
    assert_eq!(infos[0].environment_id, "env_lan_b");

    let endpoint = member_b.endpoints[0].clone();
    sync_cluster_connectivity_records_to_kv(
        &paths,
        &node_a,
        vec![ClusterConnectivityRecord {
            cluster_id: cluster_id.clone(),
            source_node_id: node_a.clone(),
            target_node_id: node_b.clone(),
            endpoint_id: endpoint.endpoint_id.clone(),
            transport: endpoint.transport.clone(),
            address: endpoint.address.clone(),
            state: ClusterConnectivityState::Healthy,
            latency_ms: Some(7),
            consecutive_failures: 0,
            last_success_at: Some("now".to_string()),
            last_failure_at: None,
            observed_at: now,
            expires_at: now + time::Duration::seconds(30),
        }],
    )
    .expect("sync connectivity");
    let graph = read_cluster_connectivity_graph_from_kv(&paths).expect("graph");
    assert_eq!(graph.len(), 1);
    assert_eq!(graph[0].state, ClusterConnectivityState::Healthy);

    sync_cluster_network_info_to_kv(
        &paths,
        ClusterNetworkInfoRecord {
            cluster_id: cluster_id.clone(),
            node_id: node_b.clone(),
            environment_id: "env_expired".to_string(),
            observed_addresses: vec!["203.0.113.20".to_string()],
            endpoints: Vec::new(),
            observed_at: now - time::Duration::seconds(60),
            expires_at: now - time::Duration::seconds(1),
        },
    )
    .expect("sync expired network info");
    sync_cluster_connectivity_records_to_kv(
        &paths,
        &node_b,
        vec![ClusterConnectivityRecord {
            cluster_id: cluster_id.clone(),
            source_node_id: node_b.clone(),
            target_node_id: node_a.clone(),
            endpoint_id: endpoint.endpoint_id.clone(),
            transport: endpoint.transport.clone(),
            address: endpoint.address.clone(),
            state: ClusterConnectivityState::Healthy,
            latency_ms: Some(11),
            consecutive_failures: 0,
            last_success_at: Some("expired".to_string()),
            last_failure_at: None,
            observed_at: now - time::Duration::seconds(60),
            expires_at: now - time::Duration::seconds(1),
        }],
    )
    .expect("sync expired connectivity");
    assert_eq!(
        read_cluster_network_info_from_kv(&paths)
            .expect("read live infos")
            .len(),
        1,
        "expired network info should be ignored by read views"
    );
    assert_eq!(
        read_cluster_connectivity_graph_from_kv(&paths)
            .expect("read live graph")
            .len(),
        1,
        "expired connectivity should be ignored by read views"
    );

    let partial_probe_ops =
        cluster_connectivity_sync_ops(&paths, &node_a, &[]).expect("partial probe ops");
    assert!(
        partial_probe_ops.is_empty(),
        "unprobed but unexpired connectivity records should be retained"
    );

    expire_cluster_network_records(&paths, now + time::Duration::seconds(31))
        .expect("expire network");
    assert!(
        read_cluster_network_info_from_kv(&paths)
            .expect("read expired infos")
            .is_empty()
    );
    assert!(
        read_cluster_connectivity_graph_from_kv(&paths)
            .expect("read expired graph")
            .is_empty()
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn raft_config_allows_voter_removal_with_overlapping_quorum() {
    let cluster_id = ClusterId::from_legacy_named("cluster_raft_config_overlap").expect("cluster");
    let node_a = NodeId::from_legacy_named("node_raft_config_overlap_a").expect("node a");
    let node_b = NodeId::from_legacy_named("node_raft_config_overlap_b").expect("node b");
    let node_c = NodeId::from_legacy_named("node_raft_config_overlap_c").expect("node c");
    let current = ClusterRaftConfigRecord {
        cluster_id: cluster_id.clone(),
        term: 1,
        kv_revision: 1,
        voters: vec![node_a.clone(), node_b.clone(), node_c],
        learners: Vec::new(),
        non_voting: Vec::new(),
        updated_at: OffsetDateTime::now_utc(),
    };
    let next = ClusterRaftConfigRecord {
        cluster_id,
        term: 1,
        kv_revision: 1,
        voters: vec![node_a, node_b],
        learners: Vec::new(),
        non_voting: Vec::new(),
        updated_at: OffsetDateTime::now_utc(),
    };

    validate_raft_config_transition(&current, &next).expect("overlapping quorum");
}

#[test]
fn raft_config_rejects_voter_removal_without_old_quorum_overlap() {
    let cluster_id =
        ClusterId::from_legacy_named("cluster_raft_config_no_overlap").expect("cluster");
    let node_a = NodeId::from_legacy_named("node_raft_config_no_overlap_a").expect("node a");
    let node_b = NodeId::from_legacy_named("node_raft_config_no_overlap_b").expect("node b");
    let current = ClusterRaftConfigRecord {
        cluster_id: cluster_id.clone(),
        term: 1,
        kv_revision: 1,
        voters: vec![node_a.clone(), node_b],
        learners: Vec::new(),
        non_voting: Vec::new(),
        updated_at: OffsetDateTime::now_utc(),
    };
    let next = ClusterRaftConfigRecord {
        cluster_id,
        term: 1,
        kv_revision: 1,
        voters: vec![node_a],
        learners: Vec::new(),
        non_voting: Vec::new(),
        updated_at: OffsetDateTime::now_utc(),
    };

    assert!(validate_raft_config_transition(&current, &next).is_err());
}

#[test]
fn cluster_kv_watch_event_log_is_bounded() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-watch-events-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");
    let events = (0..(MAX_CLUSTER_KV_WATCH_EVENTS + 10))
        .map(|index| KvEvent {
            revision: KvRevision(index as u64 + 1),
            key: format!("/watch/{index}"),
            kind: KvEventKind::Put {
                value: index.to_string().into_bytes(),
            },
        })
        .collect::<Vec<_>>();

    append_cluster_kv_watch_events(&paths, &events).expect("append watch events");

    let retained = cluster_kv_watch_events(&paths).expect("read watch events");
    assert_eq!(retained.len(), MAX_CLUSTER_KV_WATCH_EVENTS);
    assert_eq!(
        retained.first().expect("first retained").revision,
        KvRevision(11)
    );
    assert_eq!(
        retained.last().expect("last retained").revision,
        KvRevision(MAX_CLUSTER_KV_WATCH_EVENTS as u64 + 10)
    );
    std::fs::remove_dir_all(root).ok();
}

fn node(id: &str) -> NodeProfile {
    let mut profile = NodeProfile::local(id).expect("profile");
    profile.node_id = NodeId::from_legacy_named(id).expect("node id");
    profile
}

fn fixture_event(id: &str, origin_node_id: NodeId) -> EventEnvelope {
    EventEnvelope::new(
        EventId::from_legacy_named(id).expect("event"),
        time::OffsetDateTime::now_utc(),
        origin_node_id,
        Source::new("test.network").expect("source"),
        EventKind::new("network.test.event").expect("kind"),
        Salience::new(0.5).expect("salience"),
        Confidence::new(1.0).expect("confidence"),
        RoutingMetadata::new(
            RoutingScope::Session,
            LatencyClass::Interactive,
            PrivacyClass::Derived,
        ),
    )
}

fn certificate(
    cluster_id: ClusterId,
    node_id: NodeId,
    issuer_node_id: NodeId,
    public_key: &str,
    expires_at: time::OffsetDateTime,
) -> ClusterCertificate {
    ClusterCertificate {
        certificate_id: ClusterCertificateId::generate(),
        cluster_id,
        node_id,
        issuer_node_id,
        public_key: public_key.to_string(),
        capability_scope: Vec::new(),
        issued_at: time::OffsetDateTime::now_utc(),
        expires_at,
        signature: "sig".to_string(),
    }
}

#[test]
fn peer_registry_tracks_cluster_members_and_endpoint_candidates() {
    let cluster_id = ClusterId::from_legacy_named("cluster_home").expect("cluster");
    let node_id = NodeId::from_legacy_named("node_desktop").expect("node");
    let timestamp = time::OffsetDateTime::now_utc();
    let mut member = ClusterMemberRecord::local_voter(
        cluster_id.clone(),
        node_id.clone(),
        "desktop",
        "pk_desktop",
        timestamp,
    );
    member.endpoints.push(ClusterEndpointRecord {
        endpoint_id: ClusterEndpointId::from_legacy_named("endpoint_lan").expect("endpoint"),
        node_id: node_id.clone(),
        transport: ClusterTransportKind::LanQuic,
        address: "192.168.1.20:21353".to_string(),
        source: ClusterEndpointSource::Mdns,
        privacy: ClusterEndpointPrivacy::Lan,
        priority: 10,
        last_observed_latency_ms: None,
    });
    member.endpoints.push(ClusterEndpointRecord {
        endpoint_id: ClusterEndpointId::from_legacy_named("endpoint_webrtc").expect("endpoint"),
        node_id: node_id.clone(),
        transport: ClusterTransportKind::WebRtc,
        address: "webrtc://candidate".to_string(),
        source: ClusterEndpointSource::Ice,
        privacy: ClusterEndpointPrivacy::InternetDirect,
        priority: 50,
        last_observed_latency_ms: None,
    });
    let mut registry = PeerRegistry::new();

    registry.upsert_cluster_member(member.clone());

    assert_eq!(registry.cluster_id(), Some(&cluster_id));
    assert_eq!(registry.cluster_member(&node_id), Some(&member));
    assert_eq!(registry.endpoint_candidates(&node_id).len(), 2);
    assert_eq!(
        registry
            .best_endpoint_candidate(&node_id)
            .expect("best endpoint")
            .endpoint
            .transport,
        ClusterTransportKind::LanQuic
    );
}

#[test]
fn node_endpoint_maps_cluster_roaming_transports() {
    let node_id = NodeId::from_legacy_named("node_phone").expect("node");
    let endpoint = ClusterEndpointRecord {
        endpoint_id: ClusterEndpointId::from_legacy_named("endpoint_ice").expect("endpoint"),
        node_id: node_id.clone(),
        transport: ClusterTransportKind::WebRtc,
        address: "webrtc://candidate".to_string(),
        source: ClusterEndpointSource::Ice,
        privacy: ClusterEndpointPrivacy::InternetDirect,
        priority: 20,
        last_observed_latency_ms: Some(40),
    };

    let network_endpoint = NodeEndpoint::from_cluster_endpoint(&endpoint);

    assert_eq!(network_endpoint.node_id, node_id);
    assert_eq!(network_endpoint.transport, TransportKind::WebRtc);
    assert_eq!(network_endpoint.address, "webrtc://candidate");
}

#[test]
fn node_endpoint_maps_iroh_transport() {
    let node_id = NodeId::from_legacy_named("node_phone").expect("node");
    let endpoint = ClusterEndpointRecord {
        endpoint_id: ClusterEndpointId::from_legacy_named("endpoint_iroh").expect("endpoint"),
        node_id: node_id.clone(),
        transport: ClusterTransportKind::Iroh,
        address: "iroh://iroh_pk".to_string(),
        source: ClusterEndpointSource::Rendezvous,
        privacy: ClusterEndpointPrivacy::InternetDirect,
        priority: 40,
        last_observed_latency_ms: None,
    };

    let network_endpoint = NodeEndpoint::from_cluster_endpoint(&endpoint);

    assert_eq!(network_endpoint.node_id, node_id);
    assert_eq!(network_endpoint.transport, TransportKind::Iroh);
    assert_eq!(network_endpoint.address, "iroh://iroh_pk");
}

#[test]
fn lan_quic_frame_codec_round_trips_network_frames_and_limits_size() {
    let node_a = NodeId::from_legacy_named("node_a").expect("node");
    let node_b = NodeId::from_legacy_named("node_b").expect("node");
    let event = fixture_event("evt_codec", node_a.clone());
    let frame = NetworkFrame {
        frame_id: NetworkFrameId(7),
        source_node_id: node_a.clone(),
        target_node_id: node_b.clone(),
        message: MeshMessage {
            message_id: MeshMessageId(8),
            origin_node_id: node_a,
            target_node_id: node_b,
            body: MeshMessageBody::Event {
                delivery_id: RouteDeliveryId(9),
                event: Box::new(event),
            },
        },
    };
    let codec = LanQuicFrameCodec {
        max_frame_bytes: 16 * 1024,
    };

    let encoded = codec.encode(&frame).expect("encode");
    let decoded = codec.decode(&encoded).expect("decode");

    assert_eq!(&encoded[..4], sinomo_object::MAGIC);
    assert_eq!(decoded, frame);
    assert!(matches!(
        LanQuicFrameCodec { max_frame_bytes: 1 }.encode(&frame),
        Err(NetworkError::FrameTooLarge { .. })
    ));
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct FixtureProtocolPayload {
    path: String,
    bytes: Vec<u8>,
}

#[test]
fn protocol_and_stream_metadata_use_sinomo_object_payloads() {
    let payload = FixtureProtocolPayload {
        path: "/users/alice/readme.txt".to_string(),
        bytes: vec![1, 2, 3, 4],
    };

    let message = ProtocolMessage::encode("fixture.protocol.v1", &payload).expect("payload");
    assert_eq!(message.protocol, "fixture.protocol.v1");
    assert_eq!(&message.payload[..4], sinomo_object::MAGIC);
    assert_eq!(
        message
            .decode::<FixtureProtocolPayload>()
            .expect("decode payload"),
        payload
    );

    let open =
        NetworkStreamOpen::encode_metadata(NetworkStreamId(42), "fixture.stream.v1", &payload)
            .expect("metadata");
    assert_eq!(open.stream_id, NetworkStreamId(42));
    assert_eq!(&open.metadata[..4], sinomo_object::MAGIC);
    assert_eq!(
        open.decode_metadata::<FixtureProtocolPayload>()
            .expect("decode metadata"),
        payload
    );
}

#[test]
fn mdns_discovery_cache_returns_lan_quic_candidates_and_expires_records() {
    let cluster_id = ClusterId::from_legacy_named("cluster_home").expect("cluster");
    let node_id = NodeId::from_legacy_named("node_desktop").expect("node");
    let mut cache = MdnsDiscoveryCache::new();
    cache.upsert(MdnsDiscoveryRecord::lan_quic(
        cluster_id.clone(),
        node_id.clone(),
        "192.168.1.20:21355",
        10,
        5,
    ));

    let candidates = cache.endpoint_candidates_for_node(&node_id);

    assert_eq!(cache.endpoints_for_cluster(&cluster_id).len(), 1);
    assert_eq!(candidates.len(), 1);
    assert_eq!(
        candidates[0].endpoint.transport,
        ClusterTransportKind::LanQuic
    );
    assert_eq!(cache.expire_after_tick(14), 0);
    assert_eq!(cache.expire_after_tick(15), 1);
    assert!(cache.endpoint_candidates_for_node(&node_id).is_empty());
}

#[test]
fn cluster_auth_verifier_establishes_mutual_session_for_authorized_members() {
    let cluster_id = ClusterId::from_legacy_named("cluster_home").expect("cluster");
    let node_a = NodeId::from_legacy_named("node_a").expect("node");
    let node_b = NodeId::from_legacy_named("node_b").expect("node");
    let now = time::OffsetDateTime::now_utc();
    let expires_at = now + time::Duration::hours(1);
    let member_a =
        ClusterMemberRecord::local_voter(cluster_id.clone(), node_a.clone(), "a", "pk_a", now);
    let member_b =
        ClusterMemberRecord::local_voter(cluster_id.clone(), node_b.clone(), "b", "pk_b", now);
    let verifier = ClusterAuthVerifier::new(vec![member_a, member_b]);
    let material_a = ClusterAuthMaterial {
        cluster_id: cluster_id.clone(),
        node_id: node_a.clone(),
        node_public_key: "pk_a".to_string(),
        certificate: certificate(
            cluster_id.clone(),
            node_a.clone(),
            node_a.clone(),
            "pk_a",
            expires_at,
        ),
    };
    let material_b = ClusterAuthMaterial {
        cluster_id: cluster_id.clone(),
        node_id: node_b.clone(),
        node_public_key: "pk_b".to_string(),
        certificate: certificate(
            cluster_id.clone(),
            node_b.clone(),
            node_a.clone(),
            "pk_b",
            expires_at,
        ),
    };

    let session = verifier
        .establish_mutual_session(&material_a, &material_b, now)
        .expect("mutual auth");

    assert_eq!(session.cluster_id, cluster_id);
    assert_eq!(session.local_node_id, node_a);
    assert_eq!(session.remote_node_id, node_b);
}

#[test]
fn cluster_auth_verifier_rejects_expired_or_wrong_key_certificate() {
    let cluster_id = ClusterId::from_legacy_named("cluster_home").expect("cluster");
    let node_a = NodeId::from_legacy_named("node_a").expect("node");
    let now = time::OffsetDateTime::now_utc();
    let member =
        ClusterMemberRecord::local_voter(cluster_id.clone(), node_a.clone(), "a", "pk_a", now);
    let verifier = ClusterAuthVerifier::new(vec![member]);
    let wrong_key = ClusterAuthMaterial {
        cluster_id: cluster_id.clone(),
        node_id: node_a.clone(),
        node_public_key: "pk_a".to_string(),
        certificate: certificate(
            cluster_id.clone(),
            node_a.clone(),
            node_a.clone(),
            "pk_other",
            now + time::Duration::hours(1),
        ),
    };
    let expired = ClusterAuthMaterial {
        cluster_id: cluster_id.clone(),
        node_id: node_a.clone(),
        node_public_key: "pk_a".to_string(),
        certificate: certificate(cluster_id, node_a.clone(), node_a, "pk_a", now),
    };

    assert!(matches!(
        verifier.verify_peer(&wrong_key.cluster_id, &wrong_key, now),
        Err(NetworkError::CertificateKeyMismatch { .. })
    ));
    assert!(matches!(
        verifier.verify_peer(&expired.cluster_id, &expired, now),
        Err(NetworkError::CertificateExpired { .. })
    ));
}

#[test]
fn lan_connection_planner_prefers_lan_quic_over_relays() {
    let node_id = NodeId::from_legacy_named("node_desktop").expect("node");
    let relay = PeerEndpointCandidate::discovered(ClusterEndpointRecord {
        endpoint_id: ClusterEndpointId::from_legacy_named("endpoint_relay").expect("endpoint"),
        node_id: node_id.clone(),
        transport: ClusterTransportKind::TurnRelay,
        address: "turn://relay".to_string(),
        source: ClusterEndpointSource::Rendezvous,
        privacy: ClusterEndpointPrivacy::Relayed,
        priority: 1,
        last_observed_latency_ms: None,
    });
    let lan = PeerEndpointCandidate::discovered(ClusterEndpointRecord {
        endpoint_id: ClusterEndpointId::from_legacy_named("endpoint_lan").expect("endpoint"),
        node_id,
        transport: ClusterTransportKind::LanQuic,
        address: "192.168.1.20:21355".to_string(),
        source: ClusterEndpointSource::Mdns,
        privacy: ClusterEndpointPrivacy::Lan,
        priority: 10,
        last_observed_latency_ms: None,
    });

    let candidates = [relay, lan];
    let selected =
        LanConnectionPlanner::select_direct_candidate(&candidates).expect("direct candidate");

    assert_eq!(selected.endpoint.transport, ClusterTransportKind::LanQuic);
}

#[test]
fn webrtc_kv_exchange_round_trips_offer_answer_and_ice_candidates() {
    let cluster_id = ClusterId::from_legacy_named("cluster_home").expect("cluster");
    let node_a = NodeId::from_legacy_named("node_a").expect("node");
    let node_b = NodeId::from_legacy_named("node_b").expect("node");
    let exchange_id = "exchange_1";
    let mut store = sinomo_kv::InProcessKvStore::new();
    store
        .apply_command(sinomo_kv::KvCommand::Op(
            WebRtcKvExchange::put_signal(
                WebRtcSignalingEnvelope::offer(
                    cluster_id.clone(),
                    exchange_id,
                    node_a.clone(),
                    node_b.clone(),
                    "v=0 offer",
                    10,
                ),
                None,
            )
            .expect("offer op"),
        ))
        .expect("put offer");
    store
        .apply_command(sinomo_kv::KvCommand::Op(
            WebRtcKvExchange::put_signal(
                WebRtcSignalingEnvelope::answer(
                    cluster_id.clone(),
                    exchange_id,
                    node_b.clone(),
                    node_a.clone(),
                    "v=0 answer",
                    10,
                ),
                None,
            )
            .expect("answer op"),
        ))
        .expect("put answer");
    let relay_candidate = IceCandidateRecord {
        cluster_id: cluster_id.clone(),
        exchange_id: exchange_id.to_string(),
        node_id: node_b.clone(),
        candidate_id: "relay_1".to_string(),
        candidate_type: IceCandidateType::Relay,
        address: "turn://relay.example:3478".to_string(),
        priority: 10,
        raw_candidate: "candidate:relay".to_string(),
        expires_after_tick: 10,
    };
    store
        .apply_command(sinomo_kv::KvCommand::Op(
            WebRtcKvExchange::put_candidate(relay_candidate.clone(), None).expect("candidate op"),
        ))
        .expect("put candidate");

    let signals = store.range_prefix(&WebRtcKvExchange::signal_prefix(&cluster_id, exchange_id));
    let candidates = store.range_prefix(&WebRtcKvExchange::candidate_prefix(
        &cluster_id,
        exchange_id,
    ));
    let parsed_offer = WebRtcKvExchange::signal_from_entry(&signals[0]).expect("offer");
    let parsed_candidate =
        WebRtcKvExchange::candidate_from_entry(&candidates[0]).expect("candidate");

    assert_eq!(signals.len(), 2);
    assert!(matches!(
        parsed_offer.message,
        WebRtcSignalMessage::Offer { .. }
    ));
    assert_eq!(parsed_candidate, relay_candidate);
    assert_eq!(
        parsed_candidate.endpoint().transport,
        ClusterTransportKind::TurnRelay
    );
}

#[test]
fn iroh_kv_exchange_round_trips_endpoint_records() {
    let cluster_id = ClusterId::from_legacy_named("cluster_home").expect("cluster");
    let node_id = NodeId::from_legacy_named("node_mobile").expect("node");
    let endpoint = IrohEndpointRecord {
        cluster_id: cluster_id.clone(),
        node_id: node_id.clone(),
        endpoint_public_key: "iroh_public_key".to_string(),
        direct_addresses: vec!["203.0.113.10:21356".to_string()],
        relay_url: Some("https://relay.example".to_string()),
        relay_mode: IrohRelayMode::Default,
        expires_after_tick: 50,
    };
    let mut store = sinomo_kv::InProcessKvStore::new();
    store
        .apply_command(sinomo_kv::KvCommand::Op(
            IrohKvExchange::put_endpoint(endpoint.clone(), None).expect("endpoint op"),
        ))
        .expect("put endpoint");

    let entries = store.range_prefix(&IrohKvExchange::endpoint_prefix(&cluster_id));
    let parsed = IrohKvExchange::endpoint_from_entry(&entries[0]).expect("endpoint");
    let cluster_endpoint = parsed.endpoint();

    assert_eq!(entries.len(), 1);
    assert_eq!(parsed, endpoint);
    assert_eq!(cluster_endpoint.node_id, node_id);
    assert_eq!(cluster_endpoint.transport, ClusterTransportKind::Iroh);
    assert_eq!(cluster_endpoint.privacy, ClusterEndpointPrivacy::Relayed);
    assert!(!cluster_endpoint.address.contains("direct="));
    assert!(
        cluster_endpoint
            .address
            .contains("relay=https://relay.example")
    );
    let parsed_endpoint =
        iroh_endpoint_record_from_cluster_endpoint(&cluster_endpoint, &cluster_id)
            .expect("cluster endpoint parse");
    assert!(parsed_endpoint.direct_addresses.is_empty());
    assert_eq!(parsed_endpoint.relay_url, endpoint.relay_url);
}

#[test]
fn iroh_endpoint_record_uses_stable_id_for_public_key() {
    let cluster_id = ClusterId::from_legacy_named("cluster_home").expect("cluster");
    let node_id = NodeId::from_legacy_named("node_mobile").expect("node");
    let first = IrohEndpointRecord {
        cluster_id: cluster_id.clone(),
        node_id: node_id.clone(),
        endpoint_public_key: "iroh_public_key".to_string(),
        direct_addresses: vec!["203.0.113.10:21356".to_string()],
        relay_url: None,
        relay_mode: IrohRelayMode::Default,
        expires_after_tick: 50,
    }
    .endpoint();
    let second = IrohEndpointRecord {
        cluster_id,
        node_id,
        endpoint_public_key: "iroh_public_key".to_string(),
        direct_addresses: vec!["203.0.113.11:21356".to_string()],
        relay_url: None,
        relay_mode: IrohRelayMode::Default,
        expires_after_tick: 51,
    }
    .endpoint();

    assert_eq!(first.endpoint_id, second.endpoint_id);
    assert_eq!(first.address, second.address);
}

#[test]
fn upsert_member_endpoint_replaces_iroh_record_for_same_public_key() {
    let cluster_id = ClusterId::from_legacy_named("cluster_home").expect("cluster");
    let node_id = NodeId::from_legacy_named("node_mobile").expect("node");
    let mut member = ClusterMemberRecord::local_voter(
        cluster_id.clone(),
        node_id.clone(),
        "mobile",
        "public-key",
        OffsetDateTime::now_utc(),
    );
    member.endpoints.push(ClusterEndpointRecord {
        endpoint_id: ClusterEndpointId::from_legacy_named("endpoint_legacy_iroh")
            .expect("legacy endpoint"),
        node_id: node_id.clone(),
        transport: ClusterTransportKind::Iroh,
        address: "iroh://iroh_public_key?direct=203.0.113.9:21356".to_string(),
        source: ClusterEndpointSource::Rendezvous,
        privacy: ClusterEndpointPrivacy::InternetDirect,
        priority: 40,
        last_observed_latency_ms: None,
    });
    let first = IrohEndpointRecord {
        cluster_id: cluster_id.clone(),
        node_id: node_id.clone(),
        endpoint_public_key: "iroh_public_key".to_string(),
        direct_addresses: vec!["203.0.113.10:21356".to_string()],
        relay_url: None,
        relay_mode: IrohRelayMode::Default,
        expires_after_tick: 50,
    }
    .endpoint();
    let second = IrohEndpointRecord {
        cluster_id,
        node_id,
        endpoint_public_key: "iroh_public_key".to_string(),
        direct_addresses: vec!["203.0.113.11:21356".to_string()],
        relay_url: None,
        relay_mode: IrohRelayMode::Default,
        expires_after_tick: 51,
    }
    .endpoint();

    assert!(upsert_member_endpoint(&mut member, first));
    assert!(!upsert_member_endpoint(&mut member, second));

    let iroh: Vec<_> = member
        .endpoints
        .iter()
        .filter(|endpoint| endpoint.transport == ClusterTransportKind::Iroh)
        .collect();
    assert_eq!(iroh.len(), 1);
    assert_eq!(iroh[0].address, "iroh://iroh_public_key");
}

#[test]
fn read_cluster_members_from_kv_collapses_duplicate_iroh_endpoint_records() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-iroh-kv-cleanup-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");

    let now = OffsetDateTime::now_utc();
    let cluster_id = ClusterId::from_legacy_named("cluster_iroh_kv_cleanup").expect("cluster");
    let node_id = NodeId::from_legacy_named("node_iroh_kv_cleanup").expect("node");
    let mut member = ClusterMemberRecord::local_voter(
        cluster_id.clone(),
        node_id.clone(),
        "iroh cleanup",
        "public-key",
        now,
    );
    member.endpoints.push(ClusterEndpointRecord {
        endpoint_id: ClusterEndpointId::from_legacy_named("endpoint_legacy_iroh_a")
            .expect("legacy endpoint"),
        node_id: node_id.clone(),
        transport: ClusterTransportKind::Iroh,
        address: "iroh://same_public_key?direct=203.0.113.9:21356".to_string(),
        source: ClusterEndpointSource::Rendezvous,
        privacy: ClusterEndpointPrivacy::InternetDirect,
        priority: 40,
        last_observed_latency_ms: None,
    });
    let stable = IrohEndpointRecord {
        cluster_id: cluster_id.clone(),
        node_id: node_id.clone(),
        endpoint_public_key: "same_public_key".to_string(),
        direct_addresses: vec!["203.0.113.11:21356".to_string()],
        relay_url: None,
        relay_mode: IrohRelayMode::Default,
        expires_after_tick: 51,
    }
    .endpoint();
    let legacy_b = ClusterEndpointRecord {
        endpoint_id: ClusterEndpointId::from_legacy_named("endpoint_legacy_iroh_b")
            .expect("legacy endpoint"),
        node_id: node_id.clone(),
        transport: ClusterTransportKind::Iroh,
        address: "iroh://same_public_key?direct=203.0.113.10:21356".to_string(),
        source: ClusterEndpointSource::Rendezvous,
        privacy: ClusterEndpointPrivacy::InternetDirect,
        priority: 40,
        last_observed_latency_ms: None,
    };

    let mut store = InProcessKvStore::new();
    store
        .put_serde(sinomo_kv::cluster_member_key(&node_id), &member, None)
        .expect("put member");
    store
        .put_serde(cluster_endpoint_key(&node_id, &legacy_b), &legacy_b, None)
        .expect("put legacy endpoint");
    store
        .put_serde(cluster_endpoint_key(&node_id, &stable), &stable, None)
        .expect("put stable endpoint");
    write_cluster_kv_snapshot(&paths, &store.snapshot()).expect("write snapshot");

    let members = read_cluster_members_from_kv(&paths).expect("members");
    let iroh: Vec<_> = members[0]
        .endpoints
        .iter()
        .filter(|endpoint| endpoint.transport == ClusterTransportKind::Iroh)
        .collect();

    assert_eq!(iroh.len(), 1);
    assert_eq!(iroh[0].endpoint_id, stable.endpoint_id);
    assert_eq!(iroh[0].address, "iroh://same_public_key");

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn roaming_connection_planner_uses_webrtc_then_turn_fallback_when_lan_missing() {
    let node_id = NodeId::from_legacy_named("node_mobile").expect("node");
    let webrtc = PeerEndpointCandidate::discovered(ClusterEndpointRecord {
        endpoint_id: ClusterEndpointId::from_legacy_named("endpoint_webrtc").expect("endpoint"),
        node_id: node_id.clone(),
        transport: ClusterTransportKind::WebRtc,
        address: "webrtc://srflx".to_string(),
        source: ClusterEndpointSource::Ice,
        privacy: ClusterEndpointPrivacy::InternetDirect,
        priority: 60,
        last_observed_latency_ms: None,
    })
    .healthy(35);
    let turn = PeerEndpointCandidate::discovered(ClusterEndpointRecord {
        endpoint_id: ClusterEndpointId::from_legacy_named("endpoint_turn").expect("endpoint"),
        node_id: node_id.clone(),
        transport: ClusterTransportKind::TurnRelay,
        address: "turn://relay".to_string(),
        source: ClusterEndpointSource::Rendezvous,
        privacy: ClusterEndpointPrivacy::Relayed,
        priority: 120,
        last_observed_latency_ms: None,
    });
    let selected = RoamingConnectionPlanner::select_path(&[turn.clone(), webrtc.clone()])
        .expect("roaming path");

    assert!(matches!(
        selected,
        RoamingPathSelection::WebRtc(PeerEndpointCandidate { endpoint, .. })
            if endpoint.address == "webrtc://srflx"
    ));

    let failed_webrtc = webrtc.failed("ice failed");
    let fallback =
        RoamingConnectionPlanner::select_path(&[failed_webrtc, turn]).expect("fallback path");

    assert!(matches!(
        fallback,
        RoamingPathSelection::Relay(PeerEndpointCandidate { endpoint, .. })
            if endpoint.transport == ClusterTransportKind::TurnRelay
    ));
}

#[test]
fn roaming_connection_planner_prefers_iroh_before_webrtc_and_relay() {
    let node_id = NodeId::from_legacy_named("node_mobile").expect("node");
    let iroh = PeerEndpointCandidate::discovered(ClusterEndpointRecord {
        endpoint_id: ClusterEndpointId::from_legacy_named("endpoint_iroh").expect("endpoint"),
        node_id: node_id.clone(),
        transport: ClusterTransportKind::Iroh,
        address: "iroh://iroh_public_key?relay=https://relay.example".to_string(),
        source: ClusterEndpointSource::Rendezvous,
        privacy: ClusterEndpointPrivacy::Relayed,
        priority: 90,
        last_observed_latency_ms: None,
    });
    let webrtc = PeerEndpointCandidate::discovered(ClusterEndpointRecord {
        endpoint_id: ClusterEndpointId::from_legacy_named("endpoint_webrtc").expect("endpoint"),
        node_id: node_id.clone(),
        transport: ClusterTransportKind::WebRtc,
        address: "webrtc://srflx".to_string(),
        source: ClusterEndpointSource::Ice,
        privacy: ClusterEndpointPrivacy::InternetDirect,
        priority: 10,
        last_observed_latency_ms: None,
    });
    let relay = PeerEndpointCandidate::discovered(ClusterEndpointRecord {
        endpoint_id: ClusterEndpointId::from_legacy_named("endpoint_turn").expect("endpoint"),
        node_id,
        transport: ClusterTransportKind::TurnRelay,
        address: "turn://relay".to_string(),
        source: ClusterEndpointSource::Rendezvous,
        privacy: ClusterEndpointPrivacy::Relayed,
        priority: 5,
        last_observed_latency_ms: None,
    });

    let selected =
        RoamingConnectionPlanner::select_path(&[relay, webrtc, iroh]).expect("roaming path");

    assert!(matches!(
        selected,
        RoamingPathSelection::Iroh(PeerEndpointCandidate { endpoint, .. })
            if endpoint.transport == ClusterTransportKind::Iroh
    ));
}

#[test]
fn routes_session_events_to_participants() {
    let session_id = SessionId::from_legacy_named("sess_demo").expect("session");
    let mut registry = PeerRegistry::new();
    registry.register_node(node("node_mobile"));
    registry.register_node(node("node_desktop"));
    registry
        .join_session(
            session_id.clone(),
            NodeId::from_legacy_named("node_mobile").expect("node"),
        )
        .expect("join mobile");
    registry
        .join_session(
            session_id.clone(),
            NodeId::from_legacy_named("node_desktop").expect("node"),
        )
        .expect("join desktop");

    let event = EventEnvelope::new(
        EventId::from_legacy_named("evt_route").expect("event"),
        time::OffsetDateTime::now_utc(),
        NodeId::from_legacy_named("node_mobile").expect("node"),
        Source::new("test.route").expect("source"),
        EventKind::new("evolution.job.scoped").expect("kind"),
        Salience::new(0.8).expect("salience"),
        Confidence::new(1.0).expect("confidence"),
        RoutingMetadata::new(
            RoutingScope::Session,
            LatencyClass::Background,
            PrivacyClass::Derived,
        ),
    )
    .with_session(session_id);

    let mut store = InMemoryEventStore::new();
    let receipt = store.append(event).expect("append");
    let stored = store
        .replay(ReplayQuery::all().after(EventSequence(receipt.sequence.0 - 1)))
        .expect("replay")
        .pop()
        .expect("stored event");

    let mut router = InProcessRouter::new(registry);
    let deliveries = router.route_event(&stored).expect("route");
    assert_eq!(deliveries.len(), 2);
}

#[test]
fn in_process_transport_delivers_typed_frames() {
    let node_a = NodeId::from_legacy_named("node_a").expect("node");
    let node_b = NodeId::from_legacy_named("node_b").expect("node");
    let network = InProcessNetwork::new();
    let mut transport_a = network.add_node(node_a.clone());
    let mut transport_b = network.add_node(node_b.clone());
    let event = fixture_event("evt_transport", node_a.clone());

    let frame = NetworkFrame {
        frame_id: NetworkFrameId(1),
        source_node_id: node_a.clone(),
        target_node_id: node_b.clone(),
        message: MeshMessage {
            message_id: MeshMessageId(1),
            origin_node_id: node_a,
            target_node_id: node_b.clone(),
            body: MeshMessageBody::Event {
                delivery_id: RouteDeliveryId(1),
                event: Box::new(event.clone()),
            },
        },
    };

    transport_a
        .send(&node_b, frame.clone())
        .expect("send frame");
    let inbound = transport_b.drain_incoming().expect("drain");

    assert_eq!(inbound.len(), 1);
    assert_eq!(inbound[0].received_by, node_b);
    assert_eq!(inbound[0].frame, frame);
}

#[test]
fn in_process_transport_rejects_unknown_targets() {
    let node_a = NodeId::from_legacy_named("node_a").expect("node");
    let node_missing = NodeId::from_legacy_named("node_missing").expect("node");
    let network = InProcessNetwork::new();
    let mut transport_a = network.add_node(node_a.clone());
    let event = fixture_event("evt_missing_target", node_a.clone());

    let error = transport_a
        .send(
            &node_missing,
            NetworkFrame {
                frame_id: NetworkFrameId(1),
                source_node_id: node_a.clone(),
                target_node_id: node_missing.clone(),
                message: MeshMessage {
                    message_id: MeshMessageId(1),
                    origin_node_id: node_a,
                    target_node_id: node_missing.clone(),
                    body: MeshMessageBody::Event {
                        delivery_id: RouteDeliveryId(1),
                        event: Box::new(event),
                    },
                },
            },
        )
        .expect_err("missing target should fail");

    assert_eq!(
        error,
        NetworkError::UnknownNode {
            node_id: node_missing
        }
    );
}

#[test]
fn network_service_delivers_event_and_processes_ack() {
    let node_a = NodeId::from_legacy_named("node_a").expect("node");
    let node_b = NodeId::from_legacy_named("node_b").expect("node");
    let session_id = SessionId::from_legacy_named("sess_network").expect("session");
    let network = InProcessNetwork::new();
    let transport_a = network.add_node(node_a.clone());
    let transport_b = network.add_node(node_b.clone());
    let mut registry = PeerRegistry::new();
    registry.register_node(node("node_a"));
    registry.register_node(node("node_b"));
    registry
        .join_session(session_id.clone(), node_a.clone())
        .expect("join a");
    registry
        .join_session(session_id.clone(), node_b.clone())
        .expect("join b");
    let mut service_a = NetworkService::new(registry.clone(), transport_a);
    let mut service_b = NetworkService::new(registry, transport_b);
    let mut store_a = InMemoryEventStore::new();
    let mut store_b = InMemoryEventStore::new();
    let event = fixture_event("evt_service_delivery", node_a.clone()).with_session(session_id);
    let receipt = store_a.append(event).expect("append");
    let stored = store_a
        .replay(ReplayQuery::all().after(EventSequence(receipt.sequence.0 - 1)))
        .expect("replay")
        .pop()
        .expect("stored");

    let deliveries = service_a.enqueue_event(&stored).expect("enqueue");
    assert_eq!(deliveries.len(), 2);
    assert_eq!(service_a.pump_outbound().expect("send"), 2);

    let outcomes = service_b
        .process_incoming(&mut store_b)
        .expect("process event");
    assert_eq!(outcomes.len(), 1);
    assert_eq!(store_b.len(), 1);

    let ack_outcomes = service_a
        .process_incoming(&mut store_a)
        .expect("process ack");
    assert_eq!(ack_outcomes.len(), 2);
    assert_eq!(store_a.len(), 1);
    assert_eq!(
        ack_outcomes
            .iter()
            .filter(|outcome| matches!(outcome, InboundOutcome::AckAccepted { .. }))
            .count(),
        1
    );
    let delivered = service_a
        .ledger()
        .records()
        .into_iter()
        .filter(|record| record.delivery.status == DeliveryStatus::Delivered)
        .count();
    assert_eq!(delivered, 1);
}

#[test]
fn delivery_ledger_suppresses_duplicate_event_targets() {
    let node_a = NodeId::from_legacy_named("node_a").expect("node");
    let node_b = NodeId::from_legacy_named("node_b").expect("node");
    let network = InProcessNetwork::new();
    let transport_a = network.add_node(node_a.clone());
    let mut registry = PeerRegistry::new();
    registry.register_node(node("node_a"));
    registry.register_node(node("node_b"));
    let mut service = NetworkService::new(registry, transport_a);
    let mut store = InMemoryEventStore::new();
    let event = fixture_event("evt_duplicate_delivery", node_a.clone()).with_target_node(node_b);
    let receipt = store.append(event).expect("append");
    let stored = store
        .replay(ReplayQuery::all().after(EventSequence(receipt.sequence.0 - 1)))
        .expect("replay")
        .pop()
        .expect("stored");

    assert_eq!(service.enqueue_event(&stored).expect("first").len(), 1);
    assert_eq!(service.enqueue_event(&stored).expect("duplicate").len(), 0);
    assert_eq!(service.ledger().records().len(), 1);
}

#[test]
fn inbound_duplicate_event_is_acked_without_duplicate_append() {
    let node_a = NodeId::from_legacy_named("node_a").expect("node");
    let node_b = NodeId::from_legacy_named("node_b").expect("node");
    let network = InProcessNetwork::new();
    let transport_a = network.add_node(node_a.clone());
    let transport_b = network.add_node(node_b.clone());
    let mut registry = PeerRegistry::new();
    registry.register_node(node("node_a"));
    registry.register_node(node("node_b"));
    let mut service_a = NetworkService::new(registry.clone(), transport_a);
    let mut service_b = NetworkService::new(registry, transport_b);
    let event = fixture_event("evt_duplicate_inbound", node_a.clone()).with_target_node(node_b);
    let mut store_b = InMemoryEventStore::new();

    for frame_id in [1, 2] {
        let frame = NetworkFrame {
            frame_id: NetworkFrameId(frame_id),
            source_node_id: node_a.clone(),
            target_node_id: service_b.transport.local_node_id().clone(),
            message: MeshMessage {
                message_id: MeshMessageId(frame_id),
                origin_node_id: node_a.clone(),
                target_node_id: service_b.transport.local_node_id().clone(),
                body: MeshMessageBody::Event {
                    delivery_id: RouteDeliveryId(1),
                    event: Box::new(event.clone()),
                },
            },
        };
        service_a
            .transport
            .send(service_b.transport.local_node_id(), frame)
            .expect("send duplicate");
    }

    let outcomes = service_b
        .process_incoming(&mut store_b)
        .expect("process duplicates");
    assert!(matches!(outcomes[0], InboundOutcome::EventAccepted { .. }));
    assert!(matches!(outcomes[1], InboundOutcome::EventDuplicate { .. }));
    assert_eq!(store_b.len(), 1);
}

#[test]
fn retry_policy_marks_delivery_failed_after_max_attempts() {
    let node_a = NodeId::from_legacy_named("node_a").expect("node");
    let node_b = NodeId::from_legacy_named("node_b").expect("node");
    let network = InProcessNetwork::new();
    let transport_a = network.add_node(node_a.clone());
    let mut registry = PeerRegistry::new();
    registry.register_node(node("node_a"));
    registry.register_node(node("node_b"));
    let mut service = NetworkService::new(registry, transport_a)
        .with_retry_policy(RetryPolicy { max_attempts: 2 });
    let mut store = InMemoryEventStore::new();
    let event = fixture_event("evt_retry_failed", node_a.clone()).with_target_node(node_b);
    let receipt = store.append(event).expect("append");
    let stored = store
        .replay(ReplayQuery::all().after(EventSequence(receipt.sequence.0 - 1)))
        .expect("replay")
        .pop()
        .expect("stored");
    let delivery = service.enqueue_event(&stored).expect("enqueue")[0].clone();

    assert!(service.pump_outbound().is_ok());
    assert_eq!(
        service
            .ledger()
            .record(&delivery.delivery_id)
            .unwrap()
            .delivery
            .status,
        DeliveryStatus::Retrying
    );
    let error = service
        .pump_outbound()
        .expect_err("final attempt should return send failure");
    assert!(matches!(error, NetworkError::UnknownNode { .. }));
    assert_eq!(
        service
            .ledger()
            .record(&delivery.delivery_id)
            .unwrap()
            .delivery
            .status,
        DeliveryStatus::Failed
    );
}

#[test]
fn ack_rejects_wrong_target_node() {
    let node_a = NodeId::from_legacy_named("node_a").expect("node");
    let node_b = NodeId::from_legacy_named("node_b").expect("node");
    let node_c = NodeId::from_legacy_named("node_c").expect("node");
    let mut ledger = DeliveryLedger::new(RetryPolicy::default());
    let event = fixture_event("evt_wrong_ack", node_a);
    let delivery = RouteDelivery {
        delivery_id: RouteDeliveryId(1),
        event_sequence: EventSequence(1),
        event_id: event.event_id.clone(),
        target_node_id: node_b.clone(),
        latency_class: LatencyClass::Interactive,
        status: DeliveryStatus::Queued,
    };
    ledger.enqueue(delivery, event).expect("enqueue");

    let error = ledger
        .ack(RouteDeliveryId(1), &node_c)
        .expect_err("wrong target should fail");
    assert!(matches!(error, NetworkError::AckTargetMismatch { .. }));
    assert_eq!(
        ledger
            .record(&RouteDeliveryId(1))
            .unwrap()
            .delivery
            .target_node_id,
        node_b
    );
}

#[test]
fn heartbeat_marks_peer_healthy_and_updates_node_health() {
    let node_a = NodeId::from_legacy_named("node_a").expect("node");
    let mut registry = PeerRegistry::new();
    registry.register_node(node("node_a"));

    let status = registry.record_heartbeat(&node_a, 7).expect("heartbeat");

    assert_eq!(status, PeerStatus::Healthy);
    assert_eq!(
        registry
            .peer_state(&node_a)
            .expect("peer state")
            .link
            .last_seen_tick,
        Some(7)
    );
    assert_eq!(
        registry.node(&node_a).expect("node").health.availability,
        NodeAvailability::Available
    );
}

#[test]
fn missed_heartbeats_degrade_then_mark_offline_without_removing_session() {
    let node_a = NodeId::from_legacy_named("node_a").expect("node");
    let session_id = SessionId::from_legacy_named("sess_heartbeat").expect("session");
    let mut registry = PeerRegistry::new();
    registry.register_node(node("node_a"));
    registry
        .join_session(session_id.clone(), node_a.clone())
        .expect("join");

    assert_eq!(
        registry
            .record_missed_heartbeat(&node_a, HeartbeatPolicy::default())
            .expect("missed one"),
        PeerStatus::Degraded
    );
    assert_eq!(
        registry
            .record_missed_heartbeat(&node_a, HeartbeatPolicy::default())
            .expect("missed two"),
        PeerStatus::Suspect
    );
    assert_eq!(
        registry
            .record_missed_heartbeat(&node_a, HeartbeatPolicy::default())
            .expect("missed three"),
        PeerStatus::Offline
    );
    assert_eq!(
        registry.node(&node_a).expect("node").health.availability,
        NodeAvailability::Offline
    );
    assert_eq!(registry.session_nodes(&session_id), vec![node_a]);
}

#[test]
fn heartbeat_recovery_requeues_failed_delivery_for_drain() {
    let node_a = NodeId::from_legacy_named("node_a").expect("node");
    let node_b = NodeId::from_legacy_named("node_b").expect("node");
    let network = InProcessNetwork::new();
    let transport_a = network.add_node(node_a.clone());
    let mut registry = PeerRegistry::new();
    registry.register_node(node("node_a"));
    registry.register_node(node("node_b"));
    let mut service_a = NetworkService::new(registry.clone(), transport_a)
        .with_retry_policy(RetryPolicy { max_attempts: 2 });
    let mut store_a = InMemoryEventStore::new();
    let event =
        fixture_event("evt_recovery_requeue", node_a.clone()).with_target_node(node_b.clone());
    let receipt = store_a.append(event).expect("append");
    let stored = store_a
        .replay(ReplayQuery::all().after(EventSequence(receipt.sequence.0 - 1)))
        .expect("replay")
        .pop()
        .expect("stored");
    let delivery_id = service_a.enqueue_event(&stored).expect("enqueue")[0].delivery_id;

    assert_eq!(service_a.pump_outbound().expect("first attempt"), 0);
    assert!(service_a.pump_outbound().is_err());
    assert_eq!(
        service_a
            .ledger()
            .record(&delivery_id)
            .expect("record")
            .delivery
            .status,
        DeliveryStatus::Failed
    );

    let transport_b = network.add_node(node_b.clone());
    let mut service_b = NetworkService::new(registry, transport_b);
    let mut store_b = InMemoryEventStore::new();
    service_b
        .send_heartbeat(&node_a, 42)
        .expect("send heartbeat");

    let outcomes = service_a
        .process_incoming(&mut store_a)
        .expect("process heartbeat");
    assert!(matches!(
        outcomes.as_slice(),
        [InboundOutcome::HeartbeatReceived {
            status: PeerStatus::Healthy,
            requeued: 1,
            ..
        }]
    ));
    assert_eq!(
        service_a
            .ledger()
            .record(&delivery_id)
            .expect("record")
            .delivery
            .status,
        DeliveryStatus::Retrying
    );

    assert_eq!(service_a.pump_outbound().expect("drain recovered"), 1);
    service_b
        .process_incoming(&mut store_b)
        .expect("receive recovered event");
    assert_eq!(store_b.len(), 1);
}

#[test]
fn session_routing_excludes_offline_participants() {
    let node_a = NodeId::from_legacy_named("node_a").expect("node");
    let node_b = NodeId::from_legacy_named("node_b").expect("node");
    let session_id = SessionId::from_legacy_named("sess_offline_route").expect("session");
    let mut registry = PeerRegistry::new();
    registry.register_node(node("node_a"));
    registry.register_node(node("node_b"));
    registry
        .join_session(session_id.clone(), node_a.clone())
        .expect("join a");
    registry
        .join_session(session_id.clone(), node_b.clone())
        .expect("join b");
    for _ in 0..3 {
        registry
            .record_missed_heartbeat(&node_b, HeartbeatPolicy::default())
            .expect("miss heartbeat");
    }
    let mut store = InMemoryEventStore::new();
    let event = fixture_event("evt_skip_offline", node_a.clone()).with_session(session_id);
    let receipt = store.append(event).expect("append");
    let stored = store
        .replay(ReplayQuery::all().after(EventSequence(receipt.sequence.0 - 1)))
        .expect("replay")
        .pop()
        .expect("stored");

    let mut router = InProcessRouter::new(registry);
    let deliveries = router.route_event(&stored).expect("route");

    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].target_node_id, node_a);
}

#[test]
fn direct_target_offline_buffers_instead_of_sending() {
    let node_a = NodeId::from_legacy_named("node_a").expect("node");
    let node_b = NodeId::from_legacy_named("node_b").expect("node");
    let network = InProcessNetwork::new();
    let transport_a = network.add_node(node_a.clone());
    let mut transport_b = network.add_node(node_b.clone());
    let mut registry = PeerRegistry::new();
    registry.register_node(node("node_a"));
    registry.register_node(node("node_b"));
    for _ in 0..3 {
        registry
            .record_missed_heartbeat(&node_b, HeartbeatPolicy::default())
            .expect("miss heartbeat");
    }
    let mut service = NetworkService::new(registry, transport_a);
    let mut store = InMemoryEventStore::new();
    let event =
        fixture_event("evt_direct_offline", node_a.clone()).with_target_node(node_b.clone());
    let receipt = store.append(event).expect("append");
    let stored = store
        .replay(ReplayQuery::all().after(EventSequence(receipt.sequence.0 - 1)))
        .expect("replay")
        .pop()
        .expect("stored");
    let delivery_id = service.enqueue_event(&stored).expect("enqueue")[0].delivery_id;

    assert_eq!(service.pump_outbound().expect("buffer offline"), 0);
    assert_eq!(transport_b.drain_incoming().expect("drain").len(), 0);
    assert_eq!(
        service
            .ledger()
            .record(&delivery_id)
            .expect("record")
            .delivery
            .status,
        DeliveryStatus::Retrying
    );
}

#[test]
fn trusted_network_routing_skips_offline_peers() {
    let node_a = NodeId::from_legacy_named("node_a").expect("node");
    let node_b = NodeId::from_legacy_named("node_b").expect("node");
    let node_c = NodeId::from_legacy_named("node_c").expect("node");
    let mut registry = PeerRegistry::new();
    registry.register_node(node("node_a"));
    registry.register_node(node("node_b"));
    registry.register_node(node("node_c"));
    for _ in 0..3 {
        registry
            .record_missed_heartbeat(&node_b, HeartbeatPolicy::default())
            .expect("miss heartbeat");
    }
    registry
        .record_heartbeat(&node_c, 1)
        .expect("mark c healthy");
    let mut event = fixture_event("evt_trusted_network", node_a);
    event.routing.scope = RoutingScope::TrustedNetwork;
    let mut store = InMemoryEventStore::new();
    let receipt = store.append(event).expect("append");
    let stored = store
        .replay(ReplayQuery::all().after(EventSequence(receipt.sequence.0 - 1)))
        .expect("replay")
        .pop()
        .expect("stored");

    let mut router = InProcessRouter::new(registry);
    let deliveries = router.route_event(&stored).expect("route");
    let targets: BTreeSet<NodeId> = deliveries
        .into_iter()
        .map(|delivery| delivery.target_node_id)
        .collect();

    assert_eq!(
        targets,
        BTreeSet::from([stored.event.origin_node_id, node_c])
    );
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum LocalhostKvRequest {
    Apply { command: KvCommand },
    Get { key: String },
    Snapshot,
    InstallSnapshot { snapshot: KvSnapshot },
    Stop,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum LocalhostKvResponse {
    Revision { revision: KvRevision },
    Entry { entry: Option<KvEntry> },
    Snapshot { snapshot: KvSnapshot },
    Ok,
    Error { message: String },
}

struct LocalhostKvNode {
    address: String,
    handle: Option<JoinHandle<()>>,
}

impl LocalhostKvNode {
    fn start() -> Self {
        let listener = DaemonControlListener::bind("127.0.0.1:0").expect("bind localhost kv node");
        let address = listener.local_addr_string().expect("local addr");
        let store = Arc::new(Mutex::new(InProcessKvStore::new()));
        let handle_store = Arc::clone(&store);
        let handle = thread::spawn(move || {
            loop {
                let mut stream = listener
                    .accept()
                    .expect("accept localhost kv request")
                    .expect("blocking listener accepted stream");
                let request = read_localhost_kv_request(&mut stream);
                let should_stop = matches!(request, Ok(LocalhostKvRequest::Stop));
                let response = match request {
                    Ok(request) => handle_localhost_kv_request(&handle_store, request),
                    Err(error) => LocalhostKvResponse::Error {
                        message: error.to_string(),
                    },
                };
                write_object_frame_blocking(&mut stream, &response)
                    .expect("write localhost kv response");
                if should_stop {
                    break;
                }
            }
        });

        Self {
            address,
            handle: Some(handle),
        }
    }

    fn address(&self) -> &str {
        &self.address
    }

    fn stop(mut self) {
        let _ = send_localhost_kv_request(self.address(), LocalhostKvRequest::Stop);
        if let Some(handle) = self.handle.take() {
            handle.join().expect("join localhost kv node");
        }
    }
}

fn read_localhost_kv_request(
    stream: &mut DaemonControlStream,
) -> Result<LocalhostKvRequest, NetworkError> {
    read_object_frame_blocking(stream, IROH_DAEMON_COMMAND_MAX_BYTES)
}

fn handle_localhost_kv_request(
    store: &Arc<Mutex<InProcessKvStore>>,
    request: LocalhostKvRequest,
) -> LocalhostKvResponse {
    let mut store = store.lock().expect("lock kv store");
    match request {
        LocalhostKvRequest::Apply { command } => match store.apply_command(command) {
            Ok(revision) => LocalhostKvResponse::Revision { revision },
            Err(error) => LocalhostKvResponse::Error {
                message: error.to_string(),
            },
        },
        LocalhostKvRequest::Get { key } => LocalhostKvResponse::Entry {
            entry: store.get(&key).cloned(),
        },
        LocalhostKvRequest::Snapshot => LocalhostKvResponse::Snapshot {
            snapshot: store.snapshot(),
        },
        LocalhostKvRequest::InstallSnapshot { snapshot } => {
            *store = InProcessKvStore::restore(snapshot);
            LocalhostKvResponse::Ok
        }
        LocalhostKvRequest::Stop => LocalhostKvResponse::Ok,
    }
}

fn send_localhost_kv_request(
    address: &str,
    request: LocalhostKvRequest,
) -> Result<LocalhostKvResponse, String> {
    let mut stream = DaemonControlStream::connect(address).map_err(|source| source.to_string())?;
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .map_err(|source| source.to_string())?;
    stream
        .set_write_timeout(Some(Duration::from_secs(2)))
        .map_err(|source| source.to_string())?;
    write_object_frame_blocking(&mut stream, &request).map_err(|source| source.to_string())?;

    let response: LocalhostKvResponse =
        read_object_frame_blocking(&mut stream, IROH_DAEMON_COMMAND_MAX_BYTES)
            .map_err(|source| source.to_string())?;
    if let LocalhostKvResponse::Error { message } = &response {
        return Err(message.clone());
    }
    Ok(response)
}

fn apply_to_localhost_kv(address: &str, command: KvCommand) -> KvRevision {
    match send_localhost_kv_request(address, LocalhostKvRequest::Apply { command })
        .expect("apply command over localhost")
    {
        LocalhostKvResponse::Revision { revision } => revision,
        response => panic!("unexpected apply response: {response:?}"),
    }
}

fn replicate_to_localhost_kv(addresses: &[String], command: KvCommand) -> Vec<KvRevision> {
    addresses
        .iter()
        .map(|address| apply_to_localhost_kv(address, command.clone()))
        .collect()
}

fn get_localhost_kv_entry(address: &str, key: &str) -> Option<KvEntry> {
    match send_localhost_kv_request(
        address,
        LocalhostKvRequest::Get {
            key: key.to_string(),
        },
    )
    .expect("get entry over localhost")
    {
        LocalhostKvResponse::Entry { entry } => entry,
        response => panic!("unexpected get response: {response:?}"),
    }
}

fn localhost_kv_snapshot(address: &str) -> KvSnapshot {
    match send_localhost_kv_request(address, LocalhostKvRequest::Snapshot)
        .expect("snapshot over localhost")
    {
        LocalhostKvResponse::Snapshot { snapshot } => snapshot,
        response => panic!("unexpected snapshot response: {response:?}"),
    }
}

fn install_localhost_kv_snapshot(address: &str, snapshot: KvSnapshot) {
    match send_localhost_kv_request(address, LocalhostKvRequest::InstallSnapshot { snapshot })
        .expect("install snapshot over localhost")
    {
        LocalhostKvResponse::Ok => {}
        response => panic!("unexpected install snapshot response: {response:?}"),
    }
}

fn wait_for_localhost_kv_entry(address: &str, key: &str, expected: &[u8]) -> KvEntry {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if let Some(entry) = get_localhost_kv_entry(address, key) {
            if entry.value == expected {
                return entry;
            }
        }
        assert!(
            Instant::now() < deadline,
            "entry {key} did not converge on {address}"
        );
        thread::sleep(Duration::from_millis(20));
    }
}

fn localhost_kv_cluster_member(
    cluster_id: ClusterId,
    node_id: NodeId,
    display_name: &str,
) -> ClusterMemberRecord {
    ClusterMemberRecord::local_voter(
        cluster_id,
        node_id,
        display_name,
        format!("pk_{display_name}"),
        time::OffsetDateTime::now_utc(),
    )
}

fn start_three_localhost_kv_nodes() -> (Vec<LocalhostKvNode>, Vec<String>) {
    let nodes = vec![
        LocalhostKvNode::start(),
        LocalhostKvNode::start(),
        LocalhostKvNode::start(),
    ];
    let addresses = nodes
        .iter()
        .map(|node| node.address().to_string())
        .collect();
    (nodes, addresses)
}

fn stop_localhost_kv_nodes(nodes: Vec<LocalhostKvNode>) {
    for node in nodes {
        node.stop();
    }
}

#[test]
fn localhost_kv_cluster_replicates_membership_and_kv_operations_over_network_object_frames() {
    let (nodes, addresses) = start_three_localhost_kv_nodes();
    let cluster_id = ClusterId::from_legacy_named("cluster_localhost").expect("cluster");
    let node_ids = [
        NodeId::from_legacy_named("node_alpha").expect("node"),
        NodeId::from_legacy_named("node_beta").expect("node"),
        NodeId::from_legacy_named("node_gamma").expect("node"),
    ];

    for (node_id, display_name) in node_ids.iter().zip(["alpha", "beta", "gamma"]) {
        let member = localhost_kv_cluster_member(cluster_id.clone(), node_id.clone(), display_name);
        replicate_to_localhost_kv(&addresses, KvCommand::PutClusterMember(member));
    }
    replicate_to_localhost_kv(
        &addresses,
        KvCommand::Op(KvOp::LeaseGrant {
            lease_id: LeaseId(1),
            ttl_ticks: 10,
        }),
    );
    replicate_to_localhost_kv(
        &addresses,
        KvCommand::Op(KvOp::Put {
            key: "/services/kv/node_alpha".to_string(),
            value: addresses[0].as_bytes().to_vec(),
            lease_id: Some(LeaseId(1)),
        }),
    );
    let revisions = replicate_to_localhost_kv(
        &addresses,
        KvCommand::Txn(KvTxn {
            compares: vec![KvCompare::Missing {
                key: "/config/epoch".to_string(),
            }],
            success: vec![KvOp::Put {
                key: "/config/epoch".to_string(),
                value: b"1".to_vec(),
                lease_id: None,
            }],
            failure: vec![],
        }),
    );

    assert_eq!(revisions, vec![KvRevision(5), KvRevision(5), KvRevision(5)]);
    for address in &addresses {
        for node_id in &node_ids {
            assert!(
                get_localhost_kv_entry(address, &cluster_member_key(node_id)).is_some(),
                "missing member {node_id} on {address}"
            );
        }
        assert_eq!(
            get_localhost_kv_entry(address, "/config/epoch")
                .expect("epoch")
                .value,
            b"1"
        );
        assert_eq!(
            get_localhost_kv_entry(address, "/services/kv/node_alpha")
                .expect("service")
                .lease_id,
            Some(LeaseId(1))
        );
    }

    stop_localhost_kv_nodes(nodes);
}

#[test]
fn localhost_kv_cluster_catches_up_delayed_node_from_snapshot_over_network_object_frames() {
    let (nodes, addresses) = start_three_localhost_kv_nodes();
    let first_two = addresses[..2].to_vec();

    replicate_to_localhost_kv(
        &first_two,
        KvCommand::Op(KvOp::Put {
            key: "/config/a".to_string(),
            value: b"one".to_vec(),
            lease_id: None,
        }),
    );
    replicate_to_localhost_kv(
        &first_two,
        KvCommand::Op(KvOp::Put {
            key: "/config/b".to_string(),
            value: b"two".to_vec(),
            lease_id: None,
        }),
    );

    assert!(get_localhost_kv_entry(&addresses[2], "/config/b").is_none());
    install_localhost_kv_snapshot(&addresses[2], localhost_kv_snapshot(&addresses[0]));
    wait_for_localhost_kv_entry(&addresses[2], "/config/b", b"two");

    replicate_to_localhost_kv(
        &addresses,
        KvCommand::Op(KvOp::Put {
            key: "/config/c".to_string(),
            value: b"three".to_vec(),
            lease_id: None,
        }),
    );

    let snapshots: Vec<KvSnapshot> = addresses
        .iter()
        .map(|address| localhost_kv_snapshot(address))
        .collect();
    assert!(
        snapshots
            .iter()
            .all(|snapshot| snapshot.revision == KvRevision(3))
    );
    for address in &addresses {
        assert_eq!(
            wait_for_localhost_kv_entry(address, "/config/a", b"one").value,
            b"one"
        );
        assert_eq!(
            wait_for_localhost_kv_entry(address, "/config/b", b"two").value,
            b"two"
        );
        assert_eq!(
            wait_for_localhost_kv_entry(address, "/config/c", b"three").value,
            b"three"
        );
    }

    stop_localhost_kv_nodes(nodes);
}
