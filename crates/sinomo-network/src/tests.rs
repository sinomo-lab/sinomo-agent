use crate::prelude::*;
use crate::*;

use sinomo_core::{
    CapabilityName, ClusterCertificate, ClusterEndpointPrivacy, ClusterEndpointRecord,
    ClusterEndpointSource, ClusterMemberRecord, ClusterMemberRole, ClusterMemberState,
    ClusterNodeAvailability, ClusterNodeRolloutStatus, ClusterNodeStatusRecord,
    ClusterTransportKind, Confidence, EventEnvelope, EventKind, LatencyClass, PrivacyClass,
    RollbackPolicy, RolloutPlan, RolloutStrategy, RoutingMetadata, RoutingScope, Salience, Source,
    TrustRing,
};
use sinomo_events::{EventId, EventStore, InMemoryEventStore, ReplayQuery, SessionId};
use sinomo_kv::{
    InProcessKvStore, KvCommand, KvCompare, KvEntry, KvError, KvEventKind, KvOp, KvRevision,
    KvSnapshot, KvTxn, LeaseId, RaftAppendEntriesRequest, RaftSnapshot, cluster_member_key,
};
use sinomo_node::NodeId;
use std::io::Write;
use std::sync::{Arc, Barrier, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

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
fn cluster_kv_txn_rpc_preserves_nested_sobj_values_as_raw_bytes() {
    #[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
    struct ClusterConfigFixture {
        name: String,
        value: u64,
    }

    let record = ClusterConfigFixture {
        name: "runtime".to_string(),
        value: 42,
    };
    let op = KvOp::put_serde("/config/test/runtime", &record, None).expect("typed kv op");
    let raw_value = match &op {
        KvOp::Put { value, .. } => value.clone(),
        _ => unreachable!("put_serde must produce a put op"),
    };
    assert!(raw_value.starts_with(sinomo_object::MAGIC));
    assert!(
        !raw_value.starts_with(b"U09CSg"),
        "typed KV values must be raw SOBJ bytes, not base64 text"
    );

    let txn = KvTxn {
        compares: Vec::new(),
        success: vec![op],
        failure: Vec::new(),
    };
    let request =
        ObjectRpcRequest::encode_body(CLUSTER_KV_TXN_ROUTE, CLUSTER_KV_RPC_CATEGORY, &txn)
            .expect("encode txn request");
    assert!(
        request
            .body
            .windows(raw_value.len())
            .any(|window| window == raw_value.as_slice()),
        "cluster RPC encoding must embed nested KV values verbatim instead of base64 wrapping them"
    );

    let decoded: KvTxn = request.decode_body().expect("decode txn request");
    let decoded_value = match decoded.success.first().expect("put op") {
        KvOp::Put { value, .. } => value,
        _ => unreachable!("expected put op"),
    };
    assert_eq!(decoded_value, &raw_value);
    assert!(decoded_value.starts_with(sinomo_object::MAGIC));
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
fn interface_ip_filter_keeps_routable_lan_addresses() {
    assert!(is_advertisable_interface_ip(
        "10.14.5.15".parse().expect("wireguard lan")
    ));
    assert!(is_advertisable_interface_ip(
        "192.168.1.154".parse().expect("ethernet lan")
    ));
    assert!(!is_advertisable_interface_ip(
        "169.254.147.92".parse().expect("link local")
    ));
    assert!(!is_advertisable_interface_ip(
        "127.0.0.1".parse().expect("loopback")
    ));
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
fn default_invite_compaction_keeps_lan_public_iroh_order() {
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
    assert_eq!(introducers[0].address, "10.0.0.41:21353");
    assert_eq!(introducers[1].address, "203.0.113.41:21353");
    assert_eq!(introducers[2].node_id, local);
    assert!(
        introducers[2]
            .address
            .starts_with("iroh://iroh_public_key_local")
    );
}

#[test]
fn default_invite_sort_prefers_lan_direct_over_public_direct() {
    let node = NodeId::from_legacy_named("node_invite_lan_direct").expect("node");
    let private = invite_introducer_endpoint(&node, "10.0.0.41:21353".to_string());
    let public = invite_introducer_endpoint(&node, "203.0.113.41:21353".to_string());
    let mut introducers = vec![public, private.clone()];

    sort_default_invite_introducers(&mut introducers);

    assert_eq!(introducers.first(), Some(&private));
}

#[test]
fn join_registration_channels_keep_lan_before_iroh_fallback() {
    let cluster_id = ClusterId::from_legacy_named("cluster_invite_lan_first").expect("cluster");
    let node_id = NodeId::from_legacy_named("node_invite_lan_first").expect("node");
    let lan_endpoint = ClusterEndpointRecord {
        endpoint_id: ClusterEndpointId::from_legacy_named("endpoint_invite_lan_first_lan")
            .expect("endpoint"),
        node_id: node_id.clone(),
        transport: ClusterTransportKind::LanTcp,
        address: "10.0.0.10:21353".to_string(),
        source: ClusterEndpointSource::Static,
        privacy: ClusterEndpointPrivacy::Lan,
        priority: 20,
        last_observed_latency_ms: None,
    };
    let iroh_endpoint = ClusterEndpointRecord {
        endpoint_id: ClusterEndpointId::from_legacy_named("endpoint_invite_lan_first_iroh")
            .expect("endpoint"),
        node_id: node_id.clone(),
        transport: ClusterTransportKind::Iroh,
        address: "iroh://iroh_public_key_lan_first?relay=https://relay.example./".to_string(),
        source: ClusterEndpointSource::Rendezvous,
        privacy: ClusterEndpointPrivacy::Relayed,
        priority: 90,
        last_observed_latency_ms: None,
    };
    let invite = ClusterJoinInvite {
        invite_id: ClusterInviteId::from_legacy_named("invite_lan_first").expect("invite"),
        cluster_id,
        cluster_name: Some("lan first".to_string()),
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
    assert_eq!(channels[0].endpoint.transport, ClusterTransportKind::LanTcp);
    assert_eq!(channels[1].endpoint.transport, ClusterTransportKind::Iroh);
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
fn command_target_selection_deduplicates_identical_routes() {
    let node_id = NodeId::from_legacy_named("node_target_duplicate_route").expect("node");
    let direct = daemon_lan_endpoint(&node_id, "10.0.0.44:21353".to_string(), 20);
    let mut duplicate = direct.clone();
    duplicate.endpoint_id =
        ClusterEndpointId::from_legacy_named("endpoint_target_duplicate_route").expect("endpoint");

    let selected = prefer_bounded_daemon_command_targets(vec![direct.clone(), duplicate]);

    assert_eq!(selected, vec![direct]);
}

#[test]
fn command_target_selection_keeps_extra_tcp_before_iroh_fallbacks() {
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
    assert_eq!(selected[1], extra_tcp);
    assert_eq!(selected[2], iroh);
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
fn connectivity_sort_uses_iroh_after_direct_tcp_is_known_unavailable() {
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
fn connectivity_graph_keys_records_by_source_target_and_endpoint() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-connectivity-graph-key-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("paths");
    let now = OffsetDateTime::now_utc();
    let cluster_id =
        ClusterId::from_legacy_named("cluster_connectivity_graph_key").expect("cluster");
    let node_a = NodeId::from_legacy_named("node_connectivity_graph_key_a").expect("node a");
    let node_b = NodeId::from_legacy_named("node_connectivity_graph_key_b").expect("node b");
    let node_c = NodeId::from_legacy_named("node_connectivity_graph_key_c").expect("node c");
    let mut state = ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "connectivity graph key",
        node_a.clone(),
        "node a",
        "pk-a",
        now,
    );
    let shared_endpoint_id =
        ClusterEndpointId::from_legacy_named("endpoint_connectivity_graph_key_shared")
            .expect("endpoint");
    let endpoint_b = ClusterEndpointRecord {
        endpoint_id: shared_endpoint_id.clone(),
        node_id: node_b.clone(),
        transport: ClusterTransportKind::LanTcp,
        address: "10.0.0.41:21353".to_string(),
        source: ClusterEndpointSource::Observed,
        privacy: ClusterEndpointPrivacy::Lan,
        priority: 1,
        last_observed_latency_ms: None,
    };
    let endpoint_c = ClusterEndpointRecord {
        endpoint_id: shared_endpoint_id,
        node_id: node_c.clone(),
        transport: ClusterTransportKind::LanTcp,
        address: "10.0.0.42:21353".to_string(),
        source: ClusterEndpointSource::Observed,
        privacy: ClusterEndpointPrivacy::Lan,
        priority: 20,
        last_observed_latency_ms: None,
    };
    for (node_id, endpoint, public_key) in [
        (node_b.clone(), endpoint_b.clone(), "pk-b"),
        (node_c.clone(), endpoint_c.clone(), "pk-c"),
    ] {
        let mut member =
            ClusterMemberRecord::local_voter(cluster_id.clone(), node_id, "node", public_key, now);
        member.capabilities = main_daemon_capabilities(&member.role);
        member.allowed_capabilities = member.capabilities.clone();
        member.endpoints = vec![endpoint];
        state.members.push(member);
    }
    write_cluster_state(&paths, &state).expect("write state");
    sync_cluster_state_to_local_raft_unchecked(&paths, &state).expect("sync state");
    sync_cluster_connectivity_records_to_kv(
        &paths,
        &node_a,
        vec![cluster_connectivity_record(
            &state,
            &endpoint_b,
            ClusterConnectivityState::Unreachable,
            None,
            3,
            None,
            Some("failed".to_string()),
            now,
            60,
        )],
    )
    .expect("connectivity records");

    let targets = cluster_sync_targets_with_connectivity(&paths, &state);

    assert_eq!(
        targets.first().map(|endpoint| &endpoint.node_id),
        Some(&node_c)
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn cluster_connectivity_sync_is_noop_detects_stable_changed_and_removed() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-connectivity-noop-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("paths");
    let now = OffsetDateTime::now_utc();
    let cluster_id = ClusterId::from_legacy_named("cluster_conn_noop").expect("cluster");
    let node_a = NodeId::from_legacy_named("node_conn_noop_a").expect("node a");
    let node_b = NodeId::from_legacy_named("node_conn_noop_b").expect("node b");
    let mut state = ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "connectivity noop",
        node_a.clone(),
        "node a",
        "pk-a",
        now,
    );
    let endpoint_b = daemon_lan_endpoint(&node_b, "10.0.0.51:21353".to_string(), 10);
    let mut member_b =
        ClusterMemberRecord::local_voter(cluster_id.clone(), node_b.clone(), "node b", "pk-b", now);
    member_b.capabilities = main_daemon_capabilities(&member_b.role);
    member_b.allowed_capabilities = member_b.capabilities.clone();
    member_b.endpoints = vec![endpoint_b.clone()];
    state.members.push(member_b);
    write_cluster_state(&paths, &state).expect("write state");
    sync_cluster_state_to_local_raft_unchecked(&paths, &state).expect("sync state");

    let healthy = |observed_at| {
        cluster_connectivity_record(
            &state,
            &endpoint_b,
            ClusterConnectivityState::Healthy,
            Some(5),
            0,
            Some("ok".to_string()),
            None,
            observed_at,
            600,
        )
    };

    // First publish writes the record.
    assert!(
        !cluster_connectivity_sync_is_noop(&paths, &node_a, std::slice::from_ref(&healthy(now)))
            .expect("noop check"),
        "first publish should not be a no-op"
    );
    sync_cluster_connectivity_records_to_kv(&paths, &node_a, vec![healthy(now)])
        .expect("sync connectivity");

    // Re-publishing the same record (fresh observed_at, far from expiry) is a no-op.
    let later = now + time::Duration::seconds(5);
    assert!(
        cluster_connectivity_sync_is_noop(&paths, &node_a, std::slice::from_ref(&healthy(later)))
            .expect("noop check"),
        "stable record should be a no-op"
    );

    // A changed state is not a no-op.
    let degraded = cluster_connectivity_record(
        &state,
        &endpoint_b,
        ClusterConnectivityState::Degraded,
        Some(5),
        1,
        Some("ok".to_string()),
        Some("blip".to_string()),
        later,
        600,
    );
    assert!(
        !cluster_connectivity_sync_is_noop(&paths, &node_a, std::slice::from_ref(&degraded))
            .expect("noop check"),
        "changed state should not be a no-op"
    );

    // Dropping the record entirely (now-empty set) requires a delete, not a no-op.
    assert!(
        !cluster_connectivity_sync_is_noop(&paths, &node_a, &[]).expect("noop check"),
        "removed record should not be a no-op"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn local_connectivity_records_are_in_memory_not_disk() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-connectivity-memory-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("paths");
    let now = OffsetDateTime::now_utc();
    let cluster_id = ClusterId::from_legacy_named("cluster_conn_memory").expect("cluster");
    let node_a = NodeId::from_legacy_named("node_conn_memory_a").expect("node a");
    let node_b = NodeId::from_legacy_named("node_conn_memory_b").expect("node b");
    let state = ClusterBootstrapState::initialized_locally(
        cluster_id,
        "connectivity memory",
        node_a.clone(),
        "node a",
        "pk-a",
        now,
    );
    let endpoint_b = daemon_lan_endpoint(&node_b, "10.0.0.61:21353".to_string(), 10);
    let record = cluster_connectivity_record(
        &state,
        &endpoint_b,
        ClusterConnectivityState::Healthy,
        Some(4),
        0,
        Some("ok".to_string()),
        None,
        now,
        600,
    );

    write_local_cluster_connectivity_records(&paths, &node_a, std::slice::from_ref(&record))
        .expect("write local connectivity");

    assert!(
        !paths.connectivity_local.exists(),
        "local connectivity observations must not be persisted to disk"
    );
    assert_eq!(
        read_local_cluster_connectivity_records(&paths).expect("read local"),
        vec![record.clone()]
    );
    assert_eq!(
        read_cluster_connectivity_graph(&paths).expect("read merged graph"),
        vec![record]
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
fn raft_command_targets_try_unknown_tcp_before_unknown_iroh_fallbacks() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-raft-route-unknown-tcp-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("paths");
    let now = OffsetDateTime::now_utc();
    let cluster_id =
        ClusterId::from_legacy_named("cluster_network_raft_unknown_tcp").expect("cluster");
    let local = NodeId::from_legacy_named("node_network_raft_unknown_tcp_local").expect("local");
    let remote = NodeId::from_legacy_named("node_network_raft_unknown_tcp_remote").expect("remote");
    let mut state = ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "network raft routes",
        local,
        "local",
        "pk-local",
        now,
    );
    let public_tcp = daemon_lan_endpoint(&remote, "203.0.113.41:21353".to_string(), 10);
    let private_tcp = daemon_lan_endpoint(&remote, "10.0.0.41:21353".to_string(), 20);
    let iroh = ClusterEndpointRecord {
        endpoint_id: stable_iroh_endpoint_id(&remote, "iroh_public_key_network_raft_unknown_tcp"),
        node_id: remote.clone(),
        transport: ClusterTransportKind::Iroh,
        address: "iroh://iroh_public_key_network_raft_unknown_tcp?relay=https://relay.example./"
            .to_string(),
        source: ClusterEndpointSource::Rendezvous,
        privacy: ClusterEndpointPrivacy::Relayed,
        priority: 90,
        last_observed_latency_ms: None,
    };
    let mut member =
        ClusterMemberRecord::local_voter(cluster_id, remote, "remote", "pk-remote", now);
    member.capabilities = main_daemon_capabilities(&member.role);
    member.allowed_capabilities = member.capabilities.clone();
    member.endpoints = vec![public_tcp.clone(), private_tcp.clone(), iroh.clone()];
    state.members.push(member);
    write_cluster_state(&paths, &state).expect("write state");
    sync_cluster_state_to_local_raft_unchecked(&paths, &state).expect("sync state");

    let targets = cluster_raft_command_targets(&paths, &state);

    assert_eq!(
        targets.first().map(|endpoint| &endpoint.endpoint_id),
        Some(&private_tcp.endpoint_id)
    );
    let public_position = targets
        .iter()
        .position(|endpoint| endpoint.endpoint_id == public_tcp.endpoint_id)
        .expect("public tcp");
    let iroh_position = targets
        .iter()
        .position(|endpoint| endpoint.endpoint_id == iroh.endpoint_id)
        .expect("iroh");
    assert!(public_position < iroh_position);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn raft_command_targets_preserve_verified_connectivity_before_unknown_tcp() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-raft-route-connectivity-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("paths");
    let now = OffsetDateTime::now_utc();
    let cluster_id =
        ClusterId::from_legacy_named("cluster_network_raft_connectivity").expect("cluster");
    let local = NodeId::from_legacy_named("node_network_raft_connectivity_local").expect("local");
    let remote =
        NodeId::from_legacy_named("node_network_raft_connectivity_remote").expect("remote");
    let mut state = ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "network raft connectivity",
        local.clone(),
        "local",
        "pk-local",
        now,
    );
    let tcp = daemon_lan_endpoint(&remote, "10.0.0.41:21353".to_string(), 10);
    let iroh = ClusterEndpointRecord {
        endpoint_id: stable_iroh_endpoint_id(&remote, "iroh_public_key_network_raft_connectivity"),
        node_id: remote.clone(),
        transport: ClusterTransportKind::Iroh,
        address: "iroh://iroh_public_key_network_raft_connectivity?relay=https://relay.example./"
            .to_string(),
        source: ClusterEndpointSource::Rendezvous,
        privacy: ClusterEndpointPrivacy::Relayed,
        priority: 90,
        last_observed_latency_ms: None,
    };
    let mut member =
        ClusterMemberRecord::local_voter(cluster_id, remote, "remote", "pk-remote", now);
    member.capabilities = main_daemon_capabilities(&member.role);
    member.allowed_capabilities = member.capabilities.clone();
    member.endpoints = vec![tcp.clone(), iroh.clone()];
    state.members.push(member);
    write_cluster_state(&paths, &state).expect("write state");
    sync_cluster_state_to_local_raft_unchecked(&paths, &state).expect("sync state");
    sync_cluster_connectivity_records_to_kv(
        &paths,
        &local,
        vec![
            cluster_connectivity_record(
                &state,
                &tcp,
                ClusterConnectivityState::Unreachable,
                None,
                4,
                None,
                Some("timed out".to_string()),
                now,
                60,
            ),
            cluster_connectivity_record(
                &state,
                &iroh,
                ClusterConnectivityState::Healthy,
                Some(40),
                0,
                Some("ok".to_string()),
                None,
                now,
                60,
            ),
        ],
    )
    .expect("connectivity records");

    let targets = cluster_raft_command_targets(&paths, &state);

    assert_eq!(
        targets.first().map(|endpoint| &endpoint.endpoint_id),
        Some(&iroh.endpoint_id)
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn raft_command_targets_keep_degraded_direct_before_iroh() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-raft-route-degraded-private-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("paths");
    let now = OffsetDateTime::now_utc();
    let cluster_id =
        ClusterId::from_legacy_named("cluster_network_raft_degraded_private").expect("cluster");
    let local =
        NodeId::from_legacy_named("node_network_raft_degraded_private_local").expect("local");
    let remote =
        NodeId::from_legacy_named("node_network_raft_degraded_private_remote").expect("remote");
    let mut state = ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "network raft degraded private",
        local.clone(),
        "local",
        "pk-local",
        now,
    );
    let private_tcp = daemon_lan_endpoint(&remote, "10.0.0.41:21353".to_string(), 10);
    let public_tcp = ClusterEndpointRecord {
        endpoint_id: ClusterEndpointId::from_legacy_named(
            "endpoint_network_raft_degraded_private_public",
        )
        .expect("endpoint"),
        node_id: remote.clone(),
        transport: ClusterTransportKind::LanTcp,
        address: "203.0.113.41:21353".to_string(),
        source: ClusterEndpointSource::Observed,
        privacy: ClusterEndpointPrivacy::InternetDirect,
        priority: 10,
        last_observed_latency_ms: None,
    };
    let iroh = ClusterEndpointRecord {
        endpoint_id: stable_iroh_endpoint_id(
            &remote,
            "iroh_public_key_network_raft_degraded_private",
        ),
        node_id: remote.clone(),
        transport: ClusterTransportKind::Iroh,
        address:
            "iroh://iroh_public_key_network_raft_degraded_private?relay=https://relay.example./"
                .to_string(),
        source: ClusterEndpointSource::Rendezvous,
        privacy: ClusterEndpointPrivacy::Relayed,
        priority: 90,
        last_observed_latency_ms: None,
    };
    let mut member =
        ClusterMemberRecord::local_voter(cluster_id, remote, "remote", "pk-remote", now);
    member.capabilities = main_daemon_capabilities(&member.role);
    member.allowed_capabilities = member.capabilities.clone();
    member.endpoints = vec![private_tcp.clone(), public_tcp.clone(), iroh.clone()];
    state.members.push(member);
    write_cluster_state(&paths, &state).expect("write state");
    sync_cluster_state_to_local_raft_unchecked(&paths, &state).expect("sync state");
    sync_cluster_connectivity_records_to_kv(
        &paths,
        &local,
        vec![
            cluster_connectivity_record(
                &state,
                &private_tcp,
                ClusterConnectivityState::Unreachable,
                None,
                3,
                None,
                Some("timed out".to_string()),
                now,
                60,
            ),
            cluster_connectivity_record(
                &state,
                &public_tcp,
                ClusterConnectivityState::Degraded,
                Some(40),
                1,
                None,
                Some("timed out".to_string()),
                now,
                60,
            ),
            cluster_connectivity_record(
                &state,
                &iroh,
                ClusterConnectivityState::Healthy,
                Some(30),
                0,
                Some("ok".to_string()),
                None,
                now,
                60,
            ),
        ],
    )
    .expect("connectivity records");

    let targets = cluster_raft_command_targets(&paths, &state);

    assert_eq!(
        targets.first().map(|endpoint| &endpoint.endpoint_id),
        Some(&public_tcp.endpoint_id)
    );
    let iroh_position = targets
        .iter()
        .position(|endpoint| endpoint.endpoint_id == iroh.endpoint_id)
        .expect("iroh target");
    let public_position = targets
        .iter()
        .position(|endpoint| endpoint.endpoint_id == public_tcp.endpoint_id)
        .expect("public target");
    assert!(public_position < iroh_position);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn raft_command_targets_prefer_reachable_voter_over_unreachable_voter() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-raft-route-unreachable-voter-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("paths");
    let now = OffsetDateTime::now_utc();
    let cluster_id =
        ClusterId::from_legacy_named("cluster_network_raft_unreachable_voter").expect("cluster");
    let local =
        NodeId::from_legacy_named("node_network_raft_unreachable_voter_local").expect("local");
    let reachable = NodeId::from_legacy_named("node_network_raft_unreachable_voter_reachable")
        .expect("reachable");
    let unreachable = NodeId::from_legacy_named("node_network_raft_unreachable_voter_unreachable")
        .expect("unreachable");
    let mut state = ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "network raft unreachable voter",
        local.clone(),
        "local",
        "pk-local",
        now,
    );
    let reachable_endpoint = daemon_lan_endpoint(&reachable, "198.18.0.76:21353".to_string(), 10);
    let unreachable_endpoint = daemon_lan_endpoint(&unreachable, "10.0.0.15:21353".to_string(), 10);
    for (node_id, endpoint) in [
        (reachable.clone(), reachable_endpoint.clone()),
        (unreachable.clone(), unreachable_endpoint.clone()),
    ] {
        let mut member =
            ClusterMemberRecord::local_voter(cluster_id.clone(), node_id, "remote", "pk", now);
        member.capabilities = main_daemon_capabilities(&member.role);
        member.allowed_capabilities = member.capabilities.clone();
        member.endpoints = vec![endpoint];
        state.members.push(member);
    }
    write_cluster_state(&paths, &state).expect("write state");
    sync_cluster_state_to_local_raft_unchecked(&paths, &state).expect("sync state");
    // Both voters are equally Online — node-status observed_at is no longer a
    // routing/liveness signal — so the preference comes from the active
    // connectivity probes: the reachable peer must outrank the one the probes can
    // no longer reach.
    for node_id in [reachable.clone(), unreachable.clone()] {
        sync_cluster_node_status_to_kv(
            &paths,
            ClusterNodeStatusRecord {
                cluster_id: state.cluster_id.clone(),
                node_id,
                process: "daemon-main".to_string(),
                availability: ClusterNodeAvailability::Online,
                daemon_version: "0.1.0-test".to_string(),
                commit: None,
                target_os: Some("test-os".to_string()),
                target_arch: Some("test-arch".to_string()),
                pid: Some(42),
                address: Some("127.0.0.1:21353".to_string()),
                bind_address: Some("0.0.0.0:21353".to_string()),
                executable: Some("sinomo".to_string()),
                rollout: None,
                daemon_started_at: None,
                observed_at: now,
            },
        )
        .expect("sync status");
    }
    sync_cluster_connectivity_records_to_kv(
        &paths,
        &state.local_node_id,
        vec![
            cluster_connectivity_record(
                &state,
                &reachable_endpoint,
                ClusterConnectivityState::Healthy,
                Some(9),
                0,
                Some("ok".to_string()),
                None,
                now,
                300,
            ),
            cluster_connectivity_record(
                &state,
                &unreachable_endpoint,
                ClusterConnectivityState::Unreachable,
                None,
                3,
                None,
                Some("failed".to_string()),
                now,
                300,
            ),
        ],
    )
    .expect("sync connectivity");

    let targets = cluster_raft_command_targets(&paths, &state);

    assert_eq!(
        targets.first().map(|endpoint| &endpoint.node_id),
        Some(&reachable)
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn main_daemon_command_targets_select_one_route_per_remote_main() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-main-daemon-targets-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("paths");
    let now = OffsetDateTime::now_utc();
    let cluster_id =
        ClusterId::from_legacy_named("cluster_network_main_daemon_targets").expect("cluster");
    let local = NodeId::from_legacy_named("node_network_main_daemon_targets_local").expect("local");
    let remote =
        NodeId::from_legacy_named("node_network_main_daemon_targets_remote").expect("remote");
    let mut state = ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "network main daemon targets",
        local,
        "local",
        "pk-local",
        now,
    );
    let remote_tcp = daemon_lan_endpoint(&remote, "10.0.0.41:21353".to_string(), 10);
    let remote_iroh = ClusterEndpointRecord {
        endpoint_id: stable_iroh_endpoint_id(&remote, "iroh_public_key_network_main_targets"),
        node_id: remote.clone(),
        transport: ClusterTransportKind::Iroh,
        address: "iroh://iroh_public_key_network_main_targets?relay=https://relay.example./"
            .to_string(),
        source: ClusterEndpointSource::Rendezvous,
        privacy: ClusterEndpointPrivacy::Relayed,
        priority: 90,
        last_observed_latency_ms: None,
    };
    let mut member =
        ClusterMemberRecord::local_voter(cluster_id, remote, "remote", "pk-remote", now);
    member.capabilities = main_daemon_capabilities(&member.role);
    member.allowed_capabilities = member.capabilities.clone();
    member.endpoints = vec![remote_tcp.clone(), remote_iroh];
    state.members.push(member);
    write_cluster_state(&paths, &state).expect("write state");
    sync_cluster_state_to_local_raft_unchecked(&paths, &state).expect("sync state");
    sync_cluster_connectivity_records_to_kv(
        &paths,
        &state.local_node_id,
        vec![cluster_connectivity_record(
            &state,
            &remote_tcp,
            ClusterConnectivityState::Healthy,
            Some(1),
            0,
            Some("ok".to_string()),
            None,
            now,
            60,
        )],
    )
    .expect("connectivity records");

    let targets = cluster_main_daemon_command_targets(&paths, &state);

    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0].endpoint_id, remote_tcp.endpoint_id);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn main_daemon_endpoint_candidates_keep_static_fallback_order() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-main-daemon-candidate-order-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("paths");
    let now = OffsetDateTime::now_utc();
    let cluster_id =
        ClusterId::from_legacy_named("cluster_network_main_daemon_order").expect("cluster");
    let local = NodeId::from_legacy_named("node_network_main_order_local").expect("local");
    let remote = NodeId::from_legacy_named("node_network_main_order_remote").expect("remote");
    let mut state = ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "network main daemon order",
        local,
        "local",
        "pk-local",
        now,
    );
    let lan = daemon_lan_endpoint(&remote, "10.0.0.41:21353".to_string(), 10);
    let public = ClusterEndpointRecord {
        endpoint_id: ClusterEndpointId::from_legacy_named("endpoint_main_daemon_public_order")
            .expect("endpoint"),
        node_id: remote.clone(),
        transport: ClusterTransportKind::LanTcp,
        address: "203.0.113.41:21353".to_string(),
        source: ClusterEndpointSource::Observed,
        privacy: ClusterEndpointPrivacy::InternetDirect,
        priority: 10,
        last_observed_latency_ms: None,
    };
    let iroh = ClusterEndpointRecord {
        endpoint_id: stable_iroh_endpoint_id(&remote, "iroh_public_key_main_order"),
        node_id: remote.clone(),
        transport: ClusterTransportKind::Iroh,
        address: "iroh://iroh_public_key_main_order?relay=https://relay.example./".to_string(),
        source: ClusterEndpointSource::Rendezvous,
        privacy: ClusterEndpointPrivacy::Relayed,
        priority: 90,
        last_observed_latency_ms: None,
    };
    let mut member =
        ClusterMemberRecord::local_voter(cluster_id, remote, "remote", "pk-remote", now);
    member.capabilities = main_daemon_capabilities(&member.role);
    member.allowed_capabilities = member.capabilities.clone();
    member.endpoints = vec![iroh.clone(), public.clone(), lan.clone()];
    state.members.push(member);
    write_cluster_state(&paths, &state).expect("write state");
    sync_cluster_state_to_local_raft_unchecked(&paths, &state).expect("sync state");

    let candidates = cluster_main_daemon_command_endpoint_candidates(&paths, &state);
    let ordered_endpoint_ids = candidates
        .iter()
        .map(|endpoint| endpoint.endpoint_id.clone())
        .collect::<Vec<_>>();

    assert_eq!(
        ordered_endpoint_ids,
        vec![lan.endpoint_id, public.endpoint_id, iroh.endpoint_id]
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn rollout_target_selection_prefers_main_route_before_shadow_backup() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-rollout-shadow-route-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("paths");
    let now = OffsetDateTime::now_utc();
    let cluster_id =
        ClusterId::from_legacy_named("cluster_network_rollout_shadow").expect("cluster");
    let local = NodeId::from_legacy_named("node_network_rollout_shadow_local").expect("local");
    let main = NodeId::from_legacy_named("node_network_rollout_shadow_main").expect("main");
    let shadow = shadow_daemon_node_id(&main);
    let state = ClusterBootstrapState::initialized_locally(
        cluster_id,
        "network rollout shadow",
        local,
        "local",
        "pk-local",
        now,
    );
    let main_endpoint = daemon_lan_endpoint(&main, "10.0.0.41:21353".to_string(), 10);
    let shadow_endpoint = daemon_lan_endpoint(&shadow, "10.0.0.42:21354".to_string(), 10);

    let selected = cluster_rollout_target_endpoint_for_main(
        &paths,
        &state,
        &main,
        &[main_endpoint.clone(), shadow_endpoint.clone()],
    )
    .expect("rollout endpoint");

    assert_eq!(selected.endpoint_id, main_endpoint.endpoint_id);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn rollout_target_selection_applies_connectivity_to_equivalent_tcp_route() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-rollout-equivalent-route-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("paths");
    let now = OffsetDateTime::now_utc();
    let cluster_id =
        ClusterId::from_legacy_named("cluster_network_rollout_equivalent").expect("cluster");
    let local = NodeId::from_legacy_named("node_network_rollout_equivalent_local").expect("local");
    let main = NodeId::from_legacy_named("node_network_rollout_equivalent_main").expect("main");
    let state = ClusterBootstrapState::initialized_locally(
        cluster_id,
        "network rollout equivalent",
        local.clone(),
        "local",
        "pk-local",
        now,
    );
    write_cluster_state(&paths, &state).expect("write state");
    sync_cluster_state_to_local_raft_unchecked(&paths, &state).expect("sync state");
    let private_recorded = daemon_lan_endpoint(&main, "10.0.0.41:21353".to_string(), 10);
    let mut private_readvertised = private_recorded.clone();
    private_readvertised.endpoint_id =
        ClusterEndpointId::from_legacy_named("endpoint_rollout_equivalent_readvertised")
            .expect("endpoint");
    let public = daemon_lan_endpoint(&main, "203.0.113.41:21353".to_string(), 20);
    sync_cluster_connectivity_records_to_kv(
        &paths,
        &local,
        vec![cluster_connectivity_record(
            &state,
            &private_recorded,
            ClusterConnectivityState::Unreachable,
            None,
            3,
            None,
            Some("failed".to_string()),
            now,
            60,
        )],
    )
    .expect("connectivity record");

    let selected = cluster_rollout_target_endpoint_for_main(
        &paths,
        &state,
        &main,
        &[private_readvertised.clone(), public.clone()],
    )
    .expect("rollout endpoint");

    assert_eq!(selected.endpoint_id, public.endpoint_id);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn rollout_target_selection_keeps_transient_public_tcp_before_iroh() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-rollout-transient-public-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("paths");
    let now = OffsetDateTime::now_utc();
    let cluster_id =
        ClusterId::from_legacy_named("cluster_network_rollout_transient").expect("cluster");
    let local = NodeId::from_legacy_named("node_network_rollout_transient_local").expect("local");
    let main = NodeId::from_legacy_named("node_network_rollout_transient_main").expect("main");
    let state = ClusterBootstrapState::initialized_locally(
        cluster_id,
        "network rollout transient",
        local.clone(),
        "local",
        "pk-local",
        now,
    );
    write_cluster_state(&paths, &state).expect("write state");
    sync_cluster_state_to_local_raft_unchecked(&paths, &state).expect("sync state");
    let public = daemon_lan_endpoint(&main, "203.0.113.41:21353".to_string(), 10);
    let iroh = ClusterEndpointRecord {
        endpoint_id: stable_iroh_endpoint_id(&main, "iroh_public_key_rollout_transient"),
        node_id: main.clone(),
        transport: ClusterTransportKind::Iroh,
        address: "iroh://iroh_public_key_rollout_transient?relay=https://relay.example./"
            .to_string(),
        source: ClusterEndpointSource::Rendezvous,
        privacy: ClusterEndpointPrivacy::Relayed,
        priority: 90,
        last_observed_latency_ms: None,
    };
    sync_cluster_connectivity_records_to_kv(
        &paths,
        &local,
        vec![cluster_connectivity_record(
            &state,
            &public,
            ClusterConnectivityState::Unreachable,
            None,
            1,
            None,
            Some("transient".to_string()),
            now,
            60,
        )],
    )
    .expect("connectivity record");

    let selected =
        cluster_rollout_target_endpoint_for_main(&paths, &state, &main, &[iroh, public.clone()])
            .expect("rollout endpoint");

    assert_eq!(selected.endpoint_id, public.endpoint_id);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn rollout_target_selection_uses_command_route_context_for_shadow_fallbacks() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-rollout-shadow-context-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("paths");
    let now = OffsetDateTime::now_utc();
    let cluster_id =
        ClusterId::from_legacy_named("cluster_network_rollout_shadow_context").expect("cluster");
    let local =
        NodeId::from_legacy_named("node_network_rollout_shadow_context_local").expect("local");
    let main = NodeId::from_legacy_named("node_network_rollout_shadow_context_main").expect("main");
    let shadow = shadow_daemon_node_id(&main);
    let state = ClusterBootstrapState::initialized_locally(
        cluster_id,
        "network rollout shadow context",
        local,
        "local",
        "pk-local",
        now,
    );
    let private_main = daemon_lan_endpoint(&main, "10.0.0.41:21353".to_string(), 10);
    let public_main = daemon_lan_endpoint(&main, "203.0.113.41:21353".to_string(), 10);
    let private_shadow = daemon_lan_endpoint(&shadow, "10.0.0.42:21354".to_string(), 10);
    let shadow_iroh = ClusterEndpointRecord {
        endpoint_id: stable_iroh_endpoint_id(&shadow, "iroh_public_key_rollout_shadow_context"),
        node_id: shadow.clone(),
        transport: ClusterTransportKind::Iroh,
        address: "iroh://iroh_public_key_rollout_shadow_context?relay=https://relay.example./"
            .to_string(),
        source: ClusterEndpointSource::Rendezvous,
        privacy: ClusterEndpointPrivacy::Relayed,
        priority: 90,
        last_observed_latency_ms: None,
    };

    let selected = cluster_rollout_target_endpoint_for_main_with_command_endpoint(
        &paths,
        &state,
        &main,
        Some(&public_main),
        &[
            private_shadow.clone(),
            shadow_iroh.clone(),
            public_main.clone(),
        ],
    )
    .expect("public-context rollout endpoint");
    assert_eq!(selected.endpoint_id, public_main.endpoint_id);
    let ordered = cluster_rollout_target_endpoints_for_main_with_command_endpoint(
        &paths,
        &state,
        &main,
        Some(&public_main),
        &[
            private_shadow.clone(),
            shadow_iroh.clone(),
            public_main.clone(),
        ],
    );
    assert_eq!(ordered[0].endpoint_id, public_main.endpoint_id);
    assert_eq!(ordered[1].endpoint_id, private_shadow.endpoint_id);
    assert_eq!(ordered[2].endpoint_id, shadow_iroh.endpoint_id);
    let ordered = cluster_rollout_target_endpoints_for_main_with_command_endpoint(
        &paths,
        &state,
        &main,
        Some(&public_main),
        &[private_shadow.clone(), shadow_iroh.clone()],
    );
    assert_eq!(ordered[0].endpoint_id, public_main.endpoint_id);

    let selected = cluster_rollout_target_endpoint_for_main_with_command_endpoint(
        &paths,
        &state,
        &main,
        Some(&private_main),
        &[private_shadow.clone(), shadow_iroh, private_main.clone()],
    )
    .expect("private-context rollout endpoint");
    assert_eq!(selected.endpoint_id, private_main.endpoint_id);

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn cluster_network_probe_targets_include_remote_shadow_endpoints() {
    let now = OffsetDateTime::now_utc();
    let cluster_id = ClusterId::from_legacy_named("cluster_network_probe_shadow").expect("cluster");
    let local = NodeId::from_legacy_named("node_network_probe_shadow_local").expect("local");
    let main = NodeId::from_legacy_named("node_network_probe_shadow_main").expect("main");
    let shadow = shadow_daemon_node_id(&main);
    let mut state = ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "network probe shadow",
        local,
        "local",
        "pk-local",
        now,
    );

    let main_endpoint = daemon_lan_endpoint(&main, "10.0.0.41:21353".to_string(), 10);
    let shadow_endpoint = daemon_lan_endpoint(&shadow, "10.0.0.41:21354".to_string(), 10);
    let mut main_member =
        ClusterMemberRecord::local_voter(cluster_id.clone(), main, "main", "pk-main", now);
    main_member.capabilities = main_daemon_capabilities(&main_member.role);
    main_member.allowed_capabilities = main_member.capabilities.clone();
    main_member.endpoints = vec![main_endpoint.clone()];

    let mut shadow_member =
        ClusterMemberRecord::local_voter(cluster_id, shadow, "shadow", "pk-shadow", now);
    shadow_member.role = ClusterMemberRole::NonVoting;
    shadow_member.state = ClusterMemberState::NonVoting;
    shadow_member.capabilities = shadow_daemon_capabilities();
    shadow_member.allowed_capabilities = shadow_member.capabilities.clone();
    shadow_member.endpoints = vec![shadow_endpoint.clone()];
    state.members.push(main_member);
    state.members.push(shadow_member);

    let targets = cluster_network_probe_targets(&state);
    let endpoint_ids = targets
        .into_iter()
        .map(|endpoint| endpoint.endpoint_id)
        .collect::<BTreeSet<_>>();

    assert!(endpoint_ids.contains(&main_endpoint.endpoint_id));
    assert!(
        endpoint_ids.contains(&shadow_endpoint.endpoint_id),
        "connectivity graph probing must cover shadow endpoints used by rollout"
    );
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
        &["198.18.0.76".parse().expect("public")],
    );

    assert!(endpoints.iter().any(|endpoint| {
        endpoint.address == "198.18.0.76:21353"
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
        &["198.18.0.76".parse().expect("public")],
        false,
    );

    assert!(
        endpoints
            .iter()
            .any(|endpoint| endpoint.address == "10.0.0.41:21354")
    );
    assert!(!endpoints.iter().any(|endpoint| {
        endpoint.address == "198.18.0.76:21354"
            && endpoint.privacy == ClusterEndpointPrivacy::InternetDirect
    }));
}

static PUBLIC_ENDPOINT_CACHE_TEST_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[test]
fn public_ip_discovery_cache_reuses_recent_result() {
    let _guard = PUBLIC_ENDPOINT_CACHE_TEST_MUTEX
        .lock()
        .expect("cache mutex");
    crate::lan::clear_public_ip_discovery_cache();
    let calls = std::sync::atomic::AtomicU64::new(0);
    let first = crate::lan::cached_default_public_ip_candidates_with(|| {
        calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        vec!["203.0.113.10".parse().expect("ip")]
    });
    let second = crate::lan::cached_default_public_ip_candidates_with(|| {
        calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        vec!["203.0.113.11".parse().expect("ip")]
    });

    assert_eq!(first, vec!["203.0.113.10".parse::<IpAddr>().expect("ip")]);
    assert_eq!(
        second, first,
        "recent public IP discovery should be served from cache"
    );
    assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);
    crate::lan::clear_public_ip_discovery_cache();
}

#[test]
fn public_endpoint_reachability_cache_reuses_recent_result() {
    let _guard = PUBLIC_ENDPOINT_CACHE_TEST_MUTEX
        .lock()
        .expect("cache mutex");
    clear_public_endpoint_reachability_cache();
    let calls = std::sync::atomic::AtomicU64::new(0);
    let ip = "203.0.113.12".parse().expect("ip");
    let first = public_endpoint_is_reachable_with_probe(ip, 21353, |_| {
        calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        true
    });
    let second = public_endpoint_is_reachable_with_probe(ip, 21353, |_| {
        calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        false
    });

    assert!(first);
    assert!(
        second,
        "recent public endpoint reachability should be served from cache"
    );
    assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);
    clear_public_endpoint_reachability_cache();
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
fn raft_heartbeat_does_not_rewrite_materialized_snapshot() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-raft-heartbeat-snapshot-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");
    let node_id = NodeId::from_legacy_named("node_heartbeat_snapshot").expect("node");

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
    let heartbeat = {
        let node = open_cluster_raft_kv_node(&paths, &node_id).expect("open raft");
        RaftAppendEntriesRequest {
            term: node.hard_state().current_term,
            leader_id: node_id.clone(),
            prev_log_index: node.last_log_index(),
            prev_log_term: node.last_log_term(),
            entries: Vec::new(),
            leader_commit: node.hard_state().commit_index,
        }
    };
    std::fs::write(&paths.kv_snapshot, b"sentinel snapshot").expect("sentinel snapshot");

    let response =
        apply_cluster_raft_append(&paths, &node_id, None, heartbeat).expect("apply heartbeat");

    assert!(response.success);
    assert_eq!(
        std::fs::read(&paths.kv_snapshot).expect("read snapshot"),
        b"sentinel snapshot"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn cluster_kv_store_reads_materialized_snapshot_without_opening_raft() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-kv-store-cache-only-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");

    let mut snapshot_store = InProcessKvStore::new();
    snapshot_store
        .put("/snapshot-only", b"snapshot".to_vec(), None)
        .expect("seed snapshot store");
    write_cluster_kv_snapshot(&paths, &snapshot_store.snapshot()).expect("write snapshot");

    let store = read_cluster_kv_store(&paths).expect("read cluster kv store");
    assert!(store.get("/snapshot-only").is_some());
    assert!(!paths.kv_raft.exists());
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn cluster_kv_store_prefers_local_raft_over_stale_materialized_snapshot() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-kv-store-raft-over-cache-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");
    let now = OffsetDateTime::now_utc();
    let cluster_id = ClusterId::from_legacy_named("cluster_raft_over_cache").expect("cluster");
    let node_id = NodeId::from_legacy_named("node_raft_over_cache").expect("node");
    let state = ClusterBootstrapState::initialized_locally(
        cluster_id,
        "raft over cache",
        node_id.clone(),
        "node",
        "public-key",
        now,
    );
    write_cluster_state(&paths, &state).expect("write state");

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

    let mut stale_store = InProcessKvStore::new();
    stale_store
        .put("/stale-cache-only", b"cache".to_vec(), None)
        .expect("seed stale cache");
    write_cluster_kv_snapshot(&paths, &stale_store.snapshot()).expect("write stale snapshot");

    let store = read_cluster_kv_store(&paths).expect("read cluster kv store");
    assert!(store.get("/raft-only").is_some());
    assert!(store.get("/stale-cache-only").is_none());
    let refreshed = read_cluster_kv_snapshot(&paths).expect("read refreshed cache");
    assert_eq!(refreshed.revision, store.revision());
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

    // Commit well beyond the retained-tail bound (256) so compaction keeps only a
    // recent tail and earlier entries are reachable solely via a snapshot.
    for index in 0..400 {
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
    // The log is no longer truncated to empty: a bounded committed tail is kept for
    // incremental replication.
    let log = node.log();
    assert!(!log.is_empty(), "compaction should retain a log tail");
    assert!(log.len() <= 256, "retained tail must be bounded");
    let snapshot_index = node.snapshot_index();
    let earliest_retained = log.first().map(|entry| entry.index).expect("tail");
    let last_log_index = node.last_log_index();
    drop(node);

    // A caught-up follower (prev == snapshot index) gets a heartbeat, no snapshot.
    let heartbeat = build_cluster_raft_append_request_from_next_index(
        &paths,
        &state,
        last_log_index.saturating_add(1),
    )
    .expect("build heartbeat");
    assert!(heartbeat.snapshot.is_none());
    assert!(heartbeat.append.entries.is_empty());
    assert_eq!(heartbeat.append.prev_log_index, snapshot_index);

    // A follower a few entries behind, but still within the retained tail, is caught
    // up with incremental entries — no snapshot install.
    let near =
        build_cluster_raft_append_request_from_next_index(&paths, &state, earliest_retained + 1)
            .expect("build near catchup");
    assert!(
        near.snapshot.is_none(),
        "follower within the retained tail must be served incrementally"
    );
    assert!(!near.append.entries.is_empty());

    // A follower behind the retained tail still requires a full snapshot install.
    let far = build_cluster_raft_append_request_from_next_index(
        &paths,
        &state,
        earliest_retained.saturating_sub(1).max(1),
    )
    .expect("build far catchup");
    assert!(
        far.snapshot.is_some(),
        "follower behind the retained tail must receive a snapshot"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn connectivity_sort_prefers_unknown_private_tcp_over_unknown_iroh() {
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
        Some(&private.endpoint_id)
    );
    assert!(
        targets
            .iter()
            .any(|endpoint| endpoint.endpoint_id == iroh.endpoint_id)
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn connectivity_sort_prefers_unknown_public_tcp_over_unknown_iroh() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-unknown-public-tcp-sort-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("paths");
    let now = OffsetDateTime::now_utc();
    let cluster_id =
        ClusterId::from_legacy_named("cluster_unknown_public_tcp_sort").expect("cluster");
    let node_a = NodeId::from_legacy_named("node_unknown_public_tcp_sort_a").expect("node a");
    let node_b = NodeId::from_legacy_named("node_unknown_public_tcp_sort_b").expect("node b");
    let mut state = ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "unknown public tcp sort",
        node_a,
        "node a",
        "pk-a",
        now,
    );
    let public = ClusterEndpointRecord {
        endpoint_id: ClusterEndpointId::from_legacy_named("endpoint_unknown_public_tcp_sort")
            .expect("endpoint"),
        node_id: node_b.clone(),
        transport: ClusterTransportKind::LanTcp,
        address: "203.0.113.41:21353".to_string(),
        source: ClusterEndpointSource::Observed,
        privacy: ClusterEndpointPrivacy::InternetDirect,
        priority: 10,
        last_observed_latency_ms: None,
    };
    let iroh = ClusterEndpointRecord {
        endpoint_id: stable_iroh_endpoint_id(&node_b, "iroh_public_key_unknown_public_sort"),
        node_id: node_b.clone(),
        transport: ClusterTransportKind::Iroh,
        address: "iroh://iroh_public_key_unknown_public_sort?relay=https://relay.example./"
            .to_string(),
        source: ClusterEndpointSource::Rendezvous,
        privacy: ClusterEndpointPrivacy::Relayed,
        priority: 90,
        last_observed_latency_ms: None,
    };
    let mut member_b = ClusterMemberRecord::local_voter(cluster_id, node_b, "node b", "pk-b", now);
    member_b.capabilities = main_daemon_capabilities(&member_b.role);
    member_b.allowed_capabilities = member_b.capabilities.clone();
    member_b.endpoints = vec![iroh.clone(), public.clone()];
    state.members.push(member_b);

    let targets = cluster_sync_targets_with_connectivity(&paths, &state);

    assert_eq!(
        targets.first().map(|endpoint| &endpoint.endpoint_id),
        Some(&public.endpoint_id)
    );
    assert!(
        targets
            .iter()
            .any(|endpoint| endpoint.endpoint_id == iroh.endpoint_id)
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn cluster_sync_targets_keep_lan_public_iroh_order_before_connectivity() {
    let now = OffsetDateTime::now_utc();
    let cluster_id = ClusterId::from_legacy_named("cluster_static_route_order").expect("cluster");
    let node_a = NodeId::from_legacy_named("node_static_route_order_a").expect("node a");
    let node_b = NodeId::from_legacy_named("node_static_route_order_b").expect("node b");
    let mut state = ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "static route order",
        node_a,
        "node a",
        "pk-a",
        now,
    );
    let lan = daemon_lan_endpoint(&node_b, "10.0.0.41:21353".to_string(), 80);
    let public = ClusterEndpointRecord {
        endpoint_id: ClusterEndpointId::from_legacy_named("endpoint_static_route_order_public")
            .expect("endpoint"),
        node_id: node_b.clone(),
        transport: ClusterTransportKind::LanTcp,
        address: "203.0.113.41:21353".to_string(),
        source: ClusterEndpointSource::Observed,
        privacy: ClusterEndpointPrivacy::InternetDirect,
        priority: 90,
        last_observed_latency_ms: None,
    };
    let iroh = ClusterEndpointRecord {
        endpoint_id: stable_iroh_endpoint_id(&node_b, "iroh_public_key_static_route_order"),
        node_id: node_b.clone(),
        transport: ClusterTransportKind::Iroh,
        address: "iroh://iroh_public_key_static_route_order?relay=https://relay.example./"
            .to_string(),
        source: ClusterEndpointSource::Rendezvous,
        privacy: ClusterEndpointPrivacy::Relayed,
        priority: 0,
        last_observed_latency_ms: None,
    };
    let mut member_b = ClusterMemberRecord::local_voter(cluster_id, node_b, "node b", "pk-b", now);
    member_b.capabilities = main_daemon_capabilities(&member_b.role);
    member_b.allowed_capabilities = member_b.capabilities.clone();
    member_b.endpoints = vec![iroh.clone(), public.clone(), lan.clone()];
    state.members.push(member_b);

    let targets = cluster_sync_targets(&state);
    let ordered_endpoint_ids = targets
        .iter()
        .map(|endpoint| endpoint.endpoint_id.clone())
        .collect::<Vec<_>>();

    assert_eq!(
        &ordered_endpoint_ids[..3],
        &[
            lan.endpoint_id.clone(),
            public.endpoint_id.clone(),
            iroh.endpoint_id.clone(),
        ]
    );
}

#[test]
fn connectivity_sort_uses_lan_public_iroh_fallback_order() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-lan-public-iroh-sort-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("paths");
    let now = OffsetDateTime::now_utc();
    let cluster_id = ClusterId::from_legacy_named("cluster_lan_public_iroh_sort").expect("cluster");
    let node_a = NodeId::from_legacy_named("node_lan_public_iroh_sort_a").expect("node a");
    let node_b = NodeId::from_legacy_named("node_lan_public_iroh_sort_b").expect("node b");
    let mut state = ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "lan public iroh sort",
        node_a.clone(),
        "node a",
        "pk-a",
        now,
    );
    let lan = daemon_lan_endpoint(&node_b, "10.0.0.41:21353".to_string(), 10);
    let public = ClusterEndpointRecord {
        endpoint_id: ClusterEndpointId::from_legacy_named("endpoint_lan_public_iroh_public")
            .expect("endpoint"),
        node_id: node_b.clone(),
        transport: ClusterTransportKind::LanTcp,
        address: "203.0.113.41:21353".to_string(),
        source: ClusterEndpointSource::Observed,
        privacy: ClusterEndpointPrivacy::InternetDirect,
        priority: 10,
        last_observed_latency_ms: None,
    };
    let iroh = ClusterEndpointRecord {
        endpoint_id: stable_iroh_endpoint_id(&node_b, "iroh_public_key_lan_public_sort"),
        node_id: node_b.clone(),
        transport: ClusterTransportKind::Iroh,
        address: "iroh://iroh_public_key_lan_public_sort?relay=https://relay.example./".to_string(),
        source: ClusterEndpointSource::Rendezvous,
        privacy: ClusterEndpointPrivacy::Relayed,
        priority: 90,
        last_observed_latency_ms: None,
    };
    let mut member_b = ClusterMemberRecord::local_voter(cluster_id, node_b, "node b", "pk-b", now);
    member_b.capabilities = main_daemon_capabilities(&member_b.role);
    member_b.allowed_capabilities = member_b.capabilities.clone();
    member_b.endpoints = vec![iroh.clone(), public.clone(), lan.clone()];
    state.members.push(member_b);
    write_cluster_state(&paths, &state).expect("write state");
    sync_cluster_state_to_local_raft_unchecked(&paths, &state).expect("sync state");
    sync_cluster_connectivity_records_to_kv(
        &paths,
        &node_a,
        vec![
            cluster_connectivity_record(
                &state,
                &lan,
                ClusterConnectivityState::Degraded,
                Some(80),
                1,
                Some("slow".to_string()),
                None,
                now,
                60,
            ),
            cluster_connectivity_record(
                &state,
                &public,
                ClusterConnectivityState::Healthy,
                Some(20),
                0,
                Some("ok".to_string()),
                None,
                now,
                60,
            ),
            cluster_connectivity_record(
                &state,
                &iroh,
                ClusterConnectivityState::Healthy,
                Some(10),
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
    let ordered_endpoint_ids = targets
        .iter()
        .map(|endpoint| endpoint.endpoint_id.clone())
        .collect::<Vec<_>>();

    assert_eq!(
        &ordered_endpoint_ids[..3],
        &[
            lan.endpoint_id.clone(),
            public.endpoint_id.clone(),
            iroh.endpoint_id.clone(),
        ]
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn member_daemon_status_targets_use_connectivity_without_promoting_iroh() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-status-route-connectivity-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("paths");
    let now = OffsetDateTime::now_utc();
    let cluster_id =
        ClusterId::from_legacy_named("cluster_status_route_connectivity").expect("cluster");
    let local = NodeId::from_legacy_named("node_status_route_local").expect("local");
    let remote = NodeId::from_legacy_named("node_status_route_remote").expect("remote");
    let mut state = ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "status route connectivity",
        local.clone(),
        "local",
        "pk-local",
        now,
    );
    let lan = daemon_lan_endpoint(&remote, "10.0.0.41:21353".to_string(), 10);
    let public = ClusterEndpointRecord {
        endpoint_id: ClusterEndpointId::from_legacy_named("endpoint_status_route_public")
            .expect("endpoint"),
        node_id: remote.clone(),
        transport: ClusterTransportKind::LanTcp,
        address: "203.0.113.41:21353".to_string(),
        source: ClusterEndpointSource::Observed,
        privacy: ClusterEndpointPrivacy::InternetDirect,
        priority: 10,
        last_observed_latency_ms: None,
    };
    let iroh = ClusterEndpointRecord {
        endpoint_id: stable_iroh_endpoint_id(&remote, "iroh_public_key_status_route"),
        node_id: remote.clone(),
        transport: ClusterTransportKind::Iroh,
        address: "iroh://iroh_public_key_status_route?relay=https://relay.example./".to_string(),
        source: ClusterEndpointSource::Rendezvous,
        privacy: ClusterEndpointPrivacy::Relayed,
        priority: 90,
        last_observed_latency_ms: None,
    };
    let mut member =
        ClusterMemberRecord::local_voter(cluster_id, remote, "remote", "pk-remote", now);
    member.capabilities = main_daemon_capabilities(&member.role);
    member.allowed_capabilities = member.capabilities.clone();
    member.endpoints = vec![iroh.clone(), public.clone(), lan.clone()];
    state.members.push(member.clone());
    write_cluster_state(&paths, &state).expect("write state");
    sync_cluster_state_to_local_raft_unchecked(&paths, &state).expect("sync state");

    sync_cluster_connectivity_records_to_kv(
        &paths,
        &local,
        vec![
            cluster_connectivity_record(
                &state,
                &lan,
                ClusterConnectivityState::Degraded,
                Some(80),
                1,
                Some("slow".to_string()),
                None,
                now,
                60,
            ),
            cluster_connectivity_record(
                &state,
                &public,
                ClusterConnectivityState::Healthy,
                Some(20),
                0,
                Some("ok".to_string()),
                None,
                now,
                60,
            ),
            cluster_connectivity_record(
                &state,
                &iroh,
                ClusterConnectivityState::Healthy,
                Some(10),
                0,
                Some("ok".to_string()),
                None,
                now,
                60,
            ),
        ],
    )
    .expect("connectivity records");
    let targets = cluster_member_daemon_status_targets_with_connectivity(&paths, &state, &member);
    assert_eq!(
        targets
            .iter()
            .take(3)
            .map(|endpoint| endpoint.endpoint_id.clone())
            .collect::<Vec<_>>(),
        vec![
            lan.endpoint_id.clone(),
            public.endpoint_id.clone(),
            iroh.endpoint_id.clone(),
        ]
    );

    sync_cluster_connectivity_records_to_kv(
        &paths,
        &local,
        vec![
            cluster_connectivity_record(
                &state,
                &lan,
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
                &public,
                ClusterConnectivityState::Healthy,
                Some(20),
                0,
                Some("ok".to_string()),
                None,
                now,
                60,
            ),
            cluster_connectivity_record(
                &state,
                &iroh,
                ClusterConnectivityState::Healthy,
                Some(10),
                0,
                Some("ok".to_string()),
                None,
                now,
                60,
            ),
        ],
    )
    .expect("connectivity records");
    let targets = cluster_member_daemon_status_targets_with_connectivity(&paths, &state, &member);
    assert_eq!(targets[0].endpoint_id, public.endpoint_id);
    assert_eq!(targets[1].endpoint_id, iroh.endpoint_id);

    sync_cluster_connectivity_records_to_kv(
        &paths,
        &local,
        vec![
            cluster_connectivity_record(
                &state,
                &lan,
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
                &public,
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
                Some(10),
                0,
                Some("ok".to_string()),
                None,
                now,
                60,
            ),
        ],
    )
    .expect("connectivity records");
    let targets = cluster_member_daemon_status_targets_with_connectivity(&paths, &state, &member);
    assert_eq!(targets[0].endpoint_id, iroh.endpoint_id);

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn rollout_target_selection_uses_lan_public_iroh_fallback_order() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-rollout-lan-public-iroh-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("paths");
    let now = OffsetDateTime::now_utc();
    let cluster_id =
        ClusterId::from_legacy_named("cluster_rollout_lan_public_iroh").expect("cluster");
    let local = NodeId::from_legacy_named("node_rollout_lan_public_iroh_local").expect("local");
    let main = NodeId::from_legacy_named("node_rollout_lan_public_iroh_main").expect("main");
    let state = ClusterBootstrapState::initialized_locally(
        cluster_id,
        "rollout lan public iroh",
        local,
        "local",
        "pk-local",
        now,
    );
    write_cluster_state(&paths, &state).expect("write state");
    sync_cluster_state_to_local_raft_unchecked(&paths, &state).expect("sync state");
    let lan = daemon_lan_endpoint(&main, "10.0.0.41:21353".to_string(), 10);
    let public = ClusterEndpointRecord {
        endpoint_id: ClusterEndpointId::from_legacy_named("endpoint_rollout_public")
            .expect("endpoint"),
        node_id: main.clone(),
        transport: ClusterTransportKind::LanTcp,
        address: "203.0.113.41:21353".to_string(),
        source: ClusterEndpointSource::Observed,
        privacy: ClusterEndpointPrivacy::InternetDirect,
        priority: 10,
        last_observed_latency_ms: None,
    };
    let iroh = ClusterEndpointRecord {
        endpoint_id: stable_iroh_endpoint_id(&main, "iroh_public_key_rollout_sort"),
        node_id: main.clone(),
        transport: ClusterTransportKind::Iroh,
        address: "iroh://iroh_public_key_rollout_sort?relay=https://relay.example./".to_string(),
        source: ClusterEndpointSource::Rendezvous,
        privacy: ClusterEndpointPrivacy::Relayed,
        priority: 90,
        last_observed_latency_ms: None,
    };
    let candidates = vec![iroh.clone(), public.clone(), lan.clone()];

    let selected = cluster_rollout_target_endpoint_for_main(&paths, &state, &main, &candidates)
        .expect("rollout endpoint");
    assert_eq!(selected.endpoint_id, lan.endpoint_id);

    // A route must fail CLUSTER_CONNECTIVITY_UNAVAILABLE_AFTER_FAILURES (3)
    // consecutive probes before it is demoted below a healthier fallback; a single
    // failure keeps the preferred LAN route to avoid flapping.
    sync_cluster_connectivity_records_to_kv(
        &paths,
        &state.local_node_id,
        vec![cluster_connectivity_record(
            &state,
            &lan,
            ClusterConnectivityState::Unreachable,
            None,
            3,
            None,
            Some("failed".to_string()),
            now,
            60,
        )],
    )
    .expect("connectivity records");
    let selected = cluster_rollout_target_endpoint_for_main(&paths, &state, &main, &candidates)
        .expect("rollout endpoint");
    assert_eq!(selected.endpoint_id, public.endpoint_id);

    sync_cluster_connectivity_records_to_kv(
        &paths,
        &state.local_node_id,
        vec![
            cluster_connectivity_record(
                &state,
                &lan,
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
                &public,
                ClusterConnectivityState::Unreachable,
                None,
                3,
                None,
                Some("failed".to_string()),
                now,
                60,
            ),
        ],
    )
    .expect("connectivity records");
    let selected = cluster_rollout_target_endpoint_for_main(&paths, &state, &main, &candidates)
        .expect("rollout endpoint");
    assert_eq!(selected.endpoint_id, iroh.endpoint_id);

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn rollout_target_selection_treats_hostname_tcp_as_direct_before_iroh() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-rollout-hostname-direct-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("paths");
    let now = OffsetDateTime::now_utc();
    let cluster_id =
        ClusterId::from_legacy_named("cluster_rollout_hostname_direct").expect("cluster");
    let local = NodeId::from_legacy_named("node_rollout_hostname_direct_local").expect("local");
    let main = NodeId::from_legacy_named("node_rollout_hostname_direct_main").expect("main");
    let state = ClusterBootstrapState::initialized_locally(
        cluster_id,
        "rollout hostname direct",
        local,
        "local",
        "pk-local",
        now,
    );
    write_cluster_state(&paths, &state).expect("write state");
    sync_cluster_state_to_local_raft_unchecked(&paths, &state).expect("sync state");
    let hostname = daemon_lan_endpoint(&main, "node.example.test:21353".to_string(), 10);
    let iroh = ClusterEndpointRecord {
        endpoint_id: stable_iroh_endpoint_id(&main, "iroh_public_key_rollout_hostname"),
        node_id: main.clone(),
        transport: ClusterTransportKind::Iroh,
        address: "iroh://iroh_public_key_rollout_hostname?relay=https://relay.example./"
            .to_string(),
        source: ClusterEndpointSource::Rendezvous,
        privacy: ClusterEndpointPrivacy::Relayed,
        priority: 90,
        last_observed_latency_ms: None,
    };

    let selected =
        cluster_rollout_target_endpoint_for_main(&paths, &state, &main, &[iroh, hostname.clone()])
            .expect("rollout endpoint");
    assert_eq!(selected.endpoint_id, hostname.endpoint_id);

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn cluster_state_sync_txn_is_empty_when_kv_already_matches() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-sync-idempotent-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");

    let now = OffsetDateTime::now_utc();
    let cluster_id = ClusterId::from_legacy_named("cluster_sync_idem").expect("cluster");
    let node_id = NodeId::from_legacy_named("node_sync_idem").expect("node");
    let mut state = ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "sync idem",
        node_id.clone(),
        "node",
        "pk",
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
        endpoint_id: ClusterEndpointId::from_legacy_named("endpoint_sync_idem").expect("endpoint"),
        node_id: node_id.clone(),
        transport: ClusterTransportKind::LanTcp,
        address: "127.0.0.1:21353".to_string(),
        source: ClusterEndpointSource::Static,
        privacy: ClusterEndpointPrivacy::Lan,
        priority: 0,
        last_observed_latency_ms: None,
    }];
    write_cluster_state(&paths, &state).expect("write state");

    // First sync against an empty KV emits the full state.
    let first = cluster_state_sync_txn(&paths, &state).expect("first sync txn");
    assert!(!first.success.is_empty(), "first sync should publish state");
    append_cluster_raft_command(&paths, &node_id, KvCommand::Txn(first)).expect("apply first sync");

    // Re-syncing identical state is a no-op even though the in-memory state's
    // updated_at would advance on a real publish.
    let mut bumped = state.clone();
    bumped.updated_at = now + time::Duration::seconds(120);
    bumped.kv_revision = state.kv_revision + 5;
    let second = cluster_state_sync_txn(&paths, &bumped).expect("second sync txn");
    assert!(
        second.success.is_empty(),
        "re-syncing semantically identical state must be a no-op, got ops: {:?}",
        second.success
    );

    // A real change (cluster display name -> identity record) must still publish.
    bumped.local_node_display_name = "renamed node".to_string();
    let third = cluster_state_sync_txn(&paths, &bumped).expect("third sync txn");
    assert!(
        !third.success.is_empty(),
        "a real config change must publish"
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
        commit: None,
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
            phase: None,
            progress_current: None,
            progress_total: None,
            progress_message: None,
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
    let mut clean_status = status.clone();
    clean_status.rollout = None;

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
    sync_cluster_node_status_to_kv(&paths, clean_status).expect("clear rollout status");
    let statuses = read_cluster_node_statuses_from_kv(&paths).expect("clean statuses");
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].rollout, None);
    assert!(
        read_cluster_kv_store(&paths)
            .expect("cluster kv store")
            .get(&cluster_rollout_node_key(&state.local_node_id))
            .is_none()
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

#[test]
fn cluster_module_rollout_status_syncs_to_kv() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-module-rollout-status-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");

    let now = OffsetDateTime::now_utc();
    let cluster_id =
        ClusterId::from_legacy_named("cluster_module_rollout_status").expect("cluster");
    let node_id = NodeId::from_legacy_named("node_module_rollout_status").expect("node");
    let state = ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "module rollout status",
        node_id.clone(),
        "network node",
        "public-key",
        now,
    );
    write_cluster_state(&paths, &state).expect("write state");
    sync_cluster_state_to_local_raft_unchecked(&paths, &state).expect("sync members");

    let module_id =
        sinomo_core::Id::from_legacy_named("module.trigger.silence_policy").expect("module");
    let record = ClusterModuleRolloutStatusRecord {
        cluster_id,
        node_id: node_id.clone(),
        rollout_plan_id: sinomo_core::Id::from_legacy_named("rollout_candidate").expect("rollout"),
        module_id: module_id.clone(),
        artifact_id: sinomo_core::Id::from_legacy_named("artifact_candidate").expect("artifact"),
        stage: "canary".to_string(),
        current_weight_bps: 500,
        active_artifact_id: sinomo_core::Id::from_legacy_named("artifact_active").expect("active"),
        previous_artifact_id: None,
        candidate_artifact_id: Some(
            sinomo_core::Id::from_legacy_named("artifact_candidate").expect("candidate"),
        ),
        shadow_artifact_id: None,
        hot_swap_state: "active-candidate".to_string(),
        telemetry_kind: Some("module.invocation.decision".to_string()),
        telemetry_last_consumed_sequence: Some(sinomo_events::EventSequence(12)),
        last_transition_record_id: Some(
            sinomo_core::Id::from_legacy_named("transition_candidate").expect("transition"),
        ),
        updated_at: now,
    };

    sync_cluster_module_rollout_node_to_kv(&paths, &record).expect("sync module rollout status");
    let statuses = read_cluster_module_rollout_statuses_from_kv(&paths).expect("statuses");

    assert_eq!(statuses, vec![record]);
    assert!(
        read_cluster_kv_store(&paths)
            .expect("cluster kv store")
            .get(&cluster_module_rollout_node_key(&node_id, &module_id))
            .is_some()
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn cluster_module_rollout_desired_syncs_to_kv() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-module-rollout-desired-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");

    let now = OffsetDateTime::now_utc();
    let cluster_id =
        ClusterId::from_legacy_named("cluster_module_rollout_desired").expect("cluster");
    let node_id = NodeId::from_legacy_named("node_module_rollout_desired").expect("node");
    let state = ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "module rollout desired",
        node_id.clone(),
        "network node",
        "public-key",
        now,
    );
    write_cluster_state(&paths, &state).expect("write state");
    sync_cluster_state_to_local_raft_unchecked(&paths, &state).expect("sync members");

    let module_id =
        sinomo_core::Id::from_legacy_named("module.trigger.silence_policy").expect("module");
    let rollout_plan_id =
        sinomo_core::Id::from_legacy_named("rollout_desired_candidate").expect("rollout");
    let desired = ClusterModuleRolloutDesiredRecord {
        cluster_id,
        state: "active".to_string(),
        rollout_plan: RolloutPlan {
            rollout_plan_id: rollout_plan_id.clone(),
            artifact_id: sinomo_core::Id::from_legacy_named("artifact_candidate")
                .expect("artifact"),
            module_id: module_id.clone(),
            target_ring: TrustRing::R3SandboxedModule,
            strategy: RolloutStrategy::Shadow,
            target_nodes: vec![node_id.clone()],
            initial_weight_bps: 0,
            promotion_steps_bps: vec![500, 2_500],
            rollback_policy: RollbackPolicy::default(),
        },
        requested_by_node_id: node_id,
        release_id: Some(sinomo_core::Id::from_legacy_named("release_job").expect("release")),
        release_material_hash: Some("blake3:release".to_string()),
        evidence_refs: vec!["registry://artifact-evidence/release_job".to_string()],
        requested_at: Some("2026-06-01T00:00:00Z".to_string()),
        updated_at: now,
    };

    sync_cluster_module_rollout_desired_to_kv(&paths, &desired)
        .expect("sync module rollout desired");
    let desired_records = read_cluster_module_rollout_desired_from_kv(&paths).expect("desired");

    assert_eq!(desired_records, vec![desired]);
    assert!(
        read_cluster_kv_store(&paths)
            .expect("cluster kv store")
            .get(&cluster_module_rollout_desired_key(
                &module_id,
                &rollout_plan_id
            ))
            .is_some()
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn cluster_module_rollout_reconciliation_syncs_to_kv() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-module-rollout-reconcile-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");

    let now = OffsetDateTime::now_utc();
    let cluster_id =
        ClusterId::from_legacy_named("cluster_module_rollout_reconcile").expect("cluster");
    let node_id = NodeId::from_legacy_named("node_module_rollout_reconcile").expect("node");
    let state = ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "module rollout reconcile",
        node_id.clone(),
        "network node",
        "public-key",
        now,
    );
    write_cluster_state(&paths, &state).expect("write state");
    sync_cluster_state_to_local_raft_unchecked(&paths, &state).expect("sync members");

    let module_id =
        sinomo_core::Id::from_legacy_named("module.trigger.silence_policy").expect("module");
    let rollout_plan_id =
        sinomo_core::Id::from_legacy_named("rollout_reconcile_candidate").expect("rollout");
    let record = ClusterModuleRolloutReconciliationRecord {
        cluster_id,
        node_id: node_id.clone(),
        rollout_plan_id: rollout_plan_id.clone(),
        module_id: module_id.clone(),
        artifact_id: sinomo_core::Id::from_legacy_named("artifact_candidate").expect("artifact"),
        state: "blocked".to_string(),
        reason: "release material is not available locally".to_string(),
        release_id: Some(sinomo_core::Id::from_legacy_named("release_job").expect("release")),
        release_material_hash: Some("blake3:release".to_string()),
        local_runtime_stage: None,
        updated_at: now,
    };

    sync_cluster_module_rollout_reconciliation_to_kv(&paths, &record)
        .expect("sync module rollout reconciliation");
    let records =
        read_cluster_module_rollout_reconciliations_from_kv(&paths).expect("reconciliations");

    assert_eq!(records, vec![record]);
    assert!(
        read_cluster_kv_store(&paths)
            .expect("cluster kv store")
            .get(&cluster_module_rollout_reconcile_key(
                &node_id,
                &module_id,
                &rollout_plan_id
            ))
            .is_some()
    );
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
fn cluster_state_read_rejects_oversized_object_file_before_reading() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-cluster-state-oversized-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");
    std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&paths.state)
        .expect("open oversized state")
        .set_len(MAX_OBJECT_FILE_READ_BYTES + 1)
        .expect("make sparse oversized state");

    let error = read_cluster_state(&paths).expect_err("oversized object file should be rejected");
    assert!(
        matches!(error, NetworkError::StorageIo { ref detail, .. } if detail.contains("max is")),
        "unexpected error for oversized state: {error}"
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
fn connectivity_sync_refreshes_stable_records_near_expiry() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-connectivity-refresh-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = sinomo_local::SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");

    let now = OffsetDateTime::now_utc();
    let cluster_id =
        ClusterId::from_legacy_named("cluster_network_connectivity_refresh").expect("cluster");
    let source_node_id =
        NodeId::from_legacy_named("node_network_connectivity_refresh_source").expect("source");
    let target_node_id =
        NodeId::from_legacy_named("node_network_connectivity_refresh_target").expect("target");
    let endpoint_id = ClusterEndpointId::from_legacy_named("endpoint_network_connectivity_refresh")
        .expect("endpoint");
    let mut stored = ClusterConnectivityRecord {
        cluster_id,
        source_node_id: source_node_id.clone(),
        target_node_id,
        endpoint_id,
        transport: ClusterTransportKind::LanTcp,
        address: "192.0.2.12:21353".to_string(),
        state: ClusterConnectivityState::Healthy,
        latency_ms: Some(12),
        consecutive_failures: 0,
        last_success_at: Some("2026-05-06T00:00:00Z".to_string()),
        last_failure_at: None,
        observed_at: now - time::Duration::seconds(300),
        expires_at: now + time::Duration::seconds(30),
    };
    let mut incoming = stored.clone();
    incoming.latency_ms = Some(18);
    incoming.observed_at = now;
    incoming.expires_at = now + time::Duration::seconds(900);

    let mut store = InProcessKvStore::new();
    store
        .put_serde(
            cluster_connectivity_key(
                &stored.source_node_id,
                &stored.target_node_id,
                &stored.endpoint_id,
            ),
            &stored,
            None,
        )
        .expect("stored connectivity");
    write_cluster_kv_snapshot(&paths, &store.snapshot()).expect("cluster kv snapshot");

    let ops = cluster_connectivity_sync_ops(&paths, &source_node_id, &[incoming.clone()])
        .expect("connectivity sync ops");
    assert_eq!(
        ops.len(),
        1,
        "stable connectivity records near expiry must refresh their KV TTL"
    );

    stored.expires_at = now + time::Duration::seconds(300);
    store
        .put_serde(
            cluster_connectivity_key(
                &stored.source_node_id,
                &stored.target_node_id,
                &stored.endpoint_id,
            ),
            &stored,
            None,
        )
        .expect("fresh stored connectivity");
    write_cluster_kv_snapshot(&paths, &store.snapshot()).expect("fresh cluster kv snapshot");
    let ops =
        cluster_connectivity_sync_ops(&paths, &source_node_id, &[incoming]).expect("fresh ops");
    assert!(
        ops.is_empty(),
        "fresh stable connectivity records should not be rewritten just to update timestamps"
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

#[test]
fn cluster_kv_watch_events_append_log_suppresses_duplicate_batches() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-watch-events-append-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");
    let event = KvEvent {
        revision: KvRevision(1),
        key: "/watch/append".to_string(),
        kind: KvEventKind::Put {
            value: b"value".to_vec(),
        },
    };

    assert_eq!(
        append_cluster_kv_watch_events(&paths, std::slice::from_ref(&event))
            .expect("append watch event"),
        1
    );
    assert!(
        paths.kv_watch_events_log.exists(),
        "new watch events should append to the log"
    );
    assert!(
        !paths.kv_watch_events.exists(),
        "small batches should not rewrite the compacted checkpoint"
    );
    let log_len = std::fs::metadata(&paths.kv_watch_events_log)
        .expect("watch event log metadata")
        .len();

    assert_eq!(
        append_cluster_kv_watch_events(&paths, std::slice::from_ref(&event))
            .expect("duplicate watch event"),
        0
    );
    assert_eq!(
        std::fs::metadata(&paths.kv_watch_events_log)
            .expect("watch event log metadata after duplicate")
            .len(),
        log_len,
        "duplicate batches should not append more log bytes"
    );
    assert_eq!(
        cluster_kv_watch_events(&paths).expect("read watch events"),
        vec![event]
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn cluster_kv_watch_event_log_compacts_to_checkpoint_after_large_batch() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-watch-events-compact-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");
    let events = (0..(MAX_CLUSTER_KV_WATCH_EVENTS * 2))
        .map(|index| KvEvent {
            revision: KvRevision(index as u64 + 1),
            key: format!("/watch/compact/{index}"),
            kind: KvEventKind::Put {
                value: index.to_string().into_bytes(),
            },
        })
        .collect::<Vec<_>>();

    assert_eq!(
        append_cluster_kv_watch_events(&paths, &events).expect("append watch events"),
        events.len()
    );

    let retained = cluster_kv_watch_events(&paths).expect("read compacted watch events");
    assert_eq!(retained.len(), MAX_CLUSTER_KV_WATCH_EVENTS);
    assert_eq!(
        retained.first().expect("first retained").revision,
        KvRevision(MAX_CLUSTER_KV_WATCH_EVENTS as u64 + 1)
    );
    assert!(
        paths.kv_watch_events.exists(),
        "large batches should rewrite a bounded checkpoint"
    );
    assert_eq!(
        std::fs::metadata(&paths.kv_watch_events_log)
            .expect("watch event log metadata")
            .len(),
        0,
        "compaction should clear the append log after checkpointing"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn cluster_kv_watch_event_log_keeps_complete_boundary_revision() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-watch-events-boundary-revision-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");
    let mut events = (1..=(MAX_CLUSTER_KV_WATCH_EVENTS as u64 + 10))
        .map(|revision| KvEvent {
            revision: KvRevision(revision),
            key: format!("/watch/boundary/{revision:03}"),
            kind: KvEventKind::Put {
                value: revision.to_string().into_bytes(),
            },
        })
        .collect::<Vec<_>>();
    events.push(KvEvent {
        revision: KvRevision(11),
        key: "/watch/boundary/011-extra".to_string(),
        kind: KvEventKind::Put {
            value: b"extra".to_vec(),
        },
    });

    append_cluster_kv_watch_events(&paths, &events).expect("append watch events");

    let retained = cluster_kv_watch_events(&paths).expect("read retained events");
    assert_eq!(
        retained.first().expect("first retained").revision,
        KvRevision(11)
    );
    assert_eq!(
        retained
            .iter()
            .filter(|event| event.revision == KvRevision(11))
            .count(),
        2,
        "all events in the boundary revision should be retained together"
    );
    assert_eq!(retained.len(), MAX_CLUSTER_KV_WATCH_EVENTS + 1);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn cluster_kv_watch_event_log_drops_delayed_events_older_than_retention_floor() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-watch-events-drop-stale-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");
    let events = (1..=(MAX_CLUSTER_KV_WATCH_EVENTS as u64 * 2))
        .map(|revision| KvEvent {
            revision: KvRevision(revision),
            key: format!("/watch/drop-stale/{revision:03}"),
            kind: KvEventKind::Put {
                value: revision.to_string().into_bytes(),
            },
        })
        .collect::<Vec<_>>();
    append_cluster_kv_watch_events(&paths, &events).expect("append watch events");
    let log_len = std::fs::metadata(&paths.kv_watch_events_log)
        .expect("watch event log metadata")
        .len();
    let stale = KvEvent {
        revision: KvRevision(1),
        key: "/watch/drop-stale/001".to_string(),
        kind: KvEventKind::Put {
            value: b"stale".to_vec(),
        },
    };

    assert_eq!(
        append_cluster_kv_watch_events(&paths, std::slice::from_ref(&stale))
            .expect("append stale watch event"),
        0
    );
    assert_eq!(
        std::fs::metadata(&paths.kv_watch_events_log)
            .expect("watch event log metadata after stale")
            .len(),
        log_len,
        "stale delayed events should not add write amplification"
    );
    assert!(
        !cluster_kv_watch_events(&paths)
            .expect("read watch events")
            .contains(&stale)
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn cluster_kv_watch_event_log_repairs_partial_tail_before_append() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-watch-events-partial-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");
    let first = KvEvent {
        revision: KvRevision(1),
        key: "/watch/partial/1".to_string(),
        kind: KvEventKind::Put {
            value: b"one".to_vec(),
        },
    };
    let second = KvEvent {
        revision: KvRevision(2),
        key: "/watch/partial/2".to_string(),
        kind: KvEventKind::Put {
            value: b"two".to_vec(),
        },
    };

    append_cluster_kv_watch_events(&paths, std::slice::from_ref(&first)).expect("append first");
    let mut log = std::fs::OpenOptions::new()
        .append(true)
        .open(&paths.kv_watch_events_log)
        .expect("open watch event log");
    log.write_all(&16_u32.to_be_bytes())
        .expect("write partial length");
    log.write_all(b"partial").expect("write partial body");
    log.sync_data().expect("sync partial tail");
    drop(log);

    assert_eq!(
        append_cluster_kv_watch_events(&paths, std::slice::from_ref(&second))
            .expect("append after partial tail"),
        1
    );
    assert_eq!(
        cluster_kv_watch_events(&paths).expect("read repaired events"),
        vec![first, second]
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn cluster_kv_watch_event_log_repairs_oversized_tail_before_append() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-watch-events-oversized-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");
    let first = KvEvent {
        revision: KvRevision(1),
        key: "/watch/oversized/1".to_string(),
        kind: KvEventKind::Put {
            value: b"one".to_vec(),
        },
    };
    let second = KvEvent {
        revision: KvRevision(2),
        key: "/watch/oversized/2".to_string(),
        kind: KvEventKind::Put {
            value: b"two".to_vec(),
        },
    };

    append_cluster_kv_watch_events(&paths, std::slice::from_ref(&first)).expect("append first");
    let mut log = std::fs::OpenOptions::new()
        .append(true)
        .open(&paths.kv_watch_events_log)
        .expect("open watch event log");
    log.write_all(&((MAX_CLUSTER_KV_WATCH_EVENT_RECORD_BYTES as u32 + 1).to_be_bytes()))
        .expect("write oversized length");
    log.write_all(b"ignored").expect("write oversized tail");
    log.sync_data().expect("sync oversized tail");
    drop(log);

    assert_eq!(
        append_cluster_kv_watch_events(&paths, std::slice::from_ref(&second))
            .expect("append after oversized tail"),
        1
    );
    assert_eq!(
        cluster_kv_watch_events(&paths).expect("read repaired events"),
        vec![first, second]
    );
    let log = std::fs::read(&paths.kv_watch_events_log).expect("read repaired log");
    assert!(
        !log.windows(b"ignored".len())
            .any(|window| window == b"ignored"),
        "oversized watch-event tail should be truncated before appending"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn cluster_kv_watch_event_log_repairs_unreadable_tail_before_append() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-watch-events-unreadable-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");
    let first = KvEvent {
        revision: KvRevision(1),
        key: "/watch/unreadable/1".to_string(),
        kind: KvEventKind::Put {
            value: b"one".to_vec(),
        },
    };
    let second = KvEvent {
        revision: KvRevision(2),
        key: "/watch/unreadable/2".to_string(),
        kind: KvEventKind::Put {
            value: b"two".to_vec(),
        },
    };

    append_cluster_kv_watch_events(&paths, std::slice::from_ref(&first)).expect("append first");
    let mut log = std::fs::OpenOptions::new()
        .append(true)
        .open(&paths.kv_watch_events_log)
        .expect("open watch event log");
    log.write_all(&4_u32.to_be_bytes())
        .expect("write unreadable length");
    log.write_all(b"junk").expect("write unreadable body");
    log.sync_data().expect("sync unreadable tail");
    drop(log);

    assert_eq!(
        append_cluster_kv_watch_events(&paths, std::slice::from_ref(&second))
            .expect("append after unreadable tail"),
        1
    );
    assert_eq!(
        cluster_kv_watch_events(&paths).expect("read repaired events"),
        vec![first, second]
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn cluster_kv_watch_event_log_serializes_concurrent_duplicate_appends() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-watch-events-concurrent-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");
    let event = KvEvent {
        revision: KvRevision(1),
        key: "/watch/concurrent".to_string(),
        kind: KvEventKind::Put {
            value: b"value".to_vec(),
        },
    };
    let threads = 8;
    let barrier = Arc::new(Barrier::new(threads));
    let handles = (0..threads)
        .map(|_| {
            let paths = paths.clone();
            let event = event.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                append_cluster_kv_watch_events(&paths, std::slice::from_ref(&event))
                    .expect("append duplicate watch event")
            })
        })
        .collect::<Vec<_>>();
    let stored = handles
        .into_iter()
        .map(|handle| handle.join().expect("append thread"))
        .sum::<usize>();

    assert_eq!(stored, 1);
    assert_eq!(
        cluster_kv_watch_events(&paths).expect("read concurrent events"),
        vec![event]
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn cluster_kv_watch_drain_reads_current_raft_view_when_snapshot_is_stale() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-watch-current-raft-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");
    let now = OffsetDateTime::now_utc();
    let cluster_id = ClusterId::from_legacy_named("cluster_watch_current_raft").expect("cluster");
    let node_id = NodeId::from_legacy_named("node_watch_current_raft").expect("node");
    let state = ClusterBootstrapState::initialized_locally(
        cluster_id,
        "watch current raft",
        node_id.clone(),
        "node",
        "public-key",
        now,
    );
    write_cluster_state(&paths, &state).expect("write state");
    append_cluster_raft_command(
        &paths,
        &node_id,
        KvCommand::Op(KvOp::Put {
            key: "/watch/current".to_string(),
            value: b"current".to_vec(),
            lease_id: None,
        }),
    )
    .expect("append current raft value");
    write_cluster_kv_snapshot(&paths, &InProcessKvStore::new().snapshot())
        .expect("overwrite stale materialized snapshot");

    let watch = create_cluster_kv_watch(
        &paths,
        ClusterKvWatchPrefixRequest {
            prefix: "/watch/".to_string(),
            start_revision: KvRevision(1),
        },
    )
    .expect("create watch");
    let drained = drain_cluster_kv_watch(
        &paths,
        ClusterKvWatchDrainRequest {
            watch_id: watch.watch_id,
            wait_timeout_millis: 0,
        },
    )
    .expect("drain watch");

    assert!(
        drained
            .events
            .iter()
            .any(|event| event.key == "/watch/current")
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn cluster_kv_watch_drain_waits_for_fanned_out_events() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-watch-wait-fanout-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");
    write_cluster_kv_snapshot(&paths, &InProcessKvStore::new().snapshot())
        .expect("write initial snapshot");
    let watch = create_cluster_kv_watch(
        &paths,
        ClusterKvWatchPrefixRequest {
            prefix: "/watch/wait/".to_string(),
            start_revision: KvRevision(1),
        },
    )
    .expect("create watch");
    let (started_tx, started_rx) = mpsc::channel();
    let drain_paths = paths.clone();
    let handle = thread::spawn(move || {
        started_tx.send(()).expect("signal drain start");
        let started = Instant::now();
        let drained = drain_cluster_kv_watch(
            &drain_paths,
            ClusterKvWatchDrainRequest {
                watch_id: watch.watch_id,
                wait_timeout_millis: 5_000,
            },
        )
        .expect("wait drain");
        (started.elapsed(), drained)
    });
    started_rx.recv().expect("drain thread started");
    thread::sleep(Duration::from_millis(50));
    let event = KvEvent {
        revision: KvRevision(1),
        key: "/watch/wait/fanout".to_string(),
        kind: KvEventKind::Put {
            value: b"fanout".to_vec(),
        },
    };

    append_cluster_kv_watch_events(&paths, std::slice::from_ref(&event))
        .expect("append fanout event");
    let (elapsed, drained) = handle.join().expect("wait drain thread");

    assert!(
        elapsed < Duration::from_secs(2),
        "waiting drain should be notified instead of timing out after {elapsed:?}"
    );
    assert_eq!(drained.events, vec![event]);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn cluster_kv_watch_drain_waits_for_local_raft_commit() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-watch-wait-raft-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");
    let now = OffsetDateTime::now_utc();
    let cluster_id = ClusterId::from_legacy_named("cluster_watch_wait_raft").expect("cluster");
    let node_id = NodeId::from_legacy_named("node_watch_wait_raft").expect("node");
    let state = ClusterBootstrapState::initialized_locally(
        cluster_id,
        "watch wait raft",
        node_id.clone(),
        "node",
        "public-key",
        now,
    );
    write_cluster_state(&paths, &state).expect("write state");
    write_cluster_kv_snapshot(&paths, &InProcessKvStore::new().snapshot())
        .expect("write initial snapshot");
    let watch = create_cluster_kv_watch(
        &paths,
        ClusterKvWatchPrefixRequest {
            prefix: "/watch/wait/".to_string(),
            start_revision: KvRevision(1),
        },
    )
    .expect("create watch");
    let (started_tx, started_rx) = mpsc::channel();
    let drain_paths = paths.clone();
    let handle = thread::spawn(move || {
        started_tx.send(()).expect("signal drain start");
        let started = Instant::now();
        let drained = drain_cluster_kv_watch(
            &drain_paths,
            ClusterKvWatchDrainRequest {
                watch_id: watch.watch_id,
                wait_timeout_millis: 5_000,
            },
        )
        .expect("wait drain");
        (started.elapsed(), drained)
    });
    started_rx.recv().expect("drain thread started");
    thread::sleep(Duration::from_millis(50));

    append_cluster_raft_command(
        &paths,
        &node_id,
        KvCommand::Op(KvOp::Put {
            key: "/watch/wait/raft".to_string(),
            value: b"raft".to_vec(),
            lease_id: None,
        }),
    )
    .expect("append local raft value");
    let (elapsed, drained) = handle.join().expect("wait drain thread");

    assert!(
        elapsed < Duration::from_secs(2),
        "waiting drain should be notified by committed raft events after {elapsed:?}"
    );
    assert!(
        drained
            .events
            .iter()
            .any(|event| event.key == "/watch/wait/raft")
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn cluster_kv_watch_create_allocates_unique_ids_concurrently() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-watch-create-concurrent-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");
    write_cluster_kv_snapshot(&paths, &InProcessKvStore::new().snapshot())
        .expect("write initial snapshot");
    let threads = 16;
    let barrier = Arc::new(Barrier::new(threads));
    let handles = (0..threads)
        .map(|index| {
            let paths = paths.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                create_cluster_kv_watch(
                    &paths,
                    ClusterKvWatchPrefixRequest {
                        prefix: format!("/watch/concurrent/{index}/"),
                        start_revision: KvRevision(1),
                    },
                )
                .expect("create concurrent watch")
            })
        })
        .collect::<Vec<_>>();
    let mut registrations = handles
        .into_iter()
        .map(|handle| handle.join().expect("watch create thread"))
        .collect::<Vec<_>>();
    registrations.sort_by_key(|registration| registration.watch_id);
    let mut unique_ids = registrations
        .iter()
        .map(|registration| registration.watch_id)
        .collect::<Vec<_>>();
    unique_ids.dedup();

    assert_eq!(unique_ids.len(), threads);
    assert!(
        paths.kv_watches_lock.exists(),
        "concurrent watch allocation should use a durable lock file"
    );
    for registration in registrations {
        let watch_path = paths
            .kv_watches
            .join(format!("watch-{}.sobj", registration.watch_id.0));
        let stored: ClusterKvWatchRegistration =
            read_object_file(&watch_path).expect("read watch registration");
        assert_eq!(stored, registration);
    }
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn cluster_kv_watch_drain_removes_compacted_registration() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-watch-compacted-drain-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");
    write_cluster_kv_snapshot(&paths, &InProcessKvStore::new().snapshot())
        .expect("write initial snapshot");
    let watch = create_cluster_kv_watch(
        &paths,
        ClusterKvWatchPrefixRequest {
            prefix: "/watch/".to_string(),
            start_revision: KvRevision(1),
        },
    )
    .expect("create watch");
    let watch_path = paths
        .kv_watches
        .join(format!("watch-{}.sobj", watch.watch_id.0));
    assert!(watch_path.exists(), "watch registration should exist");

    let mut compacted = InProcessKvStore::new();
    compacted
        .put("/watch/old", b"old".to_vec(), None)
        .expect("put compacted value");
    compacted.compact(KvRevision(1)).expect("compact store");
    write_cluster_kv_snapshot(&paths, &compacted.snapshot()).expect("write compacted snapshot");

    let error = drain_cluster_kv_watch(
        &paths,
        ClusterKvWatchDrainRequest {
            watch_id: watch.watch_id,
            wait_timeout_millis: 0,
        },
    )
    .expect_err("compacted watch");
    assert!(matches!(
        error,
        NetworkError::Kv(KvError::Compacted {
            requested: KvRevision(1),
            compacted: KvRevision(1),
        })
    ));
    assert!(
        !watch_path.exists(),
        "compacted watch registration should be removed after failed drain"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn create_cluster_kv_watch_prunes_compacted_registrations() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-watch-compacted-create-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");
    write_cluster_kv_snapshot(&paths, &InProcessKvStore::new().snapshot())
        .expect("write initial snapshot");
    let stale = create_cluster_kv_watch(
        &paths,
        ClusterKvWatchPrefixRequest {
            prefix: "/watch/".to_string(),
            start_revision: KvRevision(1),
        },
    )
    .expect("create stale watch");
    let live = create_cluster_kv_watch(
        &paths,
        ClusterKvWatchPrefixRequest {
            prefix: "/watch/".to_string(),
            start_revision: KvRevision(2),
        },
    )
    .expect("create live watch");
    let stale_path = paths
        .kv_watches
        .join(format!("watch-{}.sobj", stale.watch_id.0));
    let live_path = paths
        .kv_watches
        .join(format!("watch-{}.sobj", live.watch_id.0));
    assert!(stale_path.exists(), "stale watch registration should exist");
    assert!(live_path.exists(), "live watch registration should exist");

    let mut compacted = InProcessKvStore::new();
    compacted
        .put("/watch/old", b"old".to_vec(), None)
        .expect("put compacted value");
    compacted.compact(KvRevision(1)).expect("compact store");
    write_cluster_kv_snapshot(&paths, &compacted.snapshot()).expect("write compacted snapshot");

    let newest = create_cluster_kv_watch(
        &paths,
        ClusterKvWatchPrefixRequest {
            prefix: "/watch/".to_string(),
            start_revision: KvRevision(2),
        },
    )
    .expect("create watch after compaction");
    let newest_path = paths
        .kv_watches
        .join(format!("watch-{}.sobj", newest.watch_id.0));

    assert!(
        !stale_path.exists(),
        "watch registrations behind the compaction floor should be pruned"
    );
    assert!(
        live_path.exists(),
        "watch registrations newer than the compaction floor should be kept"
    );
    assert!(newest_path.exists(), "new watch registration should exist");
    assert!(
        newest.watch_id > live.watch_id,
        "new watch ids should continue after retained registrations"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn cluster_kv_materialized_snapshot_event_history_is_bounded() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-materialized-events-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");
    let mut store = InProcessKvStore::new();
    for index in 0..(MAX_CLUSTER_KV_MATERIALIZED_EVENTS + 10) {
        store
            .put(
                format!("/materialized/{index}"),
                index.to_string().into_bytes(),
                None,
            )
            .expect("put event");
    }

    write_cluster_kv_snapshot(&paths, &store.snapshot()).expect("write snapshot");

    let stored = read_cluster_kv_snapshot(&paths).expect("read snapshot");
    assert_eq!(stored.events.len(), MAX_CLUSTER_KV_MATERIALIZED_EVENTS);
    assert_eq!(stored.compacted_revision, KvRevision(10));
    assert_eq!(
        stored.events.first().expect("first retained").revision,
        KvRevision(11)
    );
    assert_eq!(
        stored.events.last().expect("last retained").revision,
        KvRevision(MAX_CLUSTER_KV_MATERIALIZED_EVENTS as u64 + 10)
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn committed_raft_index_reports_only_new_events() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-committed-events-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");
    let node_id = NodeId::from_legacy_named("node_committed_events").expect("node");
    let first = append_cluster_raft_command_uncommitted(
        &paths,
        &node_id,
        KvCommand::Op(KvOp::Put {
            key: "/committed/one".to_string(),
            value: b"one".to_vec(),
            lease_id: None,
        }),
    )
    .expect("append first");
    commit_cluster_raft_index_with_events(&paths, &node_id, first.index).expect("commit first");
    let second = append_cluster_raft_command_uncommitted(
        &paths,
        &node_id,
        KvCommand::Txn(KvTxn {
            compares: Vec::new(),
            success: vec![
                KvOp::Put {
                    key: "/committed/two".to_string(),
                    value: b"two".to_vec(),
                    lease_id: None,
                },
                KvOp::Put {
                    key: "/committed/three".to_string(),
                    value: b"three".to_vec(),
                    lease_id: None,
                },
            ],
            failure: Vec::new(),
        }),
    )
    .expect("append second");

    let committed =
        commit_cluster_raft_index_with_events(&paths, &node_id, second.index).expect("commit");

    assert_eq!(committed.events.len(), 2);
    assert_eq!(committed.events[0].key, "/committed/two");
    assert_eq!(committed.events[1].key, "/committed/three");
    assert!(
        committed
            .events
            .iter()
            .all(|event| event.revision > KvRevision(1))
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn cluster_raft_snapshot_event_history_is_bounded_when_compacted() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-raft-snapshot-events-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");
    let node_id = NodeId::from_legacy_named("node_raft_snapshot_events").expect("node");

    // Compaction only fires once the un-snapshotted applied tail exceeds
    // 2 * RAFT_COMPACTION_RETAINED_ENTRIES, so commit well past that watermark for a
    // snapshot to be written at all; compaction then truncates the snapshot's event
    // history to MAX_CLUSTER_KV_RAFT_SNAPSHOT_EVENTS.
    for index in 0..(2 * sinomo_kv::RAFT_COMPACTION_RETAINED_ENTRIES as usize + 80) {
        let entry = append_cluster_raft_command_uncommitted(
            &paths,
            &node_id,
            KvCommand::Op(KvOp::Put {
                key: format!("/raft-snapshot/{index}"),
                value: vec![b'x'; 256],
                lease_id: None,
            }),
        )
        .expect("append");
        commit_cluster_raft_index_with_events(&paths, &node_id, entry.index).expect("commit");
    }

    let wal_paths = cluster_raft_wal_paths(&paths, &node_id);
    let stored: RaftSnapshot = read_object_file(&wal_paths.snapshot).expect("raft snapshot");
    assert!(
        stored.kv.events.len() <= MAX_CLUSTER_KV_RAFT_SNAPSHOT_EVENTS,
        "raft snapshots should not retain an unbounded event tail"
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
        EventKind::new("workflow.job.scoped").expect("kind"),
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

/// Look up a delivery's current status via a spawned service's ledger snapshot.
async fn delivery_status<T, S>(
    service: &NetworkServiceHandle<T, S>,
    delivery_id: RouteDeliveryId,
) -> DeliveryStatus
where
    T: NetworkTransport + Send + 'static,
    S: EventStore + Send + 'static,
{
    service
        .ledger_records()
        .await
        .expect("ledger")
        .into_iter()
        .find(|record| record.delivery.delivery_id == delivery_id)
        .expect("record")
        .delivery
        .status
}

#[tokio::test]
async fn network_service_delivers_event_and_processes_ack() {
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

    let mut store_a = InMemoryEventStore::new();
    let event = fixture_event("evt_service_delivery", node_a.clone()).with_session(session_id);
    let receipt = store_a.append(event).expect("append");
    let stored = store_a
        .replay(ReplayQuery::all().after(EventSequence(receipt.sequence.0 - 1)))
        .expect("replay")
        .pop()
        .expect("stored");

    let service_a = NetworkService::new(registry.clone(), transport_a).spawn(store_a);
    let service_b = NetworkService::new(registry, transport_b).spawn(InMemoryEventStore::new());

    let deliveries = service_a.enqueue_event(stored).await.expect("enqueue");
    assert_eq!(deliveries.len(), 2);
    assert_eq!(service_a.pump_outbound().await.expect("send"), 2);

    let outcomes = service_b.process_incoming().await.expect("process event");
    assert_eq!(outcomes.len(), 1);
    let store_b = service_b.shutdown().await;
    assert_eq!(store_b.len(), 1);

    let ack_outcomes = service_a.process_incoming().await.expect("process ack");
    assert_eq!(ack_outcomes.len(), 2);
    assert_eq!(
        ack_outcomes
            .iter()
            .filter(|outcome| matches!(outcome, InboundOutcome::AckAccepted { .. }))
            .count(),
        1
    );
    let delivered = service_a
        .ledger_records()
        .await
        .expect("ledger")
        .into_iter()
        .filter(|record| record.delivery.status == DeliveryStatus::Delivered)
        .count();
    assert_eq!(delivered, 1);
    let store_a = service_a.shutdown().await;
    assert_eq!(store_a.len(), 1);
}

#[tokio::test]
async fn delivery_ledger_suppresses_duplicate_event_targets() {
    let node_a = NodeId::from_legacy_named("node_a").expect("node");
    let node_b = NodeId::from_legacy_named("node_b").expect("node");
    let network = InProcessNetwork::new();
    let transport_a = network.add_node(node_a.clone());
    let mut registry = PeerRegistry::new();
    registry.register_node(node("node_a"));
    registry.register_node(node("node_b"));
    let mut store = InMemoryEventStore::new();
    let event = fixture_event("evt_duplicate_delivery", node_a.clone()).with_target_node(node_b);
    let receipt = store.append(event).expect("append");
    let stored = store
        .replay(ReplayQuery::all().after(EventSequence(receipt.sequence.0 - 1)))
        .expect("replay")
        .pop()
        .expect("stored");
    let service = NetworkService::new(registry, transport_a).spawn(store);

    assert_eq!(
        service
            .enqueue_event(stored.clone())
            .await
            .expect("first")
            .len(),
        1
    );
    assert_eq!(
        service
            .enqueue_event(stored)
            .await
            .expect("duplicate")
            .len(),
        0
    );
    assert_eq!(service.ledger_records().await.expect("ledger").len(), 1);
    service.shutdown().await;
}

#[tokio::test]
async fn inbound_duplicate_event_is_acked_without_duplicate_append() {
    let node_a = NodeId::from_legacy_named("node_a").expect("node");
    let node_b = NodeId::from_legacy_named("node_b").expect("node");
    let network = InProcessNetwork::new();
    // A bare transport handle injects the duplicate frames; only the receiver runs as
    // a service actor.
    let mut transport_a = network.add_node(node_a.clone());
    let transport_b = network.add_node(node_b.clone());
    let mut registry = PeerRegistry::new();
    registry.register_node(node("node_a"));
    registry.register_node(node("node_b"));
    let service_b = NetworkService::new(registry, transport_b).spawn(InMemoryEventStore::new());
    let event =
        fixture_event("evt_duplicate_inbound", node_a.clone()).with_target_node(node_b.clone());

    for frame_id in [1, 2] {
        let frame = NetworkFrame {
            frame_id: NetworkFrameId(frame_id),
            source_node_id: node_a.clone(),
            target_node_id: node_b.clone(),
            message: MeshMessage {
                message_id: MeshMessageId(frame_id),
                origin_node_id: node_a.clone(),
                target_node_id: node_b.clone(),
                body: MeshMessageBody::Event {
                    delivery_id: RouteDeliveryId(1),
                    event: Box::new(event.clone()),
                },
            },
        };
        transport_a.send(&node_b, frame).expect("send duplicate");
    }

    let outcomes = service_b
        .process_incoming()
        .await
        .expect("process duplicates");
    assert!(matches!(outcomes[0], InboundOutcome::EventAccepted { .. }));
    assert!(matches!(outcomes[1], InboundOutcome::EventDuplicate { .. }));
    let store_b = service_b.shutdown().await;
    assert_eq!(store_b.len(), 1);
}

#[tokio::test]
async fn retry_policy_marks_delivery_failed_after_max_attempts() {
    let node_a = NodeId::from_legacy_named("node_a").expect("node");
    let node_b = NodeId::from_legacy_named("node_b").expect("node");
    let network = InProcessNetwork::new();
    let transport_a = network.add_node(node_a.clone());
    let mut registry = PeerRegistry::new();
    registry.register_node(node("node_a"));
    registry.register_node(node("node_b"));
    let mut store = InMemoryEventStore::new();
    let event = fixture_event("evt_retry_failed", node_a.clone()).with_target_node(node_b);
    let receipt = store.append(event).expect("append");
    let stored = store
        .replay(ReplayQuery::all().after(EventSequence(receipt.sequence.0 - 1)))
        .expect("replay")
        .pop()
        .expect("stored");
    let service = NetworkService::new(registry, transport_a)
        .with_retry_policy(RetryPolicy { max_attempts: 2 })
        .spawn(store);
    let delivery = service.enqueue_event(stored).await.expect("enqueue")[0].clone();

    assert!(service.pump_outbound().await.is_ok());
    assert_eq!(
        delivery_status(&service, delivery.delivery_id).await,
        DeliveryStatus::Retrying
    );
    let error = service
        .pump_outbound()
        .await
        .expect_err("final attempt should return send failure");
    assert!(matches!(error, NetworkError::UnknownNode { .. }));
    assert_eq!(
        delivery_status(&service, delivery.delivery_id).await,
        DeliveryStatus::Failed
    );
    service.shutdown().await;
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

#[tokio::test]
async fn heartbeat_recovery_requeues_failed_delivery_for_drain() {
    let node_a = NodeId::from_legacy_named("node_a").expect("node");
    let node_b = NodeId::from_legacy_named("node_b").expect("node");
    let network = InProcessNetwork::new();
    let transport_a = network.add_node(node_a.clone());
    let mut registry = PeerRegistry::new();
    registry.register_node(node("node_a"));
    registry.register_node(node("node_b"));
    let mut store_a = InMemoryEventStore::new();
    let event =
        fixture_event("evt_recovery_requeue", node_a.clone()).with_target_node(node_b.clone());
    let receipt = store_a.append(event).expect("append");
    let stored = store_a
        .replay(ReplayQuery::all().after(EventSequence(receipt.sequence.0 - 1)))
        .expect("replay")
        .pop()
        .expect("stored");
    let service_a = NetworkService::new(registry.clone(), transport_a)
        .with_retry_policy(RetryPolicy { max_attempts: 2 })
        .spawn(store_a);
    let delivery_id = service_a.enqueue_event(stored).await.expect("enqueue")[0].delivery_id;

    assert_eq!(service_a.pump_outbound().await.expect("first attempt"), 0);
    assert!(service_a.pump_outbound().await.is_err());
    assert_eq!(
        delivery_status(&service_a, delivery_id).await,
        DeliveryStatus::Failed
    );

    let transport_b = network.add_node(node_b.clone());
    let service_b = NetworkService::new(registry, transport_b).spawn(InMemoryEventStore::new());
    service_b
        .send_heartbeat(node_a.clone(), 42)
        .await
        .expect("send heartbeat");

    let outcomes = service_a
        .process_incoming()
        .await
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
        delivery_status(&service_a, delivery_id).await,
        DeliveryStatus::Retrying
    );

    assert_eq!(service_a.pump_outbound().await.expect("drain recovered"), 1);
    service_a.shutdown().await;
    service_b
        .process_incoming()
        .await
        .expect("receive recovered event");
    let store_b = service_b.shutdown().await;
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

#[tokio::test]
async fn direct_target_offline_buffers_instead_of_sending() {
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
    let mut store = InMemoryEventStore::new();
    let event =
        fixture_event("evt_direct_offline", node_a.clone()).with_target_node(node_b.clone());
    let receipt = store.append(event).expect("append");
    let stored = store
        .replay(ReplayQuery::all().after(EventSequence(receipt.sequence.0 - 1)))
        .expect("replay")
        .pop()
        .expect("stored");
    let service = NetworkService::new(registry, transport_a).spawn(store);
    let delivery_id = service.enqueue_event(stored).await.expect("enqueue")[0].delivery_id;

    assert_eq!(service.pump_outbound().await.expect("buffer offline"), 0);
    assert_eq!(transport_b.drain_incoming().expect("drain").len(), 0);
    assert_eq!(
        delivery_status(&service, delivery_id).await,
        DeliveryStatus::Retrying
    );
    service.shutdown().await;
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

#[derive(Default)]
struct CountingIoObserver {
    actual: std::sync::atomic::AtomicU64,
    skipped: std::sync::atomic::AtomicU64,
    disk_read_bytes: std::sync::atomic::AtomicU64,
    disk_write_bytes: std::sync::atomic::AtomicU64,
}

impl IoObserver for CountingIoObserver {
    fn on_disk_read(&self, bytes: u64) {
        self.disk_read_bytes
            .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
    }
    fn on_disk_write(&self, bytes: u64) {
        self.disk_write_bytes
            .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
    }
    fn on_object_file_actual_write(&self) {
        self.actual
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    fn on_object_file_skipped_write(&self) {
        self.skipped
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

static IO_OBSERVER_TEST_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[test]
fn write_cluster_state_observer_counts_actual_then_skipped() {
    // Serialize with any other test that swaps the global I/O observer.
    let _guard = IO_OBSERVER_TEST_MUTEX.lock().expect("test mutex");

    let observer = Arc::new(CountingIoObserver::default());
    let previous: Option<Arc<dyn IoObserver>> =
        set_io_observer(Some(observer.clone() as Arc<dyn IoObserver>));

    let root = std::env::temp_dir().join(format!(
        "sinomo-network-storage-observer-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("paths");

    let cluster_id = ClusterId::from_legacy_named("cluster_storage_observer").expect("cluster id");
    let node = NodeId::from_legacy_named("node_storage_observer").expect("node id");
    let state = ClusterBootstrapState::initialized_locally(
        cluster_id,
        "storage observer test",
        node,
        "node",
        "pk",
        OffsetDateTime::now_utc(),
    );

    write_cluster_state(&paths, &state).expect("first write");
    write_cluster_state(&paths, &state).expect("second write should short-circuit");

    let actual = observer.actual.load(std::sync::atomic::Ordering::Relaxed);
    let skipped = observer.skipped.load(std::sync::atomic::Ordering::Relaxed);
    let write_bytes = observer
        .disk_write_bytes
        .load(std::sync::atomic::Ordering::Relaxed);
    // `IO_OBSERVER` is process-wide, so a concurrently-running test that writes an
    // object file is counted here too. Our two writes contribute exactly one
    // actual write (first) and one skipped write (second, identical content, to a
    // unique temp dir no other test touches), so assert lower bounds. The precise
    // "identical re-write is skipped" behavior is covered race-free by
    // `write_object_file_skips_identical_and_rewrites_after_external_change`.
    assert!(
        actual >= 1,
        "expected at least our one actual write, got {actual}"
    );
    assert!(
        skipped >= 1,
        "expected at least our one skipped write, got {skipped}"
    );
    assert!(write_bytes > 0, "actual write should report disk bytes");

    // Restore the previous observer so we don't poison sibling tests.
    let _ = set_io_observer(previous);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn write_object_file_skips_compare_read_when_length_differs() {
    let _guard = IO_OBSERVER_TEST_MUTEX.lock().expect("test mutex");

    let observer = Arc::new(CountingIoObserver::default());
    let previous: Option<Arc<dyn IoObserver>> =
        set_io_observer(Some(observer.clone() as Arc<dyn IoObserver>));

    let root = std::env::temp_dir().join(format!(
        "sinomo-network-sparse-objfile-rewrite-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("paths");
    std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&paths.state)
        .expect("open sparse state")
        .set_len(MAX_OBJECT_FILE_READ_BYTES + 1)
        .expect("make sparse oversized state");

    let cluster_id = ClusterId::from_legacy_named("cluster_sparse_rewrite").expect("cluster id");
    let node = NodeId::from_legacy_named("node_sparse_rewrite").expect("node id");
    let state = ClusterBootstrapState::initialized_locally(
        cluster_id,
        "sparse rewrite test",
        node,
        "node",
        "pk",
        OffsetDateTime::now_utc(),
    );

    write_cluster_state(&paths, &state).expect("rewrite sparse state");

    let read_bytes = observer
        .disk_read_bytes
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        read_bytes < MAX_OBJECT_FILE_READ_BYTES,
        "length-mismatched object files should not be read back before rewrite; observed {read_bytes} bytes"
    );

    let _ = set_io_observer(previous);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn write_object_file_skips_identical_and_rewrites_after_external_change() {
    // Unique path so this test is independent of the process-wide I/O observer.
    let root = std::env::temp_dir().join(format!(
        "sinomo-network-objfile-guard-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).expect("root");
    let path = root.join("obj.sobj");

    let value = "alpha".to_string();
    crate::storage::write_object_file(&path, &value).expect("initial write");
    let mtime_after_write = std::fs::metadata(&path)
        .expect("metadata")
        .modified()
        .expect("mtime");

    // An identical re-write takes the fast path and leaves the file untouched.
    crate::storage::write_object_file(&path, &value).expect("identical re-write");
    assert_eq!(
        std::fs::metadata(&path)
            .expect("metadata")
            .modified()
            .expect("mtime"),
        mtime_after_write,
        "an identical re-write must be skipped without rewriting the file"
    );

    // Another writer replaces the content behind our back with the same byte
    // length. This must be detected by the file-identity guard, not just length.
    let original_len = std::fs::metadata(&path).expect("metadata").len() as usize;
    let replacement = vec![b'x'; original_len];
    let replacement_path = root.join("replacement.sobj");
    std::fs::write(&replacement_path, replacement).expect("external write");
    std::fs::rename(&replacement_path, &path).expect("external replace");

    // Writing our content again must detect the change via the stat guard, fall
    // back to read + compare, and actually rewrite -- never falsely skip.
    crate::storage::write_object_file(&path, &value).expect("rewrite after external change");
    let restored: String = SinomoObjectCodec::default()
        .decode(&std::fs::read(&path).expect("read"))
        .expect("decode");
    assert_eq!(
        restored, value,
        "a file changed since our last write must be rewritten, not skipped"
    );

    let _ = std::fs::remove_dir_all(&root);
}
