//! Cluster network, peer transport, and routing primitives.
//!
//! This crate owns route choice, endpoint selection, relay decisions, peer
//! transport contracts, and interpretation of the cluster connectivity graph.
//! Connectivity records are network-domain facts even when they are persisted
//! in KV. This crate interprets those records for routing through narrow query
//! adapters; it must not own KV transactions, watches, compaction, snapshots, or
//! Raft leaders/elections/storage layout. When cluster routes carry KV traffic,
//! this crate wraps KV-owned payload specs in the generic object-RPC envelope and
//! chooses how that envelope reaches a peer. KV local read/watch RPC dispatch
//! and Raft election semantics belong to `sinomo-kv`; network may temporarily
//! host filesystem and transport adapters while cluster-aware validation remains
//! here.

mod channel;
mod channel_transport;
mod channel_wire;
mod connectivity;
mod connectivity_snapshot;
mod connectivity_store;
mod daemon_auth;
mod direct_link;
mod directory;
mod endpoint_advertisement;
mod endpoint_records;
mod endpoint_selector;
mod error;
mod fallible_serde;
mod file_mark;
mod io_observer;
mod lan;
mod maintenance;
mod network_info;
mod network_module;
pub mod network_module_abi;
mod object;
mod object_file_store;
mod object_rpc_stream;
mod path_metrics;
mod paths;
mod peer;
mod roaming;
mod route;
mod route_graph_read_model;
mod routed_identity;
mod routing;
mod rpc;
mod rpc_future;
mod session;
mod tcp_tunnel;
mod timeline;
// Framing and transfer state stay private beneath the canonical session API so
// higher-level crates cannot assemble protocol frames themselves.
#[cfg(test)]
mod layer_boundary_tests;
mod transfer;

pub use channel::{
    OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES, OBJECT_RPC_CHANNEL_INLINE_VALUE_BYTES,
    OBJECT_RPC_CHANNEL_MAX_CALLS, OBJECT_RPC_CHANNEL_MAX_MATERIALIZED_VALUE_BYTES,
    OBJECT_RPC_CHANNEL_MAX_QUEUED_BYTES, ObjectRpcChannelBodyDescriptor,
    ObjectRpcChannelBodyDirection, ObjectRpcChannelBodyKind, ObjectRpcChannelCallId,
    ObjectRpcChannelClient, ObjectRpcChannelConfig, ObjectRpcChannelEgress,
    ObjectRpcChannelEndpoint, ObjectRpcChannelInboundRequest, ObjectRpcChannelIngress,
    ObjectRpcChannelPendingCall, ObjectRpcChannelQueueSnapshot, ObjectRpcChannelResponder,
    ObjectRpcChannelRole, ObjectRpcChannelServer, ObjectRpcChannelWireFrame,
    ObjectRpcChannelWriteTimeline,
};
pub use channel_transport::{
    AcceptedLocalObjectRpcChannel, ConnectedLocalObjectRpcChannel,
    LOCAL_OBJECT_RPC_CHANNEL_PROTOCOL_VERSION, LocalObjectRpcChannelTransport,
    accept_local_object_rpc_channel_async, connect_local_object_rpc_channel_async,
    local_object_rpc_channel_wire_prefix_matches,
};
/// Carrier-only surface needed by the daemon crate through the R0 facade.
/// This is not a second application RPC API.
#[doc(hidden)]
pub mod __channel_carrier {
    pub use super::channel_wire::{
        OBJECT_RPC_CHANNEL_DATA_HEADER_BYTES, OBJECT_RPC_CHANNEL_DATA_MAGIC,
        OBJECT_RPC_CHANNEL_DATA_VERSION, OBJECT_RPC_CHANNEL_RECEIVE_BUFFER_BYTES,
        ObjectRpcChannelDataFrame, ObjectRpcChannelDataHeader, ObjectRpcChannelReceiveBufferPool,
        ObjectRpcChannelTransportFrame, read_object_rpc_channel_transport_frame_async,
        read_object_rpc_channel_transport_frame_async_timed,
        write_object_rpc_channel_data_frame_async, write_object_rpc_channel_transport_frame_async,
        write_object_rpc_channel_transport_frame_async_timed,
    };
}
/// Internal generation-fenced local connectivity-store boundary used by the
/// daemon's reactive projection owner.
#[doc(hidden)]
pub mod __connectivity_store {
    pub use super::connectivity_store::{
        ClusterConnectivityRoutingProjection,
        LocalClusterConnectivityRoutingGenerationSubscription,
        LocalClusterConnectivitySourceSetCommit, LocalClusterConnectivityStoreSnapshot,
        capture_local_cluster_connectivity_store_snapshot, cluster_connectivity_routing_projection,
        commit_local_cluster_connectivity_source_set_if_snapshot,
        commit_local_cluster_connectivity_source_set_if_snapshot_async,
        local_cluster_connectivity_content_generation,
        record_daemon_owned_local_cluster_endpoint_command_connectivity,
        subscribe_local_cluster_connectivity_routing_generation,
    };
}
pub use connectivity::cluster_network_record_live;
pub use connectivity::{
    CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD, CLUSTER_CONNECTIVITY_TTL_SECONDS,
    CLUSTER_CONNECTIVITY_UNREACHABLE_TTL_SECONDS, ClusterConnectivityFailureReason,
    ClusterConnectivityGraph, ClusterConnectivityProbeBatch, ClusterConnectivityProbeBatchEntry,
    ClusterConnectivityProbeObservation, ClusterConnectivityProbeState, ClusterConnectivityRecord,
    ClusterConnectivityRecordQuery, ClusterConnectivityState,
    MAX_CLUSTER_CONNECTIVITY_SNAPSHOT_RECORDS, MAX_CLUSTER_CONNECTIVITY_SOURCE_RECORDS,
    apply_cluster_connectivity_probe_results, cluster_connectivity_probe_due_with_graph,
    cluster_connectivity_probe_due_with_startup_settle, cluster_connectivity_record,
    cluster_connectivity_record_for_command_auth_denied,
    cluster_connectivity_record_for_probe_observation, select_cluster_connectivity_probe_batch,
};
#[cfg(any(test, feature = "test-support"))]
pub use connectivity::{
    cluster_connectivity_probe_key, cluster_connectivity_record_for_hard_failure,
    cluster_connectivity_record_for_identity_mismatch,
    cluster_connectivity_record_for_probe_outcome,
    cluster_connectivity_record_for_startup_probe_failure,
};
pub use connectivity::{
    cluster_connectivity_record_should_refresh, cluster_connectivity_stable_eq,
    validate_cluster_connectivity_records_for_source,
};
pub use connectivity_snapshot::{
    ConnectivityGeneration, ConnectivityObservationSink, ConnectivitySnapshot, ConnectivityTracker,
    ConnectivityView, DirectConnectivityObservation, DirectConnectivityState,
    DirectConnectivityStatus, MAX_CONNECTIVITY_SNAPSHOT_EDGES,
};
pub use connectivity_store::{
    invalidate_local_cluster_connectivity_cache,
    prune_local_cluster_connectivity_records_for_state,
    prune_local_cluster_connectivity_records_for_state_async,
    read_local_cluster_connectivity_records, read_local_cluster_connectivity_records_for_source,
    record_local_cluster_endpoint_command_auth_denied,
    record_local_cluster_endpoint_command_connectivity,
    write_local_cluster_connectivity_record_groups,
    write_local_cluster_connectivity_record_groups_async, write_local_cluster_connectivity_records,
    write_local_cluster_connectivity_records_async,
};
pub use connectivity_store::{
    write_local_cluster_connectivity_record_set, write_local_cluster_connectivity_record_set_async,
};
pub use daemon_auth::{
    DAEMON_AUTH_PROTOCOL, SignedDaemonCommand, cached_daemon_signing_public_key,
    daemon_auth_decode_public_key, daemon_auth_encode_public_key, daemon_auth_signing_payload,
    daemon_rpc_body_sha256, invalidate_cached_daemon_signing_identity,
    sign_daemon_rpc_request_digest_for_local_node, sign_daemon_rpc_request_digest_from_data_dir,
    sign_daemon_rpc_request_for_local_node, sign_daemon_rpc_request_from_data_dir,
    try_sign_daemon_rpc_request_digest_from_cached_data_dir,
};
pub use direct_link::{
    AUTHENTICATED_TRANSPORT_PRELUDE_PREFIX, AuthenticatedDirectStream, AuthenticatedLink,
    AuthenticatedTransportMetadata, BoxedNetworkStream, DirectLinkFuture, DirectStreamCapabilities,
    LinkProvider, MAX_AUTHENTICATED_TRANSPORT_IDENTITY_BYTES,
    MAX_AUTHENTICATED_TRANSPORT_PROOF_BYTES, NetworkDuplexStream,
    authenticated_transport_prelude_prefix_matches, read_authenticated_transport_prelude,
    write_authenticated_transport_prelude,
};
pub use directory::{
    ClusterDirectory, ClusterDirectoryFactory, DirectoryNode, DirectorySnapshot, DirectoryVersion,
    EmptyClusterDirectory, cluster_directory_for, set_cluster_directory_provider,
};
#[cfg(any(test, feature = "test-support"))]
pub use endpoint_advertisement::{
    MAX_PUBLIC_ENDPOINT_REACHABILITY_CACHE_ENTRIES,
    PUBLIC_ENDPOINT_REACHABILITY_NEGATIVE_CACHE_TTL, clear_public_endpoint_reachability_cache,
    public_endpoint_is_reachable_for_local_ips_async,
    public_endpoint_is_reachable_for_local_ips_with_probe_at,
    public_endpoint_is_reachable_with_probe, public_endpoint_is_reachable_with_probe_at,
};
pub use endpoint_advertisement::{
    daemon_advertised_endpoints_from_addresses_with_public_discovery_and_advertise_host_async,
    daemon_advertised_endpoints_from_static_addresses, daemon_lan_endpoint,
    daemon_loopback_endpoint,
};
pub use endpoint_records::apply_cluster_endpoint_advertisement;
pub use endpoint_records::cleanup_cluster_endpoint_duplicates;
pub use endpoint_records::cleanup_member_endpoint_duplicates;
pub use endpoint_records::cluster_endpoint_duplicate_identity_eq;
pub use endpoint_records::cluster_endpoint_module_url_identity;
pub use endpoint_records::upsert_member_endpoint;
pub use endpoint_records::{cluster_is_settled_solo, cluster_state_needs_join_registration};
pub use endpoint_selector::{
    ClusterEndpointInternalSelectionPurpose, ClusterMemberMainDaemonConnectivity,
    cluster_member_main_daemon_connectivity, cluster_member_unreachable_via_connectivity,
    cluster_node_blocks_leader_preference, cluster_node_blocks_leader_preference_async,
};
pub use endpoint_selector::{
    ClusterEndpointSelectionPurpose, ClusterEndpointSelector,
    cluster_endpoint_can_carry_main_daemon_rpc,
};
#[cfg(any(test, feature = "test-support"))]
pub use endpoint_selector::{
    cluster_sync_targets, cluster_sync_targets_with_connectivity,
    prefer_bounded_daemon_command_targets,
};
pub use error::{NetworkDeliveryState, NetworkError, NetworkTimeoutPhase};
pub use fallible_serde::{
    deserialize_bytes, deserialize_bytes_with_operation, deserialize_vec, serialize_bytes,
    serialize_optional_bytes,
};
pub use file_mark::{ClusterFileMark, cluster_file_mark};
pub use io_observer::io_observer as current_io_observer;
pub(crate) use io_observer::io_observer;
pub use io_observer::{IoObserver, set_io_observer};
pub use lan::is_loopback_connect_address;
pub use lan::{
    AsyncMdnsDiscoverySocket, MdnsDiscoveryAdvertisement, MdnsDiscoveryCache, MdnsDiscoveryRecord,
    apply_lan_discovery_advertisements, discover_default_lan_ip_candidates,
    discover_default_lan_ip_candidates_async, format_ip_port,
    lan_discovery_advertisement_for_state,
};
pub use maintenance::{
    CLUSTER_CONNECTIVITY_MOBILE_PROBE_MAX_ENDPOINTS_PER_TICK,
    CLUSTER_CONNECTIVITY_PROBE_MAX_ENDPOINTS_PER_TICK, CLUSTER_CONNECTIVITY_REFRESH_MARGIN_SECONDS,
    CLUSTER_CONNECTIVITY_SENSOR_PROBE_MAX_ENDPOINTS_PER_TICK,
    CLUSTER_NETWORK_CONVERGENCE_TICK_INTERVAL, CLUSTER_NETWORK_DISCOVERY_INTERVAL,
    CLUSTER_NETWORK_MOBILE_PROBE_INTERVAL, CLUSTER_NETWORK_MOBILE_PUBLISH_INTERVAL,
    CLUSTER_NETWORK_PROBE_INTERVAL, CLUSTER_NETWORK_PUBLISH_INTERVAL,
    CLUSTER_NETWORK_SENSOR_PROBE_INTERVAL, CLUSTER_NETWORK_SENSOR_PUBLISH_INTERVAL,
    CLUSTER_NETWORK_STABLE_DISCOVERY_ANNOUNCE_TICK_STRIDE,
    CLUSTER_NETWORK_STABLE_REFRESH_MARGIN_SECONDS, ClusterNetworkMaintenanceDue,
    ClusterNetworkMaintenanceSchedule, cluster_connectivity_probe_max_endpoints_per_tick_for_role,
    cluster_local_node_role, cluster_network_discovery_interval_for_role,
    cluster_network_maintenance_due, cluster_network_maintenance_interval_due,
    cluster_network_maintenance_last_timestamp_for_next_due,
    cluster_network_maintenance_next_due_duration, cluster_network_module_republish_sleep_duration,
    cluster_network_module_start_retry_sleep_duration, cluster_network_probe_interval_for_role,
    cluster_network_publish_interval_for_role, cluster_network_stable_discovery_announcement_due,
};
pub use network_info::cluster_network_info_stable_eq;
pub use network_info::cluster_network_record_should_refresh;
pub use network_info::{
    CLUSTER_NETWORK_INFO_TTL_SECONDS, ClusterNetworkInfoRecord, build_cluster_network_info_record,
    cluster_network_info_publish_record_matches,
};
pub use network_module::{
    NETWORK_MODULES_ADMIN_CAPABILITY, NETWORK_MODULES_LIST_ROUTE, NETWORK_MODULES_RPC_CATEGORY,
    NETWORK_MODULES_RPC_ROUTE_POLICIES, NETWORK_MODULES_RPC_ROUTES,
    NETWORK_MODULES_SET_ENABLED_ROUTE, NetworkFuture, NetworkModule, NetworkModuleContext,
    NetworkModuleDescriptor, NetworkModuleEndpointRole, NetworkModuleListResponse,
    NetworkModuleRegistry, NetworkModuleRoutePolicy, NetworkModuleSetEnabledRequest,
    NetworkModuleSetEnabledResponse, NetworkModuleStatus, RegisteredNetworkModuleLinkProvider,
    authenticated_transport_metadata_matches_endpoint, network_module_registry,
    network_modules_rpc_route_policy, set_network_module_registry,
};
pub use network_module_abi::FfiNetworkModule;
pub use object::try_clone_id;
pub use object::{try_reserve_vec, try_zeroed_vec};
pub use object_file_store::remove_optional_file;
pub use object_file_store::{
    read_object_file, read_optional_object_file, write_object_file,
    write_object_file_with_semantic_skip,
};
pub use path_metrics::{
    NetworkPathEstimate, NetworkPathEstimator, NetworkPathKey, shared_network_path_estimator,
};
pub use route::cluster_healthy_route_next_hop;
pub use route::cluster_route_next_hop;
pub use route::invalidate_cluster_delivery_routing_table_cache;
pub use route_graph_read_model::invalidate_cluster_route_graph_cache;
pub use route_graph_read_model::{
    read_cluster_connectivity_graph, read_cluster_connectivity_graph_with_query,
};
pub use routed_identity::DaemonRoutedSessionIdentity;
pub use routing::{
    CandidatePath, ConnectOptions, DirectRoutedConnector, MAX_ROUTE_CANDIDATES, PeerStream,
    RouteConstraints, RouteHop, RouteTrace, RoutedAvailability, RoutedConnectFuture,
    RoutedConnector, RoutedSessionIdentity, accept_direct_routed_session,
    connect_direct_routed_endpoint, plan_routes,
};
pub use rpc::{
    CLUSTER_HEALTH_INCLUDE_PEER_VIEWS_METADATA_KEY, CLUSTER_HEALTH_SCOPE_LOCAL,
    CLUSTER_HEALTH_SCOPE_METADATA_KEY,
};
pub use transfer::{NetworkCallPolicy, NetworkHealthPolicy};
// Unconditional since stage C: dylib network modules reuse the object frame
// codec through the sinomo-core facade.
pub use object::{
    AsyncDaemonControlListener, AsyncDaemonControlStream, DEFAULT_NETWORK_TIMEOUT,
    DaemonControlRequest, DaemonControlResponse, DaemonRpcTimeouts,
    MAX_OBJECT_RPC_CHANNEL_WIRE_FRAME_BYTES, ObjectFrameIoOptions, ObjectFrameReadTiming,
    ObjectFrameReadinessProbe, ObjectFrameWriteTiming, SinomoObjectCodec,
    write_borrowed_object_frame_async,
};
pub use object::{
    read_object_frame_async, read_object_frame_async_with_max_bytes,
    read_object_frame_async_with_max_bytes_after_first_byte,
    read_object_frame_async_with_max_bytes_after_first_byte_timed,
};
pub use object::{write_object_frame_async, write_object_frame_async_timed};
// Unconditional since stage C: see read_object_frame_async above.
pub use object::{decode_single_object_frame_from_slice, encode_object_frame_to_vec};
/// Pre-production Stage A object-RPC contract and conformance entry points.
/// Production delivery must not call these until the matching legacy carrier
/// is deleted in the same bounded migration slice.
#[doc(hidden)]
pub mod __object_rpc_stream_contract {
    pub use super::object_rpc_stream::{
        OBJECT_RPC_STREAM_MAX_HEAD_BYTES, ObjectRpcStreamCall, ObjectRpcStreamReply,
        RpcDeliveryOutcome, RpcPeerContext, RpcPeerIdentity, call_object_rpc_stream,
        serve_object_rpc_stream,
    };
}
pub use paths::ClusterPaths;
pub use peer::{
    AsyncInProcessNetwork, AsyncInProcessTransport, AsyncNetworkSendOptions, InProcessNetwork,
    InProcessTransport,
};
pub use roaming::open_daemon_rpc_stream_to_cluster_endpoint_async;
pub use route::{
    CLUSTER_RELAY_ORIGIN_NODE_ID_METADATA_KEY, CLUSTER_RELAY_ROUTE, CLUSTER_ROUTING_RPC_CATEGORY,
    CLUSTER_ROUTING_RPC_ROUTE_METADATA, ClusterObjectRpcClient, ClusterObjectRpcOptions,
    ClusterRelaySessionRequestAction, ClusterSignedObjectRpcAsyncHost,
    ClusterSignedObjectRpcFuture, ClusterSignedObjectRpcObservation, RelayEnvelope,
    cluster_relay_session_request_action, relay_request_inner_route,
    relay_response_is_routing_error,
};
pub use rpc::{
    CLUSTER_HEALTH_ROUTE, CLUSTER_HEALTH_RPC_CATEGORY, CLUSTER_HEALTH_RPC_ROUTE_METADATA,
    CLUSTER_INVITE_PUBLISH_ROUTE, CLUSTER_JOIN_CHALLENGE_ROUTE, CLUSTER_JOIN_REGISTER_ROUTE,
    CLUSTER_MEMBER_PUBLISH_ROUTE, CLUSTER_MEMBERSHIP_RPC_CATEGORY,
    CLUSTER_MEMBERSHIP_RPC_ROUTE_METADATA, CLUSTER_NETWORK_CONNECTIVITY_ROUTE,
    CLUSTER_NETWORK_PING_ROUTE, CLUSTER_NETWORK_RPC_CATEGORY, CLUSTER_NETWORK_RPC_ROUTE_METADATA,
    OBJECT_RPC_ERROR_DETAIL_METADATA_KEY, OBJECT_RPC_OBSERVE_SAMPLED_METADATA_KEY,
    OBJECT_RPC_TRACEPARENT_METADATA_KEY, OBJECT_RPC_TRACESTATE_METADATA_KEY, ObjectRouteDescriptor,
    ObjectRpcAuthorizationContext, ObjectRpcAuthorizationLayer, ObjectRpcBodyMode,
    ObjectRpcCancelRequest, ObjectRpcCancelResponse, ObjectRpcCategoryDefinition, ObjectRpcRequest,
    ObjectRpcResponse, ObjectRpcRouteCatalog, ObjectRpcRoutePolicyLayer, ObjectRpcRouteReadiness,
    ObjectRpcRouteRegistry, ObjectRpcRouteRegistryBuilder, ObjectRpcRouter, ObjectRpcSessionFuture,
    ServiceCatalog, ServiceCatalogSnapshot, ServiceCategory, call_object_rpc_session_service_async,
};
pub use rpc_future::ClusterMembershipRpcFuture;
pub use session::{
    ObjectRpcBodySource, ObjectRpcIncomingBody, ObjectRpcSessionCall, ObjectRpcSessionRequest,
    ObjectRpcSessionResponder, ObjectRpcSessionResponse, ObjectRpcSessionResult,
    accept_object_rpc_server_session, object_rpc_session_wire_prefix_matches,
};
#[cfg(any(test, feature = "test-support"))]
pub use sinomo_node::upsert_member_record;
pub use tcp_tunnel::{
    CLUSTER_TCP_TUNNEL_ENDPOINT_ADDRESS_PREFIX, CLUSTER_TCP_TUNNEL_HANDSHAKE_PROTOCOL,
    CLUSTER_TCP_TUNNEL_OPEN_ROUTE, CLUSTER_TCP_TUNNEL_PROTOCOL_VERSION,
    CLUSTER_TCP_TUNNEL_RPC_CATEGORY, ClusterTcpTunnelFrame, ClusterTcpTunnelHandshakeProof,
    ClusterTcpTunnelHandshakeTranscript, ClusterTcpTunnelOpenRequest, ClusterTcpTunnelOpenResponse,
    negotiate_cluster_tcp_tunnel_protocol, sign_cluster_tcp_tunnel_handshake_from_data_dir,
    verify_cluster_tcp_tunnel_handshake,
};
pub use timeline::{
    NETWORK_TIMELINE_DEFAULT_MAX_EVENTS, NETWORK_TIMELINE_EVENT_BYTES,
    NETWORK_TIMELINE_MAX_CAPTURE_BYTES, NETWORK_TIMELINE_MAX_ENDPOINT_IDENTITIES,
    NetworkTimelineCaptureError, NetworkTimelineEndpointIdentity, NetworkTimelineEvent,
    NetworkTimelineSnapshot, NetworkTimelineTraceContext, NetworkTimelineTraceparentError,
    network_timeline_capture_is_active, network_timeline_channel_lookup_outcome,
    network_timeline_flag, network_timeline_monotonic_ns, network_timeline_stage,
    network_timeline_stage_name, network_timeline_trace_context_for_request,
    parse_network_timeline_traceparent, record_network_timeline_event,
    record_network_timeline_event_now, register_network_timeline_endpoint,
    snapshot_network_timeline_capture, start_network_timeline_capture,
    stop_network_timeline_capture,
};

pub type ClusterEndpointId = sinomo_base::Id;

pub const NETWORK_PROTOCOL: &str = "sinomo.network.v1";
pub const SINOMO_NETWORK_MODULE_ID: &str = "module.network";
pub const SINOMO_NETWORK_MODULE_OWNER: &str = "sinomo-network";
pub const SINOMO_NETWORK_NATIVE_REGISTRATION_KEY: &str = "sinomo-network";
pub const NETWORK_OBJECT_RPC_ROUTES: &[&str] = &[
    CLUSTER_HEALTH_ROUTE,
    CLUSTER_NETWORK_CONNECTIVITY_ROUTE,
    CLUSTER_NETWORK_PING_ROUTE,
];
pub const CLUSTER_HEALTH_READ_CAPABILITY: &str = "cluster.health.read";
pub const CLUSTER_NETWORK_CONNECTIVITY_READ_CAPABILITY: &str = "cluster.network.connectivity.read";
pub const CLUSTER_NETWORK_PING_CAPABILITY: &str = "cluster.network.ping";
pub const NETWORK_CAPABILITY_IMPORTS: &[&str] = &[
    "storage.local.read",
    "storage.local.write",
    "cluster.kv.read",
];
pub const NETWORK_CAPABILITY_EXPORTS: &[&str] = &[
    CLUSTER_HEALTH_READ_CAPABILITY,
    CLUSTER_NETWORK_CONNECTIVITY_READ_CAPABILITY,
    CLUSTER_NETWORK_PING_CAPABILITY,
];
pub const CLUSTER_NETWORK_INSPECTION_SKILL_ID: &str = "skill.cluster-network.inspect";
pub const CLUSTER_NETWORK_INSPECTION_SKILL_PATH: &str = "skills/cluster-network/SKILL.md";
pub const CLUSTER_NETWORK_INSPECTION_PROMPT_ID: &str = "prompt.cluster-network.inspect";
pub const CLUSTER_NETWORK_INSPECTION_PROMPT_PATH: &str = "prompts/cluster-network-inspection.md";

pub const CLUSTER_NETWORK_INSPECTION_SKILL_MARKDOWN: &str = r#"# Cluster Network Inspection

Use the `module_rpc_cluster_health_cluster_health_read` module tool with the
`cluster.health.read` capability before reasoning about live cluster
connectivity, membership reachability, peer transport, or rollout availability.

Treat the cluster health rows as point-in-time evidence. Compare node id,
availability, role, daemon endpoint, health status, and last observation fields
before recommending operator action. Do not infer membership changes from a
single stale row.

Prefer read-only inspection. Do not use relay, membership mutation, rollout, or
storage repair tools unless the user explicitly asks for remediation and the
current health evidence supports that action.
"#;

pub const CLUSTER_NETWORK_INSPECTION_PROMPT_MARKDOWN: &str = r#"# Cluster Network Inspection Prompt

Inspect the current Sinomo cluster network state with the native network module
health route. Summarize reachable and unreachable nodes, stale observations,
route or endpoint symptoms, and the narrowest next diagnostic command or tool.
"#;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NetworkModuleRouteAuth {
    LocalCommand,
    LocalOrClusterMember,
    ClusterMember,
    Admin,
}

impl NetworkModuleRouteAuth {
    pub fn as_scope(self) -> &'static str {
        match self {
            Self::LocalCommand => "local-command",
            Self::LocalOrClusterMember => "local-or-cluster-member",
            Self::ClusterMember => "cluster-member",
            Self::Admin => "admin",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NetworkObjectRpcToolSurface {
    pub route: &'static str,
    pub category: &'static str,
    pub operation: &'static str,
    pub auth: NetworkModuleRouteAuth,
    pub request_body_mode: ObjectRpcBodyMode,
    pub response_body_mode: ObjectRpcBodyMode,
}

pub const NETWORK_OBJECT_RPC_TOOL_SURFACES: &[NetworkObjectRpcToolSurface] = &[
    NetworkObjectRpcToolSurface {
        route: CLUSTER_HEALTH_ROUTE,
        category: CLUSTER_HEALTH_RPC_CATEGORY,
        operation: CLUSTER_HEALTH_READ_CAPABILITY,
        auth: NetworkModuleRouteAuth::LocalOrClusterMember,
        request_body_mode: ObjectRpcBodyMode::None,
        response_body_mode: ObjectRpcBodyMode::None,
    },
    NetworkObjectRpcToolSurface {
        route: CLUSTER_NETWORK_CONNECTIVITY_ROUTE,
        category: CLUSTER_NETWORK_RPC_CATEGORY,
        operation: CLUSTER_NETWORK_CONNECTIVITY_READ_CAPABILITY,
        auth: NetworkModuleRouteAuth::LocalOrClusterMember,
        request_body_mode: ObjectRpcBodyMode::None,
        response_body_mode: ObjectRpcBodyMode::None,
    },
    NetworkObjectRpcToolSurface {
        route: CLUSTER_NETWORK_PING_ROUTE,
        category: CLUSTER_NETWORK_RPC_CATEGORY,
        operation: CLUSTER_NETWORK_PING_CAPABILITY,
        auth: NetworkModuleRouteAuth::LocalOrClusterMember,
        request_body_mode: ObjectRpcBodyMode::Optional,
        response_body_mode: ObjectRpcBodyMode::Optional,
    },
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NetworkDeclarativeResourceKind {
    Skill,
    Prompt,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NetworkDeclarativeResourceSurface {
    pub id: &'static str,
    pub kind: NetworkDeclarativeResourceKind,
    pub path: &'static str,
    pub description: &'static str,
    pub model_families: &'static [&'static str],
    pub activation_tags: &'static [&'static str],
    pub required_capabilities: &'static [&'static str],
}

pub const NETWORK_DECLARATIVE_RESOURCE_SURFACES: &[NetworkDeclarativeResourceSurface] = &[
    NetworkDeclarativeResourceSurface {
        id: CLUSTER_NETWORK_INSPECTION_SKILL_ID,
        kind: NetworkDeclarativeResourceKind::Skill,
        path: CLUSTER_NETWORK_INSPECTION_SKILL_PATH,
        description: "Guide agents through read-only Sinomo cluster network inspection.",
        model_families: &["gpt"],
        activation_tags: &["cluster", "network"],
        required_capabilities: &[CLUSTER_HEALTH_READ_CAPABILITY],
    },
    NetworkDeclarativeResourceSurface {
        id: CLUSTER_NETWORK_INSPECTION_PROMPT_ID,
        kind: NetworkDeclarativeResourceKind::Prompt,
        path: CLUSTER_NETWORK_INSPECTION_PROMPT_PATH,
        description: "Prompt fragment for summarizing cluster network health evidence.",
        model_families: &["gpt"],
        activation_tags: &["cluster", "network"],
        required_capabilities: &[CLUSTER_HEALTH_READ_CAPABILITY],
    },
];

pub fn register_storage_paths(
    layout: &mut sinomo_storage::StorageLayout,
) -> Result<(), sinomo_storage::StorageContractError> {
    use sinomo_storage::{ObjectSchema, STORAGE_CONTRACT_OBJECT_VERSION, StorageRing};

    layout.register_sinomo_object(
        "sinomo-network",
        StorageRing::R1,
        "cluster/connectivity-local.sobj",
        ObjectSchema::new(
            "Vec<ClusterConnectivityRecord>",
            STORAGE_CONTRACT_OBJECT_VERSION,
        )
        .field("source_node_id", "NodeId")
        .field("target_node_id", "NodeId")
        .field("endpoint_id", "ClusterEndpointId"),
    )?;
    layout.register_binary(
        "sinomo-network",
        StorageRing::R1,
        "cluster/connectivity-local.sobj.lock",
    )?;
    layout.register_escape_path(
        "sinomo-network",
        StorageRing::R1,
        "cluster/iroh",
        "dynamic Iroh endpoint, identity, and lock records",
    )?;
    Ok(())
}

pub(crate) mod prelude {
    pub(crate) use crate::ClusterEndpointId;
    pub(crate) use serde::{Deserialize, Serialize};
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) use sinomo_base::ClusterCertificate;
    pub(crate) use sinomo_base::{
        ClusterBootstrapState, ClusterEndpointPrivacy, ClusterEndpointRecord,
        ClusterEndpointSource, ClusterMemberRecord, ClusterMemberRole, ClusterNodeRole,
        ClusterTransportKind,
    };
    pub(crate) use sinomo_events::EventStoreError;
    #[cfg(test)]
    pub(crate) use sinomo_local::SinomoDataDir;
    pub(crate) use sinomo_node::ClusterId;
    pub(crate) use sinomo_node::NodeId;
    #[cfg(test)]
    pub(crate) use sinomo_node::main_daemon_capabilities;
    pub(crate) use std::collections::{BTreeMap, BTreeSet, VecDeque};
    pub(crate) use std::io;
    pub(crate) use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4, UdpSocket};
    pub(crate) use std::sync::{Arc, Mutex};
    pub(crate) use std::time::Duration;
    pub(crate) use thiserror::Error;
    pub(crate) use time::OffsetDateTime;
}

#[cfg(test)]
mod storage_contract_tests {
    use super::register_storage_paths;
    use sinomo_storage::StorageLayout;

    #[test]
    fn network_storage_contract_covers_runtime_locks_and_dynamic_iroh_records() {
        let mut layout = StorageLayout::new();
        register_storage_paths(&mut layout).expect("register network storage paths");

        for path in [
            "cluster/connectivity-local.sobj.lock",
            "cluster/iroh/node.endpoint.sobj",
            "cluster/iroh/node.secret.sobj",
            "cluster/iroh/node.secret.lock",
        ] {
            assert!(
                layout.owner_for(path).is_some(),
                "storage contract should cover {path}"
            );
        }
    }
}

// test-support re-exports (crate internals for sibling integration tests)
#[cfg(any(test, feature = "test-support"))]
pub use connectivity_store::local_cluster_connectivity_content_generation;
#[cfg(any(test, feature = "test-support"))]
pub use connectivity_store::local_cluster_connectivity_prune_passes_for_test;
#[cfg(any(test, feature = "test-support"))]
pub use connectivity_store::local_cluster_connectivity_record_cache_capacity_for_test;
#[cfg(any(test, feature = "test-support"))]
pub use connectivity_store::local_cluster_connectivity_record_cache_contains_for_test;
#[cfg(any(test, feature = "test-support"))]
pub use connectivity_store::remove_local_cluster_connectivity_record_cache_entries_under_root_for_test;
#[cfg(any(test, feature = "test-support"))]
pub use connectivity_store::reset_local_cluster_connectivity_prune_passes_for_test;
#[cfg(any(test, feature = "test-support"))]
pub use lan::MAX_MDNS_DISCOVERY_RECORDS;
#[cfg(any(test, feature = "test-support"))]
pub use lan::MAX_PUBLIC_IP_DISCOVERY_CACHE_ENTRIES;
#[cfg(any(test, feature = "test-support"))]
pub use lan::MAX_SINOMO_LAN_DISCOVERY_TTL_TICKS;
#[cfg(any(test, feature = "test-support"))]
pub use lan::PUBLIC_IP_DISCOVERY_DEFERRED_CACHE_TTL;
#[cfg(any(test, feature = "test-support"))]
pub use lan::PUBLIC_IP_DISCOVERY_EMPTY_CACHE_TTL;
#[cfg(any(test, feature = "test-support"))]
pub use lan::PublicIpDiscoveryResult;
#[cfg(any(test, feature = "test-support"))]
pub use lan::age_public_ip_discovery_cache_for_test;
#[cfg(any(test, feature = "test-support"))]
pub use lan::cached_default_lan_ip_candidates_with;
#[cfg(any(test, feature = "test-support"))]
pub use lan::cached_default_lan_ip_candidates_with_async;
#[cfg(any(test, feature = "test-support"))]
pub use lan::cached_default_public_ip_candidates_for_local_ips_with;
#[cfg(any(test, feature = "test-support"))]
pub use lan::cached_default_public_ip_candidates_for_local_ips_with_async;
#[cfg(any(test, feature = "test-support"))]
pub use lan::clear_default_lan_ip_discovery_cache;
#[cfg(any(test, feature = "test-support"))]
pub use lan::clear_public_ip_discovery_cache;
#[cfg(any(test, feature = "test-support"))]
pub use lan::decode_lan_discovery_datagram;
#[cfg(any(test, feature = "test-support"))]
pub use lan::discover_local_ip_candidates_for_targets_async;
#[cfg(any(test, feature = "test-support"))]
pub use lan::fetch_public_ip_candidates_from_service_async;
#[cfg(any(test, feature = "test-support"))]
pub use lan::is_advertisable_interface_candidate;
#[cfg(any(test, feature = "test-support"))]
pub use lan::is_advertisable_interface_ip;
#[cfg(any(test, feature = "test-support"))]
pub use lan::is_invitable_ip;
#[cfg(any(test, feature = "test-support"))]
pub use lan::parse_public_ip_discovery_response;
#[cfg(any(test, feature = "test-support"))]
pub use object::DaemonControlStream;
#[cfg(any(test, feature = "test-support"))]
pub use object::read_object_frame_blocking;
#[cfg(any(test, feature = "test-support"))]
pub use object::write_object_frame_blocking;
#[cfg(any(test, feature = "test-support"))]
pub use object_file_store::clear_write_object_file_create_new_unsupported_link_for_test;
#[cfg(any(test, feature = "test-support"))]
pub use object_file_store::force_write_object_file_create_new_unsupported_link_for_test;
#[cfg(any(test, feature = "test-support"))]
pub use object_file_store::object_file_write_mark_cache_capacity_for_test;
#[cfg(any(test, feature = "test-support"))]
pub use object_file_store::object_file_write_mark_cache_contains_for_test;
#[cfg(any(test, feature = "test-support"))]
pub use object_file_store::record_object_file_write_mark_for_test;
#[cfg(any(test, feature = "test-support"))]
pub use object_file_store::refresh_object_file_write_mark_for_test;
#[cfg(any(test, feature = "test-support"))]
pub use object_file_store::remove_object_file_write_mark_entries_under_root_for_test;
#[cfg(any(test, feature = "test-support"))]
pub use object_file_store::write_object_file_compare_read_bytes_for_test;
pub use object_file_store::write_object_file_create_new_serialized;
#[cfg(any(test, feature = "test-support"))]
pub use peer::IN_PROCESS_MAILBOX_CAPACITY;
#[cfg(any(test, feature = "test-support"))]
pub use peer::PeerEndpointCandidate;
#[cfg(any(test, feature = "test-support"))]
pub use roaming::ClusterAuthMaterial;
#[cfg(any(test, feature = "test-support"))]
pub use roaming::ClusterAuthVerifier;
#[cfg(any(test, feature = "test-support"))]
pub use roaming::IceCandidateRecord;
#[cfg(any(test, feature = "test-support"))]
pub use roaming::IceCandidateType;
#[cfg(any(test, feature = "test-support"))]
pub use roaming::LanConnectionPlanner;
#[cfg(any(test, feature = "test-support"))]
pub use roaming::RoamingConnectionPlanner;
#[cfg(any(test, feature = "test-support"))]
pub use roaming::RoamingPathSelection;
#[cfg(any(test, feature = "test-support"))]
pub use roaming::WebRtcKvExchange;
#[cfg(any(test, feature = "test-support"))]
pub use roaming::WebRtcSignalMessage;
#[cfg(any(test, feature = "test-support"))]
pub use roaming::WebRtcSignalingEnvelope;
#[cfg(any(test, feature = "test-support"))]
pub use roaming::configure_daemon_control_stream_idle_timeout;
#[cfg(any(test, feature = "test-support"))]
#[cfg(any(test, feature = "test-support"))]
pub use route::ClusterDeliveryFailedEdges;
#[cfg(any(test, feature = "test-support"))]
pub use route::ClusterDeliveryRouteGraph;
#[cfg(any(test, feature = "test-support"))]
pub use route::cluster_delivery_build_routing_table;
#[cfg(any(test, feature = "test-support"))]
pub use route::cluster_delivery_route_plan;
#[cfg(any(test, feature = "test-support"))]
pub use route::cluster_delivery_route_plan_async;
#[cfg(any(test, feature = "test-support"))]
pub use route::cluster_delivery_routing_table_cache_mark;
#[cfg(any(test, feature = "test-support"))]
pub use route::cluster_delivery_shortest_path_next_hop;
#[cfg(any(test, feature = "test-support"))]
pub use route_graph_read_model::cluster_route_graph_cache_mark;
