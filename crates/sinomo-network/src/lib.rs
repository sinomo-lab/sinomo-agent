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

mod connectivity;
mod connectivity_store;
mod daemon_auth;
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
mod paths;
mod peer;
mod roaming;
mod route;
mod route_graph_read_model;
mod rpc;
mod rpc_future;
mod tcp_tunnel;

pub use connectivity::cluster_network_record_live;
pub use connectivity::{
    CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD, CLUSTER_CONNECTIVITY_TTL_SECONDS,
    CLUSTER_CONNECTIVITY_UNREACHABLE_TTL_SECONDS, ClusterConnectivityFailureReason,
    ClusterConnectivityGraph, ClusterConnectivityProbeBatch, ClusterConnectivityProbeBatchEntry,
    ClusterConnectivityProbeObservation, ClusterConnectivityProbeState, ClusterConnectivityRecord,
    ClusterConnectivityRecordQuery, ClusterConnectivityState,
    MAX_CLUSTER_CONNECTIVITY_SOURCE_RECORDS, apply_cluster_connectivity_probe_results,
    cluster_connectivity_probe_due_with_graph, cluster_connectivity_probe_due_with_startup_settle,
    cluster_connectivity_record, cluster_connectivity_record_for_probe_observation,
    select_cluster_connectivity_probe_batch,
};
#[cfg(any(test, feature = "test-support"))]
pub use connectivity::{
    cluster_connectivity_probe_key, cluster_connectivity_record_for_hard_failure,
    cluster_connectivity_record_for_identity_mismatch,
    cluster_connectivity_record_for_probe_outcome,
    cluster_connectivity_record_for_startup_probe_failure,
};
pub use connectivity::{
    cluster_connectivity_stable_eq, validate_cluster_connectivity_records_for_source,
};
pub use connectivity_store::{
    read_local_cluster_connectivity_records, read_local_cluster_connectivity_records_for_source,
    record_local_cluster_endpoint_command_connectivity, write_local_cluster_connectivity_records,
};
pub use connectivity_store::{
    write_local_cluster_connectivity_record_set, write_local_cluster_connectivity_record_set_async,
};
pub use daemon_auth::{
    DAEMON_AUTH_PROTOCOL, SignedDaemonCommand, daemon_auth_decode_public_key,
    daemon_auth_encode_public_key, daemon_auth_signing_payload, daemon_rpc_body_sha256,
    sign_daemon_rpc_request_digest_for_local_node, sign_daemon_rpc_request_digest_from_data_dir,
    sign_daemon_rpc_request_for_local_node, sign_daemon_rpc_request_from_data_dir,
};
pub use directory::{
    ClusterDirectory, ClusterDirectoryFactory, DirectoryVersion, EmptyClusterDirectory,
    cluster_directory_for, set_cluster_directory_provider,
};
#[cfg(any(test, feature = "test-support"))]
pub use endpoint_advertisement::{
    MAX_PUBLIC_ENDPOINT_REACHABILITY_CACHE_ENTRIES,
    PUBLIC_ENDPOINT_REACHABILITY_NEGATIVE_CACHE_TTL, clear_public_endpoint_reachability_cache,
    daemon_loopback_endpoint, public_endpoint_is_reachable_for_local_ips_async,
    public_endpoint_is_reachable_for_local_ips_with_probe_at,
    public_endpoint_is_reachable_with_probe, public_endpoint_is_reachable_with_probe_at,
};
pub use endpoint_advertisement::{
    daemon_advertised_endpoints_from_addresses_with_public_discovery_and_advertise_host_async,
    daemon_advertised_endpoints_from_static_addresses, daemon_lan_endpoint,
};
pub use endpoint_records::apply_cluster_endpoint_advertisement;
pub use endpoint_records::cleanup_cluster_endpoint_duplicates;
pub use endpoint_records::cleanup_member_endpoint_duplicates;
pub use endpoint_records::upsert_member_endpoint;
pub use endpoint_records::{cluster_is_settled_solo, cluster_state_needs_join_registration};
pub use endpoint_selector::{
    ClusterEndpointInternalSelectionPurpose, cluster_node_blocks_leader_preference,
    cluster_node_blocks_leader_preference_async,
};
pub use endpoint_selector::{
    ClusterEndpointSelectionPurpose, ClusterEndpointSelector, ClusterEndpointTransferProfile,
    cluster_endpoint_can_carry_main_daemon_rpc,
};
#[cfg(any(test, feature = "test-support"))]
pub use endpoint_selector::{
    cluster_sync_targets, cluster_sync_targets_with_connectivity,
    prefer_bounded_daemon_command_targets,
};
pub use error::NetworkError;
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
    NetworkModuleSetEnabledResponse, NetworkModuleStatus, network_module_registry,
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
pub use route::cluster_healthy_route_next_hop;
pub use route::cluster_route_next_hop;
pub use route::invalidate_cluster_delivery_routing_table_cache;
pub use route_graph_read_model::invalidate_cluster_route_graph_cache;
pub use route_graph_read_model::{
    read_cluster_connectivity_graph, read_cluster_connectivity_graph_with_query,
};
pub use rpc::{
    CLUSTER_HEALTH_INCLUDE_PEER_VIEWS_METADATA_KEY, CLUSTER_HEALTH_SCOPE_LOCAL,
    CLUSTER_HEALTH_SCOPE_METADATA_KEY,
};
// Unconditional since stage C: dylib network modules reuse the object frame
// codec through the sinomo-core facade.
pub use object::read_object_frame_async;
pub use object::write_object_frame_async;
pub use object::{
    AsyncDaemonControlListener, AsyncDaemonControlStream, DAEMON_CONTROL_RPC_MAX_FRAME_BYTES,
    DaemonControlRequest, DaemonControlResponse, DaemonRpcTimeouts, OBJECT_STREAM_MAX_CHUNK_BYTES,
    ObjectFrameIoOptions, ObjectRpcRequestStreamMode, ObjectRpcResponseStreamMode,
    ObjectRpcStreamBody, ObjectRpcStreamResponse, RawByteStreamChunkOptions, SinomoObjectCodec,
    forward_raw_byte_stream_chunks_async, object_rpc_request_stream_mode,
    object_rpc_response_stream_mode, read_raw_byte_stream_chunks_to_vec_async,
    send_daemon_control_rpc_request_async, send_daemon_control_rpc_request_stream_response_async,
    set_object_rpc_request_stream_mode, set_object_rpc_response_stream_mode,
    write_borrowed_object_frame_async,
};
pub(crate) use object::{
    DAEMON_CONTROL_RAW_RPC_RESPONSE_METADATA_KEY, DAEMON_CONTROL_RAW_RPC_RESPONSE_METADATA_VALUE,
    DAEMON_CONTROL_STREAMED_RPC_RESPONSE_METADATA_KEY,
    DAEMON_CONTROL_STREAMED_RPC_RESPONSE_METADATA_VALUE,
    object_rpc_request_accepts_streamed_daemon_response,
    object_rpc_request_expects_raw_daemon_response, send_daemon_control_request_async,
    send_daemon_control_rpc_request_raw_response_async,
};
// Unconditional since stage C: see read_object_frame_async above.
pub use object::{
    OBJECT_FRAME_EXTENDED_LENGTH_SENTINEL, decode_single_object_frame_from_slice,
    encode_object_frame_to_vec,
};
pub use paths::ClusterPaths;
pub use peer::{
    AsyncInProcessNetwork, AsyncInProcessTransport, AsyncNetworkSendOptions, InProcessNetwork,
    InProcessTransport,
};
pub use roaming::{
    send_daemon_rpc_stream_response_to_cluster_endpoint_async,
    send_daemon_rpc_to_cluster_endpoint_async,
};
pub use route::{
    CLUSTER_RELAY_ROUTE, CLUSTER_ROUTING_RPC_CATEGORY, CLUSTER_ROUTING_RPC_ROUTE_METADATA,
    ClusterObjectRpcClient, ClusterObjectRpcOptions, ClusterRelayAsyncHost, ClusterRelayOptions,
    ClusterRelayStreamForwardAsyncHost, ClusterRelayStreamForwardOutcome,
    ClusterRelayStreamRequestAction, ClusterRelayStreamSendOutcome,
    ClusterSignedEndpointRpcAsyncHost, ClusterSignedEndpointRpcFuture,
    ClusterSignedEndpointRpcObservation, RelayEnvelope, cluster_relay_stream_request_action,
    forward_signed_streamed_daemon_rpc_to_cluster_endpoint_with_host_async,
    forward_streamed_daemon_rpc_to_cluster_endpoint_async, handle_cluster_relay_async_with_host,
    handle_cluster_relay_stream_forward_async_with_host, relay_request_inner_route,
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
    ObjectRpcAuthorizationContext, ObjectRpcAuthorizationLayer, ObjectRpcCancelRequest,
    ObjectRpcCancelResponse, ObjectRpcCategoryDefinition, ObjectRpcRequest, ObjectRpcResponse,
    ObjectRpcRouteCatalog, ObjectRpcRoutePolicyLayer, ObjectRpcRouteReadiness,
    ObjectRpcRouteRegistry, ObjectRpcRouteRegistryBuilder, ObjectRpcRouter, ServiceCatalog,
    ServiceCatalogSnapshot, ServiceCategory, call_object_rpc_service_async,
};
#[cfg(any(test, feature = "test-support"))]
pub use rpc::{call_object_rpc_frame_async, serve_object_rpc_frame_async};
pub use rpc_future::ClusterMembershipRpcFuture;
#[cfg(any(test, feature = "test-support"))]
pub use sinomo_node::upsert_member_record;
pub use tcp_tunnel::{
    CLUSTER_TCP_TUNNEL_ENDPOINT_ADDRESS_PREFIX, CLUSTER_TCP_TUNNEL_OPEN_ROUTE,
    CLUSTER_TCP_TUNNEL_PROTOCOL_VERSION, CLUSTER_TCP_TUNNEL_RPC_CATEGORY, ClusterTcpTunnelFrame,
    ClusterTcpTunnelOpenRequest, ClusterTcpTunnelOpenResponse,
};

pub type ClusterEndpointId = sinomo_base::Id;

pub const NETWORK_PROTOCOL: &str = "sinomo.network.v1";
pub const SINOMO_NETWORK_MODULE_LEGACY_ID: &str = "module.network";
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
}

pub const NETWORK_OBJECT_RPC_TOOL_SURFACES: &[NetworkObjectRpcToolSurface] = &[
    NetworkObjectRpcToolSurface {
        route: CLUSTER_HEALTH_ROUTE,
        category: CLUSTER_HEALTH_RPC_CATEGORY,
        operation: CLUSTER_HEALTH_READ_CAPABILITY,
        auth: NetworkModuleRouteAuth::LocalOrClusterMember,
    },
    NetworkObjectRpcToolSurface {
        route: CLUSTER_NETWORK_CONNECTIVITY_ROUTE,
        category: CLUSTER_NETWORK_RPC_CATEGORY,
        operation: CLUSTER_NETWORK_CONNECTIVITY_READ_CAPABILITY,
        auth: NetworkModuleRouteAuth::LocalOrClusterMember,
    },
    NetworkObjectRpcToolSurface {
        route: CLUSTER_NETWORK_PING_ROUTE,
        category: CLUSTER_NETWORK_RPC_CATEGORY,
        operation: CLUSTER_NETWORK_PING_CAPABILITY,
        auth: NetworkModuleRouteAuth::LocalOrClusterMember,
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
    Ok(())
}

pub(crate) mod prelude {
    pub(crate) use crate::ClusterEndpointId;
    pub(crate) use serde::{Deserialize, Serialize};
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) use sinomo_base::ClusterCertificate;
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) use sinomo_base::EventEnvelope;
    pub(crate) use sinomo_base::{
        ClusterBootstrapState, ClusterEndpointPrivacy, ClusterEndpointRecord,
        ClusterEndpointSource, ClusterMemberRecord, ClusterMemberRole, ClusterNodeRole,
        ClusterTransportKind,
    };
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) use sinomo_base::{LatencyClass, RoutingScope};
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) use sinomo_events::EventStore;
    pub(crate) use sinomo_events::{EventId, EventStoreError};
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) use sinomo_events::{EventSequence, SessionId, StoredEvent};
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) use sinomo_local::SinomoDataDir;
    pub(crate) use sinomo_node::ClusterId;
    pub(crate) use sinomo_node::NodeId;
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) use sinomo_node::main_daemon_capabilities;
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) use sinomo_node::{NodeAvailability, NodeHealth, NodeProfile};
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) use std::collections::HashSet;
    pub(crate) use std::collections::{BTreeMap, BTreeSet, VecDeque};
    pub(crate) use std::io;
    pub(crate) use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4, UdpSocket};
    pub(crate) use std::sync::{Arc, Mutex};
    pub(crate) use std::time::Duration;
    pub(crate) use thiserror::Error;
    pub(crate) use time::OffsetDateTime;
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
pub use lan::LanQuicEventFrameCodec;
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
pub use peer::AsyncNetworkServiceHandle;
#[cfg(any(test, feature = "test-support"))]
pub use peer::AsyncNetworkTransport;
#[cfg(any(test, feature = "test-support"))]
pub use peer::AsyncNetworkTransportFuture;
#[cfg(any(test, feature = "test-support"))]
pub use peer::DeliveryAck;
#[cfg(any(test, feature = "test-support"))]
pub use peer::DeliveryLedger;
#[cfg(any(test, feature = "test-support"))]
pub use peer::DeliveryStatus;
#[cfg(any(test, feature = "test-support"))]
pub use peer::EventFrame;
#[cfg(any(test, feature = "test-support"))]
pub use peer::EventFrameId;
#[cfg(any(test, feature = "test-support"))]
pub use peer::Heartbeat;
#[cfg(any(test, feature = "test-support"))]
pub use peer::HeartbeatPolicy;
#[cfg(any(test, feature = "test-support"))]
pub use peer::IN_PROCESS_MAILBOX_CAPACITY;
#[cfg(any(test, feature = "test-support"))]
pub use peer::InProcessRouter;
#[cfg(any(test, feature = "test-support"))]
pub use peer::InboundEventFrame;
#[cfg(any(test, feature = "test-support"))]
pub use peer::InboundOutcome;
#[cfg(any(test, feature = "test-support"))]
pub use peer::MeshMessage;
#[cfg(any(test, feature = "test-support"))]
pub use peer::MeshMessageBody;
#[cfg(any(test, feature = "test-support"))]
pub use peer::MeshMessageId;
#[cfg(any(test, feature = "test-support"))]
pub use peer::NETWORK_SERVICE_INBOUND_BATCH_LIMIT;
#[cfg(any(test, feature = "test-support"))]
pub use peer::NETWORK_SERVICE_OUTBOUND_BATCH_LIMIT;
#[cfg(any(test, feature = "test-support"))]
pub use peer::NetworkService;
#[cfg(any(test, feature = "test-support"))]
pub use peer::NetworkServiceActorOptions;
#[cfg(any(test, feature = "test-support"))]
pub use peer::NetworkServiceCommandOptions;
#[cfg(any(test, feature = "test-support"))]
pub use peer::NodeEndpoint;
#[cfg(any(test, feature = "test-support"))]
pub use peer::PeerEndpointCandidate;
#[cfg(any(test, feature = "test-support"))]
pub use peer::PeerRegistry;
#[cfg(any(test, feature = "test-support"))]
pub use peer::PeerStatus;
#[cfg(any(test, feature = "test-support"))]
pub use peer::RetryPolicy;
#[cfg(any(test, feature = "test-support"))]
pub use peer::RouteDelivery;
#[cfg(any(test, feature = "test-support"))]
pub use peer::RouteDeliveryId;
#[cfg(any(test, feature = "test-support"))]
pub use peer::SeenEventCache;
#[cfg(any(test, feature = "test-support"))]
pub use peer::TransportKind;
#[cfg(any(test, feature = "test-support"))]
pub use peer::ack_timeout_for_latency_class;
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
pub use route::CLUSTER_TARGET_ENDPOINT_ATTEMPT_MAX;
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
#[cfg(any(test, feature = "test-support"))]
pub use rpc::ObjectRpcFuture;
