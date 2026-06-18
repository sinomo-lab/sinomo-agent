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

mod endpoint_selector;
mod error;
mod health;
mod invite;
mod lan;
mod object;
mod peer;
mod roaming;
mod route;
mod rpc;
mod storage;

pub use endpoint_selector::{ClusterEndpointSelectionPurpose, ClusterEndpointSelector};
pub use error::*;
pub use health::{
    ClusterDoctorFinding, ClusterHealthRow, ClusterNetworkPingRequest, ClusterNetworkPingResponse,
    ClusterStatusAsyncProbe, ClusterStatusProbe, cluster_doctor_findings,
    cluster_health_rows_with_probe, cluster_health_rpc_request, cluster_local_health_rpc_request,
    cluster_network_ping_rpc_request, cluster_network_ping_rpc_request_with_response_payload,
    decode_cluster_health_rpc_response, decode_cluster_network_ping_rpc_response,
    handle_cluster_network_ping_rpc_request_for_node,
    handle_current_cluster_health_rpc_request_async, unknown_cluster_node_status,
};
pub use invite::{
    ClusterInviteChallengeProof, ClusterInviteChallengeRequest, PENDING_INVITE_CLUSTER_ID,
    SINOMO_JOIN_URI_PREFIX, build_cluster_join_invite, cluster_join_command,
    compact_default_invite_introducers, create_cluster_invite_challenge_proof,
    decode_cluster_join_invite, encode_cluster_join_invite, extract_invite_from_join_command,
    invite_channels_for_join_registration, invite_communication_channels,
    invite_has_embedded_cluster_identity, invite_has_embedded_signature,
    invite_introducer_endpoint, invite_introducer_endpoint_from_state, is_loopback_connect_address,
    is_remote_usable_invite_endpoint, push_unique_introducer, sign_cluster_join_invite,
    sort_default_invite_introducers, verify_cluster_join_invite_signature,
};
pub use lan::{
    AsyncMdnsDiscoverySocket, MdnsDiscoveryAdvertisement, MdnsDiscoveryCache, MdnsDiscoveryRecord,
    apply_lan_discovery_advertisements, discover_default_lan_ip_candidates,
    discover_default_lan_ip_candidates_async, format_ip_port, iroh_disabled_by_env,
    lan_discovery_advertisement_for_state,
};
pub use object::{
    AsyncDaemonControlListener, AsyncDaemonControlStream, DAEMON_CONTROL_RPC_MAX_FRAME_BYTES,
    DaemonControlRequest, DaemonControlResponse, DaemonRpcTimeouts,
    OBJECT_FRAME_EXTENDED_LENGTH_SENTINEL, OBJECT_STREAM_DEFAULT_CHUNK_BYTES,
    OBJECT_STREAM_MAX_CHUNK_BYTES, ObjectFrameIoOptions, ObjectStreamChunk,
    ObjectStreamChunkOptions, RawByteStreamChunkOptions, SINOMO_NETWORK_OBJECT_VERSION,
    SinomoObjectCodec, daemon_control_request_expects_raw_rpc_response,
    decode_single_object_frame_from_slice, encode_object_frame_to_vec,
    forward_raw_byte_stream_chunks_async,
    mark_object_rpc_request_accepts_raw_streamed_daemon_response,
    mark_object_rpc_request_reuses_daemon_stream,
    mark_object_rpc_response_has_streamed_daemon_body,
    object_rpc_request_accepts_streamed_daemon_response,
    object_rpc_request_expects_raw_daemon_response, object_rpc_request_reuses_daemon_stream,
    object_rpc_response_has_streamed_daemon_body, read_object_frame_async,
    read_object_stream_chunks_async, read_object_stream_chunks_to_vec_async,
    read_raw_byte_stream_chunks_to_vec_async, send_daemon_control_request_async,
    send_daemon_control_rpc_request_raw_response_async, write_object_frame_async,
    write_object_stream_chunks_async,
};
pub(crate) use object::{
    DAEMON_CONTROL_RAW_RPC_RESPONSE_METADATA_KEY, DAEMON_CONTROL_RAW_RPC_RESPONSE_METADATA_VALUE,
    DAEMON_CONTROL_STREAMED_RPC_RESPONSE_METADATA_KEY,
    DAEMON_CONTROL_STREAMED_RPC_RESPONSE_METADATA_VALUE, ENDPOINT_RPC_RAW_DIRECT_METADATA_KEY,
    ENDPOINT_RPC_RAW_DIRECT_METADATA_VALUE,
};
pub use peer::{
    AsyncInProcessNetwork, AsyncInProcessTransport, AsyncNetworkSendOptions, InProcessNetwork,
    InProcessObjectRpcMessage, InProcessTransport, InboundObjectRpcMessage,
};
pub(crate) use roaming::read_iroh_endpoint_record_async;
pub use roaming::{
    IROH_DAEMON_COMMAND_MAX_BYTES, IrohDaemonProxyHandle, IrohEndpointRecord, IrohRelayMode,
    read_iroh_endpoint_record, send_daemon_rpc_to_address_async,
    send_daemon_rpc_to_cluster_endpoint_async, stable_iroh_endpoint_id, start_iroh_daemon_proxy,
    write_iroh_endpoint_record,
};
pub use route::{
    CLUSTER_RELAY_ROUTE, CLUSTER_ROUTING_RPC_CATEGORY, CLUSTER_ROUTING_RPC_ROUTE_METADATA,
    ClusterObjectRpcNodeOptions, ClusterObjectRpcOptions, ClusterObjectRpcRouteContext,
    ClusterRelayAsyncHost, ClusterRelayFuture, ClusterRelayOptions,
    ClusterRelayStreamForwardAsyncHost, ClusterRelayStreamForwardOutcome,
    ClusterRelayStreamSendOutcome, ClusterSignedEndpointRpcAsyncHost,
    ClusterSignedEndpointRpcFuture, RelayEnvelope, call_cluster_endpoint_rpc_async,
    call_cluster_node_rpc_async, handle_cluster_relay_async_with_host,
    handle_cluster_relay_stream_forward_async_with_host, relay_response_is_routing_error,
};
pub(crate) use route::{cluster_endpoint_prefers_relay_with_graph, cluster_route_next_hop};
pub use rpc::{
    CLUSTER_HEALTH_ROUTE, CLUSTER_HEALTH_RPC_CATEGORY, CLUSTER_HEALTH_RPC_ROUTE_METADATA,
    CLUSTER_INVITE_PUBLISH_ROUTE, CLUSTER_JOIN_CHALLENGE_ROUTE, CLUSTER_JOIN_REGISTER_ROUTE,
    CLUSTER_MEMBER_PUBLISH_ROUTE, CLUSTER_MEMBERSHIP_RPC_CATEGORY,
    CLUSTER_MEMBERSHIP_RPC_ROUTE_METADATA, CLUSTER_NETWORK_PING_ROUTE,
    CLUSTER_NETWORK_RPC_CATEGORY, CLUSTER_NETWORK_RPC_ROUTE_METADATA,
    OBJECT_RPC_ERROR_DETAIL_METADATA_KEY, ObjectRouteDescriptor, ObjectRpcAuthorizationContext,
    ObjectRpcAuthorizationLayer, ObjectRpcCancelRequest, ObjectRpcCancelResponse,
    ObjectRpcCategoryDefinition, ObjectRpcRequest, ObjectRpcResponse, ObjectRpcRouteCatalog,
    ObjectRpcRoutePolicyLayer, ObjectRpcRouteReadiness, ObjectRpcRouteRegistry,
    ObjectRpcRouteRegistryBuilder, ObjectRpcRouter, ServiceCatalog, ServiceCatalogSnapshot,
    ServiceCategory, call_object_rpc_frame_async, call_object_rpc_service_async,
    serve_object_rpc_frame_async,
};
pub use storage::{
    CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD,
    CLUSTER_CONNECTIVITY_MOBILE_PROBE_MAX_ENDPOINTS_PER_TICK,
    CLUSTER_CONNECTIVITY_PROBE_MAX_ENDPOINTS_PER_TICK, CLUSTER_CONNECTIVITY_REFRESH_MARGIN_SECONDS,
    CLUSTER_CONNECTIVITY_SENSOR_PROBE_MAX_ENDPOINTS_PER_TICK, CLUSTER_CONNECTIVITY_TTL_SECONDS,
    CLUSTER_CONNECTIVITY_UNREACHABLE_TTL_SECONDS, CLUSTER_NETWORK_CONVERGENCE_TICK_INTERVAL,
    CLUSTER_NETWORK_DISCOVERY_INTERVAL, CLUSTER_NETWORK_INFO_TTL_SECONDS,
    CLUSTER_NETWORK_MOBILE_PROBE_INTERVAL, CLUSTER_NETWORK_MOBILE_PUBLISH_INTERVAL,
    CLUSTER_NETWORK_PROBE_INTERVAL, CLUSTER_NETWORK_PUBLISH_INTERVAL,
    CLUSTER_NETWORK_SENSOR_PROBE_INTERVAL, CLUSTER_NETWORK_SENSOR_PUBLISH_INTERVAL,
    CLUSTER_NETWORK_STABLE_DISCOVERY_ANNOUNCE_TICK_STRIDE, ClusterAuthenticatedPeer,
    ClusterConnectivityGraph, ClusterConnectivityProbeBatchEntry, ClusterConnectivityProbeState,
    ClusterConnectivityRecord, ClusterConnectivityState, ClusterControlPlaneDriver,
    ClusterControlPlaneTimeouts, ClusterControlPlaneTransport, ClusterIdentityRecord,
    ClusterInvitationStateRecord, ClusterInvitePublishHost, ClusterJoinRegistrationAsyncHost,
    ClusterJoinRegistrationDriver, ClusterJoinRegistrationHost, ClusterJoinRegistrationRequest,
    ClusterJoinRegistrationResponse, ClusterKvSnapshotRefreshHost, ClusterKvSnapshotRefreshOptions,
    ClusterKvTxnForwardAsyncHost, ClusterKvTxnForwardHost, ClusterKvTxnForwardOptions,
    ClusterKvTxnForwardResult, ClusterKvTxnQuorumAsyncHost, ClusterKvTxnQuorumHost,
    ClusterKvTxnQuorumOptions, ClusterLocalDaemonMembershipAsyncObserver,
    ClusterLocalDaemonMembershipObservation, ClusterLocalDaemonMembershipObserver,
    ClusterLocalDaemonMembershipRefresh, ClusterLocalMemberPublishAsyncHost,
    ClusterLocalMemberPublishHost, ClusterLocalMemberPublishOptions,
    ClusterMemberMutationAsyncHost, ClusterMemberMutationDriver, ClusterMemberMutationHost,
    ClusterMemberPublishRequest, ClusterMemberPublishResponse,
    ClusterMembershipRpcAsyncForwardHost, ClusterMembershipRpcForwardHost,
    ClusterMembershipRpcFuture, ClusterNetworkInfoPublishAsyncHost, ClusterNetworkInfoRecord,
    ClusterNetworkMaintenanceSchedule, ClusterNetworkRecordExpiryHost, ClusterNodeRoleUpdate,
    ClusterNodeStatusPublishAsyncHost, ClusterPaths, ClusterRaftAppendReplicationAsyncHost,
    ClusterRaftAppendReplicationHost, ClusterRaftAppendReplicationOptions,
    ClusterRaftAppendReplicationOutcome, ClusterRaftCommittedFanoutAsyncHost,
    ClusterRaftCommittedFanoutOptions, ClusterRaftLeaderElectionOptions,
    ClusterRaftQuorumAsyncHost, ClusterRaftQuorumCommitOptions, ClusterRaftQuorumCommitResult,
    ClusterRaftQuorumTransportHost, ClusterRaftVoteTransportAsyncHost,
    ClusterRaftVoteTransportHost, ClusterVoterReachabilityAsyncHost, ClusterVoterReachabilityHost,
    ClusterVoterReachabilityOptions, DAEMON_MAIN_CLUSTER_ROLE, DAEMON_MAIN_RUN_CAPABILITY,
    DAEMON_SHADOW_CLUSTER_ROLE, DEFAULT_CLUSTER_CONTROL_PLANE_IROH_COMMAND_TIMEOUT,
    DEFAULT_CLUSTER_CONTROL_PLANE_IROH_FOREGROUND_ATTEMPT_TIMEOUT,
    DEFAULT_CLUSTER_CONTROL_PLANE_PUBLIC_FOREGROUND_ATTEMPT_TIMEOUT,
    DEFAULT_CLUSTER_CONTROL_PLANE_RAFT_APPEND_TIMEOUT,
    DEFAULT_CLUSTER_CONTROL_PLANE_TCP_FOREGROUND_ATTEMPT_TIMEOUT, IoObserver,
    MAX_CLUSTER_CONNECTIVITY_SOURCE_RECORDS, accepted_cluster_join_state_from_kv_snapshot,
    advertised_peer_route_targets, apply_cluster_connectivity_probe_results,
    apply_cluster_iroh_endpoint_advertisement, apply_cluster_node_role,
    assign_local_daemon_member_public_keys, build_cluster_network_info_record,
    cleanup_cluster_iroh_endpoints, cleanup_legacy_shadow_daemon_members_with_host,
    cluster_bootstrap_state_stable_eq, cluster_connectivity_full_sync_ops,
    cluster_connectivity_key, cluster_connectivity_probe_due_with_graph,
    cluster_connectivity_probe_due_with_startup_settle, cluster_connectivity_probe_key,
    cluster_connectivity_record, cluster_connectivity_record_for_hard_failure,
    cluster_connectivity_record_for_identity_mismatch,
    cluster_connectivity_record_for_probe_outcome,
    cluster_connectivity_record_for_startup_probe_failure, cluster_connectivity_source_prefix,
    cluster_connectivity_stable_eq, cluster_connectivity_sync_ops, cluster_endpoint_key,
    cluster_endpoint_prefix, cluster_has_local_quorum_with_host, cluster_identity_key,
    cluster_invitation_key, cluster_invitation_state_key,
    cluster_iroh_proxy_republish_sleep_duration, cluster_iroh_proxy_start_retry_sleep_duration,
    cluster_is_settled_solo, cluster_join_challenge_rpc_request, cluster_local_node_role,
    cluster_member_expected_daemon_process, cluster_member_is_local_process, cluster_member_key,
    cluster_member_publish_txn_from_kv, cluster_network_discovery_interval_for_role,
    cluster_network_info_key, cluster_network_info_stable_eq, cluster_network_maintenance_due,
    cluster_network_maintenance_interval_due,
    cluster_network_maintenance_last_timestamp_for_next_due,
    cluster_network_maintenance_next_due_duration, cluster_network_probe_interval_for_role,
    cluster_network_publish_interval_for_node, cluster_network_publish_interval_for_role,
    cluster_network_record_should_refresh, cluster_node_blocks_leader_preference,
    cluster_node_is_local_process, cluster_node_status_stable_eq,
    cluster_reachable_voter_count_with_host, cluster_state_needs_join_registration,
    cluster_state_sync_txn, cluster_status_summary_label,
    commit_cluster_kv_txn_with_quorum_response_with_host,
    commit_cluster_raft_command_with_quorum_driver, discover_peer_route_ip_candidates,
    elect_cluster_raft_leader_with_host, ensure_daemon_cluster_members,
    expire_cluster_network_records_with_host, forward_cluster_kv_txn_to_voter_with_host,
    handle_cluster_join_registration_request_with_host, handle_cluster_kv_watch_events,
    handle_cluster_membership_rpc_request_with_hosts, handle_cluster_raft_rpc_request,
    join_channel_endpoint_for_accepted_state, merge_cluster_join_registration,
    prepare_cluster_join_retry_state, prepare_pending_cluster_join_state,
    publish_cluster_invitation_to_quorum_with_host,
    publish_cluster_network_info_to_quorum_with_host,
    publish_cluster_node_status_to_quorum_with_host,
    publish_local_members_to_quorum_if_needed_with_host, publish_local_members_via_voter_with_host,
    read_cluster_connectivity_graph_from_kv, read_cluster_endpoints_from_kv,
    read_cluster_invitation_from_kv, read_cluster_members_from_kv,
    read_cluster_node_statuses_from_kv, read_cluster_state, read_cluster_state_cached,
    read_cluster_state_without_kv, read_local_cluster_connectivity_records,
    reconcile_accepted_cluster_join_response, reconcile_cluster_join_with_introducers_with_host,
    record_forwarded_join_registration_acceptance_with_host,
    refresh_current_cluster_kv_store_with_host,
    refresh_current_cluster_kv_store_with_local_storage_and_transport,
    refresh_local_daemon_membership_from_disk, refresh_local_iroh_endpoints_from_disk,
    remove_cluster_members_with_host, replicate_cluster_raft_to_endpoint_with_host,
    select_cluster_connectivity_probe_batch, send_cluster_join_registration_over_membership_rpc,
    set_io_observer, spawn_cluster_raft_committed_fanout_worker_on_runtime,
    stored_cluster_network_info_matches, stored_cluster_node_status_matches,
    sync_cluster_connectivity_records_to_kv, sync_cluster_connectivity_records_to_quorum_with_host,
    sync_cluster_invitation_to_kv, sync_cluster_network_info_to_kv, sync_cluster_node_status_to_kv,
    sync_cluster_state_to_kv_with_host, sync_cluster_state_to_local_raft_unchecked,
    update_cluster_node_role_with_host, update_cluster_raft_role_with_host, upsert_member_endpoint,
    upsert_member_record, write_cluster_state, write_local_cluster_connectivity_records,
};
pub use storage::{
    ClusterConnectivityPublishAsyncHost,
    cluster_connectivity_probe_max_endpoints_per_tick_for_role,
    cluster_has_local_quorum_async_with_host, cluster_kv_key_can_defer_materialized_snapshot,
    cluster_network_stable_discovery_announcement_due,
    commit_cluster_kv_txn_with_quorum_response_async_with_host,
    commit_cluster_raft_index_with_events_async,
    daemon_advertised_endpoints_from_addresses_with_public_discovery_and_advertise_host_async,
    daemon_advertised_endpoints_from_static_addresses, daemon_lan_endpoint,
    discard_cluster_raft_uncommitted_tail_and_clear_leader_async,
    discover_peer_route_ip_candidates_async,
    forward_cluster_join_challenge_rpc_to_voter_async_with_host,
    handle_cluster_invite_publish_request_async_with_host,
    handle_cluster_join_registration_request_async_with_host,
    handle_cluster_kv_txn_request_async_with_host,
    handle_cluster_member_publish_request_async_with_host, handle_cluster_raft_rpc_request_async,
    publish_cluster_network_info_to_quorum_async_with_host,
    publish_cluster_node_status_to_quorum_async_with_host,
    publish_local_members_to_quorum_if_needed_async_with_host,
    publish_local_members_via_voter_async_with_host, read_cluster_state_async,
    read_cluster_state_without_kv_async, refresh_cluster_connectivity_environment,
    refresh_cluster_connectivity_environment_async,
    refresh_local_daemon_membership_from_disk_async,
    replicate_cluster_raft_to_endpoint_async_with_host,
    sync_cluster_connectivity_records_to_quorum_async_with_host,
    sync_cluster_state_to_kv_async_with_host, write_cluster_state_async,
};

pub type ClusterId = sinomo_core::Id;
pub type ClusterEndpointId = sinomo_core::Id;
pub type ClusterInviteId = sinomo_core::Id;
pub type ClusterCertificateId = sinomo_core::Id;

pub const NETWORK_PROTOCOL: &str = "sinomo.network.v1";
pub const SINOMO_NETWORK_MODULE_LEGACY_ID: &str = "module.network";
pub const SINOMO_NETWORK_MODULE_OWNER: &str = "sinomo-network";
pub const SINOMO_NETWORK_NATIVE_REGISTRATION_KEY: &str = "sinomo-network";
pub const NETWORK_OBJECT_RPC_ROUTES: &[&str] = &[CLUSTER_HEALTH_ROUTE, CLUSTER_NETWORK_PING_ROUTE];
pub const CLUSTER_HEALTH_READ_CAPABILITY: &str = "cluster.health.read";
pub const CLUSTER_NETWORK_PING_CAPABILITY: &str = "cluster.network.ping";
pub const NETWORK_CAPABILITY_IMPORTS: &[&str] = &[
    "storage.local.read",
    "storage.local.write",
    "cluster.kv.read",
];
pub const NETWORK_CAPABILITY_EXPORTS: &[&str] = &[
    CLUSTER_HEALTH_READ_CAPABILITY,
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
DFS repair tools unless the user explicitly asks for remediation and the current
health evidence supports that action.
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
        "cluster/state.sobj",
        ObjectSchema::new("ClusterBootstrapState", STORAGE_CONTRACT_OBJECT_VERSION)
            .field("cluster_id", "ClusterId")
            .field("local_node_id", "NodeId")
            .field("members", "Vec<ClusterMemberRecord>"),
    )?;
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
    layout.register_escape_path(
        "sinomo-network",
        StorageRing::R1,
        "cluster/iroh",
        "dynamic iroh key and endpoint object files keyed by node id",
    )
}

pub(crate) mod prelude {
    pub(crate) use crate::{ClusterEndpointId, ClusterId, ClusterInviteId};
    pub(crate) use serde::{Deserialize, Serialize};
    #[cfg(test)]
    pub(crate) use sinomo_core::ClusterCertificate;
    pub(crate) use sinomo_core::{
        CapabilityName, ClusterBootstrapState, ClusterEndpointPrivacy, ClusterEndpointRecord,
        ClusterEndpointSource, ClusterInvitationChannel, ClusterJoinInvite, ClusterMemberRecord,
        ClusterMemberRole, ClusterMemberState, ClusterNodeAvailability, ClusterNodeRole,
        ClusterNodeStatusRecord, ClusterTransportKind, EventEnvelope, TrustDomain,
    };
    #[cfg(test)]
    pub(crate) use sinomo_core::{LatencyClass, RoutingScope};
    #[cfg(test)]
    pub(crate) use sinomo_events::EventStore;
    pub(crate) use sinomo_events::{EventId, EventStoreError};
    #[cfg(test)]
    pub(crate) use sinomo_events::{EventSequence, SessionId, StoredEvent};
    #[cfg(test)]
    pub(crate) use sinomo_kv::KvLogEntry;
    #[cfg(test)]
    pub(crate) use sinomo_kv::LeaseId;
    pub(crate) use sinomo_kv::{
        ClusterKvWatchEventsRequest, ClusterKvWatchEventsResponse, DurableRaftKvNode,
        InProcessKvStore, KvCommand, KvCompare, KvEntry, KvError, KvEvent, KvLocalCache, KvOp,
        KvSnapshot, KvTxn, KvTxnResponse, RaftAppendEntriesRequest, RaftAppendEntriesResponse,
        RaftRequestVoteRequest, RaftRequestVoteResponse, RaftSnapshot,
    };
    #[cfg(test)]
    pub(crate) use sinomo_kv::{KvWalPaths, WatchId};
    pub(crate) use sinomo_local::SinomoDataDir;
    #[cfg(test)]
    pub(crate) use sinomo_node::{NodeAvailability, NodeHealth, NodeProfile};
    pub(crate) use sinomo_node::{
        NodeId, daemon_lan_endpoint_id, daemon_loopback_endpoint_id, main_daemon_capabilities,
        shadow_daemon_node_id,
    };
    #[cfg(test)]
    pub(crate) use std::collections::HashSet;
    pub(crate) use std::collections::{BTreeMap, BTreeSet, VecDeque};
    pub(crate) use std::fs;
    pub(crate) use std::io;
    pub(crate) use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4, UdpSocket};
    pub(crate) use std::path::PathBuf;
    pub(crate) use std::sync::{Arc, Mutex};
    #[cfg(feature = "iroh-transport")]
    pub(crate) use std::thread;
    pub(crate) use std::time::Duration;
    pub(crate) use thiserror::Error;
    pub(crate) use time::OffsetDateTime;
}

#[cfg(test)]
mod tests;
