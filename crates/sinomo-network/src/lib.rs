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
mod connectivity_publish;
mod connectivity_store;
mod control_plane_driver;
mod control_plane_options;
mod endpoint_advertisement;
mod endpoint_records;
mod endpoint_selector;
mod error;
mod file_mark;
mod health;
mod invite;
mod invite_publish;
mod io_observer;
mod join_registration;
mod keyspace;
mod kv_forward;
mod kv_local_store;
mod kv_read_model;
mod kv_semantics;
mod kv_snapshot;
mod kv_txn_ops;
mod kv_txn_quorum;
mod kv_watch_store;
mod lan;
mod local_membership;
mod local_state_store;
mod maintenance;
mod member_mutation;
mod member_publish;
mod member_store;
mod membership;
mod membership_forward;
mod membership_rpc;
mod network_info;
mod object;
mod object_file_store;
mod paths;
mod peer;
mod raft_election;
mod raft_fanout;
mod raft_quorum;
mod raft_replication;
mod records;
mod roaming;
mod route;
mod route_graph_read_model;
mod rpc;
mod rpc_future;
mod status_publish;
mod storage;
mod voter_reachability;

pub use connectivity::{
    CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD, CLUSTER_CONNECTIVITY_TTL_SECONDS,
    CLUSTER_CONNECTIVITY_UNREACHABLE_TTL_SECONDS, ClusterConnectivityGraph,
    ClusterConnectivityProbeBatch, ClusterConnectivityProbeBatchEntry,
    ClusterConnectivityProbeState, ClusterConnectivityRecord, ClusterConnectivityRecordQuery,
    ClusterConnectivityState, apply_cluster_connectivity_probe_results,
    cluster_connectivity_probe_due_with_graph, cluster_connectivity_probe_due_with_startup_settle,
    cluster_connectivity_probe_key, cluster_connectivity_record,
    cluster_connectivity_record_for_hard_failure,
    cluster_connectivity_record_for_identity_mismatch,
    cluster_connectivity_record_for_probe_outcome,
    cluster_connectivity_record_for_startup_probe_failure, select_cluster_connectivity_probe_batch,
};
pub use connectivity_publish::{
    ClusterConnectivityPublishAsyncHost, ClusterConnectivityPublishHost,
    ClusterNetworkRecordExpiryHost, MAX_CLUSTER_CONNECTIVITY_SOURCE_RECORDS,
    cluster_connectivity_full_sync_ops, cluster_connectivity_stable_eq,
    cluster_connectivity_sync_ops, cluster_network_publish_interval_for_node,
    cluster_network_record_should_refresh, expire_cluster_network_records_with_host,
    sync_cluster_connectivity_records_to_kv,
    sync_cluster_connectivity_records_to_quorum_async_with_host,
    sync_cluster_connectivity_records_to_quorum_with_host,
};
pub use connectivity_store::{
    read_local_cluster_connectivity_records, write_local_cluster_connectivity_records,
};
pub use control_plane_driver::{ClusterControlPlaneDriver, ClusterControlPlaneTransport};
pub use control_plane_options::{
    ClusterControlPlaneTimeouts, ClusterKvTxnQuorumOptions, ClusterRaftAppendReplicationOptions,
    ClusterRaftAppendReplicationOutcome, ClusterRaftCommittedFanoutOptions,
    ClusterRaftLeaderElectionOptions, DEFAULT_CLUSTER_CONTROL_PLANE_IROH_COMMAND_TIMEOUT,
    DEFAULT_CLUSTER_CONTROL_PLANE_IROH_FOREGROUND_ATTEMPT_TIMEOUT,
    DEFAULT_CLUSTER_CONTROL_PLANE_MEMBERSHIP_RPC_TIMEOUT,
    DEFAULT_CLUSTER_CONTROL_PLANE_PUBLIC_FOREGROUND_ATTEMPT_TIMEOUT,
    DEFAULT_CLUSTER_CONTROL_PLANE_RAFT_APPEND_MAX_ATTEMPTS,
    DEFAULT_CLUSTER_CONTROL_PLANE_RAFT_APPEND_TIMEOUT, DEFAULT_CLUSTER_CONTROL_PLANE_RPC_TIMEOUT,
    DEFAULT_CLUSTER_CONTROL_PLANE_TCP_COMMAND_TIMEOUT,
    DEFAULT_CLUSTER_CONTROL_PLANE_TCP_FOREGROUND_ATTEMPT_TIMEOUT,
};
#[cfg(test)]
pub(crate) use endpoint_advertisement::{
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
#[cfg(test)]
pub(crate) use endpoint_records::cleanup_member_iroh_endpoints;
pub(crate) use endpoint_records::iroh_endpoint_public_key;
pub use endpoint_records::{
    apply_cluster_iroh_endpoint_advertisement, cleanup_cluster_iroh_endpoints,
    cluster_endpoint_stable_eq, refresh_local_iroh_endpoints_from_disk,
    refresh_local_iroh_endpoints_from_disk_async, upsert_member_endpoint,
};
pub use endpoint_selector::{
    ClusterEndpointSelectionPurpose, ClusterEndpointSelector, advertised_peer_route_targets,
    cluster_node_blocks_leader_preference, discover_peer_route_ip_candidates,
    discover_peer_route_ip_candidates_async,
};
#[cfg(test)]
pub(crate) use endpoint_selector::{
    cluster_sync_targets, cluster_sync_targets_with_connectivity,
    prefer_bounded_daemon_command_targets,
};
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
pub use invite_publish::{
    ClusterInvitePublishHost, handle_cluster_invite_publish_request_async_with_host,
    publish_cluster_invitation_to_quorum_with_host, read_cluster_invitation_from_kv,
    sync_cluster_invitation_to_kv,
};
#[cfg(test)]
pub(crate) use invite_publish::{
    cluster_invitation_publish_txn_from_kv, handle_cluster_invite_publish_request_with_host,
};
pub(crate) use io_observer::io_observer;
pub use io_observer::{IoObserver, set_io_observer};
#[cfg(test)]
pub(crate) use join_registration::prepare_cluster_join_registration_response;
pub use join_registration::{
    ClusterJoinReconciliation, ClusterJoinReconciliationHost, ClusterJoinRegistrationAsyncHost,
    ClusterJoinRegistrationDriver, ClusterJoinRegistrationHost,
    ClusterLocalDaemonMembershipAsyncObserver, ClusterLocalDaemonMembershipObserver,
    accepted_cluster_join_state_from_kv_snapshot, cluster_join_challenge_rpc_request,
    handle_cluster_join_registration_request_async_with_host,
    handle_cluster_join_registration_request_with_host, join_channel_endpoint_for_accepted_state,
    reconcile_accepted_cluster_join_response, reconcile_cluster_join_with_introducers_with_host,
    record_forwarded_join_registration_acceptance_with_host,
    send_cluster_join_registration_over_membership_rpc,
};
#[cfg(test)]
pub(crate) use keyspace::{
    cluster_capability_policy_key, cluster_node_status_key, cluster_node_status_key_for_process,
    cluster_node_status_key_for_record,
};
pub use keyspace::{
    cluster_connectivity_key, cluster_connectivity_source_prefix, cluster_endpoint_key,
    cluster_endpoint_prefix, cluster_identity_key, cluster_invitation_key,
    cluster_invitation_state_key, cluster_member_key, cluster_network_info_key,
};
pub use kv_forward::{
    ClusterKvTxnForwardHost, ClusterKvTxnForwardOptions, ClusterKvTxnForwardResult,
    forward_cluster_kv_txn_to_voter_async_with_host, forward_cluster_kv_txn_to_voter_with_host,
};
pub use kv_local_store::{
    cluster_kv_key_can_defer_materialized_snapshot, commit_cluster_raft_index_with_events_async,
    discard_cluster_raft_uncommitted_tail_and_clear_leader_async,
};
pub use kv_read_model::{
    read_cluster_connectivity_graph_from_kv, read_cluster_members_from_kv,
    read_cluster_node_statuses_from_kv,
};
#[cfg(test)]
pub(crate) use kv_semantics::cluster_semantic_txn_no_effect_response;
pub use kv_snapshot::{
    ClusterKvSnapshotRefreshHost, ClusterKvSnapshotRefreshOptions,
    refresh_current_cluster_kv_store_with_host,
    refresh_current_cluster_kv_store_with_local_storage_and_transport,
};
pub use kv_txn_quorum::{
    ClusterKvTxnQuorumAsyncHost, ClusterKvTxnQuorumHost,
    commit_cluster_kv_txn_with_quorum_response_async_with_host,
    commit_cluster_kv_txn_with_quorum_response_with_host,
    handle_cluster_kv_txn_request_async_with_host,
};
pub use kv_watch_store::handle_cluster_kv_watch_events;
pub use lan::{
    AsyncMdnsDiscoverySocket, MdnsDiscoveryAdvertisement, MdnsDiscoveryCache, MdnsDiscoveryRecord,
    apply_lan_discovery_advertisements, discover_default_lan_ip_candidates,
    discover_default_lan_ip_candidates_async, format_ip_port, iroh_disabled_by_env,
    lan_discovery_advertisement_for_state,
};
pub use local_membership::{
    ClusterLocalDaemonMembershipObservation, ClusterLocalDaemonMembershipRefresh,
    apply_cluster_node_role, assign_local_daemon_member_public_keys,
    cluster_state_needs_join_registration, cluster_status_summary_label,
    ensure_daemon_cluster_members, merge_cluster_join_registration,
    prepare_cluster_join_retry_state, prepare_pending_cluster_join_state,
    refresh_cluster_connectivity_environment, refresh_cluster_connectivity_environment_async,
    refresh_local_daemon_membership_from_disk, refresh_local_daemon_membership_from_disk_async,
};
pub use local_state_store::{
    read_cluster_state, read_cluster_state_async, read_cluster_state_cached,
    read_cluster_state_without_kv, read_cluster_state_without_kv_async, write_cluster_state,
    write_cluster_state_async,
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
    cluster_iroh_proxy_republish_sleep_duration, cluster_iroh_proxy_start_retry_sleep_duration,
    cluster_local_node_role, cluster_network_discovery_interval_for_role,
    cluster_network_maintenance_due, cluster_network_maintenance_interval_due,
    cluster_network_maintenance_last_timestamp_for_next_due,
    cluster_network_maintenance_next_due_duration, cluster_network_probe_interval_for_role,
    cluster_network_publish_interval_for_role, cluster_network_stable_discovery_announcement_due,
};
pub use member_mutation::{
    ClusterMemberMutationAsyncHost, ClusterMemberMutationDriver, ClusterMemberMutationHost,
    ClusterNodeRoleUpdate,
};
pub use member_publish::{
    ClusterLocalMemberPublishAsyncHost, ClusterLocalMemberPublishHost,
    ClusterLocalMemberPublishOptions, ClusterMemberPublishHost, cluster_member_publish_txn_from_kv,
    handle_cluster_member_publish_request_async_with_host,
    publish_local_members_to_quorum_if_needed_async_with_host,
    publish_local_members_to_quorum_if_needed_with_host,
    publish_local_members_via_voter_async_with_host, publish_local_members_via_voter_with_host,
};
#[cfg(test)]
pub(crate) use member_publish::{
    cluster_member_publish_authorized_for_signer, cluster_member_publish_matches_kv,
    cluster_member_publish_txn, cluster_member_publish_txn_from_snapshot,
    handle_cluster_member_publish_request_with_host,
};
pub use member_store::{cluster_bootstrap_state_stable_eq, upsert_member_record};
pub use membership::{
    DAEMON_MAIN_CLUSTER_ROLE, DAEMON_SHADOW_CLUSTER_ROLE, DAEMON_SHADOW_SUPERVISE_CAPABILITY,
    cluster_is_settled_solo, cluster_member_expected_daemon_process,
    cluster_member_is_legacy_shadow_daemon, cluster_member_is_local_process,
    cluster_node_is_local_process, live_cluster_member_probe_possible,
};
#[cfg(test)]
pub(crate) use membership::{cluster_has_remote_peers, cluster_local_process_members};
#[cfg(test)]
pub(crate) use membership_forward::forward_cluster_join_challenge_rpc_to_voter_with_host;
#[cfg(test)]
pub(crate) use membership_forward::forward_cluster_membership_rpc_to_voter_with_host;
pub use membership_forward::{
    ClusterMembershipRpcForwardHost, forward_cluster_join_challenge_rpc_to_voter_async_with_host,
};
pub use membership_rpc::{
    ClusterAuthenticatedPeer, ClusterInvitePublishRequest, ClusterInvitePublishResponse,
    ClusterJoinRegistrationRequest, ClusterJoinRegistrationResponse, ClusterMemberPublishRequest,
    ClusterMemberPublishResponse, handle_cluster_membership_rpc_request_with_hosts,
};
pub use network_info::{
    CLUSTER_NETWORK_INFO_TTL_SECONDS, ClusterNetworkInfoRecord, build_cluster_network_info_record,
    cluster_network_info_stable_eq,
};
#[cfg(any(feature = "iroh-transport", test))]
pub(crate) use object::read_object_frame_async;
#[cfg(test)]
pub(crate) use object::write_object_frame_async;
pub use object::{
    AsyncDaemonControlListener, AsyncDaemonControlStream, DAEMON_CONTROL_RPC_MAX_FRAME_BYTES,
    DaemonControlRequest, DaemonControlResponse, DaemonRpcTimeouts, OBJECT_STREAM_MAX_CHUNK_BYTES,
    ObjectFrameIoOptions, RawByteStreamChunkOptions, SinomoObjectCodec,
    mark_object_rpc_request_accepts_raw_streamed_daemon_response,
    mark_object_rpc_request_reuses_daemon_stream,
    mark_object_rpc_response_has_streamed_daemon_body,
    object_rpc_request_accepts_streamed_daemon_response,
    object_rpc_request_expects_raw_daemon_response, object_rpc_request_reuses_daemon_stream,
    object_rpc_response_has_streamed_daemon_body, send_daemon_control_request_async,
};
pub(crate) use object::{
    DAEMON_CONTROL_RAW_RPC_RESPONSE_METADATA_KEY, DAEMON_CONTROL_RAW_RPC_RESPONSE_METADATA_VALUE,
    DAEMON_CONTROL_STREAMED_RPC_RESPONSE_METADATA_KEY,
    DAEMON_CONTROL_STREAMED_RPC_RESPONSE_METADATA_VALUE, ENDPOINT_RPC_RAW_DIRECT_METADATA_KEY,
    ENDPOINT_RPC_RAW_DIRECT_METADATA_VALUE, send_daemon_control_rpc_request_raw_response_async,
};
#[cfg(any(feature = "iroh-transport", test))]
pub(crate) use object::{
    OBJECT_FRAME_EXTENDED_LENGTH_SENTINEL, decode_single_object_frame_from_slice,
    encode_object_frame_to_vec,
};
pub use paths::ClusterPaths;
pub use peer::{
    AsyncInProcessNetwork, AsyncInProcessTransport, AsyncNetworkSendOptions, InProcessNetwork,
    InProcessObjectRpcMessage, InProcessTransport, InboundObjectRpcMessage,
};
pub use raft_election::{
    ClusterRaftVoteTransportAsyncHost, ClusterRaftVoteTransportHost,
    elect_cluster_raft_leader_async_with_host, elect_cluster_raft_leader_with_host,
};
pub use raft_fanout::{
    ClusterRaftCommittedFanoutAsyncHost, ClusterRaftCommittedFanoutReport,
    fan_out_committed_cluster_raft_command_async_with_host,
    spawn_cluster_raft_committed_fanout_worker_on_runtime,
};
pub use raft_quorum::{
    ClusterRaftQuorumAsyncHost, ClusterRaftQuorumCommitOptions, ClusterRaftQuorumCommitResult,
    ClusterRaftQuorumTransportHost, commit_cluster_raft_command_with_quorum_async_with_host,
    commit_cluster_raft_command_with_quorum_driver,
};
pub use raft_replication::{
    ClusterRaftAppendReplicationAsyncHost, ClusterRaftAppendReplicationHost,
    replicate_cluster_raft_to_endpoint_async_with_host,
    replicate_cluster_raft_to_endpoint_with_host,
};
pub use records::{ClusterIdentityRecord, ClusterInvitationStateRecord};
pub(crate) use roaming::read_iroh_endpoint_record_async;
#[cfg(test)]
pub(crate) use roaming::send_daemon_rpc_to_cluster_endpoint_async;
pub use roaming::{
    IROH_DAEMON_COMMAND_MAX_BYTES, IrohDaemonProxyHandle, IrohEndpointRecord, IrohRelayMode,
    read_iroh_endpoint_record, send_daemon_rpc_to_address_async, stable_iroh_endpoint_id,
    start_iroh_daemon_proxy, write_iroh_endpoint_record,
};
pub use route::{
    CLUSTER_RELAY_ROUTE, CLUSTER_ROUTING_RPC_CATEGORY, CLUSTER_ROUTING_RPC_ROUTE_METADATA,
    ClusterObjectRpcClient, ClusterObjectRpcNodeOptions, ClusterObjectRpcOptions,
    ClusterObjectRpcRouteContext, ClusterRelayAsyncHost, ClusterRelayFuture, ClusterRelayOptions,
    ClusterRelayStreamForwardAsyncHost, ClusterRelayStreamForwardOutcome,
    ClusterRelayStreamSendOutcome, ClusterSignedEndpointRpcAsyncHost,
    ClusterSignedEndpointRpcFuture, ClusterSignedEndpointRpcObservation,
    ClusterSignedEndpointRpcObservationKind, RelayEnvelope, handle_cluster_relay_async_with_host,
    handle_cluster_relay_stream_forward_async_with_host, relay_response_is_routing_error,
};
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
    ServiceCategory, call_object_rpc_service_async,
};
#[cfg(test)]
pub(crate) use rpc::{call_object_rpc_frame_async, serve_object_rpc_frame_async};
pub use rpc_future::ClusterMembershipRpcFuture;
pub use sinomo_node::DAEMON_MAIN_RUN_CAPABILITY;
pub use status_publish::{
    ClusterNetworkInfoPublishAsyncHost, ClusterNetworkInfoPublishHost,
    ClusterNodeStatusPublishAsyncHost, ClusterNodeStatusPublishHost, cluster_node_status_stable_eq,
    publish_cluster_network_info_to_quorum_async_with_host,
    publish_cluster_network_info_to_quorum_with_host,
    publish_cluster_node_status_to_quorum_async_with_host,
    publish_cluster_node_status_to_quorum_with_host, stored_cluster_network_info_matches,
    stored_cluster_node_status_matches, sync_cluster_network_info_to_kv,
    sync_cluster_node_status_to_kv,
};
pub use storage::{
    cleanup_legacy_shadow_daemon_members_with_host, cluster_state_sync_txn,
    handle_cluster_raft_rpc_request, handle_cluster_raft_rpc_request_async,
    remove_cluster_members_with_host, sync_cluster_state_to_kv_async_with_host,
    sync_cluster_state_to_kv_with_host, sync_cluster_state_to_local_raft_unchecked,
    update_cluster_node_role_with_host, update_cluster_raft_role_with_host,
};
pub use voter_reachability::{
    ClusterVoterReachabilityAsyncHost, ClusterVoterReachabilityHost,
    ClusterVoterReachabilityOptions, cluster_has_local_quorum_async_with_host,
    cluster_has_local_quorum_with_host, cluster_reachable_voter_count_async_with_host,
    cluster_reachable_voter_count_with_host,
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
    pub(crate) use sinomo_kv::LeaseId;
    #[cfg(test)]
    pub(crate) use sinomo_kv::WatchId;
    pub(crate) use sinomo_kv::{
        ClusterKvWatchEventsRequest, ClusterKvWatchEventsResponse, InProcessKvStore, KvCommand,
        KvError, KvEvent, KvOp, KvSnapshot, KvTxn, KvTxnResponse, RaftRequestVoteRequest,
        RaftRequestVoteResponse,
    };
    #[cfg(test)]
    pub(crate) use sinomo_local::SinomoDataDir;
    #[cfg(test)]
    pub(crate) use sinomo_node::main_daemon_capabilities;
    #[cfg(test)]
    pub(crate) use sinomo_node::{NodeAvailability, NodeHealth, NodeProfile};
    pub(crate) use sinomo_node::{NodeId, shadow_daemon_node_id};
    #[cfg(test)]
    pub(crate) use std::collections::HashSet;
    pub(crate) use std::collections::{BTreeMap, BTreeSet, VecDeque};
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
