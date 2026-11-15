//! Cluster control plane: membership, invites, joins, KV/Raft replication
//! drivers, cluster state publishing, and the KV-backed cluster directory.
//!
//! This crate owns the transport-coupled cluster orchestration between
//! `sinomo-network` (mechanism: routing, RPC, transports) and `sinomo-kv`
//! (storage and consensus core). It implements the network crate's
//! [`sinomo_network::ClusterDirectory`] by reading cluster KV and registers
//! the cluster control-plane object-RPC routes.

pub use sinomo_base::cluster::*;

mod command_observation;
mod connectivity_import;
mod connectivity_projection;
mod connectivity_publish;
mod consensus_service;
mod control_authority;
mod control_plane_adapters;
mod control_plane_driver;
mod control_plane_options;
mod control_routes;
mod control_service;
mod route_hint;
pub use route_hint::{
    CLUSTER_CONTROL_ROUTE_HINT_ROUTE, ClusterRouteHintRequest, ClusterRouteHintResponse,
    apply_cluster_route_hint,
};
mod health;
mod invite;
mod invite_publish;
mod join_registration;
mod keyspace;
mod kv_forward;
mod kv_local_store;
mod kv_read_model;
#[cfg(any(test, feature = "test-support"))]
mod kv_semantics;
mod kv_service;
mod kv_snapshot;
mod kv_snapshot_observer;
mod kv_txn_quorum;
mod local_membership;
mod local_state_store;
mod maintenance_reactor;
mod member_mutation;
mod member_publish;
mod member_store;
mod membership_forward;
mod membership_rpc;
mod membership_service;
mod migration;
mod network_maintenance;
mod network_service;
mod publication_cache;
mod raft_election;
mod raft_fanout;
mod raft_peer_queue;
mod raft_proposal_queue;
mod raft_quorum;
pub use raft_peer_queue::send_shared_cluster_raft_append;
#[cfg(any(test, feature = "test-support"))]
mod raft_replication;
mod raft_rpc;
mod service_providers;
mod state_authority_reachability;
mod status_publish;

pub use command_observation::{
    CLUSTER_COMMAND_OBSERVATION_MAX_ENDPOINTS, CLUSTER_COMMAND_OBSERVATION_MAX_IN_FLIGHT,
    CLUSTER_COMMAND_SUCCESS_OBSERVATION_INTERVAL, ClusterCommandConnectivityObservation,
    ClusterCommandObservationCoordinator, ClusterCommandObservationReservation,
    cluster_command_observation_key,
};
pub use connectivity_import::{
    accepted_join_connectivity_record_groups, filter_peer_connectivity_response_records,
    merge_connectivity_probe_records_with_live_unprobed,
    newly_healthy_connectivity_target_node_ids,
};
pub use connectivity_projection::{
    ClusterConnectivityEndpointProjection, ClusterConnectivityMemberProjection,
    ClusterConnectivityProjectionChange, ClusterConnectivityTopologyProjection,
    ClusterConnectivityTopologyValue, cluster_connectivity_events_cover_revision_range,
    cluster_connectivity_probe_input_projection, cluster_connectivity_probe_topology_is_current,
};
pub use connectivity_publish::{
    ClusterConnectivityPublishAsyncHost, ClusterConnectivityPublishHost,
    cluster_connectivity_publish_record_matches, cluster_network_publish_interval_for_node,
    publish_current_cluster_connectivity_records_to_quorum_async_with_state_and_host,
    sync_cluster_connectivity_records_to_kv,
    sync_cluster_connectivity_records_to_quorum_async_with_host,
    sync_cluster_connectivity_records_to_quorum_async_with_state_and_host,
    sync_cluster_connectivity_records_to_quorum_with_host,
};
pub use consensus_service::{
    ClusterConsensusServiceHost, HostedClusterConsensusService, register_cluster_consensus_service,
};
pub use control_authority::RemoteClusterControlAuthorityFence;
pub use control_plane_driver::{ClusterControlPlaneDriver, ClusterControlPlaneTransport};
pub use control_plane_options::{
    ClusterControlPlaneTimeouts, ClusterKvTxnQuorumOptions,
    DEFAULT_CLUSTER_CONTROL_PLANE_COMMAND_TIMEOUT,
    DEFAULT_CLUSTER_CONTROL_PLANE_IROH_FOREGROUND_ATTEMPT_TIMEOUT,
    DEFAULT_CLUSTER_CONTROL_PLANE_MEMBERSHIP_RPC_TIMEOUT,
    DEFAULT_CLUSTER_CONTROL_PLANE_PUBLIC_FOREGROUND_ATTEMPT_TIMEOUT,
    DEFAULT_CLUSTER_CONTROL_PLANE_RAFT_APPEND_MAX_ATTEMPTS,
    DEFAULT_CLUSTER_CONTROL_PLANE_RAFT_APPEND_TIMEOUT,
    DEFAULT_CLUSTER_CONTROL_PLANE_RAFT_CAMPAIGN_JITTER_MAX,
    DEFAULT_CLUSTER_CONTROL_PLANE_TCP_FOREGROUND_ATTEMPT_TIMEOUT, main_daemon_cluster_rpc_options,
};
pub use control_routes::{
    CLUSTER_CONTROL_ADMIN_CAPABILITY, CLUSTER_CONTROL_INIT_ROUTE, CLUSTER_CONTROL_INVITE_ROUTE,
    CLUSTER_CONTROL_JOIN_ROUTE, CLUSTER_CONTROL_LEAVE_ROUTE, CLUSTER_CONTROL_OBJECT_VERSION_ID,
    CLUSTER_CONTROL_REMOVE_ROUTE, CLUSTER_CONTROL_RENAME_ROUTE, CLUSTER_CONTROL_ROLE_ROUTE,
    CLUSTER_CONTROL_RPC_CATEGORY, CLUSTER_CONTROL_RPC_ROUTE_POLICIES, CLUSTER_CONTROL_RPC_ROUTES,
    CLUSTER_CONTROL_STATE_AUTHORITY_ROUTE, ClusterControlInitRequest, ClusterControlInitResponse,
    ClusterControlInviteRequest, ClusterControlInviteResponse, ClusterControlJoinMigrationSummary,
    ClusterControlJoinRequest, ClusterControlJoinResponse, ClusterControlLeaveRequest,
    ClusterControlLeaveResponse, ClusterControlRemoveRequest, ClusterControlRemoveResponse,
    ClusterControlRemovedMember, ClusterControlRenameRequest, ClusterControlRenameResponse,
    ClusterControlRoleRequest, ClusterControlRoleResponse, ClusterControlRoutePolicy,
    ClusterControlRpcPayload, ClusterControlStateAuthorityRequest,
    ClusterControlStateAuthorityResponse, cluster_control_init_response_rpc_payload,
    cluster_control_init_rpc_payload, cluster_control_invite_response_rpc_payload,
    cluster_control_invite_rpc_payload, cluster_control_join_response_rpc_payload,
    cluster_control_join_rpc_payload, cluster_control_leave_response_rpc_payload,
    cluster_control_leave_rpc_payload, cluster_control_remove_response_rpc_payload,
    cluster_control_remove_rpc_payload, cluster_control_rename_response_rpc_payload,
    cluster_control_rename_rpc_payload, cluster_control_role_response_rpc_payload,
    cluster_control_role_rpc_payload, cluster_control_route_requires_runtime_restart,
    cluster_control_rpc_route_policy, cluster_control_state_authority_response_rpc_payload,
    cluster_control_state_authority_rpc_payload, decode_cluster_control_init_rpc_request_body,
    decode_cluster_control_init_rpc_response_body, decode_cluster_control_invite_rpc_request_body,
    decode_cluster_control_invite_rpc_response_body, decode_cluster_control_join_rpc_request_body,
    decode_cluster_control_join_rpc_response_body, decode_cluster_control_leave_rpc_request_body,
    decode_cluster_control_leave_rpc_response_body, decode_cluster_control_remove_rpc_request_body,
    decode_cluster_control_remove_rpc_response_body,
    decode_cluster_control_rename_rpc_request_body,
    decode_cluster_control_rename_rpc_response_body, decode_cluster_control_role_rpc_request_body,
    decode_cluster_control_role_rpc_response_body,
    decode_cluster_control_state_authority_rpc_request_body,
    decode_cluster_control_state_authority_rpc_response_body,
};
pub use control_service::{
    ClusterControlServiceFuture, ClusterControlServiceHost, HostedClusterControlService,
    register_cluster_control_service, validate_remote_cluster_control_authority,
};
pub use health::{
    CLUSTER_NETWORK_PING_ATTACHMENT_MAX_BYTES, ClusterDoctorFinding, ClusterHealthRow,
    ClusterNetworkPingPayloadMode, ClusterNetworkPingRequest, ClusterNetworkPingResponse,
    ClusterStateAuthorityHealth, ClusterStatusAsyncProbe, ClusterStatusProbe,
    cluster_doctor_findings, cluster_health_rows_with_probe, cluster_health_rpc_request,
    cluster_health_rpc_request_without_peer_views,
    cluster_health_rpc_request_without_recursive_peer_views, cluster_local_health_rpc_request,
    cluster_network_ping_attachment_rpc_request,
    cluster_network_ping_attachment_rpc_response_for_node, cluster_network_ping_response_for_node,
    cluster_network_ping_rpc_call_with_response_payload, cluster_network_ping_rpc_request,
    cluster_network_ping_rpc_request_with_response_payload, cluster_state_authority_health,
    current_cluster_health_rows_for_decoded_request_async, decode_cluster_health_rpc_response,
    decode_cluster_network_ping_rpc_request, decode_cluster_network_ping_rpc_response,
    handle_cluster_network_ping_rpc_request_for_node,
    handle_current_cluster_health_rpc_request_async, unknown_cluster_node_status,
    validate_cluster_network_ping_attachment_request,
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
#[cfg(any(test, feature = "test-support"))]
pub use invite_publish::sync_cluster_invitation_to_local_raft_unchecked;
pub use invite_publish::{
    ClusterInvitePublishHost, handle_cluster_invite_publish_request_async_with_host,
    publish_cluster_invitation_to_quorum_with_host, read_cluster_invitation_for_join_introducer,
    read_cluster_invitation_from_kv,
};
pub use join_registration::{
    ClusterJoinReconciliation, ClusterJoinReconciliationHost, ClusterJoinRegistrationAsyncHost,
    ClusterJoinRegistrationDriver, ClusterJoinRegistrationHost,
    ClusterLocalDaemonMembershipAsyncObserver, ClusterLocalDaemonMembershipObserver,
    accepted_cluster_join_state_from_kv_snapshot, cluster_join_challenge_rpc_request,
    handle_cluster_join_registration_request_async_with_host,
    handle_cluster_join_registration_request_with_host, join_channel_endpoint_for_accepted_state,
    persist_accepted_cluster_join_snapshot, reconcile_accepted_cluster_join_response,
    reconcile_cluster_join_with_introducers_with_host,
    record_forwarded_join_registration_acceptance_with_host,
    send_cluster_join_registration_over_membership_rpc,
};
pub use kv_local_store::{
    ClusterKvPathsExt, invalidate_cluster_kv_observed_read_models, kv_err,
    read_cluster_kv_latest_observed_entry, read_cluster_kv_latest_observed_prefix_entries,
    read_cluster_kv_latest_observed_snapshot,
    read_cluster_kv_latest_observed_snapshot_arc_if_present, read_cluster_kv_local_snapshot,
};
pub use kv_read_model::KvClusterDirectory;
pub use kv_read_model::{
    read_cluster_connectivity_graph_from_kv, read_cluster_members_from_kv,
    read_cluster_members_from_snapshot, read_cluster_node_statuses_from_kv,
};
pub use kv_service::{
    ClusterKvServiceFuture, ClusterKvServiceHost, HostedClusterKvService,
    fence_remote_cluster_kv_txn_request_with_entries, register_cluster_kv_service,
    remote_cluster_kv_authorization_keys,
};
pub use kv_snapshot::{
    ClusterKvSnapshotRefreshHost, ClusterKvSnapshotRefreshOptions, ClusterKvSnapshotRefreshOutcome,
    refresh_current_cluster_kv_store_with_host,
    refresh_current_cluster_kv_store_with_local_storage_and_transport,
    refresh_current_cluster_kv_store_with_outcome,
};
pub use kv_snapshot_observer::{
    ClusterKvCommittedDeltaSubscription, ClusterKvCommittedSnapshotSubscription,
    subscribe_cluster_kv_committed_deltas, subscribe_cluster_kv_committed_observations,
    subscribe_cluster_kv_committed_snapshots,
};
pub use kv_txn_quorum::{
    ClusterKvTxnQuorumAsyncHost, ClusterKvTxnQuorumHost,
    commit_cluster_kv_txn_compact_with_quorum_response_async_with_host,
    commit_cluster_kv_txn_with_quorum_response_async_with_host,
    commit_cluster_kv_txn_with_quorum_response_with_host,
    handle_cluster_kv_authoritative_snapshot_request_async_with_host,
    handle_cluster_kv_txn_request_async_with_host,
    handle_cluster_kv_txn_request_compact_async_with_host,
    handle_forwarded_cluster_kv_txn_rpc_request_compact_async_with_host,
    read_cluster_kv_authoritative_complete_range_prefix_async_with_host,
    read_cluster_kv_authoritative_entries_async_with_host,
    read_cluster_kv_authoritative_entry_async_with_host,
    read_cluster_kv_authoritative_range_prefix_async_with_host,
    read_cluster_kv_authoritative_snapshot_async_with_host,
    read_cluster_kv_authoritative_snapshot_with_host,
    read_cluster_kv_request_outcome_async_with_host,
};
pub use local_membership::{
    ClusterLocalDaemonMembershipObservation, ClusterLocalDaemonMembershipRefresh,
    apply_cluster_node_role, assign_local_daemon_member_public_keys, cluster_status_summary_label,
    ensure_daemon_cluster_members, merge_cluster_join_registration,
    prepare_pending_cluster_join_state, refresh_cluster_connectivity_environment,
    refresh_cluster_connectivity_environment_async, refresh_local_daemon_membership_from_disk,
    refresh_local_daemon_membership_from_disk_async,
};
pub use local_state_store::{
    materialize_cluster_remote_endpoints_async, notify_local_cluster_state_changed,
    project_committed_cluster_raft_config, read_cluster_consensus_state, read_cluster_state,
    read_cluster_state_async, read_cluster_state_cached, read_cluster_state_cached_async,
    read_cluster_state_cached_shared, read_cluster_state_without_kv,
    read_cluster_state_without_kv_async, read_cluster_state_without_kv_cached_shared,
    subscribe_local_cluster_state_changes, write_cluster_state, write_cluster_state_async,
};
pub use maintenance_reactor::{
    ClusterMaintenanceFuture, ClusterMaintenanceReactorConfig, ClusterMaintenanceReactorHost,
    run_cluster_maintenance_reactor,
};
pub use member_mutation::{
    ClusterMemberMutationAsyncHost, ClusterMemberMutationDriver, ClusterMemberMutationHost,
    ClusterMemberRemoval, ClusterNodeNameUpdate, ClusterNodeRoleUpdate,
    ClusterRaftConfigReconciliationReport, ClusterStateAuthorityUpdate,
    MAX_CLUSTER_RAFT_CONFIG_RECONCILIATIONS_PER_PASS, add_cluster_raft_learner_async_with_host,
    bootstrap_single_voter_cluster_state_to_local_kv,
    cleanup_legacy_shadow_daemon_members_with_host, cluster_state_sync_txn,
    reconcile_cluster_raft_config_changes_bounded_with_host,
    reconcile_cluster_raft_config_changes_with_host, remove_cluster_members_with_host,
    sync_cluster_state_to_kv_async_with_host, sync_cluster_state_to_kv_with_host,
    sync_cluster_state_to_local_kv_unchecked, update_cluster_node_name_with_host,
    update_cluster_node_role_with_host, update_cluster_state_authority_with_host,
};
pub use member_publish::{
    ClusterLocalMemberPublishAsyncHost, ClusterLocalMemberPublishHost,
    ClusterLocalMemberPublishOptions, ClusterMemberPublishHost,
    handle_cluster_member_publish_request_async_with_host,
    publish_local_members_to_quorum_if_needed_async_with_host,
    publish_local_members_to_quorum_if_needed_with_host,
};
pub use membership_rpc::{
    ClusterAuthenticatedPeer, ClusterInvitePublishRequest, ClusterInvitePublishResponse,
    ClusterJoinRaftAnchor, ClusterJoinRegistrationRequest, ClusterJoinRegistrationResponse,
    ClusterMemberPublishRequest, ClusterMemberPublishResponse,
    handle_cluster_membership_rpc_request_with_hosts,
};
pub use membership_service::{
    ClusterMembershipServiceFuture, ClusterMembershipServiceHost, HostedClusterMembershipService,
    register_cluster_membership_service,
};
pub use migration::{
    ClusterMigrationKvImportReport, ClusterMigrationPlanReport, ClusterMigrationQuarantine,
    cluster_migration_kv_import_report_path, cluster_storage_has_live_artifacts,
    import_node_local_configs_from_source_kv_with_host, plan_cluster_migration_with_host,
    quarantine_cluster_storage_for_leave, quarantine_cluster_storage_for_migration,
    rollback_cluster_storage_quarantine, write_cluster_migration_record_to_source_kv_with_host,
    write_local_cluster_migration_record,
};
pub use network_maintenance::{
    CLUSTER_NETWORK_MAINTENANCE_FAILURE_BACKOFF,
    CLUSTER_NETWORK_MAINTENANCE_FAILURE_INITIAL_BACKOFF,
    CLUSTER_NETWORK_MAINTENANCE_STARTUP_SETTLE_DURATION,
    CLUSTER_UNRENEWED_CONNECTIVITY_EXPIRY_GRACE, ClusterConnectivityProbePlanAction,
    ClusterNetworkMaintenanceCadence, ClusterNetworkMaintenanceDueClaims,
    ClusterPeerVerificationCadence, ClusterPeerVerificationEpisode, ClusterPeerVerificationRound,
    ClusterPeerVerificationSettlement, cluster_connectivity_probe_plan_action,
    cluster_connectivity_probe_result_needs_followup, cluster_network_due_class_failure_backoff,
};
pub use network_service::{
    ClusterNetworkServiceFuture, ClusterNetworkServiceHost, HostedClusterNetworkService,
    handle_cluster_network_ping_service_request_for_node, register_cluster_network_service,
};
pub use publication_cache::{
    CLUSTER_PUBLICATION_CACHE_MAX_CONNECTIVITY_RECORDS,
    CLUSTER_PUBLICATION_CACHE_MAX_CONNECTIVITY_SOURCES, CLUSTER_PUBLICATION_CACHE_MAX_NODE_RECORDS,
    CLUSTER_PUBLICATION_CACHE_MAX_RECORDS_PER_SOURCE, ClusterPublicationCache,
    ClusterPublicationCacheLimits,
};
pub use raft_fanout::{
    ClusterKvCommittedFanoutAsyncHost, claim_cluster_raft_peer_catch_up,
    cluster_raft_peer_is_behind, finish_cluster_raft_peer_catch_up,
    record_cluster_raft_peer_append_results, replicate_cluster_kv_to_node_async_with_host,
    spawn_cluster_kv_committed_fanout_worker_on_runtime,
};
pub use raft_quorum::{
    ClusterKvQuorumAsyncHost, ClusterRaftQuorumBatch, ClusterRaftQuorumBatchOutcomes,
    ClusterRaftQuorumCommitHost,
};
pub use raft_rpc::{
    handle_cluster_kv_consensus_rpc_request_async, handle_cluster_raft_append_request_async,
    handle_cluster_raft_vote_request_async,
};
pub use service_providers::resolve_cluster_service_providers;
pub use state_authority_reachability::{
    ClusterStateAuthorityQuorumStatus, ClusterStateAuthorityReachabilityAsyncHost,
    ClusterStateAuthorityReachabilityHost, ClusterStateAuthorityReachabilityOptions,
    cluster_has_local_quorum_async_with_host, cluster_has_local_quorum_with_host,
    cluster_reachable_state_authority_count_async_with_host,
    cluster_reachable_state_authority_count_with_host,
    cluster_state_authority_quorum_status_async_with_host,
    cluster_state_authority_quorum_status_for_config_with_host,
    cluster_state_authority_quorum_status_with_host, local_cluster_state_authority_member,
};
pub use status_publish::{
    ClusterNetworkInfoPublishAsyncHost, ClusterNetworkInfoPublishHost,
    ClusterNodePresencePublishMatches, ClusterNodeStatusPublishAsyncHost,
    ClusterNodeStatusPublishHost, cluster_node_presence_publish_matches_async_with_host,
    cluster_node_presence_publish_matches_with_host, cluster_node_status_publish_record_matches,
    publish_cluster_network_info_to_quorum_async_with_host,
    publish_cluster_network_info_to_quorum_with_host,
    publish_cluster_node_status_to_quorum_async_with_host,
    publish_cluster_node_status_to_quorum_with_host,
};

use sinomo_storage::{ObjectSchema, StorageContractError, StorageLayout, StorageRing};

const STORAGE_CONTRACT_OBJECT_VERSION: u16 = 1;

/// Registers the durable storage paths owned by the cluster control plane.
pub fn register_storage_paths(layout: &mut StorageLayout) -> Result<(), StorageContractError> {
    layout.register_escape_path(
        "sinomo-cluster",
        StorageRing::R1,
        "cluster/migrations",
        "retained single-node migration receipts and quarantined original cluster storage",
    )?;
    layout.register_sinomo_object(
        "sinomo-cluster",
        StorageRing::R1,
        "cluster/state.sobj",
        ObjectSchema::new("ClusterBootstrapState", STORAGE_CONTRACT_OBJECT_VERSION)
            .field("cluster_id", "ClusterId")
            .field("local_node_id", "NodeId")
            .field("members", "Vec<ClusterMemberRecord>"),
    )?;
    layout.register_binary("sinomo-cluster", StorageRing::R1, "cluster/state.sobj.lock")?;
    Ok(())
}

/// Installs the KV-backed [`sinomo_network::ClusterDirectory`] as the process
/// directory provider so routing and endpoint selection see cluster state.
pub fn install_kv_cluster_directory() {
    sinomo_network::set_cluster_directory_provider(std::sync::Arc::new(|paths| {
        std::sync::Arc::new(KvClusterDirectory::new(paths.clone()))
    }));
}

pub(crate) mod prelude {
    pub(crate) use serde::{Deserialize, Serialize};
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) use sinomo_base::ClusterCertificate;

    pub(crate) use sinomo_base::{
        CapabilityName, ClusterBootstrapState, ClusterEndpointPrivacy, ClusterEndpointRecord,
        ClusterEndpointSource, ClusterInvitationChannel, ClusterJoinInvite, ClusterMemberRecord,
        ClusterMemberRole, ClusterMemberState, ClusterNodeAvailability, ClusterNodeRole,
        ClusterNodeStatusRecord, ClusterTransportKind, TrustDomain,
    };

    #[allow(unused_imports)]
    pub(crate) use sinomo_kv::{
        ClusterKvWatchEventsRequest, ClusterKvWatchEventsResponse, InProcessKvStore, KvCommand,
        KvError, KvEvent, KvOp, KvSnapshot, KvTxn, KvTxnResponse, RaftRequestVoteRequest,
        RaftRequestVoteResponse,
    };
    #[cfg(test)]
    pub(crate) use sinomo_local::SinomoDataDir;
    pub(crate) use sinomo_network::ClusterEndpointId;
    pub(crate) use sinomo_node::ClusterId;
    pub(crate) use sinomo_node::ClusterInviteId;

    pub(crate) use sinomo_node::{NodeId, shadow_daemon_node_id};

    pub(crate) use std::collections::{BTreeMap, BTreeSet};
    pub(crate) use std::time::Duration;
    #[allow(unused_imports)]
    pub(crate) use thiserror::Error;
    pub(crate) use time::OffsetDateTime;
}

pub use connectivity_publish::cluster_connectivity_full_sync_batch_ops_from_snapshot;
// test-support re-exports (crate internals for sibling integration tests)
#[cfg(any(test, feature = "test-support"))]
pub use connectivity_publish::{
    cluster_connectivity_full_sync_batch_ops, cluster_connectivity_sync_is_noop,
};
#[cfg(any(test, feature = "test-support"))]
pub use control_plane_options::{
    ClusterRaftAppendReplicationOptions, ClusterRaftCommittedFanoutOptions,
    ClusterRaftLeaderElectionOptions,
};
#[cfg(any(test, feature = "test-support"))]
pub use health::{
    cluster_health_rows_with_async_probe, cluster_health_rpc_request_is_local_scope,
    handle_cluster_health_rpc_request, handle_cluster_health_rpc_request_async,
    handle_current_cluster_health_rpc_request,
};
#[cfg(any(test, feature = "test-support"))]
pub use invite::{
    CLUSTER_INVITE_CHALLENGE_PROTOCOL, cluster_invite_challenge_signing_payload,
    verify_cluster_invite_challenge_proof,
};
#[cfg(any(test, feature = "test-support"))]
pub use invite_publish::{
    cluster_invitation_publish_txn_from_kv, handle_cluster_invite_publish_request_with_host,
};
#[cfg(any(test, feature = "test-support"))]
pub use join_registration::prepare_cluster_join_registration_response;
#[cfg(any(test, feature = "test-support"))]
pub use keyspace::cluster_connectivity_key;
#[cfg(any(test, feature = "test-support"))]
pub use keyspace::cluster_network_info_key;
#[cfg(any(test, feature = "test-support"))]
pub use keyspace::{
    cluster_connectivity_source_prefix, cluster_endpoint_key, cluster_endpoint_prefix,
};
#[cfg(any(test, feature = "test-support"))]
pub use kv_forward::forward_cluster_kv_txn_to_state_authority_async_with_host;
#[cfg(any(test, feature = "test-support"))]
pub use kv_forward::{
    ClusterKvTxnForwardHost, ClusterKvTxnForwardOptions,
    forward_cluster_kv_txn_to_state_authority_with_host,
};
#[cfg(any(test, feature = "test-support"))]
pub use kv_local_store::write_cluster_kv_snapshot;
#[cfg(any(test, feature = "test-support"))]
pub use kv_local_store::{
    LocalClusterRaftCommandStorage, write_imported_cluster_kv_snapshot_if_changed,
};
#[cfg(any(test, feature = "test-support"))]
pub use kv_local_store::{
    append_cluster_raft_command, append_cluster_raft_command_uncommitted,
    append_cluster_raft_command_with_response, apply_cluster_raft_append,
    cached_cluster_kv_reader_node_id_for_test, cluster_kv_cached_snapshot_reads_for_test,
    cluster_kv_metadata_cache_capacity_for_test, cluster_kv_reader_node_id_cache_contains_for_test,
    cluster_raft_wal_paths, open_cluster_raft_kv_node, open_cluster_raft_kv_node_async,
    read_cluster_kv_current_snapshot, read_cluster_kv_prefix_entries,
    read_cluster_kv_prefix_entries_limited,
    read_cluster_kv_prefix_entries_limited_by_payload_bytes, read_cluster_kv_snapshot,
    read_cluster_kv_store, record_cluster_kv_reader_node_id_cache_for_test,
    refresh_cluster_kv_reader_node_id_cache_for_test,
    remove_cluster_kv_reader_node_id_cache_entries_under_root_for_test,
    reset_cluster_kv_cached_snapshot_reads_for_test,
};
#[cfg(any(test, feature = "test-support"))]
pub use kv_read_model::{read_cluster_endpoints_from_kv, read_cluster_network_info_from_kv};
#[cfg(any(test, feature = "test-support"))]
pub use kv_semantics::cluster_semantic_txn_no_effect_response;
#[cfg(any(test, feature = "test-support"))]
pub use kv_txn_quorum::handle_cluster_kv_txn_request_with_host;
#[cfg(any(test, feature = "test-support"))]
pub use local_membership::{
    apply_local_daemon_membership_observation, ensure_main_daemon_cluster_member,
    prepare_cluster_join_registration_state_with_local_daemon_members,
};
#[cfg(any(test, feature = "test-support"))]
pub use local_state_store::remove_local_cluster_state;
#[cfg(any(test, feature = "test-support"))]
pub use member_mutation::{
    sync_cluster_certificate_to_kv, sync_cluster_state_to_local_raft_unchecked,
};
#[cfg(any(test, feature = "test-support"))]
pub use member_publish::{
    cluster_member_publish_authorized_for_signer, cluster_member_publish_matches_kv,
    cluster_member_publish_txn, cluster_member_publish_txn_from_snapshot,
    handle_cluster_member_publish_request_with_host,
};
#[cfg(any(test, feature = "test-support"))]
pub use member_store::MAX_CLUSTER_MEMBER_ENDPOINT_RECORDS;
#[cfg(any(test, feature = "test-support"))]
pub use member_store::{apply_cluster_raft_role, cluster_member_put_command};
#[cfg(any(test, feature = "test-support"))]
pub use member_store::{
    cluster_member_removal_txns_from_store, cluster_member_update_ops_from_store,
    cluster_raft_config_from_store,
};
#[cfg(any(test, feature = "test-support"))]
pub use membership_forward::{
    ClusterMembershipRpcForwardHost,
    forward_cluster_join_challenge_rpc_to_state_authority_async_with_host,
    forward_cluster_join_challenge_rpc_to_state_authority_with_host,
    forward_cluster_membership_rpc_to_state_authority_with_host,
};
#[cfg(any(test, feature = "test-support"))]
pub use raft_election::{
    ClusterRaftVoteTransportHost, elect_cluster_raft_leader_with_host,
    locked_set_cluster_raft_leader,
};
#[cfg(any(test, feature = "test-support"))]
pub use raft_fanout::ClusterRaftCommittedFanoutAsyncHost;
#[cfg(any(test, feature = "test-support"))]
pub use raft_fanout::{
    claim_next_cluster_raft_fanout_pass_for_test,
    fan_out_committed_cluster_raft_command_async_with_host,
};
#[cfg(any(test, feature = "test-support"))]
pub use raft_quorum::commit_cluster_raft_command_with_quorum_async_with_host;
#[cfg(any(test, feature = "test-support"))]
pub use raft_quorum::{
    ClusterRaftQuorumAsyncHost, ClusterRaftQuorumTransportHost,
    commit_cluster_raft_command_with_local_storage_and_transport,
    commit_cluster_raft_command_with_quorum_driver,
};
pub use raft_quorum::{mark_cluster_kv_node_started, mark_cluster_kv_node_stopping};
#[cfg(any(test, feature = "test-support"))]
pub use raft_replication::{
    ClusterRaftAppendReplicationAsyncHost, ClusterRaftAppendReplicationHost,
    replicate_cluster_kv_to_endpoint_async_with_host,
    replicate_cluster_raft_to_endpoint_async_with_host,
    replicate_cluster_raft_to_endpoint_with_host,
    replicate_cluster_raft_to_endpoint_with_local_host,
};
#[cfg(any(test, feature = "test-support"))]
pub use raft_rpc::{handle_cluster_raft_append_request, handle_cluster_raft_rpc_request};
#[cfg(any(test, feature = "test-support"))]
pub use status_publish::{
    cluster_network_info_publish_matches_with_host, cluster_node_status_publish_matches_with_host,
    sync_cluster_network_info_to_kv, sync_cluster_node_status_to_kv,
};
