mod auth;
mod client;
mod cluster;
mod control;
mod dfs;
mod error;
mod install;
mod paths;
mod process;
mod rollout;
mod status;

pub use auth::{
    AuthenticatedDaemonCommand, ClusterInviteChallengeProof, ClusterInviteChallengeRequest,
    DaemonSignerAuthority, SignedDaemonCommand, authenticate_daemon_control_request,
    authorize_authenticated_daemon_command, create_cluster_invite_challenge_proof,
    daemon_rpc_body_sha256, daemon_rpc_command, sign_daemon_rpc_request_for_local_node,
    sign_daemon_rpc_request_from_data_dir, verify_cluster_invite_challenge_proof,
};
pub use client::{
    fetch_cluster_kv_snapshot, fetch_cluster_kv_snapshot_from_endpoint,
    fetch_daemon_status_from_address, fetch_daemon_status_from_endpoint,
    send_cluster_join_registration, send_cluster_join_registration_to_channel,
    send_cluster_join_registration_to_endpoint, send_cluster_kv_txn, send_cluster_kv_watch_events,
    send_cluster_member_publish, send_cluster_raft_append, send_cluster_raft_vote,
    send_daemon_finalize_shadow_rollout_request, send_daemon_rollout_binary,
    send_daemon_rollout_binary_to_endpoint, send_daemon_rollout_build,
    send_daemon_rollout_build_to_endpoint, send_daemon_rollout_diagnostics, send_daemon_rpc,
    send_daemon_rpc_to_endpoint, send_daemon_shutdown_request,
};
pub use cluster::{
    CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD, CLUSTER_CONNECTIVITY_TTL_SECONDS,
    CLUSTER_HEALTH_PROBE_TIMEOUT, CLUSTER_NETWORK_INFO_TTL_SECONDS, ClusterConnectivityProbeState,
    ClusterDoctorFinding, ClusterHealthRow, ClusterJoinReconciliation, ClusterMemberRemoval,
    ClusterNetworkMaintenance, ClusterNodeRoleUpdate, ClusterRaftRoleUpdate,
    DAEMON_CLUSTER_DISCOVERY_INTERVAL, DAEMON_CLUSTER_JOIN_STATUS_TIMEOUT,
    DAEMON_CLUSTER_MEMBER_PUBLISH_TIMEOUT, DAEMON_CLUSTER_NETWORK_PROBE_INTERVAL,
    DAEMON_CLUSTER_PUBLISH_INTERVAL, DAEMON_CLUSTER_RPC_TIMEOUT, DAEMON_CLUSTER_TICK_INTERVAL,
    advertise_daemon_iroh_endpoint, assign_local_node_public_keys, build_cluster_join_invite,
    cluster_doctor_findings, cluster_has_local_quorum, cluster_health_rows,
    cluster_invite_publish_targets, cluster_paths_from_daemon_paths, cluster_peer_members,
    cluster_reachable_voter_count, commit_cluster_kv_txn_with_quorum,
    commit_cluster_raft_command_with_quorum, daemon_cluster_registration_if_initialized,
    default_cluster_invite_introducers, elect_cluster_raft_leader, ensure_daemon_status_cluster,
    expire_cluster_network_records_in_kv, handle_daemon_cluster_invite_publish_request,
    handle_daemon_cluster_join_request, handle_daemon_cluster_kv_range_prefix,
    handle_daemon_cluster_kv_snapshot, handle_daemon_cluster_kv_txn_request,
    handle_daemon_cluster_kv_watch_drain_request, handle_daemon_cluster_kv_watch_events_request,
    handle_daemon_cluster_kv_watch_prefix_request, handle_daemon_cluster_member_publish_request,
    handle_daemon_cluster_raft_append_request, handle_daemon_cluster_raft_vote_request,
    initialize_local_cluster, local_daemon_cluster_options, prepare_cluster_join_retry_state,
    prepare_local_cluster_state, prepare_pending_cluster_join_state,
    publish_cluster_invitation_to_quorum, publish_local_members_to_quorum_if_needed,
    publish_local_members_via_voter, put_cluster_kv_control_record,
    read_cluster_connectivity_graph_from_kv, read_cluster_kv_snapshot, read_local_cluster_state,
    read_local_cluster_state_without_kv, read_local_iroh_endpoint_record,
    reconcile_cluster_join_with_introducers, refresh_daemon_node_status_in_kv,
    register_daemon_cluster_node, remove_cluster_members, remove_local_cluster_state,
    replicate_cluster_raft_to_endpoint, start_daemon_iroh_proxy_if_clustered,
    start_shadow_daemon_status_publisher, sync_cluster_connectivity_records_to_kv,
    sync_cluster_network_info_to_kv, sync_cluster_node_status_to_kv, sync_cluster_state_to_kv,
    update_cluster_node_role, update_cluster_raft_role, write_cluster_kv_snapshot_if_changed,
    write_local_cluster_state,
};
pub use control::{
    DAEMON_CLIENT_READ_TIMEOUT, DAEMON_CLIENT_WRITE_TIMEOUT, DAEMON_COMMAND_MAX_BYTES,
    handle_authenticated_daemon_connection, handle_daemon_connection, run_daemon_control_loop,
    write_daemon_rpc_response_frame,
};
pub use dfs::{
    ensure_default_distributed_dfs, ensure_default_local_dfs_expose,
    handle_daemon_dfs_expose_config,
};
pub use error::DaemonError;
pub use install::{
    DaemonStopBeforeInstallStart, DaemonStopBeforeInstallStatus, DaemonStopIfRunningStatus,
    INSTALL_MAIN_BOOT_NAME, INSTALL_SHADOW_BOOT_NAME, InstallLayout, InstalledDaemonInstallPlan,
    InstalledDaemonInstallPlanRequest, InstalledDaemonPaths, InstalledDaemonStartSpec,
    InstalledDaemonStartStatus, ManagedSourceRepoInstallStatus, activate_boot_entries,
    boot_entry_content, boot_entry_path, deactivate_boot_entries, default_boot_dir,
    ensure_install_layout_dirs, git_is_available, install_layout_exists,
    install_managed_source_repo, installed_daemon_install_stop_reason, installed_daemon_paths,
    installed_daemon_start_specs, installed_daemons_need_restart, managed_source_git_status_lines,
    prepare_installed_daemon_install_plan, remove_boot_entry,
    start_installed_sinomo_daemon as daemon_install_start_installed_sinomo_daemon,
    stop_daemon_before_install as daemon_install_stop_daemon_before_install,
    stop_daemon_if_running as daemon_install_stop_daemon_if_running, write_boot_entry,
    write_install_boot_entries,
};
#[cfg(not(windows))]
pub use install::{
    LocalBinSymlinkStatus, SystemctlUserOutcome, ensure_local_bin_symlink,
    local_bin_symlink_is_current, systemctl_user_best_effort,
};
#[cfg(windows)]
pub use install::{UserPathUpdate, ensure_windows_user_path_contains};
pub use paths::{DaemonPaths, daemon_paths_from_data_dir, shadow_daemon_paths_from_data_dir};
pub use process::{
    CandidateMainDiagnostics, main_exit_observation_from_status, prepare_background_command,
    spawn_candidate_main_daemon_process, spawn_main_daemon_process, start_daemon_command_process,
};
pub use rollout::{
    LocalRolloutMainActivationPlan, LocalShadowRolloutRestartPlan, PendingShadowMainExit,
    SHADOW_MAIN_BINARY_ENV, SHADOW_MAIN_HOST_ENV, SHADOW_MAIN_PORT_ENV, SHADOW_START_DELAY_MS_ENV,
    ShadowMainExit, ShadowMainRolloutMonitor, ShadowMainStart,
    cluster_rollout_should_use_remote_build, daemon_paths_are_shadow,
    ensure_rollout_target_platform_matches_local, handle_daemon_finalize_shadow_rollout,
    handle_daemon_rollout_binary_request, handle_daemon_rollout_build_request,
    handle_daemon_rollout_diagnostics, handle_pending_shadow_main_process_exit,
    handle_shadow_main_process_exit, prepare_local_rollout_main_activation,
    prepare_local_shadow_rollout_restart, promote_shadow_main_rollout,
    recover_failed_shadow_main_rollout, rollout_target_platform_matches_local,
    run_shadow_daemon_supervision_loop, shadow_main_binary_from_env, shadow_main_options_from_env,
    sleep_for_shadow_start_delay, spawn_shadow_for_cluster_rollout, stage_rollout_main_activation,
    start_shadow_main_if_needed, wait_for_local_shadow_rollout_completion,
    wait_for_main_daemon_health, wait_for_shadow_rollout_health,
    write_rollout_main_failure_diagnostics,
};
pub use status::{
    CLUSTER_NODE_STATUS_STALE_AFTER_SECONDS, DAEMON_MAIN_CLUSTER_ROLE, DAEMON_SHADOW_CLUSTER_ROLE,
    DaemonClusterProcess, DaemonClusterRegistration, DaemonClusterStatus, DaemonLock,
    DaemonProbeStatus, DaemonRunOptions, DaemonStatus, SINOMO_DAEMON_DEFAULT_HOST,
    SINOMO_DAEMON_DEFAULT_PORT, SINOMO_SHADOW_DAEMON_DEFAULT_PORT,
    authenticated_signer_matches_cluster_node, cluster_node_status_from_daemon_status,
    cluster_node_status_is_stale, current_shadow_daemon_options,
    daemon_advertised_endpoints_for_state, daemon_advertised_endpoints_from_ips,
    daemon_bind_address, daemon_cluster_options_from_status, daemon_cluster_process_from_paths,
    daemon_cluster_registration, daemon_cluster_status, daemon_cluster_status_matches,
    daemon_connect_address, daemon_options_from_status, daemon_package_version,
    daemon_probe_status_needs_restart_for_options, daemon_status_matches_options,
    daemon_status_matches_probe, default_main_daemon_status, ensure_daemon_cluster_members,
    invite_introducers_from_daemon_status, probe_daemon_status, probe_daemon_status_at_address,
    read_daemon_status, unknown_cluster_node_status, write_daemon_status,
};

pub fn register_storage_paths(
    layout: &mut sinomo_storage::StorageLayout,
) -> Result<(), sinomo_storage::StorageContractError> {
    use sinomo_storage::{ObjectSchema, STORAGE_CONTRACT_OBJECT_VERSION, StorageRing};

    for relative_path in ["daemon/status.sobj", "shadow-daemon/status.sobj"] {
        layout.register_sinomo_object(
            "sinomo-daemon",
            StorageRing::R1,
            relative_path,
            ObjectSchema::new("DaemonStatus", STORAGE_CONTRACT_OBJECT_VERSION)
                .field("pid", "u32")
                .field("address", "String")
                .field("started_at", "String")
                .field("cluster", "Option<DaemonClusterStatus>"),
        )?;
    }
    for relative_path in ["daemon/daemon.lock", "shadow-daemon/daemon.lock"] {
        layout.register_binary("sinomo-daemon", StorageRing::R1, relative_path)?;
    }
    for relative_path in ["daemon/auth-nonces.sobj", "shadow-daemon/auth-nonces.sobj"] {
        layout.register_sinomo_object(
            "sinomo-daemon",
            StorageRing::R1,
            relative_path,
            ObjectSchema::new(
                "BTreeMap<DaemonAuthNonceKey, Timestamp>",
                STORAGE_CONTRACT_OBJECT_VERSION,
            ),
        )?;
    }
    layout.register_escape_path(
        "sinomo-daemon",
        StorageRing::Application,
        "startup",
        "installed OS boot entries and launch scripts",
    )
}
