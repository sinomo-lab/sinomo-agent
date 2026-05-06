use super::{
    CliCommand, CliError, ClusterCliCommand, ClusterInitOptions, ClusterInviteOptions,
    ClusterJoinOptions, ClusterJoinRegistrationRequest, ClusterPaths, ClusterRaftRoleOptions,
    ClusterRoleOptions, ClusterRolloutBuildRequest, DaemonCliCommand, DaemonClusterStatus,
    DaemonPaths, DaemonProbeStatus, DaemonRunOptions, DaemonStatus, DfsCliCommand,
    DfsCommonOptions, DfsExposeCliCommand, DfsFilterOptions, DfsPinCliCommand, EvolutionJobStatus,
    EvolutionPaths, InstallLayout, InstallOptions, InteractionModel, InteractionRuntime,
    InteractionSession, InteractionSurface, InteractionSurfaceKind, ModelBackedInteractionHandler,
    ModelsCliCommand, ProviderCliCommand, ProviderName, RolloutCliCommand, RolloutMainActivation,
    RolloutOptions, SelfRolloutPaths, UninstallOptions, advertised_peer_route_targets,
    apply_cluster_node_role, apply_cluster_raft_role, authenticate_daemon_command,
    build_cluster_raft_append_request_from_next_index, cli_command_requires_cluster_state,
    cluster_connectivity_sync_ops, cluster_doctor_findings, cluster_has_local_quorum,
    cluster_init_with_paths, cluster_invite_publish_targets, cluster_invite_with_paths,
    cluster_join_with_paths, cluster_peer_members, cluster_raft_known_leader_id,
    cluster_state_sync_txn, cluster_sync_targets, compact_default_invite_introducers,
    create_source_repo_bundle, daemon_advertised_endpoints_for_state, daemon_lan_endpoint,
    daemon_package_version, daemon_status_matches_options, decode_cluster_join_invite,
    elect_cluster_raft_leader, encode_cluster_join_invite, ensure_rollout_layout,
    expire_cluster_network_records_in_kv, handle_daemon_cluster_join,
    handle_daemon_cluster_kv_range_prefix, handle_daemon_cluster_kv_snapshot,
    handle_daemon_cluster_kv_txn, handle_daemon_cluster_kv_watch_drain,
    handle_daemon_cluster_kv_watch_events, handle_daemon_cluster_kv_watch_prefix,
    handle_daemon_cluster_raft_append, handle_daemon_cluster_raft_vote, handle_daemon_connection,
    handle_daemon_rollout_build, install_should_update_binary, install_with_layout,
    invite_channels_for_join_registration, invite_communication_channels,
    invite_introducer_endpoint, invite_introducer_endpoint_from_state, is_terminal_status,
    main_daemon_capabilities, managed_source_status, models_default_with_data_dir,
    normalize_model_provider, parse_cli_command, parse_cluster_command,
    parse_cluster_join_invite_argument, parse_daemon_command, parse_dfs_command,
    parse_install_options, parse_model_default_options, parse_models_list_options,
    parse_propose_options, parse_provider_command, parse_rollout_command, parse_slash_command,
    parse_uninstall_options, parse_usage_options, read_cluster_kv_snapshot,
    read_cluster_members_from_kv, read_cluster_node_statuses_from_kv, read_cluster_rollout_status,
    read_cluster_state, refresh_daemon_node_status_in_kv, run_cui_with_handler_io,
    run_cui_with_handler_io_and_daemon, run_evolve_command, run_shadow_daemon_with_paths,
    send_daemon_command_raw, shadow_daemon_node_id, sign_cluster_join_invite,
    sign_daemon_command_from_data_dir, stage_rollout_main_activation, sync_cluster_state_to_kv,
    uninstall_with_layout, write_json,
};
use base64::Engine;
use sinomo_lm::{
    LanguageModelError, LanguageModelRequest, LanguageModelResponse, LanguageModelUsage,
};
use sinomo_runtime::core::{
    CapabilityName, ClusterBootstrapState, ClusterEndpointId, ClusterEndpointPrivacy,
    ClusterEndpointRecord, ClusterEndpointSource, ClusterId, ClusterInviteId, ClusterJoinInvite,
    ClusterMemberRecord, ClusterMemberRole, ClusterMemberState, ClusterNodeAvailability,
    ClusterNodeRole, ClusterTransportKind, EvolutionJobId, NodeId, SessionId, TrustDomain,
    TrustRing,
};
use sinomo_runtime::events::{EventStore, InMemoryEventStore, ReplayQuery};
use sinomo_runtime::kv::{InProcessKvStore, KvCompare, KvOp, KvRevision, KvTxn};
use sinomo_runtime::local::SinomoDataDir;
use std::io::{BufRead, Cursor, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use time::OffsetDateTime;

fn authenticated_test_command(
    command: impl Into<String>,
    signer: super::DaemonSignerAuthority,
    signer_node_id: impl Into<String>,
    signer_public_key: impl Into<String>,
) -> super::AuthenticatedDaemonCommand {
    super::AuthenticatedDaemonCommand {
        command: command.into(),
        signer,
        signer_node_id: signer_node_id.into(),
        signer_public_key: signer_public_key.into(),
    }
}

fn start_one_shot_raft_append_responder(
    state: ClusterBootstrapState,
) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake raft responder");
    let address = listener.local_addr().expect("fake raft responder address");
    let handle = thread::spawn(move || {
        if let Ok((mut stream, _)) = listener.accept() {
            let mut line = String::new();
            let _ = std::io::BufReader::new(&mut stream).read_line(&mut line);
            let response = sinomo_runtime::network::ClusterRaftAppendResponse {
                accepted: true,
                detail: "fake raft append accepted".to_string(),
                state,
                append: sinomo_runtime::kv::RaftAppendEntriesResponse {
                    term: 1,
                    success: true,
                    match_index: 1,
                },
            };
            let body = serde_json::to_string(&response).expect("fake response json");
            let _ = stream.write_all(body.as_bytes());
        }
    });
    (address.to_string(), handle)
}

#[test]
fn provider_commands_parse_action_before_provider() {
    assert_eq!(
        parse_provider_command(&["auth".to_string(), "gemini".to_string(),])
            .expect("provider command"),
        ProviderCliCommand::Auth(ProviderName::Gemini)
    );
    assert_eq!(
        parse_provider_command(&["test".to_string(), "github-copilot".to_string(),])
            .expect("provider command"),
        ProviderCliCommand::Test(ProviderName::GitHubCopilot)
    );
    assert_eq!(
        parse_provider_command(&["auth".to_string(), "codex".to_string(),])
            .expect("provider command"),
        ProviderCliCommand::Auth(ProviderName::Codex)
    );
}

#[test]
fn provider_commands_reject_provider_before_action_order() {
    assert!(matches!(
        parse_provider_command(&["gemini".to_string(), "auth".to_string()]),
        Err(CliError::Usage)
    ));
}

#[test]
fn top_level_cli_parser_routes_subcommands() {
    assert!(matches!(
        parse_cli_command(&[]).expect("cui"),
        CliCommand::Cui
    ));
    assert!(matches!(
        parse_cli_command(&["help".to_string()]).expect("help"),
        CliCommand::Help
    ));
    assert!(matches!(
        parse_cli_command(&["daemon".to_string(), "status".to_string()]).expect("daemon"),
        CliCommand::Daemon(DaemonCliCommand::Status(_))
    ));
    assert!(matches!(
        parse_cli_command(&["daemon".to_string()]).expect("daemon help"),
        CliCommand::Daemon(DaemonCliCommand::Help)
    ));
    assert!(matches!(
        parse_cli_command(&["cluster".to_string(), "status".to_string()]).expect("cluster status"),
        CliCommand::Cluster(ClusterCliCommand::Status)
    ));
    assert!(matches!(
        parse_cli_command(&["cluster".to_string(), "health".to_string()]).expect("cluster health"),
        CliCommand::Cluster(ClusterCliCommand::Health)
    ));
    assert!(matches!(
        parse_cli_command(&["cluster".to_string(), "doctor".to_string()]).expect("cluster doctor"),
        CliCommand::Cluster(ClusterCliCommand::Doctor)
    ));
    assert!(matches!(
        parse_cli_command(&["install".to_string(), "help".to_string()]).expect("install help"),
        CliCommand::InstallHelp
    ));
    assert!(matches!(
        parse_cli_command(&["install".to_string(), "--no-start".to_string()]).expect("install"),
        CliCommand::Install(InstallOptions { start: false, .. })
    ));
    assert!(matches!(
        parse_cli_command(&["uninstall".to_string(), "--no-stop".to_string()]).expect("uninstall"),
        CliCommand::Uninstall(UninstallOptions { stop: false })
    ));
    assert!(matches!(
        parse_cli_command(&[
            "providers".to_string(),
            "test".to_string(),
            "openai".to_string()
        ])
        .expect("providers"),
        CliCommand::Providers(ProviderCliCommand::Test(ProviderName::OpenAi))
    ));
    assert!(matches!(
        parse_cli_command(&["models".to_string(), "list".to_string()]).expect("models"),
        CliCommand::Models(ModelsCliCommand::List(_))
    ));
    assert!(matches!(
        parse_cli_command(&["models".to_string()]).expect("models help"),
        CliCommand::Models(ModelsCliCommand::Help)
    ));
    assert!(matches!(
        parse_cli_command(&["model".to_string(), "list".to_string()]).expect("model alias"),
        CliCommand::Models(ModelsCliCommand::List(_))
    ));
    assert!(matches!(
        parse_cli_command(&[
            "usage".to_string(),
            "--provider".to_string(),
            "codex".to_string()
        ])
        .expect("usage"),
        CliCommand::Usage(_)
    ));
    assert!(matches!(
        parse_cli_command(&["providers".to_string()]).expect("providers help"),
        CliCommand::Providers(ProviderCliCommand::Help)
    ));
    assert!(matches!(
        parse_cli_command(&["rollout".to_string(), "status".to_string()]).expect("rollout"),
        CliCommand::Rollout(RolloutCliCommand::Status)
    ));
    assert!(matches!(
        parse_cli_command(&["dfs".to_string(), "status".to_string()]).expect("dfs"),
        CliCommand::Dfs(DfsCliCommand::Status(_))
    ));
    assert!(matches!(
        parse_cli_command(&["evolve".to_string()]).expect("evolve help"),
        CliCommand::Evolve(super::EvolveCliCommand::Help)
    ));
}

#[test]
fn install_parser_accepts_custom_daemon_ports() {
    assert_eq!(
        parse_install_options(&[
            "--host".to_string(),
            "127.0.0.1".to_string(),
            "--port".to_string(),
            "21400".to_string(),
            "--shadow-port".to_string(),
            "21401".to_string(),
            "--no-start".to_string(),
        ])
        .expect("install options"),
        InstallOptions {
            host: "127.0.0.1".to_string(),
            port: 21400,
            shadow_port: 21401,
            start: false,
        }
    );
    assert_eq!(
        parse_uninstall_options(&["--no-stop".to_string()]).expect("uninstall options"),
        UninstallOptions { stop: false }
    );
}

#[test]
fn cluster_sync_targets_skip_remote_local_only_endpoints() {
    let cluster_id = ClusterId::new("cluster_sync_targets").expect("cluster");
    let local_node = NodeId::new("node_sync_local").expect("node");
    let remote_node = NodeId::new("node_sync_remote").expect("node");
    let mut state = ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "sync targets",
        local_node,
        "local",
        "local-public-key",
        OffsetDateTime::now_utc(),
    );
    let mut remote = ClusterMemberRecord::local_voter(
        cluster_id,
        remote_node.clone(),
        "remote",
        "remote-public-key",
        OffsetDateTime::now_utc(),
    );
    remote
        .capabilities
        .push(CapabilityName::new("daemon.main.run").expect("capability"));
    remote.endpoints.push(ClusterEndpointRecord::loopback(
        remote_node.clone(),
        "127.0.0.1:21353",
    ));
    let mut lan_endpoint = ClusterEndpointRecord::loopback(remote_node, "192.168.1.10:21353");
    lan_endpoint.transport = ClusterTransportKind::LanTcp;
    lan_endpoint.privacy = ClusterEndpointPrivacy::Lan;
    remote.endpoints.push(lan_endpoint);
    state.members.push(remote);

    let targets = cluster_sync_targets(&state);

    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0].address, "192.168.1.10:21353");
}

#[test]
fn cluster_invite_publish_targets_prefer_tcp_voters_over_iroh_when_available() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-cli-invite-target-order-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
    let cluster_id = ClusterId::new("cluster_invite_target_order").expect("cluster");
    let local_node = NodeId::new("node_invite_local").expect("node");
    let voter_node = NodeId::new("node_invite_voter").expect("node");
    let mut state = ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "invite target order",
        local_node,
        "local",
        "local-public-key",
        OffsetDateTime::now_utc(),
    );
    let mut voter = ClusterMemberRecord::local_voter(
        cluster_id,
        voter_node.clone(),
        "remote voter",
        "remote-voter-public-key",
        OffsetDateTime::now_utc(),
    );
    voter.capabilities = main_daemon_capabilities(&ClusterMemberRole::Voter);
    voter.allowed_capabilities = voter.capabilities.clone();
    voter.endpoints = vec![
        ClusterEndpointRecord {
            endpoint_id: ClusterEndpointId::new("endpoint_invite_voter_iroh").expect("endpoint"),
            node_id: voter_node.clone(),
            transport: ClusterTransportKind::Iroh,
            address: "iroh://remote_voter_key?direct=203.0.113.10:21353".to_string(),
            source: ClusterEndpointSource::Rendezvous,
            privacy: ClusterEndpointPrivacy::InternetDirect,
            priority: 1,
            last_observed_latency_ms: None,
        },
        daemon_lan_endpoint(&voter_node, "10.14.1.11:21353".to_string(), 20),
    ];
    state.members.push(voter);

    let targets = cluster_invite_publish_targets(&paths, &state);

    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0].transport, ClusterTransportKind::LanTcp);
    assert_eq!(targets[0].address, "10.14.1.11:21353");
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn default_invite_compaction_keeps_iroh_fallback() {
    let node_id = NodeId::new("node_invite_compact").expect("node");
    let mut endpoints = vec![
        daemon_lan_endpoint(&node_id, "10.14.1.11:21353".to_string(), 0),
        daemon_lan_endpoint(&node_id, "10.14.1.12:21353".to_string(), 1),
        daemon_lan_endpoint(&node_id, "10.14.1.13:21353".to_string(), 2),
        ClusterEndpointRecord {
            endpoint_id: ClusterEndpointId::new("endpoint_invite_compact_iroh").expect("endpoint"),
            node_id,
            transport: ClusterTransportKind::Iroh,
            address: "iroh://invite_compact_key?relay=https://relay.example/".to_string(),
            source: ClusterEndpointSource::Rendezvous,
            privacy: ClusterEndpointPrivacy::InternetDirect,
            priority: 3,
            last_observed_latency_ms: None,
        },
    ];

    compact_default_invite_introducers(&mut endpoints);

    assert_eq!(endpoints.len(), 3);
    assert_eq!(endpoints[0].address, "10.14.1.11:21353");
    assert_eq!(endpoints[1].address, "10.14.1.12:21353");
    assert_eq!(endpoints[2].transport, ClusterTransportKind::Iroh);
}

#[test]
fn cluster_state_sync_txn_removes_stale_member_endpoint_keys_before_republishing() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-cli-endpoint-key-cleanup-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");

    let cluster_id = ClusterId::new("cluster_endpoint_key_cleanup").expect("cluster");
    let node_id = NodeId::new("node_endpoint_key_cleanup").expect("node");
    let mut state = ClusterBootstrapState::initialized_locally(
        cluster_id,
        "endpoint cleanup",
        node_id.clone(),
        "endpoint cleanup node",
        "public-key",
        OffsetDateTime::now_utc(),
    );
    state.members[0].endpoints.push(ClusterEndpointRecord {
        endpoint_id: ClusterEndpointId::new("endpoint_cleanup_current").expect("endpoint"),
        node_id: node_id.clone(),
        transport: ClusterTransportKind::LanTcp,
        address: "10.14.1.11:21353".to_string(),
        source: ClusterEndpointSource::Static,
        privacy: ClusterEndpointPrivacy::Lan,
        priority: 20,
        last_observed_latency_ms: None,
    });

    let txn = cluster_state_sync_txn(&paths, &state).expect("cluster state sync txn");
    let endpoint_prefix = format!("/cluster/endpoints/{node_id}/");
    let delete_prefix_index = txn
        .success
        .iter()
        .position(|op| matches!(op, KvOp::DeletePrefix { prefix } if prefix == &endpoint_prefix));
    let first_endpoint_put_index = txn
        .success
        .iter()
        .position(|op| matches!(op, KvOp::Put { key, .. } if key.starts_with(&endpoint_prefix)));

    assert!(delete_prefix_index.is_some());
    assert!(
        delete_prefix_index < first_endpoint_put_index,
        "endpoint prefix must be deleted before canonical endpoints are put"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn cluster_node_role_update_keeps_raft_state_and_capabilities() {
    let cluster_id = ClusterId::new("cluster_role_update").expect("cluster");
    let node_id = NodeId::new("node_role_update").expect("node");
    let timestamp = OffsetDateTime::now_utc();
    let mut member = ClusterMemberRecord::local_voter(
        cluster_id,
        node_id,
        "role update",
        "public-key",
        timestamp,
    );
    member.role = ClusterMemberRole::Learner;
    member.state = ClusterMemberState::Learner;
    member.capabilities = main_daemon_capabilities(&ClusterMemberRole::Learner);
    member.allowed_capabilities = member.capabilities.clone();

    apply_cluster_node_role(
        &mut member,
        ClusterNodeRole::Server,
        timestamp + time::Duration::seconds(1),
    );

    assert_eq!(member.node_role, ClusterNodeRole::Server);
    assert_eq!(member.role, ClusterMemberRole::Learner);
    assert_eq!(member.state, ClusterMemberState::Learner);
    assert!(
        !member
            .capabilities
            .iter()
            .any(|capability| capability.as_str() == "cluster.kv.vote")
    );
    assert_eq!(member.allowed_capabilities, member.capabilities);
}

#[test]
fn cluster_raft_role_update_sets_state_and_capabilities() {
    let cluster_id = ClusterId::new("cluster_raft_role_update").expect("cluster");
    let node_id = NodeId::new("node_raft_role_update").expect("node");
    let timestamp = OffsetDateTime::now_utc();
    let mut member = ClusterMemberRecord::pending_learner(
        cluster_id,
        node_id,
        "raft role update",
        "public-key",
        timestamp,
    );
    member.node_role = ClusterNodeRole::Server;

    apply_cluster_raft_role(
        &mut member,
        ClusterMemberRole::Voter,
        timestamp + time::Duration::seconds(1),
    );

    assert_eq!(member.node_role, ClusterNodeRole::Server);
    assert_eq!(member.role, ClusterMemberRole::Voter);
    assert_eq!(member.state, ClusterMemberState::Voter);
    assert!(
        member
            .capabilities
            .iter()
            .any(|capability| capability.as_str() == "cluster.kv.vote")
    );
    assert_eq!(member.allowed_capabilities, member.capabilities);
}

#[test]
fn cluster_network_expiration_keeps_local_owned_records() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-cli-local-network-expiration-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");

    let cluster_id = ClusterId::new("cluster_local_network_expiration").expect("cluster");
    let node_id = NodeId::new("node_local_network_expiration").expect("node");
    let remote_node_id = NodeId::new("node_remote_network_expiration").expect("node");
    let now = OffsetDateTime::now_utc();
    let expired = now - time::Duration::seconds(10);
    let state = ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "local network expiration",
        node_id.clone(),
        "local",
        "local-public-key",
        now,
    );
    sinomo_runtime::network::write_cluster_state(&paths, &state).expect("cluster state");

    let mut store = InProcessKvStore::new();
    for node_id in [&node_id, &shadow_daemon_node_id(&node_id)] {
        let info = sinomo_runtime::network::ClusterNetworkInfoRecord {
            cluster_id: cluster_id.clone(),
            node_id: node_id.clone(),
            environment_id: "env_local".to_string(),
            observed_addresses: vec!["10.14.1.11:21353".to_string()],
            endpoints: Vec::new(),
            observed_at: expired,
            expires_at: expired,
        };
        store
            .put_serde(
                sinomo_runtime::network::cluster_network_info_key(node_id),
                &info,
                None,
            )
            .expect("local network info");
    }
    let connectivity = sinomo_runtime::network::ClusterConnectivityRecord {
        cluster_id: cluster_id.clone(),
        source_node_id: node_id.clone(),
        target_node_id: remote_node_id.clone(),
        endpoint_id: ClusterEndpointId::new("endpoint_remote").expect("endpoint"),
        transport: ClusterTransportKind::LanTcp,
        address: "10.14.1.12:21353".to_string(),
        state: sinomo_runtime::network::ClusterConnectivityState::Unreachable,
        latency_ms: None,
        consecutive_failures: 3,
        last_success_at: None,
        last_failure_at: Some("2026-05-06T00:00:00Z".to_string()),
        observed_at: expired,
        expires_at: expired,
    };
    store
        .put_serde(
            sinomo_runtime::network::cluster_connectivity_key(
                &connectivity.source_node_id,
                &connectivity.target_node_id,
                &connectivity.endpoint_id,
            ),
            &connectivity,
            None,
        )
        .expect("local connectivity");
    sinomo_runtime::network::write_cluster_kv_snapshot(&paths, &store.snapshot())
        .expect("cluster kv snapshot");

    let snapshot =
        expire_cluster_network_records_in_kv(&paths, &state, now).expect("expire local records");

    assert!(snapshot.is_none());
    assert!(
        !sinomo_runtime::network::cluster_raft_wal_paths(&paths, &node_id)
            .wal
            .exists(),
        "skipping local expiration must not append a raft delete transaction"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn stable_connectivity_sync_does_not_rewrite_source_prefix() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-cli-stable-connectivity-sync-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");

    let cluster_id = ClusterId::new("cluster_stable_connectivity_sync").expect("cluster");
    let source_node_id = NodeId::new("node_stable_connectivity_source").expect("node");
    let target_node_id = NodeId::new("node_stable_connectivity_target").expect("node");
    let now = OffsetDateTime::now_utc();
    let stored = sinomo_runtime::network::ClusterConnectivityRecord {
        cluster_id,
        source_node_id: source_node_id.clone(),
        target_node_id,
        endpoint_id: ClusterEndpointId::new("endpoint_stable_connectivity").expect("endpoint"),
        transport: ClusterTransportKind::LanTcp,
        address: "10.14.1.12:21353".to_string(),
        state: sinomo_runtime::network::ClusterConnectivityState::Healthy,
        latency_ms: Some(12),
        consecutive_failures: 0,
        last_success_at: Some("2026-05-06T00:00:00Z".to_string()),
        last_failure_at: None,
        observed_at: now - time::Duration::seconds(60),
        expires_at: now + time::Duration::seconds(120),
    };
    let mut incoming = stored.clone();
    incoming.latency_ms = Some(18);
    incoming.observed_at = now;
    incoming.expires_at = now + time::Duration::seconds(180);

    let mut store = InProcessKvStore::new();
    store
        .put_serde(
            sinomo_runtime::network::cluster_connectivity_key(
                &stored.source_node_id,
                &stored.target_node_id,
                &stored.endpoint_id,
            ),
            &stored,
            None,
        )
        .expect("stored connectivity");
    sinomo_runtime::network::write_cluster_kv_snapshot(&paths, &store.snapshot())
        .expect("cluster kv snapshot");

    let ops = cluster_connectivity_sync_ops(&paths, &source_node_id, &[incoming])
        .expect("connectivity sync ops");

    assert!(ops.is_empty());
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn daemon_advertised_endpoints_use_route_addresses_for_known_peers() {
    let cluster_id = ClusterId::new("cluster_route_advertise").expect("cluster");
    let local_node = NodeId::new("node_route_local").expect("node");
    let remote_node = NodeId::new("node_route_remote").expect("node");
    let mut state = ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "route advertise",
        local_node.clone(),
        "local",
        "local-public-key",
        OffsetDateTime::now_utc(),
    );
    let mut remote = ClusterMemberRecord::local_voter(
        cluster_id,
        remote_node.clone(),
        "remote",
        "remote-public-key",
        OffsetDateTime::now_utc(),
    );
    remote.capabilities = main_daemon_capabilities(&ClusterMemberRole::Voter);
    remote.allowed_capabilities = remote.capabilities.clone();
    remote.endpoints = vec![daemon_lan_endpoint(
        &remote_node,
        "10.14.1.11:21353".to_string(),
        20,
    )];
    state.members.push(remote);

    let route_targets = advertised_peer_route_targets(&state);
    assert_eq!(route_targets.len(), 1);
    assert_eq!(route_targets[0].to_string(), "10.14.1.11:21353");

    let endpoints = daemon_advertised_endpoints_for_state(
        &state,
        &local_node,
        &DaemonRunOptions {
            host: "0.0.0.0".to_string(),
            port: 21353,
        },
        0,
        &["192.168.1.154".parse().expect("lan ip")],
        &["10.14.5.15".parse().expect("wireguard ip")],
    );

    assert!(
        endpoints
            .iter()
            .any(|endpoint| endpoint.address == "192.168.1.154:21353")
    );
    assert!(
        endpoints
            .iter()
            .any(|endpoint| endpoint.address == "10.14.5.15:21353")
    );
}

#[test]
fn cluster_doctor_reports_duplicate_iroh_endpoint_public_keys() {
    let cluster_id = ClusterId::new("cluster_doctor_iroh").expect("cluster");
    let local_node = NodeId::new("node_doctor_local").expect("node");
    let mut state = ClusterBootstrapState::initialized_locally(
        cluster_id,
        "doctor",
        local_node.clone(),
        "local",
        "local-public-key",
        OffsetDateTime::now_utc(),
    );
    let member = state
        .members
        .iter_mut()
        .find(|member| member.node_id == local_node)
        .expect("local member");
    member.endpoints.push(ClusterEndpointRecord {
        endpoint_id: ClusterEndpointId::new("endpoint_iroh_old").expect("endpoint"),
        node_id: local_node.clone(),
        transport: ClusterTransportKind::Iroh,
        address: "iroh://same_public_key?direct=203.0.113.10:21353".to_string(),
        source: ClusterEndpointSource::Rendezvous,
        privacy: ClusterEndpointPrivacy::InternetDirect,
        priority: 40,
        last_observed_latency_ms: None,
    });
    member.endpoints.push(ClusterEndpointRecord {
        endpoint_id: ClusterEndpointId::new("endpoint_iroh_new").expect("endpoint"),
        node_id: local_node.clone(),
        transport: ClusterTransportKind::Iroh,
        address: "iroh://same_public_key?direct=203.0.113.11:21353".to_string(),
        source: ClusterEndpointSource::Rendezvous,
        privacy: ClusterEndpointPrivacy::InternetDirect,
        priority: 40,
        last_observed_latency_ms: None,
    });

    let findings = cluster_doctor_findings(&state, &[], &[]);

    assert!(findings.iter().any(|finding| {
        finding.code == "duplicate-iroh-endpoint"
            && finding.detail.contains("same_public_key")
            && finding.detail.contains("node_doctor_local")
    }));
}

#[test]
fn cluster_peer_members_preserves_pending_local_member() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-cli-cluster-peers-offline-introducer-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(ClusterId::new("cluster_peers_offline").expect("cluster")),
            node_id: Some(NodeId::new("node_peers_host").expect("node")),
            name: "offline peers".to_string(),
        },
        &paths,
    )
    .expect("cluster init");
    let mut state = ClusterBootstrapState::pending_join(
        ClusterId::new("cluster_peers_offline").expect("cluster"),
        "offline peers".to_string(),
        NodeId::new("node_peers_joined").expect("node"),
        "joined peer".to_string(),
        "joined-public-key".to_string(),
        OffsetDateTime::now_utc(),
    );
    state.members.push(ClusterMemberRecord::local_voter(
        ClusterId::new("cluster_peers_offline").expect("cluster"),
        NodeId::new("node_existing_peer").expect("node"),
        "existing peer",
        "existing-public-key",
        OffsetDateTime::now_utc(),
    ));
    write_json(&paths.state, &state).expect("write joined state");
    super::network::sync_cluster_state_to_local_raft_unchecked(&paths, &state).expect("sync kv");
    std::fs::write(&paths.kv_snapshot, "{\"revision\":1}trailing").expect("corrupt kv");

    let members = cluster_peer_members(&paths).expect("peer members");

    assert!(
        members
            .iter()
            .any(|member| member.node_id == NodeId::new("node_peers_joined").expect("node"))
    );
    read_cluster_kv_snapshot(&paths).expect("repaired kv snapshot");
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn install_writes_boot_entries_and_uninstall_retains_data_dir() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-cli-install-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let layout = InstallLayout::new(&data_dir, root.join("startup"));

    install_with_layout(
        InstallOptions {
            host: "127.0.0.1".to_string(),
            port: 21400,
            shadow_port: 21401,
            start: false,
        },
        &layout,
    )
    .expect("install");

    let main_entry = std::fs::read_to_string(&layout.main_boot_entry).expect("main boot");
    let shadow_entry = std::fs::read_to_string(&layout.shadow_boot_entry).expect("shadow boot");
    assert!(layout.binary.exists());
    assert!(layout.shadow_binary.exists());
    assert!(main_entry.contains("SINOMO_DATA_DIR"));
    assert!(shadow_entry.contains("SINOMO_SHADOW_MAIN_BINARY"));
    assert!(shadow_entry.contains(&layout.shadow_binary.display().to_string()));
    if cfg!(windows) {
        assert!(main_entry.contains("daemon start"));
        assert!(shadow_entry.contains("daemon shadow start"));
    } else {
        assert!(main_entry.contains("daemon run"));
        assert!(shadow_entry.contains("daemon shadow run"));
    }
    assert!(main_entry.contains("21400"));
    assert!(shadow_entry.contains("21401"));

    uninstall_with_layout(UninstallOptions { stop: false }, &layout).expect("uninstall");
    assert!(!layout.main_boot_entry.exists());
    assert!(!layout.shadow_boot_entry.exists());
    assert!(layout.data_dir.exists());
    assert!(layout.binary.exists());
    assert!(layout.shadow_binary.exists());
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn install_detects_existing_binary_update() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-cli-install-update-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    let destination = root.join("bin").join(if cfg!(windows) {
        "sinomo.exe"
    } else {
        "sinomo"
    });

    assert!(!install_should_update_binary(&destination).expect("missing destination"));
    std::fs::create_dir_all(destination.parent().expect("parent")).expect("mkdir");
    std::fs::write(&destination, b"old binary").expect("write old binary");
    assert!(install_should_update_binary(&destination).expect("existing destination"));
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn install_detects_daemon_option_mismatch() {
    let status = DaemonStatus {
        pid: 4242,
        host: "127.0.0.1".to_string(),
        port: 21_353,
        address: "127.0.0.1:21353".to_string(),
        version: daemon_package_version(),
        target_os: Some(std::env::consts::OS.to_string()),
        target_arch: Some(std::env::consts::ARCH.to_string()),
        executable: None,
        rollout: None,
        started_at: "2026-05-04T00:00:00Z".to_string(),
        data_dir: PathBuf::from("data"),
        lock_file: PathBuf::from("data/daemon/daemon.lock"),
        cluster: None,
    };

    assert!(daemon_status_matches_options(&status, "127.0.0.1", 21_353));
    assert!(!daemon_status_matches_options(&status, "0.0.0.0", 21_353));
    assert!(!daemon_status_matches_options(&status, "127.0.0.1", 21_354));
}

#[test]
fn orphan_guard_applies_to_distributed_runtime_commands() {
    assert!(!cli_command_requires_cluster_state(&CliCommand::Daemon(
        DaemonCliCommand::Status(DaemonRunOptions::default())
    )));
    assert!(cli_command_requires_cluster_state(&CliCommand::Rollout(
        RolloutCliCommand::Status
    )));
    assert!(cli_command_requires_cluster_state(&CliCommand::Evolve(
        super::EvolveCliCommand::Help
    )));
    assert!(!cli_command_requires_cluster_state(&CliCommand::Cluster(
        ClusterCliCommand::Status
    )));
    assert!(!cli_command_requires_cluster_state(&CliCommand::Dfs(
        DfsCliCommand::Help
    )));
    assert!(!cli_command_requires_cluster_state(&CliCommand::Models(
        ModelsCliCommand::Help
    )));
    assert!(!cli_command_requires_cluster_state(&CliCommand::Providers(
        ProviderCliCommand::Help
    )));
}

fn signed_test_invite_uri(
    root: &std::path::Path,
    cluster_id: &str,
    issuer_node_id: &str,
) -> String {
    let identity = super::load_or_create_main_node_identity(root).expect("test identity");
    let mut invite = ClusterJoinInvite {
        invite_id: ClusterInviteId::generate(),
        cluster_id: ClusterId::new(cluster_id).expect("cluster"),
        cluster_name: Some("test cluster".to_string()),
        cluster_public_key: identity.public_key.clone(),
        invitation_secret: "secret_test".to_string(),
        communication_channels: Vec::new(),
        introducers: Vec::new(),
        token_id: "token_test".to_string(),
        expires_at: time::OffsetDateTime::now_utc() + time::Duration::seconds(60),
        trust_domain: TrustDomain::new("trust.local").expect("trust"),
        allowed_member_roles: vec![ClusterMemberRole::Learner, ClusterMemberRole::NonVoting],
        issuer_node_id: NodeId::new(issuer_node_id).expect("issuer node"),
        issuer_public_key: identity.public_key.clone(),
        signature: String::new(),
    };
    invite.signature = sign_cluster_join_invite(&invite, &identity).expect("sign invite");
    encode_cluster_join_invite(&invite).expect("invite uri")
}

#[test]
fn dfs_parser_accepts_operator_and_pin_commands() {
    let metadata_root = std::env::temp_dir().join("sinomo-cli-dfs-meta");
    assert_eq!(
        parse_dfs_command(&[
            "status".to_string(),
            "--metadata-root".to_string(),
            metadata_root.display().to_string(),
            "--json".to_string(),
        ])
        .expect("dfs status"),
        DfsCliCommand::Status(DfsCommonOptions {
            metadata_root: Some(metadata_root.clone()),
            json: true,
        })
    );
    assert_eq!(
        parse_dfs_command(&[
            "conflicts".to_string(),
            "--filesystem".to_string(),
            "sifs".to_string(),
            "--object".to_string(),
            "obj_artifact".to_string(),
        ])
        .expect("dfs conflicts"),
        DfsCliCommand::Conflicts(DfsFilterOptions {
            common: DfsCommonOptions::default(),
            file_system_id: Some(
                sinomo_runtime::core::DfsFileSystemId::new("sifs").expect("filesystem")
            ),
            object_id: Some(
                sinomo_runtime::core::DfsObjectId::new("obj_artifact").expect("object")
            ),
        })
    );
    assert!(matches!(
        parse_dfs_command(&[
            "pin".to_string(),
            "add".to_string(),
            "--path".to_string(),
            "src/lib.rs".to_string(),
            "--mode".to_string(),
            "metadata".to_string(),
        ])
        .expect("dfs pin add"),
        DfsCliCommand::Pin(DfsPinCliCommand::Add(options))
            if options.mode == sinomo_runtime::dfs::DfsPinMode::Metadata
                && options.path == "src/lib.rs"
    ));
    assert!(matches!(
        parse_dfs_command(&[
            "expose".to_string(),
            "add".to_string(),
            "--source".to_string(),
            "/tmp/shared".to_string(),
            "--dfs-path".to_string(),
            "/sinomo/nodes/node_alpha/files/shared".to_string(),
            "--node".to_string(),
            "node_alpha".to_string(),
            "--read-only".to_string(),
        ])
        .expect("dfs expose add"),
        DfsCliCommand::Expose(DfsExposeCliCommand::Add(options))
            if options.source == PathBuf::from("/tmp/shared")
                && options.dfs_path == "/sinomo/nodes/node_alpha/files/shared"
                && options.node_id == Some(NodeId::new("node_alpha").expect("node"))
                && !options.writable
    ));
}

#[test]
fn cluster_parser_accepts_init_and_join_options() {
    assert_eq!(
        parse_cluster_command(&[
            "init".to_string(),
            "--name".to_string(),
            "home".to_string(),
            "--cluster-id".to_string(),
            "cluster_home".to_string(),
            "--node-id".to_string(),
            "node_desktop".to_string(),
        ])
        .expect("cluster init"),
        ClusterCliCommand::Init(ClusterInitOptions {
            cluster_id: Some(ClusterId::new("cluster_home").expect("cluster")),
            node_id: Some(NodeId::new("node_desktop").expect("node")),
            name: "home".to_string(),
        })
    );
    assert_eq!(
        parse_cluster_command(&[
            "join".to_string(),
            "sinomo-join://token".to_string(),
            "--name".to_string(),
            "laptop".to_string(),
        ])
        .expect("cluster join"),
        ClusterCliCommand::Join(ClusterJoinOptions {
            invite: "sinomo-join://token".to_string(),
            cluster_id: None,
            node_id: None,
            name: "laptop".to_string(),
        })
    );
    assert_eq!(
        parse_cluster_command(&[
            "invite".to_string(),
            "--introducer".to_string(),
            "192.168.1.10:21353".to_string(),
            "--ttl-seconds".to_string(),
            "60".to_string(),
            "--output".to_string(),
            "invite.txt".to_string(),
        ])
        .expect("cluster invite"),
        ClusterCliCommand::Invite(ClusterInviteOptions {
            ttl_seconds: 60,
            introducers: vec!["192.168.1.10:21353".to_string()],
            output: Some(PathBuf::from("invite.txt")),
        })
    );
    assert_eq!(
        parse_cluster_command(&[
            "role".to_string(),
            "node_laptop".to_string(),
            "server".to_string(),
        ])
        .expect("cluster role"),
        ClusterCliCommand::Role(ClusterRoleOptions {
            node_id: NodeId::new("node_laptop").expect("node"),
            role: ClusterNodeRole::Server,
        })
    );
    assert_eq!(
        parse_cluster_command(&[
            "raft-role".to_string(),
            "node_laptop".to_string(),
            "non_voting".to_string(),
        ])
        .expect("cluster raft-role"),
        ClusterCliCommand::RaftRole(ClusterRaftRoleOptions {
            node_id: NodeId::new("node_laptop").expect("node"),
            role: ClusterMemberRole::NonVoting,
        })
    );
    assert_eq!(
        parse_cluster_command(&["promote".to_string(), "node_laptop".to_string()])
            .expect("cluster promote"),
        ClusterCliCommand::RaftRole(ClusterRaftRoleOptions {
            node_id: NodeId::new("node_laptop").expect("node"),
            role: ClusterMemberRole::Voter,
        })
    );
}

#[test]
fn cluster_init_persists_local_cluster_state() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-cli-cluster-init-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    let paths = ClusterPaths::new(&SinomoDataDir::new(&root));

    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(ClusterId::new("cluster_test").expect("cluster")),
            node_id: Some(NodeId::new("node_local").expect("node")),
            name: "test cluster".to_string(),
        },
        &paths,
    )
    .expect("cluster init");

    let state = read_cluster_state(&paths).expect("cluster state");
    assert_eq!(
        state.cluster_id,
        ClusterId::new("cluster_test").expect("cluster")
    );
    assert_eq!(
        state.local_node_id,
        NodeId::new("node_local").expect("node")
    );
    assert_eq!(state.term, 1);
    assert_eq!(state.kv_revision, 1);
    assert_eq!(state.members.len(), 2);
    let main = state
        .members
        .iter()
        .find(|member| member.node_id == NodeId::new("node_local").expect("node"))
        .expect("main daemon member");
    assert_eq!(main.role, ClusterMemberRole::Voter);
    assert_eq!(main.state, ClusterMemberState::Voter);
    assert!(!main.node_public_key.trim().is_empty());
    assert_eq!(main.endpoints[0].address, "127.0.0.1:21353");
    let shadow = state
        .members
        .iter()
        .find(|member| member.node_id == NodeId::new("node_local-shadow").expect("node"))
        .expect("shadow daemon member");
    assert_eq!(shadow.role, ClusterMemberRole::NonVoting);
    assert_eq!(shadow.state, ClusterMemberState::NonVoting);
    assert!(!shadow.node_public_key.trim().is_empty());
    assert_ne!(shadow.node_public_key, main.node_public_key);
    assert_eq!(shadow.endpoints[0].address, "127.0.0.1:21354");
    let kv_members = read_cluster_members_from_kv(&paths).expect("kv members");
    assert_eq!(kv_members.len(), 2);
    let kv_main = kv_members
        .iter()
        .find(|member| member.node_id == NodeId::new("node_local").expect("node"))
        .expect("kv main");
    assert_eq!(kv_main.state, ClusterMemberState::Voter);
    assert_eq!(kv_main.node_public_key, main.node_public_key);
    assert_eq!(
        cluster_raft_known_leader_id(&paths, &state).expect("known leader"),
        Some(NodeId::new("node_local").expect("node"))
    );
    sinomo_runtime::network::clear_cluster_raft_leader(&paths, &state.local_node_id)
        .expect("clear leader");
    assert_eq!(
        cluster_raft_known_leader_id(&paths, &state).expect("cleared leader"),
        None
    );
    assert!(elect_cluster_raft_leader(&paths, &state).expect("single-voter election"));
    assert_eq!(
        cluster_raft_known_leader_id(&paths, &state).expect("reelected leader"),
        Some(NodeId::new("node_local").expect("node"))
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn cluster_state_sync_rejects_unsafe_voter_reconfiguration() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-cli-unsafe-voter-config-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(ClusterId::new("cluster_unsafe_config").expect("cluster")),
            node_id: Some(NodeId::new("node_config_host").expect("node")),
            name: "unsafe config cluster".to_string(),
        },
        &paths,
    )
    .expect("cluster init");
    let state = read_cluster_state(&paths).expect("cluster state");

    let mut removed_voter_state = state.clone();
    removed_voter_state.members.clear();
    let removed_error =
        sync_cluster_state_to_kv(&paths, &removed_voter_state).expect_err("reject voter removal");
    assert!(matches!(removed_error, CliError::ClusterReadOnly { .. }));

    let mut direct_voter_state = state.clone();
    let peer_id = NodeId::new("node_direct_voter").expect("peer");
    let mut direct_voter = ClusterMemberRecord::local_voter(
        state.cluster_id.clone(),
        peer_id.clone(),
        "direct voter",
        "pk_direct_voter",
        OffsetDateTime::now_utc(),
    );
    direct_voter.capabilities = main_daemon_capabilities(&ClusterMemberRole::Voter);
    direct_voter.allowed_capabilities = direct_voter.capabilities.clone();
    direct_voter.endpoints = vec![daemon_lan_endpoint(
        &peer_id,
        "127.0.0.1:32153".to_string(),
        0,
    )];
    direct_voter_state.members.push(direct_voter);
    let added_error =
        sync_cluster_state_to_kv(&paths, &direct_voter_state).expect_err("reject direct voter add");
    assert!(matches!(added_error, CliError::ClusterReadOnly { .. }));
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn daemon_auth_accepts_signed_local_command_and_rejects_tampering() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-cli-daemon-auth-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(ClusterId::new("cluster_auth").expect("cluster")),
            node_id: Some(NodeId::new("node_auth").expect("node")),
            name: "auth cluster".to_string(),
        },
        &paths,
    )
    .expect("cluster init");
    let daemon_paths = DaemonPaths::with_root(root.join("daemon"));
    let identity = super::load_or_create_main_node_identity(&root).expect("identity");

    let signed = sign_daemon_command_from_data_dir(&data_dir, "STATUS").expect("signed command");
    assert_eq!(
        authenticate_daemon_command(&daemon_paths, &signed).expect("auth command"),
        authenticated_test_command(
            "STATUS",
            super::DaemonSignerAuthority::Local,
            "node_auth",
            identity.public_key
        )
    );
    assert!(matches!(
        authenticate_daemon_command(&daemon_paths, "STATUS"),
        Err(super::DaemonError::AuthDenied)
    ));
    assert!(matches!(
        authenticate_daemon_command(&daemon_paths, &signed),
        Err(super::DaemonError::AuthDenied)
    ));

    let tampered = signed.replace("\"command\":\"STATUS\"", "\"command\":\"SHUTDOWN\"");
    assert!(matches!(
        authenticate_daemon_command(&daemon_paths, &tampered),
        Err(super::DaemonError::AuthDenied)
    ));
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn daemon_auth_only_accepts_active_signed_join_invites() {
    let host_root = std::env::temp_dir().join(format!(
        "sinomo-cli-daemon-join-auth-host-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    let join_root = std::env::temp_dir().join(format!(
        "sinomo-cli-daemon-join-auth-join-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    let host_data_dir = SinomoDataDir::new(&host_root);
    let host_cluster_paths = ClusterPaths::new(&host_data_dir);
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(ClusterId::new("cluster_join_auth").expect("cluster")),
            node_id: Some(NodeId::new("node_join_auth_host").expect("node")),
            name: "join auth cluster".to_string(),
        },
        &host_cluster_paths,
    )
    .expect("cluster init");
    let rendered = cluster_invite_with_paths(
        ClusterInviteOptions {
            ttl_seconds: 120,
            introducers: vec!["127.0.0.1:21353".to_string()],
            output: None,
        },
        &host_cluster_paths,
    )
    .expect("cluster invite");

    let join_data_dir = SinomoDataDir::new(&join_root);
    let join_identity =
        super::load_or_create_main_node_identity(&join_root).expect("join identity");
    let joined_member = ClusterMemberRecord::pending_learner(
        rendered.invite.cluster_id.clone(),
        NodeId::new("node_join_auth_candidate").expect("node"),
        "join candidate",
        join_identity.public_key.clone(),
        OffsetDateTime::now_utc(),
    );
    let request = ClusterJoinRegistrationRequest {
        invitation_secret: rendered.invite.invitation_secret.clone(),
        invite: rendered.invite.clone(),
        members: vec![joined_member],
    };
    let payload =
        sinomo_runtime::network::encode_object_text_payload(&request).expect("join request");
    let signed =
        sign_daemon_command_from_data_dir(&join_data_dir, &format!("CLUSTER_JOIN {payload}"))
            .expect("signed join");
    assert_eq!(
        authenticate_daemon_command(&DaemonPaths::with_root(host_root.join("daemon")), &signed)
            .expect("active invite auth")
            .signer,
        super::DaemonSignerAuthority::JoinCandidate
    );

    let mut tampered = request;
    tampered.invite.cluster_public_key.push_str("_tampered");
    let tampered_payload = sinomo_runtime::network::encode_object_text_payload(&tampered)
        .expect("tampered join request");
    let tampered_signed = sign_daemon_command_from_data_dir(
        &join_data_dir,
        &format!("CLUSTER_JOIN {tampered_payload}"),
    )
    .expect("signed tampered join");
    assert!(matches!(
        authenticate_daemon_command(
            &DaemonPaths::with_root(host_root.join("daemon")),
            &tampered_signed
        ),
        Err(super::DaemonError::AuthDenied)
    ));
    std::fs::remove_dir_all(host_root).ok();
    std::fs::remove_dir_all(join_root).ok();
}

#[test]
fn daemon_authorization_restricts_admin_and_local_commands() {
    assert!(
        super::authorize_authenticated_daemon_command(&authenticated_test_command(
            "STATUS",
            super::DaemonSignerAuthority::Member,
            "node_member",
            "pk_member"
        ))
        .is_ok()
    );
    assert!(
        super::authorize_authenticated_daemon_command(&authenticated_test_command(
            "ROLLOUT_BUILD {}",
            super::DaemonSignerAuthority::Member,
            "node_member",
            "pk_member"
        ))
        .is_err()
    );
    assert!(
        super::authorize_authenticated_daemon_command(&authenticated_test_command(
            "ROLLOUT_BUILD {}",
            super::DaemonSignerAuthority::Voter,
            "node_voter",
            "pk_voter"
        ))
        .is_ok()
    );
    assert!(
        super::authorize_authenticated_daemon_command(&authenticated_test_command(
            "SHUTDOWN",
            super::DaemonSignerAuthority::Voter,
            "node_voter",
            "pk_voter"
        ))
        .is_err()
    );
    assert!(
        super::authorize_authenticated_daemon_command(&authenticated_test_command(
            "SHUTDOWN",
            super::DaemonSignerAuthority::Local,
            "node_local",
            "pk_local"
        ))
        .is_ok()
    );
    assert!(
        super::authorize_authenticated_daemon_command(&authenticated_test_command(
            "UNKNOWN_PROBE",
            super::DaemonSignerAuthority::Local,
            "node_local",
            "pk_local"
        ))
        .is_err()
    );
}

#[test]
fn cluster_join_records_pending_learner_state() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-cli-cluster-join-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    let paths = ClusterPaths::new(&SinomoDataDir::new(&root));

    cluster_join_with_paths(
        ClusterJoinOptions {
            invite: signed_test_invite_uri(&root, "cluster_joined", "node_host"),
            cluster_id: Some(ClusterId::new("cluster_joined").expect("cluster")),
            node_id: Some(NodeId::new("node_joiner").expect("node")),
            name: "joiner".to_string(),
        },
        &paths,
    )
    .expect("cluster join");

    let state = read_cluster_state(&paths).expect("cluster state");
    assert_eq!(
        state.cluster_id,
        ClusterId::new("cluster_joined").expect("cluster")
    );
    assert_eq!(state.term, 0);
    assert_eq!(state.kv_revision, 0);
    assert_eq!(state.members[0].role, ClusterMemberRole::Learner);
    assert_eq!(state.members[0].state, ClusterMemberState::Pending);
    assert!(!state.members[0].node_public_key.trim().is_empty());
    let shadow = state
        .members
        .iter()
        .find(|member| member.node_id == NodeId::new("node_joiner-shadow").expect("node"))
        .expect("shadow daemon member");
    assert_eq!(shadow.role, ClusterMemberRole::NonVoting);
    assert_eq!(shadow.state, ClusterMemberState::Pending);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn cluster_invite_generates_portable_join_payload() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-cli-cluster-invite-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
    let output = root.join("join.txt");
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(ClusterId::new("cluster_invite").expect("cluster")),
            node_id: Some(NodeId::new("node_host").expect("node")),
            name: "invite cluster".to_string(),
        },
        &paths,
    )
    .expect("cluster init");

    let rendered = cluster_invite_with_paths(
        ClusterInviteOptions {
            ttl_seconds: 120,
            introducers: vec!["192.168.1.10:21353".to_string()],
            output: Some(output.clone()),
        },
        &paths,
    )
    .expect("cluster invite");
    let parsed = decode_cluster_join_invite(&rendered.uri).expect("decode invite");

    assert!(
        rendered
            .uri
            .starts_with(&format!("{}?public_key=", super::SINOMO_JOIN_URI_PREFIX))
    );
    assert!(rendered.uri.contains("&invite_id="));
    assert!(rendered.uri.contains("&secret="));
    assert!(rendered.uri.contains("&channel="));
    assert!(rendered.uri.contains("192.168.1.10:21353"));
    assert!(
        rendered
            .uri
            .contains("&channel=lan-tcp,192.168.1.10:21353,")
    );
    assert!(!rendered.uri.contains("&channel=node_host,"));
    assert!(!rendered.uri.contains("&cluster_id="));
    assert!(!rendered.uri.contains("&cluster_name="));
    assert!(!rendered.uri.contains("&introducer="));
    assert!(!rendered.uri.contains("&signature="));
    assert!(!rendered.uri.contains("%7B"));
    assert_eq!(
        parsed.cluster_id,
        ClusterId::new("cluster_pending_join").expect("cluster")
    );
    assert_eq!(parsed.cluster_name, None);
    assert!(!parsed.cluster_public_key.trim().is_empty());
    assert!(!parsed.invitation_secret.trim().is_empty());
    assert_eq!(parsed.communication_channels.len(), 1);
    assert_eq!(
        parsed.communication_channels[0].endpoint.address,
        "192.168.1.10:21353"
    );
    assert_eq!(
        parsed.communication_channels[0].node_public_key,
        parsed.cluster_public_key
    );
    assert!(parsed.introducers.is_empty());
    let persisted_invite =
        super::network::read_cluster_invitation_from_kv(&paths, &parsed.invite_id)
            .expect("published invite");
    assert_eq!(persisted_invite, rendered.invite);
    assert_eq!(parsed.invite_id, persisted_invite.invite_id);
    assert_eq!(
        parsed.cluster_public_key,
        persisted_invite.cluster_public_key
    );
    assert_eq!(parsed.invitation_secret, persisted_invite.invitation_secret);
    let invite_state_entry = super::network::read_cluster_kv_store(&paths)
        .expect("cluster kv store")
        .get(&super::network::cluster_invitation_state_key(
            &parsed.invite_id,
        ))
        .expect("invite state")
        .clone();
    let invite_state: super::network::ClusterInvitationStateRecord =
        invite_state_entry.decode().expect("invite state record");
    assert_eq!(invite_state.state, "active");
    assert!(rendered.persisted_path.exists());
    assert_eq!(
        std::fs::read_to_string(output).expect("invite file").trim(),
        rendered.join_command
    );
    assert!(rendered.join_command.starts_with("sinomo cluster join "));
    let parsed_from_command =
        parse_cluster_join_invite_argument(&rendered.join_command).expect("parse join command");
    assert_eq!(parsed_from_command.invite_id, parsed.invite_id);
    assert_eq!(
        parsed_from_command.cluster_public_key,
        parsed.cluster_public_key
    );
    assert_eq!(
        parsed_from_command.invitation_secret,
        parsed.invitation_secret
    );
    assert_eq!(
        parsed_from_command.communication_channels[0]
            .endpoint
            .address,
        parsed.communication_channels[0].endpoint.address
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn join_registration_skips_loopback_when_remote_invite_channels_exist() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-cli-cluster-invite-channel-filter-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(ClusterId::new("cluster_invite_filter").expect("cluster")),
            node_id: Some(NodeId::new("node_host").expect("node")),
            name: "invite filter cluster".to_string(),
        },
        &paths,
    )
    .expect("cluster init");

    let rendered = cluster_invite_with_paths(
        ClusterInviteOptions {
            ttl_seconds: 120,
            introducers: vec![
                "127.0.0.1:21353".to_string(),
                "192.168.1.10:21353".to_string(),
            ],
            output: None,
        },
        &paths,
    )
    .expect("cluster invite");
    let channels = invite_channels_for_join_registration(&rendered.invite);

    assert_eq!(channels.len(), 1);
    assert_eq!(channels[0].endpoint.address, "192.168.1.10:21353");
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn join_registration_keeps_loopback_when_it_is_the_only_invite_channel() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-cli-cluster-invite-loopback-only-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(ClusterId::new("cluster_loopback_only").expect("cluster")),
            node_id: Some(NodeId::new("node_host").expect("node")),
            name: "loopback only cluster".to_string(),
        },
        &paths,
    )
    .expect("cluster init");

    let rendered = cluster_invite_with_paths(
        ClusterInviteOptions {
            ttl_seconds: 120,
            introducers: vec!["127.0.0.1:21353".to_string()],
            output: None,
        },
        &paths,
    )
    .expect("cluster invite");
    let channels = invite_channels_for_join_registration(&rendered.invite);

    assert_eq!(channels.len(), 1);
    assert_eq!(channels[0].endpoint.address, "127.0.0.1:21353");
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn explicit_invite_introducer_uses_known_member_endpoint_identity() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-cli-cluster-explicit-introducer-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(ClusterId::new("cluster_explicit_intro").expect("cluster")),
            node_id: Some(NodeId::new("node_host").expect("node")),
            name: "explicit introducer cluster".to_string(),
        },
        &paths,
    )
    .expect("cluster init");
    let mut state = read_cluster_state(&paths).expect("cluster state");
    let now = OffsetDateTime::now_utc();
    let remote_node = NodeId::new("node_known_intro").expect("node");
    let mut remote = ClusterMemberRecord::local_non_voting(
        state.cluster_id.clone(),
        remote_node.clone(),
        "known introducer",
        "known-public-key",
        now,
    );
    remote.endpoints.push(invite_introducer_endpoint(
        &remote_node,
        "10.14.1.11:21353".to_string(),
    ));
    state.members.push(remote);

    let endpoint = invite_introducer_endpoint_from_state(&state, "10.14.1.11:21353".to_string());
    let channels = invite_communication_channels(&state, &[endpoint]);

    assert_eq!(channels.len(), 1);
    assert_eq!(channels[0].endpoint.node_id, remote_node);
    assert_eq!(channels[0].node_public_key, "known-public-key");
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn cluster_join_uses_structured_invite_cluster_identity() {
    let host_root = std::env::temp_dir().join(format!(
        "sinomo-cli-cluster-host-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    let join_root = std::env::temp_dir().join(format!(
        "sinomo-cli-cluster-join-invite-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    let host_paths = ClusterPaths::new(&SinomoDataDir::new(&host_root));
    let join_paths = ClusterPaths::new(&SinomoDataDir::new(&join_root));
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(ClusterId::new("cluster_remote").expect("cluster")),
            node_id: Some(NodeId::new("node_host").expect("node")),
            name: "remote cluster".to_string(),
        },
        &host_paths,
    )
    .expect("cluster init");
    let rendered = cluster_invite_with_paths(
        ClusterInviteOptions {
            ttl_seconds: 120,
            introducers: vec!["192.168.1.10:21353".to_string()],
            output: None,
        },
        &host_paths,
    )
    .expect("cluster invite");
    let mut offline_invite = rendered.invite.clone();
    offline_invite.introducers.clear();
    offline_invite.communication_channels.clear();
    let host_identity =
        super::load_or_create_main_node_identity(&host_root).expect("host identity");
    offline_invite.signature =
        sign_cluster_join_invite(&offline_invite, &host_identity).expect("resign invite");
    let offline_uri = encode_cluster_join_invite(&offline_invite).expect("offline invite");

    cluster_join_with_paths(
        ClusterJoinOptions {
            invite: offline_uri,
            cluster_id: Some(ClusterId::new("cluster_remote").expect("cluster")),
            node_id: Some(NodeId::new("node_remote").expect("node")),
            name: "remote".to_string(),
        },
        &join_paths,
    )
    .expect("cluster join");
    let state = read_cluster_state(&join_paths).expect("cluster state");

    assert_eq!(
        state.cluster_id,
        ClusterId::new("cluster_remote").expect("cluster")
    );
    assert_eq!(state.cluster_name, "remote");
    assert!(state.members.iter().any(|member| {
        member.node_id == NodeId::new("node_remote").expect("node")
            && member.role == ClusterMemberRole::Learner
            && member.state == ClusterMemberState::Pending
    }));
    std::fs::remove_dir_all(host_root).ok();
    std::fs::remove_dir_all(join_root).ok();
}

#[test]
fn introducer_records_joined_members_and_returns_peer_view() {
    let host_root = std::env::temp_dir().join(format!(
        "sinomo-cli-cluster-introducer-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    let join_root = std::env::temp_dir().join(format!(
        "sinomo-cli-cluster-joining-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    let host_paths = ClusterPaths::new(&SinomoDataDir::new(&host_root));
    let join_paths = ClusterPaths::new(&SinomoDataDir::new(&join_root));
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(ClusterId::new("cluster_register").expect("cluster")),
            node_id: Some(NodeId::new("node_host").expect("node")),
            name: "registered cluster".to_string(),
        },
        &host_paths,
    )
    .expect("cluster init");
    let rendered = cluster_invite_with_paths(
        ClusterInviteOptions {
            ttl_seconds: 120,
            introducers: vec!["127.0.0.1:21353".to_string()],
            output: None,
        },
        &host_paths,
    )
    .expect("cluster invite");
    let mut offline_invite = rendered.invite.clone();
    offline_invite.introducers.clear();
    offline_invite.communication_channels.clear();
    let host_identity =
        super::load_or_create_main_node_identity(&host_root).expect("host identity");
    offline_invite.signature =
        sign_cluster_join_invite(&offline_invite, &host_identity).expect("resign invite");
    write_json(
        &host_paths.invite_path(&offline_invite.invite_id),
        &offline_invite,
    )
    .expect("active offline invite");
    super::network::sync_cluster_invitation_to_kv(&host_paths, &offline_invite)
        .expect("active offline invite kv");
    let offline_uri = encode_cluster_join_invite(&offline_invite).expect("offline invite");
    cluster_join_with_paths(
        ClusterJoinOptions {
            invite: offline_uri,
            cluster_id: Some(ClusterId::new("cluster_register").expect("cluster")),
            node_id: Some(NodeId::new("node_remote").expect("node")),
            name: "remote".to_string(),
        },
        &join_paths,
    )
    .expect("cluster join local");
    let remote_state = read_cluster_state(&join_paths).expect("remote state");
    let mut malicious_members = remote_state.members.clone();
    for member in &mut malicious_members {
        if member.node_id == NodeId::new("node_remote").expect("node") {
            member.role = ClusterMemberRole::Voter;
            member.state = ClusterMemberState::Pending;
        }
    }
    let malicious_request = ClusterJoinRegistrationRequest {
        invitation_secret: offline_invite.invitation_secret.clone(),
        invite: offline_invite.clone(),
        members: malicious_members,
    };
    let malicious_payload = sinomo_runtime::network::encode_object_text_payload(&malicious_request)
        .expect("malicious request object");
    let malicious_response = handle_daemon_cluster_join(
        &DaemonPaths::with_root(host_root.join("daemon")),
        &malicious_payload,
    )
    .expect("malicious introducer join");
    assert!(!malicious_response.accepted);
    assert!(
        !read_cluster_state(&host_paths)
            .expect("host state after rejected join")
            .members
            .iter()
            .any(|member| member.node_id == NodeId::new("node_remote").expect("node"))
    );

    let request = ClusterJoinRegistrationRequest {
        invitation_secret: offline_invite.invitation_secret.clone(),
        invite: offline_invite,
        members: remote_state.members,
    };
    let payload =
        sinomo_runtime::network::encode_object_text_payload(&request).expect("request object");
    let response =
        handle_daemon_cluster_join(&DaemonPaths::with_root(host_root.join("daemon")), &payload)
            .expect("introducer join");

    assert!(response.accepted);
    assert!(response.kv_snapshot.is_some());
    let host_state = read_cluster_state(&host_paths).expect("host state");
    assert!(host_state.members.iter().any(|member| {
        member.node_id == NodeId::new("node_remote").expect("node")
            && member.state == ClusterMemberState::Learner
    }));
    let host_kv_members = read_cluster_members_from_kv(&host_paths).expect("host kv members");
    assert!(host_kv_members.iter().any(|member| {
        member.node_id == NodeId::new("node_remote").expect("node")
            && member.state == ClusterMemberState::Learner
    }));
    assert!(
        response
            .state
            .members
            .iter()
            .any(|member| { member.node_id == NodeId::new("node_remote").expect("node") })
    );
    std::fs::remove_dir_all(host_root).ok();
    std::fs::remove_dir_all(join_root).ok();
}

#[test]
fn daemon_cluster_kv_txn_persists_snapshot() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-cli-cluster-kv-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(ClusterId::new("cluster_kv").expect("cluster")),
            node_id: Some(NodeId::new("node_kv").expect("node")),
            name: "kv cluster".to_string(),
        },
        &paths,
    )
    .expect("cluster init");

    let txn = KvTxn {
        compares: vec![KvCompare::Missing {
            key: "/services/test".to_string(),
        }],
        success: vec![KvOp::Put {
            key: "/services/test".to_string(),
            value: br#"{"healthy":true}"#.to_vec(),
            lease_id: None,
        }],
        failure: Vec::new(),
    };
    let payload = serde_json::to_string(&txn).expect("txn json");
    let response =
        handle_daemon_cluster_kv_txn(&DaemonPaths::with_root(root.join("daemon")), &payload)
            .expect("kv txn");

    assert!(response.succeeded);
    let store = super::read_cluster_kv_store(&paths).expect("kv store");
    let service = store.get("/services/test").expect("service entry");
    assert_eq!(service.value, br#"{"healthy":true}"#.to_vec());
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn daemon_cluster_kv_txn_does_not_ack_without_append_quorum() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-cli-cluster-kv-no-quorum-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(ClusterId::new("cluster_kv_no_quorum").expect("cluster")),
            node_id: Some(NodeId::new("node_kv_no_quorum").expect("node")),
            name: "kv no quorum cluster".to_string(),
        },
        &paths,
    )
    .expect("cluster init");
    let mut state = read_cluster_state(&paths).expect("cluster state");
    let (peer_address, responder) = start_one_shot_raft_append_responder(state.clone());
    let peer_node_id = NodeId::new("node_kv_no_quorum_peer").expect("node");
    let mut peer = ClusterMemberRecord::local_voter(
        state.cluster_id.clone(),
        peer_node_id.clone(),
        "peer",
        "pk_peer",
        OffsetDateTime::now_utc(),
    );
    peer.capabilities = main_daemon_capabilities(&ClusterMemberRole::Voter);
    peer.allowed_capabilities = peer.capabilities.clone();
    peer.endpoints = vec![daemon_lan_endpoint(&peer_node_id, peer_address, 0)];
    state.members.push(peer);
    write_json(&paths.state, &state).expect("write cluster state");
    super::network::sync_cluster_state_to_local_raft_unchecked(&paths, &state)
        .expect("sync state to kv");

    let txn = KvTxn {
        compares: Vec::new(),
        success: vec![KvOp::Put {
            key: "/config/unsafe".to_string(),
            value: b"must-not-commit".to_vec(),
            lease_id: None,
        }],
        failure: Vec::new(),
    };
    let result = handle_daemon_cluster_kv_txn(
        &DaemonPaths::with_root(root.join("daemon")),
        &serde_json::to_string(&txn).expect("txn json"),
    );

    assert!(matches!(result, Err(CliError::ClusterReadOnly { .. })));
    assert!(
        !read_cluster_kv_snapshot(&paths)
            .expect("cluster snapshot")
            .entries
            .iter()
            .any(|entry| entry.key == "/config/unsafe")
    );
    responder.join().expect("join fake responder");
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn daemon_cluster_kv_is_read_only_without_cluster_state() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-cli-cluster-kv-no-cluster-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    let daemon_paths = DaemonPaths::with_root(root.join("daemon"));
    let txn = KvTxn {
        compares: Vec::new(),
        success: vec![KvOp::Put {
            key: "/config/no-cluster".to_string(),
            value: b"blocked".to_vec(),
            lease_id: None,
        }],
        failure: Vec::new(),
    };

    let result = handle_daemon_cluster_kv_txn(
        &daemon_paths,
        &serde_json::to_string(&txn).expect("txn json"),
    );

    assert!(matches!(result, Err(CliError::ClusterReadOnly { .. })));
    assert_eq!(
        handle_daemon_cluster_kv_snapshot(&daemon_paths)
            .expect("snapshot")
            .entries,
        Vec::new()
    );
    assert_eq!(
        handle_daemon_cluster_kv_range_prefix(&daemon_paths, "/config").expect("range"),
        Vec::new()
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn single_node_cluster_has_local_quorum() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-cli-single-node-quorum-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(ClusterId::new("cluster_single_quorum").expect("cluster")),
            node_id: Some(NodeId::new("node_single_quorum").expect("node")),
            name: "single node quorum cluster".to_string(),
        },
        &paths,
    )
    .expect("cluster init");
    let state = read_cluster_state(&paths).expect("cluster state");

    assert!(cluster_has_local_quorum(&paths, &state));
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn daemon_raft_handlers_reject_mismatched_authenticated_signer() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-cli-raft-auth-binding-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(ClusterId::new("cluster_raft_auth").expect("cluster")),
            node_id: Some(NodeId::new("node_raft_auth").expect("node")),
            name: "raft auth cluster".to_string(),
        },
        &paths,
    )
    .expect("cluster init");
    let state = read_cluster_state(&paths).expect("cluster state");
    super::network::sync_cluster_state_to_local_raft_unchecked(&paths, &state)
        .expect("sync state to kv");
    let bad_auth = authenticated_test_command(
        "CLUSTER_RAFT_APPEND",
        super::DaemonSignerAuthority::Voter,
        "node_other",
        "pk_other",
    );

    let append_request = build_cluster_raft_append_request_from_next_index(&paths, &state, 1)
        .expect("append request");
    let append_response = handle_daemon_cluster_raft_append(
        &DaemonPaths::with_root(root.join("daemon")),
        &sinomo_runtime::network::encode_object_text_payload(&append_request)
            .expect("append request object"),
        &bad_auth,
    )
    .expect("append response");
    assert!(!append_response.accepted);

    let (vote_term, last_log_index, last_log_term) = {
        let node = super::network::open_cluster_raft_kv_node(&paths, &state.local_node_id)
            .expect("raft node");
        (
            node.hard_state().current_term.saturating_add(1),
            node.last_log_index(),
            node.last_log_term(),
        )
    };
    let vote_request = sinomo_runtime::network::ClusterRaftVoteRequest {
        state: state.clone(),
        vote: sinomo_runtime::kv::RaftRequestVoteRequest {
            term: vote_term,
            candidate_id: state.local_node_id.clone(),
            last_log_index,
            last_log_term,
        },
    };
    let vote_response = handle_daemon_cluster_raft_vote(
        &DaemonPaths::with_root(root.join("daemon")),
        &sinomo_runtime::network::encode_object_text_payload(&vote_request)
            .expect("vote request object"),
        &bad_auth,
    )
    .expect("vote response");
    assert!(!vote_response.accepted);

    let remote_node_id = NodeId::new("node_forged_voter").expect("remote node");
    let mut forged_state = state.clone();
    let mut forged_member = forged_state.members[0].clone();
    forged_member.node_id = remote_node_id.clone();
    forged_member.display_name = "forged voter".to_string();
    forged_member.role = ClusterMemberRole::Voter;
    forged_member.state = ClusterMemberState::Voter;
    forged_member.node_public_key = "pk_forged_voter".to_string();
    forged_member.endpoints.clear();
    forged_state.members.push(forged_member);
    let forged_auth = authenticated_test_command(
        "CLUSTER_RAFT_APPEND",
        super::DaemonSignerAuthority::Voter,
        remote_node_id.as_str(),
        "pk_forged_voter",
    );
    let forged_append = sinomo_runtime::network::ClusterRaftAppendRequest {
        state: forged_state.clone(),
        snapshot: None,
        append: sinomo_runtime::kv::RaftAppendEntriesRequest {
            term: 1,
            leader_id: remote_node_id.clone(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: Vec::new(),
            leader_commit: 0,
        },
    };
    let forged_append_response = handle_daemon_cluster_raft_append(
        &DaemonPaths::with_root(root.join("daemon")),
        &sinomo_runtime::network::encode_object_text_payload(&forged_append)
            .expect("forged append request"),
        &forged_auth,
    )
    .expect("forged append response");
    assert!(!forged_append_response.accepted);
    assert!(
        !read_cluster_state(&paths)
            .expect("state after forged append")
            .members
            .iter()
            .any(|member| member.node_id == remote_node_id)
    );

    let forged_vote = sinomo_runtime::network::ClusterRaftVoteRequest {
        state: forged_state,
        vote: sinomo_runtime::kv::RaftRequestVoteRequest {
            term: 2,
            candidate_id: remote_node_id.clone(),
            last_log_index: 0,
            last_log_term: 0,
        },
    };
    let forged_vote_response = handle_daemon_cluster_raft_vote(
        &DaemonPaths::with_root(root.join("daemon")),
        &sinomo_runtime::network::encode_object_text_payload(&forged_vote)
            .expect("forged vote request"),
        &forged_auth,
    )
    .expect("forged vote response");
    assert!(!forged_vote_response.accepted);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn stale_raft_snapshot_append_does_not_roll_back_local_state() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-cli-stale-snapshot-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(ClusterId::new("cluster_stale_snapshot").expect("cluster")),
            node_id: Some(NodeId::new("node_stale_snapshot").expect("node")),
            name: "stale snapshot cluster".to_string(),
        },
        &paths,
    )
    .expect("cluster init");
    let state = read_cluster_state(&paths).expect("cluster state");
    super::network::sync_cluster_state_to_local_raft_unchecked(&paths, &state)
        .expect("sync state to kv");
    let stale_snapshot = read_cluster_kv_snapshot(&paths).expect("stale snapshot");
    super::network::append_cluster_raft_command_with_response(
        &paths,
        &state.local_node_id,
        sinomo_runtime::kv::KvCommand::Op(KvOp::Put {
            key: "/config/current".to_string(),
            value: b"current".to_vec(),
            lease_id: None,
        }),
    )
    .expect("current write");
    assert!(
        read_cluster_kv_snapshot(&paths)
            .expect("current snapshot")
            .entries
            .iter()
            .any(|entry| entry.key == "/config/current")
    );

    let (stale_snapshot_term, current_term) = {
        let node = super::network::open_cluster_raft_kv_node(&paths, &state.local_node_id)
            .expect("raft node");
        (
            node.term_at(stale_snapshot.revision.0).unwrap_or(1),
            node.hard_state().current_term,
        )
    };
    let response = super::network::apply_cluster_raft_append(
        &paths,
        &state.local_node_id,
        Some(sinomo_runtime::kv::RaftSnapshot {
            last_included_index: stale_snapshot.revision.0,
            last_included_term: stale_snapshot_term,
            kv: stale_snapshot.clone(),
        }),
        sinomo_runtime::kv::RaftAppendEntriesRequest {
            term: current_term,
            leader_id: state.local_node_id.clone(),
            prev_log_index: stale_snapshot.revision.0,
            prev_log_term: stale_snapshot_term,
            entries: Vec::new(),
            leader_commit: stale_snapshot.revision.0,
        },
    )
    .expect("stale append response");
    assert!(!response.success);
    assert!(
        read_cluster_kv_snapshot(&paths)
            .expect("snapshot after stale append")
            .entries
            .iter()
            .any(|entry| entry.key == "/config/current" && entry.value == b"current")
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn raft_append_request_can_target_follower_next_index() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-cli-targeted-append-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(ClusterId::new("cluster_targeted_append").expect("cluster")),
            node_id: Some(NodeId::new("node_targeted_append").expect("node")),
            name: "targeted append cluster".to_string(),
        },
        &paths,
    )
    .expect("cluster init");
    let state = read_cluster_state(&paths).expect("cluster state");
    super::network::append_cluster_raft_command_with_response(
        &paths,
        &state.local_node_id,
        sinomo_runtime::kv::KvCommand::Op(KvOp::Put {
            key: "/config/targeted/a".to_string(),
            value: b"a".to_vec(),
            lease_id: None,
        }),
    )
    .expect("first raft write");
    super::network::append_cluster_raft_command_with_response(
        &paths,
        &state.local_node_id,
        sinomo_runtime::kv::KvCommand::Op(KvOp::Put {
            key: "/config/targeted/b".to_string(),
            value: b"b".to_vec(),
            lease_id: None,
        }),
    )
    .expect("second raft write");

    let full =
        build_cluster_raft_append_request_from_next_index(&paths, &state, 1).expect("full append");
    let tail =
        build_cluster_raft_append_request_from_next_index(&paths, &state, 2).expect("tail append");
    let heartbeat = build_cluster_raft_append_request_from_next_index(
        &paths,
        &state,
        tail.append.entries.last().expect("tail entry").index + 1,
    )
    .expect("heartbeat append");

    assert!(full.append.entries.len() >= 2);
    assert_eq!(tail.append.prev_log_index, 1);
    assert_eq!(
        tail.append.entries.first().expect("first tail entry").index,
        2
    );
    assert!(heartbeat.append.entries.is_empty());
    assert_eq!(
        heartbeat.append.prev_log_index,
        full.append.entries.last().expect("last entry").index
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn daemon_node_status_publishes_version_and_rollout_to_kv() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-cli-node-status-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let cluster_paths = ClusterPaths::new(&data_dir);
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(ClusterId::new("cluster_status").expect("cluster")),
            node_id: Some(NodeId::new("node_status").expect("node")),
            name: "status cluster".to_string(),
        },
        &cluster_paths,
    )
    .expect("cluster init");
    let daemon_paths = DaemonPaths::with_root(root.join("daemon"));
    daemon_paths.ensure().expect("daemon paths");
    let rollout_paths = SelfRolloutPaths::new(&root);
    ensure_rollout_layout(&rollout_paths).expect("rollout paths");
    std::fs::write(
            &rollout_paths.state,
            r#"{"status":"accepted","mode":"cluster-remote-build","updated_at":"2026-05-04T00:00:00Z"}"#,
        )
        .expect("rollout state");
    let status = DaemonStatus {
        pid: 4242,
        host: "0.0.0.0".to_string(),
        port: 21_353,
        address: "127.0.0.1:21353".to_string(),
        version: daemon_package_version(),
        target_os: Some(std::env::consts::OS.to_string()),
        target_arch: Some(std::env::consts::ARCH.to_string()),
        executable: None,
        rollout: None,
        started_at: "2026-05-04T00:00:00Z".to_string(),
        data_dir: root.clone(),
        lock_file: daemon_paths.lock.clone(),
        cluster: Some(DaemonClusterStatus {
            cluster_id: ClusterId::new("cluster_status").expect("cluster"),
            node_id: NodeId::new("node_status").expect("node"),
            process: super::DAEMON_MAIN_CLUSTER_ROLE.to_string(),
            member_role: ClusterMemberRole::Voter,
            member_state: ClusterMemberState::Active,
        }),
    };
    std::fs::write(
        &daemon_paths.status,
        serde_json::to_vec_pretty(&status).expect("status json"),
    )
    .expect("write status");

    refresh_daemon_node_status_in_kv(&daemon_paths).expect("refresh status");
    let statuses =
        read_cluster_node_statuses_from_kv(&cluster_paths).expect("node statuses from kv");

    assert_eq!(statuses.len(), 1);
    assert_eq!(
        statuses[0].node_id,
        NodeId::new("node_status").expect("node")
    );
    assert_eq!(statuses[0].availability, ClusterNodeAvailability::Online);
    assert_eq!(statuses[0].daemon_version, daemon_package_version());
    assert_eq!(
        statuses[0].rollout.as_ref().expect("rollout").status,
        "accepted"
    );
    assert_eq!(
        read_cluster_rollout_status(&rollout_paths)
            .expect("read rollout")
            .expect("rollout")
            .mode,
        Some("cluster-remote-build".to_string())
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn daemon_cluster_raft_append_replicates_leader_kv_to_peer() {
    let host_root = std::env::temp_dir().join(format!(
        "sinomo-cli-cluster-sync-host-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    let peer_root = std::env::temp_dir().join(format!(
        "sinomo-cli-cluster-sync-peer-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    let host_paths = ClusterPaths::new(&SinomoDataDir::new(&host_root));
    let peer_paths = ClusterPaths::new(&SinomoDataDir::new(&peer_root));
    let cluster_id = ClusterId::new("cluster_sync").expect("cluster");
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(cluster_id.clone()),
            node_id: Some(NodeId::new("node_host").expect("node")),
            name: "sync cluster".to_string(),
        },
        &host_paths,
    )
    .expect("cluster init");

    let host_identity =
        super::load_or_create_main_node_identity(&host_root).expect("host identity");
    let mut invite = ClusterJoinInvite {
        invite_id: ClusterInviteId::new("invite_sync").expect("invite"),
        cluster_id: cluster_id.clone(),
        cluster_name: Some("sync cluster".to_string()),
        cluster_public_key: host_identity.public_key.clone(),
        invitation_secret: "secret_sync".to_string(),
        communication_channels: Vec::new(),
        introducers: Vec::new(),
        token_id: "token_sync".to_string(),
        expires_at: time::OffsetDateTime::now_utc() + time::Duration::seconds(60),
        trust_domain: TrustDomain::new("trust.local").expect("trust"),
        allowed_member_roles: vec![ClusterMemberRole::Learner],
        issuer_node_id: NodeId::new("node_host").expect("node"),
        issuer_public_key: host_identity.public_key.clone(),
        signature: String::new(),
    };
    invite.signature = sign_cluster_join_invite(&invite, &host_identity).expect("sign invite");
    cluster_join_with_paths(
        ClusterJoinOptions {
            invite: encode_cluster_join_invite(&invite).expect("invite uri"),
            cluster_id: Some(cluster_id.clone()),
            node_id: Some(NodeId::new("node_peer").expect("node")),
            name: "peer".to_string(),
        },
        &peer_paths,
    )
    .expect("cluster join");

    let host_state = read_cluster_state(&host_paths).expect("host state");
    let host_auth = authenticated_test_command(
        "CLUSTER_RAFT_APPEND",
        super::DaemonSignerAuthority::Voter,
        host_state.local_node_id.as_str(),
        host_identity.public_key.clone(),
    );
    super::network::sync_cluster_state_to_local_raft_unchecked(&host_paths, &host_state)
        .expect("host kv");
    let request = build_cluster_raft_append_request_from_next_index(&host_paths, &host_state, 1)
        .expect("raft append");
    let peer_response = handle_daemon_cluster_raft_append(
        &DaemonPaths::with_root(peer_root.join("daemon")),
        &sinomo_runtime::network::encode_object_text_payload(&request)
            .expect("raft append request"),
        &host_auth,
    )
    .expect("peer append");

    assert!(peer_response.accepted);
    let peer_members = read_cluster_members_from_kv(&peer_paths).expect("peer kv members");
    assert!(
        peer_members
            .iter()
            .any(|member| member.node_id == NodeId::new("node_host").expect("node"))
    );

    let latency_txn = KvTxn {
        compares: Vec::new(),
        success: vec![KvOp::Put {
            key: "/config/latency".to_string(),
            value: b"current".to_vec(),
            lease_id: None,
        }],
        failure: Vec::new(),
    };
    handle_daemon_cluster_kv_txn(
        &DaemonPaths::with_root(host_root.join("daemon")),
        &serde_json::to_string(&latency_txn).expect("latency txn"),
    )
    .expect("host latency txn");
    let current_request =
        build_cluster_raft_append_request_from_next_index(&host_paths, &host_state, 1)
            .expect("current raft append");
    let current_response = handle_daemon_cluster_raft_append(
        &DaemonPaths::with_root(peer_root.join("daemon")),
        &sinomo_runtime::network::encode_object_text_payload(&current_request)
            .expect("current append request"),
        &host_auth,
    )
    .expect("peer current append");
    assert!(current_response.accepted);
    assert!(
        read_cluster_kv_snapshot(&peer_paths)
            .expect("peer current snapshot")
            .entries
            .iter()
            .any(|entry| entry.key == "/config/latency" && entry.value == b"current")
    );

    let delayed_stale_response = handle_daemon_cluster_raft_append(
        &DaemonPaths::with_root(peer_root.join("daemon")),
        &sinomo_runtime::network::encode_object_text_payload(&request)
            .expect("stale append request"),
        &host_auth,
    )
    .expect("peer stale append");
    assert!(delayed_stale_response.accepted);
    assert!(
        read_cluster_kv_snapshot(&peer_paths)
            .expect("peer after stale snapshot")
            .entries
            .iter()
            .any(|entry| entry.key == "/config/latency" && entry.value == b"current")
    );

    let watch = handle_daemon_cluster_kv_watch_prefix(
        &DaemonPaths::with_root(peer_root.join("daemon")),
        &serde_json::to_string(&sinomo_runtime::network::ClusterKvWatchPrefixRequest {
            prefix: "/config/watch/".to_string(),
            start_revision: KvRevision(1),
        })
        .expect("watch request"),
    )
    .expect("create peer watch");
    let watch_txn = KvTxn {
        compares: Vec::new(),
        success: vec![KvOp::Put {
            key: "/config/watch/a".to_string(),
            value: b"watch-value".to_vec(),
            lease_id: None,
        }],
        failure: Vec::new(),
    };
    handle_daemon_cluster_kv_txn(
        &DaemonPaths::with_root(host_root.join("daemon")),
        &serde_json::to_string(&watch_txn).expect("watch txn"),
    )
    .expect("host watch txn");
    let watch_append =
        build_cluster_raft_append_request_from_next_index(&host_paths, &host_state, 1)
            .expect("watch raft append");
    assert!(
        handle_daemon_cluster_raft_append(
            &DaemonPaths::with_root(peer_root.join("daemon")),
            &sinomo_runtime::network::encode_object_text_payload(&watch_append)
                .expect("watch append request"),
            &host_auth,
        )
        .expect("peer watch append")
        .accepted
    );
    let host_events = read_cluster_kv_snapshot(&host_paths)
        .expect("host watch snapshot")
        .events;
    let watch_fanout = handle_daemon_cluster_kv_watch_events(
        &DaemonPaths::with_root(peer_root.join("daemon")),
        &sinomo_runtime::network::encode_object_text_payload(
            &sinomo_runtime::network::ClusterKvWatchEventsRequest {
                state: host_state.clone(),
                leader_id: host_state.local_node_id.clone(),
                events: host_events,
            },
        )
        .expect("watch events request"),
    )
    .expect("watch event fanout");
    assert!(watch_fanout.accepted);
    let drained = handle_daemon_cluster_kv_watch_drain(
        &DaemonPaths::with_root(peer_root.join("daemon")),
        &serde_json::to_string(&sinomo_runtime::network::ClusterKvWatchDrainRequest {
            watch_id: watch.watch_id,
        })
        .expect("watch drain request"),
    )
    .expect("drain peer watch");
    assert!(
        drained
            .events
            .iter()
            .any(|event| event.key == "/config/watch/a")
    );
    let drained_again = handle_daemon_cluster_kv_watch_drain(
        &DaemonPaths::with_root(peer_root.join("daemon")),
        &serde_json::to_string(&sinomo_runtime::network::ClusterKvWatchDrainRequest {
            watch_id: watch.watch_id,
        })
        .expect("watch drain request"),
    )
    .expect("drain peer watch again");
    assert!(drained_again.events.is_empty());
    std::fs::remove_dir_all(host_root).ok();
    std::fs::remove_dir_all(peer_root).ok();
}

#[test]
fn rollout_parser_accepts_apply_status_and_init_source() {
    assert_eq!(
        parse_rollout_command(&["status".to_string()]).expect("status"),
        RolloutCliCommand::Status
    );
    assert_eq!(
        parse_rollout_command(&["apply".to_string()]).expect("apply"),
        RolloutCliCommand::Apply(RolloutOptions::default())
    );
    assert_eq!(
        parse_rollout_command(&[
            "import-source".to_string(),
            "--repo".to_string(),
            "/work/sinomo".to_string()
        ])
        .expect("import-source"),
        RolloutCliCommand::ImportSource(RolloutOptions {
            repo: Some(std::path::PathBuf::from("/work/sinomo")),
            ..RolloutOptions::default()
        })
    );
    assert_eq!(
        parse_rollout_command(&[
            "export-source".to_string(),
            "--repo".to_string(),
            "/work/sinomo".to_string()
        ])
        .expect("export-source"),
        RolloutCliCommand::ExportSource(RolloutOptions {
            repo: Some(std::path::PathBuf::from("/work/sinomo")),
            ..RolloutOptions::default()
        })
    );
    assert_eq!(
        parse_rollout_command(&[
            "apply-external".to_string(),
            "--repo".to_string(),
            "/work/sinomo".to_string()
        ])
        .expect("apply-external"),
        RolloutCliCommand::ApplyExternal(RolloutOptions {
            repo: Some(std::path::PathBuf::from("/work/sinomo")),
            ..RolloutOptions::default()
        })
    );
    assert_eq!(
        parse_rollout_command(&[
            "init-source".to_string(),
            "--repo".to_string(),
            "/work/sinomo".to_string()
        ])
        .expect("init-source"),
        RolloutCliCommand::InitSource(RolloutOptions {
            repo: Some(std::path::PathBuf::from("/work/sinomo")),
            host: "127.0.0.1".to_string(),
            port: 21_353,
            ..RolloutOptions::default()
        })
    );
    assert_eq!(
        parse_rollout_command(&[
            "cluster".to_string(),
            "--repo".to_string(),
            "/work/sinomo".to_string(),
            "--target".to_string(),
            "192.168.1.20:21353".to_string(),
            "--shadow-port".to_string(),
            "21454".to_string(),
            "--skip-local".to_string(),
        ])
        .expect("cluster"),
        RolloutCliCommand::Cluster(RolloutOptions {
            repo: Some(std::path::PathBuf::from("/work/sinomo")),
            targets: vec!["192.168.1.20:21353".to_string()],
            shadow_port: 21_454,
            skip_local: true,
            ..RolloutOptions::default()
        })
    );
    assert_eq!(
        parse_rollout_command(&[
            "cluster".to_string(),
            "--remote-build".to_string(),
            "--remote-repo".to_string(),
            "/srv/sinomo-agent".to_string(),
            "--target".to_string(),
            "10.14.1.11:21353".to_string(),
            "--skip-local".to_string(),
        ])
        .expect("remote-build cluster"),
        RolloutCliCommand::Cluster(RolloutOptions {
            remote_build: true,
            remote_repo: Some(std::path::PathBuf::from("/srv/sinomo-agent")),
            targets: vec!["10.14.1.11:21353".to_string()],
            skip_local: true,
            ..RolloutOptions::default()
        })
    );
    assert_eq!(
        parse_rollout_command(&[
            "diagnostics".to_string(),
            "--target".to_string(),
            "10.14.1.11:21353".to_string(),
        ])
        .expect("diagnostics"),
        RolloutCliCommand::Diagnostics(RolloutOptions {
            targets: vec!["10.14.1.11:21353".to_string()],
            ..RolloutOptions::default()
        })
    );
}

#[test]
fn daemon_rollout_build_rejects_missing_remote_repo() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-cli-rollout-build-missing-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    let request = ClusterRolloutBuildRequest {
        source: "test".to_string(),
        remote_repo: None,
        source_bundle_base64: None,
        source_head: None,
        host: "127.0.0.1".to_string(),
        port: 21_353,
        shadow_port: 21_354,
        requested_at: "now".to_string(),
    };
    let payload =
        sinomo_runtime::network::encode_object_text_payload(&request).expect("request object");
    let response =
        handle_daemon_rollout_build(&DaemonPaths::with_root(root.join("daemon")), &payload)
            .expect("build response");

    assert_eq!(response.status, "rejected");
    assert!(
        response
            .detail
            .contains("remote source repo is not initialized")
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn daemon_rollout_build_imports_source_bundle_before_building() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-cli-rollout-build-bundle-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    let source_repo = root.join("source");
    let bundle = root.join("source.bundle");
    std::fs::create_dir_all(&source_repo).expect("source repo dir");
    assert!(
        Command::new("git")
            .arg("init")
            .current_dir(&source_repo)
            .status()
            .expect("git init")
            .success()
    );
    assert!(
        Command::new("git")
            .args(["config", "user.email", "test@example.invalid"])
            .current_dir(&source_repo)
            .status()
            .expect("git config email")
            .success()
    );
    assert!(
        Command::new("git")
            .args(["config", "user.name", "Sinomo Test"])
            .current_dir(&source_repo)
            .status()
            .expect("git config name")
            .success()
    );
    std::fs::write(source_repo.join("README.md"), "bundle source\n").expect("readme");
    assert!(
        Command::new("git")
            .args(["add", "-A"])
            .current_dir(&source_repo)
            .status()
            .expect("git add")
            .success()
    );
    assert!(
        Command::new("git")
            .args(["commit", "-m", "bundle-source"])
            .current_dir(&source_repo)
            .status()
            .expect("git commit")
            .success()
    );
    let expected_head = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(&source_repo)
        .output()
        .expect("git rev-parse");
    assert!(expected_head.status.success());
    let expected_head = String::from_utf8_lossy(&expected_head.stdout)
        .trim()
        .to_string();

    create_source_repo_bundle(&source_repo, &bundle).expect("source bundle");
    let request = ClusterRolloutBuildRequest {
        source: "test".to_string(),
        remote_repo: None,
        source_bundle_base64: Some(
            super::URL_SAFE_NO_PAD.encode(std::fs::read(&bundle).expect("bundle bytes")),
        ),
        source_head: Some(expected_head.clone()),
        host: "127.0.0.1".to_string(),
        port: 21_353,
        shadow_port: 21_354,
        requested_at: "now".to_string(),
    };
    let payload =
        sinomo_runtime::network::encode_object_text_payload(&request).expect("request object");
    let response =
        handle_daemon_rollout_build(&DaemonPaths::with_root(root.join("daemon")), &payload)
            .expect("build response");
    let rollout_paths = SelfRolloutPaths::new(&root);
    let source_status = managed_source_status(&rollout_paths).expect("managed source status");

    assert_eq!(response.status, "rejected");
    assert!(
        response
            .detail
            .contains("remote source repo is not initialized")
    );
    assert_eq!(source_status.head.as_deref(), Some(expected_head.as_str()));
    assert!(!source_status.has_external_remotes);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn rollout_main_activation_requires_running_shadow() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-cli-rollout-shadow-required-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    let rollout_paths = SelfRolloutPaths::new(&root);
    ensure_rollout_layout(&rollout_paths).expect("layout");
    let error = stage_rollout_main_activation(
        &root,
        &rollout_paths,
        RolloutMainActivation {
            source: "test".to_string(),
            main_binary: root.join("bin").join("candidate-main"),
            shadow_binary: root.join("bin").join("candidate-shadow"),
            staged_main_binary: None,
            staged_shadow_binary: None,
            previous_main_binary: Some(root.join("bin").join("previous-main")),
            main_host: "0.0.0.0".to_string(),
            main_port: 21_353,
            shadow_port: 21_354,
            requested_at: "now".to_string(),
            mode: "test".to_string(),
        },
    )
    .expect_err("shadow required");

    assert!(matches!(error, CliError::DaemonNotRunning { .. }));
    assert!(!super::rollout_main_activation_path(&rollout_paths).exists());
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn rollout_parser_reuses_daemon_endpoint_defaults() {
    assert_eq!(
        parse_rollout_command(&[
            "apply".to_string(),
            "--host".to_string(),
            "0.0.0.0".to_string(),
            "--port".to_string(),
            "21400".to_string()
        ])
        .expect("apply custom"),
        RolloutCliCommand::Apply(RolloutOptions {
            repo: None,
            host: "0.0.0.0".to_string(),
            port: 21_400,
            ..RolloutOptions::default()
        })
    );
}

#[test]
fn daemon_parser_supports_shadow_daemon_on_separate_default_port() {
    assert_eq!(
        parse_daemon_command(&["shadow".to_string(), "run".to_string()]).expect("shadow"),
        DaemonCliCommand::ShadowRun(DaemonRunOptions {
            host: "127.0.0.1".to_string(),
            port: 21_354,
        })
    );
    assert_eq!(
        parse_daemon_command(&["shadow".to_string(), "status".to_string()]).expect("shadow"),
        DaemonCliCommand::ShadowStatus(DaemonRunOptions {
            host: "127.0.0.1".to_string(),
            port: 21_354,
        })
    );
}

#[test]
fn daemon_paths_for_shadow_are_separate_from_main_daemon() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-cli-daemon-shadow-paths-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let main = super::DaemonPaths::new(&data_dir);
    let shadow = super::DaemonPaths::for_shadow(&data_dir);

    assert_eq!(main.root, root.join("daemon"));
    assert_eq!(shadow.root, root.join("shadow-daemon"));
    assert_ne!(main.lock, shadow.lock);
    assert_ne!(main.status, shadow.status);
}

#[test]
fn daemon_parser_uses_port_21353_by_default() {
    assert_eq!(
        parse_daemon_command(&["run".to_string()]).expect("daemon run"),
        DaemonCliCommand::Run(DaemonRunOptions {
            host: "127.0.0.1".to_string(),
            port: 21_353,
        })
    );
    assert_eq!(
        parse_daemon_command(&["status".to_string()]).expect("daemon status"),
        DaemonCliCommand::Status(DaemonRunOptions {
            host: "127.0.0.1".to_string(),
            port: 21_353,
        })
    );
    assert_eq!(
        parse_daemon_command(&[
            "run".to_string(),
            "--host".to_string(),
            "0.0.0.0".to_string(),
            "--port".to_string(),
            "21354".to_string(),
        ])
        .expect("daemon run custom"),
        DaemonCliCommand::Run(DaemonRunOptions {
            host: "0.0.0.0".to_string(),
            port: 21_354,
        })
    );
}

#[test]
fn daemon_paths_live_under_data_directory() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-cli-daemon-paths-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_nanos()
    ));
    let paths = super::DaemonPaths::new(&SinomoDataDir::new(&root));

    assert_eq!(paths.root, root.join("daemon"));
    assert_eq!(paths.lock, root.join("daemon").join("daemon.lock"));
    assert_eq!(paths.status, root.join("daemon").join("status.json"));
}

#[test]
fn daemon_client_stream_is_blocking_after_connect_timeout() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let address = listener.local_addr().expect("local addr").to_string();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept");
        let mut request = String::new();
        std::io::BufReader::new(stream.try_clone().expect("clone"))
            .read_line(&mut request)
            .expect("read request");
        assert_eq!(request.trim(), "PING");
        thread::sleep(Duration::from_millis(50));
        stream.write_all(b"PONG\n").expect("write response");
    });

    let response =
        send_daemon_command_raw(&address, "PING", Duration::from_secs(1)).expect("response");
    server.join().expect("server");

    assert_eq!(response.trim(), "PONG");
}

#[test]
fn daemon_accepted_stream_is_reset_to_blocking_for_delayed_client_input() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-cli-daemon-blocking-stream-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = DaemonPaths::new(&data_dir);
    paths.ensure().expect("daemon paths");
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    listener.set_nonblocking(true).expect("nonblocking");
    let address = listener.local_addr().expect("local addr");
    let client = thread::spawn(move || {
        let mut stream = TcpStream::connect(address).expect("connect");
        thread::sleep(Duration::from_millis(50));
        stream.write_all(b"STATUS\n").expect("write");
        stream
            .shutdown(std::net::Shutdown::Write)
            .expect("shutdown");
        let mut response = String::new();
        match stream.read_to_string(&mut response) {
            Ok(_) => {}
            Err(error)
                if error.kind() == std::io::ErrorKind::ConnectionReset && !response.is_empty() => {}
            Err(error) => panic!("read: {error}"),
        }
        response
    });
    let mut accepted = loop {
        match listener.accept() {
            Ok((stream, _)) => break stream,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(5));
            }
            Err(error) => panic!("accept: {error}"),
        }
    };
    accepted.set_nonblocking(false).expect("blocking stream");

    let should_shutdown = handle_daemon_connection(&mut accepted, &paths).expect("handle");
    drop(accepted);
    let response = client.join().expect("client");

    assert!(!should_shutdown);
    assert!(response.is_empty());
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn shadow_daemon_status_probe_does_not_wait_for_idle_sleep() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-cli-shadow-status-latency-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    data_dir.ensure_root_layout().expect("data dir");
    let port_listener = TcpListener::bind("127.0.0.1:0").expect("reserve port");
    let port = port_listener.local_addr().expect("local addr").port();
    drop(port_listener);
    let address = format!("127.0.0.1:{port}");
    let shadow_paths = DaemonPaths::for_shadow(&data_dir);
    let main_paths = DaemonPaths::new(&data_dir);
    let rollout_paths = SelfRolloutPaths::new(&root);
    let shadow_options = DaemonRunOptions {
        host: "127.0.0.1".to_string(),
        port,
    };
    let daemon = thread::spawn({
        let shadow_options = shadow_options.clone();
        let root = root.clone();
        move || {
            run_shadow_daemon_with_paths(
                shadow_options,
                shadow_paths,
                main_paths,
                rollout_paths,
                DaemonRunOptions::default(),
                root.join("missing-main-binary"),
                None,
            )
        }
    });

    for _ in 0..50 {
        let signed = sign_daemon_command_from_data_dir(&data_dir, "STATUS").expect("sign status");
        if send_daemon_command_raw(&address, &signed, Duration::from_millis(250)).is_ok() {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }

    let mut durations = Vec::new();
    for _ in 0..3 {
        let signed = sign_daemon_command_from_data_dir(&data_dir, "STATUS").expect("sign status");
        let started = Instant::now();
        send_daemon_command_raw(&address, &signed, Duration::from_secs(2))
            .expect("status response");
        durations.push(started.elapsed());
    }

    let signed = sign_daemon_command_from_data_dir(&data_dir, "SHUTDOWN").expect("sign shutdown");
    let _ = send_daemon_command_raw(&address, &signed, Duration::from_secs(2));
    daemon
        .join()
        .expect("shadow daemon")
        .expect("shadow result");

    assert!(
        durations
            .iter()
            .all(|duration| *duration < Duration::from_millis(250)),
        "shadow daemon status probes waited for the idle sleep: {durations:?}"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn daemon_invalid_invite_hello_is_silent() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-cli-daemon-invalid-invite-hello-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let cluster_paths = ClusterPaths::new(&data_dir);
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(ClusterId::new("cluster_invalid_invite_hello").expect("cluster")),
            node_id: Some(NodeId::new("node_invalid_invite_hello").expect("node")),
            name: "invalid invite hello".to_string(),
        },
        &cluster_paths,
    )
    .expect("cluster init");
    let paths = DaemonPaths::new(&data_dir);
    paths.ensure().expect("daemon paths");
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let address = listener.local_addr().expect("local addr");
    let client = thread::spawn(move || {
        let mut stream = TcpStream::connect(address).expect("connect");
        stream
            .write_all(
                br#"INVITE_HELLO {"protocol":"sinomo.cluster-invite-challenge.v1","invite_id":"invite_missing","challenge":"probe"}
"#,
            )
            .expect("write invite hello");
        stream
            .shutdown(std::net::Shutdown::Write)
            .expect("shutdown");
        let mut response = String::new();
        match stream.read_to_string(&mut response) {
            Ok(_) => {}
            Err(error)
                if error.kind() == std::io::ErrorKind::ConnectionReset && response.is_empty() => {}
            Err(error) => panic!("read: {error}"),
        }
        response
    });
    let (mut accepted, _) = listener.accept().expect("accept");
    let should_shutdown = handle_daemon_connection(&mut accepted, &paths).expect("handle");
    drop(accepted);
    let response = client.join().expect("client");

    assert!(!should_shutdown);
    assert!(response.is_empty());
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn daemon_lock_prevents_duplicate_instances_and_preserves_probe_status() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-cli-daemon-lock-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_nanos()
    ));
    let paths = super::DaemonPaths::new(&SinomoDataDir::new(&root));
    paths.ensure().expect("daemon paths");

    let lock = super::DaemonLock::acquire(&paths, &DaemonRunOptions::default(), None)
        .expect("first daemon lock");
    assert!(matches!(
        super::probe_daemon_status(&paths, Duration::from_millis(1)),
        DaemonProbeStatus::Locked { .. }
    ));
    assert!(super::DaemonLock::acquire(&paths, &DaemonRunOptions::default(), None).is_err());

    drop(lock);
    assert!(matches!(
        super::probe_daemon_status(&paths, Duration::from_millis(1)),
        DaemonProbeStatus::NotRunning
    ));
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn evolve_propose_parser_accepts_common_flags() {
    let options = parse_propose_options(&[
        "improve scorecards".to_string(),
        "--provider".to_string(),
        "github-copilot".to_string(),
        "--model".to_string(),
        "gpt-5.4".to_string(),
        "--allow".to_string(),
        "crates/sinomo-eval/**".to_string(),
        "--verify".to_string(),
        "cargo test -p sinomo-eval".to_string(),
        "--target-ring".to_string(),
        "r1".to_string(),
        "--dry-run".to_string(),
    ])
    .expect("options");

    assert_eq!(options.objective, "improve scorecards");
    assert_eq!(options.provider.as_deref(), Some("github-copilot"));
    assert_eq!(options.model.as_deref(), Some("gpt-5.4"));
    assert_eq!(options.allow, vec!["crates/sinomo-eval/**"]);
    assert_eq!(options.verify, vec!["cargo test -p sinomo-eval"]);
    assert_eq!(options.target_ring, TrustRing::R1TrustedRuntime);
    assert!(options.dry_run);
}

#[test]
fn evolve_propose_parser_accepts_campaign_phase_zero_flags() {
    let options = parse_propose_options(&[
        "--objective".to_string(),
        "speedrun minecraft".to_string(),
        "--horizon".to_string(),
        "30 days".to_string(),
        "--budget-policy".to_string(),
        "free-and-prepaid".to_string(),
        "--reserve".to_string(),
        "coding-plan-weekly:30%".to_string(),
        "--reserve".to_string(),
        "free-api-openrouter:40%".to_string(),
        "--approval".to_string(),
        "stage-rollout".to_string(),
        "--max-wall-time".to_string(),
        "10m".to_string(),
        "--pause-after-proposal".to_string(),
        "--dry-run".to_string(),
    ])
    .expect("options");

    assert_eq!(options.objective, "speedrun minecraft");
    assert_eq!(options.horizon.as_deref(), Some("30 days"));
    assert_eq!(options.budget_policy.as_deref(), Some("free-and-prepaid"));
    assert_eq!(
        options.reserve_floors,
        vec!["coding-plan-weekly:30%", "free-api-openrouter:40%"]
    );
    assert_eq!(options.approval_policy.as_deref(), Some("stage-rollout"));
    assert_eq!(options.max_wall_time.as_deref(), Some("10m"));
    assert!(options.pause_after_proposal);
    assert!(options.dry_run);
}

#[test]
fn evolve_tick_parser_accepts_phase_four_background_flags() {
    let command = super::parse_evolve_command(&[
        "tick".to_string(),
        "--dry-run".to_string(),
        "--scheduler-service".to_string(),
        "sinomo-cluster-scheduler".to_string(),
        "--max-concurrent".to_string(),
        "1".to_string(),
        "--max-concurrent-workloads".to_string(),
        "4".to_string(),
        "--workload".to_string(),
        "backup:backup-index-01:ready:Backup index".to_string(),
        "--workload-estimate".to_string(),
        "backup:backup-index-01:cluster-background-hours:40".to_string(),
        "--workload-human-checkpoint".to_string(),
        "memory:dataset-curation-42".to_string(),
        "--resource".to_string(),
        "coding-plan-weekly:75/100:weekly".to_string(),
        "--estimate".to_string(),
        "coding-plan-weekly:10".to_string(),
        "--reset-window-open".to_string(),
        "--idle-window-open".to_string(),
    ])
    .expect("tick command");
    assert!(matches!(command, super::EvolveCliCommand::Tick(_)));

    let options = super::parse_background_tick_options(&[
        "--dry-run".to_string(),
        "--scheduler-service".to_string(),
        "sinomo-cluster-scheduler".to_string(),
        "--max-concurrent".to_string(),
        "1".to_string(),
        "--max-concurrent-workloads".to_string(),
        "4".to_string(),
        "--workload".to_string(),
        "backup:backup-index-01:ready:Backup index".to_string(),
        "--workload-estimate".to_string(),
        "backup:backup-index-01:cluster-background-hours:40".to_string(),
        "--workload-human-checkpoint".to_string(),
        "memory:dataset-curation-42".to_string(),
        "--resource".to_string(),
        "coding-plan-weekly:75/100:weekly".to_string(),
        "--estimate".to_string(),
        "coding-plan-weekly:10".to_string(),
        "--reset-window-open".to_string(),
        "--idle-window-open".to_string(),
    ])
    .expect("tick options");

    assert!(options.dry_run);
    assert_eq!(options.scheduler_service_id, "sinomo-cluster-scheduler");
    assert_eq!(options.max_concurrent_campaigns, 1);
    assert_eq!(options.max_concurrent_workloads, 4);
    assert_eq!(options.resources, vec!["coding-plan-weekly:75/100:weekly"]);
    assert_eq!(options.estimates, vec!["coding-plan-weekly:10"]);
    assert_eq!(
        options.external_workloads,
        vec!["backup:backup-index-01:ready:Backup index"]
    );
    assert_eq!(
        options.external_workload_estimates,
        vec!["backup:backup-index-01:cluster-background-hours:40"]
    );
    assert_eq!(
        options.external_human_checkpoints,
        vec!["memory:dataset-curation-42"]
    );
    let workloads = super::parse_background_tick_external_workloads(
        &options.external_workloads,
        &options.external_workload_estimates,
        &options.external_human_checkpoints,
    )
    .expect("external workloads");
    assert_eq!(workloads.len(), 2);
    assert_eq!(workloads[0].system, "backup");
    assert_eq!(
        workloads[0].resource_estimates[0].resource,
        "cluster-background-hours"
    );
    assert_eq!(workloads[0].resource_estimates[0].estimated_units, 40);
    assert!(workloads[1].human_checkpoint_required);
    assert!(options.reset_window_open);
    assert!(options.idle_window_open);
}

#[test]
fn evolve_tick_parser_rejects_cluster_identifier_flags() {
    let result = super::parse_background_tick_options(&[
        "--dry-run".to_string(),
        "--cluster".to_string(),
        "home-cluster".to_string(),
    ]);

    assert!(result.is_err());
}

#[test]
fn evolve_evaluate_parser_accepts_phase_three_stage_flags() {
    let command = super::parse_evolve_command(&[
        "evaluate".to_string(),
        "camp_demo".to_string(),
        "--job".to_string(),
        "evojob_demo".to_string(),
        "--task".to_string(),
        "task_demo".to_string(),
        "--failure-class".to_string(),
        "foundation-blocker".to_string(),
        "--root-cause".to_string(),
        "stage evidence missing from packet".to_string(),
        "--learned".to_string(),
        "add stage evidence before retry".to_string(),
    ])
    .expect("evaluate command");
    assert!(matches!(command, super::EvolveCliCommand::Evaluate(_)));

    let options = super::parse_stage_evaluation_options(&[
        "camp_demo".to_string(),
        "--job".to_string(),
        "evojob_demo".to_string(),
        "--task".to_string(),
        "task_demo".to_string(),
        "--failure-class".to_string(),
        "foundation-blocker".to_string(),
        "--root-cause".to_string(),
        "stage evidence missing from packet".to_string(),
        "--learned".to_string(),
        "add stage evidence before retry".to_string(),
    ])
    .expect("stage evaluation options");

    assert_eq!(options.campaign_id, "camp_demo");
    assert_eq!(options.job_id.as_str(), "evojob_demo");
    assert_eq!(options.task_id, "task_demo");
    assert_eq!(
        options.failure_class,
        Some(sinomo_evolution::EvolutionFailureClass::FoundationBlocker)
    );
    assert_eq!(
        options.root_cause_summary.as_deref(),
        Some("stage evidence missing from packet")
    );
    assert_eq!(
        options.what_was_learned,
        vec!["add stage evidence before retry".to_string()]
    );
}

#[test]
fn evolve_iterate_parser_accepts_phase_two_dry_run_flags() {
    let command = super::parse_evolve_command(&[
        "iterate".to_string(),
        "camp_demo".to_string(),
        "--task".to_string(),
        "task_demo".to_string(),
        "--path".to_string(),
        "crates/sinomo-evolution/**".to_string(),
        "--verify".to_string(),
        "cargo test -p sinomo-evolution --lib".to_string(),
        "--dry-run".to_string(),
    ])
    .expect("iterate command");
    assert!(matches!(command, super::EvolveCliCommand::Iterate(_)));

    let options = super::parse_iteration_options(&[
        "camp_demo".to_string(),
        "--task".to_string(),
        "task_demo".to_string(),
        "--path".to_string(),
        "crates/sinomo-evolution/**".to_string(),
        "--verify".to_string(),
        "cargo test -p sinomo-evolution --lib".to_string(),
        "--dry-run".to_string(),
    ])
    .expect("iteration options");

    assert_eq!(options.campaign_id, "camp_demo");
    assert_eq!(options.task_id.as_deref(), Some("task_demo"));
    assert_eq!(
        options.intended_paths,
        vec!["crates/sinomo-evolution/**".to_string()]
    );
    assert_eq!(
        options.verification_commands,
        vec!["cargo test -p sinomo-evolution --lib".to_string()]
    );
    assert!(options.dry_run);
}

#[test]
fn campaign_iteration_dry_run_persists_phase_two_handoff_artifacts() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-cli-campaign-iterate-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = EvolutionPaths::new(&data_dir);
    paths.ensure().expect("paths");
    let mut campaign =
        sinomo_evolution::propose_evolution_campaign(sinomo_evolution::CampaignProposalRequest {
            objective: "improve scheduler iteration dispatch".to_string(),
            horizon: "14 days".to_string(),
            budget_policy: "free-and-prepaid".to_string(),
            reserve_floors: vec![sinomo_evolution::ReserveFloor {
                resource: "coding-plan-weekly".to_string(),
                floor_percent: 30,
            }],
            approval_policy: "stage-rollout".to_string(),
            max_wall_time: Some("10m".to_string()),
            pause_after_proposal: false,
            dry_run: true,
            approved: true,
            target_ring: TrustRing::R1TrustedRuntime,
            source_repo: Some("/repo".to_string()),
        });
    campaign.status = sinomo_evolution::EvolutionCampaignStatus::Scheduled;
    let task_id = campaign.near_term_tasks[0].task_id.clone();
    super::write_campaign_bundle(&paths, &campaign).expect("bundle");
    let plan = sinomo_evolution::prepare_scheduled_iteration(
        sinomo_evolution::ScheduledIterationRequest {
            campaign: campaign.clone(),
            task_id: Some(task_id.clone()),
            intended_paths: vec!["crates/sinomo-evolution/**".to_string()],
            extra_verification_commands: vec!["cargo test -p sinomo-evolution --lib".to_string()],
            dry_run: true,
        },
    )
    .expect("iteration plan");

    let bundle = super::write_campaign_iteration_dry_run(&paths, &mut campaign, &plan)
        .expect("iteration artifacts");

    assert!(bundle.iteration_plan_json.exists());
    assert!(bundle.iteration_request_json.exists());
    assert!(bundle.iteration_dispatches_jsonl.exists());
    let persisted: sinomo_evolution::EvolutionCampaign = serde_json::from_str(
        &std::fs::read_to_string(&bundle.campaign_json).expect("campaign json"),
    )
    .expect("campaign parse");
    assert_eq!(
        persisted.status,
        sinomo_evolution::EvolutionCampaignStatus::Scheduled
    );
    assert!(
        std::fs::read_to_string(&bundle.iteration_dispatches_jsonl)
            .expect("dispatch jsonl")
            .contains(&plan.iteration_id)
    );
    assert!(
        std::fs::read_to_string(&bundle.decisions_jsonl)
            .expect("decisions jsonl")
            .contains("TaskSelected")
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn evolve_schedule_parser_accepts_phase_one_resource_dry_run_flags() {
    let command = super::parse_evolve_command(&[
        "schedule".to_string(),
        "camp_demo".to_string(),
        "--resource".to_string(),
        "coding-plan-weekly:65/100:weekly".to_string(),
        "--estimate".to_string(),
        "coding-plan-weekly:10".to_string(),
        "--dry-run".to_string(),
    ])
    .expect("schedule command");
    assert!(matches!(command, super::EvolveCliCommand::Schedule(_)));

    let options = super::parse_schedule_options(&[
        "camp_demo".to_string(),
        "--resource".to_string(),
        "coding-plan-weekly:65/100:weekly".to_string(),
        "--estimate".to_string(),
        "coding-plan-weekly:10".to_string(),
        "--dry-run".to_string(),
    ])
    .expect("schedule options");

    assert_eq!(options.campaign_id, "camp_demo");
    assert_eq!(options.resources, vec!["coding-plan-weekly:65/100:weekly"]);
    assert_eq!(options.estimates, vec!["coding-plan-weekly:10"]);
    assert!(options.dry_run);
}

#[test]
fn campaign_scheduler_dry_run_persists_phase_one_artifacts() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-cli-campaign-schedule-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = EvolutionPaths::new(&data_dir);
    paths.ensure().expect("paths");
    let mut campaign =
        sinomo_evolution::propose_evolution_campaign(sinomo_evolution::CampaignProposalRequest {
            objective: "improve scheduler".to_string(),
            horizon: "14 days".to_string(),
            budget_policy: "free-and-prepaid".to_string(),
            reserve_floors: vec![sinomo_evolution::ReserveFloor {
                resource: "coding-plan-weekly".to_string(),
                floor_percent: 30,
            }],
            approval_policy: "stage-rollout".to_string(),
            max_wall_time: Some("10m".to_string()),
            pause_after_proposal: false,
            dry_run: true,
            approved: true,
            target_ring: TrustRing::R1TrustedRuntime,
            source_repo: Some("/repo".to_string()),
        });
    let task_id = campaign.near_term_tasks[0].task_id.clone();
    let _bundle = super::write_campaign_bundle(&paths, &campaign).expect("bundle");
    let dry_run = sinomo_evolution::dry_run_resource_scheduler(
        sinomo_evolution::ResourceSchedulerDryRunRequest {
            campaign: campaign.clone(),
            preview_horizon: "24h".to_string(),
            resources: vec![sinomo_evolution::ResourceClassDefinition {
                resource_id: "coding-plan-weekly".to_string(),
                unit: "percent".to_string(),
                total_units: 100,
                used_units: 65,
                reserve_floor_percent: 30,
                reset_window: "weekly".to_string(),
                notes: "test budget".to_string(),
            }],
            task_estimates: vec![sinomo_evolution::ResourceUsageEstimate {
                task_id,
                resource: "coding-plan-weekly".to_string(),
                estimated_units: 10,
                actual_units: Some(65),
            }],
        },
    );

    let bundle = super::write_campaign_schedule_dry_run(&paths, &mut campaign, &dry_run)
        .expect("scheduler artifacts");

    assert!(bundle.resource_ledger_json.exists());
    assert!(bundle.scheduler_dry_run_json.exists());
    assert!(bundle.lease_requests_jsonl.exists());
    assert!(bundle.lease_decisions_jsonl.exists());
    let persisted: sinomo_evolution::EvolutionCampaign = serde_json::from_str(
        &std::fs::read_to_string(&bundle.campaign_json).expect("campaign json"),
    )
    .expect("campaign parse");
    assert_eq!(
        persisted.status,
        sinomo_evolution::EvolutionCampaignStatus::PausedBudget
    );
    assert!(
        std::fs::read_to_string(&bundle.decisions_jsonl)
            .expect("decisions jsonl")
            .contains("Paused")
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn campaign_scheduler_dry_run_records_resume_when_budget_recovers() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-cli-campaign-resume-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = EvolutionPaths::new(&data_dir);
    paths.ensure().expect("paths");
    let mut campaign =
        sinomo_evolution::propose_evolution_campaign(sinomo_evolution::CampaignProposalRequest {
            objective: "improve scheduler".to_string(),
            horizon: "14 days".to_string(),
            budget_policy: "free-and-prepaid".to_string(),
            reserve_floors: vec![sinomo_evolution::ReserveFloor {
                resource: "coding-plan-weekly".to_string(),
                floor_percent: 30,
            }],
            approval_policy: "stage-rollout".to_string(),
            max_wall_time: Some("10m".to_string()),
            pause_after_proposal: false,
            dry_run: true,
            approved: true,
            target_ring: TrustRing::R1TrustedRuntime,
            source_repo: Some("/repo".to_string()),
        });
    campaign.status = sinomo_evolution::EvolutionCampaignStatus::PausedBudget;
    let task_id = campaign.near_term_tasks[0].task_id.clone();
    super::write_campaign_bundle(&paths, &campaign).expect("bundle");
    let dry_run = sinomo_evolution::dry_run_resource_scheduler(
        sinomo_evolution::ResourceSchedulerDryRunRequest {
            campaign: campaign.clone(),
            preview_horizon: "24h".to_string(),
            resources: vec![sinomo_evolution::ResourceClassDefinition {
                resource_id: "coding-plan-weekly".to_string(),
                unit: "percent".to_string(),
                total_units: 100,
                used_units: 20,
                reserve_floor_percent: 30,
                reset_window: "weekly".to_string(),
                notes: "test budget".to_string(),
            }],
            task_estimates: vec![sinomo_evolution::ResourceUsageEstimate {
                task_id,
                resource: "coding-plan-weekly".to_string(),
                estimated_units: 10,
                actual_units: Some(20),
            }],
        },
    );

    let bundle = super::write_campaign_schedule_dry_run(&paths, &mut campaign, &dry_run)
        .expect("scheduler artifacts");

    let persisted: sinomo_evolution::EvolutionCampaign = serde_json::from_str(
        &std::fs::read_to_string(&bundle.campaign_json).expect("campaign json"),
    )
    .expect("campaign parse");
    assert_eq!(
        persisted.status,
        sinomo_evolution::EvolutionCampaignStatus::Scheduled
    );
    assert!(
        std::fs::read_to_string(&bundle.decisions_jsonl)
            .expect("decisions jsonl")
            .contains("Resumed")
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn campaign_writer_persists_phase_zero_bundle_under_data_dir() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-cli-campaign-bundle-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = EvolutionPaths::new(&data_dir);
    paths.ensure().expect("paths");
    let campaign =
        sinomo_evolution::propose_evolution_campaign(sinomo_evolution::CampaignProposalRequest {
            objective: "improve scheduler".to_string(),
            horizon: "14 days".to_string(),
            budget_policy: "free-and-prepaid".to_string(),
            reserve_floors: vec![sinomo_evolution::ReserveFloor {
                resource: "coding-plan-weekly".to_string(),
                floor_percent: 25,
            }],
            approval_policy: "stage-rollout".to_string(),
            max_wall_time: Some("10m".to_string()),
            pause_after_proposal: true,
            dry_run: true,
            approved: false,
            target_ring: TrustRing::R1TrustedRuntime,
            source_repo: Some("/repo".to_string()),
        });

    let bundle = super::write_campaign_bundle(&paths, &campaign).expect("bundle");

    assert_eq!(bundle.root, paths.campaign_dir(&campaign.campaign_id));
    assert!(bundle.campaign_json.exists());
    assert!(bundle.objective_card_json.exists());
    assert!(bundle.stages_json.exists());
    assert!(bundle.near_term_tasks_json.exists());
    assert!(bundle.resource_schedule_preview_json.exists());
    assert!(bundle.approval_checkpoint_json.exists());
    assert!(bundle.decisions_jsonl.exists());
    let persisted: sinomo_evolution::EvolutionCampaign = serde_json::from_str(
        &std::fs::read_to_string(&bundle.campaign_json).expect("campaign json"),
    )
    .expect("campaign parse");
    assert_eq!(persisted.campaign_id, campaign.campaign_id);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn slash_commands_reuse_cli_argument_shape() {
    let args = parse_slash_command(
        "/evolve propose \"improve scorecards\" --verify \"cargo check\" --dry-run",
    )
    .expect("parse")
    .expect("slash command");

    assert_eq!(
        args,
        vec![
            "evolve",
            "propose",
            "improve scorecards",
            "--verify",
            "cargo check",
            "--dry-run"
        ]
    );
    assert_eq!(parse_slash_command("hello sinomo").expect("parse"), None);
    assert_eq!(
        parse_slash_command("/evolve approve --note \"\"")
            .expect("parse")
            .expect("slash command"),
        vec!["evolve", "approve", "--note", ""]
    );
}

#[test]
fn interaction_runtime_records_message_input_as_events() {
    let session = InteractionSession::new(
        SessionId::new("sess_cli_test").expect("session"),
        NodeId::new("node_cli_test").expect("node"),
        InteractionSurface::new("surface.test", InteractionSurfaceKind::CommandLine),
    );
    let mut runtime = InteractionRuntime::new(
        InMemoryEventStore::new(),
        fixture_handler("hello from fixture"),
        session,
    );

    let outputs = runtime
        .ingest_user_input("start a conversation")
        .expect("input");

    assert_eq!(outputs.len(), 1);
    assert_eq!(
        outputs[0],
        super::InteractionOutput::Line("sinomo: hello from fixture".to_string())
    );
    let events = runtime.store().replay(ReplayQuery::all()).expect("replay");
    assert_eq!(events.len(), 4);
    assert_eq!(events[0].event.kind.as_str(), "interaction.input.received");
    assert_eq!(
        events[1].event.kind.as_str(),
        "interaction.message.received"
    );
    assert_eq!(events[2].event.kind.as_str(), "interaction.model.requested");
    assert_eq!(events[3].event.kind.as_str(), "interaction.model.responded");
    assert_eq!(
        events[1].event.links.caused_by,
        vec![events[0].event.event_id.clone()]
    );
    assert_eq!(
        events[3].event.links.caused_by,
        vec![events[2].event.event_id.clone()]
    );
}

#[test]
fn cui_loop_starts_and_exits_from_quit_command() {
    let mut output = Vec::new();

    run_cui_with_handler_io(
        Cursor::new("/quit\n"),
        &mut output,
        fixture_handler("unused"),
    )
    .expect("cui");

    let output = String::from_utf8(output).expect("utf8");
    assert!(output.contains("Sinomo CUI"));
    assert!(output.contains("ending session"));
}

#[test]
fn cui_loop_uses_connected_daemon_cluster_node_when_available() {
    let mut output = Vec::new();
    let daemon = DaemonStatus {
        pid: 4242,
        host: "0.0.0.0".to_string(),
        port: 21_353,
        address: "127.0.0.1:21353".to_string(),
        version: daemon_package_version(),
        target_os: Some(std::env::consts::OS.to_string()),
        target_arch: Some(std::env::consts::ARCH.to_string()),
        executable: None,
        rollout: None,
        started_at: "2026-05-04T00:00:00Z".to_string(),
        data_dir: PathBuf::from("data"),
        lock_file: PathBuf::from("data/daemon/daemon.lock"),
        cluster: Some(DaemonClusterStatus {
            cluster_id: ClusterId::new("cluster_cui").expect("cluster"),
            node_id: NodeId::new("node_daemon").expect("node"),
            process: super::DAEMON_MAIN_CLUSTER_ROLE.to_string(),
            member_role: ClusterMemberRole::Voter,
            member_state: ClusterMemberState::Active,
        }),
    };

    run_cui_with_handler_io_and_daemon(
        Cursor::new("/quit\n"),
        &mut output,
        fixture_handler("unused"),
        Some(daemon),
    )
    .expect("cui");

    let output = String::from_utf8(output).expect("utf8");
    assert!(output.contains("node: node_daemon"));
    assert!(output.contains("daemon: connected pid=4242 address=127.0.0.1:21353"));
    assert!(output.contains("cluster: cluster_cui"));
}

fn fixture_handler(
    response: &'static str,
) -> ModelBackedInteractionHandler<FixtureInteractionModel> {
    ModelBackedInteractionHandler::new(
        FixtureInteractionModel { response },
        "fixture",
        "fixture-model",
    )
}

#[derive(Clone, Debug)]
struct FixtureInteractionModel {
    response: &'static str,
}

impl InteractionModel for FixtureInteractionModel {
    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        assert_eq!(request.provider, "fixture");
        assert_eq!(request.model, "fixture-model");
        assert!(request.messages[1].content.contains("start a conversation"));
        Ok(LanguageModelResponse {
            provider: request.provider.clone(),
            model: request.model.clone(),
            content: self.response.to_string(),
            usage: Some(LanguageModelUsage {
                input_tokens: Some(1),
                output_tokens: Some(1),
            }),
        })
    }
}

#[test]
fn model_list_parser_accepts_optional_provider() {
    assert_eq!(
        parse_models_list_options(&[]).expect("empty args"),
        super::ModelListOptions { provider: None }
    );
    assert_eq!(
        parse_models_list_options(&["--provider".to_string(), "github-copilot".to_string()])
            .expect("provider"),
        super::ModelListOptions {
            provider: Some("github-copilot".to_string())
        }
    );
    assert!(matches!(
        parse_models_list_options(&["github-copilot".to_string()]),
        Err(CliError::Usage)
    ));
}

#[test]
fn usage_parser_accepts_provider_and_json() {
    let options = parse_usage_options(&[
        "--provider".to_string(),
        "codex".to_string(),
        "--json".to_string(),
    ])
    .expect("usage options");

    assert_eq!(options.provider.as_deref(), Some("codex"));
    assert!(options.json);
    assert!(!options.help);
}

#[test]
fn model_default_parser_accepts_provider_model() {
    let options = parse_model_default_options(&[
        "--provider".to_string(),
        "openai-api".to_string(),
        "--model".to_string(),
        "gpt-5.5".to_string(),
    ])
    .expect("options");

    assert_eq!(options.provider, "openai");
    assert_eq!(options.model, "gpt-5.5");
}

#[test]
fn model_default_parser_normalizes_nvidia_provider_qualified_model() {
    let options = parse_model_default_options(&[
        "--provider".to_string(),
        "nvidia".to_string(),
        "--model".to_string(),
        "nvidia/nvidia/nemotron-3-super-120b-a12b".to_string(),
    ])
    .expect("options");

    assert_eq!(options.provider, "nvidia");
    assert_eq!(options.model, "nvidia/nemotron-3-super-120b-a12b");
}

#[test]
fn models_default_writes_global_model_config() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-cli-models-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);

    models_default_with_data_dir(
        super::ModelDefaultOptions {
            provider: "openai".to_string(),
            model: "gpt-5.5".to_string(),
        },
        &data_dir,
    )
    .expect("write model defaults");

    let config_path = root.join("config").join("models.json");
    let config = serde_json::from_str::<serde_json::Value>(
        &std::fs::read_to_string(&config_path).expect("model config"),
    )
    .expect("model config json");
    assert_eq!(config["default_provider"], "openai");
    assert_eq!(config["default_model"], "gpt-5.5");

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn model_default_parser_requires_known_provider() {
    let error = parse_model_default_options(&[
        "--provider".to_string(),
        "unknown".to_string(),
        "--model".to_string(),
        "model".to_string(),
    ])
    .expect_err("unknown provider rejected");

    assert!(matches!(error, CliError::Parse { .. }));
}

#[test]
fn model_provider_aliases_normalize_to_supported_provider_ids() {
    assert_eq!(
        normalize_model_provider("google-gemini").as_deref(),
        Some("gemini")
    );
    assert_eq!(
        normalize_model_provider("anthropic-claude-code").as_deref(),
        Some("claude-code")
    );
    assert_eq!(
        normalize_model_provider("github-copilot").as_deref(),
        Some("github-copilot")
    );
    assert_eq!(normalize_model_provider("codex-api"), None);
    assert_eq!(normalize_model_provider("openai-codex"), None);
    assert_eq!(normalize_model_provider("openai-codex-cli"), None);
}

#[test]
fn active_job_terminal_statuses_are_explicit() {
    assert!(is_terminal_status(&EvolutionJobStatus::Approved));
    assert!(is_terminal_status(&EvolutionJobStatus::Rejected));
    assert!(!is_terminal_status(&EvolutionJobStatus::AwaitingApproval));
}

#[test]
fn evolve_init_command_is_removed() {
    let error = run_evolve_command(&["init".to_string()]).expect_err("init removed");

    assert!(matches!(error, CliError::Usage));
}

#[test]
fn evolution_paths_store_record_inside_job_folder() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-cli-layout-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_nanos()
    ));
    let paths = EvolutionPaths::new(&SinomoDataDir::new(&root));
    let job_id = EvolutionJobId::new("evojob_layout").expect("job id");

    assert_eq!(
        paths.job_path(&job_id),
        root.join("evolution")
            .join("jobs")
            .join("evojob_layout")
            .join("record.json")
    );
}
