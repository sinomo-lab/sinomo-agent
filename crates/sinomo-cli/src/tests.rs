use super::{
    CliCommand, CliError, ClusterCliCommand, ClusterInitOptions, ClusterInviteOptions,
    ClusterJoinOptions, ClusterJoinRegistrationRequest, ClusterPaths, ClusterRolloutBuildRequest,
    DaemonCliCommand, DaemonClusterStatus, DaemonPaths, DaemonProbeStatus, DaemonRunOptions,
    DaemonStatus, DfsCliCommand, DfsCommonOptions, DfsFilterOptions, DfsPinCliCommand,
    EvolutionJobStatus, EvolutionPaths, InstallLayout, InstallOptions, InteractionModel,
    InteractionRuntime, InteractionSession, InteractionSurface, InteractionSurfaceKind,
    ModelBackedInteractionHandler, ModelDefaultRepoMode, ModelsCliCommand, ProviderCliCommand,
    ProviderName, RolloutCliCommand, RolloutMainActivation, RolloutOptions, SelfRolloutPaths,
    UninstallOptions, authenticate_daemon_command, build_cluster_raft_append_request,
    cli_command_requires_cluster_state, cluster_init_with_paths, cluster_invite_with_paths,
    cluster_join_with_paths, cluster_peer_members, cluster_raft_known_leader_id,
    cluster_sync_targets, daemon_package_version, daemon_status_matches_options,
    decode_cluster_join_invite, elect_cluster_raft_leader, encode_cluster_join_invite,
    ensure_rollout_layout, handle_daemon_cluster_join, handle_daemon_cluster_kv_txn,
    handle_daemon_cluster_kv_watch_drain, handle_daemon_cluster_kv_watch_events,
    handle_daemon_cluster_kv_watch_prefix, handle_daemon_cluster_raft_append,
    handle_daemon_connection, handle_daemon_rollout_build, install_should_update_binary,
    install_with_layout, is_terminal_status, models_default_with_data_dir,
    normalize_model_provider, parse_cli_command, parse_cluster_command,
    parse_cluster_join_invite_argument, parse_daemon_command, parse_dfs_command,
    parse_install_options, parse_model_default_options, parse_models_list_options,
    parse_propose_options, parse_provider_command, parse_rollout_command, parse_slash_command,
    parse_uninstall_options, parse_usage_options, read_cluster_kv_snapshot,
    read_cluster_members_from_kv, read_cluster_node_statuses_from_kv, read_cluster_rollout_status,
    read_cluster_state, refresh_daemon_node_status_in_kv, run_cui_with_handler_io,
    run_cui_with_handler_io_and_daemon, run_evolve_command, send_daemon_command_raw,
    sign_cluster_join_invite, sign_daemon_command_from_data_dir, stage_rollout_main_activation,
    sync_cluster_state_to_kv, uninstall_with_layout, write_json,
};
use sinomo_lm::{
    LanguageModelError, LanguageModelRequest, LanguageModelResponse, LanguageModelUsage,
};
use sinomo_runtime::core::{
    CapabilityName, ClusterBootstrap, ClusterBootstrapState, ClusterEndpointPrivacy,
    ClusterEndpointRecord, ClusterId, ClusterInviteId, ClusterJoinInvite, ClusterMemberRecord,
    ClusterMemberRole, ClusterMemberState, ClusterNodeAvailability, ClusterTransportKind,
    EvolutionJobId, NodeId, SessionId, TrustDomain, TrustRing,
};
use sinomo_runtime::events::{EventStore, InMemoryEventStore, ReplayQuery};
use sinomo_runtime::kv::{KvCompare, KvOp, KvRevision, KvTxn};
use sinomo_runtime::local::SinomoDataDir;
use std::io::{BufRead, Cursor, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use time::OffsetDateTime;

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
fn read_cluster_state_accepts_legacy_unsigned_join_invite() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-cli-legacy-cluster-state-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");
    std::fs::write(
        &paths.state,
        r#"{
  "cluster_id": "cluster_legacy",
  "cluster_name": "legacy",
  "local_node_id": "node_legacy",
  "local_node_display_name": "legacy node",
  "bootstrap": {
    "joined": {
      "invite": {
        "invite_id": "invite_legacy",
        "cluster_id": "cluster_legacy",
        "cluster_name": "legacy",
        "introducers": [],
        "token_id": "token_legacy",
        "expires_at": "2026-05-05T08:52:04Z",
        "trust_domain": "trust.local",
        "allowed_member_roles": ["learner"],
        "signature": "local-bootstrap-signature-pending"
      }
    }
  },
  "term": 1,
  "kv_revision": 0,
  "members": [],
  "initialized_at": "2026-05-04T08:16:46Z",
  "updated_at": "2026-05-04T11:45:01Z"
}"#,
    )
    .expect("write legacy state");

    let state = read_cluster_state(&paths).expect("legacy cluster state");
    assert_eq!(state.cluster_id.as_str(), "cluster_legacy");
    assert!(matches!(state.bootstrap, ClusterBootstrap::Joined { .. }));
    std::fs::remove_dir_all(root).ok();
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
fn cluster_peer_members_tolerates_unreachable_join_introducers() {
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
    let rendered = cluster_invite_with_paths(
        ClusterInviteOptions {
            ttl_seconds: 120,
            introducers: vec!["127.0.0.1:1".to_string()],
            output: None,
        },
        &paths,
    )
    .expect("cluster invite");
    let mut state = ClusterBootstrapState::pending_join(
        ClusterId::new("cluster_peers_offline").expect("cluster"),
        "offline peers".to_string(),
        NodeId::new("node_peers_joined").expect("node"),
        "joined peer".to_string(),
        "joined-public-key".to_string(),
        rendered.invite,
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
    sync_cluster_state_to_kv(&paths, &state).expect("sync kv");
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
    assert!(cli_command_requires_cluster_state(&CliCommand::Models(
        ModelsCliCommand::Help
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
}

#[test]
fn cluster_init_persists_local_bootstrap_state() {
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
    assert_eq!(main.state, ClusterMemberState::Active);
    assert!(!main.node_public_key.trim().is_empty());
    assert_eq!(main.endpoints[0].address, "127.0.0.1:21353");
    let shadow = state
        .members
        .iter()
        .find(|member| member.node_id == NodeId::new("node_local-shadow").expect("node"))
        .expect("shadow daemon member");
    assert_eq!(shadow.role, ClusterMemberRole::NonVoting);
    assert_eq!(shadow.state, ClusterMemberState::Active);
    assert!(!shadow.node_public_key.trim().is_empty());
    assert_ne!(shadow.node_public_key, main.node_public_key);
    assert_eq!(shadow.endpoints[0].address, "127.0.0.1:21354");
    let kv_members = read_cluster_members_from_kv(&paths).expect("kv members");
    assert_eq!(kv_members.len(), 2);
    let kv_main = kv_members
        .iter()
        .find(|member| member.node_id == NodeId::new("node_local").expect("node"))
        .expect("kv main");
    assert_eq!(kv_main.state, ClusterMemberState::Active);
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

    let signed = sign_daemon_command_from_data_dir(&data_dir, "STATUS").expect("signed command");
    assert_eq!(
        authenticate_daemon_command(&daemon_paths, &signed).expect("auth command"),
        super::AuthenticatedDaemonCommand {
            command: "STATUS".to_string(),
            signer: super::DaemonSignerAuthority::Local,
        }
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
    let payload = serde_json::to_string(&request).expect("join request");
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
    tampered.invite.token_id.push_str("_tampered");
    let tampered_payload = serde_json::to_string(&tampered).expect("tampered join request");
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
        super::authorize_authenticated_daemon_command(&super::AuthenticatedDaemonCommand {
            command: "STATUS".to_string(),
            signer: super::DaemonSignerAuthority::Member,
        })
        .is_ok()
    );
    assert!(
        super::authorize_authenticated_daemon_command(&super::AuthenticatedDaemonCommand {
            command: "ROLLOUT_BUILD {}".to_string(),
            signer: super::DaemonSignerAuthority::Member,
        })
        .is_err()
    );
    assert!(
        super::authorize_authenticated_daemon_command(&super::AuthenticatedDaemonCommand {
            command: "ROLLOUT_BUILD {}".to_string(),
            signer: super::DaemonSignerAuthority::Voter,
        })
        .is_ok()
    );
    assert!(
        super::authorize_authenticated_daemon_command(&super::AuthenticatedDaemonCommand {
            command: "SHUTDOWN".to_string(),
            signer: super::DaemonSignerAuthority::Voter,
        })
        .is_err()
    );
    assert!(
        super::authorize_authenticated_daemon_command(&super::AuthenticatedDaemonCommand {
            command: "SHUTDOWN".to_string(),
            signer: super::DaemonSignerAuthority::Local,
        })
        .is_ok()
    );
    assert!(
        super::authorize_authenticated_daemon_command(&super::AuthenticatedDaemonCommand {
            command: "UNKNOWN_PROBE".to_string(),
            signer: super::DaemonSignerAuthority::Local,
        })
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
            cluster_id: None,
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

    assert!(rendered.uri.starts_with(super::SINOMO_JOIN_URI_PREFIX));
    assert_eq!(
        parsed.cluster_id,
        ClusterId::new("cluster_invite").expect("cluster")
    );
    assert_eq!(parsed.cluster_name.as_deref(), Some("invite cluster"));
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
    assert_eq!(parsed.introducers.len(), 1);
    assert_eq!(parsed.introducers[0].address, "192.168.1.10:21353");
    assert_eq!(
        super::network::read_cluster_invitation_from_kv(&paths, &parsed.invite_id)
            .expect("published invite"),
        parsed
    );
    assert!(rendered.persisted_path.exists());
    assert_eq!(
        std::fs::read_to_string(output).expect("invite file").trim(),
        rendered.join_command
    );
    assert!(rendered.join_command.starts_with("sinomo cluster join "));
    assert_eq!(
        parse_cluster_join_invite_argument(&rendered.join_command).expect("parse join command"),
        parsed
    );
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
            cluster_id: None,
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
    assert_eq!(state.cluster_name, "remote cluster");
    assert!(matches!(state.bootstrap, ClusterBootstrap::Joined { .. }));
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
    super::sync_cluster_invitation_to_kv(&host_paths, &offline_invite)
        .expect("active offline invite kv");
    let offline_uri = encode_cluster_join_invite(&offline_invite).expect("offline invite");
    cluster_join_with_paths(
        ClusterJoinOptions {
            invite: offline_uri,
            cluster_id: None,
            node_id: Some(NodeId::new("node_remote").expect("node")),
            name: "remote".to_string(),
        },
        &join_paths,
    )
    .expect("cluster join local");
    let remote_state = read_cluster_state(&join_paths).expect("remote state");
    let invite = match &remote_state.bootstrap {
        ClusterBootstrap::Joined { invite } => invite.clone(),
        ClusterBootstrap::InitializedLocally => panic!("expected joined state"),
    };
    let request = ClusterJoinRegistrationRequest {
        invitation_secret: invite.invitation_secret.clone(),
        invite,
        members: remote_state.members,
    };
    let payload = serde_json::to_string(&request).expect("request");
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
            cluster_id: None,
            node_id: Some(NodeId::new("node_peer").expect("node")),
            name: "peer".to_string(),
        },
        &peer_paths,
    )
    .expect("cluster join");

    let host_state = read_cluster_state(&host_paths).expect("host state");
    sync_cluster_state_to_kv(&host_paths, &host_state).expect("host kv");
    let request = build_cluster_raft_append_request(&host_paths, &host_state).expect("raft append");
    let peer_response = handle_daemon_cluster_raft_append(
        &DaemonPaths::with_root(peer_root.join("daemon")),
        &serde_json::to_string(&request).expect("raft append request"),
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
        build_cluster_raft_append_request(&host_paths, &host_state).expect("current raft append");
    let current_response = handle_daemon_cluster_raft_append(
        &DaemonPaths::with_root(peer_root.join("daemon")),
        &serde_json::to_string(&current_request).expect("current append request"),
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
        &serde_json::to_string(&request).expect("stale append request"),
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
        build_cluster_raft_append_request(&host_paths, &host_state).expect("watch raft append");
    assert!(
        handle_daemon_cluster_raft_append(
            &DaemonPaths::with_root(peer_root.join("daemon")),
            &serde_json::to_string(&watch_append).expect("watch append request"),
        )
        .expect("peer watch append")
        .accepted
    );
    let host_events = read_cluster_kv_snapshot(&host_paths)
        .expect("host watch snapshot")
        .events;
    let watch_fanout = handle_daemon_cluster_kv_watch_events(
        &DaemonPaths::with_root(peer_root.join("daemon")),
        &serde_json::to_string(&sinomo_runtime::network::ClusterKvWatchEventsRequest {
            state: host_state.clone(),
            leader_id: host_state.local_node_id.clone(),
            events: host_events,
        })
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
        host: "127.0.0.1".to_string(),
        port: 21_353,
        shadow_port: 21_354,
        requested_at: "now".to_string(),
    };
    let payload = serde_json::to_string(&request).expect("request json");
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
    let options = parse_model_default_options(
        &[
            "--provider".to_string(),
            "openai-api".to_string(),
            "--model".to_string(),
            "gpt-5.5".to_string(),
        ],
        ModelDefaultRepoMode::Reject,
    )
    .expect("options");

    assert_eq!(options.provider, "openai");
    assert_eq!(options.model, "gpt-5.5");
    assert_eq!(options.repo, None);
}

#[test]
fn model_default_parser_normalizes_nvidia_provider_qualified_model() {
    let options = parse_model_default_options(
        &[
            "--provider".to_string(),
            "nvidia".to_string(),
            "--model".to_string(),
            "nvidia/nvidia/nemotron-3-super-120b-a12b".to_string(),
        ],
        ModelDefaultRepoMode::Reject,
    )
    .expect("options");

    assert_eq!(options.provider, "nvidia");
    assert_eq!(options.model, "nvidia/nemotron-3-super-120b-a12b");
}

#[test]
fn evolve_model_default_parser_accepts_repo_for_compatibility() {
    let options = parse_model_default_options(
        &[
            "--provider".to_string(),
            "openai-api".to_string(),
            "--model".to_string(),
            "gpt-5.5".to_string(),
            "--repo".to_string(),
            "/repo".to_string(),
        ],
        ModelDefaultRepoMode::Allow,
    )
    .expect("options");

    assert_eq!(options.provider, "openai");
    assert_eq!(options.model, "gpt-5.5");
    assert_eq!(options.repo.as_deref(), Some(std::path::Path::new("/repo")));
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
            repo: None,
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
    let error = parse_model_default_options(
        &[
            "--provider".to_string(),
            "unknown".to_string(),
            "--model".to_string(),
            "model".to_string(),
        ],
        ModelDefaultRepoMode::Reject,
    )
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
    assert_eq!(
        paths.legacy_job_path(&job_id),
        root.join("evolution")
            .join("jobs")
            .join("evojob_layout.json")
    );
}
