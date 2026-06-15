use super::{
    AgentCliCommand, AgentExportOptions, AgentImportOptions, AgentInspectOptions,
    AgentReadinessOptions, AgentRunOptions, AgentSessionCliCommand, AgentSessionInspectOptions,
    AgentSessionMessageOptions, AgentSessionProgressOptions, AgentSessionResumeOptions,
    AgentSessionTerminalResultsOptions, AgentValidateOptions, BUILTIN_TOP_LEVEL_USAGE_COMMANDS,
    CliClusterMemberMutationHost, CliCommand, CliError, CliRuntimeOptions, ClusterCliCommand,
    ClusterInitOptions, ClusterInviteOptions, ClusterJoinOptions, ClusterJoinRegistrationRequest,
    ClusterPaths, ClusterRaftRoleOptions, ClusterRemoveOptions, ClusterRoleOptions,
    ClusterRolloutBuildRequest, DaemonCliCommand, DaemonClusterStatus, DaemonPaths,
    DaemonProbeStatus, DaemonRunOptions, DaemonStatus, DebugCliCommand, DfsCliCommand,
    DfsCommonOptions, DfsExposeCliCommand, DfsFilterOptions, DfsMountOptions, DfsPinCliCommand,
    InstallLayout, InstallOptions, InteractionModel, InteractionRuntime, InteractionSession,
    InteractionSurface, InteractionSurfaceKind, ModelBackedInteractionHandler,
    ModelOptionValueUpdate, ModelOptionsCliCommand, ModelOptionsSetOptions, ModelOptionsTarget,
    ModelRuntimeCliCommand, ModelRuntimeSetOptions, ModelsCliCommand, ProviderCliCommand,
    ProviderCommandOptions, ProviderName, RolloutAdvanceOptions, RolloutCliCommand,
    RolloutMainActivation, RolloutModuleNativeDylibOptions, RolloutOptions,
    RolloutProgressExpectation, RolloutTelemetryGateSetOptions, RuntimeMainAgentInteractionHandler,
    SelfRolloutPaths, TestCliCommand, UninstallOptions, advertised_peer_route_targets,
    apply_cluster_node_role, apply_cluster_raft_role, authenticate_daemon_control_request,
    build_cluster_raft_append_request_from_next_index, cli_command_is_help,
    cli_command_requires_cluster_state, cli_command_supports_degraded_local_mode,
    cli_invocation_force_local_mode, cluster_connectivity_sync_ops, cluster_doctor_findings,
    cluster_has_local_quorum, cluster_init_with_paths, cluster_invite_with_paths,
    cluster_join_with_paths, cluster_node_status_from_daemon_status, cluster_peer_members,
    cluster_raft_known_leader_id, cluster_state_sync_txn, cluster_sync_targets,
    compact_default_invite_introducers, daemon_advertised_endpoints_for_state,
    daemon_handle_daemon_sandbox_http_fetch, daemon_package_version,
    data_dir_storage_contract_findings, decode_cluster_join_invite, encode_cluster_join_invite,
    ensure_rollout_layout, expire_cluster_network_records_in_kv, extend_rollout_endpoints_from_kv,
    format_rollout_progress_line, handle_daemon_cluster_join_request,
    handle_daemon_cluster_kv_txn_request, handle_daemon_rollout_build_request,
    install_should_update_binary, install_with_layout, invite_channels_for_join_registration,
    invite_communication_channels, invite_introducer_endpoint,
    invite_introducer_endpoint_from_state, load_tui_model_runtime_config,
    main_agent_config_tool_registry, main_agent_handler_for_tui_with_data_dir,
    managed_source_status, model_config_uses_cluster, models_default_with_data_dir,
    normalize_model_provider, open_dfs_metadata_for_inspection_with_data_dir, parse_cli_command,
    parse_cli_invocation, parse_cluster_command, parse_cluster_join_invite_argument,
    parse_daemon_command, parse_dfs_command, parse_install_options, parse_model_default_options,
    parse_model_options_command, parse_models_list_options, parse_models_runtime_command,
    parse_provider_command, parse_rollout_command, parse_slash_command, parse_test_command,
    parse_uninstall_options, parse_usage_options, push_unique_introducer, read_cluster_kv_snapshot,
    read_cluster_members_from_kv, read_cluster_node_statuses_from_kv, read_cluster_rollout_status,
    read_cluster_state, require_cluster_runtime_quorum_with_paths_and_daemon,
    rollout_advance_with_data_dir, rollout_cluster_targets, rollout_cluster_targets_from_state,
    rollout_endpoint_candidates, rollout_pending_error_grace, rollout_progress_bar,
    rollout_status_matches_expectation, rollout_target_endpoint_for_main,
    rollout_target_endpoint_for_main_in_order, rollout_target_endpoint_for_main_with_connectivity,
    rollout_telemetry_gates_set_with_data_dir, rollout_terminal_response, run_tui_with_handler_io,
    run_tui_with_handler_io_and_daemon, send_cluster_join_registration_over_iroh,
    shadow_daemon_node_id, sign_cluster_join_invite, sign_daemon_rpc_request_from_data_dir,
    sort_rollout_endpoints_by_connectivity, stage_rollout_main_activation,
    sync_cluster_state_to_kv, test_command_requires_cluster_state, tui_default_daemon_status,
    uninstall_with_layout, with_cli_force_local_mode, write_daemon_status,
    write_imported_cluster_kv_snapshot, write_model_runtime_config_for_node, write_rollout_status,
};
#[cfg(feature = "sandbox")]
use super::{
    daemon_handle_daemon_sandbox_http_fetch_to_file, daemon_handle_daemon_sandbox_import_busybox,
    daemon_handle_daemon_sandbox_read, daemon_handle_daemon_sandbox_toolchain_add_artifact,
    daemon_handle_daemon_sandbox_toolchain_add_command,
    daemon_handle_daemon_sandbox_toolchain_import_package, daemon_handle_daemon_sandbox_write,
};
use base64::Engine;
use sinomo_agent::{AgentRuntimeControlHost, AgentSessionResumeRequest, AgentSessionSteerRequest};
use sinomo_lm::{
    LanguageModelError, LanguageModelRequest, LanguageModelResponse, LanguageModelUsage,
    ProviderReasoningEffort, ProviderTextVerbosity,
};
use sinomo_runtime::cli_registry::{
    CliCommandContext, CliCommandError, CliCommandHandler, CliCommandHelp, CliCommandMetadata,
    CliCommandPolicy, CliParseError, RegisteredCliCommand,
};
use sinomo_runtime::core::{
    CapabilityName, ClusterBootstrapState, ClusterEndpointPrivacy, ClusterEndpointRecord,
    ClusterEndpointSource, ClusterJoinInvite, ClusterMemberRecord, ClusterMemberRole,
    ClusterMemberState, ClusterNodeAvailability, ClusterNodeRole, ClusterTransportKind, Confidence,
    DfsObjectPolicy, EvaluationOutcome, EvaluationScorecard, EventEnvelope, EventKind, Id,
    LatencyClass, PrivacyClass, RollbackPolicy, RollbackTrigger, RolloutPlan, RolloutStrategy,
    RoutingMetadata, RoutingScope, Salience, Source, TrustDomain, TrustRing,
};
use sinomo_runtime::daemon::daemon_status_matches_options;
use sinomo_runtime::dfs::{
    DfsMetadataBackend, DfsMetadataShardAssignment, DfsMetadataShardConfig, DfsObjectVersionRecord,
    distributed_sifs, distributed_sifs_bootstrap_objects, metadata_shard_key,
};
use sinomo_runtime::events::{
    EventId, EventStore, InMemoryEventStore, JsonlEventStore, ReplayQuery, SessionId,
};
use sinomo_runtime::kv::{InProcessKvStore, KvCommand, KvCompare, KvOp, KvRevision, KvTxn};
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::module::{
    MODULE_SHADOW_DECISION_EVENT_KIND, ModuleReplayCaseOutcome, ModuleReplayReport,
    read_module_loader_runtime_state,
};
use sinomo_runtime::network::{
    self, ClusterEndpointId, ClusterId, ClusterInviteId, daemon_lan_endpoint,
};
use sinomo_runtime::node::{NodeId, main_daemon_capabilities};
use sinomo_runtime::rollout::{
    RolloutAction, RolloutController, create_source_repo_bundle, read_rollout_runtime_state,
    write_rollout_runtime_state,
};
use sinomo_runtime::sandbox::SandboxFileSystem;
use sinomo_runtime::sandbox::*;
use std::collections::BTreeMap;
#[cfg(feature = "sandbox")]
use std::collections::BTreeSet;
use std::io::{Cursor, Read, Write};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::Command;
#[cfg(feature = "sandbox")]
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use time::OffsetDateTime;

fn append_cluster_raft_command(
    paths: &ClusterPaths,
    node_id: &NodeId,
    command: KvCommand,
) -> Result<sinomo_runtime::kv::KvSnapshot, network::NetworkError> {
    paths
        .kv_paths()
        .raft_store()
        .append_command_and_publish(
            node_id,
            command,
            network::cluster_kv_key_can_defer_materialized_snapshot,
        )
        .map_err(Into::into)
}

fn append_cluster_raft_command_with_response(
    paths: &ClusterPaths,
    node_id: &NodeId,
    command: KvCommand,
) -> Result<
    (
        sinomo_runtime::kv::KvSnapshot,
        Option<sinomo_runtime::kv::KvTxnResponse>,
    ),
    network::NetworkError,
> {
    paths
        .kv_paths()
        .raft_store()
        .append_command_with_response_and_publish(
            node_id,
            command,
            network::cluster_kv_key_can_defer_materialized_snapshot,
        )
        .map(|result| (result.snapshot, result.txn_response))
        .map_err(Into::into)
}

struct CliLocalMemberPublishHost;

impl network::ClusterLocalMemberPublishHost for CliLocalMemberPublishHost {
    fn cluster_has_local_quorum(
        &mut self,
        _paths: &ClusterPaths,
        _state: &ClusterBootstrapState,
    ) -> bool {
        true
    }

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> Result<sinomo_runtime::kv::KvSnapshot, network::NetworkError> {
        append_cluster_raft_command(paths, &state.local_node_id, KvCommand::Txn(txn))
    }

    fn send_cluster_member_publish(
        &mut self,
        _endpoint: &ClusterEndpointRecord,
        _request: &network::ClusterMemberPublishRequest,
        _timeout: Duration,
    ) -> Result<network::ClusterMemberPublishResponse, network::NetworkError> {
        panic!("local quorum host should not forward member publish requests")
    }
}

#[cfg(feature = "sandbox")]
fn with_test_sandbox_host<T>(
    f: impl FnOnce() -> Result<T, SandboxCliError>,
) -> Result<T, SandboxCliError> {
    sinomo_runtime::sandbox::with_sandbox_cli_host(
        Arc::new(super::sandbox_host::SinomoCliSandboxHost),
        f,
    )
}

#[cfg(feature = "sandbox")]
fn sandbox_doctor_findings_with_stores(
    options: SandboxDoctorOptions,
    store: &sinomo_runtime::sandbox::LocalSandboxInstanceStore,
    toolchain_store: &sinomo_runtime::sandbox::LocalSandboxToolchainStore,
    policy: &sinomo_runtime::sandbox::SandboxPolicy,
) -> Result<Vec<SandboxDoctorFinding>, SandboxCliError> {
    with_test_sandbox_host(|| {
        sinomo_runtime::sandbox::sandbox_doctor_findings_with_stores(
            options,
            store,
            toolchain_store,
            policy,
        )
    })
}

#[cfg(feature = "sandbox")]
fn sandbox_doctor_findings_with_stores_and_events(
    options: SandboxDoctorOptions,
    store: &sinomo_runtime::sandbox::LocalSandboxInstanceStore,
    toolchain_store: &sinomo_runtime::sandbox::LocalSandboxToolchainStore,
    policy: &sinomo_runtime::sandbox::SandboxPolicy,
    resource_events: &[sinomo_runtime::events::StoredEvent],
) -> Result<Vec<SandboxDoctorFinding>, SandboxCliError> {
    with_test_sandbox_host(|| {
        sinomo_runtime::sandbox::sandbox_doctor_findings_with_stores_and_events(
            options,
            store,
            toolchain_store,
            policy,
            resource_events,
        )
    })
}

#[cfg(feature = "sandbox")]
fn sandbox_module_artifact_register_with_data_dir(
    options: SandboxModuleArtifactRegisterOptions,
    data_dir_root: &std::path::Path,
) -> Result<(), SandboxCliError> {
    with_test_sandbox_host(|| {
        sinomo_runtime::sandbox::sandbox_module_artifact_register_with_data_dir(
            options,
            data_dir_root,
        )
    })
}

#[cfg(feature = "sandbox")]
fn sandbox_run_with_store(
    options: SandboxRunOptions,
    store: &sinomo_runtime::sandbox::LocalSandboxInstanceStore,
) -> Result<(), SandboxCliError> {
    with_test_sandbox_host(|| sinomo_runtime::sandbox::sandbox_run_with_store(options, store))
}

#[cfg(feature = "sandbox")]
fn sandbox_run_with_stores(
    options: SandboxRunOptions,
    store: &sinomo_runtime::sandbox::LocalSandboxInstanceStore,
    toolchain_store: &sinomo_runtime::sandbox::LocalSandboxToolchainStore,
) -> Result<(), SandboxCliError> {
    with_test_sandbox_host(|| {
        sinomo_runtime::sandbox::sandbox_run_with_stores(options, store, toolchain_store)
    })
}

#[cfg(feature = "sandbox")]
fn sandbox_process_plan_with_stores(
    options: SandboxProcessPlanOptions,
    store: &sinomo_runtime::sandbox::LocalSandboxInstanceStore,
    toolchain_store: &sinomo_runtime::sandbox::LocalSandboxToolchainStore,
) -> Result<(), SandboxCliError> {
    with_test_sandbox_host(|| {
        sinomo_runtime::sandbox::sandbox_process_plan_with_stores(options, store, toolchain_store)
    })
}

#[cfg(feature = "sandbox")]
fn sandbox_shell_with_stores(
    options: SandboxShellOptions,
    stdin: Vec<u8>,
    store: &sinomo_runtime::sandbox::LocalSandboxInstanceStore,
    toolchain_store: &sinomo_runtime::sandbox::LocalSandboxToolchainStore,
) -> Result<(), SandboxCliError> {
    with_test_sandbox_host(|| {
        sinomo_runtime::sandbox::sandbox_shell_with_stores(options, stdin, store, toolchain_store)
    })
}

#[cfg(feature = "sandbox")]
fn sandbox_policy_uses_cluster(data_dir: &SinomoDataDir) -> bool {
    with_test_sandbox_host(|| {
        Ok(sinomo_runtime::sandbox::sandbox_policy_uses_cluster(
            data_dir,
        ))
    })
    .expect("sandbox policy source")
}

#[cfg(feature = "sandbox")]
fn write_sandbox_policy_config_for_node(
    data_dir: &SinomoDataDir,
    config: &sinomo_runtime::sandbox::SandboxClusterPolicyConfig,
) -> Result<PathBuf, SandboxCliError> {
    with_test_sandbox_host(|| {
        sinomo_runtime::sandbox::write_sandbox_policy_config_for_node(data_dir, config)
    })
}

#[derive(Clone, Copy, Debug)]
struct TestRegisteredCommandHandler {
    name: &'static str,
    policy: CliCommandPolicy,
    is_help: bool,
}

impl CliCommandHandler for TestRegisteredCommandHandler {
    fn metadata(&self) -> CliCommandMetadata {
        CliCommandMetadata::new(self.name, &[], "test registered command", self.policy)
    }

    fn help(&self) -> CliCommandHelp {
        CliCommandHelp::new("Test:", ["  test command".to_string()])
    }

    fn parse(
        &self,
        _args: &[String],
    ) -> Result<Box<dyn sinomo_runtime::cli_registry::CliCommand>, CliParseError> {
        Ok(Box::new(TestRegisteredCommand {
            name: self.name,
            policy: self.policy,
            is_help: self.is_help,
        }))
    }
}

#[derive(Debug)]
struct TestRegisteredCommand {
    name: &'static str,
    policy: CliCommandPolicy,
    is_help: bool,
}

impl sinomo_runtime::cli_registry::CliCommand for TestRegisteredCommand {
    fn name(&self) -> &'static str {
        self.name
    }

    fn policy(&self) -> CliCommandPolicy {
        self.policy
    }

    fn is_help(&self) -> bool {
        self.is_help
    }

    fn run(self: Box<Self>, _context: &mut CliCommandContext<'_>) -> Result<(), CliCommandError> {
        Ok(())
    }
}

fn registered_test_command(name: &'static str, policy: CliCommandPolicy) -> CliCommand {
    registered_test_command_with_help(name, policy, false)
}

fn registered_test_help_command(name: &'static str, policy: CliCommandPolicy) -> CliCommand {
    registered_test_command_with_help(name, policy, true)
}

fn registered_test_command_with_help(
    name: &'static str,
    policy: CliCommandPolicy,
    is_help: bool,
) -> CliCommand {
    let handler = TestRegisteredCommandHandler {
        name,
        policy,
        is_help,
    };
    CliCommand::Registered(RegisteredCliCommand::new(
        handler.parse(&[]).expect("parse registered test command"),
    ))
}

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

fn authenticated_cluster_peer(
    authenticated: &super::AuthenticatedDaemonCommand,
) -> sinomo_runtime::network::ClusterAuthenticatedPeer {
    sinomo_runtime::network::ClusterAuthenticatedPeer {
        node_id: authenticated.signer_node_id.clone(),
        public_key: authenticated.signer_public_key.clone(),
    }
}

fn handle_cluster_raft_append_rpc(
    paths: &ClusterPaths,
    request: sinomo_runtime::kv::ClusterRaftAppendRequest,
    authenticated: &super::AuthenticatedDaemonCommand,
) -> Result<sinomo_runtime::kv::ClusterRaftAppendResponse, network::NetworkError> {
    let payload = sinomo_runtime::kv::cluster_raft_append_rpc_payload(&request)?;
    let rpc =
        network::ObjectRpcRequest::from_encoded_body(payload.route, payload.category, payload.body);
    let response = network::handle_cluster_raft_rpc_request(
        paths,
        rpc,
        &authenticated_cluster_peer(authenticated),
    )?;
    response.error_detail()?;
    sinomo_runtime::kv::decode_cluster_raft_append_rpc_response_body(&response.body)
        .map_err(Into::into)
}

fn handle_cluster_raft_vote_rpc(
    paths: &ClusterPaths,
    request: sinomo_runtime::kv::ClusterRaftVoteRequest,
    authenticated: &super::AuthenticatedDaemonCommand,
) -> Result<sinomo_runtime::kv::ClusterRaftVoteResponse, network::NetworkError> {
    let payload = sinomo_runtime::kv::cluster_raft_vote_rpc_payload(&request)?;
    let rpc =
        network::ObjectRpcRequest::from_encoded_body(payload.route, payload.category, payload.body);
    let response = network::handle_cluster_raft_rpc_request(
        paths,
        rpc,
        &authenticated_cluster_peer(authenticated),
    )?;
    response.error_detail()?;
    sinomo_runtime::kv::decode_cluster_raft_vote_rpc_response_body(&response.body)
        .map_err(Into::into)
}

#[test]
fn provider_commands_parse_action_before_provider() {
    assert_eq!(
        parse_provider_command(&["auth".to_string(), "gemini".to_string(),])
            .expect("provider command"),
        ProviderCliCommand::Auth(ProviderCommandOptions {
            provider: ProviderName::Gemini,
            account: "default".to_string(),
        })
    );
    assert_eq!(
        parse_provider_command(&["test".to_string(), "github-copilot".to_string(),])
            .expect("provider command"),
        ProviderCliCommand::Test(ProviderCommandOptions {
            provider: ProviderName::GitHubCopilot,
            account: "default".to_string(),
        })
    );
    assert_eq!(
        parse_provider_command(&[
            "auth".to_string(),
            "codex".to_string(),
            "--account".to_string(),
            "team-a".to_string(),
        ])
        .expect("provider command"),
        ProviderCliCommand::Auth(ProviderCommandOptions {
            provider: ProviderName::Codex,
            account: "team-a".to_string(),
        })
    );
    assert_eq!(
        parse_provider_command(&[
            "auth".to_string(),
            "deepseek".to_string(),
            "--account".to_string(),
            "bench".to_string(),
        ])
        .expect("provider command"),
        ProviderCliCommand::Auth(ProviderCommandOptions {
            provider: ProviderName::DeepSeek,
            account: "bench".to_string(),
        })
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
fn top_level_cli_parser_accepts_local_flag_before_command() {
    let invocation = parse_cli_invocation(&[
        "--local".to_string(),
        "providers".to_string(),
        "test".to_string(),
        "openai".to_string(),
    ])
    .expect("local invocation");

    assert!(invocation.options.force_local);
    assert!(matches!(
        invocation.command,
        CliCommand::Providers(ProviderCliCommand::Test(ProviderCommandOptions {
            provider: ProviderName::OpenAi,
            account: _,
        }))
    ));
}

#[test]
fn top_level_cli_parser_routes_subcommands() {
    assert!(matches!(
        parse_cli_command(&[]).expect("tui"),
        CliCommand::Tui
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
        parse_cli_command(&["debug".to_string()]).expect("debug help"),
        CliCommand::Debug(DebugCliCommand::Help)
    ));
    assert!(matches!(
        parse_cli_command(&[
            "debug".to_string(),
            "sobj".to_string(),
            "state.sobj".to_string()
        ])
        .expect("debug sobj"),
        CliCommand::Debug(DebugCliCommand::Sobj(_))
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
        CliCommand::Providers(ProviderCliCommand::Test(ProviderCommandOptions {
            provider: ProviderName::OpenAi,
            account: _,
        }))
    ));
    assert!(matches!(
        parse_cli_command(&["models".to_string(), "list".to_string()]).expect("models"),
        CliCommand::Models(ModelsCliCommand::List(_))
    ));
    assert!(matches!(
        parse_cli_command(&["test".to_string(), "list".to_string()]).expect("test list"),
        CliCommand::Test(TestCliCommand::List(_))
    ));
    assert!(matches!(
        parse_cli_command(&[
            "test".to_string(),
            "run".to_string(),
            "--tag".to_string(),
            "agent-readiness".to_string(),
            "--require-cluster".to_string(),
        ])
        .expect("test run tag"),
        CliCommand::Test(TestCliCommand::Run(options))
            if options.id.is_none()
                && options.tag.as_deref() == Some("agent-readiness")
                && options.require_cluster
    ));
    assert!(matches!(
        parse_cli_command(&["models".to_string()]).expect("models help"),
        CliCommand::Models(ModelsCliCommand::Help)
    ));
    assert!(matches!(
        parse_cli_command(&["model".to_string()]).expect("model alias help"),
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
        parse_cli_command(&["observe".to_string(), "events".to_string()]).expect("observe"),
        CliCommand::Registered(command) if command.name() == "observe"
    ));
    assert!(matches!(
        parse_cli_command(&[
            "module".to_string(),
            "native-rust".to_string(),
            "disable".to_string(),
            "module.observe".to_string()
        ])
        .expect("module native-rust disable"),
        CliCommand::Registered(command) if command.name() == "module"
    ));
    assert!(matches!(
        parse_cli_command(&["dfs".to_string(), "status".to_string()]).expect("dfs"),
        CliCommand::Dfs(DfsCliCommand::Status(_))
    ));
    assert!(matches!(
        parse_cli_command(&["sandbox".to_string()]).expect("sandbox help"),
        CliCommand::Registered(command) if command.name() == "sandbox"
    ));
    assert!(matches!(
        parse_cli_command(&["sandbox".to_string(), "create".to_string()]).expect("sandbox create"),
        CliCommand::Registered(command) if command.name() == "sandbox"
    ));
    assert!(matches!(
        parse_cli_command(&[
            "sandbox".to_string(),
            "status".to_string(),
            "01kreqaaaaaaaaaaaaaaaaaaaa".to_string()
        ])
        .expect("sandbox status"),
        CliCommand::Registered(command) if command.name() == "sandbox"
    ));
    assert!(matches!(
        parse_cli_command(&["sandbox".to_string(), "doctor".to_string()]).expect("sandbox doctor"),
        CliCommand::Registered(command) if command.name() == "sandbox"
    ));
    assert!(matches!(
        parse_cli_command(&["agent".to_string()]).expect("agent help"),
        CliCommand::Agent(AgentCliCommand::Help)
    ));
    assert!(matches!(
        parse_cli_command(&["agent".to_string(), "help".to_string()]).expect("agent help"),
        CliCommand::Agent(AgentCliCommand::Help)
    ));
    assert!(matches!(
        parse_cli_command(&[
            "agent".to_string(),
            "export".to_string(),
            "coding-agent".to_string(),
            "--output".to_string(),
            "coding-agent.json".to_string(),
        ])
        .expect("agent export"),
        CliCommand::Agent(AgentCliCommand::Export(options))
            if options == (AgentExportOptions {
                agent: "coding-agent".to_string(),
                output: Some(PathBuf::from("coding-agent.json")),
            })
    ));
    assert!(matches!(
        parse_cli_command(&[
            "agent".to_string(),
            "import".to_string(),
            "coding-agent.json".to_string(),
            "--json".to_string(),
        ])
        .expect("agent import"),
        CliCommand::Agent(AgentCliCommand::Import(options))
            if options == (AgentImportOptions {
                path: PathBuf::from("coding-agent.json"),
                json: true,
            })
    ));
    assert!(matches!(
        parse_cli_command(&[
            "agent".to_string(),
            "inspect".to_string(),
            "coding-agent".to_string(),
            "--json".to_string(),
        ])
        .expect("agent inspect"),
        CliCommand::Agent(AgentCliCommand::Inspect(options))
            if options == (AgentInspectOptions {
                agent: "coding-agent".to_string(),
                json: true,
            })
    ));
    assert!(matches!(
        parse_cli_command(&[
            "agent".to_string(),
            "validate".to_string(),
            "--file".to_string(),
            "coding-agent.json".to_string(),
            "--json".to_string(),
        ])
        .expect("agent validate"),
        CliCommand::Agent(AgentCliCommand::Validate(options))
            if options == (AgentValidateOptions {
                agent: "main-agent".to_string(),
                file: Some(PathBuf::from("coding-agent.json")),
                json: true,
            })
    ));
    assert!(matches!(
        parse_cli_command(&[
            "agent".to_string(),
            "run".to_string(),
            "--prompt".to_string(),
            "hello".to_string(),
        ])
        .expect("agent"),
        CliCommand::Agent(AgentCliCommand::Run(_))
    ));
    assert!(matches!(
        parse_cli_command(&[
            "agent".to_string(),
            "readiness".to_string(),
            "--json".to_string(),
        ])
        .expect("agent readiness"),
        CliCommand::Agent(AgentCliCommand::Readiness(options))
            if options == (AgentReadinessOptions { json: true })
    ));
    assert!(matches!(
        parse_cli_command(&[
            "agent".to_string(),
            "session".to_string(),
            "inspect".to_string(),
            "sess_cli_agent_session".to_string(),
            "--json".to_string(),
        ])
        .expect("agent session inspect"),
        CliCommand::Agent(AgentCliCommand::Session(AgentSessionCliCommand::Inspect(options)))
            if options == (AgentSessionInspectOptions {
                session_id: SessionId::from_legacy_named("sess_cli_agent_session").expect("session id"),
                json: true,
            })
    ));
    assert!(parse_cli_command(&["evolve".to_string()]).is_err());
}

#[test]
fn documented_builtin_top_level_commands_are_parseable() {
    for command in BUILTIN_TOP_LEVEL_USAGE_COMMANDS {
        parse_cli_command(&[command.to_string()]).unwrap_or_else(|error| {
            panic!("documented command `{command}` did not parse: {error}")
        });
    }
}

#[test]
fn test_parser_accepts_live_cluster_smoke_options() {
    let command = parse_test_command(&[
        "smoke".to_string(),
        "--provider".to_string(),
        "deepseek".to_string(),
        "--account".to_string(),
        "work".to_string(),
        "--model".to_string(),
        "deepseek-v4-pro".to_string(),
        "--live".to_string(),
        "--require-cluster".to_string(),
        "--json".to_string(),
    ])
    .expect("test smoke");

    let TestCliCommand::Smoke(options) = command else {
        panic!("expected smoke command");
    };
    assert_eq!(options.provider, "deepseek");
    assert_eq!(options.account, "work");
    assert_eq!(options.model.as_deref(), Some("deepseek-v4-pro"));
    assert!(options.live);
    assert!(options.require_cluster);
    assert!(options.json);
    assert!(test_command_requires_cluster_state(&TestCliCommand::Smoke(
        options
    )));
}

#[test]
fn test_registry_lists_agent_cluster_readiness_case() {
    let registry = super::test_registry().expect("registry");
    let cases = registry.metadata();
    let case = cases
        .iter()
        .find(|case| case.id == "cluster.agent.runtime-routes")
        .expect("agent cluster runtime routes test case");
    assert!(case.tags.iter().any(|tag| tag == "agent-readiness"));
    assert!(
        case.requirements
            .iter()
            .any(|requirement| requirement.label() == "cluster-runtime")
    );
}

#[test]
fn sandbox_create_parser_accepts_tmp_policy_and_toolchain() {
    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();
    let toolchain_id = sinomo_runtime::sandbox::SandboxToolchainId::generate();
    let command = parse_sandbox_command(&[
        "create".to_string(),
        "--id".to_string(),
        sandbox_id.to_string(),
        "--toolchain".to_string(),
        toolchain_id.to_string(),
        "--memory-mb".to_string(),
        "4096".to_string(),
        "--wall-time-ms".to_string(),
        "900000".to_string(),
        "--max-input-bytes".to_string(),
        "10485760".to_string(),
        "--max-output-bytes".to_string(),
        "10485760".to_string(),
        "--max-workspace-bytes".to_string(),
        "1073741824".to_string(),
        "--max-written-bytes-per-command".to_string(),
        "1073741824".to_string(),
        "--max-file-count".to_string(),
        "100000".to_string(),
        "--tmp".to_string(),
        "retain".to_string(),
        "--mount".to_string(),
        "/services/sandbox/shared:/shared:ro".to_string(),
    ])
    .expect("sandbox create");

    let SandboxCliCommand::Create(options) = command else {
        panic!("expected create");
    };
    assert_eq!(options.sandbox_id, Some(sandbox_id));
    assert_eq!(options.toolchain_id, toolchain_id);
    assert_eq!(options.memory_mb, Some(4096));
    assert_eq!(options.wall_time_ms, Some(900000));
    assert_eq!(options.max_input_bytes, Some(10485760));
    assert_eq!(options.max_output_bytes, Some(10485760));
    assert_eq!(options.max_workspace_bytes, Some(1073741824));
    assert_eq!(options.max_written_bytes_per_command, Some(1073741824));
    assert_eq!(options.max_file_count, Some(100000));
    assert_eq!(
        options.temp_policy,
        sinomo_runtime::sandbox::SandboxTempPolicy::RetainAcrossSessions
    );
    assert_eq!(options.mounts.len(), 1);
    assert_eq!(
        options.mounts[0].source_sifs_path,
        "/services/sandbox/shared"
    );
    assert_eq!(
        options.mounts[0].mount_path,
        sinomo_runtime::sandbox::SandboxPath::new("/shared").expect("mount path")
    );
    assert_eq!(
        options.mounts[0].mode,
        sinomo_runtime::sandbox::SandboxMountMode::ReadOnly
    );
    assert_eq!(options.execution, SandboxMetadataExecution::Local);

    let command =
        parse_sandbox_command(&["create".to_string(), "--daemon".to_string()]).expect("daemon");
    let SandboxCliCommand::Create(options) = command else {
        panic!("expected create");
    };
    assert_eq!(options.execution, SandboxMetadataExecution::Daemon);
}

#[test]
fn sandbox_doctor_parser_accepts_optional_sandbox_id() {
    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();
    let command =
        parse_sandbox_command(&["doctor".to_string(), sandbox_id.to_string()]).expect("doctor");
    let SandboxCliCommand::Doctor(options) = command else {
        panic!("expected doctor");
    };
    assert_eq!(options.sandbox_id, Some(sandbox_id));

    let command = parse_sandbox_command(&["doctor".to_string()]).expect("doctor all");
    let SandboxCliCommand::Doctor(options) = command else {
        panic!("expected doctor");
    };
    assert_eq!(options.sandbox_id, None);
}

#[test]
fn sandbox_status_parser_accepts_daemon_execution() {
    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();
    let command = parse_sandbox_command(&[
        "status".to_string(),
        sandbox_id.to_string(),
        "--daemon".to_string(),
    ])
    .expect("status");
    let SandboxCliCommand::Status(options) = command else {
        panic!("expected status");
    };
    assert_eq!(options.sandbox_id, sandbox_id);
    assert_eq!(options.execution, SandboxMetadataExecution::Daemon);
}

#[test]
fn sandbox_config_parser_accepts_cwd_env_and_daemon() {
    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();
    let command = parse_sandbox_command(&[
        "config".to_string(),
        sandbox_id.to_string(),
        "--cwd".to_string(),
        "/work/src".to_string(),
        "--env".to_string(),
        "RUST_LOG=debug".to_string(),
        "--unset-env".to_string(),
        "OLD_VAR".to_string(),
        "--daemon".to_string(),
    ])
    .expect("config");
    let SandboxCliCommand::Config(options) = command else {
        panic!("expected config");
    };
    assert_eq!(options.sandbox_id, sandbox_id);
    assert_eq!(options.cwd.expect("cwd").as_str(), "/work/src");
    assert_eq!(
        options.env_set.get("RUST_LOG").map(String::as_str),
        Some("debug")
    );
    assert_eq!(options.env_unset, vec!["OLD_VAR"]);
    assert_eq!(options.execution, SandboxMetadataExecution::Daemon);
}

#[test]
fn sandbox_config_parser_rejects_embedded_nul_env_value() {
    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();
    let error = parse_sandbox_command(&[
        "config".to_string(),
        sandbox_id.to_string(),
        "--env".to_string(),
        "RUST_LOG=bad\0value".to_string(),
    ])
    .expect_err("embedded NUL env denied");

    match error {
        SandboxCliError::Parse { detail } => {
            assert!(detail.contains("sandbox env `RUST_LOG` contains an embedded NUL byte"))
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn sandbox_run_parser_accepts_wasm_module_timeout_and_args() {
    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();
    let command = parse_sandbox_command(&[
        "run".to_string(),
        sandbox_id.to_string(),
        "--timeout-ms".to_string(),
        "1234".to_string(),
        "--wasm".to_string(),
        "/tmp/hello.wasm".to_string(),
        "--".to_string(),
        "one".to_string(),
        "two".to_string(),
    ])
    .expect("sandbox run");

    let SandboxCliCommand::Run(options) = command else {
        panic!("expected run");
    };
    assert_eq!(options.sandbox_id, sandbox_id);
    assert_eq!(
        options.target,
        SandboxRunTarget::WasmModule(PathBuf::from("/tmp/hello.wasm"))
    );
    assert_eq!(options.timeout_ms, 1234);
    assert!(!options.stdin);
    assert!(options.allow_unsigned);
    assert!(options.allowed_capabilities.is_empty());
    assert_eq!(options.argv, vec!["hello.wasm", "one", "two"]);
    assert_eq!(options.execution, SandboxRunExecution::Local);
}

#[test]
fn sandbox_run_parser_accepts_toolchain_command_and_args() {
    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();
    let command = parse_sandbox_command(&[
        "run".to_string(),
        sandbox_id.to_string(),
        "--daemon".to_string(),
        "cat".to_string(),
        "--".to_string(),
        "/work/main.rs".to_string(),
    ])
    .expect("sandbox run");

    let SandboxCliCommand::Run(options) = command else {
        panic!("expected run");
    };
    assert_eq!(options.sandbox_id, sandbox_id);
    assert_eq!(
        options.target,
        SandboxRunTarget::ToolchainCommand("cat".to_string())
    );
    assert_eq!(options.argv, vec!["cat", "/work/main.rs"]);
    assert!(!options.stdin);
    assert!(!options.allow_unsigned);
    assert!(options.allowed_capabilities.is_empty());
    assert_eq!(options.execution, SandboxRunExecution::Daemon);
    assert!(matches!(
        parse_sandbox_command(&[
            "run".to_string(),
            sandbox_id.to_string(),
            "--daemon".to_string(),
            "--wasm".to_string(),
            "/tmp/hello.wasm".to_string(),
        ]),
        Err(SandboxCliError::Parse { .. })
    ));
}

#[test]
fn sandbox_run_parser_accepts_container_backend_options() {
    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();
    let command = parse_sandbox_command(&[
        "run".to_string(),
        sandbox_id.to_string(),
        "--container-runtime".to_string(),
        "podman".to_string(),
        "--container-image".to_string(),
        "alpine:3.20".to_string(),
        "sh".to_string(),
        "--".to_string(),
        "-c".to_string(),
        "pwd".to_string(),
    ])
    .expect("sandbox run");

    let SandboxCliCommand::Run(options) = command else {
        panic!("expected run");
    };
    assert_eq!(options.sandbox_id, sandbox_id);
    assert_eq!(
        options.target,
        SandboxRunTarget::ToolchainCommand("sh".to_string())
    );
    assert_eq!(options.argv, vec!["sh", "-c", "pwd"]);
    let SandboxRunExecution::Container(config) = options.execution else {
        panic!("expected container execution");
    };
    assert_eq!(
        config.runtime,
        sinomo_runtime::sandbox::SandboxContainerRuntime::Podman
    );
    assert_eq!(config.image, "alpine:3.20");
}

#[test]
fn sandbox_run_parser_rejects_container_wasm_and_policy_overrides() {
    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();
    assert!(matches!(
        parse_sandbox_command(&[
            "run".to_string(),
            sandbox_id.to_string(),
            "--docker".to_string(),
            "alpine:3.20".to_string(),
            "--wasm".to_string(),
            "/tmp/hello.wasm".to_string(),
        ]),
        Err(SandboxCliError::Parse { .. })
    ));
    assert!(matches!(
        parse_sandbox_command(&[
            "run".to_string(),
            sandbox_id.to_string(),
            "--docker".to_string(),
            "alpine:3.20".to_string(),
            "--allow-capability".to_string(),
            "wasix.unpinned-child-exec".to_string(),
            "sh".to_string(),
        ]),
        Err(SandboxCliError::Parse { .. })
    ));
    assert!(matches!(
        parse_sandbox_command(&[
            "run".to_string(),
            sandbox_id.to_string(),
            "--daemon".to_string(),
            "--podman".to_string(),
            "alpine:3.20".to_string(),
            "sh".to_string(),
        ]),
        Err(SandboxCliError::Parse { .. })
    ));
}

#[test]
fn sandbox_run_parser_accepts_local_capability_overrides() {
    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();
    let command = parse_sandbox_command(&[
        "run".to_string(),
        sandbox_id.to_string(),
        "--allow-unsigned".to_string(),
        "--allow-capability".to_string(),
        "wasix.unpinned-child-exec".to_string(),
        "python".to_string(),
        "--".to_string(),
        "-m".to_string(),
        "pip".to_string(),
        "--version".to_string(),
    ])
    .expect("sandbox run");

    let SandboxCliCommand::Run(options) = command else {
        panic!("expected run");
    };
    assert_eq!(
        options.target,
        SandboxRunTarget::ToolchainCommand("python".to_string())
    );
    assert!(options.allow_unsigned);
    assert_eq!(
        options.allowed_capabilities,
        vec!["wasix.unpinned-child-exec"]
    );
    assert_eq!(options.argv, vec!["python", "-m", "pip", "--version"]);
    assert_eq!(options.execution, SandboxRunExecution::Local);

    assert!(matches!(
        parse_sandbox_command(&[
            "run".to_string(),
            sandbox_id.to_string(),
            "--daemon".to_string(),
            "--allow-capability".to_string(),
            "wasix.unpinned-child-exec".to_string(),
            "python".to_string(),
        ]),
        Err(SandboxCliError::Parse { .. })
    ));
    assert!(matches!(
        parse_sandbox_command(&[
            "run".to_string(),
            sandbox_id.to_string(),
            "--allow-capability".to_string(),
            "wasix.unpinned-child-exec".to_string(),
            "--wasm".to_string(),
            "/tmp/hello.wasm".to_string(),
        ]),
        Err(SandboxCliError::Parse { .. })
    ));
}

#[test]
fn sandbox_run_parser_accepts_stdin_forwarding() {
    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();
    let command = parse_sandbox_command(&[
        "run".to_string(),
        sandbox_id.to_string(),
        "--stdin".to_string(),
        "xargs".to_string(),
        "--".to_string(),
        "echo".to_string(),
    ])
    .expect("sandbox run");

    let SandboxCliCommand::Run(options) = command else {
        panic!("expected run");
    };
    assert_eq!(options.sandbox_id, sandbox_id);
    assert_eq!(
        options.target,
        SandboxRunTarget::ToolchainCommand("xargs".to_string())
    );
    assert!(options.stdin);
    assert!(options.allow_unsigned);
    assert!(options.allowed_capabilities.is_empty());
    assert_eq!(options.argv, vec!["xargs", "echo"]);
}

#[test]
fn sandbox_process_plan_parser_accepts_plan_path_and_timeout() {
    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();
    let command = parse_sandbox_command(&[
        "process-plan".to_string(),
        sandbox_id.to_string(),
        "/tmp/plan.json".to_string(),
        "--timeout-ms".to_string(),
        "3456".to_string(),
    ])
    .expect("sandbox process plan");

    let SandboxCliCommand::ProcessPlan(options) = command else {
        panic!("expected process plan");
    };
    assert_eq!(options.sandbox_id, sandbox_id);
    assert_eq!(options.plan_path, PathBuf::from("/tmp/plan.json"));
    assert_eq!(options.timeout_ms, 3456);
    assert_eq!(options.execution, SandboxRunExecution::Local);
    let daemon_command = parse_sandbox_command(&[
        "process-plan".to_string(),
        sandbox_id.to_string(),
        "/tmp/plan.json".to_string(),
        "--daemon".to_string(),
    ])
    .expect("daemon sandbox process plan");
    let SandboxCliCommand::ProcessPlan(daemon_options) = daemon_command else {
        panic!("expected process plan");
    };
    assert_eq!(daemon_options.execution, SandboxRunExecution::Daemon);

    let container_command = parse_sandbox_command(&[
        "process-plan".to_string(),
        sandbox_id.to_string(),
        "--docker".to_string(),
        "alpine:3.20".to_string(),
        "/tmp/plan.json".to_string(),
    ])
    .expect("container sandbox process plan");
    let SandboxCliCommand::ProcessPlan(container_options) = container_command else {
        panic!("expected process plan");
    };
    assert!(matches!(
        container_options.execution,
        SandboxRunExecution::Container(_)
    ));
}

#[test]
fn sandbox_shell_parser_accepts_script_and_timeout() {
    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();
    let command = parse_sandbox_command(&[
        "shell".to_string(),
        sandbox_id.to_string(),
        "--timeout-ms".to_string(),
        "2345".to_string(),
        "--daemon".to_string(),
        "--".to_string(),
        "printf hello".to_string(),
    ])
    .expect("sandbox shell");

    let SandboxCliCommand::Shell(options) = command else {
        panic!("expected shell");
    };
    assert_eq!(options.sandbox_id, sandbox_id);
    assert_eq!(options.timeout_ms, 2345);
    assert_eq!(options.script.as_deref(), Some("printf hello"));
    assert_eq!(options.execution, SandboxRunExecution::Daemon);

    let container_command = parse_sandbox_command(&[
        "shell".to_string(),
        sandbox_id.to_string(),
        "--podman".to_string(),
        "alpine:3.20".to_string(),
        "--".to_string(),
        "echo hi".to_string(),
    ])
    .expect("container sandbox shell");
    let SandboxCliCommand::Shell(container_options) = container_command else {
        panic!("expected shell");
    };
    assert!(matches!(
        container_options.execution,
        SandboxRunExecution::Container(_)
    ));
}

#[test]
fn sandbox_toolchain_add_command_parser_accepts_options() {
    let toolchain_id = sinomo_runtime::sandbox::SandboxToolchainId::generate();
    let command = parse_sandbox_command(&[
        "toolchain".to_string(),
        "add-command".to_string(),
        "--toolchain".to_string(),
        toolchain_id.to_string(),
        "--version".to_string(),
        "busybox-test".to_string(),
        "--target-abi".to_string(),
        "wasix".to_string(),
        "--provenance".to_string(),
        "unit test".to_string(),
        "--daemon".to_string(),
        "--capability".to_string(),
        "network.http".to_string(),
        "--memory-mb".to_string(),
        "64".to_string(),
        "--wall-time-ms".to_string(),
        "1234".to_string(),
        "--max-input-bytes".to_string(),
        "2048".to_string(),
        "--max-output-bytes".to_string(),
        "4096".to_string(),
        "cat".to_string(),
        "/tmp/cat.wasm".to_string(),
    ])
    .expect("toolchain add-command");

    let SandboxCliCommand::ToolchainAddCommand(options) = command else {
        panic!("expected toolchain add-command");
    };
    assert_eq!(options.toolchain_id, toolchain_id);
    assert_eq!(options.version, "busybox-test");
    assert_eq!(
        options.target_abi,
        sinomo_runtime::sandbox::SandboxWasiAbi::Wasix
    );
    assert_eq!(options.provenance, "unit test");
    assert_eq!(options.command_name, "cat");
    assert_eq!(options.module_path, PathBuf::from("/tmp/cat.wasm"));
    assert_eq!(options.execution, SandboxMetadataExecution::Daemon);
    assert_eq!(options.required_capabilities, vec!["network.http"]);
    assert_eq!(options.default_limits.memory_mb, 64);
    assert_eq!(options.default_limits.wall_time_ms, 1234);
    assert_eq!(options.default_limits.max_input_bytes, 2048);
    assert_eq!(options.default_limits.max_output_bytes, 4096);
}

#[test]
fn sandbox_toolchain_add_command_parser_defaults_to_wasix() {
    let command = parse_sandbox_command(&[
        "toolchain".to_string(),
        "add-command".to_string(),
        "cat".to_string(),
        "/tmp/cat.wasm".to_string(),
    ])
    .expect("toolchain add-command");

    let SandboxCliCommand::ToolchainAddCommand(options) = command else {
        panic!("expected toolchain add-command");
    };
    assert_eq!(
        options.target_abi,
        sinomo_runtime::sandbox::SandboxWasiAbi::Wasix
    );
}

#[test]
fn sandbox_toolchain_add_command_parser_rejects_invalid_capability_value() {
    let error = parse_sandbox_command(&[
        "toolchain".to_string(),
        "add-command".to_string(),
        "--capability".to_string(),
        "network\0http".to_string(),
        "cat".to_string(),
        "/tmp/cat.wasm".to_string(),
    ])
    .expect_err("embedded NUL capability denied");

    match error {
        SandboxCliError::Parse { detail } => {
            assert!(detail.contains("sandbox toolchain command capability"));
            assert!(detail.contains("embedded NUL byte"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn sandbox_toolchain_add_artifact_parser_accepts_options() {
    let toolchain_id = sinomo_runtime::sandbox::SandboxToolchainId::generate();
    let command = parse_sandbox_command(&[
        "toolchain".to_string(),
        "add-artifact".to_string(),
        "--toolchain".to_string(),
        toolchain_id.to_string(),
        "--version".to_string(),
        "python-wasi-test".to_string(),
        "--provenance".to_string(),
        "unit test".to_string(),
        "--daemon".to_string(),
        "lib/python/os.py".to_string(),
        "/tmp/os.py".to_string(),
    ])
    .expect("toolchain add-artifact");

    let SandboxCliCommand::ToolchainAddArtifact(options) = command else {
        panic!("expected toolchain add-artifact");
    };
    assert_eq!(options.toolchain_id, toolchain_id);
    assert_eq!(options.version, "python-wasi-test");
    assert_eq!(options.provenance, "unit test");
    assert_eq!(options.relative_path, "lib/python/os.py");
    assert_eq!(options.source_path, PathBuf::from("/tmp/os.py"));
    assert_eq!(options.execution, SandboxMetadataExecution::Daemon);
}

#[test]
fn sandbox_toolchain_import_busybox_parser_accepts_options() {
    let command = parse_sandbox_command(&[
        "toolchain".to_string(),
        "import-busybox".to_string(),
        "--version".to_string(),
        "busybox-wasi-test".to_string(),
        "--provenance".to_string(),
        "unit test build".to_string(),
        "/tmp/busybox.wasm".to_string(),
    ])
    .expect("toolchain import busybox");

    let SandboxCliCommand::ToolchainImportBusyBox(options) = command else {
        panic!("expected toolchain import busybox");
    };
    assert_eq!(options.version, "busybox-wasi-test");
    assert_eq!(options.provenance, "unit test build");
    assert_eq!(options.module_path, PathBuf::from("/tmp/busybox.wasm"));
    assert_eq!(options.execution, SandboxMetadataExecution::Local);

    let command = parse_sandbox_command(&[
        "toolchain".to_string(),
        "import-busybox".to_string(),
        "--daemon".to_string(),
        "--version".to_string(),
        "busybox-wasi-test".to_string(),
        "/tmp/busybox.wasm".to_string(),
    ])
    .expect("daemon toolchain import busybox");
    let SandboxCliCommand::ToolchainImportBusyBox(options) = command else {
        panic!("expected toolchain import busybox");
    };
    assert_eq!(options.execution, SandboxMetadataExecution::Daemon);
}

#[test]
fn sandbox_toolchain_import_package_parser_accepts_options() {
    let command = parse_sandbox_command(&[
        "toolchain".to_string(),
        "import-package".to_string(),
        "--daemon".to_string(),
        "--into".to_string(),
        "toolchain_busybox".to_string(),
        "/tmp/package.json".to_string(),
    ])
    .expect("import package");

    let SandboxCliCommand::ToolchainImportPackage(options) = command else {
        panic!("expected toolchain import package");
    };
    assert_eq!(options.package_path, PathBuf::from("/tmp/package.json"));
    assert_eq!(
        options.target_toolchain_id,
        Some(
            sinomo_runtime::sandbox::SandboxToolchainId::from_legacy_named("toolchain_busybox")
                .expect("busybox id")
        )
    );
    assert_eq!(options.execution, SandboxMetadataExecution::Daemon);
}

#[test]
fn sandbox_toolchain_verify_package_parser_accepts_manifest() {
    let command = parse_sandbox_command(&[
        "toolchain".to_string(),
        "verify-package".to_string(),
        "/tmp/package.json".to_string(),
    ])
    .expect("verify package");

    let SandboxCliCommand::ToolchainVerifyPackage(options) = command else {
        panic!("expected toolchain verify package");
    };
    assert_eq!(options.package_path, PathBuf::from("/tmp/package.json"));
}

#[test]
fn sandbox_toolchain_hash_parser_accepts_file() {
    let command = parse_sandbox_command(&[
        "toolchain".to_string(),
        "hash".to_string(),
        "/tmp/script.wasm".to_string(),
    ])
    .expect("hash");

    let SandboxCliCommand::ToolchainHash(options) = command else {
        panic!("expected toolchain hash");
    };
    assert_eq!(options.path, PathBuf::from("/tmp/script.wasm"));
}

#[test]
fn sandbox_module_artifact_parser_accepts_component_register() {
    let artifact_id = sinomo_runtime::module::ArtifactId::generate();
    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();
    let command = parse_sandbox_command(&[
        "module-artifact".to_string(),
        "register".to_string(),
        "--artifact".to_string(),
        artifact_id.to_string(),
        "--component-model".to_string(),
        "--abi".to_string(),
        "wasix".to_string(),
        "--sandbox".to_string(),
        sandbox_id.to_string(),
        "--argv".to_string(),
        "component.wasm".to_string(),
        "--cwd".to_string(),
        "/work".to_string(),
        "--env".to_string(),
        "RUST_LOG=debug".to_string(),
        "--memory-mb".to_string(),
        "128".to_string(),
        "--wall-time-ms".to_string(),
        "2000".to_string(),
        "--max-output-bytes".to_string(),
        "4096".to_string(),
        "--timeout-ms".to_string(),
        "1500".to_string(),
        "/tmp/component.wasm".to_string(),
    ])
    .expect("module artifact register");

    let SandboxCliCommand::ModuleArtifact(SandboxModuleArtifactCliCommand::Register(options)) =
        command
    else {
        panic!("expected module artifact register");
    };
    assert_eq!(options.artifact_id, artifact_id);
    assert_eq!(options.source_path, PathBuf::from("/tmp/component.wasm"));
    assert_eq!(
        options.interface_version.as_deref(),
        Some(sinomo_runtime::module::SINOMO_MODULE_V1_INTERFACE_VERSION)
    );
    assert!(options.component_model);
    assert_eq!(options.abi, sinomo_runtime::sandbox::SandboxWasiAbi::Wasix);
    assert_eq!(options.sandbox_id, Some(sandbox_id));
    assert_eq!(options.argv, vec!["component.wasm"]);
    assert_eq!(options.cwd.expect("cwd").as_str(), "/work");
    assert_eq!(
        options.env.get("RUST_LOG").map(String::as_str),
        Some("debug")
    );
    assert_eq!(options.limits.memory_mb, 128);
    assert_eq!(options.limits.wall_time_ms, 2000);
    assert_eq!(options.limits.max_output_bytes, 4096);
    assert_eq!(options.timeout_ms, Some(1500));
    assert_eq!(options.rollout_material, None);
}

#[test]
fn sandbox_module_artifact_parser_accepts_rollout_material_options() {
    let artifact_id = sinomo_runtime::module::ArtifactId::from_legacy_named("artifact_candidate")
        .expect("artifact");
    let module_id =
        sinomo_runtime::module::ModuleId::from_legacy_named("module.trigger.silence_policy")
            .expect("module");
    let active_artifact_id =
        sinomo_runtime::module::ArtifactId::from_legacy_named("artifact_active").expect("active");
    let rollout_plan_id = Id::from_legacy_named("rollout_candidate").expect("rollout");
    let policy_decision_id = Id::from_legacy_named("policy_candidate").expect("policy");
    let command = parse_sandbox_command(&[
        "module-artifact".to_string(),
        "register".to_string(),
        "--artifact".to_string(),
        artifact_id.to_string(),
        "--component-model".to_string(),
        "--candidate-bundle".to_string(),
        "/tmp/bundle".to_string(),
        "--module".to_string(),
        module_id.to_string(),
        "--active-artifact".to_string(),
        active_artifact_id.to_string(),
        "--rollout-plan".to_string(),
        rollout_plan_id.to_string(),
        "--policy-decision".to_string(),
        policy_decision_id.to_string(),
        "--module-version".to_string(),
        "0.2.0".to_string(),
        "--module-owner".to_string(),
        "trigger".to_string(),
        "--evidence".to_string(),
        "module-replay://report".to_string(),
        "/tmp/component.wasm".to_string(),
    ])
    .expect("module artifact register");

    let SandboxCliCommand::ModuleArtifact(SandboxModuleArtifactCliCommand::Register(options)) =
        command
    else {
        panic!("expected module artifact register");
    };
    let rollout_material = options.rollout_material.expect("rollout material");
    assert_eq!(
        rollout_material.candidate_bundle_path,
        PathBuf::from("/tmp/bundle")
    );
    assert_eq!(rollout_material.module_id, module_id);
    assert_eq!(rollout_material.active_artifact_id, active_artifact_id);
    assert_eq!(rollout_material.rollout_plan_id, Some(rollout_plan_id));
    assert_eq!(
        rollout_material.policy_decision_id,
        Some(policy_decision_id)
    );
    assert_eq!(rollout_material.module_version, "0.2.0");
    assert_eq!(rollout_material.module_owner, "trigger");
    assert_eq!(
        rollout_material.evidence_refs,
        vec!["module-replay://report".to_string()]
    );
}

#[test]
fn sandbox_module_replay_parser_accepts_fixture_and_report() {
    let command = parse_sandbox_command(&[
        "module-replay".to_string(),
        "run".to_string(),
        "--fixture".to_string(),
        "/tmp/fixture.json".to_string(),
        "--report".to_string(),
        "/tmp/report.json".to_string(),
    ])
    .expect("module replay run");

    let SandboxCliCommand::ModuleReplay(SandboxModuleReplayCliCommand::Run(options)) = command
    else {
        panic!("expected module replay run");
    };
    assert_eq!(options.fixture_path, PathBuf::from("/tmp/fixture.json"));
    assert_eq!(options.report_path, PathBuf::from("/tmp/report.json"));
}

#[test]
fn sandbox_toolchain_busybox_plan_parser_accepts_command() {
    let command = parse_sandbox_command(&["toolchain".to_string(), "busybox-plan".to_string()])
        .expect("busybox plan");

    assert_eq!(command, SandboxCliCommand::ToolchainBusyBoxPlan);
}

#[test]
fn sandbox_toolchain_bash_plan_parser_accepts_command() {
    let command = parse_sandbox_command(&["toolchain".to_string(), "bash-plan".to_string()])
        .expect("bash plan");

    assert_eq!(command, SandboxCliCommand::ToolchainBashPlan);
}

#[test]
fn sandbox_toolchain_build_tools_plan_parser_accepts_command() {
    let command = parse_sandbox_command(&["toolchain".to_string(), "build-tools-plan".to_string()])
        .expect("build tools plan");

    assert_eq!(command, SandboxCliCommand::ToolchainBuildToolsPlan);
}

#[test]
fn sandbox_toolchain_build_tools_plan_reports_planned_real_tool_gates() {
    let plan = sandbox_toolchain_build_tools_plan_text();
    assert!(plan.contains("toolchains/build-tools/README.md"));
    assert!(plan.contains("tracked build tool commands:"));
    assert!(plan.contains("  tcc (implemented):"));
    assert!(plan.contains("  make (implemented):"));
    assert!(plan.contains("  makepkg (required):"));
    assert!(plan.contains("toolchains/build-tools/build-tcc-wasix.sh"));
    assert!(plan.contains("toolchains/build-tools/build-make-wasix.sh"));
    assert!(plan.contains("toolchains/build-tools/build-makepkg-wasix.sh"));
    assert!(plan.contains("verify-tcc <sandbox-id>"));
    assert!(plan.contains("verify-make <sandbox-id>"));
    assert!(plan.contains("verify-makepkg <sandbox-id>"));
    assert!(plan.contains("real WASI packages"));
    assert!(plan.contains("do not expose host fallbacks"));
    assert!(plan.contains("local and daemon/SIFS verifier coverage"));
}

#[test]
fn sandbox_toolchain_verify_make_parser_accepts_command() {
    let command = parse_sandbox_command(&[
        "toolchain".to_string(),
        "verify-make".to_string(),
        "01k4z8q5d00000000000000001".to_string(),
        "--allow-unsigned".to_string(),
    ])
    .expect("verify make");

    assert_eq!(
        command,
        SandboxCliCommand::ToolchainVerifyMake(SandboxToolchainVerifyMakeOptions {
            sandbox_id: sinomo_runtime::sandbox::SandboxId::new(
                "01k4z8q5d00000000000000001".to_string()
            )
            .expect("sandbox id"),
            allow_unsigned: true,
            execution: SandboxMetadataExecution::Local,
        })
    );
}

#[test]
fn sandbox_toolchain_verify_makepkg_parser_accepts_command() {
    let command = parse_sandbox_command(&[
        "toolchain".to_string(),
        "verify-makepkg".to_string(),
        "01k4z8q5d00000000000000001".to_string(),
        "--allow-unsigned".to_string(),
    ])
    .expect("verify makepkg");

    assert_eq!(
        command,
        SandboxCliCommand::ToolchainVerifyMakepkg(SandboxToolchainVerifyMakepkgOptions {
            sandbox_id: sinomo_runtime::sandbox::SandboxId::new(
                "01k4z8q5d00000000000000001".to_string()
            )
            .expect("sandbox id"),
            allow_unsigned: true,
            execution: SandboxMetadataExecution::Local,
        })
    );
}

#[test]
fn sandbox_toolchain_verify_tcc_parser_accepts_command() {
    let command = parse_sandbox_command(&[
        "toolchain".to_string(),
        "verify-tcc".to_string(),
        "01k4z8q5d00000000000000001".to_string(),
        "--allow-unsigned".to_string(),
    ])
    .expect("verify tcc");

    assert_eq!(
        command,
        SandboxCliCommand::ToolchainVerifyTcc(SandboxToolchainVerifyTccOptions {
            sandbox_id: sinomo_runtime::sandbox::SandboxId::new(
                "01k4z8q5d00000000000000001".to_string()
            )
            .expect("sandbox id"),
            allow_unsigned: true,
            execution: SandboxMetadataExecution::Local,
        })
    );
}

#[test]
fn sandbox_toolchain_bash_plan_reports_wasix_gate() {
    let plan = sandbox_toolchain_bash_plan_text();
    assert!(plan.contains("commands: bash, sh"));
    assert!(plan.contains("target ABI: wasix"));
    assert!(plan.contains("toolchains/bash/build-bash-wasix.sh"));
    assert!(plan.contains("maintained Bash WASIX package passes"));
    assert!(plan.contains("verify-bash <sandbox-id>"));
    assert!(plan.contains("bash-pipe-external-grep: bash"));
    assert!(plan.contains("sh-pipe-external-grep: sh"));
    assert!(!plan.contains("BusyBox `sh`"));
}

#[test]
fn sandbox_toolchain_busybox_plan_reports_known_mvp_gaps() {
    let plan = sandbox_toolchain_busybox_plan_text();
    assert!(plan.contains("core applets:"));
    assert!(plan.contains("  grep\n"));
    assert!(plan.contains("fidelity cases:"));
    assert!(plan.contains("awk-system-spawn: awk"));
    assert!(!plan.contains("known MVP gaps:"));
}

#[test]
fn sandbox_toolchain_python_plan_parser_accepts_command() {
    let command = parse_sandbox_command(&["toolchain".to_string(), "python-plan".to_string()])
        .expect("python plan");

    assert_eq!(command, SandboxCliCommand::ToolchainPythonPlan);
}

#[test]
fn sandbox_toolchain_python_plan_reports_wasix_gate() {
    let plan = sandbox_toolchain_python_plan_text();
    assert!(plan.contains("  python\n"));
    assert!(plan.contains("  pip\n"));
    assert!(plan.contains("target ABI: wasix"));
    assert!(plan.contains("toolchains/python/build-cpython-wasix.sh"));
    assert!(plan.contains("maintained CPython WASIX package is the active POSIX/process gate"));
    assert!(plan.contains("native-extension loading"));
    assert!(plan.contains("numpy array execution"));
    assert!(plan.contains("matplotlib Agg rendering"));
    assert!(plan.contains("verify-python <sandbox-id>"));
    assert!(plan.contains("python-json-stdlib: python"));
    assert!(plan.contains("pip-wheel-local-pure-package: python"));
    assert!(plan.contains("pip-install-local-pure-package: python"));
    assert!(plan.contains("pip-install-local-simple-index: pip"));
    assert!(plan.contains("pip-version: pip"));
}

#[test]
fn sandbox_python_wasix_native_verifier_script_tracks_active_gate() {
    let script =
        include_str!("../../sinomo-sandbox/toolchains/python/verify-python-wasix-native.sh");

    assert!(script.contains("subprocess.run"));
    assert!(script.contains("Popen"));
    assert!(script.contains("TimeoutExpired"));
    assert!(script.contains("--no-index"));
    assert!(script.contains("--find-links /work/wheelhouse"));
    assert!(script.contains("numpy matplotlib"));
    assert!(script.contains("sandbox checkpoint"));
    assert!(script.contains("sandbox resume"));
    assert!(script.contains("np.arange(4) ** 2"));
    assert!(script.contains("matplotlib.use('Agg')"));
}

#[test]
fn sandbox_toolchain_verify_bash_parser_accepts_options() {
    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();
    let command = parse_sandbox_command(&[
        "toolchain".to_string(),
        "verify-bash".to_string(),
        sandbox_id.to_string(),
        "--allow-unsigned".to_string(),
    ])
    .expect("verify bash");

    let SandboxCliCommand::ToolchainVerifyBash(options) = command else {
        panic!("expected toolchain verify bash");
    };
    assert_eq!(options.sandbox_id, sandbox_id);
    assert!(options.allow_unsigned);
    assert_eq!(options.execution, SandboxMetadataExecution::Local);

    let command = parse_sandbox_command(&[
        "toolchain".to_string(),
        "verify-bash".to_string(),
        sandbox_id.to_string(),
        "--daemon".to_string(),
    ])
    .expect("verify bash daemon");
    let SandboxCliCommand::ToolchainVerifyBash(options) = command else {
        panic!("expected toolchain verify bash");
    };
    assert!(!options.allow_unsigned);
    assert_eq!(options.execution, SandboxMetadataExecution::Daemon);
}

#[test]
fn sandbox_toolchain_verify_busybox_parser_accepts_options() {
    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();
    let command = parse_sandbox_command(&[
        "toolchain".to_string(),
        "verify-busybox".to_string(),
        sandbox_id.to_string(),
        "--allow-unsigned".to_string(),
    ])
    .expect("verify busybox");

    let SandboxCliCommand::ToolchainVerifyBusyBox(options) = command else {
        panic!("expected toolchain verify busybox");
    };
    assert_eq!(options.sandbox_id, sandbox_id);
    assert!(options.allow_unsigned);
    assert_eq!(options.execution, SandboxMetadataExecution::Local);

    let command = parse_sandbox_command(&[
        "toolchain".to_string(),
        "verify-busybox".to_string(),
        sandbox_id.to_string(),
        "--daemon".to_string(),
    ])
    .expect("verify busybox daemon");
    let SandboxCliCommand::ToolchainVerifyBusyBox(options) = command else {
        panic!("expected toolchain verify busybox");
    };
    assert!(!options.allow_unsigned);
    assert_eq!(options.execution, SandboxMetadataExecution::Daemon);
}

#[test]
fn sandbox_toolchain_verify_python_parser_accepts_options() {
    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();
    let command = parse_sandbox_command(&[
        "toolchain".to_string(),
        "verify-python".to_string(),
        sandbox_id.to_string(),
        "--allow-unsigned".to_string(),
    ])
    .expect("verify python");

    let SandboxCliCommand::ToolchainVerifyPython(options) = command else {
        panic!("expected toolchain verify python");
    };
    assert_eq!(options.sandbox_id, sandbox_id);
    assert!(options.allow_unsigned);
    assert_eq!(options.execution, SandboxMetadataExecution::Local);

    let command = parse_sandbox_command(&[
        "toolchain".to_string(),
        "verify-python".to_string(),
        sandbox_id.to_string(),
        "--daemon".to_string(),
    ])
    .expect("verify python daemon");
    let SandboxCliCommand::ToolchainVerifyPython(options) = command else {
        panic!("expected toolchain verify python");
    };
    assert!(!options.allow_unsigned);
    assert_eq!(options.execution, SandboxMetadataExecution::Daemon);
}

#[test]
fn sandbox_toolchain_verify_coding_parser_accepts_options() {
    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();
    let command = parse_sandbox_command(&[
        "toolchain".to_string(),
        "verify-coding".to_string(),
        sandbox_id.to_string(),
        "--allow-unsigned".to_string(),
    ])
    .expect("toolchain verify coding");
    let SandboxCliCommand::ToolchainVerifyCoding(options) = command else {
        panic!("expected toolchain verify coding");
    };
    assert_eq!(options.sandbox_id, sandbox_id);
    assert!(options.allow_unsigned);
    assert_eq!(options.execution, SandboxMetadataExecution::Local);
    assert!(!options.full);

    let command = parse_sandbox_command(&[
        "toolchain".to_string(),
        "verify-coding".to_string(),
        sandbox_id.to_string(),
        "--daemon".to_string(),
        "--full".to_string(),
    ])
    .expect("toolchain verify coding daemon");
    let SandboxCliCommand::ToolchainVerifyCoding(options) = command else {
        panic!("expected toolchain verify coding");
    };
    assert!(!options.allow_unsigned);
    assert_eq!(options.execution, SandboxMetadataExecution::Daemon);
    assert!(options.full);
}

#[test]
fn sandbox_toolchain_verify_parser_rejects_daemon_unsigned_override() {
    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();
    for command in [
        "verify-bash",
        "verify-busybox",
        "verify-python",
        "verify-coding",
        "verify-make",
        "verify-makepkg",
        "verify-tcc",
    ] {
        let error = parse_sandbox_command(&[
            "toolchain".to_string(),
            command.to_string(),
            sandbox_id.to_string(),
            "--daemon".to_string(),
            "--allow-unsigned".to_string(),
        ])
        .expect_err("daemon unsigned override rejected");
        assert!(format!("{error:?}").contains("cluster sandbox policy"));
    }
}

#[test]
fn sandbox_verify_coding_workflow_tracks_phase3_acceptance_commands() {
    let cases: BTreeMap<_, _> = CODING_WORKFLOW_BUSYBOX_CASES
        .iter()
        .map(|case| (case.name, case))
        .collect();
    for expected in [
        "phase3-mkdir-work-dir",
        "phase3-touch-file",
        "phase3-printf-stdout",
        "phase3-grep-stdin",
        "phase3-find-work-file",
        "phase3-diff-file-with-self",
        "phase3-cp-work-file",
        "phase3-cmp-copy",
        "phase3-mv-copy",
        "phase3-test-renamed-file",
        "phase3-chmod-renamed-file",
        "phase3-tar-create",
        "phase3-tar-list",
        "phase3-sed-stdin",
        "phase3-awk-stdin",
        "phase3-sort-stdin",
        "phase3-wc-stdin",
        "phase3-head-stdin",
        "phase3-tail-stdin",
        "phase3-rm-renamed-file",
        "phase3-rmdir-src-dir",
    ] {
        assert!(
            cases.contains_key(expected),
            "missing Phase 3 acceptance workflow case {expected}"
        );
    }
    assert_eq!(
        cases["phase3-find-work-file"].expected_stdout,
        "/work/phase3/file\n"
    );
    assert_eq!(cases["phase3-tar-list"].expected_stdout, "renamed\n");
    assert_eq!(cases["phase3-awk-stdin"].expected_stdout, "5\n");
}

#[test]
fn sandbox_busybox_daemon_toolchain_request_preserves_stdin_and_command() {
    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();
    let case = sinomo_runtime::sandbox::BUSYBOX_TOOLCHAIN_FIDELITY_CASES
        .iter()
        .find(|case| case.name == "grep-stdin")
        .expect("grep stdin case");

    let request = daemon_run_toolchain_request(
        sandbox_id.clone(),
        case.command.to_string(),
        case.argv_vec(),
        case.stdin_bytes(),
        5_000,
    );

    assert_eq!(request.sandbox_id, sandbox_id);
    assert_eq!(request.command_name, "grep");
    assert_eq!(request.argv, vec!["grep".to_string(), "hell".to_string()]);
    assert_eq!(request.stdin, Some(b"hello\n".to_vec()));
    assert_eq!(request.timeout_ms, 5_000);
}

#[test]
fn sandbox_policy_parser_accepts_show_and_set_options() {
    let show =
        parse_sandbox_command(&["policy".to_string(), "show".to_string()]).expect("policy show");
    assert_eq!(
        show,
        SandboxCliCommand::Policy(SandboxPolicyCliCommand::Show)
    );

    let toolchain_id = sinomo_runtime::sandbox::SandboxToolchainId::generate();
    let set = parse_sandbox_command(&[
        "policy".to_string(),
        "set".to_string(),
        "--allow-unsigned-toolchains".to_string(),
        "--allow-remote-local-owned-mounts".to_string(),
        "--max-memory-mb".to_string(),
        "1024".to_string(),
        "--max-wall-time-ms".to_string(),
        "5000".to_string(),
        "--max-input-bytes".to_string(),
        "3072".to_string(),
        "--max-output-bytes".to_string(),
        "4096".to_string(),
        "--max-workspace-bytes".to_string(),
        "8192".to_string(),
        "--max-written-bytes-per-command".to_string(),
        "2048".to_string(),
        "--max-file-count".to_string(),
        "128".to_string(),
        "--max-http-timeout-ms".to_string(),
        "30000".to_string(),
        "--max-concurrent-commands-per-sandbox".to_string(),
        "2".to_string(),
        "--max-concurrent-sandboxes-per-node".to_string(),
        "4".to_string(),
        "--allow-capability".to_string(),
        "network".to_string(),
        "--allow-toolchain".to_string(),
        toolchain_id.to_string(),
        "--allow-toolchain-provenance-prefix".to_string(),
        "sinomo://toolchains/".to_string(),
        "--allow-package-cache-prefix".to_string(),
        "/services/packages".to_string(),
        "--allow-mount-prefix".to_string(),
        "/sinomo/nodes".to_string(),
        "--allow-http-host".to_string(),
        "Packages.Example".to_string(),
    ])
    .expect("policy set");
    let SandboxCliCommand::Policy(SandboxPolicyCliCommand::Set(options)) = set else {
        panic!("expected policy set");
    };
    assert_eq!(options.allow_unsigned_toolchains, Some(true));
    assert_eq!(options.allow_remote_local_owned_mounts, Some(true));
    assert_eq!(options.max_memory_mb, Some(1024));
    assert_eq!(options.max_wall_time_ms, Some(5000));
    assert_eq!(options.max_input_bytes, Some(3072));
    assert_eq!(options.max_output_bytes, Some(4096));
    assert_eq!(options.max_workspace_bytes, Some(8192));
    assert_eq!(options.max_written_bytes_per_command, Some(2048));
    assert_eq!(options.max_file_count, Some(128));
    assert_eq!(options.max_http_timeout_ms, Some(30000));
    assert_eq!(options.max_concurrent_commands_per_sandbox, Some(2));
    assert_eq!(options.max_concurrent_sandboxes_per_node, Some(4));
    assert_eq!(options.allowed_capabilities, vec!["network"]);
    assert_eq!(options.allowed_toolchains, vec![toolchain_id]);
    assert_eq!(
        options.allowed_toolchain_provenance_prefixes,
        vec!["sinomo://toolchains/"]
    );
    assert_eq!(
        options.allowed_package_cache_prefixes,
        vec!["/services/packages"]
    );
    assert_eq!(options.allowed_mount_prefixes, vec!["/sinomo/nodes"]);
    assert_eq!(options.allowed_http_hosts, vec!["packages.example"]);

    let mut policy = sinomo_runtime::sandbox::SandboxPolicy::default();
    apply_sandbox_policy_set_options(&mut policy, options);
    assert_eq!(policy.max_limits.max_input_bytes, 3072);
    assert_eq!(policy.max_limits.max_output_bytes, 4096);
}

#[test]
fn sandbox_policy_parser_rejects_invalid_allowed_capability() {
    let error = parse_sandbox_command(&[
        "policy".to_string(),
        "set".to_string(),
        "--allow-capability".to_string(),
        "network\0http".to_string(),
    ])
    .expect_err("embedded NUL policy capability denied");

    match error {
        SandboxCliError::Parse { detail } => {
            assert!(detail.contains("sandbox capability"));
            assert!(detail.contains("embedded NUL byte"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn sandbox_policy_parser_rejects_invalid_prefix_and_host_values() {
    for (flag, value, expected) in [
        (
            "--allow-mount-prefix",
            "/services\0sandbox",
            "invalid SIFS path",
        ),
        (
            "--allow-package-cache-prefix",
            "/services//packages",
            "invalid SIFS path",
        ),
        (
            "--allow-toolchain-provenance-prefix",
            "sinomo://toolchains/\0bad",
            "embedded NUL byte",
        ),
        (
            "--allow-http-host",
            "packages.example:443",
            "invalid HTTP host",
        ),
    ] {
        let error = parse_sandbox_command(&[
            "policy".to_string(),
            "set".to_string(),
            flag.to_string(),
            value.to_string(),
        ])
        .expect_err("invalid policy value denied");
        assert!(
            format!("{error:?}").contains(expected),
            "expected {expected:?} in {error:?}"
        );
    }
}

#[test]
fn sandbox_policy_parser_applies_mediated_pip_index_policy_flags() {
    let set = parse_sandbox_command(&[
        "policy".to_string(),
        "set".to_string(),
        "--allow-capability".to_string(),
        "network.http".to_string(),
        "--allow-http-host".to_string(),
        "127.0.0.1".to_string(),
    ])
    .expect("policy set");
    let SandboxCliCommand::Policy(SandboxPolicyCliCommand::Set(options)) = set else {
        panic!("expected policy set");
    };

    assert_eq!(options.allowed_capabilities, vec!["network.http"]);
    assert_eq!(options.allowed_http_hosts, vec!["127.0.0.1"]);

    let mut policy = sinomo_runtime::sandbox::SandboxPolicy::default();
    apply_sandbox_policy_set_options(&mut policy, options);
    assert!(policy.allowed_command_capabilities.contains("network.http"));
    assert!(policy.allowed_http_hosts.contains("127.0.0.1"));
}

#[test]
fn sandbox_file_parser_accepts_ls_default_path_and_cat_path() {
    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();
    let ls = parse_sandbox_command(&["ls".to_string(), sandbox_id.to_string()]).expect("ls");
    let SandboxCliCommand::Ls(options) = ls else {
        panic!("expected ls");
    };
    assert_eq!(options.sandbox_id, sandbox_id);
    assert_eq!(options.path.as_str(), "/");
    assert_eq!(options.execution, SandboxMetadataExecution::Local);

    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();
    let cat = parse_sandbox_command(&[
        "cat".to_string(),
        sandbox_id.to_string(),
        "/work/main.rs".to_string(),
        "--daemon".to_string(),
    ])
    .expect("cat");
    let SandboxCliCommand::Cat(options) = cat else {
        panic!("expected cat");
    };
    assert_eq!(options.sandbox_id, sandbox_id);
    assert_eq!(options.path.as_str(), "/work/main.rs");
    assert_eq!(options.range, None);
    assert_eq!(options.execution, SandboxMetadataExecution::Daemon);

    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();
    let cat = parse_sandbox_command(&[
        "cat".to_string(),
        sandbox_id.to_string(),
        "/dev/zero".to_string(),
        "--range".to_string(),
        "2:6".to_string(),
    ])
    .expect("cat range");
    let SandboxCliCommand::Cat(options) = cat else {
        panic!("expected cat range");
    };
    assert_eq!(options.sandbox_id, sandbox_id);
    assert_eq!(options.path.as_str(), "/dev/zero");
    assert_eq!(
        options.range,
        Some(sinomo_runtime::sandbox::ByteRange {
            start: 2,
            end: Some(6)
        })
    );
    assert_eq!(options.execution, SandboxMetadataExecution::Local);

    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();
    let write = parse_sandbox_command(&[
        "write".to_string(),
        sandbox_id.to_string(),
        "/work/main.rs".to_string(),
        "--file".to_string(),
        "/tmp/main.rs".to_string(),
        "--daemon".to_string(),
    ])
    .expect("write");
    let SandboxCliCommand::Write(options) = write else {
        panic!("expected write");
    };
    assert_eq!(options.sandbox_id, sandbox_id);
    assert_eq!(options.path.as_str(), "/work/main.rs");
    assert_eq!(
        options.input,
        SandboxWriteInput::HostFile(PathBuf::from("/tmp/main.rs"))
    );
    assert_eq!(options.execution, SandboxMetadataExecution::Daemon);

    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();
    let write = parse_sandbox_command(&[
        "write".to_string(),
        sandbox_id.to_string(),
        "/work/message.txt".to_string(),
        "hello".to_string(),
    ])
    .expect("write literal");
    let SandboxCliCommand::Write(options) = write else {
        panic!("expected write");
    };
    assert_eq!(options.input, SandboxWriteInput::Literal(b"hello".to_vec()));
}

#[test]
fn sandbox_fetch_parser_accepts_url_output_and_limits() {
    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();
    let command = parse_sandbox_command(&[
        "fetch".to_string(),
        sandbox_id.to_string(),
        "https://packages.example/archive.tar.gz".to_string(),
        "/work/archive.tar.gz".to_string(),
        "--timeout-ms".to_string(),
        "1234".to_string(),
        "--max-response-bytes".to_string(),
        "4096".to_string(),
        "--header".to_string(),
        "Accept: application/octet-stream".to_string(),
        "--daemon".to_string(),
    ])
    .expect("fetch");

    let SandboxCliCommand::Fetch(options) = command else {
        panic!("expected fetch");
    };
    assert_eq!(options.sandbox_id, sandbox_id);
    assert_eq!(options.url, "https://packages.example/archive.tar.gz");
    assert_eq!(options.output_path.as_str(), "/work/archive.tar.gz");
    assert_eq!(options.timeout_ms, 1234);
    assert_eq!(options.max_response_bytes, 4096);
    assert_eq!(
        options.headers.get("Accept").map(String::as_str),
        Some("application/octet-stream")
    );
    assert_eq!(options.execution, SandboxMetadataExecution::Daemon);
}

#[test]
fn sandbox_lifecycle_parser_accepts_checkpoint_resume_reboot_and_destroy() {
    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();
    let checkpoint = parse_sandbox_command(&["checkpoint".to_string(), sandbox_id.to_string()])
        .expect("checkpoint");
    let SandboxCliCommand::Checkpoint(options) = checkpoint else {
        panic!("expected checkpoint");
    };
    assert_eq!(options.sandbox_id, sandbox_id);
    assert_eq!(options.execution, SandboxMetadataExecution::Local);

    let checkpoint_id = sinomo_runtime::sandbox::SandboxCheckpointId::generate();
    let resume = parse_sandbox_command(&[
        "resume".to_string(),
        sandbox_id.to_string(),
        checkpoint_id.to_string(),
        "--daemon".to_string(),
    ])
    .expect("resume");
    let SandboxCliCommand::Resume(options) = resume else {
        panic!("expected resume");
    };
    assert_eq!(options.sandbox_id, sandbox_id);
    assert_eq!(options.checkpoint_id, checkpoint_id);
    assert_eq!(options.execution, SandboxMetadataExecution::Daemon);

    let reboot = parse_sandbox_command(&[
        "reboot".to_string(),
        sandbox_id.to_string(),
        "--daemon".to_string(),
    ])
    .expect("reboot");
    let SandboxCliCommand::Reboot(options) = reboot else {
        panic!("expected reboot");
    };
    assert_eq!(options.sandbox_id, sandbox_id);
    assert_eq!(options.execution, SandboxMetadataExecution::Daemon);

    let destroy = parse_sandbox_command(&[
        "destroy".to_string(),
        sandbox_id.to_string(),
        "--daemon".to_string(),
    ])
    .expect("destroy");
    let SandboxCliCommand::Destroy(options) = destroy else {
        panic!("expected destroy");
    };
    assert_eq!(options.sandbox_id, sandbox_id);
    assert_eq!(options.execution, SandboxMetadataExecution::Daemon);
}

#[test]
fn sandbox_create_and_status_use_local_store() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-sandbox-store");
    let store = sinomo_runtime::sandbox::LocalSandboxInstanceStore::new(&root);
    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();
    let toolchain_id = sinomo_runtime::sandbox::SandboxToolchainId::generate();

    sandbox_create_with_store(
        SandboxCreateOptions {
            sandbox_id: Some(sandbox_id.clone()),
            toolchain_id: toolchain_id.clone(),
            temp_policy: sinomo_runtime::sandbox::SandboxTempPolicy::RetainAcrossSessions,
            memory_mb: None,
            wall_time_ms: None,
            max_input_bytes: None,
            max_output_bytes: None,
            max_workspace_bytes: None,
            max_written_bytes_per_command: None,
            max_file_count: None,
            mounts: vec![sinomo_runtime::sandbox::SandboxMount {
                source_sifs_path: "/services/sandbox/shared".to_string(),
                mount_path: sinomo_runtime::sandbox::SandboxPath::new("/shared")
                    .expect("mount path"),
                mode: sinomo_runtime::sandbox::SandboxMountMode::ReadOnly,
            }],
            execution: SandboxMetadataExecution::Local,
        },
        &store,
    )
    .expect("create");

    let (manifest, layout) = store.load(&sandbox_id).expect("manifest");
    assert_eq!(manifest.toolchain_id, toolchain_id);
    assert_eq!(
        manifest.temp_policy,
        sinomo_runtime::sandbox::SandboxTempPolicy::RetainAcrossSessions
    );
    assert_eq!(manifest.mounts.len(), 1);
    assert_eq!(
        manifest.mounts[0].source_sifs_path,
        "/services/sandbox/shared"
    );
    assert!(layout.work.is_dir());

    sandbox_status_with_store(
        SandboxStatusOptions {
            sandbox_id: sandbox_id.clone(),
            execution: SandboxMetadataExecution::Local,
        },
        &store,
    )
    .expect("status");
    let (manifest, layout) = store.load(&sandbox_id).expect("load sandbox");
    let mut fs = sinomo_runtime::sandbox::LocalSandboxFileSystem::new(layout);
    fs.create_dir_all(&sinomo_runtime::sandbox::SandboxPath::new("/work/src").expect("src"))
        .expect("create cwd");
    store.save_manifest(&manifest).expect("save manifest");
    sandbox_config_with_store(
        SandboxConfigOptions {
            sandbox_id: sandbox_id.clone(),
            cwd: Some(sinomo_runtime::sandbox::SandboxPath::new("/work/src").expect("config cwd")),
            env_set: BTreeMap::from([("RUST_LOG".to_string(), "debug".to_string())]),
            env_unset: vec!["OLD_VAR".to_string()],
            execution: SandboxMetadataExecution::Local,
        },
        &store,
    )
    .expect("config");
    let (manifest, _) = store.load(&sandbox_id).expect("configured manifest");
    assert_eq!(manifest.cwd.as_str(), "/work/src");
    assert_eq!(
        manifest.env.get("RUST_LOG").map(String::as_str),
        Some("debug")
    );
    let missing_cwd = sandbox_config_with_store(
        SandboxConfigOptions {
            sandbox_id: sandbox_id.clone(),
            cwd: Some(
                sinomo_runtime::sandbox::SandboxPath::new("/work/missing").expect("missing cwd"),
            ),
            env_set: BTreeMap::new(),
            env_unset: Vec::new(),
            execution: SandboxMetadataExecution::Local,
        },
        &store,
    )
    .expect_err("missing cwd rejected");
    assert!(format!("{missing_cwd:?}").contains("not found"));
    let invalid_env = sandbox_config_with_store(
        SandboxConfigOptions {
            sandbox_id: sandbox_id.clone(),
            cwd: None,
            env_set: BTreeMap::from([("BAD-NAME".to_string(), "value".to_string())]),
            env_unset: Vec::new(),
            execution: SandboxMetadataExecution::Local,
        },
        &store,
    )
    .expect_err("invalid env rejected");
    assert!(format!("{invalid_env:?}").contains("invalid sandbox env name `BAD-NAME`"));
    let invalid_env_value = sandbox_config_with_store(
        SandboxConfigOptions {
            sandbox_id: sandbox_id.clone(),
            cwd: None,
            env_set: BTreeMap::from([("RUST_LOG".to_string(), "bad\0value".to_string())]),
            env_unset: Vec::new(),
            execution: SandboxMetadataExecution::Local,
        },
        &store,
    )
    .expect_err("invalid env value rejected");
    assert!(format!("{invalid_env_value:?}").contains("embedded NUL byte"));
}

#[test]
fn sandbox_doctor_reports_missing_toolchain_and_policy_denial() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-sandbox-doctor");
    let store = sinomo_runtime::sandbox::LocalSandboxInstanceStore::new(root.join("runtime"));
    let toolchain_store =
        sinomo_runtime::sandbox::LocalSandboxToolchainStore::new(root.join("cache"));
    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();
    sandbox_create_with_store(
        SandboxCreateOptions {
            sandbox_id: Some(sandbox_id.clone()),
            toolchain_id: sinomo_runtime::sandbox::SandboxToolchainId::generate(),
            temp_policy: sinomo_runtime::sandbox::SandboxTempPolicy::ClearOnSessionStart,
            memory_mb: None,
            wall_time_ms: None,
            max_input_bytes: None,
            max_output_bytes: None,
            max_workspace_bytes: None,
            max_written_bytes_per_command: None,
            max_file_count: None,
            mounts: vec![sinomo_runtime::sandbox::SandboxMount {
                source_sifs_path: "/users/shared".to_string(),
                mount_path: sinomo_runtime::sandbox::SandboxPath::new("/shared")
                    .expect("mount path"),
                mode: sinomo_runtime::sandbox::SandboxMountMode::ReadWrite,
            }],
            execution: SandboxMetadataExecution::Local,
        },
        &store,
    )
    .expect("create");

    let findings = sandbox_doctor_findings_with_stores(
        SandboxDoctorOptions {
            sandbox_id: Some(sandbox_id),
        },
        &store,
        &toolchain_store,
        &sinomo_runtime::sandbox::SandboxPolicy::default(),
    )
    .expect("doctor");
    assert!(findings.iter().any(|finding| {
        finding.severity == SandboxDoctorSeverity::Warning
            && finding.code == "busybox-toolchain-missing"
    }));
    assert!(findings.iter().any(|finding| {
        finding.severity == SandboxDoctorSeverity::Error
            && finding.code == "sandbox-toolchain-missing"
    }));
    assert!(findings.iter().any(|finding| {
        finding.severity == SandboxDoctorSeverity::Error && finding.code == "policy-denied"
    }));
}

#[test]
fn sandbox_doctor_warns_when_policy_allows_wasix_unpinned_child_exec() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-sandbox-doctor-policy-unpinned");
    let store = sinomo_runtime::sandbox::LocalSandboxInstanceStore::new(root.join("runtime"));
    let toolchain_store =
        sinomo_runtime::sandbox::LocalSandboxToolchainStore::new(root.join("cache"));
    let mut policy = sinomo_runtime::sandbox::SandboxPolicy::default();
    policy
        .allowed_command_capabilities
        .insert(sinomo_runtime::sandbox::SANDBOX_WASIX_UNPINNED_CHILD_EXEC_CAPABILITY.to_string());

    let findings = sandbox_doctor_findings_with_stores(
        SandboxDoctorOptions { sandbox_id: None },
        &store,
        &toolchain_store,
        &policy,
    )
    .expect("doctor");

    assert!(
        findings.iter().any(|finding| {
            finding.code == "policy-allows-wasix-unpinned-child-exec"
                && finding.severity == SandboxDoctorSeverity::Warning
                && finding
                    .detail
                    .contains(sinomo_runtime::sandbox::SANDBOX_WASIX_UNPINNED_CHILD_EXEC_CAPABILITY)
                && finding.detail.contains("writable mounts")
        }),
        "doctor should report when policy allows the temporary WASIX child-exec escape hatch"
    );
}

#[test]
fn sandbox_doctor_reports_recent_resource_exhaustion_for_target_sandbox() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-sandbox-doctor-resource");
    let store = sinomo_runtime::sandbox::LocalSandboxInstanceStore::new(root.join("runtime"));
    let toolchain_store =
        sinomo_runtime::sandbox::LocalSandboxToolchainStore::new(root.join("cache"));
    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();
    let other_sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();
    sandbox_create_with_store(
        SandboxCreateOptions {
            sandbox_id: Some(sandbox_id.clone()),
            toolchain_id: sinomo_runtime::sandbox::SandboxToolchainId::generate(),
            temp_policy: sinomo_runtime::sandbox::SandboxTempPolicy::ClearOnSessionStart,
            memory_mb: None,
            wall_time_ms: None,
            max_input_bytes: None,
            max_output_bytes: None,
            max_workspace_bytes: None,
            max_written_bytes_per_command: None,
            max_file_count: None,
            mounts: Vec::new(),
            execution: SandboxMetadataExecution::Local,
        },
        &store,
    )
    .expect("create");
    let node_id = NodeId::from_legacy_named("sandbox_doctor_resource_node").expect("node");
    let mut event_store = InMemoryEventStore::new();
    event_store
        .append(sinomo_runtime::sandbox::sandbox_resource_exhausted_event(
            node_id.clone(),
            &other_sandbox_id,
            "sandbox-write",
            "file-count",
            12,
            8,
        ))
        .expect("append other resource event");
    event_store
        .append(sinomo_runtime::sandbox::sandbox_resource_exhausted_event(
            node_id,
            &sandbox_id,
            "sandbox-write",
            "written-bytes-per-command",
            13,
            8,
        ))
        .expect("append target resource event");
    event_store
        .append(sinomo_runtime::sandbox::sandbox_policy_denied_event(
            NodeId::from_legacy_named("sandbox_doctor_policy_node").expect("node"),
            Some(&sandbox_id),
            "sandbox-run",
            &sinomo_runtime::sandbox::SandboxPolicyDenial::new(
                "command-capability-denied",
                "command wget requires network",
            ),
        ))
        .expect("append target policy event");
    event_store
        .append(sinomo_runtime::sandbox::sandbox_policy_denied_event(
            NodeId::from_legacy_named("sandbox_doctor_toolchain_policy_node").expect("node"),
            None,
            "sandbox-toolchain-import-package",
            &sinomo_runtime::sandbox::SandboxPolicyDenial::new(
                "toolchain-provenance-denied",
                "toolchain provenance is not allowed",
            ),
        ))
        .expect("append global policy event");
    let events = event_store
        .replay(ReplayQuery::by_kind_prefix("sandbox."))
        .expect("replay sandbox events");

    let findings = sandbox_doctor_findings_with_stores_and_events(
        SandboxDoctorOptions {
            sandbox_id: Some(sandbox_id),
        },
        &store,
        &toolchain_store,
        &sinomo_runtime::sandbox::SandboxPolicy::default(),
        &events,
    )
    .expect("doctor");
    let resource_findings: Vec<_> = findings
        .iter()
        .filter(|finding| finding.code == "recent-resource-exhausted")
        .collect();
    assert_eq!(resource_findings.len(), 1);
    assert!(
        resource_findings[0]
            .detail
            .contains("written-bytes-per-command")
    );
    assert!(!resource_findings[0].detail.contains("file-count"));
    let policy_findings: Vec<_> = findings
        .iter()
        .filter(|finding| finding.code == "recent-policy-denied")
        .collect();
    assert_eq!(policy_findings.len(), 1);
    assert!(
        policy_findings[0]
            .detail
            .contains("command-capability-denied")
    );
    assert!(
        !policy_findings[0]
            .detail
            .contains("toolchain-provenance-denied")
    );

    let global_findings = sandbox_doctor_findings_with_stores_and_events(
        SandboxDoctorOptions { sandbox_id: None },
        &store,
        &toolchain_store,
        &sinomo_runtime::sandbox::SandboxPolicy::default(),
        &events,
    )
    .expect("global doctor");
    assert_eq!(
        global_findings
            .iter()
            .filter(|finding| finding.code == "recent-resource-exhausted")
            .count(),
        2
    );
    assert_eq!(
        global_findings
            .iter()
            .filter(|finding| finding.code == "recent-policy-denied")
            .count(),
        2
    );
    assert!(global_findings.iter().any(|finding| {
        finding.code == "recent-policy-denied"
            && finding.detail.contains("cluster sandbox service")
            && finding.detail.contains("toolchain-provenance-denied")
    }));
    assert!(
        global_findings
            .iter()
            .filter(|finding| finding.code == "recent-policy-denied")
            .all(|finding| !finding.detail.contains("sandbox unknown"))
    );
}
#[test]
fn sandbox_doctor_warns_when_wasix_shell_uses_unpinned_child_exec_acknowledgement() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-sandbox-doctor-wasix-unpinned");
    let store = sinomo_runtime::sandbox::LocalSandboxInstanceStore::new(root.join("runtime"));
    let toolchain_store =
        sinomo_runtime::sandbox::LocalSandboxToolchainStore::new(root.join("cache"));
    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();
    let toolchain_id = sinomo_runtime::sandbox::SandboxToolchainId::generate();
    let mut toolchain = sinomo_runtime::sandbox::SandboxToolchainManifest::new(
        toolchain_id.clone(),
        "wasix-shell-test",
        sinomo_runtime::sandbox::SandboxWasiAbi::Wasix,
        "unit test",
        OffsetDateTime::now_utc(),
    )
    .expect("toolchain");
    toolchain_store
        .put_command_module_with_capabilities(
            &mut toolchain,
            "sh",
            wasi_stdin_to_stdout_module().as_bytes(),
            sinomo_runtime::sandbox::SandboxResourceLimits::default(),
            vec![
                sinomo_runtime::sandbox::SANDBOX_CHILD_PROCESS_EXEC_CAPABILITY.to_string(),
                sinomo_runtime::sandbox::SANDBOX_WASIX_UNPINNED_CHILD_EXEC_CAPABILITY.to_string(),
            ],
        )
        .expect("sh");
    sandbox_create_with_store(
        SandboxCreateOptions {
            sandbox_id: Some(sandbox_id.clone()),
            toolchain_id,
            temp_policy: sinomo_runtime::sandbox::SandboxTempPolicy::ClearOnSessionStart,
            memory_mb: None,
            wall_time_ms: None,
            max_input_bytes: None,
            max_output_bytes: None,
            max_workspace_bytes: None,
            max_written_bytes_per_command: None,
            max_file_count: None,
            mounts: Vec::new(),
            execution: SandboxMetadataExecution::Local,
        },
        &store,
    )
    .expect("create");

    let findings = sandbox_doctor_findings_with_stores(
        SandboxDoctorOptions {
            sandbox_id: Some(sandbox_id),
        },
        &store,
        &toolchain_store,
        &sinomo_runtime::sandbox::SandboxPolicy::default(),
    )
    .expect("doctor");

    assert!(
        findings.iter().any(|finding| {
            finding.code == "wasix-unpinned-child-exec-acknowledged"
                && finding.severity == SandboxDoctorSeverity::Warning
                && finding
                    .detail
                    .contains(sinomo_runtime::sandbox::SANDBOX_WASIX_UNPINNED_CHILD_EXEC_CAPABILITY)
                && finding.detail.contains("writable mounts")
        }),
        "doctor should identify sandboxes whose WASIX shell relies on the temporary child-exec escape hatch"
    );
}

#[test]
fn sandbox_ls_cat_and_write_use_local_filesystem() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-sandbox-files");
    let store = sinomo_runtime::sandbox::LocalSandboxInstanceStore::new(&root);
    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();

    sandbox_create_with_store(
        SandboxCreateOptions {
            sandbox_id: Some(sandbox_id.clone()),
            toolchain_id: sinomo_runtime::sandbox::SandboxToolchainId::generate(),
            temp_policy: sinomo_runtime::sandbox::SandboxTempPolicy::ClearOnSessionStart,
            memory_mb: None,
            wall_time_ms: None,
            max_input_bytes: None,
            max_output_bytes: None,
            max_workspace_bytes: None,
            max_written_bytes_per_command: None,
            max_file_count: None,
            mounts: Vec::new(),
            execution: SandboxMetadataExecution::Local,
        },
        &store,
    )
    .expect("create");
    let mut event_store =
        sinomo_runtime::events::JsonlEventStore::new(root.join("events/event-log.jsonl"));
    sandbox_write_with_store_and_event_store(
        SandboxWriteOptions {
            sandbox_id: sandbox_id.clone(),
            path: sinomo_runtime::sandbox::SandboxPath::new("/work/main.rs").expect("file"),
            input: SandboxWriteInput::Literal(b"fn main() {}\n".to_vec()),
            execution: SandboxMetadataExecution::Local,
        },
        b"fn main() {}\n".to_vec(),
        &store,
        &mut event_store,
    )
    .expect("write");
    let file_events = event_store
        .replay(ReplayQuery::by_kind_prefix("sandbox.file"))
        .expect("file events");
    assert_eq!(file_events.len(), 1);
    assert_eq!(file_events[0].event.kind.as_str(), "sandbox.file.written");
    assert_eq!(
        file_events[0].event.payload["sandbox_id"],
        sandbox_id.to_string()
    );
    assert_eq!(
        file_events[0].event.payload["workspace_root"],
        sinomo_runtime::sandbox::sandbox_instance_root(&sandbox_id)
    );
    assert_eq!(file_events[0].event.payload["phase"], "sandbox-write");
    assert_eq!(file_events[0].event.payload["path"], "/work/main.rs");
    assert_eq!(file_events[0].event.payload["bytes_written"], 13);

    sandbox_ls_with_store(
        SandboxLsOptions {
            sandbox_id: sandbox_id.clone(),
            path: sinomo_runtime::sandbox::SandboxPath::new("/work").expect("work"),
            execution: SandboxMetadataExecution::Local,
        },
        &store,
    )
    .expect("ls");
    sandbox_cat_with_store(
        SandboxCatOptions {
            sandbox_id: sandbox_id.clone(),
            path: sinomo_runtime::sandbox::SandboxPath::new("/work/main.rs").expect("file"),
            range: None,
            execution: SandboxMetadataExecution::Local,
        },
        &store,
    )
    .expect("cat");
    let (_manifest, layout) = store.load(&sandbox_id).expect("load");
    let fs = sinomo_runtime::sandbox::LocalSandboxFileSystem::new(layout);
    assert_eq!(
        fs.read(
            &sinomo_runtime::sandbox::SandboxPath::new("/work/main.rs").expect("file"),
            None,
        )
        .expect("read"),
        b"fn main() {}\n"
    );
    let (mut manifest, _layout) = store.load(&sandbox_id).expect("load manifest");
    manifest.limits.max_written_bytes_per_command = 8;
    manifest.limits.max_workspace_bytes = 16;
    store
        .save_manifest(&manifest)
        .expect("save limited manifest");
    sandbox_ls_with_store(
        SandboxLsOptions {
            sandbox_id: sandbox_id.clone(),
            path: sinomo_runtime::sandbox::SandboxPath::new("/dev").expect("dev"),
            execution: SandboxMetadataExecution::Local,
        },
        &store,
    )
    .expect("ls dev");
    sandbox_cat_with_store(
        SandboxCatOptions {
            sandbox_id: sandbox_id.clone(),
            path: sinomo_runtime::sandbox::SandboxPath::new("/dev/null").expect("null"),
            range: None,
            execution: SandboxMetadataExecution::Local,
        },
        &store,
    )
    .expect("cat null");
    sandbox_cat_with_store(
        SandboxCatOptions {
            sandbox_id: sandbox_id.clone(),
            path: sinomo_runtime::sandbox::SandboxPath::new("/dev/zero").expect("zero"),
            range: Some(sinomo_runtime::sandbox::ByteRange {
                start: 0,
                end: Some(4),
            }),
            execution: SandboxMetadataExecution::Local,
        },
        &store,
    )
    .expect("cat bounded zero");
    sandbox_write_with_store(
        SandboxWriteOptions {
            sandbox_id: sandbox_id.clone(),
            path: sinomo_runtime::sandbox::SandboxPath::new("/dev/null").expect("null"),
            input: SandboxWriteInput::Literal(b"discarded by device".to_vec()),
            execution: SandboxMetadataExecution::Local,
        },
        b"discarded by device".to_vec(),
        &store,
    )
    .expect("write null ignores workspace quota");
    let _readonly_error = sandbox_write_with_store_and_event_store(
        SandboxWriteOptions {
            sandbox_id: sandbox_id.clone(),
            path: sinomo_runtime::sandbox::SandboxPath::new("/dev").expect("dev"),
            input: SandboxWriteInput::Literal(b"no".to_vec()),
            execution: SandboxMetadataExecution::Local,
        },
        b"no".to_vec(),
        &store,
        &mut event_store,
    )
    .expect_err("read-only device write should be denied");
    let policy_events = event_store
        .replay(ReplayQuery::by_kind_prefix("sandbox.policy"))
        .expect("policy events");
    assert_eq!(policy_events.len(), 1);
    assert_eq!(
        policy_events[0].event.kind.as_str(),
        "sandbox.policy.denied"
    );
    assert_eq!(
        policy_events[0].event.payload["workspace_root"],
        sinomo_runtime::sandbox::sandbox_instance_root(&sandbox_id)
    );
    assert_eq!(
        policy_events[0].event.payload["code"],
        "mount-read-only-denied"
    );
    assert_eq!(policy_events[0].event.payload["phase"], "sandbox-write");
    let mount_events = event_store
        .replay(ReplayQuery::by_kind_prefix("sandbox.mount"))
        .expect("mount events");
    assert_eq!(mount_events.len(), 1);
    assert_eq!(mount_events[0].event.kind.as_str(), "sandbox.mount.denied");
    assert_eq!(
        mount_events[0].event.payload["workspace_root"],
        sinomo_runtime::sandbox::sandbox_instance_root(&sandbox_id)
    );
    assert_eq!(
        mount_events[0].event.payload["code"],
        "mount-read-only-denied"
    );
    assert_eq!(mount_events[0].event.payload["phase"], "sandbox-write");
    let error = sandbox_write_with_store_and_event_store(
        SandboxWriteOptions {
            sandbox_id: sandbox_id.clone(),
            path: sinomo_runtime::sandbox::SandboxPath::new("/work/main.rs").expect("file"),
            input: SandboxWriteInput::Literal(b"too much content\n".to_vec()),
            execution: SandboxMetadataExecution::Local,
        },
        b"too much content\n".to_vec(),
        &store,
        &mut event_store,
    )
    .expect_err("quota should reject write before mutating file");
    assert!(format!("{error:?}").contains("written-bytes-per-command"));
    let resource_events = event_store
        .replay(ReplayQuery::by_kind_prefix("sandbox.resource"))
        .expect("resource events");
    assert_eq!(resource_events.len(), 1);
    assert_eq!(
        resource_events[0].event.kind.as_str(),
        "sandbox.resource.exhausted"
    );
    assert_eq!(
        resource_events[0].event.payload["workspace_root"],
        sinomo_runtime::sandbox::sandbox_instance_root(&sandbox_id)
    );
    assert_eq!(resource_events[0].event.payload["phase"], "sandbox-write");
    assert_eq!(
        resource_events[0].event.payload["kind"],
        "written-bytes-per-command"
    );
    let (_manifest, layout) = store.load(&sandbox_id).expect("reload");
    let fs = sinomo_runtime::sandbox::LocalSandboxFileSystem::new(layout);
    assert_eq!(
        fs.read(
            &sinomo_runtime::sandbox::SandboxPath::new("/work/main.rs").expect("file"),
            None,
        )
        .expect("read after rejected write"),
        b"fn main() {}\n"
    );
}

#[test]
fn sandbox_fetch_uses_mediated_transport_and_persists_output_and_audit() {
    struct FakeTransport;

    impl sinomo_runtime::sandbox::SandboxHttpTransport for FakeTransport {
        fn fetch(
            &self,
            request: &sinomo_runtime::sandbox::SandboxHttpRequest,
        ) -> Result<
            sinomo_runtime::sandbox::SandboxHttpResponse,
            sinomo_runtime::sandbox::SandboxNetworkError,
        > {
            assert_eq!(request.url, "https://packages.example/archive.tar.gz");
            assert_eq!(request.timeout_ms, 1234);
            Ok(sinomo_runtime::sandbox::SandboxHttpResponse {
                status: 200,
                headers: Default::default(),
                body: b"archive".to_vec(),
            })
        }
    }

    let root = sinomo_testdir::TestDir::new("sinomo-cli-sandbox-fetch");
    let store = sinomo_runtime::sandbox::LocalSandboxInstanceStore::new(&root);
    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();

    sandbox_create_with_store(
        SandboxCreateOptions {
            sandbox_id: Some(sandbox_id.clone()),
            toolchain_id: sinomo_runtime::sandbox::SandboxToolchainId::generate(),
            temp_policy: sinomo_runtime::sandbox::SandboxTempPolicy::ClearOnSessionStart,
            memory_mb: None,
            wall_time_ms: None,
            max_input_bytes: None,
            max_output_bytes: None,
            max_workspace_bytes: None,
            max_written_bytes_per_command: None,
            max_file_count: None,
            mounts: Vec::new(),
            execution: SandboxMetadataExecution::Local,
        },
        &store,
    )
    .expect("create");
    let mut policy = sinomo_runtime::sandbox::SandboxPolicy::default();
    policy
        .allowed_http_hosts
        .insert("packages.example".to_string());
    policy
        .allowed_command_capabilities
        .insert(sinomo_runtime::sandbox::SANDBOX_NETWORK_HTTP_CAPABILITY.to_string());
    let output_path =
        sinomo_runtime::sandbox::SandboxPath::new("/work/archive.tar.gz").expect("output path");
    let mut event_store =
        sinomo_runtime::events::JsonlEventStore::new(root.join("events/event-log.jsonl"));

    let result = sandbox_fetch_with_store_transport_and_event_store(
        SandboxFetchOptions {
            sandbox_id: sandbox_id.clone(),
            url: "https://packages.example/archive.tar.gz".to_string(),
            output_path: output_path.clone(),
            timeout_ms: 1234,
            max_response_bytes: 4096,
            headers: Default::default(),
            execution: SandboxMetadataExecution::Local,
        },
        &store,
        &policy,
        &FakeTransport,
        &mut event_store,
    )
    .expect("fetch");

    let (_manifest, layout) = store.load(&sandbox_id).expect("load");
    let mut fs = sinomo_runtime::sandbox::LocalSandboxFileSystem::new(layout);
    assert_eq!(fs.read(&output_path, None).expect("output"), b"archive");
    assert_eq!(
        sinomo_runtime::sandbox::load_http_exchange_record(
            &mut fs,
            &result.record.command_id,
            &result.request_id
        )
        .expect("audit"),
        result.record
    );
    let events = event_store
        .replay(ReplayQuery::by_kind_prefix("sandbox.network"))
        .expect("replay sandbox network events");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event.kind.as_str(), "sandbox.network.http");
    assert_eq!(
        events[0].event.payload["sandbox_id"],
        sandbox_id.to_string()
    );
    assert_eq!(
        events[0].event.payload["workspace_root"],
        sinomo_runtime::sandbox::sandbox_instance_root(&sandbox_id)
    );
    assert_eq!(
        events[0].event.payload["request_id"],
        result.request_id.to_string()
    );
    let file_events = event_store
        .replay(ReplayQuery::by_kind_prefix("sandbox.file"))
        .expect("replay sandbox file events");
    assert_eq!(file_events.len(), 1);
    assert_eq!(file_events[0].event.kind.as_str(), "sandbox.file.written");
    assert_eq!(
        file_events[0].event.payload["sandbox_id"],
        sandbox_id.to_string()
    );
    assert_eq!(
        file_events[0].event.payload["workspace_root"],
        sinomo_runtime::sandbox::sandbox_instance_root(&sandbox_id)
    );
    assert_eq!(
        file_events[0].event.payload["path"],
        output_path.to_string()
    );
    assert_eq!(file_events[0].event.payload["bytes_written"], 7);
    assert_eq!(file_events[0].event.payload["phase"], "sandbox-fetch");
    assert_eq!(
        file_events[0].event.payload["command_id"],
        result.record.command_id.to_string()
    );
    assert_eq!(
        file_events[0].event.payload["request_id"],
        result.request_id.to_string()
    );
}

#[test]
fn sandbox_fetch_records_resource_exhaustion_when_output_write_exceeds_quota() {
    struct FakeTransport;

    impl sinomo_runtime::sandbox::SandboxHttpTransport for FakeTransport {
        fn fetch(
            &self,
            _request: &sinomo_runtime::sandbox::SandboxHttpRequest,
        ) -> Result<
            sinomo_runtime::sandbox::SandboxHttpResponse,
            sinomo_runtime::sandbox::SandboxNetworkError,
        > {
            Ok(sinomo_runtime::sandbox::SandboxHttpResponse {
                status: 200,
                headers: Default::default(),
                body: b"archive".to_vec(),
            })
        }
    }

    let root = sinomo_testdir::TestDir::new("sinomo-cli-sandbox-fetch-quota");
    let store = sinomo_runtime::sandbox::LocalSandboxInstanceStore::new(&root);
    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();

    sandbox_create_with_store(
        SandboxCreateOptions {
            sandbox_id: Some(sandbox_id.clone()),
            toolchain_id: sinomo_runtime::sandbox::SandboxToolchainId::generate(),
            temp_policy: sinomo_runtime::sandbox::SandboxTempPolicy::ClearOnSessionStart,
            memory_mb: None,
            wall_time_ms: None,
            max_input_bytes: None,
            max_output_bytes: None,
            max_workspace_bytes: None,
            max_written_bytes_per_command: None,
            max_file_count: None,
            mounts: Vec::new(),
            execution: SandboxMetadataExecution::Local,
        },
        &store,
    )
    .expect("create");
    let (mut manifest, _) = store.load(&sandbox_id).expect("load manifest");
    manifest.limits.max_written_bytes_per_command = 4;
    store.save_manifest(&manifest).expect("save quota");

    let mut policy = sinomo_runtime::sandbox::SandboxPolicy::default();
    policy
        .allowed_http_hosts
        .insert("packages.example".to_string());
    policy
        .allowed_command_capabilities
        .insert(sinomo_runtime::sandbox::SANDBOX_NETWORK_HTTP_CAPABILITY.to_string());
    let output_path =
        sinomo_runtime::sandbox::SandboxPath::new("/work/archive.tar.gz").expect("output path");
    let mut event_store =
        sinomo_runtime::events::JsonlEventStore::new(root.join("events/event-log.jsonl"));

    let error = sandbox_fetch_with_store_transport_and_event_store(
        SandboxFetchOptions {
            sandbox_id: sandbox_id.clone(),
            url: "https://packages.example/archive.tar.gz".to_string(),
            output_path: output_path.clone(),
            timeout_ms: 1234,
            max_response_bytes: 4096,
            headers: Default::default(),
            execution: SandboxMetadataExecution::Local,
        },
        &store,
        &policy,
        &FakeTransport,
        &mut event_store,
    )
    .expect_err("fetch body should exceed write quota");
    assert!(format!("{error:?}").contains("written-bytes-per-command"));

    let (_manifest, layout) = store.load(&sandbox_id).expect("load");
    let fs = sinomo_runtime::sandbox::LocalSandboxFileSystem::new(layout);
    assert!(matches!(
        fs.read(&output_path, None),
        Err(sinomo_runtime::sandbox::SandboxFsError::NotFound { .. })
    ));

    let resource_events = event_store
        .replay(ReplayQuery::by_kind_prefix("sandbox.resource"))
        .expect("resource events");
    assert_eq!(resource_events.len(), 1);
    assert_eq!(
        resource_events[0].event.kind.as_str(),
        "sandbox.resource.exhausted"
    );
    assert_eq!(
        resource_events[0].event.payload["sandbox_id"],
        sandbox_id.to_string()
    );
    assert_eq!(
        resource_events[0].event.payload["workspace_root"],
        sinomo_runtime::sandbox::sandbox_instance_root(&sandbox_id)
    );
    assert_eq!(resource_events[0].event.payload["phase"], "sandbox-fetch");
    assert_eq!(
        resource_events[0].event.payload["kind"],
        "written-bytes-per-command"
    );
    assert_eq!(resource_events[0].event.payload["actual"], 7);
    assert_eq!(resource_events[0].event.payload["limit"], 4);

    let network_events = event_store
        .replay(ReplayQuery::by_kind_prefix("sandbox.network"))
        .expect("network events");
    assert_eq!(
        network_events.len(),
        1,
        "quota-denied body writes should still keep the HTTP audit event"
    );
    assert_eq!(
        network_events[0].event.payload["workspace_root"],
        sinomo_runtime::sandbox::sandbox_instance_root(&sandbox_id)
    );
}

#[test]
fn sandbox_fetch_audits_failed_post_policy_transport_attempt() {
    struct FailingTransport;

    impl sinomo_runtime::sandbox::SandboxHttpTransport for FailingTransport {
        fn fetch(
            &self,
            request: &sinomo_runtime::sandbox::SandboxHttpRequest,
        ) -> Result<
            sinomo_runtime::sandbox::SandboxHttpResponse,
            sinomo_runtime::sandbox::SandboxNetworkError,
        > {
            assert_eq!(request.url, "https://packages.example/archive.tar.gz");
            Err(sinomo_runtime::sandbox::SandboxNetworkError::Transport {
                detail: "offline".to_string(),
            })
        }
    }

    let root = sinomo_testdir::TestDir::new("sinomo-cli-sandbox-fetch-failed");
    let store = sinomo_runtime::sandbox::LocalSandboxInstanceStore::new(&root);
    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();

    sandbox_create_with_store(
        SandboxCreateOptions {
            sandbox_id: Some(sandbox_id.clone()),
            toolchain_id: sinomo_runtime::sandbox::SandboxToolchainId::generate(),
            temp_policy: sinomo_runtime::sandbox::SandboxTempPolicy::ClearOnSessionStart,
            memory_mb: None,
            wall_time_ms: None,
            max_input_bytes: None,
            max_output_bytes: None,
            max_workspace_bytes: None,
            max_written_bytes_per_command: None,
            max_file_count: None,
            mounts: Vec::new(),
            execution: SandboxMetadataExecution::Local,
        },
        &store,
    )
    .expect("create");
    let mut policy = sinomo_runtime::sandbox::SandboxPolicy::default();
    policy
        .allowed_http_hosts
        .insert("packages.example".to_string());
    policy
        .allowed_command_capabilities
        .insert(sinomo_runtime::sandbox::SANDBOX_NETWORK_HTTP_CAPABILITY.to_string());
    let mut event_store =
        sinomo_runtime::events::JsonlEventStore::new(root.join("events/event-log.jsonl"));

    let error = sandbox_fetch_with_store_transport_and_event_store(
        SandboxFetchOptions {
            sandbox_id: sandbox_id.clone(),
            url: "https://packages.example/archive.tar.gz".to_string(),
            output_path: sinomo_runtime::sandbox::SandboxPath::new("/work/archive.tar.gz")
                .expect("output path"),
            timeout_ms: 1234,
            max_response_bytes: 4096,
            headers: Default::default(),
            execution: SandboxMetadataExecution::Local,
        },
        &store,
        &policy,
        &FailingTransport,
        &mut event_store,
    )
    .expect_err("transport failure");
    assert!(format!("{error:?}").contains("offline"));

    let events = event_store
        .replay(ReplayQuery::by_kind_prefix("sandbox.network"))
        .expect("replay sandbox network events");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event.kind.as_str(), "sandbox.network.http");
    assert_eq!(
        events[0].event.payload["sandbox_id"],
        sandbox_id.to_string()
    );
    assert_eq!(
        events[0].event.payload["workspace_root"],
        sinomo_runtime::sandbox::sandbox_instance_root(&sandbox_id)
    );
    assert!(events[0].event.payload["status"].is_null());
    assert_eq!(events[0].event.payload["response_bytes"], 0);
}

#[test]
fn sandbox_fetch_audits_policy_denial() {
    struct UnusedTransport;

    impl sinomo_runtime::sandbox::SandboxHttpTransport for UnusedTransport {
        fn fetch(
            &self,
            _request: &sinomo_runtime::sandbox::SandboxHttpRequest,
        ) -> Result<
            sinomo_runtime::sandbox::SandboxHttpResponse,
            sinomo_runtime::sandbox::SandboxNetworkError,
        > {
            panic!("policy denial should happen before transport")
        }
    }

    let root = sinomo_testdir::TestDir::new("sinomo-cli-sandbox-fetch-policy-denied");
    let store = sinomo_runtime::sandbox::LocalSandboxInstanceStore::new(&root);
    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();

    sandbox_create_with_store(
        SandboxCreateOptions {
            sandbox_id: Some(sandbox_id.clone()),
            toolchain_id: sinomo_runtime::sandbox::SandboxToolchainId::generate(),
            temp_policy: sinomo_runtime::sandbox::SandboxTempPolicy::ClearOnSessionStart,
            memory_mb: None,
            wall_time_ms: None,
            max_input_bytes: None,
            max_output_bytes: None,
            max_workspace_bytes: None,
            max_written_bytes_per_command: None,
            max_file_count: None,
            mounts: Vec::new(),
            execution: SandboxMetadataExecution::Local,
        },
        &store,
    )
    .expect("create");
    let mut policy = sinomo_runtime::sandbox::SandboxPolicy::default();
    policy
        .allowed_command_capabilities
        .insert(sinomo_runtime::sandbox::SANDBOX_NETWORK_HTTP_CAPABILITY.to_string());
    let mut event_store =
        sinomo_runtime::events::JsonlEventStore::new(root.join("events/event-log.jsonl"));

    let error = sandbox_fetch_with_store_transport_and_event_store(
        SandboxFetchOptions {
            sandbox_id: sandbox_id.clone(),
            url: "https://packages.example/archive.tar.gz".to_string(),
            output_path: sinomo_runtime::sandbox::SandboxPath::new("/work/archive.tar.gz")
                .expect("output path"),
            timeout_ms: 1234,
            max_response_bytes: 4096,
            headers: Default::default(),
            execution: SandboxMetadataExecution::Local,
        },
        &store,
        &policy,
        &UnusedTransport,
        &mut event_store,
    )
    .expect_err("host should be denied");
    assert!(format!("{error:?}").contains("network-host-denied"));

    let events = event_store
        .replay(ReplayQuery::by_kind_prefix("sandbox.policy"))
        .expect("replay sandbox policy events");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event.kind.as_str(), "sandbox.policy.denied");
    assert_eq!(
        events[0].event.payload["sandbox_id"],
        sandbox_id.to_string()
    );
    assert_eq!(events[0].event.payload["phase"], "sandbox-fetch");
    assert_eq!(events[0].event.payload["code"], "network-host-denied");
}

#[test]
fn sandbox_checkpoint_resume_and_destroy_use_local_store() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-sandbox-lifecycle");
    let store = sinomo_runtime::sandbox::LocalSandboxInstanceStore::new(&root);
    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();

    sandbox_create_with_store(
        SandboxCreateOptions {
            sandbox_id: Some(sandbox_id.clone()),
            toolchain_id: sinomo_runtime::sandbox::SandboxToolchainId::generate(),
            temp_policy: sinomo_runtime::sandbox::SandboxTempPolicy::ClearOnSessionStart,
            memory_mb: None,
            wall_time_ms: None,
            max_input_bytes: None,
            max_output_bytes: None,
            max_workspace_bytes: None,
            max_written_bytes_per_command: None,
            max_file_count: None,
            mounts: Vec::new(),
            execution: SandboxMetadataExecution::Local,
        },
        &store,
    )
    .expect("create");
    let (_manifest, layout) = store.load(&sandbox_id).expect("load");
    let mut fs = sinomo_runtime::sandbox::LocalSandboxFileSystem::new(layout.clone());
    let path = sinomo_runtime::sandbox::SandboxPath::new("/work/main.rs").expect("path");
    fs.write(&path, b"before").expect("write");

    sandbox_checkpoint_with_store(
        SandboxCheckpointOptions {
            sandbox_id: sandbox_id.clone(),
            execution: SandboxMetadataExecution::Local,
        },
        &store,
    )
    .expect("checkpoint");
    let checkpoint_id = std::fs::read_dir(&layout.checkpoints)
        .expect("checkpoints")
        .next()
        .expect("checkpoint entry")
        .expect("checkpoint dir")
        .file_name()
        .to_string_lossy()
        .to_string();
    let checkpoint_id =
        sinomo_runtime::sandbox::SandboxCheckpointId::new(checkpoint_id).expect("checkpoint id");

    fs.write(&path, b"after").expect("overwrite");
    sandbox_resume_with_store(
        SandboxResumeOptions {
            sandbox_id: sandbox_id.clone(),
            checkpoint_id,
            execution: SandboxMetadataExecution::Local,
        },
        &store,
    )
    .expect("resume");
    let (_manifest, layout) = store.load(&sandbox_id).expect("reload");
    let fs = sinomo_runtime::sandbox::LocalSandboxFileSystem::new(layout.clone());
    assert_eq!(fs.read(&path, None).expect("read"), b"before");

    std::fs::write(layout.tmp.join("session-cache.txt"), b"tmp").expect("write tmp");
    sandbox_reboot_with_store(
        SandboxRebootOptions {
            sandbox_id: sandbox_id.clone(),
            execution: SandboxMetadataExecution::Local,
        },
        &store,
    )
    .expect("reboot");
    assert!(!layout.tmp.join("session-cache.txt").exists());

    sandbox_destroy_with_store(
        SandboxDestroyOptions {
            sandbox_id: sandbox_id.clone(),
            execution: SandboxMetadataExecution::Local,
        },
        &store,
    )
    .expect("destroy");
    assert!(!layout.root.exists());
}

#[test]
fn sandbox_reboot_respects_retain_tmp_policy_in_local_store() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-sandbox-reboot-retain");
    let store = sinomo_runtime::sandbox::LocalSandboxInstanceStore::new(&root);
    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();

    sandbox_create_with_store(
        SandboxCreateOptions {
            sandbox_id: Some(sandbox_id.clone()),
            toolchain_id: sinomo_runtime::sandbox::SandboxToolchainId::generate(),
            temp_policy: sinomo_runtime::sandbox::SandboxTempPolicy::RetainAcrossSessions,
            memory_mb: None,
            wall_time_ms: None,
            max_input_bytes: None,
            max_output_bytes: None,
            max_workspace_bytes: None,
            max_written_bytes_per_command: None,
            max_file_count: None,
            mounts: Vec::new(),
            execution: SandboxMetadataExecution::Local,
        },
        &store,
    )
    .expect("create");
    let (_manifest, layout) = store.load(&sandbox_id).expect("load");
    std::fs::write(layout.tmp.join("session-cache.txt"), b"tmp").expect("write tmp");

    sandbox_reboot_with_store(
        SandboxRebootOptions {
            sandbox_id: sandbox_id.clone(),
            execution: SandboxMetadataExecution::Local,
        },
        &store,
    )
    .expect("reboot");
    assert_eq!(
        std::fs::read(layout.tmp.join("session-cache.txt")).expect("read retained tmp"),
        b"tmp"
    );
}

#[test]
#[cfg(feature = "sandbox")]
fn sandbox_run_persists_stdout_and_stderr_logs() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-sandbox-run");
    let store = sinomo_runtime::sandbox::LocalSandboxInstanceStore::new(&root);
    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();
    let module_path = root.join("stdout.wat");
    std::fs::create_dir_all(&root).expect("root");
    std::fs::write(
        &module_path,
        r#"
        (module
          (import "wasi_snapshot_preview1" "fd_write"
            (func $fd_write (param i32 i32 i32 i32) (result i32)))
          (memory (export "memory") 1 128)
          (data (i32.const 0) "\08\00\00\00\06\00\00\00hello\0a")
          (func (export "_start")
            i32.const 1
            i32.const 0
            i32.const 1
            i32.const 20
            call $fd_write
            drop))
        "#,
    )
    .expect("module");

    sandbox_create_with_store(
        SandboxCreateOptions {
            sandbox_id: Some(sandbox_id.clone()),
            toolchain_id: sinomo_runtime::sandbox::SandboxToolchainId::generate(),
            temp_policy: sinomo_runtime::sandbox::SandboxTempPolicy::ClearOnSessionStart,
            memory_mb: None,
            wall_time_ms: None,
            max_input_bytes: None,
            max_output_bytes: None,
            max_workspace_bytes: None,
            max_written_bytes_per_command: None,
            max_file_count: None,
            mounts: Vec::new(),
            execution: SandboxMetadataExecution::Local,
        },
        &store,
    )
    .expect("create");

    sandbox_run_with_store(
        SandboxRunOptions {
            sandbox_id: sandbox_id.clone(),
            target: SandboxRunTarget::WasmModule(module_path),
            argv: vec!["stdout.wasm".to_string()],
            timeout_ms: 1_000,
            stdin: false,
            allow_unsigned: true,
            allowed_capabilities: Vec::new(),
            execution: SandboxRunExecution::Local,
        },
        &store,
    )
    .expect("run");

    let (_manifest, layout) = store.load(&sandbox_id).expect("load");
    let fs = sinomo_runtime::sandbox::LocalSandboxFileSystem::new(layout);
    let entries = fs
        .list(&sinomo_runtime::sandbox::SandboxPath::new("/logs").expect("logs"))
        .expect("log list");
    let stdout_log = entries
        .iter()
        .find(|entry| entry.path.as_str().ends_with(".stdout"))
        .expect("stdout log")
        .path
        .clone();
    assert_eq!(fs.read(&stdout_log, None).expect("stdout"), b"hello\n");
    let record_log = entries
        .iter()
        .find(|entry| entry.path.as_str().ends_with(".record.sobj"))
        .expect("record log")
        .path
        .clone();
    let record = sinomo_runtime::sandbox::decode_command_record(
        &fs.read(&record_log, None).expect("record bytes"),
    )
    .expect("record");
    assert_eq!(
        record.status,
        sinomo_runtime::sandbox::SandboxCommandRecordStatus::Completed
    );
    assert_eq!(record.argv, vec!["stdout.wasm"]);
    assert!(record.response.is_some());
    let command_id = record.command_id.clone();

    sandbox_logs_with_store(
        SandboxLogsOptions {
            sandbox_id: sandbox_id.clone(),
            command_id: None,
            execution: SandboxMetadataExecution::Local,
        },
        &store,
    )
    .expect("logs");
    sandbox_logs_with_store(
        SandboxLogsOptions {
            sandbox_id,
            command_id: Some(command_id),
            execution: SandboxMetadataExecution::Local,
        },
        &store,
    )
    .expect("command logs");
}

#[test]
#[cfg(feature = "sandbox")]
fn sandbox_run_returns_error_when_guest_exits_nonzero() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-sandbox-run-nonzero");
    let store = sinomo_runtime::sandbox::LocalSandboxInstanceStore::new(&root);
    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();
    let module_path = root.join("exit7.wat");
    std::fs::create_dir_all(&root).expect("root");
    std::fs::write(
        &module_path,
        r#"
        (module
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 128)
          (func (export "_start")
            i32.const 7
            call $proc_exit))
        "#,
    )
    .expect("module");

    sandbox_create_with_store(
        SandboxCreateOptions {
            sandbox_id: Some(sandbox_id.clone()),
            toolchain_id: sinomo_runtime::sandbox::SandboxToolchainId::generate(),
            temp_policy: sinomo_runtime::sandbox::SandboxTempPolicy::ClearOnSessionStart,
            memory_mb: None,
            wall_time_ms: None,
            max_input_bytes: None,
            max_output_bytes: None,
            max_workspace_bytes: None,
            max_written_bytes_per_command: None,
            max_file_count: None,
            mounts: Vec::new(),
            execution: SandboxMetadataExecution::Local,
        },
        &store,
    )
    .expect("create");

    let error = sandbox_run_with_store(
        SandboxRunOptions {
            sandbox_id,
            target: SandboxRunTarget::WasmModule(module_path),
            argv: vec!["exit7.wasm".to_string()],
            timeout_ms: 1_000,
            stdin: false,
            allow_unsigned: true,
            allowed_capabilities: Vec::new(),
            execution: SandboxRunExecution::Local,
        },
        &store,
    )
    .expect_err("nonzero guest exit");
    assert!(
        matches!(error, SandboxCliError::Process { status: 7, .. }),
        "unexpected error: {error:?}"
    );
}

#[test]
#[cfg(feature = "sandbox")]
fn sandbox_run_rejects_mounts_outside_default_policy() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-sandbox-mount-policy");
    let store = sinomo_runtime::sandbox::LocalSandboxInstanceStore::new(&root);
    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();
    let module_path = root.join("noop.wat");
    std::fs::create_dir_all(&root).expect("root");
    std::fs::write(&module_path, r#"(module (func (export "_start")))"#).expect("module");

    sandbox_create_with_store(
        SandboxCreateOptions {
            sandbox_id: Some(sandbox_id.clone()),
            toolchain_id: sinomo_runtime::sandbox::SandboxToolchainId::generate(),
            temp_policy: sinomo_runtime::sandbox::SandboxTempPolicy::ClearOnSessionStart,
            memory_mb: None,
            wall_time_ms: None,
            max_input_bytes: None,
            max_output_bytes: None,
            max_workspace_bytes: None,
            max_written_bytes_per_command: None,
            max_file_count: None,
            mounts: vec![sinomo_runtime::sandbox::SandboxMount {
                source_sifs_path: "/users/shared".to_string(),
                mount_path: sinomo_runtime::sandbox::SandboxPath::new("/shared")
                    .expect("mount path"),
                mode: sinomo_runtime::sandbox::SandboxMountMode::ReadWrite,
            }],
            execution: SandboxMetadataExecution::Local,
        },
        &store,
    )
    .expect("create");

    let error = sandbox_run_with_store(
        SandboxRunOptions {
            sandbox_id,
            target: SandboxRunTarget::WasmModule(module_path),
            argv: vec!["noop.wasm".to_string()],
            timeout_ms: 1000,
            stdin: false,
            allow_unsigned: true,
            allowed_capabilities: Vec::new(),
            execution: SandboxRunExecution::Local,
        },
        &store,
    )
    .expect_err("mount source denied");
    assert!(matches!(error, SandboxCliError::SandboxPolicyDenied { .. }));
}

#[test]
#[cfg(feature = "sandbox")]
fn sandbox_run_uses_pinned_toolchain_command() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-sandbox-toolchain-run");
    let store = sinomo_runtime::sandbox::LocalSandboxInstanceStore::new(root.join("runtime"));
    let toolchain_store =
        sinomo_runtime::sandbox::LocalSandboxToolchainStore::new(root.join("cache"));
    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();
    let toolchain_id = sinomo_runtime::sandbox::SandboxToolchainId::generate();
    let mut toolchain = sinomo_runtime::sandbox::SandboxToolchainManifest::new(
        toolchain_id.clone(),
        "test-1",
        sinomo_runtime::sandbox::SandboxWasiAbi::Wasix,
        "test module",
        OffsetDateTime::now_utc(),
    )
    .expect("toolchain");
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "hello",
            r#"
            (module
              (import "wasi_snapshot_preview1" "fd_write"
                (func $fd_write (param i32 i32 i32 i32) (result i32)))
              (memory (export "memory") 1 128)
              (data (i32.const 0) "\08\00\00\00\06\00\00\00hello\0a")
              (func (export "_start")
                i32.const 1
                i32.const 0
                i32.const 1
                i32.const 20
                call $fd_write
                drop))
            "#
            .as_bytes(),
            sinomo_runtime::sandbox::SandboxResourceLimits::default(),
        )
        .expect("put command");
    toolchain_store
        .put_command_module_with_capabilities(
            &mut toolchain,
            "networked",
            wasi_stdin_to_stdout_module().as_bytes(),
            sinomo_runtime::sandbox::SandboxResourceLimits::default(),
            vec!["network.http".to_string()],
        )
        .expect("put networked command");

    sandbox_create_with_store(
        SandboxCreateOptions {
            sandbox_id: Some(sandbox_id.clone()),
            toolchain_id,
            temp_policy: sinomo_runtime::sandbox::SandboxTempPolicy::ClearOnSessionStart,
            memory_mb: None,
            wall_time_ms: None,
            max_input_bytes: None,
            max_output_bytes: None,
            max_workspace_bytes: None,
            max_written_bytes_per_command: None,
            max_file_count: None,
            mounts: Vec::new(),
            execution: SandboxMetadataExecution::Local,
        },
        &store,
    )
    .expect("create");

    sandbox_run_with_stores(
        SandboxRunOptions {
            sandbox_id: sandbox_id.clone(),
            target: SandboxRunTarget::ToolchainCommand("hello".to_string()),
            argv: vec!["hello".to_string()],
            timeout_ms: 1_000,
            stdin: false,
            allow_unsigned: true,
            allowed_capabilities: Vec::new(),
            execution: SandboxRunExecution::Local,
        },
        &store,
        &toolchain_store,
    )
    .expect("run");
    let mismatch = sandbox_run_with_stores(
        SandboxRunOptions {
            sandbox_id: sandbox_id.clone(),
            target: SandboxRunTarget::ToolchainCommand("hello".to_string()),
            argv: vec!["cat".to_string()],
            timeout_ms: 1_000,
            stdin: false,
            allow_unsigned: true,
            allowed_capabilities: Vec::new(),
            execution: SandboxRunExecution::Local,
        },
        &store,
        &toolchain_store,
    )
    .expect_err("argv0 mismatch");
    assert!(matches!(
        mismatch,
        SandboxCliError::Parse { ref detail }
            if detail.contains("matching argv[0]")
                && detail.contains("`hello`")
                && detail.contains("`cat`")
    ));
    let capability_denied = sandbox_run_with_stores(
        SandboxRunOptions {
            sandbox_id: sandbox_id.clone(),
            target: SandboxRunTarget::ToolchainCommand("networked".to_string()),
            argv: vec!["networked".to_string()],
            timeout_ms: 1_000,
            stdin: false,
            allow_unsigned: true,
            allowed_capabilities: Vec::new(),
            execution: SandboxRunExecution::Local,
        },
        &store,
        &toolchain_store,
    )
    .expect_err("capability denied");
    assert!(matches!(
        capability_denied,
        SandboxCliError::SandboxPolicyDenied { ref detail }
            if detail.contains("command-capability-denied")
                && detail.contains("network.http")
    ));
    let (_manifest, layout) = store.load(&sandbox_id).expect("load");
    let fs = sinomo_runtime::sandbox::LocalSandboxFileSystem::new(layout);
    let entries = fs
        .list(&sinomo_runtime::sandbox::SandboxPath::new("/logs").expect("logs"))
        .expect("logs");
    let stdout_log = entries
        .iter()
        .find(|entry| entry.path.as_str().ends_with(".stdout"))
        .expect("stdout log")
        .path
        .clone();
    assert_eq!(fs.read(&stdout_log, None).expect("stdout"), b"hello\n");
}

#[test]
#[cfg(feature = "sandbox")]
fn sandbox_process_plan_runs_pinned_commands_from_json_plan() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-sandbox-process-plan");
    let store = sinomo_runtime::sandbox::LocalSandboxInstanceStore::new(root.join("runtime"));
    let toolchain_store =
        sinomo_runtime::sandbox::LocalSandboxToolchainStore::new(root.join("cache"));
    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();
    let toolchain_id = sinomo_runtime::sandbox::SandboxToolchainId::generate();
    let mut toolchain = sinomo_runtime::sandbox::SandboxToolchainManifest::new(
        toolchain_id.clone(),
        "test-1",
        sinomo_runtime::sandbox::SandboxWasiAbi::Wasix,
        "test process plan",
        OffsetDateTime::now_utc(),
    )
    .expect("toolchain");
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "echo",
            r#"
            (module
              (import "wasi_snapshot_preview1" "fd_write"
                (func $fd_write (param i32 i32 i32 i32) (result i32)))
              (memory (export "memory") 1 128)
              (data (i32.const 0) "\08\00\00\00\06\00\00\00hello\0a")
              (func (export "_start")
                i32.const 1
                i32.const 0
                i32.const 1
                i32.const 20
                call $fd_write
                drop))
            "#
            .as_bytes(),
            sinomo_runtime::sandbox::SandboxResourceLimits::default(),
        )
        .expect("put echo");
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "cat",
            wasi_stdin_to_stdout_module().as_bytes(),
            sinomo_runtime::sandbox::SandboxResourceLimits::default(),
        )
        .expect("put cat");

    sandbox_create_with_store(
        SandboxCreateOptions {
            sandbox_id: Some(sandbox_id.clone()),
            toolchain_id,
            temp_policy: sinomo_runtime::sandbox::SandboxTempPolicy::ClearOnSessionStart,
            memory_mb: None,
            wall_time_ms: None,
            max_input_bytes: None,
            max_output_bytes: None,
            max_workspace_bytes: None,
            max_written_bytes_per_command: None,
            max_file_count: None,
            mounts: Vec::new(),
            execution: SandboxMetadataExecution::Local,
        },
        &store,
    )
    .expect("create");

    let plan_path = root.join("process-plan.json");
    std::fs::create_dir_all(&root).expect("root");
    std::fs::write(
        &plan_path,
        serde_json::to_vec(&sinomo_runtime::sandbox::SandboxProcessPlan {
            steps: vec![
                sinomo_runtime::sandbox::SandboxProcessPlanStep {
                    command_name: "echo".to_string(),
                    argv: vec!["echo".to_string()],
                    stdin: None,
                    cwd: None,
                    env: Default::default(),
                    pipe_stdout_to_next: true,
                },
                sinomo_runtime::sandbox::SandboxProcessPlanStep {
                    command_name: "cat".to_string(),
                    argv: vec!["cat".to_string()],
                    stdin: None,
                    cwd: None,
                    env: Default::default(),
                    pipe_stdout_to_next: false,
                },
            ],
        })
        .expect("encode plan"),
    )
    .expect("write plan");

    sandbox_process_plan_with_stores(
        SandboxProcessPlanOptions {
            sandbox_id: sandbox_id.clone(),
            plan_path,
            timeout_ms: 1_000,
            execution: SandboxRunExecution::Local,
        },
        &store,
        &toolchain_store,
    )
    .expect("process plan");

    let (manifest, layout) = store.load(&sandbox_id).expect("load");
    assert_eq!(manifest.command_history.len(), 2);
    let fs = sinomo_runtime::sandbox::LocalSandboxFileSystem::new(layout);
    let entries = fs
        .list(&sinomo_runtime::sandbox::SandboxPath::new("/logs").expect("logs"))
        .expect("logs");
    let stdout_logs: Vec<_> = entries
        .iter()
        .filter(|entry| entry.path.as_str().ends_with(".stdout"))
        .collect();
    assert_eq!(stdout_logs.len(), 2);
    assert!(
        stdout_logs
            .iter()
            .any(|entry| fs.read(&entry.path, None).expect("stdout") == b"hello\n")
    );
}

#[test]
#[cfg(feature = "sandbox")]
fn sandbox_shell_rejects_toolchain_without_real_sh() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-sandbox-shell-missing");
    let store = sinomo_runtime::sandbox::LocalSandboxInstanceStore::new(root.join("runtime"));
    let toolchain_store =
        sinomo_runtime::sandbox::LocalSandboxToolchainStore::new(root.join("cache"));
    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();
    let toolchain_id = sinomo_runtime::sandbox::SandboxToolchainId::generate();
    let mut toolchain = sinomo_runtime::sandbox::SandboxToolchainManifest::new(
        toolchain_id.clone(),
        "test-no-shell",
        sinomo_runtime::sandbox::SandboxWasiAbi::Wasix,
        "test toolchain without shell",
        OffsetDateTime::now_utc(),
    )
    .expect("toolchain");
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "cat",
            wasi_stdin_to_stdout_module().as_bytes(),
            sinomo_runtime::sandbox::SandboxResourceLimits::default(),
        )
        .expect("put command");

    sandbox_create_with_store(
        SandboxCreateOptions {
            sandbox_id: Some(sandbox_id.clone()),
            toolchain_id,
            temp_policy: sinomo_runtime::sandbox::SandboxTempPolicy::ClearOnSessionStart,
            memory_mb: None,
            wall_time_ms: None,
            max_input_bytes: None,
            max_output_bytes: None,
            max_workspace_bytes: None,
            max_written_bytes_per_command: None,
            max_file_count: None,
            mounts: Vec::new(),
            execution: SandboxMetadataExecution::Local,
        },
        &store,
    )
    .expect("create");

    let error = sandbox_shell_with_stores(
        SandboxShellOptions {
            sandbox_id,
            script: None,
            timeout_ms: 1_000,
            execution: SandboxRunExecution::Local,
        },
        b"echo shell\n".to_vec(),
        &store,
        &toolchain_store,
    )
    .expect_err("missing shell should fail");
    match error {
        SandboxCliError::Parse { detail } => {
            assert!(detail.contains("sandbox toolchain command not found"));
            assert!(detail.contains(":sh"));
        }
        other => panic!("unexpected missing shell error: {other:?}"),
    }
}

#[test]
fn sandbox_toolchain_add_command_installs_local_command_module() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-sandbox-toolchain-add");
    let module_path = root.join("hello.wasm");
    std::fs::create_dir_all(&root).expect("root");
    std::fs::write(
        &module_path,
        r#"
        (module
          (import "wasi_snapshot_preview1" "fd_write"
            (func $fd_write (param i32 i32 i32 i32) (result i32)))
          (memory (export "memory") 1 128)
          (data (i32.const 0) "\08\00\00\00\06\00\00\00hello\0a")
          (func (export "_start")
            i32.const 1
            i32.const 0
            i32.const 1
            i32.const 20
            call $fd_write
            drop))
        "#,
    )
    .expect("module");
    let toolchain_store =
        sinomo_runtime::sandbox::LocalSandboxToolchainStore::new(root.join("cache"));
    let toolchain_id = sinomo_runtime::sandbox::SandboxToolchainId::generate();

    sandbox_toolchain_add_command_with_store(
        SandboxToolchainAddCommandOptions {
            toolchain_id: toolchain_id.clone(),
            version: "test-1".to_string(),
            target_abi: sinomo_runtime::sandbox::SandboxWasiAbi::Wasix,
            provenance: "unit test".to_string(),
            command_name: "hello".to_string(),
            module_path: module_path.clone(),
            execution: SandboxMetadataExecution::Local,
            required_capabilities: vec!["network.http".to_string()],
            default_limits: sinomo_runtime::sandbox::SandboxResourceLimits {
                memory_mb: 64,
                wall_time_ms: 1_234,
                ..sinomo_runtime::sandbox::SandboxResourceLimits::default()
            },
        },
        &toolchain_store,
    )
    .expect("add command");

    let (manifest, _layout) = toolchain_store
        .load_manifest(&toolchain_id)
        .expect("manifest");
    assert_eq!(manifest.version, "test-1");
    let command = manifest.command("hello").expect("command");
    assert_eq!(
        command.effective_target_abi(&manifest),
        sinomo_runtime::sandbox::SandboxWasiAbi::Wasix
    );
    assert_eq!(command.required_capabilities, vec!["network.http"]);
    assert_eq!(command.default_limits.memory_mb, 64);
    assert_eq!(command.default_limits.wall_time_ms, 1_234);
    assert_eq!(
        toolchain_store
            .load_command_module(&manifest, "hello")
            .expect("module"),
        std::fs::read(module_path).expect("module bytes")
    );
}

#[test]
fn sandbox_module_artifact_register_writes_registry_and_copies_bytes() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-sandbox-module-artifact");
    let source_path = root.join("source.wasm");
    std::fs::write(&source_path, b"module bytes").expect("source");
    let artifact_id = sinomo_runtime::module::ArtifactId::from_legacy_named("artifact_cli_module")
        .expect("artifact");
    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();
    let cwd = sinomo_runtime::sandbox::SandboxPath::new("/work".to_string()).expect("cwd");
    let mut env = BTreeMap::new();
    env.insert("RUST_LOG".to_string(), "debug".to_string());
    let limits = sinomo_runtime::sandbox::SandboxResourceLimits {
        memory_mb: 128,
        wall_time_ms: 2_000,
        ..sinomo_runtime::sandbox::SandboxResourceLimits::default()
    };

    sandbox_module_artifact_register_with_data_dir(
        SandboxModuleArtifactRegisterOptions {
            artifact_id: artifact_id.clone(),
            source_path: source_path.clone(),
            interface_version: Some("custom-v0".to_string()),
            component_model: false,
            abi: sinomo_runtime::sandbox::SandboxWasiAbi::Wasix,
            sandbox_id: Some(sandbox_id.clone()),
            argv: vec!["module.wasm".to_string()],
            cwd: Some(cwd.clone()),
            env: env.clone(),
            limits: limits.clone(),
            timeout_ms: Some(1_500),
            rollout_material: None,
        },
        &root,
    )
    .expect("register module artifact");

    let artifact_path = root
        .join("sandbox")
        .join("module-artifacts")
        .join(format!("{artifact_id}.wasm"));
    assert_eq!(
        std::fs::read(&artifact_path).expect("artifact bytes"),
        b"module bytes"
    );
    let registry = sinomo_runtime::sandbox::read_sandbox_wasm_module_artifact_registry(&root)
        .expect("read registry")
        .expect("registry");
    let artifact = registry
        .artifact(&artifact_id)
        .expect("registered artifact");
    assert_eq!(
        artifact.wasm_path,
        PathBuf::from(format!("sandbox/module-artifacts/{artifact_id}.wasm"))
    );
    assert_eq!(
        artifact.content_hash,
        sinomo_runtime::sandbox::sandbox_module_hash(b"module bytes")
    );
    assert_eq!(artifact.interface_version.as_deref(), Some("custom-v0"));
    assert!(!artifact.component_model);
    assert_eq!(artifact.abi, sinomo_runtime::sandbox::SandboxWasiAbi::Wasix);
    assert_eq!(artifact.sandbox_id, Some(sandbox_id));
    assert_eq!(artifact.argv, vec!["module.wasm"]);
    assert_eq!(artifact.cwd, Some(cwd));
    assert_eq!(artifact.env, env);
    assert_eq!(artifact.limits, limits);
    assert_eq!(artifact.timeout_ms, Some(1_500));
}

#[test]
fn sandbox_module_artifact_register_writes_candidate_rollout_material() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-sandbox-module-rollout-material");
    let source_path = root.join("candidate.wasm");
    std::fs::write(&source_path, b"candidate module bytes").expect("source");
    let bundle_dir = root.join("candidate-bundle");
    std::fs::create_dir_all(&bundle_dir).expect("bundle");
    let mutation_packet_path = bundle_dir.join("mutation-packet.json");
    std::fs::write(
        &mutation_packet_path,
        "{\"objective\":\"module rollout\"}\n",
    )
    .expect("mutation packet");
    let job_id = Id::from_legacy_named("evojob_module_rollout_material").expect("job");
    let manifest = serde_json::json!({
        "job_id": job_id,
        "bundle_path": bundle_dir.display().to_string(),
        "created_at": OffsetDateTime::now_utc(),
        "mutation_packet_path": mutation_packet_path.display().to_string(),
        "bundle_content_hash": null,
        "artifact_refs": [],
        "command_logs": [],
    });
    std::fs::write(
        bundle_dir.join("manifest.json"),
        serde_json::to_string_pretty(&manifest).expect("manifest json"),
    )
    .expect("write manifest");
    let artifact_id =
        sinomo_runtime::module::ArtifactId::from_legacy_named("artifact_cli_rollout_candidate")
            .expect("artifact");
    let active_artifact_id =
        sinomo_runtime::module::ArtifactId::from_legacy_named("artifact_cli_rollout_active")
            .expect("active");
    let module_id =
        sinomo_runtime::module::ModuleId::from_legacy_named("module.trigger.silence_policy")
            .expect("module");

    sandbox_module_artifact_register_with_data_dir(
        SandboxModuleArtifactRegisterOptions {
            artifact_id: artifact_id.clone(),
            source_path: source_path.clone(),
            interface_version: Some(
                sinomo_runtime::module::SINOMO_MODULE_V1_INTERFACE_VERSION.to_string(),
            ),
            component_model: false,
            abi: sinomo_runtime::sandbox::SandboxWasiAbi::Wasix,
            sandbox_id: None,
            argv: Vec::new(),
            cwd: None,
            env: BTreeMap::new(),
            limits: sinomo_runtime::sandbox::SandboxResourceLimits::default(),
            timeout_ms: Some(1_500),
            rollout_material: Some(SandboxModuleArtifactRolloutMaterialOptions {
                candidate_bundle_path: bundle_dir.clone(),
                module_id: module_id.clone(),
                active_artifact_id: active_artifact_id.clone(),
                rollout_plan_id: Some(
                    Id::from_legacy_named("rollout_cli_material").expect("rollout"),
                ),
                policy_decision_id: Some(
                    Id::from_legacy_named("policy_cli_material").expect("policy"),
                ),
                module_version: "0.2.0".to_string(),
                module_owner: "trigger".to_string(),
                evidence_refs: vec!["module-replay://silence-policy".to_string()],
            }),
        },
        &root,
    )
    .expect("register module artifact with rollout material");

    let material_path = bundle_dir.join(sinomo_runtime::registry::RELEASE_MATERIAL_RELATIVE_PATH);
    assert!(material_path.exists());
    let material: sinomo_runtime::registry::VerifiedReleaseMaterialRecord = serde_json::from_str(
        &std::fs::read_to_string(&material_path).expect("rollout material json"),
    )
    .expect("rollout material");
    assert_eq!(material.release_id, job_id);
    assert_eq!(material.artifact.artifact_id, artifact_id);
    assert_eq!(material.artifact.module_id, module_id);
    assert_eq!(
        material.artifact.content_hash,
        sinomo_runtime::sandbox::sandbox_module_hash(b"candidate module bytes")
    );
    assert_eq!(material.active_artifact_id, active_artifact_id);
    assert_eq!(
        material.policy_decision.action,
        sinomo_runtime::core::PolicyAction::Allow
    );
    assert_eq!(
        material.rollout_plan.strategy,
        sinomo_runtime::core::RolloutStrategy::Shadow
    );
    assert_eq!(material.rollout_plan.initial_weight_bps, 0);
    assert!(
        material
            .evidence_refs
            .iter()
            .any(|evidence| evidence == &format!("candidate-bundle://{job_id}"))
    );
    assert!(
        material
            .artifact
            .provenance
            .attestations
            .iter()
            .any(|evidence| evidence == &format!("registry://artifact-evidence/{job_id}"))
    );
    sinomo_runtime::module::verify_artifact_signature(&material.artifact)
        .expect("artifact signature verifies");
}

#[test]
fn sandbox_toolchain_add_artifact_installs_local_support_file() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-sandbox-artifact-add");
    let artifact_path = root.join("os.py");
    std::fs::create_dir_all(&root).expect("root");
    std::fs::write(&artifact_path, b"print('support')\n").expect("artifact");
    let toolchain_store =
        sinomo_runtime::sandbox::LocalSandboxToolchainStore::new(root.join("cache"));
    let toolchain_id = sinomo_runtime::sandbox::SandboxToolchainId::generate();

    sandbox_toolchain_add_artifact_with_store(
        SandboxToolchainAddArtifactOptions {
            toolchain_id: toolchain_id.clone(),
            version: "python-1".to_string(),
            provenance: "unit test".to_string(),
            relative_path: "lib/python/os.py".to_string(),
            source_path: artifact_path.clone(),
            execution: SandboxMetadataExecution::Local,
        },
        &toolchain_store,
    )
    .expect("add artifact");

    let (manifest, _layout) = toolchain_store
        .load_manifest(&toolchain_id)
        .expect("manifest");
    assert_eq!(manifest.version, "python-1");
    let artifact = manifest.artifact("lib/python/os.py").expect("artifact");
    assert_eq!(artifact.byte_len, b"print('support')\n".len() as u64);
    assert_eq!(
        toolchain_store
            .load_artifact(&manifest, "lib/python/os.py")
            .expect("artifact bytes"),
        std::fs::read(artifact_path).expect("artifact bytes")
    );
}

#[test]
fn sandbox_toolchain_import_package_installs_local_commands_and_artifacts() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-sandbox-package-import");
    std::fs::create_dir_all(root.join("bin")).expect("bin");
    std::fs::create_dir_all(root.join("share")).expect("share");
    std::fs::write(root.join("bin/python.wasm"), b"python wasm").expect("module");
    std::fs::write(root.join("share/stdlib.zip"), b"stdlib").expect("artifact");
    let toolchain_id = sinomo_runtime::sandbox::SandboxToolchainId::generate();
    let package_path = root.join("package.json");
    std::fs::write(
        &package_path,
        serde_json::json!({
            "toolchain_id": toolchain_id,
            "version": "python-wasi-test",
            "provenance": "sinomo://toolchains/python/test",
            "commands": [{
                "name": "python",
                "module": "bin/python.wasm",
                "content_hash": sinomo_runtime::sandbox::sandbox_module_hash(b"python wasm"),
                "required_capabilities": ["network.http"],
                "default_limits": {
                    "memory_mb": 256,
                    "wall_time_ms": 10000,
                    "max_input_bytes": 1048576,
                    "max_output_bytes": 1048576,
                    "max_workspace_bytes": 104857600,
                    "max_written_bytes_per_command": 10485760,
                    "max_file_count": 10000
                }
            }],
            "artifacts": [{
                "relative_path": "share/python/stdlib.zip",
                "file": "share/stdlib.zip",
                "content_hash": sinomo_runtime::sandbox::sandbox_module_hash(b"stdlib")
            }]
        })
        .to_string(),
    )
    .expect("package");
    let toolchain_store =
        sinomo_runtime::sandbox::LocalSandboxToolchainStore::new(root.join("cache"));

    sandbox_toolchain_import_package_with_store(
        SandboxToolchainImportPackageOptions {
            package_path: package_path.clone(),
            target_toolchain_id: None,
            execution: SandboxMetadataExecution::Local,
        },
        &toolchain_store,
    )
    .expect("import package");

    let (manifest, _layout) = toolchain_store
        .load_manifest(&toolchain_id)
        .expect("manifest");
    assert_eq!(manifest.version, "python-wasi-test");
    let command = manifest.command("python").expect("command");
    assert_eq!(command.required_capabilities, vec!["network.http"]);
    assert_eq!(command.default_limits.memory_mb, 256);
    assert_eq!(command.default_limits.max_input_bytes, 1048576);
    assert_eq!(command.default_limits.max_output_bytes, 1048576);
    assert_eq!(
        toolchain_store
            .load_command_module(&manifest, "python")
            .expect("module"),
        b"python wasm"
    );
    assert_eq!(
        toolchain_store
            .load_artifact(&manifest, "share/python/stdlib.zip")
            .expect("artifact"),
        b"stdlib"
    );
}

#[test]
fn sandbox_toolchain_verify_package_accepts_valid_package_without_importing() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-sandbox-package-verify");
    std::fs::create_dir_all(root.join("bin")).expect("bin");
    std::fs::create_dir_all(root.join("share")).expect("share");
    std::fs::write(root.join("bin/script.wasm"), b"script wasm").expect("module");
    std::fs::write(root.join("share/runtime.js"), b"export const ok = true;\n").expect("artifact");
    let toolchain_id = sinomo_runtime::sandbox::SandboxToolchainId::generate();
    let package_path = root.join("package.json");
    std::fs::write(
        &package_path,
        serde_json::json!({
            "toolchain_id": toolchain_id,
            "version": "script-wasix-test",
            "provenance": "sinomo://toolchains/script/test",
            "commands": [{
                "name": "script",
                "module": "bin/script.wasm",
                "content_hash": sinomo_runtime::sandbox::sandbox_module_hash(b"script wasm")
            }],
            "artifacts": [{
                "relative_path": "share/script/runtime.js",
                "file": "share/runtime.js",
                "content_hash": sinomo_runtime::sandbox::sandbox_module_hash(b"export const ok = true;\n")
            }]
        })
        .to_string(),
    )
    .expect("package");
    let cache_root = root.join("cache");

    sandbox_toolchain_verify_package(SandboxToolchainVerifyPackageOptions { package_path })
        .expect("verify package");

    assert!(
        !cache_root.exists(),
        "verify-package must not import package bytes into the local toolchain cache"
    );
}

#[test]
fn sandbox_toolchain_verify_package_rejects_invalid_package_without_importing() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-sandbox-package-verify-invalid");
    std::fs::create_dir_all(root.join("bin")).expect("bin");
    std::fs::write(root.join("bin/python.wasm"), b"python wasm").expect("module");
    let toolchain_id = sinomo_runtime::sandbox::SandboxToolchainId::generate();
    let package_path = root.join("package.json");
    let module_hash = sinomo_runtime::sandbox::sandbox_module_hash(b"python wasm");
    std::fs::write(
        &package_path,
        serde_json::json!({
            "toolchain_id": toolchain_id,
            "version": "bad-package",
            "provenance": "unit test",
            "commands": [
                {
                    "name": "python",
                    "module": "bin/python.wasm",
                    "content_hash": module_hash
                },
                {
                    "name": "python",
                    "module": "bin/python.wasm",
                    "content_hash": module_hash
                }
            ],
            "artifacts": []
        })
        .to_string(),
    )
    .expect("package");
    let cache_root = root.join("cache");

    let error =
        sandbox_toolchain_verify_package(SandboxToolchainVerifyPackageOptions { package_path })
            .expect_err("duplicate command denied");

    match error {
        SandboxCliError::Parse { detail } => assert!(detail.contains("duplicate")),
        other => panic!("unexpected error: {other:?}"),
    }
    assert!(
        !cache_root.exists(),
        "verify-package must not create a toolchain cache on failure"
    );
}

#[test]
fn sandbox_toolchain_verify_package_rejects_invalid_default_env_name() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-sandbox-package-verify-bad-env");
    std::fs::create_dir_all(root.join("bin")).expect("bin");
    std::fs::write(root.join("bin/python.wasm"), b"python wasm").expect("module");
    let toolchain_id = sinomo_runtime::sandbox::SandboxToolchainId::generate();
    let package_path = root.join("package.json");
    std::fs::write(
        &package_path,
        serde_json::json!({
            "toolchain_id": toolchain_id,
            "version": "bad-env-package",
            "provenance": "unit test",
            "commands": [{
                "name": "python",
                "module": "bin/python.wasm",
                "content_hash": sinomo_runtime::sandbox::sandbox_module_hash(b"python wasm"),
                "default_env": {
                    "BAD-NAME": "value"
                }
            }],
            "artifacts": []
        })
        .to_string(),
    )
    .expect("package");

    let error =
        sandbox_toolchain_verify_package(SandboxToolchainVerifyPackageOptions { package_path })
            .expect_err("invalid default env denied");

    match error {
        SandboxCliError::Parse { detail } => {
            assert!(detail.contains("invalid default environment variable name `BAD-NAME`"))
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn sandbox_toolchain_verify_package_rejects_invalid_default_env_value() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-sandbox-package-verify-bad-env-value");
    std::fs::create_dir_all(root.join("bin")).expect("bin");
    std::fs::write(root.join("bin/python.wasm"), b"python wasm").expect("module");
    let toolchain_id = sinomo_runtime::sandbox::SandboxToolchainId::generate();
    let package_path = root.join("package.json");
    std::fs::write(
        &package_path,
        serde_json::json!({
            "toolchain_id": toolchain_id,
            "version": "bad-env-value-package",
            "provenance": "unit test",
            "commands": [{
                "name": "python",
                "module": "bin/python.wasm",
                "content_hash": sinomo_runtime::sandbox::sandbox_module_hash(b"python wasm"),
                "default_env": {
                    "PYTHONHOME": "/\u{0}bad"
                }
            }],
            "artifacts": []
        })
        .to_string(),
    )
    .expect("package");

    let error =
        sandbox_toolchain_verify_package(SandboxToolchainVerifyPackageOptions { package_path })
            .expect_err("invalid default env value denied");

    match error {
        SandboxCliError::Parse { detail } => {
            assert!(detail.contains(
                "default environment variable `PYTHONHOME` contains an embedded NUL byte"
            ))
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn sandbox_toolchain_verify_package_rejects_invalid_argv_prefix_value() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-sandbox-package-verify-bad-argv-prefix");
    std::fs::create_dir_all(root.join("bin")).expect("bin");
    std::fs::write(root.join("bin/python.wasm"), b"python wasm").expect("module");
    let toolchain_id = sinomo_runtime::sandbox::SandboxToolchainId::generate();
    let package_path = root.join("package.json");
    std::fs::write(
        &package_path,
        serde_json::json!({
            "toolchain_id": toolchain_id,
            "version": "bad-argv-prefix-package",
            "provenance": "unit test",
            "commands": [{
                "name": "python",
                "module": "bin/python.wasm",
                "content_hash": sinomo_runtime::sandbox::sandbox_module_hash(b"python wasm"),
                "argv_prefix": ["-m", "bad\u{0}module"]
            }],
            "artifacts": []
        })
        .to_string(),
    )
    .expect("package");

    let error =
        sandbox_toolchain_verify_package(SandboxToolchainVerifyPackageOptions { package_path })
            .expect_err("invalid argv prefix denied");

    match error {
        SandboxCliError::Parse { detail } => {
            assert!(detail.contains("argv_prefix[1] contains an embedded NUL byte"))
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn sandbox_toolchain_verify_package_rejects_invalid_required_capability() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-sandbox-package-verify-bad-capability");
    std::fs::create_dir_all(root.join("bin")).expect("bin");
    std::fs::write(root.join("bin/fetch.wasm"), b"fetch wasm").expect("module");
    let toolchain_id = sinomo_runtime::sandbox::SandboxToolchainId::generate();
    let package_path = root.join("package.json");
    std::fs::write(
        &package_path,
        serde_json::json!({
            "toolchain_id": toolchain_id,
            "version": "bad-capability-package",
            "provenance": "unit test",
            "commands": [{
                "name": "fetch",
                "module": "bin/fetch.wasm",
                "content_hash": sinomo_runtime::sandbox::sandbox_module_hash(b"fetch wasm"),
                "required_capabilities": ["network\u{0}http"]
            }],
            "artifacts": []
        })
        .to_string(),
    )
    .expect("package");

    let error =
        sandbox_toolchain_verify_package(SandboxToolchainVerifyPackageOptions { package_path })
            .expect_err("invalid capability denied");

    match error {
        SandboxCliError::Parse { detail } => {
            assert!(detail.contains("sandbox toolchain command capability"));
            assert!(detail.contains("embedded NUL byte"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn sandbox_toolchain_import_package_can_extend_existing_local_toolchain() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-sandbox-package-extend");
    std::fs::create_dir_all(root.join("bin")).expect("bin");
    std::fs::write(root.join("bin/script.wasm"), b"script wasm").expect("module");
    let package_toolchain_id = sinomo_runtime::sandbox::SandboxToolchainId::generate();
    let target_toolchain_id =
        sinomo_runtime::sandbox::SandboxToolchainId::from_legacy_named("toolchain_busybox")
            .expect("busybox");
    let package_path = root.join("package.json");
    std::fs::write(
        &package_path,
        serde_json::json!({
            "toolchain_id": package_toolchain_id,
            "version": "script-wasix-test",
            "target_abi": "wasix",
            "provenance": "sinomo://toolchains/script/test",
            "commands": [{
                "name": "script",
                "module": "bin/script.wasm",
                "target_abi": "wasix",
                "content_hash": sinomo_runtime::sandbox::sandbox_module_hash(b"script wasm")
            }],
            "artifacts": []
        })
        .to_string(),
    )
    .expect("package");
    let toolchain_store =
        sinomo_runtime::sandbox::LocalSandboxToolchainStore::new(root.join("cache"));
    let mut manifest = sinomo_runtime::sandbox::SandboxToolchainManifest::new(
        target_toolchain_id.clone(),
        "busybox-wasi-test",
        sinomo_runtime::sandbox::SandboxWasiAbi::Wasix,
        "sinomo://toolchains/busybox/test",
        OffsetDateTime::now_utc(),
    )
    .expect("manifest");
    toolchain_store
        .put_command_module(
            &mut manifest,
            "cat",
            b"busybox module",
            sinomo_runtime::sandbox::SandboxResourceLimits::default(),
        )
        .expect("existing command");

    sandbox_toolchain_import_package_with_store(
        SandboxToolchainImportPackageOptions {
            package_path: package_path.clone(),
            target_toolchain_id: Some(target_toolchain_id.clone()),
            execution: SandboxMetadataExecution::Local,
        },
        &toolchain_store,
    )
    .expect("extend package");

    let (manifest, _layout) = toolchain_store
        .load_manifest(&target_toolchain_id)
        .expect("manifest");
    assert_eq!(manifest.version, "busybox-wasi-test");
    assert_eq!(
        manifest.target_abi,
        sinomo_runtime::sandbox::SandboxWasiAbi::Wasix
    );
    assert!(manifest.command("cat").is_ok());
    assert_eq!(
        manifest
            .command("script")
            .expect("script")
            .effective_target_abi(&manifest),
        sinomo_runtime::sandbox::SandboxWasiAbi::Wasix
    );
    assert_eq!(
        toolchain_store
            .load_command_module(&manifest, "script")
            .expect("module"),
        b"script wasm"
    );
    assert!(matches!(
        toolchain_store.load_manifest(&package_toolchain_id),
        Err(sinomo_runtime::sandbox::SandboxToolchainError::NotFound { .. })
    ));
}

#[test]
fn sandbox_toolchain_import_package_preflights_existing_manifest_conflicts() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-sandbox-package-preflight");
    std::fs::create_dir_all(root.join("bin")).expect("bin");
    std::fs::write(root.join("bin/script.wasm"), b"script wasm").expect("script");
    std::fs::write(root.join("bin/cat.wasm"), b"duplicate cat").expect("cat");
    let target_toolchain_id =
        sinomo_runtime::sandbox::SandboxToolchainId::from_legacy_named("toolchain_busybox")
            .expect("busybox");
    let package_path = root.join("package.json");
    std::fs::write(
        &package_path,
        serde_json::json!({
            "toolchain_id": sinomo_runtime::sandbox::SandboxToolchainId::generate(),
            "version": "bad-extension",
            "provenance": "unit test",
            "commands": [
                {
                    "name": "script",
                    "module": "bin/script.wasm",
                    "content_hash": sinomo_runtime::sandbox::sandbox_module_hash(b"script wasm")
                },
                {
                    "name": "cat",
                    "module": "bin/cat.wasm",
                    "content_hash": sinomo_runtime::sandbox::sandbox_module_hash(b"duplicate cat")
                }
            ],
            "artifacts": []
        })
        .to_string(),
    )
    .expect("package");
    let toolchain_store =
        sinomo_runtime::sandbox::LocalSandboxToolchainStore::new(root.join("cache"));
    let mut manifest = sinomo_runtime::sandbox::SandboxToolchainManifest::new(
        target_toolchain_id.clone(),
        "busybox-wasi-test",
        sinomo_runtime::sandbox::SandboxWasiAbi::Wasix,
        "unit test base",
        OffsetDateTime::now_utc(),
    )
    .expect("manifest");
    toolchain_store
        .put_command_module(
            &mut manifest,
            "cat",
            b"busybox module",
            sinomo_runtime::sandbox::SandboxResourceLimits::default(),
        )
        .expect("existing command");

    let error = sandbox_toolchain_import_package_with_store(
        SandboxToolchainImportPackageOptions {
            package_path,
            target_toolchain_id: Some(target_toolchain_id.clone()),
            execution: SandboxMetadataExecution::Local,
        },
        &toolchain_store,
    )
    .expect_err("duplicate existing command denied");
    assert!(format!("{error:?}").contains("duplicate sandbox toolchain command"));
    let (manifest, _layout) = toolchain_store
        .load_manifest(&target_toolchain_id)
        .expect("manifest");
    assert!(manifest.command("cat").is_ok());
    assert!(manifest.command("script").is_err());
}

#[test]
fn sandbox_toolchain_import_package_rejects_host_path_escape() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-sandbox-package-escape");
    let toolchain_id = sinomo_runtime::sandbox::SandboxToolchainId::generate();
    let package_path = root.join("package.json");
    std::fs::write(
        &package_path,
        serde_json::json!({
            "toolchain_id": toolchain_id,
            "version": "bad-package",
            "provenance": "unit test",
            "commands": [{
                "name": "python",
                "module": "../outside.wasm",
                "content_hash": sinomo_runtime::sandbox::sandbox_module_hash(b"outside")
            }],
            "artifacts": []
        })
        .to_string(),
    )
    .expect("package");
    let toolchain_store =
        sinomo_runtime::sandbox::LocalSandboxToolchainStore::new(root.join("cache"));

    let error = sandbox_toolchain_import_package_with_store(
        SandboxToolchainImportPackageOptions {
            package_path: package_path.clone(),
            target_toolchain_id: None,
            execution: SandboxMetadataExecution::Local,
        },
        &toolchain_store,
    )
    .expect_err("path escape denied");

    match error {
        SandboxCliError::Parse { detail } => assert!(detail.contains("cannot escape")),
        other => panic!("unexpected error: {other:?}"),
    }
    assert!(matches!(
        toolchain_store.load_manifest(&toolchain_id),
        Err(sinomo_runtime::sandbox::SandboxToolchainError::NotFound { .. })
    ));
}

#[test]
fn sandbox_toolchain_import_package_rejects_absolute_module_path() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-sandbox-package-absolute-module");
    let toolchain_id = sinomo_runtime::sandbox::SandboxToolchainId::generate();
    let package_path = root.join("package.json");
    std::fs::write(
        &package_path,
        serde_json::json!({
            "toolchain_id": toolchain_id,
            "version": "bad-package",
            "provenance": "unit test",
            "commands": [{
                "name": "python",
                "module": "/tmp/outside.wasm",
                "content_hash": sinomo_runtime::sandbox::sandbox_module_hash(b"outside")
            }],
            "artifacts": []
        })
        .to_string(),
    )
    .expect("package");
    let toolchain_store =
        sinomo_runtime::sandbox::LocalSandboxToolchainStore::new(root.join("cache"));

    let error = sandbox_toolchain_import_package_with_store(
        SandboxToolchainImportPackageOptions {
            package_path: package_path.clone(),
            target_toolchain_id: None,
            execution: SandboxMetadataExecution::Local,
        },
        &toolchain_store,
    )
    .expect_err("absolute module path denied");

    match error {
        SandboxCliError::Parse { detail } => assert!(detail.contains("must be relative")),
        other => panic!("unexpected error: {other:?}"),
    }
    assert!(matches!(
        toolchain_store.load_manifest(&toolchain_id),
        Err(sinomo_runtime::sandbox::SandboxToolchainError::NotFound { .. })
    ));
}

#[test]
fn sandbox_toolchain_import_package_rejects_artifact_file_path_escape() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-sandbox-package-artifact-escape");
    std::fs::create_dir_all(root.join("bin")).expect("bin");
    std::fs::write(root.join("bin/python.wasm"), b"python wasm").expect("module");
    let toolchain_id = sinomo_runtime::sandbox::SandboxToolchainId::generate();
    let package_path = root.join("package.json");
    std::fs::write(
        &package_path,
        serde_json::json!({
            "toolchain_id": toolchain_id,
            "version": "bad-package",
            "provenance": "unit test",
            "commands": [{
                "name": "python",
                "module": "bin/python.wasm",
                "content_hash": sinomo_runtime::sandbox::sandbox_module_hash(b"python wasm")
            }],
            "artifacts": [{
                "relative_path": "share/python/stdlib.zip",
                "file": "../stdlib.zip",
                "content_hash": sinomo_runtime::sandbox::sandbox_module_hash(b"stdlib")
            }]
        })
        .to_string(),
    )
    .expect("package");
    let toolchain_store =
        sinomo_runtime::sandbox::LocalSandboxToolchainStore::new(root.join("cache"));

    let error = sandbox_toolchain_import_package_with_store(
        SandboxToolchainImportPackageOptions {
            package_path: package_path.clone(),
            target_toolchain_id: None,
            execution: SandboxMetadataExecution::Local,
        },
        &toolchain_store,
    )
    .expect_err("artifact file path escape denied");

    match error {
        SandboxCliError::Parse { detail } => assert!(detail.contains("cannot escape")),
        other => panic!("unexpected error: {other:?}"),
    }
    assert!(matches!(
        toolchain_store.load_manifest(&toolchain_id),
        Err(sinomo_runtime::sandbox::SandboxToolchainError::NotFound { .. })
    ));
}

#[test]
fn sandbox_toolchain_import_package_rejects_invalid_artifact_relative_path() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-sandbox-package-artifact-destination");
    std::fs::create_dir_all(root.join("bin")).expect("bin");
    std::fs::create_dir_all(root.join("data")).expect("data");
    std::fs::write(root.join("bin/python.wasm"), b"python wasm").expect("module");
    std::fs::write(root.join("data/stdlib.zip"), b"stdlib").expect("artifact");
    let toolchain_id = sinomo_runtime::sandbox::SandboxToolchainId::generate();
    let package_path = root.join("package.json");
    std::fs::write(
        &package_path,
        serde_json::json!({
            "toolchain_id": toolchain_id,
            "version": "bad-package",
            "provenance": "unit test",
            "commands": [{
                "name": "python",
                "module": "bin/python.wasm",
                "content_hash": sinomo_runtime::sandbox::sandbox_module_hash(b"python wasm")
            }],
            "artifacts": [{
                "relative_path": "../stdlib.zip",
                "file": "data/stdlib.zip",
                "content_hash": sinomo_runtime::sandbox::sandbox_module_hash(b"stdlib")
            }]
        })
        .to_string(),
    )
    .expect("package");
    let toolchain_store =
        sinomo_runtime::sandbox::LocalSandboxToolchainStore::new(root.join("cache"));

    let error = sandbox_toolchain_import_package_with_store(
        SandboxToolchainImportPackageOptions {
            package_path: package_path.clone(),
            target_toolchain_id: None,
            execution: SandboxMetadataExecution::Local,
        },
        &toolchain_store,
    )
    .expect_err("invalid artifact destination path denied");

    match error {
        SandboxCliError::Parse { detail } => {
            assert!(detail.contains("invalid sandbox toolchain artifact path"))
        }
        other => panic!("unexpected error: {other:?}"),
    }
    assert!(matches!(
        toolchain_store.load_manifest(&toolchain_id),
        Err(sinomo_runtime::sandbox::SandboxToolchainError::NotFound { .. })
    ));
}

#[test]
fn sandbox_toolchain_import_package_rejects_hash_mismatch() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-sandbox-package-hash");
    std::fs::create_dir_all(root.join("bin")).expect("bin");
    std::fs::write(root.join("bin/python.wasm"), b"tampered wasm").expect("module");
    let toolchain_id = sinomo_runtime::sandbox::SandboxToolchainId::generate();
    let package_path = root.join("package.json");
    std::fs::write(
        &package_path,
        serde_json::json!({
            "toolchain_id": toolchain_id,
            "version": "bad-package",
            "provenance": "unit test",
            "commands": [{
                "name": "python",
                "module": "bin/python.wasm",
                "content_hash": sinomo_runtime::sandbox::sandbox_module_hash(b"expected wasm")
            }],
            "artifacts": []
        })
        .to_string(),
    )
    .expect("package");
    let toolchain_store =
        sinomo_runtime::sandbox::LocalSandboxToolchainStore::new(root.join("cache"));

    let error = sandbox_toolchain_import_package_with_store(
        SandboxToolchainImportPackageOptions {
            package_path: package_path.clone(),
            target_toolchain_id: None,
            execution: SandboxMetadataExecution::Local,
        },
        &toolchain_store,
    )
    .expect_err("hash mismatch denied");

    match error {
        SandboxCliError::Parse { detail } => assert!(detail.contains("hash mismatch")),
        other => panic!("unexpected error: {other:?}"),
    }
    assert!(matches!(
        toolchain_store.load_manifest(&toolchain_id),
        Err(sinomo_runtime::sandbox::SandboxToolchainError::NotFound { .. })
    ));
}

#[test]
fn sandbox_toolchain_import_package_validates_before_writing_manifest() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-sandbox-package-duplicate");
    std::fs::create_dir_all(root.join("bin")).expect("bin");
    std::fs::write(root.join("bin/python.wasm"), b"python wasm").expect("module");
    let toolchain_id = sinomo_runtime::sandbox::SandboxToolchainId::generate();
    let package_path = root.join("package.json");
    let module_hash = sinomo_runtime::sandbox::sandbox_module_hash(b"python wasm");
    std::fs::write(
        &package_path,
        serde_json::json!({
            "toolchain_id": toolchain_id,
            "version": "bad-package",
            "provenance": "unit test",
            "commands": [
                {
                    "name": "python",
                    "module": "bin/python.wasm",
                    "content_hash": module_hash
                },
                {
                    "name": "python",
                    "module": "bin/python.wasm",
                    "content_hash": module_hash
                }
            ],
            "artifacts": []
        })
        .to_string(),
    )
    .expect("package");
    let toolchain_store =
        sinomo_runtime::sandbox::LocalSandboxToolchainStore::new(root.join("cache"));

    let error = sandbox_toolchain_import_package_with_store(
        SandboxToolchainImportPackageOptions {
            package_path: package_path.clone(),
            target_toolchain_id: None,
            execution: SandboxMetadataExecution::Local,
        },
        &toolchain_store,
    )
    .expect_err("duplicate command denied");

    match error {
        SandboxCliError::Parse { detail } => assert!(detail.contains("duplicate")),
        other => panic!("unexpected error: {other:?}"),
    }
    assert!(matches!(
        toolchain_store.load_manifest(&toolchain_id),
        Err(sinomo_runtime::sandbox::SandboxToolchainError::NotFound { .. })
    ));
}

#[test]
fn sandbox_toolchain_import_busybox_installs_core_applets() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-sandbox-busybox-import");
    let module_path = root.join("busybox.wasm");
    std::fs::create_dir_all(&root).expect("root");
    std::fs::write(
        &module_path,
        r#"
        (module
          (import "wasi_snapshot_preview1" "fd_write"
            (func $fd_write (param i32 i32 i32 i32) (result i32)))
          (memory (export "memory") 1 128)
          (data (i32.const 0) "\08\00\00\00\06\00\00\00hello\0a")
          (func (export "_start")
            i32.const 1
            i32.const 0
            i32.const 1
            i32.const 20
            call $fd_write
            drop))
        "#,
    )
    .expect("module");
    let toolchain_store =
        sinomo_runtime::sandbox::LocalSandboxToolchainStore::new(root.join("cache"));

    sandbox_toolchain_import_busybox_with_store(
        SandboxToolchainImportBusyBoxOptions {
            version: "busybox-wasi-test".to_string(),
            provenance: "unit test build".to_string(),
            module_path: module_path.clone(),
            execution: SandboxMetadataExecution::Local,
        },
        &toolchain_store,
    )
    .expect("import busybox");

    let toolchain_id = sinomo_runtime::sandbox::SandboxToolchainId::from_legacy_named(
        sinomo_runtime::sandbox::BUSYBOX_TOOLCHAIN_LEGACY_ID,
    )
    .expect("toolchain");
    let (manifest, _layout) = toolchain_store
        .load_manifest(&toolchain_id)
        .expect("manifest");
    assert_eq!(manifest.version, "busybox-wasi-test");
    for command in ["cat", "grep", "find", "printf", "sha256sum"] {
        assert!(manifest.command(command).is_ok(), "missing {command}");
        assert_eq!(
            toolchain_store
                .load_command_module(&manifest, command)
                .expect("module"),
            std::fs::read(&module_path).expect("module bytes")
        );
    }
    assert_eq!(
        manifest.commands.len(),
        sinomo_runtime::sandbox::BUSYBOX_TOOLCHAIN_COMMANDS.len()
    );
}

#[test]
fn sandbox_run_applies_toolchain_command_default_limits() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-sandbox-command-limits");
    let store = sinomo_runtime::sandbox::LocalSandboxInstanceStore::new(root.join("runtime"));
    let toolchain_store =
        sinomo_runtime::sandbox::LocalSandboxToolchainStore::new(root.join("cache"));
    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();
    let toolchain_id = sinomo_runtime::sandbox::SandboxToolchainId::generate();
    let mut toolchain = sinomo_runtime::sandbox::SandboxToolchainManifest::new(
        toolchain_id.clone(),
        "test-1",
        sinomo_runtime::sandbox::SandboxWasiAbi::Wasix,
        "test module",
        OffsetDateTime::now_utc(),
    )
    .expect("toolchain");
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "limited",
            r#"
            (module
              (import "wasi_snapshot_preview1" "fd_write"
                (func $fd_write (param i32 i32 i32 i32) (result i32)))
              (memory (export "memory") 1 128)
              (data (i32.const 0) "\08\00\00\00\06\00\00\00hello\0a")
              (func (export "_start")
                i32.const 1
                i32.const 0
                i32.const 1
                i32.const 20
                call $fd_write
                drop))
            "#
            .as_bytes(),
            sinomo_runtime::sandbox::SandboxResourceLimits {
                max_output_bytes: 4,
                ..sinomo_runtime::sandbox::SandboxResourceLimits::default()
            },
        )
        .expect("put command");
    sandbox_create_with_store(
        SandboxCreateOptions {
            sandbox_id: Some(sandbox_id.clone()),
            toolchain_id,
            temp_policy: sinomo_runtime::sandbox::SandboxTempPolicy::ClearOnSessionStart,
            memory_mb: None,
            wall_time_ms: None,
            max_input_bytes: None,
            max_output_bytes: None,
            max_workspace_bytes: None,
            max_written_bytes_per_command: None,
            max_file_count: None,
            mounts: Vec::new(),
            execution: SandboxMetadataExecution::Local,
        },
        &store,
    )
    .expect("create");

    let error = sandbox_run_with_stores(
        SandboxRunOptions {
            sandbox_id,
            target: SandboxRunTarget::ToolchainCommand("limited".to_string()),
            argv: vec!["limited".to_string()],
            timeout_ms: 1_000,
            stdin: false,
            allow_unsigned: true,
            allowed_capabilities: Vec::new(),
            execution: SandboxRunExecution::Local,
        },
        &store,
        &toolchain_store,
    )
    .expect_err("command output limit");

    assert!(matches!(error, SandboxCliError::Parse { .. }));
}

fn wasi_stdin_to_stdout_module() -> &'static str {
    r#"
    (module
      (import "wasi_snapshot_preview1" "fd_read"
        (func $fd_read (param i32 i32 i32 i32) (result i32)))
      (import "wasi_snapshot_preview1" "fd_write"
        (func $fd_write (param i32 i32 i32 i32) (result i32)))
      (import "wasi_snapshot_preview1" "proc_exit"
        (func $proc_exit (param i32)))
      (memory (export "memory") 1 128)
      (data (i32.const 0) "\80\00\00\00\40\00\00\00")
      (func $exit_if_errno (param $errno i32)
        local.get $errno
        i32.eqz
        if
        else
          local.get $errno
          call $proc_exit
        end)
      (func (export "_start")
        i32.const 0
        i32.const 0
        i32.const 1
        i32.const 20
        call $fd_read
        call $exit_if_errno
        i32.const 4
        i32.const 20
        i32.load
        i32.store
        i32.const 1
        i32.const 0
        i32.const 1
        i32.const 20
        call $fd_write
        call $exit_if_errno))
    "#
}
#[test]
fn agent_run_parser_accepts_provider_model_account_and_prompt() {
    assert_eq!(
        super::parse_agent_run_options(&[
            "--provider".to_string(),
            "codex".to_string(),
            "--model".to_string(),
            "gpt-5.2-codex".to_string(),
            "--account".to_string(),
            "work".to_string(),
            "--prompt".to_string(),
            "check the cluster".to_string(),
        ])
        .expect("agent run"),
        AgentRunOptions {
            prompt: "check the cluster".to_string(),
            provider: "codex".to_string(),
            model: "gpt-5.2-codex".to_string(),
            account: Some("work".to_string()),
        }
    );
}

#[test]
fn agent_inspect_parser_accepts_agent_and_json() {
    assert_eq!(
        super::parse_agent_inspect_options(&[
            "--agent".to_string(),
            "evolution-agent".to_string(),
            "--json".to_string(),
        ])
        .expect("agent inspect"),
        AgentInspectOptions {
            agent: "evolution-agent".to_string(),
            json: true,
        }
    );
    assert_eq!(
        super::parse_agent_inspect_options(&[]).expect("default agent inspect"),
        AgentInspectOptions {
            agent: "main-agent".to_string(),
            json: false,
        }
    );
}

#[test]
fn agent_export_import_validate_parsers_accept_package_artifacts() {
    assert_eq!(
        super::parse_agent_export_options(&[
            "coding-agent".to_string(),
            "--output".to_string(),
            "coding-agent.json".to_string(),
        ])
        .expect("agent export"),
        AgentExportOptions {
            agent: "coding-agent".to_string(),
            output: Some(PathBuf::from("coding-agent.json")),
        }
    );
    assert_eq!(
        super::parse_agent_export_options(&[]).expect("default export"),
        AgentExportOptions {
            agent: "main-agent".to_string(),
            output: None,
        }
    );
    assert_eq!(
        super::parse_agent_import_options(&[
            "--file".to_string(),
            "coding-agent.json".to_string(),
            "--json".to_string(),
        ])
        .expect("agent import"),
        AgentImportOptions {
            path: PathBuf::from("coding-agent.json"),
            json: true,
        }
    );
    assert_eq!(
        super::parse_agent_validate_options(&[
            "--file".to_string(),
            "coding-agent.json".to_string(),
            "--json".to_string(),
        ])
        .expect("agent validate file"),
        AgentValidateOptions {
            agent: "main-agent".to_string(),
            file: Some(PathBuf::from("coding-agent.json")),
            json: true,
        }
    );
    assert_eq!(
        super::parse_agent_validate_options(&["coding-agent".to_string()])
            .expect("agent validate catalog"),
        AgentValidateOptions {
            agent: "coding-agent".to_string(),
            file: None,
            json: false,
        }
    );
}

#[test]
fn agent_package_json_artifact_loads_and_validates() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-agent-package-json");
    let path = root.join("coding-agent.json");
    let definition = sinomo_agent::AgentDefinition::coding()
        .with_skill("rust.coding")
        .with_package_version("2.0.0");
    std::fs::write(
        &path,
        serde_json::to_vec_pretty(&definition).expect("definition json"),
    )
    .expect("write definition");

    let loaded = super::read_agent_package_definition(&path).expect("read package");

    loaded.validate().expect("valid package");
    assert_eq!(loaded.key(), "coding-agent");
    assert_eq!(loaded.package.version, "2.0.0");
    assert_eq!(loaded.skills, vec!["rust.coding".to_string()]);
}

#[test]
fn agent_readiness_parser_accepts_json() {
    assert_eq!(
        super::parse_agent_readiness_options(&["--json".to_string()]).expect("agent readiness"),
        AgentReadinessOptions { json: true }
    );
    assert_eq!(
        super::parse_agent_readiness_options(&[]).expect("default readiness"),
        AgentReadinessOptions { json: false }
    );
}

#[test]
fn agent_session_parsers_accept_inspect_progress_results_and_control() {
    let session_id = SessionId::from_legacy_named("sess_agent_session_parser").expect("session");
    let task_id = sinomo_agent::TaskId::generate();
    let event_id = sinomo_agent::TaskId::generate();

    assert_eq!(
        super::parse_agent_session_command(&[
            "inspect".to_string(),
            "sess_agent_session_parser".to_string(),
            "--json".to_string(),
        ])
        .expect("inspect"),
        AgentSessionCliCommand::Inspect(AgentSessionInspectOptions {
            session_id: session_id.clone(),
            json: true,
        })
    );
    assert_eq!(
        super::parse_agent_session_command(&[
            "progress".to_string(),
            "sess_agent_session_parser".to_string(),
            "--cursor".to_string(),
            "7".to_string(),
            "--last-event".to_string(),
            event_id.to_string(),
            "--json".to_string(),
        ])
        .expect("progress"),
        AgentSessionCliCommand::Progress(AgentSessionProgressOptions {
            session_id: session_id.clone(),
            cursor: Some(sinomo_agent::AgentSessionControlEventCursor {
                offset: 7,
                last_event_id: Some(event_id),
            }),
            json: true,
        })
    );
    assert_eq!(
        super::parse_agent_session_command(&[
            "terminal-results".to_string(),
            "sess_agent_session_parser".to_string(),
            "--task".to_string(),
            task_id.to_string(),
        ])
        .expect("terminal results"),
        AgentSessionCliCommand::TerminalResults(AgentSessionTerminalResultsOptions {
            session_id: session_id.clone(),
            task_id: Some(task_id),
            json: false,
        })
    );
    assert_eq!(
        super::parse_agent_session_command(&[
            "steer".to_string(),
            "sess_agent_session_parser".to_string(),
            "keep".to_string(),
            "the".to_string(),
            "constraint".to_string(),
            "--json".to_string(),
        ])
        .expect("steer"),
        AgentSessionCliCommand::Steer(AgentSessionMessageOptions {
            session_id: session_id.clone(),
            message: "keep the constraint".to_string(),
            json: true,
        })
    );
    assert_eq!(
        super::parse_agent_session_command(&[
            "follow-up".to_string(),
            "--session".to_string(),
            "sess_agent_session_parser".to_string(),
            "--message".to_string(),
            "one more thing".to_string(),
        ])
        .expect("follow-up"),
        AgentSessionCliCommand::FollowUp(AgentSessionMessageOptions {
            session_id: session_id.clone(),
            message: "one more thing".to_string(),
            json: false,
        })
    );
    assert_eq!(
        super::parse_agent_session_command(&[
            "interrupt".to_string(),
            "sess_agent_session_parser".to_string(),
            "--reason".to_string(),
            "stop now".to_string(),
        ])
        .expect("interrupt"),
        AgentSessionCliCommand::Interrupt(AgentSessionMessageOptions {
            session_id: session_id.clone(),
            message: "stop now".to_string(),
            json: false,
        })
    );
    assert_eq!(
        super::parse_agent_session_command(&[
            "resume".to_string(),
            "sess_agent_session_parser".to_string(),
            "--json".to_string(),
        ])
        .expect("resume"),
        AgentSessionCliCommand::Resume(AgentSessionResumeOptions {
            session_id,
            json: true,
        })
    );
}

#[test]
fn agent_session_text_renderers_summarize_persisted_state() {
    let session_id = SessionId::from_legacy_named("sess_agent_session_text").expect("session");
    let event_id =
        sinomo_agent::TaskId::from_legacy_named("event_agent_session_text").expect("event id");
    let session =
        sinomo_agent::AgentSession::new(session_id.clone(), sinomo_agent::AgentRole::Main);
    let inspect = sinomo_agent::AgentSessionInspectResponse {
        session_id: session_id.clone(),
        session: Some(session),
        control_snapshot: None,
        event_cursor: sinomo_agent::AgentSessionControlEventCursor {
            offset: 4,
            last_event_id: Some(event_id.clone()),
        },
        terminal_results: Vec::new(),
        persisted_event_count: 4,
    };

    let text = super::agent_session_inspection_text(&inspect);

    assert!(text.contains("Agent session:"));
    assert!(text.contains("role: Main"));
    assert!(text.contains("persisted events: 4"));
    assert!(text.contains(&format!("event cursor: offset 4, last event {event_id}")));
    assert!(text.contains("control snapshot: not found"));

    let progress = sinomo_agent::AgentSessionProgressResponse {
        session_id: session_id.clone(),
        batch: sinomo_agent::AgentSessionControlEventBatch {
            cursor: sinomo_agent::AgentSessionControlEventCursor {
                offset: 4,
                last_event_id: Some(event_id.clone()),
            },
            next_cursor: sinomo_agent::AgentSessionControlEventCursor {
                offset: 4,
                last_event_id: Some(event_id.clone()),
            },
            events: Vec::new(),
            task_updates: Vec::new(),
            terminal_results: Vec::new(),
        },
    };
    let progress_text = super::agent_session_progress_text(&progress);
    assert!(progress_text.contains(&format!(
        "cursor: offset 4, last event {event_id} -> offset 4, last event {event_id}"
    )));

    let resume = sinomo_agent::AgentSessionResumeResponse {
        session_id,
        accepted: false,
        runtime_attached: false,
        session: None,
        control_snapshot: None,
        event_cursor: sinomo_agent::AgentSessionControlEventCursor {
            offset: 4,
            last_event_id: Some(event_id.clone()),
        },
        reattach: sinomo_agent::AgentSessionReattachReport {
            state: sinomo_agent::AgentSessionReattachState::PersistedMetadataOnly,
            active_turn_persisted: false,
            running_task_count: 0,
            persisted_event_count: 4,
            reason: "readiness resume fixture".to_string(),
        },
        detail: None,
    };
    let resume_text = super::agent_session_resume_text(&resume);
    assert!(resume_text.contains(&format!("event cursor: offset 4, last event {event_id}")));
}

#[test]
fn agent_inspection_renders_package_surface_and_prompt() {
    let mut definition = sinomo_agent::AgentDefinition::coding()
        .with_skill("rust.coding")
        .with_package_version("2.0.0");
    definition.temperature = Some(0.3);
    definition.max_output_tokens = Some(321);

    let text = super::agent_package_inspection_text(&definition);
    let json = super::agent_package_inspection_json(&definition);

    assert!(text.contains("Agent package: coding-agent"));
    assert!(text.contains("version: 2.0.0"));
    assert!(text.contains("skills: rust.coding"));
    assert!(text.contains("context:"));
    assert!(text.contains("compaction:"));
    assert!(text.contains("provider preferences:"));
    assert!(text.contains("temperature: 0.3"));
    assert!(text.contains("max output tokens: 321"));
    assert!(text.contains("runtime limits:"));
    assert!(text.contains("max steps: unbounded"));
    assert!(text.contains("max tool calls per step: unbounded"));
    assert!(text.contains("max model repair attempts: unbounded"));
    assert!(text.contains("system prompt:"));
    assert_eq!(json["id"], "coding-agent");
    assert_eq!(json["package"]["version"], "2.0.0");
    assert_eq!(json["skills"][0], "rust.coding");
    let temperature = json["effective_model_policy"]["temperature"]
        .as_f64()
        .expect("temperature");
    assert!((temperature - 0.3).abs() < 0.000001);
    assert_eq!(json["effective_model_policy"]["max_output_tokens"], 321);

    definition.runtime_limits.max_steps = Some(12);
    definition.runtime_limits.max_tool_calls_per_step = Some(3);
    definition.runtime_limits.max_model_repair_attempts = Some(2);
    let capped_text = super::agent_package_inspection_text(&definition);
    assert!(capped_text.contains("max steps: 12"));
    assert!(capped_text.contains("max tool calls per step: 3"));
    assert!(capped_text.contains("max model repair attempts: 2"));
}

#[test]
fn agent_readiness_text_renders_gate_results() {
    let report = sinomo_agent::run_local_agent_readiness_gate(env!("CARGO_MANIFEST_DIR"));
    let text = super::agent_readiness_text(&report);

    assert!(report.is_ready(), "{report:#?}");
    assert!(text.contains("Agent readiness: Ready"));
    assert!(text.contains("[pass] built_in_package_validation"));
    assert!(text.contains("[pass] package_rejects_malformed_definitions"));
    assert!(text.contains("[pass] local_command_evidence_smoke"));
}

#[test]
fn cluster_repair_members_parser_accepts_peer_and_promotion() {
    assert_eq!(
        super::parse_cluster_command(&[
            "repair-members".to_string(),
            "--from".to_string(),
            "10.0.0.2:21353".to_string(),
            "--promote".to_string(),
            "01kr3jcdha49k8z4qdp9n3wvr8".to_string(),
        ])
        .expect("repair members"),
        ClusterCliCommand::RepairMembers(super::ClusterRepairMembersOptions {
            from: "10.0.0.2:21353".to_string(),
            promote: Some(NodeId::new("01kr3jcdha49k8z4qdp9n3wvr8".to_string()).expect("node")),
        })
    );
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
fn cluster_join_timeout_covers_registration_work_and_stays_capped() {
    assert!(super::DAEMON_JOIN_TIMEOUT >= Duration::from_secs(3));
    assert!(super::DAEMON_JOIN_TIMEOUT <= Duration::from_secs(10));
    assert!(super::IROH_DAEMON_JOIN_TIMEOUT >= Duration::from_secs(3));
    assert!(super::IROH_DAEMON_JOIN_TIMEOUT <= Duration::from_secs(5));
}

#[test]
fn cluster_sync_targets_skip_remote_local_only_endpoints() {
    let cluster_id = ClusterId::from_legacy_named("cluster_sync_targets").expect("cluster");
    let local_node = NodeId::from_legacy_named("node_sync_local").expect("node");
    let remote_node = NodeId::from_legacy_named("node_sync_remote").expect("node");
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
fn default_invite_compaction_keeps_iroh_fallback() {
    let node_id = NodeId::from_legacy_named("node_invite_compact").expect("node");
    let mut endpoints = vec![
        daemon_lan_endpoint(&node_id, "192.0.2.11:21353".to_string(), 0),
        daemon_lan_endpoint(&node_id, "192.0.2.12:21353".to_string(), 1),
        daemon_lan_endpoint(&node_id, "192.0.2.13:21353".to_string(), 2),
        ClusterEndpointRecord {
            endpoint_id: ClusterEndpointId::from_legacy_named("endpoint_invite_compact_iroh")
                .expect("endpoint"),
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
    assert_eq!(endpoints[0].address, "192.0.2.11:21353");
    assert_eq!(endpoints[1].address, "192.0.2.12:21353");
    assert_eq!(endpoints[2].transport, ClusterTransportKind::Iroh);
}

#[test]
fn invite_introducers_keep_fresh_iroh_endpoint_for_same_key() {
    let node_id = NodeId::from_legacy_named("node_invite_iroh_fresh").expect("node");
    let fresh = ClusterEndpointRecord {
        endpoint_id: ClusterEndpointId::from_legacy_named("endpoint_invite_iroh_fresh")
            .expect("endpoint"),
        node_id: node_id.clone(),
        transport: ClusterTransportKind::Iroh,
        address: "iroh://same_key?direct=192.0.2.11:32001&relay=https://relay.example/".to_string(),
        source: ClusterEndpointSource::Rendezvous,
        privacy: ClusterEndpointPrivacy::InternetDirect,
        priority: 0,
        last_observed_latency_ms: None,
    };
    let stale = ClusterEndpointRecord {
        endpoint_id: ClusterEndpointId::from_legacy_named("endpoint_invite_iroh_stale")
            .expect("endpoint"),
        node_id,
        transport: ClusterTransportKind::Iroh,
        address: "iroh://same_key?direct=192.0.2.11:32002&relay=https://relay.example/".to_string(),
        source: ClusterEndpointSource::KvMembership,
        privacy: ClusterEndpointPrivacy::InternetDirect,
        priority: 0,
        last_observed_latency_ms: None,
    };
    let mut endpoints = Vec::new();

    push_unique_introducer(&mut endpoints, fresh);
    push_unique_introducer(&mut endpoints, stale);

    assert_eq!(endpoints.len(), 1);
    assert!(endpoints[0].address.contains("32001"));
}

#[test]
fn cluster_join_registration_works_over_iroh() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-iroh-join");
    let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
    paths.ensure().expect("cluster paths");
    let cluster_id = ClusterId::from_legacy_named("cluster_iroh_join").expect("cluster");
    let responder_node_id = NodeId::from_legacy_named("node_iroh_join_responder").expect("node");
    let responder_identity =
        super::load_or_create_main_node_identity(&root).expect("responder identity");
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake iroh join daemon");
    let daemon_address = listener
        .local_addr()
        .expect("fake iroh join daemon address")
        .to_string();
    let invite_id = ClusterInviteId::from_legacy_named("invite_iroh_join").expect("invite");
    let response_state = ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "iroh join",
        responder_node_id.clone(),
        "responder",
        responder_identity.public_key.clone(),
        OffsetDateTime::now_utc(),
    );
    let fixture_identity = responder_identity.clone();
    let fixture_invite_id = invite_id.clone();
    let fixture_cluster_id = cluster_id.clone();
    let fixture_node_id = responder_node_id.clone();
    let fixture_response_state = response_state.clone();
    let fixture_daemon_address = daemon_address.clone();
    let fixture = thread::spawn(move || {
        listener
            .set_nonblocking(true)
            .expect("fake iroh join daemon nonblocking listener");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("fake iroh join daemon runtime");
        runtime.block_on(async move {
            let listener =
                tokio::net::TcpListener::from_std(listener).expect("async fake iroh join daemon");
            for index in 0..2 {
                let (stream, _) = listener
                    .accept()
                    .await
                    .expect("accept fake iroh join daemon");
                let mut stream = sinomo_runtime::network::AsyncDaemonControlStream::new(stream);
                let request = stream
                    .read_request(
                        &fixture_daemon_address,
                        sinomo_runtime::network::IROH_DAEMON_COMMAND_MAX_BYTES,
                        sinomo_runtime::network::ObjectFrameIoOptions::none(),
                    )
                    .await
                    .expect("read fake iroh join command");
                let request_line = request.command;
                if index == 0 {
                    assert_eq!(
                        request_line,
                        super::daemon_rpc_command(
                            sinomo_runtime::network::CLUSTER_JOIN_CHALLENGE_ROUTE
                        )
                    );
                    let rpc = request.rpc.expect("challenge rpc");
                    assert_eq!(
                        rpc.route,
                        sinomo_runtime::network::CLUSTER_JOIN_CHALLENGE_ROUTE
                    );
                    let challenge: super::ClusterInviteChallengeRequest =
                        rpc.decode_body().expect("challenge request");
                    assert_eq!(challenge.invite_id, fixture_invite_id);
                    let timestamp = OffsetDateTime::now_utc()
                        .format(&time::format_description::well_known::Rfc3339)
                        .expect("timestamp");
                    let signing_payload = format!(
                        "sinomo.cluster-invite-challenge.v1\n{}\n{}\n{}\n{}\n{}\n{}",
                        challenge.invite_id,
                        fixture_cluster_id,
                        fixture_node_id,
                        fixture_identity.public_key,
                        challenge.challenge,
                        timestamp
                    );
                    let signature = fixture_identity
                        .key_pair()
                        .expect("responder key pair")
                        .sign(signing_payload.as_bytes());
                    let proof = super::ClusterInviteChallengeProof {
                        protocol: "sinomo.cluster-invite-challenge.v1".to_string(),
                        invite_id: challenge.invite_id,
                        cluster_id: fixture_cluster_id.clone(),
                        responder_node_id: fixture_node_id.to_string(),
                        responder_public_key: fixture_identity.public_key.clone(),
                        challenge: challenge.challenge,
                        timestamp,
                        signature: super::URL_SAFE_NO_PAD.encode(signature.as_ref()),
                    };
                    let rpc_response = sinomo_runtime::network::ObjectRpcResponse::encode_body(
                        sinomo_runtime::network::CLUSTER_JOIN_CHALLENGE_ROUTE,
                        &proof,
                    )
                    .expect("proof");
                    stream
                        .write_response(
                            &fixture_daemon_address,
                            &sinomo_runtime::network::DaemonControlResponse::rpc(
                                String::new(),
                                rpc_response,
                            ),
                            sinomo_runtime::network::ObjectFrameIoOptions::none(),
                        )
                        .await
                        .expect("write proof");
                } else {
                    assert_eq!(
                        request_line,
                        super::daemon_rpc_command(
                            sinomo_runtime::network::CLUSTER_JOIN_REGISTER_ROUTE
                        )
                    );
                    let auth: super::SignedDaemonCommand =
                        sinomo_runtime::network::SinomoObjectCodec::default()
                            .decode(request.auth.as_deref().expect("join auth"))
                            .expect("decode join auth");
                    assert_eq!(auth.command, request_line);
                    let rpc = request.rpc.expect("join rpc");
                    assert_eq!(
                        rpc.route,
                        sinomo_runtime::network::CLUSTER_JOIN_REGISTER_ROUTE
                    );
                    let response = super::ClusterJoinRegistrationResponse {
                        accepted: true,
                        detail: "join registered".to_string(),
                        state: fixture_response_state.clone(),
                        kv_snapshot: None,
                    };
                    let rpc_response = sinomo_runtime::network::ObjectRpcResponse::encode_body(
                        sinomo_runtime::network::CLUSTER_JOIN_REGISTER_ROUTE,
                        &response,
                    )
                    .expect("response");
                    stream
                        .write_response(
                            &fixture_daemon_address,
                            &sinomo_runtime::network::DaemonControlResponse::rpc(
                                String::new(),
                                rpc_response,
                            ),
                            sinomo_runtime::network::ObjectFrameIoOptions::none(),
                        )
                        .await
                        .expect("write join response");
                }
            }
        });
    });
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("iroh proxy runtime");
    let proxy = runtime
        .block_on(sinomo_runtime::network::start_iroh_daemon_proxy(
            runtime.handle().clone(),
            paths.clone(),
            cluster_id.clone(),
            responder_node_id,
            daemon_address,
        ))
        .expect("start iroh daemon proxy");
    let invite = ClusterJoinInvite {
        invite_id: invite_id.clone(),
        cluster_id: ClusterId::new("01hxy6qa9k0t9vr4g9n8d3q5c0").expect("pending cluster"),
        cluster_name: None,
        cluster_public_key: responder_identity.public_key.clone(),
        invitation_secret: "secret_iroh_join".to_string(),
        communication_channels: vec![sinomo_runtime::core::ClusterInvitationChannel {
            endpoint: proxy.endpoint(),
            node_public_key: responder_identity.public_key.clone(),
        }],
        introducers: Vec::new(),
        token_id: String::new(),
        expires_at: OffsetDateTime::UNIX_EPOCH,
        trust_domain: TrustDomain::new("trust.local").expect("trust"),
        allowed_member_roles: Vec::new(),
        issuer_node_id: NodeId::from_legacy_named("node_pending_invite").expect("pending node"),
        issuer_public_key: String::new(),
        signature: String::new(),
    };
    let request = ClusterJoinRegistrationRequest {
        invitation_secret: invite.invitation_secret.clone(),
        invite,
        members: Vec::new(),
    };

    let response = send_cluster_join_registration_over_iroh(&proxy.endpoint(), &request)
        .expect("iroh join registration");

    assert!(response.accepted);
    assert_eq!(response.state.cluster_id, cluster_id);
    fixture.join().expect("fake iroh join daemon");
}

#[test]
fn cluster_state_sync_txn_removes_stale_member_endpoint_keys_before_republishing() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-endpoint-key-cleanup");
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");

    let cluster_id = ClusterId::from_legacy_named("cluster_endpoint_key_cleanup").expect("cluster");
    let node_id = NodeId::from_legacy_named("node_endpoint_key_cleanup").expect("node");
    let mut state = ClusterBootstrapState::initialized_locally(
        cluster_id,
        "endpoint cleanup",
        node_id.clone(),
        "endpoint cleanup node",
        "public-key",
        OffsetDateTime::now_utc(),
    );
    let current_endpoint = ClusterEndpointRecord {
        endpoint_id: ClusterEndpointId::from_legacy_named("endpoint_cleanup_current")
            .expect("endpoint"),
        node_id: node_id.clone(),
        transport: ClusterTransportKind::LanTcp,
        address: "192.0.2.11:21353".to_string(),
        source: ClusterEndpointSource::Static,
        privacy: ClusterEndpointPrivacy::Lan,
        priority: 20,
        last_observed_latency_ms: None,
    };
    let stale_endpoint = ClusterEndpointRecord {
        endpoint_id: ClusterEndpointId::from_legacy_named("endpoint_cleanup_stale")
            .expect("endpoint"),
        node_id: node_id.clone(),
        transport: ClusterTransportKind::LanTcp,
        address: "192.0.2.12:21353".to_string(),
        source: ClusterEndpointSource::Static,
        privacy: ClusterEndpointPrivacy::Lan,
        priority: 30,
        last_observed_latency_ms: None,
    };
    state.members[0].endpoints = vec![current_endpoint.clone()];

    let stale_key = sinomo_runtime::network::cluster_endpoint_key(&node_id, &stale_endpoint);
    let current_key = sinomo_runtime::network::cluster_endpoint_key(&node_id, &current_endpoint);
    let mut store = InProcessKvStore::new();
    store
        .put_serde(stale_key.clone(), &stale_endpoint, None)
        .expect("stale endpoint");
    sinomo_runtime::kv::write_cluster_kv_snapshot(&paths.kv_paths(), &store.snapshot())
        .expect("cluster kv snapshot");

    let txn = cluster_state_sync_txn(&paths, &state).expect("cluster state sync txn");
    let endpoint_prefix = format!("/cluster/endpoints/{node_id}/");
    assert!(
        txn.success
            .iter()
            .all(|op| !matches!(op, KvOp::DeletePrefix { prefix } if prefix == &endpoint_prefix)),
        "endpoint sync should avoid broad prefix deletes"
    );
    let stale_delete_index = txn
        .success
        .iter()
        .position(|op| matches!(op, KvOp::Delete { key } if key == &stale_key));
    let current_put_index = txn
        .success
        .iter()
        .position(|op| matches!(op, KvOp::Put { key, .. } if key == &current_key));

    assert!(stale_delete_index.is_some());
    assert!(
        stale_delete_index < current_put_index,
        "stale endpoint must be deleted before canonical endpoints are put"
    );
}

#[test]
fn cluster_node_role_update_keeps_raft_state_and_capabilities() {
    let cluster_id = ClusterId::from_legacy_named("cluster_role_update").expect("cluster");
    let node_id = NodeId::from_legacy_named("node_role_update").expect("node");
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
    let cluster_id = ClusterId::from_legacy_named("cluster_raft_role_update").expect("cluster");
    let node_id = NodeId::from_legacy_named("node_raft_role_update").expect("node");
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
    let root = sinomo_testdir::TestDir::new("sinomo-cli-local-network-expiration");
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");

    let cluster_id =
        ClusterId::from_legacy_named("cluster_local_network_expiration").expect("cluster");
    let node_id = NodeId::from_legacy_named("node_local_network_expiration").expect("node");
    let remote_node_id = NodeId::from_legacy_named("node_remote_network_expiration").expect("node");
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
            observed_addresses: vec!["192.0.2.11:21353".to_string()],
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
        endpoint_id: ClusterEndpointId::from_legacy_named("endpoint_remote").expect("endpoint"),
        transport: ClusterTransportKind::LanTcp,
        address: "192.0.2.12:21353".to_string(),
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
    sinomo_runtime::kv::write_cluster_kv_snapshot(&paths.kv_paths(), &store.snapshot())
        .expect("cluster kv snapshot");

    let snapshot =
        expire_cluster_network_records_in_kv(&paths, &state, now).expect("expire local records");

    assert!(snapshot.is_none());
    assert!(
        !paths.kv_paths().raft_wal_paths(&node_id).wal.exists(),
        "skipping local expiration must not append a raft delete transaction"
    );
}

#[test]
fn imported_daemon_snapshot_defers_volatile_only_materialized_refresh() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-imported-daemon-snapshot-defer");
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");

    let baseline = InProcessKvStore::new().snapshot();
    sinomo_runtime::kv::write_cluster_kv_snapshot(&paths.kv_paths(), &baseline)
        .expect("write baseline snapshot");
    let before_metadata = std::fs::metadata(&paths.kv_snapshot).expect("snapshot metadata");
    let before_modified = before_metadata.modified().expect("snapshot mtime");

    let cluster_id =
        ClusterId::from_legacy_named("cluster_imported_snapshot_defer").expect("cluster");
    let node_id = NodeId::from_legacy_named("node_imported_snapshot_defer").expect("node");
    let now = OffsetDateTime::now_utc();
    let info = sinomo_runtime::network::ClusterNetworkInfoRecord {
        cluster_id,
        node_id: node_id.clone(),
        environment_id: "env_imported_snapshot_defer".to_string(),
        observed_addresses: vec!["192.0.2.20:21353".to_string()],
        endpoints: Vec::new(),
        observed_at: now,
        expires_at: now + time::Duration::hours(1),
    };
    let mut imported = InProcessKvStore::restore(baseline.clone());
    imported
        .put_serde(
            sinomo_runtime::network::cluster_network_info_key(&node_id),
            &info,
            None,
        )
        .expect("volatile network info");
    let imported = imported.snapshot();
    assert!(
        imported.revision > baseline.revision,
        "test setup must create a newer imported daemon snapshot"
    );

    write_imported_cluster_kv_snapshot(&paths, &imported).expect("import daemon snapshot");

    let after_metadata = std::fs::metadata(&paths.kv_snapshot).expect("snapshot metadata");
    assert_eq!(
        before_metadata.len(),
        after_metadata.len(),
        "volatile-only daemon snapshot imports should not rewrite a fresh materialized file"
    );
    assert_eq!(
        before_modified,
        after_metadata.modified().expect("snapshot mtime"),
        "volatile-only daemon snapshot imports should leave the materialized file mtime untouched"
    );
    let cached = sinomo_runtime::kv::read_cluster_kv_cached_snapshot(&paths.kv_paths())
        .expect("cached snapshot");
    assert_eq!(
        cached.revision, baseline.revision,
        "the materialized snapshot should remain a stale-but-fresh fallback until a structural change or checkpoint interval"
    );
    assert!(
        cached
            .get(&sinomo_runtime::network::cluster_network_info_key(&node_id))
            .is_none(),
        "deferred volatile network info should stay out of the materialized fallback"
    );
}

#[test]
fn stable_connectivity_sync_does_not_rewrite_source_prefix() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-stable-connectivity-sync");
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");

    let cluster_id =
        ClusterId::from_legacy_named("cluster_stable_connectivity_sync").expect("cluster");
    let source_node_id =
        NodeId::from_legacy_named("node_stable_connectivity_source").expect("node");
    let target_node_id =
        NodeId::from_legacy_named("node_stable_connectivity_target").expect("node");
    let now = OffsetDateTime::now_utc();
    let stored = sinomo_runtime::network::ClusterConnectivityRecord {
        cluster_id,
        source_node_id: source_node_id.clone(),
        target_node_id,
        endpoint_id: ClusterEndpointId::from_legacy_named("endpoint_stable_connectivity")
            .expect("endpoint"),
        transport: ClusterTransportKind::LanTcp,
        address: "192.0.2.12:21353".to_string(),
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
    sinomo_runtime::kv::write_cluster_kv_snapshot(&paths.kv_paths(), &store.snapshot())
        .expect("cluster kv snapshot");

    let ops = cluster_connectivity_sync_ops(&paths, &source_node_id, &[incoming])
        .expect("connectivity sync ops");

    assert!(ops.is_empty());
}

#[test]
fn daemon_advertised_endpoints_use_route_addresses_for_known_peers() {
    let cluster_id = ClusterId::from_legacy_named("cluster_route_advertise").expect("cluster");
    let local_node = NodeId::from_legacy_named("node_route_local").expect("node");
    let remote_node = NodeId::from_legacy_named("node_route_remote").expect("node");
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
        "192.0.2.11:21353".to_string(),
        20,
    )];
    state.members.push(remote);

    let route_targets = advertised_peer_route_targets(&state);
    assert_eq!(route_targets.len(), 1);
    assert_eq!(route_targets[0].to_string(), "192.0.2.11:21353");

    let endpoints = daemon_advertised_endpoints_for_state(
        &state,
        &local_node,
        &DaemonRunOptions {
            host: "0.0.0.0".to_string(),
            port: 21353,
        },
        0,
        &["198.51.100.154".parse().expect("lan ip")],
        &["203.0.113.15".parse().expect("wireguard ip")],
    );

    assert!(
        endpoints
            .iter()
            .any(|endpoint| endpoint.address == "198.51.100.154:21353")
    );
    assert!(
        endpoints
            .iter()
            .any(|endpoint| endpoint.address == "203.0.113.15:21353")
    );
}

#[test]
fn cluster_doctor_reports_duplicate_iroh_endpoint_public_keys() {
    let cluster_id = ClusterId::from_legacy_named("cluster_doctor_iroh").expect("cluster");
    let local_node = NodeId::from_legacy_named("node_doctor_local").expect("node");
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
        endpoint_id: ClusterEndpointId::from_legacy_named("endpoint_iroh_old").expect("endpoint"),
        node_id: local_node.clone(),
        transport: ClusterTransportKind::Iroh,
        address: "iroh://same_public_key?direct=203.0.113.10:21353".to_string(),
        source: ClusterEndpointSource::Rendezvous,
        privacy: ClusterEndpointPrivacy::InternetDirect,
        priority: 40,
        last_observed_latency_ms: None,
    });
    member.endpoints.push(ClusterEndpointRecord {
        endpoint_id: ClusterEndpointId::from_legacy_named("endpoint_iroh_new").expect("endpoint"),
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
            && finding.detail.contains(local_node.as_str())
    }));
}

#[test]
fn cluster_peer_members_preserves_pending_local_member() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-cluster-peers-offline-introducer");
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(
                ClusterId::from_legacy_named("cluster_peers_offline").expect("cluster"),
            ),
            node_id: Some(NodeId::from_legacy_named("node_peers_host").expect("node")),
            name: "offline peers".to_string(),
        },
        &paths,
    )
    .expect("cluster init");
    let mut state = ClusterBootstrapState::pending_join(
        ClusterId::from_legacy_named("cluster_peers_offline").expect("cluster"),
        "offline peers".to_string(),
        NodeId::from_legacy_named("node_peers_joined").expect("node"),
        "joined peer".to_string(),
        "joined-public-key".to_string(),
        OffsetDateTime::now_utc(),
    );
    state.members.push(ClusterMemberRecord::local_voter(
        ClusterId::from_legacy_named("cluster_peers_offline").expect("cluster"),
        NodeId::from_legacy_named("node_existing_peer").expect("node"),
        "existing peer",
        "existing-public-key",
        OffsetDateTime::now_utc(),
    ));
    sinomo_runtime::network::write_cluster_state(&paths, &state).expect("write joined state");
    super::network::sync_cluster_state_to_local_raft_unchecked(&paths, &state).expect("sync kv");
    std::fs::write(&paths.kv_snapshot, "{\"revision\":1}trailing").expect("corrupt kv");

    let members = cluster_peer_members(&paths).expect("peer members");

    assert!(
        members.iter().any(|member| member.node_id
            == NodeId::from_legacy_named("node_peers_joined").expect("node"))
    );
    read_cluster_kv_snapshot(&paths).expect("repaired kv snapshot");
}

#[test]
fn install_writes_boot_entries_and_uninstall_retains_data_dir() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-install");
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

    let storage_report = sinomo_runtime::storage::verify_sinomo_data_dir(&layout.data_dir)
        .expect("storage verifier");
    assert!(
        storage_report.is_complete(),
        "unregistered data-dir files: {:?}",
        storage_report.unregistered
    );
    assert!(
        storage_report
            .covered
            .iter()
            .any(|path| path.relative_path == "bin/sinomo")
    );

    uninstall_with_layout(UninstallOptions { stop: false }, &layout).expect("uninstall");
    assert!(!layout.main_boot_entry.exists());
    assert!(!layout.shadow_boot_entry.exists());
    assert!(layout.data_dir.exists());
    assert!(layout.binary.exists());
    assert!(layout.shadow_binary.exists());
}

#[test]
fn install_reports_unregistered_storage_without_blocking_startup() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-install-storage-warning");
    std::fs::write(root.join("unexpected-local-file"), b"debug").expect("unexpected file");
    let data_dir = SinomoDataDir::new(&root);
    let layout = InstallLayout::new(&data_dir, root.join("startup"));

    install_with_layout(
        InstallOptions {
            host: "127.0.0.1".to_string(),
            port: 21410,
            shadow_port: 21411,
            start: false,
        },
        &layout,
    )
    .expect("install should warn, not fail");

    let findings = data_dir_storage_contract_findings(&layout.data_dir).expect("storage findings");
    assert!(findings.iter().any(|finding| {
        finding.code == "storage-unregistered-paths"
            && finding.detail.contains("unexpected-local-file")
    }));

    uninstall_with_layout(UninstallOptions { stop: false }, &layout).expect("uninstall");
}

#[test]
fn install_detects_existing_binary_update() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-install-update");
    let destination = root.join("bin").join(if cfg!(windows) {
        "sinomo.exe"
    } else {
        "sinomo"
    });

    assert!(!install_should_update_binary(&destination).expect("missing destination"));
    std::fs::create_dir_all(destination.parent().expect("parent")).expect("mkdir");
    std::fs::write(&destination, b"old binary").expect("write old binary");
    assert!(install_should_update_binary(&destination).expect("existing destination"));
}

#[test]
fn install_detects_daemon_option_mismatch() {
    let status = DaemonStatus {
        pid: 4242,
        host: "127.0.0.1".to_string(),
        port: 21_353,
        address: "127.0.0.1:21353".to_string(),
        version: daemon_package_version(),
        commit: None,
        target_os: Some(std::env::consts::OS.to_string()),
        target_arch: Some(std::env::consts::ARCH.to_string()),
        executable: None,
        rollout: None,
        started_at: "2026-05-04T00:00:00Z".to_string(),
        data_dir: PathBuf::from("data"),
        lock_file: PathBuf::from("data/daemon/daemon.lock"),
        cluster: None,
        idle_stats: None,
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
    assert!(!cli_command_requires_cluster_state(&CliCommand::Rollout(
        RolloutCliCommand::Status
    )));
    let registered_cluster_command =
        registered_test_command("registered-cluster", CliCommandPolicy::cluster_required());
    assert!(cli_command_requires_cluster_state(
        &registered_cluster_command
    ));
    assert!(!cli_command_is_help(&registered_cluster_command));
    let registered_cluster_help_command = registered_test_help_command(
        "registered-cluster-help",
        CliCommandPolicy::cluster_required(),
    );
    assert!(cli_command_requires_cluster_state(
        &registered_cluster_help_command
    ));
    assert!(cli_command_is_help(&registered_cluster_help_command));
    assert!(!cli_command_requires_cluster_state(&CliCommand::Cluster(
        ClusterCliCommand::Status
    )));
    assert!(!cli_command_requires_cluster_state(&CliCommand::Dfs(
        DfsCliCommand::Help
    )));
    assert!(cli_command_requires_cluster_state(&CliCommand::Dfs(
        DfsCliCommand::Status(DfsCommonOptions {
            cluster: true,
            ..DfsCommonOptions::default()
        })
    )));
    assert!(cli_command_requires_cluster_state(&CliCommand::Dfs(
        DfsCliCommand::Services { json: false }
    )));
    assert!(cli_command_requires_cluster_state(&CliCommand::Dfs(
        DfsCliCommand::MetadataPublish { json: false }
    )));
    assert!(!cli_command_requires_cluster_state(&CliCommand::Models(
        ModelsCliCommand::Help
    )));
    assert!(cli_command_is_help(&CliCommand::Models(
        ModelsCliCommand::Help
    )));
    assert!(!cli_command_requires_cluster_state(&CliCommand::Providers(
        ProviderCliCommand::Help
    )));
    let observe = parse_cli_command(&["observe".to_string()]).expect("observe");
    assert!(!cli_command_requires_cluster_state(&observe));
}

#[test]
fn daemonless_cli_uses_degraded_local_mode_for_local_capable_commands() {
    let models = CliCommand::Models(ModelsCliCommand::Help);
    assert!(cli_command_supports_degraded_local_mode(&models));
    assert!(cli_invocation_force_local_mode(
        CliRuntimeOptions::default(),
        &models,
        false,
        None
    ));
    assert!(cli_invocation_force_local_mode(
        CliRuntimeOptions::default(),
        &models,
        true,
        Some(false)
    ));
    assert!(!cli_invocation_force_local_mode(
        CliRuntimeOptions::default(),
        &models,
        true,
        Some(true)
    ));

    let dfs_local = CliCommand::Dfs(DfsCliCommand::Status(DfsCommonOptions::default()));
    assert!(cli_command_supports_degraded_local_mode(&dfs_local));
    assert!(cli_invocation_force_local_mode(
        CliRuntimeOptions::default(),
        &dfs_local,
        false,
        None
    ));

    let dfs_cluster = CliCommand::Dfs(DfsCliCommand::Status(DfsCommonOptions {
        cluster: true,
        ..DfsCommonOptions::default()
    }));
    assert!(!cli_command_supports_degraded_local_mode(&dfs_cluster));
    assert!(!cli_invocation_force_local_mode(
        CliRuntimeOptions::default(),
        &dfs_cluster,
        false,
        None
    ));

    let registered_cluster_command =
        registered_test_command("registered-cluster", CliCommandPolicy::cluster_required());
    assert!(!cli_command_supports_degraded_local_mode(
        &registered_cluster_command
    ));
    assert!(!cli_invocation_force_local_mode(
        CliRuntimeOptions::default(),
        &registered_cluster_command,
        false,
        None
    ));

    let observe = parse_cli_command(&["observe".to_string()]).expect("observe");
    assert!(cli_command_supports_degraded_local_mode(&observe));
    assert!(cli_invocation_force_local_mode(
        CliRuntimeOptions::default(),
        &observe,
        false,
        None
    ));
}

#[test]
fn cluster_runtime_quorum_requires_local_daemon_even_with_local_voter_quorum() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-cluster-daemon-guard");
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(ClusterId::from_legacy_named("daemon_guard").expect("cluster")),
            node_id: Some(NodeId::from_legacy_named("daemon_guard_node").expect("node")),
            name: "daemon guard".to_string(),
        },
        &paths,
    )
    .expect("cluster init");
    assert!(cluster_has_local_quorum(
        &paths,
        &read_cluster_state(&paths).expect("cluster state")
    ));

    let error = require_cluster_runtime_quorum_with_paths_and_daemon(&paths, None)
        .expect_err("daemon required");

    match error {
        CliError::ClusterReadOnly { detail } => {
            assert!(detail.contains("local daemon is not running"));
        }
        other => panic!("unexpected error: {other}"),
    }
}

fn signed_test_invite_uri(
    root: &std::path::Path,
    cluster_id: &str,
    issuer_node_id: &str,
) -> String {
    let identity = super::load_or_create_main_node_identity(root).expect("test identity");
    let mut invite = ClusterJoinInvite {
        invite_id: ClusterInviteId::generate(),
        cluster_id: ClusterId::from_legacy_named(cluster_id).expect("cluster"),
        cluster_name: Some("test cluster".to_string()),
        cluster_public_key: identity.public_key.clone(),
        invitation_secret: "secret_test".to_string(),
        communication_channels: Vec::new(),
        introducers: Vec::new(),
        token_id: "token_test".to_string(),
        expires_at: time::OffsetDateTime::now_utc() + time::Duration::seconds(60),
        trust_domain: TrustDomain::new("trust.local").expect("trust"),
        allowed_member_roles: vec![ClusterMemberRole::Learner, ClusterMemberRole::NonVoting],
        issuer_node_id: NodeId::from_legacy_named(issuer_node_id).expect("issuer node"),
        issuer_public_key: identity.public_key.clone(),
        signature: String::new(),
    };
    invite.signature = sign_cluster_join_invite(&invite, &identity).expect("sign invite");
    encode_cluster_join_invite(&invite).expect("invite uri")
}

#[test]
fn dfs_parser_accepts_operator_and_pin_commands() {
    let metadata_root = sinomo_testdir::TestDir::new("sinomo-cli-dfs-meta");
    assert_eq!(
        parse_dfs_command(&[
            "status".to_string(),
            "--metadata-root".to_string(),
            metadata_root.display().to_string(),
            "--json".to_string(),
        ])
        .expect("dfs status"),
        DfsCliCommand::Status(DfsCommonOptions {
            metadata_root: Some(metadata_root.to_path_buf()),
            cluster: false,
            json: true,
        })
    );
    assert_eq!(
        parse_dfs_command(&[
            "status".to_string(),
            "--cluster".to_string(),
            "--json".to_string(),
        ])
        .expect("dfs cluster status"),
        DfsCliCommand::Status(DfsCommonOptions {
            metadata_root: None,
            cluster: true,
            json: true,
        })
    );
    assert_eq!(
        parse_dfs_command(&[
            "conflicts".to_string(),
            "--filesystem".to_string(),
            "01hxy6qa9k0t9vr4g9n8d3q5d0".to_string(),
            "--object".to_string(),
            "01hxy6qa9k0t9vr4g9n8d3q5d1".to_string(),
            "--limit".to_string(),
            "128".to_string(),
        ])
        .expect("dfs conflicts"),
        DfsCliCommand::Conflicts(DfsFilterOptions {
            common: DfsCommonOptions::default(),
            file_system_id: Some(
                sinomo_runtime::dfs::DfsFileSystemId::new("01hxy6qa9k0t9vr4g9n8d3q5d0")
                    .expect("filesystem")
            ),
            object_id: Some(
                sinomo_runtime::dfs::DfsObjectId::new("01hxy6qa9k0t9vr4g9n8d3q5d1")
                    .expect("object")
            ),
            limit: Some(128),
            cursor: None,
        })
    );
    assert!(matches!(
        parse_dfs_command(&[
            "path".to_string(),
            "/services/sandbox/instances/01hxy6qa9k0t9vr4g9n8d3q5d2/manifest.sobj"
                .to_string(),
            "--filesystem".to_string(),
            "01hxy6qa9k0t9vr4g9n8d3q5d0".to_string(),
            "--json".to_string(),
        ])
        .expect("dfs path"),
        DfsCliCommand::Path { path, file_system_id, options }
            if path == "/services/sandbox/instances/01hxy6qa9k0t9vr4g9n8d3q5d2/manifest.sobj"
                && file_system_id == sinomo_runtime::dfs::DfsFileSystemId::new("01hxy6qa9k0t9vr4g9n8d3q5d0").expect("filesystem")
                && options.json
    ));
    assert_eq!(
        parse_dfs_command(&["services".to_string(), "--json".to_string()]).expect("dfs services"),
        DfsCliCommand::Services { json: true }
    );
    assert_eq!(
        parse_dfs_command(&["smoke".to_string(), "--json".to_string()]).expect("dfs smoke"),
        DfsCliCommand::Smoke { json: true }
    );
    assert_eq!(
        parse_dfs_command(&[
            "mount".to_string(),
            "/mnt/sifs".to_string(),
            "--root".to_string(),
            "/sinomo/nodes/node_alpha/files".to_string(),
            "--read-only".to_string(),
            "--allow-other".to_string(),
            "--data-dir".to_string(),
            "/tmp/sinomo-data".to_string(),
        ])
        .expect("dfs mount"),
        DfsCliCommand::Mount(DfsMountOptions {
            mount_point: PathBuf::from("/mnt/sifs"),
            root_path: "/sinomo/nodes/node_alpha/files".to_string(),
            data_dir: Some(PathBuf::from("/tmp/sinomo-data")),
            read_only: true,
            allow_other: true,
            foreground: true,
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
            "01hxy6qa9k0t9vr4g9n8d3q5d5".to_string(),
            "--read-only".to_string(),
        ])
        .expect("dfs expose add"),
        DfsCliCommand::Expose(DfsExposeCliCommand::Add(options))
            if options.source == PathBuf::from("/tmp/shared")
                && options.dfs_path == "/sinomo/nodes/node_alpha/files/shared"
                && options.node_id == Some(NodeId::new("01hxy6qa9k0t9vr4g9n8d3q5d5").expect("node"))
                && !options.writable
    ));
}

#[test]
fn dfs_operator_inspection_uses_configured_metadata_shards() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-dfs-sharded-inspection");
    let data_dir = SinomoDataDir::new(&root);
    data_dir.ensure_root_layout().expect("root layout");
    let cluster_paths = ClusterPaths::new(&data_dir);
    cluster_paths.ensure().expect("cluster paths");
    let now = time::OffsetDateTime::now_utc();
    let cluster_id =
        ClusterId::from_legacy_named("cluster_cli_dfs_sharded_inspection").expect("cluster");
    let node_id = NodeId::from_legacy_named("node_cli_dfs_sharded_inspection").expect("node");
    let state = ClusterBootstrapState::initialized_locally(
        cluster_id,
        "cli dfs sharded inspection",
        node_id.clone(),
        "local",
        "pk-local",
        now,
    );
    sinomo_runtime::network::write_cluster_state(&cluster_paths, &state)
        .expect("write cluster state");
    super::network::sync_cluster_state_to_local_raft_unchecked(&cluster_paths, &state)
        .expect("sync cluster state");

    let file_system = distributed_sifs(node_id.clone());
    let root_shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "root".to_string(),
        primary_node_id: node_id.clone(),
        replica_node_ids: Vec::new(),
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let services_shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "services".to_string(),
        primary_node_id: node_id.clone(),
        replica_node_ids: Vec::new(),
        epoch: 1,
        assignment: DfsMetadataShardAssignment::PathPrefix {
            prefix: "/services".to_string(),
        },
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    append_cluster_raft_command(
        &cluster_paths,
        &node_id,
        KvCommand::Txn(KvTxn {
            compares: Vec::new(),
            success: vec![
                KvOp::put_serde(
                    metadata_shard_key(&file_system.file_system_id, "root"),
                    &root_shard,
                    None,
                )
                .expect("root shard op"),
                KvOp::put_serde(
                    metadata_shard_key(&file_system.file_system_id, "services"),
                    &services_shard,
                    None,
                )
                .expect("services shard op"),
            ],
            failure: Vec::new(),
        }),
    )
    .expect("write shard configs");

    let mut backend = sinomo_runtime::dfs::open_local_sharded_metadata_backend(
        sinomo_runtime::dfs::default_dfs_metadata_shards_root(&data_dir),
        vec![root_shard, services_shard],
    )
    .expect("open sharded backend");
    let mut object = distributed_sifs_bootstrap_objects(node_id)
        .into_iter()
        .next()
        .expect("bootstrap object");
    object.path = "/services/cli-sharded-inspection.txt".to_string();
    backend.put_file_system(&file_system).expect("filesystem");
    backend.put_object(&object).expect("object");
    backend
        .put_version(&DfsObjectVersionRecord {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            version: object.version.clone(),
            content_hash: object.content_hash.clone(),
            size_bytes: object.content_size_bytes,
            locations: object.current_locations.clone(),
            created_by_event_id: object.created_by_event_id.clone(),
            created_at: now,
            visible: true,
        })
        .expect("version");

    let metadata = open_dfs_metadata_for_inspection_with_data_dir(None, &data_dir)
        .expect("open inspection metadata");
    let inspector = metadata.inspector();
    let inspected = inspector
        .object_by_path(&file_system.file_system_id, &object.path)
        .expect("inspect path")
        .expect("object by path");

    assert_eq!(inspected.object.object_id, object.object_id);
    assert_eq!(inspected.object.path, object.path);
}

#[test]
fn cluster_parser_accepts_init_and_join_options() {
    assert_eq!(
        parse_cluster_command(&[
            "init".to_string(),
            "--name".to_string(),
            "home".to_string(),
            "--cluster-id".to_string(),
            "01hxy6qa9k0t9vr4g9n8d3q5d2".to_string(),
            "--node-id".to_string(),
            "01hxy6qa9k0t9vr4g9n8d3q5d3".to_string(),
        ])
        .expect("cluster init"),
        ClusterCliCommand::Init(ClusterInitOptions {
            cluster_id: Some(ClusterId::new("01hxy6qa9k0t9vr4g9n8d3q5d2").expect("cluster")),
            node_id: Some(NodeId::new("01hxy6qa9k0t9vr4g9n8d3q5d3").expect("node")),
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
            "01hxy6qa9k0t9vr4g9n8d3q5d6".to_string(),
            "server".to_string(),
        ])
        .expect("cluster role"),
        ClusterCliCommand::Role(ClusterRoleOptions {
            node_id: NodeId::new("01hxy6qa9k0t9vr4g9n8d3q5d6").expect("node"),
            role: ClusterNodeRole::Server,
        })
    );
    assert_eq!(
        parse_cluster_command(&[
            "raft-role".to_string(),
            "01hxy6qa9k0t9vr4g9n8d3q5d6".to_string(),
            "non_voting".to_string(),
        ])
        .expect("cluster raft-role"),
        ClusterCliCommand::RaftRole(ClusterRaftRoleOptions {
            node_id: NodeId::new("01hxy6qa9k0t9vr4g9n8d3q5d6").expect("node"),
            role: ClusterMemberRole::NonVoting,
        })
    );
    assert_eq!(
        parse_cluster_command(&[
            "promote".to_string(),
            "01hxy6qa9k0t9vr4g9n8d3q5d6".to_string()
        ])
        .expect("cluster promote"),
        ClusterCliCommand::RaftRole(ClusterRaftRoleOptions {
            node_id: NodeId::new("01hxy6qa9k0t9vr4g9n8d3q5d6").expect("node"),
            role: ClusterMemberRole::Voter,
        })
    );
    assert_eq!(
        parse_cluster_command(&[
            "remove".to_string(),
            "01hxy6qa9k0t9vr4g9n8d3q5d6".to_string(),
            "01hxy6qa9k0t9vr4g9n8d3q5d7".to_string(),
        ])
        .expect("cluster remove"),
        ClusterCliCommand::Remove(ClusterRemoveOptions {
            node_ids: vec![
                NodeId::new("01hxy6qa9k0t9vr4g9n8d3q5d6").expect("node"),
                NodeId::new("01hxy6qa9k0t9vr4g9n8d3q5d7").expect("node"),
            ],
        })
    );
}

#[test]
fn cluster_init_persists_local_cluster_state() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-cluster-init");
    let paths = ClusterPaths::new(&SinomoDataDir::new(&root));

    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(ClusterId::from_legacy_named("cluster_test").expect("cluster")),
            node_id: Some(NodeId::from_legacy_named("node_local").expect("node")),
            name: "test cluster".to_string(),
        },
        &paths,
    )
    .expect("cluster init");

    let state = read_cluster_state(&paths).expect("cluster state");
    assert_eq!(
        state.cluster_id,
        ClusterId::from_legacy_named("cluster_test").expect("cluster")
    );
    assert_eq!(
        state.local_node_id,
        NodeId::from_legacy_named("node_local").expect("node")
    );
    assert_eq!(state.term, 1);
    assert_eq!(state.kv_revision, 1);
    assert_eq!(state.members.len(), 2);
    let main = state
        .members
        .iter()
        .find(|member| member.node_id == NodeId::from_legacy_named("node_local").expect("node"))
        .expect("main daemon member");
    assert_eq!(main.role, ClusterMemberRole::Voter);
    assert_eq!(main.state, ClusterMemberState::Voter);
    assert!(!main.node_public_key.trim().is_empty());
    assert_eq!(main.endpoints[0].address, "127.0.0.1:21353");
    let shadow = state
        .members
        .iter()
        .find(|member| {
            member.node_id
                == shadow_daemon_node_id(&NodeId::from_legacy_named("node_local").expect("node"))
        })
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
        .find(|member| member.node_id == NodeId::from_legacy_named("node_local").expect("node"))
        .expect("kv main");
    assert_eq!(kv_main.state, ClusterMemberState::Voter);
    assert_eq!(kv_main.node_public_key, main.node_public_key);
    assert_eq!(
        cluster_raft_known_leader_id(&paths, &state).expect("known leader"),
        Some(NodeId::from_legacy_named("node_local").expect("node"))
    );
    crate::clear_cluster_raft_leader(&paths, &state.local_node_id).expect("clear leader");
    assert_eq!(
        cluster_raft_known_leader_id(&paths, &state).expect("cleared leader"),
        None
    );
    crate::set_cluster_raft_leader(&paths, &state.local_node_id, 65)
        .expect("set single-voter leader");
    assert_eq!(
        cluster_raft_known_leader_id(&paths, &state).expect("reelected leader"),
        Some(NodeId::from_legacy_named("node_local").expect("node"))
    );
}

#[test]
fn cluster_state_sync_rejects_unsafe_voter_reconfiguration() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-unsafe-voter-config");
    let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(
                ClusterId::from_legacy_named("cluster_unsafe_config").expect("cluster"),
            ),
            node_id: Some(NodeId::from_legacy_named("node_config_host").expect("node")),
            name: "unsafe config cluster".to_string(),
        },
        &paths,
    )
    .expect("cluster init");
    let state = read_cluster_state(&paths).expect("cluster state");
    sync_cluster_state_to_kv(&paths, &state).expect("publish baseline cluster config");

    let mut removed_voter_state = state.clone();
    removed_voter_state.members.clear();
    let removed_error =
        sync_cluster_state_to_kv(&paths, &removed_voter_state).expect_err("reject voter removal");
    match removed_error {
        CliError::Kv(sinomo_runtime::kv::KvError::InvalidRaftConfigTransition { detail }) => {
            assert!(detail.contains("overlapping old/new quorum"));
            assert!(detail.contains("refusing to remove"));
        }
        other => panic!("unexpected voter removal error: {other:?}"),
    }

    let mut direct_voter_state = state.clone();
    let peer_id = NodeId::from_legacy_named("node_direct_voter").expect("peer");
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
    match added_error {
        CliError::Kv(sinomo_runtime::kv::KvError::InvalidRaftConfigTransition { detail }) => {
            assert!(detail.contains("must already be a committed learner or non-voting member"));
            assert!(detail.contains(peer_id.as_str()));
        }
        other => panic!("unexpected direct voter add error: {other:?}"),
    }
}

#[test]
fn cluster_remove_deletes_non_voter_members_from_state_and_kv() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-cluster-remove");
    let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(ClusterId::from_legacy_named("cluster_remove").expect("cluster")),
            node_id: Some(NodeId::from_legacy_named("node_remove_host").expect("node")),
            name: "remove cluster".to_string(),
        },
        &paths,
    )
    .expect("cluster init");
    let mut state = read_cluster_state(&paths).expect("cluster state");
    let learner_id = NodeId::from_legacy_named("node_remove_learner").expect("learner");
    let non_voter_id = NodeId::from_legacy_named("node_remove_non_voter").expect("non voter");
    state.members.push(ClusterMemberRecord::pending_learner(
        state.cluster_id.clone(),
        learner_id.clone(),
        "learner",
        "pk_learner",
        OffsetDateTime::now_utc(),
    ));
    state.members.push(ClusterMemberRecord::local_non_voting(
        state.cluster_id.clone(),
        non_voter_id.clone(),
        "non voter",
        "pk_non_voter",
        OffsetDateTime::now_utc(),
    ));
    sync_cluster_state_to_kv(&paths, &state).expect("sync state");
    network::write_cluster_state(&paths, &state).expect("write state");

    let removal = network::remove_cluster_members_with_host(
        &paths,
        read_cluster_state(&paths).expect("state before remove"),
        vec![learner_id.clone(), non_voter_id.clone()],
        &mut CliClusterMemberMutationHost,
    )
    .expect("remove members");

    assert_eq!(removal.removed.len(), 2);
    let state = read_cluster_state(&paths).expect("updated state");
    assert!(
        !state
            .members
            .iter()
            .any(|member| member.node_id == learner_id)
    );
    assert!(
        !state
            .members
            .iter()
            .any(|member| member.node_id == non_voter_id)
    );
    let kv_members = read_cluster_members_from_kv(&paths).expect("kv members");
    assert!(!kv_members.iter().any(|member| member.node_id == learner_id));
    assert!(
        !kv_members
            .iter()
            .any(|member| member.node_id == non_voter_id)
    );
}

#[test]
fn local_voter_republishes_local_members_missing_from_kv() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-local-member-republish");
    let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(
                ClusterId::from_legacy_named("cluster_local_republish").expect("cluster"),
            ),
            node_id: Some(NodeId::from_legacy_named("node_local_republish").expect("node")),
            name: "local republish".to_string(),
        },
        &paths,
    )
    .expect("cluster init");
    let local_state =
        super::network::read_cluster_state_without_kv(&paths).expect("state without kv");
    let shadow_id = shadow_daemon_node_id(&local_state.local_node_id);

    append_cluster_raft_command_with_response(
        &paths,
        &local_state.local_node_id,
        sinomo_runtime::kv::KvCommand::Op(KvOp::Delete {
            key: sinomo_runtime::network::cluster_member_key(&shadow_id),
        }),
    )
    .expect("delete shadow from kv");
    assert!(
        !read_cluster_members_from_kv(&paths)
            .expect("members after delete")
            .iter()
            .any(|member| member.node_id == shadow_id)
    );

    let merged_state = read_cluster_state(&paths).expect("merged local state");
    assert!(
        merged_state
            .members
            .iter()
            .any(|member| member.node_id == shadow_id)
    );
    network::publish_local_members_to_quorum_if_needed_with_host(
        &paths,
        &merged_state,
        network::ClusterLocalMemberPublishOptions {
            member_publish_timeout: Duration::from_secs(1),
            force_kv_snapshot_sync: false,
        },
        &mut CliLocalMemberPublishHost,
    )
    .expect("republish local members");

    assert!(
        read_cluster_members_from_kv(&paths)
            .expect("members after republish")
            .iter()
            .any(|member| member.node_id == shadow_id)
    );
}

#[test]
fn daemon_auth_accepts_signed_local_command_and_rejects_tampering() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-daemon-auth");
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(ClusterId::from_legacy_named("cluster_auth").expect("cluster")),
            node_id: Some(NodeId::from_legacy_named("node_auth").expect("node")),
            name: "auth cluster".to_string(),
        },
        &paths,
    )
    .expect("cluster init");
    let daemon_paths = DaemonPaths::with_root(root.join("daemon"));
    let identity = super::load_or_create_main_node_identity(&root).expect("identity");

    let rpc_request = sinomo_runtime::network::ObjectRpcRequest::encode_body(
        sinomo_runtime::network::DAEMON_STATUS_ROUTE,
        sinomo_runtime::network::DAEMON_RPC_CATEGORY,
        &(),
    )
    .expect("rpc request");
    let signed_rpc =
        sign_daemon_rpc_request_from_data_dir(&data_dir, &rpc_request).expect("signed rpc route");
    assert_eq!(
        authenticate_daemon_control_request(
            &daemon_paths,
            &sinomo_runtime::network::DaemonControlRequest::authenticated_rpc(
                super::daemon_rpc_command(sinomo_runtime::network::DAEMON_STATUS_ROUTE),
                rpc_request.clone(),
                signed_rpc.clone(),
            )
        )
        .expect("auth rpc command"),
        authenticated_test_command(
            &super::daemon_rpc_command(sinomo_runtime::network::DAEMON_STATUS_ROUTE),
            super::DaemonSignerAuthority::Local,
            NodeId::from_legacy_named("node_auth")
                .expect("node")
                .as_str(),
            identity.public_key
        )
    );
    assert!(matches!(
        authenticate_daemon_control_request(
            &daemon_paths,
            &sinomo_runtime::network::DaemonControlRequest::new("INVALID_CONTROL_FRAME")
        ),
        Err(super::DaemonError::AuthDenied)
    ));
    let tampered_rpc_request = sinomo_runtime::network::ObjectRpcRequest::encode_body(
        sinomo_runtime::network::DAEMON_STATUS_ROUTE,
        sinomo_runtime::network::DAEMON_RPC_CATEGORY,
        &"tampered",
    )
    .expect("tampered rpc request");
    assert!(matches!(
        authenticate_daemon_control_request(
            &daemon_paths,
            &sinomo_runtime::network::DaemonControlRequest::authenticated_rpc(
                super::daemon_rpc_command(sinomo_runtime::network::DAEMON_STATUS_ROUTE),
                tampered_rpc_request,
                signed_rpc,
            )
        ),
        Err(super::DaemonError::AuthDenied)
    ));
}

#[test]
fn daemon_auth_only_accepts_active_signed_join_invites() {
    let host_root = sinomo_testdir::TestDir::new("sinomo-cli-daemon-join-auth-host");
    let join_root = sinomo_testdir::TestDir::new("sinomo-cli-daemon-join-auth-join");
    let host_data_dir = SinomoDataDir::new(&host_root);
    let host_cluster_paths = ClusterPaths::new(&host_data_dir);
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(ClusterId::from_legacy_named("cluster_join_auth").expect("cluster")),
            node_id: Some(NodeId::from_legacy_named("node_join_auth_host").expect("node")),
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
        NodeId::from_legacy_named("node_join_auth_candidate").expect("node"),
        "join candidate",
        join_identity.public_key.clone(),
        OffsetDateTime::now_utc(),
    );
    let request = ClusterJoinRegistrationRequest {
        invitation_secret: rendered.invite.invitation_secret.clone(),
        invite: rendered.invite.clone(),
        members: vec![joined_member],
    };
    let rpc_request = sinomo_runtime::network::ObjectRpcRequest::encode_body(
        sinomo_runtime::network::CLUSTER_JOIN_REGISTER_ROUTE,
        sinomo_runtime::network::CLUSTER_MEMBERSHIP_RPC_CATEGORY,
        &request,
    )
    .expect("join request");
    let signed =
        sign_daemon_rpc_request_from_data_dir(&join_data_dir, &rpc_request).expect("signed join");
    assert_eq!(
        authenticate_daemon_control_request(
            &DaemonPaths::with_root(host_root.join("daemon")),
            &sinomo_runtime::network::DaemonControlRequest::authenticated_rpc(
                super::daemon_rpc_command(sinomo_runtime::network::CLUSTER_JOIN_REGISTER_ROUTE),
                rpc_request,
                signed,
            )
        )
        .expect("active invite auth")
        .signer,
        super::DaemonSignerAuthority::JoinCandidate
    );

    let mut tampered = request;
    tampered.invite.cluster_public_key.push_str("_tampered");
    let tampered_rpc_request = sinomo_runtime::network::ObjectRpcRequest::encode_body(
        sinomo_runtime::network::CLUSTER_JOIN_REGISTER_ROUTE,
        sinomo_runtime::network::CLUSTER_MEMBERSHIP_RPC_CATEGORY,
        &tampered,
    )
    .expect("tampered join request");
    let tampered_signed =
        sign_daemon_rpc_request_from_data_dir(&join_data_dir, &tampered_rpc_request)
            .expect("signed tampered join");
    assert!(matches!(
        authenticate_daemon_control_request(
            &DaemonPaths::with_root(host_root.join("daemon")),
            &sinomo_runtime::network::DaemonControlRequest::authenticated_rpc(
                super::daemon_rpc_command(sinomo_runtime::network::CLUSTER_JOIN_REGISTER_ROUTE),
                tampered_rpc_request,
                tampered_signed,
            )
        ),
        Err(super::DaemonError::AuthDenied)
    ));
}

#[test]
fn daemon_authorization_restricts_admin_and_local_commands() {
    assert!(
        super::authorize_authenticated_daemon_command(&authenticated_test_command(
            &super::daemon_rpc_command(sinomo_runtime::network::DAEMON_STATUS_ROUTE),
            super::DaemonSignerAuthority::Member,
            "node_member",
            "pk_member"
        ))
        .is_ok()
    );
    assert!(
        super::authorize_authenticated_daemon_command(&authenticated_test_command(
            &super::daemon_rpc_command(sinomo_runtime::kv::CLUSTER_KV_SNAPSHOT_ROUTE),
            super::DaemonSignerAuthority::Member,
            "node_member",
            "pk_member"
        ))
        .is_ok()
    );
    assert!(
        super::authorize_authenticated_daemon_command(&authenticated_test_command(
            &super::daemon_rpc_command(sinomo_runtime::kv::CLUSTER_KV_RANGE_PREFIX_ROUTE),
            super::DaemonSignerAuthority::Member,
            "node_member",
            "pk_member"
        ))
        .is_ok()
    );
    assert!(
        super::authorize_authenticated_daemon_command(&authenticated_test_command(
            &super::daemon_rpc_command(sinomo_runtime::kv::CLUSTER_KV_WATCH_PREFIX_ROUTE),
            super::DaemonSignerAuthority::Member,
            "node_member",
            "pk_member"
        ))
        .is_ok()
    );
    assert!(
        super::authorize_authenticated_daemon_command(&authenticated_test_command(
            &super::daemon_rpc_command(sinomo_runtime::kv::CLUSTER_KV_TXN_ROUTE),
            super::DaemonSignerAuthority::Member,
            "node_member",
            "pk_member"
        ))
        .is_ok()
    );
    assert!(
        super::authorize_authenticated_daemon_command(&authenticated_test_command(
            &super::daemon_rpc_command(sinomo_runtime::kv::CLUSTER_KV_TXN_ROUTE),
            super::DaemonSignerAuthority::Voter,
            "node_voter",
            "pk_voter"
        ))
        .is_ok()
    );
    assert!(
        super::authorize_authenticated_daemon_command(&authenticated_test_command(
            &super::daemon_rpc_command(sinomo_runtime::kv::CLUSTER_RAFT_APPEND_ROUTE),
            super::DaemonSignerAuthority::Member,
            "node_member",
            "pk_member"
        ))
        .is_err()
    );
    assert!(
        super::authorize_authenticated_daemon_command(&authenticated_test_command(
            &super::daemon_rpc_command(sinomo_runtime::kv::CLUSTER_RAFT_VOTE_ROUTE),
            super::DaemonSignerAuthority::Voter,
            "node_voter",
            "pk_voter"
        ))
        .is_ok()
    );
    assert!(
        super::authorize_authenticated_daemon_command(&authenticated_test_command(
            &super::daemon_rpc_command(sinomo_runtime::network::CLUSTER_MEMBER_PUBLISH_ROUTE),
            super::DaemonSignerAuthority::Member,
            "node_member",
            "pk_member"
        ))
        .is_ok()
    );
    assert!(
        super::authorize_authenticated_daemon_command(&authenticated_test_command(
            &super::daemon_rpc_command(sinomo_runtime::network::CLUSTER_INVITE_PUBLISH_ROUTE),
            super::DaemonSignerAuthority::JoinCandidate,
            "node_join",
            "pk_join"
        ))
        .is_err()
    );
    assert!(
        super::authorize_authenticated_daemon_command(&authenticated_test_command(
            &super::daemon_rpc_command(sinomo_runtime::network::CLUSTER_JOIN_REGISTER_ROUTE),
            super::DaemonSignerAuthority::JoinCandidate,
            "node_join",
            "pk_join"
        ))
        .is_ok()
    );
    assert!(
        super::authorize_authenticated_daemon_command(&authenticated_test_command(
            &super::daemon_rpc_command(sinomo_runtime::dfs::DFS_EXPOSE_CONFIG_ROUTE),
            super::DaemonSignerAuthority::Member,
            "node_member",
            "pk_member"
        ))
        .is_err()
    );
    assert!(
        super::authorize_authenticated_daemon_command(&authenticated_test_command(
            &super::daemon_rpc_command(sinomo_runtime::dfs::DFS_EXPOSE_CONFIG_ROUTE),
            super::DaemonSignerAuthority::Local,
            "node_local",
            "pk_local"
        ))
        .is_ok()
    );
    assert!(
        super::authorize_authenticated_daemon_command(&authenticated_test_command(
            &super::daemon_rpc_command(sinomo_runtime::dfs::DFS_SMOKE_ROUTE),
            super::DaemonSignerAuthority::Member,
            "node_member",
            "pk_member"
        ))
        .is_err()
    );
    assert!(
        super::authorize_authenticated_daemon_command(&authenticated_test_command(
            &super::daemon_rpc_command(sinomo_runtime::dfs::DFS_SMOKE_ROUTE),
            super::DaemonSignerAuthority::Local,
            "node_local",
            "pk_local"
        ))
        .is_ok()
    );
    assert!(
        super::authorize_authenticated_daemon_command(&authenticated_test_command(
            &super::daemon_rpc_command(sinomo_runtime::network::SANDBOX_HTTP_FETCH_ROUTE),
            super::DaemonSignerAuthority::Member,
            "node_member",
            "pk_member"
        ))
        .is_err()
    );
    assert!(
        super::authorize_authenticated_daemon_command(&authenticated_test_command(
            &super::daemon_rpc_command(sinomo_runtime::network::SANDBOX_HTTP_FETCH_ROUTE),
            super::DaemonSignerAuthority::Local,
            "node_local",
            "pk_local"
        ))
        .is_ok()
    );
    for route in [
        sinomo_runtime::network::SANDBOX_CREATE_ROUTE,
        sinomo_runtime::network::SANDBOX_STATUS_ROUTE,
        sinomo_runtime::network::SANDBOX_UPDATE_ROUTE,
        sinomo_runtime::network::SANDBOX_CHECKPOINT_ROUTE,
        sinomo_runtime::network::SANDBOX_RESUME_ROUTE,
        sinomo_runtime::network::SANDBOX_REBOOT_ROUTE,
        sinomo_runtime::network::SANDBOX_DESTROY_ROUTE,
        sinomo_runtime::network::SANDBOX_LIST_ROUTE,
        sinomo_runtime::network::SANDBOX_READ_ROUTE,
        sinomo_runtime::network::SANDBOX_WRITE_ROUTE,
        sinomo_runtime::network::SANDBOX_HTTP_FETCH_ROUTE,
        sinomo_runtime::network::SANDBOX_HTTP_FETCH_TO_FILE_ROUTE,
        sinomo_runtime::network::SANDBOX_RUN_TOOLCHAIN_ROUTE,
        sinomo_runtime::network::SANDBOX_RUN_PROCESS_PLAN_ROUTE,
        sinomo_runtime::network::SANDBOX_RUN_CANCEL_ROUTE,
        sinomo_runtime::network::SANDBOX_RUN_TOOLCHAIN_STREAM_ROUTE,
        sinomo_runtime::network::SANDBOX_RUN_PROCESS_PLAN_STREAM_ROUTE,
        sinomo_runtime::network::SANDBOX_IMPORT_BUSYBOX_ROUTE,
        sinomo_runtime::network::SANDBOX_TOOLCHAIN_IMPORT_PACKAGE_ROUTE,
        sinomo_runtime::network::SANDBOX_TOOLCHAIN_ADD_COMMAND_ROUTE,
        sinomo_runtime::network::SANDBOX_TOOLCHAIN_ADD_ARTIFACT_ROUTE,
    ] {
        assert!(
            super::authorize_authenticated_daemon_command(&authenticated_test_command(
                &super::daemon_rpc_command(route),
                super::DaemonSignerAuthority::Member,
                "node_member",
                "pk_member"
            ))
            .is_err(),
            "member should not be authorized for {route}"
        );
        assert!(
            super::authorize_authenticated_daemon_command(&authenticated_test_command(
                &super::daemon_rpc_command(route),
                super::DaemonSignerAuthority::Local,
                "node_local",
                "pk_local"
            ))
            .is_ok(),
            "local signer should be authorized for {route}"
        );
    }
    assert!(
        super::authorize_authenticated_daemon_command(&authenticated_test_command(
            &super::daemon_rpc_command(sinomo_runtime::network::SANDBOX_RUN_TOOLCHAIN_ROUTE),
            super::DaemonSignerAuthority::Member,
            "node_member",
            "pk_member"
        ))
        .is_err()
    );
    assert!(
        super::authorize_authenticated_daemon_command(&authenticated_test_command(
            &super::daemon_rpc_command(sinomo_runtime::network::SANDBOX_RUN_TOOLCHAIN_ROUTE),
            super::DaemonSignerAuthority::Local,
            "node_local",
            "pk_local"
        ))
        .is_ok()
    );
    assert!(
        super::authorize_authenticated_daemon_command(&authenticated_test_command(
            &super::daemon_rpc_command(sinomo_runtime::network::SANDBOX_WRITE_ROUTE),
            super::DaemonSignerAuthority::Member,
            "node_member",
            "pk_member"
        ))
        .is_err()
    );
    assert!(
        super::authorize_authenticated_daemon_command(&authenticated_test_command(
            &super::daemon_rpc_command(sinomo_runtime::network::SANDBOX_WRITE_ROUTE),
            super::DaemonSignerAuthority::Local,
            "node_local",
            "pk_local"
        ))
        .is_ok()
    );
    assert!(
        super::authorize_authenticated_daemon_command(&authenticated_test_command(
            &super::daemon_rpc_command(sinomo_runtime::rollout::ROLLOUT_DIAGNOSTICS_ROUTE),
            super::DaemonSignerAuthority::Member,
            "node_member",
            "pk_member"
        ))
        .is_err()
    );
    assert!(
        super::authorize_authenticated_daemon_command(&authenticated_test_command(
            &super::daemon_rpc_command(sinomo_runtime::rollout::ROLLOUT_DIAGNOSTICS_ROUTE),
            super::DaemonSignerAuthority::Voter,
            "node_voter",
            "pk_voter"
        ))
        .is_ok()
    );
    assert!(
        super::authorize_authenticated_daemon_command(&authenticated_test_command(
            &super::daemon_rpc_command(sinomo_runtime::rollout::ROLLOUT_BUILD_ROUTE),
            super::DaemonSignerAuthority::Member,
            "node_member",
            "pk_member"
        ))
        .is_err()
    );
    assert!(
        super::authorize_authenticated_daemon_command(&authenticated_test_command(
            &super::daemon_rpc_command(sinomo_runtime::rollout::ROLLOUT_BUILD_ROUTE),
            super::DaemonSignerAuthority::Voter,
            "node_voter",
            "pk_voter"
        ))
        .is_ok()
    );
    assert!(
        super::authorize_authenticated_daemon_command(&authenticated_test_command(
            &super::daemon_rpc_command(sinomo_runtime::rollout::ROLLOUT_BINARY_ROUTE),
            super::DaemonSignerAuthority::Voter,
            "node_voter",
            "pk_voter"
        ))
        .is_ok()
    );
    assert!(
        super::authorize_authenticated_daemon_command(&authenticated_test_command(
            &super::daemon_rpc_command(sinomo_runtime::network::DAEMON_SHUTDOWN_ROUTE),
            super::DaemonSignerAuthority::Voter,
            "node_voter",
            "pk_voter"
        ))
        .is_err()
    );
    assert!(
        super::authorize_authenticated_daemon_command(&authenticated_test_command(
            &super::daemon_rpc_command(sinomo_runtime::rollout::ROLLOUT_FINALIZE_SHADOW_ROUTE),
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
    let root = sinomo_testdir::TestDir::new("sinomo-cli-cluster-join");
    let paths = ClusterPaths::new(&SinomoDataDir::new(&root));

    cluster_join_with_paths(
        ClusterJoinOptions {
            invite: signed_test_invite_uri(&root, "cluster_joined", "node_host"),
            cluster_id: Some(ClusterId::from_legacy_named("cluster_joined").expect("cluster")),
            node_id: Some(NodeId::from_legacy_named("node_joiner").expect("node")),
            name: "joiner".to_string(),
        },
        &paths,
    )
    .expect("cluster join");

    let state = read_cluster_state(&paths).expect("cluster state");
    assert_eq!(
        state.cluster_id,
        ClusterId::from_legacy_named("cluster_joined").expect("cluster")
    );
    assert_eq!(state.term, 0);
    assert_eq!(state.kv_revision, 0);
    let local = state
        .members
        .iter()
        .find(|member| member.node_id == state.local_node_id)
        .expect("local main daemon member");
    assert_eq!(local.role, ClusterMemberRole::Learner);
    assert_eq!(local.state, ClusterMemberState::Pending);
    assert!(!local.node_public_key.trim().is_empty());
    let shadow = state
        .members
        .iter()
        .find(|member| {
            member.node_id
                == shadow_daemon_node_id(&NodeId::from_legacy_named("node_joiner").expect("node"))
        })
        .expect("shadow daemon member");
    assert_eq!(shadow.role, ClusterMemberRole::NonVoting);
    assert_eq!(shadow.state, ClusterMemberState::Pending);
}

#[test]
fn cluster_invite_generates_portable_join_payload() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-cluster-invite");
    let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
    let output = root.join("join.txt");
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(ClusterId::from_legacy_named("cluster_invite").expect("cluster")),
            node_id: Some(NodeId::from_legacy_named("node_host").expect("node")),
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
    assert!(rendered.uri.contains("lan-tcp"));
    assert!(rendered.uri.contains("192.168.1.10:21353"));
    assert!(!rendered.uri.contains("&cluster_id="));
    assert!(!rendered.uri.contains("&cluster_name="));
    assert!(!rendered.uri.contains("&introducer="));
    assert!(!rendered.uri.contains("&signature="));
    assert!(!rendered.uri.contains("%7B"));
    assert_eq!(
        parsed.cluster_id,
        ClusterId::new("01hxy6qa9k0t9vr4g9n8d3q5c0").expect("cluster")
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
    let invite_state_entry = super::read_cluster_kv_store(&paths)
        .expect("cluster kv store")
        .get(&super::network::cluster_invitation_state_key(
            &parsed.invite_id,
        ))
        .expect("invite state")
        .clone();
    let invite_state: super::network::ClusterInvitationStateRecord =
        invite_state_entry.decode().expect("invite state record");
    assert_eq!(invite_state.state, "active");
    assert_eq!(
        rendered.persisted_key,
        super::network::cluster_invitation_key(&parsed.invite_id)
    );
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
}

#[test]
fn join_registration_skips_loopback_when_remote_invite_channels_exist() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-cluster-invite-channel-filter");
    let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(
                ClusterId::from_legacy_named("cluster_invite_filter").expect("cluster"),
            ),
            node_id: Some(NodeId::from_legacy_named("node_host").expect("node")),
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
}

#[test]
fn join_registration_keeps_loopback_when_it_is_the_only_invite_channel() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-cluster-invite-loopback-only");
    let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(
                ClusterId::from_legacy_named("cluster_loopback_only").expect("cluster"),
            ),
            node_id: Some(NodeId::from_legacy_named("node_host").expect("node")),
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
}

#[test]
fn explicit_invite_introducer_uses_known_member_endpoint_identity() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-cluster-explicit-introducer");
    let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(
                ClusterId::from_legacy_named("cluster_explicit_intro").expect("cluster"),
            ),
            node_id: Some(NodeId::from_legacy_named("node_host").expect("node")),
            name: "explicit introducer cluster".to_string(),
        },
        &paths,
    )
    .expect("cluster init");
    let mut state = read_cluster_state(&paths).expect("cluster state");
    let now = OffsetDateTime::now_utc();
    let remote_node = NodeId::from_legacy_named("node_known_intro").expect("node");
    let mut remote = ClusterMemberRecord::local_non_voting(
        state.cluster_id.clone(),
        remote_node.clone(),
        "known introducer",
        "known-public-key",
        now,
    );
    remote.endpoints.push(invite_introducer_endpoint(
        &remote_node,
        "192.0.2.11:21353".to_string(),
    ));
    state.members.push(remote);

    let endpoint = invite_introducer_endpoint_from_state(&state, "192.0.2.11:21353".to_string());
    let channels = invite_communication_channels(&state, &[endpoint]);

    assert_eq!(channels.len(), 1);
    assert_eq!(channels[0].endpoint.node_id, remote_node);
    assert_eq!(channels[0].node_public_key, "known-public-key");
}

#[test]
fn cluster_join_uses_structured_invite_cluster_identity() {
    let host_root = sinomo_testdir::TestDir::new("sinomo-cli-cluster-host");
    let join_root = sinomo_testdir::TestDir::new("sinomo-cli-cluster-join-invite");
    let host_paths = ClusterPaths::new(&SinomoDataDir::new(&host_root));
    let join_paths = ClusterPaths::new(&SinomoDataDir::new(&join_root));
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(ClusterId::from_legacy_named("cluster_remote").expect("cluster")),
            node_id: Some(NodeId::from_legacy_named("node_host").expect("node")),
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
            cluster_id: Some(ClusterId::from_legacy_named("cluster_remote").expect("cluster")),
            node_id: Some(NodeId::from_legacy_named("node_remote").expect("node")),
            name: "remote".to_string(),
        },
        &join_paths,
    )
    .expect("cluster join");
    let state = read_cluster_state(&join_paths).expect("cluster state");

    assert_eq!(
        state.cluster_id,
        ClusterId::from_legacy_named("cluster_remote").expect("cluster")
    );
    assert_eq!(state.cluster_name, "remote");
    assert!(state.members.iter().any(|member| {
        member.node_id == NodeId::from_legacy_named("node_remote").expect("node")
            && member.role == ClusterMemberRole::Learner
            && member.state == ClusterMemberState::Pending
    }));
}

#[test]
fn introducer_records_joined_members_and_returns_peer_view() {
    let host_root = sinomo_testdir::TestDir::new("sinomo-cli-cluster-introducer");
    let join_root = sinomo_testdir::TestDir::new("sinomo-cli-cluster-joining");
    let host_paths = ClusterPaths::new(&SinomoDataDir::new(&host_root));
    let join_paths = ClusterPaths::new(&SinomoDataDir::new(&join_root));
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(ClusterId::from_legacy_named("cluster_register").expect("cluster")),
            node_id: Some(NodeId::from_legacy_named("node_host").expect("node")),
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
    super::network::sync_cluster_invitation_to_kv(&host_paths, &offline_invite)
        .expect("active offline invite kv");
    let offline_uri = encode_cluster_join_invite(&offline_invite).expect("offline invite");
    cluster_join_with_paths(
        ClusterJoinOptions {
            invite: offline_uri,
            cluster_id: Some(ClusterId::from_legacy_named("cluster_register").expect("cluster")),
            node_id: Some(NodeId::from_legacy_named("node_remote").expect("node")),
            name: "remote".to_string(),
        },
        &join_paths,
    )
    .expect("cluster join local");
    let remote_state = read_cluster_state(&join_paths).expect("remote state");
    let mut malicious_members = remote_state.members.clone();
    for member in &mut malicious_members {
        if member.node_id == NodeId::from_legacy_named("node_remote").expect("node") {
            member.role = ClusterMemberRole::Voter;
            member.state = ClusterMemberState::Pending;
        }
    }
    let malicious_request = ClusterJoinRegistrationRequest {
        invitation_secret: offline_invite.invitation_secret.clone(),
        invite: offline_invite.clone(),
        members: malicious_members,
    };
    let malicious_response = handle_daemon_cluster_join_request(
        &DaemonPaths::with_root(host_root.join("daemon")),
        malicious_request,
    )
    .expect("malicious introducer join");
    assert!(!malicious_response.accepted);
    assert!(
        !read_cluster_state(&host_paths)
            .expect("host state after rejected join")
            .members
            .iter()
            .any(|member| member.node_id == NodeId::from_legacy_named("node_remote").expect("node"))
    );

    let request = ClusterJoinRegistrationRequest {
        invitation_secret: offline_invite.invitation_secret.clone(),
        invite: offline_invite,
        members: remote_state.members,
    };
    let response = handle_daemon_cluster_join_request(
        &DaemonPaths::with_root(host_root.join("daemon")),
        request,
    )
    .expect("introducer join");

    assert!(response.accepted);
    assert!(response.kv_snapshot.is_some());
    let host_state = read_cluster_state(&host_paths).expect("host state");
    assert!(host_state.members.iter().any(|member| {
        member.node_id == NodeId::from_legacy_named("node_remote").expect("node")
            && member.state == ClusterMemberState::Learner
    }));
    let host_kv_members = read_cluster_members_from_kv(&host_paths).expect("host kv members");
    assert!(host_kv_members.iter().any(|member| {
        member.node_id == NodeId::from_legacy_named("node_remote").expect("node")
            && member.state == ClusterMemberState::Learner
    }));
    assert!(response.state.members.iter().any(|member| {
        member.node_id == NodeId::from_legacy_named("node_remote").expect("node")
    }));
}

#[test]
fn daemon_cluster_kv_txn_persists_snapshot() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-cluster-kv");
    let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(ClusterId::from_legacy_named("cluster_kv").expect("cluster")),
            node_id: Some(NodeId::from_legacy_named("node_kv").expect("node")),
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
    let response =
        handle_daemon_cluster_kv_txn_request(&DaemonPaths::with_root(root.join("daemon")), txn)
            .expect("kv txn");

    assert!(response.succeeded);
    let store = super::read_cluster_kv_store(&paths).expect("kv store");
    let service = store.get("/services/test").expect("service entry");
    assert_eq!(service.value, br#"{"healthy":true}"#.to_vec());
}

#[test]
fn daemon_sandbox_http_fetch_uses_cluster_policy_and_returns_body() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-daemon-sandbox-http");
    let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(
                ClusterId::from_legacy_named("cluster_sandbox_http").expect("cluster"),
            ),
            node_id: Some(NodeId::from_legacy_named("node_sandbox_http").expect("node")),
            name: "sandbox http cluster".to_string(),
        },
        &paths,
    )
    .expect("cluster init");
    let mut policy_config = sinomo_runtime::sandbox::SandboxClusterPolicyConfig::default();
    policy_config
        .policy
        .allowed_http_hosts
        .insert("127.0.0.1".to_string());
    policy_config
        .policy
        .allowed_command_capabilities
        .insert(sinomo_runtime::sandbox::SANDBOX_NETWORK_HTTP_CAPABILITY.to_string());
    let txn = sinomo_runtime::sandbox::sandbox_policy_config_cluster_kv_txn(&policy_config)
        .expect("policy txn");
    handle_daemon_cluster_kv_txn_request(&DaemonPaths::with_root(root.join("daemon")), txn)
        .expect("write policy");

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind http server");
    let address = listener.local_addr().expect("http server address");
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept http");
        let mut buffer = [0_u8; 1024];
        let _ = stream.read(&mut buffer).expect("read request");
        stream
            .write_all(
                b"HTTP/1.1 200 OK\r\nContent-Length: 11\r\nContent-Type: text/plain\r\n\r\nhello world",
            )
            .expect("write response");
    });
    let mut request = sinomo_runtime::sandbox::SandboxHttpRequest::get(format!(
        "http://{address}/package?token=secret#fragment"
    ));
    request.max_response_bytes = 1024;

    let response = daemon_handle_daemon_sandbox_http_fetch(
        &DaemonPaths::with_root(root.join("daemon")),
        request,
    )
    .expect("fetch");

    server.join().expect("server");
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"hello world");
}

#[test]
#[cfg(feature = "sandbox")]
fn daemon_sandbox_http_fetch_to_file_persists_body_audit_and_event_in_sifs() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-daemon-sandbox-http-to-file");
    let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(
                ClusterId::from_legacy_named("cluster_sandbox_http_file").expect("cluster"),
            ),
            node_id: Some(NodeId::from_legacy_named("node_sandbox_http_file").expect("node")),
            name: "sandbox http file cluster".to_string(),
        },
        &paths,
    )
    .expect("cluster init");
    let daemon_paths = DaemonPaths::with_root(root.join("daemon"));
    let mut policy_config = sinomo_runtime::sandbox::SandboxClusterPolicyConfig::default();
    policy_config
        .policy
        .allowed_http_hosts
        .insert("127.0.0.1".to_string());
    policy_config
        .policy
        .allowed_command_capabilities
        .insert(sinomo_runtime::sandbox::SANDBOX_NETWORK_HTTP_CAPABILITY.to_string());
    let txn = sinomo_runtime::sandbox::sandbox_policy_config_cluster_kv_txn(&policy_config)
        .expect("policy txn");
    handle_daemon_cluster_kv_txn_request(&daemon_paths, txn).expect("write policy");

    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();
    let toolchain_id = sinomo_runtime::sandbox::SandboxToolchainId::generate();
    let dfs =
        super::daemon_open_daemon_sandbox_dfs_service(&daemon_paths).expect("open dfs service");
    let mut instance_store = sinomo_runtime::sandbox::SifsSandboxInstanceStore::new(dfs);
    let manifest = instance_store
        .create(sandbox_id.clone(), toolchain_id, OffsetDateTime::now_utc())
        .expect("create sandbox");
    drop(instance_store);

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind http server");
    let address = listener.local_addr().expect("http server address");
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept http");
        let mut buffer = [0_u8; 1024];
        let _ = stream.read(&mut buffer).expect("read request");
        stream
            .write_all(
                b"HTTP/1.1 200 OK\r\nContent-Length: 11\r\nContent-Type: text/plain\r\n\r\nhello world",
            )
            .expect("write response");
    });
    let mut request =
        sinomo_runtime::sandbox::SandboxHttpRequest::get(format!("http://{address}/package"));
    request.max_response_bytes = 1024;
    let output_path =
        sinomo_runtime::sandbox::SandboxPath::new("/work/package.txt").expect("output");

    let result = daemon_handle_daemon_sandbox_http_fetch_to_file(
        &daemon_paths,
        sinomo_runtime::sandbox::SandboxHttpFetchToFileRequest {
            sandbox_id: sandbox_id.clone(),
            request,
            output_path: output_path.clone(),
        },
    )
    .expect("fetch to file");

    server.join().expect("server");
    assert_eq!(result.status, 200);
    assert_eq!(result.response_bytes, 11);
    let dfs =
        super::daemon_open_daemon_sandbox_dfs_service(&daemon_paths).expect("reopen dfs service");
    let mut fs = sinomo_runtime::sandbox::SifsSandboxFileSystem::new(dfs, &manifest);
    assert_eq!(fs.read(&output_path, None).expect("body"), b"hello world");
    let audit = sinomo_runtime::sandbox::load_http_exchange_record(
        &mut fs,
        &result.record.command_id,
        &result.request_id,
    )
    .expect("audit");
    assert_eq!(audit, result.record);
    assert_eq!(audit.url, format!("http://{address}/package"));
    assert!(!audit.url.contains("secret"));
    assert!(!audit.url.contains("fragment"));
    let event_store =
        sinomo_runtime::events::JsonlEventStore::new(root.join("events").join("event-log.jsonl"));
    let events = event_store
        .replay(ReplayQuery::by_kind_prefix("sandbox.network"))
        .expect("network events");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event.kind.as_str(), "sandbox.network.http");
    assert_eq!(
        events[0].event.payload["sandbox_id"],
        sandbox_id.to_string()
    );
    assert_eq!(
        events[0].event.payload["workspace_root"],
        sinomo_runtime::sandbox::sandbox_instance_root(&sandbox_id)
    );
    assert_eq!(
        events[0].event.payload["request_id"],
        result.request_id.to_string()
    );
    assert_eq!(
        events[0].event.payload["url"],
        format!("http://{address}/package")
    );
    assert!(
        !events[0].event.payload["url"]
            .as_str()
            .unwrap_or_default()
            .contains("secret")
    );
    assert!(
        !events[0].event.payload["url"]
            .as_str()
            .unwrap_or_default()
            .contains("fragment")
    );
    let file_events = event_store
        .replay(ReplayQuery::by_kind_prefix("sandbox.file"))
        .expect("file events");
    assert_eq!(file_events.len(), 1);
    assert_eq!(file_events[0].event.kind.as_str(), "sandbox.file.written");
    assert_eq!(
        file_events[0].event.payload["sandbox_id"],
        sandbox_id.to_string()
    );
    assert_eq!(
        file_events[0].event.payload["workspace_root"],
        sinomo_runtime::sandbox::sandbox_instance_root(&sandbox_id)
    );
    assert_eq!(
        file_events[0].event.payload["path"],
        output_path.to_string()
    );
    assert_eq!(file_events[0].event.payload["bytes_written"], 11);
    assert_eq!(file_events[0].event.payload["phase"], "sandbox-fetch");
    assert_eq!(
        file_events[0].event.payload["command_id"],
        result.record.command_id.to_string()
    );
    assert_eq!(
        file_events[0].event.payload["request_id"],
        result.request_id.to_string()
    );
}

#[test]
#[cfg(feature = "sandbox")]
fn daemon_sandbox_http_fetch_to_file_records_resource_exhaustion_event() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-daemon-sandbox-http-to-file-quota");
    let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(
                ClusterId::from_legacy_named("cluster_sandbox_http_file_quota").expect("cluster"),
            ),
            node_id: Some(NodeId::from_legacy_named("node_sandbox_http_file_quota").expect("node")),
            name: "sandbox http file quota cluster".to_string(),
        },
        &paths,
    )
    .expect("cluster init");
    let daemon_paths = DaemonPaths::with_root(root.join("daemon"));
    let mut policy_config = sinomo_runtime::sandbox::SandboxClusterPolicyConfig::default();
    policy_config
        .policy
        .allowed_http_hosts
        .insert("127.0.0.1".to_string());
    policy_config
        .policy
        .allowed_command_capabilities
        .insert(sinomo_runtime::sandbox::SANDBOX_NETWORK_HTTP_CAPABILITY.to_string());
    let txn = sinomo_runtime::sandbox::sandbox_policy_config_cluster_kv_txn(&policy_config)
        .expect("policy txn");
    handle_daemon_cluster_kv_txn_request(&daemon_paths, txn).expect("write policy");

    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();
    let toolchain_id = sinomo_runtime::sandbox::SandboxToolchainId::generate();
    let dfs =
        super::daemon_open_daemon_sandbox_dfs_service(&daemon_paths).expect("open dfs service");
    let mut instance_store = sinomo_runtime::sandbox::SifsSandboxInstanceStore::new(dfs);
    let mut manifest = instance_store
        .create(sandbox_id.clone(), toolchain_id, OffsetDateTime::now_utc())
        .expect("create sandbox");
    manifest.limits.max_written_bytes_per_command = 4;
    instance_store
        .save_manifest(&manifest)
        .expect("save quota manifest");
    drop(instance_store);

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind http server");
    let address = listener.local_addr().expect("http server address");
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept http");
        let mut buffer = [0_u8; 1024];
        let _ = stream.read(&mut buffer).expect("read request");
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 7\r\n\r\narchive")
            .expect("write response");
    });
    let mut request =
        sinomo_runtime::sandbox::SandboxHttpRequest::get(format!("http://{address}/package"));
    request.max_response_bytes = 1024;
    let output_path =
        sinomo_runtime::sandbox::SandboxPath::new("/work/package.txt").expect("output");

    let error = daemon_handle_daemon_sandbox_http_fetch_to_file(
        &daemon_paths,
        sinomo_runtime::sandbox::SandboxHttpFetchToFileRequest {
            sandbox_id: sandbox_id.clone(),
            request,
            output_path: output_path.clone(),
        },
    )
    .expect_err("fetch body should exceed write quota");

    server.join().expect("server");
    assert!(format!("{error:?}").contains("written-bytes-per-command"));
    let dfs =
        super::daemon_open_daemon_sandbox_dfs_service(&daemon_paths).expect("reopen dfs service");
    let fs = sinomo_runtime::sandbox::SifsSandboxFileSystem::new(dfs, &manifest);
    assert!(matches!(
        fs.read(&output_path, None),
        Err(sinomo_runtime::sandbox::SandboxFsError::NotFound { .. })
    ));

    let event_store =
        sinomo_runtime::events::JsonlEventStore::new(root.join("events").join("event-log.jsonl"));
    let resource_events = event_store
        .replay(ReplayQuery::by_kind_prefix("sandbox.resource"))
        .expect("resource events");
    assert_eq!(resource_events.len(), 1);
    assert_eq!(
        resource_events[0].event.kind.as_str(),
        "sandbox.resource.exhausted"
    );
    assert_eq!(
        resource_events[0].event.payload["sandbox_id"],
        sandbox_id.to_string()
    );
    assert_eq!(
        resource_events[0].event.payload["workspace_root"],
        sinomo_runtime::sandbox::sandbox_instance_root(&sandbox_id)
    );
    assert_eq!(resource_events[0].event.payload["phase"], "sandbox-fetch");
    assert_eq!(
        resource_events[0].event.payload["kind"],
        "written-bytes-per-command"
    );
    assert_eq!(resource_events[0].event.payload["actual"], 7);
    assert_eq!(resource_events[0].event.payload["limit"], 4);
    let network_events = event_store
        .replay(ReplayQuery::by_kind_prefix("sandbox.network"))
        .expect("network events");
    assert_eq!(
        network_events.len(),
        1,
        "quota-denied body writes should still keep the HTTP audit event"
    );
    assert_eq!(
        network_events[0].event.payload["workspace_root"],
        sinomo_runtime::sandbox::sandbox_instance_root(&sandbox_id)
    );
}

#[test]
#[cfg(feature = "sandbox")]
fn daemon_sandbox_http_fetch_to_file_audits_policy_denial_without_sifs_writes() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-daemon-sandbox-http-to-file-denied");
    let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(
                ClusterId::from_legacy_named("cluster_sandbox_http_file_denied").expect("cluster"),
            ),
            node_id: Some(
                NodeId::from_legacy_named("node_sandbox_http_file_denied").expect("node"),
            ),
            name: "sandbox http file denied cluster".to_string(),
        },
        &paths,
    )
    .expect("cluster init");
    let daemon_paths = DaemonPaths::with_root(root.join("daemon"));

    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();
    let toolchain_id = sinomo_runtime::sandbox::SandboxToolchainId::generate();
    let dfs =
        super::daemon_open_daemon_sandbox_dfs_service(&daemon_paths).expect("open dfs service");
    let mut instance_store = sinomo_runtime::sandbox::SifsSandboxInstanceStore::new(dfs);
    let manifest = instance_store
        .create(sandbox_id.clone(), toolchain_id, OffsetDateTime::now_utc())
        .expect("create sandbox");
    drop(instance_store);

    let output_path =
        sinomo_runtime::sandbox::SandboxPath::new("/work/package.txt").expect("output");
    let error = daemon_handle_daemon_sandbox_http_fetch_to_file(
        &daemon_paths,
        sinomo_runtime::sandbox::SandboxHttpFetchToFileRequest {
            sandbox_id: sandbox_id.clone(),
            request: sinomo_runtime::sandbox::SandboxHttpRequest::get(
                "https://packages.example/archive.tar.gz",
            ),
            output_path: output_path.clone(),
        },
    )
    .expect_err("default policy denies network fetch");
    assert!(format!("{error:?}").contains("network-capability-denied"));

    let dfs =
        super::daemon_open_daemon_sandbox_dfs_service(&daemon_paths).expect("reopen dfs service");
    let fs = sinomo_runtime::sandbox::SifsSandboxFileSystem::new(dfs, &manifest);
    assert!(
        matches!(
            fs.read(&output_path, None),
            Err(sinomo_runtime::sandbox::SandboxFsError::NotFound { .. })
        ),
        "denied fetch should not write the requested output"
    );
    assert!(
        fs.list(&sinomo_runtime::sandbox::SandboxPath::new("/logs").expect("logs"))
            .expect("logs")
            .iter()
            .all(|entry| !entry.path.as_str().ends_with(".http.sobj")),
        "denied pre-policy fetch should not write an HTTP audit object"
    );
    let event_store =
        sinomo_runtime::events::JsonlEventStore::new(root.join("events").join("event-log.jsonl"));
    let policy_events = event_store
        .replay(ReplayQuery::by_kind_prefix("sandbox.policy"))
        .expect("policy events");
    assert_eq!(policy_events.len(), 1);
    assert_eq!(
        policy_events[0].event.kind.as_str(),
        "sandbox.policy.denied"
    );
    assert_eq!(
        policy_events[0].event.payload["sandbox_id"],
        sandbox_id.to_string()
    );
    assert_eq!(
        policy_events[0].event.payload["workspace_root"],
        sinomo_runtime::sandbox::sandbox_instance_root(&sandbox_id)
    );
    assert_eq!(policy_events[0].event.payload["phase"], "sandbox-fetch");
    assert_eq!(
        policy_events[0].event.payload["code"],
        "network-capability-denied"
    );
    let network_events = event_store
        .replay(ReplayQuery::by_kind_prefix("sandbox.network"))
        .expect("network events");
    assert!(network_events.is_empty());
}

#[test]
#[cfg(feature = "sandbox")]
fn daemon_sandbox_http_fetch_to_file_audits_transport_failure_in_sifs() {
    let root =
        sinomo_testdir::TestDir::new("sinomo-cli-daemon-sandbox-http-to-file-transport-failure");
    let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(
                ClusterId::from_legacy_named("cluster_sandbox_http_file_transport")
                    .expect("cluster"),
            ),
            node_id: Some(
                NodeId::from_legacy_named("node_sandbox_http_file_transport").expect("node"),
            ),
            name: "sandbox http file transport cluster".to_string(),
        },
        &paths,
    )
    .expect("cluster init");
    let daemon_paths = DaemonPaths::with_root(root.join("daemon"));
    let mut policy_config = sinomo_runtime::sandbox::SandboxClusterPolicyConfig::default();
    policy_config
        .policy
        .allowed_http_hosts
        .insert("127.0.0.1".to_string());
    policy_config
        .policy
        .allowed_command_capabilities
        .insert(sinomo_runtime::sandbox::SANDBOX_NETWORK_HTTP_CAPABILITY.to_string());
    let txn = sinomo_runtime::sandbox::sandbox_policy_config_cluster_kv_txn(&policy_config)
        .expect("policy txn");
    handle_daemon_cluster_kv_txn_request(&daemon_paths, txn).expect("write policy");

    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();
    let toolchain_id = sinomo_runtime::sandbox::SandboxToolchainId::generate();
    let dfs =
        super::daemon_open_daemon_sandbox_dfs_service(&daemon_paths).expect("open dfs service");
    let mut instance_store = sinomo_runtime::sandbox::SifsSandboxInstanceStore::new(dfs);
    let manifest = instance_store
        .create(sandbox_id.clone(), toolchain_id, OffsetDateTime::now_utc())
        .expect("create sandbox");
    drop(instance_store);

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind broken http server");
    let address = listener.local_addr().expect("http server address");
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept http");
        let mut buffer = [0_u8; 1024];
        let _ = stream.read(&mut buffer).expect("read request");
    });
    let mut request =
        sinomo_runtime::sandbox::SandboxHttpRequest::get(format!("http://{address}/package"));
    request.max_response_bytes = 1024;
    request.timeout_ms = 1_000;
    let output_path =
        sinomo_runtime::sandbox::SandboxPath::new("/work/package.txt").expect("output");

    let error = daemon_handle_daemon_sandbox_http_fetch_to_file(
        &daemon_paths,
        sinomo_runtime::sandbox::SandboxHttpFetchToFileRequest {
            sandbox_id: sandbox_id.clone(),
            request,
            output_path: output_path.clone(),
        },
    )
    .expect_err("broken server should fail after policy");
    server.join().expect("server");
    assert!(format!("{error:?}").contains("sandbox mediated http"));

    let dfs =
        super::daemon_open_daemon_sandbox_dfs_service(&daemon_paths).expect("reopen dfs service");
    let fs = sinomo_runtime::sandbox::SifsSandboxFileSystem::new(dfs, &manifest);
    assert!(
        matches!(
            fs.read(&output_path, None),
            Err(sinomo_runtime::sandbox::SandboxFsError::NotFound { .. })
        ),
        "failed fetch should not write the requested output"
    );
    let log_entries = fs
        .list(&sinomo_runtime::sandbox::SandboxPath::new("/logs").expect("logs"))
        .expect("logs");
    let audit_path = log_entries
        .iter()
        .find(|entry| entry.path.as_str().ends_with(".http.sobj"))
        .expect("transport failure audit object")
        .path
        .clone();
    let record = sinomo_runtime::sandbox::decode_http_exchange_record(
        &fs.read(&audit_path, None).expect("audit bytes"),
    )
    .expect("decode audit");
    assert_eq!(record.sandbox_id, sandbox_id);
    assert!(record.status.is_none());
    assert_eq!(record.response_bytes, 0);

    let event_store =
        sinomo_runtime::events::JsonlEventStore::new(root.join("events").join("event-log.jsonl"));
    let network_events = event_store
        .replay(ReplayQuery::by_kind_prefix("sandbox.network"))
        .expect("network events");
    assert_eq!(network_events.len(), 1);
    assert_eq!(
        network_events[0].event.kind.as_str(),
        "sandbox.network.http"
    );
    assert_eq!(
        network_events[0].event.payload["sandbox_id"],
        sandbox_id.to_string()
    );
    assert_eq!(
        network_events[0].event.payload["workspace_root"],
        sinomo_runtime::sandbox::sandbox_instance_root(&sandbox_id)
    );
    assert_eq!(
        network_events[0].event.payload["request_id"],
        record.request_id.to_string()
    );
    assert!(network_events[0].event.payload["status"].is_null());
    assert_eq!(network_events[0].event.payload["response_bytes"], 0);
}

#[test]
#[cfg(feature = "sandbox")]
fn daemon_sandbox_import_busybox_preflights_policy_before_sifs_writes() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-daemon-sandbox-busybox-policy");
    let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(
                ClusterId::from_legacy_named("cluster_sandbox_busybox_policy").expect("cluster"),
            ),
            node_id: Some(NodeId::from_legacy_named("node_sandbox_busybox_policy").expect("node")),
            name: "sandbox busybox policy cluster".to_string(),
        },
        &paths,
    )
    .expect("cluster init");
    let daemon_paths = DaemonPaths::with_root(root.join("daemon"));
    let mut restrictive_policy = sinomo_runtime::sandbox::SandboxClusterPolicyConfig::default();
    restrictive_policy
        .policy
        .allowed_toolchain_provenance_prefixes =
        BTreeSet::from(["sinomo://toolchains/".to_string()]);
    let txn = sinomo_runtime::sandbox::sandbox_policy_config_cluster_kv_txn(&restrictive_policy)
        .expect("restrictive policy txn");
    handle_daemon_cluster_kv_txn_request(&daemon_paths, txn).expect("write restrictive policy");

    let denied = daemon_handle_daemon_sandbox_import_busybox(
        &daemon_paths,
        sinomo_runtime::sandbox::SandboxImportBusyBoxToolchainRequest {
            version: "busybox-wasi-test".to_string(),
            provenance: "unit test".to_string(),
            module_bytes: b"busybox module".to_vec(),
        },
    )
    .expect_err("policy-denied busybox import rejected");
    let denied = format!("{denied:?}");
    assert!(denied.contains("toolchain-provenance-denied"), "{denied}");

    let dfs = super::daemon_open_daemon_sandbox_dfs_service(&daemon_paths).expect("open dfs");
    let missing_toolchains = dfs
        .posix_stat("/services/sandbox/toolchains")
        .expect_err("busybox denial did not create toolchain subtree");
    let missing_toolchains = format!("{missing_toolchains:?}");
    assert!(
        missing_toolchains.contains("NotFound") || missing_toolchains.contains("MissingParent"),
        "{missing_toolchains}"
    );
    let event_store =
        sinomo_runtime::events::JsonlEventStore::new(root.join("events").join("event-log.jsonl"));
    let toolchain_events = event_store
        .replay(ReplayQuery::by_kind_prefix("sandbox.toolchain"))
        .expect("replay toolchain events");
    assert!(toolchain_events.is_empty());
    let policy_events = event_store
        .replay(ReplayQuery::by_kind_prefix("sandbox.policy"))
        .expect("replay policy events");
    assert!(
        policy_events
            .iter()
            .any(|event| event.event.payload["phase"] == "sandbox-toolchain-import-busybox")
    );
}
#[test]
#[cfg(feature = "sandbox")]
fn daemon_sandbox_toolchain_add_command_persists_sifs_command() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-daemon-sandbox-toolchain-add");
    let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(
                ClusterId::from_legacy_named("cluster_sandbox_toolchain_add").expect("cluster"),
            ),
            node_id: Some(NodeId::from_legacy_named("node_sandbox_toolchain_add").expect("node")),
            name: "sandbox toolchain add cluster".to_string(),
        },
        &paths,
    )
    .expect("cluster init");
    let daemon_paths = DaemonPaths::with_root(root.join("daemon"));
    let toolchain_id = sinomo_runtime::sandbox::SandboxToolchainId::generate();

    let response = daemon_handle_daemon_sandbox_toolchain_add_command(
        &daemon_paths,
        sinomo_runtime::sandbox::SandboxAddToolchainCommandRequest {
            toolchain_id: toolchain_id.clone(),
            version: "python-wasi-test".to_string(),
            target_abi: sinomo_runtime::sandbox::SandboxWasiAbi::Wasix,
            provenance: "unit test".to_string(),
            command_name: "python".to_string(),
            module_bytes: b"python module".to_vec(),
            required_capabilities: vec!["network.http".to_string()],
            default_limits: sinomo_runtime::sandbox::SandboxResourceLimits {
                memory_mb: 256,
                wall_time_ms: 10_000,
                ..sinomo_runtime::sandbox::SandboxResourceLimits::default()
            },
        },
    )
    .expect("add command");
    assert_eq!(response.manifest.toolchain_id, toolchain_id);
    assert_eq!(response.manifest.version, "python-wasi-test");
    assert_eq!(
        response.module_hash,
        sinomo_runtime::sandbox::sandbox_module_hash(b"python module")
    );

    let dfs = super::daemon_open_daemon_sandbox_dfs_service(&daemon_paths).expect("open dfs");
    let mut toolchain_store = sinomo_runtime::sandbox::SifsSandboxToolchainStore::new(dfs);
    let manifest = toolchain_store
        .load_manifest(&toolchain_id)
        .expect("load toolchain");
    let command = manifest.command("python").expect("python command");
    assert_eq!(command.required_capabilities, vec!["network.http"]);
    assert_eq!(command.default_limits.memory_mb, 256);
    assert_eq!(command.default_limits.wall_time_ms, 10_000);
    assert_eq!(
        toolchain_store
            .load_command_module(&manifest, "python")
            .expect("load python"),
        b"python module"
    );
    let mut restrictive_policy = sinomo_runtime::sandbox::SandboxClusterPolicyConfig::default();
    restrictive_policy.policy.max_limits.memory_mb = 128;
    let txn = sinomo_runtime::sandbox::sandbox_policy_config_cluster_kv_txn(&restrictive_policy)
        .expect("restrictive policy txn");
    handle_daemon_cluster_kv_txn_request(&daemon_paths, txn).expect("write restrictive policy");
    let denied = daemon_handle_daemon_sandbox_toolchain_add_command(
        &daemon_paths,
        sinomo_runtime::sandbox::SandboxAddToolchainCommandRequest {
            toolchain_id: toolchain_id.clone(),
            version: "python-wasi-test".to_string(),
            target_abi: sinomo_runtime::sandbox::SandboxWasiAbi::Wasix,
            provenance: "unit test".to_string(),
            command_name: "node".to_string(),
            module_bytes: b"node module".to_vec(),
            required_capabilities: Vec::new(),
            default_limits: sinomo_runtime::sandbox::SandboxResourceLimits {
                memory_mb: 512,
                ..sinomo_runtime::sandbox::SandboxResourceLimits::default()
            },
        },
    )
    .expect_err("policy-denied toolchain command rejected");
    let denied = format!("{denied:?}");
    assert!(denied.contains("resource-limit-denied"), "{denied}");
    assert!(denied.contains("memory-mb"), "{denied}");
    let malformed = daemon_handle_daemon_sandbox_toolchain_add_command(
        &daemon_paths,
        sinomo_runtime::sandbox::SandboxAddToolchainCommandRequest {
            toolchain_id: toolchain_id.clone(),
            version: "python-wasi-test".to_string(),
            target_abi: sinomo_runtime::sandbox::SandboxWasiAbi::Wasix,
            provenance: "unit test".to_string(),
            command_name: "curl".to_string(),
            module_bytes: b"curl module".to_vec(),
            required_capabilities: vec!["network\0http".to_string()],
            default_limits: sinomo_runtime::sandbox::SandboxResourceLimits::default(),
        },
    )
    .expect_err("malformed capability rejected before write");
    assert!(format!("{malformed:?}").contains("embedded NUL byte"));
    let manifest = toolchain_store
        .load_manifest(&toolchain_id)
        .expect("reload toolchain after denial");
    assert!(manifest.command("python").is_ok());
    assert!(manifest.command("node").is_err());
    assert!(manifest.command("curl").is_err());

    let event_store =
        sinomo_runtime::events::JsonlEventStore::new(root.join("events").join("event-log.jsonl"));
    let events = event_store
        .replay(ReplayQuery::by_kind_prefix("sandbox.toolchain"))
        .expect("replay toolchain events");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event.kind.as_str(), "sandbox.toolchain.updated");
    assert_eq!(
        events[0].event.payload["toolchain_id"],
        toolchain_id.to_string()
    );
    assert_eq!(events[0].event.payload["module_hash"], response.module_hash);
    let policy_events = event_store
        .replay(ReplayQuery::by_kind_prefix("sandbox.policy"))
        .expect("replay policy events");
    assert!(
        policy_events
            .iter()
            .any(|event| event.event.payload["phase"] == "sandbox-toolchain-add-command")
    );
}

#[test]
#[cfg(feature = "sandbox")]
fn daemon_sandbox_toolchain_add_artifact_persists_sifs_support_file() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-daemon-sandbox-toolchain-artifact");
    let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(
                ClusterId::from_legacy_named("cluster_sandbox_toolchain_artifact")
                    .expect("cluster"),
            ),
            node_id: Some(
                NodeId::from_legacy_named("node_sandbox_toolchain_artifact").expect("node"),
            ),
            name: "sandbox toolchain artifact cluster".to_string(),
        },
        &paths,
    )
    .expect("cluster init");
    let daemon_paths = DaemonPaths::with_root(root.join("daemon"));
    let toolchain_id = sinomo_runtime::sandbox::SandboxToolchainId::generate();

    let response = daemon_handle_daemon_sandbox_toolchain_add_artifact(
        &daemon_paths,
        sinomo_runtime::sandbox::SandboxAddToolchainArtifactRequest {
            toolchain_id: toolchain_id.clone(),
            version: "python-wasi-test".to_string(),
            provenance: "unit test".to_string(),
            relative_path: "share/python/stdlib.zip".to_string(),
            bytes: b"stdlib archive".to_vec(),
        },
    )
    .expect("add artifact");
    assert_eq!(response.manifest.toolchain_id, toolchain_id);
    assert_eq!(response.manifest.version, "python-wasi-test");
    assert_eq!(
        response.artifact_hash,
        sinomo_runtime::sandbox::sandbox_module_hash(b"stdlib archive")
    );

    let dfs = super::daemon_open_daemon_sandbox_dfs_service(&daemon_paths).expect("open dfs");
    let mut toolchain_store = sinomo_runtime::sandbox::SifsSandboxToolchainStore::new(dfs);
    let manifest = toolchain_store
        .load_manifest(&toolchain_id)
        .expect("load toolchain");
    let artifact = manifest
        .artifact("share/python/stdlib.zip")
        .expect("artifact");
    assert_eq!(artifact.byte_len, b"stdlib archive".len() as u64);
    let artifacts = toolchain_store
        .load_artifacts(&manifest)
        .expect("load artifacts");
    assert_eq!(
        artifacts
            .iter()
            .find(|(path, _)| path == "share/python/stdlib.zip")
            .map(|(_, bytes)| bytes.as_slice()),
        Some(b"stdlib archive".as_slice())
    );
    let mut restrictive_policy = sinomo_runtime::sandbox::SandboxClusterPolicyConfig::default();
    restrictive_policy.policy.allowed_package_cache_prefixes = vec!["/users".to_string()];
    let txn = sinomo_runtime::sandbox::sandbox_policy_config_cluster_kv_txn(&restrictive_policy)
        .expect("restrictive policy txn");
    handle_daemon_cluster_kv_txn_request(&daemon_paths, txn).expect("write restrictive policy");
    let denied = daemon_handle_daemon_sandbox_toolchain_add_artifact(
        &daemon_paths,
        sinomo_runtime::sandbox::SandboxAddToolchainArtifactRequest {
            toolchain_id: toolchain_id.clone(),
            version: "python-wasi-test".to_string(),
            provenance: "unit test".to_string(),
            relative_path: "share/python/denied.zip".to_string(),
            bytes: b"denied archive".to_vec(),
        },
    )
    .expect_err("policy-denied artifact rejected");
    let denied = format!("{denied:?}");
    assert!(denied.contains("package-cache-path-denied"), "{denied}");
    let manifest = toolchain_store
        .load_manifest(&toolchain_id)
        .expect("reload toolchain after denied artifact");
    assert!(manifest.artifact("share/python/stdlib.zip").is_ok());
    assert!(manifest.artifact("share/python/denied.zip").is_err());

    let event_store =
        sinomo_runtime::events::JsonlEventStore::new(root.join("events").join("event-log.jsonl"));
    let events = event_store
        .replay(ReplayQuery::by_kind_prefix("sandbox.toolchain"))
        .expect("replay toolchain events");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event.kind.as_str(), "sandbox.toolchain.updated");
    assert_eq!(
        events[0].event.payload["module_hash"],
        response.artifact_hash
    );
    let policy_events = event_store
        .replay(ReplayQuery::by_kind_prefix("sandbox.policy"))
        .expect("replay policy events");
    assert!(
        policy_events
            .iter()
            .any(|event| event.event.payload["phase"] == "sandbox-toolchain-add-artifact")
    );
}

#[test]
#[cfg(feature = "sandbox")]
fn daemon_sandbox_toolchain_import_package_persists_sifs_package() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-daemon-sandbox-toolchain-package");
    let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(
                ClusterId::from_legacy_named("cluster_sandbox_toolchain_package").expect("cluster"),
            ),
            node_id: Some(
                NodeId::from_legacy_named("node_sandbox_toolchain_package").expect("node"),
            ),
            name: "sandbox toolchain package cluster".to_string(),
        },
        &paths,
    )
    .expect("cluster init");
    let daemon_paths = DaemonPaths::with_root(root.join("daemon"));
    let toolchain_id = sinomo_runtime::sandbox::SandboxToolchainId::generate();
    daemon_handle_daemon_sandbox_toolchain_add_command(
        &daemon_paths,
        sinomo_runtime::sandbox::SandboxAddToolchainCommandRequest {
            toolchain_id: toolchain_id.clone(),
            version: "busybox-plus-python-test".to_string(),
            target_abi: sinomo_runtime::sandbox::SandboxWasiAbi::Wasix,
            provenance: "unit test base".to_string(),
            command_name: "cat".to_string(),
            module_bytes: b"busybox module".to_vec(),
            required_capabilities: Vec::new(),
            default_limits: sinomo_runtime::sandbox::SandboxResourceLimits::default(),
        },
    )
    .expect("add base command");

    let response = daemon_handle_daemon_sandbox_toolchain_import_package(
        &daemon_paths,
        sinomo_runtime::sandbox::SandboxImportToolchainPackageRequest {
            toolchain_id: toolchain_id.clone(),
            version: "python-wasi-test".to_string(),
            target_abi: sinomo_runtime::sandbox::SandboxWasiAbi::Wasix,
            provenance: "unit test".to_string(),
            commands: vec![
                sinomo_runtime::sandbox::SandboxImportToolchainPackageCommand {
                    name: "python".to_string(),
                    module_bytes: b"python module".to_vec(),
                    target_abi: Some(sinomo_runtime::sandbox::SandboxWasiAbi::Wasix),
                    argv_prefix: Vec::new(),
                    exec_name: None,
                    default_env: std::collections::BTreeMap::new(),
                    required_capabilities: vec!["network.http".to_string()],
                    default_limits: sinomo_runtime::sandbox::SandboxResourceLimits {
                        memory_mb: 512,
                        ..sinomo_runtime::sandbox::SandboxResourceLimits::default()
                    },
                },
            ],
            artifacts: vec![
                sinomo_runtime::sandbox::SandboxImportToolchainPackageArtifact {
                    relative_path: "share/python/stdlib.zip".to_string(),
                    bytes: b"stdlib archive".to_vec(),
                },
            ],
        },
    )
    .expect("import package");
    assert_eq!(response.manifest.toolchain_id, toolchain_id);
    assert_eq!(response.manifest.version, "busybox-plus-python-test");
    assert_eq!(response.manifest.commands.len(), 2);
    assert_eq!(response.manifest.artifacts.len(), 1);
    assert_eq!(
        response
            .manifest
            .command("python")
            .expect("python")
            .effective_target_abi(&response.manifest),
        sinomo_runtime::sandbox::SandboxWasiAbi::Wasix
    );

    let dfs = super::daemon_open_daemon_sandbox_dfs_service(&daemon_paths).expect("open dfs");
    let mut toolchain_store = sinomo_runtime::sandbox::SifsSandboxToolchainStore::new(dfs);
    let manifest = toolchain_store
        .load_manifest(&toolchain_id)
        .expect("load toolchain");
    assert_eq!(
        toolchain_store
            .load_command_module(&manifest, "cat")
            .expect("load cat"),
        b"busybox module"
    );
    assert_eq!(
        toolchain_store
            .load_command_module(&manifest, "python")
            .expect("load python"),
        b"python module"
    );
    let artifacts = toolchain_store
        .load_artifacts(&manifest)
        .expect("load artifacts");
    assert_eq!(
        artifacts
            .iter()
            .find(|(path, _)| path == "share/python/stdlib.zip")
            .map(|(_, bytes)| bytes.as_slice()),
        Some(b"stdlib archive".as_slice())
    );
    let error = daemon_handle_daemon_sandbox_toolchain_import_package(
        &daemon_paths,
        sinomo_runtime::sandbox::SandboxImportToolchainPackageRequest {
            toolchain_id: toolchain_id.clone(),
            version: "bad-extension".to_string(),
            target_abi: sinomo_runtime::sandbox::SandboxWasiAbi::Wasix,
            provenance: "unit test".to_string(),
            commands: vec![
                sinomo_runtime::sandbox::SandboxImportToolchainPackageCommand {
                    name: "node".to_string(),
                    module_bytes: b"node module".to_vec(),
                    target_abi: None,
                    argv_prefix: Vec::new(),
                    exec_name: None,
                    default_env: std::collections::BTreeMap::new(),
                    required_capabilities: Vec::new(),
                    default_limits: sinomo_runtime::sandbox::SandboxResourceLimits::default(),
                },
                sinomo_runtime::sandbox::SandboxImportToolchainPackageCommand {
                    name: "python".to_string(),
                    module_bytes: b"duplicate python".to_vec(),
                    target_abi: None,
                    argv_prefix: Vec::new(),
                    exec_name: None,
                    default_env: std::collections::BTreeMap::new(),
                    required_capabilities: Vec::new(),
                    default_limits: sinomo_runtime::sandbox::SandboxResourceLimits::default(),
                },
            ],
            artifacts: Vec::new(),
        },
    )
    .expect_err("duplicate package command denied");
    assert!(format!("{error:?}").contains("duplicate sandbox toolchain command"));
    let manifest = toolchain_store
        .load_manifest(&toolchain_id)
        .expect("reload toolchain");
    assert!(manifest.command("python").is_ok());
    assert!(manifest.command("node").is_err());
    let mut restrictive_policy = sinomo_runtime::sandbox::SandboxClusterPolicyConfig::default();
    restrictive_policy.policy.max_limits.memory_mb = 128;
    let txn = sinomo_runtime::sandbox::sandbox_policy_config_cluster_kv_txn(&restrictive_policy)
        .expect("restrictive policy txn");
    handle_daemon_cluster_kv_txn_request(&daemon_paths, txn).expect("write restrictive policy");
    let denied = daemon_handle_daemon_sandbox_toolchain_import_package(
        &daemon_paths,
        sinomo_runtime::sandbox::SandboxImportToolchainPackageRequest {
            toolchain_id: toolchain_id.clone(),
            version: "denied-extension".to_string(),
            target_abi: sinomo_runtime::sandbox::SandboxWasiAbi::Wasix,
            provenance: "unit test".to_string(),
            commands: vec![
                sinomo_runtime::sandbox::SandboxImportToolchainPackageCommand {
                    name: "node".to_string(),
                    module_bytes: b"node module".to_vec(),
                    target_abi: None,
                    argv_prefix: Vec::new(),
                    exec_name: None,
                    default_env: std::collections::BTreeMap::new(),
                    required_capabilities: Vec::new(),
                    default_limits: sinomo_runtime::sandbox::SandboxResourceLimits::default(),
                },
            ],
            artifacts: vec![
                sinomo_runtime::sandbox::SandboxImportToolchainPackageArtifact {
                    relative_path: "share/node/runtime.zip".to_string(),
                    bytes: b"node runtime".to_vec(),
                },
            ],
        },
    )
    .expect_err("policy-denied package rejected");
    let denied = format!("{denied:?}");
    assert!(denied.contains("resource-limit-denied"), "{denied}");
    assert!(denied.contains("memory-mb"), "{denied}");
    let malformed = daemon_handle_daemon_sandbox_toolchain_import_package(
        &daemon_paths,
        sinomo_runtime::sandbox::SandboxImportToolchainPackageRequest {
            toolchain_id: toolchain_id.clone(),
            version: "malformed-extension".to_string(),
            target_abi: sinomo_runtime::sandbox::SandboxWasiAbi::Wasix,
            provenance: "unit test".to_string(),
            commands: vec![
                sinomo_runtime::sandbox::SandboxImportToolchainPackageCommand {
                    name: "curl".to_string(),
                    module_bytes: b"curl module".to_vec(),
                    target_abi: None,
                    argv_prefix: vec!["--bad\0arg".to_string()],
                    exec_name: None,
                    default_env: std::collections::BTreeMap::new(),
                    required_capabilities: Vec::new(),
                    default_limits: sinomo_runtime::sandbox::SandboxResourceLimits::default(),
                },
            ],
            artifacts: Vec::new(),
        },
    )
    .expect_err("malformed argv prefix rejected before write");
    assert!(format!("{malformed:?}").contains("embedded NUL byte"));
    let manifest = toolchain_store
        .load_manifest(&toolchain_id)
        .expect("reload toolchain after denied package");
    assert!(manifest.command("python").is_ok());
    assert!(manifest.command("node").is_err());
    assert!(manifest.command("curl").is_err());
    assert!(manifest.artifact("share/python/stdlib.zip").is_ok());
    assert!(manifest.artifact("share/node/runtime.zip").is_err());

    let event_store =
        sinomo_runtime::events::JsonlEventStore::new(root.join("events").join("event-log.jsonl"));
    let events = event_store
        .replay(ReplayQuery::by_kind_prefix("sandbox.toolchain"))
        .expect("replay toolchain events");
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].event.kind.as_str(), "sandbox.toolchain.updated");
    assert_eq!(events[1].event.kind.as_str(), "sandbox.toolchain.updated");
    assert_eq!(
        events[1].event.payload["module_hash"],
        response.package_hash
    );
    let policy_events = event_store
        .replay(ReplayQuery::by_kind_prefix("sandbox.policy"))
        .expect("replay policy events");
    assert!(
        policy_events
            .iter()
            .any(|event| event.event.payload["phase"] == "sandbox-toolchain-import-package")
    );
}
#[test]
#[cfg(feature = "sandbox")]
fn daemon_sandbox_write_to_read_only_mount_emits_denial_events() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-daemon-sandbox-write-ro-mount");
    let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(
                ClusterId::from_legacy_named("cluster_sandbox_write_ro_mount").expect("cluster"),
            ),
            node_id: Some(NodeId::from_legacy_named("node_sandbox_write_ro_mount").expect("node")),
            name: "sandbox write read-only mount cluster".to_string(),
        },
        &paths,
    )
    .expect("cluster init");
    let daemon_paths = DaemonPaths::with_root(root.join("daemon"));
    let sandbox_id = sinomo_runtime::sandbox::SandboxId::generate();
    let toolchain_id = sinomo_runtime::sandbox::SandboxToolchainId::generate();
    let mut dfs =
        super::daemon_open_daemon_sandbox_dfs_service(&daemon_paths).expect("open dfs service");
    dfs.posix_mkdir_all("/services/sandbox/shared")
        .expect("shared source");
    let mut instance_store = sinomo_runtime::sandbox::SifsSandboxInstanceStore::new(dfs);
    let mut manifest = instance_store
        .create(sandbox_id.clone(), toolchain_id, OffsetDateTime::now_utc())
        .expect("create sandbox");
    manifest.mounts.push(sinomo_runtime::sandbox::SandboxMount {
        source_sifs_path: "/services/sandbox/shared".to_string(),
        mount_path: sinomo_runtime::sandbox::SandboxPath::new("/host").expect("mount path"),
        mode: sinomo_runtime::sandbox::SandboxMountMode::ReadWrite,
    });
    instance_store.save_manifest(&manifest).expect("save mount");
    drop(instance_store);

    daemon_handle_daemon_sandbox_write(
        &daemon_paths,
        sinomo_runtime::sandbox::SandboxWriteRequest {
            sandbox_id: sandbox_id.clone(),
            path: sinomo_runtime::sandbox::SandboxPath::new("/host/existing.txt").expect("path"),
            bytes: b"from mount\n".to_vec(),
        },
    )
    .expect("rw mount write seeds source");
    let dfs =
        super::daemon_open_daemon_sandbox_dfs_service(&daemon_paths).expect("reopen dfs service");
    let mut instance_store = sinomo_runtime::sandbox::SifsSandboxInstanceStore::new(dfs);
    let mut manifest = instance_store.load(&sandbox_id).expect("reload manifest");
    manifest.mounts[0].mode = sinomo_runtime::sandbox::SandboxMountMode::ReadOnly;
    instance_store
        .save_manifest(&manifest)
        .expect("save ro mount");
    drop(instance_store);
    let mounted_read = daemon_handle_daemon_sandbox_read(
        &daemon_paths,
        sinomo_runtime::sandbox::SandboxReadRequest {
            sandbox_id: sandbox_id.clone(),
            path: sinomo_runtime::sandbox::SandboxPath::new("/host/existing.txt").expect("path"),
            range: None,
        },
    )
    .expect("read from mounted source");
    assert_eq!(mounted_read.bytes, b"from mount\n");

    let error = daemon_handle_daemon_sandbox_write(
        &daemon_paths,
        sinomo_runtime::sandbox::SandboxWriteRequest {
            sandbox_id: sandbox_id.clone(),
            path: sinomo_runtime::sandbox::SandboxPath::new("/host/out.txt").expect("path"),
            bytes: b"denied\n".to_vec(),
        },
    )
    .expect_err("read-only mount write denied");
    assert!(format!("{error:?}").contains("read-only"));

    let dfs =
        super::daemon_open_daemon_sandbox_dfs_service(&daemon_paths).expect("reopen dfs service");
    let missing_mount_write = dfs
        .posix_read_file("/services/sandbox/shared/out.txt")
        .expect_err("denied write must not mutate the read-only mount source");
    assert!(format!("{missing_mount_write:?}").contains("NotFound"));
    let event_store =
        sinomo_runtime::events::JsonlEventStore::new(root.join("events").join("event-log.jsonl"));
    let policy_events = event_store
        .replay(ReplayQuery::by_kind_prefix("sandbox.policy"))
        .expect("replay policy events");
    assert_eq!(policy_events.len(), 1);
    assert_eq!(
        policy_events[0].event.kind.as_str(),
        "sandbox.policy.denied"
    );
    assert_eq!(policy_events[0].event.payload["phase"], "sandbox-write");
    assert_eq!(
        policy_events[0].event.payload["code"],
        "mount-read-only-denied"
    );
    assert_eq!(
        policy_events[0].event.payload["sandbox_id"],
        sandbox_id.to_string()
    );
    let mount_events = event_store
        .replay(ReplayQuery::by_kind_prefix("sandbox.mount"))
        .expect("replay mount events");
    assert_eq!(mount_events.len(), 1);
    assert_eq!(mount_events[0].event.kind.as_str(), "sandbox.mount.denied");
    assert_eq!(mount_events[0].event.payload["phase"], "sandbox-write");
    assert_eq!(
        mount_events[0].event.payload["code"],
        "mount-read-only-denied"
    );
}

#[test]
fn daemon_cluster_kv_txn_does_not_ack_without_append_quorum() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-cluster-kv-no-quorum");
    let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(
                ClusterId::from_legacy_named("cluster_kv_no_quorum").expect("cluster"),
            ),
            node_id: Some(NodeId::from_legacy_named("node_kv_no_quorum").expect("node")),
            name: "kv no quorum cluster".to_string(),
        },
        &paths,
    )
    .expect("cluster init");
    let mut state = read_cluster_state(&paths).expect("cluster state");
    let closed_peer_listener = TcpListener::bind("127.0.0.1:0").expect("bind closed peer address");
    let peer_address = closed_peer_listener
        .local_addr()
        .expect("closed peer address")
        .to_string();
    drop(closed_peer_listener);
    let peer_node_id = NodeId::from_legacy_named("node_kv_no_quorum_peer").expect("node");
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
    sinomo_runtime::network::write_cluster_state(&paths, &state).expect("write cluster state");
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
    let result =
        handle_daemon_cluster_kv_txn_request(&DaemonPaths::with_root(root.join("daemon")), txn);

    assert!(result.is_err(), "unexpected successful txn: {result:?}");
    assert!(
        !read_cluster_kv_snapshot(&paths)
            .expect("cluster snapshot")
            .entries
            .iter()
            .any(|entry| entry.key == "/config/unsafe")
    );
}

#[test]
fn daemon_cluster_kv_is_read_only_without_cluster_state() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-cluster-kv-no-cluster");
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

    let result = handle_daemon_cluster_kv_txn_request(&daemon_paths, txn);

    assert!(matches!(result, Err(CliError::ClusterReadOnly { .. })));
    let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(&root));
    assert_eq!(
        read_cluster_kv_snapshot(&cluster_paths)
            .expect("snapshot")
            .entries,
        Vec::new()
    );
    assert_eq!(
        sinomo_runtime::kv::read_cluster_kv_prefix_entries(&cluster_paths.kv_paths(), "/config")
            .expect("range"),
        Vec::new()
    );
}

#[test]
fn single_node_cluster_has_local_quorum() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-single-node-quorum");
    let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(
                ClusterId::from_legacy_named("cluster_single_quorum").expect("cluster"),
            ),
            node_id: Some(NodeId::from_legacy_named("node_single_quorum").expect("node")),
            name: "single node quorum cluster".to_string(),
        },
        &paths,
    )
    .expect("cluster init");
    let state = read_cluster_state(&paths).expect("cluster state");

    assert!(cluster_has_local_quorum(&paths, &state));
}

#[test]
fn network_raft_handlers_reject_mismatched_authenticated_peer() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-raft-auth-binding");
    let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(ClusterId::from_legacy_named("cluster_raft_auth").expect("cluster")),
            node_id: Some(NodeId::from_legacy_named("node_raft_auth").expect("node")),
            name: "raft auth cluster".to_string(),
        },
        &paths,
    )
    .expect("cluster init");
    let state = read_cluster_state(&paths).expect("cluster state");
    super::network::sync_cluster_state_to_local_raft_unchecked(&paths, &state)
        .expect("sync state to kv");
    let bad_auth = authenticated_test_command(
        &super::daemon_rpc_command(sinomo_runtime::kv::CLUSTER_RAFT_APPEND_ROUTE),
        super::DaemonSignerAuthority::Voter,
        "node_other",
        "pk_other",
    );

    let append_request = build_cluster_raft_append_request_from_next_index(&paths, &state, 1)
        .expect("append request");
    let append_response =
        handle_cluster_raft_append_rpc(&paths, append_request, &bad_auth).expect("append response");
    assert!(!append_response.accepted);

    let vote_metadata = paths
        .kv_paths()
        .raft_store()
        .local_vote_metadata(&state.local_node_id)
        .expect("raft vote metadata");
    let vote_request = sinomo_runtime::kv::ClusterRaftVoteRequest {
        state: state.clone(),
        vote: sinomo_runtime::kv::RaftRequestVoteRequest {
            term: vote_metadata.current_term.saturating_add(1),
            candidate_id: state.local_node_id.clone(),
            last_log_index: vote_metadata.last_log_index,
            last_log_term: vote_metadata.last_log_term,
        },
    };
    let vote_response =
        handle_cluster_raft_vote_rpc(&paths, vote_request, &bad_auth).expect("vote response");
    assert!(!vote_response.accepted);

    let remote_node_id = NodeId::from_legacy_named("node_forged_voter").expect("remote node");
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
        &super::daemon_rpc_command(sinomo_runtime::kv::CLUSTER_RAFT_APPEND_ROUTE),
        super::DaemonSignerAuthority::Voter,
        remote_node_id.as_str(),
        "pk_forged_voter",
    );
    let forged_append = sinomo_runtime::kv::ClusterRaftAppendRequest {
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
    let forged_append_response =
        handle_cluster_raft_append_rpc(&paths, forged_append, &forged_auth)
            .expect("forged append response");
    assert!(!forged_append_response.accepted);
    assert!(
        !read_cluster_state(&paths)
            .expect("state after forged append")
            .members
            .iter()
            .any(|member| member.node_id == remote_node_id)
    );

    let forged_vote = sinomo_runtime::kv::ClusterRaftVoteRequest {
        state: forged_state,
        vote: sinomo_runtime::kv::RaftRequestVoteRequest {
            term: 2,
            candidate_id: remote_node_id.clone(),
            last_log_index: 0,
            last_log_term: 0,
        },
    };
    let forged_vote_response = handle_cluster_raft_vote_rpc(&paths, forged_vote, &forged_auth)
        .expect("forged vote response");
    assert!(!forged_vote_response.accepted);
}

#[test]
fn stale_raft_snapshot_append_does_not_roll_back_local_state() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-stale-snapshot");
    let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(
                ClusterId::from_legacy_named("cluster_stale_snapshot").expect("cluster"),
            ),
            node_id: Some(NodeId::from_legacy_named("node_stale_snapshot").expect("node")),
            name: "stale snapshot cluster".to_string(),
        },
        &paths,
    )
    .expect("cluster init");
    let state = read_cluster_state(&paths).expect("cluster state");
    super::network::sync_cluster_state_to_local_raft_unchecked(&paths, &state)
        .expect("sync state to kv");
    let stale_snapshot = read_cluster_kv_snapshot(&paths).expect("stale snapshot");
    append_cluster_raft_command_with_response(
        &paths,
        &state.local_node_id,
        sinomo_runtime::kv::KvCommand::Op(KvOp::Put {
            key: "/config/current".to_string(),
            value: b"current".to_vec(),
            lease_id: None,
        }),
    )
    .expect("current write");
    let current_snapshot = read_cluster_kv_snapshot(&paths).expect("current snapshot");
    assert!(
        current_snapshot
            .entries
            .iter()
            .any(|entry| entry.key == "/config/current")
    );

    let restored = paths
        .kv_paths()
        .local_cache()
        .restore_raft_from_snapshot_if_newer(state.local_node_id.clone(), stale_snapshot)
        .expect("stale snapshot restore");
    assert!(!restored);
    let snapshot_after_stale =
        read_cluster_kv_snapshot(&paths).expect("snapshot after stale restore");
    assert!(
        snapshot_after_stale
            .entries
            .iter()
            .any(|entry| entry.key == "/config/current" && entry.value == b"current")
    );
}

#[test]
fn raft_append_request_can_target_follower_next_index() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-targeted-append");
    let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(
                ClusterId::from_legacy_named("cluster_targeted_append").expect("cluster"),
            ),
            node_id: Some(NodeId::from_legacy_named("node_targeted_append").expect("node")),
            name: "targeted append cluster".to_string(),
        },
        &paths,
    )
    .expect("cluster init");
    let state = read_cluster_state(&paths).expect("cluster state");
    append_cluster_raft_command_with_response(
        &paths,
        &state.local_node_id,
        sinomo_runtime::kv::KvCommand::Op(KvOp::Put {
            key: "/config/targeted/a".to_string(),
            value: b"a".to_vec(),
            lease_id: None,
        }),
    )
    .expect("first raft write");
    append_cluster_raft_command_with_response(
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
}

#[test]
fn daemon_node_status_publishes_version_and_rollout_to_kv() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-node-status");
    let data_dir = SinomoDataDir::new(&root);
    let cluster_paths = ClusterPaths::new(&data_dir);
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(ClusterId::from_legacy_named("cluster_status").expect("cluster")),
            node_id: Some(NodeId::from_legacy_named("node_status").expect("node")),
            name: "status cluster".to_string(),
        },
        &cluster_paths,
    )
    .expect("cluster init");
    let daemon_paths = DaemonPaths::with_root(root.join("daemon"));
    daemon_paths.ensure().expect("daemon paths");
    let rollout_paths = SelfRolloutPaths::new(&root);
    ensure_rollout_layout(&rollout_paths).expect("rollout paths");
    write_rollout_status(
        &rollout_paths,
        &sinomo_runtime::core::ClusterNodeRolloutStatus {
            status: "accepted".to_string(),
            mode: Some("cluster-remote-build".to_string()),
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
        },
    )
    .expect("rollout state");
    let status = DaemonStatus {
        pid: 4242,
        host: "0.0.0.0".to_string(),
        port: 21_353,
        address: "127.0.0.1:21353".to_string(),
        version: daemon_package_version(),
        commit: None,
        target_os: Some(std::env::consts::OS.to_string()),
        target_arch: Some(std::env::consts::ARCH.to_string()),
        executable: None,
        rollout: None,
        started_at: "2026-05-04T00:00:00Z".to_string(),
        data_dir: root.to_path_buf(),
        lock_file: daemon_paths.lock.clone(),
        cluster: Some(DaemonClusterStatus {
            cluster_id: ClusterId::from_legacy_named("cluster_status").expect("cluster"),
            node_id: NodeId::from_legacy_named("node_status").expect("node"),
            process: super::DAEMON_MAIN_CLUSTER_ROLE.to_string(),
            member_role: ClusterMemberRole::Voter,
            member_state: ClusterMemberState::Active,
        }),
        idle_stats: None,
    };
    write_daemon_status(&daemon_paths, &status).expect("write status");

    let cluster = status.cluster.as_ref().expect("cluster status");
    let record = cluster_node_status_from_daemon_status(
        &status,
        cluster,
        read_cluster_rollout_status(&rollout_paths).expect("read rollout"),
        time::OffsetDateTime::now_utc(),
    );
    sinomo_runtime::network::sync_cluster_node_status_to_kv(&cluster_paths, record)
        .expect("refresh status");
    let statuses =
        read_cluster_node_statuses_from_kv(&cluster_paths).expect("node statuses from kv");

    assert_eq!(statuses.len(), 1);
    assert_eq!(
        statuses[0].node_id,
        NodeId::from_legacy_named("node_status").expect("node")
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
}

#[test]
fn daemon_cluster_raft_append_replicates_leader_kv_to_peer() {
    let host_root = sinomo_testdir::TestDir::new("sinomo-cli-cluster-sync-host");
    let peer_root = sinomo_testdir::TestDir::new("sinomo-cli-cluster-sync-peer");
    let host_paths = ClusterPaths::new(&SinomoDataDir::new(&host_root));
    let peer_paths = ClusterPaths::new(&SinomoDataDir::new(&peer_root));
    let cluster_id = ClusterId::from_legacy_named("cluster_sync").expect("cluster");
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(cluster_id.clone()),
            node_id: Some(NodeId::from_legacy_named("node_host").expect("node")),
            name: "sync cluster".to_string(),
        },
        &host_paths,
    )
    .expect("cluster init");

    let host_identity =
        super::load_or_create_main_node_identity(&host_root).expect("host identity");
    let mut invite = ClusterJoinInvite {
        invite_id: ClusterInviteId::from_legacy_named("invite_sync").expect("invite"),
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
        issuer_node_id: NodeId::from_legacy_named("node_host").expect("node"),
        issuer_public_key: host_identity.public_key.clone(),
        signature: String::new(),
    };
    invite.signature = sign_cluster_join_invite(&invite, &host_identity).expect("sign invite");
    cluster_join_with_paths(
        ClusterJoinOptions {
            invite: encode_cluster_join_invite(&invite).expect("invite uri"),
            cluster_id: Some(cluster_id.clone()),
            node_id: Some(NodeId::from_legacy_named("node_peer").expect("node")),
            name: "peer".to_string(),
        },
        &peer_paths,
    )
    .expect("cluster join");

    let host_state = read_cluster_state(&host_paths).expect("host state");
    let peer_state = read_cluster_state(&peer_paths).expect("peer state");
    let host_auth = authenticated_test_command(
        &super::daemon_rpc_command(sinomo_runtime::kv::CLUSTER_RAFT_APPEND_ROUTE),
        super::DaemonSignerAuthority::Voter,
        host_state.local_node_id.as_str(),
        host_identity.public_key.clone(),
    );
    super::network::sync_cluster_state_to_local_raft_unchecked(&host_paths, &host_state)
        .expect("host kv");
    let request = build_cluster_raft_append_request_from_next_index(&host_paths, &host_state, 1)
        .expect("raft append");
    let peer_response = handle_cluster_raft_append_rpc(&peer_paths, request.clone(), &host_auth)
        .expect("peer append");

    assert!(peer_response.accepted);
    let peer_members = read_cluster_members_from_kv(&peer_paths).expect("peer kv members");
    assert!(
        peer_members
            .iter()
            .any(|member| member.node_id == NodeId::from_legacy_named("node_host").expect("node"))
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
    handle_daemon_cluster_kv_txn_request(
        &DaemonPaths::with_root(host_root.join("daemon")),
        latency_txn,
    )
    .expect("host latency txn");
    let current_request =
        build_cluster_raft_append_request_from_next_index(&host_paths, &host_state, 1)
            .expect("current raft append");
    let current_response = handle_cluster_raft_append_rpc(&peer_paths, current_request, &host_auth)
        .expect("peer current append");
    assert!(current_response.accepted);
    assert!(
        read_cluster_kv_snapshot(&peer_paths)
            .expect("peer current snapshot")
            .entries
            .iter()
            .any(|entry| entry.key == "/config/latency" && entry.value == b"current")
    );

    let delayed_stale_response = handle_cluster_raft_append_rpc(&peer_paths, request, &host_auth)
        .expect("peer stale append");
    assert!(delayed_stale_response.accepted);
    assert!(
        read_cluster_kv_snapshot(&peer_paths)
            .expect("peer after stale snapshot")
            .entries
            .iter()
            .any(|entry| entry.key == "/config/latency" && entry.value == b"current")
    );

    let watch = sinomo_runtime::kv::create_cluster_kv_watch_for_node(
        &peer_paths.kv_paths(),
        peer_state.local_node_id.clone(),
        sinomo_runtime::kv::ClusterKvWatchPrefixRequest {
            prefix: "/config/watch/".to_string(),
            start_revision: KvRevision(1),
        },
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
    handle_daemon_cluster_kv_txn_request(
        &DaemonPaths::with_root(host_root.join("daemon")),
        watch_txn,
    )
    .expect("host watch txn");
    let watch_append =
        build_cluster_raft_append_request_from_next_index(&host_paths, &host_state, 1)
            .expect("watch raft append");
    assert!(
        handle_cluster_raft_append_rpc(&peer_paths, watch_append, &host_auth)
            .expect("peer watch append")
            .accepted
    );
    let host_events = read_cluster_kv_snapshot(&host_paths)
        .expect("host watch snapshot")
        .events;
    let watch_fanout = network::handle_cluster_kv_watch_events(
        &peer_paths,
        sinomo_runtime::kv::ClusterKvWatchEventsRequest {
            state: host_state.clone(),
            leader_id: host_state.local_node_id.clone(),
            events: host_events,
        },
    )
    .expect("watch event fanout");
    assert!(watch_fanout.accepted);
    let drained = sinomo_runtime::kv::drain_cluster_kv_watch_for_node(
        &peer_paths.kv_paths(),
        peer_state.local_node_id.clone(),
        sinomo_runtime::kv::ClusterKvWatchDrainRequest {
            watch_id: watch.watch_id,
            wait_timeout_millis: 0,
        },
    )
    .expect("drain peer watch");
    assert!(
        drained
            .events
            .iter()
            .any(|event| event.key == "/config/watch/a")
    );
    let drained_again = sinomo_runtime::kv::drain_cluster_kv_watch_for_node(
        &peer_paths.kv_paths(),
        peer_state.local_node_id.clone(),
        sinomo_runtime::kv::ClusterKvWatchDrainRequest {
            watch_id: watch.watch_id,
            wait_timeout_millis: 0,
        },
    )
    .expect("drain peer watch again");
    assert!(drained_again.events.is_empty());
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
            "--force-source".to_string(),
            "--target".to_string(),
            "192.0.2.11:21353".to_string(),
            "--skip-local".to_string(),
        ])
        .expect("remote-build cluster"),
        RolloutCliCommand::Cluster(RolloutOptions {
            remote_build: true,
            remote_repo: Some(std::path::PathBuf::from("/srv/sinomo-agent")),
            force_source_overwrite: true,
            targets: vec!["192.0.2.11:21353".to_string()],
            skip_local: true,
            ..RolloutOptions::default()
        })
    );
    assert_eq!(
        parse_rollout_command(&[
            "module".to_string(),
            "native-dylib".to_string(),
            "module.remote-management".to_string(),
            "--repo".to_string(),
            "/work/sinomo".to_string(),
            "--target".to_string(),
            "192.0.2.11:21353".to_string(),
            "--skip-local".to_string(),
        ])
        .expect("module native-dylib rollout"),
        RolloutCliCommand::ModuleNativeDylib(RolloutModuleNativeDylibOptions {
            module_selector: "module.remote-management".to_string(),
            rollout: RolloutOptions {
                repo: Some(std::path::PathBuf::from("/work/sinomo")),
                targets: vec!["192.0.2.11:21353".to_string()],
                skip_local: true,
                ..RolloutOptions::default()
            },
        })
    );
    assert_eq!(
        parse_rollout_command(&["apply".to_string(), "--force-overwrite".to_string()])
            .expect("apply force overwrite"),
        RolloutCliCommand::Apply(RolloutOptions {
            force_source_overwrite: true,
            ..RolloutOptions::default()
        })
    );
    assert_eq!(
        parse_rollout_command(&[
            "diagnostics".to_string(),
            "--target".to_string(),
            "192.0.2.11:21353".to_string(),
        ])
        .expect("diagnostics"),
        RolloutCliCommand::Diagnostics(RolloutOptions {
            targets: vec!["192.0.2.11:21353".to_string()],
            ..RolloutOptions::default()
        })
    );
    assert_eq!(
        parse_rollout_command(&[
            "clear-status".to_string(),
            "--target".to_string(),
            "192.0.2.11:21353".to_string(),
        ])
        .expect("clear-status"),
        RolloutCliCommand::ClearStatus(RolloutOptions {
            targets: vec!["192.0.2.11:21353".to_string()],
            ..RolloutOptions::default()
        })
    );
    assert_eq!(
        parse_rollout_command(&[
            "advance".to_string(),
            "--node".to_string(),
            "01hxy6qa9k0t9vr4g9n8d3q5d5".to_string(),
            "--scorecard".to_string(),
            "/tmp/scorecard.json".to_string(),
            "--incident".to_string(),
            "01hxy6qa9k0t9vr4g9n8d3q5d6".to_string(),
            "--user-corrections".to_string(),
            "2".to_string(),
            "--rollback-trigger".to_string(),
            "timeout".to_string(),
            "--evidence".to_string(),
            "manual://operator-note".to_string(),
        ])
        .expect("advance"),
        RolloutCliCommand::Advance(RolloutAdvanceOptions {
            node_id: Some(NodeId::new("01hxy6qa9k0t9vr4g9n8d3q5d5").expect("node")),
            scorecard: Some(PathBuf::from("/tmp/scorecard.json")),
            module_replay_report: None,
            shadow_telemetry_limit: None,
            incident_event_ids: vec![EventId::new("01hxy6qa9k0t9vr4g9n8d3q5d6").expect("event")],
            user_correction_count: 2,
            rollback_triggers: vec![RollbackTrigger::Timeout],
            evidence_refs: vec!["manual://operator-note".to_string()],
        })
    );
    assert_eq!(
        parse_rollout_command(&[
            "advance".to_string(),
            "--module-replay-report".to_string(),
            "/tmp/replay-report.json".to_string(),
        ])
        .expect("advance replay"),
        RolloutCliCommand::Advance(RolloutAdvanceOptions {
            node_id: None,
            scorecard: None,
            module_replay_report: Some(PathBuf::from("/tmp/replay-report.json")),
            shadow_telemetry_limit: None,
            incident_event_ids: Vec::new(),
            user_correction_count: 0,
            rollback_triggers: Vec::new(),
            evidence_refs: Vec::new(),
        })
    );
    assert_eq!(
        parse_rollout_command(&[
            "advance".to_string(),
            "--shadow-telemetry".to_string(),
            "--shadow-telemetry-limit".to_string(),
            "25".to_string(),
        ])
        .expect("advance shadow telemetry"),
        RolloutCliCommand::Advance(RolloutAdvanceOptions {
            node_id: None,
            scorecard: None,
            module_replay_report: None,
            shadow_telemetry_limit: Some(25),
            incident_event_ids: Vec::new(),
            user_correction_count: 0,
            rollback_triggers: Vec::new(),
            evidence_refs: Vec::new(),
        })
    );
    assert_eq!(
        parse_rollout_command(&[
            "telemetry-gates".to_string(),
            "set".to_string(),
            "--disable-auto-promotion".to_string(),
            "--event-limit".to_string(),
            "250".to_string(),
            "--min-events".to_string(),
            "20".to_string(),
            "--max-auto-weight-bps".to_string(),
            "750".to_string(),
            "--max-candidate-latency-p95-ms".to_string(),
            "125".to_string(),
        ])
        .expect("telemetry gates set"),
        RolloutCliCommand::TelemetryGatesSet(RolloutTelemetryGateSetOptions {
            automatic_promotion_enabled: Some(false),
            event_limit: Some(250),
            minimum_event_count: Some(20),
            maximum_automatic_weight_bps: Some(Some(750)),
            maximum_candidate_latency_p95_ms: Some(Some(125)),
        })
    );
    assert_eq!(
        parse_rollout_command(&["telemetry-gates".to_string(), "show".to_string(),])
            .expect("telemetry gates show"),
        RolloutCliCommand::TelemetryGatesShow
    );
}

#[test]
fn local_rollout_wait_timeout_covers_shadow_candidate_handoff() {
    assert!(super::local_rollout_wait_timeout() >= Duration::from_secs(30));
}

#[test]
fn daemon_rollout_timeout_covers_windows_shadow_handoff() {
    assert!(super::DAEMON_ROLLOUT_TIMEOUT >= Duration::from_secs(30));
}

#[test]
fn rollout_progress_bar_formats_known_and_unknown_progress() {
    assert_eq!(rollout_progress_bar(None), "[........................]   -");
    let status = sinomo_runtime::core::ClusterNodeRolloutStatus {
        status: "in-progress".to_string(),
        mode: Some("cluster-remote-build".to_string()),
        detail: None,
        phase: Some("building".to_string()),
        progress_current: Some(2),
        progress_total: Some(5),
        progress_message: Some("cargo build running".to_string()),
        source: None,
        requested_at: None,
        staged_at: None,
        updated_at: None,
        failed_at: None,
        diagnostics: None,
        stdout_log: None,
        stderr_log: None,
        stdout_tail: None,
        stderr_tail: None,
    };
    assert_eq!(
        rollout_progress_bar(Some(&status)),
        "[=========...............]  40%"
    );
}

#[test]
fn rollout_progress_line_includes_phase_message_and_elapsed_time() {
    let status = sinomo_runtime::core::ClusterNodeRolloutStatus {
        status: "in-progress".to_string(),
        mode: Some("cluster-remote-build".to_string()),
        detail: None,
        phase: Some("building".to_string()),
        progress_current: Some(2),
        progress_total: Some(5),
        progress_message: Some("cargo build running".to_string()),
        source: None,
        requested_at: None,
        staged_at: None,
        updated_at: None,
        failed_at: None,
        diagnostics: None,
        stdout_log: None,
        stderr_log: None,
        stdout_tail: None,
        stderr_tail: None,
    };
    assert_eq!(
        format_rollout_progress_line(
            "node-a",
            "remote build",
            Some(&status),
            Duration::from_secs(12),
        ),
        "node-a: [=========...............]  40% building - cargo build running (12s)"
    );
}

#[test]
fn rollout_status_expectation_rejects_stale_terminal_progress() {
    let expectation = RolloutProgressExpectation {
        source: "repo-a".to_string(),
        requested_at: "2026-06-02T00:00:01Z".to_string(),
        source_head: Some("head-new".to_string()),
    };
    let status = sinomo_runtime::core::ClusterNodeRolloutStatus {
        status: "accepted".to_string(),
        mode: Some("cluster-remote-build".to_string()),
        detail: Some("old rollout completed".to_string()),
        phase: Some("shadow-finalized".to_string()),
        progress_current: Some(5),
        progress_total: Some(5),
        progress_message: Some("shadow rollout finalized".to_string()),
        source: Some("repo-a".to_string()),
        requested_at: Some("2026-06-02T00:00:00Z".to_string()),
        staged_at: None,
        updated_at: None,
        failed_at: None,
        diagnostics: None,
        stdout_log: None,
        stderr_log: None,
        stdout_tail: None,
        stderr_tail: None,
    };

    assert!(!rollout_status_matches_expectation(&status, &expectation));
    assert!(rollout_terminal_response(&status, &expectation).is_none());
}

#[test]
fn rollout_terminal_response_preserves_expected_source_head() {
    let expectation = RolloutProgressExpectation {
        source: "repo-a".to_string(),
        requested_at: "2026-06-02T00:00:01Z".to_string(),
        source_head: Some("head-new".to_string()),
    };
    let status = sinomo_runtime::core::ClusterNodeRolloutStatus {
        status: "accepted".to_string(),
        mode: Some("cluster-remote-build".to_string()),
        detail: Some("shadow rollout finalized".to_string()),
        phase: Some("shadow-finalized".to_string()),
        progress_current: Some(5),
        progress_total: Some(5),
        progress_message: Some("shadow rollout finalized".to_string()),
        source: Some("repo-a".to_string()),
        requested_at: Some("2026-06-02T00:00:01Z".to_string()),
        staged_at: None,
        updated_at: None,
        failed_at: None,
        diagnostics: None,
        stdout_log: None,
        stderr_log: None,
        stdout_tail: None,
        stderr_tail: None,
    };

    assert!(rollout_status_matches_expectation(&status, &expectation));
    let response = rollout_terminal_response(&status, &expectation).expect("terminal response");
    assert_eq!(response.status, "accepted");
    assert_eq!(response.source_head.as_deref(), Some("head-new"));
}

#[test]
fn rollout_pending_error_grace_extends_for_matching_active_status() {
    let expectation = RolloutProgressExpectation {
        source: "repo-a".to_string(),
        requested_at: "2026-06-02T00:00:01Z".to_string(),
        source_head: Some("head-new".to_string()),
    };
    let mut status = sinomo_runtime::core::ClusterNodeRolloutStatus {
        status: "in-progress".to_string(),
        mode: Some("cluster-remote-build".to_string()),
        detail: Some("cargo build running".to_string()),
        phase: Some("building".to_string()),
        progress_current: Some(2),
        progress_total: Some(5),
        progress_message: Some("cargo build running".to_string()),
        source: Some("repo-a".to_string()),
        requested_at: Some("2026-06-02T00:00:01Z".to_string()),
        staged_at: None,
        updated_at: None,
        failed_at: None,
        diagnostics: None,
        stdout_log: None,
        stderr_log: None,
        stdout_tail: None,
        stderr_tail: None,
    };

    assert_eq!(
        rollout_pending_error_grace(None, &expectation),
        Duration::from_secs(45)
    );
    assert_eq!(
        rollout_pending_error_grace(Some(&status), &expectation),
        Duration::from_secs(15 * 60)
    );

    status.requested_at = Some("2026-06-02T00:00:00Z".to_string());
    assert_eq!(
        rollout_pending_error_grace(Some(&status), &expectation),
        Duration::from_secs(45)
    );

    status.requested_at = Some("2026-06-02T00:00:01Z".to_string());
    status.status = "accepted".to_string();
    status.phase = Some("shadow-finalized".to_string());
    assert_eq!(
        rollout_pending_error_grace(Some(&status), &expectation),
        Duration::from_secs(45)
    );
}

#[test]
fn rollout_cluster_targets_deduplicate_explicit_targets() {
    let targets = rollout_cluster_targets(&RolloutOptions {
        targets: vec![
            "192.0.2.11:21353".to_string(),
            "192.0.2.11:21353".to_string(),
            "iroh://iroh_public_key_dedup?relay=https://relay.example./".to_string(),
            "iroh://iroh_public_key_dedup?relay=https://relay.example./".to_string(),
        ],
        skip_local: true,
        ..RolloutOptions::default()
    })
    .expect("targets");

    assert_eq!(targets.len(), 2);
    assert_eq!(targets[0].address, "192.0.2.11:21353");
    assert_eq!(
        targets[1].address,
        "iroh://iroh_public_key_dedup?relay=https://relay.example./"
    );
}

#[test]
fn rollout_cluster_targets_include_shadow_only_repair_route() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-rollout-shadow-only-repair");
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");

    let cluster_id =
        ClusterId::from_legacy_named("cluster_rollout_shadow_only_repair").expect("cluster");
    let local_node_id =
        NodeId::from_legacy_named("node_rollout_shadow_only_repair_local").expect("local node");
    let remote_main_id =
        NodeId::from_legacy_named("node_rollout_shadow_only_repair_main").expect("main node");
    let remote_shadow_id = shadow_daemon_node_id(&remote_main_id);
    let now = OffsetDateTime::now_utc();
    let mut state = ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "rollout shadow-only repair",
        local_node_id,
        "local",
        "local-key",
        now,
    );
    let mut remote_main = ClusterMemberRecord::local_voter(
        cluster_id.clone(),
        remote_main_id.clone(),
        "remote main",
        "remote-main-key",
        now,
    );
    remote_main.role = ClusterMemberRole::Learner;
    remote_main.state = ClusterMemberState::Learner;
    remote_main.capabilities = main_daemon_capabilities(&ClusterMemberRole::Learner);
    remote_main.allowed_capabilities = remote_main.capabilities.clone();
    let mut remote_shadow = ClusterMemberRecord::local_non_voting(
        cluster_id,
        remote_shadow_id.clone(),
        "remote shadow",
        "remote-shadow-key",
        now,
    );
    let shadow_iroh = invite_introducer_endpoint(
        &remote_shadow_id,
        "iroh://rollout_shadow_only_repair?relay=https://relay.example./".to_string(),
    );
    remote_shadow.endpoints.push(shadow_iroh.clone());
    state.members.push(remote_main);
    state.members.push(remote_shadow);

    let targets = rollout_cluster_targets_from_state(&paths, &state, None);

    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0].address, shadow_iroh.address);
    assert_eq!(targets[0].rollout_node_id.as_ref(), Some(&remote_main_id));
}

#[test]
fn rollout_cluster_targets_keep_one_route_per_main_node() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-rollout-one-route-per-main");
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");

    let cluster_id =
        ClusterId::from_legacy_named("cluster_rollout_one_route_per_main").expect("cluster");
    let local_node_id =
        NodeId::from_legacy_named("node_rollout_one_route_per_main_local").expect("local node");
    let remote_main_id =
        NodeId::from_legacy_named("node_rollout_one_route_per_main_main").expect("main node");
    let remote_shadow_id = shadow_daemon_node_id(&remote_main_id);
    let now = OffsetDateTime::now_utc();
    let mut state = ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "rollout one route per main",
        local_node_id,
        "local",
        "local-key",
        now,
    );
    let mut remote_main = ClusterMemberRecord::local_voter(
        cluster_id.clone(),
        remote_main_id.clone(),
        "remote main",
        "remote-main-key",
        now,
    );
    remote_main.role = ClusterMemberRole::Learner;
    remote_main.state = ClusterMemberState::Learner;
    remote_main.capabilities = main_daemon_capabilities(&ClusterMemberRole::Learner);
    remote_main.allowed_capabilities = remote_main.capabilities.clone();
    let main_tcp = invite_introducer_endpoint(&remote_main_id, "10.0.0.42:21353".to_string());
    let main_iroh = invite_introducer_endpoint(
        &remote_main_id,
        "iroh://rollout_main_duplicate?relay=https://relay.example./".to_string(),
    );
    remote_main.endpoints.push(main_tcp.clone());
    remote_main.endpoints.push(main_iroh);
    let mut remote_shadow = ClusterMemberRecord::local_non_voting(
        cluster_id,
        remote_shadow_id.clone(),
        "remote shadow",
        "remote-shadow-key",
        now,
    );
    remote_shadow.endpoints.push(invite_introducer_endpoint(
        &remote_shadow_id,
        "iroh://rollout_shadow_duplicate?relay=https://relay.example./".to_string(),
    ));
    state.members.push(remote_main);
    state.members.push(remote_shadow);

    let targets = rollout_cluster_targets_from_state(&paths, &state, None);

    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0].address, main_tcp.address);
    assert_eq!(targets[0].rollout_node_id.as_ref(), Some(&remote_main_id));
}

#[test]
fn rollout_endpoint_candidates_include_remote_shadow_members() {
    let cluster_id =
        ClusterId::from_legacy_named("cluster_rollout_endpoint_candidates").expect("cluster");
    let local_node_id =
        NodeId::from_legacy_named("node_rollout_endpoint_candidates_local").expect("local node");
    let remote_main_id =
        NodeId::from_legacy_named("node_rollout_endpoint_candidates_remote").expect("remote node");
    let remote_shadow_id = shadow_daemon_node_id(&remote_main_id);
    let now = OffsetDateTime::now_utc();
    let mut state = ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "rollout endpoint candidates",
        local_node_id,
        "local",
        "local-key",
        now,
    );
    let mut remote_main = ClusterMemberRecord::local_voter(
        cluster_id.clone(),
        remote_main_id.clone(),
        "remote main",
        "remote-main-key",
        now,
    );
    remote_main.role = ClusterMemberRole::Learner;
    remote_main.state = ClusterMemberState::Learner;
    remote_main.capabilities = main_daemon_capabilities(&ClusterMemberRole::Learner);
    remote_main.allowed_capabilities = remote_main.capabilities.clone();
    remote_main.endpoints.push(invite_introducer_endpoint(
        &remote_main_id,
        "10.0.0.42:21353".to_string(),
    ));
    let mut remote_shadow = ClusterMemberRecord::local_non_voting(
        cluster_id,
        remote_shadow_id.clone(),
        "remote shadow",
        "remote-shadow-key",
        now,
    );
    remote_shadow.endpoints.push(invite_introducer_endpoint(
        &remote_shadow_id,
        "10.0.0.42:21354".to_string(),
    ));
    state.members.push(remote_main);
    state.members.push(remote_shadow);

    let endpoints = rollout_endpoint_candidates(&state);

    assert!(endpoints.iter().any(|endpoint| {
        endpoint.node_id == remote_shadow_id && endpoint.address == "10.0.0.42:21354"
    }));
}

#[test]
fn rollout_cluster_target_prefers_main_endpoint_for_healthy_main_member() {
    let main_node_id = NodeId::from_legacy_named("node_rollout_shadow_main").expect("main node");
    let shadow_node_id = shadow_daemon_node_id(&main_node_id);
    let main_endpoint = invite_introducer_endpoint(&main_node_id, "10.0.0.42:21353".to_string());
    let shadow_endpoint =
        invite_introducer_endpoint(&shadow_node_id, "10.0.0.42:21354".to_string());

    let selected = rollout_target_endpoint_for_main(
        &main_node_id,
        &[main_endpoint.clone(), shadow_endpoint.clone()],
    )
    .expect("rollout target");

    assert_eq!(selected.node_id, main_node_id);
    assert_eq!(selected.address, "10.0.0.42:21353");
}

#[test]
fn rollout_cluster_target_prefers_direct_shadow_endpoint_before_iroh() {
    let main_node_id =
        NodeId::from_legacy_named("node_rollout_shadow_iroh_main").expect("main node");
    let shadow_node_id = shadow_daemon_node_id(&main_node_id);
    let lan_endpoint = invite_introducer_endpoint(&shadow_node_id, "10.0.0.42:21354".to_string());
    let mut iroh_endpoint = invite_introducer_endpoint(
        &shadow_node_id,
        "iroh://shadow_iroh_rollout?relay=https://relay.example./".to_string(),
    );
    iroh_endpoint.priority = 10;

    let selected = rollout_target_endpoint_for_main(&main_node_id, &[lan_endpoint, iroh_endpoint])
        .expect("shadow target");

    assert_eq!(selected.node_id, shadow_node_id);
    assert_eq!(selected.transport, ClusterTransportKind::LanTcp);
}

#[test]
fn rollout_cluster_target_can_preserve_connectivity_order() {
    let main_node_id =
        NodeId::from_legacy_named("node_rollout_shadow_connectivity_main").expect("main node");
    let shadow_node_id = shadow_daemon_node_id(&main_node_id);
    let lan_endpoint = invite_introducer_endpoint(&shadow_node_id, "192.0.2.42:21354".to_string());
    let iroh_endpoint = invite_introducer_endpoint(
        &shadow_node_id,
        "iroh://shadow_connectivity_rollout?relay=https://relay.example./".to_string(),
    );

    let selected =
        rollout_target_endpoint_for_main_in_order(&main_node_id, &[iroh_endpoint, lan_endpoint])
            .expect("shadow target");

    assert_eq!(selected.node_id, shadow_node_id);
    assert_eq!(selected.transport, ClusterTransportKind::Iroh);
}

#[test]
fn rollout_endpoint_order_uses_connectivity_before_transport_rank() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-rollout-connectivity-order");
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");

    let cluster_id =
        ClusterId::from_legacy_named("cluster_rollout_connectivity_order").expect("cluster");
    let local_node_id = NodeId::from_legacy_named("node_rollout_connectivity_local").expect("node");
    let remote_node_id =
        NodeId::from_legacy_named("node_rollout_connectivity_remote").expect("node");
    let state = ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "rollout connectivity order",
        local_node_id.clone(),
        "local",
        "local-public-key",
        OffsetDateTime::now_utc(),
    );
    sinomo_runtime::network::write_cluster_state(&paths, &state).expect("write state");
    super::network::sync_cluster_state_to_local_raft_unchecked(&paths, &state).expect("sync state");

    let lan_endpoint = invite_introducer_endpoint(&remote_node_id, "192.0.2.42:21354".to_string());
    let iroh_endpoint = invite_introducer_endpoint(
        &remote_node_id,
        "iroh://rollout_connectivity_remote?relay=https://relay.example./".to_string(),
    );

    let now = OffsetDateTime::now_utc();
    let records = [
        (
            &lan_endpoint,
            sinomo_runtime::network::ClusterConnectivityState::Unreachable,
        ),
        (
            &iroh_endpoint,
            sinomo_runtime::network::ClusterConnectivityState::Healthy,
        ),
    ]
    .into_iter()
    .map(|(endpoint, connectivity_state)| {
        let consecutive_failures = if matches!(
            connectivity_state,
            sinomo_runtime::network::ClusterConnectivityState::Unreachable
        ) {
            3
        } else {
            0
        };
        sinomo_runtime::network::cluster_connectivity_record(
            &state,
            endpoint,
            connectivity_state,
            None,
            consecutive_failures,
            None,
            None,
            now,
            120,
        )
    })
    .collect::<Vec<_>>();
    sinomo_runtime::network::sync_cluster_connectivity_records_to_kv(
        &paths,
        &local_node_id,
        records,
    )
    .expect("connectivity records");

    let mut endpoints = vec![lan_endpoint, iroh_endpoint.clone()];
    sort_rollout_endpoints_by_connectivity(&paths, &state, &mut endpoints);

    assert_eq!(
        endpoints.first().expect("endpoint").endpoint_id,
        iroh_endpoint.endpoint_id
    );
}

#[test]
fn rollout_target_uses_main_endpoint_when_shadow_is_unreachable() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-rollout-main-fallback");
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");

    let cluster_id =
        ClusterId::from_legacy_named("cluster_rollout_main_fallback").expect("cluster");
    let local_node_id =
        NodeId::from_legacy_named("node_rollout_main_fallback_local").expect("node");
    let main_node_id =
        NodeId::from_legacy_named("node_rollout_main_fallback_remote").expect("node");
    let shadow_node_id = shadow_daemon_node_id(&main_node_id);
    let state = ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "rollout main fallback",
        local_node_id.clone(),
        "local",
        "local-public-key",
        OffsetDateTime::now_utc(),
    );

    let main_endpoint = invite_introducer_endpoint(&main_node_id, "192.0.2.53:21353".to_string());
    let shadow_endpoint =
        invite_introducer_endpoint(&shadow_node_id, "192.0.2.54:21354".to_string());
    let now = OffsetDateTime::now_utc();
    let mut store = InProcessKvStore::new();
    for (endpoint, connectivity_state) in [
        (
            &main_endpoint,
            sinomo_runtime::network::ClusterConnectivityState::Healthy,
        ),
        (
            &shadow_endpoint,
            sinomo_runtime::network::ClusterConnectivityState::Unreachable,
        ),
    ] {
        let record = sinomo_runtime::network::ClusterConnectivityRecord {
            cluster_id: cluster_id.clone(),
            source_node_id: local_node_id.clone(),
            target_node_id: endpoint.node_id.clone(),
            endpoint_id: endpoint.endpoint_id.clone(),
            transport: endpoint.transport.clone(),
            address: endpoint.address.clone(),
            state: connectivity_state,
            latency_ms: None,
            consecutive_failures: 0,
            last_success_at: None,
            last_failure_at: None,
            observed_at: now,
            expires_at: now + time::Duration::seconds(120),
        };
        store
            .put_serde(
                sinomo_runtime::network::cluster_connectivity_key(
                    &record.source_node_id,
                    &record.target_node_id,
                    &record.endpoint_id,
                ),
                &record,
                None,
            )
            .expect("connectivity record");
    }
    sinomo_runtime::kv::write_cluster_kv_snapshot(&paths.kv_paths(), &store.snapshot())
        .expect("cluster kv snapshot");

    let selected = rollout_target_endpoint_for_main_with_connectivity(
        &paths,
        &state,
        &main_node_id,
        &[shadow_endpoint, main_endpoint.clone()],
    )
    .expect("rollout target");

    assert_eq!(selected.node_id, main_node_id);
    assert_eq!(selected.endpoint_id, main_endpoint.endpoint_id);
}

#[test]
fn rollout_target_prefers_unknown_private_tcp_over_shadow_iroh() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-rollout-shadow-iroh-private");
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");

    let cluster_id =
        ClusterId::from_legacy_named("cluster_rollout_shadow_iroh_private").expect("cluster");
    let local_node_id =
        NodeId::from_legacy_named("node_rollout_shadow_iroh_private_local").expect("node");
    let main_node_id =
        NodeId::from_legacy_named("node_rollout_shadow_iroh_private_remote").expect("node");
    let shadow_node_id = shadow_daemon_node_id(&main_node_id);
    let state = ClusterBootstrapState::initialized_locally(
        cluster_id,
        "rollout shadow iroh private",
        local_node_id,
        "local",
        "local-public-key",
        OffsetDateTime::now_utc(),
    );

    let private_shadow = invite_introducer_endpoint(&shadow_node_id, "10.0.0.42:21354".to_string());
    let iroh_shadow = invite_introducer_endpoint(
        &shadow_node_id,
        "iroh://rollout_shadow_private?relay=https://relay.example./".to_string(),
    );

    let selected = rollout_target_endpoint_for_main_with_connectivity(
        &paths,
        &state,
        &main_node_id,
        &[iroh_shadow, private_shadow.clone()],
    )
    .expect("rollout target");

    assert_eq!(selected.node_id, shadow_node_id);
    assert_eq!(selected.endpoint_id, private_shadow.endpoint_id);
}

#[test]
fn rollout_target_prefers_unknown_private_shadow_tcp_over_public_tcp() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-rollout-shadow-private-before-public");
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");

    let cluster_id =
        ClusterId::from_legacy_named("cluster_rollout_shadow_private_public").expect("cluster");
    let local_node_id =
        NodeId::from_legacy_named("node_rollout_shadow_private_public_local").expect("node");
    let main_node_id =
        NodeId::from_legacy_named("node_rollout_shadow_private_public_remote").expect("node");
    let shadow_node_id = shadow_daemon_node_id(&main_node_id);
    let state = ClusterBootstrapState::initialized_locally(
        cluster_id,
        "rollout shadow private public",
        local_node_id,
        "local",
        "local-public-key",
        OffsetDateTime::now_utc(),
    );

    let private_shadow = invite_introducer_endpoint(&shadow_node_id, "10.0.0.42:21354".to_string());
    let public_shadow =
        invite_introducer_endpoint(&shadow_node_id, "203.0.113.42:21354".to_string());

    let selected = rollout_target_endpoint_for_main_with_connectivity(
        &paths,
        &state,
        &main_node_id,
        &[public_shadow, private_shadow.clone()],
    )
    .expect("rollout target");

    assert_eq!(selected.node_id, shadow_node_id);
    assert_eq!(selected.endpoint_id, private_shadow.endpoint_id);
}

#[test]
fn rollout_target_prefers_healthy_main_tcp_over_shadow_iroh_backup() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-rollout-shadow-before-main");
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");

    let cluster_id =
        ClusterId::from_legacy_named("cluster_rollout_shadow_before_main").expect("cluster");
    let local_node_id =
        NodeId::from_legacy_named("node_rollout_shadow_before_main_local").expect("node");
    let main_node_id =
        NodeId::from_legacy_named("node_rollout_shadow_before_main_remote").expect("node");
    let shadow_node_id = shadow_daemon_node_id(&main_node_id);
    let state = ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "rollout shadow before main",
        local_node_id.clone(),
        "local",
        "local-public-key",
        OffsetDateTime::now_utc(),
    );

    let main_endpoint = invite_introducer_endpoint(&main_node_id, "192.0.2.53:21353".to_string());
    let shadow_endpoint = invite_introducer_endpoint(
        &shadow_node_id,
        "iroh://rollout_shadow_before_main?relay=https://relay.example./".to_string(),
    );
    let now = OffsetDateTime::now_utc();
    let mut store = InProcessKvStore::new();
    store
        .put_serde(
            sinomo_runtime::network::cluster_connectivity_key(
                &local_node_id,
                &main_node_id,
                &main_endpoint.endpoint_id,
            ),
            &sinomo_runtime::network::ClusterConnectivityRecord {
                cluster_id,
                source_node_id: local_node_id,
                target_node_id: main_node_id.clone(),
                endpoint_id: main_endpoint.endpoint_id.clone(),
                transport: main_endpoint.transport.clone(),
                address: main_endpoint.address.clone(),
                state: sinomo_runtime::network::ClusterConnectivityState::Healthy,
                latency_ms: None,
                consecutive_failures: 0,
                last_success_at: Some("ok".to_string()),
                last_failure_at: None,
                observed_at: now,
                expires_at: now + time::Duration::seconds(120),
            },
            None,
        )
        .expect("connectivity record");
    sinomo_runtime::kv::write_cluster_kv_snapshot(&paths.kv_paths(), &store.snapshot())
        .expect("cluster kv snapshot");

    let selected = rollout_target_endpoint_for_main_with_connectivity(
        &paths,
        &state,
        &main_node_id,
        &[main_endpoint.clone(), shadow_endpoint.clone()],
    )
    .expect("rollout target");

    assert_eq!(selected.node_id, main_node_id);
    assert_eq!(selected.endpoint_id, main_endpoint.endpoint_id);
}

#[test]
fn rollout_endpoint_candidates_include_kv_only_endpoint_records() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-rollout-kv-endpoint-candidates");
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");

    let remote_node_id =
        NodeId::from_legacy_named("node_rollout_kv_endpoint_remote").expect("node");
    let iroh_endpoint = invite_introducer_endpoint(
        &remote_node_id,
        "iroh://rollout_kv_endpoint?relay=https://relay.example./".to_string(),
    );

    let mut store = InProcessKvStore::new();
    store
        .put_serde(
            sinomo_runtime::network::cluster_endpoint_key(&remote_node_id, &iroh_endpoint),
            &iroh_endpoint,
            None,
        )
        .expect("endpoint record");
    sinomo_runtime::kv::write_cluster_kv_snapshot(&paths.kv_paths(), &store.snapshot())
        .expect("cluster kv snapshot");

    let mut endpoints = Vec::new();
    extend_rollout_endpoints_from_kv(&paths, &mut endpoints);

    assert!(
        endpoints
            .iter()
            .any(|endpoint| endpoint.endpoint_id == iroh_endpoint.endpoint_id)
    );
}

#[test]
fn rollout_cluster_target_ignores_local_only_shadow_endpoint() {
    let main_node_id = NodeId::from_legacy_named("node_rollout_local_shadow").expect("main node");
    let shadow_node_id = shadow_daemon_node_id(&main_node_id);
    let shadow_endpoint =
        invite_introducer_endpoint(&shadow_node_id, "127.0.0.1:21354".to_string());

    assert!(rollout_target_endpoint_for_main(&main_node_id, &[shadow_endpoint]).is_none());
}

#[test]
fn daemon_rollout_build_rejects_missing_remote_repo() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-rollout-build-missing");
    let request = ClusterRolloutBuildRequest {
        source: "test".to_string(),
        remote_repo: None,
        source_bundle: None,
        source_head: None,
        force_source_overwrite: false,
        host: "127.0.0.1".to_string(),
        port: 21_353,
        shadow_port: 21_354,
        requested_at: "now".to_string(),
    };
    let response =
        handle_daemon_rollout_build_request(&DaemonPaths::with_root(root.join("daemon")), request)
            .expect("build response");

    assert_eq!(response.status, "rejected");
    assert!(
        response
            .detail
            .contains("remote source repo is not initialized")
    );
}

#[test]
fn daemon_rollout_build_imports_source_bundle_before_building() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-rollout-build-bundle");
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
        source_bundle: Some(std::fs::read(&bundle).expect("bundle bytes")),
        source_head: Some(expected_head.clone()),
        force_source_overwrite: false,
        host: "127.0.0.1".to_string(),
        port: 21_353,
        shadow_port: 21_354,
        requested_at: "now".to_string(),
    };
    let response =
        handle_daemon_rollout_build_request(&DaemonPaths::with_root(root.join("daemon")), request)
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
}

#[test]
fn rollout_main_activation_without_shadow_requires_staged_binary() {
    let root =
        sinomo_testdir::TestDir::new("sinomo-cli-rollout-main-shadow-bootstrap-staged-required");
    let rollout_paths = SelfRolloutPaths::new(&root);
    ensure_rollout_layout(&rollout_paths).expect("layout");
    let error = stage_rollout_main_activation(
        &root,
        &rollout_paths,
        RolloutMainActivation {
            source: "test".to_string(),
            main_binary: rollout_paths.main_binary.clone(),
            shadow_binary: rollout_paths.shadow_binary.clone(),
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
    .expect_err("staged binary required");

    assert!(matches!(error, CliError::DaemonNotRunning { .. }));
    assert!(!super::rollout_main_activation_path(&rollout_paths).exists());
}

#[test]
fn rollout_advance_with_data_dir_records_persisted_runtime_transition() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-rollout-advance");
    let data_dir = SinomoDataDir::new(&root);
    data_dir.ensure_root_layout().expect("root layout");
    let rollout_paths = SelfRolloutPaths::new(&root);
    let node_id = NodeId::new("01hxy6qa9k0t9vr4g9n8d3q5d5").expect("node");
    let artifact_id = Id::from_legacy_named("artifact_cli_rollout_candidate").expect("artifact");
    let active_artifact_id = Id::from_legacy_named("artifact_cli_rollout_active").expect("active");
    let module_id = Id::from_legacy_named("module.cli.rollout").expect("module");
    let plan = RolloutPlan {
        rollout_plan_id: Id::from_legacy_named("rollout_cli_advance").expect("rollout"),
        artifact_id: artifact_id.clone(),
        module_id,
        target_ring: TrustRing::R3SandboxedModule,
        strategy: RolloutStrategy::Shadow,
        target_nodes: vec![node_id.clone()],
        initial_weight_bps: 0,
        promotion_steps_bps: vec![500],
        rollback_policy: RollbackPolicy::default(),
    };
    let controller = RolloutController::new(node_id.clone());
    let state = controller.initial_state(plan, active_artifact_id);
    write_rollout_runtime_state(&rollout_paths, &state).expect("runtime state");

    let scorecard = EvaluationScorecard {
        scorecard_id: Id::from_legacy_named("score_cli_rollout_pass").expect("scorecard"),
        baseline_artifact_id: None,
        candidate_artifact_id: Some(artifact_id),
        outcome: EvaluationOutcome::Passed,
        gates: Vec::new(),
        metrics: Vec::new(),
    };
    let scorecard_path = root.join("scorecard.json");
    std::fs::write(
        &scorecard_path,
        serde_json::to_string_pretty(&scorecard).expect("scorecard json"),
    )
    .expect("write scorecard");

    let applied = rollout_advance_with_data_dir(
        &data_dir,
        RolloutAdvanceOptions {
            node_id: Some(node_id),
            scorecard: Some(scorecard_path.clone()),
            module_replay_report: None,
            shadow_telemetry_limit: None,
            incident_event_ids: Vec::new(),
            user_correction_count: 0,
            rollback_triggers: Vec::new(),
            evidence_refs: vec!["manual://promotion-review".to_string()],
        },
    )
    .expect("rollout advance");

    assert_eq!(applied.transition.action, RolloutAction::AdvanceCanary);
    assert_eq!(applied.transition.current_weight_bps, 500);
    assert_eq!(
        read_rollout_runtime_state(&rollout_paths)
            .expect("read runtime")
            .expect("runtime")
            .current_weight_bps,
        500
    );
    let transition = std::fs::read_to_string(&applied.transition_path).expect("transition record");
    assert!(transition.contains("manual://promotion-review"));
    assert!(transition.contains(&format!("scorecard:{}", scorecard_path.display())));
    assert_eq!(applied.module_state.traffic_weight_bps, 500);
    assert_eq!(
        read_module_loader_runtime_state(&root, &applied.module_state.module_id)
            .expect("read module state"),
        Some(applied.module_state)
    );
}

#[test]
fn rollout_advance_with_data_dir_accepts_module_replay_report_evidence() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-rollout-advance-replay");
    let data_dir = SinomoDataDir::new(&root);
    data_dir.ensure_root_layout().expect("root layout");
    let rollout_paths = SelfRolloutPaths::new(&root);
    let node_id = NodeId::new("01hxy6qa9k0t9vr4g9n8d3q5d5").expect("node");
    let artifact_id =
        Id::from_legacy_named("artifact_cli_rollout_replay_candidate").expect("artifact");
    let active_artifact_id =
        Id::from_legacy_named("artifact_cli_rollout_replay_active").expect("active");
    let module_id = Id::from_legacy_named("module.cli.rollout.replay").expect("module");
    let plan = RolloutPlan {
        rollout_plan_id: Id::from_legacy_named("rollout_cli_advance_replay").expect("rollout"),
        artifact_id: artifact_id.clone(),
        module_id: module_id.clone(),
        target_ring: TrustRing::R3SandboxedModule,
        strategy: RolloutStrategy::Shadow,
        target_nodes: vec![node_id.clone()],
        initial_weight_bps: 0,
        promotion_steps_bps: vec![250],
        rollback_policy: RollbackPolicy::default(),
    };
    let controller = RolloutController::new(node_id.clone());
    let state = controller.initial_state(plan, active_artifact_id.clone());
    write_rollout_runtime_state(&rollout_paths, &state).expect("runtime state");

    let report = ModuleReplayReport {
        fixture_id: Id::from_legacy_named("fixture_cli_rollout_replay").expect("fixture"),
        module_id,
        cases: vec![ModuleReplayCaseOutcome {
            case_id: "case-1".to_string(),
            route_key: "default".to_string(),
            source_event_id: None,
            outcome: None,
            shadow_matched: Some(true),
        }],
        replay_event_count: 1,
        missing_route_count: 0,
        primary_error_count: 0,
        shadow_error_count: 0,
        shadow_mismatch_count: 0,
        events: Vec::new(),
    };
    let report_path = root.join("module-replay-report.json");
    std::fs::write(
        &report_path,
        serde_json::to_string_pretty(&report).expect("report json"),
    )
    .expect("write report");

    let applied = rollout_advance_with_data_dir(
        &data_dir,
        RolloutAdvanceOptions {
            node_id: Some(node_id),
            scorecard: None,
            module_replay_report: Some(report_path.clone()),
            shadow_telemetry_limit: None,
            incident_event_ids: Vec::new(),
            user_correction_count: 0,
            rollback_triggers: Vec::new(),
            evidence_refs: Vec::new(),
        },
    )
    .expect("rollout advance");

    assert_eq!(applied.transition.action, RolloutAction::AdvanceCanary);
    assert_eq!(applied.transition.current_weight_bps, 250);
    assert_eq!(applied.transition.active_artifact_id, active_artifact_id);
    assert_eq!(applied.transition.candidate_artifact_id, Some(artifact_id));
    let transition = std::fs::read_to_string(&applied.transition_path).expect("transition record");
    assert!(transition.contains(&format!("module-replay-report:{}", report_path.display())));
}

#[test]
fn rollout_advance_with_data_dir_accepts_shadow_telemetry_evidence() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-rollout-advance-shadow-telemetry");
    let data_dir = SinomoDataDir::new(&root);
    data_dir.ensure_root_layout().expect("root layout");
    let rollout_paths = SelfRolloutPaths::new(&root);
    let node_id = NodeId::new("01hxy6qa9k0t9vr4g9n8d3q5d5").expect("node");
    let artifact_id =
        Id::from_legacy_named("artifact_cli_rollout_shadow_candidate").expect("artifact");
    let active_artifact_id =
        Id::from_legacy_named("artifact_cli_rollout_shadow_active").expect("active");
    let module_id = Id::from_legacy_named("module.cli.rollout.shadow").expect("module");
    let plan = RolloutPlan {
        rollout_plan_id: Id::from_legacy_named("rollout_cli_advance_shadow").expect("rollout"),
        artifact_id: artifact_id.clone(),
        module_id: module_id.clone(),
        target_ring: TrustRing::R3SandboxedModule,
        strategy: RolloutStrategy::Shadow,
        target_nodes: vec![node_id.clone()],
        initial_weight_bps: 0,
        promotion_steps_bps: vec![300],
        rollback_policy: RollbackPolicy::default(),
    };
    let controller = RolloutController::new(node_id.clone());
    let state = controller.initial_state(plan, active_artifact_id.clone());
    write_rollout_runtime_state(&rollout_paths, &state).expect("runtime state");

    let mut event_store = JsonlEventStore::new(root.join("events").join("event-log.jsonl"));
    event_store
        .append(module_shadow_telemetry_event(
            &node_id,
            &module_id,
            &active_artifact_id,
            &artifact_id,
            true,
        ))
        .expect("append event");
    event_store
        .append(module_shadow_telemetry_event(
            &node_id,
            &module_id,
            &active_artifact_id,
            &artifact_id,
            true,
        ))
        .expect("append event");

    let applied = rollout_advance_with_data_dir(
        &data_dir,
        RolloutAdvanceOptions {
            node_id: Some(node_id),
            scorecard: None,
            module_replay_report: None,
            shadow_telemetry_limit: Some(10),
            incident_event_ids: Vec::new(),
            user_correction_count: 0,
            rollback_triggers: Vec::new(),
            evidence_refs: Vec::new(),
        },
    )
    .expect("rollout advance");

    assert_eq!(applied.transition.action, RolloutAction::AdvanceCanary);
    assert_eq!(applied.transition.current_weight_bps, 300);
    assert_eq!(applied.transition.active_artifact_id, active_artifact_id);
    assert_eq!(applied.transition.candidate_artifact_id, Some(artifact_id));
    let transition = std::fs::read_to_string(&applied.transition_path).expect("transition record");
    assert!(transition.contains("module-shadow-telemetry:events/event-log.jsonl?limit=10"));
}

#[test]
fn rollout_telemetry_gates_set_with_data_dir_persists_config() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-rollout-telemetry-gates");
    let data_dir = SinomoDataDir::new(&root);
    data_dir.ensure_root_layout().expect("root layout");
    let rollout_paths = SelfRolloutPaths::new(&root);

    let config = rollout_telemetry_gates_set_with_data_dir(
        &data_dir,
        RolloutTelemetryGateSetOptions {
            automatic_promotion_enabled: Some(false),
            event_limit: Some(250),
            minimum_event_count: Some(20),
            maximum_automatic_weight_bps: Some(None),
            maximum_candidate_latency_p95_ms: Some(Some(125)),
        },
    )
    .expect("set telemetry gates");
    let stored = sinomo_runtime::rollout::read_rollout_telemetry_gate_config(&rollout_paths)
        .expect("read telemetry gates")
        .expect("telemetry gates");

    assert_eq!(stored, config);
    assert!(!stored.automatic_promotion_enabled);
    assert_eq!(stored.event_limit, 250);
    assert_eq!(stored.minimum_event_count, 20);
    assert_eq!(stored.maximum_automatic_weight_bps, None);
    assert_eq!(stored.maximum_candidate_latency_p95_ms, Some(125));
}

fn module_shadow_telemetry_event(
    node_id: &NodeId,
    module_id: &Id,
    primary_artifact_id: &Id,
    shadow_artifact_id: &Id,
    matched: bool,
) -> EventEnvelope {
    EventEnvelope::new(
        EventId::generate(),
        OffsetDateTime::now_utc(),
        node_id.clone(),
        Source::new("module.live").expect("valid source"),
        EventKind::new(MODULE_SHADOW_DECISION_EVENT_KIND).expect("valid event kind"),
        Salience::new(0.72).expect("valid salience"),
        Confidence::new(0.95).expect("valid confidence"),
        RoutingMetadata::new(
            RoutingScope::TrustedNetwork,
            LatencyClass::Background,
            PrivacyClass::Derived,
        ),
    )
    .with_target_node(node_id.clone())
    .with_payload(serde_json::json!({
        "case_id": null,
        "route_key": "live-session",
        "telemetry_source": "live",
        "module_id": module_id.as_str(),
        "primary_artifact_id": primary_artifact_id.as_str(),
        "primary_lane": "active",
        "shadow_artifact_id": shadow_artifact_id.as_str(),
        "shadow_matched": matched,
        "primary_output_bytes": 12,
        "shadow_output_bytes": 12,
        "primary_error": null,
        "shadow_error": null,
    }))
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
    assert_eq!(
        parse_daemon_command(&["__complete-shadow-rollout".to_string()]).expect("helper"),
        DaemonCliCommand::CompleteShadowRollout
    );
}

#[test]
fn daemon_paths_for_shadow_are_separate_from_main_daemon() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-daemon-shadow-paths");
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
    let root = sinomo_testdir::TestDir::new("sinomo-cli-daemon-paths");
    let paths = super::DaemonPaths::new(&SinomoDataDir::new(&root));

    assert_eq!(paths.root, root.join("daemon"));
    assert_eq!(paths.lock, root.join("daemon").join("daemon.lock"));
    assert_eq!(paths.status, root.join("daemon").join("status.sobj"));
}

#[test]
fn daemon_lock_prevents_duplicate_instances_and_preserves_probe_status() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-daemon-lock");
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
fn captured_cli_command_collects_stdout_for_embedded_callers() {
    let captured = super::execute_cli_command_captured(&["help".to_string()]);

    assert_eq!(captured.status, super::CliCommandStatus::Completed);
    let stdout = String::from_utf8_lossy(&captured.output.stdout);
    assert!(stdout.contains("Usage:"));
    assert!(stdout.contains("sinomo cluster"));
    assert!(captured.output.stderr.is_empty());
}

#[test]
fn captured_cli_command_collects_registered_command_stdout() {
    let captured = super::execute_cli_command_captured(&["observe".to_string()]);

    assert_eq!(captured.status, super::CliCommandStatus::Completed);
    let stdout = String::from_utf8_lossy(&captured.output.stdout);
    assert!(stdout.contains("Observe:"));
    assert!(stdout.contains("sinomo observe events"));
    assert!(captured.output.stderr.is_empty());
}

#[test]
fn debug_sobj_command_prints_typed_text() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-debug-sobj");
    let path = root.join("state.sobj");
    let document = sinomo_runtime::object::Document::dynamic(
        1,
        sinomo_runtime::object::LengthMode::U32,
        sinomo_runtime::object::Value::Object(sinomo_runtime::object::Object::new(vec![
            sinomo_runtime::object::Field::new(
                "node",
                sinomo_runtime::object::Value::String("aarch64".to_string()),
            ),
            sinomo_runtime::object::Field::new(
                "port",
                sinomo_runtime::object::Value::Uint16(21353),
            ),
        ])),
    );
    std::fs::write(&path, document.to_bytes().expect("encode")).expect("write sobj");

    let captured = super::execute_cli_command_captured(&[
        "debug".to_string(),
        "sobj".to_string(),
        path.display().to_string(),
    ]);

    assert_eq!(captured.status, super::CliCommandStatus::Completed);
    let stdout = String::from_utf8_lossy(&captured.output.stdout);
    assert!(stdout.contains("document(version=1,length=uint32"));
    assert!(stdout.contains("\"node\": \"aarch64\""));
    assert!(stdout.contains("\"port\": uint16(21353)"));
    assert!(captured.output.stderr.is_empty());
}

#[cfg(unix)]
#[test]
fn captured_cli_command_collects_raw_stdio_for_embedded_callers() {
    let captured = super::run_cli_closure_with_process_stdio_capture(|| {
        std::io::stdout()
            .write_all(b"raw stdout from dependency\n")
            .expect("write raw stdout");
        std::io::stderr()
            .write_all(b"raw stderr warning from dependency\n")
            .expect("write raw stderr");
        Ok(())
    });

    assert!(captured.result.is_ok());
    let stdout = String::from_utf8_lossy(&captured.output.stdout);
    let stderr = String::from_utf8_lossy(&captured.output.stderr);
    assert!(stdout.contains("raw stdout from dependency"));
    assert!(stderr.contains("raw stderr warning from dependency"));
}

#[test]
fn slash_command_returns_captured_cli_output_as_interaction_lines() {
    let session = InteractionSession::new(
        SessionId::from_legacy_named("sess_slash_capture_test").expect("session"),
        NodeId::from_legacy_named("node_slash_capture_test").expect("node"),
        InteractionSurface::new("surface.test", InteractionSurfaceKind::CommandLine),
    );
    let mut runtime = InteractionRuntime::new(
        InMemoryEventStore::new(),
        RuntimeMainAgentInteractionHandler::with_thinking(
            MainAgentFixtureModel,
            "fixture-provider",
            "fixture-model",
            super::ThinkingControl::default(),
        ),
        session,
    );

    let outputs = runtime.ingest_user_input("/help").expect("slash help");

    assert!(outputs.iter().any(|output| {
        matches!(
            output,
            super::InteractionOutput::Line(line) if line.contains("Usage:")
        )
    }));
    assert!(!outputs.iter().any(|output| {
        matches!(
            output,
            super::InteractionOutput::Line(line) if line == "completed command: /help"
        )
    }));
}

#[test]
fn files_slash_command_is_tui_only() {
    let session = InteractionSession::new(
        SessionId::from_legacy_named("sess_files_tui_only_test").expect("session"),
        NodeId::from_legacy_named("node_files_tui_only_test").expect("node"),
        InteractionSurface::new("surface.test", InteractionSurfaceKind::CommandLine),
    );
    let mut runtime = InteractionRuntime::new(
        InMemoryEventStore::new(),
        RuntimeMainAgentInteractionHandler::with_thinking(
            MainAgentFixtureModel,
            "fixture-provider",
            "fixture-model",
            super::ThinkingControl::default(),
        ),
        session,
    );

    let outputs = runtime.ingest_user_input("/files").expect("slash files");

    assert!(outputs.iter().any(|output| {
        matches!(
            output,
            super::InteractionOutput::Line(line) if line.starts_with("command failed:")
        )
    }));
    assert!(!outputs.iter().any(|output| {
        matches!(
            output,
            super::InteractionOutput::Line(line) if line.contains("file browser is available in the TUI")
        )
    }));
    let events = runtime.store().replay(ReplayQuery::all()).expect("events");
    assert!(
        events
            .iter()
            .all(|event| event.event.kind.as_str() != "interaction.file_browser.requested")
    );
}

#[test]
fn interaction_runtime_records_message_input_as_events() {
    let session = InteractionSession::new(
        SessionId::from_legacy_named("sess_cli_test").expect("session"),
        NodeId::from_legacy_named("node_cli_test").expect("node"),
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
fn main_agent_handler_routes_plain_input_through_agent_loop() {
    let session = InteractionSession::new(
        SessionId::from_legacy_named("sess_main_agent_cli_test").expect("session"),
        NodeId::from_legacy_named("node_main_agent_cli_test").expect("node"),
        InteractionSurface::new("surface.test", InteractionSurfaceKind::CommandLine),
    );
    let mut runtime = InteractionRuntime::new(
        InMemoryEventStore::new(),
        RuntimeMainAgentInteractionHandler::with_thinking(
            MainAgentFixtureModel,
            "fixture-provider",
            "fixture-model",
            super::ThinkingControl::default(),
        ),
        session,
    );

    let outputs = runtime
        .ingest_user_input("what is the cluster doing?")
        .expect("input");

    assert!(outputs.is_empty());
    runtime.handler_mut().join_active_tui_turn().expect("join");
    let runtime_events = runtime
        .handler_mut()
        .drain_runtime_events()
        .expect("runtime events");
    assert!(runtime_events.iter().any(|event| matches!(
        &event.payload,
        sinomo_agent::AgentRuntimeEventPayload::TurnFinalDecision { decision, .. }
            if decision.text == "cluster-aware reply"
    )));
    let events = runtime.store().replay(ReplayQuery::all()).expect("replay");
    assert_eq!(events.len(), 4);
    assert_eq!(
        events[2].event.kind.as_str(),
        "interaction.main_agent.requested"
    );
    assert_eq!(
        events[3].event.kind.as_str(),
        "interaction.main_agent.queued"
    );
}

#[test]
fn main_agent_handler_registers_live_runtime_for_control_host() {
    let session = InteractionSession::new(
        SessionId::from_legacy_named("sess_main_agent_control_host_test").expect("session"),
        NodeId::from_legacy_named("node_main_agent_control_host_test").expect("node"),
        InteractionSurface::new("surface.test", InteractionSurfaceKind::CommandLine),
    );
    let mut runtime = InteractionRuntime::new(
        InMemoryEventStore::new(),
        RuntimeMainAgentInteractionHandler::with_thinking(
            MainAgentFixtureModel,
            "fixture-provider",
            "fixture-model",
            super::ThinkingControl::default(),
        ),
        session.clone(),
    );
    let control_registry = runtime.handler().runtime_control_registry();

    let outputs = runtime
        .ingest_user_input("what is the cluster doing?")
        .expect("input");
    assert!(outputs.is_empty());

    let steer = control_registry
        .steer(AgentSessionSteerRequest {
            session_id: session.session_id.clone(),
            message: "keep the current constraint".to_string(),
        })
        .expect("steer via control host");
    assert!(steer.accepted);
    let resumed = control_registry
        .resume(AgentSessionResumeRequest {
            session_id: session.session_id.clone(),
        })
        .expect("resume via control host");
    assert!(resumed.runtime_attached);
    let live_session = resumed.session.expect("live session");
    assert_eq!(live_session.steering_queue.len(), 1);
    assert_eq!(
        live_session.steering_queue[0].message,
        "keep the current constraint"
    );

    runtime.handler_mut().join_active_tui_turn().expect("join");
}

#[test]
fn app_agent_chat_exposes_first_class_runtime_control_methods() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-app-chat-control");
    let data_dir = SinomoDataDir::new(&root);
    let chat = super::app_api::AgentChat::new(data_dir, false).expect("chat");

    let initial = chat.control_state().expect("initial control state");
    assert!(initial.accepted);
    assert!(!initial.active_turn);
    assert_eq!(initial.steering_count, 0);
    assert_eq!(initial.follow_up_count, 0);

    let steering = chat.steer("preserve this instruction").expect("steer");
    assert!(steering.accepted);
    assert_eq!(steering.session_id, chat.session_id());
    let follow_up = chat.follow_up("also mention tests").expect("follow up");
    assert!(follow_up.accepted);

    let state = chat.control_state().expect("control state");
    assert_eq!(state.steering_count, 1);
    assert_eq!(state.follow_up_count, 1);
}

#[test]
fn main_agent_handler_streams_visible_thinking_chunks() {
    let session = InteractionSession::new(
        SessionId::from_legacy_named("sess_main_agent_streaming_test").expect("session"),
        NodeId::from_legacy_named("node_main_agent_streaming_test").expect("node"),
        InteractionSurface::new("surface.test", InteractionSurfaceKind::CommandLine),
    );
    let mut runtime = InteractionRuntime::new(
        InMemoryEventStore::new(),
        RuntimeMainAgentInteractionHandler::with_thinking(
            StreamingMainAgentFixtureModel,
            "fixture-provider",
            "fixture-model",
            super::ThinkingControl::default(),
        ),
        session,
    );
    let mut streamed = Vec::new();

    let outputs = runtime
        .ingest_user_input_streaming("what is the cluster doing?", &mut |output| {
            streamed.push(output);
        })
        .expect("input");

    assert!(streamed.is_empty());
    assert!(outputs.is_empty());
    runtime.handler_mut().join_active_tui_turn().expect("join");
    let runtime_events = runtime
        .handler_mut()
        .drain_runtime_events()
        .expect("runtime events");
    assert!(runtime_events.iter().any(|event| matches!(
        &event.payload,
        sinomo_agent::AgentRuntimeEventPayload::ModelProgress {
            event: sinomo_agent::AgentModelEvent::AssistantTextDelta { delta, .. },
            ..
        } if delta.contains("checking quorum")
    )));
    assert!(runtime_events.iter().any(|event| matches!(
        &event.payload,
        sinomo_agent::AgentRuntimeEventPayload::TurnFinalDecision { decision, .. }
            if decision.text == "streamed reply"
    )));
}

#[test]
fn thinking_stream_display_reports_progress_and_prompt_seeded_thinking() {
    let mut display = super::ThinkingStreamDisplay::new(true);
    assert_eq!(
        display.push("<sinomo_progress>loading native MiniCPM-V 4.6</sinomo_progress>"),
        vec!["model> loading native MiniCPM-V 4.6\n".to_string()]
    );
    assert_eq!(
        display.push("first thought"),
        vec!["thinking> ".to_string(), "first thought".to_string(),]
    );
    assert_eq!(
        display.push(" and then done</think>{\"text\":\"hello\"}"),
        vec![" and then done".to_string()]
    );
}

#[test]
fn main_agent_handler_toggles_thinking_with_slash_command() {
    let session = InteractionSession::new(
        SessionId::from_legacy_named("sess_thinking_cli_test").expect("session"),
        NodeId::from_legacy_named("node_thinking_cli_test").expect("node"),
        InteractionSurface::new("surface.test", InteractionSurfaceKind::CommandLine),
    );
    let mut runtime = InteractionRuntime::new(
        InMemoryEventStore::new(),
        RuntimeMainAgentInteractionHandler::with_thinking(
            MainAgentFixtureModel,
            "fixture-provider",
            "fixture-model",
            super::ThinkingControl::default(),
        ),
        session,
    );

    assert_eq!(
        runtime
            .ingest_user_input("/thinking status")
            .expect("status"),
        vec![super::InteractionOutput::Line("thinking is on".to_string())]
    );
    assert_eq!(
        runtime.ingest_user_input("/thinking off").expect("off"),
        vec![super::InteractionOutput::Line(
            "thinking is off".to_string()
        )]
    );
    assert_eq!(
        runtime.ingest_user_input("/thinking on").expect("on"),
        vec![super::InteractionOutput::Line("thinking is on".to_string())]
    );
}

#[test]
fn minicpm_main_agent_prompt_uses_official_chat_template_shape() {
    let request = LanguageModelRequest {
        provider: "sinomo-models".to_string(),
        model: "openbmb/MiniCPM-V-4_6".to_string(),
        messages: vec![
            sinomo_lm::LanguageModelMessage {
                role: sinomo_lm::LanguageModelRole::System,
                content: "You are Sinomo.".to_string(),
            },
            sinomo_lm::LanguageModelMessage {
                role: sinomo_lm::LanguageModelRole::User,
                content: serde_json::json!({
                    "session": {"node_id": "node_test"},
                    "latest_event": {"payload": {"text": "hello"}},
                    "context": {"events": [{
                        "event": {
                            "kind": "interaction.cluster.context",
                            "payload": {
                                "daemon": {
                                    "address": "127.0.0.1:21353",
                                    "cluster": {"cluster_id": "cluster_test"}
                                }
                            }
                        }
                    }]},
                    "tool_specs": [{"name": "cluster_status", "description": "Summarize cluster health."}]
                })
                .to_string(),
            },
        ],
        items: Vec::new(),
        tools: Vec::new(),
        tool_choice: None,
        parallel_tool_calls: None,
        temperature: None,
        max_output_tokens: None,
        response_format: sinomo_lm::LanguageModelResponseFormat::JsonObject,
        metadata: serde_json::json!({"loop": "main-agent"}),
    };

    let prompt = super::minicpm_main_agent_prompt(&request, false).expect("prompt");

    assert!(prompt.contains("<|im_start|>system\n# Tools"));
    assert!(prompt.contains("<tools>"));
    assert!(prompt.contains("<|im_start|>user\nNode: node_test"));
    assert!(prompt.contains("Cluster: cluster cluster_test through daemon 127.0.0.1:21353"));
    assert!(prompt.ends_with("<|im_start|>assistant\n<think>\n\n</think>\n\n"));

    let prompt = super::minicpm_main_agent_prompt(&request, true).expect("prompt");
    assert!(prompt.ends_with("<|im_start|>assistant\n<think>\n"));
}

#[test]
fn minicpm_response_trimming_removes_thinking_and_prompt_continuations() {
    assert_eq!(
        super::trim_generated_chat_turn("<think>\nprivate\n</think>\n\nHello.\nUser: next"),
        "Hello."
    );
    assert_eq!(
        super::trim_generated_chat_turn("Hello.<|im_end|>\n<|im_start|>user\nnext"),
        "Hello."
    );
    assert_eq!(
        super::trim_generated_chat_turn("<think>\nprivate unfinished reasoning"),
        ""
    );
}

#[test]
fn minicpm_tool_call_response_maps_to_agent_decision_json() {
    let json = super::minicpm_tool_call_to_agent_decision_json(
        "Checking cluster.\n<tool_call>\n<function=cluster_status>\n<parameter=scope>\nlocal\n</parameter>\n</function>\n</tool_call>",
    )
    .expect("tool call json");
    let value: serde_json::Value = serde_json::from_str(&json).expect("json");

    assert_eq!(value["response_mode"], "defer");
    assert_eq!(value["text"], "Checking cluster.");
    assert_eq!(value["tool_calls"][0]["tool"], "cluster_status");
    assert_eq!(value["tool_calls"][0]["input"]["scope"], "local");
}

#[test]
fn cui_entry_starts_without_daemon_using_local_runtime_config() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-tui-no-daemon");
    let data_dir = SinomoDataDir::new(&root);
    let cluster_paths = ClusterPaths::new(&data_dir);
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(ClusterId::from_legacy_named("cui_no_daemon").expect("cluster")),
            node_id: Some(NodeId::from_legacy_named("cui_no_daemon_node").expect("node")),
            name: "tui no daemon".to_string(),
        },
        &cluster_paths,
    )
    .expect("cluster init");
    assert!(model_config_uses_cluster(&data_dir));

    let cluster_config = sinomo_lm::ModelRuntimeConfig {
        pytorch_bridge: sinomo_lm::PytorchBridgeRuntimeConfig {
            endpoint: Some("http://127.0.0.1:41111".to_string()),
            timeout_seconds: Some(45),
            ..sinomo_lm::PytorchBridgeRuntimeConfig::default()
        },
        ..sinomo_lm::ModelRuntimeConfig::default()
    };
    write_model_runtime_config_for_node(&data_dir, &cluster_config)
        .expect("write clustered runtime config");

    let local_config = sinomo_lm::ModelRuntimeConfig {
        pytorch_bridge: sinomo_lm::PytorchBridgeRuntimeConfig {
            endpoint: Some("http://127.0.0.1:42222".to_string()),
            timeout_seconds: Some(30),
            ..sinomo_lm::PytorchBridgeRuntimeConfig::default()
        },
        ..sinomo_lm::ModelRuntimeConfig::default()
    };
    let local_path = with_cli_force_local_mode(true, || {
        write_model_runtime_config_for_node(&data_dir, &local_config)
    })
    .expect("write local runtime config");

    let (loaded, loaded_path) = load_tui_model_runtime_config(&data_dir, false)
        .expect("load daemonless tui runtime config");
    assert_eq!(loaded, local_config);
    assert_eq!(loaded_path, local_path);

    let (handler, daemon) =
        main_agent_handler_for_tui_with_data_dir(None, &data_dir).expect("daemonless handler");
    assert!(daemon.is_none());
    let mut output = Vec::new();

    run_tui_with_handler_io_and_daemon(Cursor::new("/quit\n"), &mut output, handler, daemon)
        .expect("daemonless tui starts");

    let output = String::from_utf8(output).expect("utf8");
    assert!(output.contains("Sinomo main agent"));
    assert!(output.contains("daemon: not connected"));
    assert!(output.contains("ending session"));
}

#[test]
fn cui_local_mode_skips_default_daemon_probe() {
    let daemon = with_cli_force_local_mode(true, tui_default_daemon_status)
        .expect("local mode daemon status");

    assert!(daemon.is_none());
}

#[test]
fn cui_loop_starts_and_exits_from_quit_command() {
    let mut output = Vec::new();

    run_tui_with_handler_io(
        Cursor::new("/quit\n"),
        &mut output,
        fixture_handler("unused"),
    )
    .expect("tui");

    let output = String::from_utf8(output).expect("utf8");
    assert!(output.contains("Sinomo main agent"));
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
        commit: None,
        target_os: Some(std::env::consts::OS.to_string()),
        target_arch: Some(std::env::consts::ARCH.to_string()),
        executable: None,
        rollout: None,
        started_at: "2026-05-04T00:00:00Z".to_string(),
        data_dir: PathBuf::from("data"),
        lock_file: PathBuf::from("data/daemon/daemon.lock"),
        cluster: Some(DaemonClusterStatus {
            cluster_id: ClusterId::from_legacy_named("cluster_cui").expect("cluster"),
            node_id: NodeId::from_legacy_named("node_daemon").expect("node"),
            process: super::DAEMON_MAIN_CLUSTER_ROLE.to_string(),
            member_role: ClusterMemberRole::Voter,
            member_state: ClusterMemberState::Active,
        }),
        idle_stats: None,
    };

    run_tui_with_handler_io_and_daemon(
        Cursor::new("/quit\n"),
        &mut output,
        fixture_handler("unused"),
        Some(daemon),
    )
    .expect("tui");

    let output = String::from_utf8(output).expect("utf8");
    assert!(output.contains(&format!(
        "node: {}",
        NodeId::from_legacy_named("node_daemon")
            .expect("node")
            .as_str()
    )));
    assert!(output.contains("daemon: connected pid=4242 address=127.0.0.1:21353"));
    assert!(output.contains(&format!(
        "cluster: {}",
        ClusterId::from_legacy_named("cluster_cui")
            .expect("cluster")
            .as_str()
    )));
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

struct MainAgentFixtureModel;

impl InteractionModel for MainAgentFixtureModel {
    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        assert_eq!(request.provider, "fixture-provider");
        assert_eq!(request.model, "fixture-model");
        assert_eq!(
            request.response_format,
            sinomo_lm::LanguageModelResponseFormat::Text
        );
        assert!(
            request.messages[0]
                .content
                .contains("user-facing main agent")
        );
        assert!(
            request
                .messages
                .iter()
                .any(|message| message.content.contains("what is the cluster doing?"))
        );
        Ok(LanguageModelResponse {
            provider: request.provider.clone(),
            model: request.model.clone(),
            content: "cluster-aware reply".to_string(),
            items: Vec::new(),
            tool_calls: Vec::new(),
            usage: None,
        })
    }
}

struct StreamingMainAgentFixtureModel;

impl InteractionModel for StreamingMainAgentFixtureModel {
    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        Ok(main_agent_fixture_response(request, "streamed reply"))
    }

    fn complete_streaming(
        &self,
        request: &LanguageModelRequest,
        on_delta: &mut dyn FnMut(&str),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        on_delta("checking quorum");
        on_delta(" and rollout state\n</think>\n");
        Ok(main_agent_fixture_response(request, "streamed reply"))
    }
}

fn main_agent_fixture_response(
    request: &LanguageModelRequest,
    text: &str,
) -> LanguageModelResponse {
    LanguageModelResponse {
        provider: request.provider.clone(),
        model: request.model.clone(),
        content: text.to_string(),
        items: Vec::new(),
        tool_calls: Vec::new(),
        usage: None,
    }
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
            items: Vec::new(),
            tool_calls: Vec::new(),
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
fn model_options_parser_accepts_reasoning_and_vendor_options() {
    let command = parse_model_options_command(&[
        "set".to_string(),
        "--provider".to_string(),
        "openai-api".to_string(),
        "--account".to_string(),
        "team-a".to_string(),
        "--model".to_string(),
        "gpt-5.5".to_string(),
        "--reasoning-effort".to_string(),
        "none".to_string(),
        "--reasoning-budget-tokens".to_string(),
        "4096".to_string(),
        "--text-verbosity".to_string(),
        "low".to_string(),
        "--vendor-option".to_string(),
        "reasoning.summary=auto".to_string(),
    ])
    .expect("model options");

    assert_eq!(
        command,
        ModelOptionsCliCommand::Set(ModelOptionsSetOptions {
            target: ModelOptionsTarget {
                provider: Some("openai".to_string()),
                account: "team-a".to_string(),
                model: Some("gpt-5.5".to_string()),
            },
            reasoning_effort: Some(ModelOptionValueUpdate::Set(ProviderReasoningEffort::None)),
            reasoning_budget_tokens: Some(ModelOptionValueUpdate::Set(4096)),
            text_verbosity: Some(ModelOptionValueUpdate::Set(ProviderTextVerbosity::Low)),
            vendor_options: vec![("reasoning.summary".to_string(), "auto".to_string())],
            clear_vendor_options: Vec::new(),
        })
    );
}

#[test]
fn model_options_parser_distinguishes_clear_from_none() {
    let command = parse_model_options_command(&[
        "set".to_string(),
        "--provider".to_string(),
        "anthropic".to_string(),
        "--reasoning-effort".to_string(),
        "clear".to_string(),
        "--text-verbosity".to_string(),
        "clear".to_string(),
    ])
    .expect("model options");

    assert_eq!(
        command,
        ModelOptionsCliCommand::Set(ModelOptionsSetOptions {
            target: ModelOptionsTarget {
                provider: Some("anthropic".to_string()),
                account: "default".to_string(),
                model: None,
            },
            reasoning_effort: Some(ModelOptionValueUpdate::Clear),
            reasoning_budget_tokens: None,
            text_verbosity: Some(ModelOptionValueUpdate::Clear),
            vendor_options: Vec::new(),
            clear_vendor_options: Vec::new(),
        })
    );
}

#[test]
fn models_default_writes_global_model_config() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-models");
    let data_dir = SinomoDataDir::new(&root);

    models_default_with_data_dir(
        super::ModelDefaultOptions {
            provider: "openai".to_string(),
            account: "default".to_string(),
            model: "gpt-5.5".to_string(),
        },
        &data_dir,
    )
    .expect("write model defaults");

    let config_path = root.join("configs").join("lm").join("providers.json");
    let document = serde_json::from_str::<sinomo_runtime::config::ConfigDocument>(
        &std::fs::read_to_string(&config_path).expect("model config"),
    )
    .expect("model config document");
    assert_eq!(document.key.as_str(), sinomo_lm::LM_PROVIDER_CONFIG_KEY);
    let config = document.value;
    assert_eq!(config["default_provider"], "openai");
    assert_eq!(config["default_account"], "default");
    assert_eq!(config["default_model"], "gpt-5.5");
}

#[test]
fn main_agent_config_tool_registry_exposes_host_config_catalog() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-agent-config-tools");
    let data_dir = SinomoDataDir::new(&root);
    let registry = main_agent_config_tool_registry(&data_dir).expect("config tool registry");
    let names: Vec<_> = registry
        .descriptors()
        .into_iter()
        .map(|descriptor| descriptor.spec.name)
        .collect();
    for expected in ["config_search", "config_get", "config_set"] {
        assert!(names.iter().any(|name| name == expected), "{expected}");
    }

    let session = InteractionSession::new(
        SessionId::generate(),
        NodeId::from_legacy_named("node_agent_config_tools").expect("node"),
        InteractionSurface::new("test", InteractionSurfaceKind::CommandLine),
    );
    let outcome = registry.execute(
        &session,
        0,
        0,
        &sinomo_agent::AgentToolCall {
            call_id: None,
            tool: "config_search".to_string(),
            input: serde_json::json!({ "query": "" }),
            r#async: false,
            summary: "search configs".to_string(),
        },
    );
    assert_eq!(outcome.status, sinomo_agent::AgentToolStatus::Succeeded);
    let keys: Vec<_> = outcome
        .output
        .get("definitions")
        .and_then(serde_json::Value::as_array)
        .expect("definitions")
        .iter()
        .filter_map(|definition| definition.get("key").and_then(serde_json::Value::as_str))
        .collect();
    for expected in [
        "dfs.expose",
        "rollout.telemetry.gates",
        "sandbox.policy",
        "lm.providers",
        "lm.runtime",
        "lm.credentials.openai",
        "lm.credentials.codex",
    ] {
        assert!(keys.iter().any(|key| key == &expected), "{expected}");
    }
}

#[test]
fn models_default_writes_cluster_model_config_to_kv_for_clustered_node() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-cluster-models");
    let data_dir = SinomoDataDir::new(&root);
    let cluster_paths = ClusterPaths::new(&data_dir);
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(ClusterId::from_legacy_named("lm_config").expect("cluster")),
            node_id: Some(NodeId::from_legacy_named("lm_config_node").expect("node")),
            name: "lm config".to_string(),
        },
        &cluster_paths,
    )
    .expect("cluster init");

    models_default_with_data_dir(
        super::ModelDefaultOptions {
            provider: "openai".to_string(),
            account: "work".to_string(),
            model: "gpt-5.5".to_string(),
        },
        &data_dir,
    )
    .expect("write clustered model defaults");

    assert!(
        !root
            .join("configs")
            .join("lm")
            .join("providers.json")
            .exists()
    );
    let store = sinomo_runtime::kv::InProcessKvStore::restore(
        sinomo_runtime::kv::read_cluster_kv_cached_snapshot(&cluster_paths.kv_paths())
            .expect("cluster kv"),
    );
    let entry = store
        .get(sinomo_lm::LM_PROVIDER_CONFIG_KV_KEY)
        .expect("lm config kv entry");
    let document: sinomo_runtime::config::ConfigDocument =
        entry.decode().expect("lm config document");
    assert_eq!(document.key.as_str(), sinomo_lm::LM_PROVIDER_CONFIG_KEY);
    let config: sinomo_lm::ModelConfig = serde_json::from_value(document.value).expect("lm config");
    assert_eq!(config.default_provider.as_deref(), Some("openai"));
    assert_eq!(config.default_account.as_deref(), Some("work"));
    assert_eq!(config.default_model.as_deref(), Some("gpt-5.5"));
}

#[test]
fn local_mode_forces_model_config_to_local_file_on_clustered_node() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-local-mode-models");
    let data_dir = SinomoDataDir::new(&root);
    let cluster_paths = ClusterPaths::new(&data_dir);
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(ClusterId::from_legacy_named("local_models").expect("cluster")),
            node_id: Some(NodeId::from_legacy_named("local_models_node").expect("node")),
            name: "local models".to_string(),
        },
        &cluster_paths,
    )
    .expect("cluster init");

    with_cli_force_local_mode(true, || {
        assert!(!model_config_uses_cluster(&data_dir));
        models_default_with_data_dir(
            super::ModelDefaultOptions {
                provider: "openai".to_string(),
                account: "local".to_string(),
                model: "gpt-5.5".to_string(),
            },
            &data_dir,
        )
    })
    .expect("write local model defaults");

    assert!(model_config_uses_cluster(&data_dir));
    let config_path = root.join("configs").join("lm").join("providers.json");
    let document = serde_json::from_str::<sinomo_runtime::config::ConfigDocument>(
        &std::fs::read_to_string(&config_path).expect("model config"),
    )
    .expect("model config document");
    assert_eq!(document.key.as_str(), sinomo_lm::LM_PROVIDER_CONFIG_KEY);
    let config: sinomo_lm::ModelConfig =
        serde_json::from_value(document.value).expect("model config json");
    assert_eq!(config.default_provider.as_deref(), Some("openai"));
    assert_eq!(config.default_account.as_deref(), Some("local"));
    assert_eq!(config.default_model.as_deref(), Some("gpt-5.5"));

    let store = sinomo_runtime::kv::InProcessKvStore::restore(
        sinomo_runtime::kv::read_cluster_kv_cached_snapshot(&cluster_paths.kv_paths())
            .expect("cluster kv"),
    );
    assert!(store.get(sinomo_lm::LM_PROVIDER_CONFIG_KV_KEY).is_none());
}

#[test]
fn daemonless_cli_model_defaults_use_local_file_on_clustered_node() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-daemonless-models");
    let data_dir = SinomoDataDir::new(&root);
    let cluster_paths = ClusterPaths::new(&data_dir);
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(ClusterId::from_legacy_named("daemonless_models").expect("cluster")),
            node_id: Some(NodeId::from_legacy_named("daemonless_models_node").expect("node")),
            name: "daemonless models".to_string(),
        },
        &cluster_paths,
    )
    .expect("cluster init");
    assert!(model_config_uses_cluster(&data_dir));

    let command = CliCommand::Models(ModelsCliCommand::Default(super::ModelDefaultOptions {
        provider: "openai".to_string(),
        account: "local".to_string(),
        model: "gpt-5.5".to_string(),
    }));
    let force_local =
        cli_invocation_force_local_mode(CliRuntimeOptions::default(), &command, false, None);
    assert!(force_local);

    with_cli_force_local_mode(force_local, || {
        models_default_with_data_dir(
            super::ModelDefaultOptions {
                provider: "openai".to_string(),
                account: "local".to_string(),
                model: "gpt-5.5".to_string(),
            },
            &data_dir,
        )
    })
    .expect("write daemonless local model defaults");

    let config_path = root.join("configs").join("lm").join("providers.json");
    let document = serde_json::from_str::<sinomo_runtime::config::ConfigDocument>(
        &std::fs::read_to_string(&config_path).expect("model config"),
    )
    .expect("model config document");
    assert_eq!(document.key.as_str(), sinomo_lm::LM_PROVIDER_CONFIG_KEY);
    let config: sinomo_lm::ModelConfig =
        serde_json::from_value(document.value).expect("model config json");
    assert_eq!(config.default_provider.as_deref(), Some("openai"));
    assert_eq!(config.default_account.as_deref(), Some("local"));
    assert_eq!(config.default_model.as_deref(), Some("gpt-5.5"));

    let store = sinomo_runtime::kv::InProcessKvStore::restore(
        sinomo_runtime::kv::read_cluster_kv_cached_snapshot(&cluster_paths.kv_paths())
            .expect("cluster kv"),
    );
    assert!(store.get(sinomo_lm::LM_PROVIDER_CONFIG_KV_KEY).is_none());
}

#[test]
fn models_runtime_writes_cluster_runtime_config_to_kv_for_clustered_node() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-cluster-model-runtime");
    let data_dir = SinomoDataDir::new(&root);
    let cluster_paths = ClusterPaths::new(&data_dir);
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(ClusterId::from_legacy_named("lm_runtime").expect("cluster")),
            node_id: Some(NodeId::from_legacy_named("lm_runtime_node").expect("node")),
            name: "lm runtime".to_string(),
        },
        &cluster_paths,
    )
    .expect("cluster init");

    let config = sinomo_lm::ModelRuntimeConfig {
        native_models_cache_dir: Some("/cluster/models".to_string()),
        minicpm_v46: sinomo_lm::MiniCpmV46RuntimeConfig {
            model_dir: Some("/cluster/models/minicpm-v46".to_string()),
            max_input_tokens: Some(32_768),
            max_output_tokens: Some(4096),
        },
        pytorch_bridge: sinomo_lm::PytorchBridgeRuntimeConfig {
            endpoint: Some("https://runtime.example.invalid/api".to_string()),
            python: Some("/cluster/bin/python".to_string()),
            device: Some("cuda".to_string()),
            timeout_seconds: Some(60),
            mock: Some(false),
        },
    };
    write_model_runtime_config_for_node(&data_dir, &config).expect("write runtime config");

    assert!(
        !root
            .join("configs")
            .join("lm")
            .join("runtime.json")
            .exists()
    );
    let store = sinomo_runtime::kv::InProcessKvStore::restore(
        sinomo_runtime::kv::read_cluster_kv_cached_snapshot(&cluster_paths.kv_paths())
            .expect("cluster kv"),
    );
    let entry = store
        .get(sinomo_lm::LM_RUNTIME_CONFIG_KV_KEY)
        .expect("lm runtime kv entry");
    let document: sinomo_runtime::config::ConfigDocument =
        entry.decode().expect("runtime config document");
    assert_eq!(document.key.as_str(), sinomo_lm::LM_RUNTIME_CONFIG_KEY);
    let saved: sinomo_lm::ModelRuntimeConfig =
        serde_json::from_value(document.value).expect("runtime config");
    assert_eq!(saved, config);
}

#[test]
fn local_mode_writes_model_runtime_config_to_local_file_on_clustered_node() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-local-mode-model-runtime");
    let data_dir = SinomoDataDir::new(&root);
    let cluster_paths = ClusterPaths::new(&data_dir);
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(ClusterId::from_legacy_named("local_runtime").expect("cluster")),
            node_id: Some(NodeId::from_legacy_named("local_runtime_node").expect("node")),
            name: "local runtime".to_string(),
        },
        &cluster_paths,
    )
    .expect("cluster init");

    let config = sinomo_lm::ModelRuntimeConfig {
        pytorch_bridge: sinomo_lm::PytorchBridgeRuntimeConfig {
            endpoint: Some("http://127.0.0.1:32154".to_string()),
            timeout_seconds: Some(60),
            ..sinomo_lm::PytorchBridgeRuntimeConfig::default()
        },
        ..sinomo_lm::ModelRuntimeConfig::default()
    };
    let path = with_cli_force_local_mode(true, || {
        write_model_runtime_config_for_node(&data_dir, &config)
    })
    .expect("write local runtime config");

    assert_eq!(path, root.join("configs").join("lm").join("runtime.json"));
    let document: sinomo_runtime::config::ConfigDocument =
        serde_json::from_str(&std::fs::read_to_string(&path).expect("runtime config"))
            .expect("runtime config document");
    assert_eq!(document.key.as_str(), sinomo_lm::LM_RUNTIME_CONFIG_KEY);
    let saved: sinomo_lm::ModelRuntimeConfig =
        serde_json::from_value(document.value).expect("runtime config json");
    assert_eq!(saved, config);
    let store = sinomo_runtime::kv::InProcessKvStore::restore(
        sinomo_runtime::kv::read_cluster_kv_cached_snapshot(&cluster_paths.kv_paths())
            .expect("cluster kv"),
    );
    assert!(store.get(sinomo_lm::LM_RUNTIME_CONFIG_KV_KEY).is_none());
}

#[test]
fn sandbox_policy_writes_cluster_policy_config_to_kv_for_clustered_node() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-cluster-sandbox-policy");
    let data_dir = SinomoDataDir::new(&root);
    let cluster_paths = ClusterPaths::new(&data_dir);
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(ClusterId::from_legacy_named("sandbox_policy").expect("cluster")),
            node_id: Some(NodeId::from_legacy_named("sandbox_policy_node").expect("node")),
            name: "sandbox policy".to_string(),
        },
        &cluster_paths,
    )
    .expect("cluster init");

    assert!(sandbox_policy_uses_cluster(&data_dir));
    let toolchain_id = sinomo_runtime::sandbox::SandboxToolchainId::generate();
    let mut config = sinomo_runtime::sandbox::SandboxClusterPolicyConfig::default();
    config.policy.allow_unsigned_toolchains = true;
    config.policy.allow_remote_local_owned_mounts = true;
    config
        .policy
        .allowed_mount_prefixes
        .push("/sinomo/nodes".to_string());
    config
        .policy
        .allowed_command_capabilities
        .insert("network".to_string());
    config
        .policy
        .allowed_http_hosts
        .insert("packages.example".to_string());
    config
        .policy
        .allowed_toolchain_ids
        .insert(toolchain_id.clone());
    config
        .policy
        .allowed_toolchain_provenance_prefixes
        .insert("sinomo://toolchains/".to_string());
    config
        .policy
        .allowed_package_cache_prefixes
        .push("/services/packages".to_string());
    config.policy.max_limits.memory_mb = 1024;
    config.policy.max_limits.wall_time_ms = 5000;
    config.policy.max_limits.max_input_bytes = 3072;
    config.policy.max_limits.max_output_bytes = 4096;
    config.policy.max_limits.max_workspace_bytes = 8192;
    config.policy.max_limits.max_written_bytes_per_command = 2048;
    config.policy.max_limits.max_file_count = 128;

    write_sandbox_policy_config_for_node(&data_dir, &config).expect("write sandbox policy");

    let store = sinomo_runtime::kv::InProcessKvStore::restore(
        sinomo_runtime::kv::read_cluster_kv_cached_snapshot(&cluster_paths.kv_paths())
            .expect("cluster kv"),
    );
    let entry = store
        .get(sinomo_runtime::sandbox::SANDBOX_POLICY_CONFIG_KV_KEY)
        .expect("sandbox policy kv entry");
    let document: sinomo_runtime::config::ConfigDocument =
        entry.decode().expect("sandbox policy config document");
    assert_eq!(
        document.key.as_str(),
        sinomo_runtime::sandbox::SANDBOX_POLICY_CONFIG_KEY
    );
    let saved: sinomo_runtime::sandbox::SandboxClusterPolicyConfig =
        serde_json::from_value(document.value).expect("sandbox policy config");
    assert_eq!(saved, config);
}

#[test]
fn local_mode_writes_sandbox_policy_to_local_file_on_clustered_node() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-local-mode-sandbox-policy");
    let data_dir = SinomoDataDir::new(&root);
    let cluster_paths = ClusterPaths::new(&data_dir);
    cluster_init_with_paths(
        ClusterInitOptions {
            cluster_id: Some(ClusterId::from_legacy_named("local_policy").expect("cluster")),
            node_id: Some(NodeId::from_legacy_named("local_policy_node").expect("node")),
            name: "local policy".to_string(),
        },
        &cluster_paths,
    )
    .expect("cluster init");

    let mut config = sinomo_runtime::sandbox::SandboxClusterPolicyConfig::default();
    config.policy.allow_unsigned_toolchains = true;
    config
        .policy
        .allowed_command_capabilities
        .insert("network".to_string());

    let path = with_cli_force_local_mode(true, || {
        assert!(!sandbox_policy_uses_cluster(&data_dir));
        write_sandbox_policy_config_for_node(&data_dir, &config)
    })
    .expect("write local sandbox policy");

    assert_eq!(
        path,
        root.join("configs").join("sandbox").join("policy.json")
    );
    let document: sinomo_runtime::config::ConfigDocument =
        serde_json::from_str(&std::fs::read_to_string(&path).expect("sandbox policy config"))
            .expect("sandbox policy config document");
    assert_eq!(
        document.key.as_str(),
        sinomo_runtime::sandbox::SANDBOX_POLICY_CONFIG_KEY
    );
    let saved: sinomo_runtime::sandbox::SandboxClusterPolicyConfig =
        serde_json::from_value(document.value).expect("sandbox policy json");
    assert_eq!(saved, config);
    let store = sinomo_runtime::kv::InProcessKvStore::restore(
        sinomo_runtime::kv::read_cluster_kv_cached_snapshot(&cluster_paths.kv_paths())
            .expect("cluster kv"),
    );
    assert!(
        store
            .get(sinomo_runtime::sandbox::SANDBOX_POLICY_CONFIG_KV_KEY)
            .is_none()
    );
}

#[test]
fn models_runtime_parser_accepts_cluster_runtime_settings() {
    let command = parse_models_runtime_command(&[
        "set".to_string(),
        "--native-models-cache-dir".to_string(),
        "/cluster/models".to_string(),
        "--minicpm-v46-model-dir".to_string(),
        "/cluster/models/minicpm-v46".to_string(),
        "--minicpm-v46-max-input-tokens".to_string(),
        "32768".to_string(),
        "--minicpm-v46-max-output-tokens".to_string(),
        "4096".to_string(),
        "--pytorch-endpoint".to_string(),
        "https://runtime.example.invalid/api".to_string(),
        "--pytorch-python".to_string(),
        "/cluster/bin/python".to_string(),
        "--pytorch-device".to_string(),
        "cuda".to_string(),
        "--pytorch-timeout-seconds".to_string(),
        "60".to_string(),
        "--pytorch-mock".to_string(),
        "false".to_string(),
    ])
    .expect("runtime command");

    assert_eq!(
        command,
        ModelRuntimeCliCommand::Set(ModelRuntimeSetOptions {
            native_models_cache_dir: Some("/cluster/models".to_string()),
            minicpm_v46_model_dir: Some("/cluster/models/minicpm-v46".to_string()),
            minicpm_v46_max_input_tokens: Some(32_768),
            minicpm_v46_max_output_tokens: Some(4096),
            pytorch_endpoint: Some("https://runtime.example.invalid/api".to_string()),
            pytorch_python: Some("/cluster/bin/python".to_string()),
            pytorch_device: Some("cuda".to_string()),
            pytorch_timeout_seconds: Some(60),
            pytorch_mock: Some(false),
        })
    );
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
    assert_eq!(
        normalize_model_provider("deepseek-api").as_deref(),
        Some("deepseek")
    );
    assert_eq!(normalize_model_provider("codex-api"), None);
    assert_eq!(normalize_model_provider("openai-codex"), None);
    assert_eq!(normalize_model_provider("openai-codex-cli"), None);
}
