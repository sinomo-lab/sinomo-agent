use super::*;

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum RolloutCliCommand {
    Help,
    InitSource(RolloutOptions),
    ImportSource(RolloutOptions),
    ExportSource(RolloutOptions),
    ApplyExternal(RolloutOptions),
    Apply(RolloutOptions),
    Cluster(RolloutOptions),
    ModuleNativeDylib(RolloutModuleNativeDylibOptions),
    Advance(RolloutAdvanceOptions),
    TelemetryGatesShow,
    TelemetryGatesSet(RolloutTelemetryGateSetOptions),
    Diagnostics(RolloutOptions),
    ClearStatus(RolloutOptions),
    Status,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct RolloutOptions {
    pub(crate) repo: Option<PathBuf>,
    pub(crate) binary: Option<PathBuf>,
    pub(crate) remote_repo: Option<PathBuf>,
    pub(crate) targets: Vec<String>,
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) shadow_port: u16,
    pub(crate) skip_local: bool,
    pub(crate) remote_build: bool,
    pub(crate) force_source_overwrite: bool,
}

impl Default for RolloutOptions {
    fn default() -> Self {
        Self {
            repo: None,
            binary: None,
            remote_repo: None,
            targets: Vec::new(),
            host: SINOMO_DAEMON_DEFAULT_HOST.to_string(),
            port: SINOMO_DAEMON_DEFAULT_PORT,
            shadow_port: SINOMO_SHADOW_DAEMON_DEFAULT_PORT,
            skip_local: false,
            remote_build: false,
            force_source_overwrite: false,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct RolloutModuleNativeDylibOptions {
    pub(crate) module_selector: String,
    pub(crate) rollout: RolloutOptions,
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub(crate) struct RolloutAdvanceOptions {
    pub(crate) node_id: Option<NodeId>,
    pub(crate) scorecard: Option<PathBuf>,
    pub(crate) module_replay_report: Option<PathBuf>,
    pub(crate) shadow_telemetry_limit: Option<usize>,
    pub(crate) incident_event_ids: Vec<EventId>,
    pub(crate) user_correction_count: u32,
    pub(crate) rollback_triggers: Vec<RollbackTrigger>,
    pub(crate) evidence_refs: Vec<String>,
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub(crate) struct RolloutTelemetryGateSetOptions {
    pub(crate) automatic_promotion_enabled: Option<bool>,
    pub(crate) event_limit: Option<usize>,
    pub(crate) minimum_event_count: Option<u64>,
    pub(crate) maximum_automatic_weight_bps: Option<Option<u16>>,
    pub(crate) maximum_candidate_latency_p95_ms: Option<Option<u64>>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct RolloutTarget {
    pub(crate) address: String,
    pub(crate) endpoint: Option<ClusterEndpointRecord>,
    pub(crate) rollout_node_id: Option<NodeId>,
}

impl RolloutTarget {
    pub(crate) fn address(address: String) -> Self {
        Self {
            address,
            endpoint: None,
            rollout_node_id: None,
        }
    }

    pub(crate) fn endpoint(endpoint: ClusterEndpointRecord) -> Self {
        Self {
            address: endpoint.address.clone(),
            rollout_node_id: Some(endpoint.node_id.clone()),
            endpoint: Some(endpoint),
        }
    }

    pub(crate) fn endpoint_for_node(
        endpoint: ClusterEndpointRecord,
        rollout_node_id: NodeId,
    ) -> Self {
        Self {
            address: endpoint.address.clone(),
            endpoint: Some(endpoint),
            rollout_node_id: Some(rollout_node_id),
        }
    }

    pub(crate) fn label(&self) -> &str {
        &self.address
    }

    pub(crate) fn rollout_node_id(&self) -> Option<&NodeId> {
        self.rollout_node_id.as_ref()
    }
}

pub(crate) fn parse_rollout_command(args: &[String]) -> Result<RolloutCliCommand, CliError> {
    match args.split_first() {
        None => Ok(RolloutCliCommand::Help),
        Some((command, [])) if matches!(command.as_str(), "help" | "--help" | "-h") => {
            Ok(RolloutCliCommand::Help)
        }
        Some((command, rest)) if command == "init-source" => {
            Ok(RolloutCliCommand::InitSource(parse_rollout_options(rest)?))
        }
        Some((command, rest))
            if matches!(command.as_str(), "import-source" | "sync-from-external") =>
        {
            Ok(RolloutCliCommand::ImportSource(parse_rollout_options(
                rest,
            )?))
        }
        Some((command, rest))
            if matches!(command.as_str(), "export-source" | "sync-to-external") =>
        {
            Ok(RolloutCliCommand::ExportSource(parse_rollout_options(
                rest,
            )?))
        }
        Some((command, rest)) if matches!(command.as_str(), "apply-external" | "from-external") => {
            Ok(RolloutCliCommand::ApplyExternal(parse_rollout_options(
                rest,
            )?))
        }
        Some((command, rest)) if command == "apply" => {
            Ok(RolloutCliCommand::Apply(parse_rollout_options(rest)?))
        }
        Some((command, rest)) if matches!(command.as_str(), "cluster" | "apply-cluster") => {
            Ok(RolloutCliCommand::Cluster(parse_rollout_options(rest)?))
        }
        Some((command, rest)) if command == "module" => parse_rollout_module_command(rest),
        Some((command, rest)) if command == "advance" => Ok(RolloutCliCommand::Advance(
            parse_rollout_advance_options(rest)?,
        )),
        Some((command, rest))
            if matches!(command.as_str(), "telemetry-gates" | "telemetry-gate") =>
        {
            parse_rollout_telemetry_gates_command(rest)
        }
        Some((command, rest)) if matches!(command.as_str(), "diagnostics" | "diag") => {
            Ok(RolloutCliCommand::Diagnostics(parse_rollout_options(rest)?))
        }
        Some((command, rest)) if matches!(command.as_str(), "clear-status" | "clear-state") => {
            Ok(RolloutCliCommand::ClearStatus(parse_rollout_options(rest)?))
        }
        Some((command, [])) if command == "status" => Ok(RolloutCliCommand::Status),
        _ => Err(CliError::Usage),
    }
}

fn parse_rollout_module_command(args: &[String]) -> Result<RolloutCliCommand, CliError> {
    match args.split_first() {
        Some((kind, rest)) if kind == "native-dylib" => Ok(RolloutCliCommand::ModuleNativeDylib(
            parse_rollout_module_native_dylib_options(rest)?,
        )),
        _ => Err(CliError::Usage),
    }
}

pub(crate) fn parse_rollout_options(args: &[String]) -> Result<RolloutOptions, CliError> {
    let mut options = RolloutOptions::default();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--repo" => {
                index += 1;
                let Some(repo) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.repo = Some(PathBuf::from(repo));
            }
            "--binary" => {
                index += 1;
                let Some(binary) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.binary = Some(PathBuf::from(binary));
            }
            "--remote-repo" => {
                index += 1;
                let Some(repo) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.remote_repo = Some(PathBuf::from(repo));
            }
            "--target" => {
                index += 1;
                let Some(target) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.targets.push(non_empty_address(target)?);
            }
            "--host" => {
                index += 1;
                let Some(host) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.host = host.clone();
            }
            "--port" => {
                index += 1;
                let Some(port) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.port = port.parse::<u16>().map_err(|source| CliError::Parse {
                    detail: format!("invalid rollout daemon port `{port}`: {source}"),
                })?;
            }
            "--shadow-port" => {
                index += 1;
                let Some(port) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.shadow_port = port.parse::<u16>().map_err(|source| CliError::Parse {
                    detail: format!("invalid rollout shadow daemon port `{port}`: {source}"),
                })?;
            }
            "--skip-local" => options.skip_local = true,
            "--remote-build" => options.remote_build = true,
            "--force-source" | "--force-overwrite" => options.force_source_overwrite = true,
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }
    Ok(options)
}

fn parse_rollout_module_native_dylib_options(
    args: &[String],
) -> Result<RolloutModuleNativeDylibOptions, CliError> {
    let mut rollout_args = Vec::new();
    let mut selector = None;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--module" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                if selector
                    .replace(non_empty_rollout_module_selector(value)?)
                    .is_some()
                {
                    return Err(CliError::Usage);
                }
            }
            value if value.starts_with('-') => {
                rollout_args.push(args[index].clone());
                if matches!(
                    value,
                    "--repo"
                        | "--binary"
                        | "--remote-repo"
                        | "--target"
                        | "--host"
                        | "--port"
                        | "--shadow-port"
                ) {
                    index += 1;
                    let Some(next) = args.get(index) else {
                        return Err(CliError::Usage);
                    };
                    rollout_args.push(next.clone());
                }
            }
            value => {
                if selector
                    .replace(non_empty_rollout_module_selector(value)?)
                    .is_some()
                {
                    return Err(CliError::Usage);
                }
            }
        }
        index += 1;
    }
    let Some(module_selector) = selector else {
        return Err(CliError::Usage);
    };
    let rollout = parse_rollout_options(&rollout_args)?;
    if rollout.binary.is_some()
        || rollout.remote_build
        || rollout.remote_repo.is_some()
        || rollout.force_source_overwrite
    {
        return Err(CliError::Parse {
            detail:
                "module native-dylib rollout supports --repo, --target, --host, --port, --shadow-port, and --skip-local"
                    .to_string(),
        });
    }
    Ok(RolloutModuleNativeDylibOptions {
        module_selector,
        rollout,
    })
}

fn non_empty_rollout_module_selector(value: &str) -> Result<String, CliError> {
    let value = value.trim();
    if value.is_empty() {
        return Err(CliError::Parse {
            detail: "module selector must not be empty".to_string(),
        });
    }
    Ok(value.to_string())
}

pub(crate) fn parse_rollout_advance_options(
    args: &[String],
) -> Result<RolloutAdvanceOptions, CliError> {
    let mut options = RolloutAdvanceOptions::default();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--node" | "--node-id" => {
                index += 1;
                let Some(node_id) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.node_id = Some(parse_rollout_node_id(node_id)?);
            }
            "--scorecard" => {
                index += 1;
                let Some(scorecard) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.scorecard = Some(PathBuf::from(scorecard));
            }
            "--module-replay-report" => {
                index += 1;
                let Some(report) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.module_replay_report = Some(PathBuf::from(report));
            }
            "--shadow-telemetry" => {
                options
                    .shadow_telemetry_limit
                    .get_or_insert(DEFAULT_SHADOW_TELEMETRY_LIMIT);
            }
            "--shadow-telemetry-limit" => {
                index += 1;
                let Some(limit) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                let limit = limit.parse::<usize>().map_err(|source| CliError::Parse {
                    detail: format!("invalid rollout shadow telemetry limit `{limit}`: {source}"),
                })?;
                if limit == 0 {
                    return Err(CliError::Parse {
                        detail: "rollout shadow telemetry limit must be non-zero".to_string(),
                    });
                }
                options.shadow_telemetry_limit = Some(limit);
            }
            "--incident" | "--incident-event" => {
                index += 1;
                let Some(event_id) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options
                    .incident_event_ids
                    .push(parse_rollout_event_id(event_id)?);
            }
            "--user-corrections" | "--user-correction-count" => {
                index += 1;
                let Some(count) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.user_correction_count =
                    count.parse::<u32>().map_err(|source| CliError::Parse {
                        detail: format!("invalid user correction count `{count}`: {source}"),
                    })?;
            }
            "--rollback-trigger" => {
                index += 1;
                let Some(trigger) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options
                    .rollback_triggers
                    .push(parse_rollout_rollback_trigger(trigger)?);
            }
            "--evidence" => {
                index += 1;
                let Some(reference) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                if reference.trim().is_empty() {
                    return Err(CliError::Parse {
                        detail: "rollout evidence reference cannot be empty".to_string(),
                    });
                }
                options.evidence_refs.push(reference.clone());
            }
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }
    Ok(options)
}

pub(crate) fn parse_rollout_telemetry_gates_command(
    args: &[String],
) -> Result<RolloutCliCommand, CliError> {
    match args.split_first() {
        None => Ok(RolloutCliCommand::TelemetryGatesShow),
        Some((command, [])) if command == "show" => Ok(RolloutCliCommand::TelemetryGatesShow),
        Some((command, rest)) if command == "set" => Ok(RolloutCliCommand::TelemetryGatesSet(
            parse_rollout_telemetry_gate_set_options(rest)?,
        )),
        _ => Err(CliError::Usage),
    }
}

pub(crate) fn parse_rollout_telemetry_gate_set_options(
    args: &[String],
) -> Result<RolloutTelemetryGateSetOptions, CliError> {
    let mut options = RolloutTelemetryGateSetOptions::default();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--enable-auto-promotion" => {
                options.automatic_promotion_enabled = Some(true);
            }
            "--disable-auto-promotion" => {
                options.automatic_promotion_enabled = Some(false);
            }
            "--event-limit" => {
                index += 1;
                let Some(limit) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                let limit = parse_nonzero_usize("rollout telemetry event limit", limit)?;
                options.event_limit = Some(limit);
            }
            "--min-events" | "--minimum-event-count" => {
                index += 1;
                let Some(count) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                let count = count.parse::<u64>().map_err(|source| CliError::Parse {
                    detail: format!(
                        "invalid rollout telemetry minimum event count `{count}`: {source}"
                    ),
                })?;
                if count == 0 {
                    return Err(CliError::Parse {
                        detail: "rollout telemetry minimum event count must be non-zero"
                            .to_string(),
                    });
                }
                options.minimum_event_count = Some(count);
            }
            "--max-auto-weight-bps" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.maximum_automatic_weight_bps = Some(parse_optional_bps(value)?);
            }
            "--max-candidate-latency-p95-ms" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.maximum_candidate_latency_p95_ms =
                    Some(parse_optional_u64("candidate latency p95", value)?);
            }
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }
    Ok(options)
}

fn parse_nonzero_usize(label: &str, value: &str) -> Result<usize, CliError> {
    let value = value.parse::<usize>().map_err(|source| CliError::Parse {
        detail: format!("invalid {label} `{value}`: {source}"),
    })?;
    if value == 0 {
        return Err(CliError::Parse {
            detail: format!("{label} must be non-zero"),
        });
    }
    Ok(value)
}

fn parse_optional_bps(value: &str) -> Result<Option<u16>, CliError> {
    if matches!(value, "none" | "off" | "unlimited") {
        return Ok(None);
    }
    let weight = value.parse::<u16>().map_err(|source| CliError::Parse {
        detail: format!("invalid rollout telemetry max automatic weight `{value}`: {source}"),
    })?;
    if weight > 10_000 {
        return Err(CliError::Parse {
            detail: "rollout telemetry max automatic weight must be <= 10000 bps".to_string(),
        });
    }
    Ok(Some(weight))
}

fn parse_optional_u64(label: &str, value: &str) -> Result<Option<u64>, CliError> {
    if matches!(value, "none" | "off" | "unlimited") {
        return Ok(None);
    }
    let parsed = value.parse::<u64>().map_err(|source| CliError::Parse {
        detail: format!("invalid rollout telemetry {label} `{value}`: {source}"),
    })?;
    if parsed == 0 {
        return Err(CliError::Parse {
            detail: format!("rollout telemetry {label} must be non-zero"),
        });
    }
    Ok(Some(parsed))
}

fn parse_rollout_node_id(value: &str) -> Result<NodeId, CliError> {
    NodeId::new(value.to_string()).map_err(|source| CliError::Parse {
        detail: format!("invalid rollout node id `{value}`: {source}"),
    })
}

fn parse_rollout_event_id(value: &str) -> Result<EventId, CliError> {
    EventId::new(value.to_string()).map_err(|source| CliError::Parse {
        detail: format!("invalid rollout incident event id `{value}`: {source}"),
    })
}

fn parse_rollout_rollback_trigger(value: &str) -> Result<RollbackTrigger, CliError> {
    match value {
        "build-failure" => Ok(RollbackTrigger::BuildFailure),
        "load-failure" => Ok(RollbackTrigger::LoadFailure),
        "panic" => Ok(RollbackTrigger::Panic),
        "timeout" => Ok(RollbackTrigger::Timeout),
        "resource-budget-violation" => Ok(RollbackTrigger::ResourceBudgetViolation),
        "new-denied-capability-request" => Ok(RollbackTrigger::NewDeniedCapabilityRequest),
        "policy-incident" => Ok(RollbackTrigger::PolicyIncident),
        "score-regression" => Ok(RollbackTrigger::ScoreRegression),
        "emergency-stop" => Ok(RollbackTrigger::EmergencyStop),
        "user-correction-spike" => Ok(RollbackTrigger::UserCorrectionSpike),
        "node-health-degradation" => Ok(RollbackTrigger::NodeHealthDegradation),
        _ => Err(CliError::Parse {
            detail: format!("invalid rollback trigger `{value}`"),
        }),
    }
}

pub(crate) fn run_rollout_cli_command(command: RolloutCliCommand) -> Result<(), CliError> {
    match command {
        RolloutCliCommand::Help => {
            print_rollout_help();
            Ok(())
        }
        RolloutCliCommand::InitSource(options) => rollout_init_source(options),
        RolloutCliCommand::ImportSource(options) => rollout_import_source(options),
        RolloutCliCommand::ExportSource(options) => rollout_export_source(options),
        RolloutCliCommand::ApplyExternal(options) => rollout_apply_external(options),
        RolloutCliCommand::Apply(options) => rollout_apply(options),
        RolloutCliCommand::Cluster(options) => rollout_cluster(options),
        RolloutCliCommand::ModuleNativeDylib(options) => rollout_module_native_dylib(options),
        RolloutCliCommand::Advance(options) => rollout_advance(options),
        RolloutCliCommand::TelemetryGatesShow => rollout_telemetry_gates_show(),
        RolloutCliCommand::TelemetryGatesSet(options) => rollout_telemetry_gates_set(options),
        RolloutCliCommand::Diagnostics(options) => rollout_diagnostics(options),
        RolloutCliCommand::ClearStatus(options) => rollout_clear_status(options),
        RolloutCliCommand::Status => rollout_status(),
    }
}

pub(crate) fn rollout_paths_from_defaults() -> Result<SelfRolloutPaths, CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    Ok(SelfRolloutPaths::new(&data_dir.root()))
}

pub(crate) fn rollout_module_native_dylib(
    options: RolloutModuleNativeDylibOptions,
) -> Result<(), CliError> {
    let targets = rollout_cluster_targets(&options.rollout)?;
    if targets.is_empty() {
        return Err(CliError::Parse {
            detail: "no module rollout targets found; pass --target <host:port> or start a clustered local daemon".to_string(),
        });
    }
    let paths = rollout_paths_from_defaults()?;
    ensure_rollout_layout(&paths)?;
    let repo = rollout_source_repo_from_options(&options.rollout)?;
    let requested_at = OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "unknown".to_string());
    let artifact = rollout_build_dev_native_dylib_module(
        &repo,
        &paths.plan().build_log,
        &options.module_selector,
    )?;
    println!(
        "module rollout artifact: {}",
        artifact.built_library.display()
    );
    println!("  module: {}", artifact.module_id);
    println!("  interface: {}", artifact.interface_version);
    println!("  source: {}", artifact.source);
    let request = prepare_cluster_rollout_native_dylib_module_request(
        artifact.source.clone(),
        &artifact,
        options.rollout.host.clone(),
        options.rollout.port,
        options.rollout.shadow_port,
        requested_at,
    )?;

    for target in targets {
        let target_status = match read_rollout_target_status(&target) {
            Ok(status) => status,
            Err(error) => {
                println!("{}: failed: {error}", target.label());
                continue;
            }
        };
        if let Err(error) =
            ensure_rollout_target_platform_from_status(target.label(), &target_status)
        {
            println!("{}: skipped: {error}", target.label());
            continue;
        }
        match send_daemon_rollout_native_dylib_module(&target, &request) {
            Ok(response) => {
                println!("{}: {}", target.label(), response.status);
                if !response.detail.is_empty() {
                    println!("  {}", response.detail);
                }
            }
            Err(error) => println!("{}: failed: {error}", target.label()),
        }
    }
    Ok(())
}

pub(crate) fn rollout_init_source(options: RolloutOptions) -> Result<(), CliError> {
    let paths = rollout_paths_from_defaults()?;
    ensure_rollout_layout(&paths)?;
    ensure_rollout_source_repo_with_options(
        &paths,
        options.repo.as_deref(),
        rollout_source_deploy_options(&options),
    )?;
    println!("source repo: {}", paths.source_repo.display());
    Ok(())
}

pub(crate) fn rollout_import_source(options: RolloutOptions) -> Result<(), CliError> {
    let paths = rollout_paths_from_defaults()?;
    ensure_rollout_layout(&paths)?;
    ensure_rollout_source_repo_with_options(
        &paths,
        options.repo.as_deref(),
        rollout_source_deploy_options(&options),
    )?;
    println!("imported external source into managed repo");
    println!("  managed source: {}", paths.source_repo.display());
    print_managed_source_status(&paths)?;
    Ok(())
}

pub(crate) fn rollout_export_source(options: RolloutOptions) -> Result<(), CliError> {
    let Some(repo) = options.repo.as_deref() else {
        return Err(CliError::Parse {
            detail: "export-source requires --repo <external-repo>".to_string(),
        });
    };
    let paths = rollout_paths_from_defaults()?;
    ensure_rollout_layout(&paths)?;
    sync_external_repo_from_managed(&paths, repo)?;
    println!("exported managed source changes into external repo");
    println!("  managed source: {}", paths.source_repo.display());
    println!("  external repo: {}", repo.display());
    Ok(())
}

pub(crate) fn rollout_apply_external(options: RolloutOptions) -> Result<(), CliError> {
    rollout_import_source(options.clone())?;
    let mut apply_options = options;
    apply_options.repo = None;
    rollout_apply(apply_options)
}

pub(crate) fn rollout_advance(options: RolloutAdvanceOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    let applied = rollout_advance_with_data_dir(&data_dir, options)?;
    println!("rollout action: {:?}", applied.transition.action);
    println!("stage: {:?}", applied.transition.stage);
    println!("weight bps: {}", applied.transition.current_weight_bps);
    println!("record: {}", applied.transition_path.display());
    println!("module state: {}", applied.module_state_path.display());
    Ok(())
}

pub(crate) fn rollout_advance_with_data_dir(
    data_dir: &SinomoDataDir,
    options: RolloutAdvanceOptions,
) -> Result<AppliedRolloutTransition, CliError> {
    data_dir.ensure_root_layout()?;
    let paths = SelfRolloutPaths::new(&data_dir.root());
    let node_id = resolve_rollout_controller_node_id(data_dir, options.node_id.clone())?;
    let runtime_state =
        if options.module_replay_report.is_some() || options.shadow_telemetry_limit.is_some() {
            read_rollout_runtime_state(&paths).map_err(CliError::from)?
        } else {
            None
        };
    let data_dir_root = data_dir.root();
    let (observation, evidence_refs) =
        rollout_observation_from_advance_options(&data_dir_root, options, runtime_state.as_ref())?;
    let controller = RolloutController::new(node_id);
    let recorded = controller
        .advance_recorded_rollout(&paths, observation, evidence_refs)
        .map_err(CliError::from)?;
    controller
        .apply_recorded_transition(data_dir.root(), &recorded)
        .map_err(Into::into)
}

pub(crate) fn rollout_telemetry_gates_show() -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    let paths = SelfRolloutPaths::new(&data_dir.root());
    let config = read_rollout_telemetry_gate_config(&paths)?.unwrap_or_default();
    println!(
        "automatic promotion: {}",
        config.automatic_promotion_enabled
    );
    println!("event limit: {}", config.event_limit);
    println!("minimum events: {}", config.minimum_event_count);
    println!(
        "max automatic weight bps: {}",
        config
            .maximum_automatic_weight_bps
            .map(|weight| weight.to_string())
            .unwrap_or_else(|| "unlimited".to_string())
    );
    println!(
        "max candidate latency p95 ms: {}",
        config
            .maximum_candidate_latency_p95_ms
            .map(|latency| latency.to_string())
            .unwrap_or_else(|| "unlimited".to_string())
    );
    Ok(())
}

pub(crate) fn rollout_telemetry_gates_set(
    options: RolloutTelemetryGateSetOptions,
) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    let config = rollout_telemetry_gates_set_with_data_dir(&data_dir, options)?;
    println!("rollout telemetry gates updated");
    println!(
        "automatic promotion: {}",
        config.automatic_promotion_enabled
    );
    println!("event limit: {}", config.event_limit);
    println!("minimum events: {}", config.minimum_event_count);
    println!(
        "max automatic weight bps: {}",
        config
            .maximum_automatic_weight_bps
            .map(|weight| weight.to_string())
            .unwrap_or_else(|| "unlimited".to_string())
    );
    println!(
        "max candidate latency p95 ms: {}",
        config
            .maximum_candidate_latency_p95_ms
            .map(|latency| latency.to_string())
            .unwrap_or_else(|| "unlimited".to_string())
    );
    Ok(())
}

pub(crate) fn rollout_telemetry_gates_set_with_data_dir(
    data_dir: &SinomoDataDir,
    options: RolloutTelemetryGateSetOptions,
) -> Result<RolloutTelemetryGateConfig, CliError> {
    data_dir.ensure_root_layout()?;
    let paths = SelfRolloutPaths::new(&data_dir.root());
    let mut config = read_rollout_telemetry_gate_config(&paths)?.unwrap_or_default();
    if let Some(enabled) = options.automatic_promotion_enabled {
        config.automatic_promotion_enabled = enabled;
    }
    if let Some(limit) = options.event_limit {
        config.event_limit = limit;
    }
    if let Some(count) = options.minimum_event_count {
        config.minimum_event_count = count;
    }
    if let Some(maximum_weight) = options.maximum_automatic_weight_bps {
        config.maximum_automatic_weight_bps = maximum_weight;
    }
    if let Some(maximum_latency) = options.maximum_candidate_latency_p95_ms {
        config.maximum_candidate_latency_p95_ms = maximum_latency;
    }
    write_rollout_telemetry_gate_config(&paths, &config)?;
    Ok(config)
}

fn rollout_observation_from_advance_options(
    data_dir_root: &Path,
    options: RolloutAdvanceOptions,
    runtime_state: Option<&RolloutRuntimeState>,
) -> Result<(RolloutObservation, Vec<String>), CliError> {
    let RolloutAdvanceOptions {
        node_id: _,
        scorecard,
        module_replay_report,
        shadow_telemetry_limit,
        incident_event_ids,
        user_correction_count,
        rollback_triggers,
        mut evidence_refs,
    } = options;
    let evidence_source_count = usize::from(scorecard.is_some())
        + usize::from(module_replay_report.is_some())
        + usize::from(shadow_telemetry_limit.is_some());
    if evidence_source_count > 1 {
        return Err(CliError::Parse {
            detail: "rollout advance accepts only one of --scorecard, --module-replay-report, or --shadow-telemetry".to_string(),
        });
    }
    let scorecard = if let Some(path) = scorecard {
        evidence_refs.push(format!("scorecard:{}", path.display()));
        Some(read_rollout_scorecard(&path)?)
    } else if let Some(path) = module_replay_report {
        evidence_refs.push(format!("module-replay-report:{}", path.display()));
        Some(read_rollout_module_replay_scorecard(&path, runtime_state)?)
    } else if let Some(limit) = shadow_telemetry_limit {
        evidence_refs.push(format!(
            "module-shadow-telemetry:events/event-log.jsonl?limit={limit}"
        ));
        Some(read_rollout_shadow_telemetry_scorecard(
            data_dir_root,
            limit,
            runtime_state,
        )?)
    } else {
        None
    };
    Ok((
        RolloutObservation {
            scorecard,
            incident_event_ids,
            user_correction_count,
            rollback_triggers,
        },
        evidence_refs,
    ))
}

fn read_rollout_scorecard(path: &Path) -> Result<EvaluationScorecard, CliError> {
    let content = fs::read_to_string(path).map_err(|source| CliError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    serde_json::from_str(&content).map_err(|source| CliError::Json {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

fn read_rollout_module_replay_scorecard(
    path: &Path,
    runtime_state: Option<&RolloutRuntimeState>,
) -> Result<EvaluationScorecard, CliError> {
    let content = fs::read_to_string(path).map_err(|source| CliError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    let report: ModuleReplayReport =
        serde_json::from_str(&content).map_err(|source| CliError::Json {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
    if let Some(state) = runtime_state
        && report.module_id != state.plan.module_id
    {
        return Err(CliError::Parse {
            detail: format!(
                "module replay report {} targets module {}, but rollout state targets {}",
                path.display(),
                report.module_id,
                state.plan.module_id
            ),
        });
    }

    let baseline_artifact_id =
        runtime_state.map(|state| state.loader_state.active_artifact_id.clone());
    let candidate_artifact_id = runtime_state.and_then(|state| {
        state
            .loader_state
            .candidate_artifact_id
            .clone()
            .or_else(|| state.loader_state.shadow_artifact_id.clone())
            .or_else(|| Some(state.plan.artifact_id.clone()))
    });
    Ok(rollout_module_replay_report_scorecard(
        &report,
        baseline_artifact_id,
        candidate_artifact_id,
    ))
}

fn rollout_module_replay_report_scorecard(
    report: &ModuleReplayReport,
    baseline_artifact_id: Option<sinomo_runtime::module::ArtifactId>,
    candidate_artifact_id: Option<sinomo_runtime::module::ArtifactId>,
) -> EvaluationScorecard {
    let outcome = rollout_module_replay_report_outcome(report);
    EvaluationScorecard {
        scorecard_id: sinomo_runtime::core::Id::generate(),
        baseline_artifact_id,
        candidate_artifact_id,
        outcome: outcome.clone(),
        gates: vec![
            sinomo_runtime::core::EvaluationGateResult {
                gate: sinomo_runtime::core::EvaluationGate::ReplayTests,
                outcome: if report.missing_route_count == 0 && !report.cases.is_empty() {
                    sinomo_runtime::core::EvaluationOutcome::Passed
                } else {
                    sinomo_runtime::core::EvaluationOutcome::Failed
                },
                summary: format!(
                    "{} replay cases, {} missing routes",
                    report.cases.len(),
                    report.missing_route_count
                ),
            },
            sinomo_runtime::core::EvaluationGateResult {
                gate: sinomo_runtime::core::EvaluationGate::ShadowMode,
                outcome,
                summary: format!(
                    "{} primary errors, {} shadow errors, {} shadow mismatches",
                    report.primary_error_count,
                    report.shadow_error_count,
                    report.shadow_mismatch_count
                ),
            },
        ],
        metrics: vec![
            rollout_module_replay_metric("module_replay_case_count", report.cases.len() as i64),
            rollout_module_replay_metric(
                "module_replay_event_count",
                report.replay_event_count as i64,
            ),
            rollout_module_replay_metric(
                "module_replay_missing_route_count",
                report.missing_route_count as i64,
            ),
            rollout_module_replay_metric(
                "module_replay_primary_error_count",
                report.primary_error_count as i64,
            ),
            rollout_module_replay_metric(
                "module_replay_shadow_error_count",
                report.shadow_error_count as i64,
            ),
            rollout_module_replay_metric(
                "module_replay_shadow_mismatch_count",
                report.shadow_mismatch_count as i64,
            ),
        ],
    }
}

fn rollout_module_replay_report_outcome(
    report: &ModuleReplayReport,
) -> sinomo_runtime::core::EvaluationOutcome {
    if report.cases.is_empty() {
        return sinomo_runtime::core::EvaluationOutcome::Inconclusive;
    }
    if report.missing_route_count == 0
        && report.primary_error_count == 0
        && report.shadow_error_count == 0
        && report.shadow_mismatch_count == 0
    {
        sinomo_runtime::core::EvaluationOutcome::Passed
    } else {
        sinomo_runtime::core::EvaluationOutcome::Failed
    }
}

fn rollout_module_replay_metric(name: &str, value: i64) -> sinomo_runtime::core::EvaluationMetric {
    sinomo_runtime::core::EvaluationMetric {
        name: name.to_string(),
        value: sinomo_runtime::core::MetricValue::Integer(value),
        sample_count: None,
    }
}

fn read_rollout_shadow_telemetry_scorecard(
    data_dir_root: &Path,
    limit: usize,
    runtime_state: Option<&RolloutRuntimeState>,
) -> Result<EvaluationScorecard, CliError> {
    let state = runtime_state.ok_or_else(|| CliError::Parse {
        detail: "rollout shadow telemetry requires persisted rollout runtime state".to_string(),
    })?;
    let event_store = JsonlEventStore::new(data_dir_root.join("events").join("event-log.jsonl"));
    Ok(shadow_telemetry_scorecard_from_event_store(
        state,
        &event_store,
        limit,
    )?)
}

fn resolve_rollout_controller_node_id(
    data_dir: &SinomoDataDir,
    explicit_node_id: Option<NodeId>,
) -> Result<NodeId, CliError> {
    if let Some(node_id) = explicit_node_id {
        return Ok(node_id);
    }
    if let Some(status) = daemon_default_main_daemon_status(DAEMON_PROBE_TIMEOUT)?
        && let Some(cluster) = status.cluster
    {
        return Ok(cluster.node_id);
    }
    let cluster_paths = ClusterPaths::new(data_dir);
    if let Ok(state) = read_cluster_state(&cluster_paths) {
        return Ok(state.local_node_id);
    }
    let identity = load_or_create_main_node_identity(&data_dir.root())?;
    Ok(NodeId::derived(
        "sinomo.rollout.local-node",
        identity.public_key.as_bytes(),
    ))
}

pub(crate) fn rollout_status() -> Result<(), CliError> {
    let paths = rollout_paths_from_defaults()?;
    let status = rollout_status_view(&paths)?;
    println!("rollout source: {}", status.source_repo.display());
    println!("main binary: {}", status.main_binary.display());
    println!("shadow binary: {}", status.shadow_binary.display());
    println!("state: {}", status.state.display());
    println!("source repo exists: {}", status.source_repo_exists);
    println!("main binary exists: {}", status.main_binary_exists);
    println!("shadow binary exists: {}", status.shadow_binary_exists);
    print_managed_source_status_view(status.source_status);
    Ok(())
}

pub(crate) fn rollout_diagnostics(options: RolloutOptions) -> Result<(), CliError> {
    let targets = if options.targets.is_empty() {
        vec![
            default_main_daemon_status()?
                .map(|status| status.address)
                .unwrap_or_else(|| daemon_connect_address(&options.host, options.port)),
        ]
    } else {
        unique_rollout_targets(options.targets)
    };
    for target in targets {
        println!("== {target} ==");
        match daemon_send_daemon_rollout_diagnostics(&target, CLUSTER_HEALTH_PROBE_TIMEOUT)
            .map_err(CliError::from)
        {
            Ok(response) => print!("{response}"),
            Err(error) => println!("failed: {error}"),
        }
    }
    Ok(())
}

pub(crate) fn rollout_clear_status(options: RolloutOptions) -> Result<(), CliError> {
    if options.targets.is_empty() {
        if let Some(status) = default_main_daemon_status()? {
            match daemon_send_daemon_rollout_clear_status(
                &status.address,
                DAEMON_CLUSTER_KV_TXN_TIMEOUT,
            )
            .map_err(CliError::from)
            {
                Ok(response) => {
                    print!("{response}");
                    return Ok(());
                }
                Err(error) => eprintln!(
                    "warning: could not clear rollout status through local daemon: {error}; clearing local state file directly"
                ),
            }
        }
        let paths = rollout_paths_from_defaults()?;
        if clear_rollout_status(&paths)? {
            println!("cleared rollout status at {}", paths.state.display());
        } else {
            println!("no rollout status to clear at {}", paths.state.display());
        }
        return Ok(());
    }

    for target in unique_rollout_targets(options.targets) {
        match daemon_send_daemon_rollout_clear_status(&target, DAEMON_CLUSTER_KV_TXN_TIMEOUT)
            .map_err(CliError::from)
        {
            Ok(response) => print!("{target}: {response}"),
            Err(error) => println!("{target}: failed: {error}"),
        }
    }
    Ok(())
}

pub(crate) fn print_managed_source_status(paths: &SelfRolloutPaths) -> Result<(), CliError> {
    let status = managed_source_status(paths)?;
    print_managed_source_status_view(status);
    Ok(())
}

pub(crate) fn print_managed_source_status_view(status: ManagedSourceStatus) {
    println!("source git repo: {}", status.is_git_repo);
    if let Some(head) = status.head {
        println!("source head: {head}");
    }
    println!("source external remotes: {}", status.has_external_remotes);
    if !status.remotes.is_empty() {
        println!("source remotes: {}", status.remotes.join(", "));
    }
}

pub(crate) fn rollout_apply(options: RolloutOptions) -> Result<(), CliError> {
    let paths = rollout_paths_from_defaults()?;
    let rollout_plan = paths.plan();
    let artifacts = prepare_local_rollout_artifacts_with_options(
        &paths,
        options.repo.as_deref(),
        rollout_source_deploy_options(&options),
    )?;

    let data_dir = SinomoDataDir::from_env_or_home()?;
    let installed_modules = rollout_install_local_rollout_native_dylib_modules(
        &data_dir.root(),
        &artifacts.native_dylib_modules,
    )?;
    let requested_at = OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "unknown".to_string());
    let plan = daemon_prepare_local_rollout_main_activation(
        &data_dir.root(),
        &paths,
        &artifacts,
        &options.host,
        options.port,
        options.shadow_port,
        requested_at,
        DAEMON_PROBE_TIMEOUT,
    );
    // Do NOT stop the shadow supervisor here. Stopping it leaves the node
    // unsupervised whenever the shadow is the root of the process tree — PID 1
    // in a container, or a dev box with no external init to respawn it — which
    // is the dead-node rollout failure. Instead keep the shadow running and let
    // it drive the handoff: it restarts the main on the new binary, then
    // re-execs its own process image in place to adopt the new shadow binary
    // (see handle_daemon_finalize_shadow_rollout). The node is supervised the
    // entire time. When no shadow is running, stage_rollout_main_activation
    // takes the main-only bootstrap path and brings a fresh supervisor up.
    let activation_strategy =
        stage_rollout_main_activation(&data_dir.root(), &paths, plan.activation)?;
    if let Some(status) = plan.previous_main_status.as_ref() {
        let _ = send_daemon_shutdown_request(&status.address, DAEMON_PROBE_TIMEOUT);
    }
    wait_for_main_daemon_health(
        &options,
        plan.previous_main_status.as_ref().map(|status| status.pid),
    )?;
    if matches!(
        activation_strategy,
        RolloutActivationStrategy::ShadowSupervised
    ) {
        wait_for_local_shadow_rollout_completion(&data_dir, &options)?;
    }
    println!("rollout applied");
    println!("  source: {}", rollout_plan.build_dir.display());
    println!(
        "  main binary: {}",
        rollout_plan.install_main_binary.display()
    );
    println!(
        "  shadow binary: {}",
        rollout_plan.install_shadow_binary.display()
    );
    for module in installed_modules {
        println!(
            "  native module: {} ({})",
            module.state.module_id, module.state.content_hash
        );
    }
    Ok(())
}

pub(crate) fn rollout_cluster(options: RolloutOptions) -> Result<(), CliError> {
    let targets = rollout_cluster_targets(&options)?;
    if targets.is_empty() {
        return Err(CliError::Parse {
            detail: "no rollout targets found; pass --target <host:port> or start a clustered local daemon".to_string(),
        });
    }
    let source = options
        .remote_repo
        .as_ref()
        .or(options.repo.as_ref())
        .or(options.binary.as_ref())
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "remote source repo".to_string());
    let requested_at = OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "unknown".to_string());
    let build_request = prepare_cluster_rollout_build_request(
        source.clone(),
        options.remote_repo.clone(),
        options.force_source_overwrite,
        options.host.clone(),
        options.port,
        options.shadow_port,
        requested_at.clone(),
    );
    let mut binary_request: Option<PreparedClusterBinaryRollout> = None;
    let mut source_bundle: Option<(Vec<u8>, String)> = None;
    let mut desired_source_head = None;
    if options.targets.is_empty() && options.remote_build && options.remote_repo.is_none() {
        let repo = rollout_source_repo_from_options(&options)?;
        desired_source_head = Some(rollout_source_head(&repo)?);
    }
    if options.targets.is_empty()
        && options.remote_build
        && let Err(error) = publish_cluster_rollout_desired(
            &options,
            &source,
            &requested_at,
            desired_source_head.as_deref(),
        )
    {
        eprintln!(
            "warning: cluster rollout desired-state publication failed: {error}; continuing with direct rollout targets"
        );
    }

    let mut completed_rollout_nodes = std::collections::BTreeSet::new();
    for target in targets {
        if target
            .rollout_node_id()
            .is_some_and(|node_id| completed_rollout_nodes.contains(node_id))
        {
            continue;
        }
        let target_status = match read_rollout_target_status(&target) {
            Ok(status) => status,
            Err(error) => {
                println!("{}: failed: {error}", target.label());
                continue;
            }
        };
        let platform_match = rollout_target_platform_matches_local(&target_status);
        let use_remote_build = daemon_cluster_rollout_should_use_remote_build(
            target.label(),
            options.remote_build,
            options.binary.is_some(),
            platform_match,
        );
        if use_remote_build {
            let mut request = build_request.clone();
            if request.remote_repo.is_none() {
                if source_bundle.is_none() {
                    source_bundle = Some(rollout_source_bundle(&options)?);
                }
                if let Some((bundle, head)) = source_bundle.as_ref() {
                    request.source_bundle = Some(bundle.clone());
                    request.source_head = Some(head.clone());
                }
            }
            match send_daemon_rollout_build(&target, &request) {
                Ok(response) => {
                    let source_head_mismatch = response.status == "accepted"
                        && request.source_head.is_some()
                        && !source_heads_match(
                            response.source_head.as_deref(),
                            request.source_head.as_deref().unwrap_or_default(),
                        );
                    if source_head_mismatch {
                        println!("{}: rejected", target.label());
                        println!(
                            "  remote build did not acknowledge requested source head {}; target reported {}",
                            request.source_head.as_deref().unwrap_or("unknown"),
                            response.source_head.as_deref().unwrap_or("unknown")
                        );
                    } else {
                        println!("{}: {}", target.label(), response.status);
                        if response.status == "accepted"
                            && let Some(node_id) = target.rollout_node_id()
                        {
                            completed_rollout_nodes.insert(node_id.clone());
                        }
                    }
                    if !response.detail.is_empty() {
                        println!("  {}", response.detail);
                    }
                }
                Err(error) => {
                    println!("{}: failed: {error}", target.label());
                }
            }
            continue;
        }

        if let Err(error) =
            ensure_rollout_target_platform_from_status(target.label(), &target_status)
        {
            println!("{}: skipped: {error}", target.label());
            continue;
        }
        if binary_request.is_none() {
            let artifacts = rollout_prepare_dev_artifacts(&options)?;
            println!(
                "cluster rollout binary: {}",
                artifacts.built_binary.display()
            );
            for module in &artifacts.native_dylib_modules {
                println!("cluster rollout module: {}", module.built_library.display());
            }
            binary_request = Some(prepare_cluster_binary_rollout(
                source.clone(),
                &artifacts.built_binary,
                &artifacts.native_dylib_modules,
                options.host.clone(),
                options.port,
                options.shadow_port,
                requested_at.clone(),
            )?);
        }
        let request = binary_request.as_ref().expect("binary rollout request");
        match send_prepared_daemon_rollout_binary(&target, request) {
            Ok(response) => {
                println!("{}: {}", target.label(), response.status);
                if response.status == "accepted"
                    && let Some(node_id) = target.rollout_node_id()
                {
                    completed_rollout_nodes.insert(node_id.clone());
                }
                if !response.detail.is_empty() {
                    println!("  {}", response.detail);
                }
            }
            Err(error) => println!("{}: failed: {error}", target.label()),
        }
    }
    Ok(())
}

fn publish_cluster_rollout_desired(
    options: &RolloutOptions,
    source: &str,
    requested_at: &str,
    source_head: Option<&str>,
) -> Result<(), CliError> {
    let paths = cluster_paths_from_defaults()?;
    let state = read_cluster_state(&paths)?;
    let desired = sinomo_runtime::r0::ClusterRolloutDesiredRecord {
        cluster_id: state.cluster_id,
        state: "active".to_string(),
        source: Some(source.to_string()),
        mode: Some("cluster-remote-build".to_string()),
        source_head: source_head.map(str::to_string),
        source_bundle: None,
        force_source_overwrite: options.force_source_overwrite,
        remote_repo: options.remote_repo.clone(),
        host: Some(options.host.clone()),
        port: Some(options.port),
        shadow_port: Some(options.shadow_port),
        requested_at: Some(requested_at.to_string()),
        updated_at: OffsetDateTime::now_utc(),
    };
    let Some(status) = daemon_default_main_daemon_status(DAEMON_PROBE_TIMEOUT)? else {
        return Err(CliError::DaemonNotRunning {
            detail: "cluster rollout desired state requires the local daemon".to_string(),
        });
    };
    let txn = KvTxn {
        compares: Vec::new(),
        success: vec![KvOp::put_serde(
            sinomo_runtime::r0::cluster_rollout_desired_key(),
            &desired,
            None,
        )?],
        failure: Vec::new(),
    };
    let request = network::ObjectRpcRequest::encode_body(
        sinomo_runtime::kv::CLUSTER_KV_TXN_ROUTE,
        sinomo_runtime::kv::CLUSTER_KV_RPC_CATEGORY,
        &txn,
    )
    .map_err(CliError::from)?;
    let response = sinomo_runtime::daemon::send_daemon_rpc(
        &status.address,
        &request,
        DAEMON_CLUSTER_KV_TXN_TIMEOUT,
    )?;
    let _: KvTxnResponse = response.decode_body().map_err(CliError::from)?;
    Ok(())
}

fn rollout_source_deploy_options(options: &RolloutOptions) -> ManagedSourceDeployOptions {
    ManagedSourceDeployOptions {
        force_overwrite: options.force_source_overwrite,
    }
}

pub(crate) fn rollout_source_bundle(
    options: &RolloutOptions,
) -> Result<(Vec<u8>, String), CliError> {
    let repo = rollout_source_repo_from_options(options)?;
    let paths = rollout_paths_from_defaults()?;
    prepare_rollout_source_bundle(&paths, &repo).map_err(Into::into)
}

pub(crate) fn rollout_prepare_dev_artifacts(
    options: &RolloutOptions,
) -> Result<sinomo_runtime::rollout::RolloutBuiltArtifacts, CliError> {
    let repo = rollout_source_repo_from_options(options)?;
    let paths = rollout_paths_from_defaults()?;
    prepare_rollout_dev_artifacts(&paths, &repo, options.binary.as_deref()).map_err(Into::into)
}

pub(crate) fn rollout_source_repo_from_options(
    options: &RolloutOptions,
) -> Result<PathBuf, CliError> {
    options.repo.clone().map(Ok).unwrap_or_else(|| {
        env::current_dir().map_err(|source| CliError::Io {
            path: ".".to_string(),
            detail: source.to_string(),
        })
    })
}

pub(crate) fn rollout_cluster_targets(
    options: &RolloutOptions,
) -> Result<Vec<RolloutTarget>, CliError> {
    let local_target = local_rollout_target(options)?;
    let data_dir = SinomoDataDir::from_env_or_home()?;
    let paths = ClusterPaths::new(&data_dir);
    let state = if options.targets.is_empty() {
        let _ = refresh_cluster_kv_from_local_daemon(&paths);
        require_cluster_bootstrap_state().ok()
    } else {
        None
    };
    if !options.targets.is_empty() {
        let mut targets = Vec::new();
        for address in options.targets.iter().cloned() {
            let target = if address.starts_with("iroh://") {
                RolloutTarget::endpoint(network::invite_introducer_endpoint(
                    &NodeId::from_legacy_named("node_rollout_explicit_target")
                        .expect("static rollout target node id"),
                    address,
                ))
            } else {
                RolloutTarget::address(address)
            };
            push_unique_rollout_target(&mut targets, target);
        }
        return Ok(targets);
    }
    if let Some(state) = state.as_ref() {
        return Ok(rollout_cluster_targets_from_state(
            &paths,
            state,
            local_target,
        ));
    }
    let mut targets = Vec::new();
    if let Some(local_target) = local_target {
        push_unique_rollout_target(&mut targets, local_target);
    }
    Ok(targets)
}

pub(crate) fn rollout_cluster_targets_from_state(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    local_target: Option<RolloutTarget>,
) -> Vec<RolloutTarget> {
    let mut targets = Vec::new();
    if let Some(local_target) = local_target {
        push_unique_rollout_target(&mut targets, local_target);
    }

    let command_endpoints = network::cluster_endpoint_targets(
        paths,
        state,
        network::ClusterEndpointSelectionPurpose::MainDaemonCommand,
    );
    let mut command_endpoints_by_node = std::collections::BTreeMap::new();
    let mut main_node_ids = Vec::new();
    let mut seen_main_node_ids = std::collections::BTreeSet::new();
    for endpoint in command_endpoints {
        if seen_main_node_ids.insert(endpoint.node_id.clone()) {
            main_node_ids.push(endpoint.node_id.clone());
        }
        command_endpoints_by_node
            .entry(endpoint.node_id.clone())
            .or_insert(endpoint);
    }
    for member in state.members.iter().filter(|member| {
        member.node_id != state.local_node_id
            && member.state.is_authorized()
            && member
                .capabilities
                .iter()
                .any(|capability| capability.as_str() == "daemon.main.run")
    }) {
        if seen_main_node_ids.insert(member.node_id.clone()) {
            main_node_ids.push(member.node_id.clone());
        }
    }

    let rollout_endpoints = network::cluster_endpoint_targets(
        paths,
        state,
        network::ClusterEndpointSelectionPurpose::Rollout,
    );
    for main_node_id in main_node_ids {
        let command_endpoint = command_endpoints_by_node.get(&main_node_id);
        let mut target_endpoints =
            network::cluster_rollout_target_endpoints_for_main_with_command_endpoint(
                paths,
                state,
                &main_node_id,
                command_endpoint,
                &rollout_endpoints,
            );
        if target_endpoints.is_empty()
            && let Some(command_endpoint) = command_endpoint
        {
            target_endpoints.push(command_endpoint.clone());
        }
        if let Some(rollout_endpoint) = target_endpoints.into_iter().next() {
            push_unique_rollout_target(
                &mut targets,
                RolloutTarget::endpoint_for_node(rollout_endpoint, main_node_id.clone()),
            );
        }
    }

    targets
}

fn local_rollout_target(options: &RolloutOptions) -> Result<Option<RolloutTarget>, CliError> {
    if !options.targets.is_empty() || options.skip_local {
        return Ok(None);
    }
    if let Some(status) = default_main_daemon_status()? {
        return Ok(Some(RolloutTarget::address(status.address)));
    }
    let shadow_paths = shadow_daemon_paths_from_defaults()?;
    if let DaemonProbeStatus::Running(status) =
        probe_daemon_status(&shadow_paths, DAEMON_PROBE_TIMEOUT)
    {
        return Ok(Some(RolloutTarget::address(status.address)));
    }
    Ok(Some(RolloutTarget::address(daemon_connect_address(
        &options.host,
        options.port,
    ))))
}

#[cfg(test)]
pub(crate) fn rollout_target_endpoint_for_main_with_connectivity(
    paths: &ClusterPaths,
    state: &sinomo_runtime::core::ClusterBootstrapState,
    main_node_id: &NodeId,
    candidates: &[ClusterEndpointRecord],
) -> Option<ClusterEndpointRecord> {
    network::cluster_rollout_target_endpoints_for_main_with_command_endpoint(
        paths,
        state,
        main_node_id,
        None,
        candidates,
    )
    .into_iter()
    .next()
}

fn push_unique_rollout_target(targets: &mut Vec<RolloutTarget>, target: RolloutTarget) {
    if !targets
        .iter()
        .any(|candidate| candidate.address == target.address)
    {
        targets.push(target);
    }
}

const ROLLOUT_PROGRESS_POLL_INTERVAL: Duration = Duration::from_millis(750);
const ROLLOUT_TERMINAL_STATUS_GRACE: Duration = Duration::from_secs(45);
const ROLLOUT_ACTIVE_STATUS_ERROR_GRACE: Duration = Duration::from_secs(15 * 60);
const ROLLOUT_PROGRESS_BAR_WIDTH: usize = 24;

pub(crate) fn rollout_progress_bar(status: Option<&ClusterNodeRolloutStatus>) -> String {
    let Some(status) = status else {
        return format!("[{}]   -", ".".repeat(ROLLOUT_PROGRESS_BAR_WIDTH));
    };
    let Some(total) = status.progress_total.filter(|total| *total > 0) else {
        return format!("[{}]   -", ".".repeat(ROLLOUT_PROGRESS_BAR_WIDTH));
    };
    let current = status.progress_current.unwrap_or_default().min(total);
    let filled = ((current as usize * ROLLOUT_PROGRESS_BAR_WIDTH) / total as usize)
        .min(ROLLOUT_PROGRESS_BAR_WIDTH);
    let empty = ROLLOUT_PROGRESS_BAR_WIDTH.saturating_sub(filled);
    let percent = ((current * 100) / total).min(100);
    format!(
        "[{}{}] {:>3}%",
        "=".repeat(filled),
        ".".repeat(empty),
        percent
    )
}

pub(crate) fn format_rollout_progress_line(
    label: &str,
    operation: &str,
    status: Option<&ClusterNodeRolloutStatus>,
    elapsed: Duration,
) -> String {
    let elapsed_secs = elapsed.as_secs();
    let Some(status) = status else {
        return format!(
            "{label}: {} {operation} - waiting for status ({elapsed_secs}s)",
            rollout_progress_bar(None)
        );
    };
    let phase = status
        .phase
        .as_deref()
        .filter(|phase| !phase.is_empty())
        .unwrap_or(status.status.as_str());
    let message = status
        .progress_message
        .as_deref()
        .or(status.detail.as_deref())
        .filter(|message| !message.is_empty())
        .unwrap_or(operation);
    format!(
        "{label}: {} {phase} - {message} ({elapsed_secs}s)",
        rollout_progress_bar(Some(status))
    )
}

fn write_rollout_progress_line(line: &str, last_len: &mut usize) -> Result<(), CliError> {
    let padding = last_len.saturating_sub(line.len());
    let mut output = format!("\r{line}");
    if padding > 0 {
        output.push_str(&" ".repeat(padding));
    }
    write_cli_stdout_bytes(output.as_bytes()).map_err(CliError::stdio)?;
    *last_len = line.len();
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RolloutProgressExpectation {
    pub(crate) source: String,
    pub(crate) requested_at: String,
    pub(crate) source_head: Option<String>,
}

impl RolloutProgressExpectation {
    fn matches(&self, status: &ClusterNodeRolloutStatus) -> bool {
        status.source.as_deref() == Some(self.source.as_str())
            && status.requested_at.as_deref() == Some(self.requested_at.as_str())
    }
}

pub(crate) fn rollout_status_matches_expectation(
    status: &ClusterNodeRolloutStatus,
    expectation: &RolloutProgressExpectation,
) -> bool {
    expectation.matches(status)
}

pub(crate) fn rollout_terminal_response(
    status: &ClusterNodeRolloutStatus,
    expectation: &RolloutProgressExpectation,
) -> Option<ClusterRolloutBinaryResponse> {
    if !expectation.matches(status) {
        return None;
    }
    let terminal = status.phase.as_deref() == Some("shadow-finalized")
        || matches!(
            status.status.as_str(),
            "failed" | "rejected" | "rolled-back"
        );
    if !terminal {
        return None;
    }
    Some(ClusterRolloutBinaryResponse {
        status: status.status.clone(),
        detail: status
            .detail
            .clone()
            .or_else(|| status.progress_message.clone())
            .unwrap_or_else(|| {
                status
                    .phase
                    .clone()
                    .unwrap_or_else(|| status.status.clone())
            }),
        main_binary: PathBuf::new(),
        shadow_binary: PathBuf::new(),
        source_head: expectation.source_head.clone(),
    })
}

pub(crate) fn rollout_pending_error_grace(
    status: Option<&ClusterNodeRolloutStatus>,
    expectation: &RolloutProgressExpectation,
) -> Duration {
    let Some(status) = status else {
        return ROLLOUT_TERMINAL_STATUS_GRACE;
    };
    if !expectation.matches(status) || rollout_terminal_response(status, expectation).is_some() {
        return ROLLOUT_TERMINAL_STATUS_GRACE;
    }
    ROLLOUT_ACTIVE_STATUS_ERROR_GRACE
}

fn send_rollout_request_with_progress<F>(
    target: &RolloutTarget,
    operation: &'static str,
    expectation: RolloutProgressExpectation,
    send: F,
) -> Result<ClusterRolloutBinaryResponse, CliError>
where
    F: FnOnce(RolloutTarget) -> Result<ClusterRolloutBinaryResponse, CliError> + Send + 'static,
{
    let target_for_send = target.clone();
    let target_for_poll = target.clone();
    let (result_tx, result_rx) = std::sync::mpsc::channel();
    thread::spawn(move || {
        let _ = result_tx.send(send(target_for_send));
    });

    let started = Instant::now();
    let mut last_len = 0usize;
    let mut drew_progress = false;
    let mut pending_error: Option<CliError> = None;
    let mut pending_error_started_at: Option<Instant> = None;
    loop {
        if pending_error.is_none() {
            match result_rx.recv_timeout(ROLLOUT_PROGRESS_POLL_INTERVAL) {
                Ok(Ok(response)) => {
                    if drew_progress {
                        println!();
                    }
                    return Ok(response);
                }
                Ok(Err(error)) => {
                    pending_error = Some(error);
                    pending_error_started_at = Some(Instant::now());
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    pending_error = Some(CliError::Network {
                        address: target.label().to_string(),
                        detail: format!("{operation} worker stopped before returning a result"),
                    });
                    pending_error_started_at = Some(Instant::now());
                }
            }
        } else {
            thread::sleep(ROLLOUT_PROGRESS_POLL_INTERVAL);
        }

        let rollout_status = read_rollout_target_status(&target_for_poll)
            .ok()
            .and_then(|status| status.rollout)
            .filter(|status| rollout_status_matches_expectation(status, &expectation));
        let line = format_rollout_progress_line(
            target.label(),
            operation,
            rollout_status.as_ref(),
            started.elapsed(),
        );
        write_rollout_progress_line(&line, &mut last_len)?;
        drew_progress = true;
        if let Some(response) = rollout_status
            .as_ref()
            .and_then(|status| rollout_terminal_response(status, &expectation))
        {
            println!();
            return Ok(response);
        }
        if pending_error_started_at.is_some_and(|started_at| {
            started_at.elapsed()
                >= rollout_pending_error_grace(rollout_status.as_ref(), &expectation)
        }) {
            if let Some(error) = pending_error {
                if drew_progress {
                    println!();
                }
                return Err(error);
            }
        }
    }
}

pub(crate) fn read_rollout_target_status(target: &RolloutTarget) -> Result<DaemonStatus, CliError> {
    if let Some(endpoint) = &target.endpoint {
        sinomo_runtime::daemon::fetch_daemon_status_from_endpoint(endpoint, DAEMON_JOIN_TIMEOUT)
            .map_err(Into::into)
    } else {
        daemon_fetch_daemon_status_from_address(&target.address, DAEMON_JOIN_TIMEOUT)
            .map_err(Into::into)
    }
}

const ROLLOUT_INLINE_BINARY_MAX_BYTES: u64 = 64 * 1024 * 1024;
const ROLLOUT_UPLOAD_CHUNK_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Debug)]
pub(crate) enum PreparedClusterBinaryRollout {
    Inline(ClusterRolloutBinaryRequest),
    Chunked {
        finalize: ClusterRolloutBinaryUploadFinalizeRequest,
        artifacts: Vec<RolloutUploadSource>,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct RolloutUploadSource {
    artifact_id: String,
    path: PathBuf,
    total_bytes: u64,
}

pub(crate) fn rollout_target_platform_matches_local(status: &DaemonStatus) -> Option<bool> {
    sinomo_runtime::rollout::rollout_target_platform_matches_local(
        status.target_os.as_deref(),
        status.target_arch.as_deref(),
    )
}

pub(crate) fn ensure_rollout_target_platform_from_status(
    address: &str,
    status: &DaemonStatus,
) -> Result<(), CliError> {
    sinomo_runtime::rollout::ensure_rollout_target_platform_matches_local(
        address,
        status.target_os.as_deref(),
        status.target_arch.as_deref(),
    )
    .map_err(Into::into)
}

pub(crate) fn prepare_cluster_binary_rollout(
    source: String,
    binary: &Path,
    native_dylib_modules: &[LocalRolloutNativeDylibModuleArtifact],
    host: String,
    port: u16,
    shadow_port: u16,
    requested_at: String,
) -> Result<PreparedClusterBinaryRollout, CliError> {
    let total_bytes = rollout_artifact_size(binary)?
        + native_dylib_modules
            .iter()
            .map(|module| rollout_artifact_size(&module.built_library))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .sum::<u64>();
    if total_bytes <= ROLLOUT_INLINE_BINARY_MAX_BYTES {
        return Ok(PreparedClusterBinaryRollout::Inline(
            prepare_cluster_rollout_binary_request(
                source,
                binary,
                native_dylib_modules,
                host,
                port,
                shadow_port,
                requested_at,
            )?,
        ));
    }

    let upload_id = format!(
        "rollout-{}-{}",
        std::process::id(),
        OffsetDateTime::now_utc().unix_timestamp_nanos()
    );
    let binary_spec = rollout_upload_artifact_spec("binary", binary)?;
    let mut artifacts = vec![RolloutUploadSource {
        artifact_id: binary_spec.artifact_id.clone(),
        path: binary.to_path_buf(),
        total_bytes: binary_spec.total_bytes,
    }];
    let mut module_specs = Vec::new();
    for (index, module) in native_dylib_modules.iter().enumerate() {
        let artifact_id = format!("module-{index}");
        let artifact = rollout_upload_artifact_spec(&artifact_id, &module.built_library)?;
        artifacts.push(RolloutUploadSource {
            artifact_id: artifact.artifact_id.clone(),
            path: module.built_library.clone(),
            total_bytes: artifact.total_bytes,
        });
        module_specs.push(ClusterRolloutNativeDylibModuleUploadSpec {
            module_id: module.module_id.clone(),
            interface_version: module.interface_version.clone(),
            cache_key: module.cache_key.clone(),
            library_name: module.library_name.clone(),
            source: module.source.clone(),
            artifact,
        });
    }

    Ok(PreparedClusterBinaryRollout::Chunked {
        finalize: ClusterRolloutBinaryUploadFinalizeRequest {
            manifest: ClusterRolloutBinaryUploadManifest {
                upload_id,
                source,
                target_os: std::env::consts::OS.to_string(),
                target_arch: std::env::consts::ARCH.to_string(),
                host,
                port,
                shadow_port,
                requested_at,
                binary: binary_spec,
                native_dylib_modules: module_specs,
            },
        },
        artifacts,
    })
}

fn rollout_artifact_size(path: &Path) -> Result<u64, CliError> {
    fs::metadata(path)
        .map(|metadata| metadata.len())
        .map_err(|source| CliError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })
}

fn rollout_upload_artifact_spec(
    artifact_id: &str,
    path: &Path,
) -> Result<ClusterRolloutUploadArtifactSpec, CliError> {
    Ok(ClusterRolloutUploadArtifactSpec {
        artifact_id: artifact_id.to_string(),
        total_bytes: rollout_artifact_size(path)?,
        blake3: rollout_file_blake3(path)?,
    })
}

fn rollout_file_blake3(path: &Path) -> Result<Vec<u8>, CliError> {
    let mut file = fs::File::open(path).map_err(|source| CliError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(|source| CliError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize().as_bytes().to_vec())
}

pub(crate) fn send_daemon_rollout_binary(
    target: &RolloutTarget,
    request: &ClusterRolloutBinaryRequest,
) -> Result<ClusterRolloutBinaryResponse, CliError> {
    let request = request.clone();
    let expectation = RolloutProgressExpectation {
        source: request.source.clone(),
        requested_at: request.requested_at.clone(),
        source_head: None,
    };
    send_rollout_request_with_progress(target, "binary rollout", expectation, move |target| {
        send_daemon_rollout_binary_direct(&target, &request)
    })
}

pub(crate) fn send_prepared_daemon_rollout_binary(
    target: &RolloutTarget,
    request: &PreparedClusterBinaryRollout,
) -> Result<ClusterRolloutBinaryResponse, CliError> {
    match request {
        PreparedClusterBinaryRollout::Inline(request) => {
            send_daemon_rollout_binary(target, request)
        }
        PreparedClusterBinaryRollout::Chunked {
            finalize,
            artifacts,
        } => send_daemon_rollout_binary_chunked(target, finalize, artifacts),
    }
}

fn send_daemon_rollout_binary_chunked(
    target: &RolloutTarget,
    finalize: &ClusterRolloutBinaryUploadFinalizeRequest,
    artifacts: &[RolloutUploadSource],
) -> Result<ClusterRolloutBinaryResponse, CliError> {
    for artifact in artifacts {
        upload_rollout_artifact_chunks(target, &finalize.manifest.upload_id, artifact)?;
    }
    let finalize = finalize.clone();
    let expectation = RolloutProgressExpectation {
        source: finalize.manifest.source.clone(),
        requested_at: finalize.manifest.requested_at.clone(),
        source_head: None,
    };
    send_rollout_request_with_progress(target, "binary rollout", expectation, move |target| {
        send_daemon_rollout_binary_finalize_direct(&target, &finalize)
    })
}

fn upload_rollout_artifact_chunks(
    target: &RolloutTarget,
    upload_id: &str,
    artifact: &RolloutUploadSource,
) -> Result<(), CliError> {
    let mut file = fs::File::open(&artifact.path).map_err(|source| CliError::Io {
        path: artifact.path.display().to_string(),
        detail: source.to_string(),
    })?;
    let mut offset = 0_u64;
    let mut last_len = 0usize;
    let mut drew_progress = false;
    let mut buffer = vec![0_u8; ROLLOUT_UPLOAD_CHUNK_BYTES];
    while offset < artifact.total_bytes {
        let read = file.read(&mut buffer).map_err(|source| CliError::Io {
            path: artifact.path.display().to_string(),
            detail: source.to_string(),
        })?;
        if read == 0 {
            return Err(CliError::Io {
                path: artifact.path.display().to_string(),
                detail: format!(
                    "unexpected EOF at byte {offset}, expected {}",
                    artifact.total_bytes
                ),
            });
        }
        let chunk = ClusterRolloutBinaryUploadChunkRequest {
            upload_id: upload_id.to_string(),
            artifact_id: artifact.artifact_id.clone(),
            offset,
            total_bytes: artifact.total_bytes,
            bytes: buffer[..read].to_vec(),
        };
        let response = if let Some(endpoint) = &target.endpoint {
            daemon_send_daemon_rollout_binary_upload_chunk_to_endpoint(
                endpoint,
                &chunk,
                DAEMON_ROLLOUT_TIMEOUT,
            )
        } else {
            daemon_send_daemon_rollout_binary_upload_chunk(
                &target.address,
                &chunk,
                DAEMON_ROLLOUT_TIMEOUT,
            )
        }?;
        let expected_received =
            offset + u64::try_from(read).expect("chunk length should fit in u64");
        if !response.accepted || response.received_bytes != expected_received {
            return Err(CliError::Network {
                address: target.label().to_string(),
                detail: format!(
                    "rollout upload for {} stopped at {}, expected {}: {}",
                    artifact.artifact_id,
                    response.received_bytes,
                    expected_received,
                    response.detail
                ),
            });
        }
        offset = expected_received;
        let percent = if artifact.total_bytes == 0 {
            100
        } else {
            ((offset.saturating_mul(100)) / artifact.total_bytes).min(100)
        };
        let line = format!(
            "{}: uploading {} {percent}% ({offset}/{} bytes)",
            target.label(),
            artifact.artifact_id,
            artifact.total_bytes
        );
        write_rollout_progress_line(&line, &mut last_len)?;
        drew_progress = true;
    }
    if drew_progress {
        println!();
    }
    Ok(())
}

fn send_daemon_rollout_binary_direct(
    target: &RolloutTarget,
    request: &ClusterRolloutBinaryRequest,
) -> Result<ClusterRolloutBinaryResponse, CliError> {
    if let Some(endpoint) = &target.endpoint {
        daemon_send_daemon_rollout_binary_to_endpoint(endpoint, request, DAEMON_ROLLOUT_TIMEOUT)
            .map_err(Into::into)
    } else {
        daemon_send_daemon_rollout_binary(&target.address, request, DAEMON_ROLLOUT_TIMEOUT)
            .map_err(Into::into)
    }
}

fn send_daemon_rollout_binary_finalize_direct(
    target: &RolloutTarget,
    request: &ClusterRolloutBinaryUploadFinalizeRequest,
) -> Result<ClusterRolloutBinaryResponse, CliError> {
    if let Some(endpoint) = &target.endpoint {
        daemon_send_daemon_rollout_binary_upload_finalize_to_endpoint(
            endpoint,
            request,
            DAEMON_ROLLOUT_TIMEOUT,
        )
        .map_err(Into::into)
    } else {
        daemon_send_daemon_rollout_binary_upload_finalize(
            &target.address,
            request,
            DAEMON_ROLLOUT_TIMEOUT,
        )
        .map_err(Into::into)
    }
}

pub(crate) fn send_daemon_rollout_native_dylib_module(
    target: &RolloutTarget,
    request: &ClusterRolloutNativeDylibModuleRequest,
) -> Result<ClusterRolloutBinaryResponse, CliError> {
    if let Some(endpoint) = &target.endpoint {
        daemon_send_daemon_rollout_native_dylib_module_to_endpoint(
            endpoint,
            request,
            DAEMON_ROLLOUT_TIMEOUT,
        )
        .map_err(Into::into)
    } else {
        daemon_send_daemon_rollout_native_dylib_module(
            &target.address,
            request,
            DAEMON_ROLLOUT_TIMEOUT,
        )
        .map_err(Into::into)
    }
}

pub(crate) fn send_daemon_rollout_build(
    target: &RolloutTarget,
    request: &ClusterRolloutBuildRequest,
) -> Result<ClusterRolloutBinaryResponse, CliError> {
    let request = request.clone();
    let expectation = RolloutProgressExpectation {
        source: request.source.clone(),
        requested_at: request.requested_at.clone(),
        source_head: request.source_head.clone(),
    };
    send_rollout_request_with_progress(target, "remote build", expectation, move |target| {
        send_daemon_rollout_build_direct(&target, &request)
    })
}

fn send_daemon_rollout_build_direct(
    target: &RolloutTarget,
    request: &ClusterRolloutBuildRequest,
) -> Result<ClusterRolloutBinaryResponse, CliError> {
    if let Some(endpoint) = &target.endpoint {
        daemon_send_daemon_rollout_build_to_endpoint(
            endpoint,
            request,
            remote_build_rollout_timeout(),
        )
        .map_err(Into::into)
    } else {
        daemon_send_daemon_rollout_build(&target.address, request, remote_build_rollout_timeout())
            .map_err(Into::into)
    }
}

fn remote_build_rollout_timeout() -> Duration {
    Duration::from_secs(15 * 60)
}

pub(crate) fn local_rollout_wait_timeout() -> Duration {
    Duration::from_secs(60)
}

pub(crate) fn wait_for_main_daemon_health(
    options: &RolloutOptions,
    previous_pid: Option<u32>,
) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    let status = daemon_wait_for_main_daemon_health(
        &data_dir.root(),
        &options.host,
        options.port,
        previous_pid,
        local_rollout_wait_timeout(),
        DAEMON_PROBE_TIMEOUT,
    )?;
    println!(
        "main daemon healthy: pid={} bind={} control={}",
        status.pid,
        daemon_bind_address(&status),
        status.address
    );
    Ok(())
}

pub(crate) fn wait_for_local_shadow_rollout_completion(
    data_dir: &SinomoDataDir,
    options: &RolloutOptions,
) -> Result<(), CliError> {
    daemon_wait_for_local_shadow_rollout_completion(
        &data_dir.root(),
        &options.host,
        options.port,
        &options.host,
        options.shadow_port,
        local_rollout_wait_timeout(),
        DAEMON_PROBE_TIMEOUT,
    )
    .map_err(Into::into)
}
