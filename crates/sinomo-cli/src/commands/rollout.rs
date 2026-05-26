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
    Diagnostics(RolloutOptions),
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
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct RolloutTarget {
    pub(crate) address: String,
    pub(crate) endpoint: Option<ClusterEndpointRecord>,
}

impl RolloutTarget {
    pub(crate) fn address(address: String) -> Self {
        Self {
            address,
            endpoint: None,
        }
    }

    pub(crate) fn endpoint(endpoint: ClusterEndpointRecord) -> Self {
        Self {
            address: endpoint.address.clone(),
            endpoint: Some(endpoint),
        }
    }

    pub(crate) fn label(&self) -> &str {
        &self.address
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
        Some((command, rest)) if matches!(command.as_str(), "diagnostics" | "diag") => {
            Ok(RolloutCliCommand::Diagnostics(parse_rollout_options(rest)?))
        }
        Some((command, [])) if command == "status" => Ok(RolloutCliCommand::Status),
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
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }
    Ok(options)
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
        RolloutCliCommand::Diagnostics(options) => rollout_diagnostics(options),
        RolloutCliCommand::Status => rollout_status(),
    }
}

pub(crate) fn rollout_paths_from_defaults() -> Result<SelfRolloutPaths, CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    Ok(SelfRolloutPaths::new(&data_dir.root()))
}

pub(crate) fn rollout_init_source(options: RolloutOptions) -> Result<(), CliError> {
    let paths = rollout_paths_from_defaults()?;
    ensure_rollout_layout(&paths)?;
    ensure_rollout_source_repo(&paths, options.repo.as_deref())?;
    println!("source repo: {}", paths.source_repo.display());
    Ok(())
}

pub(crate) fn rollout_import_source(options: RolloutOptions) -> Result<(), CliError> {
    let paths = rollout_paths_from_defaults()?;
    ensure_rollout_layout(&paths)?;
    ensure_rollout_source_repo(&paths, options.repo.as_deref())?;
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
    let artifacts = prepare_local_rollout_artifacts(&paths, options.repo.as_deref())?;

    let data_dir = SinomoDataDir::from_env_or_home()?;
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
    restart_local_shadow_for_rollout(
        &artifacts.built_binary,
        &plan.activation.shadow_binary,
        &plan.activation.main_binary,
        &data_dir,
        &options,
        &plan.activation.main_host,
        plan.activation.main_port,
    )?;
    stage_rollout_main_activation(&data_dir.root(), &paths, plan.activation)?;
    if let Some(status) = plan.previous_main_status.as_ref() {
        let _ = send_daemon_shutdown_request(&status.address, DAEMON_PROBE_TIMEOUT);
    }
    wait_for_main_daemon_health(
        &options,
        plan.previous_main_status.as_ref().map(|status| status.pid),
    )?;
    wait_for_local_shadow_rollout_completion(&data_dir, &options)?;
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
    Ok(())
}

pub(crate) fn restart_local_shadow_for_rollout(
    built_binary: &Path,
    shadow_binary: &Path,
    main_binary: &Path,
    data_dir: &SinomoDataDir,
    options: &RolloutOptions,
    main_host: &str,
    main_port: u16,
) -> Result<(), CliError> {
    let restart = daemon_prepare_local_shadow_rollout_restart(
        &data_dir.root(),
        main_binary,
        main_host,
        main_port,
        &options.host,
        options.shadow_port,
        DAEMON_PROBE_TIMEOUT,
    );
    stop_daemon_before_install(
        &restart.shadow_paths,
        "shadow daemon",
        "local rollout shadow update",
    )?;
    install_binary_atomically(built_binary, shadow_binary)?;
    start_installed_sinomo_daemon(
        shadow_binary,
        &data_dir.root(),
        &restart.args,
        &restart.shadow_paths,
        "shadow daemon",
        &restart.extra_env,
    )
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
        options.host.clone(),
        options.port,
        options.shadow_port,
        requested_at.clone(),
    );
    let mut binary_request: Option<ClusterRolloutBinaryRequest> = None;
    let mut source_bundle: Option<(String, String)> = None;
    if options.targets.is_empty() && options.remote_build && options.remote_repo.is_none() {
        source_bundle = Some(rollout_source_bundle_base64(&options)?);
    }
    if options.targets.is_empty() && options.remote_build {
        publish_cluster_rollout_desired(&options, &source, &requested_at, source_bundle.as_ref())?;
    }

    for target in targets {
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
                    source_bundle = Some(rollout_source_bundle_base64(&options)?);
                }
                if let Some((bundle, head)) = source_bundle.as_ref() {
                    request.source_bundle_base64 = Some(bundle.clone());
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
            let binary = rollout_prepare_dev_binary(&options)?;
            println!("cluster rollout binary: {}", binary.display());
            binary_request = Some(prepare_cluster_rollout_binary_request(
                source.clone(),
                &binary,
                options.host.clone(),
                options.port,
                options.shadow_port,
                requested_at.clone(),
            )?);
        }
        let request = binary_request.as_ref().expect("binary rollout request");
        match send_daemon_rollout_binary(&target, request) {
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

fn publish_cluster_rollout_desired(
    options: &RolloutOptions,
    source: &str,
    requested_at: &str,
    source_bundle: Option<&(String, String)>,
) -> Result<(), CliError> {
    let paths = cluster_paths_from_defaults()?;
    let state = read_cluster_state(&paths)?;
    let desired = network::ClusterRolloutDesiredRecord {
        cluster_id: state.cluster_id,
        state: "active".to_string(),
        source: Some(source.to_string()),
        mode: Some("cluster-remote-build".to_string()),
        source_head: source_bundle.map(|(_, head)| head.clone()),
        source_bundle_base64: source_bundle.map(|(bundle, _)| bundle.clone()),
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
            network::cluster_rollout_desired_key(),
            &desired,
            None,
        )?],
        failure: Vec::new(),
    };
    let request = network::ObjectRpcRequest::encode_body(
        network::CLUSTER_KV_TXN_ROUTE,
        network::CLUSTER_KV_RPC_CATEGORY,
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

pub(crate) fn rollout_source_bundle_base64(
    options: &RolloutOptions,
) -> Result<(String, String), CliError> {
    let repo = rollout_source_repo_from_options(options)?;
    let paths = rollout_paths_from_defaults()?;
    prepare_rollout_source_bundle_base64(&paths, &repo).map_err(Into::into)
}

pub(crate) fn rollout_prepare_dev_binary(options: &RolloutOptions) -> Result<PathBuf, CliError> {
    let repo = rollout_source_repo_from_options(options)?;
    let paths = rollout_paths_from_defaults()?;
    prepare_rollout_dev_binary(&paths, &repo, options.binary.as_deref()).map_err(Into::into)
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
    let local_target = if options.targets.is_empty() && !options.skip_local {
        if let Some(status) = default_main_daemon_status()? {
            Some(status.address)
        } else {
            Some(daemon_connect_address(&options.host, options.port))
        }
    } else {
        None
    };
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
        let mut targets = Vec::new();
        if let Some(local_target) = local_target {
            push_unique_rollout_target(&mut targets, RolloutTarget::address(local_target));
        }
        let rollout_endpoints = network::cluster_rollout_endpoint_candidates(&paths, state);
        for endpoint in network::cluster_main_daemon_command_targets(&paths, state) {
            let rollout_endpoint = network::cluster_rollout_target_endpoint_for_main(
                &paths,
                state,
                &endpoint.node_id,
                &rollout_endpoints,
            )
            .unwrap_or_else(|| endpoint.clone());
            push_unique_rollout_target(&mut targets, RolloutTarget::endpoint(rollout_endpoint));
        }
        return Ok(targets);
    }
    Ok(
        prepare_rollout_cluster_targets(Vec::new(), local_target, None)
            .into_iter()
            .map(RolloutTarget::address)
            .fold(Vec::new(), |mut targets, target| {
                push_unique_rollout_target(&mut targets, target);
                targets
            }),
    )
}

#[cfg(test)]
pub(crate) fn rollout_endpoint_candidates(
    state: &sinomo_runtime::core::ClusterBootstrapState,
) -> Vec<ClusterEndpointRecord> {
    let mut endpoints = Vec::new();
    for member in &state.members {
        if member.node_id == state.local_node_id || !member.state.is_authorized() {
            continue;
        }
        endpoints.extend(
            member
                .endpoints
                .iter()
                .filter(|endpoint| {
                    endpoint.privacy != sinomo_runtime::core::ClusterEndpointPrivacy::LocalOnly
                        && matches!(
                            endpoint.transport,
                            sinomo_runtime::core::ClusterTransportKind::LanTcp
                                | sinomo_runtime::core::ClusterTransportKind::Loopback
                                | sinomo_runtime::core::ClusterTransportKind::Iroh
                        )
                })
                .cloned(),
        );
    }
    endpoints
}

#[cfg(test)]
pub(crate) fn extend_rollout_endpoints_from_kv(
    paths: &ClusterPaths,
    endpoints: &mut Vec<ClusterEndpointRecord>,
) {
    let Ok(kv_endpoints) = network::read_cluster_endpoints_from_kv(paths) else {
        return;
    };
    for endpoint in kv_endpoints {
        if !endpoints
            .iter()
            .any(|candidate| candidate.endpoint_id == endpoint.endpoint_id)
        {
            endpoints.push(endpoint);
        }
    }
}

#[cfg(test)]
pub(crate) fn rollout_target_endpoint_for_main(
    main_node_id: &NodeId,
    candidates: &[ClusterEndpointRecord],
) -> Option<ClusterEndpointRecord> {
    let shadow_node_id = shadow_daemon_node_id(main_node_id);
    let mut candidates = candidates
        .iter()
        .filter(|endpoint| {
            endpoint.node_id == shadow_node_id
                && endpoint.privacy != sinomo_runtime::core::ClusterEndpointPrivacy::LocalOnly
        })
        .cloned()
        .collect::<Vec<_>>();
    candidates.sort_by_key(|endpoint| (endpoint.priority, endpoint.address.clone()));
    candidates.into_iter().next()
}

#[cfg(test)]
pub(crate) fn rollout_target_endpoint_for_main_in_order(
    main_node_id: &NodeId,
    candidates: &[ClusterEndpointRecord],
) -> Option<ClusterEndpointRecord> {
    let shadow_node_id = shadow_daemon_node_id(main_node_id);
    candidates
        .iter()
        .find(|endpoint| {
            endpoint.node_id == shadow_node_id
                && endpoint.privacy != sinomo_runtime::core::ClusterEndpointPrivacy::LocalOnly
        })
        .cloned()
}

#[cfg(test)]
pub(crate) fn rollout_target_endpoint_for_main_with_connectivity(
    paths: &ClusterPaths,
    state: &sinomo_runtime::core::ClusterBootstrapState,
    main_node_id: &NodeId,
    candidates: &[ClusterEndpointRecord],
) -> Option<ClusterEndpointRecord> {
    network::cluster_rollout_target_endpoint_for_main(paths, state, main_node_id, candidates)
}

#[cfg(test)]
pub(crate) fn sort_rollout_endpoints_by_connectivity(
    paths: &ClusterPaths,
    state: &sinomo_runtime::core::ClusterBootstrapState,
    endpoints: &mut [ClusterEndpointRecord],
) {
    network::sort_cluster_rollout_endpoints(paths, state, endpoints);
}

fn push_unique_rollout_target(targets: &mut Vec<RolloutTarget>, target: RolloutTarget) {
    if !targets
        .iter()
        .any(|candidate| candidate.address == target.address)
    {
        targets.push(target);
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

pub(crate) fn rollout_target_platform_matches_local(status: &DaemonStatus) -> Option<bool> {
    daemon_rollout_target_platform_matches_local(status)
}

pub(crate) fn ensure_rollout_target_platform_from_status(
    address: &str,
    status: &DaemonStatus,
) -> Result<(), CliError> {
    daemon_ensure_rollout_target_platform_matches_local(address, status).map_err(Into::into)
}

pub(crate) fn send_daemon_rollout_binary(
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

pub(crate) fn send_daemon_rollout_build(
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
