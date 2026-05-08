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

    for target in targets {
        let target_status = match read_rollout_target_status(&target) {
            Ok(status) => status,
            Err(error) => {
                println!("{target}: failed: {error}");
                continue;
            }
        };
        let platform_match = rollout_target_platform_matches_local(&target_status);
        let use_remote_build = daemon_cluster_rollout_should_use_remote_build(
            &target,
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
                        println!("{target}: rejected");
                        println!(
                            "  remote build did not acknowledge requested source head {}; target reported {}",
                            request.source_head.as_deref().unwrap_or("unknown"),
                            response.source_head.as_deref().unwrap_or("unknown")
                        );
                    } else {
                        println!("{target}: {}", response.status);
                    }
                    if !response.detail.is_empty() {
                        println!("  {}", response.detail);
                    }
                }
                Err(error) => {
                    println!("{target}: failed: {error}");
                }
            }
            continue;
        }

        if let Err(error) = ensure_rollout_target_platform_from_status(&target, &target_status) {
            println!("{target}: skipped: {error}");
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
                println!("{target}: {}", response.status);
                if !response.detail.is_empty() {
                    println!("  {}", response.detail);
                }
            }
            Err(error) => println!("{target}: failed: {error}"),
        }
    }
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

pub(crate) fn rollout_cluster_targets(options: &RolloutOptions) -> Result<Vec<String>, CliError> {
    let local_target = if options.targets.is_empty() && !options.skip_local {
        if let Some(status) = default_main_daemon_status()? {
            Some(status.address)
        } else {
            Some(daemon_connect_address(&options.host, options.port))
        }
    } else {
        None
    };
    let state = if options.targets.is_empty() {
        require_cluster_bootstrap_state().ok()
    } else {
        None
    };
    Ok(prepare_rollout_cluster_targets(
        options.targets.clone(),
        local_target,
        state.as_ref(),
    ))
}

pub(crate) fn read_rollout_target_status(address: &str) -> Result<DaemonStatus, CliError> {
    daemon_fetch_daemon_status_from_address(address, DAEMON_JOIN_TIMEOUT).map_err(Into::into)
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
    address: &str,
    request: &ClusterRolloutBinaryRequest,
) -> Result<ClusterRolloutBinaryResponse, CliError> {
    daemon_send_daemon_rollout_binary(address, request, DAEMON_ROLLOUT_TIMEOUT).map_err(Into::into)
}

pub(crate) fn send_daemon_rollout_build(
    address: &str,
    request: &ClusterRolloutBuildRequest,
) -> Result<ClusterRolloutBinaryResponse, CliError> {
    daemon_send_daemon_rollout_build(address, request, DAEMON_ROLLOUT_TIMEOUT).map_err(Into::into)
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
        Duration::from_secs(10),
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
        Duration::from_secs(10),
        DAEMON_PROBE_TIMEOUT,
    )
    .map_err(Into::into)
}
