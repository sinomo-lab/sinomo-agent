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
    ensure_source_repo(&paths, options.repo.as_deref())?;
    println!("source repo: {}", paths.source_repo.display());
    Ok(())
}

pub(crate) fn rollout_import_source(options: RolloutOptions) -> Result<(), CliError> {
    let paths = rollout_paths_from_defaults()?;
    ensure_rollout_layout(&paths)?;
    ensure_source_repo(&paths, options.repo.as_deref())?;
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
    println!("rollout source: {}", paths.source_repo.display());
    println!("main binary: {}", paths.main_binary.display());
    println!("shadow binary: {}", paths.shadow_binary.display());
    println!("state: {}", paths.state.display());
    println!(
        "source repo exists: {}",
        paths.source_repo.join(".git").exists()
    );
    println!("main binary exists: {}", paths.main_binary.exists());
    println!("shadow binary exists: {}", paths.shadow_binary.exists());
    print_managed_source_status(&paths)?;
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
        let request = network::ObjectRpcRequest::encode_body(
            network::ROLLOUT_DIAGNOSTICS_ROUTE,
            network::ROLLOUT_RPC_CATEGORY,
            &(),
        )?;
        match send_daemon_rpc(&target, &request, CLUSTER_HEALTH_PROBE_TIMEOUT)
            .and_then(|response| response.decode_body::<String>().map_err(Into::into))
        {
            Ok(response) => print!("{response}"),
            Err(error) => println!("failed: {error}"),
        }
    }
    Ok(())
}

pub(crate) fn print_managed_source_status(paths: &SelfRolloutPaths) -> Result<(), CliError> {
    let status = managed_source_status(paths)?;
    println!("source git repo: {}", status.is_git_repo);
    if let Some(head) = status.head {
        println!("source head: {head}");
    }
    println!("source external remotes: {}", status.has_external_remotes);
    if !status.remotes.is_empty() {
        println!("source remotes: {}", status.remotes.join(", "));
    }
    Ok(())
}

pub(crate) fn rollout_apply(options: RolloutOptions) -> Result<(), CliError> {
    let paths = rollout_paths_from_defaults()?;
    ensure_rollout_layout(&paths)?;
    ensure_source_repo(&paths, options.repo.as_deref())?;
    let plan = paths.plan();
    let build = Command::new(&plan.build_program)
        .args(&plan.build_args)
        .current_dir(&plan.build_dir)
        .output()
        .map_err(|source| CliError::Io {
            path: format!("{} {}", plan.build_program, plan.build_args.join(" ")),
            detail: source.to_string(),
        })?;
    let build_log = format!(
        "command: {} {}\nstatus: {}\n\nstdout:\n{}\n\nstderr:\n{}\n",
        plan.build_program,
        plan.build_args.join(" "),
        build.status,
        String::from_utf8_lossy(&build.stdout),
        String::from_utf8_lossy(&build.stderr)
    );
    fs::write(&plan.build_log, build_log).map_err(|source| CliError::Io {
        path: plan.build_log.display().to_string(),
        detail: source.to_string(),
    })?;
    if !build.status.success() {
        return Err(CliError::Process {
            command: format!("{} {}", plan.build_program, plan.build_args.join(" ")),
            status: build.status.code().unwrap_or(-1),
            stderr: String::from_utf8_lossy(&build.stderr).trim().to_string(),
        });
    }

    let data_dir = SinomoDataDir::from_env_or_home()?;
    let main_paths = DaemonPaths::new(&data_dir);
    let previous_main_status = match probe_daemon_status(&main_paths, DAEMON_PROBE_TIMEOUT) {
        DaemonProbeStatus::Running(status) => Some(status),
        DaemonProbeStatus::Locked { status } => status,
        DaemonProbeStatus::NotRunning => None,
    };
    let previous_main_binary = previous_main_status
        .as_ref()
        .and_then(|status| status.executable.clone())
        .unwrap_or_else(|| plan.install_main_binary.clone());
    let staged_main_binary = rollout_staged_main_binary(&paths);
    let staged_shadow_binary = rollout_staged_shadow_binary(&paths);
    stage_rollout_file(&plan.built_binary, &staged_main_binary)?;
    stage_rollout_file(&plan.built_binary, &staged_shadow_binary)?;

    let main_host = previous_main_status
        .as_ref()
        .map(|status| status.host.clone())
        .unwrap_or_else(|| options.host.clone());
    let main_port = previous_main_status
        .as_ref()
        .map(|status| status.port)
        .unwrap_or(options.port);
    restart_local_shadow_for_rollout(
        &plan.built_binary,
        &plan.install_shadow_binary,
        &plan.install_main_binary,
        &data_dir,
        &options,
        &main_host,
        main_port,
    )?;
    stage_rollout_main_activation(
        &data_dir.root(),
        &paths,
        RolloutMainActivation {
            source: paths.source_repo.display().to_string(),
            main_binary: plan.install_main_binary.clone(),
            shadow_binary: plan.install_shadow_binary.clone(),
            staged_main_binary: Some(staged_main_binary),
            staged_shadow_binary: Some(staged_shadow_binary),
            previous_main_binary: Some(previous_main_binary),
            main_host,
            main_port,
            shadow_port: options.shadow_port,
            requested_at: OffsetDateTime::now_utc()
                .format(&Rfc3339)
                .unwrap_or_else(|_| "unknown".to_string()),
            mode: "local-apply".to_string(),
        },
    )?;
    if let Some(status) = previous_main_status.as_ref() {
        let _ = send_daemon_shutdown_request(&status.address, DAEMON_PROBE_TIMEOUT);
    }
    wait_for_main_daemon_health(
        &options,
        previous_main_status.as_ref().map(|status| status.pid),
    )?;
    wait_for_local_shadow_rollout_completion(&data_dir, &options)?;
    println!("rollout applied");
    println!("  source: {}", plan.build_dir.display());
    println!("  main binary: {}", plan.install_main_binary.display());
    println!("  shadow binary: {}", plan.install_shadow_binary.display());
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
    let shadow_paths = DaemonPaths::for_shadow(data_dir);
    let shadow_options = match probe_daemon_status(&shadow_paths, DAEMON_PROBE_TIMEOUT) {
        DaemonProbeStatus::Running(status) => DaemonRunOptions {
            host: status.host,
            port: status.port,
        },
        DaemonProbeStatus::Locked {
            status: Some(status),
        } => DaemonRunOptions {
            host: status.host,
            port: status.port,
        },
        DaemonProbeStatus::Locked { status: None } | DaemonProbeStatus::NotRunning => {
            DaemonRunOptions {
                host: options.host.clone(),
                port: options.shadow_port,
            }
        }
    };
    stop_daemon_before_install(
        &shadow_paths,
        "shadow daemon",
        "local rollout shadow update",
    )?;
    install_binary_atomically(built_binary, shadow_binary)?;
    let args = vec![
        "daemon".to_string(),
        "shadow".to_string(),
        "run".to_string(),
        "--host".to_string(),
        shadow_options.host.clone(),
        "--port".to_string(),
        shadow_options.port.to_string(),
    ];
    let extra_env = vec![
        (SHADOW_MAIN_BINARY_ENV, main_binary.display().to_string()),
        (SHADOW_MAIN_HOST_ENV, main_host.to_string()),
        (SHADOW_MAIN_PORT_ENV, main_port.to_string()),
    ];
    start_installed_sinomo_daemon(
        shadow_binary,
        &data_dir.root(),
        &args,
        &shadow_paths,
        "shadow daemon",
        &extra_env,
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
    let build_request = ClusterRolloutBuildRequest {
        source: source.clone(),
        remote_repo: options.remote_repo.clone(),
        source_bundle_base64: None,
        source_head: None,
        host: options.host.clone(),
        port: options.port,
        shadow_port: options.shadow_port,
        requested_at: requested_at.clone(),
    };
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
        let use_remote_build = options.remote_build
            || (!is_loopback_connect_address(&target)
                && options.binary.is_none()
                && platform_match != Some(true));
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
            let binary_bytes = fs::read(&binary).map_err(|source| CliError::Io {
                path: binary.display().to_string(),
                detail: source.to_string(),
            })?;
            println!("cluster rollout binary: {}", binary.display());
            binary_request = Some(ClusterRolloutBinaryRequest {
                source: source.clone(),
                binary_base64: URL_SAFE_NO_PAD.encode(binary_bytes),
                target_os: env::consts::OS.to_string(),
                target_arch: env::consts::ARCH.to_string(),
                host: options.host.clone(),
                port: options.port,
                shadow_port: options.shadow_port,
                requested_at: requested_at.clone(),
            });
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
    let repo = options
        .repo
        .clone()
        .unwrap_or(env::current_dir().map_err(|source| CliError::Io {
            path: ".".to_string(),
            detail: source.to_string(),
        })?);
    let paths = rollout_paths_from_defaults()?;
    ensure_rollout_layout(&paths)?;
    let bundle = paths
        .root
        .join("staged")
        .join(format!("source-{}.bundle", rollout_artifact_suffix()));
    create_source_repo_bundle(&repo, &bundle)?;
    let bytes = fs::read(&bundle).map_err(|source| CliError::Io {
        path: bundle.display().to_string(),
        detail: source.to_string(),
    })?;
    Ok((URL_SAFE_NO_PAD.encode(bytes), rollout_source_head(&repo)?))
}

pub(crate) fn rollout_source_head(repo: &Path) -> Result<String, CliError> {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(["rev-parse", "--short=8", "HEAD"])
        .output()
        .map_err(|source| CliError::Io {
            path: format!("git -C {} rev-parse --short=8 HEAD", repo.display()),
            detail: source.to_string(),
        })?;
    if !output.status.success() {
        return Err(CliError::Process {
            command: format!("git -C {} rev-parse --short=8 HEAD", repo.display()),
            status: output.status.code().unwrap_or(-1),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        });
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

pub(crate) fn source_heads_match(actual: Option<&str>, expected: &str) -> bool {
    let Some(actual) = actual else {
        return false;
    };
    actual == expected || actual.starts_with(expected) || expected.starts_with(actual)
}

pub(crate) fn rollout_prepare_dev_binary(options: &RolloutOptions) -> Result<PathBuf, CliError> {
    if let Some(binary) = &options.binary {
        if !binary.exists() {
            return Err(CliError::MissingArtifact {
                artifact: binary.display().to_string(),
            });
        }
        return Ok(binary.clone());
    }
    let repo = options
        .repo
        .clone()
        .unwrap_or(env::current_dir().map_err(|source| CliError::Io {
            path: ".".to_string(),
            detail: source.to_string(),
        })?);
    let paths = rollout_paths_from_defaults()?;
    ensure_rollout_layout(&paths)?;
    let plan = SelfRolloutPaths::new(paths.root.parent().ok_or_else(|| CliError::Io {
        path: paths.root.display().to_string(),
        detail: "rollout root has no data directory parent".to_string(),
    })?)
    .plan();
    rollout_build_dev_binary(&repo, &plan.build_log)
}

pub(crate) fn rollout_build_dev_binary(repo: &Path, build_log: &Path) -> Result<PathBuf, CliError> {
    let build = Command::new("cargo")
        .args(["build", "-p", "sinomo"])
        .current_dir(repo)
        .output()
        .map_err(|source| CliError::Io {
            path: "cargo build -p sinomo".to_string(),
            detail: source.to_string(),
        })?;
    let log = format!(
        "command: cargo build -p sinomo\nrepo: {}\nstatus: {}\n\nstdout:\n{}\n\nstderr:\n{}\n",
        repo.display(),
        build.status,
        String::from_utf8_lossy(&build.stdout),
        String::from_utf8_lossy(&build.stderr)
    );
    fs::write(build_log, log).map_err(|source| CliError::Io {
        path: build_log.display().to_string(),
        detail: source.to_string(),
    })?;
    if !build.status.success() {
        return Err(CliError::Process {
            command: "cargo build -p sinomo".to_string(),
            status: build.status.code().unwrap_or(-1),
            stderr: String::from_utf8_lossy(&build.stderr).trim().to_string(),
        });
    }
    Ok(repo
        .join("target")
        .join("debug")
        .join(format!("sinomo{}", env::consts::EXE_SUFFIX)))
}

pub(crate) fn rollout_remote_repo_path(paths: &SelfRolloutPaths, repo: Option<&Path>) -> PathBuf {
    match repo {
        Some(repo) if repo.is_absolute() => repo.to_path_buf(),
        Some(repo) => paths.root.parent().unwrap_or(&paths.root).join(repo),
        None => paths.source_repo.clone(),
    }
}

pub(crate) fn rollout_cluster_targets(options: &RolloutOptions) -> Result<Vec<String>, CliError> {
    if !options.targets.is_empty() {
        return Ok(unique_rollout_targets(options.targets.clone()));
    }

    let mut targets = Vec::new();
    if !options.skip_local {
        if let Some(status) = default_main_daemon_status()? {
            targets.push(status.address);
        } else {
            targets.push(daemon_connect_address(&options.host, options.port));
        }
    }
    targets.extend(options.targets.iter().cloned());

    if let Ok(state) = require_cluster_bootstrap_state() {
        for member in state.members {
            let is_local_member = member.node_id == state.local_node_id;
            if is_local_member {
                continue;
            }
            let is_main_daemon = member
                .capabilities
                .iter()
                .any(|capability| capability.as_str() == "daemon.main.run");
            if !is_main_daemon {
                continue;
            }
            for endpoint in member.endpoints {
                if !is_local_member && matches!(endpoint.privacy, ClusterEndpointPrivacy::LocalOnly)
                {
                    continue;
                }
                targets.push(endpoint.address);
            }
        }
    }

    Ok(unique_rollout_targets(targets))
}

pub(crate) fn unique_rollout_targets(targets: Vec<String>) -> Vec<String> {
    let mut unique = Vec::new();
    for target in targets {
        if !unique.contains(&target) {
            unique.push(target);
        }
    }
    unique
}

pub(crate) fn read_rollout_target_status(address: &str) -> Result<DaemonStatus, CliError> {
    let request = network::ObjectRpcRequest::encode_body(
        network::DAEMON_STATUS_ROUTE,
        network::DAEMON_RPC_CATEGORY,
        &(),
    )?;
    let response = send_daemon_rpc(address, &request, DAEMON_JOIN_TIMEOUT)?;
    response.decode_body().map_err(CliError::from)
}

pub(crate) fn rollout_target_platform_matches_local(status: &DaemonStatus) -> Option<bool> {
    match (&status.target_os, &status.target_arch) {
        (Some(target_os), Some(target_arch)) => {
            Some(target_os == env::consts::OS && target_arch == env::consts::ARCH)
        }
        _ => None,
    }
}

pub(crate) fn ensure_rollout_target_platform_from_status(
    address: &str,
    status: &DaemonStatus,
) -> Result<(), CliError> {
    let local = is_loopback_connect_address(address);
    match (&status.target_os, &status.target_arch) {
        (Some(target_os), Some(target_arch))
            if target_os == env::consts::OS && target_arch == env::consts::ARCH =>
        {
            Ok(())
        }
        (Some(target_os), Some(target_arch)) => Err(CliError::Parse {
            detail: format!(
                "target platform {target_os}/{target_arch} does not match local artifact platform {}/{}",
                env::consts::OS,
                env::consts::ARCH
            ),
        }),
        _ if local => Ok(()),
        _ => Err(CliError::Parse {
            detail:
                "target daemon does not report platform metadata; update it locally before binary rollout"
                    .to_string(),
        }),
    }
}

pub(crate) fn send_daemon_rollout_binary(
    address: &str,
    request: &ClusterRolloutBinaryRequest,
) -> Result<ClusterRolloutBinaryResponse, CliError> {
    let request = network::ObjectRpcRequest::encode_body(
        network::ROLLOUT_BINARY_ROUTE,
        network::ROLLOUT_RPC_CATEGORY,
        request,
    )?;
    let response = send_daemon_rpc(address, &request, DAEMON_ROLLOUT_TIMEOUT)?;
    response.decode_body().map_err(CliError::from)
}

pub(crate) fn send_daemon_rollout_build(
    address: &str,
    request: &ClusterRolloutBuildRequest,
) -> Result<ClusterRolloutBinaryResponse, CliError> {
    let request = network::ObjectRpcRequest::encode_body(
        network::ROLLOUT_BUILD_ROUTE,
        network::ROLLOUT_RPC_CATEGORY,
        request,
    )?;
    let response = send_daemon_rpc(address, &request, DAEMON_ROLLOUT_TIMEOUT)?;
    response.decode_body().map_err(CliError::from)
}

pub(crate) fn ensure_source_repo(
    paths: &SelfRolloutPaths,
    repo: Option<&Path>,
) -> Result<(), CliError> {
    let source = match repo {
        Some(repo) => repo.to_path_buf(),
        None => env::current_dir().map_err(|source| CliError::Io {
            path: ".".to_string(),
            detail: source.to_string(),
        })?,
    };
    Ok(deploy_managed_source_repo(paths, &source)?)
}

pub(crate) fn install_binary_atomically(source: &Path, destination: &Path) -> Result<(), CliError> {
    if !source.exists() {
        return Err(CliError::MissingArtifact {
            artifact: source.display().to_string(),
        });
    }
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent).map_err(|source| CliError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let next = destination.with_extension("next");
    fs::copy(source, &next).map_err(|error| CliError::Io {
        path: format!("{} -> {}", source.display(), next.display()),
        detail: error.to_string(),
    })?;
    if destination.exists() {
        let previous = destination.with_extension("previous");
        let _ = fs::remove_file(&previous);
        fs::rename(destination, &previous).map_err(|source| CliError::Io {
            path: format!("{} -> {}", destination.display(), previous.display()),
            detail: source.to_string(),
        })?;
    }
    fs::rename(&next, destination).map_err(|source| CliError::Io {
        path: format!("{} -> {}", next.display(), destination.display()),
        detail: source.to_string(),
    })?;
    Ok(())
}

pub(crate) fn wait_for_main_daemon_health(
    options: &RolloutOptions,
    previous_pid: Option<u32>,
) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    let paths = DaemonPaths::new(&data_dir);
    let deadline = Instant::now() + Duration::from_secs(90);
    while Instant::now() < deadline {
        if let DaemonProbeStatus::Running(status) =
            probe_daemon_status(&paths, DAEMON_PROBE_TIMEOUT)
        {
            if previous_pid.is_none_or(|pid| status.pid != pid) {
                println!(
                    "main daemon healthy: pid={} bind={} control={}",
                    status.pid,
                    daemon_bind_address(&status),
                    status.address
                );
                return Ok(());
            }
        }
        thread::sleep(Duration::from_millis(100));
    }
    Err(CliError::Network {
        address: daemon_connect_address(&options.host, options.port),
        detail: "rolled out main daemon did not become ready under shadow supervision".to_string(),
    })
}

pub(crate) fn wait_for_local_shadow_rollout_completion(
    data_dir: &SinomoDataDir,
    options: &RolloutOptions,
) -> Result<(), CliError> {
    let rollout_paths = SelfRolloutPaths::new(&data_dir.root());
    let shadow_paths = DaemonPaths::for_shadow(data_dir);
    let main_paths = DaemonPaths::new(data_dir);
    let deadline = Instant::now() + Duration::from_secs(90);
    let mut next_restart_nudge = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        let pending_main = rollout_main_activation_path(&rollout_paths).exists();
        let pending_shadow = rollout_shadow_finalize_path(&rollout_paths).exists();
        if !pending_main
            && !pending_shadow
            && matches!(
                probe_daemon_status(&shadow_paths, DAEMON_PROBE_TIMEOUT),
                DaemonProbeStatus::Running(_)
            )
        {
            return Ok(());
        }
        if pending_main && Instant::now() >= next_restart_nudge {
            if let DaemonProbeStatus::Running(status) =
                probe_daemon_status(&main_paths, DAEMON_PROBE_TIMEOUT)
            {
                let _ = send_daemon_shutdown_request(&status.address, DAEMON_PROBE_TIMEOUT);
            }
            next_restart_nudge = Instant::now() + Duration::from_secs(2);
        }
        thread::sleep(Duration::from_millis(100));
    }
    Err(CliError::Network {
        address: daemon_connect_address(&options.host, options.shadow_port),
        detail: "rolled out shadow daemon did not complete local rollout finalization".to_string(),
    })
}
