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
        match send_daemon_command(&target, "ROLLOUT_DIAGNOSTICS", CLUSTER_HEALTH_PROBE_TIMEOUT) {
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
    install_binary_atomically(&plan.built_binary, &plan.install_main_binary)?;
    stop_main_daemon_if_running(&options)?;
    install_binary_atomically(&plan.built_binary, &plan.install_shadow_binary)?;
    restart_shadow_daemon_from_binary(
        &plan.install_shadow_binary,
        &plan.install_main_binary,
        &options,
    )?;
    wait_for_main_daemon_health(&options)?;
    write_json(
        &plan.state,
        &serde_json::json!({
            "source_repo": paths.source_repo,
            "main_binary": plan.install_main_binary,
            "shadow_binary": plan.install_shadow_binary,
            "built_binary": plan.built_binary,
            "build_log": plan.build_log,
            "updated_at": OffsetDateTime::now_utc().format(&Rfc3339).unwrap_or_else(|_| "unknown".to_string())
        }),
    )?;
    println!("rollout applied");
    println!("  source: {}", plan.build_dir.display());
    println!("  main binary: {}", plan.install_main_binary.display());
    println!("  shadow binary: {}", plan.install_shadow_binary.display());
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
    let build_request = ClusterRolloutBuildRequest {
        source: source.clone(),
        remote_repo: options.remote_repo.clone(),
        host: options.host.clone(),
        port: options.port,
        shadow_port: options.shadow_port,
        requested_at: requested_at.clone(),
    };
    let mut binary_request: Option<ClusterRolloutBinaryRequest> = None;

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
            match send_daemon_rollout_build(&target, &build_request) {
                Ok(response) => {
                    println!("{target}: {}", response.status);
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
    let response = send_daemon_command(address, "STATUS", DAEMON_JOIN_TIMEOUT)?;
    serde_json::from_str(response.trim()).map_err(|source| CliError::Json {
        path: format!("daemon status from {address}"),
        detail: source.to_string(),
    })
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
    let payload = serde_json::to_string(request).map_err(|source| CliError::Json {
        path: "cluster rollout request".to_string(),
        detail: source.to_string(),
    })?;
    let response = send_daemon_command(
        address,
        &format!("ROLLOUT_BINARY {payload}"),
        DAEMON_ROLLOUT_TIMEOUT,
    )?;
    serde_json::from_str(response.trim()).map_err(|source| CliError::Json {
        path: format!("rollout response from {address}"),
        detail: source.to_string(),
    })
}

pub(crate) fn send_daemon_rollout_build(
    address: &str,
    request: &ClusterRolloutBuildRequest,
) -> Result<ClusterRolloutBinaryResponse, CliError> {
    let payload = serde_json::to_string(request).map_err(|source| CliError::Json {
        path: "cluster rollout build request".to_string(),
        detail: source.to_string(),
    })?;
    let response = send_daemon_command(
        address,
        &format!("ROLLOUT_BUILD {payload}"),
        DAEMON_ROLLOUT_TIMEOUT,
    )?;
    serde_json::from_str(response.trim()).map_err(|source| CliError::Json {
        path: format!("rollout response from {address}"),
        detail: source.to_string(),
    })
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

pub(crate) fn stop_main_daemon_if_running(_options: &RolloutOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    let paths = DaemonPaths::new(&data_dir);
    if let DaemonProbeStatus::Running(status) = probe_daemon_status(&paths, DAEMON_PROBE_TIMEOUT) {
        let _ = send_daemon_command(&status.address, "SHUTDOWN", DAEMON_PROBE_TIMEOUT);
        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline {
            if matches!(
                probe_daemon_status(&paths, DAEMON_PROBE_TIMEOUT),
                DaemonProbeStatus::NotRunning
            ) {
                return Ok(());
            }
            thread::sleep(Duration::from_millis(100));
        }
    }
    Ok(())
}

pub(crate) fn restart_shadow_daemon_from_binary(
    binary: &Path,
    main_binary: &Path,
    options: &RolloutOptions,
) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    let paths = DaemonPaths::for_shadow(&data_dir);
    if let DaemonProbeStatus::Running(status) = probe_daemon_status(&paths, DAEMON_PROBE_TIMEOUT) {
        let _ = send_daemon_command(&status.address, "SHUTDOWN", DAEMON_PROBE_TIMEOUT);
        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline {
            if matches!(
                probe_daemon_status(&paths, DAEMON_PROBE_TIMEOUT),
                DaemonProbeStatus::NotRunning
            ) {
                break;
            }
            thread::sleep(Duration::from_millis(100));
        }
    }
    start_daemon_binary(
        binary,
        main_binary,
        &["daemon", "shadow", "run"],
        options,
        paths,
        "shadow daemon",
    )
}

pub(crate) fn start_daemon_binary(
    binary: &Path,
    main_binary: &Path,
    command_prefix: &[&str],
    options: &RolloutOptions,
    paths: DaemonPaths,
    label: &str,
) -> Result<(), CliError> {
    let mut command = Command::new(binary);
    command.args(command_prefix);
    command.env(SHADOW_MAIN_BINARY_ENV, main_binary);
    command.env(SHADOW_MAIN_HOST_ENV, &options.host);
    command.env(SHADOW_MAIN_PORT_ENV, options.port.to_string());
    let child = command
        .arg("--host")
        .arg(&options.host)
        .arg("--port")
        .arg(SINOMO_SHADOW_DAEMON_DEFAULT_PORT.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|source| CliError::Io {
            path: binary.display().to_string(),
            detail: source.to_string(),
        })?;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if let DaemonProbeStatus::Running(status) =
            probe_daemon_status(&paths, DAEMON_PROBE_TIMEOUT)
        {
            println!(
                "{label} healthy: pid={} bind={} control={}",
                status.pid,
                daemon_bind_address(&status),
                status.address
            );
            return Ok(());
        }
        thread::sleep(Duration::from_millis(100));
    }
    Err(CliError::Network {
        address: daemon_connect_address(&options.host, SINOMO_SHADOW_DAEMON_DEFAULT_PORT),
        detail: format!(
            "rolled out {label} process {} did not become ready",
            child.id()
        ),
    })
}

pub(crate) fn wait_for_main_daemon_health(options: &RolloutOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    let paths = DaemonPaths::new(&data_dir);
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if let DaemonProbeStatus::Running(status) =
            probe_daemon_status(&paths, DAEMON_PROBE_TIMEOUT)
        {
            println!(
                "main daemon healthy: pid={} bind={} control={}",
                status.pid,
                daemon_bind_address(&status),
                status.address
            );
            return Ok(());
        }
        thread::sleep(Duration::from_millis(100));
    }
    Err(CliError::Network {
        address: daemon_connect_address(&options.host, options.port),
        detail: "rolled out main daemon did not become ready under shadow supervision".to_string(),
    })
}
