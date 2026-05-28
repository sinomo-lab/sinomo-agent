use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DaemonCliCommand {
    Help,
    Run(DaemonRunOptions),
    Start(DaemonRunOptions),
    Status(DaemonRunOptions),
    Stop(DaemonRunOptions),
    ShadowRun(DaemonRunOptions),
    ShadowStart(DaemonRunOptions),
    ShadowStatus(DaemonRunOptions),
    ShadowStop(DaemonRunOptions),
}

pub(crate) fn print_daemon_platform(status: &DaemonStatus) {
    if let (Some(target_os), Some(target_arch)) = (&status.target_os, &status.target_arch) {
        println!("  platform: {target_os}/{target_arch}");
    }
}

pub(crate) fn parse_daemon_command(args: &[String]) -> Result<DaemonCliCommand, CliError> {
    match args.split_first() {
        None => Ok(DaemonCliCommand::Help),
        Some((command, [])) if matches!(command.as_str(), "help" | "--help" | "-h") => {
            Ok(DaemonCliCommand::Help)
        }
        Some((command, rest)) if command == "shadow" => parse_shadow_daemon_command(rest),
        Some((command, rest)) if command == "run" => {
            Ok(DaemonCliCommand::Run(parse_daemon_run_options(rest)?))
        }
        Some((command, rest)) if command == "start" => {
            Ok(DaemonCliCommand::Start(parse_daemon_run_options(rest)?))
        }
        Some((command, rest)) if command == "status" => {
            Ok(DaemonCliCommand::Status(parse_daemon_run_options(rest)?))
        }
        Some((command, rest)) if command == "stop" => {
            Ok(DaemonCliCommand::Stop(parse_daemon_run_options(rest)?))
        }
        _ => Err(CliError::Usage),
    }
}

pub(crate) fn parse_shadow_daemon_command(args: &[String]) -> Result<DaemonCliCommand, CliError> {
    match args.split_first() {
        None => Ok(DaemonCliCommand::Help),
        Some((command, [])) if matches!(command.as_str(), "help" | "--help" | "-h") => {
            Ok(DaemonCliCommand::Help)
        }
        Some((command, rest)) if command == "run" => Ok(DaemonCliCommand::ShadowRun(
            parse_daemon_options_with_default(rest, DaemonRunOptions::shadow_default())?,
        )),
        Some((command, rest)) if command == "start" => Ok(DaemonCliCommand::ShadowStart(
            parse_daemon_options_with_default(rest, DaemonRunOptions::shadow_default())?,
        )),
        Some((command, rest)) if command == "status" => Ok(DaemonCliCommand::ShadowStatus(
            parse_daemon_options_with_default(rest, DaemonRunOptions::shadow_default())?,
        )),
        Some((command, rest)) if command == "stop" => Ok(DaemonCliCommand::ShadowStop(
            parse_daemon_options_with_default(rest, DaemonRunOptions::shadow_default())?,
        )),
        _ => Err(CliError::Usage),
    }
}

pub(crate) fn parse_daemon_run_options(args: &[String]) -> Result<DaemonRunOptions, CliError> {
    parse_daemon_options_with_default(args, DaemonRunOptions::default())
}

pub(crate) fn parse_daemon_options_with_default(
    args: &[String],
    mut options: DaemonRunOptions,
) -> Result<DaemonRunOptions, CliError> {
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
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
                    detail: format!("invalid daemon port `{port}`: {source}"),
                })?;
            }
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }
    Ok(options)
}

pub(crate) fn run_daemon_cli_command(command: DaemonCliCommand) -> Result<(), CliError> {
    match command {
        DaemonCliCommand::Help => {
            print_daemon_help();
            Ok(())
        }
        DaemonCliCommand::Run(options) => run_daemon(options),
        DaemonCliCommand::Start(options) => start_daemon(options),
        DaemonCliCommand::Status(options) => print_daemon_status(options),
        DaemonCliCommand::Stop(options) => stop_daemon(options),
        DaemonCliCommand::ShadowRun(options) => run_shadow_daemon(options),
        DaemonCliCommand::ShadowStart(options) => start_shadow_daemon(options),
        DaemonCliCommand::ShadowStatus(options) => print_shadow_daemon_status(options),
        DaemonCliCommand::ShadowStop(options) => stop_shadow_daemon(options),
    }
}

pub(crate) fn daemon_paths_from_defaults() -> Result<DaemonPaths, CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    Ok(DaemonPaths::new(&data_dir))
}

pub(crate) fn default_main_daemon_status() -> Result<Option<DaemonStatus>, CliError> {
    daemon_default_main_daemon_status(DAEMON_PROBE_TIMEOUT).map_err(Into::into)
}

pub(crate) fn shadow_daemon_paths_from_defaults() -> Result<DaemonPaths, CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    Ok(DaemonPaths::for_shadow(&data_dir))
}

pub(crate) fn run_daemon(options: DaemonRunOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    daemon_complete_pending_main_activation_from_startup(&data_dir.root())?;
    let registration = daemon_cluster_registration_if_initialized(daemon_cluster_registration(
        DaemonClusterProcess::Main,
        options.clone(),
        DaemonRunOptions::shadow_default(),
    ))?;
    run_daemon_with_paths(options, DaemonPaths::new(&data_dir), "daemon", registration)
}

pub(crate) fn run_shadow_daemon(options: DaemonRunOptions) -> Result<(), CliError> {
    daemon_sleep_for_shadow_start_delay()?;
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    let shadow_paths = DaemonPaths::for_shadow(&data_dir);
    let main_paths = DaemonPaths::new(&data_dir);
    let rollout_paths = SelfRolloutPaths::new(&data_dir.root());
    let main_options = daemon_shadow_main_options_from_env()?;
    let main_binary = daemon_shadow_main_binary_from_env(&rollout_paths);
    let registration = daemon_cluster_registration_if_initialized(daemon_cluster_registration(
        DaemonClusterProcess::Shadow,
        main_options.clone(),
        options.clone(),
    ))?;
    run_shadow_daemon_with_paths(
        options,
        shadow_paths,
        main_paths,
        rollout_paths,
        main_options,
        main_binary,
        registration,
    )
}

pub(crate) fn run_shadow_daemon_with_paths(
    options: DaemonRunOptions,
    shadow_paths: DaemonPaths,
    main_paths: DaemonPaths,
    rollout_paths: SelfRolloutPaths,
    main_options: DaemonRunOptions,
    main_binary: PathBuf,
    registration: Option<DaemonClusterRegistration>,
) -> Result<(), CliError> {
    let cluster = registration
        .as_ref()
        .map(|registration| {
            register_shadow_daemon_cluster_node_best_effort(&shadow_paths, registration)
        })
        .transpose()?
        .flatten();
    daemon_run_shadow_daemon_supervision_loop(
        options,
        shadow_paths,
        main_paths,
        rollout_paths,
        main_options,
        main_binary,
        cluster,
        DAEMON_ROLLOUT_TIMEOUT,
        DAEMON_PROBE_TIMEOUT,
        handle_daemon_connection,
    )
    .map_err(Into::into)
}

fn register_shadow_daemon_cluster_node_best_effort(
    shadow_paths: &DaemonPaths,
    registration: &DaemonClusterRegistration,
) -> Result<Option<DaemonClusterStatus>, CliError> {
    match register_daemon_cluster_node(registration) {
        Ok(cluster) => Ok(Some(cluster)),
        Err(register_error) => {
            let data_dir = shadow_paths.root.parent().ok_or_else(|| CliError::Io {
                path: shadow_paths.root.display().to_string(),
                detail: "shadow daemon paths have no data directory parent".to_string(),
            })?;
            let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
            match daemon_cluster_status_from_local_state(&cluster_paths, registration) {
                Ok(cluster) => {
                    eprintln!(
                        "sinomo shadow daemon cluster registration failed: {register_error}; continuing with local cluster identity for repair routing"
                    );
                    Ok(Some(cluster))
                }
                Err(fallback_error) => {
                    eprintln!(
                        "sinomo shadow daemon cluster registration failed: {register_error}; local cluster identity unavailable: {fallback_error}; continuing unclustered"
                    );
                    Ok(None)
                }
            }
        }
    }
}

pub(crate) fn run_daemon_with_paths(
    options: DaemonRunOptions,
    paths: DaemonPaths,
    label: &str,
    registration: Option<DaemonClusterRegistration>,
) -> Result<(), CliError> {
    let cluster = registration
        .map(|registration| register_daemon_cluster_node(&registration))
        .transpose()?;
    daemon_run_daemon_control_loop(options, paths, label, cluster).map_err(Into::into)
}

pub(crate) fn start_daemon(options: DaemonRunOptions) -> Result<(), CliError> {
    start_daemon_with_paths(
        options,
        daemon_paths_from_defaults()?,
        &["daemon", "run"],
        "daemon",
    )
}

pub(crate) fn start_shadow_daemon(options: DaemonRunOptions) -> Result<(), CliError> {
    let main_binary = env::current_exe().map_err(|source| CliError::Io {
        path: "current executable".to_string(),
        detail: source.to_string(),
    })?;
    let extra_env = vec![
        (SHADOW_MAIN_BINARY_ENV, main_binary.display().to_string()),
        (SHADOW_MAIN_HOST_ENV, options.host.clone()),
        (SHADOW_MAIN_PORT_ENV, SINOMO_DAEMON_DEFAULT_PORT.to_string()),
    ];
    start_daemon_with_paths_with_env(
        options,
        shadow_daemon_paths_from_defaults()?,
        &["daemon", "shadow", "run"],
        "shadow daemon",
        &extra_env,
    )
}

pub(crate) fn start_daemon_with_paths(
    options: DaemonRunOptions,
    paths: DaemonPaths,
    command_prefix: &[&str],
    label: &str,
) -> Result<(), CliError> {
    start_daemon_with_paths_with_env(options, paths, command_prefix, label, &[])
}

pub(crate) fn start_daemon_with_paths_with_env(
    options: DaemonRunOptions,
    paths: DaemonPaths,
    command_prefix: &[&str],
    label: &str,
    extra_env: &[(&str, String)],
) -> Result<(), CliError> {
    let status = daemon_start_daemon_command_process(
        &options,
        &paths,
        command_prefix,
        label,
        extra_env,
        Duration::from_secs(10),
        DAEMON_PROBE_TIMEOUT,
    )?;
    println!(
        "sinomo {label} started: pid={} bind={} control={}",
        status.pid,
        daemon_bind_address(&status),
        status.address
    );
    Ok(())
}

pub(crate) fn print_daemon_status(options: DaemonRunOptions) -> Result<(), CliError> {
    print_daemon_status_with_paths(options, daemon_paths_from_defaults()?, "daemon")
}

pub(crate) fn print_shadow_daemon_status(options: DaemonRunOptions) -> Result<(), CliError> {
    print_daemon_status_with_paths(
        options,
        shadow_daemon_paths_from_defaults()?,
        "shadow daemon",
    )
}

pub(crate) fn print_daemon_status_with_paths(
    options: DaemonRunOptions,
    paths: DaemonPaths,
    label: &str,
) -> Result<(), CliError> {
    match probe_daemon_status(&paths, DAEMON_PROBE_TIMEOUT) {
        DaemonProbeStatus::Running(status) => {
            println!("sinomo {label} is running");
            println!("  pid: {}", status.pid);
            println!("  version: {}", status.version);
            println!("  bind: {}", daemon_bind_address(&status));
            println!("  control address: {}", status.address);
            print_daemon_platform(&status);
            if let Some(executable) = &status.executable {
                println!("  executable: {}", executable.display());
            }
            if let Some(rollout) = &status.rollout {
                println!("  rollout: {}", format_rollout_status(rollout));
            }
            if let Some(cluster) = &status.cluster {
                println!("  cluster: {}", cluster.cluster_id);
                println!("  node: {}", cluster.node_id);
                println!("  cluster process: {}", cluster.process);
                println!(
                    "  member: {} {}",
                    format_member_role(&cluster.member_role),
                    format_member_state(&cluster.member_state)
                );
                print_local_daemon_iroh_endpoint(&paths, &status);
            }
            println!("  lock: {}", status.lock_file.display());
            println!("  status: {}", paths.status.display());
            Ok(())
        }
        DaemonProbeStatus::Locked { status } => {
            println!("sinomo {label} lock is held but the listener did not answer");
            if let Some(status) = status {
                println!("  pid: {}", status.pid);
                println!("  version: {}", status.version);
                println!("  bind: {}", daemon_bind_address(&status));
                println!("  control address: {}", status.address);
                print_daemon_platform(&status);
                if let Some(executable) = &status.executable {
                    println!("  executable: {}", executable.display());
                }
                if let Some(rollout) = &status.rollout {
                    println!("  rollout: {}", format_rollout_status(rollout));
                }
                if let Some(cluster) = &status.cluster {
                    println!("  cluster: {}", cluster.cluster_id);
                    println!("  node: {}", cluster.node_id);
                    println!("  cluster process: {}", cluster.process);
                    println!(
                        "  member: {} {}",
                        format_member_role(&cluster.member_role),
                        format_member_state(&cluster.member_state)
                    );
                    print_local_daemon_iroh_endpoint(&paths, &status);
                }
            } else {
                println!("  default bind: {}:{}", options.host, options.port);
                println!(
                    "  default control address: {}",
                    daemon_connect_address(&options.host, options.port)
                );
            }
            println!("  lock: {}", paths.lock.display());
            Ok(())
        }
        DaemonProbeStatus::NotRunning => {
            println!("sinomo {label} is not running");
            println!("  default bind: {}:{}", options.host, options.port);
            println!(
                "  default control address: {}",
                daemon_connect_address(&options.host, options.port)
            );
            Ok(())
        }
    }
}

fn print_local_daemon_iroh_endpoint(paths: &DaemonPaths, status: &DaemonStatus) {
    let Some(cluster) = status.cluster.as_ref() else {
        return;
    };
    let Some(data_dir) = paths.root.parent() else {
        return;
    };
    let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
    let Ok(record) = daemon_read_local_iroh_endpoint_record(&cluster_paths, &cluster.node_id)
    else {
        return;
    };
    println!("  iroh endpoint: {}", record.endpoint().address);
}

pub(crate) fn stop_daemon(options: DaemonRunOptions) -> Result<(), CliError> {
    stop_daemon_with_paths(daemon_paths_from_defaults()?, options, "daemon")
}

pub(crate) fn stop_shadow_daemon(options: DaemonRunOptions) -> Result<(), CliError> {
    stop_daemon_with_paths(
        shadow_daemon_paths_from_defaults()?,
        options,
        "shadow daemon",
    )
}

pub(crate) fn stop_daemon_with_paths(
    paths: DaemonPaths,
    _options: DaemonRunOptions,
    label: &str,
) -> Result<(), CliError> {
    match probe_daemon_status(&paths, DAEMON_PROBE_TIMEOUT) {
        DaemonProbeStatus::Running(status) => {
            let body = send_daemon_shutdown_request(&status.address, DAEMON_PROBE_TIMEOUT)?;
            println!("{}", body.trim());
            Ok(())
        }
        DaemonProbeStatus::Locked { .. } => Err(CliError::DaemonNotRunning {
            detail: format!("{label} lock is held but the listener did not answer"),
        }),
        DaemonProbeStatus::NotRunning => Err(CliError::DaemonNotRunning {
            detail: "no daemon lock is held".to_string(),
        }),
    }
}

pub(crate) fn handle_daemon_connection(
    stream: &mut network::DaemonControlStream,
    paths: &DaemonPaths,
) -> Result<bool, CliError> {
    daemon_handle_daemon_connection(stream, paths).map_err(Into::into)
}

#[cfg(test)]
pub(crate) fn handle_daemon_cluster_kv_txn_request(
    paths: &DaemonPaths,
    txn: KvTxn,
) -> Result<KvTxnResponse, CliError> {
    daemon_handle_daemon_cluster_kv_txn_request(paths, txn).map_err(Into::into)
}

#[cfg(test)]
pub(crate) fn handle_daemon_cluster_join_request(
    paths: &DaemonPaths,
    request: ClusterJoinRegistrationRequest,
) -> Result<ClusterJoinRegistrationResponse, CliError> {
    daemon_handle_daemon_cluster_join_request(paths, request).map_err(Into::into)
}

pub(crate) fn stage_rollout_main_activation(
    data_dir: &Path,
    rollout_paths: &SelfRolloutPaths,
    activation: RolloutMainActivation,
) -> Result<RolloutActivationStrategy, CliError> {
    let main_paths = DaemonPaths::with_root(data_dir.join("daemon"));
    daemon_stage_rollout_main_activation(&main_paths, data_dir, rollout_paths, activation)
        .map_err(Into::into)
}

#[cfg(test)]
pub(crate) fn handle_daemon_rollout_build_request(
    paths: &DaemonPaths,
    request: ClusterRolloutBuildRequest,
) -> Result<ClusterRolloutBinaryResponse, CliError> {
    daemon_handle_daemon_rollout_build_request(paths, request).map_err(Into::into)
}
