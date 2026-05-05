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
    let paths = daemon_paths_from_defaults()?;
    Ok(match probe_daemon_status(&paths, DAEMON_PROBE_TIMEOUT) {
        DaemonProbeStatus::Running(status) => Some(status),
        DaemonProbeStatus::Locked { .. } | DaemonProbeStatus::NotRunning => None,
    })
}

pub(crate) fn shadow_daemon_paths_from_defaults() -> Result<DaemonPaths, CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    Ok(DaemonPaths::for_shadow(&data_dir))
}

pub(crate) fn run_daemon(options: DaemonRunOptions) -> Result<(), CliError> {
    let registration = daemon_cluster_registration_if_initialized(daemon_cluster_registration(
        DaemonClusterProcess::Main,
        options.clone(),
        DaemonRunOptions::shadow_default(),
    ))?;
    run_daemon_with_paths(
        options,
        daemon_paths_from_defaults()?,
        "daemon",
        registration,
    )
}

pub(crate) fn run_shadow_daemon(options: DaemonRunOptions) -> Result<(), CliError> {
    sleep_for_shadow_start_delay()?;
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    let shadow_paths = DaemonPaths::for_shadow(&data_dir);
    let main_paths = DaemonPaths::new(&data_dir);
    let rollout_paths = SelfRolloutPaths::new(&data_dir.root());
    let main_options = shadow_main_options_from_env()?;
    let main_binary = shadow_main_binary_from_env(&rollout_paths);
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

pub(crate) fn sleep_for_shadow_start_delay() -> Result<(), CliError> {
    let Ok(delay) = env::var(SHADOW_START_DELAY_MS_ENV) else {
        return Ok(());
    };
    let delay = delay.parse::<u64>().map_err(|source| CliError::Parse {
        detail: format!("invalid {SHADOW_START_DELAY_MS_ENV} `{delay}`: {source}"),
    })?;
    if delay > 0 {
        thread::sleep(Duration::from_millis(delay));
    }
    Ok(())
}

pub(crate) fn run_shadow_daemon_with_paths(
    options: DaemonRunOptions,
    shadow_paths: DaemonPaths,
    main_paths: DaemonPaths,
    rollout_paths: SelfRolloutPaths,
    mut main_options: DaemonRunOptions,
    main_binary: PathBuf,
    registration: Option<DaemonClusterRegistration>,
) -> Result<(), CliError> {
    let cluster = registration
        .map(|registration| register_daemon_cluster_node(&registration))
        .transpose()?;
    let _lock = DaemonLock::acquire(&shadow_paths, &options, cluster)?;
    let bind_address = format!("{}:{}", options.host, options.port);
    let listener = TcpListener::bind(&bind_address).map_err(|source| CliError::Network {
        address: bind_address.clone(),
        detail: source.to_string(),
    })?;
    listener
        .set_nonblocking(true)
        .map_err(|source| CliError::Network {
            address: bind_address.clone(),
            detail: source.to_string(),
        })?;
    println!("sinomo shadow daemon listening on {bind_address}");

    let mut main_binary = main_binary;
    let mut supervised_main: Option<Child> = None;
    let mut main_restart_suppressed = false;
    let mut pending_rollout: Option<ShadowMainRolloutMonitor> = None;
    let mut next_status_tick = Instant::now();
    loop {
        match listener.accept() {
            Ok((mut stream, _address)) => {
                match handle_daemon_connection(&mut stream, &shadow_paths) {
                    Ok(true) => break,
                    Ok(false) => {}
                    Err(error) => eprintln!("sinomo shadow daemon client error: {error}"),
                }
            }
            Err(source) if source.kind() == io::ErrorKind::WouldBlock => {}
            Err(source) => {
                return Err(CliError::Network {
                    address: bind_address.clone(),
                    detail: source.to_string(),
                });
            }
        }

        if let Some(pending) = pending_rollout.as_ref() {
            if matches!(
                probe_daemon_status(&main_paths, DAEMON_PROBE_TIMEOUT),
                DaemonProbeStatus::Running(_)
            ) {
                stage_pending_shadow_rollout(&rollout_paths, pending, &options.host, options.port)?;
                let main_address =
                    daemon_connect_address(&pending.main_options.host, pending.main_options.port);
                let promoted_main_options = pending.main_options.clone();
                thread::spawn(move || {
                    let _ = send_daemon_command(
                        &main_address,
                        "FINALIZE_SHADOW_ROLLOUT",
                        DAEMON_ROLLOUT_TIMEOUT,
                    );
                });
                let _ = fs::remove_file(rollout_main_activation_path(&rollout_paths));
                pending_rollout = None;
                main_options = promoted_main_options;
            } else if Instant::now() >= pending.deadline {
                let detail = "candidate main did not become healthy before rollout timeout";
                if let Some(child) = supervised_main.as_mut() {
                    let _ = child.kill();
                    let _ = child.wait();
                }
                let diagnostics =
                    write_rollout_main_failure_diagnostics(&rollout_paths, pending, detail, None)?;
                record_main_rollout_failure_with_diagnostics(
                    &rollout_paths,
                    &pending.activation,
                    &pending.previous_main_binary,
                    &pending.previous_shadow_binary,
                    detail,
                    Some(&diagnostics),
                )?;
                main_binary = pending.previous_main_binary.clone();
                if let Some(backup) = pending.previous_main_backup.as_ref() {
                    let _ = restore_rollout_binary_from_backup(backup, &main_binary);
                }
                main_options = pending.main_options.clone();
                supervised_main = spawn_main_for_shadow(&main_binary, &main_options).ok();
                pending_rollout = None;
                let _ = fs::remove_file(rollout_main_activation_path(&rollout_paths));
            }
        }

        if !main_restart_suppressed
            && supervised_main.is_none()
            && matches!(
                probe_daemon_status(&main_paths, DAEMON_PROBE_TIMEOUT),
                DaemonProbeStatus::NotRunning
            )
        {
            if let Some(activation) = read_rollout_main_activation(&rollout_paths)? {
                let previous_main_binary = activation
                    .previous_main_binary
                    .clone()
                    .unwrap_or_else(|| main_binary.clone());
                let activation_main_options = DaemonRunOptions {
                    host: activation.main_host.clone(),
                    port: activation.main_port,
                };
                let previous_main_backup = activation
                    .staged_main_binary
                    .as_ref()
                    .map(|staged| activate_staged_rollout_binary(staged, &activation.main_binary))
                    .transpose()?
                    .flatten();
                match spawn_candidate_main_for_shadow(
                    &activation.main_binary,
                    &activation_main_options,
                    &rollout_paths,
                ) {
                    Ok((child, diagnostics)) => {
                        main_binary = activation.main_binary.clone();
                        supervised_main = Some(child);
                        pending_rollout = Some(ShadowMainRolloutMonitor {
                            activation,
                            main_options: activation_main_options,
                            previous_main_binary,
                            previous_main_backup,
                            previous_shadow_binary: env::current_exe().unwrap_or_default(),
                            diagnostics,
                            deadline: Instant::now() + Duration::from_secs(10),
                        });
                    }
                    Err(error) => {
                        record_activation_failure(
                            &rollout_paths,
                            &activation,
                            &previous_main_binary,
                            &format!("candidate main could not execute: {error}"),
                        )?;
                        main_binary = previous_main_binary;
                        if let Some(backup) = previous_main_backup {
                            let _ = restore_rollout_binary_from_backup(&backup, &main_binary);
                        }
                        main_options = activation_main_options;
                        supervised_main = spawn_main_for_shadow(&main_binary, &main_options).ok();
                        let _ = fs::remove_file(rollout_main_activation_path(&rollout_paths));
                    }
                }
            } else if main_binary.exists() {
                supervised_main = Some(spawn_main_for_shadow(&main_binary, &main_options)?);
            }
        }

        if let Some(child) = supervised_main.as_mut() {
            if let Some(status) = child.try_wait().map_err(|source| CliError::Io {
                path: main_binary.display().to_string(),
                detail: source.to_string(),
            })? {
                if let Some(pending) = pending_rollout.take() {
                    let observation = main_exit_observation_from_status(status);
                    let detail = format!(
                        "candidate main exited before health check: exit_code={:?} signal={:?}",
                        observation.exit_code, observation.signal
                    );
                    let diagnostics = write_rollout_main_failure_diagnostics(
                        &rollout_paths,
                        &pending,
                        &detail,
                        Some(observation),
                    )?;
                    record_main_rollout_failure_with_diagnostics(
                        &rollout_paths,
                        &pending.activation,
                        &pending.previous_main_binary,
                        &pending.previous_shadow_binary,
                        &detail,
                        Some(&diagnostics),
                    )?;
                    main_binary = pending.previous_main_binary;
                    if let Some(backup) = pending.previous_main_backup {
                        let _ = restore_rollout_binary_from_backup(&backup, &main_binary);
                    }
                    main_options = pending.main_options;
                    supervised_main = spawn_main_for_shadow(&main_binary, &main_options).ok();
                    let _ = fs::remove_file(rollout_main_activation_path(&rollout_paths));
                    thread::sleep(Duration::from_millis(200));
                    continue;
                }
                if read_rollout_main_activation(&rollout_paths)?.is_some() {
                    supervised_main = None;
                    main_restart_suppressed = false;
                    thread::sleep(Duration::from_millis(200));
                    continue;
                }
                let observation = main_exit_observation_from_status(status);
                let kind = classify_main_exit(observation);
                write_json(
                    &rollout_paths.root.join("last-main-exit.json"),
                    &serde_json::json!({
                        "exit_code": observation.exit_code,
                        "signal": observation.signal,
                        "requested_shutdown": observation.requested_shutdown,
                        "kind": kind,
                        "observed_at": OffsetDateTime::now_utc().format(&Rfc3339).unwrap_or_else(|_| "unknown".to_string())
                    }),
                )?;
                supervised_main = None;
                if kind == MainExitKind::Crash {
                    supervised_main = Some(spawn_main_for_shadow(&main_binary, &main_options)?);
                } else {
                    main_restart_suppressed = true;
                }
            }
        }

        if Instant::now() >= next_status_tick {
            let _ = refresh_daemon_node_status_in_kv(&shadow_paths);
            next_status_tick = Instant::now() + DAEMON_CLUSTER_SYNC_INTERVAL;
        }

        thread::sleep(Duration::from_millis(200));
    }
    Ok(())
}

pub(crate) fn spawn_main_for_shadow(
    binary: &Path,
    options: &DaemonRunOptions,
) -> Result<Child, CliError> {
    Command::new(binary)
        .arg("daemon")
        .arg("run")
        .arg("--host")
        .arg(&options.host)
        .arg("--port")
        .arg(options.port.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|source| CliError::Io {
            path: binary.display().to_string(),
            detail: source.to_string(),
        })
}

pub(crate) fn spawn_candidate_main_for_shadow(
    binary: &Path,
    options: &DaemonRunOptions,
    rollout_paths: &SelfRolloutPaths,
) -> Result<(Child, CandidateMainDiagnostics), CliError> {
    let logs_dir = rollout_paths.root.join("logs");
    fs::create_dir_all(&logs_dir).map_err(|source| CliError::Io {
        path: logs_dir.display().to_string(),
        detail: source.to_string(),
    })?;
    let suffix = rollout_artifact_suffix();
    let stdout_log = logs_dir.join(format!("candidate-main-{suffix}.stdout.log"));
    let stderr_log = logs_dir.join(format!("candidate-main-{suffix}.stderr.log"));
    let diagnostics_path = logs_dir.join(format!("candidate-main-{suffix}.diagnostics.json"));
    let stdout = File::create(&stdout_log).map_err(|source| CliError::Io {
        path: stdout_log.display().to_string(),
        detail: source.to_string(),
    })?;
    let stderr = File::create(&stderr_log).map_err(|source| CliError::Io {
        path: stderr_log.display().to_string(),
        detail: source.to_string(),
    })?;
    let child = Command::new(binary)
        .arg("daemon")
        .arg("run")
        .arg("--host")
        .arg(&options.host)
        .arg("--port")
        .arg(options.port.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()
        .map_err(|source| CliError::Io {
            path: binary.display().to_string(),
            detail: source.to_string(),
        })?;
    Ok((
        child,
        CandidateMainDiagnostics {
            stdout_log,
            stderr_log,
            diagnostics_path,
        },
    ))
}

#[derive(Clone, Debug)]
pub(crate) struct CandidateMainDiagnostics {
    pub(crate) stdout_log: PathBuf,
    pub(crate) stderr_log: PathBuf,
    pub(crate) diagnostics_path: PathBuf,
}

#[derive(Clone, Debug)]
pub(crate) struct ShadowMainRolloutMonitor {
    pub(crate) activation: RolloutMainActivation,
    pub(crate) main_options: DaemonRunOptions,
    pub(crate) previous_main_binary: PathBuf,
    pub(crate) previous_main_backup: Option<PathBuf>,
    pub(crate) previous_shadow_binary: PathBuf,
    pub(crate) diagnostics: CandidateMainDiagnostics,
    pub(crate) deadline: Instant,
}

impl PendingShadowRolloutContext for ShadowMainRolloutMonitor {
    fn activation(&self) -> &RolloutMainActivation {
        &self.activation
    }

    fn main_binary(&self) -> &Path {
        &self.activation.main_binary
    }

    fn main_host(&self) -> &str {
        &self.main_options.host
    }

    fn main_port(&self) -> u16 {
        self.main_options.port
    }

    fn previous_shadow_binary(&self) -> &Path {
        &self.previous_shadow_binary
    }
}

pub(crate) fn main_exit_observation_from_status(status: ExitStatus) -> MainExitObservation {
    #[cfg(unix)]
    {
        if let Some(signal) = status.signal() {
            return MainExitObservation::signal(signal);
        }
    }
    MainExitObservation::exit_code(status.code().unwrap_or(1))
}

pub(crate) fn write_rollout_main_failure_diagnostics(
    rollout_paths: &SelfRolloutPaths,
    pending: &ShadowMainRolloutMonitor,
    detail: &str,
    observation: Option<MainExitObservation>,
) -> Result<RolloutFailureDiagnostics, CliError> {
    let stdout_tail = read_tail_lossy(
        &pending.diagnostics.stdout_log,
        ROLLOUT_DIAGNOSTIC_TAIL_BYTES,
    )?;
    let stderr_tail = read_tail_lossy(
        &pending.diagnostics.stderr_log,
        ROLLOUT_DIAGNOSTIC_TAIL_BYTES,
    )?;
    let diagnostics = RolloutFailureDiagnostics {
        diagnostics_path: pending.diagnostics.diagnostics_path.clone(),
        stdout_log: Some(pending.diagnostics.stdout_log.clone()),
        stderr_log: Some(pending.diagnostics.stderr_log.clone()),
        stdout_tail: stdout_tail.clone().filter(|tail| !tail.trim().is_empty()),
        stderr_tail: stderr_tail.clone().filter(|tail| !tail.trim().is_empty()),
    };
    write_json(
        &pending.diagnostics.diagnostics_path,
        &serde_json::json!({
            "status": "failed",
            "detail": detail,
            "source": pending.activation.source,
            "mode": pending.activation.mode,
            "main_binary": pending.activation.main_binary,
            "previous_main_binary": pending.previous_main_binary,
            "stdout_log": pending.diagnostics.stdout_log,
            "stderr_log": pending.diagnostics.stderr_log,
            "stdout_tail": stdout_tail,
            "stderr_tail": stderr_tail,
            "exit_code": observation.and_then(|observation| observation.exit_code),
            "signal": observation.and_then(|observation| observation.signal),
            "requested_shutdown": observation.map(|observation| observation.requested_shutdown),
            "observed_at": OffsetDateTime::now_utc().format(&Rfc3339).unwrap_or_else(|_| "unknown".to_string()),
            "rollout_state": rollout_paths.state
        }),
    )?;
    Ok(diagnostics)
}

pub(crate) fn read_tail_lossy(path: &Path, max_bytes: usize) -> Result<Option<String>, CliError> {
    match fs::read(path) {
        Ok(bytes) => {
            let start = bytes.len().saturating_sub(max_bytes);
            Ok(Some(String::from_utf8_lossy(&bytes[start..]).to_string()))
        }
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(CliError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        }),
    }
}

pub(crate) fn shadow_main_options_from_env() -> Result<DaemonRunOptions, CliError> {
    let mut options = DaemonRunOptions::default();
    if let Ok(host) = env::var(SHADOW_MAIN_HOST_ENV) {
        options.host = host;
    }
    if let Ok(port) = env::var(SHADOW_MAIN_PORT_ENV) {
        options.port = port.parse::<u16>().map_err(|source| CliError::Parse {
            detail: format!("invalid {SHADOW_MAIN_PORT_ENV} `{port}`: {source}"),
        })?;
    }
    Ok(options)
}

pub(crate) fn shadow_main_binary_from_env(rollout_paths: &SelfRolloutPaths) -> PathBuf {
    env::var(SHADOW_MAIN_BINARY_ENV)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| rollout_paths.main_binary.clone())
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
    let _lock = DaemonLock::acquire(&paths, &options, cluster)?;
    let bind_address = format!("{}:{}", options.host, options.port);
    let listener = TcpListener::bind(&bind_address).map_err(|source| CliError::Network {
        address: bind_address.clone(),
        detail: source.to_string(),
    })?;
    listener
        .set_nonblocking(true)
        .map_err(|source| CliError::Network {
            address: bind_address.clone(),
            detail: source.to_string(),
        })?;
    println!("sinomo {label} listening on {bind_address}");

    let mut cluster_network = ClusterNetworkMaintenance::new();
    let mut next_cluster_tick = Instant::now();
    loop {
        match listener.accept() {
            Ok((mut stream, _address)) => {
                if let Err(source) = stream.set_nonblocking(false) {
                    eprintln!(
                        "sinomo {label} client error: {}",
                        CliError::Network {
                            address: "daemon client".to_string(),
                            detail: source.to_string(),
                        }
                    );
                    continue;
                }
                match handle_daemon_connection(&mut stream, &paths) {
                    Ok(true) => break,
                    Ok(false) => {}
                    Err(error) => eprintln!("sinomo {label} client error: {error}"),
                }
            }
            Err(source) if source.kind() == io::ErrorKind::WouldBlock => {}
            Err(source) => {
                return Err(CliError::Network {
                    address: bind_address.clone(),
                    detail: source.to_string(),
                });
            }
        }

        if Instant::now() >= next_cluster_tick {
            if let Err(error) = cluster_network.tick(&paths, &options) {
                eprintln!("sinomo {label} cluster network tick failed: {error}");
            }
            next_cluster_tick = Instant::now() + DAEMON_CLUSTER_TICK_INTERVAL;
        }
        thread::sleep(Duration::from_millis(25));
    }
    Ok(())
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
    if matches!(
        probe_daemon_status(&paths, DAEMON_PROBE_TIMEOUT),
        DaemonProbeStatus::Running(_) | DaemonProbeStatus::Locked { .. }
    ) {
        return Err(CliError::DaemonAlreadyRunning {
            lock_file: paths.lock.display().to_string(),
            detail: "daemon lock is already held".to_string(),
        });
    }

    let exe = env::current_exe().map_err(|source| CliError::Io {
        path: "current executable".to_string(),
        detail: source.to_string(),
    })?;
    let mut command = Command::new(exe);
    command.args(command_prefix);
    for (key, value) in extra_env {
        command.env(key, value);
    }
    let child = command
        .arg("--host")
        .arg(&options.host)
        .arg("--port")
        .arg(options.port.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|source| CliError::Io {
            path: format!("sinomo {label} start"),
            detail: source.to_string(),
        })?;

    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline {
        if let DaemonProbeStatus::Running(status) =
            probe_daemon_status(&paths, DAEMON_PROBE_TIMEOUT)
        {
            println!(
                "sinomo {label} started: pid={} bind={} control={}",
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
        detail: format!("daemon process {} did not become ready", child.id()),
    })
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
            let response = send_daemon_command(&status.address, "SHUTDOWN", DAEMON_PROBE_TIMEOUT)?;
            println!("{}", response.trim());
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
    stream: &mut TcpStream,
    paths: &DaemonPaths,
) -> Result<bool, CliError> {
    handle_authenticated_daemon_connection(stream, paths, |authenticated, stream| {
        let command = authenticated.command.as_str();
        match command {
            "STATUS" => {
                let mut status = read_daemon_status(paths)?;
                ensure_daemon_status_cluster(paths, &mut status)?;
                if let Some(data_dir) = paths.root.parent() {
                    status.rollout = read_cluster_rollout_status(&SelfRolloutPaths::new(data_dir))?;
                }
                let body =
                    serde_json::to_string_pretty(&status).map_err(|source| CliError::Json {
                        path: paths.status.display().to_string(),
                        detail: source.to_string(),
                    })?;
                stream
                    .write_all(body.as_bytes())
                    .map_err(|source| CliError::Network {
                        address: status.address.clone(),
                        detail: source.to_string(),
                    })?;
                Ok(false)
            }
            "SHUTDOWN" => {
                stream
                    .write_all(b"sinomo daemon stopping\n")
                    .map_err(|source| CliError::Network {
                        address: "daemon client".to_string(),
                        detail: source.to_string(),
                    })?;
                Ok(true)
            }
            "FINALIZE_SHADOW_ROLLOUT" => {
                let response = handle_daemon_finalize_shadow_rollout(paths)?;
                stream
                    .write_all(response.as_bytes())
                    .map_err(|source| CliError::Network {
                        address: "daemon client".to_string(),
                        detail: source.to_string(),
                    })?;
                Ok(false)
            }
            "ROLLOUT_DIAGNOSTICS" => {
                let response = handle_daemon_rollout_diagnostics(paths)?;
                stream
                    .write_all(response.as_bytes())
                    .map_err(|source| CliError::Network {
                        address: "daemon client".to_string(),
                        detail: source.to_string(),
                    })?;
                Ok(false)
            }
            command if command.starts_with("CLUSTER_JOIN ") => {
                let payload = command
                    .strip_prefix("CLUSTER_JOIN ")
                    .unwrap_or_default()
                    .trim();
                let response = handle_daemon_cluster_join(paths, payload)?;
                let body =
                    serde_json::to_string_pretty(&response).map_err(|source| CliError::Json {
                        path: "cluster join response".to_string(),
                        detail: source.to_string(),
                    })?;
                stream
                    .write_all(body.as_bytes())
                    .map_err(|source| CliError::Network {
                        address: "daemon client".to_string(),
                        detail: source.to_string(),
                    })?;
                Ok(false)
            }
            "CLUSTER_KV_SNAPSHOT" => {
                let snapshot = handle_daemon_cluster_kv_snapshot(paths)?;
                let body =
                    serde_json::to_string_pretty(&snapshot).map_err(|source| CliError::Json {
                        path: "cluster kv snapshot response".to_string(),
                        detail: source.to_string(),
                    })?;
                stream
                    .write_all(body.as_bytes())
                    .map_err(|source| CliError::Network {
                        address: "daemon client".to_string(),
                        detail: source.to_string(),
                    })?;
                Ok(false)
            }
            command if command.starts_with("CLUSTER_KV_TXN ") => {
                let payload = command
                    .strip_prefix("CLUSTER_KV_TXN ")
                    .unwrap_or_default()
                    .trim();
                let response = handle_daemon_cluster_kv_txn(paths, payload)?;
                let body =
                    serde_json::to_string_pretty(&response).map_err(|source| CliError::Json {
                        path: "cluster kv txn response".to_string(),
                        detail: source.to_string(),
                    })?;
                stream
                    .write_all(body.as_bytes())
                    .map_err(|source| CliError::Network {
                        address: "daemon client".to_string(),
                        detail: source.to_string(),
                    })?;
                Ok(false)
            }
            command if command.starts_with("CLUSTER_KV_RANGE_PREFIX ") => {
                let prefix = command
                    .strip_prefix("CLUSTER_KV_RANGE_PREFIX ")
                    .unwrap_or_default()
                    .trim();
                let response = handle_daemon_cluster_kv_range_prefix(paths, prefix)?;
                let body =
                    serde_json::to_string_pretty(&response).map_err(|source| CliError::Json {
                        path: "cluster kv range response".to_string(),
                        detail: source.to_string(),
                    })?;
                stream
                    .write_all(body.as_bytes())
                    .map_err(|source| CliError::Network {
                        address: "daemon client".to_string(),
                        detail: source.to_string(),
                    })?;
                Ok(false)
            }
            command if command.starts_with("CLUSTER_SYNC ") => {
                let payload = command
                    .strip_prefix("CLUSTER_SYNC ")
                    .unwrap_or_default()
                    .trim();
                let response = handle_daemon_cluster_sync(paths, payload)?;
                let body =
                    serde_json::to_string_pretty(&response).map_err(|source| CliError::Json {
                        path: "cluster sync response".to_string(),
                        detail: source.to_string(),
                    })?;
                stream
                    .write_all(body.as_bytes())
                    .map_err(|source| CliError::Network {
                        address: "daemon client".to_string(),
                        detail: source.to_string(),
                    })?;
                Ok(false)
            }
            command if command.starts_with("ROLLOUT_BINARY ") => {
                let payload = command
                    .strip_prefix("ROLLOUT_BINARY ")
                    .unwrap_or_default()
                    .trim();
                let response = match handle_daemon_rollout_binary(paths, payload) {
                    Ok(response) => response,
                    Err(error) => ClusterRolloutBinaryResponse {
                        status: "failed".to_string(),
                        detail: error.to_string(),
                        main_binary: PathBuf::new(),
                        shadow_binary: PathBuf::new(),
                    },
                };
                let body =
                    serde_json::to_string_pretty(&response).map_err(|source| CliError::Json {
                        path: "rollout response".to_string(),
                        detail: source.to_string(),
                    })?;
                stream
                    .write_all(body.as_bytes())
                    .map_err(|source| CliError::Network {
                        address: "daemon client".to_string(),
                        detail: source.to_string(),
                    })?;
                stream.flush().map_err(|source| CliError::Network {
                    address: "daemon client".to_string(),
                    detail: source.to_string(),
                })?;
                let _ = stream.shutdown(Shutdown::Write);
                Ok(response.status == "accepted")
            }
            command if command.starts_with("ROLLOUT_BUILD ") => {
                let payload = command
                    .strip_prefix("ROLLOUT_BUILD ")
                    .unwrap_or_default()
                    .trim();
                let response = match handle_daemon_rollout_build(paths, payload) {
                    Ok(response) => response,
                    Err(error) => ClusterRolloutBinaryResponse {
                        status: "failed".to_string(),
                        detail: error.to_string(),
                        main_binary: PathBuf::new(),
                        shadow_binary: PathBuf::new(),
                    },
                };
                let body =
                    serde_json::to_string_pretty(&response).map_err(|source| CliError::Json {
                        path: "rollout response".to_string(),
                        detail: source.to_string(),
                    })?;
                stream
                    .write_all(body.as_bytes())
                    .map_err(|source| CliError::Network {
                        address: "daemon client".to_string(),
                        detail: source.to_string(),
                    })?;
                stream.flush().map_err(|source| CliError::Network {
                    address: "daemon client".to_string(),
                    detail: source.to_string(),
                })?;
                let _ = stream.shutdown(Shutdown::Write);
                Ok(response.status == "accepted")
            }
            _ => {
                stream
                    .write_all(b"unknown daemon command\n")
                    .map_err(|source| CliError::Network {
                        address: "daemon client".to_string(),
                        detail: source.to_string(),
                    })?;
                Ok(false)
            }
        }
    })
}

pub(crate) fn handle_daemon_cluster_kv_snapshot(
    paths: &DaemonPaths,
) -> Result<KvSnapshot, CliError> {
    let data_dir = paths.root.parent().ok_or_else(|| CliError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
    read_cluster_kv_store(&cluster_paths).map(|store| store.snapshot())
}

pub(crate) fn handle_daemon_cluster_kv_txn(
    paths: &DaemonPaths,
    payload: &str,
) -> Result<KvTxnResponse, CliError> {
    let txn: KvTxn = serde_json::from_str(payload).map_err(|source| CliError::Json {
        path: "cluster kv txn".to_string(),
        detail: source.to_string(),
    })?;
    let data_dir = paths.root.parent().ok_or_else(|| CliError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
    let mut store = read_cluster_kv_store(&cluster_paths)?;
    let response = store.txn(txn)?;
    write_cluster_kv_snapshot(&cluster_paths, &store.snapshot())?;
    Ok(response)
}

pub(crate) fn handle_daemon_cluster_kv_range_prefix(
    paths: &DaemonPaths,
    prefix: &str,
) -> Result<Vec<KvEntry>, CliError> {
    let data_dir = paths.root.parent().ok_or_else(|| CliError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
    let store = read_cluster_kv_store(&cluster_paths)?;
    Ok(store.range_prefix(prefix))
}

pub(crate) fn handle_daemon_cluster_join(
    paths: &DaemonPaths,
    payload: &str,
) -> Result<ClusterJoinRegistrationResponse, CliError> {
    let request: ClusterJoinRegistrationRequest =
        serde_json::from_str(payload).map_err(|source| CliError::Json {
            path: "cluster join registration".to_string(),
            detail: source.to_string(),
        })?;
    let data_dir = paths.root.parent().ok_or_else(|| CliError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
    let mut state = read_cluster_state(&cluster_paths)?;
    if let Err(error) = verify_cluster_join_invite_signature(&request.invite) {
        return Ok(ClusterJoinRegistrationResponse {
            accepted: false,
            detail: format!("invite signature rejected: {error}"),
            state,
            kv_snapshot: None,
        });
    }
    if !state.members.iter().any(|member| {
        member.node_id == request.invite.issuer_node_id
            && member.node_public_key == request.invite.issuer_public_key
            && member.state.is_authorized()
    }) {
        return Ok(ClusterJoinRegistrationResponse {
            accepted: false,
            detail: format!(
                "invite issuer {} is not an authorized cluster member",
                request.invite.issuer_node_id
            ),
            state,
            kv_snapshot: None,
        });
    }
    if let Ok(main_status) = read_daemon_status(paths) {
        let main_options = daemon_options_from_status(&main_status);
        let shadow_options = match probe_daemon_status(
            &DaemonPaths::with_root(data_dir.join("shadow-daemon")),
            DAEMON_PROBE_TIMEOUT,
        ) {
            DaemonProbeStatus::Running(status)
            | DaemonProbeStatus::Locked {
                status: Some(status),
            } => daemon_options_from_status(&status),
            DaemonProbeStatus::Locked { status: None } | DaemonProbeStatus::NotRunning => {
                DaemonRunOptions::shadow_default()
            }
        };
        ensure_daemon_cluster_members(
            &mut state,
            &main_options,
            &shadow_options,
            OffsetDateTime::now_utc(),
        );
    }
    if request.invite.cluster_id != state.cluster_id {
        return Ok(ClusterJoinRegistrationResponse {
            accepted: false,
            detail: format!(
                "invite cluster `{}` does not match introducer cluster `{}`",
                request.invite.cluster_id, state.cluster_id
            ),
            state,
            kv_snapshot: None,
        });
    }
    if request.invite.expires_at != OffsetDateTime::UNIX_EPOCH
        && request.invite.expires_at <= OffsetDateTime::now_utc()
    {
        return Ok(ClusterJoinRegistrationResponse {
            accepted: false,
            detail: format!(
                "invite expired at {}",
                format_rfc3339(request.invite.expires_at)
            ),
            state,
            kv_snapshot: None,
        });
    }
    let timestamp = OffsetDateTime::now_utc();
    for mut member in request.members {
        member.cluster_id = state.cluster_id.clone();
        if matches!(member.state, ClusterMemberState::Pending) {
            member.state = match member.role {
                ClusterMemberRole::Learner => ClusterMemberState::Learner,
                ClusterMemberRole::NonVoting => ClusterMemberState::NonVoting,
                ClusterMemberRole::Voter => ClusterMemberState::Voter,
            };
        }
        member.updated_at = timestamp;
        upsert_member_record(&mut state, member);
    }
    state.kv_revision = state.kv_revision.saturating_add(1);
    state.updated_at = timestamp;
    network::write_cluster_state(&cluster_paths, &state)?;
    let snapshot = sync_cluster_state_to_kv(&cluster_paths, &state)?;
    Ok(ClusterJoinRegistrationResponse {
        accepted: true,
        detail: "join registered".to_string(),
        state,
        kv_snapshot: Some(snapshot),
    })
}

pub(crate) fn upsert_member_record(state: &mut ClusterBootstrapState, member: ClusterMemberRecord) {
    if let Some(existing) = state
        .members
        .iter_mut()
        .find(|existing| existing.node_id == member.node_id)
    {
        *existing = member;
    } else {
        state.members.push(member);
    }
}

pub(crate) fn handle_daemon_rollout_binary(
    paths: &DaemonPaths,
    payload: &str,
) -> Result<ClusterRolloutBinaryResponse, CliError> {
    let request: ClusterRolloutBinaryRequest =
        serde_json::from_str(payload).map_err(|source| CliError::Json {
            path: "cluster rollout request".to_string(),
            detail: source.to_string(),
        })?;
    let data_dir = paths.root.parent().ok_or_else(|| CliError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    if (!request.target_os.is_empty() && request.target_os != env::consts::OS)
        || (!request.target_arch.is_empty() && request.target_arch != env::consts::ARCH)
    {
        return Ok(ClusterRolloutBinaryResponse {
            status: "rejected".to_string(),
            detail: format!(
                "artifact platform {}/{} does not match target platform {}/{}",
                request.target_os,
                request.target_arch,
                env::consts::OS,
                env::consts::ARCH
            ),
            main_binary: PathBuf::new(),
            shadow_binary: PathBuf::new(),
        });
    }
    let rollout_paths = SelfRolloutPaths::new(data_dir);
    ensure_rollout_layout(&rollout_paths)?;
    let binary = URL_SAFE_NO_PAD
        .decode(request.binary_base64.as_bytes())
        .map_err(|source| CliError::Parse {
            detail: format!("invalid rollout binary encoding: {source}"),
        })?;
    let main_binary = rollout_paths.main_binary.clone();
    let shadow_binary = rollout_paths.shadow_binary.clone();
    let staged_main_binary = rollout_staged_main_binary(&rollout_paths);
    let staged_shadow_binary = rollout_staged_shadow_binary(&rollout_paths);
    stage_rollout_bytes(&binary, &staged_main_binary)?;
    stage_rollout_bytes(&binary, &staged_shadow_binary)?;

    let current = read_daemon_status(paths)?;
    let previous_main_binary = env::current_exe().map_err(|source| CliError::Io {
        path: "current executable".to_string(),
        detail: source.to_string(),
    })?;
    stage_rollout_main_activation(
        data_dir,
        &rollout_paths,
        RolloutMainActivation {
            source: request.source,
            main_binary: main_binary.clone(),
            shadow_binary: shadow_binary.clone(),
            staged_main_binary: Some(staged_main_binary),
            staged_shadow_binary: Some(staged_shadow_binary),
            previous_main_binary: Some(previous_main_binary),
            main_host: current.host.clone(),
            main_port: current.port,
            shadow_port: request.shadow_port,
            requested_at: request.requested_at,
            mode: "cluster-binary-push".to_string(),
        },
    )?;
    Ok(ClusterRolloutBinaryResponse {
        status: "accepted".to_string(),
        detail: format!(
            "candidate main staged; existing shadow daemon will restart main, verify health, then update shadow (current main pid {} will stop)",
            current.pid
        ),
        main_binary,
        shadow_binary,
    })
}

pub(crate) fn stage_rollout_main_activation(
    data_dir: &Path,
    rollout_paths: &SelfRolloutPaths,
    activation: RolloutMainActivation,
) -> Result<(), CliError> {
    let shadow_paths = DaemonPaths::with_root(data_dir.join("shadow-daemon"));
    if !matches!(
        probe_daemon_status(&shadow_paths, CLUSTER_HEALTH_PROBE_TIMEOUT),
        DaemonProbeStatus::Running(_)
    ) {
        return Err(CliError::DaemonNotRunning {
            detail: "safe rollout requires the existing shadow daemon to be running".to_string(),
        });
    }
    write_main_activation(rollout_paths, &activation)?;
    record_main_activation_staged(rollout_paths, &activation)?;
    Ok(())
}

pub(crate) fn handle_daemon_rollout_diagnostics(paths: &DaemonPaths) -> Result<String, CliError> {
    let data_dir = paths.root.parent().ok_or_else(|| CliError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let rollout_paths = SelfRolloutPaths::new(data_dir);
    let mut output = String::new();
    output.push_str(&format!("rollout root: {}\n", rollout_paths.root.display()));
    append_rollout_diagnostic_file(&mut output, "state", &rollout_paths.state)?;
    append_rollout_diagnostic_file(&mut output, "build log", &rollout_paths.build_log)?;
    append_rollout_diagnostic_file(
        &mut output,
        "pending main",
        &rollout_main_activation_path(&rollout_paths),
    )?;
    append_rollout_diagnostic_file(
        &mut output,
        "pending shadow",
        &sinomo_rollout::rollout_shadow_finalize_path(&rollout_paths),
    )?;
    append_rollout_diagnostic_file(
        &mut output,
        "last main exit",
        &rollout_paths.root.join("last-main-exit.json"),
    )?;

    if let Ok(Some(status)) = read_cluster_rollout_status(&rollout_paths) {
        if let Some(path) = status.diagnostics.as_deref() {
            append_rollout_diagnostic_file(&mut output, "failure diagnostics", Path::new(path))?;
        }
        if let Some(path) = status.stdout_log.as_deref() {
            append_rollout_diagnostic_file(&mut output, "candidate stdout", Path::new(path))?;
        }
        if let Some(path) = status.stderr_log.as_deref() {
            append_rollout_diagnostic_file(&mut output, "candidate stderr", Path::new(path))?;
        }
    }

    let logs_dir = rollout_paths.root.join("logs");
    if logs_dir.exists() {
        output.push_str("\n[rollout logs]\n");
        let mut entries = fs::read_dir(&logs_dir)
            .map_err(|source| CliError::Io {
                path: logs_dir.display().to_string(),
                detail: source.to_string(),
            })?
            .filter_map(Result::ok)
            .filter_map(|entry| {
                let metadata = entry.metadata().ok()?;
                Some((entry.path(), metadata.modified().ok(), metadata.len()))
            })
            .collect::<Vec<_>>();
        entries.sort_by_key(|(_, modified, _)| *modified);
        for (path, _, len) in entries.into_iter().rev().take(8) {
            output.push_str(&format!("{} ({} bytes)\n", path.display(), len));
        }
    }

    Ok(output)
}

pub(crate) fn append_rollout_diagnostic_file(
    output: &mut String,
    label: &str,
    path: &Path,
) -> Result<(), CliError> {
    output.push_str(&format!("\n[{label}] {}\n", path.display()));
    match read_tail_lossy(path, ROLLOUT_DIAGNOSTIC_TAIL_BYTES)? {
        Some(content) if !content.is_empty() => output.push_str(&content),
        Some(_) => output.push_str("<empty>\n"),
        None => output.push_str("<missing>\n"),
    }
    if !output.ends_with('\n') {
        output.push('\n');
    }
    Ok(())
}

pub(crate) fn handle_daemon_finalize_shadow_rollout(
    paths: &DaemonPaths,
) -> Result<String, CliError> {
    let data_dir = paths.root.parent().ok_or_else(|| CliError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let rollout_paths = SelfRolloutPaths::new(data_dir);
    let Some(finalize) = read_pending_shadow_rollout(&rollout_paths)? else {
        return Ok("no pending shadow rollout\n".to_string());
    };

    let shadow_paths = DaemonPaths::with_root(data_dir.join("shadow-daemon"));
    if let DaemonProbeStatus::Running(status) =
        probe_daemon_status(&shadow_paths, DAEMON_PROBE_TIMEOUT)
    {
        let _ = send_daemon_command(&status.address, "SHUTDOWN", DAEMON_PROBE_TIMEOUT);
        let deadline = Instant::now() + Duration::from_secs(15);
        while Instant::now() < deadline {
            if matches!(
                probe_daemon_status(&shadow_paths, DAEMON_PROBE_TIMEOUT),
                DaemonProbeStatus::NotRunning
            ) {
                break;
            }
            thread::sleep(Duration::from_millis(100));
        }
    }

    let mut detail = "shadow rollout finalized".to_string();
    let mut status = "accepted";
    let previous_shadow_backup = finalize
        .staged_shadow_binary
        .as_ref()
        .map(|staged| activate_staged_rollout_binary(staged, &finalize.shadow_binary))
        .transpose()?
        .flatten();
    let candidate_result = spawn_shadow_for_cluster_rollout(
        &finalize.shadow_binary,
        &finalize.main_binary,
        &finalize.main_host,
        finalize.main_port,
        &finalize.shadow_host,
        finalize.shadow_port,
    )
    .and_then(|()| {
        wait_for_shadow_rollout_health(
            data_dir,
            &finalize.shadow_host,
            finalize.shadow_port,
            Duration::from_secs(30),
        )
    });
    if let Err(error) = candidate_result {
        if wait_for_shadow_rollout_health(
            data_dir,
            &finalize.shadow_host,
            finalize.shadow_port,
            Duration::from_secs(15),
        )
        .is_ok()
        {
            detail = format!("shadow rollout finalized after handoff grace period: {error}");
        } else {
            status = "recovered";
            detail = format!("candidate shadow failed: {error}; restarting previous shadow");
            let recovery_shadow_binary = if let Some(backup) = previous_shadow_backup.as_ref() {
                let _ = restore_rollout_binary_from_backup(backup, &finalize.shadow_binary);
                &finalize.shadow_binary
            } else {
                &finalize.previous_shadow_binary
            };
            let _ = spawn_shadow_for_cluster_rollout(
                recovery_shadow_binary,
                &finalize.main_binary,
                &finalize.main_host,
                finalize.main_port,
                &finalize.shadow_host,
                finalize.shadow_port,
            );
        }
    }

    record_shadow_rollout_finalized(&rollout_paths, &finalize, status, &detail)?;
    remove_shadow_finalize(&rollout_paths);
    Ok(format!("{status}: {detail}\n"))
}

pub(crate) fn handle_daemon_rollout_build(
    paths: &DaemonPaths,
    payload: &str,
) -> Result<ClusterRolloutBinaryResponse, CliError> {
    let request: ClusterRolloutBuildRequest =
        serde_json::from_str(payload).map_err(|source| CliError::Json {
            path: "cluster rollout build request".to_string(),
            detail: source.to_string(),
        })?;
    let data_dir = paths.root.parent().ok_or_else(|| CliError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let rollout_paths = SelfRolloutPaths::new(data_dir);
    ensure_rollout_layout(&rollout_paths)?;
    let repo = rollout_remote_repo_path(&rollout_paths, request.remote_repo.as_deref());
    if !repo.join("Cargo.toml").exists() {
        return Ok(ClusterRolloutBinaryResponse {
            status: "rejected".to_string(),
            detail: format!(
                "remote source repo is not initialized at {}; run `sinomo rollout init-source --repo <path>` on this node or pass --remote-repo <path>",
                repo.display()
            ),
            main_binary: PathBuf::new(),
            shadow_binary: PathBuf::new(),
        });
    }

    let built_binary = match rollout_build_dev_binary(&repo, &rollout_paths.build_log) {
        Ok(binary) => binary,
        Err(error) => {
            return Ok(ClusterRolloutBinaryResponse {
                status: "rejected".to_string(),
                detail: format!("remote build failed: {error}"),
                main_binary: PathBuf::new(),
                shadow_binary: PathBuf::new(),
            });
        }
    };
    let main_binary = rollout_paths.main_binary.clone();
    let shadow_binary = rollout_paths.shadow_binary.clone();
    let staged_main_binary = rollout_staged_main_binary(&rollout_paths);
    let staged_shadow_binary = rollout_staged_shadow_binary(&rollout_paths);
    stage_rollout_file(&built_binary, &staged_main_binary)?;
    stage_rollout_file(&built_binary, &staged_shadow_binary)?;

    let current = read_daemon_status(paths)?;
    let previous_main_binary = env::current_exe().map_err(|source| CliError::Io {
        path: "current executable".to_string(),
        detail: source.to_string(),
    })?;
    let source = request.source.clone();
    let requested_at = request.requested_at.clone();
    stage_rollout_main_activation(
        data_dir,
        &rollout_paths,
        RolloutMainActivation {
            source: source.clone(),
            main_binary: main_binary.clone(),
            shadow_binary: shadow_binary.clone(),
            staged_main_binary: Some(staged_main_binary),
            staged_shadow_binary: Some(staged_shadow_binary),
            previous_main_binary: Some(previous_main_binary),
            main_host: current.host.clone(),
            main_port: current.port,
            shadow_port: request.shadow_port,
            requested_at: requested_at.clone(),
            mode: "cluster-remote-build".to_string(),
        },
    )?;
    write_json(
        &rollout_paths.state,
        &serde_json::json!({
            "source": source,
            "source_repo": repo,
            "main_binary": main_binary,
            "shadow_binary": shadow_binary,
            "built_binary": built_binary,
            "build_log": rollout_paths.build_log,
            "requested_at": requested_at,
            "staged_at": OffsetDateTime::now_utc().format(&Rfc3339).unwrap_or_else(|_| "unknown".to_string()),
            "status": "main-staged",
            "mode": "cluster-remote-build"
        }),
    )?;
    Ok(ClusterRolloutBinaryResponse {
        status: "accepted".to_string(),
        detail: format!(
            "remote build staged; existing shadow daemon will restart main, verify health, then update shadow (current main pid {} will stop)",
            current.pid
        ),
        main_binary,
        shadow_binary,
    })
}

pub(crate) fn rollout_artifact_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!("{}-{nanos}", std::process::id())
}

pub(crate) fn spawn_shadow_for_cluster_rollout(
    shadow_binary: &Path,
    main_binary: &Path,
    main_host: &str,
    main_port: u16,
    shadow_host: &str,
    shadow_port: u16,
) -> Result<(), CliError> {
    Command::new(shadow_binary)
        .args(["daemon", "shadow", "run"])
        .arg("--host")
        .arg(shadow_host)
        .arg("--port")
        .arg(shadow_port.to_string())
        .env(SHADOW_MAIN_BINARY_ENV, main_binary)
        .env(SHADOW_MAIN_HOST_ENV, main_host)
        .env(SHADOW_MAIN_PORT_ENV, main_port.to_string())
        .env(SHADOW_START_DELAY_MS_ENV, "0")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|source| CliError::Io {
            path: shadow_binary.display().to_string(),
            detail: source.to_string(),
        })?;
    Ok(())
}

pub(crate) fn wait_for_shadow_rollout_health(
    data_dir: &Path,
    shadow_host: &str,
    shadow_port: u16,
    timeout: Duration,
) -> Result<(), CliError> {
    let shadow_paths = DaemonPaths::with_root(data_dir.join("shadow-daemon"));
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if matches!(
            probe_daemon_status(&shadow_paths, Duration::from_secs(2)),
            DaemonProbeStatus::Running(_)
        ) {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(100));
    }
    Err(CliError::Network {
        address: daemon_connect_address(shadow_host, shadow_port),
        detail: "rolled out shadow daemon did not become ready".to_string(),
    })
}
