use super::*;
use sinomo_runtime::cli_registry::{
    CliCommandMatches, CliCommandPolicy, CliCommandSpec, CliOptionSpec, CliParseError, CliValueSpec,
};
#[allow(unused_imports)]
use sinomo_runtime::cluster;
#[allow(unused_imports)]
use sinomo_runtime::cluster::ClusterKvPathsExt;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DaemonCliCommand {
    Run(DaemonRunOptions),
    Start(DaemonRunOptions),
    Status(DaemonStatusOptions),
    Stop(DaemonRunOptions),
    ShadowRun(DaemonRunOptions),
    ShadowStart(DaemonRunOptions),
    ShadowStatus(DaemonStatusOptions),
    ShadowStop(DaemonStatusOptions),
    CompleteShadowRollout,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DaemonStatusOptions {
    pub(crate) endpoint: DaemonRunOptions,
    pub(crate) explicit_address: bool,
}

pub(crate) fn print_daemon_platform(status: &DaemonStatus) {
    if let (Some(target_os), Some(target_arch)) = (&status.target_os, &status.target_arch) {
        println!("  platform: {target_os}/{target_arch}");
    }
}

pub(crate) fn daemon_cli_command_handler() -> DeclarativeCommandHandler<DaemonCliCommand> {
    DeclarativeCommandHandler::new(
        "daemon",
        CliCommandPolicy::default(),
        daemon_cli_spec,
        parse_daemon_matches,
        run_daemon_cli_command,
    )
}

fn daemon_cli_spec() -> CliCommandSpec {
    let daemon_action = |name: &'static str, about: &'static str| {
        CliCommandSpec::new(name, about).with_options([
            CliOptionSpec::value(
                "--host",
                "daemon bind host",
                CliValueSpec::new("host", "host"),
            ),
            CliOptionSpec::value("--port", "daemon port", CliValueSpec::new("port", "port")),
            CliOptionSpec::flag(
                "--no-port-fallback",
                "fail instead of selecting another port when the preferred port is unavailable",
            ),
        ])
    };
    CliCommandSpec::new("daemon", "control local Sinomo daemons")
        .help_on_empty()
        .with_children([
            daemon_action("run", "run the main daemon in the foreground"),
            daemon_action("start", "start the main daemon detached"),
            daemon_action("status", "show main daemon status"),
            daemon_action("stop", "stop the main daemon"),
            CliCommandSpec::new("shadow", "control the shadow daemon")
                .help_on_empty()
                .with_children([
                    daemon_action("run", "run the shadow daemon in the foreground"),
                    daemon_action("start", "start the shadow daemon detached"),
                    daemon_action("status", "show shadow daemon status"),
                    daemon_action("stop", "stop the shadow daemon"),
                ]),
            CliCommandSpec::new(
                "__complete-shadow-rollout",
                "complete a deferred shadow rollout",
            )
            .hidden(),
        ])
}

fn daemon_options_from_matches(
    matches: &CliCommandMatches,
    mut options: DaemonRunOptions,
) -> Result<DaemonRunOptions, CliParseError> {
    if let Some(host) = matches.value("--host") {
        options.host = host.to_string();
    }
    if let Some(port) = matches.value("--port") {
        options.port = port.parse::<u16>().map_err(|source| CliParseError::Parse {
            detail: format!("invalid daemon port `{port}`: {source}"),
        })?;
        options.port_fallback = false;
    }
    if matches.contains("--no-port-fallback") {
        options.port_fallback = false;
    }
    Ok(options)
}

fn daemon_status_options_from_matches(
    matches: &CliCommandMatches,
    options: DaemonRunOptions,
) -> Result<DaemonStatusOptions, CliParseError> {
    let explicit_address = matches.value("--host").is_some() || matches.value("--port").is_some();
    Ok(DaemonStatusOptions {
        endpoint: daemon_options_from_matches(matches, options)?,
        explicit_address,
    })
}

fn parse_daemon_matches(matches: &CliCommandMatches) -> Result<DaemonCliCommand, CliParseError> {
    let path = matches
        .path()
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let main = || {
        DaemonRunOptions::main_from_environment()
            .map_err(|detail| CliParseError::Parse { detail })
            .and_then(|options| daemon_options_from_matches(matches, options))
    };
    let shadow = || {
        DaemonRunOptions::shadow_from_environment()
            .map_err(|detail| CliParseError::Parse { detail })
            .and_then(|options| daemon_options_from_matches(matches, options))
    };
    let main_status = || {
        DaemonRunOptions::main_from_environment()
            .map_err(|detail| CliParseError::Parse { detail })
            .and_then(|options| daemon_status_options_from_matches(matches, options))
    };
    let shadow_status = || {
        DaemonRunOptions::shadow_from_environment()
            .map_err(|detail| CliParseError::Parse { detail })
            .and_then(|options| daemon_status_options_from_matches(matches, options))
    };
    match path.as_slice() {
        ["daemon", "run"] => main().map(DaemonCliCommand::Run),
        ["daemon", "start"] => main().map(DaemonCliCommand::Start),
        ["daemon", "status"] => main_status().map(DaemonCliCommand::Status),
        ["daemon", "stop"] => main().map(DaemonCliCommand::Stop),
        ["daemon", "shadow", "run"] => shadow().map(DaemonCliCommand::ShadowRun),
        ["daemon", "shadow", "start"] => shadow().map(DaemonCliCommand::ShadowStart),
        ["daemon", "shadow", "status"] => shadow_status().map(DaemonCliCommand::ShadowStatus),
        ["daemon", "shadow", "stop"] => shadow_status().map(DaemonCliCommand::ShadowStop),
        ["daemon", "__complete-shadow-rollout"] => Ok(DaemonCliCommand::CompleteShadowRollout),
        _ => Err(CliParseError::Parse {
            detail: "daemon requires a subcommand".to_string(),
        }),
    }
}

#[cfg(test)]
pub(crate) fn parse_daemon_schema_for_test(args: &[String]) -> Result<DaemonCliCommand, CliError> {
    parse_schema_command_for_test(daemon_cli_spec(), args, parse_daemon_matches)
}

pub(crate) fn run_daemon_cli_command(command: DaemonCliCommand) -> Result<(), CliError> {
    match command {
        DaemonCliCommand::Run(options) => run_daemon(options),
        DaemonCliCommand::Start(options) => start_daemon(options),
        DaemonCliCommand::Status(options) => print_daemon_status(options),
        DaemonCliCommand::Stop(options) => stop_daemon(options),
        DaemonCliCommand::ShadowRun(options) => run_shadow_daemon(options),
        DaemonCliCommand::ShadowStart(options) => start_shadow_daemon(options),
        DaemonCliCommand::ShadowStatus(options) => print_shadow_daemon_status(options),
        DaemonCliCommand::ShadowStop(options) => stop_shadow_daemon(options),
        DaemonCliCommand::CompleteShadowRollout => complete_shadow_rollout_from_helper(),
    }
}

pub(crate) fn daemon_paths_from_defaults() -> Result<DaemonPaths, CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    Ok(DaemonPaths::new(&data_dir))
}

pub(crate) fn default_main_daemon_status() -> Result<Option<DaemonStatus>, CliError> {
    if let Some(status) = cli_default_main_daemon_status() {
        return Ok(Some(status));
    }
    let paths = daemon_paths_from_defaults()?;
    Ok(probe_daemon_status_until_settled(
        DAEMON_LOCKED_DISCOVERY_TIMEOUT,
        DAEMON_LOCKED_REPROBE_INTERVAL,
        |timeout| probe_daemon_status(&paths, timeout),
    ))
}

pub(crate) fn probe_daemon_status_until_settled(
    locked_discovery_timeout: Duration,
    retry_interval: Duration,
    mut probe: impl FnMut(Duration) -> DaemonProbeStatus,
) -> Option<DaemonStatus> {
    let deadline = Instant::now() + locked_discovery_timeout;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let attempt_timeout = DAEMON_PROBE_TIMEOUT.min(remaining.max(Duration::from_millis(1)));
        match probe(attempt_timeout) {
            DaemonProbeStatus::Running(status) => return Some(status),
            DaemonProbeStatus::NotRunning => return None,
            DaemonProbeStatus::Locked { .. } if Instant::now() < deadline => {
                std::thread::sleep(
                    retry_interval.min(deadline.saturating_duration_since(Instant::now())),
                );
            }
            DaemonProbeStatus::Locked { .. } => return None,
        }
    }
}

/// Probe the main daemon that owns `data_dir` — not the env/home default.
/// Callers operating on a foreign data directory (tests, tooling) must never
/// observe another data dir's daemon as their own.
pub(crate) fn main_daemon_status_for_data_dir(
    data_dir: &SinomoDataDir,
) -> Result<Option<DaemonStatus>, CliError> {
    Ok(
        match probe_daemon_status(&DaemonPaths::new(data_dir), DAEMON_PROBE_TIMEOUT) {
            DaemonProbeStatus::Running(status) => Some(status),
            DaemonProbeStatus::Locked { .. } | DaemonProbeStatus::NotRunning => None,
        },
    )
}

pub(crate) fn shadow_daemon_paths_from_defaults() -> Result<DaemonPaths, CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    Ok(DaemonPaths::for_shadow(&data_dir))
}

pub(crate) fn complete_shadow_rollout_from_helper() -> Result<(), CliError> {
    std::thread::sleep(std::time::Duration::from_secs(2));
    let response =
        daemon_handle_deferred_shadow_rollout_completion(&daemon_paths_from_defaults()?)?;
    print!("{response}");
    Ok(())
}

pub(crate) fn run_daemon(options: DaemonRunOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    let registration = daemon_cluster_registration_if_initialized(daemon_cluster_registration(
        DaemonClusterProcess::Main,
        options.clone(),
        DaemonRunOptions::shadow_default(),
    ))?;
    run_daemon_with_paths(options, DaemonPaths::new(&data_dir), "daemon", registration)
}

/// Run the main daemon as a foreground child owned by the graphical app.
///
/// The process lifecycle is supplied by the small `sinomo-app-daemon` wrapper; this function
/// deliberately changes only the product host role. It keeps the same data directory, lock,
/// cluster identity, service routes, and authority paths as a normal main daemon.
pub(crate) fn run_app_owned_daemon_host(options: DaemonRunOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    let registration = daemon_cluster_registration_if_initialized(daemon_cluster_registration(
        DaemonClusterProcess::Main,
        options.clone(),
        DaemonRunOptions::shadow_default(),
    ))?;
    run_daemon_with_paths_and_profile(
        options,
        DaemonPaths::new(&data_dir),
        "daemon",
        registration,
        sinomo_product_profile::ProductComponentProfile::embedded,
    )
}

pub(crate) fn run_shadow_daemon(options: DaemonRunOptions) -> Result<(), CliError> {
    daemon_sleep_for_shadow_start_delay()?;
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    let shadow_paths = DaemonPaths::for_shadow(&data_dir);
    let main_paths = DaemonPaths::new(&data_dir);
    let rollout_paths = SelfRolloutPaths::new(&data_dir.root());
    sinomo_runtime::rollout_host::resume_pending_pid1_shadow_boot(&rollout_paths, &options)?;
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
    let listener = daemon_prepare_daemon_control_listener(options)?;
    let options = listener.options().clone();
    let registration = registration.map(|mut registration| {
        registration.shadow_options = options.clone();
        registration
    });
    let cluster = registration
        .as_ref()
        .map(|registration| {
            register_shadow_daemon_cluster_node_best_effort(&shadow_paths, registration)
        })
        .transpose()?
        .flatten();
    let data_dir_root = shadow_paths.root.parent().ok_or_else(|| CliError::Io {
        path: shadow_paths.root.display().to_string(),
        detail: "shadow daemon paths have no data directory parent".to_string(),
    })?;
    let data_dir = SinomoDataDir::new(data_dir_root);
    let component_catalog = sinomo_product_profile::base_component_catalog(
        &data_dir,
        sinomo_product_profile::ProductComponentProfile::shadow(cluster.is_some()),
    )
    .map_err(|error| CliError::Parse {
        detail: format!("build shadow product component profile: {error}"),
    })?;
    daemon_run_shadow_daemon_supervision_loop_with_prepared_listener(
        listener,
        component_catalog,
        shadow_paths,
        main_paths,
        rollout_paths,
        main_options,
        main_binary,
        cluster,
        DAEMON_ROLLOUT_TIMEOUT,
        DAEMON_PROBE_TIMEOUT,
    )
    .map_err(Into::into)
}

fn register_shadow_daemon_cluster_node_best_effort(
    shadow_paths: &DaemonPaths,
    registration: &DaemonClusterRegistration,
) -> Result<Option<DaemonClusterStatus>, CliError> {
    let data_dir = shadow_paths.root.parent().ok_or_else(|| CliError::Io {
        path: shadow_paths.root.display().to_string(),
        detail: "shadow daemon paths have no data directory parent".to_string(),
    })?;
    let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
    match daemon_cluster_status_from_local_state(&cluster_paths, registration) {
        Ok(cluster) => Ok(Some(cluster)),
        Err(error) => {
            eprintln!(
                "sinomo shadow daemon local cluster identity unavailable: {error}; continuing unclustered"
            );
            Ok(None)
        }
    }
}

pub(crate) fn run_daemon_with_paths(
    options: DaemonRunOptions,
    paths: DaemonPaths,
    label: &str,
    registration: Option<DaemonClusterRegistration>,
) -> Result<(), CliError> {
    run_daemon_with_paths_and_profile(options, paths, label, registration, |cluster_initialized| {
        if label == "daemon" {
            sinomo_product_profile::ProductComponentProfile::main(cluster_initialized)
        } else {
            sinomo_product_profile::ProductComponentProfile::shadow(cluster_initialized)
        }
    })
}

fn run_daemon_with_paths_and_profile(
    options: DaemonRunOptions,
    paths: DaemonPaths,
    label: &str,
    registration: Option<DaemonClusterRegistration>,
    product_profile: impl FnOnce(bool) -> sinomo_product_profile::ProductComponentProfile,
) -> Result<(), CliError> {
    let listener = daemon_prepare_daemon_control_listener(options)?;
    let options = listener.options().clone();
    let registration = registration.map(|mut registration| {
        match registration.process {
            DaemonClusterProcess::Main => registration.main_options = options.clone(),
            DaemonClusterProcess::Shadow => registration.shadow_options = options.clone(),
        }
        registration
    });
    let cluster = registration
        .map(|registration| register_daemon_cluster_node(&registration))
        .transpose()?;
    let data_dir = paths.root.parent().ok_or_else(|| CliError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let data_dir = SinomoDataDir::new(data_dir);
    let profile = product_profile(cluster.is_some());
    let base_catalog =
        sinomo_product_profile::base_component_catalog(&data_dir, profile).map_err(|error| {
            CliError::Parse {
                detail: format!("build daemon product component profile: {error}"),
            }
        })?;
    let mut component_factories = vec![
        cli_agent_component_factory(&options)?,
        audio_component_factory()?,
    ];
    if label == "daemon" {
        component_factories.push(
            crate::dfs_mount_supervisor::sifs_mount_supervisor_component_factory(
                data_dir,
                paths.clone(),
            ),
        );
    }
    let component_catalog =
        sinomo_product_profile::with_components(&base_catalog, component_factories).map_err(
            |error| CliError::Parse {
                detail: format!("extend daemon product component profile: {error}"),
            },
        )?;
    daemon_run_daemon_control_loop_with_prepared_listener(
        listener,
        paths,
        label,
        cluster,
        component_catalog,
        sinomo_runtime::daemon::DaemonRuntimeConfig::default(),
    )
    .map_err(Into::into)
}

fn cli_agent_component_factory(
    options: &DaemonRunOptions,
) -> Result<sinomo_runtime::daemon::DaemonComponentFactory, CliError> {
    crate::hosted_agent_chat_component_factory(daemon_connect_address(&options.host, options.port))
        .map_err(|detail| CliError::Parse { detail })
}

fn audio_component_factory() -> Result<sinomo_runtime::daemon::DaemonComponentFactory, CliError> {
    let factory = sinomo_runtime::module::StaticComponentFactory::new(
        sinomo_runtime::module::ComponentDescriptor::new("sinomo-audio", env!("CARGO_PKG_VERSION")),
        |_, scope| {
            scope.set_services(
                sinomo_audio::service::register_services(
                    sinomo_runtime::service::ServiceRegistry::builder(),
                )
                .build(),
            );
            Ok(())
        },
    )
    .map_err(|error| CliError::Parse {
        detail: error.to_string(),
    })?;
    Ok(Arc::new(factory))
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
    let invoked_exe = env::current_exe().map_err(|source| CliError::Io {
        path: "current executable".to_string(),
        detail: source.to_string(),
    })?;
    let main_binary = daemon_main_binary_for_shadow_start(&invoked_exe);
    let main_options = default_main_daemon_status()?
        .as_ref()
        .map(daemon_options_from_status)
        .unwrap_or_default();
    let extra_env = vec![
        (SHADOW_MAIN_BINARY_ENV, main_binary.display().to_string()),
        (SHADOW_MAIN_HOST_ENV, main_options.host),
        (SHADOW_MAIN_PORT_ENV, main_options.port.to_string()),
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

pub(crate) fn print_daemon_status(options: DaemonStatusOptions) -> Result<(), CliError> {
    print_daemon_status_with_paths(options, daemon_paths_from_defaults()?, "daemon")
}

pub(crate) fn print_shadow_daemon_status(options: DaemonStatusOptions) -> Result<(), CliError> {
    print_daemon_status_with_paths(
        options,
        shadow_daemon_paths_from_defaults()?,
        "shadow daemon",
    )
}

pub(crate) fn print_daemon_status_with_paths(
    options: DaemonStatusOptions,
    paths: DaemonPaths,
    label: &str,
) -> Result<(), CliError> {
    if options.explicit_address {
        let address = daemon_connect_address(&options.endpoint.host, options.endpoint.port);
        let status = daemon_fetch_daemon_status_from_address(&address, DAEMON_PROBE_TIMEOUT)?;
        print_running_daemon_status(&status, label);
        println!("  target: {address}");
        return Ok(());
    }

    match probe_daemon_status(&paths, DAEMON_PROBE_TIMEOUT) {
        DaemonProbeStatus::Running(status) => {
            print_running_daemon_status(&status, label);
            println!("  status: {}", paths.status.display());
            Ok(())
        }
        DaemonProbeStatus::Locked { status } => {
            println!("sinomo {label} lock is held but the listener did not answer");
            if let Some(status) = status {
                println!("  pid: {}", status.pid);
                println!("  version: {}", status.version);
                if let Some(commit) = &status.commit {
                    println!("  commit: {commit}");
                }
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
                println!(
                    "  default bind: {}:{}",
                    options.endpoint.host, options.endpoint.port
                );
                println!(
                    "  default control address: {}",
                    daemon_connect_address(&options.endpoint.host, options.endpoint.port)
                );
            }
            println!("  lock: {}", paths.lock.display());
            Ok(())
        }
        DaemonProbeStatus::NotRunning => {
            println!("sinomo {label} is not running");
            println!(
                "  default bind: {}:{}",
                options.endpoint.host, options.endpoint.port
            );
            println!(
                "  default control address: {}",
                daemon_connect_address(&options.endpoint.host, options.endpoint.port)
            );
            Ok(())
        }
    }
}

fn print_running_daemon_status(status: &DaemonStatus, label: &str) {
    println!("sinomo {label} is running");
    println!("  pid: {}", status.pid);
    println!("  version: {}", status.version);
    if let Some(commit) = &status.commit {
        println!("  commit: {commit}");
    }
    println!("  bind: {}", daemon_bind_address(status));
    println!("  control address: {}", status.address);
    print_daemon_platform(status);
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
    if let Some(stats) = &status.idle_stats {
        print_daemon_idle_stats(stats);
    }
    println!("  lock: {}", status.lock_file.display());
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

fn print_daemon_idle_stats(stats: &IdleStatsSnapshot) {
    println!("  idle stats (cumulative since daemon start):");
    println!(
        "    disk io: read {} write {}",
        format_bytes(stats.disk_bytes_read),
        format_bytes(stats.disk_bytes_written)
    );
    println!(
        "    net io: sent {} recv {}",
        format_bytes(stats.net_bytes_sent),
        format_bytes(stats.net_bytes_recv)
    );
    println!(
        "    ticks: {} total, {} with work",
        stats.ticks_total, stats.ticks_with_work
    );
    println!(
        "    disk reads: cluster_state={} iroh_endpoints={} dfs_metadata={}",
        stats.disk_reads_cluster_state,
        stats.disk_reads_iroh_endpoints,
        stats.disk_reads_dfs_metadata
    );
    println!(
        "    object file writes: actual={} skipped={}",
        stats.disk_writes_object_file_actual, stats.disk_writes_object_file_skipped
    );
    println!(
        "    kv snapshot writes: actual={} skipped={}",
        stats.kv_snapshot_writes_actual, stats.kv_snapshot_writes_skipped
    );
    println!(
        "    kv publishes: member={} node_status={} network_info={}",
        stats.kv_publish_member, stats.kv_publish_node_status, stats.kv_publish_network_info
    );
    println!(
        "    connectivity record sets written: {}",
        stats.connectivity_record_sets_written
    );
    println!(
        "    mdns announces: sent={} recv={}",
        stats.mdns_announces_sent, stats.mdns_announces_recv
    );
    println!(
        "    raft appends: sent={} recv={}",
        stats.raft_appends_sent, stats.raft_appends_recv
    );
    println!(
        "    connectivity probes sent: {}",
        stats.connectivity_probes_sent
    );
    println!(
        "    dfs metadata replications sent: {}",
        stats.dfs_metadata_replications_sent
    );
}

pub(crate) fn stop_daemon(options: DaemonRunOptions) -> Result<(), CliError> {
    stop_daemon_with_paths(daemon_paths_from_defaults()?, options, "daemon")
}

pub(crate) fn stop_shadow_daemon(options: DaemonStatusOptions) -> Result<(), CliError> {
    if options.explicit_address {
        let body = stop_explicit_shadow_daemon(&options.endpoint)?;
        println!("{}", body.trim());
        return Ok(());
    }
    stop_daemon_with_paths(
        shadow_daemon_paths_from_defaults()?,
        options.endpoint,
        "shadow daemon",
    )
}

fn stop_explicit_shadow_daemon(options: &DaemonRunOptions) -> Result<String, CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    let cluster_paths = ClusterPaths::new(&data_dir);
    let state = cluster::read_cluster_state_without_kv(&cluster_paths)?;
    stop_explicit_shadow_daemon_with(
        &state,
        options,
        |address| {
            daemon_fetch_daemon_status_from_address(address, DAEMON_PROBE_TIMEOUT)
                .map_err(Into::into)
        },
        |address| {
            send_daemon_remote_shadow_shutdown_request(address, DAEMON_PROBE_TIMEOUT)
                .map_err(Into::into)
        },
    )
}

fn stop_explicit_shadow_daemon_with(
    state: &ClusterBootstrapState,
    options: &DaemonRunOptions,
    mut fetch_status: impl FnMut(&str) -> Result<DaemonStatus, CliError>,
    mut send_shutdown: impl FnMut(&str) -> Result<String, CliError>,
) -> Result<String, CliError> {
    let address = daemon_connect_address(&options.host, options.port);
    let mut matching = state
        .members
        .iter()
        .flat_map(|member| member.endpoints.iter())
        .filter(|endpoint| endpoint.address == address);
    let endpoint = matching.next().ok_or_else(|| CliError::Parse {
        detail: format!(
            "refusing to stop unregistered remote shadow endpoint {address}; no exact cluster endpoint is recorded"
        ),
    })?;
    if !network::cluster_endpoint_identifies_shadow_daemon_for_node(endpoint)
        || matching.any(|candidate| {
            candidate.node_id != endpoint.node_id
                || !network::cluster_endpoint_identifies_shadow_daemon_for_node(candidate)
        })
    {
        return Err(CliError::Parse {
            detail: format!(
                "refusing to stop {address}; the exact endpoint is not uniquely classified as daemon-shadow"
            ),
        });
    }

    let status = fetch_status(&address)?;
    let cluster = status.cluster.as_ref().ok_or_else(|| CliError::Parse {
        detail: format!(
            "refusing to stop {address}; the endpoint did not report a cluster process identity"
        ),
    })?;
    if cluster.cluster_id != state.cluster_id
        || cluster.node_id != endpoint.node_id
        || cluster.process != sinomo_runtime::node::DAEMON_SHADOW_CLUSTER_ROLE
        || status.port != options.port
    {
        return Err(CliError::Parse {
            detail: format!(
                "refusing to stop {address}; expected cluster {} node {} daemon-shadow on port {}, got cluster {} node {} {} on port {}",
                state.cluster_id,
                endpoint.node_id,
                options.port,
                cluster.cluster_id,
                cluster.node_id,
                cluster.process,
                status.port,
            ),
        });
    }
    send_shutdown(&address)
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

pub(crate) fn cli_commit_cluster_kv_request_with_quorum_response(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    request: KvTxnRequest,
) -> Result<ClusterKvTxnCommitResult, network::NetworkError> {
    let mut driver = cli_cluster_control_plane_driver(paths);
    cluster::commit_cluster_kv_txn_with_quorum_response_with_host(
        paths,
        state,
        request,
        cli_cluster_kv_txn_quorum_options(true),
        &mut driver,
    )
}

#[cfg(test)]
pub(crate) fn handle_daemon_cluster_kv_txn_request(
    paths: &DaemonPaths,
    txn: KvTxn,
) -> Result<KvTxnResponse, CliError> {
    let data_dir = paths.root.parent().ok_or_else(|| CliError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
    let state =
        cluster::read_cluster_state_without_kv(&cluster_paths).map_err(|error| match error {
            network::NetworkError::ClusterOrphan => CliError::ClusterReadOnly {
                detail: "local node has no cluster state".to_string(),
            },
            other => other.into(),
        })?;
    let mut state = state;
    let mut driver = cli_cluster_control_plane_driver(&cluster_paths);
    let request = KvTxnRequest::generated(txn);
    let result = cluster::commit_cluster_kv_txn_with_quorum_response_with_host(
        &cluster_paths,
        &mut state,
        request,
        cli_cluster_kv_txn_quorum_options(true),
        &mut driver,
    )?;
    sinomo_runtime::kv::write_imported_cluster_kv_snapshot_if_changed(
        &cluster_paths.kv_paths(),
        &result.snapshot,
        sinomo_runtime::kv::cluster_kv_key_can_defer_materialized_snapshot,
    )?;
    result.txn_response.ok_or_else(|| CliError::Parse {
        detail: "cluster KV returned a barrier result for a transaction request".to_string(),
    })
}

pub(crate) fn cli_cluster_control_plane_driver(
    paths: &ClusterPaths,
) -> cluster::ClusterControlPlaneDriver<CliClusterControlPlaneTransport> {
    cluster::ClusterControlPlaneDriver::with_timeouts(
        CliClusterControlPlaneTransport::new(paths),
        cli_cluster_control_plane_timeouts(),
    )
}

pub(crate) struct CliClusterControlPlaneTransport {
    cluster_paths: ClusterPaths,
}

impl CliClusterControlPlaneTransport {
    fn new(cluster_paths: &ClusterPaths) -> Self {
        Self {
            cluster_paths: cluster_paths.clone(),
        }
    }

    fn matching_data_dir(&self) -> Option<SinomoDataDir> {
        self.cluster_paths.root.parent().map(SinomoDataDir::new)
    }

    fn matching_daemon_paths(&self) -> Option<DaemonPaths> {
        self.matching_data_dir()
            .map(|data_dir| DaemonPaths::new(&data_dir))
    }

    /// Delegate node-routed calls to the daemon that owns this cluster state.
    /// `None` means daemon discovery failed before the request was sent, so
    /// bootstrap workflows may use their direct endpoint path.
    fn call_node_object_rpc_via_matching_daemon(
        &self,
        target_node_id: &NodeId,
        request: &network::ObjectRpcRequest,
        timeout: Duration,
    ) -> Option<Result<network::ObjectRpcResponse, network::NetworkError>> {
        let daemon_paths = self.matching_daemon_paths()?;
        let client = DaemonApiClient::connect_for_paths(&daemon_paths).ok()?;
        let connect_timeout = timeout.min(network::DEFAULT_NETWORK_TIMEOUT);
        Some(
            client
                .object_rpc_to_target_with_deadline_and_affinity(
                    target_node_id,
                    request,
                    connect_timeout,
                    Some(timeout),
                    true,
                    sinomo_runtime::api::DaemonObjectRpcDestinationProcessAffinity::MainDaemon,
                )
                .map_err(|error| {
                    cli_daemon_forward_network_error("daemon-mediated cluster node rpc", error)
                }),
        )
    }

    fn call_node_object_rpc_direct(
        &self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        request: &network::ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<network::ObjectRpcResponse, network::NetworkError> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| network::NetworkError::Io {
                operation: "cli daemon node object rpc runtime".to_string(),
                detail: error.to_string(),
            })?;
        let mut host =
            CliEndpointRpcHost::new(Some(state.cluster_id.clone()), "cli endpoint rpc signing");
        if let Some(data_dir) = self.matching_data_dir() {
            host = host.with_data_dir(data_dir);
        }
        let options = cluster::main_daemon_cluster_rpc_options(
            network::ClusterObjectRpcOptions::default().with_overall_timeout(timeout),
        );
        let mut client = network::ClusterObjectRpcClient::routed_from_fenced_bootstrap_state(
            &mut host,
            &self.cluster_paths,
            state,
        )?;
        let result = runtime.block_on(async {
            let result = client
                .call_target(
                    state,
                    target_node_id,
                    network::ObjectRpcCall::value(request.clone()),
                    options,
                )
                .await?;
            result.into_materialized_value_response(usize::MAX).await
        });
        runtime.shutdown_timeout(Duration::from_millis(250));
        result
    }
}

fn daemon_mediated_result_or_direct_fallback<T, E>(
    daemon_result: Option<Result<T, E>>,
    direct_fallback: impl FnOnce() -> Result<T, E>,
) -> Result<T, E> {
    match daemon_result {
        Some(result) => result,
        None => direct_fallback(),
    }
}

impl cluster::ClusterControlPlaneTransport for CliClusterControlPlaneTransport {
    fn send_kv_replication(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &sinomo_runtime::kv::ClusterKvReplicationRequest,
        timeout: Duration,
    ) -> Result<sinomo_runtime::kv::ClusterKvReplicationResponse, network::NetworkError> {
        cli_send_cluster_raft_append(endpoint, request, timeout)
    }

    fn send_kv_read_index_probes(
        &mut self,
        state: &ClusterBootstrapState,
        targets: Vec<NodeId>,
        request: &sinomo_runtime::kv::ClusterKvReplicationRequest,
        timeout: Duration,
        required_acks: usize,
    ) -> Result<
        Vec<(
            NodeId,
            Result<sinomo_runtime::kv::ClusterKvReplicationResponse, network::NetworkError>,
        )>,
        network::NetworkError,
    > {
        let expected_term = request.append.term;
        let payload =
            sinomo_runtime::kv::cluster_raft_append_rpc_payload(request).map_err(|error| {
                network::NetworkError::Io {
                    operation: "encode concurrent CLI ReadIndex probe".to_string(),
                    detail: error.to_string(),
                }
            })?;
        let rpc_request = network::ObjectRpcRequest::from_encoded_body(
            payload.route,
            payload.category,
            payload.body,
        )
        .with_response_retry_safe();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| network::NetworkError::Io {
                operation: "CLI concurrent ReadIndex runtime".to_string(),
                detail: error.to_string(),
            })?;
        let cluster_paths = self.cluster_paths.clone();
        let data_dir = self.matching_data_dir();
        let state = std::sync::Arc::new(state.clone());
        let request = std::sync::Arc::new(rpc_request);
        let result = runtime.block_on(async move {
            let mut tasks = tokio::task::JoinSet::new();
            for target in targets {
                let cluster_paths = cluster_paths.clone();
                let data_dir = data_dir.clone();
                let state = std::sync::Arc::clone(&state);
                let request = std::sync::Arc::clone(&request);
                tasks.spawn(async move {
                    let mut host = CliEndpointRpcHost::new(
                        Some(state.cluster_id.clone()),
                        "cli concurrent ReadIndex signing",
                    );
                    if let Some(data_dir) = data_dir {
                        host = host.with_data_dir(data_dir);
                    }
                    let mut client =
                        network::ClusterObjectRpcClient::routed(&mut host, &cluster_paths);
                    let result = async {
                        let response = client
                            .call_target(
                                &state,
                                &target,
                                network::ObjectRpcCall::value((*request).clone()),
                                cluster::main_daemon_cluster_rpc_options(
                                    network::ClusterObjectRpcOptions::default()
                                        .with_overall_timeout(timeout),
                                ),
                            )
                            .await?
                            .into_materialized_value_response(usize::MAX)
                            .await?;
                        response.error_detail()?;
                        sinomo_runtime::kv::decode_cluster_raft_append_rpc_response_body(
                            &response.body,
                        )
                        .map_err(|error| network::NetworkError::Io {
                            operation: "decode concurrent CLI ReadIndex response".to_string(),
                            detail: error.to_string(),
                        })
                    }
                    .await;
                    (target, result)
                });
            }

            let mut accepted = 0_usize;
            let mut outcomes = Vec::new();
            let collect = tokio::time::timeout(timeout, async {
                while let Some(joined) = tasks.join_next().await {
                    let (target, result) = joined.map_err(|source| network::NetworkError::Io {
                        operation: "join concurrent CLI ReadIndex probe".to_string(),
                        detail: source.to_string(),
                    })?;
                    if result.as_ref().is_ok_and(|response| {
                        response.accepted && response.append.term == expected_term
                    }) {
                        accepted = accepted.saturating_add(1);
                    }
                    outcomes.push((target, result));
                    if accepted >= required_acks {
                        break;
                    }
                }
                Ok::<(), network::NetworkError>(())
            })
            .await;
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
            if let Ok(result) = collect {
                result?;
            }
            Ok(outcomes)
        });
        runtime.shutdown_timeout(Duration::from_millis(250));
        result
    }

    fn request_kv_state_authority_vote(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &sinomo_runtime::kv::ClusterKvStateAuthorityVoteRequest,
        timeout: Duration,
    ) -> Result<sinomo_runtime::kv::ClusterKvStateAuthorityVoteResponse, network::NetworkError>
    {
        cli_send_cluster_raft_vote(endpoint, request, timeout)
    }

    fn request_kv_state_authority_votes_from_nodes(
        &mut self,
        state: &ClusterBootstrapState,
        targets: Vec<NodeId>,
        request: &sinomo_runtime::kv::ClusterKvStateAuthorityVoteRequest,
        timeout: Duration,
        required_grants: usize,
    ) -> Result<
        Vec<(
            NodeId,
            Result<sinomo_runtime::kv::ClusterKvStateAuthorityVoteResponse, network::NetworkError>,
        )>,
        network::NetworkError,
    > {
        let expected_term = request.vote.term;
        let payload =
            sinomo_runtime::kv::cluster_raft_vote_rpc_payload(request).map_err(|error| {
                network::NetworkError::Io {
                    operation: "encode concurrent CLI Raft vote".to_string(),
                    detail: error.to_string(),
                }
            })?;
        let rpc_request = network::ObjectRpcRequest::from_encoded_body(
            payload.route,
            payload.category,
            payload.body,
        );
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| network::NetworkError::Io {
                operation: "CLI concurrent Raft vote runtime".to_string(),
                detail: error.to_string(),
            })?;
        let cluster_paths = self.cluster_paths.clone();
        let data_dir = self.matching_data_dir();
        let state = std::sync::Arc::new(state.clone());
        let request = std::sync::Arc::new(rpc_request);
        let result = runtime.block_on(async move {
            let mut tasks = tokio::task::JoinSet::new();
            for target in targets {
                let cluster_paths = cluster_paths.clone();
                let data_dir = data_dir.clone();
                let state = std::sync::Arc::clone(&state);
                let request = std::sync::Arc::clone(&request);
                tasks.spawn(async move {
                    let mut host = CliEndpointRpcHost::new(
                        Some(state.cluster_id.clone()),
                        "cli concurrent Raft vote signing",
                    );
                    if let Some(data_dir) = data_dir {
                        host = host.with_data_dir(data_dir);
                    }
                    let mut client =
                        network::ClusterObjectRpcClient::routed(&mut host, &cluster_paths);
                    let result = async {
                        let response = client
                            .call_target(
                                &state,
                                &target,
                                network::ObjectRpcCall::value((*request).clone()),
                                cluster::main_daemon_cluster_rpc_options(
                                    network::ClusterObjectRpcOptions::default()
                                        .with_overall_timeout(timeout),
                                ),
                            )
                            .await?
                            .into_materialized_value_response(usize::MAX)
                            .await?;
                        response.error_detail()?;
                        sinomo_runtime::kv::decode_cluster_raft_vote_rpc_response_body(
                            &response.body,
                        )
                        .map_err(|error| network::NetworkError::Io {
                            operation: "decode concurrent CLI Raft vote response".to_string(),
                            detail: error.to_string(),
                        })
                    }
                    .await;
                    (target, result)
                });
            }
            let mut granted = 0_usize;
            let mut outcomes = Vec::new();
            let collect = tokio::time::timeout(timeout, async {
                while let Some(joined) = tasks.join_next().await {
                    let (target, result) = joined.map_err(|source| network::NetworkError::Io {
                        operation: "join concurrent CLI Raft vote".to_string(),
                        detail: source.to_string(),
                    })?;
                    if result.as_ref().is_ok_and(|response| {
                        response.accepted
                            && response.vote.vote_granted
                            && response.vote.term == expected_term
                    }) {
                        granted = granted.saturating_add(1);
                    }
                    outcomes.push((target, result));
                    if granted >= required_grants {
                        while let Some(joined) = tasks.try_join_next() {
                            let (target, result) =
                                joined.map_err(|source| network::NetworkError::Io {
                                    operation: "join ready concurrent CLI Raft vote".to_string(),
                                    detail: source.to_string(),
                                })?;
                            outcomes.push((target, result));
                        }
                        break;
                    }
                }
                Ok::<(), network::NetworkError>(())
            })
            .await;
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
            if let Ok(result) = collect {
                result?;
            }
            Ok(outcomes)
        });
        runtime.shutdown_timeout(Duration::from_millis(250));
        result
    }

    fn probe_cluster_state_authority(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        timeout: Duration,
    ) -> bool {
        daemon_fetch_daemon_status_from_endpoint(endpoint, timeout).is_ok()
    }

    fn fetch_cluster_kv_snapshot(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        timeout: Duration,
    ) -> Result<KvSnapshot, network::NetworkError> {
        let request = cluster_kv_local_snapshot_rpc_request()
            .map_err(sinomo_runtime::network::NetworkError::directory)?;
        let response = daemon_send_daemon_rpc_to_endpoint(endpoint, &request, timeout)
            .map_err(|error| cli_daemon_rpc_network_error("cluster kv snapshot refresh", error))?;
        decode_cluster_kv_local_snapshot_rpc_response(&response).map(|read| read.value)
    }

    fn call_endpoint_object_rpc(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &network::ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<network::ObjectRpcResponse, network::NetworkError> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| network::NetworkError::Io {
                operation: "cli daemon endpoint object rpc runtime".to_string(),
                detail: error.to_string(),
            })?;
        let state = cluster::read_cluster_state_without_kv(&self.cluster_paths)?;
        let mut host = CliEndpointRpcHost::new(None, "cli endpoint rpc signing");
        if let Some(data_dir) = self.matching_data_dir() {
            host = host.with_data_dir(data_dir);
        }
        let options = network::ClusterObjectRpcOptions::default().with_overall_timeout(timeout);
        let mut client = network::ClusterObjectRpcClient::routed(&mut host, &self.cluster_paths);
        let result = runtime.block_on(async {
            let result = client
                .call_endpoint(
                    &state,
                    endpoint,
                    network::ObjectRpcCall::value(request.clone()),
                    options,
                )
                .await?;
            result.into_materialized_value_response(usize::MAX).await
        });
        runtime.shutdown_timeout(Duration::from_millis(250));
        result
    }

    fn call_node_object_rpc(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        request: &network::ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<network::ObjectRpcResponse, network::NetworkError> {
        daemon_mediated_result_or_direct_fallback(
            self.call_node_object_rpc_via_matching_daemon(target_node_id, request, timeout),
            || self.call_node_object_rpc_direct(state, target_node_id, request, timeout),
        )
    }
}

fn cli_cluster_control_plane_timeouts() -> cluster::ClusterControlPlaneTimeouts {
    let defaults = cluster::ClusterControlPlaneTimeouts::default_daemon_transport();
    cluster::ClusterControlPlaneTimeouts {
        // CLI mutations use a smaller attempt count and total latency budget
        // than daemon background convergence, but share its transport policy.
        max_raft_append_attempts: 3,
        raft_append_timeout: Duration::from_secs(10),
        membership_rpc_timeout: Duration::from_secs(10),
        raft_campaign_jitter_max: Duration::ZERO,
        ..defaults
    }
}

pub(crate) fn cli_cluster_kv_txn_quorum_options(
    allow_forwarding: bool,
) -> cluster::ClusterKvTxnQuorumOptions {
    cli_cluster_control_plane_timeouts()
        .kv_txn_quorum_options(allow_forwarding)
        .with_state_authority_reachability_options(
            cli_cluster_state_authority_reachability_options(),
        )
}

fn cli_send_cluster_raft_append(
    endpoint: &ClusterEndpointRecord,
    request: &sinomo_runtime::kv::ClusterRaftAppendRequest,
    timeout: Duration,
) -> Result<sinomo_runtime::kv::ClusterRaftAppendResponse, network::NetworkError> {
    let payload = sinomo_runtime::kv::cluster_raft_append_rpc_payload(request)
        .map_err(sinomo_runtime::network::NetworkError::directory)?;
    let rpc_request =
        network::ObjectRpcRequest::from_encoded_body(payload.route, payload.category, payload.body);
    let response = daemon_send_daemon_rpc_to_endpoint(endpoint, &rpc_request, timeout)
        .map_err(|error| cli_daemon_rpc_network_error("cluster raft append", error))?;
    response.error_detail()?;
    sinomo_runtime::kv::decode_cluster_raft_append_rpc_response_body(&response.body)
        .map_err(sinomo_runtime::network::NetworkError::directory)
}

fn cli_send_cluster_raft_vote(
    endpoint: &ClusterEndpointRecord,
    request: &sinomo_runtime::kv::ClusterRaftVoteRequest,
    timeout: Duration,
) -> Result<sinomo_runtime::kv::ClusterRaftVoteResponse, network::NetworkError> {
    let payload = sinomo_runtime::kv::cluster_raft_vote_rpc_payload(request)
        .map_err(sinomo_runtime::network::NetworkError::directory)?;
    let rpc_request =
        network::ObjectRpcRequest::from_encoded_body(payload.route, payload.category, payload.body);
    let response = daemon_send_daemon_rpc_to_endpoint(endpoint, &rpc_request, timeout)
        .map_err(|error| cli_daemon_rpc_network_error("cluster raft vote", error))?;
    response.error_detail()?;
    sinomo_runtime::kv::decode_cluster_raft_vote_rpc_response_body(&response.body)
        .map_err(sinomo_runtime::network::NetworkError::directory)
}

fn cli_daemon_rpc_network_error(
    operation: &str,
    error: impl std::fmt::Display,
) -> network::NetworkError {
    network::NetworkError::Io {
        operation: operation.to_string(),
        detail: error.to_string(),
    }
}

fn cli_daemon_forward_network_error(
    operation: &str,
    error: sinomo_runtime::api::DaemonApiError,
) -> network::NetworkError {
    match error {
        sinomo_runtime::api::DaemonApiError::ForwardNetwork {
            detail,
            delivery_state: Some(delivery_state),
        } => network::NetworkError::delivery_failed(operation, delivery_state, detail),
        other => cli_daemon_rpc_network_error(operation, other),
    }
}

#[cfg(test)]
pub(crate) fn handle_daemon_cluster_join_request(
    paths: &DaemonPaths,
    request: ClusterJoinRegistrationRequest,
) -> Result<ClusterJoinRegistrationResponse, CliError> {
    let data_dir = paths.root.parent().ok_or_else(|| CliError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
    cluster::handle_cluster_join_registration_request_with_host(
        &cluster_paths,
        request,
        OffsetDateTime::now_utc(),
        &mut CliClusterJoinRegistrationHost,
    )
    .map_err(Into::into)
}

#[cfg(test)]
struct CliClusterJoinRegistrationHost;

#[cfg(test)]
impl cluster::ClusterJoinRegistrationHost for CliClusterJoinRegistrationHost {
    fn call_node_object_rpc(
        &mut self,
        _state: &ClusterBootstrapState,
        _target_node_id: &NodeId,
        _request: &network::ObjectRpcRequest,
    ) -> Result<network::ObjectRpcResponse, network::NetworkError> {
        Err(network::NetworkError::Io {
            operation: "cluster membership node rpc".to_string(),
            detail: "CLI test join host does not route membership node RPCs".to_string(),
        })
    }

    fn authoritative_cluster_kv_snapshot(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
    ) -> Result<sinomo_runtime::kv::KvAuthoritativeRead<KvSnapshot>, network::NetworkError> {
        cluster::ClusterMemberMutationHost::authoritative_cluster_kv_snapshot(
            &mut super::cluster::CliClusterMemberMutationHost,
            paths,
            state,
        )
    }

    fn commit_join_registration_txn(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        request: sinomo_runtime::kv::KvTxnRequest,
    ) -> Result<sinomo_runtime::kv::ClusterKvTxnCommitResult, network::NetworkError> {
        cluster::ClusterMemberMutationHost::commit_cluster_kv_txn_with_quorum(
            &mut super::cluster::CliClusterMemberMutationHost,
            paths,
            state,
            request,
        )
    }

    fn admit_cluster_raft_learner(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        node_id: &NodeId,
        now: OffsetDateTime,
    ) -> Result<KvSnapshot, network::NetworkError> {
        let mut host = super::cluster::CliClusterMemberMutationHost;
        cluster::update_cluster_state_authority_with_host(
            paths,
            state.clone(),
            node_id.clone(),
            ClusterMemberRole::Learner,
            now,
            &mut host,
        )?;
        cluster::ClusterMemberMutationHost::authoritative_cluster_kv_snapshot(
            &mut host, paths, state,
        )
        .map(|read| read.value)
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    fn explicit_shadow_stop_fixture(
        process: &str,
        status_node_id: NodeId,
    ) -> (ClusterBootstrapState, DaemonRunOptions, DaemonStatus) {
        let timestamp = OffsetDateTime::UNIX_EPOCH;
        let cluster_id = Id::from_legacy_named("cluster_explicit_shadow_stop").expect("cluster id");
        let local_node_id =
            NodeId::from_legacy_named("node_explicit_shadow_stop_local").expect("local node");
        let shadow_node_id =
            NodeId::from_legacy_named("node_explicit_shadow_stop_remote").expect("remote node");
        let mut member = ClusterMemberRecord::local_voter(
            cluster_id.clone(),
            shadow_node_id.clone(),
            "remote",
            "remote-key",
            timestamp,
        );
        member.endpoints.push(network::daemon_lan_endpoint(
            &shadow_node_id,
            "10.14.5.15:21354".to_string(),
            10,
        ));
        let state = ClusterBootstrapState {
            cluster_id: cluster_id.clone(),
            cluster_name: "explicit-shadow-stop".to_string(),
            local_node_id,
            local_node_display_name: "local".to_string(),
            term: 1,
            kv_revision: 1,
            members: vec![member],
            initialized_at: timestamp,
            updated_at: timestamp,
        };
        let options = DaemonRunOptions {
            host: "10.14.5.15".to_string(),
            port: 21_354,
            port_fallback: false,
        };
        let root = sinomo_testdir::TestDir::new("sinomo-cli-explicit-shadow-stop-status");
        let data_dir = SinomoDataDir::new(&root);
        let status = DaemonStatus::new(
            &DaemonRunOptions {
                host: "0.0.0.0".to_string(),
                port: 21_354,
                port_fallback: false,
            },
            &DaemonPaths::for_shadow(&data_dir),
            Some(DaemonClusterStatus {
                cluster_id,
                node_id: status_node_id,
                process: process.to_string(),
                member_role: ClusterMemberRole::Voter,
                member_state: ClusterMemberState::Voter,
            }),
        );
        (state, options, status)
    }

    #[test]
    fn explicit_shadow_stop_requires_exact_shadow_process_before_shutdown() {
        let remote_node =
            NodeId::from_legacy_named("node_explicit_shadow_stop_remote").expect("remote node");
        let (state, options, status) = explicit_shadow_stop_fixture(
            sinomo_runtime::node::DAEMON_SHADOW_CLUSTER_ROLE,
            remote_node.clone(),
        );
        let shutdowns = Cell::new(0_u32);
        let response = stop_explicit_shadow_daemon_with(
            &state,
            &options,
            |_| Ok(status.clone()),
            |address| {
                assert_eq!(address, "10.14.5.15:21354");
                shutdowns.set(shutdowns.get().saturating_add(1));
                Ok("shadow daemon shutdown requested".to_string())
            },
        )
        .expect("exact shadow stop");
        assert_eq!(response, "shadow daemon shutdown requested");
        assert_eq!(shutdowns.get(), 1);

        for (process, node_id) in [
            (
                sinomo_runtime::node::DAEMON_MAIN_CLUSTER_ROLE,
                remote_node.clone(),
            ),
            (
                sinomo_runtime::node::DAEMON_SHADOW_CLUSTER_ROLE,
                NodeId::from_legacy_named("node_explicit_shadow_stop_wrong").expect("wrong node"),
            ),
        ] {
            let (state, options, status) = explicit_shadow_stop_fixture(process, node_id);
            let shutdowns = Cell::new(0_u32);
            let error = stop_explicit_shadow_daemon_with(
                &state,
                &options,
                |_| Ok(status.clone()),
                |_| {
                    shutdowns.set(shutdowns.get().saturating_add(1));
                    Ok("must not stop".to_string())
                },
            )
            .expect_err("wrong process identity must fail closed");
            assert!(
                error.to_string().contains("refusing to stop"),
                "unexpected error: {error}"
            );
            assert_eq!(shutdowns.get(), 0);
        }
    }

    #[test]
    fn cli_control_plane_transport_scopes_daemon_discovery_to_supplied_paths() {
        let root = sinomo_testdir::TestDir::new("sinomo-cli-control-plane-path-scope");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data dir layout");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let transport = CliClusterControlPlaneTransport::new(&cluster_paths);

        assert_eq!(transport.cluster_paths.root, cluster_paths.root);
        assert_eq!(
            transport
                .matching_daemon_paths()
                .expect("daemon paths")
                .root,
            DaemonPaths::new(&data_dir).root
        );
    }

    #[test]
    fn cli_control_plane_transport_falls_back_only_before_daemon_send() {
        let fallback_called = Cell::new(false);
        let fallback = || {
            fallback_called.set(true);
            Ok::<_, &'static str>("direct")
        };
        let result = daemon_mediated_result_or_direct_fallback(None, fallback);
        assert_eq!(result, Ok("direct"));
        assert!(fallback_called.get());

        fallback_called.set(false);
        let result =
            daemon_mediated_result_or_direct_fallback(Some(Err("possibly applied")), || {
                fallback_called.set(true);
                Ok("direct")
            });
        assert_eq!(result, Err("possibly applied"));
        assert!(!fallback_called.get());
    }

    #[test]
    fn cli_control_plane_transport_missing_matching_daemon_allows_direct_fallback() {
        let root = sinomo_testdir::TestDir::new("sinomo-cli-control-plane-no-daemon");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data dir layout");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let transport = CliClusterControlPlaneTransport::new(&cluster_paths);
        let target = NodeId::from_legacy_named("node_cli_control_plane_target").expect("node");
        let request =
            network::ObjectRpcRequest::from_encoded_body("/test/control-plane", "test", Vec::new());

        assert!(
            transport
                .call_node_object_rpc_via_matching_daemon(
                    &target,
                    &request,
                    Duration::from_millis(10),
                )
                .is_none(),
            "daemon discovery failure before send must retain direct fallback"
        );
    }

    #[test]
    fn cli_daemon_forward_preserves_authority_retry_safety() {
        for delivery_state in [
            network::NetworkDeliveryState::DefinitelyNotSent,
            network::NetworkDeliveryState::PossiblyApplied,
        ] {
            let error = cli_daemon_forward_network_error(
                "test authority forward",
                sinomo_runtime::api::DaemonApiError::ForwardNetwork {
                    detail: "forward failed".to_string(),
                    delivery_state: Some(delivery_state),
                },
            );
            assert_eq!(error.delivery_state(), Some(delivery_state));
        }
    }
}
