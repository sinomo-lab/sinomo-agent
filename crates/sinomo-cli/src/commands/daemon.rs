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
    CompleteShadowRollout,
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
        Some((command, [])) if command == "__complete-shadow-rollout" => {
            Ok(DaemonCliCommand::CompleteShadowRollout)
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
        DaemonCliCommand::CompleteShadowRollout => complete_shadow_rollout_from_helper(),
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

pub(crate) fn complete_shadow_rollout_from_helper() -> Result<(), CliError> {
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
    daemon_run_daemon_control_loop_with_services(
        options,
        paths,
        label,
        cluster,
        vec![
            sinomo_runtime::daemon::DaemonServiceRegistryHook::new(sinomo_agent::register_services)
                .with_config_definitions(|builder| {
                    sinomo_lm::register_config_definitions(builder)
                        .map_err(|error| error.to_string())
                }),
        ],
    )
    .map_err(Into::into)
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
                print_local_daemon_iroh_endpoint(&paths, &status);
            }
            if let Some(stats) = &status.idle_stats {
                print_daemon_idle_stats(stats);
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
        "    kv publishes: member={} node_status={} network_info={} connectivity={}",
        stats.kv_publish_member,
        stats.kv_publish_node_status,
        stats.kv_publish_network_info,
        stats.kv_publish_connectivity
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

fn print_local_daemon_iroh_endpoint(paths: &DaemonPaths, status: &DaemonStatus) {
    let Some(cluster) = status.cluster.as_ref() else {
        return;
    };
    let Some(data_dir) = paths.root.parent() else {
        return;
    };
    let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
    let Ok(record) = network::read_iroh_endpoint_record(&cluster_paths, &cluster.node_id) else {
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

pub(crate) fn cli_current_cluster_kv_store(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<InProcessKvStore, network::NetworkError> {
    network::refresh_current_cluster_kv_store_with_host(
        paths,
        state,
        cli_cluster_kv_snapshot_refresh_options(),
        &mut CliClusterKvSnapshotRefreshHost,
    )
}

pub(crate) fn cli_commit_cluster_kv_txn_with_quorum(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    txn: KvTxn,
) -> Result<KvSnapshot, network::NetworkError> {
    Ok(cli_commit_cluster_kv_txn_with_quorum_response(paths, state, txn)?.snapshot)
}

pub(crate) fn cli_commit_cluster_kv_txn_with_quorum_response(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    txn: KvTxn,
) -> Result<network::ClusterKvTxnForwardResult, network::NetworkError> {
    network::commit_cluster_kv_txn_with_quorum_response_with_host(
        paths,
        state,
        txn,
        cli_cluster_kv_txn_quorum_options(true),
        &mut CliClusterKvTxnQuorumHost,
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
        network::read_cluster_state_without_kv(&cluster_paths).map_err(|error| match error {
            network::NetworkError::ClusterOrphan => CliError::ClusterReadOnly {
                detail: "local node has no cluster state".to_string(),
            },
            other => other.into(),
        })?;
    let mut state = state;
    let result = network::commit_cluster_kv_txn_with_quorum_response_with_host(
        &cluster_paths,
        &mut state,
        txn,
        cli_cluster_kv_txn_quorum_options(true),
        &mut CliClusterKvTxnQuorumHost,
    )?;
    sinomo_runtime::kv::write_imported_cluster_kv_snapshot_if_changed(
        &cluster_paths.kv_paths(),
        &result.snapshot,
        network::cluster_kv_key_can_defer_materialized_snapshot,
    )?;
    Ok(result.txn_response.unwrap_or(KvTxnResponse {
        succeeded: true,
        revision: result.snapshot.revision,
        responses: Vec::new(),
    }))
}

struct CliClusterKvSnapshotRefreshHost;

impl network::ClusterKvSnapshotRefreshHost for CliClusterKvSnapshotRefreshHost {
    fn known_raft_leader_id(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<Option<NodeId>, network::NetworkError> {
        locked_cluster_raft_known_leader_id(paths, state)
    }

    fn fetch_cluster_kv_snapshot(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        timeout: Duration,
    ) -> Result<KvSnapshot, network::NetworkError> {
        let request = cluster_kv_snapshot_rpc_request()?;
        let response = daemon_send_daemon_rpc_to_endpoint(endpoint, &request, timeout)
            .map_err(|error| cli_daemon_rpc_network_error("cluster kv snapshot refresh", error))?;
        decode_cluster_kv_snapshot_rpc_response(&response)
    }

    fn restore_cluster_raft_from_snapshot_if_newer(
        &mut self,
        paths: &ClusterPaths,
        local_node_id: &NodeId,
        snapshot: KvSnapshot,
    ) -> Result<bool, network::NetworkError> {
        let _raft_guard =
            sinomo_runtime::kv::lock_cluster_kv_raft_operation_for_paths(&paths.kv_paths())
                .map_err(network::NetworkError::from)?;
        paths
            .kv_paths()
            .local_cache()
            .restore_raft_from_snapshot_if_newer(local_node_id.clone(), snapshot)
            .map_err(network::NetworkError::from)
    }
}

struct CliClusterKvTxnQuorumHost;

impl network::ClusterKvTxnQuorumHost for CliClusterKvTxnQuorumHost {
    fn refresh_current_cluster_kv_store(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<(), network::NetworkError> {
        sinomo_runtime::kv::read_cluster_kv_store_for_node(
            &paths.kv_paths(),
            state.local_node_id.clone(),
        )
        .map(|_| ())
        .map_err(network::NetworkError::from)
    }

    fn local_is_raft_leader(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<bool, network::NetworkError> {
        locked_cluster_raft_local_is_leader(paths, state)
    }

    fn forward_cluster_kv_txn_to_voter(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        txn: &KvTxn,
        options: network::ClusterKvTxnForwardOptions,
    ) -> Result<Option<network::ClusterKvTxnForwardResult>, network::NetworkError> {
        network::forward_cluster_kv_txn_to_voter_with_host(
            paths,
            state,
            txn,
            options,
            &mut CliClusterKvTxnForwardHost,
        )
    }

    fn elect_cluster_raft_leader(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        options: network::ClusterRaftLeaderElectionOptions,
    ) -> Result<bool, network::NetworkError> {
        network::elect_cluster_raft_leader_with_host(
            paths,
            state,
            options,
            &mut CliClusterRaftLeaderElectionHost,
        )
    }

    fn reachable_voter_count(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        options: network::ClusterVoterReachabilityOptions,
    ) -> usize {
        network::cluster_reachable_voter_count_with_host(
            paths,
            state,
            options,
            &mut CliClusterVoterReachabilityHost,
        )
    }

    fn commit_cluster_raft_command_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        command: sinomo_runtime::kv::KvCommand,
        options: network::ClusterRaftQuorumCommitOptions,
    ) -> Result<network::ClusterRaftQuorumCommitResult, network::NetworkError> {
        let mut host = CliClusterRaftQuorumCommitHost {
            storage: CliClusterRaftCommandStorage::new(paths, state.local_node_id.clone()),
        };
        network::commit_cluster_raft_command_with_quorum_driver(
            paths, state, command, options, &mut host,
        )
    }
}

struct CliClusterRaftQuorumCommitHost<'a> {
    storage: CliClusterRaftCommandStorage<'a>,
}

struct CliClusterRaftCommandStorage<'a> {
    paths: &'a ClusterPaths,
    local_node_id: NodeId,
}

impl<'a> CliClusterRaftCommandStorage<'a> {
    fn new(paths: &'a ClusterPaths, local_node_id: NodeId) -> Self {
        Self {
            paths,
            local_node_id,
        }
    }

    fn raft_store(&self) -> sinomo_runtime::kv::ClusterKvRaftStore {
        self.paths.kv_paths().raft_store()
    }
}

impl sinomo_runtime::kv::ClusterKvRaftCommandStorage for CliClusterRaftCommandStorage<'_> {
    type Error = network::NetworkError;

    fn append_cluster_raft_command_uncommitted_or_no_effect(
        &mut self,
        command: sinomo_runtime::kv::KvCommand,
    ) -> Result<sinomo_runtime::kv::ClusterKvRaftUncommittedAppend, network::NetworkError> {
        let _raft_guard =
            sinomo_runtime::kv::lock_cluster_kv_raft_operation_for_paths(&self.paths.kv_paths())
                .map_err(network::NetworkError::from)?;
        self.raft_store()
            .append_command_uncommitted_or_no_effect(&self.local_node_id, command)
            .map_err(Into::into)
    }

    fn discard_cluster_raft_uncommitted_tail_and_clear_leader(
        &mut self,
    ) -> Result<(), network::NetworkError> {
        let _raft_guard =
            sinomo_runtime::kv::lock_cluster_kv_raft_operation_for_paths(&self.paths.kv_paths())
                .map_err(network::NetworkError::from)?;
        let snapshot = self
            .raft_store()
            .discard_uncommitted_tail_and_clear_leader(&self.local_node_id)
            .map_err(network::NetworkError::from)?;
        sinomo_runtime::kv::write_cluster_kv_snapshot_if_changed(&self.paths.kv_paths(), &snapshot)
            .map_err(network::NetworkError::from)?;
        Ok(())
    }

    fn commit_cluster_raft_index_with_events(
        &mut self,
        index: u64,
    ) -> Result<sinomo_runtime::kv::ClusterKvRaftCommitResult, network::NetworkError> {
        let _raft_guard =
            sinomo_runtime::kv::lock_cluster_kv_raft_operation_for_paths(&self.paths.kv_paths())
                .map_err(network::NetworkError::from)?;
        let store = self.raft_store();
        let result = store
            .commit_index_with_events(&self.local_node_id, index)
            .map_err(network::NetworkError::from)?;
        store
            .publish_commit_result(
                &result,
                network::cluster_kv_key_can_defer_materialized_snapshot,
            )
            .map_err(network::NetworkError::from)?;
        Ok(result)
    }
}

impl sinomo_runtime::kv::ClusterKvRaftCommandStorage for CliClusterRaftQuorumCommitHost<'_> {
    type Error = network::NetworkError;

    fn append_cluster_raft_command_uncommitted_or_no_effect(
        &mut self,
        command: sinomo_runtime::kv::KvCommand,
    ) -> Result<sinomo_runtime::kv::ClusterKvRaftUncommittedAppend, network::NetworkError> {
        sinomo_runtime::kv::ClusterKvRaftCommandStorage::append_cluster_raft_command_uncommitted_or_no_effect(
            &mut self.storage,
            command,
        )
    }

    fn discard_cluster_raft_uncommitted_tail_and_clear_leader(
        &mut self,
    ) -> Result<(), network::NetworkError> {
        sinomo_runtime::kv::ClusterKvRaftCommandStorage::discard_cluster_raft_uncommitted_tail_and_clear_leader(
            &mut self.storage,
        )
    }

    fn commit_cluster_raft_index_with_events(
        &mut self,
        index: u64,
    ) -> Result<sinomo_runtime::kv::ClusterKvRaftCommitResult, network::NetworkError> {
        sinomo_runtime::kv::ClusterKvRaftCommandStorage::commit_cluster_raft_index_with_events(
            &mut self.storage,
            index,
        )
    }
}

impl network::ClusterRaftQuorumTransportHost for CliClusterRaftQuorumCommitHost<'_> {
    fn replicate_cluster_raft_to_endpoint(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        endpoint: &ClusterEndpointRecord,
        timeout: Duration,
    ) -> Result<sinomo_runtime::kv::ClusterRaftAppendResponse, network::NetworkError> {
        network::replicate_cluster_raft_to_endpoint_with_host(
            paths,
            state,
            endpoint,
            &[],
            cli_cluster_raft_append_replication_options(timeout),
            &mut CliClusterRaftAppendReplicationHost,
        )
        .map(|outcome| outcome.response)
    }
}

struct CliClusterRaftAppendReplicationHost;

impl network::ClusterRaftAppendReplicationHost for CliClusterRaftAppendReplicationHost {
    fn last_local_raft_log_index(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<u64, network::NetworkError> {
        paths
            .kv_paths()
            .raft_store()
            .last_log_index(&state.local_node_id)
            .map_err(Into::into)
    }

    fn build_raft_append_request(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        next_index: u64,
    ) -> Result<sinomo_runtime::kv::ClusterRaftAppendRequest, network::NetworkError> {
        paths
            .kv_paths()
            .raft_store()
            .build_cluster_raft_append_request_from_next_index(state, next_index)
            .map_err(Into::into)
    }

    fn send_raft_append(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &sinomo_runtime::kv::ClusterRaftAppendRequest,
        timeout: Duration,
    ) -> Result<sinomo_runtime::kv::ClusterRaftAppendResponse, network::NetworkError> {
        cli_send_cluster_raft_append(endpoint, request, timeout)
    }
}

struct CliClusterRaftLeaderElectionHost;

impl network::ClusterRaftVoteTransportHost for CliClusterRaftLeaderElectionHost {
    fn send_raft_vote(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &sinomo_runtime::kv::ClusterRaftVoteRequest,
        timeout: Duration,
    ) -> Result<sinomo_runtime::kv::ClusterRaftVoteResponse, network::NetworkError> {
        cli_send_cluster_raft_vote(endpoint, request, timeout)
    }
}

struct CliClusterKvTxnForwardHost;

impl network::ClusterKvTxnForwardHost for CliClusterKvTxnForwardHost {
    fn known_raft_leader_id(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<Option<NodeId>, network::NetworkError> {
        locked_cluster_raft_known_leader_id(paths, state)
    }

    fn send_cluster_kv_txn(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        txn: &KvTxn,
        timeout: Duration,
    ) -> Result<KvTxnResponse, network::NetworkError> {
        cli_send_cluster_kv_txn(endpoint, txn, timeout)
    }

    fn fetch_cluster_kv_snapshot(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        timeout: Duration,
    ) -> Result<KvSnapshot, network::NetworkError> {
        let request = cluster_kv_snapshot_rpc_request()?;
        let response = daemon_send_daemon_rpc_to_endpoint(endpoint, &request, timeout)
            .map_err(|error| cli_daemon_rpc_network_error("cluster kv snapshot", error))?;
        decode_cluster_kv_snapshot_rpc_response(&response)
    }

    fn send_object_rpc_to_node(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        request: &network::ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<network::ObjectRpcResponse, network::NetworkError> {
        let data_dir =
            SinomoDataDir::from_env_or_home().map_err(|error| network::NetworkError::Io {
                operation: "cli daemon cluster paths".to_string(),
                detail: error.to_string(),
            })?;
        let paths = ClusterPaths::new(&data_dir);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| network::NetworkError::Io {
                operation: "cli daemon node object rpc runtime".to_string(),
                detail: error.to_string(),
            })?;
        let mut host = CliSignedEndpointRpcHost::new(
            Some(state.cluster_id.clone()),
            "cli endpoint rpc signing",
        );
        let options = network::ClusterObjectRpcNodeOptions::new(
            network::ClusterEndpointSelectionPurpose::VoterCommand,
            network::DaemonRpcTimeouts::connect_and_idle(timeout),
        );
        let result = runtime.block_on(network::call_cluster_node_rpc_async(
            &mut host,
            network::ClusterObjectRpcRouteContext::new(runtime.handle(), &paths),
            state,
            target_node_id,
            request,
            options,
        ));
        runtime.shutdown_timeout(Duration::from_millis(250));
        result
    }
}

fn cli_cluster_kv_txn_quorum_options(allow_forwarding: bool) -> network::ClusterKvTxnQuorumOptions {
    network::ClusterKvTxnQuorumOptions {
        allow_forwarding,
        raft_append_timeout: Duration::from_secs(10),
        raft_append_replication_options: cli_cluster_raft_append_replication_options(
            Duration::from_secs(10),
        ),
        forward_options: network::ClusterKvTxnForwardOptions {
            timeout: Duration::from_secs(10),
            tcp_command_timeout: Duration::from_secs(8),
            iroh_command_timeout: Duration::from_secs(6),
        },
        leader_election_options: network::ClusterRaftLeaderElectionOptions {
            kv: sinomo_runtime::kv::ClusterKvRaftLeaderElectionOptions {
                max_attempts: sinomo_runtime::kv::CLUSTER_KV_RAFT_LEADER_ELECTION_ATTEMPTS,
            },
            timeout: Duration::from_secs(10),
            tcp_command_timeout: Duration::from_secs(8),
            iroh_command_timeout: Duration::from_secs(6),
            tcp_foreground_attempt_timeout: Duration::from_secs(2),
            public_foreground_attempt_timeout: Duration::from_secs(5),
            iroh_foreground_attempt_timeout: Duration::from_secs(3),
        },
        voter_reachability_options: cli_cluster_voter_reachability_options(),
    }
}

fn cli_cluster_kv_snapshot_refresh_options() -> network::ClusterKvSnapshotRefreshOptions {
    network::ClusterKvSnapshotRefreshOptions {
        without_local_snapshot_timeout: Duration::from_secs(10),
        with_local_snapshot_timeout: Duration::from_secs(5),
        tcp_command_timeout: Duration::from_secs(5),
        iroh_command_timeout: Duration::from_secs(5),
        tcp_foreground_attempt_timeout: Duration::from_secs(2),
        public_foreground_attempt_timeout: Duration::from_secs(5),
        iroh_foreground_attempt_timeout: Duration::from_secs(3),
    }
}

fn cli_cluster_raft_append_replication_options(
    timeout: Duration,
) -> network::ClusterRaftAppendReplicationOptions {
    network::ClusterRaftAppendReplicationOptions {
        max_attempts: 3,
        timeout,
        tcp_command_timeout: timeout,
        iroh_command_timeout: timeout,
        tcp_foreground_attempt_timeout: Duration::from_secs(2).min(timeout),
        public_foreground_attempt_timeout: Duration::from_secs(5).min(timeout),
        iroh_foreground_attempt_timeout: Duration::from_secs(3).min(timeout),
    }
}

fn cli_send_cluster_kv_txn(
    endpoint: &ClusterEndpointRecord,
    txn: &KvTxn,
    timeout: Duration,
) -> Result<KvTxnResponse, network::NetworkError> {
    let request = cluster_kv_txn_rpc_request(txn)?;
    let response = daemon_send_daemon_rpc_to_endpoint(endpoint, &request, timeout)
        .map_err(|error| cli_daemon_rpc_network_error("cluster kv txn", error))?;
    decode_cluster_kv_txn_rpc_response(&response)
}

fn cli_send_cluster_raft_append(
    endpoint: &ClusterEndpointRecord,
    request: &sinomo_runtime::kv::ClusterRaftAppendRequest,
    timeout: Duration,
) -> Result<sinomo_runtime::kv::ClusterRaftAppendResponse, network::NetworkError> {
    let payload = sinomo_runtime::kv::cluster_raft_append_rpc_payload(request)?;
    let rpc_request =
        network::ObjectRpcRequest::from_encoded_body(payload.route, payload.category, payload.body);
    let response = daemon_send_daemon_rpc_to_endpoint(endpoint, &rpc_request, timeout)
        .map_err(|error| cli_daemon_rpc_network_error("cluster raft append", error))?;
    response.error_detail()?;
    sinomo_runtime::kv::decode_cluster_raft_append_rpc_response_body(&response.body)
        .map_err(Into::into)
}

fn cli_send_cluster_raft_vote(
    endpoint: &ClusterEndpointRecord,
    request: &sinomo_runtime::kv::ClusterRaftVoteRequest,
    timeout: Duration,
) -> Result<sinomo_runtime::kv::ClusterRaftVoteResponse, network::NetworkError> {
    let payload = sinomo_runtime::kv::cluster_raft_vote_rpc_payload(request)?;
    let rpc_request =
        network::ObjectRpcRequest::from_encoded_body(payload.route, payload.category, payload.body);
    let response = daemon_send_daemon_rpc_to_endpoint(endpoint, &rpc_request, timeout)
        .map_err(|error| cli_daemon_rpc_network_error("cluster raft vote", error))?;
    response.error_detail()?;
    sinomo_runtime::kv::decode_cluster_raft_vote_rpc_response_body(&response.body)
        .map_err(Into::into)
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
    network::handle_cluster_join_registration_request_with_host(
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
impl network::ClusterMembershipRpcForwardHost for CliClusterJoinRegistrationHost {
    fn send_membership_rpc_to_node(
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
}

#[cfg(test)]
impl network::ClusterJoinRegistrationHost for CliClusterJoinRegistrationHost {
    fn commit_join_registration_state(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<KvSnapshot, network::NetworkError> {
        network::sync_cluster_state_to_local_raft_unchecked(paths, state)
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
