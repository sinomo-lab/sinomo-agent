use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct InstallOptions {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) shadow_port: u16,
    pub(crate) start: bool,
}

impl Default for InstallOptions {
    fn default() -> Self {
        Self {
            host: INSTALL_DEFAULT_HOST.to_string(),
            port: SINOMO_DAEMON_DEFAULT_PORT,
            shadow_port: SINOMO_SHADOW_DAEMON_DEFAULT_PORT,
            start: true,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct UninstallOptions {
    pub(crate) stop: bool,
}

impl Default for UninstallOptions {
    fn default() -> Self {
        Self { stop: true }
    }
}

pub(crate) fn parse_install_options(args: &[String]) -> Result<InstallOptions, CliError> {
    let mut options = InstallOptions::default();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "help" | "--help" | "-h" if args.len() == 1 => return Err(CliError::Usage),
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
                options.port = parse_port("install port", port)?;
            }
            "--shadow-port" => {
                index += 1;
                let Some(port) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.shadow_port = parse_port("install shadow port", port)?;
            }
            "--no-start" => options.start = false,
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }
    Ok(options)
}

pub(crate) fn parse_uninstall_options(args: &[String]) -> Result<UninstallOptions, CliError> {
    let mut options = UninstallOptions::default();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "help" | "--help" | "-h" if args.len() == 1 => return Err(CliError::Usage),
            "--no-stop" => options.stop = false,
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }
    Ok(options)
}

pub(crate) fn parse_port(label: &str, value: &str) -> Result<u16, CliError> {
    value.parse::<u16>().map_err(|source| CliError::Parse {
        detail: format!("invalid {label} `{value}`: {source}"),
    })
}

pub(crate) fn ensure_data_dir_storage_contracts(
    data_dir: impl AsRef<Path>,
) -> Result<(), CliError> {
    let report = cli_data_dir_layout()?.verify_data_dir(data_dir)?;
    if report.is_complete() {
        Ok(())
    } else {
        Err(
            sinomo_runtime::storage::StorageContractError::UnregisteredPaths {
                paths: report.unregistered,
            }
            .into(),
        )
    }
}

fn cli_data_dir_layout()
-> Result<sinomo_runtime::storage::StorageLayout, sinomo_runtime::storage::StorageContractError> {
    let mut layout = sinomo_runtime::storage::sinomo_data_dir_layout()?;
    sinomo_evolution::register_storage_paths(&mut layout)?;
    sinomo_pytorch::register_storage_paths(&mut layout)?;
    Ok(layout)
}

pub(crate) fn install(options: InstallOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    let boot_dir = default_boot_dir(&data_dir)?;
    let layout = InstallLayout::new(&data_dir, boot_dir);
    install_with_layout(options, &layout)
}

pub(crate) fn install_with_layout(
    options: InstallOptions,
    layout: &InstallLayout,
) -> Result<(), CliError> {
    let managed_source_head_before = installed_managed_source_head(layout);
    let plan = daemon_prepare_installed_daemon_install_plan(
        layout,
        InstalledDaemonInstallPlanRequest {
            host: options.host.clone(),
            port: options.port,
            shadow_port: options.shadow_port,
            start: options.start,
        },
        DAEMON_PROBE_TIMEOUT,
    )?;
    daemon_ensure_install_layout_dirs(layout)?;
    if let Some(reason) = plan.stop_reason {
        stop_installed_daemons_before_install(layout, reason)?;
    }
    install_current_binary(&layout.binary)?;
    install_current_binary(&layout.shadow_binary)?;
    install_managed_source_repo(layout)?;
    let managed_source_updated =
        managed_source_head_before != installed_managed_source_head(layout);
    write_boot_entries(layout, &options)?;
    activate_boot_entries(layout);
    #[cfg(not(test))]
    configure_user_bin_access(layout)?;
    ensure_data_dir_storage_contracts(&layout.data_dir)?;
    println!("installed binary: {}", layout.binary.display());
    println!(
        "installed shadow binary: {}",
        layout.shadow_binary.display()
    );
    println!(
        "managed source repo: {}",
        SelfRolloutPaths::new(&layout.data_dir)
            .source_repo
            .display()
    );
    println!("boot entry: {}", layout.main_boot_entry.display());
    println!("boot entry: {}", layout.shadow_boot_entry.display());
    println!("data directory retained at: {}", layout.data_dir.display());
    if plan.existing_install {
        print_existing_install_rollout_hint(layout);
    }

    if options.start && plan.stop_reason.is_none() && managed_source_updated {
        stop_installed_daemons_before_install(layout, "managed source update")?;
    }

    for spec in plan.start_specs {
        ensure_installed_daemon_started(
            layout,
            &spec.binary,
            &spec.args,
            &spec.paths,
            spec.label,
            &spec.extra_env,
        )?;
    }
    Ok(())
}

fn installed_managed_source_head(layout: &InstallLayout) -> Option<String> {
    managed_source_status(&SelfRolloutPaths::new(&layout.data_dir))
        .ok()
        .and_then(|status| status.head)
}

pub(crate) fn uninstall(options: UninstallOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    let boot_dir = default_boot_dir(&data_dir)?;
    let layout = InstallLayout::new(&data_dir, boot_dir);
    uninstall_with_layout(options, &layout)
}

pub(crate) fn uninstall_with_layout(
    options: UninstallOptions,
    layout: &InstallLayout,
) -> Result<(), CliError> {
    if options.stop {
        let daemon_paths = daemon_installed_daemon_paths(layout);
        stop_daemon_if_running(&daemon_paths.shadow, "shadow daemon")?;
        stop_daemon_if_running(&daemon_paths.main, "daemon")?;
    }
    deactivate_boot_entries(layout);
    remove_boot_entry(&layout.main_boot_entry)?;
    remove_boot_entry(&layout.shadow_boot_entry)?;
    println!("removed boot entry: {}", layout.main_boot_entry.display());
    println!("removed boot entry: {}", layout.shadow_boot_entry.display());
    println!("data directory retained at: {}", layout.data_dir.display());
    Ok(())
}

pub(crate) fn install_current_binary(destination: &Path) -> Result<(), CliError> {
    Ok(install_current_executable(destination)?)
}

#[cfg(test)]
pub(crate) fn install_should_update_binary(destination: &Path) -> Result<bool, CliError> {
    Ok(sinomo_runtime::rollout::current_executable_should_update(
        destination,
    )?)
}

pub(crate) fn install_managed_source_repo(layout: &InstallLayout) -> Result<(), CliError> {
    match daemon_install_managed_source_repo(layout)? {
        ManagedSourceRepoInstallStatus::Updated => Ok(()),
        ManagedSourceRepoInstallStatus::CurrentDirectoryNotInGitCheckout => {
            println!(
                "warning: managed source repo was not updated because the current directory is not inside a git checkout"
            );
            Ok(())
        }
    }
}

pub(crate) fn default_boot_dir(data_dir: &SinomoDataDir) -> Result<PathBuf, CliError> {
    Ok(daemon_default_boot_dir(data_dir))
}

pub(crate) fn write_boot_entries(
    layout: &InstallLayout,
    options: &InstallOptions,
) -> Result<(), CliError> {
    daemon_write_install_boot_entries(layout, &options.host, options.port, options.shadow_port)
        .map_err(Into::into)
}

pub(crate) fn remove_boot_entry(path: &Path) -> Result<(), CliError> {
    daemon_remove_boot_entry(path).map_err(Into::into)
}

#[cfg(windows)]
#[cfg_attr(test, allow(dead_code))]
pub(crate) fn configure_user_bin_access(layout: &InstallLayout) -> Result<(), CliError> {
    match daemon_ensure_windows_user_path_contains(&layout.bin_dir)? {
        UserPathUpdate::AlreadyPresent => {
            println!("user PATH already contains: {}", layout.bin_dir.display());
        }
        UserPathUpdate::Added => {
            println!("added to user PATH: {}", layout.bin_dir.display());
        }
    }
    Ok(())
}

#[cfg(not(windows))]
#[cfg_attr(test, allow(dead_code))]
pub(crate) fn configure_user_bin_access(layout: &InstallLayout) -> Result<(), CliError> {
    match ensure_local_bin_symlink(layout)? {
        LocalBinSymlinkStatus::HomeNotSet => {
            println!("warning: could not create ~/.local/bin/sinomo because HOME is not set");
        }
        LocalBinSymlinkStatus::AlreadyCurrent { link } => {
            println!("user bin symlink already current: {}", link.display());
        }
        LocalBinSymlinkStatus::Created { link } => {
            println!("created user bin symlink: {}", link.display());
        }
        LocalBinSymlinkStatus::BlockedByNonSymlink { link } => {
            println!(
                "warning: could not create {} because a non-symlink already exists",
                link.display()
            );
        }
    }
    Ok(())
}

#[cfg(not(windows))]
pub(crate) fn ensure_local_bin_symlink(
    layout: &InstallLayout,
) -> Result<LocalBinSymlinkStatus, CliError> {
    daemon_ensure_local_bin_symlink(layout).map_err(Into::into)
}

pub(crate) fn print_existing_install_rollout_hint(layout: &InstallLayout) {
    let rollout_paths = SelfRolloutPaths::new(&layout.data_dir);
    println!("existing installation detected");
    if git_is_available() {
        println!("managed source status:");
        match managed_source_status(&rollout_paths) {
            Ok(status) => {
                println!("  repo: {}", status.path.display());
                println!("  git repo: {}", status.is_git_repo);
                if let Some(head) = status.head {
                    println!("  head: {head}");
                }
                println!("  external remotes: {}", status.has_external_remotes);
                if !status.remotes.is_empty() {
                    println!("  remotes: {}", status.remotes.join(", "));
                }
                match managed_source_git_status_lines(&rollout_paths.source_repo) {
                    Ok(lines) if lines.is_empty() => println!("  worktree: clean"),
                    Ok(lines) => {
                        println!("  worktree:");
                        for line in lines {
                            println!("    {line}");
                        }
                    }
                    Err(error) => println!("  worktree status unavailable: {error}"),
                }
            }
            Err(error) => println!("  managed source status unavailable: {error}"),
        }
    } else {
        println!("managed source status unavailable: git was not found");
    }
    println!(
        "roll out the managed source to the cluster with: sinomo rollout cluster --remote-build"
    );
}

pub(crate) fn git_is_available() -> bool {
    daemon_git_is_available()
}

pub(crate) fn managed_source_git_status_lines(repo: &Path) -> Result<Vec<String>, CliError> {
    daemon_managed_source_git_status_lines(repo).map_err(Into::into)
}

#[cfg(windows)]
pub(crate) fn activate_boot_entries(layout: &InstallLayout) {
    let _ = daemon_activate_boot_entries(layout);
}

#[cfg(not(windows))]
pub(crate) fn activate_boot_entries(layout: &InstallLayout) {
    print_systemctl_warnings(daemon_activate_boot_entries(layout));
}

#[cfg(windows)]
pub(crate) fn deactivate_boot_entries(layout: &InstallLayout) {
    let _ = daemon_deactivate_boot_entries(layout);
}

#[cfg(not(windows))]
pub(crate) fn deactivate_boot_entries(layout: &InstallLayout) {
    print_systemctl_warnings(daemon_deactivate_boot_entries(layout));
}

#[cfg(not(windows))]
pub(crate) fn print_systemctl_warnings(outcomes: Vec<SystemctlUserOutcome>) {
    for outcome in outcomes {
        match outcome {
            SystemctlUserOutcome::Succeeded { .. } => {}
            SystemctlUserOutcome::Failed { args, stderr } => {
                println!(
                    "warning: systemctl --user {} failed: {}",
                    args.join(" "),
                    stderr
                );
            }
            SystemctlUserOutcome::CouldNotRun { args, detail } => {
                println!(
                    "warning: systemctl --user {} could not run: {}",
                    args.join(" "),
                    detail
                );
            }
        }
    }
}

pub(crate) fn ensure_installed_daemon_started(
    layout: &InstallLayout,
    binary: &Path,
    args: &[String],
    paths: &DaemonPaths,
    label: &str,
    extra_env: &[(&str, String)],
) -> Result<(), CliError> {
    if matches!(
        probe_daemon_status(paths, DAEMON_PROBE_TIMEOUT),
        DaemonProbeStatus::Running(_)
    ) {
        println!("sinomo {label} already running");
        return Ok(());
    }
    start_installed_sinomo_daemon(binary, &layout.data_dir, args, paths, label, extra_env)?;
    Ok(())
}

pub(crate) fn stop_installed_daemons_before_install(
    layout: &InstallLayout,
    reason: &str,
) -> Result<(), CliError> {
    let daemon_paths = daemon_installed_daemon_paths(layout);
    stop_daemon_before_install(&daemon_paths.shadow, "shadow daemon", reason)?;
    stop_daemon_before_install(&daemon_paths.main, "daemon", reason)?;
    Ok(())
}

pub(crate) fn stop_daemon_before_install(
    paths: &DaemonPaths,
    label: &str,
    reason: &str,
) -> Result<(), CliError> {
    match daemon_install_stop_daemon_before_install(
        paths,
        Duration::from_secs(10),
        DAEMON_PROBE_TIMEOUT,
    )? {
        DaemonStopBeforeInstallStatus::NotRunning => Ok(()),
        DaemonStopBeforeInstallStatus::Stopped { start } => {
            print_stop_daemon_before_install_start(label, reason, start);
            println!("sinomo {label} stopped");
            Ok(())
        }
        DaemonStopBeforeInstallStatus::Terminated { start, pid } => {
            print_stop_daemon_before_install_start(label, reason, start);
            println!(
                "sinomo {label} did not answer shutdown for {reason}; sending SIGTERM to pid {pid}"
            );
            println!("sinomo {label} stopped");
            Ok(())
        }
        DaemonStopBeforeInstallStatus::TimedOut => Err(CliError::Network {
            address: paths.status.display().to_string(),
            detail: format!("{label} did not stop before {reason}"),
        }),
    }
}

pub(crate) fn print_stop_daemon_before_install_start(
    label: &str,
    reason: &str,
    start: DaemonStopBeforeInstallStart,
) {
    match start {
        DaemonStopBeforeInstallStart::StopRequested => {
            println!("sinomo {label} stop requested for {reason}");
        }
        DaemonStopBeforeInstallStart::ShutdownRequestFailed { detail } => {
            println!(
                "sinomo {label} shutdown request failed during {reason}; will retry: {detail}"
            );
        }
        DaemonStopBeforeInstallStart::Locked => {
            println!("sinomo {label} lock is held; waiting before {reason}");
        }
    }
}

pub(crate) fn start_installed_sinomo_daemon(
    binary: &Path,
    data_dir: &Path,
    args: &[String],
    paths: &DaemonPaths,
    label: &str,
    extra_env: &[(&str, String)],
) -> Result<(), CliError> {
    match daemon_install_start_installed_sinomo_daemon(
        binary,
        data_dir,
        args,
        paths,
        extra_env,
        Duration::from_secs(10),
        DAEMON_PROBE_TIMEOUT,
    )? {
        InstalledDaemonStartStatus::Started(status) => {
            println!(
                "sinomo {label} started: pid={} bind={} control={}",
                status.pid,
                daemon_bind_address(&status),
                status.address
            );
            Ok(())
        }
        InstalledDaemonStartStatus::ExitedBeforeReady { command, status } => {
            Err(CliError::Process {
                command,
                status,
                stderr: format!("{label} process exited before becoming ready"),
            })
        }
        InstalledDaemonStartStatus::TimedOut {
            status_path,
            child_id,
        } => Err(CliError::Network {
            address: status_path.display().to_string(),
            detail: format!("{label} process {child_id} did not become ready after install"),
        }),
    }
}

pub(crate) fn stop_daemon_if_running(paths: &DaemonPaths, label: &str) -> Result<(), CliError> {
    match daemon_install_stop_daemon_if_running(paths, DAEMON_PROBE_TIMEOUT)? {
        DaemonStopIfRunningStatus::StopRequested => {
            println!("sinomo {label} stop requested");
            Ok(())
        }
        DaemonStopIfRunningStatus::Locked => {
            println!("sinomo {label} lock is held; boot entry removal will continue");
            Ok(())
        }
        DaemonStopIfRunningStatus::NotRunning => {
            println!("sinomo {label} is not running");
            Ok(())
        }
    }
}
