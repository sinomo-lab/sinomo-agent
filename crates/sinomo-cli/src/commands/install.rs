use super::*;
use sinomo_runtime::cli_registry::{
    CliCommandMatches, CliCommandPolicy, CliCommandSpec, CliOptionSpec, CliParseError, CliValueSpec,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct InstallOptions {
    pub(crate) desktop: sinomo_runtime::installer::DesktopInstallProfile,
    pub(crate) app_bundle: Option<PathBuf>,
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) shadow_port: u16,
    pub(crate) port_fallback: bool,
    pub(crate) shadow_port_fallback: bool,
    pub(crate) start: bool,
}

impl Default for InstallOptions {
    fn default() -> Self {
        Self {
            desktop: sinomo_runtime::installer::DesktopInstallProfile::Auto,
            app_bundle: None,
            host: INSTALL_DEFAULT_HOST.to_string(),
            port: SINOMO_DAEMON_DEFAULT_PORT,
            shadow_port: SINOMO_SHADOW_DAEMON_DEFAULT_PORT,
            port_fallback: true,
            shadow_port_fallback: true,
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

pub(crate) fn install_cli_command_handler() -> DeclarativeCommandHandler<InstallOptions> {
    DeclarativeCommandHandler::new(
        "install",
        CliCommandPolicy::default(),
        install_cli_spec,
        parse_install_matches,
        install,
    )
}

pub(crate) fn uninstall_cli_command_handler() -> DeclarativeCommandHandler<UninstallOptions> {
    DeclarativeCommandHandler::new(
        "uninstall",
        CliCommandPolicy::default(),
        uninstall_cli_spec,
        parse_uninstall_matches,
        uninstall,
    )
}

fn install_cli_spec() -> CliCommandSpec {
    CliCommandSpec::new("install", "install or update the local Sinomo runtime").with_options([
        CliOptionSpec::flag(
            "--desktop",
            "install the bundled desktop app as well as the runtime",
        ),
        CliOptionSpec::flag("--headless", "install only the runtime"),
        CliOptionSpec::value(
            "--app-bundle",
            "verified desktop release directory",
            CliValueSpec::new("path", "path"),
        ),
        CliOptionSpec::value(
            "--host",
            "daemon bind host",
            CliValueSpec::new("host", "host"),
        ),
        CliOptionSpec::value(
            "--port",
            "main daemon port",
            CliValueSpec::new("port", "port"),
        ),
        CliOptionSpec::value(
            "--shadow-port",
            "shadow daemon port",
            CliValueSpec::new("port", "port"),
        ),
        CliOptionSpec::flag(
            "--no-port-fallback",
            "require the preferred main daemon port",
        ),
        CliOptionSpec::flag(
            "--no-shadow-port-fallback",
            "require the preferred shadow daemon port",
        ),
        CliOptionSpec::flag("--no-start", "install without starting daemons"),
    ])
}

fn uninstall_cli_spec() -> CliCommandSpec {
    CliCommandSpec::new("uninstall", "remove local daemon boot entries").with_options([
        CliOptionSpec::flag("--no-stop", "leave running daemons untouched"),
    ])
}

fn parse_install_matches(matches: &CliCommandMatches) -> Result<InstallOptions, CliParseError> {
    let mut options = InstallOptions::default();
    if matches.contains("--desktop") && matches.contains("--headless") {
        return Err(CliParseError::Parse {
            detail: "choose --desktop or --headless".into(),
        });
    }
    options.desktop = if matches.contains("--headless") {
        sinomo_runtime::installer::DesktopInstallProfile::Headless
    } else if matches.contains("--desktop") {
        sinomo_runtime::installer::DesktopInstallProfile::Desktop
    } else {
        sinomo_runtime::installer::DesktopInstallProfile::Auto
    };
    options.app_bundle = matches.value("--app-bundle").map(PathBuf::from);
    if matches.contains("--headless") && options.app_bundle.is_some() {
        return Err(CliParseError::Parse {
            detail: "--headless cannot be combined with --app-bundle".into(),
        });
    }
    if let Some(host) = matches.value("--host") {
        options.host = host.to_string();
    }
    if let Some(value) = matches.value("--port") {
        options.port = parse_port("install port", value).map_err(semantic_parse_error)?;
        options.port_fallback = false;
    }
    if let Some(value) = matches.value("--shadow-port") {
        options.shadow_port =
            parse_port("install shadow port", value).map_err(semantic_parse_error)?;
        options.shadow_port_fallback = false;
    }
    if matches.contains("--no-port-fallback") {
        options.port_fallback = false;
    }
    if matches.contains("--no-shadow-port-fallback") {
        options.shadow_port_fallback = false;
    }
    options.start = !matches.contains("--no-start");
    Ok(options)
}

fn parse_uninstall_matches(matches: &CliCommandMatches) -> Result<UninstallOptions, CliParseError> {
    Ok(UninstallOptions {
        stop: !matches.contains("--no-stop"),
    })
}

#[cfg(test)]
pub(crate) fn parse_install_schema_for_test(args: &[String]) -> Result<InstallOptions, CliError> {
    parse_schema_command_for_test(install_cli_spec(), args, parse_install_matches)
}

#[cfg(test)]
pub(crate) fn parse_uninstall_schema_for_test(
    args: &[String],
) -> Result<UninstallOptions, CliError> {
    parse_schema_command_for_test(uninstall_cli_spec(), args, parse_uninstall_matches)
}

pub(crate) fn parse_port(label: &str, value: &str) -> Result<u16, CliError> {
    value.parse::<u16>().map_err(|source| CliError::Parse {
        detail: format!("invalid {label} `{value}`: {source}"),
    })
}

pub(crate) fn data_dir_storage_contract_findings(
    data_dir: impl AsRef<Path>,
) -> Result<Vec<ClusterDoctorFinding>, CliError> {
    let data_dir = data_dir.as_ref();
    let report = cli_data_dir_layout(data_dir)?.verify_data_dir(data_dir)?;
    let mut unregistered = report.unregistered;
    unregistered.retain(|path| !known_transient_data_dir_path(path));
    if unregistered.is_empty() {
        Ok(Vec::new())
    } else {
        Ok(vec![ClusterDoctorFinding {
            severity: "warning".to_string(),
            code: "storage-unregistered-paths".to_string(),
            detail: format!(
                "local Sinomo data directory contains unregistered paths: {}",
                unregistered.join(", ")
            ),
        }])
    }
}

fn known_transient_data_dir_path(relative_path: &str) -> bool {
    if !relative_path.starts_with("cluster/.") {
        return false;
    }
    let Some(file_name) = relative_path.rsplit('/').next() else {
        return false;
    };
    file_name.contains(".tmp-") || file_name.contains(".create-new-tmp-")
}

pub(crate) fn warn_data_dir_storage_contracts(data_dir: impl AsRef<Path>) -> Result<(), CliError> {
    for finding in data_dir_storage_contract_findings(data_dir)? {
        eprintln!("warning: {}: {}", finding.code, finding.detail);
    }
    Ok(())
}

pub(crate) fn cli_data_dir_layout(
    data_dir_root: &Path,
) -> Result<sinomo_runtime::storage::StorageLayout, CliError> {
    let data_dir = SinomoDataDir::new(data_dir_root);
    let cluster_initialized = ClusterPaths::new(&data_dir).state.exists();
    let catalog = sinomo_product_profile::base_component_catalog(
        &data_dir,
        sinomo_product_profile::ProductComponentProfile::main(cluster_initialized),
    )
    .map_err(|error| CliError::Parse {
        detail: format!("build storage component catalog: {error}"),
    })?;
    let mut layout = sinomo_runtime::storage::sinomo_data_dir_layout_with_catalog(&catalog)?;
    sinomo_agent::register_storage_paths(&mut layout)?;
    sinomo_pytorch::register_storage_paths(&mut layout)?;
    register_cli_application_storage_paths(&mut layout)?;
    for hook in &crate::cli_runtime_config().storage_layout_hooks {
        hook(&mut layout)?;
    }
    Ok(layout)
}

fn register_cli_application_storage_paths(
    layout: &mut sinomo_runtime::storage::StorageLayout,
) -> Result<(), sinomo_runtime::storage::StorageContractError> {
    for relative_path in ["logs/app.log", "logs/app.log.1"] {
        layout.register_human_text(
            "sinomo-app",
            sinomo_runtime::storage::StorageRing::Application,
            relative_path,
        )?;
    }
    layout.register_escape_path(
        "sinomo-cli",
        sinomo_runtime::storage::StorageRing::Application,
        "events/tui/sessions",
        "terminal UI session records, transcripts, and event logs",
    )?;
    layout.register_escape_path(
        "sinomo-app",
        sinomo_runtime::storage::StorageRing::Application,
        "app/sessions",
        "desktop app chat session cache and metadata",
    )
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
    let desktop = sinomo_runtime::installer::prepare_desktop_install(
        options.desktop,
        options.app_bundle.as_deref(),
    )
    .map_err(|detail| CliError::Parse { detail })?;
    let _work_drain =
        sinomo_runtime::rollout::drain_runtime_work(&layout.data_dir, Duration::from_secs(300))?;
    let managed_source_head_before = installed_managed_source_head(layout);
    let plan = daemon_prepare_installed_daemon_install_plan(
        layout,
        InstalledDaemonInstallPlanRequest {
            host: options.host.clone(),
            port: options.port,
            shadow_port: options.shadow_port,
            port_fallback: options.port_fallback,
            shadow_port_fallback: options.shadow_port_fallback,
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
    let installed_modules = install_current_modules(layout)?;
    if let Some(source) = desktop {
        let status = sinomo_runtime::installer::install_desktop_bundle(layout, &source)
            .map_err(|detail| CliError::Parse { detail })?;
        println!(
            "desktop app installed: {:?}; launch with sinomo-app",
            status.phase
        );
    }
    let managed_source_updated =
        managed_source_head_before != installed_managed_source_head(layout);
    write_boot_entries(layout, &options)?;
    activate_boot_entries(layout);
    #[cfg(not(test))]
    configure_user_bin_access(layout)?;
    warn_data_dir_storage_contracts(&layout.data_dir)?;
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
    for module in installed_modules {
        println!(
            "installed native module: {} ({})",
            module.state.module_id, module.state.content_hash
        );
    }
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

#[cfg(not(test))]
pub(crate) fn install_current_binary(destination: &Path) -> Result<(), CliError> {
    Ok(install_current_executable(destination)?)
}

#[cfg(test)]
pub(crate) fn install_current_binary(destination: &Path) -> Result<(), CliError> {
    // The CLI test executable is over a gigabyte with debug symbols. Copying it
    // twice per install-layout test exhausts small/tmpfs CI quotas and does not
    // exercise the atomic installer (that behavior is covered in
    // `sinomo-rollout`). These tests need only a stable installed artifact.
    if let Some(parent) = destination.parent() {
        std::fs::create_dir_all(parent).map_err(|source| CliError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    std::fs::write(destination, b"sinomo cli install test executable").map_err(|source| {
        CliError::Io {
            path: destination.display().to_string(),
            detail: source.to_string(),
        }
    })
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

#[cfg(test)]
pub(crate) fn install_current_modules(
    _layout: &InstallLayout,
) -> Result<Vec<sinomo_runtime::module::NativeDylibModuleInstallResult>, CliError> {
    Ok(Vec::new())
}

#[cfg(not(test))]
pub(crate) fn install_current_modules(
    layout: &InstallLayout,
) -> Result<Vec<sinomo_runtime::module::NativeDylibModuleInstallResult>, CliError> {
    let rollout_paths = SelfRolloutPaths::new(&layout.data_dir);
    let repo = rollout_paths.source_repo.clone();
    if !repo.join("Cargo.toml").exists() {
        println!(
            "warning: native modules were not installed because the managed source repo is not initialized at {}",
            repo.display()
        );
        return Ok(Vec::new());
    }
    let artifacts = prepare_rollout_artifacts(&rollout_paths, &repo, Some(&layout.binary))?;
    install_prepared_native_dylib_modules(&layout.data_dir, &repo, &artifacts.native_dylib_modules)
}

pub(crate) fn install_prepared_native_dylib_modules(
    data_dir: &Path,
    installed_managed_source: &Path,
    artifacts: &[LocalRolloutNativeDylibModuleArtifact],
) -> Result<Vec<sinomo_runtime::module::NativeDylibModuleInstallResult>, CliError> {
    let rollout_paths = SelfRolloutPaths::new(data_dir);
    with_rollout_activation_admission(&rollout_paths, || {
        let installed = rollout_install_local_rollout_native_dylib_modules(data_dir, artifacts)?;
        reconcile_rollout_native_dylib_platforms(data_dir, installed_managed_source)?;
        Ok(installed)
    })
}

pub(crate) fn default_boot_dir(data_dir: &SinomoDataDir) -> Result<PathBuf, CliError> {
    Ok(daemon_default_boot_dir(data_dir))
}

pub(crate) fn write_boot_entries(
    layout: &InstallLayout,
    options: &InstallOptions,
) -> Result<(), CliError> {
    daemon_write_install_boot_entries(
        layout,
        &options.host,
        options.port,
        options.shadow_port,
        options.port_fallback,
        options.shadow_port_fallback,
    )
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
    println!("roll out the managed source to the cluster with: sinomo rollout cluster");
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
