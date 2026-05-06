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
    let existing_install = install_layout_exists(layout);
    fs::create_dir_all(&layout.bin_dir).map_err(|source| CliError::Io {
        path: layout.bin_dir.display().to_string(),
        detail: source.to_string(),
    })?;
    fs::create_dir_all(&layout.boot_dir).map_err(|source| CliError::Io {
        path: layout.boot_dir.display().to_string(),
        detail: source.to_string(),
    })?;
    let main_paths = DaemonPaths::with_root(layout.data_dir.join("daemon"));
    let shadow_paths = DaemonPaths::with_root(layout.data_dir.join("shadow-daemon"));
    let updating_binary = install_should_update_binary(&layout.binary)?
        || install_should_update_binary(&layout.shadow_binary)?;
    let restarting_for_options =
        options.start && install_running_daemons_need_restart(&options, &main_paths, &shadow_paths);
    if updating_binary || restarting_for_options {
        let reason = if updating_binary {
            "binary update"
        } else {
            "install option update"
        };
        stop_installed_daemons_before_install(layout, reason)?;
    }
    install_current_binary(&layout.binary)?;
    install_current_binary(&layout.shadow_binary)?;
    install_managed_source_repo(layout)?;
    write_boot_entries(layout, &options)?;
    remove_legacy_boot_entries(layout)?;
    activate_boot_entries(layout);
    #[cfg(not(test))]
    configure_user_bin_access(layout)?;
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
    if existing_install {
        print_existing_install_rollout_hint(layout);
    }

    if options.start {
        let main_args = vec![
            "daemon".to_string(),
            "run".to_string(),
            "--host".to_string(),
            options.host.clone(),
            "--port".to_string(),
            options.port.to_string(),
        ];
        ensure_installed_daemon_started(
            layout,
            &layout.binary,
            &main_args,
            &main_paths,
            "daemon",
            &[],
        )?;
        let shadow_args = vec![
            "daemon".to_string(),
            "shadow".to_string(),
            "run".to_string(),
            "--host".to_string(),
            options.host.clone(),
            "--port".to_string(),
            options.shadow_port.to_string(),
        ];
        let shadow_env = vec![
            (SHADOW_MAIN_BINARY_ENV, layout.binary.display().to_string()),
            (SHADOW_MAIN_HOST_ENV, options.host.clone()),
            (SHADOW_MAIN_PORT_ENV, options.port.to_string()),
        ];
        ensure_installed_daemon_started(
            layout,
            &layout.shadow_binary,
            &shadow_args,
            &shadow_paths,
            "shadow daemon",
            &shadow_env,
        )?;
    }
    Ok(())
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
        stop_daemon_if_running(
            &DaemonPaths::with_root(layout.data_dir.join("shadow-daemon")),
            "shadow daemon",
        )?;
        stop_daemon_if_running(
            &DaemonPaths::with_root(layout.data_dir.join("daemon")),
            "daemon",
        )?;
    }
    deactivate_boot_entries(layout);
    remove_boot_entry(&layout.main_boot_entry)?;
    remove_boot_entry(&layout.shadow_boot_entry)?;
    remove_legacy_boot_entries(layout)?;
    println!("removed boot entry: {}", layout.main_boot_entry.display());
    println!("removed boot entry: {}", layout.shadow_boot_entry.display());
    println!("data directory retained at: {}", layout.data_dir.display());
    Ok(())
}

pub(crate) fn install_layout_exists(layout: &InstallLayout) -> bool {
    layout.binary.exists()
        || layout.shadow_binary.exists()
        || layout.main_boot_entry.exists()
        || layout.shadow_boot_entry.exists()
        || SelfRolloutPaths::new(&layout.data_dir).source_repo.exists()
        || legacy_boot_entry_paths(layout)
            .iter()
            .any(|path| path.exists())
}

pub(crate) fn install_current_binary(destination: &Path) -> Result<(), CliError> {
    let current = current_executable_path()?;
    if same_path_if_exists(&current, destination) {
        return Ok(());
    }
    install_binary_atomically(&current, destination)
}

pub(crate) fn install_should_update_binary(destination: &Path) -> Result<bool, CliError> {
    if !destination.exists() {
        return Ok(false);
    }
    let current = current_executable_path()?;
    if same_path_if_exists(&current, destination) {
        return Ok(false);
    }
    let current_bytes = fs::read(&current).map_err(|source| CliError::Io {
        path: current.display().to_string(),
        detail: source.to_string(),
    })?;
    let destination_bytes = fs::read(destination).map_err(|source| CliError::Io {
        path: destination.display().to_string(),
        detail: source.to_string(),
    })?;
    Ok(current_bytes != destination_bytes)
}

pub(crate) fn install_managed_source_repo(layout: &InstallLayout) -> Result<(), CliError> {
    let rollout_paths = SelfRolloutPaths::new(&layout.data_dir);
    ensure_rollout_layout(&rollout_paths)?;
    let Some(source_repo) = discover_current_git_repo()? else {
        println!(
            "warning: managed source repo was not updated because the current directory is not inside a git checkout"
        );
        return Ok(());
    };
    Ok(deploy_managed_source_repo(&rollout_paths, &source_repo)?)
}

pub(crate) fn install_running_daemons_need_restart(
    options: &InstallOptions,
    main_paths: &DaemonPaths,
    shadow_paths: &DaemonPaths,
) -> bool {
    daemon_probe_status_needs_install_restart(
        probe_daemon_status(main_paths, DAEMON_PROBE_TIMEOUT),
        &options.host,
        options.port,
    ) || daemon_probe_status_needs_install_restart(
        probe_daemon_status(shadow_paths, DAEMON_PROBE_TIMEOUT),
        &options.host,
        options.shadow_port,
    )
}

pub(crate) fn daemon_probe_status_needs_install_restart(
    status: DaemonProbeStatus,
    host: &str,
    port: u16,
) -> bool {
    match status {
        DaemonProbeStatus::Running(status)
        | DaemonProbeStatus::Locked {
            status: Some(status),
        } => !daemon_status_matches_options(&status, host, port),
        DaemonProbeStatus::Locked { status: None } | DaemonProbeStatus::NotRunning => false,
    }
}

pub(crate) fn daemon_status_matches_options(status: &DaemonStatus, host: &str, port: u16) -> bool {
    status.host == host && status.port == port
}

pub(crate) fn current_executable_path() -> Result<PathBuf, CliError> {
    env::current_exe().map_err(|source| CliError::Io {
        path: "current executable".to_string(),
        detail: source.to_string(),
    })
}

pub(crate) fn same_path_if_exists(left: &Path, right: &Path) -> bool {
    let Ok(left) = fs::canonicalize(left) else {
        return false;
    };
    let Ok(right) = fs::canonicalize(right) else {
        return false;
    };
    left == right
}

pub(crate) fn default_boot_dir(data_dir: &SinomoDataDir) -> Result<PathBuf, CliError> {
    #[cfg(windows)]
    {
        if let Ok(appdata) = env::var("APPDATA") {
            let appdata = appdata.trim();
            if !appdata.is_empty() {
                return Ok(PathBuf::from(appdata)
                    .join("Microsoft")
                    .join("Windows")
                    .join("Start Menu")
                    .join("Programs")
                    .join("Startup"));
            }
        }
        Ok(data_dir.root().join("startup"))
    }
    #[cfg(not(windows))]
    {
        if let Ok(config_home) = env::var("XDG_CONFIG_HOME") {
            let config_home = config_home.trim();
            if !config_home.is_empty() {
                return Ok(PathBuf::from(config_home).join("systemd").join("user"));
            }
        }
        if let Ok(home) = env::var("HOME") {
            let home = home.trim();
            if !home.is_empty() {
                return Ok(PathBuf::from(home)
                    .join(".config")
                    .join("systemd")
                    .join("user"));
            }
        }
        Ok(data_dir.root().join("startup"))
    }
}

pub(crate) fn boot_entry_path(boot_dir: &Path, name: &str) -> PathBuf {
    #[cfg(windows)]
    {
        boot_dir.join(format!("{name}.vbs"))
    }
    #[cfg(not(windows))]
    {
        let file_name = name.to_ascii_lowercase().replace(' ', "-");
        boot_dir.join(format!("{file_name}.service"))
    }
}

pub(crate) fn write_boot_entries(
    layout: &InstallLayout,
    options: &InstallOptions,
) -> Result<(), CliError> {
    #[cfg(windows)]
    let main_command_prefix = ["daemon", "start"];
    #[cfg(not(windows))]
    let main_command_prefix = ["daemon", "run"];
    #[cfg(windows)]
    let shadow_command_prefix = ["daemon", "shadow", "start"];
    #[cfg(not(windows))]
    let shadow_command_prefix = ["daemon", "shadow", "run"];

    write_boot_entry(
        &layout.main_boot_entry,
        &layout.data_dir,
        &layout.binary,
        &main_command_prefix,
        &options.host,
        options.port,
        "Sinomo daemon",
        &[],
    )?;
    let shadow_env = vec![
        (SHADOW_MAIN_BINARY_ENV, layout.binary.display().to_string()),
        (SHADOW_MAIN_HOST_ENV, options.host.clone()),
        (SHADOW_MAIN_PORT_ENV, options.port.to_string()),
    ];
    write_boot_entry(
        &layout.shadow_boot_entry,
        &layout.data_dir,
        &layout.shadow_binary,
        &shadow_command_prefix,
        &options.host,
        options.shadow_port,
        "Sinomo shadow daemon",
        &shadow_env,
    )
}

pub(crate) fn write_boot_entry(
    path: &Path,
    data_dir: &Path,
    binary: &Path,
    command_prefix: &[&str],
    host: &str,
    port: u16,
    description: &str,
    extra_env: &[(&str, String)],
) -> Result<(), CliError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| CliError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let content = boot_entry_content(
        data_dir,
        binary,
        command_prefix,
        host,
        port,
        description,
        extra_env,
    );
    fs::write(path, content).map_err(|source| CliError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

pub(crate) fn boot_entry_content(
    data_dir: &Path,
    binary: &Path,
    command_prefix: &[&str],
    host: &str,
    port: u16,
    description: &str,
    extra_env: &[(&str, String)],
) -> String {
    let command = command_prefix.join(" ");
    #[cfg(windows)]
    {
        let _ = description;
        let extra_env = extra_env
            .iter()
            .map(|(key, value)| {
                format!(
                    "env(\"{}\") = \"{}\"\r\n",
                    vbs_string_literal(key),
                    vbs_string_literal(value)
                )
            })
            .collect::<String>();
        let command_line = format!(
            "\"{}\" {} --host {} --port {}",
            binary.display(),
            command,
            host,
            port
        );
        format!(
            "' Generated by sinomo install.\r\nDim shell\r\nSet shell = CreateObject(\"WScript.Shell\")\r\nDim env\r\nSet env = shell.Environment(\"Process\")\r\nenv(\"SINOMO_DATA_DIR\") = \"{}\"\r\n{}shell.Run \"{}\", 0, False\r\n",
            vbs_string_literal(&data_dir.display().to_string()),
            extra_env,
            vbs_string_literal(&command_line)
        )
    }
    #[cfg(not(windows))]
    {
        let extra_env = extra_env
            .iter()
            .map(|(key, value)| format!("Environment={key}={value}\n"))
            .collect::<String>();
        format!(
            "[Unit]\nDescription={description}\n\n[Service]\nEnvironment=SINOMO_DATA_DIR={}\n{}ExecStart={} {} --host {} --port {}\nRestart=on-failure\n\n[Install]\nWantedBy=default.target\n",
            data_dir.display(),
            extra_env,
            binary.display(),
            command,
            host,
            port
        )
    }
}

#[cfg(windows)]
pub(crate) fn vbs_string_literal(value: &str) -> String {
    value.replace('"', "\"\"")
}

pub(crate) fn remove_boot_entry(path: &Path) -> Result<(), CliError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(CliError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        }),
    }
}

pub(crate) fn remove_legacy_boot_entries(layout: &InstallLayout) -> Result<(), CliError> {
    for path in legacy_boot_entry_paths(layout) {
        remove_boot_entry(&path)?;
    }
    Ok(())
}

pub(crate) fn legacy_boot_entry_paths(layout: &InstallLayout) -> Vec<PathBuf> {
    #[cfg(windows)]
    {
        vec![
            layout
                .boot_dir
                .join(format!("{INSTALL_MAIN_BOOT_NAME}.cmd")),
            layout
                .boot_dir
                .join(format!("{INSTALL_SHADOW_BOOT_NAME}.cmd")),
        ]
    }
    #[cfg(not(windows))]
    {
        let _ = layout;
        Vec::new()
    }
}

#[cfg(windows)]
#[cfg_attr(test, allow(dead_code))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum UserPathUpdate {
    AlreadyPresent,
    Added,
}

#[cfg(windows)]
#[cfg_attr(test, allow(dead_code))]
pub(crate) fn configure_user_bin_access(layout: &InstallLayout) -> Result<(), CliError> {
    match ensure_windows_user_path_contains(&layout.bin_dir)? {
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
    ensure_local_bin_symlink(layout)
}

#[cfg(windows)]
#[cfg_attr(test, allow(dead_code))]
pub(crate) fn ensure_windows_user_path_contains(
    bin_dir: &Path,
) -> Result<UserPathUpdate, CliError> {
    let script = r#"
$bin = [System.IO.Path]::GetFullPath($env:SINOMO_INSTALL_BIN_DIR)
$userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
if ([string]::IsNullOrEmpty($userPath)) {
    $entries = @()
} else {
    $entries = $userPath -split ';'
}
$normalizedBin = $bin.TrimEnd('\')
$exists = $false
foreach ($entry in $entries) {
    if ($entry.Trim().TrimEnd('\') -ieq $normalizedBin) {
        $exists = $true
        break
    }
}
if ($exists) {
    Write-Output 'already'
} else {
    if ([string]::IsNullOrEmpty($userPath)) {
        $nextPath = $bin
    } else {
        $nextPath = $userPath.TrimEnd(';') + ';' + $bin
    }
    [Environment]::SetEnvironmentVariable('Path', $nextPath, 'User')
    Write-Output 'added'
}
"#;
    let output = Command::new("powershell")
        .args(["-NoProfile", "-NonInteractive", "-Command", script])
        .env("SINOMO_INSTALL_BIN_DIR", bin_dir)
        .output()
        .map_err(|source| CliError::Io {
            path: "powershell user PATH update".to_string(),
            detail: source.to_string(),
        })?;
    if !output.status.success() {
        return Err(CliError::Process {
            command: "powershell user PATH update".to_string(),
            status: output.status.code().unwrap_or(-1),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        });
    }
    match String::from_utf8_lossy(&output.stdout).trim() {
        "already" => Ok(UserPathUpdate::AlreadyPresent),
        "added" => Ok(UserPathUpdate::Added),
        other => Err(CliError::Process {
            command: "powershell user PATH update".to_string(),
            status: 0,
            stderr: format!("unexpected output `{other}`"),
        }),
    }
}

#[cfg(not(windows))]
pub(crate) fn ensure_local_bin_symlink(layout: &InstallLayout) -> Result<(), CliError> {
    let Some(home) = env::var_os("HOME") else {
        println!("warning: could not create ~/.local/bin/sinomo because HOME is not set");
        return Ok(());
    };
    let user_bin = PathBuf::from(home).join(".local").join("bin");
    fs::create_dir_all(&user_bin).map_err(|source| CliError::Io {
        path: user_bin.display().to_string(),
        detail: source.to_string(),
    })?;
    let link = user_bin.join("sinomo");
    if local_bin_symlink_is_current(&link, &layout.binary) {
        println!("user bin symlink already current: {}", link.display());
        return Ok(());
    }
    match fs::symlink_metadata(&link) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            fs::remove_file(&link).map_err(|source| CliError::Io {
                path: link.display().to_string(),
                detail: source.to_string(),
            })?;
        }
        Ok(_) => {
            println!(
                "warning: could not create {} because a non-symlink already exists",
                link.display()
            );
            return Ok(());
        }
        Err(source) if source.kind() == io::ErrorKind::NotFound => {}
        Err(source) => {
            return Err(CliError::Io {
                path: link.display().to_string(),
                detail: source.to_string(),
            });
        }
    }
    std::os::unix::fs::symlink(&layout.binary, &link).map_err(|source| CliError::Io {
        path: format!("{} -> {}", link.display(), layout.binary.display()),
        detail: source.to_string(),
    })?;
    println!("created user bin symlink: {}", link.display());
    Ok(())
}

#[cfg(not(windows))]
pub(crate) fn local_bin_symlink_is_current(link: &Path, target: &Path) -> bool {
    let Ok(existing_target) = fs::read_link(link) else {
        return false;
    };
    if existing_target == target {
        return true;
    }
    match (fs::canonicalize(&existing_target), fs::canonicalize(target)) {
        (Ok(existing_target), Ok(target)) => existing_target == target,
        _ => false,
    }
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
    Command::new("git")
        .arg("--version")
        .output()
        .is_ok_and(|output| output.status.success())
}

pub(crate) fn managed_source_git_status_lines(repo: &Path) -> Result<Vec<String>, CliError> {
    if !repo.join(".git").exists() {
        return Ok(Vec::new());
    }
    let output = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(["status", "--short"])
        .output()
        .map_err(|source| CliError::Io {
            path: format!("git -C {} status --short", repo.display()),
            detail: source.to_string(),
        })?;
    if !output.status.success() {
        return Err(CliError::Process {
            command: format!("git -C {} status --short", repo.display()),
            status: output.status.code().unwrap_or(-1),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        });
    }
    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect())
}

#[cfg(windows)]
pub(crate) fn activate_boot_entries(_layout: &InstallLayout) {}

#[cfg(not(windows))]
pub(crate) fn activate_boot_entries(layout: &InstallLayout) {
    systemctl_user_best_effort(&["daemon-reload"]);
    for entry in [&layout.main_boot_entry, &layout.shadow_boot_entry] {
        let Some(unit) = entry.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        systemctl_user_best_effort(&["enable", unit]);
    }
}

#[cfg(windows)]
pub(crate) fn deactivate_boot_entries(_layout: &InstallLayout) {}

#[cfg(not(windows))]
pub(crate) fn deactivate_boot_entries(layout: &InstallLayout) {
    for entry in [&layout.main_boot_entry, &layout.shadow_boot_entry] {
        let Some(unit) = entry.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        systemctl_user_best_effort(&["disable", unit]);
    }
    systemctl_user_best_effort(&["daemon-reload"]);
}

#[cfg(not(windows))]
pub(crate) fn systemctl_user_best_effort(args: &[&str]) {
    let output = Command::new("systemctl").arg("--user").args(args).output();
    match output {
        Ok(output) if output.status.success() => {}
        Ok(output) => {
            let stderr = String::from_utf8_lossy(&output.stderr);
            println!(
                "warning: systemctl --user {} failed: {}",
                args.join(" "),
                stderr.trim()
            );
        }
        Err(source) => {
            println!(
                "warning: systemctl --user {} could not run: {}",
                args.join(" "),
                source
            );
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
    stop_daemon_before_install(
        &DaemonPaths::with_root(layout.data_dir.join("shadow-daemon")),
        "shadow daemon",
        reason,
    )?;
    stop_daemon_before_install(
        &DaemonPaths::with_root(layout.data_dir.join("daemon")),
        "daemon",
        reason,
    )?;
    Ok(())
}

pub(crate) fn stop_daemon_before_install(
    paths: &DaemonPaths,
    label: &str,
    reason: &str,
) -> Result<(), CliError> {
    let mut next_shutdown_nudge = Instant::now();
    match probe_daemon_status(paths, DAEMON_PROBE_TIMEOUT) {
        DaemonProbeStatus::Running(status) => {
            match send_daemon_command(&status.address, "SHUTDOWN", DAEMON_PROBE_TIMEOUT) {
                Ok(_) => println!("sinomo {label} stop requested for {reason}"),
                Err(error) => {
                    if matches!(
                        probe_daemon_status(paths, DAEMON_PROBE_TIMEOUT),
                        DaemonProbeStatus::NotRunning
                    ) {
                        return Ok(());
                    }
                    println!(
                        "sinomo {label} shutdown request failed during {reason}; will retry: {error}"
                    );
                    next_shutdown_nudge = Instant::now() + Duration::from_secs(1);
                }
            }
        }
        DaemonProbeStatus::Locked { .. } => {
            println!("sinomo {label} lock is held; waiting before {reason}");
        }
        DaemonProbeStatus::NotRunning => return Ok(()),
    }

    let deadline = Instant::now() + Duration::from_secs(90);
    while Instant::now() < deadline {
        match probe_daemon_status(paths, DAEMON_PROBE_TIMEOUT) {
            DaemonProbeStatus::NotRunning => {
                println!("sinomo {label} stopped");
                return Ok(());
            }
            DaemonProbeStatus::Running(status) if Instant::now() >= next_shutdown_nudge => {
                let _ = send_daemon_command(&status.address, "SHUTDOWN", DAEMON_PROBE_TIMEOUT);
                next_shutdown_nudge = Instant::now() + Duration::from_secs(1);
            }
            DaemonProbeStatus::Running(_) | DaemonProbeStatus::Locked { .. } => {}
        }
        thread::sleep(Duration::from_millis(100));
    }

    if terminate_daemon_from_status_file(paths, label, reason)? {
        return Ok(());
    }

    Err(CliError::Network {
        address: paths.status.display().to_string(),
        detail: format!("{label} did not stop before {reason}"),
    })
}

pub(crate) fn terminate_daemon_from_status_file(
    paths: &DaemonPaths,
    label: &str,
    reason: &str,
) -> Result<bool, CliError> {
    let Ok(status) = read_daemon_status(paths) else {
        return Ok(false);
    };
    println!(
        "sinomo {label} did not answer shutdown for {reason}; sending SIGTERM to pid {}",
        status.pid
    );
    #[cfg(unix)]
    {
        unsafe extern "C" {
            fn kill(pid: i32, sig: i32) -> i32;
        }
        const SIGTERM: i32 = 15;
        let result = unsafe { kill(status.pid as i32, SIGTERM) };
        if result == -1 {
            return Err(CliError::Io {
                path: format!("pid {}", status.pid),
                detail: std::io::Error::last_os_error().to_string(),
            });
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            if matches!(
                probe_daemon_status(paths, DAEMON_PROBE_TIMEOUT),
                DaemonProbeStatus::NotRunning
            ) {
                println!("sinomo {label} stopped");
                return Ok(true);
            }
            thread::sleep(Duration::from_millis(100));
        }
    }
    Ok(false)
}

pub(crate) fn start_installed_sinomo_daemon(
    binary: &Path,
    data_dir: &Path,
    args: &[String],
    paths: &DaemonPaths,
    label: &str,
    extra_env: &[(&str, String)],
) -> Result<(), CliError> {
    let mut command = Command::new(binary);
    command.args(args).env("SINOMO_DATA_DIR", data_dir);
    for (key, value) in extra_env {
        command.env(key, value);
    }
    prepare_background_command(&mut command);
    let mut child = command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|source| CliError::Io {
            path: binary.display().to_string(),
            detail: source.to_string(),
        })?;

    let deadline = Instant::now() + Duration::from_secs(90);
    while Instant::now() < deadline {
        if let DaemonProbeStatus::Running(status) = probe_daemon_status(paths, DAEMON_PROBE_TIMEOUT)
        {
            println!(
                "sinomo {label} started: pid={} bind={} control={}",
                status.pid,
                daemon_bind_address(&status),
                status.address
            );
            return Ok(());
        }
        if let Some(status) = child.try_wait().map_err(|source| CliError::Io {
            path: binary.display().to_string(),
            detail: source.to_string(),
        })? {
            return Err(CliError::Process {
                command: format!("{} {}", binary.display(), args.join(" ")),
                status: status.code().unwrap_or(-1),
                stderr: format!("{label} process exited before becoming ready"),
            });
        }
        thread::sleep(Duration::from_millis(100));
    }

    Err(CliError::Network {
        address: paths.status.display().to_string(),
        detail: format!(
            "{label} process {} did not become ready after install",
            child.id()
        ),
    })
}

pub(crate) fn stop_daemon_if_running(paths: &DaemonPaths, label: &str) -> Result<(), CliError> {
    match probe_daemon_status(paths, DAEMON_PROBE_TIMEOUT) {
        DaemonProbeStatus::Running(status) => {
            let _ = send_daemon_command(&status.address, "SHUTDOWN", DAEMON_PROBE_TIMEOUT)?;
            println!("sinomo {label} stop requested");
            Ok(())
        }
        DaemonProbeStatus::Locked { .. } => {
            println!("sinomo {label} lock is held; boot entry removal will continue");
            Ok(())
        }
        DaemonProbeStatus::NotRunning => {
            println!("sinomo {label} is not running");
            Ok(())
        }
    }
}
