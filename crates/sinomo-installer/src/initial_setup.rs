//! Shared initial-installation, integration and removal contract for CLI and graphical setup.
mod removal;
pub mod services;
use serde::{Deserialize, Serialize};
use sinomo_core::local;
use sinomo_rollout::desktop;
use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
pub const RECEIPT: &str = "sinomo-install.json";
const DATA_OWNER: &str = "config/installer-owner";

#[derive(Debug, Serialize, Deserialize)]
pub struct Receipt {
    pub schema: u32,
    pub install_dir: PathBuf,
    pub data_dir: PathBuf,
    pub owns_data: bool,
    pub files: Vec<String>,
    #[serde(default)]
    pub integrations: Vec<PathBuf>,
    #[serde(default)]
    pub services: Option<services::Registration>,
    #[serde(default)]
    pub setup_owner: Option<ProcessIdentity>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProcessIdentity {
    pub pid: u32,
    pub start_time: u64,
    pub exe: PathBuf,
}

/// Optional shell integration shared by CLI and graphical initial setup.
#[derive(Clone, Debug, Default)]
pub struct IntegrationOptions {
    pub add_to_path: bool,
    pub desktop_shortcut: bool,
    pub menu_shortcut: bool,
}

pub fn default_install_dir() -> Result<PathBuf> {
    if cfg!(windows) {
        Ok(env::var_os("LOCALAPPDATA")
            .map(PathBuf::from)
            .unwrap_or(home()?.join("AppData/Local"))
            .join("Programs/Sinomo"))
    } else {
        Ok(home()?.join(".local/opt/sinomo"))
    }
}

/// Initial setup preserves source/releases and registers and starts per-user services.
pub fn install(
    executable: &Path,
    desktop_source: Option<&Path>,
    root: &Path,
    data: &Path,
    integrations: &IntegrationOptions,
    services: &services::Options,
) -> Result<()> {
    if !root.is_absolute() || !data.is_absolute() {
        return Err("installation and data directories must be absolute".into());
    }
    if desktop_source.is_none() && (integrations.desktop_shortcut || integrations.menu_shortcut) {
        return Err(
            "App shortcuts require a desktop bundle; supply --app-bundle DIRECTORY.".into(),
        );
    }
    if let Some(source) = desktop_source {
        desktop::verify_bundle(source, &desktop::read_manifest(source)?)?;
    }
    fs::create_dir_all(root)?;
    fs::create_dir_all(data)?;
    validate_locations(root, data)?;
    validate_new_owner(root, data)?;
    if fs::read_dir(root)?.next().is_some() {
        return Err("Choose an empty installation directory. Use rollout to evolve an existing installation.".into());
    }
    let retain_components = retained_runtime(data);
    let bin = data.join("bin");
    fs::create_dir_all(&bin)?;
    let _admission =
        sinomo_rollout::acquire_installation_guard(&sinomo_rollout::SelfRolloutPaths::new(data))?;
    local::inhibit_deployment(data)?;
    if !bin.join(binary("sinomo")).exists() {
        fs::copy(executable, bin.join(binary("sinomo")))?;
    }
    if let Some(source) = desktop_source {
        let manifest = desktop::read_manifest(source)?;
        for file in &manifest.files {
            if !desktop::is_desktop_payload_file(&file.path) {
                continue;
            }
            let destination = bin.join(&file.path);
            if !destination.exists() {
                fs::create_dir_all(destination.parent().ok_or("no component parent")?)?;
                fs::copy(source.join(&file.path), destination)?;
            }
        }
        if !bin.join("desktop-release.json").exists() {
            fs::copy(
                source.join("desktop-release.json"),
                bin.join("desktop-release.json"),
            )?;
        }
        if source.join(binary("sinomo-setup")).is_file() {
            let destination = if cfg!(target_os = "linux") {
                bin.join(binary("sinomo-setup"))
            } else {
                root.join(binary("sinomo-setup"))
            };
            if !destination.exists() {
                fs::copy(source.join(binary("sinomo-setup")), destination)?;
            }
        }
    }
    let mut files = Vec::new();
    files.extend([
        binary("sinomo-setup"),
        local::INSTALLED_DATA_DIR_FILE.into(),
        RECEIPT.into(),
        "windows-integration.ps1".into(),
        "windows-integration.json.next".into(),
        "uninstall.ps1".into(),
        crate::bundle::PAYLOAD.into(),
    ]);
    configure_with_files(root, data, desktop_source, files)?;
    let package = executable
        .parent()
        .filter(|p| p.join(crate::bundle::PAYLOAD).is_file())
        .or(desktop_source)
        .or_else(|| executable.parent())
        .ok_or("no package directory")?;
    complete_payload(package, data, desktop_source, retain_components)?;
    drop(_admission);
    #[cfg(windows)]
    {
        fs::write(
            root.join("windows-integration.ps1"),
            include_str!("initial_setup/windows-integration.ps1"),
        )?;
        fs::write(
            root.join("uninstall.ps1"),
            include_str!("initial_setup/uninstall.ps1"),
        )?;
        let mut args = vec!["--register".into()];
        if integrations.add_to_path {
            args.push("--path".into());
        }
        if integrations.desktop_shortcut {
            args.push("--desktop".into());
        }
        if integrations.menu_shortcut {
            args.push("--menu".into());
        }
        windows_integration(root, "install", &args)?;
    }
    #[cfg(target_os = "linux")]
    linux_integration(
        root,
        integrations.add_to_path,
        integrations.menu_shortcut,
        integrations.desktop_shortcut,
    )?;
    complete_services(root, services)?;
    Ok(())
}

pub fn complete_services(root: &Path, options: &services::Options) -> Result<()> {
    services::install(root, options)
}

pub fn home() -> Result<PathBuf> {
    env::var_os(if cfg!(windows) { "USERPROFILE" } else { "HOME" })
        .map(PathBuf::from)
        .ok_or_else(|| "home directory is unavailable".into())
}

pub fn binary(stem: &str) -> String {
    format!("{stem}{}", env::consts::EXE_SUFFIX)
}

pub fn payload_files() -> Vec<String> {
    desktop::DESKTOP_BINARY_STEMS
        .into_iter()
        .map(binary)
        .chain([binary("sinomo"), "desktop-release.json".to_owned()])
        .collect()
}

pub fn validate_locations(root: &Path, data: &Path) -> Result<()> {
    if !root.is_absolute() || !data.is_absolute() {
        return Err("installation and data directories must be absolute".into());
    }
    // Resolve filesystem aliases before any destructive operation or ownership claim.
    let root = fs::canonicalize(root)?;
    let data = fs::canonicalize(data)?;
    let home = fs::canonicalize(home()?)?;
    if root.parent().is_none()
        || data.parent().is_none()
        || home.starts_with(&root)
        || home.starts_with(&data)
        || root.starts_with(&data)
        || data.starts_with(&root)
    {
        return Err("Choose separate application and data folders, neither a filesystem root nor your home directory or its ancestors.".into());
    }
    if root.to_str().is_none_or(|p| p.contains(['\n', '\r', '\0']))
        || data.to_str().is_none_or(|p| p.contains(['\n', '\r', '\0']))
    {
        return Err("directory names must be UTF-8 and cannot contain line breaks".into());
    }
    Ok(())
}

pub fn configure(root: &Path, data: &Path) -> Result<()> {
    fs::create_dir_all(data)?;
    validate_locations(root, data)?;
    validate_new_owner(root, data)?;
    let _admission =
        sinomo_rollout::acquire_installation_guard(&sinomo_rollout::SelfRolloutPaths::new(data))?;
    let retain_components = retained_runtime(data);
    local::inhibit_deployment(data)?;
    configure_from_guarded(root, data, root)?;
    complete_payload(root, data, Some(root), retain_components)?;
    remove_extracted_payload(root, root)?;
    fs::remove_file(root.join(crate::bundle::PAYLOAD))?;
    Ok(())
}

pub fn configure_from(root: &Path, data: &Path, source: &Path) -> Result<()> {
    let _admission =
        sinomo_rollout::acquire_installation_guard(&sinomo_rollout::SelfRolloutPaths::new(data))?;
    configure_from_guarded(root, data, source)?;
    remove_extracted_payload(root, source)
}

fn configure_from_guarded(root: &Path, data: &Path, source: &Path) -> Result<()> {
    fs::create_dir_all(data)?;
    validate_locations(root, data)?;
    validate_new_owner(root, data)?;
    let bin = data.join("bin");
    fs::create_dir_all(&bin)?;
    // NSIS extracts temporary payloads beside setup. Only DATA/bin is an
    // executable installation; retained/evolved components always win.
    for name in payload_files() {
        if source.join(&name).is_file() && !bin.join(&name).exists() {
            fs::copy(source.join(&name), bin.join(&name))?;
        }
    }
    let files = installed_files()
        .into_iter()
        .chain([
            binary("sinomo-setup"),
            local::INSTALLED_DATA_DIR_FILE.into(),
            RECEIPT.into(),
            "windows-integration.ps1".into(),
            "windows-integration.json.next".into(),
            crate::bundle::PAYLOAD.into(),
        ])
        .collect();
    configure_with_files(root, data, Some(source), files)?;
    Ok(())
}

fn remove_extracted_payload(root: &Path, source: &Path) -> Result<()> {
    if fs::canonicalize(source)? == fs::canonicalize(root)? {
        for name in payload_files() {
            if root.join(&name).is_file() {
                fs::remove_file(root.join(name))?;
            }
        }
    }
    Ok(())
}

fn configure_with_files(
    root: &Path,
    data: &Path,
    source: Option<&Path>,
    files: Vec<String>,
) -> Result<()> {
    if !root.is_absolute() || !data.is_absolute() {
        return Err("installation and data directories must be absolute".into());
    }
    fs::create_dir_all(data)?;
    validate_locations(root, data)?;
    let root = fs::canonicalize(root)?;
    let data = fs::canonicalize(data)?;
    if root.join(RECEIPT).exists() || root.join(local::INSTALLED_DATA_DIR_FILE).exists() {
        return Err(
            "Sinomo is already installed here; setup never replaces an evolved installation."
                .into(),
        );
    }
    if let Some(source) = source {
        desktop::verify_bundle(source, &desktop::read_manifest(source)?)?;
    }
    // Bind the explicitly selected directory, including reused cluster data. Never
    // take deletion ownership away from another registered installation.
    let owns_data = match fs::read_to_string(data.join(DATA_OWNER)) {
        Ok(owner) => owner == root.to_string_lossy() || !Path::new(&owner).join(RECEIPT).is_file(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
        Err(error) => return Err(error.into()),
    };
    if !owns_data {
        return Err("data belongs to another registered installation; uninstall it first".into());
    }
    let receipt = Receipt {
        schema: 1,
        install_dir: root.clone(),
        data_dir: data.clone(),
        owns_data,
        files,
        integrations: Vec::new(),
        services: None,
        setup_owner: match fs::read(data.join("rollout/activation.owner.json")) {
            Ok(bytes) => Some(serde_json::from_slice(&bytes)?),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        },
    };
    // Persist recovery/removal information before installing managed state or integrations.
    write_receipt(&root, &receipt)?;
    if owns_data {
        fs::create_dir_all(data.join("config"))?;
        fs::write(data.join(DATA_OWNER), root.to_string_lossy().as_bytes())?;
    }
    fs::write(
        root.join(local::INSTALLED_DATA_DIR_FILE),
        data.to_str().ok_or("invalid data path")?,
    )?;
    fs::create_dir_all(data.join("bin"))?;
    fs::write(
        data.join("bin").join(local::INSTALLED_DATA_DIR_FILE),
        data.to_str().ok_or("invalid data path")?,
    )?;
    // Reuse an evolved data directory without even offering the installer as a candidate.
    let releases = desktop::DesktopReleases::new(&data);
    if let Some(source) = source {
        if releases.status()?.active_release.is_none()
            && local::ensure_deployment_enabled(&data).is_ok()
        {
            let layout =
                crate::InstallLayout::new(&local::SinomoDataDir::new(&data), data.join("boot"));
            crate::install_desktop_bundle(&layout, source)?;
        }
    }
    Ok(())
}

fn validate_new_owner(root: &Path, data: &Path) -> Result<()> {
    let root = fs::canonicalize(root)?;
    if root.join(RECEIPT).exists() || root.join(local::INSTALLED_DATA_DIR_FILE).exists() {
        return Err("Sinomo is already registered here; uninstall before reinstalling".into());
    }
    match fs::read_to_string(data.join(DATA_OWNER)) {
        Ok(owner) if Path::new(&owner) != root && Path::new(&owner).join(RECEIPT).is_file() => {
            Err("data belongs to another registered installation; uninstall it first".into())
        }
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => Err(error.into()),
        _ => Ok(()),
    }
}

fn retained_runtime(data: &Path) -> bool {
    local::SinomoDataDir::new(data).binary("sinomo").is_file()
        && sinomo_build::BuildPaths::new(data)
            .managed_source
            .join(".git")
            .exists()
}

fn complete_payload(
    package: &Path,
    data: &Path,
    desktop_source: Option<&Path>,
    retain_components: bool,
) -> Result<()> {
    crate::bundle::install_missing(package, data, !retain_components)?;
    local::enable_deployment(data)?;
    if let Some(source) = desktop_source {
        let releases = desktop::DesktopReleases::new(data);
        if releases.status()?.active_release.is_none() {
            let layout =
                crate::InstallLayout::new(&local::SinomoDataDir::new(data), data.join("boot"));
            if let Err(error) = crate::install_desktop_bundle(&layout, source) {
                local::inhibit_deployment(data)?;
                return Err(error.into());
            }
        }
    }
    Ok(())
}

fn installed_files() -> Vec<String> {
    if cfg!(target_os = "linux") {
        vec![
            "sinomo".into(),
            "sinomo-app".into(),
            "Sinomo.AppImage".into(),
        ]
    } else {
        payload_files()
    }
}

fn write_receipt(root: &Path, receipt: &Receipt) -> Result<()> {
    sinomo_core::storage::durable_file::write_file_atomic(
        &root.join(RECEIPT),
        &serde_json::to_vec_pretty(receipt)?,
        sinomo_core::storage::durable_file::DurableAtomicFileOptions::new("installer", "receipt")
            .sync_all(),
    )?;
    Ok(())
}

pub fn read_receipt(root: &Path) -> Result<Receipt> {
    let receipt: Receipt = serde_json::from_slice(&fs::read(root.join(RECEIPT))?)?;
    if receipt.schema != 1 || fs::canonicalize(root)? != receipt.install_dir {
        return Err("installation receipt does not match this directory".into());
    }
    for file in &receipt.files {
        if Path::new(file).components().count() != 1 || file == "." || file == ".." {
            return Err("invalid installed file in receipt".into());
        }
    }
    Ok(receipt)
}

pub fn uninstall(root: &Path, delete_data: bool) -> Result<()> {
    uninstall_inner(root, delete_data, false)
}

/// NSIS retains responsibility for its registered uninstaller and the running helper.
#[cfg(windows)]
pub fn uninstall_nsis(root: &Path, delete_data: bool) -> Result<()> {
    uninstall_inner(root, delete_data, true)
}

fn uninstall_inner(root: &Path, delete_data: bool, _nsis_managed: bool) -> Result<()> {
    let mut receipt = read_receipt(root)?;
    let data_existed = match fs::symlink_metadata(&receipt.data_dir) {
        Ok(_) => true,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(error) => return Err(error.into()),
    };
    if !receipt.data_dir.is_absolute() || receipt.data_dir.parent().is_none() {
        return Err("invalid recorded data directory".into());
    }
    if data_existed {
        validate_locations(root, &receipt.data_dir)?;
        if fs::canonicalize(&receipt.data_dir)? != receipt.data_dir
            || !receipt.owns_data
            || fs::read_to_string(receipt.data_dir.join(DATA_OWNER))?
                != receipt.install_dir.to_string_lossy()
        {
            return Err(
                "deployment ownership changed; refusing to stop or remove another installation"
                    .into(),
            );
        }
        if delete_data {
            validate_locations(root, &receipt.data_dir)?;
            if !receipt.owns_data
                || fs::read_to_string(receipt.data_dir.join(DATA_OWNER))?
                    != receipt.install_dir.to_string_lossy()
            {
                return Err("This data directory is bound to another installation, or its ownership changed. Data removal is refused.".into());
            }
            if fs::symlink_metadata(&receipt.data_dir)?
                .file_type()
                .is_symlink()
                || fs::canonicalize(&receipt.data_dir)? != receipt.data_dir
            {
                return Err("Data directory identity changed; data removal is refused.".into());
            }
        }
        local::inhibit_deployment(&receipt.data_dir)?;
    }
    removal::stop(root, &receipt.data_dir, receipt.services.as_ref())?;
    // Concurrent setup records prepared effects before making them.
    receipt = read_receipt(root)?;
    let paths = sinomo_rollout::SelfRolloutPaths::new(&receipt.data_dir);
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut admission = if receipt.data_dir.try_exists()? {
        Some(loop {
            match sinomo_rollout::acquire_installation_guard(&paths) {
                Ok(guard) => break guard,
                Err(error) if Instant::now() >= deadline => return Err(error.into()),
                Err(_) => {
                    removal::terminate_owned(root, &receipt.data_dir)?;
                    std::thread::sleep(Duration::from_millis(50));
                }
            }
        })
    } else {
        None
    };
    if admission.is_some() {
        // A setup that was already admitted may have cleared the first fence
        // before being stopped. Removal now owns admission and wins that race.
        local::inhibit_deployment(&receipt.data_dir)?;
    }
    if let Some(services) = &receipt.services {
        services::remove(services)?;
    }
    #[cfg(windows)]
    windows_integration(root, "uninstall", &[])?;
    #[cfg(target_os = "linux")]
    linux_remove_integrations(&receipt)?;
    // Windows cannot unlink an open advisory lock. The durable fence remains
    // authoritative after the lock closes, including while data is removed.
    if (delete_data || !data_existed) && receipt.data_dir.try_exists()? {
        drop(admission.take());
        if fs::canonicalize(&receipt.data_dir)? != receipt.data_dir {
            return Err("data identity changed during removal".into());
        }
        if let Ok(owner) = fs::read_to_string(receipt.data_dir.join(DATA_OWNER)) {
            if owner != receipt.install_dir.to_string_lossy() {
                return Err("data now belongs to another installation".into());
            }
        }
        #[cfg(windows)]
        {
            let executable = fs::canonicalize(env::current_exe()?)?;
            if executable.starts_with(&receipt.data_dir) {
                // Rename the running maintenance image out of DATA before
                // deletion. No native modules are loaded by maintenance CLI.
                let retired = receipt
                    .data_dir
                    .parent()
                    .ok_or("no data parent")?
                    .join(format!(".sinomo-uninstall-{}.exe", std::process::id()));
                if retired.exists() {
                    return Err("retired uninstall image already exists".into());
                }
                fs::rename(&executable, &retired)?;
                schedule_running_file_removal(
                    &retired,
                    receipt.data_dir.parent().ok_or("no data parent")?,
                    false,
                )?;
            }
        }
        fs::remove_dir_all(&receipt.data_dir)?;
        // Catch a finite startup that raced shutdown before its image disappeared.
        removal::terminate_owned(root, &receipt.data_dir)?;
        if receipt.data_dir.try_exists()? {
            if fs::canonicalize(&receipt.data_dir)? != receipt.data_dir {
                return Err("data identity changed during removal".into());
            }
            fs::remove_dir_all(&receipt.data_dir)?;
        }
    }
    // Never recursively remove an installation folder: users may have added their own files.
    for file in &receipt.files {
        #[cfg(windows)]
        if env::current_exe()
            .ok()
            .and_then(|exe| fs::canonicalize(exe).ok())
            .is_some_and(|exe| fs::canonicalize(root.join(file)).is_ok_and(|path| path == exe))
        {
            if !_nsis_managed {
                schedule_running_file_removal(&root.join(file), root, true)?;
            }
            continue;
        }
        let path = root.join(file);
        match fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    #[cfg(windows)]
    if !_nsis_managed {
        let nsis = root.join("Uninstall.exe");
        if nsis.is_file() {
            fs::remove_file(nsis)?;
        }
        unregister_windows(root)?;
    }
    if let Some(owner) = receipt.setup_owner {
        let prefix = format!(".sinomo-install.json.receipt-{}-", owner.pid);
        for entry in fs::read_dir(root)? {
            let entry = entry?;
            if entry.file_type()?.is_file()
                && entry
                    .file_name()
                    .to_str()
                    .and_then(|name| name.strip_prefix(&prefix))
                    .is_some_and(|sequence| sequence.parse::<u64>().is_ok())
            {
                fs::remove_file(entry.path())?;
            }
        }
    }
    let _ = fs::remove_dir(root);
    Ok(())
}

#[cfg(windows)]
fn unregister_windows(root: &Path) -> Result<()> {
    use std::os::windows::process::CommandExt;
    let output = Command::new("powershell.exe").creation_flags(0x08000000)
        .args(["-NoProfile", "-NonInteractive", "-Command",
            "$ErrorActionPreference='Stop'; $key='HKCU:\\Software\\Microsoft\\Windows\\CurrentVersion\\Uninstall\\Sinomo'; if (Test-Path $key) { $stored=(Get-ItemProperty $key).InstallLocation; if ($stored -and ($stored.Replace('\\\\?\\','').TrimEnd('\\') -ieq $env:SINOMO_UNREGISTER_ROOT.Replace('\\\\?\\','').TrimEnd('\\'))) { Remove-Item -LiteralPath $key } }"])
        .env("SINOMO_UNREGISTER_ROOT", local::command_path(&fs::canonicalize(root)?)).output()?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).into_owned().into());
    }
    Ok(())
}

#[cfg(windows)]
fn schedule_running_file_removal(file: &Path, root: &Path, remove_empty_root: bool) -> Result<()> {
    use std::os::windows::process::CommandExt;
    Command::new("powershell.exe").creation_flags(0x08000000)
        .args(["-NoProfile", "-NonInteractive", "-Command",
            "$ErrorActionPreference='Stop'; Wait-Process -Id ([int]$env:SINOMO_REMOVE_PID) -ErrorAction SilentlyContinue; Remove-Item -LiteralPath $env:SINOMO_REMOVE_FILE -ErrorAction SilentlyContinue; if ($env:SINOMO_REMOVE_EMPTY_ROOT -eq '1' -and (Get-ChildItem -LiteralPath $env:SINOMO_REMOVE_ROOT -Force -ErrorAction SilentlyContinue | Measure-Object).Count -eq 0) { Remove-Item -LiteralPath $env:SINOMO_REMOVE_ROOT -ErrorAction SilentlyContinue }"])
        .env("SINOMO_REMOVE_PID", std::process::id().to_string())
        .env("SINOMO_REMOVE_FILE", file).env("SINOMO_REMOVE_ROOT", root)
        .env("SINOMO_REMOVE_EMPTY_ROOT", if remove_empty_root { "1" } else { "0" })
        .stdin(std::process::Stdio::null()).stdout(std::process::Stdio::null()).stderr(std::process::Stdio::null()).spawn()?;
    Ok(())
}

#[cfg(windows)]
pub fn windows_integration(root: &Path, action: &str, args: &[String]) -> Result<()> {
    use std::os::windows::process::CommandExt;
    if action == "uninstall" && !root.join("windows-integration.json").exists() {
        return Ok(());
    }
    let mut command = Command::new("powershell.exe");
    command
        .creation_flags(0x08000000)
        .args([
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
        ])
        .arg(local::command_path(&root.join("windows-integration.ps1")))
        .args(["-Action", action])
        .arg("-InstallDir")
        .arg(local::command_path(root));
    for (flag, switch) in [
        ("--path", "-AddPath"),
        ("--menu", "-Menu"),
        ("--desktop", "-Desktop"),
        ("--register", "-Register"),
    ] {
        if args.iter().any(|arg| arg == flag) {
            command.arg(switch);
        }
    }
    command.env("SINOMO_PRODUCT_VERSION", env!("CARGO_PKG_VERSION"));
    let output = command.output()?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).into_owned().into());
    }
    Ok(())
}

#[cfg(target_os = "linux")]
pub fn shell_quote(text: &str) -> String {
    format!("'{}'", text.replace('\'', "'\\''"))
}

#[cfg(target_os = "linux")]
fn desktop_quote(path: &Path) -> String {
    // Exec strings have their own escaping rules, independent of shell quoting.
    format!(
        "\"{}\"",
        path.to_string_lossy()
            .replace('\\', "\\\\\\\\")
            .replace('"', "\\\"")
            .replace('`', "\\`")
            .replace('$', "\\$")
            .replace('%', "%%")
    )
}

#[cfg(target_os = "linux")]
pub fn linux_integration(root: &Path, path: bool, menu: bool, desktop: bool) -> Result<()> {
    let mut receipt = read_receipt(root)?;
    let home = home()?;
    let applications = env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join(".local/share"))
        .join("applications");
    fs::create_dir_all(&applications)?;
    let app_entry = format!(
        "[Desktop Entry]\nType=Application\nName=Sinomo\nExec={}\nTerminal=false\nCategories=Utility;\n",
        desktop_quote(&receipt.data_dir.join("bin/sinomo-app"))
    );
    let remove_command = if receipt.data_dir.join("bin/sinomo-setup").is_file() {
        format!(
            "{} --uninstall",
            desktop_quote(&receipt.data_dir.join("bin/sinomo-setup"))
        )
    } else {
        format!(
            "{} uninstall --install-dir {}",
            desktop_quote(&receipt.data_dir.join("bin/sinomo")),
            desktop_quote(root)
        )
    };
    let mut entries = vec![(
        applications.join("net.sinomo.uninstall.desktop"),
        format!(
            "[Desktop Entry]\nType=Application\nName=Uninstall Sinomo\nExec={}\nTerminal=false\nCategories=Utility;\n",
            remove_command
        ),
    )];
    if menu {
        entries.push((
            applications.join("net.sinomo.app.desktop"),
            app_entry.clone(),
        ));
    }
    if desktop {
        let directory = Command::new("xdg-user-dir")
            .arg("DESKTOP")
            .output()
            .ok()
            .filter(|o| o.status.success())
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .map(|s| PathBuf::from(s.trim()))
            .filter(|p| p.is_absolute())
            .unwrap_or_else(|| home.join("Desktop"));
        fs::create_dir_all(&directory)?;
        entries.push((directory.join("net.sinomo.app.desktop"), app_entry));
    }
    for (file, content) in entries {
        use std::io::Write;
        let mut output = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&file)?;
        receipt.integrations.push(file.clone());
        write_receipt(root, &receipt)?;
        output.write_all(content.as_bytes())?;
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(file, fs::Permissions::from_mode(0o755))?;
    }
    if path {
        let profile = home.join(".profile");
        let mut content = fs::read_to_string(&profile).or_else(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                Ok(String::new())
            } else {
                Err(e)
            }
        })?;
        let block = profile_block(&receipt.data_dir.join("bin"));
        if !content.contains(&block) {
            content.push_str(&block);
            fs::write(&profile, content)?;
            receipt.integrations.push(profile);
            write_receipt(root, &receipt)?;
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn profile_block(root: &Path) -> String {
    format!(
        "\n# Sinomo installer: {}\nexport PATH=\"$PATH\":{}\n# End Sinomo installer\n",
        root.display(),
        shell_quote(root.to_str().unwrap())
    )
}

#[cfg(target_os = "linux")]
fn linux_remove_integrations(receipt: &Receipt) -> Result<()> {
    for file in &receipt.integrations {
        if file == &home()?.join(".profile") {
            let content = fs::read_to_string(file)?;
            fs::write(
                file,
                content
                    .replace(&profile_block(&receipt.data_dir.join("bin")), "")
                    .replace(&profile_block(&receipt.install_dir), ""),
            )?;
        } else if file.exists() {
            let content = fs::read_to_string(file)?;
            // Keep a shortcut if the user replaced it with a different target.
            if content.contains(&desktop_quote(&receipt.data_dir.join("bin/sinomo-app")))
                || content.contains(&desktop_quote(&receipt.data_dir.join("bin/sinomo")))
                || content.contains(&desktop_quote(&receipt.data_dir.join("bin/sinomo-setup")))
                || content.contains(&desktop_quote(&receipt.install_dir.join("sinomo-app")))
                || content.contains(&desktop_quote(&receipt.install_dir.join("sinomo-setup")))
                || content.contains(&desktop_quote(&receipt.install_dir.join("sinomo")))
            {
                fs::remove_file(file)?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "subprocess fixture, invoked by self-removal regression"]
    fn installed_uninstall_worker_fixture() {
        let root = PathBuf::from(env::var_os("SINOMO_UNINSTALL_SELF_FIXTURE").unwrap());
        uninstall(&root, false).unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn installed_uninstaller_does_not_kill_its_own_worker_threads() {
        let fixture = sinomo_testdir::TestDir::new("installed-uninstaller-own-threads");
        let root = fixture.join("setup");
        let data = fixture.join("data");
        fs::create_dir_all(&root).unwrap();
        fs::create_dir_all(&data).unwrap();
        configure_with_files(
            &root,
            &data,
            None,
            vec![RECEIPT.into(), local::INSTALLED_DATA_DIR_FILE.into()],
        )
        .unwrap();
        let executable = data.join("bin/sinomo");
        fs::copy(env::current_exe().unwrap(), &executable).unwrap();
        // The real test harness runs the removal on a worker thread. Its
        // executable name/data binding match the installed CLI, so an ownership
        // scan must distinguish that caller's threads from owned child processes.
        let output = Command::new(&executable)
            .args([
                "--exact",
                "initial_setup::tests::installed_uninstall_worker_fixture",
                "--ignored",
                "--nocapture",
            ])
            .env("SINOMO_DATA_DIR", &data)
            .env("SINOMO_UNINSTALL_SELF_FIXTURE", &root)
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        assert!(
            executable.is_file(),
            "keep-data uninstall preserves the runtime"
        );
        assert!(local::ensure_deployment_enabled(&data).is_err());
    }

    #[test]
    #[ignore = "subprocess fixture, invoked by prioritized removal acceptance"]
    fn removal_fixture_process() {
        let data = PathBuf::from(env::var_os("SINOMO_REMOVAL_TEST_DATA").unwrap());
        if env::var_os("SINOMO_REMOVAL_FOREIGN").is_some() {
            fs::write(data.join("ready"), b"ready").unwrap();
            loop {
                std::thread::park_timeout(Duration::from_secs(60));
            }
        }
        let _work = sinomo_rollout::acquire_runtime_work(&data).unwrap();
        let paths = sinomo_rollout::SelfRolloutPaths::new(&data);
        let _activation = sinomo_rollout::acquire_rollout_activation_guard(&paths).unwrap();
        fs::write(
            sinomo_rollout::rollout_main_activation_path(&paths),
            b"blocked activation",
        )
        .unwrap();
        fs::write(data.join("ready"), b"ready").unwrap();
        loop {
            std::thread::park_timeout(Duration::from_secs(60));
        }
    }

    #[test]
    fn uninstall_outranks_a_live_rollout_and_retains_the_entire_data_directory() {
        let fixture = sinomo_testdir::TestDir::new("uninstall-busy-rollout");
        let root = fixture.join("setup");
        let data = fixture.join("data");
        fs::create_dir_all(&root).unwrap();
        configure_with_files(
            &root,
            &data,
            None,
            vec![RECEIPT.into(), local::INSTALLED_DATA_DIR_FILE.into()],
        )
        .unwrap();
        let executable = data.join("bin").join(binary("sinomo"));
        fs::copy(env::current_exe().unwrap(), &executable).unwrap();
        fs::create_dir_all(data.join("module/cache/native-dylib/evolved")).unwrap();
        fs::write(
            data.join("module/cache/native-dylib/evolved/module"),
            b"evolved native bytes",
        )
        .unwrap();
        let mut command = Command::new(&executable);
        #[cfg(windows)]
        sinomo_daemon::prepare_noninteractive_command(&mut command);
        let mut child = command
            .args([
                "--exact",
                "initial_setup::tests::removal_fixture_process",
                "--ignored",
                "--nocapture",
            ])
            .env("SINOMO_REMOVAL_TEST_DATA", &data)
            .env("SINOMO_DATA_DIR", &data)
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(20);
        while !data.join("ready").exists() {
            assert!(
                child.try_wait().unwrap().is_none(),
                "removal fixture exited early"
            );
            assert!(
                Instant::now() < deadline,
                "removal fixture did not acquire admission"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        let foreign = fixture.join("other-deployment");
        fs::create_dir_all(&foreign).unwrap();
        let mut foreign_command = Command::new(&executable);
        #[cfg(windows)]
        sinomo_daemon::prepare_noninteractive_command(&mut foreign_command);
        let mut foreign_child = foreign_command
            .args([
                "--exact",
                "initial_setup::tests::removal_fixture_process",
                "--ignored",
                "--nocapture",
            ])
            .env("SINOMO_REMOVAL_TEST_DATA", &foreign)
            .env("SINOMO_DATA_DIR", &foreign)
            .env("SINOMO_REMOVAL_FOREIGN", "1")
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(20);
        while !foreign.join("ready").exists() {
            assert!(foreign_child.try_wait().unwrap().is_none());
            assert!(Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(20));
        }
        let started = Instant::now();
        uninstall(&root, false).unwrap();
        child.wait().unwrap();
        let preserved_other_deployment = foreign_child.try_wait().unwrap().is_none();
        let _ = foreign_child.kill();
        let _ = foreign_child.wait();
        assert!(
            preserved_other_deployment,
            "uninstall killed a process selecting another data directory"
        );
        assert!(started.elapsed() < Duration::from_secs(20));
        assert!(executable.is_file());
        assert_eq!(
            fs::read(data.join("module/cache/native-dylib/evolved/module")).unwrap(),
            b"evolved native bytes"
        );
        assert!(data.join("rollout/pending-main-rollout.sobj").is_file());
        assert!(local::ensure_deployment_enabled(&data).is_err());
        assert!(sinomo_rollout::acquire_runtime_work(&data).is_err());
        assert!(
            sinomo_rollout::acquire_rollout_activation_guard(
                &sinomo_rollout::SelfRolloutPaths::new(&data)
            )
            .is_err()
        );
    }

    #[test]
    fn headless_staging_binds_directories_before_service_completion() {
        let fixture = sinomo_testdir::TestDir::new("initial-cli-headless");
        let root = fixture.join("application");
        let data = fixture.join("data");
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join(binary("sinomo")), b"CLI fixture").unwrap();
        configure_with_files(
            &root,
            &data,
            None,
            vec![
                binary("sinomo"),
                local::INSTALLED_DATA_DIR_FILE.into(),
                RECEIPT.into(),
            ],
        )
        .unwrap();
        assert_eq!(
            local::installed_data_dir(&root).unwrap(),
            Some(fs::canonicalize(&data).unwrap())
        );
        for directory in ["daemon", "shadow-daemon", "boot", "rollout/source"] {
            assert!(!data.join(directory).exists());
        }
        fs::write(root.join("personal-file"), b"keep").unwrap();
        uninstall(&root, false).unwrap();
        assert!(!root.join(binary("sinomo")).exists());
        assert!(root.join("personal-file").exists());
        assert!(data.exists());
    }

    #[test]
    fn uninstall_removes_registration_after_data_was_already_deleted() {
        for delete_data in [false, true] {
            let fixture = sinomo_testdir::TestDir::new("uninstall-missing-data");
            let root = fixture.join("setup");
            let data = fixture.join("data");
            fs::create_dir_all(&root).unwrap();
            configure_with_files(
                &root,
                &data,
                None,
                vec![RECEIPT.into(), local::INSTALLED_DATA_DIR_FILE.into()],
            )
            .unwrap();
            fs::remove_dir_all(&data).unwrap();
            uninstall(&root, delete_data).unwrap();
            assert!(!root.exists());
            assert!(!data.exists());
        }
    }

    #[test]
    fn initial_install_refuses_existing_destination_before_replacing_files() {
        let fixture = sinomo_testdir::TestDir::new("initial-refuse-overwrite");
        let root = fixture.join("application");
        let data = fixture.join("data");
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("user-file"), b"keep").unwrap();
        let source = fixture.join("source-cli");
        fs::write(&source, b"CLI fixture").unwrap();
        assert!(
            install(
                &source,
                None,
                &root,
                &data,
                &IntegrationOptions::default(),
                &services::Options::default()
            )
            .is_err()
        );
        assert_eq!(fs::read(root.join("user-file")).unwrap(), b"keep");
        assert!(!root.join(RECEIPT).exists());
    }

    #[test]
    fn setup_retains_evolved_data_and_uninstall_preserves_user_files() {
        let temp = sinomo_testdir::TestDir::new("setup-retain-data");
        let root = temp.join("install");
        let data = temp.join("data");
        fs::create_dir_all(&root).unwrap();
        fs::create_dir_all(&data).unwrap();
        fs::write(data.join("personal-file"), b"keep").unwrap();
        for name in desktop::DESKTOP_BINARY_STEMS.map(binary) {
            fs::write(root.join(name), b"fixture").unwrap();
        }
        desktop::write_bundle_manifest(
            &root,
            "fixture".into(),
            &desktop::DESKTOP_BINARY_STEMS.map(binary),
        )
        .unwrap();
        configure_from(&root, &data, &root).unwrap();
        assert!(read_receipt(&root).unwrap().owns_data);
        assert!(configure(&root, &data).is_err());
        fs::write(root.join("user-file"), b"keep").unwrap();
        uninstall(&root, false).unwrap();
        assert!(root.join("user-file").exists());
        assert!(data.join("personal-file").exists());
    }

    #[test]
    fn deletion_rejects_overlapping_locations_and_changed_ownership() {
        let temp = sinomo_testdir::TestDir::new("setup-data-owner");
        let root = temp.join("install");
        let data = temp.join("data");
        fs::create_dir_all(&root).unwrap();
        fs::create_dir_all(&data).unwrap();
        assert!(validate_locations(&root, &root).is_err());
        assert!(validate_locations(&root, &home().unwrap()).is_err());
        for name in desktop::DESKTOP_BINARY_STEMS.map(binary) {
            fs::write(root.join(name), b"fixture").unwrap();
        }
        desktop::write_bundle_manifest(
            &root,
            "fixture".into(),
            &desktop::DESKTOP_BINARY_STEMS.map(binary),
        )
        .unwrap();
        configure_from(&root, &data, &root).unwrap();
        assert!(read_receipt(&root).unwrap().owns_data);
        fs::write(data.join(DATA_OWNER), "different install").unwrap();
        assert!(uninstall(&root, true).is_err());
        assert!(root.join(RECEIPT).exists());
    }

    #[test]
    fn explicit_uninstall_removes_owned_data_and_reused_release_is_not_offered() {
        let temp = sinomo_testdir::TestDir::new("setup-preserve-release");
        let root = temp.join("install");
        let second = temp.join("second");
        let data = temp.join("data");
        for directory in [&root, &second, &data] {
            fs::create_dir_all(directory).unwrap();
        }
        for (directory, revision) in [(&root, "evolved"), (&second, "upstream")] {
            for name in desktop::DESKTOP_BINARY_STEMS.map(binary) {
                fs::write(directory.join(name), revision.as_bytes()).unwrap();
            }
            desktop::write_bundle_manifest(
                directory,
                revision.into(),
                &desktop::DESKTOP_BINARY_STEMS.map(binary),
            )
            .unwrap();
        }
        configure_from(&root, &data, &root).unwrap();
        let before = desktop::DesktopReleases::new(&data).status().unwrap();
        assert!(configure_from(&second, &data, &second).is_err());
        uninstall(&root, false).unwrap();
        configure_from(&second, &data, &second).unwrap();
        let after = desktop::DesktopReleases::new(&data).status().unwrap();
        assert_eq!(before.active_release, after.active_release);
        assert_eq!(before.candidate_release, after.candidate_release);
        uninstall(&second, true).unwrap();
        assert!(!data.exists());
    }
}
