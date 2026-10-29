//! Shared initial-installation, integration and removal contract for CLI and graphical setup.
pub mod services;
use fs2::FileExt;
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
    if fs::read_dir(root)?.next().is_some() {
        return Err("Choose an empty installation directory. Use rollout to evolve an existing installation.".into());
    }
    let mut files = if let Some(source) = desktop_source {
        let files = payload_files();
        for name in &files {
            fs::copy(source.join(name), root.join(name))?;
        }
        if source.join(binary("sinomo-setup")).is_file() {
            fs::copy(
                source.join(binary("sinomo-setup")),
                root.join(binary("sinomo-setup")),
            )?;
        }
        files
    } else {
        fs::copy(executable, root.join(binary("sinomo")))?;
        vec![binary("sinomo")]
    };
    files.extend([
        binary("sinomo-setup"),
        local::INSTALLED_DATA_DIR_FILE.into(),
        RECEIPT.into(),
        "windows-integration.ps1".into(),
        "uninstall.ps1".into(),
    ]);
    configure_with_files(root, data, desktop_source, files)?;
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
        .chain(["desktop-release.json".to_owned()])
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
    configure_from(root, data, root)
}

pub fn configure_from(root: &Path, data: &Path, source: &Path) -> Result<()> {
    let files = installed_files()
        .into_iter()
        .chain([
            binary("sinomo-setup"),
            local::INSTALLED_DATA_DIR_FILE.into(),
            RECEIPT.into(),
            "windows-integration.ps1".into(),
        ])
        .collect();
    configure_with_files(root, data, Some(source), files)
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
        Ok(owner) => owner == root.to_string_lossy(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
        Err(error) => return Err(error.into()),
    };
    let receipt = Receipt {
        schema: 1,
        install_dir: root.clone(),
        data_dir: data.clone(),
        owns_data,
        files,
        integrations: Vec::new(),
        services: None,
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
    // Reuse an evolved data directory without even offering the installer as a candidate.
    let releases = desktop::DesktopReleases::new(&data);
    if let Some(source) = source {
        if releases.status()?.active_release.is_none() {
            let layout =
                crate::InstallLayout::new(&local::SinomoDataDir::new(&data), data.join("boot"));
            crate::install_desktop_bundle(&layout, source)?;
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
    fs::write(root.join(RECEIPT), serde_json::to_vec_pretty(receipt)?)?;
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

fn ensure_stopped(
    data: &Path,
    services: Option<&services::Registration>,
) -> Result<Option<fs::File>> {
    use sinomo_daemon::{DaemonPaths, DaemonProbeStatus, probe_daemon_status};
    let launcher = data.join("rollout/desktop/launcher.lock");
    let lease = if data.is_dir() {
        fs::create_dir_all(launcher.parent().ok_or("no launcher lock directory")?)?;
        let file = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(launcher)?;
        file.try_lock_exclusive()
            .map_err(|_| "Quit the Sinomo app before uninstalling.")?;
        Some(file)
    } else {
        None
    };
    if let Some(services) = services {
        services::stop_manager(services)?;
    }
    // Stop recovery first so it cannot restart main during removal.
    for name in ["shadow-daemon", "daemon"] {
        let paths = DaemonPaths::with_root(data.join(name));
        match probe_daemon_status(&paths, Duration::from_secs(1)) {
            DaemonProbeStatus::NotRunning => continue,
            DaemonProbeStatus::Running(status) => {
                sinomo_daemon::send_daemon_shutdown_request(
                    &status.address,
                    Duration::from_secs(5),
                )?;
            }
            _ => {
                return Err(
                    "The runtime is starting or stopping; retry uninstall when it has stopped."
                        .into(),
                );
            }
        }
        let deadline = Instant::now() + Duration::from_secs(15);
        while !matches!(
            probe_daemon_status(&paths, Duration::from_millis(500)),
            DaemonProbeStatus::NotRunning
        ) {
            if Instant::now() >= deadline {
                return Err("Runtime shutdown timed out; no files were removed.".into());
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }
    Ok(lease)
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
    let receipt = read_receipt(root)?;
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
    let _launcher_lease = ensure_stopped(&receipt.data_dir, receipt.services.as_ref())?;
    if let Some(services) = &receipt.services {
        services::remove(services)?;
    }
    #[cfg(windows)]
    windows_integration(root, "uninstall", &[])?;
    #[cfg(target_os = "linux")]
    linux_remove_integrations(&receipt)?;
    if delete_data {
        fs::remove_dir_all(&receipt.data_dir)?;
    }
    // Never recursively remove an installation folder: users may have added their own files.
    for file in &receipt.files {
        #[cfg(windows)]
        if env::current_exe()
            .ok()
            .and_then(|exe| fs::canonicalize(exe).ok())
            == fs::canonicalize(root.join(file)).ok()
        {
            if !_nsis_managed {
                schedule_running_file_removal(&root.join(file), root)?;
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
    let _ = fs::remove_dir(root);
    Ok(())
}

#[cfg(windows)]
fn unregister_windows(root: &Path) -> Result<()> {
    use std::os::windows::process::CommandExt;
    let output = Command::new("powershell.exe").creation_flags(0x08000000)
        .args(["-NoProfile", "-NonInteractive", "-Command",
            "$ErrorActionPreference='Stop'; $key='HKCU:\\Software\\Microsoft\\Windows\\CurrentVersion\\Uninstall\\Sinomo'; if (Test-Path $key) { $stored=(Get-ItemProperty $key).InstallLocation; if ($stored -and ($stored.Replace('\\\\?\\','').TrimEnd('\\') -ieq $env:SINOMO_UNREGISTER_ROOT.Replace('\\\\?\\','').TrimEnd('\\'))) { Remove-Item -LiteralPath $key } }"])
        .env("SINOMO_UNREGISTER_ROOT", root).output()?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).into_owned().into());
    }
    Ok(())
}

#[cfg(windows)]
fn schedule_running_file_removal(file: &Path, root: &Path) -> Result<()> {
    use std::os::windows::process::CommandExt;
    Command::new("powershell.exe").creation_flags(0x08000000)
        .args(["-NoProfile", "-NonInteractive", "-Command",
            "$ErrorActionPreference='Stop'; Wait-Process -Id ([int]$env:SINOMO_REMOVE_PID) -ErrorAction SilentlyContinue; Remove-Item -LiteralPath $env:SINOMO_REMOVE_FILE -ErrorAction SilentlyContinue; if ((Get-ChildItem -LiteralPath $env:SINOMO_REMOVE_ROOT -Force -ErrorAction SilentlyContinue | Measure-Object).Count -eq 0) { Remove-Item -LiteralPath $env:SINOMO_REMOVE_ROOT -ErrorAction SilentlyContinue }"])
        .env("SINOMO_REMOVE_PID", std::process::id().to_string())
        .env("SINOMO_REMOVE_FILE", file).env("SINOMO_REMOVE_ROOT", root)
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
        .arg(root.join("windows-integration.ps1"))
        .args(["-Action", action])
        .arg("-InstallDir")
        .arg(root);
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
        desktop_quote(&root.join("sinomo-app"))
    );
    let remove_command = if root.join("sinomo-setup").is_file() {
        format!("{} --uninstall", desktop_quote(&root.join("sinomo-setup")))
    } else {
        format!(
            "{} uninstall --install-dir {}",
            desktop_quote(&root.join("sinomo")),
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
        let block = profile_block(root);
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
                content.replace(&profile_block(&receipt.install_dir), ""),
            )?;
        } else if file.exists() {
            let content = fs::read_to_string(file)?;
            // Keep a shortcut if the user replaced it with a different target.
            if content.contains(&desktop_quote(&receipt.install_dir.join("sinomo-app")))
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
        configure(&root, &data).unwrap();
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
        configure(&root, &data).unwrap();
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
        configure(&root, &data).unwrap();
        let before = desktop::DesktopReleases::new(&data).status().unwrap();
        configure(&second, &data).unwrap();
        let after = desktop::DesktopReleases::new(&data).status().unwrap();
        assert_eq!(before.active_release, after.active_release);
        assert_eq!(before.candidate_release, after.candidate_release);
        uninstall(&root, true).unwrap();
        assert!(!data.exists());
    }
}
