//! One per-user daemon registration contract for both installation frontends.
use super::{Receipt, Result, binary, read_receipt, write_receipt};
use crate::{InstallLayout, InstalledDaemonStartStatus};
use serde::{Deserialize, Serialize};
use sinomo_core::local::SinomoDataDir;
use sinomo_daemon::{DaemonProbeStatus, probe_daemon_status};
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

pub const DEFAULT_HOST: &str = "0.0.0.0";

#[derive(Clone, Debug)]
pub struct Options {
    pub start: bool,
    pub host: String,
    pub main_port: u16,
    pub shadow_port: u16,
    pub main_port_fallback: bool,
    pub shadow_port_fallback: bool,
    pub preserve_existing: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            start: true,
            host: DEFAULT_HOST.into(),
            main_port: sinomo_daemon::SINOMO_DAEMON_DEFAULT_PORT,
            shadow_port: sinomo_daemon::SINOMO_SHADOW_DAEMON_DEFAULT_PORT,
            main_port_fallback: true,
            shadow_port_fallback: true,
            preserve_existing: true,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Registration {
    pub main_entry: PathBuf,
    pub shadow_entry: PathBuf,
    pub main_content: String,
    pub shadow_content: String,
    #[serde(default)]
    pub enabled_links: Vec<(PathBuf, PathBuf)>,
}

fn layout(receipt: &Receipt, boot: PathBuf) -> InstallLayout {
    let mut layout = InstallLayout::new(&SinomoDataDir::new(&receipt.data_dir), boot);
    let id = blake3::hash(receipt.data_dir.to_string_lossy().as_bytes()).to_hex();
    let id = &id.as_str()[..12];
    if cfg!(windows) {
        layout.main_boot_entry = layout.boot_dir.join(format!("Sinomo Daemon {id}.vbs"));
        layout.shadow_boot_entry = layout
            .boot_dir
            .join(format!("Sinomo Shadow Daemon {id}.vbs"));
    } else {
        layout.main_boot_entry = layout.boot_dir.join(format!("sinomo-daemon-{id}.service"));
        layout.shadow_boot_entry = layout
            .boot_dir
            .join(format!("sinomo-shadow-daemon-{id}.service"));
    }
    layout
}

fn registration(layout: &InstallLayout, options: &Options) -> Registration {
    let main = if cfg!(windows) {
        ["daemon", "start"]
    } else {
        ["daemon", "run"]
    };
    let shadow = if cfg!(windows) {
        ["daemon", "shadow", "start"]
    } else {
        ["daemon", "shadow", "run"]
    };
    let mut shadow_env = vec![
        (
            sinomo_rollout::SHADOW_MAIN_BINARY_ENV,
            layout.binary.display().to_string(),
        ),
        (sinomo_rollout::SHADOW_MAIN_HOST_ENV, options.host.clone()),
    ];
    if !options.main_port_fallback {
        shadow_env.push((
            sinomo_rollout::SHADOW_MAIN_PORT_ENV,
            options.main_port.to_string(),
        ));
    }
    #[allow(unused_mut)]
    let mut record = Registration {
        main_entry: layout.main_boot_entry.clone(),
        shadow_entry: layout.shadow_boot_entry.clone(),
        main_content: crate::boot_entry_content(
            &layout.data_dir,
            &layout.binary,
            &main,
            &options.host,
            options.main_port,
            options.main_port_fallback,
            "Sinomo daemon",
            crate::BootEntryChildProcessPolicy::SystemdDefault,
            &[],
        ),
        shadow_content: crate::boot_entry_content(
            &layout.data_dir,
            &layout.shadow_binary,
            &shadow,
            &options.host,
            options.shadow_port,
            options.shadow_port_fallback,
            "Sinomo shadow daemon",
            crate::BootEntryChildProcessPolicy::PreserveDetachedChildren,
            &shadow_env,
        ),
        enabled_links: Vec::new(),
    };
    #[cfg(unix)]
    for entry in [&record.main_entry, &record.shadow_entry] {
        record.enabled_links.push((
            layout
                .boot_dir
                .join("default.target.wants")
                .join(entry.file_name().expect("service filename")),
            entry.clone(),
        ));
    }
    record
}

fn legacy_owned(path: &Path, data: &Path) -> Result<Option<String>> {
    let content = match fs::read_to_string(path) {
        Ok(content) => content,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    #[cfg(windows)]
    let owned = content.lines().any(|line| {
        line.strip_prefix("env(\"SINOMO_DATA_DIR\") = \"")
            .and_then(|value| value.strip_suffix('"'))
            .and_then(|value| fs::canonicalize(value.replace("\"\"", "\"")).ok())
            .zip(fs::canonicalize(data).ok())
            .is_some_and(|(actual, expected)| actual == expected)
    });
    #[cfg(not(windows))]
    let owned = content.lines().any(|line| {
        linux_environment_data(line).and_then(|value| fs::canonicalize(value).ok())
            .zip(fs::canonicalize(data).ok()).is_some_and(|(actual, expected)| actual == expected)
    });
    Ok(owned.then_some(content))
}

#[cfg(any(not(windows), test))]
fn linux_environment_data(line: &str) -> Option<PathBuf> {
    if let Some(value) = line.strip_prefix("Environment=SINOMO_DATA_DIR=") {
        return Some(PathBuf::from(value));
    }
    let value = line.strip_prefix("Environment=\"SINOMO_DATA_DIR=")?.strip_suffix('"')?;
    let mut decoded = String::new();
    let mut chars = value.chars();
    while let Some(character) = chars.next() {
        decoded.push(if character == '\\' { match chars.next()? {
            '\\' => '\\', '"' => '"', 'n' => '\n', 'r' => '\r', 's' => ' ', _ => return None,
        }} else { character });
    }
    Some(PathBuf::from(decoded.replace("%%", "%")))
}

fn old_bind(content: &str) -> Result<(String, Option<u16>)> {
    let command = content
        .lines()
        .find(|line| line.contains(" --host "))
        .ok_or("owned service has no host option")?;
    let host = command
        .split_once(" --host ")
        .expect("checked host marker")
        .1
        .trim_start()
        .trim_start_matches('"')
        .split(|c: char| c.is_whitespace() || c == '"' || c == ',')
        .next()
        .ok_or("invalid service host")?;
    let host = if cfg!(windows) {
        host.to_owned()
    } else {
        host.replace("%%", "%").replace("$$", "$")
    };
    if host.is_empty() {
        return Err("owned service has an empty bind host".into());
    }
    let port = command
        .split_once(" --port ")
        .map(|(_, tail)| {
            tail.chars()
                .take_while(char::is_ascii_digit)
                .collect::<String>()
                .parse::<u16>()
        })
        .transpose()?;
    Ok((host, port))
}

fn adopt_legacy(
    receipt: &Receipt,
    boot: &Path,
    layout: &mut InstallLayout,
    options: &Options,
) -> Result<(Options, Vec<(PathBuf, String)>)> {
    let legacy = InstallLayout::new(&SinomoDataDir::new(&receipt.data_dir), boot.to_owned());
    let mut effective = options.clone();
    let mut previous = Vec::new();
    let mut existing_host = None;
    for (shadow, entry) in [
        (false, &legacy.main_boot_entry),
        (true, &legacy.shadow_boot_entry),
    ] {
        if let Some(content) = legacy_owned(entry, &receipt.data_dir)? {
            if options.preserve_existing {
                let (host, port) = old_bind(&content)?;
                if existing_host
                    .as_ref()
                    .is_some_and(|previous| previous != &host)
                {
                    return Err("existing main/shadow startup settings use different hosts; inspect them before installation".into());
                }
                effective.host = host.clone();
                existing_host = Some(host);
                if shadow {
                    effective.shadow_port = port.unwrap_or(Options::default().shadow_port);
                    effective.shadow_port_fallback = port.is_none();
                } else {
                    effective.main_port = port.unwrap_or(Options::default().main_port);
                    effective.main_port_fallback = port.is_none();
                }
            }
            if shadow {
                layout.shadow_boot_entry = entry.clone();
            } else {
                layout.main_boot_entry = entry.clone();
            }
            previous.push((entry.clone(), content));
        }
    }
    Ok((effective, previous))
}

fn verify_entry(path: &Path, expected: &str) -> Result<()> {
    match fs::read_to_string(path) {
        Ok(content) if content == expected => Ok(()),
        Ok(_) => Err(format!(
            "service registration changed or belongs to another deployment: {}",
            path.display()
        )
        .into()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn check(record: &Registration) -> Result<()> {
    verify_entry(&record.main_entry, &record.main_content)?;
    verify_entry(&record.shadow_entry, &record.shadow_content)?;
    for (link, target) in &record.enabled_links {
        match fs::read_link(link) {
            Ok(actual) if &actual == target => {}
            Ok(actual)
                if fs::canonicalize(if actual.is_absolute() {
                    actual.clone()
                } else {
                    link.parent().ok_or("enablement parent")?.join(&actual)
                })
                .ok()
                .zip(fs::canonicalize(target).ok())
                .is_some_and(|(actual, expected)| actual == expected) => {}
            Ok(_) => {
                return Err(format!(
                    "service enablement belongs to another deployment: {}",
                    link.display()
                )
                .into());
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

pub(super) fn install(root: &Path, options: &Options) -> Result<()> {
    let mut receipt = read_receipt(root)?;
    if receipt.services.is_some() {
        return Err("daemon services are already registered for this installation".into());
    }
    let boot = crate::default_boot_dir(&SinomoDataDir::new(&receipt.data_dir));
    let mut layout = layout(&receipt, boot.clone());
    let (effective, previous) = adopt_legacy(&receipt, &boot, &mut layout, options)?;
    let options = &effective;
    let record = registration(&layout, options);
    let mut before = record.clone();
    for (entry, content) in &previous {
        if entry == &before.main_entry {
            before.main_content = content.clone();
        }
        if entry == &before.shadow_entry {
            before.shadow_content = content.clone();
        }
    }
    check(&before)?;
    fs::create_dir_all(&layout.bin_dir)?;
    for destination in [&layout.binary, &layout.shadow_binary] {
        if !destination.exists() {
            let source = if layout.binary.is_file() {
                layout.binary.clone()
            } else {
                root.join(binary("sinomo"))
            };
            sinomo_rollout::install_binary_atomically(&source, destination)?;
        }
    }
    receipt.services = Some(record.clone());
    write_receipt(root, &receipt)?;
    fs::create_dir_all(&layout.boot_dir)?;
    fs::write(&record.main_entry, &record.main_content)?;
    fs::write(&record.shadow_entry, &record.shadow_content)?;
    #[cfg(unix)]
    for (link, target) in &record.enabled_links {
        fs::create_dir_all(link.parent().expect("enablement directory"))?;
        if fs::symlink_metadata(link).is_err() {
            std::os::unix::fs::symlink(target, link)?;
        }
    }
    let managed = manager_available();
    if managed {
        systemctl(&["daemon-reload"])?;
    }
    if options.start {
        let specs = crate::installed_daemon_start_specs(
            &layout,
            &options.host,
            options.main_port,
            options.shadow_port,
            options.main_port_fallback,
            options.shadow_port_fallback,
        );
        for (spec, entry) in specs.iter().zip([&record.main_entry, &record.shadow_entry]) {
            match probe_daemon_status(&spec.paths, Duration::from_secs(1)) {
                DaemonProbeStatus::Running(status) => {
                    let port = if spec.label == "daemon" {
                        (options.main_port, options.main_port_fallback)
                    } else {
                        (options.shadow_port, options.shadow_port_fallback)
                    };
                    if !options.preserve_existing
                        && (status.host != options.host || (!port.1 && status.port != port.0))
                    {
                        return Err(format!("{} is running with different bind settings; stop it before changing them", spec.label).into());
                    }
                    continue;
                }
                DaemonProbeStatus::Locked { .. } => {
                    return Err(format!(
                        "{} is starting or stopping; retry after it settles",
                        spec.label
                    )
                    .into());
                }
                DaemonProbeStatus::NotRunning => {}
            }
            if managed {
                systemctl(&[
                    "start",
                    entry
                        .file_name()
                        .and_then(|n| n.to_str())
                        .ok_or("invalid unit filename")?,
                ])?;
                let deadline = std::time::Instant::now() + Duration::from_secs(60);
                while !matches!(
                    probe_daemon_status(&spec.paths, Duration::from_millis(500)),
                    DaemonProbeStatus::Running(_)
                ) {
                    if std::time::Instant::now() >= deadline {
                        return Err(format!("{} did not become ready", spec.label).into());
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
            } else {
                match crate::start_installed_sinomo_daemon(
                    &spec.binary,
                    &layout.data_dir,
                    &spec.args,
                    &spec.paths,
                    &spec.extra_env,
                    Duration::from_secs(60),
                    Duration::from_secs(1),
                )? {
                    InstalledDaemonStartStatus::Started(_) => {
                        if !matches!(
                            probe_daemon_status(&spec.paths, Duration::from_secs(1)),
                            DaemonProbeStatus::Running(_)
                        ) {
                            return Err(format!("{} did not become ready for this data directory; the endpoint may belong to another deployment", spec.label).into());
                        }
                    }
                    status => {
                        return Err(format!("{} failed to start: {status:?}", spec.label).into());
                    }
                }
            }
        }
    }
    if cfg!(target_os = "linux") && !managed {
        println!(
            "User service units registered and enabled; no running systemd user manager. {}",
            if options.start {
                "Daemon readiness verified directly for this session."
            } else {
                "Startup skipped (--no-start)."
            }
        );
    }
    Ok(())
}

fn manager_available() -> bool {
    cfg!(target_os = "linux")
        && Command::new("systemctl")
            .args(["--user", "show-environment"])
            .output()
            .is_ok_and(|output| output.status.success())
}

fn systemctl(args: &[&str]) -> Result<()> {
    let output = Command::new("systemctl")
        .arg("--user")
        .args(args)
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "systemctl --user {}: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(())
}

pub(super) fn stop_manager(record: &Registration) -> Result<()> {
    check(record)?;
    if manager_available() {
        for entry in [&record.shadow_entry, &record.main_entry] {
            systemctl(&[
                "stop",
                entry
                    .file_name()
                    .and_then(|n| n.to_str())
                    .ok_or("invalid unit filename")?,
            ])?;
        }
    }
    Ok(())
}

pub(super) fn remove(record: &Registration) -> Result<()> {
    check(record)?;
    for (link, _) in &record.enabled_links {
        match fs::remove_file(link) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    for entry in [&record.shadow_entry, &record.main_entry] {
        match fs::remove_file(entry) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    if manager_available() {
        systemctl(&["daemon-reload"])?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn test_receipt(root: &Path) -> Receipt {
        Receipt {
            schema: 1,
            install_dir: root.join("app"),
            data_dir: root.join("data with spaces"),
            owns_data: true,
            files: Vec::new(),
            integrations: Vec::new(),
            services: None,
        }
    }
    #[test]
    fn defaults_start_and_names_separate_data_directories() {
        assert!(Options::default().start);
        let fixture = sinomo_testdir::TestDir::new("setup-service-plan");
        let receipt = test_receipt(&fixture);
        let layout = layout(&receipt, fixture.join("startup"));
        let record = registration(&layout, &Options::default());
        fs::create_dir_all(&layout.boot_dir).unwrap();
        fs::write(&record.main_entry, &record.main_content).unwrap();
        fs::write(&record.shadow_entry, &record.shadow_content).unwrap();
        assert!(check(&record).is_ok());
        fs::write(&record.main_entry, "another deployment").unwrap();
        assert!(remove(&record).is_err());
        assert!(record.shadow_entry.exists());
        assert!(record.main_content.contains("SINOMO_DATA_DIR"));
        assert!(record.shadow_content.contains("SINOMO_SHADOW_MAIN_BINARY"));
        #[cfg(windows)]
        assert!(!record.main_content.contains(r#"shell.Run """\\?\"#));
        let other = Receipt {
            data_dir: fixture.join("other-data"),
            ..receipt
        };
        assert_ne!(
            layout.main_boot_entry,
            super::layout(&other, layout.boot_dir).main_boot_entry
        );
    }
    #[test]
    fn systemd_paths_preserve_spaces_percent_dollars_and_quotes() {
        let path = r#"/data with spaces/%u/$HOME/quoted"name"#;
        assert_eq!(
            crate::systemd_value(path, false),
            r#""/data with spaces/%%u/$HOME/quoted\"name""#
        );
        assert_eq!(
            crate::systemd_value(path, true),
            r#""/data with spaces/%%u/$$HOME/quoted\"name""#
        );
        let environment = format!("Environment={}", crate::systemd_value(&format!("SINOMO_DATA_DIR={path}"), false));
        assert_eq!(linux_environment_data(&environment), Some(PathBuf::from(path)));
    }
    #[test]
    fn resident_daemons_drop_only_appimage_library_paths() {
        let image = PathBuf::from("image");
        let retained = PathBuf::from("user-libraries");
        let value = std::env::join_paths([image.join("usr/lib"), retained.clone()]).unwrap();
        let filtered = crate::standalone_library_path(&value, &image).unwrap();
        assert_eq!(
            std::env::split_paths(&filtered).collect::<Vec<_>>(),
            vec![retained]
        );
        assert!(
            crate::standalone_library_path(
                &std::env::join_paths([image.join("lib")]).unwrap(),
                &image
            )
            .is_none()
        );
    }
    #[test]
    fn old_cli_startup_is_adopted_without_changing_bind_settings() {
        let fixture = sinomo_testdir::TestDir::new("setup-adopt-service");
        let receipt = test_receipt(&fixture);
        fs::create_dir_all(&receipt.data_dir).unwrap();
        let legacy = InstallLayout::new(
            &SinomoDataDir::new(&receipt.data_dir),
            fixture.join("startup"),
        );
        crate::write_install_boot_entries(&legacy, "127.0.0.1", 24100, 24101, false, false)
            .unwrap();
        let mut fresh = layout(&receipt, legacy.boot_dir.clone());
        let (effective, original) =
            adopt_legacy(&receipt, &legacy.boot_dir, &mut fresh, &Options::default()).unwrap();
        assert_eq!(fresh.main_boot_entry, legacy.main_boot_entry);
        assert_eq!(fresh.shadow_boot_entry, legacy.shadow_boot_entry);
        assert_eq!(effective.host, "127.0.0.1");
        assert_eq!((effective.main_port, effective.shadow_port), (24100, 24101));
        assert_eq!(original.len(), 2);
        let foreign = test_receipt(&fixture.join("foreign"));
        fs::create_dir_all(&foreign.data_dir).unwrap();
        let (unchanged, old) = adopt_legacy(
            &foreign,
            &legacy.boot_dir,
            &mut layout(&foreign, legacy.boot_dir.clone()),
            &Options::default(),
        )
        .unwrap();
        assert!(old.is_empty());
        assert_eq!(unchanged.host, DEFAULT_HOST);
    }
}
