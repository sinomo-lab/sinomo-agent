//! Removal outranks work draining. The durable fence is written before shutdown;
//! process identity, rather than a global image name, limits forced termination.
use super::*;
use std::collections::BTreeSet;
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System, UpdateKind};

pub(super) fn terminate_owned(root: &Path, data: &Path) -> Result<()> {
    let bin = image_path(&data.join("bin"));
    let rollout = image_path(&data.join("rollout"));
    let selected_data = image_path(data);
    let mut system = System::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        system.refresh_processes(ProcessesToUpdate::All, true);
        let mut owned = BTreeSet::new();
        if let Ok(bytes) = fs::read(data.join("rollout/activation.owner.json")) {
            if let Ok(owner) = serde_json::from_slice::<ProcessIdentity>(&bytes) {
                if let Some(pid) = owner_pid(&system, &owner) {
                    owned.insert(pid);
                }
            }
        }
        if let Ok(receipt) = read_receipt(root) {
            if let Some(owner) = receipt.setup_owner {
                if let Some(pid) = owner_pid(&system, &owner) {
                    owned.insert(pid);
                }
            }
        }
        // Inspect deployment selection only for Sinomo runtime images. This
        // includes long-running CLI module calls outside DATA/bin without
        // touching unrelated applications' environment or process state.
        let candidates: Vec<_> = system
            .processes()
            .iter()
            .filter_map(|(pid, p)| {
                p.exe()
                    .and_then(|path| path.file_stem())
                    .and_then(|name| name.to_str())
                    .filter(|name| {
                        [
                            "sinomo",
                            "sinomo-shadow",
                            "sinomo-app",
                            "sinomo-ui-extension-host",
                            "sinomo-app-daemon",
                        ]
                        .contains(name)
                    })
                    .map(|_| *pid)
            })
            .collect();
        system.refresh_processes_specifics(
            ProcessesToUpdate::Some(&candidates),
            true,
            ProcessRefreshKind::nothing().with_environ(UpdateKind::Always),
        );
        for (pid, process) in system.processes() {
            if process.status() == sysinfo::ProcessStatus::Zombie {
                continue;
            }
            let Some(exe) = process.exe() else { continue };
            let exe = image_path(exe);
            let name = exe.file_stem().and_then(|s| s.to_str()).unwrap_or("");
            let runtime = candidates.contains(pid);
            let inside = exe.parent() == Some(bin.as_path()) || exe.starts_with(&rollout);
            let deployment = runtime
                .then(|| environment_data(process, &exe, !inside))
                .flatten();
            let belongs = match deployment {
                Some(selected) => image_path(&selected) == selected_data,
                None => name.starts_with("sinomo") && inside,
            };
            if belongs {
                owned.insert(*pid);
            }
        }
        owned.remove(&Pid::from_u32(std::process::id()));
        // Capture descendants before killing parents; builders and extension hosts
        // need not have a Sinomo executable name.
        loop {
            let before = owned.len();
            for (pid, process) in system.processes() {
                if process.status() == sysinfo::ProcessStatus::Zombie {
                    continue;
                }
                if process
                    .parent()
                    .is_some_and(|parent| owned.contains(&parent))
                {
                    owned.insert(*pid);
                }
            }
            if before == owned.len() {
                break;
            }
        }
        owned.remove(&Pid::from_u32(std::process::id()));
        if owned.is_empty() {
            return Ok(());
        }
        for pid in &owned {
            if let Some(process) = system.process(*pid) {
                process.kill();
            }
        }
        if Instant::now() >= deadline {
            return Err(format!("OS refused to terminate deployment processes: {owned:?}").into());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

pub(super) fn stop(
    root: &Path,
    data: &Path,
    services: Option<&services::Registration>,
) -> Result<()> {
    use sinomo_daemon::{DaemonPaths, DaemonProbeStatus, probe_daemon_status};
    if let Some(services) = services {
        if let Err(error) = services::stop_manager(services) {
            eprintln!("Service shutdown: {error}; forcing deployment shutdown.");
        }
    }
    // Recovery is requested to stop first. A failed RPC or stalled drain cannot
    // veto removal; the final process scan also handles unresponsive launchers.
    for name in ["shadow-daemon", "daemon"] {
        let paths = DaemonPaths::with_root(data.join(name));
        if let DaemonProbeStatus::Running(status) =
            probe_daemon_status(&paths, Duration::from_millis(250))
        {
            let _ = sinomo_daemon::send_daemon_shutdown_request(
                &status.address,
                Duration::from_secs(1),
            );
        }
    }
    terminate_owned(root, data)
}

// A mapped image can outlive its directory entry. Keep exact path ownership
// usable during the final scan after deletion, including Linux's deleted suffix.
fn image_path(path: &Path) -> PathBuf {
    let text = path.to_string_lossy();
    let path = Path::new(text.strip_suffix(" (deleted)").unwrap_or(&text));
    local::command_path(&fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf()))
}

fn owner_pid(system: &System, owner: &ProcessIdentity) -> Option<Pid> {
    let pid = Pid::from_u32(owner.pid);
    system
        .process(pid)
        .filter(|process| {
            process.status() != sysinfo::ProcessStatus::Zombie
                && process.start_time() == owner.start_time
                && process
                    .exe()
                    .is_some_and(|exe| image_path(exe) == image_path(&owner.exe))
        })
        .map(|_| pid)
}

fn environment_data(process: &sysinfo::Process, exe: &Path, include_home: bool) -> Option<PathBuf> {
    let value = |key: &str| {
        process
            .environ()
            .iter()
            .filter_map(|item| item.to_str())
            .filter_map(|item| item.split_once('='))
            .find(|(name, _)| name.eq_ignore_ascii_case(key))
            .map(|(_, value)| value)
            .filter(|value| !value.trim().is_empty())
    };
    value("SINOMO_DATA_DIR")
        .map(PathBuf::from)
        .or_else(|| {
            exe.parent()
                .and_then(|parent| local::installed_data_dir(parent).ok().flatten())
        })
        .or_else(|| {
            include_home
                .then(|| value("USERPROFILE").or_else(|| value("HOME")))
                .flatten()
                .map(|home| PathBuf::from(home).join(".sinomo"))
        })
}
