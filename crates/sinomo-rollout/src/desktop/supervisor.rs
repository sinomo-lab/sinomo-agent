use super::*;
use sinomo_core::local::SinomoDataDir;
use std::{
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

const READY_TIMEOUT: Duration = Duration::from_secs(90);

#[cfg(test)]
struct LeasedDaemon {
    child: Child,
}
#[cfg(test)]
impl Drop for LeasedDaemon {
    fn drop(&mut self) {
        self.child.stdin.take();
        let deadline = Instant::now() + Duration::from_secs(12);
        while Instant::now() < deadline {
            if self.child.try_wait().ok().flatten().is_some() {
                return;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct SupervisedChild(Child);
impl std::ops::Deref for SupervisedChild {
    type Target = Child;
    fn deref(&self) -> &Child {
        &self.0
    }
}
impl std::ops::DerefMut for SupervisedChild {
    fn deref_mut(&mut self) -> &mut Child {
        &mut self.0
    }
}
impl Drop for SupervisedChild {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

/// The launcher retains the daemon lease while successive UI processes attach. It never
/// replaces/stops an installed daemon, and rollback always uses the same durable data directory.
pub fn supervise_app<Lease>(
    data_dir: &SinomoDataDir,
    bootstrap_app: &Path,
    args: &[String],
    ensure_runtime: impl FnOnce(Option<&Path>) -> Result<Lease>,
) -> Result<i32> {
    let releases = DesktopReleases::new(data_dir.root());
    releases.ensure_root()?;
    let singleton = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(releases.root.join("launcher.lock"))
        .map_err(err)?;
    singleton
        .try_lock_exclusive()
        .map_err(|_| "an app launcher is already running for this data directory".to_string())?;

    if matches!(
        releases.status()?.phase,
        AppUpdatePhase::Starting | AppUpdatePhase::RestartRequested
    ) {
        releases.rollback(
            "Recovered an interrupted app activation; retained the previous version.".into(),
        )?;
    }
    let original_app = fs::canonicalize(bootstrap_app).map_err(err)?;
    let active = active_launch(&releases)?;
    let bundled_cli = active.as_ref().and_then(|(_, cli)| cli.as_deref());
    let _daemon = ensure_runtime(bundled_cli)?;
    run_app_loop(data_dir, &releases, original_app, args, READY_TIMEOUT)
}

fn run_app_loop(
    data_dir: &SinomoDataDir,
    releases: &DesktopReleases,
    original_app: PathBuf,
    args: &[String],
    ready_timeout: Duration,
) -> Result<i32> {
    let mut app = active_app(releases)?.unwrap_or(original_app.clone());
    let mut activating = false;
    loop {
        let token = releases.begin_launch(activating)?;
        let mut child = match Command::new(&app)
            .args(args)
            .env("SINOMO_DATA_DIR", data_dir.root())
            .env(APP_SUPERVISED_ENV, "1")
            .env(APP_LAUNCH_TOKEN_ENV, &token)
            .stdin(Stdio::null())
            .spawn()
        {
            Ok(child) => SupervisedChild(child),
            Err(error) if activating => {
                releases.rollback(format!(
                    "App update failed to start; restored previous version: {error}"
                ))?;
                app = active_app(releases)?.unwrap_or(original_app.clone());
                activating = false;
                continue;
            }
            Err(error) => return Err(err(error)),
        };
        let deadline = Instant::now() + ready_timeout;
        let exit = loop {
            if let Some(exit) = child.try_wait().map_err(err)? {
                break exit.code().unwrap_or(1);
            }
            if activating && Instant::now() >= deadline {
                if releases.read()?.launch_ready {
                    activating = false;
                } else {
                    child.kill().map_err(err)?;
                    let _ = child.wait();
                    break 1;
                }
            }
            std::thread::sleep(Duration::from_millis(100));
        };
        let state = releases.read()?;
        if activating && !state.launch_ready {
            releases.rollback(format!(
                "App update did not restore the workspace (exit {exit}); restored previous version."
            ))?;
            app = active_app(releases)?.unwrap_or(original_app.clone());
            activating = false;
            continue;
        }
        // Durable consent survives a lost reply or a GUI crash before the restart exit code.
        if state.status.phase == AppUpdatePhase::RestartRequested {
            let id = state
                .status
                .candidate_release
                .as_deref()
                .ok_or("restart has no candidate")?;
            let candidate: Result<PathBuf> = (|| {
                let directory = releases.release_dir(id)?;
                let manifest = read_manifest(&directory)?;
                if release_id(&manifest)? != id {
                    return Err("candidate manifest identity changed".into());
                }
                verify_bundle(&directory, &manifest)?;
                Ok(directory.join(crate::executable_name("sinomo-app")))
            })();
            match candidate {
                Ok(candidate) => {
                    app = candidate;
                    activating = true;
                }
                Err(error) => {
                    releases.rollback(format!(
                        "Update verification failed; restored previous version: {error}"
                    ))?;
                    app = active_app(releases)?.unwrap_or(original_app.clone());
                    activating = false;
                }
            }
            continue;
        }
        releases.change(|state| {
            state.launch_token = None;
            state.launch_ready = false;
            Ok(())
        })?;
        return Ok(exit);
    }
}

fn active_app(releases: &DesktopReleases) -> Result<Option<PathBuf>> {
    Ok(active_launch(releases)?.map(|(app, _)| app))
}

// A pre-consolidation installed release can still restore its GUI, but its retired helper
// is never executed. The caller's current CLI hosts the daemon until a new bundle is activated.
// New imports and candidate activation continue to require the CLI in the verified manifest.
fn active_launch(releases: &DesktopReleases) -> Result<Option<(PathBuf, Option<PathBuf>)>> {
    let Some(id) = releases.status()?.active_release else {
        return Ok(None);
    };
    let directory = releases.release_dir(&id)?;
    let manifest = read_manifest(&directory)?;
    if release_id(&manifest)? != id {
        return Err("active manifest identity changed".into());
    }
    let cli_name = crate::executable_name("sinomo");
    let bundled_cli = manifest.files.iter().any(|file| file.path == cli_name);
    if bundled_cli {
        verify_bundle(&directory, &manifest)?;
    } else {
        if manifest.target_os != std::env::consts::OS
            || manifest.target_arch != std::env::consts::ARCH
        {
            return Err("desktop bundle is for a different platform".into());
        }
        validate_manifest_shape_for_binaries(
            &manifest,
            &[
                "sinomo-app",
                "sinomo-app-daemon",
                "sinomo-ui-extension-host",
            ],
        )?;
        verify_manifest_files(&directory, &manifest)?;
    }
    Ok(Some((
        directory.join(crate::executable_name("sinomo-app")),
        bundled_cli.then(|| directory.join(cli_name)),
    )))
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    fn script_bundle(root: &Path, name: &str, script: &str) -> PathBuf {
        let directory = root.join(name);
        fs::create_dir_all(&directory).unwrap();
        fs::write(directory.join("sinomo-app"), script).unwrap();
        fs::write(directory.join("sinomo"), "fixture helper").unwrap();
        fs::write(
            directory.join("sinomo-ui-extension-host"),
            "fixture UI host",
        )
        .unwrap();
        write_bundle_manifest(
            &directory,
            name.into(),
            &DESKTOP_BINARY_STEMS.map(str::to_owned),
        )
        .unwrap();
        directory
    }

    fn wait_until(mut condition: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !condition() {
            assert!(
                Instant::now() < deadline,
                "supervised process did not reach expected state"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn real_child_update_restores_previous_executable_when_candidate_exits_without_ready() {
        let root = sinomo_testdir::TestDir::new("app-supervisor-rollback");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let releases = DesktopReleases::new(data_dir.root());
        let old = script_bundle(
            &root,
            "old",
            "#!/bin/sh\n[ -f \"$SINOMO_DATA_DIR/finish\" ] && exit 0\ntouch \"$SINOMO_DATA_DIR/started\"\nwhile [ ! -f \"$SINOMO_DATA_DIR/restart\" ]; do sleep 0.01; done\nexit 75\n",
        );
        let active = releases.install_initial(&old).unwrap().active_release;
        let initial_app = active_app(&releases).unwrap().unwrap();
        let worker_releases = releases.clone();
        let worker_dir = data_dir.clone();
        let worker = std::thread::spawn(move || {
            run_app_loop(
                &worker_dir,
                &worker_releases,
                initial_app,
                &[],
                Duration::from_secs(2),
            )
        });
        wait_until(|| data_dir.root().join("started").exists());
        let token = releases.read().unwrap().launch_token.unwrap();
        releases
            .acknowledge_ready(AppUpdateReadyRequest {
                launch_token: token.clone(),
                restoration_schema: 1,
            })
            .unwrap();
        let bad = script_bundle(&root, "bad", "#!/bin/sh\nexit 17\n");
        let candidate = releases.stage(&bad).unwrap().candidate_release.unwrap();
        releases
            .request_restart(AppUpdateRestartRequest {
                candidate_release: candidate,
                launch_token: token,
            })
            .unwrap();
        fs::write(data_dir.root().join("finish"), "").unwrap();
        fs::write(data_dir.root().join("restart"), "").unwrap();
        assert_eq!(worker.join().unwrap().unwrap(), 0);
        assert_eq!(releases.status().unwrap().active_release, active);
        assert_eq!(releases.status().unwrap().phase, AppUpdatePhase::RolledBack);
    }

    #[test]
    fn real_child_update_commits_only_after_ready_and_keeps_runtime_lease_alive() {
        let root = sinomo_testdir::TestDir::new("app-supervisor-ready");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let releases = DesktopReleases::new(data_dir.root());
        let old = script_bundle(
            &root,
            "old",
            "#!/bin/sh\ntouch \"$SINOMO_DATA_DIR/started\"\nwhile [ ! -f \"$SINOMO_DATA_DIR/restart\" ]; do sleep 0.01; done\nexit 23\n",
        );
        let previous = releases.install_initial(&old).unwrap().active_release;
        let initial_app = active_app(&releases).unwrap().unwrap();
        let mut daemon = LeasedDaemon {
            child: Command::new("cat")
                .stdin(Stdio::piped())
                .stdout(Stdio::null())
                .spawn()
                .unwrap(),
        };
        let worker_releases = releases.clone();
        let worker_dir = data_dir.clone();
        let worker = std::thread::spawn(move || {
            run_app_loop(
                &worker_dir,
                &worker_releases,
                initial_app,
                &[],
                Duration::from_secs(2),
            )
        });
        wait_until(|| data_dir.root().join("started").exists());
        let token = releases.read().unwrap().launch_token.unwrap();
        let good = script_bundle(
            &root,
            "good",
            "#!/bin/sh\nwhile [ ! -f \"$SINOMO_DATA_DIR/finish\" ]; do sleep 0.01; done\nexit 0\n",
        );
        let candidate = releases.stage(&good).unwrap().candidate_release.unwrap();
        releases
            .request_restart(AppUpdateRestartRequest {
                candidate_release: candidate.clone(),
                launch_token: token,
            })
            .unwrap();
        fs::write(data_dir.root().join("restart"), "").unwrap();
        wait_until(|| releases.status().unwrap().phase == AppUpdatePhase::Starting);
        assert_eq!(releases.status().unwrap().active_release, previous);
        assert!(daemon.child.try_wait().unwrap().is_none());
        let token = releases.read().unwrap().launch_token.unwrap();
        releases
            .acknowledge_ready(AppUpdateReadyRequest {
                launch_token: token,
                restoration_schema: 1,
            })
            .unwrap();
        fs::write(data_dir.root().join("finish"), "").unwrap();
        assert_eq!(worker.join().unwrap().unwrap(), 0);
        assert_eq!(releases.status().unwrap().active_release, Some(candidate));
        assert_eq!(releases.status().unwrap().previous_release, previous);
        assert!(daemon.child.try_wait().unwrap().is_none());
    }
}
