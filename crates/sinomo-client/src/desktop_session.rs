//! Own the desktop session's daemon independently of successive GUI processes.
use crate::{LocalDaemonConnection, connect_or_start_local_daemon_with_binary};
use sinomo_runtime::{
    api::{DaemonApiClient, DaemonPaths, DaemonProbeStatus, DaemonStatus, probe_daemon_status},
    cluster::ClusterControlInitRequest,
    local::SinomoDataDir,
};
use std::{
    path::PathBuf,
    sync::mpsc::{self, RecvTimeoutError, Sender},
    thread::JoinHandle,
    time::{Duration, Instant},
};

pub struct AppDaemonSupervisor {
    stop: Sender<()>,
    worker: Option<JoinHandle<()>>,
}

impl AppDaemonSupervisor {
    /// Startup runs off the launcher's thread so the GUI immediately displays progress and
    /// recovery controls. An attached external daemon is never restarted or stopped by us.
    pub fn start(data_dir: SinomoDataDir, binary: Option<PathBuf>) -> Result<Self, String> {
        let (stop, stopped) = mpsc::channel();
        let worker = std::thread::Builder::new()
            .name("sinomo-desktop-runtime".into())
            .spawn(move || {
                let mut delay = Duration::from_millis(250);
                loop {
                    if stopped.try_recv().is_ok() {
                        return;
                    }
                    match connect_or_start_local_daemon_with_binary(
                        &data_dir,
                        binary.as_deref(),
                        &|message| eprintln!("{message}"),
                    ) {
                        Ok(LocalDaemonConnection::Attached(_)) => {
                            let _ = stopped.recv();
                            return;
                        }
                        Ok(LocalDaemonConnection::Owned { status, handle }) => {
                            // Initialization drains the bootstrap host. Closing our exact child
                            // lease and restarting loads the full product with its cluster identity.
                            if status.cluster.is_none()
                                && status.app_launch_id.as_deref().is_some_and(|id| {
                                    id.ends_with(crate::daemon_process::BOOTSTRAP_LAUNCH_SUFFIX)
                                })
                            {
                                match DaemonApiClient::at_address(&status.address).cluster_init(
                                    ClusterControlInitRequest {
                                        cluster_name: "Sinomo App".into(),
                                        node_display_name: Some("Sinomo App".into()),
                                        node_role: None,
                                    },
                                ) {
                                    Ok(_) => {
                                        drop(handle);
                                        continue;
                                    }
                                    Err(error) => {
                                        eprintln!("desktop cluster initialization: {error}")
                                    }
                                }
                            }
                            let running_since = Instant::now();
                            loop {
                                match stopped.recv_timeout(Duration::from_millis(500)) {
                                    Err(RecvTimeoutError::Timeout) => {}
                                    _ => return,
                                }
                                match handle.has_exited() {
                                    Ok(false) => {}
                                    Ok(true) => break,
                                    Err(error) => {
                                        eprintln!("desktop daemon supervision: {error}");
                                        break;
                                    }
                                }
                            }
                            if running_since.elapsed() >= Duration::from_secs(30) {
                                delay = Duration::from_millis(250);
                            }
                            // The handle also closes the Windows process-tree job before another
                            // daemon generation can own this node's work.
                        }
                        Err(error) => eprintln!("desktop daemon startup: {error}"),
                    }
                    match stopped.recv_timeout(delay) {
                        Err(RecvTimeoutError::Timeout) => {}
                        _ => return,
                    }
                    delay = (delay * 2).min(Duration::from_secs(30));
                }
            })
            .map_err(|e| e.to_string())?;
        Ok(Self {
            stop,
            worker: Some(worker),
        })
    }
}

impl Drop for AppDaemonSupervisor {
    fn drop(&mut self) {
        let _ = self.stop.send(());
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

/// A supervised GUI only attaches; it must not race its parent to spawn a daemon whose lease
/// would be lost when the GUI is replaced.
pub fn connect_to_supervised_daemon(
    data_dir: &SinomoDataDir,
    progress: &impl Fn(&str),
) -> Result<DaemonStatus, String> {
    progress("Starting local services…");
    let paths = DaemonPaths::new(data_dir);
    let deadline = Instant::now() + Duration::from_secs(45);
    loop {
        if let DaemonProbeStatus::Running(status) =
            probe_daemon_status(&paths, Duration::from_millis(300))
        {
            if !crate::daemon_protocol_is_compatible(&status) {
                return Err(crate::incompatible_daemon_protocol_detail(&status));
            }
            // An installed daemon retains the GUI-driven init path. Our parent's bootstrap
            // helper must first be replaced with the fully initialized product.
            let bootstrap = status
                .app_launch_id
                .as_deref()
                .is_some_and(|id| id.ends_with(crate::daemon_process::BOOTSTRAP_LAUNCH_SUFFIX));
            if !bootstrap && (status.cluster.is_some() || status.app_launch_id.is_none()) {
                return Ok(status);
            }
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "local services did not become ready; inspect {}",
                data_dir.root().join("daemon/main.stderr.log").display()
            ));
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}
