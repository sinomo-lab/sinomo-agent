//! The app's view of the Sinomo runtime.
//!
//! The app shares the cluster's data directory and always operates *through a daemon*: on
//! launch it probes for one and attaches over RPC if present. If none is running it starts its
//! own daemon **in-process** (a background thread running the daemon control loop) and attaches
//! to that as a **mobile node** — unlike the CLI, which degrades to daemonless local mode. The
//! app is a pure frontend; it holds no config/runtime state of its own.

pub mod chat_engine;
pub mod config;
pub mod sessions;
pub mod sifs;

use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use sinomo_runtime::config::{ConfigClient, ConfigRegistry, ConfigStore, LocalConfigClient};
use sinomo_runtime::daemon::{
    DaemonPaths, DaemonProbeStatus, DaemonRunOptions, DaemonStatus, RemoteConfigClient,
    probe_daemon_status, run_daemon_control_loop,
};
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::node::{NodeProfile, NodeRole};

const DAEMON_PROBE_TIMEOUT: Duration = Duration::from_millis(400);
/// Timeout for config RPCs to a connected daemon.
const DAEMON_RPC_TIMEOUT: Duration = Duration::from_secs(5);
/// How long to wait for an app-started in-process daemon to bind and publish its status.
const IN_PROCESS_DAEMON_READY_TIMEOUT: Duration = Duration::from_secs(15);

/// Owns the thread running an app-started in-process daemon, keeping it alive for the lifetime
/// of the [`Backend`]. The OS reclaims the thread on process exit; a clean shutdown RPC is a
/// future refinement.
struct InProcessDaemon {
    _thread: JoinHandle<()>,
}

/// How the app is talking to the runtime. The app always works through a daemon — either a
/// pre-existing one, or one it starts itself in-process. Only a hard failure leaves it on the
/// degraded local fallback.
#[derive(Clone)]
pub enum Connection {
    /// Attached to a daemon we did not start (and, through it, possibly a cluster).
    Daemon(Box<DaemonStatus>),
    /// No external daemon was running, so the app started its own in-process daemon and
    /// attached to it as a mobile node.
    InProcessDaemon(Box<DaemonStatus>),
    /// Degraded: no daemon could be reached or started; reading local config directly.
    LocalFallback,
}

impl Connection {
    pub fn is_daemon(&self) -> bool {
        matches!(self, Connection::Daemon(_) | Connection::InProcessDaemon(_))
    }

    pub fn is_clustered(&self) -> bool {
        matches!(
            self,
            Connection::Daemon(status) | Connection::InProcessDaemon(status)
                if status.cluster.is_some()
        )
    }

    /// The control address of the attached daemon, if any.
    fn daemon_address(&self) -> Option<&str> {
        match self {
            Connection::Daemon(status) | Connection::InProcessDaemon(status) => {
                Some(status.address.as_str())
            }
            Connection::LocalFallback => None,
        }
    }
}

/// Shared, cheaply-cloneable handle to runtime facilities.
#[derive(Clone)]
pub struct Backend {
    pub data_dir: Arc<SinomoDataDir>,
    pub node: NodeProfile,
    pub connection: Connection,
    /// The configuration service the app reads/writes through — always RPC to a daemon (the
    /// app's own in-process one when no external daemon exists). The app holds no config state.
    pub config: Arc<dyn ConfigClient>,
    /// Keeps an app-started in-process daemon alive; `None` when attached to an external one.
    _daemon: Option<Arc<InProcessDaemon>>,
}

impl Backend {
    /// Probe for a daemon and, failing that, stand up an embedded mobile node. This blocks while
    /// the in-process daemon starts, so it is meant to run off the UI thread; `progress` reports
    /// each step for the launch status indicator.
    pub fn connect(progress: impl Fn(&str)) -> Self {
        progress("Preparing…");
        let data_dir = SinomoDataDir::from_env_or_home()
            .unwrap_or_else(|_| SinomoDataDir::new(std::env::temp_dir().join("sinomo")));
        // Best-effort: a missing layout shouldn't stop the app from opening.
        let _ = data_dir.ensure_root_layout();
        let data_dir = Arc::new(data_dir);

        let mut node = NodeProfile::local("Sinomo App")
            .unwrap_or_else(|_| panic!("local node profile uses a valid trust domain"));
        node.role = NodeRole::Mobile;

        progress("Looking for a daemon…");
        let paths = DaemonPaths::new(&data_dir);
        let (connection, daemon) = match probe_daemon_status(&paths, DAEMON_PROBE_TIMEOUT) {
            DaemonProbeStatus::Running(status)
            | DaemonProbeStatus::Locked {
                status: Some(status),
            } => {
                progress("Connecting to daemon…");
                (Connection::Daemon(Box::new(status)), None)
            }
            // No usable daemon. The app is near-useless without one, so — unlike the CLI, which
            // degrades to daemonless local mode — we start our own daemon in-process and run as
            // a mobile node, falling back to direct local config only if that fails.
            DaemonProbeStatus::Locked { status: None } | DaemonProbeStatus::NotRunning => {
                progress("Starting a local daemon…");
                match start_in_process_daemon(&paths, &progress) {
                    Some((status, handle)) => (
                        Connection::InProcessDaemon(Box::new(status)),
                        Some(Arc::new(handle)),
                    ),
                    None => (Connection::LocalFallback, None),
                }
            }
        };

        progress("Loading workspace…");
        let config = build_config_client(&connection, &data_dir);

        Backend {
            data_dir,
            node,
            connection,
            config,
            _daemon: daemon,
        }
    }

    /// A fast, daemonless backend that reads local config directly — used by headless tooling and
    /// visual tests, which render the UI without the async connection (and without standing up a
    /// real daemon).
    pub fn local_fallback() -> Self {
        let data_dir = SinomoDataDir::from_env_or_home()
            .unwrap_or_else(|_| SinomoDataDir::new(std::env::temp_dir().join("sinomo")));
        let _ = data_dir.ensure_root_layout();
        let data_dir = Arc::new(data_dir);

        let mut node = NodeProfile::local("Sinomo App")
            .unwrap_or_else(|_| panic!("local node profile uses a valid trust domain"));
        node.role = NodeRole::Mobile;

        let connection = Connection::LocalFallback;
        let config = build_config_client(&connection, &data_dir);

        Backend {
            data_dir,
            node,
            connection,
            config,
            _daemon: None,
        }
    }

    /// Short label for the title-bar status pill.
    pub fn mode_label(&self) -> String {
        match &self.connection {
            Connection::Daemon(status) => {
                if status.cluster.is_some() {
                    "Cluster".to_string()
                } else {
                    "Daemon".to_string()
                }
            }
            Connection::InProcessDaemon(status) => {
                if status.cluster.is_some() {
                    "Cluster · mobile".to_string()
                } else {
                    "Local · mobile".to_string()
                }
            }
            Connection::LocalFallback => "Local (no daemon)".to_string(),
        }
    }

    /// One-line human description of the current connection.
    pub fn status_detail(&self) -> String {
        match &self.connection {
            Connection::Daemon(status) | Connection::InProcessDaemon(status) => {
                match &status.cluster {
                    Some(cluster) => format!(
                        "Connected to daemon at {} · cluster {} · {}",
                        status.address,
                        short_id(&format!("{:?}", cluster.cluster_id)),
                        format!("{:?}", cluster.member_role).to_lowercase(),
                    ),
                    None => {
                        let kind = if matches!(self.connection, Connection::InProcessDaemon(_)) {
                            "in-process"
                        } else {
                            "local"
                        };
                        format!(
                            "Connected to {kind} daemon at {} (mobile node)",
                            status.address
                        )
                    }
                }
            }
            Connection::LocalFallback => {
                "No daemon available · reading local config directly".to_string()
            }
        }
    }

    /// Whether the runtime is healthy enough to talk to.
    pub fn is_online(&self) -> bool {
        true
    }
}

/// Build the [`ConfigClient`] for the current connection. Normally this is an RPC client to the
/// attached daemon (external or the app's own in-process one). Only in the degraded
/// [`Connection::LocalFallback`] case does it read local config directly.
fn build_config_client(
    connection: &Connection,
    data_dir: &Arc<SinomoDataDir>,
) -> Arc<dyn ConfigClient> {
    match connection.daemon_address() {
        Some(address) => Arc::new(RemoteConfigClient::new(
            address.to_string(),
            DAEMON_RPC_TIMEOUT,
        )),
        None => {
            let store = config::build_config_store()
                .map(Arc::new)
                .unwrap_or_else(|_| Arc::new(ConfigStore::new(ConfigRegistry::builder().build())));
            Arc::new(LocalConfigClient::new(store, (**data_dir).clone(), None))
        }
    }
}

/// Start an in-process daemon (mobile node) on a background thread and wait for it to become
/// reachable. Returns `None` — leaving the app on the degraded local fallback — if it can't be
/// started or doesn't come up within [`IN_PROCESS_DAEMON_READY_TIMEOUT`].
fn start_in_process_daemon(
    paths: &DaemonPaths,
    progress: &impl Fn(&str),
) -> Option<(DaemonStatus, InProcessDaemon)> {
    let options = DaemonRunOptions::default();
    let loop_paths = paths.clone();
    let thread = thread::Builder::new()
        .name("sinomo-app-daemon".to_string())
        .spawn(move || {
            // A fresh mobile node has no cluster to resume; the daemon serves local config and
            // joins a cluster later if one is available.
            if let Err(error) = run_daemon_control_loop(options, loop_paths, "daemon", None) {
                tracing::warn!(%error, "in-process daemon exited");
            }
        })
        .ok()?;

    progress("Waiting for the daemon to come up…");
    let deadline = Instant::now() + IN_PROCESS_DAEMON_READY_TIMEOUT;
    loop {
        if let DaemonProbeStatus::Running(status) =
            probe_daemon_status(paths, Duration::from_millis(300))
        {
            return Some((status, InProcessDaemon { _thread: thread }));
        }
        if Instant::now() >= deadline {
            tracing::warn!("in-process daemon did not become ready within the timeout");
            return None;
        }
        thread::sleep(Duration::from_millis(200));
    }
}

fn short_id(id: &str) -> String {
    let trimmed = id.trim();
    if trimmed.len() <= 10 {
        trimmed.to_string()
    } else {
        format!("{}…", &trimmed[..10])
    }
}
