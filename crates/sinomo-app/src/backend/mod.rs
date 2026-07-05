//! The app's view of the Sinomo runtime.
//!
//! The app shares the cluster's data directory and always operates *through a daemon*: on
//! launch it probes for one and attaches over RPC if present. If none is running it starts its
//! own daemon **in-process** (a background thread running the daemon control loop) and attaches
//! to that as a desktop app surface. The app is a pure frontend; runtime/cluster config still
//! goes through daemon services.
//! App-only UI preferences live locally under the Sinomo data directory.

pub mod app_config;
pub mod chat_engine;
pub mod cluster;
pub mod config;
pub mod evolution;
pub mod modules;
pub mod sessions;
pub mod sifs;

#[cfg(test)]
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use sinomo_runtime::cluster::ClusterControlInitRequest;
use sinomo_runtime::config::{ConfigClient, ConfigRegistry, ConfigStore, LocalConfigClient};
use sinomo_runtime::daemon::{
    DaemonApiClient, DaemonPaths, DaemonProbeStatus, DaemonRunOptions, DaemonServiceRegistryHook,
    DaemonStatus, RemoteConfigClient, probe_daemon_status, run_daemon_control_loop_with_services,
};
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::node::NodeProfile;

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
    /// attached to it as a desktop app surface.
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
    pub(crate) fn daemon_address(&self) -> Option<&str> {
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
    pub app_runtime: Arc<tokio::runtime::Runtime>,
    pub node: NodeProfile,
    pub connection: Connection,
    /// The configuration service the app reads/writes through — always RPC to a daemon (the
    /// app's own in-process one when no external daemon exists). The app holds no config state.
    pub config: Arc<dyn ConfigClient>,
    /// Runtime-control host shared by the chat worker and the app-started in-process daemon.
    pub agent_runtime_control: sinomo_agent::AgentRuntimeControlRegistry,
    /// Keeps an app-started in-process daemon alive; `None` when attached to an external one.
    _daemon: Option<Arc<InProcessDaemon>>,
    #[cfg(test)]
    _test_data_dir_cleanup: Option<Arc<TestDataDirCleanup>>,
}

impl Backend {
    /// Probe for a daemon and, failing that, stand up an embedded daemon for this desktop app
    /// surface. This blocks while the in-process daemon starts, so it is meant to run off the UI
    /// thread; `progress` reports each step for the launch status indicator.
    pub fn connect(progress: impl Fn(&str)) -> Self {
        progress("Preparing…");
        // When attached to an external daemon no in-process daemon installs the
        // KV-backed cluster directory, and the app's direct-probe fallbacks
        // (cluster health, endpoint hydration) would read an empty directory.
        sinomo_runtime::cluster::install_kv_cluster_directory();
        let data_dir = app_config::default_data_dir();
        // Best-effort: a missing layout shouldn't stop the app from opening.
        let _ = data_dir.ensure_root_layout();
        let data_dir = Arc::new(data_dir);
        let app_runtime = build_app_runtime();

        let node = NodeProfile::local("Sinomo App")
            .unwrap_or_else(|_| panic!("local node profile uses a valid trust domain"));
        let agent_runtime_control = sinomo_agent::AgentRuntimeControlRegistry::new();

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
            // No usable daemon. The app is near-useless without one, so start our own daemon
            // in-process, falling back to direct config only if that fails.
            DaemonProbeStatus::Locked { status: None } | DaemonProbeStatus::NotRunning => {
                progress("Starting a local daemon…");
                match start_in_process_daemon(&paths, &progress, agent_runtime_control.clone()) {
                    Some((status, handle)) => (
                        Connection::InProcessDaemon(Box::new(status)),
                        Some(Arc::new(handle)),
                    ),
                    None => (Connection::LocalFallback, None),
                }
            }
        };

        let connection =
            ensure_app_cluster(connection, &paths, data_dir.as_ref(), &node, &progress);
        progress("Loading workspace…");
        let config = build_config_client(&connection, &data_dir, app_runtime.handle());

        Backend {
            data_dir,
            app_runtime,
            node,
            connection,
            config,
            agent_runtime_control,
            _daemon: daemon,
            #[cfg(test)]
            _test_data_dir_cleanup: None,
        }
    }

    /// A fast, daemonless backend that reads local config directly — used by headless tooling and
    /// visual tests, which render the UI without the async connection (and without standing up a
    /// real daemon).
    pub fn local_fallback() -> Self {
        let (data_dir, _test_data_dir_cleanup) = local_fallback_data_dir();
        #[cfg(test)]
        {
            let mut backend = Self::local_fallback_for_data_dir(data_dir);
            backend._test_data_dir_cleanup = _test_data_dir_cleanup;
            backend
        }
        #[cfg(not(test))]
        {
            Self::local_fallback_for_data_dir(data_dir)
        }
    }

    /// Build a daemonless backend around an explicit data directory. This is intended for
    /// headless tooling and integration visual fixtures that need isolated SIFS-backed state
    /// without mutating the operator's default data directory.
    pub fn local_fallback_for_data_dir(data_dir: SinomoDataDir) -> Self {
        let _ = data_dir.ensure_root_layout();
        let data_dir = Arc::new(data_dir);
        let app_runtime = build_app_runtime();

        let node = NodeProfile::local("Sinomo App")
            .unwrap_or_else(|_| panic!("local node profile uses a valid trust domain"));

        let connection = Connection::LocalFallback;
        let config = build_config_client(&connection, &data_dir, app_runtime.handle());

        Backend {
            data_dir,
            app_runtime,
            node,
            connection,
            config,
            agent_runtime_control: sinomo_agent::AgentRuntimeControlRegistry::new(),
            _daemon: None,
            #[cfg(test)]
            _test_data_dir_cleanup: None,
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
                            "Connected to {kind} daemon at {} (desktop app surface)",
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

#[cfg(not(test))]
fn local_fallback_data_dir() -> (SinomoDataDir, ()) {
    (app_config::default_data_dir(), ())
}

#[cfg(test)]
fn local_fallback_data_dir() -> (SinomoDataDir, Option<Arc<TestDataDirCleanup>>) {
    let path = std::env::temp_dir().join(format!(
        "sinomo-app-local-fallback-test-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&path);
    let cleanup = Arc::new(TestDataDirCleanup { path: path.clone() });
    (SinomoDataDir::new(path), Some(cleanup))
}

#[cfg(test)]
struct TestDataDirCleanup {
    path: PathBuf,
}

#[cfg(test)]
impl Drop for TestDataDirCleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn ensure_app_cluster(
    connection: Connection,
    paths: &DaemonPaths,
    data_dir: &SinomoDataDir,
    node: &NodeProfile,
    progress: &impl Fn(&str),
) -> Connection {
    if !connection.is_daemon() || connection.is_clustered() {
        return connection;
    }

    progress("Initializing single-node cluster...");
    match ensure_app_cluster_through_daemon(&connection, data_dir, node) {
        Ok(_) => match probe_daemon_status(paths, DAEMON_RPC_TIMEOUT) {
            DaemonProbeStatus::Running(status)
            | DaemonProbeStatus::Locked {
                status: Some(status),
            } => match connection {
                Connection::Daemon(_) => Connection::Daemon(Box::new(status)),
                Connection::InProcessDaemon(_) => Connection::InProcessDaemon(Box::new(status)),
                Connection::LocalFallback => Connection::LocalFallback,
            },
            DaemonProbeStatus::Locked { status: None } | DaemonProbeStatus::NotRunning => {
                connection
            }
        },
        Err(error) => {
            tracing::warn!(%error, "sinomo-app could not initialize local cluster");
            connection
        }
    }
}

/// Daemon-first cluster init for a fresh app node: the attached daemon
/// initializes its own cluster through `/cluster/control/init`. Skipped when
/// cluster state already exists (the daemon's status simply lagged).
fn ensure_app_cluster_through_daemon(
    connection: &Connection,
    data_dir: &SinomoDataDir,
    node: &NodeProfile,
) -> Result<(), String> {
    if sinomo_cli::app_api::cluster_status(data_dir).clustered {
        return Ok(());
    }
    let address = connection
        .daemon_address()
        .ok_or_else(|| "no daemon connection for cluster init".to_string())?;
    let name = node.display_name.trim();
    let name = if name.is_empty() { "Sinomo App" } else { name };
    DaemonApiClient::at_address(address)
        .cluster_init(ClusterControlInitRequest {
            cluster_name: name.to_string(),
            node_display_name: Some(name.to_string()),
        })
        .map(|_| ())
        .map_err(|error| error.to_string())
}

/// Build the [`ConfigClient`] for the current connection. Normally this is an RPC client to the
/// attached daemon (external or the app's own in-process one). Only in the degraded
/// [`Connection::LocalFallback`] case does it read local config directly.
fn build_config_client(
    connection: &Connection,
    data_dir: &Arc<SinomoDataDir>,
    app_runtime: &tokio::runtime::Handle,
) -> Arc<dyn ConfigClient> {
    match connection.daemon_address() {
        Some(address) => Arc::new(RemoteConfigClient::new_on_runtime(
            address.to_string(),
            DAEMON_RPC_TIMEOUT,
            app_runtime,
        )),
        None => {
            let store = config::build_config_store()
                .map(Arc::new)
                .unwrap_or_else(|_| Arc::new(ConfigStore::new(ConfigRegistry::builder().build())));
            Arc::new(LocalConfigClient::new(store, (**data_dir).clone(), None))
        }
    }
}

fn build_app_runtime() -> Arc<tokio::runtime::Runtime> {
    Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .thread_name("sinomo-app-runtime")
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("sinomo-app Tokio runtime should start"),
    )
}

/// Start an in-process daemon on a background thread and wait for it to become reachable. Returns
/// `None` — leaving the app on the daemonless fallback — if it can't be started or doesn't come up
/// within [`IN_PROCESS_DAEMON_READY_TIMEOUT`].
fn start_in_process_daemon(
    paths: &DaemonPaths,
    progress: &impl Fn(&str),
    agent_runtime_control: sinomo_agent::AgentRuntimeControlRegistry,
) -> Option<(DaemonStatus, InProcessDaemon)> {
    let options = DaemonRunOptions::default();
    let loop_paths = paths.clone();
    let control_host = agent_runtime_control.clone();
    let thread = thread::Builder::new()
        .name("sinomo-app-daemon".to_string())
        .spawn(move || {
            // A fresh app-started daemon may still be pre-bootstrap; it serves config and joins a
            // cluster later if one is available.
            let hooks = vec![agent_runtime_control_service_hook(control_host)];
            if let Err(error) =
                run_daemon_control_loop_with_services(options, loop_paths, "daemon", None, hooks)
            {
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

fn agent_runtime_control_service_hook(
    control_host: sinomo_agent::AgentRuntimeControlRegistry,
) -> DaemonServiceRegistryHook {
    DaemonServiceRegistryHook::new(move |builder| {
        let host: Arc<dyn sinomo_agent::AgentRuntimeControlHost> = Arc::new(control_host.clone());
        sinomo_agent::register_services_with_runtime_control(builder, host)
    })
    .with_config_definitions(|builder| {
        let builder = sinomo_runtime::module::register_config_definitions(builder)?;
        sinomo_lm::register_config_definitions(builder).map_err(|error| error.to_string())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_runtime::node::NodeRole;
    use sinomo_runtime::service::ServiceRegistry;

    #[test]
    fn test_local_fallback_uses_temp_data_dir_and_cleans_up() {
        let default_root = app_config::default_data_dir().root();
        let backend = Backend::local_fallback();
        let root = backend.data_dir.root();
        assert_eq!(
            backend.node.role,
            NodeRole::Workstation,
            "the desktop app should identify as a workstation app surface"
        );

        assert_ne!(
            root, default_root,
            "unit-test fallback backends must not touch the operator data dir"
        );
        assert!(
            root.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("sinomo-app-local-fallback-test-")),
            "unit-test fallback data dir should be visibly test-scoped: {root:?}"
        );
        assert!(root.exists(), "fallback test data dir should be created");

        drop(backend);

        assert!(
            !root.exists(),
            "fallback test data dir should be removed when the backend drops"
        );
    }

    #[test]
    fn app_agent_runtime_control_hook_registers_read_and_control_routes() {
        let hook =
            agent_runtime_control_service_hook(sinomo_agent::AgentRuntimeControlRegistry::new());
        let config_registry = hook
            .register_config_definitions(sinomo_runtime::config::ConfigRegistry::builder())
            .expect("register app config definitions")
            .build();
        assert!(
            config_registry
                .get(sinomo_lm::LM_PROVIDER_CONFIG_KEY)
                .is_ok()
        );
        assert!(
            config_registry
                .get(sinomo_runtime::module::MODULE_CLUSTER_CONFIG_KEY)
                .is_ok()
        );

        let registry = hook.register(ServiceRegistry::builder()).build();

        for route in sinomo_agent::AGENT_OBJECT_RPC_ROUTES
            .iter()
            .chain(sinomo_agent::AGENT_RUNTIME_CONTROL_OBJECT_RPC_ROUTES.iter())
        {
            assert!(registry.owns(route), "missing app agent route {route}");
            assert_eq!(
                registry.category_for_route(route),
                Some(sinomo_agent::AGENT_RPC_CATEGORY)
            );
        }
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
