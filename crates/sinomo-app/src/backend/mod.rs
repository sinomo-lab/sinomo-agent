//! The app's view of the Sinomo runtime.
//!
//! The app shares the cluster's data directory and always operates *through a daemon*: on
//! launch it probes for one and attaches over RPC if present. On desktop, if none is running it
//! starts the full daemon in a sibling process whose lifetime is leased by this app. Android keeps
//! its resource-constrained embedded host until its Activity callbacks have a cross-process bridge.
//! Runtime and cluster configuration still go through daemon services.
//! App-only UI preferences live locally under the Sinomo data directory.

pub mod agents;
pub mod app_config;
pub mod chat_engine;
pub mod cluster;
pub mod config;
pub mod modules;
pub mod mounts;
pub mod observe;
pub mod remote_management;
pub mod settings_node;
pub mod sifs;
pub mod ui_extensions;

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
#[cfg(target_os = "android")]
use std::thread::JoinHandle;
use std::time::Duration;
#[cfg(target_os = "android")]
use std::time::Instant;

#[cfg(target_os = "android")]
use sinomo_client::api::DaemonClusterStatus;
#[cfg(any(target_os = "android", test))]
use sinomo_client::api::DaemonRunOptions;
use sinomo_client::api::{
    DaemonApiClient, DaemonApiError, DaemonPaths, DaemonProbeStatus, DaemonStatus,
    RemoteConfigClient, probe_daemon_status, probe_daemon_status_at_address,
};
use sinomo_runtime::cluster::{
    CLUSTER_CONTROL_ROLE_ROUTE, ClusterControlInitRequest, ClusterControlRoleRequest,
};
use sinomo_runtime::config::{ConfigClient, ConfigRegistry, ConfigStore, LocalConfigClient};
use sinomo_runtime::core::ClusterNodeRole;
#[cfg(target_os = "android")]
use sinomo_runtime::daemon::{
    DaemonClusterProcess, DaemonExecutionProfile, DaemonRuntimeConfig, daemon_cluster_status,
    prepare_daemon_control_listener, run_daemon_control_loop_with_prepared_listener,
};
use sinomo_runtime::local::SinomoDataDir;
#[cfg(target_os = "android")]
use sinomo_runtime::network::ClusterPaths;
use sinomo_runtime::node::{NodeId, NodeProfile};
#[cfg(test)]
use sinomo_runtime::vfs::VfsHandle;

#[cfg(target_os = "android")]
type AppAgentRuntimeControl = sinomo_agent::AgentRuntimeControlRegistry;
#[cfg(not(target_os = "android"))]
type AppAgentRuntimeControl = ();

fn app_agent_runtime_control() -> AppAgentRuntimeControl {
    #[cfg(target_os = "android")]
    {
        sinomo_agent::AgentRuntimeControlRegistry::new()
    }
    #[cfg(not(target_os = "android"))]
    {}
}

const DAEMON_PROBE_TIMEOUT: Duration = Duration::from_millis(400);
/// Timeout for bounded startup/config RPCs to a connected daemon.
const DAEMON_RPC_TIMEOUT: Duration = Duration::from_secs(5);
/// How long to wait for an Android embedded daemon to bind and publish its status.
#[cfg(target_os = "android")]
const IN_PROCESS_DAEMON_READY_TIMEOUT: Duration = Duration::from_secs(15);
/// Cluster initialization replaces the identity captured by foundational daemon services. Give
/// the old embedded daemon time to drain before binding its replacement to the same address.
const IN_PROCESS_DAEMON_STOP_TIMEOUT: Duration = Duration::from_secs(5);
static LOCAL_FALLBACK_DATA_DIR_SEQUENCE: AtomicU64 = AtomicU64::new(0);
/// A legacy Android node only needs this migration once. Keep transient retries
/// bounded and sparse so a disconnected phone does not turn compatibility work
/// into an unbounded background wake source.
const MOBILE_ROLE_MIGRATION_RETRY_DELAYS: [Duration; 5] = [
    Duration::from_secs(2),
    Duration::from_secs(10),
    Duration::from_secs(30),
    Duration::from_secs(2 * 60),
    Duration::from_secs(5 * 60),
];

/// Android nodes opt into the cluster's mobile maintenance cadence at their
/// first typed init/join transaction. Other app targets omit the field and
/// retain the control API's workstation default.
pub(crate) fn app_requested_cluster_node_role() -> Option<ClusterNodeRole> {
    #[cfg(target_os = "android")]
    {
        Some(ClusterNodeRole::Mobile)
    }
    #[cfg(not(target_os = "android"))]
    {
        None
    }
}

#[cfg(target_os = "android")]
fn embedded_daemon_execution_profile() -> DaemonExecutionProfile {
    DaemonExecutionProfile::Mobile
}

#[cfg(target_os = "android")]
fn embedded_product_component_profile(
    cluster_initialized: bool,
) -> sinomo_product_profile::ProductComponentProfile {
    sinomo_product_profile::ProductComponentProfile::mobile_embedded(cluster_initialized)
}

/// Owns the thread running the Android embedded daemon.
#[cfg(target_os = "android")]
struct OwnedDaemonHandle {
    _thread: JoinHandle<()>,
}

#[cfg(target_os = "android")]
impl OwnedDaemonHandle {
    fn owns_status(&self, status: &DaemonStatus) -> bool {
        status.pid == std::process::id()
    }
}

#[cfg(not(target_os = "android"))]
use sinomo_client::OwnedDaemonHandle;

/// How the app is talking to the runtime. The app always works through a daemon — either a
/// pre-existing one or one whose lifecycle it owns. Only a hard failure leaves it on the degraded
/// local fallback.
#[derive(Clone)]
pub enum Connection {
    /// Attached to a daemon we did not start (and, through it, possibly a cluster).
    Daemon(Box<DaemonStatus>),
    /// No external daemon was running, so the app started and owns this daemon's lifecycle.
    /// It is a sibling process on desktop and remains embedded on Android.
    AppOwnedDaemon(Box<DaemonStatus>),
    /// Degraded: no daemon could be reached or started; reading local config directly.
    LocalFallback { reason: Option<String> },
}

impl Connection {
    pub fn is_daemon(&self) -> bool {
        matches!(self, Connection::Daemon(_) | Connection::AppOwnedDaemon(_))
    }

    pub fn is_clustered(&self) -> bool {
        matches!(
            self,
            Connection::Daemon(status) | Connection::AppOwnedDaemon(status)
                if status.cluster.is_some()
        )
    }

    /// The control address of the attached daemon, if any.
    pub(crate) fn daemon_address(&self) -> Option<&str> {
        match self {
            Connection::Daemon(status) | Connection::AppOwnedDaemon(status) => {
                Some(status.address.as_str())
            }
            Connection::LocalFallback { .. } => None,
        }
    }

    pub fn local_fallback_reason(&self) -> Option<&str> {
        match self {
            Connection::LocalFallback { reason } => reason.as_deref(),
            Connection::Daemon(_) | Connection::AppOwnedDaemon(_) => None,
        }
    }
}

#[cfg(test)]
fn build_catalog_read_vfs(connection: &Connection) -> Result<VfsHandle, String> {
    let address = connection.daemon_address().ok_or_else(|| {
        "resident daemon SIFS is unavailable; catalog reads require a daemon connection".to_string()
    })?;
    sinomo_runtime::vfs::daemon_sifs(address.to_string()).map_err(|error| error.to_string())
}

/// Shared, cheaply-cloneable handle to runtime facilities.
#[derive(Clone)]
pub struct Backend {
    pub data_dir: Arc<SinomoDataDir>,
    pub app_runtime: Arc<tokio::runtime::Runtime>,
    pub node: NodeProfile,
    pub connection: Connection,
    #[cfg(test)]
    catalog_read_vfs: Result<VfsHandle, String>,
    /// The configuration service the app reads/writes through — always RPC to a daemon (the
    /// app's own in-process one when no external daemon exists). The app holds no config state.
    pub config: Arc<dyn ConfigClient>,
    /// Captured node target for a Settings worker; the app connection stays local.
    pub settings_node_id: Option<String>,
    /// Keeps the exact app-owned daemon alive; `None` when attached to an external one.
    _daemon: Option<Arc<OwnedDaemonHandle>>,
    /// Drop catalog clients before draining their native owner, and drain before DATA cleanup.
    #[cfg(test)]
    _native_catalog_owner:
        Option<Arc<sinomo_runtime::vfs::native_test_support::NativeMountedOwner>>,
    /// Keeps the isolated data directory used by [`Backend::local_fallback`] alive for exactly as
    /// long as the headless backend. Integration tests compile this crate without `cfg(test)`, so
    /// this cannot be test-only without silently falling back to the operator's real data dir.
    _local_fallback_data_dir_cleanup: Option<Arc<LocalFallbackDataDirCleanup>>,
}

impl Backend {
    /// Probe for a daemon and, failing that, start an app-owned daemon for this surface. This
    /// blocks during readiness, so it is meant to run off the UI thread; `progress` reports each
    /// step for the launch status indicator.
    pub fn connect(progress: impl Fn(&str)) -> Self {
        Self::connect_with_node_resources(
            progress,
            sinomo_runtime::device::NodeResourceMonitor::default(),
        )
    }

    pub(crate) fn connect_with_node_resources(
        progress: impl Fn(&str),
        node_resources: sinomo_runtime::device::NodeResourceMonitor,
    ) -> Self {
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
        let agent_runtime_control = app_agent_runtime_control();

        let paths = DaemonPaths::new(&data_dir);
        let daemon_connection = connect_local_daemon(
            data_dir.as_ref(),
            &paths,
            &progress,
            agent_runtime_control.clone(),
            node_resources.clone(),
        );
        let (connection, mut daemon) = match daemon_connection {
            Ok((status, Some(handle))) => {
                (Connection::AppOwnedDaemon(Box::new(status)), Some(handle))
            }
            Ok((status, None)) => {
                progress("Connecting to daemon…");
                (Connection::Daemon(Box::new(status)), None)
            }
            Err(error) => {
                tracing::warn!(%error, "sinomo-app could not connect to an app-compatible daemon");
                progress(&format!("Local daemon failed: {error}"));
                (
                    Connection::LocalFallback {
                        reason: Some(error),
                    },
                    None,
                )
            }
        };

        let (mut connection, restart_embedded_daemon) =
            ensure_app_cluster(connection, &paths, data_dir.as_ref(), &node, &progress);
        if restart_embedded_daemon {
            progress("Restarting the local daemon with its cluster identity...");
            match restart_owned_daemon_after_cluster_init(
                &paths,
                &progress,
                agent_runtime_control.clone(),
                node_resources.clone(),
                &mut daemon,
            ) {
                Ok((status, true)) => {
                    connection = Connection::AppOwnedDaemon(Box::new(status));
                }
                Ok((status, false)) => {
                    connection = Connection::Daemon(Box::new(status));
                }
                Err(error) => {
                    tracing::warn!(%error, "sinomo-app could not restart app-owned daemon after cluster initialization");
                    progress(&format!("Local daemon failed: {error}"));
                    connection = Connection::LocalFallback {
                        reason: Some(error),
                    };
                }
            }
        }
        reconcile_android_cluster_node_role(&connection, data_dir.as_ref());
        progress("Preparing configuration…");
        let config = build_config_client(&connection, &data_dir, app_runtime.handle());
        #[cfg(test)]
        let catalog_read_vfs = build_catalog_read_vfs(&connection);

        Backend {
            data_dir,
            app_runtime,
            node,
            connection,
            #[cfg(test)]
            catalog_read_vfs,
            config,
            settings_node_id: None,
            _daemon: daemon.map(Arc::new),
            #[cfg(test)]
            _native_catalog_owner: None,
            _local_fallback_data_dir_cleanup: None,
        }
    }

    /// A fast, daemonless backend that reads local config directly — used by headless tooling and
    /// visual tests, which render the UI without the async connection (and without standing up a
    /// real daemon).
    pub fn local_fallback() -> Self {
        #[cfg(test)]
        let owner = {
            let name = format!("app-native-{}", sinomo_runtime::core::Id::generate());
            Arc::new(
                sinomo_runtime::vfs::native_test_support::NativeMountedOwner::start(&name)
                    .expect("headless test native owner"),
            )
        };
        #[cfg(test)]
        let data_dir = owner.data_dir().clone();
        #[cfg(not(test))]
        let (data_dir, cleanup) = local_fallback_data_dir();
        let mut backend = Self::local_fallback_for_data_dir(data_dir);
        #[cfg(not(test))]
        {
            backend._local_fallback_data_dir_cleanup = Some(cleanup);
        }
        #[cfg(test)]
        {
            backend.node.node_id = owner.node_id().clone();
            backend._native_catalog_owner = Some(owner);
            backend
                .use_mounted_catalog_reads_for_tests()
                .expect("headless test catalog VFS");
        }
        backend
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

        let connection = Connection::LocalFallback { reason: None };
        let config = build_config_client(&connection, &data_dir, app_runtime.handle());
        #[cfg(test)]
        let catalog_read_vfs = build_catalog_read_vfs(&connection);

        Backend {
            data_dir,
            app_runtime,
            node,
            connection,
            #[cfg(test)]
            catalog_read_vfs,
            config,
            settings_node_id: None,
            _daemon: None,
            #[cfg(test)]
            _native_catalog_owner: None,
            _local_fallback_data_dir_cleanup: None,
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
            Connection::AppOwnedDaemon(status) => {
                #[cfg(target_os = "android")]
                let surface = "mobile";
                #[cfg(not(target_os = "android"))]
                let surface = "app-owned";
                if status.cluster.is_some() {
                    format!("Cluster · {surface}")
                } else {
                    format!("Local · {surface}")
                }
            }
            Connection::LocalFallback { reason: Some(_) } => "Local (daemon failed)".to_string(),
            Connection::LocalFallback { reason: None } => "Local (no daemon)".to_string(),
        }
    }

    /// One-line human description of the current connection.
    pub fn status_detail(&self) -> String {
        match &self.connection {
            Connection::Daemon(status) | Connection::AppOwnedDaemon(status) => {
                match &status.cluster {
                    Some(cluster) => format!(
                        "Connected to daemon at {} · cluster {} · {}",
                        status.address,
                        short_id(&format!("{:?}", cluster.cluster_id)),
                        format!("{:?}", cluster.member_role).to_lowercase(),
                    ),
                    None => {
                        let kind = if matches!(self.connection, Connection::AppOwnedDaemon(_)) {
                            #[cfg(target_os = "android")]
                            {
                                "embedded"
                            }
                            #[cfg(not(target_os = "android"))]
                            {
                                "app-owned"
                            }
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
            Connection::LocalFallback {
                reason: Some(reason),
            } => {
                format!("No daemon available · app-owned daemon failed: {reason}")
            }
            Connection::LocalFallback { reason: None } => {
                "No daemon available · reading local config directly".to_string()
            }
        }
    }

    pub fn connection_issue_detail(&self) -> Option<String> {
        self.connection.local_fallback_reason().map(|reason| {
            format!(
                "app-owned daemon failed: {reason}; daemon-backed chat and SIFS are unavailable"
            )
        })
    }

    #[cfg(test)]
    pub(crate) fn catalog_read_vfs(&self) -> Result<VfsHandle, String> {
        self.catalog_read_vfs.clone()
    }

    #[cfg(test)]
    pub(crate) fn use_mounted_catalog_reads_for_tests(&mut self) -> Result<(), String> {
        if self._native_catalog_owner.is_none() {
            let name = format!("app-native-{}", sinomo_runtime::core::Id::generate());
            let owner =
                sinomo_runtime::vfs::native_test_support::NativeMountedOwner::start_in_data_dir(
                    &name,
                    self.data_dir.as_ref().clone(),
                )?;
            self.node.node_id = owner.node_id().clone();
            self._native_catalog_owner = Some(Arc::new(owner));
        }
        self.catalog_read_vfs = self
            ._native_catalog_owner
            .as_ref()
            .expect("native catalog owner initialized above")
            .mounted_vfs()
            .map_err(|error| error.to_string());
        self.catalog_read_vfs
            .as_ref()
            .map(|_| ())
            .map_err(|error| error.clone())
    }

    #[cfg(test)]
    pub(crate) fn checkpoint_native_fixture_observations(&self) -> Result<usize, String> {
        self._native_catalog_owner
            .as_ref()
            .ok_or_else(|| "backend has no native catalog owner".to_string())?
            .checkpoint_rpc_observation_window()
    }

    /// Apply a live daemon observation and rebuild connection-bound clients as one backend update.
    ///
    /// App ownership is retained only when the probe reached the exact process instance this
    /// backend started and still owns. If a replacement appears at the same address, it is an
    /// external daemon from this app surface's perspective.
    pub(crate) fn apply_live_daemon_status(&mut self, status: DaemonStatus) -> bool {
        let binding_changed = !matches!(
            &self.connection,
            Connection::Daemon(previous) | Connection::AppOwnedDaemon(previous)
                if previous.pid == status.pid && previous.address == status.address
        );
        let remains_in_process = self
            ._daemon
            .as_ref()
            .is_some_and(|daemon| daemon.owns_status(&status))
            && matches!(
                &self.connection,
                Connection::AppOwnedDaemon(previous)
                    if previous.pid == status.pid && previous.address == status.address
            );
        self.connection = if remains_in_process {
            Connection::AppOwnedDaemon(Box::new(status))
        } else {
            Connection::Daemon(Box::new(status))
        };
        if binding_changed {
            self.config =
                build_config_client(&self.connection, &self.data_dir, self.app_runtime.handle());
            #[cfg(test)]
            {
                self.catalog_read_vfs = build_catalog_read_vfs(&self.connection);
            }
        }
        binding_changed
    }

    /// Whether the runtime is healthy enough to talk to.
    pub fn is_online(&self) -> bool {
        true
    }
}

/// Produces the idempotent role update used to migrate Android nodes that were
/// initialized before cluster init/join accepted a typed node role. Supplying
/// no requested role short-circuits before inspecting cluster state, keeping
/// non-Android app startup unchanged.
fn app_cluster_node_role_migration_request(
    status: &sinomo_client::AppClusterStatus,
    requested_role: Option<ClusterNodeRole>,
) -> Result<Option<ClusterControlRoleRequest>, String> {
    let Some(role) = requested_role else {
        return Ok(None);
    };
    if !status.clustered {
        return Ok(None);
    }
    let node_id = status
        .local_node_id
        .as_deref()
        .ok_or_else(|| "cluster status omitted the local node id".to_string())?
        .parse::<NodeId>()
        .map_err(|error| format!("cluster status has an invalid local node id: {error}"))?;
    Ok(Some(ClusterControlRoleRequest { node_id, role }))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MobileRoleMigrationFailureOutcome {
    RetryAfter(Duration),
    Exhausted,
    Unsupported,
    Permanent,
}

fn mobile_role_migration_failure_outcome(
    error: &DaemonApiError,
    failed_attempt_index: usize,
) -> MobileRoleMigrationFailureOutcome {
    if mobile_role_migration_route_is_unsupported(error) {
        return MobileRoleMigrationFailureOutcome::Unsupported;
    }
    if matches!(
        error,
        DaemonApiError::Network { address, .. } if address == CLUSTER_CONTROL_ROLE_ROUTE
    ) {
        // An RPC error response uses the request route as its address. It is a
        // semantic rejection, not a transport failure that backoff can heal.
        return MobileRoleMigrationFailureOutcome::Permanent;
    }

    let retryable = matches!(
        error,
        DaemonApiError::Network { .. }
            | DaemonApiError::NetworkDelivery { .. }
            | DaemonApiError::ForwardNetwork { .. }
            | DaemonApiError::NotRunning { .. }
            | DaemonApiError::ClusterOrphan
            | DaemonApiError::ClusterReadOnly { .. }
    );
    if !retryable {
        return MobileRoleMigrationFailureOutcome::Permanent;
    }

    MOBILE_ROLE_MIGRATION_RETRY_DELAYS
        .get(failed_attempt_index)
        .copied()
        .map(MobileRoleMigrationFailureOutcome::RetryAfter)
        .unwrap_or(MobileRoleMigrationFailureOutcome::Exhausted)
}

fn mobile_role_migration_route_is_unsupported(error: &DaemonApiError) -> bool {
    if let DaemonApiError::RemoteService { route, detail } = error {
        let detail = detail.to_ascii_lowercase();
        return route == CLUSTER_CONTROL_ROLE_ROUTE
            && ["unknown", "unsupported", "not registered", "no handler"]
                .iter()
                .any(|marker| detail.contains(marker));
    }
    let detail = match error {
        DaemonApiError::Parse { detail }
        | DaemonApiError::Network { detail, .. }
        | DaemonApiError::NetworkDelivery { detail, .. }
        | DaemonApiError::ForwardNetwork { detail, .. } => detail,
        _ => return false,
    };
    let detail = detail.to_ascii_lowercase();
    detail.contains(CLUSTER_CONTROL_ROLE_ROUTE)
        && ["unknown", "unsupported", "not registered", "no handler"]
            .iter()
            .any(|marker| detail.contains(marker))
}

fn wait_until_mobile_role_migration_is_interactive() {
    #[cfg(target_os = "android")]
    {
        let _ = crate::mobile_runtime::wait_until_interactive_blocking();
    }
}

fn wait_for_mobile_role_migration_retry(delay: Duration) {
    #[cfg(target_os = "android")]
    {
        let generation = crate::mobile_runtime::wait_until_interactive_blocking();
        crate::mobile_runtime::wait_for_change_or_timeout(generation, delay);
        // A network-change notification may wake the retry early. A transition
        // to the background may not: park until the Activity is interactive
        // again before the next cluster mutation.
        let _ = crate::mobile_runtime::wait_until_interactive_blocking();
    }
    #[cfg(not(target_os = "android"))]
    {
        thread::sleep(delay);
    }
}

/// Best-effort Android compatibility migration. New daemons normally persist
/// the mobile role during init/join, while older daemons may reject the role
/// route entirely; neither outcome is allowed to make app startup fail.
fn reconcile_android_cluster_node_role(connection: &Connection, _data_dir: &SinomoDataDir) {
    let requested_role = app_requested_cluster_node_role();
    if requested_role.is_none() {
        return;
    }
    let Some(address) = connection.daemon_address() else {
        return;
    };
    let status = match connection {
        Connection::Daemon(status) | Connection::AppOwnedDaemon(status) => {
            sinomo_client::AppClusterStatus::from_daemon_status(status)
        }
        Connection::LocalFallback { .. } => sinomo_client::AppClusterStatus::default(),
    };
    let request = match app_cluster_node_role_migration_request(&status, requested_role) {
        Ok(Some(request)) => request,
        Ok(None) => return,
        Err(error) => {
            tracing::warn!(%error, "sinomo-app could not prepare Android node-role migration");
            return;
        }
    };
    let address = address.to_string();
    if let Err(error) = thread::Builder::new()
        .name("sinomo-app-mobile-role-migration".to_string())
        .spawn(move || {
            let client = DaemonApiClient::at_address(address).with_timeout(DAEMON_RPC_TIMEOUT);
            let mut failed_attempt_index = 0;
            wait_until_mobile_role_migration_is_interactive();
            loop {
                match client.cluster_role(request.clone()) {
                    Ok(update) if update.changed => {
                        tracing::info!(
                            node_id = %update.node_id,
                            previous_role = ?update.previous_role,
                            "sinomo-app migrated the Android cluster node to the mobile role"
                        );
                        return;
                    }
                    Ok(_) => return,
                    Err(error) => match mobile_role_migration_failure_outcome(
                        &error,
                        failed_attempt_index,
                    ) {
                        MobileRoleMigrationFailureOutcome::RetryAfter(delay) => {
                            tracing::debug!(
                                %error,
                                retry_after_ms = delay.as_millis(),
                                "sinomo-app Android node-role migration will retry after a transient failure"
                            );
                            wait_for_mobile_role_migration_retry(delay);
                            failed_attempt_index = failed_attempt_index.saturating_add(1);
                        }
                        MobileRoleMigrationFailureOutcome::Unsupported => {
                            tracing::debug!(
                                %error,
                                "attached daemon does not support Android node-role migration"
                            );
                            return;
                        }
                        MobileRoleMigrationFailureOutcome::Exhausted => {
                            tracing::debug!(
                                %error,
                                attempts = failed_attempt_index + 1,
                                "sinomo-app exhausted this launch's Android node-role migration retries"
                            );
                            return;
                        }
                        MobileRoleMigrationFailureOutcome::Permanent => {
                            tracing::warn!(
                                %error,
                                "sinomo-app could not migrate the Android cluster node role; startup will continue"
                            );
                            return;
                        }
                    },
                }
            }
        })
    {
        tracing::warn!(
            %error,
            "sinomo-app could not start the Android node-role migration; startup will continue"
        );
    }
}

/// Probe the live daemon endpoint without consulting or mutating application UI state.
///
/// The preferred address is tried first so an attached daemon remains observable even if its
/// status file is briefly stale. The data-directory probe is the recovery path: it can discover a
/// replacement daemon after the endpoint used at startup disappears. This function is blocking by
/// design and must only be called from the app's background connection-operation lane.
pub(crate) fn probe_runtime_connection(
    data_dir: &SinomoDataDir,
    preferred_address: Option<&str>,
) -> Result<DaemonStatus, String> {
    if let Some(address) = preferred_address
        && let Some(status) = probe_daemon_status_at_address(address, DAEMON_PROBE_TIMEOUT)
    {
        return Ok(status);
    }

    let paths = DaemonPaths::new(data_dir);
    match probe_daemon_status(&paths, DAEMON_PROBE_TIMEOUT) {
        DaemonProbeStatus::Running(status) => Ok(status),
        DaemonProbeStatus::Locked {
            status: Some(status),
        } => Err(format!(
            "daemon at {} is not responding; its process lock is still present",
            status.address
        )),
        DaemonProbeStatus::Locked { status: None } => {
            Err("daemon is not responding; its process lock is still present".to_string())
        }
        DaemonProbeStatus::NotRunning => Err("no daemon is currently running".to_string()),
    }
}

/// Run `work` on a detached named thread and wait up to `timeout` for its result.
///
/// On timeout the worker keeps running in the background and its late result is
/// discarded; the caller gets an `Err` describing the timeout. This bounds
/// SIFS/DFS-backed loads so an unhealthy distributed file system degrades the
/// affected surface instead of wedging it (the app must stay usable — cluster
/// status and settings in particular — while DFS is down).
pub(crate) fn run_with_timeout<T: Send + 'static>(
    label: &'static str,
    timeout: Duration,
    work: impl FnOnce() -> T + Send + 'static,
) -> Result<T, String> {
    let (tx, rx) = mpsc::channel();
    thread::Builder::new()
        .name(format!("sinomo-app-timed-{label}"))
        .spawn(move || {
            let _ = tx.send(work());
        })
        .map_err(|error| format!("{label}: failed to spawn worker: {error}"))?;
    match rx.recv_timeout(timeout) {
        Ok(value) => Ok(value),
        Err(mpsc::RecvTimeoutError::Timeout) => Err(format!(
            "{label} timed out after {}s; the distributed file system (SIFS) may be unavailable",
            timeout.as_secs()
        )),
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            Err(format!("{label}: worker exited before returning a result"))
        }
    }
}

fn local_fallback_data_dir() -> (SinomoDataDir, Arc<LocalFallbackDataDirCleanup>) {
    let sequence = LOCAL_FALLBACK_DATA_DIR_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "sinomo-app-local-fallback-test-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
        sequence,
    ));
    let _ = std::fs::remove_dir_all(&path);
    let cleanup = Arc::new(LocalFallbackDataDirCleanup { path: path.clone() });
    (SinomoDataDir::new(path), cleanup)
}

struct LocalFallbackDataDirCleanup {
    path: PathBuf,
}

impl Drop for LocalFallbackDataDirCleanup {
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
) -> (Connection, bool) {
    if !connection.is_daemon() || connection.is_clustered() {
        return (connection, false);
    }

    progress("Initializing single-node cluster...");
    match ensure_app_cluster_through_daemon(&connection, data_dir, node) {
        Ok(initialized) if initialized && matches!(&connection, Connection::AppOwnedDaemon(_)) => {
            // `/cluster/control/init` drains the serving daemon because its foundational services
            // captured the pre-cluster identity. An app-owned daemon has no detached shadow
            // supervisor, so the app must replace it before exposing the backend as ready.
            (connection, true)
        }
        Ok(_) => (
            match probe_daemon_status(paths, DAEMON_RPC_TIMEOUT) {
                DaemonProbeStatus::Running(status)
                | DaemonProbeStatus::Locked {
                    status: Some(status),
                } => match connection {
                    Connection::Daemon(_) => Connection::Daemon(Box::new(status)),
                    Connection::AppOwnedDaemon(_) => Connection::AppOwnedDaemon(Box::new(status)),
                    Connection::LocalFallback { reason } => Connection::LocalFallback { reason },
                },
                DaemonProbeStatus::Locked { status: None } | DaemonProbeStatus::NotRunning => {
                    connection
                }
            },
            false,
        ),
        Err(error) => {
            tracing::warn!(%error, "sinomo-app could not initialize local cluster");
            (connection, false)
        }
    }
}

/// Daemon-first cluster init for a fresh app node: the attached daemon
/// initializes its own cluster through `/cluster/control/init`. Skipped when
/// cluster state already exists (the daemon's status simply lagged).
fn ensure_app_cluster_through_daemon(
    connection: &Connection,
    _data_dir: &SinomoDataDir,
    node: &NodeProfile,
) -> Result<bool, String> {
    let address = connection
        .daemon_address()
        .ok_or_else(|| "no daemon connection for cluster init".to_string())?;
    let name = node.display_name.trim();
    let name = if name.is_empty() { "Sinomo App" } else { name };
    DaemonApiClient::at_address(address)
        .cluster_init(ClusterControlInitRequest {
            cluster_name: name.to_string(),
            node_display_name: Some(name.to_string()),
            node_role: app_requested_cluster_node_role(),
        })
        .map(|_| true)
        .map_err(|error| error.to_string())
}

fn restart_owned_daemon_after_cluster_init(
    paths: &DaemonPaths,
    progress: &impl Fn(&str),
    agent_runtime_control: AppAgentRuntimeControl,
    node_resources: sinomo_runtime::device::NodeResourceMonitor,
    daemon: &mut Option<OwnedDaemonHandle>,
) -> Result<(DaemonStatus, bool), String> {
    let current = daemon.as_ref().ok_or_else(|| {
        "app-owned daemon ownership was lost during cluster initialization".to_string()
    })?;
    wait_for_owned_daemon_exit(current, IN_PROCESS_DAEMON_STOP_TIMEOUT)?;
    daemon.take();

    let (status, replacement) =
        start_local_daemon(paths, progress, agent_runtime_control, node_resources)?;
    let owned = replacement.is_some();
    *daemon = replacement;
    if status.cluster.is_none() {
        return Err(
            "replacement app-owned daemon started without the initialized cluster identity"
                .to_string(),
        );
    }
    Ok((status, owned))
}

#[cfg(not(target_os = "android"))]
fn wait_for_owned_daemon_exit(daemon: &OwnedDaemonHandle, timeout: Duration) -> Result<(), String> {
    daemon.wait_for_exit(timeout)
}

#[cfg(target_os = "android")]
fn wait_for_owned_daemon_exit(daemon: &OwnedDaemonHandle, timeout: Duration) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    while !daemon._thread.is_finished() {
        if Instant::now() >= deadline {
            return Err(format!(
                "embedded daemon did not stop within {} seconds after cluster initialization",
                timeout.as_secs()
            ));
        }
        thread::sleep(Duration::from_millis(25));
    }
    Ok(())
}

#[cfg(not(target_os = "android"))]
fn connect_local_daemon(
    data_dir: &SinomoDataDir,
    _paths: &DaemonPaths,
    progress: &impl Fn(&str),
    _agent_runtime_control: AppAgentRuntimeControl,
    _node_resources: sinomo_runtime::device::NodeResourceMonitor,
) -> Result<(DaemonStatus, Option<OwnedDaemonHandle>), String> {
    if std::env::var_os(sinomo_client::api::app::update::APP_SUPERVISED_ENV).is_some() {
        return sinomo_client::connect_to_supervised_daemon(data_dir, progress)
            .map(|status| (status, None));
    }
    sinomo_client::connect_or_start_local_daemon(data_dir, progress)
        .map(sinomo_client::LocalDaemonConnection::into_parts)
}

#[cfg(target_os = "android")]
fn connect_local_daemon(
    _data_dir: &SinomoDataDir,
    paths: &DaemonPaths,
    progress: &impl Fn(&str),
    agent_runtime_control: AppAgentRuntimeControl,
    node_resources: sinomo_runtime::device::NodeResourceMonitor,
) -> Result<(DaemonStatus, Option<OwnedDaemonHandle>), String> {
    progress("Looking for a daemon…");
    match probe_daemon_status(paths, DAEMON_PROBE_TIMEOUT) {
        DaemonProbeStatus::Running(status)
        | DaemonProbeStatus::Locked {
            status: Some(status),
        } => {
            if sinomo_client::daemon_protocol_is_compatible(&status) {
                Ok((status, None))
            } else {
                Err(sinomo_client::incompatible_daemon_protocol_detail(&status))
            }
        }
        DaemonProbeStatus::Locked { status: None } | DaemonProbeStatus::NotRunning => {
            progress("Starting a local daemon…");
            start_local_daemon(paths, progress, agent_runtime_control, node_resources)
        }
    }
}

#[cfg(not(target_os = "android"))]
fn start_local_daemon(
    paths: &DaemonPaths,
    progress: &impl Fn(&str),
    _agent_runtime_control: AppAgentRuntimeControl,
    _node_resources: sinomo_runtime::device::NodeResourceMonitor,
) -> Result<(DaemonStatus, Option<OwnedDaemonHandle>), String> {
    match sinomo_client::start_app_owned_daemon(paths, progress)? {
        sinomo_client::StartAppDaemonOutcome::Started(status, daemon) => Ok((status, Some(daemon))),
        sinomo_client::StartAppDaemonOutcome::Attached(status) => {
            if sinomo_client::daemon_protocol_is_compatible(&status) {
                Ok((status, None))
            } else {
                Err(sinomo_client::incompatible_daemon_protocol_detail(&status))
            }
        }
    }
}

#[cfg(target_os = "android")]
fn start_local_daemon(
    paths: &DaemonPaths,
    progress: &impl Fn(&str),
    agent_runtime_control: AppAgentRuntimeControl,
    node_resources: sinomo_runtime::device::NodeResourceMonitor,
) -> Result<(DaemonStatus, Option<OwnedDaemonHandle>), String> {
    start_in_process_daemon(paths, progress, agent_runtime_control, node_resources)
        .map(|(status, daemon)| (status, Some(daemon)))
}

/// Build the [`ConfigClient`] for the current connection. Normally this is an RPC client to the
/// attached daemon (external or app-owned). Only in the degraded
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
/// An error leaves the app on the daemonless fallback with a concrete reason to show in the UI.
#[cfg(target_os = "android")]
fn start_in_process_daemon(
    paths: &DaemonPaths,
    progress: &impl Fn(&str),
    agent_runtime_control: AppAgentRuntimeControl,
    node_resources: sinomo_runtime::device::NodeResourceMonitor,
) -> Result<(DaemonStatus, OwnedDaemonHandle), String> {
    let listener = prepare_daemon_control_listener(DaemonRunOptions::default())
        .map_err(|error| error.to_string())?;
    let options = listener.options().clone();
    let daemon_address = format!("{}:{}", options.host, options.port);
    let execution_profile = embedded_daemon_execution_profile();
    let cluster = in_process_daemon_cluster_status(paths)?;
    let loop_paths = paths.clone();
    let _ = agent_runtime_control;
    let (exit_tx, exit_rx) = mpsc::channel::<String>();
    let thread = thread::Builder::new()
        .name("sinomo-app-connection".to_string())
        .spawn(move || {
            let data_dir = loop_paths
                .root
                .parent()
                .map(SinomoDataDir::new)
                .expect("embedded daemon paths must have a data directory parent");
            let result = (|| {
                let product_profile = embedded_product_component_profile(cluster.is_some());
                let base_catalog =
                    sinomo_product_profile::base_component_catalog(&data_dir, product_profile)
                        .map_err(|error| {
                            format!("build embedded product component profile: {error}")
                        })?;
                let hooks = vec![
                    sinomo_cli::hosted_agent_chat_component_factory(daemon_address)
                        .map_err(|error| format!("build embedded chat component: {error}"))?,
                ];
                #[cfg(not(target_os = "android"))]
                let hooks = {
                    let mut hooks = hooks;
                    if product_profile.hosts_native_filesystem_projection() {
                        hooks.push(sinomo_cli::sifs_mount_supervisor_component_factory(
                            data_dir,
                            loop_paths.clone(),
                        ));
                    }
                    hooks
                };
                let component_catalog =
                    sinomo_product_profile::with_components(&base_catalog, hooks).map_err(
                        |error| format!("extend embedded product component profile: {error}"),
                    )?;
                run_daemon_control_loop_with_prepared_listener(
                    listener,
                    loop_paths,
                    "daemon",
                    cluster,
                    component_catalog,
                    DaemonRuntimeConfig::new(execution_profile).with_node_resources(node_resources),
                )
                .map_err(|error| error.to_string())
            })();
            if let Err(error) = result {
                let detail = error.to_string();
                tracing::warn!(%error, "in-process daemon exited");
                let _ = exit_tx.send(detail);
            } else {
                let _ = exit_tx.send("in-process daemon exited before it became ready".to_string());
            }
        })
        .map_err(|error| format!("failed to spawn embedded daemon thread: {error}"))?;

    progress("Waiting for the daemon to come up…");
    let deadline = Instant::now() + IN_PROCESS_DAEMON_READY_TIMEOUT;
    loop {
        match exit_rx.try_recv() {
            Ok(detail) => return Err(format!("embedded daemon exited before ready: {detail}")),
            Err(mpsc::TryRecvError::Disconnected) if thread.is_finished() => {
                return Err("embedded daemon exited before ready".to_string());
            }
            Err(mpsc::TryRecvError::Disconnected) | Err(mpsc::TryRecvError::Empty) => {}
        }
        if let DaemonProbeStatus::Running(status) =
            probe_daemon_status(paths, Duration::from_millis(300))
        {
            return Ok((status, OwnedDaemonHandle { _thread: thread }));
        }
        if Instant::now() >= deadline {
            tracing::warn!("in-process daemon did not become ready within the timeout");
            return Err(format!(
                "embedded daemon did not become ready within {} seconds",
                IN_PROCESS_DAEMON_READY_TIMEOUT.as_secs()
            ));
        }
        thread::sleep(Duration::from_millis(200));
    }
}

#[cfg(target_os = "android")]
fn in_process_daemon_cluster_status(
    paths: &DaemonPaths,
) -> Result<Option<DaemonClusterStatus>, String> {
    let data_dir = paths.root.parent().ok_or_else(|| {
        format!(
            "embedded daemon path {} has no data directory parent",
            paths.root.display()
        )
    })?;
    let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
    if !cluster_paths.state.exists() {
        return Ok(None);
    }

    let state = sinomo_runtime::cluster::read_cluster_state_without_kv(&cluster_paths)
        .map_err(|error| format!("failed to read embedded daemon cluster state: {error}"))?;
    daemon_cluster_status(&state, DaemonClusterProcess::Main)
        .map(Some)
        .ok_or_else(|| {
            format!(
                "embedded daemon cluster state at {} has no local member",
                cluster_paths.state.display()
            )
        })
}

fn short_id(id: &str) -> String {
    let trimmed = id.trim();
    if trimmed.len() <= 10 {
        trimmed.to_string()
    } else {
        format!("{}…", &trimmed[..10])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(target_os = "android")]
    use sinomo_runtime::core::ClusterBootstrapState;
    use sinomo_runtime::core::Id;
    use sinomo_runtime::node::NodeRole;
    #[cfg(target_os = "android")]
    use time::OffsetDateTime;

    #[cfg(not(target_os = "android"))]
    #[test]
    fn desktop_app_does_not_request_mobile_cluster_role() {
        assert_eq!(app_requested_cluster_node_role(), None);
    }

    #[test]
    fn cluster_node_role_migration_builds_mobile_update_for_clustered_node() {
        let node_id = Id::generate();
        let status = sinomo_client::AppClusterStatus {
            clustered: true,
            local_node_id: Some(node_id.to_string()),
            ..Default::default()
        };

        assert_eq!(
            app_cluster_node_role_migration_request(&status, Some(ClusterNodeRole::Mobile))
                .expect("migration request"),
            Some(ClusterControlRoleRequest {
                node_id,
                role: ClusterNodeRole::Mobile,
            })
        );
    }

    #[test]
    fn cluster_node_role_migration_is_noop_without_android_role_or_cluster() {
        let malformed_clustered_status = sinomo_client::AppClusterStatus {
            clustered: true,
            local_node_id: None,
            ..Default::default()
        };
        assert_eq!(
            app_cluster_node_role_migration_request(&malformed_clustered_status, None)
                .expect("desktop migration is disabled"),
            None,
            "desktop startup must not inspect or mutate cluster role state"
        );

        let unclustered_status = sinomo_client::AppClusterStatus::default();
        assert_eq!(
            app_cluster_node_role_migration_request(
                &unclustered_status,
                Some(ClusterNodeRole::Mobile),
            )
            .expect("unclustered migration is skipped"),
            None
        );
    }

    #[test]
    fn mobile_role_migration_transient_backoff_is_sparse_and_bounded() {
        let error = DaemonApiError::NotRunning {
            detail: "daemon is restarting".to_string(),
        };
        assert_eq!(
            (0..=MOBILE_ROLE_MIGRATION_RETRY_DELAYS.len() + 1)
                .map(|attempt| mobile_role_migration_failure_outcome(&error, attempt))
                .collect::<Vec<_>>(),
            vec![
                MobileRoleMigrationFailureOutcome::RetryAfter(Duration::from_secs(2)),
                MobileRoleMigrationFailureOutcome::RetryAfter(Duration::from_secs(10)),
                MobileRoleMigrationFailureOutcome::RetryAfter(Duration::from_secs(30)),
                MobileRoleMigrationFailureOutcome::RetryAfter(Duration::from_secs(2 * 60)),
                MobileRoleMigrationFailureOutcome::RetryAfter(Duration::from_secs(5 * 60)),
                MobileRoleMigrationFailureOutcome::Exhausted,
                MobileRoleMigrationFailureOutcome::Exhausted,
            ]
        );
    }

    #[test]
    fn mobile_role_migration_stops_for_unsupported_or_permanent_failures() {
        let unsupported_service = DaemonApiError::RemoteService {
            route: CLUSTER_CONTROL_ROLE_ROUTE.to_string(),
            detail: format!("unknown object rpc route: {CLUSTER_CONTROL_ROLE_ROUTE}"),
        };
        assert_eq!(
            mobile_role_migration_failure_outcome(&unsupported_service, 0),
            MobileRoleMigrationFailureOutcome::Unsupported
        );
        let unsupported = DaemonApiError::Network {
            address: CLUSTER_CONTROL_ROLE_ROUTE.to_string(),
            detail: format!("unknown object rpc route: {CLUSTER_CONTROL_ROLE_ROUTE}"),
        };
        assert_eq!(
            mobile_role_migration_failure_outcome(&unsupported, 0),
            MobileRoleMigrationFailureOutcome::Unsupported
        );
        let legacy_parse_error = DaemonApiError::Parse {
            detail: format!("unknown service route: {CLUSTER_CONTROL_ROLE_ROUTE}"),
        };
        assert_eq!(
            mobile_role_migration_failure_outcome(&legacy_parse_error, 0),
            MobileRoleMigrationFailureOutcome::Unsupported
        );

        assert_eq!(
            mobile_role_migration_failure_outcome(&DaemonApiError::AuthDenied, 0),
            MobileRoleMigrationFailureOutcome::Permanent
        );
    }

    #[test]
    fn run_with_timeout_returns_result_within_budget() {
        let value = run_with_timeout("fast-load", Duration::from_secs(10), || 42)
            .expect("fast work completes");
        assert_eq!(value, 42);
    }

    #[test]
    fn run_with_timeout_reports_timeout_and_discards_late_result() {
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let error = run_with_timeout("wedged-load", Duration::from_millis(50), move || {
            // Simulate a SIFS load stuck behind distributed timeouts until released.
            let _ = release_rx.recv();
            "late result"
        })
        .expect_err("wedged work should time out");

        assert!(
            error.contains("wedged-load timed out after 0s"),
            "unexpected timeout message: {error}"
        );
        assert!(
            error.contains("SIFS"),
            "timeout message should point at the distributed file system: {error}"
        );
        // Unblock the detached worker; its late result must be discarded harmlessly.
        let _ = release_tx.send(());
    }

    #[test]
    fn daemon_protocol_compatibility_is_explicit_not_package_version_based() {
        let (data_dir, _cleanup) = local_fallback_data_dir();
        let mut status = DaemonStatus::new(
            &DaemonRunOptions::default(),
            &DaemonPaths::new(&data_dir),
            None,
        );
        status.version = "999.0.0".to_string();
        assert!(sinomo_client::daemon_protocol_is_compatible(&status));

        status.api_protocol_version = 0;
        assert!(!sinomo_client::daemon_protocol_is_compatible(&status));
        assert!(
            sinomo_client::incompatible_daemon_protocol_detail(&status)
                .contains("daemon protocol 0")
        );
    }

    #[cfg(target_os = "android")]
    #[test]
    fn embedded_daemon_restart_wait_observes_completed_thread() {
        let daemon = OwnedDaemonHandle {
            _thread: thread::spawn(|| {}),
        };

        wait_for_owned_daemon_exit(&daemon, Duration::from_secs(1))
            .expect("completed embedded daemon should be ready to replace");
    }

    #[cfg(target_os = "android")]
    #[test]
    fn embedded_daemon_restart_wait_times_out_without_losing_thread_ownership() {
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let daemon = OwnedDaemonHandle {
            _thread: thread::spawn(move || {
                let _ = release_rx.recv();
            }),
        };

        let error = wait_for_owned_daemon_exit(&daemon, Duration::from_millis(25))
            .expect_err("a running embedded daemon must not be replaced on the same address");
        assert!(
            error.contains("did not stop"),
            "unexpected restart wait error: {error}"
        );
        assert!(
            !daemon._thread.is_finished(),
            "timing out must retain ownership of the running daemon thread"
        );

        let _ = release_tx.send(());
        wait_for_owned_daemon_exit(&daemon, Duration::from_secs(1))
            .expect("released daemon thread should finish");
    }

    #[cfg(target_os = "android")]
    #[test]
    fn embedded_daemon_startup_has_no_cluster_for_fresh_data_dir() {
        let (data_dir, _cleanup) = local_fallback_data_dir();
        data_dir
            .ensure_root_layout()
            .expect("create data directory");

        let cluster = in_process_daemon_cluster_status(&DaemonPaths::new(&data_dir))
            .expect("fresh data directory should be readable");

        assert!(cluster.is_none());
    }

    #[cfg(target_os = "android")]
    #[test]
    fn embedded_daemon_startup_loads_initialized_cluster_identity() {
        let (data_dir, _cleanup) = local_fallback_data_dir();
        data_dir
            .ensure_root_layout()
            .expect("create data directory");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let cluster_id = Id::generate();
        let node_id = Id::generate();
        let state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "App cluster",
            node_id.clone(),
            "App node",
            "test-public-key",
            OffsetDateTime::now_utc(),
        );
        sinomo_runtime::cluster::write_cluster_state(&cluster_paths, &state)
            .expect("write cluster state");

        let cluster = in_process_daemon_cluster_status(&DaemonPaths::new(&data_dir))
            .expect("initialized cluster state should be readable")
            .expect("initialized data directory should produce daemon cluster status");

        assert_eq!(cluster.cluster_id, cluster_id);
        assert_eq!(cluster.node_id, node_id);
        assert_eq!(cluster.process, "daemon-main");
    }

    #[test]
    fn test_local_fallback_uses_temp_data_dir_and_cleans_up() {
        // Exercise normal cleanup in one child without changing sibling tests' retention.
        if std::env::var("SINOMO_TEST_KEEP_TMP")
            .is_ok_and(|value| !value.is_empty() && value != "0")
        {
            let evidence = sinomo_testdir::TestDir::new("app-fallback-cleanup-child");
            let output = std::process::Command::new(
                std::env::current_exe().expect("current app test executable"),
            )
            .args([
                "--exact",
                "backend::tests::test_local_fallback_uses_temp_data_dir_and_cleans_up",
                "--nocapture",
            ])
            .env("SINOMO_TEST_KEEP_TMP", "0")
            .output()
            .expect("run cleanup oracle without retaining successful child fixtures");
            std::fs::write(evidence.join("child.stdout"), &output.stdout)
                .expect("retain cleanup child stdout");
            std::fs::write(evidence.join("child.stderr"), &output.stderr)
                .expect("retain cleanup child stderr");
            assert!(
                output.status.success(),
                "cleanup child failed: {}; raw evidence: {}",
                output.status,
                evidence.display(),
            );
            assert!(
                String::from_utf8_lossy(&output.stdout)
                    .lines()
                    .any(|line| line.starts_with("test result: ok. 1 passed; 0 failed; 0 ignored;")),
                "cleanup child must run exactly one test; raw evidence: {}",
                evidence.display(),
            );
            return;
        }
        let default_root = app_config::default_data_dir().root();
        let backend = Backend::local_fallback();
        let root = backend.data_dir.root();
        let second_backend = Backend::local_fallback();
        let second_root = second_backend.data_dir.root();
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
            root.starts_with(std::env::temp_dir()),
            "unit-test fallback data dir should live under the test temporary root: {root:?}"
        );
        assert_ne!(
            root, second_root,
            "concurrent fallback backends must never share a data directory"
        );
        assert!(root.exists(), "fallback test data dir should be created");

        drop(backend);

        assert!(
            !root.exists(),
            "fallback test data dir should be removed when the backend drops"
        );
        assert!(
            second_root.exists(),
            "dropping one backend must not remove another backend's data directory"
        );
    }

    #[test]
    fn live_daemon_status_rebinds_config_and_classifies_process_ownership() {
        let mut backend = Backend::local_fallback();
        let paths = DaemonPaths::new(backend.data_dir.as_ref());
        let status = DaemonStatus::new(&DaemonRunOptions::default(), &paths, None);
        let old_config = backend.config.clone();

        assert!(backend.apply_live_daemon_status(status.clone()));

        assert!(matches!(backend.connection, Connection::Daemon(_)));
        assert!(
            !Arc::ptr_eq(&old_config, &backend.config),
            "connection-bound config client must be rebuilt after recovery"
        );
        assert_eq!(
            backend.connection.daemon_address(),
            Some(status.address.as_str())
        );

        let rebound_config = backend.config.clone();
        assert!(!backend.apply_live_daemon_status(status.clone()));
        assert!(
            Arc::ptr_eq(&rebound_config, &backend.config),
            "a healthy same-process probe must not churn connection-bound clients"
        );

        #[cfg(target_os = "android")]
        {
            backend.connection = Connection::AppOwnedDaemon(Box::new(status.clone()));
            backend._daemon = Some(Arc::new(OwnedDaemonHandle {
                _thread: thread::spawn(|| {}),
            }));
            assert!(!backend.apply_live_daemon_status(status.clone()));
            assert!(
                matches!(backend.connection, Connection::AppOwnedDaemon(_)),
                "the exact daemon process still owned by this backend remains embedded"
            );
        }

        let mut replacement = status;
        replacement.pid = replacement.pid.wrapping_add(1);
        assert!(backend.apply_live_daemon_status(replacement));
        assert!(
            matches!(backend.connection, Connection::Daemon(_)),
            "a replacement process at the same address is external to this app surface"
        );
    }
}
