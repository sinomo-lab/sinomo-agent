use crate::cluster::{
    ClusterNetworkMaintenance, DAEMON_CLUSTER_PUBLISH_INTERVAL, DAEMON_CLUSTER_TICK_INTERVAL,
    DaemonDesiredRolloutReactor, cluster_paths_from_daemon_paths,
    handle_daemon_cluster_kv_snapshot, prune_local_cluster_raft_uncommitted_tail,
};
use crate::dfs::{
    DaemonDfsMaintenanceTickOutcome, DaemonDfsPendingMaintenanceWork,
    daemon_dfs_maintenance_notify, daemon_dfs_node_transport, daemon_dfs_node_transport_on_runtime,
    daemon_dfs_now_unix_nanos, daemon_dfs_pending_reactive_maintenance_work_on_runtime,
    daemon_dfs_repair_queue_work_is_known, daemon_dfs_storage_environment_dirty_generation,
    daemon_dfs_storage_environment_notify, evict_daemon_dfs_message_service_cache_on_shutdown,
    notify_daemon_dfs_maintenance_work,
    notify_daemon_dfs_metadata_publish_route_generation_on_runtime,
    register_daemon_dfs_data_owner_lock, run_daemon_dfs_maintenance_tick_in_blocking_task,
    subscribe_daemon_dfs_remote_placement_observation,
};
use crate::{
    AuthenticatedDaemonCommand, DAEMON_SANDBOX_LIFECYCLE_RECOVERY_STATE_DIR, DaemonClusterStatus,
    DaemonDataOwnerLock, DaemonError, DaemonLock, DaemonPaths, DaemonSignerAuthority,
    daemon_paths_are_shadow, handle_daemon_finalize_shadow_rollout, read_daemon_status,
};
use sinomo_core::ClusterModuleRolloutStatusRecord;
use sinomo_core::cluster::{
    ClusterConnectivityProjectionChange as DaemonConnectivityProjectionChange,
    ClusterConnectivityTopologyProjection as DaemonConnectivityTopologyProjection,
    ClusterKvPathsExt,
};
#[cfg(test)]
use sinomo_core::cluster::{
    ClusterConnectivityTopologyValue as DaemonConnectivityTopologyValue,
    cluster_connectivity_events_cover_revision_range as daemon_connectivity_events_cover_revision_range,
};
use sinomo_core::events::EventStore;
use sinomo_core::node::NodeId;
#[cfg(test)]
use sinomo_core::{ClusterMemberRecord, ClusterTransportKind};
#[cfg(test)]
use sinomo_dfs::{
    DFS_DEFERRED_WORK_REPROBE_INTERVAL as DAEMON_DFS_DEFERRED_WORK_REPROBE_INTERVAL,
    DFS_MAINTENANCE_MEMBER_PHASE_SLOT as DAEMON_DFS_MAINTENANCE_MEMBER_PHASE_SLOT,
    DFS_MAINTENANCE_MEMBER_PHASE_WINDOW as DAEMON_DFS_MAINTENANCE_MEMBER_PHASE_WINDOW,
    DFS_MAINTENANCE_NETWORK_STARTUP_GRACE as DAEMON_DFS_MAINTENANCE_NETWORK_STARTUP_GRACE,
    DFS_MAINTENANCE_PROBE_ERROR_RETRY_INTERVAL as DAEMON_DFS_MAINTENANCE_PROBE_ERROR_RETRY_INTERVAL,
    dfs_maintenance_revalidation_decision as daemon_dfs_maintenance_revalidation_decision,
};
use sinomo_dfs::{
    DFS_MAINTENANCE_LEASE_RETRY_DELAY as DAEMON_DFS_MAINTENANCE_LEASE_RETRY_DELAY,
    DFS_TRIGGERED_MAINTENANCE_MIN_INTERVAL as DAEMON_DFS_TRIGGERED_MAINTENANCE_MIN_INTERVAL,
    dfs_failed_maintenance_decision as daemon_dfs_failed_maintenance_decision,
    dfs_initial_maintenance_delay as daemon_dfs_initial_maintenance_delay,
    dfs_maintenance_completion_decision as daemon_dfs_maintenance_completion_decision,
    dfs_maintenance_due_in as daemon_dfs_maintenance_due_in,
    dfs_maintenance_member_phase as daemon_dfs_maintenance_member_phase,
    dfs_maintenance_notification_decision as daemon_dfs_maintenance_notification_decision,
    dfs_maintenance_probe_decision as daemon_dfs_maintenance_probe_decision,
    dfs_maintenance_probe_is_due as daemon_dfs_maintenance_probe_is_due,
    dfs_maintenance_should_launch as daemon_dfs_maintenance_should_launch,
};
use sinomo_rollout::{
    RolloutController, RolloutTelemetryAdvanceOptions, RolloutTelemetryAdvanceStatus,
    SelfRolloutPaths, read_rollout_status, rollout_main_activation_path,
    rollout_shadow_finalize_path,
};
use std::collections::BTreeSet;
use std::future::Future;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicU8, AtomicU64, Ordering},
};
#[cfg(test)]
use std::thread;
use std::time::{Duration, Instant};
use time::OffsetDateTime;
use tokio::io::AsyncWriteExt as _;

// Durable work that is known but currently cannot run (for example, a repair
// whose target is offline) retains a bounded retry cadence. Quiet nodes park:
// an O(total-state) inventory is not an acceptable liveness probe.
const DAEMON_DFS_MAINTENANCE_TASK_COMPLETION_POLL_INTERVAL: Duration = Duration::from_secs(1);
const DAEMON_DFS_STORAGE_DIRTY_SETTLE_DELAY: Duration = Duration::from_secs(2);
const DAEMON_DFS_STORAGE_REFRESH_ERROR_RETRY_DELAY: Duration = Duration::from_secs(30);
// Full DFS passes own the process's sole backend-bearing service lane. Stagger
// their first acquisition by stable membership rank so a rolling restart does
// not make every node hold its local lane while asking a peer to enter theirs.
// Reserve startup for liveness publication and one live doctor/readiness pass.
// Rank-zero DFS work is intentionally not allowed to consume that same window.
pub const DAEMON_CLUSTER_NETWORK_MAINTENANCE_PHASE_TIMEOUT: Duration = Duration::from_secs(10);
pub const DAEMON_SHADOW_ROLLOUT_IDLE_FINALIZATION_INTERVAL: Duration =
    DAEMON_CLUSTER_PUBLISH_INTERVAL;
pub const DAEMON_NETWORK_ENVIRONMENT_WATCH_ENV: &str = "SINOMO_DAEMON_NETWORK_ENVIRONMENT_WATCH";
const DAEMON_RUNTIME_TASK_JOIN_TIMEOUT: Duration = Duration::from_secs(3);
const DAEMON_NETWORK_MODULE_CONTEXT_TASK_JOIN_TIMEOUT: Duration = Duration::from_secs(30);
const DAEMON_RUNTIME_TASK_DRAIN_GRACE: Duration = Duration::from_millis(100);
const DAEMON_RUNTIME_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);
const DAEMON_CONNECTIVITY_EXTERNAL_SNAPSHOT_DEBOUNCE: Duration = Duration::from_millis(100);
const DAEMON_SANDBOX_STREAM_CHANNEL_CAPACITY: usize = 64;
const DAEMON_SANDBOX_SESSION_BODY_BUFFER_BYTES: usize = 256 * 1024;
#[cfg(feature = "sandbox")]
const DAEMON_SANDBOX_LIFECYCLE_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(30);

static DAEMON_CLUSTER_MAINTENANCE_NOTIFY: OnceLock<Arc<tokio::sync::Notify>> = OnceLock::new();
static DAEMON_CLUSTER_MAINTENANCE_DIRTY: AtomicU8 = AtomicU8::new(0);

const DAEMON_CLUSTER_MAINTENANCE_DIRTY_PUBLISH: u8 = 1 << 0;
const DAEMON_CLUSTER_MAINTENANCE_DIRTY_CONNECTIVITY: u8 = 1 << 1;
const DAEMON_CLUSTER_MAINTENANCE_DIRTY_DISCOVERY: u8 = 1 << 2;
const DAEMON_CLUSTER_MAINTENANCE_DIRTY_NETWORK_ENVIRONMENT: u8 = 1 << 3;

/// Identifies the durable input that invalidated a cluster-maintenance
/// projection. Callers publish intent instead of resetting the whole
/// scheduler: the single maintenance owner then advances only the affected
/// generation(s), preserving unrelated cadence and failure backoff.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DaemonClusterMaintenanceReason {
    /// Wake identity-fenced workers after init/join/leave without declaring
    /// any projection stale. The replacement process owns the new identity.
    LifecycleWake,
    /// A local interface/address generation changed.
    NetworkEnvironmentChanged,
    /// An accepted join changed membership and graph topology.
    JoinAccepted,
    /// A committed member/endpoint projection changed the set of direct probe
    /// targets. The KV observer owns this wake for every node, including peers
    /// that did not execute the join request locally.
    CommittedConnectivityTopologyChanged,
    /// A committed member display-name generation changed.
    Rename,
    /// A committed role generation changed scheduler and member projections.
    RoleChanged,
    /// Explicit operator request to reconcile every network projection.
    ManualBroad,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DaemonClusterMaintenanceRequestPlan {
    dirty: u8,
}

const fn daemon_cluster_maintenance_request_plan(
    reason: DaemonClusterMaintenanceReason,
) -> DaemonClusterMaintenanceRequestPlan {
    use DaemonClusterMaintenanceReason as Reason;

    match reason {
        Reason::LifecycleWake => DaemonClusterMaintenanceRequestPlan { dirty: 0 },
        Reason::NetworkEnvironmentChanged => DaemonClusterMaintenanceRequestPlan {
            dirty: DAEMON_CLUSTER_MAINTENANCE_DIRTY_CONNECTIVITY
                | DAEMON_CLUSTER_MAINTENANCE_DIRTY_NETWORK_ENVIRONMENT,
        },
        Reason::JoinAccepted => DaemonClusterMaintenanceRequestPlan {
            dirty: DAEMON_CLUSTER_MAINTENANCE_DIRTY_PUBLISH
                | DAEMON_CLUSTER_MAINTENANCE_DIRTY_CONNECTIVITY,
        },
        Reason::CommittedConnectivityTopologyChanged => DaemonClusterMaintenanceRequestPlan {
            dirty: DAEMON_CLUSTER_MAINTENANCE_DIRTY_CONNECTIVITY,
        },
        Reason::Rename => DaemonClusterMaintenanceRequestPlan {
            dirty: DAEMON_CLUSTER_MAINTENANCE_DIRTY_PUBLISH,
        },
        Reason::RoleChanged => DaemonClusterMaintenanceRequestPlan {
            dirty: DAEMON_CLUSTER_MAINTENANCE_DIRTY_PUBLISH,
        },
        Reason::ManualBroad => DaemonClusterMaintenanceRequestPlan {
            dirty: DAEMON_CLUSTER_MAINTENANCE_DIRTY_PUBLISH
                | DAEMON_CLUSTER_MAINTENANCE_DIRTY_CONNECTIVITY
                | DAEMON_CLUSTER_MAINTENANCE_DIRTY_DISCOVERY
                | DAEMON_CLUSTER_MAINTENANCE_DIRTY_NETWORK_ENVIRONMENT,
        },
    }
}

/// The main daemon is the only process role which can own the default DFS
/// lifecycle. Node-local module activation independently decides whether that
/// role actually materializes DFS on this node.
fn daemon_role_owns_default_dfs(label: &str) -> bool {
    label == "daemon"
}

fn daemon_owns_enabled_default_dfs(paths: &DaemonPaths, label: &str) -> bool {
    daemon_role_owns_default_dfs(label) && crate::service_adapter::dfs_module_should_register(paths)
}

/// A settled one-node cluster has no peer whose later publication can repair
/// missing DFS placement. Complete its compact control-plane bootstrap before
/// accepting app traffic so a fresh embedded daemon cannot expose SIFS with an
/// empty metadata-shard topology.
fn daemon_default_dfs_bootstrap_must_complete_before_serving(
    paths: &DaemonPaths,
    label: &str,
) -> Result<bool, DaemonError> {
    if !daemon_owns_enabled_default_dfs(paths, label) {
        return Ok(false);
    }
    let cluster_paths = cluster_paths_from_daemon_paths(paths)?;
    if !cluster_paths.state.exists() {
        return Ok(false);
    }
    let state = sinomo_core::cluster::read_cluster_state_without_kv_cached_shared(&cluster_paths)?;
    Ok(sinomo_core::network::cluster_is_settled_solo(&state))
}

async fn daemon_optional_notify(notify: Option<Arc<tokio::sync::Notify>>) {
    if let Some(notify) = notify {
        notify.notified().await;
    } else {
        std::future::pending::<()>().await;
    }
}

async fn daemon_dfs_storage_refresh_error_retry(failed: bool) {
    if failed {
        tokio::time::sleep(DAEMON_DFS_STORAGE_REFRESH_ERROR_RETRY_DELAY).await;
    } else {
        std::future::pending::<()>().await;
    }
}

fn daemon_cluster_maintenance_notify() -> Arc<tokio::sync::Notify> {
    Arc::clone(
        DAEMON_CLUSTER_MAINTENANCE_NOTIFY.get_or_init(|| Arc::new(tokio::sync::Notify::new())),
    )
}

pub(crate) fn request_daemon_cluster_maintenance(reason: DaemonClusterMaintenanceReason) {
    let plan = daemon_cluster_maintenance_request_plan(reason);
    if plan.dirty != 0 {
        DAEMON_CLUSTER_MAINTENANCE_DIRTY.fetch_or(plan.dirty, Ordering::Release);
    }
    daemon_cluster_maintenance_notify().notify_one();
}

fn take_daemon_cluster_maintenance_dirty() -> u8 {
    DAEMON_CLUSTER_MAINTENANCE_DIRTY.swap(0, Ordering::AcqRel)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DaemonClusterNetworkMaintenancePhaseDisposition {
    Completed,
    TimedOut,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DaemonClusterNetworkMaintenanceSubphase {
    TickPlan,
    PendingWork,
}

fn cluster_network_maintenance_subphase_name(
    subphase: DaemonClusterNetworkMaintenanceSubphase,
) -> &'static str {
    match subphase {
        DaemonClusterNetworkMaintenanceSubphase::TickPlan => "tick_plan",
        DaemonClusterNetworkMaintenanceSubphase::PendingWork => "pending_work",
    }
}

fn cluster_network_maintenance_phase_disposition_name(
    disposition: DaemonClusterNetworkMaintenancePhaseDisposition,
) -> &'static str {
    match disposition {
        DaemonClusterNetworkMaintenancePhaseDisposition::Completed => "completed",
        DaemonClusterNetworkMaintenancePhaseDisposition::TimedOut => "timed_out",
    }
}

fn maintenance_duration_us(duration: Duration) -> u64 {
    duration.as_micros().min(u128::from(u64::MAX)) as u64
}

fn observe_cluster_network_maintenance_phase(
    disposition: DaemonClusterNetworkMaintenancePhaseDisposition,
    duration: Duration,
) {
    let disposition = cluster_network_maintenance_phase_disposition_name(disposition);
    let duration_us = maintenance_duration_us(duration);
    metrics::counter!(
        "sinomo.daemon.cluster_network.maintenance.phases",
        "disposition" => disposition
    )
    .increment(1);
    metrics::histogram!(
        "sinomo.daemon.cluster_network.maintenance.phase.us",
        "disposition" => disposition
    )
    .record(duration_us as f64);
    tracing::trace!(
        target: "sinomo.daemon.cluster_network_maintenance",
        disposition,
        duration_us,
        "cluster network maintenance phase completed"
    );
}

fn node_resource_mode_name(mode: sinomo_device::NodeResourceMode) -> &'static str {
    match mode {
        sinomo_device::NodeResourceMode::Normal => "normal",
        sinomo_device::NodeResourceMode::Constrained => "constrained",
        sinomo_device::NodeResourceMode::Restricted => "restricted",
    }
}

fn node_network_metering_name(metering: sinomo_device::NodeNetworkMetering) -> &'static str {
    match metering {
        sinomo_device::NodeNetworkMetering::Unmetered => "unmetered",
        sinomo_device::NodeNetworkMetering::Metered => "metered",
        sinomo_device::NodeNetworkMetering::Unknown => "unknown",
    }
}

fn observe_daemon_node_resources(resources: sinomo_device::NodeResourceSnapshot) {
    if !resources.observed {
        return;
    }
    let mode = node_resource_mode_name(resources.mode);
    let network_metering = node_network_metering_name(resources.network_metering);
    metrics::gauge!("sinomo.daemon.node_resources.mode").set(match resources.mode {
        sinomo_device::NodeResourceMode::Normal => 0.0,
        sinomo_device::NodeResourceMode::Constrained => 1.0,
        sinomo_device::NodeResourceMode::Restricted => 2.0,
    });
    metrics::counter!(
        "sinomo.daemon.node_resources.changes",
        "mode" => mode,
        "network_metering" => network_metering,
    )
    .increment(1);
    tracing::info!(
        target: "sinomo.daemon.node_resources",
        mode,
        network_metering,
        "daemon node resource conditions changed"
    );
}

struct DaemonClusterNetworkMaintenanceSubphaseObservation {
    subphase: DaemonClusterNetworkMaintenanceSubphase,
    started: Instant,
    completed: bool,
    has_pending_work: Option<bool>,
}

impl DaemonClusterNetworkMaintenanceSubphaseObservation {
    fn start(subphase: DaemonClusterNetworkMaintenanceSubphase) -> Self {
        tracing::trace!(
            target: "sinomo.daemon.cluster_network_maintenance",
            subphase = cluster_network_maintenance_subphase_name(subphase),
            "cluster network maintenance subphase started"
        );
        Self {
            subphase,
            started: Instant::now(),
            completed: false,
            has_pending_work: None,
        }
    }

    fn complete(&mut self, has_pending_work: Option<bool>) {
        self.completed = true;
        self.has_pending_work = has_pending_work;
    }
}

impl Drop for DaemonClusterNetworkMaintenanceSubphaseObservation {
    fn drop(&mut self) {
        let subphase = cluster_network_maintenance_subphase_name(self.subphase);
        let completed = if self.completed { "true" } else { "false" };
        let duration_us = maintenance_duration_us(self.started.elapsed());
        metrics::histogram!(
            "sinomo.daemon.cluster_network.maintenance.subphase.us",
            "subphase" => subphase,
            "completed" => completed
        )
        .record(duration_us as f64);
        metrics::counter!(
            "sinomo.daemon.cluster_network.maintenance.subphases",
            "subphase" => subphase,
            "completed" => completed
        )
        .increment(1);
        tracing::trace!(
            target: "sinomo.daemon.cluster_network_maintenance",
            subphase,
            completed = self.completed,
            has_pending_work = self.has_pending_work,
            duration_us,
            "cluster network maintenance subphase ended"
        );
    }
}

fn daemon_network_environment_watch_enabled() -> bool {
    daemon_network_environment_watch_enabled_for(
        std::env::var(DAEMON_NETWORK_ENVIRONMENT_WATCH_ENV)
            .ok()
            .as_deref(),
        cfg!(target_os = "linux"),
    )
}

fn daemon_network_environment_watch_enabled_for(value: Option<&str>, is_linux: bool) -> bool {
    match value.map(str::trim).map(str::to_ascii_lowercase).as_deref() {
        Some("1" | "true" | "yes" | "on") => true,
        Some("0" | "false" | "no" | "off") => false,
        Some(_) | None => !is_linux,
    }
}

struct DaemonConnectivityTopologySubscriptions {
    _snapshot_subscription: sinomo_core::cluster::ClusterKvCommittedSnapshotSubscription,
    _delta_subscription: sinomo_core::cluster::ClusterKvCommittedDeltaSubscription,
    _external_snapshot_watcher: Option<notify::RecommendedWatcher>,
    // The callbacks retain this too. Keeping it here makes the ownership
    // explicit and lets focused tests inspect the independent observer.
    _projection: Arc<Mutex<DaemonConnectivityTopologyProjection>>,
}

#[derive(Clone)]
struct DaemonConnectivityTopologyObserver {
    cluster_paths: sinomo_core::network::ClusterPaths,
    daemon_paths: DaemonPaths,
    runtime: tokio::runtime::Handle,
    projection: Arc<Mutex<DaemonConnectivityTopologyProjection>>,
    wake_default_dfs: bool,
}

impl DaemonConnectivityTopologyObserver {
    fn observe_snapshot(&self, snapshot: &sinomo_core::kv::KvSnapshot, source: &'static str) {
        let changed = self
            .projection
            .lock()
            .map(|mut projection| projection.observe(snapshot))
            .unwrap_or(DaemonConnectivityProjectionChange {
                probe_inputs_changed: true,
                route_generation_changed: true,
            });
        self.publish_change(changed, source, snapshot.revision.0);
    }

    fn observe_delta(&self, delta: &sinomo_core::kv::ClusterKvCommittedDelta) {
        let observed = self
            .projection
            .lock()
            .map(|mut projection| projection.observe_delta(delta));
        match observed {
            Ok(Some(changed)) => {
                self.publish_change(changed, "committed connectivity delta", delta.revision.0)
            }
            Ok(None) => self.rebuild_after_delta_gap(delta),
            Err(_) => self.publish_change(
                DaemonConnectivityProjectionChange {
                    probe_inputs_changed: true,
                    route_generation_changed: true,
                },
                "poisoned committed connectivity delta projection",
                delta.revision.0,
            ),
        }
    }

    fn rebuild_after_delta_gap(&self, delta: &sinomo_core::kv::ClusterKvCommittedDelta) {
        tracing::warn!(
            observed_revision = self
                .projection
                .lock()
                .ok()
                .and_then(|projection| projection.revision()),
            delta_revision = delta.revision.0,
            compacted_revision = delta.compacted_revision.0,
            "connectivity topology delta has no continuous baseline; rebuilding its resident projection"
        );
        match sinomo_core::cluster::read_cluster_kv_latest_observed_snapshot_arc_if_present(
            &self.cluster_paths,
        ) {
            Ok(Some(snapshot)) => {
                self.observe_snapshot(snapshot.as_ref(), "rebuilt committed connectivity snapshot")
            }
            Ok(None) => self.publish_change(
                DaemonConnectivityProjectionChange {
                    probe_inputs_changed: true,
                    route_generation_changed: true,
                },
                "missing committed connectivity rebuild snapshot",
                delta.revision.0,
            ),
            Err(error) => {
                tracing::warn!(
                    "connectivity topology delta gap could not rebuild its resident projection: {error}"
                );
                self.publish_change(
                    DaemonConnectivityProjectionChange {
                        probe_inputs_changed: true,
                        route_generation_changed: true,
                    },
                    "failed committed connectivity rebuild snapshot",
                    delta.revision.0,
                );
            }
        }
    }

    fn publish_change(
        &self,
        changed: DaemonConnectivityProjectionChange,
        source: &'static str,
        generation: u64,
    ) {
        if changed.probe_inputs_changed {
            request_daemon_cluster_maintenance(
                DaemonClusterMaintenanceReason::CommittedConnectivityTopologyChanged,
            );
        }
        if changed.route_generation_changed && self.wake_default_dfs {
            notify_daemon_dfs_metadata_publish_route_generation(
                &self.daemon_paths,
                &self.runtime,
                source,
                generation,
            );
        }
    }
}

fn daemon_connectivity_topology_snapshot_watcher(
    cluster_paths: &sinomo_core::network::ClusterPaths,
    observer: DaemonConnectivityTopologyObserver,
) -> Result<notify::RecommendedWatcher, DaemonError> {
    let snapshot_path = cluster_paths.kv_paths().snapshot;
    let callback_snapshot_path = snapshot_path.clone();
    let callback_cluster_paths = cluster_paths.clone();
    let callback_runtime = observer.runtime.clone();
    let event_generation = Arc::new(AtomicU64::new(0));
    let mut watcher = notify::recommended_watcher(
        move |result: Result<notify::Event, notify::Error>| match result {
            Ok(event)
                if event
                    .paths
                    .iter()
                    .any(|path| path == &callback_snapshot_path) =>
            {
                let event_id = event_generation
                    .fetch_add(1, Ordering::AcqRel)
                    .saturating_add(1);
                let event_generation = Arc::clone(&event_generation);
                let cluster_paths = callback_cluster_paths.clone();
                let snapshot_path = callback_snapshot_path.clone();
                let observer = observer.clone();
                callback_runtime.spawn(async move {
                    tokio::time::sleep(DAEMON_CONNECTIVITY_EXTERNAL_SNAPSHOT_DEBOUNCE).await;
                    if event_generation.load(Ordering::Acquire) != event_id {
                        return;
                    }
                    // The writer's process-local observer cannot invalidate a
                    // shadow daemon's caches. Invalidate before sampling the
                    // shared critical projection so every later route/state
                    // read is forced through the new generation.
                    sinomo_core::cluster::invalidate_cluster_kv_observed_read_models(
                        &cluster_paths,
                    );
                    match sinomo_core::cluster::read_cluster_kv_latest_observed_snapshot_arc_if_present(
                        &cluster_paths,
                    ) {
                        Ok(Some(snapshot)) => observer.observe_snapshot(
                            snapshot.as_ref(),
                            "external committed connectivity snapshot",
                        ),
                        Ok(None) => {}
                        Err(error) => tracing::warn!(
                            path = %snapshot_path.display(),
                            "committed connectivity snapshot change could not be read: {error}"
                        ),
                    }
                });
            }
            Ok(_) => {}
            Err(error) => tracing::warn!(
                path = %callback_snapshot_path.display(),
                "committed connectivity snapshot watcher failed: {error}"
            ),
        },
    )
    .map_err(|source| DaemonError::Network {
        address: snapshot_path.display().to_string(),
        detail: format!("cannot watch committed connectivity snapshot: {source}"),
    })?;
    let watch_root = snapshot_path.parent().ok_or_else(|| DaemonError::Network {
        address: snapshot_path.display().to_string(),
        detail: "committed connectivity snapshot has no parent directory".to_string(),
    })?;
    notify::Watcher::watch(
        &mut watcher,
        watch_root,
        notify::RecursiveMode::NonRecursive,
    )
    .map_err(|source| DaemonError::Network {
        address: watch_root.display().to_string(),
        detail: format!("cannot watch committed connectivity snapshot directory: {source}"),
    })?;
    Ok(watcher)
}

fn subscribe_daemon_connectivity_topology_sources(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
    wake_default_dfs: bool,
) -> Result<DaemonConnectivityTopologySubscriptions, DaemonError> {
    let cluster_paths = cluster_paths_from_daemon_paths(paths)?;
    let projection = Arc::new(Mutex::new(DaemonConnectivityTopologyProjection::new(
        wake_default_dfs,
    )));
    let observer = DaemonConnectivityTopologyObserver {
        cluster_paths: cluster_paths.clone(),
        daemon_paths: paths.clone(),
        runtime: runtime.clone(),
        projection: Arc::clone(&projection),
        wake_default_dfs,
    };
    let snapshot_observer = observer.clone();
    let snapshot_subscription = sinomo_core::cluster::subscribe_cluster_kv_committed_snapshots(
        &cluster_paths,
        move |snapshot| {
            snapshot_observer.observe_snapshot(snapshot, "committed connectivity snapshot")
        },
    );
    let delta_observer = observer.clone();
    let delta_subscription =
        sinomo_core::cluster::subscribe_cluster_kv_committed_deltas(&cluster_paths, move |delta| {
            delta_observer.observe_delta(delta)
        });
    // Main-daemon Raft/read-model mutations publish exact process-local
    // snapshot and delta callbacks above. Only the storage-fenced shadow needs
    // a filesystem bridge for generations written by the main process.
    let external_snapshot_watcher = crate::daemon_paths_are_shadow(paths)
        .then(|| daemon_connectivity_topology_snapshot_watcher(&cluster_paths, observer))
        .transpose()?;
    // Close the register/read race without creating a synthetic startup wake:
    // the maintenance owner already performs one startup reconciliation, and
    // callbacks are blocked on this projection lock until the coherent sample
    // is installed. A callback that won the lock first is included by the
    // subsequent resident read.
    if let Ok(mut projection_guard) = projection.lock() {
        match sinomo_core::cluster::read_cluster_kv_latest_observed_snapshot_arc_if_present(
            &cluster_paths,
        ) {
            Ok(Some(snapshot)) => {
                projection_guard.seed_if_newer(&snapshot);
            }
            Ok(None) => {}
            Err(error) => tracing::warn!(
                "connectivity topology observation started without an initial KV projection: {error}"
            ),
        }
    }
    Ok(DaemonConnectivityTopologySubscriptions {
        _snapshot_subscription: snapshot_subscription,
        _delta_subscription: delta_subscription,
        _external_snapshot_watcher: external_snapshot_watcher,
        _projection: projection,
    })
}

fn notify_daemon_dfs_metadata_publish_route_generation(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
    source: &'static str,
    generation: u64,
) {
    // The same route-generation edge that re-dirties retained metadata
    // publishers also clears the only external blocker for connectivity-
    // parked content repairs. Wake an executor only when the bounded durable
    // queue is known non-empty; ordinary route refreshes on a healthy/empty
    // cluster must not launch a broad DFS maintenance pass.
    if daemon_dfs_repair_queue_work_is_known(paths) {
        notify_daemon_dfs_maintenance_work();
    }
    match notify_daemon_dfs_metadata_publish_route_generation_on_runtime(paths, runtime) {
        Ok(0) => {}
        Ok(lanes) => tracing::debug!(
            source,
            generation,
            lanes,
            "DFS retained metadata lanes re-dirtied for route generation"
        ),
        Err(error) => tracing::warn!(
            source,
            generation,
            "DFS retained metadata lanes could not observe route generation: {error}"
        ),
    }
}

async fn run_daemon_dfs_metadata_publish_route_generation_watch(
    paths: DaemonPaths,
    runtime: tokio::runtime::Handle,
    mut subscription: sinomo_core::network::__connectivity_store::LocalClusterConnectivityRoutingGenerationSubscription,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            biased;
            changed = shutdown_rx.changed() => {
                if changed.is_err() || daemon_shutdown_requested(&shutdown_rx) {
                    return;
                }
            }
            generation = subscription.changed() => {
                notify_daemon_dfs_metadata_publish_route_generation(
                    &paths,
                    &runtime,
                    "local connectivity store",
                    generation,
                );
            }
        }
    }
}

struct DaemonNetworkEnvironmentWatch {
    task: tokio::task::JoinHandle<()>,
}

async fn start_daemon_network_environment_watch(
    label: &str,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> Option<DaemonNetworkEnvironmentWatch> {
    if !daemon_network_environment_watch_enabled() {
        tracing::debug!(
            "sinomo {label} network environment watcher disabled; scheduled cluster maintenance remains active"
        );
        return None;
    }
    let mut watch = match netwatcher::watch_interfaces_async::<netwatcher::async_adapter::Tokio>() {
        Ok(watch) => watch,
        Err(error) => {
            tracing::warn!("sinomo {label} network environment watcher unavailable: {error}");
            return None;
        }
    };
    let initial = watch.changed().await;
    tracing::debug!(
        "sinomo {label} network environment watcher initialized with {} interface(s)",
        initial.interfaces.len()
    );
    let task_label = label.to_string();
    let task = tokio::spawn(async move {
        loop {
            let update = tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        return;
                    }
                    continue;
                }
                update = watch.changed() => update,
            };
            tracing::debug!(
                "sinomo {task_label} network environment changed: +{} -{} ~{}",
                update.diff.added.len(),
                update.diff.removed.len(),
                update.diff.modified.len()
            );
            request_daemon_cluster_maintenance(
                DaemonClusterMaintenanceReason::NetworkEnvironmentChanged,
            );
        }
    });
    Some(DaemonNetworkEnvironmentWatch { task })
}

fn daemon_sandbox_lifecycle_recovery_state(
    paths: &DaemonPaths,
) -> sinomo_sandbox::SandboxLifecycleRecoveryState {
    sinomo_sandbox::SandboxLifecycleRecoveryState::new(
        paths.root.join(DAEMON_SANDBOX_LIFECYCLE_RECOVERY_STATE_DIR),
    )
}

type DaemonSandboxDfsService = sinomo_dfs::NetworkedDfsService<sinomo_dfs::DfsNodeTransport>;
type DaemonSandboxInstanceStore =
    sinomo_sandbox::SifsSandboxInstanceStore<sinomo_dfs::DfsNodeTransport>;
type DaemonSandboxExecutionLease =
    sinomo_sandbox::ClusteredSandboxExecutionLease<sinomo_dfs::DfsNodeTransport>;

struct DaemonSandboxLifecycleHooks<'a> {
    paths: &'a DaemonPaths,
}

impl sinomo_sandbox::ClusteredSandboxLifecycleHooks for DaemonSandboxLifecycleHooks<'_> {
    type Error = DaemonError;

    fn load_policy_config(
        &mut self,
    ) -> Result<sinomo_sandbox::SandboxClusterPolicyConfig, Self::Error> {
        load_daemon_sandbox_policy_config(self.paths)
    }

    fn append_event(
        &mut self,
        event: sinomo_core::EventEnvelope,
        durability: sinomo_sandbox::SandboxAuditDurability,
    ) -> Result<(), Self::Error> {
        match durability {
            sinomo_sandbox::SandboxAuditDurability::Buffered => {
                append_daemon_sandbox_event(self.paths, event)
            }
            sinomo_sandbox::SandboxAuditDurability::Durable => {
                append_daemon_sandbox_event_durable(self.paths, event)
            }
        }
    }

    fn clear_authoritative_recovery_pending(
        &mut self,
        sandbox_id: &sinomo_sandbox::SandboxId,
        context: &'static str,
    ) {
        daemon_sandbox_lifecycle_recovery_state(self.paths)
            .clear_authoritative_recovery_pending_best_effort(sandbox_id, context);
    }

    fn mark_authoritative_recovery_pending(
        &mut self,
        sandbox_id: &sinomo_sandbox::SandboxId,
    ) -> Result<(), Self::Error> {
        daemon_sandbox_lifecycle_recovery_state(self.paths)
            .mark_authoritative_recovery_pending(sandbox_id)
            .map_err(Into::into)
    }

    fn invalid(&mut self, detail: String) -> Self::Error {
        DaemonError::Parse { detail }
    }
}

impl sinomo_sandbox::ClusteredSandboxToolchainHooks for DaemonSandboxLifecycleHooks<'_> {
    type Error = DaemonError;

    fn load_policy_config(
        &mut self,
    ) -> Result<sinomo_sandbox::SandboxClusterPolicyConfig, Self::Error> {
        load_daemon_sandbox_policy_config(self.paths)
    }

    fn append_event(&mut self, event: sinomo_core::EventEnvelope) -> Result<(), Self::Error> {
        append_daemon_sandbox_event(self.paths, event)
    }

    fn invalid(&mut self, detail: String) -> Self::Error {
        DaemonError::Parse { detail }
    }
}

fn open_daemon_sandbox_store_with_mutation_lease(
    paths: &DaemonPaths,
    sandbox_id: &sinomo_sandbox::SandboxId,
    runtime: Option<&tokio::runtime::Handle>,
) -> Result<
    (
        DaemonSandboxExecutionLease,
        DaemonSandboxInstanceStore,
        Vec<sinomo_core::node::NodeId>,
    ),
    DaemonError,
> {
    open_daemon_sandbox_store_with_lease(
        paths,
        sandbox_id,
        runtime,
        sinomo_sandbox::ClusteredSandboxOperationKind::Mutation,
        cfg!(feature = "sandbox"),
    )
}

fn open_daemon_sandbox_store_with_transaction_lease(
    paths: &DaemonPaths,
    sandbox_id: &sinomo_sandbox::SandboxId,
    runtime: Option<&tokio::runtime::Handle>,
) -> Result<
    (
        DaemonSandboxExecutionLease,
        DaemonSandboxInstanceStore,
        Vec<sinomo_core::node::NodeId>,
    ),
    DaemonError,
> {
    open_daemon_sandbox_store_with_lease(
        paths,
        sandbox_id,
        runtime,
        sinomo_sandbox::ClusteredSandboxOperationKind::AuthoritativeTransaction,
        false,
    )
}

fn open_daemon_sandbox_store_with_lease(
    paths: &DaemonPaths,
    sandbox_id: &sinomo_sandbox::SandboxId,
    runtime: Option<&tokio::runtime::Handle>,
    kind: sinomo_sandbox::ClusteredSandboxOperationKind,
    recover_existing: bool,
) -> Result<
    (
        DaemonSandboxExecutionLease,
        DaemonSandboxInstanceStore,
        Vec<sinomo_core::node::NodeId>,
    ),
    DaemonError,
> {
    // The renewable lease requires an independently owned metadata service for
    // its background renewal thread. Both services can still share one
    // clone-backed content store and one replica view for the whole operation.
    let content = open_daemon_sandbox_content_store(paths)?;
    let (lease_dfs, replica_candidates) =
        open_daemon_sandbox_dfs_service_with_replica_candidates_and_content_on_runtime(
            paths,
            runtime,
            content.clone(),
        )?;
    let dfs = open_daemon_sandbox_dfs_service_with_content_on_runtime(paths, runtime, content)?;
    let mut hooks = DaemonSandboxLifecycleHooks { paths };
    sinomo_sandbox::ClusteredSandboxLifecycleRuntime::new(&mut hooks).open_operation(
        lease_dfs,
        dfs,
        replica_candidates,
        sandbox_id,
        kind,
        recover_existing,
    )
}

pub fn run_daemon_control_loop(
    options: crate::DaemonRunOptions,
    paths: DaemonPaths,
    label: &str,
    cluster: Option<DaemonClusterStatus>,
) -> Result<(), DaemonError> {
    run_daemon_control_loop_with_components(options, paths, label, cluster, Vec::new())
}

pub fn run_daemon_control_loop_with_components(
    options: crate::DaemonRunOptions,
    paths: DaemonPaths,
    label: &str,
    cluster: Option<DaemonClusterStatus>,
    component_factories: Vec<crate::DaemonComponentFactory>,
) -> Result<(), DaemonError> {
    let component_catalog =
        crate::service_adapter::component_catalog_from_factories(component_factories)?;
    run_daemon_control_loop_with_catalog_and_profile(
        options,
        paths,
        label,
        cluster,
        component_catalog,
        crate::DaemonExecutionProfile::Standard,
    )
}

pub fn run_daemon_control_loop_with_catalog(
    options: crate::DaemonRunOptions,
    paths: DaemonPaths,
    label: &str,
    cluster: Option<DaemonClusterStatus>,
    component_catalog: crate::DaemonComponentCatalog,
) -> Result<(), DaemonError> {
    run_daemon_control_loop_with_catalog_and_profile(
        options,
        paths,
        label,
        cluster,
        component_catalog,
        crate::DaemonExecutionProfile::Standard,
    )
}

/// Runs the daemon with an explicit resource profile. Embedded application
/// hosts use this entry point so mobile process constraints do not change the
/// standard standalone daemon defaults.
pub fn run_daemon_control_loop_with_components_and_profile(
    options: crate::DaemonRunOptions,
    paths: DaemonPaths,
    label: &str,
    cluster: Option<DaemonClusterStatus>,
    component_factories: Vec<crate::DaemonComponentFactory>,
    execution_profile: crate::DaemonExecutionProfile,
) -> Result<(), DaemonError> {
    let component_catalog =
        crate::service_adapter::component_catalog_from_factories(component_factories)?;
    run_daemon_control_loop_with_catalog_and_profile(
        options,
        paths,
        label,
        cluster,
        component_catalog,
        execution_profile,
    )
}

pub fn run_daemon_control_loop_with_catalog_and_profile(
    options: crate::DaemonRunOptions,
    paths: DaemonPaths,
    label: &str,
    cluster: Option<DaemonClusterStatus>,
    component_catalog: crate::DaemonComponentCatalog,
    execution_profile: crate::DaemonExecutionProfile,
) -> Result<(), DaemonError> {
    run_daemon_control_loop_with_catalog_and_runtime(
        options,
        paths,
        label,
        cluster,
        component_catalog,
        crate::DaemonRuntimeConfig::new(execution_profile),
    )
}

pub fn run_daemon_control_loop_with_catalog_and_runtime(
    options: crate::DaemonRunOptions,
    paths: DaemonPaths,
    label: &str,
    cluster: Option<DaemonClusterStatus>,
    component_catalog: crate::DaemonComponentCatalog,
    runtime_config: crate::DaemonRuntimeConfig,
) -> Result<(), DaemonError> {
    crate::cluster::deny_shadow_daemon_local_raft_access(&paths)?;
    crate::cluster::ensure_cluster_directory_installed();
    let _lock = DaemonLock::acquire(&paths, &options, cluster.clone())?;
    let _data_owner_lock = if label == "daemon" {
        let lock = Arc::new(DaemonDataOwnerLock::acquire(&paths)?);
        register_daemon_dfs_data_owner_lock(&paths, &lock)?;
        Some(lock)
    } else {
        None
    };
    // This must precede every loader-state read. Both the process-wide network
    // registry below and the service module registry load native dylibs.
    crate::rollout::recover_native_loader_intents_before_registry_load(&paths)?;
    if let Err(error) =
        sinomo_observe::init_observe_runtime(sinomo_observe::ObserveRuntimeConfig::daemon(
            label,
            cluster.as_ref().map(|status| status.node_id.to_string()),
        ))
    {
        tracing::warn!("sinomo {label} observe runtime initialization failed: {error}");
    }
    sinomo_observe::register_builtin_observe_metric_exporters();
    crate::dfs::register_daemon_dfs_observe_metric_exporter();
    let startup_local_raft_resident_initialized =
        match prune_local_cluster_raft_uncommitted_tail(&paths) {
            Ok(initialized) => initialized,
            Err(error) => {
                tracing::warn!("sinomo {label} cluster raft startup repair skipped: {error}");
                false
            }
        };
    let daemon_runtime =
        crate::build_daemon_async_runtime_with_profile(label, runtime_config.execution_profile())?;
    let daemon_runtime_handle = daemon_runtime.handle().clone();
    let node_resources = runtime_config.node_resources();
    // Identity-bound background work must use the immutable identity captured
    // at process startup. Init/join/leave replace the process after responding;
    // a daemon that started unclustered must never adopt the new disk identity.
    let startup_cluster_runtime_enabled = cluster.is_some();
    let bind_address = format!("{}:{}", options.host, options.port);
    let listener = daemon_runtime.block_on(
        sinomo_core::network::AsyncDaemonControlListener::bind(&bind_address),
    )?;
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let shutdown_signal = shutdown_tx.clone();
    let mut runtime_tasks: Vec<(&'static str, tokio::task::JoinHandle<()>)> = Vec::new();
    tracing::info!("sinomo {label} listening on {bind_address}");
    crate::append_operational_event_best_effort(
        &paths,
        crate::OperationalSeverity::Info,
        "daemon-control",
        "daemon.lifecycle.started",
        format!("sinomo {label} listening on {bind_address}"),
        None,
        serde_json::json!({
            "label": label,
            "address": bind_address,
            "pid": std::process::id(),
            "cluster_node_id": cluster.as_ref().map(|status| status.node_id.to_string()),
        }),
    );
    // Load native-dylib network modules into the process-wide
    // NetworkModuleRegistry before serving traffic. Loopback/LAN-TCP keep
    // their in-crate dial paths inside sinomo-network; every other transport
    // goes through the registry.
    match daemon_data_dir_path(&paths) {
        Ok(data_dir_root) => {
            let network_module_context = match cluster_paths_from_daemon_paths(&paths) {
                Ok(cluster_paths) => {
                    cluster
                        .as_ref()
                        .map(|status| sinomo_core::network::NetworkModuleContext {
                            cluster_root: cluster_paths.root.clone(),
                            node_id: crate::cluster::daemon_network_module_node_id(status),
                            cluster_id: Some(status.cluster_id.clone()),
                            // Connect address, not the bind address: a wildcard
                            // bind (0.0.0.0/::) is not dialable by the module's
                            // inbound-frame forwarder.
                            daemon_forward_address: crate::daemon_connect_address(
                                &options.host,
                                options.port,
                            ),
                        })
                }
                Err(error) => {
                    tracing::warn!("sinomo {label} network module context unavailable: {error}");
                    None
                }
            };
            let modules = crate::registry::load_daemon_network_module_registry(
                &data_dir_root,
                network_module_context,
            );
            let loaded = modules.registry.snapshot();
            if !loaded.is_empty() {
                tracing::info!(
                    "sinomo {label} network modules loaded: {}",
                    loaded
                        .iter()
                        .map(|status| {
                            format!(
                                "{}({})",
                                status.descriptor.id,
                                if status.enabled {
                                    "enabled"
                                } else {
                                    "disabled"
                                }
                            )
                        })
                        .collect::<Vec<_>>()
                        .join(",")
                );
            }
        }
        Err(error) => {
            tracing::warn!("sinomo {label} network module registry skipped: {error}");
        }
    }

    if startup_cluster_runtime_enabled
        && let Some(task) = crate::cluster::start_daemon_network_module_context_task(
            &paths,
            &options,
            cluster.as_ref(),
            daemon_runtime_handle.clone(),
            shutdown_rx.clone(),
            label,
        )
    {
        runtime_tasks.push(("network module context", task));
    }

    let owns_clustered_default_dfs =
        daemon_owns_enabled_default_dfs(&paths, label) && startup_cluster_runtime_enabled;
    let default_dfs_bootstrap_must_complete = owns_clustered_default_dfs
        && daemon_default_dfs_bootstrap_must_complete_before_serving(&paths, label)?;
    if default_dfs_bootstrap_must_complete
        && daemon_runtime.block_on(
            crate::cluster::publish_startup_local_daemon_capabilities_async(
                &paths,
                &options,
                &daemon_runtime_handle,
            ),
        )?
    {
        tracing::info!(
            "sinomo {label} published current local capabilities before default DFS bootstrap"
        );
    }

    let mut default_dfs_reconciliation = if owns_clustered_default_dfs {
        let cluster_paths = cluster_paths_from_daemon_paths(&paths)?;
        let startup_cluster = cluster
            .as_ref()
            .expect("clustered default DFS startup has captured daemon identity");
        Some(
            crate::dfs::DaemonDefaultDfsReconciliationReactor::new_on_runtime(
                &cluster_paths,
                &startup_cluster.node_id,
                &daemon_runtime_handle,
            )?,
        )
    } else {
        None
    };
    if let Some(reactor) = default_dfs_reconciliation.as_mut() {
        // A settled solo node has no peer whose later publication can repair
        // missing DFS placement. Consume this reactor's dirty startup
        // generation before serving, then retain the same observed generation
        // in the background task below. Multi-member nodes start the task
        // dirty after the service host is ready so joining bridges can serve
        // membership traffic while quorum becomes reachable.
        if default_dfs_bootstrap_must_complete {
            let startup_cluster = cluster
                .as_ref()
                .expect("clustered default DFS startup has captured daemon identity");
            daemon_runtime.block_on(reactor.reconcile_startup_until_idle_async(
                &paths,
                &options,
                &startup_cluster.cluster_id,
                &startup_cluster.node_id,
                &daemon_runtime_handle,
            ))?;
        }
    }

    let idle_stats = Arc::new(crate::IdleStatsAtomics::new());
    crate::install_process_idle_stats(idle_stats.clone());
    crate::register_io_observer(idle_stats.clone());

    // The service host owns a lifecycle-scoped catalog publisher component;
    // generic cluster maintenance no longer carries service discovery state.
    let service_host = daemon_runtime.block_on(
        crate::service_adapter::DaemonServiceRegistryHost::new_on_runtime_with_catalog(
            paths.clone(),
            daemon_runtime_handle.clone(),
            component_catalog,
        ),
    )?;
    service_host.notify_component_host_start();

    let maintenance_paths = paths.clone();
    let maintenance_options = options.clone();
    let maintenance_label = label.to_string();
    let maintenance_idle_stats = idle_stats.clone();
    let maintenance_runtime = daemon_runtime_handle.clone();
    let maintenance_startup_cluster = cluster.clone();
    // The main daemon owns DFS data. Shadow processes still observe committed
    // topology for their own network maintenance but must not schedule a DFS
    // lane, even when both roles share one data root during a handoff.
    let maintenance_default_dfs_enabled =
        daemon_owns_enabled_default_dfs(&paths, label) && startup_cluster_runtime_enabled;
    let maintenance_connectivity_topology_subscription = if startup_cluster_runtime_enabled {
        Some(subscribe_daemon_connectivity_topology_sources(
            &paths,
            &daemon_runtime_handle,
            maintenance_default_dfs_enabled,
        )?)
    } else {
        None
    };
    if daemon_owns_enabled_default_dfs(&paths, label) {
        runtime_tasks.push((
            "DFS storage environment",
            daemon_runtime_handle.spawn(run_daemon_dfs_storage_environment_runtime_task(
                paths.clone(),
                daemon_runtime_handle.clone(),
                node_resources.clone(),
                shutdown_rx.clone(),
            )),
        ));
        runtime_tasks.push((
            "DFS remote placement observations",
            daemon_runtime_handle.spawn(run_daemon_dfs_remote_placement_observation_runtime_task(
                paths.clone(),
                daemon_runtime_handle.clone(),
                shutdown_rx.clone(),
            )),
        ));
    }
    // A daemon that started without cluster identity is intentionally short-lived
    // after a successful join. Its replacement owns distributed DFS maintenance;
    // the stale process must not acquire a backend generation while draining.
    if maintenance_default_dfs_enabled {
        let cluster_paths = cluster_paths_from_daemon_paths(&paths)?;
        let route_generation_subscription =
            sinomo_core::network::__connectivity_store::subscribe_local_cluster_connectivity_routing_generation(
                &cluster_paths,
            );
        runtime_tasks.push((
            "DFS metadata route generation",
            daemon_runtime_handle.spawn(run_daemon_dfs_metadata_publish_route_generation_watch(
                paths.clone(),
                daemon_runtime_handle.clone(),
                route_generation_subscription,
                shutdown_rx.clone(),
            )),
        ));
    }
    if let Some(reactor) = default_dfs_reconciliation.take() {
        let startup_cluster = cluster
            .as_ref()
            .expect("default DFS reactor has captured daemon identity");
        runtime_tasks.push((
            "default DFS reconciliation",
            daemon_runtime_handle.spawn(crate::dfs::run_daemon_default_dfs_reconciliation_reactor(
                reactor,
                paths.clone(),
                options.clone(),
                startup_cluster.cluster_id.clone(),
                startup_cluster.node_id.clone(),
                daemon_runtime_handle.clone(),
                shutdown_rx.clone(),
            )),
        ));
    }
    runtime_tasks.push((
        "background maintenance",
        daemon_runtime_handle.spawn(run_daemon_background_maintenance_loop(
            maintenance_paths,
            maintenance_options,
            maintenance_label,
            maintenance_idle_stats,
            maintenance_runtime,
            maintenance_startup_cluster,
            startup_local_raft_resident_initialized,
            maintenance_connectivity_topology_subscription,
            startup_cluster_runtime_enabled,
            maintenance_default_dfs_enabled,
            node_resources,
            shutdown_rx.clone(),
        )),
    ));

    // The listener is already bound and will accept queued connections as
    // soon as the runtime enters the loop below. Notify service-owned workers
    // through an in-process edge so startup never depends on a polling timer.
    service_host.notify_component_host_ready();
    daemon_runtime.block_on(run_daemon_control_accept_loop(
        listener,
        paths.clone(),
        label.to_string(),
        bind_address.clone(),
        service_host,
        daemon_runtime_handle.clone(),
        shutdown_tx,
        shutdown_rx.clone(),
    ))?;
    let _ = shutdown_signal.send(true);
    daemon_cluster_maintenance_notify().notify_waiters();
    for (task_label, task) in runtime_tasks {
        join_daemon_runtime_task(
            &daemon_runtime,
            task_label,
            task,
            daemon_runtime_task_abort_before_join(task_label),
        );
    }
    drain_daemon_runtime_after_task_shutdown(&daemon_runtime);
    if daemon_owns_enabled_default_dfs(&paths, label)
        && let Err(error) = evict_daemon_dfs_message_service_cache_on_shutdown(&paths)
    {
        tracing::warn!(
            target: "sinomo.dfs.placement",
            "failed to evict daemon DFS message cache during shutdown: {error}"
        );
    }
    crate::append_operational_event_best_effort(
        &paths,
        crate::OperationalSeverity::Info,
        "daemon-control",
        "daemon.lifecycle.stopped",
        format!("sinomo {label} stopped"),
        None,
        serde_json::json!({
            "label": label,
            "address": bind_address,
            "pid": std::process::id(),
        }),
    );
    match daemon_data_dir_path(&paths) {
        Ok(data_dir_root) => {
            if let Err(error) =
                sinomo_observe::persist_active_observe_network_timeline(&data_dir_root)
            {
                tracing::warn!(
                    "sinomo {label} network timeline shutdown artifact was not preserved: {error}"
                );
            }
        }
        Err(error) => tracing::warn!(
            "sinomo {label} network timeline shutdown artifact root was unavailable: {error}"
        ),
    }
    daemon_runtime.shutdown_timeout(DAEMON_RUNTIME_SHUTDOWN_TIMEOUT);
    Ok(())
}

fn join_daemon_runtime_task(
    runtime: &tokio::runtime::Runtime,
    label: &str,
    task: tokio::task::JoinHandle<()>,
    abort_first: bool,
) {
    let abort_handle = task.abort_handle();
    if abort_first {
        abort_handle.abort();
    }
    let join_timeout = daemon_runtime_task_join_timeout(label);
    let result = runtime.block_on(async { tokio::time::timeout(join_timeout, task).await });
    match result {
        Ok(Ok(())) => {}
        Ok(Err(error)) if error.is_cancelled() => {}
        Ok(Err(error)) => {
            tracing::warn!("sinomo daemon {label} task stopped with error: {error}");
        }
        Err(_) => {
            abort_handle.abort();
            tracing::warn!(
                "sinomo daemon {label} task did not stop within {}ms",
                join_timeout.as_millis()
            );
        }
    }
}

fn daemon_runtime_task_join_timeout(label: &str) -> Duration {
    match label {
        "network module context" => DAEMON_NETWORK_MODULE_CONTEXT_TASK_JOIN_TIMEOUT,
        _ => DAEMON_RUNTIME_TASK_JOIN_TIMEOUT,
    }
}

fn daemon_runtime_task_abort_before_join(label: &str) -> bool {
    label == "background maintenance"
}

fn drain_daemon_runtime_after_task_shutdown(runtime: &tokio::runtime::Runtime) {
    runtime.block_on(async {
        tokio::task::yield_now().await;
        tokio::time::sleep(DAEMON_RUNTIME_TASK_DRAIN_GRACE).await;
        tokio::task::yield_now().await;
    });
}

fn daemon_dfs_initial_maintenance_phase(paths: &DaemonPaths) -> Result<Duration, DaemonError> {
    let cluster_paths = cluster_paths_from_daemon_paths(paths)?;
    if !cluster_paths.state.exists() {
        return Ok(Duration::ZERO);
    }
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
        .map_err(DaemonError::from)?;
    Ok(daemon_dfs_maintenance_member_phase(&state))
}

fn spawn_daemon_dfs_maintenance_task(
    paths: DaemonPaths,
    runtime: tokio::runtime::Handle,
    node_resources: sinomo_device::NodeResourceSnapshot,
) -> OwnedDaemonDfsMaintenanceTask {
    OwnedDaemonDfsMaintenanceTask::new(tokio::spawn(
        run_daemon_dfs_maintenance_tick_in_blocking_task(paths, runtime, node_resources),
    ))
}

struct OwnedDaemonDfsMaintenanceTask {
    task: Option<tokio::task::JoinHandle<Result<DaemonDfsMaintenanceTickOutcome, DaemonError>>>,
}

impl OwnedDaemonDfsMaintenanceTask {
    fn new(
        task: tokio::task::JoinHandle<Result<DaemonDfsMaintenanceTickOutcome, DaemonError>>,
    ) -> Self {
        Self { task: Some(task) }
    }

    fn is_finished(&self) -> bool {
        self.task
            .as_ref()
            .is_none_or(tokio::task::JoinHandle::is_finished)
    }

    async fn join(&mut self, label: &str) -> Option<DaemonDfsMaintenanceTickOutcome> {
        let result = match self.task.as_mut() {
            Some(task) => task.await,
            None => return None,
        };
        self.task.take();
        daemon_dfs_maintenance_task_result(result, label)
    }
}

impl Drop for OwnedDaemonDfsMaintenanceTask {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            // The background-maintenance parent is deliberately abort-first
            // so daemon replacement remains bounded. Explicitly abort its DFS
            // child on cancellation instead of detaching it. A spawn_blocking
            // section already in progress cannot be preempted by Tokio, but it
            // retains the exclusive data-owner lock until the synchronous work
            // exits, preventing a replacement generation from overlapping it.
            task.abort();
        }
    }
}

fn daemon_dfs_maintenance_task_result(
    result: Result<Result<DaemonDfsMaintenanceTickOutcome, DaemonError>, tokio::task::JoinError>,
    label: &str,
) -> Option<DaemonDfsMaintenanceTickOutcome> {
    match result {
        Ok(Ok(outcome)) => Some(outcome),
        Ok(Err(error)) => {
            tracing::warn!("sinomo {label} DFS maintenance tick failed: {error}");
            None
        }
        Err(error) => {
            tracing::warn!("sinomo {label} DFS maintenance task failed: {error}");
            None
        }
    }
}

#[cfg(test)]
async fn join_daemon_dfs_maintenance_task(
    task: tokio::task::JoinHandle<Result<DaemonDfsMaintenanceTickOutcome, DaemonError>>,
    label: &str,
) -> Option<DaemonDfsMaintenanceTickOutcome> {
    daemon_dfs_maintenance_task_result(task.await, label)
}

fn apply_daemon_dfs_maintenance_probe_result(
    work: Option<DaemonDfsPendingMaintenanceWork>,
    node_resources: sinomo_device::NodeResourceSnapshot,
    triggered_maintenance_requested: &mut bool,
    immediate_continuation_requested: &mut bool,
    next_probe_at: &mut Option<Instant>,
) {
    let work_for_policy = work.clone();
    let decision =
        daemon_dfs_maintenance_probe_decision(Instant::now(), daemon_dfs_now_unix_nanos(), work);
    let decision = daemon_dfs_resource_adjusted_probe_decision(
        node_resources,
        work_for_policy.as_ref(),
        decision,
    );
    *triggered_maintenance_requested = decision.trigger_maintenance;
    *immediate_continuation_requested = decision.immediate_continuation;
    *next_probe_at = decision.next_probe_at;
}

fn daemon_dfs_pending_work_requires_local_recovery(work: &DaemonDfsPendingMaintenanceWork) -> bool {
    work.pending_write_count > 0
        || work.pending_namespace_job_count > 0
        || work.actionable_conflict_count > 0
        || work.pending_local_membership_handoff_count > 0
        || work.completed_local_primary_handoff_rebind_count > 0
        || work.mounted_mutation_recoverable_exact_count > 0
        || work.mounted_mutation_recoverable_full_count > 0
        || work.due_time_based_maintenance_count > 0
}

fn daemon_dfs_resource_adjusted_probe_decision(
    node_resources: sinomo_device::NodeResourceSnapshot,
    work: Option<&DaemonDfsPendingMaintenanceWork>,
    mut decision: sinomo_dfs::DfsMaintenanceProbeDecision,
) -> sinomo_dfs::DfsMaintenanceProbeDecision {
    if crate::dfs::daemon_dfs_background_network_work_deferred(node_resources)
        && work.is_some_and(|work| !daemon_dfs_pending_work_requires_local_recovery(work))
    {
        decision.trigger_maintenance = false;
        decision.immediate_continuation = false;
        // The node-resource watch is the exact wake source. Leaving a periodic
        // probe here would keep reading DFS state while the same OS restriction
        // is still active.
        decision.next_probe_at = None;
    }
    decision
}

async fn run_daemon_dfs_storage_environment_runtime_task(
    paths: DaemonPaths,
    runtime: tokio::runtime::Handle,
    mut node_resources: sinomo_device::NodeResourceMonitor,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
    let Some(storage_data_root) = paths.root.parent() else {
        tracing::warn!(
            target: "sinomo.dfs.placement",
            "DFS storage environment task has no daemon data-directory parent"
        );
        return;
    };
    let storage_data_dir = sinomo_core::local::SinomoDataDir::new(storage_data_root);
    let mut current_node_resources = node_resources.take_latest();
    let storage_environment_notify = daemon_dfs_storage_environment_notify(&storage_data_dir);
    let mut sampled_storage_generation = 0;
    let mut sample_storage = true;
    loop {
        if daemon_shutdown_requested(&shutdown_rx) {
            break;
        }
        if !sample_storage
            && daemon_dfs_storage_environment_dirty_generation(&storage_data_dir)
                != sampled_storage_generation
        {
            tokio::time::sleep(DAEMON_DFS_STORAGE_DIRTY_SETTLE_DELAY).await;
            sample_storage = true;
            continue;
        }
        let sampled_generation = sample_storage
            .then(|| daemon_dfs_storage_environment_dirty_generation(&storage_data_dir));
        let mut storage_sample_completed = false;
        let mut storage_refresh_failed = false;
        if !daemon_rollout_handoff_pending(&paths) {
            match crate::dfs::refresh_daemon_dfs_storage_environment_with_resources_on_runtime(
                &paths,
                &runtime,
                current_node_resources,
                sample_storage,
            )
            .await
            {
                Ok(true) => {
                    storage_sample_completed = sample_storage;
                    tracing::info!(
                        target: "sinomo.dfs.placement",
                        "published refreshed DFS storage environment advertisement"
                    );
                }
                Ok(false) => {
                    storage_sample_completed = sample_storage;
                }
                Err(error) => {
                    storage_refresh_failed = true;
                    tracing::warn!(
                        target: "sinomo.dfs.placement",
                        "DFS storage environment refresh failed: {error}"
                    );
                }
            }
        }
        if storage_sample_completed && let Some(sampled_generation) = sampled_generation {
            sampled_storage_generation = sampled_generation;
            sample_storage = false;
        }
        if daemon_dfs_storage_environment_dirty_generation(&storage_data_dir)
            != sampled_storage_generation
        {
            // Coalesce a burst of foreground writes into one capacity sample.
            // A generation arriving during the sample remains unequal and
            // receives another bounded pass rather than being lost.
            tokio::time::sleep(DAEMON_DFS_STORAGE_DIRTY_SETTLE_DELAY).await;
            sample_storage = true;
            continue;
        }
        tokio::select! {
            _ = daemon_dfs_storage_refresh_error_retry(storage_refresh_failed) => {}
            _ = storage_environment_notify.notified() => {}
            resources = node_resources.changed() => {
                current_node_resources = resources;
            }
            changed = shutdown_rx.changed() => {
                if changed.is_err() || daemon_shutdown_requested(&shutdown_rx) {
                    break;
                }
            }
        }
    }
}

async fn run_daemon_dfs_remote_placement_observation_runtime_task(
    paths: DaemonPaths,
    _runtime: tokio::runtime::Handle,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
    let _subscription = match subscribe_daemon_dfs_remote_placement_observation(&paths) {
        Ok(subscription) => subscription,
        Err(error) => {
            tracing::warn!(
                target: "sinomo.dfs.placement",
                "DFS remote placement subscription failed: {error}"
            );
            None
        }
    };
    while !daemon_shutdown_requested(&shutdown_rx) {
        if shutdown_rx.changed().await.is_err() {
            break;
        }
    }
}

#[cfg(feature = "sandbox")]
fn daemon_sandbox_lifecycle_maintenance_allowed(
    label: &str,
    startup_cluster_maintenance_attempted: bool,
) -> bool {
    label == "daemon" && startup_cluster_maintenance_attempted
}

fn daemon_dfs_maintenance_allowed(
    maintain_default_dfs: bool,
    startup_cluster_maintenance_attempted: bool,
) -> bool {
    maintain_default_dfs && startup_cluster_maintenance_attempted
}

async fn run_daemon_background_maintenance_loop(
    paths: DaemonPaths,
    options: crate::DaemonRunOptions,
    label: String,
    idle_stats: Arc<crate::IdleStatsAtomics>,
    runtime: tokio::runtime::Handle,
    startup_cluster: Option<DaemonClusterStatus>,
    startup_local_raft_resident_initialized: bool,
    connectivity_topology_subscription: Option<DaemonConnectivityTopologySubscriptions>,
    maintain_cluster_runtime: bool,
    maintain_default_dfs: bool,
    mut node_resources: sinomo_device::NodeResourceMonitor,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
    // This subscription is the durable membership/endpoint invalidation
    // source for connectivity. Retain it for exactly the maintenance owner's
    // lifetime; periodic probing remains only an expiry refresh mechanism.
    let _connectivity_topology_subscription = connectivity_topology_subscription;
    let mut cluster_network = ClusterNetworkMaintenance::with_idle_stats(
        idle_stats.clone(),
        startup_local_raft_resident_initialized,
    )
    .await;
    let mut current_node_resources = node_resources.take_latest();
    cluster_network.set_resource_mode(current_node_resources.mode);
    observe_daemon_node_resources(current_node_resources);
    let desired_rollout_task = if maintain_cluster_runtime && daemon_paths_are_shadow(&paths) {
        match cluster_paths_from_daemon_paths(&paths).and_then(|cluster_paths| {
            DaemonDesiredRolloutReactor::new(&cluster_paths).map(|reactor| (cluster_paths, reactor))
        }) {
            Ok((cluster_paths, reactor)) => {
                let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
                let reactor_paths = paths.clone();
                let reactor_runtime = runtime.clone();
                let task = runtime.spawn(reactor.run_until_stopped_async(
                    reactor_paths,
                    cluster_paths,
                    reactor_runtime,
                    stop_rx,
                ));
                Some((stop_tx, task))
            }
            Err(error) => {
                tracing::warn!(
                    "sinomo {label} could not initialize the durable rollout watch: {error}"
                );
                None
            }
        }
    } else {
        None
    };
    let maintenance_started = std::time::Instant::now();
    let initial_dfs_maintenance_phase = if maintain_default_dfs {
        let phase_paths = paths.clone();
        match tokio::task::spawn_blocking(move || {
            daemon_dfs_initial_maintenance_phase(&phase_paths)
        })
        .await
        {
            Ok(Ok(phase)) => phase,
            Ok(Err(error)) => {
                tracing::warn!("sinomo {label} could not phase initial DFS maintenance: {error}");
                Duration::ZERO
            }
            Err(error) => {
                tracing::warn!(
                    "sinomo {label} initial DFS maintenance phasing task failed: {error}"
                );
                Duration::ZERO
            }
        }
    } else {
        Duration::ZERO
    };
    let initial_dfs_maintenance_delay = daemon_dfs_initial_maintenance_delay(
        initial_dfs_maintenance_phase,
        maintain_cluster_runtime,
    );
    let initial_dfs_maintenance_not_before = maintenance_started
        .checked_add(initial_dfs_maintenance_delay)
        .unwrap_or(maintenance_started);
    let mut last_dfs_maintenance = maintenance_started
        .checked_sub(DAEMON_DFS_TRIGGERED_MAINTENANCE_MIN_INTERVAL)
        .unwrap_or(maintenance_started);
    // Probe once after startup to recover bounded durable work queues. A quiet
    // result parks indefinitely; committed-edge notifications and exact retry
    // deadlines are the normal wake sources.
    let mut next_dfs_maintenance_probe = Some(maintenance_started);
    #[cfg(feature = "sandbox")]
    let mut last_sandbox_lifecycle_maintenance = maintenance_started
        .checked_sub(DAEMON_SANDBOX_LIFECYCLE_MAINTENANCE_INTERVAL)
        .unwrap_or(maintenance_started);
    // A clustered replacement must publish its startup liveness before broad
    // sandbox recovery can consume a transport timeout reaching DFS peers.
    // Unclustered daemons retain the existing immediate recovery opportunity.
    let mut startup_cluster_maintenance_attempted = !maintain_cluster_runtime;
    let dfs_maintenance_notify = daemon_dfs_maintenance_notify();
    let cluster_maintenance_notify = daemon_cluster_maintenance_notify();
    let network_environment_watch = if label == "daemon" && maintain_cluster_runtime {
        start_daemon_network_environment_watch(&label, shutdown_rx.clone()).await
    } else {
        None
    };
    let mut dfs_triggered_maintenance_requested = false;
    let mut dfs_immediate_continuation_requested = false;
    let mut dfs_maintenance_task: Option<OwnedDaemonDfsMaintenanceTask> = None;

    loop {
        if daemon_shutdown_requested(&shutdown_rx) {
            break;
        }
        if maintain_cluster_runtime {
            match daemon_cluster_identity_matches_startup(&paths, startup_cluster.as_ref()) {
                Ok(true) => {}
                Ok(false) => {
                    tracing::info!(
                        "sinomo {label} background maintenance stopped after daemon identity changed"
                    );
                    break;
                }
                Err(error) => {
                    // Identity-bound work must fail closed while the durable
                    // state is unreadable, but an I/O or decode failure is not
                    // evidence that a replacement daemon owns this process.
                    // Retry instead of silently abandoning all cluster and DFS
                    // maintenance for the remainder of the daemon lifetime.
                    tracing::warn!(
                        "sinomo {label} background maintenance deferred while startup identity could not be verified: {error}"
                    );
                    let retry = tokio::select! {
                        _ = tokio::time::sleep(DAEMON_CLUSTER_TICK_INTERVAL) => true,
                        changed = shutdown_rx.changed() => {
                            changed.is_ok() && !daemon_shutdown_requested(&shutdown_rx)
                        }
                    };
                    if !retry {
                        break;
                    }
                    continue;
                }
            }
        }

        if label == "daemon" {
            let finalization_paths = paths.clone();
            match tokio::task::spawn_blocking(move || {
                handle_daemon_finalize_shadow_rollout(&finalization_paths)
            })
            .await
            {
                Ok(Ok(response)) if !response.starts_with("no pending shadow rollout") => {
                    tracing::info!("sinomo {label} rollout finalization: {response}");
                }
                Ok(Ok(_)) => {}
                Ok(Err(error)) => {
                    tracing::warn!(
                        "sinomo {label} shadow rollout finalization tick failed: {error}"
                    );
                }
                Err(error) => {
                    tracing::warn!(
                        "sinomo {label} shadow rollout finalization task failed: {error}"
                    );
                }
            }
        }

        if daemon_shutdown_requested(&shutdown_rx) {
            break;
        }

        if label == "daemon" && daemon_rollout_handoff_pending(&paths) {
            tokio::select! {
                _ = tokio::time::sleep(DAEMON_CLUSTER_TICK_INTERVAL) => {}
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || daemon_shutdown_requested(&shutdown_rx) {
                        break;
                    }
                }
            }
            continue;
        }

        #[cfg(feature = "sandbox")]
        if daemon_sandbox_lifecycle_maintenance_allowed(
            &label,
            startup_cluster_maintenance_attempted,
        ) && last_sandbox_lifecycle_maintenance.elapsed()
            >= DAEMON_SANDBOX_LIFECYCLE_MAINTENANCE_INTERVAL
        {
            let lifecycle_paths = paths.clone();
            let lifecycle_runtime = runtime.clone();
            match tokio::task::spawn_blocking(move || {
                run_daemon_sandbox_lifecycle_maintenance_tick(&lifecycle_paths, &lifecycle_runtime)
            })
            .await
            {
                Ok(Ok(recovered)) if recovered > 0 => tracing::info!(
                    "sinomo {label} recovered {recovered} pending sandbox lifecycle operation(s)"
                ),
                Ok(Ok(_)) => {}
                Ok(Err(error)) => {
                    tracing::warn!("sinomo {label} sandbox lifecycle maintenance failed: {error}")
                }
                Err(error) => tracing::warn!(
                    "sinomo {label} sandbox lifecycle maintenance task failed: {error}"
                ),
            }
            last_sandbox_lifecycle_maintenance = std::time::Instant::now();
        }

        if daemon_shutdown_requested(&shutdown_rx) {
            break;
        }

        if dfs_maintenance_task
            .as_ref()
            .is_some_and(OwnedDaemonDfsMaintenanceTask::is_finished)
        {
            let mut completed_task = dfs_maintenance_task
                .take()
                .expect("finished DFS maintenance task exists");
            let completed_outcome = completed_task.join(label.as_str()).await;
            match completed_outcome {
                Some(outcome) => {
                    // A committed repair/namespace edge can arrive while the
                    // previous bounded pass is still running. The notifier
                    // owns an immediate continuation independently of the
                    // older pass's report; do not demote that new work to the
                    // ordinary maintenance interval at join time.
                    let committed_edge_latched = dfs_immediate_continuation_requested;
                    let probe_paths = paths.clone();
                    let probe_runtime = runtime.clone();
                    let work = match tokio::task::spawn_blocking(move || {
                        daemon_dfs_pending_reactive_maintenance_work_on_runtime(
                            &probe_paths,
                            &probe_runtime,
                        )
                    })
                    .await
                    {
                        Ok(Ok(work)) => Some(work),
                        Ok(Err(error)) => {
                            tracing::warn!(
                                "sinomo {label} DFS maintenance follow-up probe failed: {error}"
                            );
                            None
                        }
                        Err(error) => {
                            tracing::warn!(
                                "sinomo {label} DFS maintenance follow-up probe task failed: {error}"
                            );
                            None
                        }
                    };
                    let work_for_policy = work.clone();
                    let decision = daemon_dfs_maintenance_completion_decision(
                        outcome,
                        Instant::now(),
                        daemon_dfs_now_unix_nanos(),
                        work,
                        committed_edge_latched,
                    );
                    let decision = daemon_dfs_resource_adjusted_probe_decision(
                        current_node_resources,
                        work_for_policy.as_ref(),
                        decision,
                    );
                    dfs_triggered_maintenance_requested = decision.trigger_maintenance;
                    dfs_immediate_continuation_requested = decision.immediate_continuation;
                    next_dfs_maintenance_probe = decision.next_probe_at;
                }
                None => {
                    let mut decision = daemon_dfs_failed_maintenance_decision(
                        Instant::now(),
                        next_dfs_maintenance_probe,
                    );
                    if crate::dfs::daemon_dfs_background_network_work_deferred(
                        current_node_resources,
                    ) {
                        decision.trigger_maintenance = false;
                        decision.immediate_continuation = false;
                    }
                    dfs_triggered_maintenance_requested = decision.trigger_maintenance;
                    dfs_immediate_continuation_requested = decision.immediate_continuation;
                    next_dfs_maintenance_probe = decision.next_probe_at;
                }
            }
            let completed_at = std::time::Instant::now();
            last_dfs_maintenance =
                if completed_outcome == Some(DaemonDfsMaintenanceTickOutcome::Deferred) {
                    completed_at
                        .checked_sub(
                            DAEMON_DFS_TRIGGERED_MAINTENANCE_MIN_INTERVAL
                                .saturating_sub(DAEMON_DFS_MAINTENANCE_LEASE_RETRY_DELAY),
                        )
                        .unwrap_or(completed_at)
                } else {
                    completed_at
                };
        }

        if daemon_dfs_maintenance_allowed(
            maintain_default_dfs,
            startup_cluster_maintenance_attempted,
        ) && dfs_maintenance_task.is_none()
            && !dfs_triggered_maintenance_requested
            && daemon_dfs_maintenance_probe_is_due(Instant::now(), next_dfs_maintenance_probe)
        {
            let probe_paths = paths.clone();
            let probe_runtime = runtime.clone();
            let work = match tokio::task::spawn_blocking(move || {
                daemon_dfs_pending_reactive_maintenance_work_on_runtime(
                    &probe_paths,
                    &probe_runtime,
                )
            })
            .await
            {
                Ok(Ok(work)) => Some(work),
                Ok(Err(error)) => {
                    tracing::warn!("sinomo {label} DFS maintenance probe failed: {error}");
                    None
                }
                Err(error) => {
                    tracing::warn!("sinomo {label} DFS maintenance probe task failed: {error}");
                    None
                }
            };
            apply_daemon_dfs_maintenance_probe_result(
                work,
                current_node_resources,
                &mut dfs_triggered_maintenance_requested,
                &mut dfs_immediate_continuation_requested,
                &mut next_dfs_maintenance_probe,
            );
        }

        if daemon_shutdown_requested(&shutdown_rx) {
            break;
        }

        let maintenance_launch_now = Instant::now();
        let dfs_triggered_maintenance_due = daemon_dfs_maintenance_due_in(
            maintenance_launch_now,
            initial_dfs_maintenance_not_before,
            last_dfs_maintenance,
            dfs_triggered_maintenance_requested,
            dfs_immediate_continuation_requested,
        )
        .is_some_and(|due_in| due_in.is_zero());
        if daemon_dfs_maintenance_allowed(
            maintain_default_dfs,
            startup_cluster_maintenance_attempted,
        ) && dfs_maintenance_task.is_none()
            && dfs_triggered_maintenance_due
            && daemon_dfs_maintenance_should_launch(
                dfs_triggered_maintenance_due,
                dfs_triggered_maintenance_requested,
            )
        {
            let dfs_paths = paths.clone();
            let dfs_runtime = runtime.clone();
            dfs_maintenance_task = Some(spawn_daemon_dfs_maintenance_task(
                dfs_paths,
                dfs_runtime,
                current_node_resources,
            ));
            dfs_triggered_maintenance_requested = false;
            dfs_immediate_continuation_requested = false;
        }

        if daemon_shutdown_requested(&shutdown_rx) {
            break;
        }

        if maintain_cluster_runtime {
            let cluster_network_phase_started = Instant::now();
            let mut tick_observation = DaemonClusterNetworkMaintenanceSubphaseObservation::start(
                DaemonClusterNetworkMaintenanceSubphase::TickPlan,
            );
            let Some(tick_result) = wait_for_daemon_maintenance_or_shutdown(
                tokio::time::timeout(
                    DAEMON_CLUSTER_NETWORK_MAINTENANCE_PHASE_TIMEOUT,
                    cluster_network.tick_async(&paths, &options, runtime.clone()),
                ),
                &mut shutdown_rx,
            )
            .await
            else {
                break;
            };
            let (pending_work, tick_timed_out) = match tick_result {
                Ok(Ok(pending_work)) => (pending_work, false),
                Ok(Err(error)) => {
                    cluster_network.record_active_tick_plan_failure(Instant::now());
                    tracing::warn!("sinomo {label} cluster network tick failed: {error}");
                    (None, false)
                }
                Err(_) => {
                    cluster_network.record_active_tick_plan_failure(Instant::now());
                    tracing::warn!(
                        "sinomo {label} cluster network tick planning timed out after {}ms",
                        DAEMON_CLUSTER_NETWORK_MAINTENANCE_PHASE_TIMEOUT.as_millis()
                    );
                    (None, true)
                }
            };
            tick_observation.complete(Some(pending_work.is_some()));
            drop(tick_observation);

            // The first bounded network pass has now had its opportunity to
            // publish node status. Restore the ordinary sandbox recovery
            // cadence even when planning was partial or timed out.
            startup_cluster_maintenance_attempted = true;

            if let Some(pending_work) = pending_work {
                let pending_work_deadline = Instant::now()
                    .checked_add(
                        DAEMON_CLUSTER_NETWORK_MAINTENANCE_PHASE_TIMEOUT
                            .saturating_sub(Duration::from_millis(500)),
                    )
                    .unwrap_or_else(Instant::now);
                let mut pending_work_observation =
                    DaemonClusterNetworkMaintenanceSubphaseObservation::start(
                        DaemonClusterNetworkMaintenanceSubphase::PendingWork,
                    );
                let Some(()) = wait_for_daemon_maintenance_or_shutdown(
                    cluster_network.run_pending_work_until_async(
                        pending_work,
                        runtime.clone(),
                        pending_work_deadline,
                    ),
                    &mut shutdown_rx,
                )
                .await
                else {
                    break;
                };
                pending_work_observation.complete(Some(true));
            }

            observe_cluster_network_maintenance_phase(
                if tick_timed_out {
                    DaemonClusterNetworkMaintenancePhaseDisposition::TimedOut
                } else {
                    DaemonClusterNetworkMaintenancePhaseDisposition::Completed
                },
                cluster_network_phase_started.elapsed(),
            );
        }

        if daemon_shutdown_requested(&shutdown_rx) {
            break;
        }

        let sleep_started = Instant::now();
        let shadow_finalization_due = if label == "daemon" {
            let due_paths = paths.clone();
            match tokio::task::spawn_blocking(move || {
                shadow_rollout_finalization_poll_due_in(&due_paths)
            })
            .await
            {
                Ok(due) => Some(due),
                Err(error) => {
                    tracing::warn!(
                        "sinomo {label} shadow rollout finalization due task failed: {error}"
                    );
                    None
                }
            }
        } else {
            None
        };
        let dfs_triggered_maintenance_due =
            if maintain_default_dfs && dfs_maintenance_task.is_none() {
                daemon_dfs_maintenance_due_in(
                    sleep_started,
                    initial_dfs_maintenance_not_before,
                    last_dfs_maintenance,
                    dfs_triggered_maintenance_requested,
                    dfs_immediate_continuation_requested,
                )
            } else {
                None
            };
        let dfs_maintenance_probe_due = if maintain_default_dfs
            && dfs_maintenance_task.is_none()
            && !dfs_triggered_maintenance_requested
        {
            next_dfs_maintenance_probe
                .map(|next_probe_at| next_probe_at.saturating_duration_since(sleep_started))
        } else {
            None
        };
        #[cfg(feature = "sandbox")]
        let sandbox_lifecycle_maintenance_due = daemon_sandbox_lifecycle_maintenance_allowed(
            &label,
            startup_cluster_maintenance_attempted,
        )
        .then(|| {
            daemon_maintenance_interval_remaining(
                sleep_started,
                last_sandbox_lifecycle_maintenance,
                DAEMON_SANDBOX_LIFECYCLE_MAINTENANCE_INTERVAL,
            )
        });
        #[cfg(not(feature = "sandbox"))]
        let sandbox_lifecycle_maintenance_due = None;
        // Give task completion its own bounded wakeup so a completed or
        // deferred pass is observed promptly even when all other maintenance
        // schedules are quiet.
        let dfs_maintenance_task_completion_due = dfs_maintenance_task
            .as_ref()
            .map(|_| DAEMON_DFS_MAINTENANCE_TASK_COMPLETION_POLL_INTERVAL);
        let sleep_duration = daemon_maintenance_sleep_duration(
            sleep_started,
            cluster_network.next_due_duration(sleep_started),
            dfs_triggered_maintenance_due,
            dfs_maintenance_probe_due,
            dfs_maintenance_task_completion_due,
            shadow_finalization_due,
            sandbox_lifecycle_maintenance_due,
        );
        let dfs_notify = maintain_default_dfs.then(|| Arc::clone(&dfs_maintenance_notify));
        let cluster_notify = Some(Arc::clone(&cluster_maintenance_notify));
        tokio::select! {
            _ = tokio::time::sleep(sleep_duration) => {}
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    break;
                }
            }
            _ = daemon_optional_notify(dfs_notify) => {
                let decision = if crate::dfs::daemon_dfs_background_network_work_deferred(
                    current_node_resources,
                ) {
                    sinomo_dfs::DfsMaintenanceProbeDecision {
                        trigger_maintenance: false,
                        immediate_continuation: false,
                        next_probe_at: Some(Instant::now()),
                    }
                } else {
                    daemon_dfs_maintenance_notification_decision(
                        Instant::now(),
                        dfs_triggered_maintenance_requested,
                        dfs_immediate_continuation_requested,
                        next_dfs_maintenance_probe,
                    )
                };
                dfs_triggered_maintenance_requested = decision.trigger_maintenance;
                dfs_immediate_continuation_requested = decision.immediate_continuation;
                next_dfs_maintenance_probe = decision.next_probe_at;
            }
            _ = daemon_optional_notify(cluster_notify) => {
                let dirty = take_daemon_cluster_maintenance_dirty();
                tracing::info!(dirty, "sinomo {label} cluster maintenance refresh requested");
                let now = Instant::now();
                if dirty & DAEMON_CLUSTER_MAINTENANCE_DIRTY_PUBLISH != 0 {
                    cluster_network.request_publish_refresh(now);
                }
                if dirty & DAEMON_CLUSTER_MAINTENANCE_DIRTY_CONNECTIVITY != 0 {
                    if dirty & DAEMON_CLUSTER_MAINTENANCE_DIRTY_NETWORK_ENVIRONMENT != 0 {
                        cluster_network.request_network_environment_refresh(now);
                    } else {
                        cluster_network.request_connectivity_refresh(now);
                    }
                }
                if dirty & DAEMON_CLUSTER_MAINTENANCE_DIRTY_DISCOVERY != 0 {
                    cluster_network.request_discovery_refresh(now);
                }
            }
            resources = node_resources.changed() => {
                let resource_policy_changed = current_node_resources != resources;
                current_node_resources = resources;
                cluster_network.set_resource_mode(resources.mode);
                observe_daemon_node_resources(resources);
                if resource_policy_changed {
                    // Reclassify the durable queue under the new policy. This
                    // wakes parked remote work when a restriction lifts and
                    // prevents a previously latched edge from bypassing a new
                    // restriction before its exact pending-work probe.
                    dfs_triggered_maintenance_requested = false;
                    dfs_immediate_continuation_requested = false;
                    next_dfs_maintenance_probe = Some(Instant::now());
                }
            }
        }
    }
    if let Some((stop_tx, task)) = desired_rollout_task {
        let _ = stop_tx.send(true);
        if let Err(error) = task.await {
            tracing::warn!("sinomo {label} durable rollout watch task failed: {error}");
        }
    }
    // Graceful shutdown drains a completed/cooperative pass. If the bounded
    // outer shutdown aborts this future, `OwnedDaemonDfsMaintenanceTask::drop`
    // explicitly aborts the async child, while any non-cancellable blocking
    // section keeps the data-owner lock until it actually exits.
    if let Some(mut task) = dfs_maintenance_task {
        let _ = task.join(label.as_str()).await;
    }
    if let Some(watch) = network_environment_watch {
        let _ = watch.task.await;
    }
}

fn daemon_cluster_identity_matches_startup(
    paths: &DaemonPaths,
    startup_cluster: Option<&DaemonClusterStatus>,
) -> Result<bool, DaemonError> {
    let Some(startup_cluster) = startup_cluster else {
        return Ok(false);
    };
    let cluster_paths = cluster_paths_from_daemon_paths(paths)?;
    match sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths) {
        Ok(state) => Ok(state.cluster_id == startup_cluster.cluster_id
            && state.local_node_id == startup_cluster.node_id),
        Err(sinomo_core::network::NetworkError::ClusterOrphan) => Ok(false),
        Err(error) => Err(DaemonError::from(error)),
    }
}

async fn wait_for_daemon_maintenance_or_shutdown<F>(
    maintenance: F,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
) -> Option<F::Output>
where
    F: Future,
{
    tokio::pin!(maintenance);
    loop {
        tokio::select! {
            biased;
            changed = shutdown_rx.changed() => {
                if changed.is_err() || daemon_shutdown_requested(shutdown_rx) {
                    return None;
                }
            }
            result = &mut maintenance => return Some(result),
        }
    }
}

pub(crate) async fn run_daemon_control_accept_loop(
    listener: sinomo_core::network::AsyncDaemonControlListener,
    paths: DaemonPaths,
    label: String,
    bind_address: String,
    service_host: crate::service_adapter::DaemonServiceRegistryHost,
    service_runtime: tokio::runtime::Handle,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> Result<(), DaemonError> {
    let cluster_paths = cluster_paths_from_daemon_paths(&paths)?;
    let mut routed_stream_incoming =
        match sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths) {
            Ok(state) => Some(sinomo_core::network::register_routed_stream_incoming(
                state.local_node_id,
            )?),
            Err(sinomo_core::network::NetworkError::ClusterOrphan) => None,
            Err(error) => return Err(error.into()),
        };
    loop {
        if *shutdown_rx.borrow() {
            break;
        }
        // A daemon can acquire its cluster identity while this listener is
        // still serving the final requests of the pre-join process. Register
        // the reverse logical-stream dispatcher as soon as that identity
        // becomes visible so a persistent carrier cannot observe a clustered
        // endpoint with no service sink.
        if routed_stream_incoming.is_none() {
            match sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths) {
                Ok(state) => {
                    routed_stream_incoming = Some(
                        sinomo_core::network::register_routed_stream_incoming(state.local_node_id)?,
                    );
                }
                Err(sinomo_core::network::NetworkError::ClusterOrphan) => {}
                Err(error) => return Err(error.into()),
            }
        }
        tokio::select! {
            biased;

            changed = shutdown_rx.changed() => {
                match changed {
                    Ok(()) if *shutdown_rx.borrow() => break,
                    Ok(()) => continue,
                    Err(_) => break,
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(100)), if routed_stream_incoming.is_none() => {
                continue;
            }
            routed = async {
                match routed_stream_incoming.as_mut() {
                    Some(incoming) => incoming.accept().await,
                    None => std::future::pending().await,
                }
            } => {
                match routed {
                    Ok(Some(stream)) => {
                        let client_paths = paths.clone();
                        let client_label = label.clone();
                        let client_service_host = service_host.clone();
                        let client_service_runtime = service_runtime.clone();
                        let client_shutdown_tx = shutdown_tx.clone();
                        tokio::spawn(async move {
                            match handle_daemon_peer_opened_routed_stream_async(
                                stream,
                                client_paths,
                                client_service_host,
                                client_service_runtime,
                            )
                            .await
                            {
                                Ok(true) => {
                                    let _ = client_shutdown_tx.send(true);
                                }
                                Ok(false) => {}
                                Err(error) => tracing::warn!(
                                    "sinomo {client_label} peer-opened routed stream error: {error}"
                                ),
                            }
                        });
                    }
                    Ok(None) => routed_stream_incoming = None,
                    Err(error) => tracing::warn!(
                        "sinomo {label} routed stream dispatcher error: {error}"
                    ),
                }
            }
            accepted = listener.accept() => {
                let stream = accepted.map_err(|source| DaemonError::Network {
                    address: bind_address.clone(),
                    detail: source.to_string(),
                })?;
                let client_paths = paths.clone();
                let client_label = label.clone();
                let client_service_host = service_host.clone();
                let client_service_runtime = service_runtime.clone();
                let client_shutdown_tx = shutdown_tx.clone();
                let client_address = bind_address.clone();
                tokio::spawn(async move {
                    match handle_daemon_connection_async(
                        stream,
                        client_paths,
                        client_service_host,
                        client_service_runtime,
                        client_address,
                    )
                    .await
                    {
                        Ok(true) => {
                            let _ = client_shutdown_tx.send(true);
                        }
                        Ok(false) => {}
                        Err(error) => tracing::warn!("sinomo {client_label} client error: {error}"),
                    }
                });
            }
        }
    }
    Ok(())
}

async fn handle_daemon_peer_opened_routed_stream_async(
    stream: sinomo_core::network::PeerStream,
    paths: DaemonPaths,
    service_host: crate::service_adapter::DaemonServiceRegistryHost,
    service_runtime: tokio::runtime::Handle,
) -> Result<bool, DaemonError> {
    let cluster_paths = cluster_paths_from_daemon_paths(&paths)?;
    let state = sinomo_core::cluster::read_cluster_state_without_kv_cached_shared(&cluster_paths)?;
    let directory = sinomo_core::network::cluster_directory_with_bootstrap_state(
        sinomo_core::network::cluster_directory_for(&cluster_paths),
        &state,
    )
    .snapshot()?;
    handle_daemon_stream_native_service_rpc_async(
        stream,
        directory,
        paths,
        service_host,
        service_runtime,
    )
    .await
}

pub(crate) async fn handle_daemon_connection_async(
    stream: sinomo_core::network::AsyncDaemonControlStream,
    paths: DaemonPaths,
    service_host: crate::service_adapter::DaemonServiceRegistryHost,
    service_runtime: tokio::runtime::Handle,
    stream_address: String,
) -> Result<bool, DaemonError> {
    if daemon_control_stream_uses_authenticated_transport_prelude(&stream).await? {
        return handle_daemon_authenticated_transport_stream_async(
            stream,
            paths,
            service_host,
            service_runtime,
            stream_address,
        )
        .await;
    }
    if daemon_control_stream_uses_local_object_rpc_carrier(&stream).await? {
        return handle_daemon_local_object_rpc_carrier_async(
            stream,
            paths,
            service_host,
            service_runtime,
            stream_address,
        )
        .await;
    }
    // Raw framing is reserved for bootstrap handshakes. Consume the daemon
    // control wrapper at this ownership boundary so ordinary RPC code cannot
    // accidentally regain a generic raw-frame transport.
    let raw_frame_started_ns = sinomo_core::network::network_timeline_capture_is_active()
        .then(sinomo_core::network::network_timeline_monotonic_ns)
        .unwrap_or(0);
    let mut stream = stream.into_inner();
    let request = match read_daemon_control_frame_async(&mut stream).await {
        Ok(request) => request,
        Err(_) => {
            let _ = stream.shutdown().await;
            return Ok(false);
        }
    };
    if let Some(timeline) =
        sinomo_core::network::network_timeline_trace_context_for_request(&request)
    {
        sinomo_core::network::record_network_timeline_event_now(
            sinomo_core::network::NetworkTimelineEvent::new(
                timeline,
                sinomo_core::network::network_timeline_stage::FRAME_INGRESS,
                sinomo_core::network::network_timeline_flag::REQUEST
                    | sinomo_core::network::network_timeline_flag::SUCCESS,
                0,
                0,
                1,
                u32::try_from(request.body.len()).unwrap_or(u32::MAX),
                0,
                sinomo_core::network::network_timeline_monotonic_ns()
                    .saturating_sub(raw_frame_started_ns),
            ),
        );
    }
    if request.route == sinomo_core::network::CLUSTER_JOIN_CHALLENGE_ROUTE
        && request.category == sinomo_core::network::CLUSTER_MEMBERSHIP_RPC_CATEGORY
    {
        let handled = crate::service_adapter::try_dispatch_pre_auth_via_registry_async(
            &request,
            &mut stream,
            &service_host,
        )
        .await?;
        if !handled {
            tracing::warn!(
                route = %request.route,
                "daemon pre-cluster bootstrap handshake had no registered handler"
            );
        }
        let _ = stream.shutdown().await;
        return Ok(false);
    }

    if request.route == sinomo_core::network::CLUSTER_JOIN_REGISTER_ROUTE
        && request.category == sinomo_core::network::CLUSTER_MEMBERSHIP_RPC_CATEGORY
    {
        let authenticated =
            match crate::service_adapter::authenticate_daemon_control_request_via_registry(
                &paths,
                &request,
                &service_host,
            ) {
                Ok(authenticated) => authenticated,
                Err(error) => {
                    crate::cluster_join_trace(format_args!(
                        "raw join registration auth denied: {error}"
                    ));
                    if let Ok(response) = sinomo_core::network::ObjectRpcResponse::encode_error(
                        request.route.clone(),
                        DaemonError::AuthDenied.to_string(),
                    ) {
                        let _ = write_daemon_rpc_response_frame_async(&mut stream, response).await;
                    }
                    let _ = stream.shutdown().await;
                    return Ok(false);
                }
            };
        let route = request.route.clone();
        let (response, stop) = crate::service_adapter::dispatch_rpc_via_registry_to_response_async(
            &service_host,
            &authenticated,
            request,
            Some(&paths),
        )
        .await?;
        write_daemon_rpc_response_frame_async(&mut stream, response).await?;
        let _ = stream.shutdown().await;
        crate::service_adapter::mark_cluster_identity_restart_after_response(
            &route,
            stop,
            Some(&paths),
        );
        return Ok(stop);
    }

    tracing::debug!(
        route = %request.route,
        category = %request.category,
        "rejecting raw object frame; ordinary object RPC requires a stream-native call"
    );
    let _ = stream.shutdown().await;
    Ok(false)
}

async fn daemon_control_stream_uses_authenticated_transport_prelude(
    stream: &sinomo_core::network::AsyncDaemonControlStream,
) -> Result<bool, DaemonError> {
    let mut prefix = [0_u8; 4];
    let deadline = tokio::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    loop {
        let available = tokio::time::timeout_at(deadline, stream.peek_prefix(&mut prefix))
            .await
            .map_err(|_| {
                DaemonError::from(sinomo_core::network::NetworkError::timeout(
                    "peek authenticated transport prelude",
                    sinomo_core::network::NetworkTimeoutPhase::Idle,
                    sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
                    sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent,
                ))
            })??;
        if available == 0 {
            return Ok(false);
        }
        if available >= prefix.len() {
            return Ok(
                sinomo_core::network::authenticated_transport_prelude_prefix_matches(&prefix),
            );
        }
        if !sinomo_core::network::authenticated_transport_prelude_prefix_matches(
            &prefix[..available],
        ) {
            return Ok(false);
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

fn resolve_authenticated_transport_peer(
    paths: &DaemonPaths,
    state: &sinomo_core::ClusterBootstrapState,
    metadata: &sinomo_core::network::AuthenticatedTransportMetadata,
    local_node_id: &NodeId,
    identity: &dyn sinomo_core::network::RoutedSessionIdentity,
) -> Result<(NodeId, sinomo_core::network::DirectorySnapshot), DaemonError> {
    let cluster_paths = cluster_paths_from_daemon_paths(paths)?;
    let directory = sinomo_core::network::cluster_directory_with_bootstrap_state(
        sinomo_core::network::cluster_directory_for(&cluster_paths),
        state,
    )
    .snapshot()?;
    if matches!(
        metadata.transport,
        sinomo_core::base::ClusterTransportKind::Loopback
            | sinomo_core::base::ClusterTransportKind::LanTcp
    ) {
        let peer_node_id = sinomo_core::network::verify_builtin_authenticated_transport_metadata(
            metadata,
            local_node_id,
            &directory,
            identity,
        )?;
        return Ok((peer_node_id, directory));
    }
    let mut matching_nodes = BTreeSet::new();
    for node in directory.nodes().values() {
        if node.endpoints.iter().any(|endpoint| {
            sinomo_core::network::authenticated_transport_metadata_matches_endpoint(
                metadata, endpoint,
            )
        }) {
            matching_nodes.insert(node.node_id.clone());
        }
    }
    if matching_nodes.len() != 1 {
        tracing::warn!(
            transport = ?metadata.transport,
            matching_nodes = matching_nodes.len(),
            "authenticated transport principal did not resolve to exactly one authorized node"
        );
        return Err(DaemonError::AuthDenied);
    }
    Ok((
        matching_nodes
            .into_iter()
            .next()
            .expect("one authenticated transport node"),
        directory,
    ))
}

struct DaemonRoutedConnectivityFailureSink<'a> {
    cluster_paths: &'a sinomo_core::network::ClusterPaths,
    state: &'a sinomo_core::ClusterBootstrapState,
    directory: &'a sinomo_core::network::DirectorySnapshot,
}

impl sinomo_core::network::ConnectivityFailureSink for DaemonRoutedConnectivityFailureSink<'_> {
    fn submit_edge_failure(
        &self,
        evidence: sinomo_core::network::DirectConnectivityFailureEvidence,
    ) -> Result<(), sinomo_core::network::NetworkError> {
        if evidence.source_node_id != self.state.local_node_id
            || evidence.observer_node_id != self.state.local_node_id
        {
            return Err(sinomo_core::network::NetworkError::AuthDenied);
        }
        let endpoint = self
            .directory
            .node(&evidence.target_node_id)
            .and_then(|node| {
                node.endpoints
                    .iter()
                    .find(|endpoint| endpoint.endpoint_id == evidence.endpoint_id)
            })
            .ok_or(sinomo_core::network::NetworkError::AuthDenied)?;
        tracing::warn!(
            source_node_id = %evidence.source_node_id,
            target_node_id = %evidence.target_node_id,
            endpoint_id = %evidence.endpoint_id,
            reason = %evidence.reason,
            "routed-session realization rejected a direct edge"
        );
        sinomo_core::network::__connectivity_store::record_daemon_owned_local_cluster_endpoint_command_connectivity(
            self.cluster_paths,
            self.state,
            endpoint,
            sinomo_core::network::ClusterConnectivityState::Unreachable,
            None,
            false,
        )?;
        Ok(())
    }
}

async fn handle_daemon_authenticated_transport_stream_async(
    stream: sinomo_core::network::AsyncDaemonControlStream,
    paths: DaemonPaths,
    service_host: crate::service_adapter::DaemonServiceRegistryHost,
    service_runtime: tokio::runtime::Handle,
    stream_address: String,
) -> Result<bool, DaemonError> {
    let mut stream = stream.into_inner();
    let metadata = sinomo_core::network::read_authenticated_transport_prelude(&mut stream).await?;
    let cluster_paths = cluster_paths_from_daemon_paths(&paths)?;
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)?;
    let local_node_id = state.local_node_id.clone();
    let data_dir = sinomo_core::local::SinomoDataDir::new(daemon_data_dir_path(&paths)?);
    let identity = Arc::new(sinomo_core::network::DaemonRoutedSessionIdentity::new(
        local_node_id.clone(),
        data_dir,
    )?);
    let (peer_node_id, directory) = resolve_authenticated_transport_peer(
        &paths,
        &state,
        &metadata,
        &local_node_id,
        identity.as_ref(),
    )?;
    let mut link_identity = Vec::new();
    link_identity.extend_from_slice(local_node_id.as_str().as_bytes());
    link_identity.push(0);
    link_identity.extend_from_slice(peer_node_id.as_str().as_bytes());
    link_identity.push(0);
    link_identity.extend_from_slice(&metadata.channel_binding);
    let link_id = sinomo_core::Id::derived(
        "sinomo.network.accepted-module-authenticated-link",
        &link_identity,
    );
    let direct_stream = sinomo_core::network::AuthenticatedDirectStream::accepted(
        Box::new(stream),
        local_node_id.clone(),
        peer_node_id.clone(),
        link_id,
        metadata.channel_binding,
        metadata.capabilities,
    )?;
    let link_provider = sinomo_core::network::ClusterLinkProvider::new(
        local_node_id,
        identity.clone(),
        sinomo_core::network::NetworkHealthPolicy::default(),
    )?;
    let failure_sink = DaemonRoutedConnectivityFailureSink {
        cluster_paths: &cluster_paths,
        state: &state,
        directory: &directory,
    };
    let routed_pool_scopes =
        sinomo_core::network::cluster_routed_stream_pool_scopes_from_fenced_bootstrap_state(
            &state,
        )?
        .into_iter()
        .collect::<Vec<_>>();
    match sinomo_core::network::accept_or_relay_routed_session(
        direct_stream,
        &directory,
        identity.as_ref(),
        &link_provider,
        Some(&failure_sink),
    )
    .await?
    {
        sinomo_core::network::RoutedSessionDisposition::Destination(stream) => {
            let routed_peer_node_id = stream.peer_node_id.clone();
            tracing::debug!(
                adjacent_peer_node_id = %peer_node_id,
                %routed_peer_node_id,
                transport = ?metadata.transport,
                %stream_address,
                "accepted authenticated routed-session carrier"
            );
            handle_daemon_routed_stream_carrier_async(
                stream,
                directory,
                paths,
                service_host,
                service_runtime,
                routed_pool_scopes,
            )
            .await
        }
        sinomo_core::network::RoutedSessionDisposition::Relayed(report) => {
            tracing::debug!(
                session_id = %report.session_id,
                source_node_id = %report.source_node_id,
                destination_node_id = %report.destination_node_id,
                relay_node_id = %report.relay_node_id,
                next_node_id = %report.next_node_id,
                bytes_toward_destination = report.bytes_toward_destination,
                bytes_toward_source = report.bytes_toward_source,
                %stream_address,
                "completed opaque routed-session relay"
            );
            Ok(false)
        }
    }
}

async fn handle_daemon_routed_stream_carrier_async(
    carrier: sinomo_core::network::PeerStream,
    directory: sinomo_core::network::DirectorySnapshot,
    paths: DaemonPaths,
    service_host: crate::service_adapter::DaemonServiceRegistryHost,
    service_runtime: tokio::runtime::Handle,
    routed_pool_scopes: Vec<String>,
) -> Result<bool, DaemonError> {
    let destination_process_affinity = if daemon_paths_are_shadow(&paths) {
        sinomo_core::network::DestinationProcessAffinity::Any
    } else {
        sinomo_core::network::DestinationProcessAffinity::MainDaemon
    };
    let mut acceptor = sinomo_core::network::accept_pooled_routed_stream_carrier_for_scopes_with_destination_process_affinity(
        routed_pool_scopes,
        directory.version().0,
        sinomo_core::network::stream_native_service_route_constraints(),
        destination_process_affinity,
        carrier,
    )
    .await?;
    let mut handlers = tokio::task::JoinSet::new();
    let mut carrier_open = true;
    while carrier_open || !handlers.is_empty() {
        if !carrier_open {
            let Some(joined) = handlers.join_next().await else {
                break;
            };
            match joined {
                Ok(Ok(true)) => return Ok(true),
                Ok(Ok(false)) => continue,
                Ok(Err(error)) => {
                    tracing::warn!(%error, "routed logical service RPC failed");
                    continue;
                }
                Err(error) => {
                    tracing::warn!(%error, "routed logical service RPC task failed");
                    continue;
                }
            }
        }

        tokio::select! {
            accepted = acceptor.accept() => {
                match accepted? {
                    Some(stream) => {
                        let handler_directory = directory.clone();
                        let handler_paths = paths.clone();
                        let handler_host = service_host.clone();
                        let handler_runtime = service_runtime.clone();
                        handlers.spawn(async move {
                            handle_daemon_stream_native_service_rpc_async(
                                stream,
                                handler_directory,
                                handler_paths,
                                handler_host,
                                handler_runtime,
                            )
                            .await
                        });
                    }
                    None => carrier_open = false,
                }
            }
            joined = handlers.join_next(), if !handlers.is_empty() => {
                let Some(joined) = joined else {
                    continue;
                };
                match joined {
                    Ok(Ok(true)) => {
                        handlers.abort_all();
                        return Ok(true);
                    }
                    Ok(Ok(false)) => {}
                    Ok(Err(error)) => {
                        tracing::warn!(%error, "routed logical service RPC failed");
                    }
                    Err(error) => {
                        tracing::warn!(%error, "routed logical service RPC task failed");
                    }
                }
            }
        }
    }
    Ok(false)
}

async fn daemon_control_stream_uses_local_object_rpc_carrier(
    stream: &sinomo_core::network::AsyncDaemonControlStream,
) -> Result<bool, DaemonError> {
    let mut prefix = [0_u8; 4];
    let deadline = tokio::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    loop {
        let available = tokio::time::timeout_at(deadline, stream.peek_prefix(&mut prefix))
            .await
            .map_err(|_| {
                DaemonError::from(sinomo_core::network::NetworkError::timeout(
                    "peek daemon local object RPC carrier protocol",
                    sinomo_core::network::NetworkTimeoutPhase::Idle,
                    sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
                    sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent,
                ))
            })??;
        if available == 0 {
            return Ok(false);
        }
        if available >= prefix.len() {
            return Ok(
                sinomo_core::network::local_object_rpc_carrier_prelude_prefix_matches(&prefix),
            );
        }
        if !sinomo_core::network::local_object_rpc_carrier_prelude_prefix_matches(
            &prefix[..available],
        ) {
            return Ok(false);
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

async fn handle_daemon_local_object_rpc_carrier_async(
    stream: sinomo_core::network::AsyncDaemonControlStream,
    paths: DaemonPaths,
    service_host: crate::service_adapter::DaemonServiceRegistryHost,
    service_runtime: tokio::runtime::Handle,
    stream_address: String,
) -> Result<bool, DaemonError> {
    if !stream.peer_addr()?.ip().is_loopback() {
        return Err(DaemonError::AuthDenied);
    }
    let mut stream = stream.into_inner();
    let context = sinomo_core::network::read_local_object_rpc_carrier_prelude(&mut stream).await?;
    sinomo_core::network::serve_local_object_rpc_stream_carrier(
        Box::new(stream),
        &context,
        move |stream| {
            let handler_paths = paths.clone();
            let handler_host = service_host.clone();
            let handler_runtime = service_runtime.clone();
            let handler_address = stream_address.clone();
            async move {
                match handle_daemon_pooled_local_stream_native_service_rpc_async(
                    stream,
                    handler_paths,
                    handler_host,
                    handler_runtime,
                    handler_address,
                )
                .await
                {
                    Ok(stop) => Ok(stop),
                    Err(error) => {
                        tracing::warn!(%error, "local logical service RPC failed");
                        Ok(false)
                    }
                }
            }
        },
    )
    .await
    .map_err(DaemonError::from)
}

async fn handle_daemon_pooled_local_stream_native_service_rpc_async(
    stream: sinomo_core::network::PeerStream,
    paths: DaemonPaths,
    service_host: crate::service_adapter::DaemonServiceRegistryHost,
    service_runtime: tokio::runtime::Handle,
    stream_address: String,
) -> Result<bool, DaemonError> {
    let context = sinomo_core::network::RpcPeerContext::unauthenticated_local(
        stream.session_id.clone(),
        stream.channel_binding,
    );
    handle_daemon_local_stream_native_service_rpc_with_context_async(
        stream,
        context,
        paths,
        service_host,
        service_runtime,
        stream_address,
    )
    .await
}

async fn handle_daemon_local_stream_native_service_rpc_with_context_async(
    stream: sinomo_core::network::PeerStream,
    context: sinomo_core::network::RpcPeerContext,
    paths: DaemonPaths,
    service_host: crate::service_adapter::DaemonServiceRegistryHost,
    service_runtime: tokio::runtime::Handle,
    _stream_address: String,
) -> Result<bool, DaemonError> {
    let data_dir = sinomo_core::local::SinomoDataDir::new(daemon_data_dir_path(&paths)?);
    let local_public_key = sinomo_core::network::cached_daemon_signing_public_key(&data_dir)
        .map_err(DaemonError::from)?;
    let local_public_key = sinomo_core::network::daemon_auth_encode_public_key(&local_public_key);
    let responder_node_id = cluster_paths_from_daemon_paths(&paths)
        .ok()
        .and_then(|cluster_paths| {
            sinomo_core::cluster::read_cluster_state_without_kv_cached_shared(&cluster_paths).ok()
        })
        .map(|state| Arc::<str>::from(state.local_node_id.as_str()));
    let local_node_id = responder_node_id
        .as_deref()
        .map(str::to_string)
        .unwrap_or_else(|| {
            NodeId::derived(
                "sinomo.network.local-object-rpc-stream",
                local_public_key.as_bytes(),
            )
            .to_string()
        });
    let completion = Arc::new(Mutex::new(("<unavailable>".to_string(), false)));
    let handler_completion = Arc::clone(&completion);
    let handler_paths = paths.clone();
    let handler_host = service_host.clone();
    let handler_runtime = service_runtime.clone();
    let handler = move |request: sinomo_core::network::ObjectRpcIncomingRequest,
                        peer: sinomo_core::network::RpcPeerContext| async move {
        if !matches!(
            peer.identity,
            sinomo_core::network::RpcPeerIdentity::UnauthenticatedLocal
        ) {
            return Err(sinomo_core::network::NetworkError::AuthDenied);
        }
        let route = request.request.route.clone();
        let authenticated = crate::AuthenticatedDaemonCommand {
            route: route.clone(),
            signer: DaemonSignerAuthority::Local,
            signer_node_id: local_node_id.clone(),
            signer_public_key: local_public_key.clone(),
        };
        let dispatch = dispatch_daemon_object_rpc_call_request_async(
            &handler_paths,
            &handler_host,
            &handler_runtime,
            &authenticated,
            request,
        )
        .await;
        let (mut response, stop, effective_route) = match dispatch {
            Ok(response) => response,
            Err(error) => (
                sinomo_core::network::ObjectRpcOutgoingResponse::value(
                    sinomo_core::network::ObjectRpcResponse::encode_error(
                        &route,
                        error.to_string(),
                    )?,
                ),
                false,
                route,
            ),
        };
        response.response.mark_responder_node_id(local_node_id);
        *handler_completion
            .lock()
            .map_err(|_| sinomo_core::network::NetworkError::Io {
                operation: "record local stream-native service completion".to_string(),
                detail: "service completion lock poisoned".to_string(),
            })? = (effective_route, stop);
        Ok(response)
    };
    let result = sinomo_core::network::serve_service_rpc_peer_stream(
        stream,
        &context,
        sinomo_core::network::ObjectFrameIoOptions::default(),
        handler,
    )
    .await
    .map_err(DaemonError::from);
    let (effective_route, stop) = completion
        .lock()
        .map_err(|_| DaemonError::Parse {
            detail: "local stream-native service completion lock poisoned".to_string(),
        })?
        .clone();
    finish_daemon_object_rpc_stream_response(&effective_route, stop, &paths, result)
}

/// Final service boundary for one authenticated routed stream. The transport
/// identity is the request authority; the service registry applies route and
/// node-local policy without signing or hashing each RPC.
async fn handle_daemon_stream_native_service_rpc_async(
    stream: sinomo_core::network::PeerStream,
    directory: sinomo_core::network::DirectorySnapshot,
    paths: DaemonPaths,
    service_host: crate::service_adapter::DaemonServiceRegistryHost,
    service_runtime: tokio::runtime::Handle,
) -> Result<bool, DaemonError> {
    let peer_node_id = stream.peer_node_id.clone();
    let local_node_id = stream.local_node_id.clone();
    let context = sinomo_core::network::RpcPeerContext::authenticated(
        local_node_id.clone(),
        peer_node_id.clone(),
        stream.session_id.clone(),
        stream.channel_binding,
    )?;
    let peer_directory_node = directory.node(&peer_node_id).ok_or_else(|| {
        sinomo_core::network::NetworkError::UnauthorizedNode {
            node_id: peer_node_id.clone(),
        }
    })?;
    let peer_public_key_encoded = peer_directory_node.public_key.clone();
    let peer_is_raft_voter = peer_directory_node.has_raft_vote();
    let completion = Arc::new(Mutex::new(("<unavailable>".to_string(), false)));
    let handler_completion = Arc::clone(&completion);
    let handler_paths = paths.clone();
    let handler_host = service_host.clone();
    let handler_runtime = service_runtime.clone();
    let handler_peer_node_id = peer_node_id.clone();
    let handler_peer_public_key = peer_public_key_encoded;
    let result = sinomo_core::network::serve_service_rpc_peer_stream(
        stream,
        &context,
        sinomo_core::network::ObjectFrameIoOptions::default(),
        move |request, peer| async move {
            let timeline =
                sinomo_core::network::network_timeline_trace_context_for_request(&request.request);
            let auth_started_ns = timeline
                .map(|_| sinomo_core::network::network_timeline_monotonic_ns())
                .unwrap_or(0);
            let context_peer = match peer.identity {
                sinomo_core::network::RpcPeerIdentity::Authenticated { peer_node_id, .. } => {
                    peer_node_id
                }
                sinomo_core::network::RpcPeerIdentity::UnauthenticatedLocal => {
                    return Err(sinomo_core::network::NetworkError::AuthDenied);
                }
            };
            let authenticated_result = (context_peer == handler_peer_node_id)
                .then(|| crate::AuthenticatedDaemonCommand {
                    route: request.request.route.clone(),
                    signer: if peer_is_raft_voter {
                        DaemonSignerAuthority::Voter
                    } else {
                        DaemonSignerAuthority::Member
                    },
                    signer_node_id: context_peer.to_string(),
                    signer_public_key: handler_peer_public_key.clone(),
                })
                .ok_or(sinomo_core::network::NetworkError::AuthDenied);
            record_daemon_stream_timeline_stage(
                timeline,
                sinomo_core::network::network_timeline_stage::DAEMON_AUTH,
                auth_started_ns,
                authenticated_result.is_ok(),
            );
            let authenticated = authenticated_result?;
            let route = request.request.route.clone();
            let dispatch_started_ns = timeline
                .map(|_| sinomo_core::network::network_timeline_monotonic_ns())
                .unwrap_or(0);
            let dispatch = dispatch_daemon_object_rpc_call_request_async(
                &handler_paths,
                &handler_host,
                &handler_runtime,
                &authenticated,
                request,
            )
            .await;
            record_daemon_stream_timeline_stage(
                timeline,
                sinomo_core::network::network_timeline_stage::DAEMON_DISPATCH,
                dispatch_started_ns,
                dispatch.is_ok(),
            );
            let finalize_started_ns = timeline
                .map(|_| sinomo_core::network::network_timeline_monotonic_ns())
                .unwrap_or(0);
            let (mut response, stop, effective_route) = match dispatch {
                Ok(response) => response,
                Err(error) => (
                    sinomo_core::network::ObjectRpcOutgoingResponse::value(
                        sinomo_core::network::ObjectRpcResponse::encode_error(
                            &route,
                            error.to_string(),
                        )?,
                    ),
                    false,
                    route,
                ),
            };
            response
                .response
                .mark_responder_node_id(local_node_id.to_string());
            *handler_completion
                .lock()
                .map_err(|_| sinomo_core::network::NetworkError::Io {
                    operation: "record stream-native service completion".to_string(),
                    detail: "service completion lock poisoned".to_string(),
                })? = (effective_route, stop);
            record_daemon_stream_timeline_stage(
                timeline,
                sinomo_core::network::network_timeline_stage::DAEMON_RESPONSE_FINALIZE,
                finalize_started_ns,
                true,
            );
            Ok(response)
        },
    )
    .await
    .map_err(DaemonError::from);
    let (effective_route, stop) = completion
        .lock()
        .map_err(|_| DaemonError::Parse {
            detail: "stream-native service completion lock poisoned".to_string(),
        })?
        .clone();
    finish_daemon_object_rpc_stream_response(&effective_route, stop, &paths, result)
}

fn finish_daemon_object_rpc_stream_response(
    effective_route: &str,
    stop: bool,
    paths: &DaemonPaths,
    respond_result: Result<(), DaemonError>,
) -> Result<bool, DaemonError> {
    // Dispatch has already committed an identity-changing operation when it
    // returns `stop`. Preserve the supervisor restart request even when the
    // peer disconnects before receiving that committed response, then report
    // the transport failure to the connection task.
    crate::service_adapter::mark_cluster_identity_restart_after_response(
        effective_route,
        stop,
        Some(paths),
    );
    respond_result?;
    Ok(stop)
}

fn record_daemon_stream_timeline_stage(
    timeline: Option<sinomo_core::network::NetworkTimelineTraceContext>,
    stage: u16,
    started_ns: u64,
    success: bool,
) {
    let Some(trace) = timeline else {
        return;
    };
    sinomo_core::network::record_network_timeline_event_now(
        sinomo_core::network::NetworkTimelineEvent::new(
            trace,
            stage,
            sinomo_core::network::network_timeline_flag::REQUEST
                | if success {
                    sinomo_core::network::network_timeline_flag::SUCCESS
                } else {
                    0
                },
            0,
            0,
            0,
            0,
            0,
            sinomo_core::network::network_timeline_monotonic_ns().saturating_sub(started_ns),
        ),
    );
}

async fn dispatch_daemon_object_rpc_call_request_async(
    paths: &DaemonPaths,
    service_host: &crate::service_adapter::DaemonServiceRegistryHost,
    service_runtime: &tokio::runtime::Handle,
    authenticated: &AuthenticatedDaemonCommand,
    request: sinomo_core::network::ObjectRpcIncomingRequest,
) -> Result<
    (
        sinomo_core::network::ObjectRpcOutgoingResponse,
        bool,
        String,
    ),
    DaemonError,
> {
    let route = request.request.route.clone();
    if route == sinomo_core::network::CLUSTER_NETWORK_PING_ROUTE {
        let timeline =
            sinomo_core::network::network_timeline_trace_context_for_request(&request.request);
        let data_dir = sinomo_core::local::SinomoDataDir::new(daemon_data_dir_path(paths)?);
        let response =
            crate::service_adapter::handle_daemon_cluster_network_ping_request_with_timeline(
                paths, &data_dir, request, timeline,
            )
            .await
            .map_err(DaemonError::from)?;
        Ok((response, false, route))
    } else if route == crate::DAEMON_OBJECT_RPC_FORWARD_ROUTE
        || route == crate::DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE
        || route == crate::DAEMON_OBJECT_RPC_FORWARD_MAIN_ROUTE
        || route == crate::DAEMON_DFS_OBJECT_RPC_FORWARD_MAIN_ROUTE
    {
        let timeline =
            sinomo_core::network::network_timeline_trace_context_for_request(&request.request);
        // This route is the trusted local API-to-daemon proxy boundary. The
        // session admission immediately above this dispatcher has already
        // applied the live route descriptor's local-command
        // authorization. Keep the remaining route-policy invariant here while
        // avoiding a second router/catalog/authorization traversal on every
        // small remote RPC.
        if request.request.category != crate::DAEMON_RPC_CATEGORY {
            return Err(sinomo_core::network::NetworkError::Serialization {
                detail: format!(
                    "object rpc route {route} expected category {}, got {}",
                    crate::DAEMON_RPC_CATEGORY,
                    request.request.category
                ),
            }
            .into());
        }
        let decode_started_ns = timeline
            .map(|_| sinomo_core::network::network_timeline_monotonic_ns())
            .unwrap_or(0);
        // A forward with no inner attachment carries this outer control value
        // in the owned body stream. A forward with an inner attachment keeps
        // the small control value in the head so the one available body stream
        // can transfer the attachment unchanged.
        let payload = if request.has_streamed_value() {
            request
                .decode_body::<crate::DaemonObjectRpcForwardRequest>()
                .await
                .map(|payload| {
                    (
                        payload,
                        sinomo_core::network::ObjectRpcIncomingBody::empty(),
                    )
                })
        } else {
            request
                .request
                .decode_body::<crate::DaemonObjectRpcForwardRequest>()
                .map(|payload| (payload, request.body))
        }
        .map_err(DaemonError::from);
        record_daemon_stream_timeline_stage(
            timeline,
            sinomo_core::network::network_timeline_stage::FORWARD_DECODE,
            decode_started_ns,
            payload.is_ok(),
        );
        let (mut payload, request_body) = payload?;
        sinomo_api::enforce_daemon_object_rpc_forward_route_affinity(&route, &mut payload)?;
        let (forwarded, body) =
            crate::service_adapter::forward_object_rpc_call_from_daemon_control(
                paths,
                payload,
                request_body,
                service_runtime,
            )
            .await
            .map_err(DaemonError::from)?;
        let encode_started_ns = timeline
            .map(|_| sinomo_core::network::network_timeline_monotonic_ns())
            .unwrap_or(0);
        let response =
            sinomo_core::network::ObjectRpcResponse::encode_body(route.clone(), &forwarded)
                .map_err(DaemonError::from);
        record_daemon_stream_timeline_stage(
            timeline,
            sinomo_core::network::network_timeline_stage::FORWARD_ENCODE,
            encode_started_ns,
            response.is_ok(),
        );
        let mut response = response?;
        // The generic registry adapter normally supplies this outer response
        // attestation; this specialized forwarding path supplies it here.
        crate::service_adapter::mark_daemon_rpc_response_responder(paths, &mut response);
        let response = if body.is_empty() {
            sinomo_core::network::ObjectRpcOutgoingResponse::value(response)
        } else {
            sinomo_core::network::ObjectRpcOutgoingResponse::new(response, body)
        };
        Ok((response, false, route))
    } else if crate::service_adapter::is_sandbox_stream_route(&request.request) {
        if request.body.is_attachment() {
            return Err(DaemonError::Parse {
                detail: "sandbox stream session does not accept an attached request body"
                    .to_string(),
            });
        }
        let stream_request = match route.as_str() {
            sinomo_sandbox::SANDBOX_RUN_TOOLCHAIN_STREAM_ROUTE => {
                DaemonSandboxStreamRequest::RunToolchain(
                    request
                        .decode_body::<sinomo_sandbox::SandboxRunToolchainRequest>()
                        .await
                        .map_err(DaemonError::from)?,
                )
            }
            sinomo_sandbox::SANDBOX_RUN_PROCESS_PLAN_STREAM_ROUTE => {
                DaemonSandboxStreamRequest::RunProcessPlan(
                    request
                        .decode_body::<sinomo_sandbox::SandboxRunProcessPlanRequest>()
                        .await
                        .map_err(DaemonError::from)?,
                )
            }
            _ => unreachable!("sandbox stream route predicate matched an unknown route"),
        };
        Ok((
            daemon_sandbox_stream_rpc_response(
                paths,
                route.clone(),
                stream_request,
                service_runtime,
            )?,
            false,
            route,
        ))
    } else {
        crate::service_adapter::dispatch_rpc_via_registry_with_body_async(
            service_host,
            authenticated,
            request,
            Some(paths),
        )
        .await
        .map(|(response, stop)| (response, stop, route))
    }
}

enum DaemonSandboxStreamRequest {
    RunToolchain(sinomo_sandbox::SandboxRunToolchainRequest),
    RunProcessPlan(sinomo_sandbox::SandboxRunProcessPlanRequest),
}

fn daemon_sandbox_stream_rpc_response(
    paths: &DaemonPaths,
    route: String,
    request: DaemonSandboxStreamRequest,
    runtime: &tokio::runtime::Handle,
) -> Result<sinomo_core::network::ObjectRpcOutgoingResponse, DaemonError> {
    let response = sinomo_core::network::ObjectRpcResponse::encode_body(&route, &())
        .map_err(DaemonError::from)?;
    let (reader, mut writer) = tokio::io::duplex(DAEMON_SANDBOX_SESSION_BODY_BUFFER_BYTES);
    let paths = paths.clone();
    let runtime = runtime.clone();
    let producer = match request {
        DaemonSandboxStreamRequest::RunToolchain(payload) => tokio::spawn(async move {
            if let Err(error) = handle_daemon_sandbox_run_toolchain_stream_async_on_runtime(
                &paths,
                payload,
                &mut writer,
                &runtime,
            )
            .await
            {
                let frame = sinomo_sandbox::SandboxRunToolchainStreamFrame::Error {
                    detail: error.to_string(),
                };
                let _ = write_daemon_sandbox_stream_frame_async(&mut writer, &frame).await;
            }
            let _ = writer.shutdown().await;
        }),
        DaemonSandboxStreamRequest::RunProcessPlan(payload) => tokio::spawn(async move {
            if let Err(error) = handle_daemon_sandbox_run_process_plan_stream_async_on_runtime(
                &paths,
                payload,
                &mut writer,
                &runtime,
            )
            .await
            {
                let frame = sinomo_sandbox::SandboxRunProcessPlanStreamFrame::Error {
                    detail: error.to_string(),
                };
                let _ =
                    write_daemon_sandbox_process_plan_stream_frame_async(&mut writer, &frame).await;
            }
            let _ = writer.shutdown().await;
        }),
    };
    let body = sinomo_core::network::ObjectRpcBodySource::async_reader(reader, None)
        .with_lifetime_guard(producer)
        .map_err(DaemonError::from)?;
    Ok(sinomo_core::network::ObjectRpcOutgoingResponse::new(
        response, body,
    ))
}

async fn read_daemon_control_frame_async(
    stream: &mut tokio::net::TcpStream,
) -> Result<sinomo_core::network::DaemonControlRequest, DaemonError> {
    sinomo_core::network::read_object_frame_async(
        stream,
        sinomo_core::network::ObjectFrameIoOptions::default(),
    )
    .await
    .map_err(DaemonError::from)
}

async fn write_daemon_rpc_response_frame_async(
    stream: &mut tokio::net::TcpStream,
    response: sinomo_core::network::ObjectRpcResponse,
) -> Result<(), DaemonError> {
    sinomo_core::network::write_object_frame_async(
        stream,
        &response,
        sinomo_core::network::ObjectFrameIoOptions::default(),
    )
    .await
    .map_err(DaemonError::from)
}

pub(crate) fn accepted_rollout_should_stop_current_daemon(
    paths: &DaemonPaths,
    response: &sinomo_rollout::ClusterRolloutBinaryResponse,
) -> bool {
    response.status == "accepted" && !daemon_paths_are_shadow(paths)
}

#[cfg(feature = "sandbox")]
fn daemon_maintenance_interval_remaining(
    now: Instant,
    last: Instant,
    interval: Duration,
) -> Duration {
    interval.saturating_sub(now.saturating_duration_since(last))
}

fn daemon_sleep_duration_or_min(duration: Duration) -> Duration {
    if duration.is_zero() {
        Duration::from_millis(1)
    } else {
        duration
    }
}

fn daemon_shutdown_requested(shutdown_rx: &tokio::sync::watch::Receiver<bool>) -> bool {
    *shutdown_rx.borrow()
}

fn daemon_maintenance_sleep_duration(
    _now: Instant,
    cluster_next_due: Duration,
    dfs_triggered_maintenance_due: Option<Duration>,
    dfs_maintenance_probe_due: Option<Duration>,
    dfs_maintenance_task_completion_due: Option<Duration>,
    shadow_finalization_due: Option<Duration>,
    sandbox_lifecycle_maintenance_due: Option<Duration>,
) -> Duration {
    let mut next_due = cluster_next_due;
    if let Some(dfs_triggered_maintenance_due) = dfs_triggered_maintenance_due {
        next_due = next_due.min(dfs_triggered_maintenance_due);
    }
    if let Some(dfs_maintenance_probe_due) = dfs_maintenance_probe_due {
        next_due = next_due.min(dfs_maintenance_probe_due);
    }
    if let Some(dfs_maintenance_task_completion_due) = dfs_maintenance_task_completion_due {
        next_due = next_due.min(dfs_maintenance_task_completion_due);
    }
    if let Some(shadow_finalization_due) = shadow_finalization_due {
        next_due = next_due.min(shadow_finalization_due);
    }
    if let Some(sandbox_lifecycle_maintenance_due) = sandbox_lifecycle_maintenance_due {
        next_due = next_due.min(sandbox_lifecycle_maintenance_due);
    }
    daemon_sleep_duration_or_min(next_due)
}

fn daemon_rollout_handoff_pending(paths: &DaemonPaths) -> bool {
    let Some(data_dir) = paths.root.parent() else {
        return false;
    };
    let rollout_paths = SelfRolloutPaths::new(data_dir);
    rollout_shadow_finalize_path(&rollout_paths).exists()
        || rollout_main_activation_path(&rollout_paths).exists()
}

fn shadow_rollout_finalization_poll_due_in(paths: &DaemonPaths) -> Duration {
    if paths.root.parent().is_none() {
        return DAEMON_CLUSTER_TICK_INTERVAL;
    }
    if daemon_rollout_handoff_pending(paths) {
        DAEMON_CLUSTER_TICK_INTERVAL
    } else {
        DAEMON_SHADOW_ROLLOUT_IDLE_FINALIZATION_INTERVAL
    }
}

pub fn handle_daemon_sandbox_http_fetch(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxHttpRequest,
) -> Result<sinomo_sandbox::SandboxHttpResponse, DaemonError> {
    let config = load_daemon_sandbox_policy_config(paths)?;
    config
        .policy
        .validate_http_request(&request)
        .map_err(|source| DaemonError::Parse {
            detail: source.to_string(),
        })?;
    daemon_sandbox_http_fetch(request)
}

pub fn handle_daemon_sandbox_http_fetch_to_file(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxHttpFetchToFileRequest,
) -> Result<sinomo_sandbox::SandboxHttpFetchFileResult, DaemonError> {
    handle_daemon_sandbox_http_fetch_to_file_on_runtime(paths, request, None)
}

pub(crate) fn handle_daemon_sandbox_http_fetch_to_file_on_runtime(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxHttpFetchToFileRequest,
    runtime: Option<&tokio::runtime::Handle>,
) -> Result<sinomo_sandbox::SandboxHttpFetchFileResult, DaemonError> {
    let (lifecycle_lease, store, replica_candidates) =
        open_daemon_sandbox_store_with_mutation_lease(paths, &request.sandbox_id, runtime)?;
    let mut hooks = DaemonSandboxLifecycleHooks { paths };
    sinomo_sandbox::ClusteredSandboxLifecycleRuntime::new(&mut hooks).http_fetch_to_file(
        &lifecycle_lease,
        store,
        replica_candidates,
        &DaemonSandboxHttpTransport,
        request,
    )
}

pub fn handle_daemon_sandbox_create(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxCreateInstanceRequest,
) -> Result<sinomo_sandbox::SandboxCreateInstanceResponse, DaemonError> {
    handle_daemon_sandbox_create_on_runtime(paths, request, None)
}

pub(crate) fn handle_daemon_sandbox_create_on_runtime(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxCreateInstanceRequest,
    runtime: Option<&tokio::runtime::Handle>,
) -> Result<sinomo_sandbox::SandboxCreateInstanceResponse, DaemonError> {
    let sandbox_id = request
        .sandbox_id
        .clone()
        .unwrap_or_else(sinomo_sandbox::SandboxId::generate);
    let (lifecycle_lease, mut store, _replica_candidates) =
        open_daemon_sandbox_store_with_transaction_lease(paths, &sandbox_id, runtime)?;
    let mut hooks = DaemonSandboxLifecycleHooks { paths };
    sinomo_sandbox::ClusteredSandboxLifecycleRuntime::new(&mut hooks).create(
        &lifecycle_lease,
        &mut store,
        sandbox_id,
        request,
    )
}

pub fn handle_daemon_sandbox_status(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxStatusRequest,
) -> Result<sinomo_sandbox::SandboxStatusResponse, DaemonError> {
    handle_daemon_sandbox_status_on_runtime(paths, request, None)
}

pub(crate) fn handle_daemon_sandbox_status_on_runtime(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxStatusRequest,
    runtime: Option<&tokio::runtime::Handle>,
) -> Result<sinomo_sandbox::SandboxStatusResponse, DaemonError> {
    let mut store = open_daemon_sandbox_instance_store_on_runtime(paths, runtime)?;
    let mut hooks = DaemonSandboxLifecycleHooks { paths };
    sinomo_sandbox::ClusteredSandboxLifecycleRuntime::new(&mut hooks).status(&mut store, request)
}

pub fn handle_daemon_sandbox_update(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxUpdateInstanceRequest,
) -> Result<sinomo_sandbox::SandboxUpdateInstanceResponse, DaemonError> {
    handle_daemon_sandbox_update_on_runtime(paths, request, None)
}

pub(crate) fn handle_daemon_sandbox_update_on_runtime(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxUpdateInstanceRequest,
    runtime: Option<&tokio::runtime::Handle>,
) -> Result<sinomo_sandbox::SandboxUpdateInstanceResponse, DaemonError> {
    let sandbox_id = request.sandbox_id.clone();
    let (lifecycle_lease, store, replica_candidates) =
        open_daemon_sandbox_store_with_mutation_lease(paths, &sandbox_id, runtime)?;
    let mut hooks = DaemonSandboxLifecycleHooks { paths };
    sinomo_sandbox::ClusteredSandboxLifecycleRuntime::new(&mut hooks).update(
        &lifecycle_lease,
        store,
        replica_candidates,
        request,
    )
}

pub fn handle_daemon_sandbox_checkpoint(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxCheckpointRequest,
) -> Result<sinomo_sandbox::SandboxCheckpointResponse, DaemonError> {
    handle_daemon_sandbox_checkpoint_on_runtime(paths, request, None)
}

pub(crate) fn handle_daemon_sandbox_checkpoint_on_runtime(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxCheckpointRequest,
    runtime: Option<&tokio::runtime::Handle>,
) -> Result<sinomo_sandbox::SandboxCheckpointResponse, DaemonError> {
    let (lifecycle_lease, mut store, _replica_candidates) =
        open_daemon_sandbox_store_with_mutation_lease(paths, &request.sandbox_id, runtime)?;
    let mut hooks = DaemonSandboxLifecycleHooks { paths };
    sinomo_sandbox::ClusteredSandboxLifecycleRuntime::new(&mut hooks).checkpoint(
        &lifecycle_lease,
        &mut store,
        request,
    )
}

pub fn handle_daemon_sandbox_resume(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxResumeRequest,
) -> Result<sinomo_sandbox::SandboxResumeResponse, DaemonError> {
    handle_daemon_sandbox_resume_on_runtime(paths, request, None)
}

pub(crate) fn handle_daemon_sandbox_resume_on_runtime(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxResumeRequest,
    runtime: Option<&tokio::runtime::Handle>,
) -> Result<sinomo_sandbox::SandboxResumeResponse, DaemonError> {
    let (lifecycle_lease, mut store, _replica_candidates) =
        open_daemon_sandbox_store_with_transaction_lease(paths, &request.sandbox_id, runtime)?;
    let mut hooks = DaemonSandboxLifecycleHooks { paths };
    sinomo_sandbox::ClusteredSandboxLifecycleRuntime::new(&mut hooks).resume(
        &lifecycle_lease,
        &mut store,
        request,
    )
}

pub fn handle_daemon_sandbox_reboot(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxRebootRequest,
) -> Result<sinomo_sandbox::SandboxRebootResponse, DaemonError> {
    handle_daemon_sandbox_reboot_on_runtime(paths, request, None)
}

pub(crate) fn handle_daemon_sandbox_reboot_on_runtime(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxRebootRequest,
    runtime: Option<&tokio::runtime::Handle>,
) -> Result<sinomo_sandbox::SandboxRebootResponse, DaemonError> {
    let (lifecycle_lease, mut store, _replica_candidates) =
        open_daemon_sandbox_store_with_transaction_lease(paths, &request.sandbox_id, runtime)?;
    let mut hooks = DaemonSandboxLifecycleHooks { paths };
    sinomo_sandbox::ClusteredSandboxLifecycleRuntime::new(&mut hooks).reboot(
        &lifecycle_lease,
        &mut store,
        request,
    )
}

pub fn handle_daemon_sandbox_destroy(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxDestroyRequest,
) -> Result<sinomo_sandbox::SandboxDestroyResponse, DaemonError> {
    handle_daemon_sandbox_destroy_on_runtime(paths, request, None)
}

pub(crate) fn handle_daemon_sandbox_destroy_on_runtime(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxDestroyRequest,
    runtime: Option<&tokio::runtime::Handle>,
) -> Result<sinomo_sandbox::SandboxDestroyResponse, DaemonError> {
    let (lifecycle_lease, mut store, _replica_candidates) =
        open_daemon_sandbox_store_with_transaction_lease(paths, &request.sandbox_id, runtime)?;
    let mut hooks = DaemonSandboxLifecycleHooks { paths };
    sinomo_sandbox::ClusteredSandboxLifecycleRuntime::new(&mut hooks).destroy(
        &lifecycle_lease,
        &mut store,
        request,
    )
}

pub fn handle_daemon_sandbox_list(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxListRequest,
) -> Result<sinomo_sandbox::SandboxListResponse, DaemonError> {
    handle_daemon_sandbox_list_on_runtime(paths, request, None)
}

pub(crate) fn handle_daemon_sandbox_list_on_runtime(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxListRequest,
    runtime: Option<&tokio::runtime::Handle>,
) -> Result<sinomo_sandbox::SandboxListResponse, DaemonError> {
    let store = open_daemon_sandbox_instance_store_on_runtime(paths, runtime)?;
    let mut hooks = DaemonSandboxLifecycleHooks { paths };
    sinomo_sandbox::ClusteredSandboxLifecycleRuntime::new(&mut hooks).list(store, request)
}

pub fn handle_daemon_sandbox_read(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxReadRequest,
) -> Result<sinomo_sandbox::SandboxReadResponse, DaemonError> {
    handle_daemon_sandbox_read_on_runtime(paths, request, None)
}

pub(crate) fn handle_daemon_sandbox_read_on_runtime(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxReadRequest,
    runtime: Option<&tokio::runtime::Handle>,
) -> Result<sinomo_sandbox::SandboxReadResponse, DaemonError> {
    let store = open_daemon_sandbox_instance_store_on_runtime(paths, runtime)?;
    let mut hooks = DaemonSandboxLifecycleHooks { paths };
    sinomo_sandbox::ClusteredSandboxLifecycleRuntime::new(&mut hooks).read(store, request)
}

pub fn handle_daemon_sandbox_write(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxWriteRequest,
) -> Result<sinomo_sandbox::SandboxWriteResponse, DaemonError> {
    handle_daemon_sandbox_write_on_runtime(paths, request, None)
}

pub(crate) fn handle_daemon_sandbox_write_on_runtime(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxWriteRequest,
    runtime: Option<&tokio::runtime::Handle>,
) -> Result<sinomo_sandbox::SandboxWriteResponse, DaemonError> {
    let (lifecycle_lease, store, replica_candidates) =
        open_daemon_sandbox_store_with_mutation_lease(paths, &request.sandbox_id, runtime)?;
    let mut hooks = DaemonSandboxLifecycleHooks { paths };
    sinomo_sandbox::ClusteredSandboxLifecycleRuntime::new(&mut hooks).write(
        &lifecycle_lease,
        store,
        replica_candidates,
        request,
    )
}

pub fn handle_daemon_sandbox_import_busybox(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxImportBusyBoxToolchainRequest,
) -> Result<sinomo_sandbox::SandboxImportBusyBoxToolchainResponse, DaemonError> {
    handle_daemon_sandbox_import_busybox_on_runtime(paths, request, None)
}

pub(crate) fn handle_daemon_sandbox_import_busybox_on_runtime(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxImportBusyBoxToolchainRequest,
    runtime: Option<&tokio::runtime::Handle>,
) -> Result<sinomo_sandbox::SandboxImportBusyBoxToolchainResponse, DaemonError> {
    let (dfs, replica_candidates) =
        open_daemon_sandbox_dfs_service_with_replica_candidates_on_runtime(paths, runtime)?;
    let store = sinomo_sandbox::SifsSandboxToolchainStore::new(dfs)
        .with_replica_candidates(replica_candidates);
    let mut hooks = DaemonSandboxLifecycleHooks { paths };
    sinomo_sandbox::ClusteredSandboxToolchainRuntime::new(&mut hooks).import_busybox(store, request)
}

pub fn handle_daemon_sandbox_toolchain_add_command(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxAddToolchainCommandRequest,
) -> Result<sinomo_sandbox::SandboxAddToolchainCommandResponse, DaemonError> {
    handle_daemon_sandbox_toolchain_add_command_on_runtime(paths, request, None)
}

pub(crate) fn handle_daemon_sandbox_toolchain_add_command_on_runtime(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxAddToolchainCommandRequest,
    runtime: Option<&tokio::runtime::Handle>,
) -> Result<sinomo_sandbox::SandboxAddToolchainCommandResponse, DaemonError> {
    let (dfs, replica_candidates) =
        open_daemon_sandbox_dfs_service_with_replica_candidates_on_runtime(paths, runtime)?;
    let store = sinomo_sandbox::SifsSandboxToolchainStore::new(dfs)
        .with_replica_candidates(replica_candidates);
    let mut hooks = DaemonSandboxLifecycleHooks { paths };
    sinomo_sandbox::ClusteredSandboxToolchainRuntime::new(&mut hooks).add_command(store, request)
}

pub(crate) fn handle_daemon_sandbox_toolchain_import_package_from_path_on_runtime(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxImportToolchainPackageFromPathRequest,
    runtime: Option<&tokio::runtime::Handle>,
) -> Result<sinomo_sandbox::SandboxImportToolchainPackageResponse, DaemonError> {
    let request = sinomo_sandbox::load_sandbox_toolchain_package_from_path(request)?;
    handle_daemon_sandbox_toolchain_import_package_on_runtime(paths, request, runtime)
}

pub fn handle_daemon_sandbox_toolchain_import_package(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxImportToolchainPackageRequest,
) -> Result<sinomo_sandbox::SandboxImportToolchainPackageResponse, DaemonError> {
    handle_daemon_sandbox_toolchain_import_package_on_runtime(paths, request, None)
}

pub(crate) fn handle_daemon_sandbox_toolchain_import_package_on_runtime(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxImportToolchainPackageRequest,
    runtime: Option<&tokio::runtime::Handle>,
) -> Result<sinomo_sandbox::SandboxImportToolchainPackageResponse, DaemonError> {
    let (dfs, replica_candidates) =
        open_daemon_sandbox_dfs_service_with_replica_candidates_on_runtime(paths, runtime)?;
    let store = sinomo_sandbox::SifsSandboxToolchainStore::new(dfs)
        .with_replica_candidates(replica_candidates);
    let mut hooks = DaemonSandboxLifecycleHooks { paths };
    sinomo_sandbox::ClusteredSandboxToolchainRuntime::new(&mut hooks).import_package(store, request)
}

pub fn handle_daemon_sandbox_toolchain_add_artifact(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxAddToolchainArtifactRequest,
) -> Result<sinomo_sandbox::SandboxAddToolchainArtifactResponse, DaemonError> {
    handle_daemon_sandbox_toolchain_add_artifact_on_runtime(paths, request, None)
}

pub(crate) fn handle_daemon_sandbox_toolchain_add_artifact_on_runtime(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxAddToolchainArtifactRequest,
    runtime: Option<&tokio::runtime::Handle>,
) -> Result<sinomo_sandbox::SandboxAddToolchainArtifactResponse, DaemonError> {
    let (dfs, replica_candidates) =
        open_daemon_sandbox_dfs_service_with_replica_candidates_on_runtime(paths, runtime)?;
    let store = sinomo_sandbox::SifsSandboxToolchainStore::new(dfs)
        .with_replica_candidates(replica_candidates);
    let mut hooks = DaemonSandboxLifecycleHooks { paths };
    sinomo_sandbox::ClusteredSandboxToolchainRuntime::new(&mut hooks).add_artifact(store, request)
}

pub fn handle_daemon_sandbox_run_toolchain(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxRunToolchainRequest,
) -> Result<sinomo_sandbox::SandboxLocalCommandOutcome, DaemonError> {
    handle_daemon_sandbox_run_toolchain_on_runtime(paths, request, None)
}

pub(crate) fn handle_daemon_sandbox_run_toolchain_on_runtime(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxRunToolchainRequest,
    runtime: Option<&tokio::runtime::Handle>,
) -> Result<sinomo_sandbox::SandboxLocalCommandOutcome, DaemonError> {
    handle_daemon_sandbox_run_toolchain_with_output_sink_on_runtime(
        paths,
        request,
        None,
        runtime,
        sinomo_sandbox::SandboxCancellationToken::new(),
    )
}

pub(crate) fn handle_daemon_sandbox_run_toolchain_with_cancellation_on_runtime(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxRunToolchainRequest,
    runtime: Option<&tokio::runtime::Handle>,
    cancellation_token: sinomo_sandbox::SandboxCancellationToken,
) -> Result<sinomo_sandbox::SandboxLocalCommandOutcome, DaemonError> {
    handle_daemon_sandbox_run_toolchain_with_output_sink_on_runtime(
        paths,
        request,
        None,
        runtime,
        cancellation_token,
    )
}

pub fn handle_daemon_sandbox_run_process_plan(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxRunProcessPlanRequest,
) -> Result<sinomo_sandbox::SandboxProcessPlanOutcome, DaemonError> {
    handle_daemon_sandbox_run_process_plan_on_runtime(paths, request, None)
}

pub(crate) fn handle_daemon_sandbox_run_process_plan_on_runtime(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxRunProcessPlanRequest,
    runtime: Option<&tokio::runtime::Handle>,
) -> Result<sinomo_sandbox::SandboxProcessPlanOutcome, DaemonError> {
    handle_daemon_sandbox_run_process_plan_with_output_sink_on_runtime(
        paths,
        request,
        None,
        runtime,
        sinomo_sandbox::SandboxCancellationToken::new(),
    )
}

pub(crate) fn handle_daemon_sandbox_run_process_plan_with_cancellation_on_runtime(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxRunProcessPlanRequest,
    runtime: Option<&tokio::runtime::Handle>,
    cancellation_token: sinomo_sandbox::SandboxCancellationToken,
) -> Result<sinomo_sandbox::SandboxProcessPlanOutcome, DaemonError> {
    handle_daemon_sandbox_run_process_plan_with_output_sink_on_runtime(
        paths,
        request,
        None,
        runtime,
        cancellation_token,
    )
}

/// Error returned by sandbox-execution RPC handlers when the daemon was built
/// without the `sandbox` feature, so external sandbox executors are not
/// available.
#[cfg(not(feature = "sandbox"))]
fn sandbox_runtime_not_compiled_error() -> DaemonError {
    DaemonError::Parse {
        detail: "sandbox runtime not compiled in; rebuild with `--features sandbox`".to_string(),
    }
}

#[cfg(not(feature = "sandbox"))]
fn handle_daemon_sandbox_run_process_plan_with_output_sink_on_runtime(
    _paths: &DaemonPaths,
    _request: sinomo_sandbox::SandboxRunProcessPlanRequest,
    _output_sink: Option<Arc<dyn sinomo_sandbox::SandboxOutputSink>>,
    _runtime: Option<&tokio::runtime::Handle>,
    _cancellation_token: sinomo_sandbox::SandboxCancellationToken,
) -> Result<sinomo_sandbox::SandboxProcessPlanOutcome, DaemonError> {
    Err(sandbox_runtime_not_compiled_error())
}

#[cfg(feature = "sandbox")]
fn handle_daemon_sandbox_run_process_plan_with_output_sink_on_runtime(
    _paths: &DaemonPaths,
    _request: sinomo_sandbox::SandboxRunProcessPlanRequest,
    _output_sink: Option<Arc<dyn sinomo_sandbox::SandboxOutputSink>>,
    _runtime: Option<&tokio::runtime::Handle>,
    _cancellation_token: sinomo_sandbox::SandboxCancellationToken,
) -> Result<sinomo_sandbox::SandboxProcessPlanOutcome, DaemonError> {
    Err(DaemonError::Parse {
        detail: "daemon-hosted dynamic SIFS sandbox process plans were removed; execute through an external sandbox executor or the live DFS/FUSE simple sandbox".to_string(),
    })
}

#[cfg(not(feature = "sandbox"))]
fn handle_daemon_sandbox_run_toolchain_with_output_sink_on_runtime(
    _paths: &DaemonPaths,
    _request: sinomo_sandbox::SandboxRunToolchainRequest,
    _output_sink: Option<Arc<dyn sinomo_sandbox::SandboxOutputSink>>,
    _runtime: Option<&tokio::runtime::Handle>,
    _cancellation_token: sinomo_sandbox::SandboxCancellationToken,
) -> Result<sinomo_sandbox::SandboxLocalCommandOutcome, DaemonError> {
    Err(sandbox_runtime_not_compiled_error())
}

#[cfg(feature = "sandbox")]
fn handle_daemon_sandbox_run_toolchain_with_output_sink_on_runtime(
    _paths: &DaemonPaths,
    _request: sinomo_sandbox::SandboxRunToolchainRequest,
    _output_sink: Option<Arc<dyn sinomo_sandbox::SandboxOutputSink>>,
    _runtime: Option<&tokio::runtime::Handle>,
    _cancellation_token: sinomo_sandbox::SandboxCancellationToken,
) -> Result<sinomo_sandbox::SandboxLocalCommandOutcome, DaemonError> {
    Err(DaemonError::Parse {
        detail: "daemon-hosted dynamic SIFS sandbox commands were removed; execute through an external sandbox executor or the live DFS/FUSE simple sandbox".to_string(),
    })
}

#[cfg(feature = "sandbox")]
fn run_daemon_sandbox_lifecycle_maintenance_scan(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
    marked_sandbox_ids: BTreeSet<sinomo_sandbox::SandboxId>,
) -> Result<usize, DaemonError> {
    // Metadata generations remain independent across listing, lease mutation,
    // and recovery, while the immutable-content caches and initialized local
    // layout are safe to share for the complete bounded scan.
    let content = open_daemon_sandbox_content_store(paths)?;
    let (dfs, replica_candidates) =
        open_daemon_sandbox_dfs_service_with_replica_candidates_and_content_on_runtime(
            paths,
            Some(runtime),
            content.clone(),
        )?;
    let mut authoritative_store = sinomo_sandbox::SifsSandboxInstanceStore::new(dfs)
        .with_replica_candidates(replica_candidates.clone());
    let pending_manifests = authoritative_store.list_pending_authoritative_manifests()?;
    drop(authoritative_store);

    let pending_sandbox_ids = pending_manifests
        .into_iter()
        .map(|manifest| manifest.sandbox_id)
        .collect::<BTreeSet<_>>();
    let mut candidate_sandbox_ids = marked_sandbox_ids;
    for sandbox_id in &pending_sandbox_ids {
        // Safety sweeps can discover fences written by an older binary or a
        // direct store user. Persist the marker before attempting recovery so
        // a second crash cannot return the daemon to unindexed work.
        daemon_sandbox_lifecycle_recovery_state(paths)
            .mark_authoritative_recovery_pending(sandbox_id)?;
        candidate_sandbox_ids.insert(sandbox_id.clone());
    }

    let mut recovered = 0_usize;
    for sandbox_id in candidate_sandbox_ids {
        let result = (|| -> Result<(), DaemonError> {
            // Acquire the lease first, then open the recovery generation. A
            // store retained from the listing scan would have a stale journal
            // view after the lease mutation and could not safely append.
            let lease_dfs = open_daemon_sandbox_dfs_service_with_content_on_runtime(
                paths,
                Some(runtime),
                content.clone(),
            )?;
            let recovery_dfs = open_daemon_sandbox_dfs_service_with_content_on_runtime(
                paths,
                Some(runtime),
                content.clone(),
            )?;
            let mut hooks = DaemonSandboxLifecycleHooks { paths };
            sinomo_sandbox::ClusteredSandboxLifecycleRuntime::new(&mut hooks)
                .open_operation(
                    lease_dfs,
                    recovery_dfs,
                    replica_candidates.clone(),
                    &sandbox_id,
                    sinomo_sandbox::ClusteredSandboxOperationKind::Mutation,
                    true,
                )
                .map(|_| ())
        })();
        match result {
            Ok(()) if pending_sandbox_ids.contains(&sandbox_id) => {
                recovered = recovered.saturating_add(1)
            }
            Ok(()) => {}
            Err(error) => tracing::warn!(
                sandbox_id = %sandbox_id,
                error = %error,
                "authoritative sandbox transaction remains fenced for retry"
            ),
        }
    }
    Ok(recovered)
}

#[cfg(feature = "sandbox")]
fn run_daemon_sandbox_lifecycle_maintenance_tick_with_scan<F>(
    paths: &DaemonPaths,
    now_unix_nanos: u64,
    scan: F,
) -> Result<usize, DaemonError>
where
    F: FnOnce(BTreeSet<sinomo_sandbox::SandboxId>) -> Result<usize, DaemonError>,
{
    daemon_sandbox_lifecycle_recovery_state(paths).run_due_recovery(now_unix_nanos, scan)
}

#[cfg(feature = "sandbox")]
fn run_daemon_sandbox_lifecycle_maintenance_tick(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
) -> Result<usize, DaemonError> {
    run_daemon_sandbox_lifecycle_maintenance_tick_with_scan(
        paths,
        daemon_dfs_now_unix_nanos(),
        |marked_sandbox_ids| {
            run_daemon_sandbox_lifecycle_maintenance_scan(paths, runtime, marked_sandbox_ids)
        },
    )
}

pub(crate) async fn handle_daemon_sandbox_run_toolchain_stream_async_on_runtime(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxRunToolchainRequest,
    writer: &mut (impl tokio::io::AsyncWrite + Unpin),
    runtime: &tokio::runtime::Handle,
) -> Result<(), DaemonError> {
    enum StreamMessage {
        Output(sinomo_sandbox::SandboxOutputChunk),
        Complete {
            sandbox_id: sinomo_sandbox::SandboxId,
            result: Result<sinomo_sandbox::SandboxLocalCommandOutcome, String>,
        },
    }

    let (sender, mut receiver) = tokio::sync::mpsc::channel(DAEMON_SANDBOX_STREAM_CHANNEL_CAPACITY);
    let output_sender = sender.clone();
    let paths = paths.clone();
    let runtime = runtime.clone();
    let worker = tokio::task::spawn_blocking(move || {
        let sandbox_id = request.sandbox_id.clone();
        let output_sink = Arc::new(move |chunk: sinomo_sandbox::SandboxOutputChunk| {
            let _ = output_sender.blocking_send(StreamMessage::Output(chunk));
        });
        let result = handle_daemon_sandbox_run_toolchain_with_output_sink_on_runtime(
            &paths,
            request,
            Some(output_sink),
            Some(&runtime),
            sinomo_sandbox::SandboxCancellationToken::new(),
        )
        .map_err(|error| error.to_string());
        let _ = sender.blocking_send(StreamMessage::Complete { sandbox_id, result });
    });

    let mut sent_output_chunks = BTreeSet::new();
    while let Some(message) = receiver.recv().await {
        let frame = match message {
            StreamMessage::Output(chunk) => {
                sent_output_chunks.insert(sinomo_sandbox::sandbox_output_chunk_stream_key(&chunk));
                sinomo_sandbox::SandboxRunToolchainStreamFrame::Output { chunk }
            }
            StreamMessage::Complete {
                sandbox_id,
                result: Ok(outcome),
            } => {
                let mut completion_output_chunks = outcome.output_chunks.clone();
                if completion_output_chunks.is_empty() {
                    completion_output_chunks.extend(
                        sinomo_sandbox::sandbox_output_chunks_from_bytes(
                            &outcome.response.command_id,
                            &sandbox_id,
                            sinomo_sandbox::SandboxOutputStream::Stdout,
                            &outcome.stdout,
                        ),
                    );
                    completion_output_chunks.extend(
                        sinomo_sandbox::sandbox_output_chunks_from_bytes(
                            &outcome.response.command_id,
                            &sandbox_id,
                            sinomo_sandbox::SandboxOutputStream::Stderr,
                            &outcome.stderr,
                        ),
                    );
                }
                for chunk in &completion_output_chunks {
                    if sent_output_chunks
                        .insert(sinomo_sandbox::sandbox_output_chunk_stream_key(chunk))
                    {
                        let frame = sinomo_sandbox::SandboxRunToolchainStreamFrame::Output {
                            chunk: chunk.clone(),
                        };
                        write_daemon_sandbox_stream_frame_async(writer, &frame).await?;
                    }
                }
                let frame = sinomo_sandbox::SandboxRunToolchainStreamFrame::Complete { outcome };
                write_daemon_sandbox_stream_frame_async(writer, &frame).await?;
                worker.await.map_err(|source| DaemonError::Network {
                    address: "daemon sandbox stream".to_string(),
                    detail: format!("sandbox stream task failed: {source}"),
                })?;
                return Ok(());
            }
            StreamMessage::Complete {
                result: Err(detail),
                ..
            } => {
                let frame = sinomo_sandbox::SandboxRunToolchainStreamFrame::Error { detail };
                write_daemon_sandbox_stream_frame_async(writer, &frame).await?;
                worker.await.map_err(|source| DaemonError::Network {
                    address: "daemon sandbox stream".to_string(),
                    detail: format!("sandbox stream task failed: {source}"),
                })?;
                return Ok(());
            }
        };
        write_daemon_sandbox_stream_frame_async(writer, &frame).await?;
    }

    worker.await.map_err(|source| DaemonError::Network {
        address: "daemon sandbox stream".to_string(),
        detail: format!("sandbox stream task failed: {source}"),
    })?;
    Ok(())
}

pub(crate) async fn handle_daemon_sandbox_run_process_plan_stream_async_on_runtime(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxRunProcessPlanRequest,
    writer: &mut (impl tokio::io::AsyncWrite + Unpin),
    runtime: &tokio::runtime::Handle,
) -> Result<(), DaemonError> {
    enum StreamMessage {
        Output(sinomo_sandbox::SandboxOutputChunk),
        Complete(Result<sinomo_sandbox::SandboxProcessPlanOutcome, String>),
    }

    let (sender, mut receiver) = tokio::sync::mpsc::channel(DAEMON_SANDBOX_STREAM_CHANNEL_CAPACITY);
    let output_sender = sender.clone();
    let paths = paths.clone();
    let runtime = runtime.clone();
    let worker = tokio::task::spawn_blocking(move || {
        let output_sink = Arc::new(move |chunk: sinomo_sandbox::SandboxOutputChunk| {
            let _ = output_sender.blocking_send(StreamMessage::Output(chunk));
        });
        let result = handle_daemon_sandbox_run_process_plan_with_output_sink_on_runtime(
            &paths,
            request,
            Some(output_sink),
            Some(&runtime),
            sinomo_sandbox::SandboxCancellationToken::new(),
        )
        .map_err(|error| error.to_string());
        let _ = sender.blocking_send(StreamMessage::Complete(result));
    });

    let mut sent_output_chunks = BTreeSet::new();
    while let Some(message) = receiver.recv().await {
        match message {
            StreamMessage::Output(chunk) => {
                sent_output_chunks.insert(sinomo_sandbox::sandbox_output_chunk_stream_key(&chunk));
                let frame = sinomo_sandbox::SandboxRunProcessPlanStreamFrame::Output { chunk };
                write_daemon_sandbox_process_plan_stream_frame_async(writer, &frame).await?;
            }
            StreamMessage::Complete(Ok(outcome)) => {
                for step in &outcome.step_outcomes {
                    for chunk in &step.output_chunks {
                        if sent_output_chunks
                            .insert(sinomo_sandbox::sandbox_output_chunk_stream_key(chunk))
                        {
                            let frame = sinomo_sandbox::SandboxRunProcessPlanStreamFrame::Output {
                                chunk: chunk.clone(),
                            };
                            write_daemon_sandbox_process_plan_stream_frame_async(writer, &frame)
                                .await?;
                        }
                    }
                }
                let frame = sinomo_sandbox::SandboxRunProcessPlanStreamFrame::Complete { outcome };
                write_daemon_sandbox_process_plan_stream_frame_async(writer, &frame).await?;
                worker.await.map_err(|source| DaemonError::Network {
                    address: "daemon sandbox stream".to_string(),
                    detail: format!("sandbox stream task failed: {source}"),
                })?;
                return Ok(());
            }
            StreamMessage::Complete(Err(detail)) => {
                let frame = sinomo_sandbox::SandboxRunProcessPlanStreamFrame::Error { detail };
                write_daemon_sandbox_process_plan_stream_frame_async(writer, &frame).await?;
                worker.await.map_err(|source| DaemonError::Network {
                    address: "daemon sandbox stream".to_string(),
                    detail: format!("sandbox stream task failed: {source}"),
                })?;
                return Ok(());
            }
        }
    }

    worker.await.map_err(|source| DaemonError::Network {
        address: "daemon sandbox stream".to_string(),
        detail: format!("sandbox stream task failed: {source}"),
    })?;
    Ok(())
}

async fn write_daemon_sandbox_stream_frame_async(
    writer: &mut (impl tokio::io::AsyncWrite + Unpin),
    frame: &sinomo_sandbox::SandboxRunToolchainStreamFrame,
) -> Result<(), DaemonError> {
    let response = sinomo_core::network::ObjectRpcResponse::encode_body(
        sinomo_sandbox::SANDBOX_RUN_TOOLCHAIN_STREAM_ROUTE,
        frame,
    )
    .map_err(DaemonError::from)?;
    sinomo_core::network::write_object_frame_async(
        writer,
        &response,
        sinomo_core::network::ObjectFrameIoOptions::none(),
    )
    .await
    .map_err(DaemonError::from)
}

async fn write_daemon_sandbox_process_plan_stream_frame_async(
    writer: &mut (impl tokio::io::AsyncWrite + Unpin),
    frame: &sinomo_sandbox::SandboxRunProcessPlanStreamFrame,
) -> Result<(), DaemonError> {
    let response = sinomo_core::network::ObjectRpcResponse::encode_body(
        sinomo_sandbox::SANDBOX_RUN_PROCESS_PLAN_STREAM_ROUTE,
        frame,
    )
    .map_err(DaemonError::from)?;
    sinomo_core::network::write_object_frame_async(
        writer,
        &response,
        sinomo_core::network::ObjectFrameIoOptions::none(),
    )
    .await
    .map_err(DaemonError::from)
}

fn load_daemon_sandbox_policy_config(
    paths: &DaemonPaths,
) -> Result<sinomo_sandbox::SandboxClusterPolicyConfig, DaemonError> {
    let snapshot = handle_daemon_cluster_kv_snapshot(paths)?;
    sinomo_sandbox::sandbox_policy_config_from_cluster_kv_snapshot(&snapshot)
        .map_err(|source| DaemonError::Parse {
            detail: source.to_string(),
        })
        .map(Option::unwrap_or_default)
}

fn daemon_sandbox_replica_candidates(
    paths: &DaemonPaths,
    local_node_id: &sinomo_core::node::NodeId,
) -> Vec<sinomo_core::node::NodeId> {
    let advertisements = cluster_paths_from_daemon_paths(paths)
        .and_then(|cluster_paths| {
            sinomo_core::kv::read_cluster_kv_prefix_entries(
                &cluster_paths.kv_paths(),
                sinomo_dfs::content_services_prefix(),
            )
            .map_err(Into::into)
        })
        .map(|entries| {
            entries
                .into_iter()
                .filter_map(|entry| entry.decode::<sinomo_dfs::DfsNodeAdvertisement>().ok())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    sinomo_sandbox::clustered_sandbox_replica_candidates(local_node_id, advertisements)
}

pub fn open_daemon_sandbox_dfs_service(
    paths: &DaemonPaths,
) -> Result<sinomo_dfs::NetworkedDfsService<sinomo_dfs::DfsNodeTransport>, DaemonError> {
    open_daemon_sandbox_dfs_service_on_runtime(paths, None)
}

fn open_daemon_sandbox_dfs_service_with_replica_candidates_on_runtime(
    paths: &DaemonPaths,
    runtime: Option<&tokio::runtime::Handle>,
) -> Result<(DaemonSandboxDfsService, Vec<sinomo_core::node::NodeId>), DaemonError> {
    let content = open_daemon_sandbox_content_store(paths)?;
    open_daemon_sandbox_dfs_service_with_replica_candidates_and_content_on_runtime(
        paths, runtime, content,
    )
}

fn open_daemon_sandbox_dfs_service_with_replica_candidates_and_content_on_runtime(
    paths: &DaemonPaths,
    runtime: Option<&tokio::runtime::Handle>,
    content: sinomo_dfs::LocalContentStore,
) -> Result<(DaemonSandboxDfsService, Vec<sinomo_core::node::NodeId>), DaemonError> {
    let dfs = open_daemon_sandbox_dfs_service_with_content_on_runtime(paths, runtime, content)?;
    let replica_candidates = daemon_sandbox_replica_candidates(paths, dfs.node_id());
    Ok((dfs, replica_candidates))
}

fn open_daemon_sandbox_instance_store_on_runtime(
    paths: &DaemonPaths,
    runtime: Option<&tokio::runtime::Handle>,
) -> Result<DaemonSandboxInstanceStore, DaemonError> {
    open_daemon_sandbox_dfs_service_on_runtime(paths, runtime)
        .map(sinomo_sandbox::SifsSandboxInstanceStore::new)
}

pub(crate) fn open_daemon_sandbox_dfs_service_on_runtime(
    paths: &DaemonPaths,
    runtime: Option<&tokio::runtime::Handle>,
) -> Result<sinomo_dfs::NetworkedDfsService<sinomo_dfs::DfsNodeTransport>, DaemonError> {
    let content = open_daemon_sandbox_content_store(paths)?;
    open_daemon_sandbox_dfs_service_with_content_on_runtime(paths, runtime, content)
}

fn open_daemon_sandbox_content_store(
    paths: &DaemonPaths,
) -> Result<sinomo_dfs::LocalContentStore, DaemonError> {
    let data_dir = daemon_data_dir_path(paths)?;
    let data_dir = sinomo_core::local::SinomoDataDir::new(&data_dir);
    let content_root = data_dir.dfs_dir().join("content");
    sinomo_dfs::LocalContentStore::new(&content_root).map_err(|source| DaemonError::Io {
        path: content_root.display().to_string(),
        detail: source.to_string(),
    })
}

fn open_daemon_sandbox_dfs_service_with_content_on_runtime(
    paths: &DaemonPaths,
    runtime: Option<&tokio::runtime::Handle>,
    content: sinomo_dfs::LocalContentStore,
) -> Result<sinomo_dfs::NetworkedDfsService<sinomo_dfs::DfsNodeTransport>, DaemonError> {
    let data_dir = daemon_data_dir_path(paths)?;
    let data_dir = sinomo_core::local::SinomoDataDir::new(&data_dir);
    let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)?;
    let peer_node_ids = sinomo_dfs::discovered_cluster_dfs_peer_node_ids(&cluster_paths, &state)?;
    let transport = if let Some(runtime) = runtime {
        daemon_dfs_node_transport_on_runtime(
            state.local_node_id.clone(),
            peer_node_ids,
            &data_dir,
            runtime,
        )
    } else {
        daemon_dfs_node_transport(state.local_node_id.clone(), peer_node_ids)
    };
    let dfs = sinomo_dfs::NetworkedDfsService::new(transport, content);
    let dfs =
        crate::dfs::with_daemon_owned_dfs_control_plane_on_runtime(dfs, &cluster_paths, runtime);
    let metadata = crate::dfs::open_daemon_dfs_metadata_backend(&data_dir, &cluster_paths)?;
    // Sandbox POSIX calls resolve targeted durable records. Retain only the
    // filesystem and mutation-guard bootstrap instead of cloning every object,
    // version, intent, and receipt into each short-lived service.
    let mut dfs = dfs
        .with_boxed_metadata_backend_lazy(metadata)
        .map_err(|source| DaemonError::Parse {
            detail: source.to_string(),
        })?;
    dfs.bootstrap_distributed_sifs()
        .map_err(|source| DaemonError::Parse {
            detail: source.to_string(),
        })?;
    Ok(dfs)
}

#[derive(Clone, Debug)]
pub(crate) struct DaemonRolloutTelemetryHost {
    pub(crate) paths: DaemonPaths,
}

impl sinomo_rollout::RolloutTelemetryHost for DaemonRolloutTelemetryHost {
    fn reconcile_rollout_telemetry(
        &self,
    ) -> Result<sinomo_rollout::RolloutTelemetryReconcileOutcome, String> {
        run_daemon_rollout_telemetry_reconciliation_tick(&self.paths)
            .map_err(|error| error.to_string())
    }
}

pub(crate) fn run_daemon_rollout_telemetry_reconciliation_tick(
    paths: &DaemonPaths,
) -> Result<sinomo_rollout::RolloutTelemetryReconcileOutcome, DaemonError> {
    let data_dir = daemon_data_dir_path(paths)?;
    let rollout_paths = SelfRolloutPaths::new(&data_dir);
    let event_store = daemon_event_store(paths)?;
    let node_id = daemon_rollout_controller_node_id(&data_dir)?;
    let controller = RolloutController::new(node_id);
    let gate_config =
        sinomo_rollout::read_rollout_telemetry_gate_config(&rollout_paths)?.unwrap_or_default();
    let result = controller.advance_rollout_from_live_telemetry(
        &data_dir,
        &rollout_paths,
        &event_store,
        RolloutTelemetryAdvanceOptions::from(gate_config),
    )?;
    let status = result.status;
    let applied_transition = if status == RolloutTelemetryAdvanceStatus::AppliedTransition {
        result.applied_transition
    } else {
        None
    };
    publish_daemon_module_rollout_status_from_data_dir(&data_dir, &rollout_paths)?;
    Ok(sinomo_rollout::RolloutTelemetryReconcileOutcome {
        status,
        applied_transition,
    })
}

fn publish_daemon_module_rollout_status_from_data_dir(
    data_dir: &Path,
    rollout_paths: &SelfRolloutPaths,
) -> Result<Option<ClusterModuleRolloutStatusRecord>, DaemonError> {
    let data_dir_handle = sinomo_core::local::SinomoDataDir::new(data_dir);
    let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir_handle);
    let Ok(cluster_state) = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
    else {
        return Ok(None);
    };
    let Some(runtime_state) = sinomo_rollout::read_rollout_runtime_state(rollout_paths)? else {
        return Ok(None);
    };
    let telemetry_state = sinomo_rollout::read_rollout_telemetry_advance_state(rollout_paths)?
        .filter(|telemetry| {
            telemetry.rollout_plan_id == runtime_state.plan.rollout_plan_id
                && telemetry.module_id == runtime_state.plan.module_id
        });
    let record = ClusterModuleRolloutStatusRecord {
        cluster_id: cluster_state.cluster_id.clone(),
        node_id: cluster_state.local_node_id.clone(),
        rollout_plan_id: runtime_state.plan.rollout_plan_id.clone(),
        module_id: runtime_state.plan.module_id.clone(),
        artifact_id: runtime_state.plan.artifact_id.clone(),
        stage: daemon_rollout_stage_label(&runtime_state.stage).to_string(),
        current_weight_bps: runtime_state.current_weight_bps,
        active_artifact_id: runtime_state.loader_state.active_artifact_id.clone(),
        previous_artifact_id: runtime_state.loader_state.previous_artifact_id.clone(),
        candidate_artifact_id: runtime_state.loader_state.candidate_artifact_id.clone(),
        shadow_artifact_id: runtime_state.loader_state.shadow_artifact_id.clone(),
        hot_swap_state: daemon_hot_swap_state_label(&runtime_state.loader_state.hot_swap_state)
            .to_string(),
        telemetry_kind: telemetry_state
            .as_ref()
            .map(|telemetry| telemetry.telemetry_kind.clone()),
        telemetry_last_consumed_sequence: telemetry_state
            .as_ref()
            .and_then(|telemetry| telemetry.last_consumed_sequence)
            .map(|sequence| sequence.0),
        last_transition_record_id: telemetry_state
            .as_ref()
            .and_then(|telemetry| telemetry.last_transition_record_id.clone()),
        updated_at: OffsetDateTime::now_utc(),
    };
    let op = sinomo_rollout::build_cluster_module_rollout_status_put_op(&record)?;
    commit_daemon_rollout_kv_ops(&cluster_paths, &cluster_state, vec![op])?;
    Ok(Some(record))
}

fn commit_daemon_rollout_kv_ops(
    cluster_paths: &sinomo_core::network::ClusterPaths,
    cluster_state: &sinomo_core::ClusterBootstrapState,
    ops: Vec<sinomo_core::kv::KvOp>,
) -> Result<sinomo_core::kv::KvSnapshot, DaemonError> {
    let mut commit_state = cluster_state.clone();
    crate::cluster::commit_cluster_kv_txn_with_quorum(
        cluster_paths,
        &mut commit_state,
        sinomo_core::kv::KvTxn {
            compares: Vec::new(),
            success: ops,
            failure: Vec::new(),
        },
    )
}

fn daemon_rollout_controller_node_id(
    data_dir: &Path,
) -> Result<sinomo_core::node::NodeId, DaemonError> {
    let data_dir_handle = sinomo_core::local::SinomoDataDir::new(data_dir);
    let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir_handle);
    if let Ok(state) = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths) {
        return Ok(state.local_node_id);
    }
    let identity = sinomo_core::node::load_or_create_main_node_identity(data_dir)?;
    Ok(sinomo_core::node::NodeId::derived(
        "sinomo.rollout.local-node",
        identity.public_key.as_bytes(),
    ))
}

fn daemon_rollout_stage_label(stage: &sinomo_rollout::RolloutStage) -> &'static str {
    match stage {
        sinomo_rollout::RolloutStage::Shadow => "shadow",
        sinomo_rollout::RolloutStage::Canary => "canary",
        sinomo_rollout::RolloutStage::GuardedR1 => "guarded-r1",
        sinomo_rollout::RolloutStage::Promoted => "promoted",
        sinomo_rollout::RolloutStage::RolledBack => "rolled-back",
        sinomo_rollout::RolloutStage::Quarantined => "quarantined",
    }
}

fn daemon_hot_swap_state_label(state: &sinomo_core::HotSwapState) -> &'static str {
    match state {
        sinomo_core::HotSwapState::Stable => "stable",
        sinomo_core::HotSwapState::Loading => "loading",
        sinomo_core::HotSwapState::Shadowing => "shadowing",
        sinomo_core::HotSwapState::Draining => "draining",
        sinomo_core::HotSwapState::ActivationCheckpoint => "activation-checkpoint",
        sinomo_core::HotSwapState::ActiveCandidate => "active-candidate",
        sinomo_core::HotSwapState::RollbackCheckpoint => "rollback-checkpoint",
        sinomo_core::HotSwapState::RollingBack => "rolling-back",
        sinomo_core::HotSwapState::Quarantined => "quarantined",
    }
}

pub(crate) fn daemon_data_dir_path(paths: &DaemonPaths) -> Result<PathBuf, DaemonError> {
    paths
        .root
        .parent()
        .map(PathBuf::from)
        .ok_or_else(|| DaemonError::Io {
            path: paths.root.display().to_string(),
            detail: "daemon root has no data directory parent".to_string(),
        })
}

fn append_daemon_sandbox_event(
    paths: &DaemonPaths,
    event: sinomo_core::EventEnvelope,
) -> Result<(), DaemonError> {
    let mut store = daemon_event_store(paths)?;
    store.append(event)?;
    Ok(())
}

/// Append a transaction-derived lifecycle event idempotently and make it
/// durable before the authoritative sandbox fence can be cleared. A retry
/// after append-before-clear observes the same derived event ID.
fn append_daemon_sandbox_event_durable(
    paths: &DaemonPaths,
    event: sinomo_core::EventEnvelope,
) -> Result<(), DaemonError> {
    let event_id = event.event_id.clone();
    let expected_kind = event.kind.clone();
    let expected_payload = event.payload.clone();
    let mut store = daemon_event_store(paths)?;
    match store.append(event) {
        Ok(_) => {}
        Err(sinomo_core::events::EventStoreError::DuplicateEventId {
            event_id: duplicate,
        }) if duplicate == event_id => {
            let existing = store
                .replay(sinomo_core::events::ReplayQuery::all())?
                .into_iter()
                .find(|stored| stored.event.event_id == event_id)
                .ok_or_else(|| DaemonError::Parse {
                    detail: format!(
                        "event store reported duplicate {event_id} but replay could not find it"
                    ),
                })?;
            if existing.event.kind != expected_kind || existing.event.payload != expected_payload {
                return Err(DaemonError::Parse {
                    detail: format!(
                        "event {event_id} duplicate payload does not match authoritative sandbox transaction"
                    ),
                });
            }
        }
        Err(error) => return Err(error.into()),
    }
    store.sync_all()?;
    Ok(())
}

fn daemon_event_store(
    paths: &DaemonPaths,
) -> Result<sinomo_core::events::JsonlEventStore, DaemonError> {
    let data_dir = daemon_data_dir_path(paths)?;
    Ok(sinomo_core::events::JsonlEventStore::new(
        data_dir.join("events/event-log.jsonl"),
    ))
}

fn daemon_sandbox_http_fetch(
    request: sinomo_sandbox::SandboxHttpRequest,
) -> Result<sinomo_sandbox::SandboxHttpResponse, DaemonError> {
    let timeout = Duration::from_millis(request.timeout_ms);
    let client = reqwest::blocking::Client::builder()
        .timeout(timeout)
        .redirect(reqwest::redirect::Policy::limited(10))
        .build()
        .map_err(|source| DaemonError::Network {
            address: "sandbox http client".to_string(),
            detail: source.to_string(),
        })?;
    let method = match request.method {
        sinomo_sandbox::SandboxHttpMethod::Get => reqwest::Method::GET,
        sinomo_sandbox::SandboxHttpMethod::Post => reqwest::Method::POST,
        sinomo_sandbox::SandboxHttpMethod::Put => reqwest::Method::PUT,
        sinomo_sandbox::SandboxHttpMethod::Delete => reqwest::Method::DELETE,
        sinomo_sandbox::SandboxHttpMethod::Head => reqwest::Method::HEAD,
    };
    let mut builder = client.request(method, &request.url).timeout(timeout);
    for (name, value) in &request.headers {
        builder = builder.header(name, value);
    }
    if !request.body.is_empty() {
        builder = builder.body(request.body);
    }
    let mut response = builder.send().map_err(|source| DaemonError::Network {
        address: request.url.clone(),
        detail: source.to_string(),
    })?;
    if let Some(length) = response.content_length()
        && length > request.max_response_bytes
    {
        return Err(DaemonError::Network {
            address: request.url,
            detail: format!(
                "sandbox HTTP response is {length} bytes; max is {}",
                request.max_response_bytes
            ),
        });
    }
    let status = response.status().as_u16();
    let mut headers = std::collections::BTreeMap::new();
    for (name, value) in response.headers() {
        if let Ok(value) = value.to_str() {
            headers.insert(name.as_str().to_string(), value.to_string());
        }
    }
    let mut body = Vec::new();
    response
        .by_ref()
        .take(request.max_response_bytes.saturating_add(1))
        .read_to_end(&mut body)
        .map_err(|source| DaemonError::Network {
            address: request.url.clone(),
            detail: source.to_string(),
        })?;
    if body.len() as u64 > request.max_response_bytes {
        return Err(DaemonError::Network {
            address: request.url,
            detail: format!(
                "sandbox HTTP response exceeded max of {} bytes",
                request.max_response_bytes
            ),
        });
    }
    Ok(sinomo_sandbox::SandboxHttpResponse {
        status,
        headers,
        body,
    })
}

struct DaemonSandboxHttpTransport;

impl sinomo_sandbox::SandboxHttpTransport for DaemonSandboxHttpTransport {
    fn fetch(
        &self,
        request: &sinomo_sandbox::SandboxHttpRequest,
    ) -> Result<sinomo_sandbox::SandboxHttpResponse, sinomo_sandbox::SandboxNetworkError> {
        daemon_sandbox_http_fetch(request.clone()).map_err(|source| {
            sinomo_sandbox::SandboxNetworkError::Transport {
                detail: source.to_string(),
            }
        })
    }
}

pub(crate) async fn daemon_status_response_async(
    paths: &DaemonPaths,
    _runtime: Option<&tokio::runtime::Handle>,
) -> Result<crate::DaemonStatus, DaemonError> {
    let read_paths = paths.clone();
    let read_path = read_paths.root.display().to_string();
    let mut status = tokio::task::spawn_blocking(move || read_daemon_status(&read_paths))
        .await
        .map_err(|source| DaemonError::Io {
            path: read_path,
            detail: format!("daemon status read task failed: {source}"),
        })??;
    // This route is the rollout and supervision liveness probe. It must report
    // the local process state without waiting on cluster/KV repair or quorum.
    if let Some(data_dir) = paths.root.parent() {
        let rollout_paths = SelfRolloutPaths::new(data_dir);
        let rollout_path = rollout_paths.root.display().to_string();
        match tokio::task::spawn_blocking(move || read_rollout_status(&rollout_paths))
            .await
            .map_err(|source| DaemonError::Io {
                path: rollout_path.clone(),
                detail: format!("rollout status read task failed: {source}"),
            }) {
            Ok(Ok(rollout)) => status.rollout = rollout,
            Ok(Err(error)) => {
                tracing::warn!("sinomo daemon status rollout decoration skipped: {error}");
            }
            Err(error) => {
                tracing::warn!("sinomo daemon status rollout decoration skipped: {error}");
            }
        }
    }
    if let Some(stats) = crate::process_idle_stats() {
        status.idle_stats = Some(stats.snapshot());
    }
    Ok(status)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DaemonRunOptions, sign_daemon_rpc_request_from_data_dir};
    use sinomo_core::local::SinomoDataDir;
    use sinomo_core::node::NodeId;
    use sinomo_dfs::DfsMetadataBackend;
    use std::fs;
    use std::net::{TcpListener, TcpStream};
    use std::time::{Duration, Instant};

    #[test]
    fn sandbox_create_omitted_limits_inherit_policy_ceiling() {
        let policy_max = sinomo_sandbox::SandboxResourceLimits {
            memory_mb: 31_811,
            wall_time_ms: 60_000,
            max_input_bytes: 1_000,
            max_output_bytes: 2_000,
            max_workspace_bytes: 3_000,
            max_written_bytes_per_command: 4_000,
            max_file_count: 5_000,
        };
        let request = sinomo_sandbox::SandboxCreateInstanceRequest {
            sandbox_id: None,
            toolchain_id: sinomo_sandbox::SandboxToolchainId::from_legacy_named(
                "sandbox_create_policy_defaults",
            )
            .expect("toolchain id"),
            temp_policy: sinomo_sandbox::SandboxTempPolicy::RetainAcrossSessions,
            memory_mb: None,
            wall_time_ms: Some(12_345),
            max_input_bytes: None,
            max_output_bytes: None,
            max_workspace_bytes: None,
            max_written_bytes_per_command: None,
            max_file_count: Some(42),
            mounts: Vec::new(),
        };

        let limits = sinomo_sandbox::sandbox_create_limits(&request, &policy_max);

        assert_eq!(limits.memory_mb, policy_max.memory_mb);
        assert_eq!(limits.wall_time_ms, 12_345);
        assert_eq!(limits.max_input_bytes, policy_max.max_input_bytes);
        assert_eq!(limits.max_output_bytes, policy_max.max_output_bytes);
        assert_eq!(limits.max_workspace_bytes, policy_max.max_workspace_bytes);
        assert_eq!(
            limits.max_written_bytes_per_command,
            policy_max.max_written_bytes_per_command
        );
        assert_eq!(limits.max_file_count, 42);
    }

    #[tokio::test]
    async fn cluster_network_ping_semantic_attachment_is_streamed_and_length_checked() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-network-ping-attachment");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data dir");
        let paths = DaemonPaths::new(&data_dir);
        paths.ensure().expect("daemon paths");
        let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
        let local_node =
            NodeId::from_legacy_named("network_ping_attachment_local").expect("local node");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            sinomo_core::node::ClusterId::from_legacy_named("network_ping_attachment_cluster")
                .expect("cluster id"),
            "network ping attachment",
            local_node.clone(),
            "local",
            "local-public-key",
            time::OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");

        let request_head = sinomo_core::cluster::cluster_network_ping_attachment_rpc_request(
            "streamed-attachment",
            4096,
            8192,
        )
        .expect("request head");
        let response = crate::service_adapter::handle_daemon_cluster_network_ping_request(
            &paths,
            &data_dir,
            sinomo_core::network::ObjectRpcIncomingRequest::new(
                request_head,
                sinomo_core::network::ObjectRpcIncomingBody::bytes(vec![0; 4096]),
            ),
        )
        .await
        .expect("stream attachment response");
        let decoded_response: sinomo_core::cluster::ClusterNetworkPingResponse =
            response.response.decode_body().expect("response head");
        assert_eq!(decoded_response.responder_node_id, local_node);
        assert_eq!(decoded_response.request_attachment_bytes, 4096);
        assert_eq!(decoded_response.response_attachment_bytes, 8192);
        assert_eq!(response.body.total_bytes(), Some(8192));

        let empty_response_head =
            sinomo_core::cluster::cluster_network_ping_attachment_rpc_request(
                "empty-response-attachment",
                16,
                0,
            )
            .expect("empty response request head");
        let empty_response = crate::service_adapter::handle_daemon_cluster_network_ping_request(
            &paths,
            &data_dir,
            sinomo_core::network::ObjectRpcIncomingRequest::new(
                empty_response_head,
                sinomo_core::network::ObjectRpcIncomingBody::bytes(vec![0; 16]),
            ),
        )
        .await
        .expect("zero-byte response attachment is valid");
        assert!(empty_response.body.is_empty());

        let mismatched_head = sinomo_core::cluster::cluster_network_ping_attachment_rpc_request(
            "mismatched-attachment",
            4096,
            0,
        )
        .expect("mismatched request head");
        let error = crate::service_adapter::handle_daemon_cluster_network_ping_request(
            &paths,
            &data_dir,
            sinomo_core::network::ObjectRpcIncomingRequest::new(
                mismatched_head,
                sinomo_core::network::ObjectRpcIncomingBody::bytes(vec![0; 2048]),
            ),
        )
        .await
        .expect_err("descriptor/head mismatch must fail");
        assert!(error.to_string().contains("descriptor declares Some(2048)"));
    }

    #[test]
    fn cluster_network_maintenance_observation_labels_are_bounded() {
        assert_eq!(
            [
                DaemonClusterNetworkMaintenancePhaseDisposition::Completed,
                DaemonClusterNetworkMaintenancePhaseDisposition::TimedOut,
            ]
            .map(cluster_network_maintenance_phase_disposition_name),
            ["completed", "timed_out"]
        );
        assert_eq!(
            [
                DaemonClusterNetworkMaintenanceSubphase::TickPlan,
                DaemonClusterNetworkMaintenanceSubphase::PendingWork,
            ]
            .map(cluster_network_maintenance_subphase_name),
            ["tick_plan", "pending_work"]
        );
    }

    #[test]
    fn committed_route_projection_wakes_only_for_semantic_changes() {
        use sinomo_core::kv::{
            KvEntry, KvEvent, KvEventKind, KvRevision, KvSnapshot, KvVersion, encode_kv_value,
        };

        fn entry(key: &str, value: Vec<u8>, revision: u64) -> KvEntry {
            KvEntry {
                key: key.to_string(),
                value,
                create_revision: KvRevision(1),
                modify_revision: KvRevision(revision),
                version: KvVersion(revision),
                lease_id: None,
            }
        }

        fn snapshot(revision: u64, entries: Vec<KvEntry>, events: Vec<KvEvent>) -> KvSnapshot {
            KvSnapshot {
                revision: KvRevision(revision),
                compacted_revision: KvRevision::ZERO,
                entries,
                leases: Vec::new(),
                events,
                committed_requests: Vec::new(),
                expired_requests: Vec::new(),
            }
        }

        fn put_event(revision: u64, key: &str, value: Vec<u8>) -> KvEvent {
            KvEvent {
                revision: KvRevision(revision),
                key: key.to_string(),
                kind: KvEventKind::Put { value },
            }
        }

        let now = OffsetDateTime::now_utc();
        let cluster_id = sinomo_core::node::ClusterId::generate();
        let local_node = NodeId::generate();
        let remote_node = NodeId::generate();
        let member_key = sinomo_core::node::cluster_member_key(&remote_node);
        let mut endpoint = sinomo_core::network::daemon_lan_endpoint(
            &remote_node,
            "127.0.0.1:41001".to_string(),
            10,
        );
        let mut member = ClusterMemberRecord::local_voter(
            cluster_id.clone(),
            remote_node,
            "remote",
            "public-key",
            now,
        );
        member.endpoints = vec![endpoint.clone()];

        let initial_value = encode_kv_value(&member).expect("encode initial member");
        let initial = snapshot(1, vec![entry(&member_key, initial_value, 1)], Vec::new());
        let mut projection = DaemonConnectivityTopologyProjection::default();
        projection.seed_if_newer(&initial);
        assert_eq!(
            projection.observe(&initial),
            DaemonConnectivityProjectionChange::default()
        );

        member.updated_at = now + time::Duration::seconds(1);
        let timestamp_only_value =
            encode_kv_value(&member).expect("encode timestamp-only member update");
        let timestamp_only = snapshot(
            2,
            vec![entry(&member_key, timestamp_only_value.clone(), 2)],
            vec![put_event(2, &member_key, timestamp_only_value)],
        );
        assert_eq!(
            projection.observe(&timestamp_only),
            DaemonConnectivityProjectionChange::default(),
            "member liveness timestamps are not connectivity inputs"
        );

        endpoint.address = "127.0.0.1:41002".to_string();
        member.endpoints = vec![endpoint];
        let changed_route_value = encode_kv_value(&member).expect("encode changed member route");
        let changed_route = snapshot(
            3,
            vec![entry(&member_key, changed_route_value.clone(), 3)],
            vec![put_event(3, &member_key, changed_route_value)],
        );
        assert_eq!(
            projection.observe(&changed_route),
            DaemonConnectivityProjectionChange {
                probe_inputs_changed: true,
                route_generation_changed: true,
            },
            "a committed route change must immediately invalidate connectivity"
        );

        let unrelated_key = "/cluster/node-status/unrelated";
        let unrelated_value = vec![1, 2, 3];
        let unrelated = snapshot(
            4,
            vec![
                entry(
                    &member_key,
                    encode_kv_value(&member).expect("encode unchanged member"),
                    3,
                ),
                entry(unrelated_key, unrelated_value.clone(), 4),
            ],
            vec![put_event(4, unrelated_key, unrelated_value)],
        );
        assert_eq!(
            projection.observe(&unrelated),
            DaemonConnectivityProjectionChange::default(),
            "unrelated KV commits must leave a quiet connectivity owner asleep"
        );

        let mut external_endpoint = sinomo_core::network::daemon_lan_endpoint(
            &member.node_id,
            "127.0.0.1:41003".to_string(),
            20,
        );
        let endpoint_key =
            sinomo_core::cluster::cluster_endpoint_key(&member.node_id, &external_endpoint);
        let endpoint_value = encode_kv_value(&external_endpoint).expect("encode external endpoint");
        let external_route = snapshot(
            5,
            vec![
                entry(
                    &member_key,
                    encode_kv_value(&member).expect("encode unchanged member"),
                    3,
                ),
                entry(&endpoint_key, endpoint_value.clone(), 5),
            ],
            vec![put_event(5, &endpoint_key, endpoint_value)],
        );
        assert_eq!(
            projection.observe(&external_route),
            DaemonConnectivityProjectionChange {
                probe_inputs_changed: true,
                route_generation_changed: true,
            },
            "a committed /cluster/endpoints route must invalidate connectivity"
        );

        external_endpoint.last_observed_latency_ms = Some(7);
        let latency_only_value =
            encode_kv_value(&external_endpoint).expect("encode endpoint latency update");
        let latency_only = snapshot(
            6,
            vec![entry(&endpoint_key, latency_only_value.clone(), 6)],
            vec![put_event(6, &endpoint_key, latency_only_value)],
        );
        assert_eq!(
            projection.observe(&latency_only),
            DaemonConnectivityProjectionChange::default(),
            "derived endpoint latency is an output, not a probe-input generation"
        );

        let connectivity_key = sinomo_core::cluster::cluster_connectivity_key(
            &local_node,
            &member.node_id,
            &external_endpoint.endpoint_id,
        );
        let mut connectivity = sinomo_core::network::ClusterConnectivityRecord {
            cluster_id,
            source_node_id: local_node,
            target_node_id: member.node_id.clone(),
            endpoint_id: external_endpoint.endpoint_id.clone(),
            transport: external_endpoint.transport.clone(),
            address: external_endpoint.address.clone(),
            state: sinomo_core::network::ClusterConnectivityState::Healthy,
            latency_ms: Some(3),
            consecutive_failures: 0,
            last_success_at: Some("initial success".to_string()),
            last_failure_at: None,
            failure_reason: None,
            observed_at: now,
            expires_at: now + time::Duration::minutes(15),
        };
        let connectivity_value =
            encode_kv_value(&connectivity).expect("encode committed connectivity record");
        let connectivity_route = snapshot(
            7,
            vec![entry(&connectivity_key, connectivity_value.clone(), 7)],
            vec![put_event(7, &connectivity_key, connectivity_value)],
        );
        assert_eq!(
            projection.observe(&connectivity_route),
            DaemonConnectivityProjectionChange {
                probe_inputs_changed: false,
                route_generation_changed: true,
            },
            "a committed relay edge must wake retained DFS lanes without forcing a probe round"
        );
        let topology_only_baseline = snapshot(
            6,
            vec![
                entry(
                    &member_key,
                    encode_kv_value(&member).expect("encode unchanged member"),
                    3,
                ),
                entry(
                    &endpoint_key,
                    encode_kv_value(&external_endpoint).expect("encode current external endpoint"),
                    6,
                ),
            ],
            Vec::new(),
        );
        let mut topology_only_projection = DaemonConnectivityTopologyProjection::new(false);
        topology_only_projection.seed_if_newer(&topology_only_baseline);
        assert_eq!(
            topology_only_projection.observe(&connectivity_route),
            DaemonConnectivityProjectionChange::default(),
            "a shadow/network-only observer must not retain or wake on DFS route records"
        );

        connectivity.observed_at += time::Duration::seconds(1);
        connectivity.expires_at += time::Duration::seconds(1);
        connectivity.last_success_at = Some("refreshed success".to_string());
        connectivity.latency_ms = Some(777);
        let observation_value =
            encode_kv_value(&connectivity).expect("encode observation-only connectivity refresh");
        let observation_only = snapshot(
            8,
            vec![entry(&connectivity_key, observation_value.clone(), 8)],
            vec![put_event(8, &connectivity_key, observation_value)],
        );
        assert_eq!(
            projection.observe(&observation_only),
            DaemonConnectivityProjectionChange::default(),
            "connectivity timestamps and latency samples must not wake DFS lanes"
        );

        connectivity.state = sinomo_core::network::ClusterConnectivityState::Degraded;
        connectivity.latency_ms = None;
        connectivity.consecutive_failures = 1;
        connectivity.last_failure_at = Some("route failed".to_string());
        let degraded_value =
            encode_kv_value(&connectivity).expect("encode degraded connectivity record");
        let degraded_route = snapshot(
            9,
            vec![entry(&connectivity_key, degraded_value.clone(), 9)],
            vec![put_event(9, &connectivity_key, degraded_value)],
        );
        assert_eq!(
            projection.observe(&degraded_route),
            DaemonConnectivityProjectionChange {
                probe_inputs_changed: false,
                route_generation_changed: true,
            },
            "a committed reachability change must wake retained DFS lanes without feedback probing"
        );
    }

    #[test]
    fn connectivity_revision_coverage_accepts_multi_key_transaction_revision() {
        use sinomo_core::kv::{KvEvent, KvEventKind, KvRevision};

        let events = vec![
            KvEvent {
                revision: KvRevision(8),
                key: "/cluster/members/first".to_string(),
                kind: KvEventKind::Delete,
            },
            KvEvent {
                revision: KvRevision(8),
                key: "/cluster/endpoints/second".to_string(),
                kind: KvEventKind::Delete,
            },
        ];

        assert!(daemon_connectivity_events_cover_revision_range(
            7, 8, &events
        ));
    }

    #[test]
    fn committed_route_projection_applies_complete_delta_and_rebuilds_gaps() {
        use sinomo_core::kv::{
            ClusterKvCommittedDelta, ClusterKvCommittedKeyState, KvEntry, KvEvent, KvEventKind,
            KvRevision, KvSnapshot, KvVersion, encode_kv_value,
        };

        let now = OffsetDateTime::now_utc();
        let cluster_id = sinomo_core::node::ClusterId::generate();
        let remote_node = NodeId::generate();
        let member_key = sinomo_core::node::cluster_member_key(&remote_node);
        let mut member = ClusterMemberRecord::local_voter(
            cluster_id,
            remote_node.clone(),
            "remote",
            "public-key",
            now,
        );
        let initial_value = encode_kv_value(&member).expect("initial member");
        let initial_entry = KvEntry {
            key: member_key.clone(),
            value: initial_value,
            create_revision: KvRevision(1),
            modify_revision: KvRevision(1),
            version: KvVersion(1),
            lease_id: None,
        };
        let mut projection = DaemonConnectivityTopologyProjection::default();
        projection.seed_if_newer(&KvSnapshot {
            revision: KvRevision(1),
            compacted_revision: KvRevision::ZERO,
            entries: vec![initial_entry],
            leases: Vec::new(),
            events: Vec::new(),
            committed_requests: Vec::new(),
            expired_requests: Vec::new(),
        });

        member.endpoints = vec![sinomo_core::network::daemon_lan_endpoint(
            &remote_node,
            "127.0.0.1:42002".to_string(),
            10,
        )];
        let changed_value = encode_kv_value(&member).expect("changed member");
        let changed_entry = KvEntry {
            key: member_key.clone(),
            value: changed_value.clone(),
            create_revision: KvRevision(1),
            modify_revision: KvRevision(2),
            version: KvVersion(2),
            lease_id: None,
        };
        let delta = ClusterKvCommittedDelta {
            revision: KvRevision(2),
            compacted_revision: KvRevision::ZERO,
            events: vec![KvEvent {
                revision: KvRevision(2),
                key: member_key.clone(),
                kind: KvEventKind::Put {
                    value: changed_value,
                },
            }],
            final_key_states: vec![ClusterKvCommittedKeyState {
                key: member_key.clone(),
                entry: Some(changed_entry),
            }],
        };
        assert_eq!(
            projection.observe_delta(&delta),
            Some(DaemonConnectivityProjectionChange {
                probe_inputs_changed: true,
                route_generation_changed: true,
            })
        );
        assert_eq!(projection.revision(), Some(2));
        assert_eq!(
            projection.observe_delta(&delta),
            Some(DaemonConnectivityProjectionChange::default()),
            "an equal delta must not retrigger or regress the projection"
        );

        let mut gapped = delta.clone();
        gapped.revision = KvRevision(4);
        gapped.events[0].revision = KvRevision(4);
        gapped.final_key_states[0]
            .entry
            .as_mut()
            .expect("put state")
            .modify_revision = KvRevision(4);
        assert_eq!(
            projection.observe_delta(&gapped),
            None,
            "a missing revision must request a conservative full rebuild"
        );
        assert_eq!(projection.revision(), Some(2));

        let mut compacted = delta;
        compacted.revision = KvRevision(3);
        compacted.compacted_revision = KvRevision(3);
        compacted.events[0].revision = KvRevision(3);
        assert_eq!(
            projection.observe_delta(&compacted),
            None,
            "a delta beyond the retained baseline must request a full rebuild"
        );
        assert_eq!(projection.revision(), Some(2));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn external_critical_snapshot_advances_separate_connectivity_observer_once() {
        let root = sinomo_testdir::TestDir::new("daemon-connectivity-external-critical-snapshot");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data layout");
        let daemon_paths = DaemonPaths::for_shadow(&data_dir);
        daemon_paths.ensure().expect("daemon paths");
        let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let local_node =
            NodeId::from_legacy_named("node_connectivity_external_snapshot").expect("local node");
        let cluster_id = sinomo_core::node::ClusterId::from_legacy_named(
            "cluster_connectivity_external_snapshot",
        )
        .expect("cluster id");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id,
            "external connectivity snapshot",
            local_node.clone(),
            "local",
            "local-public-key",
            OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
            .expect("initial cluster KV");

        let subscriptions = subscribe_daemon_connectivity_topology_sources(
            &daemon_paths,
            &tokio::runtime::Handle::current(),
            false,
        )
        .expect("connectivity topology subscriptions");
        let initial_revision = subscriptions
            ._projection
            .lock()
            .expect("initial projection")
            .revision()
            .expect("initial revision");

        let current = sinomo_core::cluster::read_cluster_kv_current_snapshot(&cluster_paths)
            .expect("current cluster KV");
        let stale_snapshot = current.clone();
        let mut external_store =
            sinomo_core::kv::InProcessKvStore::restore(current).expect("external store");
        let member_key = sinomo_core::node::cluster_member_key(&local_node);
        let mut member = state.members[0].clone();
        member.endpoints = vec![sinomo_core::network::daemon_lan_endpoint(
            &local_node,
            "127.0.0.1:43002".to_string(),
            20,
        )];
        external_store
            .put_serde(member_key.clone(), &member, None)
            .expect("external critical member update");
        let external_snapshot = external_store.snapshot().expect("external snapshot");
        assert!(external_snapshot.revision.0 > initial_revision);

        // Deliberately bypass sinomo-cluster's in-process observers. Only the
        // shared materialized-snapshot watcher can deliver this generation to
        // the retained connectivity projection, matching a main/shadow split.
        let external_write_at = Instant::now();
        sinomo_core::kv::write_cluster_kv_snapshot(&cluster_paths.kv_paths(), &external_snapshot)
            .expect("external critical snapshot write");
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            let observed = subscriptions
                ._projection
                .lock()
                .expect("observed projection")
                .revision();
            if observed == Some(external_snapshot.revision.0) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "separate connectivity observer did not see the newer critical snapshot"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            external_write_at.elapsed()
                >= DAEMON_CONNECTIVITY_EXTERNAL_SNAPSHOT_DEBOUNCE
                    .saturating_sub(Duration::from_millis(20)),
            "the update must arrive through the debounced cross-process watcher"
        );

        let mut projection = subscriptions._projection.lock().expect("final projection");
        assert!(matches!(
            projection.values.get(&member_key),
            Some(DaemonConnectivityTopologyValue::Member(member))
                if member.endpoints.iter().any(|endpoint| endpoint.address == "127.0.0.1:43002")
        ));
        let observed_values = projection.values.clone();
        assert_eq!(
            projection.observe(&external_snapshot),
            DaemonConnectivityProjectionChange::default(),
            "an equal shared snapshot must not retrigger the observer"
        );
        assert!(stale_snapshot.revision < external_snapshot.revision);
        assert_eq!(
            projection.observe(&stale_snapshot),
            DaemonConnectivityProjectionChange::default(),
            "a stale shared snapshot must not regress or retrigger the observer"
        );
        assert_eq!(projection.revision(), Some(external_snapshot.revision.0));
        assert_eq!(projection.values, observed_values);
    }

    #[tokio::test]
    async fn sandbox_session_body_uses_standard_object_frames() {
        let expected_detail = "sandbox stream test".to_string();
        let frame = sinomo_sandbox::SandboxRunToolchainStreamFrame::Error {
            detail: expected_detail.clone(),
        };
        let (mut writer, mut reader) = tokio::io::duplex(4096);

        write_daemon_sandbox_stream_frame_async(&mut writer, &frame)
            .await
            .expect("write sandbox response frame");
        drop(writer);

        let response: sinomo_core::network::ObjectRpcResponse =
            sinomo_core::network::read_object_frame_async(
                &mut reader,
                sinomo_core::network::ObjectFrameIoOptions::none(),
            )
            .await
            .expect("read standard object frame");
        assert_eq!(
            response.route,
            sinomo_sandbox::SANDBOX_RUN_TOOLCHAIN_STREAM_ROUTE
        );
        let decoded: sinomo_sandbox::SandboxRunToolchainStreamFrame = response
            .decode_body()
            .expect("decode sandbox response body");
        match decoded {
            sinomo_sandbox::SandboxRunToolchainStreamFrame::Error { detail } => {
                assert_eq!(detail, expected_detail);
            }
            other => panic!("unexpected sandbox response frame: {other:?}"),
        }
    }

    #[tokio::test]
    async fn daemon_forward_fast_path_rejects_wrong_category_before_target_io() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-forward-fast-category");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("layout");
        let paths = DaemonPaths::new(&data_dir);
        paths.ensure().expect("daemon paths");
        let service_host =
            crate::service_adapter::DaemonServiceRegistryHost::from_registry_for_test(
                sinomo_core::service::ServiceRegistry::builder().build(),
                &paths,
            )
            .expect("empty service host");
        let runtime = tokio::runtime::Handle::current();
        let target_node =
            NodeId::from_legacy_named("node_forward_fast_category_target").expect("target node");
        let inner = sinomo_core::network::ObjectRpcRequest::encode_body(
            "/test/forward-fast-category-inner",
            "test",
            &(),
        )
        .expect("inner request");
        let authenticated = AuthenticatedDaemonCommand {
            route: String::new(),
            signer: crate::DaemonSignerAuthority::Local,
            signer_node_id: "local".to_string(),
            signer_public_key: "local-key".to_string(),
        };

        for route in [
            crate::DAEMON_OBJECT_RPC_FORWARD_ROUTE,
            crate::DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE,
            crate::DAEMON_OBJECT_RPC_FORWARD_MAIN_ROUTE,
            crate::DAEMON_DFS_OBJECT_RPC_FORWARD_MAIN_ROUTE,
        ] {
            let payload = crate::DaemonObjectRpcForwardRequest {
                target_node_id: target_node.clone(),
                request: inner.clone(),
                streamed_value_body: false,
                connect_timeout_millis: 5_000,
                idle_timeout_millis: None,
                overall_timeout_millis: None,
                observe_connectivity: false,
                destination_process_affinity: crate::DaemonObjectRpcDestinationProcessAffinity::Any,
            };
            let request = sinomo_core::network::ObjectRpcRequest::encode_body(
                route,
                "wrong-category",
                &payload,
            )
            .expect("outer request");
            let error = dispatch_daemon_object_rpc_call_request_async(
                &paths,
                &service_host,
                &runtime,
                &authenticated,
                sinomo_core::network::ObjectRpcIncomingRequest::value(request),
            )
            .await
            .expect_err("wrong category must fail before target I/O");
            assert!(
                error.to_string().contains("expected category daemon"),
                "unexpected category error: {error}"
            );
        }
    }

    #[test]
    fn local_relay_identity_route_marks_supervisor_restart_after_response() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-local-relay-restart-route");
        let data_dir = sinomo_core::local::SinomoDataDir::new(&root);
        let paths = DaemonPaths::new(&data_dir);
        let marker = root.join("rollout").join("supervised-main-restart.request");
        // DeliverLocal re-enters the common dispatcher, which returns the
        // inner route as its effective route. Forward retains the outer relay
        // route and always returns `stop = false`.
        let effective_route = sinomo_core::cluster::CLUSTER_CONTROL_JOIN_ROUTE.to_string();

        assert_eq!(
            effective_route,
            sinomo_core::cluster::CLUSTER_CONTROL_JOIN_ROUTE
        );
        crate::service_adapter::mark_cluster_identity_restart_after_response(
            &effective_route,
            true,
            Some(&paths),
        );
        assert_eq!(
            std::fs::read_to_string(marker).expect("local relay restart marker"),
            format!("{}\n", std::process::id())
        );
    }

    #[test]
    fn canonical_identity_response_failure_still_marks_supervisor_restart() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-canonical-response-failure-restart");
        let data_dir = sinomo_core::local::SinomoDataDir::new(&root);
        let paths = DaemonPaths::new(&data_dir);
        let marker = root.join("rollout").join("supervised-main-restart.request");
        let response_error = DaemonError::Network {
            address: "canonical response".to_string(),
            detail: "injected peer disconnect".to_string(),
        };

        let error = finish_daemon_object_rpc_stream_response(
            sinomo_core::cluster::CLUSTER_CONTROL_JOIN_ROUTE,
            true,
            &paths,
            Err(response_error),
        )
        .expect_err("the response transport error must still propagate");

        assert!(matches!(error, DaemonError::Network { .. }));
        assert_eq!(
            std::fs::read_to_string(marker).expect("canonical restart marker"),
            format!("{}\n", std::process::id()),
            "a committed identity mutation must restart even when its response is lost"
        );
    }

    #[test]
    fn daemon_network_module_context_runtime_task_gets_graceful_join_budget() {
        assert_eq!(
            daemon_runtime_task_join_timeout("network module context"),
            DAEMON_NETWORK_MODULE_CONTEXT_TASK_JOIN_TIMEOUT
        );
        assert!(
            !daemon_runtime_task_abort_before_join("network module context"),
            "network module shutdown keeps its graceful shutdown path"
        );
        assert!(
            daemon_runtime_task_join_timeout("network module context")
                > DAEMON_RUNTIME_TASK_JOIN_TIMEOUT,
            "network module shutdown needs more time than generic helper task shutdown"
        );
        assert_eq!(
            daemon_runtime_task_join_timeout("control accept"),
            DAEMON_RUNTIME_TASK_JOIN_TIMEOUT
        );
    }

    #[test]
    fn daemon_background_maintenance_shutdown_remains_bounded() {
        assert!(
            daemon_runtime_task_abort_before_join("background maintenance"),
            "background shutdown must start by cancelling non-owning orchestration work"
        );
        assert_eq!(
            daemon_runtime_task_join_timeout("background maintenance"),
            DAEMON_RUNTIME_TASK_JOIN_TIMEOUT,
        );
    }

    #[test]
    fn daemon_background_abort_explicitly_cancels_inflight_dfs_child() {
        struct MarkDropped(Arc<std::sync::atomic::AtomicBool>);

        impl Drop for MarkDropped {
            fn drop(&mut self) {
                self.0.store(true, std::sync::atomic::Ordering::Release);
            }
        }

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let child_dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let dropped_in_child = Arc::clone(&child_dropped);
        let background = runtime.spawn(async move {
            let dfs_generation = tokio::spawn(async move {
                let _drop_guard = MarkDropped(dropped_in_child);
                started_tx.send(()).expect("signal DFS generation start");
                std::future::pending::<Result<DaemonDfsMaintenanceTickOutcome, DaemonError>>().await
            });
            let _owned_generation = OwnedDaemonDfsMaintenanceTask::new(dfs_generation);
            std::future::pending::<()>().await;
        });
        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("DFS generation started");

        join_daemon_runtime_task(
            &runtime,
            "background maintenance",
            background,
            daemon_runtime_task_abort_before_join("background maintenance"),
        );
        runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(1), async {
                while !child_dropped.load(std::sync::atomic::Ordering::Acquire) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("owned DFS child cancellation");
        });
    }

    #[test]
    fn daemon_background_maintenance_is_fenced_to_startup_identity() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-maintenance-startup-identity");
        let data_dir = sinomo_core::local::SinomoDataDir::new(&root);
        let paths = DaemonPaths::new(&data_dir);
        let cluster_paths = cluster_paths_from_daemon_paths(&paths).expect("cluster paths");
        let cluster_id = sinomo_core::node::ClusterId::from_legacy_named(
            "daemon_maintenance_startup_identity_cluster",
        )
        .expect("cluster id");
        let node_id =
            NodeId::from_legacy_named("daemon_maintenance_startup_identity_node").expect("node id");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "maintenance startup identity",
            node_id.clone(),
            "node",
            "pk-node",
            OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("write state");
        let startup = DaemonClusterStatus {
            cluster_id,
            node_id,
            process: "daemon".to_string(),
            member_role: sinomo_core::ClusterMemberRole::Voter,
            member_state: sinomo_core::ClusterMemberState::Voter,
        };

        assert!(
            daemon_cluster_identity_matches_startup(&paths, Some(&startup))
                .expect("read matching startup identity")
        );
        let mut replacement = startup.clone();
        replacement.cluster_id = sinomo_core::node::ClusterId::from_legacy_named(
            "daemon_maintenance_replacement_cluster",
        )
        .expect("replacement cluster");
        assert!(
            !daemon_cluster_identity_matches_startup(&paths, Some(&replacement))
                .expect("read changed startup identity")
        );
        assert!(
            !daemon_cluster_identity_matches_startup(&paths, None)
                .expect("missing startup identity is a confirmed mismatch")
        );

        std::fs::write(&cluster_paths.state, b"malformed cluster state")
            .expect("write unreadable cluster state");
        assert!(
            daemon_cluster_identity_matches_startup(&paths, Some(&startup)).is_err(),
            "unreadable durable state must not be misclassified as a replacement identity"
        );
    }

    #[tokio::test]
    async fn daemon_shutdown_interrupts_inflight_non_owning_maintenance() {
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
        let maintenance = tokio::spawn(async move {
            wait_for_daemon_maintenance_or_shutdown(std::future::pending::<()>(), &mut shutdown_rx)
                .await
        });

        tokio::task::yield_now().await;
        shutdown_tx.send_replace(true);
        let result = tokio::time::timeout(Duration::from_secs(1), maintenance)
            .await
            .expect("shutdown must interrupt non-owning maintenance")
            .expect("maintenance task");

        assert_eq!(result, None);
    }

    #[test]
    fn daemon_status_response_survives_rollout_decoration_error() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-status-rollout-error");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("layout");
        let paths = DaemonPaths::new(&data_dir);
        paths.ensure().expect("daemon paths");
        let options = DaemonRunOptions::default();
        let expected = crate::DaemonStatus::new(&options, &paths, None);
        crate::write_daemon_status(&paths, &expected).expect("write daemon status");
        let rollout_paths = SelfRolloutPaths::new(&data_dir.root());
        fs::create_dir_all(&rollout_paths.root).expect("rollout dir");
        fs::write(
            rollout_paths.root.join("state.sobj"),
            b"not a rollout state",
        )
        .expect("malformed rollout state");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");

        let actual = runtime
            .block_on(daemon_status_response_async(&paths, Some(runtime.handle())))
            .expect("status response");

        assert_eq!(actual.pid, expected.pid);
        assert_eq!(actual.data_dir, expected.data_dir);
        assert!(actual.rollout.is_none());
    }

    #[test]
    fn daemon_liveness_status_response_uses_rollout_state_file() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-liveness-rollout-state");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("layout");
        let paths = DaemonPaths::new(&data_dir);
        paths.ensure().expect("daemon paths");
        let options = DaemonRunOptions::default();
        let mut raw = crate::DaemonStatus::new(&options, &paths, None);
        raw.rollout = Some(sinomo_core::ClusterNodeRolloutStatus {
            status: "in-progress".to_string(),
            mode: Some("cluster-remote-build".to_string()),
            detail: Some("building".to_string()),
            phase: Some("building".to_string()),
            progress_current: Some(2),
            progress_total: Some(5),
            progress_message: Some("building".to_string()),
            source: None,
            requested_at: None,
            staged_at: None,
            updated_at: None,
            failed_at: None,
            diagnostics: None,
            stdout_log: None,
            stderr_log: None,
            stdout_tail: None,
            stderr_tail: None,
        });
        crate::write_daemon_status(&paths, &raw).expect("write daemon status");
        let rollout_paths = SelfRolloutPaths::new(&data_dir.root());
        sinomo_rollout::write_rollout_status(
            &rollout_paths,
            &sinomo_core::ClusterNodeRolloutStatus {
                status: "accepted".to_string(),
                mode: Some("local-apply".to_string()),
                detail: Some("shadow rollout finalized".to_string()),
                phase: Some("shadow-finalized".to_string()),
                progress_current: Some(5),
                progress_total: Some(5),
                progress_message: Some("shadow rollout finalized".to_string()),
                source: None,
                requested_at: None,
                staged_at: None,
                updated_at: None,
                failed_at: None,
                diagnostics: None,
                stdout_log: None,
                stderr_log: None,
                stdout_tail: None,
                stderr_tail: None,
            },
        )
        .expect("write rollout status");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");

        let actual = runtime
            .block_on(daemon_status_response_async(&paths, None))
            .expect("liveness status");

        let rollout = actual.rollout.expect("decorated rollout status");
        assert_eq!(rollout.status, "accepted");
        assert_eq!(rollout.phase.as_deref(), Some("shadow-finalized"));
        assert_eq!(rollout.progress_current, Some(5));
    }

    #[test]
    fn shadow_liveness_status_response_uses_shared_rollout_state_file() {
        let root = sinomo_testdir::TestDir::new("sinomo-shadow-liveness-rollout-state");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("layout");
        let paths = DaemonPaths::for_shadow(&data_dir);
        paths.ensure().expect("shadow daemon paths");
        let options = DaemonRunOptions::default();
        let expected = crate::DaemonStatus::new(&options, &paths, None);
        crate::write_daemon_status(&paths, &expected).expect("write shadow daemon status");
        let rollout_paths = SelfRolloutPaths::new(&data_dir.root());
        sinomo_rollout::write_rollout_status(
            &rollout_paths,
            &sinomo_core::ClusterNodeRolloutStatus {
                status: "main-staged".to_string(),
                mode: Some("cluster-remote-build".to_string()),
                detail: None,
                phase: Some("main-staged".to_string()),
                progress_current: Some(5),
                progress_total: Some(5),
                progress_message: Some("candidate main staged".to_string()),
                source: Some("snapshot".to_string()),
                requested_at: Some("2026-08-08T22:37:42Z".to_string()),
                staged_at: Some("2026-08-08T22:40:00Z".to_string()),
                updated_at: None,
                failed_at: None,
                diagnostics: None,
                stdout_log: None,
                stderr_log: None,
                stdout_tail: None,
                stderr_tail: None,
            },
        )
        .expect("write shared rollout status");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");

        let actual = runtime
            .block_on(daemon_status_response_async(&paths, None))
            .expect("shadow liveness status");

        assert_eq!(actual.pid, expected.pid);
        let rollout = actual.rollout.expect("shared rollout status");
        assert_eq!(rollout.status, "main-staged");
        assert_eq!(rollout.phase.as_deref(), Some("main-staged"));
        assert_eq!(rollout.source.as_deref(), Some("snapshot"));
        assert_eq!(
            rollout.requested_at.as_deref(),
            Some("2026-08-08T22:37:42Z")
        );
    }

    fn append_cluster_raft_command(
        paths: &sinomo_core::network::ClusterPaths,
        node_id: &NodeId,
        command: sinomo_core::kv::KvCommand,
    ) -> Result<sinomo_core::kv::KvSnapshot, sinomo_core::network::NetworkError> {
        paths
            .kv_paths()
            .append_kv_command_and_publish(
                node_id,
                command,
                sinomo_core::kv::cluster_kv_key_can_defer_materialized_snapshot,
            )
            .map_err(sinomo_core::network::NetworkError::directory)
    }

    fn authorize_local_raft_voter_for_test(state: &mut sinomo_core::ClusterBootstrapState) {
        let capabilities =
            sinomo_core::node::main_daemon_capabilities(&sinomo_core::ClusterMemberRole::Voter);
        let member = state
            .members
            .iter_mut()
            .find(|member| member.node_id == state.local_node_id)
            .expect("local cluster member");
        member.capabilities = capabilities.clone();
        member.allowed_capabilities = capabilities;
    }

    fn cluster_module_rollout_statuses_for_test(
        paths: &sinomo_core::network::ClusterPaths,
    ) -> Result<Vec<ClusterModuleRolloutStatusRecord>, sinomo_core::network::NetworkError> {
        let state = sinomo_core::cluster::read_cluster_state_without_kv(paths)?;
        let snapshot = sinomo_core::cluster::read_cluster_kv_local_snapshot(paths, &state)?.value;
        sinomo_rollout::project_cluster_module_rollout_statuses_from_snapshot(&snapshot)
            .map_err(sinomo_core::network::NetworkError::directory)
    }

    #[test]
    fn raw_ordinary_object_rpc_is_rejected_without_dispatch() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-auth-rpc-error");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("layout");
        let paths = DaemonPaths::new(&data_dir);
        let request = sinomo_core::network::ObjectRpcRequest::encode_body(
            sinomo_rollout::ROLLOUT_DIAGNOSTICS_ROUTE,
            sinomo_rollout::ROLLOUT_RPC_CATEGORY,
            &(),
        )
        .expect("request");
        let control_request = request.with_auth(b"not-a-signed-command".to_vec());
        let (address_tx, address_rx) = std::sync::mpsc::channel();
        let server = thread::spawn(move || {
            let runtime = crate::build_daemon_async_runtime("daemon-auth-test").expect("runtime");
            let service_runtime = runtime.handle().clone();
            runtime
                .block_on(async move {
                    let listener =
                        sinomo_core::network::AsyncDaemonControlListener::bind("127.0.0.1:0")
                            .await
                            .expect("listener");
                    let stream_address = listener.local_addr_string().expect("listener address");
                    address_tx
                        .send(stream_address.clone())
                        .expect("send listener address");
                    let service_host =
                        crate::service_adapter::DaemonServiceRegistryHost::new_on_runtime(
                            paths.clone(),
                            service_runtime.clone(),
                        )
                        .await
                        .expect("service host");
                    let stream = listener.accept().await.expect("accept");
                    handle_daemon_connection_async(
                        stream,
                        paths,
                        service_host,
                        service_runtime,
                        stream_address,
                    )
                    .await
                })
                .expect("auth failure is handled as a closed connection");
        });

        let address = address_rx.recv().expect("listener address");
        let client_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("client runtime");
        let error = client_runtime
            .block_on(async {
                let timeouts = sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(
                    Duration::from_secs(2),
                );
                let frame_options = sinomo_core::network::ObjectFrameIoOptions::none()
                    .with_idle_timeout(timeouts.idle_timeout);
                let mut stream =
                    sinomo_core::network::AsyncDaemonControlStream::connect_with_options(
                        &address, timeouts,
                    )
                    .await?
                    .into_inner();
                sinomo_core::network::write_object_frame_async(
                    &mut stream,
                    &control_request,
                    frame_options,
                )
                .await?;
                sinomo_core::network::read_object_frame_async::<
                    _,
                    sinomo_core::network::ObjectRpcResponse,
                >(&mut stream, frame_options)
                .await
            })
            .expect_err("raw ordinary RPC must be rejected");
        assert!(
            error.to_string().contains("peer closed"),
            "unexpected raw RPC rejection: {error}"
        );
        server.join().expect("server thread");
    }

    fn test_local_object_rpc_carrier_scope(data_dir: &SinomoDataDir, address: &str) -> String {
        let public_key = sinomo_core::network::cached_daemon_signing_public_key(data_dir)
            .expect("local daemon public key");
        let endpoint_node_id = NodeId::derived("sinomo.network.local-object-rpc-call", &public_key);
        format!("local-object-rpc:{address}:{endpoint_node_id}")
    }

    fn retire_test_local_object_rpc_carrier(
        runtime: &tokio::runtime::Runtime,
        data_dir: &SinomoDataDir,
        address: &str,
    ) {
        let scope = test_local_object_rpc_carrier_scope(data_dir, address);
        assert_eq!(
            runtime.block_on(sinomo_core::network::retire_pooled_routed_streams(&scope)),
            1,
            "one test-local carrier should be retired"
        );
    }

    struct FailingRegistryService;

    impl sinomo_core::service::Service for FailingRegistryService {
        fn name(&self) -> &'static str {
            "failing-registry"
        }

        fn routes(&self) -> &'static [&'static str] {
            &["/benchmarks/cancel-probe"]
        }

        fn route_categories(&self) -> Vec<sinomo_core::service::ServiceRoute> {
            vec![
                sinomo_core::service::ServiceRoute::new("/benchmarks/cancel-probe", "benchmarks")
                    .with_concurrency_limit(1),
            ]
        }

        fn handle(
            &self,
            _ctx: sinomo_core::service::ServiceContext,
            _request: sinomo_core::network::ObjectRpcRequest,
        ) -> sinomo_core::service::ServiceFuture<'_> {
            Box::pin(async {
                Err(sinomo_core::service::ServiceError::Service(
                    "injected registry failure".to_string(),
                ))
            })
        }
    }

    struct PermissionDeniedRegistryService;

    impl sinomo_core::service::Service for PermissionDeniedRegistryService {
        fn name(&self) -> &'static str {
            "permission-denied-registry"
        }

        fn routes(&self) -> &'static [&'static str] {
            &["/test/permission-denied-session"]
        }

        fn route_categories(&self) -> Vec<sinomo_core::service::ServiceRoute> {
            vec![
                sinomo_core::service::ServiceRoute::new("/test/permission-denied-session", "test")
                    .with_auth_scope("authenticated")
                    .require_local_permission("remote-management"),
            ]
        }

        fn handle(
            &self,
            _ctx: sinomo_core::service::ServiceContext,
            _request: sinomo_core::network::ObjectRpcRequest,
        ) -> sinomo_core::service::ServiceFuture<'_> {
            Box::pin(async {
                Err(sinomo_core::service::ServiceError::Service(
                    "permission-denied handler must not run".to_string(),
                ))
            })
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn authenticated_transport_prelude_is_detected_without_consumption() {
        let listener = sinomo_core::network::AsyncDaemonControlListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr_string().expect("listener address");
        let metadata = sinomo_core::network::AuthenticatedTransportMetadata::try_new(
            ClusterTransportKind::Iroh,
            "test-transport-principal",
            *blake3::hash(b"daemon authenticated prelude detector").as_bytes(),
            b"test-transport-proof".to_vec(),
            sinomo_core::network::DirectStreamCapabilities::multiplexed_full_duplex(8),
        )
        .expect("metadata");
        let expected = metadata.clone();
        let client = tokio::spawn(async move {
            let mut stream = tokio::net::TcpStream::connect(address)
                .await
                .expect("connect");
            sinomo_core::network::write_authenticated_transport_prelude(&mut stream, &metadata)
                .await
                .expect("write prelude");
        });
        let stream = listener.accept().await.expect("accept");

        assert!(
            daemon_control_stream_uses_authenticated_transport_prelude(&stream)
                .await
                .expect("detect prelude")
        );
        let mut raw = stream.into_inner();
        assert_eq!(
            sinomo_core::network::read_authenticated_transport_prelude(&mut raw)
                .await
                .expect("read unconsumed prelude"),
            expected
        );
        client.await.expect("client task");
    }

    #[test]
    fn authenticated_registry_failure_returns_session_error() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-registry-rpc-error");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("layout");
        let paths = DaemonPaths::new(&data_dir);
        let request = sinomo_core::network::ObjectRpcRequest::encode_body(
            "/benchmarks/cancel-probe",
            "benchmarks",
            &(),
        )
        .expect("request");
        let (address_tx, address_rx) = std::sync::mpsc::channel();
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        let server = thread::spawn(move || {
            let runtime =
                crate::build_daemon_async_runtime("daemon-registry-error-test").expect("runtime");
            let service_runtime = runtime.handle().clone();
            runtime
                .block_on(async move {
                    let listener =
                        sinomo_core::network::AsyncDaemonControlListener::bind("127.0.0.1:0")
                            .await
                            .expect("listener");
                    let stream_address = listener.local_addr_string().expect("listener address");
                    address_tx
                        .send(stream_address.clone())
                        .expect("send listener address");
                    let service_host =
                        crate::service_adapter::DaemonServiceRegistryHost::new_on_runtime_with_components(
                            paths.clone(),
                            service_runtime.clone(),
                            vec![Arc::new(
                                sinomo_module::StaticComponentFactory::new(
                                    sinomo_module::ComponentDescriptor::new(
                                        "test.failing-registry",
                                        "1.0.0",
                                    ),
                                    |_, scope| {
                                        scope.set_services(
                                            sinomo_core::service::ServiceRegistry::builder()
                                                .register(FailingRegistryService)
                                                .build(),
                                        );
                                        Ok(())
                                    },
                                )
                                .expect("failing registry test component"),
                            )],
                        )
                        .await
                        .expect("service host");
                    let stream = listener.accept().await.expect("accept");
                    tokio::select! {
                        result = handle_daemon_connection_async(
                            stream,
                            paths,
                            service_host,
                            service_runtime,
                            stream_address,
                        ) => result,
                        _ = stop_rx => Ok(false),
                    }
                })
                .expect("registry failure is returned through the session");
        });

        let address = address_rx.recv().expect("listener address");
        let client_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("client runtime");
        let client_paths = DaemonPaths::new(&data_dir);
        let mut host = crate::client::DaemonObjectRpcAsyncHost::new(client_runtime.handle(), false)
            .with_paths(&client_paths);
        let error = client_runtime
            .block_on(
                sinomo_core::network::ClusterObjectRpcClient::endpoint_only(&mut host)
                    .call_loopback(
                        &data_dir,
                        &address,
                        sinomo_core::network::ObjectRpcCall::value(request),
                        sinomo_core::network::ClusterObjectRpcOptions::new(
                            sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(
                                Duration::from_secs(2),
                            ),
                        ),
                    ),
            )
            .expect_err("handler failures are session errors");
        assert!(
            error.to_string().contains("injected registry failure"),
            "unexpected error detail: {error}"
        );
        retire_test_local_object_rpc_carrier(&client_runtime, &data_dir, &address);
        stop_tx.send(()).expect("stop test carrier server");
        server.join().expect("server thread");
    }

    #[test]
    fn node_local_permission_denial_returns_structured_session_error() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-permission-rpc-error");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("layout");
        let paths = DaemonPaths::new(&data_dir);
        let request = sinomo_core::network::ObjectRpcRequest::encode_body(
            "/test/permission-denied-session",
            "test",
            &(),
        )
        .expect("request");
        let (address_tx, address_rx) = std::sync::mpsc::channel();
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        let server = thread::spawn(move || {
            let runtime =
                crate::build_daemon_async_runtime("daemon-permission-error-test").expect("runtime");
            let service_runtime = runtime.handle().clone();
            let stop = runtime
                .block_on(async move {
                    let listener =
                        sinomo_core::network::AsyncDaemonControlListener::bind("127.0.0.1:0")
                            .await
                            .expect("listener");
                    let stream_address = listener.local_addr_string().expect("listener address");
                    address_tx
                        .send(stream_address.clone())
                        .expect("send listener address");
                    let service_host = crate::service_adapter::DaemonServiceRegistryHost::new_on_runtime_with_components(
                        paths.clone(),
                        service_runtime.clone(),
                        vec![Arc::new(
                            sinomo_module::StaticComponentFactory::new(
                                sinomo_module::ComponentDescriptor::new(
                                    "test.permission-denied-registry",
                                    "1.0.0",
                                ),
                                |_, scope| {
                                    scope.set_services(
                                        sinomo_core::service::ServiceRegistry::builder()
                                            .register(PermissionDeniedRegistryService)
                                            .build(),
                                    );
                                    Ok(())
                                },
                            )
                            .expect("permission registry test component"),
                        )],
                    )
                    .await
                    .expect("service host");
                    let stream = listener.accept().await.expect("accept");
                    tokio::select! {
                        result = handle_daemon_connection_async(
                            stream,
                            paths,
                            service_host,
                            service_runtime,
                            stream_address,
                        ) => result,
                        _ = stop_rx => Ok(false),
                    }
                })
                .expect("node-local permission rejection is returned to the client");
            assert!(!stop, "permission rejection must not stop the daemon");
        });

        let address = address_rx.recv().expect("listener address");
        let client_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("client runtime");
        let client_paths = DaemonPaths::new(&data_dir);
        let mut host = crate::client::DaemonObjectRpcAsyncHost::new(client_runtime.handle(), false)
            .with_paths(&client_paths);
        let error = client_runtime
            .block_on(
                sinomo_core::network::ClusterObjectRpcClient::endpoint_only(&mut host)
                    .call_loopback(
                        &data_dir,
                        &address,
                        sinomo_core::network::ObjectRpcCall::value(request),
                        sinomo_core::network::ClusterObjectRpcOptions::new(
                            sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(
                                Duration::from_secs(2),
                            ),
                        ),
                    ),
            )
            .expect_err("permission denial must be returned as an RPC error");
        assert!(
            matches!(error, sinomo_core::network::NetworkError::AuthDenied),
            "unexpected permission error: {error}"
        );
        assert!(
            !error
                .to_string()
                .contains("expected object RPC response head"),
            "permission denial regressed to transport EOF: {error}"
        );
        retire_test_local_object_rpc_carrier(&client_runtime, &data_dir, &address);
        stop_tx.send(()).expect("stop test carrier server");
        server.join().expect("server thread");
    }

    #[test]
    fn signed_dfs_status_dispatches_through_stream_native_rpc_for_cluster_member() {
        let target_root = sinomo_testdir::TestDir::new("sinomo-daemon-control-dfs-status-target");
        let target_data_dir = SinomoDataDir::new(&target_root);
        target_data_dir.ensure_root_layout().expect("target layout");
        let target_daemon_paths = DaemonPaths::new(&target_data_dir);
        let target_identity =
            sinomo_core::node::load_or_create_main_node_identity(&target_data_dir.root())
                .expect("target identity");
        let peer_root = sinomo_testdir::TestDir::new("sinomo-daemon-control-dfs-status-peer");
        let peer_data_dir = SinomoDataDir::new(&peer_root);
        peer_data_dir.ensure_root_layout().expect("peer layout");
        let peer_identity =
            sinomo_core::node::load_or_create_main_node_identity(&peer_data_dir.root())
                .expect("peer identity");
        let cluster_id =
            sinomo_core::node::ClusterId::from_legacy_named("cluster_daemon_control_dfs_status")
                .expect("cluster id");
        let target_node = NodeId::from_legacy_named("node_daemon_control_dfs_status_target")
            .expect("target node");
        let peer_node =
            NodeId::from_legacy_named("node_daemon_control_dfs_status_peer").expect("peer node");
        let now = OffsetDateTime::now_utc();

        let mut target_state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "control-dfs-status",
            target_node.clone(),
            "target",
            target_identity.public_key.clone(),
            now,
        );
        target_state
            .members
            .push(sinomo_core::ClusterMemberRecord::local_non_voting(
                cluster_id.clone(),
                peer_node.clone(),
                "peer",
                peer_identity.public_key.clone(),
                now,
            ));
        sinomo_core::cluster::write_cluster_state(
            &sinomo_core::network::ClusterPaths::new(&target_data_dir),
            &target_state,
        )
        .expect("write target state");

        let mut peer_state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "control-dfs-status",
            peer_node,
            "peer",
            peer_identity.public_key,
            now,
        );
        peer_state
            .members
            .push(sinomo_core::ClusterMemberRecord::local_voter(
                cluster_id,
                target_node,
                "target",
                target_identity.public_key,
                now,
            ));
        sinomo_core::cluster::write_cluster_state(
            &sinomo_core::network::ClusterPaths::new(&peer_data_dir),
            &peer_state,
        )
        .expect("write peer state");

        let request = sinomo_dfs::dfs_status_rpc_request()
            .expect("dfs status request")
            .with_trace_context_if_missing();
        let signed =
            sign_daemon_rpc_request_from_data_dir(&peer_data_dir, &request).expect("signed rpc");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");

        let data_owner = Arc::new(
            crate::DaemonDataOwnerLock::acquire(&target_daemon_paths)
                .expect("target main data owner"),
        );
        crate::dfs::register_daemon_dfs_data_owner_lock(&target_daemon_paths, &data_owner)
            .expect("register target main data owner");

        let request = request.with_auth(signed);
        let service_host = runtime
            .block_on(
                crate::service_adapter::DaemonServiceRegistryHost::new_on_runtime(
                    target_daemon_paths.clone(),
                    runtime.handle().clone(),
                ),
            )
            .expect("daemon service host");
        let authenticated =
            crate::service_adapter::authenticate_daemon_control_request_via_registry(
                &target_daemon_paths,
                &request,
                &service_host,
            )
            .expect("authenticate cluster member");
        let (response, stop) = runtime
            .block_on(
                crate::service_adapter::dispatch_rpc_via_registry_with_body_async(
                    &service_host,
                    &authenticated,
                    sinomo_core::network::ObjectRpcIncomingRequest::value(request),
                    Some(&target_daemon_paths),
                ),
            )
            .expect("stream-native daemon RPC dispatch");

        assert!(!stop);
        assert!(
            response.response.body.is_empty(),
            "typed DFS status must stay out of the legacy response head"
        );
        assert!(
            !response.body.is_empty(),
            "typed DFS status must use the owned response-value stream"
        );
        let response = runtime
            .block_on(response.into_materialized_value_response(usize::MAX))
            .expect("materialize test-only DFS status response");
        let status =
            sinomo_dfs::decode_dfs_status_rpc_response(&response).expect("dfs status response");
        assert_eq!(status.file_system_count, 0);
    }

    #[test]
    fn signed_shadow_dfs_dispatch_rejects_before_raft_or_backend_hydration() {
        let target_root = sinomo_testdir::TestDir::new("sinomo-shadow-dfs-no-raft-target");
        let target_data_dir = SinomoDataDir::new(&target_root);
        target_data_dir.ensure_root_layout().expect("target layout");
        let target_daemon_paths = DaemonPaths::for_shadow(&target_data_dir);
        let target_identity =
            sinomo_core::node::load_or_create_main_node_identity(&target_data_dir.root())
                .expect("target identity");
        let peer_root = sinomo_testdir::TestDir::new("sinomo-shadow-dfs-no-raft-peer");
        let peer_data_dir = SinomoDataDir::new(&peer_root);
        peer_data_dir.ensure_root_layout().expect("peer layout");
        let peer_identity =
            sinomo_core::node::load_or_create_main_node_identity(&peer_data_dir.root())
                .expect("peer identity");
        let cluster_id =
            sinomo_core::node::ClusterId::from_legacy_named("cluster_shadow_dfs_no_raft")
                .expect("cluster id");
        let target_node =
            NodeId::from_legacy_named("node_shadow_dfs_no_raft_target").expect("target node");
        let peer_node =
            NodeId::from_legacy_named("node_shadow_dfs_no_raft_peer").expect("peer node");
        let now = OffsetDateTime::now_utc();
        let mut target_state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "shadow DFS no Raft",
            target_node.clone(),
            "target",
            target_identity.public_key.clone(),
            now,
        );
        target_state
            .members
            .push(sinomo_core::ClusterMemberRecord::local_non_voting(
                cluster_id.clone(),
                peer_node.clone(),
                "peer",
                peer_identity.public_key.clone(),
                now,
            ));
        let target_cluster_paths = sinomo_core::network::ClusterPaths::new(&target_data_dir);
        sinomo_core::cluster::write_cluster_state(&target_cluster_paths, &target_state)
            .expect("write target state");

        let mut peer_state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "shadow DFS no Raft",
            peer_node,
            "peer",
            peer_identity.public_key,
            now,
        );
        peer_state
            .members
            .push(sinomo_core::ClusterMemberRecord::local_voter(
                cluster_id,
                target_node.clone(),
                "target",
                target_identity.public_key,
                now,
            ));
        sinomo_core::cluster::write_cluster_state(
            &sinomo_core::network::ClusterPaths::new(&peer_data_dir),
            &peer_state,
        )
        .expect("write peer state");

        crate::cluster::deny_shadow_daemon_local_raft_access(&target_daemon_paths)
            .expect("install shadow local-Raft denial");
        let request = sinomo_dfs::dfs_status_rpc_request()
            .expect("DFS status request")
            .with_trace_context_if_missing();
        let signed = sign_daemon_rpc_request_from_data_dir(&peer_data_dir, &request)
            .expect("signed DFS request");
        let request = request.with_auth(signed);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let service_host = runtime
            .block_on(
                crate::service_adapter::DaemonServiceRegistryHost::new_on_runtime(
                    target_daemon_paths.clone(),
                    runtime.handle().clone(),
                ),
            )
            .expect("shadow service host");
        let authenticated =
            crate::service_adapter::authenticate_daemon_control_request_via_registry(
                &target_daemon_paths,
                &request,
                &service_host,
            )
            .expect("authenticate accepted peer from passive membership projection");
        let error = runtime
            .block_on(
                crate::service_adapter::dispatch_rpc_via_registry_with_body_async(
                    &service_host,
                    &authenticated,
                    sinomo_core::network::ObjectRpcIncomingRequest::value(request),
                    Some(&target_daemon_paths),
                ),
            )
            .expect_err("shadow DFS host must fail closed without main data ownership");
        assert!(
            error.to_string().contains("data-owner capability"),
            "unexpected shadow DFS rejection: {error}"
        );
        assert_eq!(
            crate::dfs::daemon_dfs_test_message_service_lane_open_count(&target_data_dir),
            0,
            "shadow rejection must precede DFS service-lane hydration"
        );
        let kv_paths = target_cluster_paths.kv_paths();
        let wal_paths = kv_paths.raft_wal_paths(&target_node);
        assert!(
            !wal_paths.root.exists(),
            "signed shadow dispatch must not create the main voter's Raft node root"
        );

        assert!(sinomo_core::kv::clear_process_local_raft_access_denial_for_test(&kv_paths.raft));
        drop(
            sinomo_core::kv::DurableRaftKvNode::open(target_node, wal_paths)
                .expect("fresh main can acquire node.lock after shadow rejection"),
        );
    }

    #[test]
    fn daemon_rollout_telemetry_tick_advances_from_event_log() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-rollout-telemetry");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("layout");
        let daemon_paths = DaemonPaths::new(&data_dir);
        let rollout_paths = SelfRolloutPaths::new(&root);
        let node = daemon_rollout_controller_node_id(&root).expect("node id");
        let module_id =
            sinomo_core::Id::from_legacy_named("module.trigger.silence_policy").expect("module");
        let artifact_id =
            sinomo_core::Id::from_legacy_named("artifact_daemon_candidate").expect("artifact");
        let active_artifact_id =
            sinomo_core::Id::from_legacy_named("artifact_daemon_active").expect("active");
        let plan = sinomo_core::RolloutPlan {
            rollout_plan_id: sinomo_core::Id::from_legacy_named("rollout_daemon_telemetry")
                .expect("rollout"),
            artifact_id: artifact_id.clone(),
            module_id: module_id.clone(),
            target_ring: sinomo_core::TrustRing::R3SandboxedModule,
            strategy: sinomo_core::RolloutStrategy::Shadow,
            target_nodes: vec![node.clone()],
            initial_weight_bps: 0,
            promotion_steps_bps: vec![500],
            rollback_policy: sinomo_core::RollbackPolicy::default(),
        };
        let controller = RolloutController::new(node.clone());
        let state = controller.initial_state(plan, active_artifact_id.clone());
        sinomo_rollout::write_rollout_runtime_state(&rollout_paths, &state).expect("runtime state");
        sinomo_rollout::write_rollout_telemetry_gate_config(
            &rollout_paths,
            &sinomo_rollout::RolloutTelemetryGateConfig {
                automatic_promotion_enabled: true,
                ..Default::default()
            },
        )
        .expect("telemetry gate config");
        let mut event_store =
            sinomo_core::events::JsonlEventStore::new(root.join("events").join("event-log.jsonl"));
        event_store
            .append(module_shadow_decision_event(
                &node,
                &module_id,
                &active_artifact_id,
                &artifact_id,
                true,
            ))
            .expect("append telemetry");

        let applied = run_daemon_rollout_telemetry_reconciliation_tick(&daemon_paths)
            .expect("telemetry tick")
            .applied_transition
            .expect("applied transition");

        assert_eq!(
            applied.transition.action,
            sinomo_rollout::RolloutAction::AdvanceCanary
        );
        assert_eq!(applied.transition.current_weight_bps, 500);
        assert!(
            sinomo_rollout::read_rollout_telemetry_advance_state(&rollout_paths)
                .expect("checkpoint")
                .is_some()
        );
    }

    #[test]
    fn daemon_rollout_telemetry_tick_respects_configured_promotion_gates() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-rollout-telemetry-gated");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("layout");
        let daemon_paths = DaemonPaths::new(&data_dir);
        let rollout_paths = SelfRolloutPaths::new(&root);
        let node = daemon_rollout_controller_node_id(&root).expect("node id");
        let module_id =
            sinomo_core::Id::from_legacy_named("module.trigger.silence_policy").expect("module");
        let artifact_id = sinomo_core::Id::from_legacy_named("artifact_daemon_gated_candidate")
            .expect("artifact");
        let active_artifact_id =
            sinomo_core::Id::from_legacy_named("artifact_daemon_gated_active").expect("active");
        let plan = sinomo_core::RolloutPlan {
            rollout_plan_id: sinomo_core::Id::from_legacy_named("rollout_daemon_gated_telemetry")
                .expect("rollout"),
            artifact_id: artifact_id.clone(),
            module_id: module_id.clone(),
            target_ring: sinomo_core::TrustRing::R3SandboxedModule,
            strategy: sinomo_core::RolloutStrategy::Shadow,
            target_nodes: vec![node.clone()],
            initial_weight_bps: 0,
            promotion_steps_bps: vec![500],
            rollback_policy: sinomo_core::RollbackPolicy::default(),
        };
        let controller = RolloutController::new(node.clone());
        let state = controller.initial_state(plan, active_artifact_id.clone());
        sinomo_rollout::write_rollout_runtime_state(&rollout_paths, &state).expect("runtime state");
        sinomo_rollout::write_rollout_telemetry_gate_config(
            &rollout_paths,
            &sinomo_rollout::RolloutTelemetryGateConfig {
                automatic_promotion_enabled: true,
                maximum_automatic_weight_bps: Some(250),
                ..Default::default()
            },
        )
        .expect("telemetry gate config");
        let mut event_store =
            sinomo_core::events::JsonlEventStore::new(root.join("events").join("event-log.jsonl"));
        event_store
            .append(module_shadow_decision_event(
                &node,
                &module_id,
                &active_artifact_id,
                &artifact_id,
                true,
            ))
            .expect("append telemetry");

        let applied = run_daemon_rollout_telemetry_reconciliation_tick(&daemon_paths)
            .expect("telemetry tick")
            .applied_transition;

        assert_eq!(applied, None);
        assert_eq!(
            sinomo_rollout::read_rollout_runtime_state(&rollout_paths)
                .expect("runtime state")
                .expect("runtime state"),
            state
        );
    }

    #[test]
    fn daemon_rollout_telemetry_tick_uses_canary_events_in_canary_stage() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-rollout-canary-telemetry");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("layout");
        let daemon_paths = DaemonPaths::new(&data_dir);
        let rollout_paths = SelfRolloutPaths::new(&root);
        let node = daemon_rollout_controller_node_id(&root).expect("node id");
        let module_id =
            sinomo_core::Id::from_legacy_named("module.trigger.silence_policy").expect("module");
        let artifact_id = sinomo_core::Id::from_legacy_named("artifact_daemon_canary_candidate")
            .expect("artifact");
        let active_artifact_id =
            sinomo_core::Id::from_legacy_named("artifact_daemon_canary_active").expect("active");
        let plan = sinomo_core::RolloutPlan {
            rollout_plan_id: sinomo_core::Id::from_legacy_named("rollout_daemon_canary_telemetry")
                .expect("rollout"),
            artifact_id: artifact_id.clone(),
            module_id: module_id.clone(),
            target_ring: sinomo_core::TrustRing::R3SandboxedModule,
            strategy: sinomo_core::RolloutStrategy::Shadow,
            target_nodes: vec![node.clone()],
            initial_weight_bps: 0,
            promotion_steps_bps: vec![500, 2_500],
            rollback_policy: sinomo_core::RollbackPolicy::default(),
        };
        let controller = RolloutController::new(node.clone());
        let mut state = controller.initial_state(plan, active_artifact_id);
        state.stage = sinomo_rollout::RolloutStage::Canary;
        state.current_weight_bps = 500;
        state.loader_state.hot_swap_state = sinomo_core::HotSwapState::ActiveCandidate;
        sinomo_rollout::write_rollout_runtime_state(&rollout_paths, &state).expect("runtime state");
        sinomo_rollout::write_rollout_telemetry_gate_config(
            &rollout_paths,
            &sinomo_rollout::RolloutTelemetryGateConfig {
                automatic_promotion_enabled: true,
                maximum_automatic_weight_bps: Some(2_500),
                ..Default::default()
            },
        )
        .expect("telemetry gate config");
        let mut event_store =
            sinomo_core::events::JsonlEventStore::new(root.join("events").join("event-log.jsonl"));
        event_store
            .append(module_invocation_decision_event(
                &node,
                &module_id,
                &artifact_id,
                "candidate",
                None,
            ))
            .expect("append telemetry");

        let applied = run_daemon_rollout_telemetry_reconciliation_tick(&daemon_paths)
            .expect("telemetry tick")
            .applied_transition
            .expect("applied transition");

        assert_eq!(
            applied.transition.action,
            sinomo_rollout::RolloutAction::AdvanceCanary
        );
        assert_eq!(applied.transition.current_weight_bps, 2_500);
        assert_eq!(
            sinomo_rollout::read_rollout_telemetry_advance_state(&rollout_paths)
                .expect("checkpoint")
                .expect("checkpoint")
                .telemetry_kind,
            "module.invocation.decision"
        );
    }

    #[test]
    fn daemon_publishes_module_rollout_status_to_cluster_kv() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-module-rollout-status");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("layout");
        let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let cluster_id =
            sinomo_core::node::ClusterId::from_legacy_named("cluster_daemon_module_rollout_status")
                .expect("cluster");
        let node = NodeId::from_legacy_named("node_daemon_module_rollout_status").expect("node id");
        let mut cluster_state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "daemon module rollout status",
            node.clone(),
            "daemon node",
            "public-key",
            OffsetDateTime::now_utc(),
        );
        authorize_local_raft_voter_for_test(&mut cluster_state);
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &cluster_state)
            .expect("write cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(
            &cluster_paths,
            &cluster_state,
        )
        .expect("sync cluster state");
        sinomo_core::cluster::locked_set_cluster_raft_leader(
            &cluster_paths,
            &cluster_state.local_node_id,
            1,
        )
        .expect("set local raft leader");

        let rollout_paths = SelfRolloutPaths::new(&root);
        let module_id =
            sinomo_core::Id::from_legacy_named("module.trigger.silence_policy").expect("module");
        let artifact_id = sinomo_core::Id::from_legacy_named("artifact_daemon_status_candidate")
            .expect("artifact");
        let active_artifact_id =
            sinomo_core::Id::from_legacy_named("artifact_daemon_status_active").expect("active");
        let plan = sinomo_core::RolloutPlan {
            rollout_plan_id: sinomo_core::Id::from_legacy_named("rollout_daemon_status")
                .expect("rollout"),
            artifact_id: artifact_id.clone(),
            module_id: module_id.clone(),
            target_ring: sinomo_core::TrustRing::R3SandboxedModule,
            strategy: sinomo_core::RolloutStrategy::Shadow,
            target_nodes: vec![node.clone()],
            initial_weight_bps: 0,
            promotion_steps_bps: vec![500],
            rollback_policy: sinomo_core::RollbackPolicy::default(),
        };
        let controller = RolloutController::new(node.clone());
        let mut state = controller.initial_state(plan, active_artifact_id.clone());
        state.stage = sinomo_rollout::RolloutStage::Canary;
        state.current_weight_bps = 500;
        state.loader_state.hot_swap_state = sinomo_core::HotSwapState::ActiveCandidate;
        sinomo_rollout::write_rollout_runtime_state(&rollout_paths, &state).expect("runtime state");
        sinomo_rollout::write_rollout_telemetry_advance_state(
            &rollout_paths,
            &sinomo_rollout::RolloutTelemetryAdvanceState {
                rollout_plan_id: state.plan.rollout_plan_id.clone(),
                module_id: module_id.clone(),
                candidate_artifact_id: artifact_id.clone(),
                telemetry_kind: "module.invocation.decision".to_string(),
                last_consumed_sequence: Some(sinomo_core::events::EventSequence(7)),
                last_transition_record_id: Some(
                    sinomo_core::Id::from_legacy_named("transition_daemon_status")
                        .expect("transition"),
                ),
                updated_at: "2026-06-01T00:00:00Z".to_string(),
            },
        )
        .expect("telemetry checkpoint");

        let published = publish_daemon_module_rollout_status_from_data_dir(&root, &rollout_paths)
            .expect("publish module rollout status")
            .expect("published status");
        let statuses =
            cluster_module_rollout_statuses_for_test(&cluster_paths).expect("cluster statuses");

        assert_eq!(published.cluster_id, cluster_id);
        assert_eq!(published.node_id, node);
        assert_eq!(published.module_id, module_id);
        assert_eq!(published.stage, "canary");
        assert_eq!(published.current_weight_bps, 500);
        assert_eq!(published.active_artifact_id, active_artifact_id);
        assert_eq!(published.candidate_artifact_id, Some(artifact_id));
        assert_eq!(
            published.telemetry_kind.as_deref(),
            Some("module.invocation.decision")
        );
        assert_eq!(published.telemetry_last_consumed_sequence, Some(7));
        assert_eq!(statuses, vec![published]);
    }

    #[test]
    fn daemon_opens_configured_sharded_dfs_metadata_backend() {
        use sinomo_dfs::DfsMetadataBackend as _;

        let root = sinomo_testdir::TestDir::new("sinomo-daemon-sharded-dfs-metadata");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data dir");
        let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            sinomo_core::node::ClusterId::from_legacy_named("cluster_daemon_sharded_dfs")
                .expect("cluster");
        let local = NodeId::from_legacy_named("node_daemon_sharded_dfs").expect("node");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id,
            "daemon sharded dfs",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("write state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
            .expect("sync raft");
        let file_system = sinomo_dfs::distributed_sifs(local.clone());
        let root_shard = sinomo_dfs::DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: local.clone(),
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: sinomo_dfs::DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
        };
        let services_shard = sinomo_dfs::DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "services".to_string(),
            primary_node_id: local.clone(),
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: sinomo_dfs::DfsMetadataShardAssignment::PathPrefix {
                prefix: "/services".to_string(),
            },
            namespace_prepare_codec: Default::default(),
            default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
        };
        append_cluster_raft_command(
            &cluster_paths,
            &local,
            sinomo_core::kv::KvCommand::Txn(sinomo_core::kv::KvTxn {
                compares: Vec::new(),
                success: vec![
                    sinomo_core::kv::KvOp::put_serde(
                        sinomo_dfs::metadata_shard_key(&file_system.file_system_id, "root"),
                        &root_shard,
                        None,
                    )
                    .expect("root shard op"),
                    sinomo_core::kv::KvOp::put_serde(
                        sinomo_dfs::metadata_shard_key(&file_system.file_system_id, "services"),
                        &services_shard,
                        None,
                    )
                    .expect("services shard op"),
                ],
                failure: Vec::new(),
            }),
        )
        .expect("write shard configs");

        let mut backend =
            sinomo_dfs::open_cluster_sharded_dfs_metadata_backend(&data_dir, &cluster_paths)
                .expect("open backend")
                .expect("configured sharded backend");
        backend.put_file_system(&file_system).expect("filesystem");
        let mut object = sinomo_dfs::distributed_sifs_bootstrap_objects(local)
            .into_iter()
            .next()
            .expect("object");
        object.path = "/services/daemon-sharded.txt".to_string();
        backend.put_object(&object).expect("object");

        let services_batch = backend
            .journal_batch_for_shard_since_limited(
                &sinomo_dfs::DfsMetadataShardId::new("services"),
                0,
                sinomo_dfs::DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
                sinomo_dfs::DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
            )
            .expect("services journal")
            .expect("services entries");
        assert!(services_batch.entries.iter().any(|entry| matches!(
            entry.mutation,
            sinomo_dfs::DfsMetadataMutation::PutObject { .. }
        )));
    }

    #[test]
    fn settled_solo_daemon_publishes_dfs_topology_before_serving() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-control-solo-dfs-ready");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data dir");
        let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            sinomo_core::node::ClusterId::from_legacy_named("cluster_daemon_solo_dfs_ready")
                .expect("cluster");
        let local = NodeId::from_legacy_named("node_daemon_solo_dfs_ready").expect("local node");
        let identity = sinomo_core::node::load_or_create_main_node_identity(&data_dir.root())
            .expect("local daemon identity");
        let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id,
            "daemon solo DFS ready",
            local,
            "local",
            identity.public_key,
            now,
        );
        state.members[0].capabilities =
            sinomo_core::node::main_daemon_capabilities(&sinomo_core::ClusterMemberRole::Voter);
        state.members[0].allowed_capabilities = state.members[0].capabilities.clone();
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
            .expect("cluster KV");

        let port_listener = TcpListener::bind("127.0.0.1:0").expect("reserve port");
        let port = port_listener.local_addr().expect("local addr").port();
        drop(port_listener);
        let address = format!("127.0.0.1:{port}");
        let paths = DaemonPaths::new(&data_dir);
        let options = DaemonRunOptions {
            host: "127.0.0.1".to_string(),
            port,
        };
        let cluster = crate::daemon_cluster_status(&state, crate::DaemonClusterProcess::Main)
            .expect("main cluster status");
        let daemon = thread::spawn({
            let paths = paths.clone();
            move || run_daemon_control_loop(options, paths, "daemon", Some(cluster))
        });

        let start = Instant::now();
        loop {
            match send_test_status_request(&data_dir, &address, Duration::from_millis(200)) {
                Ok(_) => break,
                Err(error) => {
                    if daemon.is_finished() {
                        let outcome = daemon.join().expect("daemon thread");
                        panic!("daemon exited before becoming ready: {outcome:?}");
                    }
                    assert!(
                        start.elapsed() < Duration::from_secs(5),
                        "daemon did not become ready: {error:?}"
                    );
                    thread::sleep(Duration::from_millis(20));
                }
            }
        }

        send_test_shutdown_request(&data_dir, &address, Duration::from_secs(3))
            .expect("shutdown request");
        daemon
            .join()
            .expect("daemon thread")
            .expect("daemon exits cleanly");

        // Read durable startup output only after the daemon has released its
        // Raft and DFS ownership. The successful status response above is the
        // readiness boundary; racing the live owner here tests lock timing,
        // not publication-before-serving.
        let file_system = sinomo_dfs::default_cluster_sifs(&state);
        let shard_entries = sinomo_core::cluster::read_cluster_kv_current_snapshot(&cluster_paths)
            .expect("current canonical shard authority")
            .range_prefix_limited(
                &format!(
                    "{}/",
                    sinomo_dfs::metadata_shards_prefix(&file_system.file_system_id)
                ),
                sinomo_core::kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
            )
            .expect("metadata shard topology");
        let backend =
            sinomo_dfs::open_cluster_sharded_dfs_metadata_backend(&data_dir, &cluster_paths)
                .expect("open sharded backend")
                .expect("configured sharded backend");
        let services_anchor = backend
            .get_object_by_path(&file_system.file_system_id, sinomo_dfs::SIFS_SERVICES_PATH)
            .expect("services anchor lookup");

        assert_eq!(
            shard_entries.len(),
            3,
            "a ready solo daemon must expose root, users, and services shard placement"
        );
        assert!(
            services_anchor.is_some(),
            "a ready solo daemon must materialize the /services namespace anchor"
        );
    }

    #[test]
    fn daemon_control_loop_answers_status_while_idle_probe_connection_is_open() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-control-concurrent");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data dir");
        let port_listener = TcpListener::bind("127.0.0.1:0").expect("reserve port");
        let port = port_listener.local_addr().expect("local addr").port();
        drop(port_listener);
        let address = format!("127.0.0.1:{port}");
        let paths = DaemonPaths::new(&data_dir);
        let options = DaemonRunOptions {
            host: "127.0.0.1".to_string(),
            port,
        };
        let daemon = thread::spawn({
            let paths = paths.clone();
            move || run_daemon_control_loop(options, paths, "daemon-test", None)
        });
        let start = Instant::now();
        loop {
            match send_test_status_request(&data_dir, &address, Duration::from_millis(200)) {
                Ok(_) => break,
                Err(error) => {
                    assert!(
                        start.elapsed() < Duration::from_secs(3),
                        "daemon did not become ready: {error:?}"
                    );
                    thread::sleep(Duration::from_millis(20));
                }
            }
        }

        let idle_probe = TcpStream::connect(&address).expect("connect idle probe");
        thread::sleep(Duration::from_millis(100));
        let status_result =
            send_test_status_request(&data_dir, &address, Duration::from_millis(500));
        drop(idle_probe);
        send_test_shutdown_request(&data_dir, &address, Duration::from_secs(3))
            .expect("shutdown request");
        daemon
            .join()
            .expect("daemon thread")
            .expect("daemon exits cleanly");

        assert!(
            status_result.is_ok(),
            "canonical loopback status should not be blocked by an idle TCP probe: {status_result:?}"
        );
    }

    #[test]
    fn accepted_rollout_stops_main_but_not_shadow() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-control-rollout-main-stop");
        std::fs::create_dir_all(root.join("daemon")).expect("main root");
        std::fs::create_dir_all(root.join("shadow-daemon")).expect("shadow root");
        let main_paths = DaemonPaths::with_root(root.join("daemon"));
        let shadow_paths = DaemonPaths::with_root(root.join("shadow-daemon"));
        let accepted = sinomo_rollout::ClusterRolloutBinaryResponse {
            status: "accepted".to_string(),
            detail: "staged".to_string(),
            main_binary: root.join("bin/sinomo"),
            shadow_binary: root.join("bin/sinomo-shadow"),
            source_head: None,
        };

        assert!(accepted_rollout_should_stop_current_daemon(
            &main_paths,
            &accepted
        ));
        assert!(!accepted_rollout_should_stop_current_daemon(
            &shadow_paths,
            &accepted
        ));
    }

    #[test]
    fn daemon_dfs_idle_work_parks_without_an_inventory_probe() {
        let observed_at = Instant::now();
        let decision = daemon_dfs_maintenance_probe_decision(
            observed_at,
            1_000_000_000,
            Some(DaemonDfsPendingMaintenanceWork::default()),
        );

        assert!(!decision.trigger_maintenance);
        assert!(!decision.immediate_continuation);
        assert_eq!(decision.next_probe_at, None);
        assert!(
            !daemon_dfs_maintenance_should_launch(true, false),
            "an idle projection must never acquire the DFS lane or distributed lease"
        );
    }

    #[test]
    fn daemon_dfs_deferred_work_keeps_its_bounded_retry() {
        let observed_at = Instant::now();
        let decision = daemon_dfs_maintenance_probe_decision(
            observed_at,
            1_000_000_000,
            Some(DaemonDfsPendingMaintenanceWork {
                pending_repair_count: 3,
                repair_attempt_count: 3,
                deferred_work_count: 3,
                ..DaemonDfsPendingMaintenanceWork::default()
            }),
        );

        assert!(!decision.trigger_maintenance);
        assert_eq!(
            decision
                .next_probe_at
                .expect("deferred work deadline")
                .duration_since(observed_at),
            DAEMON_DFS_DEFERRED_WORK_REPROBE_INTERVAL
        );
    }

    #[test]
    fn restricted_or_metered_resources_park_remote_only_dfs_work_but_keep_local_recovery() {
        let observed_at = Instant::now();
        let remote_only = DaemonDfsPendingMaintenanceWork {
            pending_repair_count: 2,
            due_repair_attempt_count: 1,
            unattempted_repair_count: 1,
            actionable_placement_reconciliation_count: 3,
            metadata_replica_lag_count: 1,
            ..DaemonDfsPendingMaintenanceWork::default()
        };
        let decision = daemon_dfs_maintenance_probe_decision(
            observed_at,
            1_000_000_000,
            Some(remote_only.clone()),
        );
        let restricted = daemon_dfs_resource_adjusted_probe_decision(
            sinomo_device::NodeResourceSnapshot::observed(
                sinomo_device::NodeResourceMode::Restricted,
                sinomo_device::NodeNetworkMetering::Unknown,
            ),
            Some(&remote_only),
            decision,
        );
        assert!(!restricted.trigger_maintenance);
        assert!(!restricted.immediate_continuation);
        assert_eq!(restricted.next_probe_at, None);

        let local_and_remote = DaemonDfsPendingMaintenanceWork {
            pending_write_count: 1,
            ..remote_only.clone()
        };
        let decision = daemon_dfs_maintenance_probe_decision(
            observed_at,
            1_000_000_000,
            Some(local_and_remote.clone()),
        );
        let restricted = daemon_dfs_resource_adjusted_probe_decision(
            sinomo_device::NodeResourceSnapshot::observed(
                sinomo_device::NodeResourceMode::Restricted,
                sinomo_device::NodeNetworkMetering::Unknown,
            ),
            Some(&local_and_remote),
            decision,
        );
        assert!(restricted.trigger_maintenance);
        assert!(!restricted.immediate_continuation);

        let decision = daemon_dfs_maintenance_probe_decision(
            observed_at,
            1_000_000_000,
            Some(remote_only.clone()),
        );
        let metered = daemon_dfs_resource_adjusted_probe_decision(
            sinomo_device::NodeResourceSnapshot::observed(
                sinomo_device::NodeResourceMode::Constrained,
                sinomo_device::NodeNetworkMetering::Metered,
            ),
            Some(&remote_only),
            decision,
        );
        assert!(!metered.trigger_maintenance);
        assert!(!metered.immediate_continuation);
        assert_eq!(metered.next_probe_at, None);
    }

    #[test]
    fn storage_dirty_edge_is_generation_counted_and_coalescible() {
        let root = sinomo_testdir::TestDir::new("storage-dirty-generation");
        let data_dir = SinomoDataDir::new(&root);
        let before = daemon_dfs_storage_environment_dirty_generation(&data_dir);
        crate::dfs::notify_daemon_dfs_storage_environment_dirty(&data_dir);
        assert!(daemon_dfs_storage_environment_dirty_generation(&data_dir) > before);
    }

    #[test]
    fn daemon_dfs_probe_uses_exact_future_repair_retry_deadline() {
        let observed_at = Instant::now();
        let observed_at_unix_nanos = 1_000_000_000;
        let retry_delay = Duration::from_secs(17);
        let decision = daemon_dfs_maintenance_probe_decision(
            observed_at,
            observed_at_unix_nanos,
            Some(DaemonDfsPendingMaintenanceWork {
                pending_repair_count: 1,
                repair_attempt_count: 1,
                next_repair_retry_unix_nanos: Some(
                    observed_at_unix_nanos
                        .saturating_add(retry_delay.as_nanos().try_into().expect("retry nanos")),
                ),
                ..DaemonDfsPendingMaintenanceWork::default()
            }),
        );

        assert!(!decision.trigger_maintenance);
        assert_eq!(
            decision
                .next_probe_at
                .expect("repair deadline")
                .duration_since(observed_at),
            retry_delay
        );
    }

    #[test]
    fn daemon_dfs_probe_preserves_far_durable_work_deadline() {
        let observed_at = Instant::now();
        let observed_at_unix_nanos = 1_000_000_000;
        let retry_delay = Duration::from_secs(900);
        let decision = daemon_dfs_maintenance_probe_decision(
            observed_at,
            observed_at_unix_nanos,
            Some(DaemonDfsPendingMaintenanceWork {
                pending_repair_count: 1,
                repair_attempt_count: 1,
                next_repair_retry_unix_nanos: Some(
                    observed_at_unix_nanos
                        .saturating_add(retry_delay.as_nanos().try_into().expect("retry nanos")),
                ),
                ..DaemonDfsPendingMaintenanceWork::default()
            }),
        );

        assert!(!decision.trigger_maintenance);
        assert_eq!(
            decision
                .next_probe_at
                .expect("far repair deadline")
                .duration_since(observed_at),
            retry_delay
        );
    }

    #[test]
    fn daemon_dfs_probe_uses_earliest_repair_or_lifecycle_deadline() {
        let observed_at = Instant::now();
        let observed_at_unix_nanos = 1_000_000_000;
        let decision = daemon_dfs_maintenance_probe_decision(
            observed_at,
            observed_at_unix_nanos,
            Some(DaemonDfsPendingMaintenanceWork {
                next_repair_retry_unix_nanos: Some(observed_at_unix_nanos + 90_000_000_000),
                next_time_based_maintenance_unix_nanos: Some(
                    observed_at_unix_nanos + 17_000_000_000,
                ),
                ..DaemonDfsPendingMaintenanceWork::default()
            }),
        );

        assert_eq!(
            decision
                .next_probe_at
                .expect("lifecycle deadline")
                .duration_since(observed_at),
            Duration::from_secs(17)
        );
    }

    #[test]
    fn daemon_dfs_probe_error_uses_short_safety_retry() {
        let observed_at = Instant::now();
        let decision = daemon_dfs_maintenance_probe_decision(observed_at, 1_000_000_000, None);

        assert!(!decision.trigger_maintenance);
        assert!(!decision.immediate_continuation);
        assert_eq!(
            decision
                .next_probe_at
                .expect("probe error retry")
                .duration_since(observed_at),
            DAEMON_DFS_MAINTENANCE_PROBE_ERROR_RETRY_INTERVAL
        );
    }

    #[test]
    fn failed_dfs_maintenance_preserves_latched_request_and_short_probe() {
        let observed_at = Instant::now();
        let future_probe = observed_at
            .checked_add(DAEMON_DFS_DEFERRED_WORK_REPROBE_INTERVAL)
            .expect("future probe");
        let decision = daemon_dfs_failed_maintenance_decision(observed_at, Some(future_probe));

        assert!(
            decision.trigger_maintenance,
            "a request latched while the failed task was running must survive completion handling"
        );
        assert!(
            !decision.immediate_continuation,
            "task failure must clear ownership of an immediate continuation"
        );
        assert_eq!(
            decision
                .next_probe_at
                .expect("failure retry")
                .duration_since(observed_at),
            DAEMON_DFS_MAINTENANCE_PROBE_ERROR_RETRY_INTERVAL,
            "failed work must retain the bounded recovery probe"
        );
    }

    #[test]
    fn dfs_maintenance_notification_launches_reactive_work_immediately() {
        let observed_at = Instant::now();
        let mut next_probe_at = None;
        let triggered_maintenance_requested = false;

        let notification = daemon_dfs_maintenance_notification_decision(
            observed_at,
            triggered_maintenance_requested,
            false,
            next_probe_at,
        );
        next_probe_at = notification.next_probe_at;

        assert!(
            notification.trigger_maintenance,
            "a committed queue edge must launch reactive maintenance without a metadata inventory"
        );
        assert!(
            notification.immediate_continuation,
            "a committed edge transfers ready work directly to the post-response executor"
        );
        assert_eq!(next_probe_at, None);
        assert_eq!(
            daemon_dfs_maintenance_due_in(
                observed_at,
                observed_at + Duration::from_secs(60),
                observed_at,
                notification.trigger_maintenance,
                notification.immediate_continuation,
            ),
            Some(Duration::ZERO),
            "reactive work must not wait for startup phasing or the generic maintenance interval"
        );

        let decision = daemon_dfs_failed_maintenance_decision(observed_at, next_probe_at);
        assert_eq!(
            decision.next_probe_at,
            observed_at.checked_add(DAEMON_DFS_MAINTENANCE_PROBE_ERROR_RETRY_INTERVAL),
            "task failure must retain a bounded retry without running an inventory"
        );
    }

    #[test]
    fn completed_dfs_followup_probe_parks_when_state_is_idle() {
        let observed_at = Instant::now();
        let decision = daemon_dfs_maintenance_completion_decision(
            DaemonDfsMaintenanceTickOutcome::Completed {
                reached_limit: false,
            },
            observed_at,
            1_000_000_000,
            Some(DaemonDfsPendingMaintenanceWork::default()),
            false,
        );

        assert!(!decision.trigger_maintenance);
        assert!(!decision.immediate_continuation);
        assert_eq!(decision.next_probe_at, None);
    }

    #[test]
    fn completed_dfs_idle_followup_preserves_committed_edge_latched_during_active_pass() {
        let observed_at = Instant::now();
        let startup_not_before = observed_at
            .checked_add(Duration::from_secs(60))
            .expect("startup deadline");
        let decision = daemon_dfs_maintenance_completion_decision(
            DaemonDfsMaintenanceTickOutcome::Completed {
                reached_limit: false,
            },
            observed_at,
            1_000_000_000,
            Some(DaemonDfsPendingMaintenanceWork::default()),
            true,
        );

        assert!(
            decision.trigger_maintenance,
            "an idle bounded classifier must not consume a committed edge received during the active pass"
        );
        assert!(
            decision.immediate_continuation,
            "the newer committed edge owns an immediate follow-up pass"
        );
        assert_eq!(decision.next_probe_at, None);
        assert_eq!(
            daemon_dfs_maintenance_due_in(
                observed_at,
                startup_not_before,
                observed_at,
                decision.trigger_maintenance,
                decision.immediate_continuation,
            ),
            Some(Duration::ZERO),
            "the latched edge must bypass startup phasing and the ordinary maintenance interval"
        );
    }

    #[test]
    fn deferred_dfs_followup_probe_does_not_latch_quiet_work() {
        let observed_at = Instant::now();
        let decision = daemon_dfs_maintenance_completion_decision(
            DaemonDfsMaintenanceTickOutcome::Deferred,
            observed_at,
            1_000_000_000,
            Some(DaemonDfsPendingMaintenanceWork::default()),
            false,
        );

        assert!(
            !decision.trigger_maintenance,
            "foreground preemption alone must not create triggered DFS work"
        );
        assert!(!decision.immediate_continuation);
        assert_eq!(decision.next_probe_at, None);
    }

    #[test]
    fn deferred_dfs_followup_probe_relatches_actionable_work() {
        let observed_at = Instant::now();
        let decision = daemon_dfs_maintenance_completion_decision(
            DaemonDfsMaintenanceTickOutcome::Deferred,
            observed_at,
            1_000_000_000,
            Some(DaemonDfsPendingMaintenanceWork {
                pending_write_count: 1,
                ..DaemonDfsPendingMaintenanceWork::default()
            }),
            false,
        );

        assert!(decision.trigger_maintenance);
        assert!(
            !decision.immediate_continuation,
            "a deferred pass must retry lease/admission instead of claiming a successful continuation"
        );
        assert_eq!(
            decision
                .next_probe_at
                .expect("actionable safety retry")
                .duration_since(observed_at),
            DAEMON_DFS_MAINTENANCE_PROBE_ERROR_RETRY_INTERVAL,
            "deferred actionable work retains the bounded safety probe"
        );
    }

    #[test]
    fn completed_dfs_reached_limit_actionable_followup_is_an_immediate_continuation() {
        let observed_at = Instant::now();
        let decision = daemon_dfs_maintenance_completion_decision(
            DaemonDfsMaintenanceTickOutcome::Completed {
                reached_limit: true,
            },
            observed_at,
            1_000_000_000,
            Some(DaemonDfsPendingMaintenanceWork {
                pending_namespace_job_count: 1,
                ..DaemonDfsPendingMaintenanceWork::default()
            }),
            false,
        );

        assert!(decision.trigger_maintenance);
        assert!(decision.immediate_continuation);
        assert_eq!(
            daemon_dfs_maintenance_due_in(
                observed_at,
                observed_at,
                observed_at,
                decision.trigger_maintenance,
                decision.immediate_continuation,
            ),
            Some(Duration::ZERO),
            "bounded continuation must not pay the ordinary 30-second interval"
        );
    }

    #[test]
    fn completed_dfs_fresh_work_race_retains_the_ordinary_gate() {
        let observed_at = Instant::now();
        let decision = daemon_dfs_maintenance_completion_decision(
            DaemonDfsMaintenanceTickOutcome::Completed {
                reached_limit: false,
            },
            observed_at,
            1_000_000_000,
            Some(DaemonDfsPendingMaintenanceWork {
                pending_write_count: 1,
                ..DaemonDfsPendingMaintenanceWork::default()
            }),
            false,
        );

        assert!(decision.trigger_maintenance);
        assert!(
            !decision.immediate_continuation,
            "work first observed after an unbounded completion is a new request, not pass-owned"
        );
        assert_eq!(
            daemon_dfs_maintenance_due_in(
                observed_at,
                observed_at,
                observed_at,
                decision.trigger_maintenance,
                decision.immediate_continuation,
            ),
            Some(DAEMON_DFS_TRIGGERED_MAINTENANCE_MIN_INTERVAL),
        );
    }

    #[test]
    fn completed_dfs_no_progress_work_retains_the_ordinary_gate() {
        let observed_at = Instant::now();
        let decision = daemon_dfs_maintenance_completion_decision(
            DaemonDfsMaintenanceTickOutcome::Completed {
                reached_limit: false,
            },
            observed_at,
            1_000_000_000,
            Some(DaemonDfsPendingMaintenanceWork {
                pending_namespace_job_count: 1,
                ..DaemonDfsPendingMaintenanceWork::default()
            }),
            false,
        );

        assert!(decision.trigger_maintenance);
        assert!(!decision.immediate_continuation);
        assert_eq!(
            daemon_dfs_maintenance_due_in(
                observed_at,
                observed_at,
                observed_at,
                decision.trigger_maintenance,
                decision.immediate_continuation,
            ),
            Some(DAEMON_DFS_TRIGGERED_MAINTENANCE_MIN_INTERVAL),
            "a successful pass without explicit bounded progress must not hot-loop unchanged work"
        );
    }

    #[test]
    fn newly_probed_dfs_work_remains_interval_and_startup_gated() {
        let observed_at = Instant::now();
        let decision = daemon_dfs_maintenance_probe_decision(
            observed_at,
            1_000_000_000,
            Some(DaemonDfsPendingMaintenanceWork {
                pending_write_count: 1,
                ..DaemonDfsPendingMaintenanceWork::default()
            }),
        );

        assert!(decision.trigger_maintenance);
        assert!(!decision.immediate_continuation);
        assert_eq!(
            daemon_dfs_maintenance_due_in(
                observed_at,
                observed_at,
                observed_at,
                decision.trigger_maintenance,
                decision.immediate_continuation,
            ),
            Some(DAEMON_DFS_TRIGGERED_MAINTENANCE_MIN_INTERVAL),
        );

        let startup_delay = Duration::from_secs(7);
        let startup_not_before = observed_at
            .checked_add(startup_delay)
            .expect("startup deadline");
        assert_eq!(
            daemon_dfs_maintenance_due_in(observed_at, startup_not_before, observed_at, true, true),
            Some(Duration::ZERO),
            "an owned reactive continuation bypasses startup phasing"
        );
    }

    #[test]
    fn deferred_dfs_actionable_work_retains_lease_retry_instead_of_immediate_continuation() {
        let observed_at = Instant::now();
        let decision = daemon_dfs_maintenance_completion_decision(
            DaemonDfsMaintenanceTickOutcome::Deferred,
            observed_at,
            1_000_000_000,
            Some(DaemonDfsPendingMaintenanceWork {
                pending_write_count: 1,
                ..DaemonDfsPendingMaintenanceWork::default()
            }),
            false,
        );
        let deferred_last = observed_at
            .checked_sub(
                DAEMON_DFS_TRIGGERED_MAINTENANCE_MIN_INTERVAL
                    .saturating_sub(DAEMON_DFS_MAINTENANCE_LEASE_RETRY_DELAY),
            )
            .expect("deferred last-maintenance projection");

        assert!(decision.trigger_maintenance);
        assert!(!decision.immediate_continuation);
        assert_eq!(
            daemon_dfs_maintenance_due_in(
                observed_at,
                observed_at,
                deferred_last,
                decision.trigger_maintenance,
                decision.immediate_continuation,
            ),
            Some(DAEMON_DFS_MAINTENANCE_LEASE_RETRY_DELAY),
        );
    }

    #[test]
    fn dfs_prelaunch_revalidation_preserves_only_actionable_immediate_continuation() {
        let observed_at = Instant::now();
        let actionable = Some(DaemonDfsPendingMaintenanceWork {
            pending_write_count: 1,
            ..DaemonDfsPendingMaintenanceWork::default()
        });
        let preserved = daemon_dfs_maintenance_revalidation_decision(
            observed_at,
            1_000_000_000,
            actionable.clone(),
            true,
        );
        assert!(preserved.trigger_maintenance);
        assert!(preserved.immediate_continuation);

        let not_promoted = daemon_dfs_maintenance_revalidation_decision(
            observed_at,
            1_000_000_000,
            actionable,
            false,
        );
        assert!(not_promoted.trigger_maintenance);
        assert!(!not_promoted.immediate_continuation);

        let cleared = daemon_dfs_maintenance_revalidation_decision(
            observed_at,
            1_000_000_000,
            Some(DaemonDfsPendingMaintenanceWork::default()),
            true,
        );
        assert!(!cleared.trigger_maintenance);
        assert!(!cleared.immediate_continuation);
        assert_eq!(cleared.next_probe_at, None);
    }

    #[test]
    fn triggered_dfs_maintenance_revalidates_work_at_launch() {
        assert!(
            !daemon_dfs_maintenance_should_launch(true, false),
            "a stale triggered latch must not launch a broad maintenance pass"
        );
        assert!(
            daemon_dfs_maintenance_should_launch(true, true),
            "revalidated actionable work must still launch promptly"
        );
        assert!(
            !daemon_dfs_maintenance_should_launch(false, true),
            "work cannot launch before its admission phase/minimum interval"
        );
    }

    #[test]
    fn daemon_maintenance_sleep_uses_next_due_work_instead_of_fixed_tick() {
        let now = Instant::now();

        assert_eq!(
            daemon_maintenance_sleep_duration(
                now,
                Duration::from_secs(30),
                None,
                None,
                None,
                Some(DAEMON_SHADOW_ROLLOUT_IDLE_FINALIZATION_INTERVAL),
                None,
            ),
            Duration::from_secs(30),
            "quiet main daemons may still wake for earlier cluster maintenance"
        );
        assert_eq!(
            daemon_maintenance_sleep_duration(
                now,
                Duration::from_secs(30),
                None,
                None,
                None,
                Some(Duration::from_secs(5)),
                None,
            ),
            Duration::from_secs(5),
            "active rollout fallback polling can still wake sooner than cluster maintenance"
        );
        assert_eq!(
            daemon_maintenance_sleep_duration(
                now,
                Duration::from_secs(600),
                Some(Duration::from_secs(3)),
                None,
                None,
                None,
                None,
            ),
            Duration::from_secs(3),
            "triggered DFS work should wake at its explicit admission deadline"
        );
        assert_eq!(
            daemon_maintenance_sleep_duration(
                now,
                Duration::from_secs(600),
                None,
                None,
                None,
                None,
                None,
            ),
            Duration::from_secs(600),
            "an idle DFS projection must not add a generic maintenance wakeup"
        );
        assert_eq!(
            daemon_maintenance_sleep_duration(now, Duration::ZERO, None, None, None, None, None,),
            Duration::from_millis(1),
            "overdue work should not create a zero-duration spin"
        );
        assert_eq!(
            daemon_maintenance_sleep_duration(
                now,
                Duration::from_secs(600),
                None,
                None,
                None,
                None,
                Some(Duration::from_secs(2)),
            ),
            Duration::from_secs(2),
            "pending sandbox lifecycle recovery must wake on its own cadence"
        );
    }

    #[test]
    fn rollout_telemetry_reconciliation_does_not_own_desired_rollout_state() {
        let source = include_str!("control.rs");
        let telemetry_reconciliation = source
            .split_once("pub(crate) fn run_daemon_rollout_telemetry_reconciliation_tick")
            .expect("telemetry reconciliation function")
            .1
            .split_once("fn publish_daemon_module_rollout_status_from_data_dir")
            .expect("rollout status publisher")
            .0;
        assert!(
            !telemetry_reconciliation.contains("module_rollout_desired"),
            "telemetry source reconciliation must not poll or own desired rollout state"
        );
    }

    #[test]
    fn clustered_startup_network_pass_precedes_dfs_maintenance() {
        assert!(
            !daemon_dfs_maintenance_allowed(true, false),
            "a clustered replacement must publish its first network-liveness pass before DFS work"
        );
        assert!(
            daemon_dfs_maintenance_allowed(true, true),
            "DFS maintenance resumes after the bounded startup network pass"
        );
        assert!(!daemon_dfs_maintenance_allowed(false, true));
        assert_eq!(
            daemon_dfs_initial_maintenance_delay(Duration::ZERO, true),
            DAEMON_DFS_MAINTENANCE_NETWORK_STARTUP_GRACE,
            "rank-zero DFS work must leave the startup liveness window idle"
        );
        assert_eq!(
            daemon_dfs_initial_maintenance_delay(Duration::from_secs(20), true),
            DAEMON_DFS_MAINTENANCE_NETWORK_STARTUP_GRACE + Duration::from_secs(20),
            "membership staggering starts after the common liveness window"
        );
        assert_eq!(
            daemon_dfs_initial_maintenance_delay(Duration::from_secs(20), false),
            Duration::from_secs(20),
            "unclustered processes do not reserve a cluster-liveness window"
        );
    }

    #[cfg(feature = "sandbox")]
    #[test]
    fn clustered_startup_maintenance_precedes_sandbox_lifecycle_recovery() {
        assert!(
            !daemon_sandbox_lifecycle_maintenance_allowed("daemon", false),
            "a fresh clustered replacement must get its first node-status pass before sandbox recovery"
        );
        assert!(
            daemon_sandbox_lifecycle_maintenance_allowed("daemon", true),
            "sandbox recovery resumes immediately after the bounded cluster startup pass"
        );
        assert!(
            !daemon_sandbox_lifecycle_maintenance_allowed("shadow-daemon", true),
            "the shadow supervisor never owns sandbox lifecycle recovery"
        );
    }

    #[cfg(feature = "sandbox")]
    #[test]
    fn sandbox_lifecycle_quiet_ticks_skip_dfs_scan_after_startup_sweep() {
        let root = sinomo_testdir::TestDir::new("sandbox-lifecycle-quiet-fast-path");
        let paths = DaemonPaths::with_root(root.join("daemon"));
        fs::create_dir_all(&paths.root).expect("daemon root");
        let started_at = 1_000_000_000_000_u64;
        let tick_nanos = u64::try_from(DAEMON_SANDBOX_LIFECYCLE_MAINTENANCE_INTERVAL.as_nanos())
            .expect("tick nanos");
        let mut scan_count = 0_usize;

        run_daemon_sandbox_lifecycle_maintenance_tick_with_scan(&paths, started_at, |marked| {
            scan_count += 1;
            assert!(marked.is_empty());
            Ok(0)
        })
        .expect("startup sweep");
        for now in [started_at + tick_nanos, started_at + tick_nanos * 2] {
            run_daemon_sandbox_lifecycle_maintenance_tick_with_scan(&paths, now, |_| {
                scan_count += 1;
                Ok(0)
            })
            .expect("quiet tick");
        }
        assert_eq!(scan_count, 1, "two quiet ticks must not hydrate DFS");

        let safety_interval_nanos =
            u64::try_from(sinomo_sandbox::SANDBOX_LIFECYCLE_SAFETY_SWEEP_INTERVAL.as_nanos())
                .expect("safety interval nanos");
        run_daemon_sandbox_lifecycle_maintenance_tick_with_scan(
            &paths,
            started_at + safety_interval_nanos,
            |marked| {
                scan_count += 1;
                assert!(marked.is_empty());
                Ok(0)
            },
        )
        .expect("bounded safety sweep");
        assert_eq!(scan_count, 2, "quiet recovery must retain a safety sweep");
    }

    #[cfg(feature = "sandbox")]
    #[test]
    fn sandbox_lifecycle_marker_survives_failure_and_wakes_recovery() {
        let root = sinomo_testdir::TestDir::new("sandbox-lifecycle-pending-marker");
        let paths = DaemonPaths::with_root(root.join("daemon"));
        fs::create_dir_all(&paths.root).expect("daemon root");
        let now = 2_000_000_000_000_u64;
        daemon_sandbox_lifecycle_recovery_state(&paths)
            .record_safety_sweep(now)
            .expect("sweep stamp");
        let sandbox_id =
            sinomo_sandbox::SandboxId::from_legacy_named("sandbox_pending_marker_recovery")
                .expect("sandbox id");
        daemon_sandbox_lifecycle_recovery_state(&paths)
            .mark_authoritative_recovery_pending(&sandbox_id)
            .expect("pending marker");

        run_daemon_sandbox_lifecycle_maintenance_tick_with_scan(&paths, now, |marked| {
            assert_eq!(marked, BTreeSet::from([sandbox_id.clone()]));
            Err(DaemonError::Parse {
                detail: "injected recovery failure".to_string(),
            })
        })
        .expect_err("failed recovery");
        assert_eq!(
            daemon_sandbox_lifecycle_recovery_state(&paths)
                .pending_sandbox_ids()
                .expect("pending ids"),
            BTreeSet::from([sandbox_id.clone()])
        );

        run_daemon_sandbox_lifecycle_maintenance_tick_with_scan(&paths, now + 1, |marked| {
            assert_eq!(marked, BTreeSet::from([sandbox_id.clone()]));
            daemon_sandbox_lifecycle_recovery_state(&paths)
                .clear_authoritative_recovery_pending(&sandbox_id)?;
            Ok(1)
        })
        .expect("successful recovery");
        assert!(
            daemon_sandbox_lifecycle_recovery_state(&paths)
                .pending_sandbox_ids()
                .expect("settled ids")
                .is_empty()
        );
    }

    #[test]
    fn initial_dfs_maintenance_is_staggered_by_stable_membership_rank() {
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            sinomo_core::node::ClusterId::from_legacy_named("cluster_daemon_dfs_maintenance_phase")
                .expect("cluster id");
        let mut node_ids = [
            "node_daemon_dfs_phase_a",
            "node_daemon_dfs_phase_b",
            "node_daemon_dfs_phase_c",
            "node_daemon_dfs_phase_d",
        ]
        .into_iter()
        .map(|name| NodeId::from_legacy_named(name).expect("node id"))
        .collect::<Vec<_>>();
        node_ids.sort();
        let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id,
            "DFS maintenance phase",
            node_ids[0].clone(),
            "node 0",
            "pk-node-0",
            now,
        );
        let member_template = state.members[0].clone();
        state.members = node_ids
            .iter()
            .enumerate()
            .map(|(index, node_id)| {
                let mut member = member_template.clone();
                member.node_id = node_id.clone();
                member.display_name = format!("node {index}");
                member.node_public_key = format!("pk-node-{index}");
                member
            })
            .collect();

        for (rank, node_id) in node_ids.iter().enumerate() {
            state.local_node_id = node_id.clone();
            assert_eq!(
                daemon_dfs_maintenance_member_phase(&state),
                DAEMON_DFS_MAINTENANCE_MEMBER_PHASE_SLOT
                    .saturating_mul(u32::try_from(rank).expect("small rank")),
                "each configured member gets a distinct deterministic startup slot"
            );
        }

        state.members.reverse();
        state.local_node_id = node_ids[2].clone();
        assert_eq!(
            daemon_dfs_maintenance_member_phase(&state),
            DAEMON_DFS_MAINTENANCE_MEMBER_PHASE_SLOT.saturating_mul(2),
            "serialized member ordering must not change the phase"
        );

        let mut large_node_ids = (0..20)
            .map(|index| {
                NodeId::from_legacy_named(format!("node_daemon_dfs_phase_large_{index:02}"))
                    .expect("large-cluster node id")
            })
            .collect::<Vec<_>>();
        large_node_ids.sort();
        state.members = large_node_ids
            .iter()
            .enumerate()
            .map(|(index, node_id)| {
                let mut member = member_template.clone();
                member.node_id = node_id.clone();
                member.display_name = format!("large node {index}");
                member.node_public_key = format!("pk-large-node-{index}");
                member
            })
            .collect();
        let phases = large_node_ids
            .iter()
            .map(|node_id| {
                state.local_node_id = node_id.clone();
                daemon_dfs_maintenance_member_phase(&state)
            })
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            phases.len(),
            large_node_ids.len(),
            "large memberships must retain one distinct phase per member"
        );
        assert!(
            phases
                .iter()
                .all(|phase| *phase < DAEMON_DFS_MAINTENANCE_MEMBER_PHASE_WINDOW),
            "every phase must stay inside the admission phase window"
        );
    }

    #[test]
    fn dfs_maintenance_task_join_does_not_detach_inflight_work() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
            let task = tokio::spawn(async move {
                release_rx.await.expect("release maintenance task");
                Ok(DaemonDfsMaintenanceTickOutcome::Completed {
                    reached_limit: false,
                })
            });
            let joined = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let joined_after_wait = Arc::clone(&joined);
            let waiter = tokio::spawn(async move {
                assert_eq!(
                    join_daemon_dfs_maintenance_task(task, "daemon-test").await,
                    Some(DaemonDfsMaintenanceTickOutcome::Completed {
                        reached_limit: false,
                    })
                );
                joined_after_wait.store(true, std::sync::atomic::Ordering::Release);
            });

            tokio::task::yield_now().await;
            assert!(
                !joined.load(std::sync::atomic::Ordering::Acquire),
                "joining must wait for the owned DFS generation to finish"
            );
            release_tx.send(()).expect("release maintenance");
            waiter.await.expect("join waiter");
            assert!(joined.load(std::sync::atomic::Ordering::Acquire));
        });
    }

    #[test]
    fn in_flight_dfs_maintenance_polls_completion_without_spinning() {
        let now = Instant::now();
        let converged_cluster_due = Duration::from_secs(60);
        assert_eq!(
            daemon_maintenance_sleep_duration(
                now,
                converged_cluster_due,
                None,
                None,
                Some(DAEMON_DFS_MAINTENANCE_TASK_COMPLETION_POLL_INTERVAL),
                None,
                None,
            ),
            DAEMON_DFS_MAINTENANCE_TASK_COMPLETION_POLL_INTERVAL,
            "a completed or deferred DFS task must be observed before the converged cluster cadence"
        );
    }

    #[test]
    fn default_dfs_role_has_no_environment_disable_path() {
        for ignored_legacy_value in [None, Some("0"), Some("false"), Some("off")] {
            assert!(
                daemon_role_owns_default_dfs("daemon"),
                "legacy value {ignored_legacy_value:?} must not disable the main daemon's DFS lifecycle"
            );
        }
        assert!(!daemon_role_owns_default_dfs("shadow-daemon"));

        let root = sinomo_testdir::TestDir::new("sinomo-daemon-default-dfs-module-toggle");
        let data_dir = sinomo_core::local::SinomoDataDir::new(&root);
        let paths = DaemonPaths::new(&data_dir);
        assert!(daemon_owns_enabled_default_dfs(&paths, "daemon"));
        sinomo_dfs::set_dfs_module_enabled(&root, false, "test", "disable DFS")
            .expect("disable DFS");
        assert!(!daemon_owns_enabled_default_dfs(&paths, "daemon"));

        let source = include_str!("control.rs");
        assert!(!source.contains(concat!("SINOMO_ENABLE_BACKGROUND_DFS_", "MAINTENANCE")));
        let dfs_source = include_str!("dfs.rs");
        assert!(!dfs_source.contains(concat!("SINOMO_ENABLE_DEFAULT_DISTRIBUTED_", "DFS")));
    }

    #[test]
    fn network_environment_watch_defaults_off_on_linux_with_override() {
        assert!(
            !daemon_network_environment_watch_enabled_for(None, true),
            "Linux defaults to scheduled maintenance because netwatcher can poll sysfs aggressively"
        );
        assert!(
            daemon_network_environment_watch_enabled_for(None, false),
            "non-Linux keeps the event watcher by default"
        );
        assert!(daemon_network_environment_watch_enabled_for(
            Some("1"),
            true
        ));
        assert!(daemon_network_environment_watch_enabled_for(
            Some(" true "),
            true
        ));
        assert!(!daemon_network_environment_watch_enabled_for(
            Some("off"),
            false
        ));
        assert!(
            !daemon_network_environment_watch_enabled_for(Some("unexpected"), true),
            "unknown override values preserve the Linux default"
        );
    }

    #[test]
    fn shadow_rollout_finalization_poll_backs_off_when_idle() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-shadow-finalization-poll");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("layout");
        let paths = DaemonPaths::new(&data_dir);
        let rollout_paths = SelfRolloutPaths::new(&root);

        assert_eq!(
            shadow_rollout_finalization_poll_due_in(&paths),
            DAEMON_SHADOW_ROLLOUT_IDLE_FINALIZATION_INTERVAL,
            "quiet daemons should not wake every tick just to check absent rollout files"
        );
        assert!(
            !daemon_rollout_handoff_pending(&paths),
            "quiet daemons have no rollout handoff marker"
        );

        std::fs::create_dir_all(&rollout_paths.root).expect("rollout dir");
        std::fs::write(rollout_shadow_finalize_path(&rollout_paths), b"pending")
            .expect("pending shadow marker");

        assert!(
            daemon_rollout_handoff_pending(&paths),
            "pending shadow finalization pauses heavy maintenance"
        );
        assert_eq!(
            shadow_rollout_finalization_poll_due_in(&paths),
            DAEMON_CLUSTER_TICK_INTERVAL,
            "active shadow finalization keeps the short fallback poll interval"
        );

        std::fs::remove_file(rollout_shadow_finalize_path(&rollout_paths))
            .expect("remove pending shadow marker");
        std::fs::write(rollout_main_activation_path(&rollout_paths), b"pending")
            .expect("pending main activation marker");

        assert!(
            daemon_rollout_handoff_pending(&paths),
            "pending main activation pauses heavy maintenance"
        );
        assert_eq!(
            shadow_rollout_finalization_poll_due_in(&paths),
            DAEMON_CLUSTER_TICK_INTERVAL,
            "active main activation keeps the short fallback poll interval"
        );
    }

    fn module_shadow_decision_event(
        node_id: &NodeId,
        module_id: &sinomo_core::Id,
        primary_artifact_id: &sinomo_core::Id,
        shadow_artifact_id: &sinomo_core::Id,
        matched: bool,
    ) -> sinomo_core::EventEnvelope {
        sinomo_core::EventEnvelope::new(
            sinomo_core::events::EventId::generate(),
            OffsetDateTime::now_utc(),
            node_id.clone(),
            sinomo_core::Source::new("module.live").expect("source"),
            sinomo_core::EventKind::new("module.shadow.decision").expect("kind"),
            sinomo_core::Salience::new(0.72).expect("salience"),
            sinomo_core::Confidence::new(0.95).expect("confidence"),
            sinomo_core::RoutingMetadata::new(
                sinomo_core::RoutingScope::TrustedNetwork,
                sinomo_core::LatencyClass::Background,
                sinomo_core::PrivacyClass::Derived,
            ),
        )
        .with_target_node(node_id.clone())
        .with_payload(serde_json::json!({
            "route_key": "live-session",
            "telemetry_source": "live",
            "module_id": module_id.as_str(),
            "primary_artifact_id": primary_artifact_id.as_str(),
            "primary_lane": "active",
            "shadow_artifact_id": shadow_artifact_id.as_str(),
            "shadow_matched": matched,
            "primary_output_bytes": 12,
            "shadow_output_bytes": 12,
            "primary_error": null,
            "shadow_error": null,
        }))
    }

    fn module_invocation_decision_event(
        node_id: &NodeId,
        module_id: &sinomo_core::Id,
        primary_artifact_id: &sinomo_core::Id,
        primary_lane: &str,
        primary_error: Option<&str>,
    ) -> sinomo_core::EventEnvelope {
        sinomo_core::EventEnvelope::new(
            sinomo_core::events::EventId::generate(),
            OffsetDateTime::now_utc(),
            node_id.clone(),
            sinomo_core::Source::new("module.live").expect("source"),
            sinomo_core::EventKind::new("module.invocation.decision").expect("kind"),
            sinomo_core::Salience::new(0.72).expect("salience"),
            sinomo_core::Confidence::new(0.95).expect("confidence"),
            sinomo_core::RoutingMetadata::new(
                sinomo_core::RoutingScope::TrustedNetwork,
                sinomo_core::LatencyClass::Background,
                sinomo_core::PrivacyClass::Derived,
            ),
        )
        .with_target_node(node_id.clone())
        .with_payload(serde_json::json!({
            "route_key": "live-session",
            "telemetry_source": "live",
            "module_id": module_id.as_str(),
            "primary_artifact_id": primary_artifact_id.as_str(),
            "primary_lane": primary_lane,
            "traffic_weight_bps": 500,
            "route_bucket_bps": 42,
            "hot_swap_state": "active-candidate",
            "primary_output_bytes": 12,
            "primary_error": primary_error,
        }))
    }

    fn send_test_status_request(
        data_dir: &SinomoDataDir,
        address: &str,
        timeout: Duration,
    ) -> Result<crate::DaemonStatus, DaemonError> {
        let request = sinomo_core::network::ObjectRpcRequest::encode_body(
            crate::DAEMON_STATUS_ROUTE,
            crate::DAEMON_RPC_CATEGORY,
            &(),
        )?;
        let response = send_test_rpc(data_dir, address, &request, timeout)?;
        response.decode_body().map_err(DaemonError::from)
    }

    fn send_test_shutdown_request(
        data_dir: &SinomoDataDir,
        address: &str,
        timeout: Duration,
    ) -> Result<String, DaemonError> {
        let request = sinomo_core::network::ObjectRpcRequest::encode_body(
            crate::DAEMON_SHUTDOWN_ROUTE,
            crate::DAEMON_RPC_CATEGORY,
            &(),
        )?;
        let response = send_test_rpc(data_dir, address, &request, timeout)?;
        response.decode_body().map_err(DaemonError::from)
    }

    fn send_test_rpc(
        data_dir: &SinomoDataDir,
        address: &str,
        request: &sinomo_core::network::ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<sinomo_core::network::ObjectRpcResponse, DaemonError> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|source| DaemonError::Network {
                address: address.to_string(),
                detail: source.to_string(),
            })?;
        let paths = DaemonPaths::new(data_dir);
        let mut host = crate::client::DaemonObjectRpcAsyncHost::new(runtime.handle(), false)
            .with_paths(&paths);
        let mut request = request.clone();
        request.auth = None;
        let pool_scope = test_local_object_rpc_carrier_scope(data_dir, address);
        runtime
            .block_on(async {
                let response =
                    sinomo_core::network::ClusterObjectRpcClient::endpoint_only(&mut host)
                        .call_loopback(
                            data_dir,
                            address,
                            sinomo_core::network::ObjectRpcCall::value(request),
                            sinomo_core::network::ClusterObjectRpcOptions::new(
                                sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(timeout),
                            ),
                        )
                        .await?
                        .into_materialized_value_response(usize::MAX)
                        .await;
                sinomo_core::network::retire_pooled_routed_streams(&pool_scope).await;
                response
            })
            .map_err(DaemonError::from)
    }
}
