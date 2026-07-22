use crate::cluster::{
    ClusterNetworkMaintenance, DAEMON_CLUSTER_PUBLISH_INTERVAL, DAEMON_CLUSTER_TICK_INTERVAL,
    DaemonDesiredRolloutReactor, cluster_paths_from_daemon_paths,
    handle_daemon_cluster_kv_snapshot, prune_local_cluster_raft_uncommitted_tail,
};
use crate::dfs::{
    DaemonDfsMaintenanceTickOutcome, DaemonDfsPendingMaintenanceWork,
    daemon_dfs_maintenance_notify, daemon_dfs_node_transport, daemon_dfs_node_transport_on_runtime,
    daemon_dfs_now_unix_nanos, daemon_dfs_pending_maintenance_work,
    evict_daemon_dfs_message_service_cache_on_shutdown,
    notify_daemon_dfs_metadata_publish_route_generation_on_runtime,
    register_daemon_dfs_data_owner_lock, run_daemon_dfs_maintenance_tick_in_blocking_task,
};
use crate::{
    AuthenticatedDaemonCommand, DAEMON_SANDBOX_LIFECYCLE_RECOVERY_STATE_DIR, DaemonClusterStatus,
    DaemonDataOwnerLock, DaemonError, DaemonLock, DaemonPaths, DaemonSignerAuthority,
    daemon_paths_are_shadow, handle_daemon_finalize_shadow_rollout, read_daemon_status,
};
#[allow(unused_imports)]
use sinomo_core::cluster::ClusterKvPathsExt;
use sinomo_core::events::EventStore;
use sinomo_core::node::NodeId;
use sinomo_core::{
    ClusterEndpointPrivacy, ClusterEndpointRecord, ClusterEndpointSource, ClusterMemberRecord,
    ClusterModuleRolloutDesiredRecord, ClusterModuleRolloutReconciliationRecord,
    ClusterModuleRolloutStatusRecord, ClusterTransportKind, cluster_module_rollout_reconcile_key,
};
use sinomo_rollout::{
    AppliedRolloutTransition, RolloutController, RolloutTelemetryAdvanceOptions,
    RolloutTelemetryAdvanceStatus, SelfRolloutPaths, read_rollout_status,
    rollout_main_activation_path, rollout_shadow_finalize_path,
};
use sinomo_sandbox::SandboxFileSystem;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::io::Read;
use std::path::{Component, Path, PathBuf};
use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicU8, Ordering},
};
#[cfg(test)]
use std::thread;
use std::time::{Duration, Instant};
use time::OffsetDateTime;
use tokio::io::AsyncWriteExt as _;

// Durable work that is known but currently cannot run (for example, a repair
// whose target is offline) gets a bounded read-only re-probe. This is not a
// periodic maintenance pass: a fully idle projection has no DFS timer at all.
const DAEMON_DFS_DEFERRED_WORK_REPROBE_INTERVAL: Duration = Duration::from_secs(300);
const DAEMON_DFS_MAINTENANCE_PROBE_ERROR_RETRY_INTERVAL: Duration = Duration::from_secs(30);
pub const DAEMON_DFS_TRIGGERED_MAINTENANCE_MIN_INTERVAL: Duration = Duration::from_secs(30);
const DAEMON_DFS_MAINTENANCE_LEASE_RETRY_DELAY: Duration = Duration::from_secs(5);
const DAEMON_DFS_MAINTENANCE_TASK_COMPLETION_POLL_INTERVAL: Duration = Duration::from_secs(1);
// Full DFS passes own the process's sole backend-bearing service lane. Stagger
// their first acquisition by stable membership rank so a rolling restart does
// not make every node hold its local lane while asking a peer to enter theirs.
const DAEMON_DFS_MAINTENANCE_MEMBER_PHASE_SLOT: Duration = Duration::from_secs(20);
const DAEMON_DFS_MAINTENANCE_MEMBER_PHASE_WINDOW: Duration = Duration::from_secs(300);
// Reserve startup for liveness publication and one live doctor/readiness pass.
// Rank-zero DFS work is intentionally not allowed to consume that same window.
const DAEMON_DFS_MAINTENANCE_NETWORK_STARTUP_GRACE: Duration = Duration::from_secs(15);
pub const DAEMON_CLUSTER_NETWORK_MAINTENANCE_PHASE_TIMEOUT: Duration = Duration::from_secs(10);
pub const DAEMON_SHADOW_ROLLOUT_IDLE_FINALIZATION_INTERVAL: Duration =
    DAEMON_CLUSTER_PUBLISH_INTERVAL;
pub const DAEMON_NETWORK_ENVIRONMENT_WATCH_ENV: &str = "SINOMO_DAEMON_NETWORK_ENVIRONMENT_WATCH";
const DAEMON_RUNTIME_TASK_JOIN_TIMEOUT: Duration = Duration::from_secs(3);
const DAEMON_NETWORK_MODULE_CONTEXT_TASK_JOIN_TIMEOUT: Duration = Duration::from_secs(30);
const DAEMON_RUNTIME_TASK_DRAIN_GRACE: Duration = Duration::from_millis(100);
const DAEMON_RUNTIME_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);
const DAEMON_SANDBOX_STREAM_CHANNEL_CAPACITY: usize = 64;
const DAEMON_SANDBOX_SESSION_BODY_BUFFER_BYTES: usize = 256 * 1024;
const DAEMON_SANDBOX_LOCK_ANCHOR_CONTENT: &[u8] = b"sinomo-sandbox-lifecycle-lock-v1\n";
#[cfg(feature = "sandbox")]
const DAEMON_SANDBOX_LIFECYCLE_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(30);
#[cfg(feature = "sandbox")]
const DAEMON_SANDBOX_LIFECYCLE_SAFETY_SWEEP_INTERVAL: Duration = Duration::from_secs(15 * 60);
const DAEMON_SANDBOX_LIFECYCLE_PENDING_DIR: &str = "pending";
#[cfg(feature = "sandbox")]
const DAEMON_SANDBOX_LIFECYCLE_SWEEP_STAMP: &str = "last-safety-sweep";
const DAEMON_SANDBOX_LIFECYCLE_PENDING_MARKER_CONTENT: &[u8] =
    b"sinomo-sandbox-authoritative-recovery-pending-v1\n";

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
    /// The bounded join connectivity handoff could not publish its generation.
    JoinConnectivityPublishFailed,
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
        Reason::JoinConnectivityPublishFailed => DaemonClusterMaintenanceRequestPlan {
            dirty: DAEMON_CLUSTER_MAINTENANCE_DIRTY_CONNECTIVITY,
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

/// The main daemon owns the mandatory default DFS lifecycle. The shadow daemon
/// is a supervisor and never owns cluster data; there is intentionally no
/// configuration or environment input to this decision.
fn daemon_role_owns_default_dfs(label: &str) -> bool {
    label == "daemon"
}

/// A settled one-node cluster has no peer whose later publication can repair
/// missing DFS placement. Complete its compact control-plane bootstrap before
/// accepting app traffic so a fresh embedded daemon cannot expose SIFS with an
/// empty metadata-shard topology.
fn daemon_default_dfs_bootstrap_must_complete_before_serving(
    paths: &DaemonPaths,
    label: &str,
) -> Result<bool, DaemonError> {
    if !daemon_role_owns_default_dfs(label) {
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

#[derive(Clone, Debug, Eq, PartialEq)]
struct DaemonConnectivityEndpointProjection {
    endpoint_id: sinomo_core::Id,
    node_id: NodeId,
    transport: ClusterTransportKind,
    address: String,
    source: ClusterEndpointSource,
    privacy: ClusterEndpointPrivacy,
    priority: u16,
}

impl From<&ClusterEndpointRecord> for DaemonConnectivityEndpointProjection {
    fn from(endpoint: &ClusterEndpointRecord) -> Self {
        Self {
            endpoint_id: endpoint.endpoint_id.clone(),
            node_id: endpoint.node_id.clone(),
            transport: endpoint.transport.clone(),
            address: endpoint.address.clone(),
            source: endpoint.source.clone(),
            privacy: endpoint.privacy.clone(),
            priority: endpoint.priority,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DaemonConnectivityMemberProjection {
    node_id: NodeId,
    authorized: bool,
    endpoints: Vec<DaemonConnectivityEndpointProjection>,
}

impl From<&ClusterMemberRecord> for DaemonConnectivityMemberProjection {
    fn from(member: &ClusterMemberRecord) -> Self {
        let mut endpoints = member
            .endpoints
            .iter()
            .map(DaemonConnectivityEndpointProjection::from)
            .collect::<Vec<_>>();
        endpoints.sort_by(|left, right| {
            (&left.node_id, &left.endpoint_id, &left.address).cmp(&(
                &right.node_id,
                &right.endpoint_id,
                &right.address,
            ))
        });
        Self {
            node_id: member.node_id.clone(),
            authorized: member.state.is_authorized(),
            endpoints,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum DaemonConnectivityTopologyValue {
    Member(DaemonConnectivityMemberProjection),
    Endpoint(DaemonConnectivityEndpointProjection),
    Connectivity(sinomo_core::network::__connectivity_store::ClusterConnectivityRoutingProjection),
    Malformed(Vec<u8>),
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct DaemonConnectivityProjectionChange {
    probe_inputs_changed: bool,
    route_generation_changed: bool,
}

impl DaemonConnectivityProjectionChange {
    fn record_key(&mut self, key: &str) {
        if daemon_connectivity_probe_input_key(key) {
            self.probe_inputs_changed = true;
            self.route_generation_changed = true;
        } else if daemon_connectivity_record_key(key) {
            self.route_generation_changed = true;
        }
    }
}

#[derive(Debug)]
struct DaemonConnectivityTopologyProjection {
    revision: Option<u64>,
    values: BTreeMap<String, DaemonConnectivityTopologyValue>,
    include_connectivity_records: bool,
}

impl Default for DaemonConnectivityTopologyProjection {
    fn default() -> Self {
        Self::new(true)
    }
}

impl DaemonConnectivityTopologyProjection {
    fn new(include_connectivity_records: bool) -> Self {
        Self {
            revision: None,
            values: BTreeMap::new(),
            include_connectivity_records,
        }
    }

    fn seed_if_newer(&mut self, snapshot: &sinomo_core::kv::KvSnapshot) {
        if self
            .revision
            .is_some_and(|revision| snapshot.revision.0 <= revision)
        {
            return;
        }
        self.values = daemon_connectivity_topology_projection_from_snapshot(
            snapshot,
            self.include_connectivity_records,
        );
        self.revision = Some(snapshot.revision.0);
    }

    fn observe(
        &mut self,
        snapshot: &sinomo_core::kv::KvSnapshot,
    ) -> DaemonConnectivityProjectionChange {
        let Some(revision) = self.revision else {
            let next = daemon_connectivity_topology_projection_from_snapshot(
                snapshot,
                self.include_connectivity_records,
            );
            let changed = daemon_connectivity_projection_change(&self.values, &next);
            self.values = next;
            self.revision = Some(snapshot.revision.0);
            return changed;
        };
        if snapshot.revision.0 <= revision {
            return DaemonConnectivityProjectionChange::default();
        }
        if revision < snapshot.compacted_revision.0 {
            let next = daemon_connectivity_topology_projection_from_snapshot(
                snapshot,
                self.include_connectivity_records,
            );
            let changed = daemon_connectivity_projection_change(&self.values, &next);
            self.values = next;
            self.revision = Some(snapshot.revision.0);
            return changed;
        }

        let first_new = snapshot
            .events
            .partition_point(|event| event.revision.0 <= revision);
        let mut before = BTreeMap::new();
        for event in &snapshot.events[first_new..] {
            if !daemon_connectivity_topology_key(&event.key, self.include_connectivity_records) {
                continue;
            }
            before
                .entry(event.key.clone())
                .or_insert_with(|| self.values.get(&event.key).cloned());
            match &event.kind {
                sinomo_core::kv::KvEventKind::Put { value } => {
                    self.values.insert(
                        event.key.clone(),
                        daemon_connectivity_topology_value(&event.key, value),
                    );
                }
                sinomo_core::kv::KvEventKind::Delete => {
                    self.values.remove(&event.key);
                }
            }
        }
        self.revision = Some(snapshot.revision.0);
        let mut changed = DaemonConnectivityProjectionChange::default();
        for (key, previous) in before {
            if previous != self.values.get(&key).cloned() {
                changed.record_key(&key);
            }
        }
        changed
    }
}

fn daemon_connectivity_projection_change(
    before: &BTreeMap<String, DaemonConnectivityTopologyValue>,
    after: &BTreeMap<String, DaemonConnectivityTopologyValue>,
) -> DaemonConnectivityProjectionChange {
    let mut changed = DaemonConnectivityProjectionChange::default();
    let mut keys = before.keys().map(String::as_str).collect::<BTreeSet<_>>();
    keys.extend(after.keys().map(String::as_str));
    for key in keys {
        if before.get(key) != after.get(key) {
            changed.record_key(key);
        }
    }
    changed
}

fn daemon_connectivity_topology_projection_from_snapshot(
    snapshot: &sinomo_core::kv::KvSnapshot,
    include_connectivity_records: bool,
) -> BTreeMap<String, DaemonConnectivityTopologyValue> {
    snapshot
        .entries
        .iter()
        .filter(|entry| daemon_connectivity_topology_key(&entry.key, include_connectivity_records))
        .map(|entry| {
            (
                entry.key.clone(),
                daemon_connectivity_topology_value(&entry.key, &entry.value),
            )
        })
        .collect()
}

fn daemon_connectivity_topology_value(key: &str, value: &[u8]) -> DaemonConnectivityTopologyValue {
    if key.starts_with("/cluster/members/") {
        return sinomo_core::kv::decode_kv_value::<ClusterMemberRecord>(value)
            .map(|member| {
                DaemonConnectivityTopologyValue::Member(DaemonConnectivityMemberProjection::from(
                    &member,
                ))
            })
            .unwrap_or_else(|_| DaemonConnectivityTopologyValue::Malformed(value.to_vec()));
    }
    if key.starts_with("/cluster/endpoints/") {
        return sinomo_core::kv::decode_kv_value::<ClusterEndpointRecord>(value)
            .map(|endpoint| {
                DaemonConnectivityTopologyValue::Endpoint(
                    DaemonConnectivityEndpointProjection::from(&endpoint),
                )
            })
            .unwrap_or_else(|_| DaemonConnectivityTopologyValue::Malformed(value.to_vec()));
    }
    sinomo_core::kv::decode_kv_value::<sinomo_core::network::ClusterConnectivityRecord>(value)
        .map(|record| {
            DaemonConnectivityTopologyValue::Connectivity(
                sinomo_core::network::__connectivity_store::cluster_connectivity_routing_projection(
                    &record,
                ),
            )
        })
        .unwrap_or_else(|_| DaemonConnectivityTopologyValue::Malformed(value.to_vec()))
}

fn daemon_connectivity_topology_key(key: &str, include_connectivity_records: bool) -> bool {
    daemon_connectivity_probe_input_key(key)
        || (include_connectivity_records && daemon_connectivity_record_key(key))
}

fn daemon_connectivity_probe_input_key(key: &str) -> bool {
    key.starts_with("/cluster/members/") || key.starts_with("/cluster/endpoints/")
}

fn daemon_connectivity_record_key(key: &str) -> bool {
    key.starts_with("/cluster/network/connectivity/")
}

fn subscribe_daemon_connectivity_topology_sources(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
    wake_default_dfs: bool,
) -> Result<sinomo_core::cluster::ClusterKvCommittedSnapshotSubscription, DaemonError> {
    let cluster_paths = cluster_paths_from_daemon_paths(paths)?;
    let projection = Arc::new(Mutex::new(DaemonConnectivityTopologyProjection::new(
        wake_default_dfs,
    )));
    let callback_projection = Arc::clone(&projection);
    let callback_paths = paths.clone();
    let callback_runtime = runtime.clone();
    let subscription = sinomo_core::cluster::subscribe_cluster_kv_committed_snapshots(
        &cluster_paths,
        move |snapshot| {
            let changed = callback_projection
                .lock()
                .map(|mut projection| projection.observe(snapshot))
                .unwrap_or(DaemonConnectivityProjectionChange {
                    probe_inputs_changed: true,
                    route_generation_changed: true,
                });
            if changed.probe_inputs_changed {
                request_daemon_cluster_maintenance(
                    DaemonClusterMaintenanceReason::CommittedConnectivityTopologyChanged,
                );
            }
            if changed.route_generation_changed && wake_default_dfs {
                notify_daemon_dfs_metadata_publish_route_generation(
                    &callback_paths,
                    &callback_runtime,
                    "committed connectivity directory",
                    snapshot.revision.0,
                );
            }
        },
    );
    // Close the register/read race without creating a synthetic startup wake:
    // the maintenance owner already performs one startup reconciliation, and
    // a callback that won the race keeps its newer revision.
    match sinomo_core::cluster::read_cluster_kv_latest_observed_snapshot_arc_if_present(
        &cluster_paths,
    ) {
        Ok(Some(snapshot)) => {
            if let Ok(mut projection) = projection.lock() {
                projection.seed_if_newer(&snapshot);
            }
        }
        Ok(None) => {}
        Err(error) => tracing::warn!(
            "connectivity topology observation started without an initial KV projection: {error}"
        ),
    }
    Ok(subscription)
}

fn notify_daemon_dfs_metadata_publish_route_generation(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
    source: &'static str,
    generation: u64,
) {
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

fn daemon_sandbox_lifecycle_recovery_state_dir(paths: &DaemonPaths) -> PathBuf {
    paths.root.join(DAEMON_SANDBOX_LIFECYCLE_RECOVERY_STATE_DIR)
}

fn daemon_sandbox_lifecycle_pending_dir(paths: &DaemonPaths) -> PathBuf {
    daemon_sandbox_lifecycle_recovery_state_dir(paths).join(DAEMON_SANDBOX_LIFECYCLE_PENDING_DIR)
}

fn daemon_sandbox_lifecycle_pending_marker_path(
    paths: &DaemonPaths,
    sandbox_id: &sinomo_sandbox::SandboxId,
) -> PathBuf {
    daemon_sandbox_lifecycle_pending_dir(paths).join(sandbox_id.as_str())
}

#[cfg(feature = "sandbox")]
fn daemon_sandbox_lifecycle_sweep_stamp_path(paths: &DaemonPaths) -> PathBuf {
    daemon_sandbox_lifecycle_recovery_state_dir(paths).join(DAEMON_SANDBOX_LIFECYCLE_SWEEP_STAMP)
}

#[cfg(feature = "sandbox")]
fn daemon_sandbox_lifecycle_io_error(path: &Path, source: std::io::Error) -> DaemonError {
    DaemonError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    }
}

fn daemon_sandbox_lifecycle_durable_file_error(
    source: sinomo_core::storage::durable_file::DurableFileError,
) -> DaemonError {
    DaemonError::Io {
        path: source.path().to_string(),
        detail: source.detail().to_string(),
    }
}

fn ensure_daemon_sandbox_lifecycle_state_dirs(paths: &DaemonPaths) -> Result<(), DaemonError> {
    let state_dir = daemon_sandbox_lifecycle_recovery_state_dir(paths);
    let pending_dir = daemon_sandbox_lifecycle_pending_dir(paths);
    // Create each level separately through the shared durable-file primitive.
    // It syncs both the new directory and its already-existing parent, using a
    // backup-semantics directory handle where Windows permits it. The periodic
    // safety sweep remains the fallback on filesystems that cannot flush
    // directory metadata explicitly.
    sinomo_core::storage::durable_file::ensure_private_directory(&state_dir)
        .map_err(daemon_sandbox_lifecycle_durable_file_error)?;
    sinomo_core::storage::durable_file::ensure_private_directory(&pending_dir)
        .map_err(daemon_sandbox_lifecycle_durable_file_error)
}

fn write_daemon_sandbox_lifecycle_state(path: &Path, bytes: &[u8]) -> Result<(), DaemonError> {
    sinomo_core::storage::durable_file::write_file_atomic(
        path,
        bytes,
        sinomo_core::storage::durable_file::DurableAtomicFileOptions::new(
            "sandbox-lifecycle-state",
            "tmp",
        )
        .sync_all(),
    )
    .map_err(daemon_sandbox_lifecycle_durable_file_error)
}

fn mark_daemon_sandbox_authoritative_recovery_pending(
    paths: &DaemonPaths,
    sandbox_id: &sinomo_sandbox::SandboxId,
) -> Result<(), DaemonError> {
    ensure_daemon_sandbox_lifecycle_state_dirs(paths)?;
    write_daemon_sandbox_lifecycle_state(
        &daemon_sandbox_lifecycle_pending_marker_path(paths, sandbox_id),
        DAEMON_SANDBOX_LIFECYCLE_PENDING_MARKER_CONTENT,
    )
}

fn clear_daemon_sandbox_authoritative_recovery_pending(
    paths: &DaemonPaths,
    sandbox_id: &sinomo_sandbox::SandboxId,
) -> Result<(), DaemonError> {
    let path = daemon_sandbox_lifecycle_pending_marker_path(paths, sandbox_id);
    sinomo_core::storage::durable_file::remove_file_if_exists_and_sync_parent(&path)
        .map(|_| ())
        .map_err(daemon_sandbox_lifecycle_durable_file_error)
}

fn best_effort_clear_daemon_sandbox_authoritative_recovery_pending(
    paths: &DaemonPaths,
    sandbox_id: &sinomo_sandbox::SandboxId,
    context: &'static str,
) {
    if let Err(error) = clear_daemon_sandbox_authoritative_recovery_pending(paths, sandbox_id) {
        // The authoritative operation has already settled. A stale marker is
        // safe: the next maintenance tick rechecks DFS and clears it. Returning
        // an RPC error here would make a committed mutation look retryable.
        tracing::warn!(
            sandbox_id = %sandbox_id,
            context,
            error = %error,
            "failed to clear settled sandbox lifecycle recovery marker"
        );
    }
}

#[cfg(feature = "sandbox")]
fn daemon_sandbox_authoritative_recovery_pending_ids(
    paths: &DaemonPaths,
) -> Result<BTreeSet<sinomo_sandbox::SandboxId>, DaemonError> {
    let pending_dir = daemon_sandbox_lifecycle_pending_dir(paths);
    let entries = match std::fs::read_dir(&pending_dir) {
        Ok(entries) => entries,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeSet::new()),
        Err(source) => return Err(daemon_sandbox_lifecycle_io_error(&pending_dir, source)),
    };
    let mut sandbox_ids = BTreeSet::new();
    for entry in entries {
        let entry =
            entry.map_err(|source| daemon_sandbox_lifecycle_io_error(&pending_dir, source))?;
        let path = entry.path();
        if !entry
            .file_type()
            .map_err(|source| daemon_sandbox_lifecycle_io_error(&path, source))?
            .is_file()
        {
            return Err(DaemonError::Io {
                path: path.display().to_string(),
                detail: "sandbox lifecycle pending marker is not a regular file".to_string(),
            });
        }
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| DaemonError::Io {
                path: path.display().to_string(),
                detail: "sandbox lifecycle pending marker name is not UTF-8".to_string(),
            })?;
        let sandbox_id =
            sinomo_sandbox::SandboxId::new(name).map_err(|source| DaemonError::Io {
                path: path.display().to_string(),
                detail: format!("invalid sandbox lifecycle pending marker: {source}"),
            })?;
        sandbox_ids.insert(sandbox_id);
    }
    Ok(sandbox_ids)
}

#[cfg(feature = "sandbox")]
fn record_daemon_sandbox_lifecycle_safety_sweep(
    paths: &DaemonPaths,
    now_unix_nanos: u64,
) -> Result<(), DaemonError> {
    ensure_daemon_sandbox_lifecycle_state_dirs(paths)?;
    write_daemon_sandbox_lifecycle_state(
        &daemon_sandbox_lifecycle_sweep_stamp_path(paths),
        now_unix_nanos.to_string().as_bytes(),
    )
}

#[cfg(feature = "sandbox")]
fn daemon_sandbox_lifecycle_safety_sweep_due_or_initialize(
    paths: &DaemonPaths,
    now_unix_nanos: u64,
) -> Result<bool, DaemonError> {
    let path = daemon_sandbox_lifecycle_sweep_stamp_path(paths);
    let stored = match std::fs::read_to_string(&path) {
        Ok(stored) => stored,
        // A one-time startup scan preserves recovery for fences written by an
        // older binary that predates the local marker. The successful scan
        // records the stamp, so subsequent quiet 30-second ticks stay local.
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(true),
        Err(source) => return Err(daemon_sandbox_lifecycle_io_error(&path, source)),
    };
    let Ok(last_sweep_unix_nanos) = stored.trim().parse::<u64>() else {
        return Ok(true);
    };
    if last_sweep_unix_nanos > now_unix_nanos {
        return Ok(true);
    }
    let interval_nanos = u64::try_from(DAEMON_SANDBOX_LIFECYCLE_SAFETY_SWEEP_INTERVAL.as_nanos())
        .unwrap_or(u64::MAX);
    Ok(now_unix_nanos.saturating_sub(last_sweep_unix_nanos) >= interval_nanos)
}

type DaemonSandboxLifecycleLease =
    sinomo_dfs::DfsRenewableExclusiveLease<sinomo_dfs::DfsNodeTransport>;

struct DaemonSandboxExecutionLease {
    lease: Option<DaemonSandboxLifecycleLease>,
    loss_monitor: Option<std::thread::JoinHandle<()>>,
    cancellation: sinomo_sandbox::SandboxCancellationToken,
}

impl DaemonSandboxExecutionLease {
    fn start(
        lease: DaemonSandboxLifecycleLease,
        cancellation: sinomo_sandbox::SandboxCancellationToken,
    ) -> Result<Self, DaemonError> {
        let status = lease.status();
        let monitor_cancellation = cancellation.clone();
        let loss_monitor = std::thread::Builder::new()
            .name("sinomo-sandbox-lease-loss".to_string())
            .spawn(move || {
                if status.wait_until_lost() {
                    monitor_cancellation.fence_lifecycle_mutation();
                }
            })
            .map_err(|source| DaemonError::Parse {
                detail: format!("start sandbox lifecycle lease loss monitor: {source}"),
            })?;
        Ok(Self {
            lease: Some(lease),
            loss_monitor: Some(loss_monitor),
            cancellation,
        })
    }

    fn ensure_held(&self) -> Result<(), sinomo_dfs::DfsRenewableLeaseError> {
        self.lease
            .as_ref()
            .ok_or(sinomo_dfs::DfsRenewableLeaseError::Released)?
            .ensure_held()
    }

    fn cancellation_token(&self) -> sinomo_sandbox::SandboxCancellationToken {
        self.cancellation.clone()
    }
}

impl Drop for DaemonSandboxExecutionLease {
    fn drop(&mut self) {
        if let Some(lease) = self.lease.take()
            && let Err(error) = lease.release()
        {
            tracing::debug!(error = %error, "sandbox lifecycle lease release failed");
        }
        if let Some(monitor) = self.loss_monitor.take()
            && monitor.join().is_err()
        {
            tracing::warn!("sandbox lifecycle lease loss monitor panicked");
        }
    }
}

fn acquire_daemon_sandbox_lifecycle_lease(
    paths: &DaemonPaths,
    sandbox_id: &sinomo_sandbox::SandboxId,
    runtime: Option<&tokio::runtime::Handle>,
) -> Result<DaemonSandboxLifecycleLease, DaemonError> {
    let mut dfs = open_daemon_sandbox_dfs_service_on_runtime(paths, runtime)?;
    let local_node_id = dfs.node_id().clone();
    let replica_candidates = daemon_sandbox_replica_candidates(paths, &local_node_id);
    let lock_root = sinomo_sandbox::sandbox_lock_root();
    dfs.posix_mkdir_all(&lock_root)?;
    let anchor_path = sinomo_sandbox::sandbox_lock_anchor_path(sandbox_id);
    let stat = match dfs.posix_stat_distributed(&anchor_path) {
        Ok(stat) => stat,
        Err(sinomo_dfs::DfsPosixError::NotFound { .. })
        | Err(sinomo_dfs::DfsPosixError::MissingParent { .. }) => {
            match dfs.posix_write_file(
                &anchor_path,
                DAEMON_SANDBOX_LOCK_ANCHOR_CONTENT,
                sinomo_dfs::DfsWriteOptions::replicated_eventual(replica_candidates),
            ) {
                Ok(stat) => stat,
                Err(write_error) => match dfs.posix_stat_distributed(&anchor_path) {
                    Ok(stat) => stat,
                    Err(stat_error) => {
                        return Err(DaemonError::Parse {
                            detail: format!(
                                "failed to create sandbox lifecycle lock anchor {anchor_path}: {write_error}; post-write stat also failed: {stat_error}"
                            ),
                        });
                    }
                },
            }
        }
        Err(error) => return Err(error.into()),
    };
    if matches!(
        stat.kind,
        sinomo_dfs::DfsObjectKind::Directory | sinomo_dfs::DfsObjectKind::Tombstone
    ) {
        return Err(DaemonError::Parse {
            detail: format!(
                "sandbox lifecycle lock anchor is not a regular file: {anchor_path} ({:?})",
                stat.kind
            ),
        });
    }
    let anchor = sinomo_dfs::DfsRenewableLeaseAnchor::new(anchor_path, stat.object_id);
    let session_id = sinomo_dfs::DfsAdvisoryLockSessionId::new(format!(
        "sandbox-lifecycle:{}:{}:{}",
        sandbox_id,
        std::process::id(),
        sinomo_core::Id::generate()
    ));
    sinomo_dfs::DfsRenewableExclusiveLease::try_acquire(
        Arc::new(Mutex::new(dfs)),
        anchor,
        session_id,
        Some(std::process::id()),
        sinomo_dfs::DfsRenewableLeaseConfig::default(),
    )
    .map_err(|source| DaemonError::Parse {
        detail: format!(
            "sandbox {sandbox_id} is busy or its lifecycle lease is unavailable: {source}"
        ),
    })
}

fn ensure_daemon_sandbox_lifecycle_lease(
    lease: &DaemonSandboxLifecycleLease,
    sandbox_id: &sinomo_sandbox::SandboxId,
    phase: &str,
) -> Result<(), DaemonError> {
    lease.ensure_held().map_err(|source| DaemonError::Parse {
        detail: format!("sandbox {sandbox_id} lifecycle lease lost {phase}: {source}"),
    })
}

fn acquire_daemon_sandbox_mutation_lease(
    paths: &DaemonPaths,
    sandbox_id: &sinomo_sandbox::SandboxId,
    runtime: Option<&tokio::runtime::Handle>,
) -> Result<DaemonSandboxExecutionLease, DaemonError> {
    acquire_daemon_sandbox_mutation_lease_with_recovery(paths, sandbox_id, runtime, true)
}

fn acquire_daemon_sandbox_transaction_lease(
    paths: &DaemonPaths,
    sandbox_id: &sinomo_sandbox::SandboxId,
    runtime: Option<&tokio::runtime::Handle>,
) -> Result<DaemonSandboxExecutionLease, DaemonError> {
    let lease =
        acquire_daemon_sandbox_mutation_lease_with_recovery(paths, sandbox_id, runtime, false)?;
    // Publish the marker through the durable-file path before any authoritative
    // SIFS fence can be written. A crash in any later phase then makes the cheap
    // 30-second precheck wake recovery without hydrating DFS while idle.
    mark_daemon_sandbox_authoritative_recovery_pending(paths, sandbox_id)?;
    Ok(lease)
}

fn acquire_daemon_sandbox_mutation_lease_with_recovery(
    paths: &DaemonPaths,
    sandbox_id: &sinomo_sandbox::SandboxId,
    runtime: Option<&tokio::runtime::Handle>,
    recover_authoritative: bool,
) -> Result<DaemonSandboxExecutionLease, DaemonError> {
    let raw_lease = acquire_daemon_sandbox_lifecycle_lease(paths, sandbox_id, runtime)?;
    ensure_daemon_sandbox_lifecycle_lease(&raw_lease, sandbox_id, "before pending settlement")?;
    let lease = DaemonSandboxExecutionLease::start(
        raw_lease,
        sinomo_sandbox::SandboxCancellationToken::new(),
    )?;
    recover_daemon_sandbox_lifecycle_transaction_under_lease(
        paths,
        sandbox_id,
        runtime,
        &lease,
        recover_authoritative,
    )?;
    lease.ensure_held().map_err(|source| DaemonError::Parse {
        detail: format!(
            "sandbox {sandbox_id} lifecycle lease lost after pending settlement: {source}"
        ),
    })?;
    Ok(lease)
}

fn ensure_daemon_sandbox_mutation_lease(
    lease: &DaemonSandboxExecutionLease,
    sandbox_id: &sinomo_sandbox::SandboxId,
    phase: &str,
) -> Result<(), DaemonError> {
    lease.ensure_held().map_err(|source| DaemonError::Parse {
        detail: format!("sandbox {sandbox_id} lifecycle lease lost {phase}: {source}"),
    })
}

fn daemon_sandbox_store_mutation_checkpoint(
    lease: &DaemonSandboxExecutionLease,
    cancellation: &sinomo_sandbox::SandboxCancellationToken,
    sandbox_id: &sinomo_sandbox::SandboxId,
    phase: &str,
) -> Result<(), sinomo_sandbox::SandboxStoreError> {
    if cancellation.lifecycle_mutation_is_fenced() {
        return Err(sinomo_sandbox::SandboxStoreError::Io {
            path: sinomo_sandbox::sandbox_manifest_path(sandbox_id),
            detail: format!("sandbox lifecycle lease was lost {phase}"),
        });
    }
    ensure_daemon_sandbox_mutation_lease(lease, sandbox_id, phase).map_err(|error| {
        sinomo_sandbox::SandboxStoreError::Io {
            path: sinomo_sandbox::sandbox_manifest_path(sandbox_id),
            detail: error.to_string(),
        }
    })
}

#[cfg(feature = "sandbox")]
fn recover_daemon_sandbox_lifecycle_transaction_under_lease(
    paths: &DaemonPaths,
    sandbox_id: &sinomo_sandbox::SandboxId,
    runtime: Option<&tokio::runtime::Handle>,
    lifecycle_lease: &DaemonSandboxExecutionLease,
    recover_authoritative: bool,
) -> Result<(), DaemonError> {
    if recover_authoritative {
        recover_daemon_authoritative_sandbox_transaction(
            paths,
            sandbox_id,
            runtime,
            lifecycle_lease,
        )?;
    }
    Ok(())
}

#[cfg(feature = "sandbox")]
fn recover_daemon_authoritative_sandbox_transaction(
    paths: &DaemonPaths,
    sandbox_id: &sinomo_sandbox::SandboxId,
    runtime: Option<&tokio::runtime::Handle>,
    lifecycle_lease: &DaemonSandboxExecutionLease,
) -> Result<(), DaemonError> {
    ensure_daemon_sandbox_mutation_lease(
        lifecycle_lease,
        sandbox_id,
        "before authoritative transaction recovery",
    )?;
    let dfs = open_daemon_sandbox_dfs_service_on_runtime(paths, runtime)?;
    let node_id = dfs.node_id().clone();
    let replica_candidates = daemon_sandbox_replica_candidates(paths, &node_id);
    let mut store = sinomo_sandbox::SifsSandboxInstanceStore::new(dfs)
        .with_replica_candidates(replica_candidates);
    let manifest = match store.load_allow_pending_reconciliation(sandbox_id) {
        Ok(manifest) => manifest,
        Err(sinomo_sandbox::SandboxStoreError::NotFound { .. })
        | Err(sinomo_sandbox::SandboxStoreError::LegacyLayout { .. }) => {
            best_effort_clear_daemon_sandbox_authoritative_recovery_pending(
                paths,
                sandbox_id,
                "recovery target absent",
            );
            return Ok(());
        }
        Err(error) => return Err(error.into()),
    };
    let Some(pending) = manifest.pending_reconciliation.clone() else {
        best_effort_clear_daemon_sandbox_authoritative_recovery_pending(
            paths,
            sandbox_id,
            "recovery already settled",
        );
        return Ok(());
    };
    match pending.operation {
        sinomo_sandbox::SandboxAuthoritativeMutationOperation::UnsupportedLegacyCommandTransaction => {
            return Err(DaemonError::Parse {
                detail: format!(
                    "sandbox {sandbox_id} has obsolete command transaction {}; recreate the internal-development sandbox",
                    pending.transaction_id
                ),
            });
        }
        sinomo_sandbox::SandboxAuthoritativeMutationOperation::Create => {
            let manifest = store.recover_create(sandbox_id, OffsetDateTime::now_utc())?;
            append_daemon_sandbox_event_durable(
                paths,
                sinomo_sandbox::sandbox_instance_created_event(node_id.clone(), &manifest),
            )?;
            append_daemon_sandbox_event_durable(
                paths,
                sinomo_sandbox::sandbox_toolchain_pinned_event(node_id.clone(), &manifest),
            )?;
            for mount in &manifest.mounts {
                append_daemon_sandbox_event_durable(
                    paths,
                    sinomo_sandbox::sandbox_mount_added_event(node_id.clone(), &manifest, mount),
                )?;
            }
            ensure_daemon_sandbox_mutation_lease(
                lifecycle_lease,
                sandbox_id,
                "after recovered create audit",
            )?;
            store.clear_reconciliation_transaction(
                sandbox_id,
                &pending.transaction_id,
                OffsetDateTime::now_utc(),
            )?;
        }
        sinomo_sandbox::SandboxAuthoritativeMutationOperation::ResumeCheckpoint {
            checkpoint_id,
        } => {
            let manifest =
                store.resume_checkpoint(sandbox_id, &checkpoint_id, OffsetDateTime::now_utc())?;
            append_daemon_sandbox_event_durable(
                paths,
                sinomo_sandbox::sandbox_instance_resumed_event(node_id, &manifest, &checkpoint_id),
            )?;
            ensure_daemon_sandbox_mutation_lease(
                lifecycle_lease,
                sandbox_id,
                "after recovered resume audit",
            )?;
            store.clear_reconciliation_transaction(
                sandbox_id,
                &pending.transaction_id,
                OffsetDateTime::now_utc(),
            )?;
        }
        sinomo_sandbox::SandboxAuthoritativeMutationOperation::Reboot { .. } => {
            let (manifest, cleared_tmp) =
                store.reboot_session(sandbox_id, OffsetDateTime::now_utc())?;
            append_daemon_sandbox_event_durable(
                paths,
                sinomo_sandbox::sandbox_instance_rebooted_event(node_id, &manifest, cleared_tmp),
            )?;
            ensure_daemon_sandbox_mutation_lease(
                lifecycle_lease,
                sandbox_id,
                "after recovered reboot audit",
            )?;
            store.clear_reconciliation_transaction(
                sandbox_id,
                &pending.transaction_id,
                OffsetDateTime::now_utc(),
            )?;
        }
        sinomo_sandbox::SandboxAuthoritativeMutationOperation::LegacyMigration => {
            let cancellation = lifecycle_lease.cancellation_token();
            let manifest = store.migrate_legacy_instance_with_checkpoints(
                sandbox_id,
                OffsetDateTime::now_utc(),
                || {
                    daemon_sandbox_store_mutation_checkpoint(
                        lifecycle_lease,
                        &cancellation,
                        sandbox_id,
                        "during legacy migration",
                    )
                },
            )?;
            append_daemon_sandbox_event_durable(
                paths,
                sinomo_sandbox::sandbox_instance_migrated_event(node_id, &manifest),
            )?;
            ensure_daemon_sandbox_mutation_lease(
                lifecycle_lease,
                sandbox_id,
                "after recovered migration audit",
            )?;
            store.clear_reconciliation_transaction(
                sandbox_id,
                &pending.transaction_id,
                OffsetDateTime::now_utc(),
            )?;
        }
        sinomo_sandbox::SandboxAuthoritativeMutationOperation::Destroy => {
            let cancellation = lifecycle_lease.cancellation_token();
            store.apply_destroy_with_checkpoints(sandbox_id, &pending.transaction_id, || {
                daemon_sandbox_store_mutation_checkpoint(
                    lifecycle_lease,
                    &cancellation,
                    sandbox_id,
                    "during destroy recovery",
                )
            })?;
            append_daemon_sandbox_event_durable(
                paths,
                sinomo_sandbox::sandbox_instance_destroyed_transaction_event(node_id, &manifest),
            )?;
            ensure_daemon_sandbox_mutation_lease(
                lifecycle_lease,
                sandbox_id,
                "after recovered destroy audit",
            )?;
            store.finalize_destroy_with_checkpoints(sandbox_id, &pending.transaction_id, || {
                daemon_sandbox_store_mutation_checkpoint(
                    lifecycle_lease,
                    &cancellation,
                    sandbox_id,
                    "during destroy finalization recovery",
                )
            })?;
        }
    }
    best_effort_clear_daemon_sandbox_authoritative_recovery_pending(
        paths,
        sandbox_id,
        "authoritative recovery settled",
    );
    Ok(())
}

#[cfg(not(feature = "sandbox"))]
fn recover_daemon_sandbox_lifecycle_transaction_under_lease(
    _paths: &DaemonPaths,
    _sandbox_id: &sinomo_sandbox::SandboxId,
    _runtime: Option<&tokio::runtime::Handle>,
    _lifecycle_lease: &DaemonSandboxExecutionLease,
    _recover_authoritative: bool,
) -> Result<(), DaemonError> {
    Ok(())
}

pub fn run_daemon_control_loop(
    options: crate::DaemonRunOptions,
    paths: DaemonPaths,
    label: &str,
    cluster: Option<DaemonClusterStatus>,
) -> Result<(), DaemonError> {
    run_daemon_control_loop_with_services(options, paths, label, cluster, Vec::new())
}

pub fn run_daemon_control_loop_with_services(
    options: crate::DaemonRunOptions,
    paths: DaemonPaths,
    label: &str,
    cluster: Option<DaemonClusterStatus>,
    service_hooks: Vec<crate::DaemonServiceRegistryHook>,
) -> Result<(), DaemonError> {
    run_daemon_control_loop_with_services_and_profile(
        options,
        paths,
        label,
        cluster,
        service_hooks,
        crate::DaemonExecutionProfile::Standard,
    )
}

/// Runs the daemon with an explicit resource profile. Embedded application
/// hosts use this entry point so mobile process constraints do not change the
/// standard standalone daemon defaults.
pub fn run_daemon_control_loop_with_services_and_profile(
    options: crate::DaemonRunOptions,
    paths: DaemonPaths,
    label: &str,
    cluster: Option<DaemonClusterStatus>,
    service_hooks: Vec<crate::DaemonServiceRegistryHook>,
    execution_profile: crate::DaemonExecutionProfile,
) -> Result<(), DaemonError> {
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
    crate::observe::register_observe_metric_exporters(&service_hooks);
    if let Err(error) = prune_local_cluster_raft_uncommitted_tail(&paths) {
        tracing::warn!("sinomo {label} cluster raft startup repair skipped: {error}");
    }
    let daemon_runtime = crate::build_daemon_async_runtime_with_profile(label, execution_profile)?;
    let daemon_runtime_handle = daemon_runtime.handle().clone();
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

    let mut default_dfs_reconciliation =
        if daemon_role_owns_default_dfs(label) && startup_cluster_runtime_enabled {
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
        if daemon_default_dfs_bootstrap_must_complete_before_serving(&paths, label)? {
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

    // The service host is built before the maintenance loop so the loop can
    // publish the active service-registry snapshot's instances on its cluster
    // tick.
    let service_host = daemon_runtime.block_on(
        crate::service_adapter::DaemonServiceRegistryHost::new_on_runtime_with_services(
            paths.clone(),
            daemon_runtime_handle.clone(),
            service_hooks,
        ),
    )?;

    let maintenance_paths = paths.clone();
    let maintenance_options = options.clone();
    let maintenance_label = label.to_string();
    let maintenance_idle_stats = idle_stats.clone();
    let maintenance_service_registry = service_host.service_registry_snapshot_store();
    let maintenance_runtime = daemon_runtime_handle.clone();
    let maintenance_startup_cluster = cluster.clone();
    // The main daemon owns DFS data. Shadow processes still observe committed
    // topology for their own network maintenance but must not schedule a DFS
    // lane, even when both roles share one data root during a handoff.
    let maintenance_default_dfs_enabled =
        daemon_role_owns_default_dfs(label) && startup_cluster_runtime_enabled;
    let maintenance_connectivity_topology_subscription = if startup_cluster_runtime_enabled {
        Some(subscribe_daemon_connectivity_topology_sources(
            &paths,
            &daemon_runtime_handle,
            maintenance_default_dfs_enabled,
        )?)
    } else {
        None
    };
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
    if daemon_rollout_telemetry_enabled(label, startup_cluster_runtime_enabled) {
        let reactor = crate::rollout::DaemonRolloutTelemetryReactor::new(&paths)?;
        runtime_tasks.push((
            "rollout telemetry reconciliation",
            daemon_runtime_handle.spawn(reactor.run_until_stopped_async(
                paths.clone(),
                shutdown_rx.clone(),
                label.to_string(),
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
            maintenance_service_registry,
            maintenance_runtime,
            maintenance_startup_cluster,
            maintenance_connectivity_topology_subscription,
            startup_cluster_runtime_enabled,
            maintenance_default_dfs_enabled,
            shutdown_rx.clone(),
        )),
    ));

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
    if daemon_role_owns_default_dfs(label)
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

fn daemon_dfs_maintenance_member_phase(state: &sinomo_core::ClusterBootstrapState) -> Duration {
    let mut member_node_ids = state
        .members
        .iter()
        .filter(|member| member.state.is_authorized())
        .map(|member| member.node_id.clone())
        .collect::<Vec<_>>();
    if !member_node_ids.contains(&state.local_node_id) {
        member_node_ids.push(state.local_node_id.clone());
    }
    member_node_ids.sort();
    member_node_ids.dedup();
    let rank = member_node_ids
        .iter()
        .position(|node_id| node_id == &state.local_node_id)
        .unwrap_or_default();
    let member_count = u128::try_from(member_node_ids.len())
        .unwrap_or(u128::MAX)
        .max(1);
    let rank = u128::try_from(rank).unwrap_or(u128::MAX);
    // Keep the ordinary 20s spacing, but compress it evenly when a larger
    // membership would otherwise make later ranks collide at the interval cap.
    let slot_nanos = DAEMON_DFS_MAINTENANCE_MEMBER_PHASE_SLOT
        .as_nanos()
        .min(DAEMON_DFS_MAINTENANCE_MEMBER_PHASE_WINDOW.as_nanos() / member_count);
    let phase_nanos = slot_nanos.saturating_mul(rank);
    Duration::from_nanos(u64::try_from(phase_nanos).unwrap_or(u64::MAX))
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
) -> OwnedDaemonDfsMaintenanceTask {
    OwnedDaemonDfsMaintenanceTask::new(tokio::spawn(
        run_daemon_dfs_maintenance_tick_in_blocking_task(paths, runtime),
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DaemonDfsMaintenanceProbeDecision {
    trigger_maintenance: bool,
    immediate_continuation: bool,
    next_probe_at: Option<Instant>,
}

fn daemon_dfs_maintenance_probe_decision(
    observed_at: Instant,
    observed_at_unix_nanos: u64,
    work: Option<DaemonDfsPendingMaintenanceWork>,
) -> DaemonDfsMaintenanceProbeDecision {
    let Some(work) = work else {
        return DaemonDfsMaintenanceProbeDecision {
            trigger_maintenance: false,
            immediate_continuation: false,
            next_probe_at: Some(
                observed_at
                    .checked_add(DAEMON_DFS_MAINTENANCE_PROBE_ERROR_RETRY_INTERVAL)
                    .unwrap_or(observed_at),
            ),
        };
    };
    let trigger_maintenance = work.should_trigger_background_maintenance();
    let next_probe_delay = if trigger_maintenance {
        // A successful maintenance pass performs its own follow-up probe. Keep
        // the short safety deadline in case the request is cancelled before a
        // pass starts or completes.
        Some(DAEMON_DFS_MAINTENANCE_PROBE_ERROR_RETRY_INTERVAL)
    } else {
        work.next_maintenance_deadline_unix_nanos()
            .map(|retry_at| Duration::from_nanos(retry_at.saturating_sub(observed_at_unix_nanos)))
            .into_iter()
            .chain(
                work.has_deferred_work()
                    .then_some(DAEMON_DFS_DEFERRED_WORK_REPROBE_INTERVAL),
            )
            .min()
    };
    DaemonDfsMaintenanceProbeDecision {
        trigger_maintenance,
        // A probe classifies newly observed durable work. Only the follow-up
        // probe owned by a successfully completed bounded pass may turn the
        // same classification into a work-conserving continuation.
        immediate_continuation: false,
        next_probe_at: next_probe_delay
            .map(|delay| observed_at.checked_add(delay).unwrap_or(observed_at)),
    }
}

fn daemon_dfs_maintenance_completion_decision(
    outcome: DaemonDfsMaintenanceTickOutcome,
    observed_at: Instant,
    observed_at_unix_nanos: u64,
    work: Option<DaemonDfsPendingMaintenanceWork>,
) -> DaemonDfsMaintenanceProbeDecision {
    let mut decision =
        daemon_dfs_maintenance_probe_decision(observed_at, observed_at_unix_nanos, work);
    match outcome {
        DaemonDfsMaintenanceTickOutcome::Completed { reached_limit } => {
            // Only the pass that produced an explicit bounded remainder owns
            // a work-conserving continuation. The follow-up probe is still
            // required so externally resolved work cannot retain that owner;
            // unrelated work first observed by this probe remains ordinarily
            // interval-gated.
            decision.immediate_continuation = reached_limit && decision.trigger_maintenance;
        }
        DaemonDfsMaintenanceTickOutcome::Deferred => {
            // A deferred pass is not evidence that actionable work remains. It
            // may only mean that a quiet periodic pass yielded to foreground
            // DFS traffic or lost cluster admission. Reclassify durable state
            // like newly probed work so repeated preemption cannot turn an idle
            // node into a short-cadence maintenance loop.
            decision.immediate_continuation = false;
        }
    }
    decision
}

fn daemon_dfs_maintenance_revalidation_decision(
    observed_at: Instant,
    observed_at_unix_nanos: u64,
    work: Option<DaemonDfsPendingMaintenanceWork>,
    immediate_continuation_requested: bool,
) -> DaemonDfsMaintenanceProbeDecision {
    let mut decision =
        daemon_dfs_maintenance_probe_decision(observed_at, observed_at_unix_nanos, work);
    // Revalidation may preserve an already-owned continuation while the work
    // remains actionable, but it must never promote newly discovered work.
    decision.immediate_continuation =
        immediate_continuation_requested && decision.trigger_maintenance;
    decision
}

fn daemon_dfs_failed_maintenance_decision(
    observed_at: Instant,
    triggered_maintenance_requested: bool,
    next_probe_at: Option<Instant>,
) -> DaemonDfsMaintenanceProbeDecision {
    let error_retry_at = observed_at
        .checked_add(DAEMON_DFS_MAINTENANCE_PROBE_ERROR_RETRY_INTERVAL)
        .unwrap_or(observed_at);
    DaemonDfsMaintenanceProbeDecision {
        // Task failure must not consume an already-classified request.
        trigger_maintenance: triggered_maintenance_requested,
        // A failed pass is no longer a successful bounded continuation. Its
        // next attempt uses the ordinary admission and error-backoff path.
        immediate_continuation: false,
        // A notification is a request to inspect pending work, not proof that
        // a full pass is required. Preserve an already-due notification probe
        // across task failure while retaining the bounded error retry.
        next_probe_at: Some(
            next_probe_at
                .map(|next_probe_at| next_probe_at.min(error_retry_at))
                .unwrap_or(error_retry_at),
        ),
    }
}

fn daemon_dfs_maintenance_notification_decision(
    observed_at: Instant,
    triggered_maintenance_requested: bool,
    immediate_continuation_requested: bool,
    next_probe_at: Option<Instant>,
) -> DaemonDfsMaintenanceProbeDecision {
    // Notifications are edge-triggered hints from several sources, including
    // generic cluster refreshes and replicated metadata. They must wake the
    // classifier, not bypass it and launch a full metadata pass directly.
    DaemonDfsMaintenanceProbeDecision {
        trigger_maintenance: triggered_maintenance_requested,
        immediate_continuation: triggered_maintenance_requested && immediate_continuation_requested,
        next_probe_at: Some(
            next_probe_at
                .map(|next_probe_at| next_probe_at.min(observed_at))
                .unwrap_or(observed_at),
        ),
    }
}

fn apply_daemon_dfs_maintenance_probe_result(
    work: Option<DaemonDfsPendingMaintenanceWork>,
    triggered_maintenance_requested: &mut bool,
    immediate_continuation_requested: &mut bool,
    next_probe_at: &mut Option<Instant>,
) {
    let decision =
        daemon_dfs_maintenance_probe_decision(Instant::now(), daemon_dfs_now_unix_nanos(), work);
    *triggered_maintenance_requested = decision.trigger_maintenance;
    *immediate_continuation_requested = decision.immediate_continuation;
    *next_probe_at = decision.next_probe_at;
}

fn daemon_dfs_maintenance_due_in(
    now: Instant,
    initial_not_before: Instant,
    last_maintenance: Instant,
    triggered_maintenance_requested: bool,
    immediate_continuation_requested: bool,
) -> Option<Duration> {
    triggered_maintenance_requested.then(|| {
        let startup_remaining = initial_not_before.saturating_duration_since(now);
        let interval_remaining = if immediate_continuation_requested {
            Duration::ZERO
        } else {
            daemon_maintenance_interval_remaining(
                now,
                last_maintenance,
                DAEMON_DFS_TRIGGERED_MAINTENANCE_MIN_INTERVAL,
            )
        };
        startup_remaining.max(interval_remaining)
    })
}

fn daemon_dfs_maintenance_probe_is_due(now: Instant, next_probe_at: Option<Instant>) -> bool {
    next_probe_at.is_some_and(|next_probe_at| now >= next_probe_at)
}

fn daemon_dfs_maintenance_should_launch(
    triggered_due: bool,
    triggered_work_revalidated: bool,
) -> bool {
    triggered_due && triggered_work_revalidated
}

fn daemon_rollout_telemetry_enabled(label: &str, maintain_cluster_runtime: bool) -> bool {
    label == "daemon" && maintain_cluster_runtime
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

fn daemon_dfs_initial_maintenance_delay(
    member_phase: Duration,
    maintain_cluster_runtime: bool,
) -> Duration {
    if maintain_cluster_runtime {
        member_phase.saturating_add(DAEMON_DFS_MAINTENANCE_NETWORK_STARTUP_GRACE)
    } else {
        member_phase
    }
}

async fn run_daemon_background_maintenance_loop(
    paths: DaemonPaths,
    options: crate::DaemonRunOptions,
    label: String,
    idle_stats: Arc<crate::IdleStatsAtomics>,
    service_registry: sinomo_core::service::ServiceRegistrySnapshotStore,
    runtime: tokio::runtime::Handle,
    startup_cluster: Option<DaemonClusterStatus>,
    connectivity_topology_subscription: Option<
        sinomo_core::cluster::ClusterKvCommittedSnapshotSubscription,
    >,
    maintain_cluster_runtime: bool,
    maintain_default_dfs: bool,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
    // This subscription is the durable membership/endpoint invalidation
    // source for connectivity. Retain it for exactly the maintenance owner's
    // lifetime; periodic probing remains only an expiry refresh mechanism.
    let _connectivity_topology_subscription = connectivity_topology_subscription;
    let mut cluster_network = ClusterNetworkMaintenance::with_idle_stats(idle_stats.clone())
        .await
        .with_service_registry(service_registry);
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
    // Probe once immediately after startup. Successful quiet probes choose an
    // adaptive next deadline; only probe failures retain the short safety
    // retry cadence.
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
                    let probe_paths = paths.clone();
                    let work = match tokio::task::spawn_blocking(move || {
                        daemon_dfs_pending_maintenance_work(&probe_paths)
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
                    let decision = daemon_dfs_maintenance_completion_decision(
                        outcome,
                        Instant::now(),
                        daemon_dfs_now_unix_nanos(),
                        work,
                    );
                    dfs_triggered_maintenance_requested = decision.trigger_maintenance;
                    dfs_immediate_continuation_requested = decision.immediate_continuation;
                    next_dfs_maintenance_probe = decision.next_probe_at;
                }
                None => {
                    let decision = daemon_dfs_failed_maintenance_decision(
                        Instant::now(),
                        dfs_triggered_maintenance_requested,
                        next_dfs_maintenance_probe,
                    );
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
            let work = match tokio::task::spawn_blocking(move || {
                daemon_dfs_pending_maintenance_work(&probe_paths)
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
        {
            // A trigger is a durable-work classification observed at an
            // earlier point in time. Revalidate it at the launch boundary so
            // resolved work never acquires the broad DFS service lane or its
            // distributed maintenance lease.
            let probe_paths = paths.clone();
            let work = match tokio::task::spawn_blocking(move || {
                daemon_dfs_pending_maintenance_work(&probe_paths)
            })
            .await
            {
                Ok(Ok(work)) => Some(work),
                Ok(Err(error)) => {
                    tracing::warn!(
                        "sinomo {label} DFS maintenance pre-launch probe failed: {error}"
                    );
                    None
                }
                Err(error) => {
                    tracing::warn!(
                        "sinomo {label} DFS maintenance pre-launch probe task failed: {error}"
                    );
                    None
                }
            };
            let decision = daemon_dfs_maintenance_revalidation_decision(
                Instant::now(),
                daemon_dfs_now_unix_nanos(),
                work,
                dfs_immediate_continuation_requested,
            );
            dfs_triggered_maintenance_requested = decision.trigger_maintenance;
            dfs_immediate_continuation_requested = decision.immediate_continuation;
            next_dfs_maintenance_probe = decision.next_probe_at;
            if daemon_shutdown_requested(&shutdown_rx) {
                break;
            }
            if daemon_dfs_maintenance_should_launch(
                dfs_triggered_maintenance_due,
                dfs_triggered_maintenance_requested,
            ) {
                let dfs_paths = paths.clone();
                let dfs_runtime = runtime.clone();
                dfs_maintenance_task =
                    Some(spawn_daemon_dfs_maintenance_task(dfs_paths, dfs_runtime));
                dfs_triggered_maintenance_requested = false;
                dfs_immediate_continuation_requested = false;
            }
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
                let decision = daemon_dfs_maintenance_notification_decision(
                    Instant::now(),
                    dfs_triggered_maintenance_requested,
                    dfs_immediate_continuation_requested,
                    next_dfs_maintenance_probe,
                );
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
    loop {
        if *shutdown_rx.borrow() {
            break;
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
    if daemon_control_stream_uses_object_rpc_call(&stream).await? {
        return handle_daemon_object_rpc_call_async(
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
) -> Result<bool, DaemonError> {
    let mut acceptor = sinomo_core::network::accept_routed_stream_carrier(carrier);
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

async fn daemon_control_stream_uses_object_rpc_call(
    stream: &sinomo_core::network::AsyncDaemonControlStream,
) -> Result<bool, DaemonError> {
    let mut prefix = [0_u8; 4];
    let deadline = tokio::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    loop {
        let available = tokio::time::timeout_at(deadline, stream.peek_prefix(&mut prefix))
            .await
            .map_err(|_| {
                DaemonError::from(sinomo_core::network::NetworkError::timeout(
                    "peek daemon object RPC protocol",
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
                sinomo_core::network::local_object_rpc_stream_prelude_prefix_matches(&prefix),
            );
        }
        if !sinomo_core::network::local_object_rpc_stream_prelude_prefix_matches(
            &prefix[..available],
        ) {
            return Ok(false);
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

async fn handle_daemon_object_rpc_call_async(
    stream: sinomo_core::network::AsyncDaemonControlStream,
    paths: DaemonPaths,
    service_host: crate::service_adapter::DaemonServiceRegistryHost,
    service_runtime: tokio::runtime::Handle,
    stream_address: String,
) -> Result<bool, DaemonError> {
    handle_daemon_local_stream_native_service_rpc_async(
        stream.into_inner(),
        paths,
        service_host,
        service_runtime,
        stream_address,
    )
    .await
}

async fn handle_daemon_local_stream_native_service_rpc_async(
    mut stream: tokio::net::TcpStream,
    paths: DaemonPaths,
    service_host: crate::service_adapter::DaemonServiceRegistryHost,
    service_runtime: tokio::runtime::Handle,
    _stream_address: String,
) -> Result<bool, DaemonError> {
    if !stream
        .peer_addr()
        .is_ok_and(|address| address.ip().is_loopback())
    {
        return Err(DaemonError::AuthDenied);
    }
    let context = sinomo_core::network::read_local_object_rpc_stream_prelude(&mut stream).await?;
    let data_dir = sinomo_core::local::SinomoDataDir::new(daemon_data_dir_path(&paths)?);
    let local_public_key = sinomo_core::network::cached_daemon_signing_public_key(&data_dir)
        .map_err(DaemonError::from)?;
    let local_public_key = sinomo_core::network::daemon_auth_encode_public_key(&local_public_key);
    let responder_node_id = cluster_paths_from_daemon_paths(&paths)
        .ok()
        .and_then(|cluster_paths| {
            sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths).ok()
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
    let result = sinomo_core::network::serve_service_rpc_stream(
        stream,
        &context,
        sinomo_core::network::ObjectFrameIoOptions::default(),
        move |request, peer| async move {
            if !matches!(
                peer.identity,
                sinomo_core::network::RpcPeerIdentity::UnauthenticatedLocal
            ) {
                return Err(sinomo_core::network::NetworkError::AuthDenied);
            }
            let authenticated =
                crate::service_adapter::authenticate_daemon_control_request_via_registry(
                    &handler_paths,
                    &request.request,
                    &handler_host,
                )
                .map_err(|_| sinomo_core::network::NetworkError::AuthDenied)?;
            if authenticated.signer != DaemonSignerAuthority::Local
                || authenticated.signer_public_key != local_public_key
                || responder_node_id
                    .as_deref()
                    .is_some_and(|node_id| authenticated.signer_node_id.as_str() != node_id)
            {
                return Err(sinomo_core::network::NetworkError::AuthDenied);
            }
            let route = request.request.route.clone();
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
        },
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

/// Final service boundary for one authenticated routed stream. Routing has
/// already proven the end-to-end source and destination identities; this
/// layer independently verifies that the typed request is signed by that
/// source before consulting the service registry.
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
    let peer_public_key =
        sinomo_core::network::daemon_auth_decode_public_key(&peer_public_key_encoded)?;
    let peer_is_raft_voter = peer_directory_node.has_raft_vote();
    let completion = Arc::new(Mutex::new(("<unavailable>".to_string(), false)));
    let handler_completion = Arc::clone(&completion);
    let handler_paths = paths.clone();
    let handler_host = service_host.clone();
    let handler_runtime = service_runtime.clone();
    let handler_peer_node_id = peer_node_id.clone();
    let handler_peer_public_key = peer_public_key_encoded;
    let result = sinomo_core::network::serve_service_rpc_stream(
        stream,
        &context,
        sinomo_core::network::ObjectFrameIoOptions::default(),
        move |request, peer| async move {
            let authenticated =
                crate::service_adapter::authenticate_routed_daemon_control_request_via_registry(
                    &handler_paths,
                    &request.request,
                    &handler_host,
                    handler_peer_node_id.as_str(),
                    &handler_peer_public_key,
                    peer_is_raft_voter,
                )
                .map_err(|_| sinomo_core::network::NetworkError::AuthDenied)?;
            let context_peer = match peer.identity {
                sinomo_core::network::RpcPeerIdentity::Authenticated { peer_node_id, .. } => {
                    peer_node_id
                }
                sinomo_core::network::RpcPeerIdentity::UnauthenticatedLocal => {
                    return Err(sinomo_core::network::NetworkError::AuthDenied);
                }
            };
            let signer_public_key = sinomo_core::network::daemon_auth_decode_public_key(
                &authenticated.signer_public_key,
            )?;
            if context_peer != handler_peer_node_id
                || authenticated.signer_node_id != context_peer.as_str()
                || signer_public_key != peer_public_key
            {
                return Err(sinomo_core::network::NetworkError::AuthDenied);
            }
            let route = request.request.route.clone();
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
            response
                .response
                .mark_responder_node_id(local_node_id.to_string());
            *handler_completion
                .lock()
                .map_err(|_| sinomo_core::network::NetworkError::Io {
                    operation: "record stream-native service completion".to_string(),
                    detail: "service completion lock poisoned".to_string(),
                })? = (effective_route, stop);
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
        let data_dir = sinomo_core::local::SinomoDataDir::new(daemon_data_dir_path(paths)?);
        let response = crate::service_adapter::handle_daemon_cluster_network_ping_request(
            paths, &data_dir, request,
        )
        .await
        .map_err(DaemonError::from)?;
        Ok((response, false, route))
    } else if route == crate::DAEMON_OBJECT_RPC_FORWARD_ROUTE
        || route == crate::DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE
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
        let payload = request
            .request
            .decode_body::<crate::DaemonObjectRpcForwardRequest>()
            .map_err(DaemonError::from);
        record_daemon_stream_timeline_stage(
            timeline,
            sinomo_core::network::network_timeline_stage::FORWARD_DECODE,
            decode_started_ns,
            payload.is_ok(),
        );
        let payload = payload?;
        let (forwarded, body) =
            crate::service_adapter::forward_object_rpc_call_from_daemon_control(
                paths,
                payload,
                request.body,
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
        if !request.body.is_empty() {
            return Err(DaemonError::Parse {
                detail: "sandbox stream session does not accept an attached request body"
                    .to_string(),
            });
        }
        Ok((
            daemon_sandbox_stream_rpc_response(paths, request.request, service_runtime)?,
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

fn daemon_sandbox_stream_rpc_response(
    paths: &DaemonPaths,
    request: sinomo_core::network::ObjectRpcRequest,
    runtime: &tokio::runtime::Handle,
) -> Result<sinomo_core::network::ObjectRpcOutgoingResponse, DaemonError> {
    let route = request.route.clone();
    let response = sinomo_core::network::ObjectRpcResponse::encode_body(&route, &())
        .map_err(DaemonError::from)?;
    let (reader, mut writer) = tokio::io::duplex(DAEMON_SANDBOX_SESSION_BODY_BUFFER_BYTES);
    let paths = paths.clone();
    let runtime = runtime.clone();
    let producer = match route.as_str() {
        sinomo_sandbox::SANDBOX_RUN_TOOLCHAIN_STREAM_ROUTE => {
            let payload = request.decode_body().map_err(DaemonError::from)?;
            tokio::spawn(async move {
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
            })
        }
        sinomo_sandbox::SANDBOX_RUN_PROCESS_PLAN_STREAM_ROUTE => {
            let payload = request.decode_body().map_err(DaemonError::from)?;
            tokio::spawn(async move {
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
                        write_daemon_sandbox_process_plan_stream_frame_async(&mut writer, &frame)
                            .await;
                }
                let _ = writer.shutdown().await;
            })
        }
        _ => {
            return Err(DaemonError::Network {
                address: route,
                detail: "unsupported sandbox session stream route".to_string(),
            });
        }
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
    let lifecycle_lease =
        acquire_daemon_sandbox_mutation_lease(paths, &request.sandbox_id, runtime)?;
    let config = load_daemon_sandbox_policy_config(paths)?;
    let dfs = open_daemon_sandbox_dfs_service_on_runtime(paths, runtime)?;
    let execution_node_id = dfs.node_id().clone();
    let replica_candidates = daemon_sandbox_replica_candidates(paths, &execution_node_id);
    let mut store = sinomo_sandbox::SifsSandboxInstanceStore::new(dfs)
        .with_replica_candidates(replica_candidates.clone());
    let manifest = store.load(&request.sandbox_id)?;
    if let Err(denial) = config
        .policy
        .validate_manifest(&manifest, &execution_node_id)
    {
        append_daemon_sandbox_event(
            paths,
            sinomo_sandbox::sandbox_policy_denied_event(
                execution_node_id,
                Some(&request.sandbox_id),
                "sandbox-fetch",
                &denial,
            ),
        )?;
        return Err(DaemonError::Parse {
            detail: denial.to_string(),
        });
    }
    if let Err(denial) = config.policy.validate_http_request(&request.request) {
        append_daemon_sandbox_event(
            paths,
            sinomo_sandbox::sandbox_policy_denied_event(
                execution_node_id,
                Some(&request.sandbox_id),
                "sandbox-fetch",
                &denial,
            ),
        )?;
        return Err(DaemonError::Parse {
            detail: denial.to_string(),
        });
    }
    let mut manifest = manifest;
    manifest
        .advance_generation()
        .map_err(|source| DaemonError::Parse {
            detail: source.to_string(),
        })?;
    manifest.updated_at = OffsetDateTime::now_utc();
    ensure_daemon_sandbox_mutation_lease(
        &lifecycle_lease,
        &request.sandbox_id,
        "before fetch generation fence",
    )?;
    store.save_manifest(&manifest)?;
    ensure_daemon_sandbox_mutation_lease(
        &lifecycle_lease,
        &request.sandbox_id,
        "after fetch generation fence",
    )?;

    let command_id = sinomo_sandbox::SandboxCommandId::generate();
    let mut fs = sinomo_sandbox::SifsSandboxFileSystem::new(store.into_inner(), &manifest)
        .with_replica_candidates(replica_candidates.clone());
    let fetch_result = sinomo_sandbox::sandbox_http_fetch_to_file(
        &config.policy,
        &DaemonSandboxHttpTransport,
        &mut fs,
        manifest.sandbox_id.clone(),
        command_id.clone(),
        &request.request,
        &request.output_path,
        &manifest.limits,
    );
    ensure_daemon_sandbox_mutation_lease(
        &lifecycle_lease,
        &request.sandbox_id,
        "after fetch workspace mutation",
    )?;
    let result = match fetch_result {
        Ok(result) => result,
        Err(error) => {
            if let sinomo_sandbox::SandboxRecordedHttpFetchError::Fs(
                sinomo_sandbox::SandboxFsError::QuotaExceeded {
                    kind,
                    actual,
                    limit,
                },
            ) = &error
            {
                append_daemon_sandbox_event(
                    paths,
                    sinomo_sandbox::sandbox_resource_exhausted_event(
                        execution_node_id.clone(),
                        &manifest.sandbox_id,
                        "sandbox-fetch",
                        kind.clone(),
                        *actual,
                        *limit,
                    ),
                )?;
            }
            if let Some(record) = daemon_sandbox_http_exchange_record_for_command(&fs, &command_id)?
            {
                append_daemon_sandbox_event(
                    paths,
                    sinomo_sandbox::sandbox_http_exchange_event(execution_node_id, &record),
                )?;
            }
            return Err(error.into());
        }
    };
    append_daemon_sandbox_event(
        paths,
        sinomo_sandbox::sandbox_file_written_event_with_context(
            execution_node_id.clone(),
            &manifest.sandbox_id,
            &request.output_path,
            result.response_bytes,
            "sandbox-fetch",
            Some(&result.record.command_id),
            Some(&result.request_id),
        ),
    )?;
    append_daemon_sandbox_event(
        paths,
        sinomo_sandbox::sandbox_http_exchange_event(execution_node_id, &result.record),
    )?;
    Ok(result)
}

pub fn handle_daemon_sandbox_create(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxCreateInstanceRequest,
) -> Result<sinomo_sandbox::SandboxCreateInstanceResponse, DaemonError> {
    handle_daemon_sandbox_create_on_runtime(paths, request, None)
}

fn daemon_sandbox_create_limits(
    request: &sinomo_sandbox::SandboxCreateInstanceRequest,
    policy_max: &sinomo_sandbox::SandboxResourceLimits,
) -> sinomo_sandbox::SandboxResourceLimits {
    sinomo_sandbox::SandboxResourceLimits {
        memory_mb: request.memory_mb.unwrap_or(policy_max.memory_mb),
        wall_time_ms: request.wall_time_ms.unwrap_or(policy_max.wall_time_ms),
        max_input_bytes: request
            .max_input_bytes
            .unwrap_or(policy_max.max_input_bytes),
        max_output_bytes: request
            .max_output_bytes
            .unwrap_or(policy_max.max_output_bytes),
        max_workspace_bytes: request
            .max_workspace_bytes
            .unwrap_or(policy_max.max_workspace_bytes),
        max_written_bytes_per_command: request
            .max_written_bytes_per_command
            .unwrap_or(policy_max.max_written_bytes_per_command),
        max_file_count: request.max_file_count.unwrap_or(policy_max.max_file_count),
    }
}

pub(crate) fn handle_daemon_sandbox_create_on_runtime(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxCreateInstanceRequest,
    runtime: Option<&tokio::runtime::Handle>,
) -> Result<sinomo_sandbox::SandboxCreateInstanceResponse, DaemonError> {
    let config = load_daemon_sandbox_policy_config(paths)?;
    let create_limits = daemon_sandbox_create_limits(&request, &config.policy.max_limits);
    let mut dfs = open_daemon_sandbox_dfs_service_on_runtime(paths, runtime)?;
    let execution_node_id = dfs.node_id().clone();
    let replica_candidates = daemon_sandbox_replica_candidates(paths, &execution_node_id);
    let sandbox_id = request
        .sandbox_id
        .unwrap_or_else(sinomo_sandbox::SandboxId::generate);
    let lifecycle_lease = acquire_daemon_sandbox_transaction_lease(paths, &sandbox_id, runtime)?;
    ensure_daemon_sandbox_mutation_lease(&lifecycle_lease, &sandbox_id, "before create")?;
    let mut manifest = sinomo_sandbox::SandboxInstanceManifest::new(
        sandbox_id,
        request.toolchain_id,
        OffsetDateTime::now_utc(),
    )
    .map_err(|source| DaemonError::Parse {
        detail: source.to_string(),
    })?;
    manifest.temp_policy = request.temp_policy;
    manifest.limits = create_limits;
    manifest.mounts = request.mounts;
    if let Err(denial) = config
        .policy
        .validate_manifest(&manifest, &execution_node_id)
    {
        append_daemon_sandbox_event(
            paths,
            sinomo_sandbox::sandbox_policy_denied_event(
                execution_node_id.clone(),
                Some(&manifest.sandbox_id),
                "sandbox-create",
                &denial,
            ),
        )?;
        if denial.code.starts_with("mount-") {
            append_daemon_sandbox_event(
                paths,
                sinomo_sandbox::sandbox_mount_denied_event(
                    execution_node_id,
                    Some(&manifest.sandbox_id),
                    "sandbox-create",
                    &denial,
                ),
            )?;
        }
        return Err(DaemonError::Parse {
            detail: denial.to_string(),
        });
    }
    validate_daemon_sandbox_resolved_mount_policy(
        paths,
        &config.policy,
        &mut dfs,
        &manifest,
        &execution_node_id,
        "sandbox-create-resolved-source",
    )?;

    let mut store = sinomo_sandbox::SifsSandboxInstanceStore::new(dfs)
        .with_replica_candidates(replica_candidates);
    let created = store
        .create_manifest(&manifest)
        .map_err(|source| DaemonError::Parse {
            detail: format!("create sandbox SIFS state: {source}"),
        })?;
    manifest = created;
    ensure_daemon_sandbox_mutation_lease(&lifecycle_lease, &manifest.sandbox_id, "after create")?;
    let transaction_id = manifest
        .pending_reconciliation
        .as_ref()
        .ok_or_else(|| DaemonError::Parse {
            detail: format!(
                "sandbox {} create lost its durable fence",
                manifest.sandbox_id
            ),
        })?
        .transaction_id
        .clone();
    append_daemon_sandbox_event_durable(
        paths,
        sinomo_sandbox::sandbox_instance_created_event(execution_node_id.clone(), &manifest),
    )
    .map_err(|source| DaemonError::Parse {
        detail: format!("append sandbox created event: {source}"),
    })?;
    append_daemon_sandbox_event_durable(
        paths,
        sinomo_sandbox::sandbox_toolchain_pinned_event(execution_node_id.clone(), &manifest),
    )
    .map_err(|source| DaemonError::Parse {
        detail: format!("append sandbox toolchain pinned event: {source}"),
    })?;
    for mount in &manifest.mounts {
        append_daemon_sandbox_event_durable(
            paths,
            sinomo_sandbox::sandbox_mount_added_event(execution_node_id.clone(), &manifest, mount),
        )
        .map_err(|source| DaemonError::Parse {
            detail: format!("append sandbox mount added event: {source}"),
        })?;
    }
    ensure_daemon_sandbox_mutation_lease(
        &lifecycle_lease,
        &manifest.sandbox_id,
        "after durable create audit",
    )?;
    manifest = store.clear_reconciliation_transaction(
        &manifest.sandbox_id,
        &transaction_id,
        OffsetDateTime::now_utc(),
    )?;
    best_effort_clear_daemon_sandbox_authoritative_recovery_pending(
        paths,
        &manifest.sandbox_id,
        "sandbox create committed",
    );
    Ok(sinomo_sandbox::SandboxCreateInstanceResponse { manifest })
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
    let dfs = open_daemon_sandbox_dfs_service_on_runtime(paths, runtime)?;
    let mut store = sinomo_sandbox::SifsSandboxInstanceStore::new(dfs);
    let manifest = store.load(&request.sandbox_id)?;
    Ok(sinomo_sandbox::SandboxStatusResponse { manifest })
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
    let sinomo_sandbox::SandboxUpdateInstanceRequest {
        sandbox_id,
        cwd,
        mounts,
        env_set,
        env_unset,
    } = request;
    let lifecycle_lease = acquire_daemon_sandbox_mutation_lease(paths, &sandbox_id, runtime)?;
    let config = load_daemon_sandbox_policy_config(paths)?;
    let dfs = open_daemon_sandbox_dfs_service_on_runtime(paths, runtime)?;
    let node_id = dfs.node_id().clone();
    let replica_candidates = daemon_sandbox_replica_candidates(paths, &node_id);
    let mut store = sinomo_sandbox::SifsSandboxInstanceStore::new(dfs)
        .with_replica_candidates(replica_candidates.clone());
    let mut manifest = store.load(&sandbox_id)?;
    let cwd_changed = cwd.is_some();
    let mut env_set_keys = env_set.keys().cloned().collect::<Vec<_>>();
    let mut env_unset_keys = env_unset.clone();
    for key in env_set_keys.iter().chain(env_unset_keys.iter()) {
        if !sinomo_sandbox::sandbox_env_name_is_valid(key) {
            return Err(DaemonError::Parse {
                detail: format!("invalid sandbox env name `{key}`"),
            });
        }
    }
    for (key, value) in &env_set {
        if value.contains('\0') {
            return Err(DaemonError::Parse {
                detail: format!("sandbox env `{key}` contains an embedded NUL byte"),
            });
        }
    }
    env_set_keys.sort();
    env_unset_keys.sort();
    env_unset_keys.dedup();
    let mounts_reconciliation =
        sinomo_sandbox::reconcile_sandbox_mounts(&mut manifest.mounts, mounts);
    if let Some(cwd) = cwd {
        manifest.cwd = cwd;
    }
    for key in env_unset {
        manifest.env.remove(&key);
    }
    manifest.env.extend(env_set);
    validate_daemon_sandbox_manifest_policy(
        paths,
        &config.policy,
        &node_id,
        &manifest,
        "sandbox-update",
    )?;
    if mounts_reconciliation.replacement_requested {
        let mut dfs = store.into_inner();
        validate_daemon_sandbox_resolved_mount_policy(
            paths,
            &config.policy,
            &mut dfs,
            &manifest,
            &node_id,
            "sandbox-update-resolved-source",
        )?;
        store = sinomo_sandbox::SifsSandboxInstanceStore::new(dfs)
            .with_replica_candidates(replica_candidates.clone());
    }
    if cwd_changed || mounts_reconciliation.replacement_requested {
        let fs = sinomo_sandbox::SifsSandboxFileSystem::new(store.into_inner(), &manifest);
        sinomo_sandbox::validate_sandbox_working_directory(&fs, &manifest.cwd)?;
        store = sinomo_sandbox::SifsSandboxInstanceStore::new(fs.into_inner())
            .with_replica_candidates(replica_candidates);
    }
    manifest
        .advance_generation()
        .map_err(|source| DaemonError::Parse {
            detail: source.to_string(),
        })?;
    manifest.updated_at = OffsetDateTime::now_utc();
    ensure_daemon_sandbox_mutation_lease(&lifecycle_lease, &sandbox_id, "before update")?;
    store.save_manifest(&manifest)?;
    ensure_daemon_sandbox_mutation_lease(&lifecycle_lease, &sandbox_id, "after update")?;
    append_daemon_sandbox_event(
        paths,
        sinomo_sandbox::sandbox_instance_updated_event(
            node_id.clone(),
            &manifest,
            cwd_changed,
            &env_set_keys,
            &env_unset_keys,
            mounts_reconciliation.replacement_requested,
            mounts_reconciliation.changed,
            &mounts_reconciliation.added,
            &mounts_reconciliation.removed,
        ),
    )?;
    for mount in &mounts_reconciliation.removed {
        append_daemon_sandbox_event(
            paths,
            sinomo_sandbox::sandbox_mount_removed_event(node_id.clone(), &manifest, mount),
        )?;
    }
    for mount in &mounts_reconciliation.added {
        append_daemon_sandbox_event(
            paths,
            sinomo_sandbox::sandbox_mount_added_event(node_id.clone(), &manifest, mount),
        )?;
    }
    ensure_daemon_sandbox_mutation_lease(&lifecycle_lease, &sandbox_id, "after update audit")?;
    Ok(sinomo_sandbox::SandboxUpdateInstanceResponse { manifest })
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
    let lifecycle_lease =
        acquire_daemon_sandbox_mutation_lease(paths, &request.sandbox_id, runtime)?;
    let config = load_daemon_sandbox_policy_config(paths)?;
    let dfs = open_daemon_sandbox_dfs_service_on_runtime(paths, runtime)?;
    let node_id = dfs.node_id().clone();
    let replica_candidates = daemon_sandbox_replica_candidates(paths, &node_id);
    let mut store = sinomo_sandbox::SifsSandboxInstanceStore::new(dfs)
        .with_replica_candidates(replica_candidates);
    let manifest = store.load(&request.sandbox_id)?;
    validate_daemon_sandbox_manifest_policy(
        paths,
        &config.policy,
        &node_id,
        &manifest,
        "sandbox-checkpoint",
    )?;
    ensure_daemon_sandbox_mutation_lease(
        &lifecycle_lease,
        &request.sandbox_id,
        "before checkpoint",
    )?;
    let cancellation = lifecycle_lease.cancellation_token();
    let checkpoint = store.create_checkpoint_with_checkpoints(
        &request.sandbox_id,
        OffsetDateTime::now_utc(),
        || {
            daemon_sandbox_store_mutation_checkpoint(
                &lifecycle_lease,
                &cancellation,
                &request.sandbox_id,
                "during checkpoint",
            )
        },
    )?;
    ensure_daemon_sandbox_mutation_lease(
        &lifecycle_lease,
        &request.sandbox_id,
        "after checkpoint",
    )?;
    let manifest = store.load(&request.sandbox_id)?;
    append_daemon_sandbox_event(
        paths,
        sinomo_sandbox::sandbox_instance_checkpointed_event(node_id, &manifest, &checkpoint),
    )?;
    Ok(sinomo_sandbox::SandboxCheckpointResponse { checkpoint })
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
    let lifecycle_lease =
        acquire_daemon_sandbox_transaction_lease(paths, &request.sandbox_id, runtime)?;
    let config = load_daemon_sandbox_policy_config(paths)?;
    let dfs = open_daemon_sandbox_dfs_service_on_runtime(paths, runtime)?;
    let node_id = dfs.node_id().clone();
    let replica_candidates = daemon_sandbox_replica_candidates(paths, &node_id);
    let mut store = sinomo_sandbox::SifsSandboxInstanceStore::new(dfs)
        .with_replica_candidates(replica_candidates);
    let current = store.load_allow_pending_reconciliation(&request.sandbox_id)?;
    if let Some(pending) = current.pending_reconciliation.as_ref()
        && !matches!(
            &pending.operation,
            sinomo_sandbox::SandboxAuthoritativeMutationOperation::ResumeCheckpoint {
                checkpoint_id
            } if checkpoint_id == &request.checkpoint_id
        )
    {
        return Err(sinomo_sandbox::SandboxStoreError::ReconciliationPending {
            sandbox_id: request.sandbox_id.clone(),
            transaction_id: pending.transaction_id.clone(),
        }
        .into());
    }
    validate_daemon_sandbox_manifest_policy(
        paths,
        &config.policy,
        &node_id,
        &current,
        "sandbox-resume",
    )?;
    ensure_daemon_sandbox_mutation_lease(&lifecycle_lease, &request.sandbox_id, "before resume")?;
    let cancellation = lifecycle_lease.cancellation_token();
    let pending_manifest = store.resume_checkpoint_with_checkpoints(
        &request.sandbox_id,
        &request.checkpoint_id,
        OffsetDateTime::now_utc(),
        || {
            daemon_sandbox_store_mutation_checkpoint(
                &lifecycle_lease,
                &cancellation,
                &request.sandbox_id,
                "during resume",
            )
        },
    )?;
    ensure_daemon_sandbox_mutation_lease(&lifecycle_lease, &request.sandbox_id, "after resume")?;
    let transaction_id = pending_manifest
        .pending_reconciliation
        .as_ref()
        .ok_or_else(|| DaemonError::Parse {
            detail: format!(
                "sandbox {} resume committed without an authoritative transaction fence",
                request.sandbox_id
            ),
        })?
        .transaction_id
        .clone();
    append_daemon_sandbox_event_durable(
        paths,
        sinomo_sandbox::sandbox_instance_resumed_event(
            node_id,
            &pending_manifest,
            &request.checkpoint_id,
        ),
    )?;
    ensure_daemon_sandbox_mutation_lease(
        &lifecycle_lease,
        &request.sandbox_id,
        "after durable resume event",
    )?;
    let manifest = store.clear_reconciliation_transaction(
        &request.sandbox_id,
        &transaction_id,
        OffsetDateTime::now_utc(),
    )?;
    best_effort_clear_daemon_sandbox_authoritative_recovery_pending(
        paths,
        &request.sandbox_id,
        "sandbox resume committed",
    );
    Ok(sinomo_sandbox::SandboxResumeResponse { manifest })
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
    let lifecycle_lease =
        acquire_daemon_sandbox_transaction_lease(paths, &request.sandbox_id, runtime)?;
    let config = load_daemon_sandbox_policy_config(paths)?;
    let dfs = open_daemon_sandbox_dfs_service_on_runtime(paths, runtime)?;
    let node_id = dfs.node_id().clone();
    let replica_candidates = daemon_sandbox_replica_candidates(paths, &node_id);
    let mut store = sinomo_sandbox::SifsSandboxInstanceStore::new(dfs)
        .with_replica_candidates(replica_candidates);
    let manifest = store.load_allow_pending_reconciliation(&request.sandbox_id)?;
    if let Some(pending) = manifest.pending_reconciliation.as_ref()
        && !matches!(
            pending.operation,
            sinomo_sandbox::SandboxAuthoritativeMutationOperation::Reboot { .. }
        )
    {
        return Err(sinomo_sandbox::SandboxStoreError::ReconciliationPending {
            sandbox_id: request.sandbox_id.clone(),
            transaction_id: pending.transaction_id.clone(),
        }
        .into());
    }
    validate_daemon_sandbox_manifest_policy(
        paths,
        &config.policy,
        &node_id,
        &manifest,
        "sandbox-reboot",
    )?;
    ensure_daemon_sandbox_mutation_lease(&lifecycle_lease, &request.sandbox_id, "before reboot")?;
    let cancellation = lifecycle_lease.cancellation_token();
    let (pending_manifest, cleared_tmp) = store.reboot_session_with_checkpoints(
        &request.sandbox_id,
        OffsetDateTime::now_utc(),
        || {
            daemon_sandbox_store_mutation_checkpoint(
                &lifecycle_lease,
                &cancellation,
                &request.sandbox_id,
                "during reboot",
            )
        },
    )?;
    ensure_daemon_sandbox_mutation_lease(&lifecycle_lease, &request.sandbox_id, "after reboot")?;
    let transaction_id = pending_manifest
        .pending_reconciliation
        .as_ref()
        .ok_or_else(|| DaemonError::Parse {
            detail: format!(
                "sandbox {} reboot committed without an authoritative transaction fence",
                request.sandbox_id
            ),
        })?
        .transaction_id
        .clone();
    append_daemon_sandbox_event_durable(
        paths,
        sinomo_sandbox::sandbox_instance_rebooted_event(node_id, &pending_manifest, cleared_tmp),
    )?;
    ensure_daemon_sandbox_mutation_lease(
        &lifecycle_lease,
        &request.sandbox_id,
        "after durable reboot event",
    )?;
    let manifest = store.clear_reconciliation_transaction(
        &request.sandbox_id,
        &transaction_id,
        OffsetDateTime::now_utc(),
    )?;
    best_effort_clear_daemon_sandbox_authoritative_recovery_pending(
        paths,
        &request.sandbox_id,
        "sandbox reboot committed",
    );
    Ok(sinomo_sandbox::SandboxRebootResponse {
        manifest,
        cleared_tmp,
    })
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
    let lifecycle_lease =
        acquire_daemon_sandbox_transaction_lease(paths, &request.sandbox_id, runtime)?;
    let dfs = open_daemon_sandbox_dfs_service_on_runtime(paths, runtime)?;
    let node_id = dfs.node_id().clone();
    let replica_candidates = daemon_sandbox_replica_candidates(paths, &node_id);
    let mut store = sinomo_sandbox::SifsSandboxInstanceStore::new(dfs)
        .with_replica_candidates(replica_candidates);
    ensure_daemon_sandbox_mutation_lease(&lifecycle_lease, &request.sandbox_id, "before destroy")?;
    let pending_manifest = store.prepare_destroy(&request.sandbox_id, OffsetDateTime::now_utc())?;
    let transaction_id = pending_manifest
        .pending_reconciliation
        .as_ref()
        .expect("destroy fence")
        .transaction_id
        .clone();
    let cancellation = lifecycle_lease.cancellation_token();
    store.apply_destroy_with_checkpoints(&request.sandbox_id, &transaction_id, || {
        daemon_sandbox_store_mutation_checkpoint(
            &lifecycle_lease,
            &cancellation,
            &request.sandbox_id,
            "during destroy",
        )
    })?;
    ensure_daemon_sandbox_mutation_lease(&lifecycle_lease, &request.sandbox_id, "after destroy")?;
    append_daemon_sandbox_event_durable(
        paths,
        sinomo_sandbox::sandbox_instance_destroyed_transaction_event(node_id, &pending_manifest),
    )?;
    ensure_daemon_sandbox_mutation_lease(
        &lifecycle_lease,
        &request.sandbox_id,
        "after durable destroy event",
    )?;
    store.finalize_destroy_with_checkpoints(&request.sandbox_id, &transaction_id, || {
        daemon_sandbox_store_mutation_checkpoint(
            &lifecycle_lease,
            &cancellation,
            &request.sandbox_id,
            "during destroy finalization",
        )
    })?;
    best_effort_clear_daemon_sandbox_authoritative_recovery_pending(
        paths,
        &request.sandbox_id,
        "sandbox destroy committed",
    );
    Ok(sinomo_sandbox::SandboxDestroyResponse {
        sandbox_id: request.sandbox_id,
    })
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
    let dfs = open_daemon_sandbox_dfs_service_on_runtime(paths, runtime)?;
    let mut store = sinomo_sandbox::SifsSandboxInstanceStore::new(dfs);
    let manifest = store.load(&request.sandbox_id)?;
    let fs = sinomo_sandbox::SifsSandboxFileSystem::new(store.into_inner(), &manifest);
    let entries = fs.list(&request.path)?;
    Ok(sinomo_sandbox::SandboxListResponse { entries })
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
    let dfs = open_daemon_sandbox_dfs_service_on_runtime(paths, runtime)?;
    let mut store = sinomo_sandbox::SifsSandboxInstanceStore::new(dfs);
    let manifest = store.load(&request.sandbox_id)?;
    let fs = sinomo_sandbox::SifsSandboxFileSystem::new(store.into_inner(), &manifest);
    let bytes = fs.read(&request.path, request.range)?;
    Ok(sinomo_sandbox::SandboxReadResponse { bytes })
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
    let lifecycle_lease =
        acquire_daemon_sandbox_mutation_lease(paths, &request.sandbox_id, runtime)?;
    let config = load_daemon_sandbox_policy_config(paths)?;
    let dfs = open_daemon_sandbox_dfs_service_on_runtime(paths, runtime)?;
    let node_id = dfs.node_id().clone();
    let replica_candidates = daemon_sandbox_replica_candidates(paths, &node_id);
    let mut store = sinomo_sandbox::SifsSandboxInstanceStore::new(dfs)
        .with_replica_candidates(replica_candidates.clone());
    let manifest = store.load(&request.sandbox_id)?;
    validate_daemon_sandbox_manifest_policy(
        paths,
        &config.policy,
        &node_id,
        &manifest,
        "sandbox-write",
    )?;
    let fs = sinomo_sandbox::SifsSandboxFileSystem::new(store.into_inner(), &manifest)
        .with_replica_candidates(replica_candidates.clone());
    if let Err(error) = sinomo_sandbox::validate_sandbox_write_resource_limits(
        &fs,
        &request.path,
        request.bytes.len() as u64,
        &manifest.limits,
    ) {
        if let sinomo_sandbox::SandboxFsError::QuotaExceeded {
            kind,
            actual,
            limit,
        } = &error
        {
            append_daemon_sandbox_event(
                paths,
                sinomo_sandbox::sandbox_resource_exhausted_event(
                    node_id.clone(),
                    &request.sandbox_id,
                    "sandbox-write",
                    kind.clone(),
                    *actual,
                    *limit,
                ),
            )?;
        }
        append_daemon_sandbox_write_denial_event(
            paths,
            &node_id,
            &request.sandbox_id,
            &request.path,
            &error,
        )?;
        return Err(error.into());
    }
    let mut manifest = manifest;
    manifest
        .advance_generation()
        .map_err(|source| DaemonError::Parse {
            detail: source.to_string(),
        })?;
    manifest.updated_at = OffsetDateTime::now_utc();
    let mut store = sinomo_sandbox::SifsSandboxInstanceStore::new(fs.into_inner())
        .with_replica_candidates(replica_candidates.clone());
    ensure_daemon_sandbox_mutation_lease(
        &lifecycle_lease,
        &request.sandbox_id,
        "before write generation fence",
    )?;
    store.save_manifest(&manifest)?;
    ensure_daemon_sandbox_mutation_lease(
        &lifecycle_lease,
        &request.sandbox_id,
        "after write generation fence",
    )?;
    let mut fs = sinomo_sandbox::SifsSandboxFileSystem::new(store.into_inner(), &manifest)
        .with_replica_candidates(replica_candidates);
    let write_result = fs.write(&request.path, &request.bytes);
    ensure_daemon_sandbox_mutation_lease(
        &lifecycle_lease,
        &request.sandbox_id,
        "after write workspace mutation",
    )?;
    if let Err(error) = write_result {
        append_daemon_sandbox_write_denial_event(
            paths,
            &node_id,
            &request.sandbox_id,
            &request.path,
            &error,
        )?;
        return Err(error.into());
    }
    if !sinomo_sandbox::sandbox_path_is_virtual_device_sink(&request.path) {
        append_daemon_sandbox_event(
            paths,
            sinomo_sandbox::sandbox_file_written_event(
                node_id,
                &request.sandbox_id,
                &request.path,
                request.bytes.len() as u64,
            ),
        )?;
    }
    Ok(sinomo_sandbox::SandboxWriteResponse {
        path: request.path,
        bytes_written: request.bytes.len() as u64,
    })
}

fn append_daemon_sandbox_write_denial_event(
    paths: &DaemonPaths,
    node_id: &sinomo_core::node::NodeId,
    sandbox_id: &sinomo_sandbox::SandboxId,
    request_path: &sinomo_sandbox::SandboxPath,
    error: &sinomo_sandbox::SandboxFsError,
) -> Result<(), DaemonError> {
    let sinomo_sandbox::SandboxFsError::ReadOnly { path } = error else {
        return Ok(());
    };
    let denial = sinomo_sandbox::SandboxPolicyDenial::new(
        "mount-read-only-denied",
        format!("write to read-only sandbox path {path} denied for request path {request_path}"),
    );
    append_daemon_sandbox_event(
        paths,
        sinomo_sandbox::sandbox_policy_denied_event(
            node_id.clone(),
            Some(sandbox_id),
            "sandbox-write",
            &denial,
        ),
    )?;
    append_daemon_sandbox_event(
        paths,
        sinomo_sandbox::sandbox_mount_denied_event(
            node_id.clone(),
            Some(sandbox_id),
            "sandbox-write",
            &denial,
        ),
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
    let config = load_daemon_sandbox_policy_config(paths)?;
    let dfs = open_daemon_sandbox_dfs_service_on_runtime(paths, runtime)?;
    let node_id = dfs.node_id().clone();
    let replica_candidates = daemon_sandbox_replica_candidates(paths, &node_id);
    let mut store = sinomo_sandbox::SifsSandboxToolchainStore::new(dfs)
        .with_replica_candidates(replica_candidates);
    let spec = sinomo_sandbox::SandboxBusyBoxToolchainSpec::new(
        request.version,
        request.provenance,
        OffsetDateTime::now_utc(),
    )?;
    let module_hash = sinomo_sandbox::sandbox_module_hash(&request.module_bytes);
    let candidate = spec.manifest(&request.module_bytes)?;
    validate_daemon_sandbox_toolchain_update_policy(
        paths,
        &config.policy,
        &node_id,
        &candidate,
        "sandbox-toolchain-import-busybox",
    )?;
    let manifest = sinomo_sandbox::install_busybox_toolchain_to_sifs_store(
        &mut store,
        &spec,
        &request.module_bytes,
    )?;
    let origin_node_id = store.into_inner().node_id().clone();
    append_daemon_sandbox_event(
        paths,
        sinomo_sandbox::sandbox_toolchain_updated_event(
            origin_node_id,
            &manifest,
            module_hash.clone(),
        ),
    )?;
    Ok(sinomo_sandbox::SandboxImportBusyBoxToolchainResponse {
        manifest,
        module_hash,
    })
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
    let config = load_daemon_sandbox_policy_config(paths)?;
    let dfs = open_daemon_sandbox_dfs_service_on_runtime(paths, runtime)?;
    let node_id = dfs.node_id().clone();
    let replica_candidates = daemon_sandbox_replica_candidates(paths, &node_id);
    let mut store = sinomo_sandbox::SifsSandboxToolchainStore::new(dfs)
        .with_replica_candidates(replica_candidates);
    let mut manifest = match store.load_manifest(&request.toolchain_id) {
        Ok(manifest) => manifest,
        Err(sinomo_sandbox::SandboxToolchainError::NotFound { .. }) => {
            sinomo_sandbox::SandboxToolchainManifest::new(
                request.toolchain_id.clone(),
                request.version,
                request.target_abi.clone(),
                request.provenance,
                OffsetDateTime::now_utc(),
            )?
        }
        Err(error) => return Err(error.into()),
    };
    let module_hash = sinomo_sandbox::sandbox_module_hash(&request.module_bytes);
    let mut candidate = manifest.clone();
    let mut command = sinomo_sandbox::SandboxToolchainCommand::new(
        &candidate.toolchain_id,
        &request.command_name,
        &request.module_bytes,
        request.default_limits.clone(),
    )?;
    command.target_abi = Some(request.target_abi.clone());
    command.required_capabilities = request.required_capabilities.clone();
    candidate.insert_command(command)?;
    validate_daemon_sandbox_toolchain_update_policy(
        paths,
        &config.policy,
        &node_id,
        &candidate,
        "sandbox-toolchain-add-command",
    )?;
    store.put_command_module_with_target_abi_capabilities_and_argv_prefix(
        &mut manifest,
        &request.command_name,
        &request.module_bytes,
        request.default_limits,
        Some(request.target_abi),
        request.required_capabilities,
        Vec::new(),
        BTreeMap::new(),
    )?;
    let origin_node_id = store.into_inner().node_id().clone();
    append_daemon_sandbox_event(
        paths,
        sinomo_sandbox::sandbox_toolchain_updated_event(
            origin_node_id,
            &manifest,
            module_hash.clone(),
        ),
    )?;
    Ok(sinomo_sandbox::SandboxAddToolchainCommandResponse {
        manifest,
        module_hash,
    })
}

pub(crate) fn handle_daemon_sandbox_toolchain_import_package_from_path_on_runtime(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxImportToolchainPackageFromPathRequest,
    runtime: Option<&tokio::runtime::Handle>,
) -> Result<sinomo_sandbox::SandboxImportToolchainPackageResponse, DaemonError> {
    let package_path = PathBuf::from(&request.package_path);
    let package_dir = package_path
        .parent()
        .ok_or_else(|| DaemonError::Parse {
            detail: format!("package path has no parent: {}", package_path.display()),
        })?
        .to_path_buf();
    let spec_bytes = std::fs::read(&package_path).map_err(|source| DaemonError::Io {
        path: package_path.display().to_string(),
        detail: source.to_string(),
    })?;
    let spec: sinomo_sandbox::SandboxToolchainPackageSpec = serde_json::from_slice(&spec_bytes)
        .map_err(|source| DaemonError::Parse {
            detail: format!(
                "failed to parse package manifest {}: {source}",
                package_path.display()
            ),
        })?;
    let mut commands = Vec::new();
    for command in &spec.commands {
        let module_path = sandbox_package_file_path(&package_dir, &command.module)?;
        let module_bytes = std::fs::read(&module_path).map_err(|source| DaemonError::Io {
            path: module_path.display().to_string(),
            detail: source.to_string(),
        })?;
        verify_sandbox_package_hash(&module_path, &module_bytes, &command.content_hash)?;
        commands.push(sinomo_sandbox::SandboxImportToolchainPackageCommand {
            name: command.name.clone(),
            module_bytes,
            target_abi: command.target_abi.clone(),
            argv_prefix: command.argv_prefix.clone(),
            exec_name: command.exec_name.clone(),
            default_env: command.default_env.clone(),
            required_capabilities: command.required_capabilities.clone(),
            default_limits: command.default_limits.clone(),
        });
    }
    let mut artifacts = Vec::new();
    for artifact in &spec.artifacts {
        let artifact_path = sandbox_package_file_path(&package_dir, &artifact.file)?;
        let bytes = std::fs::read(&artifact_path).map_err(|source| DaemonError::Io {
            path: artifact_path.display().to_string(),
            detail: source.to_string(),
        })?;
        verify_sandbox_package_hash(&artifact_path, &bytes, &artifact.content_hash)?;
        artifacts.push(sinomo_sandbox::SandboxImportToolchainPackageArtifact {
            relative_path: artifact.relative_path.clone(),
            bytes,
        });
    }
    handle_daemon_sandbox_toolchain_import_package_on_runtime(
        paths,
        sinomo_sandbox::SandboxImportToolchainPackageRequest {
            toolchain_id: request.toolchain_id.unwrap_or(spec.toolchain_id),
            version: spec.version,
            target_abi: spec.target_abi,
            provenance: spec.provenance,
            commands,
            artifacts,
        },
        runtime,
    )
}

fn sandbox_package_file_path(
    package_dir: &Path,
    relative_path: &str,
) -> Result<PathBuf, DaemonError> {
    let path = Path::new(relative_path);
    if path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(DaemonError::Parse {
            detail: format!("invalid package-relative path `{relative_path}`"),
        });
    }
    Ok(package_dir.join(path))
}

fn verify_sandbox_package_hash(
    path: &Path,
    bytes: &[u8],
    expected: &str,
) -> Result<(), DaemonError> {
    let actual = sinomo_sandbox::sandbox_module_hash(bytes);
    if actual != expected {
        return Err(DaemonError::Parse {
            detail: format!(
                "package file {} hash mismatch: expected {expected}, actual {actual}",
                path.display()
            ),
        });
    }
    Ok(())
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
    let config = load_daemon_sandbox_policy_config(paths)?;
    let dfs = open_daemon_sandbox_dfs_service_on_runtime(paths, runtime)?;
    let node_id = dfs.node_id().clone();
    let replica_candidates = daemon_sandbox_replica_candidates(paths, &node_id);
    let mut store = sinomo_sandbox::SifsSandboxToolchainStore::new(dfs)
        .with_replica_candidates(replica_candidates);
    let mut manifest = match store.load_manifest(&request.toolchain_id) {
        Ok(manifest) => manifest,
        Err(sinomo_sandbox::SandboxToolchainError::NotFound { .. }) => {
            sinomo_sandbox::SandboxToolchainManifest::new(
                request.toolchain_id.clone(),
                request.version,
                request.target_abi.clone(),
                request.provenance,
                OffsetDateTime::now_utc(),
            )?
        }
        Err(error) => return Err(error.into()),
    };
    for command in &request.commands {
        if manifest.commands.contains_key(&command.name) {
            return Err(sinomo_sandbox::SandboxToolchainError::DuplicateCommand {
                name: command.name.clone(),
            }
            .into());
        }
    }
    for artifact in &request.artifacts {
        if manifest.artifacts.contains_key(&artifact.relative_path) {
            return Err(sinomo_sandbox::SandboxToolchainError::DuplicateArtifact {
                path: artifact.relative_path.clone(),
            }
            .into());
        }
    }
    let mut candidate = manifest.clone();
    for command in &request.commands {
        let mut candidate_command = sinomo_sandbox::SandboxToolchainCommand::new(
            &candidate.toolchain_id,
            &command.name,
            &command.module_bytes,
            command.default_limits.clone(),
        )?;
        candidate_command.target_abi = Some(
            command
                .target_abi
                .clone()
                .unwrap_or_else(|| request.target_abi.clone()),
        );
        candidate_command.required_capabilities = command.required_capabilities.clone();
        candidate_command.argv_prefix = command.argv_prefix.clone();
        candidate_command.exec_name = command.exec_name.clone();
        candidate_command.default_env = command.default_env.clone();
        candidate.insert_command(candidate_command)?;
    }
    for artifact in &request.artifacts {
        let candidate_artifact = sinomo_sandbox::SandboxToolchainArtifact::new(
            &candidate.toolchain_id,
            &artifact.relative_path,
            &artifact.bytes,
        )?;
        candidate.insert_artifact(candidate_artifact)?;
    }
    let artifact_bundle_bytes = if request.artifacts.is_empty() {
        None
    } else {
        Some(sinomo_sandbox::pack_toolchain_artifact_bundle(
            &request
                .artifacts
                .iter()
                .map(|artifact| (artifact.relative_path.clone(), artifact.bytes.clone()))
                .collect::<Vec<_>>(),
        )?)
    };
    let artifact_bundle = if let Some(bytes) = &artifact_bundle_bytes {
        let bundle_hash = sinomo_sandbox::sandbox_module_hash(bytes);
        let bundle_name = format!(
            "package-{}",
            bundle_hash
                .strip_prefix("blake3:")
                .unwrap_or(bundle_hash.as_str())
        );
        let mut bundled_artifacts = Vec::new();
        for artifact in &request.artifacts {
            bundled_artifacts.push(candidate.artifact(&artifact.relative_path)?.clone());
        }
        let bundle = sinomo_sandbox::SandboxToolchainArtifactBundle::new(
            &candidate.toolchain_id,
            &bundle_name,
            bytes,
            bundled_artifacts,
        )?;
        candidate.insert_artifact_bundle(bundle.clone())?;
        Some(bundle)
    } else {
        None
    };
    validate_daemon_sandbox_toolchain_update_policy(
        paths,
        &config.policy,
        &node_id,
        &candidate,
        "sandbox-toolchain-import-package",
    )?;
    let mut package_hasher = sinomo_sandbox::SandboxModuleHasher::new();
    for command in &request.commands {
        package_hasher.update(command.name.as_bytes());
        if let Some(target_abi) = &command.target_abi {
            package_hasher.update(format!("{target_abi:?}").as_bytes());
        }
        for arg in &command.argv_prefix {
            package_hasher.update(arg.as_bytes());
        }
        if let Some(exec_name) = &command.exec_name {
            package_hasher.update(exec_name.as_bytes());
        }
        for (key, value) in &command.default_env {
            package_hasher.update(key.as_bytes());
            package_hasher.update(value.as_bytes());
        }
        package_hasher.update(&command.module_bytes);
        let manifest_command = candidate.command(&command.name)?;
        store.write_command_module_file(manifest_command, &command.module_bytes)?;
    }
    for artifact in &request.artifacts {
        package_hasher.update(artifact.relative_path.as_bytes());
        package_hasher.update(&artifact.bytes);
    }
    if let (Some(bundle), Some(bytes)) = (&artifact_bundle, &artifact_bundle_bytes) {
        store.write_artifact_bundle_file(bundle, bytes)?;
    }
    store.save_manifest(&candidate)?;
    manifest = candidate;
    let package_hash = package_hasher.finalize();
    let origin_node_id = store.into_inner().node_id().clone();
    append_daemon_sandbox_event(
        paths,
        sinomo_sandbox::sandbox_toolchain_updated_event(
            origin_node_id,
            &manifest,
            package_hash.clone(),
        ),
    )?;
    Ok(sinomo_sandbox::SandboxImportToolchainPackageResponse {
        manifest,
        package_hash,
    })
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
    let config = load_daemon_sandbox_policy_config(paths)?;
    let dfs = open_daemon_sandbox_dfs_service_on_runtime(paths, runtime)?;
    let node_id = dfs.node_id().clone();
    let replica_candidates = daemon_sandbox_replica_candidates(paths, &node_id);
    let mut store = sinomo_sandbox::SifsSandboxToolchainStore::new(dfs)
        .with_replica_candidates(replica_candidates);
    let mut manifest = match store.load_manifest(&request.toolchain_id) {
        Ok(manifest) => manifest,
        Err(sinomo_sandbox::SandboxToolchainError::NotFound { .. }) => {
            sinomo_sandbox::SandboxToolchainManifest::new(
                request.toolchain_id.clone(),
                request.version,
                sinomo_sandbox::SandboxWasiAbi::Wasix,
                request.provenance,
                OffsetDateTime::now_utc(),
            )?
        }
        Err(error) => return Err(error.into()),
    };
    let artifact_hash = sinomo_sandbox::sandbox_module_hash(&request.bytes);
    let mut candidate = manifest.clone();
    let candidate_artifact = sinomo_sandbox::SandboxToolchainArtifact::new(
        &candidate.toolchain_id,
        &request.relative_path,
        &request.bytes,
    )?;
    candidate.insert_artifact(candidate_artifact)?;
    validate_daemon_sandbox_toolchain_update_policy(
        paths,
        &config.policy,
        &node_id,
        &candidate,
        "sandbox-toolchain-add-artifact",
    )?;
    store.put_artifact(&mut manifest, &request.relative_path, &request.bytes)?;
    let origin_node_id = store.into_inner().node_id().clone();
    append_daemon_sandbox_event(
        paths,
        sinomo_sandbox::sandbox_toolchain_updated_event(
            origin_node_id,
            &manifest,
            artifact_hash.clone(),
        ),
    )?;
    Ok(sinomo_sandbox::SandboxAddToolchainArtifactResponse {
        manifest,
        artifact_hash,
    })
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
/// without the `sandbox` feature, so the build-heavy native WASIX module
/// backend is not linked in.
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
        detail: "daemon-hosted dynamic SIFS WASIX process plans were removed; execute through direct-SIFS WASIX or the live DFS/FUSE simple sandbox".to_string(),
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
        detail: "daemon-hosted dynamic SIFS WASIX commands were removed; execute through direct-SIFS WASIX or the live DFS/FUSE simple sandbox".to_string(),
    })
}

#[cfg(feature = "sandbox")]
fn run_daemon_sandbox_lifecycle_maintenance_scan(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
    marked_sandbox_ids: BTreeSet<sinomo_sandbox::SandboxId>,
) -> Result<usize, DaemonError> {
    let dfs = open_daemon_sandbox_dfs_service_on_runtime(paths, Some(runtime))?;
    let mut authoritative_store = sinomo_sandbox::SifsSandboxInstanceStore::new(dfs);
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
        mark_daemon_sandbox_authoritative_recovery_pending(paths, sandbox_id)?;
        candidate_sandbox_ids.insert(sandbox_id.clone());
    }

    let mut recovered = 0_usize;
    for sandbox_id in candidate_sandbox_ids {
        let result = (|| -> Result<(), DaemonError> {
            let raw_lease =
                acquire_daemon_sandbox_lifecycle_lease(paths, &sandbox_id, Some(runtime))?;
            let cancellation = sinomo_sandbox::SandboxCancellationToken::new();
            let lifecycle_lease = DaemonSandboxExecutionLease::start(raw_lease, cancellation)?;
            recover_daemon_authoritative_sandbox_transaction(
                paths,
                &sandbox_id,
                Some(runtime),
                &lifecycle_lease,
            )
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
    let marked_sandbox_ids = daemon_sandbox_authoritative_recovery_pending_ids(paths)?;
    let safety_sweep_due =
        daemon_sandbox_lifecycle_safety_sweep_due_or_initialize(paths, now_unix_nanos)?;
    if marked_sandbox_ids.is_empty() && !safety_sweep_due {
        return Ok(0);
    }

    let recovered = scan(marked_sandbox_ids)?;
    record_daemon_sandbox_lifecycle_safety_sweep(paths, now_unix_nanos)?;
    Ok(recovered)
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
                sent_output_chunks.insert(sandbox_output_chunk_stream_key(&chunk));
                sinomo_sandbox::SandboxRunToolchainStreamFrame::Output { chunk }
            }
            StreamMessage::Complete {
                sandbox_id,
                result: Ok(outcome),
            } => {
                let mut completion_output_chunks = outcome.output_chunks.clone();
                if completion_output_chunks.is_empty() {
                    completion_output_chunks.extend(sandbox_output_chunks_from_bytes(
                        &outcome.response.command_id,
                        &sandbox_id,
                        sinomo_sandbox::SandboxOutputStream::Stdout,
                        &outcome.stdout,
                    ));
                    completion_output_chunks.extend(sandbox_output_chunks_from_bytes(
                        &outcome.response.command_id,
                        &sandbox_id,
                        sinomo_sandbox::SandboxOutputStream::Stderr,
                        &outcome.stderr,
                    ));
                }
                for chunk in &completion_output_chunks {
                    if sent_output_chunks.insert(sandbox_output_chunk_stream_key(chunk)) {
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
                sent_output_chunks.insert(sandbox_output_chunk_stream_key(&chunk));
                let frame = sinomo_sandbox::SandboxRunProcessPlanStreamFrame::Output { chunk };
                write_daemon_sandbox_process_plan_stream_frame_async(writer, &frame).await?;
            }
            StreamMessage::Complete(Ok(outcome)) => {
                for step in &outcome.step_outcomes {
                    for chunk in &step.output_chunks {
                        if sent_output_chunks.insert(sandbox_output_chunk_stream_key(chunk)) {
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

fn sandbox_output_chunks_from_bytes(
    command_id: &sinomo_sandbox::SandboxCommandId,
    sandbox_id: &sinomo_sandbox::SandboxId,
    stream: sinomo_sandbox::SandboxOutputStream,
    bytes: &[u8],
) -> Vec<sinomo_sandbox::SandboxOutputChunk> {
    bytes
        .chunks(sinomo_sandbox::SANDBOX_OUTPUT_CHUNK_BYTES)
        .enumerate()
        .map(|(index, chunk)| sinomo_sandbox::SandboxOutputChunk {
            sandbox_id: sandbox_id.clone(),
            command_id: command_id.clone(),
            stream: stream.clone(),
            offset: (index * sinomo_sandbox::SANDBOX_OUTPUT_CHUNK_BYTES) as u64,
            bytes: chunk.to_vec(),
        })
        .collect()
}

fn sandbox_output_chunk_stream_key(
    chunk: &sinomo_sandbox::SandboxOutputChunk,
) -> (String, &'static str, u64) {
    let stream = match chunk.stream {
        sinomo_sandbox::SandboxOutputStream::Stdout => "stdout",
        sinomo_sandbox::SandboxOutputStream::Stderr => "stderr",
    };
    (chunk.command_id.to_string(), stream, chunk.offset)
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

fn validate_daemon_sandbox_manifest_policy(
    paths: &DaemonPaths,
    policy: &sinomo_sandbox::SandboxPolicy,
    node_id: &sinomo_core::node::NodeId,
    manifest: &sinomo_sandbox::SandboxInstanceManifest,
    phase: &str,
) -> Result<(), DaemonError> {
    if let Err(denial) = policy.validate_manifest(manifest, node_id) {
        append_daemon_sandbox_event(
            paths,
            sinomo_sandbox::sandbox_policy_denied_event(
                node_id.clone(),
                Some(&manifest.sandbox_id),
                phase,
                &denial,
            ),
        )?;
        if denial.code.starts_with("mount-") {
            append_daemon_sandbox_event(
                paths,
                sinomo_sandbox::sandbox_mount_denied_event(
                    node_id.clone(),
                    Some(&manifest.sandbox_id),
                    phase,
                    &denial,
                ),
            )?;
        }
        return Err(DaemonError::Parse {
            detail: denial.to_string(),
        });
    }
    Ok(())
}

fn validate_daemon_sandbox_resolved_mount_policy(
    paths: &DaemonPaths,
    policy: &sinomo_sandbox::SandboxPolicy,
    dfs: &mut sinomo_dfs::NetworkedDfsService<sinomo_dfs::DfsNodeTransport>,
    manifest: &sinomo_sandbox::SandboxInstanceManifest,
    node_id: &sinomo_core::node::NodeId,
    phase: &str,
) -> Result<(), DaemonError> {
    for mount in &manifest.mounts {
        let resolved = dfs.posix_resolve_path_distributed(&mount.source_sifs_path, true)?;
        let mut resolved_mount = mount.clone();
        resolved_mount.source_sifs_path = resolved;
        if let Err(denial) = policy.validate_mounts(&[resolved_mount], node_id) {
            append_daemon_sandbox_event(
                paths,
                sinomo_sandbox::sandbox_mount_denied_event(
                    node_id.clone(),
                    Some(&manifest.sandbox_id),
                    phase,
                    &denial,
                ),
            )?;
            return Err(DaemonError::Parse {
                detail: denial.to_string(),
            });
        }
    }
    Ok(())
}

fn validate_daemon_sandbox_toolchain_update_policy(
    paths: &DaemonPaths,
    policy: &sinomo_sandbox::SandboxPolicy,
    node_id: &sinomo_core::node::NodeId,
    manifest: &sinomo_sandbox::SandboxToolchainManifest,
    phase: &str,
) -> Result<(), DaemonError> {
    if let Err(denial) = policy.validate_toolchain_update(manifest) {
        append_daemon_sandbox_event(
            paths,
            sinomo_sandbox::sandbox_policy_denied_event(node_id.clone(), None, phase, &denial),
        )?;
        return Err(DaemonError::Parse {
            detail: denial.to_string(),
        });
    }
    Ok(())
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
    daemon_sandbox_replica_candidates_from_content_advertisements(local_node_id, advertisements)
}

fn daemon_sandbox_replica_candidates_from_content_advertisements(
    local_node_id: &sinomo_core::node::NodeId,
    advertisements: Vec<sinomo_dfs::DfsNodeAdvertisement>,
) -> Vec<sinomo_core::node::NodeId> {
    let mut candidates = BTreeSet::new();
    candidates.insert(local_node_id.clone());
    for advertisement in advertisements {
        if advertisement.accepts_content {
            candidates.insert(advertisement.node_id);
        }
    }
    candidates.into_iter().collect()
}

pub fn open_daemon_sandbox_dfs_service(
    paths: &DaemonPaths,
) -> Result<sinomo_dfs::NetworkedDfsService<sinomo_dfs::DfsNodeTransport>, DaemonError> {
    open_daemon_sandbox_dfs_service_on_runtime(paths, None)
}

pub(crate) fn open_daemon_sandbox_dfs_service_on_runtime(
    paths: &DaemonPaths,
    runtime: Option<&tokio::runtime::Handle>,
) -> Result<sinomo_dfs::NetworkedDfsService<sinomo_dfs::DfsNodeTransport>, DaemonError> {
    let data_dir = daemon_data_dir_path(paths)?;
    let data_dir = sinomo_core::local::SinomoDataDir::new(&data_dir);
    data_dir.ensure_root_layout()?;
    let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)?;
    let content = sinomo_dfs::LocalContentStore::new(data_dir.dfs_dir().join("content")).map_err(
        |source| DaemonError::Io {
            path: data_dir.dfs_dir().join("content").display().to_string(),
            detail: source.to_string(),
        },
    )?;
    let peer_node_ids = sinomo_dfs::cluster_dfs_peer_node_ids(&state);
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
    let mut dfs = dfs
        .with_boxed_metadata_backend(metadata)
        .map_err(|source| DaemonError::Parse {
            detail: source.to_string(),
        })?;
    dfs.bootstrap_distributed_sifs()
        .map_err(|source| DaemonError::Parse {
            detail: source.to_string(),
        })?;
    Ok(dfs)
}

#[derive(Debug)]
pub(crate) struct DaemonRolloutTelemetryTickOutcome {
    pub(crate) status: RolloutTelemetryAdvanceStatus,
    pub(crate) applied_transition: Option<AppliedRolloutTransition>,
}

pub(crate) fn run_daemon_rollout_telemetry_reconciliation_tick(
    paths: &DaemonPaths,
) -> Result<DaemonRolloutTelemetryTickOutcome, DaemonError> {
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
    Ok(DaemonRolloutTelemetryTickOutcome {
        status,
        applied_transition,
    })
}

pub fn run_daemon_rollout_telemetry_maintenance_tick(
    paths: &DaemonPaths,
) -> Result<Option<AppliedRolloutTransition>, DaemonError> {
    Ok(run_daemon_rollout_telemetry_reconciliation_tick(paths)?.applied_transition)
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

fn read_daemon_rollout_authoritative_kv_snapshot(
    cluster_paths: &sinomo_core::network::ClusterPaths,
    cluster_state: &sinomo_core::ClusterBootstrapState,
) -> Result<sinomo_core::kv::KvSnapshot, DaemonError> {
    // An empty transaction is an explicit Raft read barrier. It must still
    // quorum-commit even though it does not advance the state revision.
    commit_daemon_rollout_kv_ops(cluster_paths, cluster_state, Vec::new())
}

pub fn run_daemon_module_rollout_desired_reconciliation_tick(
    paths: &DaemonPaths,
) -> Result<Vec<ClusterModuleRolloutReconciliationRecord>, DaemonError> {
    let data_dir = daemon_data_dir_path(paths)?;
    let data_dir_handle = sinomo_core::local::SinomoDataDir::new(&data_dir);
    let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir_handle);
    let Ok(cluster_state) = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
    else {
        return Ok(Vec::new());
    };
    let authoritative_snapshot =
        read_daemon_rollout_authoritative_kv_snapshot(&cluster_paths, &cluster_state)?;
    let desired_records = sinomo_rollout::project_cluster_module_rollout_desired_from_snapshot(
        &authoritative_snapshot,
    )?;
    if desired_records.is_empty() {
        return Ok(Vec::new());
    }

    let rollout_paths = SelfRolloutPaths::new(&data_dir);
    let runtime_state = sinomo_rollout::read_rollout_runtime_state(&rollout_paths)?;
    let existing_reconciliations: BTreeMap<String, ClusterModuleRolloutReconciliationRecord> =
        sinomo_rollout::project_cluster_module_rollout_reconciliations_from_snapshot(
            &authoritative_snapshot,
        )?
        .into_iter()
        .map(|record| {
            (
                cluster_module_rollout_reconcile_key(
                    &record.node_id,
                    &record.module_id,
                    &record.rollout_plan_id,
                ),
                record,
            )
        })
        .collect();
    let mut reconciliations = Vec::new();
    let mut reconciliation_ops = Vec::new();
    for desired in desired_records {
        if !daemon_module_rollout_desired_targets_node(&desired, &cluster_state.local_node_id) {
            continue;
        }
        let reconciliation = daemon_module_rollout_reconciliation_record(
            &data_dir,
            &cluster_state.cluster_id,
            &cluster_state.local_node_id,
            &desired,
            runtime_state.as_ref(),
        )?;
        let reconciliation_key = cluster_module_rollout_reconcile_key(
            &reconciliation.node_id,
            &reconciliation.module_id,
            &reconciliation.rollout_plan_id,
        );
        if let Some(existing) = existing_reconciliations.get(&reconciliation_key)
            && daemon_module_rollout_reconciliation_stable_eq(existing, &reconciliation)
        {
            reconciliations.push(existing.clone());
            continue;
        }
        reconciliation_ops.push(
            sinomo_rollout::build_cluster_module_rollout_reconciliation_put_op(&reconciliation)?,
        );
        reconciliations.push(reconciliation);
    }
    if !reconciliation_ops.is_empty() {
        commit_daemon_rollout_kv_ops(&cluster_paths, &cluster_state, reconciliation_ops)?;
    }
    Ok(reconciliations)
}

fn daemon_module_rollout_reconciliation_stable_eq(
    left: &ClusterModuleRolloutReconciliationRecord,
    right: &ClusterModuleRolloutReconciliationRecord,
) -> bool {
    left.cluster_id == right.cluster_id
        && left.node_id == right.node_id
        && left.rollout_plan_id == right.rollout_plan_id
        && left.module_id == right.module_id
        && left.artifact_id == right.artifact_id
        && left.state == right.state
        && left.reason == right.reason
        && left.release_id == right.release_id
        && left.release_material_hash == right.release_material_hash
        && left.local_runtime_stage == right.local_runtime_stage
}

fn daemon_module_rollout_desired_targets_node(
    desired: &ClusterModuleRolloutDesiredRecord,
    node_id: &NodeId,
) -> bool {
    desired.state == "active"
        && (desired.rollout_plan.target_nodes.is_empty()
            || desired
                .rollout_plan
                .target_nodes
                .iter()
                .any(|target| target == node_id))
}

fn daemon_module_rollout_reconciliation_record(
    data_dir: &Path,
    cluster_id: &sinomo_core::node::ClusterId,
    node_id: &NodeId,
    desired: &ClusterModuleRolloutDesiredRecord,
    runtime_state: Option<&sinomo_rollout::RolloutRuntimeState>,
) -> Result<ClusterModuleRolloutReconciliationRecord, DaemonError> {
    let mut local_runtime_stage = runtime_state
        .filter(|state| state.plan.module_id == desired.rollout_plan.module_id)
        .map(|state| daemon_rollout_stage_label(&state.stage).to_string());
    let (state, reason, prepared_stage) = if runtime_state.is_some_and(|state| {
        state.plan.rollout_plan_id == desired.rollout_plan.rollout_plan_id
            && state.plan.module_id == desired.rollout_plan.module_id
            && state.plan.artifact_id == desired.rollout_plan.artifact_id
    }) {
        (
            "matched".to_string(),
            "local runtime state already matches desired rollout plan".to_string(),
            None,
        )
    } else {
        daemon_module_rollout_material_reconciliation(data_dir, node_id, desired)?
    };
    local_runtime_stage = local_runtime_stage.or(prepared_stage);

    Ok(ClusterModuleRolloutReconciliationRecord {
        cluster_id: cluster_id.clone(),
        node_id: node_id.clone(),
        rollout_plan_id: desired.rollout_plan.rollout_plan_id.clone(),
        module_id: desired.rollout_plan.module_id.clone(),
        artifact_id: desired.rollout_plan.artifact_id.clone(),
        state,
        reason,
        release_id: desired.release_id.clone(),
        release_material_hash: desired.release_material_hash.clone(),
        local_runtime_stage,
        updated_at: OffsetDateTime::now_utc(),
    })
}

fn daemon_module_rollout_material_reconciliation(
    data_dir: &Path,
    node_id: &NodeId,
    desired: &ClusterModuleRolloutDesiredRecord,
) -> Result<(String, String, Option<String>), DaemonError> {
    let Some(release_id) = desired.release_id.as_ref() else {
        return Ok((
            "blocked".to_string(),
            "desired rollout does not reference a release id".to_string(),
            None,
        ));
    };
    let Some(evidence_record) =
        sinomo_registry::read_artifact_evidence_record(data_dir, release_id)?
    else {
        return Ok((
            "blocked".to_string(),
            format!("artifact evidence {release_id} is not available in the local registry"),
            None,
        ));
    };
    if let Some(expected_hash) = desired.release_material_hash.as_deref()
        && evidence_record.content_hash != expected_hash
    {
        return Ok((
            "blocked".to_string(),
            format!(
                "artifact evidence {release_id} hash {} does not match desired hash {expected_hash}",
                evidence_record.content_hash
            ),
            None,
        ));
    }
    let Some(material) = sinomo_registry::read_verified_release_material(data_dir, release_id)?
    else {
        return Ok((
            "blocked".to_string(),
            format!("verified release material for release {release_id} is not available locally"),
            None,
        ));
    };
    if let Some(reason) = daemon_verified_rollout_material_mismatch(&material, desired) {
        return Ok(("blocked".to_string(), reason, None));
    }
    let trusted_signers = sinomo_registry::read_trusted_artifact_signers(data_dir)?;
    let rollout_paths = SelfRolloutPaths::new(data_dir);
    let preparation = RolloutController::new(node_id.clone())
        .prepare_and_record_verified_artifact_rollout(
            &rollout_paths,
            sinomo_rollout::VerifiedArtifactRolloutRequest {
                node_id: node_id.clone(),
                artifact: material.artifact,
                policy_decision: material.policy_decision,
                rollout_plan: material.rollout_plan,
                release_id: evidence_record.evidence_id,
                release_material_hash: material.release_material_hash,
                evidence_refs: material.evidence_refs,
                trusted_signers,
                active_artifact_id: material.active_artifact_id,
            },
        )?;
    if preparation.accepted {
        return Ok((
            "prepared".to_string(),
            preparation.reason,
            Some("shadow".to_string()),
        ));
    }
    Ok(("blocked".to_string(), preparation.reason, None))
}

fn daemon_verified_rollout_material_mismatch(
    material: &sinomo_registry::VerifiedReleaseMaterialRecord,
    desired: &ClusterModuleRolloutDesiredRecord,
) -> Option<String> {
    if Some(&material.release_id) != desired.release_id.as_ref() {
        return Some(format!(
            "verified release material references release {}, but desired references {}",
            material.release_id,
            desired
                .release_id
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| "none".to_string())
        ));
    }
    if Some(&material.release_material_hash) != desired.release_material_hash.as_ref() {
        return Some(
            "verified release material hash does not match desired release hash".to_string(),
        );
    }
    if material.rollout_plan != desired.rollout_plan {
        return Some(
            "verified release material rollout plan does not match desired rollout plan"
                .to_string(),
        );
    }
    if material.artifact.artifact_id != desired.rollout_plan.artifact_id {
        return Some(
            "verified rollout material artifact id does not match desired plan".to_string(),
        );
    }
    if material.artifact.module_id != desired.rollout_plan.module_id {
        return Some("verified rollout material module id does not match desired plan".to_string());
    }
    if material.artifact.target_ring != desired.rollout_plan.target_ring {
        return Some(
            "verified rollout material target ring does not match desired plan".to_string(),
        );
    }
    if material.policy_decision.target_artifact_id != desired.rollout_plan.artifact_id {
        return Some(
            "verified rollout material policy target artifact does not match desired plan"
                .to_string(),
        );
    }
    if material.policy_decision.target_ring != desired.rollout_plan.target_ring {
        return Some(
            "verified rollout material policy target ring does not match desired plan".to_string(),
        );
    }
    None
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

fn daemon_sandbox_http_exchange_record_for_command<F>(
    sandbox_fs: &F,
    command_id: &sinomo_sandbox::SandboxCommandId,
) -> Result<Option<sinomo_sandbox::SandboxHttpExchangeRecord>, DaemonError>
where
    F: sinomo_sandbox::SandboxFileSystem,
{
    let logs_path = sinomo_sandbox::SandboxPath::new("/logs").expect("static logs path");
    let mut entries = sandbox_fs.list(&logs_path)?;
    entries.sort_by(|left, right| left.path.cmp(&right.path));
    let prefix = format!("/logs/{command_id}.");
    for entry in entries {
        let path = entry.path;
        let path_text = path.as_str();
        if path_text.starts_with(&prefix) && path_text.ends_with(".http.sobj") {
            let bytes = sandbox_fs.read(&path, None)?;
            return Ok(Some(sinomo_sandbox::decode_http_exchange_record(&bytes)?));
        }
    }
    Ok(None)
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

        let limits = daemon_sandbox_create_limits(&request, &policy_max);

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
        let mut topology_only_projection = DaemonConnectivityTopologyProjection::new(false);
        topology_only_projection.seed_if_newer(&changed_route);
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
        ] {
            let payload = crate::DaemonObjectRpcForwardRequest {
                target_node_id: target_node.clone(),
                request: inner.clone(),
                connect_timeout_millis: 5_000,
                idle_timeout_millis: None,
                overall_timeout_millis: None,
                observe_connectivity: false,
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

    fn append_cluster_module_rollout_desired_for_test(
        paths: &sinomo_core::network::ClusterPaths,
        node_id: &NodeId,
        desired: &ClusterModuleRolloutDesiredRecord,
    ) -> Result<sinomo_core::kv::KvSnapshot, sinomo_core::network::NetworkError> {
        let op = sinomo_rollout::build_cluster_module_rollout_desired_put_op(desired)
            .map_err(sinomo_core::network::NetworkError::directory)?;
        append_cluster_raft_command(paths, node_id, sinomo_core::kv::KvCommand::Op(op))
    }

    fn cluster_module_rollout_statuses_for_test(
        paths: &sinomo_core::network::ClusterPaths,
    ) -> Result<Vec<ClusterModuleRolloutStatusRecord>, sinomo_core::network::NetworkError> {
        let state = sinomo_core::cluster::read_cluster_state_without_kv(paths)?;
        let snapshot = sinomo_core::cluster::read_cluster_kv_local_snapshot(paths, &state)?.value;
        sinomo_rollout::project_cluster_module_rollout_statuses_from_snapshot(&snapshot)
            .map_err(sinomo_core::network::NetworkError::directory)
    }

    fn cluster_module_rollout_reconciliations_for_test(
        paths: &sinomo_core::network::ClusterPaths,
    ) -> Result<Vec<ClusterModuleRolloutReconciliationRecord>, sinomo_core::network::NetworkError>
    {
        let state = sinomo_core::cluster::read_cluster_state_without_kv(paths)?;
        let snapshot = sinomo_core::cluster::read_cluster_kv_local_snapshot(paths, &state)?.value;
        sinomo_rollout::project_cluster_module_rollout_reconciliations_from_snapshot(&snapshot)
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
                        crate::service_adapter::DaemonServiceRegistryHost::new_on_runtime_with_services(
                            paths.clone(),
                            service_runtime.clone(),
                            vec![crate::DaemonServiceRegistryHook::new(|builder| {
                                builder.register(FailingRegistryService)
                            })],
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
                .expect("registry failure is returned through the session");
        });

        let address = address_rx.recv().expect("listener address");
        let client_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("client runtime");
        let client_paths = DaemonPaths::new(&data_dir);
        let mut host =
            crate::client::DaemonSignedObjectRpcAsyncHost::new(client_runtime.handle(), false)
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
                    let service_host = crate::service_adapter::DaemonServiceRegistryHost::new_on_runtime_with_services(
                        paths.clone(),
                        service_runtime.clone(),
                        vec![crate::DaemonServiceRegistryHook::new(|builder| {
                            builder.register(PermissionDeniedRegistryService)
                        })],
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
                .expect("node-local permission rejection is returned to the client");
            assert!(!stop, "permission rejection must not stop the daemon");
        });

        let address = address_rx.recv().expect("listener address");
        let client_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("client runtime");
        let client_paths = DaemonPaths::new(&data_dir);
        let mut host =
            crate::client::DaemonSignedObjectRpcAsyncHost::new(client_runtime.handle(), false)
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
        assert!(response.body.is_empty());
        let status = sinomo_dfs::decode_dfs_status_rpc_response(&response.response)
            .expect("dfs status response");
        assert_eq!(status.file_system_count, 0);
    }

    #[test]
    fn sandbox_replica_candidates_use_advertised_content_services_and_local_fallback() {
        let local = NodeId::from_legacy_named("node_local").expect("local node");
        let replica = NodeId::from_legacy_named("node_replica").expect("replica node");
        let metadata_only = NodeId::from_legacy_named("node_metadata").expect("metadata node");
        let candidates = daemon_sandbox_replica_candidates_from_content_advertisements(
            &local,
            vec![
                sinomo_dfs::DfsNodeAdvertisement::metadata(metadata_only),
                sinomo_dfs::DfsNodeAdvertisement::content(replica.clone()),
                sinomo_dfs::DfsNodeAdvertisement::content(local.clone()),
            ],
        );

        assert!(candidates.contains(&local));
        assert!(candidates.contains(&replica));
        assert_eq!(candidates.len(), 2);
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

        let applied = run_daemon_rollout_telemetry_maintenance_tick(&daemon_paths)
            .expect("telemetry tick")
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

        let applied =
            run_daemon_rollout_telemetry_maintenance_tick(&daemon_paths).expect("telemetry tick");

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

        let applied = run_daemon_rollout_telemetry_maintenance_tick(&daemon_paths)
            .expect("telemetry tick")
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
    fn daemon_reconciles_missing_local_module_rollout_material_as_blocked() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-module-rollout-desired-blocked");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("layout");
        let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let cluster_id =
            sinomo_core::node::ClusterId::from_legacy_named("cluster_daemon_desired_blocked")
                .expect("cluster");
        let node = NodeId::from_legacy_named("node_daemon_desired_blocked").expect("node id");
        let mut cluster_state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "daemon desired blocked",
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

        let module_id =
            sinomo_core::Id::from_legacy_named("module.trigger.silence_policy").expect("module");
        let rollout_plan_id =
            sinomo_core::Id::from_legacy_named("rollout_daemon_desired_blocked").expect("rollout");
        let release_id = sinomo_core::Id::from_legacy_named("release_missing").expect("release");
        let desired = ClusterModuleRolloutDesiredRecord {
            cluster_id: cluster_id.clone(),
            state: "active".to_string(),
            rollout_plan: sinomo_core::RolloutPlan {
                rollout_plan_id: rollout_plan_id.clone(),
                artifact_id: sinomo_core::Id::from_legacy_named("artifact_daemon_desired")
                    .expect("artifact"),
                module_id: module_id.clone(),
                target_ring: sinomo_core::TrustRing::R3SandboxedModule,
                strategy: sinomo_core::RolloutStrategy::Shadow,
                target_nodes: vec![node.clone()],
                initial_weight_bps: 0,
                promotion_steps_bps: vec![500],
                rollback_policy: sinomo_core::RollbackPolicy::default(),
            },
            requested_by_node_id: node.clone(),
            release_id: Some(release_id.clone()),
            release_material_hash: Some("blake3:release".to_string()),
            evidence_refs: vec!["registry://artifact-evidence/release_missing".to_string()],
            requested_at: Some("2026-06-01T00:00:00Z".to_string()),
            updated_at: OffsetDateTime::now_utc(),
        };
        append_cluster_module_rollout_desired_for_test(&cluster_paths, &node, &desired)
            .expect("sync desired");

        let daemon_paths = DaemonPaths::with_root(root.join("daemon"));
        let reconciliations = run_daemon_module_rollout_desired_reconciliation_tick(&daemon_paths)
            .expect("reconcile desired");
        let records = cluster_module_rollout_reconciliations_for_test(&cluster_paths)
            .expect("read reconciliations");

        assert_eq!(reconciliations.len(), 1);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].cluster_id, cluster_id);
        assert_eq!(records[0].node_id, node);
        assert_eq!(records[0].module_id, module_id);
        assert_eq!(records[0].rollout_plan_id, rollout_plan_id);
        assert_eq!(records[0].state, "blocked");
        assert_eq!(records[0].release_id.as_ref(), Some(&release_id));
        assert!(
            records[0]
                .reason
                .contains("is not available in the local registry")
        );
        assert!(records[0].local_runtime_stage.is_none());
        let first_record = records[0].clone();
        let second_reconciliations =
            run_daemon_module_rollout_desired_reconciliation_tick(&daemon_paths)
                .expect("second reconcile desired");
        let second_records = cluster_module_rollout_reconciliations_for_test(&cluster_paths)
            .expect("read second reconciliations");
        assert_eq!(second_reconciliations, vec![first_record.clone()]);
        assert_eq!(second_records, vec![first_record]);
    }

    #[test]
    fn daemon_prepares_verified_local_material_for_desired_module_rollout() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-module-rollout-desired-prepared");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("layout");
        let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let cluster_id =
            sinomo_core::node::ClusterId::from_legacy_named("cluster_daemon_desired_prepared")
                .expect("cluster");
        let node = NodeId::from_legacy_named("node_daemon_desired_prepared").expect("node id");
        let mut cluster_state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "daemon desired prepared",
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

        let module_id =
            sinomo_core::Id::from_legacy_named("module.trigger.silence_policy").expect("module");
        let artifact_id = sinomo_core::Id::from_legacy_named("artifact_daemon_prepared_candidate")
            .expect("artifact");
        let active_artifact_id =
            sinomo_core::Id::from_legacy_named("artifact_daemon_prepared_active").expect("active");
        let rollout_plan = sinomo_core::RolloutPlan {
            rollout_plan_id: sinomo_core::Id::from_legacy_named("rollout_daemon_desired_prepared")
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
        let release_id = sinomo_core::Id::from_legacy_named("release_prepared").expect("release");
        let release_ref = format!("registry://artifact-evidence/{release_id}");
        let (artifact, signer) = signed_daemon_test_artifact(
            &root,
            "daemon-prepared-signer",
            &artifact_id,
            &module_id,
            "blake3:candidate",
            vec![release_ref.clone()],
        );
        let evidence_record = daemon_test_artifact_evidence_record(
            &root,
            &release_id,
            "blake3:release",
            vec![release_ref.clone()],
        );
        sinomo_registry::write_artifact_evidence_record(&root, &evidence_record)
            .expect("write evidence record");
        sinomo_registry::write_trusted_artifact_signers(&root, std::slice::from_ref(&signer))
            .expect("write trusted signer");
        let material = sinomo_registry::VerifiedReleaseMaterialRecord {
            release_id: release_id.clone(),
            release_material_hash: "blake3:release".to_string(),
            artifact: artifact.clone(),
            policy_decision: sinomo_core::PolicyDecision {
                decision_id: sinomo_core::Id::from_legacy_named("policy_daemon_prepared")
                    .expect("policy"),
                target_ring: sinomo_core::TrustRing::R3SandboxedModule,
                target_artifact_id: artifact_id.clone(),
                action: sinomo_core::PolicyAction::Allow,
                capability_diff: sinomo_core::CapabilityDiff::default(),
                required_additional_evals: Vec::new(),
                max_rollout_weight_bps: Some(500),
                reason: "verified rollout material accepted".to_string(),
            },
            rollout_plan: rollout_plan.clone(),
            active_artifact_id: active_artifact_id.clone(),
            evidence_refs: vec![release_ref.clone()],
            registered_at: OffsetDateTime::now_utc(),
        };
        sinomo_registry::write_verified_release_material(&root, &material)
            .expect("write rollout material");
        let desired = ClusterModuleRolloutDesiredRecord {
            cluster_id: cluster_id.clone(),
            state: "active".to_string(),
            rollout_plan: rollout_plan.clone(),
            requested_by_node_id: node.clone(),
            release_id: Some(release_id.clone()),
            release_material_hash: Some("blake3:release".to_string()),
            evidence_refs: vec![release_ref],
            requested_at: Some("2026-06-01T00:00:00Z".to_string()),
            updated_at: OffsetDateTime::now_utc(),
        };
        append_cluster_module_rollout_desired_for_test(&cluster_paths, &node, &desired)
            .expect("sync desired");

        let daemon_paths = DaemonPaths::with_root(root.join("daemon"));
        let reconciliations = run_daemon_module_rollout_desired_reconciliation_tick(&daemon_paths)
            .expect("reconcile desired");
        let runtime_state =
            sinomo_rollout::read_rollout_runtime_state(&SelfRolloutPaths::new(&root))
                .expect("read runtime state")
                .expect("runtime state");

        assert_eq!(reconciliations.len(), 1);
        assert_eq!(reconciliations[0].cluster_id, cluster_id);
        assert_eq!(reconciliations[0].node_id, node);
        assert_eq!(reconciliations[0].state, "prepared");
        assert_eq!(reconciliations[0].release_id.as_ref(), Some(&release_id));
        assert_eq!(
            reconciliations[0].local_runtime_stage.as_deref(),
            Some("shadow")
        );
        assert!(reconciliations[0].reason.contains("prepared for rollout"));
        assert_eq!(runtime_state.plan, rollout_plan);
        assert_eq!(
            runtime_state.loader_state.active_artifact_id,
            active_artifact_id
        );
        assert_eq!(
            runtime_state.loader_state.candidate_artifact_id.as_ref(),
            Some(&artifact_id)
        );
        assert_eq!(runtime_state.stage, sinomo_rollout::RolloutStage::Shadow);
    }

    #[test]
    fn daemon_reconciles_matching_local_module_rollout_runtime_state() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-module-rollout-desired-matched");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("layout");
        let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let cluster_id =
            sinomo_core::node::ClusterId::from_legacy_named("cluster_daemon_desired_matched")
                .expect("cluster");
        let node = NodeId::from_legacy_named("node_daemon_desired_matched").expect("node id");
        let mut cluster_state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "daemon desired matched",
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

        let module_id =
            sinomo_core::Id::from_legacy_named("module.trigger.silence_policy").expect("module");
        let artifact_id = sinomo_core::Id::from_legacy_named("artifact_daemon_desired_matched")
            .expect("artifact");
        let plan = sinomo_core::RolloutPlan {
            rollout_plan_id: sinomo_core::Id::from_legacy_named("rollout_daemon_desired_matched")
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
        let active_artifact_id =
            sinomo_core::Id::from_legacy_named("artifact_daemon_desired_active").expect("active");
        let mut runtime_state =
            RolloutController::new(node.clone()).initial_state(plan.clone(), active_artifact_id);
        runtime_state.stage = sinomo_rollout::RolloutStage::Canary;
        runtime_state.current_weight_bps = 500;
        let rollout_paths = SelfRolloutPaths::new(&root);
        sinomo_rollout::write_rollout_runtime_state(&rollout_paths, &runtime_state)
            .expect("runtime state");
        let desired = ClusterModuleRolloutDesiredRecord {
            cluster_id: cluster_id.clone(),
            state: "active".to_string(),
            rollout_plan: plan.clone(),
            requested_by_node_id: node.clone(),
            release_id: None,
            release_material_hash: None,
            evidence_refs: Vec::new(),
            requested_at: Some("2026-06-01T00:00:00Z".to_string()),
            updated_at: OffsetDateTime::now_utc(),
        };
        append_cluster_module_rollout_desired_for_test(&cluster_paths, &node, &desired)
            .expect("sync desired");

        let daemon_paths = DaemonPaths::with_root(root.join("daemon"));
        let reconciliations = run_daemon_module_rollout_desired_reconciliation_tick(&daemon_paths)
            .expect("reconcile desired");
        let records = cluster_module_rollout_reconciliations_for_test(&cluster_paths)
            .expect("read reconciliations");

        assert_eq!(reconciliations.len(), 1);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].cluster_id, cluster_id);
        assert_eq!(records[0].node_id, node);
        assert_eq!(records[0].module_id, module_id);
        assert_eq!(records[0].rollout_plan_id, plan.rollout_plan_id);
        assert_eq!(records[0].artifact_id, artifact_id);
        assert_eq!(records[0].state, "matched");
        assert_eq!(records[0].local_runtime_stage.as_deref(), Some("canary"));
        assert!(
            records[0]
                .reason
                .contains("already matches desired rollout plan")
        );
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
        let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id,
            "daemon solo DFS ready",
            local,
            "local",
            "pk-local",
            now,
        );
        state.members[0].capabilities =
            sinomo_core::node::main_daemon_capabilities(&sinomo_core::ClusterMemberRole::Voter);
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
        while send_test_status_request(&data_dir, &address, Duration::from_millis(200)).is_err() {
            assert!(
                start.elapsed() < Duration::from_secs(5),
                "daemon did not become ready"
            );
            thread::sleep(Duration::from_millis(20));
        }

        let file_system = sinomo_dfs::default_cluster_sifs(&state);
        let shard_entries = sinomo_core::kv::read_cluster_kv_prefix_entries(
            &cluster_paths.kv_paths(),
            &format!(
                "{}/",
                sinomo_dfs::metadata_shards_prefix(&file_system.file_system_id)
            ),
        )
        .expect("metadata shard topology");
        let backend =
            sinomo_dfs::open_cluster_sharded_dfs_metadata_backend(&data_dir, &cluster_paths)
                .expect("open sharded backend")
                .expect("configured sharded backend");
        let services_anchor = backend
            .get_object_by_path(&file_system.file_system_id, sinomo_dfs::SIFS_SERVICES_PATH)
            .expect("services anchor lookup");

        send_test_shutdown_request(&data_dir, &address, Duration::from_secs(3))
            .expect("shutdown request");
        daemon
            .join()
            .expect("daemon thread")
            .expect("daemon exits cleanly");

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
    fn daemon_dfs_idle_work_parks_without_a_timer_or_launch() {
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
    fn daemon_dfs_deferred_work_uses_bounded_read_only_reprobe() {
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
    fn daemon_dfs_probe_uses_exact_future_repair_retry_deadline() {
        let observed_at = Instant::now();
        let observed_at_unix_nanos = 1_000_000_000;
        let retry_delay = Duration::from_secs(47);
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
    fn daemon_dfs_probe_preserves_far_exact_deadline_without_periodic_cap() {
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
        let decision =
            daemon_dfs_failed_maintenance_decision(observed_at, true, Some(future_probe));

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
    fn dfs_maintenance_notification_requests_a_probe_without_forcing_a_pass() {
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

        assert!(daemon_dfs_maintenance_probe_is_due(
            observed_at,
            next_probe_at
        ));
        assert!(
            !notification.trigger_maintenance,
            "an edge hint must not bypass pending-work classification"
        );
        assert!(!notification.immediate_continuation);

        let decision = daemon_dfs_failed_maintenance_decision(
            observed_at,
            triggered_maintenance_requested,
            next_probe_at,
        );
        assert_eq!(
            decision.next_probe_at,
            Some(observed_at),
            "task failure must not discard a notification probe already due"
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
        );

        assert!(!decision.trigger_maintenance);
        assert!(!decision.immediate_continuation);
        assert_eq!(decision.next_probe_at, None);
        assert!(!daemon_dfs_maintenance_probe_is_due(observed_at, None));
    }

    #[test]
    fn deferred_dfs_followup_probe_does_not_latch_quiet_work() {
        let observed_at = Instant::now();
        let decision = daemon_dfs_maintenance_completion_decision(
            DaemonDfsMaintenanceTickOutcome::Deferred,
            observed_at,
            1_000_000_000,
            Some(DaemonDfsPendingMaintenanceWork::default()),
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
            Some(startup_delay),
            "even an owned continuation must respect the startup phase"
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
            actionable,
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
    fn rollout_telemetry_requires_startup_cluster_runtime() {
        assert!(daemon_rollout_telemetry_enabled("daemon", true));
        assert!(
            !daemon_rollout_telemetry_enabled("daemon", false),
            "a daemon that started unclustered must not adopt later cluster state through telemetry"
        );
        assert!(
            !daemon_rollout_telemetry_enabled("shadow-daemon", true),
            "the shadow supervisor never owns cluster rollout telemetry"
        );
    }

    #[test]
    fn rollout_telemetry_reconciliation_does_not_own_desired_rollout_state() {
        let source = include_str!("control.rs");
        let telemetry_reconciliation = source
            .split_once("pub(crate) fn run_daemon_rollout_telemetry_reconciliation_tick")
            .expect("telemetry reconciliation function")
            .1
            .split_once("pub fn run_daemon_rollout_telemetry_maintenance_tick")
            .expect("telemetry compatibility wrapper")
            .0;
        assert!(
            !telemetry_reconciliation
                .contains("run_daemon_module_rollout_desired_reconciliation_tick"),
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
            u64::try_from(DAEMON_SANDBOX_LIFECYCLE_SAFETY_SWEEP_INTERVAL.as_nanos())
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
        record_daemon_sandbox_lifecycle_safety_sweep(&paths, now).expect("sweep stamp");
        let sandbox_id =
            sinomo_sandbox::SandboxId::from_legacy_named("sandbox_pending_marker_recovery")
                .expect("sandbox id");
        mark_daemon_sandbox_authoritative_recovery_pending(&paths, &sandbox_id)
            .expect("pending marker");

        run_daemon_sandbox_lifecycle_maintenance_tick_with_scan(&paths, now, |marked| {
            assert_eq!(marked, BTreeSet::from([sandbox_id.clone()]));
            Err(DaemonError::Parse {
                detail: "injected recovery failure".to_string(),
            })
        })
        .expect_err("failed recovery");
        assert_eq!(
            daemon_sandbox_authoritative_recovery_pending_ids(&paths).expect("pending ids"),
            BTreeSet::from([sandbox_id.clone()])
        );

        run_daemon_sandbox_lifecycle_maintenance_tick_with_scan(&paths, now + 1, |marked| {
            assert_eq!(marked, BTreeSet::from([sandbox_id.clone()]));
            clear_daemon_sandbox_authoritative_recovery_pending(&paths, &sandbox_id)?;
            Ok(1)
        })
        .expect("successful recovery");
        assert!(
            daemon_sandbox_authoritative_recovery_pending_ids(&paths)
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
    fn mandatory_default_dfs_has_no_environment_disable_path() {
        for ignored_legacy_value in [None, Some("0"), Some("false"), Some("off")] {
            assert!(
                daemon_role_owns_default_dfs("daemon"),
                "legacy value {ignored_legacy_value:?} must not disable the main daemon's DFS lifecycle"
            );
        }
        assert!(!daemon_role_owns_default_dfs("shadow-daemon"));

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

    fn signed_daemon_test_artifact(
        root: &Path,
        signer_id: &str,
        artifact_id: &sinomo_core::Id,
        module_id: &sinomo_core::Id,
        content_hash: &str,
        attestations: Vec<String>,
    ) -> (
        sinomo_core::ArtifactManifest,
        sinomo_registry::TrustedArtifactSigner,
    ) {
        let identity_path = root.join(format!("{signer_id}.sobj"));
        let identity = sinomo_core::node::load_or_create_node_identity(&identity_path)
            .expect("signer identity");
        let mut artifact = sinomo_core::ArtifactManifest {
            artifact_id: artifact_id.clone(),
            module_id: module_id.clone(),
            target_ring: sinomo_core::TrustRing::R3SandboxedModule,
            content_hash: content_hash.to_string(),
            signature: None,
            source_workspace: None,
            source_revision: None,
            module_manifest: sinomo_core::ModuleManifest::r3_wasm(
                module_id.clone(),
                "0.1.0",
                "trigger",
            ),
            provenance: sinomo_core::ArtifactProvenance {
                produced_by: sinomo_core::ActorRef {
                    kind: sinomo_core::ActorKind::Registry,
                    id: "local-registry".to_string(),
                },
                source_event_id: None,
                build_log_artifact_id: None,
                attestations,
            },
        };
        artifact.signature = Some(
            sinomo_module::sign_artifact_manifest_with_node_identity(&artifact, &identity)
                .expect("sign artifact"),
        );
        let signer = sinomo_registry::TrustedArtifactSigner::local_ed25519(
            signer_id,
            identity.public_key,
            "daemon test signer",
        );
        let _ = std::fs::remove_file(identity_path);
        (artifact, signer)
    }

    fn daemon_test_artifact_evidence_record(
        root: &Path,
        evidence_id: &sinomo_core::Id,
        content_hash: &str,
        evidence_refs: Vec<String>,
    ) -> sinomo_registry::ArtifactEvidenceRecord {
        let evidence_path = root.join("artifact-evidence").join(evidence_id.as_str());
        sinomo_registry::ArtifactEvidenceRecord {
            evidence_id: evidence_id.clone(),
            evidence_kind: "daemon-test-release".to_string(),
            evidence_path: evidence_path.display().to_string(),
            content_hash: content_hash.to_string(),
            content_manifest_path: None,
            attestation_path: None,
            evidence_refs,
            verification: sinomo_registry::ArtifactEvidenceIntegrityReport {
                evidence_id: evidence_id.clone(),
                evidence_kind: "daemon-test-release".to_string(),
                evidence_path: evidence_path.display().to_string(),
                declared_content_hash: Some(content_hash.to_string()),
                recomputed_content_hash: Some(content_hash.to_string()),
                content_manifest_path: None,
                attestation_path: None,
                checks: vec![sinomo_registry::ArtifactEvidenceIntegrityCheck {
                    name: "daemon-test-integrity".to_string(),
                    passed: true,
                    detail: "daemon test fixture is pre-verified".to_string(),
                }],
                verified_at: OffsetDateTime::now_utc(),
            },
            registered_at: OffsetDateTime::now_utc(),
        }
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
        let mut host = crate::client::DaemonSignedObjectRpcAsyncHost::new(runtime.handle(), false)
            .with_paths(&paths);
        let mut request = request.clone();
        request.auth = None;
        let result = runtime.block_on(
            sinomo_core::network::ClusterObjectRpcClient::endpoint_only(&mut host).call_loopback(
                data_dir,
                address,
                sinomo_core::network::ObjectRpcCall::value(request),
                sinomo_core::network::ClusterObjectRpcOptions::new(
                    sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(timeout),
                ),
            ),
        )?;
        if !result.body.is_empty() {
            return Err(DaemonError::Parse {
                detail: "test value RPC returned an unexpected attachment".to_string(),
            });
        }
        Ok(result.response)
    }
}
