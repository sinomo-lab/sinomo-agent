use crate::cluster::{
    ClusterNetworkMaintenance, DAEMON_CLUSTER_TICK_INTERVAL, cluster_paths_from_daemon_paths,
    initialize_local_cluster_raft_resident,
};
use crate::{
    AuthenticatedDaemonCommand, DaemonClusterStatus, DaemonError, DaemonLock, DaemonPaths,
    DaemonSignerAuthority, daemon_paths_are_shadow, read_daemon_status,
};
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
use sinomo_core::node::NodeId;
#[cfg(test)]
use sinomo_core::{ClusterMemberRecord, ClusterTransportKind};
#[cfg(test)]
use sinomo_rollout::SelfRolloutPaths;
use std::collections::BTreeSet;
#[cfg(test)]
use std::future::Future;
use std::path::PathBuf;
use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicU8, AtomicU64, Ordering},
};
#[cfg(test)]
use std::thread;
use std::time::{Duration, Instant};
#[cfg(test)]
use time::OffsetDateTime;
use tokio::io::AsyncWriteExt as _;

// Durable work that is known but currently cannot run (for example, a repair
// whose target is offline) retains a bounded retry cadence. Quiet nodes park:
// an O(total-state) inventory is not an acceptable liveness probe.
pub const DAEMON_CLUSTER_NETWORK_MAINTENANCE_PHASE_TIMEOUT: Duration = Duration::from_secs(10);
pub const DAEMON_NETWORK_ENVIRONMENT_WATCH_ENV: &str = "SINOMO_DAEMON_NETWORK_ENVIRONMENT_WATCH";
const DAEMON_RUNTIME_TASK_JOIN_TIMEOUT: Duration = Duration::from_secs(3);
const DAEMON_NETWORK_MODULE_CONTEXT_TASK_JOIN_TIMEOUT: Duration = Duration::from_secs(30);
const DAEMON_RUNTIME_TASK_DRAIN_GRACE: Duration = Duration::from_millis(100);
const DAEMON_RUNTIME_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);
const DAEMON_CONNECTIVITY_EXTERNAL_SNAPSHOT_DEBOUNCE: Duration = Duration::from_millis(100);

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
    #[cfg(test)]
    PendingWork,
}

fn cluster_network_maintenance_subphase_name(
    subphase: DaemonClusterNetworkMaintenanceSubphase,
) -> &'static str {
    match subphase {
        DaemonClusterNetworkMaintenanceSubphase::TickPlan => "tick_plan",
        #[cfg(test)]
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
    _observation_subscription: sinomo_core::kv::KvCommittedObservationSubscription,
    _external_snapshot_watcher: Option<notify::RecommendedWatcher>,
    // The callbacks retain this too. Keeping it here makes the ownership
    // explicit and lets focused tests inspect the independent observer.
    _projection: Arc<Mutex<DaemonConnectivityTopologyProjection>>,
    #[cfg(test)]
    _scheduled_changes: Arc<AtomicU64>,
}

#[derive(Clone)]
struct DaemonConnectivityTopologyObserver {
    cluster_paths: sinomo_core::network::ClusterPaths,
    runtime: tokio::runtime::Handle,
    projection: Arc<Mutex<DaemonConnectivityTopologyProjection>>,
    projection_generation: Arc<AtomicU64>,
    required_revision: Arc<AtomicU64>,
    #[cfg(test)]
    scheduled_changes: Arc<AtomicU64>,
}

impl DaemonConnectivityTopologyObserver {
    fn observe_snapshot(&self, snapshot: &sinomo_core::kv::KvSnapshot, source: &'static str) {
        self.observe_native_snapshot_at_generation(
            snapshot,
            source,
            self.projection_generation.load(Ordering::Acquire),
        );
    }

    fn observe_native_snapshot_at_generation(
        &self,
        snapshot: &sinomo_core::kv::KvSnapshot,
        source: &'static str,
        generation: u64,
    ) {
        let changed = self
            .projection
            .lock()
            .map(|mut projection| {
                if self.projection_generation.load(Ordering::Acquire) != generation
                    || snapshot.revision.0 < self.required_revision.load(Ordering::Acquire)
                {
                    return DaemonConnectivityProjectionChange::default();
                }
                self.required_revision
                    .fetch_max(snapshot.revision.0, Ordering::AcqRel);
                projection.observe(snapshot)
            })
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
        self.observe_discontinuity(delta.revision.0);
    }

    fn observe_discontinuity(&self, revision: u64) {
        let (generation, had_proof) = if let Ok(mut projection) = self.projection.lock() {
            let had_proof = projection.revision().is_some();
            self.required_revision.fetch_max(
                revision.max(projection.revision().unwrap_or(0)),
                Ordering::AcqRel,
            );
            // Values remain a typed comparison base while revision None
            // withdraws proof. Recovery still reads native state every time.
            let comparison = std::mem::take(&mut projection.values);
            *projection = DaemonConnectivityTopologyProjection::new(false);
            projection.values = comparison;
            let generation = self
                .projection_generation
                .fetch_add(1, Ordering::AcqRel)
                .wrapping_add(1);
            (generation, had_proof)
        } else {
            return;
        };
        // Materialized files are dirty hints. Only existing native state or a
        // validated main-daemon ReadIndex result can restore this generation.
        match self.resident_connectivity_snapshot() {
            Ok(Some(snapshot)) => self.observe_native_snapshot_at_generation(
                &snapshot,
                "canonical connectivity discontinuity rebuild",
                generation,
            ),
            Ok(None) => {
                if sinomo_core::kv::process_local_raft_access_is_denied(
                    &self.cluster_paths.kv_paths().raft,
                ) {
                    self.request_main_native_snapshot(generation, had_proof);
                } else if had_proof {
                    self.publish_unavailable(generation, revision);
                }
            }
            Err(error) => {
                tracing::warn!("canonical connectivity resident recovery is unavailable: {error}");
                if had_proof {
                    self.publish_unavailable(generation, revision);
                }
            }
        }
    }

    fn publish_unavailable(&self, generation: u64, revision: u64) {
        if self.projection_generation.load(Ordering::Acquire) != generation {
            return;
        }
        self.publish_change(
            DaemonConnectivityProjectionChange {
                probe_inputs_changed: true,
                route_generation_changed: true,
            },
            "canonical connectivity observation discontinuity",
            revision,
        );
    }

    fn resident_connectivity_snapshot(
        &self,
    ) -> Result<Option<sinomo_core::kv::KvSnapshot>, DaemonError> {
        let kv_paths = self.cluster_paths.kv_paths();
        if sinomo_core::kv::process_local_raft_access_is_denied(&kv_paths.raft) {
            return Ok(None);
        }
        let state = sinomo_core::cluster::read_cluster_state_without_kv(&self.cluster_paths)?;
        let Some(read) = sinomo_core::kv::with_cluster_kv_resident_read_projection_before(
            &kv_paths.raft_wal_paths(&state.local_node_id),
            &state.local_node_id,
            Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            |projection| {
                let mut entries = Vec::new();
                for prefix in ["/cluster/members/", "/cluster/endpoints/"] {
                    entries.extend(projection.read_index_prefix_limited_by_payload_bytes(
                        prefix,
                        sinomo_core::kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
                        sinomo_core::kv::CLUSTER_KV_RANGE_PREFIX_MAX_PAYLOAD_BYTES,
                    )?);
                }
                Ok::<_, sinomo_core::kv::KvError>(entries)
            },
        ) else {
            return Ok(None);
        };
        let read = read?;
        Ok(Some(sinomo_core::kv::KvSnapshot {
            revision: read.revision,
            compacted_revision: read.revision,
            entries: read.value?,
            leases: Vec::new(),
            events: Vec::new(),
            committed_requests: Vec::new(),
            expired_requests: Vec::new(),
        }))
    }

    fn request_main_native_snapshot(&self, generation: u64, had_proof: bool) {
        let observer = self.clone();
        let runtime = self.runtime.clone();
        self.runtime.spawn(async move {
            let result = async {
                let root = observer.cluster_paths.root.parent().ok_or_else(|| DaemonError::Network {
                    address: observer.cluster_paths.root.display().to_string(),
                    detail: "cluster paths have no data root".to_string(),
                })?;
                let data_dir = sinomo_core::local::SinomoDataDir::new(root);
                let main_paths = DaemonPaths::new(&data_dir);
                let state = sinomo_core::cluster::read_cluster_state_without_kv(&observer.cluster_paths)?;
                let payload = sinomo_core::kv::cluster_kv_authoritative_snapshot_rpc_payload()?;
                let request = sinomo_core::network::ObjectRpcRequest::from_encoded_body(
                    payload.route, payload.category, payload.body,
                );
                let timeout = sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
                let read: sinomo_core::kv::KvAuthoritativeRead<sinomo_core::kv::KvSnapshot> =
                    crate::client::send_object_rpc_call_to_target_async_with_daemon_paths_and_network_error(
                        &main_paths, &state.local_node_id,
                        sinomo_core::network::ObjectRpcCall::value(request),
                        sinomo_core::network::ClusterObjectRpcOptions::new(
                            sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(timeout),
                        ).with_overall_timeout(timeout),
                        &runtime, false,
                    ).await?.decode_body().await?;
                sinomo_core::kv::validate_cluster_kv_authoritative_snapshot_read(&read)?;
                Ok::<_, DaemonError>(read.value)
            }.await;
            match result {
                Ok(snapshot) => observer.observe_native_snapshot_at_generation(
                    &snapshot, "main-daemon ReadIndex connectivity recovery", generation,
                ),
                Err(error) => {
                    tracing::warn!("main-daemon ReadIndex connectivity recovery is unavailable: {error}");
                    if had_proof { observer.publish_unavailable(generation, observer.required_revision.load(Ordering::Acquire)); }
                }
            }
        });
    }

    fn publish_change(
        &self,
        changed: DaemonConnectivityProjectionChange,
        source: &'static str,
        generation: u64,
    ) {
        if changed.probe_inputs_changed {
            #[cfg(test)]
            self.scheduled_changes.fetch_add(1, Ordering::AcqRel);
            request_daemon_cluster_maintenance(
                DaemonClusterMaintenanceReason::CommittedConnectivityTopologyChanged,
            );
        }
        let _ = (changed.route_generation_changed, source, generation);
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
                    observer.observe_discontinuity(0);
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
) -> Result<DaemonConnectivityTopologySubscriptions, DaemonError> {
    let cluster_paths = cluster_paths_from_daemon_paths(paths)?;
    let projection = Arc::new(Mutex::new(DaemonConnectivityTopologyProjection::new(false)));
    #[cfg(test)]
    let scheduled_changes = Arc::new(AtomicU64::new(0));
    let observer = DaemonConnectivityTopologyObserver {
        cluster_paths: cluster_paths.clone(),
        runtime: runtime.clone(),
        projection: Arc::clone(&projection),
        projection_generation: Arc::new(AtomicU64::new(0)),
        required_revision: Arc::new(AtomicU64::new(0)),
        #[cfg(test)]
        scheduled_changes: Arc::clone(&scheduled_changes),
    };
    let canonical_observer = observer.clone();
    let observation_subscription =
        sinomo_core::cluster::subscribe_cluster_kv_committed_observations(
            &cluster_paths,
            move |observation| {
                if observation.applied_index.is_none() {
                    // Main owns the complete native stream. Shadow's separate
                    // filesystem bridge validates an explicit main proof.
                    return;
                }
                match &observation.value {
                    sinomo_core::kv::KvCommittedObservationValue::Delta(delta) => {
                        canonical_observer.observe_delta(delta)
                    }
                    sinomo_core::kv::KvCommittedObservationValue::Snapshot(snapshot) => {
                        if observation.kind
                            == sinomo_core::kv::KvCommittedObservationKind::Replacement
                        {
                            if let Ok(mut projection) = canonical_observer.projection.lock() {
                                *projection = DaemonConnectivityTopologyProjection::new(false);
                                canonical_observer
                                    .projection_generation
                                    .fetch_add(1, Ordering::AcqRel);
                                canonical_observer
                                    .required_revision
                                    .store(snapshot.revision.0, Ordering::Release);
                            }
                        }
                        canonical_observer.observe_snapshot(
                            snapshot,
                            "canonical committed connectivity snapshot",
                        );
                    }
                    sinomo_core::kv::KvCommittedObservationValue::Discontinuity(_) => {
                        let known_revision = observation
                            .applied_index
                            .and(observation.revision)
                            .map(|revision| revision.0)
                            .or_else(|| {
                                canonical_observer
                                    .projection
                                    .lock()
                                    .ok()
                                    .and_then(|projection| projection.revision())
                            })
                            .unwrap_or(0);
                        canonical_observer.observe_discontinuity(known_revision);
                    }
                }
            },
        );
    // Main-daemon Raft/read-model mutations publish exact process-local
    // snapshot and delta callbacks above. Only the storage-fenced shadow needs
    // a filesystem bridge for generations written by the main process.
    let external_snapshot_watcher = crate::daemon_paths_are_shadow(paths)
        .then(|| daemon_connectivity_topology_snapshot_watcher(&cluster_paths, observer.clone()))
        .transpose()?;
    // Registration precedes the coherent native seed. A discontinuity that
    // crosses the read invalidates its generation; a shadow instead joins one
    // bounded main-daemon ReadIndex request without opening local Raft.
    let generation = observer.projection_generation.load(Ordering::Acquire);
    match observer.resident_connectivity_snapshot() {
        Ok(Some(snapshot)) => {
            if let Ok(mut projection_guard) = projection.lock()
                && observer.projection_generation.load(Ordering::Acquire) == generation
                && snapshot.revision.0 >= observer.required_revision.load(Ordering::Acquire)
            {
                projection_guard.seed_if_newer(&snapshot);
                observer
                    .required_revision
                    .fetch_max(snapshot.revision.0, Ordering::AcqRel);
            }
        }
        Ok(None)
            if sinomo_core::kv::process_local_raft_access_is_denied(
                &cluster_paths.kv_paths().raft,
            ) =>
        {
            observer.request_main_native_snapshot(generation, false)
        }
        Ok(None) => {}
        Err(error) => tracing::warn!(
            "connectivity topology observation started without an initial native projection: {error}"
        ),
    }
    Ok(DaemonConnectivityTopologySubscriptions {
        _observation_subscription: observation_subscription,
        _external_snapshot_watcher: external_snapshot_watcher,
        _projection: projection,
        #[cfg(test)]
        _scheduled_changes: scheduled_changes,
    })
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
    let listener = crate::prepare_daemon_control_listener(options)?;
    run_daemon_control_loop_with_prepared_listener(
        listener,
        paths,
        label,
        cluster,
        component_catalog,
        runtime_config,
    )
}

pub fn run_daemon_control_loop_with_prepared_listener(
    listener: crate::PreparedDaemonControlListener,
    paths: DaemonPaths,
    label: &str,
    cluster: Option<DaemonClusterStatus>,
    component_catalog: crate::DaemonComponentCatalog,
    runtime_config: crate::DaemonRuntimeConfig,
) -> Result<(), DaemonError> {
    let (options, listener, fallback_from_port) = listener.into_parts();
    if let Some(requested_port) = fallback_from_port {
        eprintln!(
            "warning: sinomo {label} port {requested_port} is unavailable; listening on automatic fallback port {}",
            options.port
        );
    }
    crate::cluster::deny_shadow_daemon_local_raft_access(&paths)?;
    crate::cluster::ensure_cluster_directory_installed();
    let _lock = DaemonLock::acquire(&paths, &options, cluster.clone())?;
    // Product startup contributors run before every loader-state read. Rollout
    // uses this hook to recover interrupted native-loader transactions without
    // making the daemon depend on Rollout implementation code.
    let data_dir_root = daemon_data_dir_path(&paths)?;
    component_catalog
        .run_startup_contributors(&data_dir_root)
        .map_err(|detail| DaemonError::Parse { detail })?;
    if let Some(requested_port) = fallback_from_port {
        tracing::warn!(
            requested_port,
            fallback_port = options.port,
            "sinomo {label} preferred port unavailable; using automatic fallback"
        );
    }
    let startup_local_raft_resident_initialized =
        match initialize_local_cluster_raft_resident(&paths) {
            Ok(initialized) => initialized,
            Err(error) => {
                tracing::warn!(
                    "sinomo {label} cluster raft resident initialization deferred: {error}"
                );
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
        sinomo_core::network::AsyncDaemonControlListener::from_std(listener, &bind_address),
    )?;
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let shutdown_signal = shutdown_tx.clone();
    let mut runtime_tasks: Vec<(&'static str, tokio::task::JoinHandle<()>)> = Vec::new();
    tracing::info!("sinomo {label} listening on {bind_address}");
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

    let idle_stats = Arc::new(crate::IdleStatsAtomics::new());
    crate::install_process_idle_stats(idle_stats.clone());
    crate::register_io_observer(idle_stats.clone());
    let maintenance_inhibitors = component_catalog.maintenance_inhibitors();

    // The service host owns a lifecycle-scoped catalog publisher component;
    // generic cluster maintenance no longer carries service discovery state.
    let service_host = daemon_runtime.block_on(
        crate::service_adapter::DaemonServiceRegistryHost::new_on_runtime_with_catalog_and_resources(
            paths.clone(),
            daemon_runtime_handle.clone(),
            component_catalog,
            node_resources.clone(),
        ),
    )?;
    service_host.notify_component_host_start();
    if let Some(requested_port) = fallback_from_port {
        crate::append_operational_event_best_effort(
            &paths,
            crate::OperationalSeverity::Warn,
            "daemon-control",
            "daemon.lifecycle.port-fallback",
            format!(
                "sinomo {label} preferred port {requested_port} was unavailable; listening on fallback port {}",
                options.port
            ),
            None,
            serde_json::json!({
                "label": label,
                "requested_port": requested_port,
                "fallback_port": options.port,
                "address": bind_address,
                "pid": std::process::id(),
            }),
        );
    }
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
            "fallback_from_port": fallback_from_port,
            "pid": std::process::id(),
            "cluster_node_id": cluster.as_ref().map(|status| status.node_id.to_string()),
        }),
    );

    let maintenance_paths = paths.clone();
    let maintenance_options = options.clone();
    let maintenance_label = label.to_string();
    let maintenance_idle_stats = idle_stats.clone();
    let maintenance_runtime = daemon_runtime_handle.clone();
    let maintenance_startup_cluster = cluster.clone();
    let maintenance_connectivity_topology_subscription = if startup_cluster_runtime_enabled {
        Some(subscribe_daemon_connectivity_topology_sources(
            &paths,
            &daemon_runtime_handle,
        )?)
    } else {
        None
    };
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
            maintenance_inhibitors,
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
        service_host.clone(),
        daemon_runtime_handle.clone(),
        shutdown_tx,
        shutdown_rx.clone(),
    ))?;
    let component_shutdown =
        service_host.shutdown_components(&daemon_runtime_handle, DAEMON_RUNTIME_TASK_JOIN_TIMEOUT);
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
    crate::append_operational_event_best_effort(
        &paths,
        if component_shutdown.is_ok() {
            crate::OperationalSeverity::Info
        } else {
            crate::OperationalSeverity::Error
        },
        "daemon-control",
        if component_shutdown.is_ok() {
            "daemon.lifecycle.stopped"
        } else {
            "daemon.lifecycle.stop-incomplete"
        },
        match &component_shutdown {
            Ok(()) => format!("sinomo {label} stopped"),
            Err(error) => format!("sinomo {label} component shutdown remains incomplete: {error}"),
        },
        None,
        serde_json::json!({
            "label": label,
            "address": bind_address,
            "pid": std::process::id(),
        }),
    );
    daemon_runtime.shutdown_timeout(DAEMON_RUNTIME_SHUTDOWN_TIMEOUT);
    component_shutdown
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

struct DaemonClusterMaintenanceReactorHost {
    cluster_network: ClusterNetworkMaintenance,
    paths: DaemonPaths,
    options: crate::DaemonRunOptions,
    label: String,
    runtime: tokio::runtime::Handle,
    startup_cluster: Option<DaemonClusterStatus>,
    maintain_cluster_runtime: bool,
    maintenance_inhibitors: Vec<sinomo_module::ComponentMaintenanceInhibitor>,
    node_resources: sinomo_device::NodeResourceMonitor,
}

impl sinomo_core::cluster::ClusterMaintenanceReactorHost for DaemonClusterMaintenanceReactorHost {
    type Work = crate::cluster::ClusterNetworkMaintenancePendingWork;
    type Error = DaemonError;

    fn identity_is_current<'a>(
        &'a mut self,
    ) -> sinomo_core::cluster::ClusterMaintenanceFuture<'a, bool, Self::Error> {
        Box::pin(async move {
            if !self.maintain_cluster_runtime {
                return Ok(true);
            }
            daemon_cluster_identity_matches_startup(&self.paths, self.startup_cluster.as_ref())
        })
    }

    fn inhibited(&self) -> bool {
        self.label == "daemon"
            && component_maintenance_is_inhibited(&self.paths, &self.maintenance_inhibitors)
    }

    fn plan<'a>(
        &'a mut self,
    ) -> sinomo_core::cluster::ClusterMaintenanceFuture<'a, Option<Self::Work>, Self::Error> {
        Box::pin(async move {
            if !self.maintain_cluster_runtime {
                return Ok(None);
            }
            let mut observation = DaemonClusterNetworkMaintenanceSubphaseObservation::start(
                DaemonClusterNetworkMaintenanceSubphase::TickPlan,
            );
            let work = self
                .cluster_network
                .tick_async(&self.paths, &self.options, self.runtime.clone())
                .await?;
            observation.complete(Some(work.is_some()));
            Ok(work)
        })
    }

    fn execute<'a>(
        &'a mut self,
        work: Self::Work,
        deadline: Instant,
    ) -> sinomo_core::cluster::ClusterMaintenanceFuture<'a, (), Self::Error> {
        Box::pin(async move {
            self.cluster_network
                .run_pending_work_until_async(work, self.runtime.clone(), deadline)
                .await;
            Ok(())
        })
    }

    fn next_due(&self, now: Instant) -> Duration {
        if self.maintain_cluster_runtime {
            self.cluster_network.next_due_duration(now)
        } else {
            DAEMON_CLUSTER_TICK_INTERVAL
        }
    }

    fn record_plan_failure(&mut self, now: Instant) {
        self.cluster_network.record_active_tick_plan_failure(now);
    }

    fn observe_phase(&mut self, timed_out: bool, elapsed: Duration) {
        observe_cluster_network_maintenance_phase(
            if timed_out {
                DaemonClusterNetworkMaintenancePhaseDisposition::TimedOut
            } else {
                DaemonClusterNetworkMaintenancePhaseDisposition::Completed
            },
            elapsed,
        );
    }

    fn on_wake(&mut self) {
        let dirty = take_daemon_cluster_maintenance_dirty();
        let now = Instant::now();
        if dirty & DAEMON_CLUSTER_MAINTENANCE_DIRTY_PUBLISH != 0 {
            self.cluster_network.request_publish_refresh(now);
        }
        if dirty & DAEMON_CLUSTER_MAINTENANCE_DIRTY_CONNECTIVITY != 0 {
            if dirty & DAEMON_CLUSTER_MAINTENANCE_DIRTY_NETWORK_ENVIRONMENT != 0 {
                self.cluster_network
                    .request_network_environment_refresh(now);
            } else {
                self.cluster_network.request_connectivity_refresh(now);
            }
        }
        if dirty & DAEMON_CLUSTER_MAINTENANCE_DIRTY_DISCOVERY != 0 {
            self.cluster_network.request_discovery_refresh(now);
        }
    }

    fn wait_for_external_change<'a>(
        &'a mut self,
    ) -> sinomo_core::cluster::ClusterMaintenanceFuture<'a, (), Self::Error> {
        Box::pin(async move {
            let resources = self.node_resources.changed().await;
            self.cluster_network.set_resource_mode(resources.mode);
            observe_daemon_node_resources(resources);
            Ok(())
        })
    }
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
    maintenance_inhibitors: Vec<sinomo_module::ComponentMaintenanceInhibitor>,
    mut node_resources: sinomo_device::NodeResourceMonitor,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
    let _connectivity_topology_subscription = connectivity_topology_subscription;
    let mut cluster_network = ClusterNetworkMaintenance::with_idle_stats(
        idle_stats,
        startup_local_raft_resident_initialized,
    )
    .await;
    let current_node_resources = node_resources.take_latest();
    cluster_network.set_resource_mode(current_node_resources.mode);
    observe_daemon_node_resources(current_node_resources);
    let cluster_notify = daemon_cluster_maintenance_notify();
    let network_environment_watch = if label == "daemon" && maintain_cluster_runtime {
        start_daemon_network_environment_watch(&label, shutdown_rx.clone()).await
    } else {
        None
    };
    let mut host = DaemonClusterMaintenanceReactorHost {
        cluster_network,
        paths,
        options,
        label,
        runtime,
        startup_cluster,
        maintain_cluster_runtime,
        maintenance_inhibitors,
        node_resources,
    };
    sinomo_core::cluster::run_cluster_maintenance_reactor(
        &mut host,
        sinomo_core::cluster::ClusterMaintenanceReactorConfig {
            phase_timeout: DAEMON_CLUSTER_NETWORK_MAINTENANCE_PHASE_TIMEOUT,
            execution_tail_reserve: Duration::from_millis(500),
            inhibited_retry: DAEMON_CLUSTER_TICK_INTERVAL,
            identity_retry: DAEMON_CLUSTER_TICK_INTERVAL,
        },
        cluster_notify,
        shutdown_rx,
    )
    .await;
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

#[cfg(test)]
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

pub async fn run_daemon_control_accept_loop(
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
    let reverse_affinity = if daemon_paths_are_shadow(&paths) {
        sinomo_core::network::DestinationProcessAffinity::Any
    } else {
        sinomo_core::network::DestinationProcessAffinity::MainDaemon
    };
    let mut routed_stream_incoming =
        match sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths) {
            Ok(state) => Some(
                sinomo_core::network::register_routed_stream_incoming_with_process_affinity(
                    state.local_node_id,
                    reverse_affinity,
                )?,
            ),
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
                        sinomo_core::network::register_routed_stream_incoming_with_process_affinity(state.local_node_id, reverse_affinity)?,
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
                        let dispatch_timeline_identity = sinomo_core::network::network_timeline_capture_is_active()
                            .then(|| stream.timeline_stream_identity());
                        if let Some((connection, call)) = dispatch_timeline_identity {
                            sinomo_core::network::record_network_timeline_event_now(
                                sinomo_core::network::NetworkTimelineEvent::new(
                                    Default::default(), sinomo_core::network::network_timeline_stage::DISPATCH_STREAM_ACCEPTED,
                                    sinomo_core::network::network_timeline_flag::SUCCESS, connection, call, 0, 0, 0, 0,
                                ),
                            );
                        }
                        let client_paths = paths.clone();
                        let client_label = label.clone();
                        let client_service_host = service_host.clone();
                        let client_service_runtime = service_runtime.clone();
                        let client_shutdown_tx = shutdown_tx.clone();
                        tokio::spawn(async move {
                            if let Some((connection, call)) = dispatch_timeline_identity {
                                sinomo_core::network::record_network_timeline_event_now(
                                    sinomo_core::network::NetworkTimelineEvent::new(
                                        Default::default(), sinomo_core::network::network_timeline_stage::DISPATCH_TASK_POLL,
                                        sinomo_core::network::network_timeline_flag::SUCCESS, connection, call, 0, 0, 0, 0,
                                    ),
                                );
                            }
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
            failure_kind = ?evidence.kind,
            reason = %evidence.reason,
            "routed-session realization rejected a direct edge"
        );
        sinomo_core::network::__connectivity_store::record_daemon_owned_local_cluster_endpoint_command_connectivity(
            self.cluster_paths,
            self.state,
            endpoint,
            match evidence.kind {
                sinomo_core::network::DirectConnectivityFailureKind::Transport => {
                    sinomo_core::network::ClusterConnectivityState::Degraded
                }
                sinomo_core::network::DirectConnectivityFailureKind::IdentityMismatch => {
                    sinomo_core::network::ClusterConnectivityState::Unreachable
                }
            },
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
    let mut acceptor = sinomo_core::network::accept_pooled_routed_stream_carrier_for_scopes(
        routed_pool_scopes,
        directory.version().0,
        sinomo_core::network::stream_native_service_route_constraints(),
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
        crate::host_api::enforce_daemon_object_rpc_forward_route_affinity(&route, &mut payload)?;
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

#[cfg(test)]
fn daemon_shutdown_requested(shutdown_rx: &tokio::sync::watch::Receiver<bool>) -> bool {
    *shutdown_rx.borrow()
}

fn component_maintenance_is_inhibited(
    paths: &DaemonPaths,
    inhibitors: &[sinomo_module::ComponentMaintenanceInhibitor],
) -> bool {
    let Some(data_dir) = paths.root.parent() else {
        return false;
    };
    inhibitors.iter().any(|inhibit| inhibit(data_dir))
}

pub fn daemon_data_dir_path(paths: &DaemonPaths) -> Result<PathBuf, DaemonError> {
    paths
        .root
        .parent()
        .map(PathBuf::from)
        .ok_or_else(|| DaemonError::Io {
            path: paths.root.display().to_string(),
            detail: "daemon root has no data directory parent".to_string(),
        })
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
    fn transient_routed_edge_failure_remains_probeable_during_restart() {
        use sinomo_core::network::{
            ConnectivityFailureSink, DirectConnectivityFailureEvidence, DirectorySnapshot,
            DirectoryVersion,
        };
        let root = sinomo_testdir::TestDir::new("restart-edge-failure");
        let data_dir = SinomoDataDir::new(&root);
        let paths = sinomo_core::network::ClusterPaths::new(&data_dir);
        paths.ensure().unwrap();
        let now = OffsetDateTime::now_utc();
        let local = NodeId::generate();
        let remote = NodeId::generate();
        let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
            sinomo_core::Id::generate(),
            "restart failure",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let endpoint =
            sinomo_core::network::daemon_lan_endpoint(&remote, "127.0.0.2:21353".into(), 0);
        let mut member = sinomo_core::ClusterMemberRecord::local_non_voting(
            state.cluster_id.clone(),
            remote.clone(),
            "remote",
            "pk-remote",
            now,
        );
        member.endpoints.push(endpoint.clone());
        state.members.push(member);
        let directory =
            DirectorySnapshot::try_from_members(DirectoryVersion(1), state.members.clone())
                .unwrap();
        let sink = DaemonRoutedConnectivityFailureSink {
            cluster_paths: &paths,
            state: &state,
            directory: &directory,
        };
        let evidence = DirectConnectivityFailureEvidence {
            kind: sinomo_core::network::DirectConnectivityFailureKind::Transport,
            observer_node_id: local.clone(),
            source_node_id: local.clone(),
            target_node_id: remote,
            endpoint_id: endpoint.endpoint_id,
            reason: "connection refused while the peer restarts".into(),
            observed_at: now,
        };
        for _ in 0..3 {
            sink.submit_edge_failure(evidence.clone()).unwrap();
        }
        let records = sinomo_core::network::read_local_cluster_connectivity_records_for_source(
            &paths, &local,
        )
        .unwrap();
        let record = &records[0];
        assert!(
            sinomo_core::network::cluster_connectivity_probe_due_with_startup_settle(
                record,
                now + time::Duration::seconds(1),
                now + time::Duration::seconds(120),
                &sinomo_core::ClusterNodeRole::Workstation,
                true,
            ),
            "a connection refusal must not suppress restart probes as an identity mismatch: {record:?}"
        );
        assert_eq!(record.failure_reason, None);
        sink.submit_edge_failure(DirectConnectivityFailureEvidence {
            kind: sinomo_core::network::DirectConnectivityFailureKind::IdentityMismatch,
            reason: "authenticated peer differs from the directory".into(),
            ..evidence
        })
        .unwrap();
        let records = sinomo_core::network::read_local_cluster_connectivity_records_for_source(
            &paths, &local,
        )
        .unwrap();
        assert_eq!(
            records[0].failure_reason,
            Some(sinomo_core::network::ClusterConnectivityFailureReason::IdentityMismatch)
        );
        assert!(
            !sinomo_core::network::cluster_connectivity_probe_due_with_startup_settle(
                &records[0],
                now + time::Duration::seconds(1),
                now + time::Duration::seconds(120),
                &sinomo_core::ClusterNodeRole::Workstation,
                true,
            ),
            "real identity failures must retain their startup quarantine"
        );
    }

    fn dfs_component_catalog_for_test() -> sinomo_module::ComponentCatalog {
        sinomo_module::ComponentCatalog::builder()
            .register_module_factory(
                sinomo_dfs::dfs_module_registration(),
                Arc::new(sinomo_dfs::component_factory().expect("DFS component factory")),
            )
            .expect("register DFS test component")
            .build()
            .expect("DFS test component catalog")
    }

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
            portability: None,
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
            events: (vec![KvEvent {
                revision: KvRevision(2),
                key: member_key.clone(),
                kind: KvEventKind::Put {
                    value: changed_value,
                },
            }])
            .into(),
            final_key_states: (vec![ClusterKvCommittedKeyState {
                key: member_key.clone(),
                entry: Some(changed_entry),
            }])
            .into(),
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
        Arc::make_mut(&mut gapped.events)[0].revision = KvRevision(4);
        Arc::make_mut(&mut gapped.final_key_states)[0]
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
        Arc::make_mut(&mut compacted.events)[0].revision = KvRevision(3);
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

        let DaemonConnectivityTopologySubscriptions {
            _observation_subscription,
            _external_snapshot_watcher: _watcher,
            _projection: projection,
            ..
        } = subscribe_daemon_connectivity_topology_sources(
            &daemon_paths,
            &tokio::runtime::Handle::current(),
        )
        .expect("connectivity topology subscriptions");
        // Isolate the actual cross-process watcher from same-process native
        // receipts; the changed bytes below are genuinely committed in Raft.
        drop(_observation_subscription);
        let initial_revision = projection
            .lock()
            .expect("initial projection")
            .revision()
            .expect("initial revision");

        let current = sinomo_core::cluster::read_cluster_kv_current_snapshot(&cluster_paths)
            .expect("current cluster KV");
        let stale_snapshot = current.clone();
        let member_key = sinomo_core::node::cluster_member_key(&local_node);
        let mut member = state.members[0].clone();
        member.endpoints = vec![sinomo_core::network::daemon_lan_endpoint(
            &local_node,
            "127.0.0.1:43002".to_string(),
            20,
        )];
        let external_write_at = Instant::now();
        let external_snapshot = sinomo_core::cluster::append_cluster_raft_command(
            &cluster_paths,
            &local_node,
            sinomo_core::kv::KvCommand::Op(
                sinomo_core::kv::KvOp::put_serde(member_key.clone(), &member, None).unwrap(),
            ),
        )
        .expect("native critical member update");
        assert!(external_snapshot.revision.0 > initial_revision);
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            let observed = projection.lock().expect("observed projection").revision();
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

        let mut projection = projection.lock().expect("final projection");
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn derived_future_snapshot_and_gap_cannot_replace_native_connectivity() {
        let root = sinomo_testdir::TestDir::new("daemon-connectivity-derived-future");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().unwrap();
        let paths = DaemonPaths::new(&data_dir);
        paths.ensure().unwrap();
        let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
        let local = NodeId::from_legacy_named("connectivity-derived-future-local").unwrap();
        let cluster =
            sinomo_core::node::ClusterId::from_legacy_named("connectivity-derived-future-cluster")
                .unwrap();
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster,
            "derived future",
            local.clone(),
            "local",
            "local-key",
            OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).unwrap();
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
            .unwrap();
        let subscriptions = subscribe_daemon_connectivity_topology_sources(
            &paths,
            &tokio::runtime::Handle::current(),
        )
        .unwrap();
        let native =
            sinomo_core::cluster::read_cluster_kv_current_snapshot(&cluster_paths).unwrap();
        let native_values = subscriptions._projection.lock().unwrap().values.clone();
        let scheduled = subscriptions._scheduled_changes.load(Ordering::Acquire);
        let mut image = sinomo_core::kv::InProcessKvStore::restore(native.clone()).unwrap();
        let mut forged = state.members[0].clone();
        forged.endpoints = vec![sinomo_core::network::daemon_lan_endpoint(
            &local,
            "127.0.0.1:43999".to_string(),
            20,
        )];
        image
            .put_serde(sinomo_core::node::cluster_member_key(&local), &forged, None)
            .unwrap();
        let future = image.try_snapshot().unwrap();
        assert!(future.revision > native.revision);
        sinomo_core::kv::write_cluster_kv_snapshot(&cluster_paths.kv_paths(), &future).unwrap();
        {
            let projection = subscriptions._projection.lock().unwrap();
            assert_eq!(projection.revision(), Some(native.revision.0));
            assert_eq!(
                projection.values, native_values,
                "future derived routes cannot replace native membership"
            );
        }
        assert_eq!(
            subscriptions._scheduled_changes.load(Ordering::Acquire),
            scheduled
        );
        // This is another actual source event, not an unchanged-write shortcut.
        image.put("/derived/unrelated", vec![8], None).unwrap();
        sinomo_core::kv::write_cluster_kv_snapshot(
            &cluster_paths.kv_paths(),
            &image.try_snapshot().unwrap(),
        )
        .unwrap();
        assert_eq!(
            subscriptions._scheduled_changes.load(Ordering::Acquire),
            scheduled,
            "repeated derived hints read native inputs without scheduling duplicate work"
        );
        image.put("/derived/next", vec![9], None).unwrap();
        let desired = image.try_snapshot().unwrap();
        let kv_paths = cluster_paths.kv_paths();
        let mut classifier_calls = 0;
        let error = sinomo_core::kv::write_imported_cluster_kv_snapshot_if_changed(
            &kv_paths,
            &desired,
            |_| {
                classifier_calls += 1;
                assert_eq!(classifier_calls, 1);
                // Inject external IO failure after the real known-prestate capture.
                std::fs::remove_file(&kv_paths.snapshot).unwrap();
                std::fs::create_dir(&kv_paths.snapshot).unwrap();
                false
            },
        );
        assert_eq!(classifier_calls, 1);
        assert!(error.is_err());
        let projection = subscriptions._projection.lock().unwrap();
        assert_eq!(projection.revision(), Some(native.revision.0));
        assert_eq!(
            projection.values, native_values,
            "future-prestate Gap recovers only native routes"
        );
        assert_eq!(
            subscriptions._scheduled_changes.load(Ordering::Acquire),
            scheduled
        );
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
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");

        let request = request.with_auth(signed);
        let service_host = runtime
            .block_on(
                crate::service_adapter::DaemonServiceRegistryHost::new_on_runtime_with_catalog(
                    target_daemon_paths.clone(),
                    runtime.handle().clone(),
                    dfs_component_catalog_for_test(),
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
        service_host
            .shutdown_components(runtime.handle(), DAEMON_RUNTIME_TASK_JOIN_TIMEOUT)
            .expect("normal bounded component shutdown");
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
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
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
        assert!(matches!(
            crate::service_adapter::authenticate_daemon_control_request_via_registry(
                &target_daemon_paths,
                &request,
                &service_host,
            ),
            Err(DaemonError::AuthDenied)
        ));
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
        service_host
            .shutdown_components(runtime.handle(), DAEMON_RUNTIME_TASK_JOIN_TIMEOUT)
            .expect("normal bounded component shutdown");
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
            port_fallback: false,
        };
        let cluster = crate::daemon_cluster_status(&state, crate::DaemonClusterProcess::Main)
            .expect("main cluster status");
        let daemon = thread::spawn({
            let paths = paths.clone();
            move || {
                run_daemon_control_loop_with_catalog_and_profile(
                    options,
                    paths,
                    "daemon",
                    Some(cluster),
                    dfs_component_catalog_for_test(),
                    crate::DaemonExecutionProfile::Standard,
                )
            }
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
            port_fallback: false,
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
