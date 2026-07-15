use crate::cluster::{
    ClusterNetworkMaintenance, DAEMON_CLUSTER_PUBLISH_INTERVAL, DAEMON_CLUSTER_TICK_INTERVAL,
    cluster_paths_from_daemon_paths, handle_daemon_cluster_kv_snapshot,
    handle_daemon_cluster_relay_request_on_runtime, prune_local_cluster_raft_uncommitted_tail,
};
use crate::dfs::{
    daemon_dfs_maintenance_notify, daemon_dfs_node_transport, daemon_dfs_node_transport_on_runtime,
    daemon_dfs_pending_maintenance_work, ensure_default_distributed_dfs_async_on_runtime,
    ensure_default_local_dfs_expose, run_daemon_dfs_maintenance_tick_in_blocking_task,
};
use crate::{
    AuthenticatedDaemonCommand, DaemonClusterStatus, DaemonDataOwnerLock, DaemonError, DaemonLock,
    DaemonPaths, daemon_paths_are_shadow, handle_daemon_clear_rollout_status,
    handle_daemon_finalize_shadow_rollout, read_daemon_status,
};
#[allow(unused_imports)]
use sinomo_core::cluster::ClusterKvPathsExt;
use sinomo_core::events::EventStore;
use sinomo_core::node::NodeId;
use sinomo_core::{
    ClusterModuleRolloutDesiredRecord, ClusterModuleRolloutReconciliationRecord,
    ClusterModuleRolloutStatusRecord, cluster_module_rollout_reconcile_key,
};
use sinomo_rollout::{
    AppliedRolloutTransition, ROLLOUT_CLEAR_STATUS_ROUTE, ROLLOUT_FINALIZE_SHADOW_ROUTE,
    ROLLOUT_RPC_CATEGORY, RolloutController, RolloutTelemetryAdvanceOptions,
    RolloutTelemetryAdvanceStatus, SelfRolloutPaths, read_rollout_status,
    rollout_main_activation_path, rollout_shadow_finalize_path,
};
use sinomo_sandbox::SandboxFileSystem;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
#[cfg(test)]
use std::thread;
use std::time::{Duration, Instant};
use time::OffsetDateTime;
use tokio::io::AsyncWriteExt as _;

pub const DAEMON_COMMAND_MAX_BYTES: usize =
    sinomo_core::network::DAEMON_CONTROL_RPC_MAX_FRAME_BYTES;
pub const DAEMON_DFS_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(300);
pub const DAEMON_DFS_MAINTENANCE_PROBE_INTERVAL: Duration = Duration::from_secs(30);
pub const DAEMON_DFS_TRIGGERED_MAINTENANCE_MIN_INTERVAL: Duration = Duration::from_secs(30);
pub const DAEMON_CLUSTER_NETWORK_MAINTENANCE_PHASE_TIMEOUT: Duration = Duration::from_secs(10);
pub const DAEMON_ROLLOUT_TELEMETRY_INTERVAL: Duration = Duration::from_secs(60);
pub const DAEMON_SHADOW_ROLLOUT_IDLE_FINALIZATION_INTERVAL: Duration =
    DAEMON_CLUSTER_PUBLISH_INTERVAL;
pub const DAEMON_NETWORK_ENVIRONMENT_WATCH_ENV: &str = "SINOMO_DAEMON_NETWORK_ENVIRONMENT_WATCH";
pub(crate) const ACCEPTED_ROLLOUT_RESPONSE_DRAIN_GRACE: Duration = Duration::from_millis(200);
const DAEMON_RUNTIME_TASK_JOIN_TIMEOUT: Duration = Duration::from_secs(3);
const DAEMON_NETWORK_MODULE_CONTEXT_TASK_JOIN_TIMEOUT: Duration = Duration::from_secs(30);
const DAEMON_BACKGROUND_MAINTENANCE_TASK_JOIN_TIMEOUT: Duration = DAEMON_RUNTIME_TASK_JOIN_TIMEOUT;
const DAEMON_RUNTIME_TASK_DRAIN_GRACE: Duration = Duration::from_millis(100);
const DAEMON_RUNTIME_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);
const DAEMON_SANDBOX_STREAM_CHANNEL_CAPACITY: usize = 64;
const DAEMON_SANDBOX_SESSION_BODY_BUFFER_BYTES: usize = 256 * 1024;
const DAEMON_DFS_MESSAGE_REPLAY_RETENTION: Duration = Duration::from_secs(120);
const DAEMON_DFS_MESSAGE_REPLAY_WAIT: Duration = Duration::from_secs(30);
const DAEMON_DFS_MESSAGE_REPLAY_MAX_CACHED_BODY_BYTES: usize =
    sinomo_core::network::OBJECT_RPC_UNARY_ADAPTER_MAX_BODY_BYTES;
const DAEMON_SANDBOX_LOCK_ANCHOR_CONTENT: &[u8] = b"sinomo-sandbox-lifecycle-lock-v1\n";
#[cfg(feature = "sandbox")]
const DAEMON_SANDBOX_LIFECYCLE_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(30);

static DAEMON_DFS_MESSAGE_REPLAY_CACHE: OnceLock<
    Mutex<BTreeMap<PathBuf, BTreeMap<Vec<u8>, DaemonDfsMessageReplayEntry>>>,
> = OnceLock::new();
static DAEMON_CLUSTER_MAINTENANCE_NOTIFY: OnceLock<Arc<tokio::sync::Notify>> = OnceLock::new();

#[derive(Clone)]
enum DaemonDfsMessageReplayState {
    InFlight,
    Complete(sinomo_core::network::ObjectRpcResponse),
}

#[derive(Clone)]
struct DaemonDfsMessageReplayEntry {
    state: DaemonDfsMessageReplayState,
    notify: Arc<tokio::sync::Notify>,
    updated_at: Instant,
}

/// The main daemon owns the mandatory default DFS lifecycle. The shadow daemon
/// is a supervisor and never owns cluster data; there is intentionally no
/// configuration or environment input to this decision.
fn daemon_role_owns_default_dfs(label: &str) -> bool {
    label == "daemon"
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

pub(crate) fn request_daemon_cluster_maintenance_refresh() {
    daemon_cluster_maintenance_notify().notify_one();
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

struct DaemonNetworkEnvironmentWatch {
    notify: Arc<tokio::sync::Notify>,
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
    let notify = Arc::new(tokio::sync::Notify::new());
    let task_notify = Arc::clone(&notify);
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
            task_notify.notify_one();
        }
    });
    Some(DaemonNetworkEnvironmentWatch { notify, task })
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
                Err(_) => dfs.posix_stat_distributed(&anchor_path)?,
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
    acquire_daemon_sandbox_mutation_lease_with_recovery(paths, sandbox_id, runtime, false)
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
        | Err(sinomo_sandbox::SandboxStoreError::LegacyLayout { .. }) => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    let Some(pending) = manifest.pending_reconciliation.clone() else {
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
    crate::cluster::ensure_cluster_directory_installed();
    let _lock = DaemonLock::acquire(&paths, &options, cluster.clone())?;
    let _data_owner_lock = if label == "daemon" {
        Some(DaemonDataOwnerLock::acquire(&paths)?)
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
    if daemon_role_owns_default_dfs(label) {
        ensure_default_local_dfs_expose(&paths)?;
    }
    let daemon_runtime = crate::build_daemon_async_runtime(label)?;
    let daemon_runtime_handle = daemon_runtime.handle().clone();
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

    if let Some(task) = crate::cluster::start_daemon_network_module_context_task(
        &paths,
        &options,
        cluster.as_ref(),
        daemon_runtime_handle.clone(),
        shutdown_rx.clone(),
        label,
    ) {
        runtime_tasks.push(("network module context", task));
    }

    runtime_tasks.push((
        "tcp tunnel maintenance",
        crate::tcp_tunnel::start_tcp_tunnel_maintenance_task(
            paths.clone(),
            daemon_runtime_handle.clone(),
            shutdown_rx.clone(),
            label.to_string(),
        ),
    ));

    if daemon_role_owns_default_dfs(label) {
        let bootstrap_paths = paths.clone();
        let bootstrap_options = options.clone();
        let bootstrap_label = label.to_string();
        let bootstrap_runtime = daemon_runtime_handle.clone();
        daemon_runtime_handle.spawn(async move {
            if let Err(error) = ensure_default_distributed_dfs_async_on_runtime(
                &bootstrap_paths,
                &bootstrap_options,
                false,
                bootstrap_runtime,
            )
            .await
            {
                tracing::warn!(
                    "sinomo {bootstrap_label} distributed DFS bootstrap failed: {error}"
                );
            }
        });
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
    runtime_tasks.push((
        "background maintenance",
        daemon_runtime_handle.spawn(run_daemon_background_maintenance_loop(
            maintenance_paths,
            maintenance_options,
            maintenance_label,
            maintenance_idle_stats,
            maintenance_service_registry,
            maintenance_runtime,
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
        "background maintenance" => DAEMON_BACKGROUND_MAINTENANCE_TASK_JOIN_TIMEOUT,
        _ => DAEMON_RUNTIME_TASK_JOIN_TIMEOUT,
    }
}

fn daemon_runtime_task_abort_before_join(label: &str) -> bool {
    matches!(label, "background maintenance")
}

fn drain_daemon_runtime_after_task_shutdown(runtime: &tokio::runtime::Runtime) {
    runtime.block_on(async {
        tokio::task::yield_now().await;
        tokio::time::sleep(DAEMON_RUNTIME_TASK_DRAIN_GRACE).await;
        tokio::task::yield_now().await;
    });
}

async fn run_daemon_background_maintenance_loop(
    paths: DaemonPaths,
    options: crate::DaemonRunOptions,
    label: String,
    idle_stats: Arc<crate::IdleStatsAtomics>,
    service_registry: sinomo_core::service::ServiceRegistrySnapshotStore,
    runtime: tokio::runtime::Handle,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
    let mut cluster_network = ClusterNetworkMaintenance::with_idle_stats(idle_stats.clone())
        .await
        .with_service_registry(service_registry);
    let maintenance_started = std::time::Instant::now();
    let mut last_dfs_maintenance = maintenance_started
        .checked_sub(DAEMON_DFS_MAINTENANCE_INTERVAL)
        .unwrap_or(maintenance_started);
    let mut last_dfs_maintenance_probe = maintenance_started
        .checked_sub(DAEMON_DFS_MAINTENANCE_PROBE_INTERVAL)
        .unwrap_or(maintenance_started);
    let mut last_rollout_telemetry = maintenance_started
        .checked_sub(DAEMON_ROLLOUT_TELEMETRY_INTERVAL)
        .unwrap_or(maintenance_started);
    #[cfg(feature = "sandbox")]
    let mut last_sandbox_lifecycle_maintenance = maintenance_started
        .checked_sub(DAEMON_SANDBOX_LIFECYCLE_MAINTENANCE_INTERVAL)
        .unwrap_or(maintenance_started);
    let dfs_maintenance_notify = daemon_dfs_maintenance_notify();
    let cluster_maintenance_notify = daemon_cluster_maintenance_notify();
    let network_environment_watch = if label == "daemon" {
        start_daemon_network_environment_watch(&label, shutdown_rx.clone()).await
    } else {
        None
    };
    let mut dfs_triggered_maintenance_requested = false;

    loop {
        if daemon_shutdown_requested(&shutdown_rx) {
            break;
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
        if label == "daemon"
            && last_sandbox_lifecycle_maintenance.elapsed()
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

        if daemon_role_owns_default_dfs(&label)
            && !dfs_triggered_maintenance_requested
            && last_dfs_maintenance_probe.elapsed() >= DAEMON_DFS_MAINTENANCE_PROBE_INTERVAL
        {
            last_dfs_maintenance_probe = std::time::Instant::now();
            let probe_paths = paths.clone();
            match tokio::task::spawn_blocking(move || {
                daemon_dfs_pending_maintenance_work(&probe_paths)
            })
            .await
            {
                Ok(Ok(work)) if work.should_trigger_background_maintenance() => {
                    dfs_triggered_maintenance_requested = true;
                }
                Ok(Ok(_)) => {}
                Ok(Err(error)) => {
                    tracing::warn!("sinomo {label} DFS maintenance probe failed: {error}");
                }
                Err(error) => {
                    tracing::warn!("sinomo {label} DFS maintenance probe task failed: {error}");
                }
            }
        }

        if daemon_shutdown_requested(&shutdown_rx) {
            break;
        }

        if daemon_role_owns_default_dfs(&label)
            && (last_dfs_maintenance.elapsed() >= DAEMON_DFS_MAINTENANCE_INTERVAL
                || (dfs_triggered_maintenance_requested
                    && last_dfs_maintenance.elapsed()
                        >= DAEMON_DFS_TRIGGERED_MAINTENANCE_MIN_INTERVAL))
        {
            let dfs_paths = paths.clone();
            let dfs_runtime = runtime.clone();
            match run_daemon_dfs_maintenance_tick_in_blocking_task(dfs_paths, dfs_runtime).await {
                Ok(()) => {
                    let probe_paths = paths.clone();
                    match tokio::task::spawn_blocking(move || {
                        daemon_dfs_pending_maintenance_work(&probe_paths)
                    })
                    .await
                    {
                        Ok(Ok(work)) => {
                            dfs_triggered_maintenance_requested =
                                work.should_trigger_background_maintenance();
                        }
                        Ok(Err(error)) => {
                            dfs_triggered_maintenance_requested = false;
                            tracing::warn!(
                                "sinomo {label} DFS maintenance follow-up probe failed: {error}"
                            );
                        }
                        Err(error) => {
                            dfs_triggered_maintenance_requested = false;
                            tracing::warn!(
                                "sinomo {label} DFS maintenance follow-up probe task failed: {error}"
                            );
                        }
                    }
                }
                Err(error) => {
                    dfs_triggered_maintenance_requested = false;
                    tracing::warn!("sinomo {label} DFS maintenance tick failed: {error}");
                }
            }
            last_dfs_maintenance = std::time::Instant::now();
        }

        if daemon_shutdown_requested(&shutdown_rx) {
            break;
        }

        let cluster_network_phase = async {
            let pending_work_deadline = Instant::now()
                .checked_add(
                    DAEMON_CLUSTER_NETWORK_MAINTENANCE_PHASE_TIMEOUT
                        .saturating_sub(Duration::from_millis(500)),
                )
                .unwrap_or_else(Instant::now);
            match cluster_network
                .tick_async(&paths, &options, runtime.clone())
                .await
            {
                Ok(pending_work) => {
                    if let Some(pending_work) = pending_work {
                        cluster_network
                            .run_pending_work_until_async(
                                pending_work,
                                runtime.clone(),
                                pending_work_deadline,
                            )
                            .await;
                    }
                }
                Err(error) => {
                    tracing::warn!("sinomo {label} cluster network tick failed: {error}");
                }
            }
        };
        if tokio::time::timeout(
            DAEMON_CLUSTER_NETWORK_MAINTENANCE_PHASE_TIMEOUT,
            cluster_network_phase,
        )
        .await
        .is_err()
        {
            tracing::warn!(
                "sinomo {label} cluster network maintenance timed out after {}ms",
                DAEMON_CLUSTER_NETWORK_MAINTENANCE_PHASE_TIMEOUT.as_millis()
            );
            cluster_network.request_network_environment_refresh(Instant::now());
        }

        if daemon_shutdown_requested(&shutdown_rx) {
            break;
        }

        if label == "daemon"
            && last_rollout_telemetry.elapsed() >= DAEMON_ROLLOUT_TELEMETRY_INTERVAL
        {
            let telemetry_paths = paths.clone();
            match tokio::task::spawn_blocking(move || {
                run_daemon_rollout_telemetry_maintenance_tick(&telemetry_paths)
            })
            .await
            {
                Ok(Ok(Some(applied))) => tracing::info!(
                    "sinomo {label} rollout telemetry applied {:?} at {} bps",
                    applied.transition.action,
                    applied.transition.current_weight_bps
                ),
                Ok(Ok(None)) => {}
                Ok(Err(error)) => {
                    tracing::warn!("sinomo {label} rollout telemetry tick failed: {error}");
                }
                Err(error) => {
                    tracing::warn!("sinomo {label} rollout telemetry task failed: {error}");
                }
            }
            last_rollout_telemetry = std::time::Instant::now();
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
            if daemon_role_owns_default_dfs(&label) && dfs_triggered_maintenance_requested {
                Some(daemon_maintenance_interval_remaining(
                    sleep_started,
                    last_dfs_maintenance,
                    DAEMON_DFS_TRIGGERED_MAINTENANCE_MIN_INTERVAL,
                ))
            } else {
                None
            };
        let dfs_maintenance_probe_due =
            if daemon_role_owns_default_dfs(&label) && !dfs_triggered_maintenance_requested {
                Some(daemon_maintenance_interval_remaining(
                    sleep_started,
                    last_dfs_maintenance_probe,
                    DAEMON_DFS_MAINTENANCE_PROBE_INTERVAL,
                ))
            } else {
                None
            };
        #[cfg(feature = "sandbox")]
        let sandbox_lifecycle_maintenance_due = (label == "daemon").then(|| {
            daemon_maintenance_interval_remaining(
                sleep_started,
                last_sandbox_lifecycle_maintenance,
                DAEMON_SANDBOX_LIFECYCLE_MAINTENANCE_INTERVAL,
            )
        });
        #[cfg(not(feature = "sandbox"))]
        let sandbox_lifecycle_maintenance_due = None;
        let sleep_duration = daemon_maintenance_sleep_duration(
            sleep_started,
            cluster_network.next_due_duration(sleep_started),
            last_dfs_maintenance,
            last_rollout_telemetry,
            dfs_triggered_maintenance_due,
            dfs_maintenance_probe_due,
            shadow_finalization_due,
            sandbox_lifecycle_maintenance_due,
        );
        let dfs_notify =
            daemon_role_owns_default_dfs(&label).then(|| Arc::clone(&dfs_maintenance_notify));
        let network_notify = network_environment_watch
            .as_ref()
            .map(|watch| Arc::clone(&watch.notify));
        let cluster_notify = Some(Arc::clone(&cluster_maintenance_notify));
        tokio::select! {
            _ = tokio::time::sleep(sleep_duration) => {}
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    break;
                }
            }
            _ = daemon_optional_notify(dfs_notify) => {
                dfs_triggered_maintenance_requested = true;
                last_dfs_maintenance = Instant::now()
                    .checked_sub(DAEMON_DFS_TRIGGERED_MAINTENANCE_MIN_INTERVAL)
                    .unwrap_or(last_dfs_maintenance);
            }
            _ = daemon_optional_notify(network_notify) => {
                tracing::info!("sinomo {label} cluster maintenance refreshed after network environment change");
                cluster_network.request_network_environment_refresh(Instant::now());
            }
            _ = daemon_optional_notify(cluster_notify) => {
                tracing::info!("sinomo {label} cluster maintenance refresh requested");
                cluster_network.request_network_environment_refresh(Instant::now());
            }
        }
    }
    if let Some(watch) = network_environment_watch {
        let _ = watch.task.await;
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
    mut stream: sinomo_core::network::AsyncDaemonControlStream,
    paths: DaemonPaths,
    service_host: crate::service_adapter::DaemonServiceRegistryHost,
    service_runtime: tokio::runtime::Handle,
    stream_address: String,
) -> Result<bool, DaemonError> {
    loop {
        if daemon_control_stream_uses_object_rpc_session(&stream).await? {
            return handle_daemon_object_rpc_session_async(
                stream,
                paths,
                service_host,
                service_runtime,
                stream_address,
            )
            .await;
        }
        let mut request = match read_daemon_control_frame_async(&mut stream, &stream_address).await
        {
            Ok(request) => request,
            Err(_) => {
                let _ = stream.shutdown_write(&stream_address).await;
                return Ok(false);
            }
        };
        let request_received_at = Instant::now();
        let request_traceparent = request.traceparent().unwrap_or_default().to_string();
        let is_dfs_data_plane = sinomo_dfs::dfs_route_is_message_data_plane(&request.route);
        tracing::trace!(
            target: "sinomo.daemon.control_rpc",
            traceparent = %request_traceparent,
            route = %request.route,
            category = %request.category,
            stream_address = %stream_address,
            body_bytes = request.body.len(),
            auth_bytes = request.auth.as_ref().map(Vec::len).unwrap_or(0),
            dfs_data_plane = is_dfs_data_plane,
            "daemon control rpc received"
        );
        if try_handle_loopback_daemon_status_request_pre_auth(
            &request,
            &mut stream,
            &stream_address,
            &paths,
        )
        .await?
        {
            return Ok(false);
        }
        if crate::service_adapter::try_dispatch_pre_auth_via_registry_async(
            &request,
            &mut stream,
            &stream_address,
            &service_host,
        )
        .await?
        {
            request = match read_daemon_control_frame_async(&mut stream, &stream_address).await {
                Ok(request) => request,
                Err(_) => return Ok(false),
            };
        }

        let replay_key = daemon_dfs_message_replay_key(&request);
        if let Some(key) = replay_key.as_ref()
            && let Some(response) = wait_for_daemon_dfs_message_replay(&paths, key).await
        {
            tracing::trace!(
                target: "sinomo.daemon.control_rpc",
                traceparent = %request_traceparent,
                route = %request.route,
                category = %request.category,
                stream_address = %stream_address,
                elapsed_ms = request_received_at.elapsed().as_millis(),
                "daemon control rpc served from DFS replay before auth"
            );
            let _ = write_daemon_rpc_response_frame_async_for_control_request(
                &mut stream,
                &stream_address,
                &paths,
                &request,
                response,
            )
            .await;
            let _ = stream.shutdown_write(&stream_address).await;
            return Ok(false);
        }

        let route = request.route.trim().to_string();
        let auth_started = Instant::now();
        let authenticated =
            match crate::service_adapter::authenticate_daemon_control_request_via_registry(
                &paths,
                &request,
                &service_host,
            ) {
                Ok(authenticated) => authenticated,
                Err(error) => {
                    tracing::debug!(
                        target: "sinomo.daemon.control_rpc",
                        traceparent = %request_traceparent,
                        route = %request.route,
                        category = %request.category,
                        stream_address = %stream_address,
                        elapsed_ms = request_received_at.elapsed().as_millis(),
                        auth_ms = auth_started.elapsed().as_millis(),
                        error = %error,
                        "daemon control rpc auth failed"
                    );
                    if crate::auth::is_cluster_join_register_control_request(&request) {
                        tracing::warn!("sinomo daemon client auth failed for {route}: {error}");
                    }
                    if let Some(key) = replay_key.as_ref()
                        && let Some(response) =
                            wait_for_daemon_dfs_message_replay(&paths, key).await
                    {
                        tracing::trace!(
                            target: "sinomo.daemon.control_rpc",
                            traceparent = %request_traceparent,
                            route = %request.route,
                            category = %request.category,
                            stream_address = %stream_address,
                            elapsed_ms = request_received_at.elapsed().as_millis(),
                            "daemon control rpc served from DFS replay after auth failure"
                        );
                        let _ = write_daemon_rpc_response_frame_async_for_control_request(
                            &mut stream,
                            &stream_address,
                            &paths,
                            &request,
                            response,
                        )
                        .await;
                        let _ = stream.shutdown_write(&stream_address).await;
                        return Ok(false);
                    }
                    if let Ok(response) = sinomo_core::network::ObjectRpcResponse::encode_error(
                        request.route.clone(),
                        DaemonError::AuthDenied.to_string(),
                    ) {
                        let _ = write_daemon_rpc_response_frame_async_for_rpc_request(
                            &mut stream,
                            &stream_address,
                            &paths,
                            &request,
                            response,
                        )
                        .await;
                    }
                    let _ = stream.shutdown_write(&stream_address).await;
                    return Ok(false);
                }
            };
        tracing::trace!(
            target: "sinomo.daemon.control_rpc",
            traceparent = %request_traceparent,
            route = %request.route,
            category = %request.category,
            stream_address = %stream_address,
            signer_node_id = %authenticated.signer_node_id,
            elapsed_ms = request_received_at.elapsed().as_millis(),
            auth_ms = auth_started.elapsed().as_millis(),
            "daemon control rpc authenticated"
        );

        if let Some(key) = replay_key.as_ref() {
            begin_daemon_dfs_message_replay(&paths, key);
            tracing::trace!(
                target: "sinomo.daemon.control_rpc",
                traceparent = %request_traceparent,
                route = %request.route,
                category = %request.category,
                stream_address = %stream_address,
                elapsed_ms = request_received_at.elapsed().as_millis(),
                "daemon control rpc began DFS replay"
            );
        }
        if request.route == sinomo_core::network::CLUSTER_TCP_TUNNEL_OPEN_ROUTE {
            crate::tcp_tunnel::accept_tcp_tunnel_connection_async(
                stream,
                paths,
                service_runtime,
                stream_address.clone(),
                authenticated,
                request,
            )
            .await?;
            return Ok(false);
        }
        let rpc_request = Some(request);
        if let Some(request) = rpc_request.as_ref()
            && request.route == sinomo_core::network::CLUSTER_NETWORK_PING_ROUTE
        {
            let response = daemon_cluster_network_ping_response(&paths, request.clone())?;
            write_daemon_rpc_response_frame_async_for_rpc_request(
                &mut stream,
                &stream_address,
                &paths,
                request,
                response,
            )
            .await?;
            continue;
        }
        if let Some(request) = rpc_request.as_ref()
            && request.route == sinomo_core::network::CLUSTER_RELAY_ROUTE
        {
            let response = match handle_daemon_cluster_relay_request_on_runtime(
                &paths,
                request,
                Some(&service_runtime),
            )
            .await
            {
                Ok(response) => response,
                Err(error) => sinomo_core::network::ObjectRpcResponse::encode_error(
                    sinomo_core::network::CLUSTER_RELAY_ROUTE,
                    error.to_string(),
                )
                .map_err(DaemonError::from)?,
            };
            write_daemon_rpc_response_frame_async_for_rpc_request(
                &mut stream,
                &stream_address,
                &paths,
                request,
                response,
            )
            .await?;
            continue;
        }
        return handle_authenticated_daemon_command_async(
            authenticated,
            rpc_request,
            replay_key,
            stream,
            paths,
            service_host,
            service_runtime,
            stream_address.clone(),
        )
        .await;
    }
}

async fn daemon_control_stream_uses_object_rpc_session(
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
            return Ok(sinomo_core::network::object_rpc_session_wire_prefix_matches(&prefix));
        }
        // A normal length-prefixed object frame starts with a zero high byte
        // under the compatibility frame limit. Only a partial session magic
        // needs another packet before protocol selection is safe.
        if prefix[..available] != b"SXFR"[..available] {
            return Ok(false);
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

async fn handle_daemon_object_rpc_session_async(
    stream: sinomo_core::network::AsyncDaemonControlStream,
    paths: DaemonPaths,
    service_host: crate::service_adapter::DaemonServiceRegistryHost,
    service_runtime: tokio::runtime::Handle,
    _stream_address: String,
) -> Result<bool, DaemonError> {
    handle_daemon_object_rpc_stream_async(
        Box::new(stream.into_inner()),
        paths,
        service_host,
        service_runtime,
    )
    .await
}

/// Runs one transport-neutral canonical object-RPC session through the same
/// authentication, authorization, registry, relay, and response path used by
/// a directly accepted daemon TCP connection.
pub(crate) async fn handle_daemon_object_rpc_stream_async(
    stream: Box<dyn sinomo_core::network::NetworkDuplexStream>,
    paths: DaemonPaths,
    service_host: crate::service_adapter::DaemonServiceRegistryHost,
    service_runtime: tokio::runtime::Handle,
) -> Result<bool, DaemonError> {
    let data_dir = sinomo_core::local::SinomoDataDir::new(daemon_data_dir_path(&paths)?);
    let authenticated = Arc::new(Mutex::new(None::<AuthenticatedDaemonCommand>));
    let authenticated_slot = Arc::clone(&authenticated);
    let auth_paths = paths.clone();
    let auth_service_host = service_host.clone();
    let (request, responder) = sinomo_core::network::accept_object_rpc_server_session(
        stream,
        data_dir,
        sinomo_core::network::NetworkCallPolicy::default(),
        move |request, claimed_link_node_id, claimed_link_public_key| {
            let command = crate::service_adapter::authenticate_daemon_control_request_via_registry(
                &auth_paths,
                request,
                &auth_service_host,
            )
            .map_err(|_| sinomo_core::network::NetworkError::AuthDenied)?;
            authorize_daemon_session_link_peer(
                &auth_paths,
                request,
                &command,
                claimed_link_node_id,
                claimed_link_public_key,
            )?;
            *authenticated_slot
                .lock()
                .map_err(|_| sinomo_core::network::NetworkError::Io {
                    operation: "record authenticated object RPC session".to_string(),
                    detail: "authenticated session slot lock poisoned".to_string(),
                })? = Some(command);
            Ok(())
        },
    )
    .await
    .map_err(DaemonError::from)?;
    let authenticated = authenticated
        .lock()
        .map_err(|_| DaemonError::Parse {
            detail: "authenticated object RPC session slot lock poisoned".to_string(),
        })?
        .take()
        .ok_or(DaemonError::AuthDenied)?;

    let route = request.request.route.clone();
    let dispatch = async {
        if route == sinomo_core::network::CLUSTER_NETWORK_PING_ROUTE {
            if !request.body.is_empty() {
                return Err(DaemonError::Parse {
                    detail: "cluster network ping does not accept an attached body".to_string(),
                });
            }
            Ok((
                sinomo_core::network::ObjectRpcSessionResponse::buffered(
                    daemon_cluster_network_ping_response(&paths, request.request)?,
                ),
                false,
                route.clone(),
            ))
        } else if route == sinomo_core::network::CLUSTER_RELAY_ROUTE {
            handle_daemon_cluster_relay_session_async(
                &paths,
                &service_host,
                &service_runtime,
                request,
            )
            .await
        } else if crate::service_adapter::is_sandbox_stream_route(&request.request) {
            if !request.body.is_empty() {
                return Err(DaemonError::Parse {
                    detail: "sandbox stream session does not accept an attached request body"
                        .to_string(),
                });
            }
            Ok((
                daemon_sandbox_stream_session_response(&paths, request.request, &service_runtime)?,
                false,
                route.clone(),
            ))
        } else {
            crate::service_adapter::dispatch_rpc_via_registry_to_session_response_async(
                &service_host,
                &authenticated,
                request,
                Some(&paths),
                None,
            )
            .await
            .map(|(response, stop)| (response, stop, route.clone()))
        }
    }
    .await;
    let (response, stop, effective_route) = match dispatch {
        Ok(response) => response,
        Err(error) => (
            sinomo_core::network::ObjectRpcSessionResponse::buffered(
                sinomo_core::network::ObjectRpcResponse::encode_error(&route, error.to_string())?,
            ),
            false,
            route.clone(),
        ),
    };
    responder
        .respond(response)
        .await
        .map_err(DaemonError::from)?;
    crate::service_adapter::mark_cluster_identity_restart_after_response(
        &effective_route,
        stop,
        Some(&paths),
    );
    Ok(stop)
}

fn daemon_sandbox_stream_session_response(
    paths: &DaemonPaths,
    request: sinomo_core::network::ObjectRpcRequest,
    runtime: &tokio::runtime::Handle,
) -> Result<sinomo_core::network::ObjectRpcSessionResponse, DaemonError> {
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
    Ok(sinomo_core::network::ObjectRpcSessionResponse::new(
        response, body,
    ))
}

fn authorize_daemon_session_link_peer(
    paths: &DaemonPaths,
    request: &sinomo_core::network::ObjectRpcRequest,
    authenticated: &AuthenticatedDaemonCommand,
    claimed_link_node_id: &str,
    claimed_link_public_key: &[u8],
) -> Result<(), sinomo_core::network::NetworkError> {
    let origin_public_key =
        sinomo_core::network::daemon_auth_decode_public_key(&authenticated.signer_public_key)?;
    if authenticated.signer_node_id == claimed_link_node_id
        && origin_public_key == claimed_link_public_key
    {
        return Ok(());
    }
    if request.route == sinomo_core::network::CLUSTER_RELAY_ROUTE {
        // The outer relay head is freshly signed by the immediate hop. Only a
        // final-hop inner request may retain an origin signature while using a
        // different, separately authorized link signer.
        return Err(sinomo_core::network::NetworkError::AuthDenied);
    }
    let cluster_paths = cluster_paths_from_daemon_paths(paths).map_err(|error| {
        sinomo_core::network::NetworkError::Io {
            operation: "authorize daemon session link peer".to_string(),
            detail: error.to_string(),
        }
    })?;
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)?;
    let authorized = state.members.iter().any(|member| {
        member.state.is_authorized()
            && member.node_id.as_str() == claimed_link_node_id
            && sinomo_core::network::daemon_auth_decode_public_key(&member.node_public_key)
                .is_ok_and(|key| key == claimed_link_public_key)
    });
    if authorized {
        Ok(())
    } else {
        Err(sinomo_core::network::NetworkError::AuthDenied)
    }
}

async fn handle_daemon_cluster_relay_session_async(
    paths: &DaemonPaths,
    service_host: &crate::service_adapter::DaemonServiceRegistryHost,
    service_runtime: &tokio::runtime::Handle,
    request: sinomo_core::network::ObjectRpcSessionRequest,
) -> Result<(sinomo_core::network::ObjectRpcSessionResponse, bool, String), DaemonError> {
    let cluster_paths = cluster_paths_from_daemon_paths(paths)?;
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
        .map_err(DaemonError::from)?;
    match sinomo_core::network::cluster_relay_session_request_action(&state, &request.request)
        .map_err(DaemonError::from)?
    {
        sinomo_core::network::ClusterRelaySessionRequestAction::DeliverLocal(inner_request) => {
            let effective_route = daemon_object_rpc_effective_route(
                sinomo_core::network::CLUSTER_RELAY_ROUTE,
                Some(&inner_request.route),
            );
            let request = request
                .bind_signed_inner_request(inner_request)
                .map_err(DaemonError::from)?;
            let authenticated =
                crate::service_adapter::authenticate_daemon_control_request_via_registry(
                    paths,
                    &request.request,
                    service_host,
                )?;
            crate::service_adapter::dispatch_rpc_via_registry_to_session_response_async(
                service_host,
                &authenticated,
                request,
                Some(paths),
                None,
            )
            .await
            .map(|(response, stop)| (response, stop, effective_route))
        }
        sinomo_core::network::ClusterRelaySessionRequestAction::Forward => {
            let mut host =
                crate::client::DaemonSignedObjectRpcAsyncHost::new(service_runtime, true)
                    .with_paths(paths);
            let mut client = sinomo_core::network::ClusterObjectRpcClient::routed(
                &mut host,
                service_runtime,
                &cluster_paths,
            );
            match client
                .forward_relay_session(
                    &state,
                    request,
                    sinomo_core::network::ClusterObjectRpcOptions::default(),
                )
                .await
            {
                Ok(result) => Ok((
                    sinomo_core::network::ObjectRpcSessionResponse::new(
                        result.response,
                        sinomo_core::network::ObjectRpcBodySource::from_incoming_body(result.body),
                    ),
                    false,
                    sinomo_core::network::CLUSTER_RELAY_ROUTE.to_string(),
                )),
                Err(error) => Ok((
                    sinomo_core::network::ObjectRpcSessionResponse::buffered(
                        sinomo_core::network::ObjectRpcResponse::encode_error(
                            sinomo_core::network::CLUSTER_RELAY_ROUTE,
                            format!("relay dropped while forwarding session: {error}"),
                        )
                        .map_err(DaemonError::from)?,
                    ),
                    false,
                    sinomo_core::network::CLUSTER_RELAY_ROUTE.to_string(),
                )),
            }
        }
    }
}

fn daemon_object_rpc_effective_route(
    outer_route: &str,
    delivered_inner_route: Option<&str>,
) -> String {
    delivered_inner_route.unwrap_or(outer_route).to_string()
}

async fn try_handle_loopback_daemon_status_request_pre_auth(
    request: &sinomo_core::network::ObjectRpcRequest,
    stream: &mut sinomo_core::network::AsyncDaemonControlStream,
    stream_address: &str,
    paths: &DaemonPaths,
) -> Result<bool, DaemonError> {
    if request.route != crate::DAEMON_STATUS_ROUTE {
        return Ok(false);
    }
    if !stream
        .peer_addr()
        .map(|address| address.ip().is_loopback())
        .unwrap_or(false)
    {
        return Ok(false);
    }
    let _: () = request.decode_body().map_err(DaemonError::from)?;
    let status = daemon_liveness_status_response_async(paths).await?;
    let response =
        sinomo_core::network::ObjectRpcResponse::encode_body(crate::DAEMON_STATUS_ROUTE, &status)
            .map_err(DaemonError::from)?;
    write_daemon_rpc_response_frame_async(stream, stream_address, response).await?;
    Ok(true)
}

async fn daemon_liveness_status_response_async(
    paths: &DaemonPaths,
) -> Result<crate::DaemonStatus, DaemonError> {
    daemon_status_response_async(paths, None).await
}

pub(crate) async fn dispatch_daemon_control_request_in_process_async(
    paths: DaemonPaths,
    service_runtime: tokio::runtime::Handle,
    request: sinomo_core::network::DaemonControlRequest,
) -> Result<sinomo_core::network::ObjectRpcResponse, DaemonError> {
    let replay_key = daemon_dfs_message_replay_key(&request);
    if let Some(key) = replay_key.as_ref()
        && let Some(response) = wait_for_daemon_dfs_message_replay(&paths, key).await
    {
        return Ok(response);
    }

    let service_host = crate::service_adapter::DaemonServiceRegistryHost::new_on_runtime(
        paths.clone(),
        service_runtime,
    )
    .await?;

    if request.route == sinomo_core::network::CLUSTER_JOIN_CHALLENGE_ROUTE
        && request.category == sinomo_core::network::CLUSTER_MEMBERSHIP_RPC_CATEGORY
    {
        if let Some(response) =
            crate::service_adapter::dispatch_pre_auth_via_registry_to_response_async(
                &request,
                &service_host,
            )
            .await?
        {
            return Ok(response);
        }
    }

    let authenticated =
        match crate::service_adapter::authenticate_daemon_control_request_via_registry(
            &paths,
            &request,
            &service_host,
        ) {
            Ok(authenticated) => authenticated,
            Err(error) => {
                if let Some(key) = replay_key.as_ref()
                    && let Some(response) = wait_for_daemon_dfs_message_replay(&paths, key).await
                {
                    return Ok(response);
                }
                let detail = error.to_string();
                return sinomo_core::network::ObjectRpcResponse::encode_error(
                    request.route.clone(),
                    detail,
                )
                .map_err(DaemonError::from);
            }
        };
    if let Some(key) = replay_key.as_ref() {
        begin_daemon_dfs_message_replay(&paths, key);
    }

    let rpc_request = request;
    if crate::service_adapter::is_sandbox_stream_route(&rpc_request) {
        if let Some(key) = replay_key.as_ref() {
            abandon_daemon_dfs_message_replay(&paths, key);
        }
        return Err(DaemonError::Network {
            address: rpc_request.route,
            detail: "sandbox stream routes cannot be delivered through cluster relay".to_string(),
        });
    }
    if rpc_request.route == sinomo_core::network::CLUSTER_NETWORK_PING_ROUTE {
        return daemon_cluster_network_ping_response(&paths, rpc_request);
    }
    if service_host
        .object_rpc_route_descriptor(&rpc_request.route)
        .is_none()
    {
        if let Some(key) = replay_key.as_ref() {
            abandon_daemon_dfs_message_replay(&paths, key);
        }
        return Err(DaemonError::Network {
            address: rpc_request.route,
            detail: "in-process daemon dispatch has no registered object RPC route".to_string(),
        });
    }
    match crate::service_adapter::dispatch_rpc_via_registry_to_response_async(
        &service_host,
        &authenticated,
        rpc_request.clone(),
        Some(&paths),
        replay_key.clone(),
    )
    .await
    {
        Ok((response, _stop)) => Ok(response),
        Err(error) => {
            if let Some(key) = replay_key.as_ref() {
                abandon_daemon_dfs_message_replay(&paths, key);
            }
            sinomo_core::network::ObjectRpcResponse::encode_error(
                rpc_request.route,
                error.to_string(),
            )
            .map_err(DaemonError::from)
        }
    }
}

fn daemon_cluster_network_ping_response(
    paths: &DaemonPaths,
    request: sinomo_core::network::ObjectRpcRequest,
) -> Result<sinomo_core::network::ObjectRpcResponse, DaemonError> {
    let data_dir = sinomo_core::local::SinomoDataDir::new(daemon_data_dir_path(paths)?);
    let node_id = crate::service_adapter::cached_cluster_network_ping_node_id(paths, &data_dir)?;
    sinomo_core::cluster::handle_cluster_network_ping_rpc_request_for_node(
        &node_id,
        request,
        time::OffsetDateTime::now_utc(),
    )
    .map_err(DaemonError::from)
}

pub(crate) fn daemon_dfs_message_replay_key(
    request: &sinomo_core::network::DaemonControlRequest,
) -> Option<Vec<u8>> {
    if !sinomo_dfs::dfs_route_is_message_data_plane(&request.route) {
        return None;
    }
    let auth = request.auth.as_ref()?;
    let mut key = Vec::with_capacity(request.route.len() + 1 + auth.len());
    key.extend_from_slice(request.route.as_bytes());
    key.push(0);
    key.extend_from_slice(auth);
    Some(key)
}

fn daemon_dfs_message_replay_cache()
-> &'static Mutex<BTreeMap<PathBuf, BTreeMap<Vec<u8>, DaemonDfsMessageReplayEntry>>> {
    DAEMON_DFS_MESSAGE_REPLAY_CACHE.get_or_init(|| Mutex::new(BTreeMap::new()))
}

fn prune_daemon_dfs_message_replay_cache(
    cache: &mut BTreeMap<PathBuf, BTreeMap<Vec<u8>, DaemonDfsMessageReplayEntry>>,
) {
    let now = Instant::now();
    cache.retain(|_, entries| {
        entries.retain(|_, entry| {
            now.saturating_duration_since(entry.updated_at) <= DAEMON_DFS_MESSAGE_REPLAY_RETENTION
        });
        !entries.is_empty()
    });
}

fn daemon_dfs_message_response_is_replay_cacheable(
    response: &sinomo_core::network::ObjectRpcResponse,
) -> bool {
    !sinomo_dfs::dfs_route_is_message_data_plane(&response.route)
        || response.body.len() < DAEMON_DFS_MESSAGE_REPLAY_MAX_CACHED_BODY_BYTES
}

async fn wait_for_daemon_dfs_message_replay(
    paths: &DaemonPaths,
    key: &[u8],
) -> Option<sinomo_core::network::ObjectRpcResponse> {
    let started = Instant::now();
    loop {
        let notify = {
            let mut cache = daemon_dfs_message_replay_cache().lock().ok()?;
            prune_daemon_dfs_message_replay_cache(&mut cache);
            let entry = cache.get(&paths.root)?.get(key)?;
            match &entry.state {
                DaemonDfsMessageReplayState::Complete(response) => return Some(response.clone()),
                DaemonDfsMessageReplayState::InFlight => Arc::clone(&entry.notify),
            }
        };
        let remaining = DAEMON_DFS_MESSAGE_REPLAY_WAIT.checked_sub(started.elapsed())?;
        if tokio::time::timeout(remaining, notify.notified())
            .await
            .is_err()
        {
            return None;
        }
    }
}

fn begin_daemon_dfs_message_replay(paths: &DaemonPaths, key: &[u8]) {
    let Ok(mut cache) = daemon_dfs_message_replay_cache().lock() else {
        return;
    };
    prune_daemon_dfs_message_replay_cache(&mut cache);
    cache
        .entry(paths.root.clone())
        .or_default()
        .entry(key.to_vec())
        .or_insert_with(|| DaemonDfsMessageReplayEntry {
            state: DaemonDfsMessageReplayState::InFlight,
            notify: Arc::new(tokio::sync::Notify::new()),
            updated_at: Instant::now(),
        });
}

pub(crate) fn complete_daemon_dfs_message_replay(
    paths: &DaemonPaths,
    key: &[u8],
    response: &sinomo_core::network::ObjectRpcResponse,
) {
    let notify = {
        let Ok(mut cache) = daemon_dfs_message_replay_cache().lock() else {
            return;
        };
        prune_daemon_dfs_message_replay_cache(&mut cache);
        if !daemon_dfs_message_response_is_replay_cacheable(response) {
            let Some(entries) = cache.get_mut(&paths.root) else {
                return;
            };
            let Some(entry) = entries.remove(key) else {
                return;
            };
            let notify = entry.notify;
            if entries.is_empty() {
                cache.remove(&paths.root);
            }
            tracing::debug!(
                target: "sinomo.daemon.dfs",
                route = %response.route,
                body_bytes = response.body.len(),
                "skipped DFS message replay cache for large response"
            );
            notify
        } else {
            let entries = cache.entry(paths.root.clone()).or_default();
            let entry =
                entries
                    .entry(key.to_vec())
                    .or_insert_with(|| DaemonDfsMessageReplayEntry {
                        state: DaemonDfsMessageReplayState::InFlight,
                        notify: Arc::new(tokio::sync::Notify::new()),
                        updated_at: Instant::now(),
                    });
            entry.state = DaemonDfsMessageReplayState::Complete(response.clone());
            entry.updated_at = Instant::now();
            Arc::clone(&entry.notify)
        }
    };
    notify.notify_waiters();
}

pub(crate) fn abandon_daemon_dfs_message_replay(paths: &DaemonPaths, key: &[u8]) {
    let notify = {
        let Ok(mut cache) = daemon_dfs_message_replay_cache().lock() else {
            return;
        };
        let Some(entries) = cache.get_mut(&paths.root) else {
            return;
        };
        let Some(entry) = entries.remove(key) else {
            return;
        };
        if entries.is_empty() {
            cache.remove(&paths.root);
        }
        entry.notify
    };
    notify.notify_waiters();
}

async fn handle_authenticated_daemon_command_async(
    authenticated: AuthenticatedDaemonCommand,
    rpc_request: Option<sinomo_core::network::ObjectRpcRequest>,
    replay_key: Option<Vec<u8>>,
    mut stream: sinomo_core::network::AsyncDaemonControlStream,
    paths: DaemonPaths,
    service_host: crate::service_adapter::DaemonServiceRegistryHost,
    service_runtime: tokio::runtime::Handle,
    stream_address: String,
) -> Result<bool, DaemonError> {
    if let Some(request) = rpc_request.as_ref()
        && request.route == sinomo_core::network::CLUSTER_NETWORK_PING_ROUTE
    {
        let response = daemon_cluster_network_ping_response(&paths, request.clone())?;
        write_daemon_rpc_response_frame_async_for_rpc_request(
            &mut stream,
            &stream_address,
            &paths,
            request,
            response,
        )
        .await?;
        return Ok(false);
    }
    if let Some(request) = rpc_request.as_ref()
        && request.route == sinomo_core::network::CLUSTER_RELAY_ROUTE
    {
        let response = match handle_daemon_cluster_relay_request_on_runtime(
            &paths,
            request,
            Some(&service_runtime),
        )
        .await
        {
            Ok(response) => response,
            Err(error) => sinomo_core::network::ObjectRpcResponse::encode_error(
                sinomo_core::network::CLUSTER_RELAY_ROUTE,
                error.to_string(),
            )
            .map_err(DaemonError::from)?,
        };
        write_daemon_rpc_response_frame_async_for_rpc_request(
            &mut stream,
            &stream_address,
            &paths,
            request,
            response,
        )
        .await?;
        return Ok(false);
    }
    if let Some(result) = crate::service_adapter::try_dispatch_via_registry_async(
        &authenticated,
        &rpc_request,
        &mut stream,
        &stream_address,
        &service_host,
        &paths,
        replay_key.clone(),
    )
    .await
    {
        if let Err(error) = &result
            && let Some(response) = rpc_error_response_for_request(&rpc_request, error)
        {
            let _ =
                write_daemon_rpc_response_frame_async(&mut stream, &stream_address, response).await;
        }
        if result.is_err()
            && let Some(key) = replay_key.as_ref()
        {
            abandon_daemon_dfs_message_replay(&paths, key);
        }
        return match result {
            Ok(stop) => Ok(stop),
            Err(_) => Ok(false),
        };
    }

    let route = authenticated.route.as_str();
    let error_response_request = rpc_request.clone();
    let result = match route {
        route if route == ROLLOUT_FINALIZE_SHADOW_ROUTE => {
            let request = require_daemon_rpc_request(
                rpc_request,
                ROLLOUT_FINALIZE_SHADOW_ROUTE,
                ROLLOUT_RPC_CATEGORY,
            )?;
            let _: () = request.decode_body().map_err(DaemonError::from)?;
            let response = tokio::task::spawn_blocking({
                let paths = paths.clone();
                move || handle_daemon_finalize_shadow_rollout(&paths)
            })
            .await
            .map_err(|source| DaemonError::Network {
                address: "daemon rollout finalization".to_string(),
                detail: format!("rollout finalization task failed: {source}"),
            })??;
            let response = sinomo_core::network::ObjectRpcResponse::encode_body(
                ROLLOUT_FINALIZE_SHADOW_ROUTE,
                &response,
            )
            .map_err(DaemonError::from)?;
            write_daemon_rpc_response_frame_async(&mut stream, &stream_address, response).await?;
            Ok(false)
        }
        route if route == ROLLOUT_CLEAR_STATUS_ROUTE => {
            let request = require_daemon_rpc_request(
                rpc_request,
                ROLLOUT_CLEAR_STATUS_ROUTE,
                ROLLOUT_RPC_CATEGORY,
            )?;
            let _: () = request.decode_body().map_err(DaemonError::from)?;
            let response = tokio::task::spawn_blocking({
                let paths = paths.clone();
                move || handle_daemon_clear_rollout_status(&paths)
            })
            .await
            .map_err(|source| DaemonError::Network {
                address: "daemon rollout clear-status".to_string(),
                detail: format!("rollout clear-status task failed: {source}"),
            })??;
            let response = sinomo_core::network::ObjectRpcResponse::encode_body(
                ROLLOUT_CLEAR_STATUS_ROUTE,
                &response,
            )
            .map_err(DaemonError::from)?;
            write_daemon_rpc_response_frame_async(&mut stream, &stream_address, response).await?;
            Ok(false)
        }
        _ => Ok(false),
    };
    if let Err(error) = &result
        && let Some(response) = rpc_error_response_for_request(&error_response_request, error)
    {
        let _ = write_daemon_rpc_response_frame_async(&mut stream, &stream_address, response).await;
    }
    if result.is_err()
        && let Some(key) = replay_key.as_ref()
    {
        abandon_daemon_dfs_message_replay(&paths, key);
    }
    result
}

async fn read_daemon_control_frame_async(
    stream: &mut sinomo_core::network::AsyncDaemonControlStream,
    address: &str,
) -> Result<sinomo_core::network::DaemonControlRequest, DaemonError> {
    stream
        .read_request(
            address,
            DAEMON_COMMAND_MAX_BYTES,
            sinomo_core::network::ObjectFrameIoOptions::default(),
        )
        .await
        .map_err(DaemonError::from)
}

async fn write_daemon_rpc_response_frame_async(
    stream: &mut sinomo_core::network::AsyncDaemonControlStream,
    address: &str,
    response: sinomo_core::network::ObjectRpcResponse,
) -> Result<(), DaemonError> {
    stream
        .write_response(
            address,
            &response,
            sinomo_core::network::ObjectFrameIoOptions::default(),
        )
        .await
        .map_err(DaemonError::from)
}

async fn write_daemon_rpc_response_frame_async_for_control_request(
    stream: &mut sinomo_core::network::AsyncDaemonControlStream,
    address: &str,
    paths: &DaemonPaths,
    _request: &sinomo_core::network::DaemonControlRequest,
    mut response: sinomo_core::network::ObjectRpcResponse,
) -> Result<(), DaemonError> {
    crate::service_adapter::mark_daemon_rpc_response_responder(paths, &mut response);
    write_daemon_rpc_response_frame_async(stream, address, response).await
}

async fn write_daemon_rpc_response_frame_async_for_rpc_request(
    stream: &mut sinomo_core::network::AsyncDaemonControlStream,
    address: &str,
    paths: &DaemonPaths,
    _request: &sinomo_core::network::ObjectRpcRequest,
    mut response: sinomo_core::network::ObjectRpcResponse,
) -> Result<(), DaemonError> {
    crate::service_adapter::mark_daemon_rpc_response_responder(paths, &mut response);
    write_daemon_rpc_response_frame_async(stream, address, response).await
}

fn rpc_error_response_for_request(
    request: &Option<sinomo_core::network::ObjectRpcRequest>,
    error: &DaemonError,
) -> Option<sinomo_core::network::ObjectRpcResponse> {
    let route = request.as_ref()?.route.clone();
    sinomo_core::network::ObjectRpcResponse::encode_error(route, error.to_string()).ok()
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
    now: Instant,
    cluster_next_due: Duration,
    last_dfs_maintenance: Instant,
    last_rollout_telemetry: Instant,
    dfs_triggered_maintenance_due: Option<Duration>,
    dfs_maintenance_probe_due: Option<Duration>,
    shadow_finalization_due: Option<Duration>,
    sandbox_lifecycle_maintenance_due: Option<Duration>,
) -> Duration {
    let mut next_due = cluster_next_due;
    next_due = next_due
        .min(daemon_maintenance_interval_remaining(
            now,
            last_dfs_maintenance,
            DAEMON_DFS_MAINTENANCE_INTERVAL,
        ))
        .min(daemon_maintenance_interval_remaining(
            now,
            last_rollout_telemetry,
            DAEMON_ROLLOUT_TELEMETRY_INTERVAL,
        ));
    if let Some(dfs_triggered_maintenance_due) = dfs_triggered_maintenance_due {
        next_due = next_due.min(dfs_triggered_maintenance_due);
    }
    if let Some(dfs_maintenance_probe_due) = dfs_maintenance_probe_due {
        next_due = next_due.min(dfs_maintenance_probe_due);
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

fn require_daemon_rpc_request(
    request: Option<sinomo_core::network::ObjectRpcRequest>,
    expected_route: &str,
    expected_category: &str,
) -> Result<sinomo_core::network::ObjectRpcRequest, DaemonError> {
    let request = request.ok_or_else(|| DaemonError::Parse {
        detail: format!("missing daemon rpc request for {expected_route}"),
    })?;
    if request.route != expected_route || request.category != expected_category {
        return Err(DaemonError::Parse {
            detail: format!(
                "daemon rpc route mismatch: expected {expected_category}:{expected_route}, got {}:{}",
                request.category, request.route
            ),
        });
    }
    Ok(request)
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

pub(crate) fn handle_daemon_sandbox_create_on_runtime(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxCreateInstanceRequest,
    runtime: Option<&tokio::runtime::Handle>,
) -> Result<sinomo_sandbox::SandboxCreateInstanceResponse, DaemonError> {
    let config = load_daemon_sandbox_policy_config(paths)?;
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
    if let Some(memory_mb) = request.memory_mb {
        manifest.limits.memory_mb = memory_mb;
    }
    if let Some(wall_time_ms) = request.wall_time_ms {
        manifest.limits.wall_time_ms = wall_time_ms;
    }
    if let Some(max_input_bytes) = request.max_input_bytes {
        manifest.limits.max_input_bytes = max_input_bytes;
    }
    if let Some(max_output_bytes) = request.max_output_bytes {
        manifest.limits.max_output_bytes = max_output_bytes;
    }
    if let Some(max_workspace_bytes) = request.max_workspace_bytes {
        manifest.limits.max_workspace_bytes = max_workspace_bytes;
    }
    if let Some(max_written_bytes_per_command) = request.max_written_bytes_per_command {
        manifest.limits.max_written_bytes_per_command = max_written_bytes_per_command;
    }
    if let Some(max_file_count) = request.max_file_count {
        manifest.limits.max_file_count = max_file_count;
    }
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
fn run_daemon_sandbox_lifecycle_maintenance_tick(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
) -> Result<usize, DaemonError> {
    let mut recovered = 0_usize;
    let dfs = open_daemon_sandbox_dfs_service_on_runtime(paths, Some(runtime))?;
    let mut authoritative_store = sinomo_sandbox::SifsSandboxInstanceStore::new(dfs);
    let pending_manifests = authoritative_store.list_pending_authoritative_manifests()?;
    for manifest in pending_manifests {
        let sandbox_id = manifest.sandbox_id.clone();
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
            Ok(()) => recovered = recovered.saturating_add(1),
            Err(error) => tracing::warn!(
                sandbox_id = %sandbox_id,
                error = %error,
                "authoritative sandbox transaction remains fenced for retry"
            ),
        }
    }
    Ok(recovered)
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
    let dfs = crate::dfs::with_daemon_dfs_control_plane_on_runtime(dfs, &cluster_paths, runtime);
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

pub fn run_daemon_rollout_telemetry_maintenance_tick(
    paths: &DaemonPaths,
) -> Result<Option<AppliedRolloutTransition>, DaemonError> {
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
    let applied = match result.status {
        RolloutTelemetryAdvanceStatus::AppliedTransition => result.applied_transition,
        RolloutTelemetryAdvanceStatus::NoRuntimeState
        | RolloutTelemetryAdvanceStatus::NoCandidate
        | RolloutTelemetryAdvanceStatus::TerminalStage
        | RolloutTelemetryAdvanceStatus::WaitingForTelemetry
        | RolloutTelemetryAdvanceStatus::PromotionGateBlocked => None,
    };
    publish_daemon_module_rollout_status_from_data_dir(&data_dir, &rollout_paths)?;
    run_daemon_module_rollout_desired_reconciliation_tick(paths)?;
    Ok(applied)
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
        cluster_id: cluster_state.cluster_id,
        node_id: cluster_state.local_node_id,
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
    sinomo_rollout::sync_cluster_module_rollout_node_to_kv(&cluster_paths, &record)?;
    Ok(Some(record))
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
    let desired_records =
        sinomo_rollout::read_cluster_module_rollout_desired_from_kv(&cluster_paths)?;
    if desired_records.is_empty() {
        return Ok(Vec::new());
    }

    let rollout_paths = SelfRolloutPaths::new(&data_dir);
    let runtime_state = sinomo_rollout::read_rollout_runtime_state(&rollout_paths)?;
    let existing_reconciliations: BTreeMap<String, ClusterModuleRolloutReconciliationRecord> =
        sinomo_rollout::read_cluster_module_rollout_reconciliations_from_kv(&cluster_paths)?
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
        sinomo_rollout::sync_cluster_module_rollout_reconciliation_to_kv(
            &cluster_paths,
            &reconciliation,
        )?;
        reconciliations.push(reconciliation);
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
    use std::fs;
    use std::net::{TcpListener, TcpStream};
    use std::time::{Duration, Instant};

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
                sinomo_core::network::DAEMON_CONTROL_RPC_MAX_FRAME_BYTES,
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

    #[test]
    fn local_relay_identity_route_marks_supervisor_restart_after_response() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-local-relay-restart-route");
        let data_dir = sinomo_core::local::SinomoDataDir::new(&root);
        let paths = DaemonPaths::new(&data_dir);
        let marker = root.join("rollout").join("supervised-main-restart.request");
        let effective_route = daemon_object_rpc_effective_route(
            sinomo_core::network::CLUSTER_RELAY_ROUTE,
            Some(sinomo_core::cluster::CLUSTER_CONTROL_JOIN_ROUTE),
        );

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
        assert_eq!(
            daemon_object_rpc_effective_route(sinomo_core::network::CLUSTER_RELAY_ROUTE, None,),
            sinomo_core::network::CLUSTER_RELAY_ROUTE,
            "a forwarded relay keeps the non-stopping outer route"
        );
    }

    #[test]
    fn outer_relay_session_requires_request_signer_to_match_link_signer() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-relay-session-link-signer");
        let paths = DaemonPaths::with_root(root.join("daemon"));
        let origin_public_key = vec![1_u8; 32];
        let claimed_link_public_key = vec![2_u8; 32];
        let authenticated = AuthenticatedDaemonCommand {
            route: sinomo_core::network::CLUSTER_RELAY_ROUTE.to_string(),
            signer: crate::DaemonSignerAuthority::Member,
            signer_node_id: "origin-node".to_string(),
            signer_public_key: sinomo_core::network::daemon_auth_encode_public_key(
                &origin_public_key,
            ),
        };
        let request = sinomo_core::network::ObjectRpcRequest::from_encoded_body(
            sinomo_core::network::CLUSTER_RELAY_ROUTE,
            "cluster-relay",
            Vec::new(),
        );

        let error = authorize_daemon_session_link_peer(
            &paths,
            &request,
            &authenticated,
            "different-link-node",
            &claimed_link_public_key,
        )
        .expect_err("an outer relay must be signed by its immediate link peer");

        assert!(matches!(
            error,
            sinomo_core::network::NetworkError::AuthDenied
        ));
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
    fn daemon_background_maintenance_shutdown_is_abortable() {
        assert_eq!(
            DAEMON_BACKGROUND_MAINTENANCE_TASK_JOIN_TIMEOUT,
            DAEMON_RUNTIME_TASK_JOIN_TIMEOUT
        );
        assert_eq!(
            daemon_runtime_task_join_timeout("background maintenance"),
            DAEMON_RUNTIME_TASK_JOIN_TIMEOUT
        );
        assert!(
            daemon_runtime_task_abort_before_join("background maintenance"),
            "periodic maintenance must not hold the daemon lock during rollout activation"
        );
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
            .block_on(daemon_liveness_status_response_async(&paths))
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

    #[test]
    fn dfs_message_replay_skips_large_data_plane_responses() {
        let small = sinomo_core::network::ObjectRpcResponse::from_encoded_body(
            sinomo_dfs::DFS_DATA_FETCH_BLOB_ROUTE,
            vec![0_u8; DAEMON_DFS_MESSAGE_REPLAY_MAX_CACHED_BODY_BYTES - 1],
        );
        assert!(daemon_dfs_message_response_is_replay_cacheable(&small));

        let large = sinomo_core::network::ObjectRpcResponse::from_encoded_body(
            sinomo_dfs::DFS_DATA_FETCH_BLOB_ROUTE,
            vec![0_u8; DAEMON_DFS_MESSAGE_REPLAY_MAX_CACHED_BODY_BYTES],
        );
        assert!(!daemon_dfs_message_response_is_replay_cacheable(&large));

        let large_non_dfs = sinomo_core::network::ObjectRpcResponse::from_encoded_body(
            sinomo_core::network::CLUSTER_NETWORK_PING_ROUTE,
            vec![0_u8; DAEMON_DFS_MESSAGE_REPLAY_MAX_CACHED_BODY_BYTES],
        );
        assert!(daemon_dfs_message_response_is_replay_cacheable(
            &large_non_dfs
        ));
    }

    #[test]
    fn authenticated_rpc_auth_failure_returns_framed_error() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-auth-rpc-error");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("layout");
        let paths = DaemonPaths::new(&data_dir);
        let request = sinomo_core::network::ObjectRpcRequest::encode_body(
            sinomo_rollout::ROLLOUT_DIAGNOSTICS_ROUTE,
            ROLLOUT_RPC_CATEGORY,
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
        let response = client_runtime
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
                    .await?;
                stream
                    .write_request(&address, &control_request, frame_options)
                    .await?;
                stream
                    .read_response(&address, DAEMON_COMMAND_MAX_BYTES, frame_options)
                    .await
            })
            .expect("framed auth error response");
        assert_eq!(response.route, sinomo_rollout::ROLLOUT_DIAGNOSTICS_ROUTE);
        let error = response
            .error_detail()
            .expect_err("auth failures are RPC errors");
        assert!(matches!(
            error,
            sinomo_core::network::NetworkError::AuthDenied
        ));
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

    #[test]
    fn authenticated_registry_failure_returns_framed_error_without_connection_error() {
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
        let signed =
            sign_daemon_rpc_request_from_data_dir(&data_dir, &request).expect("signed request");
        let control_request = request.with_auth(signed);
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
                .expect("registry failure is framed and handled");
        });

        let address = address_rx.recv().expect("listener address");
        let client_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("client runtime");
        let response = client_runtime
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
                    .await?;
                stream
                    .write_request(&address, &control_request, frame_options)
                    .await?;
                stream
                    .read_response(&address, DAEMON_COMMAND_MAX_BYTES, frame_options)
                    .await
            })
            .expect("framed registry error response");
        assert_eq!(response.route, "/benchmarks/cancel-probe");
        let error = response
            .error_detail()
            .expect_err("handler failures are RPC errors");
        assert!(
            error.to_string().contains("injected registry failure"),
            "unexpected error detail: {error}"
        );
        server.join().expect("server thread");
    }

    #[test]
    fn signed_dfs_status_dispatches_through_daemon_control_for_cluster_member() {
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

        let response = runtime
            .block_on(dispatch_daemon_control_request_in_process_async(
                target_daemon_paths,
                runtime.handle().clone(),
                request.with_auth(signed),
            ))
            .expect("daemon control dispatch");

        let status =
            sinomo_dfs::decode_dfs_status_rpc_response(&response).expect("dfs status response");
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
        let cluster_state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "daemon module rollout status",
            node.clone(),
            "daemon node",
            "public-key",
            OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &cluster_state)
            .expect("write cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(
            &cluster_paths,
            &cluster_state,
        )
        .expect("sync cluster state");

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
        let statuses = sinomo_rollout::read_cluster_module_rollout_statuses_from_kv(&cluster_paths)
            .expect("cluster statuses");

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
        let cluster_state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "daemon desired blocked",
            node.clone(),
            "daemon node",
            "public-key",
            OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &cluster_state)
            .expect("write cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(
            &cluster_paths,
            &cluster_state,
        )
        .expect("sync cluster state");

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
        sinomo_rollout::sync_cluster_module_rollout_desired_to_kv(&cluster_paths, &desired)
            .expect("sync desired");

        let daemon_paths = DaemonPaths::with_root(root.join("daemon"));
        let reconciliations = run_daemon_module_rollout_desired_reconciliation_tick(&daemon_paths)
            .expect("reconcile desired");
        let records =
            sinomo_rollout::read_cluster_module_rollout_reconciliations_from_kv(&cluster_paths)
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
        let second_records =
            sinomo_rollout::read_cluster_module_rollout_reconciliations_from_kv(&cluster_paths)
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
        let cluster_state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "daemon desired prepared",
            node.clone(),
            "daemon node",
            "public-key",
            OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &cluster_state)
            .expect("write cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(
            &cluster_paths,
            &cluster_state,
        )
        .expect("sync cluster state");

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
        sinomo_rollout::sync_cluster_module_rollout_desired_to_kv(&cluster_paths, &desired)
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
        let cluster_state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "daemon desired matched",
            node.clone(),
            "daemon node",
            "public-key",
            OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &cluster_state)
            .expect("write cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(
            &cluster_paths,
            &cluster_state,
        )
        .expect("sync cluster state");

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
        sinomo_rollout::sync_cluster_module_rollout_desired_to_kv(&cluster_paths, &desired)
            .expect("sync desired");

        let daemon_paths = DaemonPaths::with_root(root.join("daemon"));
        let reconciliations = run_daemon_module_rollout_desired_reconciliation_tick(&daemon_paths)
            .expect("reconcile desired");
        let records =
            sinomo_rollout::read_cluster_module_rollout_reconciliations_from_kv(&cluster_paths)
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
        while send_test_status_request(&data_dir, &address, Duration::from_millis(200)).is_err() {
            assert!(
                start.elapsed() < Duration::from_secs(3),
                "daemon did not become ready"
            );
            thread::sleep(Duration::from_millis(20));
        }

        let idle_probe = TcpStream::connect(&address).expect("connect idle probe");
        thread::sleep(Duration::from_millis(100));
        let status_result = send_test_unsigned_status_request(&address, Duration::from_millis(500));
        drop(idle_probe);
        send_test_shutdown_request(&data_dir, &address, Duration::from_secs(3))
            .expect("shutdown request");
        daemon
            .join()
            .expect("daemon thread")
            .expect("daemon exits cleanly");

        assert!(
            status_result.is_ok(),
            "loopback status should bypass auth/registry work and not be blocked by an idle TCP probe: {status_result:?}"
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
    fn daemon_maintenance_sleep_uses_next_due_work_instead_of_fixed_tick() {
        let now = Instant::now();
        let last_dfs = now
            .checked_sub(Duration::from_secs(10))
            .expect("dfs instant");
        let last_rollout = now
            .checked_sub(Duration::from_secs(20))
            .expect("rollout instant");

        assert_eq!(
            daemon_maintenance_sleep_duration(
                now,
                Duration::from_secs(30),
                last_dfs,
                last_rollout,
                None,
                Some(DAEMON_DFS_MAINTENANCE_PROBE_INTERVAL),
                Some(DAEMON_SHADOW_ROLLOUT_IDLE_FINALIZATION_INTERVAL),
                None,
            ),
            DAEMON_DFS_MAINTENANCE_PROBE_INTERVAL,
            "quiet main daemons should wake periodically to discover durable DFS maintenance work"
        );
        assert_eq!(
            daemon_maintenance_sleep_duration(
                now,
                Duration::from_secs(30),
                last_dfs,
                last_rollout,
                None,
                Some(DAEMON_DFS_MAINTENANCE_PROBE_INTERVAL),
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
                last_dfs,
                last_rollout,
                Some(Duration::from_secs(3)),
                Some(DAEMON_DFS_MAINTENANCE_PROBE_INTERVAL),
                None,
                None,
            ),
            Duration::from_secs(3),
            "triggered DFS namespace work should wake before the fixed five-minute tick"
        );
        let almost_due_dfs = now
            .checked_sub(DAEMON_DFS_MAINTENANCE_INTERVAL - Duration::from_secs(1))
            .expect("almost due dfs instant");
        assert_eq!(
            daemon_maintenance_sleep_duration(
                now,
                Duration::from_secs(600),
                almost_due_dfs,
                now,
                None,
                Some(DAEMON_DFS_MAINTENANCE_PROBE_INTERVAL),
                None,
                None,
            ),
            Duration::from_secs(1),
            "DFS maintenance must wake on its own even when no shadow finalization is pending"
        );
        assert_eq!(
            daemon_maintenance_sleep_duration(
                now,
                Duration::ZERO,
                last_dfs,
                last_rollout,
                None,
                None,
                None,
                None,
            ),
            Duration::from_millis(1),
            "overdue work should not create a zero-duration spin"
        );
        assert_eq!(
            daemon_maintenance_sleep_duration(
                now,
                Duration::from_secs(600),
                last_dfs,
                last_rollout,
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

    fn send_test_unsigned_status_request(
        address: &str,
        timeout: Duration,
    ) -> Result<crate::DaemonStatus, DaemonError> {
        let request = sinomo_core::network::ObjectRpcRequest::encode_body(
            crate::DAEMON_STATUS_ROUTE,
            crate::DAEMON_RPC_CATEGORY,
            &(),
        )?;
        let response = crate::send_daemon_rpc_raw(address, None, &request, timeout)?;
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
        let signed = sign_daemon_rpc_request_from_data_dir(data_dir, request)?;
        crate::send_daemon_rpc_raw(address, Some(&signed), request, timeout)
            .map_err(DaemonError::from)
    }
}
