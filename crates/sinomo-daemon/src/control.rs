use crate::cluster::{
    ClusterNetworkMaintenance, DAEMON_CLUSTER_PUBLISH_INTERVAL, DAEMON_CLUSTER_TICK_INTERVAL,
    cluster_paths_from_daemon_paths, ensure_daemon_status_cluster,
    ensure_daemon_status_cluster_async, handle_daemon_cluster_kv_snapshot,
    prune_local_cluster_raft_uncommitted_tail, start_daemon_iroh_proxy_if_clustered,
};
use crate::dfs::{
    daemon_dfs_endpoint_transport, daemon_dfs_endpoint_transport_on_runtime,
    daemon_dfs_maintenance_notify, daemon_dfs_pending_maintenance_work,
    ensure_default_distributed_dfs_async_on_runtime, ensure_default_local_dfs_expose,
    run_daemon_dfs_maintenance_tick_on_runtime,
};
use crate::{
    AuthenticatedDaemonCommand, DaemonClusterStatus, DaemonError, DaemonLock, DaemonPaths,
    authenticate_daemon_control_request, daemon_paths_are_shadow, daemon_rpc_command,
    handle_daemon_clear_rollout_status, handle_daemon_finalize_shadow_rollout, read_daemon_status,
};
use sinomo_core::{
    ClusterModuleRolloutDesiredRecord, ClusterModuleRolloutReconciliationRecord,
    ClusterModuleRolloutStatusRecord, cluster_module_rollout_reconcile_key,
};
use sinomo_events::EventStore;
use sinomo_node::NodeId;
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
use std::sync::Arc;
#[cfg(feature = "sandbox")]
use std::sync::{Mutex, OnceLock};
#[cfg(test)]
use std::thread;
use std::time::{Duration, Instant};
use time::OffsetDateTime;

pub const DAEMON_COMMAND_MAX_BYTES: usize = sinomo_network::DAEMON_CONTROL_RPC_MAX_FRAME_BYTES;
pub const DAEMON_DFS_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(300);
pub const DAEMON_DFS_TRIGGERED_MAINTENANCE_MIN_INTERVAL: Duration = Duration::from_secs(5);
pub const DAEMON_ROLLOUT_TELEMETRY_INTERVAL: Duration = Duration::from_secs(60);
pub const DAEMON_SHADOW_ROLLOUT_IDLE_FINALIZATION_INTERVAL: Duration =
    DAEMON_CLUSTER_PUBLISH_INTERVAL;
pub(crate) const ACCEPTED_ROLLOUT_RESPONSE_DRAIN_GRACE: Duration = Duration::from_millis(200);
const DAEMON_SANDBOX_STREAM_CHANNEL_CAPACITY: usize = 64;

// The sandbox concurrency tracker is only exercised by the sandbox-exec RPC
// handlers below, which are gated on the `sandbox` feature. Gate it to match so
// the default (sandbox-off) build does not carry it as dead code.
#[cfg(feature = "sandbox")]
static SANDBOX_ACTIVE_EXECUTIONS: OnceLock<Mutex<SandboxActiveExecutions>> = OnceLock::new();

#[cfg(feature = "sandbox")]
#[derive(Default)]
struct SandboxActiveExecutions {
    counts: BTreeMap<(PathBuf, sinomo_sandbox::SandboxId), u64>,
}

fn background_dfs_maintenance_enabled() -> bool {
    background_dfs_maintenance_enabled_for(
        std::env::var("SINOMO_ENABLE_BACKGROUND_DFS_MAINTENANCE")
            .ok()
            .as_deref(),
    )
}

fn background_dfs_maintenance_enabled_for(value: Option<&str>) -> bool {
    !matches!(
        value.map(|value| value.trim().to_ascii_lowercase()),
        Some(value) if matches!(value.as_str(), "0" | "false" | "no" | "off")
    )
}

#[cfg(feature = "sandbox")]
#[derive(Debug)]
struct SandboxActiveExecutionGuard {
    root: PathBuf,
    sandbox_id: sinomo_sandbox::SandboxId,
}

#[cfg(feature = "sandbox")]
impl Drop for SandboxActiveExecutionGuard {
    fn drop(&mut self) {
        let Some(lock) = SANDBOX_ACTIVE_EXECUTIONS.get() else {
            return;
        };
        let Ok(mut active) = lock.lock() else {
            return;
        };
        let key = (self.root.clone(), self.sandbox_id.clone());
        if let Some(count) = active.counts.get_mut(&key) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                active.counts.remove(&key);
            }
        }
    }
}

#[cfg(feature = "sandbox")]
fn acquire_sandbox_active_execution(
    paths: &DaemonPaths,
    sandbox_id: &sinomo_sandbox::SandboxId,
    policy: &sinomo_sandbox::SandboxPolicy,
) -> Result<SandboxActiveExecutionGuard, sinomo_sandbox::SandboxPolicyDenial> {
    let lock =
        SANDBOX_ACTIVE_EXECUTIONS.get_or_init(|| Mutex::new(SandboxActiveExecutions::default()));
    let mut active = lock.lock().map_err(|_| {
        sinomo_sandbox::SandboxPolicyDenial::new(
            "concurrency-tracker-unavailable",
            "sandbox active execution tracker is unavailable",
        )
    })?;
    let root = paths.root.clone();
    let key = (root.clone(), sandbox_id.clone());
    let current_for_sandbox = active.counts.get(&key).copied().unwrap_or(0);
    let current_active_sandboxes = active
        .counts
        .keys()
        .filter(|(active_root, _)| active_root == &root)
        .count() as u64;
    let active_sandboxes_after = if current_for_sandbox == 0 {
        current_active_sandboxes.saturating_add(1)
    } else {
        current_active_sandboxes
    };
    policy.validate_active_execution_counts(
        sandbox_id,
        current_for_sandbox.saturating_add(1),
        active_sandboxes_after,
    )?;
    active
        .counts
        .insert(key, current_for_sandbox.saturating_add(1));
    Ok(SandboxActiveExecutionGuard {
        root,
        sandbox_id: sandbox_id.clone(),
    })
}

pub fn run_daemon_control_loop(
    options: crate::DaemonRunOptions,
    paths: DaemonPaths,
    label: &str,
    cluster: Option<DaemonClusterStatus>,
) -> Result<(), DaemonError> {
    let _lock = DaemonLock::acquire(&paths, &options, cluster.clone())?;
    if let Err(error) = prune_local_cluster_raft_uncommitted_tail(&paths) {
        eprintln!("sinomo {label} cluster raft startup repair skipped: {error}");
    }
    if label == "daemon" && crate::dfs::default_distributed_dfs_enabled() {
        ensure_default_local_dfs_expose(&paths)?;
    }
    let daemon_runtime = crate::build_daemon_async_runtime(label)?;
    let daemon_runtime_handle = daemon_runtime.handle().clone();
    let bind_address = format!("{}:{}", options.host, options.port);
    let listener = daemon_runtime.block_on(sinomo_network::AsyncDaemonControlListener::bind(
        &bind_address,
    ))?;
    println!("sinomo {label} listening on {bind_address}");
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
    if let Err(error) = start_daemon_iroh_proxy_if_clustered(
        &paths,
        &options,
        cluster.as_ref(),
        daemon_runtime_handle.clone(),
        label,
    ) {
        eprintln!("sinomo {label} iroh proxy disabled: {error}");
        crate::append_operational_log_best_effort(
            &paths,
            crate::OperationalSeverity::Warn,
            "daemon-control",
            format!("sinomo {label} iroh proxy disabled"),
            None,
            serde_json::json!({
                "label": label,
                "error": error.to_string(),
            }),
        );
    }

    if label == "daemon" && crate::dfs::default_distributed_dfs_enabled() {
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
                eprintln!("sinomo {bootstrap_label} distributed DFS bootstrap failed: {error}");
            }
        });
    }

    let idle_stats = Arc::new(crate::IdleStatsAtomics::new());
    crate::install_process_idle_stats(idle_stats.clone());
    crate::register_io_observer(idle_stats.clone());

    let maintenance_paths = paths.clone();
    let maintenance_options = options.clone();
    let maintenance_label = label.to_string();
    let maintenance_idle_stats = idle_stats.clone();
    let maintenance_runtime = daemon_runtime_handle.clone();
    daemon_runtime_handle.spawn(run_daemon_background_maintenance_loop(
        maintenance_paths,
        maintenance_options,
        maintenance_label,
        maintenance_idle_stats,
        maintenance_runtime,
    ));

    let service_host = daemon_runtime.block_on(
        crate::service_adapter::DaemonServiceRegistryHost::new_on_runtime(
            paths.clone(),
            daemon_runtime_handle.clone(),
        ),
    )?;

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    daemon_runtime.block_on(run_daemon_control_accept_loop(
        listener,
        paths.clone(),
        label.to_string(),
        bind_address.clone(),
        service_host,
        daemon_runtime_handle.clone(),
        shutdown_tx,
        shutdown_rx,
    ))?;
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
    Ok(())
}

async fn run_daemon_background_maintenance_loop(
    paths: DaemonPaths,
    options: crate::DaemonRunOptions,
    label: String,
    idle_stats: Arc<crate::IdleStatsAtomics>,
    runtime: tokio::runtime::Handle,
) {
    let mut cluster_network = ClusterNetworkMaintenance::with_idle_stats(idle_stats.clone()).await;
    let dfs_maintenance_enabled = background_dfs_maintenance_enabled();
    let maintenance_started = std::time::Instant::now();
    let mut last_dfs_maintenance = maintenance_started
        .checked_sub(if dfs_maintenance_enabled {
            DAEMON_DFS_MAINTENANCE_INTERVAL
        } else {
            Duration::ZERO
        })
        .unwrap_or(maintenance_started);
    let mut last_rollout_telemetry = maintenance_started
        .checked_sub(DAEMON_ROLLOUT_TELEMETRY_INTERVAL)
        .unwrap_or(maintenance_started);
    let dfs_maintenance_notify = daemon_dfs_maintenance_notify();
    let mut dfs_triggered_maintenance_requested = false;

    loop {
        if label == "daemon" {
            let finalization_paths = paths.clone();
            match tokio::task::spawn_blocking(move || {
                handle_daemon_finalize_shadow_rollout(&finalization_paths)
            })
            .await
            {
                Ok(Ok(response)) if !response.starts_with("no pending shadow rollout") => {
                    print!("sinomo {label} rollout finalization: {response}");
                }
                Ok(Ok(_)) => {}
                Ok(Err(error)) => {
                    eprintln!("sinomo {label} shadow rollout finalization tick failed: {error}");
                }
                Err(error) => {
                    eprintln!("sinomo {label} shadow rollout finalization task failed: {error}");
                }
            }
        }

        match cluster_network
            .tick_async(&paths, &options, runtime.clone())
            .await
        {
            Ok(pending_work) => {
                if let Some(pending_work) = pending_work {
                    cluster_network
                        .run_pending_work_async(pending_work, runtime.clone())
                        .await;
                }
            }
            Err(error) => {
                eprintln!("sinomo {label} cluster network tick failed: {error}");
            }
        }

        if dfs_maintenance_enabled && label == "daemon" && !dfs_triggered_maintenance_requested {
            let probe_paths = paths.clone();
            match tokio::task::spawn_blocking(move || {
                daemon_dfs_pending_maintenance_work(&probe_paths)
            })
            .await
            {
                Ok(Ok(work)) if work.has_work() => {
                    dfs_triggered_maintenance_requested = true;
                }
                Ok(Ok(_)) => {}
                Ok(Err(error)) => {
                    eprintln!("sinomo {label} DFS maintenance probe failed: {error}");
                }
                Err(error) => {
                    eprintln!("sinomo {label} DFS maintenance probe task failed: {error}");
                }
            }
        }

        if dfs_maintenance_enabled
            && label == "daemon"
            && (last_dfs_maintenance.elapsed() >= DAEMON_DFS_MAINTENANCE_INTERVAL
                || (dfs_triggered_maintenance_requested
                    && last_dfs_maintenance.elapsed()
                        >= DAEMON_DFS_TRIGGERED_MAINTENANCE_MIN_INTERVAL))
        {
            let dfs_paths = paths.clone();
            let dfs_runtime = runtime.clone();
            match run_daemon_dfs_maintenance_tick_on_runtime(&dfs_paths, &dfs_runtime).await {
                Ok(()) => {
                    let probe_paths = paths.clone();
                    match tokio::task::spawn_blocking(move || {
                        daemon_dfs_pending_maintenance_work(&probe_paths)
                    })
                    .await
                    {
                        Ok(Ok(work)) => {
                            dfs_triggered_maintenance_requested = work.has_work();
                        }
                        Ok(Err(error)) => {
                            dfs_triggered_maintenance_requested = false;
                            eprintln!(
                                "sinomo {label} DFS maintenance follow-up probe failed: {error}"
                            );
                        }
                        Err(error) => {
                            dfs_triggered_maintenance_requested = false;
                            eprintln!(
                                "sinomo {label} DFS maintenance follow-up probe task failed: {error}"
                            );
                        }
                    }
                }
                Err(error) => {
                    dfs_triggered_maintenance_requested = false;
                    eprintln!("sinomo {label} DFS maintenance tick failed: {error}");
                }
            }
            last_dfs_maintenance = std::time::Instant::now();
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
                Ok(Ok(Some(applied))) => println!(
                    "sinomo {label} rollout telemetry applied {:?} at {} bps",
                    applied.transition.action, applied.transition.current_weight_bps
                ),
                Ok(Ok(None)) => {}
                Ok(Err(error)) => {
                    eprintln!("sinomo {label} rollout telemetry tick failed: {error}");
                }
                Err(error) => {
                    eprintln!("sinomo {label} rollout telemetry task failed: {error}");
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
                    eprintln!(
                        "sinomo {label} shadow rollout finalization due task failed: {error}"
                    );
                    None
                }
            }
        } else {
            None
        };
        let dfs_triggered_maintenance_due = if dfs_maintenance_enabled
            && label == "daemon"
            && dfs_triggered_maintenance_requested
        {
            Some(daemon_maintenance_interval_remaining(
                sleep_started,
                last_dfs_maintenance,
                DAEMON_DFS_TRIGGERED_MAINTENANCE_MIN_INTERVAL,
            ))
        } else {
            None
        };
        let sleep_duration = daemon_maintenance_sleep_duration(
            sleep_started,
            cluster_network.next_due_duration(sleep_started),
            last_dfs_maintenance,
            last_rollout_telemetry,
            dfs_triggered_maintenance_due,
            shadow_finalization_due,
        );
        if dfs_maintenance_enabled && label == "daemon" {
            let notify = Arc::clone(&dfs_maintenance_notify);
            tokio::select! {
                _ = tokio::time::sleep(sleep_duration) => {}
                _ = notify.notified() => {
                    dfs_triggered_maintenance_requested = true;
                }
            }
        } else {
            tokio::time::sleep(sleep_duration).await;
        }
    }
}

pub(crate) async fn run_daemon_control_accept_loop(
    listener: sinomo_network::AsyncDaemonControlListener,
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
                        Err(error) => eprintln!("sinomo {client_label} client error: {error}"),
                    }
                });
            }
        }
    }
    Ok(())
}

pub(crate) async fn handle_daemon_connection_async(
    mut stream: sinomo_network::AsyncDaemonControlStream,
    paths: DaemonPaths,
    service_host: crate::service_adapter::DaemonServiceRegistryHost,
    service_runtime: tokio::runtime::Handle,
    stream_address: String,
) -> Result<bool, DaemonError> {
    let mut request = match read_daemon_control_frame_async(&mut stream, &stream_address).await {
        Ok(request) => request,
        Err(_) => {
            let _ = stream.shutdown_write(&stream_address).await;
            return Ok(false);
        }
    };
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

    let command = request.command.trim().to_string();
    let authenticated = match authenticate_daemon_control_request(&paths, &request) {
        Ok(authenticated) => authenticated,
        Err(error) => {
            if crate::auth::is_cluster_join_register_control_request(&request) {
                eprintln!("sinomo daemon client auth failed for {command}: {error}");
            }
            if let Some(rpc_request) = request.rpc.as_ref()
                && let Ok(response) = sinomo_network::ObjectRpcResponse::encode_error(
                    rpc_request.route.clone(),
                    DaemonError::AuthDenied.to_string(),
                )
            {
                let _ =
                    write_daemon_rpc_response_frame_async(&mut stream, &stream_address, response)
                        .await;
            }
            let _ = stream.shutdown_write(&stream_address).await;
            return Ok(false);
        }
    };

    let rpc_request = request.rpc;
    handle_authenticated_daemon_command_async(
        authenticated,
        rpc_request,
        stream,
        paths,
        service_host,
        service_runtime,
        stream_address.clone(),
    )
    .await
}

async fn handle_authenticated_daemon_command_async(
    authenticated: AuthenticatedDaemonCommand,
    rpc_request: Option<sinomo_network::ObjectRpcRequest>,
    mut stream: sinomo_network::AsyncDaemonControlStream,
    paths: DaemonPaths,
    service_host: crate::service_adapter::DaemonServiceRegistryHost,
    service_runtime: tokio::runtime::Handle,
    stream_address: String,
) -> Result<bool, DaemonError> {
    if let Some(request) = rpc_request.as_ref()
        && crate::service_adapter::is_sandbox_stream_route(request)
    {
        return dispatch_sandbox_stream_route_async(
            stream,
            stream_address,
            paths,
            request.clone(),
            service_runtime.clone(),
        )
        .await;
    }
    if let Some(result) = crate::service_adapter::try_dispatch_via_registry_async(
        &authenticated,
        &rpc_request,
        &mut stream,
        &stream_address,
        &service_host,
    )
    .await
    {
        if let Err(error) = &result
            && let Some(response) = rpc_error_response_for_request(&rpc_request, error)
        {
            let _ =
                write_daemon_rpc_response_frame_async(&mut stream, &stream_address, response).await;
        }
        return result;
    }

    let command = authenticated.command.as_str();
    let error_response_request = rpc_request.clone();
    let result = match command {
        command if command == daemon_rpc_command(ROLLOUT_FINALIZE_SHADOW_ROUTE) => {
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
            let response = sinomo_network::ObjectRpcResponse::encode_body(
                ROLLOUT_FINALIZE_SHADOW_ROUTE,
                &response,
            )
            .map_err(DaemonError::from)?;
            write_daemon_rpc_response_frame_async(&mut stream, &stream_address, response).await?;
            Ok(false)
        }
        command if command == daemon_rpc_command(ROLLOUT_CLEAR_STATUS_ROUTE) => {
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
            let response = sinomo_network::ObjectRpcResponse::encode_body(
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
    result
}

async fn dispatch_sandbox_stream_route_async(
    mut stream: sinomo_network::AsyncDaemonControlStream,
    stream_address: String,
    paths: DaemonPaths,
    request: sinomo_network::ObjectRpcRequest,
    service_runtime: tokio::runtime::Handle,
) -> Result<bool, DaemonError> {
    crate::service_adapter::dispatch_sandbox_stream_route_async_on_runtime(
        &request,
        &mut stream,
        &stream_address,
        &paths,
        &service_runtime,
    )
    .await
    .unwrap_or(Ok(false))
}

async fn read_daemon_control_frame_async(
    stream: &mut sinomo_network::AsyncDaemonControlStream,
    address: &str,
) -> Result<sinomo_network::DaemonControlRequest, DaemonError> {
    stream
        .read_request(
            address,
            DAEMON_COMMAND_MAX_BYTES,
            sinomo_network::ObjectFrameIoOptions::none(),
        )
        .await
        .map_err(DaemonError::from)
}

async fn write_daemon_rpc_response_frame_async(
    stream: &mut sinomo_network::AsyncDaemonControlStream,
    address: &str,
    response: sinomo_network::ObjectRpcResponse,
) -> Result<(), DaemonError> {
    stream
        .write_response(
            address,
            &sinomo_network::DaemonControlResponse::rpc(String::new(), response),
            sinomo_network::ObjectFrameIoOptions::none(),
        )
        .await
        .map_err(DaemonError::from)
}

fn rpc_error_response_for_request(
    request: &Option<sinomo_network::ObjectRpcRequest>,
    error: &DaemonError,
) -> Option<sinomo_network::ObjectRpcResponse> {
    let route = request.as_ref()?.route.clone();
    sinomo_network::ObjectRpcResponse::encode_error(route, error.to_string()).ok()
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

fn daemon_maintenance_sleep_duration(
    now: Instant,
    cluster_next_due: Duration,
    last_dfs_maintenance: Instant,
    last_rollout_telemetry: Instant,
    dfs_triggered_maintenance_due: Option<Duration>,
    shadow_finalization_due: Option<Duration>,
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
    if let Some(shadow_finalization_due) = shadow_finalization_due {
        next_due = next_due.min(shadow_finalization_due);
    }
    daemon_sleep_duration_or_min(next_due)
}

fn shadow_rollout_finalization_poll_due_in(paths: &DaemonPaths) -> Duration {
    let Some(data_dir) = paths.root.parent() else {
        return DAEMON_CLUSTER_TICK_INTERVAL;
    };
    let rollout_paths = SelfRolloutPaths::new(data_dir);
    if rollout_shadow_finalize_path(&rollout_paths).exists()
        || rollout_main_activation_path(&rollout_paths).exists()
    {
        DAEMON_CLUSTER_TICK_INTERVAL
    } else {
        DAEMON_SHADOW_ROLLOUT_IDLE_FINALIZATION_INTERVAL
    }
}

fn require_daemon_rpc_request(
    request: Option<sinomo_network::ObjectRpcRequest>,
    expected_route: &str,
    expected_category: &str,
) -> Result<sinomo_network::ObjectRpcRequest, DaemonError> {
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
    let command_id = sinomo_sandbox::SandboxCommandId::generate();
    let mut fs = sinomo_sandbox::SifsSandboxFileSystem::new(store.into_inner(), &manifest)
        .with_replica_candidates(replica_candidates);
    let result = match sinomo_sandbox::sandbox_http_fetch_to_file(
        &config.policy,
        &DaemonSandboxHttpTransport,
        &mut fs,
        manifest.sandbox_id.clone(),
        command_id.clone(),
        &request.request,
        &request.output_path,
        &manifest.limits,
    ) {
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
    let dfs = open_daemon_sandbox_dfs_service_on_runtime(paths, runtime)?;
    let execution_node_id = dfs.node_id().clone();
    let replica_candidates = daemon_sandbox_replica_candidates(paths, &execution_node_id);
    let sandbox_id = request
        .sandbox_id
        .unwrap_or_else(sinomo_sandbox::SandboxId::generate);
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

    let mut store = sinomo_sandbox::SifsSandboxInstanceStore::new(dfs)
        .with_replica_candidates(replica_candidates);
    let created = store
        .create(
            manifest.sandbox_id.clone(),
            manifest.toolchain_id.clone(),
            manifest.created_at,
        )
        .map_err(|source| DaemonError::Parse {
            detail: format!("create sandbox SIFS state: {source}"),
        })?;
    manifest.created_at = created.created_at;
    manifest.updated_at = created.updated_at;
    store
        .save_manifest(&manifest)
        .map_err(|source| DaemonError::Parse {
            detail: format!("save sandbox SIFS manifest: {source}"),
        })?;
    append_daemon_sandbox_event(
        paths,
        sinomo_sandbox::sandbox_instance_created_event(execution_node_id.clone(), &manifest),
    )
    .map_err(|source| DaemonError::Parse {
        detail: format!("append sandbox created event: {source}"),
    })?;
    append_daemon_sandbox_event(
        paths,
        sinomo_sandbox::sandbox_toolchain_pinned_event(execution_node_id.clone(), &manifest),
    )
    .map_err(|source| DaemonError::Parse {
        detail: format!("append sandbox toolchain pinned event: {source}"),
    })?;
    for mount in &manifest.mounts {
        append_daemon_sandbox_event(
            paths,
            sinomo_sandbox::sandbox_mount_added_event(execution_node_id.clone(), &manifest, mount),
        )
        .map_err(|source| DaemonError::Parse {
            detail: format!("append sandbox mount added event: {source}"),
        })?;
    }
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
    let config = load_daemon_sandbox_policy_config(paths)?;
    let dfs = open_daemon_sandbox_dfs_service_on_runtime(paths, runtime)?;
    let node_id = dfs.node_id().clone();
    let replica_candidates = daemon_sandbox_replica_candidates(paths, &node_id);
    let mut store = sinomo_sandbox::SifsSandboxInstanceStore::new(dfs)
        .with_replica_candidates(replica_candidates);
    let mut manifest = store.load(&request.sandbox_id)?;
    let cwd_changed = request.cwd.is_some();
    let mut env_set_keys = request.env_set.keys().cloned().collect::<Vec<_>>();
    let mut env_unset_keys = request.env_unset.clone();
    for key in env_set_keys.iter().chain(env_unset_keys.iter()) {
        if !sinomo_sandbox::sandbox_env_name_is_valid(key) {
            return Err(DaemonError::Parse {
                detail: format!("invalid sandbox env name `{key}`"),
            });
        }
    }
    for (key, value) in &request.env_set {
        if value.contains('\0') {
            return Err(DaemonError::Parse {
                detail: format!("sandbox env `{key}` contains an embedded NUL byte"),
            });
        }
    }
    env_set_keys.sort();
    env_unset_keys.sort();
    env_unset_keys.dedup();
    if let Some(cwd) = request.cwd.as_ref() {
        let fs = sinomo_sandbox::SifsSandboxFileSystem::new(store.into_inner(), &manifest);
        sinomo_sandbox::validate_sandbox_working_directory(&fs, cwd)?;
        store = sinomo_sandbox::SifsSandboxInstanceStore::new(fs.into_inner())
            .with_replica_candidates(daemon_sandbox_replica_candidates(paths, &node_id));
    }
    if let Some(cwd) = request.cwd {
        manifest.cwd = cwd;
    }
    for key in request.env_unset {
        manifest.env.remove(&key);
    }
    manifest.env.extend(request.env_set);
    manifest.updated_at = OffsetDateTime::now_utc();
    if let Err(denial) = config.policy.validate_manifest(&manifest, &node_id) {
        append_daemon_sandbox_event(
            paths,
            sinomo_sandbox::sandbox_policy_denied_event(
                node_id.clone(),
                Some(&manifest.sandbox_id),
                "sandbox-update",
                &denial,
            ),
        )?;
        if denial.code.starts_with("mount-") {
            append_daemon_sandbox_event(
                paths,
                sinomo_sandbox::sandbox_mount_denied_event(
                    node_id,
                    Some(&manifest.sandbox_id),
                    "sandbox-update",
                    &denial,
                ),
            )?;
        }
        return Err(DaemonError::Parse {
            detail: denial.to_string(),
        });
    }
    store.save_manifest(&manifest)?;
    append_daemon_sandbox_event(
        paths,
        sinomo_sandbox::sandbox_instance_updated_event(
            node_id,
            &manifest,
            cwd_changed,
            &env_set_keys,
            &env_unset_keys,
        ),
    )?;
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
    let checkpoint = store.create_checkpoint(&request.sandbox_id, OffsetDateTime::now_utc())?;
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
    let config = load_daemon_sandbox_policy_config(paths)?;
    let dfs = open_daemon_sandbox_dfs_service_on_runtime(paths, runtime)?;
    let node_id = dfs.node_id().clone();
    let replica_candidates = daemon_sandbox_replica_candidates(paths, &node_id);
    let mut store = sinomo_sandbox::SifsSandboxInstanceStore::new(dfs)
        .with_replica_candidates(replica_candidates);
    let current = store.load(&request.sandbox_id)?;
    validate_daemon_sandbox_manifest_policy(
        paths,
        &config.policy,
        &node_id,
        &current,
        "sandbox-resume",
    )?;
    let manifest = store.resume_checkpoint(
        &request.sandbox_id,
        &request.checkpoint_id,
        OffsetDateTime::now_utc(),
    )?;
    append_daemon_sandbox_event(
        paths,
        sinomo_sandbox::sandbox_instance_resumed_event(node_id, &manifest, &request.checkpoint_id),
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
        "sandbox-reboot",
    )?;
    let (manifest, cleared_tmp) =
        store.reboot_session(&request.sandbox_id, OffsetDateTime::now_utc())?;
    append_daemon_sandbox_event(
        paths,
        sinomo_sandbox::sandbox_instance_rebooted_event(node_id, &manifest, cleared_tmp),
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
    let dfs = open_daemon_sandbox_dfs_service_on_runtime(paths, runtime)?;
    let node_id = dfs.node_id().clone();
    let replica_candidates = daemon_sandbox_replica_candidates(paths, &node_id);
    let mut store = sinomo_sandbox::SifsSandboxInstanceStore::new(dfs)
        .with_replica_candidates(replica_candidates);
    let workspace_root = store
        .load(&request.sandbox_id)
        .map(|manifest| manifest.workspace_root)
        .unwrap_or_else(|_| sinomo_sandbox::sandbox_instance_root(&request.sandbox_id));
    store.destroy(&request.sandbox_id)?;
    append_daemon_sandbox_event(
        paths,
        sinomo_sandbox::sandbox_instance_destroyed_event(
            node_id,
            &request.sandbox_id,
            workspace_root,
        ),
    )?;
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
    let mut fs = sinomo_sandbox::SifsSandboxFileSystem::new(store.into_inner(), &manifest)
        .with_replica_candidates(replica_candidates);
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
    if let Err(error) = fs.write(&request.path, &request.bytes) {
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
    node_id: &sinomo_node::NodeId,
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
    handle_daemon_sandbox_run_toolchain_with_output_sink_on_runtime(paths, request, None, runtime)
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
        paths, request, None, runtime,
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
) -> Result<sinomo_sandbox::SandboxProcessPlanOutcome, DaemonError> {
    Err(sandbox_runtime_not_compiled_error())
}

#[cfg(feature = "sandbox")]
fn handle_daemon_sandbox_run_process_plan_with_output_sink_on_runtime(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxRunProcessPlanRequest,
    output_sink: Option<Arc<dyn sinomo_sandbox::SandboxOutputSink>>,
    runtime: Option<&tokio::runtime::Handle>,
) -> Result<sinomo_sandbox::SandboxProcessPlanOutcome, DaemonError> {
    let config = load_daemon_sandbox_policy_config(paths)?;
    let data_dir = daemon_data_dir_path(paths)?;
    let mut dfs = open_daemon_sandbox_dfs_service_on_runtime(paths, runtime)?;
    let execution_node_id = dfs.node_id().clone();
    let replica_candidates = daemon_sandbox_replica_candidates(paths, &execution_node_id);
    let _active_execution =
        match acquire_sandbox_active_execution(paths, &request.sandbox_id, &config.policy) {
            Ok(guard) => guard,
            Err(denial) => {
                append_daemon_sandbox_event(
                    paths,
                    sinomo_sandbox::sandbox_policy_denied_event(
                        execution_node_id.clone(),
                        Some(&request.sandbox_id),
                        "sandbox-process-plan",
                        &denial,
                    ),
                )?;
                return Err(DaemonError::Parse {
                    detail: denial.to_string(),
                });
            }
        };
    let local_store = sinomo_sandbox::LocalSandboxInstanceStore::new(
        data_dir.join("sandbox").join("tmp").join("daemon-staging"),
    );
    let before_command_history = {
        let mut store = sinomo_sandbox::SifsSandboxInstanceStore::new(dfs);
        let manifest = store.load(&request.sandbox_id)?;
        dfs = store.into_inner();
        manifest.command_history
    };
    let runner =
        sinomo_sandbox_wasix_host::SandboxWasixSifsCommandRunner::from_active_module(&data_dir)
            .map_err(|source| DaemonError::Parse {
                detail: source.to_string(),
            })?
            .with_replica_candidates(replica_candidates);
    let outcome = match runner.run_toolchain_process_plan_with_policy_and_output_sink(
        dfs,
        &local_store,
        &request.sandbox_id,
        &request.plan,
        request.timeout_ms,
        &config.policy,
        output_sink,
    ) {
        Ok(outcome) => outcome,
        Err(source) => {
            match &source {
                sinomo_sandbox::SandboxCommandExecutionError::PolicyDenied(denial) => {
                    append_daemon_sandbox_event(
                        paths,
                        sinomo_sandbox::sandbox_policy_denied_event(
                            execution_node_id.clone(),
                            Some(&request.sandbox_id),
                            "sandbox-process-plan",
                            denial,
                        ),
                    )?;
                    if denial.code.starts_with("mount-") {
                        append_daemon_sandbox_event(
                            paths,
                            sinomo_sandbox::sandbox_mount_denied_event(
                                execution_node_id.clone(),
                                Some(&request.sandbox_id),
                                "sandbox-process-plan",
                                denial,
                            ),
                        )?;
                    }
                }
                sinomo_sandbox::SandboxCommandExecutionError::Scheduling(error) => {
                    append_daemon_sandbox_event(
                        paths,
                        sinomo_sandbox::sandbox_scheduling_failed_event(
                            execution_node_id.clone(),
                            &request.sandbox_id,
                            error,
                        ),
                    )?;
                }
                _ => {}
            }
            return Err(DaemonError::Parse {
                detail: source.to_string(),
            });
        }
    };
    let dfs = outcome.dfs;
    let node_id = dfs.node_id().clone();
    let mut store = sinomo_sandbox::SifsSandboxInstanceStore::new(dfs);
    let manifest = store.load(&request.sandbox_id)?;
    let mut fs = sinomo_sandbox::SifsSandboxFileSystem::new(store.into_inner(), &manifest);
    let mut event_store = daemon_event_store(paths)?;
    let process = match outcome.process {
        Ok(process) => process,
        Err(source) => {
            append_daemon_sandbox_new_command_record_events(
                &mut event_store,
                node_id,
                &manifest,
                &mut fs,
                &before_command_history,
                &manifest.command_history,
            )?;
            return Err(DaemonError::Parse {
                detail: source.to_string(),
            });
        }
    };
    for step in &process.step_outcomes {
        append_daemon_sandbox_command_events(
            &mut event_store,
            node_id.clone(),
            &manifest,
            &mut fs,
            step,
        )?;
    }
    Ok(process)
}

#[cfg(feature = "sandbox")]
fn append_daemon_sandbox_new_command_record_events<T: sinomo_dfs::DfsFrameTransport>(
    event_store: &mut sinomo_events::JsonlEventStore,
    node_id: sinomo_node::NodeId,
    manifest: &sinomo_sandbox::SandboxInstanceManifest,
    fs: &mut sinomo_sandbox::SifsSandboxFileSystem<T>,
    before_command_history: &[sinomo_sandbox::SandboxCommandId],
    after_command_history: &[sinomo_sandbox::SandboxCommandId],
) -> Result<(), DaemonError> {
    let before = before_command_history
        .iter()
        .collect::<std::collections::BTreeSet<_>>();
    for command_id in after_command_history
        .iter()
        .filter(|command_id| !before.contains(command_id))
    {
        let record = sinomo_sandbox::load_command_record(fs, command_id)?;
        event_store.append(sinomo_sandbox::sandbox_command_started_event(
            node_id.clone(),
            manifest,
            &record.command_id,
            &record.argv,
            &record.cwd,
            record.started_at,
        ))?;
        let stdout_path =
            sinomo_sandbox::sandbox_command_stdout_log_path(command_id).map_err(|source| {
                DaemonError::Parse {
                    detail: source.to_string(),
                }
            })?;
        if let Ok(stdout) = fs.read(&stdout_path, None) {
            for chunk in sandbox_output_chunks_from_bytes(
                command_id,
                &record.sandbox_id,
                sinomo_sandbox::SandboxOutputStream::Stdout,
                &stdout,
            ) {
                event_store.append(sinomo_sandbox::sandbox_command_output_event(
                    node_id.clone(),
                    &chunk,
                ))?;
            }
        }
        let stderr_path =
            sinomo_sandbox::sandbox_command_stderr_log_path(command_id).map_err(|source| {
                DaemonError::Parse {
                    detail: source.to_string(),
                }
            })?;
        if let Ok(stderr) = fs.read(&stderr_path, None) {
            for chunk in sandbox_output_chunks_from_bytes(
                command_id,
                &record.sandbox_id,
                sinomo_sandbox::SandboxOutputStream::Stderr,
                &stderr,
            ) {
                event_store.append(sinomo_sandbox::sandbox_command_output_event(
                    node_id.clone(),
                    &chunk,
                ))?;
            }
        }
        event_store.append(sinomo_sandbox::sandbox_command_record_event(
            node_id.clone(),
            &record,
        ))?;
    }
    Ok(())
}

#[cfg(not(feature = "sandbox"))]
fn handle_daemon_sandbox_run_toolchain_with_output_sink_on_runtime(
    _paths: &DaemonPaths,
    _request: sinomo_sandbox::SandboxRunToolchainRequest,
    _output_sink: Option<Arc<dyn sinomo_sandbox::SandboxOutputSink>>,
    _runtime: Option<&tokio::runtime::Handle>,
) -> Result<sinomo_sandbox::SandboxLocalCommandOutcome, DaemonError> {
    Err(sandbox_runtime_not_compiled_error())
}

#[cfg(feature = "sandbox")]
fn handle_daemon_sandbox_run_toolchain_with_output_sink_on_runtime(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxRunToolchainRequest,
    output_sink: Option<Arc<dyn sinomo_sandbox::SandboxOutputSink>>,
    runtime: Option<&tokio::runtime::Handle>,
) -> Result<sinomo_sandbox::SandboxLocalCommandOutcome, DaemonError> {
    let config = load_daemon_sandbox_policy_config(paths)?;
    let data_dir = daemon_data_dir_path(paths)?;
    let dfs = open_daemon_sandbox_dfs_service_on_runtime(paths, runtime)?;
    let execution_node_id = dfs.node_id().clone();
    let replica_candidates = daemon_sandbox_replica_candidates(paths, &execution_node_id);
    let _active_execution =
        match acquire_sandbox_active_execution(paths, &request.sandbox_id, &config.policy) {
            Ok(guard) => guard,
            Err(denial) => {
                append_daemon_sandbox_event(
                    paths,
                    sinomo_sandbox::sandbox_policy_denied_event(
                        execution_node_id.clone(),
                        Some(&request.sandbox_id),
                        "sandbox-run",
                        &denial,
                    ),
                )?;
                return Err(DaemonError::Parse {
                    detail: denial.to_string(),
                });
            }
        };
    let local_store = sinomo_sandbox::LocalSandboxInstanceStore::new(
        data_dir.join("sandbox").join("tmp").join("daemon-staging"),
    );
    let runner =
        sinomo_sandbox_wasix_host::SandboxWasixSifsCommandRunner::from_active_module(&data_dir)
            .map_err(|source| DaemonError::Parse {
                detail: source.to_string(),
            })?
            .with_replica_candidates(replica_candidates);
    let outcome = match runner.run_toolchain_command_with_stdin_policy_and_output_sink(
        dfs,
        &local_store,
        &request.sandbox_id,
        &request.command_name,
        request.argv,
        request.stdin,
        request.timeout_ms,
        &config.policy,
        output_sink,
    ) {
        Ok(outcome) => outcome,
        Err(source) => {
            match &source {
                sinomo_sandbox::SandboxCommandExecutionError::PolicyDenied(denial) => {
                    append_daemon_sandbox_event(
                        paths,
                        sinomo_sandbox::sandbox_policy_denied_event(
                            execution_node_id.clone(),
                            Some(&request.sandbox_id),
                            "sandbox-run",
                            denial,
                        ),
                    )?;
                    if denial.code.starts_with("mount-") {
                        append_daemon_sandbox_event(
                            paths,
                            sinomo_sandbox::sandbox_mount_denied_event(
                                execution_node_id.clone(),
                                Some(&request.sandbox_id),
                                "sandbox-run",
                                denial,
                            ),
                        )?;
                    }
                }
                sinomo_sandbox::SandboxCommandExecutionError::Scheduling(error) => {
                    append_daemon_sandbox_event(
                        paths,
                        sinomo_sandbox::sandbox_scheduling_failed_event(
                            execution_node_id.clone(),
                            &request.sandbox_id,
                            error,
                        ),
                    )?;
                }
                _ => {}
            }
            return Err(DaemonError::Parse {
                detail: source.to_string(),
            });
        }
    };
    let dfs = outcome.dfs;
    let node_id = dfs.node_id().clone();
    let command = outcome.command.map_err(|source| DaemonError::Parse {
        detail: source.to_string(),
    })?;
    let mut store = sinomo_sandbox::SifsSandboxInstanceStore::new(dfs);
    let manifest = store.load(&request.sandbox_id)?;
    let mut fs = sinomo_sandbox::SifsSandboxFileSystem::new(store.into_inner(), &manifest);
    let mut event_store = daemon_event_store(paths)?;
    append_daemon_sandbox_command_events(&mut event_store, node_id, &manifest, &mut fs, &command)?;
    Ok(command)
}

#[cfg(feature = "sandbox")]
fn append_daemon_sandbox_command_events<T: sinomo_dfs::DfsFrameTransport>(
    event_store: &mut sinomo_events::JsonlEventStore,
    node_id: sinomo_node::NodeId,
    manifest: &sinomo_sandbox::SandboxInstanceManifest,
    fs: &mut sinomo_sandbox::SifsSandboxFileSystem<T>,
    command: &sinomo_sandbox::SandboxLocalCommandOutcome,
) -> Result<(), DaemonError> {
    let record = sinomo_sandbox::load_command_record(fs, &command.response.command_id)?;
    event_store.append(sinomo_sandbox::sandbox_command_started_event(
        node_id.clone(),
        manifest,
        &record.command_id,
        &record.argv,
        &record.cwd,
        record.started_at,
    ))?;
    for chunk in &command.output_chunks {
        event_store.append(sinomo_sandbox::sandbox_command_output_event(
            node_id.clone(),
            chunk,
        ))?;
    }
    event_store.append(sinomo_sandbox::sandbox_command_record_event(
        node_id, &record,
    ))?;
    Ok(())
}

pub(crate) async fn handle_daemon_sandbox_run_toolchain_stream_async_on_runtime(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxRunToolchainRequest,
    stream: &mut sinomo_network::AsyncDaemonControlStream,
    stream_address: &str,
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
                        write_daemon_sandbox_stream_frame_async(stream, stream_address, &frame)
                            .await?;
                    }
                }
                let frame = sinomo_sandbox::SandboxRunToolchainStreamFrame::Complete { outcome };
                write_daemon_sandbox_stream_frame_async(stream, stream_address, &frame).await?;
                stream
                    .shutdown_write(stream_address)
                    .await
                    .map_err(DaemonError::from)?;
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
                write_daemon_sandbox_stream_frame_async(stream, stream_address, &frame).await?;
                stream
                    .shutdown_write(stream_address)
                    .await
                    .map_err(DaemonError::from)?;
                worker.await.map_err(|source| DaemonError::Network {
                    address: "daemon sandbox stream".to_string(),
                    detail: format!("sandbox stream task failed: {source}"),
                })?;
                return Ok(());
            }
        };
        write_daemon_sandbox_stream_frame_async(stream, stream_address, &frame).await?;
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
    stream: &mut sinomo_network::AsyncDaemonControlStream,
    stream_address: &str,
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
                write_daemon_sandbox_process_plan_stream_frame_async(
                    stream,
                    stream_address,
                    &frame,
                )
                .await?;
            }
            StreamMessage::Complete(Ok(outcome)) => {
                for step in &outcome.step_outcomes {
                    for chunk in &step.output_chunks {
                        if sent_output_chunks.insert(sandbox_output_chunk_stream_key(chunk)) {
                            let frame = sinomo_sandbox::SandboxRunProcessPlanStreamFrame::Output {
                                chunk: chunk.clone(),
                            };
                            write_daemon_sandbox_process_plan_stream_frame_async(
                                stream,
                                stream_address,
                                &frame,
                            )
                            .await?;
                        }
                    }
                }
                let frame = sinomo_sandbox::SandboxRunProcessPlanStreamFrame::Complete { outcome };
                write_daemon_sandbox_process_plan_stream_frame_async(
                    stream,
                    stream_address,
                    &frame,
                )
                .await?;
                stream
                    .shutdown_write(stream_address)
                    .await
                    .map_err(DaemonError::from)?;
                worker.await.map_err(|source| DaemonError::Network {
                    address: "daemon sandbox stream".to_string(),
                    detail: format!("sandbox stream task failed: {source}"),
                })?;
                return Ok(());
            }
            StreamMessage::Complete(Err(detail)) => {
                let frame = sinomo_sandbox::SandboxRunProcessPlanStreamFrame::Error { detail };
                write_daemon_sandbox_process_plan_stream_frame_async(
                    stream,
                    stream_address,
                    &frame,
                )
                .await?;
                stream
                    .shutdown_write(stream_address)
                    .await
                    .map_err(DaemonError::from)?;
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
    stream: &mut sinomo_network::AsyncDaemonControlStream,
    stream_address: &str,
    frame: &sinomo_sandbox::SandboxRunToolchainStreamFrame,
) -> Result<(), DaemonError> {
    let response = sinomo_network::ObjectRpcResponse::encode_body(
        sinomo_network::SANDBOX_RUN_TOOLCHAIN_STREAM_ROUTE,
        frame,
    )
    .map_err(DaemonError::from)?;
    write_daemon_rpc_response_frame_async(stream, stream_address, response).await
}

async fn write_daemon_sandbox_process_plan_stream_frame_async(
    stream: &mut sinomo_network::AsyncDaemonControlStream,
    stream_address: &str,
    frame: &sinomo_sandbox::SandboxRunProcessPlanStreamFrame,
) -> Result<(), DaemonError> {
    let response = sinomo_network::ObjectRpcResponse::encode_body(
        sinomo_network::SANDBOX_RUN_PROCESS_PLAN_STREAM_ROUTE,
        frame,
    )
    .map_err(DaemonError::from)?;
    write_daemon_rpc_response_frame_async(stream, stream_address, response).await
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
    node_id: &sinomo_node::NodeId,
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

fn validate_daemon_sandbox_toolchain_update_policy(
    paths: &DaemonPaths,
    policy: &sinomo_sandbox::SandboxPolicy,
    node_id: &sinomo_node::NodeId,
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
    local_node_id: &sinomo_node::NodeId,
) -> Vec<sinomo_node::NodeId> {
    let advertisements = cluster_paths_from_daemon_paths(paths)
        .and_then(|cluster_paths| {
            sinomo_kv::read_cluster_kv_prefix_entries(
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
    local_node_id: &sinomo_node::NodeId,
    advertisements: Vec<sinomo_dfs::DfsNodeAdvertisement>,
) -> Vec<sinomo_node::NodeId> {
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
) -> Result<sinomo_dfs::NetworkedDfsService<sinomo_dfs::DfsEndpointTransport>, DaemonError> {
    open_daemon_sandbox_dfs_service_on_runtime(paths, None)
}

pub(crate) fn open_daemon_sandbox_dfs_service_on_runtime(
    paths: &DaemonPaths,
    runtime: Option<&tokio::runtime::Handle>,
) -> Result<sinomo_dfs::NetworkedDfsService<sinomo_dfs::DfsEndpointTransport>, DaemonError> {
    let data_dir = daemon_data_dir_path(paths)?;
    let data_dir = sinomo_local::SinomoDataDir::new(&data_dir);
    data_dir.ensure_root_layout()?;
    let cluster_paths = sinomo_network::ClusterPaths::new(&data_dir);
    let state = sinomo_network::read_cluster_state_without_kv(&cluster_paths)?;
    let content = sinomo_dfs::LocalContentStore::new(data_dir.dfs_dir().join("content")).map_err(
        |source| DaemonError::Io {
            path: data_dir.dfs_dir().join("content").display().to_string(),
            detail: source.to_string(),
        },
    )?;
    let endpoints = sinomo_network::cluster_dfs_protocol_targets(&cluster_paths, &state);
    let transport = if let Some(runtime) = runtime {
        daemon_dfs_endpoint_transport_on_runtime(state.local_node_id.clone(), endpoints, runtime)
    } else {
        daemon_dfs_endpoint_transport(state.local_node_id.clone(), endpoints)
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
    let data_dir_handle = sinomo_local::SinomoDataDir::new(data_dir);
    let cluster_paths = sinomo_network::ClusterPaths::new(&data_dir_handle);
    let Ok(cluster_state) = sinomo_network::read_cluster_state_without_kv(&cluster_paths) else {
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
    let data_dir_handle = sinomo_local::SinomoDataDir::new(&data_dir);
    let cluster_paths = sinomo_network::ClusterPaths::new(&data_dir_handle);
    let Ok(cluster_state) = sinomo_network::read_cluster_state_without_kv(&cluster_paths) else {
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
    cluster_id: &sinomo_network::ClusterId,
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

fn daemon_rollout_controller_node_id(data_dir: &Path) -> Result<sinomo_node::NodeId, DaemonError> {
    let data_dir_handle = sinomo_local::SinomoDataDir::new(data_dir);
    let cluster_paths = sinomo_network::ClusterPaths::new(&data_dir_handle);
    if let Ok(state) = sinomo_network::read_cluster_state_without_kv(&cluster_paths) {
        return Ok(state.local_node_id);
    }
    let identity = sinomo_node::load_or_create_main_node_identity(data_dir)?;
    Ok(sinomo_node::NodeId::derived(
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

fn daemon_event_store(paths: &DaemonPaths) -> Result<sinomo_events::JsonlEventStore, DaemonError> {
    let data_dir = daemon_data_dir_path(paths)?;
    Ok(sinomo_events::JsonlEventStore::new(
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
    runtime: Option<&tokio::runtime::Handle>,
) -> Result<crate::DaemonStatus, DaemonError> {
    let read_paths = paths.clone();
    let read_path = read_paths.root.display().to_string();
    let mut status = tokio::task::spawn_blocking(move || read_daemon_status(&read_paths))
        .await
        .map_err(|source| DaemonError::Io {
            path: read_path,
            detail: format!("daemon status read task failed: {source}"),
        })??;
    if let Some(runtime) = runtime {
        ensure_daemon_status_cluster_async(paths, &mut status, runtime).await?;
    } else {
        ensure_daemon_status_cluster(paths, &mut status)?;
    }
    if let Some(data_dir) = paths.root.parent() {
        let rollout_paths = SelfRolloutPaths::new(data_dir);
        let rollout_path = rollout_paths.root.display().to_string();
        status.rollout = tokio::task::spawn_blocking(move || read_rollout_status(&rollout_paths))
            .await
            .map_err(|source| DaemonError::Io {
                path: rollout_path,
                detail: format!("rollout status read task failed: {source}"),
            })??;
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
    use sinomo_local::SinomoDataDir;
    use sinomo_node::NodeId;
    use std::net::{TcpListener, TcpStream};
    use std::time::{Duration, Instant};

    fn append_cluster_raft_command(
        paths: &sinomo_network::ClusterPaths,
        node_id: &NodeId,
        command: sinomo_kv::KvCommand,
    ) -> Result<sinomo_kv::KvSnapshot, sinomo_network::NetworkError> {
        paths
            .kv_paths()
            .raft_store()
            .append_command_and_publish(
                node_id,
                command,
                sinomo_network::cluster_kv_key_can_defer_materialized_snapshot,
            )
            .map_err(Into::into)
    }

    #[test]
    fn authenticated_rpc_auth_failure_returns_framed_error() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-auth-rpc-error");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("layout");
        let paths = DaemonPaths::new(&data_dir);
        let request = sinomo_network::ObjectRpcRequest::encode_body(
            sinomo_rollout::ROLLOUT_DIAGNOSTICS_ROUTE,
            ROLLOUT_RPC_CATEGORY,
            &(),
        )
        .expect("request");
        let control_request = sinomo_network::DaemonControlRequest::authenticated_rpc(
            daemon_rpc_command(sinomo_rollout::ROLLOUT_DIAGNOSTICS_ROUTE),
            request,
            b"not-a-signed-command".to_vec(),
        );
        let (address_tx, address_rx) = std::sync::mpsc::channel();
        let server = thread::spawn(move || {
            let runtime = crate::build_daemon_async_runtime("daemon-auth-test").expect("runtime");
            let service_runtime = runtime.handle().clone();
            runtime
                .block_on(async move {
                    let listener = sinomo_network::AsyncDaemonControlListener::bind("127.0.0.1:0")
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
            .block_on(sinomo_network::send_daemon_control_request_async(
                &address,
                &control_request,
                sinomo_network::DaemonRpcTimeouts::connect_and_idle(Duration::from_secs(2)),
                DAEMON_COMMAND_MAX_BYTES,
            ))
            .expect("framed auth error response");
        let rpc = response.rpc.expect("rpc error response");
        assert_eq!(rpc.route, sinomo_rollout::ROLLOUT_DIAGNOSTICS_ROUTE);
        let error = rpc
            .error_detail()
            .expect_err("auth failures are RPC errors");
        assert!(matches!(error, sinomo_network::NetworkError::AuthDenied));
        server.join().expect("server thread");
    }

    #[test]
    fn sandbox_replica_candidates_use_advertised_content_services_and_local_fallback() {
        let local = NodeId::from_legacy_named("node_local").expect("local node");
        let replica = NodeId::from_legacy_named("node_replica").expect("replica node");
        let metadata_only = NodeId::from_legacy_named("node_metadata").expect("metadata node");
        let candidates = daemon_sandbox_replica_candidates_from_content_advertisements(
            &local,
            vec![
                sinomo_dfs::DfsNodeAdvertisement::metadata(metadata_only, "127.0.0.1:1"),
                sinomo_dfs::DfsNodeAdvertisement::content(replica.clone(), "127.0.0.1:2"),
                sinomo_dfs::DfsNodeAdvertisement::content(local.clone(), "127.0.0.1:3"),
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
            sinomo_events::JsonlEventStore::new(root.join("events").join("event-log.jsonl"));
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
            sinomo_events::JsonlEventStore::new(root.join("events").join("event-log.jsonl"));
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
            sinomo_events::JsonlEventStore::new(root.join("events").join("event-log.jsonl"));
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
        let cluster_paths = sinomo_network::ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let cluster_id =
            sinomo_network::ClusterId::from_legacy_named("cluster_daemon_module_rollout_status")
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
        sinomo_network::write_cluster_state(&cluster_paths, &cluster_state)
            .expect("write cluster state");
        sinomo_network::sync_cluster_state_to_local_raft_unchecked(&cluster_paths, &cluster_state)
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
                last_consumed_sequence: Some(sinomo_events::EventSequence(7)),
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
        let cluster_paths = sinomo_network::ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let cluster_id =
            sinomo_network::ClusterId::from_legacy_named("cluster_daemon_desired_blocked")
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
        sinomo_network::write_cluster_state(&cluster_paths, &cluster_state)
            .expect("write cluster state");
        sinomo_network::sync_cluster_state_to_local_raft_unchecked(&cluster_paths, &cluster_state)
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
        let cluster_paths = sinomo_network::ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let cluster_id =
            sinomo_network::ClusterId::from_legacy_named("cluster_daemon_desired_prepared")
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
        sinomo_network::write_cluster_state(&cluster_paths, &cluster_state)
            .expect("write cluster state");
        sinomo_network::sync_cluster_state_to_local_raft_unchecked(&cluster_paths, &cluster_state)
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
        let cluster_paths = sinomo_network::ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let cluster_id =
            sinomo_network::ClusterId::from_legacy_named("cluster_daemon_desired_matched")
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
        sinomo_network::write_cluster_state(&cluster_paths, &cluster_state)
            .expect("write cluster state");
        sinomo_network::sync_cluster_state_to_local_raft_unchecked(&cluster_paths, &cluster_state)
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
        let cluster_paths = sinomo_network::ClusterPaths::new(&data_dir);
        let now = OffsetDateTime::now_utc();
        let cluster_id = sinomo_network::ClusterId::from_legacy_named("cluster_daemon_sharded_dfs")
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
        sinomo_network::write_cluster_state(&cluster_paths, &state).expect("write state");
        sinomo_network::sync_cluster_state_to_local_raft_unchecked(&cluster_paths, &state)
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
            sinomo_kv::KvCommand::Txn(sinomo_kv::KvTxn {
                compares: Vec::new(),
                success: vec![
                    sinomo_kv::KvOp::put_serde(
                        sinomo_dfs::metadata_shard_key(&file_system.file_system_id, "root"),
                        &root_shard,
                        None,
                    )
                    .expect("root shard op"),
                    sinomo_kv::KvOp::put_serde(
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
            "status should not be blocked by an idle TCP probe: {status_result:?}"
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
                Some(DAEMON_SHADOW_ROLLOUT_IDLE_FINALIZATION_INTERVAL),
            ),
            Duration::from_secs(30),
            "quiet main daemons should sleep until cluster discovery/publish/probe work is due"
        );
        assert_eq!(
            daemon_maintenance_sleep_duration(
                now,
                Duration::from_secs(30),
                last_dfs,
                last_rollout,
                None,
                Some(Duration::from_secs(5)),
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
                None,
            ),
            Duration::from_secs(3),
            "pending DFS repair or namespace work should wake before the fixed five-minute tick"
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
            ),
            Duration::from_millis(1),
            "overdue work should not create a zero-duration spin"
        );
    }

    #[test]
    fn background_dfs_maintenance_defaults_on_with_disable_override() {
        assert!(background_dfs_maintenance_enabled_for(None));
        assert!(background_dfs_maintenance_enabled_for(Some("1")));
        assert!(background_dfs_maintenance_enabled_for(Some("true")));
        assert!(background_dfs_maintenance_enabled_for(Some(" yes ")));
        assert!(background_dfs_maintenance_enabled_for(Some("unexpected")));

        assert!(!background_dfs_maintenance_enabled_for(Some("0")));
        assert!(!background_dfs_maintenance_enabled_for(Some("false")));
        assert!(!background_dfs_maintenance_enabled_for(Some(" no ")));
        assert!(!background_dfs_maintenance_enabled_for(Some("OFF")));
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

        std::fs::create_dir_all(&rollout_paths.root).expect("rollout dir");
        std::fs::write(rollout_shadow_finalize_path(&rollout_paths), b"pending")
            .expect("pending shadow marker");

        assert_eq!(
            shadow_rollout_finalization_poll_due_in(&paths),
            DAEMON_CLUSTER_TICK_INTERVAL,
            "active shadow finalization keeps the short fallback poll interval"
        );
    }

    #[cfg(feature = "sandbox")]
    #[test]
    fn sandbox_active_execution_guard_enforces_policy_limits_and_releases() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-sandbox-active");
        let paths = DaemonPaths::with_root(root.to_path_buf());
        let mut policy = sinomo_sandbox::SandboxPolicy {
            max_concurrent_commands_per_sandbox: 1,
            max_concurrent_sandboxes_per_node: 1,
            ..sinomo_sandbox::SandboxPolicy::default()
        };
        let sandbox_a =
            sinomo_sandbox::SandboxId::from_legacy_named("sandbox_active_a").expect("sandbox a");
        let sandbox_b =
            sinomo_sandbox::SandboxId::from_legacy_named("sandbox_active_b").expect("sandbox b");

        let guard = acquire_sandbox_active_execution(&paths, &sandbox_a, &policy)
            .expect("first sandbox admitted");
        assert_eq!(
            acquire_sandbox_active_execution(&paths, &sandbox_a, &policy)
                .expect_err("second command for same sandbox denied")
                .code,
            "concurrent-commands-denied"
        );
        assert_eq!(
            acquire_sandbox_active_execution(&paths, &sandbox_b, &policy)
                .expect_err("second active sandbox denied")
                .code,
            "concurrent-sandboxes-denied"
        );

        drop(guard);
        let _guard = acquire_sandbox_active_execution(&paths, &sandbox_b, &policy)
            .expect("released slot can be reused");

        policy.max_concurrent_commands_per_sandbox = 0;
        let sandbox_c =
            sinomo_sandbox::SandboxId::from_legacy_named("sandbox_active_c").expect("sandbox c");
        assert_eq!(
            acquire_sandbox_active_execution(&paths, &sandbox_c, &policy)
                .expect_err("zero command limit denies new command")
                .code,
            "concurrent-commands-denied"
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
        let identity =
            sinomo_node::load_or_create_node_identity(&identity_path).expect("signer identity");
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
            sinomo_events::EventId::generate(),
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
            sinomo_events::EventId::generate(),
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
        let request = sinomo_network::ObjectRpcRequest::encode_body(
            sinomo_network::DAEMON_STATUS_ROUTE,
            sinomo_network::DAEMON_RPC_CATEGORY,
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
        let request = sinomo_network::ObjectRpcRequest::encode_body(
            sinomo_network::DAEMON_SHUTDOWN_ROUTE,
            sinomo_network::DAEMON_RPC_CATEGORY,
            &(),
        )?;
        let response = send_test_rpc(data_dir, address, &request, timeout)?;
        response.decode_body().map_err(DaemonError::from)
    }

    fn send_test_rpc(
        data_dir: &SinomoDataDir,
        address: &str,
        request: &sinomo_network::ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<sinomo_network::ObjectRpcResponse, DaemonError> {
        let signed = sign_daemon_rpc_request_from_data_dir(data_dir, request)?;
        crate::send_daemon_rpc_raw(
            address,
            &daemon_rpc_command(&request.route),
            Some(&signed),
            request,
            timeout,
        )
        .map_err(DaemonError::from)
    }
}
