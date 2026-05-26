use crate::DaemonSignerAuthority;
use crate::{
    AuthenticatedDaemonCommand, ClusterInviteChallengeRequest, ClusterNetworkMaintenance,
    DAEMON_CLUSTER_TICK_INTERVAL, DaemonClusterStatus, DaemonDfsEndpointTransport, DaemonError,
    DaemonLock, DaemonPaths, authenticate_daemon_control_request,
    create_cluster_invite_challenge_proof, daemon_paths_are_shadow, daemon_rpc_command,
    ensure_daemon_status_cluster, ensure_default_distributed_dfs, ensure_default_local_dfs_expose,
    handle_daemon_cluster_invite_publish_request, handle_daemon_cluster_join_request,
    handle_daemon_cluster_kv_range_prefix, handle_daemon_cluster_kv_snapshot,
    handle_daemon_cluster_kv_txn_request, handle_daemon_cluster_kv_watch_drain_request,
    handle_daemon_cluster_kv_watch_events_request, handle_daemon_cluster_kv_watch_prefix_request,
    handle_daemon_cluster_member_publish_request, handle_daemon_cluster_raft_append_request,
    handle_daemon_cluster_raft_vote_request, handle_daemon_dfs_expose_config,
    handle_daemon_dfs_metadata_journal_batch, handle_daemon_dfs_protocol_frame,
    handle_daemon_dfs_status, handle_daemon_finalize_shadow_rollout,
    handle_daemon_rollout_binary_request, handle_daemon_rollout_build_request,
    handle_daemon_rollout_diagnostics, handle_forwarded_daemon_cluster_kv_txn_request,
    read_daemon_status, run_daemon_dfs_maintenance_tick, start_daemon_iroh_proxy_if_clustered,
};
use sinomo_events::EventStore;
use sinomo_rollout::{SelfRolloutPaths, read_rollout_status};
use sinomo_sandbox::SandboxFileSystem;
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, mpsc};
use std::thread;
use std::time::Duration;
use time::OffsetDateTime;

pub const DAEMON_CLIENT_READ_TIMEOUT: Duration = Duration::from_secs(10);
pub const DAEMON_CLIENT_WRITE_TIMEOUT: Duration = Duration::from_secs(10);
pub const DAEMON_COMMAND_MAX_BYTES: usize = 128 * 1024 * 1024;
pub const DAEMON_ACCEPT_IDLE_INTERVAL: Duration = Duration::from_millis(100);
pub const DAEMON_DFS_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(300);

static SANDBOX_ACTIVE_EXECUTIONS: OnceLock<Mutex<SandboxActiveExecutions>> = OnceLock::new();

#[derive(Default)]
struct SandboxActiveExecutions {
    counts: BTreeMap<(PathBuf, sinomo_sandbox::SandboxId), u64>,
}

#[derive(Debug)]
struct SandboxActiveExecutionGuard {
    root: PathBuf,
    sandbox_id: sinomo_sandbox::SandboxId,
}

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

pub fn handle_authenticated_daemon_connection<F, E>(
    stream: &mut sinomo_network::DaemonControlStream,
    paths: &DaemonPaths,
    handler: F,
) -> Result<bool, E>
where
    F: FnOnce(
        AuthenticatedDaemonCommand,
        Option<sinomo_network::ObjectRpcRequest>,
        &mut sinomo_network::DaemonControlStream,
    ) -> Result<bool, E>,
    E: From<DaemonError> + std::fmt::Display,
{
    prepare_daemon_client_stream(stream)?;
    let mut request = match read_daemon_control_frame(stream) {
        Ok(request) => request,
        Err(_) => {
            stream.shutdown_both();
            return Ok(false);
        }
    };
    let command = request.command.trim().to_string();
    if command == daemon_rpc_command(sinomo_network::CLUSTER_JOIN_CHALLENGE_ROUTE) {
        let Some(rpc_request) = request.rpc.as_ref() else {
            stream.shutdown_both();
            return Ok(false);
        };
        if rpc_request.route != sinomo_network::CLUSTER_JOIN_CHALLENGE_ROUTE
            || rpc_request.category != sinomo_network::CLUSTER_MEMBERSHIP_RPC_CATEGORY
        {
            stream.shutdown_both();
            return Ok(false);
        }
        let challenge: ClusterInviteChallengeRequest = match rpc_request.decode_body() {
            Ok(challenge) => challenge,
            Err(_) => {
                stream.shutdown_both();
                return Ok(false);
            }
        };
        let proof = match create_cluster_invite_challenge_proof(paths, &challenge) {
            Ok(proof) => proof,
            Err(_error) => {
                stream.shutdown_both();
                return Ok(false);
            }
        };
        let response = sinomo_network::ObjectRpcResponse::encode_body(
            sinomo_network::CLUSTER_JOIN_CHALLENGE_ROUTE,
            &proof,
        )
        .map_err(DaemonError::from)?;
        write_daemon_rpc_response_frame(stream, response)?;
        stream.flush().map_err(|source| DaemonError::Network {
            address: "daemon client".to_string(),
            detail: source.to_string(),
        })?;
        request = match read_daemon_control_frame(stream) {
            Ok(request) => request,
            Err(_) => return Ok(false),
        };
    }
    let authenticated = match authenticate_daemon_control_request(paths, &request) {
        Ok(authenticated) => authenticated,
        Err(error) => {
            if is_cluster_join_register_request(&request) {
                eprintln!("sinomo daemon client auth failed for {command}: {error}");
            }
            stream.shutdown_both();
            return Ok(false);
        }
    };
    let error_route = request
        .rpc
        .as_ref()
        .map(|rpc| rpc.route.clone())
        .unwrap_or_else(|| command.clone());
    match handler(authenticated, request.rpc, stream) {
        Ok(value) => Ok(value),
        Err(error) => {
            if let Ok(response) =
                sinomo_network::ObjectRpcResponse::encode_error(error_route, error.to_string())
            {
                let _ = write_daemon_rpc_response_frame(stream, response);
                let _ = stream.flush();
            }
            Err(error)
        }
    }
}

fn is_cluster_join_register_request(request: &sinomo_network::DaemonControlRequest) -> bool {
    let Some(rpc) = request.rpc.as_ref() else {
        return false;
    };
    request.command.trim() == daemon_rpc_command(sinomo_network::CLUSTER_JOIN_REGISTER_ROUTE)
        && rpc.route == sinomo_network::CLUSTER_JOIN_REGISTER_ROUTE
        && rpc.category == sinomo_network::CLUSTER_MEMBERSHIP_RPC_CATEGORY
}

fn prepare_daemon_client_stream(
    stream: &sinomo_network::DaemonControlStream,
) -> Result<(), DaemonError> {
    stream.set_nonblocking(false).map_err(DaemonError::from)?;
    stream
        .set_read_timeout(Some(DAEMON_CLIENT_READ_TIMEOUT))
        .map_err(DaemonError::from)?;
    stream
        .set_write_timeout(Some(DAEMON_CLIENT_WRITE_TIMEOUT))
        .map_err(DaemonError::from)
}

pub fn read_daemon_control_frame(
    stream: &mut sinomo_network::DaemonControlStream,
) -> Result<sinomo_network::DaemonControlRequest, DaemonError> {
    let request: sinomo_network::DaemonControlRequest =
        sinomo_network::read_object_frame_blocking(stream, DAEMON_COMMAND_MAX_BYTES)
            .map_err(DaemonError::from)?;
    Ok(request)
}

pub fn write_daemon_rpc_response_frame(
    stream: &mut sinomo_network::DaemonControlStream,
    response: sinomo_network::ObjectRpcResponse,
) -> Result<(), DaemonError> {
    sinomo_network::write_object_frame_blocking(
        stream,
        &sinomo_network::DaemonControlResponse::rpc(String::new(), response),
    )
    .map_err(DaemonError::from)
}

pub fn run_daemon_control_loop(
    options: crate::DaemonRunOptions,
    paths: DaemonPaths,
    label: &str,
    cluster: Option<DaemonClusterStatus>,
) -> Result<(), DaemonError> {
    let _lock = DaemonLock::acquire(&paths, &options, cluster.clone())?;
    if label == "daemon" {
        ensure_default_local_dfs_expose(&paths)?;
    }
    let bind_address = format!("{}:{}", options.host, options.port);
    let listener = sinomo_network::DaemonControlListener::bind(&bind_address)?;
    listener.set_nonblocking(true)?;
    println!("sinomo {label} listening on {bind_address}");
    if let Err(error) =
        start_daemon_iroh_proxy_if_clustered(&paths, &options, cluster.as_ref(), label)
    {
        eprintln!("sinomo {label} iroh proxy disabled: {error}");
    }

    if label == "daemon" {
        let bootstrap_paths = paths.clone();
        let bootstrap_options = options.clone();
        let bootstrap_label = label.to_string();
        thread::Builder::new()
            .name("sinomo-dfs-bootstrap".to_string())
            .spawn(move || {
                if let Err(error) =
                    ensure_default_distributed_dfs(&bootstrap_paths, &bootstrap_options, false)
                {
                    eprintln!("sinomo {bootstrap_label} distributed DFS bootstrap failed: {error}");
                }
            })
            .map_err(|source| DaemonError::Io {
                path: "spawn distributed DFS bootstrap thread".to_string(),
                detail: source.to_string(),
            })?;
    }

    let maintenance_paths = paths.clone();
    let maintenance_options = options.clone();
    let maintenance_label = label.to_string();
    thread::Builder::new()
        .name(format!("sinomo-{label}-cluster"))
        .spawn(move || {
            let mut cluster_network = ClusterNetworkMaintenance::new();
            let mut last_dfs_maintenance = std::time::Instant::now();
            loop {
                if let Err(error) = cluster_network.tick(&maintenance_paths, &maintenance_options) {
                    eprintln!("sinomo {maintenance_label} cluster network tick failed: {error}");
                }
                if maintenance_label == "daemon"
                    && last_dfs_maintenance.elapsed() >= DAEMON_DFS_MAINTENANCE_INTERVAL
                    && let Err(error) = run_daemon_dfs_maintenance_tick(&maintenance_paths)
                {
                    eprintln!("sinomo {maintenance_label} DFS maintenance tick failed: {error}");
                }
                if maintenance_label == "daemon"
                    && last_dfs_maintenance.elapsed() >= DAEMON_DFS_MAINTENANCE_INTERVAL
                {
                    last_dfs_maintenance = std::time::Instant::now();
                }
                thread::sleep(DAEMON_CLUSTER_TICK_INTERVAL);
            }
        })
        .map_err(|source| DaemonError::Io {
            path: "spawn cluster maintenance thread".to_string(),
            detail: source.to_string(),
        })?;

    let (shutdown_tx, shutdown_rx) = std::sync::mpsc::channel::<()>();
    loop {
        if shutdown_rx.try_recv().is_ok() {
            break;
        }
        match listener.accept() {
            Ok(Some(mut stream)) => {
                let client_paths = paths.clone();
                let client_label = label.to_string();
                let client_shutdown_tx = shutdown_tx.clone();
                thread::Builder::new()
                    .name(format!("sinomo-{label}-client"))
                    .spawn(
                        move || match handle_daemon_connection(&mut stream, &client_paths) {
                            Ok(true) => {
                                let _ = client_shutdown_tx.send(());
                            }
                            Ok(false) => {}
                            Err(error) => eprintln!("sinomo {client_label} client error: {error}"),
                        },
                    )
                    .map_err(|source| DaemonError::Io {
                        path: "spawn daemon client handler".to_string(),
                        detail: source.to_string(),
                    })?;
            }
            Ok(None) => thread::sleep(DAEMON_ACCEPT_IDLE_INTERVAL),
            Err(source) => {
                return Err(DaemonError::Network {
                    address: bind_address.clone(),
                    detail: source.to_string(),
                });
            }
        }
    }
    Ok(())
}

pub fn handle_daemon_connection(
    stream: &mut sinomo_network::DaemonControlStream,
    paths: &DaemonPaths,
) -> Result<bool, DaemonError> {
    handle_authenticated_daemon_connection(stream, paths, |authenticated, rpc_request, stream| {
        let command = authenticated.command.as_str();
        match command {
            command if command == daemon_rpc_command(sinomo_network::DAEMON_STATUS_ROUTE) => {
                let request = require_daemon_rpc_request(
                    rpc_request,
                    sinomo_network::DAEMON_STATUS_ROUTE,
                    sinomo_network::DAEMON_RPC_CATEGORY,
                )?;
                let _: () = request.decode_body().map_err(DaemonError::from)?;
                let status = daemon_status_response(paths)?;
                let response = sinomo_network::ObjectRpcResponse::encode_body(
                    sinomo_network::DAEMON_STATUS_ROUTE,
                    &status,
                )
                .map_err(DaemonError::from)?;
                write_daemon_rpc_response_frame(stream, response)?;
                Ok(false)
            }
            command if command == daemon_rpc_command(sinomo_network::DAEMON_SHUTDOWN_ROUTE) => {
                let request = require_daemon_rpc_request(
                    rpc_request,
                    sinomo_network::DAEMON_SHUTDOWN_ROUTE,
                    sinomo_network::DAEMON_RPC_CATEGORY,
                )?;
                let _: () = request.decode_body().map_err(DaemonError::from)?;
                let response = sinomo_network::ObjectRpcResponse::encode_body(
                    sinomo_network::DAEMON_SHUTDOWN_ROUTE,
                    &"sinomo daemon stopping".to_string(),
                )
                .map_err(DaemonError::from)?;
                write_daemon_rpc_response_frame(stream, response)?;
                Ok(true)
            }
            command if command == daemon_rpc_command(sinomo_network::CLUSTER_KV_SNAPSHOT_ROUTE) => {
                let request = require_daemon_rpc_request(
                    rpc_request,
                    sinomo_network::CLUSTER_KV_SNAPSHOT_ROUTE,
                    sinomo_network::CLUSTER_KV_RPC_CATEGORY,
                )?;
                let _: () = request.decode_body().map_err(DaemonError::from)?;
                let snapshot = handle_daemon_cluster_kv_snapshot(paths)?;
                let response = sinomo_network::ObjectRpcResponse::encode_body(
                    sinomo_network::CLUSTER_KV_SNAPSHOT_ROUTE,
                    &snapshot,
                )
                .map_err(DaemonError::from)?;
                write_daemon_rpc_response_frame(stream, response)?;
                Ok(false)
            }
            command if command == daemon_rpc_command(sinomo_network::CLUSTER_KV_TXN_ROUTE) => {
                let request = require_daemon_rpc_request(
                    rpc_request,
                    sinomo_network::CLUSTER_KV_TXN_ROUTE,
                    sinomo_network::CLUSTER_KV_RPC_CATEGORY,
                )?;
                let txn = request.decode_body().map_err(DaemonError::from)?;
                let result = if authenticated.signer == DaemonSignerAuthority::Local {
                    handle_daemon_cluster_kv_txn_request(paths, txn)
                } else {
                    handle_forwarded_daemon_cluster_kv_txn_request(paths, txn)
                }?;
                let response = sinomo_network::ObjectRpcResponse::encode_body(
                    sinomo_network::CLUSTER_KV_TXN_ROUTE,
                    &result,
                )
                .map_err(DaemonError::from)?;
                write_daemon_rpc_response_frame(stream, response)?;
                Ok(false)
            }
            command
                if command == daemon_rpc_command(sinomo_network::CLUSTER_KV_RANGE_PREFIX_ROUTE) =>
            {
                let request = require_daemon_rpc_request(
                    rpc_request,
                    sinomo_network::CLUSTER_KV_RANGE_PREFIX_ROUTE,
                    sinomo_network::CLUSTER_KV_RPC_CATEGORY,
                )?;
                let prefix: String = request.decode_body().map_err(DaemonError::from)?;
                let result = handle_daemon_cluster_kv_range_prefix(paths, &prefix)?;
                let response = sinomo_network::ObjectRpcResponse::encode_body(
                    sinomo_network::CLUSTER_KV_RANGE_PREFIX_ROUTE,
                    &result,
                )
                .map_err(DaemonError::from)?;
                write_daemon_rpc_response_frame(stream, response)?;
                Ok(false)
            }
            command
                if command == daemon_rpc_command(sinomo_network::CLUSTER_KV_WATCH_PREFIX_ROUTE) =>
            {
                let request = require_daemon_rpc_request(
                    rpc_request,
                    sinomo_network::CLUSTER_KV_WATCH_PREFIX_ROUTE,
                    sinomo_network::CLUSTER_KV_RPC_CATEGORY,
                )?;
                let request = request.decode_body().map_err(DaemonError::from)?;
                let result = handle_daemon_cluster_kv_watch_prefix_request(paths, request)?;
                let response = sinomo_network::ObjectRpcResponse::encode_body(
                    sinomo_network::CLUSTER_KV_WATCH_PREFIX_ROUTE,
                    &result,
                )
                .map_err(DaemonError::from)?;
                write_daemon_rpc_response_frame(stream, response)?;
                Ok(false)
            }
            command
                if command == daemon_rpc_command(sinomo_network::CLUSTER_KV_WATCH_DRAIN_ROUTE) =>
            {
                let request = require_daemon_rpc_request(
                    rpc_request,
                    sinomo_network::CLUSTER_KV_WATCH_DRAIN_ROUTE,
                    sinomo_network::CLUSTER_KV_RPC_CATEGORY,
                )?;
                let request = request.decode_body().map_err(DaemonError::from)?;
                let result = handle_daemon_cluster_kv_watch_drain_request(paths, request)?;
                let response = sinomo_network::ObjectRpcResponse::encode_body(
                    sinomo_network::CLUSTER_KV_WATCH_DRAIN_ROUTE,
                    &result,
                )
                .map_err(DaemonError::from)?;
                write_daemon_rpc_response_frame(stream, response)?;
                Ok(false)
            }
            command
                if command == daemon_rpc_command(sinomo_network::ROLLOUT_FINALIZE_SHADOW_ROUTE) =>
            {
                let request = require_daemon_rpc_request(
                    rpc_request,
                    sinomo_network::ROLLOUT_FINALIZE_SHADOW_ROUTE,
                    sinomo_network::ROLLOUT_RPC_CATEGORY,
                )?;
                let _: () = request.decode_body().map_err(DaemonError::from)?;
                let response = handle_daemon_finalize_shadow_rollout(paths)?;
                let response = sinomo_network::ObjectRpcResponse::encode_body(
                    sinomo_network::ROLLOUT_FINALIZE_SHADOW_ROUTE,
                    &response,
                )
                .map_err(DaemonError::from)?;
                write_daemon_rpc_response_frame(stream, response)?;
                Ok(false)
            }
            command if command == daemon_rpc_command(sinomo_network::ROLLOUT_DIAGNOSTICS_ROUTE) => {
                let request = require_daemon_rpc_request(
                    rpc_request,
                    sinomo_network::ROLLOUT_DIAGNOSTICS_ROUTE,
                    sinomo_network::ROLLOUT_RPC_CATEGORY,
                )?;
                let _: () = request.decode_body().map_err(DaemonError::from)?;
                let response = handle_daemon_rollout_diagnostics(paths)?;
                let response = sinomo_network::ObjectRpcResponse::encode_body(
                    sinomo_network::ROLLOUT_DIAGNOSTICS_ROUTE,
                    &response,
                )
                .map_err(DaemonError::from)?;
                write_daemon_rpc_response_frame(stream, response)?;
                Ok(false)
            }
            command if command == daemon_rpc_command(sinomo_network::DFS_EXPOSE_CONFIG_ROUTE) => {
                let request = require_daemon_rpc_request(
                    rpc_request,
                    sinomo_network::DFS_EXPOSE_CONFIG_ROUTE,
                    sinomo_network::DFS_RPC_CATEGORY,
                )?;
                let _: () = request.decode_body().map_err(DaemonError::from)?;
                let result = handle_daemon_dfs_expose_config(paths)?;
                let response = sinomo_network::ObjectRpcResponse::encode_body(
                    sinomo_network::DFS_EXPOSE_CONFIG_ROUTE,
                    &result,
                )
                .map_err(DaemonError::from)?;
                write_daemon_rpc_response_frame(stream, response)?;
                Ok(false)
            }
            command if command == daemon_rpc_command(sinomo_network::DFS_STATUS_ROUTE) => {
                let request = require_daemon_rpc_request(
                    rpc_request,
                    sinomo_network::DFS_STATUS_ROUTE,
                    sinomo_network::DFS_RPC_CATEGORY,
                )?;
                let _: () = request.decode_body().map_err(DaemonError::from)?;
                let result = handle_daemon_dfs_status(paths)?;
                let response = sinomo_network::ObjectRpcResponse::encode_body(
                    sinomo_network::DFS_STATUS_ROUTE,
                    &result,
                )
                .map_err(DaemonError::from)?;
                write_daemon_rpc_response_frame(stream, response)?;
                Ok(false)
            }
            command
                if command
                    == daemon_rpc_command(sinomo_network::DFS_METADATA_JOURNAL_BATCH_ROUTE) =>
            {
                let request = require_daemon_rpc_request(
                    rpc_request,
                    sinomo_network::DFS_METADATA_JOURNAL_BATCH_ROUTE,
                    sinomo_network::DFS_RPC_CATEGORY,
                )?;
                let request = request.decode_body().map_err(DaemonError::from)?;
                let result = handle_daemon_dfs_metadata_journal_batch(paths, request)?;
                let response = sinomo_network::ObjectRpcResponse::encode_body(
                    sinomo_network::DFS_METADATA_JOURNAL_BATCH_ROUTE,
                    &result,
                )
                .map_err(DaemonError::from)?;
                write_daemon_rpc_response_frame(stream, response)?;
                Ok(false)
            }
            command if command == daemon_rpc_command(sinomo_network::DFS_PROTOCOL_FRAME_ROUTE) => {
                let request = require_daemon_rpc_request(
                    rpc_request,
                    sinomo_network::DFS_PROTOCOL_FRAME_ROUTE,
                    sinomo_network::DFS_RPC_CATEGORY,
                )?;
                let request = request.decode_body().map_err(DaemonError::from)?;
                let result = handle_daemon_dfs_protocol_frame(paths, request)?;
                let response = sinomo_network::ObjectRpcResponse::encode_body(
                    sinomo_network::DFS_PROTOCOL_FRAME_ROUTE,
                    &result,
                )
                .map_err(DaemonError::from)?;
                write_daemon_rpc_response_frame(stream, response)?;
                Ok(false)
            }
            command if command == daemon_rpc_command(sinomo_network::SANDBOX_HTTP_FETCH_ROUTE) => {
                let request = require_daemon_rpc_request(
                    rpc_request,
                    sinomo_network::SANDBOX_HTTP_FETCH_ROUTE,
                    sinomo_network::SANDBOX_RPC_CATEGORY,
                )?;
                let request = request.decode_body().map_err(DaemonError::from)?;
                let response = handle_daemon_sandbox_http_fetch(paths, request)?;
                let response = sinomo_network::ObjectRpcResponse::encode_body(
                    sinomo_network::SANDBOX_HTTP_FETCH_ROUTE,
                    &response,
                )
                .map_err(DaemonError::from)?;
                write_daemon_rpc_response_frame(stream, response)?;
                Ok(false)
            }
            command
                if command
                    == daemon_rpc_command(sinomo_network::SANDBOX_HTTP_FETCH_TO_FILE_ROUTE) =>
            {
                let request = require_daemon_rpc_request(
                    rpc_request,
                    sinomo_network::SANDBOX_HTTP_FETCH_TO_FILE_ROUTE,
                    sinomo_network::SANDBOX_RPC_CATEGORY,
                )?;
                let request = request.decode_body().map_err(DaemonError::from)?;
                let response = handle_daemon_sandbox_http_fetch_to_file(paths, request)?;
                let response = sinomo_network::ObjectRpcResponse::encode_body(
                    sinomo_network::SANDBOX_HTTP_FETCH_TO_FILE_ROUTE,
                    &response,
                )
                .map_err(DaemonError::from)?;
                write_daemon_rpc_response_frame(stream, response)?;
                Ok(false)
            }
            command if command == daemon_rpc_command(sinomo_network::SANDBOX_CREATE_ROUTE) => {
                let request = require_daemon_rpc_request(
                    rpc_request,
                    sinomo_network::SANDBOX_CREATE_ROUTE,
                    sinomo_network::SANDBOX_RPC_CATEGORY,
                )?;
                let request = request.decode_body().map_err(DaemonError::from)?;
                let response = handle_daemon_sandbox_create(paths, request)?;
                let response = sinomo_network::ObjectRpcResponse::encode_body(
                    sinomo_network::SANDBOX_CREATE_ROUTE,
                    &response,
                )
                .map_err(DaemonError::from)?;
                write_daemon_rpc_response_frame(stream, response)?;
                Ok(false)
            }
            command if command == daemon_rpc_command(sinomo_network::SANDBOX_STATUS_ROUTE) => {
                let request = require_daemon_rpc_request(
                    rpc_request,
                    sinomo_network::SANDBOX_STATUS_ROUTE,
                    sinomo_network::SANDBOX_RPC_CATEGORY,
                )?;
                let request = request.decode_body().map_err(DaemonError::from)?;
                let response = handle_daemon_sandbox_status(paths, request)?;
                let response = sinomo_network::ObjectRpcResponse::encode_body(
                    sinomo_network::SANDBOX_STATUS_ROUTE,
                    &response,
                )
                .map_err(DaemonError::from)?;
                write_daemon_rpc_response_frame(stream, response)?;
                Ok(false)
            }
            command if command == daemon_rpc_command(sinomo_network::SANDBOX_UPDATE_ROUTE) => {
                let request = require_daemon_rpc_request(
                    rpc_request,
                    sinomo_network::SANDBOX_UPDATE_ROUTE,
                    sinomo_network::SANDBOX_RPC_CATEGORY,
                )?;
                let request = request.decode_body().map_err(DaemonError::from)?;
                let response = handle_daemon_sandbox_update(paths, request)?;
                let response = sinomo_network::ObjectRpcResponse::encode_body(
                    sinomo_network::SANDBOX_UPDATE_ROUTE,
                    &response,
                )
                .map_err(DaemonError::from)?;
                write_daemon_rpc_response_frame(stream, response)?;
                Ok(false)
            }
            command if command == daemon_rpc_command(sinomo_network::SANDBOX_CHECKPOINT_ROUTE) => {
                let request = require_daemon_rpc_request(
                    rpc_request,
                    sinomo_network::SANDBOX_CHECKPOINT_ROUTE,
                    sinomo_network::SANDBOX_RPC_CATEGORY,
                )?;
                let request = request.decode_body().map_err(DaemonError::from)?;
                let response = handle_daemon_sandbox_checkpoint(paths, request)?;
                let response = sinomo_network::ObjectRpcResponse::encode_body(
                    sinomo_network::SANDBOX_CHECKPOINT_ROUTE,
                    &response,
                )
                .map_err(DaemonError::from)?;
                write_daemon_rpc_response_frame(stream, response)?;
                Ok(false)
            }
            command if command == daemon_rpc_command(sinomo_network::SANDBOX_RESUME_ROUTE) => {
                let request = require_daemon_rpc_request(
                    rpc_request,
                    sinomo_network::SANDBOX_RESUME_ROUTE,
                    sinomo_network::SANDBOX_RPC_CATEGORY,
                )?;
                let request = request.decode_body().map_err(DaemonError::from)?;
                let response = handle_daemon_sandbox_resume(paths, request)?;
                let response = sinomo_network::ObjectRpcResponse::encode_body(
                    sinomo_network::SANDBOX_RESUME_ROUTE,
                    &response,
                )
                .map_err(DaemonError::from)?;
                write_daemon_rpc_response_frame(stream, response)?;
                Ok(false)
            }
            command if command == daemon_rpc_command(sinomo_network::SANDBOX_REBOOT_ROUTE) => {
                let request = require_daemon_rpc_request(
                    rpc_request,
                    sinomo_network::SANDBOX_REBOOT_ROUTE,
                    sinomo_network::SANDBOX_RPC_CATEGORY,
                )?;
                let request = request.decode_body().map_err(DaemonError::from)?;
                let response = handle_daemon_sandbox_reboot(paths, request)?;
                let response = sinomo_network::ObjectRpcResponse::encode_body(
                    sinomo_network::SANDBOX_REBOOT_ROUTE,
                    &response,
                )
                .map_err(DaemonError::from)?;
                write_daemon_rpc_response_frame(stream, response)?;
                Ok(false)
            }
            command if command == daemon_rpc_command(sinomo_network::SANDBOX_DESTROY_ROUTE) => {
                let request = require_daemon_rpc_request(
                    rpc_request,
                    sinomo_network::SANDBOX_DESTROY_ROUTE,
                    sinomo_network::SANDBOX_RPC_CATEGORY,
                )?;
                let request = request.decode_body().map_err(DaemonError::from)?;
                let response = handle_daemon_sandbox_destroy(paths, request)?;
                let response = sinomo_network::ObjectRpcResponse::encode_body(
                    sinomo_network::SANDBOX_DESTROY_ROUTE,
                    &response,
                )
                .map_err(DaemonError::from)?;
                write_daemon_rpc_response_frame(stream, response)?;
                Ok(false)
            }
            command if command == daemon_rpc_command(sinomo_network::SANDBOX_LIST_ROUTE) => {
                let request = require_daemon_rpc_request(
                    rpc_request,
                    sinomo_network::SANDBOX_LIST_ROUTE,
                    sinomo_network::SANDBOX_RPC_CATEGORY,
                )?;
                let request = request.decode_body().map_err(DaemonError::from)?;
                let response = handle_daemon_sandbox_list(paths, request)?;
                let response = sinomo_network::ObjectRpcResponse::encode_body(
                    sinomo_network::SANDBOX_LIST_ROUTE,
                    &response,
                )
                .map_err(DaemonError::from)?;
                write_daemon_rpc_response_frame(stream, response)?;
                Ok(false)
            }
            command if command == daemon_rpc_command(sinomo_network::SANDBOX_READ_ROUTE) => {
                let request = require_daemon_rpc_request(
                    rpc_request,
                    sinomo_network::SANDBOX_READ_ROUTE,
                    sinomo_network::SANDBOX_RPC_CATEGORY,
                )?;
                let request = request.decode_body().map_err(DaemonError::from)?;
                let response = handle_daemon_sandbox_read(paths, request)?;
                let response = sinomo_network::ObjectRpcResponse::encode_body(
                    sinomo_network::SANDBOX_READ_ROUTE,
                    &response,
                )
                .map_err(DaemonError::from)?;
                write_daemon_rpc_response_frame(stream, response)?;
                Ok(false)
            }
            command if command == daemon_rpc_command(sinomo_network::SANDBOX_WRITE_ROUTE) => {
                let request = require_daemon_rpc_request(
                    rpc_request,
                    sinomo_network::SANDBOX_WRITE_ROUTE,
                    sinomo_network::SANDBOX_RPC_CATEGORY,
                )?;
                let request = request.decode_body().map_err(DaemonError::from)?;
                let response = handle_daemon_sandbox_write(paths, request)?;
                let response = sinomo_network::ObjectRpcResponse::encode_body(
                    sinomo_network::SANDBOX_WRITE_ROUTE,
                    &response,
                )
                .map_err(DaemonError::from)?;
                write_daemon_rpc_response_frame(stream, response)?;
                Ok(false)
            }
            command
                if command == daemon_rpc_command(sinomo_network::SANDBOX_IMPORT_BUSYBOX_ROUTE) =>
            {
                let request = require_daemon_rpc_request(
                    rpc_request,
                    sinomo_network::SANDBOX_IMPORT_BUSYBOX_ROUTE,
                    sinomo_network::SANDBOX_RPC_CATEGORY,
                )?;
                let request = request.decode_body().map_err(DaemonError::from)?;
                let response = handle_daemon_sandbox_import_busybox(paths, request)?;
                let response = sinomo_network::ObjectRpcResponse::encode_body(
                    sinomo_network::SANDBOX_IMPORT_BUSYBOX_ROUTE,
                    &response,
                )
                .map_err(DaemonError::from)?;
                write_daemon_rpc_response_frame(stream, response)?;
                Ok(false)
            }
            command
                if command
                    == daemon_rpc_command(
                        sinomo_network::SANDBOX_TOOLCHAIN_IMPORT_PACKAGE_ROUTE,
                    ) =>
            {
                let request = require_daemon_rpc_request(
                    rpc_request,
                    sinomo_network::SANDBOX_TOOLCHAIN_IMPORT_PACKAGE_ROUTE,
                    sinomo_network::SANDBOX_RPC_CATEGORY,
                )?;
                let request = request.decode_body().map_err(DaemonError::from)?;
                let response = handle_daemon_sandbox_toolchain_import_package(paths, request)?;
                let response = sinomo_network::ObjectRpcResponse::encode_body(
                    sinomo_network::SANDBOX_TOOLCHAIN_IMPORT_PACKAGE_ROUTE,
                    &response,
                )
                .map_err(DaemonError::from)?;
                write_daemon_rpc_response_frame(stream, response)?;
                Ok(false)
            }
            command
                if command
                    == daemon_rpc_command(
                        sinomo_network::SANDBOX_TOOLCHAIN_IMPORT_PACKAGE_FROM_PATH_ROUTE,
                    ) =>
            {
                let request = require_daemon_rpc_request(
                    rpc_request,
                    sinomo_network::SANDBOX_TOOLCHAIN_IMPORT_PACKAGE_FROM_PATH_ROUTE,
                    sinomo_network::SANDBOX_RPC_CATEGORY,
                )?;
                let request: sinomo_sandbox::SandboxImportToolchainPackageFromPathRequest =
                    request.decode_body().map_err(DaemonError::from)?;
                let response =
                    handle_daemon_sandbox_toolchain_import_package_from_path(paths, request)?;
                let response = sinomo_network::ObjectRpcResponse::encode_body(
                    sinomo_network::SANDBOX_TOOLCHAIN_IMPORT_PACKAGE_FROM_PATH_ROUTE,
                    &response,
                )
                .map_err(DaemonError::from)?;
                write_daemon_rpc_response_frame(stream, response)?;
                Ok(false)
            }
            command
                if command
                    == daemon_rpc_command(sinomo_network::SANDBOX_TOOLCHAIN_ADD_COMMAND_ROUTE) =>
            {
                let request = require_daemon_rpc_request(
                    rpc_request,
                    sinomo_network::SANDBOX_TOOLCHAIN_ADD_COMMAND_ROUTE,
                    sinomo_network::SANDBOX_RPC_CATEGORY,
                )?;
                let request = request.decode_body().map_err(DaemonError::from)?;
                let response = handle_daemon_sandbox_toolchain_add_command(paths, request)?;
                let response = sinomo_network::ObjectRpcResponse::encode_body(
                    sinomo_network::SANDBOX_TOOLCHAIN_ADD_COMMAND_ROUTE,
                    &response,
                )
                .map_err(DaemonError::from)?;
                write_daemon_rpc_response_frame(stream, response)?;
                Ok(false)
            }
            command
                if command
                    == daemon_rpc_command(sinomo_network::SANDBOX_TOOLCHAIN_ADD_ARTIFACT_ROUTE) =>
            {
                let request = require_daemon_rpc_request(
                    rpc_request,
                    sinomo_network::SANDBOX_TOOLCHAIN_ADD_ARTIFACT_ROUTE,
                    sinomo_network::SANDBOX_RPC_CATEGORY,
                )?;
                let request = request.decode_body().map_err(DaemonError::from)?;
                let response = handle_daemon_sandbox_toolchain_add_artifact(paths, request)?;
                let response = sinomo_network::ObjectRpcResponse::encode_body(
                    sinomo_network::SANDBOX_TOOLCHAIN_ADD_ARTIFACT_ROUTE,
                    &response,
                )
                .map_err(DaemonError::from)?;
                write_daemon_rpc_response_frame(stream, response)?;
                Ok(false)
            }
            command
                if command == daemon_rpc_command(sinomo_network::SANDBOX_RUN_TOOLCHAIN_ROUTE) =>
            {
                let request = require_daemon_rpc_request(
                    rpc_request,
                    sinomo_network::SANDBOX_RUN_TOOLCHAIN_ROUTE,
                    sinomo_network::SANDBOX_RPC_CATEGORY,
                )?;
                let request = request.decode_body().map_err(DaemonError::from)?;
                let response = handle_daemon_sandbox_run_toolchain(paths, request)?;
                let response = sinomo_network::ObjectRpcResponse::encode_body(
                    sinomo_network::SANDBOX_RUN_TOOLCHAIN_ROUTE,
                    &response,
                )
                .map_err(DaemonError::from)?;
                write_daemon_rpc_response_frame(stream, response)?;
                Ok(false)
            }
            command
                if command
                    == daemon_rpc_command(sinomo_network::SANDBOX_RUN_PROCESS_PLAN_ROUTE) =>
            {
                let request = require_daemon_rpc_request(
                    rpc_request,
                    sinomo_network::SANDBOX_RUN_PROCESS_PLAN_ROUTE,
                    sinomo_network::SANDBOX_RPC_CATEGORY,
                )?;
                let request = request.decode_body().map_err(DaemonError::from)?;
                let response = handle_daemon_sandbox_run_process_plan(paths, request)?;
                let response = sinomo_network::ObjectRpcResponse::encode_body(
                    sinomo_network::SANDBOX_RUN_PROCESS_PLAN_ROUTE,
                    &response,
                )
                .map_err(DaemonError::from)?;
                write_daemon_rpc_response_frame(stream, response)?;
                Ok(false)
            }
            command
                if command
                    == daemon_rpc_command(
                        sinomo_network::SANDBOX_RUN_PROCESS_PLAN_STREAM_ROUTE,
                    ) =>
            {
                let request = require_daemon_rpc_request(
                    rpc_request,
                    sinomo_network::SANDBOX_RUN_PROCESS_PLAN_STREAM_ROUTE,
                    sinomo_network::SANDBOX_RPC_CATEGORY,
                )?;
                let request = request.decode_body().map_err(DaemonError::from)?;
                handle_daemon_sandbox_run_process_plan_stream(paths, request, stream)?;
                Ok(false)
            }
            command
                if command
                    == daemon_rpc_command(sinomo_network::SANDBOX_RUN_TOOLCHAIN_STREAM_ROUTE) =>
            {
                let request = require_daemon_rpc_request(
                    rpc_request,
                    sinomo_network::SANDBOX_RUN_TOOLCHAIN_STREAM_ROUTE,
                    sinomo_network::SANDBOX_RPC_CATEGORY,
                )?;
                let request = request.decode_body().map_err(DaemonError::from)?;
                handle_daemon_sandbox_run_toolchain_stream(paths, request, stream)?;
                Ok(false)
            }
            command
                if command == daemon_rpc_command(sinomo_network::CLUSTER_JOIN_REGISTER_ROUTE) =>
            {
                let request = require_daemon_rpc_request(
                    rpc_request,
                    sinomo_network::CLUSTER_JOIN_REGISTER_ROUTE,
                    sinomo_network::CLUSTER_MEMBERSHIP_RPC_CATEGORY,
                )?;
                let request = request.decode_body().map_err(DaemonError::from)?;
                let result = handle_daemon_cluster_join_request(paths, request)?;
                let response = sinomo_network::ObjectRpcResponse::encode_body(
                    sinomo_network::CLUSTER_JOIN_REGISTER_ROUTE,
                    &result,
                )
                .map_err(DaemonError::from)?;
                write_daemon_rpc_response_frame(stream, response)?;
                Ok(false)
            }
            command
                if command == daemon_rpc_command(sinomo_network::CLUSTER_INVITE_PUBLISH_ROUTE) =>
            {
                let request = require_daemon_rpc_request(
                    rpc_request,
                    sinomo_network::CLUSTER_INVITE_PUBLISH_ROUTE,
                    sinomo_network::CLUSTER_MEMBERSHIP_RPC_CATEGORY,
                )?;
                let request = request.decode_body().map_err(DaemonError::from)?;
                let result =
                    handle_daemon_cluster_invite_publish_request(paths, request, &authenticated)?;
                let response = sinomo_network::ObjectRpcResponse::encode_body(
                    sinomo_network::CLUSTER_INVITE_PUBLISH_ROUTE,
                    &result,
                )
                .map_err(DaemonError::from)?;
                write_daemon_rpc_response_frame(stream, response)?;
                Ok(false)
            }
            command
                if command == daemon_rpc_command(sinomo_network::CLUSTER_MEMBER_PUBLISH_ROUTE) =>
            {
                let request = require_daemon_rpc_request(
                    rpc_request,
                    sinomo_network::CLUSTER_MEMBER_PUBLISH_ROUTE,
                    sinomo_network::CLUSTER_MEMBERSHIP_RPC_CATEGORY,
                )?;
                let request = request.decode_body().map_err(DaemonError::from)?;
                let result =
                    handle_daemon_cluster_member_publish_request(paths, request, &authenticated)?;
                let response = sinomo_network::ObjectRpcResponse::encode_body(
                    sinomo_network::CLUSTER_MEMBER_PUBLISH_ROUTE,
                    &result,
                )
                .map_err(DaemonError::from)?;
                write_daemon_rpc_response_frame(stream, response)?;
                Ok(false)
            }
            command if command == daemon_rpc_command(sinomo_network::CLUSTER_RAFT_APPEND_ROUTE) => {
                let request = require_daemon_rpc_request(
                    rpc_request,
                    sinomo_network::CLUSTER_RAFT_APPEND_ROUTE,
                    sinomo_network::CLUSTER_RAFT_RPC_CATEGORY,
                )?;
                let request = request.decode_body().map_err(DaemonError::from)?;
                let result =
                    handle_daemon_cluster_raft_append_request(paths, request, &authenticated)?;
                let response = sinomo_network::ObjectRpcResponse::encode_body(
                    sinomo_network::CLUSTER_RAFT_APPEND_ROUTE,
                    &result,
                )
                .map_err(DaemonError::from)?;
                write_daemon_rpc_response_frame(stream, response)?;
                Ok(false)
            }
            command
                if command == daemon_rpc_command(sinomo_network::CLUSTER_KV_WATCH_EVENTS_ROUTE) =>
            {
                let request = require_daemon_rpc_request(
                    rpc_request,
                    sinomo_network::CLUSTER_KV_WATCH_EVENTS_ROUTE,
                    sinomo_network::CLUSTER_KV_RPC_CATEGORY,
                )?;
                let request = request.decode_body().map_err(DaemonError::from)?;
                let result = handle_daemon_cluster_kv_watch_events_request(paths, request)?;
                let response = sinomo_network::ObjectRpcResponse::encode_body(
                    sinomo_network::CLUSTER_KV_WATCH_EVENTS_ROUTE,
                    &result,
                )
                .map_err(DaemonError::from)?;
                write_daemon_rpc_response_frame(stream, response)?;
                Ok(false)
            }
            command if command == daemon_rpc_command(sinomo_network::CLUSTER_RAFT_VOTE_ROUTE) => {
                let request = require_daemon_rpc_request(
                    rpc_request,
                    sinomo_network::CLUSTER_RAFT_VOTE_ROUTE,
                    sinomo_network::CLUSTER_RAFT_RPC_CATEGORY,
                )?;
                let request = request.decode_body().map_err(DaemonError::from)?;
                let result =
                    handle_daemon_cluster_raft_vote_request(paths, request, &authenticated)?;
                let response = sinomo_network::ObjectRpcResponse::encode_body(
                    sinomo_network::CLUSTER_RAFT_VOTE_ROUTE,
                    &result,
                )
                .map_err(DaemonError::from)?;
                write_daemon_rpc_response_frame(stream, response)?;
                Ok(false)
            }
            command if command == daemon_rpc_command(sinomo_network::ROLLOUT_BINARY_ROUTE) => {
                let request = require_daemon_rpc_request(
                    rpc_request,
                    sinomo_network::ROLLOUT_BINARY_ROUTE,
                    sinomo_network::ROLLOUT_RPC_CATEGORY,
                )?;
                let request = request.decode_body().map_err(DaemonError::from)?;
                let response = match handle_daemon_rollout_binary_request(paths, request) {
                    Ok(response) => response,
                    Err(error) => sinomo_rollout::ClusterRolloutBinaryResponse {
                        status: "failed".to_string(),
                        detail: error.to_string(),
                        main_binary: PathBuf::new(),
                        shadow_binary: PathBuf::new(),
                        source_head: None,
                    },
                };
                let rpc_response = sinomo_network::ObjectRpcResponse::encode_body(
                    sinomo_network::ROLLOUT_BINARY_ROUTE,
                    &response,
                )
                .map_err(DaemonError::from)?;
                write_daemon_rpc_response_frame(stream, rpc_response)?;
                stream.flush().map_err(|source| DaemonError::Network {
                    address: "daemon client".to_string(),
                    detail: source.to_string(),
                })?;
                stream.shutdown_write();
                Ok(accepted_rollout_should_stop_current_daemon(
                    paths, &response,
                ))
            }
            command if command == daemon_rpc_command(sinomo_network::ROLLOUT_BUILD_ROUTE) => {
                let request = require_daemon_rpc_request(
                    rpc_request,
                    sinomo_network::ROLLOUT_BUILD_ROUTE,
                    sinomo_network::ROLLOUT_RPC_CATEGORY,
                )?;
                let request = request.decode_body().map_err(DaemonError::from)?;
                let response = match handle_daemon_rollout_build_request(paths, request) {
                    Ok(response) => response,
                    Err(error) => sinomo_rollout::ClusterRolloutBinaryResponse {
                        status: "failed".to_string(),
                        detail: error.to_string(),
                        main_binary: PathBuf::new(),
                        shadow_binary: PathBuf::new(),
                        source_head: None,
                    },
                };
                let rpc_response = sinomo_network::ObjectRpcResponse::encode_body(
                    sinomo_network::ROLLOUT_BUILD_ROUTE,
                    &response,
                )
                .map_err(DaemonError::from)?;
                write_daemon_rpc_response_frame(stream, rpc_response)?;
                stream.flush().map_err(|source| DaemonError::Network {
                    address: "daemon client".to_string(),
                    detail: source.to_string(),
                })?;
                stream.shutdown_write();
                Ok(accepted_rollout_should_stop_current_daemon(
                    paths, &response,
                ))
            }
            _ => Ok(false),
        }
    })
}

fn accepted_rollout_should_stop_current_daemon(
    paths: &DaemonPaths,
    response: &sinomo_rollout::ClusterRolloutBinaryResponse,
) -> bool {
    response.status == "accepted" && !daemon_paths_are_shadow(paths)
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
    let config = load_daemon_sandbox_policy_config(paths)?;
    let dfs = open_daemon_sandbox_dfs_service(paths)?;
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
    let config = load_daemon_sandbox_policy_config(paths)?;
    let dfs = open_daemon_sandbox_dfs_service(paths)?;
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
    let dfs = open_daemon_sandbox_dfs_service(paths)?;
    let mut store = sinomo_sandbox::SifsSandboxInstanceStore::new(dfs);
    let manifest = store.load(&request.sandbox_id)?;
    Ok(sinomo_sandbox::SandboxStatusResponse { manifest })
}

pub fn handle_daemon_sandbox_update(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxUpdateInstanceRequest,
) -> Result<sinomo_sandbox::SandboxUpdateInstanceResponse, DaemonError> {
    let config = load_daemon_sandbox_policy_config(paths)?;
    let dfs = open_daemon_sandbox_dfs_service(paths)?;
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
    let config = load_daemon_sandbox_policy_config(paths)?;
    let dfs = open_daemon_sandbox_dfs_service(paths)?;
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
    let config = load_daemon_sandbox_policy_config(paths)?;
    let dfs = open_daemon_sandbox_dfs_service(paths)?;
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
    let config = load_daemon_sandbox_policy_config(paths)?;
    let dfs = open_daemon_sandbox_dfs_service(paths)?;
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
    let dfs = open_daemon_sandbox_dfs_service(paths)?;
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
    let dfs = open_daemon_sandbox_dfs_service(paths)?;
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
    let dfs = open_daemon_sandbox_dfs_service(paths)?;
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
    let config = load_daemon_sandbox_policy_config(paths)?;
    let dfs = open_daemon_sandbox_dfs_service(paths)?;
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
    let config = load_daemon_sandbox_policy_config(paths)?;
    let dfs = open_daemon_sandbox_dfs_service(paths)?;
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
    let config = load_daemon_sandbox_policy_config(paths)?;
    let dfs = open_daemon_sandbox_dfs_service(paths)?;
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

pub fn handle_daemon_sandbox_toolchain_import_package_from_path(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxImportToolchainPackageFromPathRequest,
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
    handle_daemon_sandbox_toolchain_import_package(
        paths,
        sinomo_sandbox::SandboxImportToolchainPackageRequest {
            toolchain_id: request.toolchain_id.unwrap_or(spec.toolchain_id),
            version: spec.version,
            target_abi: spec.target_abi,
            provenance: spec.provenance,
            commands,
            artifacts,
        },
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
    let config = load_daemon_sandbox_policy_config(paths)?;
    let dfs = open_daemon_sandbox_dfs_service(paths)?;
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
    let config = load_daemon_sandbox_policy_config(paths)?;
    let dfs = open_daemon_sandbox_dfs_service(paths)?;
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
                sinomo_sandbox::SandboxWasiAbi::Preview1,
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
    handle_daemon_sandbox_run_toolchain_with_output_sink(paths, request, None)
}

pub fn handle_daemon_sandbox_run_process_plan(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxRunProcessPlanRequest,
) -> Result<sinomo_sandbox::SandboxProcessPlanOutcome, DaemonError> {
    handle_daemon_sandbox_run_process_plan_with_output_sink(paths, request, None)
}

fn handle_daemon_sandbox_run_process_plan_with_output_sink(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxRunProcessPlanRequest,
    output_sink: Option<Arc<dyn sinomo_sandbox::SandboxOutputSink>>,
) -> Result<sinomo_sandbox::SandboxProcessPlanOutcome, DaemonError> {
    let config = load_daemon_sandbox_policy_config(paths)?;
    let data_dir = daemon_data_dir_path(paths)?;
    let mut dfs = open_daemon_sandbox_dfs_service(paths)?;
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
    let runner = sinomo_sandbox::SifsStagedSandboxCommandRunner::new()
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

fn append_daemon_sandbox_new_command_record_events<T: sinomo_network::NetworkTransport>(
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

fn handle_daemon_sandbox_run_toolchain_with_output_sink(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxRunToolchainRequest,
    output_sink: Option<Arc<dyn sinomo_sandbox::SandboxOutputSink>>,
) -> Result<sinomo_sandbox::SandboxLocalCommandOutcome, DaemonError> {
    let config = load_daemon_sandbox_policy_config(paths)?;
    let data_dir = daemon_data_dir_path(paths)?;
    let dfs = open_daemon_sandbox_dfs_service(paths)?;
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
    let runner = sinomo_sandbox::SifsStagedSandboxCommandRunner::new()
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

fn append_daemon_sandbox_command_events<T: sinomo_network::NetworkTransport>(
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

pub fn handle_daemon_sandbox_run_toolchain_stream(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxRunToolchainRequest,
    stream: &mut sinomo_network::DaemonControlStream,
) -> Result<(), DaemonError> {
    enum StreamMessage {
        Output(sinomo_sandbox::SandboxOutputChunk),
        Complete {
            sandbox_id: sinomo_sandbox::SandboxId,
            result: Result<sinomo_sandbox::SandboxLocalCommandOutcome, String>,
        },
    }

    let (sender, receiver) = mpsc::channel();
    let output_sender = sender.clone();
    let paths = paths.clone();
    thread::spawn(move || {
        let sandbox_id = request.sandbox_id.clone();
        let output_sink = Arc::new(move |chunk: sinomo_sandbox::SandboxOutputChunk| {
            let _ = output_sender.send(StreamMessage::Output(chunk));
        });
        let result = handle_daemon_sandbox_run_toolchain_with_output_sink(
            &paths,
            request,
            Some(output_sink),
        )
        .map_err(|error| error.to_string());
        let _ = sender.send(StreamMessage::Complete { sandbox_id, result });
    });

    let mut sent_output_chunks = BTreeSet::new();
    for message in receiver {
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
                        write_daemon_sandbox_stream_frame(stream, &frame)?;
                    }
                }
                let frame = sinomo_sandbox::SandboxRunToolchainStreamFrame::Complete { outcome };
                write_daemon_sandbox_stream_frame(stream, &frame)?;
                stream.flush().map_err(|source| DaemonError::Network {
                    address: "daemon client".to_string(),
                    detail: source.to_string(),
                })?;
                stream.shutdown_write();
                return Ok(());
            }
            StreamMessage::Complete {
                result: Err(detail),
                ..
            } => {
                let frame = sinomo_sandbox::SandboxRunToolchainStreamFrame::Error { detail };
                write_daemon_sandbox_stream_frame(stream, &frame)?;
                stream.flush().map_err(|source| DaemonError::Network {
                    address: "daemon client".to_string(),
                    detail: source.to_string(),
                })?;
                stream.shutdown_write();
                return Ok(());
            }
        };
        write_daemon_sandbox_stream_frame(stream, &frame)?;
        stream.flush().map_err(|source| DaemonError::Network {
            address: "daemon client".to_string(),
            detail: source.to_string(),
        })?;
    }
    Ok(())
}

pub fn handle_daemon_sandbox_run_process_plan_stream(
    paths: &DaemonPaths,
    request: sinomo_sandbox::SandboxRunProcessPlanRequest,
    stream: &mut sinomo_network::DaemonControlStream,
) -> Result<(), DaemonError> {
    enum StreamMessage {
        Output(sinomo_sandbox::SandboxOutputChunk),
        Complete(Result<sinomo_sandbox::SandboxProcessPlanOutcome, String>),
    }

    let (sender, receiver) = mpsc::channel();
    let output_sender = sender.clone();
    let paths = paths.clone();
    thread::spawn(move || {
        let output_sink = Arc::new(move |chunk: sinomo_sandbox::SandboxOutputChunk| {
            let _ = output_sender.send(StreamMessage::Output(chunk));
        });
        let result = handle_daemon_sandbox_run_process_plan_with_output_sink(
            &paths,
            request,
            Some(output_sink),
        )
        .map_err(|error| error.to_string());
        let _ = sender.send(StreamMessage::Complete(result));
    });

    let mut sent_output_chunks = BTreeSet::new();
    for message in receiver {
        match message {
            StreamMessage::Output(chunk) => {
                sent_output_chunks.insert(sandbox_output_chunk_stream_key(&chunk));
                let frame = sinomo_sandbox::SandboxRunProcessPlanStreamFrame::Output { chunk };
                write_daemon_sandbox_process_plan_stream_frame(stream, &frame)?;
                stream.flush().map_err(|source| DaemonError::Network {
                    address: "daemon client".to_string(),
                    detail: source.to_string(),
                })?;
            }
            StreamMessage::Complete(Ok(outcome)) => {
                for step in &outcome.step_outcomes {
                    for chunk in &step.output_chunks {
                        if sent_output_chunks.insert(sandbox_output_chunk_stream_key(chunk)) {
                            let frame = sinomo_sandbox::SandboxRunProcessPlanStreamFrame::Output {
                                chunk: chunk.clone(),
                            };
                            write_daemon_sandbox_process_plan_stream_frame(stream, &frame)?;
                        }
                    }
                }
                let frame = sinomo_sandbox::SandboxRunProcessPlanStreamFrame::Complete { outcome };
                write_daemon_sandbox_process_plan_stream_frame(stream, &frame)?;
                stream.flush().map_err(|source| DaemonError::Network {
                    address: "daemon client".to_string(),
                    detail: source.to_string(),
                })?;
                stream.shutdown_write();
                return Ok(());
            }
            StreamMessage::Complete(Err(detail)) => {
                let frame = sinomo_sandbox::SandboxRunProcessPlanStreamFrame::Error { detail };
                write_daemon_sandbox_process_plan_stream_frame(stream, &frame)?;
                stream.flush().map_err(|source| DaemonError::Network {
                    address: "daemon client".to_string(),
                    detail: source.to_string(),
                })?;
                stream.shutdown_write();
                return Ok(());
            }
        }
    }
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

fn write_daemon_sandbox_stream_frame(
    stream: &mut sinomo_network::DaemonControlStream,
    frame: &sinomo_sandbox::SandboxRunToolchainStreamFrame,
) -> Result<(), DaemonError> {
    let response = sinomo_network::ObjectRpcResponse::encode_body(
        sinomo_network::SANDBOX_RUN_TOOLCHAIN_STREAM_ROUTE,
        frame,
    )
    .map_err(DaemonError::from)?;
    write_daemon_rpc_response_frame(stream, response)
}

fn write_daemon_sandbox_process_plan_stream_frame(
    stream: &mut sinomo_network::DaemonControlStream,
    frame: &sinomo_sandbox::SandboxRunProcessPlanStreamFrame,
) -> Result<(), DaemonError> {
    let response = sinomo_network::ObjectRpcResponse::encode_body(
        sinomo_network::SANDBOX_RUN_PROCESS_PLAN_STREAM_ROUTE,
        frame,
    )
    .map_err(DaemonError::from)?;
    write_daemon_rpc_response_frame(stream, response)
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
    let advertisements =
        handle_daemon_cluster_kv_range_prefix(paths, sinomo_dfs::content_services_prefix())
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
) -> Result<sinomo_dfs::NetworkedDfsService<DaemonDfsEndpointTransport>, DaemonError> {
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
    let dfs = sinomo_dfs::NetworkedDfsService::new(
        DaemonDfsEndpointTransport::new(state.local_node_id.clone(), endpoints),
        content,
    );
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

fn daemon_data_dir_path(paths: &DaemonPaths) -> Result<PathBuf, DaemonError> {
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

fn daemon_status_response(paths: &DaemonPaths) -> Result<crate::DaemonStatus, DaemonError> {
    let mut status = read_daemon_status(paths)?;
    ensure_daemon_status_cluster(paths, &mut status)?;
    if let Some(data_dir) = paths.root.parent() {
        status.rollout = read_rollout_status(&SelfRolloutPaths::new(data_dir))?;
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
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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
    fn daemon_opens_configured_sharded_dfs_metadata_backend() {
        use sinomo_dfs::DfsMetadataBackend as _;

        let root = std::env::temp_dir().join(format!(
            "sinomo-daemon-sharded-dfs-metadata-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
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
        sinomo_network::append_cluster_raft_command(
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
            crate::dfs::open_daemon_sharded_dfs_metadata_backend(&data_dir, &cluster_paths)
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

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn daemon_control_loop_answers_status_while_idle_probe_connection_is_open() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-daemon-control-concurrent-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
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
        let _ = send_test_shutdown_request(&data_dir, &address, Duration::from_secs(3));
        daemon
            .join()
            .expect("daemon thread")
            .expect("daemon exits cleanly");
        std::fs::remove_dir_all(root).ok();

        assert!(
            status_result.is_ok(),
            "status should not be blocked by an idle TCP probe: {status_result:?}"
        );
    }

    #[test]
    fn accepted_rollout_keeps_shadow_control_loop_running() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-daemon-control-rollout-shadow-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
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
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn sandbox_active_execution_guard_enforces_policy_limits_and_releases() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-daemon-sandbox-active-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        let paths = DaemonPaths::with_root(root.clone());
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
        std::fs::remove_dir_all(root).ok();
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
        sinomo_network::send_daemon_rpc_raw(
            address,
            &daemon_rpc_command(&request.route),
            Some(&signed),
            request,
            timeout,
        )
        .map_err(DaemonError::from)
    }
}
