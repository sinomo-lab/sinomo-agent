use crate::{
    AuthenticatedDaemonCommand, ClusterInviteChallengeRequest, ClusterNetworkMaintenance,
    DAEMON_CLUSTER_TICK_INTERVAL, DaemonClusterStatus, DaemonError, DaemonLock, DaemonPaths,
    authenticate_daemon_control_request, create_cluster_invite_challenge_proof, daemon_rpc_command,
    ensure_daemon_status_cluster, ensure_default_distributed_dfs, ensure_default_local_dfs_expose,
    handle_daemon_cluster_invite_publish_request, handle_daemon_cluster_join_request,
    handle_daemon_cluster_kv_range_prefix, handle_daemon_cluster_kv_snapshot,
    handle_daemon_cluster_kv_txn_request, handle_daemon_cluster_kv_watch_drain_request,
    handle_daemon_cluster_kv_watch_events_request, handle_daemon_cluster_kv_watch_prefix_request,
    handle_daemon_cluster_member_publish_request, handle_daemon_cluster_raft_append_request,
    handle_daemon_cluster_raft_vote_request, handle_daemon_dfs_expose_config,
    handle_daemon_finalize_shadow_rollout, handle_daemon_rollout_binary_request,
    handle_daemon_rollout_build_request, handle_daemon_rollout_diagnostics, read_daemon_status,
    start_daemon_iroh_proxy_if_clustered,
};
use sinomo_rollout::{SelfRolloutPaths, read_rollout_status};
use std::io::Write;
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

pub const DAEMON_CLIENT_READ_TIMEOUT: Duration = Duration::from_secs(10);
pub const DAEMON_CLIENT_WRITE_TIMEOUT: Duration = Duration::from_secs(10);
pub const DAEMON_COMMAND_MAX_BYTES: usize = 128 * 1024 * 1024;

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
    E: From<DaemonError>,
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
    handler(authenticated, request.rpc, stream)
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
            loop {
                if let Err(error) = cluster_network.tick(&maintenance_paths, &maintenance_options) {
                    eprintln!("sinomo {maintenance_label} cluster network tick failed: {error}");
                }
                thread::sleep(DAEMON_CLUSTER_TICK_INTERVAL);
            }
        })
        .map_err(|source| DaemonError::Io {
            path: "spawn cluster maintenance thread".to_string(),
            detail: source.to_string(),
        })?;

    loop {
        match listener.accept() {
            Ok(Some(mut stream)) => match handle_daemon_connection(&mut stream, &paths) {
                Ok(true) => break,
                Ok(false) => {}
                Err(error) => eprintln!("sinomo {label} client error: {error}"),
            },
            Ok(None) => {}
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
                let result = handle_daemon_cluster_kv_txn_request(paths, txn)?;
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
                Ok(response.status == "accepted")
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
                Ok(response.status == "accepted")
            }
            _ => Ok(false),
        }
    })
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

fn daemon_status_response(paths: &DaemonPaths) -> Result<crate::DaemonStatus, DaemonError> {
    let mut status = read_daemon_status(paths)?;
    ensure_daemon_status_cluster(paths, &mut status)?;
    if let Some(data_dir) = paths.root.parent() {
        status.rollout = read_rollout_status(&SelfRolloutPaths::new(data_dir))?;
    }
    Ok(status)
}
