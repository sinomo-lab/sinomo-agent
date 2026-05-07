use crate::{
    AuthenticatedDaemonCommand, ClusterInviteChallengeRequest, DaemonError, DaemonPaths,
    authenticate_daemon_control_request, create_cluster_invite_challenge_proof, daemon_rpc_command,
};
use std::io::Write;
use std::time::Duration;

pub const DAEMON_CLIENT_READ_TIMEOUT: Duration = Duration::from_secs(10);
pub const DAEMON_CLIENT_WRITE_TIMEOUT: Duration = Duration::from_secs(60);
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
    let mut request = read_daemon_control_frame(stream)?;
    let command = request.command.trim().to_string();
    if command == daemon_rpc_command(sinomo_network::CLUSTER_JOIN_CHALLENGE_ROUTE) {
        let rpc_request = request.rpc.as_ref().ok_or(DaemonError::AuthDenied)?;
        if rpc_request.route != sinomo_network::CLUSTER_JOIN_CHALLENGE_ROUTE
            || rpc_request.category != sinomo_network::CLUSTER_MEMBERSHIP_RPC_CATEGORY
        {
            return Err(DaemonError::AuthDenied.into());
        }
        let challenge: ClusterInviteChallengeRequest =
            rpc_request.decode_body().map_err(DaemonError::from)?;
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
        Err(_error) => {
            stream.shutdown_both();
            return Ok(false);
        }
    };
    handler(authenticated, request.rpc, stream)
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
