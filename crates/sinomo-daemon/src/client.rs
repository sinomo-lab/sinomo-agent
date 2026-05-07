use crate::{DaemonError, daemon_rpc_command, sign_daemon_rpc_request_for_local_node};
use std::time::Duration;

pub fn send_daemon_rpc(
    address: &str,
    request: &sinomo_network::ObjectRpcRequest,
    timeout: Duration,
) -> Result<sinomo_network::ObjectRpcResponse, DaemonError> {
    let signed = sign_daemon_rpc_request_for_local_node(request)?;
    sinomo_network::send_daemon_rpc_raw(
        address,
        &daemon_rpc_command(&request.route),
        Some(&signed),
        request,
        timeout,
    )
    .map_err(DaemonError::from)
}
