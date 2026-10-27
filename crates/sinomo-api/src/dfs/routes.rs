//! Shared DFS wire values and validation; no storage or service runtime.

use crate::dfs::*;
use sinomo_core::network::NetworkError;
use sinomo_core::network::ObjectRpcRequest;
use sinomo_core::network::ObjectRpcResponse;

pub const DFS_RPC_CATEGORY: &str = "dfs";

/// Read the active content owner's already accepted in-memory advertisement.
pub const DFS_CONTENT_ADVERTISEMENT_LIVE_ROUTE: &str = "/dfs/content/advertisement-live";

pub const DFS_INSPECTION_ROUTE: &str = "/dfs/inspection";
pub const DFS_REPAIR_RETRY_ROUTE: &str = "/dfs/repair/retry";

pub fn dfs_inspection_rpc_request(
    request: &DaemonDfsInspectionRequest,
) -> Result<ObjectRpcRequest, NetworkError> {
    ObjectRpcRequest::encode_body(DFS_INSPECTION_ROUTE, DFS_RPC_CATEGORY, request)
}

pub fn decode_dfs_inspection_rpc_response(
    response: &ObjectRpcResponse,
) -> Result<DaemonDfsInspectionResponse, NetworkError> {
    response.decode_body()
}
