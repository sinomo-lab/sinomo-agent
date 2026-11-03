//! Typed client for daemon-owned usage records.
use sinomo_runtime::{
    api::{DaemonApiClient, DaemonApiError, app::usage::*},
    network::ObjectRpcRequest,
};
#[derive(Clone, Debug)]
pub struct UsageClient {
    daemon: DaemonApiClient,
}
impl UsageClient {
    pub fn at_address(address: impl Into<String>) -> Self {
        Self {
            daemon: DaemonApiClient::at_address(address),
        }
    }
    pub fn query(&self, query: &UsageQuery) -> Result<UsageQueryResponse, DaemonApiError> {
        self.daemon
            .call_request(&ObjectRpcRequest::encode_body(
                USAGE_QUERY_ROUTE,
                USAGE_RPC_CATEGORY,
                query,
            )?)?
            .decode_body()
            .map_err(Into::into)
    }
}
