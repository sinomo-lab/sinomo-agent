use super::*;

pub(crate) struct CliSignedEndpointRpcHost {
    sign_operation: &'static str,
}

impl CliSignedEndpointRpcHost {
    pub(crate) fn new(
        _cluster_id: Option<sinomo_runtime::node::ClusterId>,
        sign_operation: &'static str,
    ) -> Self {
        Self { sign_operation }
    }
}

impl network::ClusterSignedEndpointRpcAsyncHost for CliSignedEndpointRpcHost {
    fn sign_endpoint_rpc_request_async<'a>(
        &'a mut self,
        request: &'a network::ObjectRpcRequest,
    ) -> network::ClusterSignedEndpointRpcFuture<'a, Vec<u8>> {
        let sign_operation = self.sign_operation;
        Box::pin(async move {
            daemon_sign_daemon_rpc_request_for_local_node(request).map_err(|source| {
                network::NetworkError::Io {
                    operation: sign_operation.to_string(),
                    detail: source.to_string(),
                }
            })
        })
    }
}

pub(crate) fn format_rfc3339(timestamp: OffsetDateTime) -> String {
    timestamp
        .format(&Rfc3339)
        .unwrap_or_else(|_| "unknown".to_string())
}
