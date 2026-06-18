use super::*;

pub(crate) struct CliSignedEndpointRpcHost {
    cluster_id: Option<network::ClusterId>,
    sign_operation: &'static str,
}

impl CliSignedEndpointRpcHost {
    pub(crate) fn new(
        cluster_id: Option<network::ClusterId>,
        sign_operation: &'static str,
    ) -> Self {
        Self {
            cluster_id,
            sign_operation,
        }
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

    fn try_gateway_signed_rpc_async<'a>(
        &'a mut self,
        _endpoint: &'a ClusterEndpointRecord,
        _request: &'a network::ObjectRpcRequest,
        _request_auth: &'a [u8],
        _timeouts: network::DaemonRpcTimeouts,
    ) -> network::ClusterSignedEndpointRpcFuture<'a, Option<network::ObjectRpcResponse>> {
        Box::pin(async { Ok(None) })
    }

    fn endpoint_rpc_cluster_id_async<'a>(
        &'a mut self,
        _endpoint: &'a ClusterEndpointRecord,
    ) -> network::ClusterSignedEndpointRpcFuture<'a, Option<network::ClusterId>> {
        Box::pin(async move { Ok(self.cluster_id.clone()) })
    }

    fn send_signed_endpoint_rpc_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        auth: &'a [u8],
        request: &'a network::ObjectRpcRequest,
        timeouts: network::DaemonRpcTimeouts,
    ) -> network::ClusterSignedEndpointRpcFuture<'a, network::ObjectRpcResponse> {
        Box::pin(async move {
            network::send_daemon_rpc_to_cluster_endpoint_async(
                endpoint,
                Some(auth),
                request,
                timeouts,
            )
            .await
        })
    }
}

pub(crate) fn format_rfc3339(timestamp: OffsetDateTime) -> String {
    timestamp
        .format(&Rfc3339)
        .unwrap_or_else(|_| "unknown".to_string())
}
