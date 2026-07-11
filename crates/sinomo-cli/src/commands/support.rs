use super::*;

pub(crate) struct CliSignedEndpointRpcHost {
    sign_operation: &'static str,
    data_dir: Option<SinomoDataDir>,
}

impl CliSignedEndpointRpcHost {
    pub(crate) fn new(
        _cluster_id: Option<sinomo_runtime::node::ClusterId>,
        sign_operation: &'static str,
    ) -> Self {
        Self {
            sign_operation,
            data_dir: None,
        }
    }

    pub(crate) fn with_data_dir(mut self, data_dir: SinomoDataDir) -> Self {
        self.data_dir = Some(data_dir);
        self
    }
}

impl network::ClusterSignedObjectRpcAsyncHost for CliSignedEndpointRpcHost {
    fn sign_object_rpc_request_async<'a>(
        &'a mut self,
        request: &'a network::ObjectRpcRequest,
    ) -> network::ClusterSignedObjectRpcFuture<'a, Vec<u8>> {
        let sign_operation = self.sign_operation;
        let data_dir = self.data_dir.clone();
        Box::pin(async move {
            let result = match data_dir.as_ref() {
                Some(data_dir) => network::sign_daemon_rpc_request_from_data_dir(data_dir, request)
                    .map_err(|source| source.to_string()),
                None => daemon_sign_daemon_rpc_request_for_local_node(request)
                    .map_err(|source| source.to_string()),
            };
            result.map_err(|detail| network::NetworkError::Io {
                operation: sign_operation.to_string(),
                detail,
            })
        })
    }
}

pub(crate) fn format_rfc3339(timestamp: OffsetDateTime) -> String {
    timestamp
        .format(&Rfc3339)
        .unwrap_or_else(|_| "unknown".to_string())
}
