use super::*;

pub(crate) struct CliEndpointRpcHost;

impl CliEndpointRpcHost {
    pub(crate) fn new(
        _cluster_id: Option<sinomo_runtime::node::ClusterId>,
        _sign_operation: &'static str,
    ) -> Self {
        Self
    }

    pub(crate) fn with_data_dir(self, _data_dir: SinomoDataDir) -> Self {
        self
    }
}

impl network::ClusterObjectRpcAsyncHost for CliEndpointRpcHost {}

pub(crate) fn format_rfc3339(timestamp: OffsetDateTime) -> String {
    timestamp
        .format(&Rfc3339)
        .unwrap_or_else(|_| "unknown".to_string())
}
