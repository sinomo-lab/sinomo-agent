//! Configuration for an explicitly selected node, routed through the local daemon.

use std::time::Duration;

use sinomo_runtime::api::{DaemonApiClient, DaemonObjectRpcDestinationProcessAffinity};
use sinomo_runtime::config::{
    CONFIG_CATALOG_ROUTE, CONFIG_GET_ROUTE, CONFIG_RPC_CATEGORY, CONFIG_SET_ROUTE,
    ConfigCatalogRequest, ConfigCatalogResponse, ConfigClient, ConfigDefinition, ConfigError,
    ConfigGetRequest, ConfigGetResponse, ConfigResolution, ConfigSetRequest, ConfigSetResponse,
    ConfigWriteResult, ConfigWriteTarget,
};
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::network::ObjectRpcRequest;
use sinomo_runtime::node::NodeId;

#[derive(Clone)]
pub struct NodeConfigClient {
    daemon: DaemonApiClient,
    data_dir: SinomoDataDir,
    node_id: NodeId,
    timeout: Duration,
}

impl NodeConfigClient {
    pub fn new(data_dir: SinomoDataDir, address: &str, node_id: NodeId, timeout: Duration) -> Self {
        Self {
            daemon: DaemonApiClient::at_address(address),
            data_dir,
            node_id,
            timeout,
        }
    }

    fn call<Req: serde::Serialize, Resp: serde::de::DeserializeOwned>(
        &self,
        route: &str,
        body: &Req,
    ) -> Result<Resp, ConfigError> {
        let call = || -> Result<Resp, String> {
            let request = ObjectRpcRequest::encode_body(route, CONFIG_RPC_CATEGORY, body)
                .map_err(|error| error.to_string())?;
            let response = self
                .daemon
                .object_rpc_to_target_from_data_dir_with_affinity(
                    &self.data_dir,
                    &self.node_id,
                    request,
                    self.timeout,
                    DaemonObjectRpcDestinationProcessAffinity::MainDaemon,
                )
                .map_err(|error| error.to_string())?;
            response.decode_body().map_err(|error| error.to_string())
        };
        call().map_err(|detail| ConfigError::Client { detail })
    }
}

impl ConfigClient for NodeConfigClient {
    fn catalog(&self, owner: Option<&str>) -> Result<Vec<ConfigDefinition>, ConfigError> {
        self.call::<_, ConfigCatalogResponse>(
            CONFIG_CATALOG_ROUTE,
            &ConfigCatalogRequest {
                owner: owner.map(str::to_string),
            },
        )
        .map(|response| response.definitions)
    }

    fn get(&self, key: &str) -> Result<ConfigResolution, ConfigError> {
        self.call::<_, ConfigGetResponse>(CONFIG_GET_ROUTE, &ConfigGetRequest { key: key.into() })
            .map(|response| response.resolution)
    }

    fn set(
        &self,
        key: &str,
        value: serde_json::Value,
        target: ConfigWriteTarget,
    ) -> Result<ConfigWriteResult, ConfigError> {
        self.call::<_, ConfigSetResponse>(
            CONFIG_SET_ROUTE,
            &ConfigSetRequest {
                request_id: Some(sinomo_runtime::kv::KvRequestId::generate()),
                key: key.into(),
                value,
                target,
            },
        )
        .map(|response| response.result)
    }
}
