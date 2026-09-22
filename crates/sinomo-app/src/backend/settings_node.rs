//! Explicit node targeting for settings, without retargeting the application itself.

use super::Backend;
use serde::{Serialize, de::DeserializeOwned};
use sinomo_client::{
    NodeConfigClient,
    api::{DaemonApiClient, DaemonObjectRpcDestinationProcessAffinity},
};
use sinomo_runtime::{dfs::SifsMountPlatform, network::ObjectRpcRequest, node::NodeId};
use std::sync::Arc;

impl Backend {
    pub fn with_settings_node(&self, node_id: Option<String>) -> Result<Self, String> {
        let mut targeted = self.clone();
        targeted.settings_node_id = node_id;
        if targeted.settings_node_is_remote() {
            let node_id = NodeId::new(targeted.settings_node_id.clone().unwrap())
                .map_err(|error| error.to_string())?;
            let address = self
                .connection
                .daemon_address()
                .ok_or_else(|| "Managing another node requires a daemon connection".to_string())?;
            targeted.config = Arc::new(NodeConfigClient::new(
                (*self.data_dir).clone(),
                address,
                node_id,
                super::DAEMON_RPC_TIMEOUT,
            ));
        }
        Ok(targeted)
    }

    pub fn settings_node_is_remote(&self) -> bool {
        self.settings_node_id.as_deref().is_some_and(|selected| {
            self.app_cluster_status()
                .local_node_id
                .as_deref()
                .unwrap_or(self.node.node_id.as_str())
                != selected
        })
    }

    pub fn settings_mount_platform(&self) -> Result<SifsMountPlatform, String> {
        if !self.settings_node_is_remote() {
            return Ok(SifsMountPlatform::current());
        }
        self.settings_node_rpc::<_, sinomo_runtime::dfs::SifsMountStatusResponse>(
            sinomo_runtime::dfs::SIFS_MOUNT_STATUS_ROUTE,
            "sinomo.dfs.mount.status",
            &sinomo_runtime::dfs::SifsMountStatusRequest,
        )
        .map(|response| response.status.platform)
    }

    pub fn settings_node_rpc<Req: Serialize, Resp: DeserializeOwned>(
        &self,
        route: &str,
        category: &str,
        body: &Req,
    ) -> Result<Resp, String> {
        let address = self
            .connection
            .daemon_address()
            .ok_or_else(|| "Node settings require a daemon connection".to_string())?;
        let request = ObjectRpcRequest::encode_body(route, category, body)
            .map_err(|error| error.to_string())?;
        let client = DaemonApiClient::at_address(address).with_timeout(super::DAEMON_RPC_TIMEOUT);
        let response = if self.settings_node_is_remote() {
            let node = NodeId::new(self.settings_node_id.clone().unwrap())
                .map_err(|error| error.to_string())?;
            client.object_rpc_to_target_from_data_dir_with_affinity(
                self.data_dir.as_ref(),
                &node,
                request,
                super::DAEMON_RPC_TIMEOUT,
                DaemonObjectRpcDestinationProcessAffinity::MainDaemon,
            )
        } else {
            client.call_request(&request)
        }
        .map_err(|error| error.to_string())?;
        response.decode_body().map_err(|error| error.to_string())
    }
}
