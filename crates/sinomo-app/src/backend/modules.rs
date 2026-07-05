//! Module runtime configuration snapshot and mutations for the Settings UI.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;
use sinomo_runtime::config::{
    CONFIG_GET_ROUTE, CONFIG_RPC_CATEGORY, CONFIG_SET_ROUTE, ConfigGetRequest, ConfigGetResponse,
    ConfigSetRequest, ConfigSetResponse, ConfigWriteTarget,
};
use sinomo_runtime::core::ClusterEndpointRecord;
use sinomo_runtime::module::{
    MODULE_CLUSTER_CONFIG_KEY, MODULE_NODE_CONFIG_KEY, ModuleRuntimeConfig,
    ModuleRuntimeConfigEntry, read_native_dylib_module_cache_entries_excluding_cache_keys,
    read_native_dylib_module_loader_states, read_native_rust_module_runtime_states,
};
use sinomo_runtime::network::{ClusterPaths, ObjectRpcRequest};
use sinomo_runtime::node::NodeId;

use crate::backend::Backend;

const MODULE_CONFIG_RPC_TIMEOUT: Duration = Duration::from_secs(8);

#[derive(Clone, Debug, Default)]
pub struct ModuleConfigSnapshot {
    pub cluster_config: ModuleRuntimeConfig,
    pub selected_node: Option<ModuleConfigNode>,
    pub nodes: Vec<ModuleConfigNodeSummary>,
    pub modules: Vec<ModuleConfigModuleSummary>,
    pub warnings: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct ModuleConfigNodeSummary {
    pub node_id: String,
    pub display_name: String,
    pub availability: String,
    pub local: bool,
}

#[derive(Clone, Debug)]
pub struct ModuleConfigNode {
    pub summary: ModuleConfigNodeSummary,
    pub config: ModuleRuntimeConfig,
    pub error: Option<String>,
}

#[derive(Clone, Debug)]
pub struct ModuleConfigModuleSummary {
    pub module_id: String,
    pub label: String,
    pub backend: String,
    pub detail: String,
}

impl Backend {
    pub fn module_config_snapshot(&self, selected_node_id: Option<&str>) -> ModuleConfigSnapshot {
        let mut warnings = Vec::new();
        let cluster_config = match self.load_cluster_module_config() {
            Ok(config) => config,
            Err(error) => {
                warnings.push(format!("cluster module defaults unavailable: {error}"));
                ModuleRuntimeConfig::default()
            }
        };

        let mut nodes = self.module_config_nodes();
        if nodes.is_empty() {
            nodes.push(ModuleConfigNodeSummary {
                node_id: self.node.node_id.to_string(),
                display_name: self.node.display_name.clone(),
                availability: "local".to_string(),
                local: true,
            });
        }
        let selected_id = selected_node_id
            .filter(|node_id| nodes.iter().any(|node| node.node_id == *node_id))
            .map(str::to_string)
            .or_else(|| {
                nodes
                    .iter()
                    .find(|node| node.local)
                    .map(|node| node.node_id.clone())
            })
            .or_else(|| nodes.first().map(|node| node.node_id.clone()));
        let selected_node = selected_id.and_then(|node_id| {
            let summary = nodes.iter().find(|node| node.node_id == node_id)?.clone();
            let (config, error) = match self.load_node_module_config(&node_id) {
                Ok(config) => (config, None),
                Err(error) => (ModuleRuntimeConfig::default(), Some(error)),
            };
            Some(ModuleConfigNode {
                summary,
                config,
                error,
            })
        });

        let modules = self.module_config_catalog(&cluster_config, selected_node.as_ref());
        ModuleConfigSnapshot {
            cluster_config,
            selected_node,
            nodes,
            modules,
            warnings,
        }
    }

    pub fn set_cluster_module_default(&self, module_id: &str, enabled: bool) -> Result<(), String> {
        let mut config = self.load_cluster_module_config()?;
        config.set(module_id, ModuleRuntimeConfigEntry::enabled(enabled))?;
        let value = serde_json::to_value(&config).map_err(|error| error.to_string())?;
        self.config
            .set(MODULE_CLUSTER_CONFIG_KEY, value, ConfigWriteTarget::Cluster)
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    pub fn set_node_module_override(
        &self,
        node_id: &str,
        module_id: &str,
        enabled: Option<bool>,
    ) -> Result<(), String> {
        let mut config = self.load_node_module_config(node_id)?;
        match enabled {
            Some(enabled) => config.set(module_id, ModuleRuntimeConfigEntry::enabled(enabled))?,
            None => config.clear(module_id)?,
        }
        if self.module_config_node_is_local(node_id) {
            let value = serde_json::to_value(&config).map_err(|error| error.to_string())?;
            return self
                .config
                .set(MODULE_NODE_CONFIG_KEY, value, ConfigWriteTarget::Local)
                .map(|_| ())
                .map_err(|error| error.to_string());
        }
        self.set_remote_node_module_config(node_id, &config)
    }

    fn load_cluster_module_config(&self) -> Result<ModuleRuntimeConfig, String> {
        let resolution = self
            .config
            .get(MODULE_CLUSTER_CONFIG_KEY)
            .map_err(|error| error.to_string())?;
        serde_json::from_value(resolution.value).map_err(|error| error.to_string())
    }

    fn load_node_module_config(&self, node_id: &str) -> Result<ModuleRuntimeConfig, String> {
        if self.module_config_node_is_local(node_id) {
            let resolution = self
                .config
                .get(MODULE_NODE_CONFIG_KEY)
                .map_err(|error| error.to_string())?;
            return serde_json::from_value(resolution.value).map_err(|error| error.to_string());
        }
        self.remote_node_module_config(node_id)
    }

    fn module_config_nodes(&self) -> Vec<ModuleConfigNodeSummary> {
        let snapshot = self.cluster_snapshot();
        let local_node_id = snapshot.local_node_id.clone();
        let mut nodes = snapshot
            .nodes
            .into_iter()
            .map(|node| ModuleConfigNodeSummary {
                local: local_node_id
                    .as_deref()
                    .is_some_and(|local| local == node.node_id),
                node_id: node.node_id,
                display_name: if node.display_name.trim().is_empty() {
                    "Unnamed node".to_string()
                } else {
                    node.display_name
                },
                availability: node.availability,
            })
            .collect::<Vec<_>>();
        nodes.sort_by(|left, right| {
            right
                .local
                .cmp(&left.local)
                .then_with(|| left.display_name.cmp(&right.display_name))
                .then_with(|| left.node_id.cmp(&right.node_id))
        });
        nodes
    }

    fn module_config_catalog(
        &self,
        cluster_config: &ModuleRuntimeConfig,
        selected_node: Option<&ModuleConfigNode>,
    ) -> Vec<ModuleConfigModuleSummary> {
        let mut modules = BTreeMap::<String, ModuleConfigModuleSummary>::new();
        for module_id in cluster_config.module_ids() {
            insert_module_summary(
                &mut modules,
                module_id,
                "configured",
                "Configured in cluster defaults",
            );
        }
        if let Some(node) = selected_node {
            for module_id in node.config.module_ids() {
                insert_module_summary(
                    &mut modules,
                    module_id,
                    "configured",
                    "Configured in node overrides",
                );
            }
        }
        if let Ok(states) = read_native_dylib_module_loader_states(&self.data_dir.root()) {
            let mut active_cache_keys = BTreeSet::new();
            for state in states {
                active_cache_keys.insert(state.cache_key.clone());
                insert_module_summary(
                    &mut modules,
                    &state.module_id.to_string(),
                    "native-dylib",
                    format!("Active native dylib · {}", state.interface_version),
                );
            }
            if let Ok(entries) = read_native_dylib_module_cache_entries_excluding_cache_keys(
                &self.data_dir.root(),
                &active_cache_keys,
            ) {
                for entry in entries {
                    insert_module_summary(
                        &mut modules,
                        &entry.module_id.to_string(),
                        "native-dylib",
                        format!("Installed native dylib · {}", entry.interface_version),
                    );
                }
            }
        }
        if let Ok(states) = read_native_rust_module_runtime_states(&self.data_dir.root()) {
            for state in states {
                insert_module_summary(
                    &mut modules,
                    &state.module_id.to_string(),
                    "native-rust",
                    if state.enabled {
                        "Enabled native Rust module"
                    } else {
                        "Disabled native Rust module"
                    },
                );
            }
        }
        insert_module_summary(
            &mut modules,
            sinomo_runtime::observe::SINOMO_OBSERVE_MODULE_LEGACY_ID,
            "native-rust",
            "Operational observation module",
        );
        insert_module_summary(
            &mut modules,
            sinomo_runtime::sandbox_wasix_api::SINOMO_SANDBOX_WASIX_MODULE_ID,
            "wasix",
            "WASIX sandbox runtime module",
        );
        modules.into_values().collect()
    }

    fn remote_node_module_config(&self, node_id: &str) -> Result<ModuleRuntimeConfig, String> {
        let response: ConfigGetResponse = self.relay_config_rpc(
            node_id,
            CONFIG_GET_ROUTE,
            &ConfigGetRequest {
                key: MODULE_NODE_CONFIG_KEY.to_string(),
            },
        )?;
        serde_json::from_value(response.resolution.value).map_err(|error| error.to_string())
    }

    fn set_remote_node_module_config(
        &self,
        node_id: &str,
        config: &ModuleRuntimeConfig,
    ) -> Result<(), String> {
        let value = serde_json::to_value(config).map_err(|error| error.to_string())?;
        let _: ConfigSetResponse = self.relay_config_rpc(
            node_id,
            CONFIG_SET_ROUTE,
            &ConfigSetRequest {
                key: MODULE_NODE_CONFIG_KEY.to_string(),
                value,
                target: ConfigWriteTarget::Local,
            },
        )?;
        Ok(())
    }

    fn relay_config_rpc<Req, Resp>(
        &self,
        node_id: &str,
        route: &str,
        body: &Req,
    ) -> Result<Resp, String>
    where
        Req: Serialize + ?Sized,
        Resp: DeserializeOwned,
    {
        let address = self
            .connection
            .daemon_address()
            .ok_or_else(|| "remote node module config requires a daemon connection".to_string())?;
        let target_node_id = parse_node_id(node_id)?;
        let paths = ClusterPaths::new(self.data_dir.as_ref());
        let state = sinomo_runtime::cluster::read_cluster_state(&paths)
            .map_err(|error| error.to_string())?;
        let inner = ObjectRpcRequest::encode_body(route, CONFIG_RPC_CATEGORY, body)
            .map_err(|error| error.to_string())?;
        let auth = sinomo_runtime::daemon::sign_daemon_rpc_request_from_data_dir(
            self.data_dir.as_ref(),
            &inner,
        )
        .map_err(|error| error.to_string())?;
        let relay = ObjectRpcRequest::encode_body(
            sinomo_runtime::network::CLUSTER_RELAY_ROUTE,
            sinomo_runtime::network::CLUSTER_ROUTING_RPC_CATEGORY,
            &ModuleConfigRelayEnvelope {
                cluster_id: state.cluster_id,
                target_node_id,
                ttl: 8,
                visited: Vec::new(),
                planned_path: Vec::new(),
                target_endpoints: Vec::new(),
                max_hop_timeout_ms: None,
                inner: inner.with_auth(auth),
            },
        )
        .map_err(|error| error.to_string())?;
        let response =
            sinomo_runtime::daemon::send_daemon_rpc(address, &relay, MODULE_CONFIG_RPC_TIMEOUT)
                .map_err(|error| error.to_string())?;
        response.decode_body().map_err(|error| error.to_string())
    }

    fn module_config_node_is_local(&self, node_id: &str) -> bool {
        let paths = ClusterPaths::new(self.data_dir.as_ref());
        sinomo_runtime::cluster::read_cluster_state(&paths)
            .map(|state| state.local_node_id.to_string() == node_id)
            .unwrap_or(true)
    }
}

fn insert_module_summary(
    modules: &mut BTreeMap<String, ModuleConfigModuleSummary>,
    module_id: &str,
    backend: impl Into<String>,
    detail: impl Into<String>,
) {
    let module_id = module_id.trim();
    if module_id.is_empty() {
        return;
    }
    modules
        .entry(module_id.to_string())
        .or_insert_with(|| ModuleConfigModuleSummary {
            module_id: module_id.to_string(),
            label: module_label(module_id),
            backend: backend.into(),
            detail: detail.into(),
        });
}

fn parse_node_id(node_id: &str) -> Result<NodeId, String> {
    let node_id = node_id.trim();
    NodeId::new(node_id.to_string())
        .or_else(|_| NodeId::from_legacy_named(node_id.to_string()))
        .map_err(|error| format!("invalid node id `{node_id}`: {error}"))
}

#[derive(Serialize)]
struct ModuleConfigRelayEnvelope {
    cluster_id: sinomo_runtime::node::ClusterId,
    target_node_id: NodeId,
    ttl: u8,
    visited: Vec<NodeId>,
    planned_path: Vec<NodeId>,
    target_endpoints: Vec<ClusterEndpointRecord>,
    max_hop_timeout_ms: Option<u64>,
    inner: ObjectRpcRequest,
}

fn module_label(module_id: &str) -> String {
    module_id
        .strip_prefix("module.")
        .unwrap_or(module_id)
        .chars()
        .map(|ch| {
            if matches!(ch, '.' | '-' | '_') {
                ' '
            } else {
                ch
            }
        })
        .collect()
}
