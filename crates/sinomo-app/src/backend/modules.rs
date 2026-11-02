//! Module runtime configuration snapshot and mutations for the Settings UI.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

use serde::de::DeserializeOwned;
use sinomo_client::api::DaemonApiClient;
use sinomo_runtime::config::{
    CONFIG_GET_ROUTE, CONFIG_RPC_CATEGORY, CONFIG_SET_ROUTE, ConfigGetRequest, ConfigGetResponse,
    ConfigSetRequest, ConfigSetResponse, ConfigWriteTarget,
};
use sinomo_runtime::module::{
    MODULE_CLUSTER_CONFIG_KEY, MODULE_NATIVE_RUST_LIST_ROUTE, MODULE_NATIVE_RUST_SET_ENABLED_ROUTE,
    MODULE_NODE_CONFIG_KEY, ModuleObservationsResponse, ModuleRuntimeConfig,
    ModuleRuntimeConfigEntry, ModuleRuntimeConfigSource, NativeRustModuleListRequest,
    NativeRustModuleListResponse, NativeRustModuleSetEnabledRequest,
    NativeRustModuleSetEnabledResponse,
};
use sinomo_runtime::network::{ClusterPaths, ObjectRpcRequest};
use sinomo_runtime::node::NodeId;

use crate::backend::Backend;
use crate::backend::remote_management::REMOTE_MANAGEMENT_MODULE_ID;

const MODULE_CONFIG_RPC_TIMEOUT: Duration = Duration::from_secs(8);
static LOCAL_MODULE_OVERRIDE_WRITE_LOCK: Mutex<()> = Mutex::new(());

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
    pub desired_enabled: Option<bool>,
    pub effective_enabled: bool,
    pub effective_source: ModuleRuntimeConfigSource,
    pub observed: ModuleObservedState,
    pub observed_detail: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ModuleObservedState {
    Running,
    Stopped,
    Installed,
    NotReported,
    Unavailable,
}

impl Backend {
    fn local_module_observations(&self) -> ModuleObservationsResponse {
        if self.settings_node_is_remote() {
            return self
                .settings_node_rpc(
                    sinomo_runtime::module::MODULE_OBSERVATIONS_ROUTE,
                    sinomo_runtime::module::MODULE_UI_RPC_CATEGORY,
                    &sinomo_runtime::module::ModuleObservationsRequest::default(),
                )
                .unwrap_or_else(ModuleObservationsResponse::unavailable);
        }
        // Unit fixtures may inspect their isolated host data without a live daemon.
        #[cfg(test)]
        if self.connection.daemon_address().is_none() {
            return sinomo_runtime::module::module_observations(&self.data_dir.root());
        }
        let result = (|| {
            let address = self
                .connection
                .daemon_address()
                .ok_or_else(|| "module observations require a daemon connection".to_string())?;
            sinomo_client::read_module_observations(address, MODULE_CONFIG_RPC_TIMEOUT)
        })();
        result.unwrap_or_else(ModuleObservationsResponse::unavailable)
    }

    pub fn module_config_snapshot(&self, selected_node_id: Option<&str>) -> ModuleConfigSnapshot {
        let selected_node_id = selected_node_id.or(self.settings_node_id.as_deref());
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
            .map(str::to_string)
            .or_else(|| {
                nodes
                    .iter()
                    .find(|node| node.local)
                    .map(|node| node.node_id.clone())
            })
            .or_else(|| nodes.first().map(|node| node.node_id.clone()));
        let selected_node = selected_id.and_then(|node_id| {
            let mut summary = nodes.iter().find(|node| node.node_id == node_id)?.clone();
            let (config, error) = match self.load_node_module_config(&node_id) {
                Ok(config) => {
                    if !summary.local {
                        // The selected node just answered a settings RPC. Local-only
                        // topology observations can still carry stale availability.
                        summary.availability = "reachable".to_string();
                    }
                    (config, None)
                }
                Err(error) => (ModuleRuntimeConfig::default(), Some(error)),
            };
            Some(ModuleConfigNode {
                summary,
                config,
                error,
            })
        });

        let modules =
            self.module_config_catalog(&cluster_config, selected_node.as_ref(), &mut warnings);
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
        native_rust: bool,
    ) -> Result<(), String> {
        if self
            .settings_node_id
            .as_deref()
            .is_some_and(|selected| selected != node_id)
        {
            return Err("The selected node changed before this module edit was submitted".into());
        }
        if self.module_config_node_is_local(node_id) {
            self.set_local_module_override(module_id, enabled)?;
        } else {
            let mut config = self.load_node_module_config(node_id)?;
            apply_module_override(&mut config, module_id, enabled)?;
            self.set_remote_node_module_config(node_id, &config)?;
        }
        if let Some(enabled) = enabled
            && native_rust
        {
            self.set_node_native_rust_module_enabled(node_id, module_id, enabled)?;
        }
        Ok(())
    }

    /// Writes a node override to the attached host's local configuration
    /// target. This path deliberately takes no node ID and can never fall back
    /// to the signed remote-config relay for another node.
    pub fn set_local_module_override(
        &self,
        module_id: &str,
        enabled: Option<bool>,
    ) -> Result<(), String> {
        // Settings can have more than one retained surface. Serialize their
        // whole-document edits so the shortcut and Modules page cannot lose
        // one another's unrelated overrides inside this process.
        let _write_guard = LOCAL_MODULE_OVERRIDE_WRITE_LOCK
            .lock()
            .map_err(|_| "local module override write lock is poisoned".to_string())?;
        let resolution = self
            .config
            .get(MODULE_NODE_CONFIG_KEY)
            .map_err(|error| error.to_string())?;
        let mut config: ModuleRuntimeConfig =
            serde_json::from_value(resolution.value).map_err(|error| error.to_string())?;
        apply_module_override(&mut config, module_id, enabled)?;
        let value = serde_json::to_value(&config).map_err(|error| error.to_string())?;
        self.config
            .set(MODULE_NODE_CONFIG_KEY, value, ConfigWriteTarget::Local)
            .map(|_| ())
            .map_err(|error| error.to_string())
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
        warnings: &mut Vec<String>,
    ) -> Vec<ModuleConfigModuleSummary> {
        let mut modules = BTreeMap::<String, ModuleConfigModuleSummary>::new();
        let mut observations = BTreeMap::<String, (ModuleObservedState, String)>::new();
        let mut selected_native_rust_observations =
            BTreeMap::<String, (ModuleObservedState, String)>::new();
        let mut native_dylib_observations =
            BTreeMap::<String, (ModuleObservedState, String)>::new();
        let mut native_dylib_observation_error = None;
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
        let loader_observations = self.local_module_observations();
        let display_names = loader_observations.display_names;
        match loader_observations.native_dylib {
            Ok(states) => {
                for state in states {
                    native_dylib_observations.insert(
                        state.module_id.to_string(),
                        (
                            ModuleObservedState::NotReported,
                            format!(
                                "Native loader activation record present · {} · daemon registration is not independently reported",
                                state.interface_version
                            ),
                        ),
                    );
                    insert_native_dylib_module_summary(
                        &mut modules,
                        &state.module_id.to_string(),
                        format!(
                            "Native dylib activation record · {}",
                            state.interface_version
                        ),
                    );
                }
                match loader_observations.inactive_cache {
                    Ok(entries) => {
                        for entry in entries {
                            native_dylib_observations
                                .entry(entry.module_id.to_string())
                                .or_insert_with(|| {
                                    (
                                        ModuleObservedState::Installed,
                                        format!(
                                            "Installed but not active · {}",
                                            entry.interface_version
                                        ),
                                    )
                                });
                            insert_native_dylib_module_summary(
                                &mut modules,
                                &entry.module_id.to_string(),
                                format!("Installed native dylib · {}", entry.interface_version),
                            );
                        }
                    }
                    Err(error) => {
                        let detail =
                            format!("native module cache observations unavailable: {error}");
                        warnings.push(detail.clone());
                        native_dylib_observation_error = Some(detail);
                    }
                }
            }
            Err(error) => {
                let detail = format!("native module loader observations unavailable: {error}");
                warnings.push(detail.clone());
                native_dylib_observation_error = Some(detail);
            }
        }
        if let Ok(states) = loader_observations.native_rust {
            for state in states {
                observations.insert(
                    state.module_id.to_string(),
                    (
                        if state.enabled {
                            ModuleObservedState::Running
                        } else {
                            ModuleObservedState::Stopped
                        },
                        format!("{} · {}", state.reason, state.updated_at),
                    ),
                );
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
        if let Some(node) = selected_node.filter(|_| self.connection.daemon_address().is_some()) {
            match self.remote_native_rust_module_statuses(&node.summary.node_id) {
                Ok(response) => {
                    for module in response.modules {
                        selected_native_rust_observations.insert(
                            module.module_id,
                            (
                                if module.active {
                                    ModuleObservedState::Running
                                } else {
                                    ModuleObservedState::Stopped
                                },
                                module.detail,
                            ),
                        );
                    }
                }
                Err(error) => warnings.push(format!(
                    "native Rust module observations unavailable on {}: {error}",
                    node.summary.display_name
                )),
            }
        }
        if let Ok(states) = loader_observations.declarative {
            for state in states {
                observations.insert(
                    state.module_id.to_string(),
                    (
                        if state.active {
                            ModuleObservedState::Running
                        } else {
                            ModuleObservedState::Stopped
                        },
                        format!(
                            "Declarative package {} · {}",
                            state.version, state.installed_at
                        ),
                    ),
                );
                insert_module_summary(
                    &mut modules,
                    &state.module_id.to_string(),
                    "declarative",
                    format!("Declarative package · {}", state.version),
                );
            }
        }
        insert_module_summary(
            &mut modules,
            sinomo_runtime::dfs::SINOMO_DFS_MODULE_LEGACY_ID,
            "native-rust",
            "Distributed filesystem provider module",
        );
        insert_module_summary(
            &mut modules,
            sinomo_runtime::observe::SINOMO_OBSERVE_MODULE_LEGACY_ID,
            "native-rust",
            "Operational observation module",
        );
        insert_module_summary(
            &mut modules,
            REMOTE_MANAGEMENT_MODULE_ID,
            "native-dylib",
            "Privileged node administration module",
        );
        let remote_management_module = modules
            .keys()
            .find(|module_id| module_ids_match(module_id, REMOTE_MANAGEMENT_MODULE_ID))
            .cloned();
        if let Some(module) = remote_management_module
            .as_ref()
            .and_then(|module_id| modules.get_mut(module_id))
        {
            module.backend = "native-dylib".to_string();
            module.detail = "Privileged node administration module".to_string();
        }

        match self.config.catalog(None) {
            Ok(definitions) => {
                for definition in definitions {
                    let Some(module_id) = definition.module_id.as_deref() else {
                        continue;
                    };
                    insert_module_summary(
                        &mut modules,
                        module_id,
                        "settings",
                        "Module-owned settings surface",
                    );
                    let matching = modules
                        .keys()
                        .find(|existing| module_ids_match(existing, module_id))
                        .cloned();
                    if let Some(module) = matching.and_then(|key| modules.get_mut(&key)) {
                        if let Some(label) = definition.module_label {
                            module.label = label;
                        }
                        if let Some(description) = definition.module_description {
                            module.detail = description;
                        }
                    }
                }
            }
            Err(error) => warnings.push(format!(
                "module settings presentation metadata unavailable: {error}"
            )),
        }

        let observations_match_selected = selected_node.is_some_and(|node| {
            node.summary.local && !self.settings_node_is_remote()
                || self.settings_node_id.as_deref() == Some(node.summary.node_id.as_str())
        });
        let empty_node_config = ModuleRuntimeConfig::default();
        let node_config = selected_node
            .map(|node| &node.config)
            .unwrap_or(&empty_node_config);
        for module in modules.values_mut() {
            if let Some(name) = module_display_name(&module.module_id, &display_names) {
                module.label = name;
            }
            module.desired_enabled = module_config_setting(cluster_config, &module.module_id)
                .map(|setting| setting.enabled);
            let (effective_enabled, effective_source) = if let Some(setting) =
                module_config_setting(node_config, &module.module_id)
            {
                (setting.enabled, ModuleRuntimeConfigSource::Node)
            } else if let Some(setting) = module_config_setting(cluster_config, &module.module_id) {
                (setting.enabled, ModuleRuntimeConfigSource::Cluster)
            } else {
                (false, ModuleRuntimeConfigSource::DisabledDefault)
            };
            module.effective_enabled = effective_enabled;
            module.effective_source = effective_source;
            if observations_match_selected {
                let observation = if module.backend == "native-dylib" {
                    remove_matching_observation(&mut native_dylib_observations, &module.module_id)
                } else if module.backend == "native-rust" {
                    remove_matching_observation(
                        &mut selected_native_rust_observations,
                        &module.module_id,
                    )
                    .or_else(|| remove_matching_observation(&mut observations, &module.module_id))
                } else {
                    remove_matching_observation(&mut observations, &module.module_id)
                };
                let (state, detail) = observation.unwrap_or_else(|| {
                    if module.backend == "native-dylib"
                        && let Some(error) = native_dylib_observation_error.as_ref()
                    {
                        return (ModuleObservedState::Unavailable, error.clone());
                    }
                    (
                        ModuleObservedState::NotReported,
                        "The local loader has not published an observation".to_string(),
                    )
                });
                module.observed = state;
                module.observed_detail = detail;
            } else if module.backend == "native-rust" {
                let (state, detail) = remove_matching_observation(
                    &mut selected_native_rust_observations,
                    &module.module_id,
                )
                .unwrap_or_else(|| {
                    (
                        ModuleObservedState::Unavailable,
                        if selected_node.is_some() {
                            "This node did not report native Rust loader state".to_string()
                        } else {
                            "Select a node to inspect loader observations".to_string()
                        },
                    )
                });
                module.observed = state;
                module.observed_detail = detail;
            } else {
                module.observed = ModuleObservedState::Unavailable;
                module.observed_detail = if selected_node.is_some() {
                    "This node does not publish loader observations yet".to_string()
                } else {
                    "Select a node to inspect loader observations".to_string()
                };
            }
        }
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
                request_id: Some(sinomo_runtime::kv::KvRequestId::generate()),
                key: MODULE_NODE_CONFIG_KEY.to_string(),
                value,
                target: ConfigWriteTarget::Local,
            },
        )?;
        Ok(())
    }

    fn set_node_native_rust_module_enabled(
        &self,
        node_id: &str,
        module_id: &str,
        enabled: bool,
    ) -> Result<(), String> {
        if self.module_config_node_is_local(node_id) {
            // Isolated unit fixtures provide the host store without a resident daemon.
            #[cfg(test)]
            if self.connection.daemon_address().is_none() {
                sinomo_runtime::module::set_native_rust_module_enabled(
                    &self.data_dir.root(),
                    &parse_module_id(module_id)?,
                    enabled,
                    "app-settings",
                    "operator changed node override from the Modules settings page",
                )
                .map_err(|error| error.to_string())?;
                return Ok(());
            }
            let address = self
                .connection
                .daemon_address()
                .ok_or_else(|| "local module changes require a daemon connection".to_string())?;
            sinomo_client::modules::set_native_rust_module_enabled(
                address,
                module_id,
                enabled,
                "operator changed node override from the Modules settings page",
                MODULE_CONFIG_RPC_TIMEOUT,
            )?;
            return Ok(());
        }

        let _: NativeRustModuleSetEnabledResponse = self.relay_rpc(
            node_id,
            MODULE_NATIVE_RUST_SET_ENABLED_ROUTE,
            sinomo_runtime::module::MODULE_NATIVE_RUST_RPC_CATEGORY,
            &NativeRustModuleSetEnabledRequest {
                module_id: module_id.to_string(),
                enabled,
                reason: "operator changed node override from the Modules settings page".to_string(),
            },
        )?;
        Ok(())
    }

    fn remote_native_rust_module_statuses(
        &self,
        node_id: &str,
    ) -> Result<NativeRustModuleListResponse, String> {
        self.relay_rpc(
            node_id,
            MODULE_NATIVE_RUST_LIST_ROUTE,
            sinomo_runtime::module::MODULE_NATIVE_RUST_RPC_CATEGORY,
            &NativeRustModuleListRequest::default(),
        )
    }

    fn relay_config_rpc<Req, Resp>(
        &self,
        node_id: &str,
        route: &str,
        body: &Req,
    ) -> Result<Resp, String>
    where
        Req: serde::Serialize + ?Sized,
        Resp: DeserializeOwned,
    {
        self.relay_rpc(node_id, route, CONFIG_RPC_CATEGORY, body)
    }

    fn relay_rpc<Req, Resp>(
        &self,
        node_id: &str,
        route: &str,
        category: &str,
        body: &Req,
    ) -> Result<Resp, String>
    where
        Req: serde::Serialize + ?Sized,
        Resp: DeserializeOwned,
    {
        let address = self
            .connection
            .daemon_address()
            .ok_or_else(|| "remote node module update requires a daemon connection".to_string())?;
        let target_node_id = parse_node_id(node_id)?;
        let inner = ObjectRpcRequest::encode_body(route, category, body)
            .map_err(|error| error.to_string())?;
        let response = DaemonApiClient::at_address(address)
            .object_rpc_to_target_from_data_dir_with_affinity(
                self.data_dir.as_ref(),
                &target_node_id,
                inner,
                MODULE_CONFIG_RPC_TIMEOUT,
                sinomo_client::api::DaemonObjectRpcDestinationProcessAffinity::MainDaemon,
            )
            .map_err(|error| error.to_string())?;
        response.decode_body().map_err(|error| error.to_string())
    }

    fn module_config_node_is_local(&self, node_id: &str) -> bool {
        let paths = ClusterPaths::new(self.data_dir.as_ref());
        sinomo_runtime::cluster::read_cluster_state(&paths)
            .map(|state| state.local_node_id.to_string() == node_id)
            .unwrap_or_else(|_| {
                matches!(
                    &self.connection,
                    crate::backend::Connection::LocalFallback { .. }
                ) && self.node.node_id.to_string() == node_id
            })
    }
}

fn apply_module_override(
    config: &mut ModuleRuntimeConfig,
    module_id: &str,
    enabled: Option<bool>,
) -> Result<(), String> {
    match enabled {
        Some(enabled) => {
            let existing = config
                .module_ids()
                .find(|configured| module_ids_match(configured, module_id))
                .map(str::to_string);
            config
                .set(
                    existing.as_deref().unwrap_or(module_id),
                    ModuleRuntimeConfigEntry::enabled(enabled),
                )
                .map_err(|error| error.to_string())
        }
        None => {
            let matching = config
                .module_ids()
                .filter(|configured| module_ids_match(configured, module_id))
                .map(str::to_string)
                .collect::<Vec<_>>();
            for configured in matching {
                config.clear(&configured)?;
            }
            Ok(())
        }
    }
}

pub(crate) fn module_ids_match(left: &str, right: &str) -> bool {
    fn normalized(value: &str) -> Option<sinomo_runtime::module::ModuleId> {
        sinomo_runtime::module::ModuleId::new(value.to_string())
            .or_else(|_| sinomo_runtime::module::ModuleId::from_legacy_named(value))
            .ok()
    }

    normalized(left)
        .zip(normalized(right))
        .is_some_and(|(left, right)| left == right)
}

pub(crate) fn module_config_setting(
    config: &ModuleRuntimeConfig,
    module_id: &str,
) -> Option<ModuleRuntimeConfigEntry> {
    config.setting(module_id).or_else(|| {
        config
            .module_ids()
            .find(|configured| module_ids_match(configured, module_id))
            .and_then(|configured| config.setting(configured))
    })
}

fn remove_matching_observation(
    observations: &mut BTreeMap<String, (ModuleObservedState, String)>,
    module_id: &str,
) -> Option<(ModuleObservedState, String)> {
    let key = observations
        .keys()
        .find(|observed| module_ids_match(observed, module_id))
        .cloned()?;
    observations.remove(&key)
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
    if modules
        .keys()
        .any(|existing| module_ids_match(existing, module_id))
    {
        return;
    }
    modules
        .entry(module_id.to_string())
        .or_insert_with(|| ModuleConfigModuleSummary {
            module_id: module_id.to_string(),
            label: "Unnamed module".to_string(),
            backend: backend.into(),
            detail: detail.into(),
            desired_enabled: None,
            effective_enabled: false,
            effective_source: ModuleRuntimeConfigSource::DisabledDefault,
            observed: ModuleObservedState::Unavailable,
            observed_detail: String::new(),
        });
}

fn insert_native_dylib_module_summary(
    modules: &mut BTreeMap<String, ModuleConfigModuleSummary>,
    module_id: &str,
    detail: impl Into<String>,
) {
    let detail = detail.into();
    insert_module_summary(modules, module_id, "native-dylib", detail.clone());
    let matching = modules
        .keys()
        .find(|existing| module_ids_match(existing, module_id))
        .cloned();
    if let Some(module) = matching.and_then(|matching| modules.get_mut(&matching)) {
        if module.backend != "native-dylib" {
            module.backend = "native-dylib".to_string();
            module.detail = detail;
        }
    }
}

fn parse_node_id(node_id: &str) -> Result<NodeId, String> {
    let node_id = node_id.trim();
    NodeId::new(node_id.to_string())
        .or_else(|_| NodeId::from_legacy_named(node_id.to_string()))
        .map_err(|error| format!("invalid node id `{node_id}`: {error}"))
}

#[cfg(test)]
fn parse_module_id(module_id: &str) -> Result<sinomo_runtime::module::ModuleId, String> {
    let module_id = module_id.trim();
    sinomo_runtime::module::ModuleId::new(module_id.to_string())
        .or_else(|_| sinomo_runtime::module::ModuleId::from_legacy_named(module_id.to_string()))
        .map_err(|error| format!("invalid module id `{module_id}`: {error}"))
}

fn module_display_name(
    module_id: &str,
    names: &BTreeMap<sinomo_runtime::module::ModuleId, String>,
) -> Option<String> {
    names.iter().find_map(|(id, name)| {
        (module_ids_match(module_id, id.as_str()) && !name.trim().is_empty()).then(|| name.clone())
    })
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn module_titles_use_owner_supplied_display_names() {
        let legacy = "module.custom-provider";
        let id = sinomo_runtime::module::ModuleId::from_legacy_named(legacy).unwrap();
        let names = BTreeMap::from([(id.clone(), "Example Provider".to_string())]);
        assert_eq!(
            module_display_name(id.as_str(), &names).as_deref(),
            Some("Example Provider")
        );
        assert_eq!(
            module_display_name(legacy, &names).as_deref(),
            Some("Example Provider")
        );
        assert_eq!(module_display_name("module.unregistered", &names), None);
        let blank = BTreeMap::from([(id.clone(), String::new())]);
        assert_eq!(module_display_name(id.as_str(), &blank), None);
    }
    #[test]
    fn catalog_always_exposes_the_builtin_dfs_module() {
        let backend = Backend::local_fallback();
        let snapshot = backend.module_config_snapshot(None);
        let dfs = snapshot
            .modules
            .iter()
            .find(|module| {
                module_ids_match(
                    &module.module_id,
                    sinomo_runtime::dfs::SINOMO_DFS_MODULE_LEGACY_ID,
                )
            })
            .expect("built-in DFS module");
        assert_eq!(dfs.label, "Unnamed module");
        assert_eq!(dfs.backend, "native-rust");
    }

    #[test]
    fn catalog_uses_module_registered_nfs_presentation() {
        let backend = Backend::local_fallback();
        let snapshot = backend.module_config_snapshot(None);
        let nfs = snapshot
            .modules
            .iter()
            .find(|module| {
                module_ids_match(
                    &module.module_id,
                    sinomo_runtime::dfs_nfs::SINOMO_DFS_NFS_MODULE_LEGACY_ID,
                )
            })
            .expect("NFS module settings card");
        assert_eq!(nfs.label, sinomo_runtime::dfs_nfs::NFS_MODULE_LABEL);
        assert_eq!(nfs.detail, sinomo_runtime::dfs_nfs::NFS_MODULE_DESCRIPTION);
    }

    #[test]
    fn local_dfs_node_override_persists_native_rust_enablement() {
        let backend = Backend::local_fallback();
        let node_id = backend.node.node_id.to_string();

        backend
            .set_node_module_override(
                &node_id,
                sinomo_runtime::dfs::SINOMO_DFS_MODULE_LEGACY_ID,
                Some(false),
                true,
            )
            .expect("disable DFS from module settings");

        assert!(
            !sinomo_runtime::dfs::dfs_module_enabled(backend.data_dir.root())
                .expect("read DFS native Rust state")
        );
        let snapshot = backend.module_config_snapshot(Some(&node_id));
        assert_eq!(
            snapshot
                .selected_node
                .as_ref()
                .and_then(|node| {
                    module_config_setting(
                        &node.config,
                        sinomo_runtime::dfs::SINOMO_DFS_MODULE_LEGACY_ID,
                    )
                })
                .map(|setting| setting.enabled),
            Some(false)
        );
    }

    #[test]
    fn local_nfs_node_override_uses_generic_native_rust_path() {
        let backend = Backend::local_fallback();
        let node_id = backend.node.node_id.to_string();
        backend
            .set_node_module_override(
                &node_id,
                sinomo_runtime::dfs_nfs::SINOMO_DFS_NFS_MODULE_LEGACY_ID,
                Some(false),
                true,
            )
            .expect("disable NFS module from generic settings path");
        assert!(
            !sinomo_runtime::dfs_nfs::nfs_module_enabled(&backend.data_dir)
                .expect("read NFS native Rust state")
        );
    }

    #[test]
    fn missing_cluster_state_never_classifies_an_arbitrary_node_as_local() {
        let fixture = sinomo_testdir::TestDir::new("app-module-locality-without-cluster");
        let backend = Backend::local_fallback_for_data_dir(
            sinomo_runtime::local::SinomoDataDir::new(fixture.path()),
        );
        assert!(!ClusterPaths::new(backend.data_dir.as_ref()).state.exists());
        assert!(backend.module_config_node_is_local(&backend.node.node_id.to_string()));
        assert!(!backend.module_config_node_is_local("remote-node"));
    }

    #[test]
    fn catalog_keeps_desired_effective_and_observed_state_separate() {
        let backend = Backend::local_fallback();
        let module_id = "module.test-lifecycle";
        let mut cluster = ModuleRuntimeConfig::default();
        cluster
            .set(module_id, ModuleRuntimeConfigEntry::enabled(true))
            .expect("cluster desired state");
        let mut node_config = ModuleRuntimeConfig::default();
        node_config
            .set(module_id, ModuleRuntimeConfigEntry::enabled(false))
            .expect("node override");
        let node = ModuleConfigNode {
            summary: ModuleConfigNodeSummary {
                node_id: backend.node.node_id.to_string(),
                display_name: "Local workstation".to_string(),
                availability: "online".to_string(),
                local: true,
            },
            config: node_config,
            error: None,
        };

        let modules = backend.module_config_catalog(&cluster, Some(&node), &mut Vec::new());
        let module = modules
            .iter()
            .find(|module| module.module_id == module_id)
            .expect("configured module");
        assert_eq!(module.label, "Unnamed module");
        assert_eq!(module.desired_enabled, Some(true));
        assert!(!module.effective_enabled);
        assert_eq!(module.effective_source, ModuleRuntimeConfigSource::Node);
        assert_eq!(module.observed, ModuleObservedState::NotReported);
    }

    #[test]
    fn remote_observation_is_explicitly_unavailable() {
        let backend = Backend::local_fallback();
        let module_id = "module.remote-test";
        let mut cluster = ModuleRuntimeConfig::default();
        cluster
            .set(module_id, ModuleRuntimeConfigEntry::enabled(true))
            .expect("cluster desired state");
        let node = ModuleConfigNode {
            summary: ModuleConfigNodeSummary {
                node_id: "remote-node".to_string(),
                display_name: "Studio node".to_string(),
                availability: "online".to_string(),
                local: false,
            },
            config: ModuleRuntimeConfig::default(),
            error: None,
        };

        let modules = backend.module_config_catalog(&cluster, Some(&node), &mut Vec::new());
        let module = modules
            .iter()
            .find(|module| module.module_id == module_id)
            .expect("configured module");
        assert_eq!(module.observed, ModuleObservedState::Unavailable);
        assert!(module.observed_detail.contains("does not publish"));
    }

    #[test]
    fn configured_native_dylib_keeps_its_loader_observation() {
        let backend = Backend::local_fallback();
        let module_id = "module.configured-native-test";
        let typed_module_id = sinomo_runtime::module::ModuleId::from_legacy_named(module_id)
            .expect("configured native module id");
        let canonical_module_id = typed_module_id.to_string();
        sinomo_runtime::module::write_native_dylib_module_loader_state(
            &backend.data_dir.root(),
            &sinomo_runtime::module::NativeDylibModuleLoaderState {
                module_id: typed_module_id,
                artifact_id: None,
                content_hash: "00".repeat(32),
                replaces_content_hash: None,
                cache_key: "configured/native-test".to_string(),
                library_name: "libconfigured_native_test.so".to_string(),
                platform_triple: format!(
                    "{}-{}-{}",
                    std::env::consts::ARCH,
                    std::env::consts::OS,
                    std::env::consts::FAMILY
                ),
                native_abi_version: sinomo_runtime::module::NATIVE_DYLIB_HOST_ABI_VERSION,
                interface_version: "sinomo.configured-native-test.test".to_string(),
                activated_at: "2026-07-18T00:00:00Z".to_string(),
                source: "test".to_string(),
            },
        )
        .expect("write native loader record");
        let mut cluster = ModuleRuntimeConfig::default();
        cluster
            .set(
                &canonical_module_id,
                ModuleRuntimeConfigEntry::enabled(true),
            )
            .expect("cluster desired state");
        let node = ModuleConfigNode {
            summary: ModuleConfigNodeSummary {
                node_id: backend.node.node_id.to_string(),
                display_name: "Local workstation".to_string(),
                availability: "online".to_string(),
                local: true,
            },
            config: ModuleRuntimeConfig::default(),
            error: None,
        };

        let modules = backend.module_config_catalog(&cluster, Some(&node), &mut Vec::new());
        let module = modules
            .iter()
            .find(|module| module.module_id == canonical_module_id)
            .expect("configured native module");
        assert_eq!(module.backend, "native-dylib");
        assert_eq!(
            module.label, "Unnamed module",
            "never infer a name from a library filename"
        );
        assert_eq!(module.observed, ModuleObservedState::NotReported);
        assert!(
            module
                .observed_detail
                .contains("daemon registration is not independently reported")
        );
    }
}
