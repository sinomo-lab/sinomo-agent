//! Module runtime configuration snapshot and mutations for the Settings UI.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;
use std::time::Duration;

use serde::de::DeserializeOwned;
use sinomo_runtime::api::DaemonApiClient;
use sinomo_runtime::config::{
    CONFIG_GET_ROUTE, CONFIG_RPC_CATEGORY, CONFIG_SET_ROUTE, ConfigGetRequest, ConfigGetResponse,
    ConfigSetRequest, ConfigSetResponse, ConfigWriteTarget,
};
use sinomo_runtime::module::{
    MODULE_CLUSTER_CONFIG_KEY, MODULE_NODE_CONFIG_KEY, ModuleRuntimeConfig,
    ModuleRuntimeConfigEntry, ModuleRuntimeConfigSource, read_declarative_module_loader_states,
    read_native_dylib_module_cache_entries_excluding_cache_keys,
    read_native_dylib_module_loader_states, read_native_rust_module_runtime_states,
    resolve_module_runtime_config,
};
use sinomo_runtime::network::{ClusterPaths, ObjectRpcRequest};
use sinomo_runtime::node::NodeId;

use crate::backend::Backend;

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
    ) -> Result<(), String> {
        if self.module_config_node_is_local(node_id) {
            return self.set_local_module_override(module_id, enabled);
        }
        let mut config = self.load_node_module_config(node_id)?;
        apply_module_override(&mut config, module_id, enabled)?;
        self.set_remote_node_module_config(node_id, &config)
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
        match read_native_dylib_module_loader_states(&self.data_dir.root()) {
            Ok(states) => {
                let mut active_cache_keys = BTreeSet::new();
                for state in states {
                    active_cache_keys.insert(state.cache_key.clone());
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
                match read_native_dylib_module_cache_entries_excluding_cache_keys(
                    &self.data_dir.root(),
                    &active_cache_keys,
                ) {
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
        if let Ok(states) = read_native_rust_module_runtime_states(&self.data_dir.root()) {
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
        if let Ok(states) = read_declarative_module_loader_states(&self.data_dir.root()) {
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
        insert_module_summary(
            &mut modules,
            sinomo_runtime::remote_management::REMOTE_MANAGEMENT_MODULE_ID,
            "native-dylib",
            "Privileged node administration module",
        );
        if let Some(module) =
            modules.get_mut(sinomo_runtime::remote_management::REMOTE_MANAGEMENT_MODULE_ID)
        {
            module.backend = "native-dylib".to_string();
            module.detail = "Privileged node administration module".to_string();
        }

        let selected_is_local = selected_node.is_some_and(|node| node.summary.local);
        let empty_node_config = ModuleRuntimeConfig::default();
        let node_config = selected_node
            .map(|node| &node.config)
            .unwrap_or(&empty_node_config);
        for module in modules.values_mut() {
            module.desired_enabled = cluster_config
                .setting(&module.module_id)
                .map(|setting| setting.enabled);
            let effective =
                resolve_module_runtime_config(node_config, cluster_config, &module.module_id);
            module.effective_enabled = effective.enabled;
            module.effective_source = effective.source;
            if selected_is_local {
                let observation = if module.backend == "native-dylib" {
                    native_dylib_observations.remove(&module.module_id)
                } else {
                    observations.remove(&module.module_id)
                };
                let (state, detail) = observation.unwrap_or_else(|| {
                    if module.backend == "native-dylib" {
                        if let Some(error) = native_dylib_observation_error.as_ref() {
                            return (ModuleObservedState::Unavailable, error.clone());
                        }
                    }
                    (
                        ModuleObservedState::NotReported,
                        "The local loader has not published an observation".to_string(),
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
        Req: serde::Serialize + ?Sized,
        Resp: DeserializeOwned,
    {
        let address = self
            .connection
            .daemon_address()
            .ok_or_else(|| "remote node module config requires a daemon connection".to_string())?;
        let target_node_id = parse_node_id(node_id)?;
        let inner = ObjectRpcRequest::encode_body(route, CONFIG_RPC_CATEGORY, body)
            .map_err(|error| error.to_string())?;
        let response = DaemonApiClient::at_address(address)
            .relay_signed_request_to_node_from_data_dir(
                self.data_dir.as_ref(),
                &target_node_id,
                inner,
                MODULE_CONFIG_RPC_TIMEOUT,
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
        Some(enabled) => config
            .set(module_id, ModuleRuntimeConfigEntry::enabled(enabled))
            .map_err(|error| error.to_string()),
        None => config.clear(module_id).map_err(|error| error.to_string()),
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
    if let Some(module) = modules.get_mut(module_id.trim()) {
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

fn module_label(module_id: &str) -> String {
    match module_id {
        "module.observe" => return "Observability".to_string(),
        "module.sandbox.wasix" => return "WASIX Sandbox".to_string(),
        "module.dfs.cloud-files" => return "Cloud Files".to_string(),
        "module.remote-management" => return "Remote Management".to_string(),
        "module.network-iroh" => return "Iroh Networking".to_string(),
        "module.benchmarks" => return "Benchmarks".to_string(),
        "module.dfs" => return "Distributed Filesystem".to_string(),
        "module.network" => return "Networking".to_string(),
        "module.rollout" => return "Rollout".to_string(),
        "module.evolution" => return "Evolution".to_string(),
        _ => {}
    }
    module_id
        .strip_prefix("module.")
        .unwrap_or(module_id)
        .split(|ch| matches!(ch, '.' | '-' | '_'))
        .filter(|part| !part.is_empty())
        .map(|part| {
            if part.eq_ignore_ascii_case("wasix") {
                "WASIX".to_string()
            } else {
                let mut chars = part.chars();
                chars
                    .next()
                    .map(|first| first.to_uppercase().collect::<String>() + chars.as_str())
                    .unwrap_or_default()
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn module_labels_hide_opaque_runtime_ids() {
        assert_eq!(module_label("module.sandbox.wasix"), "WASIX Sandbox");
        assert_eq!(module_label("module.dfs.cloud-files"), "Cloud Files");
        assert_eq!(module_label("module.custom-data_sync"), "Custom Data Sync");
    }

    #[test]
    fn missing_cluster_state_never_classifies_an_arbitrary_node_as_local() {
        let backend = Backend::local_fallback();
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
        assert_eq!(module.label, "Test Lifecycle");
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
        sinomo_runtime::module::write_native_dylib_module_loader_state(
            &backend.data_dir.root(),
            &sinomo_runtime::module::NativeDylibModuleLoaderState {
                module_id: typed_module_id,
                artifact_id: None,
                content_hash: "00".repeat(32),
                cache_key: "configured/native-test".to_string(),
                library_name: "configured-native-test.test".to_string(),
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
            .set(module_id, ModuleRuntimeConfigEntry::enabled(true))
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
            .find(|module| module.module_id == module_id)
            .expect("configured native module");
        assert_eq!(module.backend, "native-dylib");
        assert_eq!(module.observed, ModuleObservedState::NotReported);
        assert!(
            module
                .observed_detail
                .contains("daemon registration is not independently reported")
        );
    }
}
