//! Shared module configuration and observation values.
//! Component construction, dynamic library loading, and execution belong to sinomo-module.

use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::path::PathBuf;

pub const MODULE_UI_CATALOG_ROUTE: &str = "/module/ui/catalog";

pub const MODULE_UI_RPC_CATEGORY: &str = "module.ui";

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct UiExtensionCatalogRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extension_point_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct UiExtensionCatalogEntry {
    pub module_id: String,
    pub module_version: String,
    pub module_backend: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub component_path: Option<String>,
    pub contribution: sinomo_module_api::UiExtensionContribution,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct UiExtensionCatalogResponse {
    pub registry_generation: u64,
    pub entries: Vec<UiExtensionCatalogEntry>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

pub type ArtifactId = sinomo_core::Id;

pub type ModuleId = sinomo_core::Id;

pub const MODULE_OBJECT_VERSION: u16 = 1;

pub const MODULE_NATIVE_RUST_LIST_ROUTE: &str = "/module/native-rust";

pub const MODULE_NATIVE_RUST_SET_ENABLED_ROUTE: &str = "/module/native-rust/set-enabled";

pub const MODULE_NATIVE_RUST_RPC_CATEGORY: &str = "module.native-rust";

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct NativeRustModuleListRequest {}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NativeRustModuleStatus {
    pub module_id: String,
    pub configured_enabled: bool,
    pub active: bool,
    pub explicit: bool,
    pub restart_required: bool,
    pub detail: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NativeRustModuleListResponse {
    pub modules: Vec<NativeRustModuleStatus>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NativeRustModuleSetEnabledRequest {
    pub module_id: String,
    pub enabled: bool,
    pub reason: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NativeRustModuleSetEnabledResponse {
    pub module: NativeRustModuleStatus,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NativeDylibModuleLoaderState {
    pub module_id: ModuleId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_id: Option<ArtifactId>,
    pub content_hash: String,
    /// Exact previously active content hash that this activation replaces.
    ///
    /// Consumers with durable provider ownership use this as an explicit,
    /// one-step replacement intent. A bare loader mismatch is not sufficient
    /// authority to retire an owned provider.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replaces_content_hash: Option<String>,
    pub cache_key: String,
    pub library_name: String,
    pub platform_triple: String,
    pub native_abi_version: u32,
    pub interface_version: String,
    pub activated_at: String,
    pub source: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NativeDylibModuleCacheEntry {
    pub module_id: ModuleId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    pub content_hash: String,
    pub cache_key: String,
    pub library_name: String,
    pub platform_triple: String,
    pub native_abi_version: u32,
    pub interface_version: String,
    pub library_path: PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NativeRustModuleRuntimeState {
    pub module_id: ModuleId,
    pub enabled: bool,
    pub updated_at: String,
    pub source: String,
    pub reason: String,
}

pub const MODULE_CLUSTER_CONFIG_KEY: &str = "module.runtime.cluster-defaults";

pub const MODULE_NODE_CONFIG_KEY: &str = "module.runtime.node-overrides";

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ModuleRuntimeConfig {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub modules: BTreeMap<String, ModuleRuntimeConfigEntry>,
}

impl ModuleRuntimeConfig {
    pub fn module_ids(&self) -> impl Iterator<Item = &str> {
        self.modules.keys().map(String::as_str)
    }

    pub fn setting(&self, module_id: &str) -> Option<ModuleRuntimeConfigEntry> {
        self.modules.get(module_id).copied()
    }

    pub fn set(
        &mut self,
        module_id: impl AsRef<str>,
        setting: ModuleRuntimeConfigEntry,
    ) -> Result<(), String> {
        let module_id = normalize_module_config_id(module_id.as_ref())?;
        self.modules.insert(module_id, setting);
        Ok(())
    }

    pub fn clear(&mut self, module_id: impl AsRef<str>) -> Result<(), String> {
        let module_id = normalize_module_config_id(module_id.as_ref())?;
        self.modules.remove(&module_id);
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ModuleRuntimeConfigEntry {
    pub enabled: bool,
}

impl ModuleRuntimeConfigEntry {
    pub fn enabled(enabled: bool) -> Self {
        Self { enabled }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ModuleRuntimeConfigSource {
    Node,
    Cluster,
    DisabledDefault,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ResolvedModuleRuntimeConfig {
    pub enabled: bool,
    pub source: ModuleRuntimeConfigSource,
}

pub fn resolve_module_runtime_config(
    node: &ModuleRuntimeConfig,
    cluster: &ModuleRuntimeConfig,
    module_id: &str,
) -> ResolvedModuleRuntimeConfig {
    if let Some(setting) = node.setting(module_id) {
        return ResolvedModuleRuntimeConfig {
            enabled: setting.enabled,
            source: ModuleRuntimeConfigSource::Node,
        };
    }
    if let Some(setting) = cluster.setting(module_id) {
        return ResolvedModuleRuntimeConfig {
            enabled: setting.enabled,
            source: ModuleRuntimeConfigSource::Cluster,
        };
    }
    ResolvedModuleRuntimeConfig {
        enabled: false,
        source: ModuleRuntimeConfigSource::DisabledDefault,
    }
}

fn normalize_module_config_id(module_id: &str) -> Result<String, String> {
    let module_id = module_id.trim();
    if module_id.is_empty() {
        return Err("module id must not be empty".to_string());
    }
    if ModuleId::new(module_id).is_ok() || ModuleId::from_legacy_named(module_id).is_ok() {
        Ok(module_id.to_string())
    } else {
        Err(format!("invalid module id `{module_id}`"))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DeclarativeModuleLoaderState {
    pub module_id: ModuleId,
    pub version: String,
    pub content_hash: String,
    pub payload_ref: String,
    pub active: bool,
    pub installed_at: String,
    pub source: String,
}

/// Local daemon projection of installed and active module records.
pub const MODULE_OBSERVATIONS_ROUTE: &str = "/module/observations";

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ModuleObservationsRequest {}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ModuleObservationsResponse {
    /// Display names declared by registered or installed module descriptors.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub display_names: BTreeMap<ModuleId, String>,
    pub native_dylib: Result<Vec<NativeDylibModuleLoaderState>, String>,
    pub inactive_cache: Result<Vec<NativeDylibModuleCacheEntry>, String>,
    pub native_rust: Result<Vec<NativeRustModuleRuntimeState>, String>,
    pub declarative: Result<Vec<DeclarativeModuleLoaderState>, String>,
}

impl ModuleObservationsResponse {
    pub fn unavailable(detail: String) -> Self {
        Self {
            display_names: BTreeMap::new(),
            native_dylib: Err(detail.clone()),
            inactive_cache: Err(detail.clone()),
            native_rust: Err(detail.clone()),
            declarative: Err(detail),
        }
    }
}
