//! Shared configuration registry, resolution, storage, and management service.
//!
//! Owner crates declare config definitions. The config layer resolves whether a value
//! is local, cluster-backed, synchronized, or defaulted, and hides the storage details
//! from callers. Local values are human-readable JSON documents under
//! `$SINOMO_DATA_DIR/configs/`; cluster values are typed Sinomo-object values in
//! `sinomo-kv` under `/config/...`.

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sinomo_kv::{KvEntry, KvOp, KvTxn};
use sinomo_local::SinomoDataDir;
use sinomo_network::{ObjectRpcRequest, ObjectRpcResponse};
use sinomo_service::{
    ClusterAccess, Service, ServiceContext, ServiceDisposition, ServiceError, ServiceFuture,
    ServiceRegistryBuilder, ServiceRoute,
};
use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

pub const CONFIG_SERVICE_NAME: &str = "config";
pub const CONFIG_RPC_CATEGORY: &str = "config";
pub const CONFIG_CATALOG_ROUTE: &str = "/config/catalog";
pub const CONFIG_SEARCH_ROUTE: &str = "/config/search";
pub const CONFIG_GET_ROUTE: &str = "/config/get";
pub const CONFIG_SET_ROUTE: &str = "/config/set";
pub const CONFIG_OBJECT_VERSION: u16 = 1;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
pub struct ConfigKey(String);

impl ConfigKey {
    pub fn new(value: impl Into<String>) -> Result<Self, ConfigError> {
        let value = value.into();
        validate_config_key(&value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn cluster_key(&self) -> String {
        format!(
            "/config/{}",
            key_storage_path(&self.0).trim_end_matches(".json")
        )
    }

    fn local_relative_path(&self) -> PathBuf {
        PathBuf::from(key_storage_path(&self.0))
    }
}

impl fmt::Display for ConfigKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ConfigClusterMode {
    LocalOnly,
    LocalOverrideCluster,
    ClusterOverrideLocal,
    Synced,
    ClusterOnly,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum ConfigValueType {
    Bool,
    I64 { range: Option<ConfigNumberRange> },
    U64 { range: Option<ConfigNumberRange> },
    F64 { range: Option<ConfigNumberRange> },
    String { choices: Vec<String> },
    StringList,
    Object { schema_name: String },
    Json,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ConfigNumberRange {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<f64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ConfigDefinition {
    pub key: ConfigKey,
    pub owner: String,
    pub value_type: ConfigValueType,
    pub description: String,
    pub cluster_mode: ConfigClusterMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_value: Option<Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub local_legacy_paths: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cluster_legacy_keys: Vec<String>,
}

impl ConfigDefinition {
    pub fn new(
        key: impl Into<String>,
        owner: impl Into<String>,
        value_type: ConfigValueType,
        description: impl Into<String>,
        cluster_mode: ConfigClusterMode,
    ) -> Result<Self, ConfigError> {
        Ok(Self {
            key: ConfigKey::new(key)?,
            owner: owner.into(),
            value_type,
            description: description.into(),
            cluster_mode,
            default_value: None,
            local_legacy_paths: Vec::new(),
            cluster_legacy_keys: Vec::new(),
        })
    }

    pub fn with_default<T: Serialize>(mut self, value: &T) -> Result<Self, ConfigError> {
        let value = serde_json::to_value(value).map_err(|source| ConfigError::Serde {
            path: self.key.to_string(),
            detail: source.to_string(),
        })?;
        self.validate_value(&value)?;
        self.default_value = Some(value);
        Ok(self)
    }

    pub fn with_local_legacy_path(mut self, relative_path: impl Into<String>) -> Self {
        self.local_legacy_paths.push(relative_path.into());
        self
    }

    pub fn with_cluster_legacy_key(mut self, key: impl Into<String>) -> Self {
        self.cluster_legacy_keys.push(key.into());
        self
    }

    pub fn cluster_key(&self) -> String {
        self.key.cluster_key()
    }

    pub fn local_path(&self, data_dir: &SinomoDataDir) -> PathBuf {
        configs_dir(data_dir).join(self.key.local_relative_path())
    }

    pub fn validate_value(&self, value: &Value) -> Result<(), ConfigError> {
        validate_value_type(&self.key, &self.value_type, value)
    }
}

#[derive(Clone, Debug, Default)]
pub struct ConfigRegistry {
    definitions: BTreeMap<ConfigKey, ConfigDefinition>,
}

impl ConfigRegistry {
    pub fn builder() -> ConfigRegistryBuilder {
        ConfigRegistryBuilder::default()
    }

    pub fn get(&self, key: &str) -> Result<&ConfigDefinition, ConfigError> {
        let key = ConfigKey::new(key.to_string())?;
        self.definitions
            .get(&key)
            .ok_or_else(|| ConfigError::UnknownKey {
                key: key.to_string(),
            })
    }

    pub fn definitions(&self) -> impl Iterator<Item = &ConfigDefinition> {
        self.definitions.values()
    }

    pub fn search(&self, query: &str) -> Vec<ConfigDefinition> {
        let query = query.to_ascii_lowercase();
        self.definitions()
            .filter(|definition| {
                query.is_empty()
                    || definition
                        .key
                        .as_str()
                        .to_ascii_lowercase()
                        .contains(&query)
                    || definition.owner.to_ascii_lowercase().contains(&query)
                    || definition.description.to_ascii_lowercase().contains(&query)
            })
            .cloned()
            .collect()
    }
}

#[derive(Clone, Debug, Default)]
pub struct ConfigRegistryBuilder {
    definitions: BTreeMap<ConfigKey, ConfigDefinition>,
}

impl ConfigRegistryBuilder {
    pub fn register(mut self, definition: ConfigDefinition) -> Result<Self, ConfigError> {
        if self
            .definitions
            .insert(definition.key.clone(), definition.clone())
            .is_some()
        {
            return Err(ConfigError::DuplicateKey {
                key: definition.key.to_string(),
            });
        }
        Ok(self)
    }

    pub fn build(self) -> ConfigRegistry {
        ConfigRegistry {
            definitions: self.definitions,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ConfigDocument {
    pub key: ConfigKey,
    pub value: Value,
    pub updated_at: String,
}

impl ConfigDocument {
    pub fn new(key: ConfigKey, value: Value) -> Self {
        Self {
            key,
            value,
            updated_at: config_timestamp_now(),
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ConfigValueSource {
    Default,
    Local,
    Cluster,
    MigratedLocal,
    LegacyCluster,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ConfigResolution {
    pub key: ConfigKey,
    pub value: Value,
    pub source: ConfigValueSource,
    pub definition: ConfigDefinition,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ConfigWriteTarget {
    Auto,
    Local,
    Cluster,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ConfigWriteResult {
    pub key: ConfigKey,
    pub value: Value,
    pub source: ConfigValueSource,
}

#[derive(Clone, Debug)]
pub struct ConfigStore {
    registry: ConfigRegistry,
}

impl ConfigStore {
    pub fn new(registry: ConfigRegistry) -> Self {
        Self { registry }
    }

    pub fn registry(&self) -> &ConfigRegistry {
        &self.registry
    }

    pub fn get(
        &self,
        data_dir: &SinomoDataDir,
        cluster: Option<&dyn ClusterAccess>,
        key: &str,
    ) -> Result<ConfigResolution, ConfigError> {
        let definition = self.registry.get(key)?.clone();
        resolve_config(data_dir, cluster, &definition)
    }

    pub fn get_typed<T: DeserializeOwned>(
        &self,
        data_dir: &SinomoDataDir,
        cluster: Option<&dyn ClusterAccess>,
        key: &str,
    ) -> Result<T, ConfigError> {
        let resolution = self.get(data_dir, cluster, key)?;
        serde_json::from_value(resolution.value).map_err(|source| ConfigError::Serde {
            path: key.to_string(),
            detail: source.to_string(),
        })
    }

    pub fn set(
        &self,
        data_dir: &SinomoDataDir,
        cluster: Option<&dyn ClusterAccess>,
        key: &str,
        value: Value,
        target: ConfigWriteTarget,
    ) -> Result<ConfigWriteResult, ConfigError> {
        let definition = self.registry.get(key)?.clone();
        definition.validate_value(&value)?;
        write_config(data_dir, cluster, &definition, value, target)
    }

    pub async fn set_async(
        &self,
        data_dir: &SinomoDataDir,
        cluster: Option<&dyn ClusterAccess>,
        key: &str,
        value: Value,
        target: ConfigWriteTarget,
    ) -> Result<ConfigWriteResult, ConfigError> {
        let definition = self.registry.get(key)?.clone();
        definition.validate_value(&value)?;
        write_config_async(data_dir, cluster, &definition, value, target).await
    }

    pub fn set_typed<T: Serialize>(
        &self,
        data_dir: &SinomoDataDir,
        cluster: Option<&dyn ClusterAccess>,
        key: &str,
        value: &T,
        target: ConfigWriteTarget,
    ) -> Result<ConfigWriteResult, ConfigError> {
        let value = serde_json::to_value(value).map_err(|source| ConfigError::Serde {
            path: key.to_string(),
            detail: source.to_string(),
        })?;
        self.set(data_dir, cluster, key, value, target)
    }
}

/// A client-facing view of the configuration service — the single surface that the app,
/// daemon, and CLI all share. Implementations may be in-process ([`LocalConfigClient`]) or
/// talk to a remote daemon over RPC; callers cannot tell the difference, which keeps every
/// component on the same configuration source of truth.
///
/// Object-safe on purpose (typed conveniences live in [`ConfigClientExt`]).
pub trait ConfigClient: Send + Sync {
    /// All config definitions, optionally filtered to an owner crate.
    fn catalog(&self, owner: Option<&str>) -> Result<Vec<ConfigDefinition>, ConfigError>;

    /// Resolve a single key. The cluster/local layering is applied server-side.
    fn get(&self, key: &str) -> Result<ConfigResolution, ConfigError>;

    /// Write a key to the requested target (cluster / local / auto).
    fn set(
        &self,
        key: &str,
        value: Value,
        target: ConfigWriteTarget,
    ) -> Result<ConfigWriteResult, ConfigError>;
}

/// Typed conveniences over a [`ConfigClient`]; kept off the object-safe trait so `dyn
/// ConfigClient` stays usable.
pub trait ConfigClientExt: ConfigClient {
    fn get_typed<T: DeserializeOwned>(&self, key: &str) -> Result<T, ConfigError> {
        let resolution = self.get(key)?;
        serde_json::from_value(resolution.value).map_err(|source| ConfigError::Serde {
            path: key.to_string(),
            detail: source.to_string(),
        })
    }

    fn set_typed<T: Serialize>(
        &self,
        key: &str,
        value: &T,
        target: ConfigWriteTarget,
    ) -> Result<ConfigWriteResult, ConfigError> {
        let value = serde_json::to_value(value).map_err(|source| ConfigError::Serde {
            path: key.to_string(),
            detail: source.to_string(),
        })?;
        self.set(key, value, target)
    }
}

impl<C: ConfigClient + ?Sized> ConfigClientExt for C {}

/// In-process [`ConfigClient`] backed directly by a [`ConfigStore`] and an optional cluster
/// handle — the "same process, no channel" implementation. Resolves exactly as the config
/// service does for a remote caller.
#[derive(Clone)]
pub struct LocalConfigClient {
    store: Arc<ConfigStore>,
    data_dir: SinomoDataDir,
    cluster: Option<Arc<dyn ClusterAccess>>,
}

impl LocalConfigClient {
    pub fn new(
        store: Arc<ConfigStore>,
        data_dir: SinomoDataDir,
        cluster: Option<Arc<dyn ClusterAccess>>,
    ) -> Self {
        Self {
            store,
            data_dir,
            cluster,
        }
    }
}

impl ConfigClient for LocalConfigClient {
    fn catalog(&self, owner: Option<&str>) -> Result<Vec<ConfigDefinition>, ConfigError> {
        Ok(self
            .store
            .registry()
            .definitions()
            .filter(|definition| owner.is_none_or(|owner| definition.owner.as_str() == owner))
            .cloned()
            .collect())
    }

    fn get(&self, key: &str) -> Result<ConfigResolution, ConfigError> {
        self.store.get(&self.data_dir, self.cluster.as_deref(), key)
    }

    fn set(
        &self,
        key: &str,
        value: Value,
        target: ConfigWriteTarget,
    ) -> Result<ConfigWriteResult, ConfigError> {
        self.store
            .set(&self.data_dir, self.cluster.as_deref(), key, value, target)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ConfigCatalogRequest {
    #[serde(default)]
    pub owner: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ConfigCatalogResponse {
    pub definitions: Vec<ConfigDefinition>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ConfigSearchRequest {
    pub query: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ConfigSearchResponse {
    pub definitions: Vec<ConfigDefinition>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ConfigGetRequest {
    pub key: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ConfigGetResponse {
    pub resolution: ConfigResolution,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ConfigSetRequest {
    pub key: String,
    pub value: Value,
    #[serde(default = "default_write_target")]
    pub target: ConfigWriteTarget,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ConfigSetResponse {
    pub result: ConfigWriteResult,
}

fn default_write_target() -> ConfigWriteTarget {
    ConfigWriteTarget::Auto
}

#[derive(Clone)]
pub struct ConfigService {
    store: Arc<ConfigStore>,
}

impl ConfigService {
    pub fn new(registry: ConfigRegistry) -> Self {
        Self {
            store: Arc::new(ConfigStore::new(registry)),
        }
    }
}

impl Service for ConfigService {
    fn name(&self) -> &'static str {
        CONFIG_SERVICE_NAME
    }

    fn routes(&self) -> &'static [&'static str] {
        &[
            CONFIG_CATALOG_ROUTE,
            CONFIG_SEARCH_ROUTE,
            CONFIG_GET_ROUTE,
            CONFIG_SET_ROUTE,
        ]
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        vec![
            ServiceRoute::new(CONFIG_CATALOG_ROUTE, CONFIG_RPC_CATEGORY),
            ServiceRoute::new(CONFIG_SEARCH_ROUTE, CONFIG_RPC_CATEGORY),
            ServiceRoute::new(CONFIG_GET_ROUTE, CONFIG_RPC_CATEGORY).with_concurrency_limit(8),
            ServiceRoute::new(CONFIG_SET_ROUTE, CONFIG_RPC_CATEGORY).with_concurrency_limit(8),
        ]
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let store = Arc::clone(&self.store);
        Box::pin(async move {
            let cluster = ctx.cluster.as_deref();
            let response = match request.route.as_str() {
                CONFIG_CATALOG_ROUTE => {
                    let request: ConfigCatalogRequest = request.decode_body()?;
                    let definitions = store
                        .registry()
                        .definitions()
                        .filter(|definition| {
                            request
                                .owner
                                .as_ref()
                                .is_none_or(|owner| definition.owner == *owner)
                        })
                        .cloned()
                        .collect();
                    ObjectRpcResponse::encode_body(
                        CONFIG_CATALOG_ROUTE,
                        &ConfigCatalogResponse { definitions },
                    )?
                }
                CONFIG_SEARCH_ROUTE => {
                    let request: ConfigSearchRequest = request.decode_body()?;
                    let definitions = store.registry().search(&request.query);
                    ObjectRpcResponse::encode_body(
                        CONFIG_SEARCH_ROUTE,
                        &ConfigSearchResponse { definitions },
                    )?
                }
                CONFIG_GET_ROUTE => {
                    let request: ConfigGetRequest = request.decode_body()?;
                    let resolution = store.get(&ctx.data_dir, cluster, &request.key)?;
                    ObjectRpcResponse::encode_body(
                        CONFIG_GET_ROUTE,
                        &ConfigGetResponse { resolution },
                    )?
                }
                CONFIG_SET_ROUTE => {
                    let request: ConfigSetRequest = request.decode_body()?;
                    let result = store
                        .set_async(
                            &ctx.data_dir,
                            cluster,
                            &request.key,
                            request.value,
                            request.target,
                        )
                        .await?;
                    ObjectRpcResponse::encode_body(CONFIG_SET_ROUTE, &ConfigSetResponse { result })?
                }
                other => return Err(ServiceError::UnknownRoute(other.to_string())),
            };
            ctx.respond(response)?;
            Ok(ServiceDisposition::Continue)
        })
    }
}

pub fn register_services(
    builder: ServiceRegistryBuilder,
    registry: ConfigRegistry,
) -> ServiceRegistryBuilder {
    builder.register(ConfigService::new(registry))
}

pub fn register_storage_paths(
    layout: &mut sinomo_storage::StorageLayout,
) -> Result<(), sinomo_storage::StorageContractError> {
    layout.register_escape_path(
        "sinomo-config",
        sinomo_storage::StorageRing::R1,
        "configs",
        "human-readable shared local configuration documents managed by sinomo-config",
    )
}

pub fn configs_dir(data_dir: &SinomoDataDir) -> PathBuf {
    data_dir.root().join("configs")
}

pub fn cluster_config_put_op(
    definition: &ConfigDefinition,
    value: Value,
) -> Result<KvOp, ConfigError> {
    definition.validate_value(&value)?;
    let document = ConfigDocument::new(definition.key.clone(), value);
    KvOp::put_serde(definition.cluster_key(), &document, None).map_err(ConfigError::Kv)
}

pub fn cluster_config_put_txn(
    definition: &ConfigDefinition,
    value: Value,
) -> Result<KvTxn, ConfigError> {
    Ok(KvTxn {
        compares: Vec::new(),
        success: vec![cluster_config_put_op(definition, value)?],
        failure: Vec::new(),
    })
}

pub fn decode_cluster_config_entry(
    entry: &KvEntry,
    definition: &ConfigDefinition,
) -> Result<Value, ConfigError> {
    if let Ok(document) = entry.decode::<ConfigDocument>() {
        if document.key != definition.key {
            return Err(ConfigError::InvalidValue {
                key: definition.key.to_string(),
                detail: format!(
                    "cluster config document key `{}` does not match requested `{}`",
                    document.key, definition.key
                ),
            });
        }
        definition.validate_value(&document.value)?;
        return Ok(document.value);
    }
    let value: Value = entry.decode().map_err(ConfigError::Kv)?;
    definition.validate_value(&value)?;
    Ok(value)
}

fn resolve_config(
    data_dir: &SinomoDataDir,
    cluster: Option<&dyn ClusterAccess>,
    definition: &ConfigDefinition,
) -> Result<ConfigResolution, ConfigError> {
    let resolved = match definition.cluster_mode {
        ConfigClusterMode::LocalOnly => read_or_migrate_local_config(data_dir, definition)?,
        ConfigClusterMode::LocalOverrideCluster => {
            if let Some(local) = read_or_migrate_local_config(data_dir, definition)? {
                Some(local)
            } else {
                read_cluster_config(cluster, definition)?
            }
        }
        ConfigClusterMode::ClusterOverrideLocal | ConfigClusterMode::Synced => {
            if let Some(cluster) = read_cluster_config(cluster, definition)? {
                Some(cluster)
            } else {
                read_or_migrate_local_config(data_dir, definition)?
            }
        }
        ConfigClusterMode::ClusterOnly => read_cluster_config(cluster, definition)?,
    };
    resolved
        .or_else(|| {
            definition
                .default_value
                .clone()
                .map(|value| ConfigResolution {
                    key: definition.key.clone(),
                    value,
                    source: ConfigValueSource::Default,
                    definition: definition.clone(),
                })
        })
        .ok_or_else(|| ConfigError::MissingValue {
            key: definition.key.to_string(),
        })
}

fn write_config(
    data_dir: &SinomoDataDir,
    cluster: Option<&dyn ClusterAccess>,
    definition: &ConfigDefinition,
    value: Value,
    target: ConfigWriteTarget,
) -> Result<ConfigWriteResult, ConfigError> {
    let target = match target {
        ConfigWriteTarget::Auto => {
            default_write_target_for(definition.cluster_mode, cluster.is_some())
        }
        other => other,
    };
    match target {
        ConfigWriteTarget::Local => {
            if definition.cluster_mode == ConfigClusterMode::ClusterOnly {
                return Err(ConfigError::WriteTargetUnavailable {
                    key: definition.key.to_string(),
                    target,
                    detail: "config is cluster-only".to_string(),
                });
            }
            write_local_config(data_dir, definition, value.clone())?;
            Ok(ConfigWriteResult {
                key: definition.key.clone(),
                value,
                source: ConfigValueSource::Local,
            })
        }
        ConfigWriteTarget::Cluster => {
            if definition.cluster_mode == ConfigClusterMode::LocalOnly {
                return Err(ConfigError::WriteTargetUnavailable {
                    key: definition.key.to_string(),
                    target,
                    detail: "config is local-only".to_string(),
                });
            }
            let _ = cluster.ok_or_else(|| ConfigError::ClusterUnavailable {
                key: definition.key.to_string(),
            })?;
            return Err(ConfigError::WriteTargetUnavailable {
                key: definition.key.to_string(),
                target,
                detail: "cluster config writes require the async config service path".to_string(),
            });
        }
        ConfigWriteTarget::Auto => unreachable!("auto target resolved above"),
    }
}

async fn write_config_async(
    data_dir: &SinomoDataDir,
    cluster: Option<&dyn ClusterAccess>,
    definition: &ConfigDefinition,
    value: Value,
    target: ConfigWriteTarget,
) -> Result<ConfigWriteResult, ConfigError> {
    let target = match target {
        ConfigWriteTarget::Auto => {
            default_write_target_for(definition.cluster_mode, cluster.is_some())
        }
        other => other,
    };
    match target {
        ConfigWriteTarget::Local => {
            if definition.cluster_mode == ConfigClusterMode::ClusterOnly {
                return Err(ConfigError::WriteTargetUnavailable {
                    key: definition.key.to_string(),
                    target,
                    detail: "config is cluster-only".to_string(),
                });
            }
            write_local_config(data_dir, definition, value.clone())?;
            Ok(ConfigWriteResult {
                key: definition.key.clone(),
                value,
                source: ConfigValueSource::Local,
            })
        }
        ConfigWriteTarget::Cluster => {
            if definition.cluster_mode == ConfigClusterMode::LocalOnly {
                return Err(ConfigError::WriteTargetUnavailable {
                    key: definition.key.to_string(),
                    target,
                    detail: "config is local-only".to_string(),
                });
            }
            write_cluster_config_async(cluster, definition, value.clone()).await?;
            Ok(ConfigWriteResult {
                key: definition.key.clone(),
                value,
                source: ConfigValueSource::Cluster,
            })
        }
        ConfigWriteTarget::Auto => unreachable!("auto target resolved above"),
    }
}

fn default_write_target_for(mode: ConfigClusterMode, cluster_available: bool) -> ConfigWriteTarget {
    match mode {
        ConfigClusterMode::LocalOnly | ConfigClusterMode::LocalOverrideCluster => {
            ConfigWriteTarget::Local
        }
        ConfigClusterMode::ClusterOnly => ConfigWriteTarget::Cluster,
        ConfigClusterMode::ClusterOverrideLocal | ConfigClusterMode::Synced => {
            if cluster_available {
                ConfigWriteTarget::Cluster
            } else {
                ConfigWriteTarget::Local
            }
        }
    }
}

fn read_local_config(
    data_dir: &SinomoDataDir,
    definition: &ConfigDefinition,
) -> Result<Option<ConfigResolution>, ConfigError> {
    let path = definition.local_path(data_dir);
    if !path.exists() {
        return Ok(None);
    }
    let value = read_config_json_document_or_value(&path, definition)?;
    Ok(Some(ConfigResolution {
        key: definition.key.clone(),
        value,
        source: ConfigValueSource::Local,
        definition: definition.clone(),
    }))
}

fn read_or_migrate_local_config(
    data_dir: &SinomoDataDir,
    definition: &ConfigDefinition,
) -> Result<Option<ConfigResolution>, ConfigError> {
    if let Some(resolution) = read_local_config(data_dir, definition)? {
        return Ok(Some(resolution));
    }
    for legacy in &definition.local_legacy_paths {
        let path = data_dir.root().join(legacy);
        if !path.exists() {
            continue;
        }
        let value = read_config_json_document_or_value(&path, definition)?;
        write_local_config(data_dir, definition, value.clone())?;
        return Ok(Some(ConfigResolution {
            key: definition.key.clone(),
            value,
            source: ConfigValueSource::MigratedLocal,
            definition: definition.clone(),
        }));
    }
    Ok(None)
}

fn write_local_config(
    data_dir: &SinomoDataDir,
    definition: &ConfigDefinition,
    value: Value,
) -> Result<PathBuf, ConfigError> {
    let path = definition.local_path(data_dir);
    let document = ConfigDocument::new(definition.key.clone(), value);
    write_json_atomic(&path, &document)?;
    Ok(path)
}

fn read_cluster_config(
    cluster: Option<&dyn ClusterAccess>,
    definition: &ConfigDefinition,
) -> Result<Option<ConfigResolution>, ConfigError> {
    let Some(cluster) = cluster else {
        return Ok(None);
    };
    let key = definition.cluster_key();
    if let Some(entry) = cluster.kv_get(&key).map_err(ConfigError::Service)? {
        let value = decode_cluster_config_entry(&entry, definition)?;
        return Ok(Some(ConfigResolution {
            key: definition.key.clone(),
            value,
            source: ConfigValueSource::Cluster,
            definition: definition.clone(),
        }));
    }
    for legacy in &definition.cluster_legacy_keys {
        if let Some(entry) = cluster.kv_get(legacy).map_err(ConfigError::Service)? {
            let value = decode_cluster_config_entry(&entry, definition)?;
            return Ok(Some(ConfigResolution {
                key: definition.key.clone(),
                value,
                source: ConfigValueSource::LegacyCluster,
                definition: definition.clone(),
            }));
        }
    }
    Ok(None)
}

async fn write_cluster_config_async(
    cluster: Option<&dyn ClusterAccess>,
    definition: &ConfigDefinition,
    value: Value,
) -> Result<(), ConfigError> {
    let cluster = cluster.ok_or_else(|| ConfigError::ClusterUnavailable {
        key: definition.key.to_string(),
    })?;
    let op = cluster_config_put_op(definition, value)?;
    cluster
        .kv_commit_async(KvTxn {
            compares: Vec::new(),
            success: vec![op],
            failure: Vec::new(),
        })
        .await
        .map_err(ConfigError::Service)?;
    Ok(())
}

fn read_config_json_document_or_value(
    path: &Path,
    definition: &ConfigDefinition,
) -> Result<Value, ConfigError> {
    let bytes = fs::read(path).map_err(|source| ConfigError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    let value: Value = serde_json::from_slice(&bytes).map_err(|source| ConfigError::Serde {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    let value = match serde_json::from_value::<ConfigDocument>(value.clone()) {
        Ok(document) if document.key == definition.key => document.value,
        _ => value,
    };
    definition.validate_value(&value)?;
    Ok(value)
}

fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> Result<(), ConfigError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| ConfigError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let tmp_path = path.with_extension("json.tmp");
    {
        let mut file = fs::File::create(&tmp_path).map_err(|source| ConfigError::Io {
            path: tmp_path.display().to_string(),
            detail: source.to_string(),
        })?;
        serde_json::to_writer_pretty(&mut file, value).map_err(|source| ConfigError::Serde {
            path: tmp_path.display().to_string(),
            detail: source.to_string(),
        })?;
        file.write_all(b"\n").map_err(|source| ConfigError::Io {
            path: tmp_path.display().to_string(),
            detail: source.to_string(),
        })?;
        file.sync_all().map_err(|source| ConfigError::Io {
            path: tmp_path.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    fs::rename(&tmp_path, path).map_err(|source| ConfigError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

fn validate_config_key(value: &str) -> Result<(), ConfigError> {
    if value.is_empty() {
        return Err(ConfigError::InvalidKey {
            key: value.to_string(),
            detail: "key must not be empty".to_string(),
        });
    }
    if value.starts_with('/') || value.ends_with('/') || value.contains("..") {
        return Err(ConfigError::InvalidKey {
            key: value.to_string(),
            detail: "key must be a relative dotted or slash-separated path".to_string(),
        });
    }
    for segment in value.split(['.', '/']) {
        if segment.is_empty()
            || !segment
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
        {
            return Err(ConfigError::InvalidKey {
                key: value.to_string(),
                detail: format!("invalid key segment `{segment}`"),
            });
        }
    }
    Ok(())
}

fn key_storage_path(key: &str) -> String {
    format!("{}.json", key.replace('.', "/"))
}

fn validate_value_type(
    key: &ConfigKey,
    value_type: &ConfigValueType,
    value: &Value,
) -> Result<(), ConfigError> {
    match value_type {
        ConfigValueType::Bool if value.is_boolean() => Ok(()),
        ConfigValueType::I64 { range } => {
            let Some(number) = value.as_i64() else {
                return invalid_type(key, "i64");
            };
            validate_number_range(key, number as f64, range.as_ref())
        }
        ConfigValueType::U64 { range } => {
            let Some(number) = value.as_u64() else {
                return invalid_type(key, "u64");
            };
            validate_number_range(key, number as f64, range.as_ref())
        }
        ConfigValueType::F64 { range } => {
            let Some(number) = value.as_f64() else {
                return invalid_type(key, "f64");
            };
            validate_number_range(key, number, range.as_ref())
        }
        ConfigValueType::String { choices } => {
            let Some(text) = value.as_str() else {
                return invalid_type(key, "string");
            };
            if !choices.is_empty() && !choices.iter().any(|choice| choice == text) {
                return Err(ConfigError::InvalidValue {
                    key: key.to_string(),
                    detail: format!("value `{text}` is not one of {}", choices.join(", ")),
                });
            }
            Ok(())
        }
        ConfigValueType::StringList => {
            let Some(values) = value.as_array() else {
                return invalid_type(key, "string list");
            };
            if values.iter().all(Value::is_string) {
                Ok(())
            } else {
                invalid_type(key, "string list")
            }
        }
        ConfigValueType::Object { .. } if value.is_object() => Ok(()),
        ConfigValueType::Json => Ok(()),
        _ => invalid_type(key, "matching config value type"),
    }
}

fn validate_number_range(
    key: &ConfigKey,
    value: f64,
    range: Option<&ConfigNumberRange>,
) -> Result<(), ConfigError> {
    let Some(range) = range else {
        return Ok(());
    };
    if let Some(min) = range.min
        && value < min
    {
        return Err(ConfigError::InvalidValue {
            key: key.to_string(),
            detail: format!("value {value} is below minimum {min}"),
        });
    }
    if let Some(max) = range.max
        && value > max
    {
        return Err(ConfigError::InvalidValue {
            key: key.to_string(),
            detail: format!("value {value} is above maximum {max}"),
        });
    }
    Ok(())
}

fn invalid_type<T>(key: &ConfigKey, expected: &str) -> Result<T, ConfigError> {
    Err(ConfigError::InvalidValue {
        key: key.to_string(),
        detail: format!("expected {expected}"),
    })
}

fn config_timestamp_now() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "unknown".to_string())
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("invalid config key `{key}`: {detail}")]
    InvalidKey { key: String, detail: String },
    #[error("duplicate config key `{key}`")]
    DuplicateKey { key: String },
    #[error("unknown config key `{key}`")]
    UnknownKey { key: String },
    #[error("missing config value for `{key}` and no default is registered")]
    MissingValue { key: String },
    #[error("invalid config value for `{key}`: {detail}")]
    InvalidValue { key: String, detail: String },
    #[error("cluster config unavailable for `{key}`")]
    ClusterUnavailable { key: String },
    #[error("config write target {target:?} unavailable for `{key}`: {detail}")]
    WriteTargetUnavailable {
        key: String,
        target: ConfigWriteTarget,
        detail: String,
    },
    #[error("config I/O error at {path}: {detail}")]
    Io { path: String, detail: String },
    #[error("config serialization error at {path}: {detail}")]
    Serde { path: String, detail: String },
    #[error(transparent)]
    Kv(#[from] sinomo_kv::KvError),
    #[error(transparent)]
    Service(#[from] ServiceError),
    #[error("config client/transport error: {detail}")]
    Client { detail: String },
}

impl From<ConfigError> for ServiceError {
    fn from(error: ConfigError) -> Self {
        match error {
            ConfigError::Service(error) => error,
            other => ServiceError::Service(other.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_kv::{KvEntry, KvRevision, KvVersion};
    use sinomo_service::ResponseSink;
    use std::sync::Mutex;

    fn bool_definition() -> ConfigDefinition {
        ConfigDefinition::new(
            "test.enabled",
            "test",
            ConfigValueType::Bool,
            "test switch",
            ConfigClusterMode::ClusterOverrideLocal,
        )
        .expect("definition")
        .with_default(&false)
        .expect("default")
        .with_local_legacy_path("config/test-enabled.json")
    }

    fn registry() -> ConfigRegistry {
        ConfigRegistry::builder()
            .register(bool_definition())
            .expect("register")
            .build()
    }

    #[test]
    fn local_configs_are_written_as_human_readable_documents_under_configs_dir() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-config-local-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let data_dir = SinomoDataDir::new(&root);
        let store = ConfigStore::new(registry());

        store
            .set(
                &data_dir,
                None,
                "test.enabled",
                Value::Bool(true),
                ConfigWriteTarget::Local,
            )
            .expect("write");

        let path = root.join("configs").join("test").join("enabled.json");
        let contents = fs::read_to_string(path).expect("read config json");
        assert!(contents.contains("\"key\": \"test.enabled\""));
        assert!(contents.contains("\"value\": true"));
        let resolved = store
            .get(&data_dir, None, "test.enabled")
            .expect("resolve local");
        assert_eq!(resolved.value, Value::Bool(true));
        assert_eq!(resolved.source, ConfigValueSource::Local);
    }

    #[test]
    fn legacy_local_json_is_migrated_on_read() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-config-migrate-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        fs::create_dir_all(root.join("config")).expect("legacy dir");
        fs::write(root.join("config").join("test-enabled.json"), "true\n").expect("legacy");
        let data_dir = SinomoDataDir::new(&root);
        let store = ConfigStore::new(registry());

        let resolved = store.get(&data_dir, None, "test.enabled").expect("migrate");
        assert_eq!(resolved.value, Value::Bool(true));
        assert_eq!(resolved.source, ConfigValueSource::MigratedLocal);
        assert!(
            root.join("configs")
                .join("test")
                .join("enabled.json")
                .exists()
        );
    }

    #[derive(Default)]
    struct InMemoryCluster {
        entries: Mutex<BTreeMap<String, Vec<u8>>>,
    }

    impl ClusterAccess for InMemoryCluster {
        fn kv_get(&self, key: &str) -> Result<Option<KvEntry>, ServiceError> {
            Ok(self
                .entries
                .lock()
                .expect("cluster lock")
                .get(key)
                .cloned()
                .map(|value| KvEntry {
                    key: key.to_string(),
                    value,
                    create_revision: KvRevision(1),
                    modify_revision: KvRevision(1),
                    version: KvVersion(1),
                    lease_id: None,
                }))
        }

        fn kv_range_prefix(&self, _prefix: &str) -> Result<Vec<KvEntry>, ServiceError> {
            Ok(Vec::new())
        }

        fn kv_commit_async<'a>(
            &'a self,
            txn: KvTxn,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = Result<sinomo_kv::KvTxnResponse, ServiceError>>
                    + Send
                    + 'a,
            >,
        > {
            Box::pin(async move {
                let mut entries = self.entries.lock().expect("cluster lock");
                for op in txn.success {
                    if let KvOp::Put { key, value, .. } = op {
                        entries.insert(key, value);
                    }
                }
                Ok(sinomo_kv::KvTxnResponse {
                    succeeded: true,
                    revision: KvRevision(1),
                    responses: Vec::new(),
                })
            })
        }
    }

    #[tokio::test]
    async fn cluster_override_local_prefers_cluster_when_available() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-config-cluster-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let data_dir = SinomoDataDir::new(&root);
        let store = ConfigStore::new(registry());
        store
            .set(
                &data_dir,
                None,
                "test.enabled",
                Value::Bool(false),
                ConfigWriteTarget::Local,
            )
            .expect("local");
        let cluster = InMemoryCluster::default();
        store
            .set_async(
                &data_dir,
                Some(&cluster),
                "test.enabled",
                Value::Bool(true),
                ConfigWriteTarget::Cluster,
            )
            .await
            .expect("cluster");

        let resolved = store
            .get(&data_dir, Some(&cluster), "test.enabled")
            .expect("resolve");
        assert_eq!(resolved.value, Value::Bool(true));
        assert_eq!(resolved.source, ConfigValueSource::Cluster);
    }

    #[derive(Default)]
    struct VecSink {
        frames: Arc<Mutex<Vec<ObjectRpcResponse>>>,
    }

    impl ResponseSink for VecSink {
        fn emit(&mut self, response: ObjectRpcResponse) -> Result<(), ServiceError> {
            self.frames.lock().expect("sink lock").push(response);
            Ok(())
        }
    }

    #[tokio::test]
    async fn config_service_searches_and_sets_local_values() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-config-service-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let sink = VecSink::default();
        let frames = Arc::clone(&sink.frames);
        let ctx = ServiceContext {
            data_dir: Arc::new(SinomoDataDir::new(&root)),
            cluster: None,
            signer: sinomo_service::ServiceSigner::Local,
            sink: Box::new(sink),
        };
        let registry =
            register_services(sinomo_service::ServiceRegistry::builder(), registry()).build();
        let request = ObjectRpcRequest::encode_body(
            CONFIG_SEARCH_ROUTE,
            CONFIG_RPC_CATEGORY,
            &ConfigSearchRequest {
                query: "enabled".to_string(),
            },
        )
        .expect("encode");

        registry.dispatch(ctx, request).await.expect("dispatch");
        let response = frames
            .lock()
            .expect("frames")
            .pop()
            .expect("response frame");
        let response: ConfigSearchResponse = response.decode_body().expect("decode");
        assert_eq!(response.definitions.len(), 1);
    }
}
