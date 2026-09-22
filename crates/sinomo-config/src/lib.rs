//! Shared configuration registry, resolution, storage, and management service.
//!
//! Owner crates declare config definitions. The config layer resolves whether a value
//! is local, cluster-backed, synchronized, or defaulted, and hides the storage details
//! from callers. SOBJ is the native module, object-RPC, and cluster-KV format.
//! Local values are projected as human-readable JSON documents under
//! `$SINOMO_DATA_DIR/configs/`; the JSON-shaped [`serde_json::Value`] tree is an
//! interface representation, not a second distributed wire contract. Cluster
//! values are typed Sinomo-object values in `sinomo-kv` under `/config/...`.

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sinomo_core::kv::{KvEntry, KvOp, KvRequestId, KvTxn, KvTxnRequest};
use sinomo_core::local::SinomoDataDir;
use sinomo_core::network::ObjectRpcRequest;
use sinomo_core::service::{
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
pub struct ConfigSchema {
    pub name: String,
    pub description: String,
    pub value: ConfigSchemaValue,
}

impl ConfigSchema {
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        value: ConfigSchemaValue,
    ) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            value,
        }
    }

    pub fn object(
        name: impl Into<String>,
        description: impl Into<String>,
        fields: Vec<ConfigSchemaField>,
    ) -> Self {
        Self::new(
            name,
            description,
            ConfigSchemaValue::object(fields).without_unknown_fields(),
        )
    }

    pub fn validate(&self, key: &ConfigKey, value: &Value) -> Result<(), ConfigError> {
        validate_schema_value(key, &self.value, value, "$")
    }

    /// Validate the schema declaration itself, independently from any one
    /// configured value. Schemas are part of the SOBJ config contract, so an
    /// invalid declaration must be rejected when the registry is assembled
    /// rather than discovered by a settings client later.
    pub fn validate_declaration(&self, key: &ConfigKey) -> Result<(), ConfigError> {
        if self.name.trim().is_empty() {
            return invalid_schema_declaration(key, "$", "schema name cannot be empty");
        }
        validate_schema_value_declaration(key, &self.value, "$")
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ConfigSchemaField {
    pub name: String,
    pub label: String,
    pub description: String,
    pub value: ConfigSchemaValue,
    #[serde(default = "default_schema_field_required")]
    pub required: bool,
    /// Field-level default used by schema clients when materializing new
    /// objects or collection entries. The owning typed config remains the
    /// authority for applying defaults at read time; use `optional()` as well
    /// when its deserializer accepts omission.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_value: Option<Value>,
}

impl ConfigSchemaField {
    pub fn new(
        name: impl Into<String>,
        label: impl Into<String>,
        description: impl Into<String>,
        value: ConfigSchemaValue,
    ) -> Self {
        Self {
            name: name.into(),
            label: label.into(),
            description: description.into(),
            value,
            required: true,
            default_value: None,
        }
    }

    pub fn optional(mut self) -> Self {
        self.required = false;
        self
    }

    pub fn with_default_value(mut self, value: Value) -> Self {
        self.default_value = Some(value);
        self
    }
}

fn default_schema_field_required() -> bool {
    true
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum ConfigSchemaValue {
    Bool,
    I64 {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        range: Option<ConfigNumberRange>,
    },
    U64 {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        range: Option<ConfigNumberRange>,
    },
    F64 {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        range: Option<ConfigNumberRange>,
    },
    String {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        choices: Vec<ConfigSchemaChoice>,
        #[serde(default, skip_serializing_if = "is_false")]
        secret: bool,
    },
    StringList,
    Object {
        fields: Vec<ConfigSchemaField>,
        #[serde(default = "default_allow_unknown_fields")]
        allow_unknown_fields: bool,
    },
    Map {
        value: Box<ConfigSchemaValue>,
    },
    List {
        value: Box<ConfigSchemaValue>,
    },
    Json,
}

impl ConfigSchemaValue {
    pub fn bool() -> Self {
        Self::Bool
    }

    pub fn i64(range: Option<ConfigNumberRange>) -> Self {
        Self::I64 { range }
    }

    pub fn u64(range: Option<ConfigNumberRange>) -> Self {
        Self::U64 { range }
    }

    pub fn f64(range: Option<ConfigNumberRange>) -> Self {
        Self::F64 { range }
    }

    pub fn string() -> Self {
        Self::String {
            choices: Vec::new(),
            secret: false,
        }
    }

    pub fn secret_string() -> Self {
        Self::String {
            choices: Vec::new(),
            secret: true,
        }
    }

    pub fn string_choices(choices: Vec<ConfigSchemaChoice>) -> Self {
        Self::String {
            choices,
            secret: false,
        }
    }

    pub fn object(fields: Vec<ConfigSchemaField>) -> Self {
        Self::Object {
            fields,
            allow_unknown_fields: true,
        }
    }

    pub fn map(value: ConfigSchemaValue) -> Self {
        Self::Map {
            value: Box::new(value),
        }
    }

    pub fn list(value: ConfigSchemaValue) -> Self {
        Self::List {
            value: Box::new(value),
        }
    }

    pub fn without_unknown_fields(mut self) -> Self {
        if let Self::Object {
            allow_unknown_fields,
            ..
        } = &mut self
        {
            *allow_unknown_fields = false;
        }
        self
    }
}

fn default_allow_unknown_fields() -> bool {
    true
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn legacy_remote_write_requires_host_permission() -> bool {
    // Older external modules did not classify host effects. Preserve their
    // previous remote-management requirement until they declare a policy.
    true
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConfigSchemaChoice {
    pub value: String,
    pub label: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
}

impl ConfigSchemaChoice {
    pub fn new(value: impl Into<String>, label: impl Into<String>) -> Self {
        Self {
            value: value.into(),
            label: label.into(),
            description: String::new(),
        }
    }

    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = description.into();
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ConfigDefinition {
    pub key: ConfigKey,
    pub owner: String,
    /// Module lifecycle identity that owns this user-facing setting. This is
    /// explicit catalog metadata; clients must not infer it from crate names.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub module_id: Option<String>,
    /// Module-provided user-facing identity for generic settings catalogs.
    /// Clients must treat this as presentation metadata, never authority.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub module_label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub module_description: Option<String>,
    /// Module-declared paths for generic actionable-error presentation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operational_error: Option<ConfigOperationalErrorPresentation>,
    pub value_type: ConfigValueType,
    pub description: String,
    pub cluster_mode: ConfigClusterMode,
    /// Remote writes that configure host-system effects require the target
    /// node's local remote-management consent in addition to config authority.
    #[serde(default = "legacy_remote_write_requires_host_permission")]
    pub remote_write_requires_host_permission: bool,
    /// Operational documents can share the config transport without being
    /// presented as editable user settings. Writes by their owning service are
    /// still permitted; this flag is UI/catalog intent, not an authority grant.
    #[serde(default, skip_serializing_if = "is_false")]
    pub read_only: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<ConfigSchema>,
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
            module_id: None,
            module_label: None,
            module_description: None,
            operational_error: None,
            value_type,
            description: description.into(),
            cluster_mode,
            remote_write_requires_host_permission: false,
            read_only: false,
            schema: None,
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

    pub fn with_remote_host_management(mut self) -> Self {
        self.remote_write_requires_host_permission = true;
        self
    }

    pub fn with_cluster_legacy_key(mut self, key: impl Into<String>) -> Self {
        self.cluster_legacy_keys.push(key.into());
        self
    }

    pub fn with_schema(mut self, schema: ConfigSchema) -> Result<Self, ConfigError> {
        schema.validate_declaration(&self.key)?;
        validate_schema_root_type(&self.key, &self.value_type, &schema)?;
        if let Some(default_value) = &self.default_value {
            schema.validate(&self.key, default_value)?;
        }
        self.schema = Some(schema);
        if self.module_id.is_some() {
            self.validate_contract()?;
        }
        Ok(self)
    }

    pub fn with_module_id(mut self, module_id: impl Into<String>) -> Result<Self, ConfigError> {
        let module_id = module_id.into();
        if module_id.trim().is_empty() {
            return Err(ConfigError::InvalidValue {
                key: self.key.to_string(),
                detail: "module id cannot be empty".to_string(),
            });
        }
        let module_id = sinomo_core::Id::new(module_id.clone())
            .or_else(|_| sinomo_core::Id::from_legacy_named(module_id))
            .map_err(|error| ConfigError::InvalidValue {
                key: self.key.to_string(),
                detail: format!("invalid module id: {error}"),
            })?;
        self.module_id = Some(module_id.to_string());
        if self.schema.is_some() {
            self.validate_contract()?;
        }
        Ok(self)
    }

    pub fn with_module_presentation(
        mut self,
        label: impl Into<String>,
        description: impl Into<String>,
    ) -> Result<Self, ConfigError> {
        let label = label.into();
        let description = description.into();
        if self.module_id.is_none() {
            return Err(ConfigError::InvalidValue {
                key: self.key.to_string(),
                detail: "module presentation requires a module id".to_string(),
            });
        }
        if label.trim().is_empty() || label.trim() != label {
            return Err(ConfigError::InvalidValue {
                key: self.key.to_string(),
                detail: "module label must be non-empty without surrounding whitespace".to_string(),
            });
        }
        if description.trim().is_empty() || description.trim() != description {
            return Err(ConfigError::InvalidValue {
                key: self.key.to_string(),
                detail: "module description must be non-empty without surrounding whitespace"
                    .to_string(),
            });
        }
        self.module_label = Some(label);
        self.module_description = Some(description);
        self.validate_contract()?;
        Ok(self)
    }

    pub fn with_operational_error_presentation(
        mut self,
        error_field: impl Into<String>,
        resolution_field: impl Into<String>,
    ) -> Result<Self, ConfigError> {
        self.operational_error = Some(ConfigOperationalErrorPresentation {
            error_field: error_field.into(),
            resolution_field: resolution_field.into(),
        });
        self.validate_contract()?;
        Ok(self)
    }

    pub fn read_only(mut self) -> Self {
        self.read_only = true;
        self
    }

    pub fn cluster_key(&self) -> String {
        self.key.cluster_key()
    }

    pub fn local_path(&self, data_dir: &SinomoDataDir) -> PathBuf {
        configs_dir(data_dir).join(self.key.local_relative_path())
    }

    pub fn validate_value(&self, value: &Value) -> Result<(), ConfigError> {
        validate_value_type(&self.key, &self.value_type, value)?;
        if let Some(schema) = &self.schema {
            schema.validate(&self.key, value)?;
        }
        Ok(())
    }

    pub fn validate_contract(&self) -> Result<(), ConfigError> {
        if self.owner.trim().is_empty() {
            return Err(ConfigError::InvalidValue {
                key: self.key.to_string(),
                detail: "config owner cannot be empty".to_string(),
            });
        }
        if let Some(module_id) = &self.module_id {
            sinomo_core::Id::new(module_id.clone()).map_err(|error| ConfigError::InvalidValue {
                key: self.key.to_string(),
                detail: format!("module id must use its canonical value: {error}"),
            })?;
        }
        if self.module_id.is_some() && self.schema.is_none() {
            return Err(ConfigError::InvalidValue {
                key: self.key.to_string(),
                detail: "module-owned settings require a detailed schema".to_string(),
            });
        }
        if self.module_id.is_none()
            && (self.module_label.is_some() || self.module_description.is_some())
        {
            return Err(ConfigError::InvalidValue {
                key: self.key.to_string(),
                detail: "module presentation requires a module id".to_string(),
            });
        }
        if self.module_label.is_some() != self.module_description.is_some() {
            return Err(ConfigError::InvalidValue {
                key: self.key.to_string(),
                detail: "module presentation requires both label and description".to_string(),
            });
        }
        if let Some(label) = &self.module_label
            && (label.trim().is_empty() || label.trim() != label)
        {
            return Err(ConfigError::InvalidValue {
                key: self.key.to_string(),
                detail: "module label must be non-empty without surrounding whitespace".to_string(),
            });
        }
        if let Some(description) = &self.module_description
            && (description.trim().is_empty() || description.trim() != description)
        {
            return Err(ConfigError::InvalidValue {
                key: self.key.to_string(),
                detail: "module description must be non-empty without surrounding whitespace"
                    .to_string(),
            });
        }
        if let Some(presentation) = &self.operational_error {
            if !self.read_only {
                return Err(ConfigError::InvalidValue {
                    key: self.key.to_string(),
                    detail: "operational error presentation requires a read-only definition"
                        .to_string(),
                });
            }
            if presentation.error_field.trim().is_empty()
                || presentation.resolution_field.trim().is_empty()
            {
                return Err(ConfigError::InvalidValue {
                    key: self.key.to_string(),
                    detail: "operational error and resolution fields must be non-empty".to_string(),
                });
            }
            let fields = self.schema.as_ref().and_then(|schema| match &schema.value {
                ConfigSchemaValue::Object { fields, .. } => Some(fields),
                _ => None,
            });
            for field in [&presentation.error_field, &presentation.resolution_field] {
                if fields
                    .is_none_or(|fields| !fields.iter().any(|candidate| &candidate.name == field))
                {
                    return Err(ConfigError::InvalidValue {
                        key: self.key.to_string(),
                        detail: format!(
                            "operational presentation field `{field}` is absent from the root schema"
                        ),
                    });
                }
            }
        }
        if let Some(schema) = &self.schema {
            schema.validate_declaration(&self.key)?;
            validate_schema_root_type(&self.key, &self.value_type, schema)?;
            if self.module_id.is_some() {
                if schema.description.trim().is_empty() {
                    return invalid_schema_declaration(
                        &self.key,
                        "$",
                        "module settings schema description cannot be empty",
                    );
                }
                validate_module_schema_value(&self.key, &schema.value, "$")?;
            }
        }
        if let Some(default_value) = &self.default_value {
            self.validate_value(default_value)?;
            if let Some(schema) = &self.schema {
                validate_no_secret_defaults(&self.key, &schema.value, default_value, "$")?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ConfigOperationalErrorPresentation {
    pub error_field: String,
    pub resolution_field: String,
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
    /// Returns a definition already staged in this builder without consuming it.
    ///
    /// Hosts that compose a static catalog with module-declared settings use
    /// this to treat an identical declaration as idempotent while continuing
    /// to reject or retain authority over conflicting declarations.
    pub fn get(&self, key: &str) -> Result<&ConfigDefinition, ConfigError> {
        let key = ConfigKey::new(key.to_string())?;
        self.definitions
            .get(&key)
            .ok_or_else(|| ConfigError::UnknownKey {
                key: key.to_string(),
            })
    }

    pub fn register(mut self, definition: ConfigDefinition) -> Result<Self, ConfigError> {
        definition.validate_contract()?;
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

/// Logical configuration document. It is SOBJ-encoded on RPC/KV paths and
/// JSON-encoded only by the human-facing local storage adapter.
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

    /// Resolve a key while allowing cluster-backed modes to establish an
    /// authoritative quorum barrier. Synchronous callers remain available for
    /// local-only resolution, but may not silently substitute a local KV view
    /// for cluster authority.
    pub async fn get_async(
        &self,
        data_dir: &SinomoDataDir,
        cluster: Option<&dyn ClusterAccess>,
        key: &str,
    ) -> Result<ConfigResolution, ConfigError> {
        let definition = self.registry.get(key)?.clone();
        resolve_config_async(data_dir, cluster, &definition).await
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
        self.set_async_with_request_id(data_dir, cluster, key, value, target, None)
            .await
    }

    /// Writes configuration while preserving a caller-supplied cluster KV
    /// request identity for reconciliation after an uncertain response.
    pub async fn set_async_with_request_id(
        &self,
        data_dir: &SinomoDataDir,
        cluster: Option<&dyn ClusterAccess>,
        key: &str,
        value: Value,
        target: ConfigWriteTarget,
        request_id: Option<KvRequestId>,
    ) -> Result<ConfigWriteResult, ConfigError> {
        let definition = self.registry.get(key)?.clone();
        definition.validate_value(&value)?;
        write_config_async(data_dir, cluster, &definition, value, target, request_id).await
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
    /// Stable identity to reuse when reconciling or retrying this exact cluster
    /// write. Local-target writes ignore it.
    pub request_id: Option<KvRequestId>,
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
            ServiceRoute::new(CONFIG_CATALOG_ROUTE, CONFIG_RPC_CATEGORY).with_auth_scope("admin"),
            ServiceRoute::new(CONFIG_SEARCH_ROUTE, CONFIG_RPC_CATEGORY).with_auth_scope("admin"),
            ServiceRoute::new(CONFIG_GET_ROUTE, CONFIG_RPC_CATEGORY)
                .with_auth_scope("admin")
                .with_concurrency_limit(8),
            ServiceRoute::new(CONFIG_SET_ROUTE, CONFIG_RPC_CATEGORY)
                .with_auth_scope("admin")
                .with_concurrency_limit(8),
        ]
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let store = Arc::clone(&self.store);
        Box::pin(async move {
            // Incremental request decoding mutably consumes the streamed
            // request value. Keep an owned handle instead of borrowing it
            // through `ctx` across that decode.
            let cluster = ctx.cluster.clone();
            match request.route.as_str() {
                CONFIG_CATALOG_ROUTE => {
                    let request: ConfigCatalogRequest = ctx.decode_request_body(&request).await?;
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
                    ctx.respond_value(CONFIG_CATALOG_ROUTE, ConfigCatalogResponse { definitions })?;
                }
                CONFIG_SEARCH_ROUTE => {
                    let request: ConfigSearchRequest = ctx.decode_request_body(&request).await?;
                    let definitions = store.registry().search(&request.query);
                    ctx.respond_value(CONFIG_SEARCH_ROUTE, ConfigSearchResponse { definitions })?;
                }
                CONFIG_GET_ROUTE => {
                    let request: ConfigGetRequest = ctx.decode_request_body(&request).await?;
                    let resolution = store
                        .get_async(&ctx.data_dir, cluster.as_deref(), &request.key)
                        .await?;
                    ctx.respond_value(CONFIG_GET_ROUTE, ConfigGetResponse { resolution })?;
                }
                CONFIG_SET_ROUTE => {
                    let request: ConfigSetRequest = ctx.decode_request_body(&request).await?;
                    let definition = store.registry().get(&request.key)?;
                    if !ctx.signer.is_local() && definition.remote_write_requires_host_permission {
                        let granted =
                            sinomo_core::node::LocalNodePermissionStore::new(ctx.data_dir.root())
                                .try_permission_granted(
                                    sinomo_core::node::LocalNodePermission::RemoteManagement,
                                )
                                .map_err(|error| {
                                    ServiceError::Service(format!(
                                        "host permission unavailable: {error}"
                                    ))
                                })?;
                        if !granted {
                            return Err(ServiceError::Service(format!(
                                "remote write to {} requires remote-management permission on the target node because it configures the host system",
                                request.key
                            )));
                        }
                    }
                    let result = store
                        .set_async_with_request_id(
                            &ctx.data_dir,
                            cluster.as_deref(),
                            &request.key,
                            request.value,
                            request.target,
                            request.request_id,
                        )
                        .await?;
                    ctx.respond_value(CONFIG_SET_ROUTE, ConfigSetResponse { result })?;
                }
                other => return Err(ServiceError::UnknownRoute(other.to_string())),
            }
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
    layout: &mut sinomo_core::storage::StorageLayout,
) -> Result<(), sinomo_core::storage::StorageContractError> {
    layout.register_escape_path(
        "sinomo-config",
        sinomo_core::storage::StorageRing::R1,
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
                reject_sync_cluster_config_read(cluster, definition)?
            }
        }
        ConfigClusterMode::ClusterOverrideLocal | ConfigClusterMode::Synced => {
            if let Some(cluster) = reject_sync_cluster_config_read(cluster, definition)? {
                Some(cluster)
            } else {
                read_or_migrate_local_config(data_dir, definition)?
            }
        }
        ConfigClusterMode::ClusterOnly => reject_sync_cluster_config_read(cluster, definition)?,
    };
    finish_config_resolution(resolved, definition)
}

async fn resolve_config_async(
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
                read_cluster_config_authoritative(cluster, definition).await?
            }
        }
        ConfigClusterMode::ClusterOverrideLocal | ConfigClusterMode::Synced => {
            if let Some(cluster) = read_cluster_config_authoritative(cluster, definition).await? {
                Some(cluster)
            } else {
                read_or_migrate_local_config(data_dir, definition)?
            }
        }
        ConfigClusterMode::ClusterOnly => {
            read_cluster_config_authoritative(cluster, definition).await?
        }
    };
    finish_config_resolution(resolved, definition)
}

fn finish_config_resolution(
    resolved: Option<ConfigResolution>,
    definition: &ConfigDefinition,
) -> Result<ConfigResolution, ConfigError> {
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

fn reject_sync_cluster_config_read(
    cluster: Option<&dyn ClusterAccess>,
    definition: &ConfigDefinition,
) -> Result<Option<ConfigResolution>, ConfigError> {
    if cluster.is_none() {
        return Ok(None);
    }
    Err(ConfigError::Service(ServiceError::Service(format!(
        "cluster-backed config `{}` requires authoritative async resolution",
        definition.key
    ))))
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
            Err(ConfigError::WriteTargetUnavailable {
                key: definition.key.to_string(),
                target,
                detail: "cluster config writes require the async config service path".to_string(),
            })
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
    request_id: Option<KvRequestId>,
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
            write_cluster_config_async(cluster, definition, value.clone(), request_id).await?;
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

async fn read_cluster_config_authoritative(
    cluster: Option<&dyn ClusterAccess>,
    definition: &ConfigDefinition,
) -> Result<Option<ConfigResolution>, ConfigError> {
    let Some(cluster) = cluster else {
        return Ok(None);
    };
    let key = definition.cluster_key();
    if let Some(entry) = cluster
        .kv_get_authoritative_async(&key)
        .await
        .map_err(ConfigError::Service)?
        .value
    {
        let value = decode_cluster_config_entry(&entry, definition)?;
        return Ok(Some(ConfigResolution {
            key: definition.key.clone(),
            value,
            source: ConfigValueSource::Cluster,
            definition: definition.clone(),
        }));
    }
    for legacy in &definition.cluster_legacy_keys {
        if let Some(entry) = cluster
            .kv_get_authoritative_async(legacy)
            .await
            .map_err(ConfigError::Service)?
            .value
        {
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
    request_id: Option<KvRequestId>,
) -> Result<(), ConfigError> {
    let cluster = cluster.ok_or_else(|| ConfigError::ClusterUnavailable {
        key: definition.key.to_string(),
    })?;
    let op = cluster_config_put_op(definition, value)?;
    let txn = KvTxn {
        compares: Vec::new(),
        success: vec![op],
        failure: Vec::new(),
    };
    let request = match request_id {
        Some(request_id) => KvTxnRequest::new(request_id, txn),
        None => KvTxnRequest::generated(txn),
    };
    let committed = cluster
        .kv_commit_compact_async(request)
        .await
        .map_err(ConfigError::Service)?;
    let response = committed.txn_response.ok_or_else(|| {
        ConfigError::Service(ServiceError::Service(format!(
            "cluster config transaction for `{}` committed without a transaction response",
            definition.key
        )))
    })?;
    if !response.succeeded {
        return Err(ConfigError::Service(ServiceError::Service(format!(
            "cluster config transaction for `{}` did not apply its success branch",
            definition.key
        ))));
    }
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

fn validate_schema_root_type(
    key: &ConfigKey,
    value_type: &ConfigValueType,
    schema: &ConfigSchema,
) -> Result<(), ConfigError> {
    let compatible = match (value_type, &schema.value) {
        (ConfigValueType::Bool, ConfigSchemaValue::Bool)
        | (ConfigValueType::I64 { .. }, ConfigSchemaValue::I64 { .. })
        | (ConfigValueType::U64 { .. }, ConfigSchemaValue::U64 { .. })
        | (ConfigValueType::F64 { .. }, ConfigSchemaValue::F64 { .. })
        | (ConfigValueType::String { .. }, ConfigSchemaValue::String { .. })
        | (ConfigValueType::StringList, ConfigSchemaValue::StringList)
        | (ConfigValueType::Object { .. }, ConfigSchemaValue::Object { .. })
        | (ConfigValueType::Object { .. }, ConfigSchemaValue::Map { .. })
        | (ConfigValueType::Json, _) => true,
        (ConfigValueType::StringList, ConfigSchemaValue::List { value }) => {
            matches!(value.as_ref(), ConfigSchemaValue::String { .. })
        }
        _ => false,
    };
    if compatible {
        if let ConfigValueType::Object { schema_name } = value_type
            && schema_name != &schema.name
        {
            return Err(ConfigError::InvalidValue {
                key: key.to_string(),
                detail: format!(
                    "object value type names schema `{schema_name}` but definition supplies `{}`",
                    schema.name
                ),
            });
        }
        match (value_type, &schema.value) {
            (
                ConfigValueType::I64 { range: value_range },
                ConfigSchemaValue::I64 {
                    range: schema_range,
                },
            )
            | (
                ConfigValueType::U64 { range: value_range },
                ConfigSchemaValue::U64 {
                    range: schema_range,
                },
            )
            | (
                ConfigValueType::F64 { range: value_range },
                ConfigSchemaValue::F64 {
                    range: schema_range,
                },
            ) => validate_schema_range_within_value_type(
                key,
                value_range.as_ref(),
                schema_range.as_ref(),
            )?,
            (
                ConfigValueType::String {
                    choices: value_choices,
                },
                ConfigSchemaValue::String {
                    choices: schema_choices,
                    ..
                },
            ) if !value_choices.is_empty()
                && (schema_choices.is_empty()
                    || schema_choices
                        .iter()
                        .any(|choice| !value_choices.contains(&choice.value))) =>
            {
                return Err(ConfigError::InvalidValue {
                    key: key.to_string(),
                    detail: "schema string choices must be a non-empty subset of the declared value-type choices"
                        .to_string(),
                });
            }
            _ => {}
        }
        Ok(())
    } else {
        Err(ConfigError::InvalidValue {
            key: key.to_string(),
            detail: format!("schema root kind does not match declared value type {value_type:?}"),
        })
    }
}

fn validate_schema_range_within_value_type(
    key: &ConfigKey,
    value_range: Option<&ConfigNumberRange>,
    schema_range: Option<&ConfigNumberRange>,
) -> Result<(), ConfigError> {
    let value_min = value_range.and_then(|range| range.min);
    let value_max = value_range.and_then(|range| range.max);
    let schema_min = schema_range.and_then(|range| range.min);
    let schema_max = schema_range.and_then(|range| range.max);
    let widens_min =
        value_min.is_some_and(|minimum| schema_min.is_none_or(|value| value < minimum));
    let widens_max =
        value_max.is_some_and(|maximum| schema_max.is_none_or(|value| value > maximum));
    if widens_min || widens_max {
        return Err(ConfigError::InvalidValue {
            key: key.to_string(),
            detail: "schema number range must stay within the declared value-type range"
                .to_string(),
        });
    }
    Ok(())
}

fn validate_schema_value_declaration(
    key: &ConfigKey,
    schema: &ConfigSchemaValue,
    path: &str,
) -> Result<(), ConfigError> {
    match schema {
        ConfigSchemaValue::I64 { range }
        | ConfigSchemaValue::U64 { range }
        | ConfigSchemaValue::F64 { range } => {
            validate_schema_range_declaration(key, path, range.as_ref())
        }
        ConfigSchemaValue::String { choices, secret } => {
            if *secret && !choices.is_empty() {
                return invalid_schema_declaration(
                    key,
                    path,
                    "secret strings cannot declare choices",
                );
            }
            let mut values = BTreeMap::<&str, ()>::new();
            let mut labels = BTreeMap::<&str, ()>::new();
            for choice in choices {
                if choice.value.trim().is_empty() {
                    return invalid_schema_declaration(key, path, "choice value cannot be empty");
                }
                if choice.label.trim().is_empty() {
                    return invalid_schema_declaration(
                        key,
                        path,
                        format!("choice `{}` label cannot be empty", choice.value),
                    );
                }
                if values.insert(choice.value.as_str(), ()).is_some() {
                    return invalid_schema_declaration(
                        key,
                        path,
                        format!("duplicate choice value `{}`", choice.value),
                    );
                }
                if labels.insert(choice.label.as_str(), ()).is_some() {
                    return invalid_schema_declaration(
                        key,
                        path,
                        format!("duplicate choice label `{}`", choice.label),
                    );
                }
            }
            Ok(())
        }
        ConfigSchemaValue::Object { fields, .. } => {
            let mut names = BTreeMap::<&str, ()>::new();
            for field in fields {
                let field_path = schema_child_path(path, &field.name);
                if field.name.trim().is_empty() {
                    return invalid_schema_declaration(key, path, "field name cannot be empty");
                }
                if field.label.trim().is_empty() {
                    return invalid_schema_declaration(
                        key,
                        &field_path,
                        "field label cannot be empty",
                    );
                }
                if names.insert(field.name.as_str(), ()).is_some() {
                    return invalid_schema_declaration(
                        key,
                        path,
                        format!("duplicate field `{}`", field.name),
                    );
                }
                validate_schema_value_declaration(key, &field.value, &field_path)?;
                if let Some(default_value) = &field.default_value {
                    if matches!(&field.value, ConfigSchemaValue::String { secret: true, .. }) {
                        return invalid_schema_declaration(
                            key,
                            &field_path,
                            "secret fields cannot publish a default value",
                        );
                    }
                    validate_schema_value(key, &field.value, default_value, &field_path)?;
                    validate_no_secret_defaults(key, &field.value, default_value, &field_path)?;
                }
            }
            Ok(())
        }
        ConfigSchemaValue::Map { value } => {
            validate_schema_value_declaration(key, value, &format!("{path}.*"))
        }
        ConfigSchemaValue::List { value } => {
            validate_schema_value_declaration(key, value, &format!("{path}[]"))
        }
        ConfigSchemaValue::Bool | ConfigSchemaValue::StringList | ConfigSchemaValue::Json => Ok(()),
    }
}

fn validate_no_secret_defaults(
    key: &ConfigKey,
    schema: &ConfigSchemaValue,
    value: &Value,
    path: &str,
) -> Result<(), ConfigError> {
    match schema {
        ConfigSchemaValue::String { secret: true, .. } => {
            if value.as_str().is_some_and(|value| !value.is_empty()) {
                return Err(ConfigError::InvalidValue {
                    key: key.to_string(),
                    detail: format!(
                        "{path}: config defaults cannot publish a secret value through the schema catalog"
                    ),
                });
            }
        }
        ConfigSchemaValue::Object { fields, .. } => {
            if let Some(values) = value.as_object() {
                for field in fields {
                    if let Some(value) = values.get(&field.name) {
                        validate_no_secret_defaults(
                            key,
                            &field.value,
                            value,
                            &schema_child_path(path, &field.name),
                        )?;
                    }
                }
            }
        }
        ConfigSchemaValue::Map { value: item_schema } => {
            if let Some(values) = value.as_object() {
                for (entry, value) in values {
                    validate_no_secret_defaults(
                        key,
                        item_schema,
                        value,
                        &schema_child_path(path, entry),
                    )?;
                }
            }
        }
        ConfigSchemaValue::List { value: item_schema } => {
            if let Some(values) = value.as_array() {
                for (index, value) in values.iter().enumerate() {
                    validate_no_secret_defaults(
                        key,
                        item_schema,
                        value,
                        &format!("{path}[{index}]"),
                    )?;
                }
            }
        }
        ConfigSchemaValue::Bool
        | ConfigSchemaValue::I64 { .. }
        | ConfigSchemaValue::U64 { .. }
        | ConfigSchemaValue::F64 { .. }
        | ConfigSchemaValue::String { .. }
        | ConfigSchemaValue::StringList
        | ConfigSchemaValue::Json => {}
    }
    Ok(())
}

fn validate_module_schema_value(
    key: &ConfigKey,
    schema: &ConfigSchemaValue,
    path: &str,
) -> Result<(), ConfigError> {
    match schema {
        ConfigSchemaValue::Json => invalid_schema_declaration(
            key,
            path,
            "module settings must describe this value instead of using raw JSON",
        ),
        ConfigSchemaValue::Object {
            fields,
            allow_unknown_fields,
        } => {
            if fields.is_empty() {
                return invalid_schema_declaration(
                    key,
                    path,
                    "module settings objects must declare at least one field",
                );
            }
            if *allow_unknown_fields {
                return invalid_schema_declaration(
                    key,
                    path,
                    "module settings objects must be field-complete; use a typed map for dynamic keys",
                );
            }
            for field in fields {
                if field.description.trim().is_empty() {
                    return invalid_schema_declaration(
                        key,
                        &schema_child_path(path, &field.name),
                        "module settings field description cannot be empty",
                    );
                }
                validate_module_schema_value(
                    key,
                    &field.value,
                    &schema_child_path(path, &field.name),
                )?;
            }
            Ok(())
        }
        ConfigSchemaValue::Map { value } => {
            validate_module_schema_value(key, value, &format!("{path}.*"))
        }
        ConfigSchemaValue::List { value } => {
            validate_module_schema_value(key, value, &format!("{path}[]"))
        }
        ConfigSchemaValue::Bool
        | ConfigSchemaValue::I64 { .. }
        | ConfigSchemaValue::U64 { .. }
        | ConfigSchemaValue::F64 { .. }
        | ConfigSchemaValue::String { .. }
        | ConfigSchemaValue::StringList => Ok(()),
    }
}

fn validate_schema_range_declaration(
    key: &ConfigKey,
    path: &str,
    range: Option<&ConfigNumberRange>,
) -> Result<(), ConfigError> {
    let Some(range) = range else {
        return Ok(());
    };
    if range.min.is_some_and(|value| !value.is_finite())
        || range.max.is_some_and(|value| !value.is_finite())
    {
        return invalid_schema_declaration(key, path, "number range must be finite");
    }
    if let (Some(min), Some(max)) = (range.min, range.max)
        && min > max
    {
        return invalid_schema_declaration(
            key,
            path,
            format!("number range minimum {min} exceeds maximum {max}"),
        );
    }
    Ok(())
}

fn invalid_schema_declaration<T>(
    key: &ConfigKey,
    path: &str,
    detail: impl Into<String>,
) -> Result<T, ConfigError> {
    Err(ConfigError::InvalidValue {
        key: key.to_string(),
        detail: format!("{path}: invalid schema declaration: {}", detail.into()),
    })
}

fn validate_schema_value(
    key: &ConfigKey,
    schema: &ConfigSchemaValue,
    value: &Value,
    path: &str,
) -> Result<(), ConfigError> {
    match schema {
        ConfigSchemaValue::Bool if value.is_boolean() => Ok(()),
        ConfigSchemaValue::Bool => invalid_schema_type(key, path, "bool"),
        ConfigSchemaValue::I64 { range } => {
            let Some(number) = value.as_i64() else {
                return invalid_schema_type(key, path, "i64");
            };
            validate_schema_number_range(key, path, number as f64, range.as_ref())
        }
        ConfigSchemaValue::U64 { range } => {
            let Some(number) = value.as_u64() else {
                return invalid_schema_type(key, path, "u64");
            };
            validate_schema_number_range(key, path, number as f64, range.as_ref())
        }
        ConfigSchemaValue::F64 { range } => {
            let Some(number) = value.as_f64() else {
                return invalid_schema_type(key, path, "f64");
            };
            validate_schema_number_range(key, path, number, range.as_ref())
        }
        ConfigSchemaValue::String { choices, .. } => {
            let Some(text) = value.as_str() else {
                return invalid_schema_type(key, path, "string");
            };
            if !choices.is_empty() && !choices.iter().any(|choice| choice.value == text) {
                return Err(ConfigError::InvalidValue {
                    key: key.to_string(),
                    detail: format!(
                        "{path}: value `{text}` is not one of {}",
                        choices
                            .iter()
                            .map(|choice| choice.value.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                });
            }
            Ok(())
        }
        ConfigSchemaValue::StringList => {
            let Some(values) = value.as_array() else {
                return invalid_schema_type(key, path, "string list");
            };
            if values.iter().all(Value::is_string) {
                Ok(())
            } else {
                invalid_schema_type(key, path, "string list")
            }
        }
        ConfigSchemaValue::Object {
            fields,
            allow_unknown_fields,
        } => {
            let Some(map) = value.as_object() else {
                return invalid_schema_type(key, path, "object");
            };
            for field in fields {
                let field_path = schema_child_path(path, &field.name);
                match map.get(&field.name) {
                    Some(value) if value.is_null() && !field.required => {}
                    Some(value) => validate_schema_value(key, &field.value, value, &field_path)?,
                    None if field.required => {
                        return Err(ConfigError::InvalidValue {
                            key: key.to_string(),
                            detail: format!("{field_path}: required field is missing"),
                        });
                    }
                    None => {}
                }
            }
            if !allow_unknown_fields {
                for present in map.keys() {
                    if fields.iter().all(|field| field.name != *present) {
                        return Err(ConfigError::InvalidValue {
                            key: key.to_string(),
                            detail: format!(
                                "{}: unknown field `{present}`",
                                schema_child_path(path, present)
                            ),
                        });
                    }
                }
            }
            Ok(())
        }
        ConfigSchemaValue::Map { value: item_schema } => {
            let Some(map) = value.as_object() else {
                return invalid_schema_type(key, path, "object map");
            };
            for (entry_key, entry_value) in map {
                validate_schema_value(
                    key,
                    item_schema,
                    entry_value,
                    &schema_child_path(path, entry_key),
                )?;
            }
            Ok(())
        }
        ConfigSchemaValue::List { value: item_schema } => {
            let Some(values) = value.as_array() else {
                return invalid_schema_type(key, path, "array");
            };
            for (index, item) in values.iter().enumerate() {
                validate_schema_value(key, item_schema, item, &format!("{path}[{index}]"))?;
            }
            Ok(())
        }
        ConfigSchemaValue::Json => Ok(()),
    }
}

fn validate_schema_number_range(
    key: &ConfigKey,
    path: &str,
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
            detail: format!("{path}: value {value} is below minimum {min}"),
        });
    }
    if let Some(max) = range.max
        && value > max
    {
        return Err(ConfigError::InvalidValue {
            key: key.to_string(),
            detail: format!("{path}: value {value} is above maximum {max}"),
        });
    }
    Ok(())
}

fn invalid_schema_type<T>(key: &ConfigKey, path: &str, expected: &str) -> Result<T, ConfigError> {
    Err(ConfigError::InvalidValue {
        key: key.to_string(),
        detail: format!("{path}: expected {expected}"),
    })
}

fn schema_child_path(parent: &str, child: &str) -> String {
    if parent == "$" {
        format!("$.{child}")
    } else {
        format!("{parent}.{child}")
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
    Kv(#[from] sinomo_core::kv::KvError),
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
    use sinomo_core::kv::{
        ClusterKvTxnCommitResult, KvAuthoritativeRead, KvAuthorityBasis, KvAuthorityProof,
        KvCommandDigest, KvEntry, KvLocalRead, KvRevision, KvSnapshot, KvTxnResponse, KvVersion,
    };
    use sinomo_core::network::{
        ObjectRpcIncomingBody, ObjectRpcOutgoingResponse, ObjectRpcResponse,
    };
    use sinomo_core::service::ResponseSink;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

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

    fn module_like_definition() -> ConfigDefinition {
        ConfigDefinition::new(
            "test.modules",
            "test",
            ConfigValueType::Object {
                schema_name: "TestModules".to_string(),
            },
            "test module map",
            ConfigClusterMode::LocalOnly,
        )
        .expect("definition")
        .with_default(&serde_json::json!({ "modules": {} }))
        .expect("default")
        .with_schema(ConfigSchema::object(
            "TestModules",
            "test module map",
            vec![ConfigSchemaField::new(
                "modules",
                "Modules",
                "Module enablement keyed by module id.",
                ConfigSchemaValue::map(
                    ConfigSchemaValue::object(vec![
                        ConfigSchemaField::new(
                            "enabled",
                            "Enabled",
                            "Whether this module should run.",
                            ConfigSchemaValue::bool(),
                        )
                        .with_default_value(Value::Bool(false)),
                    ])
                    .without_unknown_fields(),
                ),
            )],
        ))
        .expect("schema")
        .with_module_id("module.test")
        .expect("module id")
    }

    #[test]
    fn operational_error_presentation_is_read_only_and_schema_bound() {
        let definition = ConfigDefinition::new(
            "test.module-status",
            "test",
            ConfigValueType::Object {
                schema_name: "TestModuleStatus".to_string(),
            },
            "test module status",
            ConfigClusterMode::LocalOnly,
        )
        .expect("definition")
        .with_default(&serde_json::json!({
            "last_error": null,
            "resolution": null,
        }))
        .expect("default")
        .with_schema(ConfigSchema::object(
            "TestModuleStatus",
            "test module status",
            vec![
                ConfigSchemaField::new(
                    "last_error",
                    "Error",
                    "Current failure.",
                    ConfigSchemaValue::string(),
                )
                .optional(),
                ConfigSchemaField::new(
                    "resolution",
                    "Resolution",
                    "How to resolve the failure.",
                    ConfigSchemaValue::string(),
                )
                .optional(),
            ],
        ))
        .expect("schema")
        .with_module_id("module.test-status")
        .expect("module id");
        assert!(
            definition
                .clone()
                .with_operational_error_presentation("last_error", "resolution")
                .expect_err("editable operational status must be rejected")
                .to_string()
                .contains("read-only")
        );
        let definition = definition
            .read_only()
            .with_operational_error_presentation("last_error", "resolution")
            .expect("read-only operational presentation");
        definition.validate_contract().expect("valid contract");
        assert_eq!(
            definition.operational_error,
            Some(ConfigOperationalErrorPresentation {
                error_field: "last_error".to_string(),
                resolution_field: "resolution".to_string(),
            })
        );
    }

    fn registry() -> ConfigRegistry {
        ConfigRegistry::builder()
            .register(bool_definition())
            .expect("register")
            .build()
    }

    #[test]
    fn config_catalog_object_rpc_preserves_nested_schema() {
        let definition = module_like_definition().read_only();
        let response = ConfigCatalogResponse {
            definitions: vec![definition.clone()],
        };
        let frame = ObjectRpcResponse::encode_body(CONFIG_CATALOG_ROUTE, &response)
            .expect("encode config catalog response");

        let decoded: ConfigCatalogResponse =
            frame.decode_body().expect("decode config catalog response");
        let expected_module_id =
            sinomo_core::Id::from_legacy_named("module.test").expect("module id");

        assert_eq!(decoded.definitions, vec![definition]);
        assert_eq!(
            decoded.definitions[0].module_id.as_deref(),
            Some(expected_module_id.as_str())
        );
        assert!(decoded.definitions[0].read_only);
        assert!(matches!(
            &decoded.definitions[0].schema,
            Some(ConfigSchema {
                value: ConfigSchemaValue::Object { fields, .. },
                ..
            }) if matches!(
                fields.as_slice(),
                [ConfigSchemaField {
                    value: ConfigSchemaValue::Map { value },
                    ..
                }] if matches!(
                    value.as_ref(),
                    ConfigSchemaValue::Object { fields, .. }
                        if matches!(
                            fields.as_slice(),
                            [ConfigSchemaField {
                                value: ConfigSchemaValue::Bool,
                                ..
                            }]
                        )
                )
            )
        ));
    }

    #[derive(Serialize)]
    struct LegacyConfigDefinitionWithoutSchema {
        key: ConfigKey,
        owner: String,
        value_type: ConfigValueType,
        description: String,
        cluster_mode: ConfigClusterMode,
    }

    #[derive(Serialize)]
    struct LegacyConfigCatalogResponseWithoutSchema {
        definitions: Vec<LegacyConfigDefinitionWithoutSchema>,
    }

    #[test]
    fn config_catalog_object_rpc_decodes_schema_less_definition_for_fallback() {
        let legacy = LegacyConfigCatalogResponseWithoutSchema {
            definitions: vec![LegacyConfigDefinitionWithoutSchema {
                key: ConfigKey::new("test.legacy-object").expect("legacy config key"),
                owner: "legacy-module".to_string(),
                value_type: ConfigValueType::Object {
                    schema_name: "LegacyObject".to_string(),
                },
                description: "legacy object without a field schema".to_string(),
                cluster_mode: ConfigClusterMode::LocalOnly,
            }],
        };
        let frame = ObjectRpcResponse::encode_body(CONFIG_CATALOG_ROUTE, &legacy)
            .expect("encode legacy config catalog response");

        let decoded: ConfigCatalogResponse = frame
            .decode_body()
            .expect("decode legacy config catalog response");

        assert_eq!(decoded.definitions.len(), 1);
        let definition = &decoded.definitions[0];
        assert_eq!(definition.key.as_str(), "test.legacy-object");
        assert_eq!(definition.owner, "legacy-module");
        assert_eq!(definition.module_id, None);
        assert!(!definition.read_only);
        assert_eq!(definition.schema, None);
        assert_eq!(definition.default_value, None);
        assert!(definition.local_legacy_paths.is_empty());
        assert!(definition.cluster_legacy_keys.is_empty());
    }

    #[test]
    fn registry_rejects_module_settings_without_a_schema() {
        let definition = ConfigDefinition::new(
            "test.missing-schema",
            "test",
            ConfigValueType::Object {
                schema_name: "MissingSchema".to_string(),
            },
            "invalid module settings",
            ConfigClusterMode::LocalOnly,
        )
        .expect("definition")
        .with_module_id("module.test")
        .expect("module id");

        let error = ConfigRegistry::builder()
            .register(definition)
            .expect_err("module settings must have a detailed schema");
        assert!(
            error.to_string().contains("require a detailed schema"),
            "{error}"
        );
    }

    #[test]
    fn registry_rejects_untyped_or_open_module_schema_nodes() {
        let open_error = ConfigDefinition::new(
            "test.open-schema",
            "test",
            ConfigValueType::Object {
                schema_name: "OpenSchema".to_string(),
            },
            "invalid open module settings",
            ConfigClusterMode::LocalOnly,
        )
        .expect("definition")
        .with_schema(ConfigSchema::new(
            "OpenSchema",
            "open object",
            ConfigSchemaValue::object(vec![ConfigSchemaField::new(
                "known",
                "Known",
                "Known field.",
                ConfigSchemaValue::bool(),
            )]),
        ))
        .expect("schema declaration")
        .with_module_id("module.test")
        .expect_err("module object schemas must be field-complete");
        assert!(
            open_error.to_string().contains("field-complete"),
            "{open_error}"
        );

        let json_error = ConfigDefinition::new(
            "test.raw-json-schema",
            "test",
            ConfigValueType::Object {
                schema_name: "RawJsonSchema".to_string(),
            },
            "invalid raw JSON module settings",
            ConfigClusterMode::LocalOnly,
        )
        .expect("definition")
        .with_schema(ConfigSchema::object(
            "RawJsonSchema",
            "raw child",
            vec![ConfigSchemaField::new(
                "payload",
                "Payload",
                "Untyped payload.",
                ConfigSchemaValue::Json,
            )],
        ))
        .expect("schema declaration")
        .with_module_id("module.test")
        .expect_err("module schemas must not fall back to raw JSON");
        assert!(json_error.to_string().contains("raw JSON"), "{json_error}");
    }

    #[test]
    fn schema_declaration_rejects_duplicate_fields_and_invalid_ranges() {
        let default_error = ConfigDefinition::new(
            "test.invalid-field-default",
            "test",
            ConfigValueType::Object {
                schema_name: "InvalidFieldDefault".to_string(),
            },
            "invalid field default",
            ConfigClusterMode::LocalOnly,
        )
        .expect("definition")
        .with_schema(ConfigSchema::object(
            "InvalidFieldDefault",
            "invalid field default",
            vec![
                ConfigSchemaField::new(
                    "enabled",
                    "Enabled",
                    "Whether this value is enabled.",
                    ConfigSchemaValue::bool(),
                )
                .with_default_value(Value::String("yes".to_string())),
            ],
        ))
        .expect_err("field defaults must satisfy their schema");
        assert!(default_error.to_string().contains("expected bool"));

        let secret_default_error = ConfigDefinition::new(
            "test.secret-field-default",
            "test",
            ConfigValueType::Object {
                schema_name: "SecretFieldDefault".to_string(),
            },
            "secret field default",
            ConfigClusterMode::LocalOnly,
        )
        .expect("definition")
        .with_schema(ConfigSchema::object(
            "SecretFieldDefault",
            "secret field default",
            vec![
                ConfigSchemaField::new(
                    "token",
                    "Token",
                    "Secret token.",
                    ConfigSchemaValue::secret_string(),
                )
                .with_default_value(Value::String("do-not-publish".to_string())),
            ],
        ))
        .expect_err("secret defaults must not be published in schemas");
        assert!(
            secret_default_error
                .to_string()
                .contains("secret fields cannot publish a default value")
        );

        let nested_secret_field_default_error = ConfigDefinition::new(
            "test.nested-secret-field-default",
            "test",
            ConfigValueType::Object {
                schema_name: "NestedSecretFieldDefault".to_string(),
            },
            "nested secret field default",
            ConfigClusterMode::LocalOnly,
        )
        .expect("definition")
        .with_schema(ConfigSchema::object(
            "NestedSecretFieldDefault",
            "nested secret field default",
            vec![
                ConfigSchemaField::new(
                    "auth",
                    "Authentication",
                    "Authentication settings.",
                    ConfigSchemaValue::object(vec![ConfigSchemaField::new(
                        "token",
                        "Token",
                        "Secret token.",
                        ConfigSchemaValue::secret_string(),
                    )])
                    .without_unknown_fields(),
                )
                .with_default_value(serde_json::json!({
                    "token": "do-not-publish"
                })),
            ],
        ))
        .expect_err("nested schema defaults must not publish credentials");
        assert!(
            nested_secret_field_default_error
                .to_string()
                .contains("$.auth.token: config defaults cannot publish a secret value")
        );

        let secret_choices_error = ConfigDefinition::new(
            "test.secret-choices",
            "test",
            ConfigValueType::String {
                choices: vec!["token-a".to_string()],
            },
            "secret choices",
            ConfigClusterMode::LocalOnly,
        )
        .expect("definition")
        .with_schema(ConfigSchema::new(
            "SecretChoices",
            "secret choices",
            ConfigSchemaValue::String {
                choices: vec![ConfigSchemaChoice::new("token-a", "Token A")],
                secret: true,
            },
        ))
        .expect_err("secret strings must not publish choices");
        assert!(
            secret_choices_error
                .to_string()
                .contains("secret strings cannot declare choices")
        );

        let secret_document_error = ConfigDefinition::new(
            "test.secret-document-default",
            "test",
            ConfigValueType::Object {
                schema_name: "SecretDocumentDefault".to_string(),
            },
            "secret document default",
            ConfigClusterMode::LocalOnly,
        )
        .expect("definition")
        .with_default(&serde_json::json!({
            "auth": { "token": "do-not-publish" }
        }))
        .expect("untyped default")
        .with_schema(ConfigSchema::object(
            "SecretDocumentDefault",
            "secret document default",
            vec![ConfigSchemaField::new(
                "auth",
                "Authentication",
                "Authentication settings.",
                ConfigSchemaValue::object(vec![ConfigSchemaField::new(
                    "token",
                    "Token",
                    "Secret token.",
                    ConfigSchemaValue::secret_string(),
                )])
                .without_unknown_fields(),
            )],
        ))
        .expect("typed schema")
        .with_module_id("module.test")
        .expect_err("module defaults must not publish nested credentials");
        assert!(
            secret_document_error
                .to_string()
                .contains("$.auth.token: config defaults cannot publish a secret value")
        );

        let name_error = ConfigDefinition::new(
            "test.schema-name-mismatch",
            "test",
            ConfigValueType::Object {
                schema_name: "ExpectedSchema".to_string(),
            },
            "mismatched schema name",
            ConfigClusterMode::LocalOnly,
        )
        .expect("definition")
        .with_schema(ConfigSchema::object(
            "OtherSchema",
            "mismatched schema",
            vec![ConfigSchemaField::new(
                "value",
                "Value",
                "Typed value.",
                ConfigSchemaValue::bool(),
            )],
        ))
        .expect_err("object schema names must match their declared value type");
        assert!(name_error.to_string().contains("ExpectedSchema"));

        let duplicate_error = ConfigDefinition::new(
            "test.duplicate-fields",
            "test",
            ConfigValueType::Object {
                schema_name: "DuplicateFields".to_string(),
            },
            "duplicate fields",
            ConfigClusterMode::LocalOnly,
        )
        .expect("definition")
        .with_schema(ConfigSchema::object(
            "DuplicateFields",
            "duplicate fields",
            vec![
                ConfigSchemaField::new("value", "Value", "First.", ConfigSchemaValue::bool()),
                ConfigSchemaField::new("value", "Value", "Second.", ConfigSchemaValue::bool()),
            ],
        ))
        .expect_err("duplicate schema fields must be rejected");
        assert!(
            duplicate_error
                .to_string()
                .contains("duplicate field `value`")
        );

        let range_error = ConfigDefinition::new(
            "test.invalid-range",
            "test",
            ConfigValueType::I64 { range: None },
            "invalid range",
            ConfigClusterMode::LocalOnly,
        )
        .expect("definition")
        .with_schema(ConfigSchema::new(
            "InvalidRange",
            "invalid range",
            ConfigSchemaValue::i64(Some(ConfigNumberRange {
                min: Some(10.0),
                max: Some(1.0),
            })),
        ))
        .expect_err("inverted schema ranges must be rejected");
        assert!(
            range_error
                .to_string()
                .contains("minimum 10 exceeds maximum 1")
        );

        let wider_range_error = ConfigDefinition::new(
            "test.wider-schema-range",
            "test",
            ConfigValueType::I64 {
                range: Some(ConfigNumberRange {
                    min: Some(0.0),
                    max: Some(10.0),
                }),
            },
            "wider schema range",
            ConfigClusterMode::LocalOnly,
        )
        .expect("definition")
        .with_schema(ConfigSchema::new(
            "WiderSchemaRange",
            "wider schema range",
            ConfigSchemaValue::i64(Some(ConfigNumberRange {
                min: Some(-1.0),
                max: Some(10.0),
            })),
        ))
        .expect_err("UI ranges must not allow values rejected by the value type");
        assert!(
            wider_range_error
                .to_string()
                .contains("schema number range must stay within")
        );

        let choice_mismatch_error = ConfigDefinition::new(
            "test.schema-choice-mismatch",
            "test",
            ConfigValueType::String {
                choices: vec!["stable".to_string()],
            },
            "mismatched choices",
            ConfigClusterMode::LocalOnly,
        )
        .expect("definition")
        .with_schema(ConfigSchema::new(
            "SchemaChoiceMismatch",
            "mismatched choices",
            ConfigSchemaValue::string_choices(vec![ConfigSchemaChoice::new(
                "experimental",
                "Experimental",
            )]),
        ))
        .expect_err("UI choices must be accepted by the value type");
        assert!(
            choice_mismatch_error
                .to_string()
                .contains("schema string choices must be a non-empty subset")
        );

        let list_item_error = ConfigDefinition::new(
            "test.string-list-schema-item",
            "test",
            ConfigValueType::StringList,
            "invalid string list item schema",
            ConfigClusterMode::LocalOnly,
        )
        .expect("definition")
        .with_schema(ConfigSchema::new(
            "StringListSchemaItem",
            "invalid string list item schema",
            ConfigSchemaValue::list(ConfigSchemaValue::bool()),
        ))
        .expect_err("string-list UI items must also be strings");
        assert!(list_item_error.to_string().contains("root kind"));
    }

    #[test]
    fn schema_field_default_does_not_relax_a_required_field() {
        let key = ConfigKey::new("test.schema-field-default").expect("config key");
        let schema = ConfigSchema::object(
            "SchemaFieldDefault",
            "field default behavior",
            vec![
                ConfigSchemaField::new(
                    "enabled",
                    "Enabled",
                    "Whether the feature is enabled.",
                    ConfigSchemaValue::bool(),
                )
                .with_default_value(Value::Bool(false)),
                ConfigSchemaField::new(
                    "name",
                    "Name",
                    "Required name.",
                    ConfigSchemaValue::string(),
                ),
            ],
        );

        let missing_enabled = schema
            .validate(&key, &serde_json::json!({}))
            .expect_err("a UI construction default does not make a field optional");
        assert!(missing_enabled.to_string().contains("$.enabled"));

        schema
            .validate(
                &key,
                &serde_json::json!({ "enabled": false, "name": "example" }),
            )
            .expect("explicit required fields are valid");
    }

    #[test]
    fn schema_validates_nested_map_fields() {
        let definition = module_like_definition();
        definition
            .validate_value(&serde_json::json!({
                "modules": {
                    "observe": { "enabled": true }
                }
            }))
            .expect("valid nested object");

        let error = definition
            .validate_value(&serde_json::json!({
                "modules": {
                    "observe": { "enabled": "yes" }
                }
            }))
            .expect_err("invalid nested value");
        assert!(
            error
                .to_string()
                .contains("$.modules.observe.enabled: expected bool"),
            "{error}"
        );

        let error = definition
            .validate_value(&serde_json::json!({
                "modules": {
                    "observe": { "enabled": true, "extra": true }
                }
            }))
            .expect_err("unknown nested field");
        assert!(
            error
                .to_string()
                .contains("$.modules.observe.extra: unknown field"),
            "{error}"
        );
    }

    #[test]
    fn local_configs_are_written_as_human_readable_documents_under_configs_dir() {
        let root = sinomo_testdir::TestDir::new("sinomo-config-local");
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
        let root = sinomo_testdir::TestDir::new("sinomo-config-migrate");
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
        committed_request_ids: Mutex<Vec<KvRequestId>>,
        authoritative_failure: Mutex<Option<String>>,
        local_get_calls: AtomicUsize,
        authoritative_get_calls: AtomicUsize,
    }

    impl ClusterAccess for InMemoryCluster {
        fn kv_get_local(&self, key: &str) -> Result<KvLocalRead<Option<KvEntry>>, ServiceError> {
            self.local_get_calls.fetch_add(1, Ordering::Relaxed);
            let value = self
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
                });
            Ok(KvLocalRead {
                value,
                revision: KvRevision(1),
                observed_term: 1,
                applied_index: 1,
                known_leader: None,
            })
        }

        fn kv_get_authoritative_async<'a>(
            &'a self,
            key: &'a str,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<KvAuthoritativeRead<Option<KvEntry>>, ServiceError>,
                    > + Send
                    + 'a,
            >,
        > {
            Box::pin(async move {
                self.authoritative_get_calls.fetch_add(1, Ordering::Relaxed);
                if let Some(detail) = self
                    .authoritative_failure
                    .lock()
                    .expect("authoritative failure lock")
                    .clone()
                {
                    return Err(ServiceError::Service(detail));
                }
                let value = self
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
                    });
                Ok(KvAuthoritativeRead {
                    value,
                    authority: KvAuthorityProof {
                        term: 1,
                        commit_index: 1,
                        revision: KvRevision(1),
                        basis: KvAuthorityBasis::CurrentTermQuorum {
                            voter_acks: 1,
                            required_quorum: 1,
                        },
                    },
                })
            })
        }

        fn kv_range_prefix_local(
            &self,
            prefix: &str,
        ) -> Result<KvLocalRead<Vec<KvEntry>>, ServiceError> {
            let entries = self.entries.lock().expect("cluster lock");
            let value = entries
                .iter()
                .filter(|(key, _)| key.starts_with(prefix))
                .map(|(key, value)| KvEntry {
                    key: key.clone(),
                    value: value.clone(),
                    create_revision: KvRevision(1),
                    modify_revision: KvRevision(1),
                    version: KvVersion(1),
                    lease_id: None,
                })
                .collect();
            Ok(KvLocalRead {
                value,
                revision: KvRevision(1),
                observed_term: 1,
                applied_index: 1,
                known_leader: None,
            })
        }

        fn kv_commit_async<'a>(
            &'a self,
            request: KvTxnRequest,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = Result<ClusterKvTxnCommitResult, ServiceError>>
                    + Send
                    + 'a,
            >,
        > {
            Box::pin(async move {
                let request_id = request.request_id.clone();
                self.committed_request_ids
                    .lock()
                    .expect("request id lock")
                    .push(request_id.clone());
                let mut entries = self.entries.lock().expect("cluster lock");
                for op in request.txn.success {
                    if let KvOp::Put { key, value, .. } = op {
                        entries.insert(key, value);
                    }
                }
                let snapshot_entries = entries
                    .iter()
                    .map(|(key, value)| KvEntry {
                        key: key.clone(),
                        value: value.clone(),
                        create_revision: KvRevision(1),
                        modify_revision: KvRevision(1),
                        version: KvVersion(1),
                        lease_id: None,
                    })
                    .collect();
                Ok(ClusterKvTxnCommitResult {
                    request_id,
                    command_digest: KvCommandDigest([0; 32]),
                    snapshot: KvSnapshot {
                        revision: KvRevision(1),
                        compacted_revision: KvRevision::ZERO,
                        entries: snapshot_entries,
                        leases: Vec::new(),
                        events: Vec::new(),
                        committed_requests: Vec::new(),
                        expired_requests: Vec::new(),
                    },
                    txn_response: Some(KvTxnResponse {
                        succeeded: true,
                        revision: KvRevision(1),
                        responses: Vec::new(),
                    }),
                    authority: KvAuthorityProof {
                        term: 1,
                        commit_index: 1,
                        revision: KvRevision(1),
                        basis: KvAuthorityBasis::CurrentTermQuorum {
                            voter_acks: 1,
                            required_quorum: 1,
                        },
                    },
                    replayed_committed_request: false,
                })
            })
        }
    }

    #[tokio::test]
    async fn cluster_config_write_preserves_caller_supplied_request_id() {
        let root = sinomo_testdir::TestDir::new("sinomo-config-request-id");
        let data_dir = SinomoDataDir::new(&root);
        let store = ConfigStore::new(registry());
        let cluster = InMemoryCluster::default();
        let request_id = KvRequestId::generate();

        store
            .set_async_with_request_id(
                &data_dir,
                Some(&cluster),
                "test.enabled",
                Value::Bool(true),
                ConfigWriteTarget::Cluster,
                Some(request_id.clone()),
            )
            .await
            .expect("cluster write");

        assert_eq!(
            cluster
                .committed_request_ids
                .lock()
                .expect("request id lock")
                .as_slice(),
            std::slice::from_ref(&request_id)
        );
    }

    #[tokio::test]
    async fn cluster_override_local_prefers_cluster_when_available() {
        let root = sinomo_testdir::TestDir::new("sinomo-config-cluster");
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
            .get_async(&data_dir, Some(&cluster), "test.enabled")
            .await
            .expect("resolve");
        assert_eq!(resolved.value, Value::Bool(true));
        assert_eq!(resolved.source, ConfigValueSource::Cluster);
        assert_eq!(cluster.local_get_calls.load(Ordering::Relaxed), 0);
        assert_eq!(cluster.authoritative_get_calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn cluster_override_local_does_not_fallback_when_authority_fails() {
        let root = sinomo_testdir::TestDir::new("sinomo-config-authority-failure");
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
            .expect("local fallback candidate");
        let cluster = InMemoryCluster::default();
        *cluster
            .authoritative_failure
            .lock()
            .expect("authoritative failure lock") = Some("quorum unavailable".to_string());

        let error = store
            .get_async(&data_dir, Some(&cluster), "test.enabled")
            .await
            .expect_err("authoritative failure must not fall back to local config");
        assert!(error.to_string().contains("quorum unavailable"), "{error}");
        assert_eq!(cluster.local_get_calls.load(Ordering::Relaxed), 0);
        assert_eq!(cluster.authoritative_get_calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn local_only_config_does_not_request_cluster_authority() {
        let root = sinomo_testdir::TestDir::new("sinomo-config-local-only-authority");
        let data_dir = SinomoDataDir::new(&root);
        let definition = ConfigDefinition::new(
            "test.local-only",
            "test",
            ConfigValueType::Bool,
            "local-only test switch",
            ConfigClusterMode::LocalOnly,
        )
        .expect("local-only definition");
        let local_registry = ConfigRegistry::builder()
            .register(definition)
            .expect("register local-only definition")
            .build();
        let store = ConfigStore::new(local_registry);
        store
            .set(
                &data_dir,
                None,
                "test.local-only",
                Value::Bool(true),
                ConfigWriteTarget::Local,
            )
            .expect("local-only value");
        let cluster = InMemoryCluster::default();
        *cluster
            .authoritative_failure
            .lock()
            .expect("authoritative failure lock") = Some("authority must not be used".to_string());

        let resolved = store
            .get_async(&data_dir, Some(&cluster), "test.local-only")
            .await
            .expect("local-only resolution");
        assert_eq!(resolved.value, Value::Bool(true));
        assert_eq!(resolved.source, ConfigValueSource::Local);
        assert_eq!(cluster.authoritative_get_calls.load(Ordering::Relaxed), 0);
    }

    #[derive(Default)]
    struct VecSink {
        frames: Arc<Mutex<Vec<ObjectRpcOutgoingResponse>>>,
    }

    impl ResponseSink for VecSink {
        fn send(self: Box<Self>, response: ObjectRpcOutgoingResponse) -> Result<(), ServiceError> {
            self.frames.lock().expect("sink lock").push(response);
            Ok(())
        }
    }

    #[tokio::test]
    async fn config_service_searches_and_sets_local_values() {
        let root = sinomo_testdir::TestDir::new("sinomo-config-service");
        let sink = VecSink::default();
        let frames = Arc::clone(&sink.frames);
        let ctx = ServiceContext::new(
            Arc::new(SinomoDataDir::new(&root)),
            None,
            sinomo_core::service::ServiceSigner::Local,
            ObjectRpcIncomingBody::empty(),
            Box::new(sink),
        );
        let registry =
            register_services(sinomo_core::service::ServiceRegistry::builder(), registry()).build();
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
        let response = response
            .into_materialized_value_response(usize::MAX)
            .await
            .expect("materialize response value");
        let response: ConfigSearchResponse = response.decode_body().expect("decode");
        assert_eq!(response.definitions.len(), 1);
    }

    #[tokio::test]
    async fn remote_configuration_only_requires_host_consent_for_host_effects() {
        use sinomo_core::node::{LocalNodePermissionStore, LocalNodePermissions};
        use sinomo_core::service::ServiceSigner;
        let root = sinomo_testdir::TestDir::new("config-remote-host-scope");
        let data_dir = Arc::new(SinomoDataDir::new(&root));
        let mut builder = ConfigRegistry::builder();
        for (key, host_effect) in [("test.ordinary", false), ("test.host", true)] {
            let mut definition = ConfigDefinition::new(
                key,
                "test",
                ConfigValueType::Bool,
                "test setting",
                ConfigClusterMode::LocalOnly,
            )
            .unwrap()
            .with_default(&false)
            .unwrap();
            if host_effect {
                definition = definition.with_remote_host_management();
            }
            builder = builder.register(definition).unwrap();
        }
        let config_registry = builder.build();
        let store = ConfigStore::new(config_registry.clone());
        let services = register_services(
            sinomo_core::service::ServiceRegistry::builder(),
            config_registry,
        )
        .build();
        let permissions = LocalNodePermissionStore::new(data_dir.root());
        // Permission changes are checked on every request. Local configuration
        // remains available and reads do not grant host-effect authority.
        for (key, local, grant, expected) in [
            ("test.ordinary", false, false, true),
            ("test.host", false, false, false),
            ("test.host", false, true, true),
            ("test.host", false, false, false),
            ("test.host", true, false, true),
        ] {
            let mut document = LocalNodePermissions::new();
            document.set_remote_management_enabled(grant);
            permissions.save(&document).unwrap();
            let before = store.get(data_dir.as_ref(), None, key).unwrap().value;
            let signer = if local {
                ServiceSigner::Local
            } else {
                ServiceSigner::Remote {
                    node_id: "peer".into(),
                    public_key: "authenticated-peer".into(),
                }
            };
            let ctx = ServiceContext::new(
                Arc::clone(&data_dir),
                None,
                signer,
                ObjectRpcIncomingBody::empty(),
                Box::new(VecSink::default()),
            );
            let request = ObjectRpcRequest::encode_body(
                CONFIG_SET_ROUTE,
                CONFIG_RPC_CATEGORY,
                &ConfigSetRequest {
                    request_id: Some(KvRequestId::generate()),
                    key: key.into(),
                    value: Value::Bool(!before.as_bool().unwrap()),
                    target: ConfigWriteTarget::Local,
                },
            )
            .unwrap();
            let result = services.dispatch(ctx, request).await;
            assert_eq!(
                result.is_ok(),
                expected,
                "{key}, local={local}, host grant={grant}: {result:?}"
            );
            if !expected {
                assert_eq!(
                    store.get(data_dir.as_ref(), None, key).unwrap().value,
                    before
                );
            }
        }
        let ctx = ServiceContext::new(
            Arc::clone(&data_dir),
            None,
            ServiceSigner::Remote {
                node_id: "peer".into(),
                public_key: "authenticated-peer".into(),
            },
            ObjectRpcIncomingBody::empty(),
            Box::new(VecSink::default()),
        );
        let request = ObjectRpcRequest::encode_body(
            CONFIG_GET_ROUTE,
            CONFIG_RPC_CATEGORY,
            &ConfigGetRequest {
                key: "test.host".into(),
            },
        )
        .unwrap();
        services
            .dispatch(ctx, request)
            .await
            .expect("remote host configuration can be inspected without granting host effects");
    }

    #[test]
    fn legacy_module_config_definitions_preserve_their_remote_host_gate() {
        let definition = bool_definition();
        assert!(!definition.remote_write_requires_host_permission);
        let mut encoded = serde_json::to_value(&definition).unwrap();
        assert_eq!(encoded["remote_write_requires_host_permission"], false);
        encoded
            .as_object_mut()
            .unwrap()
            .remove("remote_write_requires_host_permission");
        let legacy: ConfigDefinition = serde_json::from_value(encoded).unwrap();
        assert!(legacy.remote_write_requires_host_permission);
    }

    #[tokio::test]
    async fn config_service_get_does_not_fallback_after_authority_failure() {
        let root = sinomo_testdir::TestDir::new("sinomo-config-service-authority-failure");
        let data_dir = SinomoDataDir::new(&root);
        ConfigStore::new(registry())
            .set(
                &data_dir,
                None,
                "test.enabled",
                Value::Bool(false),
                ConfigWriteTarget::Local,
            )
            .expect("local fallback candidate");
        let cluster = Arc::new(InMemoryCluster::default());
        *cluster
            .authoritative_failure
            .lock()
            .expect("authoritative failure lock") = Some("service quorum unavailable".to_string());
        let cluster_access: Arc<dyn ClusterAccess> = cluster.clone();
        let sink = VecSink::default();
        let frames = Arc::clone(&sink.frames);
        let ctx = ServiceContext::new(
            Arc::new(data_dir),
            Some(cluster_access),
            sinomo_core::service::ServiceSigner::Local,
            ObjectRpcIncomingBody::empty(),
            Box::new(sink),
        );
        let service_registry =
            register_services(sinomo_core::service::ServiceRegistry::builder(), registry()).build();
        let request = ObjectRpcRequest::encode_body(
            CONFIG_GET_ROUTE,
            CONFIG_RPC_CATEGORY,
            &ConfigGetRequest {
                key: "test.enabled".to_string(),
            },
        )
        .expect("encode config get");

        let error = service_registry
            .dispatch(ctx, request)
            .await
            .expect_err("service must fail closed when config authority is unavailable");
        assert!(
            error.to_string().contains("service quorum unavailable"),
            "{error}"
        );
        assert!(frames.lock().expect("frames").is_empty());
        assert_eq!(cluster.local_get_calls.load(Ordering::Relaxed), 0);
        assert_eq!(cluster.authoritative_get_calls.load(Ordering::Relaxed), 1);
    }
}
