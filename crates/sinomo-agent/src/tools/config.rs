use super::{
    AgentTool, AgentToolCapability, AgentToolDescriptor, AgentToolEvidence, AgentToolRequest,
    AgentToolResult,
};
use crate::{AgentToolCall, AgentToolKind, AgentToolSpec};
use sinomo_runtime::config::{
    ConfigDefinition, ConfigError, ConfigStore, ConfigValueSource, ConfigWriteTarget,
};
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::service::ClusterAccess;
use std::sync::Arc;

#[derive(Clone)]
pub struct ConfigAgentToolConfig {
    data_dir: SinomoDataDir,
    store: ConfigStore,
    cluster: Option<Arc<dyn ClusterAccess>>,
    allow_writes: bool,
}

impl ConfigAgentToolConfig {
    pub fn new(data_dir: SinomoDataDir, store: ConfigStore) -> Self {
        Self {
            data_dir,
            store,
            cluster: None,
            allow_writes: true,
        }
    }

    pub fn with_cluster(mut self, cluster: Arc<dyn ClusterAccess>) -> Self {
        self.cluster = Some(cluster);
        self
    }

    pub fn allowing_writes(mut self, allow_writes: bool) -> Self {
        self.allow_writes = allow_writes;
        self
    }

    fn cluster(&self) -> Option<&dyn ClusterAccess> {
        self.cluster.as_deref()
    }
}

#[derive(Clone)]
pub struct ConfigSearchTool {
    config: ConfigAgentToolConfig,
}

impl ConfigSearchTool {
    pub fn new(config: ConfigAgentToolConfig) -> Self {
        Self { config }
    }
}

impl AgentTool for ConfigSearchTool {
    fn descriptor(&self) -> AgentToolDescriptor {
        config_search_descriptor()
    }

    fn execute(&self, request: AgentToolRequest<'_>) -> AgentToolResult {
        let query = input_string(request.call, "query").unwrap_or_default();
        let owner = input_string(request.call, "owner");
        let mut definitions = self.config.store.registry().search(&query);
        if let Some(owner) = owner.as_deref() {
            definitions.retain(|definition| definition.owner == owner);
        }
        let output = serde_json::json!({
            "query": query,
            "owner": owner,
            "count": definitions.len(),
            "definitions": definitions.iter().map(summarize_definition).collect::<Vec<_>>(),
        });
        AgentToolResult::succeeded(
            format!("found {} config definition(s)", definitions.len()),
            output,
        )
        .with_evidence(AgentToolEvidence::policy_only(
            true,
            "searched registered config definitions",
        ))
    }
}

#[derive(Clone)]
pub struct ConfigGetTool {
    config: ConfigAgentToolConfig,
}

impl ConfigGetTool {
    pub fn new(config: ConfigAgentToolConfig) -> Self {
        Self { config }
    }
}

impl AgentTool for ConfigGetTool {
    fn descriptor(&self) -> AgentToolDescriptor {
        config_get_descriptor()
    }

    fn execute(&self, request: AgentToolRequest<'_>) -> AgentToolResult {
        let Some(key) = input_string(request.call, "key") else {
            return AgentToolResult::failed(
                "config_get requires a string key",
                serde_json::json!({ "error": "missing_key" }),
            );
        };
        match self
            .config
            .store
            .get(&self.config.data_dir, self.config.cluster(), &key)
        {
            Ok(resolution) => {
                let output = serde_json::json!({
                    "key": resolution.key.as_str(),
                    "source": config_value_source_label(resolution.source),
                    "definition": summarize_definition(&resolution.definition),
                    "value": redact_config_value(resolution.value),
                });
                AgentToolResult::succeeded(format!("read config `{key}`"), output).with_evidence(
                    AgentToolEvidence::policy_only(true, "resolved config through sinomo-config"),
                )
            }
            Err(error) => config_error_result("config_get", &key, error),
        }
    }
}

#[derive(Clone)]
pub struct ConfigSetTool {
    config: ConfigAgentToolConfig,
}

impl ConfigSetTool {
    pub fn new(config: ConfigAgentToolConfig) -> Self {
        Self { config }
    }
}

impl AgentTool for ConfigSetTool {
    fn descriptor(&self) -> AgentToolDescriptor {
        config_set_descriptor()
    }

    fn execute(&self, request: AgentToolRequest<'_>) -> AgentToolResult {
        if !self.config.allow_writes {
            return AgentToolResult::blocked(
                "config_set is disabled for this agent runtime",
                serde_json::json!({ "error": "config_writes_disabled" }),
            );
        }
        let Some(key) = input_string(request.call, "key") else {
            return AgentToolResult::failed(
                "config_set requires a string key",
                serde_json::json!({ "error": "missing_key" }),
            );
        };
        let Some(value) = request.call.input.get("value").cloned() else {
            return AgentToolResult::failed(
                "config_set requires a value",
                serde_json::json!({ "error": "missing_value" }),
            );
        };
        let target = match input_string(request.call, "target")
            .as_deref()
            .map(parse_write_target)
            .transpose()
        {
            Ok(target) => target.unwrap_or(ConfigWriteTarget::Auto),
            Err(detail) => {
                return AgentToolResult::failed(
                    detail.clone(),
                    serde_json::json!({ "error": "invalid_target", "detail": detail }),
                );
            }
        };
        match self.config.store.set(
            &self.config.data_dir,
            self.config.cluster(),
            &key,
            value,
            target,
        ) {
            Ok(result) => {
                let output = serde_json::json!({
                    "key": result.key.as_str(),
                    "source": config_value_source_label(result.source),
                    "value": redact_config_value(result.value),
                });
                AgentToolResult::succeeded(format!("updated config `{key}`"), output).with_evidence(
                    AgentToolEvidence::policy_only(true, "updated config through sinomo-config"),
                )
            }
            Err(error) => config_error_result("config_set", &key, error),
        }
    }
}

pub fn config_tool_descriptors() -> Vec<AgentToolDescriptor> {
    vec![
        config_search_descriptor(),
        config_get_descriptor(),
        config_set_descriptor(),
    ]
}

pub fn registry(config: ConfigAgentToolConfig) -> super::AgentToolRegistry {
    super::AgentToolRegistry::new()
        .with_tool(ConfigSearchTool::new(config.clone()))
        .with_tool(ConfigGetTool::new(config.clone()))
        .with_tool(ConfigSetTool::new(config))
}

pub fn register_tools(
    registry: &super::AgentToolRegistry,
    config: ConfigAgentToolConfig,
) -> Vec<Option<AgentToolDescriptor>> {
    vec![
        registry.register(ConfigSearchTool::new(config.clone())),
        registry.register(ConfigGetTool::new(config.clone())),
        registry.register(ConfigSetTool::new(config)),
    ]
}

fn config_search_descriptor() -> AgentToolDescriptor {
    AgentToolDescriptor::new(AgentToolSpec::new(
        AgentToolKind::ConfigSearch,
        "config_search",
        "Search registered Sinomo configuration keys by key, owner, or description.",
        false,
    ))
    .with_capabilities([AgentToolCapability::ManageConfig])
    .with_input_schema(serde_json::json!({
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "Optional case-insensitive text matched against key, owner, and description."
            },
            "owner": {
                "type": "string",
                "description": "Optional exact owner filter, for example sinomo-lm or sinomo-sandbox."
            }
        }
    }))
}

fn config_get_descriptor() -> AgentToolDescriptor {
    AgentToolDescriptor::new(AgentToolSpec::new(
        AgentToolKind::ConfigGet,
        "config_get",
        "Read a registered Sinomo configuration value through the shared config resolver.",
        false,
    ))
    .with_capabilities([AgentToolCapability::ManageConfig])
    .with_input_schema(serde_json::json!({
        "type": "object",
        "required": ["key"],
        "properties": {
            "key": {
                "type": "string",
                "description": "Registered config key, for example lm.runtime or sandbox.policy."
            }
        }
    }))
}

fn config_set_descriptor() -> AgentToolDescriptor {
    AgentToolDescriptor::new(AgentToolSpec::new(
        AgentToolKind::ConfigSet,
        "config_set",
        "Update a registered Sinomo configuration value through the shared config resolver.",
        true,
    ))
    .with_capabilities([AgentToolCapability::ManageConfig])
    .with_input_schema(serde_json::json!({
        "type": "object",
        "required": ["key", "value"],
        "properties": {
            "key": {
                "type": "string",
                "description": "Registered config key to update."
            },
            "value": {
                "description": "JSON value matching the registered config definition."
            },
            "target": {
                "type": "string",
                "enum": ["auto", "local", "cluster"],
                "description": "Optional write target. Auto uses the registered cluster cooperation mode."
            }
        }
    }))
}

fn summarize_definition(definition: &ConfigDefinition) -> serde_json::Value {
    serde_json::json!({
        "key": definition.key.as_str(),
        "owner": definition.owner,
        "description": definition.description,
        "cluster_mode": definition.cluster_mode,
        "value_type": definition.value_type,
        "local_path": definition.local_path(&SinomoDataDir::new("$SINOMO_DATA_DIR")),
        "cluster_key": definition.cluster_key(),
    })
}

fn config_value_source_label(source: ConfigValueSource) -> &'static str {
    match source {
        ConfigValueSource::Default => "default",
        ConfigValueSource::Local => "local",
        ConfigValueSource::Cluster => "cluster",
        ConfigValueSource::MigratedLocal => "migrated-local",
        ConfigValueSource::LegacyCluster => "legacy-cluster",
    }
}

fn parse_write_target(value: &str) -> Result<ConfigWriteTarget, String> {
    match value {
        "auto" => Ok(ConfigWriteTarget::Auto),
        "local" => Ok(ConfigWriteTarget::Local),
        "cluster" => Ok(ConfigWriteTarget::Cluster),
        other => Err(format!(
            "config target must be one of auto, local, cluster, got `{other}`"
        )),
    }
}

fn config_error_result(operation: &str, key: &str, error: ConfigError) -> AgentToolResult {
    let output = serde_json::json!({
        "error": "config_error",
        "operation": operation,
        "key": key,
        "detail": error.to_string(),
    });
    match error {
        ConfigError::ClusterUnavailable { .. } | ConfigError::WriteTargetUnavailable { .. } => {
            AgentToolResult::blocked(format!("{operation} blocked for `{key}`: {error}"), output)
        }
        _ => AgentToolResult::failed(format!("{operation} failed for `{key}`: {error}"), output),
    }
}

fn input_string(call: &AgentToolCall, key: &str) -> Option<String> {
    call.input
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

fn redact_config_value(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => serde_json::Value::Object(
            map.into_iter()
                .map(|(key, value)| {
                    if is_sensitive_key(&key) {
                        (key, serde_json::Value::String("<redacted>".to_string()))
                    } else {
                        (key, redact_config_value(value))
                    }
                })
                .collect(),
        ),
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(redact_config_value).collect())
        }
        other => other,
    }
}

fn is_sensitive_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    key.contains("api_key")
        || key.contains("apikey")
        || key.contains("token")
        || key.contains("secret")
        || key.contains("password")
        || key.contains("credential")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AgentToolCall, InteractionSession, InteractionSurface, InteractionSurfaceKind};
    use sinomo_runtime::config::{
        ConfigClusterMode, ConfigRegistry, ConfigValueType, ConfigWriteTarget,
    };
    use sinomo_runtime::node::NodeId;

    fn test_config(root: std::path::PathBuf) -> ConfigAgentToolConfig {
        let definition = sinomo_runtime::config::ConfigDefinition::new(
            "test.enabled",
            "test",
            ConfigValueType::Bool,
            "test config",
            ConfigClusterMode::LocalOnly,
        )
        .expect("definition")
        .with_default(&false)
        .expect("default");
        let registry = ConfigRegistry::builder()
            .register(definition)
            .expect("register")
            .build();
        ConfigAgentToolConfig::new(SinomoDataDir::new(root), ConfigStore::new(registry))
    }

    fn session() -> InteractionSession {
        InteractionSession::new(
            crate::SessionId::generate(),
            NodeId::from_legacy_named("node_config_tool").expect("node id"),
            InteractionSurface::new("test", InteractionSurfaceKind::CommandLine),
        )
    }

    #[test]
    fn config_tools_search_get_and_set_local_values() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-config-tool");
        let registry = registry(test_config(root.to_path_buf()));
        let session = session();

        let search = registry.execute(
            &session,
            0,
            0,
            &AgentToolCall {
                call_id: None,
                tool: "config_search".to_string(),
                input: serde_json::json!({ "query": "enabled" }),
                r#async: false,
                summary: "search".to_string(),
            },
        );
        assert_eq!(search.status, super::super::AgentToolStatus::Succeeded);
        assert_eq!(search.output["count"], 1);

        let set = registry.execute(
            &session,
            0,
            1,
            &AgentToolCall {
                call_id: None,
                tool: "config_set".to_string(),
                input: serde_json::json!({
                    "key": "test.enabled",
                    "value": true,
                    "target": "local"
                }),
                r#async: false,
                summary: "set".to_string(),
            },
        );
        assert_eq!(set.status, super::super::AgentToolStatus::Succeeded);
        assert_eq!(set.output["source"], "local");

        let get = registry.execute(
            &session,
            0,
            2,
            &AgentToolCall {
                call_id: None,
                tool: "config_get".to_string(),
                input: serde_json::json!({ "key": "test.enabled" }),
                r#async: false,
                summary: "get".to_string(),
            },
        );
        assert_eq!(get.status, super::super::AgentToolStatus::Succeeded);
        assert_eq!(get.output["value"], true);
        assert!(
            root.join("configs")
                .join("test")
                .join("enabled.json")
                .exists()
        );
    }

    #[test]
    fn config_set_can_be_disabled_by_runtime() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-config-tool-disabled");
        let registry = registry(test_config(root.to_path_buf()).allowing_writes(false));
        let outcome = registry.execute(
            &session(),
            0,
            0,
            &AgentToolCall {
                call_id: None,
                tool: "config_set".to_string(),
                input: serde_json::json!({
                    "key": "test.enabled",
                    "value": true,
                    "target": "local"
                }),
                r#async: false,
                summary: "set".to_string(),
            },
        );
        assert_eq!(outcome.status, super::super::AgentToolStatus::Blocked);
    }

    #[test]
    fn config_tool_redacts_sensitive_values() {
        let value = serde_json::json!({
            "api_key": "secret",
            "nested": { "access_token": "token" },
            "safe": "visible"
        });

        let redacted = redact_config_value(value);
        assert_eq!(redacted["api_key"], "<redacted>");
        assert_eq!(redacted["nested"]["access_token"], "<redacted>");
        assert_eq!(redacted["safe"], "visible");
    }

    #[test]
    fn parse_config_write_target() {
        assert_eq!(
            parse_write_target("local").expect("local"),
            ConfigWriteTarget::Local
        );
        assert!(parse_write_target("remote").is_err());
    }
}
