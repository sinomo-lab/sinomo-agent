//! Cluster + app configuration access, via the `sinomo-config` registry.
//!
//! Local mode resolves values from the local data dir (cluster = `None`); when a
//! cluster is present the daemon would supply a `ClusterAccess`. We expose a
//! flat, UI-friendly view plus a typed setter for boolean keys (the common,
//! safely-togglable case).

use serde_json::Value;
use sinomo_runtime::config::{ConfigStore, ConfigValueSource, ConfigWriteTarget};
use sinomo_runtime::local::SinomoDataDir;

/// One resolved configuration key.
#[derive(Clone, Debug)]
pub struct ConfigItem {
    pub key: String,
    pub owner: String,
    pub description: String,
    pub value: String,
    pub source: &'static str,
    /// `Some` when the value is a boolean (rendered as a toggle).
    pub bool_value: Option<bool>,
}

/// Build the config registry/store the app knows about (mirrors the CLI wiring).
pub fn build_config_store() -> Result<ConfigStore, String> {
    let builder = sinomo_runtime::config::ConfigRegistry::builder();
    let builder = sinomo_runtime::dfs::register_dfs_config_definitions(builder)
        .map_err(|e| format!("dfs config: {e:?}"))?;
    let builder = sinomo_runtime::rollout::register_config_definitions(builder)
        .map_err(|e| format!("rollout config: {e:?}"))?;
    let builder = sinomo_runtime::sandbox::register_config_definitions(builder)
        .map_err(|e| format!("sandbox config: {e:?}"))?;
    let builder =
        sinomo_lm::register_config_definitions(builder).map_err(|e| format!("lm config: {e:?}"))?;
    Ok(ConfigStore::new(builder.build()))
}

/// Resolve every known config key against the local data dir.
pub fn resolve_items(store: &ConfigStore, data_dir: &SinomoDataDir) -> Vec<ConfigItem> {
    let mut keys: Vec<String> = store
        .registry()
        .definitions()
        .map(|def| def.key.as_str().to_string())
        .collect();
    keys.sort();
    keys.dedup();

    let mut items = Vec::new();
    for key in keys {
        let Ok(def) = store.registry().get(&key) else {
            continue;
        };
        let owner = def.owner.clone();
        let description = def.description.clone();
        let (value, source) = match store.get(data_dir, None, &key) {
            Ok(resolution) => (resolution.value, source_label(resolution.source)),
            Err(_) => (Value::Null, "unset"),
        };
        let bool_value = value.as_bool();
        items.push(ConfigItem {
            key,
            owner,
            description,
            value: compact(&value),
            source,
            bool_value,
        });
    }
    items
}

/// Toggle a boolean config key (writes to the local layer).
pub fn set_bool(
    store: &ConfigStore,
    data_dir: &SinomoDataDir,
    key: &str,
    value: bool,
) -> Result<(), String> {
    store
        .set(
            data_dir,
            None,
            key,
            Value::Bool(value),
            ConfigWriteTarget::Local,
        )
        .map(|_| ())
        .map_err(|e| e.to_string())
}

fn source_label(source: ConfigValueSource) -> &'static str {
    match source {
        ConfigValueSource::Default => "default",
        ConfigValueSource::Local => "local",
        ConfigValueSource::Cluster => "cluster",
        ConfigValueSource::MigratedLocal => "local",
        ConfigValueSource::LegacyCluster => "cluster",
    }
}

fn compact(value: &Value) -> String {
    match value {
        Value::Null => "—".to_string(),
        Value::String(s) => s.clone(),
        other => {
            let s = other.to_string();
            if s.chars().count() > 80 {
                let truncated: String = s.chars().take(79).collect();
                format!("{truncated}…")
            } else {
                s
            }
        }
    }
}
