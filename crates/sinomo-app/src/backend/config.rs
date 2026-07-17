//! App configuration access, expressed entirely against the `sinomo-config`
//! [`ConfigClient`] service boundary.
//!
//! The app is a *client*: it never resolves config locally itself. Reads and writes go
//! through `backend.config` — a [`ConfigClient`] that is either the in-process node store
//! (embedded) or an RPC connection to the daemon. Either way the cluster/local layering is
//! resolved server-side, so the app stays on the same configuration source of truth as the
//! daemon and CLI. This module just builds the registry (for the embedded client) and adapts
//! the client into flat, UI-friendly views.

use serde_json::Value;
use sinomo_cli::app_api::ProviderSummary;
use sinomo_lm::{
    DEFAULT_PROVIDER_ACCOUNT, LM_PROVIDER_CONFIG_KEY, LM_PROVIDER_HEALTH_CONFIG_KEY,
    LanguageModelProviderHealthConfig, LanguageModelProviderHealthSnapshot,
    LanguageModelProviderOptions, ModelConfig, ProviderAccountConfig, ProviderReasoningEffort,
    ProviderTextVerbosity, model_supports_reasoning_effort, normalize_model_provider,
    normalize_provider_account_name, provider_health_account_key, upsert_provider_account_config,
};
use sinomo_runtime::config::{
    ConfigClient, ConfigClusterMode, ConfigSchema, ConfigStore, ConfigValueSource, ConfigValueType,
    ConfigWriteTarget,
};
use sinomo_runtime::local::SinomoDataDir;

/// How a config value is edited in the Advanced settings UI.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfigEditKind {
    /// A boolean — rendered as a toggle, never text-edited.
    Bool,
    /// A free-form string — edited as raw text.
    Text,
    /// A number — edited as text and parsed as JSON (i64/u64/f64).
    Number,
    /// A list / object / arbitrary JSON — edited as a JSON string.
    Json,
}

impl ConfigEditKind {
    fn from_value_type(value_type: &ConfigValueType) -> Self {
        match value_type {
            ConfigValueType::Bool => Self::Bool,
            ConfigValueType::I64 { .. }
            | ConfigValueType::U64 { .. }
            | ConfigValueType::F64 { .. } => Self::Number,
            ConfigValueType::String { .. } => Self::Text,
            ConfigValueType::StringList
            | ConfigValueType::Object { .. }
            | ConfigValueType::Json => Self::Json,
        }
    }
}

/// One resolved configuration key.
#[derive(Clone, Debug)]
pub struct ConfigItem {
    pub key: String,
    pub owner: String,
    /// Explicit module lifecycle identity supplied by the config catalog.
    /// Settings surfaces must not infer this association from crate/owner names.
    pub module_id: Option<String>,
    pub description: String,
    pub value: String,
    pub source: &'static str,
    /// Human-readable authority/synchronization scope declared by the catalog.
    pub scope: &'static str,
    /// `Some` when the value is a boolean (rendered as a toggle).
    pub bool_value: Option<bool>,
    /// How the value is edited in the Advanced editor.
    pub edit_kind: ConfigEditKind,
    /// The full (non-elided) editable representation seeded into the inline editor.
    pub edit_value: String,
    /// Full resolved value used to seed schema-driven, whole-definition drafts.
    pub raw_value: Value,
    /// Optional structured schema declared by the owner crate/module.
    pub schema: Option<ConfigSchema>,
    /// Catalog intent for operational documents that may be inspected but not
    /// edited from the user-facing settings surface.
    pub read_only: bool,
}

impl ConfigItem {
    pub fn update_value(&mut self, value: Value) {
        self.bool_value = value.as_bool();
        self.edit_value = editable_repr(self.edit_kind, &value);
        self.value = compact(&value);
        self.raw_value = value;
    }
}

/// Build the config registry/store the app knows about (mirrors the CLI wiring).
pub fn build_config_store() -> Result<ConfigStore, String> {
    let builder = sinomo_runtime::config::ConfigRegistry::builder();
    let builder = sinomo_runtime::dfs::register_dfs_config_definitions(builder)
        .map_err(|e| format!("dfs config: {e:?}"))?;
    let builder = sinomo_runtime::module::register_config_definitions(builder)
        .map_err(|e| format!("module config: {e}"))?;
    let builder = sinomo_runtime::rollout::register_config_definitions(builder)
        .map_err(|e| format!("rollout config: {e:?}"))?;
    let builder = sinomo_runtime::sandbox::register_config_definitions(builder)
        .map_err(|e| format!("sandbox config: {e:?}"))?;
    let builder =
        sinomo_lm::register_config_definitions(builder).map_err(|e| format!("lm config: {e:?}"))?;
    let builder = sinomo_evolution::register_config_definitions(builder)
        .map_err(|e| format!("evolution config: {e:?}"))?;
    Ok(ConfigStore::new(builder.build()))
}

/// Resolve every known config key through the config client.
pub fn resolve_items(client: &dyn ConfigClient) -> Vec<ConfigItem> {
    let Ok(mut definitions) = client.catalog(None) else {
        return Vec::new();
    };
    definitions.sort_by(|a, b| a.key.as_str().cmp(b.key.as_str()));

    let mut items = Vec::new();
    for def in definitions {
        let key = def.key.as_str().to_string();
        let (value, source) = match client.get(&key) {
            Ok(resolution) => (resolution.value, source_label(resolution.source)),
            Err(_) => (Value::Null, "unset"),
        };
        let bool_value = value.as_bool();
        let edit_kind = ConfigEditKind::from_value_type(&def.value_type);
        let edit_value = editable_repr(edit_kind, &value);
        items.push(ConfigItem {
            key,
            owner: def.owner.clone(),
            module_id: def.module_id.clone(),
            description: def.description.clone(),
            value: compact(&value),
            source,
            scope: scope_label(def.cluster_mode),
            bool_value,
            edit_kind,
            edit_value,
            raw_value: value,
            schema: def.schema.clone(),
            read_only: def.read_only,
        });
    }
    items
}

/// The full, editable representation of a value for the inline editor (no elision): raw text for
/// strings, the number literal for numbers, compact JSON for everything else.
fn editable_repr(kind: ConfigEditKind, value: &Value) -> String {
    match kind {
        ConfigEditKind::Bool => String::new(),
        ConfigEditKind::Text => value.as_str().unwrap_or_default().to_string(),
        ConfigEditKind::Number => match value {
            Value::Null => String::new(),
            other => other.to_string(),
        },
        ConfigEditKind::Json => match value {
            Value::Null => String::new(),
            other => serde_json::to_string(other).unwrap_or_default(),
        },
    }
}

/// Write an edited config value. `raw` is parsed per `kind` (string verbatim, otherwise JSON) and
/// written with `Auto` target so the service routes it to the cluster or local layer.
pub fn set_value(
    client: &dyn ConfigClient,
    key: &str,
    kind: ConfigEditKind,
    raw: &str,
) -> Result<(), String> {
    let value = match kind {
        ConfigEditKind::Bool => Value::Bool(raw.trim().eq_ignore_ascii_case("true")),
        ConfigEditKind::Text => Value::String(raw.to_string()),
        ConfigEditKind::Number | ConfigEditKind::Json => {
            serde_json::from_str(raw.trim()).map_err(|e| format!("invalid value: {e}"))?
        }
    };
    client
        .set(key, value, ConfigWriteTarget::Auto)
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// Toggle a boolean config key. The write target is `Auto`, so the service routes it to the
/// cluster or local layer per the key's definition.
pub fn set_bool(client: &dyn ConfigClient, key: &str, value: bool) -> Result<(), String> {
    client
        .set(key, Value::Bool(value), ConfigWriteTarget::Auto)
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// The provider accounts offered in the chat composer: exactly the accounts configured and
/// enabled in the cluster's `lm.providers` configuration (resolved cluster-over-local,
/// the same registry the Settings tab reads). Returns an empty list when the cluster
/// has no providers configured. Model lists are seeded with each account's configured default
/// model when available and otherwise fetched on demand from `sinomo-lm`.
pub fn cluster_provider_summaries(client: &dyn ConfigClient) -> Vec<ProviderSummary> {
    let Some(config) = load_provider_config(client) else {
        return Vec::new();
    };

    let mut counts = std::collections::BTreeMap::<String, usize>::new();
    for account in config.accounts.iter().filter(|account| account.enabled) {
        let provider =
            normalize_model_provider(&account.provider).unwrap_or_else(|| account.provider.clone());
        *counts.entry(provider).or_default() += 1;
    }

    let mut summaries: Vec<ProviderSummary> = Vec::new();
    for account in config.accounts.iter().filter(|account| account.enabled) {
        let provider =
            normalize_model_provider(&account.provider).unwrap_or_else(|| account.provider.clone());
        let account_name = normalize_provider_account_name(Some(&account.account));
        summaries.push(ProviderSummary {
            id: provider.clone(),
            account: account_name.clone(),
            name: provider_account_display_name(
                &provider,
                &account_name,
                counts.get(&provider).copied().unwrap_or(0),
            ),
            models: vec![account.default_model.clone().unwrap_or_else(|| {
                sinomo_lm::effective_model_name(&provider, sinomo_lm::PROVIDER_DEFAULT_MODEL)
            })],
        });
    }
    summaries
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use sinomo_runtime::config::LocalConfigClient;
    use sinomo_runtime::local::SinomoDataDir;

    fn test_client(name: &str) -> (LocalConfigClient, std::path::PathBuf) {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "sinomo-app-config-{name}-{}-{stamp}",
            std::process::id()
        ));
        let data_dir = SinomoDataDir::new(root.clone());
        let store = Arc::new(build_config_store().expect("config store"));
        (LocalConfigClient::new(store, data_dir, None), root)
    }

    #[test]
    fn app_config_store_includes_evolution_schema() {
        let store = build_config_store().expect("config store");
        let definition = store
            .registry()
            .get(sinomo_evolution::EVOLUTION_CONFIG_KEY)
            .expect("evolution config definition");
        let expected_module_id = sinomo_evolution::evolution_module_id().to_string();
        assert!(definition.schema.is_some());
        assert_eq!(
            definition.module_id.as_deref(),
            Some(expected_module_id.as_str())
        );
    }

    #[test]
    fn provider_config_supports_multiple_accounts_for_same_provider() {
        let (client, _root) = test_client("multiple-accounts");

        add_provider_account(&client, "openai", "work", "sk-work", "gpt-5", "")
            .expect("add work account");
        add_provider_account(
            &client,
            "openai",
            "personal",
            "sk-personal",
            "gpt-5-mini",
            "",
        )
        .expect("add personal account");

        let config = load_provider_config(&client).expect("provider config");
        assert_eq!(config.accounts.len(), 2);
        assert!(config.accounts.iter().any(|account| {
            account.provider == "openai"
                && account.account == "work"
                && account.api_key.as_deref() == Some("sk-work")
        }));
        assert!(config.accounts.iter().any(|account| {
            account.provider == "openai"
                && account.account == "personal"
                && account.api_key.as_deref() == Some("sk-personal")
        }));

        let forms = provider_forms(&client);
        assert_eq!(forms.len(), 2);
        assert!(forms.iter().any(|form| form.account == "work"));
        assert!(forms.iter().any(|form| form.account == "personal"));

        let summaries = cluster_provider_summaries(&client);
        assert_eq!(summaries.len(), 2);
        assert!(summaries.iter().any(|summary| {
            summary.id == "openai" && summary.account == "work" && summary.name.contains("work")
        }));
        assert!(summaries.iter().any(|summary| {
            summary.id == "openai"
                && summary.account == "personal"
                && summary.name.contains("personal")
        }));
    }

    #[test]
    fn provider_forms_include_complete_known_model_catalogs() {
        let (client, _root) = test_client("known-models");
        add_provider_account(
            &client,
            "deepseek",
            "default",
            "sk-test",
            sinomo_lm::DEEPSEEK_DEFAULT_MODEL,
            "",
        )
        .expect("add provider");

        let form = provider_forms(&client).pop().expect("provider form");
        assert_eq!(
            form.known_models,
            vec![
                sinomo_lm::DEEPSEEK_V4_FLASH_MODEL.to_string(),
                sinomo_lm::DEEPSEEK_V4_PRO_MODEL.to_string()
            ]
        );
    }

    #[test]
    fn resolved_config_items_include_declared_schema() {
        let (client, _root) = test_client("schema-catalog");
        let items = resolve_items(&client);
        let item = items
            .iter()
            .find(|item| item.key == sinomo_runtime::module::MODULE_NODE_CONFIG_KEY)
            .expect("module node config");
        let schema = item.schema.as_ref().expect("module config schema");
        assert!(!item.scope.is_empty(), "catalog scope should reach the UI");
        assert_eq!(schema.name, "ModuleRuntimeConfig");
        assert!(matches!(
            &schema.value,
            sinomo_runtime::config::ConfigSchemaValue::Object { fields, .. }
                if fields.iter().any(|field| field.name == "modules")
        ));
    }

    #[test]
    fn disabling_one_provider_account_keeps_other_accounts_available() {
        let (client, _root) = test_client("disable-one-account");
        add_provider_account(&client, "openai", "work", "sk-work", "gpt-5", "")
            .expect("add work account");
        add_provider_account(
            &client,
            "openai",
            "personal",
            "sk-personal",
            "gpt-5-mini",
            "",
        )
        .expect("add personal account");

        set_provider_enabled(&client, "openai", "work", false).expect("disable work");

        let summaries = cluster_provider_summaries(&client);
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].account, "personal");
    }

    #[test]
    fn removing_one_provider_account_keeps_other_accounts_available() {
        let (client, _root) = test_client("remove-one-account");
        add_provider_account(&client, "openai", "work", "sk-work", "gpt-5", "")
            .expect("add work account");
        add_provider_account(
            &client,
            "openai",
            "personal",
            "sk-personal",
            "gpt-5-mini",
            "",
        )
        .expect("add personal account");

        remove_provider_account(&client, "openai", "work").expect("remove work account");

        let config = load_provider_config(&client).expect("provider config");
        assert_eq!(config.accounts.len(), 1);
        assert_eq!(config.accounts[0].account, "personal");
        assert_eq!(config.accounts[0].api_key.as_deref(), Some("sk-personal"));

        let summaries = cluster_provider_summaries(&client);
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].account, "personal");
    }

    #[test]
    fn removing_default_provider_account_repairs_defaults() {
        let (client, _root) = test_client("remove-default-account");
        add_provider_account(&client, "openai", "work", "sk-work", "gpt-5", "")
            .expect("add work account");
        add_provider_account(
            &client,
            "deepseek",
            "default",
            "sk-deepseek",
            "deepseek-v4-flash",
            "",
        )
        .expect("add deepseek account");

        remove_provider_account(&client, "openai", "work").expect("remove default account");

        let config = load_provider_config(&client).expect("provider config");
        assert_eq!(config.accounts.len(), 1);
        assert_eq!(config.default_provider.as_deref(), Some("deepseek"));
        assert_eq!(config.default_account.as_deref(), Some("default"));
        assert_eq!(config.default_model.as_deref(), Some("deepseek-v4-flash"));
    }

    #[test]
    fn setting_provider_chat_default_updates_account_and_model() {
        let (client, _root) = test_client("set-chat-default-account");
        add_provider_account(&client, "openai", "work", "sk-work", "gpt-5", "")
            .expect("add work account");
        add_provider_account(
            &client,
            "openai",
            "personal",
            "sk-personal",
            "gpt-5-mini",
            "",
        )
        .expect("add personal account");

        set_provider_chat_default(&client, "openai", "personal")
            .expect("set personal as chat default");

        let config = load_provider_config(&client).expect("provider config");
        assert_eq!(config.default_provider.as_deref(), Some("openai"));
        assert_eq!(config.default_account.as_deref(), Some("personal"));
        assert_eq!(config.default_model.as_deref(), Some("gpt-5-mini"));

        let forms = provider_forms(&client);
        assert!(forms.iter().any(|form| {
            form.account == "personal" && form.default_model == "gpt-5-mini" && form.is_default
        }));
        assert!(
            forms
                .iter()
                .any(|form| form.account == "work" && !form.is_default)
        );
    }

    #[test]
    fn provider_options_round_trip_through_settings_helpers() {
        let (client, _root) = test_client("provider-options");
        add_provider_account(
            &client,
            "anthropic",
            "default",
            "sk-test",
            "claude-sonnet-4-20250514",
            "",
        )
        .expect("add provider");

        set_provider_effort(&client, "anthropic", "default", 4).expect("effort");
        set_provider_reasoning_budget(&client, "anthropic", "default", "8192").expect("budget");
        set_provider_text_verbosity(&client, "anthropic", "default", 2).expect("verbosity");
        set_provider_vendor_options(
            &client,
            "anthropic",
            "default",
            r#"{"metadata.trace":"enabled"}"#,
        )
        .expect("vendor options");

        let form = provider_forms(&client).pop().expect("provider form");
        assert_eq!(form.reasoning_effort.as_deref(), Some("high"));
        assert_eq!(form.reasoning_budget_tokens, Some(8192));
        assert_eq!(form.text_verbosity.as_deref(), Some("medium"));
        assert_eq!(form.vendor_options, r#"{"metadata.trace":"enabled"}"#);
    }

    #[test]
    fn provider_health_persists_across_config_clients_and_can_be_invalidated() {
        let (client, root) = test_client("provider-health-persistence");
        let key = provider_health_account_key("openai", "work");
        let snapshot = LanguageModelProviderHealthSnapshot {
            provider: "openai".to_string(),
            account: "work".to_string(),
            checked_at_unix: 1_700_000_000,
            last_verified_at_unix: Some(1_700_000_000),
            healthy: true,
            error: None,
            models: vec!["gpt-5".to_string()],
            model_discovery_error: None,
            usage: None,
            usage_error: Some("usage unavailable".to_string()),
            stale: false,
        };
        let mut health = LanguageModelProviderHealthConfig::default();
        health.accounts.insert(key.clone(), snapshot);

        write_provider_health(&client, &health).expect("persist health");
        mark_provider_health_stale(&client, "openai", "work").expect("invalidate health");

        let store = Arc::new(build_config_store().expect("config store"));
        let reloaded_client = LocalConfigClient::new(store, SinomoDataDir::new(root.clone()), None);
        let reloaded = load_provider_health(&reloaded_client);
        let snapshot = reloaded.accounts.get(&key).expect("persisted health");

        assert!(snapshot.healthy);
        assert!(snapshot.stale);
        assert_eq!(snapshot.last_verified_at_unix, Some(1_700_000_000));
        assert_eq!(snapshot.models, ["gpt-5"]);
        assert_eq!(snapshot.usage_error.as_deref(), Some("usage unavailable"));

        remove_provider_health(&reloaded_client, "openai", "work").expect("remove health");
        assert!(load_provider_health(&reloaded_client).accounts.is_empty());
    }
}

/// Resolve the `lm.providers` config object into a `ModelConfig` via the client.
/// Returns `None` when unset or unparsable.
pub fn load_provider_config(client: &dyn ConfigClient) -> Option<ModelConfig> {
    let resolution = client.get(LM_PROVIDER_CONFIG_KEY).ok()?;
    serde_json::from_value(resolution.value).ok()
}

/// A human-friendly label for a provider id (falls back to the id itself).
pub fn provider_display_name(id: &str) -> String {
    match id {
        "claude" => "Claude",
        "anthropic" => "Anthropic",
        "openai" => "OpenAI",
        "gemini" => "Google Gemini",
        "deepseek" => "DeepSeek",
        "nvidia" => "NVIDIA",
        "claude-code" => "Claude Code (CLI)",
        "codex" => "Codex",
        "codex-cli" => "Codex (CLI)",
        "github-copilot" => "GitHub Copilot",
        "github-copilot-cli" => "GitHub Copilot (CLI)",
        other => other,
    }
    .to_string()
}

pub fn provider_default_model(id: &str) -> String {
    normalize_model_provider(id)
        .map(|provider| {
            sinomo_lm::effective_model_name(&provider, sinomo_lm::PROVIDER_DEFAULT_MODEL)
        })
        .unwrap_or_default()
}

fn provider_account_display_name(provider: &str, account: &str, provider_count: usize) -> String {
    let provider_name = provider_display_name(provider);
    if provider_count > 1 || account != DEFAULT_PROVIDER_ACCOUNT {
        format!("{provider_name} ({account})")
    } else {
        provider_name
    }
}

/// The providers shown in the Providers settings form. Some use API keys; `codex` and
/// `github-copilot` use the OAuth device-login flow instead.
pub const CONFIGURABLE_PROVIDERS: &[&str] = &[
    "anthropic",
    "openai",
    "gemini",
    "deepseek",
    "nvidia",
    "codex",
    "github-copilot",
];

/// A provider's editable configuration, flattened for the Settings UI.
#[derive(Clone, Debug, PartialEq)]
pub struct ProviderForm {
    pub provider: String,
    pub account: String,
    pub display_name: String,
    pub account_label: String,
    /// Whether this account is the configured chat default.
    pub is_default: bool,
    pub enabled: bool,
    pub default_model: String,
    /// Complete built-in model options available without credential or network discovery.
    pub known_models: Vec<String>,
    pub api_base_url: String,
    /// The configured reasoning effort (e.g. "high"), or `None` for the provider default.
    pub reasoning_effort: Option<String>,
    pub reasoning_budget_tokens: Option<u32>,
    pub text_verbosity: Option<String>,
    pub vendor_options: String,
    /// Whether reasoning effort applies to this provider/model at all.
    pub supports_reasoning: bool,
    /// Whether an API-key credential is stored for this account.
    pub has_api_key: bool,
    /// Whether this provider authenticates via OAuth device login (vs an API key).
    pub uses_device_login: bool,
    /// Whether the provider is authenticated (device-login tokens stored, or an API key set).
    pub signed_in: bool,
}

/// Build the per-account configuration forms from `lm.providers`.
pub fn provider_forms(client: &dyn ConfigClient) -> Vec<ProviderForm> {
    let config = load_provider_config(client).unwrap_or_default();
    let default_provider = config
        .default_provider
        .as_deref()
        .and_then(normalize_model_provider);
    let default_account = normalize_provider_account_name(config.default_account.as_deref());
    let mut counts = std::collections::BTreeMap::<String, usize>::new();
    for account in &config.accounts {
        let provider =
            normalize_model_provider(&account.provider).unwrap_or_else(|| account.provider.clone());
        *counts.entry(provider).or_default() += 1;
    }

    let mut forms = Vec::new();
    for account in &config.accounts {
        let provider =
            normalize_model_provider(&account.provider).unwrap_or_else(|| account.provider.clone());
        if !CONFIGURABLE_PROVIDERS.contains(&provider.as_str()) {
            continue;
        }
        let account_name = normalize_provider_account_name(Some(&account.account));
        let default_model = account.default_model.clone().unwrap_or_default();
        let uses_device_login = sinomo_cli::provider_uses_device_login(&provider);
        let has_inline_api_key = account
            .api_key
            .as_deref()
            .is_some_and(|key| !key.trim().is_empty());
        let has_api_key = has_inline_api_key;
        let signed_in = if uses_device_login {
            sinomo_cli::provider_signed_in(&provider)
        } else {
            has_api_key
        };
        forms.push(ProviderForm {
            provider: provider.clone(),
            account: account_name.clone(),
            display_name: provider_account_display_name(
                &provider,
                &account_name,
                counts.get(&provider).copied().unwrap_or(0),
            ),
            account_label: if account_name == DEFAULT_PROVIDER_ACCOUNT {
                "Default".to_string()
            } else {
                account_name.clone()
            },
            is_default: default_provider.as_deref() == Some(provider.as_str())
                && account_name == default_account,
            enabled: account.enabled,
            supports_reasoning: model_supports_reasoning_effort(&provider, &default_model),
            known_models: sinomo_lm::provider_known_model_ids(&provider),
            default_model,
            api_base_url: account.api_base_url.clone().unwrap_or_default(),
            reasoning_effort: account
                .options
                .reasoning_effort
                .map(|e| e.as_config_value().to_string()),
            reasoning_budget_tokens: account.options.reasoning_budget_tokens,
            text_verbosity: account
                .options
                .text_verbosity
                .map(|value| value.as_config_value().to_string()),
            vendor_options: serde_json::to_string(&account.options.vendor_options)
                .unwrap_or_else(|_| "{}".to_string()),
            has_api_key,
            uses_device_login,
            signed_in,
        });
    }
    forms
}

pub fn load_provider_health(client: &dyn ConfigClient) -> LanguageModelProviderHealthConfig {
    client
        .get(LM_PROVIDER_HEALTH_CONFIG_KEY)
        .ok()
        .and_then(|resolution| serde_json::from_value(resolution.value).ok())
        .unwrap_or_default()
}

fn write_provider_health(
    client: &dyn ConfigClient,
    health: &LanguageModelProviderHealthConfig,
) -> Result<(), String> {
    let value = serde_json::to_value(health).map_err(|error| error.to_string())?;
    client
        .set(
            LM_PROVIDER_HEALTH_CONFIG_KEY,
            value,
            ConfigWriteTarget::Auto,
        )
        .map(|_| ())
        .map_err(|error| error.to_string())
}

pub fn mark_provider_health_stale(
    client: &dyn ConfigClient,
    provider: &str,
    account: &str,
) -> Result<(), String> {
    let mut health = load_provider_health(client);
    if let Some(snapshot) = health
        .accounts
        .get_mut(&provider_health_account_key(provider, account))
    {
        snapshot.stale = snapshot.last_verified_at_unix.is_some();
        write_provider_health(client, &health)?;
    }
    Ok(())
}

pub fn remove_provider_health(
    client: &dyn ConfigClient,
    provider: &str,
    account: &str,
) -> Result<(), String> {
    let mut health = load_provider_health(client);
    if health
        .accounts
        .remove(&provider_health_account_key(provider, account))
        .is_some()
    {
        write_provider_health(client, &health)?;
    }
    Ok(())
}

pub fn test_provider_connection(
    data_dir: &SinomoDataDir,
    client: &dyn ConfigClient,
    provider: &str,
    account: &str,
) -> Result<LanguageModelProviderHealthSnapshot, String> {
    let provider = normalize_configurable_provider(provider)?;
    let account_name = normalize_provider_account_name(Some(account));
    let config = load_provider_config(client).unwrap_or_default();
    let account_config = config.accounts.iter().find(|candidate| {
        normalize_model_provider(&candidate.provider).as_deref() == Some(provider.as_str())
            && normalize_provider_account_name(Some(&candidate.account)) == account_name
    });
    let account_config = account_config.ok_or_else(|| {
        format!(
            "{} account {} was not found",
            provider_display_name(&provider),
            account_name
        )
    })?;

    let key = provider_health_account_key(&provider, &account_name);
    let mut health = load_provider_health(client);
    let previous = health.accounts.get(&key).cloned();
    let checked_at_unix = time::OffsetDateTime::now_utc().unix_timestamp();
    let verification = sinomo_lm::test_provider_account_credentials_in_data_dir(
        data_dir,
        &provider,
        Some(account_config),
    );
    let snapshot = match verification {
        Ok(()) => {
            let (models, model_discovery_error) =
                match sinomo_lm::provider_available_model_ids_for_account_in_data_dir(
                    data_dir,
                    &provider,
                    Some(account_config),
                ) {
                    Ok(models) => (models, None),
                    Err(error) => (Vec::new(), Some(error.to_string())),
                };
            let (usage, usage_error) =
                if sinomo_lm::subscription_usage_provider_ids().contains(&provider.as_str()) {
                    match sinomo_lm::provider_usage_in_data_dir(data_dir, &provider) {
                        Ok(usage) => (Some(usage), None),
                        Err(error) => (None, Some(error.to_string())),
                    }
                } else {
                    (None, None)
                };
            LanguageModelProviderHealthSnapshot {
                provider,
                account: account_name,
                checked_at_unix,
                last_verified_at_unix: Some(checked_at_unix),
                healthy: true,
                error: None,
                models,
                model_discovery_error,
                usage,
                usage_error,
                stale: false,
            }
        }
        Err(error) => LanguageModelProviderHealthSnapshot {
            provider,
            account: account_name,
            checked_at_unix,
            last_verified_at_unix: previous
                .as_ref()
                .and_then(|snapshot| snapshot.last_verified_at_unix),
            healthy: false,
            error: Some(error.to_string()),
            models: previous
                .as_ref()
                .map(|snapshot| snapshot.models.clone())
                .unwrap_or_default(),
            model_discovery_error: previous
                .as_ref()
                .and_then(|snapshot| snapshot.model_discovery_error.clone()),
            usage: previous
                .as_ref()
                .and_then(|snapshot| snapshot.usage.clone()),
            usage_error: previous
                .as_ref()
                .and_then(|snapshot| snapshot.usage_error.clone()),
            stale: previous
                .as_ref()
                .is_some_and(|snapshot| snapshot.last_verified_at_unix.is_some()),
        },
    };
    health.accounts.insert(key, snapshot.clone());
    write_provider_health(client, &health)?;
    Ok(snapshot)
}

/// Run a provider's OAuth device-login flow (codex / github-copilot). Blocks while polling — call
/// it off the UI thread. `on_prompt(verification_uri, user_code)` is invoked once the device code
/// is issued so the UI can show it.
pub fn run_provider_device_login(
    provider: &str,
    on_prompt: impl FnOnce(String, String),
) -> Result<(), String> {
    sinomo_cli::run_device_login(provider, |prompt| {
        on_prompt(prompt.verification_uri, prompt.user_code)
    })
    .map_err(|e| e.to_string())
}

/// Toggle a provider account enabled/disabled in `lm.providers`.
pub fn set_provider_enabled(
    client: &dyn ConfigClient,
    provider: &str,
    account: &str,
    enabled: bool,
) -> Result<(), String> {
    update_provider_account(client, provider, account, |account| {
        account.enabled = enabled
    })
}

/// Remove a provider account from `lm.providers`.
pub fn remove_provider_account(
    client: &dyn ConfigClient,
    provider: &str,
    account: &str,
) -> Result<(), String> {
    let provider = normalize_configurable_provider(provider)?;
    let account_name = normalize_provider_account_name(Some(account));
    let mut config = load_provider_config(client).unwrap_or_default();
    let before = config.accounts.len();
    config.accounts.retain(|candidate| {
        normalize_model_provider(&candidate.provider).as_deref() != Some(provider.as_str())
            || normalize_provider_account_name(Some(&candidate.account)) != account_name
    });
    if config.accounts.len() == before {
        return Err(format!(
            "{} account {} was not found",
            provider_display_name(&provider),
            account_name
        ));
    }

    repair_provider_defaults_after_removal(&mut config, &provider, &account_name);
    write_provider_config(client, &config)
}

fn repair_provider_defaults_after_removal(
    config: &mut ModelConfig,
    removed_provider: &str,
    removed_account: &str,
) {
    if config.accounts.is_empty() {
        config.default_provider = None;
        config.default_account = None;
        config.default_model = None;
        return;
    }

    let default_provider = config
        .default_provider
        .as_deref()
        .and_then(normalize_model_provider);
    let default_account = normalize_provider_account_name(config.default_account.as_deref());
    let removed_default =
        default_provider.as_deref() == Some(removed_provider) && default_account == removed_account;
    let default_still_exists = default_provider.as_deref().is_some_and(|provider| {
        config.accounts.iter().any(|account| {
            normalize_model_provider(&account.provider).as_deref() == Some(provider)
                && normalize_provider_account_name(Some(&account.account)) == default_account
        })
    });

    if !removed_default && default_still_exists {
        return;
    }

    let fallback = config
        .accounts
        .iter()
        .find(|account| account.enabled)
        .or_else(|| config.accounts.first())
        .expect("non-empty accounts has a fallback");
    config.default_provider = Some(
        normalize_model_provider(&fallback.provider).unwrap_or_else(|| fallback.provider.clone()),
    );
    config.default_account = Some(normalize_provider_account_name(Some(&fallback.account)));
    config.default_model = fallback.default_model.clone();
}

/// Set a provider's default model in `lm.providers` (empty clears it).
pub fn set_provider_model(
    client: &dyn ConfigClient,
    provider: &str,
    account: &str,
    model: &str,
) -> Result<(), String> {
    let model = model.trim().to_string();
    update_provider_account(client, provider, account, move |account| {
        account.default_model = (!model.is_empty()).then_some(model);
    })
}

/// Set a provider account as the configured chat default in `lm.providers`.
pub fn set_provider_chat_default(
    client: &dyn ConfigClient,
    provider: &str,
    account: &str,
) -> Result<(), String> {
    let provider = normalize_configurable_provider(provider)?;
    let account_name = normalize_provider_account_name(Some(account));
    let mut config = load_provider_config(client).unwrap_or_default();
    let default_model = config
        .accounts
        .iter()
        .find(|candidate| {
            normalize_model_provider(&candidate.provider).as_deref() == Some(provider.as_str())
                && normalize_provider_account_name(Some(&candidate.account)) == account_name
        })
        .map(|candidate| candidate.default_model.clone())
        .ok_or_else(|| {
            format!(
                "{} account {} was not found",
                provider_display_name(&provider),
                account_name
            )
        })?;

    config.default_provider = Some(provider);
    config.default_account = Some(account_name);
    config.default_model = default_model;
    write_provider_config(client, &config)
}

/// Set a provider's API base URL in `lm.providers` (empty clears it).
pub fn set_provider_base_url(
    client: &dyn ConfigClient,
    provider: &str,
    account: &str,
    url: &str,
) -> Result<(), String> {
    let url = url.trim().to_string();
    update_provider_account(client, provider, account, move |account| {
        account.api_base_url = (!url.is_empty()).then_some(url);
    })
}

/// Advance a provider's reasoning effort to the next option (default → low → medium → high → max →
/// default), stored in the provider account options.
pub fn cycle_provider_reasoning_effort(
    client: &dyn ConfigClient,
    provider: &str,
    account: &str,
) -> Result<(), String> {
    update_provider_account(client, provider, account, |account| {
        account.options.reasoning_effort = next_reasoning_effort(account.options.reasoning_effort);
    })
}

/// The reasoning-effort options offered in the Settings Select, in order. Index 0 (`"default"`)
/// clears the override; the rest map to a concrete [`ProviderReasoningEffort`].
pub const REASONING_EFFORT_OPTIONS: &[&str] = &[
    "default", "minimal", "low", "medium", "high", "xhigh", "max",
];

pub const TEXT_VERBOSITY_OPTIONS: &[&str] = &["default", "low", "medium", "high"];

pub fn text_verbosity_index(verbosity: Option<&str>) -> usize {
    verbosity
        .and_then(|value| {
            TEXT_VERBOSITY_OPTIONS
                .iter()
                .position(|option| *option == value)
        })
        .unwrap_or(0)
}

/// Resolve a provider form's current reasoning effort to its index into
/// [`REASONING_EFFORT_OPTIONS`] (0 = default / unset).
pub fn reasoning_effort_index(effort: Option<&str>) -> usize {
    match effort {
        None => 0,
        Some(value) => REASONING_EFFORT_OPTIONS
            .iter()
            .position(|opt| *opt == value)
            .unwrap_or(0),
    }
}

/// Set a provider's reasoning effort to the option at `index` into [`REASONING_EFFORT_OPTIONS`]
/// (index 0 clears the override). Stored in the provider account options.
pub fn set_provider_effort(
    client: &dyn ConfigClient,
    provider: &str,
    account: &str,
    index: usize,
) -> Result<(), String> {
    let effort = match REASONING_EFFORT_OPTIONS.get(index) {
        Some(&"default") | None => None,
        Some(value) => Some(value.parse::<ProviderReasoningEffort>()?),
    };
    update_provider_account(client, provider, account, move |account| {
        account.options.reasoning_effort = effort;
    })
}

pub fn set_provider_text_verbosity(
    client: &dyn ConfigClient,
    provider: &str,
    account: &str,
    index: usize,
) -> Result<(), String> {
    let verbosity = match TEXT_VERBOSITY_OPTIONS.get(index) {
        Some(&"default") | None => None,
        Some(value) => Some(value.parse::<ProviderTextVerbosity>()?),
    };
    update_provider_account(client, provider, account, move |account| {
        account.options.text_verbosity = verbosity;
    })
}

pub fn set_provider_reasoning_budget(
    client: &dyn ConfigClient,
    provider: &str,
    account: &str,
    raw: &str,
) -> Result<(), String> {
    let value = raw.trim();
    let budget = if value.is_empty() {
        None
    } else {
        Some(
            value
                .parse::<u32>()
                .map_err(|_| "reasoning budget must be a positive whole number".to_string())?,
        )
    };
    if budget == Some(0) {
        return Err("reasoning budget must be greater than zero".to_string());
    }
    update_provider_account(client, provider, account, move |account| {
        account.options.reasoning_budget_tokens = budget;
    })
}

pub fn set_provider_vendor_options(
    client: &dyn ConfigClient,
    provider: &str,
    account: &str,
    raw: &str,
) -> Result<(), String> {
    let value = raw.trim();
    let options = if value.is_empty() {
        std::collections::BTreeMap::new()
    } else {
        serde_json::from_str::<std::collections::BTreeMap<String, String>>(value).map_err(
            |error| format!("provider options must be a JSON object of strings: {error}"),
        )?
    };
    update_provider_account(client, provider, account, move |account| {
        account.options.vendor_options = options;
    })
}

fn next_reasoning_effort(
    current: Option<ProviderReasoningEffort>,
) -> Option<ProviderReasoningEffort> {
    use ProviderReasoningEffort::{High, Low, Max, Medium};
    match current {
        None => Some(Low),
        Some(Low) => Some(Medium),
        Some(Medium) => Some(High),
        Some(High) => Some(Max),
        _ => None,
    }
}

/// Store an API-key credential on a specific provider account.
pub fn set_provider_api_key(
    client: &dyn ConfigClient,
    provider: &str,
    account: &str,
    api_key: &str,
) -> Result<(), String> {
    let api_key = api_key.trim();
    if api_key.is_empty() {
        return Err("API key cannot be empty".to_string());
    }
    let api_key = api_key.to_string();
    update_provider_account(client, provider, account, move |account| {
        account.api_key = Some(api_key);
    })
}

/// Add a provider account and write its initial credentials/defaults.
pub fn add_provider_account(
    client: &dyn ConfigClient,
    provider: &str,
    account: &str,
    api_key: &str,
    model: &str,
    api_base_url: &str,
) -> Result<(), String> {
    let provider = normalize_configurable_provider(provider)?;
    let account_name = normalize_provider_account_name(Some(account));
    let uses_device_login = sinomo_cli::provider_uses_device_login(&provider);
    let api_key = api_key.trim().to_string();
    if !uses_device_login && api_key.is_empty() {
        return Err("API key cannot be empty".to_string());
    }

    let mut config = load_provider_config(client).unwrap_or_default();
    if config.accounts.iter().any(|candidate| {
        normalize_model_provider(&candidate.provider).as_deref() == Some(provider.as_str())
            && normalize_provider_account_name(Some(&candidate.account)) == account_name
    }) {
        return Err(format!(
            "{} already has an account named {}",
            provider_display_name(&provider),
            account_name
        ));
    }

    let account_config = ProviderAccountConfig {
        provider: provider.clone(),
        account: account_name.clone(),
        default_model: optional_trimmed(model),
        api_base_url: optional_trimmed(api_base_url),
        api_key: (!uses_device_login).then_some(api_key),
        enabled: true,
        options: LanguageModelProviderOptions::default(),
        model_options: Vec::new(),
    };
    upsert_provider_account_config(&mut config, account_config).map_err(|e| e.to_string())?;
    if config.default_provider.is_none() {
        config.default_provider = Some(provider.clone());
    }
    if config.default_account.is_none() {
        config.default_account = Some(account_name.clone());
    }
    if config.default_model.is_none()
        && let Some(account) = config.accounts.iter().find(|candidate| {
            candidate.provider == provider
                && normalize_provider_account_name(Some(&candidate.account)) == account_name
        })
    {
        config.default_model = account.default_model.clone();
    }
    write_provider_config(client, &config)
}

/// Load `lm.providers`, mutate (or create) the account for `provider`/`account`, and write it back.
fn update_provider_account(
    client: &dyn ConfigClient,
    provider: &str,
    account: &str,
    edit: impl FnOnce(&mut ProviderAccountConfig),
) -> Result<(), String> {
    let provider = normalize_configurable_provider(provider)?;
    let account_name = normalize_provider_account_name(Some(account));
    let mut config = load_provider_config(client).unwrap_or_default();
    let mut account = config
        .accounts
        .iter()
        .find(|a| {
            normalize_model_provider(&a.provider).as_deref() == Some(provider.as_str())
                && normalize_provider_account_name(Some(&a.account)) == account_name
        })
        .cloned()
        .unwrap_or_else(|| ProviderAccountConfig {
            provider: provider.clone(),
            account: account_name.clone(),
            default_model: None,
            api_base_url: None,
            api_key: None,
            enabled: true,
            options: LanguageModelProviderOptions::default(),
            model_options: Vec::new(),
        });
    edit(&mut account);
    upsert_provider_account_config(&mut config, account).map_err(|e| e.to_string())?;
    write_provider_config(client, &config)
}

fn normalize_configurable_provider(provider: &str) -> Result<String, String> {
    let provider = normalize_model_provider(provider)
        .ok_or_else(|| format!("unknown provider `{}`", provider.trim()))?;
    if CONFIGURABLE_PROVIDERS.contains(&provider.as_str()) {
        Ok(provider)
    } else {
        Err(format!(
            "provider `{provider}` is not configurable in the app"
        ))
    }
}

fn optional_trimmed(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_string())
}

fn write_provider_config(client: &dyn ConfigClient, config: &ModelConfig) -> Result<(), String> {
    let value = serde_json::to_value(&config).map_err(|e| e.to_string())?;
    client
        .set(LM_PROVIDER_CONFIG_KEY, value, ConfigWriteTarget::Auto)
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

fn scope_label(mode: ConfigClusterMode) -> &'static str {
    match mode {
        ConfigClusterMode::LocalOnly => "this device",
        ConfigClusterMode::LocalOverrideCluster => "device override",
        ConfigClusterMode::ClusterOverrideLocal => "cluster override",
        ConfigClusterMode::Synced => "synced",
        ConfigClusterMode::ClusterOnly => "cluster",
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
