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
    LM_PROVIDER_CONFIG_KEY, LanguageModelProviderOptions, ModelConfig, ProviderAccountConfig,
    ProviderReasoningEffort, model_supports_reasoning_effort, upsert_provider_account_config,
};
use sinomo_runtime::config::{
    ConfigClient, ConfigStore, ConfigValueSource, ConfigValueType, ConfigWriteTarget,
};

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
    pub description: String,
    pub value: String,
    pub source: &'static str,
    /// `Some` when the value is a boolean (rendered as a toggle).
    pub bool_value: Option<bool>,
    /// How the value is edited in the Advanced editor.
    pub edit_kind: ConfigEditKind,
    /// The full (non-elided) editable representation seeded into the inline editor.
    pub edit_value: String,
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
            description: def.description.clone(),
            value: compact(&value),
            source,
            bool_value,
            edit_kind,
            edit_value,
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

/// The providers offered in the chat composer: exactly the providers configured and
/// enabled in the cluster's `lm.providers` configuration (resolved cluster-over-local,
/// the same registry the Settings tab reads). Returns an empty list when the cluster
/// has no providers configured. Model lists are intentionally left empty here — they are
/// fetched on demand from `sinomo-lm` (the single source of truth for available models).
pub fn cluster_provider_summaries(client: &dyn ConfigClient) -> Vec<ProviderSummary> {
    let Some(config) = load_provider_config(client) else {
        return Vec::new();
    };

    let mut summaries: Vec<ProviderSummary> = Vec::new();
    for account in config.accounts.iter().filter(|account| account.enabled) {
        if summaries.iter().any(|p| p.id == account.provider) {
            continue; // one entry per provider (first enabled account wins)
        }
        summaries.push(ProviderSummary {
            id: account.provider.clone(),
            name: provider_display_name(&account.provider),
            models: Vec::new(),
        });
    }
    summaries
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
#[derive(Clone, Debug)]
pub struct ProviderForm {
    pub provider: String,
    pub display_name: String,
    pub enabled: bool,
    pub default_model: String,
    pub api_base_url: String,
    /// The configured reasoning effort (e.g. "high"), or `None` for the provider default.
    pub reasoning_effort: Option<String>,
    /// Whether reasoning effort applies to this provider/model at all.
    pub supports_reasoning: bool,
    /// Whether a local API-key credential is stored for this provider.
    pub has_api_key: bool,
    /// Whether this provider authenticates via OAuth device login (vs an API key).
    pub uses_device_login: bool,
    /// Whether the provider is authenticated (device-login tokens stored, or an API key set).
    pub signed_in: bool,
}

/// Build the per-provider configuration forms from `lm.providers` (+ local credential presence).
pub fn provider_forms(client: &dyn ConfigClient) -> Vec<ProviderForm> {
    let config = load_provider_config(client).unwrap_or_default();
    CONFIGURABLE_PROVIDERS
        .iter()
        .map(|&provider| {
            let account = config.accounts.iter().find(|a| a.provider == provider);
            let default_model = account
                .and_then(|a| a.default_model.clone())
                .unwrap_or_default();
            let uses_device_login = sinomo_cli::provider_uses_device_login(provider);
            let has_api_key = provider_has_api_key(client, provider);
            let signed_in = if uses_device_login {
                sinomo_cli::provider_signed_in(provider)
            } else {
                has_api_key
            };
            ProviderForm {
                provider: provider.to_string(),
                display_name: provider_display_name(provider),
                enabled: account.map(|a| a.enabled).unwrap_or(false),
                supports_reasoning: model_supports_reasoning_effort(provider, &default_model),
                default_model,
                api_base_url: account
                    .and_then(|a| a.api_base_url.clone())
                    .unwrap_or_default(),
                reasoning_effort: account
                    .and_then(|a| a.options.reasoning_effort)
                    .map(|e| e.as_config_value().to_string()),
                has_api_key,
                uses_device_login,
                signed_in,
            }
        })
        .collect()
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

fn provider_credentials_key(provider: &str) -> String {
    format!("lm.credentials.{provider}")
}

/// Whether a non-empty local API-key credential is stored for `provider`.
fn provider_has_api_key(client: &dyn ConfigClient, provider: &str) -> bool {
    client
        .get(&provider_credentials_key(provider))
        .ok()
        .and_then(|res| {
            res.value
                .get("api_key")
                .and_then(|v| v.as_str())
                .map(|s| !s.trim().is_empty())
        })
        .unwrap_or(false)
}

/// Toggle a provider account enabled/disabled in `lm.providers`.
pub fn set_provider_enabled(
    client: &dyn ConfigClient,
    provider: &str,
    enabled: bool,
) -> Result<(), String> {
    update_provider_account(client, provider, |account| account.enabled = enabled)
}

/// Set a provider's default model in `lm.providers` (empty clears it).
pub fn set_provider_model(
    client: &dyn ConfigClient,
    provider: &str,
    model: &str,
) -> Result<(), String> {
    let model = model.trim().to_string();
    update_provider_account(client, provider, move |account| {
        account.default_model = (!model.is_empty()).then_some(model);
    })
}

/// Set a provider's API base URL in `lm.providers` (empty clears it).
pub fn set_provider_base_url(
    client: &dyn ConfigClient,
    provider: &str,
    url: &str,
) -> Result<(), String> {
    let url = url.trim().to_string();
    update_provider_account(client, provider, move |account| {
        account.api_base_url = (!url.is_empty()).then_some(url);
    })
}

/// Advance a provider's reasoning effort to the next option (default → low → medium → high → max →
/// default), stored in the provider account options.
pub fn cycle_provider_reasoning_effort(
    client: &dyn ConfigClient,
    provider: &str,
) -> Result<(), String> {
    update_provider_account(client, provider, |account| {
        account.options.reasoning_effort = next_reasoning_effort(account.options.reasoning_effort);
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

/// Store a provider's API key as a local-only credential (`lm.credentials.<provider>`). Keys are
/// never written to `lm.providers` (cluster-shareable) — they stay node-local.
pub fn set_provider_api_key(
    client: &dyn ConfigClient,
    provider: &str,
    api_key: &str,
) -> Result<(), String> {
    let api_key = api_key.trim();
    if api_key.is_empty() {
        return Err("API key cannot be empty".to_string());
    }
    let created_at_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let value = serde_json::json!({
        "api_key": api_key,
        "source": "manual",
        "created_at_unix": created_at_unix,
    });
    client
        .set(
            &provider_credentials_key(provider),
            value,
            ConfigWriteTarget::Auto,
        )
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// Load `lm.providers`, mutate (or create) the account for `provider`, and write it back.
fn update_provider_account(
    client: &dyn ConfigClient,
    provider: &str,
    edit: impl FnOnce(&mut ProviderAccountConfig),
) -> Result<(), String> {
    let mut config = load_provider_config(client).unwrap_or_default();
    let mut account = config
        .accounts
        .iter()
        .find(|a| a.provider == provider)
        .cloned()
        .unwrap_or_else(|| ProviderAccountConfig {
            provider: provider.to_string(),
            account: "default".to_string(),
            default_model: None,
            api_base_url: None,
            api_key: None,
            enabled: true,
            options: LanguageModelProviderOptions::default(),
            model_options: Vec::new(),
        });
    edit(&mut account);
    upsert_provider_account_config(&mut config, account).map_err(|e| e.to_string())?;
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
