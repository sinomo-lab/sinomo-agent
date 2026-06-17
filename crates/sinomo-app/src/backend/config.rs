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
    DEFAULT_PROVIDER_ACCOUNT, LM_PROVIDER_CONFIG_KEY, LanguageModelProviderOptions, ModelConfig,
    ProviderAccountConfig, ProviderReasoningEffort, model_supports_reasoning_effort,
    normalize_model_provider, normalize_provider_account_name, upsert_provider_account_config,
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
    fn disabling_one_provider_account_keeps_other_accounts_available() {
        let (client, _root) = test_client("disable-one-account");
        add_provider_account(&client, "openai", "work", "sk-work", "gpt-5", "")
            .expect("add work account");
        add_provider_account(&client, "openai", "personal", "sk-personal", "gpt-5-mini", "")
            .expect("add personal account");

        set_provider_enabled(&client, "openai", "work", false).expect("disable work");

        let summaries = cluster_provider_summaries(&client);
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].account, "personal");
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
        .map(|provider| sinomo_lm::effective_model_name(&provider, sinomo_lm::PROVIDER_DEFAULT_MODEL))
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
#[derive(Clone, Debug)]
pub struct ProviderForm {
    pub provider: String,
    pub account: String,
    pub display_name: String,
    pub account_label: String,
    pub enabled: bool,
    pub default_model: String,
    pub api_base_url: String,
    /// The configured reasoning effort (e.g. "high"), or `None` for the provider default.
    pub reasoning_effort: Option<String>,
    /// Whether reasoning effort applies to this provider/model at all.
    pub supports_reasoning: bool,
    /// Whether an API-key credential is stored for this account.
    pub has_api_key: bool,
    /// Whether this provider authenticates via OAuth device login (vs an API key).
    pub uses_device_login: bool,
    /// Whether the provider is authenticated (device-login tokens stored, or an API key set).
    pub signed_in: bool,
}

/// Build the per-account configuration forms from `lm.providers` (+ legacy credential presence).
pub fn provider_forms(client: &dyn ConfigClient) -> Vec<ProviderForm> {
    let config = load_provider_config(client).unwrap_or_default();
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
        let has_legacy_api_key = account_name == DEFAULT_PROVIDER_ACCOUNT
            && provider_has_legacy_api_key(client, &provider);
        let has_api_key = has_inline_api_key || has_legacy_api_key;
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
            enabled: account.enabled,
            supports_reasoning: model_supports_reasoning_effort(&provider, &default_model),
            default_model,
            api_base_url: account.api_base_url.clone().unwrap_or_default(),
            reasoning_effort: account
                .options
                .reasoning_effort
                .map(|e| e.as_config_value().to_string()),
            has_api_key,
            uses_device_login,
            signed_in,
        });
    }
    forms
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

/// Whether a non-empty legacy provider-level API-key credential is stored for `provider`.
fn provider_has_legacy_api_key(client: &dyn ConfigClient, provider: &str) -> bool {
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
    account: &str,
    enabled: bool,
) -> Result<(), String> {
    update_provider_account(client, provider, account, |account| account.enabled = enabled)
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
        Err(format!("provider `{provider}` is not configurable in the app"))
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
