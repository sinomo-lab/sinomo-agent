//! Model settings, provider identities and value validation shared by app and daemon.
//! Provider HTTP clients, credentials and execution remain in sinomo-lm.

use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::str::FromStr;
use thiserror::Error;

pub const ANTHROPIC_DEFAULT_MODEL: &str = "claude-sonnet-4-20250514";
pub const ANTHROPIC_PROVIDER_ID: &str = "anthropic";
pub const CLAUDE_CODE_PROVIDER_ID: &str = "claude-code";
pub const CODEX_CLI_PROVIDER_ID: &str = "codex-cli";
pub const DEEPSEEK_PROVIDER_ID: &str = "deepseek";
pub const GEMINI_DEFAULT_MODEL: &str = "gemini-2.5-flash";
pub const GEMINI_PROVIDER_ID: &str = "gemini";
pub const GITHUB_COPILOT_CLI_DEFAULT_MODEL: &str = "copilot-default";
pub const GITHUB_COPILOT_CLI_PROVIDER_ID: &str = "github-copilot-cli";
pub const GITHUB_COPILOT_DEFAULT_MODEL: &str = "gpt-5.4";
pub const GITHUB_COPILOT_PROVIDER_ID: &str = "github-copilot";
pub const NVIDIA_PROVIDER_ID: &str = "nvidia";
pub const OPENAI_PROVIDER_ID: &str = "openai";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LanguageModelProviderUsage {
    pub provider: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan: Option<String>,
    pub captured_at_unix: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub limits: Vec<LanguageModelUsageLimit>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credits: Option<LanguageModelCreditUsage>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LanguageModelUsageLimit {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unlimited: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allowed: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exhausted: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remaining: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entitlement: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub used_percent: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remaining_percent: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window_seconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reset_at_unix: Option<i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LanguageModelCreditUsage {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub has_credits: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unlimited: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub balance: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Default)]
pub struct ModelConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_account: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_model: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub accounts: Vec<ProviderAccountConfig>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ProviderAccountConfig {
    pub provider: String,
    #[serde(default = "default_provider_account_name")]
    pub account: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_base_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key: Option<String>,
    #[serde(default = "provider_account_enabled_default")]
    pub enabled: bool,
    #[serde(
        default,
        skip_serializing_if = "LanguageModelProviderOptions::is_default"
    )]
    pub options: LanguageModelProviderOptions,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub model_options: Vec<LanguageModelModelOptions>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct LanguageModelProviderOptions {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<ProviderReasoningEffort>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_budget_tokens: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text_verbosity: Option<ProviderTextVerbosity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_context_tokens: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_compact_token_limit: Option<u32>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub vendor_options: BTreeMap<String, String>,
}

impl LanguageModelProviderOptions {
    pub fn is_default(&self) -> bool {
        self == &Self::default()
    }

    pub fn merge(&self, overrides: &Self) -> Self {
        let mut merged = self.clone();
        if overrides.reasoning_effort.is_some() {
            merged.reasoning_effort = overrides.reasoning_effort;
        }
        if overrides.reasoning_budget_tokens.is_some() {
            merged.reasoning_budget_tokens = overrides.reasoning_budget_tokens;
        }
        if overrides.text_verbosity.is_some() {
            merged.text_verbosity = overrides.text_verbosity;
        }
        if overrides.max_context_tokens.is_some() {
            merged.max_context_tokens = overrides.max_context_tokens;
        }
        if overrides.auto_compact_token_limit.is_some() {
            merged.auto_compact_token_limit = overrides.auto_compact_token_limit;
        }
        for (key, value) in &overrides.vendor_options {
            merged.vendor_options.insert(key.clone(), value.clone());
        }
        merged
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct LanguageModelModelOptions {
    pub model: String,
    #[serde(
        default,
        skip_serializing_if = "LanguageModelProviderOptions::is_default"
    )]
    pub options: LanguageModelProviderOptions,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ProviderReasoningEffort {
    None,
    Minimal,
    Low,
    Medium,
    High,
    #[serde(rename = "xhigh", alias = "x-high", alias = "extra-high")]
    XHigh,
    Max,
}

impl ProviderReasoningEffort {
    pub const VALUES: &'static [&'static str] =
        &["none", "minimal", "low", "medium", "high", "xhigh", "max"];

    pub fn as_config_value(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Minimal => "minimal",
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::XHigh => "xhigh",
            Self::Max => "max",
        }
    }

    pub fn as_openai_responses_value(self) -> Option<&'static str> {
        match self {
            Self::None => Some("none"),
            Self::Minimal => Some("minimal"),
            Self::Low => Some("low"),
            Self::Medium => Some("medium"),
            Self::High => Some("high"),
            Self::XHigh | Self::Max => Some("xhigh"),
        }
    }

    pub fn as_codex_cli_value(self) -> Option<&'static str> {
        match self {
            Self::None => Some("none"),
            Self::Minimal => Some("minimal"),
            Self::Low => Some("low"),
            Self::Medium => Some("medium"),
            Self::High => Some("high"),
            Self::XHigh | Self::Max => Some("xhigh"),
        }
    }

    pub fn as_claude_code_value(self) -> Option<&'static str> {
        match self {
            Self::None => None,
            Self::Minimal | Self::Low => Some("low"),
            Self::Medium => Some("medium"),
            Self::High => Some("high"),
            Self::XHigh => Some("xhigh"),
            Self::Max => Some("max"),
        }
    }

    pub fn as_anthropic_effort_value(self) -> Option<&'static str> {
        match self {
            Self::None => None,
            Self::Minimal | Self::Low => Some("low"),
            Self::Medium => Some("medium"),
            Self::High => Some("high"),
            Self::XHigh => Some("xhigh"),
            Self::Max => Some("max"),
        }
    }
}

impl std::fmt::Display for ProviderReasoningEffort {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_config_value())
    }
}

impl FromStr for ProviderReasoningEffort {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "none" | "off" | "disabled" => Ok(Self::None),
            "minimal" | "min" => Ok(Self::Minimal),
            "low" => Ok(Self::Low),
            "medium" | "med" => Ok(Self::Medium),
            "high" => Ok(Self::High),
            "xhigh" | "x-high" | "extra-high" => Ok(Self::XHigh),
            "max" | "maximum" => Ok(Self::Max),
            _ => Err(format!(
                "invalid reasoning effort `{value}`; expected one of {}",
                Self::VALUES.join("|")
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ProviderTextVerbosity {
    Low,
    Medium,
    High,
}

impl ProviderTextVerbosity {
    pub const VALUES: &'static [&'static str] = &["low", "medium", "high"];

    pub fn as_config_value(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
        }
    }
}

impl std::fmt::Display for ProviderTextVerbosity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_config_value())
    }
}

impl FromStr for ProviderTextVerbosity {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "low" => Ok(Self::Low),
            "medium" | "med" => Ok(Self::Medium),
            "high" => Ok(Self::High),
            _ => Err(format!(
                "invalid text verbosity `{value}`; expected one of {}",
                Self::VALUES.join("|")
            )),
        }
    }
}

pub const DEFAULT_PROVIDER_ACCOUNT: &str = "default";

pub const LM_PROVIDER_CONFIG_KEY: &str = "lm.providers";

pub const LM_PROVIDER_HEALTH_CONFIG_KEY: &str = "lm.provider_health";

fn default_provider_account_name() -> String {
    DEFAULT_PROVIDER_ACCOUNT.to_string()
}

fn provider_account_enabled_default() -> bool {
    true
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct LanguageModelProviderHealthConfig {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub accounts: BTreeMap<String, LanguageModelProviderHealthSnapshot>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LanguageModelProviderHealthSnapshot {
    pub provider: String,
    pub account: String,
    pub checked_at_unix: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_verified_at_unix: Option<i64>,
    pub healthy: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub models: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_discovery_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<LanguageModelProviderUsage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage_error: Option<String>,
    #[serde(default)]
    pub stale: bool,
}

pub fn provider_health_account_key(provider: &str, account: &str) -> String {
    let provider =
        normalize_model_provider(provider).unwrap_or_else(|| provider.trim().to_string());
    let account = normalize_provider_account_name(Some(account));
    format!("{provider}:{account}")
}

pub fn normalize_provider_account_name(account: Option<&str>) -> String {
    account
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(DEFAULT_PROVIDER_ACCOUNT)
        .to_string()
}

pub fn upsert_provider_account_config(
    config: &mut ModelConfig,
    account: ProviderAccountConfig,
) -> Result<(), LanguageModelError> {
    let provider = normalize_model_provider(&account.provider).ok_or_else(|| {
        LanguageModelError::ProviderInvocationFailed {
            provider: account.provider.clone(),
            model: PROVIDER_DEFAULT_MODEL.to_string(),
            detail: format!("unknown provider `{}`", account.provider),
        }
    })?;
    let mut account = ProviderAccountConfig {
        provider,
        account: normalize_provider_account_name(Some(&account.account)),
        ..account
    };
    account.default_model = account
        .default_model
        .map(|model| normalize_model_id_for_provider(&account.provider, &model));
    if let Some(existing) = config.accounts.iter_mut().find(|existing| {
        existing.provider == account.provider
            && normalize_provider_account_name(Some(&existing.account)) == account.account
    }) {
        *existing = account;
    } else {
        config.accounts.push(account);
    }
    Ok(())
}

pub fn effective_model_name(provider: &str, configured_model: &str) -> String {
    if configured_model != PROVIDER_DEFAULT_MODEL {
        return normalize_model_id_for_provider(provider, configured_model);
    }
    match provider {
        GITHUB_COPILOT_PROVIDER_ID => GITHUB_COPILOT_DEFAULT_MODEL.to_string(),
        GITHUB_COPILOT_CLI_PROVIDER_ID => GITHUB_COPILOT_CLI_DEFAULT_MODEL.to_string(),
        CODEX_CLI_PROVIDER_ID => CODEX_CLI_DEFAULT_MODEL.to_string(),
        CODEX_PROVIDER_ID => CODEX_DEFAULT_MODEL.to_string(),
        CLAUDE_PROVIDER_ID => CLAUDE_DEFAULT_MODEL.to_string(),
        CLAUDE_CODE_PROVIDER_ID => CLAUDE_CODE_DEFAULT_MODEL.to_string(),
        ANTHROPIC_PROVIDER_ID => ANTHROPIC_DEFAULT_MODEL.to_string(),
        OPENAI_PROVIDER_ID => OPENAI_DEFAULT_MODEL.to_string(),
        OLLAMA_PROVIDER_ID => OLLAMA_DEFAULT_MODEL.to_string(),
        LM_STUDIO_PROVIDER_ID => LM_STUDIO_DEFAULT_MODEL.to_string(),
        DEEPSEEK_PROVIDER_ID => DEEPSEEK_DEFAULT_MODEL.to_string(),
        GEMINI_PROVIDER_ID => GEMINI_DEFAULT_MODEL.to_string(),
        NVIDIA_PROVIDER_ID => NVIDIA_DEFAULT_MODEL.to_string(),
        _ => configured_model.to_string(),
    }
}

pub fn normalize_model_id_for_provider(provider: &str, model: &str) -> String {
    match provider {
        NVIDIA_PROVIDER_ID => normalize_nvidia_model_id(model),
        _ => model.to_string(),
    }
}

pub fn normalize_model_provider(provider: &str) -> Option<String> {
    match provider {
        GITHUB_COPILOT_PROVIDER_ID => Some(GITHUB_COPILOT_PROVIDER_ID.to_string()),
        GITHUB_COPILOT_CLI_PROVIDER_ID => Some(GITHUB_COPILOT_CLI_PROVIDER_ID.to_string()),
        CODEX_CLI_PROVIDER_ID => Some(CODEX_CLI_PROVIDER_ID.to_string()),
        CODEX_PROVIDER_ID => Some(CODEX_PROVIDER_ID.to_string()),
        CLAUDE_PROVIDER_ID | "claude-subscription" | "anthropic-oauth" => {
            Some(CLAUDE_PROVIDER_ID.to_string())
        }
        CLAUDE_CODE_PROVIDER_ID | "anthropic-claude-code" => {
            Some(CLAUDE_CODE_PROVIDER_ID.to_string())
        }
        ANTHROPIC_PROVIDER_ID => Some(ANTHROPIC_PROVIDER_ID.to_string()),
        OPENAI_PROVIDER_ID | "openai-api" => Some(OPENAI_PROVIDER_ID.to_string()),
        OLLAMA_PROVIDER_ID => Some(OLLAMA_PROVIDER_ID.to_string()),
        LM_STUDIO_PROVIDER_ID | "lm-studio" => Some(LM_STUDIO_PROVIDER_ID.to_string()),
        DEEPSEEK_PROVIDER_ID | "deepseek-api" => Some(DEEPSEEK_PROVIDER_ID.to_string()),
        GEMINI_PROVIDER_ID | "google-gemini" => Some(GEMINI_PROVIDER_ID.to_string()),
        NVIDIA_PROVIDER_ID | "nvidia-nim" => Some(NVIDIA_PROVIDER_ID.to_string()),
        _ => None,
    }
}

/// Whether a reasoning-effort setting is meaningful for `provider`/`model`.
///
/// This is the single source of truth for reasoning-effort applicability and is kept
/// consistent with the per-provider request builders — only the providers whose modules
/// actually apply a [`ProviderReasoningEffort`] (anthropic, openai, codex, codex-cli,
/// claude-code, github-copilot) can return `true`, and within those it is further gated
/// to the models that support reasoning. UIs use this to decide whether to surface a
/// reasoning-effort control at all.
pub fn model_supports_reasoning_effort(provider: &str, model: &str) -> bool {
    let model = model.to_ascii_lowercase();
    // OpenAI-style reasoning model families (also the ones Copilot proxies).
    let openai_reasoning = model.contains("gpt-5")
        || model.contains("codex")
        || model.starts_with("o1")
        || model.starts_with("o3")
        || model.starts_with("o4");

    match provider {
        // Agent CLIs that always run a reasoning model.
        CODEX_CLI_PROVIDER_ID | CODEX_PROVIDER_ID | CLAUDE_CODE_PROVIDER_ID => true,
        // Anthropic extended-thinking models (the Claude 4 family).
        ANTHROPIC_PROVIDER_ID => {
            model.contains("opus") || model.contains("sonnet") || model.contains("haiku-4")
        }
        // OpenAI: reasoning models only (not gpt-4*, etc.).
        OPENAI_PROVIDER_ID => openai_reasoning,
        // Copilot proxies many backends; gate on the underlying reasoning model.
        GITHUB_COPILOT_PROVIDER_ID | GITHUB_COPILOT_CLI_PROVIDER_ID => {
            openai_reasoning || model.contains("claude") || model.contains("gemini-2.5")
        }
        // gemini, deepseek, nvidia, and unknown providers do not take a reasoning effort.
        _ => false,
    }
}

pub const PROVIDER_DEFAULT_MODEL: &str = "provider-default";

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum LanguageModelError {
    #[error("failed to serialize model request: {detail}")]
    RequestSerialization { detail: String },
    #[error("authentication required for {provider}: {detail}")]
    AuthenticationRequired { provider: String, detail: String },
    #[error("unsupported token for {provider}: {detail}")]
    UnsupportedToken { provider: String, detail: String },
    #[error("token store error at {path}: {detail}")]
    TokenStore { path: String, detail: String },
    #[error("model config store error at {path}: {detail}")]
    ConfigStore { path: String, detail: String },
    #[error("failed to spawn provider {provider} command {program}: {detail}")]
    ProviderSpawnFailed {
        provider: String,
        program: String,
        detail: String,
    },
    #[error("model invocation failed for {provider}/{model}: {detail}")]
    ProviderInvocationFailed {
        provider: String,
        model: String,
        detail: String,
    },
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum LanguageModelErrorCategory {
    PromptTooLarge,
    OutputTooLarge,
    RateLimit,
    Authentication,
    MalformedResponse,
    UnsupportedToolFormat,
    RepairNeeded,
    ProviderUnavailable,
    ProviderUnsupported,
    Config,
    Unknown,
}

impl LanguageModelError {
    pub fn category(&self) -> LanguageModelErrorCategory {
        match self {
            Self::RequestSerialization { detail } => classify_language_model_error_detail(detail)
                .unwrap_or(LanguageModelErrorCategory::MalformedResponse),
            Self::AuthenticationRequired { .. } | Self::UnsupportedToken { .. } => {
                LanguageModelErrorCategory::Authentication
            }
            Self::TokenStore { .. } | Self::ConfigStore { .. } => {
                LanguageModelErrorCategory::Config
            }
            Self::ProviderSpawnFailed { .. } => LanguageModelErrorCategory::ProviderUnavailable,
            Self::ProviderInvocationFailed { detail, .. } => {
                classify_language_model_error_detail(detail)
                    .unwrap_or(LanguageModelErrorCategory::Unknown)
            }
        }
    }
}

pub fn classify_language_model_error_detail(detail: &str) -> Option<LanguageModelErrorCategory> {
    let detail = detail.to_ascii_lowercase();
    if detail.contains("repair") {
        return Some(LanguageModelErrorCategory::RepairNeeded);
    }
    if detail.contains("rate limit")
        || detail.contains("too many requests")
        || detail.contains("http 429")
        || detail.contains("status 429")
    {
        return Some(LanguageModelErrorCategory::RateLimit);
    }
    if detail.contains("max output")
        || detail.contains("output too large")
        || detail.contains("too many output tokens")
        || detail.contains("completion token limit")
    {
        return Some(LanguageModelErrorCategory::OutputTooLarge);
    }
    if detail.contains("context_length")
        || detail.contains("context length")
        || detail.contains("maximum context")
        || detail.contains("input too large")
        || detail.contains("prompt too large")
        || detail.contains("too many input tokens")
        || detail.contains("token limit")
    {
        return Some(LanguageModelErrorCategory::PromptTooLarge);
    }
    if detail.contains("api key")
        || detail.contains("unauthorized")
        || detail.contains("forbidden")
        || detail.contains("authentication")
        || detail.contains("auth")
        || detail.contains("http 401")
        || detail.contains("http 403")
        || detail.contains("status 401")
        || detail.contains("status 403")
    {
        return Some(LanguageModelErrorCategory::Authentication);
    }
    if detail.contains("unknown language model provider")
        || detail.contains("unsupported provider")
        || detail.contains("unknown provider")
    {
        return Some(LanguageModelErrorCategory::ProviderUnsupported);
    }
    if detail.contains("tool")
        && (detail.contains("unsupported")
            || detail.contains("not supported")
            || detail.contains("does not support"))
    {
        return Some(LanguageModelErrorCategory::UnsupportedToolFormat);
    }
    if detail.contains("invalid json")
        || detail.contains("malformed")
        || detail.contains("failed to parse")
        || detail.contains("missing field")
        || detail.contains("no choices")
        || detail.contains("no output")
    {
        return Some(LanguageModelErrorCategory::MalformedResponse);
    }
    if detail.contains("connection refused")
        || detail.contains("connection reset")
        || detail.contains("timed out")
        || detail.contains("timeout")
        || detail.contains("dns")
        || detail.contains("network")
        || detail.contains("failed to send")
    {
        return Some(LanguageModelErrorCategory::ProviderUnavailable);
    }
    None
}

pub const NVIDIA_DEFAULT_MODEL: &str = "nvidia/nemotron-3-super-120b-a12b";

pub fn normalize_nvidia_model_id(model: &str) -> String {
    let mut normalized = model.trim().to_string();
    while let Some(rest) = normalized.strip_prefix("nvidia/nvidia/") {
        normalized = format!("nvidia/{rest}");
    }
    normalized
}

pub const DEEPSEEK_V4_FLASH_MODEL: &str = "deepseek-v4-flash";

pub const DEEPSEEK_DEFAULT_MODEL: &str = DEEPSEEK_V4_FLASH_MODEL;

pub const OLLAMA_PROVIDER_ID: &str = "ollama";

pub const LM_STUDIO_PROVIDER_ID: &str = "lmstudio";

pub const OLLAMA_DEFAULT_MODEL: &str = "gpt-oss:20b";

pub const LM_STUDIO_DEFAULT_MODEL: &str = "openai/gpt-oss-20b";

pub const OPENAI_DEFAULT_MODEL: &str = "gpt-5.6-sol";

pub const CLAUDE_CODE_DEFAULT_MODEL: &str = "claude-code-default";

pub const CLAUDE_PROVIDER_ID: &str = "claude";

pub const CLAUDE_DEFAULT_MODEL: &str = "claude-sonnet-4-20250514";

pub const CODEX_DEFAULT_MODEL: &str = OPENAI_DEFAULT_MODEL;

pub const CODEX_PROVIDER_ID: &str = "codex";

pub const CODEX_CLI_DEFAULT_MODEL: &str = "codex-default";
