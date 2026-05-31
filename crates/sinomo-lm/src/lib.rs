//! Language model provider contracts and integrations.

mod anthropic;
mod api_key;
mod claude_code;
mod codex;
mod codex_cli;
mod deepseek;
mod gemini;
mod github_copilot;
mod nvidia;
mod openai;

pub use anthropic::{AnthropicConfig, AnthropicProvider};
pub use claude_code::{
    CLAUDE_CODE_DEFAULT_MODEL, ClaudeCodeConfig, ClaudeCodeProvider, ClaudePermissionMode,
};
pub use codex::{
    CODEX_AUTH_CLIENT_ID, CODEX_AUTH_ISSUER, CODEX_CHATGPT_API_BASE_URL, CODEX_DEFAULT_MODEL,
    CODEX_PROVIDER_ID, CodexChatGptTokens, CodexClient, CodexConfig, CodexContextManager,
    CodexDeviceCode, CodexDevicePoll, CodexMessage, CodexModelRequest, CodexModelResponse,
    CodexPromptBuilder, CodexProvider, CodexRole, CodexTool, CodexToolCall, CodexToolDescriptor,
    CodexToolRegistry, CodexToolResult, DefaultCodexContextManager, DefaultCodexPromptBuilder,
    OpenAiResponsesCodexClient, OpenAiResponsesCodexConfig, SINOMO_CODEX_CREDENTIALS_FILE_ENV_VAR,
};
pub use codex_cli::{
    CODEX_CLI_DEFAULT_MODEL, CodexCliConfig, CodexCliProvider, CodexColor, CodexSandboxMode,
};
pub use deepseek::{
    DEEPSEEK_DEFAULT_MODEL, DEEPSEEK_V4_FLASH_MODEL, DEEPSEEK_V4_PRO_MODEL, DeepSeekConfig,
    DeepSeekProvider,
};
pub use gemini::{GeminiConfig, GeminiProvider};
#[cfg(test)]
pub(crate) use gemini::{
    extract_gemini_response_text, extract_gemini_usage, gemini_generate_content_payload,
};
pub use github_copilot::{
    GITHUB_COPILOT_CLIENT_ID, GitHubCopilotCliConfig, GitHubCopilotCliProvider,
    GitHubCopilotConfig, GitHubCopilotProvider, GitHubDeviceCode, GitHubDevicePoll,
};
pub use nvidia::{NVIDIA_DEFAULT_MODEL, NvidiaConfig, NvidiaProvider, normalize_nvidia_model_id};
pub use openai::{OpenAiConfig, OpenAiProvider};

use serde::{Deserialize, Serialize};
use sinomo_runtime::kv::{KvEntry, KvOp, KvSnapshot, KvTxn, decode_kv_value};
use sinomo_runtime::local::SinomoDataDir;
use std::env;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LanguageModelRequest {
    pub provider: String,
    pub model: String,
    pub messages: Vec<LanguageModelMessage>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub items: Vec<LanguageModelRequestItem>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<LanguageModelToolSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<LanguageModelToolChoice>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parallel_tool_calls: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u32>,
    pub response_format: LanguageModelResponseFormat,
    #[serde(default)]
    pub metadata: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct LanguageModelMessage {
    pub role: LanguageModelRole,
    pub content: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LanguageModelRequestItem {
    Message {
        role: LanguageModelRole,
        content: String,
    },
    ToolCallOutput {
        call_id: String,
        name: String,
        #[serde(default)]
        output: serde_json::Value,
    },
    Context {
        label: String,
        content: String,
    },
}

impl LanguageModelRequestItem {
    pub fn message(role: LanguageModelRole, content: impl Into<String>) -> Self {
        Self::Message {
            role,
            content: content.into(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum LanguageModelRole {
    System,
    User,
    Assistant,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum LanguageModelResponseFormat {
    Text,
    JsonObject,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LanguageModelToolSpec {
    pub name: String,
    pub description: String,
    #[serde(default)]
    pub input_schema: serde_json::Value,
    #[serde(default)]
    pub output_schema: serde_json::Value,
    #[serde(default)]
    pub supports_parallel: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub provider_specs: Vec<LanguageModelProviderToolSpec>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LanguageModelProviderToolSpec {
    pub provider: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom: Option<LanguageModelCustomToolSpec>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LanguageModelCustomToolSpec {
    pub format: LanguageModelCustomToolFormat,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_input_argument: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LanguageModelCustomToolFormat {
    Grammar { syntax: String, definition: String },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LanguageModelToolChoice {
    Auto,
    None,
    Required,
    Tool { name: String },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LanguageModelToolCall {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub call_id: Option<String>,
    pub name: String,
    #[serde(default)]
    pub input: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LanguageModelResponseItem {
    Message {
        role: LanguageModelRole,
        content: String,
    },
    ReasoningSummary {
        summary: String,
    },
    ToolCall {
        call: LanguageModelToolCall,
    },
    ToolCallOutput {
        call_id: String,
        name: String,
        #[serde(default)]
        output: serde_json::Value,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LanguageModelResponse {
    pub provider: String,
    pub model: String,
    pub content: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub items: Vec<LanguageModelResponseItem>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<LanguageModelToolCall>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<LanguageModelUsage>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct LanguageModelUsage {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_tokens: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LanguageModelStreamEvent {
    Started {
        provider: String,
        model: String,
    },
    TextDelta {
        delta: String,
    },
    ReasoningSummary {
        summary: String,
    },
    ToolCallStarted {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        call_id: Option<String>,
        name: String,
    },
    ToolCallDelta {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        call_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        arguments_delta: String,
    },
    ToolCallCompleted {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        call_id: Option<String>,
        name: String,
        #[serde(default)]
        input: serde_json::Value,
    },
    Usage {
        usage: LanguageModelUsage,
    },
    Finished {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        finish_reason: Option<String>,
    },
    Error {
        detail: String,
    },
    Aborted {
        reason: String,
    },
}

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

pub trait LanguageModelUsageProvider {
    fn usage(&self) -> Result<LanguageModelProviderUsage, LanguageModelError>;
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
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

impl Default for ModelConfig {
    fn default() -> Self {
        Self {
            default_provider: None,
            default_account: None,
            default_model: None,
            accounts: Vec::new(),
        }
    }
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
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ProviderAccountInfo {
    pub provider: String,
    pub account: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_base_url: Option<String>,
    pub has_api_key: bool,
    pub enabled: bool,
}

pub const DEFAULT_PROVIDER_ACCOUNT: &str = "default";
pub const LM_PROVIDER_CONFIG_KV_KEY: &str = "/config/lm/providers";
pub const LM_RUNTIME_CONFIG_KV_KEY: &str = "/config/lm/runtime";

fn default_provider_account_name() -> String {
    DEFAULT_PROVIDER_ACCOUNT.to_string()
}

fn provider_account_enabled_default() -> bool {
    true
}

pub fn normalize_provider_account_name(account: Option<&str>) -> String {
    account
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(DEFAULT_PROVIDER_ACCOUNT)
        .to_string()
}

pub fn provider_accounts(config: &ModelConfig) -> Vec<ProviderAccountInfo> {
    config
        .accounts
        .iter()
        .map(|account| ProviderAccountInfo {
            provider: account.provider.clone(),
            account: normalize_provider_account_name(Some(&account.account)),
            default_model: account.default_model.clone(),
            api_base_url: account.api_base_url.clone(),
            has_api_key: account
                .api_key
                .as_deref()
                .is_some_and(|key| !key.trim().is_empty()),
            enabled: account.enabled,
        })
        .collect()
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ModelRuntimeConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_models_cache_dir: Option<String>,
    #[serde(default)]
    pub minicpm_v46: MiniCpmV46RuntimeConfig,
    #[serde(default)]
    pub pytorch_bridge: PytorchBridgeRuntimeConfig,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct MiniCpmV46RuntimeConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_dir: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_input_tokens: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<usize>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct PytorchBridgeRuntimeConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub python: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mock: Option<bool>,
}

pub fn inferred_model_context_window_tokens(provider: &str, model: &str) -> Option<usize> {
    let provider = normalize_model_provider(provider)?;
    let model = effective_model_name(&provider, model);
    match provider.as_str() {
        GitHubCopilotProvider::PROVIDER_ID | GitHubCopilotCliProvider::PROVIDER_ID => {
            github_copilot::inferred_context_window_tokens(&model)
        }
        CodexCliProvider::PROVIDER_ID => codex_cli::inferred_context_window_tokens(&model),
        CODEX_PROVIDER_ID => codex::inferred_context_window_tokens(&model),
        ClaudeCodeProvider::PROVIDER_ID => claude_code::inferred_context_window_tokens(&model),
        AnthropicProvider::PROVIDER_ID => anthropic::inferred_context_window_tokens(&model),
        OpenAiProvider::PROVIDER_ID => openai::inferred_context_window_tokens(&model),
        DeepSeekProvider::PROVIDER_ID => deepseek::inferred_context_window_tokens(&model),
        GeminiProvider::PROVIDER_ID => gemini::inferred_context_window_tokens(&model),
        NvidiaProvider::PROVIDER_ID => nvidia::inferred_context_window_tokens(&model),
        _ => None,
    }
}

pub(crate) fn parse_declared_context_window_tokens(model: &str) -> Option<usize> {
    model
        .to_ascii_lowercase()
        .split(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '.'))
        .filter_map(parse_context_token_count)
        .max()
}

fn parse_context_token_count(token: &str) -> Option<usize> {
    let (value, multiplier) = token
        .strip_suffix('k')
        .map(|value| (value, 1_000))
        .or_else(|| token.strip_suffix('m').map(|value| (value, 1_000_000)))?;
    let scaled = value.parse::<f64>().ok()? * multiplier as f64;
    if !scaled.is_finite() || scaled <= 0.0 {
        return None;
    }
    Some(scaled.round() as usize)
}

#[derive(Clone, Debug)]
pub struct ModelPaths {
    pub config: PathBuf,
}

impl ModelPaths {
    pub fn new(data_dir: &SinomoDataDir) -> Self {
        Self {
            config: data_dir.configs_dir().join("models.json"),
        }
    }

    pub fn ensure(&self) -> Result<(), LanguageModelError> {
        let Some(parent) = self.config.parent() else {
            return Ok(());
        };
        fs::create_dir_all(parent).map_err(|source| LanguageModelError::ConfigStore {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CurrentDefaultModel {
    pub provider: String,
    pub account: String,
    pub configured_model: String,
    pub model: String,
    pub config_path: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedDefaultModel {
    pub provider: String,
    pub account: String,
    pub configured_model: String,
    pub model: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DefaultModelConfigUpdate {
    pub config: ModelConfig,
    pub config_path: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LanguageModelProviderCatalog {
    pub provider: String,
    pub models: Vec<String>,
}

pub fn load_model_config(paths: &ModelPaths) -> Result<ModelConfig, LanguageModelError> {
    let content =
        fs::read_to_string(&paths.config).map_err(|source| LanguageModelError::ConfigStore {
            path: paths.config.display().to_string(),
            detail: source.to_string(),
        })?;
    serde_json::from_str(&content).map_err(|source| LanguageModelError::RequestSerialization {
        detail: format!(
            "failed to parse model config at {}: {source}",
            paths.config.display()
        ),
    })
}

pub fn model_config_from_cluster_kv_snapshot(
    snapshot: &KvSnapshot,
) -> Result<Option<ModelConfig>, LanguageModelError> {
    snapshot
        .entries
        .iter()
        .find(|entry| entry.key == LM_PROVIDER_CONFIG_KV_KEY)
        .map(decode_model_config_entry)
        .transpose()
}

pub fn model_config_from_cluster_kv_entries(
    entries: &[KvEntry],
) -> Result<Option<ModelConfig>, LanguageModelError> {
    entries
        .iter()
        .find(|entry| entry.key == LM_PROVIDER_CONFIG_KV_KEY)
        .map(decode_model_config_entry)
        .transpose()
}

fn decode_model_config_entry(entry: &KvEntry) -> Result<ModelConfig, LanguageModelError> {
    decode_kv_value(&entry.value).map_err(|source| LanguageModelError::ConfigStore {
        path: LM_PROVIDER_CONFIG_KV_KEY.to_string(),
        detail: source.to_string(),
    })
}

pub fn model_config_cluster_kv_txn(config: &ModelConfig) -> Result<KvTxn, LanguageModelError> {
    Ok(KvTxn {
        compares: Vec::new(),
        success: vec![
            KvOp::put_serde(LM_PROVIDER_CONFIG_KV_KEY, config, None).map_err(|source| {
                LanguageModelError::ConfigStore {
                    path: LM_PROVIDER_CONFIG_KV_KEY.to_string(),
                    detail: source.to_string(),
                }
            })?,
        ],
        failure: Vec::new(),
    })
}

pub fn model_runtime_config_from_cluster_kv_snapshot(
    snapshot: &KvSnapshot,
) -> Result<Option<ModelRuntimeConfig>, LanguageModelError> {
    snapshot
        .entries
        .iter()
        .find(|entry| entry.key == LM_RUNTIME_CONFIG_KV_KEY)
        .map(decode_model_runtime_config_entry)
        .transpose()
}

fn decode_model_runtime_config_entry(
    entry: &KvEntry,
) -> Result<ModelRuntimeConfig, LanguageModelError> {
    decode_kv_value(&entry.value).map_err(|source| LanguageModelError::ConfigStore {
        path: LM_RUNTIME_CONFIG_KV_KEY.to_string(),
        detail: source.to_string(),
    })
}

pub fn model_runtime_config_cluster_kv_txn(
    config: &ModelRuntimeConfig,
) -> Result<KvTxn, LanguageModelError> {
    Ok(KvTxn {
        compares: Vec::new(),
        success: vec![
            KvOp::put_serde(LM_RUNTIME_CONFIG_KV_KEY, config, None).map_err(|source| {
                LanguageModelError::ConfigStore {
                    path: LM_RUNTIME_CONFIG_KV_KEY.to_string(),
                    detail: source.to_string(),
                }
            })?,
        ],
        failure: Vec::new(),
    })
}

pub fn save_model_config(path: &Path, value: &ModelConfig) -> Result<(), LanguageModelError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| LanguageModelError::ConfigStore {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let content = serde_json::to_string_pretty(value).map_err(|source| {
        LanguageModelError::RequestSerialization {
            detail: source.to_string(),
        }
    })?;
    fs::write(path, content).map_err(|source| LanguageModelError::ConfigStore {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

pub fn current_default_model(
    data_dir: &SinomoDataDir,
) -> Result<CurrentDefaultModel, LanguageModelError> {
    let model_paths = ModelPaths::new(data_dir);
    let model_config = load_model_config(&model_paths).ok();
    let resolved = resolve_default_model(model_config.as_ref(), None)?;
    Ok(CurrentDefaultModel {
        provider: resolved.provider,
        account: resolved.account,
        configured_model: resolved.configured_model,
        model: resolved.model,
        config_path: model_paths.config,
    })
}

pub fn resolve_default_model(
    model_config: Option<&ModelConfig>,
    fallback_config: Option<&ModelConfig>,
) -> Result<ResolvedDefaultModel, LanguageModelError> {
    let provider = model_config
        .and_then(|config| config.default_provider.clone())
        .or_else(|| fallback_config.and_then(|config| config.default_provider.clone()))
        .unwrap_or_else(|| GitHubCopilotProvider::PROVIDER_ID.to_string());
    let provider = normalize_model_provider(&provider).ok_or_else(|| {
        LanguageModelError::ProviderInvocationFailed {
            provider: provider.clone(),
            model: PROVIDER_DEFAULT_MODEL.to_string(),
            detail: format!("unknown provider `{provider}`"),
        }
    })?;
    let account = normalize_provider_account_name(
        model_config
            .and_then(|config| config.default_account.as_deref())
            .or_else(|| fallback_config.and_then(|config| config.default_account.as_deref())),
    );
    let configured_model = model_config
        .and_then(|config| config.default_model.clone())
        .or_else(|| fallback_config.and_then(|config| config.default_model.clone()))
        .or_else(|| {
            model_config
                .and_then(|config| provider_account_config(config, &provider, Some(&account)))
                .and_then(|account| account.default_model.clone())
        })
        .unwrap_or_else(|| PROVIDER_DEFAULT_MODEL.to_string());
    let model = effective_model_name(&provider, &configured_model);
    Ok(ResolvedDefaultModel {
        provider,
        account,
        configured_model,
        model,
    })
}

pub fn write_default_model_config(
    data_dir: &SinomoDataDir,
    provider: impl Into<String>,
    model: impl Into<String>,
) -> Result<DefaultModelConfigUpdate, LanguageModelError> {
    data_dir
        .ensure_root_layout()
        .map_err(|source| LanguageModelError::ConfigStore {
            path: data_dir.root().display().to_string(),
            detail: source.to_string(),
        })?;
    let model_paths = ModelPaths::new(data_dir);
    model_paths.ensure()?;
    let config = ModelConfig {
        default_provider: Some(provider.into()),
        default_account: Some(DEFAULT_PROVIDER_ACCOUNT.to_string()),
        default_model: Some(model.into()),
        accounts: Vec::new(),
    };
    save_model_config(&model_paths.config, &config)?;
    Ok(DefaultModelConfigUpdate {
        config,
        config_path: model_paths.config,
    })
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
        enabled: true,
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

pub fn provider_account_config<'a>(
    config: &'a ModelConfig,
    provider: &str,
    account: Option<&str>,
) -> Option<&'a ProviderAccountConfig> {
    let account = normalize_provider_account_name(account);
    config.accounts.iter().find(|candidate| {
        candidate.provider == provider
            && normalize_provider_account_name(Some(&candidate.account)) == account
            && candidate.enabled
    })
}

pub fn effective_model_name(provider: &str, configured_model: &str) -> String {
    if configured_model != PROVIDER_DEFAULT_MODEL {
        return normalize_model_id_for_provider(provider, configured_model);
    }
    match provider {
        GitHubCopilotProvider::PROVIDER_ID => GitHubCopilotConfig::default().default_model,
        GitHubCopilotCliProvider::PROVIDER_ID => GitHubCopilotCliConfig::default().model,
        CodexCliProvider::PROVIDER_ID => CODEX_CLI_DEFAULT_MODEL.to_string(),
        CODEX_PROVIDER_ID => CODEX_DEFAULT_MODEL.to_string(),
        ClaudeCodeProvider::PROVIDER_ID => CLAUDE_CODE_DEFAULT_MODEL.to_string(),
        AnthropicProvider::PROVIDER_ID => AnthropicConfig::default().default_model,
        OpenAiProvider::PROVIDER_ID => OpenAiConfig::default().default_model,
        DeepSeekProvider::PROVIDER_ID => DeepSeekConfig::default().default_model,
        GeminiProvider::PROVIDER_ID => GeminiConfig::default().default_model,
        NvidiaProvider::PROVIDER_ID => NvidiaConfig::default().default_model,
        _ => configured_model.to_string(),
    }
}

pub fn normalize_model_id_for_provider(provider: &str, model: &str) -> String {
    match provider {
        NvidiaProvider::PROVIDER_ID => normalize_nvidia_model_id(model),
        _ => model.to_string(),
    }
}

pub fn default_model_provider_ids() -> &'static [&'static str] {
    &[
        GitHubCopilotProvider::PROVIDER_ID,
        GitHubCopilotCliProvider::PROVIDER_ID,
        CodexCliProvider::PROVIDER_ID,
        CODEX_PROVIDER_ID,
        ClaudeCodeProvider::PROVIDER_ID,
        AnthropicProvider::PROVIDER_ID,
        OpenAiProvider::PROVIDER_ID,
        DeepSeekProvider::PROVIDER_ID,
        GeminiProvider::PROVIDER_ID,
        NvidiaProvider::PROVIDER_ID,
    ]
}

pub fn normalize_model_provider(provider: &str) -> Option<String> {
    match provider {
        GitHubCopilotProvider::PROVIDER_ID => Some(GitHubCopilotProvider::PROVIDER_ID.to_string()),
        GitHubCopilotCliProvider::PROVIDER_ID => {
            Some(GitHubCopilotCliProvider::PROVIDER_ID.to_string())
        }
        CodexCliProvider::PROVIDER_ID => Some(CodexCliProvider::PROVIDER_ID.to_string()),
        CODEX_PROVIDER_ID => Some(CODEX_PROVIDER_ID.to_string()),
        ClaudeCodeProvider::PROVIDER_ID | "anthropic-claude-code" => {
            Some(ClaudeCodeProvider::PROVIDER_ID.to_string())
        }
        AnthropicProvider::PROVIDER_ID | "claude" => {
            Some(AnthropicProvider::PROVIDER_ID.to_string())
        }
        OpenAiProvider::PROVIDER_ID | "openai-api" => Some(OpenAiProvider::PROVIDER_ID.to_string()),
        DeepSeekProvider::PROVIDER_ID | "deepseek-api" => {
            Some(DeepSeekProvider::PROVIDER_ID.to_string())
        }
        GeminiProvider::PROVIDER_ID | "google-gemini" => {
            Some(GeminiProvider::PROVIDER_ID.to_string())
        }
        NvidiaProvider::PROVIDER_ID | "nvidia-nim" => Some(NvidiaProvider::PROVIDER_ID.to_string()),
        _ => None,
    }
}

pub fn subscription_usage_provider_ids() -> &'static [&'static str] {
    &[CODEX_PROVIDER_ID, GitHubCopilotProvider::PROVIDER_ID]
}

pub fn provider_available_model_ids(provider: &str) -> Result<Vec<String>, LanguageModelError> {
    match provider {
        GitHubCopilotProvider::PROVIDER_ID => {
            let (provider, _) = github_copilot_provider()?;
            provider.list_models()
        }
        GitHubCopilotCliProvider::PROVIDER_ID
        | CodexCliProvider::PROVIDER_ID
        | ClaudeCodeProvider::PROVIDER_ID => Err(LanguageModelError::ProviderInvocationFailed {
            provider: provider.to_string(),
            model: PROVIDER_DEFAULT_MODEL.to_string(),
            detail: format!("provider `{provider}` does not use Sinomo-managed authentication"),
        }),
        CODEX_PROVIDER_ID => codex_provider_for_model(PROVIDER_DEFAULT_MODEL).list_models(),
        AnthropicProvider::PROVIDER_ID => {
            let (provider, _) = anthropic_provider()?;
            provider.api_key()?;
            Ok(vec![AnthropicConfig::default().default_model])
        }
        OpenAiProvider::PROVIDER_ID => {
            let (provider, _) = openai_provider()?;
            provider.api_key()?;
            Ok(vec![OpenAiConfig::default().default_model])
        }
        DeepSeekProvider::PROVIDER_ID => {
            let (provider, _) = deepseek_provider()?;
            provider.list_models()
        }
        GeminiProvider::PROVIDER_ID => {
            let (provider, _) = gemini_provider()?;
            provider.api_key()?;
            Ok(vec![GeminiConfig::default().default_model])
        }
        NvidiaProvider::PROVIDER_ID => {
            let (provider, _) = nvidia_provider()?;
            provider.list_models()
        }
        _ => Err(LanguageModelError::ProviderInvocationFailed {
            provider: provider.to_string(),
            model: PROVIDER_DEFAULT_MODEL.to_string(),
            detail: format!("unknown provider `{provider}`"),
        }),
    }
}

pub fn collect_provider_model_catalogs(
    providers: impl IntoIterator<Item = impl AsRef<str>>,
    explicit_provider: bool,
) -> Result<Vec<LanguageModelProviderCatalog>, LanguageModelError> {
    let mut catalogs = Vec::new();
    for provider in providers {
        let provider = provider.as_ref();
        let models = match provider_available_model_ids(provider) {
            Ok(models) => models,
            Err(error) if explicit_provider => return Err(error),
            Err(_) => continue,
        };
        if !models.is_empty() {
            catalogs.push(LanguageModelProviderCatalog {
                provider: provider.to_string(),
                models,
            });
        }
    }
    Ok(catalogs)
}

pub fn provider_usage(provider: &str) -> Result<LanguageModelProviderUsage, LanguageModelError> {
    match provider {
        CODEX_PROVIDER_ID => codex_provider_for_model(PROVIDER_DEFAULT_MODEL).usage(),
        GitHubCopilotProvider::PROVIDER_ID => {
            let (provider, _) = github_copilot_provider()?;
            provider.usage()
        }
        _ => Err(LanguageModelError::ProviderInvocationFailed {
            provider: provider.to_string(),
            model: PROVIDER_DEFAULT_MODEL.to_string(),
            detail: format!("provider `{provider}` does not expose subscription usage"),
        }),
    }
}

pub fn collect_provider_usage(
    providers: impl IntoIterator<Item = impl AsRef<str>>,
    explicit_provider: bool,
) -> Result<Vec<LanguageModelProviderUsage>, LanguageModelError> {
    let mut snapshots = Vec::new();
    for provider in providers {
        match provider_usage(provider.as_ref()) {
            Ok(snapshot) => snapshots.push(snapshot),
            Err(error) if explicit_provider => return Err(error),
            Err(_) => {}
        }
    }
    Ok(snapshots)
}

pub fn complete_configured_model_request(
    data_dir: &SinomoDataDir,
    provider: &str,
    model: &str,
    working_dir: Option<PathBuf>,
    request: &LanguageModelRequest,
) -> Result<LanguageModelResponse, LanguageModelError> {
    complete_configured_model_request_with_account(
        data_dir,
        provider,
        model,
        None,
        working_dir,
        request,
    )
}

pub fn complete_configured_model_request_with_account(
    data_dir: &SinomoDataDir,
    provider: &str,
    model: &str,
    account: Option<&ProviderAccountConfig>,
    working_dir: Option<PathBuf>,
    request: &LanguageModelRequest,
) -> Result<LanguageModelResponse, LanguageModelError> {
    let request = LanguageModelRequest {
        provider: provider.to_string(),
        model: model.to_string(),
        ..request.clone()
    };
    match provider {
        GitHubCopilotProvider::PROVIDER_ID => {
            let credentials_path =
                data_dir.provider_credentials_file(GitHubCopilotProvider::PROVIDER_ID);
            let default_config = GitHubCopilotConfig::default();
            let provider = GitHubCopilotProvider::new(GitHubCopilotConfig {
                client_id: default_config.client_id,
                github_host: default_config.github_host,
                api_base_url: account
                    .and_then(|account| account.api_base_url.clone())
                    .unwrap_or(default_config.api_base_url),
                default_model: model_or_default(
                    model,
                    GitHubCopilotConfig::default().default_model,
                ),
                token_store_path: Some(credentials_path),
            });
            provider.complete(&request)
        }
        GitHubCopilotCliProvider::PROVIDER_ID => {
            let provider = GitHubCopilotCliProvider::new(GitHubCopilotCliConfig {
                model: model_or_default(model, GitHubCopilotCliConfig::default().model),
                ..GitHubCopilotCliConfig::default()
            });
            provider.complete(&request)
        }
        CodexCliProvider::PROVIDER_ID => {
            let provider = CodexCliProvider::new(CodexCliConfig {
                model: model_or_default(model, CODEX_CLI_DEFAULT_MODEL.to_string()),
                working_dir: working_dir.clone(),
                ..CodexCliConfig::default()
            });
            provider.complete(&request)
        }
        CODEX_PROVIDER_ID => codex_provider_for_model(model).complete(&request),
        ClaudeCodeProvider::PROVIDER_ID => {
            let provider = ClaudeCodeProvider::new(ClaudeCodeConfig {
                model: model_or_default(model, CLAUDE_CODE_DEFAULT_MODEL.to_string()),
                working_dir,
                ..ClaudeCodeConfig::default()
            });
            provider.complete(&request)
        }
        AnthropicProvider::PROVIDER_ID => {
            let credentials_path =
                data_dir.provider_credentials_file(AnthropicProvider::PROVIDER_ID);
            let default_config = AnthropicConfig::default();
            let provider = AnthropicProvider::new(AnthropicConfig {
                api_base_url: account
                    .and_then(|account| account.api_base_url.clone())
                    .unwrap_or(default_config.api_base_url),
                default_model: model_or_default(model, default_config.default_model),
                api_key: account.and_then(|account| account.api_key.clone()),
                credentials_path: account
                    .and_then(|account| account.api_key.as_ref())
                    .map(|_| None)
                    .unwrap_or(Some(credentials_path)),
            });
            provider.complete(&request)
        }
        OpenAiProvider::PROVIDER_ID => {
            let credentials_path = data_dir.provider_credentials_file(OpenAiProvider::PROVIDER_ID);
            let default_config = OpenAiConfig::default();
            let provider = OpenAiProvider::new(OpenAiConfig {
                api_base_url: account
                    .and_then(|account| account.api_base_url.clone())
                    .unwrap_or(default_config.api_base_url),
                default_model: model_or_default(model, default_config.default_model),
                api_key: account.and_then(|account| account.api_key.clone()),
                credentials_path: account
                    .and_then(|account| account.api_key.as_ref())
                    .map(|_| None)
                    .unwrap_or(Some(credentials_path)),
            });
            provider.complete(&request)
        }
        DeepSeekProvider::PROVIDER_ID => {
            let credentials_path =
                data_dir.provider_credentials_file(DeepSeekProvider::PROVIDER_ID);
            let default_config = DeepSeekConfig::default();
            let provider = DeepSeekProvider::new(DeepSeekConfig {
                api_base_url: account
                    .and_then(|account| account.api_base_url.clone())
                    .unwrap_or(default_config.api_base_url),
                default_model: model_or_default(model, default_config.default_model),
                api_key: account.and_then(|account| account.api_key.clone()),
                credentials_path: account
                    .and_then(|account| account.api_key.as_ref())
                    .map(|_| None)
                    .unwrap_or(Some(credentials_path)),
            });
            provider.complete(&request)
        }
        GeminiProvider::PROVIDER_ID => {
            let credentials_path = data_dir.provider_credentials_file(GeminiProvider::PROVIDER_ID);
            let default_config = GeminiConfig::default();
            let provider = GeminiProvider::new(GeminiConfig {
                api_base_url: account
                    .and_then(|account| account.api_base_url.clone())
                    .unwrap_or(default_config.api_base_url),
                default_model: model_or_default(model, default_config.default_model),
                api_key: account.and_then(|account| account.api_key.clone()),
                credentials_path: account
                    .and_then(|account| account.api_key.as_ref())
                    .map(|_| None)
                    .unwrap_or(Some(credentials_path)),
            });
            provider.complete(&request)
        }
        NvidiaProvider::PROVIDER_ID => {
            let credentials_path = data_dir.provider_credentials_file(NvidiaProvider::PROVIDER_ID);
            let default_config = NvidiaConfig::default();
            let provider = NvidiaProvider::new(NvidiaConfig {
                api_base_url: account
                    .and_then(|account| account.api_base_url.clone())
                    .unwrap_or(default_config.api_base_url),
                default_model: model_or_default(model, default_config.default_model),
                api_key: account.and_then(|account| account.api_key.clone()),
                credentials_path: account
                    .and_then(|account| account.api_key.as_ref())
                    .map(|_| None)
                    .unwrap_or(Some(credentials_path)),
            });
            provider.complete(&request)
        }
        _ => Err(LanguageModelError::ProviderInvocationFailed {
            provider: provider.to_string(),
            model: model.to_string(),
            detail: "unknown language model provider".to_string(),
        }),
    }
}

pub fn model_or_default(model: &str, default_model: String) -> String {
    if model == PROVIDER_DEFAULT_MODEL {
        default_model
    } else {
        model.to_string()
    }
}

pub fn github_copilot_provider() -> Result<(GitHubCopilotProvider, PathBuf), LanguageModelError> {
    let data_dir =
        SinomoDataDir::from_env_or_home().map_err(|source| LanguageModelError::ConfigStore {
            path: "<data-dir>".to_string(),
            detail: source.to_string(),
        })?;
    data_dir
        .ensure_provider_dir(GitHubCopilotProvider::PROVIDER_ID)
        .map_err(|source| LanguageModelError::ConfigStore {
            path: data_dir
                .provider_dir(GitHubCopilotProvider::PROVIDER_ID)
                .display()
                .to_string(),
            detail: source.to_string(),
        })?;
    let credentials_path = data_dir.provider_credentials_file(GitHubCopilotProvider::PROVIDER_ID);
    let client_id = GitHubCopilotConfig::default().client_id;
    let provider = GitHubCopilotProvider::new(GitHubCopilotConfig {
        client_id,
        token_store_path: Some(credentials_path.clone()),
        ..GitHubCopilotConfig::default()
    });
    Ok((provider, credentials_path))
}

pub fn anthropic_provider() -> Result<(AnthropicProvider, PathBuf), LanguageModelError> {
    let data_dir = provider_data_dir(AnthropicProvider::PROVIDER_ID)?;
    let credentials_path = data_dir.provider_credentials_file(AnthropicProvider::PROVIDER_ID);
    let provider = AnthropicProvider::new(AnthropicConfig {
        api_base_url: AnthropicConfig::default().api_base_url,
        default_model: AnthropicConfig::default().default_model,
        api_key: None,
        credentials_path: Some(credentials_path.clone()),
    });
    Ok((provider, credentials_path))
}

pub fn openai_provider() -> Result<(OpenAiProvider, PathBuf), LanguageModelError> {
    let data_dir = provider_data_dir(OpenAiProvider::PROVIDER_ID)?;
    let credentials_path = data_dir.provider_credentials_file(OpenAiProvider::PROVIDER_ID);
    let provider = OpenAiProvider::new(OpenAiConfig {
        api_base_url: OpenAiConfig::default().api_base_url,
        default_model: OpenAiConfig::default().default_model,
        api_key: None,
        credentials_path: Some(credentials_path.clone()),
    });
    Ok((provider, credentials_path))
}

pub fn deepseek_provider() -> Result<(DeepSeekProvider, PathBuf), LanguageModelError> {
    let data_dir = provider_data_dir(DeepSeekProvider::PROVIDER_ID)?;
    let credentials_path = data_dir.provider_credentials_file(DeepSeekProvider::PROVIDER_ID);
    let provider = DeepSeekProvider::new(DeepSeekConfig {
        api_base_url: DeepSeekConfig::default().api_base_url,
        default_model: DeepSeekConfig::default().default_model,
        api_key: None,
        credentials_path: Some(credentials_path.clone()),
    });
    Ok((provider, credentials_path))
}

pub fn gemini_provider() -> Result<(GeminiProvider, PathBuf), LanguageModelError> {
    let data_dir = provider_data_dir(GeminiProvider::PROVIDER_ID)?;
    let credentials_path = data_dir.provider_credentials_file(GeminiProvider::PROVIDER_ID);
    let provider = GeminiProvider::new(GeminiConfig {
        api_base_url: GeminiConfig::default().api_base_url,
        default_model: GeminiConfig::default().default_model,
        api_key: None,
        credentials_path: Some(credentials_path.clone()),
    });
    Ok((provider, credentials_path))
}

pub fn nvidia_provider() -> Result<(NvidiaProvider, PathBuf), LanguageModelError> {
    let data_dir = provider_data_dir(NvidiaProvider::PROVIDER_ID)?;
    let credentials_path = data_dir.provider_credentials_file(NvidiaProvider::PROVIDER_ID);
    let provider = NvidiaProvider::new(NvidiaConfig {
        api_base_url: NvidiaConfig::default().api_base_url,
        default_model: NvidiaConfig::default().default_model,
        api_key: None,
        credentials_path: Some(credentials_path.clone()),
    });
    Ok((provider, credentials_path))
}

pub fn codex_provider() -> Result<(CodexProvider, PathBuf), LanguageModelError> {
    let provider = codex_provider_for_model(PROVIDER_DEFAULT_MODEL);
    let credentials_path =
        provider
            .credentials_path()
            .ok_or_else(|| LanguageModelError::ConfigStore {
                path: "<codex-credentials>".to_string(),
                detail: "could not resolve Sinomo Codex credentials path; set SINOMO_DATA_DIR or SINOMO_CODEX_CREDENTIALS_FILE".to_string(),
            })?;
    Ok((provider, credentials_path))
}

pub fn codex_provider_for_model(model: &str) -> CodexProvider {
    let default_model = model_or_default(model, CODEX_DEFAULT_MODEL.to_string());
    let config_default = OpenAiResponsesCodexConfig::default();
    let client_config = OpenAiResponsesCodexConfig {
        api_base_url: config_default.api_base_url.clone(),
        auth_issuer: config_default.auth_issuer.clone(),
        client_id: config_default.client_id.clone(),
        client_version: config_default.client_version,
        default_model: default_model.clone(),
        credentials_path: config_default.credentials_path,
    };
    CodexProvider::new(
        CodexConfig {
            default_model,
            ..CodexConfig::default()
        },
        OpenAiResponsesCodexClient::new(client_config),
        CodexToolRegistry::new(),
        DefaultCodexPromptBuilder::default(),
        DefaultCodexContextManager::default(),
    )
}

fn provider_data_dir(provider: &str) -> Result<SinomoDataDir, LanguageModelError> {
    let data_dir =
        SinomoDataDir::from_env_or_home().map_err(|source| LanguageModelError::ConfigStore {
            path: "<data-dir>".to_string(),
            detail: source.to_string(),
        })?;
    data_dir
        .ensure_provider_dir(provider)
        .map_err(|source| LanguageModelError::ConfigStore {
            path: data_dir.provider_dir(provider).display().to_string(),
            detail: source.to_string(),
        })?;
    Ok(data_dir)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderCredentialWrite {
    pub provider: String,
    pub credentials_path: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderCredentialTest {
    pub provider: String,
    pub credentials_path: PathBuf,
}

pub fn save_provider_api_key(
    provider: &str,
    api_key: &str,
    source: &str,
) -> Result<ProviderCredentialWrite, LanguageModelError> {
    match provider {
        AnthropicProvider::PROVIDER_ID => {
            let (provider, credentials_path) = anthropic_provider()?;
            provider.save_api_key(api_key, source)?;
            Ok(ProviderCredentialWrite {
                provider: AnthropicProvider::PROVIDER_ID.to_string(),
                credentials_path,
            })
        }
        OpenAiProvider::PROVIDER_ID => {
            let (provider, credentials_path) = openai_provider()?;
            provider.save_api_key(api_key, source)?;
            Ok(ProviderCredentialWrite {
                provider: OpenAiProvider::PROVIDER_ID.to_string(),
                credentials_path,
            })
        }
        DeepSeekProvider::PROVIDER_ID => {
            let (provider, credentials_path) = deepseek_provider()?;
            provider.save_api_key(api_key, source)?;
            Ok(ProviderCredentialWrite {
                provider: DeepSeekProvider::PROVIDER_ID.to_string(),
                credentials_path,
            })
        }
        GeminiProvider::PROVIDER_ID => {
            let (provider, credentials_path) = gemini_provider()?;
            provider.save_api_key(api_key, source)?;
            Ok(ProviderCredentialWrite {
                provider: GeminiProvider::PROVIDER_ID.to_string(),
                credentials_path,
            })
        }
        NvidiaProvider::PROVIDER_ID => {
            let (provider, credentials_path) = nvidia_provider()?;
            provider.save_api_key(api_key, source)?;
            Ok(ProviderCredentialWrite {
                provider: NvidiaProvider::PROVIDER_ID.to_string(),
                credentials_path,
            })
        }
        _ => Err(LanguageModelError::ProviderInvocationFailed {
            provider: provider.to_string(),
            model: PROVIDER_DEFAULT_MODEL.to_string(),
            detail: "provider does not support API-key credential storage".to_string(),
        }),
    }
}

pub fn save_provider_api_key_in_model_config(
    config: &mut ModelConfig,
    provider: &str,
    account: Option<&str>,
    api_key: &str,
) -> Result<ProviderAccountInfo, LanguageModelError> {
    let provider = normalize_model_provider(provider).ok_or_else(|| {
        LanguageModelError::ProviderInvocationFailed {
            provider: provider.to_string(),
            model: PROVIDER_DEFAULT_MODEL.to_string(),
            detail: format!("unknown provider `{provider}`"),
        }
    })?;
    if provider_api_key_env_vars(&provider).is_none() {
        return Err(LanguageModelError::ProviderInvocationFailed {
            provider,
            model: PROVIDER_DEFAULT_MODEL.to_string(),
            detail: "provider does not support API-key credential storage".to_string(),
        });
    }
    let account = normalize_provider_account_name(account);
    let default_model = provider_account_config(config, &provider, Some(&account))
        .and_then(|existing| existing.default_model.clone())
        .unwrap_or_else(|| effective_model_name(&provider, PROVIDER_DEFAULT_MODEL));
    upsert_provider_account_config(
        config,
        ProviderAccountConfig {
            provider: provider.clone(),
            account: account.clone(),
            default_model: Some(default_model),
            api_base_url: provider_account_config(config, &provider, Some(&account))
                .and_then(|existing| existing.api_base_url.clone()),
            api_key: Some(api_key.trim().to_string()),
            enabled: true,
        },
    )?;
    Ok(provider_accounts(config)
        .into_iter()
        .find(|candidate| candidate.provider == provider && candidate.account == account)
        .expect("upserted account is present"))
}

pub fn provider_api_key_env_vars(provider: &str) -> Option<&'static [&'static str]> {
    match provider {
        AnthropicProvider::PROVIDER_ID => Some(&[
            "SINOMO_ANTHROPIC_API_KEY",
            "ANTHROPIC_API_KEY",
            "CLAUDE_API_KEY",
            "SINOMO_CLAUDE_API_KEY",
        ]),
        OpenAiProvider::PROVIDER_ID => Some(&["SINOMO_OPENAI_API_KEY", "OPENAI_API_KEY"]),
        DeepSeekProvider::PROVIDER_ID => Some(&["SINOMO_DEEPSEEK_API_KEY", "DEEPSEEK_API_KEY"]),
        GeminiProvider::PROVIDER_ID => Some(&[
            "SINOMO_GEMINI_API_KEY",
            "GEMINI_API_KEY",
            "GOOGLE_API_KEY",
            "GOOGLE_AI_API_KEY",
        ]),
        NvidiaProvider::PROVIDER_ID => {
            Some(&["SINOMO_NVIDIA_API_KEY", "NVIDIA_API_KEY", "NIM_API_KEY"])
        }
        _ => None,
    }
}

pub fn provider_api_key_from_env(provider: &str) -> Option<(String, String)> {
    for env_var in provider_api_key_env_vars(provider)? {
        if let Ok(api_key) = env::var(env_var) {
            let api_key = api_key.trim();
            if !api_key.is_empty() {
                return Some((api_key.to_string(), format!("env:{env_var}")));
            }
        }
    }
    None
}

pub fn test_provider_credentials(
    provider: &str,
) -> Result<ProviderCredentialTest, LanguageModelError> {
    match provider {
        GitHubCopilotProvider::PROVIDER_ID => {
            let (provider, credentials_path) = github_copilot_provider()?;
            provider.github_token()?;
            provider.test_connection()?;
            Ok(ProviderCredentialTest {
                provider: GitHubCopilotProvider::PROVIDER_ID.to_string(),
                credentials_path,
            })
        }
        CODEX_PROVIDER_ID => {
            let (provider, credentials_path) = codex_provider()?;
            provider.chatgpt_tokens()?;
            provider.test_connection()?;
            Ok(ProviderCredentialTest {
                provider: CODEX_PROVIDER_ID.to_string(),
                credentials_path,
            })
        }
        AnthropicProvider::PROVIDER_ID => {
            let (provider, credentials_path) = anthropic_provider()?;
            provider.api_key()?;
            provider.test_connection()?;
            Ok(ProviderCredentialTest {
                provider: AnthropicProvider::PROVIDER_ID.to_string(),
                credentials_path,
            })
        }
        OpenAiProvider::PROVIDER_ID => {
            let (provider, credentials_path) = openai_provider()?;
            provider.api_key()?;
            provider.test_connection()?;
            Ok(ProviderCredentialTest {
                provider: OpenAiProvider::PROVIDER_ID.to_string(),
                credentials_path,
            })
        }
        DeepSeekProvider::PROVIDER_ID => {
            let (provider, credentials_path) = deepseek_provider()?;
            provider.api_key()?;
            provider.test_connection()?;
            Ok(ProviderCredentialTest {
                provider: DeepSeekProvider::PROVIDER_ID.to_string(),
                credentials_path,
            })
        }
        GeminiProvider::PROVIDER_ID => {
            let (provider, credentials_path) = gemini_provider()?;
            provider.api_key()?;
            provider.test_connection()?;
            Ok(ProviderCredentialTest {
                provider: GeminiProvider::PROVIDER_ID.to_string(),
                credentials_path,
            })
        }
        NvidiaProvider::PROVIDER_ID => {
            let (provider, credentials_path) = nvidia_provider()?;
            provider.api_key()?;
            provider.test_connection()?;
            Ok(ProviderCredentialTest {
                provider: NvidiaProvider::PROVIDER_ID.to_string(),
                credentials_path,
            })
        }
        _ => Err(LanguageModelError::ProviderInvocationFailed {
            provider: provider.to_string(),
            model: PROVIDER_DEFAULT_MODEL.to_string(),
            detail: "unknown language model provider".to_string(),
        }),
    }
}

pub fn test_provider_account_credentials(
    provider: &str,
    account: Option<&ProviderAccountConfig>,
) -> Result<(), LanguageModelError> {
    let model = account
        .and_then(|account| account.default_model.as_deref())
        .unwrap_or(PROVIDER_DEFAULT_MODEL);
    let data_dir =
        SinomoDataDir::from_env_or_home().map_err(|source| LanguageModelError::ConfigStore {
            path: "<data-dir>".to_string(),
            detail: source.to_string(),
        })?;
    let request = LanguageModelRequest {
        provider: provider.to_string(),
        model: model.to_string(),
        messages: vec![LanguageModelMessage {
            role: LanguageModelRole::User,
            content: "Reply with ok.".to_string(),
        }],
        items: Vec::new(),
        tools: Vec::new(),
        tool_choice: None,
        parallel_tool_calls: None,
        temperature: None,
        max_output_tokens: Some(8),
        response_format: LanguageModelResponseFormat::Text,
        metadata: serde_json::Value::Null,
    };
    complete_configured_model_request_with_account(
        &data_dir, provider, model, account, None, &request,
    )
    .map(|_| ())
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct LanguageModelAttribution {
    pub provider: String,
    pub model: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct LanguageModelProviderInfo {
    pub provider: String,
    pub default_model: String,
    pub invocation: LanguageModelInvocation,
    pub supports_json_object: bool,
    pub supports_tool_permissions: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum LanguageModelInvocation {
    Process,
    Http,
    InProcess,
}

pub trait LanguageModelProvider {
    fn info(&self) -> LanguageModelProviderInfo;

    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError>;

    fn complete_streaming_events(
        &self,
        request: &LanguageModelRequest,
        on_event: &mut dyn FnMut(LanguageModelStreamEvent),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        on_event(LanguageModelStreamEvent::Started {
            provider: request.provider.clone(),
            model: request.model.clone(),
        });
        let response = self.complete(request)?;
        if !response.content.is_empty() {
            on_event(LanguageModelStreamEvent::TextDelta {
                delta: response.content.clone(),
            });
        }
        for call in &response.tool_calls {
            on_event(LanguageModelStreamEvent::ToolCallStarted {
                call_id: call.call_id.clone(),
                name: call.name.clone(),
            });
            on_event(LanguageModelStreamEvent::ToolCallCompleted {
                call_id: call.call_id.clone(),
                name: call.name.clone(),
                input: call.input.clone(),
            });
        }
        if let Some(usage) = &response.usage {
            on_event(LanguageModelStreamEvent::Usage {
                usage: usage.clone(),
            });
        }
        on_event(LanguageModelStreamEvent::Finished {
            finish_reason: None,
        });
        Ok(response)
    }

    fn complete_streaming(
        &self,
        request: &LanguageModelRequest,
        on_delta: &mut dyn FnMut(&str),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        self.complete_streaming_events(request, &mut |event| {
            if let LanguageModelStreamEvent::TextDelta { delta } = event {
                on_delta(&delta);
            }
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommandLanguageModelProvider {
    program: String,
    args: Vec<String>,
    provider: String,
    model: String,
    prompt_mode: CommandPromptMode,
}

impl CommandLanguageModelProvider {
    pub fn plain_prompt(
        program: impl Into<String>,
        args: impl IntoIterator<Item = impl Into<String>>,
        provider: impl Into<String>,
        model: impl Into<String>,
    ) -> Self {
        Self {
            program: program.into(),
            args: args.into_iter().map(Into::into).collect(),
            provider: provider.into(),
            model: model.into(),
            prompt_mode: CommandPromptMode::PlainPrompt,
        }
    }

    pub fn json_request(
        program: impl Into<String>,
        args: impl IntoIterator<Item = impl Into<String>>,
        provider: impl Into<String>,
        model: impl Into<String>,
    ) -> Self {
        Self {
            program: program.into(),
            args: args.into_iter().map(Into::into).collect(),
            provider: provider.into(),
            model: model.into(),
            prompt_mode: CommandPromptMode::JsonRequest,
        }
    }

    fn stdin_payload(&self, request: &LanguageModelRequest) -> Result<String, LanguageModelError> {
        match self.prompt_mode {
            CommandPromptMode::PlainPrompt => Ok(render_plain_prompt(request)),
            CommandPromptMode::JsonRequest => serde_json::to_string(request).map_err(|source| {
                LanguageModelError::RequestSerialization {
                    detail: source.to_string(),
                }
            }),
        }
    }
}

impl LanguageModelProvider for CommandLanguageModelProvider {
    fn info(&self) -> LanguageModelProviderInfo {
        LanguageModelProviderInfo {
            provider: self.provider.clone(),
            default_model: self.model.clone(),
            invocation: LanguageModelInvocation::Process,
            supports_json_object: matches!(self.prompt_mode, CommandPromptMode::JsonRequest),
            supports_tool_permissions: false,
        }
    }

    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        let stdin_payload = self.stdin_payload(request)?;
        let mut child = Command::new(&self.program)
            .args(&self.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|source| LanguageModelError::ProviderSpawnFailed {
                provider: self.provider.clone(),
                program: self.program.clone(),
                detail: source.to_string(),
            })?;

        let mut stdin =
            child
                .stdin
                .take()
                .ok_or_else(|| LanguageModelError::ProviderInvocationFailed {
                    provider: self.provider.clone(),
                    model: self.model.clone(),
                    detail: "model command stdin was unavailable".to_string(),
                })?;
        stdin
            .write_all(stdin_payload.as_bytes())
            .map_err(|source| LanguageModelError::ProviderInvocationFailed {
                provider: self.provider.clone(),
                model: self.model.clone(),
                detail: source.to_string(),
            })?;
        drop(stdin);

        let output = child.wait_with_output().map_err(|source| {
            LanguageModelError::ProviderInvocationFailed {
                provider: self.provider.clone(),
                model: self.model.clone(),
                detail: source.to_string(),
            }
        })?;
        if !output.status.success() {
            return Err(LanguageModelError::ProviderInvocationFailed {
                provider: self.provider.clone(),
                model: self.model.clone(),
                detail: format!(
                    "exit status {:?}: {}",
                    output.status.code(),
                    String::from_utf8_lossy(&output.stderr).trim()
                ),
            });
        }

        Ok(LanguageModelResponse {
            provider: self.provider.clone(),
            model: self.model.clone(),
            content: String::from_utf8_lossy(&output.stdout).trim().to_string(),
            items: Vec::new(),
            tool_calls: Vec::new(),
            usage: None,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommandPromptMode {
    PlainPrompt,
    JsonRequest,
}

pub(crate) fn selected_model(default_model: &str, request: &LanguageModelRequest) -> String {
    if request.model.trim().is_empty() || request.model == PROVIDER_DEFAULT_MODEL {
        default_model.to_string()
    } else {
        request.model.clone()
    }
}

pub const PROVIDER_DEFAULT_MODEL: &str = "provider-default";

pub(crate) fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs()
}

pub fn render_plain_prompt(request: &LanguageModelRequest) -> String {
    let mut prompt = String::new();
    for message in &request.messages {
        prompt.push_str(match message.role {
            LanguageModelRole::System => "system",
            LanguageModelRole::User => "user",
            LanguageModelRole::Assistant => "assistant",
        });
        prompt.push_str(":\n");
        prompt.push_str(&message.content);
        prompt.push_str("\n\n");
    }
    prompt
}

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

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> LanguageModelRequest {
        LanguageModelRequest {
            provider: "github-copilot-cli".to_string(),
            model: "provider-default".to_string(),
            messages: vec![
                LanguageModelMessage {
                    role: LanguageModelRole::System,
                    content: "Return JSON only.".to_string(),
                },
                LanguageModelMessage {
                    role: LanguageModelRole::User,
                    content: "Summarize evt_1.".to_string(),
                },
            ],
            items: Vec::new(),
            tools: Vec::new(),
            tool_choice: None,
            parallel_tool_calls: None,
            temperature: Some(0.2),
            max_output_tokens: Some(200),
            response_format: LanguageModelResponseFormat::JsonObject,
            metadata: serde_json::json!({ "job_id": "task_test" }),
        }
    }

    #[test]
    fn inferred_model_context_window_tokens_uses_model_metadata_when_known() {
        assert_eq!(
            inferred_model_context_window_tokens("openai", "custom-1m"),
            Some(1_000_000)
        );
        assert_eq!(
            inferred_model_context_window_tokens("gemini", "provider-default"),
            Some(1_000_000)
        );
        assert_eq!(
            inferred_model_context_window_tokens("claude", "provider-default"),
            Some(200_000)
        );
        assert_eq!(
            inferred_model_context_window_tokens(CODEX_PROVIDER_ID, "provider-default"),
            Some(400_000)
        );
        assert_eq!(
            inferred_model_context_window_tokens("deepseek", "provider-default"),
            Some(1_000_000)
        );
        assert_eq!(
            inferred_model_context_window_tokens("unknown", "custom-1m"),
            None
        );
    }

    #[test]
    fn write_default_model_config_persists_model_defaults() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-lm-models-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        let data_dir = SinomoDataDir::new(&root);

        let update =
            write_default_model_config(&data_dir, "openai", "gpt-5.5").expect("write defaults");

        assert_eq!(update.config.default_provider.as_deref(), Some("openai"));
        assert_eq!(update.config.default_model.as_deref(), Some("gpt-5.5"));
        assert_eq!(update.config_path, root.join("config").join("models.json"));
        let config = load_model_config(&ModelPaths::new(&data_dir)).expect("model config");
        assert_eq!(config.default_provider.as_deref(), Some("openai"));
        assert_eq!(config.default_model.as_deref(), Some("gpt-5.5"));

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn resolve_default_model_prefers_model_config_over_fallback() {
        let model_config = ModelConfig {
            default_provider: Some("openai".to_string()),
            default_model: Some("gpt-5.5".to_string()),
            ..ModelConfig::default()
        };
        let fallback_config = ModelConfig {
            default_provider: Some("gemini".to_string()),
            default_model: Some("gemini-2.5-pro".to_string()),
            ..ModelConfig::default()
        };

        let resolved =
            resolve_default_model(Some(&model_config), Some(&fallback_config)).expect("defaults");

        assert_eq!(resolved.provider, "openai");
        assert_eq!(resolved.configured_model, "gpt-5.5");
        assert_eq!(resolved.model, "gpt-5.5");
    }

    #[test]
    fn resolve_default_model_uses_fallback_and_provider_default_model() {
        let fallback_config = ModelConfig {
            default_provider: Some("nvidia".to_string()),
            default_model: None,
            ..ModelConfig::default()
        };

        let resolved = resolve_default_model(None, Some(&fallback_config)).expect("defaults");

        assert_eq!(resolved.provider, "nvidia");
        assert_eq!(resolved.configured_model, PROVIDER_DEFAULT_MODEL);
        assert_eq!(resolved.model, NVIDIA_DEFAULT_MODEL);
    }

    #[test]
    fn plain_prompt_renders_chat_roles() {
        let prompt = render_plain_prompt(&request());

        assert!(prompt.contains("system:\nReturn JSON only."));
        assert!(prompt.contains("user:\nSummarize evt_1."));
    }

    #[test]
    fn copilot_cli_args_use_programmatic_prompt_mode() {
        let provider = GitHubCopilotCliProvider::new(GitHubCopilotCliConfig {
            model: "gpt-5.3-codex".to_string(),
            ..GitHubCopilotCliConfig::default()
        });

        let args = provider.command_args(&request());

        assert_eq!(args[0], "-p");
        assert!(args.iter().any(|arg| arg == "-s"));
        assert!(args.iter().any(|arg| arg == "--no-ask-user"));
        assert!(
            args.windows(2)
                .any(|pair| pair == ["--model", "gpt-5.3-codex"])
        );
    }

    #[test]
    fn copilot_default_model_omits_model_argument() {
        let args = GitHubCopilotCliProvider::default().command_args(&request());

        assert!(!args.iter().any(|arg| arg == "--model"));
    }

    #[test]
    fn codex_cli_args_use_exec_harness_not_openai_api() {
        let provider = CodexCliProvider::new(CodexCliConfig {
            model: "gpt-5.3-codex".to_string(),
            ..CodexCliConfig::default()
        });

        let args = provider.command_args(
            &request(),
            Some(std::path::Path::new("/tmp/codex-last.txt")),
        );

        assert_eq!(args[0], "exec");
        assert!(args.iter().any(|arg| arg == "--full-auto"));
        assert!(args.windows(2).any(|pair| pair == ["--color", "never"]));
        assert!(
            args.windows(2)
                .any(|pair| pair == ["-o", "/tmp/codex-last.txt"])
        );
        assert!(
            args.windows(2)
                .any(|pair| pair == ["--model", "gpt-5.3-codex"])
        );
        assert_eq!(args.last().map(String::as_str), Some("-"));
    }

    #[test]
    fn codex_provider_reports_process_coding_agent_capabilities() {
        let info = CodexCliProvider::default().info();

        assert_eq!(info.provider, CodexCliProvider::PROVIDER_ID);
        assert_eq!(info.invocation, LanguageModelInvocation::Process);
        assert!(!info.supports_json_object);
        assert!(info.supports_tool_permissions);
    }

    #[test]
    fn claude_code_args_use_print_mode_coding_agent_harness() {
        let provider = ClaudeCodeProvider::new(ClaudeCodeConfig {
            model: "claude-sonnet-4-6".to_string(),
            ..ClaudeCodeConfig::default()
        });

        let args = provider.command_args(&request());

        assert_eq!(args[0], "-p");
        assert!(args[1].contains("system:\nReturn JSON only."));
        assert!(args[1].contains("user:\nSummarize evt_1."));
        assert!(
            args.windows(2)
                .any(|pair| pair == ["--output-format", "text"])
        );
        assert!(
            args.windows(2)
                .any(|pair| pair == ["--permission-mode", "auto"])
        );
        assert!(args.windows(2).any(|pair| pair == ["--max-turns", "10"]));
        assert!(args.iter().any(|arg| arg == "--no-session-persistence"));
        assert!(
            args.windows(2)
                .any(|pair| pair == ["--model", "claude-sonnet-4-6"])
        );
    }

    #[test]
    fn claude_code_provider_reports_process_coding_agent_capabilities() {
        let info = ClaudeCodeProvider::default().info();

        assert_eq!(info.provider, ClaudeCodeProvider::PROVIDER_ID);
        assert_eq!(info.invocation, LanguageModelInvocation::Process);
        assert!(!info.supports_json_object);
        assert!(info.supports_tool_permissions);
    }

    #[test]
    fn gemini_provider_reports_http_json_capabilities() {
        let info = GeminiProvider::default().info();

        assert_eq!(info.provider, GeminiProvider::PROVIDER_ID);
        assert_eq!(info.invocation, LanguageModelInvocation::Http);
        assert!(info.supports_json_object);
        assert!(!info.supports_tool_permissions);
    }

    #[test]
    fn deepseek_provider_reports_http_json_capabilities() {
        let info = DeepSeekProvider::default().info();

        assert_eq!(info.provider, DeepSeekProvider::PROVIDER_ID);
        assert_eq!(info.default_model, DEEPSEEK_DEFAULT_MODEL);
        assert_eq!(info.invocation, LanguageModelInvocation::Http);
        assert!(info.supports_json_object);
        assert!(!info.supports_tool_permissions);
    }

    #[test]
    fn gemini_payload_uses_generate_content_shape() {
        let payload = gemini_generate_content_payload(&request(), "gemini-2.5-flash");

        assert_eq!(
            payload["systemInstruction"]["parts"][0]["text"],
            "Return JSON only."
        );
        assert_eq!(payload["contents"][0]["role"], "user");
        assert_eq!(
            payload["contents"][0]["parts"][0]["text"],
            "Summarize evt_1."
        );
        let temperature = payload["generationConfig"]["temperature"].as_f64().unwrap();
        assert!((temperature - 0.2).abs() < 0.000001);
        assert_eq!(payload["generationConfig"]["maxOutputTokens"], 200);
        assert_eq!(
            payload["generationConfig"]["responseMimeType"],
            "application/json"
        );
    }

    #[test]
    fn gemini_text_extractor_reads_candidates_and_usage() {
        let value = serde_json::json!({
            "candidates": [{
                "content": {
                    "parts": [{ "text": "first" }, { "text": "second" }],
                    "role": "model"
                }
            }],
            "usageMetadata": {
                "promptTokenCount": 11,
                "candidatesTokenCount": 7,
                "totalTokenCount": 18
            }
        });

        assert_eq!(extract_gemini_response_text(&value), "first\nsecond");
        assert_eq!(extract_gemini_usage(&value).unwrap().input_tokens, Some(11));
        assert_eq!(extract_gemini_usage(&value).unwrap().output_tokens, Some(7));
    }

    #[test]
    fn provider_reports_copilot_capabilities() {
        let info = GitHubCopilotCliProvider::default().info();

        assert_eq!(info.provider, GitHubCopilotCliProvider::PROVIDER_ID);
        assert_eq!(info.invocation, LanguageModelInvocation::Process);
        assert!(info.supports_tool_permissions);
    }

    #[test]
    fn collect_provider_usage_skips_default_provider_failures() {
        let snapshots = collect_provider_usage(["unknown-provider"], false).unwrap();

        assert!(snapshots.is_empty());
    }

    #[test]
    fn collect_provider_usage_reports_explicit_provider_failures() {
        let error = collect_provider_usage(["unknown-provider"], true).unwrap_err();

        assert!(matches!(
            error,
            LanguageModelError::ProviderInvocationFailed { provider, .. }
                if provider == "unknown-provider"
        ));
    }

    #[test]
    fn collect_provider_model_catalogs_skips_default_provider_failures() {
        let catalogs = collect_provider_model_catalogs(["unknown-provider"], false).unwrap();

        assert!(catalogs.is_empty());
    }

    #[test]
    fn collect_provider_model_catalogs_reports_explicit_provider_failures() {
        let error = collect_provider_model_catalogs(["unknown-provider"], true).unwrap_err();

        assert!(matches!(
            error,
            LanguageModelError::ProviderInvocationFailed { provider, .. }
                if provider == "unknown-provider"
        ));
    }

    #[test]
    fn complete_configured_model_request_reports_unknown_provider() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-lm-complete-unknown-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::ZERO)
                .as_nanos()
        ));
        let data_dir = SinomoDataDir::new(root);

        let error = complete_configured_model_request(
            &data_dir,
            "unknown-provider",
            "test-model",
            None,
            &request(),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            LanguageModelError::ProviderInvocationFailed {
                provider,
                model,
                ..
            } if provider == "unknown-provider" && model == "test-model"
        ));
    }

    #[test]
    fn save_provider_api_key_rejects_non_api_key_provider() {
        let error = save_provider_api_key(GitHubCopilotProvider::PROVIDER_ID, "secret", "test")
            .unwrap_err();

        assert!(matches!(
            error,
            LanguageModelError::ProviderInvocationFailed {
                provider,
                model,
                detail
            } if provider == GitHubCopilotProvider::PROVIDER_ID
                && model == PROVIDER_DEFAULT_MODEL
                && detail.contains("API-key credential storage")
        ));
    }

    #[test]
    fn provider_api_key_env_vars_cover_api_key_providers() {
        assert_eq!(
            provider_api_key_env_vars(OpenAiProvider::PROVIDER_ID),
            Some(&["SINOMO_OPENAI_API_KEY", "OPENAI_API_KEY"][..])
        );
        assert_eq!(
            provider_api_key_env_vars(DeepSeekProvider::PROVIDER_ID),
            Some(&["SINOMO_DEEPSEEK_API_KEY", "DEEPSEEK_API_KEY"][..])
        );
        assert!(
            provider_api_key_env_vars(AnthropicProvider::PROVIDER_ID)
                .unwrap()
                .contains(&"ANTHROPIC_API_KEY")
        );
        assert!(
            provider_api_key_env_vars(GeminiProvider::PROVIDER_ID)
                .unwrap()
                .contains(&"GOOGLE_API_KEY")
        );
        assert!(
            provider_api_key_env_vars(NvidiaProvider::PROVIDER_ID)
                .unwrap()
                .contains(&"NIM_API_KEY")
        );
        assert_eq!(
            provider_api_key_env_vars(GitHubCopilotProvider::PROVIDER_ID),
            None
        );
    }

    #[test]
    fn test_provider_credentials_reports_unknown_provider() {
        let error = test_provider_credentials("unknown-provider").unwrap_err();

        assert!(matches!(
            error,
            LanguageModelError::ProviderInvocationFailed {
                provider,
                model,
                detail
            } if provider == "unknown-provider"
                && model == PROVIDER_DEFAULT_MODEL
                && detail.contains("unknown language model provider")
        ));
    }

    #[test]
    fn direct_copilot_provider_reports_http_capabilities() {
        let info = GitHubCopilotProvider::default().info();

        assert_eq!(info.provider, GitHubCopilotProvider::PROVIDER_ID);
        assert_eq!(info.invocation, LanguageModelInvocation::Http);
        assert!(info.supports_json_object);
        assert!(!info.supports_tool_permissions);
    }
}
