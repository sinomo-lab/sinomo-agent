//! Language model provider contracts and integrations.

mod anthropic;
mod api_key;
mod catalog;
mod chat_completions;
mod claude;
mod claude_code;
#[cfg(feature = "cli")]
pub mod cli;
mod codex;
mod codex_cli;
mod continuation;
mod deepseek;
mod gemini;
mod github_copilot;
mod local_responses;
mod nvidia;
mod openai;
mod realtime_audio;
#[cfg(feature = "webrtc-media")]
pub mod realtime_webrtc;
mod responses_tools;
#[cfg(feature = "test-cases")]
mod test_cases;
mod vendor_api;

pub use anthropic::{ANTHROPIC_DEFAULT_MODEL, AnthropicConfig, AnthropicProvider};
pub use claude::{
    CLAUDE_DEFAULT_MODEL, CLAUDE_OAUTH_AUTHORIZE_URL, CLAUDE_OAUTH_CLIENT_ID,
    CLAUDE_OAUTH_REDIRECT_URI, CLAUDE_PROVIDER_ID, ClaudeConfig, ClaudeOAuthTokens,
    ClaudePkceLogin, ClaudeProvider, SINOMO_CLAUDE_CREDENTIALS_FILE_ENV_VAR,
};
pub use claude_code::{
    CLAUDE_CODE_DEFAULT_MODEL, ClaudeCodeConfig, ClaudeCodeProvider, ClaudePermissionMode,
};
pub use codex::{
    CODEX_AUTH_CLIENT_ID, CODEX_AUTH_ISSUER, CODEX_CHATGPT_API_BASE_URL, CODEX_DEFAULT_IMAGE_MODEL,
    CODEX_DEFAULT_MODEL, CODEX_MODELS_CLIENT_VERSION, CODEX_PROVIDER_ID, CodexChatGptTokens,
    CodexClient, CodexConfig, CodexContextManager, CodexDeviceCode, CodexDevicePoll, CodexMessage,
    CodexModelInfo, CodexModelRequest, CodexModelResponse, CodexPromptBuilder, CodexProvider,
    CodexReasoningLevel, CodexRole, CodexServiceTier, CodexTool, CodexToolCall,
    CodexToolDescriptor, CodexToolRegistry, CodexToolResult, DefaultCodexContextManager,
    DefaultCodexPromptBuilder, OpenAiResponsesCodexClient, OpenAiResponsesCodexConfig,
    SINOMO_CODEX_CREDENTIALS_FILE_ENV_VAR, codex_builtin_model_catalog,
    codex_credentials_file_in_data_dir,
};
pub use codex_cli::{
    CODEX_CLI_DEFAULT_MODEL, CodexCliConfig, CodexCliProvider, CodexColor, CodexSandboxMode,
};
pub use deepseek::{
    DEEPSEEK_DEFAULT_MODEL, DEEPSEEK_V4_FLASH_MODEL, DEEPSEEK_V4_PRO_MODEL, DeepSeekConfig,
    DeepSeekProvider,
};
pub use gemini::{GEMINI_DEFAULT_MODEL, GeminiConfig, GeminiProvider};
#[cfg(test)]
pub(crate) use gemini::{
    extract_gemini_response_text, extract_gemini_usage, gemini_generate_content_payload,
};
pub use github_copilot::{
    GITHUB_COPILOT_CLIENT_ID, GitHubCopilotCliConfig, GitHubCopilotCliProvider,
    GitHubCopilotConfig, GitHubCopilotProvider, GitHubDeviceCode, GitHubDevicePoll,
};
pub use local_responses::{
    LM_STUDIO_DEFAULT_API_BASE_URL, LM_STUDIO_DEFAULT_MODEL, LM_STUDIO_PROVIDER_ID,
    LocalResponsesConfig, LocalResponsesProvider, LocalResponsesRuntime,
    OLLAMA_DEFAULT_API_BASE_URL, OLLAMA_DEFAULT_MODEL, OLLAMA_PROVIDER_ID,
};
pub use nvidia::{NVIDIA_DEFAULT_MODEL, NvidiaConfig, NvidiaProvider, normalize_nvidia_model_id};
pub use openai::{
    OPENAI_DEFAULT_IMAGE_MODEL, OPENAI_DEFAULT_MODEL, OPENAI_DEFAULT_WEB_SEARCH_MODEL,
    OPENAI_GPT_5_6_MAX_CONTEXT_WINDOW_TOKENS, OPENAI_LONG_CONTEXT_PRICE_THRESHOLD_TOKENS,
    OPENAI_MAX_IMAGE_REFERENCES, OpenAiConfig, OpenAiImageRequest, OpenAiImageResponse,
    OpenAiProvider, OpenAiWebSearchContextSize, OpenAiWebSearchRequest, OpenAiWebSearchResponse,
    OpenAiWebSearchSource,
};
pub use realtime_audio::{
    RealtimeAudioError, RealtimeAudioEvent, RealtimeAudioInputChunk, RealtimeAudioPlane,
    RealtimeAudioProvider, RealtimeAudioSampleFormat, RealtimeAudioSession,
    RealtimeAudioSessionConfig, RealtimeAudioToolDefinition, RealtimeAudioTranscript,
    RealtimeAudioTranscriptEntry, RealtimeAudioTranscriptRole, RealtimeAudioTransport,
    RealtimeAudioTurnDetection, RealtimeEncodedVideoFrameInput, RealtimeMediaTransport,
    RealtimeSession, RealtimeSessionFuture, RealtimeVideoFrameDetail, RealtimeVideoFrameFormat,
    RealtimeVideoFrameInput, RealtimeVideoInputConfig, RealtimeVideoPixelFormat,
    RealtimeWebrtcCall, connect_realtime_session, create_realtime_webrtc_call,
    minicpm_input_audio_append_payload, minicpm_input_video_frame_payload, minicpm_realtime_ws_url,
    minicpm_realtime_ws_url_for_video, minicpm_session_update_payload,
    openai_input_audio_append_payload, openai_input_video_frame_payload,
    openai_output_truncate_payload, openai_realtime_ws_url, openai_response_cancel_payload,
    openai_session_update_payload, openai_tool_result_payload, parse_realtime_audio_event,
    realtime_audio_event_from_value, realtime_call_session_payload, realtime_sideband_ws_url,
};
#[cfg(feature = "test-cases")]
pub use test_cases::register_lm_test_cases;
pub use vendor_api::{
    GROK_DEFAULT_MODEL, GROK_PROVIDER_ID, KIMI_DEFAULT_MODEL, KIMI_PROVIDER_ID, VendorApi,
    VendorApiConfig, VendorApiProvider,
};

use serde::{Deserialize, Serialize};
use sinomo_runtime::config::{
    ConfigNumberRange, ConfigSchema, ConfigSchemaChoice, ConfigSchemaField, ConfigSchemaValue,
};
use sinomo_runtime::kv::{KvEntry, KvSnapshot, KvTxn, decode_kv_value};
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::node::{LocalNodePermission, LocalNodePermissionStore};
use std::env;
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub use continuation::LanguageModelProviderContinuation;
pub(crate) use continuation::{provider_request_items, response_continuation};

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
    /// Opaque, provider/model-bound state required to replay a signed model turn.
    ProviderContinuation {
        continuation: LanguageModelProviderContinuation,
    },
    Message {
        role: LanguageModelRole,
        content: String,
    },
    ToolCall {
        call_id: String,
        name: String,
        #[serde(default)]
        input: serde_json::Value,
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

    pub fn tool_call(
        call_id: impl Into<String>,
        name: impl Into<String>,
        input: serde_json::Value,
    ) -> Self {
        Self::ToolCall {
            call_id: call_id.into(),
            name: name.into(),
            input,
        }
    }

    pub fn tool_call_output(
        call_id: impl Into<String>,
        name: impl Into<String>,
        output: serde_json::Value,
    ) -> Self {
        Self::ToolCallOutput {
            call_id: call_id.into(),
            name: name.into(),
            output,
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

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct LanguageModelCustomToolSpec {
    pub format: LanguageModelCustomToolFormat,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_input_argument: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LanguageModelCustomToolFormat {
    Text,
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
    ProviderContinuation {
        continuation: LanguageModelProviderContinuation,
    },
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
    ReasoningDelta {
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
pub struct LanguageModelToolAdapterOutput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<LanguageModelToolCall>,
    #[serde(default)]
    pub source: LanguageModelToolAdapterSource,
}

impl LanguageModelToolAdapterOutput {
    pub fn is_empty(&self) -> bool {
        self.tool_calls.is_empty()
    }
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum LanguageModelToolAdapterSource {
    #[default]
    None,
    Native,
    VendorFallback,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LanguageModelToolAdapter {
    fallback_syntaxes: &'static [LanguageModelVendorToolSyntax],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LanguageModelVendorToolSyntax {
    Dsml,
}

const DEFAULT_VENDOR_TOOL_SYNTAXES: &[LanguageModelVendorToolSyntax] =
    &[LanguageModelVendorToolSyntax::Dsml];
const DSML_OPEN_PREFIX: &str = "<\u{ff5c}\u{ff5c}DSML\u{ff5c}\u{ff5c}";
const DSML_CLOSE_PREFIX: &str = "</\u{ff5c}\u{ff5c}DSML\u{ff5c}\u{ff5c}";

pub fn language_model_tool_adapter(_provider: &str, _model: &str) -> LanguageModelToolAdapter {
    LanguageModelToolAdapter {
        fallback_syntaxes: DEFAULT_VENDOR_TOOL_SYNTAXES,
    }
}

pub fn language_model_prefers_native_tool_protocol(provider: &str, model: &str) -> bool {
    let provider =
        normalize_model_provider(provider).unwrap_or_else(|| provider.to_ascii_lowercase());
    match provider.as_str() {
        OpenAiProvider::PROVIDER_ID => openai::prefers_native_tool_protocol(model),
        CODEX_PROVIDER_ID => codex::prefers_native_tool_protocol(model),
        DeepSeekProvider::PROVIDER_ID => deepseek::prefers_native_tool_protocol(model),
        AnthropicProvider::PROVIDER_ID
        | ClaudeProvider::PROVIDER_ID
        | GeminiProvider::PROVIDER_ID
        | NvidiaProvider::PROVIDER_ID
        | GROK_PROVIDER_ID
        | KIMI_PROVIDER_ID
        | GitHubCopilotProvider::PROVIDER_ID
        | OLLAMA_PROVIDER_ID
        | LM_STUDIO_PROVIDER_ID => true,
        _ => false,
    }
}

pub fn language_model_protocol_system_instructions(
    provider: &str,
    model: &str,
    base: &str,
) -> String {
    if !language_model_prefers_native_tool_protocol(provider, model) {
        return base.to_string();
    }
    format!(
        "{base}\n\nModel protocol hint: native tool calls are available for this provider/model. Prefer the provider-native tool protocol for tool use; use JSON `tool_calls` only as a fallback. After native tool results are returned, continue with the required final response format."
    )
}

pub fn language_model_system_instructions(
    provider: &str,
    model: &str,
    base: &str,
    output_contract: Option<&str>,
) -> String {
    let mut instructions = language_model_protocol_system_instructions(provider, model, base);
    if let Some(output_contract) = output_contract
        && !output_contract.trim().is_empty()
    {
        instructions.push_str("\n\nResponse contract:\n");
        instructions.push_str(output_contract.trim());
    }
    instructions
}

pub fn collect_language_model_tool_calls(
    provider: &str,
    model: &str,
    response: &LanguageModelResponse,
    stream_events: &[LanguageModelStreamEvent],
    tools: &[LanguageModelToolSpec],
) -> LanguageModelToolAdapterOutput {
    language_model_tool_adapter(provider, model).collect(response, stream_events, tools)
}

impl LanguageModelToolAdapter {
    pub fn collect(
        &self,
        response: &LanguageModelResponse,
        stream_events: &[LanguageModelStreamEvent],
        tools: &[LanguageModelToolSpec],
    ) -> LanguageModelToolAdapterOutput {
        let mut tool_calls = native_language_model_tool_calls(response, stream_events, tools);
        let mut text = None;
        let mut source = if tool_calls.is_empty() {
            LanguageModelToolAdapterSource::None
        } else {
            LanguageModelToolAdapterSource::Native
        };
        if tool_calls.is_empty() {
            for syntax in self.fallback_syntaxes {
                let fallback = match syntax {
                    LanguageModelVendorToolSyntax::Dsml => {
                        parse_dsml_tool_adapter_output(&response.content)
                    }
                };
                if !fallback.tool_calls.is_empty() {
                    text = fallback.text;
                    tool_calls = fallback.tool_calls;
                    source = LanguageModelToolAdapterSource::VendorFallback;
                    break;
                }
            }
        }
        LanguageModelToolAdapterOutput {
            text,
            tool_calls,
            source,
        }
    }
}

fn native_language_model_tool_calls(
    response: &LanguageModelResponse,
    stream_events: &[LanguageModelStreamEvent],
    tools: &[LanguageModelToolSpec],
) -> Vec<LanguageModelToolCall> {
    let mut calls = Vec::new();
    for call in &response.tool_calls {
        push_language_model_tool_call(&mut calls, call, &response.provider, tools);
    }
    for item in &response.items {
        if let LanguageModelResponseItem::ToolCall { call } = item {
            push_language_model_tool_call(&mut calls, call, &response.provider, tools);
        }
    }
    for event in stream_events {
        if let LanguageModelStreamEvent::ToolCallCompleted {
            call_id,
            name,
            input,
        } = event
        {
            push_language_model_tool_call(
                &mut calls,
                &LanguageModelToolCall {
                    call_id: call_id.clone(),
                    name: name.clone(),
                    input: input.clone(),
                    summary: None,
                },
                &response.provider,
                tools,
            );
        }
    }
    calls
}

fn push_language_model_tool_call(
    calls: &mut Vec<LanguageModelToolCall>,
    call: &LanguageModelToolCall,
    provider: &str,
    tools: &[LanguageModelToolSpec],
) {
    let input = normalize_language_model_tool_call_input(call, provider, tools);
    if calls
        .iter()
        .any(|existing| existing.name == call.name && existing.input == input)
    {
        return;
    }
    calls.push(LanguageModelToolCall {
        call_id: call.call_id.clone(),
        name: call.name.clone(),
        input,
        summary: call.summary.clone(),
    });
}

fn normalize_language_model_tool_call_input(
    call: &LanguageModelToolCall,
    provider: &str,
    tools: &[LanguageModelToolSpec],
) -> serde_json::Value {
    if !call.input.is_string() {
        return call.input.clone();
    }
    let Some(raw_input) = call.input.as_str() else {
        return call.input.clone();
    };
    let Some(raw_input_argument) = tools
        .iter()
        .find(|tool| tool.name == call.name)
        .and_then(|tool| provider_tool_raw_input_argument(tool, provider))
    else {
        return call.input.clone();
    };
    serde_json::json!({ raw_input_argument: raw_input })
}

fn provider_tool_raw_input_argument<'a>(
    tool: &'a LanguageModelToolSpec,
    provider: &str,
) -> Option<&'a str> {
    tool.provider_specs
        .iter()
        .find(|spec| spec.provider.eq_ignore_ascii_case(provider))
        .and_then(|spec| spec.custom.as_ref())
        .and_then(|custom| custom.raw_input_argument.as_deref())
}

fn parse_dsml_tool_adapter_output(raw: &str) -> LanguageModelToolAdapterOutput {
    let tool_calls = parse_dsml_tool_calls(raw);
    let text = if tool_calls.is_empty() {
        None
    } else {
        Some(remove_dsml_tool_call_blocks(raw).trim().to_string())
    };
    let source = if tool_calls.is_empty() {
        LanguageModelToolAdapterSource::None
    } else {
        LanguageModelToolAdapterSource::VendorFallback
    };
    LanguageModelToolAdapterOutput {
        text,
        tool_calls,
        source,
    }
}

fn parse_dsml_tool_calls(raw: &str) -> Vec<LanguageModelToolCall> {
    let tool_calls_start = format!("{DSML_OPEN_PREFIX}tool_calls>");
    let tool_calls_end = format!("{DSML_CLOSE_PREFIX}tool_calls>");
    let mut calls = Vec::new();
    let mut remaining = raw;
    while let Some(start) = remaining.find(&tool_calls_start) {
        let after_start = &remaining[start + tool_calls_start.len()..];
        let Some(end) = after_start.find(&tool_calls_end) else {
            break;
        };
        calls.extend(parse_dsml_invocations(&after_start[..end]));
        remaining = &after_start[end + tool_calls_end.len()..];
    }
    calls
}

fn parse_dsml_invocations(raw: &str) -> Vec<LanguageModelToolCall> {
    let invoke_start = format!("{DSML_OPEN_PREFIX}invoke name=\"");
    let invoke_end = format!("{DSML_CLOSE_PREFIX}invoke>");
    let mut calls = Vec::new();
    let mut remaining = raw;
    while let Some(start) = remaining.find(&invoke_start) {
        let after_start = &remaining[start + invoke_start.len()..];
        let Some(name_end) = after_start.find('"') else {
            break;
        };
        let name = after_start[..name_end].trim();
        if name.is_empty() {
            remaining = &after_start[name_end..];
            continue;
        }
        let Some(tag_end) = after_start[name_end..].find('>') else {
            break;
        };
        let body_start = name_end + tag_end + 1;
        let after_open = &after_start[body_start..];
        let Some(body_end) = after_open.find(&invoke_end) else {
            break;
        };
        let body = &after_open[..body_end];
        calls.push(LanguageModelToolCall {
            call_id: None,
            name: name.to_string(),
            input: parse_dsml_parameters(body),
            summary: Some(format!("Recovered DSML tool call `{name}`.")),
        });
        remaining = &after_open[body_end + invoke_end.len()..];
    }
    calls
}

fn parse_dsml_parameters(body: &str) -> serde_json::Value {
    let parameter_start = format!("{DSML_OPEN_PREFIX}parameter name=\"");
    let parameter_end = format!("{DSML_CLOSE_PREFIX}parameter>");
    let mut map = serde_json::Map::new();
    let mut remaining = body;
    while let Some(start) = remaining.find(&parameter_start) {
        let after_start = &remaining[start + parameter_start.len()..];
        let Some(name_end) = after_start.find('"') else {
            break;
        };
        let name = after_start[..name_end].trim();
        let Some(tag_end) = after_start[name_end..].find('>') else {
            break;
        };
        let tag_tail = &after_start[name_end..name_end + tag_end];
        let value_start = name_end + tag_end + 1;
        let after_open = &after_start[value_start..];
        let Some(value_end) = after_open.find(&parameter_end) else {
            break;
        };
        if !name.is_empty() {
            let value_text = &after_open[..value_end];
            map.insert(
                name.to_string(),
                parse_dsml_parameter_value(tag_tail, value_text),
            );
        }
        remaining = &after_open[value_end + parameter_end.len()..];
    }
    serde_json::Value::Object(map)
}

fn parse_dsml_parameter_value(tag_tail: &str, value: &str) -> serde_json::Value {
    if tag_tail.contains("string=\"false\"") {
        serde_json::from_str(value.trim())
            .unwrap_or_else(|_| serde_json::Value::String(value.to_string()))
    } else {
        serde_json::Value::String(value.to_string())
    }
}

fn remove_dsml_tool_call_blocks(raw: &str) -> String {
    let tool_calls_start = format!("{DSML_OPEN_PREFIX}tool_calls>");
    let tool_calls_end = format!("{DSML_CLOSE_PREFIX}tool_calls>");
    let mut output = String::new();
    let mut remaining = raw;
    while let Some(start) = remaining.find(&tool_calls_start) {
        output.push_str(&remaining[..start]);
        let after_start = &remaining[start + tool_calls_start.len()..];
        let Some(end) = after_start.find(&tool_calls_end) else {
            return output;
        };
        remaining = &after_start[end + tool_calls_end.len()..];
    }
    output.push_str(remaining);
    output
}

pub use sinomo_runtime::api::lm::LanguageModelProviderUsage;

pub use sinomo_runtime::api::lm::LanguageModelUsageLimit;

pub use sinomo_runtime::api::lm::LanguageModelCreditUsage;

pub trait LanguageModelUsageProvider {
    fn usage(&self) -> Result<LanguageModelProviderUsage, LanguageModelError>;
}

pub use sinomo_runtime::api::lm::ModelConfig;

pub use sinomo_runtime::api::lm::ProviderAccountConfig;

pub use sinomo_runtime::api::lm::LanguageModelProviderOptions;

pub use sinomo_runtime::api::lm::LanguageModelModelOptions;

pub use sinomo_runtime::api::lm::ProviderReasoningEffort;

pub use sinomo_runtime::api::lm::ProviderTextVerbosity;

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

pub use sinomo_runtime::api::lm::DEFAULT_PROVIDER_ACCOUNT;
pub use sinomo_runtime::api::lm::LM_PROVIDER_CONFIG_KEY;
pub use sinomo_runtime::api::lm::LM_PROVIDER_HEALTH_CONFIG_KEY;
pub const LM_RUNTIME_CONFIG_KEY: &str = "lm.runtime";
pub const LM_PROVIDER_CONFIG_KV_KEY: &str = "/config/lm/providers";
pub const LM_PROVIDER_HEALTH_CONFIG_KV_KEY: &str = "/config/lm/provider_health";
pub const LM_RUNTIME_CONFIG_KV_KEY: &str = "/config/lm/runtime";
const LM_CREDENTIALS_DOCUMENT_PREFIX: &str = "lm.credentials";

fn default_provider_account_name() -> String {
    DEFAULT_PROVIDER_ACCOUNT.to_string()
}

fn provider_account_enabled_default() -> bool {
    true
}

pub use sinomo_runtime::api::lm::LanguageModelProviderHealthConfig;

pub use sinomo_runtime::api::lm::LanguageModelProviderHealthSnapshot;

pub use sinomo_runtime::api::lm::provider_health_account_key;

pub use sinomo_runtime::api::lm::normalize_provider_account_name;

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
        ClaudeProvider::PROVIDER_ID => claude::inferred_context_window_tokens(&model),
        ClaudeCodeProvider::PROVIDER_ID => claude_code::inferred_context_window_tokens(&model),
        AnthropicProvider::PROVIDER_ID => anthropic::inferred_context_window_tokens(&model),
        OpenAiProvider::PROVIDER_ID => openai::inferred_context_window_tokens(&model),
        DeepSeekProvider::PROVIDER_ID => deepseek::inferred_context_window_tokens(&model),
        GeminiProvider::PROVIDER_ID => gemini::inferred_context_window_tokens(&model),
        NvidiaProvider::PROVIDER_ID => nvidia::inferred_context_window_tokens(&model),
        GROK_PROVIDER_ID if model.starts_with("grok-4.7") || model.starts_with("grok-4.6") => {
            Some(500_000)
        }
        KIMI_PROVIDER_ID if model.starts_with("kimi-k3") => Some(1_000_000),
        _ => None,
    }
}

pub fn inferred_model_max_context_window_tokens(provider: &str, model: &str) -> Option<usize> {
    let provider = normalize_model_provider(provider)?;
    let model = effective_model_name(&provider, model);
    match provider.as_str() {
        CODEX_PROVIDER_ID | CodexCliProvider::PROVIDER_ID => {
            codex::inferred_max_context_window_tokens(&model)
        }
        OpenAiProvider::PROVIDER_ID => openai::inferred_max_context_window_tokens(&model),
        _ => inferred_model_context_window_tokens(&provider, &model),
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
            config: model_config_definition()
                .map(|definition| definition.local_path(data_dir))
                .unwrap_or_else(|_| {
                    sinomo_runtime::config::configs_dir(data_dir)
                        .join("lm")
                        .join("providers.json")
                }),
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
    let value: serde_json::Value = serde_json::from_str(&content).map_err(|source| {
        LanguageModelError::RequestSerialization {
            detail: format!(
                "failed to parse model config at {}: {source}",
                paths.config.display()
            ),
        }
    })?;
    model_config_from_json_value(value).map_err(|source| LanguageModelError::RequestSerialization {
        detail: format!(
            "failed to parse model config at {}: {source}",
            paths.config.display()
        ),
    })
}

fn closed_config_object(fields: Vec<ConfigSchemaField>) -> ConfigSchemaValue {
    ConfigSchemaValue::object(fields).without_unknown_fields()
}

fn optional_config_field(
    name: impl Into<String>,
    label: impl Into<String>,
    description: impl Into<String>,
    value: ConfigSchemaValue,
) -> ConfigSchemaField {
    ConfigSchemaField::new(name, label, description, value).optional()
}

fn reasoning_effort_schema() -> ConfigSchemaValue {
    ConfigSchemaValue::string_choices(vec![
        ConfigSchemaChoice::new("none", "None"),
        ConfigSchemaChoice::new("minimal", "Minimal"),
        ConfigSchemaChoice::new("low", "Low"),
        ConfigSchemaChoice::new("medium", "Medium"),
        ConfigSchemaChoice::new("high", "High"),
        ConfigSchemaChoice::new("xhigh", "Extra high"),
        ConfigSchemaChoice::new("max", "Maximum"),
        ConfigSchemaChoice::new("ultra", "Ultra"),
    ])
}

fn text_verbosity_schema() -> ConfigSchemaValue {
    ConfigSchemaValue::string_choices(vec![
        ConfigSchemaChoice::new("low", "Low"),
        ConfigSchemaChoice::new("medium", "Medium"),
        ConfigSchemaChoice::new("high", "High"),
    ])
}

fn provider_options_schema() -> ConfigSchemaValue {
    closed_config_object(vec![
        optional_config_field(
            "reasoning_effort",
            "Reasoning effort",
            "Preferred reasoning effort when the provider and model support it.",
            reasoning_effort_schema(),
        ),
        optional_config_field(
            "reasoning_budget_tokens",
            "Reasoning token budget",
            "Explicit reasoning token budget for providers that expose one.",
            ConfigSchemaValue::u64(Some(ConfigNumberRange {
                min: Some(0.0),
                max: Some(u32::MAX as f64),
            })),
        ),
        optional_config_field(
            "text_verbosity",
            "Text verbosity",
            "Preferred response verbosity when the provider supports it.",
            text_verbosity_schema(),
        ),
        optional_config_field(
            "max_context_tokens",
            "Maximum context tokens",
            "Optional user cap for the model context window. Lower limits can reduce long-context cost and usage.",
            ConfigSchemaValue::u64(Some(ConfigNumberRange {
                min: Some(1.0),
                max: Some(u32::MAX as f64),
            })),
        ),
        optional_config_field(
            "auto_compact_token_limit",
            "Automatic compaction token limit",
            "Optional context size at which Sinomo compacts older conversation history.",
            ConfigSchemaValue::u64(Some(ConfigNumberRange {
                min: Some(1.0),
                max: Some(u32::MAX as f64),
            })),
        ),
        optional_config_field(
            "vendor_options",
            "Vendor options",
            "Provider-specific string options keyed by the vendor option name.",
            ConfigSchemaValue::map(ConfigSchemaValue::string()),
        ),
    ])
}

fn provider_account_schema() -> ConfigSchemaValue {
    closed_config_object(vec![
        ConfigSchemaField::new(
            "provider",
            "Provider",
            "Stable provider id, such as openai, anthropic, codex, or deepseek.",
            ConfigSchemaValue::string(),
        ),
        optional_config_field(
            "account",
            "Account",
            "Account name within this provider; omitted legacy values resolve to default.",
            ConfigSchemaValue::string(),
        )
        .with_default_value(serde_json::Value::String(default_provider_account_name())),
        optional_config_field(
            "default_model",
            "Default model",
            "Model selected for this account when no model is requested explicitly.",
            ConfigSchemaValue::string(),
        ),
        optional_config_field(
            "api_base_url",
            "API base URL",
            "Optional provider API endpoint override.",
            ConfigSchemaValue::string(),
        ),
        optional_config_field(
            "api_key",
            "API key",
            "Secret API credential for this provider account.",
            ConfigSchemaValue::secret_string(),
        ),
        optional_config_field(
            "enabled",
            "Enabled",
            "Whether this account may be selected for model requests.",
            ConfigSchemaValue::bool(),
        )
        .with_default_value(serde_json::Value::Bool(provider_account_enabled_default())),
        optional_config_field(
            "options",
            "Provider options",
            "Default reasoning, verbosity, and vendor-specific options for this account.",
            provider_options_schema(),
        ),
        optional_config_field(
            "model_options",
            "Model overrides",
            "Per-model option overrides applied after the account defaults.",
            ConfigSchemaValue::list(closed_config_object(vec![
                ConfigSchemaField::new(
                    "model",
                    "Model",
                    "Model id to which these overrides apply.",
                    ConfigSchemaValue::string(),
                ),
                optional_config_field(
                    "options",
                    "Options",
                    "Reasoning, verbosity, and vendor-specific overrides for this model.",
                    provider_options_schema(),
                ),
            ])),
        ),
    ])
}

pub fn model_config_schema() -> ConfigSchema {
    ConfigSchema::object(
        "ModelConfig",
        "Language-model provider accounts, defaults, credentials, and per-model options.",
        vec![
            optional_config_field(
                "default_provider",
                "Default provider",
                "Provider selected when a request does not name one.",
                ConfigSchemaValue::string(),
            ),
            optional_config_field(
                "default_account",
                "Default account",
                "Account selected within the default provider.",
                ConfigSchemaValue::string(),
            ),
            optional_config_field(
                "default_model",
                "Default model",
                "Model selected when a request does not name one.",
                ConfigSchemaValue::string(),
            ),
            optional_config_field(
                "accounts",
                "Provider accounts",
                "Configured provider accounts and their model-specific behavior.",
                ConfigSchemaValue::list(provider_account_schema()),
            ),
        ],
    )
}

pub fn model_runtime_config_schema() -> ConfigSchema {
    ConfigSchema::object(
        "ModelRuntimeConfig",
        "Runtime locations and execution controls for native and bridged language models.",
        vec![
            optional_config_field(
                "native_models_cache_dir",
                "Native model cache",
                "Optional directory containing cached native model assets.",
                ConfigSchemaValue::string(),
            ),
            optional_config_field(
                "minicpm_v46",
                "MiniCPM v4.6",
                "Local MiniCPM model location and token limits.",
                closed_config_object(vec![
                    optional_config_field(
                        "model_dir",
                        "Model directory",
                        "Directory containing the MiniCPM model files.",
                        ConfigSchemaValue::string(),
                    ),
                    optional_config_field(
                        "max_input_tokens",
                        "Maximum input tokens",
                        "Optional upper bound for one MiniCPM request's input tokens.",
                        ConfigSchemaValue::u64(Some(ConfigNumberRange {
                            min: Some(1.0),
                            max: None,
                        })),
                    ),
                    optional_config_field(
                        "max_output_tokens",
                        "Maximum output tokens",
                        "Optional upper bound for one MiniCPM response's output tokens.",
                        ConfigSchemaValue::u64(Some(ConfigNumberRange {
                            min: Some(1.0),
                            max: None,
                        })),
                    ),
                ]),
            ),
            optional_config_field(
                "pytorch_bridge",
                "PyTorch bridge",
                "Endpoint and local process controls for the PyTorch model bridge.",
                closed_config_object(vec![
                    optional_config_field(
                        "endpoint",
                        "Endpoint",
                        "HTTP or socket endpoint exposed by the PyTorch bridge.",
                        ConfigSchemaValue::string(),
                    ),
                    optional_config_field(
                        "python",
                        "Python executable",
                        "Python interpreter used when starting a local bridge.",
                        ConfigSchemaValue::string(),
                    ),
                    optional_config_field(
                        "device",
                        "Device",
                        "PyTorch device selector, such as cpu, cuda, or mps.",
                        ConfigSchemaValue::string(),
                    ),
                    optional_config_field(
                        "timeout_seconds",
                        "Timeout",
                        "Maximum number of seconds to wait for a bridge operation.",
                        ConfigSchemaValue::u64(Some(ConfigNumberRange {
                            min: Some(1.0),
                            max: None,
                        })),
                    ),
                    optional_config_field(
                        "mock",
                        "Mock bridge",
                        "Use the deterministic mock bridge instead of a live model runtime.",
                        ConfigSchemaValue::bool(),
                    ),
                ]),
            ),
        ],
    )
}

fn usage_limit_schema() -> ConfigSchemaValue {
    closed_config_object(vec![
        ConfigSchemaField::new(
            "id",
            "Limit id",
            "Provider-defined stable id for this usage limit.",
            ConfigSchemaValue::string(),
        ),
        optional_config_field(
            "name",
            "Name",
            "Human-readable limit name.",
            ConfigSchemaValue::string(),
        ),
        optional_config_field(
            "scope",
            "Scope",
            "Provider-defined scope to which this limit applies.",
            ConfigSchemaValue::string(),
        ),
        optional_config_field(
            "unlimited",
            "Unlimited",
            "Whether this limit is unbounded.",
            ConfigSchemaValue::bool(),
        ),
        optional_config_field(
            "allowed",
            "Allowed",
            "Whether the account is currently allowed to consume this resource.",
            ConfigSchemaValue::bool(),
        ),
        optional_config_field(
            "exhausted",
            "Exhausted",
            "Whether the current allowance is exhausted.",
            ConfigSchemaValue::bool(),
        ),
        optional_config_field(
            "remaining",
            "Remaining",
            "Provider-reported remaining quantity.",
            ConfigSchemaValue::f64(None),
        ),
        optional_config_field(
            "entitlement",
            "Entitlement",
            "Provider-reported total entitlement for the window.",
            ConfigSchemaValue::f64(None),
        ),
        optional_config_field(
            "used_percent",
            "Used percent",
            "Percentage of the allowance consumed.",
            ConfigSchemaValue::f64(None),
        ),
        optional_config_field(
            "remaining_percent",
            "Remaining percent",
            "Percentage of the allowance remaining.",
            ConfigSchemaValue::f64(None),
        ),
        optional_config_field(
            "window_seconds",
            "Window seconds",
            "Length of the accounting window in seconds.",
            ConfigSchemaValue::u64(None),
        ),
        optional_config_field(
            "reset_at_unix",
            "Reset time",
            "Unix timestamp at which the allowance resets.",
            ConfigSchemaValue::i64(None),
        ),
    ])
}

fn credit_usage_schema() -> ConfigSchemaValue {
    closed_config_object(vec![
        optional_config_field(
            "has_credits",
            "Has credits",
            "Whether the provider reports a usable credit balance.",
            ConfigSchemaValue::bool(),
        ),
        optional_config_field(
            "unlimited",
            "Unlimited credits",
            "Whether credit-backed usage is unbounded.",
            ConfigSchemaValue::bool(),
        ),
        optional_config_field(
            "balance",
            "Balance",
            "Provider-formatted remaining credit balance.",
            ConfigSchemaValue::string(),
        ),
    ])
}

fn provider_usage_schema() -> ConfigSchemaValue {
    closed_config_object(vec![
        ConfigSchemaField::new(
            "provider",
            "Provider",
            "Provider that reported this usage snapshot.",
            ConfigSchemaValue::string(),
        ),
        optional_config_field(
            "plan",
            "Plan",
            "Provider-reported subscription or billing plan.",
            ConfigSchemaValue::string(),
        ),
        ConfigSchemaField::new(
            "captured_at_unix",
            "Captured at",
            "Unix timestamp when usage was captured.",
            ConfigSchemaValue::u64(None),
        ),
        optional_config_field(
            "limits",
            "Usage limits",
            "Provider-reported quota and rate-limit windows.",
            ConfigSchemaValue::list(usage_limit_schema()),
        ),
        optional_config_field(
            "credits",
            "Credits",
            "Provider-reported credit availability and balance.",
            credit_usage_schema(),
        ),
    ])
}

fn provider_health_snapshot_schema() -> ConfigSchemaValue {
    closed_config_object(vec![
        ConfigSchemaField::new(
            "provider",
            "Provider",
            "Provider id associated with this health snapshot.",
            ConfigSchemaValue::string(),
        ),
        ConfigSchemaField::new(
            "account",
            "Account",
            "Provider account associated with this health snapshot.",
            ConfigSchemaValue::string(),
        ),
        ConfigSchemaField::new(
            "checked_at_unix",
            "Checked at",
            "Unix timestamp of the most recent health check attempt.",
            ConfigSchemaValue::i64(None),
        ),
        optional_config_field(
            "last_verified_at_unix",
            "Last verified at",
            "Unix timestamp of the most recent successful verification.",
            ConfigSchemaValue::i64(None),
        ),
        ConfigSchemaField::new(
            "healthy",
            "Healthy",
            "Whether the most recent provider verification succeeded.",
            ConfigSchemaValue::bool(),
        ),
        optional_config_field(
            "error",
            "Verification error",
            "Error reported by the most recent provider verification.",
            ConfigSchemaValue::string(),
        ),
        optional_config_field(
            "models",
            "Discovered models",
            "Model ids discovered for this provider account.",
            ConfigSchemaValue::StringList,
        ),
        optional_config_field(
            "model_discovery_error",
            "Model discovery error",
            "Error encountered while discovering models.",
            ConfigSchemaValue::string(),
        ),
        optional_config_field(
            "usage",
            "Usage",
            "Provider usage, quota, and credit snapshot.",
            provider_usage_schema(),
        ),
        optional_config_field(
            "usage_error",
            "Usage error",
            "Error encountered while reading provider usage.",
            ConfigSchemaValue::string(),
        ),
        optional_config_field(
            "stale",
            "Stale",
            "Whether this snapshot should be refreshed before it is trusted.",
            ConfigSchemaValue::bool(),
        ),
    ])
}

pub fn provider_health_config_schema() -> ConfigSchema {
    ConfigSchema::object(
        "LanguageModelProviderHealthConfig",
        "Read-only provider verification, model discovery, quota, and credit snapshots.",
        vec![optional_config_field(
            "accounts",
            "Account health",
            "Health snapshots keyed by normalized provider and account id.",
            ConfigSchemaValue::map(provider_health_snapshot_schema()),
        )],
    )
}

pub fn model_config_definition()
-> Result<sinomo_runtime::config::ConfigDefinition, LanguageModelError> {
    let definition = sinomo_runtime::config::ConfigDefinition::new(
        LM_PROVIDER_CONFIG_KEY,
        "sinomo-lm",
        sinomo_runtime::config::ConfigValueType::Object {
            schema_name: "ModelConfig".to_string(),
        },
        "Language-model provider accounts, defaults, and provider-specific options.",
        sinomo_runtime::config::ConfigClusterMode::ClusterOverrideLocal,
    )
    .map_err(map_lm_config_error)?
    .with_default(&ModelConfig::default())
    .map_err(map_lm_config_error)?
    .with_schema(model_config_schema())
    .map_err(map_lm_config_error)?
    .with_local_legacy_path("config/models.json");
    debug_assert_eq!(definition.cluster_key(), LM_PROVIDER_CONFIG_KV_KEY);
    Ok(definition)
}

pub fn model_runtime_config_definition()
-> Result<sinomo_runtime::config::ConfigDefinition, LanguageModelError> {
    let definition = sinomo_runtime::config::ConfigDefinition::new(
        LM_RUNTIME_CONFIG_KEY,
        "sinomo-lm",
        sinomo_runtime::config::ConfigValueType::Object {
            schema_name: "ModelRuntimeConfig".to_string(),
        },
        "Local and cluster runtime options for native language-model execution.",
        sinomo_runtime::config::ConfigClusterMode::ClusterOverrideLocal,
    )
    .map_err(map_lm_config_error)?
    .with_default(&ModelRuntimeConfig::default())
    .map_err(map_lm_config_error)?
    .with_schema(model_runtime_config_schema())
    .map_err(map_lm_config_error)?
    .with_local_legacy_path("config/models-runtime.json");
    debug_assert_eq!(definition.cluster_key(), LM_RUNTIME_CONFIG_KV_KEY);
    Ok(definition)
}

pub fn provider_health_config_definition()
-> Result<sinomo_runtime::config::ConfigDefinition, LanguageModelError> {
    let definition = sinomo_runtime::config::ConfigDefinition::new(
        LM_PROVIDER_HEALTH_CONFIG_KEY,
        "sinomo-lm",
        sinomo_runtime::config::ConfigValueType::Object {
            schema_name: "LanguageModelProviderHealthConfig".to_string(),
        },
        "Cluster-visible provider verification, model discovery, and usage snapshots.",
        sinomo_runtime::config::ConfigClusterMode::ClusterOverrideLocal,
    )
    .map_err(map_lm_config_error)?
    .with_default(&LanguageModelProviderHealthConfig::default())
    .map_err(map_lm_config_error)?
    .with_schema(provider_health_config_schema())
    .map_err(map_lm_config_error)?
    .read_only();
    debug_assert_eq!(definition.cluster_key(), LM_PROVIDER_HEALTH_CONFIG_KV_KEY);
    Ok(definition)
}

fn provider_credentials_document_key(provider: &str) -> Result<String, LanguageModelError> {
    let provider = normalize_model_provider(provider).unwrap_or_else(|| provider.to_string());
    let key = format!("{LM_CREDENTIALS_DOCUMENT_PREFIX}.{provider}");
    sinomo_runtime::config::ConfigKey::new(key.clone()).map_err(map_lm_config_error)?;
    Ok(key)
}

pub fn register_config_definitions(
    builder: sinomo_runtime::config::ConfigRegistryBuilder,
) -> Result<sinomo_runtime::config::ConfigRegistryBuilder, LanguageModelError> {
    builder
        .register(model_config_definition()?)
        .map_err(map_lm_config_error)?
        .register(model_runtime_config_definition()?)
        .map_err(map_lm_config_error)?
        .register(provider_health_config_definition()?)
        .map_err(map_lm_config_error)
}

pub fn model_config_store() -> Result<sinomo_runtime::config::ConfigStore, LanguageModelError> {
    let registry =
        register_config_definitions(sinomo_runtime::config::ConfigRegistry::builder())?.build();
    Ok(sinomo_runtime::config::ConfigStore::new(registry))
}

fn map_lm_config_error(source: sinomo_runtime::config::ConfigError) -> LanguageModelError {
    LanguageModelError::ConfigStore {
        path: "sinomo-config".to_string(),
        detail: source.to_string(),
    }
}

pub(crate) fn read_provider_credentials_json_value(
    provider: &str,
    credentials_path: &Option<PathBuf>,
) -> Result<Option<serde_json::Value>, LanguageModelError> {
    let Some(path) = credentials_path else {
        return Ok(None);
    };
    match read_provider_credentials_json_value_at(provider, path) {
        Ok(Some(value)) => return Ok(Some(value)),
        Ok(None) => {}
        Err(error) => return Err(error),
    }
    if !credentials_path_is_config_document(provider, path) {
        return Ok(None);
    }
    let Some(legacy_path) = legacy_provider_credentials_path_for(provider, path) else {
        return Ok(None);
    };
    let Some(value) = read_provider_credentials_json_value_at(provider, &legacy_path)? else {
        return Ok(None);
    };
    write_provider_credentials_json_value(provider, credentials_path, value.clone())?;
    Ok(Some(value))
}

pub(crate) fn write_provider_credentials_json_value(
    provider: &str,
    credentials_path: &Option<PathBuf>,
    value: serde_json::Value,
) -> Result<(), LanguageModelError> {
    let Some(path) = credentials_path else {
        return Ok(());
    };
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| LanguageModelError::TokenStore {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let value = if credentials_path_is_config_document(provider, path) {
        serde_json::to_value(sinomo_runtime::config::ConfigDocument::new(
            sinomo_runtime::config::ConfigKey::new(provider_credentials_document_key(provider)?)
                .map_err(map_lm_config_error)?,
            value,
        ))
        .map_err(|source| LanguageModelError::RequestSerialization {
            detail: source.to_string(),
        })?
    } else {
        value
    };
    let content = serde_json::to_string_pretty(&value).map_err(|source| {
        LanguageModelError::RequestSerialization {
            detail: source.to_string(),
        }
    })?;
    fs::write(path, content).map_err(|source| LanguageModelError::TokenStore {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

fn read_provider_credentials_json_value_at(
    provider: &str,
    path: &Path,
) -> Result<Option<serde_json::Value>, LanguageModelError> {
    let content = match fs::read_to_string(path) {
        Ok(content) => content,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(LanguageModelError::TokenStore {
                path: path.display().to_string(),
                detail: source.to_string(),
            });
        }
    };
    let raw_value: serde_json::Value =
        serde_json::from_str(&content).map_err(|source| LanguageModelError::TokenStore {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
    let value =
        match serde_json::from_value::<sinomo_runtime::config::ConfigDocument>(raw_value.clone()) {
            Ok(document)
                if document.key.as_str() == provider_credentials_document_key(provider)? =>
            {
                document.value
            }
            _ => raw_value,
        };
    Ok(Some(value))
}

fn credentials_path_is_config_document(provider: &str, path: &Path) -> bool {
    let expected_file = format!("{provider}.json");
    let Some(file_name) = path.file_name().and_then(|value| value.to_str()) else {
        return false;
    };
    if file_name != expected_file {
        return false;
    }
    let Some(credentials_dir) = path.parent() else {
        return false;
    };
    let Some(lm_dir) = credentials_dir.parent() else {
        return false;
    };
    let Some(configs_dir) = lm_dir.parent() else {
        return false;
    };
    credentials_dir.file_name().and_then(|value| value.to_str()) == Some("credentials")
        && lm_dir.file_name().and_then(|value| value.to_str()) == Some("lm")
        && configs_dir.file_name().and_then(|value| value.to_str()) == Some("configs")
}

fn legacy_provider_credentials_path_for(provider: &str, path: &Path) -> Option<PathBuf> {
    let data_root = path.parent()?.parent()?.parent()?.parent()?;
    Some(
        data_root
            .join("providers")
            .join(provider)
            .join("credentials.json"),
    )
}

fn model_config_from_json_value(
    value: serde_json::Value,
) -> Result<ModelConfig, serde_json::Error> {
    match serde_json::from_value::<sinomo_runtime::config::ConfigDocument>(value.clone()) {
        Ok(document) if document.key.as_str() == LM_PROVIDER_CONFIG_KEY => {
            serde_json::from_value(document.value)
        }
        _ => serde_json::from_value(value),
    }
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
    let definition = model_config_definition()?;
    match sinomo_runtime::config::decode_cluster_config_entry(entry, &definition) {
        Ok(value) => {
            serde_json::from_value(value).map_err(|source| LanguageModelError::ConfigStore {
                path: entry.key.clone(),
                detail: source.to_string(),
            })
        }
        Err(config_error) => {
            decode_kv_value(&entry.value).map_err(|source| LanguageModelError::ConfigStore {
                path: entry.key.clone(),
                detail: format!("not a sinomo-config document ({config_error}); {source}"),
            })
        }
    }
}

pub fn model_config_cluster_kv_txn(config: &ModelConfig) -> Result<KvTxn, LanguageModelError> {
    let value = serde_json::to_value(config).map_err(|source| {
        LanguageModelError::RequestSerialization {
            detail: source.to_string(),
        }
    })?;
    sinomo_runtime::config::cluster_config_put_txn(&model_config_definition()?, value)
        .map_err(map_lm_config_error)
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
    let definition = model_runtime_config_definition()?;
    match sinomo_runtime::config::decode_cluster_config_entry(entry, &definition) {
        Ok(value) => {
            serde_json::from_value(value).map_err(|source| LanguageModelError::ConfigStore {
                path: entry.key.clone(),
                detail: source.to_string(),
            })
        }
        Err(config_error) => {
            decode_kv_value(&entry.value).map_err(|source| LanguageModelError::ConfigStore {
                path: entry.key.clone(),
                detail: format!("not a sinomo-config document ({config_error}); {source}"),
            })
        }
    }
}

pub fn model_runtime_config_cluster_kv_txn(
    config: &ModelRuntimeConfig,
) -> Result<KvTxn, LanguageModelError> {
    let value = serde_json::to_value(config).map_err(|source| {
        LanguageModelError::RequestSerialization {
            detail: source.to_string(),
        }
    })?;
    sinomo_runtime::config::cluster_config_put_txn(&model_runtime_config_definition()?, value)
        .map_err(map_lm_config_error)
}

pub fn save_model_config(path: &Path, value: &ModelConfig) -> Result<(), LanguageModelError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| LanguageModelError::ConfigStore {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let document = sinomo_runtime::config::ConfigDocument::new(
        model_config_definition()?.key,
        serde_json::to_value(value).map_err(|source| LanguageModelError::RequestSerialization {
            detail: source.to_string(),
        })?,
    );
    let content = serde_json::to_string_pretty(&document).map_err(|source| {
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
    let model_config = model_config_store()
        .and_then(|store| {
            store
                .get_typed(data_dir, None, LM_PROVIDER_CONFIG_KEY)
                .map_err(map_lm_config_error)
        })
        .ok();
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
    model_config_store()?
        .set_typed(
            data_dir,
            None,
            LM_PROVIDER_CONFIG_KEY,
            &config,
            sinomo_runtime::config::ConfigWriteTarget::Local,
        )
        .map_err(map_lm_config_error)?;
    Ok(DefaultModelConfigUpdate {
        config,
        config_path: model_paths.config,
    })
}

pub use sinomo_runtime::api::lm::upsert_provider_account_config;

/// Store an API-key credential for `provider` in the node-local credentials file. This is a
/// daemonless migration/helper path; cluster-first provider accounts store API keys in
/// [`LM_PROVIDER_CONFIG_KEY`].
pub fn set_provider_api_key(
    data_dir: &SinomoDataDir,
    provider: &str,
    api_key: &str,
) -> Result<(), LanguageModelError> {
    let provider = normalize_model_provider(provider).unwrap_or_else(|| provider.to_string());
    let credentials_path = Some(data_dir.provider_credentials_file(&provider));
    api_key::save_api_key_credential(&provider, &credentials_path, api_key, "manual")
}

/// Whether a non-empty API-key credential is stored locally for `provider`.
pub fn provider_has_stored_api_key(data_dir: &SinomoDataDir, provider: &str) -> bool {
    let provider = normalize_model_provider(provider).unwrap_or_else(|| provider.to_string());
    let credentials_path = Some(data_dir.provider_credentials_file(&provider));
    api_key::stored_api_key_is_present(&provider, &credentials_path)
}

/// Return the node-local stored API key for an API-key provider without consulting env vars.
pub fn stored_provider_api_key_in_data_dir(
    data_dir: &SinomoDataDir,
    provider: &str,
) -> Result<Option<String>, LanguageModelError> {
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
    let credentials_path = Some(data_dir.provider_credentials_file(&provider));
    match api_key::resolve_api_key(
        &provider,
        &None,
        &credentials_path,
        &[],
        "no stored API key found",
    ) {
        Ok(api_key) => Ok(Some(api_key)),
        Err(LanguageModelError::AuthenticationRequired { .. }) => Ok(None),
        Err(error) => Err(error),
    }
}

/// Return the stored API key from this process' Sinomo data dir, if one exists.
pub fn stored_provider_api_key(provider: &str) -> Result<Option<String>, LanguageModelError> {
    let data_dir = provider_data_dir(provider)?;
    stored_provider_api_key_in_data_dir(&data_dir, provider)
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

pub fn provider_options_for_model(
    account: Option<&ProviderAccountConfig>,
    model: &str,
) -> LanguageModelProviderOptions {
    let Some(account) = account else {
        return LanguageModelProviderOptions::default();
    };
    let mut options = account.options.clone();
    let normalized_model = normalize_model_id_for_provider(&account.provider, model);
    let effective_model = effective_model_name(&account.provider, model);
    for model_options in &account.model_options {
        let option_model = normalize_model_id_for_provider(&account.provider, &model_options.model);
        let option_effective_model = effective_model_name(&account.provider, &model_options.model);
        if option_model == normalized_model
            || option_model == effective_model
            || option_effective_model == normalized_model
            || option_effective_model == effective_model
        {
            options = options.merge(&model_options.options);
        }
    }
    options
}

pub use sinomo_runtime::api::lm::effective_model_name;

pub use sinomo_runtime::api::lm::normalize_model_id_for_provider;

pub fn default_model_provider_ids() -> &'static [&'static str] {
    &[
        GitHubCopilotProvider::PROVIDER_ID,
        GitHubCopilotCliProvider::PROVIDER_ID,
        CodexCliProvider::PROVIDER_ID,
        CODEX_PROVIDER_ID,
        ClaudeProvider::PROVIDER_ID,
        ClaudeCodeProvider::PROVIDER_ID,
        AnthropicProvider::PROVIDER_ID,
        OpenAiProvider::PROVIDER_ID,
        OLLAMA_PROVIDER_ID,
        LM_STUDIO_PROVIDER_ID,
        DeepSeekProvider::PROVIDER_ID,
        GeminiProvider::PROVIDER_ID,
        NvidiaProvider::PROVIDER_ID,
        GROK_PROVIDER_ID,
        KIMI_PROVIDER_ID,
    ]
}

pub use sinomo_runtime::api::lm::normalize_model_provider;

pub fn provider_capabilities(provider: &str) -> LanguageModelProviderCapabilities {
    if let Some(provider) = RealtimeAudioProvider::from_id(provider) {
        return provider.capabilities();
    }
    match normalize_model_provider(provider).as_deref() {
        Some(CODEX_PROVIDER_ID) => LanguageModelProviderCapabilities::codex(),
        Some(OpenAiProvider::PROVIDER_ID) => LanguageModelProviderCapabilities::openai(),
        Some(OLLAMA_PROVIDER_ID | LM_STUDIO_PROVIDER_ID) => {
            LanguageModelProviderCapabilities::local_responses()
        }
        _ => LanguageModelProviderCapabilities::default(),
    }
}

/// Return the provider-owned catalog for realtime audio/video sessions. Unlike ordinary model
/// discovery this is protocol-specific and does not mix response-only models into live-call UI.
pub fn provider_realtime_model_ids(provider: &str) -> Vec<String> {
    RealtimeAudioProvider::from_id(provider)
        .map(|provider| {
            provider
                .model_ids()
                .iter()
                .map(|model| (*model).to_string())
                .collect()
        })
        .unwrap_or_default()
}

pub fn subscription_usage_provider_ids() -> &'static [&'static str] {
    &[
        CODEX_PROVIDER_ID,
        ClaudeProvider::PROVIDER_ID,
        GitHubCopilotProvider::PROVIDER_ID,
    ]
}

/// Return known model names without credentials or network access. These are
/// configuration suggestions; authenticated discovery determines account access.
pub fn provider_known_model_ids(provider: &str) -> Vec<String> {
    match provider {
        CODEX_PROVIDER_ID | CodexCliProvider::PROVIDER_ID => codex_builtin_model_catalog()
            .iter()
            .filter(|info| info.visibility == "list")
            .map(|info| info.slug.clone())
            .collect(),
        OpenAiProvider::PROVIDER_ID => [
            "gpt-6-astra",
            "gpt-5.6-sol",
            "gpt-5.6-terra",
            "gpt-5.6-luna",
            "gpt-5.5",
            "gpt-5.5-pro",
            "gpt-5.4",
            "gpt-5.4-mini",
            "gpt-5.4-nano",
            "gpt-4.1",
        ]
        .map(str::to_string)
        .to_vec(),
        AnthropicProvider::PROVIDER_ID
        | ClaudeProvider::PROVIDER_ID
        | ClaudeCodeProvider::PROVIDER_ID => [
            "claude-fable-5-1",
            "claude-opus-5",
            "claude-sonnet-5",
            "claude-haiku-4-5-20251001",
        ]
        .map(str::to_string)
        .to_vec(),
        GeminiProvider::PROVIDER_ID => [
            "gemini-3.8-flash",
            "gemini-3.7-flash",
            "gemini-3.6-flash",
            "gemini-3.5-flash",
            "gemini-3.1-flash-lite",
            "gemini-3.5-flash-lite",
            "gemini-3.1-pro-preview",
            "gemini-2.5-flash",
            "gemini-2.5-pro",
        ]
        .map(str::to_string)
        .to_vec(),
        DeepSeekProvider::PROVIDER_ID => DeepSeekProvider::known_model_ids(),
        NvidiaProvider::PROVIDER_ID => NvidiaProvider::known_model_ids(),
        GROK_PROVIDER_ID => ["grok-4.7", "grok-4.6", "grok-4.5"]
            .map(str::to_string)
            .to_vec(),
        KIMI_PROVIDER_ID => [
            "kimi-k3",
            "kimi-k2.7-code",
            "kimi-k2.7-code-highspeed",
            "kimi-k2.6",
            "kimi-k2.5",
        ]
        .map(str::to_string)
        .to_vec(),
        _ => Vec::new(),
    }
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
        ClaudeProvider::PROVIDER_ID => {
            let (provider, _) = claude_provider()?;
            provider.list_models()
        }
        AnthropicProvider::PROVIDER_ID => {
            let (provider, _) = anthropic_provider()?;
            provider.list_models()
        }
        OpenAiProvider::PROVIDER_ID => {
            let (provider, _) = openai_provider()?;
            provider.list_models()
        }
        OLLAMA_PROVIDER_ID => {
            LocalResponsesProvider::new(LocalResponsesConfig::new(LocalResponsesRuntime::Ollama))
                .list_models()
        }
        LM_STUDIO_PROVIDER_ID => {
            LocalResponsesProvider::new(LocalResponsesConfig::new(LocalResponsesRuntime::LmStudio))
                .list_models()
        }
        DeepSeekProvider::PROVIDER_ID => {
            let (provider, _) = deepseek_provider()?;
            provider.list_models()
        }
        GeminiProvider::PROVIDER_ID => {
            let (provider, _) = gemini_provider()?;
            provider.list_models()
        }
        GROK_PROVIDER_ID | KIMI_PROVIDER_ID => vendor_api_provider(provider)?.0.list_models(),
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

/// Resolve picker-visible models using a configured provider account.
///
/// Subscription providers use their authenticated remote catalogs. API-key
/// providers use the account's credential/base URL and expose the provider's
/// known catalog, including a configured custom default model.
pub fn provider_available_model_ids_for_account(
    provider: &str,
    account: Option<&ProviderAccountConfig>,
) -> Result<Vec<String>, LanguageModelError> {
    let configured_model = account
        .and_then(|account| account.default_model.as_deref())
        .filter(|model| !model.trim().is_empty())
        .map(str::to_string);
    let mut models = match provider {
        AnthropicProvider::PROVIDER_ID => {
            let mut config = AnthropicConfig::default();
            apply_account_to_api_config(
                account,
                &mut config.api_base_url,
                &mut config.default_model,
                &mut config.api_key,
            );
            let provider = AnthropicProvider::new(config.clone());
            provider.list_models()?
        }
        OpenAiProvider::PROVIDER_ID => {
            let mut config = OpenAiConfig::default();
            apply_account_to_api_config(
                account,
                &mut config.api_base_url,
                &mut config.default_model,
                &mut config.api_key,
            );
            let provider = OpenAiProvider::new(config.clone());
            provider.list_models()?
        }
        OLLAMA_PROVIDER_ID | LM_STUDIO_PROVIDER_ID => {
            let runtime = if provider == OLLAMA_PROVIDER_ID {
                LocalResponsesRuntime::Ollama
            } else {
                LocalResponsesRuntime::LmStudio
            };
            let mut config = LocalResponsesConfig::new(runtime);
            if let Some(base_url) = account
                .and_then(|account| account.api_base_url.as_deref())
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                config.api_base_url = base_url.to_string();
            }
            if let Some(model) = configured_model.as_deref() {
                config.default_model = model.to_string();
            }
            LocalResponsesProvider::new(config).list_models()?
        }
        DeepSeekProvider::PROVIDER_ID => {
            let mut config = DeepSeekConfig::default();
            apply_account_to_api_config(
                account,
                &mut config.api_base_url,
                &mut config.default_model,
                &mut config.api_key,
            );
            DeepSeekProvider::new(config).list_models()?
        }
        GeminiProvider::PROVIDER_ID => {
            let mut config = GeminiConfig::default();
            apply_account_to_api_config(
                account,
                &mut config.api_base_url,
                &mut config.default_model,
                &mut config.api_key,
            );
            let provider = GeminiProvider::new(config.clone());
            provider.list_models()?
        }
        GROK_PROVIDER_ID | KIMI_PROVIDER_ID => {
            let mut config = VendorApiConfig::new(VendorApi::from_provider(provider).unwrap());
            apply_account_to_api_config(
                account,
                &mut config.api_base_url,
                &mut config.default_model,
                &mut config.api_key,
            );
            VendorApiProvider::new(config).list_models()?
        }
        NvidiaProvider::PROVIDER_ID => {
            let mut config = NvidiaConfig::default();
            apply_account_to_api_config(
                account,
                &mut config.api_base_url,
                &mut config.default_model,
                &mut config.api_key,
            );
            NvidiaProvider::new(config).list_models()?
        }
        _ => provider_available_model_ids(provider)?,
    };
    if let Some(model) = configured_model
        && !models.contains(&model)
    {
        models.push(model);
    }
    models.sort();
    models.dedup();
    Ok(models)
}

pub fn provider_available_model_ids_for_account_in_data_dir(
    data_dir: &SinomoDataDir,
    provider: &str,
    account: Option<&ProviderAccountConfig>,
) -> Result<Vec<String>, LanguageModelError> {
    let mut models = match provider {
        CODEX_PROVIDER_ID => {
            let mut config = OpenAiResponsesCodexConfig {
                credentials_path: Some(data_dir.provider_credentials_file(CODEX_PROVIDER_ID)),
                ..OpenAiResponsesCodexConfig::default()
            };
            if let Some(account) = account {
                if let Some(base_url) = account.api_base_url.as_deref() {
                    config.api_base_url = base_url.to_string();
                }
                if let Some(model) = account.default_model.as_deref() {
                    config.default_model = model.to_string();
                }
            }
            OpenAiResponsesCodexClient::new(config).list_models()?
        }
        ClaudeProvider::PROVIDER_ID => {
            let config = ClaudeConfig {
                api_base_url: account
                    .and_then(|account| account.api_base_url.clone())
                    .unwrap_or_else(|| ClaudeConfig::default().api_base_url),
                default_model: account
                    .and_then(|account| account.default_model.clone())
                    .unwrap_or_else(|| CLAUDE_DEFAULT_MODEL.into()),
                credentials_path: Some(
                    data_dir.provider_credentials_file(ClaudeProvider::PROVIDER_ID),
                ),
                ..ClaudeConfig::default()
            };
            ClaudeProvider::new(config).list_models()?
        }
        GitHubCopilotProvider::PROVIDER_ID => {
            let mut config = GitHubCopilotConfig {
                token_store_path: Some(
                    data_dir.provider_credentials_file(GitHubCopilotProvider::PROVIDER_ID),
                ),
                ..GitHubCopilotConfig::default()
            };
            if let Some(account) = account {
                if let Some(base_url) = account.api_base_url.as_deref() {
                    config.api_base_url = base_url.to_string();
                }
                if let Some(model) = account.default_model.as_deref() {
                    config.default_model = model.to_string();
                }
            }
            GitHubCopilotProvider::new(config).list_models()?
        }
        _ => configured_model_provider(data_dir, provider, PROVIDER_DEFAULT_MODEL, account, None)?
            .available_models()?,
    };
    if let Some(model) = account
        .and_then(|account| account.default_model.as_deref())
        .filter(|model| !model.trim().is_empty())
        .map(str::to_string)
        && !models.contains(&model)
    {
        models.push(model);
    }
    models.sort();
    models.dedup();
    Ok(models)
}

fn apply_account_to_api_config(
    account: Option<&ProviderAccountConfig>,
    api_base_url: &mut String,
    default_model: &mut String,
    api_key: &mut Option<String>,
) {
    let Some(account) = account else {
        return;
    };
    if let Some(value) = account
        .api_base_url
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        *api_base_url = value.trim().to_string();
    }
    if let Some(value) = account
        .default_model
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        *default_model = value.trim().to_string();
    }
    if let Some(value) = account
        .api_key
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        *api_key = Some(value.trim().to_string());
    }
}

pub use sinomo_runtime::api::lm::model_supports_reasoning_effort;

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
        ClaudeProvider::PROVIDER_ID => {
            let (provider, _) = claude_provider()?;
            provider.usage()
        }
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

pub fn provider_usage_in_data_dir(
    data_dir: &SinomoDataDir,
    provider: &str,
) -> Result<LanguageModelProviderUsage, LanguageModelError> {
    match provider {
        CODEX_PROVIDER_ID => {
            let config = OpenAiResponsesCodexConfig {
                credentials_path: Some(data_dir.provider_credentials_file(CODEX_PROVIDER_ID)),
                ..OpenAiResponsesCodexConfig::default()
            };
            OpenAiResponsesCodexClient::new(config).usage()
        }
        ClaudeProvider::PROVIDER_ID => {
            let config = ClaudeConfig {
                credentials_path: Some(
                    data_dir.provider_credentials_file(ClaudeProvider::PROVIDER_ID),
                ),
                ..ClaudeConfig::default()
            };
            ClaudeProvider::new(config).usage()
        }
        GitHubCopilotProvider::PROVIDER_ID => {
            let config = GitHubCopilotConfig {
                token_store_path: Some(
                    data_dir.provider_credentials_file(GitHubCopilotProvider::PROVIDER_ID),
                ),
                ..GitHubCopilotConfig::default()
            };
            GitHubCopilotProvider::new(config).usage()
        }
        _ => provider_usage(provider),
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
    let provider = configured_model_provider(data_dir, provider, model, account, working_dir)?;
    authorize_configured_provider_host_access(data_dir, provider.as_ref(), model)?;
    provider.complete(&request)
}

pub fn complete_configured_model_request_streaming_events_with_account(
    data_dir: &SinomoDataDir,
    provider: &str,
    model: &str,
    account: Option<&ProviderAccountConfig>,
    working_dir: Option<PathBuf>,
    request: &LanguageModelRequest,
    on_event: &mut dyn FnMut(LanguageModelStreamEvent),
) -> Result<LanguageModelResponse, LanguageModelError> {
    let cancellation = LanguageModelCancellation::default();
    complete_configured_model_request_streaming_events_with_account_and_cancellation(
        data_dir,
        provider,
        model,
        account,
        working_dir,
        request,
        &cancellation,
        on_event,
    )
}

pub fn complete_configured_model_request_streaming_events_with_account_and_cancellation(
    data_dir: &SinomoDataDir,
    provider: &str,
    model: &str,
    account: Option<&ProviderAccountConfig>,
    working_dir: Option<PathBuf>,
    request: &LanguageModelRequest,
    cancellation: &LanguageModelCancellation,
    on_event: &mut dyn FnMut(LanguageModelStreamEvent),
) -> Result<LanguageModelResponse, LanguageModelError> {
    let request = LanguageModelRequest {
        provider: provider.to_string(),
        model: model.to_string(),
        ..request.clone()
    };
    let provider = configured_model_provider(data_dir, provider, model, account, working_dir)?;
    authorize_configured_provider_host_access(data_dir, provider.as_ref(), model)?;
    provider.complete_streaming_events_with_cancellation(&request, cancellation, on_event)
}

fn authorize_configured_provider_host_access(
    data_dir: &SinomoDataDir,
    provider: &dyn LanguageModelProvider,
    requested_model: &str,
) -> Result<(), LanguageModelError> {
    let info = provider.info();
    if !info.host_access.requires_remote_management_permission() {
        return Ok(());
    }

    let permission = LocalNodePermission::RemoteManagement;
    let store = LocalNodePermissionStore::new(data_dir.root());
    let granted = store.try_permission_granted(permission).map_err(|error| {
        configured_provider_permission_error(
            &info,
            requested_model,
            format!("could not verify node-local permission: {error}"),
        )
    })?;
    if !granted {
        return Err(configured_provider_permission_error(
            &info,
            requested_model,
            "grant `remote-management` locally on this node",
        ));
    }
    Ok(())
}

fn configured_provider_permission_error(
    info: &LanguageModelProviderInfo,
    requested_model: &str,
    detail: impl std::fmt::Display,
) -> LanguageModelError {
    LanguageModelError::ProviderInvocationFailed {
        provider: info.provider.clone(),
        model: requested_model.to_string(),
        detail: format!(
            "provider requires node-local permission `remote-management` for unrestricted host access; {detail}"
        ),
    }
}

fn configured_model_provider(
    data_dir: &SinomoDataDir,
    provider: &str,
    model: &str,
    account: Option<&ProviderAccountConfig>,
    working_dir: Option<PathBuf>,
) -> Result<Box<dyn LanguageModelProvider>, LanguageModelError> {
    let provider_options = provider_options_for_model(account, model);
    match provider {
        GitHubCopilotProvider::PROVIDER_ID => {
            let credentials_path =
                data_dir.provider_credentials_file(GitHubCopilotProvider::PROVIDER_ID);
            let default_config = GitHubCopilotConfig::default();
            let provider = GitHubCopilotProvider::new(GitHubCopilotConfig {
                options: provider_options.clone(),
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
            Ok(Box::new(provider))
        }
        GitHubCopilotCliProvider::PROVIDER_ID => {
            let provider = GitHubCopilotCliProvider::new(GitHubCopilotCliConfig {
                model: model_or_default(model, GitHubCopilotCliConfig::default().model),
                reasoning_effort: provider_options.reasoning_effort,
                ..GitHubCopilotCliConfig::default()
            });
            Ok(Box::new(provider))
        }
        CodexCliProvider::PROVIDER_ID => {
            let provider = CodexCliProvider::new(CodexCliConfig {
                model: model_or_default(model, CODEX_CLI_DEFAULT_MODEL.to_string()),
                working_dir: working_dir.clone(),
                reasoning_effort: provider_options.reasoning_effort,
                ..CodexCliConfig::default()
            });
            Ok(Box::new(provider))
        }
        CODEX_PROVIDER_ID => {
            let credentials_path = data_dir.provider_credentials_file(CODEX_PROVIDER_ID);
            Ok(Box::new(
                codex_provider_for_model_with_options_and_credentials(
                    model,
                    provider_options,
                    Some(credentials_path),
                ),
            ))
        }
        ClaudeProvider::PROVIDER_ID => {
            let credentials_path = data_dir.provider_credentials_file(ClaudeProvider::PROVIDER_ID);
            let default_config = ClaudeConfig::default();
            let provider = ClaudeProvider::new(ClaudeConfig {
                api_base_url: account
                    .and_then(|account| account.api_base_url.clone())
                    .unwrap_or(default_config.api_base_url),
                default_model: model_or_default(model, default_config.default_model),
                credentials_path: Some(credentials_path),
                options: provider_options.clone(),
                claude_code_version: default_config.claude_code_version,
            });
            Ok(Box::new(provider))
        }
        ClaudeCodeProvider::PROVIDER_ID => {
            let provider = ClaudeCodeProvider::new(ClaudeCodeConfig {
                model: model_or_default(model, CLAUDE_CODE_DEFAULT_MODEL.to_string()),
                working_dir,
                reasoning_effort: provider_options.reasoning_effort,
                ..ClaudeCodeConfig::default()
            });
            Ok(Box::new(provider))
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
                options: provider_options.clone(),
            });
            Ok(Box::new(provider))
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
                options: provider_options.clone(),
            });
            Ok(Box::new(provider))
        }
        OLLAMA_PROVIDER_ID | LM_STUDIO_PROVIDER_ID => {
            let runtime = if provider == OLLAMA_PROVIDER_ID {
                LocalResponsesRuntime::Ollama
            } else {
                LocalResponsesRuntime::LmStudio
            };
            let mut config = LocalResponsesConfig::new(runtime);
            if let Some(base_url) = account
                .and_then(|account| account.api_base_url.as_deref())
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                config.api_base_url = base_url.to_string();
            }
            config.default_model = model_or_default(model, config.default_model);
            config.options = provider_options.clone();
            Ok(Box::new(LocalResponsesProvider::new(config)))
        }
        DeepSeekProvider::PROVIDER_ID => {
            let credentials_path =
                data_dir.provider_credentials_file(DeepSeekProvider::PROVIDER_ID);
            let default_config = DeepSeekConfig::default();
            let provider = DeepSeekProvider::new(DeepSeekConfig {
                options: provider_options.clone(),
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
            Ok(Box::new(provider))
        }
        GeminiProvider::PROVIDER_ID => {
            let credentials_path = data_dir.provider_credentials_file(GeminiProvider::PROVIDER_ID);
            let default_config = GeminiConfig::default();
            let provider = GeminiProvider::new(GeminiConfig {
                options: provider_options.clone(),
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
            Ok(Box::new(provider))
        }
        GROK_PROVIDER_ID | KIMI_PROVIDER_ID => {
            let mut config = VendorApiConfig::new(VendorApi::from_provider(provider).unwrap());
            config.options = provider_options.clone();
            config.default_model = model_or_default(model, config.default_model);
            config.credentials_path = Some(data_dir.provider_credentials_file(provider));
            if let Some(account) = account {
                if let Some(base) = &account.api_base_url {
                    config.api_base_url = base.clone();
                }
                config.api_key = account.api_key.clone();
                if config.api_key.is_some() {
                    config.credentials_path = None;
                }
            }
            Ok(Box::new(VendorApiProvider::new(config)))
        }
        NvidiaProvider::PROVIDER_ID => {
            let credentials_path = data_dir.provider_credentials_file(NvidiaProvider::PROVIDER_ID);
            let default_config = NvidiaConfig::default();
            let provider = NvidiaProvider::new(NvidiaConfig {
                options: provider_options.clone(),
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
            Ok(Box::new(provider))
        }
        _ => Err(LanguageModelError::ProviderInvocationFailed {
            provider: provider.to_string(),
            model: model.to_string(),
            detail: "unknown language model provider".to_string(),
        }),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ServerSentEvent {
    pub event: Option<String>,
    pub data: String,
}

pub(crate) fn read_server_sent_events<R>(
    reader: R,
    provider: &str,
    model: &str,
    mut on_event: impl FnMut(ServerSentEvent) -> Result<(), LanguageModelError>,
) -> Result<(), LanguageModelError>
where
    R: Read,
{
    let mut reader = BufReader::new(reader);
    let mut line = String::new();
    let mut event_name = None;
    let mut data_lines = Vec::new();

    loop {
        line.clear();
        let read = reader.read_line(&mut line).map_err(|source| {
            LanguageModelError::ProviderInvocationFailed {
                provider: provider.to_string(),
                model: model.to_string(),
                detail: format!("failed to read streaming response: {source}"),
            }
        })?;
        if read == 0 {
            flush_server_sent_event(&mut event_name, &mut data_lines, &mut on_event)?;
            break;
        }

        let line = line.trim_end_matches(['\r', '\n']);
        if line.is_empty() {
            flush_server_sent_event(&mut event_name, &mut data_lines, &mut on_event)?;
        } else if line.starts_with(':') {
            continue;
        } else if let Some(value) = line.strip_prefix("event:") {
            event_name = Some(value.trim().to_string());
        } else if let Some(value) = line.strip_prefix("data:") {
            data_lines.push(value.trim_start().to_string());
        }
    }

    Ok(())
}

fn flush_server_sent_event(
    event_name: &mut Option<String>,
    data_lines: &mut Vec<String>,
    on_event: &mut impl FnMut(ServerSentEvent) -> Result<(), LanguageModelError>,
) -> Result<(), LanguageModelError> {
    if data_lines.is_empty() {
        event_name.take();
        return Ok(());
    }
    let data = std::mem::take(data_lines).join("\n");
    let event = event_name.take();
    let trimmed = data.trim();
    if trimmed.is_empty() || trimmed == "[DONE]" {
        return Ok(());
    }
    on_event(ServerSentEvent { event, data })
}

pub(crate) fn incomplete_provider_stream(
    provider: &str,
    model: &str,
    on_event: &mut dyn FnMut(LanguageModelStreamEvent),
) -> LanguageModelError {
    let detail = "provider stream closed before its terminal event".to_string();
    on_event(LanguageModelStreamEvent::Error {
        detail: detail.clone(),
    });
    LanguageModelError::ProviderInvocationFailed {
        provider: provider.into(),
        model: model.into(),
        detail,
    }
}

pub(crate) fn stream_openai_chat_completion_response<R>(
    reader: R,
    provider: &str,
    model: &str,
    on_event: &mut dyn FnMut(LanguageModelStreamEvent),
) -> Result<LanguageModelResponse, LanguageModelError>
where
    R: Read,
{
    let mut content = String::new();
    let mut reasoning = String::new();
    let mut tool_calls = Vec::<ChatCompletionToolCallAccumulator>::new();
    let mut usage = None;
    let mut finish_reason = None;

    read_server_sent_events(reader, provider, model, |event| {
        let value = serde_json::from_str::<serde_json::Value>(&event.data).map_err(|source| {
            LanguageModelError::ProviderInvocationFailed {
                provider: provider.to_string(),
                model: model.to_string(),
                detail: format!(
                    "failed to parse chat completion stream event: {source}: {}",
                    event.data
                ),
            }
        })?;
        if let Some(detail) = value
            .pointer("/error/message")
            .and_then(serde_json::Value::as_str)
        {
            on_event(LanguageModelStreamEvent::Error {
                detail: detail.to_string(),
            });
            return Err(LanguageModelError::ProviderInvocationFailed {
                provider: provider.to_string(),
                model: model.to_string(),
                detail: detail.to_string(),
            });
        }
        if let Some(delta) = value
            .pointer("/choices/0/delta/content")
            .and_then(serde_json::Value::as_str)
            .or_else(|| {
                value
                    .pointer("/choices/0/text")
                    .and_then(serde_json::Value::as_str)
            })
            && !delta.is_empty()
        {
            content.push_str(delta);
            on_event(LanguageModelStreamEvent::TextDelta {
                delta: delta.to_string(),
            });
        }
        if let Some(delta) = value
            .pointer("/choices/0/delta/reasoning_content")
            .or_else(|| value.pointer("/choices/0/delta/reasoning"))
            .or_else(|| value.pointer("/choices/0/delta/thinking"))
            .and_then(serde_json::Value::as_str)
            && !delta.is_empty()
        {
            reasoning.push_str(delta);
            on_event(LanguageModelStreamEvent::ReasoningDelta {
                delta: delta.to_string(),
            });
        }
        if let Some(delta_tool_calls) = value
            .pointer("/choices/0/delta/tool_calls")
            .and_then(serde_json::Value::as_array)
        {
            for call_delta in delta_tool_calls {
                apply_chat_completion_tool_call_delta(call_delta, &mut tool_calls, on_event);
            }
        }
        if let Some(event_usage) = openai_chat_completion_usage(&value) {
            usage = Some(event_usage);
        }
        if let Some(reason) = value
            .pointer("/choices/0/finish_reason")
            .and_then(serde_json::Value::as_str)
        {
            finish_reason = Some(reason.to_string());
        }
        Ok(())
    })?;

    if finish_reason.is_none() {
        return Err(incomplete_provider_stream(provider, model, on_event));
    }
    let completed_tool_calls = tool_calls
        .into_iter()
        .filter_map(|call| call.complete(on_event))
        .collect::<Vec<_>>();
    let mut items = Vec::new();
    if !reasoning.trim().is_empty() {
        items.push(LanguageModelResponseItem::ReasoningSummary {
            summary: reasoning.clone(),
        });
    }
    let mut replay = serde_json::json!({"role":"assistant", "content":content});
    if !reasoning.is_empty() || provider == DeepSeekProvider::PROVIDER_ID {
        replay["reasoning_content"] = serde_json::json!(reasoning);
    }
    if !completed_tool_calls.is_empty() {
        replay["tool_calls"] = serde_json::json!(completed_tool_calls.iter().map(|call| serde_json::json!({
            "id":call.call_id, "type":"function", "function":{"name":call.name, "arguments":call.input.as_str().map(str::to_string).unwrap_or_else(|| call.input.to_string())}
        })).collect::<Vec<_>>());
    }
    items.push(response_continuation(
        provider,
        model,
        vec![replay],
        &completed_tool_calls,
    ));
    items.extend(
        completed_tool_calls
            .iter()
            .cloned()
            .map(|call| LanguageModelResponseItem::ToolCall { call }),
    );
    if let Some(usage) = &usage {
        on_event(LanguageModelStreamEvent::Usage {
            usage: usage.clone(),
        });
    }
    on_event(LanguageModelStreamEvent::Finished { finish_reason });

    Ok(LanguageModelResponse {
        provider: provider.to_string(),
        model: model.to_string(),
        content,
        items,
        tool_calls: completed_tool_calls,
        usage,
    })
}

#[derive(Clone, Debug, Default)]
struct ChatCompletionToolCallAccumulator {
    call_id: Option<String>,
    name: Option<String>,
    arguments: String,
    started: bool,
}

impl ChatCompletionToolCallAccumulator {
    fn complete(
        mut self,
        on_event: &mut dyn FnMut(LanguageModelStreamEvent),
    ) -> Option<LanguageModelToolCall> {
        let name = self.name.take()?;
        if !self.started {
            on_event(LanguageModelStreamEvent::ToolCallStarted {
                call_id: self.call_id.clone(),
                name: name.clone(),
            });
        }
        let input = parse_streamed_tool_arguments(&self.arguments);
        on_event(LanguageModelStreamEvent::ToolCallCompleted {
            call_id: self.call_id.clone(),
            name: name.clone(),
            input: input.clone(),
        });
        Some(LanguageModelToolCall {
            call_id: self.call_id,
            name,
            input,
            summary: None,
        })
    }
}

fn apply_chat_completion_tool_call_delta(
    value: &serde_json::Value,
    tool_calls: &mut Vec<ChatCompletionToolCallAccumulator>,
    on_event: &mut dyn FnMut(LanguageModelStreamEvent),
) {
    let index = value
        .get("index")
        .and_then(serde_json::Value::as_u64)
        .map(|index| index as usize)
        .unwrap_or(tool_calls.len());
    while tool_calls.len() <= index {
        tool_calls.push(ChatCompletionToolCallAccumulator::default());
    }
    let call = &mut tool_calls[index];
    if let Some(id) = value.get("id").and_then(serde_json::Value::as_str)
        && call.call_id.is_none()
    {
        call.call_id = Some(id.to_string());
    }
    if let Some(name) = value
        .pointer("/function/name")
        .and_then(serde_json::Value::as_str)
        && call.name.is_none()
    {
        call.name = Some(name.to_string());
    }
    if !call.started
        && let Some(name) = &call.name
    {
        call.started = true;
        on_event(LanguageModelStreamEvent::ToolCallStarted {
            call_id: call.call_id.clone(),
            name: name.clone(),
        });
    }
    if let Some(arguments_delta) = value
        .pointer("/function/arguments")
        .and_then(serde_json::Value::as_str)
        && !arguments_delta.is_empty()
    {
        call.arguments.push_str(arguments_delta);
        on_event(LanguageModelStreamEvent::ToolCallDelta {
            call_id: call.call_id.clone(),
            name: call.name.clone(),
            arguments_delta: arguments_delta.to_string(),
        });
    }
}

fn parse_streamed_tool_arguments(arguments: &str) -> serde_json::Value {
    if arguments.trim().is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_str(arguments).unwrap_or_else(|_| serde_json::json!(arguments))
    }
}

fn openai_chat_completion_usage(value: &serde_json::Value) -> Option<LanguageModelUsage> {
    value.get("usage").map(|usage| LanguageModelUsage {
        input_tokens: usage
            .get("prompt_tokens")
            .or_else(|| usage.get("input_tokens"))
            .and_then(serde_json::Value::as_u64),
        output_tokens: usage
            .get("completion_tokens")
            .or_else(|| usage.get("output_tokens"))
            .and_then(serde_json::Value::as_u64),
    })
}

pub fn model_or_default(model: &str, default_model: String) -> String {
    if model == PROVIDER_DEFAULT_MODEL {
        default_model
    } else {
        model.to_string()
    }
}

pub(crate) fn apply_provider_vendor_options_to_payload(
    payload: &mut serde_json::Value,
    options: &LanguageModelProviderOptions,
) {
    for (key, value) in &options.vendor_options {
        apply_provider_vendor_option_to_payload(payload, key, value);
    }
}

fn apply_provider_vendor_option_to_payload(
    payload: &mut serde_json::Value,
    key: &str,
    value: &str,
) {
    let path = key
        .split('.')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    let Some((last, parents)) = path.split_last() else {
        return;
    };
    let value = provider_vendor_option_json_value(value);
    let mut target = payload;
    for parent in parents {
        if !target.is_object() {
            *target = serde_json::json!({});
        }
        let object = target.as_object_mut().expect("target was object");
        target = object
            .entry((*parent).to_string())
            .or_insert_with(|| serde_json::json!({}));
    }
    if !target.is_object() {
        *target = serde_json::json!({});
    }
    target
        .as_object_mut()
        .expect("target was object")
        .insert((*last).to_string(), value);
}

fn provider_vendor_option_json_value(value: &str) -> serde_json::Value {
    let value = value.trim();
    if value.is_empty() {
        return serde_json::Value::String(String::new());
    }
    serde_json::from_str(value).unwrap_or_else(|_| serde_json::Value::String(value.to_string()))
}

pub fn github_copilot_provider() -> Result<(GitHubCopilotProvider, PathBuf), LanguageModelError> {
    let data_dir =
        SinomoDataDir::from_env_or_home().map_err(|source| LanguageModelError::ConfigStore {
            path: "<data-dir>".to_string(),
            detail: source.to_string(),
        })?;
    data_dir
        .ensure_root_layout()
        .map_err(|source| LanguageModelError::ConfigStore {
            path: data_dir.root().display().to_string(),
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
        options: LanguageModelProviderOptions::default(),
    });
    Ok((provider, credentials_path))
}

pub fn claude_provider() -> Result<(ClaudeProvider, PathBuf), LanguageModelError> {
    let data_dir = provider_data_dir(ClaudeProvider::PROVIDER_ID)?;
    let credentials_path = data_dir.provider_credentials_file(ClaudeProvider::PROVIDER_ID);
    let provider = ClaudeProvider::new(ClaudeConfig {
        api_base_url: ClaudeConfig::default().api_base_url,
        default_model: ClaudeConfig::default().default_model,
        credentials_path: Some(credentials_path.clone()),
        options: LanguageModelProviderOptions::default(),
        claude_code_version: ClaudeConfig::default().claude_code_version,
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
        options: LanguageModelProviderOptions::default(),
    });
    Ok((provider, credentials_path))
}

pub fn deepseek_provider() -> Result<(DeepSeekProvider, PathBuf), LanguageModelError> {
    let data_dir = provider_data_dir(DeepSeekProvider::PROVIDER_ID)?;
    let credentials_path = data_dir.provider_credentials_file(DeepSeekProvider::PROVIDER_ID);
    let provider = DeepSeekProvider::new(DeepSeekConfig {
        options: LanguageModelProviderOptions::default(),
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
        options: LanguageModelProviderOptions::default(),
        api_base_url: GeminiConfig::default().api_base_url,
        default_model: GeminiConfig::default().default_model,
        api_key: None,
        credentials_path: Some(credentials_path.clone()),
    });
    Ok((provider, credentials_path))
}

pub fn vendor_api_provider(
    provider: &str,
) -> Result<(VendorApiProvider, PathBuf), LanguageModelError> {
    let api = VendorApi::from_provider(provider).ok_or_else(|| {
        LanguageModelError::ProviderInvocationFailed {
            provider: provider.into(),
            model: PROVIDER_DEFAULT_MODEL.into(),
            detail: "unknown vendor API provider".into(),
        }
    })?;
    let data_dir = provider_data_dir(api.provider())?;
    let credentials_path = data_dir.provider_credentials_file(api.provider());
    let mut config = VendorApiConfig::new(api);
    config.credentials_path = Some(credentials_path.clone());
    Ok((VendorApiProvider::new(config), credentials_path))
}

pub fn nvidia_provider() -> Result<(NvidiaProvider, PathBuf), LanguageModelError> {
    let data_dir = provider_data_dir(NvidiaProvider::PROVIDER_ID)?;
    let credentials_path = data_dir.provider_credentials_file(NvidiaProvider::PROVIDER_ID);
    let provider = NvidiaProvider::new(NvidiaConfig {
        options: LanguageModelProviderOptions::default(),
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
    codex_provider_for_model_with_options(model, LanguageModelProviderOptions::default())
}

pub fn codex_provider_for_model_with_options(
    model: &str,
    options: LanguageModelProviderOptions,
) -> CodexProvider {
    codex_provider_for_model_with_options_and_credentials(model, options, None)
}

fn codex_provider_for_model_with_options_and_credentials(
    model: &str,
    options: LanguageModelProviderOptions,
    credentials_path: Option<PathBuf>,
) -> CodexProvider {
    let default_model = model_or_default(model, CODEX_DEFAULT_MODEL.to_string());
    let config_default = OpenAiResponsesCodexConfig::default();
    let client_config = OpenAiResponsesCodexConfig {
        api_base_url: config_default.api_base_url.clone(),
        auth_issuer: config_default.auth_issuer.clone(),
        client_id: config_default.client_id.clone(),
        client_version: config_default.client_version,
        default_model: default_model.clone(),
        credentials_path: credentials_path.or(config_default.credentials_path),
        transport_health_timeout: config_default.transport_health_timeout,
    };
    CodexProvider::new(
        CodexConfig {
            default_model,
            options,
            ..CodexConfig::default()
        },
        OpenAiResponsesCodexClient::new(client_config),
        CodexToolRegistry::new(),
        DefaultCodexPromptBuilder::default(),
        DefaultCodexContextManager::default(),
    )
}

fn provider_data_dir(_provider: &str) -> Result<SinomoDataDir, LanguageModelError> {
    let data_dir =
        SinomoDataDir::from_env_or_home().map_err(|source| LanguageModelError::ConfigStore {
            path: "<data-dir>".to_string(),
            detail: source.to_string(),
        })?;
    data_dir
        .ensure_root_layout()
        .map_err(|source| LanguageModelError::ConfigStore {
            path: data_dir.root().display().to_string(),
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
        GROK_PROVIDER_ID | KIMI_PROVIDER_ID => {
            let (client, credentials_path) = vendor_api_provider(provider)?;
            client.save_api_key(api_key, source)?;
            Ok(ProviderCredentialWrite {
                provider: provider.into(),
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
    let existing = provider_account_config(config, &provider, Some(&account)).cloned();
    let default_model = existing
        .as_ref()
        .and_then(|existing| existing.default_model.clone())
        .unwrap_or_else(|| effective_model_name(&provider, PROVIDER_DEFAULT_MODEL));
    upsert_provider_account_config(
        config,
        ProviderAccountConfig {
            provider: provider.clone(),
            account: account.clone(),
            default_model: Some(default_model),
            api_base_url: existing
                .as_ref()
                .and_then(|existing| existing.api_base_url.clone()),
            api_key: Some(api_key.trim().to_string()),
            enabled: true,
            options: existing
                .as_ref()
                .map(|existing| existing.options.clone())
                .unwrap_or_default(),
            model_options: existing
                .as_ref()
                .map(|existing| existing.model_options.clone())
                .unwrap_or_default(),
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
        GROK_PROVIDER_ID => Some(&["SINOMO_GROK_API_KEY", "XAI_API_KEY", "GROK_API_KEY"]),
        KIMI_PROVIDER_ID => Some(&["SINOMO_KIMI_API_KEY", "MOONSHOT_API_KEY", "KIMI_API_KEY"]),
        NvidiaProvider::PROVIDER_ID => {
            Some(&["SINOMO_NVIDIA_API_KEY", "NVIDIA_API_KEY", "NIM_API_KEY"])
        }
        "minicpm-o" | "minicpm" | "modelbest" => Some(&[
            "SINOMO_MINICPM_API_KEY",
            "SINOMO_MINICPM_O_API_KEY",
            "SINOMO_MODELBEST_API_KEY",
            "MINICPM_API_KEY",
            "MINICPM_O_API_KEY",
            "MODELBEST_API_KEY",
        ]),
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
        ClaudeProvider::PROVIDER_ID => {
            let (provider, credentials_path) = claude_provider()?;
            provider.oauth_tokens()?;
            provider.test_connection()?;
            Ok(ProviderCredentialTest {
                provider: ClaudeProvider::PROVIDER_ID.to_string(),
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
        GROK_PROVIDER_ID | KIMI_PROVIDER_ID => {
            let (client, credentials_path) = vendor_api_provider(provider)?;
            client.test_connection()?;
            Ok(ProviderCredentialTest {
                provider: provider.into(),
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
    let data_dir =
        SinomoDataDir::from_env_or_home().map_err(|source| LanguageModelError::ConfigStore {
            path: "<data-dir>".to_string(),
            detail: source.to_string(),
        })?;
    test_provider_account_credentials_in_data_dir(&data_dir, provider, account)
}

pub fn test_provider_account_credentials_in_data_dir(
    data_dir: &SinomoDataDir,
    provider: &str,
    account: Option<&ProviderAccountConfig>,
) -> Result<(), LanguageModelError> {
    let model = account
        .and_then(|account| account.default_model.as_deref())
        .unwrap_or(PROVIDER_DEFAULT_MODEL);
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
        data_dir, provider, model, account, None, &request,
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
    /// Describes the provider's ability to affect this host outside Sinomo's
    /// bounded tool surface. Missing serialized values default to
    /// `unrestricted` so older or incomplete capability records fail closed.
    #[serde(default)]
    pub host_access: LanguageModelHostAccess,
    /// Provider-owned upper bound on optional model/runtime surfaces that
    /// Sinomo has an implementation for. Agent policy, tool invocation mode,
    /// and node-local authority may further reduce what is exposed.
    #[serde(default)]
    pub capabilities: LanguageModelProviderCapabilities,
    pub supports_json_object: bool,
    pub supports_tool_permissions: bool,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct LanguageModelProviderCapabilities {
    #[serde(default)]
    pub responses_http: bool,
    #[serde(default)]
    pub responses_websocket: bool,
    #[serde(default)]
    pub realtime_websocket: bool,
    #[serde(default)]
    pub realtime_webrtc: bool,
    #[serde(default)]
    pub web_search: bool,
    #[serde(default)]
    pub image_generation: bool,
    #[serde(default)]
    pub image_editing: bool,
    #[serde(default)]
    pub transcription: bool,
    #[serde(default)]
    pub local_model_management: bool,
    #[serde(default)]
    pub mcp: bool,
}

impl LanguageModelProviderCapabilities {
    pub const fn responses_http() -> Self {
        Self {
            responses_http: true,
            responses_websocket: false,
            realtime_websocket: false,
            realtime_webrtc: false,
            web_search: false,
            image_generation: false,
            image_editing: false,
            transcription: false,
            local_model_management: false,
            mcp: false,
        }
    }

    pub const fn openai() -> Self {
        Self {
            responses_http: true,
            responses_websocket: false,
            realtime_websocket: true,
            realtime_webrtc: false,
            web_search: true,
            image_generation: true,
            image_editing: true,
            transcription: true,
            local_model_management: false,
            mcp: true,
        }
    }

    /// The Codex backend reached through a ChatGPT sign-in.
    ///
    /// It serves the same image model as the OpenAI API and a standalone web search endpoint.
    /// Realtime is not served by the Codex backend at all: the sign-in token reaches the OpenAI
    /// realtime API directly, so both of its transports are available.
    pub const fn codex() -> Self {
        Self {
            responses_http: true,
            responses_websocket: false,
            realtime_websocket: true,
            realtime_webrtc: true,
            web_search: true,
            image_generation: true,
            image_editing: true,
            transcription: false,
            local_model_management: false,
            mcp: false,
        }
    }

    pub const fn local_responses() -> Self {
        Self {
            responses_http: true,
            responses_websocket: false,
            realtime_websocket: false,
            realtime_webrtc: false,
            web_search: false,
            image_generation: false,
            image_editing: false,
            transcription: false,
            local_model_management: true,
            mcp: false,
        }
    }

    pub const fn minicpm_realtime() -> Self {
        Self {
            responses_http: false,
            responses_websocket: false,
            realtime_websocket: true,
            realtime_webrtc: false,
            web_search: false,
            image_generation: false,
            image_editing: false,
            transcription: false,
            local_model_management: false,
            mcp: false,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum LanguageModelHostAccess {
    /// The provider communicates with a remote API and has no direct host
    /// process or filesystem access through the provider transport.
    NetworkOnly,
    /// The local provider is constrained by an enforced sandbox or exposes a
    /// fixed-function inference interface without general host tools.
    Bounded,
    /// The provider can run unsandboxed commands or otherwise exercise
    /// unbounded control over the host.
    #[default]
    Unrestricted,
}

impl LanguageModelHostAccess {
    pub const fn requires_remote_management_permission(self) -> bool {
        matches!(self, Self::Unrestricted)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum LanguageModelInvocation {
    Process,
    Http,
    InProcess,
}

/// A cloneable cancellation check that provider transports can poll without
/// depending on a higher-level runtime's cancellation-token type.
#[derive(Clone)]
pub struct LanguageModelCancellation {
    check: Arc<dyn Fn() -> bool + Send + Sync + 'static>,
}

impl LanguageModelCancellation {
    pub fn new(check: impl Fn() -> bool + Send + Sync + 'static) -> Self {
        Self {
            check: Arc::new(check),
        }
    }

    pub fn is_cancelled(&self) -> bool {
        (self.check)()
    }
}

impl Default for LanguageModelCancellation {
    fn default() -> Self {
        Self::new(|| false)
    }
}

/// Low-level provider transport contract.
///
/// Calling a provider directly does not authorize its declared host access.
/// Host runtimes must use the configured request helpers (or perform the same
/// node-local permission check) before invoking an `Unrestricted` provider.
pub trait LanguageModelProvider {
    fn info(&self) -> LanguageModelProviderInfo;

    fn available_models(&self) -> Result<Vec<String>, LanguageModelError> {
        let info = self.info();
        Err(LanguageModelError::ProviderInvocationFailed {
            provider: info.provider,
            model: info.default_model,
            detail: "provider does not expose model discovery".into(),
        })
    }

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

    fn complete_streaming_events_with_cancellation(
        &self,
        request: &LanguageModelRequest,
        cancellation: &LanguageModelCancellation,
        on_event: &mut dyn FnMut(LanguageModelStreamEvent),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        if cancellation.is_cancelled() {
            return Err(cancelled_language_model_error(request, "before request"));
        }
        let response = self.complete_streaming_events(request, &mut |event| {
            if !cancellation.is_cancelled() {
                on_event(event);
            }
        })?;
        if cancellation.is_cancelled() {
            return Err(cancelled_language_model_error(request, "during response"));
        }
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

fn cancelled_language_model_error(
    request: &LanguageModelRequest,
    phase: &'static str,
) -> LanguageModelError {
    LanguageModelError::ProviderInvocationFailed {
        provider: request.provider.clone(),
        model: request.model.clone(),
        detail: format!("model invocation cancelled {phase}"),
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
            host_access: LanguageModelHostAccess::Unrestricted,
            capabilities: LanguageModelProviderCapabilities::default(),
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

pub use sinomo_runtime::api::lm::PROVIDER_DEFAULT_MODEL;

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
    if !request.tools.is_empty() {
        prompt.push_str("tools:\n```json\n");
        prompt.push_str(
            &serde_json::to_string_pretty(&request.tools)
                .expect("language model tool specs serialize"),
        );
        prompt.push_str("\n```\n\n");
    }
    prompt
}

pub use sinomo_runtime::api::lm::LanguageModelError;

pub use sinomo_runtime::api::lm::LanguageModelErrorCategory;

pub use sinomo_runtime::api::lm::classify_language_model_error_detail;

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

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
    fn tool_adapter_wraps_provider_custom_raw_input() {
        let tool = LanguageModelToolSpec {
            name: "apply_patch".to_string(),
            description: "Apply a patch.".to_string(),
            input_schema: serde_json::Value::Null,
            output_schema: serde_json::Value::Null,
            supports_parallel: false,
            provider_specs: vec![LanguageModelProviderToolSpec {
                provider: "openai".to_string(),
                custom: Some(LanguageModelCustomToolSpec {
                    format: LanguageModelCustomToolFormat::Grammar {
                        syntax: "lark".to_string(),
                        definition: "start: /.+/".to_string(),
                    },
                    raw_input_argument: Some("patch".to_string()),
                }),
            }],
        };
        let response = LanguageModelResponse {
            provider: "openai".to_string(),
            model: "gpt-5.2-codex".to_string(),
            content: String::new(),
            items: Vec::new(),
            tool_calls: vec![LanguageModelToolCall {
                call_id: Some("call_patch".to_string()),
                name: "apply_patch".to_string(),
                input: serde_json::json!("*** Begin Patch\n*** End Patch\n"),
                summary: None,
            }],
            usage: None,
        };

        let output =
            collect_language_model_tool_calls("openai", "gpt-5.2-codex", &response, &[], &[tool]);

        assert_eq!(output.tool_calls.len(), 1);
        assert_eq!(output.source, LanguageModelToolAdapterSource::Native);
        assert_eq!(
            output.tool_calls[0].input,
            serde_json::json!({ "patch": "*** Begin Patch\n*** End Patch\n" })
        );
    }

    #[test]
    fn native_tool_protocol_preference_lives_with_provider_model_contract() {
        assert!(language_model_prefers_native_tool_protocol(
            "openai-api",
            "gpt-5.2-codex"
        ));
        assert!(language_model_prefers_native_tool_protocol(
            CODEX_PROVIDER_ID,
            "codex-mini"
        ));
        assert!(language_model_prefers_native_tool_protocol(
            "deepseek-api",
            "deepseek-chat"
        ));
        assert!(language_model_prefers_native_tool_protocol(
            "gemini",
            "gemini-2.5-pro"
        ));
    }

    #[test]
    fn protocol_system_instructions_live_with_provider_model_contract() {
        let base = "Base agent instructions.";
        let native =
            language_model_protocol_system_instructions("openai-api", "gpt-5.2-codex", base);

        assert!(native.starts_with(base));
        assert!(native.contains("provider-native tool protocol"));
        assert!(native.contains("JSON `tool_calls` only as a fallback"));
        assert_eq!(
            language_model_protocol_system_instructions("unknown", "custom-model", base),
            base
        );
    }

    #[test]
    fn tool_adapter_recovers_dsml_tool_calls_from_text() {
        let content = format!(
            "I found it.\n{open}tool_calls>\n\
             {open}invoke name=\"run_command\">\n\
             {open}parameter name=\"command\" string=\"true\">python -m py_compile a.py{close}parameter>\n\
             {open}parameter name=\"timeout_ms\" string=\"false\">15000{close}parameter>\n\
             {close}invoke>\n\
             {close}tool_calls>",
            open = DSML_OPEN_PREFIX,
            close = DSML_CLOSE_PREFIX,
        );
        let response = LanguageModelResponse {
            provider: "deepseek".to_string(),
            model: "deepseek-v4-pro".to_string(),
            content,
            items: Vec::new(),
            tool_calls: Vec::new(),
            usage: None,
        };

        let output =
            collect_language_model_tool_calls("deepseek", "deepseek-v4-pro", &response, &[], &[]);

        assert_eq!(output.text.as_deref(), Some("I found it."));
        assert_eq!(
            output.source,
            LanguageModelToolAdapterSource::VendorFallback
        );
        assert_eq!(output.tool_calls.len(), 1);
        assert_eq!(output.tool_calls[0].name, "run_command");
        assert_eq!(
            output.tool_calls[0]
                .input
                .get("command")
                .and_then(serde_json::Value::as_str),
            Some("python -m py_compile a.py")
        );
        assert_eq!(
            output.tool_calls[0]
                .input
                .get("timeout_ms")
                .and_then(serde_json::Value::as_i64),
            Some(15000)
        );
    }

    #[test]
    fn language_model_errors_have_normalized_categories() {
        let rate_limit = LanguageModelError::ProviderInvocationFailed {
            provider: "openai".to_string(),
            model: "gpt".to_string(),
            detail: "HTTP 429: rate limit exceeded".to_string(),
        };
        assert_eq!(rate_limit.category(), LanguageModelErrorCategory::RateLimit);

        let prompt_too_large = LanguageModelError::ProviderInvocationFailed {
            provider: "openai".to_string(),
            model: "gpt".to_string(),
            detail: "context_length_exceeded: maximum context length".to_string(),
        };
        assert_eq!(
            prompt_too_large.category(),
            LanguageModelErrorCategory::PromptTooLarge
        );

        let output_too_large = LanguageModelError::ProviderInvocationFailed {
            provider: "openai".to_string(),
            model: "gpt".to_string(),
            detail: "completion token limit exceeded".to_string(),
        };
        assert_eq!(
            output_too_large.category(),
            LanguageModelErrorCategory::OutputTooLarge
        );

        let unsupported_tools = LanguageModelError::ProviderInvocationFailed {
            provider: "test".to_string(),
            model: "test".to_string(),
            detail: "provider does not support tool calls".to_string(),
        };
        assert_eq!(
            unsupported_tools.category(),
            LanguageModelErrorCategory::UnsupportedToolFormat
        );

        let auth = LanguageModelError::AuthenticationRequired {
            provider: "openai".to_string(),
            detail: "missing key".to_string(),
        };
        assert_eq!(auth.category(), LanguageModelErrorCategory::Authentication);
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
            Some(1_000_000)
        );
        assert_eq!(
            inferred_model_context_window_tokens(CODEX_PROVIDER_ID, "provider-default"),
            Some(OPENAI_LONG_CONTEXT_PRICE_THRESHOLD_TOKENS)
        );
        assert_eq!(
            inferred_model_max_context_window_tokens(CODEX_PROVIDER_ID, "provider-default"),
            Some(872_000)
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
        let root = sinomo_testdir::TestDir::new("sinomo-lm-models");
        let data_dir = SinomoDataDir::new(&root);

        let update =
            write_default_model_config(&data_dir, "openai", "gpt-5.5").expect("write defaults");

        assert_eq!(update.config.default_provider.as_deref(), Some("openai"));
        assert_eq!(update.config.default_model.as_deref(), Some("gpt-5.5"));
        assert_eq!(
            update.config_path,
            root.join("configs").join("lm").join("providers.json")
        );
        let config = load_model_config(&ModelPaths::new(&data_dir)).expect("model config");
        assert_eq!(config.default_provider.as_deref(), Some("openai"));
        assert_eq!(config.default_model.as_deref(), Some("gpt-5.5"));
    }

    #[test]
    fn model_config_cluster_txn_writes_config_document() {
        let config = ModelConfig {
            default_provider: Some("openai".to_string()),
            default_account: Some(DEFAULT_PROVIDER_ACCOUNT.to_string()),
            default_model: Some("gpt-5.5".to_string()),
            accounts: Vec::new(),
        };
        let txn = model_config_cluster_kv_txn(&config).expect("model config txn");
        let Some(sinomo_runtime::kv::KvOp::Put { key, value, .. }) = txn.success.into_iter().next()
        else {
            panic!("expected put op");
        };
        assert_eq!(key, LM_PROVIDER_CONFIG_KV_KEY);
        let entry = KvEntry {
            key,
            value,
            create_revision: sinomo_runtime::kv::KvRevision(1),
            modify_revision: sinomo_runtime::kv::KvRevision(1),
            version: sinomo_runtime::kv::KvVersion(1),
            lease_id: None,
        };
        let document: sinomo_runtime::config::ConfigDocument =
            entry.decode().expect("config document");
        assert_eq!(document.key.as_str(), LM_PROVIDER_CONFIG_KEY);
        let saved: ModelConfig = serde_json::from_value(document.value).expect("model config");
        assert_eq!(saved, config);
        let decoded = model_config_from_cluster_kv_entries(&[entry])
            .expect("decode entries")
            .expect("model config");
        assert_eq!(decoded, config);
    }

    #[test]
    fn model_config_reads_legacy_raw_cluster_kv_value() {
        let config = ModelConfig {
            default_provider: Some("openai".to_string()),
            default_account: Some(DEFAULT_PROVIDER_ACCOUNT.to_string()),
            default_model: Some("gpt-5.5".to_string()),
            accounts: Vec::new(),
        };
        let sinomo_runtime::kv::KvOp::Put { key, value, .. } =
            sinomo_runtime::kv::KvOp::put_serde(LM_PROVIDER_CONFIG_KV_KEY, &config, None)
                .expect("legacy op")
        else {
            panic!("expected put op");
        };
        let entry = KvEntry {
            key,
            value,
            create_revision: sinomo_runtime::kv::KvRevision(1),
            modify_revision: sinomo_runtime::kv::KvRevision(1),
            version: sinomo_runtime::kv::KvVersion(1),
            lease_id: None,
        };
        let decoded = model_config_from_cluster_kv_entries(&[entry])
            .expect("decode entries")
            .expect("model config");
        assert_eq!(decoded, config);
    }

    #[test]
    fn model_config_reads_old_account_schema_with_default_options() {
        let config: ModelConfig = serde_json::from_str(
            r#"{
                "default_provider": "openai",
                "accounts": [
                    {
                        "provider": "openai",
                        "account": "default",
                        "default_model": "gpt-5.5",
                        "enabled": true
                    }
                ]
            }"#,
        )
        .expect("old schema config");

        let account =
            provider_account_config(&config, "openai", Some("default")).expect("provider account");

        assert!(account.options.is_default());
        assert!(account.model_options.is_empty());
    }

    #[test]
    fn lm_config_registry_exposes_account_list_not_provider_credentials() {
        let store = model_config_store().expect("config store");
        let keys = store
            .registry()
            .definitions()
            .map(|definition| definition.key.as_str().to_string())
            .collect::<Vec<_>>();

        assert!(keys.iter().any(|key| key == LM_PROVIDER_CONFIG_KEY));
        assert!(keys.iter().any(|key| key == LM_RUNTIME_CONFIG_KEY));
        assert!(keys.iter().any(|key| key == LM_PROVIDER_HEALTH_CONFIG_KEY));
        assert!(
            !keys.iter().any(|key| key.starts_with("lm.credentials.")),
            "provider credentials are represented by account entries in lm.providers"
        );
    }

    fn schema_contains_json_fallback(value: &ConfigSchemaValue) -> bool {
        match value {
            ConfigSchemaValue::Json => true,
            ConfigSchemaValue::Object { fields, .. } => fields
                .iter()
                .any(|field| schema_contains_json_fallback(&field.value)),
            ConfigSchemaValue::Map { value } | ConfigSchemaValue::List { value } => {
                schema_contains_json_fallback(value)
            }
            ConfigSchemaValue::Bool
            | ConfigSchemaValue::I64 { .. }
            | ConfigSchemaValue::U64 { .. }
            | ConfigSchemaValue::F64 { .. }
            | ConfigSchemaValue::String { .. }
            | ConfigSchemaValue::StringList => false,
        }
    }

    #[test]
    fn lm_config_definitions_attach_structured_schemas_that_validate_defaults() {
        for definition in [
            model_config_definition().expect("provider definition"),
            model_runtime_config_definition().expect("runtime definition"),
            provider_health_config_definition().expect("health definition"),
        ] {
            let schema = definition.schema.as_ref().expect("detailed schema");
            assert!(
                !schema_contains_json_fallback(&schema.value),
                "{} must describe every field without an arbitrary JSON fallback",
                definition.key
            );
            let default_value = definition.default_value.as_ref().expect("default value");
            definition
                .validate_value(default_value)
                .unwrap_or_else(|error| {
                    panic!("{} default must match schema: {error}", definition.key)
                });
        }
    }

    #[test]
    fn model_config_schema_validates_nested_account_and_model_options() {
        let definition = model_config_definition().expect("provider definition");
        let config = ModelConfig {
            default_provider: Some("openai".to_string()),
            default_account: Some("work".to_string()),
            default_model: Some("gpt-5.5".to_string()),
            accounts: vec![ProviderAccountConfig {
                provider: "openai".to_string(),
                account: "work".to_string(),
                default_model: Some("gpt-5.5".to_string()),
                api_base_url: Some("https://api.example.test/v1".to_string()),
                api_key: Some("sk-test".to_string()),
                enabled: true,
                options: LanguageModelProviderOptions {
                    reasoning_effort: Some(ProviderReasoningEffort::High),
                    reasoning_budget_tokens: Some(8_192),
                    text_verbosity: Some(ProviderTextVerbosity::Medium),
                    max_context_tokens: Some(1_000_000),
                    auto_compact_token_limit: Some(900_000),
                    vendor_options: BTreeMap::from([(
                        "service_tier".to_string(),
                        "priority".to_string(),
                    )]),
                },
                model_options: vec![LanguageModelModelOptions {
                    model: "gpt-5.5-mini".to_string(),
                    options: LanguageModelProviderOptions {
                        reasoning_effort: Some(ProviderReasoningEffort::Low),
                        text_verbosity: Some(ProviderTextVerbosity::Low),
                        ..LanguageModelProviderOptions::default()
                    },
                }],
            }],
        };
        let mut value = serde_json::to_value(config).expect("provider config value");
        definition
            .validate_value(&value)
            .expect("nested provider config matches schema");

        value["accounts"][0]["model_options"][0]["options"]["reasoning_effort"] =
            serde_json::Value::String("ultra".to_string());
        definition
            .validate_value(&value)
            .expect("ultra matches provider option schema");

        value["accounts"][0]["model_options"][0]["options"]["reasoning_effort"] =
            serde_json::Value::String("impossibly-high".to_string());
        let error = definition
            .validate_value(&value)
            .expect_err("unknown reasoning choice must be rejected");
        assert!(
            error
                .to_string()
                .contains("$.accounts[0].model_options[0].options.reasoning_effort"),
            "{error}"
        );
    }

    #[test]
    fn runtime_and_health_schemas_validate_all_nested_fields() {
        let runtime = ModelRuntimeConfig {
            native_models_cache_dir: Some("/srv/sinomo/models".to_string()),
            minicpm_v46: MiniCpmV46RuntimeConfig {
                model_dir: Some("/srv/sinomo/models/minicpm-v4.6".to_string()),
                max_input_tokens: Some(32_768),
                max_output_tokens: Some(8_192),
            },
            pytorch_bridge: PytorchBridgeRuntimeConfig {
                endpoint: Some("http://127.0.0.1:32154".to_string()),
                python: Some("python3".to_string()),
                device: Some("cuda".to_string()),
                timeout_seconds: Some(120),
                mock: Some(false),
            },
        };
        model_runtime_config_definition()
            .expect("runtime definition")
            .validate_value(&serde_json::to_value(runtime).expect("runtime config value"))
            .expect("runtime config matches schema");

        let health = LanguageModelProviderHealthConfig {
            accounts: BTreeMap::from([(
                "openai:work".to_string(),
                LanguageModelProviderHealthSnapshot {
                    provider: "openai".to_string(),
                    account: "work".to_string(),
                    checked_at_unix: 10,
                    last_verified_at_unix: Some(9),
                    healthy: true,
                    error: None,
                    models: vec!["gpt-5.5".to_string()],
                    model_discovery_error: None,
                    usage: Some(LanguageModelProviderUsage {
                        provider: "openai".to_string(),
                        plan: Some("team".to_string()),
                        captured_at_unix: 10,
                        limits: vec![LanguageModelUsageLimit {
                            id: "tokens".to_string(),
                            name: Some("Tokens".to_string()),
                            scope: Some("monthly".to_string()),
                            unlimited: Some(false),
                            allowed: Some(true),
                            exhausted: Some(false),
                            remaining: Some(750_000.0),
                            entitlement: Some(1_000_000.0),
                            used_percent: Some(25.0),
                            remaining_percent: Some(75.0),
                            window_seconds: Some(2_592_000),
                            reset_at_unix: Some(2_600_000),
                        }],
                        credits: Some(LanguageModelCreditUsage {
                            has_credits: Some(true),
                            unlimited: Some(false),
                            balance: Some("42.00".to_string()),
                        }),
                    }),
                    usage_error: None,
                    stale: false,
                },
            )]),
        };
        provider_health_config_definition()
            .expect("health definition")
            .validate_value(&serde_json::to_value(health).expect("health config value"))
            .expect("health and usage snapshot match schema");
    }

    #[test]
    fn provider_health_config_is_cluster_authoritative() {
        let definition = provider_health_config_definition().expect("health definition");

        assert_eq!(definition.key.as_str(), LM_PROVIDER_HEALTH_CONFIG_KEY);
        assert_eq!(definition.cluster_key(), LM_PROVIDER_HEALTH_CONFIG_KV_KEY);
        assert_eq!(
            definition.cluster_mode,
            sinomo_runtime::config::ConfigClusterMode::ClusterOverrideLocal
        );
        assert!(definition.read_only);
    }

    #[test]
    fn provider_credentials_write_shared_config_document() {
        let root = sinomo_testdir::TestDir::new("sinomo-lm-credentials");
        let path = root
            .join("configs")
            .join("lm")
            .join("credentials")
            .join("openai.json");
        let credentials_path = Some(path.clone());
        let payload = serde_json::json!({
            "api_key": "sk-test",
            "source": "test",
            "created_at_unix": 1_u64
        });

        write_provider_credentials_json_value(
            OpenAiProvider::PROVIDER_ID,
            &credentials_path,
            payload.clone(),
        )
        .expect("write credentials");

        let document: sinomo_runtime::config::ConfigDocument =
            serde_json::from_str(&std::fs::read_to_string(&path).expect("credential document"))
                .expect("config document");
        assert_eq!(
            document.key.as_str(),
            &provider_credentials_document_key(OpenAiProvider::PROVIDER_ID).expect("key")
        );
        assert_eq!(document.value, payload);
        let value =
            read_provider_credentials_json_value(OpenAiProvider::PROVIDER_ID, &credentials_path)
                .expect("read credentials")
                .expect("credentials");
        assert_eq!(value["api_key"], "sk-test");
    }

    #[test]
    fn provider_credentials_read_legacy_file_and_migrate_to_shared_config_document() {
        let root = sinomo_testdir::TestDir::new("sinomo-lm-credentials-migrate");
        let legacy_path = root
            .join("providers")
            .join("openai")
            .join("credentials.json");
        std::fs::create_dir_all(legacy_path.parent().expect("legacy parent"))
            .expect("legacy parent");
        std::fs::write(
            &legacy_path,
            serde_json::json!({
                "api_key": "sk-legacy",
                "source": "legacy",
                "created_at_unix": 1_u64
            })
            .to_string(),
        )
        .expect("legacy credentials");
        let path = root
            .join("configs")
            .join("lm")
            .join("credentials")
            .join("openai.json");
        let credentials_path = Some(path.clone());

        let value =
            read_provider_credentials_json_value(OpenAiProvider::PROVIDER_ID, &credentials_path)
                .expect("read credentials")
                .expect("credentials");

        assert_eq!(value["api_key"], "sk-legacy");
        let document: sinomo_runtime::config::ConfigDocument = serde_json::from_str(
            &std::fs::read_to_string(&path).expect("migrated credential document"),
        )
        .expect("config document");
        assert_eq!(
            document.key.as_str(),
            &provider_credentials_document_key(OpenAiProvider::PROVIDER_ID).expect("key")
        );
        assert_eq!(document.value["api_key"], "sk-legacy");
    }

    #[test]
    fn model_config_serializes_xhigh_reasoning_effort_without_dash() {
        let config = ModelConfig {
            accounts: vec![ProviderAccountConfig {
                provider: OpenAiProvider::PROVIDER_ID.to_string(),
                account: DEFAULT_PROVIDER_ACCOUNT.to_string(),
                default_model: None,
                api_base_url: None,
                api_key: None,
                enabled: true,
                options: LanguageModelProviderOptions {
                    reasoning_effort: Some(ProviderReasoningEffort::XHigh),
                    ..LanguageModelProviderOptions::default()
                },
                model_options: Vec::new(),
            }],
            ..ModelConfig::default()
        };

        let json = serde_json::to_string(&config).expect("json");
        let parsed: ModelConfig = serde_json::from_str(&json).expect("round trip");

        assert!(json.contains(r#""reasoning_effort":"xhigh""#));
        assert_eq!(
            parsed.accounts[0].options.reasoning_effort,
            Some(ProviderReasoningEffort::XHigh)
        );
        assert_eq!(
            serde_json::from_str::<ProviderReasoningEffort>(r#""x-high""#).expect("legacy alias"),
            ProviderReasoningEffort::XHigh
        );
    }

    #[test]
    fn provider_options_for_model_merges_account_and_model_overrides() {
        let mut account_options = LanguageModelProviderOptions {
            reasoning_effort: Some(ProviderReasoningEffort::Low),
            text_verbosity: Some(ProviderTextVerbosity::Medium),
            ..LanguageModelProviderOptions::default()
        };
        account_options
            .vendor_options
            .insert("metadata.trace".to_string(), "false".to_string());
        let mut model_options = LanguageModelProviderOptions {
            reasoning_effort: Some(ProviderReasoningEffort::High),
            ..LanguageModelProviderOptions::default()
        };
        model_options
            .vendor_options
            .insert("metadata.trace".to_string(), "true".to_string());
        let account = ProviderAccountConfig {
            provider: OpenAiProvider::PROVIDER_ID.to_string(),
            account: DEFAULT_PROVIDER_ACCOUNT.to_string(),
            default_model: Some("gpt-5.5".to_string()),
            api_base_url: None,
            api_key: None,
            enabled: true,
            options: account_options,
            model_options: vec![LanguageModelModelOptions {
                model: "gpt-5.5".to_string(),
                options: model_options,
            }],
        };

        let options = provider_options_for_model(Some(&account), "gpt-5.5");

        assert_eq!(
            options.reasoning_effort,
            Some(ProviderReasoningEffort::High)
        );
        assert_eq!(options.text_verbosity, Some(ProviderTextVerbosity::Medium));
        assert_eq!(
            options
                .vendor_options
                .get("metadata.trace")
                .map(String::as_str),
            Some("true")
        );
    }

    #[test]
    fn save_provider_api_key_preserves_provider_options() {
        let mut config = ModelConfig::default();
        let mut account_options = LanguageModelProviderOptions {
            reasoning_effort: Some(ProviderReasoningEffort::XHigh),
            ..LanguageModelProviderOptions::default()
        };
        account_options
            .vendor_options
            .insert("text.format.strict".to_string(), "true".to_string());
        upsert_provider_account_config(
            &mut config,
            ProviderAccountConfig {
                provider: OpenAiProvider::PROVIDER_ID.to_string(),
                account: DEFAULT_PROVIDER_ACCOUNT.to_string(),
                default_model: Some("gpt-5.5".to_string()),
                api_base_url: Some("https://api.example.test/v1".to_string()),
                api_key: None,
                enabled: true,
                options: account_options.clone(),
                model_options: vec![LanguageModelModelOptions {
                    model: "gpt-5.5".to_string(),
                    options: LanguageModelProviderOptions {
                        text_verbosity: Some(ProviderTextVerbosity::Low),
                        ..LanguageModelProviderOptions::default()
                    },
                }],
            },
        )
        .expect("upsert account");

        save_provider_api_key_in_model_config(
            &mut config,
            OpenAiProvider::PROVIDER_ID,
            Some(DEFAULT_PROVIDER_ACCOUNT),
            "sk-test",
        )
        .expect("save key");

        let account = provider_account_config(
            &config,
            OpenAiProvider::PROVIDER_ID,
            Some(DEFAULT_PROVIDER_ACCOUNT),
        )
        .expect("provider account");
        assert_eq!(account.options, account_options);
        assert_eq!(account.model_options.len(), 1);
        assert_eq!(
            account.model_options[0].options.text_verbosity,
            Some(ProviderTextVerbosity::Low)
        );
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
    fn plain_prompt_renders_request_tools_outside_chat_messages() {
        let mut request = request();
        request.tools.push(LanguageModelToolSpec {
            name: "read_file".to_string(),
            description: "Read a workspace file.".to_string(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" }
                }
            }),
            output_schema: serde_json::Value::Null,
            supports_parallel: false,
            provider_specs: Vec::new(),
        });

        let prompt = render_plain_prompt(&request);

        assert!(prompt.contains("tools:\n```json"));
        assert!(prompt.contains("\"name\": \"read_file\""));
        assert!(prompt.contains("\"description\": \"Read a workspace file.\""));
    }

    #[test]
    fn system_instructions_append_provider_rendered_output_contract() {
        let instructions = language_model_system_instructions(
            CODEX_PROVIDER_ID,
            "gpt-5.2-codex",
            "Base prompt.",
            Some("Return JSON only."),
        );

        assert!(instructions.starts_with("Base prompt."));
        assert!(instructions.contains("Model protocol hint: native tool calls are available"));
        assert!(instructions.contains("Response contract:\nReturn JSON only."));
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
        assert_eq!(info.host_access, LanguageModelHostAccess::Unrestricted);
        assert!(!info.supports_json_object);
        assert!(info.supports_tool_permissions);
    }

    #[test]
    fn coding_agent_provider_host_access_is_explicit_and_fail_closed() {
        assert_eq!(
            ClaudeCodeProvider::default().info().host_access,
            LanguageModelHostAccess::Unrestricted
        );
        assert_eq!(
            GitHubCopilotCliProvider::default().info().host_access,
            LanguageModelHostAccess::Unrestricted
        );

        let legacy_info: LanguageModelProviderInfo = serde_json::from_value(serde_json::json!({
            "provider": "legacy-process-provider",
            "default_model": "legacy-model",
            "invocation": "process",
            "supports_json_object": false,
            "supports_tool_permissions": true
        }))
        .expect("legacy provider info");
        assert_eq!(
            legacy_info.host_access,
            LanguageModelHostAccess::Unrestricted
        );
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
    fn deepseek_provider_reports_messages_capabilities() {
        let info = DeepSeekProvider::default().info();

        assert_eq!(info.provider, DeepSeekProvider::PROVIDER_ID);
        assert_eq!(info.default_model, DEEPSEEK_DEFAULT_MODEL);
        assert_eq!(info.invocation, LanguageModelInvocation::Http);
        assert!(!info.supports_json_object);
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

        assert_eq!(extract_gemini_response_text(&value), "firstsecond");
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
    fn account_model_catalog_reports_discovery_failure_instead_of_inventing_models() {
        let account = ProviderAccountConfig {
            provider: OpenAiProvider::PROVIDER_ID.to_string(),
            account: DEFAULT_PROVIDER_ACCOUNT.to_string(),
            default_model: Some("gpt-5.5-custom".to_string()),
            api_base_url: Some("http://127.0.0.1:0/v1".to_string()),
            api_key: Some("sk-test".to_string()),
            enabled: true,
            options: LanguageModelProviderOptions::default(),
            model_options: Vec::new(),
        };

        assert!(matches!(
            provider_available_model_ids_for_account(OpenAiProvider::PROVIDER_ID, Some(&account)),
            Err(LanguageModelError::ProviderInvocationFailed { .. })
        ));
    }

    #[test]
    fn known_model_catalogs_do_not_require_credentials_or_network_access() {
        assert_eq!(
            provider_known_model_ids(DeepSeekProvider::PROVIDER_ID),
            vec![
                DEEPSEEK_DEFAULT_MODEL.to_string(),
                DEEPSEEK_V4_PRO_MODEL.to_string()
            ]
        );
        assert!(
            provider_known_model_ids(NvidiaProvider::PROVIDER_ID)
                .contains(&NVIDIA_DEFAULT_MODEL.to_string())
        );
        assert!(
            provider_known_model_ids(OpenAiProvider::PROVIDER_ID)
                .contains(&"gpt-6-astra".to_string())
        );
    }

    #[test]
    fn reasoning_effort_capability_matches_provider_and_model() {
        // OpenAI: only reasoning model families (not gpt-4*, etc.).
        assert!(model_supports_reasoning_effort(
            OpenAiProvider::PROVIDER_ID,
            "gpt-5.2-codex"
        ));
        assert!(model_supports_reasoning_effort(
            OpenAiProvider::PROVIDER_ID,
            "o3-mini"
        ));
        assert!(!model_supports_reasoning_effort(
            OpenAiProvider::PROVIDER_ID,
            "gpt-4o"
        ));
        // Anthropic extended-thinking family.
        assert!(model_supports_reasoning_effort(
            AnthropicProvider::PROVIDER_ID,
            "claude-sonnet-4-6"
        ));
        // Agent CLIs always run a reasoning model.
        assert!(model_supports_reasoning_effort(
            CodexCliProvider::PROVIDER_ID,
            "whatever"
        ));
        assert!(model_supports_reasoning_effort(
            ClaudeCodeProvider::PROVIDER_ID,
            "whatever"
        ));
        // Providers whose request builders never apply a reasoning effort.
        assert!(model_supports_reasoning_effort(
            GeminiProvider::PROVIDER_ID,
            "gemini-2.5-pro"
        ));
        assert!(!model_supports_reasoning_effort("definitely-unknown", "x"));
    }

    #[test]
    fn complete_configured_model_request_reports_unknown_provider() {
        let root = sinomo_testdir::TestDir::new("sinomo-lm-complete-unknown");
        let data_dir = SinomoDataDir::new(&root);

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
    fn configured_unrestricted_provider_requires_local_remote_management_permission() {
        let root = sinomo_testdir::TestDir::new("sinomo-lm-complete-unrestricted-denied");
        let data_dir = SinomoDataDir::new(&root);

        let error = complete_configured_model_request(
            &data_dir,
            ClaudeCodeProvider::PROVIDER_ID,
            CLAUDE_CODE_DEFAULT_MODEL,
            Some(root.to_path_buf()),
            &request(),
        )
        .expect_err("Claude Code must be denied before its process is spawned");

        assert!(matches!(
            &error,
            LanguageModelError::ProviderInvocationFailed { provider, .. }
                if provider == ClaudeCodeProvider::PROVIDER_ID
        ));
        assert!(error.to_string().contains("remote-management"));
    }

    #[test]
    fn configured_streaming_unrestricted_provider_requires_local_permission() {
        let root = sinomo_testdir::TestDir::new("sinomo-lm-stream-unrestricted-denied");
        let data_dir = SinomoDataDir::new(&root);
        let mut events = Vec::new();

        let error = complete_configured_model_request_streaming_events_with_account(
            &data_dir,
            GitHubCopilotCliProvider::PROVIDER_ID,
            PROVIDER_DEFAULT_MODEL,
            None,
            Some(root.to_path_buf()),
            &request(),
            &mut |event| events.push(event),
        )
        .expect_err("Copilot CLI must be denied before its process is spawned");

        assert!(events.is_empty());
        assert!(matches!(
            &error,
            LanguageModelError::ProviderInvocationFailed { provider, .. }
                if provider == GitHubCopilotCliProvider::PROVIDER_ID
        ));
        assert!(error.to_string().contains("remote-management"));
    }

    #[test]
    fn configured_network_only_provider_does_not_require_remote_management_permission() {
        let root = sinomo_testdir::TestDir::new("sinomo-lm-complete-network-only-allowed");
        let data_dir = SinomoDataDir::new(&root);
        let provider = OpenAiProvider::new(OpenAiConfig::default());

        authorize_configured_provider_host_access(&data_dir, &provider, PROVIDER_DEFAULT_MODEL)
            .expect("network-only provider does not require remote-management");
    }

    #[test]
    fn chat_completion_stream_emits_text_usage_and_finish() {
        let body = concat!(
            "data: {\"choices\":[{\"delta\":{\"content\":\"hel\"}}]}\n\n",
            "data: {\"choices\":[{\"delta\":{\"content\":\"lo\"}}]}\n\n",
            "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],",
            "\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":4}}\n\n",
            "data: [DONE]\n\n"
        );
        let mut events = Vec::new();

        let response = stream_openai_chat_completion_response(
            body.as_bytes(),
            "test",
            "model",
            &mut |event| {
                events.push(event);
            },
        )
        .unwrap();

        assert_eq!(response.content, "hello");
        assert_eq!(
            response.usage,
            Some(LanguageModelUsage {
                input_tokens: Some(3),
                output_tokens: Some(4),
            })
        );
        assert_eq!(
            events,
            vec![
                LanguageModelStreamEvent::TextDelta {
                    delta: "hel".to_string()
                },
                LanguageModelStreamEvent::TextDelta {
                    delta: "lo".to_string()
                },
                LanguageModelStreamEvent::Usage {
                    usage: LanguageModelUsage {
                        input_tokens: Some(3),
                        output_tokens: Some(4),
                    }
                },
                LanguageModelStreamEvent::Finished {
                    finish_reason: Some("stop".to_string())
                },
            ]
        );
    }

    #[test]
    fn chat_completion_stream_emits_provider_reasoning_fragments() {
        let body = concat!(
            "data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"inspect \"}}]}\n\n",
            "data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"context\"}}]}\n\n",
            "data: {\"choices\":[{\"delta\":{\"content\":\"done\"},\"finish_reason\":\"stop\"}]}\n\n",
            "data: [DONE]\n\n"
        );
        let mut events = Vec::new();

        let response = stream_openai_chat_completion_response(
            body.as_bytes(),
            "test",
            "reasoner",
            &mut |event| events.push(event),
        )
        .unwrap();

        assert!(matches!(
            &response.items[0],
            LanguageModelResponseItem::ReasoningSummary { summary }
                if summary == "inspect context"
        ));
        assert_eq!(
            events
                .iter()
                .filter_map(|event| match event {
                    LanguageModelStreamEvent::ReasoningDelta { delta } => Some(delta.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            vec!["inspect ", "context"]
        );
    }

    #[test]
    fn chat_completion_stream_reconstructs_tool_calls() {
        let first = serde_json::json!({
            "choices": [{
                "delta": {
                    "tool_calls": [{
                        "index": 0,
                        "id": "call_1",
                        "function": {
                            "name": "read_file",
                            "arguments": "{\"path\""
                        }
                    }]
                }
            }]
        });
        let second = serde_json::json!({
            "choices": [{
                "delta": {
                    "tool_calls": [{
                        "index": 0,
                        "function": {
                            "arguments": ":\"Cargo.toml\"}"
                        }
                    }]
                },
                "finish_reason": "tool_calls"
            }]
        });
        let body = format!("data: {first}\n\ndata: {second}\n\ndata: [DONE]\n\n");
        let mut events = Vec::new();

        let response = stream_openai_chat_completion_response(
            body.as_bytes(),
            "test",
            "model",
            &mut |event| {
                events.push(event);
            },
        )
        .unwrap();

        assert_eq!(response.content, "");
        assert_eq!(response.tool_calls.len(), 1);
        assert_eq!(response.tool_calls[0].call_id.as_deref(), Some("call_1"));
        assert_eq!(response.tool_calls[0].name, "read_file");
        assert_eq!(
            response.tool_calls[0].input,
            serde_json::json!({ "path": "Cargo.toml" })
        );
        assert!(matches!(
            events.first(),
            Some(LanguageModelStreamEvent::ToolCallStarted { name, .. }) if name == "read_file"
        ));
        assert!(matches!(
            events.last(),
            Some(LanguageModelStreamEvent::Finished {
                finish_reason: Some(reason)
            }) if reason == "tool_calls"
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
    fn stored_provider_api_key_reads_config_document_credentials() {
        let root = sinomo_testdir::TestDir::new("sinomo-lm-stored-api-key");
        let data_dir = SinomoDataDir::new(&root);

        set_provider_api_key(&data_dir, DeepSeekProvider::PROVIDER_ID, "sk-test")
            .expect("store provider api key");

        assert_eq!(
            stored_provider_api_key_in_data_dir(&data_dir, DeepSeekProvider::PROVIDER_ID)
                .expect("read stored provider api key"),
            Some("sk-test".to_string())
        );
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

    #[test]
    fn provider_capabilities_preserve_existing_minicpm_realtime_aliases() {
        for provider in ["minicpm", "minicpm-o", "modelbest", "minicpm-o-4.5"] {
            let capabilities = provider_capabilities(provider);
            assert!(capabilities.realtime_websocket, "provider={provider}");
            assert!(!capabilities.responses_http, "provider={provider}");
        }
    }

    #[test]
    fn openai_capabilities_advertise_mcp_for_responses_and_realtime() {
        assert!(OpenAiProvider::default().info().capabilities.mcp);
        assert!(provider_capabilities(OpenAiProvider::PROVIDER_ID).mcp);
        assert!(provider_capabilities("openai-api").mcp);
    }
}
