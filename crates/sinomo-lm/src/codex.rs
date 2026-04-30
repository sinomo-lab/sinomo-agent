use crate::api_key::default_api_key_credentials_path;
use crate::{
    LanguageModelError, LanguageModelInvocation, LanguageModelMessage, LanguageModelProvider,
    LanguageModelProviderInfo, LanguageModelRequest, LanguageModelResponse,
    LanguageModelResponseFormat, LanguageModelRole, LanguageModelUsage, PROVIDER_DEFAULT_MODEL,
    selected_model,
};
use reqwest::blocking::Client;
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue, USER_AGENT};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

pub const CODEX_DEFAULT_MODEL: &str = "gpt-5.2-codex";
pub const CODEX_PROVIDER_ID: &str = "openai-codex";
pub const OPENAI_API_KEY_ENV_VAR: &str = "OPENAI_API_KEY";
pub const CODEX_API_KEY_ENV_VAR: &str = "CODEX_API_KEY";
pub const SINOMO_CODEX_API_KEY_ENV_VAR: &str = "SINOMO_CODEX_API_KEY";
pub const SINOMO_CODEX_CREDENTIALS_FILE_ENV_VAR: &str = "SINOMO_CODEX_CREDENTIALS_FILE";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CodexConfig {
    pub provider_id: String,
    pub default_model: String,
    pub max_iterations: usize,
}

impl Default for CodexConfig {
    fn default() -> Self {
        Self {
            provider_id: CODEX_PROVIDER_ID.to_string(),
            default_model: CODEX_DEFAULT_MODEL.to_string(),
            max_iterations: 12,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenAiResponsesCodexConfig {
    pub api_base_url: String,
    pub default_model: String,
    pub credentials_path: Option<PathBuf>,
}

impl Default for OpenAiResponsesCodexConfig {
    fn default() -> Self {
        Self {
            api_base_url: "https://api.openai.com/v1".to_string(),
            default_model: CODEX_DEFAULT_MODEL.to_string(),
            credentials_path: default_api_key_credentials_path(
                SINOMO_CODEX_CREDENTIALS_FILE_ENV_VAR,
                CODEX_PROVIDER_ID,
            ),
        }
    }
}

fn env_secret(env_var: &str) -> Option<String> {
    std::env::var(env_var)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn resolve_codex_api_key(credentials_path: &Option<PathBuf>) -> Result<String, LanguageModelError> {
    if let Some(credentials_path) = credentials_path
        && let Some(api_key) = read_codex_auth_json_api_key(credentials_path)?
    {
        return Ok(api_key);
    }
    for env_var in [
        SINOMO_CODEX_API_KEY_ENV_VAR,
        OPENAI_API_KEY_ENV_VAR,
        CODEX_API_KEY_ENV_VAR,
    ] {
        if let Some(api_key) = env_secret(env_var) {
            return Ok(api_key);
        }
    }
    Err(LanguageModelError::AuthenticationRequired {
        provider: CODEX_PROVIDER_ID.to_string(),
        detail: "no Sinomo Codex API key found; run `sinomo providers auth codex`, set SINOMO_CODEX_API_KEY, or set OPENAI_API_KEY/CODEX_API_KEY".to_string(),
    })
}

fn read_codex_auth_json_api_key(
    credentials_path: &PathBuf,
) -> Result<Option<String>, LanguageModelError> {
    let contents = match fs::read_to_string(credentials_path) {
        Ok(contents) => contents,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(LanguageModelError::TokenStore {
                path: credentials_path.display().to_string(),
                detail: source.to_string(),
            });
        }
    };
    let auth = serde_json::from_str::<CodexAuthDotJson>(&contents).map_err(|source| {
        LanguageModelError::TokenStore {
            path: credentials_path.display().to_string(),
            detail: source.to_string(),
        }
    })?;
    Ok(auth
        .openai_api_key
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty()))
}

fn save_codex_api_key(
    credentials_path: &Option<PathBuf>,
    api_key: &str,
) -> Result<(), LanguageModelError> {
    let Some(credentials_path) = credentials_path else {
        return Err(LanguageModelError::AuthenticationRequired {
            provider: CODEX_PROVIDER_ID.to_string(),
            detail: "could not resolve a Sinomo Codex credentials path; set SINOMO_DATA_DIR or SINOMO_CODEX_CREDENTIALS_FILE".to_string(),
        });
    };
    if let Some(parent) = credentials_path.parent() {
        fs::create_dir_all(parent).map_err(|source| LanguageModelError::TokenStore {
            path: credentials_path.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let auth = CodexAuthDotJson {
        auth_mode: Some("apikey".to_string()),
        openai_api_key: Some(api_key.trim().to_string()),
        tokens: None,
        last_refresh: None,
        agent_identity: None,
    };
    let json = serde_json::to_string_pretty(&auth).map_err(|source| {
        LanguageModelError::RequestSerialization {
            detail: source.to_string(),
        }
    })?;
    fs::write(credentials_path, json).map_err(|source| LanguageModelError::TokenStore {
        path: credentials_path.display().to_string(),
        detail: source.to_string(),
    })
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct CodexAuthDotJson {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    auth_mode: Option<String>,
    #[serde(rename = "OPENAI_API_KEY")]
    openai_api_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tokens: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_refresh: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    agent_identity: Option<String>,
}

#[derive(Clone, Debug)]
pub struct OpenAiResponsesCodexClient {
    config: OpenAiResponsesCodexConfig,
    http: Client,
}

impl OpenAiResponsesCodexClient {
    pub fn new(config: OpenAiResponsesCodexConfig) -> Self {
        Self {
            config,
            http: Client::new(),
        }
    }

    pub fn api_key(&self) -> Result<String, LanguageModelError> {
        resolve_codex_api_key(&self.config.credentials_path)
    }

    pub fn save_api_key(&self, api_key: &str, _source: &str) -> Result<(), LanguageModelError> {
        save_codex_api_key(&self.config.credentials_path, api_key)
    }

    pub fn credentials_path(&self) -> Option<PathBuf> {
        self.config.credentials_path.clone()
    }
}

impl Default for OpenAiResponsesCodexClient {
    fn default() -> Self {
        Self::new(OpenAiResponsesCodexConfig::default())
    }
}

impl CodexClient for OpenAiResponsesCodexClient {
    fn complete(
        &self,
        request: &CodexModelRequest,
    ) -> Result<CodexModelResponse, LanguageModelError> {
        let api_key = self.api_key()?;
        let url = format!(
            "{}/responses",
            self.config.api_base_url.trim_end_matches('/')
        );
        let response = self
            .http
            .post(url)
            .headers(codex_headers(&api_key, CODEX_PROVIDER_ID, &request.model)?)
            .json(&openai_responses_codex_payload(request))
            .send()
            .map_err(|source| LanguageModelError::ProviderInvocationFailed {
                provider: CODEX_PROVIDER_ID.to_string(),
                model: request.model.clone(),
                detail: source.to_string(),
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(LanguageModelError::ProviderInvocationFailed {
                provider: CODEX_PROVIDER_ID.to_string(),
                model: request.model.clone(),
                detail: format!("OpenAI Responses API failed: HTTP {status}: {body}"),
            });
        }

        let value = response.json::<serde_json::Value>().map_err(|source| {
            LanguageModelError::ProviderInvocationFailed {
                provider: CODEX_PROVIDER_ID.to_string(),
                model: request.model.clone(),
                detail: source.to_string(),
            }
        })?;

        Ok(parse_openai_responses_codex_response(&value))
    }
}

pub trait CodexClient {
    fn complete(
        &self,
        request: &CodexModelRequest,
    ) -> Result<CodexModelResponse, LanguageModelError>;
}

pub trait CodexTool {
    fn descriptor(&self) -> CodexToolDescriptor;
    fn execute(&self, call: &CodexToolCall) -> CodexToolResult;
}

pub trait CodexPromptBuilder {
    fn build_system_prompt(&self, request: &LanguageModelRequest) -> Option<String>;
}

pub trait CodexContextManager {
    fn prepare_messages(&self, request: &LanguageModelRequest) -> Vec<CodexMessage>;
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DefaultCodexPromptBuilder {
    pub extra_instructions: Vec<String>,
}

impl CodexPromptBuilder for DefaultCodexPromptBuilder {
    fn build_system_prompt(&self, request: &LanguageModelRequest) -> Option<String> {
        let mut parts: Vec<String> = request
            .messages
            .iter()
            .filter(|message| message.role == LanguageModelRole::System)
            .map(|message| message.content.clone())
            .collect();
        parts.extend(self.extra_instructions.iter().cloned());
        if parts.is_empty() {
            None
        } else {
            Some(parts.join("\n\n"))
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DefaultCodexContextManager {
    pub max_messages: Option<usize>,
}

impl CodexContextManager for DefaultCodexContextManager {
    fn prepare_messages(&self, request: &LanguageModelRequest) -> Vec<CodexMessage> {
        let mut messages: Vec<CodexMessage> = request
            .messages
            .iter()
            .filter(|message| message.role != LanguageModelRole::System)
            .map(CodexMessage::from_language_model_message)
            .collect();
        if let Some(max_messages) = self.max_messages
            && messages.len() > max_messages
        {
            messages = messages[messages.len() - max_messages..].to_vec();
        }
        messages
    }
}

#[derive(Default)]
pub struct CodexToolRegistry {
    tools: BTreeMap<String, Box<dyn CodexTool>>,
}

impl CodexToolRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<T: CodexTool + 'static>(&mut self, tool: T) {
        self.tools
            .insert(tool.descriptor().name.clone(), Box::new(tool));
    }

    pub fn register_boxed(&mut self, tool: Box<dyn CodexTool>) {
        self.tools.insert(tool.descriptor().name.clone(), tool);
    }

    pub fn descriptors(&self) -> Vec<CodexToolDescriptor> {
        self.tools.values().map(|tool| tool.descriptor()).collect()
    }

    pub fn execute(&self, call: &CodexToolCall) -> CodexToolResult {
        self.tools
            .get(&call.name)
            .map(|tool| tool.execute(call))
            .unwrap_or_else(|| CodexToolResult {
                call_id: call.call_id.clone(),
                name: call.name.clone(),
                content: format!("Tool not found: {}", call.name),
                is_error: true,
                metadata: serde_json::Value::Null,
            })
    }
}

pub struct CodexProvider<
    C = OpenAiResponsesCodexClient,
    P = DefaultCodexPromptBuilder,
    M = DefaultCodexContextManager,
> {
    config: CodexConfig,
    client: C,
    tools: CodexToolRegistry,
    prompt_builder: P,
    context_manager: M,
}

impl CodexProvider<OpenAiResponsesCodexClient> {
    pub fn openai_responses() -> Self {
        Self::new(
            CodexConfig::default(),
            OpenAiResponsesCodexClient::default(),
            CodexToolRegistry::new(),
            DefaultCodexPromptBuilder::default(),
            DefaultCodexContextManager::default(),
        )
    }

    pub fn api_key(&self) -> Result<String, LanguageModelError> {
        self.client.api_key()
    }

    pub fn save_api_key(&self, api_key: &str, source: &str) -> Result<(), LanguageModelError> {
        self.client.save_api_key(api_key, source)
    }

    pub fn credentials_path(&self) -> Option<PathBuf> {
        self.client.credentials_path()
    }

    pub fn test_connection(&self) -> Result<(), LanguageModelError> {
        let request = LanguageModelRequest {
            provider: Self::PROVIDER_ID.to_string(),
            model: PROVIDER_DEFAULT_MODEL.to_string(),
            messages: vec![LanguageModelMessage {
                role: LanguageModelRole::User,
                content: "Reply with ok.".to_string(),
            }],
            temperature: None,
            max_output_tokens: Some(8),
            response_format: LanguageModelResponseFormat::Text,
            metadata: serde_json::Value::Null,
        };
        self.complete(&request).map(|_| ())
    }
}

impl<C, P, M> CodexProvider<C, P, M> {
    pub const PROVIDER_ID: &'static str = CODEX_PROVIDER_ID;

    pub fn new(
        config: CodexConfig,
        client: C,
        tools: CodexToolRegistry,
        prompt_builder: P,
        context_manager: M,
    ) -> Self {
        Self {
            config,
            client,
            tools,
            prompt_builder,
            context_manager,
        }
    }

    pub fn tools(&self) -> &CodexToolRegistry {
        &self.tools
    }

    pub fn tools_mut(&mut self) -> &mut CodexToolRegistry {
        &mut self.tools
    }
}

impl<C, P, M> LanguageModelProvider for CodexProvider<C, P, M>
where
    C: CodexClient,
    P: CodexPromptBuilder,
    M: CodexContextManager,
{
    fn info(&self) -> LanguageModelProviderInfo {
        LanguageModelProviderInfo {
            provider: self.config.provider_id.clone(),
            default_model: self.config.default_model.clone(),
            invocation: LanguageModelInvocation::Http,
            supports_json_object: true,
            supports_tool_permissions: true,
        }
    }

    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        let model = selected_model(&self.config.default_model, request);
        let mut messages = self.context_manager.prepare_messages(request);
        if let Some(system_prompt) = self.prompt_builder.build_system_prompt(request) {
            messages.insert(0, CodexMessage::system(system_prompt));
        }
        let tools = self.tools.descriptors();

        for _ in 0..self.config.max_iterations {
            let codex_request = CodexModelRequest {
                model: model.clone(),
                messages: messages.clone(),
                tools: tools.clone(),
                temperature: request.temperature,
                max_output_tokens: request.max_output_tokens,
                response_format: request.response_format.clone(),
                metadata: request.metadata.clone(),
            };
            let response = self.client.complete(&codex_request)?;
            if response.tool_calls.is_empty() {
                return Ok(LanguageModelResponse {
                    provider: self.config.provider_id.clone(),
                    model,
                    content: response.content,
                    usage: response.usage,
                });
            }

            if response.continuation_items.is_empty() {
                messages.push(CodexMessage::assistant_tool_calls(
                    response.content.clone(),
                    response.tool_calls.clone(),
                ));
            } else {
                for item in response.continuation_items {
                    messages.push(CodexMessage::raw_item(item));
                }
            }

            for call in response.tool_calls {
                let result = self.tools.execute(&call);
                messages.push(CodexMessage::tool_result(result));
            }
        }

        Err(LanguageModelError::ProviderInvocationFailed {
            provider: self.config.provider_id.clone(),
            model,
            detail: format!(
                "codex provider exceeded {} tool iteration(s)",
                self.config.max_iterations
            ),
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CodexModelRequest {
    pub model: String,
    pub messages: Vec<CodexMessage>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<CodexToolDescriptor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u32>,
    pub response_format: LanguageModelResponseFormat,
    #[serde(default)]
    pub metadata: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CodexModelResponse {
    #[serde(default)]
    pub content: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<CodexToolCall>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub continuation_items: Vec<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<LanguageModelUsage>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CodexToolDescriptor {
    pub name: String,
    pub description: String,
    pub parameters: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CodexToolCall {
    pub call_id: String,
    pub name: String,
    #[serde(default)]
    pub input: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CodexToolResult {
    pub call_id: String,
    pub name: String,
    pub content: String,
    pub is_error: bool,
    #[serde(default)]
    pub metadata: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum CodexRole {
    System,
    User,
    Assistant,
    Tool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CodexMessage {
    pub role: CodexRole,
    #[serde(default)]
    pub content: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<CodexToolCall>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_item: Option<serde_json::Value>,
}

impl CodexMessage {
    pub fn system(content: impl Into<String>) -> Self {
        Self::new(CodexRole::System, content)
    }

    pub fn user(content: impl Into<String>) -> Self {
        Self::new(CodexRole::User, content)
    }

    pub fn assistant(content: impl Into<String>) -> Self {
        Self::new(CodexRole::Assistant, content)
    }

    pub fn assistant_tool_calls(
        content: impl Into<String>,
        tool_calls: Vec<CodexToolCall>,
    ) -> Self {
        Self {
            role: CodexRole::Assistant,
            content: content.into(),
            tool_calls,
            tool_call_id: None,
            raw_item: None,
        }
    }

    pub fn tool_result(result: CodexToolResult) -> Self {
        Self {
            role: CodexRole::Tool,
            content: result.content,
            tool_calls: Vec::new(),
            tool_call_id: Some(result.call_id),
            raw_item: None,
        }
    }

    pub fn raw_item(item: serde_json::Value) -> Self {
        Self {
            role: CodexRole::Assistant,
            content: String::new(),
            tool_calls: Vec::new(),
            tool_call_id: None,
            raw_item: Some(item),
        }
    }

    fn new(role: CodexRole, content: impl Into<String>) -> Self {
        Self {
            role,
            content: content.into(),
            tool_calls: Vec::new(),
            tool_call_id: None,
            raw_item: None,
        }
    }

    fn from_language_model_message(message: &LanguageModelMessage) -> Self {
        let role = match message.role {
            LanguageModelRole::System => CodexRole::System,
            LanguageModelRole::User => CodexRole::User,
            LanguageModelRole::Assistant => CodexRole::Assistant,
        };
        Self::new(role, message.content.clone())
    }
}

fn codex_headers(
    api_key: &str,
    provider: &str,
    model: &str,
) -> Result<HeaderMap, LanguageModelError> {
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(USER_AGENT, HeaderValue::from_static("Sinomo/0.1"));
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {api_key}")).map_err(|source| {
            LanguageModelError::ProviderInvocationFailed {
                provider: provider.to_string(),
                model: model.to_string(),
                detail: source.to_string(),
            }
        })?,
    );
    Ok(headers)
}

fn openai_responses_codex_payload(request: &CodexModelRequest) -> serde_json::Value {
    let instructions = request
        .messages
        .iter()
        .filter(|message| message.role == CodexRole::System)
        .map(|message| message.content.as_str())
        .filter(|content| !content.trim().is_empty())
        .collect::<Vec<_>>()
        .join("\n\n");
    let input: Vec<serde_json::Value> = request
        .messages
        .iter()
        .filter(|message| message.role != CodexRole::System)
        .map(openai_responses_input_item)
        .collect();
    let tools: Vec<serde_json::Value> = request
        .tools
        .iter()
        .map(|tool| {
            serde_json::json!({
                "type": "function",
                "name": tool.name,
                "description": tool.description,
                "parameters": tool.parameters,
            })
        })
        .collect();
    let mut payload = serde_json::json!({
        "model": request.model,
        "input": input,
        "store": false,
    });
    if !instructions.is_empty() {
        payload["instructions"] = serde_json::json!(instructions);
    }
    if !tools.is_empty() {
        payload["tools"] = serde_json::json!(tools);
    }
    if let Some(temperature) = request.temperature {
        payload["temperature"] = serde_json::json!(temperature);
    }
    if let Some(max_output_tokens) = request.max_output_tokens {
        payload["max_output_tokens"] = serde_json::json!(max_output_tokens);
    }
    if request.response_format == LanguageModelResponseFormat::JsonObject {
        payload["text"] = serde_json::json!({ "format": { "type": "json_object" } });
    }
    payload
}

fn openai_responses_input_item(message: &CodexMessage) -> serde_json::Value {
    if let Some(raw_item) = &message.raw_item {
        return raw_item.clone();
    }
    match message.role {
        CodexRole::User => serde_json::json!({
            "role": "user",
            "content": message.content,
        }),
        CodexRole::Assistant => serde_json::json!({
            "role": "assistant",
            "content": message.content,
        }),
        CodexRole::Tool => serde_json::json!({
            "type": "function_call_output",
            "call_id": message.tool_call_id,
            "output": message.content,
        }),
        CodexRole::System => serde_json::Value::Null,
    }
}

fn parse_openai_responses_codex_response(value: &serde_json::Value) -> CodexModelResponse {
    let output = value
        .get("output")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    let content = value
        .get("output_text")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| extract_output_text(&output));
    let tool_calls = output
        .iter()
        .filter_map(parse_function_call_item)
        .collect::<Vec<_>>();
    let continuation_items = output
        .into_iter()
        .filter(|item| {
            matches!(
                item.get("type").and_then(serde_json::Value::as_str),
                Some("function_call" | "reasoning")
            )
        })
        .collect();
    let usage = value.get("usage").map(|usage| LanguageModelUsage {
        input_tokens: usage
            .get("input_tokens")
            .and_then(serde_json::Value::as_u64),
        output_tokens: usage
            .get("output_tokens")
            .and_then(serde_json::Value::as_u64),
    });
    CodexModelResponse {
        content,
        tool_calls,
        continuation_items,
        usage,
    }
}

fn extract_output_text(output: &[serde_json::Value]) -> String {
    output
        .iter()
        .flat_map(|item| {
            item.get("content")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
        })
        .filter_map(|content| content.get("text").and_then(serde_json::Value::as_str))
        .collect::<Vec<_>>()
        .join("\n")
}

fn parse_function_call_item(item: &serde_json::Value) -> Option<CodexToolCall> {
    if item.get("type").and_then(serde_json::Value::as_str) != Some("function_call") {
        return None;
    }
    let arguments = item
        .get("arguments")
        .and_then(serde_json::Value::as_str)
        .and_then(|arguments| serde_json::from_str(arguments).ok())
        .unwrap_or(serde_json::Value::Null);
    Some(CodexToolCall {
        call_id: item
            .get("call_id")
            .or_else(|| item.get("id"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string(),
        name: item
            .get("name")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string(),
        input: arguments,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PROVIDER_DEFAULT_MODEL;
    use std::cell::RefCell;
    use std::fs;
    use std::rc::Rc;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn request() -> LanguageModelRequest {
        LanguageModelRequest {
            provider: CodexProvider::<FixtureCodexClient>::PROVIDER_ID.to_string(),
            model: PROVIDER_DEFAULT_MODEL.to_string(),
            messages: vec![
                LanguageModelMessage {
                    role: LanguageModelRole::System,
                    content: "Use Sinomo tools.".to_string(),
                },
                LanguageModelMessage {
                    role: LanguageModelRole::User,
                    content: "Inspect the repo.".to_string(),
                },
            ],
            temperature: Some(0.2),
            max_output_tokens: Some(200),
            response_format: LanguageModelResponseFormat::Text,
            metadata: serde_json::Value::Null,
        }
    }

    #[test]
    fn codex_runs_tool_loop_before_final_answer() {
        let requests = Rc::new(RefCell::new(Vec::new()));
        let client = FixtureCodexClient {
            requests: Rc::clone(&requests),
            responses: RefCell::new(vec![
                CodexModelResponse {
                    content: String::new(),
                    tool_calls: vec![CodexToolCall {
                        call_id: "call_1".to_string(),
                        name: "read_context".to_string(),
                        input: serde_json::json!({ "path": "README.md" }),
                    }],
                    continuation_items: vec![serde_json::json!({
                        "type": "function_call",
                        "call_id": "call_1",
                        "name": "read_context",
                        "arguments": "{\"path\":\"README.md\"}"
                    })],
                    usage: None,
                },
                CodexModelResponse {
                    content: "Done with README context.".to_string(),
                    tool_calls: Vec::new(),
                    continuation_items: Vec::new(),
                    usage: None,
                },
            ]),
        };
        let mut tools = CodexToolRegistry::new();
        tools.register(FixtureTool);
        let provider = CodexProvider::new(
            CodexConfig::default(),
            client,
            tools,
            DefaultCodexPromptBuilder {
                extra_instructions: vec!["Return concise answers.".to_string()],
            },
            DefaultCodexContextManager::default(),
        );

        let response = provider.complete(&request()).expect("codex response");

        assert_eq!(response.content, "Done with README context.");
        let requests = requests.borrow();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].tools[0].name, "read_context");
        assert_eq!(
            requests[0].messages[0].content,
            "Use Sinomo tools.\n\nReturn concise answers."
        );
        assert!(
            requests[1]
                .messages
                .iter()
                .any(|message| message.raw_item.is_some())
        );
        assert!(requests[1].messages.iter().any(|message| {
            message.role == CodexRole::Tool
                && message.tool_call_id.as_deref() == Some("call_1")
                && message.content.contains("README context")
        }));
    }

    #[test]
    fn context_manager_can_limit_history() {
        let manager = DefaultCodexContextManager {
            max_messages: Some(1),
        };
        let messages = manager.prepare_messages(&LanguageModelRequest {
            messages: vec![
                LanguageModelMessage {
                    role: LanguageModelRole::User,
                    content: "old".to_string(),
                },
                LanguageModelMessage {
                    role: LanguageModelRole::User,
                    content: "new".to_string(),
                },
            ],
            ..request()
        });

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].content, "new");
    }

    #[test]
    fn openai_responses_payload_preserves_tools_and_tool_outputs() {
        let payload = openai_responses_codex_payload(&CodexModelRequest {
            model: "gpt-5.2-codex".to_string(),
            messages: vec![
                CodexMessage::system("system prompt"),
                CodexMessage::user("run tool"),
                CodexMessage::raw_item(serde_json::json!({
                    "type": "reasoning",
                    "id": "rs_1",
                    "summary": []
                })),
                CodexMessage::tool_result(CodexToolResult {
                    call_id: "call_1".to_string(),
                    name: "read_context".to_string(),
                    content: "{\"ok\":true}".to_string(),
                    is_error: false,
                    metadata: serde_json::Value::Null,
                }),
            ],
            tools: vec![FixtureTool.descriptor()],
            temperature: None,
            max_output_tokens: Some(100),
            response_format: LanguageModelResponseFormat::JsonObject,
            metadata: serde_json::Value::Null,
        });

        assert_eq!(payload["instructions"], "system prompt");
        assert_eq!(payload["tools"][0]["name"], "read_context");
        assert_eq!(payload["input"][1]["type"], "reasoning");
        assert_eq!(payload["input"][2]["type"], "function_call_output");
        assert_eq!(payload["max_output_tokens"], 100);
        assert_eq!(payload["text"]["format"]["type"], "json_object");
    }

    #[test]
    fn openai_responses_parser_extracts_tool_calls_and_reasoning_items() {
        let parsed = parse_openai_responses_codex_response(&serde_json::json!({
            "output": [
                { "type": "reasoning", "id": "rs_1", "summary": [] },
                {
                    "type": "function_call",
                    "call_id": "call_1",
                    "name": "read_context",
                    "arguments": "{\"path\":\"README.md\"}"
                },
                {
                    "type": "message",
                    "content": [{ "type": "output_text", "text": "partial" }]
                }
            ],
            "usage": { "input_tokens": 10, "output_tokens": 4 }
        }));

        assert_eq!(parsed.content, "partial");
        assert_eq!(parsed.tool_calls.len(), 1);
        assert_eq!(parsed.tool_calls[0].input["path"], "README.md");
        assert_eq!(parsed.continuation_items.len(), 2);
        assert_eq!(parsed.usage.unwrap().input_tokens, Some(10));
    }

    #[test]
    fn codex_auth_json_round_trips_official_api_key_shape() {
        let dir = std::env::temp_dir().join(format!(
            "sinomo-codex-auth-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::ZERO)
                .as_nanos()
        ));
        let credentials_path = Some(dir.join("credentials.json"));

        save_codex_api_key(&credentials_path, "sk-test").expect("save codex auth");
        let auth_path = dir.join("credentials.json");
        let auth = serde_json::from_str::<serde_json::Value>(
            &fs::read_to_string(&auth_path).expect("auth json"),
        )
        .expect("valid auth json");

        assert_eq!(auth["auth_mode"], "apikey");
        assert_eq!(auth["OPENAI_API_KEY"], "sk-test");
        assert!(auth.get("tokens").is_none());
        assert_eq!(
            read_codex_auth_json_api_key(&auth_path).expect("read auth"),
            Some("sk-test".to_string())
        );

        fs::remove_dir_all(dir).ok();
    }

    struct FixtureCodexClient {
        requests: Rc<RefCell<Vec<CodexModelRequest>>>,
        responses: RefCell<Vec<CodexModelResponse>>,
    }

    impl CodexClient for FixtureCodexClient {
        fn complete(
            &self,
            request: &CodexModelRequest,
        ) -> Result<CodexModelResponse, LanguageModelError> {
            self.requests.borrow_mut().push(request.clone());
            Ok(self.responses.borrow_mut().remove(0))
        }
    }

    struct FixtureTool;

    impl CodexTool for FixtureTool {
        fn descriptor(&self) -> CodexToolDescriptor {
            CodexToolDescriptor {
                name: "read_context".to_string(),
                description: "Read scoped context.".to_string(),
                parameters: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "path": { "type": "string" }
                    },
                    "required": ["path"]
                }),
            }
        }

        fn execute(&self, call: &CodexToolCall) -> CodexToolResult {
            CodexToolResult {
                call_id: call.call_id.clone(),
                name: call.name.clone(),
                content: "README context".to_string(),
                is_error: false,
                metadata: serde_json::Value::Null,
            }
        }
    }
}
