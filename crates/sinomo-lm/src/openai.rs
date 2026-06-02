use crate::api_key::{default_api_key_credentials_path, resolve_api_key, save_api_key_credential};
use crate::{
    LanguageModelCustomToolFormat, LanguageModelCustomToolSpec, LanguageModelError,
    LanguageModelInvocation, LanguageModelMessage, LanguageModelProvider,
    LanguageModelProviderInfo, LanguageModelProviderOptions, LanguageModelProviderToolSpec,
    LanguageModelRequest, LanguageModelResponse, LanguageModelResponseFormat,
    LanguageModelResponseItem, LanguageModelRole, LanguageModelToolCall, LanguageModelToolChoice,
    LanguageModelToolSpec, LanguageModelUsage, PROVIDER_DEFAULT_MODEL,
    apply_provider_vendor_options_to_payload, render_plain_prompt, selected_model,
};
use reqwest::blocking::Client;
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue, USER_AGENT};
use std::path::PathBuf;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenAiConfig {
    pub api_base_url: String,
    pub default_model: String,
    pub api_key: Option<String>,
    pub credentials_path: Option<PathBuf>,
    pub options: LanguageModelProviderOptions,
}

impl Default for OpenAiConfig {
    fn default() -> Self {
        Self {
            api_base_url: "https://api.openai.com/v1".to_string(),
            default_model: "gpt-5.2-codex".to_string(),
            api_key: None,
            credentials_path: default_api_key_credentials_path(
                "SINOMO_OPENAI_CREDENTIALS_FILE",
                OpenAiProvider::PROVIDER_ID,
            ),
            options: LanguageModelProviderOptions::default(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct OpenAiProvider {
    config: OpenAiConfig,
    http: Client,
}

impl OpenAiProvider {
    pub const PROVIDER_ID: &'static str = "openai";

    pub fn new(config: OpenAiConfig) -> Self {
        Self {
            config,
            http: Client::new(),
        }
    }

    pub fn credentials_path(&self) -> Option<PathBuf> {
        self.config.credentials_path.clone()
    }

    pub fn api_key(&self) -> Result<String, LanguageModelError> {
        resolve_api_key(
            Self::PROVIDER_ID,
            &self.config.api_key,
            &self.config.credentials_path,
            &["SINOMO_OPENAI_API_KEY", "OPENAI_API_KEY"],
            "no OpenAI API key found; set OPENAI_API_KEY or run `sinomo providers auth openai`",
        )
    }

    pub fn save_api_key(&self, api_key: &str, source: &str) -> Result<(), LanguageModelError> {
        save_api_key_credential(&self.config.credentials_path, api_key, source)
    }

    pub fn test_connection(&self) -> Result<(), LanguageModelError> {
        let request = LanguageModelRequest {
            provider: Self::PROVIDER_ID.to_string(),
            model: PROVIDER_DEFAULT_MODEL.to_string(),
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
        self.complete(&request).map(|_| ())
    }
}

impl Default for OpenAiProvider {
    fn default() -> Self {
        Self::new(OpenAiConfig::default())
    }
}

pub(crate) fn inferred_context_window_tokens(model: &str) -> Option<usize> {
    crate::parse_declared_context_window_tokens(model).or_else(|| {
        let model = model.to_ascii_lowercase();
        if model.contains("gpt-5")
            || model.contains("gpt-4.1")
            || model.contains("o3")
            || model.contains("o4")
        {
            Some(400_000)
        } else {
            None
        }
    })
}

impl LanguageModelProvider for OpenAiProvider {
    fn info(&self) -> LanguageModelProviderInfo {
        LanguageModelProviderInfo {
            provider: Self::PROVIDER_ID.to_string(),
            default_model: self.config.default_model.clone(),
            invocation: LanguageModelInvocation::Http,
            supports_json_object: true,
            supports_tool_permissions: false,
        }
    }

    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        let model = selected_model(&self.config.default_model, request);
        let api_key = self.api_key()?;
        let url = format!(
            "{}/responses",
            self.config.api_base_url.trim_end_matches('/')
        );
        let response = self
            .http
            .post(url)
            .headers(openai_headers(&api_key, Self::PROVIDER_ID, &model)?)
            .json(&openai_responses_payload(
                request,
                &model,
                &self.config.options,
            ))
            .send()
            .map_err(|source| LanguageModelError::ProviderInvocationFailed {
                provider: Self::PROVIDER_ID.to_string(),
                model: model.clone(),
                detail: source.to_string(),
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(LanguageModelError::ProviderInvocationFailed {
                provider: Self::PROVIDER_ID.to_string(),
                model,
                detail: format!("OpenAI Responses API failed: HTTP {status}: {body}"),
            });
        }

        let value = response.json::<serde_json::Value>().map_err(|source| {
            LanguageModelError::ProviderInvocationFailed {
                provider: Self::PROVIDER_ID.to_string(),
                model: model.clone(),
                detail: source.to_string(),
            }
        })?;
        let content = extract_openai_response_text(&value);
        let tool_calls = extract_openai_tool_calls(&value);
        let items = tool_calls
            .iter()
            .cloned()
            .map(|call| LanguageModelResponseItem::ToolCall { call })
            .collect();
        let usage = value.get("usage").map(|usage| LanguageModelUsage {
            input_tokens: usage
                .get("input_tokens")
                .and_then(serde_json::Value::as_u64),
            output_tokens: usage
                .get("output_tokens")
                .and_then(serde_json::Value::as_u64),
        });

        Ok(LanguageModelResponse {
            provider: Self::PROVIDER_ID.to_string(),
            model,
            content,
            items,
            tool_calls,
            usage,
        })
    }
}

fn openai_headers(
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

fn openai_responses_payload(
    request: &LanguageModelRequest,
    model: &str,
    options: &LanguageModelProviderOptions,
) -> serde_json::Value {
    let instructions = request
        .messages
        .iter()
        .filter(|message| message.role == LanguageModelRole::System)
        .map(|message| message.content.as_str())
        .collect::<Vec<_>>()
        .join("\n\n");
    let input = render_openai_input_prompt(request);
    let mut payload = serde_json::json!({
        "model": model,
        "input": input,
        "store": false,
    });
    if !instructions.is_empty() {
        payload["instructions"] = serde_json::json!(instructions);
    }
    if let Some(temperature) = request.temperature {
        payload["temperature"] = serde_json::json!(temperature);
    }
    if let Some(max_tokens) = request.max_output_tokens {
        payload["max_output_tokens"] = serde_json::json!(max_tokens);
    }
    if request.response_format == LanguageModelResponseFormat::JsonObject {
        payload["text"] = serde_json::json!({
            "format": { "type": "json_object" }
        });
    }
    if !request.tools.is_empty() {
        payload["tools"] = serde_json::json!(
            request
                .tools
                .iter()
                .map(openai_responses_tool_spec)
                .collect::<Vec<_>>()
        );
    }
    if let Some(tool_choice) = &request.tool_choice {
        payload["tool_choice"] = openai_responses_tool_choice(tool_choice, &request.tools);
    }
    if let Some(parallel_tool_calls) = request.parallel_tool_calls {
        payload["parallel_tool_calls"] = serde_json::json!(parallel_tool_calls);
    }
    if let Some(effort) = options
        .reasoning_effort
        .and_then(|effort| effort.as_openai_responses_value())
    {
        payload["reasoning"] = serde_json::json!({ "effort": effort });
    }
    if let Some(verbosity) = options.text_verbosity {
        if !payload
            .get("text")
            .is_some_and(serde_json::Value::is_object)
        {
            payload["text"] = serde_json::json!({});
        }
        payload["text"]["verbosity"] = serde_json::json!(verbosity.as_config_value());
    }
    apply_provider_vendor_options_to_payload(&mut payload, options);
    payload
}

fn openai_responses_tool_spec(tool: &LanguageModelToolSpec) -> serde_json::Value {
    if let Some(custom) = openai_custom_tool_spec(tool) {
        return serde_json::json!({
            "type": "custom",
            "name": tool.name,
            "description": tool.description,
            "format": openai_custom_tool_format(&custom.format),
        });
    }

    serde_json::json!({
        "type": "function",
        "name": tool.name,
        "description": tool.description,
        "parameters": normalized_tool_input_schema(tool),
    })
}

fn openai_responses_tool_choice(
    tool_choice: &LanguageModelToolChoice,
    tools: &[LanguageModelToolSpec],
) -> serde_json::Value {
    match tool_choice {
        LanguageModelToolChoice::Auto => serde_json::json!("auto"),
        LanguageModelToolChoice::None => serde_json::json!("none"),
        LanguageModelToolChoice::Required => serde_json::json!("required"),
        LanguageModelToolChoice::Tool { name } => match tools
            .iter()
            .find(|tool| tool.name == *name)
            .and_then(openai_custom_tool_spec)
        {
            Some(_) => serde_json::json!({ "type": "custom", "name": name }),
            None => serde_json::json!({ "type": "function", "name": name }),
        },
    }
}

fn openai_custom_tool_spec(tool: &LanguageModelToolSpec) -> Option<&LanguageModelCustomToolSpec> {
    provider_tool_spec(tool, OpenAiProvider::PROVIDER_ID).and_then(|spec| spec.custom.as_ref())
}

fn provider_tool_spec<'a>(
    tool: &'a LanguageModelToolSpec,
    provider: &str,
) -> Option<&'a LanguageModelProviderToolSpec> {
    tool.provider_specs
        .iter()
        .find(|spec| spec.provider.eq_ignore_ascii_case(provider))
}

fn openai_custom_tool_format(format: &LanguageModelCustomToolFormat) -> serde_json::Value {
    match format {
        LanguageModelCustomToolFormat::Grammar { syntax, definition } => serde_json::json!({
            "type": "grammar",
            "syntax": syntax,
            "definition": definition,
        }),
    }
}

fn normalized_tool_input_schema(tool: &LanguageModelToolSpec) -> serde_json::Value {
    if tool.input_schema.is_null() {
        serde_json::json!({
            "type": "object",
            "properties": {},
        })
    } else {
        tool.input_schema.clone()
    }
}

fn render_openai_input_prompt(request: &LanguageModelRequest) -> String {
    let mut prompt = String::new();
    for message in request
        .messages
        .iter()
        .filter(|message| message.role != LanguageModelRole::System)
    {
        prompt.push_str(match message.role {
            LanguageModelRole::User => "user",
            LanguageModelRole::Assistant => "assistant",
            LanguageModelRole::System => "system",
        });
        prompt.push_str(":\n");
        prompt.push_str(&message.content);
        prompt.push_str("\n\n");
    }
    if prompt.is_empty() {
        render_plain_prompt(request)
    } else {
        prompt
    }
}

fn extract_openai_response_text(value: &serde_json::Value) -> String {
    if let Some(output_text) = value.get("output_text").and_then(serde_json::Value::as_str) {
        return output_text.to_string();
    }

    value
        .get("output")
        .and_then(serde_json::Value::as_array)
        .map(|items| {
            items
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
        })
        .unwrap_or_default()
}

fn extract_openai_tool_calls(value: &serde_json::Value) -> Vec<LanguageModelToolCall> {
    value
        .get("output")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(openai_output_item_tool_call)
        .collect()
}

fn openai_output_item_tool_call(item: &serde_json::Value) -> Option<LanguageModelToolCall> {
    let item_type = item.get("type").and_then(serde_json::Value::as_str)?;
    match item_type {
        "function_call" => Some(LanguageModelToolCall {
            call_id: item
                .get("call_id")
                .or_else(|| item.get("id"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_string),
            name: item.get("name")?.as_str()?.to_string(),
            input: parse_tool_arguments(item.get("arguments")),
            summary: None,
        }),
        "custom_tool_call" => Some(LanguageModelToolCall {
            call_id: item
                .get("call_id")
                .or_else(|| item.get("id"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_string),
            name: item
                .get("name")
                .or_else(|| item.get("tool_name"))?
                .as_str()?
                .to_string(),
            input: item
                .get("input")
                .cloned()
                .unwrap_or_else(|| parse_tool_arguments(item.get("arguments"))),
            summary: None,
        }),
        _ => None,
    }
}

fn parse_tool_arguments(arguments: Option<&serde_json::Value>) -> serde_json::Value {
    match arguments {
        Some(serde_json::Value::String(arguments)) => {
            serde_json::from_str(arguments).unwrap_or_else(|_| serde_json::json!(arguments))
        }
        Some(value) => value.clone(),
        None => serde_json::Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ProviderReasoningEffort, ProviderTextVerbosity};

    fn request() -> LanguageModelRequest {
        LanguageModelRequest {
            provider: "openai".to_string(),
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
    fn payload_uses_responses_shape() {
        let payload = openai_responses_payload(
            &request(),
            "gpt-5.2-codex",
            &LanguageModelProviderOptions::default(),
        );

        assert_eq!(payload["model"], "gpt-5.2-codex");
        assert_eq!(payload["store"], false);
        assert_eq!(payload["instructions"], "Return JSON only.");
        assert_eq!(payload["max_output_tokens"], 200);
        assert_eq!(payload["text"]["format"]["type"], "json_object");
        assert!(
            payload["input"]
                .as_str()
                .unwrap()
                .contains("user:\nSummarize evt_1.")
        );
    }

    #[test]
    fn payload_renders_native_tool_specs() {
        let mut request = request();
        request.tools = vec![LanguageModelToolSpec {
            name: "read_file".to_string(),
            description: "Read a file.".to_string(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": { "path": { "type": "string" } },
                "required": ["path"],
            }),
            output_schema: serde_json::Value::Null,
            supports_parallel: false,
            provider_specs: Vec::new(),
        }];
        request.tool_choice = Some(LanguageModelToolChoice::Tool {
            name: "read_file".to_string(),
        });
        request.parallel_tool_calls = Some(false);

        let payload = openai_responses_payload(
            &request,
            "gpt-5.2-codex",
            &LanguageModelProviderOptions::default(),
        );

        assert_eq!(payload["tools"][0]["type"], "function");
        assert_eq!(payload["tools"][0]["name"], "read_file");
        assert_eq!(
            payload["tools"][0]["parameters"]["properties"]["path"]["type"],
            "string"
        );
        assert_eq!(payload["tool_choice"]["name"], "read_file");
        assert_eq!(payload["parallel_tool_calls"], false);
    }

    #[test]
    fn payload_renders_openai_custom_tool_specs() {
        let mut request = request();
        request.tools = vec![LanguageModelToolSpec {
            name: "apply_patch".to_string(),
            description: "Apply a patch.".to_string(),
            input_schema: serde_json::Value::Null,
            output_schema: serde_json::Value::Null,
            supports_parallel: false,
            provider_specs: vec![LanguageModelProviderToolSpec {
                provider: OpenAiProvider::PROVIDER_ID.to_string(),
                custom: Some(LanguageModelCustomToolSpec {
                    format: LanguageModelCustomToolFormat::Grammar {
                        syntax: "lark".to_string(),
                        definition: "start: /.+/".to_string(),
                    },
                    raw_input_argument: Some("patch".to_string()),
                }),
            }],
        }];
        request.tool_choice = Some(LanguageModelToolChoice::Tool {
            name: "apply_patch".to_string(),
        });

        let payload = openai_responses_payload(
            &request,
            "gpt-5.2-codex",
            &LanguageModelProviderOptions::default(),
        );

        assert_eq!(payload["tools"][0]["type"], "custom");
        assert_eq!(payload["tools"][0]["name"], "apply_patch");
        assert_eq!(payload["tools"][0]["format"]["type"], "grammar");
        assert_eq!(payload["tools"][0]["format"]["syntax"], "lark");
        assert!(payload["tools"][0].get("parameters").is_none());
        assert_eq!(payload["tool_choice"]["type"], "custom");
        assert_eq!(payload["tool_choice"]["name"], "apply_patch");
    }

    #[test]
    fn payload_applies_provider_options() {
        let mut options = LanguageModelProviderOptions {
            reasoning_effort: Some(ProviderReasoningEffort::High),
            text_verbosity: Some(ProviderTextVerbosity::Low),
            ..LanguageModelProviderOptions::default()
        };
        options
            .vendor_options
            .insert("reasoning.summary".to_string(), "auto".to_string());

        let payload = openai_responses_payload(&request(), "gpt-5.2-codex", &options);

        assert_eq!(payload["reasoning"]["effort"], "high");
        assert_eq!(payload["reasoning"]["summary"], "auto");
        assert_eq!(payload["text"]["verbosity"], "low");
    }

    #[test]
    fn response_parser_extracts_native_tool_calls() {
        let value = serde_json::json!({
            "output": [
                {
                    "type": "function_call",
                    "call_id": "call_1",
                    "name": "read_file",
                    "arguments": "{\"path\":\"Cargo.toml\"}"
                }
            ]
        });

        let calls = extract_openai_tool_calls(&value);

        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].call_id.as_deref(), Some("call_1"));
        assert_eq!(calls[0].name, "read_file");
        assert_eq!(calls[0].input["path"], "Cargo.toml");
    }

    #[test]
    fn response_parser_extracts_custom_tool_calls() {
        let value = serde_json::json!({
            "output": [
                {
                    "type": "custom_tool_call",
                    "call_id": "call_patch",
                    "name": "apply_patch",
                    "input": "*** Begin Patch\n*** End Patch\n"
                }
            ]
        });

        let calls = extract_openai_tool_calls(&value);

        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].call_id.as_deref(), Some("call_patch"));
        assert_eq!(calls[0].name, "apply_patch");
        assert_eq!(calls[0].input, "*** Begin Patch\n*** End Patch\n");
    }

    #[test]
    fn provider_reports_http_capabilities() {
        let info = OpenAiProvider::default().info();

        assert_eq!(info.provider, OpenAiProvider::PROVIDER_ID);
        assert_eq!(info.invocation, LanguageModelInvocation::Http);
        assert!(info.supports_json_object);
    }
}
