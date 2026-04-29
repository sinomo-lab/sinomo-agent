//! Language model provider contracts and integrations.

mod anthropic;
mod api_key;
mod claude_code;
mod codex_cli;
mod gemini;
mod github_copilot;
mod openai;

pub use anthropic::{AnthropicConfig, AnthropicProvider};
pub use claude_code::{
    CLAUDE_CODE_DEFAULT_MODEL, ClaudeCodeConfig, ClaudeCodeProvider, ClaudePermissionMode,
};
pub use codex_cli::{
    CODEX_CLI_DEFAULT_MODEL, CodexCliConfig, CodexCliProvider, CodexColor, CodexSandboxMode,
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
pub use openai::{OpenAiConfig, OpenAiProvider};

use serde::{Deserialize, Serialize};
use std::io::Write;
use std::process::{Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LanguageModelRequest {
    pub provider: String,
    pub model: String,
    pub messages: Vec<LanguageModelMessage>,
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

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct LanguageModelResponse {
    pub provider: String,
    pub model: String,
    pub content: String,
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
            temperature: Some(0.2),
            max_output_tokens: Some(200),
            response_format: LanguageModelResponseFormat::JsonObject,
            metadata: serde_json::json!({ "job_id": "task_test" }),
        }
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
    fn direct_copilot_provider_reports_http_capabilities() {
        let info = GitHubCopilotProvider::default().info();

        assert_eq!(info.provider, GitHubCopilotProvider::PROVIDER_ID);
        assert_eq!(info.invocation, LanguageModelInvocation::Http);
        assert!(info.supports_json_object);
        assert!(!info.supports_tool_permissions);
    }
}
