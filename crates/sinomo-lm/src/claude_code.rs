use crate::{
    LanguageModelError, LanguageModelInvocation, LanguageModelProvider, LanguageModelProviderInfo,
    LanguageModelRequest, LanguageModelResponse, ProviderReasoningEffort, render_plain_prompt,
    selected_model,
};
use std::path::PathBuf;
use std::process::{Command, Stdio};

pub const CLAUDE_CODE_DEFAULT_MODEL: &str = "claude-code-default";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClaudeCodeConfig {
    pub program: String,
    pub model: String,
    pub extra_args: Vec<String>,
    pub permission_mode: ClaudePermissionMode,
    pub max_turns: Option<u32>,
    pub no_session_persistence: bool,
    pub working_dir: Option<PathBuf>,
    pub reasoning_effort: Option<ProviderReasoningEffort>,
}

impl Default for ClaudeCodeConfig {
    fn default() -> Self {
        Self {
            program: "claude".to_string(),
            model: CLAUDE_CODE_DEFAULT_MODEL.to_string(),
            extra_args: Vec::new(),
            permission_mode: ClaudePermissionMode::Auto,
            max_turns: Some(10),
            no_session_persistence: true,
            working_dir: None,
            reasoning_effort: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClaudePermissionMode {
    Default,
    AcceptEdits,
    Plan,
    Auto,
    DontAsk,
    BypassPermissions,
}

impl ClaudePermissionMode {
    fn as_arg(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::AcceptEdits => "acceptEdits",
            Self::Plan => "plan",
            Self::Auto => "auto",
            Self::DontAsk => "dontAsk",
            Self::BypassPermissions => "bypassPermissions",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClaudeCodeProvider {
    config: ClaudeCodeConfig,
}

impl ClaudeCodeProvider {
    pub const PROVIDER_ID: &'static str = "claude-code";

    pub fn new(config: ClaudeCodeConfig) -> Self {
        Self { config }
    }

    pub fn with_model(model: impl Into<String>) -> Self {
        Self::new(ClaudeCodeConfig {
            model: model.into(),
            ..ClaudeCodeConfig::default()
        })
    }

    pub fn command_args(&self, request: &LanguageModelRequest) -> Vec<String> {
        let mut args = vec![
            "-p".to_string(),
            render_plain_prompt(request),
            "--output-format".to_string(),
            "text".to_string(),
            "--permission-mode".to_string(),
            self.config.permission_mode.as_arg().to_string(),
        ];

        if let Some(max_turns) = self.config.max_turns {
            args.push("--max-turns".to_string());
            args.push(max_turns.to_string());
        }
        if self.config.no_session_persistence {
            args.push("--no-session-persistence".to_string());
        }
        if let Some(effort) = self
            .config
            .reasoning_effort
            .and_then(|effort| effort.as_claude_code_value())
        {
            args.push("--effort".to_string());
            args.push(effort.to_string());
        }

        let model = selected_model(&self.config.model, request);
        if model != CLAUDE_CODE_DEFAULT_MODEL {
            args.push("--model".to_string());
            args.push(model);
        }

        args.extend(self.config.extra_args.iter().cloned());
        args
    }
}

impl Default for ClaudeCodeProvider {
    fn default() -> Self {
        Self::new(ClaudeCodeConfig::default())
    }
}

pub(crate) fn inferred_context_window_tokens(model: &str) -> Option<usize> {
    crate::anthropic::inferred_context_window_tokens(model)
}

impl LanguageModelProvider for ClaudeCodeProvider {
    fn info(&self) -> LanguageModelProviderInfo {
        LanguageModelProviderInfo {
            provider: Self::PROVIDER_ID.to_string(),
            default_model: self.config.model.clone(),
            invocation: LanguageModelInvocation::Process,
            host_access: crate::LanguageModelHostAccess::Unrestricted,
            supports_json_object: false,
            supports_tool_permissions: true,
        }
    }

    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        let model = selected_model(&self.config.model, request);
        let mut command = Command::new(&self.config.program);
        command
            .args(self.command_args(request))
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(working_dir) = &self.config.working_dir {
            command.current_dir(working_dir);
        }

        let output =
            command
                .output()
                .map_err(|source| LanguageModelError::ProviderSpawnFailed {
                    provider: Self::PROVIDER_ID.to_string(),
                    program: self.config.program.clone(),
                    detail: source.to_string(),
                })?;

        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        if !output.status.success() {
            let mut detail = format!("exit status {:?}: {}", output.status.code(), stderr);
            if !stdout.is_empty() {
                detail.push_str("; stdout: ");
                detail.push_str(&stdout);
            }
            return Err(LanguageModelError::ProviderInvocationFailed {
                provider: Self::PROVIDER_ID.to_string(),
                model,
                detail,
            });
        }

        Ok(LanguageModelResponse {
            provider: Self::PROVIDER_ID.to_string(),
            model,
            content: stdout,
            items: Vec::new(),
            tool_calls: Vec::new(),
            usage: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{LanguageModelMessage, LanguageModelResponseFormat, LanguageModelRole};
    use std::fs;
    use std::io::Write;

    fn request() -> LanguageModelRequest {
        LanguageModelRequest {
            provider: ClaudeCodeProvider::PROVIDER_ID.to_string(),
            model: CLAUDE_CODE_DEFAULT_MODEL.to_string(),
            messages: vec![
                LanguageModelMessage {
                    role: LanguageModelRole::System,
                    content: "Return a concise answer.".to_string(),
                },
                LanguageModelMessage {
                    role: LanguageModelRole::User,
                    content: "Say hello.".to_string(),
                },
            ],
            items: Vec::new(),
            tools: Vec::new(),
            tool_choice: None,
            parallel_tool_calls: None,
            temperature: None,
            max_output_tokens: Some(32),
            response_format: LanguageModelResponseFormat::Text,
            metadata: serde_json::Value::Null,
        }
    }

    #[cfg(unix)]
    #[test]
    fn complete_uses_claude_code_print_mode_process() {
        use std::os::unix::fs::PermissionsExt;

        let dir = sinomo_testdir::TestDir::new("sinomo-claude-code-test");
        let script = dir.join("fake-claude");
        {
            let mut file = fs::File::create(&script).expect("script create");
            file.write_all(
                b"#!/bin/sh\nif [ \"$1\" != \"-p\" ]; then echo 'missing print mode' >&2; exit 2; fi\nprintf 'claude-code:%s' \"$2\"\n",
            )
            .expect("script write");
            file.sync_all().expect("script sync");
        }
        let mut perms = fs::metadata(&script).expect("metadata").permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&script, perms).expect("chmod");

        let provider = ClaudeCodeProvider::new(ClaudeCodeConfig {
            program: script.display().to_string(),
            ..ClaudeCodeConfig::default()
        });

        let response = provider.complete(&request()).expect("fake claude succeeds");

        assert_eq!(response.provider, ClaudeCodeProvider::PROVIDER_ID);
        assert!(
            response
                .content
                .starts_with("claude-code:system:\nReturn a concise answer.")
        );
        assert!(response.content.contains("user:\nSay hello."));
    }

    #[test]
    fn command_args_include_reasoning_effort() {
        let provider = ClaudeCodeProvider::new(ClaudeCodeConfig {
            reasoning_effort: Some(ProviderReasoningEffort::Max),
            ..ClaudeCodeConfig::default()
        });

        let args = provider.command_args(&request());

        assert!(args.windows(2).any(|pair| pair == ["--effort", "max"]));
    }
}
