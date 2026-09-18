use crate::{
    LanguageModelError, LanguageModelInvocation, LanguageModelProvider, LanguageModelProviderInfo,
    LanguageModelRequest, LanguageModelResponse, ProviderReasoningEffort, render_plain_prompt,
    selected_model,
};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub use sinomo_runtime::api::lm::CODEX_CLI_DEFAULT_MODEL;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CodexCliConfig {
    pub program: String,
    pub model: String,
    pub extra_args: Vec<String>,
    pub sandbox: CodexSandboxMode,
    pub full_auto: bool,
    pub dangerously_bypass_approvals_and_sandbox: bool,
    pub skip_git_repo_check: bool,
    pub ephemeral: bool,
    pub color: CodexColor,
    pub working_dir: Option<PathBuf>,
    pub capture_last_message: bool,
    pub reasoning_effort: Option<ProviderReasoningEffort>,
}

impl Default for CodexCliConfig {
    fn default() -> Self {
        Self {
            program: "codex".to_string(),
            model: CODEX_CLI_DEFAULT_MODEL.to_string(),
            extra_args: Vec::new(),
            sandbox: CodexSandboxMode::WorkspaceWrite,
            full_auto: true,
            dangerously_bypass_approvals_and_sandbox: false,
            skip_git_repo_check: false,
            ephemeral: true,
            color: CodexColor::Never,
            working_dir: None,
            capture_last_message: true,
            reasoning_effort: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CodexSandboxMode {
    ReadOnly,
    WorkspaceWrite,
    DangerFullAccess,
}

impl CodexSandboxMode {
    fn as_arg(self) -> &'static str {
        match self {
            Self::ReadOnly => "read-only",
            Self::WorkspaceWrite => "workspace-write",
            Self::DangerFullAccess => "danger-full-access",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CodexColor {
    Always,
    Never,
    Auto,
}

impl CodexColor {
    fn as_arg(self) -> &'static str {
        match self {
            Self::Always => "always",
            Self::Never => "never",
            Self::Auto => "auto",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CodexCliProvider {
    config: CodexCliConfig,
}

impl CodexCliProvider {
    pub const PROVIDER_ID: &'static str = sinomo_runtime::api::lm::CODEX_CLI_PROVIDER_ID;

    pub fn new(config: CodexCliConfig) -> Self {
        Self { config }
    }

    pub fn with_model(model: impl Into<String>) -> Self {
        Self::new(CodexCliConfig {
            model: model.into(),
            ..CodexCliConfig::default()
        })
    }

    fn host_access(&self) -> crate::LanguageModelHostAccess {
        // Even the default program is resolved through the inherited PATH, so
        // its basename is not proof that the launched executable enforces the
        // generated sandbox flags. Fail closed until the executable itself can
        // be pinned and authenticated.
        crate::LanguageModelHostAccess::Unrestricted
    }

    pub fn command_args(
        &self,
        request: &LanguageModelRequest,
        output_last_message_path: Option<&Path>,
    ) -> Vec<String> {
        let mut args = vec!["exec".to_string()];

        if self.config.dangerously_bypass_approvals_and_sandbox {
            args.push("--dangerously-bypass-approvals-and-sandbox".to_string());
        } else if self.config.full_auto {
            args.push("--full-auto".to_string());
        } else {
            args.push("--sandbox".to_string());
            args.push(self.config.sandbox.as_arg().to_string());
        }

        if self.config.skip_git_repo_check {
            args.push("--skip-git-repo-check".to_string());
        }
        if self.config.ephemeral {
            args.push("--ephemeral".to_string());
        }

        args.push("--color".to_string());
        args.push(self.config.color.as_arg().to_string());

        if let Some(path) = output_last_message_path {
            args.push("-o".to_string());
            args.push(path.display().to_string());
        }

        let model = selected_model(&self.config.model, request);
        if model != CODEX_CLI_DEFAULT_MODEL {
            args.push("--model".to_string());
            args.push(model);
        }
        if let Some(effort) = self
            .config
            .reasoning_effort
            .and_then(|effort| effort.as_codex_cli_value())
        {
            args.push("-c".to_string());
            args.push(format!("model_reasoning_effort=\"{effort}\""));
        }

        args.extend(self.config.extra_args.iter().cloned());
        args.push("-".to_string());
        args
    }

    fn output_last_message_path(&self) -> Option<PathBuf> {
        if !self.config.capture_last_message {
            return None;
        }
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_nanos();
        let pid = std::process::id();
        Some(std::env::temp_dir().join(format!("sinomo-codex-last-{pid}-{stamp}.txt")))
    }
}

impl Default for CodexCliProvider {
    fn default() -> Self {
        Self::new(CodexCliConfig::default())
    }
}

pub(crate) fn inferred_context_window_tokens(model: &str) -> Option<usize> {
    crate::codex::inferred_context_window_tokens(model)
}

impl LanguageModelProvider for CodexCliProvider {
    fn info(&self) -> LanguageModelProviderInfo {
        LanguageModelProviderInfo {
            provider: Self::PROVIDER_ID.to_string(),
            default_model: self.config.model.clone(),
            invocation: LanguageModelInvocation::Process,
            host_access: self.host_access(),
            capabilities: crate::LanguageModelProviderCapabilities::default(),
            supports_json_object: false,
            supports_tool_permissions: true,
        }
    }

    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        let model = selected_model(&self.config.model, request);
        let prompt = render_plain_prompt(request);
        let output_path = self.output_last_message_path();
        let mut command = Command::new(&self.config.program);
        command
            .args(self.command_args(request, output_path.as_deref()))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(working_dir) = &self.config.working_dir {
            command.current_dir(working_dir);
        }

        let mut child =
            command
                .spawn()
                .map_err(|source| LanguageModelError::ProviderSpawnFailed {
                    provider: Self::PROVIDER_ID.to_string(),
                    program: self.config.program.clone(),
                    detail: source.to_string(),
                })?;

        let mut stdin =
            child
                .stdin
                .take()
                .ok_or_else(|| LanguageModelError::ProviderInvocationFailed {
                    provider: Self::PROVIDER_ID.to_string(),
                    model: model.clone(),
                    detail: "codex command stdin was unavailable".to_string(),
                })?;
        stdin.write_all(prompt.as_bytes()).map_err(|source| {
            LanguageModelError::ProviderInvocationFailed {
                provider: Self::PROVIDER_ID.to_string(),
                model: model.clone(),
                detail: source.to_string(),
            }
        })?;
        drop(stdin);

        let output = child.wait_with_output().map_err(|source| {
            LanguageModelError::ProviderInvocationFailed {
                provider: Self::PROVIDER_ID.to_string(),
                model: model.clone(),
                detail: source.to_string(),
            }
        })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
            let mut detail = format!("exit status {:?}: {}", output.status.code(), stderr);
            if !stdout.is_empty() {
                detail.push_str("; stdout: ");
                detail.push_str(&stdout);
            }
            cleanup_output_path(output_path.as_deref());
            return Err(LanguageModelError::ProviderInvocationFailed {
                provider: Self::PROVIDER_ID.to_string(),
                model,
                detail,
            });
        }

        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let content = output_path
            .as_deref()
            .and_then(|path| fs::read_to_string(path).ok())
            .map(|content| content.trim().to_string())
            .filter(|content| !content.is_empty())
            .unwrap_or(stdout);
        cleanup_output_path(output_path.as_deref());

        Ok(LanguageModelResponse {
            provider: Self::PROVIDER_ID.to_string(),
            model,
            content,
            items: Vec::new(),
            tool_calls: Vec::new(),
            usage: None,
        })
    }
}

fn cleanup_output_path(path: Option<&Path>) {
    if let Some(path) = path {
        let _ = fs::remove_file(path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{LanguageModelMessage, LanguageModelResponseFormat, LanguageModelRole};

    fn request() -> LanguageModelRequest {
        LanguageModelRequest {
            provider: CodexCliProvider::PROVIDER_ID.to_string(),
            model: CODEX_CLI_DEFAULT_MODEL.to_string(),
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
    fn complete_uses_codex_exec_process_and_reads_last_message_file() {
        let dir = sinomo_testdir::TestDir::new("sinomo-codex-test");
        let script = dir.join("exec");
        {
            let mut file = fs::File::create(&script).expect("script create");
            file.write_all(
                b"out=\"\"\nwhile [ $# -gt 0 ]; do\n  if [ \"$1\" = \"-o\" ]; then shift; out=\"$1\"; fi\n  shift\ndone\ninput=$(cat)\nprintf 'last-message:%s' \"$input\" > \"$out\"\nprintf 'stdout fallback'\n",
            )
            .expect("script write");
            file.sync_all().expect("script sync");
        }

        let provider = CodexCliProvider::new(CodexCliConfig {
            program: "/bin/sh".to_string(),
            skip_git_repo_check: true,
            working_dir: Some(dir.to_path_buf()),
            ..CodexCliConfig::default()
        });

        let response = provider.complete(&request()).expect("fake codex succeeds");

        assert_eq!(response.provider, CodexCliProvider::PROVIDER_ID);
        assert!(
            response
                .content
                .starts_with("last-message:system:\nReturn a concise answer.")
        );
        assert!(response.content.contains("user:\nSay hello."));
    }

    #[test]
    fn command_args_include_reasoning_effort_config() {
        for effort in [
            ProviderReasoningEffort::XHigh,
            ProviderReasoningEffort::Max,
            ProviderReasoningEffort::Ultra,
        ] {
            let provider = CodexCliProvider::new(CodexCliConfig {
                reasoning_effort: Some(effort),
                ..CodexCliConfig::default()
            });
            let args = provider.command_args(&request(), None);
            let setting = format!("model_reasoning_effort=\"{effort}\"");
            assert!(args.windows(2).any(|pair| pair == ["-c", setting.as_str()]));
        }
    }

    #[test]
    fn host_access_fails_closed_for_every_path_resolved_codex_process() {
        assert_eq!(
            CodexCliProvider::default().info().host_access,
            crate::LanguageModelHostAccess::Unrestricted
        );
        assert_eq!(
            CodexCliProvider::new(CodexCliConfig {
                full_auto: false,
                sandbox: CodexSandboxMode::ReadOnly,
                ..CodexCliConfig::default()
            })
            .info()
            .host_access,
            crate::LanguageModelHostAccess::Unrestricted
        );

        for config in [
            CodexCliConfig {
                dangerously_bypass_approvals_and_sandbox: true,
                ..CodexCliConfig::default()
            },
            CodexCliConfig {
                full_auto: false,
                sandbox: CodexSandboxMode::DangerFullAccess,
                ..CodexCliConfig::default()
            },
            CodexCliConfig {
                extra_args: vec!["--sandbox".to_string(), "danger-full-access".to_string()],
                ..CodexCliConfig::default()
            },
            CodexCliConfig {
                program: "/tmp/custom-codex-wrapper".to_string(),
                ..CodexCliConfig::default()
            },
        ] {
            assert_eq!(
                CodexCliProvider::new(config).info().host_access,
                crate::LanguageModelHostAccess::Unrestricted
            );
        }
    }
}
