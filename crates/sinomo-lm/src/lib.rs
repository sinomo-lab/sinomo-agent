//! Language model provider contracts and integrations.

use reqwest::blocking::Client;
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue, USER_AGENT};
use serde::{Deserialize, Serialize};
use sinomo_local::SinomoDataDir;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GitHubCopilotConfig {
    pub client_id: String,
    pub github_host: String,
    pub api_base_url: String,
    pub default_model: String,
    pub token_store_path: Option<PathBuf>,
}

impl Default for GitHubCopilotConfig {
    fn default() -> Self {
        Self {
            client_id: GITHUB_COPILOT_CLIENT_ID.to_string(),
            github_host: "github.com".to_string(),
            api_base_url: "https://api.githubcopilot.com".to_string(),
            default_model: "gpt-5.4".to_string(),
            token_store_path: default_copilot_credentials_path(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct GitHubCopilotProvider {
    config: GitHubCopilotConfig,
    http: Client,
}

impl GitHubCopilotProvider {
    pub const PROVIDER_ID: &'static str = "github-copilot";

    pub fn new(config: GitHubCopilotConfig) -> Self {
        Self {
            config,
            http: Client::new(),
        }
    }

    pub fn with_token_store_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.config.token_store_path = Some(path.into());
        self
    }

    pub fn credentials_path(&self) -> Option<PathBuf> {
        self.config.token_store_path.clone()
    }

    pub fn device_login_start(&self) -> Result<GitHubDeviceCode, LanguageModelError> {
        let url = format!(
            "https://{}/login/device/code",
            self.config.github_host.trim_end_matches('/')
        );
        let response = self
            .http
            .post(url)
            .headers(github_oauth_headers())
            .query(&[("client_id", self.config.client_id.as_str())])
            .send()
            .map_err(|source| LanguageModelError::ProviderInvocationFailed {
                provider: Self::PROVIDER_ID.to_string(),
                model: self.config.default_model.clone(),
                detail: source.to_string(),
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(LanguageModelError::ProviderInvocationFailed {
                provider: Self::PROVIDER_ID.to_string(),
                model: self.config.default_model.clone(),
                detail: format!("device login start failed: HTTP {status}: {body}"),
            });
        }

        response.json::<GitHubDeviceCode>().map_err(|source| {
            LanguageModelError::ProviderInvocationFailed {
                provider: Self::PROVIDER_ID.to_string(),
                model: self.config.default_model.clone(),
                detail: source.to_string(),
            }
        })
    }

    pub fn device_login_poll(
        &self,
        device_code: &str,
    ) -> Result<GitHubDevicePoll, LanguageModelError> {
        let url = format!(
            "https://{}/login/oauth/access_token",
            self.config.github_host.trim_end_matches('/')
        );
        let response = self
            .http
            .post(url)
            .headers(github_oauth_headers())
            .query(&[
                ("client_id", self.config.client_id.as_str()),
                ("device_code", device_code),
                ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
            ])
            .send()
            .map_err(|source| LanguageModelError::ProviderInvocationFailed {
                provider: Self::PROVIDER_ID.to_string(),
                model: self.config.default_model.clone(),
                detail: source.to_string(),
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(LanguageModelError::ProviderInvocationFailed {
                provider: Self::PROVIDER_ID.to_string(),
                model: self.config.default_model.clone(),
                detail: format!("device login poll failed: HTTP {status}: {body}"),
            });
        }

        let payload = response.json::<GitHubDevicePoll>().map_err(|source| {
            LanguageModelError::ProviderInvocationFailed {
                provider: Self::PROVIDER_ID.to_string(),
                model: self.config.default_model.clone(),
                detail: source.to_string(),
            }
        })?;

        if let Some(token) = payload.access_token.as_ref() {
            self.save_github_token(token, "device-login")?;
        }

        Ok(payload)
    }

    pub fn github_token(&self) -> Result<String, LanguageModelError> {
        if let Some(stored) = self.load_stored_github_token()? {
            validate_github_copilot_token(&stored.github_token)?;
            return Ok(stored.github_token);
        }

        for env_var in ["COPILOT_GITHUB_TOKEN", "GH_TOKEN", "GITHUB_TOKEN"] {
            if let Ok(token) = std::env::var(env_var) {
                let token = token.trim();
                if !token.is_empty() {
                    validate_github_copilot_token(token)?;
                    return Ok(token.to_string());
                }
            }
        }

        Err(LanguageModelError::AuthenticationRequired {
            provider: Self::PROVIDER_ID.to_string(),
            detail: "no GitHub token found; set COPILOT_GITHUB_TOKEN or run the device login flow"
                .to_string(),
        })
    }

    pub fn copilot_api_token(&self) -> Result<String, LanguageModelError> {
        self.github_token()
    }

    pub fn test_connection(&self) -> Result<(), LanguageModelError> {
        let token = self.github_token()?;
        let url = format!("{}/models", self.config.api_base_url.trim_end_matches('/'));
        let response = self
            .http
            .get(url)
            .headers(copilot_api_headers(&token)?)
            .send()
            .map_err(|source| LanguageModelError::ProviderInvocationFailed {
                provider: Self::PROVIDER_ID.to_string(),
                model: self.config.default_model.clone(),
                detail: source.to_string(),
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(LanguageModelError::ProviderInvocationFailed {
                provider: Self::PROVIDER_ID.to_string(),
                model: self.config.default_model.clone(),
                detail: format!("Copilot models check failed: HTTP {status}: {body}"),
            });
        }

        Ok(())
    }

    #[allow(dead_code)]
    fn exchange_copilot_api_token(
        &self,
        github_token: &str,
    ) -> Result<CopilotApiToken, LanguageModelError> {
        let api_host = if self.config.github_host == "github.com" {
            "api.github.com".to_string()
        } else {
            format!("api.{}", self.config.github_host.trim_end_matches('/'))
        };
        let urls = [
            format!("https://{api_host}/copilot_internal/user"),
            format!("https://{api_host}/copilot_internal/v2/token"),
        ];
        let mut errors = Vec::new();
        for url in urls {
            match self.try_copilot_token_endpoint(&url, github_token) {
                Ok(token) => return Ok(token),
                Err(error) => errors.push(error),
            }
        }
        Err(LanguageModelError::ProviderInvocationFailed {
            provider: Self::PROVIDER_ID.to_string(),
            model: self.config.default_model.clone(),
            detail: format!(
                "Copilot token exchange failed across known endpoints: {}",
                errors.join("; ")
            ),
        })
    }

    #[allow(dead_code)]
    fn try_copilot_token_endpoint(
        &self,
        url: &str,
        github_token: &str,
    ) -> Result<CopilotApiToken, String> {
        let response = self
            .http
            .get(url)
            .headers(copilot_exchange_headers(github_token).map_err(|source| source.to_string())?)
            .send()
            .map_err(|source| source.to_string())?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(format!(
                "{url}: {}",
                copilot_token_exchange_error_detail(status.as_u16(), &body)
            ));
        }

        let value = response
            .json::<serde_json::Value>()
            .map_err(|source| format!("{url}: {source}"))?;
        parse_copilot_api_token(&value)
            .ok_or_else(|| format!("{url}: response did not include a Copilot API token"))
    }

    fn load_stored_github_token(&self) -> Result<Option<StoredGitHubToken>, LanguageModelError> {
        let Some(path) = &self.config.token_store_path else {
            return Ok(None);
        };
        if !path.exists() {
            return Ok(None);
        }
        let content =
            fs::read_to_string(path).map_err(|source| LanguageModelError::TokenStore {
                path: path.display().to_string(),
                detail: source.to_string(),
            })?;
        serde_json::from_str(&content)
            .map(Some)
            .map_err(|source| LanguageModelError::TokenStore {
                path: path.display().to_string(),
                detail: source.to_string(),
            })
    }

    fn save_github_token(&self, token: &str, source: &str) -> Result<(), LanguageModelError> {
        let Some(path) = &self.config.token_store_path else {
            return Ok(());
        };
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|source| LanguageModelError::TokenStore {
                path: path.display().to_string(),
                detail: source.to_string(),
            })?;
        }
        let payload = StoredGitHubToken {
            github_token: token.to_string(),
            source: source.to_string(),
            created_at_unix: unix_timestamp(),
        };
        let content = serde_json::to_string_pretty(&payload).map_err(|source| {
            LanguageModelError::RequestSerialization {
                detail: source.to_string(),
            }
        })?;
        fs::write(path, content).map_err(|source| LanguageModelError::TokenStore {
            path: path.display().to_string(),
            detail: source.to_string(),
        })
    }
}

impl Default for GitHubCopilotProvider {
    fn default() -> Self {
        Self::new(GitHubCopilotConfig::default())
    }
}

impl LanguageModelProvider for GitHubCopilotProvider {
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
        let token = self.copilot_api_token()?;
        let url = format!(
            "{}/chat/completions",
            self.config.api_base_url.trim_end_matches('/')
        );
        let response = self
            .http
            .post(url)
            .headers(copilot_api_headers(&token)?)
            .json(&chat_completion_payload(request, &model))
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
                detail: format!("Copilot chat completion failed: HTTP {status}: {body}"),
            });
        }

        let value = response.json::<serde_json::Value>().map_err(|source| {
            LanguageModelError::ProviderInvocationFailed {
                provider: Self::PROVIDER_ID.to_string(),
                model: model.clone(),
                detail: source.to_string(),
            }
        })?;
        let content = value
            .pointer("/choices/0/message/content")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("")
            .to_string();
        let usage = value.get("usage").map(|usage| LanguageModelUsage {
            input_tokens: usage
                .get("prompt_tokens")
                .and_then(serde_json::Value::as_u64),
            output_tokens: usage
                .get("completion_tokens")
                .and_then(serde_json::Value::as_u64),
        });

        Ok(LanguageModelResponse {
            provider: Self::PROVIDER_ID.to_string(),
            model,
            content,
            usage,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct GitHubDeviceCode {
    pub device_code: String,
    pub user_code: String,
    pub verification_uri: String,
    #[serde(default)]
    pub expires_in: u64,
    #[serde(default)]
    pub interval: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct GitHubDevicePoll {
    #[serde(default)]
    pub access_token: Option<String>,
    #[serde(default)]
    pub token_type: Option<String>,
    #[serde(default)]
    pub scope: Option<String>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub error_description: Option<String>,
    #[serde(default)]
    pub interval: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct StoredGitHubToken {
    github_token: String,
    source: String,
    created_at_unix: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CopilotApiToken {
    token: String,
    expires_at: u64,
}

pub const GITHUB_COPILOT_CLIENT_ID: &str = "Ov23li8tweQw6odWQebz";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GitHubCopilotCliConfig {
    pub program: String,
    pub model: String,
    pub extra_args: Vec<String>,
    pub silent: bool,
    pub no_ask_user: bool,
}

impl Default for GitHubCopilotCliConfig {
    fn default() -> Self {
        Self {
            program: "copilot".to_string(),
            model: "copilot-default".to_string(),
            extra_args: Vec::new(),
            silent: true,
            no_ask_user: true,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GitHubCopilotCliProvider {
    config: GitHubCopilotCliConfig,
}

impl GitHubCopilotCliProvider {
    pub const PROVIDER_ID: &'static str = "github-copilot-cli";

    pub fn new(config: GitHubCopilotCliConfig) -> Self {
        Self { config }
    }

    pub fn with_model(model: impl Into<String>) -> Self {
        Self::new(GitHubCopilotCliConfig {
            model: model.into(),
            ..GitHubCopilotCliConfig::default()
        })
    }

    pub fn command_args(&self, request: &LanguageModelRequest) -> Vec<String> {
        let mut args = Vec::new();
        args.push("-p".to_string());
        args.push(render_plain_prompt(request));
        if self.config.silent {
            args.push("-s".to_string());
        }
        if self.config.no_ask_user {
            args.push("--no-ask-user".to_string());
        }
        let model = selected_model(&self.config.model, request);
        if model != "copilot-default" {
            args.push("--model".to_string());
            args.push(model);
        }
        args.extend(self.config.extra_args.iter().cloned());
        args
    }
}

impl Default for GitHubCopilotCliProvider {
    fn default() -> Self {
        Self::new(GitHubCopilotCliConfig::default())
    }
}

impl LanguageModelProvider for GitHubCopilotCliProvider {
    fn info(&self) -> LanguageModelProviderInfo {
        LanguageModelProviderInfo {
            provider: Self::PROVIDER_ID.to_string(),
            default_model: self.config.model.clone(),
            invocation: LanguageModelInvocation::Process,
            supports_json_object: false,
            supports_tool_permissions: true,
        }
    }

    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        let model = selected_model(&self.config.model, request);
        let output = Command::new(&self.config.program)
            .args(self.command_args(request))
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .map_err(|source| LanguageModelError::ProviderSpawnFailed {
                provider: Self::PROVIDER_ID.to_string(),
                program: self.config.program.clone(),
                detail: source.to_string(),
            })?;

        if !output.status.success() {
            return Err(LanguageModelError::ProviderInvocationFailed {
                provider: Self::PROVIDER_ID.to_string(),
                model,
                detail: format!(
                    "exit status {:?}: {}",
                    output.status.code(),
                    String::from_utf8_lossy(&output.stderr).trim()
                ),
            });
        }

        Ok(LanguageModelResponse {
            provider: Self::PROVIDER_ID.to_string(),
            model,
            content: String::from_utf8_lossy(&output.stdout).trim().to_string(),
            usage: None,
        })
    }
}

fn selected_model(default_model: &str, request: &LanguageModelRequest) -> String {
    if request.model.trim().is_empty() || request.model == PROVIDER_DEFAULT_MODEL {
        default_model.to_string()
    } else {
        request.model.clone()
    }
}

pub const PROVIDER_DEFAULT_MODEL: &str = "provider-default";

fn github_oauth_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/x-www-form-urlencoded"),
    );
    headers.insert(
        USER_AGENT,
        HeaderValue::from_static("GitHubCopilotChat/0.26.7"),
    );
    headers
}

fn copilot_exchange_headers(github_token: &str) -> Result<HeaderMap, LanguageModelError> {
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
    headers.insert(USER_AGENT, HeaderValue::from_static("Sinomo/0.1"));
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("token {github_token}")).map_err(|source| {
            LanguageModelError::ProviderInvocationFailed {
                provider: GitHubCopilotProvider::PROVIDER_ID.to_string(),
                model: GitHubCopilotConfig::default().default_model,
                detail: source.to_string(),
            }
        })?,
    );
    Ok(headers)
}

fn copilot_api_headers(copilot_token: &str) -> Result<HeaderMap, LanguageModelError> {
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(USER_AGENT, HeaderValue::from_static("Sinomo/0.1"));
    headers.insert("Editor-Version", HeaderValue::from_static("vscode/1.104.1"));
    headers.insert(
        "Copilot-Integration-Id",
        HeaderValue::from_static("vscode-chat"),
    );
    headers.insert(
        "Openai-Intent",
        HeaderValue::from_static("conversation-edits"),
    );
    headers.insert("x-initiator", HeaderValue::from_static("user"));
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {copilot_token}")).map_err(|source| {
            LanguageModelError::ProviderInvocationFailed {
                provider: GitHubCopilotProvider::PROVIDER_ID.to_string(),
                model: GitHubCopilotConfig::default().default_model,
                detail: source.to_string(),
            }
        })?,
    );
    Ok(headers)
}

fn chat_completion_payload(request: &LanguageModelRequest, model: &str) -> serde_json::Value {
    let messages: Vec<serde_json::Value> = request
        .messages
        .iter()
        .map(|message| {
            serde_json::json!({
                "role": match message.role {
                    LanguageModelRole::System => "system",
                    LanguageModelRole::User => "user",
                    LanguageModelRole::Assistant => "assistant",
                },
                "content": message.content,
            })
        })
        .collect();
    let mut payload = serde_json::json!({
        "model": model,
        "messages": messages,
        "stream": false,
    });
    if let Some(temperature) = request.temperature {
        payload["temperature"] = serde_json::json!(temperature);
    }
    if let Some(max_tokens) = request.max_output_tokens {
        payload["max_tokens"] = serde_json::json!(max_tokens);
    }
    if request.response_format == LanguageModelResponseFormat::JsonObject {
        payload["response_format"] = serde_json::json!({ "type": "json_object" });
    }
    payload
}

fn validate_github_copilot_token(token: &str) -> Result<(), LanguageModelError> {
    if token.starts_with("ghp_") {
        return Err(LanguageModelError::UnsupportedToken {
            provider: GitHubCopilotProvider::PROVIDER_ID.to_string(),
            detail: "classic GitHub PATs (ghp_*) are not supported for Copilot API requests; use a device-login token, fine-grained PAT with Copilot Requests, or GitHub App user token".to_string(),
        });
    }
    Ok(())
}

fn copilot_token_exchange_error_detail(status: u16, body: &str) -> String {
    let base = format!("Copilot token exchange failed: HTTP {status}: {body}");
    if status == 403 && body.contains("Resource not accessible by integration") {
        format!(
            "{base}; this usually means the stored GitHub token came from a GitHub App integration \
             rather than a Copilot-compatible OAuth/device flow. Re-run `sinomo providers \
             github-copilot auth` after removing the existing credentials file, or use a \
             fine-grained PAT with Copilot Requests permission."
        )
    } else {
        base
    }
}

fn parse_copilot_api_token(value: &serde_json::Value) -> Option<CopilotApiToken> {
    let token = value
        .get("token")
        .or_else(|| value.get("access_token"))
        .or_else(|| value.pointer("/copilot/token"))
        .and_then(serde_json::Value::as_str)?;
    let expires_at = value
        .get("expires_at")
        .or_else(|| value.pointer("/copilot/expires_at"))
        .and_then(|expires| {
            expires
                .as_u64()
                .or_else(|| expires.as_str().and_then(|value| value.parse().ok()))
        })
        .or_else(|| {
            value
                .get("expires_in")
                .or_else(|| value.pointer("/copilot/expires_in"))
                .and_then(serde_json::Value::as_u64)
                .map(|expires_in| unix_timestamp() + expires_in)
        })
        .unwrap_or_else(|| unix_timestamp() + 25 * 60);

    Some(CopilotApiToken {
        token: token.to_string(),
        expires_at,
    })
}

fn default_copilot_credentials_path() -> Option<PathBuf> {
    if let Ok(path) = std::env::var("SINOMO_COPILOT_TOKEN_FILE") {
        return Some(PathBuf::from(path));
    }
    SinomoDataDir::from_env_or_home()
        .ok()
        .map(|data| data.provider_credentials_file(GitHubCopilotProvider::PROVIDER_ID))
}

fn unix_timestamp() -> u64 {
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

    #[test]
    fn direct_copilot_payload_uses_chat_completion_shape() {
        let payload = chat_completion_payload(&request(), "gpt-5.4");

        assert_eq!(payload["model"], "gpt-5.4");
        assert_eq!(payload["stream"], false);
        assert_eq!(payload["response_format"]["type"], "json_object");
        assert_eq!(payload["messages"][0]["role"], "system");
    }

    #[test]
    fn copilot_token_parser_accepts_common_shapes() {
        for value in [
            serde_json::json!({ "token": "copilot-token", "expires_at": 1234 }),
            serde_json::json!({ "access_token": "copilot-token", "expires_in": 60 }),
            serde_json::json!({ "copilot": { "token": "copilot-token", "expires_at": "1234" } }),
        ] {
            let token = parse_copilot_api_token(&value).expect("token");
            assert_eq!(token.token, "copilot-token");
        }

        assert!(parse_copilot_api_token(&serde_json::json!({ "id": 1 })).is_none());
    }

    #[test]
    fn classic_pat_is_rejected_for_direct_copilot() {
        let error = validate_github_copilot_token("ghp_legacy").expect_err("classic PAT rejected");

        assert!(matches!(error, LanguageModelError::UnsupportedToken { .. }));
    }
}
