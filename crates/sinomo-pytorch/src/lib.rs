//! PyTorch/Transformers bridge for Sinomo's local model runtimes.
//!
//! The Rust side intentionally owns the stable request/response contract and
//! process lifecycle. The Python side owns model loading because local model
//! families such as MiniCPM-o depend on PyTorch/Transformers runtime packages.

pub fn register_storage_paths(
    layout: &mut sinomo_storage::StorageLayout,
) -> Result<(), sinomo_storage::StorageContractError> {
    use sinomo_storage::StorageRing;

    layout.register_escape_path(
        "sinomo-pytorch",
        StorageRing::R2,
        "hf-cache",
        "Hugging Face model cache used by local PyTorch runtimes",
    )?;
    layout.register_escape_path(
        "sinomo-pytorch",
        StorageRing::R2,
        "minicpm-venv",
        "local Python virtual environment for PyTorch model runtimes",
    )?;
    layout.register_escape_path(
        "sinomo-pytorch",
        StorageRing::R2,
        "minicpm-v46-packages",
        "local Python dependency cache used by model runtime experiments",
    )?;
    layout.register_escape_path(
        "sinomo-pytorch",
        StorageRing::R2,
        "minicpm-v46-packages-main",
        "legacy local Python dependency cache used by MiniCPM-V runtime experiments",
    )?;
    layout.register_escape_path(
        "sinomo-pytorch",
        StorageRing::R2,
        "minicpm-v46-venv",
        "legacy local Python virtual environment used by MiniCPM-V runtime experiments",
    )?;
    layout.register_escape_path(
        "sinomo-pytorch",
        StorageRing::R2,
        "pytorch_server",
        "legacy PyTorch server staging directory",
    )?;
    layout.register_escape_path(
        "sinomo-pytorch",
        StorageRing::R2,
        "logs/pytorch",
        "managed PyTorch runtime process logs and supervisor token file",
    )?;
    for relative_path in [
        "minicpm-mock.err.log",
        "minicpm-mock.log",
        "minicpm-server.err.log",
        "minicpm-server.log",
        "minicpm-server.out.log",
    ] {
        layout.register_human_text("sinomo-pytorch", StorageRing::R2, relative_path)?;
    }
    Ok(())
}

use serde::{Deserialize, Serialize};
use sinomo_lm::{
    LanguageModelError, LanguageModelMessage, LanguageModelProvider, LanguageModelProviderInfo,
    LanguageModelRequest, LanguageModelResponse, LanguageModelUsage,
};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, ErrorKind, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime};
use thiserror::Error;

pub const MINICPM_PROVIDER_ID: &str = "minicpm-o";
pub const MINICPM_DEFAULT_MODEL: &str = "openbmb/MiniCPM-o-4_5";
pub const PYTORCH_DEFAULT_PROVIDER_ID: &str = MINICPM_PROVIDER_ID;
pub const PYTORCH_DEFAULT_MODEL: &str = MINICPM_DEFAULT_MODEL;
pub const PYTORCH_DEFAULT_LOCAL_SERVER_PORT: u16 = 32153;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct PytorchRuntimeConfig {
    pub python: PathBuf,
    pub bridge_script: PathBuf,
    pub model: String,
    pub device: PytorchDevice,
    pub timeout_seconds: u64,
    #[serde(default)]
    pub mock: bool,
}

impl Default for PytorchRuntimeConfig {
    fn default() -> Self {
        Self {
            python: PathBuf::from("python3"),
            bridge_script: default_bridge_script(),
            model: PYTORCH_DEFAULT_MODEL.to_string(),
            device: PytorchDevice::Auto,
            timeout_seconds: 300,
            mock: false,
        }
    }
}

impl PytorchRuntimeConfig {
    pub fn with_bridge_script(mut self, bridge_script: impl Into<PathBuf>) -> Self {
        self.bridge_script = bridge_script.into();
        self
    }

    pub fn with_mock(mut self, mock: bool) -> Self {
        self.mock = mock;
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum PytorchDevice {
    Auto,
    Cpu,
    Cuda,
}

impl PytorchDevice {
    fn as_arg(&self) -> &'static str {
        match self {
            PytorchDevice::Auto => "auto",
            PytorchDevice::Cpu => "cpu",
            PytorchDevice::Cuda => "cuda",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PytorchRequest {
    pub mode: PytorchMode,
    pub system_prompt: String,
    pub messages: Vec<LanguageModelMessage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image_path: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image_base64: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub audio_path: Option<PathBuf>,
    #[serde(default)]
    pub stream: bool,
    #[serde(default)]
    pub metadata: serde_json::Value,
}

impl PytorchRequest {
    pub fn from_language_model_request(request: &LanguageModelRequest) -> Self {
        let mut system_prompt = String::new();
        let mut messages = Vec::new();
        for message in &request.messages {
            if matches!(message.role, sinomo_lm::LanguageModelRole::System) {
                if !system_prompt.is_empty() {
                    system_prompt.push_str("\n\n");
                }
                system_prompt.push_str(&message.content);
            } else {
                messages.push(message.clone());
            }
        }
        Self {
            mode: PytorchMode::Text,
            system_prompt,
            messages,
            image_path: None,
            image_base64: None,
            image_name: None,
            audio_path: None,
            stream: false,
            metadata: request.metadata.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum PytorchMode {
    Text,
    Vision,
    Audio,
    Duplex,
    Probe,
}

impl PytorchMode {
    fn as_arg(&self) -> &'static str {
        match self {
            PytorchMode::Text => "text",
            PytorchMode::Vision => "vision",
            PytorchMode::Audio => "audio",
            PytorchMode::Duplex => "duplex",
            PytorchMode::Probe => "probe",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PytorchResponse {
    pub content: String,
    pub runtime: PytorchRuntimeStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<LanguageModelUsage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub elapsed_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_tokens_per_second: Option<f64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct PytorchRuntimeStatus {
    pub model: String,
    pub device: PytorchDevice,
    pub backend: String,
    pub local: bool,
    pub streaming_supported: bool,
    pub vision_supported: bool,
    pub audio_supported: bool,
    pub tts_supported: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostic: Option<String>,
}

#[derive(Clone, Debug)]
pub struct PythonPytorchRuntime {
    config: PytorchRuntimeConfig,
}

impl PythonPytorchRuntime {
    pub fn new(config: PytorchRuntimeConfig) -> Self {
        Self { config }
    }

    pub fn config(&self) -> &PytorchRuntimeConfig {
        &self.config
    }

    pub fn probe(&self) -> Result<PytorchRuntimeStatus, PytorchBridgeError> {
        let response = self.complete(PytorchRequest {
            mode: PytorchMode::Probe,
            system_prompt: String::new(),
            messages: Vec::new(),
            image_path: None,
            image_base64: None,
            image_name: None,
            audio_path: None,
            stream: false,
            metadata: serde_json::Value::Null,
        })?;
        Ok(response.runtime)
    }

    pub fn complete(&self, request: PytorchRequest) -> Result<PytorchResponse, PytorchBridgeError> {
        ensure_bridge_script(&self.config.bridge_script)?;
        let payload = serde_json::to_vec(&request)?;
        let mut command = Command::new(&self.config.python);
        command
            .arg(&self.config.bridge_script)
            .arg("--model")
            .arg(&self.config.model)
            .arg("--device")
            .arg(self.config.device.as_arg())
            .arg("--timeout-seconds")
            .arg(self.config.timeout_seconds.to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if self.config.mock {
            command.env("SINOMO_PYTORCH_MOCK", "1");
            command.env("SINOMO_MINICPM_MOCK", "1");
        }
        let mut child = command
            .spawn()
            .map_err(|source| PytorchBridgeError::Process {
                detail: format!("failed to start PyTorch model bridge: {source}"),
            })?;
        child
            .stdin
            .as_mut()
            .ok_or_else(|| PytorchBridgeError::Process {
                detail: "PyTorch model bridge stdin was unavailable".to_string(),
            })?
            .write_all(&payload)
            .map_err(|source| PytorchBridgeError::Process {
                detail: format!("failed to write PyTorch model request: {source}"),
            })?;
        let output = child
            .wait_with_output()
            .map_err(|source| PytorchBridgeError::Process {
                detail: format!("failed to wait for PyTorch model bridge: {source}"),
            })?;
        if !output.status.success() {
            return Err(PytorchBridgeError::Process {
                detail: String::from_utf8_lossy(&output.stderr).trim().to_string(),
            });
        }
        Ok(serde_json::from_slice(&output.stdout)?)
    }
}

impl LanguageModelProvider for PythonPytorchRuntime {
    fn info(&self) -> LanguageModelProviderInfo {
        LanguageModelProviderInfo {
            provider: MINICPM_PROVIDER_ID.to_string(),
            default_model: self.config.model.clone(),
            invocation: sinomo_lm::LanguageModelInvocation::Process,
            supports_json_object: false,
            supports_tool_permissions: false,
        }
    }

    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        let response = self
            .complete(PytorchRequest::from_language_model_request(request))
            .map_err(|source| LanguageModelError::ProviderInvocationFailed {
                provider: MINICPM_PROVIDER_ID.to_string(),
                model: self.config.model.clone(),
                detail: source.to_string(),
            })?;
        Ok(LanguageModelResponse {
            provider: MINICPM_PROVIDER_ID.to_string(),
            model: self.config.model.clone(),
            content: response.content,
            usage: response.usage,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RemotePytorchRuntimeConfig {
    pub endpoint: String,
    pub timeout_seconds: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_token: Option<String>,
}

impl Default for RemotePytorchRuntimeConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://127.0.0.1:32153".to_string(),
            timeout_seconds: 300,
            auth_token: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct RemotePytorchRuntime {
    config: RemotePytorchRuntimeConfig,
}

impl RemotePytorchRuntime {
    pub fn new(config: RemotePytorchRuntimeConfig) -> Self {
        Self { config }
    }

    pub fn endpoint(&self) -> &str {
        &self.config.endpoint
    }

    pub fn complete(&self, request: PytorchRequest) -> Result<PytorchResponse, PytorchBridgeError> {
        let body = serde_json::to_vec(&request)?;
        let response = http_post_json(
            &self.config.endpoint,
            "/complete",
            &body,
            Duration::from_secs(self.config.timeout_seconds),
            self.config.auth_token.as_deref(),
        )?;
        Ok(serde_json::from_slice(&response)?)
    }

    pub fn complete_streaming(
        &self,
        mut request: PytorchRequest,
        on_delta: &mut dyn FnMut(&str),
    ) -> Result<PytorchResponse, PytorchBridgeError> {
        request.stream = true;
        let body = serde_json::to_vec(&request)?;
        http_post_json_event_stream(
            &self.config.endpoint,
            "/complete",
            &body,
            Duration::from_secs(self.config.timeout_seconds),
            self.config.auth_token.as_deref(),
            on_delta,
        )
    }

    pub fn probe(&self) -> Result<PytorchRuntimeStatus, PytorchBridgeError> {
        let response = http_get(
            &self.config.endpoint,
            "/health",
            Duration::from_secs(self.config.timeout_seconds),
            self.config.auth_token.as_deref(),
        )?;
        Ok(serde_json::from_slice(&response)?)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ManagedPytorchServerConfig {
    pub python: PathBuf,
    pub server_script: PathBuf,
    pub model: String,
    pub device: PytorchDevice,
    pub mode: PytorchMode,
    pub port: u16,
    pub timeout_seconds: u64,
    pub log_dir: PathBuf,
    #[serde(default)]
    pub mock: bool,
}

impl Default for ManagedPytorchServerConfig {
    fn default() -> Self {
        Self {
            python: PathBuf::from("python3"),
            server_script: default_server_script(),
            model: PYTORCH_DEFAULT_MODEL.to_string(),
            device: PytorchDevice::Auto,
            mode: PytorchMode::Vision,
            port: PYTORCH_DEFAULT_LOCAL_SERVER_PORT,
            timeout_seconds: 300,
            log_dir: PathBuf::from(".sinomo-pytorch-logs"),
            mock: false,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ManagedPytorchServerStatus {
    pub endpoint: String,
    pub pid: Option<u32>,
    pub ready: bool,
    pub restart_count: u64,
    pub stdout_log: PathBuf,
    pub stderr_log: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

#[derive(Debug)]
pub struct ManagedPytorchServer {
    config: ManagedPytorchServerConfig,
    child: Option<Child>,
    restart_count: u64,
    last_error: Option<String>,
    auth_token: String,
}

impl ManagedPytorchServer {
    pub fn new(config: ManagedPytorchServerConfig) -> Self {
        Self {
            config,
            child: None,
            restart_count: 0,
            last_error: None,
            auth_token: generate_auth_token(),
        }
    }

    pub fn endpoint(&self) -> String {
        format!("http://127.0.0.1:{}", self.config.port)
    }

    pub fn status(&mut self) -> ManagedPytorchServerStatus {
        let pid = self.child.as_ref().map(Child::id);
        let ready = self.probe_loopback().is_ok();
        ManagedPytorchServerStatus {
            endpoint: self.endpoint(),
            pid,
            ready,
            restart_count: self.restart_count,
            stdout_log: self.stdout_log_path(),
            stderr_log: self.stderr_log_path(),
            last_error: self.last_error.clone(),
        }
    }

    pub fn complete(
        &mut self,
        request: PytorchRequest,
    ) -> Result<PytorchResponse, PytorchBridgeError> {
        self.ensure_ready()?;
        let runtime = self.loopback_runtime();
        match runtime.complete(request.clone()) {
            Ok(response) => Ok(response),
            Err(first) => {
                self.last_error = Some(first.to_string());
                self.restart()?;
                self.loopback_runtime().complete(request)
            }
        }
    }

    pub fn complete_streaming(
        &mut self,
        request: PytorchRequest,
        on_delta: &mut dyn FnMut(&str),
    ) -> Result<PytorchResponse, PytorchBridgeError> {
        self.ensure_ready()?;
        let runtime = self.loopback_runtime();
        match runtime.complete_streaming(request.clone(), on_delta) {
            Ok(response) => Ok(response),
            Err(first) => {
                self.last_error = Some(first.to_string());
                on_delta(
                    "<sinomo_progress>managed PyTorch runtime failed; restarting</sinomo_progress>",
                );
                self.restart()?;
                self.loopback_runtime()
                    .complete_streaming(request, on_delta)
            }
        }
    }

    pub fn release_model(&mut self) -> Result<(), PytorchBridgeError> {
        if self.child_is_running() {
            let _ = http_post_json(
                &self.endpoint(),
                "/release",
                b"{}",
                Duration::from_secs(10),
                Some(&self.auth_token),
            )?;
        }
        Ok(())
    }

    pub fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        let _ = fs::remove_file(self.auth_token_path());
    }

    fn restart(&mut self) -> Result<(), PytorchBridgeError> {
        self.stop();
        self.restart_count += 1;
        self.spawn()?;
        self.wait_until_ready(recommended_startup_budget())
    }

    fn ensure_ready(&mut self) -> Result<(), PytorchBridgeError> {
        if self.child_is_running() && self.probe_loopback().is_ok() {
            return Ok(());
        }
        self.restart()
    }

    fn child_is_running(&mut self) -> bool {
        match self.child.as_mut() {
            Some(child) => match child.try_wait() {
                Ok(None) => true,
                Ok(Some(status)) => {
                    self.last_error = Some(format!("PyTorch server exited with status {status}"));
                    self.child = None;
                    false
                }
                Err(source) => {
                    self.last_error = Some(format!("failed to observe PyTorch server: {source}"));
                    self.child = None;
                    false
                }
            },
            None => false,
        }
    }

    fn spawn(&mut self) -> Result<(), PytorchBridgeError> {
        ensure_bridge_script(&self.config.server_script)?;
        fs::create_dir_all(&self.config.log_dir).map_err(|source| PytorchBridgeError::Process {
            detail: format!(
                "failed to create PyTorch server log directory {}: {source}",
                self.config.log_dir.display()
            ),
        })?;
        let token_path = self.auth_token_path();
        write_private_file(&token_path, self.auth_token.as_bytes())?;
        let stdout = append_log_file(&self.stdout_log_path())?;
        let stderr = append_log_file(&self.stderr_log_path())?;
        let mut command = Command::new(&self.config.python);
        command
            .arg(&self.config.server_script)
            .arg("--bind")
            .arg("127.0.0.1")
            .arg("--port")
            .arg(self.config.port.to_string())
            .arg("--model")
            .arg(&self.config.model)
            .arg("--device")
            .arg(self.config.device.as_arg())
            .arg("--mode")
            .arg(self.config.mode.as_arg())
            .arg("--auth-token-file")
            .arg(&token_path)
            .stdin(Stdio::null())
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr));
        if self.config.mock {
            command.arg("--mock");
        }
        self.child = Some(
            command
                .spawn()
                .map_err(|source| PytorchBridgeError::Process {
                    detail: format!("failed to start managed PyTorch server: {source}"),
                })?,
        );
        self.last_error = None;
        Ok(())
    }

    fn wait_until_ready(&mut self, timeout: Duration) -> Result<(), PytorchBridgeError> {
        let start = Instant::now();
        while start.elapsed() < timeout {
            if !self.child_is_running() {
                return Err(PytorchBridgeError::Process {
                    detail: self.last_error.clone().unwrap_or_else(|| {
                        "managed PyTorch server exited during startup".to_string()
                    }),
                });
            }
            if self.probe_loopback().is_ok() {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(500));
        }
        Err(PytorchBridgeError::Process {
            detail: format!(
                "managed PyTorch server did not become ready after {} seconds; stderr log: {}",
                timeout.as_secs(),
                self.stderr_log_path().display()
            ),
        })
    }

    fn loopback_runtime(&self) -> RemotePytorchRuntime {
        RemotePytorchRuntime::new(RemotePytorchRuntimeConfig {
            endpoint: self.endpoint(),
            timeout_seconds: self.config.timeout_seconds,
            auth_token: Some(self.auth_token.clone()),
        })
    }

    fn probe_loopback(&self) -> Result<PytorchRuntimeStatus, PytorchBridgeError> {
        self.loopback_runtime().probe()
    }

    fn stdout_log_path(&self) -> PathBuf {
        self.config.log_dir.join("pytorch-server.stdout.log")
    }

    fn stderr_log_path(&self) -> PathBuf {
        self.config.log_dir.join("pytorch-server.stderr.log")
    }

    fn auth_token_path(&self) -> PathBuf {
        self.config.log_dir.join("pytorch-server.token")
    }
}

impl Drop for ManagedPytorchServer {
    fn drop(&mut self) {
        self.stop();
    }
}

#[derive(Clone, Debug, Deserialize)]
struct PytorchStreamEvent {
    #[serde(default)]
    delta: String,
    #[serde(default)]
    done: bool,
    #[serde(default)]
    error: Option<String>,
}

impl LanguageModelProvider for RemotePytorchRuntime {
    fn info(&self) -> LanguageModelProviderInfo {
        LanguageModelProviderInfo {
            provider: MINICPM_PROVIDER_ID.to_string(),
            default_model: PYTORCH_DEFAULT_MODEL.to_string(),
            invocation: sinomo_lm::LanguageModelInvocation::Http,
            supports_json_object: false,
            supports_tool_permissions: false,
        }
    }

    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        let response = self
            .complete(PytorchRequest::from_language_model_request(request))
            .map_err(|source| LanguageModelError::ProviderInvocationFailed {
                provider: MINICPM_PROVIDER_ID.to_string(),
                model: request.model.clone(),
                detail: source.to_string(),
            })?;
        Ok(LanguageModelResponse {
            provider: MINICPM_PROVIDER_ID.to_string(),
            model: request.model.clone(),
            content: response.content,
            usage: response.usage,
        })
    }
}

#[derive(Debug, Error)]
pub enum PytorchBridgeError {
    #[error("PyTorch model bridge path does not exist: {path}")]
    MissingBridge { path: String },
    #[error("PyTorch model serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("PyTorch model process error: {detail}")]
    Process { detail: String },
    #[error("PyTorch model network error: {detail}")]
    Network { detail: String },
}

pub type MiniCpmRuntimeConfig = PytorchRuntimeConfig;
pub type MiniCpmDevice = PytorchDevice;
pub type MiniCpmRequest = PytorchRequest;
pub type MiniCpmMode = PytorchMode;
pub type MiniCpmResponse = PytorchResponse;
pub type MiniCpmRuntimeStatus = PytorchRuntimeStatus;
pub type PythonMiniCpmRuntime = PythonPytorchRuntime;
pub type RemoteMiniCpmRuntimeConfig = RemotePytorchRuntimeConfig;
pub type RemoteMiniCpmRuntime = RemotePytorchRuntime;
pub type MiniCpmError = PytorchBridgeError;

pub fn default_bridge_script() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("python/pytorch_bridge.py")
}

pub fn default_server_script() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("python/pytorch_server.py")
}

pub fn ensure_bridge_script(path: &Path) -> Result<(), PytorchBridgeError> {
    if path.exists() {
        Ok(())
    } else {
        Err(PytorchBridgeError::MissingBridge {
            path: path.display().to_string(),
        })
    }
}

fn append_log_file(path: &Path) -> Result<File, PytorchBridgeError> {
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|source| PytorchBridgeError::Process {
            detail: format!(
                "failed to open PyTorch server log {}: {source}",
                path.display()
            ),
        })
}

fn write_private_file(path: &Path, contents: &[u8]) -> Result<(), PytorchBridgeError> {
    let mut options = OpenOptions::new();
    options.create(true).write(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(path)
        .map_err(|source| PytorchBridgeError::Process {
            detail: format!("failed to open private file {}: {source}", path.display()),
        })?;
    file.write_all(contents)
        .map_err(|source| PytorchBridgeError::Process {
            detail: format!("failed to write private file {}: {source}", path.display()),
        })
}

fn generate_auth_token() -> String {
    let mut bytes = [0_u8; 32];
    if getrandom::fill(&mut bytes).is_err() {
        let fallback = format!(
            "{}:{}:{:?}",
            std::process::id(),
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|value| value.as_nanos())
                .unwrap_or_default(),
            std::thread::current().id()
        );
        return blake3_like_hex(fallback.as_bytes());
    }
    hex_encode(&bytes)
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn blake3_like_hex(bytes: &[u8]) -> String {
    let mut state = 0xcbf29ce484222325_u64;
    for byte in bytes {
        state ^= u64::from(*byte);
        state = state.wrapping_mul(0x100000001b3);
    }
    hex_encode(&state.to_le_bytes())
}

pub fn is_loopback_http_endpoint(endpoint: &str) -> Result<bool, PytorchBridgeError> {
    let (host, _) = parse_http_endpoint(endpoint)?;
    Ok(matches!(
        host.as_str(),
        "127.0.0.1" | "localhost" | "[::1]" | "::1"
    ))
}

pub fn recommended_startup_budget() -> Duration {
    Duration::from_secs(300)
}

fn retryable_read_error(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        ErrorKind::Interrupted | ErrorKind::WouldBlock | ErrorKind::TimedOut
    )
}

fn read_line_until_deadline<R: BufRead>(
    reader: &mut R,
    line: &mut String,
    deadline: Instant,
    context: &str,
    mut wait_progress: Option<(&mut Instant, &mut dyn FnMut(&str), &str)>,
) -> Result<usize, PytorchBridgeError> {
    loop {
        match reader.read_line(line) {
            Ok(read) => return Ok(read),
            Err(source) if retryable_read_error(&source) && Instant::now() < deadline => {
                if let Some((last_progress, on_delta, message)) = wait_progress.as_mut()
                    && last_progress.elapsed() >= Duration::from_secs(5)
                {
                    on_delta(message);
                    **last_progress = Instant::now();
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            Err(source) => {
                return Err(PytorchBridgeError::Network {
                    detail: format!("{context}: {source}"),
                });
            }
        }
    }
}

fn read_to_end_until_deadline<R: Read>(
    reader: &mut R,
    body: &mut Vec<u8>,
    deadline: Instant,
    context: &str,
    mut wait_progress: Option<(&mut Instant, &mut dyn FnMut(&str), &str)>,
) -> Result<(), PytorchBridgeError> {
    let mut buffer = [0_u8; 8192];
    loop {
        match reader.read(&mut buffer) {
            Ok(0) => return Ok(()),
            Ok(read) => body.extend_from_slice(&buffer[..read]),
            Err(source) if retryable_read_error(&source) && Instant::now() < deadline => {
                if let Some((last_progress, on_delta, message)) = wait_progress.as_mut()
                    && last_progress.elapsed() >= Duration::from_secs(5)
                {
                    on_delta(message);
                    **last_progress = Instant::now();
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            Err(source) => {
                return Err(PytorchBridgeError::Network {
                    detail: format!("{context}: {source}"),
                });
            }
        }
    }
}

fn http_post_json(
    endpoint: &str,
    path: &str,
    body: &[u8],
    timeout: Duration,
    auth_token: Option<&str>,
) -> Result<Vec<u8>, PytorchBridgeError> {
    http_request(endpoint, "POST", path, Some(body), timeout, auth_token)
}

fn http_get(
    endpoint: &str,
    path: &str,
    timeout: Duration,
    auth_token: Option<&str>,
) -> Result<Vec<u8>, PytorchBridgeError> {
    http_request(endpoint, "GET", path, None, timeout, auth_token)
}

fn http_request(
    endpoint: &str,
    method: &str,
    path: &str,
    body: Option<&[u8]>,
    timeout: Duration,
    auth_token: Option<&str>,
) -> Result<Vec<u8>, PytorchBridgeError> {
    let (host, port) = parse_http_endpoint(endpoint)?;
    let mut addrs =
        (host.as_str(), port)
            .to_socket_addrs()
            .map_err(|source| PytorchBridgeError::Network {
                detail: format!("resolve {host}:{port}: {source}"),
            })?;
    let addr = addrs.next().ok_or_else(|| PytorchBridgeError::Network {
        detail: format!("no address for {host}:{port}"),
    })?;
    let mut stream = TcpStream::connect_timeout(&addr, timeout).map_err(|source| {
        PytorchBridgeError::Network {
            detail: format!("connect {addr}: {source}"),
        }
    })?;
    stream.set_read_timeout(Some(timeout)).ok();
    stream.set_write_timeout(Some(timeout)).ok();
    let body = body.unwrap_or(&[]);
    let auth_header = auth_token
        .map(|token| format!("Authorization: Bearer {token}\r\n"))
        .unwrap_or_default();
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {host}:{port}\r\nContent-Type: application/json\r\n{auth_header}Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream
        .write_all(request.as_bytes())
        .and_then(|_| stream.write_all(body))
        .map_err(|source| PytorchBridgeError::Network {
            detail: format!("write request: {source}"),
        })?;
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .map_err(|source| PytorchBridgeError::Network {
            detail: format!("read response: {source}"),
        })?;
    parse_http_response(&response)
}

fn http_post_json_event_stream(
    endpoint: &str,
    path: &str,
    body: &[u8],
    timeout: Duration,
    auth_token: Option<&str>,
    on_delta: &mut dyn FnMut(&str),
) -> Result<PytorchResponse, PytorchBridgeError> {
    let deadline = Instant::now() + timeout;
    let (host, port) = parse_http_endpoint(endpoint)?;
    let mut addrs =
        (host.as_str(), port)
            .to_socket_addrs()
            .map_err(|source| PytorchBridgeError::Network {
                detail: format!("resolve {host}:{port}: {source}"),
            })?;
    let addr = addrs.next().ok_or_else(|| PytorchBridgeError::Network {
        detail: format!("no address for {host}:{port}"),
    })?;
    let mut stream = TcpStream::connect_timeout(&addr, timeout).map_err(|source| {
        PytorchBridgeError::Network {
            detail: format!("connect {addr}: {source}"),
        }
    })?;
    stream.set_read_timeout(Some(timeout)).ok();
    stream.set_write_timeout(Some(timeout)).ok();
    let auth_header = auth_token
        .map(|token| format!("Authorization: Bearer {token}\r\n"))
        .unwrap_or_default();
    let request = format!(
        "POST {path} HTTP/1.1\r\nHost: {host}:{port}\r\nContent-Type: application/json\r\n{auth_header}Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream
        .write_all(request.as_bytes())
        .and_then(|_| stream.write_all(body))
        .map_err(|source| PytorchBridgeError::Network {
            detail: format!("write request: {source}"),
        })?;

    let mut reader = BufReader::new(stream);
    let mut last_wait_progress = Instant::now()
        .checked_sub(Duration::from_secs(5))
        .unwrap_or_else(Instant::now);
    let wait_message =
        "<sinomo_progress>waiting for configured model runtime response</sinomo_progress>";
    let mut status = String::new();
    read_line_until_deadline(
        &mut reader,
        &mut status,
        deadline,
        "read response status",
        Some((&mut last_wait_progress, on_delta, wait_message)),
    )?;
    if !status.contains(" 200 ") {
        return Err(PytorchBridgeError::Network {
            detail: status.trim().to_string(),
        });
    }
    let mut line = String::new();
    let mut content_type = String::new();
    loop {
        line.clear();
        let read = read_line_until_deadline(
            &mut reader,
            &mut line,
            deadline,
            "read response headers",
            Some((&mut last_wait_progress, on_delta, wait_message)),
        )?;
        if read == 0 || line == "\r\n" || line == "\n" {
            break;
        }
        if let Some((name, value)) = line.split_once(':')
            && name.eq_ignore_ascii_case("content-type")
        {
            content_type = value.trim().to_string();
        }
    }
    if !content_type.starts_with("text/event-stream") {
        let mut body = Vec::new();
        read_to_end_until_deadline(
            &mut reader,
            &mut body,
            deadline,
            "read json response body",
            Some((&mut last_wait_progress, on_delta, wait_message)),
        )?;
        return Ok(serde_json::from_slice(&body)?);
    }

    let mut final_response = None;
    loop {
        line.clear();
        let read = read_line_until_deadline(
            &mut reader,
            &mut line,
            deadline,
            "read event stream",
            Some((&mut last_wait_progress, on_delta, wait_message)),
        )?;
        if read == 0 {
            break;
        }
        let Some(data) = line.trim_end().strip_prefix("data: ") else {
            continue;
        };
        let event: PytorchStreamEvent = serde_json::from_str(data)?;
        if !event.delta.is_empty() {
            on_delta(&event.delta);
        }
        if let Some(error) = event.error {
            return Err(PytorchBridgeError::Process { detail: error });
        }
        if event.done {
            final_response = Some(serde_json::from_str::<PytorchResponse>(data)?);
            break;
        }
    }
    final_response.ok_or_else(|| PytorchBridgeError::Network {
        detail: "stream ended before final model response".to_string(),
    })
}

fn parse_http_endpoint(endpoint: &str) -> Result<(String, u16), PytorchBridgeError> {
    let rest = endpoint
        .strip_prefix("http://")
        .ok_or_else(|| PytorchBridgeError::Network {
            detail: format!("only http:// endpoints are supported: {endpoint}"),
        })?;
    let authority = rest.split('/').next().unwrap_or(rest);
    let (host, port) = authority
        .rsplit_once(':')
        .ok_or_else(|| PytorchBridgeError::Network {
            detail: format!("endpoint must include a port: {endpoint}"),
        })?;
    let port = port
        .parse::<u16>()
        .map_err(|source| PytorchBridgeError::Network {
            detail: format!("invalid endpoint port `{port}`: {source}"),
        })?;
    Ok((host.to_string(), port))
}

fn parse_http_response(response: &[u8]) -> Result<Vec<u8>, PytorchBridgeError> {
    let split = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| PytorchBridgeError::Network {
            detail: "malformed HTTP response".to_string(),
        })?;
    let (head, body) = response.split_at(split + 4);
    let head_text = String::from_utf8_lossy(head);
    let status = head_text.lines().next().unwrap_or("");
    if !status.contains(" 200 ") {
        return Err(PytorchBridgeError::Network {
            detail: format!("{status}: {}", String::from_utf8_lossy(body).trim()),
        });
    }
    Ok(body.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_lm::{LanguageModelResponseFormat, LanguageModelRole};

    #[test]
    fn default_bridge_script_points_inside_crate() {
        let path = default_bridge_script();
        assert!(path.ends_with("python/pytorch_bridge.py"));
        ensure_bridge_script(&path).expect("checked-in bridge exists");
    }

    #[test]
    fn language_model_request_maps_system_prompt_and_messages() {
        let request = LanguageModelRequest {
            provider: MINICPM_PROVIDER_ID.to_string(),
            model: MINICPM_DEFAULT_MODEL.to_string(),
            messages: vec![
                LanguageModelMessage {
                    role: LanguageModelRole::System,
                    content: "system".to_string(),
                },
                LanguageModelMessage {
                    role: LanguageModelRole::User,
                    content: "hello".to_string(),
                },
            ],
            temperature: None,
            max_output_tokens: None,
            response_format: LanguageModelResponseFormat::Text,
            metadata: serde_json::json!({"session": "test"}),
        };

        let mapped = PytorchRequest::from_language_model_request(&request);

        assert_eq!(mapped.system_prompt, "system");
        assert_eq!(mapped.messages.len(), 1);
        assert_eq!(mapped.mode, PytorchMode::Text);
        assert_eq!(mapped.metadata["session"], "test");
    }

    #[test]
    fn mock_bridge_round_trips_without_model_dependencies() {
        let runtime = PythonPytorchRuntime::new(PytorchRuntimeConfig::default().with_mock(true));
        let response = runtime
            .complete(PytorchRequest {
                mode: PytorchMode::Text,
                system_prompt: "respond briefly".to_string(),
                messages: vec![LanguageModelMessage {
                    role: LanguageModelRole::User,
                    content: "hello".to_string(),
                }],
                image_path: None,
                image_base64: None,
                image_name: None,
                audio_path: None,
                stream: false,
                metadata: serde_json::Value::Null,
            })
            .expect("mock bridge");

        assert!(response.content.contains("hello"));
        assert!(response.runtime.local);
    }

    #[test]
    fn remote_runtime_posts_json_to_http_endpoint() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("listener");
        let addr = listener.local_addr().expect("addr");
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept");
            let mut buffer = [0_u8; 4096];
            let size = stream.read(&mut buffer).expect("read");
            let request = String::from_utf8_lossy(&buffer[..size]);
            assert!(request.starts_with("POST /complete HTTP/1.1"));
            let body = serde_json::to_string(&PytorchResponse {
                content: "ok".to_string(),
                runtime: PytorchRuntimeStatus {
                    model: MINICPM_DEFAULT_MODEL.to_string(),
                    device: PytorchDevice::Cuda,
                    backend: "pytorch-transformers".to_string(),
                    local: false,
                    streaming_supported: true,
                    vision_supported: true,
                    audio_supported: true,
                    tts_supported: true,
                    diagnostic: None,
                },
                usage: None,
                elapsed_ms: Some(10),
                output_tokens_per_second: Some(100.0),
            })
            .expect("json");
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).expect("write");
        });
        let runtime = RemotePytorchRuntime::new(RemotePytorchRuntimeConfig {
            endpoint: format!("http://{addr}"),
            timeout_seconds: 5,
            auth_token: None,
        });
        let response = runtime
            .complete(PytorchRequest {
                mode: PytorchMode::Text,
                system_prompt: String::new(),
                messages: vec![],
                image_path: None,
                image_base64: None,
                image_name: None,
                audio_path: None,
                stream: false,
                metadata: serde_json::Value::Null,
            })
            .expect("remote response");
        assert_eq!(response.content, "ok");
        assert_eq!(response.output_tokens_per_second, Some(100.0));
        handle.join().expect("server");
    }

    #[test]
    fn remote_runtime_streams_sse_deltas_before_final_response() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("listener");
        let addr = listener.local_addr().expect("addr");
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept");
            let mut buffer = [0_u8; 4096];
            let size = stream.read(&mut buffer).expect("read");
            let request = String::from_utf8_lossy(&buffer[..size]);
            assert!(request.starts_with("POST /complete HTTP/1.1"));
            assert!(request.contains("\"stream\":true") || request.contains("\"stream\": true"));
            let response =
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n";
            stream.write_all(response.as_bytes()).expect("headers");
            stream
                .write_all(
                    b"data: {\"delta\":\"<sinomo_progress>accepted</sinomo_progress>\",\"done\":false}\n\n",
                )
                .expect("delta");
            let final_response = serde_json::to_string(&PytorchResponse {
                content: "done".to_string(),
                runtime: PytorchRuntimeStatus {
                    model: MINICPM_DEFAULT_MODEL.to_string(),
                    device: PytorchDevice::Cuda,
                    backend: "pytorch-transformers".to_string(),
                    local: false,
                    streaming_supported: true,
                    vision_supported: true,
                    audio_supported: true,
                    tts_supported: true,
                    diagnostic: None,
                },
                usage: None,
                elapsed_ms: Some(10),
                output_tokens_per_second: Some(100.0),
            })
            .expect("json");
            let mut final_value: serde_json::Value =
                serde_json::from_str(&final_response).expect("json value");
            final_value["done"] = serde_json::Value::Bool(true);
            stream
                .write_all(format!("data: {final_value}\n\n").as_bytes())
                .expect("final");
        });
        let runtime = RemotePytorchRuntime::new(RemotePytorchRuntimeConfig {
            endpoint: format!("http://{addr}"),
            timeout_seconds: 5,
            auth_token: None,
        });
        let mut deltas = Vec::new();
        let response = runtime
            .complete_streaming(
                PytorchRequest {
                    mode: PytorchMode::Text,
                    system_prompt: String::new(),
                    messages: vec![],
                    image_path: None,
                    image_base64: None,
                    image_name: None,
                    audio_path: None,
                    stream: false,
                    metadata: serde_json::Value::Null,
                },
                &mut |delta| deltas.push(delta.to_string()),
            )
            .expect("remote streaming response");
        assert_eq!(
            deltas,
            vec!["<sinomo_progress>accepted</sinomo_progress>".to_string()]
        );
        assert_eq!(response.content, "done");
        handle.join().expect("server");
    }

    #[test]
    fn remote_runtime_includes_auth_header_when_configured() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("listener");
        let addr = listener.local_addr().expect("addr");
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept");
            let mut buffer = [0_u8; 4096];
            let size = stream.read(&mut buffer).expect("read");
            let request = String::from_utf8_lossy(&buffer[..size]);
            assert!(request.contains("Authorization: Bearer test-token\r\n"));
            let body = serde_json::to_string(&PytorchRuntimeStatus {
                model: MINICPM_DEFAULT_MODEL.to_string(),
                device: PytorchDevice::Cpu,
                backend: "pytorch-transformers".to_string(),
                local: true,
                streaming_supported: true,
                vision_supported: true,
                audio_supported: true,
                tts_supported: true,
                diagnostic: None,
            })
            .expect("json");
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).expect("write");
        });
        let runtime = RemotePytorchRuntime::new(RemotePytorchRuntimeConfig {
            endpoint: format!("http://{addr}"),
            timeout_seconds: 5,
            auth_token: Some("test-token".to_string()),
        });
        let status = runtime.probe().expect("probe");
        assert_eq!(status.device, PytorchDevice::Cpu);
        handle.join().expect("server");
    }

    #[test]
    fn loopback_endpoint_detection_rejects_non_loopback_hosts() {
        assert!(is_loopback_http_endpoint("http://127.0.0.1:1").expect("loopback"));
        assert!(is_loopback_http_endpoint("http://localhost:1").expect("loopback"));
        assert!(!is_loopback_http_endpoint("http://192.0.2.10:1").expect("non-loopback"));
    }

    #[test]
    fn managed_mock_server_requires_supervisor_auth_token() {
        let port = unused_loopback_port();
        let log_dir = std::env::temp_dir().join(format!(
            "sinomo-pytorch-test-{}-{}",
            std::process::id(),
            port
        ));
        let mut server = ManagedPytorchServer::new(ManagedPytorchServerConfig {
            port,
            log_dir: log_dir.clone(),
            mock: true,
            ..ManagedPytorchServerConfig::default()
        });
        let response = server
            .complete(PytorchRequest {
                mode: PytorchMode::Text,
                system_prompt: String::new(),
                messages: vec![LanguageModelMessage {
                    role: LanguageModelRole::User,
                    content: "hello".to_string(),
                }],
                image_path: None,
                image_base64: None,
                image_name: None,
                audio_path: None,
                stream: false,
                metadata: serde_json::Value::Null,
            })
            .expect("managed mock response");
        assert!(response.content.contains("hello"));
        let unauthorized = http_get(
            &format!("http://127.0.0.1:{port}"),
            "/health",
            Duration::from_secs(5),
            None,
        )
        .expect_err("missing token is denied");
        assert!(unauthorized.to_string().contains("403"));
        server.stop();
        let _ = std::fs::remove_dir_all(log_dir);
    }

    fn unused_loopback_port() -> u16 {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("listener");
        listener.local_addr().expect("addr").port()
    }
}
