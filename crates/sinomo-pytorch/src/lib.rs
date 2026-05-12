//! PyTorch/Transformers bridge for Sinomo's local model runtimes.
//!
//! The Rust side intentionally owns the stable request/response contract and
//! process lifecycle. The Python side owns model loading because local model
//! families such as MiniCPM-o depend on PyTorch/Transformers runtime packages.

use serde::{Deserialize, Serialize};
use sinomo_lm::{
    LanguageModelError, LanguageModelMessage, LanguageModelProvider, LanguageModelProviderInfo,
    LanguageModelRequest, LanguageModelResponse, LanguageModelUsage,
};
use std::io::{BufRead, BufReader, ErrorKind, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use thiserror::Error;

pub const MINICPM_PROVIDER_ID: &str = "minicpm-o";
pub const MINICPM_DEFAULT_MODEL: &str = "openbmb/MiniCPM-o-4_5";
pub const PYTORCH_DEFAULT_PROVIDER_ID: &str = MINICPM_PROVIDER_ID;
pub const PYTORCH_DEFAULT_MODEL: &str = MINICPM_DEFAULT_MODEL;

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
}

impl Default for RemotePytorchRuntimeConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://127.0.0.1:32153".to_string(),
            timeout_seconds: 300,
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

    pub fn complete(&self, request: PytorchRequest) -> Result<PytorchResponse, PytorchBridgeError> {
        let body = serde_json::to_vec(&request)?;
        let response = http_post_json(
            &self.config.endpoint,
            "/complete",
            &body,
            Duration::from_secs(self.config.timeout_seconds),
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
            on_delta,
        )
    }

    pub fn probe(&self) -> Result<PytorchRuntimeStatus, PytorchBridgeError> {
        let response = http_get(
            &self.config.endpoint,
            "/health",
            Duration::from_secs(self.config.timeout_seconds),
        )?;
        Ok(serde_json::from_slice(&response)?)
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

pub fn ensure_bridge_script(path: &Path) -> Result<(), PytorchBridgeError> {
    if path.exists() {
        Ok(())
    } else {
        Err(PytorchBridgeError::MissingBridge {
            path: path.display().to_string(),
        })
    }
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
) -> Result<usize, PytorchBridgeError> {
    loop {
        match reader.read_line(line) {
            Ok(read) => return Ok(read),
            Err(source) if retryable_read_error(&source) && Instant::now() < deadline => {
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
) -> Result<(), PytorchBridgeError> {
    let mut buffer = [0_u8; 8192];
    loop {
        match reader.read(&mut buffer) {
            Ok(0) => return Ok(()),
            Ok(read) => body.extend_from_slice(&buffer[..read]),
            Err(source) if retryable_read_error(&source) && Instant::now() < deadline => {
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
) -> Result<Vec<u8>, PytorchBridgeError> {
    http_request(endpoint, "POST", path, Some(body), timeout)
}

fn http_get(endpoint: &str, path: &str, timeout: Duration) -> Result<Vec<u8>, PytorchBridgeError> {
    http_request(endpoint, "GET", path, None, timeout)
}

fn http_request(
    endpoint: &str,
    method: &str,
    path: &str,
    body: Option<&[u8]>,
    timeout: Duration,
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
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {host}:{port}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
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
    let request = format!(
        "POST {path} HTTP/1.1\r\nHost: {host}:{port}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream
        .write_all(request.as_bytes())
        .and_then(|_| stream.write_all(body))
        .map_err(|source| PytorchBridgeError::Network {
            detail: format!("write request: {source}"),
        })?;

    let mut reader = BufReader::new(stream);
    let mut status = String::new();
    read_line_until_deadline(&mut reader, &mut status, deadline, "read response status")?;
    if !status.contains(" 200 ") {
        return Err(PytorchBridgeError::Network {
            detail: status.trim().to_string(),
        });
    }
    let mut line = String::new();
    let mut content_type = String::new();
    loop {
        line.clear();
        let read =
            read_line_until_deadline(&mut reader, &mut line, deadline, "read response headers")?;
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
        read_to_end_until_deadline(&mut reader, &mut body, deadline, "read json response body")?;
        return Ok(serde_json::from_slice(&body)?);
    }

    let mut final_response = None;
    loop {
        line.clear();
        let read =
            read_line_until_deadline(&mut reader, &mut line, deadline, "read event stream")?;
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
            let response = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n";
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
}
