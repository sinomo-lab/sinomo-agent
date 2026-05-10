//! MiniCPM-o bridge for Sinomo's local real-time core model.
//!
//! The Rust side intentionally owns the stable request/response contract and
//! process lifecycle. The Python side owns model loading because the reference
//! MiniCPM-o runtime is PyTorch/Transformers based.

use serde::{Deserialize, Serialize};
use sinomo_lm::{
    LanguageModelError, LanguageModelMessage, LanguageModelProvider, LanguageModelProviderInfo,
    LanguageModelRequest, LanguageModelResponse, LanguageModelUsage,
};
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;
use thiserror::Error;

pub const MINICPM_PROVIDER_ID: &str = "minicpm-o";
pub const MINICPM_DEFAULT_MODEL: &str = "openbmb/MiniCPM-o-4_5";

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MiniCpmRuntimeConfig {
    pub python: PathBuf,
    pub bridge_script: PathBuf,
    pub model: String,
    pub device: MiniCpmDevice,
    pub timeout_seconds: u64,
    #[serde(default)]
    pub mock: bool,
}

impl Default for MiniCpmRuntimeConfig {
    fn default() -> Self {
        Self {
            python: PathBuf::from("python3"),
            bridge_script: default_bridge_script(),
            model: MINICPM_DEFAULT_MODEL.to_string(),
            device: MiniCpmDevice::Auto,
            timeout_seconds: 300,
            mock: false,
        }
    }
}

impl MiniCpmRuntimeConfig {
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
pub enum MiniCpmDevice {
    Auto,
    Cpu,
    Cuda,
}

impl MiniCpmDevice {
    fn as_arg(&self) -> &'static str {
        match self {
            MiniCpmDevice::Auto => "auto",
            MiniCpmDevice::Cpu => "cpu",
            MiniCpmDevice::Cuda => "cuda",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct MiniCpmRequest {
    pub mode: MiniCpmMode,
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

impl MiniCpmRequest {
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
            mode: MiniCpmMode::Text,
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
pub enum MiniCpmMode {
    Text,
    Vision,
    Audio,
    Duplex,
    Probe,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct MiniCpmResponse {
    pub content: String,
    pub runtime: MiniCpmRuntimeStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<LanguageModelUsage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub elapsed_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_tokens_per_second: Option<f64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MiniCpmRuntimeStatus {
    pub model: String,
    pub device: MiniCpmDevice,
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
pub struct PythonMiniCpmRuntime {
    config: MiniCpmRuntimeConfig,
}

impl PythonMiniCpmRuntime {
    pub fn new(config: MiniCpmRuntimeConfig) -> Self {
        Self { config }
    }

    pub fn config(&self) -> &MiniCpmRuntimeConfig {
        &self.config
    }

    pub fn probe(&self) -> Result<MiniCpmRuntimeStatus, MiniCpmError> {
        let response = self.complete(MiniCpmRequest {
            mode: MiniCpmMode::Probe,
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

    pub fn complete(&self, request: MiniCpmRequest) -> Result<MiniCpmResponse, MiniCpmError> {
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
            command.env("SINOMO_MINICPM_MOCK", "1");
        }
        let mut child = command.spawn().map_err(|source| MiniCpmError::Process {
            detail: format!("failed to start MiniCPM bridge: {source}"),
        })?;
        child
            .stdin
            .as_mut()
            .ok_or_else(|| MiniCpmError::Process {
                detail: "MiniCPM bridge stdin was unavailable".to_string(),
            })?
            .write_all(&payload)
            .map_err(|source| MiniCpmError::Process {
                detail: format!("failed to write MiniCPM request: {source}"),
            })?;
        let output = child
            .wait_with_output()
            .map_err(|source| MiniCpmError::Process {
                detail: format!("failed to wait for MiniCPM bridge: {source}"),
            })?;
        if !output.status.success() {
            return Err(MiniCpmError::Process {
                detail: String::from_utf8_lossy(&output.stderr).trim().to_string(),
            });
        }
        Ok(serde_json::from_slice(&output.stdout)?)
    }
}

impl LanguageModelProvider for PythonMiniCpmRuntime {
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
            .complete(MiniCpmRequest::from_language_model_request(request))
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
pub struct RemoteMiniCpmRuntimeConfig {
    pub endpoint: String,
    pub timeout_seconds: u64,
}

impl Default for RemoteMiniCpmRuntimeConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://127.0.0.1:32153".to_string(),
            timeout_seconds: 300,
        }
    }
}

#[derive(Clone, Debug)]
pub struct RemoteMiniCpmRuntime {
    config: RemoteMiniCpmRuntimeConfig,
}

impl RemoteMiniCpmRuntime {
    pub fn new(config: RemoteMiniCpmRuntimeConfig) -> Self {
        Self { config }
    }

    pub fn complete(&self, request: MiniCpmRequest) -> Result<MiniCpmResponse, MiniCpmError> {
        let body = serde_json::to_vec(&request)?;
        let response = http_post_json(
            &self.config.endpoint,
            "/complete",
            &body,
            Duration::from_secs(self.config.timeout_seconds),
        )?;
        Ok(serde_json::from_slice(&response)?)
    }

    pub fn probe(&self) -> Result<MiniCpmRuntimeStatus, MiniCpmError> {
        let response = http_get(
            &self.config.endpoint,
            "/health",
            Duration::from_secs(self.config.timeout_seconds),
        )?;
        Ok(serde_json::from_slice(&response)?)
    }
}

impl LanguageModelProvider for RemoteMiniCpmRuntime {
    fn info(&self) -> LanguageModelProviderInfo {
        LanguageModelProviderInfo {
            provider: MINICPM_PROVIDER_ID.to_string(),
            default_model: MINICPM_DEFAULT_MODEL.to_string(),
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
            .complete(MiniCpmRequest::from_language_model_request(request))
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
pub enum MiniCpmError {
    #[error("MiniCPM bridge path does not exist: {path}")]
    MissingBridge { path: String },
    #[error("MiniCPM serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("MiniCPM process error: {detail}")]
    Process { detail: String },
    #[error("MiniCPM network error: {detail}")]
    Network { detail: String },
}

pub fn default_bridge_script() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("python/minicpm_bridge.py")
}

pub fn ensure_bridge_script(path: &Path) -> Result<(), MiniCpmError> {
    if path.exists() {
        Ok(())
    } else {
        Err(MiniCpmError::MissingBridge {
            path: path.display().to_string(),
        })
    }
}

pub fn recommended_startup_budget() -> Duration {
    Duration::from_secs(300)
}

fn http_post_json(
    endpoint: &str,
    path: &str,
    body: &[u8],
    timeout: Duration,
) -> Result<Vec<u8>, MiniCpmError> {
    http_request(endpoint, "POST", path, Some(body), timeout)
}

fn http_get(endpoint: &str, path: &str, timeout: Duration) -> Result<Vec<u8>, MiniCpmError> {
    http_request(endpoint, "GET", path, None, timeout)
}

fn http_request(
    endpoint: &str,
    method: &str,
    path: &str,
    body: Option<&[u8]>,
    timeout: Duration,
) -> Result<Vec<u8>, MiniCpmError> {
    let (host, port) = parse_http_endpoint(endpoint)?;
    let mut addrs =
        (host.as_str(), port)
            .to_socket_addrs()
            .map_err(|source| MiniCpmError::Network {
                detail: format!("resolve {host}:{port}: {source}"),
            })?;
    let addr = addrs.next().ok_or_else(|| MiniCpmError::Network {
        detail: format!("no address for {host}:{port}"),
    })?;
    let mut stream =
        TcpStream::connect_timeout(&addr, timeout).map_err(|source| MiniCpmError::Network {
            detail: format!("connect {addr}: {source}"),
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
        .map_err(|source| MiniCpmError::Network {
            detail: format!("write request: {source}"),
        })?;
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .map_err(|source| MiniCpmError::Network {
            detail: format!("read response: {source}"),
        })?;
    parse_http_response(&response)
}

fn parse_http_endpoint(endpoint: &str) -> Result<(String, u16), MiniCpmError> {
    let rest = endpoint
        .strip_prefix("http://")
        .ok_or_else(|| MiniCpmError::Network {
            detail: format!("only http:// endpoints are supported: {endpoint}"),
        })?;
    let authority = rest.split('/').next().unwrap_or(rest);
    let (host, port) = authority
        .rsplit_once(':')
        .ok_or_else(|| MiniCpmError::Network {
            detail: format!("endpoint must include a port: {endpoint}"),
        })?;
    let port = port
        .parse::<u16>()
        .map_err(|source| MiniCpmError::Network {
            detail: format!("invalid endpoint port `{port}`: {source}"),
        })?;
    Ok((host.to_string(), port))
}

fn parse_http_response(response: &[u8]) -> Result<Vec<u8>, MiniCpmError> {
    let split = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| MiniCpmError::Network {
            detail: "malformed HTTP response".to_string(),
        })?;
    let (head, body) = response.split_at(split + 4);
    let head_text = String::from_utf8_lossy(head);
    let status = head_text.lines().next().unwrap_or("");
    if !status.contains(" 200 ") {
        return Err(MiniCpmError::Network {
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
        assert!(path.ends_with("python/minicpm_bridge.py"));
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

        let mapped = MiniCpmRequest::from_language_model_request(&request);

        assert_eq!(mapped.system_prompt, "system");
        assert_eq!(mapped.messages.len(), 1);
        assert_eq!(mapped.mode, MiniCpmMode::Text);
        assert_eq!(mapped.metadata["session"], "test");
    }

    #[test]
    fn mock_bridge_round_trips_without_model_dependencies() {
        let runtime = PythonMiniCpmRuntime::new(MiniCpmRuntimeConfig::default().with_mock(true));
        let response = runtime
            .complete(MiniCpmRequest {
                mode: MiniCpmMode::Text,
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
            let body = serde_json::to_string(&MiniCpmResponse {
                content: "ok".to_string(),
                runtime: MiniCpmRuntimeStatus {
                    model: MINICPM_DEFAULT_MODEL.to_string(),
                    device: MiniCpmDevice::Cuda,
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
        let runtime = RemoteMiniCpmRuntime::new(RemoteMiniCpmRuntimeConfig {
            endpoint: format!("http://{addr}"),
            timeout_seconds: 5,
        });
        let response = runtime
            .complete(MiniCpmRequest {
                mode: MiniCpmMode::Text,
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
}
