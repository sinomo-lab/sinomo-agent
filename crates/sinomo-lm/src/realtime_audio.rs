use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use http::header::{AUTHORIZATION, HeaderName};
use http::{HeaderValue, Uri};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::time::Duration;
use thiserror::Error;
use tokio::net::TcpStream;
use tokio_websockets::{ClientBuilder, MaybeTlsStream, Message, WebSocketStream};

const OPENAI_REALTIME_DEFAULT_MODEL: &str = "gpt-realtime-2";
const MINICPM_REALTIME_DEFAULT_MODEL: &str = "MiniCPM-o-4.5";
const OPENAI_DEFAULT_REALTIME_BASE_URL: &str = "https://api.openai.com/v1";
const MINICPM_DEFAULT_REALTIME_BASE_URL: &str = "https://minicpmo45.modelbest.cn";
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_RECEIVE_TIMEOUT: Duration = Duration::from_secs(60);

type RealtimeWs = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum RealtimeAudioProvider {
    OpenAi,
    MiniCpmO,
}

impl RealtimeAudioProvider {
    pub fn from_id(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "openai" => Some(Self::OpenAi),
            "minicpm" | "minicpm-o" | "modelbest" | "minicpm-o-4.5" => Some(Self::MiniCpmO),
            _ => None,
        }
    }

    pub fn provider_id(self) -> &'static str {
        match self {
            Self::OpenAi => "openai",
            Self::MiniCpmO => "minicpm-o",
        }
    }

    pub fn default_model(self) -> &'static str {
        match self {
            Self::OpenAi => OPENAI_REALTIME_DEFAULT_MODEL,
            Self::MiniCpmO => MINICPM_REALTIME_DEFAULT_MODEL,
        }
    }

    pub fn default_base_url(self) -> &'static str {
        match self {
            Self::OpenAi => OPENAI_DEFAULT_REALTIME_BASE_URL,
            Self::MiniCpmO => MINICPM_DEFAULT_REALTIME_BASE_URL,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum RealtimeAudioSampleFormat {
    Pcm16Le,
    F32Le,
}

impl RealtimeAudioSampleFormat {
    pub fn bytes_per_sample(self) -> usize {
        match self {
            Self::Pcm16Le => 2,
            Self::F32Le => 4,
        }
    }

    pub fn as_config_value(self) -> &'static str {
        match self {
            Self::Pcm16Le => "pcm16-le",
            Self::F32Le => "f32-le",
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum RealtimeVideoFrameFormat {
    Jpeg,
    Png,
}

impl RealtimeVideoFrameFormat {
    pub fn mime_type(self) -> &'static str {
        match self {
            Self::Jpeg => "image/jpeg",
            Self::Png => "image/png",
        }
    }

    pub fn extension(self) -> &'static str {
        match self {
            Self::Jpeg => "jpg",
            Self::Png => "png",
        }
    }

    pub fn as_config_value(self) -> &'static str {
        match self {
            Self::Jpeg => "jpeg",
            Self::Png => "png",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum RealtimeVideoFrameDetail {
    #[default]
    Auto,
    Low,
    High,
}

impl RealtimeVideoFrameDetail {
    pub fn as_config_value(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Low => "low",
            Self::High => "high",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RealtimeVideoInputConfig {
    pub frame_format: RealtimeVideoFrameFormat,
    #[serde(default)]
    pub detail: RealtimeVideoFrameDetail,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_frame_rate_hz: Option<u32>,
}

impl Default for RealtimeVideoInputConfig {
    fn default() -> Self {
        Self {
            frame_format: RealtimeVideoFrameFormat::Jpeg,
            detail: RealtimeVideoFrameDetail::Auto,
            max_frame_rate_hz: Some(2),
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum RealtimeAudioTurnDetection {
    Manual,
    SemanticVad,
}

impl Default for RealtimeAudioTurnDetection {
    fn default() -> Self {
        Self::Manual
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RealtimeAudioSessionConfig {
    pub provider: RealtimeAudioProvider,
    pub model: String,
    pub instructions: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_base_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub websocket_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub safety_identifier: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub voice: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_video: Option<RealtimeVideoInputConfig>,
    pub input_sample_rate_hz: u32,
    pub output_sample_rate_hz: u32,
    pub input_format: RealtimeAudioSampleFormat,
    pub output_format: RealtimeAudioSampleFormat,
    #[serde(default)]
    pub turn_detection: RealtimeAudioTurnDetection,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connect_timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receive_timeout_ms: Option<u64>,
}

impl RealtimeAudioSessionConfig {
    pub fn new(provider: RealtimeAudioProvider, model: impl Into<String>) -> Self {
        let model = model.into();
        let model = if model.trim().is_empty() {
            provider.default_model().to_string()
        } else {
            model
        };
        let (input_sample_rate_hz, input_format, output_sample_rate_hz, output_format) =
            match provider {
                RealtimeAudioProvider::OpenAi => (
                    24_000,
                    RealtimeAudioSampleFormat::Pcm16Le,
                    24_000,
                    RealtimeAudioSampleFormat::Pcm16Le,
                ),
                RealtimeAudioProvider::MiniCpmO => (
                    16_000,
                    RealtimeAudioSampleFormat::F32Le,
                    24_000,
                    RealtimeAudioSampleFormat::F32Le,
                ),
            };
        Self {
            provider,
            model,
            instructions: "You are Sinomo's realtime voice agent. Keep spoken responses concise."
                .to_string(),
            api_key: None,
            api_base_url: None,
            websocket_url: None,
            safety_identifier: None,
            voice: None,
            input_video: None,
            input_sample_rate_hz,
            output_sample_rate_hz,
            input_format,
            output_format,
            turn_detection: RealtimeAudioTurnDetection::Manual,
            connect_timeout_ms: None,
            receive_timeout_ms: None,
        }
    }

    pub fn connect_timeout(&self) -> Duration {
        self.connect_timeout_ms
            .map(Duration::from_millis)
            .unwrap_or(DEFAULT_CONNECT_TIMEOUT)
    }

    pub fn receive_timeout(&self) -> Duration {
        self.receive_timeout_ms
            .map(Duration::from_millis)
            .unwrap_or(DEFAULT_RECEIVE_TIMEOUT)
    }

    pub fn websocket_url(&self) -> Result<String, RealtimeAudioError> {
        if let Some(url) = self
            .websocket_url
            .as_deref()
            .map(str::trim)
            .filter(|url| !url.is_empty())
        {
            return Ok(url.to_string());
        }
        let base_url = self
            .api_base_url
            .as_deref()
            .unwrap_or_else(|| self.provider.default_base_url());
        match self.provider {
            RealtimeAudioProvider::OpenAi => openai_realtime_ws_url(base_url, &self.model),
            RealtimeAudioProvider::MiniCpmO => {
                minicpm_realtime_ws_url_for_video(base_url, self.input_video.is_some())
            }
        }
    }

    pub fn validate(&self) -> Result<(), RealtimeAudioError> {
        match self.provider {
            RealtimeAudioProvider::OpenAi => {
                if self.input_format != RealtimeAudioSampleFormat::Pcm16Le {
                    return Err(RealtimeAudioError::InvalidConfig {
                        detail: "OpenAI realtime audio input must be 16-bit PCM little-endian"
                            .to_string(),
                    });
                }
            }
            RealtimeAudioProvider::MiniCpmO => {
                if self.input_format != RealtimeAudioSampleFormat::F32Le {
                    return Err(RealtimeAudioError::InvalidConfig {
                        detail: "MiniCPM-o realtime audio input must be float32 PCM little-endian"
                            .to_string(),
                    });
                }
                if self.input_sample_rate_hz != 16_000 {
                    return Err(RealtimeAudioError::InvalidConfig {
                        detail: "MiniCPM-o realtime audio input must be 16 kHz".to_string(),
                    });
                }
            }
        }
        Ok(())
    }

    pub fn with_video_input(mut self, config: RealtimeVideoInputConfig) -> Self {
        self.input_video = Some(config);
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RealtimeAudioInputChunk {
    pub audio: Vec<u8>,
    #[serde(default)]
    pub force_listen: bool,
}

impl RealtimeAudioInputChunk {
    pub fn new(audio: Vec<u8>) -> Self {
        Self {
            audio,
            force_listen: false,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RealtimeVideoFrameInput {
    pub frame: Vec<u8>,
    pub format: RealtimeVideoFrameFormat,
    #[serde(default)]
    pub detail: RealtimeVideoFrameDetail,
    pub sequence: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp_ms: Option<u64>,
}

impl RealtimeVideoFrameInput {
    pub fn new(frame: Vec<u8>, format: RealtimeVideoFrameFormat, sequence: u64) -> Self {
        Self {
            frame,
            format,
            detail: RealtimeVideoFrameDetail::Auto,
            sequence,
            timestamp_ms: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RealtimeAudioEvent {
    SessionQueued {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        position: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        estimated_wait_s: Option<f64>,
    },
    SessionCreated {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        provider_session_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        prompt_length: Option<u64>,
    },
    SessionUpdated,
    Listening {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        kv_cache_length: Option<u64>,
    },
    InputSpeechStarted,
    InputSpeechStopped,
    OutputAudioDelta {
        text: String,
        audio: Vec<u8>,
        end_of_turn: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        kv_cache_length: Option<u64>,
    },
    OutputTranscriptDelta {
        delta: String,
    },
    ResponseDone,
    Closed {
        reason: String,
    },
    Error {
        code: String,
        message: String,
    },
    Raw {
        event_type: String,
        value: serde_json::Value,
    },
}

impl RealtimeAudioEvent {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::ResponseDone
                | Self::Closed { .. }
                | Self::Error { .. }
                | Self::OutputAudioDelta {
                    end_of_turn: true,
                    ..
                }
        )
    }
}

#[derive(Debug, Error)]
pub enum RealtimeAudioError {
    #[error("invalid realtime audio configuration: {detail}")]
    InvalidConfig { detail: String },
    #[error("realtime audio URL error: {detail}")]
    Url { detail: String },
    #[error("realtime audio header error: {detail}")]
    Header { detail: String },
    #[error("realtime audio websocket error: {detail}")]
    WebSocket { detail: String },
    #[error("timed out waiting for realtime audio event after {timeout_ms}ms")]
    Timeout { timeout_ms: u128 },
    #[error("realtime audio protocol error: {detail}")]
    Protocol { detail: String },
    #[error("realtime audio JSON error: {detail}")]
    Json { detail: String },
}

pub struct RealtimeAudioSession {
    config: RealtimeAudioSessionConfig,
    ws: RealtimeWs,
    pending: VecDeque<RealtimeAudioEvent>,
}

impl RealtimeAudioSession {
    pub async fn connect(config: RealtimeAudioSessionConfig) -> Result<Self, RealtimeAudioError> {
        config.validate()?;
        let url = config.websocket_url()?;
        let uri: Uri =
            url.parse()
                .map_err(|source: http::uri::InvalidUri| RealtimeAudioError::Url {
                    detail: source.to_string(),
                })?;
        let mut builder = ClientBuilder::from_uri(uri);
        if let Some(api_key) = config
            .api_key
            .as_deref()
            .map(str::trim)
            .filter(|api_key| !api_key.is_empty())
        {
            builder = builder
                .add_header(
                    AUTHORIZATION,
                    HeaderValue::from_str(&format!("Bearer {api_key}")).map_err(|source| {
                        RealtimeAudioError::Header {
                            detail: source.to_string(),
                        }
                    })?,
                )
                .map_err(|source| RealtimeAudioError::Header {
                    detail: source.to_string(),
                })?;
        }
        if let Some(identifier) = config
            .safety_identifier
            .as_deref()
            .map(str::trim)
            .filter(|identifier| !identifier.is_empty())
        {
            builder = builder
                .add_header(
                    HeaderName::from_static("openai-safety-identifier"),
                    HeaderValue::from_str(identifier).map_err(|source| {
                        RealtimeAudioError::Header {
                            detail: source.to_string(),
                        }
                    })?,
                )
                .map_err(|source| RealtimeAudioError::Header {
                    detail: source.to_string(),
                })?;
        }
        let connect = builder.connect();
        let (ws, _) = tokio::time::timeout(config.connect_timeout(), connect)
            .await
            .map_err(|_| RealtimeAudioError::Timeout {
                timeout_ms: config.connect_timeout().as_millis(),
            })?
            .map_err(|source| RealtimeAudioError::WebSocket {
                detail: source.to_string(),
            })?;
        let mut session = Self {
            config,
            ws,
            pending: VecDeque::new(),
        };
        match session.config.provider {
            RealtimeAudioProvider::OpenAi => {
                session
                    .send_json(openai_session_update_payload(&session.config))
                    .await?;
            }
            RealtimeAudioProvider::MiniCpmO => {
                session.wait_minicpm_ready_and_update().await?;
            }
        }
        Ok(session)
    }

    pub fn config(&self) -> &RealtimeAudioSessionConfig {
        &self.config
    }

    pub async fn send_audio(
        &mut self,
        chunk: &RealtimeAudioInputChunk,
    ) -> Result<(), RealtimeAudioError> {
        let payload = match self.config.provider {
            RealtimeAudioProvider::OpenAi => openai_input_audio_append_payload(chunk),
            RealtimeAudioProvider::MiniCpmO => minicpm_input_audio_append_payload(chunk),
        };
        self.send_json(payload).await
    }

    pub async fn send_video_frame(
        &mut self,
        frame: &RealtimeVideoFrameInput,
    ) -> Result<(), RealtimeAudioError> {
        if self.config.input_video.is_none() {
            return Err(RealtimeAudioError::InvalidConfig {
                detail: "realtime session is not configured for video input".to_string(),
            });
        }
        let payload = match self.config.provider {
            RealtimeAudioProvider::OpenAi => openai_input_video_frame_payload(frame),
            RealtimeAudioProvider::MiniCpmO => minicpm_input_video_frame_payload(frame),
        };
        self.send_json(payload).await
    }

    pub async fn flush_turn(&mut self) -> Result<(), RealtimeAudioError> {
        if self.config.provider == RealtimeAudioProvider::OpenAi
            && self.config.turn_detection == RealtimeAudioTurnDetection::Manual
        {
            self.send_json(serde_json::json!({ "type": "input_audio_buffer.commit" }))
                .await?;
            self.request_response().await?;
        }
        Ok(())
    }

    pub async fn request_response(&mut self) -> Result<(), RealtimeAudioError> {
        self.send_json(serde_json::json!({ "type": "response.create" }))
            .await
    }

    pub async fn close(&mut self) -> Result<(), RealtimeAudioError> {
        match self.config.provider {
            RealtimeAudioProvider::OpenAi => {
                self.ws
                    .close()
                    .await
                    .map_err(|source| RealtimeAudioError::WebSocket {
                        detail: source.to_string(),
                    })?;
            }
            RealtimeAudioProvider::MiniCpmO => {
                self.send_json(serde_json::json!({
                    "type": "session.close",
                    "reason": "user_stop"
                }))
                .await?;
            }
        }
        Ok(())
    }

    pub async fn receive_event(&mut self) -> Result<RealtimeAudioEvent, RealtimeAudioError> {
        self.receive_event_with_timeout(self.config.receive_timeout())
            .await
    }

    pub async fn receive_event_with_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<RealtimeAudioEvent, RealtimeAudioError> {
        if let Some(event) = self.pending.pop_front() {
            return Ok(event);
        }
        let message = tokio::time::timeout(timeout, self.ws.next())
            .await
            .map_err(|_| RealtimeAudioError::Timeout {
                timeout_ms: timeout.as_millis(),
            })?
            .ok_or_else(|| RealtimeAudioError::WebSocket {
                detail: "websocket stream ended".to_string(),
            })?
            .map_err(|source| RealtimeAudioError::WebSocket {
                detail: source.to_string(),
            })?;
        let text = message
            .as_text()
            .ok_or_else(|| RealtimeAudioError::Protocol {
                detail: "server sent a non-text websocket frame".to_string(),
            })?;
        parse_realtime_audio_event(self.config.provider, text)
    }

    async fn wait_minicpm_ready_and_update(&mut self) -> Result<(), RealtimeAudioError> {
        loop {
            let event = self.receive_event().await?;
            match &event {
                RealtimeAudioEvent::Raw { event_type, .. }
                    if event_type == "session.queue_done" || event_type == "queue_done" =>
                {
                    self.send_json(minicpm_session_update_payload(&self.config))
                        .await?;
                }
                RealtimeAudioEvent::SessionCreated { .. } => {
                    self.pending.push_back(event);
                    return Ok(());
                }
                RealtimeAudioEvent::Error { .. } | RealtimeAudioEvent::Closed { .. } => {
                    return Err(RealtimeAudioError::Protocol {
                        detail: format!("MiniCPM-o session closed before ready: {event:?}"),
                    });
                }
                _ => {}
            }
        }
    }

    async fn send_json(&mut self, value: serde_json::Value) -> Result<(), RealtimeAudioError> {
        self.ws
            .send(Message::text(value.to_string()))
            .await
            .map_err(|source| RealtimeAudioError::WebSocket {
                detail: source.to_string(),
            })
    }
}

pub fn openai_realtime_ws_url(base_url: &str, model: &str) -> Result<String, RealtimeAudioError> {
    let mut base = base_url.trim().trim_end_matches('/').to_string();
    if base.is_empty() {
        base = OPENAI_DEFAULT_REALTIME_BASE_URL.to_string();
    }
    let base = if base.starts_with("wss://") || base.starts_with("ws://") {
        base
    } else if let Some(rest) = base.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if let Some(rest) = base.strip_prefix("http://") {
        format!("ws://{rest}")
    } else {
        return Err(RealtimeAudioError::Url {
            detail: format!("unsupported OpenAI realtime base URL `{base_url}`"),
        });
    };
    let model = model.trim();
    let model = if model.is_empty() {
        OPENAI_REALTIME_DEFAULT_MODEL
    } else {
        model
    };
    Ok(format!("{base}/realtime?model={model}"))
}

pub fn minicpm_realtime_ws_url(base_url: &str) -> Result<String, RealtimeAudioError> {
    minicpm_realtime_ws_url_for_video(base_url, false)
}

pub fn minicpm_realtime_ws_url_for_video(
    base_url: &str,
    video: bool,
) -> Result<String, RealtimeAudioError> {
    let mut base = base_url.trim().trim_end_matches('/').to_string();
    if base.is_empty() {
        base = MINICPM_DEFAULT_REALTIME_BASE_URL.to_string();
    }
    let base = if base.starts_with("wss://") || base.starts_with("ws://") {
        base
    } else if let Some(rest) = base.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if let Some(rest) = base.strip_prefix("http://") {
        format!("ws://{rest}")
    } else {
        return Err(RealtimeAudioError::Url {
            detail: format!("unsupported MiniCPM-o realtime base URL `{base_url}`"),
        });
    };
    let mode = if video { "omni" } else { "audio" };
    Ok(format!("{base}/v1/realtime?mode={mode}"))
}

pub fn openai_session_update_payload(config: &RealtimeAudioSessionConfig) -> serde_json::Value {
    let mut session = serde_json::json!({
        "type": "realtime",
        "instructions": config.instructions,
        "output_modalities": ["audio"],
        "audio": {
            "input": {
                "format": {
                    "type": "audio/pcm",
                    "rate": config.input_sample_rate_hz
                }
            },
            "output": {
                "format": {
                    "type": "audio/pcm"
                },
                "voice": config.voice.as_deref().unwrap_or("marin")
            }
        },
        "reasoning": {
            "effort": "low"
        }
    });
    if config.turn_detection == RealtimeAudioTurnDetection::SemanticVad {
        session["audio"]["input"]["turn_detection"] = serde_json::json!({
            "type": "semantic_vad"
        });
    }
    serde_json::json!({
        "type": "session.update",
        "session": session
    })
}

pub fn minicpm_session_update_payload(config: &RealtimeAudioSessionConfig) -> serde_json::Value {
    let mut session = serde_json::json!({
        "instructions": config.instructions,
    });
    if let Some(voice) = config
        .voice
        .as_deref()
        .filter(|voice| !voice.trim().is_empty())
    {
        session["ref_audio"] = serde_json::json!(voice);
        session["tts_ref_audio"] = serde_json::json!(voice);
    }
    serde_json::json!({
        "type": "session.update",
        "session": session
    })
}

pub fn openai_input_audio_append_payload(chunk: &RealtimeAudioInputChunk) -> serde_json::Value {
    serde_json::json!({
        "type": "input_audio_buffer.append",
        "audio": base64::engine::general_purpose::STANDARD.encode(&chunk.audio),
    })
}

pub fn minicpm_input_audio_append_payload(chunk: &RealtimeAudioInputChunk) -> serde_json::Value {
    let mut payload = serde_json::json!({
        "type": "input_audio_buffer.append",
        "audio": base64::engine::general_purpose::STANDARD.encode(&chunk.audio),
    });
    if chunk.force_listen {
        payload["force_listen"] = serde_json::json!(true);
    }
    payload
}

pub fn openai_input_video_frame_payload(frame: &RealtimeVideoFrameInput) -> serde_json::Value {
    let image_url = format!(
        "data:{};base64,{}",
        frame.format.mime_type(),
        base64::engine::general_purpose::STANDARD.encode(&frame.frame)
    );
    serde_json::json!({
        "type": "conversation.item.create",
        "item": {
            "type": "message",
            "role": "user",
            "content": [{
                "type": "input_image",
                "image_url": image_url,
                "detail": frame.detail.as_config_value()
            }]
        }
    })
}

pub fn minicpm_input_video_frame_payload(frame: &RealtimeVideoFrameInput) -> serde_json::Value {
    let mut payload = serde_json::json!({
        "type": "input_image_buffer.append",
        "image": base64::engine::general_purpose::STANDARD.encode(&frame.frame),
        "mime_type": frame.format.mime_type(),
        "format": frame.format.as_config_value(),
        "sequence": frame.sequence,
        "detail": frame.detail.as_config_value(),
    });
    if let Some(timestamp_ms) = frame.timestamp_ms {
        payload["timestamp_ms"] = serde_json::json!(timestamp_ms);
    }
    payload
}

pub fn parse_realtime_audio_event(
    provider: RealtimeAudioProvider,
    text: &str,
) -> Result<RealtimeAudioEvent, RealtimeAudioError> {
    let value: serde_json::Value =
        serde_json::from_str(text).map_err(|source| RealtimeAudioError::Json {
            detail: source.to_string(),
        })?;
    realtime_audio_event_from_value(provider, value)
}

pub fn realtime_audio_event_from_value(
    provider: RealtimeAudioProvider,
    value: serde_json::Value,
) -> Result<RealtimeAudioEvent, RealtimeAudioError> {
    let event_type = value
        .get("type")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_string();
    let event = match event_type.as_str() {
        "session.queued" | "session.queue_update" | "queued" | "queue_update" => {
            RealtimeAudioEvent::SessionQueued {
                position: value.get("position").and_then(serde_json::Value::as_u64),
                estimated_wait_s: value
                    .get("estimated_wait_s")
                    .and_then(serde_json::Value::as_f64),
            }
        }
        "session.created" => RealtimeAudioEvent::SessionCreated {
            provider_session_id: value
                .get("session_id")
                .or_else(|| value.pointer("/session/id"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_string),
            prompt_length: value
                .get("prompt_length")
                .and_then(serde_json::Value::as_u64),
        },
        "session.updated" => RealtimeAudioEvent::SessionUpdated,
        "response.listen" => RealtimeAudioEvent::Listening {
            kv_cache_length: value
                .get("kv_cache_length")
                .and_then(serde_json::Value::as_u64),
        },
        "input_audio_buffer.speech_started" => RealtimeAudioEvent::InputSpeechStarted,
        "input_audio_buffer.speech_stopped" => RealtimeAudioEvent::InputSpeechStopped,
        "response.output_audio.delta" | "response.audio.delta" => {
            output_audio_delta(provider, &value)?
        }
        "response.output_audio_transcript.delta" => RealtimeAudioEvent::OutputTranscriptDelta {
            delta: value
                .get("delta")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_string(),
        },
        "response.output_audio.done" | "response.done" => RealtimeAudioEvent::ResponseDone,
        "session.closed" => RealtimeAudioEvent::Closed {
            reason: value
                .get("reason")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("closed")
                .to_string(),
        },
        "error" => {
            let error = value.get("error").unwrap_or(&value);
            RealtimeAudioEvent::Error {
                code: error
                    .get("code")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("error")
                    .to_string(),
                message: error
                    .get("message")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("realtime audio error")
                    .to_string(),
            }
        }
        _ => RealtimeAudioEvent::Raw { event_type, value },
    };
    Ok(event)
}

fn output_audio_delta(
    provider: RealtimeAudioProvider,
    value: &serde_json::Value,
) -> Result<RealtimeAudioEvent, RealtimeAudioError> {
    let audio_text = match provider {
        RealtimeAudioProvider::OpenAi => value
            .get("delta")
            .or_else(|| value.get("audio"))
            .and_then(serde_json::Value::as_str),
        RealtimeAudioProvider::MiniCpmO => value
            .get("audio")
            .or_else(|| value.get("delta"))
            .and_then(serde_json::Value::as_str),
    }
    .unwrap_or_default();
    let audio = base64::engine::general_purpose::STANDARD
        .decode(audio_text)
        .map_err(|source| RealtimeAudioError::Protocol {
            detail: format!("decode output audio delta: {source}"),
        })?;
    Ok(RealtimeAudioEvent::OutputAudioDelta {
        text: value
            .get("text")
            .or_else(|| value.get("transcript"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string(),
        audio,
        end_of_turn: value
            .get("end_of_turn")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
        kv_cache_length: value
            .get("kv_cache_length")
            .and_then(serde_json::Value::as_u64),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn openai_realtime_url_converts_https_base() {
        assert_eq!(
            openai_realtime_ws_url("https://api.openai.com/v1", "gpt-realtime-2").expect("url"),
            "wss://api.openai.com/v1/realtime?model=gpt-realtime-2"
        );
    }

    #[test]
    fn minicpm_realtime_url_selects_audio_mode() {
        assert_eq!(
            minicpm_realtime_ws_url("https://minicpmo45.modelbest.cn").expect("url"),
            "wss://minicpmo45.modelbest.cn/v1/realtime?mode=audio"
        );
        assert_eq!(
            minicpm_realtime_ws_url_for_video("https://minicpmo45.modelbest.cn", true)
                .expect("url"),
            "wss://minicpmo45.modelbest.cn/v1/realtime?mode=omni"
        );
    }

    #[test]
    fn openai_session_update_uses_realtime_2_audio_shape() {
        let mut config =
            RealtimeAudioSessionConfig::new(RealtimeAudioProvider::OpenAi, "gpt-realtime-2");
        config.turn_detection = RealtimeAudioTurnDetection::SemanticVad;
        let payload = openai_session_update_payload(&config);

        assert_eq!(payload["type"], "session.update");
        assert_eq!(payload["session"]["type"], "realtime");
        assert_eq!(payload["session"]["output_modalities"][0], "audio");
        assert_eq!(
            payload["session"]["audio"]["input"]["format"]["rate"],
            24_000
        );
        assert_eq!(
            payload["session"]["audio"]["input"]["turn_detection"]["type"],
            "semantic_vad"
        );
    }

    #[test]
    fn minicpm_audio_append_uses_audio_field_and_force_listen() {
        let mut chunk = RealtimeAudioInputChunk::new(vec![0, 1, 2, 3]);
        chunk.force_listen = true;
        let payload = minicpm_input_audio_append_payload(&chunk);

        assert_eq!(payload["type"], "input_audio_buffer.append");
        assert_eq!(payload["audio"], "AAECAw==");
        assert_eq!(payload["force_listen"], true);
    }

    #[test]
    fn openai_video_frame_uses_input_image_item() {
        let mut frame =
            RealtimeVideoFrameInput::new(vec![0, 1, 2, 3], RealtimeVideoFrameFormat::Jpeg, 7);
        frame.detail = RealtimeVideoFrameDetail::Low;
        let payload = openai_input_video_frame_payload(&frame);

        assert_eq!(payload["type"], "conversation.item.create");
        assert_eq!(payload["item"]["content"][0]["type"], "input_image");
        assert_eq!(payload["item"]["content"][0]["detail"], "low");
        assert_eq!(
            payload["item"]["content"][0]["image_url"],
            "data:image/jpeg;base64,AAECAw=="
        );
    }

    #[test]
    fn minicpm_video_frame_uses_image_buffer_append() {
        let mut frame =
            RealtimeVideoFrameInput::new(vec![0, 1, 2, 3], RealtimeVideoFrameFormat::Png, 7);
        frame.timestamp_ms = Some(1234);
        let payload = minicpm_input_video_frame_payload(&frame);

        assert_eq!(payload["type"], "input_image_buffer.append");
        assert_eq!(payload["image"], "AAECAw==");
        assert_eq!(payload["mime_type"], "image/png");
        assert_eq!(payload["sequence"], 7);
        assert_eq!(payload["timestamp_ms"], 1234);
    }

    #[test]
    fn parses_openai_output_audio_delta() {
        let event = realtime_audio_event_from_value(
            RealtimeAudioProvider::OpenAi,
            serde_json::json!({
                "type": "response.output_audio.delta",
                "delta": "AAECAw==",
                "transcript": "hello"
            }),
        )
        .expect("event");

        assert_eq!(
            event,
            RealtimeAudioEvent::OutputAudioDelta {
                text: "hello".to_string(),
                audio: vec![0, 1, 2, 3],
                end_of_turn: false,
                kv_cache_length: None,
            }
        );
    }

    #[test]
    fn parses_minicpm_output_audio_delta() {
        let event = realtime_audio_event_from_value(
            RealtimeAudioProvider::MiniCpmO,
            serde_json::json!({
                "type": "response.output_audio.delta",
                "audio": "AAECAw==",
                "text": "hello",
                "end_of_turn": true,
                "kv_cache_length": 42
            }),
        )
        .expect("event");

        assert_eq!(
            event,
            RealtimeAudioEvent::OutputAudioDelta {
                text: "hello".to_string(),
                audio: vec![0, 1, 2, 3],
                end_of_turn: true,
                kv_cache_length: Some(42),
            }
        );
    }
}
