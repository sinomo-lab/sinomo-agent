use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use http::header::{AUTHORIZATION, HeaderName};
use http::{HeaderValue, Uri};
use image::codecs::{jpeg::JpegEncoder, png::PngEncoder};
use image::imageops::FilterType;
use image::{DynamicImage, ImageEncoder, RgbImage, RgbaImage};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::Arc;
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
const DEFAULT_VIDEO_ENCODING_QUALITY: u8 = 85;
const DEFAULT_VIDEO_MAX_DIMENSION: u32 = 1_280;

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

    pub fn capabilities(self) -> crate::LanguageModelProviderCapabilities {
        match self {
            Self::OpenAi => crate::LanguageModelProviderCapabilities::openai(),
            Self::MiniCpmO => crate::LanguageModelProviderCapabilities::minicpm_realtime(),
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

/// JPEG/PNG encoding used after a raw frame crosses the model-provider boundary.
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
    /// JPEG quality on a 1-100 scale. PNG encoding is lossless.
    #[serde(default = "default_video_encoding_quality")]
    pub encoding_quality: u8,
    /// Longest encoded edge in pixels, or `None` to keep the source dimensions.
    #[serde(
        default = "default_video_max_dimension",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_dimension: Option<u32>,
    #[serde(default)]
    pub detail: RealtimeVideoFrameDetail,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_frame_rate_hz: Option<u32>,
}

impl Default for RealtimeVideoInputConfig {
    fn default() -> Self {
        Self {
            frame_format: RealtimeVideoFrameFormat::Jpeg,
            encoding_quality: DEFAULT_VIDEO_ENCODING_QUALITY,
            max_dimension: Some(DEFAULT_VIDEO_MAX_DIMENSION),
            detail: RealtimeVideoFrameDetail::Auto,
            max_frame_rate_hz: Some(2),
        }
    }
}

impl RealtimeVideoInputConfig {
    /// Minimum interval between frames sampled for model inference. Native
    /// capture and local preview remain independent of this outbound cadence.
    pub fn model_frame_interval(&self) -> Option<Duration> {
        self.max_frame_rate_hz
            .filter(|rate| *rate > 0)
            .map(|rate| Duration::from_nanos((1_000_000_000_u64 / u64::from(rate)).max(1)))
    }
}

const fn default_video_encoding_quality() -> u8 {
    DEFAULT_VIDEO_ENCODING_QUALITY
}

const fn default_video_max_dimension() -> Option<u32> {
    Some(DEFAULT_VIDEO_MAX_DIMENSION)
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum RealtimeAudioTurnDetection {
    #[default]
    Manual,
    ServerVad,
    SemanticVad,
}

/// Provider-neutral function tool exposed to a realtime audio model.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RealtimeAudioToolDefinition {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub parameters: serde_json::Value,
}

impl RealtimeAudioToolDefinition {
    pub fn new(name: impl Into<String>, parameters: serde_json::Value) -> Self {
        Self {
            name: name.into(),
            description: None,
            parameters,
        }
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
    /// Optional provider-side speech-to-text model used to produce stable user
    /// transcript events for history and regular-agent handoffs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_transcription_model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_video: Option<RealtimeVideoInputConfig>,
    pub input_sample_rate_hz: u32,
    pub output_sample_rate_hz: u32,
    pub input_format: RealtimeAudioSampleFormat,
    pub output_format: RealtimeAudioSampleFormat,
    #[serde(default)]
    pub turn_detection: RealtimeAudioTurnDetection,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<RealtimeAudioToolDefinition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<String>,
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
            input_transcription_model: (provider == RealtimeAudioProvider::OpenAi)
                .then(|| "gpt-4o-mini-transcribe".to_string()),
            input_video: None,
            input_sample_rate_hz,
            output_sample_rate_hz,
            input_format,
            output_format,
            turn_detection: RealtimeAudioTurnDetection::Manual,
            tools: Vec::new(),
            tool_choice: None,
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
        for tool in &self.tools {
            if tool.name.trim().is_empty() {
                return Err(RealtimeAudioError::InvalidConfig {
                    detail: "realtime function tool names must not be empty".to_string(),
                });
            }
            if !tool.parameters.is_object() {
                return Err(RealtimeAudioError::InvalidConfig {
                    detail: format!(
                        "realtime function tool `{}` parameters must be a JSON object schema",
                        tool.name
                    ),
                });
            }
        }
        if let Some(video) = &self.input_video {
            if !(1..=100).contains(&video.encoding_quality) {
                return Err(RealtimeAudioError::InvalidConfig {
                    detail: "realtime video encoding quality must be between 1 and 100".to_string(),
                });
            }
            if video.max_dimension == Some(0) {
                return Err(RealtimeAudioError::InvalidConfig {
                    detail: "realtime video maximum dimension must be greater than zero"
                        .to_string(),
                });
            }
            if video.max_frame_rate_hz == Some(0) {
                return Err(RealtimeAudioError::InvalidConfig {
                    detail: "realtime video maximum frame rate must be greater than zero"
                        .to_string(),
                });
            }
        }
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
                if !self.tools.is_empty() {
                    return Err(RealtimeAudioError::InvalidConfig {
                        detail: "MiniCPM-o realtime function tools are not supported".to_string(),
                    });
                }
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RealtimeVideoPixelFormat {
    Rgb8,
    Rgba8,
    Bgra8,
}

impl RealtimeVideoPixelFormat {
    pub const fn bytes_per_pixel(self) -> usize {
        match self {
            Self::Rgb8 => 3,
            Self::Rgba8 | Self::Bgra8 => 4,
        }
    }
}

/// A hot camera frame. Raw pixels deliberately do not implement serialization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RealtimeVideoFrameInput {
    pub pixels: Arc<[u8]>,
    pub pixel_format: RealtimeVideoPixelFormat,
    pub width: u32,
    pub height: u32,
    pub stride: usize,
    pub detail: RealtimeVideoFrameDetail,
    pub sequence: u64,
    pub timestamp_ms: Option<u64>,
}

impl RealtimeVideoFrameInput {
    pub fn new(
        pixels: Arc<[u8]>,
        pixel_format: RealtimeVideoPixelFormat,
        width: u32,
        height: u32,
        stride: usize,
        sequence: u64,
    ) -> Self {
        Self {
            pixels,
            pixel_format,
            width,
            height,
            stride,
            detail: RealtimeVideoFrameDetail::Auto,
            sequence,
            timestamp_ms: None,
        }
    }
}

/// An image that has already crossed an explicit JPEG/PNG encoding boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RealtimeEncodedVideoFrameInput {
    pub image: Arc<[u8]>,
    pub format: RealtimeVideoFrameFormat,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub detail: RealtimeVideoFrameDetail,
    pub sequence: u64,
    pub timestamp_ms: Option<u64>,
}

impl RealtimeEncodedVideoFrameInput {
    pub fn new(
        image: impl Into<Arc<[u8]>>,
        format: RealtimeVideoFrameFormat,
        sequence: u64,
    ) -> Self {
        Self {
            image: image.into(),
            format,
            width: None,
            height: None,
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
    InputSpeechStarted {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        audio_start_ms: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        item_id: Option<String>,
    },
    InputSpeechStopped {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        audio_end_ms: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        item_id: Option<String>,
    },
    InputTranscriptDelta {
        delta: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        item_id: Option<String>,
    },
    InputTranscriptDone {
        transcript: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        item_id: Option<String>,
    },
    OutputAudioDelta {
        text: String,
        audio: Vec<u8>,
        end_of_turn: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        item_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        kv_cache_length: Option<u64>,
    },
    OutputTranscriptDelta {
        delta: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        item_id: Option<String>,
    },
    OutputTranscriptDone {
        transcript: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        item_id: Option<String>,
    },
    ToolCallStarted {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        item_id: Option<String>,
        call_id: String,
        name: String,
    },
    ToolCallArgumentsDelta {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        item_id: Option<String>,
        call_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        delta: String,
    },
    ToolCallCompleted {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        item_id: Option<String>,
        call_id: String,
        name: String,
        arguments: String,
    },
    ResponseCancelled {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        response_id: Option<String>,
    },
    OutputTruncated {
        item_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        audio_end_ms: Option<u64>,
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

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum RealtimeAudioTranscriptRole {
    User,
    Assistant,
}

impl RealtimeAudioTranscriptRole {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Assistant => "assistant",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RealtimeAudioTranscriptEntry {
    pub role: RealtimeAudioTranscriptRole,
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_id: Option<String>,
}

/// Provider-neutral, bounded handoff cursor over a realtime conversation.
///
/// Deltas are coalesced into role-bearing entries and completion events replace
/// their partial text. `take_handoff_tail` returns each entry at most once so a
/// regular agent receives the new voice context without repeatedly replaying
/// the whole call.
#[derive(Clone, Debug, Default)]
pub struct RealtimeAudioTranscript {
    entries: Vec<RealtimeAudioTranscriptEntry>,
    handoff_cursor: usize,
    new_input_entry: bool,
    new_output_entry: bool,
}

impl RealtimeAudioTranscript {
    pub fn entries(&self) -> &[RealtimeAudioTranscriptEntry] {
        &self.entries
    }

    pub fn observe(&mut self, event: &RealtimeAudioEvent) {
        match event {
            RealtimeAudioEvent::InputSpeechStarted { .. } => self.new_input_entry = true,
            RealtimeAudioEvent::InputTranscriptDelta { delta, item_id } => {
                append_realtime_transcript_delta(
                    &mut self.entries,
                    RealtimeAudioTranscriptRole::User,
                    item_id.as_deref(),
                    delta,
                    self.new_input_entry,
                );
                self.new_input_entry = false;
            }
            RealtimeAudioEvent::InputTranscriptDone {
                transcript,
                item_id,
            } => {
                apply_realtime_transcript_done(
                    &mut self.entries,
                    RealtimeAudioTranscriptRole::User,
                    item_id.as_deref(),
                    transcript,
                    self.new_input_entry,
                );
                self.new_input_entry = false;
            }
            RealtimeAudioEvent::OutputTranscriptDelta { delta, item_id } => {
                append_realtime_transcript_delta(
                    &mut self.entries,
                    RealtimeAudioTranscriptRole::Assistant,
                    item_id.as_deref(),
                    delta,
                    self.new_output_entry,
                );
                self.new_output_entry = false;
            }
            RealtimeAudioEvent::OutputTranscriptDone {
                transcript,
                item_id,
            } => {
                apply_realtime_transcript_done(
                    &mut self.entries,
                    RealtimeAudioTranscriptRole::Assistant,
                    item_id.as_deref(),
                    transcript,
                    self.new_output_entry,
                );
                self.new_output_entry = false;
            }
            RealtimeAudioEvent::ResponseDone | RealtimeAudioEvent::ResponseCancelled { .. } => {
                self.new_output_entry = true;
            }
            _ => {}
        }
    }

    pub fn take_handoff_tail(&mut self) -> Vec<RealtimeAudioTranscriptEntry> {
        let tail = self.entries[self.handoff_cursor..].to_vec();
        self.handoff_cursor = self.entries.len();
        tail
    }
}

fn append_realtime_transcript_delta(
    entries: &mut Vec<RealtimeAudioTranscriptEntry>,
    role: RealtimeAudioTranscriptRole,
    item_id: Option<&str>,
    delta: &str,
    force_new: bool,
) {
    if delta.is_empty() {
        return;
    }
    if !force_new
        && let Some(entry) = entries.last_mut()
        && entry.role == role
        && realtime_transcript_item_matches(entry.item_id.as_deref(), item_id)
    {
        entry.text.push_str(delta);
        if entry.item_id.is_none() {
            entry.item_id = item_id.map(str::to_string);
        }
        return;
    }
    entries.push(RealtimeAudioTranscriptEntry {
        role,
        text: delta.to_string(),
        item_id: item_id.map(str::to_string),
    });
}

fn apply_realtime_transcript_done(
    entries: &mut Vec<RealtimeAudioTranscriptEntry>,
    role: RealtimeAudioTranscriptRole,
    item_id: Option<&str>,
    text: &str,
    force_new: bool,
) {
    if text.is_empty() {
        return;
    }
    if !force_new
        && let Some(entry) = entries.last_mut()
        && entry.role == role
        && realtime_transcript_item_matches(entry.item_id.as_deref(), item_id)
    {
        entry.text = text.to_string();
        if entry.item_id.is_none() {
            entry.item_id = item_id.map(str::to_string);
        }
        return;
    }
    entries.push(RealtimeAudioTranscriptEntry {
        role,
        text: text.to_string(),
        item_id: item_id.map(str::to_string),
    });
}

fn realtime_transcript_item_matches(left: Option<&str>, right: Option<&str>) -> bool {
    left.is_none() || right.is_none() || left == right
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
    #[error("realtime audio operation `{operation}` is not supported by provider `{provider}`")]
    UnsupportedOperation {
        operation: &'static str,
        provider: &'static str,
    },
    #[error("realtime audio JSON error: {detail}")]
    Json { detail: String },
    #[error("invalid realtime video frame: {detail}")]
    InvalidVideoFrame { detail: String },
    #[error("realtime video frame encoding failed: {detail}")]
    VideoEncode { detail: String },
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
        self.send_video_frame_and_return_encoded(frame)
            .await
            .map(|_| ())
    }

    /// Encodes and sends a raw frame, returning the exact encoded image used by the provider.
    pub async fn send_video_frame_and_return_encoded(
        &mut self,
        frame: &RealtimeVideoFrameInput,
    ) -> Result<RealtimeEncodedVideoFrameInput, RealtimeAudioError> {
        let video_config =
            self.config
                .input_video
                .clone()
                .ok_or_else(|| RealtimeAudioError::InvalidConfig {
                    detail: "realtime session is not configured for video input".to_string(),
                })?;
        let frame = frame.clone();
        let encoded =
            tokio::task::spawn_blocking(move || encode_realtime_video_frame(&frame, &video_config))
                .await
                .map_err(|source| RealtimeAudioError::VideoEncode {
                    detail: format!("video encoder task failed: {source}"),
                })??;
        self.send_encoded_video_frame(&encoded).await?;
        Ok(encoded)
    }

    /// Sends an explicitly pre-encoded JPEG/PNG image without decoding or re-encoding it.
    pub async fn send_encoded_video_frame(
        &mut self,
        frame: &RealtimeEncodedVideoFrameInput,
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

    /// Adds a function call result to the conversation and optionally asks the
    /// model to continue its response.
    pub async fn send_tool_result(
        &mut self,
        call_id: &str,
        output: &str,
        request_response: bool,
    ) -> Result<(), RealtimeAudioError> {
        self.require_openai("send_tool_result")?;
        self.send_json(openai_tool_result_payload(call_id, output))
            .await?;
        if request_response {
            self.request_response().await?;
        }
        Ok(())
    }

    /// Cancels the response currently being generated, if any.
    pub async fn cancel_response(&mut self) -> Result<(), RealtimeAudioError> {
        self.require_openai("cancel_response")?;
        self.send_json(openai_response_cancel_payload()).await
    }

    /// Removes unplayed audio from a model item after an interruption.
    pub async fn truncate_output(
        &mut self,
        item_id: &str,
        content_index: u64,
        audio_end_ms: u64,
    ) -> Result<(), RealtimeAudioError> {
        self.require_openai("truncate_output")?;
        self.send_json(openai_output_truncate_payload(
            item_id,
            content_index,
            audio_end_ms,
        ))
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

    fn require_openai(&self, operation: &'static str) -> Result<(), RealtimeAudioError> {
        if self.config.provider == RealtimeAudioProvider::OpenAi {
            Ok(())
        } else {
            Err(RealtimeAudioError::UnsupportedOperation {
                operation,
                provider: self.config.provider.provider_id(),
            })
        }
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
    match config.turn_detection {
        RealtimeAudioTurnDetection::Manual => {
            session["audio"]["input"]["turn_detection"] = serde_json::Value::Null;
        }
        RealtimeAudioTurnDetection::ServerVad => {
            session["audio"]["input"]["turn_detection"] = serde_json::json!({
                "type": "server_vad",
                "threshold": 0.5,
                "prefix_padding_ms": 300,
                "silence_duration_ms": 500,
                "create_response": true,
                "interrupt_response": true
            });
        }
        RealtimeAudioTurnDetection::SemanticVad => {
            session["audio"]["input"]["turn_detection"] = serde_json::json!({
                "type": "semantic_vad"
            });
        }
    }
    if let Some(model) = config
        .input_transcription_model
        .as_deref()
        .map(str::trim)
        .filter(|model| !model.is_empty())
    {
        session["audio"]["input"]["transcription"] = serde_json::json!({
            "model": model
        });
    }
    if !config.tools.is_empty() {
        session["tools"] = serde_json::Value::Array(
            config
                .tools
                .iter()
                .map(|tool| {
                    let mut value = serde_json::json!({
                        "type": "function",
                        "name": tool.name,
                        "parameters": tool.parameters
                    });
                    if let Some(description) = tool.description.as_deref() {
                        value["description"] = serde_json::json!(description);
                    }
                    value
                })
                .collect(),
        );
        session["tool_choice"] = serde_json::json!(
            config
                .tool_choice
                .as_deref()
                .filter(|choice| !choice.trim().is_empty())
                .unwrap_or("auto")
        );
    }
    serde_json::json!({
        "type": "session.update",
        "session": session
    })
}

pub fn openai_tool_result_payload(call_id: &str, output: &str) -> serde_json::Value {
    serde_json::json!({
        "type": "conversation.item.create",
        "item": {
            "type": "function_call_output",
            "call_id": call_id,
            "output": output
        }
    })
}

pub fn openai_response_cancel_payload() -> serde_json::Value {
    serde_json::json!({ "type": "response.cancel" })
}

pub fn openai_output_truncate_payload(
    item_id: &str,
    content_index: u64,
    audio_end_ms: u64,
) -> serde_json::Value {
    serde_json::json!({
        "type": "conversation.item.truncate",
        "item_id": item_id,
        "content_index": content_index,
        "audio_end_ms": audio_end_ms
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

fn encode_realtime_video_frame(
    frame: &RealtimeVideoFrameInput,
    config: &RealtimeVideoInputConfig,
) -> Result<RealtimeEncodedVideoFrameInput, RealtimeAudioError> {
    if !(1..=100).contains(&config.encoding_quality) {
        return Err(RealtimeAudioError::InvalidConfig {
            detail: "realtime video encoding quality must be between 1 and 100".to_string(),
        });
    }
    if config.max_dimension == Some(0) {
        return Err(RealtimeAudioError::InvalidConfig {
            detail: "realtime video maximum dimension must be greater than zero".to_string(),
        });
    }

    let mut image = raw_video_frame_image(frame)?;
    if let Some(max_dimension) = config.max_dimension {
        let source_max = image.width().max(image.height());
        if source_max > max_dimension {
            let width = ((u64::from(image.width()) * u64::from(max_dimension)
                + u64::from(source_max) / 2)
                / u64::from(source_max))
            .max(1) as u32;
            let height = ((u64::from(image.height()) * u64::from(max_dimension)
                + u64::from(source_max) / 2)
                / u64::from(source_max))
            .max(1) as u32;
            image = image.resize_exact(width, height, FilterType::Triangle);
        }
    }

    let encoded_width = image.width();
    let encoded_height = image.height();
    let mut encoded = Vec::new();
    match config.frame_format {
        RealtimeVideoFrameFormat::Jpeg => {
            let image = image.to_rgb8();
            JpegEncoder::new_with_quality(&mut encoded, config.encoding_quality)
                .encode(
                    image.as_raw(),
                    image.width(),
                    image.height(),
                    image::ExtendedColorType::Rgb8,
                )
                .map_err(|source| RealtimeAudioError::VideoEncode {
                    detail: source.to_string(),
                })?;
        }
        RealtimeVideoFrameFormat::Png => {
            let image = image.to_rgba8();
            PngEncoder::new(&mut encoded)
                .write_image(
                    image.as_raw(),
                    image.width(),
                    image.height(),
                    image::ExtendedColorType::Rgba8,
                )
                .map_err(|source| RealtimeAudioError::VideoEncode {
                    detail: source.to_string(),
                })?;
        }
    }

    let mut output =
        RealtimeEncodedVideoFrameInput::new(encoded, config.frame_format, frame.sequence);
    output.width = Some(encoded_width);
    output.height = Some(encoded_height);
    output.detail = frame.detail;
    output.timestamp_ms = frame.timestamp_ms;
    Ok(output)
}

fn raw_video_frame_image(
    frame: &RealtimeVideoFrameInput,
) -> Result<DynamicImage, RealtimeAudioError> {
    if frame.width == 0 || frame.height == 0 {
        return Err(RealtimeAudioError::InvalidVideoFrame {
            detail: "width and height must be greater than zero".to_string(),
        });
    }
    let width =
        usize::try_from(frame.width).map_err(|_| RealtimeAudioError::InvalidVideoFrame {
            detail: "frame width does not fit this platform".to_string(),
        })?;
    let height =
        usize::try_from(frame.height).map_err(|_| RealtimeAudioError::InvalidVideoFrame {
            detail: "frame height does not fit this platform".to_string(),
        })?;
    let row_bytes = width
        .checked_mul(frame.pixel_format.bytes_per_pixel())
        .ok_or_else(|| RealtimeAudioError::InvalidVideoFrame {
            detail: "frame row size overflowed".to_string(),
        })?;
    if frame.stride < row_bytes {
        return Err(RealtimeAudioError::InvalidVideoFrame {
            detail: format!(
                "stride {} is smaller than the packed row size {row_bytes}",
                frame.stride
            ),
        });
    }
    let required_len = frame
        .stride
        .checked_mul(height.saturating_sub(1))
        .and_then(|offset| offset.checked_add(row_bytes))
        .ok_or_else(|| RealtimeAudioError::InvalidVideoFrame {
            detail: "frame buffer size overflowed".to_string(),
        })?;
    if frame.pixels.len() < required_len {
        return Err(RealtimeAudioError::InvalidVideoFrame {
            detail: format!(
                "pixel buffer has {} bytes but the dimensions and stride require {required_len}",
                frame.pixels.len()
            ),
        });
    }
    let capacity =
        row_bytes
            .checked_mul(height)
            .ok_or_else(|| RealtimeAudioError::InvalidVideoFrame {
                detail: "packed frame size overflowed".to_string(),
            })?;
    let mut packed = Vec::with_capacity(capacity);
    for row in 0..height {
        let start = row * frame.stride;
        let source = &frame.pixels[start..start + row_bytes];
        if frame.pixel_format == RealtimeVideoPixelFormat::Bgra8 {
            for pixel in source.chunks_exact(4) {
                packed.extend_from_slice(&[pixel[2], pixel[1], pixel[0], pixel[3]]);
            }
        } else {
            packed.extend_from_slice(source);
        }
    }

    match frame.pixel_format {
        RealtimeVideoPixelFormat::Rgb8 => RgbImage::from_raw(frame.width, frame.height, packed)
            .map(DynamicImage::ImageRgb8)
            .ok_or_else(|| RealtimeAudioError::InvalidVideoFrame {
                detail: "could not construct RGB image from raw pixels".to_string(),
            }),
        RealtimeVideoPixelFormat::Rgba8 | RealtimeVideoPixelFormat::Bgra8 => {
            RgbaImage::from_raw(frame.width, frame.height, packed)
                .map(DynamicImage::ImageRgba8)
                .ok_or_else(|| RealtimeAudioError::InvalidVideoFrame {
                    detail: "could not construct RGBA image from raw pixels".to_string(),
                })
        }
    }
}

pub fn openai_input_video_frame_payload(
    frame: &RealtimeEncodedVideoFrameInput,
) -> serde_json::Value {
    let image_url = format!(
        "data:{};base64,{}",
        frame.format.mime_type(),
        base64::engine::general_purpose::STANDARD.encode(frame.image.as_ref())
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

pub fn minicpm_input_video_frame_payload(
    frame: &RealtimeEncodedVideoFrameInput,
) -> serde_json::Value {
    let mut payload = serde_json::json!({
        "type": "input_image_buffer.append",
        "image": base64::engine::general_purpose::STANDARD.encode(frame.image.as_ref()),
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
        "input_audio_buffer.speech_started" => RealtimeAudioEvent::InputSpeechStarted {
            audio_start_ms: value
                .get("audio_start_ms")
                .and_then(serde_json::Value::as_u64),
            item_id: optional_string(&value, "item_id"),
        },
        "input_audio_buffer.speech_stopped" => RealtimeAudioEvent::InputSpeechStopped {
            audio_end_ms: value
                .get("audio_end_ms")
                .and_then(serde_json::Value::as_u64),
            item_id: optional_string(&value, "item_id"),
        },
        "conversation.item.input_audio_transcription.delta" => {
            RealtimeAudioEvent::InputTranscriptDelta {
                delta: value
                    .get("delta")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                item_id: optional_string(&value, "item_id"),
            }
        }
        "conversation.item.input_audio_transcription.completed" => {
            RealtimeAudioEvent::InputTranscriptDone {
                transcript: value
                    .get("transcript")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                item_id: optional_string(&value, "item_id"),
            }
        }
        "response.output_audio.delta" | "response.audio.delta" => {
            output_audio_delta(provider, &value)?
        }
        "response.output_audio_transcript.delta" | "response.output_text.delta" => {
            RealtimeAudioEvent::OutputTranscriptDelta {
                delta: value
                    .get("delta")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                item_id: optional_string(&value, "item_id"),
            }
        }
        "response.output_audio_transcript.done" => RealtimeAudioEvent::OutputTranscriptDone {
            transcript: value
                .get("transcript")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_string(),
            item_id: optional_string(&value, "item_id"),
        },
        "response.output_text.done" => RealtimeAudioEvent::OutputTranscriptDone {
            transcript: value
                .get("text")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_string(),
            item_id: optional_string(&value, "item_id"),
        },
        "response.output_item.added"
            if value
                .pointer("/item/type")
                .and_then(serde_json::Value::as_str)
                == Some("function_call") =>
        {
            RealtimeAudioEvent::ToolCallStarted {
                item_id: value
                    .pointer("/item/id")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string),
                call_id: value
                    .pointer("/item/call_id")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                name: value
                    .pointer("/item/name")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
            }
        }
        "response.function_call_arguments.delta" => RealtimeAudioEvent::ToolCallArgumentsDelta {
            item_id: optional_string(&value, "item_id"),
            call_id: value
                .get("call_id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_string(),
            name: optional_string(&value, "name"),
            delta: value
                .get("delta")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_string(),
        },
        "response.function_call_arguments.done" => RealtimeAudioEvent::ToolCallCompleted {
            item_id: optional_string(&value, "item_id"),
            call_id: value
                .get("call_id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_string(),
            name: value
                .get("name")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_string(),
            arguments: value
                .get("arguments")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_string(),
        },
        "response.cancelled" => RealtimeAudioEvent::ResponseCancelled {
            response_id: value
                .get("response_id")
                .or_else(|| value.pointer("/response/id"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_string),
        },
        "response.done"
            if value
                .pointer("/response/status")
                .and_then(serde_json::Value::as_str)
                == Some("cancelled") =>
        {
            RealtimeAudioEvent::ResponseCancelled {
                response_id: value
                    .pointer("/response/id")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string),
            }
        }
        "conversation.item.truncated" => RealtimeAudioEvent::OutputTruncated {
            item_id: value
                .get("item_id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_string(),
            audio_end_ms: value
                .get("audio_end_ms")
                .and_then(serde_json::Value::as_u64),
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

fn optional_string(value: &serde_json::Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
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
        item_id: optional_string(value, "item_id"),
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
        assert_eq!(
            payload["session"]["audio"]["input"]["transcription"]["model"],
            "gpt-4o-mini-transcribe"
        );
    }

    #[test]
    fn openai_session_update_configures_tau_compatible_tools_and_server_vad() {
        let mut config =
            RealtimeAudioSessionConfig::new(RealtimeAudioProvider::OpenAi, "gpt-realtime-2");
        config.turn_detection = RealtimeAudioTurnDetection::ServerVad;
        let mut tool = RealtimeAudioToolDefinition::new(
            "lookup_order",
            serde_json::json!({
                "type": "object",
                "properties": {"order_id": {"type": "string"}},
                "required": ["order_id"]
            }),
        );
        tool.description = Some("Look up an order".to_string());
        config.tools.push(tool);

        let payload = openai_session_update_payload(&config);

        assert_eq!(
            payload["session"]["audio"]["input"]["turn_detection"],
            serde_json::json!({
                "type": "server_vad",
                "threshold": 0.5,
                "prefix_padding_ms": 300,
                "silence_duration_ms": 500,
                "create_response": true,
                "interrupt_response": true
            })
        );
        assert_eq!(payload["session"]["tools"][0]["type"], "function");
        assert_eq!(payload["session"]["tools"][0]["name"], "lookup_order");
        assert_eq!(payload["session"]["tool_choice"], "auto");
    }

    #[test]
    fn openai_tool_control_payloads_preserve_call_and_playback_identity() {
        assert_eq!(
            openai_tool_result_payload("call_123", r#"{"ok":true}"#),
            serde_json::json!({
                "type": "conversation.item.create",
                "item": {
                    "type": "function_call_output",
                    "call_id": "call_123",
                    "output": r#"{"ok":true}"#
                }
            })
        );
        assert_eq!(
            openai_response_cancel_payload(),
            serde_json::json!({"type": "response.cancel"})
        );
        assert_eq!(
            openai_output_truncate_payload("item_123", 0, 1500),
            serde_json::json!({
                "type": "conversation.item.truncate",
                "item_id": "item_123",
                "content_index": 0,
                "audio_end_ms": 1500
            })
        );
    }

    #[test]
    fn parses_tau_realtime_tool_and_interruption_events() {
        assert_eq!(
            realtime_audio_event_from_value(
                RealtimeAudioProvider::OpenAi,
                serde_json::json!({
                    "type": "response.function_call_arguments.done",
                    "item_id": "item_tool",
                    "call_id": "call_123",
                    "name": "lookup_order",
                    "arguments": "{\"order_id\":\"A-42\"}"
                }),
            )
            .expect("tool event"),
            RealtimeAudioEvent::ToolCallCompleted {
                item_id: Some("item_tool".to_string()),
                call_id: "call_123".to_string(),
                name: "lookup_order".to_string(),
                arguments: "{\"order_id\":\"A-42\"}".to_string(),
            }
        );
        assert_eq!(
            realtime_audio_event_from_value(
                RealtimeAudioProvider::OpenAi,
                serde_json::json!({
                    "type": "input_audio_buffer.speech_started",
                    "audio_start_ms": 825,
                    "item_id": "item_audio"
                }),
            )
            .expect("speech event"),
            RealtimeAudioEvent::InputSpeechStarted {
                audio_start_ms: Some(825),
                item_id: Some("item_audio".to_string()),
            }
        );
    }

    #[test]
    fn realtime_transcript_coalesces_deltas_and_returns_each_handoff_tail_once() {
        let mut transcript = RealtimeAudioTranscript::default();
        transcript.observe(&RealtimeAudioEvent::InputSpeechStarted {
            audio_start_ms: Some(10),
            item_id: Some("user-1".to_string()),
        });
        transcript.observe(&RealtimeAudioEvent::InputTranscriptDelta {
            delta: "edit ".to_string(),
            item_id: Some("user-1".to_string()),
        });
        transcript.observe(&RealtimeAudioEvent::InputTranscriptDelta {
            delta: "the file".to_string(),
            item_id: Some("user-1".to_string()),
        });
        transcript.observe(&RealtimeAudioEvent::InputTranscriptDone {
            transcript: "Edit the file.".to_string(),
            item_id: Some("user-1".to_string()),
        });
        transcript.observe(&RealtimeAudioEvent::OutputTranscriptDone {
            transcript: "I'll handle that.".to_string(),
            item_id: Some("assistant-1".to_string()),
        });

        assert_eq!(
            transcript.take_handoff_tail(),
            vec![
                RealtimeAudioTranscriptEntry {
                    role: RealtimeAudioTranscriptRole::User,
                    text: "Edit the file.".to_string(),
                    item_id: Some("user-1".to_string()),
                },
                RealtimeAudioTranscriptEntry {
                    role: RealtimeAudioTranscriptRole::Assistant,
                    text: "I'll handle that.".to_string(),
                    item_id: Some("assistant-1".to_string()),
                },
            ]
        );
        assert!(transcript.take_handoff_tail().is_empty());
    }

    #[test]
    fn parses_openai_input_transcript_completion() {
        let event = realtime_audio_event_from_value(
            RealtimeAudioProvider::OpenAi,
            serde_json::json!({
                "type": "conversation.item.input_audio_transcription.completed",
                "item_id": "user-7",
                "transcript": "Please inspect the build."
            }),
        )
        .expect("event");

        assert_eq!(
            event,
            RealtimeAudioEvent::InputTranscriptDone {
                transcript: "Please inspect the build.".to_string(),
                item_id: Some("user-7".to_string()),
            }
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
    fn video_input_config_deserializes_encoding_defaults() {
        let config: RealtimeVideoInputConfig = serde_json::from_value(serde_json::json!({
            "frame_format": "jpeg",
            "detail": "auto",
            "max_frame_rate_hz": 2
        }))
        .expect("video input config");

        assert_eq!(config.encoding_quality, DEFAULT_VIDEO_ENCODING_QUALITY);
        assert_eq!(config.max_dimension, Some(DEFAULT_VIDEO_MAX_DIMENSION));
        assert_eq!(
            config.model_frame_interval(),
            Some(Duration::from_millis(500))
        );
    }

    #[test]
    fn video_input_config_rejects_zero_model_frame_rate() {
        let mut config =
            RealtimeAudioSessionConfig::new(RealtimeAudioProvider::OpenAi, "gpt-realtime-2");
        config.input_video = Some(RealtimeVideoInputConfig {
            max_frame_rate_hz: Some(0),
            ..Default::default()
        });

        assert!(matches!(
            config.validate(),
            Err(RealtimeAudioError::InvalidConfig { detail })
                if detail.contains("maximum frame rate")
        ));
    }

    #[test]
    fn openai_video_frame_uses_input_image_item() {
        let mut frame = RealtimeEncodedVideoFrameInput::new(
            vec![0, 1, 2, 3],
            RealtimeVideoFrameFormat::Jpeg,
            7,
        );
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
            RealtimeEncodedVideoFrameInput::new(vec![0, 1, 2, 3], RealtimeVideoFrameFormat::Png, 7);
        frame.timestamp_ms = Some(1234);
        let payload = minicpm_input_video_frame_payload(&frame);

        assert_eq!(payload["type"], "input_image_buffer.append");
        assert_eq!(payload["image"], "AAECAw==");
        assert_eq!(payload["mime_type"], "image/png");
        assert_eq!(payload["sequence"], 7);
        assert_eq!(payload["timestamp_ms"], 1234);
    }

    #[test]
    fn raw_rgba_frame_is_resized_and_jpeg_encoded_before_payload_creation() {
        let mut frame = RealtimeVideoFrameInput::new(
            Arc::<[u8]>::from([255, 0, 0, 255].repeat(8)),
            RealtimeVideoPixelFormat::Rgba8,
            4,
            2,
            16,
            9,
        );
        frame.detail = RealtimeVideoFrameDetail::High;
        frame.timestamp_ms = Some(4500);
        let config = RealtimeVideoInputConfig {
            encoding_quality: 92,
            max_dimension: Some(2),
            ..Default::default()
        };

        let encoded = encode_realtime_video_frame(&frame, &config).expect("encoded frame");

        assert_eq!(encoded.format, RealtimeVideoFrameFormat::Jpeg);
        assert_eq!((encoded.width, encoded.height), (Some(2), Some(1)));
        assert_eq!(encoded.detail, RealtimeVideoFrameDetail::High);
        assert_eq!(encoded.sequence, 9);
        assert_eq!(encoded.timestamp_ms, Some(4500));
        assert!(encoded.image.starts_with(&[0xff, 0xd8, 0xff]));
        let decoded =
            image::load_from_memory_with_format(encoded.image.as_ref(), image::ImageFormat::Jpeg)
                .expect("decode JPEG");
        assert_eq!((decoded.width(), decoded.height()), (2, 1));

        let payload = openai_input_video_frame_payload(&encoded);
        assert_eq!(payload["item"]["content"][0]["detail"], "high");
        let image_url = payload["item"]["content"][0]["image_url"]
            .as_str()
            .expect("image URL");
        assert!(image_url.starts_with("data:image/jpeg;base64,/9j/"));
        let payload_image = base64::engine::general_purpose::STANDARD
            .decode(image_url.split_once(',').expect("data URL").1)
            .expect("payload image");
        assert_eq!(payload_image, encoded.image.as_ref());
    }

    #[test]
    fn bgra_frame_with_padded_stride_is_repacked_and_png_encoded() {
        let frame = RealtimeVideoFrameInput::new(
            Arc::<[u8]>::from(vec![10, 20, 30, 40, 0, 0, 0, 0, 50, 60, 70, 80]),
            RealtimeVideoPixelFormat::Bgra8,
            1,
            2,
            8,
            3,
        );
        let config = RealtimeVideoInputConfig {
            frame_format: RealtimeVideoFrameFormat::Png,
            max_dimension: None,
            ..Default::default()
        };

        let encoded = encode_realtime_video_frame(&frame, &config).expect("encoded frame");
        let decoded =
            image::load_from_memory_with_format(encoded.image.as_ref(), image::ImageFormat::Png)
                .expect("decode PNG")
                .to_rgba8();

        assert_eq!(decoded.as_raw(), &[30, 20, 10, 40, 70, 60, 50, 80]);
    }

    #[test]
    fn packed_rgb_frame_is_png_encoded() {
        let frame = RealtimeVideoFrameInput::new(
            Arc::<[u8]>::from(vec![12, 34, 56]),
            RealtimeVideoPixelFormat::Rgb8,
            1,
            1,
            3,
            1,
        );
        let config = RealtimeVideoInputConfig {
            frame_format: RealtimeVideoFrameFormat::Png,
            max_dimension: None,
            ..Default::default()
        };

        let encoded = encode_realtime_video_frame(&frame, &config).expect("encoded frame");
        let decoded =
            image::load_from_memory_with_format(encoded.image.as_ref(), image::ImageFormat::Png)
                .expect("decode PNG")
                .to_rgba8();

        assert_eq!(decoded.as_raw(), &[12, 34, 56, 255]);
    }

    #[test]
    fn raw_frame_rejects_stride_smaller_than_packed_row() {
        let frame = RealtimeVideoFrameInput::new(
            Arc::<[u8]>::from(vec![0; 6]),
            RealtimeVideoPixelFormat::Rgb8,
            2,
            1,
            5,
            0,
        );

        let error = encode_realtime_video_frame(&frame, &RealtimeVideoInputConfig::default())
            .expect_err("invalid stride");

        assert!(matches!(
            error,
            RealtimeAudioError::InvalidVideoFrame { .. }
        ));
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
                item_id: None,
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
                item_id: None,
                kv_cache_length: Some(42),
            }
        );
    }
}
