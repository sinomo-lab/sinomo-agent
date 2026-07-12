//! Audio perception event producer skeleton.

use serde::{Deserialize, Serialize};
use sinomo_gateway::{InputChunk, InputModality};
use sinomo_runtime::core::{
    Confidence, EventEnvelope, EventKind, EventSpan, LatencyClass, PrivacyClass, RoutingMetadata,
    RoutingScope, Salience, Source,
};
use sinomo_runtime::device;
use sinomo_runtime::events::EventId;
use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::Duration;
use thiserror::Error;

#[derive(Clone, Debug, Default)]
pub struct AudioPerceptionWorker;

impl AudioPerceptionWorker {
    pub fn new() -> Self {
        Self
    }

    pub fn process_chunk(&self, chunk: &InputChunk) -> Vec<EventEnvelope> {
        if chunk.modality != InputModality::Audio {
            return Vec::new();
        }

        let text = chunk
            .payload
            .get("transcript_hint")
            .and_then(|value| value.as_str())
            .unwrap_or("")
            .to_string();

        let span = chunk.span.unwrap_or(EventSpan::new(0, 0));
        vec![
            base_event(
                chunk,
                "audio.vad.speech",
                span,
                serde_json::json!({ "state": "speech" }),
            ),
            base_event(
                chunk,
                "speech.transcript.final",
                span,
                serde_json::json!({ "text": text, "source_chunk_id": chunk.chunk_id }),
            ),
        ]
    }
}

fn base_event(
    chunk: &InputChunk,
    kind: &str,
    span: EventSpan,
    payload: serde_json::Value,
) -> EventEnvelope {
    EventEnvelope::new(
        EventId::generate(),
        chunk.captured_at,
        chunk.origin_node_id.clone(),
        Source::new(format!("{}.perception", chunk.source.as_str())).expect("valid source"),
        EventKind::new(kind).expect("valid event kind"),
        Salience::new(0.62).expect("valid salience"),
        Confidence::new(0.91).expect("valid confidence"),
        RoutingMetadata::new(
            RoutingScope::Session,
            LatencyClass::Interactive,
            PrivacyClass::Derived,
        ),
    )
    .with_origin_device(chunk.origin_device_id.clone())
    .with_payload(payload)
    .with_session(
        chunk
            .session_id
            .clone()
            .expect("audio chunks carry sessions"),
    )
    .tap_span(span)
}

trait WithSpan {
    fn tap_span(self, span: EventSpan) -> Self;
}

impl WithSpan for EventEnvelope {
    fn tap_span(mut self, span: EventSpan) -> Self {
        self.span = Some(span);
        self
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum NativeAudioSampleFormat {
    Pcm16Le,
    F32Le,
}

impl NativeAudioSampleFormat {
    pub fn bytes_per_sample(self) -> usize {
        match self {
            Self::Pcm16Le => 2,
            Self::F32Le => 4,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NativeAudioCaptureConfig {
    pub device: Option<String>,
    pub sample_rate_hz: u32,
    pub sample_format: NativeAudioSampleFormat,
    pub chunk_ms: u64,
}

impl NativeAudioCaptureConfig {
    pub fn new(sample_rate_hz: u32, sample_format: NativeAudioSampleFormat) -> Self {
        Self {
            device: None,
            sample_rate_hz,
            sample_format,
            chunk_ms: 100,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct NativeAudioChunk {
    pub sequence: u64,
    pub sample_rate_hz: u32,
    pub sample_format: NativeAudioSampleFormat,
    pub channels: u16,
    pub captured_at_unix_ms: u128,
    pub audio: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NativeSpeakerPlaybackConfig {
    pub device: Option<String>,
    pub input_sample_rate_hz: u32,
    pub input_sample_format: NativeAudioSampleFormat,
}

impl NativeSpeakerPlaybackConfig {
    pub fn new(input_sample_rate_hz: u32, input_sample_format: NativeAudioSampleFormat) -> Self {
        Self {
            device: None,
            input_sample_rate_hz,
            input_sample_format,
        }
    }
}

pub struct NativeAudioCapture {
    raw: device::NativeMicrophoneCapture,
    state: Mutex<AudioCaptureState>,
}

impl NativeAudioCapture {
    pub fn recv_timeout(
        &self,
        timeout: Duration,
    ) -> Result<Option<NativeAudioChunk>, NativeAudioError> {
        if let Some(chunk) = self.next_pending_chunk()? {
            return Ok(Some(chunk));
        }
        let Some(raw) = self.raw.recv_timeout(timeout)? else {
            return Ok(None);
        };
        {
            let mut state = self
                .state
                .lock()
                .map_err(|_| NativeAudioError::AudioStream {
                    detail: "audio capture state lock poisoned".to_string(),
                })?;
            state.process_raw_chunk(raw)?;
        }
        self.next_pending_chunk()
    }

    fn next_pending_chunk(&self) -> Result<Option<NativeAudioChunk>, NativeAudioError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| NativeAudioError::AudioStream {
                detail: "audio capture state lock poisoned".to_string(),
            })?;
        Ok(state.pending_chunks.pop_front())
    }
}

pub struct NativeSpeakerPlayback {
    raw: device::NativeSpeakerPlayback,
    input_sample_rate_hz: u32,
    input_sample_format: NativeAudioSampleFormat,
}

impl NativeSpeakerPlayback {
    pub fn check_health(&self) -> Result<(), NativeAudioError> {
        self.raw.check_health()?;
        Ok(())
    }

    pub fn write(&self, bytes: &[u8]) -> Result<(), NativeAudioError> {
        let output_format = self.raw.format();
        let encoded = convert_model_audio_to_raw_output(
            bytes,
            self.input_sample_rate_hz,
            self.input_sample_format,
            output_format,
        );
        self.raw.write_raw_interleaved(&encoded)?;
        Ok(())
    }
}

#[derive(Debug, Error, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub enum NativeAudioError {
    #[error("native audio device error: {0}")]
    Device(#[from] device::NativeDeviceError),
    #[error("native audio configuration error: {detail}")]
    Config { detail: String },
    #[error("native audio stream error: {detail}")]
    AudioStream { detail: String },
}

pub fn start_native_microphone_capture(
    config: NativeAudioCaptureConfig,
) -> Result<NativeAudioCapture, NativeAudioError> {
    if config.sample_rate_hz == 0 {
        return Err(NativeAudioError::Config {
            detail: "microphone target sample rate must be greater than zero".to_string(),
        });
    }
    let raw = device::start_native_microphone_capture(device::NativeMicrophoneCaptureConfig {
        device: config.device.clone(),
        chunk_ms: config.chunk_ms,
    })?;
    let target_chunk_bytes =
        audio_chunk_bytes(config.sample_rate_hz, config.sample_format, config.chunk_ms);
    Ok(NativeAudioCapture {
        raw,
        state: Mutex::new(AudioCaptureState::new(
            config.sample_rate_hz,
            config.sample_format,
            target_chunk_bytes,
        )),
    })
}

pub fn start_native_speaker_playback(
    config: NativeSpeakerPlaybackConfig,
) -> Result<NativeSpeakerPlayback, NativeAudioError> {
    if config.input_sample_rate_hz == 0 {
        return Err(NativeAudioError::Config {
            detail: "speaker input sample rate must be greater than zero".to_string(),
        });
    }
    let raw = device::start_native_speaker_playback(device::NativeSpeakerPlaybackConfig {
        device: config.device.clone(),
    })?;
    Ok(NativeSpeakerPlayback {
        raw,
        input_sample_rate_hz: config.input_sample_rate_hz,
        input_sample_format: config.input_sample_format,
    })
}

/// Convert mono PCM into a stable 0..=1 microphone meter value. The logarithmic
/// mapping keeps ordinary speech visible while silence remains at zero.
pub fn normalized_audio_meter_level(bytes: &[u8], format: NativeAudioSampleFormat) -> f32 {
    let samples = decode_audio_samples(bytes, format);
    if samples.is_empty() {
        return 0.0;
    }
    let mean_square = samples
        .iter()
        .map(|sample| sample.clamp(-1.0, 1.0).powi(2))
        .sum::<f32>()
        / samples.len() as f32;
    let rms = mean_square.sqrt();
    if !rms.is_finite() || rms <= f32::EPSILON {
        return 0.0;
    }
    let dbfs = 20.0 * rms.log10();
    ((dbfs + 60.0) / 60.0).clamp(0.0, 1.0)
}

/// Generate a short, softly faded tone in the same mono format accepted by
/// [`NativeSpeakerPlayback`]. The caller owns playback lifetime and completion.
pub fn speaker_test_tone(
    sample_rate_hz: u32,
    format: NativeAudioSampleFormat,
    duration: Duration,
) -> Vec<u8> {
    let sample_rate_hz = sample_rate_hz.max(1);
    let sample_count = (duration.as_secs_f64() * sample_rate_hz as f64)
        .round()
        .max(1.0) as usize;
    let fade_samples = ((sample_rate_hz as f32 * 0.025).round() as usize)
        .max(1)
        .min((sample_count / 2).max(1));
    let mut out = Vec::with_capacity(sample_count * format.bytes_per_sample());
    for index in 0..sample_count {
        let attack = (index as f32 / fade_samples.max(1) as f32).clamp(0.0, 1.0);
        let release =
            ((sample_count - 1 - index) as f32 / fade_samples.max(1) as f32).clamp(0.0, 1.0);
        let envelope = attack.min(release);
        let phase = std::f32::consts::TAU * 440.0 * index as f32 / sample_rate_hz as f32;
        encode_audio_sample(phase.sin() * 0.24 * envelope, format, &mut out);
    }
    out
}

struct AudioCaptureState {
    target_sample_rate_hz: u32,
    target_format: NativeAudioSampleFormat,
    target_chunk_bytes: usize,
    pending_source: Vec<f32>,
    resample_cursor: f64,
    encoded: Vec<u8>,
    sequence: u64,
    pending_chunks: VecDeque<NativeAudioChunk>,
}

impl AudioCaptureState {
    fn new(
        target_sample_rate_hz: u32,
        target_format: NativeAudioSampleFormat,
        target_chunk_bytes: usize,
    ) -> Self {
        Self {
            target_sample_rate_hz,
            target_format,
            target_chunk_bytes,
            pending_source: Vec::new(),
            resample_cursor: 0.0,
            encoded: Vec::with_capacity(target_chunk_bytes.saturating_mul(2)),
            sequence: 0,
            pending_chunks: VecDeque::new(),
        }
    }

    fn process_raw_chunk(
        &mut self,
        chunk: device::NativeRawAudioChunk,
    ) -> Result<(), NativeAudioError> {
        let samples = decode_raw_audio_samples(&chunk.audio, chunk.format);
        let source_channels = chunk.format.channels.max(1) as usize;
        let mut mono = Vec::with_capacity(samples.len() / source_channels);
        for frame in samples.chunks(source_channels) {
            let sum: f32 = frame.iter().copied().sum();
            mono.push(sum / frame.len().max(1) as f32);
        }
        self.process_mono(
            &mono,
            chunk.format.sample_rate_hz,
            chunk.captured_at_unix_ms,
        );
        Ok(())
    }

    fn process_mono(
        &mut self,
        mono: &[f32],
        source_sample_rate_hz: u32,
        captured_at_unix_ms: u128,
    ) {
        if source_sample_rate_hz == 0 {
            return;
        }
        self.pending_source.extend_from_slice(mono);
        let step = source_sample_rate_hz as f64 / self.target_sample_rate_hz as f64;
        while self.resample_cursor + 1.0 < self.pending_source.len() as f64 {
            let index = self.resample_cursor.floor() as usize;
            let frac = (self.resample_cursor - index as f64) as f32;
            let a = self.pending_source[index];
            let b = self.pending_source[index + 1];
            self.push_target_sample(a + (b - a) * frac, captured_at_unix_ms);
            self.resample_cursor += step;
        }
        let drain = self.resample_cursor.floor() as usize;
        if drain > 0 {
            let drain = drain.min(self.pending_source.len().saturating_sub(1));
            self.pending_source.drain(0..drain);
            self.resample_cursor -= drain as f64;
        }
    }

    fn push_target_sample(&mut self, sample: f32, captured_at_unix_ms: u128) {
        encode_audio_sample(sample, self.target_format, &mut self.encoded);
        while self.encoded.len() >= self.target_chunk_bytes {
            let audio: Vec<u8> = self.encoded.drain(0..self.target_chunk_bytes).collect();
            self.pending_chunks.push_back(NativeAudioChunk {
                sequence: self.sequence,
                sample_rate_hz: self.target_sample_rate_hz,
                sample_format: self.target_format,
                channels: 1,
                captured_at_unix_ms,
                audio,
            });
            self.sequence = self.sequence.saturating_add(1);
        }
    }
}

fn decode_raw_audio_samples(bytes: &[u8], format: device::NativeRawAudioFormat) -> Vec<f32> {
    match format.sample_format {
        device::NativeRawAudioSampleFormat::F32Le => bytes
            .chunks_exact(4)
            .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
            .collect(),
        device::NativeRawAudioSampleFormat::I16Le => bytes
            .chunks_exact(2)
            .map(|chunk| i16::from_le_bytes([chunk[0], chunk[1]]) as f32 / i16::MAX as f32)
            .collect(),
        device::NativeRawAudioSampleFormat::U16Le => bytes
            .chunks_exact(2)
            .map(|chunk| (u16::from_le_bytes([chunk[0], chunk[1]]) as f32 - 32768.0) / 32768.0)
            .collect(),
    }
}

fn decode_audio_samples(bytes: &[u8], format: NativeAudioSampleFormat) -> Vec<f32> {
    match format {
        NativeAudioSampleFormat::Pcm16Le => bytes
            .chunks_exact(2)
            .map(|chunk| i16::from_le_bytes([chunk[0], chunk[1]]) as f32 / i16::MAX as f32)
            .collect(),
        NativeAudioSampleFormat::F32Le => bytes
            .chunks_exact(4)
            .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
            .collect(),
    }
}

fn encode_audio_sample(sample: f32, format: NativeAudioSampleFormat, out: &mut Vec<u8>) {
    match format {
        NativeAudioSampleFormat::Pcm16Le => {
            let value = (sample.clamp(-1.0, 1.0) * i16::MAX as f32) as i16;
            out.extend_from_slice(&value.to_le_bytes());
        }
        NativeAudioSampleFormat::F32Le => {
            out.extend_from_slice(&sample.clamp(-1.0, 1.0).to_le_bytes());
        }
    }
}

fn encode_raw_interleaved_audio(
    mono_samples: &[f32],
    format: device::NativeRawAudioFormat,
) -> Vec<u8> {
    let channels = format.channels.max(1) as usize;
    let mut out =
        Vec::with_capacity(mono_samples.len() * channels * format.sample_format.bytes_per_sample());
    for sample in mono_samples {
        for _ in 0..channels {
            match format.sample_format {
                device::NativeRawAudioSampleFormat::F32Le => {
                    out.extend_from_slice(&sample.clamp(-1.0, 1.0).to_le_bytes());
                }
                device::NativeRawAudioSampleFormat::I16Le => {
                    let value = (sample.clamp(-1.0, 1.0) * i16::MAX as f32) as i16;
                    out.extend_from_slice(&value.to_le_bytes());
                }
                device::NativeRawAudioSampleFormat::U16Le => {
                    let value = ((sample.clamp(-1.0, 1.0) * 32767.0) + 32768.0) as u16;
                    out.extend_from_slice(&value.to_le_bytes());
                }
            }
        }
    }
    out
}

fn convert_model_audio_to_raw_output(
    bytes: &[u8],
    input_sample_rate_hz: u32,
    input_format: NativeAudioSampleFormat,
    output_format: device::NativeRawAudioFormat,
) -> Vec<u8> {
    let decoded = decode_audio_samples(bytes, input_format);
    let resampled = resample_block(&decoded, input_sample_rate_hz, output_format.sample_rate_hz);
    encode_raw_interleaved_audio(&resampled, output_format)
}

fn resample_block(samples: &[f32], source_rate: u32, target_rate: u32) -> Vec<f32> {
    if samples.is_empty() || source_rate == 0 || target_rate == 0 {
        return Vec::new();
    }
    if source_rate == target_rate {
        return samples.to_vec();
    }
    let output_len =
        ((samples.len() as u64 * target_rate as u64) / source_rate as u64).max(1) as usize;
    let step = source_rate as f64 / target_rate as f64;
    let mut out = Vec::with_capacity(output_len);
    for index in 0..output_len {
        let src = index as f64 * step;
        let base = src.floor() as usize;
        let frac = (src - base as f64) as f32;
        let a = samples[base.min(samples.len() - 1)];
        let b = samples[(base + 1).min(samples.len() - 1)];
        out.push(a + (b - a) * frac);
    }
    out
}

fn audio_chunk_bytes(sample_rate_hz: u32, format: NativeAudioSampleFormat, chunk_ms: u64) -> usize {
    let sample_count = sample_rate_hz as u64 * chunk_ms.max(1);
    let sample_count = sample_count.saturating_add(999) / 1000;
    sample_count
        .max(1)
        .saturating_mul(format.bytes_per_sample() as u64)
        .min(usize::MAX as u64) as usize
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_gateway::{InputAdapter, SyntheticInputAdapter};
    use sinomo_runtime::device::DeviceProfile;
    use sinomo_runtime::events::SessionId;
    use sinomo_runtime::node::NodeId;

    #[test]
    fn audio_worker_emits_vad_and_transcript_events() {
        let device =
            DeviceProfile::mock_microphone(NodeId::from_legacy_named("node_mobile").expect("node"));
        let session = SessionId::from_legacy_named("sess_audio").expect("session");
        let mut adapter =
            SyntheticInputAdapter::audio_transcript(&device, session, "check the alert")
                .expect("adapter");
        let chunk = adapter.next_chunk().expect("chunk").expect("chunk");
        let events = AudioPerceptionWorker::new().process_chunk(&chunk);

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].origin_device_id, Some(device.device_id.clone()));
        assert_eq!(events[1].kind.as_str(), "speech.transcript.final");
    }

    #[test]
    fn native_audio_chunk_bytes_respects_format() {
        assert_eq!(
            audio_chunk_bytes(24_000, NativeAudioSampleFormat::Pcm16Le, 100),
            4_800
        );
        assert_eq!(
            audio_chunk_bytes(16_000, NativeAudioSampleFormat::F32Le, 100),
            6_400
        );
        assert_eq!(audio_chunk_bytes(1, NativeAudioSampleFormat::Pcm16Le, 1), 2);
    }

    #[test]
    fn native_audio_resample_block_changes_sample_count() {
        let out = resample_block(&[0.0, 1.0, 0.0, -1.0], 4, 8);

        assert_eq!(out.len(), 8);
        assert_eq!(out[0], 0.0);
    }

    #[test]
    fn native_audio_resample_block_handles_boundaries() {
        assert!(resample_block(&[], 24_000, 16_000).is_empty());
        assert!(resample_block(&[0.0, 1.0], 0, 16_000).is_empty());
        assert_eq!(
            resample_block(&[0.25, -0.25], 16_000, 16_000),
            vec![0.25, -0.25]
        );

        let downsampled = resample_block(&[0.0, 0.25, 0.5, 0.75], 4, 2);
        assert_eq!(downsampled, vec![0.0, 0.5]);
    }

    #[test]
    fn native_audio_capture_state_downmixes_stereo_raw_input_to_mono_target_chunks() {
        let raw_format = sinomo_runtime::device::NativeRawAudioFormat {
            sample_rate_hz: 1_000,
            channels: 2,
            sample_format: sinomo_runtime::device::NativeRawAudioSampleFormat::I16Le,
        };
        let mut raw = Vec::new();
        for (left, right) in [
            (1_000_i16, 3_000_i16),
            (2_000, 4_000),
            (3_000, 5_000),
            (4_000, 6_000),
            (5_000, 7_000),
            (6_000, 8_000),
        ] {
            raw.extend_from_slice(&left.to_le_bytes());
            raw.extend_from_slice(&right.to_le_bytes());
        }
        let chunk = sinomo_runtime::device::NativeRawAudioChunk {
            sequence: 0,
            format: raw_format,
            captured_at_unix_ms: 123,
            audio: raw,
        };
        let mut state = AudioCaptureState::new(1_000, NativeAudioSampleFormat::Pcm16Le, 4 * 2);

        state.process_raw_chunk(chunk).expect("process raw chunk");

        let chunk = state.pending_chunks.pop_front().expect("target chunk");
        assert_eq!(chunk.sequence, 0);
        assert_eq!(chunk.channels, 1);
        assert_eq!(chunk.sample_rate_hz, 1_000);
        assert_eq!(chunk.sample_format, NativeAudioSampleFormat::Pcm16Le);
        assert_eq!(chunk.captured_at_unix_ms, 123);
        assert_eq!(chunk.audio.len(), 8);
        let samples: Vec<i16> = chunk
            .audio
            .chunks_exact(2)
            .map(|bytes| i16::from_le_bytes([bytes[0], bytes[1]]))
            .collect();
        for (actual, expected) in samples.iter().zip([2_000_i16, 3_000, 4_000, 5_000]) {
            assert!(
                (*actual as i32 - expected as i32).abs() <= 1,
                "expected {expected}, got {actual}"
            );
        }
    }

    #[test]
    fn native_audio_encodes_mono_to_raw_interleaved_device_format() {
        let format = sinomo_runtime::device::NativeRawAudioFormat {
            sample_rate_hz: 48_000,
            channels: 2,
            sample_format: sinomo_runtime::device::NativeRawAudioSampleFormat::I16Le,
        };

        let encoded = encode_raw_interleaved_audio(&[0.5], format);

        assert_eq!(encoded.len(), 4);
        let left = i16::from_le_bytes([encoded[0], encoded[1]]);
        let right = i16::from_le_bytes([encoded[2], encoded[3]]);
        assert_eq!(left, right);
        assert!(left > 0);
    }

    #[test]
    fn native_audio_converts_model_audio_to_all_raw_speaker_formats() {
        let mut pcm16 = Vec::new();
        for sample in [-16_384_i16, 0, 16_384] {
            pcm16.extend_from_slice(&sample.to_le_bytes());
        }
        let output_i16 = sinomo_runtime::device::NativeRawAudioFormat {
            sample_rate_hz: 24_000,
            channels: 2,
            sample_format: sinomo_runtime::device::NativeRawAudioSampleFormat::I16Le,
        };
        let i16_bytes = convert_model_audio_to_raw_output(
            &pcm16,
            24_000,
            NativeAudioSampleFormat::Pcm16Le,
            output_i16,
        );
        assert_eq!(i16_bytes.len(), 12);
        assert_eq!(
            i16::from_le_bytes([i16_bytes[0], i16_bytes[1]]),
            i16::from_le_bytes([i16_bytes[2], i16_bytes[3]])
        );

        let mut f32le = Vec::new();
        for sample in [-0.5_f32, 0.0, 0.5] {
            f32le.extend_from_slice(&sample.to_le_bytes());
        }
        let output_f32 = sinomo_runtime::device::NativeRawAudioFormat {
            sample_rate_hz: 24_000,
            channels: 1,
            sample_format: sinomo_runtime::device::NativeRawAudioSampleFormat::F32Le,
        };
        let f32_bytes = convert_model_audio_to_raw_output(
            &f32le,
            24_000,
            NativeAudioSampleFormat::F32Le,
            output_f32,
        );
        assert_eq!(f32_bytes.len(), 12);
        assert_eq!(
            f32::from_le_bytes([f32_bytes[0], f32_bytes[1], f32_bytes[2], f32_bytes[3]]),
            -0.5
        );

        let output_u16 = sinomo_runtime::device::NativeRawAudioFormat {
            sample_rate_hz: 12_000,
            channels: 1,
            sample_format: sinomo_runtime::device::NativeRawAudioSampleFormat::U16Le,
        };
        let u16_bytes = convert_model_audio_to_raw_output(
            &pcm16,
            24_000,
            NativeAudioSampleFormat::Pcm16Le,
            output_u16,
        );
        assert_eq!(
            u16_bytes.len(),
            2,
            "24 kHz to 12 kHz downsamples three model samples to one raw sample"
        );
        let first = u16::from_le_bytes([u16_bytes[0], u16_bytes[1]]);
        assert!(
            first < 32_768,
            "negative model audio maps below unsigned silence"
        );
    }

    #[test]
    fn normalized_audio_meter_level_handles_silence_and_full_scale_pcm16() {
        assert_eq!(
            normalized_audio_meter_level(&[0, 0, 0, 0], NativeAudioSampleFormat::Pcm16Le),
            0.0
        );
        let mut full_scale = Vec::new();
        for sample in [i16::MAX, i16::MIN, i16::MAX, i16::MIN] {
            full_scale.extend_from_slice(&sample.to_le_bytes());
        }
        assert!(normalized_audio_meter_level(&full_scale, NativeAudioSampleFormat::Pcm16Le) > 0.99);
    }

    #[test]
    fn normalized_audio_meter_level_handles_f32_and_invalid_boundaries() {
        let mut speech = Vec::new();
        for sample in [0.1_f32, -0.1, 0.1, -0.1] {
            speech.extend_from_slice(&sample.to_le_bytes());
        }
        let level = normalized_audio_meter_level(&speech, NativeAudioSampleFormat::F32Le);
        assert!(
            (0.6..0.8).contains(&level),
            "unexpected speech level {level}"
        );
        assert_eq!(
            normalized_audio_meter_level(&[], NativeAudioSampleFormat::F32Le),
            0.0
        );
    }

    #[test]
    fn speaker_test_tone_has_expected_duration_amplitude_and_fades() {
        let tone = speaker_test_tone(
            24_000,
            NativeAudioSampleFormat::Pcm16Le,
            Duration::from_millis(500),
        );
        assert_eq!(tone.len(), 24_000);
        let samples = decode_audio_samples(&tone, NativeAudioSampleFormat::Pcm16Le);
        assert_eq!(samples.len(), 12_000);
        assert!(samples.iter().all(|sample| sample.abs() <= 0.25));
        assert_eq!(samples[0], 0.0);
        assert!(samples[samples.len() - 1].abs() < 0.001);
        assert!(samples.iter().any(|sample| sample.abs() > 0.2));
    }
}
