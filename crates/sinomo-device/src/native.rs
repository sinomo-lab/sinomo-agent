use crate::{DeviceId, DeviceKind, DevicePermission, DeviceProfile};
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use serde::{Deserialize, Serialize};
use sinomo_core::{CapabilityName, DeviceCapability};
use sinomo_node::NodeId;
use std::collections::VecDeque;
#[cfg(target_os = "linux")]
use std::fs;
#[cfg(target_os = "linux")]
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;

const DEFAULT_AUDIO_CHUNK_MS: u64 = 100;
const DEFAULT_CAMERA_FRAME_MS: u64 = 500;
const DEFAULT_CAMERA_WIDTH: u32 = 640;
const DEFAULT_CAMERA_HEIGHT: u32 = 480;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum NativeDeviceBackend {
    Cpal,
    V4l2,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NativeDeviceDescriptor {
    pub native_id: String,
    pub backend: NativeDeviceBackend,
    pub profile: DeviceProfile,
    pub is_default: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NativeDeviceInventory {
    pub node_id: NodeId,
    pub devices: Vec<NativeDeviceDescriptor>,
    #[serde(default)]
    pub issues: Vec<String>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum NativeRawAudioSampleFormat {
    F32Le,
    I16Le,
    U16Le,
}

impl NativeRawAudioSampleFormat {
    pub fn bytes_per_sample(self) -> usize {
        match self {
            Self::F32Le => 4,
            Self::I16Le | Self::U16Le => 2,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NativeRawAudioFormat {
    pub sample_rate_hz: u32,
    pub channels: u16,
    pub sample_format: NativeRawAudioSampleFormat,
}

impl NativeRawAudioFormat {
    pub fn bytes_per_frame(self) -> usize {
        self.sample_format
            .bytes_per_sample()
            .saturating_mul(self.channels.max(1) as usize)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NativeMicrophoneCaptureConfig {
    pub device: Option<String>,
    pub chunk_ms: u64,
}

impl Default for NativeMicrophoneCaptureConfig {
    fn default() -> Self {
        Self {
            device: None,
            chunk_ms: DEFAULT_AUDIO_CHUNK_MS,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct NativeRawAudioChunk {
    pub sequence: u64,
    pub format: NativeRawAudioFormat,
    pub captured_at_unix_ms: u128,
    pub audio: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NativeSpeakerPlaybackConfig {
    pub device: Option<String>,
}

impl Default for NativeSpeakerPlaybackConfig {
    fn default() -> Self {
        Self { device: None }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum NativeVideoFrameFormat {
    Jpeg,
}

impl NativeVideoFrameFormat {
    pub fn mime_type(self) -> &'static str {
        match self {
            Self::Jpeg => "image/jpeg",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NativeCameraCaptureConfig {
    pub device: Option<String>,
    pub width: u32,
    pub height: u32,
    pub frame_interval_ms: u64,
}

impl Default for NativeCameraCaptureConfig {
    fn default() -> Self {
        Self {
            device: None,
            width: DEFAULT_CAMERA_WIDTH,
            height: DEFAULT_CAMERA_HEIGHT,
            frame_interval_ms: DEFAULT_CAMERA_FRAME_MS,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct NativeVideoFrame {
    pub sequence: u64,
    pub format: NativeVideoFrameFormat,
    pub width: u32,
    pub height: u32,
    pub captured_at_unix_ms: u128,
    pub bytes: Vec<u8>,
}

pub struct NativeMicrophoneCapture {
    receiver: mpsc::Receiver<Result<NativeRawAudioChunk, NativeDeviceError>>,
    format: NativeRawAudioFormat,
    _stream: cpal::Stream,
}

impl NativeMicrophoneCapture {
    pub fn format(&self) -> NativeRawAudioFormat {
        self.format
    }

    pub fn recv_timeout(
        &self,
        timeout: Duration,
    ) -> Result<Option<NativeRawAudioChunk>, NativeDeviceError> {
        match self.receiver.recv_timeout(timeout) {
            Ok(Ok(chunk)) => Ok(Some(chunk)),
            Ok(Err(error)) => Err(error),
            Err(mpsc::RecvTimeoutError::Timeout) => Ok(None),
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(NativeDeviceError::AudioStream {
                detail: "microphone stream stopped".to_string(),
            }),
        }
    }
}

pub struct NativeSpeakerPlayback {
    queue: Arc<Mutex<SpeakerQueue>>,
    format: NativeRawAudioFormat,
    _stream: cpal::Stream,
}

impl NativeSpeakerPlayback {
    pub fn format(&self) -> NativeRawAudioFormat {
        self.format
    }

    pub fn write_raw_interleaved(&self, bytes: &[u8]) -> Result<(), NativeDeviceError> {
        let mut queue = self
            .queue
            .lock()
            .map_err(|_| NativeDeviceError::AudioStream {
                detail: "speaker queue lock poisoned".to_string(),
            })?;
        queue.push_bytes(bytes);
        Ok(())
    }
}

pub struct NativeCameraCapture {
    receiver: mpsc::Receiver<Result<NativeVideoFrame, NativeDeviceError>>,
    stop: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
}

impl NativeCameraCapture {
    pub fn recv_timeout(
        &self,
        timeout: Duration,
    ) -> Result<Option<NativeVideoFrame>, NativeDeviceError> {
        match self.receiver.recv_timeout(timeout) {
            Ok(Ok(frame)) => Ok(Some(frame)),
            Ok(Err(error)) => Err(error),
            Err(mpsc::RecvTimeoutError::Timeout) => Ok(None),
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(NativeDeviceError::Camera {
                detail: "camera stream stopped".to_string(),
            }),
        }
    }
}

impl Drop for NativeCameraCapture {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

#[derive(Debug, Error, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub enum NativeDeviceError {
    #[error("native device is unsupported: {detail}")]
    Unsupported { detail: String },
    #[error("native audio device error: {detail}")]
    AudioDevice { detail: String },
    #[error("native audio stream error: {detail}")]
    AudioStream { detail: String },
    #[error("native camera error: {detail}")]
    Camera { detail: String },
    #[error("native device IO error at {path}: {detail}")]
    Io { path: String, detail: String },
}

pub fn enumerate_native_devices(node_id: NodeId) -> NativeDeviceInventory {
    let mut inventory = NativeDeviceInventory {
        node_id: node_id.clone(),
        devices: Vec::new(),
        issues: Vec::new(),
    };
    enumerate_cpal_devices(&node_id, &mut inventory);
    enumerate_camera_devices(&node_id, &mut inventory);
    inventory
}

pub fn start_native_microphone_capture(
    config: NativeMicrophoneCaptureConfig,
) -> Result<NativeMicrophoneCapture, NativeDeviceError> {
    let host = cpal::default_host();
    let device = select_cpal_device(
        host.input_devices(),
        host.default_input_device(),
        "cpal:input",
        &config.device,
    )?;
    let supported =
        device
            .default_input_config()
            .map_err(|source| NativeDeviceError::AudioDevice {
                detail: source.to_string(),
            })?;
    let stream_config = supported.config();
    let format = NativeRawAudioFormat {
        sample_rate_hz: stream_config.sample_rate.0,
        channels: stream_config.channels.max(1),
        sample_format: native_raw_audio_format(supported.sample_format())?,
    };
    let target_chunk_bytes = raw_audio_chunk_bytes(format, config.chunk_ms);
    let (tx, rx) = mpsc::channel();
    let state = Arc::new(Mutex::new(AudioCaptureState::new(
        tx,
        format,
        target_chunk_bytes,
    )));
    let stream = match supported.sample_format() {
        cpal::SampleFormat::F32 => build_input_stream::<f32>(&device, &stream_config, state)?,
        cpal::SampleFormat::I16 => build_input_stream::<i16>(&device, &stream_config, state)?,
        cpal::SampleFormat::U16 => build_input_stream::<u16>(&device, &stream_config, state)?,
        format => {
            return Err(NativeDeviceError::AudioDevice {
                detail: format!("unsupported microphone sample format {format:?}"),
            });
        }
    };
    stream
        .play()
        .map_err(|source| NativeDeviceError::AudioStream {
            detail: source.to_string(),
        })?;
    Ok(NativeMicrophoneCapture {
        receiver: rx,
        format,
        _stream: stream,
    })
}

pub fn start_native_speaker_playback(
    config: NativeSpeakerPlaybackConfig,
) -> Result<NativeSpeakerPlayback, NativeDeviceError> {
    let host = cpal::default_host();
    let device = select_cpal_device(
        host.output_devices(),
        host.default_output_device(),
        "cpal:output",
        &config.device,
    )?;
    let supported =
        device
            .default_output_config()
            .map_err(|source| NativeDeviceError::AudioDevice {
                detail: source.to_string(),
            })?;
    let stream_config = supported.config();
    let format = NativeRawAudioFormat {
        sample_rate_hz: stream_config.sample_rate.0,
        channels: stream_config.channels.max(1),
        sample_format: native_raw_audio_format(supported.sample_format())?,
    };
    let queue = Arc::new(Mutex::new(SpeakerQueue::new(format)));
    let stream = match supported.sample_format() {
        cpal::SampleFormat::F32 => {
            build_output_stream::<f32>(&device, &stream_config, queue.clone())?
        }
        cpal::SampleFormat::I16 => {
            build_output_stream::<i16>(&device, &stream_config, queue.clone())?
        }
        cpal::SampleFormat::U16 => {
            build_output_stream::<u16>(&device, &stream_config, queue.clone())?
        }
        format => {
            return Err(NativeDeviceError::AudioDevice {
                detail: format!("unsupported speaker sample format {format:?}"),
            });
        }
    };
    stream
        .play()
        .map_err(|source| NativeDeviceError::AudioStream {
            detail: source.to_string(),
        })?;
    Ok(NativeSpeakerPlayback {
        queue,
        format,
        _stream: stream,
    })
}

pub fn start_native_camera_capture(
    config: NativeCameraCaptureConfig,
) -> Result<NativeCameraCapture, NativeDeviceError> {
    start_platform_camera_capture(config)
}

fn enumerate_cpal_devices(node_id: &NodeId, inventory: &mut NativeDeviceInventory) {
    let host = cpal::default_host();
    let default_input = host
        .default_input_device()
        .and_then(|device| device.name().ok());
    match host.input_devices() {
        Ok(devices) => {
            for (index, device) in devices.enumerate() {
                let name = device
                    .name()
                    .unwrap_or_else(|_| format!("Microphone {}", index + 1));
                let native_id = format!("cpal:input:{index}:{}", stable_component(&name));
                inventory.devices.push(native_descriptor(
                    node_id,
                    native_id,
                    NativeDeviceBackend::Cpal,
                    name.clone(),
                    DeviceKind::Microphone,
                    default_input.as_deref() == Some(name.as_str()),
                    vec!["input.audio.capture", "input.audio.raw"],
                    vec![DevicePermission::Observe, DevicePermission::CaptureRaw],
                    None,
                ));
            }
        }
        Err(source) => inventory
            .issues
            .push(format!("microphone enumeration failed: {source}")),
    }

    let default_output = host
        .default_output_device()
        .and_then(|device| device.name().ok());
    match host.output_devices() {
        Ok(devices) => {
            for (index, device) in devices.enumerate() {
                let name = device
                    .name()
                    .unwrap_or_else(|_| format!("Speaker {}", index + 1));
                let native_id = format!("cpal:output:{index}:{}", stable_component(&name));
                inventory.devices.push(native_descriptor(
                    node_id,
                    native_id,
                    NativeDeviceBackend::Cpal,
                    name.clone(),
                    DeviceKind::Speaker,
                    default_output.as_deref() == Some(name.as_str()),
                    vec!["output.audio.playback", "output.audio.raw"],
                    vec![DevicePermission::EmitOutput],
                    None,
                ));
            }
        }
        Err(source) => inventory
            .issues
            .push(format!("speaker enumeration failed: {source}")),
    }
}

fn enumerate_camera_devices(node_id: &NodeId, inventory: &mut NativeDeviceInventory) {
    #[cfg(target_os = "linux")]
    {
        let entries = match fs::read_dir("/dev") {
            Ok(entries) => entries,
            Err(source) => {
                inventory
                    .issues
                    .push(format!("camera enumeration failed: {source}"));
                return;
            }
        };
        let mut paths: Vec<PathBuf> = entries
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("video"))
            })
            .collect();
        paths.sort();
        for (index, path) in paths.into_iter().enumerate() {
            let name = camera_display_name(&path).unwrap_or_else(|| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or("camera")
                    .to_string()
            });
            let native_id = path.display().to_string();
            inventory.devices.push(native_descriptor(
                node_id,
                native_id,
                NativeDeviceBackend::V4l2,
                name,
                DeviceKind::Camera,
                index == 0,
                vec!["input.video.capture", "input.video.raw"],
                vec![DevicePermission::Observe, DevicePermission::CaptureRaw],
                Some(path.display().to_string()),
            ));
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = node_id;
        inventory
            .issues
            .push("native camera enumeration is not implemented on this OS".to_string());
    }
}

fn native_descriptor(
    node_id: &NodeId,
    native_id: String,
    backend: NativeDeviceBackend,
    display_name: String,
    kind: DeviceKind,
    is_default: bool,
    capabilities: Vec<&str>,
    permissions: Vec<DevicePermission>,
    detail: Option<String>,
) -> NativeDeviceDescriptor {
    let mut profile =
        DeviceProfile::new(node_id.clone(), display_name, kind).with_device_id(DeviceId::derived(
            "native-device",
            format!("{backend:?}:{native_id}").as_bytes(),
        ));
    profile.capabilities = capabilities
        .into_iter()
        .map(|name| DeviceCapability::new(CapabilityName::new(name).expect("valid capability")))
        .collect();
    profile.permissions = permissions;
    NativeDeviceDescriptor {
        native_id,
        backend,
        profile,
        is_default,
        detail,
    }
}

fn select_cpal_device<I>(
    devices: Result<I, cpal::DevicesError>,
    default_device: Option<cpal::Device>,
    native_id_prefix: &str,
    selector: &Option<String>,
) -> Result<cpal::Device, NativeDeviceError>
where
    I: Iterator<Item = cpal::Device>,
{
    let Some(selector) = selector
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return default_device.ok_or_else(|| NativeDeviceError::AudioDevice {
            detail: "no default native audio device is available".to_string(),
        });
    };
    let devices = devices.map_err(|source| NativeDeviceError::AudioDevice {
        detail: source.to_string(),
    })?;
    let selector_lower = selector.to_ascii_lowercase();
    for (index, device) in devices.enumerate() {
        let name = device.name().unwrap_or_default();
        let component = stable_component(&name);
        let native_id = format!("{native_id_prefix}:{index}:{component}");
        let legacy_native_id = format!("cpal:{index}:{component}");
        if name == selector
            || name.to_ascii_lowercase().contains(&selector_lower)
            || native_id == selector
            || legacy_native_id == selector
        {
            return Ok(device);
        }
    }
    Err(NativeDeviceError::AudioDevice {
        detail: format!("native audio device `{selector}` was not found"),
    })
}

fn native_raw_audio_format(
    format: cpal::SampleFormat,
) -> Result<NativeRawAudioSampleFormat, NativeDeviceError> {
    match format {
        cpal::SampleFormat::F32 => Ok(NativeRawAudioSampleFormat::F32Le),
        cpal::SampleFormat::I16 => Ok(NativeRawAudioSampleFormat::I16Le),
        cpal::SampleFormat::U16 => Ok(NativeRawAudioSampleFormat::U16Le),
        format => Err(NativeDeviceError::AudioDevice {
            detail: format!("unsupported native audio sample format {format:?}"),
        }),
    }
}

fn build_input_stream<T>(
    device: &cpal::Device,
    config: &cpal::StreamConfig,
    state: Arc<Mutex<AudioCaptureState>>,
) -> Result<cpal::Stream, NativeDeviceError>
where
    T: RawInputSample + cpal::SizedSample + Send + 'static,
{
    let error_state = state.clone();
    device
        .build_input_stream(
            config,
            move |data: &[T], _| {
                if let Ok(mut state) = state.lock() {
                    state.process_samples(data);
                }
            },
            move |error| {
                if let Ok(state) = error_state.lock() {
                    let _ = state.tx.send(Err(NativeDeviceError::AudioStream {
                        detail: error.to_string(),
                    }));
                }
            },
            None,
        )
        .map_err(|source| NativeDeviceError::AudioStream {
            detail: source.to_string(),
        })
}

fn build_output_stream<T>(
    device: &cpal::Device,
    config: &cpal::StreamConfig,
    queue: Arc<Mutex<SpeakerQueue>>,
) -> Result<cpal::Stream, NativeDeviceError>
where
    T: RawOutputSample + cpal::SizedSample + Send + 'static,
{
    device
        .build_output_stream(
            config,
            move |data: &mut [T], _| {
                if let Ok(mut queue) = queue.lock() {
                    queue.fill(data);
                } else {
                    for sample in data.iter_mut() {
                        *sample = T::silence();
                    }
                }
            },
            move |_error| {},
            None,
        )
        .map_err(|source| NativeDeviceError::AudioStream {
            detail: source.to_string(),
        })
}

trait RawInputSample: Copy {
    fn encode_samples(samples: &[Self], out: &mut Vec<u8>);
}

impl RawInputSample for f32 {
    fn encode_samples(samples: &[Self], out: &mut Vec<u8>) {
        for sample in samples {
            out.extend_from_slice(&sample.to_le_bytes());
        }
    }
}

impl RawInputSample for i16 {
    fn encode_samples(samples: &[Self], out: &mut Vec<u8>) {
        for sample in samples {
            out.extend_from_slice(&sample.to_le_bytes());
        }
    }
}

impl RawInputSample for u16 {
    fn encode_samples(samples: &[Self], out: &mut Vec<u8>) {
        for sample in samples {
            out.extend_from_slice(&sample.to_le_bytes());
        }
    }
}

trait RawOutputSample: Copy {
    const BYTES: usize;

    fn silence() -> Self;
    fn from_le_bytes(bytes: &[u8]) -> Self;
}

impl RawOutputSample for f32 {
    const BYTES: usize = 4;

    fn silence() -> Self {
        0.0
    }

    fn from_le_bytes(bytes: &[u8]) -> Self {
        f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
    }
}

impl RawOutputSample for i16 {
    const BYTES: usize = 2;

    fn silence() -> Self {
        0
    }

    fn from_le_bytes(bytes: &[u8]) -> Self {
        i16::from_le_bytes([bytes[0], bytes[1]])
    }
}

impl RawOutputSample for u16 {
    const BYTES: usize = 2;

    fn silence() -> Self {
        32768
    }

    fn from_le_bytes(bytes: &[u8]) -> Self {
        u16::from_le_bytes([bytes[0], bytes[1]])
    }
}

struct AudioCaptureState {
    tx: mpsc::Sender<Result<NativeRawAudioChunk, NativeDeviceError>>,
    format: NativeRawAudioFormat,
    target_chunk_bytes: usize,
    pending: Vec<u8>,
    sequence: u64,
}

impl AudioCaptureState {
    fn new(
        tx: mpsc::Sender<Result<NativeRawAudioChunk, NativeDeviceError>>,
        format: NativeRawAudioFormat,
        target_chunk_bytes: usize,
    ) -> Self {
        Self {
            tx,
            format,
            target_chunk_bytes,
            pending: Vec::with_capacity(target_chunk_bytes.saturating_mul(2)),
            sequence: 0,
        }
    }

    fn process_samples<T: RawInputSample>(&mut self, data: &[T]) {
        T::encode_samples(data, &mut self.pending);
        while self.pending.len() >= self.target_chunk_bytes {
            let audio: Vec<u8> = self.pending.drain(0..self.target_chunk_bytes).collect();
            let chunk = NativeRawAudioChunk {
                sequence: self.sequence,
                format: self.format,
                captured_at_unix_ms: now_unix_ms(),
                audio,
            };
            self.sequence = self.sequence.saturating_add(1);
            let _ = self.tx.send(Ok(chunk));
        }
    }
}

struct SpeakerQueue {
    format: NativeRawAudioFormat,
    bytes: VecDeque<u8>,
}

impl SpeakerQueue {
    fn new(format: NativeRawAudioFormat) -> Self {
        Self {
            format,
            bytes: VecDeque::new(),
        }
    }

    fn push_bytes(&mut self, bytes: &[u8]) {
        let frame_bytes = self.format.bytes_per_frame().max(1);
        let aligned = bytes.len() - (bytes.len() % frame_bytes);
        self.bytes.extend(bytes[..aligned].iter().copied());
    }

    fn fill<T: RawOutputSample>(&mut self, data: &mut [T]) {
        for sample in data {
            if self.bytes.len() >= T::BYTES {
                let bytes: Vec<u8> = self.bytes.drain(0..T::BYTES).collect();
                *sample = T::from_le_bytes(&bytes);
            } else {
                *sample = T::silence();
            }
        }
    }
}

fn raw_audio_chunk_bytes(format: NativeRawAudioFormat, chunk_ms: u64) -> usize {
    let frames = format.sample_rate_hz as u64 * chunk_ms.max(1);
    let frames = frames.saturating_add(999) / 1000;
    frames
        .max(1)
        .saturating_mul(format.bytes_per_frame() as u64)
        .min(usize::MAX as u64) as usize
}

fn stable_component(value: &str) -> String {
    let mut out = String::new();
    for character in value.chars().flat_map(char::to_lowercase) {
        if character.is_ascii_alphanumeric() {
            out.push(character);
        } else if !out.ends_with('-') {
            out.push('-');
        }
    }
    out.trim_matches('-').to_string()
}

#[cfg(target_os = "linux")]
fn camera_display_name(path: &Path) -> Option<String> {
    let name = path.file_name()?.to_str()?;
    fs::read_to_string(format!("/sys/class/video4linux/{name}/name"))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

#[cfg(target_os = "linux")]
fn start_platform_camera_capture(
    config: NativeCameraCaptureConfig,
) -> Result<NativeCameraCapture, NativeDeviceError> {
    use v4l::buffer::Type;
    use v4l::io::mmap::Stream as MmapStream;
    use v4l::io::traits::CaptureStream;
    use v4l::video::Capture;
    use v4l::{Device, FourCC};

    let path = select_v4l2_camera(config.device.as_deref())?;
    let (tx, rx) = mpsc::channel();
    let stop = Arc::new(AtomicBool::new(false));
    let worker_stop = stop.clone();
    let worker = thread::Builder::new()
        .name("sinomo-native-camera".to_string())
        .spawn(move || {
            let result = (|| -> Result<(), NativeDeviceError> {
                let device =
                    Device::with_path(&path).map_err(|source| NativeDeviceError::Camera {
                        detail: source.to_string(),
                    })?;
                let mut format = device
                    .format()
                    .map_err(|source| NativeDeviceError::Camera {
                        detail: source.to_string(),
                    })?;
                format.width = config.width;
                format.height = config.height;
                format.fourcc = FourCC::new(b"MJPG");
                let format =
                    device
                        .set_format(&format)
                        .map_err(|source| NativeDeviceError::Camera {
                            detail: source.to_string(),
                        })?;
                let mut stream =
                    MmapStream::with_buffers(&device, Type::VideoCapture, 4).map_err(|source| {
                        NativeDeviceError::Camera {
                            detail: source.to_string(),
                        }
                    })?;
                let frame_interval = Duration::from_millis(config.frame_interval_ms.max(1));
                let mut sequence = 0_u64;
                while !worker_stop.load(Ordering::Relaxed) {
                    let (data, meta) =
                        stream.next().map_err(|source| NativeDeviceError::Camera {
                            detail: source.to_string(),
                        })?;
                    let used = (meta.bytesused as usize).min(data.len());
                    let frame = NativeVideoFrame {
                        sequence,
                        format: NativeVideoFrameFormat::Jpeg,
                        width: format.width,
                        height: format.height,
                        captured_at_unix_ms: now_unix_ms(),
                        bytes: data[..used].to_vec(),
                    };
                    sequence = sequence.saturating_add(1);
                    if tx.send(Ok(frame)).is_err() {
                        break;
                    }
                    thread::sleep(frame_interval);
                }
                Ok(())
            })();
            if let Err(error) = result {
                let _ = tx.send(Err(error));
            }
        })
        .map_err(|source| NativeDeviceError::Camera {
            detail: source.to_string(),
        })?;
    Ok(NativeCameraCapture {
        receiver: rx,
        stop,
        worker: Some(worker),
    })
}

#[cfg(not(target_os = "linux"))]
fn start_platform_camera_capture(
    _config: NativeCameraCaptureConfig,
) -> Result<NativeCameraCapture, NativeDeviceError> {
    Err(NativeDeviceError::Unsupported {
        detail: "native camera capture currently requires Linux V4L2".to_string(),
    })
}

#[cfg(target_os = "linux")]
fn select_v4l2_camera(selector: Option<&str>) -> Result<PathBuf, NativeDeviceError> {
    if let Some(selector) = selector.map(str::trim).filter(|value| !value.is_empty()) {
        let path = PathBuf::from(selector);
        if path.exists() {
            return Ok(path);
        }
        let selector_lower = selector.to_ascii_lowercase();
        for path in camera_paths()? {
            let name = camera_display_name(&path).unwrap_or_default();
            if name.to_ascii_lowercase().contains(&selector_lower) {
                return Ok(path);
            }
        }
        return Err(NativeDeviceError::Camera {
            detail: format!("native camera `{selector}` was not found"),
        });
    }
    camera_paths()?
        .into_iter()
        .next()
        .ok_or_else(|| NativeDeviceError::Camera {
            detail: "no native camera device is available".to_string(),
        })
}

#[cfg(target_os = "linux")]
fn camera_paths() -> Result<Vec<PathBuf>, NativeDeviceError> {
    let entries = fs::read_dir("/dev").map_err(|source| NativeDeviceError::Io {
        path: "/dev".to_string(),
        detail: source.to_string(),
    })?;
    let mut paths: Vec<PathBuf> = entries
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("video"))
        })
        .collect();
    paths.sort();
    Ok(paths)
}

fn now_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_descriptor_uses_stable_device_id_and_capabilities() {
        let node_id = NodeId::from_legacy_named("node_native").expect("node");
        let first = native_descriptor(
            &node_id,
            "cpal:input:0:default".to_string(),
            NativeDeviceBackend::Cpal,
            "Default Microphone".to_string(),
            DeviceKind::Microphone,
            true,
            vec!["input.audio.capture"],
            vec![DevicePermission::Observe, DevicePermission::CaptureRaw],
            None,
        );
        let second = native_descriptor(
            &node_id,
            "cpal:input:0:default".to_string(),
            NativeDeviceBackend::Cpal,
            "Default Microphone".to_string(),
            DeviceKind::Microphone,
            true,
            vec!["input.audio.capture"],
            vec![DevicePermission::Observe, DevicePermission::CaptureRaw],
            None,
        );

        assert_eq!(first.profile.device_id, second.profile.device_id);
        assert_eq!(first.profile.kind, DeviceKind::Microphone);
        assert!(
            first
                .profile
                .has_capability(&CapabilityName::new("input.audio.capture").expect("capability"))
        );
    }

    #[test]
    fn raw_audio_chunk_bytes_aligns_to_frames() {
        let format = NativeRawAudioFormat {
            sample_rate_hz: 24_000,
            channels: 2,
            sample_format: NativeRawAudioSampleFormat::I16Le,
        };

        assert_eq!(raw_audio_chunk_bytes(format, 100), 9_600);
        assert_eq!(
            raw_audio_chunk_bytes(format, 1) % format.bytes_per_frame(),
            0
        );
    }

    #[test]
    fn speaker_queue_drops_unaligned_tail() {
        let format = NativeRawAudioFormat {
            sample_rate_hz: 48_000,
            channels: 2,
            sample_format: NativeRawAudioSampleFormat::I16Le,
        };
        let mut queue = SpeakerQueue::new(format);

        queue.push_bytes(&[1, 0, 2, 0, 3]);

        let mut out = [0_i16; 4];
        queue.fill(&mut out);
        assert_eq!(out, [1, 2, 0, 0]);
    }
}
