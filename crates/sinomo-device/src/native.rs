use crate::DeviceProfile;
use crate::{DeviceId, DeviceKind, DevicePermission};
#[cfg(feature = "native-devices")]
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
#[cfg(all(
    feature = "native-devices",
    any(target_os = "windows", target_os = "macos")
))]
use nokhwa::{
    Camera,
    utils::{
        ApiBackend, CameraFormat, CameraIndex, CameraInfo, FrameFormat, RequestedFormat,
        RequestedFormatType, frame_formats,
    },
};
use serde::{Deserialize, Serialize};
use sinomo_core::node::NodeId;
use sinomo_core::{CapabilityName, DeviceCapability};
#[cfg(feature = "native-devices")]
use std::collections::VecDeque;
#[cfg(all(feature = "native-devices", target_os = "linux"))]
use std::fs;
#[cfg(all(feature = "native-devices", target_os = "linux"))]
use std::path::{Path, PathBuf};
use std::sync::Arc;
#[cfg(feature = "native-devices")]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(feature = "native-devices")]
use std::sync::{Condvar, Mutex, mpsc};
#[cfg(feature = "native-devices")]
use std::thread;
use std::time::Duration;
#[cfg(feature = "native-devices")]
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use thiserror::Error;

const DEFAULT_AUDIO_CHUNK_MS: u64 = 100;
/// Interactive local previews target roughly 30 FPS. Realtime consumers that
/// transmit fewer frames must throttle their outbound path independently so
/// local camera feedback stays responsive.
pub const NATIVE_CAMERA_PREVIEW_FRAME_INTERVAL_MS: u64 = 33;
const DEFAULT_CAMERA_WIDTH: u32 = 640;
const DEFAULT_CAMERA_HEIGHT: u32 = 480;
#[cfg(feature = "native-devices")]
const MICROPHONE_CAPTURE_QUEUE_CAPACITY: usize = 4;
#[cfg(feature = "native-devices")]
const CAMERA_WORKER_SHUTDOWN_GRACE: Duration = Duration::from_secs(1);
#[cfg(any(feature = "native-devices", test))]
const MAX_CONSECUTIVE_CAMERA_SAMPLE_FAILURES: u8 = 3;
#[cfg(feature = "native-devices")]
const SPEAKER_QUEUE_MAX_LATENCY_MS: u64 = 1_000;
#[cfg(all(feature = "native-devices", target_os = "macos"))]
static NOKHWA_CAMERA_PERMISSION_IN_FLIGHT: AtomicBool = AtomicBool::new(false);

#[cfg(all(feature = "native-devices", target_os = "macos"))]
struct NokhwaCameraPermissionGuard;

#[cfg(all(feature = "native-devices", target_os = "macos"))]
impl Drop for NokhwaCameraPermissionGuard {
    fn drop(&mut self) {
        NOKHWA_CAMERA_PERMISSION_IN_FLIGHT.store(false, Ordering::Release);
    }
}

/// Single-slot camera handoff. Producers replace an unread frame so previews
/// and realtime sends always consume the newest available image instead of a
/// stale bounded backlog.
#[cfg(feature = "native-devices")]
struct LatestCaptureState<T> {
    item: Option<T>,
    sender_alive: bool,
    receiver_alive: bool,
}

#[cfg(feature = "native-devices")]
struct LatestCaptureShared<T> {
    state: Mutex<LatestCaptureState<T>>,
    ready: Condvar,
}

#[cfg(feature = "native-devices")]
struct LatestCaptureSender<T> {
    shared: Arc<LatestCaptureShared<T>>,
}

#[cfg(feature = "native-devices")]
struct LatestCaptureReceiver<T> {
    shared: Arc<LatestCaptureShared<T>>,
}

#[cfg(feature = "native-devices")]
fn latest_capture_channel<T>() -> (LatestCaptureSender<T>, LatestCaptureReceiver<T>) {
    let shared = Arc::new(LatestCaptureShared {
        state: Mutex::new(LatestCaptureState {
            item: None,
            sender_alive: true,
            receiver_alive: true,
        }),
        ready: Condvar::new(),
    });
    (
        LatestCaptureSender {
            shared: shared.clone(),
        },
        LatestCaptureReceiver { shared },
    )
}

#[cfg(feature = "native-devices")]
impl<T> LatestCaptureSender<T> {
    fn send_latest(&self, item: T) -> bool {
        let Ok(mut state) = self.shared.state.lock() else {
            return false;
        };
        if !state.receiver_alive {
            return false;
        }
        state.item = Some(item);
        self.shared.ready.notify_one();
        true
    }
}

#[cfg(feature = "native-devices")]
impl<T> Drop for LatestCaptureSender<T> {
    fn drop(&mut self) {
        if let Ok(mut state) = self.shared.state.lock() {
            state.sender_alive = false;
        }
        self.shared.ready.notify_all();
    }
}

#[cfg(feature = "native-devices")]
impl<T> LatestCaptureReceiver<T> {
    fn recv_timeout(&self, timeout: Duration) -> Result<T, mpsc::RecvTimeoutError> {
        let mut state = self
            .shared
            .state
            .lock()
            .map_err(|_| mpsc::RecvTimeoutError::Disconnected)?;
        if let Some(item) = state.item.take() {
            return Ok(item);
        }
        if !state.sender_alive {
            return Err(mpsc::RecvTimeoutError::Disconnected);
        }
        let (mut state, _) = self
            .shared
            .ready
            .wait_timeout_while(state, timeout, |state| {
                state.item.is_none() && state.sender_alive
            })
            .map_err(|_| mpsc::RecvTimeoutError::Disconnected)?;
        if let Some(item) = state.item.take() {
            Ok(item)
        } else if state.sender_alive {
            Err(mpsc::RecvTimeoutError::Timeout)
        } else {
            Err(mpsc::RecvTimeoutError::Disconnected)
        }
    }
}

#[cfg(feature = "native-devices")]
impl<T> Drop for LatestCaptureReceiver<T> {
    fn drop(&mut self) {
        if let Ok(mut state) = self.shared.state.lock() {
            state.receiver_alive = false;
            state.item = None;
        }
        self.shared.ready.notify_all();
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum NativeDeviceBackend {
    Android,
    Cpal,
    V4l2,
    MediaFoundation,
    AvFoundation,
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
            frame_interval_ms: NATIVE_CAMERA_PREVIEW_FRAME_INTERVAL_MS,
        }
    }
}

/// Native camera sample formats preserved by the capture layer. Compressed
/// MJPEG is passed through unchanged; every other variant is raw pixel data.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeVideoSampleFormat {
    Mjpeg,
    Nv12,
    Yuyv,
    Rgb8,
    Bgr8,
    Gray8,
}

impl NativeVideoSampleFormat {
    #[must_use]
    pub const fn is_encoded(self) -> bool {
        matches!(self, Self::Mjpeg)
    }
}

/// Descriptor for one plane inside [`NativeVideoSample::data`]. `stride` is
/// bytes per row and is zero for an encoded MJPEG payload. Plane dimensions
/// describe samples stored in this plane rather than the full display size.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeVideoPlane {
    pub offset: usize,
    pub len: usize,
    pub width: u32,
    pub height: u32,
    pub stride: u32,
}

/// A timestamped sample from a continuous native camera stream.
///
/// `timestamp_ns` is a monotonic presentation timestamp relative to stream
/// start. `captured_at_unix_ms` supplies wall-clock correlation. Capture keeps
/// the native format and never performs an implicit JPEG re-encode.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeVideoSample {
    pub sequence: u64,
    pub format: NativeVideoSampleFormat,
    pub width: u32,
    pub height: u32,
    pub timestamp_ns: u64,
    pub duration_ns: u64,
    pub captured_at_unix_ms: u128,
    pub data: Arc<[u8]>,
    pub planes: Arc<[NativeVideoPlane]>,
}

impl NativeVideoSample {
    pub fn validate(&self) -> Result<(), NativeDeviceError> {
        validate_native_video_sample(self)
    }

    /// Return the immutable bytes described by one native plane.
    #[must_use]
    pub fn plane_data(&self, index: usize) -> Option<&[u8]> {
        let plane = self.planes.get(index)?;
        self.data
            .get(plane.offset..plane.offset.checked_add(plane.len)?)
    }
}

pub struct NativeMicrophoneCapture {
    #[cfg(feature = "native-devices")]
    receiver: mpsc::Receiver<Result<NativeRawAudioChunk, NativeDeviceError>>,
    #[cfg(feature = "native-devices")]
    stream_error: Arc<Mutex<Option<NativeDeviceError>>>,
    format: NativeRawAudioFormat,
    #[cfg(feature = "native-devices")]
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
        #[cfg(not(feature = "native-devices"))]
        {
            let _ = timeout;
            return Err(native_devices_disabled_error("microphone capture"));
        }
        #[cfg(feature = "native-devices")]
        {
            self.check_health()?;
            match self.receiver.recv_timeout(timeout) {
                Ok(Ok(chunk)) => Ok(Some(chunk)),
                Ok(Err(error)) => Err(error),
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    self.check_health()?;
                    Ok(None)
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    self.check_health()?;
                    Err(NativeDeviceError::AudioStream {
                        detail: "microphone stream stopped".to_string(),
                    })
                }
            }
        }
    }

    /// Return a sticky asynchronous stream failure, if the backend callback
    /// has reported one since this capture started.
    pub fn check_health(&self) -> Result<(), NativeDeviceError> {
        #[cfg(not(feature = "native-devices"))]
        {
            return Err(native_devices_disabled_error("microphone capture"));
        }
        #[cfg(feature = "native-devices")]
        {
            check_native_stream_health(
                &self.stream_error,
                NativeDeviceError::AudioStream {
                    detail: "microphone error state lock poisoned".to_string(),
                },
            )
        }
    }
}

pub struct NativeSpeakerPlayback {
    #[cfg(feature = "native-devices")]
    queue: Arc<Mutex<SpeakerQueue>>,
    #[cfg(feature = "native-devices")]
    stream_error: Arc<Mutex<Option<NativeDeviceError>>>,
    format: NativeRawAudioFormat,
    #[cfg(feature = "native-devices")]
    _stream: cpal::Stream,
}

impl NativeSpeakerPlayback {
    pub fn format(&self) -> NativeRawAudioFormat {
        self.format
    }

    pub fn write_raw_interleaved(&self, bytes: &[u8]) -> Result<(), NativeDeviceError> {
        #[cfg(not(feature = "native-devices"))]
        {
            let _ = bytes;
            return Err(native_devices_disabled_error("speaker playback"));
        }
        #[cfg(feature = "native-devices")]
        {
            self.check_health()?;
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

    /// Check for asynchronous failures reported by the output callback. This
    /// is useful after a one-shot speaker test that performs no later write.
    pub fn check_health(&self) -> Result<(), NativeDeviceError> {
        #[cfg(not(feature = "native-devices"))]
        {
            return Err(native_devices_disabled_error("speaker playback"));
        }
        #[cfg(feature = "native-devices")]
        {
            check_native_stream_health(
                &self.stream_error,
                NativeDeviceError::AudioStream {
                    detail: "speaker error state lock poisoned".to_string(),
                },
            )
        }
    }
}

pub struct NativeCameraCapture {
    #[cfg(feature = "native-devices")]
    receiver: LatestCaptureReceiver<Result<NativeVideoSample, NativeDeviceError>>,
    #[cfg(feature = "native-devices")]
    stream_error: Arc<Mutex<Option<NativeDeviceError>>>,
    #[cfg(feature = "native-devices")]
    stop: Arc<AtomicBool>,
    #[cfg(feature = "native-devices")]
    worker: Option<thread::JoinHandle<()>>,
}

impl NativeCameraCapture {
    /// Receive the newest available sample from the continuous native stream.
    /// If capture outruns the consumer, unread samples are replaced rather
    /// than queued, so latency remains bounded to one sample.
    pub fn recv_sample_timeout(
        &self,
        timeout: Duration,
    ) -> Result<Option<NativeVideoSample>, NativeDeviceError> {
        #[cfg(not(feature = "native-devices"))]
        {
            let _ = timeout;
            return Err(native_devices_disabled_error("camera capture"));
        }
        #[cfg(feature = "native-devices")]
        {
            self.check_health()?;
            match self.receiver.recv_timeout(timeout) {
                Ok(Ok(sample)) => Ok(Some(sample)),
                Ok(Err(error)) => Err(error),
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    self.check_health()?;
                    Ok(None)
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    self.check_health()?;
                    Err(NativeDeviceError::Camera {
                        detail: "camera stream stopped".to_string(),
                    })
                }
            }
        }
    }

    /// Return a sticky asynchronous worker failure even when the bounded frame
    /// queue was full when that failure occurred.
    pub fn check_health(&self) -> Result<(), NativeDeviceError> {
        #[cfg(not(feature = "native-devices"))]
        {
            return Err(native_devices_disabled_error("camera capture"));
        }
        #[cfg(feature = "native-devices")]
        {
            check_native_stream_health(
                &self.stream_error,
                NativeDeviceError::Camera {
                    detail: "camera error state lock poisoned".to_string(),
                },
            )
        }
    }
}

impl Drop for NativeCameraCapture {
    fn drop(&mut self) {
        #[cfg(feature = "native-devices")]
        {
            self.stop.store(true, Ordering::Release);
            if let Some(worker) = self.worker.take() {
                join_camera_worker_bounded(worker, CAMERA_WORKER_SHUTDOWN_GRACE);
            }
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

/// Stable failure categories used by setup and recovery UI without coupling it
/// to backend-specific CPAL, V4L2, Media Foundation, or AVFoundation text.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum NativeDeviceFailureKind {
    PermissionDenied,
    Busy,
    NotFound,
    Unsupported,
    Other,
}

impl NativeDeviceFailureKind {
    /// Classify a backend diagnostic, including inventory issue strings that
    /// are not attached to a concrete [`NativeDeviceError`].
    pub fn classify_detail(detail: &str) -> Self {
        classify_native_device_failure(detail)
    }

    /// Whether retrying, reselecting, reconnecting, or granting permission can
    /// reasonably change the outcome. Unsupported operations require a build
    /// or platform capability change instead.
    #[must_use]
    pub const fn is_recoverable(self) -> bool {
        !matches!(self, Self::Unsupported)
    }
}

impl NativeDeviceError {
    #[must_use]
    pub fn failure_kind(&self) -> NativeDeviceFailureKind {
        match self {
            Self::Unsupported { .. } => NativeDeviceFailureKind::Unsupported,
            Self::AudioDevice { detail }
            | Self::AudioStream { detail }
            | Self::Camera { detail }
            | Self::Io { detail, .. } => NativeDeviceFailureKind::classify_detail(detail),
        }
    }

    #[must_use]
    pub fn is_recoverable(&self) -> bool {
        self.failure_kind().is_recoverable()
    }
}

fn classify_native_device_failure(detail: &str) -> NativeDeviceFailureKind {
    let detail = detail.to_ascii_lowercase();
    if contains_any(
        &detail,
        &[
            "permission denied",
            "access denied",
            "authorization denied",
            "unauthorized",
            "not permitted",
            "operation not permitted",
            "user denied",
            "consent denied",
            "privacy settings",
            "e_accessdenied",
            "0x80070005",
        ],
    ) {
        return NativeDeviceFailureKind::PermissionDenied;
    }
    if contains_any(
        &detail,
        &[
            "device busy",
            "resource busy",
            "already in use",
            "device is in use",
            "camera is in use",
            "currently in use",
            "already in progress",
            "being used by another",
            "exclusive access",
            "0x800700aa",
        ],
    ) {
        return NativeDeviceFailureKind::Busy;
    }
    if contains_any(
        &detail,
        &[
            "not found",
            "was not found",
            "could not find",
            "cannot find",
            "no such file",
            "does not exist",
            "no default native",
            "no native camera",
            "no camera device",
            "element not found",
            "0x80070002",
        ],
    ) {
        return NativeDeviceFailureKind::NotFound;
    }
    if contains_any(
        &detail,
        &[
            "unsupported",
            "not supported",
            "not implemented",
            "no suitable backend",
            "requires the native-devices feature",
        ],
    ) {
        return NativeDeviceFailureKind::Unsupported;
    }
    NativeDeviceFailureKind::Other
}

fn contains_any(detail: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| detail.contains(needle))
}

#[cfg(feature = "native-devices")]
fn check_native_stream_health(
    state: &Arc<Mutex<Option<NativeDeviceError>>>,
    poisoned: NativeDeviceError,
) -> Result<(), NativeDeviceError> {
    let error = state.lock().map_err(|_| poisoned)?.clone();
    match error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

#[cfg(feature = "native-devices")]
fn record_native_stream_error(
    state: &Arc<Mutex<Option<NativeDeviceError>>>,
    error: NativeDeviceError,
) {
    if let Ok(mut state) = state.lock()
        && state.is_none()
    {
        *state = Some(error);
    }
}

/// Microphone producers must never wait for a slow consumer. A full queue keeps
/// the small, ordered audio backlog and drops the new chunk; a disconnected
/// queue tells the callback that its owner is gone. Cameras use a latest-wins
/// slot instead because stale frames have no continuity value.
#[cfg(feature = "native-devices")]
fn try_send_capture_result<T>(
    tx: &mpsc::SyncSender<Result<T, NativeDeviceError>>,
    result: Result<T, NativeDeviceError>,
) -> bool {
    match tx.try_send(result) {
        Ok(()) | Err(mpsc::TrySendError::Full(_)) => true,
        Err(mpsc::TrySendError::Disconnected(_)) => false,
    }
}

fn validate_native_video_sample(sample: &NativeVideoSample) -> Result<(), NativeDeviceError> {
    if sample.width == 0 || sample.height == 0 {
        return Err(NativeDeviceError::Camera {
            detail: "native video sample dimensions must be non-zero".to_string(),
        });
    }
    let expected_planes = usize::from(matches!(sample.format, NativeVideoSampleFormat::Nv12)) + 1;
    if sample.planes.len() != expected_planes {
        return Err(NativeDeviceError::Camera {
            detail: format!(
                "native {:?} sample has {} planes, expected {expected_planes}",
                sample.format,
                sample.planes.len()
            ),
        });
    }
    match sample.format {
        NativeVideoSampleFormat::Mjpeg => {
            let plane = &sample.planes[0];
            validate_native_video_plane_bounds(plane, sample.data.len())?;
            if plane.len == 0 {
                return Err(NativeDeviceError::Camera {
                    detail: "native MJPEG sample is empty".to_string(),
                });
            }
        }
        NativeVideoSampleFormat::Nv12 => {
            validate_native_video_plane(
                &sample.planes[0],
                sample.data.len(),
                sample.width,
                sample.height,
                sample.width,
            )?;
            validate_native_video_plane(
                &sample.planes[1],
                sample.data.len(),
                sample.width.div_ceil(2),
                sample.height.div_ceil(2),
                sample.width.div_ceil(2).saturating_mul(2),
            )?;
        }
        NativeVideoSampleFormat::Yuyv => validate_native_video_plane(
            &sample.planes[0],
            sample.data.len(),
            sample.width,
            sample.height,
            sample.width.div_ceil(2).saturating_mul(4),
        )?,
        NativeVideoSampleFormat::Rgb8 | NativeVideoSampleFormat::Bgr8 => {
            validate_native_video_plane(
                &sample.planes[0],
                sample.data.len(),
                sample.width,
                sample.height,
                sample.width.saturating_mul(3),
            )?;
        }
        NativeVideoSampleFormat::Gray8 => validate_native_video_plane(
            &sample.planes[0],
            sample.data.len(),
            sample.width,
            sample.height,
            sample.width,
        )?,
    }
    Ok(())
}

fn validate_native_video_plane(
    plane: &NativeVideoPlane,
    sample_len: usize,
    expected_width: u32,
    expected_height: u32,
    minimum_stride: u32,
) -> Result<(), NativeDeviceError> {
    validate_native_video_plane_bounds(plane, sample_len)?;
    if plane.width != expected_width || plane.height != expected_height {
        return Err(NativeDeviceError::Camera {
            detail: format!(
                "native video plane is {}x{}, expected {expected_width}x{expected_height}",
                plane.width, plane.height
            ),
        });
    }
    if plane.stride < minimum_stride {
        return Err(NativeDeviceError::Camera {
            detail: format!(
                "native video plane stride {} is smaller than {minimum_stride}",
                plane.stride
            ),
        });
    }
    let required = usize::try_from(plane.stride)
        .ok()
        .and_then(|stride| stride.checked_mul(plane.height as usize))
        .ok_or_else(|| NativeDeviceError::Camera {
            detail: "native video plane length overflowed".to_string(),
        })?;
    if plane.len < required {
        return Err(NativeDeviceError::Camera {
            detail: format!(
                "native video plane has {} bytes, expected at least {required}",
                plane.len
            ),
        });
    }
    Ok(())
}

fn validate_native_video_plane_bounds(
    plane: &NativeVideoPlane,
    sample_len: usize,
) -> Result<(), NativeDeviceError> {
    let end = plane
        .offset
        .checked_add(plane.len)
        .ok_or_else(|| NativeDeviceError::Camera {
            detail: "native video plane range overflowed".to_string(),
        })?;
    if end > sample_len {
        return Err(NativeDeviceError::Camera {
            detail: format!(
                "native video plane range {}..{end} exceeds the {sample_len}-byte sample",
                plane.offset
            ),
        });
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
#[cfg(any(feature = "native-devices", test))]
fn native_video_sample_from_contiguous(
    format: NativeVideoSampleFormat,
    width: u32,
    height: u32,
    stride: u32,
    data: Vec<u8>,
    sequence: u64,
    timestamp_ns: u64,
    duration_ns: u64,
    captured_at_unix_ms: u128,
) -> Result<NativeVideoSample, NativeDeviceError> {
    let data: Arc<[u8]> = data.into();
    let planes: Arc<[NativeVideoPlane]> = match format {
        NativeVideoSampleFormat::Mjpeg => vec![NativeVideoPlane {
            offset: 0,
            len: data.len(),
            width,
            height,
            stride: 0,
        }]
        .into(),
        NativeVideoSampleFormat::Nv12 => {
            let y_stride = stride.max(width);
            let uv_width = width.div_ceil(2);
            let uv_height = height.div_ceil(2);
            let uv_stride = y_stride.max(uv_width.saturating_mul(2));
            let y_len = usize::try_from(y_stride)
                .ok()
                .and_then(|stride| stride.checked_mul(height as usize))
                .ok_or_else(|| NativeDeviceError::Camera {
                    detail: "native NV12 luma plane length overflowed".to_string(),
                })?;
            let uv_len = usize::try_from(uv_stride)
                .ok()
                .and_then(|stride| stride.checked_mul(uv_height as usize))
                .ok_or_else(|| NativeDeviceError::Camera {
                    detail: "native NV12 chroma plane length overflowed".to_string(),
                })?;
            if data.len() < y_len.saturating_add(uv_len) {
                return Err(NativeDeviceError::Camera {
                    detail: format!(
                        "native NV12 sample has {} bytes, expected at least {}",
                        data.len(),
                        y_len.saturating_add(uv_len)
                    ),
                });
            }
            vec![
                NativeVideoPlane {
                    offset: 0,
                    len: y_len,
                    width,
                    height,
                    stride: y_stride,
                },
                NativeVideoPlane {
                    offset: y_len,
                    len: uv_len,
                    width: uv_width,
                    height: uv_height,
                    stride: uv_stride,
                },
            ]
            .into()
        }
        NativeVideoSampleFormat::Yuyv
        | NativeVideoSampleFormat::Rgb8
        | NativeVideoSampleFormat::Bgr8
        | NativeVideoSampleFormat::Gray8 => {
            let minimum_stride = match format {
                NativeVideoSampleFormat::Yuyv => width.div_ceil(2).saturating_mul(4),
                NativeVideoSampleFormat::Rgb8 | NativeVideoSampleFormat::Bgr8 => {
                    width.saturating_mul(3)
                }
                NativeVideoSampleFormat::Gray8 => width,
                NativeVideoSampleFormat::Mjpeg | NativeVideoSampleFormat::Nv12 => unreachable!(),
            };
            vec![NativeVideoPlane {
                offset: 0,
                len: data.len(),
                width,
                height,
                stride: stride.max(minimum_stride),
            }]
            .into()
        }
    };
    let sample = NativeVideoSample {
        sequence,
        format,
        width,
        height,
        timestamp_ns,
        duration_ns,
        captured_at_unix_ms,
        data,
        planes,
    };
    sample.validate()?;
    Ok(sample)
}

#[cfg(any(feature = "native-devices", test))]
fn native_video_duration_ns(frame_rate_hz: u32) -> u64 {
    1_000_000_000_u64 / u64::from(frame_rate_hz.max(1))
}

#[cfg(any(feature = "native-devices", test))]
fn camera_sample_failure_is_terminal(failures: &mut u8) -> bool {
    *failures = failures.saturating_add(1);
    *failures >= MAX_CONSECUTIVE_CAMERA_SAMPLE_FAILURES
}

#[cfg(feature = "native-devices")]
fn join_camera_worker_bounded(worker: thread::JoinHandle<()>, grace: Duration) {
    let deadline = Instant::now() + grace;
    while !worker.is_finished() && Instant::now() < deadline {
        thread::park_timeout(Duration::from_millis(10));
    }
    if worker.is_finished() {
        let _ = worker.join();
    }
    // Dropping an unfinished handle detaches a driver call that did not return
    // within the grace period. The receiver is dropped immediately afterward,
    // so the worker exits on its next nonblocking send without holding up a
    // setup-to-call handoff forever.
}

pub fn enumerate_native_devices(node_id: NodeId) -> NativeDeviceInventory {
    enumerate_native_devices_with_camera(node_id, true)
}

/// Enumerate local native devices without touching the camera backend. This is
/// used when camera setup is paused so audio selection never triggers a camera
/// permission prompt as a side effect.
pub fn enumerate_native_devices_without_camera(node_id: NodeId) -> NativeDeviceInventory {
    enumerate_native_devices_with_camera(node_id, false)
}

fn enumerate_native_devices_with_camera(
    node_id: NodeId,
    include_camera: bool,
) -> NativeDeviceInventory {
    let _ = include_camera;
    let mut inventory = NativeDeviceInventory {
        node_id: node_id.clone(),
        devices: Vec::new(),
        issues: Vec::new(),
    };
    #[cfg(target_os = "android")]
    {
        enumerate_android_mobile_devices(&node_id, &mut inventory);
        if !include_camera {
            inventory
                .devices
                .retain(|device| device.profile.kind != DeviceKind::Camera);
        }
    }
    #[cfg(not(feature = "native-devices"))]
    {
        if inventory.devices.is_empty() {
            inventory
                .issues
                .push("native device backend is disabled in this build".to_string());
        } else {
            inventory.issues.push(
                "native capture bridge is not enabled; registered logical platform devices only"
                    .to_string(),
            );
        }
        return inventory;
    }
    #[cfg(feature = "native-devices")]
    {
        enumerate_cpal_devices(&node_id, &mut inventory);
        if include_camera {
            enumerate_camera_devices(&node_id, &mut inventory);
        }
        inventory
    }
}

#[cfg(target_os = "android")]
fn enumerate_android_mobile_devices(node_id: &NodeId, inventory: &mut NativeDeviceInventory) {
    for descriptor in [
        native_descriptor(
            node_id,
            "android:microphone:default".to_string(),
            NativeDeviceBackend::Android,
            "Android microphone".to_string(),
            DeviceKind::Microphone,
            true,
            vec!["input.audio.capture", "input.audio.raw"],
            vec![DevicePermission::Observe, DevicePermission::CaptureRaw],
            Some("Android runtime permission required: RECORD_AUDIO".to_string()),
        ),
        native_descriptor(
            node_id,
            "android:speaker:default".to_string(),
            NativeDeviceBackend::Android,
            "Android speaker".to_string(),
            DeviceKind::Speaker,
            true,
            vec!["output.audio.playback", "output.audio.raw"],
            vec![DevicePermission::EmitOutput],
            None,
        ),
        native_descriptor(
            node_id,
            "android:camera:default".to_string(),
            NativeDeviceBackend::Android,
            "Android camera".to_string(),
            DeviceKind::Camera,
            true,
            vec!["input.video.capture", "input.video.raw"],
            vec![DevicePermission::Observe, DevicePermission::CaptureRaw],
            Some("Android runtime permission required: CAMERA".to_string()),
        ),
        native_descriptor(
            node_id,
            "android:location:fused".to_string(),
            NativeDeviceBackend::Android,
            "Android location".to_string(),
            DeviceKind::Location,
            true,
            vec!["input.location.gps", "input.location.network"],
            vec![DevicePermission::Observe],
            Some(
                "Android runtime permission required: ACCESS_COARSE_LOCATION or ACCESS_FINE_LOCATION"
                    .to_string(),
            ),
        ),
        native_descriptor(
            node_id,
            "android:touchscreen:primary".to_string(),
            NativeDeviceBackend::Android,
            "Android touchscreen".to_string(),
            DeviceKind::Touchscreen,
            true,
            vec!["input.touch.primary", "input.gesture.pointer"],
            vec![DevicePermission::Observe, DevicePermission::ExecuteAction],
            None,
        ),
        native_descriptor(
            node_id,
            "android:notification:default".to_string(),
            NativeDeviceBackend::Android,
            "Android notifications".to_string(),
            DeviceKind::NotificationSurface,
            true,
            vec!["output.notification.post"],
            vec![DevicePermission::EmitOutput],
            Some("Android runtime permission may be required: POST_NOTIFICATIONS".to_string()),
        ),
        native_descriptor(
            node_id,
            "android:haptic:default".to_string(),
            NativeDeviceBackend::Android,
            "Android haptics".to_string(),
            DeviceKind::Haptic,
            true,
            vec!["output.haptic.vibrate"],
            vec![DevicePermission::EmitOutput],
            None,
        ),
    ] {
        inventory.devices.push(descriptor);
    }
}

#[cfg(feature = "native-devices")]
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
        sample_rate_hz: stream_config.sample_rate,
        channels: stream_config.channels.max(1),
        sample_format: native_raw_audio_format(supported.sample_format())?,
    };
    let target_chunk_bytes = raw_audio_chunk_bytes(format, config.chunk_ms);
    let (tx, rx) = mpsc::sync_channel(MICROPHONE_CAPTURE_QUEUE_CAPACITY);
    let stream_error = Arc::new(Mutex::new(None));
    let state = Arc::new(Mutex::new(AudioCaptureState::new(
        tx.clone(),
        format,
        target_chunk_bytes,
    )));
    let stream = match supported.sample_format() {
        cpal::SampleFormat::F32 => {
            build_input_stream::<f32>(&device, &stream_config, state, tx, stream_error.clone())?
        }
        cpal::SampleFormat::I16 => {
            build_input_stream::<i16>(&device, &stream_config, state, tx, stream_error.clone())?
        }
        cpal::SampleFormat::U16 => {
            build_input_stream::<u16>(&device, &stream_config, state, tx, stream_error.clone())?
        }
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
        stream_error,
        format,
        _stream: stream,
    })
}

#[cfg(not(feature = "native-devices"))]
pub fn start_native_microphone_capture(
    _config: NativeMicrophoneCaptureConfig,
) -> Result<NativeMicrophoneCapture, NativeDeviceError> {
    Err(native_devices_disabled_error("microphone capture"))
}

#[cfg(feature = "native-devices")]
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
        sample_rate_hz: stream_config.sample_rate,
        channels: stream_config.channels.max(1),
        sample_format: native_raw_audio_format(supported.sample_format())?,
    };
    let queue = Arc::new(Mutex::new(SpeakerQueue::new(format)));
    let stream_error = Arc::new(Mutex::new(None));
    let stream = match supported.sample_format() {
        cpal::SampleFormat::F32 => build_output_stream::<f32>(
            &device,
            &stream_config,
            queue.clone(),
            stream_error.clone(),
        )?,
        cpal::SampleFormat::I16 => build_output_stream::<i16>(
            &device,
            &stream_config,
            queue.clone(),
            stream_error.clone(),
        )?,
        cpal::SampleFormat::U16 => build_output_stream::<u16>(
            &device,
            &stream_config,
            queue.clone(),
            stream_error.clone(),
        )?,
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
        stream_error,
        format,
        _stream: stream,
    })
}

#[cfg(not(feature = "native-devices"))]
pub fn start_native_speaker_playback(
    _config: NativeSpeakerPlaybackConfig,
) -> Result<NativeSpeakerPlayback, NativeDeviceError> {
    Err(native_devices_disabled_error("speaker playback"))
}

#[cfg(feature = "native-devices")]
pub fn start_native_camera_capture(
    config: NativeCameraCaptureConfig,
) -> Result<NativeCameraCapture, NativeDeviceError> {
    start_platform_camera_capture(config)
}

#[cfg(not(feature = "native-devices"))]
pub fn start_native_camera_capture(
    _config: NativeCameraCaptureConfig,
) -> Result<NativeCameraCapture, NativeDeviceError> {
    Err(native_devices_disabled_error("camera capture"))
}

#[cfg(feature = "native-devices")]
fn enumerate_cpal_devices(node_id: &NodeId, inventory: &mut NativeDeviceInventory) {
    let host = cpal::default_host();
    let default_input = host
        .default_input_device()
        .and_then(|device| cpal_device_name(&device).ok());
    match host.input_devices() {
        Ok(devices) => {
            for (index, device) in devices.enumerate() {
                let name = cpal_device_name(&device)
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
        .and_then(|device| cpal_device_name(&device).ok());
    match host.output_devices() {
        Ok(devices) => {
            for (index, device) in devices.enumerate() {
                let name =
                    cpal_device_name(&device).unwrap_or_else(|_| format!("Speaker {}", index + 1));
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

#[cfg(feature = "native-devices")]
fn cpal_device_name(device: &cpal::Device) -> Result<String, cpal::Error> {
    device
        .description()
        .map(|description| description.name().to_owned())
}

#[cfg(all(feature = "native-devices", target_os = "linux"))]
fn enumerate_camera_devices(node_id: &NodeId, inventory: &mut NativeDeviceInventory) {
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

#[cfg(all(
    feature = "native-devices",
    any(target_os = "windows", target_os = "macos")
))]
fn enumerate_camera_devices(node_id: &NodeId, inventory: &mut NativeDeviceInventory) {
    let (_, descriptor_backend, backend_label) = nokhwa_camera_backend();
    let cameras = match query_nokhwa_cameras() {
        Ok(cameras) => cameras,
        Err(error) => {
            inventory
                .issues
                .push(format!("camera enumeration failed: {error}"));
            return;
        }
    };
    for (index, camera) in cameras.into_iter().enumerate() {
        let display_name = camera.human_name();
        let native_id = nokhwa_camera_native_id(backend_label, &camera);
        let description = camera.description().trim();
        let detail = (!description.is_empty() && description != display_name)
            .then(|| description.to_string());
        inventory.devices.push(native_descriptor(
            node_id,
            native_id,
            descriptor_backend,
            display_name,
            DeviceKind::Camera,
            index == 0,
            vec!["input.video.capture", "input.video.raw"],
            vec![DevicePermission::Observe, DevicePermission::CaptureRaw],
            detail,
        ));
    }
}

#[cfg(all(
    feature = "native-devices",
    not(any(target_os = "linux", target_os = "windows", target_os = "macos"))
))]
fn enumerate_camera_devices(_node_id: &NodeId, inventory: &mut NativeDeviceInventory) {
    inventory
        .issues
        .push("native camera enumeration is not implemented on this OS".to_string());
}

#[cfg_attr(
    not(any(feature = "native-devices", target_os = "android")),
    allow(dead_code)
)]
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

#[cfg(feature = "native-devices")]
fn select_cpal_device<I>(
    devices: Result<I, cpal::Error>,
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
        let name = cpal_device_name(&device).unwrap_or_default();
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

#[cfg(feature = "native-devices")]
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

#[cfg(feature = "native-devices")]
fn build_input_stream<T>(
    device: &cpal::Device,
    config: &cpal::StreamConfig,
    state: Arc<Mutex<AudioCaptureState>>,
    error_tx: mpsc::SyncSender<Result<NativeRawAudioChunk, NativeDeviceError>>,
    stream_error: Arc<Mutex<Option<NativeDeviceError>>>,
) -> Result<cpal::Stream, NativeDeviceError>
where
    T: RawInputSample + cpal::SizedSample + Send + 'static,
{
    device
        .build_input_stream(
            *config,
            move |data: &[T], _| {
                if let Ok(mut state) = state.lock() {
                    state.process_samples(data);
                }
            },
            move |error| {
                let error = NativeDeviceError::AudioStream {
                    detail: error.to_string(),
                };
                record_native_stream_error(&stream_error, error.clone());
                let _ = try_send_capture_result(&error_tx, Err(error));
            },
            None,
        )
        .map_err(|source| NativeDeviceError::AudioStream {
            detail: source.to_string(),
        })
}

#[cfg(feature = "native-devices")]
fn build_output_stream<T>(
    device: &cpal::Device,
    config: &cpal::StreamConfig,
    queue: Arc<Mutex<SpeakerQueue>>,
    stream_error: Arc<Mutex<Option<NativeDeviceError>>>,
) -> Result<cpal::Stream, NativeDeviceError>
where
    T: RawOutputSample + cpal::SizedSample + Send + 'static,
{
    device
        .build_output_stream(
            *config,
            move |data: &mut [T], _| {
                if let Ok(mut queue) = queue.lock() {
                    queue.fill(data);
                } else {
                    for sample in data.iter_mut() {
                        *sample = T::silence();
                    }
                }
            },
            move |error| {
                record_native_stream_error(
                    &stream_error,
                    NativeDeviceError::AudioStream {
                        detail: error.to_string(),
                    },
                );
            },
            None,
        )
        .map_err(|source| NativeDeviceError::AudioStream {
            detail: source.to_string(),
        })
}

#[cfg(feature = "native-devices")]
trait RawInputSample: Copy {
    fn encode_samples(samples: &[Self], out: &mut Vec<u8>);
}

#[cfg(feature = "native-devices")]
impl RawInputSample for f32 {
    fn encode_samples(samples: &[Self], out: &mut Vec<u8>) {
        for sample in samples {
            out.extend_from_slice(&sample.to_le_bytes());
        }
    }
}

#[cfg(feature = "native-devices")]
impl RawInputSample for i16 {
    fn encode_samples(samples: &[Self], out: &mut Vec<u8>) {
        for sample in samples {
            out.extend_from_slice(&sample.to_le_bytes());
        }
    }
}

#[cfg(feature = "native-devices")]
impl RawInputSample for u16 {
    fn encode_samples(samples: &[Self], out: &mut Vec<u8>) {
        for sample in samples {
            out.extend_from_slice(&sample.to_le_bytes());
        }
    }
}

#[cfg(feature = "native-devices")]
trait RawOutputSample: Copy {
    const BYTES: usize;

    fn silence() -> Self;
    fn from_le_bytes(bytes: &[u8]) -> Self;
}

#[cfg(feature = "native-devices")]
impl RawOutputSample for f32 {
    const BYTES: usize = 4;

    fn silence() -> Self {
        0.0
    }

    fn from_le_bytes(bytes: &[u8]) -> Self {
        f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
    }
}

#[cfg(feature = "native-devices")]
impl RawOutputSample for i16 {
    const BYTES: usize = 2;

    fn silence() -> Self {
        0
    }

    fn from_le_bytes(bytes: &[u8]) -> Self {
        i16::from_le_bytes([bytes[0], bytes[1]])
    }
}

#[cfg(feature = "native-devices")]
impl RawOutputSample for u16 {
    const BYTES: usize = 2;

    fn silence() -> Self {
        32768
    }

    fn from_le_bytes(bytes: &[u8]) -> Self {
        u16::from_le_bytes([bytes[0], bytes[1]])
    }
}

#[cfg(feature = "native-devices")]
struct AudioCaptureState {
    tx: mpsc::SyncSender<Result<NativeRawAudioChunk, NativeDeviceError>>,
    format: NativeRawAudioFormat,
    target_chunk_bytes: usize,
    pending: Vec<u8>,
    sequence: u64,
}

#[cfg(feature = "native-devices")]
impl AudioCaptureState {
    fn new(
        tx: mpsc::SyncSender<Result<NativeRawAudioChunk, NativeDeviceError>>,
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
            let _ = try_send_capture_result(&self.tx, Ok(chunk));
        }
    }
}

#[cfg(feature = "native-devices")]
struct SpeakerQueue {
    format: NativeRawAudioFormat,
    bytes: VecDeque<u8>,
}

#[cfg(feature = "native-devices")]
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
        let max_bytes = raw_audio_chunk_bytes(self.format, SPEAKER_QUEUE_MAX_LATENCY_MS);
        let max_bytes = (max_bytes - (max_bytes % frame_bytes)).max(frame_bytes);
        if aligned >= max_bytes {
            self.bytes.clear();
            self.bytes
                .extend(bytes[aligned - max_bytes..aligned].iter().copied());
            return;
        }
        let overflow = self
            .bytes
            .len()
            .saturating_add(aligned)
            .saturating_sub(max_bytes);
        if overflow > 0 {
            self.bytes.drain(0..overflow);
        }
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

#[cfg(feature = "native-devices")]
fn raw_audio_chunk_bytes(format: NativeRawAudioFormat, chunk_ms: u64) -> usize {
    let frames = format.sample_rate_hz as u64 * chunk_ms.max(1);
    let frames = frames.saturating_add(999) / 1000;
    frames
        .max(1)
        .saturating_mul(format.bytes_per_frame() as u64)
        .min(usize::MAX as u64) as usize
}

#[cfg(feature = "native-devices")]
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

#[cfg(all(
    feature = "native-devices",
    any(target_os = "windows", target_os = "macos")
))]
fn nokhwa_camera_backend() -> (ApiBackend, NativeDeviceBackend, &'static str) {
    #[cfg(target_os = "windows")]
    {
        (
            ApiBackend::MediaFoundation,
            NativeDeviceBackend::MediaFoundation,
            "media-foundation",
        )
    }
    #[cfg(target_os = "macos")]
    {
        (
            ApiBackend::AVFoundation,
            NativeDeviceBackend::AvFoundation,
            "av-foundation",
        )
    }
}

#[cfg(all(
    feature = "native-devices",
    any(target_os = "windows", target_os = "macos")
))]
fn nokhwa_camera_native_id(backend_label: &str, camera: &CameraInfo) -> String {
    let platform_id = camera.misc();
    let platform_id = platform_id.trim();
    let platform_id = if platform_id.is_empty() {
        camera.index().as_string()
    } else {
        platform_id.to_string()
    };
    format!("nokhwa:{backend_label}:{platform_id}")
}

#[cfg(all(feature = "native-devices", target_os = "windows"))]
fn ensure_nokhwa_camera_access() -> Result<(), NativeDeviceError> {
    Ok(())
}

#[cfg(all(feature = "native-devices", target_os = "macos"))]
fn ensure_nokhwa_camera_access() -> Result<(), NativeDeviceError> {
    if nokhwa::nokhwa_check() {
        return Ok(());
    }
    if NOKHWA_CAMERA_PERMISSION_IN_FLIGHT
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return Err(NativeDeviceError::Camera {
            detail: "camera permission request is already in progress".to_string(),
        });
    }
    let _permission_guard = NokhwaCameraPermissionGuard;
    let (tx, rx) = mpsc::sync_channel(1);
    nokhwa::nokhwa_initialize(move |granted| {
        let _ = tx.send(granted);
    });
    match rx.recv_timeout(Duration::from_secs(60)) {
        Ok(true) => Ok(()),
        Ok(false) => Err(NativeDeviceError::Camera {
            detail: "camera permission denied by the operating system".to_string(),
        }),
        Err(mpsc::RecvTimeoutError::Timeout) => Err(NativeDeviceError::Camera {
            detail: "camera permission request timed out".to_string(),
        }),
        Err(mpsc::RecvTimeoutError::Disconnected) => Err(NativeDeviceError::Camera {
            detail: "camera permission request stopped before completing".to_string(),
        }),
    }
}

#[cfg(all(
    feature = "native-devices",
    any(target_os = "windows", target_os = "macos")
))]
fn query_nokhwa_cameras() -> Result<Vec<CameraInfo>, NativeDeviceError> {
    ensure_nokhwa_camera_access()?;
    let (api_backend, _, _) = nokhwa_camera_backend();
    nokhwa::query(api_backend).map_err(native_nokhwa_camera_error)
}

#[cfg(all(
    feature = "native-devices",
    any(target_os = "windows", target_os = "macos")
))]
fn native_nokhwa_camera_error(source: nokhwa::NokhwaError) -> NativeDeviceError {
    let unsupported = matches!(
        source,
        nokhwa::NokhwaError::UnsupportedOperationError(_)
            | nokhwa::NokhwaError::NotImplementedError(_)
    );
    let detail = source.to_string();
    if unsupported {
        NativeDeviceError::Unsupported { detail }
    } else {
        NativeDeviceError::Camera { detail }
    }
}

#[cfg(all(
    feature = "native-devices",
    any(target_os = "windows", target_os = "macos")
))]
fn nokhwa_camera_matches_selector(
    camera: &CameraInfo,
    backend_label: &str,
    selector: &str,
) -> bool {
    let native_id = nokhwa_camera_native_id(backend_label, camera);
    let display_name = camera.human_name();
    let misc = camera.misc();
    native_id == selector
        || camera.index().as_string() == selector
        || misc == selector
        || display_name == selector
        || display_name
            .to_ascii_lowercase()
            .contains(&selector.to_ascii_lowercase())
}

#[cfg(all(
    feature = "native-devices",
    any(target_os = "windows", target_os = "macos")
))]
fn select_nokhwa_camera(selector: Option<&str>) -> Result<CameraInfo, NativeDeviceError> {
    let cameras = query_nokhwa_cameras()?;
    let Some(selector) = selector.map(str::trim).filter(|value| !value.is_empty()) else {
        return cameras
            .into_iter()
            .next()
            .ok_or_else(|| NativeDeviceError::Camera {
                detail: "no native camera device is available".to_string(),
            });
    };
    let (_, _, backend_label) = nokhwa_camera_backend();
    cameras
        .into_iter()
        .find(|camera| nokhwa_camera_matches_selector(camera, backend_label, selector))
        .ok_or_else(|| NativeDeviceError::Camera {
            detail: format!("native camera `{selector}` was not found"),
        })
}

#[cfg(all(
    feature = "native-devices",
    any(target_os = "windows", target_os = "macos")
))]
fn open_nokhwa_camera(
    index: CameraIndex,
    config: &NativeCameraCaptureConfig,
) -> Result<Camera, NativeDeviceError> {
    let (api_backend, _, _) = nokhwa_camera_backend();
    let request = RequestedFormat::with_formats(RequestedFormatType::None, frame_formats());
    let mut camera =
        Camera::with_backend(index, request, api_backend).map_err(native_nokhwa_camera_error)?;
    let formats = camera
        .compatible_camera_formats()
        .map_err(native_nokhwa_camera_error)?;
    let requested_fps = requested_camera_fps(config.frame_interval_ms);
    let selected = closest_nokhwa_camera_format(
        &formats,
        config.width.max(1),
        config.height.max(1),
        requested_fps,
    )
    .ok_or_else(|| NativeDeviceError::Camera {
        detail: "camera exposes no supported native sample format".to_string(),
    })?;
    camera
        .set_camera_requset(RequestedFormat::with_formats(
            RequestedFormatType::Exact(selected),
            frame_formats(),
        ))
        .map_err(native_nokhwa_camera_error)?;
    camera.open_stream().map_err(native_nokhwa_camera_error)?;
    Ok(camera)
}

#[cfg(feature = "native-devices")]
fn requested_camera_fps(frame_interval_ms: u64) -> u32 {
    let interval = frame_interval_ms.max(1);
    ((1_000_u64 + interval / 2) / interval).clamp(1, 120) as u32
}

#[cfg(all(
    feature = "native-devices",
    any(target_os = "windows", target_os = "macos")
))]
fn closest_nokhwa_camera_format(
    formats: &[CameraFormat],
    requested_width: u32,
    requested_height: u32,
    requested_fps: u32,
) -> Option<CameraFormat> {
    formats
        .iter()
        .copied()
        .filter(|format| native_sample_format_from_nokhwa(format.format()).is_some())
        .min_by_key(|format| {
            let frame_rate = format.frame_rate();
            let minimum_acceptable_fps = requested_fps.saturating_mul(3).div_ceil(4);
            let severe_fps_shortfall = frame_rate < minimum_acceptable_fps;
            let shortfall = if severe_fps_shortfall {
                requested_fps.saturating_sub(frame_rate)
            } else {
                0
            };
            let width_delta = u64::from(format.width().abs_diff(requested_width));
            let height_delta = u64::from(format.height().abs_diff(requested_height));
            let resolution_distance = u128::from(width_delta) * u128::from(width_delta)
                + u128::from(height_delta) * u128::from(height_delta);
            let frame_rate_distance = frame_rate.abs_diff(requested_fps);
            (
                severe_fps_shortfall,
                shortfall,
                resolution_distance,
                frame_rate_distance,
            )
        })
}

#[cfg(all(
    feature = "native-devices",
    any(target_os = "windows", target_os = "macos")
))]
fn native_sample_format_from_nokhwa(format: FrameFormat) -> Option<NativeVideoSampleFormat> {
    match format {
        FrameFormat::MJPEG => Some(NativeVideoSampleFormat::Mjpeg),
        FrameFormat::NV12 => Some(NativeVideoSampleFormat::Nv12),
        FrameFormat::YUYV => Some(NativeVideoSampleFormat::Yuyv),
        FrameFormat::RAWRGB => Some(NativeVideoSampleFormat::Rgb8),
        FrameFormat::RAWBGR => Some(NativeVideoSampleFormat::Bgr8),
        FrameFormat::GRAY => Some(NativeVideoSampleFormat::Gray8),
    }
}

#[cfg(all(
    feature = "native-devices",
    any(target_os = "windows", target_os = "macos")
))]
fn native_video_sample_from_nokhwa_buffer(
    frame: &nokhwa::Buffer,
    sequence: u64,
    timestamp_ns: u64,
    duration_ns: u64,
) -> Result<NativeVideoSample, NativeDeviceError> {
    let resolution = frame.resolution();
    let format =
        native_sample_format_from_nokhwa(frame.source_frame_format()).ok_or_else(|| {
            NativeDeviceError::Unsupported {
                detail: format!(
                    "unsupported native camera sample format {}",
                    frame.source_frame_format()
                ),
            }
        })?;
    native_video_sample_from_contiguous(
        format,
        resolution.width(),
        resolution.height(),
        0,
        frame.buffer().to_vec(),
        sequence,
        timestamp_ns,
        duration_ns,
        now_unix_ms(),
    )
}

#[cfg(all(feature = "native-devices", target_os = "linux"))]
fn camera_display_name(path: &Path) -> Option<String> {
    let name = path.file_name()?.to_str()?;
    fs::read_to_string(format!("/sys/class/video4linux/{name}/name"))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

#[cfg(all(feature = "native-devices", target_os = "linux"))]
fn native_sample_format_from_v4l(fourcc: v4l::FourCC) -> Option<NativeVideoSampleFormat> {
    match &fourcc.repr {
        b"MJPG" => Some(NativeVideoSampleFormat::Mjpeg),
        b"NV12" => Some(NativeVideoSampleFormat::Nv12),
        b"YUYV" => Some(NativeVideoSampleFormat::Yuyv),
        b"RGB3" => Some(NativeVideoSampleFormat::Rgb8),
        b"BGR3" => Some(NativeVideoSampleFormat::Bgr8),
        b"GREY" => Some(NativeVideoSampleFormat::Gray8),
        _ => None,
    }
}

#[cfg(all(feature = "native-devices", target_os = "linux"))]
fn preferred_v4l_sample_format(
    current: v4l::FourCC,
    available: &[v4l::format::Description],
) -> Option<v4l::FourCC> {
    if native_sample_format_from_v4l(current).is_some() {
        return Some(current);
    }
    [b"NV12", b"YUYV", b"RGB3", b"BGR3", b"GREY", b"MJPG"]
        .into_iter()
        .map(v4l::FourCC::new)
        .find(|candidate| available.iter().any(|format| format.fourcc == *candidate))
}

#[cfg(all(feature = "native-devices", target_os = "linux"))]
fn v4l_frame_duration_ns(interval: v4l::Fraction, fallback_fps: u32) -> u64 {
    if interval.denominator == 0 {
        return native_video_duration_ns(fallback_fps);
    }
    1_000_000_000_u64
        .saturating_mul(u64::from(interval.numerator))
        .checked_div(u64::from(interval.denominator))
        .filter(|duration| *duration > 0)
        .unwrap_or_else(|| native_video_duration_ns(fallback_fps))
}

#[cfg(all(feature = "native-devices", target_os = "linux"))]
fn start_platform_camera_capture(
    config: NativeCameraCaptureConfig,
) -> Result<NativeCameraCapture, NativeDeviceError> {
    use v4l::Device;
    use v4l::buffer::Type;
    use v4l::io::mmap::Stream as MmapStream;
    use v4l::io::traits::CaptureStream;
    use v4l::video::Capture;

    let path = select_v4l2_camera(config.device.as_deref())?;
    let (tx, rx) = latest_capture_channel();
    let stream_error = Arc::new(Mutex::new(None));
    let worker_error = stream_error.clone();
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
                let available =
                    device
                        .enum_formats()
                        .map_err(|source| NativeDeviceError::Camera {
                            detail: source.to_string(),
                        })?;
                format.fourcc =
                    preferred_v4l_sample_format(format.fourcc, &available).ok_or_else(|| {
                        NativeDeviceError::Unsupported {
                            detail: "camera exposes no supported native video sample format"
                                .to_string(),
                        }
                    })?;
                format.width = config.width.max(1);
                format.height = config.height.max(1);
                let format =
                    device
                        .set_format(&format)
                        .map_err(|source| NativeDeviceError::Camera {
                            detail: source.to_string(),
                        })?;
                let sample_format =
                    native_sample_format_from_v4l(format.fourcc).ok_or_else(|| {
                        NativeDeviceError::Unsupported {
                            detail: format!(
                                "camera negotiated unsupported native format {}",
                                format.fourcc
                            ),
                        }
                    })?;
                let requested_fps = requested_camera_fps(config.frame_interval_ms);
                let params = device
                    .set_params(&v4l::video::capture::Parameters::with_fps(requested_fps))
                    .map_err(|source| NativeDeviceError::Camera {
                        detail: format!("failed to negotiate {requested_fps} FPS: {source}"),
                    })?;
                let duration_ns = v4l_frame_duration_ns(params.interval, requested_fps);
                let mut stream =
                    MmapStream::with_buffers(&device, Type::VideoCapture, 4).map_err(|source| {
                        NativeDeviceError::Camera {
                            detail: source.to_string(),
                        }
                    })?;
                // A finite driver poll lets Drop's stop flag be observed even
                // when a camera stops producing frames without disconnecting.
                stream.set_timeout(Duration::from_millis(100));
                let stream_started = Instant::now();
                let mut sequence = 0_u64;
                let mut sample_failures = 0_u8;
                while !worker_stop.load(Ordering::Acquire) {
                    let (data, meta) = match stream.next() {
                        Ok(frame) => frame,
                        Err(source) if source.kind() == std::io::ErrorKind::TimedOut => continue,
                        Err(source) => {
                            return Err(NativeDeviceError::Camera {
                                detail: source.to_string(),
                            });
                        }
                    };
                    if worker_stop.load(Ordering::Acquire) {
                        break;
                    }
                    let used = (meta.bytesused as usize).min(data.len());
                    let timestamp_ns = stream_started
                        .elapsed()
                        .as_nanos()
                        .min(u128::from(u64::MAX)) as u64;
                    let sample_sequence = sequence;
                    sequence = sequence.saturating_add(1);
                    let sample = match native_video_sample_from_contiguous(
                        sample_format,
                        format.width,
                        format.height,
                        format.stride,
                        data[..used].to_vec(),
                        sample_sequence,
                        timestamp_ns,
                        duration_ns,
                        now_unix_ms(),
                    ) {
                        Ok(sample) => {
                            sample_failures = 0;
                            sample
                        }
                        Err(error) => {
                            if camera_sample_failure_is_terminal(&mut sample_failures) {
                                return Err(error);
                            }
                            continue;
                        }
                    };
                    if !tx.send_latest(Ok(sample)) {
                        break;
                    }
                }
                Ok(())
            })();
            if let Err(error) = result {
                record_native_stream_error(&worker_error, error.clone());
                let _ = tx.send_latest(Err(error));
            }
        })
        .map_err(|source| NativeDeviceError::Camera {
            detail: source.to_string(),
        })?;
    Ok(NativeCameraCapture {
        receiver: rx,
        stream_error,
        stop,
        worker: Some(worker),
    })
}

#[cfg(all(
    feature = "native-devices",
    any(target_os = "windows", target_os = "macos")
))]
fn start_platform_camera_capture(
    config: NativeCameraCaptureConfig,
) -> Result<NativeCameraCapture, NativeDeviceError> {
    let selected = select_nokhwa_camera(config.device.as_deref())?;
    let camera_index = selected.index().clone();
    let (tx, rx) = latest_capture_channel();
    let stream_error = Arc::new(Mutex::new(None));
    let worker_error = stream_error.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let worker_stop = stop.clone();
    let worker = thread::Builder::new()
        .name("sinomo-native-camera".to_string())
        .spawn(move || {
            let result = (|| -> Result<(), NativeDeviceError> {
                let mut camera = open_nokhwa_camera(camera_index, &config)?;
                let capture_result = (|| -> Result<(), NativeDeviceError> {
                    let frame_rate_hz = camera.camera_format().frame_rate();
                    let duration_ns = native_video_duration_ns(frame_rate_hz);
                    let stream_started = Instant::now();
                    let mut sequence = 0_u64;
                    let mut sample_failures = 0_u8;
                    while !worker_stop.load(Ordering::Acquire) {
                        let frame = camera.frame().map_err(native_nokhwa_camera_error)?;
                        if worker_stop.load(Ordering::Acquire) {
                            break;
                        }
                        let timestamp_ns = stream_started
                            .elapsed()
                            .as_nanos()
                            .min(u128::from(u64::MAX))
                            as u64;
                        let sample_sequence = sequence;
                        sequence = sequence.saturating_add(1);
                        let sample = match native_video_sample_from_nokhwa_buffer(
                            &frame,
                            sample_sequence,
                            timestamp_ns,
                            duration_ns,
                        ) {
                            Ok(sample) => {
                                sample_failures = 0;
                                sample
                            }
                            Err(error) => {
                                if camera_sample_failure_is_terminal(&mut sample_failures) {
                                    return Err(error);
                                }
                                continue;
                            }
                        };
                        if !tx.send_latest(Ok(sample)) {
                            break;
                        }
                    }
                    Ok(())
                })();
                let stop_result = camera.stop_stream().map_err(native_nokhwa_camera_error);
                capture_result.and(stop_result)
            })();
            if let Err(error) = result {
                record_native_stream_error(&worker_error, error.clone());
                let _ = tx.send_latest(Err(error));
            }
        })
        .map_err(|source| NativeDeviceError::Camera {
            detail: source.to_string(),
        })?;
    Ok(NativeCameraCapture {
        receiver: rx,
        stream_error,
        stop,
        worker: Some(worker),
    })
}

#[cfg(all(
    feature = "native-devices",
    not(any(target_os = "linux", target_os = "windows", target_os = "macos"))
))]
fn start_platform_camera_capture(
    _config: NativeCameraCaptureConfig,
) -> Result<NativeCameraCapture, NativeDeviceError> {
    Err(NativeDeviceError::Unsupported {
        detail: "native camera capture is not implemented on this OS".to_string(),
    })
}

#[cfg(all(feature = "native-devices", target_os = "linux"))]
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

#[cfg(all(feature = "native-devices", target_os = "linux"))]
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

#[cfg(not(feature = "native-devices"))]
fn native_devices_disabled_error(operation: &str) -> NativeDeviceError {
    NativeDeviceError::Unsupported {
        detail: format!("{operation} requires the native-devices feature"),
    }
}

#[cfg(feature = "native-devices")]
fn now_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}

#[cfg(test)]
mod failure_tests {
    use super::*;

    #[test]
    fn native_device_failure_classification_covers_recovery_categories() {
        for (detail, expected) in [
            (
                "Access denied by Windows privacy settings (0x80070005)",
                NativeDeviceFailureKind::PermissionDenied,
            ),
            ("Device or resource busy", NativeDeviceFailureKind::Busy),
            (
                "native camera `missing` was not found",
                NativeDeviceFailureKind::NotFound,
            ),
            (
                "camera backend is not implemented",
                NativeDeviceFailureKind::Unsupported,
            ),
            (
                "backend returned an unexpected stream state",
                NativeDeviceFailureKind::Other,
            ),
        ] {
            assert_eq!(
                NativeDeviceFailureKind::classify_detail(detail),
                expected,
                "detail: {detail}"
            );
        }
    }

    #[test]
    fn native_device_error_exposes_failure_kind_and_recoverability() {
        let denied = NativeDeviceError::Camera {
            detail: "permission denied".to_string(),
        };
        assert_eq!(
            denied.failure_kind(),
            NativeDeviceFailureKind::PermissionDenied
        );
        assert!(denied.is_recoverable());

        let unsupported = NativeDeviceError::Unsupported {
            detail: "permission denied wording must not override the typed variant".to_string(),
        };
        assert_eq!(
            unsupported.failure_kind(),
            NativeDeviceFailureKind::Unsupported
        );
        assert!(!unsupported.is_recoverable());
    }

    #[test]
    fn only_unsupported_failures_are_non_recoverable() {
        for kind in [
            NativeDeviceFailureKind::PermissionDenied,
            NativeDeviceFailureKind::Busy,
            NativeDeviceFailureKind::NotFound,
            NativeDeviceFailureKind::Other,
        ] {
            assert!(kind.is_recoverable(), "kind: {kind:?}");
        }
        assert!(!NativeDeviceFailureKind::Unsupported.is_recoverable());
    }

    #[test]
    fn native_camera_default_targets_interactive_preview_cadence() {
        let config = NativeCameraCaptureConfig::default();

        assert_eq!(config.frame_interval_ms, 33);
        assert!(config.frame_interval_ms <= 1_000 / 30);
    }

    #[test]
    fn native_video_samples_preserve_format_planes_and_stride() {
        let nv12 = native_video_sample_from_contiguous(
            NativeVideoSampleFormat::Nv12,
            2,
            2,
            2,
            vec![16, 32, 48, 64, 128, 192],
            7,
            1_000,
            native_video_duration_ns(30),
            42,
        )
        .expect("NV12 sample");
        assert_eq!(nv12.planes.len(), 2);
        assert_eq!((nv12.planes[0].width, nv12.planes[0].height), (2, 2));
        assert_eq!(nv12.planes[0].stride, 2);
        assert_eq!(nv12.planes[0].offset, 0);
        assert_eq!(nv12.plane_data(0), Some(&[16, 32, 48, 64][..]));
        assert_eq!((nv12.planes[1].width, nv12.planes[1].height), (1, 1));
        assert_eq!(nv12.planes[1].stride, 2);
        assert_eq!(nv12.planes[1].offset, 4);
        assert_eq!(nv12.plane_data(1), Some(&[128, 192][..]));
        assert_eq!(nv12.timestamp_ns, 1_000);
        let cloned = nv12.clone();
        assert!(Arc::ptr_eq(&nv12.data, &cloned.data));
        assert!(Arc::ptr_eq(&nv12.planes, &cloned.planes));

        for (format, bytes, expected_stride) in [
            (NativeVideoSampleFormat::Yuyv, vec![16; 8], 4),
            (NativeVideoSampleFormat::Rgb8, vec![32; 12], 6),
            (NativeVideoSampleFormat::Bgr8, vec![48; 12], 6),
            (NativeVideoSampleFormat::Gray8, vec![64; 4], 2),
        ] {
            let sample = native_video_sample_from_contiguous(format, 2, 2, 0, bytes, 0, 0, 1, 0)
                .expect("packed raw sample");
            assert_eq!(sample.format, format);
            assert_eq!(sample.planes[0].stride, expected_stride);
        }

        let mjpeg = native_video_sample_from_contiguous(
            NativeVideoSampleFormat::Mjpeg,
            2,
            2,
            0,
            vec![0xff, 0xd8, 0xff, 0xd9],
            0,
            0,
            1,
            0,
        )
        .expect("native MJPEG sample");
        assert!(mjpeg.format.is_encoded());
        assert_eq!(mjpeg.planes[0].stride, 0);
        assert_eq!(mjpeg.plane_data(0), Some(&[0xff, 0xd8, 0xff, 0xd9][..]));
    }
}

#[cfg(all(test, feature = "native-devices"))]
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

    #[test]
    fn speaker_queue_caps_latency_and_keeps_the_newest_frames() {
        let format = NativeRawAudioFormat {
            sample_rate_hz: 1_000,
            channels: 1,
            sample_format: NativeRawAudioSampleFormat::I16Le,
        };
        let mut queue = SpeakerQueue::new(format);
        let mut input = Vec::new();
        for sample in 0_i16..1_200 {
            input.extend_from_slice(&sample.to_le_bytes());
        }

        queue.push_bytes(&input);

        assert_eq!(queue.bytes.len(), 2_000, "one second of mono i16");
        let mut output = vec![0_i16; 1_000];
        queue.fill(&mut output);
        assert_eq!(output[0], 200);
        assert_eq!(output[999], 1_199);
    }

    #[test]
    fn capture_queue_is_bounded_and_never_waits_for_capacity() {
        let (tx, rx) = mpsc::sync_channel(1);

        assert!(try_send_capture_result(&tx, Ok::<u8, NativeDeviceError>(1)));
        assert!(
            try_send_capture_result(&tx, Ok::<u8, NativeDeviceError>(2)),
            "a full queue drops the new sample without stopping the producer"
        );
        assert_eq!(rx.try_recv().expect("first sample"), Ok(1));
        assert!(matches!(rx.try_recv(), Err(mpsc::TryRecvError::Empty)));

        drop(rx);
        assert!(!try_send_capture_result(
            &tx,
            Ok::<u8, NativeDeviceError>(3)
        ));
    }

    #[test]
    fn camera_handoff_replaces_an_unread_sample_with_the_latest() {
        let (tx, rx) = latest_capture_channel();
        let sample = |sequence, value| {
            native_video_sample_from_contiguous(
                NativeVideoSampleFormat::Gray8,
                1,
                1,
                1,
                vec![value],
                sequence,
                sequence * 33_333_333,
                33_333_333,
                0,
            )
            .expect("gray sample")
        };

        assert!(tx.send_latest(Ok::<NativeVideoSample, NativeDeviceError>(sample(1, 10))));
        assert!(tx.send_latest(Ok::<NativeVideoSample, NativeDeviceError>(sample(2, 20))));
        let latest = rx
            .recv_timeout(Duration::ZERO)
            .expect("latest sample")
            .expect("healthy sample");
        assert_eq!(latest.sequence, 2);
        assert_eq!(latest.timestamp_ns, 66_666_666);
        assert_eq!(latest.plane_data(0), Some(&[20][..]));
        assert!(matches!(
            rx.recv_timeout(Duration::ZERO),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));

        drop(rx);
        assert!(!tx.send_latest(Ok::<NativeVideoSample, NativeDeviceError>(sample(3, 30))));
    }

    #[test]
    fn asynchronous_stream_health_keeps_the_first_failure() {
        let state = Arc::new(Mutex::new(None));
        let first = NativeDeviceError::Camera {
            detail: "camera disconnected".to_string(),
        };
        record_native_stream_error(&state, first.clone());
        record_native_stream_error(
            &state,
            NativeDeviceError::Camera {
                detail: "later callback noise".to_string(),
            },
        );

        assert_eq!(
            check_native_stream_health(
                &state,
                NativeDeviceError::Camera {
                    detail: "poisoned".to_string(),
                }
            ),
            Err(first)
        );
    }

    #[test]
    fn negotiated_frame_rate_defines_sample_duration_without_software_sleep() {
        assert_eq!(requested_camera_fps(33), 30);
        assert_eq!(native_video_duration_ns(30), 33_333_333);
    }

    #[test]
    fn continuous_camera_stream_drops_isolated_invalid_samples() {
        let mut failures = 0;
        assert!(!camera_sample_failure_is_terminal(&mut failures));
        assert!(!camera_sample_failure_is_terminal(&mut failures));
        assert!(camera_sample_failure_is_terminal(&mut failures));

        failures = 0;
        assert!(!camera_sample_failure_is_terminal(&mut failures));
    }

    #[cfg(any(target_os = "windows", target_os = "macos"))]
    #[test]
    fn nokhwa_camera_selector_accepts_stable_id_name_index_and_platform_id() {
        let camera = CameraInfo::new(
            "Integrated Camera",
            "native test camera",
            "platform-camera-id",
            CameraIndex::Index(3),
        );
        let (_, _, backend_label) = nokhwa_camera_backend();
        let native_id = nokhwa_camera_native_id(backend_label, &camera);

        assert!(nokhwa_camera_matches_selector(
            &camera,
            backend_label,
            &native_id
        ));
        assert!(nokhwa_camera_matches_selector(
            &camera,
            backend_label,
            "integrated"
        ));
        assert!(nokhwa_camera_matches_selector(&camera, backend_label, "3"));
        assert!(nokhwa_camera_matches_selector(
            &camera,
            backend_label,
            "platform-camera-id"
        ));
        assert!(!nokhwa_camera_matches_selector(
            &camera,
            backend_label,
            "external camera"
        ));
    }

    #[cfg(any(target_os = "windows", target_os = "macos"))]
    #[test]
    fn nokhwa_camera_buffers_preserve_their_native_format() {
        let frame = nokhwa::Buffer::new(
            nokhwa::utils::Resolution::new(1, 1),
            &[255, 0, 0],
            FrameFormat::RAWRGB,
        );

        let sample = native_video_sample_from_nokhwa_buffer(&frame, 7, 11, 13)
            .expect("preserve native RGB sample");

        assert_eq!(sample.format, NativeVideoSampleFormat::Rgb8);
        assert_eq!(sample.plane_data(0), Some(&[255, 0, 0][..]));
        assert_eq!(sample.sequence, 7);
        assert_eq!(sample.timestamp_ns, 11);
        assert_eq!(sample.duration_ns, 13);
    }

    #[cfg(any(target_os = "windows", target_os = "macos"))]
    #[test]
    fn nokhwa_camera_format_rejects_severe_fps_shortfall_before_resolution_distance() {
        let formats = [
            CameraFormat::new_from(640, 480, FrameFormat::MJPEG, 30),
            CameraFormat::new_from(1280, 720, FrameFormat::YUYV, 30),
            CameraFormat::new_from(1280, 720, FrameFormat::MJPEG, 15),
        ];

        assert_eq!(
            closest_nokhwa_camera_format(&formats, 1280, 720, 30),
            Some(formats[1])
        );
        assert_eq!(
            closest_nokhwa_camera_format(&formats, 1280, 720, 18),
            Some(formats[2])
        );
        let low_rate_exact_resolution = [
            CameraFormat::new_from(640, 480, FrameFormat::MJPEG, 5),
            CameraFormat::new_from(1280, 720, FrameFormat::MJPEG, 30),
        ];
        assert_eq!(
            closest_nokhwa_camera_format(&low_rate_exact_resolution, 640, 480, 30),
            Some(low_rate_exact_resolution[1]),
            "a severe frame-rate shortfall must not win merely because its resolution is exact"
        );
        assert_eq!(requested_camera_fps(500), 2);
        assert_eq!(requested_camera_fps(333), 3);
        assert_eq!(
            requested_camera_fps(NATIVE_CAMERA_PREVIEW_FRAME_INTERVAL_MS),
            30
        );
        assert_eq!(requested_camera_fps(2_000), 1);
        assert_eq!(requested_camera_fps(1), 120);
    }
}
