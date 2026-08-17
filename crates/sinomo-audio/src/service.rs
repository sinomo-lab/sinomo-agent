//! Cross-node audio device access as an object-RPC service.
//!
//! Sinomo places agents and devices independently, so a call may run on one node while the
//! microphone and speaker it uses are attached to another. This service exposes a node's own
//! devices to authorized cluster peers; the call path reaches them through
//! [`RealtimeAudioSource`](crate::RealtimeAudioSource) and
//! [`RealtimeAudioSink`](crate::RealtimeAudioSink) implementations without knowing which node
//! answered.
//!
//! Both routes carry media as one long-lived object-RPC call with an incrementally streamed body,
//! which is the contract's primitive for continuous payloads — not a sequence of small calls.
//! Capture streams the response body; playback streams the request body.
//!
//! Serving a device is not implied by cluster membership. Each request is re-authorized at the
//! node that owns the device against its node-local resource exposure grants, which cannot be set
//! remotely.

use crate::NativeAudioSampleFormat;
use serde::{Deserialize, Serialize};
use sinomo_runtime::network::{ObjectRpcBodySource, ObjectRpcRequest, ObjectRpcResponse};
use sinomo_runtime::node::LocalNodePermissionStore;
use sinomo_runtime::service::{
    Service, ServiceBodyMode, ServiceContext, ServiceDisposition, ServiceError, ServiceFuture,
    ServiceRegistryBuilder, ServiceRoute,
};
#[cfg(feature = "native-devices")]
use std::time::Duration;

pub const MEDIA_RPC_CATEGORY: &str = "media";
pub const MEDIA_AUDIO_DEVICES_ROUTE: &str = "/media/audio/devices";
pub const MEDIA_AUDIO_CAPTURE_ROUTE: &str = "/media/audio/capture";
pub const MEDIA_AUDIO_PLAYBACK_ROUTE: &str = "/media/audio/playback";

pub const MEDIA_AUDIO_OBJECT_RPC_ROUTES: &[&str] = &[
    MEDIA_AUDIO_DEVICES_ROUTE,
    MEDIA_AUDIO_CAPTURE_ROUTE,
    MEDIA_AUDIO_PLAYBACK_ROUTE,
];

/// Operation names checked against node-local resource exposure grants.
pub const MEDIA_CAPTURE_OPERATION: &str = "capture";
pub const MEDIA_PLAYBACK_OPERATION: &str = "play";

const MEDIA_AUDIO_ROUTE_AUTH_SCOPE: &str = "media.audio";
/// Bounds how long a handler waits for the next captured chunk before re-checking the stream.
#[cfg(feature = "native-devices")]
const CAPTURE_POLL_INTERVAL: Duration = Duration::from_millis(20);

/// Asks a node which of its audio devices it serves.
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct MediaAudioDevicesRequest {
    /// Restricts the answer to one device kind, such as `microphone`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
}

/// One audio device a node serves to the cluster.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MediaAudioDevice {
    pub kind: String,
    /// The selector to send back when opening a stream for this device.
    pub selector: String,
    pub name: String,
    /// The node-local resource whose grant makes this device servable.
    pub resource_id: String,
    /// Operations the owning node has exposed for this device.
    pub operations: Vec<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub is_default: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
}

/// The devices one node serves.
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct MediaAudioDevicesResponse {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub devices: Vec<MediaAudioDevice>,
}

/// Opens a capture stream on the node that owns the microphone.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MediaAudioCaptureRequest {
    /// Device selector on the serving node; `None` uses that node's default microphone.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device: Option<String>,
    pub sample_rate_hz: u32,
    pub sample_format: NativeAudioSampleFormat,
    pub chunk_ms: u64,
    /// Stops the stream after this many chunks so a caller cannot open an unbounded capture.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_chunks: Option<u64>,
}

/// Reports the stream a capture request opened.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MediaAudioCaptureResponse {
    pub sample_rate_hz: u32,
    pub sample_format: NativeAudioSampleFormat,
}

/// Opens a playback stream on the node that owns the speaker.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MediaAudioPlaybackRequest {
    /// Device selector on the serving node; `None` uses that node's default speaker.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device: Option<String>,
    pub input_sample_rate_hz: u32,
    pub input_sample_format: NativeAudioSampleFormat,
}

/// Reports what a completed playback stream wrote.
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct MediaAudioPlaybackResponse {
    pub written_bytes: u64,
}

/// Returns the node-local resource identifier that grants access to an audio device.
///
/// The selector is part of the identity so exposing one microphone does not expose every
/// microphone on the node.
pub fn audio_resource_id(kind: &str, device: Option<&str>) -> String {
    let selector = device
        .map(str::trim)
        .filter(|device| !device.is_empty())
        .unwrap_or("default");
    format!("{kind}.{}", sanitize_resource_component(selector))
}

/// Reduces a device selector to the characters a resource identifier accepts.
fn sanitize_resource_component(value: &str) -> String {
    let sanitized = value
        .chars()
        .map(|character| match character {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' => character,
            _ => '-',
        })
        .collect::<String>();
    let sanitized = sanitized.trim_matches(['-', '.']).to_string();
    if sanitized.is_empty() {
        "default".to_string()
    } else {
        sanitized
    }
}

pub fn register_services(builder: ServiceRegistryBuilder) -> ServiceRegistryBuilder {
    builder.register(MediaAudioService::new())
}

#[derive(Default)]
pub struct MediaAudioService;

impl MediaAudioService {
    pub fn new() -> Self {
        Self
    }
}

impl Service for MediaAudioService {
    fn name(&self) -> &'static str {
        "media-audio"
    }

    fn routes(&self) -> &'static [&'static str] {
        MEDIA_AUDIO_OBJECT_RPC_ROUTES
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        MEDIA_AUDIO_OBJECT_RPC_ROUTES
            .iter()
            .copied()
            .map(media_audio_service_route)
            .collect()
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        Box::pin(async move {
            match request.route.as_str() {
                MEDIA_AUDIO_DEVICES_ROUTE => {
                    let devices: MediaAudioDevicesRequest =
                        ctx.decode_request_body(&request).await?;
                    let response = served_audio_devices(&ctx, devices);
                    ctx.respond_value(MEDIA_AUDIO_DEVICES_ROUTE, response)?;
                }
                MEDIA_AUDIO_CAPTURE_ROUTE => {
                    let capture: MediaAudioCaptureRequest =
                        ctx.decode_request_body(&request).await?;
                    handle_capture(&mut ctx, capture).await?;
                }
                MEDIA_AUDIO_PLAYBACK_ROUTE => {
                    let playback: MediaAudioPlaybackRequest =
                        ctx.decode_request_body(&request).await?;
                    handle_playback(&mut ctx, playback).await?;
                }
                other => return Err(ServiceError::UnknownRoute(other.to_string())),
            }
            Ok(ServiceDisposition::Continue)
        })
    }
}

fn media_audio_service_route(route: &'static str) -> ServiceRoute {
    let (capability, request_body_mode, response_body_mode, concurrency_limit) = match route {
        MEDIA_AUDIO_DEVICES_ROUTE => (
            "media.audio.devices.read",
            ServiceBodyMode::Optional,
            ServiceBodyMode::None,
            8,
        ),
        MEDIA_AUDIO_CAPTURE_ROUTE => (
            "media.audio.capture",
            ServiceBodyMode::Optional,
            ServiceBodyMode::Required,
            // One in-flight stream per device keeps a caller from opening many concurrent
            // captures.
            2,
        ),
        _ => (
            "media.audio.play",
            ServiceBodyMode::Required,
            ServiceBodyMode::None,
            2,
        ),
    };
    ServiceRoute::new(route, MEDIA_RPC_CATEGORY)
        .with_auth_scope(MEDIA_AUDIO_ROUTE_AUTH_SCOPE)
        .require_capability(capability)
        .with_concurrency_limit(concurrency_limit)
        .with_request_body_mode(request_body_mode)
        .with_response_body_mode(response_body_mode)
}

/// Lists the audio devices this node serves.
///
/// Only devices with a node-local exposure grant appear. Listing is therefore a statement of what
/// this node has consented to share, not an inventory of its hardware — enumerating everything
/// attached to a node is what the remote-management observation is for, and that needs a far
/// broader grant than serving one microphone.
fn served_audio_devices(
    ctx: &ServiceContext,
    request: MediaAudioDevicesRequest,
) -> MediaAudioDevicesResponse {
    let store = LocalNodePermissionStore::new(ctx.data_dir.root());
    let wanted_kind = request
        .kind
        .as_deref()
        .map(str::trim)
        .filter(|kind| !kind.is_empty());
    let devices = node_audio_devices()
        .into_iter()
        .filter(|device| wanted_kind.is_none_or(|kind| kind == device.kind))
        .filter_map(|device| {
            let resource_id = audio_resource_id(&device.kind, Some(&device.selector));
            let operations = audio_device_operations(&device.kind)
                .iter()
                .copied()
                .filter(|operation| store.resource_operation_exposed(&resource_id, operation))
                .map(str::to_string)
                .collect::<Vec<_>>();
            if operations.is_empty() {
                return None;
            }
            Some(MediaAudioDevice {
                kind: device.kind,
                selector: device.selector,
                name: device.name,
                resource_id,
                operations,
                is_default: device.is_default,
            })
        })
        .collect();
    MediaAudioDevicesResponse { devices }
}

/// Returns the operations that make sense for a device kind.
fn audio_device_operations(kind: &str) -> &'static [&'static str] {
    match kind {
        "microphone" => &[MEDIA_CAPTURE_OPERATION],
        "speaker" => &[MEDIA_PLAYBACK_OPERATION],
        _ => &[],
    }
}

/// One audio device attached to this node, before exposure filtering.
struct NodeAudioDevice {
    kind: String,
    selector: String,
    name: String,
    is_default: bool,
}

/// Rejects a request that the serving node has not exposed for this device and operation.
///
/// This runs at the node that owns the device, immediately before the device is touched, because
/// membership and route discovery never imply consent to use a device.
fn require_exposed_device(
    ctx: &ServiceContext,
    resource_id: &str,
    operation: &str,
) -> Result<(), ServiceError> {
    let store = LocalNodePermissionStore::new(ctx.data_dir.root());
    if store.resource_operation_exposed(resource_id, operation) {
        return Ok(());
    }
    Err(ServiceError::Service(format!(
        "this node has not exposed `{resource_id}` for `{operation}`; grant it locally on the node that owns the device"
    )))
}

async fn handle_capture(
    ctx: &mut ServiceContext,
    request: MediaAudioCaptureRequest,
) -> Result<(), ServiceError> {
    let resource_id = audio_resource_id("microphone", request.device.as_deref());
    require_exposed_device(ctx, &resource_id, MEDIA_CAPTURE_OPERATION)?;
    let sample_rate_hz = request.sample_rate_hz;
    let sample_format = request.sample_format;
    let stream = capture_reader(request)?;
    // The length is unknown because the stream ends when the caller stops reading or the chunk
    // bound is reached, so the body declares deferred completion. The device worker is held by
    // the body so releasing the body releases the microphone.
    let body = ObjectRpcBodySource::async_reader(stream.reader, /*total_bytes*/ None)
        .with_lifetime_guard(stream.worker)
        .map_err(ServiceError::Network)?;
    ctx.respond_with_body(
        ObjectRpcResponse::encode_body(
            MEDIA_AUDIO_CAPTURE_ROUTE,
            &MediaAudioCaptureResponse {
                sample_rate_hz,
                sample_format,
            },
        )?,
        body,
    )
}

async fn handle_playback(
    ctx: &mut ServiceContext,
    request: MediaAudioPlaybackRequest,
) -> Result<(), ServiceError> {
    let resource_id = audio_resource_id("speaker", request.device.as_deref());
    require_exposed_device(ctx, &resource_id, MEDIA_PLAYBACK_OPERATION)?;
    let written_bytes = play_request_body(ctx, request).await?;
    ctx.respond_value(
        MEDIA_AUDIO_PLAYBACK_ROUTE,
        MediaAudioPlaybackResponse { written_bytes },
    )
}

#[cfg(feature = "native-devices")]
mod native {
    use super::*;
    use crate::{NativeAudioCaptureConfig, NativeSpeakerPlaybackConfig};
    use sinomo_runtime::device::{DeviceKind, enumerate_native_devices_without_camera};
    use sinomo_runtime::node::NodeId;

    pub(super) fn node_audio_devices() -> Vec<NodeAudioDevice> {
        // The node id only labels the inventory; this call always enumerates local hardware.
        let node_id =
            NodeId::from_legacy_named("node_media_service").unwrap_or_else(|_| NodeId::generate());
        enumerate_native_devices_without_camera(node_id)
            .devices
            .into_iter()
            .filter_map(|device| {
                let kind = match device.profile.kind {
                    DeviceKind::Microphone => "microphone",
                    DeviceKind::Speaker => "speaker",
                    _ => return None,
                };
                Some(NodeAudioDevice {
                    kind: kind.to_string(),
                    selector: device.native_id,
                    name: device.profile.display_name,
                    is_default: device.is_default,
                })
            })
            .collect()
    }
    use std::io;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::{AsyncRead, ReadBuf};
    use tokio::sync::mpsc;

    /// A capture body plus the device worker whose lifetime it owns.
    pub(super) struct CaptureStream {
        pub(super) reader: CaptureReader,
        pub(super) worker: tokio::task::JoinHandle<()>,
    }

    /// Streams captured PCM from a blocking device thread into the response body.
    pub(super) struct CaptureReader {
        frames: mpsc::Receiver<Result<Vec<u8>, String>>,
        pending: Vec<u8>,
        offset: usize,
        failed: bool,
    }

    impl AsyncRead for CaptureReader {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            loop {
                if self.offset < self.pending.len() {
                    let take = (self.pending.len() - self.offset).min(buf.remaining());
                    let offset = self.offset;
                    let chunk = self.pending[offset..offset + take].to_vec();
                    buf.put_slice(&chunk);
                    self.offset += take;
                    return Poll::Ready(Ok(()));
                }
                if self.failed {
                    return Poll::Ready(Ok(()));
                }
                match self.frames.poll_recv(cx) {
                    // A closed channel ends the body; the caller sees a clean end of stream.
                    Poll::Ready(None) => return Poll::Ready(Ok(())),
                    Poll::Ready(Some(Ok(frame))) => {
                        self.pending = frame;
                        self.offset = 0;
                    }
                    Poll::Ready(Some(Err(detail))) => {
                        self.failed = true;
                        return Poll::Ready(Err(io::Error::other(detail)));
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }
        }
    }

    pub(super) fn capture_reader(
        request: MediaAudioCaptureRequest,
    ) -> Result<CaptureStream, ServiceError> {
        let mut config =
            NativeAudioCaptureConfig::new(request.sample_rate_hz, request.sample_format);
        config.device = request.device.clone();
        config.chunk_ms = request.chunk_ms;
        let capture = crate::start_native_microphone_capture(config)
            .map_err(|error| ServiceError::Service(format!("open microphone: {error}")))?;
        let (frames_tx, frames) = mpsc::channel(super::CAPTURE_QUEUE_CAPACITY);
        let max_chunks = request.max_chunks;
        // Device reads block, so they run on a blocking thread and hand frames to the body.
        let worker = tokio::task::spawn_blocking(move || {
            let mut sent = 0_u64;
            loop {
                if max_chunks.is_some_and(|max| sent >= max) {
                    return;
                }
                match capture.recv_timeout(super::CAPTURE_POLL_INTERVAL) {
                    Ok(Some(chunk)) => {
                        sent = sent.saturating_add(1);
                        if frames_tx.blocking_send(Ok(chunk.audio)).is_err() {
                            // The caller stopped reading; releasing the device is the only
                            // correct response.
                            return;
                        }
                    }
                    Ok(None) => {
                        if frames_tx.is_closed() {
                            return;
                        }
                    }
                    Err(error) => {
                        let _ = frames_tx.blocking_send(Err(error.to_string()));
                        return;
                    }
                }
            }
        });
        Ok(CaptureStream {
            reader: CaptureReader {
                frames,
                pending: Vec::new(),
                offset: 0,
                failed: false,
            },
            worker,
        })
    }

    pub(super) async fn play_request_body(
        ctx: &mut ServiceContext,
        request: MediaAudioPlaybackRequest,
    ) -> Result<u64, ServiceError> {
        let playback = crate::start_native_speaker_playback(NativeSpeakerPlaybackConfig {
            device: request.device.clone(),
            input_sample_rate_hz: request.input_sample_rate_hz,
            input_sample_format: request.input_sample_format,
        })
        .map_err(|error| ServiceError::Service(format!("open speaker: {error}")))?;
        let mut body = ctx.take_request_body();
        let mut written_bytes = 0_u64;
        while let Some(chunk) = body
            .next_chunk()
            .await
            .map_err(|error| ServiceError::Service(format!("read playback body: {error}")))?
        {
            written_bytes = written_bytes.saturating_add(chunk.len() as u64);
            playback
                .write(&chunk)
                .map_err(|error| ServiceError::Service(format!("write speaker: {error}")))?;
        }
        Ok(written_bytes)
    }
}

#[cfg(not(feature = "native-devices"))]
mod native {
    use super::*;
    use tokio::io::Empty;

    pub(super) struct CaptureStream {
        pub(super) reader: Empty,
        pub(super) worker: tokio::task::JoinHandle<()>,
    }

    /// A node built without device support serves no devices, so it advertises none.
    pub(super) fn node_audio_devices() -> Vec<NodeAudioDevice> {
        Vec::new()
    }

    pub(super) fn capture_reader(
        _request: MediaAudioCaptureRequest,
    ) -> Result<CaptureStream, ServiceError> {
        Err(unavailable())
    }

    pub(super) async fn play_request_body(
        _ctx: &mut ServiceContext,
        _request: MediaAudioPlaybackRequest,
    ) -> Result<u64, ServiceError> {
        Err(unavailable())
    }

    fn unavailable() -> ServiceError {
        ServiceError::Service(
            "this node was built without native device support, so it serves no audio devices"
                .to_string(),
        )
    }
}

#[cfg(feature = "native-devices")]
const CAPTURE_QUEUE_CAPACITY: usize = 32;

use native::{capture_reader, node_audio_devices, play_request_body};

#[cfg(test)]
#[path = "service_tests.rs"]
mod tests;
