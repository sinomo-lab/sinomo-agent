//! Owner-authorized camera and screen sources over one video streaming contract.
use serde::{Deserialize, Serialize};
use sinomo_runtime::{
    device,
    network::{ObjectRpcRequest, ObjectRpcResponse},
    node::LocalNodePermissionStore,
    service::{
        Service, ServiceBodyMode, ServiceContext, ServiceDisposition, ServiceError, ServiceFuture,
        ServiceRoute,
    },
};
use std::{
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};

pub const CAMERA_DEVICES: &str = "/media/video/devices";
pub const CAMERA_CAPTURE: &str = "/media/video/capture";
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(try_from = "VideoCaptureWire", into = "VideoCaptureWire")]
pub struct VideoCaptureRequest {
    pub source: VideoSource,
    pub resolution: Option<device::desktop::CaptureResolution>,
    /// Omitted camera cadence preserves its native default; screens default to 30 fps.
    pub frames_per_second: Option<u32>,
    pub encoding: sinomo_lm::RealtimeVideoInputConfig,
    /// A finite consumer lease, independent of the projection lifetime.
    pub duration_ms: u64,
    pub transport: crate::codec::VideoTransport,
}
// A camera-only peer ignores unknown source fields, but requires duration_ms.
// Screen requests deliberately omit it so an old peer rejects before opening
// any device. Legacy camera messages retain their exact duration field.
#[derive(Serialize, Deserialize)]
struct VideoCaptureWire {
    #[serde(default)]
    source: VideoSource,
    #[serde(default)]
    resolution: Option<device::desktop::CaptureResolution>,
    #[serde(default)]
    frames_per_second: Option<u32>,
    #[serde(default)]
    encoding: sinomo_lm::RealtimeVideoInputConfig,
    #[serde(default)]
    transport: crate::codec::VideoTransport,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_duration",
        deserialize_with = "deserialize_duration"
    )]
    duration_ms: Option<u64>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_duration",
        deserialize_with = "deserialize_duration"
    )]
    lease_ms: Option<u64>,
}
fn deserialize_duration<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<u64>, D::Error> {
    u64::deserialize(deserializer).map(Some)
}
fn serialize_duration<S: serde::Serializer>(
    value: &Option<u64>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    value
        .ok_or_else(|| serde::ser::Error::custom("missing stream duration"))?
        .serialize(serializer)
}
impl TryFrom<VideoCaptureWire> for VideoCaptureRequest {
    type Error = String;
    fn try_from(wire: VideoCaptureWire) -> Result<Self, Self::Error> {
        let duration_ms = match (&wire.source, wire.duration_ms, wire.lease_ms) {
            (VideoSource::Camera, Some(duration), None) => duration,
            (VideoSource::Screen { .. }, None, Some(lease)) => lease,
            _ => return Err("camera requests require duration_ms; screen requests require lease_ms; do not send both".into()),
        };
        Ok(Self {
            source: wire.source,
            resolution: wire.resolution,
            frames_per_second: wire.frames_per_second,
            encoding: wire.encoding,
            transport: wire.transport,
            duration_ms,
        })
    }
}
impl From<VideoCaptureRequest> for VideoCaptureWire {
    fn from(request: VideoCaptureRequest) -> Self {
        let camera = matches!(request.source, VideoSource::Camera);
        Self {
            source: request.source,
            resolution: request.resolution,
            frames_per_second: request.frames_per_second,
            encoding: request.encoding,
            transport: request.transport,
            duration_ms: camera.then_some(request.duration_ms),
            lease_ms: (!camera).then_some(request.duration_ms),
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VideoDevice {
    pub source: VideoSource,
    pub selector: String,
    pub resource_id: String,
    pub name: String,
}
pub type CameraCaptureRequest = VideoCaptureRequest;
pub type CameraDevice = VideoDevice;
pub const VIDEO_CONTROL: &str = "/media/video/control";
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum VideoSource {
    #[default]
    Camera,
    Screen {
        target: device::desktop::DesktopTarget,
    },
}
impl Default for VideoCaptureRequest {
    fn default() -> Self {
        Self {
            source: VideoSource::Camera,
            resolution: None,
            frames_per_second: None,
            encoding: Default::default(),
            duration_ms: 0,
            transport: Default::default(),
        }
    }
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum VideoControlRequest {
    Stop { source: VideoSource },
}

/// The product supplies its existing native OS binding. Video owns transport,
/// while the provider keeps final device authorization and OS session lifetime.
pub trait ScreenVideoProvider: Send + Sync {
    fn targets(
        &self,
        root: &std::path::Path,
    ) -> device::desktop::DesktopResult<Vec<device::desktop::DesktopTargetInfo>>;
    fn capture(
        &self,
        root: &std::path::Path,
        config: device::desktop::DesktopCaptureConfig,
    ) -> device::desktop::DesktopResult<Box<dyn device::desktop::DesktopFrameSource>>;
    fn capture_with_ceiling(
        &self,
        root: &std::path::Path,
        config: device::desktop::DesktopCaptureConfig,
        _consumer_fps: Option<u32>,
    ) -> device::desktop::DesktopResult<Box<dyn device::desktop::DesktopFrameSource>> {
        self.capture(root, config)
    }
    fn stop(
        &self,
        root: &std::path::Path,
        target: device::desktop::DesktopTarget,
    ) -> device::desktop::DesktopResult<()>;
}
/// Shared by media routes and agent adapters; discovery never grants access.
pub fn video_sources(
    root: &std::path::Path,
    screens: Option<&dyn ScreenVideoProvider>,
) -> device::desktop::DesktopResult<Vec<VideoDevice>> {
    let store = LocalNodePermissionStore::new(root);
    let mut devices = Vec::new();
    if store.resource_operation_exposed("camera.default", "capture") {
        devices.push(VideoDevice {
            source: VideoSource::Camera,
            selector: "default".into(),
            resource_id: "camera.default".into(),
            name: "Default camera".into(),
        });
    }
    if let Some(screens) = screens {
        match screens.targets(root) {
            Ok(targets) => devices.extend(
                targets
                    .into_iter()
                    .filter(|target| {
                        store.resource_operation_exposed(&target.resource_id, "capture")
                    })
                    .map(|target| VideoDevice {
                        selector: target.resource_id.clone(),
                        resource_id: target.resource_id,
                        name: target.title,
                        source: VideoSource::Screen {
                            target: target.target,
                        },
                    }),
            ),
            Err(
                device::desktop::DesktopError::Unsupported(_)
                | device::desktop::DesktopError::PermissionDenied(_),
            ) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(devices)
}
pub fn stop_video_source(
    root: &std::path::Path,
    screens: &dyn ScreenVideoProvider,
    source: VideoSource,
) -> device::desktop::DesktopResult<()> {
    let VideoSource::Screen { target } = source else {
        return Err(device::desktop::DesktopError::Unsupported(
            "camera streams stop when their reader closes".into(),
        ));
    };
    device::desktop::authorize(&LocalNodePermissionStore::new(root), &target, "stop")?;
    screens.stop(root, target)
}
#[derive(Default)]
pub struct VideoService {
    screens: Option<Arc<dyn ScreenVideoProvider>>,
}
impl VideoService {
    pub fn with_screens(provider: Arc<dyn ScreenVideoProvider>) -> Self {
        Self {
            screens: Some(provider),
        }
    }
}
impl Service for VideoService {
    fn name(&self) -> &'static str {
        "media-video"
    }
    fn routes(&self) -> &'static [&'static str] {
        &[CAMERA_DEVICES, CAMERA_CAPTURE, VIDEO_CONTROL]
    }
    fn route_categories(&self) -> Vec<ServiceRoute> {
        self.routes()
            .iter()
            .map(|&route| {
                ServiceRoute::new(route, "media")
                    .with_auth_scope("local-or-cluster-member")
                    .require_capability(if route == CAMERA_DEVICES {
                        "media.video.devices.read"
                    } else if route == VIDEO_CONTROL {
                        "media.video.control"
                    } else {
                        "media.video.capture"
                    })
                    .with_concurrency_limit(2)
                    .with_request_body_mode(ServiceBodyMode::Optional)
                    .with_response_body_mode(if route == CAMERA_CAPTURE {
                        ServiceBodyMode::Required
                    } else {
                        ServiceBodyMode::None
                    })
            })
            .collect()
    }
    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        Box::pin(async move {
            let store = LocalNodePermissionStore::new(ctx.data_dir.root());
            match request.route.as_str() {
                CAMERA_DEVICES => {
                    let screens = self.screens.clone();
                    let root = ctx.data_dir.root().to_path_buf();
                    let devices = tokio::task::spawn_blocking(move || {
                        video_sources(&root, screens.as_deref())
                    })
                    .await
                    .map_err(|e| ServiceError::Service(e.to_string()))?
                    .map_err(|e| ServiceError::Service(e.to_string()))?;
                    ctx.respond_value(CAMERA_DEVICES, devices)?;
                }
                CAMERA_CAPTURE => {
                    let request: VideoCaptureRequest = ctx.decode_request_body(&request).await?;
                    if !(1..=300_000).contains(&request.duration_ms) {
                        return Err(ServiceError::Service(
                            "Video duration must be 1–300000 ms".into(),
                        ));
                    }
                    if request
                        .frames_per_second
                        .is_some_and(|fps| !(1..=120).contains(&fps))
                    {
                        return Err(ServiceError::Service(
                            "Video frame rate must be 1..120".into(),
                        ));
                    }
                    if let Some(bounds) = request.resolution {
                        bounds
                            .validate()
                            .map_err(|e| ServiceError::Service(e.to_string()))?;
                    }
                    let transport =
                        crate::codec::select(request.transport, request.encoding.coordinate_grid)
                            .map_err(ServiceError::Service)?;
                    let screen = matches!(&request.source, VideoSource::Screen { .. });
                    let source = match request.source {
                        VideoSource::Camera => {
                            if !store.resource_operation_exposed("camera.default", "capture") {
                                return Err(ServiceError::Service(
                                    "Camera sharing is not enabled on this node".into(),
                                ));
                            }
                            let mut config = device::NativeCameraCaptureConfig::default();
                            if let Some(fps) = request.frames_per_second {
                                config.frame_interval_ms = 1000u64.div_ceil(u64::from(fps));
                            }
                            let capture = tokio::task::spawn_blocking(move || {
                                device::start_native_camera_capture(config)
                            })
                            .await
                            .map_err(|e| ServiceError::Service(e.to_string()))?
                            .map_err(|e| ServiceError::Service(e.to_string()))?;
                            SharedVideo::Camera(SharedCamera { capture, store })
                        }
                        VideoSource::Screen { target } => {
                            device::desktop::authorize(&store, &target, "capture")
                                .map_err(|e| ServiceError::Service(e.to_string()))?;
                            let screens = self.screens.clone().ok_or_else(|| {
                                ServiceError::Service("Screen video provider is unavailable".into())
                            })?;
                            let root = ctx.data_dir.root().to_path_buf();
                            let config = device::desktop::DesktopCaptureConfig {
                                target,
                                frames_per_second: request.frames_per_second.unwrap_or(30),
                                include_cursor: false,
                                lease_ms: request.duration_ms,
                                resolution: request
                                    .resolution
                                    .or_else(|| {
                                        request.encoding.max_dimension.map(|edge| {
                                            device::desktop::CaptureResolution {
                                                width: edge,
                                                height: edge,
                                            }
                                        })
                                    })
                                    .or_else(|| {
                                        (transport == crate::codec::VideoTransport::H264).then_some(
                                            device::desktop::CaptureResolution {
                                                width: 1920,
                                                height: 1920,
                                            },
                                        )
                                    }),
                                pixel_format: (transport == crate::codec::VideoTransport::H264)
                                    .then_some(device::NativeVideoSampleFormat::H264),
                            };
                            let consumer_fps = request.encoding.max_frame_rate_hz;
                            let capture = tokio::task::spawn_blocking(move || {
                                screens.capture_with_ceiling(&root, config, consumer_fps)
                            })
                            .await
                            .map_err(|e| ServiceError::Service(e.to_string()))?
                            .map_err(|e| ServiceError::Service(e.to_string()))?;
                            SharedVideo::Screen(capture)
                        }
                    };
                    let (body, worker) = crate::stream::desktop_capture_body_with_options(
                        source,
                        request.encoding,
                        Duration::from_millis(request.duration_ms),
                        Arc::new(AtomicBool::new(false)),
                        transport,
                        request.resolution,
                    );
                    let body = body
                        .with_lifetime_guard(worker)
                        .map_err(ServiceError::Network)?;
                    ctx.respond_with_body(
                        ObjectRpcResponse::encode_body(
                            CAMERA_CAPTURE,
                            &if screen {
                                crate::stream::GEOMETRY_STREAM_VERSION
                            } else if transport == crate::codec::VideoTransport::H264 {
                                crate::stream::H264_STREAM_VERSION
                            } else {
                                crate::stream::VIDEO_STREAM_VERSION
                            },
                        )?,
                        body,
                    )?;
                }
                VIDEO_CONTROL => {
                    let request: VideoControlRequest = ctx.decode_request_body(&request).await?;
                    let VideoControlRequest::Stop { source } = request;
                    let screens = self.screens.clone().ok_or_else(|| {
                        ServiceError::Service("Screen video provider is unavailable".into())
                    })?;
                    let root = ctx.data_dir.root().to_path_buf();
                    tokio::task::spawn_blocking(move || {
                        stop_video_source(&root, screens.as_ref(), source)
                    })
                    .await
                    .map_err(|e| ServiceError::Service(e.to_string()))?
                    .map_err(|e| ServiceError::Service(e.to_string()))?;
                    ctx.respond_value(VIDEO_CONTROL, ())?;
                }
                route => return Err(ServiceError::UnknownRoute(route.into())),
            }
            Ok(ServiceDisposition::Continue)
        })
    }
}
enum SharedVideo {
    Camera(SharedCamera),
    Screen(Box<dyn device::desktop::DesktopFrameSource>),
}
impl device::desktop::DesktopFrameSource for SharedVideo {
    fn frame_rate_ceiling(&self) -> Option<u32> {
        match self {
            Self::Camera(_) => None,
            Self::Screen(source) => source.frame_rate_ceiling(),
        }
    }
    fn check_active(&self) -> device::desktop::DesktopResult<()> {
        match self {
            Self::Camera(source) => source.check_active(),
            Self::Screen(source) => source.check_active(),
        }
    }
    fn identity(&self) -> Option<device::desktop::surfaces::DesktopCaptureIdentity> {
        match self {
            Self::Camera(_) => None,
            Self::Screen(source) => source.identity(),
        }
    }
    fn next(
        &self,
        after: u64,
        timeout: Duration,
    ) -> device::desktop::DesktopResult<Option<device::NativeVideoSample>> {
        match self {
            Self::Camera(source) => source.next(after, timeout),
            Self::Screen(source) => source.next(after, timeout),
        }
    }
}

struct SharedCamera {
    capture: device::NativeCameraCapture,
    store: LocalNodePermissionStore,
}
impl device::desktop::DesktopFrameSource for SharedCamera {
    fn check_active(&self) -> device::desktop::DesktopResult<()> {
        if self
            .store
            .resource_operation_exposed("camera.default", "capture")
        {
            Ok(())
        } else {
            Err(device::desktop::DesktopError::PermissionDenied(
                "Camera sharing was revoked".into(),
            ))
        }
    }
    fn next(
        &self,
        _after: u64,
        timeout: Duration,
    ) -> device::desktop::DesktopResult<Option<device::NativeVideoSample>> {
        if !self
            .store
            .resource_operation_exposed("camera.default", "capture")
        {
            return Err(device::desktop::DesktopError::PermissionDenied(
                "Camera sharing was revoked".into(),
            ));
        }
        self.capture
            .recv_sample_timeout(timeout)
            .map_err(|e| device::desktop::DesktopError::Platform(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn screen_stop_requires_its_own_grant_and_discovery_does_not_stop_it() {
        use device::desktop::*;
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct Screens(AtomicUsize);
        impl ScreenVideoProvider for Screens {
            fn targets(&self, _: &std::path::Path) -> DesktopResult<Vec<DesktopTargetInfo>> {
                let target = DesktopTarget::Monitor {
                    device_name: "fixture-screen".into(),
                };
                Ok(vec![DesktopTargetInfo {
                    resource_id: target.resource_id(),
                    target,
                    title: "Fixture".into(),
                    bounds: [0, 0, 640, 480],
                    client_bounds: None,
                    minimized: false,
                }])
            }
            fn capture(
                &self,
                _: &std::path::Path,
                _: DesktopCaptureConfig,
            ) -> DesktopResult<Box<dyn DesktopFrameSource>> {
                Err(DesktopError::Unsupported("not a capture fixture".into()))
            }
            fn stop(&self, _: &std::path::Path, _: DesktopTarget) -> DesktopResult<()> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }
        let root = std::env::temp_dir().join(format!(
            "video-source-authority-{}",
            sinomo_runtime::core::Id::generate()
        ));
        let store = LocalNodePermissionStore::new(&root);
        let screens = Screens(AtomicUsize::new(0));
        let target = DesktopTarget::Monitor {
            device_name: "fixture-screen".into(),
        };
        store
            .mutate(|p| {
                p.set_remote_management_enabled(true);
                p.expose_resource_operation(&target.resource_id(), "capture")
            })
            .unwrap();
        let devices = video_sources(&root, Some(&screens)).unwrap();
        assert_eq!(devices.len(), 1);
        let source = devices[0].source.clone();
        assert!(matches!(
            stop_video_source(&root, &screens, source.clone()),
            Err(DesktopError::PermissionDenied(_))
        ));
        assert_eq!(screens.0.load(Ordering::SeqCst), 0);
        store
            .mutate(|p| p.expose_resource_operation(&target.resource_id(), "stop"))
            .unwrap();
        stop_video_source(&root, &screens, source).unwrap();
        assert_eq!(screens.0.load(Ordering::SeqCst), 1);
        assert!(stop_video_source(&root, &screens, VideoSource::Camera).is_err());
        assert_eq!(screens.0.load(Ordering::SeqCst), 1);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn camera_requests_remain_compatible_and_sources_are_explicit() {
        let camera: VideoCaptureRequest =
            serde_json::from_value(serde_json::json!({"duration_ms":1000})).unwrap();
        assert_eq!(camera.source, VideoSource::Camera);
        assert_eq!(camera.frames_per_second, None);
        let screen: VideoCaptureRequest = serde_json::from_value(serde_json::json!({"lease_ms":1000,"source":{"kind":"screen","target":{"kind":"monitor","device_name":"android:display:0:test"}}})).unwrap();
        assert!(matches!(screen.source, VideoSource::Screen { .. }));
        assert!(
            serde_json::from_value::<VideoCaptureRequest>(
                serde_json::json!({"duration_ms":1000,"source":{"kind":"unknown"}})
            )
            .is_err()
        );
    }
    #[test]
    fn camera_only_peers_reject_screen_messages_before_device_admission() {
        use sinomo_runtime::core::object::{LengthMode, serde_ext};
        #[derive(Serialize, Deserialize)]
        struct LegacyCamera {
            duration_ms: u64,
        }
        let request = VideoCaptureRequest {
            source: VideoSource::Screen {
                target: device::desktop::DesktopTarget::Monitor {
                    device_name: "android:display:0:test".into(),
                },
            },
            duration_ms: 1000,
            ..Default::default()
        };
        let wire = serde_ext::to_bytes(&request, 1, LengthMode::U32).unwrap();
        assert!(serde_ext::from_bytes::<LegacyCamera>(&wire).is_err());
        assert!(matches!(
            serde_ext::from_bytes::<VideoCaptureRequest>(&wire)
                .unwrap()
                .source,
            VideoSource::Screen { .. }
        ));
        let legacy =
            serde_ext::to_bytes(&LegacyCamera { duration_ms: 1000 }, 1, LengthMode::U32).unwrap();
        assert_eq!(
            serde_ext::from_bytes::<VideoCaptureRequest>(&legacy)
                .unwrap()
                .duration_ms,
            1000
        );
        let camera = VideoCaptureRequest {
            duration_ms: 1000,
            ..Default::default()
        };
        let wire = serde_ext::to_bytes(&camera, 1, LengthMode::U32).unwrap();
        assert_eq!(
            serde_ext::from_bytes::<LegacyCamera>(&wire)
                .unwrap()
                .duration_ms,
            1000
        );
        assert!(serde_json::from_value::<VideoCaptureRequest>(serde_json::json!({"source":{"kind":"screen","target":{"kind":"monitor","device_name":"screen"}},"duration_ms":1000})).is_err());
    }
    #[test]
    fn stopping_a_projection_has_separate_route_authority() {
        let routes = VideoService::default().route_categories();
        let control = routes
            .iter()
            .find(|route| route.route == VIDEO_CONTROL)
            .unwrap();
        assert_eq!(
            control.auth_scope.as_deref(),
            Some("local-or-cluster-member")
        );
        // The wire schema requires an explicit source, never an implicit current projection.
        assert!(
            serde_json::from_value::<VideoControlRequest>(serde_json::json!({"action":"stop"}))
                .is_err()
        );
    }
}
