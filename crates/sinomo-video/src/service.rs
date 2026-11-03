//! Owner-authorized camera streams over the existing video body framing.
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
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct CameraCaptureRequest {
    #[serde(default)]
    pub encoding: sinomo_lm::RealtimeVideoInputConfig,
    /// A finite lease; reconnecting requires a fresh exposure check.
    pub duration_ms: u64,
    #[serde(default)]
    pub transport: crate::codec::VideoTransport,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct CameraDevice {
    pub selector: String,
    pub resource_id: String,
    pub name: String,
}
pub struct CameraService;
impl Service for CameraService {
    fn name(&self) -> &'static str {
        "media-video"
    }
    fn routes(&self) -> &'static [&'static str] {
        &[CAMERA_DEVICES, CAMERA_CAPTURE]
    }
    fn route_categories(&self) -> Vec<ServiceRoute> {
        self.routes()
            .iter()
            .map(|&route| {
                ServiceRoute::new(route, "media")
                    .with_auth_scope("local-or-cluster-member")
                    .require_capability(if route == CAMERA_DEVICES {
                        "media.video.devices.read"
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
                    let devices = if store.resource_operation_exposed("camera.default", "capture") {
                        vec![CameraDevice {
                            selector: "default".into(),
                            resource_id: "camera.default".into(),
                            name: "Default camera".into(),
                        }]
                    } else {
                        Vec::new()
                    };
                    ctx.respond_value(CAMERA_DEVICES, devices)?;
                }
                CAMERA_CAPTURE => {
                    let request: CameraCaptureRequest = ctx.decode_request_body(&request).await?;
                    if !(1..=300_000).contains(&request.duration_ms) {
                        return Err(ServiceError::Service(
                            "Camera duration must be 1–300000 ms".into(),
                        ));
                    }
                    if !store.resource_operation_exposed("camera.default", "capture") {
                        return Err(ServiceError::Service(
                            "Camera sharing is not enabled on this node".into(),
                        ));
                    }
                    let transport =
                        crate::codec::select(request.transport, request.encoding.coordinate_grid)
                            .map_err(ServiceError::Service)?;
                    let capture = tokio::task::spawn_blocking(|| {
                        device::start_native_camera_capture(Default::default())
                    })
                    .await
                    .map_err(|e| ServiceError::Service(e.to_string()))?
                    .map_err(|e| ServiceError::Service(e.to_string()))?;
                    let source = SharedCamera { capture, store };
                    let (body, worker) = crate::stream::desktop_capture_body_with_transport(
                        source,
                        request.encoding,
                        Duration::from_millis(request.duration_ms),
                        Arc::new(AtomicBool::new(false)),
                        transport,
                    );
                    let body = body
                        .with_lifetime_guard(worker)
                        .map_err(ServiceError::Network)?;
                    ctx.respond_with_body(
                        ObjectRpcResponse::encode_body(
                            CAMERA_CAPTURE,
                            &if transport == crate::codec::VideoTransport::H264 {
                                crate::stream::H264_STREAM_VERSION
                            } else {
                                crate::stream::VIDEO_STREAM_VERSION
                            },
                        )?,
                        body,
                    )?;
                }
                route => return Err(ServiceError::UnknownRoute(route.into())),
            }
            Ok(ServiceDisposition::Continue)
        })
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
