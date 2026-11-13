//! Unified App Use host logic: one tool/skill with background and explicit raw
//! modes. Native OS effects are provided by the independently loaded OS module.
use serde::{Deserialize, Serialize};
use sinomo_runtime::core::{CapabilityName, ModuleCapabilities, TrustRing};
use sinomo_runtime::device::desktop::surfaces::*;
use sinomo_runtime::device::desktop::*;
use sinomo_runtime::module::*;
use sinomo_runtime::network::{ObjectRpcRequest, ObjectRpcResponse};
use sinomo_runtime::node::LocalNodePermissionStore;
use sinomo_runtime::service::{
    Service, ServiceBodyMode, ServiceContext, ServiceDisposition, ServiceError, ServiceFuture,
    ServiceRegistryBuilder, ServiceRoute, ServiceSigner,
};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
mod applications;
pub mod provider;
use provider::{DesktopProviderClient, NativeDesktopInput};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

pub const APP_CONTROL_ROUTE: &str = "/app-use/control";
pub const APP_CAPTURE_ROUTE: &str = "/app-use/capture";
pub const MEDIA_CATEGORY: &str = "media";
const MAX_SESSIONS: usize = 8;
const MAX_LEASE_MS: u64 = 300_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DesktopModule {
    AppUse,
}
impl DesktopModule {
    pub fn legacy_id(self) -> &'static str {
        "module.app-use"
    }
    pub fn name(self) -> &'static str {
        "app-use"
    }
    fn display_name(self) -> &'static str {
        "App Use"
    }
    pub fn control_route(self) -> &'static str {
        APP_CONTROL_ROUTE
    }
    pub fn capture_route(self) -> &'static str {
        APP_CAPTURE_ROUTE
    }
    fn routes(self) -> &'static [&'static str] {
        &[APP_CONTROL_ROUTE, APP_CAPTURE_ROUTE]
    }
    fn operation(self) -> &'static str {
        "app.control"
    }
    fn interface(self) -> &'static str {
        "sinomo.app-use"
    }
    fn event_kind(self) -> &'static str {
        "app.use.operation"
    }
    fn skill(self) -> &'static str {
        include_str!("../skills/app-use/SKILL.md")
    }
    pub fn module_id(self) -> ModuleId {
        ModuleId::from_legacy_named(self.legacy_id()).expect("app-use module id")
    }
    pub fn enabled(self, root: &Path) -> Result<bool, String> {
        native_rust_module_enabled(root, &self.module_id(), true).map_err(|e| e.to_string())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum DesktopControlRequest {
    VideoSources,
    StopVideo {
        source: sinomo_video::service::VideoSource,
    },
    /// Prefer this semantic observation on platforms advertising accessibility.
    Observe {
        target: DesktopTarget,
        #[serde(default = "default_max_nodes")]
        max_nodes: u32,
    },
    Act {
        target: DesktopTarget,
        snapshot_id: String,
        node_id: String,
        operation: accessibility::AccessibilityAction,
    },
    Capabilities,
    Applications,
    OpenApplication {
        application: DesktopApplication,
        #[serde(default = "default_lease_ms")]
        lease_ms: u64,
    },
    Surfaces {
        session_id: String,
    },
    OpenWindow {
        application_session_id: String,
        surface_id: String,
        #[serde(default)]
        input: bool,
        #[serde(default)]
        capture: bool,
        #[serde(default = "default_lease_ms")]
        lease_ms: u64,
    },
    OpenOverview {
        application_session_id: String,
        #[serde(default = "default_overview_windows")]
        max_windows: usize,
    },
    SelectWindow {
        overview_session_id: String,
        surface_id: Option<String>,
    },
    TrayItems,
    InvokeTray {
        item_id: String,
        operation: DesktopSemanticAction,
        #[serde(default)]
        allow_desktop_interaction: bool,
    },
    MenuItems {
        application_session_id: String,
        surface_id: String,
    },
    InvokeMenuItem {
        application_session_id: String,
        surface_id: String,
        item_id: String,
        #[serde(default)]
        allow_desktop_interaction: bool,
    },
    Targets {
        #[serde(default)]
        mode: DesktopControlMode,
    },
    Children {
        target: DesktopTarget,
    },
    Open {
        target: DesktopTarget,
        #[serde(default)]
        mode: DesktopControlMode,
        #[serde(default)]
        input: bool,
        #[serde(default)]
        capture: bool,
        #[serde(default = "default_lease_ms")]
        lease_ms: u64,
    },
    Input {
        session_id: String,
        #[serde(default)]
        surface_id: Option<String>,
        events: Vec<DesktopInput>,
    },
    Close {
        session_id: String,
    },
}
fn default_max_nodes() -> u32 {
    64
}
fn default_overview_windows() -> usize {
    4
}
fn default_lease_ms() -> u64 {
    60_000
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DesktopStreamDescriptor {
    pub route: String,
    pub category: String,
    pub session_id: String,
    pub version: u32,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum DesktopControlResponse {
    VideoSources {
        devices: Vec<sinomo_video::service::VideoDevice>,
        capture_route: String,
        control_route: String,
    },
    Observed {
        snapshot: accessibility::AccessibilitySnapshot,
    },
    Capabilities {
        capabilities: DesktopCapabilities,
        #[serde(default)]
        surfaces: DesktopSurfaceCapabilities,
    },
    Applications {
        applications: Vec<DesktopApplicationInfo>,
    },
    ApplicationOpened {
        session_id: String,
        resource_id: String,
        lease_ms: u64,
        snapshot: DesktopApplicationSnapshot,
    },
    Surfaces {
        snapshot: DesktopApplicationSnapshot,
    },
    SemanticItems {
        snapshot: DesktopSemanticSnapshot,
    },
    SemanticInvoked {
        delivery: String,
    },
    ViewSelected {
        surface_id: Option<String>,
    },
    Targets {
        targets: Vec<DesktopTargetInfo>,
    },
    Opened {
        session_id: String,
        resource_id: String,
        lease_ms: u64,
        stream: Option<DesktopStreamDescriptor>,
        #[serde(skip_serializing_if = "Option::is_none")]
        surface_id: Option<String>,
    },
    InputApplied {
        events: usize,
        delivery: String,
    },
    Closed,
    Failed {
        error: DesktopError,
    },
}
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CapturePath {
    #[default]
    Auto,
    Pixels,
    Surface,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DesktopStreamRequest {
    #[serde(default)]
    pub resolution: Option<CaptureResolution>,
    #[serde(default)]
    pub capture_path: CapturePath,
    pub session_id: String,
    #[serde(default = "default_fps")]
    pub frames_per_second: u32,
    #[serde(default = "default_cursor")]
    pub include_cursor: bool,
    #[serde(default)]
    pub encoding: sinomo_lm::RealtimeVideoInputConfig,
    #[serde(default)]
    pub transport: sinomo_video::codec::VideoTransport,
}
fn default_fps() -> u32 {
    30
}
fn default_cursor() -> bool {
    true
}

struct Session {
    owner: String,
    mode: DesktopControlMode,
    target: DesktopTarget,
    input: Option<Arc<Mutex<NativeDesktopInput>>>,
    capture: bool,
    capture_active: Arc<AtomicBool>,
    cancelled: Arc<AtomicBool>,
    expires: Instant,
    application: Option<ApplicationWindowBinding>,
}
impl Drop for Session {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Release);
        if let Some(input) = &self.input {
            input.lock().unwrap_or_else(|e| e.into_inner()).stop();
        }
    }
}
#[derive(Default)]
struct Runtime {
    root: PathBuf,
    provider: Mutex<Option<Arc<DesktopProviderClient>>>,
    sessions: Mutex<BTreeMap<String, Session>>,
    workers: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    closed: AtomicBool,
    changed: tokio::sync::Notify,
    applications: Mutex<BTreeMap<String, Arc<applications::ApplicationSession>>>,
    overviews: Mutex<BTreeMap<String, applications::OverviewSession>>,
    application_changed: tokio::sync::Notify,
}
impl Runtime {
    fn provider(&self) -> DesktopResult<Arc<DesktopProviderClient>> {
        let mut provider = self.provider.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(provider) = provider.as_ref() {
            provider.check_current()?;
            return Ok(provider.clone());
        }
        let loaded = DesktopProviderClient::resolve(&self.root)?;
        *provider = Some(loaded.clone());
        self.changed.notify_one();
        Ok(loaded)
    }
    fn stop(&self) {
        self.closed.store(true, Ordering::Release);
        for session in self
            .sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
        {
            session.cancelled.store(true, Ordering::Release);
        }
        self.changed.notify_one();
    }
    fn control(
        &self,
        kind: DesktopModule,
        permissions: LocalNodePermissionStore,
        owner: String,
        request: DesktopControlRequest,
    ) -> DesktopResult<DesktopControlResponse> {
        if self.closed.load(Ordering::Acquire) {
            return Err(DesktopError::Closed("module unloaded".into()));
        }
        // Close is always allowed for the original actor, even after consent
        // revocation, because releasing this actor's held input is compensation.
        if !matches!(
            request,
            DesktopControlRequest::Close { .. } | DesktopControlRequest::Capabilities
        ) {
            authorize_host(&permissions)?;
        }
        match request {
            DesktopControlRequest::VideoSources => Ok(DesktopControlResponse::VideoSources {
                devices: sinomo_video::service::video_sources(
                    &self.root,
                    Some(&provider::DesktopScreenVideoProvider),
                )?,
                capture_route: sinomo_video::service::CAMERA_CAPTURE.into(),
                control_route: sinomo_video::service::VIDEO_CONTROL.into(),
            }),
            DesktopControlRequest::StopVideo { source } => {
                sinomo_video::service::stop_video_source(
                    &self.root,
                    &provider::DesktopScreenVideoProvider,
                    source,
                )?;
                Ok(DesktopControlResponse::Closed)
            }
            DesktopControlRequest::Observe { target, max_nodes } => {
                authorize(&permissions, &target, "capture")?;
                match self.provider()?.invoke(
                    protocol::DesktopProviderCommand::AccessibilityObserve { target, max_nodes },
                )? {
                    protocol::DesktopProviderResponse::AccessibilityObserved { snapshot } => {
                        Ok(DesktopControlResponse::Observed { snapshot })
                    }
                    _ => Err(DesktopError::Platform(
                        "invalid accessibility observation response".into(),
                    )),
                }
            }
            DesktopControlRequest::Act {
                target,
                snapshot_id,
                node_id,
                operation,
            } => {
                authorize(&permissions, &target, "input")?;
                // Keep another actor's raw input lease exclusive through this
                // bounded semantic effect. The same actor may deliberately mix.
                let sessions = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
                if sessions.values().any(|session| {
                    session.input.is_some()
                        && session.owner != owner
                        && session.expires > Instant::now()
                        && (session.target == target || session.mode == DesktopControlMode::Raw)
                }) {
                    return Err(DesktopError::Invalid(
                        "input target already has another owner".into(),
                    ));
                }
                match self.provider()?.invoke(
                    protocol::DesktopProviderCommand::AccessibilityAct {
                        target,
                        snapshot_id,
                        node_id,
                        operation,
                    },
                )? {
                    protocol::DesktopProviderResponse::Applied { events } => {
                        Ok(DesktopControlResponse::InputApplied {
                            events,
                            delivery: "accessibility action accepted; observe to verify its effect"
                                .into(),
                        })
                    }
                    _ => Err(DesktopError::Platform(
                        "invalid accessibility action response".into(),
                    )),
                }
            }
            DesktopControlRequest::Capabilities => Ok(DesktopControlResponse::Capabilities {
                capabilities: self.provider()?.capabilities()?,
                surfaces: self
                    .provider()?
                    .surface_capabilities()
                    .unwrap_or_else(|error| DesktopSurfaceCapabilities {
                        limitations: vec![error.to_string()],
                        ..Default::default()
                    }),
            }),
            DesktopControlRequest::Targets { mode } => Ok(DesktopControlResponse::Targets {
                targets: self.provider()?.targets(mode)?,
            }),
            DesktopControlRequest::Children { target } => Ok(DesktopControlResponse::Targets {
                targets: self.provider()?.children(target)?,
            }),
            DesktopControlRequest::Open {
                target,
                mode,
                input,
                capture,
                lease_ms,
            } => {
                mode.validate_target(&target)?;
                if !(1_000..=MAX_LEASE_MS).contains(&lease_ms) || (!input && !capture) {
                    return Err(DesktopError::Invalid(
                        "open requires input and/or capture, with lease_ms in 1000..300000".into(),
                    ));
                }
                if capture {
                    authorize(&permissions, &target, "capture")?;
                }
                let cancelled = Arc::new(AtomicBool::new(false));
                let input = if input {
                    Some(Arc::new(Mutex::new(self.provider()?.open_input(
                        target.clone(),
                        mode,
                        lease_ms,
                    )?)))
                } else {
                    None
                };
                let mut sessions = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
                if self.closed.load(Ordering::Acquire) {
                    return Err(DesktopError::Closed(
                        "module unloaded during initialization".into(),
                    ));
                }
                if sessions.len() >= MAX_SESSIONS {
                    return Err(DesktopError::Invalid(
                        "module session limit reached; close an existing session".into(),
                    ));
                }
                // Only one injection owner per resource; separate concurrent
                // desktop owners could otherwise release each other's keys.
                if input.is_some()
                    && sessions.values().any(|session| {
                        session.input.is_some()
                            && (mode == DesktopControlMode::Raw
                                && session.mode == DesktopControlMode::Raw
                                || session.target == target)
                    })
                {
                    return Err(DesktopError::Invalid(
                        "input target already has an owner".into(),
                    ));
                }
                let session_id = sinomo_runtime::core::Id::generate().to_string();
                let resource_id = target.resource_id();
                sessions.insert(
                    session_id.clone(),
                    Session {
                        owner,
                        mode,
                        target,
                        input,
                        capture,
                        capture_active: Arc::new(AtomicBool::new(false)),
                        cancelled,
                        expires: Instant::now() + Duration::from_millis(lease_ms),
                        application: None,
                    },
                );
                self.changed.notify_one();
                Ok(DesktopControlResponse::Opened {
                    surface_id: None,
                    resource_id,
                    lease_ms,
                    stream: capture.then(|| DesktopStreamDescriptor {
                        route: kind.capture_route().into(),
                        category: MEDIA_CATEGORY.into(),
                        session_id: session_id.clone(),
                        version: sinomo_video::stream::GEOMETRY_STREAM_VERSION,
                    }),
                    session_id,
                })
            }
            DesktopControlRequest::Input {
                session_id,
                surface_id,
                events,
            } => {
                let (input, mode) = {
                    let sessions = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
                    let session = owned_session(&sessions, &session_id, &owner)?;
                    if session
                        .application
                        .as_ref()
                        .map(|binding| &binding.surface_id)
                        != surface_id.as_ref()
                    {
                        return Err(DesktopError::Invalid("input surface_id must match the selected window and its capture identity".into()));
                    }
                    validate_inputs(&session.target, &events)?;
                    (
                        session.input.clone().ok_or_else(|| {
                            DesktopError::PermissionDenied(
                                "session was not opened for input".into(),
                            )
                        })?,
                        session.mode,
                    )
                };
                let events = input
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .send(&events)?;
                Ok(DesktopControlResponse::InputApplied {
                    events,
                    delivery: if mode == DesktopControlMode::Background {
                        "queued window messages; application handling is best effort"
                    } else {
                        "OS input injection accepted"
                    }
                    .into(),
                })
            }
            DesktopControlRequest::Close { session_id } => {
                if self.close_application_or_overview(&session_id, &owner)? {
                    return Ok(DesktopControlResponse::Closed);
                }
                {
                    let mut sessions = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
                    if let Some(session) = sessions.get(&session_id)
                        && session.owner != owner
                    {
                        return Err(DesktopError::PermissionDenied(
                            "session belongs to another caller".into(),
                        ));
                    }
                    // Keep admission serialized until the previous input owner
                    // has released its held state, including concurrent calls.
                    drop(sessions.remove(&session_id));
                }
                self.changed.notify_one();
                Ok(DesktopControlResponse::Closed)
            }
            request => self.application_control(kind, owner, request),
        }
    }
}
fn owned_session<'a>(
    sessions: &'a BTreeMap<String, Session>,
    session_id: &str,
    owner: &str,
) -> DesktopResult<&'a Session> {
    let session = sessions
        .get(session_id)
        .ok_or_else(|| DesktopError::Closed("unknown session".into()))?;
    if session.owner != owner {
        return Err(DesktopError::PermissionDenied(
            "session belongs to another caller".into(),
        ));
    }
    if Instant::now() >= session.expires || session.cancelled.load(Ordering::Acquire) {
        return Err(DesktopError::Closed("session expired".into()));
    }
    Ok(session)
}
fn actor(ctx: &ServiceContext) -> String {
    if let Some(component) = ctx.component_caller() {
        return format!("component:{component}");
    }
    match &ctx.signer {
        ServiceSigner::Local => "local".into(),
        ServiceSigner::LocalNode {
            node_id,
            public_key,
        }
        | ServiceSigner::Remote {
            node_id,
            public_key,
        } => format!("node:{node_id}:{public_key}"),
    }
}
fn service_error(error: impl std::fmt::Display) -> ServiceError {
    ServiceError::Service(error.to_string())
}
fn publication(kind: DesktopModule) -> ComponentEventPublication {
    ComponentEventPublication::new(kind.event_kind(), "sinomo.desktop.operation", "1.0.0")
}

struct DesktopService {
    kind: DesktopModule,
    state: Arc<Runtime>,
    events: ComponentEventClient,
}
impl Service for DesktopService {
    fn name(&self) -> &'static str {
        self.kind.name()
    }
    fn routes(&self) -> &'static [&'static str] {
        self.kind.routes()
    }
    fn route_categories(&self) -> Vec<ServiceRoute> {
        self.kind
            .routes()
            .iter()
            .map(|route| service_route(self.kind, route))
            .collect()
    }
    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        Box::pin(async move {
            let owner = actor(&ctx);
            let permissions = LocalNodePermissionStore::new(ctx.data_dir.root());
            if request.route == self.kind.control_route() {
                let command: DesktopControlRequest = ctx
                    .decode_request_body_bounded(&request, 128 * 1024)
                    .await?;
                let action = match &command {
                    DesktopControlRequest::VideoSources => "video_sources",
                    DesktopControlRequest::StopVideo { .. } => "stop_video",
                    DesktopControlRequest::Observe { .. } => "observe",
                    DesktopControlRequest::Act { .. } => "act",
                    DesktopControlRequest::Capabilities => "capabilities",
                    DesktopControlRequest::Targets { .. } => "targets",
                    DesktopControlRequest::Children { .. } => "children",
                    DesktopControlRequest::Open { .. } => "open",
                    DesktopControlRequest::Input { .. } => "input",
                    DesktopControlRequest::Close { .. } => "close",
                    DesktopControlRequest::Applications => "applications",
                    DesktopControlRequest::OpenApplication { .. } => "open_application",
                    DesktopControlRequest::Surfaces { .. } => "surfaces",
                    DesktopControlRequest::OpenWindow { .. } => "open_window",
                    DesktopControlRequest::OpenOverview { .. } => "open_overview",
                    DesktopControlRequest::SelectWindow { .. } => "select_window",
                    DesktopControlRequest::TrayItems => "tray_items",
                    DesktopControlRequest::InvokeTray { .. } => "invoke_tray",
                    DesktopControlRequest::MenuItems { .. } => "menu_items",
                    DesktopControlRequest::InvokeMenuItem { .. } => "invoke_menu_item",
                };
                let subject = match &command {
                    DesktopControlRequest::StopVideo {
                        source: sinomo_video::service::VideoSource::Screen { target },
                    }
                    | DesktopControlRequest::Observe { target, .. }
                    | DesktopControlRequest::Act { target, .. }
                    | DesktopControlRequest::Open { target, .. }
                    | DesktopControlRequest::Children { target } => Some(target.resource_id()),
                    DesktopControlRequest::Input { session_id, .. }
                    | DesktopControlRequest::Close { session_id }
                    | DesktopControlRequest::Surfaces { session_id } => Some(session_id.clone()),
                    DesktopControlRequest::OpenApplication { application, .. } => {
                        Some(application.resource_id())
                    }
                    DesktopControlRequest::OpenWindow {
                        application_session_id,
                        ..
                    }
                    | DesktopControlRequest::OpenOverview {
                        application_session_id,
                        ..
                    }
                    | DesktopControlRequest::MenuItems {
                        application_session_id,
                        ..
                    }
                    | DesktopControlRequest::InvokeMenuItem {
                        application_session_id,
                        ..
                    } => Some(application_session_id.clone()),
                    DesktopControlRequest::InvokeTray { item_id, .. } => Some(item_id.clone()),
                    DesktopControlRequest::SelectWindow {
                        overview_session_id,
                        ..
                    } => Some(overview_session_id.clone()),
                    _ => None,
                };
                let state = self.state.clone();
                let kind = self.kind;
                let caller = owner.clone();
                let result = tokio::task::spawn_blocking(move || {
                    state.control(kind, permissions, caller, command)
                })
                .await
                .map_err(service_error)?;
                let response =
                    result.unwrap_or_else(|error| DesktopControlResponse::Failed { error });
                self.publish_operation(
                    &request,
                    &owner,
                    action,
                    !matches!(response, DesktopControlResponse::Failed { .. }),
                    serde_json::json!({"subject": subject, "applied_events": match &response { DesktopControlResponse::InputApplied { events, .. } => Some(*events), _ => None }}),
                )
                .await?;
                ctx.respond_value(self.kind.control_route(), response)?;
            } else {
                let capture_request: DesktopStreamRequest =
                    ctx.decode_request_body_bounded(&request, 16 * 1024).await?;
                let transport = sinomo_video::codec::select(
                    capture_request.transport,
                    capture_request.encoding.coordinate_grid,
                )
                .map_err(service_error)?;
                let version = sinomo_video::stream::GEOMETRY_STREAM_VERSION;
                let state = self.state.clone();
                let mut capture_options = capture_request.clone();
                capture_options.transport = transport;
                if let Some(resolution) = capture_request.resolution {
                    resolution.validate().map_err(service_error)?;
                }
                let actor = owner.clone();
                let (capture, cancelled, active_guard, duration) =
                    tokio::task::spawn_blocking(move || {
                        state.capture_source(&capture_options, &actor)
                    })
                    .await
                    .map_err(service_error)?
                    .map_err(service_error)?;
                let (body, worker) = sinomo_video::stream::desktop_capture_body_with_options(
                    capture,
                    capture_request.encoding,
                    duration,
                    cancelled,
                    transport,
                    capture_request.resolution,
                );
                let worker = tokio::spawn(async move {
                    let _guard = active_guard;
                    let _ = worker.await;
                });
                let mut unregistered = Some(worker);
                {
                    let mut workers = self.state.workers.lock().unwrap_or_else(|e| e.into_inner());
                    workers.retain(|worker| !worker.is_finished());
                    if !self.state.closed.load(Ordering::Acquire) {
                        workers.push(unregistered.take().expect("new capture worker"));
                    }
                }
                if let Some(worker) = unregistered {
                    drop(body);
                    let _ = worker.await;
                    return Err(service_error("module unloaded while capture opened"));
                }
                self.publish_operation(&request, &owner, "capture", true, serde_json::json!({"session_id":capture_request.session_id, "stream_version":sinomo_video::stream::GEOMETRY_STREAM_VERSION}))
                    .await?;
                ctx.respond_with_body(ObjectRpcResponse::encode_body(self.kind.capture_route(), &serde_json::json!({ "version": version, "transport":transport, "session_id": capture_request.session_id, "content_type": "application/vnd.sinomo.video-stream", "max_duration_ms": duration.as_millis() }))?, body)?;
            }
            Ok(ServiceDisposition::Continue)
        })
    }
}
struct CaptureActive(Arc<AtomicBool>);
impl Drop for CaptureActive {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}
impl DesktopService {
    async fn publish_operation(
        &self,
        request: &ObjectRpcRequest,
        actor: &str,
        action: &str,
        accepted: bool,
        evidence: serde_json::Value,
    ) -> Result<(), ServiceError> {
        use sinomo_runtime::core::*;
        // The event bus replaces the origin with the host's node identity.
        let mut event = EventEnvelope::new(Id::generate(), time::OffsetDateTime::now_utc(), Id::from_legacy_named("node.local").map_err(service_error)?, Source::new(self.kind.name()).map_err(service_error)?, EventKind::new(self.kind.event_kind()).map_err(service_error)?, Salience::new(0.5).map_err(service_error)?, Confidence::new(1.0).map_err(service_error)?, RoutingMetadata::new(RoutingScope::Local, LatencyClass::Interactive, PrivacyClass::Derived))
            .with_payload(serde_json::json!({"actor":actor,"action":action,"accepted":accepted,"route":request.route,"operation_id":request.operation_id(),"evidence":evidence}));
        if let Some(operation_id) = request.operation_id() {
            event
                .links
                .related_to
                .push(format!("operation:{operation_id}"));
        }
        self.events
            .publish(&publication(self.kind), event)
            .await
            .map_err(service_error)?;
        Ok(())
    }
}

fn service_route(kind: DesktopModule, route: &'static str) -> ServiceRoute {
    ServiceRoute::new(
        route,
        if route == kind.capture_route() {
            MEDIA_CATEGORY
        } else {
            kind.name()
        },
    )
    .with_auth_scope("local-or-cluster-member")
    .require_capability(kind.operation())
    .with_concurrency_limit(if route == kind.capture_route() { 4 } else { 8 })
    .with_timeout(Duration::from_secs(15))
    .with_response_body_mode(if route == kind.capture_route() {
        ServiceBodyMode::Required
    } else {
        ServiceBodyMode::None
    })
}

fn control_request_schema(_kind: DesktopModule) -> serde_json::Value {
    use serde_json::{Value, json};
    fn object(properties: Value, required: &[&str]) -> Value {
        json!({"type":"object","properties":properties,"required":required,"additionalProperties":false})
    }
    let integer = json!({"type":"integer"});
    let monitor = object(
        json!({"kind":{"type":"string","enum":["monitor"]},"device_name":{"type":"string","description":"Exact device_name from targets with mode raw"}}),
        &["kind", "device_name"],
    );
    let window = object(
        json!({"kind":{"type":"string","enum":["window"]},"handle":integer,"process_id":integer,"thread_id":integer,"process_started":integer}),
        &[
            "kind",
            "handle",
            "process_id",
            "thread_id",
            "process_started",
        ],
    );
    let target = json!({"oneOf":[window,monitor]});
    let mode = json!({"type":"string","enum":["background","raw"],"default":"background","description":"Prefer background window messages. Select raw explicitly only when background input is unsuitable; raw requires a monitor and affects user input/focus. No automatic fallback."});
    let mut events = vec![
        object(
            json!({"kind":{"type":"string","enum":["move"]},"x":integer,"y":integer}),
            &["kind", "x", "y"],
        ),
        object(
            json!({"kind":{"type":"string","enum":["button"]},"button":{"type":"string","enum":["left","right","middle","x1","x2"]},"down":{"type":"boolean"}}),
            &["kind", "button", "down"],
        ),
        object(
            json!({"kind":{"type":"string","enum":["text"]},"text":{"type":"string","maxLength":4096}}),
            &["kind", "text"],
        ),
        object(
            json!({"kind":{"type":"string","enum":["key"]},"code":{"type":"integer","minimum":1,"maximum":255,"description":"Windows virtual-key code in background mode; Set-1 scan code in raw mode"},"down":{"type":"boolean"},"extended":{"type":"boolean"}}),
            &["kind", "code", "down"],
        ),
        object(
            json!({"kind":{"type":"string","enum":["wheel"]},"delta":{"type":"integer","minimum":-32768,"maximum":32767,"description":"120 units per wheel tick; positive scrolls up"},"horizontal":{"type":"boolean"}}),
            &["kind", "delta"],
        ),
        object(
            json!({"kind":{"type":"string","enum":["release_all"]}}),
            &["kind"],
        ),
    ];
    {
        let fraction = json!({"type":"number","minimum":0,"maximum":1,"description":"Fraction of the entire image: 0 is left/top, 1 is right/bottom. No pixel, screen-origin, client or DPI conversion."});
        let source_size = json!({"type":"integer","minimum":1,"description":"Copy source_width/source_height from the current video frame metadata, before encoding. Stale sizes are rejected."});
        events.push(object(
            json!({"kind":{"type":"string","enum":["move_in_frame"]},"x":fraction,"y":fraction,"source_width":source_size,"source_height":source_size}),
            &["kind", "x", "y", "source_width", "source_height"],
        ));
    }
    {
        events.push(object(
            json!({"kind":{"type":"string","enum":["relative_move"]},"dx":integer,"dy":integer}),
            &["kind", "dx", "dy"],
        ));
        let axis = json!({"type":"number","minimum":-1,"maximum":1});
        let trigger = json!({"type":"number","minimum":0,"maximum":1});
        events.push(object(json!({"kind":{"type":"string","enum":["gamepad"]},"buttons":{"type":"integer","minimum":0,"description":"Windows GamepadButtons bitmask"},"left_x":axis,"left_y":axis,"right_x":axis,"right_y":axis,"left_trigger":trigger,"right_trigger":trigger}),&["kind","buttons","left_x","left_y","right_x","right_y","left_trigger","right_trigger"]));
    }
    let mut actions = vec![
        object(
            json!({"action":{"type":"string","enum":["targets"]},"mode":mode}),
            &["action"],
        ),
        object(
            json!({"action":{"type":"string","enum":["capabilities"]}}),
            &["action"],
        ),
        object(
            json!({"action":{"type":"string","enum":["open"]},"target":target,"mode":mode,"input":{"type":"boolean"},"capture":{"type":"boolean"},"lease_ms":{"type":"integer","minimum":1000,"maximum":300000}}),
            &["action", "target"],
        ),
        object(
            json!({"action":{"type":"string","enum":["input"]},"session_id":{"type":"string","description":"Exact ID from the opened input session"},"surface_id":{"type":"string","description":"Required for an application window: copy the surface_id from its open_window response and matching frame identity. Omit for legacy window/monitor sessions."},"events":{"type":"array","minItems":1,"maxItems":256,"items":{"oneOf":events}}}),
            &["action", "session_id", "events"],
        ),
        object(
            json!({"action":{"type":"string","enum":["close"]},"session_id":{"type":"string"}}),
            &["action", "session_id"],
        ),
    ];
    {
        actions.push(object(
            json!({"action":{"type":"string","enum":["children"]},"target":window}),
            &["action", "target"],
        ));
    }
    actions.push(object(json!({"action":{"type":"string","enum":["observe"]},"target":target,"max_nodes":{"type":"integer","minimum":1,"maximum":128,"default":64}}), &["action","target"]));
    actions.push(object(
        json!({"action":{"type":"string","enum":["video_sources"]}}),
        &["action"],
    ));
    actions.push(object(json!({"action":{"type":"string","enum":["stop_video"]},"source":{"type":"object","properties":{"kind":{"type":"string","enum":["screen"]},"target":target},"required":["kind","target"],"additionalProperties":false}}), &["action","source"]));
    let semantic_operations = vec![
        object(
            json!({"kind":{"type":"string","enum":["click","long_click","focus","scroll_forward","scroll_backward"]}}),
            &["kind"],
        ),
        object(
            json!({"kind":{"type":"string","enum":["set_text"]},"text":{"type":"string","maxLength":4096}}),
            &["kind", "text"],
        ),
    ];
    actions.push(object(json!({"action":{"type":"string","enum":["act"]},"target":target,"snapshot_id":{"type":"string"},"node_id":{"type":"string"},"operation":{"oneOf":semantic_operations}}), &["action","target","snapshot_id","node_id","operation"]));
    let text = json!({"type":"string"});
    let lease = json!({"type":"integer","minimum":1000,"maximum":300000});
    let app = object(
        json!({"process_id":integer,"process_started":integer}),
        &["process_id", "process_started"],
    );
    actions.extend([
        object(json!({"action":{"type":"string","enum":["applications","tray_items"]}}), &["action"]),
        object(json!({"action":{"type":"string","enum":["open_application"]},"application":app,"lease_ms":lease}), &["action","application"]),
        object(json!({"action":{"type":"string","enum":["surfaces"]},"session_id":text}), &["action","session_id"]),
        object(json!({"action":{"type":"string","enum":["open_window"]},"application_session_id":text,"surface_id":text,"input":{"type":"boolean"},"capture":{"type":"boolean"},"lease_ms":lease}), &["action","application_session_id","surface_id"]),
        object(json!({"action":{"type":"string","enum":["open_overview"]},"application_session_id":text,"max_windows":{"type":"integer","minimum":1,"maximum":4,"default":4}}), &["action","application_session_id"]),
        object(json!({"action":{"type":"string","enum":["select_window"]},"overview_session_id":text,"surface_id":{"type":["string","null"],"description":"Select a full-resolution window view; null returns to the overview. This never retargets an input session."}}), &["action","overview_session_id","surface_id"]),
        object(json!({"action":{"type":"string","enum":["invoke_tray"]},"item_id":text,"operation":{"type":"string","enum":["invoke","context_menu"]},"allow_desktop_interaction":{"type":"boolean","default":false,"description":"Explicit opt-in to app/shell UI activation. Separate native resource grants are still required."}}), &["action","item_id","operation"]),
        object(json!({"action":{"type":"string","enum":["menu_items"]},"application_session_id":text,"surface_id":text}), &["action","application_session_id","surface_id"]),
        object(json!({"action":{"type":"string","enum":["invoke_menu_item"]},"application_session_id":text,"surface_id":text,"item_id":text,"allow_desktop_interaction":{"type":"boolean","default":false}}), &["action","application_session_id","surface_id","item_id"]),
    ]);
    json!({"oneOf":actions,"description":"Unified App Use control request. Prefer observe/act when accessibility is advertised; choose or combine semantic observations, screenshots and generic media video explicitly. Background window messages are preferred; raw desktop input requires an explicit raw session. For background video grounding prefer move_in_frame: normalized fractions of the entire image plus source dimensions. Legacy move uses physical client pixels in background mode and virtual-desktop pixels in raw mode, except Wayland portal targets use portal-advertised stream-local bounds: scale video pixels to those target bounds. On Wayland, raw target discovery starts a local chooser; repeat discovery after user approval. Button names are strings, never numeric IDs. Capture is a separate media stream, not a control action."})
}

pub fn module_registration(kind: DesktopModule) -> ModuleRegistration {
    let mut registration = ModuleRegistration::native_rust(
        kind.module_id(),
        env!("CARGO_PKG_VERSION"),
        kind.name(),
        env!("CARGO_PKG_NAME"),
        kind.interface(),
        TrustRing::R2TrustedService,
        kind.name(),
    )
    .with_display_name(kind.display_name());
    let capabilities = ModuleCapabilities {
        imports: vec![CapabilityName::new("device.desktop").expect("capability")],
        exports: vec![CapabilityName::new(kind.operation()).expect("capability")],
    };
    registration.manifest.capabilities = capabilities.clone();
    // Only control is an ordinary JSON agent tool. The capture operation is a
    // media service offer; generic JSON tool adapters cannot consume live bodies.
    registration.surfaces = ModuleSurfaces::default()
        .with_object_rpc_route(
            ModuleObjectRpcRoute::new(
                kind.control_route(),
                kind.name(),
                kind.operation(),
                ModuleRouteAuth::LocalOrClusterMember,
            )
            .with_request_schema(control_request_schema(kind)),
        )
        .with_declarative_resource(
            ModuleDeclarativeResourceExport::skill(
                kind.name(),
                format!("skills/{}/SKILL.md", kind.name()),
            )
            .with_description(format!("Capture and control with {}", kind.display_name()))
            .with_embedded_text(kind.skill()),
        );
    registration.surfaces.capabilities = capabilities;
    registration.component =
        ComponentDescriptor::new(kind.module_id().as_str(), env!("CARGO_PKG_VERSION"))
            .with_display_name(kind.display_name())
            .with_offer(
                ServiceOffer::new(kind.name(), kind.interface(), "1.0.0")
                    .cluster_visible()
                    .with_provider_capability(kind.operation())
                    .with_operation(
                        ServiceOperationOffer::new("control")
                            .at_route(kind.control_route())
                            .require_caller_capability(kind.operation()),
                    )
                    .with_operation(
                        ServiceOperationOffer::new("capture")
                            .at_route(kind.capture_route())
                            .require_caller_capability(kind.operation()),
                    ),
            )
            .publish_event(publication(kind));
    for kind in [
        "app.window.added",
        "app.window.changed",
        "app.window.removed",
        "app.session.closed",
    ] {
        registration.component = registration
            .component
            .publish_event(applications::publication(kind));
    }
    registration
}

pub fn component_factory(kind: DesktopModule) -> Result<StaticComponentFactory, ComponentError> {
    let descriptor = component_descriptor_from_registration(&module_registration(kind))?;
    let caller = descriptor.clone();
    StaticComponentFactory::new(descriptor, move |context, scope| {
        let runtime = context.runtime().ok_or_else(|| {
            ComponentError::component(&caller.component_id, "desktop module requires runtime")
        })?;
        let state = Arc::new(Runtime {
            root: context.data_dir_root(),
            ..Runtime::default()
        });
        let events = context.event_client(&caller)?;
        scope.set_services(
            ServiceRegistryBuilder::default()
                .register(DesktopService {
                    kind,
                    state: state.clone(),
                    events: events.clone(),
                })
                .build(),
        );
        let mut app_shutdown = scope.shutdown_receiver();
        let app_state = state.clone();
        scope.own_task("application-window-observation", runtime.spawn(async move {
            loop {
                let active = !app_state.applications.lock().unwrap_or_else(|e| e.into_inner()).is_empty();
                let wait = async { if active { tokio::time::sleep(Duration::from_millis(250)).await; } else { std::future::pending::<()>().await; } };
                tokio::select! { _ = app_shutdown.changed() => break, _ = app_state.application_changed.notified() => {}, _ = wait => {} }
                let state = app_state.clone();
                let updates = tokio::task::spawn_blocking(move || state.application_updates()).await.unwrap_or_default();
                for (kind, payload) in updates { let _ = applications::publish(&events, &kind, payload).await; }
            }
        }));
        let mut shutdown = scope.shutdown_receiver();
        let reaper = state.clone();
        scope.own_task("desktop-session-expiry", runtime.spawn(async move {
            loop {
                let mut deadline = reaper.sessions.lock().unwrap_or_else(|e| e.into_inner()).values().map(|s| s.expires).min();
                if reaper.provider.lock().unwrap_or_else(|e| e.into_inner()).is_some() {
                    let check = Instant::now() + Duration::from_millis(500);
                    deadline = Some(deadline.map_or(check, |expires| expires.min(check)));
                }
                let wait = async { if let Some(deadline) = deadline { tokio::time::sleep_until(deadline.into()).await; } else { std::future::pending::<()>().await; } };
                tokio::select! { _ = shutdown.changed() => break, _ = reaper.changed.notified() => {}, _ = wait => {} }
                let state = reaper.clone();
                let cleanup = tokio::task::spawn_blocking(move || {
                    let mut sessions = state.sessions.lock().unwrap_or_else(|e| e.into_inner());
                    sessions.retain(|_, session| session.expires > Instant::now());
                    let mut provider = state.provider.lock().unwrap_or_else(|e| e.into_inner());
                    if provider.as_ref().is_some_and(|provider| provider.check_current().is_err()) {
                        sessions.clear();
                        provider.take();
                    } else if sessions.is_empty() && provider.as_ref().is_some_and(|provider| Arc::strong_count(provider) == 1) {
                        // No idle polling or loaded DLL once the last operation
                        // and session release their provider references.
                        provider.take();
                    }
                });
                let mut workers = reaper.workers.lock().unwrap_or_else(|e| e.into_inner());
                workers.retain(|worker| !worker.is_finished());
                workers.push(cleanup);
            }
        }));
        let drain = state.clone();
        scope.on_task_drain("desktop-devices", move |deadline| {
            let state = drain.clone();
            async move {
                state.stop();
                let cleanup = state.clone();
                tokio::task::spawn_blocking(move || {
                    let sessions = std::mem::take(
                        &mut *cleanup.sessions.lock().unwrap_or_else(|e| e.into_inner()),
                    );
                    drop(sessions);
                    cleanup
                        .overviews
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .clear();
                    let apps = std::mem::take(
                        &mut *cleanup
                            .applications
                            .lock()
                            .unwrap_or_else(|e| e.into_inner()),
                    );
                    for app in apps.values() {
                        app.cancelled.store(true, Ordering::Release);
                        app.native.close();
                    }
                    drop(apps);
                })
                .await
                .map_err(|e| e.to_string())?;
                let mut workers =
                    std::mem::take(&mut *state.workers.lock().unwrap_or_else(|e| e.into_inner()));
                while let Some(mut worker) = workers.pop() {
                    if tokio::time::timeout_at(deadline, &mut worker)
                        .await
                        .is_err()
                    {
                        workers.push(worker);
                        state
                            .workers
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .extend(workers);
                        return Err("desktop capture teardown is still draining".into());
                    }
                }
                state
                    .provider
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .take();
                Ok(())
            }
        });
        scope.defer(move || state.stop());
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn semantic_observation_and_input_require_separate_local_grants() {
        let root = sinomo_testdir::TestDir::new("semantic-consent");
        let permissions = LocalNodePermissionStore::new(root.path());
        permissions
            .mutate(|p| Ok(p.set_remote_management_enabled(true)))
            .unwrap();
        let target = DesktopTarget::Monitor {
            device_name: accessibility::ANDROID_ACCESSIBILITY_TARGET.into(),
        };
        let runtime = Runtime {
            root: root.path().into(),
            ..Default::default()
        };
        assert!(matches!(
            runtime.control(
                DesktopModule::AppUse,
                permissions.clone(),
                "test".into(),
                DesktopControlRequest::Observe {
                    target: target.clone(),
                    max_nodes: 64
                }
            ),
            Err(DesktopError::PermissionDenied(_))
        ));
        permissions
            .mutate(|p| p.expose_resource_operation(&target.resource_id(), "capture"))
            .unwrap();
        assert!(matches!(
            runtime.control(
                DesktopModule::AppUse,
                permissions,
                "test".into(),
                DesktopControlRequest::Act {
                    target,
                    snapshot_id: "untrusted".into(),
                    node_id: "1".into(),
                    operation: accessibility::AccessibilityAction::Click
                }
            ),
            Err(DesktopError::PermissionDenied(_))
        ));
    }
    #[test]
    fn application_input_rejects_a_different_surface_before_any_effect() {
        let root = sinomo_testdir::TestDir::new("app-input-surface-binding");
        let permissions = LocalNodePermissionStore::new(&root);
        let mut grants = sinomo_runtime::node::LocalNodePermissions::new();
        grants.set_remote_management_enabled(true);
        permissions.save(&grants).unwrap();
        let runtime = Runtime::default();
        runtime.sessions.lock().unwrap().insert(
            "window-session".into(),
            Session {
                owner: "owner".into(),
                mode: DesktopControlMode::Background,
                target: DesktopTarget::Window {
                    handle: 1,
                    process_id: 1,
                    thread_id: 1,
                    process_started: 1,
                },
                input: None,
                capture: false,
                capture_active: Arc::new(AtomicBool::new(false)),
                cancelled: Arc::new(AtomicBool::new(false)),
                expires: Instant::now() + Duration::from_secs(60),
                application: Some(ApplicationWindowBinding {
                    application_id: "app".into(),
                    surface_id: "window-a".into(),
                }),
            },
        );
        for surface_id in [None, Some("window-b".into())] {
            assert!(matches!(
                runtime.control(
                    DesktopModule::AppUse,
                    permissions.clone(),
                    "owner".into(),
                    DesktopControlRequest::Input {
                        session_id: "window-session".into(),
                        surface_id,
                        events: vec![DesktopInput::Text {
                            text: "never sent".into()
                        }]
                    }
                ),
                Err(DesktopError::Invalid(_))
            ));
        }
        assert!(runtime.provider.lock().unwrap().is_none());
    }
    #[test]
    fn missing_os_provider_has_no_linked_fallback() {
        let root = sinomo_testdir::TestDir::new("app-use-missing-provider");
        let permissions = LocalNodePermissionStore::new(&root);
        let mut grants = sinomo_runtime::node::LocalNodePermissions::new();
        grants.set_remote_management_enabled(true);
        permissions.save(&grants).unwrap();
        let runtime = Runtime {
            root: root.to_path_buf(),
            ..Runtime::default()
        };
        assert!(matches!(
            runtime.control(
                DesktopModule::AppUse,
                permissions,
                "owner".into(),
                DesktopControlRequest::Targets {
                    mode: DesktopControlMode::Background
                }
            ),
            Err(DesktopError::Unsupported(_))
        ));
        assert!(runtime.provider.lock().unwrap().is_none());
    }

    #[test]
    fn omitted_mode_stays_background_and_old_computer_tool_is_absent() {
        let request: DesktopControlRequest = serde_json::from_value(serde_json::json!({"action":"open","target":{"kind":"monitor","device_name":"test"},"input":true})).unwrap();
        let DesktopControlRequest::Open { mode, target, .. } = request else {
            panic!("open");
        };
        assert_eq!(mode, DesktopControlMode::Background);
        assert!(mode.validate_target(&target).is_err());
        let registration = module_registration(DesktopModule::AppUse);
        assert_eq!(registration.surfaces.object_rpc_routes.len(), 1);
        assert_eq!(
            registration.surfaces.object_rpc_routes[0].route,
            APP_CONTROL_ROUTE
        );
        assert_eq!(registration.surfaces.declarative_resources[0].id, "app-use");
    }
    #[test]
    fn desktop_control_tools_publish_typed_event_schemas() {
        {
            let kind = DesktopModule::AppUse;
            let registration = module_registration(kind);
            let schema = registration.surfaces.object_rpc_routes[0]
                .request_schema
                .as_ref()
                .unwrap();
            let actions = schema["oneOf"].as_array().unwrap();
            let input = actions
                .iter()
                .find(|action| action["properties"]["action"]["enum"][0] == "input")
                .unwrap();
            let events = input["properties"]["events"]["items"]["oneOf"]
                .as_array()
                .unwrap();
            let button = events
                .iter()
                .find(|event| event["properties"]["kind"]["enum"][0] == "button")
                .unwrap();
            assert_eq!(button["properties"]["button"]["type"], "string");
            assert!(
                button["properties"]["button"]["enum"]
                    .as_array()
                    .unwrap()
                    .contains(&serde_json::json!("left"))
            );
            let key = events
                .iter()
                .find(|event| event["properties"]["kind"]["enum"][0] == "key")
                .unwrap();
            assert_eq!(key["properties"]["code"]["minimum"], 1);
            let frame_move = events
                .iter()
                .find(|event| event["properties"]["kind"]["enum"][0] == "move_in_frame");
            assert_eq!(frame_move.is_some(), kind == DesktopModule::AppUse);
            if let Some(event) = frame_move {
                assert_eq!(event["properties"]["x"]["maximum"], 1);
                assert!(
                    event["required"]
                        .as_array()
                        .unwrap()
                        .contains(&serde_json::json!("source_width"))
                );
                let input: DesktopInput = serde_json::from_value(serde_json::json!({"kind":"move_in_frame","x":0.5,"y":0.5,"source_width":912,"source_height":681})).unwrap();
                assert!(matches!(input, DesktopInput::MoveInFrame { .. }));
            }
            assert_eq!(
                events
                    .iter()
                    .any(|event| event["properties"]["kind"]["enum"][0] == "gamepad"),
                kind == DesktopModule::AppUse
            );
        }
    }
    #[test]
    fn unified_module_finalizes_with_one_skill_and_both_input_modes() {
        let catalog = ComponentCatalog::builder()
            .register_module(module_registration(DesktopModule::AppUse))
            .unwrap()
            .build()
            .unwrap();
        drop(catalog);
        {
            let kind = DesktopModule::AppUse;
            component_factory(kind).unwrap();
            assert_eq!(
                module_registration(kind)
                    .surfaces
                    .declarative_resources
                    .len(),
                1
            );
        }
    }
    #[test]
    fn missing_consent_blocks_before_target_lookup_and_close_is_safe() {
        let root = sinomo_testdir::TestDir::new("desktop-module-consent");
        let permissions = LocalNodePermissionStore::new(&root);
        let state = Runtime::default();
        let kind = DesktopModule::AppUse;
        assert!(matches!(
            state.control(
                kind,
                permissions.clone(),
                "caller".into(),
                DesktopControlRequest::Targets {
                    mode: DesktopControlMode::Background
                }
            ),
            Err(DesktopError::PermissionDenied(_))
        ));
        assert!(matches!(
            state.control(
                kind,
                permissions,
                "caller".into(),
                DesktopControlRequest::Close {
                    session_id: "unknown".into()
                }
            ),
            Ok(DesktopControlResponse::Closed)
        ));
    }
    #[test]
    fn background_mode_rejects_monitor_targets() {
        assert!(
            DesktopControlMode::Background
                .validate_target(&DesktopTarget::Monitor {
                    device_name: "display".into()
                })
                .is_err()
        );
    }
    #[tokio::test]
    async fn mounted_routes_enforce_consent_and_withdraw_on_unload() {
        use sinomo_runtime::network::{ObjectRpcIncomingBody, ObjectRpcOutgoingResponse};
        use sinomo_runtime::service::{ResponseSink, ServiceRegistry};
        struct Sink(Arc<Mutex<Option<ObjectRpcOutgoingResponse>>>);
        impl ResponseSink for Sink {
            fn send(
                self: Box<Self>,
                response: ObjectRpcOutgoingResponse,
            ) -> Result<(), ServiceError> {
                *self.0.lock().unwrap() = Some(response);
                Ok(())
            }
        }
        let root = sinomo_testdir::TestDir::new("desktop-mounted-service");
        let data_dir = Arc::new(sinomo_runtime::local::SinomoDataDir::new(&root));
        let context =
            ComponentContext::new(data_dir.clone()).with_runtime(tokio::runtime::Handle::current());
        let mut reconciler = ComponentReconciler::new(context, ServiceRegistry::default());
        let factories: Vec<Arc<dyn ComponentFactory>> = [DesktopModule::AppUse]
            .into_iter()
            .map(|kind| Arc::new(component_factory(kind).unwrap()) as Arc<dyn ComponentFactory>)
            .collect();
        assert_eq!(reconciler.reconcile(&factories).unwrap().active.len(), 1);
        let snapshot = reconciler.service_registry().active_snapshot();
        assert_eq!(snapshot.registry().route_count(), 2);
        let response = Arc::new(Mutex::new(None));
        snapshot
            .registry()
            .dispatch(
                ServiceContext::new(
                    data_dir,
                    None,
                    ServiceSigner::Remote {
                        node_id: "peer".into(),
                        public_key: "key".into(),
                    },
                    ObjectRpcIncomingBody::default(),
                    Box::new(Sink(response.clone())),
                ),
                ObjectRpcRequest::encode_body(
                    APP_CONTROL_ROUTE,
                    "app-use",
                    &DesktopControlRequest::Targets {
                        mode: DesktopControlMode::Background,
                    },
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let outgoing = response.lock().unwrap().take().unwrap();
        let response = outgoing
            .into_materialized_value_response(64 * 1024)
            .await
            .unwrap();
        assert!(matches!(
            response.decode_body::<DesktopControlResponse>().unwrap(),
            DesktopControlResponse::Failed {
                error: DesktopError::PermissionDenied(_)
            }
        ));
        // Synchronous withdrawal intentionally reports outstanding owned work;
        // the asynchronous drain below establishes completed teardown.
        assert!(reconciler.shutdown().is_err());
        assert_eq!(
            reconciler
                .service_registry()
                .active_snapshot()
                .registry()
                .route_count(),
            0
        );
        reconciler
            .shutdown_and_wait(Duration::from_secs(3))
            .await
            .unwrap();
        assert_eq!(reconciler.reconcile(&factories).unwrap().active.len(), 1);
        reconciler
            .shutdown_and_wait(Duration::from_secs(3))
            .await
            .unwrap();
    }
    #[test]
    fn sessions_are_bound_to_actor_and_expiry() {
        let root = sinomo_testdir::TestDir::new("desktop-session-authority");
        let permissions = LocalNodePermissionStore::new(&root);
        let mut grants = sinomo_runtime::node::LocalNodePermissions::new();
        grants.set_remote_management_enabled(true);
        let target = DesktopTarget::Monitor {
            device_name: "fixture".into(),
        };
        grants
            .expose_resource_operation(target.resource_id(), "capture")
            .unwrap();
        permissions.save(&grants).unwrap();
        let state = Runtime::default();
        let response = state
            .control(
                DesktopModule::AppUse,
                permissions.clone(),
                "owner".into(),
                DesktopControlRequest::Open {
                    target,
                    mode: DesktopControlMode::Raw,
                    input: false,
                    capture: true,
                    lease_ms: 1000,
                },
            )
            .unwrap();
        let DesktopControlResponse::Opened { session_id, .. } = response else {
            panic!("open response");
        };
        assert!(
            state
                .control(
                    DesktopModule::AppUse,
                    permissions.clone(),
                    "other".into(),
                    DesktopControlRequest::Close {
                        session_id: session_id.clone()
                    }
                )
                .is_err()
        );
        {
            let mut sessions = state.sessions.lock().unwrap();
            sessions.get_mut(&session_id).unwrap().expires = Instant::now();
            assert!(owned_session(&sessions, &session_id, "owner").is_err());
        }
        state
            .control(
                DesktopModule::AppUse,
                permissions,
                "owner".into(),
                DesktopControlRequest::Close { session_id },
            )
            .unwrap();
        assert!(state.sessions.lock().unwrap().is_empty());
    }
}
