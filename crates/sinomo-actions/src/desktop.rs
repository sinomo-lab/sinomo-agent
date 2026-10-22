//! Computer Use and App Use are independently enabled lifecycle units. They
//! share device contracts and media plumbing, not input authority or sessions.
use serde::{Deserialize, Serialize};
use sinomo_runtime::core::{CapabilityName, ModuleCapabilities, TrustRing};
use sinomo_runtime::device::desktop::*;
use sinomo_runtime::module::*;
use sinomo_runtime::network::{ObjectRpcRequest, ObjectRpcResponse};
use sinomo_runtime::node::LocalNodePermissionStore;
use sinomo_runtime::service::{
    Service, ServiceBodyMode, ServiceContext, ServiceDisposition, ServiceError, ServiceFuture,
    ServiceRegistryBuilder, ServiceRoute, ServiceSigner,
};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

pub const COMPUTER_CONTROL_ROUTE: &str = "/computer-use/control";
pub const COMPUTER_CAPTURE_ROUTE: &str = "/computer-use/capture";
pub const APP_CONTROL_ROUTE: &str = "/app-use/control";
pub const APP_CAPTURE_ROUTE: &str = "/app-use/capture";
pub const MEDIA_CATEGORY: &str = "media";
const MAX_SESSIONS: usize = 8;
const MAX_LEASE_MS: u64 = 300_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DesktopModule {
    ComputerUse,
    AppUse,
}
impl DesktopModule {
    pub fn legacy_id(self) -> &'static str {
        match self {
            Self::ComputerUse => "module.computer-use",
            Self::AppUse => "module.app-use",
        }
    }
    pub fn name(self) -> &'static str {
        match self {
            Self::ComputerUse => "computer-use",
            Self::AppUse => "app-use",
        }
    }
    fn display_name(self) -> &'static str {
        match self {
            Self::ComputerUse => "Computer Use",
            Self::AppUse => "App Use",
        }
    }
    pub fn control_route(self) -> &'static str {
        match self {
            Self::ComputerUse => COMPUTER_CONTROL_ROUTE,
            Self::AppUse => APP_CONTROL_ROUTE,
        }
    }
    pub fn capture_route(self) -> &'static str {
        match self {
            Self::ComputerUse => COMPUTER_CAPTURE_ROUTE,
            Self::AppUse => APP_CAPTURE_ROUTE,
        }
    }
    fn routes(self) -> &'static [&'static str] {
        match self {
            Self::ComputerUse => &[COMPUTER_CONTROL_ROUTE, COMPUTER_CAPTURE_ROUTE],
            Self::AppUse => &[APP_CONTROL_ROUTE, APP_CAPTURE_ROUTE],
        }
    }
    fn operation(self) -> &'static str {
        match self {
            Self::ComputerUse => "computer.control",
            Self::AppUse => "app.control",
        }
    }
    fn interface(self) -> &'static str {
        match self {
            Self::ComputerUse => "sinomo.computer-use",
            Self::AppUse => "sinomo.app-use",
        }
    }
    fn event_kind(self) -> &'static str {
        match self {
            Self::ComputerUse => "computer.use.operation",
            Self::AppUse => "app.use.operation",
        }
    }
    fn skill(self) -> &'static str {
        match self {
            Self::ComputerUse => include_str!("../skills/computer-use/SKILL.md"),
            Self::AppUse => include_str!("../skills/app-use/SKILL.md"),
        }
    }
    fn check_target(self, target: &DesktopTarget) -> DesktopResult<()> {
        if target.is_window() != (self == Self::AppUse) {
            return Err(DesktopError::Invalid(
                "target belongs to the other desktop module".into(),
            ));
        }
        Ok(())
    }
    pub fn module_id(self) -> ModuleId {
        ModuleId::from_legacy_named(self.legacy_id()).expect("desktop module id")
    }
    pub fn enabled(self, root: &Path) -> Result<bool, String> {
        native_rust_module_enabled(root, &self.module_id(), true).map_err(|e| e.to_string())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum DesktopControlRequest {
    Capabilities,
    Targets,
    Children {
        target: DesktopTarget,
    },
    Open {
        target: DesktopTarget,
        #[serde(default)]
        input: bool,
        #[serde(default)]
        capture: bool,
        #[serde(default = "default_lease_ms")]
        lease_ms: u64,
    },
    Input {
        session_id: String,
        events: Vec<DesktopInput>,
    },
    Close {
        session_id: String,
    },
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
    Capabilities {
        capabilities: DesktopCapabilities,
    },
    Targets {
        targets: Vec<DesktopTargetInfo>,
    },
    Opened {
        session_id: String,
        resource_id: String,
        lease_ms: u64,
        stream: Option<DesktopStreamDescriptor>,
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
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DesktopStreamRequest {
    pub session_id: String,
    #[serde(default = "default_fps")]
    pub frames_per_second: u32,
    #[serde(default = "default_cursor")]
    pub include_cursor: bool,
    #[serde(default)]
    pub encoding: sinomo_lm::RealtimeVideoInputConfig,
}
fn default_fps() -> u32 {
    30
}
fn default_cursor() -> bool {
    true
}

struct Session {
    owner: String,
    target: DesktopTarget,
    input: Option<Arc<Mutex<DesktopInputSession>>>,
    capture: bool,
    capture_active: Arc<AtomicBool>,
    cancelled: Arc<AtomicBool>,
    expires: Instant,
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
    sessions: Mutex<BTreeMap<String, Session>>,
    workers: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    closed: AtomicBool,
    changed: tokio::sync::Notify,
}
impl Runtime {
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
            DesktopControlRequest::Capabilities => Ok(DesktopControlResponse::Capabilities {
                capabilities: capabilities(),
            }),
            DesktopControlRequest::Targets => Ok(DesktopControlResponse::Targets {
                targets: enumerate_targets(&permissions, kind == DesktopModule::AppUse)?,
            }),
            DesktopControlRequest::Children { target } => {
                if kind != DesktopModule::AppUse {
                    return Err(DesktopError::Invalid(
                        "child windows belong to App Use".into(),
                    ));
                }
                Ok(DesktopControlResponse::Targets {
                    targets: enumerate_child_windows(&permissions, &target)?,
                })
            }
            DesktopControlRequest::Open {
                target,
                input,
                capture,
                lease_ms,
            } => {
                kind.check_target(&target)?;
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
                    Some(Arc::new(Mutex::new(DesktopInputSession::open_with_lease(
                        target.clone(),
                        permissions,
                        Duration::from_millis(lease_ms),
                        cancelled.clone(),
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
                            && (kind == DesktopModule::ComputerUse || session.target == target)
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
                        target,
                        input,
                        capture,
                        capture_active: Arc::new(AtomicBool::new(false)),
                        cancelled,
                        expires: Instant::now() + Duration::from_millis(lease_ms),
                    },
                );
                self.changed.notify_one();
                Ok(DesktopControlResponse::Opened {
                    resource_id,
                    lease_ms,
                    stream: capture.then(|| DesktopStreamDescriptor {
                        route: kind.capture_route().into(),
                        category: MEDIA_CATEGORY.into(),
                        session_id: session_id.clone(),
                        version: sinomo_video::stream::VIDEO_STREAM_VERSION,
                    }),
                    session_id,
                })
            }
            DesktopControlRequest::Input { session_id, events } => {
                let input = {
                    let sessions = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
                    let session = owned_session(&sessions, &session_id, &owner)?;
                    validate_inputs(&session.target, &events)?;
                    session.input.clone().ok_or_else(|| {
                        DesktopError::PermissionDenied("session was not opened for input".into())
                    })?
                };
                let events = input
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .send(&events)?;
                Ok(DesktopControlResponse::InputApplied {
                    events,
                    delivery: if kind == DesktopModule::AppUse {
                        "queued window messages; application handling is best effort"
                    } else {
                        "OS input injection accepted"
                    }
                    .into(),
                })
            }
            DesktopControlRequest::Close { session_id } => {
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
                    DesktopControlRequest::Capabilities => "capabilities",
                    DesktopControlRequest::Targets => "targets",
                    DesktopControlRequest::Children { .. } => "children",
                    DesktopControlRequest::Open { .. } => "open",
                    DesktopControlRequest::Input { .. } => "input",
                    DesktopControlRequest::Close { .. } => "close",
                };
                let subject = match &command {
                    DesktopControlRequest::Open { target, .. }
                    | DesktopControlRequest::Children { target } => Some(target.resource_id()),
                    DesktopControlRequest::Input { session_id, .. }
                    | DesktopControlRequest::Close { session_id } => Some(session_id.clone()),
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
                let (target, cancelled, active, duration) = {
                    let sessions = self
                        .state
                        .sessions
                        .lock()
                        .unwrap_or_else(|e| e.into_inner());
                    let session = owned_session(&sessions, &capture_request.session_id, &owner)
                        .map_err(service_error)?;
                    if !session.capture {
                        return Err(service_error("session was not opened for capture"));
                    }
                    if session.capture_active.swap(true, Ordering::AcqRel) {
                        return Err(service_error("session already has a video consumer"));
                    }
                    (
                        session.target.clone(),
                        session.cancelled.clone(),
                        session.capture_active.clone(),
                        session.expires.saturating_duration_since(Instant::now()),
                    )
                };
                let active_guard = CaptureActive(active);
                let config = DesktopCaptureConfig {
                    target,
                    frames_per_second: capture_request.frames_per_second,
                    include_cursor: capture_request.include_cursor,
                };
                if self.state.closed.load(Ordering::Acquire) {
                    return Err(service_error("module is unloading"));
                }
                let capture =
                    tokio::task::spawn_blocking(move || DesktopCapture::start(config, permissions))
                        .await
                        .map_err(service_error)?
                        .map_err(service_error)?;
                let (body, worker) = sinomo_video::stream::desktop_capture_body(
                    capture,
                    capture_request.encoding,
                    duration,
                    cancelled,
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
                self.publish_operation(&request, &owner, "capture", true, serde_json::json!({"session_id":capture_request.session_id, "stream_version":sinomo_video::stream::VIDEO_STREAM_VERSION}))
                    .await?;
                ctx.respond_with_body(ObjectRpcResponse::encode_body(self.kind.capture_route(), &serde_json::json!({ "version": sinomo_video::stream::VIDEO_STREAM_VERSION, "session_id": capture_request.session_id, "content_type": "application/vnd.sinomo.video-stream", "max_duration_ms": duration.as_millis() }))?, body)?;
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
        .with_object_rpc_route(ModuleObjectRpcRoute::new(
            kind.control_route(),
            kind.name(),
            kind.operation(),
            ModuleRouteAuth::LocalOrClusterMember,
        ))
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
    registration
}

pub fn component_factory(kind: DesktopModule) -> Result<StaticComponentFactory, ComponentError> {
    let descriptor = component_descriptor_from_registration(&module_registration(kind))?;
    let caller = descriptor.clone();
    StaticComponentFactory::new(descriptor, move |context, scope| {
        let runtime = context.runtime().ok_or_else(|| {
            ComponentError::component(&caller.component_id, "desktop module requires runtime")
        })?;
        let state = Arc::new(Runtime::default());
        let events = context.event_client(&caller)?;
        scope.set_services(
            ServiceRegistryBuilder::default()
                .register(DesktopService {
                    kind,
                    state: state.clone(),
                    events,
                })
                .build(),
        );
        let mut shutdown = scope.shutdown_receiver();
        let reaper = state.clone();
        scope.own_task("desktop-session-expiry", runtime.spawn(async move {
            loop {
                let deadline = reaper.sessions.lock().unwrap_or_else(|e| e.into_inner()).values().map(|s| s.expires).min();
                let wait = async { if let Some(deadline) = deadline { tokio::time::sleep_until(deadline.into()).await; } else { std::future::pending::<()>().await; } };
                tokio::select! { _ = shutdown.changed() => break, _ = reaper.changed.notified() => {}, _ = wait => {} }
                let state = reaper.clone();
                let cleanup = tokio::task::spawn_blocking(move || {
                    let mut sessions = state.sessions.lock().unwrap_or_else(|e| e.into_inner());
                    sessions.retain(|_, session| session.expires > Instant::now());
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
    fn both_modules_finalize_without_route_collisions_and_have_independent_skills() {
        let catalog = ComponentCatalog::builder()
            .register_module(module_registration(DesktopModule::ComputerUse))
            .unwrap()
            .register_module(module_registration(DesktopModule::AppUse))
            .unwrap()
            .build()
            .unwrap();
        drop(catalog);
        for kind in [DesktopModule::ComputerUse, DesktopModule::AppUse] {
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
        let kind = DesktopModule::ComputerUse;
        assert!(matches!(
            state.control(
                kind,
                permissions.clone(),
                "caller".into(),
                DesktopControlRequest::Targets
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
    fn app_module_cannot_request_desktop_effects() {
        assert!(
            DesktopModule::AppUse
                .check_target(&DesktopTarget::Monitor {
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
        let factories: Vec<Arc<dyn ComponentFactory>> =
            [DesktopModule::ComputerUse, DesktopModule::AppUse]
                .into_iter()
                .map(|kind| Arc::new(component_factory(kind).unwrap()) as Arc<dyn ComponentFactory>)
                .collect();
        assert_eq!(reconciler.reconcile(&factories).unwrap().active.len(), 2);
        let snapshot = reconciler.service_registry().active_snapshot();
        assert_eq!(snapshot.registry().route_count(), 4);
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
                    COMPUTER_CONTROL_ROUTE,
                    "computer-use",
                    &DesktopControlRequest::Targets,
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
        assert_eq!(reconciler.reconcile(&factories).unwrap().active.len(), 2);
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
                DesktopModule::ComputerUse,
                permissions.clone(),
                "owner".into(),
                DesktopControlRequest::Open {
                    target,
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
                    DesktopModule::ComputerUse,
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
                DesktopModule::ComputerUse,
                permissions,
                "owner".into(),
                DesktopControlRequest::Close { session_id },
            )
            .unwrap();
        assert!(state.sessions.lock().unwrap().is_empty());
    }
}
