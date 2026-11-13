//! Actor-bound application sessions and overview composition. OS relationships,
//! lifetime checks and semantic effects remain in the native provider.
use super::provider::{NativeApplication, NativeDesktopCapture};
use super::*;
use sinomo_runtime::device::NativeVideoSample;

pub(super) fn publication(kind: &str) -> ComponentEventPublication {
    ComponentEventPublication::new(kind, "sinomo.app.surface", "1.0.0")
}
pub(super) async fn publish(
    events: &ComponentEventClient,
    kind: &str,
    payload: serde_json::Value,
) -> Result<(), ServiceError> {
    use sinomo_runtime::core::*;
    let mut event = EventEnvelope::new(
        Id::generate(),
        time::OffsetDateTime::now_utc(),
        Id::from_legacy_named("node.local").map_err(service_error)?,
        Source::new("app-use").map_err(service_error)?,
        EventKind::new(kind).map_err(service_error)?,
        Salience::new(0.5).map_err(service_error)?,
        Confidence::new(1.0).map_err(service_error)?,
        RoutingMetadata::new(
            RoutingScope::Local,
            LatencyClass::Interactive,
            PrivacyClass::Derived,
        ),
    )
    .with_payload(payload.clone());
    if let Some(id) = payload["session_id"].as_str() {
        event.links.related_to.push(format!("app-use-session:{id}"));
    }
    events
        .publish(&publication(kind), event)
        .await
        .map_err(service_error)?;
    Ok(())
}

pub(super) struct ApplicationSession {
    pub owner: String,
    pub native: Arc<NativeApplication>,
    pub expires: Instant,
    pub cancelled: Arc<AtomicBool>,
    snapshot: Mutex<DesktopApplicationSnapshot>,
}
impl Drop for ApplicationSession {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Release);
        self.native.close();
    }
}
pub(super) struct OverviewSession {
    pub owner: String,
    pub application: Arc<ApplicationSession>,
    pub max_windows: usize,
    pub active: Arc<AtomicBool>,
    pub cancelled: Arc<AtomicBool>,
    pub selected: Arc<Mutex<Option<String>>>,
}
impl Drop for OverviewSession {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Release);
    }
}
impl Runtime {
    pub(super) fn capture_source(
        &self,
        request: &DesktopStreamRequest,
        owner: &str,
    ) -> DesktopResult<(
        Box<dyn DesktopFrameSource>,
        Arc<AtomicBool>,
        CaptureActive,
        Duration,
    )> {
        if self.closed.load(Ordering::Acquire) {
            return Err(DesktopError::Closed("module unloading".into()));
        }
        {
            let overviews = self.overviews.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(view) = overviews.get(&request.session_id) {
                if view.owner != owner {
                    return Err(DesktopError::PermissionDenied(
                        "overview belongs to another caller".into(),
                    ));
                }
                let duration = view
                    .application
                    .expires
                    .saturating_duration_since(Instant::now());
                if duration.is_zero() || view.cancelled.load(Ordering::Acquire) {
                    return Err(DesktopError::Closed("overview ended".into()));
                }
                if view.active.swap(true, Ordering::AcqRel) {
                    return Err(DesktopError::Invalid(
                        "overview already has a consumer".into(),
                    ));
                }
                let guard = CaptureActive(view.active.clone());
                if request.capture_path == CapturePath::Surface {
                    return Err(DesktopError::Unsupported(
                        "overview composition requires pixel capture".into(),
                    ));
                }
                return Ok((
                    Box::new(ApplicationOverviewSource::new(
                        view.application.clone(),
                        view.max_windows,
                        request.frames_per_second,
                        view.selected.clone(),
                        request.resolution.or_else(|| {
                            request
                                .encoding
                                .max_dimension
                                .map(|edge| CaptureResolution {
                                    width: edge,
                                    height: edge,
                                })
                        }),
                    )),
                    view.cancelled.clone(),
                    guard,
                    duration,
                ));
            }
        }
        let (target, binding, cancelled, active, duration) = {
            let sessions = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
            let session = owned_session(&sessions, &request.session_id, owner)?;
            if !session.capture {
                return Err(DesktopError::PermissionDenied(
                    "session was not opened for capture".into(),
                ));
            }
            if session.capture_active.swap(true, Ordering::AcqRel) {
                return Err(DesktopError::Invalid(
                    "session already has a video consumer".into(),
                ));
            }
            (
                session.target.clone(),
                session.application.clone(),
                session.cancelled.clone(),
                session.capture_active.clone(),
                session.expires.saturating_duration_since(Instant::now()),
            )
        };
        let guard = CaptureActive(active);
        let provider = self.provider()?;
        let capabilities = provider.capabilities()?;
        let surface_available = !matches!(&target, DesktopTarget::Monitor { device_name } if device_name == accessibility::ANDROID_ACCESSIBILITY_TARGET)
            && request.transport == sinomo_video::codec::VideoTransport::H264
            && capabilities
                .capture_formats
                .contains(&sinomo_runtime::device::NativeVideoSampleFormat::H264);
        if request.capture_path == CapturePath::Surface && !surface_available {
            return Err(DesktopError::Unsupported(
                "native encoded capture is unavailable for this transport/backend".into(),
            ));
        }
        let mut resolution = request.resolution.or_else(|| {
            request
                .encoding
                .max_dimension
                .map(|edge| CaptureResolution {
                    width: edge,
                    height: edge,
                })
        });
        if request.transport == sinomo_video::codec::VideoTransport::H264 {
            let bounds = resolution.unwrap_or(CaptureResolution {
                width: 1920,
                height: 1920,
            });
            resolution = Some(CaptureResolution {
                width: bounds.width.min(1920),
                height: bounds.height.min(1920),
            });
        }
        let config = DesktopCaptureConfig {
            resolution,
            pixel_format: (surface_available && request.capture_path != CapturePath::Pixels)
                .then_some(sinomo_runtime::device::NativeVideoSampleFormat::H264),
            target,
            frames_per_second: if capabilities.frame_rate_ceiling {
                sinomo_video::stream::effective_capture_fps(
                    request.frames_per_second,
                    request.encoding.max_frame_rate_hz,
                )
            } else {
                request.frames_per_second
            },
            include_cursor: request.include_cursor,
            lease_ms: duration.as_millis().min(MAX_LEASE_MS as u128) as u64,
        };
        let capture = if let Some(binding) = binding {
            self.application_session(&binding.application_id, owner)?
                .native
                .capture(&binding.surface_id, config)?
        } else {
            self.provider()?.capture(config)?
        };
        Ok((Box::new(capture), cancelled, guard, duration))
    }
    pub(super) fn application_session(
        &self,
        id: &str,
        owner: &str,
    ) -> DesktopResult<Arc<ApplicationSession>> {
        let applications = self.applications.lock().unwrap_or_else(|e| e.into_inner());
        let app = applications
            .get(id)
            .ok_or_else(|| DesktopError::Closed("unknown application session".into()))?;
        if app.owner != owner {
            return Err(DesktopError::PermissionDenied(
                "application session belongs to another caller".into(),
            ));
        }
        if app.cancelled.load(Ordering::Acquire) || Instant::now() >= app.expires {
            return Err(DesktopError::Closed("application session expired".into()));
        }
        Ok(app.clone())
    }
    pub(super) fn close_application_or_overview(
        &self,
        id: &str,
        owner: &str,
    ) -> DesktopResult<bool> {
        {
            let mut overviews = self.overviews.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(view) = overviews.get(id) {
                if view.owner != owner {
                    return Err(DesktopError::PermissionDenied(
                        "overview belongs to another caller".into(),
                    ));
                }
                overviews.remove(id);
                return Ok(true);
            }
        }
        let app = {
            let mut applications = self.applications.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(app) = applications.get(id) {
                if app.owner != owner {
                    return Err(DesktopError::PermissionDenied(
                        "application belongs to another caller".into(),
                    ));
                }
            } else {
                return Ok(false);
            }
            applications.remove(id).unwrap()
        };
        app.cancelled.store(true, Ordering::Release);
        self.sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|_, session| {
                session
                    .application
                    .as_ref()
                    .is_none_or(|binding| binding.application_id != id)
            });
        self.overviews
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|_, view| view.application.native.id != id);
        app.native.close();
        self.application_changed.notify_one();
        Ok(true)
    }
    pub(super) fn application_control(
        &self,
        kind: DesktopModule,
        owner: String,
        request: DesktopControlRequest,
    ) -> DesktopResult<DesktopControlResponse> {
        use DesktopControlRequest as C;
        use DesktopControlResponse as R;
        match request {
            C::Applications => Ok(R::Applications {
                applications: self.provider()?.applications()?,
            }),
            C::OpenApplication {
                application,
                lease_ms,
            } => {
                if !(1000..=MAX_LEASE_MS).contains(&lease_ms) {
                    return Err(DesktopError::Invalid(
                        "application lease must be 1000..300000 ms".into(),
                    ));
                }
                let mut applications = self.applications.lock().unwrap_or_else(|e| e.into_inner());
                if applications.len() >= MAX_SESSIONS {
                    return Err(DesktopError::Invalid(
                        "application session capacity reached".into(),
                    ));
                }
                let resource_id = application.resource_id();
                let (native, snapshot) =
                    self.provider()?.open_application(application, lease_ms)?;
                let session_id = native.id.clone();
                applications.insert(
                    session_id.clone(),
                    Arc::new(ApplicationSession {
                        owner,
                        native,
                        expires: Instant::now() + Duration::from_millis(lease_ms),
                        cancelled: Arc::new(AtomicBool::new(false)),
                        snapshot: Mutex::new(snapshot.clone()),
                    }),
                );
                self.application_changed.notify_one();
                Ok(R::ApplicationOpened {
                    session_id,
                    resource_id,
                    lease_ms,
                    snapshot,
                })
            }
            C::Surfaces { session_id } => Ok(R::Surfaces {
                snapshot: self
                    .application_session(&session_id, &owner)?
                    .native
                    .snapshot()?,
            }),
            C::OpenWindow {
                application_session_id,
                surface_id,
                input,
                capture,
                lease_ms,
            } => {
                if !(1000..=MAX_LEASE_MS).contains(&lease_ms) || (!input && !capture) {
                    return Err(DesktopError::Invalid(
                        "window open requires input/capture and bounded lease".into(),
                    ));
                }
                let app = self.application_session(&application_session_id, &owner)?;
                let window = app
                    .native
                    .resolve(&surface_id, if capture { "capture" } else { "input" })?;
                let duration = Duration::from_millis(lease_ms)
                    .min(app.expires.saturating_duration_since(Instant::now()));
                let input = if input {
                    Some(Arc::new(Mutex::new(
                        app.native
                            .open_input(&surface_id, duration.as_millis() as u64)?,
                    )))
                } else {
                    None
                };
                let mut sessions = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
                if sessions.len() >= MAX_SESSIONS || self.closed.load(Ordering::Acquire) {
                    return Err(DesktopError::Closed(
                        "window session capacity reached or service stopped".into(),
                    ));
                }
                let session_id = sinomo_runtime::core::Id::generate().to_string();
                let resource_id = window.resource_id;
                sessions.insert(
                    session_id.clone(),
                    Session {
                        owner,
                        mode: DesktopControlMode::Background,
                        target: window.window.target,
                        input,
                        capture,
                        capture_active: Arc::new(AtomicBool::new(false)),
                        cancelled: Arc::new(AtomicBool::new(false)),
                        expires: Instant::now() + duration,
                        application: Some(ApplicationWindowBinding {
                            application_id: application_session_id,
                            surface_id: surface_id.clone(),
                        }),
                    },
                );
                self.changed.notify_one();
                Ok(R::Opened {
                    session_id: session_id.clone(),
                    resource_id,
                    lease_ms: duration.as_millis() as u64,
                    surface_id: Some(surface_id),
                    stream: capture.then(|| DesktopStreamDescriptor {
                        route: kind.capture_route().into(),
                        category: MEDIA_CATEGORY.into(),
                        session_id,
                        version: sinomo_video::stream::GEOMETRY_STREAM_VERSION,
                    }),
                })
            }
            C::OpenOverview {
                application_session_id,
                max_windows,
            } => {
                if !(1..=4).contains(&max_windows) {
                    return Err(DesktopError::Invalid(
                        "overview max_windows must be 1..4".into(),
                    ));
                }
                let app = self.application_session(&application_session_id, &owner)?;
                let permissions = LocalNodePermissionStore::new(&self.root);
                if authorize_resource(
                    &permissions,
                    &app.native.application.resource_id(),
                    "capture",
                )
                .is_err()
                    && !app
                        .native
                        .snapshot()?
                        .windows
                        .iter()
                        .any(|window| app.native.resolve(&window.surface_id, "capture").is_ok())
                {
                    return Err(DesktopError::PermissionDenied(
                        "overview requires app capture or an observed window's capture grant"
                            .into(),
                    ));
                }
                let mut overviews = self.overviews.lock().unwrap_or_else(|e| e.into_inner());
                if overviews.len() >= MAX_SESSIONS {
                    return Err(DesktopError::Invalid("overview capacity reached".into()));
                }
                let session_id = sinomo_runtime::core::Id::generate().to_string();
                let resource_id = app.native.application.resource_id();
                let lease_ms = app
                    .expires
                    .saturating_duration_since(Instant::now())
                    .as_millis() as u64;
                overviews.insert(
                    session_id.clone(),
                    OverviewSession {
                        owner,
                        application: app,
                        max_windows,
                        active: Arc::new(AtomicBool::new(false)),
                        cancelled: Arc::new(AtomicBool::new(false)),
                        selected: Arc::new(Mutex::new(None)),
                    },
                );
                Ok(R::Opened {
                    session_id: session_id.clone(),
                    resource_id,
                    lease_ms,
                    surface_id: None,
                    stream: Some(DesktopStreamDescriptor {
                        route: kind.capture_route().into(),
                        category: MEDIA_CATEGORY.into(),
                        session_id,
                        version: sinomo_video::stream::GEOMETRY_STREAM_VERSION,
                    }),
                })
            }
            C::TrayItems => Ok(R::SemanticItems {
                snapshot: self.provider()?.tray_items()?,
            }),
            C::SelectWindow {
                overview_session_id,
                surface_id,
            } => {
                let overviews = self.overviews.lock().unwrap_or_else(|e| e.into_inner());
                let view = overviews
                    .get(&overview_session_id)
                    .ok_or_else(|| DesktopError::Closed("unknown application view".into()))?;
                if view.owner != owner {
                    return Err(DesktopError::PermissionDenied(
                        "application view belongs to another caller".into(),
                    ));
                }
                if let Some(id) = &surface_id {
                    view.application.native.resolve(id, "capture")?;
                }
                *view.selected.lock().unwrap_or_else(|e| e.into_inner()) = surface_id.clone();
                Ok(R::ViewSelected { surface_id })
            }
            C::InvokeTray {
                item_id,
                operation,
                allow_desktop_interaction,
            } => {
                self.provider()?
                    .invoke_tray(item_id, operation, allow_desktop_interaction)?;
                Ok(R::SemanticInvoked {
                    delivery:
                        "UIA accepted the action; observe its result; app/shell UI may activate"
                            .into(),
                })
            }
            C::MenuItems {
                application_session_id,
                surface_id,
            } => Ok(R::SemanticItems {
                snapshot: self
                    .application_session(&application_session_id, &owner)?
                    .native
                    .menu_items(&surface_id)?,
            }),
            C::InvokeMenuItem {
                application_session_id,
                surface_id,
                item_id,
                allow_desktop_interaction,
            } => {
                self.application_session(&application_session_id, &owner)?
                    .native
                    .invoke_menu_item(&surface_id, item_id, allow_desktop_interaction)?;
                Ok(R::SemanticInvoked {
                    delivery:
                        "UIA accepted the menu action; observe the resulting application state"
                            .into(),
                })
            }
            _ => Err(DesktopError::Invalid(
                "unsupported application action".into(),
            )),
        }
    }
    pub(super) fn application_updates(&self) -> Vec<(String, serde_json::Value)> {
        let applications: Vec<_> = self
            .applications
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .map(|(id, app)| (id.clone(), app.clone()))
            .collect();
        let mut events = Vec::new();
        for (id, app) in applications {
            let snapshot = if app.expires <= Instant::now() || app.cancelled.load(Ordering::Acquire)
            {
                Err(DesktopError::Closed("application lease ended".into()))
            } else {
                app.native.snapshot()
            };
            match snapshot {
                Err(error) => {
                    let _ = self.close_application_or_overview(&id, &app.owner);
                    events.push(("app.session.closed".into(), serde_json::json!({"session_id":id,"actor":app.owner,"reason":error.to_string()})));
                }
                Ok(snapshot) => {
                    let mut previous = app.snapshot.lock().unwrap_or_else(|e| e.into_inner());
                    if *previous == snapshot {
                        continue;
                    }
                    for window in &snapshot.windows {
                        let old = previous
                            .windows
                            .iter()
                            .find(|w| w.surface_id == window.surface_id);
                        if old != Some(window) {
                            events.push((if old.is_none() { "app.window.added" } else { "app.window.changed" }.into(), serde_json::json!({"session_id":id,"actor":app.owner,"revision":snapshot.revision,"window":window})));
                        }
                    }
                    for window in &previous.windows {
                        if !snapshot
                            .windows
                            .iter()
                            .any(|w| w.surface_id == window.surface_id)
                        {
                            events.push(("app.window.removed".into(), serde_json::json!({"session_id":id,"actor":app.owner,"revision":snapshot.revision,"surface_id":window.surface_id})));
                        }
                    }
                    *previous = snapshot;
                }
            }
        }
        events
    }
}

pub(super) struct ApplicationOverviewSource {
    app: Arc<ApplicationSession>,
    max_windows: usize,
    fps: u32,
    resolution: Option<CaptureResolution>,
    started: Instant,
    state: Mutex<OverviewState>,
    selected: Arc<Mutex<Option<String>>>,
}
#[derive(Default)]
struct OverviewState {
    captures: BTreeMap<String, NativeDesktopCapture>,
    samples: BTreeMap<String, NativeVideoSample>,
    sequence: u64,
    tiles: Vec<DesktopOverviewTile>,
    omitted: Vec<String>,
    identity: Option<DesktopCaptureIdentity>,
    last_selected: Option<String>,
}
impl ApplicationOverviewSource {
    pub fn new(
        app: Arc<ApplicationSession>,
        max_windows: usize,
        fps: u32,
        selected: Arc<Mutex<Option<String>>>,
        resolution: Option<CaptureResolution>,
    ) -> Self {
        Self {
            app,
            max_windows,
            fps,
            resolution,
            started: Instant::now(),
            state: Mutex::new(OverviewState::default()),
            selected,
        }
    }
}
impl DesktopFrameSource for ApplicationOverviewSource {
    fn identity(&self) -> Option<DesktopCaptureIdentity> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .identity
            .clone()
    }
    fn tiles(&self) -> Vec<DesktopOverviewTile> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .tiles
            .clone()
    }
    fn next(&self, _: u64, timeout: Duration) -> DesktopResult<Option<NativeVideoSample>> {
        if self.app.cancelled.load(Ordering::Acquire) || Instant::now() >= self.app.expires {
            return Err(DesktopError::Closed("application overview ended".into()));
        }
        let snapshot = self.app.native.snapshot()?;
        let selected = self
            .selected
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let visible: Vec<_> = snapshot
            .windows
            .iter()
            .filter(|window| {
                if let Some(id) = &selected {
                    &window.surface_id == id
                } else {
                    window.top_level && window.visible && !window.window.minimized
                }
            })
            .collect();
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let selection_changed = state.last_selected != selected;
        state.last_selected = selected.clone();
        let previous: Vec<_> = state.captures.keys().cloned().collect();
        state.captures.retain(|id, _| {
            visible
                .iter()
                .take(self.max_windows)
                .any(|w| &w.surface_id == id)
        });
        state.samples.retain(|id, _| {
            visible
                .iter()
                .take(self.max_windows)
                .any(|w| &w.surface_id == id)
        });
        let mut changed = selection_changed || previous.len() != state.captures.len();
        let mut omitted = Vec::new();
        for (index, window) in visible.iter().enumerate() {
            if index >= self.max_windows {
                omitted.push(window.surface_id.clone());
                continue;
            }
            if !state.captures.contains_key(&window.surface_id) {
                let config = DesktopCaptureConfig {
                    resolution: self.resolution,
                    pixel_format: None,
                    target: window.window.target.clone(),
                    frames_per_second: self.fps,
                    include_cursor: false,
                    lease_ms: self
                        .app
                        .expires
                        .saturating_duration_since(Instant::now())
                        .as_millis() as u64,
                };
                match self.app.native.capture(&window.surface_id, config) {
                    Ok(capture) => {
                        state.captures.insert(window.surface_id.clone(), capture);
                    }
                    Err(_) => {
                        omitted.push(window.surface_id.clone());
                        continue;
                    }
                }
            }
            let after = state
                .samples
                .get(&window.surface_id)
                .map_or(0, |s| s.sequence);
            match state.captures[&window.surface_id].next(after, Duration::ZERO) {
                Ok(Some(sample)) => {
                    state.samples.insert(window.surface_id.clone(), sample);
                    changed = true;
                }
                Ok(None) => {}
                Err(_) => {
                    state.captures.remove(&window.surface_id);
                    state.samples.remove(&window.surface_id);
                    omitted.push(window.surface_id.clone());
                    changed = true;
                }
            }
        }
        state.omitted = omitted;
        if *self.selected.lock().unwrap_or_else(|e| e.into_inner()) != selected {
            // Selection changed during native capture. Never publish the old
            // view as the response to a newly selected window.
            return Ok(None);
        }
        let timestamp_ns = self.started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        if state.samples.is_empty() {
            let identity = DesktopCaptureIdentity::Unavailable {
                application: self.app.native.application.clone(),
                surface_id: selected,
                reason: if visible.is_empty() {
                    "no selected/visible window exists"
                } else {
                    "no authorized window pixels are currently available"
                }
                .into(),
            };
            // Explicit derived status, never a stale window presented as fresh.
            if state.identity.as_ref() == Some(&identity) {
                drop(state);
                std::thread::sleep(timeout.min(Duration::from_millis(50)));
                return Ok(None);
            }
            state.identity = Some(identity);
            state.tiles.clear();
            state.sequence += 1;
            return Ok(Some(NativeVideoSample {
                native_dimensions: None,
                sequence: state.sequence,
                format: sinomo_runtime::device::NativeVideoSampleFormat::Bgra8,
                width: 32,
                height: 32,
                timestamp_ns,
                duration_ns: 0,
                captured_at_unix_ms: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis(),
                data: [28u8, 20, 12, 255].repeat(32 * 32).into(),
                planes: Arc::from([sinomo_runtime::device::NativeVideoPlane {
                    offset: 0,
                    len: 32 * 32 * 4,
                    width: 32,
                    height: 32,
                    stride: 32 * 4,
                }]),
            }));
        }
        if !changed {
            drop(state);
            std::thread::sleep(timeout.min(Duration::from_millis(50)));
            return Ok(None);
        }
        state.sequence += 1;
        if let Some(id) = selected {
            let mut sample = state.samples[&id].clone();
            sample.sequence = state.sequence;
            sample.timestamp_ns = timestamp_ns;
            state.identity = Some(DesktopCaptureIdentity::Target {
                target: visible[0].window.target.clone(),
                application: Some(ApplicationWindowBinding {
                    application_id: self.app.native.id.clone(),
                    surface_id: id,
                }),
            });
            state.tiles.clear();
            return Ok(Some(sample));
        }
        let samples: Vec<_> = state
            .samples
            .iter()
            .map(|(id, sample)| (id.clone(), sample.clone()))
            .collect();
        let (sample, tiles) =
            sinomo_video::stream::compose_desktop_overview(&samples, state.sequence, timestamp_ns)
                .map_err(DesktopError::Platform)?;
        state.identity = Some(DesktopCaptureIdentity::Overview {
            application: self.app.native.application.clone(),
            omitted_surfaces: state.omitted.clone(),
        });
        state.tiles = tiles;
        Ok(Some(sample))
    }
}
