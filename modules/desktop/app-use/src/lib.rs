//! Independently loaded OS provider. It owns only native capture/input workers;
//! policy, tool projection, media encoding and transport stay in their owners.
mod device;
use sinomo_core::node::LocalNodePermissionStore;
use sinomo_device::desktop::{protocol::*, *};
use sinomo_module_api::*;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock, atomic::AtomicBool};
use std::time::{Duration, Instant};

#[derive(Clone, Copy)]
struct Host(SinomoModuleHostApiV2);
unsafe impl Send for Host {}
unsafe impl Sync for Host {}
static HOST: Mutex<Option<Host>> = Mutex::new(None);
static LIFECYCLE: NativeModuleLifecycle = NativeModuleLifecycle::new();
static STATE: OnceLock<Mutex<State>> = OnceLock::new();
#[derive(Default)]
struct State {
    inputs: BTreeMap<String, InputEntry>,
    captures: BTreeMap<String, CaptureEntry>,
    applications: BTreeMap<String, ApplicationEntry>,
}
struct InputEntry {
    owner: String,
    target: DesktopTarget,
    expires: Instant,
    application: Option<String>,
    input: device::DesktopInputSession,
}
struct CaptureEntry {
    owner: String,
    expires: Instant,
    application: Option<String>,
    capture: Arc<device::DesktopCapture>,
}
struct ApplicationEntry {
    owner: String,
    tracker: Arc<device::ApplicationTracker>,
}
impl Drop for ApplicationEntry {
    fn drop(&mut self) {
        self.tracker.close();
    }
}
fn application(
    state: &State,
    id: &str,
    owner: &str,
) -> DesktopResult<Arc<device::ApplicationTracker>> {
    let entry = state
        .applications
        .get(id)
        .ok_or_else(|| DesktopError::Closed("unknown application session".into()))?;
    same_owner(&entry.owner, owner)?;
    Ok(entry.tracker.clone())
}
fn state() -> &'static Mutex<State> {
    STATE.get_or_init(|| Mutex::new(State::default()))
}

pub fn descriptor() -> NativeModuleDescriptor {
    NativeModuleDescriptor {
        module_id: APP_USE_OS_MODULE_ID.into(),
        module_version: env!("CARGO_PKG_VERSION").into(),
        owner: "sinomo-app-use-module".into(),
        native_abi_version: SINOMO_NATIVE_MODULE_ABI_VERSION,
        backend: NativeModuleBackend::NativeDylib,
        interface_version: "sinomo.app-use.os.v1".into(),
        platform_triple: format!(
            "{}-{}-{}",
            std::env::consts::ARCH,
            std::env::consts::OS,
            std::env::consts::FAMILY
        ),
        supported_operations: vec![APP_USE_OS_OPERATION.into()],
        capabilities: vec![APP_USE_OS_OPERATION.into()],
        surfaces: NativeModuleSurfaces::default(),
        component: ComponentDescriptor::new(APP_USE_OS_MODULE_ID, env!("CARGO_PKG_VERSION"))
            .with_display_name("App Use OS Provider")
            .with_offer(
                ServiceOffer::new(APP_USE_OS_SERVICE, APP_USE_OS_INTERFACE, APP_USE_OS_VERSION)
                    .with_operation(ServiceOperationOffer::new(APP_USE_OS_OPERATION)),
            ),
    }
}

fn bind_store(path: &str) -> DesktopResult<LocalNodePermissionStore> {
    let host = HOST
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .as_ref()
        .copied()
        .ok_or_else(|| DesktopError::Closed("OS provider is not initialized".into()))?;
    let capability = permission_store_capability(path);
    let granted = host.0.local_permission_granted.is_some_and(|check| unsafe {
        check(
            host.0.user_data,
            SinomoModuleByteSlice::from_slice(capability.as_bytes()),
        ) != 0
    });
    if !granted {
        return Err(DesktopError::PermissionDenied(
            "host did not bind the node-local permission store".into(),
        ));
    }
    Ok(LocalNodePermissionStore::from_path(path))
}
fn same_owner(actual: &str, expected: &str) -> DesktopResult<()> {
    if actual != expected {
        return Err(DesktopError::PermissionDenied(
            "OS handle belongs to another host authority".into(),
        ));
    }
    Ok(())
}
fn lease(ms: u64) -> DesktopResult<Duration> {
    if !(1000..=300_000).contains(&ms) {
        return Err(DesktopError::Invalid(
            "OS lease must be 1000..300000 ms".into(),
        ));
    }
    Ok(Duration::from_millis(ms))
}
fn dispatch(request: DesktopProviderRequest) -> DesktopResult<DesktopProviderResponse> {
    let permissions = bind_store(&request.permission_store)?;
    let owner = request.permission_store;
    let mut state = state().lock().unwrap_or_else(|e| e.into_inner());
    let now = Instant::now();
    state.inputs.retain(|_, entry| entry.expires > now);
    state.captures.retain(|_, entry| entry.expires > now);
    state
        .applications
        .retain(|_, entry| !entry.tracker.remaining().is_zero());
    use DesktopProviderCommand as C;
    use DesktopProviderResponse as R;
    match request.command {
        C::SurfaceCapabilities => Ok(R::SurfaceCapabilities {
            capabilities: device::surface_capabilities(),
        }),
        C::Applications => Ok(R::Applications {
            applications: device::applications(&permissions)?,
        }),
        C::OpenApplication {
            application: app,
            lease_ms,
        } => {
            let duration = lease(lease_ms)?;
            if state.applications.len() >= 8 {
                return Err(DesktopError::Invalid(
                    "application session capacity reached".into(),
                ));
            }
            let tracker = device::ApplicationTracker::new(app, permissions, duration)?;
            let snapshot = tracker.snapshot()?;
            let application_id = sinomo_core::Id::generate().to_string();
            state
                .applications
                .insert(application_id.clone(), ApplicationEntry { owner, tracker });
            Ok(R::ApplicationOpened {
                application_id,
                snapshot,
            })
        }
        C::ApplicationSurfaces { application_id } => {
            let tracker = application(&state, &application_id, &owner)?;
            drop(state);
            Ok(R::ApplicationSurfaces {
                snapshot: tracker.snapshot()?,
            })
        }
        C::CloseApplication { application_id } => {
            if let Some(entry) = state.applications.get(&application_id) {
                same_owner(&entry.owner, &owner)?;
            }
            state.applications.remove(&application_id);
            state
                .inputs
                .retain(|_, entry| entry.application.as_ref() != Some(&application_id));
            state
                .captures
                .retain(|_, entry| entry.application.as_ref() != Some(&application_id));
            Ok(R::Closed)
        }
        C::ResolveApplicationWindow { binding, operation } => {
            if !["observe", "input", "capture"].contains(&operation.as_str()) {
                return Err(DesktopError::Invalid("unsupported window operation".into()));
            }
            let tracker = application(&state, &binding.application_id, &owner)?;
            Ok(R::ApplicationWindow {
                window: tracker.resolve(&binding.surface_id, &operation)?,
            })
        }
        C::OpenApplicationInput { binding, lease_ms } => {
            let tracker = application(&state, &binding.application_id, &owner)?;
            let target = tracker.resolve(&binding.surface_id, "input")?.window.target;
            let duration = lease(lease_ms)?.min(tracker.remaining());
            if state.inputs.len() >= 8 || state.inputs.values().any(|entry| entry.target == target)
            {
                return Err(DesktopError::Invalid(
                    "window already leased or input capacity reached".into(),
                ));
            }
            let input = device::DesktopInputSession::open_authorized(
                target.clone(),
                tracker.authority(binding.surface_id)?,
                duration,
                Arc::new(AtomicBool::new(false)),
            )?;
            let id = sinomo_core::Id::generate().to_string();
            state.inputs.insert(
                id.clone(),
                InputEntry {
                    owner,
                    target,
                    expires: Instant::now() + duration,
                    application: Some(binding.application_id),
                    input,
                },
            );
            Ok(R::Opened { id })
        }
        C::OpenApplicationCapture {
            binding,
            mut config,
        } => {
            let tracker = application(&state, &binding.application_id, &owner)?;
            let target = tracker
                .resolve(&binding.surface_id, "capture")?
                .window
                .target;
            if config.target != target {
                return Err(DesktopError::Invalid(
                    "capture target does not match surface reference".into(),
                ));
            }
            config.lease_ms = config.lease_ms.min(tracker.remaining().as_millis() as u64);
            if config.lease_ms == 0 || state.captures.len() >= 8 {
                return Err(DesktopError::Closed(
                    "capture expired or capacity reached".into(),
                ));
            }
            let expires = Instant::now() + Duration::from_millis(config.lease_ms);
            let capture = device::DesktopCapture::start_authorized(
                config,
                tracker.authority(binding.surface_id)?,
            )?;
            let id = sinomo_core::Id::generate().to_string();
            state.captures.insert(
                id.clone(),
                CaptureEntry {
                    owner,
                    expires,
                    application: Some(binding.application_id),
                    capture: Arc::new(capture),
                },
            );
            Ok(R::Opened { id })
        }
        C::TrayItems => {
            drop(state);
            Ok(R::SemanticItems {
                snapshot: device::tray_items(&permissions)?,
            })
        }
        C::InvokeTray {
            item_id,
            operation,
            allow_desktop_interaction,
        } => {
            drop(state);
            device::invoke_tray(&permissions, item_id, operation, allow_desktop_interaction)?;
            Ok(R::SemanticInvoked)
        }
        C::MenuItems { binding } => {
            let tracker = application(&state, &binding.application_id, &owner)?;
            drop(state);
            Ok(R::SemanticItems {
                snapshot: device::menu_items(tracker, binding.surface_id)?,
            })
        }
        C::InvokeMenuItem {
            binding,
            item_id,
            allow_desktop_interaction,
        } => {
            let tracker = application(&state, &binding.application_id, &owner)?;
            drop(state);
            device::invoke_menu_item(
                tracker,
                binding.surface_id,
                item_id,
                allow_desktop_interaction,
            )?;
            Ok(R::SemanticInvoked)
        }
        C::Capabilities => Ok(R::Capabilities {
            capabilities: device::capabilities(),
        }),
        C::Targets { mode } => Ok(R::Targets {
            targets: device::enumerate_targets(
                &permissions,
                mode == DesktopControlMode::Background,
            )?,
        }),
        C::Children { target } => Ok(R::Targets {
            targets: device::enumerate_child_windows(&permissions, &target)?,
        }),
        C::OpenInput {
            target,
            mode,
            lease_ms,
        } => {
            mode.validate_target(&target)?;
            let duration = lease(lease_ms)?;
            if state.inputs.len() >= 8
                || state.inputs.values().any(|entry| {
                    entry.target == target || (!entry.target.is_window() && !target.is_window())
                })
            {
                return Err(DesktopError::Invalid(
                    "OS input target already leased or input capacity reached".into(),
                ));
            }
            let input = device::DesktopInputSession::open_with_lease(
                target.clone(),
                permissions,
                duration,
                Arc::new(AtomicBool::new(false)),
            )?;
            let id = sinomo_core::Id::generate().to_string();
            state.inputs.insert(
                id.clone(),
                InputEntry {
                    owner,
                    target,
                    expires: Instant::now() + duration,
                    application: None,
                    input,
                },
            );
            Ok(R::Opened { id })
        }
        C::Input { id, events } => {
            let entry = state
                .inputs
                .get_mut(&id)
                .ok_or_else(|| DesktopError::Closed("unknown OS input handle".into()))?;
            same_owner(&entry.owner, &owner)?;
            Ok(R::Applied {
                events: entry.input.send(&events)?,
            })
        }
        C::CloseInput { id } => {
            if let Some(entry) = state.inputs.get(&id) {
                same_owner(&entry.owner, &owner)?;
            }
            state.inputs.remove(&id);
            Ok(R::Closed)
        }
        C::OpenCapture { config } => {
            // The host sends the remaining capture lease after stream attach;
            // it may legitimately have less than the minimum opening duration.
            if !(1..=300_000).contains(&config.lease_ms) {
                return Err(DesktopError::Closed(
                    "capture lease expired or invalid".into(),
                ));
            }
            let duration = Duration::from_millis(config.lease_ms);
            if state.captures.len() >= 8 {
                return Err(DesktopError::Invalid("OS capture capacity reached".into()));
            }
            let capture = device::DesktopCapture::start(config, permissions)?;
            let id = sinomo_core::Id::generate().to_string();
            state.captures.insert(
                id.clone(),
                CaptureEntry {
                    owner,
                    expires: Instant::now() + duration,
                    application: None,
                    capture: Arc::new(capture),
                },
            );
            Ok(R::Opened { id })
        }
        C::Frame {
            id,
            after_sequence,
            timeout_ms,
        } => {
            let entry = state
                .captures
                .get(&id)
                .ok_or_else(|| DesktopError::Closed("unknown OS capture handle".into()))?;
            same_owner(&entry.owner, &owner)?;
            let capture = entry.capture.clone();
            drop(state);
            let frame = capture
                .next(after_sequence, Duration::from_millis(timeout_ms.min(100)))?
                .map(DesktopRawFrame::from_sample)
                .transpose()?;
            let available = frame.is_some();
            if let Some(frame) = frame {
                // The stable output callback carries a single SOBJ byte blob.
                // Nesting a 4K frame inside InvocationResponse.payload_sobj
                // makes the generic envelope serialize millions of integers.
                let bytes = encode_sobj(&DesktopFrameDelivery {
                    capture_id: id,
                    frame,
                })
                .map_err(|e| DesktopError::Platform(e.to_string()))?;
                let host = HOST
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .as_ref()
                    .copied()
                    .ok_or_else(|| DesktopError::Closed("OS provider stopped".into()))?;
                let output = host.0.output_chunk.ok_or_else(|| {
                    DesktopError::Closed("capture output callback unavailable".into())
                })?;
                let status =
                    unsafe { output(host.0.user_data, SinomoModuleByteSlice::from_slice(&bytes)) };
                if status != SINOMO_MODULE_STATUS_OK {
                    return Err(DesktopError::Closed(
                        "host rejected raw capture frame".into(),
                    ));
                }
            }
            Ok(R::Frame { available })
        }
        C::CloseCapture { id } => {
            if let Some(entry) = state.captures.get(&id) {
                same_owner(&entry.owner, &owner)?;
            }
            state.captures.remove(&id);
            Ok(R::Closed)
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_api_version() -> u32 {
    SINOMO_NATIVE_MODULE_ABI_VERSION
}
#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_descriptor(
    out: *mut SinomoModuleByteBuffer,
) -> SinomoModuleStatusCode {
    write(out, &descriptor())
}
#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_init(host: SinomoModuleHostApiV2) -> SinomoModuleStatusCode {
    if host.abi_version != SINOMO_NATIVE_MODULE_ABI_VERSION {
        return SINOMO_MODULE_STATUS_UNSUPPORTED_ABI;
    }
    if !LIFECYCLE.initialize() {
        return SINOMO_MODULE_STATUS_RUNTIME_ERROR;
    }
    *HOST.lock().unwrap_or_else(|e| e.into_inner()) = Some(Host(host));
    SINOMO_MODULE_STATUS_OK
}
#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_invoke(
    bytes: SinomoModuleByteSlice,
    out: *mut SinomoModuleByteBuffer,
) -> SinomoModuleStatusCode {
    let Some(_guard) = LIFECYCLE.begin_invocation() else {
        return SINOMO_MODULE_STATUS_RUNTIME_ERROR;
    };
    if bytes.len > 256 * 1024 {
        return SINOMO_MODULE_STATUS_INVALID_ARGUMENT;
    }
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let request: NativeModuleRequest =
            decode_sobj(unsafe { bytes.as_slice() }).map_err(|e| e.to_string())?;
        let response = match request {
            NativeModuleRequest::ProbeCapabilities(_) => {
                NativeModuleResponse::Capabilities(NativeModuleCapabilitiesResponse {
                    descriptor: descriptor(),
                })
            }
            NativeModuleRequest::Invoke(request) if request.operation == APP_USE_OS_OPERATION => {
                let request: DesktopProviderRequest =
                    decode_sobj(&request.payload_sobj).map_err(|e| e.to_string())?;
                let response = dispatch(request)
                    .unwrap_or_else(|error| DesktopProviderResponse::Failed { error });
                NativeModuleResponse::Invocation(NativeModuleInvocationResponse {
                    operation: APP_USE_OS_OPERATION.into(),
                    payload_sobj: encode_sobj(&response).map_err(|e| e.to_string())?,
                    response_body: None,
                })
            }
            _ => return Err("unsupported OS provider operation".to_string()),
        };
        Ok::<_, String>(response)
    }));
    match result {
        Ok(Ok(response)) => write(out, &response),
        other => write(
            out,
            &NativeModuleResponse::Error(NativeModuleErrorResponse {
                code: "app-use-os-error".into(),
                detail: match other {
                    Ok(Err(error)) => error,
                    _ => "OS provider panicked".into(),
                },
            }),
        ),
    }
}
#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_shutdown() -> SinomoModuleStatusCode {
    if !LIFECYCLE.begin_shutdown() {
        return SINOMO_MODULE_STATUS_RUNTIME_ERROR;
    }
    // UIA calls can involve an unresponsive provider. A worker that does not
    // stop within its bound keeps the library quarantined rather than unloaded.
    if !device::shutdown_surfaces() {
        return SINOMO_MODULE_STATUS_RUNTIME_ERROR;
    }
    // Drop all OS handles and join workers before releasing callback code.
    *state().lock().unwrap_or_else(|e| e.into_inner()) = State::default();
    device::shutdown();
    *HOST.lock().unwrap_or_else(|e| e.into_inner()) = None;
    SINOMO_MODULE_STATUS_OK
}
#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_free_buffer(buffer: SinomoModuleByteBuffer) {
    unsafe {
        drop(SinomoModuleByteBuffer::into_vec(buffer));
    }
}
fn write<T: serde::Serialize>(
    out: *mut SinomoModuleByteBuffer,
    value: &T,
) -> SinomoModuleStatusCode {
    if out.is_null() {
        return SINOMO_MODULE_STATUS_INVALID_ARGUMENT;
    }
    let Ok(bytes) = encode_sobj(value) else {
        return SINOMO_MODULE_STATUS_RUNTIME_ERROR;
    };
    unsafe {
        *out = SinomoModuleByteBuffer::from_vec(bytes);
    }
    SINOMO_MODULE_STATUS_OK
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn raw_video_roundtrips_the_native_sobj_boundary() {
        let frame = DesktopRawFrame {
            sequence: 7,
            width: 2048,
            height: 1024,
            stride: 8192,
            timestamp_ns: 42,
            duration_ns: 30,
            captured_at_unix_ms: 123,
            pixels: vec![15; 8 * 1024 * 1024],
        };
        let encoded = encode_sobj(&DesktopFrameDelivery {
            capture_id: "capture".into(),
            frame,
        })
        .unwrap();
        assert!(
            encoded.len() < 8 * 1024 * 1024 + 1024,
            "raw pixels must remain a byte blob"
        );
        let decoded: DesktopFrameDelivery = decode_sobj(&encoded).unwrap();
        let sample = decoded.frame.into_sample().unwrap();
        assert_eq!(sample.sequence, 7);
        assert!(sample.data.iter().all(|byte| *byte == 15));
        assert_eq!(sample.captured_at_unix_ms, 123);
        assert_eq!(sample.planes[0].stride, 8192);
        let response = NativeModuleResponse::Invocation(NativeModuleInvocationResponse {
            operation: APP_USE_OS_OPERATION.into(),
            payload_sobj: encode_sobj(&DesktopProviderResponse::Frame { available: true }).unwrap(),
            response_body: None,
        });
        assert!(
            encode_sobj(&response).unwrap().len() < 1024,
            "pixels must not enter the nested generic response envelope"
        );
    }
}
