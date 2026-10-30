//! Provider-neutral native binding. This host adapter contains no OS APIs and
//! never falls back to linked desktop code when the native provider is absent.
use sinomo_runtime::device::{
    NativeVideoSample,
    desktop::{protocol::*, surfaces::*, *},
};
use sinomo_runtime::module::*;
use sinomo_runtime::node::LocalNodePermissionStore;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

pub struct DesktopProviderClient {
    component: ActiveNativeComponent,
    root: PathBuf,
    state: NativeDylibModuleLoaderState,
    permissions: LocalNodePermissionStore,
    // The generic ABI's callback slot belongs to one synchronous invocation.
    // Serialize requests from capture and input so no call clears another's
    // host authority callback while it is still executing.
    invocation: Mutex<()>,
}
fn error(e: impl std::fmt::Display) -> DesktopError {
    DesktopError::Platform(e.to_string())
}
impl DesktopProviderClient {
    pub fn resolve(root: &Path) -> DesktopResult<Arc<Self>> {
        let id = ModuleId::from_legacy_named(APP_USE_OS_MODULE_ID).map_err(error)?;
        let state = read_native_dylib_module_loader_state(root, &id)
            .map_err(error)?
            .ok_or_else(|| {
                DesktopError::Unsupported(
                    "App Use OS provider is not installed; install modules/desktop/app-use".into(),
                )
            })?;
        let requirement = ServiceRequirement::new(APP_USE_OS_INTERFACE, APP_USE_OS_VERSION)
            .from_service(APP_USE_OS_SERVICE)
            .with_operation(APP_USE_OS_OPERATION);
        // Installed artifacts have already passed the native loader's trust and
        // compatibility checks. The common binding verifies content and offers.
        let component =
            unsafe { ActiveNativeComponent::resolve(root, &requirement) }.map_err(error)?;
        let client = Arc::new(Self {
            component,
            root: root.into(),
            state,
            permissions: LocalNodePermissionStore::new(root),
            invocation: Mutex::new(()),
        });
        client.check_current()?;
        Ok(client)
    }
    pub fn check_current(&self) -> DesktopResult<()> {
        let state = read_native_dylib_module_loader_state(&self.root, &self.state.module_id)
            .map_err(error)?;
        if state.as_ref() != Some(&self.state) {
            return Err(DesktopError::Closed("App Use OS provider changed or was unloaded; close existing sessions before reopening".into()));
        }
        Ok(())
    }
    pub fn invoke(
        &self,
        command: DesktopProviderCommand,
    ) -> DesktopResult<DesktopProviderResponse> {
        self.invoke_collect(command).map(|(response, _)| response)
    }
    fn invoke_collect(
        &self,
        command: DesktopProviderCommand,
    ) -> DesktopResult<(DesktopProviderResponse, Option<NativeVideoSample>)> {
        let _invocation = self.invocation.lock().unwrap_or_else(|e| e.into_inner());
        // Cleanup remains possible after provider removal or consent revocation.
        if !matches!(
            command,
            DesktopProviderCommand::CloseInput { .. }
                | DesktopProviderCommand::CloseCapture { .. }
                | DesktopProviderCommand::CloseApplication { .. }
        ) {
            self.check_current()?;
        }
        let path = self.permissions.path().to_string_lossy().into_owned();
        let capture = match &command {
            DesktopProviderCommand::Frame {
                id, after_sequence, ..
            } => Some((id.clone(), *after_sequence)),
            _ => None,
        };
        let request = DesktopProviderRequest {
            permission_store: path.clone(),
            command,
        };
        let callbacks = Arc::new(InvocationCallbacks {
            permission: permission_store_capability(&path),
            capture,
            frame: Mutex::new(None),
        });
        match self
            .component
            .invoke_sobj(APP_USE_OS_OPERATION, &request, Some(callbacks.clone()))
            .map_err(error)?
        {
            ActiveNativeComponentResponse::Value(DesktopProviderResponse::Failed { error }) => {
                Err(error)
            }
            ActiveNativeComponentResponse::Value(response) => Ok((
                response,
                callbacks
                    .frame
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .take(),
            )),
            ActiveNativeComponentResponse::Error(response) => {
                Err(error(format!("{}: {}", response.code, response.detail)))
            }
        }
    }
    pub fn capabilities(&self) -> DesktopResult<DesktopCapabilities> {
        match self.invoke(DesktopProviderCommand::Capabilities)? {
            DesktopProviderResponse::Capabilities { capabilities } => Ok(capabilities),
            _ => Err(error("invalid capabilities response")),
        }
    }
    pub fn targets(&self, mode: DesktopControlMode) -> DesktopResult<Vec<DesktopTargetInfo>> {
        match self.invoke(DesktopProviderCommand::Targets { mode })? {
            DesktopProviderResponse::Targets { targets } => Ok(targets),
            _ => Err(error("invalid targets response")),
        }
    }
    pub fn children(&self, target: DesktopTarget) -> DesktopResult<Vec<DesktopTargetInfo>> {
        match self.invoke(DesktopProviderCommand::Children { target })? {
            DesktopProviderResponse::Targets { targets } => Ok(targets),
            _ => Err(error("invalid child targets response")),
        }
    }
    pub fn open_input(
        self: &Arc<Self>,
        target: DesktopTarget,
        mode: DesktopControlMode,
        lease_ms: u64,
    ) -> DesktopResult<NativeDesktopInput> {
        match self.invoke(DesktopProviderCommand::OpenInput {
            target,
            mode,
            lease_ms,
        })? {
            DesktopProviderResponse::Opened { id } => Ok(NativeDesktopInput {
                provider: self.clone(),
                id: Some(id),
            }),
            _ => Err(error("invalid input open response")),
        }
    }
    pub fn capture(
        self: &Arc<Self>,
        config: DesktopCaptureConfig,
    ) -> DesktopResult<NativeDesktopCapture> {
        let identity = DesktopCaptureIdentity::Target {
            target: config.target.clone(),
            application: None,
        };
        match self.invoke(DesktopProviderCommand::OpenCapture { config })? {
            DesktopProviderResponse::Opened { id } => Ok(NativeDesktopCapture {
                provider: self.clone(),
                id,
                identity,
            }),
            _ => Err(error("invalid capture open response")),
        }
    }
    pub fn unload(self) -> NativeModuleUnloadReport {
        self.component.unload()
    }
}
pub struct NativeApplication {
    pub id: String,
    pub application: DesktopApplication,
    provider: Arc<DesktopProviderClient>,
    closed: AtomicBool,
}
impl DesktopProviderClient {
    pub fn surface_capabilities(&self) -> DesktopResult<DesktopSurfaceCapabilities> {
        match self.invoke(DesktopProviderCommand::SurfaceCapabilities)? {
            DesktopProviderResponse::SurfaceCapabilities { capabilities } => Ok(capabilities),
            _ => Err(error("invalid surface capabilities")),
        }
    }
    pub fn applications(&self) -> DesktopResult<Vec<DesktopApplicationInfo>> {
        match self.invoke(DesktopProviderCommand::Applications)? {
            DesktopProviderResponse::Applications { applications } => Ok(applications),
            _ => Err(error("invalid applications response")),
        }
    }
    pub fn open_application(
        self: &Arc<Self>,
        application: DesktopApplication,
        lease_ms: u64,
    ) -> DesktopResult<(Arc<NativeApplication>, DesktopApplicationSnapshot)> {
        if !self.surface_capabilities()?.application_sessions {
            return Err(DesktopError::Unsupported(
                "application sessions unavailable on this provider".into(),
            ));
        }
        match self.invoke(DesktopProviderCommand::OpenApplication {
            application: application.clone(),
            lease_ms,
        })? {
            DesktopProviderResponse::ApplicationOpened {
                application_id,
                snapshot,
            } => Ok((
                Arc::new(NativeApplication {
                    id: application_id,
                    application,
                    provider: self.clone(),
                    closed: AtomicBool::new(false),
                }),
                snapshot,
            )),
            _ => Err(error("invalid application open response")),
        }
    }
    pub fn tray_items(&self) -> DesktopResult<DesktopSemanticSnapshot> {
        match self.invoke(DesktopProviderCommand::TrayItems)? {
            DesktopProviderResponse::SemanticItems { snapshot } => Ok(snapshot),
            _ => Err(error("invalid tray response")),
        }
    }
    pub fn invoke_tray(
        &self,
        item_id: String,
        operation: DesktopSemanticAction,
        allow_desktop_interaction: bool,
    ) -> DesktopResult<()> {
        match self.invoke(DesktopProviderCommand::InvokeTray {
            item_id,
            operation,
            allow_desktop_interaction,
        })? {
            DesktopProviderResponse::SemanticInvoked => Ok(()),
            _ => Err(error("invalid tray invocation response")),
        }
    }
}
impl NativeApplication {
    fn binding(&self, surface_id: &str) -> DesktopResult<ApplicationWindowBinding> {
        if self.closed.load(Ordering::Acquire) {
            return Err(DesktopError::Closed("application session closed".into()));
        }
        Ok(ApplicationWindowBinding {
            application_id: self.id.clone(),
            surface_id: surface_id.into(),
        })
    }
    pub fn snapshot(&self) -> DesktopResult<DesktopApplicationSnapshot> {
        self.binding("")?;
        match self
            .provider
            .invoke(DesktopProviderCommand::ApplicationSurfaces {
                application_id: self.id.clone(),
            })? {
            DesktopProviderResponse::ApplicationSurfaces { snapshot } => Ok(snapshot),
            _ => Err(error("invalid application snapshot")),
        }
    }
    pub fn resolve(
        &self,
        surface_id: &str,
        operation: &str,
    ) -> DesktopResult<DesktopWindowSurface> {
        match self
            .provider
            .invoke(DesktopProviderCommand::ResolveApplicationWindow {
                binding: self.binding(surface_id)?,
                operation: operation.into(),
            })? {
            DesktopProviderResponse::ApplicationWindow { window } => Ok(window),
            _ => Err(error("invalid window resolution")),
        }
    }
    pub fn open_input(&self, surface_id: &str, lease_ms: u64) -> DesktopResult<NativeDesktopInput> {
        match self
            .provider
            .invoke(DesktopProviderCommand::OpenApplicationInput {
                binding: self.binding(surface_id)?,
                lease_ms,
            })? {
            DesktopProviderResponse::Opened { id } => Ok(NativeDesktopInput {
                provider: self.provider.clone(),
                id: Some(id),
            }),
            _ => Err(error("invalid application input response")),
        }
    }
    pub fn capture(
        &self,
        surface_id: &str,
        config: DesktopCaptureConfig,
    ) -> DesktopResult<NativeDesktopCapture> {
        let binding = self.binding(surface_id)?;
        let identity = DesktopCaptureIdentity::Target {
            target: config.target.clone(),
            application: Some(binding.clone()),
        };
        match self
            .provider
            .invoke(DesktopProviderCommand::OpenApplicationCapture { binding, config })?
        {
            DesktopProviderResponse::Opened { id } => Ok(NativeDesktopCapture {
                provider: self.provider.clone(),
                id,
                identity,
            }),
            _ => Err(error("invalid application capture response")),
        }
    }
    pub fn menu_items(&self, surface_id: &str) -> DesktopResult<DesktopSemanticSnapshot> {
        match self.provider.invoke(DesktopProviderCommand::MenuItems {
            binding: self.binding(surface_id)?,
        })? {
            DesktopProviderResponse::SemanticItems { snapshot } => Ok(snapshot),
            _ => Err(error("invalid menu response")),
        }
    }
    pub fn invoke_menu_item(
        &self,
        surface_id: &str,
        item_id: String,
        allow_desktop_interaction: bool,
    ) -> DesktopResult<()> {
        match self
            .provider
            .invoke(DesktopProviderCommand::InvokeMenuItem {
                binding: self.binding(surface_id)?,
                item_id,
                allow_desktop_interaction,
            })? {
            DesktopProviderResponse::SemanticInvoked => Ok(()),
            _ => Err(error("invalid menu invocation response")),
        }
    }
    pub fn close(&self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            let _ = self
                .provider
                .invoke(DesktopProviderCommand::CloseApplication {
                    application_id: self.id.clone(),
                });
        }
    }
}
impl Drop for NativeApplication {
    fn drop(&mut self) {
        self.close();
    }
}

struct InvocationCallbacks {
    permission: String,
    capture: Option<(String, u64)>,
    frame: Mutex<Option<NativeVideoSample>>,
}
impl NativeModuleHostCallbacks for InvocationCallbacks {
    fn local_permission_granted(&self, permission: &str) -> bool {
        permission == self.permission
    }
    fn output_payload_status(&self, bytes: &[u8]) -> bool {
        let Some((expected_id, after)) = &self.capture else {
            return false;
        };
        if bytes.len() > MAX_FRAME_BYTES + 64 * 1024 {
            return false;
        }
        let Ok(delivery) = sinomo_runtime::module_api::decode_sobj::<DesktopFrameDelivery>(bytes)
        else {
            return false;
        };
        if delivery.capture_id != *expected_id || delivery.frame.sequence <= *after {
            return false;
        }
        let Ok(sample) = delivery.frame.into_sample() else {
            return false;
        };
        let mut frame = self.frame.lock().unwrap_or_else(|e| e.into_inner());
        if frame.is_some() {
            return false;
        }
        *frame = Some(sample);
        true
    }
}
pub struct NativeDesktopInput {
    provider: Arc<DesktopProviderClient>,
    id: Option<String>,
}
impl NativeDesktopInput {
    pub fn send(&mut self, events: &[DesktopInput]) -> DesktopResult<usize> {
        let id = self
            .id
            .clone()
            .ok_or_else(|| DesktopError::Closed("input session stopped".into()))?;
        match self.provider.invoke(DesktopProviderCommand::Input {
            id,
            events: events.to_vec(),
        })? {
            DesktopProviderResponse::Applied { events } => Ok(events),
            _ => Err(error("invalid input response")),
        }
    }
    pub fn stop(&mut self) {
        if let Some(id) = self.id.take() {
            let _ = self
                .provider
                .invoke(DesktopProviderCommand::CloseInput { id });
        }
    }
}
impl Drop for NativeDesktopInput {
    fn drop(&mut self) {
        self.stop();
    }
}
pub struct NativeDesktopCapture {
    provider: Arc<DesktopProviderClient>,
    id: String,
    identity: DesktopCaptureIdentity,
}
impl DesktopFrameSource for NativeDesktopCapture {
    fn identity(&self) -> Option<DesktopCaptureIdentity> {
        Some(self.identity.clone())
    }
    fn next(
        &self,
        after_sequence: u64,
        timeout: Duration,
    ) -> DesktopResult<Option<NativeVideoSample>> {
        match self
            .provider
            .invoke_collect(DesktopProviderCommand::Frame {
                id: self.id.clone(),
                after_sequence,
                timeout_ms: timeout.as_millis().min(100) as u64,
            })? {
            (DesktopProviderResponse::Frame { available }, sample)
                if available == sample.is_some() =>
            {
                Ok(sample)
            }
            _ => Err(error("invalid capture frame response")),
        }
    }
}
impl Drop for NativeDesktopCapture {
    fn drop(&mut self) {
        let _ = self.provider.invoke(DesktopProviderCommand::CloseCapture {
            id: self.id.clone(),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn capture_callback_rejects_unsolicited_stale_and_duplicate_frames() {
        let callbacks = InvocationCallbacks {
            permission: "bound".into(),
            capture: Some(("capture".into(), 4)),
            frame: Mutex::new(None),
        };
        let payload = |id: &str, sequence| {
            sinomo_runtime::module_api::encode_sobj(&DesktopFrameDelivery {
                capture_id: id.into(),
                frame: DesktopRawFrame {
                    sequence,
                    width: 1,
                    height: 1,
                    stride: 4,
                    timestamp_ns: 1,
                    duration_ns: 1,
                    captured_at_unix_ms: 1,
                    pixels: vec![0; 4],
                },
            })
            .unwrap()
        };
        assert!(!callbacks.output_payload_status(&payload("other", 5)));
        assert!(!callbacks.output_payload_status(&payload("capture", 4)));
        assert!(callbacks.output_payload_status(&payload("capture", 5)));
        assert!(!callbacks.output_payload_status(&payload("capture", 6)));
        let idle = InvocationCallbacks {
            permission: "bound".into(),
            capture: None,
            frame: Mutex::new(None),
        };
        assert!(!idle.output_payload_status(&payload("capture", 5)));
    }
}
