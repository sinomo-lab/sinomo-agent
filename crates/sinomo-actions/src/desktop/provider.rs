//! Provider-neutral native binding. This host adapter contains no OS APIs and
//! never falls back to linked desktop code when the native provider is absent.
use sinomo_runtime::device::{
    NativeVideoSample,
    desktop::{protocol::*, *},
};
use sinomo_runtime::module::*;
use sinomo_runtime::node::LocalNodePermissionStore;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
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
            DesktopProviderCommand::CloseInput { .. } | DesktopProviderCommand::CloseCapture { .. }
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
        match self.invoke(DesktopProviderCommand::OpenCapture { config })? {
            DesktopProviderResponse::Opened { id } => Ok(NativeDesktopCapture {
                provider: self.clone(),
                id,
            }),
            _ => Err(error("invalid capture open response")),
        }
    }
    pub fn unload(self) -> NativeModuleUnloadReport {
        self.component.unload()
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
}
impl DesktopFrameSource for NativeDesktopCapture {
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
