//! Typed DFS client for the active external host-filesystem component.

use std::path::{Path, PathBuf};
use std::sync::Arc;
#[cfg(windows)]
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

#[cfg(windows)]
use windows_sys::Win32::Foundation::{CloseHandle, HANDLE};
#[cfg(windows)]
use windows_sys::Win32::System::Threading::{CreateEventW, SetEvent};

use sinomo_module::{
    ActiveNativeComponent, ActiveNativeComponentError, ActiveNativeComponentResponse,
    NativeModuleHostCallbacks, NativeModuleUnloadReport, ServiceRequirement,
};
use thiserror::Error;

use crate::{
    HOST_FILESYSTEM_PROVIDER_INTERFACE_ID, HOST_FILESYSTEM_PROVIDER_INTERFACE_VERSION,
    HOST_FILESYSTEM_PROVIDER_OPERATION, HostFilesystemProviderEvent, HostFilesystemProviderRequest,
    HostFilesystemProviderResponse, SifsMountConfig, SifsMountRuntimeStatus,
};

pub trait HostFilesystemStatusSink: Send + Sync {
    fn status(&self, status: SifsMountRuntimeStatus);
}

#[derive(Clone, Debug, Default)]
pub struct HostFilesystemCancellation {
    inner: Arc<HostFilesystemCancellationInner>,
}

#[derive(Debug, Default)]
struct HostFilesystemCancellationInner {
    cancelled: AtomicBool,
    #[cfg(windows)]
    wait_event: OnceLock<WindowsCancellationEvent>,
}

#[cfg(windows)]
struct WindowsCancellationEvent(HANDLE);

#[cfg(windows)]
unsafe impl Send for WindowsCancellationEvent {}
#[cfg(windows)]
unsafe impl Sync for WindowsCancellationEvent {}

#[cfg(windows)]
impl std::fmt::Debug for WindowsCancellationEvent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("WindowsCancellationEvent")
            .field(&(self.0 as usize))
            .finish()
    }
}

#[cfg(windows)]
impl Drop for WindowsCancellationEvent {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe {
                let _ = CloseHandle(self.0);
            }
        }
    }
}

impl HostFilesystemCancellation {
    pub fn cancel(&self) {
        self.inner.cancelled.store(true, Ordering::Release);
        #[cfg(windows)]
        if let Some(event) = self.inner.wait_event.get()
            && !event.0.is_null()
        {
            unsafe {
                let _ = SetEvent(event.0);
            }
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::Acquire)
    }

    #[cfg(windows)]
    fn native_wait_handle(&self) -> Option<u64> {
        let event = self.inner.wait_event.get_or_init(|| {
            let initially_cancelled = self.is_cancelled();
            WindowsCancellationEvent(unsafe {
                CreateEventW(
                    std::ptr::null(),
                    1,
                    i32::from(initially_cancelled),
                    std::ptr::null(),
                )
            })
        });
        if event.0.is_null() {
            return None;
        }
        // Close the creation/cancellation race: cancellation may have been
        // stored after CreateEventW sampled the initial state but before the
        // event became visible through the OnceLock.
        if self.is_cancelled() {
            unsafe {
                let _ = SetEvent(event.0);
            }
        }
        Some(event.0 as usize as u64)
    }
}

#[derive(Debug, Error)]
pub enum HostFilesystemProviderClientError {
    #[error(transparent)]
    Component(#[from] ActiveNativeComponentError),
    #[error("filesystem provider protocol failed: {0}")]
    Protocol(String),
}

pub struct HostFilesystemProviderClient {
    component: ActiveNativeComponent,
}

impl HostFilesystemProviderClient {
    pub fn from_active_component(
        data_dir_root: impl AsRef<Path>,
    ) -> Result<Self, HostFilesystemProviderClientError> {
        let requirement = host_filesystem_provider_requirement();
        #[cfg(windows)]
        let component = unsafe {
            ActiveNativeComponent::resolve_process_resident(data_dir_root, &requirement)
        }?;
        #[cfg(not(windows))]
        let component = unsafe { ActiveNativeComponent::resolve(data_dir_root, &requirement) }?;
        Ok(Self { component })
    }

    pub fn reconcile(
        &self,
        data_dir_root: PathBuf,
        mount_point: PathBuf,
        config: SifsMountConfig,
        status_sink: Option<Arc<dyn HostFilesystemStatusSink>>,
        cancellation: HostFilesystemCancellation,
    ) -> Result<HostFilesystemProviderResponse, HostFilesystemProviderClientError> {
        self.invoke(
            HostFilesystemProviderRequest::Reconcile {
                data_dir_root,
                mount_point,
                config,
            },
            status_sink,
            Some(cancellation),
        )
    }

    pub fn stop(
        &self,
        data_dir_root: PathBuf,
        mount_point: PathBuf,
        config: SifsMountConfig,
        preserve_state: bool,
    ) -> Result<HostFilesystemProviderResponse, HostFilesystemProviderClientError> {
        self.invoke(
            HostFilesystemProviderRequest::Stop {
                data_dir_root,
                mount_point,
                config,
                preserve_state,
            },
            None,
            None,
        )
    }

    fn invoke(
        &self,
        request: HostFilesystemProviderRequest,
        status_sink: Option<Arc<dyn HostFilesystemStatusSink>>,
        cancellation: Option<HostFilesystemCancellation>,
    ) -> Result<HostFilesystemProviderResponse, HostFilesystemProviderClientError> {
        let callbacks = (status_sink.is_some() || cancellation.is_some()).then(|| {
            Arc::new(ProviderCallbacks {
                status_sink,
                cancellation,
            }) as Arc<dyn NativeModuleHostCallbacks>
        });
        match self
            .component
            .invoke_sobj(HOST_FILESYSTEM_PROVIDER_OPERATION, &request, callbacks)?
        {
            ActiveNativeComponentResponse::Value(response) => Ok(response),
            ActiveNativeComponentResponse::Error(error) => {
                Err(HostFilesystemProviderClientError::Protocol(format!(
                    "{}: {}",
                    error.code, error.detail
                )))
            }
        }
    }

    pub fn unload(self) -> NativeModuleUnloadReport {
        self.component.unload()
    }
}

fn host_filesystem_provider_requirement() -> ServiceRequirement {
    ServiceRequirement::new(
        HOST_FILESYSTEM_PROVIDER_INTERFACE_ID,
        HOST_FILESYSTEM_PROVIDER_INTERFACE_VERSION,
    )
    .from_service("host-filesystem-provider")
}

struct ProviderCallbacks {
    status_sink: Option<Arc<dyn HostFilesystemStatusSink>>,
    cancellation: Option<HostFilesystemCancellation>,
}

impl NativeModuleHostCallbacks for ProviderCallbacks {
    fn output_payload(&self, payload_sobj: &[u8]) {
        if let (Some(status_sink), Ok(HostFilesystemProviderEvent::Status(status))) = (
            self.status_sink.as_ref(),
            sinomo_core::object::serde_ext::from_bytes(payload_sobj),
        ) {
            status_sink.status(status);
        }
    }

    fn is_cancelled(&self) -> bool {
        self.cancellation
            .as_ref()
            .is_some_and(HostFilesystemCancellation::is_cancelled)
    }

    fn cancellation_wait_handle(&self) -> Option<u64> {
        #[cfg(windows)]
        {
            self.cancellation
                .as_ref()
                .and_then(HostFilesystemCancellation::native_wait_handle)
        }
        #[cfg(not(windows))]
        {
            None
        }
    }
}

#[cfg(all(test, windows))]
mod windows_tests {
    use super::*;
    use windows_sys::Win32::Foundation::{WAIT_OBJECT_0, WAIT_TIMEOUT};
    use windows_sys::Win32::System::Threading::WaitForSingleObject;

    #[test]
    fn native_wait_handle_is_signaled_by_cancellation() {
        let cancellation = HostFilesystemCancellation::default();
        let handle = cancellation
            .native_wait_handle()
            .expect("create cancellation wait handle");
        let handle = handle as usize as HANDLE;

        assert_eq!(unsafe { WaitForSingleObject(handle, 0) }, WAIT_TIMEOUT);
        cancellation.cancel();
        assert_eq!(unsafe { WaitForSingleObject(handle, 0) }, WAIT_OBJECT_0);
    }
}
