//! Typed DFS client for the active external host-filesystem component.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

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
    cancelled: Arc<AtomicBool>,
}

impl HostFilesystemCancellation {
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
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
        let callbacks = status_sink.map(|status_sink| {
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
    status_sink: Arc<dyn HostFilesystemStatusSink>,
    cancellation: Option<HostFilesystemCancellation>,
}

impl NativeModuleHostCallbacks for ProviderCallbacks {
    fn output_payload(&self, payload_sobj: &[u8]) {
        if let Ok(HostFilesystemProviderEvent::Status(status)) =
            sinomo_core::object::serde_ext::from_bytes(payload_sobj)
        {
            self.status_sink.status(status);
        }
    }

    fn is_cancelled(&self) -> bool {
        self.cancellation
            .as_ref()
            .is_some_and(HostFilesystemCancellation::is_cancelled)
    }
}
