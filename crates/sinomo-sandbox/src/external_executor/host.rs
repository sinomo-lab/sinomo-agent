//! Typed sandbox client for the active external executor component.

use std::path::Path;
use std::sync::Arc;

use crate::{
    SANDBOX_EXECUTOR_INTERFACE_ID, SANDBOX_EXECUTOR_INTERFACE_VERSION, SANDBOX_EXECUTOR_OPERATION,
    SandboxCancellationToken, SandboxExecutorErrorResponse, SandboxExecutorRequest,
    SandboxExecutorResponse, SandboxOutputChunk, SandboxOutputSink, decode_sandbox_executor,
};
use sinomo_module::{
    ActiveNativeComponent, ActiveNativeComponentError, ActiveNativeComponentResponse,
    NativeModuleHostCallbacks, NativeModuleUnloadReport, ServiceRequirement,
};

#[derive(Debug, thiserror::Error)]
pub enum SandboxExecutorClientError {
    #[error(transparent)]
    Component(#[from] ActiveNativeComponentError),
}

pub struct SandboxExecutorClient {
    component: ActiveNativeComponent,
}

impl SandboxExecutorClient {
    pub fn from_active_component(
        data_dir_root: impl AsRef<Path>,
    ) -> Result<Self, SandboxExecutorClientError> {
        let requirement = sandbox_executor_requirement();
        let component = unsafe { ActiveNativeComponent::resolve(data_dir_root, &requirement) }?;
        Ok(Self { component })
    }

    /// Load a trusted native sandbox executor from a content-addressed path.
    ///
    /// # Safety
    ///
    /// A native-dylib module shares the host process address space. Loading is
    /// only appropriate for trusted artifacts that already passed policy and
    /// provenance checks.
    pub unsafe fn load_from_content_addressed_path(
        library_path: impl AsRef<Path>,
        expected_content_hash: &str,
    ) -> Result<Self, SandboxExecutorClientError> {
        let requirement = sandbox_executor_requirement();
        let component = unsafe {
            ActiveNativeComponent::load_from_content_addressed_path(
                library_path,
                expected_content_hash,
                &requirement,
            )
        }?;
        Ok(Self { component })
    }

    /// Load a native sandbox executor after the caller has already verified bytes.
    ///
    /// # Safety
    ///
    /// The same trust requirements as [`Self::load_from_content_addressed_path`]
    /// apply.
    pub unsafe fn load_verified(
        library_path: impl AsRef<Path>,
    ) -> Result<Self, SandboxExecutorClientError> {
        let requirement = sandbox_executor_requirement();
        let component =
            unsafe { ActiveNativeComponent::load_verified(library_path, &requirement) }?;
        Ok(Self { component })
    }

    pub fn descriptor(&self) -> &sinomo_module::NativeModuleDescriptor {
        self.component.descriptor()
    }

    pub fn unload(self) -> NativeModuleUnloadReport {
        self.component.unload()
    }

    pub fn invoke(
        &self,
        request: &SandboxExecutorRequest,
    ) -> Result<SandboxExecutorResponse, SandboxExecutorClientError> {
        self.invoke_with_output_sink(request, None)
    }

    pub fn invoke_with_output_sink(
        &self,
        request: &SandboxExecutorRequest,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
    ) -> Result<SandboxExecutorResponse, SandboxExecutorClientError> {
        self.invoke_with_output_sink_and_cancellation(request, output_sink, None)
    }

    pub fn invoke_with_output_sink_and_cancellation(
        &self,
        request: &SandboxExecutorRequest,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
        cancellation_token: Option<SandboxCancellationToken>,
    ) -> Result<SandboxExecutorResponse, SandboxExecutorClientError> {
        let callbacks = (output_sink.is_some() || cancellation_token.is_some()).then(|| {
            Arc::new(SandboxExecutorHostCallbacks {
                output_sink,
                cancellation_token,
            }) as Arc<dyn NativeModuleHostCallbacks>
        });
        match self
            .component
            .invoke_sobj(SANDBOX_EXECUTOR_OPERATION, request, callbacks)?
        {
            ActiveNativeComponentResponse::Value(response) => Ok(response),
            ActiveNativeComponentResponse::Error(error) => Ok(SandboxExecutorResponse::Error(
                SandboxExecutorErrorResponse {
                    code: error.code,
                    detail: error.detail,
                },
            )),
        }
    }
}

fn sandbox_executor_requirement() -> ServiceRequirement {
    ServiceRequirement::new(
        SANDBOX_EXECUTOR_INTERFACE_ID,
        SANDBOX_EXECUTOR_INTERFACE_VERSION,
    )
    .from_service("sandbox-executor")
}

struct SandboxExecutorHostCallbacks {
    output_sink: Option<Arc<dyn SandboxOutputSink>>,
    cancellation_token: Option<SandboxCancellationToken>,
}

impl NativeModuleHostCallbacks for SandboxExecutorHostCallbacks {
    fn output_payload(&self, payload_sobj: &[u8]) {
        if let (Some(output_sink), Ok(chunk)) = (
            self.output_sink.as_ref(),
            decode_sandbox_executor::<SandboxOutputChunk>(payload_sobj),
        ) {
            output_sink.output_chunk(chunk);
        }
    }

    fn is_cancelled(&self) -> bool {
        let cancelled = self
            .cancellation_token
            .as_ref()
            .is_some_and(SandboxCancellationToken::is_cancelled);
        if cancelled && std::env::var_os("SINOMO_SANDBOX_EXECUTOR_DEBUG").is_some() {
            eprintln!("sinomo-debug sandbox executor callback observed cancellation");
        }
        cancelled
    }
}
