//! Typed host wrapper for the trusted native-dylib WASIX sandbox module.
//!
//! The dynamic loading logic lives in `sinomo-module`; this crate only
//! adapts the generic native-module request envelope to WASIX sandbox payloads.

mod runner;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use sinomo_module::{
    NativeModule, NativeModuleHostCallbacks, NativeModuleHostError, NativeModuleUnloadReport,
    native_module_cache_path,
};
use sinomo_module_api::{
    NativeModuleInvocationRequest, NativeModuleRequest, NativeModuleResponse, decode_sobj,
    encode_sobj,
};
use sinomo_sandbox::{SandboxCancellationToken, SandboxOutputChunk, SandboxOutputSink};
use sinomo_sandbox_wasix_api::{
    SINOMO_SANDBOX_WASIX_INTERFACE_VERSION, SINOMO_SANDBOX_WASIX_MODULE_ID,
    SINOMO_SANDBOX_WASIX_OPERATION, SandboxWasixNativeRequest, SandboxWasixNativeResponse,
};

pub use runner::{
    SandboxWasixModuleCommandRunner, SandboxWasixSifsCommandOutcome, SandboxWasixSifsCommandRunner,
    SandboxWasixSifsProcessPlanOutcome, run_sandbox_wasm_module_replay_fixture_with_active_module,
};

#[derive(Debug, thiserror::Error)]
pub enum SandboxWasixNativeModuleError {
    #[error(transparent)]
    Host(#[from] NativeModuleHostError),
    #[error("WASIX native module descriptor mismatch: {0}")]
    Descriptor(String),
    #[error("WASIX native module object error: {0}")]
    Object(String),
    #[error("WASIX native module returned an unexpected response: {0}")]
    UnexpectedResponse(String),
}

pub struct SandboxWasixNativeModule {
    module: NativeModule,
}

impl SandboxWasixNativeModule {
    /// Load a trusted native WASIX module from a content-addressed path.
    ///
    /// # Safety
    ///
    /// A native-dylib module shares the host process address space. Loading is
    /// only appropriate for trusted artifacts that already passed policy and
    /// provenance checks.
    pub unsafe fn load_from_content_addressed_path(
        library_path: impl AsRef<Path>,
        expected_content_hash: &str,
    ) -> Result<Self, SandboxWasixNativeModuleError> {
        let module = unsafe {
            NativeModule::load_from_content_addressed_path(library_path, expected_content_hash)?
        };
        Self::from_native_module(module)
    }

    /// Load a native WASIX module after the caller has already verified bytes.
    ///
    /// # Safety
    ///
    /// The same trust requirements as [`Self::load_from_content_addressed_path`]
    /// apply.
    pub unsafe fn load_verified(
        library_path: impl AsRef<Path>,
    ) -> Result<Self, SandboxWasixNativeModuleError> {
        let module = unsafe { NativeModule::load_verified(library_path)? };
        Self::from_native_module(module)
    }

    pub fn from_native_module(module: NativeModule) -> Result<Self, SandboxWasixNativeModuleError> {
        let descriptor = module.descriptor();
        if descriptor.module_id != SINOMO_SANDBOX_WASIX_MODULE_ID {
            return Err(SandboxWasixNativeModuleError::Descriptor(format!(
                "expected module_id `{}`, got `{}`",
                SINOMO_SANDBOX_WASIX_MODULE_ID, descriptor.module_id
            )));
        }
        if descriptor.interface_version != SINOMO_SANDBOX_WASIX_INTERFACE_VERSION {
            return Err(SandboxWasixNativeModuleError::Descriptor(format!(
                "expected interface `{}`, got `{}`",
                SINOMO_SANDBOX_WASIX_INTERFACE_VERSION, descriptor.interface_version
            )));
        }
        if !descriptor
            .supported_operations
            .iter()
            .any(|operation| operation == SINOMO_SANDBOX_WASIX_OPERATION)
        {
            return Err(SandboxWasixNativeModuleError::Descriptor(format!(
                "module does not support `{}`",
                SINOMO_SANDBOX_WASIX_OPERATION
            )));
        }
        Ok(Self { module })
    }

    pub fn descriptor(&self) -> &sinomo_module_api::NativeModuleDescriptor {
        self.module.descriptor()
    }

    pub fn unload(self) -> NativeModuleUnloadReport {
        self.module.unload()
    }

    pub fn invoke(
        &self,
        request: &SandboxWasixNativeRequest,
    ) -> Result<SandboxWasixNativeResponse, SandboxWasixNativeModuleError> {
        self.invoke_with_output_sink(request, None)
    }

    pub fn invoke_with_output_sink(
        &self,
        request: &SandboxWasixNativeRequest,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
    ) -> Result<SandboxWasixNativeResponse, SandboxWasixNativeModuleError> {
        self.invoke_with_output_sink_and_cancellation(request, output_sink, None)
    }

    pub fn invoke_with_output_sink_and_cancellation(
        &self,
        request: &SandboxWasixNativeRequest,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
        cancellation_token: Option<SandboxCancellationToken>,
    ) -> Result<SandboxWasixNativeResponse, SandboxWasixNativeModuleError> {
        let payload_sobj = encode_sobj(request)
            .map_err(|source| SandboxWasixNativeModuleError::Object(source.to_string()))?;
        let native_request = NativeModuleRequest::Invoke(NativeModuleInvocationRequest {
            operation: SINOMO_SANDBOX_WASIX_OPERATION.to_string(),
            payload_sobj,
        });
        let callbacks = (output_sink.is_some() || cancellation_token.is_some()).then(|| {
            Arc::new(SandboxWasixHostCallbacks {
                output_sink,
                cancellation_token,
            }) as Arc<dyn NativeModuleHostCallbacks>
        });
        match self
            .module
            .invoke_with_callbacks(&native_request, callbacks)?
        {
            NativeModuleResponse::Invocation(response)
                if response.operation == SINOMO_SANDBOX_WASIX_OPERATION =>
            {
                decode_sobj(&response.payload_sobj)
                    .map_err(|source| SandboxWasixNativeModuleError::Object(source.to_string()))
            }
            NativeModuleResponse::Error(error) => Ok(SandboxWasixNativeResponse::Error(
                sinomo_sandbox_wasix_api::SandboxWasixNativeErrorResponse {
                    code: error.code,
                    detail: error.detail,
                },
            )),
            other => Err(SandboxWasixNativeModuleError::UnexpectedResponse(format!(
                "{other:?}"
            ))),
        }
    }
}

struct SandboxWasixHostCallbacks {
    output_sink: Option<Arc<dyn SandboxOutputSink>>,
    cancellation_token: Option<SandboxCancellationToken>,
}

impl NativeModuleHostCallbacks for SandboxWasixHostCallbacks {
    fn output_payload(&self, payload_sobj: &[u8]) {
        if let (Some(output_sink), Ok(chunk)) = (
            self.output_sink.as_ref(),
            decode_sobj::<SandboxOutputChunk>(payload_sobj),
        ) {
            output_sink.output_chunk(chunk);
        }
    }

    fn is_cancelled(&self) -> bool {
        self.cancellation_token
            .as_ref()
            .is_some_and(SandboxCancellationToken::is_cancelled)
    }
}

pub fn sandbox_wasix_native_module_library_filename() -> &'static str {
    #[cfg(target_os = "windows")]
    {
        "sinomo_sandbox_wasix_module.dll"
    }
    #[cfg(target_os = "macos")]
    {
        "libsinomo_sandbox_wasix_module.dylib"
    }
    #[cfg(all(not(target_os = "windows"), not(target_os = "macos")))]
    {
        "libsinomo_sandbox_wasix_module.so"
    }
}

pub fn sandbox_wasix_native_module_cache_path(
    data_dir_root: impl AsRef<Path>,
    content_hash: &str,
) -> PathBuf {
    native_module_cache_path(
        data_dir_root,
        "module/sandbox/wasix",
        content_hash,
        sandbox_wasix_native_module_library_filename(),
    )
}
