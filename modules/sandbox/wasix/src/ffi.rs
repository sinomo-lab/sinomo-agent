use std::panic::{AssertUnwindSafe, catch_unwind};
#[cfg(feature = "wasix-runtime")]
use std::sync::Arc;
use std::sync::Mutex;

use sinomo_sandbox_wasix_api::{
    NativeModuleBackend, NativeModuleCapabilitiesResponse, NativeModuleDescriptor,
    NativeModuleErrorResponse, NativeModuleInvocationResponse, NativeModuleRequest,
    NativeModuleResponse, SINOMO_SANDBOX_WASIX_INTERFACE_VERSION, SINOMO_SANDBOX_WASIX_MODULE_ID,
    SINOMO_SANDBOX_WASIX_NATIVE_ABI_VERSION, SINOMO_SANDBOX_WASIX_OPERATION,
    SINOMO_WASIX_STATUS_INVALID_ARGUMENT, SINOMO_WASIX_STATUS_OK,
    SINOMO_WASIX_STATUS_RUNTIME_ERROR, SINOMO_WASIX_STATUS_UNSUPPORTED_ABI,
    SandboxWasixCapabilitiesResponse, SandboxWasixNativeErrorResponse, SandboxWasixNativeRequest,
    SandboxWasixNativeResponse, SandboxWasixRequestKind, SandboxWasixRunModuleRequest,
    SandboxWasixRunReplayFixtureRequest, SandboxWasixRunToolchainCommandRequest,
    SandboxWasixValidateComponentRequest, SinomoWasixByteBuffer, SinomoWasixByteSlice,
    SinomoWasixHostApiV1, SinomoWasixStatusCode, decode_sobj, encode_sobj,
};

#[cfg(feature = "wasix-runtime")]
use sinomo_sandbox_wasix_api::{
    SandboxWasixRunModuleResponse, SandboxWasixRunReplayFixtureResponse,
    SandboxWasixValidateComponentResponse, encode_output_chunk,
};

#[cfg(feature = "wasix-runtime")]
use crate::{
    SandboxLocalCommandOutcome, SandboxOutputChunk, SandboxOutputSink, SandboxWasiAbi,
    SandboxWasiPreopen, SandboxWasiRunOptions, SandboxWasixCommandOverride,
};

#[derive(Clone, Copy)]
#[cfg_attr(not(feature = "wasix-runtime"), allow(dead_code))]
struct StoredHostApi(SinomoWasixHostApiV1);

unsafe impl Send for StoredHostApi {}
unsafe impl Sync for StoredHostApi {}

static HOST_API: Mutex<Option<StoredHostApi>> = Mutex::new(None);

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_api_version() -> u32 {
    SINOMO_SANDBOX_WASIX_NATIVE_ABI_VERSION
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_descriptor(
    out: *mut SinomoWasixByteBuffer,
) -> SinomoWasixStatusCode {
    write_buffer(out, descriptor_response())
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_init(host: SinomoWasixHostApiV1) -> SinomoWasixStatusCode {
    if host.abi_version != SINOMO_SANDBOX_WASIX_NATIVE_ABI_VERSION {
        return SINOMO_WASIX_STATUS_UNSUPPORTED_ABI;
    }
    match HOST_API.lock() {
        Ok(mut api) => {
            *api = Some(StoredHostApi(host));
            SINOMO_WASIX_STATUS_OK
        }
        Err(_) => SINOMO_WASIX_STATUS_RUNTIME_ERROR,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_invoke(
    request_sobj: SinomoWasixByteSlice,
    response_sobj: *mut SinomoWasixByteBuffer,
) -> SinomoWasixStatusCode {
    let result = catch_unwind(AssertUnwindSafe(|| unsafe {
        invoke_unchecked(request_sobj, response_sobj)
    }));
    match result {
        Ok(status) => status,
        Err(_) => write_response(
            response_sobj,
            NativeModuleResponse::Error(native_error_response(
                "panic",
                "native module panicked while handling invocation",
            )),
            SINOMO_WASIX_STATUS_RUNTIME_ERROR,
        ),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_shutdown() -> SinomoWasixStatusCode {
    match HOST_API.lock() {
        Ok(mut api) => {
            *api = None;
            SINOMO_WASIX_STATUS_OK
        }
        Err(_) => SINOMO_WASIX_STATUS_RUNTIME_ERROR,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_free_buffer(buffer: SinomoWasixByteBuffer) {
    unsafe {
        drop(SinomoWasixByteBuffer::into_vec(buffer));
    }
}

unsafe fn invoke_unchecked(
    request_sobj: SinomoWasixByteSlice,
    response_sobj: *mut SinomoWasixByteBuffer,
) -> SinomoWasixStatusCode {
    let request_bytes = unsafe { request_sobj.as_slice() };
    let request = match decode_sobj::<NativeModuleRequest>(request_bytes) {
        Ok(request) => request,
        Err(error) => {
            return write_response(
                response_sobj,
                NativeModuleResponse::Error(native_error_response(
                    "invalid-request",
                    &format!("decode native module request: {error}"),
                )),
                SINOMO_WASIX_STATUS_INVALID_ARGUMENT,
            );
        }
    };
    let response = match request {
        NativeModuleRequest::ProbeCapabilities(_) => {
            NativeModuleResponse::Capabilities(NativeModuleCapabilitiesResponse {
                descriptor: descriptor(),
            })
        }
        NativeModuleRequest::Invoke(request) => invoke_wasix_operation_response(request),
    };
    write_response(response_sobj, response, SINOMO_WASIX_STATUS_OK)
}

fn invoke_wasix_operation_response(
    request: sinomo_sandbox_wasix_api::NativeModuleInvocationRequest,
) -> NativeModuleResponse {
    if request.operation != SINOMO_SANDBOX_WASIX_OPERATION {
        return NativeModuleResponse::Error(native_error_response(
            "unsupported-operation",
            &format!(
                "unsupported native module operation `{}`",
                request.operation
            ),
        ));
    }
    let wasix_request = match decode_sobj::<SandboxWasixNativeRequest>(&request.payload_sobj) {
        Ok(request) => request,
        Err(error) => {
            return NativeModuleResponse::Error(native_error_response(
                "invalid-wasix-request",
                &format!("decode WASIX request payload: {error}"),
            ));
        }
    };
    let wasix_response = match wasix_request {
        SandboxWasixNativeRequest::ProbeCapabilities(_) => {
            SandboxWasixNativeResponse::Capabilities(SandboxWasixCapabilitiesResponse {
                descriptor: legacy_wasix_descriptor(),
            })
        }
        SandboxWasixNativeRequest::RunModule(request) => run_module_response(request),
        SandboxWasixNativeRequest::RunToolchainCommand(request) => {
            run_toolchain_command_response(request)
        }
        SandboxWasixNativeRequest::RunReplayFixture(request) => {
            run_replay_fixture_response(request)
        }
        SandboxWasixNativeRequest::ValidateComponent(request) => {
            validate_component_response(request)
        }
    };
    match encode_sobj(&wasix_response) {
        Ok(payload_sobj) => NativeModuleResponse::Invocation(NativeModuleInvocationResponse {
            operation: SINOMO_SANDBOX_WASIX_OPERATION.to_string(),
            payload_sobj,
        }),
        Err(error) => NativeModuleResponse::Error(native_error_response(
            "encode-wasix-response",
            &format!("encode WASIX response payload: {error}"),
        )),
    }
}

#[cfg(feature = "wasix-runtime")]
fn run_module_response(request: SandboxWasixRunModuleRequest) -> SandboxWasixNativeResponse {
    match run_resolved_module(
        request.target_abi,
        request.wasm,
        request.command,
        request.options,
    ) {
        Ok(outcome) => {
            SandboxWasixNativeResponse::RunModule(SandboxWasixRunModuleResponse { outcome })
        }
        Err(error) => SandboxWasixNativeResponse::Error(error_response("run-module", &error)),
    }
}

#[cfg(not(feature = "wasix-runtime"))]
fn run_module_response(_request: SandboxWasixRunModuleRequest) -> SandboxWasixNativeResponse {
    runtime_disabled_response("run-module")
}

#[cfg(feature = "wasix-runtime")]
fn run_toolchain_command_response(
    request: SandboxWasixRunToolchainCommandRequest,
) -> SandboxWasixNativeResponse {
    match run_resolved_module(
        request.module.target_abi,
        request.module.wasm,
        request.command,
        request.options,
    ) {
        Ok(outcome) => {
            SandboxWasixNativeResponse::RunToolchainCommand(SandboxWasixRunModuleResponse {
                outcome,
            })
        }
        Err(error) => SandboxWasixNativeResponse::Error(error_response("run-toolchain", &error)),
    }
}

#[cfg(not(feature = "wasix-runtime"))]
fn run_toolchain_command_response(
    _request: SandboxWasixRunToolchainCommandRequest,
) -> SandboxWasixNativeResponse {
    runtime_disabled_response("run-toolchain")
}

#[cfg(feature = "wasix-runtime")]
fn run_replay_fixture_response(
    request: SandboxWasixRunReplayFixtureRequest,
) -> SandboxWasixNativeResponse {
    match crate::run_sandbox_wasm_module_replay_fixture(&request.data_dir_root, &request.fixture) {
        Ok(report) => {
            SandboxWasixNativeResponse::RunReplayFixture(SandboxWasixRunReplayFixtureResponse {
                report,
            })
        }
        Err(error) => SandboxWasixNativeResponse::Error(error_response("run-replay", &error)),
    }
}

#[cfg(not(feature = "wasix-runtime"))]
fn run_replay_fixture_response(
    _request: SandboxWasixRunReplayFixtureRequest,
) -> SandboxWasixNativeResponse {
    runtime_disabled_response("run-replay")
}

#[cfg(feature = "wasix-runtime")]
fn validate_component_response(
    request: SandboxWasixValidateComponentRequest,
) -> SandboxWasixNativeResponse {
    match crate::validate_sinomo_module_v1_component_contract(&request.wasm) {
        Ok(()) => SandboxWasixNativeResponse::ValidateComponent(
            SandboxWasixValidateComponentResponse::default(),
        ),
        Err(error) => {
            SandboxWasixNativeResponse::Error(error_response("validate-component", &error))
        }
    }
}

#[cfg(not(feature = "wasix-runtime"))]
fn validate_component_response(
    _request: SandboxWasixValidateComponentRequest,
) -> SandboxWasixNativeResponse {
    runtime_disabled_response("validate-component")
}

#[cfg(not(feature = "wasix-runtime"))]
fn runtime_disabled_response(operation: &str) -> SandboxWasixNativeResponse {
    SandboxWasixNativeResponse::Error(error_response(
        operation,
        "WASIX native module was built without the `wasix-runtime` feature",
    ))
}

#[cfg(feature = "wasix-runtime")]
fn run_resolved_module(
    target_abi: SandboxWasiAbi,
    wasm: Vec<u8>,
    command: crate::SandboxCommandRequest,
    options: sinomo_sandbox_wasix_api::SandboxWasixRunModuleOptions,
) -> Result<SandboxLocalCommandOutcome, String> {
    let runner =
        crate::LocalStagedSandboxCommandRunner::new().map_err(|error| error.to_string())?;
    runner
        .run_module_for_abi(target_abi, &wasm, command, convert_run_options(options))
        .map_err(|error| error.to_string())
}

#[cfg(feature = "wasix-runtime")]
fn convert_run_options(
    options: sinomo_sandbox_wasix_api::SandboxWasixRunModuleOptions,
) -> SandboxWasiRunOptions {
    let default_options = SandboxWasiRunOptions::default();
    SandboxWasiRunOptions {
        preopened_dirs: options
            .preopened_dirs
            .into_iter()
            .map(|preopen| SandboxWasiPreopen {
                host_path: preopen.host_path,
                guest_path: preopen.guest_path,
                mode: preopen.mode,
            })
            .collect(),
        #[cfg(feature = "wasix-runtime")]
        virtual_preopened_dirs: Vec::new(),
        #[cfg(feature = "wasix-runtime")]
        wasix_stdin_pipe: None,
        #[cfg(feature = "wasix-runtime")]
        wasix_stdout_pipe: None,
        enable_virtual_devices: options.enable_virtual_devices,
        zero_device_bytes: usize_or_default(
            options.zero_device_bytes,
            default_options.zero_device_bytes,
        ),
        memory_limit_bytes: usize_or_default(
            options.memory_limit_bytes,
            default_options.memory_limit_bytes,
        ),
        fuel: options.fuel,
        input_limit_bytes: usize_or_default(
            options.input_limit_bytes,
            default_options.input_limit_bytes,
        ),
        output_limit_bytes: usize_or_default(
            options.output_limit_bytes,
            default_options.output_limit_bytes,
        ),
        workspace_limit_bytes: options.workspace_limit_bytes,
        file_count_limit: options.file_count_limit,
        cancellation: default_options.cancellation,
        output_sink: host_output_sink(),
        wasix_pinned_child_exec_only: options.wasix_pinned_child_exec_only,
        wasix_instruction_metering: options.wasix_instruction_metering,
        wasix_auto_asyncify: options.wasix_auto_asyncify,
        wasix_fpcast_emu: options.wasix_fpcast_emu,
        wasix_command_overrides: options
            .wasix_command_overrides
            .into_iter()
            .map(|override_| SandboxWasixCommandOverride {
                name: override_.name,
                argv_prefix: override_.argv_prefix,
                exec_name: override_.exec_name,
            })
            .collect(),
    }
}

#[cfg(feature = "wasix-runtime")]
fn host_output_sink() -> Option<Arc<dyn SandboxOutputSink>> {
    let api = HOST_API.lock().ok().and_then(|api| *api)?;
    api.0
        .output_chunk
        .map(|_| Arc::new(HostOutputSink { api }) as Arc<dyn SandboxOutputSink>)
}

#[cfg(feature = "wasix-runtime")]
struct HostOutputSink {
    api: StoredHostApi,
}

#[cfg(feature = "wasix-runtime")]
unsafe impl Send for HostOutputSink {}
#[cfg(feature = "wasix-runtime")]
unsafe impl Sync for HostOutputSink {}

#[cfg(feature = "wasix-runtime")]
impl SandboxOutputSink for HostOutputSink {
    fn output_chunk(&self, chunk: SandboxOutputChunk) {
        let Some(output_chunk) = self.api.0.output_chunk else {
            return;
        };
        let Ok(bytes) = encode_output_chunk(&chunk) else {
            return;
        };
        unsafe {
            let _ = output_chunk(
                self.api.0.user_data,
                SinomoWasixByteSlice::from_slice(&bytes),
            );
        }
    }
}

fn descriptor_response() -> Result<Vec<u8>, sinomo_sandbox_wasix_api::SandboxWasixApiError> {
    encode_sobj(&descriptor())
}

fn descriptor() -> NativeModuleDescriptor {
    NativeModuleDescriptor {
        module_id: SINOMO_SANDBOX_WASIX_MODULE_ID.to_string(),
        module_version: env!("CARGO_PKG_VERSION").to_string(),
        owner: "sinomo-sandbox-wasix".to_string(),
        native_abi_version: SINOMO_SANDBOX_WASIX_NATIVE_ABI_VERSION,
        backend: NativeModuleBackend::NativeDylib,
        interface_version: SINOMO_SANDBOX_WASIX_INTERFACE_VERSION.to_string(),
        platform_triple: platform_triple(),
        supported_operations: vec![SINOMO_SANDBOX_WASIX_OPERATION.to_string()],
        capabilities: runtime_capabilities(),
        surfaces: Default::default(),
    }
}

fn legacy_wasix_descriptor() -> sinomo_sandbox_wasix_api::SandboxWasixNativeDescriptor {
    sinomo_sandbox_wasix_api::SandboxWasixNativeDescriptor {
        module_id: SINOMO_SANDBOX_WASIX_MODULE_ID.to_string(),
        module_version: env!("CARGO_PKG_VERSION").to_string(),
        native_abi_version: SINOMO_SANDBOX_WASIX_NATIVE_ABI_VERSION,
        interface_version: SINOMO_SANDBOX_WASIX_INTERFACE_VERSION.to_string(),
        platform_triple: platform_triple(),
        supported_requests: supported_wasix_requests(),
        capabilities: descriptor().capabilities,
    }
}

fn runtime_capabilities() -> Vec<String> {
    if cfg!(feature = "wasix-runtime") {
        vec![
            "wasix".to_string(),
            "wasix.virtual-fs".to_string(),
            "wasix.metering".to_string(),
            "wasix.memory-limits".to_string(),
            "wasix.output-streaming".to_string(),
        ]
    } else {
        vec!["runtime.disabled".to_string()]
    }
}

fn supported_wasix_requests() -> Vec<SandboxWasixRequestKind> {
    let mut requests = vec![SandboxWasixRequestKind::ProbeCapabilities];
    if cfg!(feature = "wasix-runtime") {
        requests.extend([
            SandboxWasixRequestKind::RunModule,
            SandboxWasixRequestKind::RunToolchainCommand,
            SandboxWasixRequestKind::RunReplayFixture,
            SandboxWasixRequestKind::ValidateComponent,
        ]);
    }
    requests
}

fn platform_triple() -> String {
    format!(
        "{}-{}-{}",
        std::env::consts::ARCH,
        std::env::consts::OS,
        std::env::consts::FAMILY
    )
}

fn write_response(
    out: *mut SinomoWasixByteBuffer,
    response: NativeModuleResponse,
    success_status: SinomoWasixStatusCode,
) -> SinomoWasixStatusCode {
    match encode_sobj(&response) {
        Ok(bytes) => {
            if out.is_null() {
                SINOMO_WASIX_STATUS_INVALID_ARGUMENT
            } else {
                unsafe {
                    *out = SinomoWasixByteBuffer::from_vec(bytes);
                }
                success_status
            }
        }
        Err(error) => write_buffer(out, Err(format!("encode WASIX native response: {error}"))),
    }
}

fn write_buffer(
    out: *mut SinomoWasixByteBuffer,
    bytes: Result<Vec<u8>, impl std::fmt::Display>,
) -> SinomoWasixStatusCode {
    if out.is_null() {
        return SINOMO_WASIX_STATUS_INVALID_ARGUMENT;
    }
    match bytes {
        Ok(bytes) => {
            unsafe {
                *out = SinomoWasixByteBuffer::from_vec(bytes);
            }
            SINOMO_WASIX_STATUS_OK
        }
        Err(error) => {
            let fallback = format!("encode WASIX native response: {error}").into_bytes();
            unsafe {
                *out = SinomoWasixByteBuffer::from_vec(fallback);
            }
            SINOMO_WASIX_STATUS_RUNTIME_ERROR
        }
    }
}

fn error_response(
    code: &str,
    detail: &(impl std::fmt::Display + ?Sized),
) -> SandboxWasixNativeErrorResponse {
    SandboxWasixNativeErrorResponse {
        code: code.to_string(),
        detail: detail.to_string(),
    }
}

fn native_error_response(
    code: &str,
    detail: &(impl std::fmt::Display + ?Sized),
) -> NativeModuleErrorResponse {
    NativeModuleErrorResponse {
        code: code.to_string(),
        detail: detail.to_string(),
    }
}

#[cfg(feature = "wasix-runtime")]
fn usize_or_default(value: u64, default: usize) -> usize {
    if value == 0 {
        default
    } else {
        usize::try_from(value).unwrap_or(usize::MAX)
    }
}
