use std::panic::{AssertUnwindSafe, catch_unwind};
#[cfg(feature = "wasix-runtime")]
use std::sync::Arc;
use std::sync::Mutex;
#[cfg(feature = "wasix-runtime")]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(feature = "wasix-runtime")]
use std::thread;
#[cfg(feature = "wasix-runtime")]
use std::time::Duration;

#[cfg(feature = "wasix-runtime")]
use sinomo_module_api::SinomoModuleHostIsCancelledFn;
use sinomo_module_api::{
    ComponentDescriptor, NativeModuleBackend, NativeModuleCapabilitiesResponse,
    NativeModuleDescriptor, NativeModuleErrorResponse, NativeModuleInvocationResponse,
    NativeModuleLifecycle, NativeModuleRequest, NativeModuleResponse,
    SINOMO_MODULE_STATUS_INVALID_ARGUMENT, SINOMO_MODULE_STATUS_OK,
    SINOMO_MODULE_STATUS_RUNTIME_ERROR, SINOMO_MODULE_STATUS_UNSUPPORTED_ABI,
    SINOMO_NATIVE_MODULE_ABI_VERSION, ServiceOffer, SinomoModuleByteBuffer, SinomoModuleByteSlice,
    SinomoModuleHostApiV2, SinomoModuleStatusCode, decode_sobj, encode_sobj,
};
use sinomo_sandbox::{
    SANDBOX_EXECUTOR_INTERFACE_ID, SANDBOX_EXECUTOR_INTERFACE_VERSION, SANDBOX_EXECUTOR_OPERATION,
    SandboxExecutorCapabilitiesResponse, SandboxExecutorErrorResponse, SandboxExecutorRequest,
    SandboxExecutorRequestKind, SandboxExecutorResponse, SandboxExecutorRunModuleRequest,
    SandboxExecutorRunReplayFixtureRequest, SandboxExecutorRunToolchainCommandRequest,
    SandboxExecutorValidateComponentRequest,
};
#[cfg(feature = "wasix-runtime")]
use sinomo_sandbox::{
    SandboxExecutorRunModuleResponse, SandboxExecutorRunReplayFixtureResponse,
    SandboxExecutorValidateComponentResponse, encode_sandbox_executor_output,
};

#[cfg(feature = "wasix-runtime")]
use crate::{
    SandboxCancellation, SandboxLocalCommandOutcome, SandboxOutputChunk, SandboxOutputSink,
    SandboxWasiAbi, SandboxWasiPreopen, SandboxWasiRunOptions, SandboxWasixCommandOverride,
};

const SINOMO_SANDBOX_WASIX_MODULE_ID: &str = "module.sandbox.wasix";

#[derive(Clone, Copy)]
#[cfg_attr(not(feature = "wasix-runtime"), allow(dead_code))]
struct StoredHostApi(SinomoModuleHostApiV2);

unsafe impl Send for StoredHostApi {}
unsafe impl Sync for StoredHostApi {}

static HOST_API: Mutex<Option<StoredHostApi>> = Mutex::new(None);
static MODULE_LIFECYCLE: NativeModuleLifecycle = NativeModuleLifecycle::new();

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_api_version() -> u32 {
    SINOMO_NATIVE_MODULE_ABI_VERSION
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_descriptor(
    out: *mut SinomoModuleByteBuffer,
) -> SinomoModuleStatusCode {
    write_buffer(out, descriptor_response())
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_init(host: SinomoModuleHostApiV2) -> SinomoModuleStatusCode {
    if host.abi_version != SINOMO_NATIVE_MODULE_ABI_VERSION {
        return SINOMO_MODULE_STATUS_UNSUPPORTED_ABI;
    }
    if !MODULE_LIFECYCLE.initialize() {
        return SINOMO_MODULE_STATUS_RUNTIME_ERROR;
    }
    match HOST_API.lock() {
        Ok(mut api) => {
            *api = Some(StoredHostApi(host));
            SINOMO_MODULE_STATUS_OK
        }
        Err(_) => {
            let _ = MODULE_LIFECYCLE.begin_shutdown();
            SINOMO_MODULE_STATUS_RUNTIME_ERROR
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_invoke(
    request_sobj: SinomoModuleByteSlice,
    response_sobj: *mut SinomoModuleByteBuffer,
) -> SinomoModuleStatusCode {
    let Some(_invocation) = MODULE_LIFECYCLE.begin_invocation() else {
        return write_response(
            response_sobj,
            NativeModuleResponse::Error(native_error_response(
                "module-unavailable",
                "WASIX module is not initialized or is shutting down",
            )),
            SINOMO_MODULE_STATUS_RUNTIME_ERROR,
        );
    };
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
            SINOMO_MODULE_STATUS_RUNTIME_ERROR,
        ),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_shutdown() -> SinomoModuleStatusCode {
    if !MODULE_LIFECYCLE.begin_shutdown() {
        return SINOMO_MODULE_STATUS_RUNTIME_ERROR;
    }
    match HOST_API.lock() {
        Ok(mut api) => {
            *api = None;
            SINOMO_MODULE_STATUS_OK
        }
        Err(_) => SINOMO_MODULE_STATUS_RUNTIME_ERROR,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_free_buffer(buffer: SinomoModuleByteBuffer) {
    unsafe {
        drop(SinomoModuleByteBuffer::into_vec(buffer));
    }
}

unsafe fn invoke_unchecked(
    request_sobj: SinomoModuleByteSlice,
    response_sobj: *mut SinomoModuleByteBuffer,
) -> SinomoModuleStatusCode {
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
                SINOMO_MODULE_STATUS_INVALID_ARGUMENT,
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
    write_response(response_sobj, response, SINOMO_MODULE_STATUS_OK)
}

fn invoke_wasix_operation_response(
    request: sinomo_module_api::NativeModuleInvocationRequest,
) -> NativeModuleResponse {
    if request.operation != SANDBOX_EXECUTOR_OPERATION {
        return NativeModuleResponse::Error(native_error_response(
            "unsupported-operation",
            &format!(
                "unsupported native module operation `{}`",
                request.operation
            ),
        ));
    }
    let wasix_request = match decode_sobj::<SandboxExecutorRequest>(&request.payload_sobj) {
        Ok(request) => request,
        Err(error) => {
            return NativeModuleResponse::Error(native_error_response(
                "invalid-wasix-request",
                &format!("decode WASIX request payload: {error}"),
            ));
        }
    };
    let wasix_response = match wasix_request {
        SandboxExecutorRequest::ProbeCapabilities(_) => {
            SandboxExecutorResponse::Capabilities(SandboxExecutorCapabilitiesResponse {
                interface_id: SANDBOX_EXECUTOR_INTERFACE_ID.to_string(),
                interface_version: SANDBOX_EXECUTOR_INTERFACE_VERSION.to_string(),
                supported_requests: supported_executor_requests(),
                provider_capabilities: runtime_capabilities(),
            })
        }
        SandboxExecutorRequest::RunModule(request) => run_module_response(request),
        SandboxExecutorRequest::RunToolchainCommand(request) => {
            run_toolchain_command_response(request)
        }
        SandboxExecutorRequest::RunReplayFixture(request) => run_replay_fixture_response(request),
        SandboxExecutorRequest::ValidateComponent(request) => validate_component_response(request),
    };
    match encode_sobj(&wasix_response) {
        Ok(payload_sobj) => NativeModuleResponse::Invocation(NativeModuleInvocationResponse {
            operation: SANDBOX_EXECUTOR_OPERATION.to_string(),
            payload_sobj,
            response_body: None,
        }),
        Err(error) => NativeModuleResponse::Error(native_error_response(
            "encode-wasix-response",
            &format!("encode WASIX response payload: {error}"),
        )),
    }
}

#[cfg(feature = "wasix-runtime")]
fn run_module_response(request: SandboxExecutorRunModuleRequest) -> SandboxExecutorResponse {
    match run_resolved_module(
        request.target_abi,
        request.wasm,
        request.command,
        request.options,
    ) {
        Ok(outcome) => {
            SandboxExecutorResponse::RunModule(SandboxExecutorRunModuleResponse { outcome })
        }
        Err(error) => SandboxExecutorResponse::Error(error_response("run-module", &error)),
    }
}

#[cfg(not(feature = "wasix-runtime"))]
fn run_module_response(_request: SandboxExecutorRunModuleRequest) -> SandboxExecutorResponse {
    runtime_disabled_response("run-module")
}

#[cfg(feature = "wasix-runtime")]
fn run_toolchain_command_response(
    request: SandboxExecutorRunToolchainCommandRequest,
) -> SandboxExecutorResponse {
    match run_resolved_module(
        request.module.target_abi,
        request.module.wasm,
        request.command,
        request.options,
    ) {
        Ok(outcome) => {
            SandboxExecutorResponse::RunToolchainCommand(SandboxExecutorRunModuleResponse {
                outcome,
            })
        }
        Err(error) => SandboxExecutorResponse::Error(error_response("run-toolchain", &error)),
    }
}

#[cfg(not(feature = "wasix-runtime"))]
fn run_toolchain_command_response(
    _request: SandboxExecutorRunToolchainCommandRequest,
) -> SandboxExecutorResponse {
    runtime_disabled_response("run-toolchain")
}

#[cfg(feature = "wasix-runtime")]
fn run_replay_fixture_response(
    request: SandboxExecutorRunReplayFixtureRequest,
) -> SandboxExecutorResponse {
    match crate::run_sandbox_wasm_module_replay_fixture(&request.data_dir_root, &request.fixture) {
        Ok(report) => {
            SandboxExecutorResponse::RunReplayFixture(SandboxExecutorRunReplayFixtureResponse {
                report,
            })
        }
        Err(error) => SandboxExecutorResponse::Error(error_response("run-replay", &error)),
    }
}

#[cfg(not(feature = "wasix-runtime"))]
fn run_replay_fixture_response(
    _request: SandboxExecutorRunReplayFixtureRequest,
) -> SandboxExecutorResponse {
    runtime_disabled_response("run-replay")
}

#[cfg(feature = "wasix-runtime")]
fn validate_component_response(
    request: SandboxExecutorValidateComponentRequest,
) -> SandboxExecutorResponse {
    match crate::validate_sinomo_module_v1_component_contract(&request.wasm) {
        Ok(()) => SandboxExecutorResponse::ValidateComponent(
            SandboxExecutorValidateComponentResponse::default(),
        ),
        Err(error) => SandboxExecutorResponse::Error(error_response("validate-component", &error)),
    }
}

#[cfg(not(feature = "wasix-runtime"))]
fn validate_component_response(
    _request: SandboxExecutorValidateComponentRequest,
) -> SandboxExecutorResponse {
    runtime_disabled_response("validate-component")
}

#[cfg(not(feature = "wasix-runtime"))]
fn runtime_disabled_response(operation: &str) -> SandboxExecutorResponse {
    SandboxExecutorResponse::Error(error_response(
        operation,
        "WASIX native module was built without the `wasix-runtime` feature",
    ))
}

#[cfg(feature = "wasix-runtime")]
fn run_resolved_module(
    target_abi: SandboxWasiAbi,
    wasm: Vec<u8>,
    command: crate::SandboxCommandRequest,
    options: sinomo_sandbox::SandboxExecutorOptions,
) -> Result<SandboxLocalCommandOutcome, String> {
    let runner =
        crate::LocalStagedSandboxCommandRunner::new().map_err(|error| error.to_string())?;
    let cancellation = HostCancellationBridge::start();
    runner
        .run_module_for_abi(
            target_abi,
            &wasm,
            command,
            convert_run_options(options, cancellation.token()),
        )
        .map_err(|error| error.to_string())
}

#[cfg(feature = "wasix-runtime")]
fn convert_run_options(
    options: sinomo_sandbox::SandboxExecutorOptions,
    cancellation: SandboxCancellation,
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
        cancellation,
        output_sink: host_output_sink(),
        wasix_pinned_child_exec_only: options.pinned_child_exec_only,
        wasix_instruction_metering: options.instruction_metering,
        wasix_auto_asyncify: options.auto_asyncify,
        wasix_fpcast_emu: options.fpcast_emulation,
        wasix_command_overrides: options
            .command_overrides
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
struct HostCancellationBridge {
    cancellation: SandboxCancellation,
    done: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

#[cfg(feature = "wasix-runtime")]
impl HostCancellationBridge {
    fn start() -> Self {
        let cancellation = SandboxCancellation::new();
        let done = Arc::new(AtomicBool::new(false));
        let callback = host_cancellation_callback();
        if std::env::var_os("SINOMO_WASIX_DEBUG").is_some() {
            eprintln!(
                "sinomo-debug wasix native cancellation bridge callback={}",
                callback.is_some()
            );
        }
        let handle = callback.map(|(is_cancelled, user_data)| {
            let done = Arc::clone(&done);
            let cancellation = cancellation.clone();
            thread::spawn(move || {
                while !done.load(Ordering::SeqCst) {
                    if unsafe { is_cancelled(user_data as *mut std::ffi::c_void) } != 0 {
                        if std::env::var_os("SINOMO_WASIX_DEBUG").is_some() {
                            eprintln!("sinomo-debug wasix native bridge observed cancellation");
                        }
                        cancellation.cancel();
                        return;
                    }
                    thread::sleep(Duration::from_millis(10));
                }
            })
        });
        Self {
            cancellation,
            done,
            handle,
        }
    }

    fn token(&self) -> SandboxCancellation {
        self.cancellation.clone()
    }
}

#[cfg(feature = "wasix-runtime")]
impl Drop for HostCancellationBridge {
    fn drop(&mut self) {
        self.done.store(true, Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(feature = "wasix-runtime")]
fn host_cancellation_callback() -> Option<(SinomoModuleHostIsCancelledFn, usize)> {
    let api = HOST_API.lock().ok().and_then(|api| *api)?;
    api.0
        .is_cancelled
        .map(|is_cancelled| (is_cancelled, api.0.user_data as usize))
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
        let Ok(bytes) = encode_sandbox_executor_output(&chunk) else {
            return;
        };
        unsafe {
            let _ = output_chunk(
                self.api.0.user_data,
                SinomoModuleByteSlice::from_slice(&bytes),
            );
        }
    }
}

fn descriptor_response() -> Result<Vec<u8>, sinomo_module_api::NativeModuleApiError> {
    encode_sobj(&descriptor())
}

fn descriptor() -> NativeModuleDescriptor {
    NativeModuleDescriptor {
        module_id: SINOMO_SANDBOX_WASIX_MODULE_ID.to_string(),
        module_version: env!("CARGO_PKG_VERSION").to_string(),
        owner: "sinomo-sandbox-wasix".to_string(),
        native_abi_version: SINOMO_NATIVE_MODULE_ABI_VERSION,
        backend: NativeModuleBackend::NativeDylib,
        interface_version: SANDBOX_EXECUTOR_INTERFACE_VERSION.to_string(),
        platform_triple: platform_triple(),
        supported_operations: vec![SANDBOX_EXECUTOR_OPERATION.to_string()],
        capabilities: runtime_capabilities(),
        surfaces: Default::default(),
        component: ComponentDescriptor::new(
            SINOMO_SANDBOX_WASIX_MODULE_ID,
            env!("CARGO_PKG_VERSION"),
        )
        .with_display_name("WASIX Sandbox")
        .with_offer(
            ServiceOffer::new(
                "sandbox-executor",
                SANDBOX_EXECUTOR_INTERFACE_ID,
                SANDBOX_EXECUTOR_INTERFACE_VERSION,
            )
            .with_provider_capability("sandbox.wasm.execute"),
        ),
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

fn supported_executor_requests() -> Vec<SandboxExecutorRequestKind> {
    let mut requests = vec![SandboxExecutorRequestKind::ProbeCapabilities];
    if cfg!(feature = "wasix-runtime") {
        requests.extend([
            SandboxExecutorRequestKind::RunModule,
            SandboxExecutorRequestKind::RunToolchainCommand,
            SandboxExecutorRequestKind::RunReplayFixture,
            SandboxExecutorRequestKind::ValidateComponent,
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
    out: *mut SinomoModuleByteBuffer,
    response: NativeModuleResponse,
    success_status: SinomoModuleStatusCode,
) -> SinomoModuleStatusCode {
    match encode_sobj(&response) {
        Ok(bytes) => {
            if out.is_null() {
                SINOMO_MODULE_STATUS_INVALID_ARGUMENT
            } else {
                unsafe {
                    *out = SinomoModuleByteBuffer::from_vec(bytes);
                }
                success_status
            }
        }
        Err(error) => write_buffer(out, Err(format!("encode WASIX native response: {error}"))),
    }
}

fn write_buffer(
    out: *mut SinomoModuleByteBuffer,
    bytes: Result<Vec<u8>, impl std::fmt::Display>,
) -> SinomoModuleStatusCode {
    if out.is_null() {
        return SINOMO_MODULE_STATUS_INVALID_ARGUMENT;
    }
    match bytes {
        Ok(bytes) => {
            unsafe {
                *out = SinomoModuleByteBuffer::from_vec(bytes);
            }
            SINOMO_MODULE_STATUS_OK
        }
        Err(error) => {
            let fallback = format!("encode WASIX native response: {error}").into_bytes();
            unsafe {
                *out = SinomoModuleByteBuffer::from_vec(fallback);
            }
            SINOMO_MODULE_STATUS_RUNTIME_ERROR
        }
    }
}

fn error_response(
    code: &str,
    detail: &(impl std::fmt::Display + ?Sized),
) -> SandboxExecutorErrorResponse {
    SandboxExecutorErrorResponse {
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

#[cfg(all(test, feature = "wasix-runtime"))]
mod tests {
    use super::*;

    static MODULE_ABI_TEST_LOCK: Mutex<()> = Mutex::new(());

    unsafe extern "C" fn test_is_cancelled(user_data: *mut std::ffi::c_void) -> u8 {
        let Some(cancelled) = (unsafe { (user_data as *const AtomicBool).as_ref() }) else {
            return 0;
        };
        cancelled.load(Ordering::SeqCst) as u8
    }

    struct HostApiRestore(Option<StoredHostApi>);

    impl Drop for HostApiRestore {
        fn drop(&mut self) {
            if let Ok(mut api) = HOST_API.lock() {
                *api = self.0.take();
            }
        }
    }

    fn install_test_host_api(cancelled: &AtomicBool) -> HostApiRestore {
        let mut api = HOST_API.lock().expect("host api lock");
        let previous = api.replace(StoredHostApi(SinomoModuleHostApiV2 {
            abi_version: SINOMO_NATIVE_MODULE_ABI_VERSION,
            user_data: cancelled as *const AtomicBool as *mut std::ffi::c_void,
            output_chunk: None,
            log: None,
            is_cancelled: Some(test_is_cancelled),
            local_permission_granted: None,
        }));
        HostApiRestore(previous)
    }

    #[test]
    fn host_cancellation_bridge_flips_module_token() {
        let _guard = MODULE_ABI_TEST_LOCK.lock().expect("ABI test lock");
        let cancelled = AtomicBool::new(false);
        let _restore = install_test_host_api(&cancelled);
        let bridge = HostCancellationBridge::start();
        let token = bridge.token();

        assert!(!token.is_cancelled());
        cancelled.store(true, Ordering::SeqCst);
        for _ in 0..100 {
            if token.is_cancelled() {
                break;
            }
            thread::sleep(Duration::from_millis(5));
        }

        assert!(token.is_cancelled());
    }

    #[test]
    fn module_shutdown_is_idempotent_and_reinitializable() {
        let _guard = MODULE_ABI_TEST_LOCK.lock().expect("ABI test lock");
        assert_eq!(sinomo_module_shutdown(), SINOMO_MODULE_STATUS_OK);
        assert_eq!(
            sinomo_module_init(SinomoModuleHostApiV2::empty()),
            SINOMO_MODULE_STATUS_OK
        );
        assert_eq!(sinomo_module_shutdown(), SINOMO_MODULE_STATUS_OK);
        assert_eq!(sinomo_module_shutdown(), SINOMO_MODULE_STATUS_OK);
        let mut response = SinomoModuleByteBuffer::null();
        assert_eq!(
            sinomo_module_invoke(SinomoModuleByteSlice::from_slice(&[]), &mut response),
            SINOMO_MODULE_STATUS_RUNTIME_ERROR
        );
        let bytes = unsafe { SinomoModuleByteBuffer::into_vec(response) };
        assert!(matches!(
            decode_sobj::<NativeModuleResponse>(&bytes).expect("rejected response"),
            NativeModuleResponse::Error(NativeModuleErrorResponse { ref code, .. })
                if code == "module-unavailable"
        ));
        assert_eq!(
            sinomo_module_init(SinomoModuleHostApiV2::empty()),
            SINOMO_MODULE_STATUS_OK
        );
        assert_eq!(sinomo_module_shutdown(), SINOMO_MODULE_STATUS_OK);
    }
}
