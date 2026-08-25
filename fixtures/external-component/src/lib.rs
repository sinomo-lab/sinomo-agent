use sinomo_module_api::{
    ComponentDescriptor, NativeModuleBackend, NativeModuleCapabilitiesResponse,
    NativeModuleDescriptor, NativeModuleErrorResponse, NativeModuleInvocationResponse,
    NativeModuleRequest, NativeModuleResponse, NativeModuleSurfaces,
    SINOMO_MODULE_STATUS_INVALID_ARGUMENT, SINOMO_MODULE_STATUS_OK,
    SINOMO_MODULE_STATUS_RUNTIME_ERROR, SINOMO_MODULE_STATUS_UNSUPPORTED_ABI,
    SINOMO_NATIVE_MODULE_ABI_VERSION, ServiceOffer, SinomoModuleByteBuffer, SinomoModuleByteSlice,
    SinomoModuleHostApiV2, SinomoModuleStatusCode, decode_sobj, encode_sobj,
};

const MODULE_ID: &str = "fixture.external.echo";
const INTERFACE_ID: &str = "sinomo.fixture.external-echo";
const INTERFACE_VERSION: &str = "1.0.0";
const OPERATION: &str = "fixture.external.echo";

fn descriptor() -> NativeModuleDescriptor {
    NativeModuleDescriptor {
        module_id: MODULE_ID.to_string(),
        module_version: env!("CARGO_PKG_VERSION").to_string(),
        owner: "external-fixture".to_string(),
        native_abi_version: SINOMO_NATIVE_MODULE_ABI_VERSION,
        backend: NativeModuleBackend::NativeDylib,
        interface_version: INTERFACE_VERSION.to_string(),
        platform_triple: format!(
            "{}-{}-{}",
            std::env::consts::ARCH,
            std::env::consts::OS,
            std::env::consts::FAMILY
        ),
        supported_operations: vec![OPERATION.to_string()],
        capabilities: Vec::new(),
        surfaces: NativeModuleSurfaces::default(),
        component: ComponentDescriptor::new(MODULE_ID, env!("CARGO_PKG_VERSION")).with_offer(
            ServiceOffer::new("external-echo", INTERFACE_ID, INTERFACE_VERSION)
                .with_provider_capability("echo.bytes"),
        ),
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
    write_buffer(out, encode_sobj(&descriptor()))
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_init(host: SinomoModuleHostApiV2) -> SinomoModuleStatusCode {
    if host.abi_version == SINOMO_NATIVE_MODULE_ABI_VERSION {
        SINOMO_MODULE_STATUS_OK
    } else {
        SINOMO_MODULE_STATUS_UNSUPPORTED_ABI
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_invoke(
    request_sobj: SinomoModuleByteSlice,
    response_sobj: *mut SinomoModuleByteBuffer,
) -> SinomoModuleStatusCode {
    let request = unsafe { request_sobj.as_slice() };
    let response = match decode_sobj::<NativeModuleRequest>(request) {
        Ok(NativeModuleRequest::ProbeCapabilities(_)) => {
            NativeModuleResponse::Capabilities(NativeModuleCapabilitiesResponse {
                descriptor: descriptor(),
            })
        }
        Ok(NativeModuleRequest::Invoke(request)) if request.operation == OPERATION => {
            NativeModuleResponse::Invocation(NativeModuleInvocationResponse {
                operation: request.operation,
                payload_sobj: request.payload_sobj,
                response_body: None,
            })
        }
        Ok(NativeModuleRequest::Invoke(request)) => {
            NativeModuleResponse::Error(NativeModuleErrorResponse {
                code: "unsupported-operation".to_string(),
                detail: format!("unsupported operation `{}`", request.operation),
            })
        }
        Err(error) => NativeModuleResponse::Error(NativeModuleErrorResponse {
            code: "invalid-request".to_string(),
            detail: error.to_string(),
        }),
    };
    match encode_sobj(&response) {
        Ok(bytes) => write_buffer(response_sobj, Ok(bytes)),
        Err(_) => SINOMO_MODULE_STATUS_RUNTIME_ERROR,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_shutdown() -> SinomoModuleStatusCode {
    SINOMO_MODULE_STATUS_OK
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_free_buffer(buffer: SinomoModuleByteBuffer) {
    unsafe {
        drop(SinomoModuleByteBuffer::into_vec(buffer));
    }
}

fn write_buffer(
    out: *mut SinomoModuleByteBuffer,
    bytes: Result<Vec<u8>, sinomo_module_api::NativeModuleApiError>,
) -> SinomoModuleStatusCode {
    let Ok(bytes) = bytes else {
        return SINOMO_MODULE_STATUS_RUNTIME_ERROR;
    };
    if out.is_null() {
        return SINOMO_MODULE_STATUS_INVALID_ARGUMENT;
    }
    unsafe {
        *out = SinomoModuleByteBuffer::from_vec(bytes);
    }
    SINOMO_MODULE_STATUS_OK
}
