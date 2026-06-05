//! Windows Cloud Files provider module for exposing SIFS.
//!
//! The module is a trusted native dynamic library. It uses the generic Sinomo
//! native-module ABI and keeps Windows Cloud Files bindings out of the main
//! `sinomo` binary on non-Windows platforms.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Mutex;

use sinomo_dfs_cloud_files_api::{
    CloudFilesModuleError, CloudFilesModuleRequest, CloudFilesModuleResponse,
    SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION, SINOMO_DFS_CLOUD_FILES_MODULE_ID,
    SINOMO_DFS_CLOUD_FILES_MOUNT_OPERATION,
};
use sinomo_module_api::{
    NativeModuleBackend, NativeModuleCapabilitiesResponse, NativeModuleDescriptor,
    NativeModuleErrorResponse, NativeModuleInvocationRequest, NativeModuleInvocationResponse,
    NativeModuleRequest, NativeModuleResponse, SINOMO_MODULE_STATUS_INVALID_ARGUMENT,
    SINOMO_MODULE_STATUS_OK, SINOMO_MODULE_STATUS_RUNTIME_ERROR,
    SINOMO_MODULE_STATUS_UNSUPPORTED_ABI, SINOMO_NATIVE_MODULE_ABI_VERSION, SinomoModuleByteBuffer,
    SinomoModuleByteSlice, SinomoModuleHostApiV1, SinomoModuleStatusCode, decode_sobj, encode_sobj,
};

#[cfg(windows)]
mod windows_impl;

#[derive(Clone, Copy)]
#[cfg_attr(not(windows), allow(dead_code))]
struct StoredHostApi(SinomoModuleHostApiV1);

unsafe impl Send for StoredHostApi {}
unsafe impl Sync for StoredHostApi {}

static HOST_API: Mutex<Option<StoredHostApi>> = Mutex::new(None);

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_api_version() -> u32 {
    SINOMO_NATIVE_MODULE_ABI_VERSION
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_descriptor(
    out: *mut SinomoModuleByteBuffer,
) -> SinomoModuleStatusCode {
    write_buffer(out, encode_sobj(&cloud_files_module_descriptor()))
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_init(host: SinomoModuleHostApiV1) -> SinomoModuleStatusCode {
    if host.abi_version != SINOMO_NATIVE_MODULE_ABI_VERSION {
        return SINOMO_MODULE_STATUS_UNSUPPORTED_ABI;
    }
    match HOST_API.lock() {
        Ok(mut api) => {
            *api = Some(StoredHostApi(host));
            SINOMO_MODULE_STATUS_OK
        }
        Err(_) => SINOMO_MODULE_STATUS_RUNTIME_ERROR,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_invoke(
    request_sobj: SinomoModuleByteSlice,
    response_sobj: *mut SinomoModuleByteBuffer,
) -> SinomoModuleStatusCode {
    let result = catch_unwind(AssertUnwindSafe(|| {
        invoke_unchecked(request_sobj, response_sobj)
    }));
    match result {
        Ok(status) => status,
        Err(_) => write_response(
            response_sobj,
            NativeModuleResponse::Error(native_error_response(
                "panic",
                "Cloud Files module panicked while handling invocation",
            )),
            SINOMO_MODULE_STATUS_RUNTIME_ERROR,
        ),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_shutdown() -> SinomoModuleStatusCode {
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

pub fn cloud_files_module_descriptor() -> NativeModuleDescriptor {
    NativeModuleDescriptor {
        module_id: SINOMO_DFS_CLOUD_FILES_MODULE_ID.to_string(),
        module_version: env!("CARGO_PKG_VERSION").to_string(),
        native_abi_version: SINOMO_NATIVE_MODULE_ABI_VERSION,
        backend: NativeModuleBackend::NativeDylib,
        interface_version: SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION.to_string(),
        platform_triple: platform_triple(),
        supported_operations: vec![SINOMO_DFS_CLOUD_FILES_MOUNT_OPERATION.to_string()],
        capabilities: vec![
            "dfs.sifs.cloud-files.register-sync-root".to_string(),
            "dfs.sifs.cloud-files.placeholder-hydrate".to_string(),
            "dfs.sifs.read".to_string(),
        ],
    }
}

fn invoke_unchecked(
    request_sobj: SinomoModuleByteSlice,
    response_sobj: *mut SinomoModuleByteBuffer,
) -> SinomoModuleStatusCode {
    let response = match decode_native_module_request(request_sobj) {
        Ok(NativeModuleRequest::ProbeCapabilities(_)) => {
            NativeModuleResponse::Capabilities(NativeModuleCapabilitiesResponse {
                descriptor: cloud_files_module_descriptor(),
            })
        }
        Ok(NativeModuleRequest::Invoke(request)) => invoke_cloud_files_operation(request),
        Err(error) => NativeModuleResponse::Error(native_error_response(
            "invalid-request",
            &format!("decode Cloud Files module request: {error}"),
        )),
    };
    write_response(response_sobj, response, SINOMO_MODULE_STATUS_OK)
}

fn invoke_cloud_files_operation(request: NativeModuleInvocationRequest) -> NativeModuleResponse {
    if request.operation != SINOMO_DFS_CLOUD_FILES_MOUNT_OPERATION {
        return NativeModuleResponse::Error(native_error_response(
            "unsupported-operation",
            &format!(
                "unsupported Cloud Files module operation `{}`",
                request.operation
            ),
        ));
    }
    let cloud_files_request = match decode_sobj::<CloudFilesModuleRequest>(&request.payload_sobj) {
        Ok(request) => request,
        Err(error) => {
            return NativeModuleResponse::Error(native_error_response(
                "invalid-cloud-files-request",
                &format!("decode Cloud Files request payload: {error}"),
            ));
        }
    };
    let cloud_files_response = match cloud_files_request {
        CloudFilesModuleRequest::Mount(request) => match mount_sifs_cloud_files(request) {
            Ok(response) => CloudFilesModuleResponse::Mounted(response),
            Err(error) => CloudFilesModuleResponse::Error(error),
        },
    };
    match encode_sobj(&cloud_files_response) {
        Ok(payload_sobj) => NativeModuleResponse::Invocation(NativeModuleInvocationResponse {
            operation: SINOMO_DFS_CLOUD_FILES_MOUNT_OPERATION.to_string(),
            payload_sobj,
        }),
        Err(error) => NativeModuleResponse::Error(native_error_response(
            "encode-cloud-files-response",
            &format!("encode Cloud Files response payload: {error}"),
        )),
    }
}

#[cfg(windows)]
fn mount_sifs_cloud_files(
    request: sinomo_dfs_cloud_files_api::CloudFilesMountRequest,
) -> Result<sinomo_dfs_cloud_files_api::CloudFilesMountResponse, CloudFilesModuleError> {
    windows_impl::mount_sifs_cloud_files(request)
}

#[cfg(not(windows))]
fn mount_sifs_cloud_files(
    _request: sinomo_dfs_cloud_files_api::CloudFilesMountRequest,
) -> Result<sinomo_dfs_cloud_files_api::CloudFilesMountResponse, CloudFilesModuleError> {
    Err(CloudFilesModuleError {
        code: "unsupported-platform".to_string(),
        detail: "Windows Cloud Files support is only available in Windows builds".to_string(),
    })
}

fn decode_native_module_request(
    request_sobj: SinomoModuleByteSlice,
) -> Result<NativeModuleRequest, sinomo_module_api::NativeModuleApiError> {
    let request_bytes = unsafe { request_sobj.as_slice() };
    decode_sobj(request_bytes)
}

fn write_response(
    out: *mut SinomoModuleByteBuffer,
    response: NativeModuleResponse,
    fallback_status: SinomoModuleStatusCode,
) -> SinomoModuleStatusCode {
    match encode_sobj(&response) {
        Ok(bytes) => write_buffer(out, Ok(bytes)),
        Err(error) => write_buffer(out, Err(error)).max(fallback_status),
    }
}

fn write_buffer(
    out: *mut SinomoModuleByteBuffer,
    bytes: Result<Vec<u8>, sinomo_module_api::NativeModuleApiError>,
) -> SinomoModuleStatusCode {
    let Ok(bytes) = bytes else {
        return SINOMO_MODULE_STATUS_INVALID_ARGUMENT;
    };
    if out.is_null() {
        return SINOMO_MODULE_STATUS_INVALID_ARGUMENT;
    }
    unsafe {
        *out = SinomoModuleByteBuffer::from_vec(bytes);
    }
    SINOMO_MODULE_STATUS_OK
}

fn native_error_response(code: &str, detail: &str) -> NativeModuleErrorResponse {
    NativeModuleErrorResponse {
        code: code.to_string(),
        detail: detail.to_string(),
    }
}

#[cfg(windows)]
pub(crate) fn host_is_cancelled() -> bool {
    let Ok(api) = HOST_API.lock() else {
        return false;
    };
    let Some(api) = *api else {
        return false;
    };
    let Some(is_cancelled) = api.0.is_cancelled else {
        return false;
    };
    unsafe { is_cancelled(api.0.user_data) != 0 }
}

#[cfg(windows)]
pub(crate) fn host_log(message: &str) {
    let Ok(api) = HOST_API.lock() else {
        return;
    };
    let Some(api) = *api else {
        return;
    };
    let Some(log) = api.0.log else {
        return;
    };
    unsafe {
        let _ = log(
            api.0.user_data,
            SinomoModuleByteSlice::from_slice(message.as_bytes()),
        );
    }
}

fn platform_triple() -> String {
    format!(
        "{}-{}-{}",
        std::env::consts::ARCH,
        std::env::consts::OS,
        std::env::consts::FAMILY
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptor_exports_cloud_files_mount_operation() {
        let descriptor = cloud_files_module_descriptor();

        assert_eq!(descriptor.module_id, SINOMO_DFS_CLOUD_FILES_MODULE_ID);
        assert_eq!(
            descriptor.interface_version,
            SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION
        );
        assert!(
            descriptor
                .supported_operations
                .contains(&SINOMO_DFS_CLOUD_FILES_MOUNT_OPERATION.to_string())
        );
    }

    #[test]
    #[cfg(not(windows))]
    fn mount_returns_unsupported_on_non_windows() {
        let response = mount_sifs_cloud_files(
            sinomo_dfs_cloud_files_api::CloudFilesMountRequest::new("/tmp/sinomo-sifs"),
        );

        assert_eq!(
            response.expect_err("unsupported").code,
            "unsupported-platform"
        );
    }
}
