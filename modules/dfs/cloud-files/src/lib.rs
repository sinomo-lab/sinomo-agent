//! Windows Cloud Files provider module for exposing SIFS.
//!
//! The module is a trusted native dynamic library. It uses the generic Sinomo
//! native-module ABI and keeps Windows Cloud Files bindings out of the main
//! `sinomo` binary on non-Windows platforms.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Mutex;

#[cfg(windows)]
use sinomo_dfs_cloud_files_api::CloudFilesModuleEvent;
#[cfg(any(windows, test))]
use sinomo_dfs_cloud_files_api::{
    CLOUD_FILES_PLACEHOLDER_IDENTITY_MAX_BYTES, CLOUD_FILES_PLACEHOLDER_IDENTITY_SCHEMA_VERSION,
    CloudFilesPlaceholderIdentity, CloudFilesStaleIdentityReason,
};
use sinomo_dfs_cloud_files_api::{
    CloudFilesModuleError, CloudFilesModuleRequest, CloudFilesModuleResponse,
    SINOMO_DFS_CLOUD_FILES_CURSOR_ONLY_RECOVERY_APPLY_CAPABILITY,
    SINOMO_DFS_CLOUD_FILES_CURSOR_ONLY_RECOVERY_APPLY_OPERATION,
    SINOMO_DFS_CLOUD_FILES_CURSOR_ONLY_RECOVERY_INSPECT_CAPABILITY,
    SINOMO_DFS_CLOUD_FILES_CURSOR_ONLY_RECOVERY_INSPECT_OPERATION,
    SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION, SINOMO_DFS_CLOUD_FILES_MODULE_ID,
    SINOMO_DFS_CLOUD_FILES_MOUNT_OPERATION,
    SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_CAPABILITY,
    SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_OPERATION,
    SINOMO_DFS_CLOUD_FILES_UNREGISTER_OPERATION,
};
use sinomo_module_api::{
    NativeModuleBackend, NativeModuleCapabilitiesResponse, NativeModuleDescriptor,
    NativeModuleErrorResponse, NativeModuleInvocationRequest, NativeModuleInvocationResponse,
    NativeModuleRequest, NativeModuleResponse, SINOMO_MODULE_STATUS_INVALID_ARGUMENT,
    SINOMO_MODULE_STATUS_OK, SINOMO_MODULE_STATUS_RUNTIME_ERROR,
    SINOMO_MODULE_STATUS_UNSUPPORTED_ABI, SINOMO_NATIVE_MODULE_ABI_VERSION, SinomoModuleByteBuffer,
    SinomoModuleByteSlice, SinomoModuleHostApiV2, SinomoModuleStatusCode, decode_sobj, encode_sobj,
};

pub mod local_change_queue;
pub mod namespace_reconciler;
#[cfg(any(windows, test))]
mod registration_journal;
pub mod sync_executor;
#[cfg(windows)]
mod windows_impl;
pub mod windows_sync;

#[cfg(any(windows, test))]
const CLOUD_FILES_PLACEHOLDER_IDENTITY_MAGIC: &[u8] = b"SCFI\0\x01";

#[cfg(any(windows, test))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DecodedCloudFilesPlaceholderIdentity {
    Versioned(CloudFilesPlaceholderIdentity),
    LegacyPath(String),
}

#[cfg(any(windows, test))]
pub(crate) fn encode_placeholder_identity(
    identity: &CloudFilesPlaceholderIdentity,
) -> Result<Vec<u8>, CloudFilesModuleError> {
    validate_placeholder_identity(identity)?;
    let payload = encode_sobj(identity).map_err(|source| CloudFilesModuleError {
        code: "encode-placeholder-identity".to_string(),
        detail: format!("encode versioned Cloud Files placeholder identity: {source}"),
    })?;
    let length = CLOUD_FILES_PLACEHOLDER_IDENTITY_MAGIC
        .len()
        .saturating_add(payload.len());
    if length > CLOUD_FILES_PLACEHOLDER_IDENTITY_MAX_BYTES {
        return Err(CloudFilesModuleError {
            code: "placeholder-identity-too-large".to_string(),
            detail: format!(
                "encoded Cloud Files placeholder identity is {length} bytes; max is {CLOUD_FILES_PLACEHOLDER_IDENTITY_MAX_BYTES}"
            ),
        });
    }
    let mut encoded = Vec::with_capacity(length);
    encoded.extend_from_slice(CLOUD_FILES_PLACEHOLDER_IDENTITY_MAGIC);
    encoded.extend_from_slice(&payload);
    Ok(encoded)
}

#[cfg(any(windows, test))]
pub(crate) fn decode_placeholder_identity(
    bytes: &[u8],
) -> Result<DecodedCloudFilesPlaceholderIdentity, CloudFilesModuleError> {
    if bytes.len() > CLOUD_FILES_PLACEHOLDER_IDENTITY_MAX_BYTES {
        return Err(CloudFilesModuleError {
            code: "placeholder-identity-too-large".to_string(),
            detail: format!(
                "Cloud Files placeholder identity is {} bytes; max is {CLOUD_FILES_PLACEHOLDER_IDENTITY_MAX_BYTES}",
                bytes.len()
            ),
        });
    }
    let Some(payload) = bytes.strip_prefix(CLOUD_FILES_PLACEHOLDER_IDENTITY_MAGIC) else {
        let path = std::str::from_utf8(bytes).map_err(|source| CloudFilesModuleError {
            code: "invalid-legacy-placeholder-identity".to_string(),
            detail: format!("legacy Cloud Files placeholder path is not UTF-8: {source}"),
        })?;
        if path.is_empty() || !path.starts_with('/') {
            return Err(CloudFilesModuleError {
                code: "invalid-legacy-placeholder-identity".to_string(),
                detail: "legacy Cloud Files placeholder identity must be an absolute SIFS path"
                    .to_string(),
            });
        }
        return Ok(DecodedCloudFilesPlaceholderIdentity::LegacyPath(
            path.to_string(),
        ));
    };
    let identity = decode_sobj::<CloudFilesPlaceholderIdentity>(payload).map_err(|source| {
        CloudFilesModuleError {
            code: "decode-placeholder-identity".to_string(),
            detail: format!("decode versioned Cloud Files placeholder identity: {source}"),
        }
    })?;
    validate_placeholder_identity(&identity)?;
    Ok(DecodedCloudFilesPlaceholderIdentity::Versioned(identity))
}

#[cfg(any(windows, test))]
fn validate_placeholder_identity(
    identity: &CloudFilesPlaceholderIdentity,
) -> Result<(), CloudFilesModuleError> {
    if identity.schema_version != CLOUD_FILES_PLACEHOLDER_IDENTITY_SCHEMA_VERSION {
        return Err(CloudFilesModuleError {
            code: "unsupported-placeholder-identity".to_string(),
            detail: format!(
                "Cloud Files placeholder identity schema {} is unsupported",
                identity.schema_version
            ),
        });
    }
    if identity.mount.trim().is_empty()
        || identity.root.is_empty()
        || identity.path.is_empty()
        || identity.object_id.trim().is_empty()
        || identity.version.trim().is_empty()
        || !identity.root.starts_with('/')
        || !identity.path.starts_with('/')
        || identity.mount.contains('\0')
        || identity.root.contains('\0')
        || identity.path.contains('\0')
        || identity.object_id.contains('\0')
        || identity.version.contains('\0')
        || identity
            .content_hash
            .as_deref()
            .is_some_and(|hash| hash.trim().is_empty() || hash.contains('\0'))
    {
        return Err(CloudFilesModuleError {
            code: "invalid-placeholder-identity".to_string(),
            detail:
                "versioned Cloud Files identity has an empty, relative, or NUL-containing field"
                    .to_string(),
        });
    }
    if identity.root != "/"
        && identity.path != identity.root
        && !identity
            .path
            .starts_with(&format!("{}/", identity.root.trim_end_matches('/')))
    {
        return Err(CloudFilesModuleError {
            code: "placeholder-path-outside-root".to_string(),
            detail: format!(
                "placeholder path `{}` is outside mounted SIFS root `{}`",
                identity.path, identity.root
            ),
        });
    }
    Ok(())
}

#[cfg(any(windows, test))]
pub(crate) fn placeholder_identity_mismatch(
    expected: &CloudFilesPlaceholderIdentity,
    observed: &CloudFilesPlaceholderIdentity,
) -> Option<CloudFilesStaleIdentityReason> {
    if observed.mount != expected.mount {
        Some(CloudFilesStaleIdentityReason::MountChanged)
    } else if observed.root != expected.root {
        Some(CloudFilesStaleIdentityReason::RootChanged)
    } else if observed.path != expected.path {
        Some(CloudFilesStaleIdentityReason::PathChanged)
    } else if observed.object_id != expected.object_id {
        Some(CloudFilesStaleIdentityReason::ObjectChanged)
    } else if observed.version != expected.version {
        Some(CloudFilesStaleIdentityReason::VersionChanged)
    } else if observed.content_hash != expected.content_hash {
        Some(CloudFilesStaleIdentityReason::ContentChanged)
    } else {
        None
    }
}

#[derive(Clone, Copy)]
#[cfg_attr(not(windows), allow(dead_code))]
struct StoredHostApi(SinomoModuleHostApiV2);

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
pub extern "C" fn sinomo_module_init(host: SinomoModuleHostApiV2) -> SinomoModuleStatusCode {
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
        owner: "sinomo-dfs-cloud-files".to_string(),
        native_abi_version: SINOMO_NATIVE_MODULE_ABI_VERSION,
        backend: NativeModuleBackend::NativeDylib,
        interface_version: SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION.to_string(),
        platform_triple: platform_triple(),
        supported_operations: vec![
            SINOMO_DFS_CLOUD_FILES_MOUNT_OPERATION.to_string(),
            SINOMO_DFS_CLOUD_FILES_UNREGISTER_OPERATION.to_string(),
            SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_OPERATION.to_string(),
            SINOMO_DFS_CLOUD_FILES_CURSOR_ONLY_RECOVERY_INSPECT_OPERATION.to_string(),
            SINOMO_DFS_CLOUD_FILES_CURSOR_ONLY_RECOVERY_APPLY_OPERATION.to_string(),
        ],
        capabilities: vec![
            "dfs.sifs.cloud-files.unregister-sync-root".to_string(),
            SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_CAPABILITY.to_string(),
            SINOMO_DFS_CLOUD_FILES_CURSOR_ONLY_RECOVERY_INSPECT_CAPABILITY.to_string(),
            SINOMO_DFS_CLOUD_FILES_CURSOR_ONLY_RECOVERY_APPLY_CAPABILITY.to_string(),
            "dfs.sifs.cloud-files.lifecycle-status".to_string(),
            "dfs.sifs.cloud-files.writable-sync-v2".to_string(),
        ],
        surfaces: Default::default(),
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
    if request.operation != SINOMO_DFS_CLOUD_FILES_MOUNT_OPERATION
        && request.operation != SINOMO_DFS_CLOUD_FILES_UNREGISTER_OPERATION
        && request.operation != SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_OPERATION
        && request.operation != SINOMO_DFS_CLOUD_FILES_CURSOR_ONLY_RECOVERY_INSPECT_OPERATION
        && request.operation != SINOMO_DFS_CLOUD_FILES_CURSOR_ONLY_RECOVERY_APPLY_OPERATION
    {
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
    let cloud_files_response = match (request.operation.as_str(), cloud_files_request) {
        (SINOMO_DFS_CLOUD_FILES_MOUNT_OPERATION, CloudFilesModuleRequest::Mount(request)) => {
            match mount_sifs_cloud_files(request) {
                Ok(response) => CloudFilesModuleResponse::Mounted(response),
                Err(error) => CloudFilesModuleResponse::Error(error),
            }
        }
        (
            SINOMO_DFS_CLOUD_FILES_UNREGISTER_OPERATION,
            CloudFilesModuleRequest::Unregister(request),
        ) => match unregister_sifs_cloud_files(request) {
            Ok(response) => CloudFilesModuleResponse::Unregistered(response),
            Err(error) => CloudFilesModuleResponse::Error(error),
        },
        (
            SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_OPERATION,
            CloudFilesModuleRequest::PreserveStateUnregister(request),
        ) => match unregister_sifs_cloud_files_preserving_state(request) {
            Ok(response) => CloudFilesModuleResponse::StatePreservingUnregistered(response),
            Err(error) => CloudFilesModuleResponse::Error(error),
        },
        (
            SINOMO_DFS_CLOUD_FILES_CURSOR_ONLY_RECOVERY_INSPECT_OPERATION,
            CloudFilesModuleRequest::InspectCursorOnlyRecovery(request),
        ) => match inspect_cursor_only_recovery(request) {
            Ok(response) => CloudFilesModuleResponse::CursorOnlyRecoveryInspected(response),
            Err(error) => CloudFilesModuleResponse::Error(error),
        },
        (
            SINOMO_DFS_CLOUD_FILES_CURSOR_ONLY_RECOVERY_APPLY_OPERATION,
            CloudFilesModuleRequest::ApplyCursorOnlyRecovery(request),
        ) => match apply_cursor_only_recovery(request) {
            Ok(response) => CloudFilesModuleResponse::CursorOnlyRecoveryApplied(response),
            Err(error) => CloudFilesModuleResponse::Error(error),
        },
        _ => CloudFilesModuleResponse::Error(CloudFilesModuleError {
            code: "operation-payload-mismatch".to_string(),
            detail: format!(
                "Cloud Files operation `{}` does not match its typed payload",
                request.operation
            ),
        }),
    };
    match encode_sobj(&cloud_files_response) {
        Ok(payload_sobj) => NativeModuleResponse::Invocation(NativeModuleInvocationResponse {
            operation: request.operation,
            payload_sobj,
            response_body: None,
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

#[cfg(windows)]
fn unregister_sifs_cloud_files(
    request: sinomo_dfs_cloud_files_api::CloudFilesUnregisterRequest,
) -> Result<sinomo_dfs_cloud_files_api::CloudFilesUnregisterResponse, CloudFilesModuleError> {
    windows_impl::unregister_sifs_cloud_files(request)
}

#[cfg(windows)]
fn unregister_sifs_cloud_files_preserving_state(
    request: sinomo_dfs_cloud_files_api::CloudFilesUnregisterRequest,
) -> Result<
    sinomo_dfs_cloud_files_api::CloudFilesPreserveStateUnregisterResponse,
    CloudFilesModuleError,
> {
    windows_impl::unregister_sifs_cloud_files_preserving_state(request)
}

#[cfg(windows)]
fn inspect_cursor_only_recovery(
    request: sinomo_dfs_cloud_files_api::CloudFilesCursorOnlyRecoveryInspectRequest,
) -> Result<sinomo_dfs_cloud_files_api::CloudFilesCursorOnlyRecoveryInspection, CloudFilesModuleError>
{
    windows_impl::inspect_cursor_only_recovery(request)
}

#[cfg(windows)]
fn apply_cursor_only_recovery(
    request: sinomo_dfs_cloud_files_api::CloudFilesCursorOnlyRecoveryApplyRequest,
) -> Result<sinomo_dfs_cloud_files_api::CloudFilesCursorOnlyRecoveryApplied, CloudFilesModuleError>
{
    windows_impl::apply_cursor_only_recovery(request)
}

#[cfg(not(windows))]
fn mount_sifs_cloud_files(
    request: sinomo_dfs_cloud_files_api::CloudFilesMountRequest,
) -> Result<sinomo_dfs_cloud_files_api::CloudFilesMountResponse, CloudFilesModuleError> {
    request.validate_supported_mode()?;
    Err(CloudFilesModuleError {
        code: "unsupported-platform".to_string(),
        detail: "Windows Cloud Files support is only available in Windows builds".to_string(),
    })
}

#[cfg(not(windows))]
fn unregister_sifs_cloud_files(
    _request: sinomo_dfs_cloud_files_api::CloudFilesUnregisterRequest,
) -> Result<sinomo_dfs_cloud_files_api::CloudFilesUnregisterResponse, CloudFilesModuleError> {
    Err(CloudFilesModuleError {
        code: "unsupported-platform".to_string(),
        detail: "Windows Cloud Files support is only available in Windows builds".to_string(),
    })
}

#[cfg(not(windows))]
fn unregister_sifs_cloud_files_preserving_state(
    _request: sinomo_dfs_cloud_files_api::CloudFilesUnregisterRequest,
) -> Result<
    sinomo_dfs_cloud_files_api::CloudFilesPreserveStateUnregisterResponse,
    CloudFilesModuleError,
> {
    Err(CloudFilesModuleError {
        code: "unsupported-platform".to_string(),
        detail: "Windows Cloud Files support is only available in Windows builds".to_string(),
    })
}

#[cfg(not(windows))]
fn inspect_cursor_only_recovery(
    _request: sinomo_dfs_cloud_files_api::CloudFilesCursorOnlyRecoveryInspectRequest,
) -> Result<sinomo_dfs_cloud_files_api::CloudFilesCursorOnlyRecoveryInspection, CloudFilesModuleError>
{
    Err(CloudFilesModuleError {
        code: "unsupported-platform".to_string(),
        detail: "Windows Cloud Files cursor recovery is only available in Windows builds"
            .to_string(),
    })
}

#[cfg(not(windows))]
fn apply_cursor_only_recovery(
    _request: sinomo_dfs_cloud_files_api::CloudFilesCursorOnlyRecoveryApplyRequest,
) -> Result<sinomo_dfs_cloud_files_api::CloudFilesCursorOnlyRecoveryApplied, CloudFilesModuleError>
{
    Err(CloudFilesModuleError {
        code: "unsupported-platform".to_string(),
        detail: "Windows Cloud Files cursor recovery is only available in Windows builds"
            .to_string(),
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

#[cfg(windows)]
pub(crate) fn host_emit_event(event: &CloudFilesModuleEvent) {
    let payload_sobj = match encode_sobj(event) {
        Ok(payload_sobj) => payload_sobj,
        Err(error) => {
            host_log(&format!(
                "encode Cloud Files lifecycle event for host output: {error}"
            ));
            return;
        }
    };
    let api = match HOST_API.lock() {
        Ok(api) => *api,
        Err(_) => return,
    };
    let Some(api) = api else {
        return;
    };
    let Some(output_chunk) = api.0.output_chunk else {
        return;
    };
    let status = unsafe {
        output_chunk(
            api.0.user_data,
            SinomoModuleByteSlice::from_slice(&payload_sobj),
        )
    };
    if status != SINOMO_MODULE_STATUS_OK {
        host_log(&format!(
            "Cloud Files lifecycle event host output returned status {status}"
        ));
    }
}

#[cfg(windows)]
pub(crate) fn host_emit_event_ack(event: &CloudFilesModuleEvent) -> Result<(), String> {
    let payload_sobj = encode_sobj(event)
        .map_err(|error| format!("encode acknowledged Cloud Files host event: {error}"))?;
    let api = HOST_API
        .lock()
        .map_err(|_| "Cloud Files host API lock is poisoned".to_string())?
        .ok_or_else(|| "Cloud Files host API is unavailable".to_string())?;
    let output_chunk = api
        .0
        .output_chunk
        .ok_or_else(|| "Cloud Files host has no acknowledged output callback".to_string())?;
    let status = unsafe {
        output_chunk(
            api.0.user_data,
            SinomoModuleByteSlice::from_slice(&payload_sobj),
        )
    };
    if status == SINOMO_MODULE_STATUS_OK {
        Ok(())
    } else {
        Err(format!(
            "Cloud Files host rejected durable recovery acknowledgement with status {status}"
        ))
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
    fn descriptor_exposes_blocked_mount_and_supported_unregister_honestly() {
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
        assert!(
            descriptor
                .supported_operations
                .contains(&SINOMO_DFS_CLOUD_FILES_UNREGISTER_OPERATION.to_string())
        );
        assert!(
            descriptor
                .supported_operations
                .contains(&SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_OPERATION.to_string())
        );
        assert!(
            descriptor.supported_operations.contains(
                &SINOMO_DFS_CLOUD_FILES_CURSOR_ONLY_RECOVERY_INSPECT_OPERATION.to_string()
            )
        );
        assert!(
            descriptor
                .supported_operations
                .contains(&SINOMO_DFS_CLOUD_FILES_CURSOR_ONLY_RECOVERY_APPLY_OPERATION.to_string())
        );
        assert!(
            descriptor
                .capabilities
                .contains(&"dfs.sifs.cloud-files.unregister-sync-root".to_string())
        );
        assert!(
            descriptor
                .capabilities
                .contains(&SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_CAPABILITY.to_string())
        );
        assert!(
            descriptor.capabilities.contains(
                &SINOMO_DFS_CLOUD_FILES_CURSOR_ONLY_RECOVERY_INSPECT_CAPABILITY.to_string()
            )
        );
        assert!(
            descriptor.capabilities.contains(
                &SINOMO_DFS_CLOUD_FILES_CURSOR_ONLY_RECOVERY_APPLY_CAPABILITY.to_string()
            )
        );
        assert!(
            descriptor
                .capabilities
                .contains(&"dfs.sifs.cloud-files.lifecycle-status".to_string())
        );
        assert!(
            !descriptor
                .capabilities
                .contains(&"dfs.sifs.cloud-files.local-change-upload".to_string())
        );
        assert!(
            !descriptor
                .capabilities
                .contains(&"dfs.sifs.cloud-files.read-only".to_string())
        );
        assert!(
            !descriptor
                .capabilities
                .contains(&"dfs.sifs.write".to_string())
        );
    }

    #[test]
    fn operation_payload_cross_pairs_fail_before_platform_dispatch() {
        let mount = CloudFilesModuleRequest::Mount(
            sinomo_dfs_cloud_files_api::CloudFilesMountRequest::new("C:\\SinomoSIFS"),
        );
        let unregister_request =
            sinomo_dfs_cloud_files_api::CloudFilesUnregisterRequest::new("C:\\SinomoSIFS");
        let unregister = CloudFilesModuleRequest::Unregister(unregister_request.clone());
        let preserve = CloudFilesModuleRequest::PreserveStateUnregister(unregister_request);
        let recovery_identity = sinomo_dfs_cloud_files_api::CloudFilesCursorOnlyRecoveryIdentity {
            schema_version:
                sinomo_dfs_cloud_files_api::CLOUD_FILES_CURSOR_ONLY_RECOVERY_SCHEMA_VERSION,
            enabled: false,
            sync_root: "C:\\SinomoSIFS".into(),
            root_path: "/services".to_string(),
            data_dir_root: "C:\\SinomoData".into(),
            provider_name: sinomo_dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_PROVIDER_NAME
                .to_string(),
            provider_version: env!("CARGO_PKG_VERSION").to_string(),
            read_only: false,
            allow_other: false,
            sync_local_changes: true,
            prepopulate_depth: 0,
            connect: true,
            max_run_seconds: None,
            disabled_config_fingerprint: "blake3:disabled-config".to_string(),
            provider_provenance_fingerprint: "blake3:provider".to_string(),
        };
        let inspect = CloudFilesModuleRequest::InspectCursorOnlyRecovery(
            sinomo_dfs_cloud_files_api::CloudFilesCursorOnlyRecoveryInspectRequest {
                identity: recovery_identity.clone(),
            },
        );
        let apply = CloudFilesModuleRequest::ApplyCursorOnlyRecovery(
            sinomo_dfs_cloud_files_api::CloudFilesCursorOnlyRecoveryApplyRequest {
                identity: recovery_identity,
                expected_token: "v1:blake3:token".to_string(),
                recovery_id: "cursor-recovery-id".to_string(),
                acknowledge_authoritative_full_reconciliation: true,
            },
        );
        let operations = [
            SINOMO_DFS_CLOUD_FILES_MOUNT_OPERATION,
            SINOMO_DFS_CLOUD_FILES_UNREGISTER_OPERATION,
            SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_OPERATION,
            SINOMO_DFS_CLOUD_FILES_CURSOR_ONLY_RECOVERY_INSPECT_OPERATION,
            SINOMO_DFS_CLOUD_FILES_CURSOR_ONLY_RECOVERY_APPLY_OPERATION,
        ];
        let payloads = [mount, unregister, preserve, inspect, apply];

        let mut rejected = 0;
        for (operation_index, operation) in operations.into_iter().enumerate() {
            for (payload_index, payload) in payloads.iter().enumerate() {
                if operation_index == payload_index {
                    continue;
                }
                rejected += 1;
                let response = invoke_cloud_files_operation(NativeModuleInvocationRequest {
                    operation: operation.to_string(),
                    payload_sobj: encode_sobj(payload).expect("encode mismatched request"),
                    request_body: None,
                    response_body_path: None,
                });
                let NativeModuleResponse::Invocation(response) = response else {
                    panic!("mismatched typed payload should return an invocation response");
                };
                let decoded = decode_sobj::<CloudFilesModuleResponse>(&response.payload_sobj)
                    .expect("decode mismatch response");
                let CloudFilesModuleResponse::Error(error) = decoded else {
                    panic!(
                        "operation {operation} accepted cross-paired payload index {payload_index}"
                    );
                };
                assert_eq!(error.code, "operation-payload-mismatch");
            }
        }
        assert_eq!(rejected, 20, "the complete 5x5 off-diagonal matrix ran");
    }

    #[test]
    fn mount_lifecycle_event_round_trips_through_native_payload_encoding() {
        let event = sinomo_dfs_cloud_files_api::CloudFilesModuleEvent::MountLifecycle(
            sinomo_dfs_cloud_files_api::CloudFilesMountLifecycleEvent::connected(
                "C:\\SinomoSIFS",
                "/services",
                23,
            ),
        );

        let encoded = encode_sobj(&event).expect("encode lifecycle event");
        let decoded = decode_sobj::<sinomo_dfs_cloud_files_api::CloudFilesModuleEvent>(&encoded)
            .expect("decode lifecycle event");

        assert_eq!(decoded, event);
    }

    fn placeholder_identity_with_path(path: String) -> CloudFilesPlaceholderIdentity {
        CloudFilesPlaceholderIdentity::new(
            "C:\\SinomoSIFS",
            "/services",
            path,
            "01kr3object0000000000000000",
            "01kr3version000000000000000",
            Some(
                "blake3:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                    .to_string(),
            ),
        )
    }

    #[test]
    fn placeholder_identity_round_trips_and_stays_within_cloud_files_limit() {
        let identity = placeholder_identity_with_path("/services/report.txt".to_string());

        let encoded = encode_placeholder_identity(&identity).expect("encode identity");
        assert!(encoded.len() <= CLOUD_FILES_PLACEHOLDER_IDENTITY_MAX_BYTES);
        assert_eq!(
            decode_placeholder_identity(&encoded).expect("decode identity"),
            DecodedCloudFilesPlaceholderIdentity::Versioned(identity)
        );
    }

    #[test]
    fn placeholder_identity_size_limit_is_enforced_before_cfapi() {
        let oversized = placeholder_identity_with_path(format!("/services/{}", "x".repeat(5000)));

        let error = encode_placeholder_identity(&oversized).expect_err("oversized identity");
        assert_eq!(error.code, "placeholder-identity-too-large");
    }

    #[test]
    fn legacy_path_only_placeholder_identity_remains_decodable() {
        assert_eq!(
            decode_placeholder_identity(b"/services/legacy.txt").expect("legacy path"),
            DecodedCloudFilesPlaceholderIdentity::LegacyPath("/services/legacy.txt".to_string())
        );
    }

    #[test]
    fn exact_identity_classifier_rejects_each_mutable_binding() {
        let expected = placeholder_identity_with_path("/services/report.txt".to_string());
        assert_eq!(placeholder_identity_mismatch(&expected, &expected), None);

        let mut observed = expected.clone();
        observed.object_id = "01kr3differentobject000000000".to_string();
        assert_eq!(
            placeholder_identity_mismatch(&expected, &observed),
            Some(CloudFilesStaleIdentityReason::ObjectChanged)
        );
        observed = expected.clone();
        observed.version = "01kr3differentversion00000000".to_string();
        assert_eq!(
            placeholder_identity_mismatch(&expected, &observed),
            Some(CloudFilesStaleIdentityReason::VersionChanged)
        );
        observed = expected.clone();
        observed.content_hash = Some("blake3:different".to_string());
        assert_eq!(
            placeholder_identity_mismatch(&expected, &observed),
            Some(CloudFilesStaleIdentityReason::ContentChanged)
        );
        observed = expected.clone();
        observed.path = "/services/replacement.txt".to_string();
        assert_eq!(
            placeholder_identity_mismatch(&expected, &observed),
            Some(CloudFilesStaleIdentityReason::PathChanged)
        );
    }

    #[test]
    #[cfg(not(windows))]
    fn mount_returns_unsupported_on_non_windows() {
        let mut request =
            sinomo_dfs_cloud_files_api::CloudFilesMountRequest::new("/tmp/sinomo-sifs");
        request.read_only = false;
        request.sync_local_changes = true;
        let response = mount_sifs_cloud_files(request);

        assert_eq!(
            response.expect_err("unsupported").code,
            "unsupported-platform"
        );
    }

    #[test]
    #[cfg(not(windows))]
    fn best_effort_writable_mount_is_rejected_before_platform_dispatch() {
        let mut request =
            sinomo_dfs_cloud_files_api::CloudFilesMountRequest::new("/tmp/sinomo-sifs");
        request.read_only = false;

        assert_eq!(
            mount_sifs_cloud_files(request)
                .expect_err("writable mode must fail closed")
                .code,
            sinomo_dfs_cloud_files_api::CLOUD_FILES_WRITABLE_SYNC_UNSUPPORTED_CODE
        );
    }

    #[test]
    #[cfg(not(windows))]
    fn unregister_returns_unsupported_on_non_windows() {
        let response = unregister_sifs_cloud_files(
            sinomo_dfs_cloud_files_api::CloudFilesUnregisterRequest::new("/tmp/sinomo-sifs"),
        );

        assert_eq!(
            response.expect_err("unsupported").code,
            "unsupported-platform"
        );
    }
}
