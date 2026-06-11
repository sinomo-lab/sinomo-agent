//! Stable native-dylib ABI and generic SOBJ envelopes for Sinomo modules.
//!
//! This crate is deliberately small. Host binaries and trusted native modules
//! can depend on it without inheriting any dynamic loader, sandbox, Wasm, or
//! service implementation dependency.

use std::ffi::c_void;
use std::ptr;
use std::slice;

use serde::{Deserialize, Serialize};
use sinomo_object::{LengthMode, serde_ext as sinomo_object_serde};
use thiserror::Error;

pub const SINOMO_NATIVE_MODULE_ABI_VERSION: u32 = 1;
pub const SINOMO_MODULE_OBJECT_VERSION: u16 = 1;

pub type SinomoModuleStatusCode = i32;

pub const SINOMO_MODULE_STATUS_OK: SinomoModuleStatusCode = 0;
pub const SINOMO_MODULE_STATUS_INVALID_ARGUMENT: SinomoModuleStatusCode = 1;
pub const SINOMO_MODULE_STATUS_UNSUPPORTED_ABI: SinomoModuleStatusCode = 2;
pub const SINOMO_MODULE_STATUS_RUNTIME_ERROR: SinomoModuleStatusCode = 3;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct SinomoModuleByteSlice {
    pub ptr: *const u8,
    pub len: usize,
}

impl SinomoModuleByteSlice {
    pub fn from_slice(bytes: &[u8]) -> Self {
        Self {
            ptr: bytes.as_ptr(),
            len: bytes.len(),
        }
    }

    /// # Safety
    ///
    /// The caller must guarantee that `ptr` is valid for `len` bytes for the
    /// duration of the returned borrow.
    pub unsafe fn as_slice<'a>(self) -> &'a [u8] {
        if self.ptr.is_null() || self.len == 0 {
            return &[];
        }
        unsafe { slice::from_raw_parts(self.ptr, self.len) }
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct SinomoModuleByteBuffer {
    pub ptr: *mut u8,
    pub len: usize,
    pub cap: usize,
}

impl SinomoModuleByteBuffer {
    pub fn null() -> Self {
        Self {
            ptr: ptr::null_mut(),
            len: 0,
            cap: 0,
        }
    }

    pub fn from_vec(mut bytes: Vec<u8>) -> Self {
        if bytes.is_empty() {
            return Self::null();
        }
        let buffer = Self {
            ptr: bytes.as_mut_ptr(),
            len: bytes.len(),
            cap: bytes.capacity(),
        };
        std::mem::forget(bytes);
        buffer
    }

    /// # Safety
    ///
    /// `buffer` must have been produced by [`SinomoModuleByteBuffer::from_vec`]
    /// in the same dynamic library that will reclaim it.
    pub unsafe fn into_vec(buffer: Self) -> Vec<u8> {
        if buffer.ptr.is_null() {
            return Vec::new();
        }
        unsafe { Vec::from_raw_parts(buffer.ptr, buffer.len, buffer.cap) }
    }
}

pub type SinomoModuleHostOutputFn = unsafe extern "C" fn(
    user_data: *mut c_void,
    payload_sobj: SinomoModuleByteSlice,
) -> SinomoModuleStatusCode;

pub type SinomoModuleHostLogFn = unsafe extern "C" fn(
    user_data: *mut c_void,
    message: SinomoModuleByteSlice,
) -> SinomoModuleStatusCode;

pub type SinomoModuleHostIsCancelledFn = unsafe extern "C" fn(user_data: *mut c_void) -> u8;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SinomoModuleHostApiV1 {
    pub abi_version: u32,
    pub user_data: *mut c_void,
    pub output_chunk: Option<SinomoModuleHostOutputFn>,
    pub log: Option<SinomoModuleHostLogFn>,
    pub is_cancelled: Option<SinomoModuleHostIsCancelledFn>,
}

impl SinomoModuleHostApiV1 {
    pub fn empty() -> Self {
        Self {
            abi_version: SINOMO_NATIVE_MODULE_ABI_VERSION,
            user_data: ptr::null_mut(),
            output_chunk: None,
            log: None,
            is_cancelled: None,
        }
    }
}

pub type SinomoModuleApiVersionFn = unsafe extern "C" fn() -> u32;
pub type SinomoModuleDescriptorFn =
    unsafe extern "C" fn(out: *mut SinomoModuleByteBuffer) -> SinomoModuleStatusCode;
pub type SinomoModuleInitFn =
    unsafe extern "C" fn(host: SinomoModuleHostApiV1) -> SinomoModuleStatusCode;
pub type SinomoModuleInvokeFn = unsafe extern "C" fn(
    request_sobj: SinomoModuleByteSlice,
    response_sobj: *mut SinomoModuleByteBuffer,
) -> SinomoModuleStatusCode;
pub type SinomoModuleShutdownFn = unsafe extern "C" fn() -> SinomoModuleStatusCode;
pub type SinomoModuleFreeBufferFn = unsafe extern "C" fn(buffer: SinomoModuleByteBuffer);

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NativeModuleDescriptor {
    pub module_id: String,
    pub module_version: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub owner: String,
    pub native_abi_version: u32,
    pub backend: NativeModuleBackend,
    pub interface_version: String,
    pub platform_triple: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub supported_operations: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<String>,
    #[serde(default, skip_serializing_if = "NativeModuleSurfaces::is_empty")]
    pub surfaces: NativeModuleSurfaces,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum NativeModuleBackend {
    NativeDylib,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct NativeModuleSurfaces {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub object_rpc_routes: Vec<NativeModuleObjectRpcRoute>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub declarative_resources: Vec<NativeModuleDeclarativeResource>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub storage_owners: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capability_imports: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capability_exports: Vec<String>,
}

impl NativeModuleSurfaces {
    pub fn is_empty(&self) -> bool {
        self.object_rpc_routes.is_empty()
            && self.declarative_resources.is_empty()
            && self.storage_owners.is_empty()
            && self.capability_imports.is_empty()
            && self.capability_exports.is_empty()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NativeModuleObjectRpcRoute {
    pub route: String,
    pub category: String,
    pub operation: String,
    pub auth: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NativeModuleDeclarativeResource {
    pub id: String,
    pub kind: String,
    pub path: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub target_agents: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub model_families: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub activation_tags: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_capabilities: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embedded_text: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum NativeModuleRequest {
    ProbeCapabilities(NativeModuleProbeCapabilitiesRequest),
    Invoke(NativeModuleInvocationRequest),
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Default)]
pub struct NativeModuleProbeCapabilitiesRequest {}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct NativeModuleInvocationRequest {
    pub operation: String,
    pub payload_sobj: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum NativeModuleResponse {
    Capabilities(NativeModuleCapabilitiesResponse),
    Invocation(NativeModuleInvocationResponse),
    Error(NativeModuleErrorResponse),
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NativeModuleCapabilitiesResponse {
    pub descriptor: NativeModuleDescriptor,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct NativeModuleInvocationResponse {
    pub operation: String,
    pub payload_sobj: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NativeModuleErrorResponse {
    pub code: String,
    pub detail: String,
}

#[derive(Debug, Error)]
pub enum NativeModuleApiError {
    #[error("SOBJ error: {0}")]
    Object(#[from] sinomo_object::Error),
    #[error("invalid native module ABI version {actual}; expected {expected}")]
    InvalidAbiVersion { expected: u32, actual: u32 },
    #[error("{0}")]
    Runtime(String),
}

pub fn encode_sobj<T: Serialize>(value: &T) -> Result<Vec<u8>, NativeModuleApiError> {
    Ok(sinomo_object_serde::to_bytes(
        value,
        SINOMO_MODULE_OBJECT_VERSION,
        LengthMode::U32,
    )?)
}

pub fn decode_sobj<T>(bytes: &[u8]) -> Result<T, NativeModuleApiError>
where
    T: for<'de> Deserialize<'de>,
{
    Ok(sinomo_object_serde::from_bytes(bytes)?)
}
