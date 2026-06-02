//! Generic host loader for trusted native-dylib Sinomo modules.
//!
//! Native modules share the host process address space. This loader verifies
//! content hashes and ABI descriptors, but it is not a security sandbox.

use std::ffi::c_void;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use libloading::Library;
use sinomo_module_api::{
    NativeModuleDescriptor, NativeModuleRequest, NativeModuleResponse, SINOMO_MODULE_STATUS_OK,
    SINOMO_NATIVE_MODULE_ABI_VERSION, SinomoModuleApiVersionFn, SinomoModuleByteBuffer,
    SinomoModuleByteSlice, SinomoModuleDescriptorFn, SinomoModuleFreeBufferFn,
    SinomoModuleHostApiV1, SinomoModuleInitFn, SinomoModuleInvokeFn, SinomoModuleShutdownFn,
    SinomoModuleStatusCode, decode_sobj, encode_sobj,
};

#[derive(Debug, thiserror::Error)]
pub enum NativeModuleHostError {
    #[error("native module I/O error at {path}: {detail}")]
    Io { path: String, detail: String },
    #[error("native module hash mismatch at {path}: expected {expected}, got {actual}")]
    HashMismatch {
        path: String,
        expected: String,
        actual: String,
    },
    #[error("native module load error at {path}: {detail}")]
    Load { path: String, detail: String },
    #[error("native module ABI mismatch: expected {expected}, got {actual}")]
    AbiMismatch { expected: u32, actual: u32 },
    #[error("native module descriptor mismatch: {detail}")]
    Descriptor { detail: String },
    #[error("native module status {status}: {detail}")]
    Status {
        status: SinomoModuleStatusCode,
        detail: String,
    },
    #[error("native module object error: {0}")]
    Object(String),
    #[error("native module unavailable: {0}")]
    Unavailable(String),
}

pub trait NativeModuleHostCallbacks: Send + Sync {
    fn output_payload(&self, _payload_sobj: &[u8]) {}
    fn log(&self, _message: &[u8]) {}
    fn is_cancelled(&self) -> bool {
        false
    }
}

pub struct NativeModule {
    _library: Library,
    descriptor: NativeModuleDescriptor,
    invoke: SinomoModuleInvokeFn,
    shutdown: Option<SinomoModuleShutdownFn>,
    free_buffer: SinomoModuleFreeBufferFn,
    callbacks: Box<NativeModuleHostCallbackState>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeModuleUnloadReport {
    pub module_id: String,
    pub interface_version: String,
    pub shutdown_called: bool,
    pub shutdown_ok: bool,
    pub shutdown_status: Option<SinomoModuleStatusCode>,
    pub detail: String,
    pub library_handle_released: bool,
}

impl NativeModule {
    /// Load a trusted native module from a content-addressed path.
    ///
    /// # Safety
    ///
    /// The loaded library can execute arbitrary native code in the host process.
    pub unsafe fn load_from_content_addressed_path(
        library_path: impl AsRef<Path>,
        expected_content_hash: &str,
    ) -> Result<Self, NativeModuleHostError> {
        let library_path = library_path.as_ref();
        verify_native_module_hash(library_path, expected_content_hash)?;
        unsafe { Self::load_verified(library_path) }
    }

    /// Load a native module after the caller has already verified its bytes.
    ///
    /// # Safety
    ///
    /// The same trust requirements as [`Self::load_from_content_addressed_path`]
    /// apply.
    pub unsafe fn load_verified(
        library_path: impl AsRef<Path>,
    ) -> Result<Self, NativeModuleHostError> {
        let library_path = library_path.as_ref();
        let library = unsafe { Library::new(library_path) }.map_err(|source| {
            NativeModuleHostError::Load {
                path: library_path.display().to_string(),
                detail: source.to_string(),
            }
        })?;
        let api_version = unsafe {
            load_symbol::<SinomoModuleApiVersionFn>(
                &library,
                b"sinomo_module_api_version\0",
                library_path,
            )?
        };
        let descriptor_fn = unsafe {
            load_symbol::<SinomoModuleDescriptorFn>(
                &library,
                b"sinomo_module_descriptor\0",
                library_path,
            )?
        };
        let init = unsafe {
            load_symbol::<SinomoModuleInitFn>(&library, b"sinomo_module_init\0", library_path)?
        };
        let invoke = unsafe {
            load_symbol::<SinomoModuleInvokeFn>(&library, b"sinomo_module_invoke\0", library_path)?
        };
        let shutdown = unsafe {
            load_symbol::<SinomoModuleShutdownFn>(
                &library,
                b"sinomo_module_shutdown\0",
                library_path,
            )?
        };
        let free_buffer = unsafe {
            load_symbol::<SinomoModuleFreeBufferFn>(
                &library,
                b"sinomo_module_free_buffer\0",
                library_path,
            )?
        };

        let actual_abi = unsafe { api_version() };
        if actual_abi != SINOMO_NATIVE_MODULE_ABI_VERSION {
            return Err(NativeModuleHostError::AbiMismatch {
                expected: SINOMO_NATIVE_MODULE_ABI_VERSION,
                actual: actual_abi,
            });
        }

        let descriptor = unsafe { read_descriptor(descriptor_fn, free_buffer)? };
        validate_descriptor(&descriptor)?;

        let callbacks = Box::new(NativeModuleHostCallbackState::default());
        let host_api = SinomoModuleHostApiV1 {
            abi_version: SINOMO_NATIVE_MODULE_ABI_VERSION,
            user_data: callbacks.as_ref() as *const NativeModuleHostCallbackState as *mut c_void,
            output_chunk: Some(native_module_output_payload),
            log: Some(native_module_log),
            is_cancelled: Some(native_module_is_cancelled),
        };
        let init_status = unsafe { init(host_api) };
        if init_status != SINOMO_MODULE_STATUS_OK {
            return Err(NativeModuleHostError::Status {
                status: init_status,
                detail: "initialize native module".to_string(),
            });
        }

        Ok(Self {
            _library: library,
            descriptor,
            invoke,
            shutdown: Some(shutdown),
            free_buffer,
            callbacks,
        })
    }

    pub fn descriptor(&self) -> &NativeModuleDescriptor {
        &self.descriptor
    }

    pub fn invoke(
        &self,
        request: &NativeModuleRequest,
    ) -> Result<NativeModuleResponse, NativeModuleHostError> {
        self.invoke_with_callbacks(request, None)
    }

    pub fn invoke_with_callbacks(
        &self,
        request: &NativeModuleRequest,
        callbacks: Option<Arc<dyn NativeModuleHostCallbacks>>,
    ) -> Result<NativeModuleResponse, NativeModuleHostError> {
        let request_bytes = encode_sobj(request)
            .map_err(|source| NativeModuleHostError::Object(source.to_string()))?;
        let _guard = self.callbacks.set_callbacks(callbacks)?;
        let mut response_buffer = SinomoModuleByteBuffer::null();
        let status = unsafe {
            (self.invoke)(
                SinomoModuleByteSlice::from_slice(&request_bytes),
                &mut response_buffer,
            )
        };
        let response_bytes = unsafe { copy_and_free_buffer(response_buffer, self.free_buffer) };
        if status != SINOMO_MODULE_STATUS_OK {
            if let Ok(response) = decode_sobj::<NativeModuleResponse>(&response_bytes) {
                return Ok(response);
            }
            return Err(NativeModuleHostError::Status {
                status,
                detail: String::from_utf8_lossy(&response_bytes).into_owned(),
            });
        }
        decode_sobj(&response_bytes)
            .map_err(|source| NativeModuleHostError::Object(source.to_string()))
    }

    /// Release this native module handle after calling its shutdown hook.
    ///
    /// Dynamic-library unloading is platform and module dependent. This method
    /// makes the unload attempt explicit and returns the module's shutdown
    /// status, then drops the library handle so `libloading` can perform its
    /// best-effort unload.
    pub fn unload(mut self) -> NativeModuleUnloadReport {
        let mut report = self.shutdown_module("explicit unload");
        drop(self);
        report.library_handle_released = true;
        report
    }

    fn shutdown_module(&mut self, reason: &str) -> NativeModuleUnloadReport {
        let Some(shutdown) = self.shutdown.take() else {
            return NativeModuleUnloadReport {
                module_id: self.descriptor.module_id.clone(),
                interface_version: self.descriptor.interface_version.clone(),
                shutdown_called: false,
                shutdown_ok: true,
                shutdown_status: None,
                detail: format!("native module already shut down before {reason}"),
                library_handle_released: false,
            };
        };
        if let Ok(mut callbacks) = self.callbacks.callbacks.lock() {
            *callbacks = None;
        }
        let status = unsafe { shutdown() };
        NativeModuleUnloadReport {
            module_id: self.descriptor.module_id.clone(),
            interface_version: self.descriptor.interface_version.clone(),
            shutdown_called: true,
            shutdown_ok: status == SINOMO_MODULE_STATUS_OK,
            shutdown_status: Some(status),
            detail: if status == SINOMO_MODULE_STATUS_OK {
                format!("native module shutdown completed during {reason}")
            } else {
                format!("native module shutdown returned status {status} during {reason}")
            },
            library_handle_released: false,
        }
    }
}

impl Drop for NativeModule {
    fn drop(&mut self) {
        let _ = self.shutdown_module("drop");
    }
}

#[derive(Default)]
struct NativeModuleHostCallbackState {
    callbacks: Mutex<Option<Arc<dyn NativeModuleHostCallbacks>>>,
}

impl NativeModuleHostCallbackState {
    fn set_callbacks(
        &self,
        callbacks: Option<Arc<dyn NativeModuleHostCallbacks>>,
    ) -> Result<NativeModuleHostCallbackGuard<'_>, NativeModuleHostError> {
        let mut guard = self.callbacks.lock().map_err(|_| {
            NativeModuleHostError::Unavailable("native module callback lock poisoned".to_string())
        })?;
        *guard = callbacks;
        Ok(NativeModuleHostCallbackGuard { state: self })
    }
}

struct NativeModuleHostCallbackGuard<'a> {
    state: &'a NativeModuleHostCallbackState,
}

impl Drop for NativeModuleHostCallbackGuard<'_> {
    fn drop(&mut self) {
        if let Ok(mut callbacks) = self.state.callbacks.lock() {
            *callbacks = None;
        }
    }
}

unsafe extern "C" fn native_module_output_payload(
    user_data: *mut c_void,
    payload_sobj: SinomoModuleByteSlice,
) -> SinomoModuleStatusCode {
    let Some(state) = (unsafe { (user_data as *const NativeModuleHostCallbackState).as_ref() })
    else {
        return 1;
    };
    let Ok(callbacks) = state.callbacks.lock() else {
        return 1;
    };
    let Some(callbacks) = callbacks.as_ref() else {
        return SINOMO_MODULE_STATUS_OK;
    };
    callbacks.output_payload(unsafe { payload_sobj.as_slice() });
    SINOMO_MODULE_STATUS_OK
}

unsafe extern "C" fn native_module_log(
    user_data: *mut c_void,
    message: SinomoModuleByteSlice,
) -> SinomoModuleStatusCode {
    let Some(state) = (unsafe { (user_data as *const NativeModuleHostCallbackState).as_ref() })
    else {
        return 1;
    };
    let Ok(callbacks) = state.callbacks.lock() else {
        return 1;
    };
    if let Some(callbacks) = callbacks.as_ref() {
        callbacks.log(unsafe { message.as_slice() });
    }
    SINOMO_MODULE_STATUS_OK
}

unsafe extern "C" fn native_module_is_cancelled(user_data: *mut c_void) -> u8 {
    let Some(state) = (unsafe { (user_data as *const NativeModuleHostCallbackState).as_ref() })
    else {
        return 0;
    };
    let Ok(callbacks) = state.callbacks.lock() else {
        return 0;
    };
    callbacks
        .as_ref()
        .is_some_and(|callbacks| callbacks.is_cancelled()) as u8
}

unsafe fn load_symbol<T: Copy>(
    library: &Library,
    symbol: &[u8],
    library_path: &Path,
) -> Result<T, NativeModuleHostError> {
    let symbol =
        unsafe { library.get::<T>(symbol) }.map_err(|source| NativeModuleHostError::Load {
            path: library_path.display().to_string(),
            detail: source.to_string(),
        })?;
    Ok(*symbol)
}

unsafe fn read_descriptor(
    descriptor_fn: SinomoModuleDescriptorFn,
    free_buffer: SinomoModuleFreeBufferFn,
) -> Result<NativeModuleDescriptor, NativeModuleHostError> {
    let mut buffer = SinomoModuleByteBuffer::null();
    let status = unsafe { descriptor_fn(&mut buffer) };
    let bytes = unsafe { copy_and_free_buffer(buffer, free_buffer) };
    if status != SINOMO_MODULE_STATUS_OK {
        return Err(NativeModuleHostError::Status {
            status,
            detail: String::from_utf8_lossy(&bytes).into_owned(),
        });
    }
    decode_sobj(&bytes).map_err(|source| NativeModuleHostError::Object(source.to_string()))
}

fn validate_descriptor(descriptor: &NativeModuleDescriptor) -> Result<(), NativeModuleHostError> {
    if descriptor.native_abi_version != SINOMO_NATIVE_MODULE_ABI_VERSION {
        return Err(NativeModuleHostError::AbiMismatch {
            expected: SINOMO_NATIVE_MODULE_ABI_VERSION,
            actual: descriptor.native_abi_version,
        });
    }
    if descriptor.module_id.trim().is_empty() {
        return Err(NativeModuleHostError::Descriptor {
            detail: "module_id is empty".to_string(),
        });
    }
    if descriptor.interface_version.trim().is_empty() {
        return Err(NativeModuleHostError::Descriptor {
            detail: "interface_version is empty".to_string(),
        });
    }
    Ok(())
}

unsafe fn copy_and_free_buffer(
    buffer: SinomoModuleByteBuffer,
    free_buffer: SinomoModuleFreeBufferFn,
) -> Vec<u8> {
    if buffer.ptr.is_null() {
        return Vec::new();
    }
    let bytes = unsafe { std::slice::from_raw_parts(buffer.ptr, buffer.len) }.to_vec();
    unsafe {
        free_buffer(buffer);
    }
    bytes
}

fn verify_native_module_hash(
    library_path: &Path,
    expected_content_hash: &str,
) -> Result<(), NativeModuleHostError> {
    let bytes = fs::read(library_path).map_err(|source| NativeModuleHostError::Io {
        path: library_path.display().to_string(),
        detail: source.to_string(),
    })?;
    let actual = native_module_content_hash(&bytes);
    if actual != expected_content_hash {
        return Err(NativeModuleHostError::HashMismatch {
            path: library_path.display().to_string(),
            expected: expected_content_hash.to_string(),
            actual,
        });
    }
    Ok(())
}

pub fn native_module_content_hash(bytes: &[u8]) -> String {
    format!("blake3:{}", blake3::hash(bytes).to_hex())
}

pub fn native_module_cache_path(
    data_dir_root: impl AsRef<Path>,
    module_path_key: &str,
    content_hash: &str,
    library_name: &str,
) -> PathBuf {
    data_dir_root
        .as_ref()
        .join("module")
        .join("cache")
        .join("native-dylib")
        .join(module_path_key)
        .join(content_hash.replace(':', "_"))
        .join(library_name)
}
