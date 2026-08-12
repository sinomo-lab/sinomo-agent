//! Generic host loader for trusted native-dylib Sinomo modules.
//!
//! Native modules share the host process address space. This loader verifies
//! content hashes and ABI descriptors, but it is not a security sandbox.

use std::collections::HashMap;
use std::ffi::c_void;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};

use libloading::Library;
use sinomo_module_api::{
    NativeModuleDescriptor, NativeModuleRequest, NativeModuleResponse, SINOMO_MODULE_STATUS_OK,
    SINOMO_NATIVE_MODULE_ABI_VERSION, SinomoModuleApiVersionFn, SinomoModuleByteBuffer,
    SinomoModuleByteSlice, SinomoModuleDescriptorFn, SinomoModuleFreeBufferFn,
    SinomoModuleHostApiV2, SinomoModuleInitFn, SinomoModuleInvokeFn, SinomoModuleShutdownFn,
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
    fn local_permission_granted(&self, _permission: &str) -> bool {
        false
    }
}

/// Content-verified raw dynamic-library handle for trusted modules that
/// export entry points beyond the six standard module symbols (e.g. the
/// network-module vtable entry). Unlike [`NativeModule`], it resolves no
/// symbols and calls no lifecycle hooks by itself.
///
/// The handle keeps the `libloading::Library` alive: every symbol resolved
/// through [`NativeLibrary::symbol`] (and everything reachable from it, such
/// as vtable function pointers an entry symbol returns) is only valid while
/// this value lives. Hosts that hand resolved pointers to long-lived
/// registries must keep the `NativeLibrary` alive alongside them (e.g. boxed
/// as an adapter's keep-alive token).
pub struct NativeLibrary {
    library: Library,
    path: PathBuf,
}

impl NativeLibrary {
    /// Load a trusted library from a content-addressed path, verifying the
    /// blake3 content hash first (same verification as
    /// [`NativeModule::load_from_content_addressed_path`]).
    ///
    /// # Safety
    ///
    /// The loaded library can execute arbitrary native code in the host
    /// process (static initializers run at load time).
    pub unsafe fn load_from_content_addressed_path(
        library_path: impl AsRef<Path>,
        expected_content_hash: &str,
    ) -> Result<Self, NativeModuleHostError> {
        let library_path = library_path.as_ref();
        verify_native_module_hash(library_path, expected_content_hash)?;
        unsafe { Self::load_verified(library_path) }
    }

    /// Load a library after the caller has already verified its bytes.
    ///
    /// # Safety
    ///
    /// The same trust requirements as
    /// [`Self::load_from_content_addressed_path`] apply.
    pub unsafe fn load_verified(
        library_path: impl AsRef<Path>,
    ) -> Result<Self, NativeModuleHostError> {
        let library_path = library_path.as_ref();
        let library = unsafe { load_native_module_library(library_path) }?;
        Ok(Self {
            library,
            path: library_path.to_path_buf(),
        })
    }

    /// Resolve a typed symbol by name. `name` may or may not include the
    /// trailing NUL byte (`libloading` appends one when missing).
    ///
    /// # Safety
    ///
    /// `T` must match the symbol's actual type in the library; the resolved
    /// value must not be used after this `NativeLibrary` is dropped.
    pub unsafe fn symbol<T: Copy>(&self, name: &[u8]) -> Result<T, NativeModuleHostError> {
        unsafe { load_symbol::<T>(&self.library, name, &self.path) }
    }

    /// The path this library was loaded from (diagnostics).
    pub fn path(&self) -> &Path {
        &self.path
    }
}

pub struct NativeModule {
    _library: Library,
    descriptor: NativeModuleDescriptor,
    invoke: SinomoModuleInvokeFn,
    shutdown: Option<SinomoModuleShutdownFn>,
    free_buffer: SinomoModuleFreeBufferFn,
    callbacks: Box<NativeModuleHostCallbackState>,
    // Native dylibs store the host API in process-global module state. Hold a
    // per-library owned permit until shutdown and unload complete so two
    // handles cannot overwrite each other's callback user_data pointers.
    _lifecycle_permit: NativeModuleLifecyclePermit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NativeModuleMappingLifetime {
    Handle,
    #[cfg_attr(not(windows), allow(dead_code))]
    Process,
}

#[derive(Default)]
struct NativeModuleLifecycleGate {
    active: Mutex<bool>,
    available: Condvar,
}

struct NativeModuleLifecyclePermit {
    gate: Arc<NativeModuleLifecycleGate>,
}

impl NativeModuleLifecycleGate {
    fn acquire(self: &Arc<Self>) -> Result<NativeModuleLifecyclePermit, NativeModuleHostError> {
        let mut active = self.active.lock().map_err(|_| {
            NativeModuleHostError::Unavailable(
                "native module lifecycle gate lock poisoned".to_string(),
            )
        })?;
        while *active {
            active = self.available.wait(active).map_err(|_| {
                NativeModuleHostError::Unavailable(
                    "native module lifecycle gate wait poisoned".to_string(),
                )
            })?;
        }
        *active = true;
        Ok(NativeModuleLifecyclePermit {
            gate: Arc::clone(self),
        })
    }
}

impl Drop for NativeModuleLifecyclePermit {
    fn drop(&mut self) {
        let mut active = self
            .gate
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *active = false;
        self.gate.available.notify_one();
    }
}

fn native_module_lifecycle_gate(
    library_path: &Path,
) -> Result<Arc<NativeModuleLifecycleGate>, NativeModuleHostError> {
    static GATES: OnceLock<Mutex<HashMap<PathBuf, Weak<NativeModuleLifecycleGate>>>> =
        OnceLock::new();
    let library_path =
        fs::canonicalize(library_path).map_err(|source| NativeModuleHostError::Io {
            path: library_path.display().to_string(),
            detail: source.to_string(),
        })?;
    let mut gates = GATES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .map_err(|_| {
            NativeModuleHostError::Unavailable(
                "native module lifecycle gate registry lock poisoned".to_string(),
            )
        })?;
    gates.retain(|_, gate| gate.strong_count() > 0);
    if let Some(gate) = gates.get(&library_path).and_then(Weak::upgrade) {
        return Ok(gate);
    }
    let gate = Arc::new(NativeModuleLifecycleGate::default());
    gates.insert(library_path, Arc::downgrade(&gate));
    Ok(gate)
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
        unsafe {
            Self::load_verified_with_mapping_lifetime(
                library_path,
                NativeModuleMappingLifetime::Handle,
            )
        }
    }

    /// Load a trusted, content-addressed native module and keep its Windows
    /// image mapped until process exit.
    ///
    /// This is an intentionally irreversible lifetime policy for a module
    /// whose own runtime threads can outlive a completed invocation or module
    /// shutdown. The normal lifecycle hook and callback ownership rules still
    /// apply; only code/data mapping reclamation is deferred to process exit.
    /// Do not use this for disposable staged copies which must be deleted in
    /// the same process.
    ///
    /// # Safety
    ///
    /// The loaded library can execute arbitrary native code in the host process.
    #[cfg(windows)]
    pub unsafe fn load_process_resident_from_content_addressed_path(
        library_path: impl AsRef<Path>,
        expected_content_hash: &str,
    ) -> Result<Self, NativeModuleHostError> {
        let library_path = library_path.as_ref();
        verify_native_module_hash(library_path, expected_content_hash)?;
        unsafe {
            Self::load_verified_with_mapping_lifetime(
                library_path,
                NativeModuleMappingLifetime::Process,
            )
        }
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
        unsafe {
            Self::load_verified_with_mapping_lifetime(
                library_path.as_ref(),
                NativeModuleMappingLifetime::Handle,
            )
        }
    }

    unsafe fn load_verified_with_mapping_lifetime(
        library_path: &Path,
        mapping_lifetime: NativeModuleMappingLifetime,
    ) -> Result<Self, NativeModuleHostError> {
        let lifecycle_permit = native_module_lifecycle_gate(library_path)?.acquire()?;
        let library = unsafe { load_native_module_library(library_path) }?;
        let api_version = unsafe { native_module_api_version(&library, library_path) }?;
        if api_version != SINOMO_NATIVE_MODULE_ABI_VERSION {
            return Err(NativeModuleHostError::AbiMismatch {
                expected: SINOMO_NATIVE_MODULE_ABI_VERSION,
                actual: api_version,
            });
        }
        let descriptor = unsafe { native_module_descriptor(&library, library_path) }?;
        validate_descriptor(&descriptor)?;

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

        let library = apply_native_module_mapping_lifetime(library, mapping_lifetime, |library| {
            pin_native_module_library_until_process_exit(library, library_path)
        })?;

        let callbacks = Box::new(NativeModuleHostCallbackState::default());
        let host_api = SinomoModuleHostApiV2 {
            abi_version: SINOMO_NATIVE_MODULE_ABI_VERSION,
            user_data: callbacks.as_ref() as *const NativeModuleHostCallbackState as *mut c_void,
            output_chunk: Some(native_module_output_payload),
            log: Some(native_module_log),
            is_cancelled: Some(native_module_is_cancelled),
            local_permission_granted: Some(native_module_local_permission_granted),
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
            _lifecycle_permit: lifecycle_permit,
        })
    }

    pub unsafe fn descriptor_from_content_addressed_path(
        library_path: impl AsRef<Path>,
        expected_content_hash: &str,
    ) -> Result<NativeModuleDescriptor, NativeModuleHostError> {
        let library_path = library_path.as_ref();
        verify_native_module_hash(library_path, expected_content_hash)?;
        unsafe { Self::descriptor_from_verified_path(library_path) }
    }

    pub unsafe fn descriptor_from_verified_path(
        library_path: impl AsRef<Path>,
    ) -> Result<NativeModuleDescriptor, NativeModuleHostError> {
        let library_path = library_path.as_ref();
        let library = unsafe { load_native_module_library(library_path) }?;
        let api_version = unsafe { native_module_api_version(&library, library_path) }?;
        if api_version != SINOMO_NATIVE_MODULE_ABI_VERSION {
            return Err(NativeModuleHostError::AbiMismatch {
                expected: SINOMO_NATIVE_MODULE_ABI_VERSION,
                actual: api_version,
            });
        }
        let descriptor = unsafe { native_module_descriptor(&library, library_path) }?;
        validate_descriptor(&descriptor)?;
        Ok(descriptor)
    }
}

fn apply_native_module_mapping_lifetime<T, E>(
    value: T,
    lifetime: NativeModuleMappingLifetime,
    make_process_resident: impl FnOnce(T) -> Result<T, E>,
) -> Result<T, E> {
    match lifetime {
        NativeModuleMappingLifetime::Handle => Ok(value),
        NativeModuleMappingLifetime::Process => make_process_resident(value),
    }
}

#[cfg(windows)]
fn pin_native_module_library_until_process_exit(
    library: Library,
    library_path: &Path,
) -> Result<Library, NativeModuleHostError> {
    let platform_library: libloading::os::windows::Library = library.into();
    let pin_result = platform_library.pin();
    let library = Library::from(platform_library);
    pin_result
        .map(|()| library)
        .map_err(|source| NativeModuleHostError::Load {
            path: library_path.display().to_string(),
            detail: format!("pin native module mapping until process exit: {source}"),
        })
}

#[cfg(not(windows))]
fn pin_native_module_library_until_process_exit(
    _library: Library,
    _library_path: &Path,
) -> Result<Library, NativeModuleHostError> {
    Err(NativeModuleHostError::Unavailable(
        "process-resident native module mappings are only supported on Windows".to_string(),
    ))
}

unsafe fn load_native_module_library(
    library_path: &Path,
) -> Result<Library, NativeModuleHostError> {
    unsafe { Library::new(library_path) }.map_err(|source| NativeModuleHostError::Load {
        path: library_path.display().to_string(),
        detail: source.to_string(),
    })
}

unsafe fn native_module_api_version(
    library: &Library,
    library_path: &Path,
) -> Result<u32, NativeModuleHostError> {
    let api_version = unsafe {
        load_symbol::<SinomoModuleApiVersionFn>(
            library,
            b"sinomo_module_api_version\0",
            library_path,
        )?
    };
    Ok(unsafe { api_version() })
}

unsafe fn native_module_descriptor(
    library: &Library,
    library_path: &Path,
) -> Result<NativeModuleDescriptor, NativeModuleHostError> {
    let descriptor_fn = unsafe {
        load_symbol::<SinomoModuleDescriptorFn>(
            library,
            b"sinomo_module_descriptor\0",
            library_path,
        )?
    };
    let free_buffer = unsafe {
        load_symbol::<SinomoModuleFreeBufferFn>(
            library,
            b"sinomo_module_free_buffer\0",
            library_path,
        )?
    };
    unsafe { read_descriptor(descriptor_fn, free_buffer) }
}

impl NativeModule {
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

unsafe extern "C" fn native_module_local_permission_granted(
    user_data: *mut c_void,
    permission: SinomoModuleByteSlice,
) -> u8 {
    let Some(state) = (unsafe { (user_data as *const NativeModuleHostCallbackState).as_ref() })
    else {
        return 0;
    };
    let Ok(permission) = std::str::from_utf8(unsafe { permission.as_slice() }) else {
        return 0;
    };
    let Ok(callbacks) = state.callbacks.lock() else {
        return 0;
    };
    callbacks
        .as_ref()
        .is_some_and(|callbacks| callbacks.local_permission_granted(permission)) as u8
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

#[cfg(test)]
mod lifecycle_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::time::Duration;

    #[test]
    fn mapping_lifetime_only_pins_process_resident_modules() {
        let pin_calls = AtomicUsize::new(0);
        let ordinary = apply_native_module_mapping_lifetime(
            "ordinary",
            NativeModuleMappingLifetime::Handle,
            |value| {
                pin_calls.fetch_add(1, Ordering::SeqCst);
                Ok::<_, ()>(value)
            },
        )
        .expect("ordinary mapping");
        assert_eq!(ordinary, "ordinary");
        assert_eq!(pin_calls.load(Ordering::SeqCst), 0);

        let resident = apply_native_module_mapping_lifetime(
            "resident",
            NativeModuleMappingLifetime::Process,
            |value| {
                pin_calls.fetch_add(1, Ordering::SeqCst);
                Ok::<_, ()>(value)
            },
        )
        .expect("process-resident mapping");
        assert_eq!(resident, "resident");
        assert_eq!(pin_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn process_resident_mapping_fails_closed_when_pinning_fails() {
        let error = apply_native_module_mapping_lifetime(
            "resident",
            NativeModuleMappingLifetime::Process,
            |_value| Err::<&str, _>("pin failed"),
        )
        .expect_err("failed pin must reject the module");
        assert_eq!(error, "pin failed");
    }

    #[cfg(windows)]
    #[test]
    fn windows_process_resident_mapping_pins_the_loaded_image() {
        let process_image = libloading::os::windows::Library::this()
            .expect("obtain the current process image handle");
        let library = Library::from(process_image);
        let library = pin_native_module_library_until_process_exit(
            library,
            Path::new("current process image"),
        )
        .expect("pin the current process image");
        drop(library);
    }

    #[test]
    fn same_library_lifecycle_gate_serializes_callback_state_ownership() {
        let root = sinomo_testdir::TestDir::new("sinomo-native-module-lifecycle-gate");
        let library_path = root.join("module.test");
        fs::write(&library_path, b"gate identity").expect("write gate identity file");
        let first_gate = native_module_lifecycle_gate(&library_path).expect("first gate");
        let second_gate = native_module_lifecycle_gate(&library_path).expect("second gate");
        assert!(Arc::ptr_eq(&first_gate, &second_gate));

        let first_permit = first_gate.acquire().expect("first permit");
        let (acquired_tx, acquired_rx) = mpsc::channel();
        let waiter = std::thread::spawn(move || {
            let _second_permit = second_gate.acquire().expect("second permit");
            acquired_tx.send(()).expect("signal second permit");
        });
        assert!(
            acquired_rx
                .recv_timeout(Duration::from_millis(100))
                .is_err(),
            "second lifecycle entered while the first callback state was active"
        );

        drop(first_permit);
        acquired_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("second lifecycle acquired after release");
        waiter.join().expect("join lifecycle waiter");
    }
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
