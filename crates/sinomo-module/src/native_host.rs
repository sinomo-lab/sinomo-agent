//! Generic host loader for trusted native-dylib Sinomo modules.
//!
//! Native modules share the host process address space. This loader verifies
//! content hashes and ABI descriptors, but it is not a security sandbox.

use sinomo_module_api::host::{
    HostServiceRequest, HostServiceResponse, INVOCATION_HOST_VERSION, InvocationHost,
    InvokeWithHostFn, MAX_HOST_CALL_BYTES,
};
use std::collections::HashMap;
use std::ffi::c_void;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};

use libloading::Library;
use sinomo_module_api::{
    NativeModuleDescriptor, NativeModuleRequest, NativeModuleResponse, SINOMO_MODULE_STATUS_OK,
    SINOMO_MODULE_STATUS_RUNTIME_ERROR, SINOMO_NATIVE_MODULE_ABI_VERSION, SinomoModuleApiVersionFn,
    SinomoModuleByteBuffer, SinomoModuleByteSlice, SinomoModuleDescriptorFn,
    SinomoModuleFreeBufferFn, SinomoModuleHostApiV2, SinomoModuleInitFn, SinomoModuleInvokeFn,
    SinomoModuleShutdownFn, SinomoModuleStatusCode, decode_sobj, encode_sobj,
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
    /// Only services explicitly attached to this invocation are available.
    fn host_service(&self, _request: &HostServiceRequest) -> Result<Vec<u8>, String> {
        Err("host service is not available in this invocation".into())
    }

    fn output_payload(&self, _payload_sobj: &[u8]) {}
    /// Synchronous output hook for module effects which require durable host
    /// acknowledgement before they may proceed. Existing hosts retain their
    /// historical fire-and-forget behavior through the default implementation.
    fn output_payload_status(&self, payload_sobj: &[u8]) -> bool {
        self.output_payload(payload_sobj);
        true
    }
    fn log(&self, _message: &[u8]) {}
    fn is_cancelled(&self) -> bool {
        false
    }
    /// Return a process-local native wait handle which is signaled when this
    /// invocation is cancelled. Modules must treat the value as borrowed for
    /// the duration of the invocation only.
    fn cancellation_wait_handle(&self) -> Option<u64> {
        None
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
    library: Option<Library>,
    descriptor: NativeModuleDescriptor,
    invoke: SinomoModuleInvokeFn,
    invoke_with_host: Option<InvokeWithHostFn>,
    shutdown: Option<SinomoModuleShutdownFn>,
    free_buffer: SinomoModuleFreeBufferFn,
    callbacks: Option<Box<NativeModuleHostCallbackState>>,
    // Native dylibs store the host API in process-global module state. Hold a
    // per-library owned permit until shutdown and unload complete so two
    // handles cannot overwrite each other's callback user_data pointers.
    lifecycle_permit: Option<NativeModuleLifecyclePermit>,
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
        let invoke_with_host = unsafe {
            library
                .get::<InvokeWithHostFn>(b"sinomo_module_invoke_with_host_v1\0")
                .ok()
                .map(|symbol| *symbol)
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
            library: Some(library),
            descriptor,
            invoke,
            invoke_with_host,
            shutdown: Some(shutdown),
            free_buffer,
            callbacks: Some(callbacks),
            lifecycle_permit: Some(lifecycle_permit),
        })
    }

    /// Loads a descriptor after verifying the library's content hash.
    ///
    /// # Safety
    ///
    /// The caller must trust the verified native library to honor Sinomo's
    /// module ABI. Loading a native library executes platform loader behavior
    /// and its exported functions may run arbitrary native code.
    pub unsafe fn descriptor_from_content_addressed_path(
        library_path: impl AsRef<Path>,
        expected_content_hash: &str,
    ) -> Result<NativeModuleDescriptor, NativeModuleHostError> {
        let library_path = library_path.as_ref();
        verify_native_module_hash(library_path, expected_content_hash)?;
        unsafe { Self::descriptor_from_verified_path(library_path) }
    }

    /// Loads a descriptor from a path whose contents were already verified.
    ///
    /// # Safety
    ///
    /// The caller must establish both the library's trust and its immutability
    /// for the duration of this call. The library must honor Sinomo's module
    /// ABI; loading it and calling its exports may execute arbitrary native
    /// code.
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
    // Android's emulated TLS can register pthread-key destructors in the
    // calling host thread. Dropping the last library handle does not remove
    // those callbacks; a later blocking-worker exit would jump into unmapped
    // code. Retain the mapping even for descriptor inspection. Logical shutdown
    // and route withdrawal still run normally; process exit reclaims the code.
    #[cfg(target_os = "android")]
    let library = unsafe {
        libloading::os::unix::Library::open(
            Some(library_path),
            libc::RTLD_NOW | libc::RTLD_LOCAL | libc::RTLD_NODELETE,
        )
    }
    .map(Library::from);
    #[cfg(not(target_os = "android"))]
    let library = unsafe { Library::new(library_path) };
    library.map_err(|source| NativeModuleHostError::Load {
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
    pub fn supports_invocation_host(&self) -> bool {
        self.invoke_with_host.is_some()
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
        let _guard = self
            .callbacks
            .as_ref()
            .expect("loaded native module callback state")
            .set_callbacks(if self.invoke_with_host.is_some() {
                None
            } else {
                callbacks.clone()
            })?;
        let mut response_buffer = SinomoModuleByteBuffer::null();
        let status = unsafe {
            if let Some(invoke) = self.invoke_with_host {
                let context = InvocationServices {
                    callbacks,
                    requirements: &self.descriptor.component.requirements,
                };
                invoke(
                    SinomoModuleByteSlice::from_slice(&request_bytes),
                    InvocationHost {
                        version: INVOCATION_HOST_VERSION,
                        user_data: &context as *const InvocationServices<'_> as *mut c_void,
                        call: invocation_host_call,
                        free: free_host_service_buffer,
                        is_cancelled: invocation_host_cancelled,
                    },
                    &mut response_buffer,
                )
            } else {
                (self.invoke)(
                    SinomoModuleByteSlice::from_slice(&request_bytes),
                    &mut response_buffer,
                )
            }
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
        if !report.shutdown_ok {
            self.quarantine_after_failed_shutdown();
            drop(self);
            report
                .detail
                .push_str("; executable mapping quarantined until process exit");
            return report;
        }
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
        let status = unsafe { shutdown() };
        if status == SINOMO_MODULE_STATUS_OK
            && let Some(callback_state) = self.callbacks.as_ref()
            && let Ok(mut callbacks) = callback_state.callbacks.lock()
        {
            *callbacks = None;
        }
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

    fn quarantine_after_failed_shutdown(&mut self) {
        if let Some(library) = self.library.take() {
            std::mem::forget(library);
        }
        if let Some(callbacks) = self.callbacks.take() {
            std::mem::forget(callbacks);
        }
        if let Some(permit) = self.lifecycle_permit.take() {
            std::mem::forget(permit);
        }
    }
}

impl Drop for NativeModule {
    fn drop(&mut self) {
        let report = self.shutdown_module("drop");
        if !report.shutdown_ok {
            self.quarantine_after_failed_shutdown();
        }
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
        return SINOMO_MODULE_STATUS_RUNTIME_ERROR;
    };
    let Ok(callbacks) = state.callbacks.lock() else {
        return SINOMO_MODULE_STATUS_RUNTIME_ERROR;
    };
    let Some(callbacks) = callbacks.as_ref() else {
        return SINOMO_MODULE_STATUS_OK;
    };
    if callbacks.output_payload_status(unsafe { payload_sobj.as_slice() }) {
        SINOMO_MODULE_STATUS_OK
    } else {
        SINOMO_MODULE_STATUS_RUNTIME_ERROR
    }
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
    if !descriptor.component.is_empty() {
        descriptor
            .component
            .validate()
            .map_err(|error| NativeModuleHostError::Descriptor {
                detail: error.to_string(),
            })?;
        if descriptor.component.component_id != descriptor.module_id
            || descriptor.component.component_version != descriptor.module_version
        {
            return Err(NativeModuleHostError::Descriptor {
                detail: "component identity does not match native module identity".to_string(),
            });
        }
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

struct InvocationServices<'a> {
    callbacks: Option<Arc<dyn NativeModuleHostCallbacks>>,
    requirements: &'a [sinomo_module_api::ServiceRequirement],
}
unsafe extern "C" fn free_host_service_buffer(buffer: SinomoModuleByteBuffer) {
    unsafe {
        drop(SinomoModuleByteBuffer::into_vec(buffer));
    }
}
unsafe extern "C" fn invocation_host_call(
    data: *mut c_void,
    request: SinomoModuleByteSlice,
    out: *mut SinomoModuleByteBuffer,
) -> SinomoModuleStatusCode {
    if data.is_null() || out.is_null() || request.len > MAX_HOST_CALL_BYTES {
        return sinomo_module_api::SINOMO_MODULE_STATUS_INVALID_ARGUMENT;
    }
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let context = unsafe { &*(data as *const InvocationServices<'_>) };
        let request: HostServiceRequest =
            sinomo_module_api::decode_sobj(unsafe { request.as_slice() })
                .map_err(|e| e.to_string())?;
        if !context.requirements.iter().any(|requirement| {
            requirement.interface_id == request.interface_id
                && (requirement.interface_version == request.interface_version
                    || requirement.interface_version == "*")
                && requirement.operations.contains(&request.operation)
        }) {
            return Err("module did not declare this host service operation".to_string());
        }
        let callbacks = context
            .callbacks
            .as_ref()
            .ok_or("no host services attached")?;
        if callbacks.is_cancelled() {
            return Err("invocation cancelled".into());
        }
        let bytes = callbacks.host_service(&request)?;
        if bytes.len() > MAX_HOST_CALL_BYTES / 2 {
            return Err("host service response exceeds limit".into());
        }
        Ok(bytes)
    }))
    .unwrap_or_else(|_| Err("host service panicked".into()));
    let response = match result {
        Ok(bytes) => HostServiceResponse::Value(bytes),
        Err(error) => HostServiceResponse::Error(error),
    };
    match sinomo_module_api::encode_sobj(&response) {
        Ok(bytes) => {
            unsafe {
                *out = SinomoModuleByteBuffer::from_vec(bytes);
            }
            SINOMO_MODULE_STATUS_OK
        }
        Err(_) => SINOMO_MODULE_STATUS_RUNTIME_ERROR,
    }
}

unsafe extern "C" fn invocation_host_cancelled(data: *mut c_void) -> u8 {
    if data.is_null() {
        return 1;
    }
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        unsafe { &*(data as *const InvocationServices<'_>) }
            .callbacks
            .as_ref()
            .is_some_and(|callbacks| callbacks.is_cancelled()) as u8
    }))
    .unwrap_or(1)
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::time::Duration;

    struct RejectingOutput;

    impl NativeModuleHostCallbacks for RejectingOutput {
        fn output_payload_status(&self, _payload_sobj: &[u8]) -> bool {
            false
        }
    }

    #[test]
    fn synchronous_output_failure_reaches_the_native_module() {
        let state = NativeModuleHostCallbackState::default();
        let _guard = state
            .set_callbacks(Some(Arc::new(RejectingOutput)))
            .expect("install rejecting output callback");
        let payload = b"requires-durable-host-ack";
        let status = unsafe {
            native_module_output_payload(
                (&state as *const NativeModuleHostCallbackState)
                    .cast_mut()
                    .cast(),
                SinomoModuleByteSlice::from_slice(payload),
            )
        };
        assert_eq!(status, SINOMO_MODULE_STATUS_RUNTIME_ERROR);
    }

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

#[cfg(test)]
mod invocation_service_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Calls(AtomicUsize);
    impl NativeModuleHostCallbacks for Calls {
        fn host_service(&self, _: &HostServiceRequest) -> Result<Vec<u8>, String> {
            self.0.fetch_add(1, Ordering::SeqCst);
            encode_sobj(&7u64).map_err(|e| e.to_string())
        }
    }
    #[test]
    fn scoped_host_calls_check_declared_operation_and_version_before_dispatch() {
        let calls = Arc::new(Calls(AtomicUsize::new(0)));
        let requirements = vec![
            sinomo_module_api::ServiceRequirement::new("test.service", "1.0.0")
                .with_operation("read"),
        ];
        let context = InvocationServices {
            callbacks: Some(calls.clone()),
            requirements: &requirements,
        };
        let host = InvocationHost {
            version: INVOCATION_HOST_VERSION,
            user_data: &context as *const _ as *mut c_void,
            call: invocation_host_call,
            free: free_host_service_buffer,
            is_cancelled: invocation_host_cancelled,
        };
        assert!(unsafe { host.call_sobj::<_, u64>("test.service", "2.0.0", "read", &()) }.is_err());
        assert!(
            unsafe { host.call_sobj::<_, u64>("test.service", "1.0.0", "write", &()) }.is_err()
        );
        assert_eq!(calls.0.load(Ordering::SeqCst), 0);
        assert_eq!(
            unsafe { host.call_sobj::<_, u64>("test.service", "1.0.0", "read", &()) }.unwrap(),
            7
        );
        assert_eq!(calls.0.load(Ordering::SeqCst), 1);
    }
}
