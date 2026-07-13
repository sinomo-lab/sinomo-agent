//! Pinned `repr(C)` FFI contract for native-dylib network modules, plus the
//! host-side safe adapter ([`FfiNetworkModule`]).
//!
//! The byte slice/buffer structs mirror `sinomo-module-api`'s
//! (`ptr/len[/cap]`) but are defined locally: `sinomo-network` is an r0 crate
//! and must not depend on the r1 `sinomo-module-api` crate. Status codes also
//! mirror `sinomo-module-api` (0 ok, 1 invalid argument, 2 unsupported ABI,
//! 3 runtime error).
//!
//! Contract (v2):
//! - The dylib exports [`SINOMO_NETWORK_MODULE_ENTRY_SYMBOL`] with signature
//!   `extern "C" fn(*mut *mut c_void, *mut SinomoNetworkModuleVtableV2) -> i32`,
//!   creating a module instance and filling the vtable.
//! - All vtable functions must be callable from any thread; the instance is
//!   shared across threads by the host.
//! - Buffers written by the module (`descriptor`, stream errors,
//!   `local_endpoints`) are allocated by the module and must be released
//!   through the module's own `free_buffer` — allocators may differ across the
//!   dylib boundary.
//! - On a non-OK stream status the module SHOULD fill the error buffer
//!   with a name-keyed SOBJ [`NetworkModuleRpcErrorV1`]. The host retains a
//!   legacy UTF-8 fallback for older modules. Other functions MAY fill the
//!   output buffer with UTF-8 error detail.
//! - Descriptor/endpoint payloads are name-keyed SOBJ documents encoded with
//!   [`encode_network_module_sobj`] (`SINOMO_NETWORK_MODULE_OBJECT_VERSION`).
//! - Timeouts are milliseconds; `0` means "no timeout".
//!
//! The entry symbol deliberately names the stream-shaped ABI. The earlier
//! response-buffer vtable also called itself v1, but had a different size and
//! field layout; resolving it as this table would turn stale module state into
//! function-pointer memory corruption. The table is therefore self-describing
//! and every callback slot is nullable until the host validates it.

use crate::error::{NetworkDeliveryState, NetworkError, NetworkTimeoutPhase};
use crate::network_module::{
    NetworkFuture, NetworkModule, NetworkModuleContext, NetworkModuleDescriptor,
    NetworkModuleEndpointRole,
};
use crate::session::NetworkDuplexStream;
use crate::transfer::NetworkHealthPolicy;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sinomo_base::ClusterEndpointRecord;
use sinomo_object::{LengthMode, serde_ext as sinomo_object_serde};
use std::any::Any;
use std::ffi::c_void;
use std::ptr;
use std::slice;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// ABI discriminator filled into [`SinomoNetworkModuleVtableV2::abi_magic`].
pub const SINOMO_NETWORK_MODULE_ABI_MAGIC: u64 = u64::from_le_bytes(*b"SNMSTRM2");
/// ABI version filled into [`SinomoNetworkModuleVtableV2::abi_version`].
pub const SINOMO_NETWORK_MODULE_ABI_VERSION: u32 = 2;
/// Exported entry symbol creating the instance and filling the vtable.
/// Not NUL-terminated; append `\0` (or let the loader do so) when resolving.
///
/// This is intentionally incompatible with the old response-buffer entry
/// symbol. A stale dylib must fail symbol resolution before the host calls into
/// a table with a different layout.
pub const SINOMO_NETWORK_MODULE_ENTRY_SYMBOL: &[u8] = b"sinomo_network_module_create_v2";
/// SOBJ document version for descriptor/endpoint payloads crossing this ABI.
pub const SINOMO_NETWORK_MODULE_OBJECT_VERSION: u16 = 1;

pub type SinomoNetworkModuleStatusCode = i32;

pub const SINOMO_NETWORK_MODULE_STATUS_OK: SinomoNetworkModuleStatusCode = 0;
pub const SINOMO_NETWORK_MODULE_STATUS_INVALID_ARGUMENT: SinomoNetworkModuleStatusCode = 1;
pub const SINOMO_NETWORK_MODULE_STATUS_UNSUPPORTED_ABI: SinomoNetworkModuleStatusCode = 2;
pub const SINOMO_NETWORK_MODULE_STATUS_RUNTIME_ERROR: SinomoNetworkModuleStatusCode = 3;

/// Borrowed byte view crossing the module boundary (host- or module-owned;
/// valid only for the duration of the call it is passed to).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct SinomoNetworkByteSlice {
    pub ptr: *const u8,
    pub len: usize,
}

impl SinomoNetworkByteSlice {
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
        // SAFETY: non-null and valid for `len` bytes per the caller contract.
        unsafe { slice::from_raw_parts(self.ptr, self.len) }
    }
}

/// Owned byte buffer transferred across the module boundary. Produced by the
/// module (from a `Vec<u8>`), released through the module's `free_buffer`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct SinomoNetworkByteBuffer {
    pub ptr: *mut u8,
    pub len: usize,
    pub cap: usize,
}

impl SinomoNetworkByteBuffer {
    pub fn null() -> Self {
        Self {
            ptr: ptr::null_mut(),
            len: 0,
            cap: 0,
        }
    }

    /// Leaks `bytes` into a raw buffer; reclaim with
    /// [`SinomoNetworkByteBuffer::into_vec`] in the same dynamic library.
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
    /// `buffer` must have been produced by [`SinomoNetworkByteBuffer::from_vec`]
    /// in the same dynamic library that reclaims it (allocators must match).
    pub unsafe fn into_vec(buffer: Self) -> Vec<u8> {
        if buffer.ptr.is_null() {
            return Vec::new();
        }
        // SAFETY: ptr/len/cap came from a forgotten Vec in this library per
        // the caller contract.
        unsafe { Vec::from_raw_parts(buffer.ptr, buffer.len, buffer.cap) }
    }
}

/// Start configuration passed to [`SinomoNetworkModuleVtableV2::start`].
/// All fields are UTF-8 byte views owned by the host for the call duration.
#[repr(C)]
pub struct SinomoNetworkModuleConfigV1 {
    /// UTF-8 cluster data root (ClusterPaths::root).
    pub cluster_root_utf8: SinomoNetworkByteSlice,
    /// UTF-8 local node id.
    pub node_id_utf8: SinomoNetworkByteSlice,
    /// UTF-8 cluster id (empty when pre-cluster).
    pub cluster_id_utf8: SinomoNetworkByteSlice,
    /// UTF-8 local daemon control address for inbound forwarding.
    pub daemon_forward_address_utf8: SinomoNetworkByteSlice,
}

/// Opaque module-owned full-duplex stream. The host never dereferences this
/// pointer; it is valid until the matching `close_rpc_stream` call.
#[repr(transparent)]
#[derive(Clone, Copy, Debug)]
pub struct SinomoNetworkRpcStreamV1 {
    pub ptr: *mut c_void,
}

impl SinomoNetworkRpcStreamV1 {
    pub const fn null() -> Self {
        Self {
            ptr: ptr::null_mut(),
        }
    }

    pub const fn is_null(self) -> bool {
        self.ptr.is_null()
    }
}

// SAFETY: the ABI requires stream callbacks to be callable from any thread.
unsafe impl Send for SinomoNetworkRpcStreamV1 {}
// SAFETY: read and write callbacks may run concurrently for full duplex.
unsafe impl Sync for SinomoNetworkRpcStreamV1 {}

pub type SinomoNetworkModuleDescriptorFn =
    unsafe extern "C" fn(*mut SinomoNetworkByteBuffer) -> SinomoNetworkModuleStatusCode;
pub type SinomoNetworkModuleStartFn = unsafe extern "C" fn(
    *mut c_void,
    *const SinomoNetworkModuleConfigV1,
) -> SinomoNetworkModuleStatusCode;
pub type SinomoNetworkModuleStopFn =
    unsafe extern "C" fn(*mut c_void) -> SinomoNetworkModuleStatusCode;
pub type SinomoNetworkModuleOpenRpcStreamFn = unsafe extern "C" fn(
    *mut c_void,
    SinomoNetworkByteSlice,
    u64,
    u64,
    *mut SinomoNetworkRpcStreamV1,
    *mut SinomoNetworkByteBuffer,
)
    -> SinomoNetworkModuleStatusCode;
pub type SinomoNetworkModuleReadRpcStreamFn = unsafe extern "C" fn(
    SinomoNetworkRpcStreamV1,
    *mut u8,
    usize,
    *mut usize,
    *mut SinomoNetworkByteBuffer,
)
    -> SinomoNetworkModuleStatusCode;
pub type SinomoNetworkModuleWriteRpcStreamFn =
    unsafe extern "C" fn(
        SinomoNetworkRpcStreamV1,
        SinomoNetworkByteSlice,
        *mut usize,
        *mut SinomoNetworkByteBuffer,
    ) -> SinomoNetworkModuleStatusCode;
pub type SinomoNetworkModuleShutdownRpcStreamWriteFn =
    unsafe extern "C" fn(
        SinomoNetworkRpcStreamV1,
        *mut SinomoNetworkByteBuffer,
    ) -> SinomoNetworkModuleStatusCode;
pub type SinomoNetworkModuleCloseRpcStreamFn = unsafe extern "C" fn(SinomoNetworkRpcStreamV1);
pub type SinomoNetworkModuleLocalEndpointsFn =
    unsafe extern "C" fn(
        *mut c_void,
        *mut SinomoNetworkByteBuffer,
    ) -> SinomoNetworkModuleStatusCode;
pub type SinomoNetworkModuleEndpointRoleFn = unsafe extern "C" fn(
    *mut c_void,
    SinomoNetworkByteSlice,
    *mut SinomoNetworkByteBuffer,
) -> SinomoNetworkModuleStatusCode;
pub type SinomoNetworkModuleFreeBufferFn = unsafe extern "C" fn(SinomoNetworkByteBuffer);
pub type SinomoNetworkModuleDestroyFn = unsafe extern "C" fn(*mut c_void);

/// The pinned v2 stream table a network module fills in its entry function.
///
/// Callback slots are `Option<fn>` so an all-zero/empty table is a valid Rust
/// value. The host starts from [`Self::empty`] and validates the discriminator,
/// exact structural size, and every required callback before constructing the
/// internal callable table. A module that only writes an obsolete prefix can
/// therefore be rejected without reading uninitialized memory.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SinomoNetworkModuleVtableV2 {
    pub abi_magic: u64,
    pub abi_version: u32,
    pub struct_size: u32,
    /// SOBJ NetworkModuleDescriptor into a module-owned buffer.
    pub descriptor: Option<SinomoNetworkModuleDescriptorFn>,
    pub start: Option<SinomoNetworkModuleStartFn>,
    pub stop: Option<SinomoNetworkModuleStopFn>,
    /// Opens a raw bidirectional byte stream. Object-RPC and transfer framing
    /// stays entirely on the host side. The blocking operation is bridged off
    /// the async executor by [`FfiNetworkModule`].
    pub open_rpc_stream: Option<SinomoNetworkModuleOpenRpcStreamFn>,
    /// Blocking read into host-owned storage. `read_out == 0` means EOF.
    pub read_rpc_stream: Option<SinomoNetworkModuleReadRpcStreamFn>,
    /// Blocking write from host-owned storage. Partial writes are allowed.
    pub write_rpc_stream: Option<SinomoNetworkModuleWriteRpcStreamFn>,
    /// Flushes and half-closes the write side while preserving the read side.
    pub shutdown_rpc_stream_write: Option<SinomoNetworkModuleShutdownRpcStreamWriteFn>,
    /// Releases the opaque stream. Called exactly once after all callbacks
    /// using the handle have returned.
    pub close_rpc_stream: Option<SinomoNetworkModuleCloseRpcStreamFn>,
    /// SOBJ Vec<ClusterEndpointRecord> the module currently advertises.
    pub local_endpoints: Option<SinomoNetworkModuleLocalEndpointsFn>,
    /// Optional SOBJ endpoint -> SOBJ NetworkModuleEndpointRole classifier.
    /// Null means the module does not classify endpoint roles.
    pub endpoint_role: Option<SinomoNetworkModuleEndpointRoleFn>,
    pub free_buffer: Option<SinomoNetworkModuleFreeBufferFn>,
    pub destroy: Option<SinomoNetworkModuleDestroyFn>,
}

impl SinomoNetworkModuleVtableV2 {
    pub const fn empty() -> Self {
        Self {
            abi_magic: 0,
            abi_version: 0,
            struct_size: 0,
            descriptor: None,
            start: None,
            stop: None,
            open_rpc_stream: None,
            read_rpc_stream: None,
            write_rpc_stream: None,
            shutdown_rpc_stream_write: None,
            close_rpc_stream: None,
            local_endpoints: None,
            endpoint_role: None,
            free_buffer: None,
            destroy: None,
        }
    }

    pub const fn expected_struct_size() -> u32 {
        std::mem::size_of::<Self>() as u32
    }
}

/// Signature of the exported [`SINOMO_NETWORK_MODULE_ENTRY_SYMBOL`]: creates
/// the instance (`*mut *mut c_void` out) and fills the vtable.
pub type SinomoNetworkModuleCreateV2Fn = unsafe extern "C" fn(
    *mut *mut c_void,
    *mut SinomoNetworkModuleVtableV2,
) -> SinomoNetworkModuleStatusCode;

/// Encodes an ABI payload (descriptor, endpoint list) as the name-keyed SOBJ
/// document both sides of the module boundary agree on.
pub fn encode_network_module_sobj<T: Serialize>(value: &T) -> Result<Vec<u8>, NetworkError> {
    sinomo_object_serde::to_bytes(value, SINOMO_NETWORK_MODULE_OBJECT_VERSION, LengthMode::U32)
        .map_err(|source| NetworkError::Serialization {
            detail: format!("encode network module payload: {source}"),
        })
}

/// Decodes an ABI payload produced by [`encode_network_module_sobj`].
pub fn decode_network_module_sobj<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, NetworkError> {
    sinomo_object_serde::from_bytes(bytes).map_err(|source| NetworkError::Serialization {
        detail: format!("decode network module payload: {source}"),
    })
}

/// Structured error returned by a v1 network module's `dial_rpc` function.
///
/// Keeping delivery certainty across the dylib boundary is load-bearing: the
/// route layer may retry another endpoint when bytes were definitely not sent,
/// but must remain conservative after a possibly-applied request. The detail
/// string preserves the module-specific cause while the optional timeout keeps
/// phase-aware connectivity demotion working in the host.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NetworkModuleRpcErrorV1 {
    pub detail: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<NetworkModuleRpcTimeoutV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery_state: Option<NetworkDeliveryState>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NetworkModuleRpcTimeoutV1 {
    pub phase: NetworkTimeoutPhase,
    pub timeout_millis: u64,
}

impl NetworkModuleRpcErrorV1 {
    pub fn from_network_error(error: &NetworkError) -> Self {
        let timeout = match error {
            NetworkError::Timeout { phase, timeout, .. } => Some(NetworkModuleRpcTimeoutV1 {
                phase: *phase,
                timeout_millis: duration_millis_for_network_module(*timeout),
            }),
            _ => None,
        };
        Self {
            detail: error.to_string(),
            timeout,
            delivery_state: error.delivery_state(),
        }
    }

    fn into_network_error(self, operation: &str) -> NetworkError {
        if let Some(timeout) = self.timeout {
            return NetworkError::timeout(
                operation,
                timeout.phase,
                Duration::from_millis(timeout.timeout_millis.max(1)),
                self.delivery_state
                    .unwrap_or(NetworkDeliveryState::PossiblyApplied),
            );
        }
        if let Some(delivery_state) = self.delivery_state {
            return NetworkError::delivery_failed(operation, delivery_state, self.detail);
        }
        NetworkError::Io {
            operation: operation.to_string(),
            detail: self.detail,
        }
    }
}

/// Encodes a dial failure without discarding its delivery or timeout state.
pub fn encode_network_module_rpc_error(error: &NetworkError) -> Result<Vec<u8>, NetworkError> {
    encode_network_module_sobj(&NetworkModuleRpcErrorV1::from_network_error(error))
}

fn duration_millis_for_network_module(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis())
        .unwrap_or(u64::MAX)
        .max(1)
}

/// Maps host-side optional timeouts onto the ABI's millisecond convention
/// (`0` = no timeout; sub-millisecond timeouts round up to 1ms).
fn network_module_timeout_ms(timeout: Option<Duration>) -> u64 {
    match timeout {
        None => 0,
        Some(timeout) if timeout.is_zero() => 1,
        Some(timeout) => duration_millis_for_network_module(timeout),
    }
}

/// Raw module instance pointer, sendable across threads.
#[derive(Clone, Copy)]
struct FfiNetworkModuleInstance(*mut c_void);

// SAFETY: the network-module ABI contract (module docs above) requires every
// vtable function and the instance it operates on to be callable from any
// thread; the pointer itself is only ever dereferenced by those functions.
unsafe impl Send for FfiNetworkModuleInstance {}
// SAFETY: as above — shared use from multiple threads is part of the ABI
// contract, so `&FfiNetworkModuleInstance` is safe to share.
unsafe impl Sync for FfiNetworkModuleInstance {}

/// Shared ownership of one loaded module instance: the raw instance pointer,
/// the vtable operating on it, and the keep-alive token (typically the
/// `libloading::Library`) that keeps the vtable's code mapped.
///
/// Held behind an `Arc` so in-flight dials (which run on `spawn_blocking`
/// threads and may outlive the adapter when a module is replaced or dropped)
/// keep the instance and library alive until they finish. The LAST owner moves
/// the instance and library token to a dedicated teardown thread; this avoids
/// invoking a module's synchronous destroy callback (which may own an async
/// runtime) from an unrelated Tokio worker.
struct FfiNetworkModuleShared {
    instance: FfiNetworkModuleInstance,
    vtable: ValidatedNetworkModuleVtable,
    /// Taken by [`Drop::drop`] and moved with the instance to the teardown
    /// thread. `None` means ownership has already been transferred.
    keep_alive: Option<Box<dyn Any + Send + Sync>>,
}

/// Complete ownership bundle transferred out of the final Arc drop. Keeping
/// the dylib token in the same allocation guarantees that its callback code
/// stays mapped until `destroy` has returned.
struct FfiNetworkModuleTeardown {
    instance: FfiNetworkModuleInstance,
    vtable: ValidatedNetworkModuleVtable,
    _keep_alive: Box<dyn Any + Send + Sync>,
}

#[derive(Clone, Copy)]
struct FfiNetworkModuleTeardownPtr(*mut FfiNetworkModuleTeardown);

// SAFETY: ownership of the pointed-to teardown bundle is transferred exactly
// once to the teardown thread. No other thread dereferences the pointer.
unsafe impl Send for FfiNetworkModuleTeardownPtr {}

impl FfiNetworkModuleTeardownPtr {
    /// # Safety
    ///
    /// This must be called exactly once after successful thread creation.
    unsafe fn destroy(self) {
        // SAFETY: the pointer came from Box::into_raw below and ownership was
        // transferred to this one teardown worker.
        let teardown = unsafe { Box::from_raw(self.0) };
        // SAFETY: the entry contract made this instance/vtable pair callable;
        // the keep-alive token remains owned by `teardown` until return.
        unsafe { (teardown.vtable.destroy)(teardown.instance.0) };
        // `teardown` now drops on this plain OS thread, unloading the library
        // only after the module has synchronously finished destruction.
    }
}

#[derive(Clone, Copy, Debug)]
struct ValidatedNetworkModuleVtable {
    descriptor: SinomoNetworkModuleDescriptorFn,
    start: SinomoNetworkModuleStartFn,
    stop: SinomoNetworkModuleStopFn,
    open_rpc_stream: SinomoNetworkModuleOpenRpcStreamFn,
    read_rpc_stream: SinomoNetworkModuleReadRpcStreamFn,
    write_rpc_stream: SinomoNetworkModuleWriteRpcStreamFn,
    shutdown_rpc_stream_write: SinomoNetworkModuleShutdownRpcStreamWriteFn,
    close_rpc_stream: SinomoNetworkModuleCloseRpcStreamFn,
    local_endpoints: SinomoNetworkModuleLocalEndpointsFn,
    endpoint_role: Option<SinomoNetworkModuleEndpointRoleFn>,
    free_buffer: SinomoNetworkModuleFreeBufferFn,
    destroy: SinomoNetworkModuleDestroyFn,
}

impl TryFrom<SinomoNetworkModuleVtableV2> for ValidatedNetworkModuleVtable {
    type Error = NetworkError;

    fn try_from(vtable: SinomoNetworkModuleVtableV2) -> Result<Self, Self::Error> {
        let invalid = |detail: String| NetworkError::Io {
            operation: "load network module".to_string(),
            detail,
        };
        if vtable.abi_magic != SINOMO_NETWORK_MODULE_ABI_MAGIC {
            return Err(invalid(format!(
                "unsupported network module ABI discriminator 0x{:016x} (host expects 0x{:016x})",
                vtable.abi_magic, SINOMO_NETWORK_MODULE_ABI_MAGIC
            )));
        }
        if vtable.abi_version != SINOMO_NETWORK_MODULE_ABI_VERSION {
            return Err(invalid(format!(
                "unsupported network module ABI version {} (host expects {})",
                vtable.abi_version, SINOMO_NETWORK_MODULE_ABI_VERSION
            )));
        }
        let expected_size = SinomoNetworkModuleVtableV2::expected_struct_size();
        if vtable.struct_size != expected_size {
            return Err(invalid(format!(
                "unsupported network module vtable size {} (host expects {expected_size})",
                vtable.struct_size
            )));
        }
        macro_rules! required {
            ($field:ident) => {
                vtable.$field.ok_or_else(|| {
                    invalid(format!(
                        "network module vtable is missing required callback `{}`",
                        stringify!($field)
                    ))
                })?
            };
        }
        Ok(Self {
            descriptor: required!(descriptor),
            start: required!(start),
            stop: required!(stop),
            open_rpc_stream: required!(open_rpc_stream),
            read_rpc_stream: required!(read_rpc_stream),
            write_rpc_stream: required!(write_rpc_stream),
            shutdown_rpc_stream_write: required!(shutdown_rpc_stream_write),
            close_rpc_stream: required!(close_rpc_stream),
            local_endpoints: required!(local_endpoints),
            endpoint_role: vtable.endpoint_role,
            free_buffer: required!(free_buffer),
            destroy: required!(destroy),
        })
    }
}

/// Shared ownership of one opaque module stream. Each bridge pump owns a
/// clone, so `close_rpc_stream` cannot race a blocking read or write. The
/// parent module/library is retained until after the stream is closed.
struct FfiNetworkModuleStreamShared {
    stream: SinomoNetworkRpcStreamV1,
    module: Arc<FfiNetworkModuleShared>,
}

impl Drop for FfiNetworkModuleStreamShared {
    fn drop(&mut self) {
        // SAFETY: this is the last Arc owner, so every callback using the
        // opaque handle has returned. The handle came from open_rpc_stream and
        // is closed exactly once here.
        unsafe { (self.module.vtable.close_rpc_stream)(self.stream) };
    }
}

const FFI_NETWORK_STREAM_PIPE_BYTES: usize = 256 * 1024;
const FFI_NETWORK_STREAM_IO_BYTES: usize = 64 * 1024;

impl Drop for FfiNetworkModuleShared {
    fn drop(&mut self) {
        let Some(keep_alive) = self.keep_alive.take() else {
            return;
        };
        let teardown = Box::new(FfiNetworkModuleTeardown {
            instance: self.instance,
            vtable: self.vtable,
            _keep_alive: keep_alive,
        });
        let teardown = FfiNetworkModuleTeardownPtr(Box::into_raw(teardown));
        let worker_teardown = teardown;
        match thread::Builder::new()
            .name("sinomo-network-module-destroy".to_string())
            .spawn(move || {
                // SAFETY: successful spawn transfers the sole Box ownership
                // to this worker.
                unsafe { worker_teardown.destroy() };
            }) {
            Ok(worker) => {
                // Detach: final Arc release must not block an async executor.
                // The worker owns both the instance and library token.
                drop(worker);
            }
            Err(error) => {
                // `teardown` still names the raw allocation because the
                // pointer is Copy. Deliberately leak instance + dylib token:
                // unloading code underneath a live async runtime is UB, while
                // a bounded process-lifetime leak is safe.
                tracing::error!(%error, "failed to spawn network module teardown worker; leaking module safely");
                let _ = teardown;
            }
        }
    }
}

/// Host-side safe adapter: implements [`NetworkModule`] over a loaded module
/// instance + vtable.
///
/// The `keep_alive` token owns whatever keeps the vtable's code mapped —
/// typically the `libloading::Library` (passed as `Box<dyn Any + Send +
/// Sync>` so this r0 crate does not depend on `libloading`). Dial futures
/// clone the shared state, so a module replaced or dropped mid-dial is
/// destroyed only after the last in-flight call returns.
pub struct FfiNetworkModule {
    shared: Arc<FfiNetworkModuleShared>,
    descriptor: NetworkModuleDescriptor,
}

impl FfiNetworkModule {
    /// Wraps the instance + vtable produced by a module's entry function.
    /// Validates the ABI version and eagerly decodes the descriptor.
    ///
    /// # Safety
    ///
    /// - `instance` and `vtable` must come from a successful call to the
    ///   module's [`SINOMO_NETWORK_MODULE_ENTRY_SYMBOL`] entry.
    /// - `keep_alive` must keep the dynamic library (and thus every vtable
    ///   function pointer) loaded for as long as it lives.
    /// - Ownership of `instance` transfers to this function. A validated
    ///   instance is destroyed through the vtable on final drop; a non-null
    ///   instance paired with an invalid vtable is deliberately leaked with
    ///   `keep_alive` because no destructor can be trusted.
    pub unsafe fn from_entry_parts(
        instance: *mut c_void,
        vtable: SinomoNetworkModuleVtableV2,
        keep_alive: Box<dyn Any + Send + Sync>,
    ) -> Result<Self, NetworkError> {
        let vtable = match ValidatedNetworkModuleVtable::try_from(vtable) {
            Ok(vtable) => vtable,
            Err(error) => {
                if !instance.is_null() {
                    // A non-null instance may own threads or other resources
                    // whose destructors live in the dylib. The malformed
                    // table gives us no callback we can safely invoke, so the
                    // only sound failure mode is to leak both the instance and
                    // its library token for the rest of the process.
                    std::mem::forget(keep_alive);
                }
                return Err(error);
            }
        };
        if instance.is_null() {
            return Err(NetworkError::Io {
                operation: "load network module".to_string(),
                detail: "network module entry returned a null instance".to_string(),
            });
        }
        // Take ownership before the fallible descriptor read: an error below
        // must still destroy the instance (via the shared state's Drop) and
        // only then release the library keep-alive. The two checks above run
        // first because destroy cannot be trusted without a matching ABI
        // version and a non-null instance — leaking the instance and library
        // token together is the safe choice.
        let shared = Arc::new(FfiNetworkModuleShared {
            instance: FfiNetworkModuleInstance(instance),
            vtable,
            keep_alive: Some(keep_alive),
        });
        let mut buffer = SinomoNetworkByteBuffer::null();
        // SAFETY: descriptor/free_buffer are valid per the entry contract.
        let status = unsafe { (vtable.descriptor)(&mut buffer) };
        let bytes = unsafe { copy_and_free_network_buffer(buffer, vtable.free_buffer) };
        if status != SINOMO_NETWORK_MODULE_STATUS_OK {
            return Err(network_module_status_error(
                "read network module descriptor",
                status,
                &bytes,
            ));
        }
        let descriptor: NetworkModuleDescriptor = decode_network_module_sobj(&bytes)?;
        Ok(Self { shared, descriptor })
    }
}

impl NetworkModule for FfiNetworkModule {
    fn descriptor(&self) -> NetworkModuleDescriptor {
        self.descriptor.clone()
    }

    fn start(&self, ctx: &NetworkModuleContext) -> Result<(), NetworkError> {
        let cluster_root = ctx.cluster_root.to_string_lossy().into_owned();
        let node_id = ctx.node_id.to_string();
        let cluster_id = ctx
            .cluster_id
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();
        let config = SinomoNetworkModuleConfigV1 {
            cluster_root_utf8: SinomoNetworkByteSlice::from_slice(cluster_root.as_bytes()),
            node_id_utf8: SinomoNetworkByteSlice::from_slice(node_id.as_bytes()),
            cluster_id_utf8: SinomoNetworkByteSlice::from_slice(cluster_id.as_bytes()),
            daemon_forward_address_utf8: SinomoNetworkByteSlice::from_slice(
                ctx.daemon_forward_address.as_bytes(),
            ),
        };
        // SAFETY: instance/vtable are valid (entry contract); the config's
        // byte views borrow locals that outlive the call.
        let status = unsafe { (self.shared.vtable.start)(self.shared.instance.0, &config) };
        if status != SINOMO_NETWORK_MODULE_STATUS_OK {
            return Err(network_module_status_error(
                "start network module",
                status,
                &[],
            ));
        }
        Ok(())
    }

    fn stop(&self) -> Result<(), NetworkError> {
        // SAFETY: instance/vtable are valid per the entry contract.
        let status = unsafe { (self.shared.vtable.stop)(self.shared.instance.0) };
        if status != SINOMO_NETWORK_MODULE_STATUS_OK {
            return Err(network_module_status_error(
                "stop network module",
                status,
                &[],
            ));
        }
        Ok(())
    }

    fn open_rpc_stream(
        &self,
        endpoint: &ClusterEndpointRecord,
        health: NetworkHealthPolicy,
    ) -> NetworkFuture<Box<dyn NetworkDuplexStream>> {
        let module = self.shared.clone();
        let address = endpoint.address.clone();
        let connect_ms = network_module_timeout_ms(Some(health.connect_timeout));
        let idle_ms = network_module_timeout_ms(Some(health.no_progress_timeout));
        Box::pin(async move {
            let opened_module = module.clone();
            let opened = tokio::task::spawn_blocking(move || {
                let mut stream = SinomoNetworkRpcStreamV1::null();
                let mut error = SinomoNetworkByteBuffer::null();
                // SAFETY: the module/library is retained by `opened_module`;
                // address bytes live through the call and both output slots
                // are valid.
                let status = unsafe {
                    (opened_module.vtable.open_rpc_stream)(
                        opened_module.instance.0,
                        SinomoNetworkByteSlice::from_slice(address.as_bytes()),
                        connect_ms,
                        idle_ms,
                        &mut stream,
                        &mut error,
                    )
                };
                // SAFETY: any error buffer was allocated by this module.
                let detail = unsafe {
                    copy_and_free_network_buffer(error, opened_module.vtable.free_buffer)
                };
                if status != SINOMO_NETWORK_MODULE_STATUS_OK {
                    return Err(network_module_rpc_status_error(
                        "open network module RPC stream",
                        status,
                        &detail,
                    ));
                }
                if stream.is_null() {
                    return Err(NetworkError::Io {
                        operation: "open network module RPC stream".to_string(),
                        detail: "network module returned a null stream handle".to_string(),
                    });
                }
                Ok(Arc::new(FfiNetworkModuleStreamShared {
                    stream,
                    module: opened_module,
                }))
            })
            .await
            .map_err(|source| NetworkError::Io {
                operation: "open network module RPC stream".to_string(),
                detail: source.to_string(),
            })??;

            // The ABI operations are deliberately blocking. A bounded duplex
            // pipe gives callers a normal AsyncRead+AsyncWrite stream while
            // two independent pumps retain true full-duplex behavior and
            // apply backpressure instead of collecting either direction.
            let (caller, bridge) = tokio::io::duplex(FFI_NETWORK_STREAM_PIPE_BYTES);
            let (mut bridge_reader, mut bridge_writer) = tokio::io::split(bridge);

            let read_stream = opened.clone();
            tokio::spawn(async move {
                loop {
                    let operation_stream = read_stream.clone();
                    let read = tokio::task::spawn_blocking(move || {
                        ffi_network_stream_read(operation_stream)
                    })
                    .await;
                    let bytes = match read {
                        Ok(Ok(bytes)) => bytes,
                        Ok(Err(error)) => {
                            tracing::debug!(%error, "network module stream read stopped");
                            break;
                        }
                        Err(error) => {
                            tracing::debug!(%error, "network module stream read task stopped");
                            break;
                        }
                    };
                    if bytes.is_empty() {
                        break;
                    }
                    if bridge_writer.write_all(&bytes).await.is_err() {
                        break;
                    }
                }
                let _ = bridge_writer.shutdown().await;
            });

            let write_stream = opened.clone();
            tokio::spawn(async move {
                let mut buffer = vec![0_u8; FFI_NETWORK_STREAM_IO_BYTES];
                loop {
                    let read = match bridge_reader.read(&mut buffer).await {
                        Ok(read) => read,
                        Err(error) => {
                            tracing::debug!(%error, "network module stream bridge read stopped");
                            return;
                        }
                    };
                    if read == 0 {
                        break;
                    }
                    let bytes = buffer[..read].to_vec();
                    let operation_stream = write_stream.clone();
                    match tokio::task::spawn_blocking(move || {
                        ffi_network_stream_write_all(operation_stream, bytes)
                    })
                    .await
                    {
                        Ok(Ok(())) => {}
                        Ok(Err(error)) => {
                            tracing::debug!(%error, "network module stream write stopped");
                            return;
                        }
                        Err(error) => {
                            tracing::debug!(%error, "network module stream write task stopped");
                            return;
                        }
                    }
                }
                let operation_stream = write_stream.clone();
                match tokio::task::spawn_blocking(move || {
                    ffi_network_stream_shutdown_write(operation_stream)
                })
                .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        tracing::debug!(%error, "network module stream shutdown stopped");
                    }
                    Err(error) => {
                        tracing::debug!(%error, "network module stream shutdown task stopped");
                    }
                }
            });

            Ok(Box::new(caller) as Box<dyn NetworkDuplexStream>)
        })
    }

    fn local_endpoints(&self) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
        let mut buffer = SinomoNetworkByteBuffer::null();
        // SAFETY: instance/vtable are valid per the entry contract.
        let status =
            unsafe { (self.shared.vtable.local_endpoints)(self.shared.instance.0, &mut buffer) };
        // SAFETY: buffer was produced by the module; released via its
        // free_buffer.
        let bytes = unsafe { copy_and_free_network_buffer(buffer, self.shared.vtable.free_buffer) };
        if status != SINOMO_NETWORK_MODULE_STATUS_OK {
            return Err(network_module_status_error(
                "read network module endpoints",
                status,
                &bytes,
            ));
        }
        if bytes.is_empty() {
            return Ok(Vec::new());
        }
        decode_network_module_sobj(&bytes)
    }

    fn endpoint_role(&self, endpoint: &ClusterEndpointRecord) -> NetworkModuleEndpointRole {
        let Some(endpoint_role) = self.shared.vtable.endpoint_role else {
            return NetworkModuleEndpointRole::Unknown;
        };
        let Ok(endpoint_bytes) = encode_network_module_sobj(endpoint) else {
            return NetworkModuleEndpointRole::Unknown;
        };
        let mut buffer = SinomoNetworkByteBuffer::null();
        // SAFETY: instance/vtable are valid per the entry contract; the input
        // byte view borrows local encoded bytes for the duration of the call.
        let status = unsafe {
            endpoint_role(
                self.shared.instance.0,
                SinomoNetworkByteSlice::from_slice(&endpoint_bytes),
                &mut buffer,
            )
        };
        // SAFETY: buffer was produced by the module; released via its
        // free_buffer.
        let bytes = unsafe { copy_and_free_network_buffer(buffer, self.shared.vtable.free_buffer) };
        if status != SINOMO_NETWORK_MODULE_STATUS_OK || bytes.is_empty() {
            return NetworkModuleEndpointRole::Unknown;
        }
        decode_network_module_sobj(&bytes).unwrap_or(NetworkModuleEndpointRole::Unknown)
    }
}

fn ffi_network_stream_read(
    shared: Arc<FfiNetworkModuleStreamShared>,
) -> Result<Vec<u8>, NetworkError> {
    let mut bytes = vec![0_u8; FFI_NETWORK_STREAM_IO_BYTES];
    let mut read = 0_usize;
    let mut error = SinomoNetworkByteBuffer::null();
    // SAFETY: `shared` retains the module and opaque stream for this call;
    // `bytes` and both output slots remain valid until it returns.
    let status = unsafe {
        (shared.module.vtable.read_rpc_stream)(
            shared.stream,
            bytes.as_mut_ptr(),
            bytes.len(),
            &mut read,
            &mut error,
        )
    };
    // SAFETY: any error buffer was allocated by this module.
    let detail = unsafe { copy_and_free_network_buffer(error, shared.module.vtable.free_buffer) };
    if status != SINOMO_NETWORK_MODULE_STATUS_OK {
        return Err(network_module_rpc_status_error(
            "read network module RPC stream",
            status,
            &detail,
        ));
    }
    if read > bytes.len() {
        return Err(NetworkError::Io {
            operation: "read network module RPC stream".to_string(),
            detail: format!(
                "network module reported {read} bytes for a {}-byte buffer",
                bytes.len()
            ),
        });
    }
    bytes.truncate(read);
    Ok(bytes)
}

fn ffi_network_stream_write_all(
    shared: Arc<FfiNetworkModuleStreamShared>,
    bytes: Vec<u8>,
) -> Result<(), NetworkError> {
    let mut offset = 0_usize;
    while offset < bytes.len() {
        let mut written = 0_usize;
        let mut error = SinomoNetworkByteBuffer::null();
        // SAFETY: `shared` retains the module and opaque stream; the byte view
        // and output slots remain valid through the blocking callback.
        let status = unsafe {
            (shared.module.vtable.write_rpc_stream)(
                shared.stream,
                SinomoNetworkByteSlice::from_slice(&bytes[offset..]),
                &mut written,
                &mut error,
            )
        };
        // SAFETY: any error buffer was allocated by this module.
        let detail =
            unsafe { copy_and_free_network_buffer(error, shared.module.vtable.free_buffer) };
        if status != SINOMO_NETWORK_MODULE_STATUS_OK {
            return Err(network_module_rpc_status_error(
                "write network module RPC stream",
                status,
                &detail,
            ));
        }
        if written == 0 || written > bytes.len() - offset {
            return Err(NetworkError::Io {
                operation: "write network module RPC stream".to_string(),
                detail: format!(
                    "network module reported invalid progress {written} for {} remaining bytes",
                    bytes.len() - offset
                ),
            });
        }
        offset += written;
    }
    Ok(())
}

fn ffi_network_stream_shutdown_write(
    shared: Arc<FfiNetworkModuleStreamShared>,
) -> Result<(), NetworkError> {
    let mut error = SinomoNetworkByteBuffer::null();
    // SAFETY: `shared` retains the module and opaque stream and the output
    // slot remains valid through the callback.
    let status =
        unsafe { (shared.module.vtable.shutdown_rpc_stream_write)(shared.stream, &mut error) };
    // SAFETY: any error buffer was allocated by this module.
    let detail = unsafe { copy_and_free_network_buffer(error, shared.module.vtable.free_buffer) };
    if status != SINOMO_NETWORK_MODULE_STATUS_OK {
        return Err(network_module_rpc_status_error(
            "shutdown network module RPC stream",
            status,
            &detail,
        ));
    }
    Ok(())
}

/// Copies a module-owned buffer into host memory and releases the module
/// allocation through the module's `free_buffer`.
///
/// # Safety
///
/// `buffer` must have been produced by the same module that owns
/// `free_buffer`, and must not be used after this call.
unsafe fn copy_and_free_network_buffer(
    buffer: SinomoNetworkByteBuffer,
    free_buffer: unsafe extern "C" fn(SinomoNetworkByteBuffer),
) -> Vec<u8> {
    if buffer.ptr.is_null() {
        return Vec::new();
    }
    // SAFETY: the module guarantees ptr is valid for len bytes until freed.
    let bytes = unsafe { slice::from_raw_parts(buffer.ptr, buffer.len) }.to_vec();
    // SAFETY: released exactly once, by the owning module's allocator.
    unsafe { free_buffer(buffer) };
    bytes
}

fn network_module_rpc_status_error(
    operation: &str,
    status: SinomoNetworkModuleStatusCode,
    detail_bytes: &[u8],
) -> NetworkError {
    if let Ok(error) = decode_network_module_sobj::<NetworkModuleRpcErrorV1>(detail_bytes) {
        return error.into_network_error(operation);
    }
    network_module_status_error(operation, status, detail_bytes)
}

/// Builds the host-side error for a non-OK module status; `detail_bytes` is
/// the (possibly empty) UTF-8 error detail the module wrote into the output
/// buffer.
fn network_module_status_error(
    operation: &str,
    status: SinomoNetworkModuleStatusCode,
    detail_bytes: &[u8],
) -> NetworkError {
    let status_label = match status {
        SINOMO_NETWORK_MODULE_STATUS_INVALID_ARGUMENT => "invalid argument",
        SINOMO_NETWORK_MODULE_STATUS_UNSUPPORTED_ABI => "unsupported ABI",
        SINOMO_NETWORK_MODULE_STATUS_RUNTIME_ERROR => "runtime error",
        _ => "unknown status",
    };
    let detail = String::from_utf8_lossy(detail_bytes);
    let detail = detail.trim();
    NetworkError::Io {
        operation: operation.to_string(),
        detail: if detail.is_empty() {
            format!("network module status {status} ({status_label})")
        } else {
            format!("network module status {status} ({status_label}): {detail}")
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_base::ClusterTransportKind;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct KeepAliveDropProbe(Arc<AtomicBool>);

    impl Drop for KeepAliveDropProbe {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    #[test]
    fn network_module_sobj_round_trips_descriptor() {
        let descriptor = NetworkModuleDescriptor {
            id: "mesh".to_string(),
            transports: vec![ClusterTransportKind::WebRtc],
            address_schemes: vec!["mesh".to_string()],
            dialer: true,
            listener: true,
            supports_runtime_toggle: true,
        };
        let bytes = encode_network_module_sobj(&descriptor).expect("encode descriptor");
        let decoded: NetworkModuleDescriptor =
            decode_network_module_sobj(&bytes).expect("decode descriptor");
        assert_eq!(decoded, descriptor);
    }

    #[test]
    fn network_module_timeouts_use_zero_for_none() {
        assert_eq!(network_module_timeout_ms(None), 0);
        assert_eq!(network_module_timeout_ms(Some(Duration::ZERO)), 1);
        assert_eq!(
            network_module_timeout_ms(Some(Duration::from_micros(300))),
            1,
            "sub-millisecond timeouts round up instead of degrading to no-timeout"
        );
        assert_eq!(
            network_module_timeout_ms(Some(Duration::from_secs(2))),
            2_000
        );
    }

    #[test]
    fn network_module_v2_rejects_zero_initialized_and_partial_tables() {
        let empty = SinomoNetworkModuleVtableV2::empty();
        let empty_error = ValidatedNetworkModuleVtable::try_from(empty)
            .expect_err("zero-initialized table must not be callable");
        assert!(empty_error.to_string().contains("ABI discriminator"));

        let mut partial = SinomoNetworkModuleVtableV2::empty();
        partial.abi_magic = SINOMO_NETWORK_MODULE_ABI_MAGIC;
        partial.abi_version = SINOMO_NETWORK_MODULE_ABI_VERSION;
        partial.struct_size = SinomoNetworkModuleVtableV2::expected_struct_size();
        let partial_error = ValidatedNetworkModuleVtable::try_from(partial)
            .expect_err("matching header without callbacks must be rejected");
        assert!(partial_error.to_string().contains("`descriptor`"));
    }

    #[test]
    fn network_module_v2_rejects_obsolete_table_size_before_callbacks() {
        let mut obsolete = SinomoNetworkModuleVtableV2::empty();
        obsolete.abi_magic = SINOMO_NETWORK_MODULE_ABI_MAGIC;
        obsolete.abi_version = SINOMO_NETWORK_MODULE_ABI_VERSION;
        obsolete.struct_size = SinomoNetworkModuleVtableV2::expected_struct_size() - 1;

        let error = ValidatedNetworkModuleVtable::try_from(obsolete)
            .expect_err("obsolete table shape must be rejected");
        assert!(error.to_string().contains("vtable size"));
        assert_eq!(
            SINOMO_NETWORK_MODULE_ENTRY_SYMBOL,
            b"sinomo_network_module_create_v2"
        );
    }

    #[test]
    fn malformed_vtable_keeps_library_loaded_for_a_leaked_live_instance() {
        let dropped = Arc::new(AtomicBool::new(false));
        let instance = Box::into_raw(Box::new(())) as *mut c_void;

        // SAFETY: this deliberately models a successful entry that returned a
        // non-null instance but a malformed table. No callback is callable.
        let error = unsafe {
            FfiNetworkModule::from_entry_parts(
                instance,
                SinomoNetworkModuleVtableV2::empty(),
                Box::new(KeepAliveDropProbe(Arc::clone(&dropped))),
            )
        }
        .err()
        .expect("malformed table must be rejected");

        assert!(error.to_string().contains("ABI discriminator"));
        assert!(
            !dropped.load(Ordering::Acquire),
            "the dylib token must remain loaded with the untrusted live instance"
        );

        // The production failure path intentionally leaks this opaque
        // instance. The test knows its concrete dummy type and may reclaim it.
        // SAFETY: `instance` came from Box<()> above and was never dereferenced.
        unsafe { drop(Box::from_raw(instance.cast::<()>())) };
    }

    #[test]
    fn malformed_vtable_releases_library_when_entry_returned_no_instance() {
        let dropped = Arc::new(AtomicBool::new(false));

        // SAFETY: a null instance carries no module-owned resources; rejecting
        // the malformed table may release its library token normally.
        let error = unsafe {
            FfiNetworkModule::from_entry_parts(
                ptr::null_mut(),
                SinomoNetworkModuleVtableV2::empty(),
                Box::new(KeepAliveDropProbe(Arc::clone(&dropped))),
            )
        }
        .err()
        .expect("malformed table must be rejected");

        assert!(error.to_string().contains("ABI discriminator"));
        assert!(dropped.load(Ordering::Acquire));
    }

    #[test]
    fn network_module_rpc_error_preserves_definitely_unsent_connect_timeout() {
        let encoded = encode_network_module_rpc_error(&NetworkError::timeout(
            "send iroh daemon command",
            NetworkTimeoutPhase::Connect,
            Duration::from_secs(2),
            NetworkDeliveryState::DefinitelyNotSent,
        ))
        .expect("encode module RPC error");

        let decoded = network_module_rpc_status_error(
            "dial network module rpc",
            SINOMO_NETWORK_MODULE_STATUS_RUNTIME_ERROR,
            &encoded,
        );

        assert_eq!(
            decoded,
            NetworkError::timeout(
                "dial network module rpc",
                NetworkTimeoutPhase::Connect,
                Duration::from_secs(2),
                NetworkDeliveryState::DefinitelyNotSent,
            )
        );
    }

    #[test]
    fn network_module_rpc_error_preserves_possibly_applied_delivery() {
        let encoded = encode_network_module_rpc_error(&NetworkError::delivery_failed(
            "send iroh daemon command",
            NetworkDeliveryState::PossiblyApplied,
            "peer closed before the response",
        ))
        .expect("encode module RPC error");

        let decoded = network_module_rpc_status_error(
            "dial network module rpc",
            SINOMO_NETWORK_MODULE_STATUS_RUNTIME_ERROR,
            &encoded,
        );

        assert_eq!(
            decoded.delivery_state(),
            Some(NetworkDeliveryState::PossiblyApplied)
        );
        assert!(matches!(decoded, NetworkError::DeliveryFailed { .. }));
        assert!(
            decoded
                .to_string()
                .contains("peer closed before the response")
        );
    }

    #[test]
    fn network_module_rpc_error_keeps_legacy_utf8_fallback() {
        let decoded = network_module_rpc_status_error(
            "dial network module rpc",
            SINOMO_NETWORK_MODULE_STATUS_RUNTIME_ERROR,
            b"legacy module failure",
        );

        assert_eq!(
            decoded,
            NetworkError::Io {
                operation: "dial network module rpc".to_string(),
                detail: "network module status 3 (runtime error): legacy module failure"
                    .to_string(),
            }
        );
    }
}
