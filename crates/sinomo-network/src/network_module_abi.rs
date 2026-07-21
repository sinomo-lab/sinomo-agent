//! Pinned `repr(C)` FFI contract for native-dylib network modules, plus the
//! host-side safe adapter ([`FfiNetworkModule`]).
//!
//! The byte slice/buffer structs mirror `sinomo-module-api`'s
//! (`ptr/len[/cap]`) but are defined locally: `sinomo-network` is an r0 crate
//! and must not depend on the r1 `sinomo-module-api` crate. Status codes also
//! mirror `sinomo-module-api` (0 ok, 1 invalid argument, 2 unsupported ABI,
//! 3 runtime error).
//!
//! Contract (v1):
//! - The dylib exports [`SINOMO_NETWORK_MODULE_ENTRY_SYMBOL`] with signature
//!   `extern "C" fn(*mut *mut c_void, *mut SinomoNetworkModuleVtableV1) -> i32`,
//!   creating a module instance and filling the vtable.
//! - All vtable functions must be callable from any thread; the instance is
//!   shared across threads by the host.
//! - Buffers written by the module (`descriptor`, authenticated metadata,
//!   stream errors, `local_endpoints`) are allocated by the module and must be released
//!   through the module's own `free_buffer` — allocators may differ across the
//!   dylib boundary.
//! - On a non-OK stream status the module fills the error buffer with a
//!   name-keyed SOBJ [`NetworkModuleStreamErrorV1`]. Other functions may fill the
//!   output buffer with UTF-8 error detail.
//! - Descriptor/endpoint payloads are name-keyed SOBJ documents encoded with
//!   [`encode_network_module_sobj`] (`SINOMO_NETWORK_MODULE_OBJECT_VERSION`).
//! - Timeouts are milliseconds; `0` means "no timeout".
//!
//! The entry symbol names the authenticated-link ABI. The table is self-describing,
//! and every callback slot is nullable until the host validates it.

use crate::direct_link::{AuthenticatedLink, AuthenticatedTransportMetadata, BoxedNetworkStream};
use crate::error::{NetworkDeliveryState, NetworkError, NetworkTimeoutPhase};
use crate::network_module::{
    NetworkFuture, NetworkModule, NetworkModuleContext, NetworkModuleDescriptor,
    NetworkModuleEndpointRole,
};
use crate::transfer::NetworkHealthPolicy;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sinomo_base::{ClusterEndpointRecord, Id};
use sinomo_node::NodeId;
use sinomo_object::{LengthMode, serde_ext as sinomo_object_serde};
use std::any::Any;
use std::ffi::c_void;
use std::ptr;
use std::slice;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// ABI discriminator filled into [`SinomoNetworkModuleVtableV1::abi_magic`].
pub const SINOMO_NETWORK_MODULE_ABI_MAGIC: u64 = u64::from_le_bytes(*b"SNMLINK1");
/// ABI version filled into [`SinomoNetworkModuleVtableV1::abi_version`].
pub const SINOMO_NETWORK_MODULE_ABI_VERSION: u32 = 1;
/// Exported entry symbol creating the instance and filling the vtable.
/// Not NUL-terminated; append `\0` (or let the loader do so) when resolving.
///
pub const SINOMO_NETWORK_MODULE_ENTRY_SYMBOL: &[u8] = b"sinomo_network_module_create_v1";
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

/// Start configuration passed to [`SinomoNetworkModuleVtableV1::start`].
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
/// pointer; it is valid until the matching `close_stream` call.
#[repr(transparent)]
#[derive(Clone, Copy, Debug)]
pub struct SinomoNetworkStreamV1 {
    pub ptr: *mut c_void,
}

impl SinomoNetworkStreamV1 {
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
unsafe impl Send for SinomoNetworkStreamV1 {}
// SAFETY: read and write callbacks may run concurrently for full duplex.
unsafe impl Sync for SinomoNetworkStreamV1 {}

pub type SinomoNetworkModuleDescriptorFn =
    unsafe extern "C" fn(*mut SinomoNetworkByteBuffer) -> SinomoNetworkModuleStatusCode;
pub type SinomoNetworkModuleStartFn = unsafe extern "C" fn(
    *mut c_void,
    *const SinomoNetworkModuleConfigV1,
) -> SinomoNetworkModuleStatusCode;
pub type SinomoNetworkModuleStopFn =
    unsafe extern "C" fn(*mut c_void) -> SinomoNetworkModuleStatusCode;
pub type SinomoNetworkModuleConnectLinkFn = unsafe extern "C" fn(
    *mut c_void,
    SinomoNetworkByteSlice,
    u64,
    u64,
    *mut SinomoNetworkStreamV1,
    *mut SinomoNetworkByteBuffer,
    *mut SinomoNetworkByteBuffer,
) -> SinomoNetworkModuleStatusCode;
pub type SinomoNetworkModuleReadStreamFn = unsafe extern "C" fn(
    SinomoNetworkStreamV1,
    *mut u8,
    usize,
    *mut usize,
    *mut SinomoNetworkByteBuffer,
) -> SinomoNetworkModuleStatusCode;
pub type SinomoNetworkModuleWriteStreamFn = unsafe extern "C" fn(
    SinomoNetworkStreamV1,
    SinomoNetworkByteSlice,
    *mut usize,
    *mut SinomoNetworkByteBuffer,
) -> SinomoNetworkModuleStatusCode;
pub type SinomoNetworkModuleShutdownStreamWriteFn =
    unsafe extern "C" fn(
        SinomoNetworkStreamV1,
        *mut SinomoNetworkByteBuffer,
    ) -> SinomoNetworkModuleStatusCode;
pub type SinomoNetworkModuleCloseStreamFn = unsafe extern "C" fn(SinomoNetworkStreamV1);
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
pub type SinomoNetworkModuleEndpointTransportIdentityFn =
    unsafe extern "C" fn(
        *mut c_void,
        SinomoNetworkByteSlice,
        *mut SinomoNetworkByteBuffer,
    ) -> SinomoNetworkModuleStatusCode;
pub type SinomoNetworkModuleVerifyAuthenticatedTransportMetadataFn =
    unsafe extern "C" fn(*mut c_void, SinomoNetworkByteSlice) -> SinomoNetworkModuleStatusCode;
pub type SinomoNetworkModuleFreeBufferFn = unsafe extern "C" fn(SinomoNetworkByteBuffer);
pub type SinomoNetworkModuleDestroyFn = unsafe extern "C" fn(*mut c_void);

/// The pinned v1 stream table a network module fills in its entry function.
///
/// Callback slots are `Option<fn>` so an all-zero/empty table is a valid Rust
/// value. The host starts from [`Self::empty`] and validates the discriminator,
/// exact structural size, and every required callback before constructing the
/// internal callable table. A module that writes only a short prefix can be
/// rejected without reading uninitialized memory.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SinomoNetworkModuleVtableV1 {
    pub abi_magic: u64,
    pub abi_version: u32,
    pub struct_size: u32,
    /// SOBJ NetworkModuleDescriptor into a module-owned buffer.
    pub descriptor: Option<SinomoNetworkModuleDescriptorFn>,
    pub start: Option<SinomoNetworkModuleStartFn>,
    pub stop: Option<SinomoNetworkModuleStopFn>,
    /// Authenticates the selected transport endpoint and opens the first raw
    /// bidirectional byte stream. The first output buffer is SOBJ
    /// [`AuthenticatedTransportMetadata`]. Application framing stays entirely
    /// on the host side. The blocking operation is bridged off the async
    /// executor by [`FfiNetworkModule`].
    pub connect_link: Option<SinomoNetworkModuleConnectLinkFn>,
    /// Blocking read into host-owned storage. `read_out == 0` means EOF.
    pub read_stream: Option<SinomoNetworkModuleReadStreamFn>,
    /// Blocking write from host-owned storage. Partial writes are allowed.
    pub write_stream: Option<SinomoNetworkModuleWriteStreamFn>,
    /// Flushes and half-closes the write side while preserving the read side.
    pub shutdown_stream_write: Option<SinomoNetworkModuleShutdownStreamWriteFn>,
    /// Releases the opaque stream. Called exactly once after all callbacks
    /// using the handle have returned.
    pub close_stream: Option<SinomoNetworkModuleCloseStreamFn>,
    /// SOBJ Vec<ClusterEndpointRecord> the module currently advertises.
    pub local_endpoints: Option<SinomoNetworkModuleLocalEndpointsFn>,
    /// Optional SOBJ endpoint -> SOBJ NetworkModuleEndpointRole classifier.
    /// Null means the module does not classify endpoint roles.
    pub endpoint_role: Option<SinomoNetworkModuleEndpointRoleFn>,
    /// Optional SOBJ endpoint -> SOBJ `Option<String>` transport principal.
    /// Address parsing stays in the module that owns the transport.
    pub endpoint_transport_identity: Option<SinomoNetworkModuleEndpointTransportIdentityFn>,
    /// Required SOBJ [`AuthenticatedTransportMetadata`] verifier. Returning
    /// OK means the module-owned proof binds the peer principal to the channel;
    /// every other status is an authentication failure.
    pub verify_authenticated_transport_metadata:
        Option<SinomoNetworkModuleVerifyAuthenticatedTransportMetadataFn>,
    pub free_buffer: Option<SinomoNetworkModuleFreeBufferFn>,
    pub destroy: Option<SinomoNetworkModuleDestroyFn>,
}

impl SinomoNetworkModuleVtableV1 {
    pub const fn empty() -> Self {
        Self {
            abi_magic: 0,
            abi_version: 0,
            struct_size: 0,
            descriptor: None,
            start: None,
            stop: None,
            connect_link: None,
            read_stream: None,
            write_stream: None,
            shutdown_stream_write: None,
            close_stream: None,
            local_endpoints: None,
            endpoint_role: None,
            endpoint_transport_identity: None,
            verify_authenticated_transport_metadata: None,
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
pub type SinomoNetworkModuleCreateV1Fn = unsafe extern "C" fn(
    *mut *mut c_void,
    *mut SinomoNetworkModuleVtableV1,
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

/// Structured error returned while a network module opens or drives a link stream.
///
/// Keeping delivery certainty across the dylib boundary is load-bearing: the
/// route layer may retry another endpoint when bytes were definitely not sent,
/// but must remain conservative after a possibly-applied request. The detail
/// string preserves the module-specific cause while the optional timeout keeps
/// phase-aware connectivity demotion working in the host.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NetworkModuleStreamErrorV1 {
    pub detail: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<NetworkModuleStreamTimeoutV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery_state: Option<NetworkDeliveryState>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NetworkModuleStreamTimeoutV1 {
    pub phase: NetworkTimeoutPhase,
    pub timeout_millis: u64,
}

impl NetworkModuleStreamErrorV1 {
    pub fn from_network_error(error: &NetworkError) -> Self {
        let timeout = match error {
            NetworkError::Timeout { phase, timeout, .. } => Some(NetworkModuleStreamTimeoutV1 {
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
pub fn encode_network_module_stream_error(error: &NetworkError) -> Result<Vec<u8>, NetworkError> {
    encode_network_module_sobj(&NetworkModuleStreamErrorV1::from_network_error(error))
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
    connect_link: SinomoNetworkModuleConnectLinkFn,
    read_stream: SinomoNetworkModuleReadStreamFn,
    write_stream: SinomoNetworkModuleWriteStreamFn,
    shutdown_stream_write: SinomoNetworkModuleShutdownStreamWriteFn,
    close_stream: SinomoNetworkModuleCloseStreamFn,
    local_endpoints: SinomoNetworkModuleLocalEndpointsFn,
    endpoint_role: Option<SinomoNetworkModuleEndpointRoleFn>,
    endpoint_transport_identity: Option<SinomoNetworkModuleEndpointTransportIdentityFn>,
    verify_authenticated_transport_metadata:
        SinomoNetworkModuleVerifyAuthenticatedTransportMetadataFn,
    free_buffer: SinomoNetworkModuleFreeBufferFn,
    destroy: SinomoNetworkModuleDestroyFn,
}

impl TryFrom<SinomoNetworkModuleVtableV1> for ValidatedNetworkModuleVtable {
    type Error = NetworkError;

    fn try_from(vtable: SinomoNetworkModuleVtableV1) -> Result<Self, Self::Error> {
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
        let expected_size = SinomoNetworkModuleVtableV1::expected_struct_size();
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
            connect_link: required!(connect_link),
            read_stream: required!(read_stream),
            write_stream: required!(write_stream),
            shutdown_stream_write: required!(shutdown_stream_write),
            close_stream: required!(close_stream),
            local_endpoints: required!(local_endpoints),
            endpoint_role: vtable.endpoint_role,
            endpoint_transport_identity: vtable.endpoint_transport_identity,
            verify_authenticated_transport_metadata: required!(
                verify_authenticated_transport_metadata
            ),
            free_buffer: required!(free_buffer),
            destroy: required!(destroy),
        })
    }
}

/// Shared ownership of one opaque module stream. Each bridge pump owns a
/// clone, so `close_stream` cannot race a blocking read or write. The
/// parent module/library is retained until after the stream is closed.
struct FfiNetworkModuleStreamShared {
    stream: SinomoNetworkStreamV1,
    module: Arc<FfiNetworkModuleShared>,
}

impl Drop for FfiNetworkModuleStreamShared {
    fn drop(&mut self) {
        // SAFETY: this is the last Arc owner, so every callback using the
        // opaque handle has returned. The handle came from connect_link and
        // is closed exactly once here.
        unsafe { (self.module.vtable.close_stream)(self.stream) };
    }
}

const FFI_NETWORK_STREAM_PIPE_BYTES: usize = 256 * 1024;
const FFI_NETWORK_STREAM_IO_BYTES: usize = 64 * 1024;

fn bridge_ffi_network_module_stream(
    opened: Arc<FfiNetworkModuleStreamShared>,
) -> BoxedNetworkStream {
    // The ABI operations are deliberately blocking. A bounded duplex pipe
    // gives callers normal AsyncRead+AsyncWrite while two independent pumps
    // retain full-duplex behavior and apply backpressure.
    let (caller, bridge) = tokio::io::duplex(FFI_NETWORK_STREAM_PIPE_BYTES);
    let (mut bridge_reader, mut bridge_writer) = tokio::io::split(bridge);

    let read_stream = opened.clone();
    tokio::spawn(async move {
        loop {
            let operation_stream = read_stream.clone();
            let read =
                tokio::task::spawn_blocking(move || ffi_network_stream_read(operation_stream))
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
            if bytes.is_empty() || bridge_writer.write_all(&bytes).await.is_err() {
                break;
            }
        }
        let _ = bridge_writer.shutdown().await;
    });

    let write_stream = opened;
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

    Box::new(caller)
}

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
    local_node_id: Mutex<Option<NodeId>>,
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
        vtable: SinomoNetworkModuleVtableV1,
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
        Ok(Self {
            shared,
            descriptor,
            local_node_id: Mutex::new(None),
        })
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
        *self
            .local_node_id
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(ctx.node_id.clone());
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
        self.local_node_id
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        Ok(())
    }

    fn connect_link(
        &self,
        endpoint: &ClusterEndpointRecord,
        health: NetworkHealthPolicy,
    ) -> NetworkFuture<AuthenticatedLink> {
        let module = self.shared.clone();
        let address = endpoint.address.clone();
        let peer_node_id = endpoint.node_id.clone();
        let endpoint_transport = endpoint.transport.clone();
        let expected_peer_transport_identity = self.endpoint_transport_identity(endpoint);
        let local_node_id = self
            .local_node_id
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let connect_ms = network_module_timeout_ms(Some(health.connect_timeout));
        let idle_ms = network_module_timeout_ms(Some(health.no_progress_timeout));
        Box::pin(async move {
            let local_node_id = local_node_id.ok_or_else(|| NetworkError::Io {
                operation: "connect network module authenticated link".to_string(),
                detail: "network module is not started with a local node identity".to_string(),
            })?;
            let expected_peer_transport_identity = expected_peer_transport_identity.ok_or_else(|| {
                NetworkError::Io {
                    operation: "connect network module authenticated link".to_string(),
                    detail: format!(
                        "network module cannot derive the authenticated transport identity for `{address}`"
                    ),
                }
            })?;
            let opened_module = module.clone();
            let (opened, metadata) = tokio::task::spawn_blocking(move || {
                let mut stream = SinomoNetworkStreamV1::null();
                let mut metadata = SinomoNetworkByteBuffer::null();
                let mut error = SinomoNetworkByteBuffer::null();
                // SAFETY: the module/library is retained by `opened_module`;
                // address bytes live through the call and all output slots
                // are valid.
                let status = unsafe {
                    (opened_module.vtable.connect_link)(
                        opened_module.instance.0,
                        SinomoNetworkByteSlice::from_slice(address.as_bytes()),
                        connect_ms,
                        idle_ms,
                        &mut stream,
                        &mut metadata,
                        &mut error,
                    )
                };
                // SAFETY: the metadata/error buffers were allocated by this module.
                let metadata = unsafe {
                    copy_and_free_network_buffer(metadata, opened_module.vtable.free_buffer)
                };
                // SAFETY: any error buffer was allocated by this module.
                let detail = unsafe {
                    copy_and_free_network_buffer(error, opened_module.vtable.free_buffer)
                };
                if status != SINOMO_NETWORK_MODULE_STATUS_OK {
                    if !stream.is_null() {
                        // SAFETY: a defensive close for a malformed module
                        // that returned a handle alongside a failing status.
                        unsafe { (opened_module.vtable.close_stream)(stream) };
                    }
                    return Err(network_module_stream_status_error(
                        "connect network module authenticated link",
                        status,
                        &detail,
                    ));
                }
                if stream.is_null() {
                    return Err(NetworkError::Io {
                        operation: "connect network module authenticated link".to_string(),
                        detail: "network module returned a null stream handle".to_string(),
                    });
                }
                // Own the opaque handle before decoding untrusted module
                // metadata so every validation failure closes it exactly once.
                let opened = Arc::new(FfiNetworkModuleStreamShared {
                    stream,
                    module: opened_module,
                });
                let metadata: AuthenticatedTransportMetadata =
                    decode_network_module_sobj(&metadata)?;
                let metadata = AuthenticatedTransportMetadata::try_new(
                    metadata.transport,
                    metadata.peer_transport_identity,
                    metadata.channel_binding,
                    metadata.transport_proof,
                    metadata.capabilities,
                )?;
                if metadata.transport != endpoint_transport {
                    return Err(NetworkError::Io {
                        operation: "connect network module authenticated link".to_string(),
                        detail: format!(
                            "module authenticated transport {:?}, expected {:?}",
                            metadata.transport, endpoint_transport
                        ),
                    });
                }
                if metadata.peer_transport_identity != expected_peer_transport_identity {
                    return Err(NetworkError::Io {
                        operation: "connect network module authenticated link".to_string(),
                        detail: "module authenticated a different transport principal than the selected endpoint"
                            .to_string(),
                    });
                }
                let metadata_bytes = encode_network_module_sobj(&metadata)?;
                // SAFETY: `opened` retains the module instance/library and the
                // encoded metadata view lives through this synchronous call.
                let proof_status = unsafe {
                    (opened
                        .module
                        .vtable
                        .verify_authenticated_transport_metadata)(
                        opened.module.instance.0,
                        SinomoNetworkByteSlice::from_slice(&metadata_bytes),
                    )
                };
                if proof_status != SINOMO_NETWORK_MODULE_STATUS_OK {
                    return Err(NetworkError::AuthDenied);
                }
                Ok((opened, metadata))
            })
            .await
            .map_err(|source| NetworkError::Io {
                operation: "connect network module authenticated link".to_string(),
                detail: source.to_string(),
            })??;

            // Do not start the blocking bridge until routing actually opens a
            // stream. Dropping an unused link therefore closes the opaque
            // module handle instead of leaving a background read pending.
            let opened = Arc::new(Mutex::new(Some(opened)));
            let opener_stream = opened.clone();
            let mut link_identity = Vec::new();
            link_identity.extend_from_slice(local_node_id.as_str().as_bytes());
            link_identity.push(0);
            link_identity.extend_from_slice(peer_node_id.as_str().as_bytes());
            link_identity.push(0);
            link_identity.extend_from_slice(&metadata.channel_binding);
            let link_id = Id::derived("sinomo.network.module-authenticated-link", &link_identity);
            AuthenticatedLink::new(
                local_node_id,
                peer_node_id,
                link_id,
                metadata.channel_binding,
                metadata.capabilities,
                move || {
                    let opened = opener_stream
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .take();
                    Box::pin(async move {
                        let opened = opened.ok_or_else(|| NetworkError::Io {
                            operation: "open network module authenticated stream".to_string(),
                            detail: "this ABI link exposes exactly one stream".to_string(),
                        })?;
                        Ok(bridge_ffi_network_module_stream(opened))
                    })
                },
            )
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

    fn endpoint_transport_identity(&self, endpoint: &ClusterEndpointRecord) -> Option<String> {
        let endpoint_transport_identity = self.shared.vtable.endpoint_transport_identity?;
        let endpoint_bytes = encode_network_module_sobj(endpoint).ok()?;
        let mut buffer = SinomoNetworkByteBuffer::null();
        // SAFETY: instance/vtable are valid per the entry contract; the input
        // byte view borrows local encoded bytes for the duration of the call.
        let status = unsafe {
            endpoint_transport_identity(
                self.shared.instance.0,
                SinomoNetworkByteSlice::from_slice(&endpoint_bytes),
                &mut buffer,
            )
        };
        // SAFETY: buffer was produced by the module; released via its
        // free_buffer.
        let bytes = unsafe { copy_and_free_network_buffer(buffer, self.shared.vtable.free_buffer) };
        if status != SINOMO_NETWORK_MODULE_STATUS_OK || bytes.is_empty() {
            return None;
        }
        decode_network_module_sobj::<Option<String>>(&bytes)
            .ok()
            .flatten()
    }

    fn verify_authenticated_transport_metadata(
        &self,
        metadata: &AuthenticatedTransportMetadata,
    ) -> bool {
        let Ok(metadata) = encode_network_module_sobj(metadata) else {
            return false;
        };
        // SAFETY: instance/vtable are valid per the entry contract; the byte
        // view borrows local encoded metadata for the duration of the call.
        unsafe {
            (self.shared.vtable.verify_authenticated_transport_metadata)(
                self.shared.instance.0,
                SinomoNetworkByteSlice::from_slice(&metadata),
            ) == SINOMO_NETWORK_MODULE_STATUS_OK
        }
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
        (shared.module.vtable.read_stream)(
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
        return Err(network_module_stream_status_error(
            "read network module stream",
            status,
            &detail,
        ));
    }
    if read > bytes.len() {
        return Err(NetworkError::Io {
            operation: "read network module stream".to_string(),
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
            (shared.module.vtable.write_stream)(
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
            return Err(network_module_stream_status_error(
                "write network module stream",
                status,
                &detail,
            ));
        }
        if written == 0 || written > bytes.len() - offset {
            return Err(NetworkError::Io {
                operation: "write network module stream".to_string(),
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
    let status = unsafe { (shared.module.vtable.shutdown_stream_write)(shared.stream, &mut error) };
    // SAFETY: any error buffer was allocated by this module.
    let detail = unsafe { copy_and_free_network_buffer(error, shared.module.vtable.free_buffer) };
    if status != SINOMO_NETWORK_MODULE_STATUS_OK {
        return Err(network_module_stream_status_error(
            "shutdown network module stream",
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

fn network_module_stream_status_error(
    operation: &str,
    status: SinomoNetworkModuleStatusCode,
    detail_bytes: &[u8],
) -> NetworkError {
    match decode_network_module_sobj::<NetworkModuleStreamErrorV1>(detail_bytes) {
        Ok(error) => error.into_network_error(operation),
        Err(error) => NetworkError::Io {
            operation: operation.to_string(),
            detail: format!(
                "network module status {status} returned an invalid v1 stream error: {error}"
            ),
        },
    }
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
    fn network_module_v1_rejects_zero_initialized_and_partial_tables() {
        let empty = SinomoNetworkModuleVtableV1::empty();
        let empty_error = ValidatedNetworkModuleVtable::try_from(empty)
            .expect_err("zero-initialized table must not be callable");
        assert!(empty_error.to_string().contains("ABI discriminator"));

        let mut partial = SinomoNetworkModuleVtableV1::empty();
        partial.abi_magic = SINOMO_NETWORK_MODULE_ABI_MAGIC;
        partial.abi_version = SINOMO_NETWORK_MODULE_ABI_VERSION;
        partial.struct_size = SinomoNetworkModuleVtableV1::expected_struct_size();
        let partial_error = ValidatedNetworkModuleVtable::try_from(partial)
            .expect_err("matching header without callbacks must be rejected");
        assert!(partial_error.to_string().contains("`descriptor`"));
    }

    #[test]
    fn network_module_v1_rejects_undersized_table_before_callbacks() {
        let mut undersized = SinomoNetworkModuleVtableV1::empty();
        undersized.abi_magic = SINOMO_NETWORK_MODULE_ABI_MAGIC;
        undersized.abi_version = SINOMO_NETWORK_MODULE_ABI_VERSION;
        undersized.struct_size = SinomoNetworkModuleVtableV1::expected_struct_size() - 1;

        let error = ValidatedNetworkModuleVtable::try_from(undersized)
            .expect_err("undersized table shape must be rejected");
        assert!(error.to_string().contains("vtable size"));
        assert_eq!(
            SINOMO_NETWORK_MODULE_ENTRY_SYMBOL,
            b"sinomo_network_module_create_v1"
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
                SinomoNetworkModuleVtableV1::empty(),
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
                SinomoNetworkModuleVtableV1::empty(),
                Box::new(KeepAliveDropProbe(Arc::clone(&dropped))),
            )
        }
        .err()
        .expect("malformed table must be rejected");

        assert!(error.to_string().contains("ABI discriminator"));
        assert!(dropped.load(Ordering::Acquire));
    }

    #[test]
    fn network_module_stream_error_preserves_definitely_unsent_connect_timeout() {
        let encoded = encode_network_module_stream_error(&NetworkError::timeout(
            "send iroh daemon command",
            NetworkTimeoutPhase::Connect,
            Duration::from_secs(2),
            NetworkDeliveryState::DefinitelyNotSent,
        ))
        .expect("encode module stream error");

        let decoded = network_module_stream_status_error(
            "connect network module stream",
            SINOMO_NETWORK_MODULE_STATUS_RUNTIME_ERROR,
            &encoded,
        );

        assert_eq!(
            decoded,
            NetworkError::timeout(
                "connect network module stream",
                NetworkTimeoutPhase::Connect,
                Duration::from_secs(2),
                NetworkDeliveryState::DefinitelyNotSent,
            )
        );
    }

    #[test]
    fn network_module_stream_error_preserves_possibly_applied_delivery() {
        let encoded = encode_network_module_stream_error(&NetworkError::delivery_failed(
            "send iroh daemon command",
            NetworkDeliveryState::PossiblyApplied,
            "peer closed before the response",
        ))
        .expect("encode module stream error");

        let decoded = network_module_stream_status_error(
            "connect network module stream",
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
    fn network_module_stream_error_rejects_non_sobj_detail() {
        let decoded = network_module_stream_status_error(
            "connect network module stream",
            SINOMO_NETWORK_MODULE_STATUS_RUNTIME_ERROR,
            b"plain text module failure",
        );

        assert!(matches!(
            decoded,
            NetworkError::Io { operation, detail }
                if operation == "connect network module stream"
                    && detail.starts_with(
                        "network module status 3 returned an invalid v1 stream error:"
                    )
                    && !detail.contains("plain text module failure")
        ));
    }
}
