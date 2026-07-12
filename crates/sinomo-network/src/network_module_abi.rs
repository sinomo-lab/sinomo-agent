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
//! - Buffers written by the module (`descriptor`, `dial_rpc` response,
//!   `local_endpoints`) are allocated by the module and must be released
//!   through the module's own `free_buffer` — allocators may differ across the
//!   dylib boundary.
//! - On a non-OK `dial_rpc` status the module SHOULD fill the output buffer
//!   with a name-keyed SOBJ [`NetworkModuleRpcErrorV1`]. The host retains a
//!   legacy UTF-8 fallback for older modules. Other functions MAY fill the
//!   output buffer with UTF-8 error detail.
//! - Descriptor/endpoint payloads are name-keyed SOBJ documents encoded with
//!   [`encode_network_module_sobj`] (`SINOMO_NETWORK_MODULE_OBJECT_VERSION`).
//! - Timeouts are milliseconds; `0` means "no timeout".
//!
//! Report deviations rather than changing this ABI: it is pinned for stage C
//! of docs/plans/r0-r1-crate-boundary-refactor.md.

use crate::error::{NetworkDeliveryState, NetworkError, NetworkTimeoutPhase};
use crate::network_module::{
    NetworkFuture, NetworkModule, NetworkModuleContext, NetworkModuleDescriptor,
    NetworkModuleEndpointRole,
};
use crate::object::DaemonRpcTimeouts;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sinomo_base::ClusterEndpointRecord;
use sinomo_object::{LengthMode, serde_ext as sinomo_object_serde};
use std::any::Any;
use std::ffi::c_void;
use std::ptr;
use std::slice;
use std::sync::Arc;
use std::time::Duration;

/// ABI version filled into [`SinomoNetworkModuleVtableV1::abi_version`].
pub const SINOMO_NETWORK_MODULE_ABI_VERSION: u32 = 1;
/// Exported entry symbol creating the instance and filling the vtable.
/// Not NUL-terminated; append `\0` (or let the loader do so) when resolving.
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

/// The pinned v1 vtable a network module fills in its entry function.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SinomoNetworkModuleVtableV1 {
    pub abi_version: u32,
    /// SOBJ NetworkModuleDescriptor into a module-owned buffer.
    pub descriptor:
        unsafe extern "C" fn(*mut SinomoNetworkByteBuffer) -> SinomoNetworkModuleStatusCode,
    pub start: unsafe extern "C" fn(
        *mut c_void,
        *const SinomoNetworkModuleConfigV1,
    ) -> SinomoNetworkModuleStatusCode,
    pub stop: unsafe extern "C" fn(*mut c_void) -> SinomoNetworkModuleStatusCode,
    /// Blocking request/response dial: endpoint address (UTF-8), request
    /// bytes, timeouts (connect_ms, idle_ms); response into module buffer.
    /// The host wraps calls in spawn_blocking.
    pub dial_rpc: unsafe extern "C" fn(
        *mut c_void,
        SinomoNetworkByteSlice,       // endpoint address utf8
        SinomoNetworkByteSlice,       // request frame bytes
        u64,                          // connect timeout ms
        u64,                          // idle timeout ms
        *mut SinomoNetworkByteBuffer, // response frame bytes
    ) -> SinomoNetworkModuleStatusCode,
    /// SOBJ Vec<ClusterEndpointRecord> the module currently advertises.
    pub local_endpoints: unsafe extern "C" fn(
        *mut c_void,
        *mut SinomoNetworkByteBuffer,
    ) -> SinomoNetworkModuleStatusCode,
    /// Optional SOBJ endpoint -> SOBJ NetworkModuleEndpointRole classifier.
    /// Null means the module does not classify endpoint roles.
    pub endpoint_role: Option<
        unsafe extern "C" fn(
            *mut c_void,
            SinomoNetworkByteSlice,
            *mut SinomoNetworkByteBuffer,
        ) -> SinomoNetworkModuleStatusCode,
    >,
    pub free_buffer: unsafe extern "C" fn(SinomoNetworkByteBuffer),
    pub destroy: unsafe extern "C" fn(*mut c_void),
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
/// keep the instance and library alive until they finish. `destroy` runs when
/// the LAST owner drops; field order keeps `_keep_alive` alive until after
/// `destroy` returns.
struct FfiNetworkModuleShared {
    instance: FfiNetworkModuleInstance,
    vtable: SinomoNetworkModuleVtableV1,
    /// Dropped last (field order): keeps the dylib mapped until after
    /// `destroy` runs in [`Drop::drop`].
    _keep_alive: Box<dyn Any + Send + Sync>,
}

impl Drop for FfiNetworkModuleShared {
    fn drop(&mut self) {
        // SAFETY: this shared state owns the instance; destroy is called
        // exactly once (last Arc owner), and `_keep_alive` (declared after
        // `instance`/`vtable`) keeps the library mapped until this returns.
        unsafe { (self.vtable.destroy)(self.instance.0) };
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
    /// - Ownership of `instance` transfers to the returned adapter, which
    ///   calls `vtable.destroy` on drop.
    pub unsafe fn from_entry_parts(
        instance: *mut c_void,
        vtable: SinomoNetworkModuleVtableV1,
        keep_alive: Box<dyn Any + Send + Sync>,
    ) -> Result<Self, NetworkError> {
        if vtable.abi_version != SINOMO_NETWORK_MODULE_ABI_VERSION {
            return Err(NetworkError::Io {
                operation: "load network module".to_string(),
                detail: format!(
                    "unsupported network module ABI version {} (host expects {})",
                    vtable.abi_version, SINOMO_NETWORK_MODULE_ABI_VERSION
                ),
            });
        }
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
        // version and a non-null instance — leaking there is the safe choice.
        let shared = Arc::new(FfiNetworkModuleShared {
            instance: FfiNetworkModuleInstance(instance),
            vtable,
            _keep_alive: keep_alive,
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

    fn dial_rpc(
        &self,
        endpoint: &ClusterEndpointRecord,
        request: &[u8],
        timeouts: DaemonRpcTimeouts,
    ) -> NetworkFuture<Vec<u8>> {
        let shared = self.shared.clone();
        let address = endpoint.address.clone();
        let request = request.to_vec();
        let connect_ms = network_module_timeout_ms(timeouts.connect_timeout);
        let idle_ms = network_module_timeout_ms(timeouts.idle_timeout);
        Box::pin(async move {
            // The module dial is blocking by contract; bridge it off the
            // async worker threads. The closure owns a clone of the shared
            // state, so the instance and its library stay alive across the
            // call even if the module is dropped or replaced mid-dial.
            tokio::task::spawn_blocking(move || {
                let mut response = SinomoNetworkByteBuffer::null();
                // SAFETY: the owned `shared` keeps instance/vtable/library
                // valid for the duration of this call; the byte views borrow
                // `address` and `request`, which live across the call.
                let status = unsafe {
                    (shared.vtable.dial_rpc)(
                        shared.instance.0,
                        SinomoNetworkByteSlice::from_slice(address.as_bytes()),
                        SinomoNetworkByteSlice::from_slice(&request),
                        connect_ms,
                        idle_ms,
                        &mut response,
                    )
                };
                // SAFETY: `response` was filled (or left null) by the module
                // and is released through the module's own free_buffer.
                let bytes =
                    unsafe { copy_and_free_network_buffer(response, shared.vtable.free_buffer) };
                if status != SINOMO_NETWORK_MODULE_STATUS_OK {
                    return Err(network_module_rpc_status_error(
                        "dial network module rpc",
                        status,
                        &bytes,
                    ));
                }
                Ok(bytes)
            })
            .await
            .map_err(|source| NetworkError::Io {
                operation: "dial network module rpc".to_string(),
                detail: source.to_string(),
            })?
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
