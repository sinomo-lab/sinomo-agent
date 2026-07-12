//! Iroh roaming transport as a trusted native-dylib network module.
//!
//! This module owns every Iroh-specific behavior: endpoint builder +
//! relay-mode config, secret-key load/create, endpoint record parse/format,
//! stable endpoint ids, endpoint record file I/O, the daemon proxy
//! supervisor/accept loop/frame budget, and the dial client. `sinomo-network`
//! only sees generic network-module descriptors, endpoint records, and the
//! optional endpoint-role classification hook.
//!
//! The dylib exports two ABI surfaces:
//! - the standard `sinomo-module-api` symbols so it installs/registers like
//!   any native-dylib module (its descriptor surfaces declare
//!   `network_transports: ["iroh"]`, telling the daemon to also resolve the
//!   network entry);
//! - the pinned network-module vtable entry
//!   `sinomo_network_module_create_v1` (see
//!   `sinomo_network::network_module_abi`), which the daemon wraps in
//!   `FfiNetworkModule` and registers in its `NetworkModuleRegistry`.

mod dial;
mod endpoint;
mod frames;
mod proxy;

use endpoint::{iroh_endpoint_public_key, stable_iroh_endpoint_id};
use sinomo_core::ClusterTransportKind;
use sinomo_core::network::network_module_abi::{
    SINOMO_NETWORK_MODULE_ABI_VERSION, SINOMO_NETWORK_MODULE_STATUS_INVALID_ARGUMENT,
    SINOMO_NETWORK_MODULE_STATUS_OK, SINOMO_NETWORK_MODULE_STATUS_RUNTIME_ERROR,
    SinomoNetworkByteBuffer, SinomoNetworkByteSlice, SinomoNetworkModuleConfigV1,
    SinomoNetworkModuleStatusCode, SinomoNetworkModuleVtableV1, encode_network_module_rpc_error,
    encode_network_module_sobj,
};
use sinomo_core::network::{
    ClusterPaths, DaemonRpcTimeouts, NetworkError, NetworkModuleDescriptor,
    NetworkModuleEndpointRole,
};
use sinomo_core::node::{ClusterId, NodeId, shadow_daemon_node_id};
use sinomo_module_api::{
    NativeModuleBackend, NativeModuleCapabilitiesResponse, NativeModuleDescriptor,
    NativeModuleErrorResponse, NativeModuleRequest, NativeModuleResponse, NativeModuleSurfaces,
    SINOMO_MODULE_STATUS_INVALID_ARGUMENT, SINOMO_MODULE_STATUS_OK,
    SINOMO_MODULE_STATUS_RUNTIME_ERROR, SINOMO_MODULE_STATUS_UNSUPPORTED_ABI,
    SINOMO_NATIVE_MODULE_ABI_VERSION, SinomoModuleByteBuffer, SinomoModuleByteSlice,
    SinomoModuleHostApiV1, SinomoModuleStatusCode, decode_sobj, encode_sobj,
};
use std::ffi::c_void;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

/// Module id under which the dylib installs (`[package.metadata.sinomo.native-dylib]`).
pub const SINOMO_NETWORK_IROH_MODULE_ID: &str = "module.network-iroh";
/// Interface version pinned in the loader state.
pub const SINOMO_NETWORK_IROH_INTERFACE_VERSION: &str = "sinomo.network-iroh.v1";
/// Network-module descriptor id (`NetworkModuleDescriptor::id`).
pub const SINOMO_NETWORK_IROH_NETWORK_MODULE_ID: &str = "iroh";
pub const IROH_DAEMON_COMMAND_MAX_BYTES: usize =
    sinomo_core::network::DAEMON_CONTROL_RPC_MAX_FRAME_BYTES;
const SINOMO_DISABLE_IROH_ENV: &str = "SINOMO_DISABLE_IROH";

fn iroh_disabled_by_env() -> bool {
    std::env::var(SINOMO_DISABLE_IROH_ENV).ok().as_deref() == Some("1")
}

// ---------------------------------------------------------------------------
// Standard sinomo-module-api surface
// ---------------------------------------------------------------------------

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_api_version() -> u32 {
    SINOMO_NATIVE_MODULE_ABI_VERSION
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_descriptor(
    out: *mut SinomoModuleByteBuffer,
) -> SinomoModuleStatusCode {
    write_module_buffer(out, encode_sobj(&iroh_native_module_descriptor()))
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_init(host: SinomoModuleHostApiV1) -> SinomoModuleStatusCode {
    if host.abi_version != SINOMO_NATIVE_MODULE_ABI_VERSION {
        return SINOMO_MODULE_STATUS_UNSUPPORTED_ABI;
    }
    SINOMO_MODULE_STATUS_OK
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
        Err(_) => write_module_response(
            response_sobj,
            NativeModuleResponse::Error(NativeModuleErrorResponse {
                code: "panic".to_string(),
                detail: "iroh network module panicked while handling invocation".to_string(),
            }),
            SINOMO_MODULE_STATUS_RUNTIME_ERROR,
        ),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_shutdown() -> SinomoModuleStatusCode {
    SINOMO_MODULE_STATUS_OK
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_free_buffer(buffer: SinomoModuleByteBuffer) {
    // SAFETY: the host only frees buffers this module produced via
    // SinomoModuleByteBuffer::from_vec in write_module_buffer below.
    unsafe {
        drop(SinomoModuleByteBuffer::into_vec(buffer));
    }
}

/// The standard native-dylib descriptor: no invokable operations — the
/// module's behavior is exposed exclusively through the network vtable —
/// but `surfaces.network_transports` marks it as a network module.
pub fn iroh_native_module_descriptor() -> NativeModuleDescriptor {
    NativeModuleDescriptor {
        module_id: SINOMO_NETWORK_IROH_MODULE_ID.to_string(),
        module_version: env!("CARGO_PKG_VERSION").to_string(),
        owner: "sinomo-network-iroh".to_string(),
        native_abi_version: SINOMO_NATIVE_MODULE_ABI_VERSION,
        backend: NativeModuleBackend::NativeDylib,
        interface_version: SINOMO_NETWORK_IROH_INTERFACE_VERSION.to_string(),
        platform_triple: platform_triple(),
        supported_operations: Vec::new(),
        capabilities: Vec::new(),
        surfaces: NativeModuleSurfaces {
            network_transports: vec![SINOMO_NETWORK_IROH_NETWORK_MODULE_ID.to_string()],
            ..Default::default()
        },
    }
}

fn invoke_unchecked(
    request_sobj: SinomoModuleByteSlice,
    response_sobj: *mut SinomoModuleByteBuffer,
) -> SinomoModuleStatusCode {
    // SAFETY: the host guarantees the request view is valid for the call.
    let request_bytes = unsafe { request_sobj.as_slice() };
    let response = match decode_sobj::<NativeModuleRequest>(request_bytes) {
        Ok(NativeModuleRequest::ProbeCapabilities(_)) => {
            NativeModuleResponse::Capabilities(NativeModuleCapabilitiesResponse {
                descriptor: iroh_native_module_descriptor(),
            })
        }
        Ok(NativeModuleRequest::Invoke(request)) => {
            NativeModuleResponse::Error(NativeModuleErrorResponse {
                code: "unsupported-operation".to_string(),
                detail: format!(
                    "iroh network module has no invokable operations (got `{}`); \
                     it is driven through the network-module vtable",
                    request.operation
                ),
            })
        }
        Err(error) => NativeModuleResponse::Error(NativeModuleErrorResponse {
            code: "invalid-request".to_string(),
            detail: format!("decode iroh network module request: {error}"),
        }),
    };
    write_module_response(response_sobj, response, SINOMO_MODULE_STATUS_OK)
}

fn write_module_response(
    out: *mut SinomoModuleByteBuffer,
    response: NativeModuleResponse,
    fallback_status: SinomoModuleStatusCode,
) -> SinomoModuleStatusCode {
    match encode_sobj(&response) {
        Ok(bytes) => write_module_buffer(out, Ok(bytes)),
        Err(error) => write_module_buffer(out, Err(error)).max(fallback_status),
    }
}

fn write_module_buffer(
    out: *mut SinomoModuleByteBuffer,
    bytes: Result<Vec<u8>, sinomo_module_api::NativeModuleApiError>,
) -> SinomoModuleStatusCode {
    let Ok(bytes) = bytes else {
        return SINOMO_MODULE_STATUS_INVALID_ARGUMENT;
    };
    if out.is_null() {
        return SINOMO_MODULE_STATUS_INVALID_ARGUMENT;
    }
    // SAFETY: `out` is a valid host-provided output slot per the module ABI.
    unsafe {
        *out = SinomoModuleByteBuffer::from_vec(bytes);
    }
    SINOMO_MODULE_STATUS_OK
}

fn platform_triple() -> String {
    format!(
        "{}-{}-{}",
        std::env::consts::ARCH,
        std::env::consts::OS,
        std::env::consts::FAMILY
    )
}

// ---------------------------------------------------------------------------
// Network-module vtable surface
// ---------------------------------------------------------------------------

/// The network-module descriptor advertised through the vtable.
pub fn iroh_network_module_descriptor() -> NetworkModuleDescriptor {
    NetworkModuleDescriptor {
        id: SINOMO_NETWORK_IROH_NETWORK_MODULE_ID.to_string(),
        transports: vec![ClusterTransportKind::Iroh],
        address_schemes: vec!["iroh".to_string()],
        dialer: true,
        listener: true,
        supports_runtime_toggle: true,
    }
}

/// One loaded module instance: a lazily created private tokio runtime and the
/// currently running proxy (when started with a cluster identity).
struct IrohNetworkModuleInstance {
    runtime: OnceLock<tokio::runtime::Runtime>,
    proxy: Mutex<Option<proxy::IrohDaemonProxy>>,
    dialer: dial::IrohDaemonDialer,
}

impl IrohNetworkModuleInstance {
    fn new() -> Self {
        Self {
            runtime: OnceLock::new(),
            proxy: Mutex::new(None),
            dialer: dial::IrohDaemonDialer::new(),
        }
    }

    /// The module's own runtime (the proxy supervisor and blocking dials run
    /// here, never on the host's executor).
    fn runtime(&self) -> Result<&tokio::runtime::Runtime, NetworkError> {
        if let Some(runtime) = self.runtime.get() {
            return Ok(runtime);
        }
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("sinomo-iroh-module")
            .enable_all()
            .build()
            .map_err(|source| NetworkError::Io {
                operation: "build iroh network module runtime".to_string(),
                detail: source.to_string(),
            })?;
        let _ = self.runtime.set(runtime);
        Ok(self.runtime.get().expect("runtime just initialized"))
    }

    fn start(&self, config: IrohModuleStartConfig) -> Result<(), NetworkError> {
        // Restart semantics: stop a running proxy before starting anew.
        self.stop()?;
        if iroh_disabled_by_env() {
            eprintln!(
                "sinomo iroh network module listener disabled by {SINOMO_DISABLE_IROH_ENV}=1"
            );
            return Ok(());
        }
        let Some(cluster_id) = config.cluster_id else {
            // Pre-cluster start: the listener/proxy needs a cluster identity
            // for its endpoint record; dialing works without start(). Treat
            // as a successful dial-only start.
            eprintln!(
                "sinomo iroh network module started dial-only (no cluster id yet); \
                 proxy listener deferred"
            );
            return Ok(());
        };
        let runtime = self.runtime()?;
        let proxy_config = proxy::IrohProxyConfig {
            paths: cluster_paths_from_root(config.cluster_root),
            cluster_id,
            node_id: config.node_id,
            daemon_forward_address: config.daemon_forward_address,
        };
        let handle = runtime.block_on(proxy::start_iroh_daemon_proxy(
            runtime.handle().clone(),
            proxy_config,
        ))?;
        *self
            .proxy
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(handle);
        Ok(())
    }

    fn stop(&self) -> Result<(), NetworkError> {
        let proxy = self
            .proxy
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        let runtime = self.runtime()?;
        if let Some(proxy) = proxy {
            runtime.block_on(proxy.shutdown());
        }
        runtime.block_on(self.dialer.shutdown());
        Ok(())
    }

    fn dial(
        &self,
        address: &str,
        request_frame: &[u8],
        timeouts: DaemonRpcTimeouts,
    ) -> Result<Vec<u8>, NetworkError> {
        if iroh_disabled_by_env() {
            return Err(NetworkError::Io {
                operation: "dial iroh network module".to_string(),
                detail: format!("{SINOMO_DISABLE_IROH_ENV}=1"),
            });
        }
        let runtime = self.runtime()?;
        runtime.block_on(self.dialer.dial_daemon_control_frame_async(
            address,
            request_frame,
            timeouts,
        ))
    }

    fn local_endpoints(&self) -> Vec<sinomo_core::ClusterEndpointRecord> {
        self.proxy
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .map(|proxy| vec![proxy.record().endpoint()])
            .unwrap_or_default()
    }
}

fn iroh_endpoint_role(endpoint: &sinomo_core::ClusterEndpointRecord) -> NetworkModuleEndpointRole {
    if endpoint.transport != ClusterTransportKind::Iroh {
        return NetworkModuleEndpointRole::Unknown;
    }
    let Some(public_key) = iroh_endpoint_public_key(endpoint) else {
        return NetworkModuleEndpointRole::Unknown;
    };
    let shadow_node_id = shadow_daemon_node_id(&endpoint.node_id);
    if endpoint.endpoint_id == stable_iroh_endpoint_id(&shadow_node_id, &public_key) {
        return NetworkModuleEndpointRole::ShadowDaemon;
    }
    if endpoint.endpoint_id == stable_iroh_endpoint_id(&endpoint.node_id, &public_key) {
        return NetworkModuleEndpointRole::MainDaemon;
    }
    NetworkModuleEndpointRole::Unknown
}

/// Parsed start config (from the FFI config struct's UTF-8 views).
struct IrohModuleStartConfig {
    cluster_root: PathBuf,
    node_id: NodeId,
    cluster_id: Option<ClusterId>,
    daemon_forward_address: String,
}

/// Rebuilds `ClusterPaths` from the cluster root the host passed. Field
/// layout mirrors `ClusterPaths::new` (which derives the same paths from the
/// data dir).
fn cluster_paths_from_root(root: PathBuf) -> ClusterPaths {
    ClusterPaths {
        state: root.join("state.sobj"),
        connectivity_local: root.join("connectivity-local.sobj"),
        root,
    }
}

fn utf8_config_field(slice: SinomoNetworkByteSlice, field: &str) -> Result<String, NetworkError> {
    // SAFETY: the host guarantees config views are valid for the start call.
    let bytes = unsafe { slice.as_slice() };
    String::from_utf8(bytes.to_vec()).map_err(|_| NetworkError::Serialization {
        detail: format!("network module config field `{field}` is not UTF-8"),
    })
}

fn parse_start_config(
    config: *const SinomoNetworkModuleConfigV1,
) -> Result<IrohModuleStartConfig, NetworkError> {
    // SAFETY: the host passes a valid config pointer for the start call.
    let config = unsafe { config.as_ref() }.ok_or_else(|| NetworkError::Serialization {
        detail: "network module start config is null".to_string(),
    })?;
    let cluster_root = PathBuf::from(utf8_config_field(config.cluster_root_utf8, "cluster_root")?);
    let node_id_text = utf8_config_field(config.node_id_utf8, "node_id")?;
    let node_id =
        NodeId::from_str(&node_id_text).map_err(|source| NetworkError::Serialization {
            detail: format!("network module config node id `{node_id_text}` is invalid: {source}"),
        })?;
    let cluster_id_text = utf8_config_field(config.cluster_id_utf8, "cluster_id")?;
    let cluster_id =
        if cluster_id_text.trim().is_empty() {
            None
        } else {
            Some(ClusterId::from_str(cluster_id_text.trim()).map_err(|source| {
            NetworkError::Serialization {
                detail: format!(
                    "network module config cluster id `{cluster_id_text}` is invalid: {source}"
                ),
            }
        })?)
        };
    let daemon_forward_address =
        utf8_config_field(config.daemon_forward_address_utf8, "daemon_forward_address")?;
    Ok(IrohModuleStartConfig {
        cluster_root,
        node_id,
        cluster_id,
        daemon_forward_address,
    })
}

fn ffi_timeout(ms: u64) -> Option<Duration> {
    // ABI convention: 0 = no timeout.
    (ms != 0).then(|| Duration::from_millis(ms))
}

/// # Safety
///
/// `instance` must be a pointer produced by [`sinomo_network_module_create_v1`]
/// that has not been destroyed.
unsafe fn module_instance<'a>(instance: *mut c_void) -> Option<&'a IrohNetworkModuleInstance> {
    // SAFETY: caller contract above.
    unsafe { (instance as *const IrohNetworkModuleInstance).as_ref() }
}

fn write_network_buffer(out: *mut SinomoNetworkByteBuffer, bytes: Vec<u8>) {
    if out.is_null() {
        return;
    }
    // SAFETY: the host passes a valid output slot per the network ABI.
    unsafe {
        *out = SinomoNetworkByteBuffer::from_vec(bytes);
    }
}

/// Writes UTF-8 error detail into the output buffer (ABI: modules MAY report
/// detail this way on non-OK status) and picks the status code.
fn network_error_status(
    out: *mut SinomoNetworkByteBuffer,
    error: &NetworkError,
) -> SinomoNetworkModuleStatusCode {
    write_network_buffer(out, error.to_string().into_bytes());
    match error {
        NetworkError::Serialization { .. } => SINOMO_NETWORK_MODULE_STATUS_INVALID_ARGUMENT,
        _ => SINOMO_NETWORK_MODULE_STATUS_RUNTIME_ERROR,
    }
}

fn network_rpc_error_status(
    out: *mut SinomoNetworkByteBuffer,
    error: &NetworkError,
) -> SinomoNetworkModuleStatusCode {
    let bytes =
        encode_network_module_rpc_error(error).unwrap_or_else(|_| error.to_string().into_bytes());
    write_network_buffer(out, bytes);
    match error {
        NetworkError::Serialization { .. } => SINOMO_NETWORK_MODULE_STATUS_INVALID_ARGUMENT,
        _ => SINOMO_NETWORK_MODULE_STATUS_RUNTIME_ERROR,
    }
}

unsafe extern "C" fn iroh_network_module_descriptor_ffi(
    out: *mut SinomoNetworkByteBuffer,
) -> SinomoNetworkModuleStatusCode {
    let result = catch_unwind(AssertUnwindSafe(|| {
        if out.is_null() {
            return SINOMO_NETWORK_MODULE_STATUS_INVALID_ARGUMENT;
        }
        match encode_network_module_sobj(&iroh_network_module_descriptor()) {
            Ok(bytes) => {
                write_network_buffer(out, bytes);
                SINOMO_NETWORK_MODULE_STATUS_OK
            }
            Err(error) => network_error_status(out, &error),
        }
    }));
    result.unwrap_or(SINOMO_NETWORK_MODULE_STATUS_RUNTIME_ERROR)
}

unsafe extern "C" fn iroh_network_module_start_ffi(
    instance: *mut c_void,
    config: *const SinomoNetworkModuleConfigV1,
) -> SinomoNetworkModuleStatusCode {
    let result = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: host passes the instance created by the entry function.
        let Some(instance) = (unsafe { module_instance(instance) }) else {
            return SINOMO_NETWORK_MODULE_STATUS_INVALID_ARGUMENT;
        };
        let config = match parse_start_config(config) {
            Ok(config) => config,
            Err(error) => {
                eprintln!("sinomo iroh network module start rejected: {error}");
                return SINOMO_NETWORK_MODULE_STATUS_INVALID_ARGUMENT;
            }
        };
        match instance.start(config) {
            Ok(()) => SINOMO_NETWORK_MODULE_STATUS_OK,
            Err(error) => {
                eprintln!("sinomo iroh network module start failed: {error}");
                SINOMO_NETWORK_MODULE_STATUS_RUNTIME_ERROR
            }
        }
    }));
    result.unwrap_or(SINOMO_NETWORK_MODULE_STATUS_RUNTIME_ERROR)
}

unsafe extern "C" fn iroh_network_module_stop_ffi(
    instance: *mut c_void,
) -> SinomoNetworkModuleStatusCode {
    let result = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: host passes the instance created by the entry function.
        let Some(instance) = (unsafe { module_instance(instance) }) else {
            return SINOMO_NETWORK_MODULE_STATUS_INVALID_ARGUMENT;
        };
        match instance.stop() {
            Ok(()) => SINOMO_NETWORK_MODULE_STATUS_OK,
            Err(error) => {
                eprintln!("sinomo iroh network module stop failed: {error}");
                SINOMO_NETWORK_MODULE_STATUS_RUNTIME_ERROR
            }
        }
    }));
    result.unwrap_or(SINOMO_NETWORK_MODULE_STATUS_RUNTIME_ERROR)
}

unsafe extern "C" fn iroh_network_module_dial_rpc_ffi(
    instance: *mut c_void,
    address_utf8: SinomoNetworkByteSlice,
    request_frame: SinomoNetworkByteSlice,
    connect_timeout_ms: u64,
    idle_timeout_ms: u64,
    response_out: *mut SinomoNetworkByteBuffer,
) -> SinomoNetworkModuleStatusCode {
    let result = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: host passes the instance created by the entry function.
        let Some(instance) = (unsafe { module_instance(instance) }) else {
            return SINOMO_NETWORK_MODULE_STATUS_INVALID_ARGUMENT;
        };
        if response_out.is_null() {
            return SINOMO_NETWORK_MODULE_STATUS_INVALID_ARGUMENT;
        }
        // SAFETY: the host guarantees both views are valid for this call.
        let address_bytes = unsafe { address_utf8.as_slice() };
        let request = unsafe { request_frame.as_slice() };
        let Ok(address) = std::str::from_utf8(address_bytes) else {
            write_network_buffer(
                response_out,
                b"network module dial address is not UTF-8".to_vec(),
            );
            return SINOMO_NETWORK_MODULE_STATUS_INVALID_ARGUMENT;
        };
        let timeouts = DaemonRpcTimeouts {
            connect_timeout: ffi_timeout(connect_timeout_ms),
            idle_timeout: ffi_timeout(idle_timeout_ms),
        };
        match instance.dial(address, request, timeouts) {
            Ok(response) => {
                write_network_buffer(response_out, response);
                SINOMO_NETWORK_MODULE_STATUS_OK
            }
            Err(error) => network_rpc_error_status(response_out, &error),
        }
    }));
    result.unwrap_or(SINOMO_NETWORK_MODULE_STATUS_RUNTIME_ERROR)
}

unsafe extern "C" fn iroh_network_module_local_endpoints_ffi(
    instance: *mut c_void,
    out: *mut SinomoNetworkByteBuffer,
) -> SinomoNetworkModuleStatusCode {
    let result = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: host passes the instance created by the entry function.
        let Some(instance) = (unsafe { module_instance(instance) }) else {
            return SINOMO_NETWORK_MODULE_STATUS_INVALID_ARGUMENT;
        };
        if out.is_null() {
            return SINOMO_NETWORK_MODULE_STATUS_INVALID_ARGUMENT;
        }
        match encode_network_module_sobj(&instance.local_endpoints()) {
            Ok(bytes) => {
                write_network_buffer(out, bytes);
                SINOMO_NETWORK_MODULE_STATUS_OK
            }
            Err(error) => network_error_status(out, &error),
        }
    }));
    result.unwrap_or(SINOMO_NETWORK_MODULE_STATUS_RUNTIME_ERROR)
}

unsafe extern "C" fn iroh_network_module_endpoint_role_ffi(
    _instance: *mut c_void,
    endpoint_sobj: SinomoNetworkByteSlice,
    out: *mut SinomoNetworkByteBuffer,
) -> SinomoNetworkModuleStatusCode {
    let result = catch_unwind(AssertUnwindSafe(|| {
        if out.is_null() {
            return SINOMO_NETWORK_MODULE_STATUS_INVALID_ARGUMENT;
        }
        // SAFETY: the host guarantees the request view is valid for the call.
        let endpoint_bytes = unsafe { endpoint_sobj.as_slice() };
        let endpoint = match sinomo_core::network::network_module_abi::decode_network_module_sobj::<
            sinomo_core::ClusterEndpointRecord,
        >(endpoint_bytes)
        {
            Ok(endpoint) => endpoint,
            Err(error) => return network_error_status(out, &error),
        };
        match encode_network_module_sobj(&iroh_endpoint_role(&endpoint)) {
            Ok(bytes) => {
                write_network_buffer(out, bytes);
                SINOMO_NETWORK_MODULE_STATUS_OK
            }
            Err(error) => network_error_status(out, &error),
        }
    }));
    result.unwrap_or(SINOMO_NETWORK_MODULE_STATUS_RUNTIME_ERROR)
}

unsafe extern "C" fn iroh_network_module_free_buffer_ffi(buffer: SinomoNetworkByteBuffer) {
    // SAFETY: the host only frees buffers this module produced via
    // SinomoNetworkByteBuffer::from_vec.
    let _ = catch_unwind(AssertUnwindSafe(|| unsafe {
        drop(SinomoNetworkByteBuffer::into_vec(buffer));
    }));
}

unsafe extern "C" fn iroh_network_module_destroy_ffi(instance: *mut c_void) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if instance.is_null() {
            return;
        }
        // SAFETY: the host destroys each instance exactly once; the pointer
        // was produced by Box::into_raw in the entry function.
        let instance = unsafe { Box::from_raw(instance as *mut IrohNetworkModuleInstance) };
        let _ = instance.stop();
        // Dropping a runtime inside an async context panics; destroy runs on
        // a plain host thread, but shutdown_background is safe either way.
        if let Some(runtime) = instance.runtime.into_inner() {
            runtime.shutdown_background();
        }
    }));
}

/// The pinned v1 entry: creates the module instance and fills the vtable.
///
/// # Safety
///
/// `instance_out` and `vtable_out` must be valid, writable pointers. The host
/// must eventually pass the returned instance to the vtable's `destroy`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sinomo_network_module_create_v1(
    instance_out: *mut *mut c_void,
    vtable_out: *mut SinomoNetworkModuleVtableV1,
) -> SinomoNetworkModuleStatusCode {
    let result = catch_unwind(AssertUnwindSafe(|| {
        if instance_out.is_null() || vtable_out.is_null() {
            return SINOMO_NETWORK_MODULE_STATUS_INVALID_ARGUMENT;
        }
        let instance = Box::new(IrohNetworkModuleInstance::new());
        // SAFETY: both out pointers were null-checked above and are writable
        // per the entry contract.
        unsafe {
            *instance_out = Box::into_raw(instance) as *mut c_void;
            *vtable_out = SinomoNetworkModuleVtableV1 {
                abi_version: SINOMO_NETWORK_MODULE_ABI_VERSION,
                descriptor: iroh_network_module_descriptor_ffi,
                start: iroh_network_module_start_ffi,
                stop: iroh_network_module_stop_ffi,
                dial_rpc: iroh_network_module_dial_rpc_ffi,
                local_endpoints: iroh_network_module_local_endpoints_ffi,
                endpoint_role: Some(iroh_network_module_endpoint_role_ffi),
                free_buffer: iroh_network_module_free_buffer_ffi,
                destroy: iroh_network_module_destroy_ffi,
            };
        }
        SINOMO_NETWORK_MODULE_STATUS_OK
    }));
    result.unwrap_or(SINOMO_NETWORK_MODULE_STATUS_RUNTIME_ERROR)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_descriptor_declares_the_network_transport_surface() {
        let descriptor = iroh_native_module_descriptor();
        assert_eq!(descriptor.module_id, SINOMO_NETWORK_IROH_MODULE_ID);
        assert_eq!(
            descriptor.interface_version,
            SINOMO_NETWORK_IROH_INTERFACE_VERSION
        );
        assert_eq!(
            descriptor.surfaces.network_transports,
            vec![SINOMO_NETWORK_IROH_NETWORK_MODULE_ID.to_string()]
        );
    }

    #[test]
    fn network_descriptor_matches_the_pinned_stage_c_shape() {
        let descriptor = iroh_network_module_descriptor();
        assert_eq!(descriptor.id, "iroh");
        assert_eq!(descriptor.transports, vec![ClusterTransportKind::Iroh]);
        assert_eq!(descriptor.address_schemes, vec!["iroh".to_string()]);
        assert!(descriptor.dialer);
        assert!(descriptor.listener);
        assert!(descriptor.supports_runtime_toggle);
    }

    #[test]
    fn entry_creates_instance_and_vtable_and_round_trips_descriptor() {
        let mut instance: *mut c_void = std::ptr::null_mut();
        let mut vtable = std::mem::MaybeUninit::<SinomoNetworkModuleVtableV1>::uninit();
        // SAFETY: valid out pointers; instance destroyed below.
        let status = unsafe { sinomo_network_module_create_v1(&mut instance, vtable.as_mut_ptr()) };
        assert_eq!(status, SINOMO_NETWORK_MODULE_STATUS_OK);
        // SAFETY: entry returned OK, so the vtable was fully written.
        let vtable = unsafe { vtable.assume_init() };
        assert_eq!(vtable.abi_version, SINOMO_NETWORK_MODULE_ABI_VERSION);

        let mut buffer = SinomoNetworkByteBuffer::null();
        // SAFETY: calling the module's own descriptor fn with a valid slot.
        let status = unsafe { (vtable.descriptor)(&mut buffer) };
        assert_eq!(status, SINOMO_NETWORK_MODULE_STATUS_OK);
        // SAFETY: buffer produced by this module above.
        let bytes = unsafe { SinomoNetworkByteBuffer::into_vec(buffer) };
        let decoded: NetworkModuleDescriptor =
            sinomo_core::network::network_module_abi::decode_network_module_sobj(&bytes)
                .expect("decode descriptor");
        assert_eq!(decoded, iroh_network_module_descriptor());

        // Stop before start is a no-op success (runtime toggle safety).
        // SAFETY: valid instance from the entry call.
        let status = unsafe { (vtable.stop)(instance) };
        assert_eq!(status, SINOMO_NETWORK_MODULE_STATUS_OK);

        let mut endpoints = SinomoNetworkByteBuffer::null();
        // SAFETY: valid instance + output slot.
        let status = unsafe { (vtable.local_endpoints)(instance, &mut endpoints) };
        assert_eq!(status, SINOMO_NETWORK_MODULE_STATUS_OK);
        // SAFETY: buffer produced by this module above.
        let bytes = unsafe { SinomoNetworkByteBuffer::into_vec(endpoints) };
        let decoded: Vec<sinomo_core::ClusterEndpointRecord> =
            sinomo_core::network::network_module_abi::decode_network_module_sobj(&bytes)
                .expect("decode endpoints");
        assert!(decoded.is_empty(), "no listener yet, no endpoints");

        // SAFETY: destroy exactly once with the instance from the entry.
        unsafe { (vtable.destroy)(instance) };
    }
}
