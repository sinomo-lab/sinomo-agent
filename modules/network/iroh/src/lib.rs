//! Iroh roaming transport as a trusted native-dylib network module.
//!
//! This module owns every Iroh-specific behavior: endpoint builder +
//! relay-mode config, secret-key load/create, endpoint record parse/format,
//! stable endpoint ids, endpoint record file I/O, the daemon proxy
//! supervisor/accept loop, and the dial client. `sinomo-network`
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
mod proxy;

use endpoint::{iroh_endpoint_public_key, stable_iroh_endpoint_id};
use sinomo_core::ClusterTransportKind;
use sinomo_core::network::network_module_abi::{
    SINOMO_NETWORK_MODULE_ABI_MAGIC, SINOMO_NETWORK_MODULE_ABI_VERSION,
    SINOMO_NETWORK_MODULE_STATUS_INVALID_ARGUMENT, SINOMO_NETWORK_MODULE_STATUS_OK,
    SINOMO_NETWORK_MODULE_STATUS_RUNTIME_ERROR, SinomoNetworkByteBuffer, SinomoNetworkByteSlice,
    SinomoNetworkModuleConfigV1, SinomoNetworkModuleStatusCode, SinomoNetworkModuleVtableV1,
    SinomoNetworkRpcStreamV1, encode_network_module_rpc_error, encode_network_module_sobj,
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
    SinomoModuleHostApiV2, SinomoModuleStatusCode, decode_sobj, encode_sobj,
};
use std::collections::HashSet;
use std::ffi::c_void;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{LazyLock, Mutex, OnceLock};
use std::thread;
use std::time::Duration;

/// Module id under which the dylib installs (`[package.metadata.sinomo.native-dylib]`).
pub const SINOMO_NETWORK_IROH_MODULE_ID: &str = "module.network-iroh";
/// Interface version pinned in the loader state.
pub const SINOMO_NETWORK_IROH_INTERFACE_VERSION: &str = "sinomo.network-iroh.v1";
/// Network-module descriptor id (`NetworkModuleDescriptor::id`).
pub const SINOMO_NETWORK_IROH_NETWORK_MODULE_ID: &str = "iroh";
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
pub extern "C" fn sinomo_module_init(host: SinomoModuleHostApiV2) -> SinomoModuleStatusCode {
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
    runtime_init: Mutex<()>,
    proxy: Mutex<Option<proxy::IrohDaemonProxy>>,
    dialer: dial::IrohDaemonDialer,
}

/// Live raw instances exported through the C ABI. Besides rejecting stale
/// callback pointers, claiming an entry before destruction makes destroy
/// idempotent: only one caller can reconstruct the owning Box.
static LIVE_IROH_NETWORK_MODULE_INSTANCES: LazyLock<Mutex<HashSet<usize>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));

#[derive(Clone, Copy)]
struct OwnedIrohNetworkModuleInstance(*mut IrohNetworkModuleInstance);

// SAFETY: the pointer represents unique Box ownership after it has been
// atomically removed from LIVE_IROH_NETWORK_MODULE_INSTANCES. It is moved to
// at most one teardown thread and never dereferenced elsewhere.
unsafe impl Send for OwnedIrohNetworkModuleInstance {}

/// One native QUIC bidirectional stream retained behind the opaque network
/// module ABI handle. Separate locks allow a blocking read and write to make
/// progress concurrently on the module runtime.
struct IrohNetworkRpcStream {
    runtime: tokio::runtime::Handle,
    recv: Mutex<Option<iroh::endpoint::RecvStream>>,
    send: Mutex<Option<iroh::endpoint::SendStream>>,
    no_progress_timeout: Option<Duration>,
}

impl IrohNetworkRpcStream {
    fn read(&self, bytes: &mut [u8]) -> Result<usize, NetworkError> {
        let mut recv = self
            .recv
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(recv) = recv.as_mut() else {
            return Ok(0);
        };
        let read = async { tokio::io::AsyncReadExt::read(recv, bytes).await };
        let result = match self.no_progress_timeout {
            Some(timeout) => self
                .runtime
                .block_on(async { tokio::time::timeout(timeout, read).await })
                .map_err(|_| {
                    NetworkError::timeout(
                        "read iroh RPC stream",
                        sinomo_core::network::NetworkTimeoutPhase::Idle,
                        timeout,
                        sinomo_core::network::NetworkDeliveryState::PossiblyApplied,
                    )
                })?,
            None => self.runtime.block_on(read),
        };
        result.map_err(|source| NetworkError::Io {
            operation: "read iroh RPC stream".to_string(),
            detail: source.to_string(),
        })
    }

    fn write(&self, bytes: &[u8]) -> Result<usize, NetworkError> {
        let mut send = self
            .send
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(send) = send.as_mut() else {
            return Err(NetworkError::Io {
                operation: "write iroh RPC stream".to_string(),
                detail: "write side is already shut down".to_string(),
            });
        };
        let write = async { tokio::io::AsyncWriteExt::write(send, bytes).await };
        let result = match self.no_progress_timeout {
            Some(timeout) => self
                .runtime
                .block_on(async { tokio::time::timeout(timeout, write).await })
                .map_err(|_| {
                    NetworkError::timeout(
                        "write iroh RPC stream",
                        sinomo_core::network::NetworkTimeoutPhase::Write,
                        timeout,
                        sinomo_core::network::NetworkDeliveryState::PossiblyApplied,
                    )
                })?,
            None => self.runtime.block_on(write),
        };
        result.map_err(|source| NetworkError::Io {
            operation: "write iroh RPC stream".to_string(),
            detail: source.to_string(),
        })
    }

    fn shutdown_write(&self) -> Result<(), NetworkError> {
        let mut send = self
            .send
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(mut send) = send.take() else {
            return Ok(());
        };
        send.finish().map_err(|source| NetworkError::Io {
            operation: "shutdown iroh RPC stream write".to_string(),
            detail: source.to_string(),
        })
    }
}

impl IrohNetworkModuleInstance {
    fn new() -> Self {
        Self {
            runtime: OnceLock::new(),
            runtime_init: Mutex::new(()),
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
        // Serialize lazy initialization. Without this guard, concurrent first
        // callers can build two runtimes and the losing Runtime would be
        // dropped on an arbitrary (possibly Tokio) caller thread.
        let _init = self
            .runtime_init
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
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
        if let Err(unused_runtime) = self.runtime.set(runtime) {
            // The initialization mutex makes this unreachable in normal
            // operation. Keep the fallback non-blocking and safe even if that
            // invariant is changed later.
            unused_runtime.shutdown_background();
        }
        self.runtime.get().ok_or_else(|| NetworkError::Io {
            operation: "build iroh network module runtime".to_string(),
            detail: "runtime initialization completed without installing a runtime".to_string(),
        })
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
        let runtime_handle = runtime.handle().clone();
        let handle = run_iroh_sync_operation_outside_tokio("start iroh network module", || {
            runtime.block_on(proxy::start_iroh_daemon_proxy(runtime_handle, proxy_config))
        })?;
        *self
            .proxy
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(handle);
        Ok(())
    }

    fn stop(&self) -> Result<(), NetworkError> {
        // A never-used or dial-only pre-cluster module has no async resources;
        // stopping it remains an idempotent no-op and no longer creates a
        // runtime merely to tear it down.
        if self.runtime.get().is_none() {
            return Ok(());
        }
        run_iroh_sync_operation_outside_tokio("stop iroh network module", || {
            self.stop_on_plain_thread()
        })
    }

    /// Stops proxy and dialer resources. The caller must not be inside any
    /// Tokio runtime because this synchronously drives the module runtime.
    fn stop_on_plain_thread(&self) -> Result<(), NetworkError> {
        let proxy = self
            .proxy
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        let Some(runtime) = self.runtime.get() else {
            return Ok(());
        };
        if let Some(proxy) = proxy {
            runtime.block_on(proxy.shutdown());
        }
        runtime.block_on(self.dialer.shutdown());
        Ok(())
    }

    fn open_stream(
        &self,
        address: &str,
        timeouts: DaemonRpcTimeouts,
    ) -> Result<IrohNetworkRpcStream, NetworkError> {
        if iroh_disabled_by_env() {
            return Err(NetworkError::Io {
                operation: "open iroh network module stream".to_string(),
                detail: format!("{SINOMO_DISABLE_IROH_ENV}=1"),
            });
        }
        let runtime = self.runtime()?;
        let (send, recv) =
            run_iroh_sync_operation_outside_tokio("open iroh network module stream", || {
                runtime.block_on(self.dialer.open_daemon_stream_async(address, timeouts))
            })?;
        Ok(IrohNetworkRpcStream {
            runtime: runtime.handle().clone(),
            recv: Mutex::new(Some(recv)),
            send: Mutex::new(Some(send)),
            no_progress_timeout: timeouts.idle_timeout,
        })
    }

    fn local_endpoints(&self) -> Vec<sinomo_core::ClusterEndpointRecord> {
        self.proxy
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .map(|proxy| vec![proxy.record().endpoint()])
            .unwrap_or_default()
    }

    /// Consumes the sole instance owner on a plain OS thread. Runtime shutdown
    /// is isolated from unwinding so no Runtime is ever dropped while another
    /// panic is active, and the private runtime is fully gone before the ABI
    /// destroy callback returns (allowing the host to unload the dylib).
    fn destroy_on_plain_thread(mut instance: Box<Self>) {
        let runtime = instance.runtime.take();
        let proxy = instance
            .proxy
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();

        if let Some(runtime) = runtime {
            let shutdown = catch_unwind(AssertUnwindSafe(|| {
                if let Some(proxy) = proxy {
                    runtime.block_on(proxy.shutdown());
                }
                runtime.block_on(instance.dialer.shutdown());
            }));
            if shutdown.is_err() {
                eprintln!("sinomo iroh network module async shutdown panicked");
            }

            // This thread has no entered Tokio context. Dropping the Runtime
            // here waits for its worker threads and prevents module code from
            // surviving past dynamic-library unload.
            let runtime_drop = catch_unwind(AssertUnwindSafe(|| drop(runtime)));
            if runtime_drop.is_err() {
                eprintln!("sinomo iroh network module runtime shutdown panicked");
            }
        } else {
            drop(proxy);
        }

        // The Runtime has already been taken, so even a pathological field
        // destructor cannot trigger Tokio's nested-runtime drop panic.
        let instance_drop = catch_unwind(AssertUnwindSafe(|| drop(instance)));
        if instance_drop.is_err() {
            eprintln!("sinomo iroh network module instance destruction panicked");
        }
    }
}

/// Runs a synchronous operation that drives the private Iroh runtime. Tokio
/// forbids Runtime::block_on from an entered runtime, so callers on Tokio
/// workers are bridged through one scoped plain OS thread. Spawn and join
/// failures become normal module errors rather than unwinding through FFI.
fn run_iroh_sync_operation_outside_tokio<T, F>(
    operation: &'static str,
    operation_fn: F,
) -> Result<T, NetworkError>
where
    T: Send,
    F: FnOnce() -> Result<T, NetworkError> + Send,
{
    let run = || catch_unwind(AssertUnwindSafe(operation_fn));
    let result = if tokio::runtime::Handle::try_current().is_ok() {
        thread::scope(|scope| {
            let worker = thread::Builder::new()
                .name("sinomo-iroh-runtime-bridge".to_string())
                .spawn_scoped(scope, run)
                .map_err(|source| NetworkError::Io {
                    operation: operation.to_string(),
                    detail: format!("spawn runtime bridge: {source}"),
                })?;
            worker.join().map_err(|_| NetworkError::Io {
                operation: operation.to_string(),
                detail: "runtime bridge thread panicked".to_string(),
            })
        })?
    } else {
        run()
    };
    result.map_err(|_| NetworkError::Io {
        operation: operation.to_string(),
        detail: "operation panicked while driving the iroh runtime".to_string(),
    })?
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

fn live_iroh_network_module_instances() -> std::sync::MutexGuard<'static, HashSet<usize>> {
    LIVE_IROH_NETWORK_MODULE_INSTANCES
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn register_iroh_network_module_instance(instance: *mut IrohNetworkModuleInstance) {
    live_iroh_network_module_instances().insert(instance as usize);
}

fn is_live_iroh_network_module_instance(instance: *mut c_void) -> bool {
    !instance.is_null() && live_iroh_network_module_instances().contains(&(instance as usize))
}

fn claim_iroh_network_module_instance(
    instance: *mut c_void,
) -> Option<OwnedIrohNetworkModuleInstance> {
    if instance.is_null() || !live_iroh_network_module_instances().remove(&(instance as usize)) {
        return None;
    }
    Some(OwnedIrohNetworkModuleInstance(
        instance as *mut IrohNetworkModuleInstance,
    ))
}

impl OwnedIrohNetworkModuleInstance {
    /// Reconstructs and consumes the Box on a thread that has no entered Tokio
    /// runtime. The live-instance claim guarantees unique ownership.
    ///
    /// # Safety
    ///
    /// `self` must have been returned by `claim_iroh_network_module_instance`
    /// and this method must be called at most once.
    unsafe fn destroy_on_plain_thread(self) {
        // SAFETY: the claim removed this exact pointer from the live registry,
        // transferring its sole allocation ownership to `self`.
        let instance = unsafe { Box::from_raw(self.0) };
        IrohNetworkModuleInstance::destroy_on_plain_thread(instance);
    }
}

/// # Safety
///
/// `instance` must be a pointer produced by [`sinomo_network_module_create_v1`]
/// that has not been destroyed.
unsafe fn module_instance<'a>(instance: *mut c_void) -> Option<&'a IrohNetworkModuleInstance> {
    if !is_live_iroh_network_module_instance(instance) {
        return None;
    }
    // SAFETY: caller contract above.
    unsafe { (instance as *const IrohNetworkModuleInstance).as_ref() }
}

/// # Safety
///
/// `stream` must be an opaque handle produced by
/// [`iroh_network_module_open_rpc_stream_ffi`] that has not been closed.
unsafe fn rpc_stream_instance<'a>(
    stream: SinomoNetworkRpcStreamV1,
) -> Option<&'a IrohNetworkRpcStream> {
    // SAFETY: caller contract above.
    unsafe { (stream.ptr as *const IrohNetworkRpcStream).as_ref() }
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

unsafe extern "C" fn iroh_network_module_open_rpc_stream_ffi(
    instance: *mut c_void,
    address_utf8: SinomoNetworkByteSlice,
    connect_timeout_ms: u64,
    idle_timeout_ms: u64,
    stream_out: *mut SinomoNetworkRpcStreamV1,
    error_out: *mut SinomoNetworkByteBuffer,
) -> SinomoNetworkModuleStatusCode {
    let result = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: host passes the instance created by the entry function.
        let Some(instance) = (unsafe { module_instance(instance) }) else {
            return SINOMO_NETWORK_MODULE_STATUS_INVALID_ARGUMENT;
        };
        if stream_out.is_null() || error_out.is_null() {
            return SINOMO_NETWORK_MODULE_STATUS_INVALID_ARGUMENT;
        }
        // SAFETY: the host guarantees the address view is valid for this call.
        let address_bytes = unsafe { address_utf8.as_slice() };
        let Ok(address) = std::str::from_utf8(address_bytes) else {
            write_network_buffer(
                error_out,
                b"network module stream address is not UTF-8".to_vec(),
            );
            return SINOMO_NETWORK_MODULE_STATUS_INVALID_ARGUMENT;
        };
        let timeouts = DaemonRpcTimeouts {
            connect_timeout: ffi_timeout(connect_timeout_ms),
            idle_timeout: ffi_timeout(idle_timeout_ms),
        };
        match instance.open_stream(address, timeouts) {
            Ok(stream) => {
                // SAFETY: output slot was validated above. Ownership transfers
                // to the host until close_rpc_stream returns it below.
                unsafe {
                    *stream_out = SinomoNetworkRpcStreamV1 {
                        ptr: Box::into_raw(Box::new(stream)) as *mut c_void,
                    };
                }
                SINOMO_NETWORK_MODULE_STATUS_OK
            }
            Err(error) => network_rpc_error_status(error_out, &error),
        }
    }));
    result.unwrap_or(SINOMO_NETWORK_MODULE_STATUS_RUNTIME_ERROR)
}

unsafe extern "C" fn iroh_network_module_read_rpc_stream_ffi(
    stream: SinomoNetworkRpcStreamV1,
    buffer: *mut u8,
    capacity: usize,
    read_out: *mut usize,
    error_out: *mut SinomoNetworkByteBuffer,
) -> SinomoNetworkModuleStatusCode {
    let result = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: host passes an open stream handle.
        let Some(stream) = (unsafe { rpc_stream_instance(stream) }) else {
            return SINOMO_NETWORK_MODULE_STATUS_INVALID_ARGUMENT;
        };
        if read_out.is_null() || error_out.is_null() || (buffer.is_null() && capacity != 0) {
            return SINOMO_NETWORK_MODULE_STATUS_INVALID_ARGUMENT;
        }
        // SAFETY: the host owns a writable `capacity`-byte buffer for the call.
        let bytes = if capacity == 0 {
            &mut []
        } else {
            unsafe { std::slice::from_raw_parts_mut(buffer, capacity) }
        };
        match stream.read(bytes) {
            Ok(read) => {
                // SAFETY: output slot was validated above.
                unsafe { *read_out = read };
                SINOMO_NETWORK_MODULE_STATUS_OK
            }
            Err(error) => network_rpc_error_status(error_out, &error),
        }
    }));
    result.unwrap_or(SINOMO_NETWORK_MODULE_STATUS_RUNTIME_ERROR)
}

unsafe extern "C" fn iroh_network_module_write_rpc_stream_ffi(
    stream: SinomoNetworkRpcStreamV1,
    bytes: SinomoNetworkByteSlice,
    written_out: *mut usize,
    error_out: *mut SinomoNetworkByteBuffer,
) -> SinomoNetworkModuleStatusCode {
    let result = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: host passes an open stream handle.
        let Some(stream) = (unsafe { rpc_stream_instance(stream) }) else {
            return SINOMO_NETWORK_MODULE_STATUS_INVALID_ARGUMENT;
        };
        if written_out.is_null() || error_out.is_null() {
            return SINOMO_NETWORK_MODULE_STATUS_INVALID_ARGUMENT;
        }
        // SAFETY: host guarantees this byte view is valid for the call.
        let bytes = unsafe { bytes.as_slice() };
        match stream.write(bytes) {
            Ok(written) => {
                // SAFETY: output slot was validated above.
                unsafe { *written_out = written };
                SINOMO_NETWORK_MODULE_STATUS_OK
            }
            Err(error) => network_rpc_error_status(error_out, &error),
        }
    }));
    result.unwrap_or(SINOMO_NETWORK_MODULE_STATUS_RUNTIME_ERROR)
}

unsafe extern "C" fn iroh_network_module_shutdown_rpc_stream_write_ffi(
    stream: SinomoNetworkRpcStreamV1,
    error_out: *mut SinomoNetworkByteBuffer,
) -> SinomoNetworkModuleStatusCode {
    let result = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: host passes an open stream handle.
        let Some(stream) = (unsafe { rpc_stream_instance(stream) }) else {
            return SINOMO_NETWORK_MODULE_STATUS_INVALID_ARGUMENT;
        };
        if error_out.is_null() {
            return SINOMO_NETWORK_MODULE_STATUS_INVALID_ARGUMENT;
        }
        match stream.shutdown_write() {
            Ok(()) => SINOMO_NETWORK_MODULE_STATUS_OK,
            Err(error) => network_rpc_error_status(error_out, &error),
        }
    }));
    result.unwrap_or(SINOMO_NETWORK_MODULE_STATUS_RUNTIME_ERROR)
}

unsafe extern "C" fn iroh_network_module_close_rpc_stream_ffi(stream: SinomoNetworkRpcStreamV1) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if stream.ptr.is_null() {
            return;
        }
        // SAFETY: the host closes each successful open exactly once and only
        // after all read/write callbacks have returned.
        unsafe { drop(Box::from_raw(stream.ptr as *mut IrohNetworkRpcStream)) };
    }));
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
        let Some(instance) = claim_iroh_network_module_instance(instance) else {
            // Null and already-destroyed handles are both harmless no-ops.
            return;
        };

        if tokio::runtime::Handle::try_current().is_ok() {
            let worker_instance = instance;
            match thread::Builder::new()
                .name("sinomo-iroh-module-destroy".to_string())
                .spawn(move || {
                    // SAFETY: successful spawn transfers the claimed Box
                    // ownership to this plain teardown thread.
                    unsafe { worker_instance.destroy_on_plain_thread() };
                }) {
                Ok(worker) => {
                    // Destruction is synchronous at the ABI boundary: once it
                    // returns, the host may unload this dylib safely.
                    if worker.join().is_err() {
                        eprintln!("sinomo iroh network module teardown thread panicked");
                    }
                }
                Err(error) => {
                    // The raw Box remains allocated and the live registry has
                    // already been claimed. Leak rather than reconstructing
                    // it on a Tokio worker, where Runtime drop can abort.
                    eprintln!(
                        "sinomo iroh network module teardown thread failed to spawn; \
                         leaking instance safely: {error}"
                    );
                    let _ = instance;
                }
            }
        } else {
            // SAFETY: this thread has no entered Tokio runtime and owns the
            // unique claimed instance.
            unsafe { instance.destroy_on_plain_thread() };
        }
    }));
}

/// The pinned v1 entry creates the module instance and fills the
/// self-describing vtable.
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
        let instance = Box::into_raw(instance);
        register_iroh_network_module_instance(instance);
        // SAFETY: both out pointers were null-checked above and are writable
        // per the entry contract.
        unsafe {
            *instance_out = instance as *mut c_void;
            *vtable_out = SinomoNetworkModuleVtableV1 {
                abi_magic: SINOMO_NETWORK_MODULE_ABI_MAGIC,
                abi_version: SINOMO_NETWORK_MODULE_ABI_VERSION,
                struct_size: SinomoNetworkModuleVtableV1::expected_struct_size(),
                descriptor: Some(iroh_network_module_descriptor_ffi),
                start: Some(iroh_network_module_start_ffi),
                stop: Some(iroh_network_module_stop_ffi),
                open_rpc_stream: Some(iroh_network_module_open_rpc_stream_ffi),
                read_rpc_stream: Some(iroh_network_module_read_rpc_stream_ffi),
                write_rpc_stream: Some(iroh_network_module_write_rpc_stream_ffi),
                shutdown_rpc_stream_write: Some(iroh_network_module_shutdown_rpc_stream_write_ffi),
                close_rpc_stream: Some(iroh_network_module_close_rpc_stream_ffi),
                local_endpoints: Some(iroh_network_module_local_endpoints_ffi),
                endpoint_role: Some(iroh_network_module_endpoint_role_ffi),
                free_buffer: Some(iroh_network_module_free_buffer_ffi),
                destroy: Some(iroh_network_module_destroy_ffi),
            };
        }
        SINOMO_NETWORK_MODULE_STATUS_OK
    }));
    result.unwrap_or(SINOMO_NETWORK_MODULE_STATUS_RUNTIME_ERROR)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::network::FfiNetworkModule;
    use std::sync::mpsc;

    struct TeardownSignal(mpsc::Sender<()>);

    impl Drop for TeardownSignal {
        fn drop(&mut self) {
            let _ = self.0.send(());
        }
    }

    fn create_test_network_module_instance() -> (*mut c_void, SinomoNetworkModuleVtableV1) {
        let mut instance: *mut c_void = std::ptr::null_mut();
        let mut vtable = SinomoNetworkModuleVtableV1::empty();
        // SAFETY: both output slots remain valid for the call.
        let status = unsafe { sinomo_network_module_create_v1(&mut instance, &mut vtable) };
        assert_eq!(status, SINOMO_NETWORK_MODULE_STATUS_OK);
        assert!(!instance.is_null());
        (instance, vtable)
    }

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
        let (instance, vtable) = create_test_network_module_instance();
        assert_eq!(vtable.abi_magic, SINOMO_NETWORK_MODULE_ABI_MAGIC);
        assert_eq!(vtable.abi_version, SINOMO_NETWORK_MODULE_ABI_VERSION);
        assert_eq!(
            vtable.struct_size,
            SinomoNetworkModuleVtableV1::expected_struct_size()
        );

        let mut buffer = SinomoNetworkByteBuffer::null();
        // SAFETY: calling the module's own descriptor fn with a valid slot.
        let status = unsafe { (vtable.descriptor.expect("descriptor"))(&mut buffer) };
        assert_eq!(status, SINOMO_NETWORK_MODULE_STATUS_OK);
        // SAFETY: buffer produced by this module above.
        let bytes = unsafe { SinomoNetworkByteBuffer::into_vec(buffer) };
        let decoded: NetworkModuleDescriptor =
            sinomo_core::network::network_module_abi::decode_network_module_sobj(&bytes)
                .expect("decode descriptor");
        assert_eq!(decoded, iroh_network_module_descriptor());

        // Stop before start is a no-op success (runtime toggle safety).
        // SAFETY: valid instance from the entry call.
        let status = unsafe { (vtable.stop.expect("stop"))(instance) };
        assert_eq!(status, SINOMO_NETWORK_MODULE_STATUS_OK);

        let mut endpoints = SinomoNetworkByteBuffer::null();
        // SAFETY: valid instance + output slot.
        let status =
            unsafe { (vtable.local_endpoints.expect("local endpoints"))(instance, &mut endpoints) };
        assert_eq!(status, SINOMO_NETWORK_MODULE_STATUS_OK);
        // SAFETY: buffer produced by this module above.
        let bytes = unsafe { SinomoNetworkByteBuffer::into_vec(endpoints) };
        let decoded: Vec<sinomo_core::ClusterEndpointRecord> =
            sinomo_core::network::network_module_abi::decode_network_module_sobj(&bytes)
                .expect("decode endpoints");
        assert!(decoded.is_empty(), "no listener yet, no endpoints");

        // The v2 ABI exposes an opaque byte stream, not a whole-response
        // buffer. A rejected open leaves the handle null and reports the
        // structured transport error through the module-owned error buffer.
        let mut stream = SinomoNetworkRpcStreamV1::null();
        let mut error = SinomoNetworkByteBuffer::null();
        let status = unsafe {
            (vtable.open_rpc_stream.expect("open rpc stream"))(
                instance,
                SinomoNetworkByteSlice::from_slice(b"not-an-iroh-address"),
                5_000,
                5_000,
                &mut stream,
                &mut error,
            )
        };
        assert_ne!(status, SINOMO_NETWORK_MODULE_STATUS_OK);
        assert!(stream.is_null());
        assert!(!error.ptr.is_null(), "failed open should carry detail");
        // SAFETY: release through the same module vtable that allocated it.
        unsafe { (vtable.free_buffer.expect("free buffer"))(error) };

        // SAFETY: destroy exactly once with the instance from the entry.
        unsafe { (vtable.destroy.expect("destroy"))(instance) };
    }

    #[test]
    fn stop_and_destroy_are_idempotent_and_destroy_is_null_safe() {
        let (instance, vtable) = create_test_network_module_instance();
        let stop = vtable.stop.expect("stop");
        let destroy = vtable.destroy.expect("destroy");

        // Stop before initialization is a true no-op and remains safe across
        // repeated runtime-toggle requests.
        // SAFETY: the instance is live for both calls.
        assert_eq!(unsafe { stop(instance) }, SINOMO_NETWORK_MODULE_STATUS_OK);
        // SAFETY: as above; stop is explicitly idempotent.
        assert_eq!(unsafe { stop(instance) }, SINOMO_NETWORK_MODULE_STATUS_OK);

        // SAFETY: destroy defines null as a harmless no-op.
        unsafe { destroy(std::ptr::null_mut()) };
        // SAFETY: the first call claims and consumes this live instance.
        unsafe { destroy(instance) };
        assert!(!is_live_iroh_network_module_instance(instance));
        // SAFETY: a repeated destroy is rejected by the live-instance claim
        // before the stale pointer is dereferenced.
        unsafe { destroy(instance) };
    }

    #[test]
    fn direct_destroy_with_private_runtime_from_current_thread_tokio_is_safe() {
        let (done_tx, done_rx) = mpsc::channel();
        let worker = thread::Builder::new()
            .name("iroh-direct-destroy-regression".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("build outer current-thread runtime");
                runtime.block_on(async {
                    let (instance, vtable) = create_test_network_module_instance();
                    // Materialize the private runtime so this covers the
                    // historical nested Runtime::drop abort, not merely the
                    // uninitialized fast path.
                    // SAFETY: instance is live and owned until destroy below.
                    unsafe { module_instance(instance) }
                        .expect("live instance")
                        .runtime()
                        .expect("build private iroh runtime");

                    let stop = vtable.stop.expect("stop");
                    // SAFETY: the instance is live. These calls exercise the
                    // runtime-toggle path while another runtime is entered.
                    assert_eq!(unsafe { stop(instance) }, SINOMO_NETWORK_MODULE_STATUS_OK);
                    // SAFETY: stop remains idempotent after initialized
                    // resources have already been drained.
                    assert_eq!(unsafe { stop(instance) }, SINOMO_NETWORK_MODULE_STATUS_OK);

                    // SAFETY: valid live instance. Destroy detects the entered
                    // outer runtime and performs teardown on a plain thread.
                    unsafe { (vtable.destroy.expect("destroy"))(instance) };
                    // SAFETY: verifies the stale-handle no-op on the same path.
                    unsafe { (vtable.destroy.expect("destroy"))(instance) };
                });
                let _ = done_tx.send(());
            })
            .expect("spawn watchdog worker");

        done_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("destroy must not panic or hang on a current-thread Tokio worker");
        worker.join().expect("destroy regression worker");
    }

    #[test]
    fn timed_iroh_stream_read_and_write_work_without_an_ambient_tokio_reactor() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("build private iroh runtime");
        let listener = runtime
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .expect("bind daemon fixture");
        let daemon_address = listener
            .local_addr()
            .expect("daemon fixture address")
            .to_string();
        let fixture = runtime.spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept daemon fixture");
            let mut request = Vec::new();
            stream
                .read_to_end(&mut request)
                .await
                .expect("read daemon fixture request");
            assert_eq!(request, b"plain-thread request");
            stream
                .write_all(b"plain-thread response")
                .await
                .expect("write daemon fixture response");
        });

        let root = sinomo_testdir::TestDir::new("sinomo-network-iroh-plain-thread-stream");
        let paths = ClusterPaths::new(&sinomo_core::local::SinomoDataDir::new(&root));
        paths.ensure().expect("cluster paths");
        let proxy = runtime
            .block_on(proxy::start_iroh_daemon_proxy(
                runtime.handle().clone(),
                proxy::IrohProxyConfig {
                    paths,
                    cluster_id: ClusterId::from_legacy_named("cluster_iroh_plain_thread_stream")
                        .expect("cluster"),
                    node_id: NodeId::from_legacy_named("node_iroh_plain_thread_stream")
                        .expect("node"),
                    daemon_forward_address: daemon_address,
                },
            ))
            .expect("start iroh proxy");
        let dialer = dial::IrohDaemonDialer::new();
        let (send, recv) = runtime
            .block_on(dialer.open_daemon_stream_async(
                &proxy.record().endpoint().address,
                DaemonRpcTimeouts::connect_and_idle(Duration::from_secs(5)),
            ))
            .expect("open raw iroh daemon stream");
        let stream = IrohNetworkRpcStream {
            runtime: runtime.handle().clone(),
            recv: Mutex::new(Some(recv)),
            send: Mutex::new(Some(send)),
            no_progress_timeout: Some(Duration::from_secs(5)),
        };

        let worker = thread::Builder::new()
            .name("iroh-timed-stream-plain-thread-regression".to_string())
            .spawn(move || {
                assert!(
                    tokio::runtime::Handle::try_current().is_err(),
                    "regression worker must not have an ambient Tokio reactor"
                );
                let request = b"plain-thread request";
                let mut written = 0;
                while written < request.len() {
                    written += stream
                        .write(&request[written..])
                        .expect("timed write from plain thread");
                }
                stream.shutdown_write().expect("finish request stream");

                let mut response = Vec::new();
                let mut buffer = [0_u8; 64];
                loop {
                    let read = stream
                        .read(&mut buffer)
                        .expect("timed read from plain thread");
                    if read == 0 {
                        break;
                    }
                    response.extend_from_slice(&buffer[..read]);
                }
                response
            })
            .expect("spawn plain-thread stream worker");
        let response = worker.join();

        runtime.block_on(async {
            dialer.shutdown().await;
            proxy.shutdown().await;
            fixture.await.expect("daemon fixture");
        });
        assert_eq!(
            response.expect("plain-thread stream worker"),
            b"plain-thread response"
        );
    }

    #[test]
    fn last_ffi_adapter_arc_drops_iroh_instance_off_current_thread_tokio_worker() {
        let (instance, vtable) = create_test_network_module_instance();
        // SAFETY: the instance is live and remains owned until it transfers to
        // FfiNetworkModule below.
        unsafe { module_instance(instance) }
            .expect("live instance")
            .runtime()
            .expect("build private iroh runtime");

        let (destroyed_tx, destroyed_rx) = mpsc::channel();
        // SAFETY: instance/vtable were returned by the entry above; the signal
        // token stands in for the dylib Library and outlives destroy.
        let module = unsafe {
            FfiNetworkModule::from_entry_parts(
                instance,
                vtable,
                Box::new(TeardownSignal(destroyed_tx)),
            )
        }
        .expect("wrap iroh FFI module");

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build current-thread runtime");
        runtime.block_on(async move {
            // This is the final adapter/Arc owner. Its Drop must return without
            // invoking or waiting on the Iroh runtime from this Tokio worker.
            drop(module);
        });

        destroyed_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("detached teardown must destroy instance before keepalive release");
    }
}
