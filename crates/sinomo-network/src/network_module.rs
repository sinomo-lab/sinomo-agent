//! Runtime network-module system (stage C of
//! docs/plans/r0-r1-crate-boundary-refactor.md).
//!
//! A network module owns one or more non-built-in cluster transports behind
//! the [`NetworkModule`] trait: it can establish authenticated links to a
//! [`ClusterEndpointRecord`], optionally run a listener/proxy, advertise the
//! endpoints it currently serves, and classify protocol-specific endpoint
//! roles. Modules are registered in a [`NetworkModuleRegistry`]; routing code
//! dispatches by endpoint transport kind and by address scheme.
//!
//! Loopback and LAN-TCP remain built-in transports inside this crate. The daemon
//! loads native-dylib network modules (see `network_module_abi`), registers
//! them here, and installs the registry process-wide with
//! [`set_network_module_registry`] (same pattern as the cluster directory
//! provider in `directory.rs`).

use crate::direct_link::{
    AuthenticatedLink, AuthenticatedTransportMetadata, DirectLinkFuture, LinkProvider,
};
use crate::error::NetworkError;
use crate::transfer::NetworkHealthPolicy;
use arc_swap::ArcSwap;
use serde::{Deserialize, Serialize};
use sinomo_base::{ClusterEndpointRecord, ClusterTransportKind};
use sinomo_node::{ClusterId, NodeId};
use std::collections::BTreeMap;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, LazyLock, Mutex};

/// Daemon object-RPC route listing registered network modules and their
/// enabled state ([`NetworkModuleListResponse`]).
pub const NETWORK_MODULES_LIST_ROUTE: &str = "/network/modules";
/// Daemon object-RPC route toggling one network module at runtime
/// ([`NetworkModuleSetEnabledRequest`] -> [`NetworkModuleSetEnabledResponse`]).
pub const NETWORK_MODULES_SET_ENABLED_ROUTE: &str = "/network/modules/set-enabled";
/// Wire category shared by the network-module control routes.
pub const NETWORK_MODULES_RPC_CATEGORY: &str = "network.modules";
/// Capability tag advertised for network-module administration.
pub const NETWORK_MODULES_ADMIN_CAPABILITY: &str = "network.modules.admin";

/// Every network-module control object-RPC route.
pub const NETWORK_MODULES_RPC_ROUTES: &[&str] = &[
    NETWORK_MODULES_LIST_ROUTE,
    NETWORK_MODULES_SET_ENABLED_ROUTE,
];

/// Auth policy for one network-module control route; same shape as
/// `sinomo_cluster::ClusterControlRoutePolicy` so the daemon can build its
/// service routes uniformly.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NetworkModuleRoutePolicy {
    pub route: &'static str,
    pub category: &'static str,
    pub auth: crate::NetworkModuleRouteAuth,
    pub required_capabilities: &'static [&'static str],
}

const fn network_modules_admin_policy(route: &'static str) -> NetworkModuleRoutePolicy {
    NetworkModuleRoutePolicy {
        route,
        category: NETWORK_MODULES_RPC_CATEGORY,
        auth: crate::NetworkModuleRouteAuth::Admin,
        required_capabilities: &[NETWORK_MODULES_ADMIN_CAPABILITY],
    }
}

/// Auth policy table: module listing and toggling are administrative
/// operations (local daemon identity or a cluster raft voter).
pub const NETWORK_MODULES_RPC_ROUTE_POLICIES: &[NetworkModuleRoutePolicy] = &[
    network_modules_admin_policy(NETWORK_MODULES_LIST_ROUTE),
    network_modules_admin_policy(NETWORK_MODULES_SET_ENABLED_ROUTE),
];

/// Looks up the auth policy for a network-module control route.
pub fn network_modules_rpc_route_policy(route: &str) -> Option<&'static NetworkModuleRoutePolicy> {
    NETWORK_MODULES_RPC_ROUTE_POLICIES
        .iter()
        .find(|policy| policy.route == route)
}

/// Boxed future returned by network-module dial operations. `'static` so the
/// host can spawn or hold the dial without borrowing the module call site.
pub type NetworkFuture<T> = Pin<Box<dyn Future<Output = Result<T, NetworkError>> + Send + 'static>>;

/// Static description of one network module: which transports and address
/// schemes it serves. Serialized as a name-keyed SOBJ document across the
/// module ABI, so fields can be added without breaking older modules.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NetworkModuleDescriptor {
    /// Stable module id, e.g. `"mesh"`, `"serial"`, `"lora"`.
    pub id: String,
    /// Endpoint transport kinds this module can serve.
    pub transports: Vec<ClusterTransportKind>,
    /// Address schemes this module dials, e.g. `"mesh"` for `mesh://...`.
    /// Dispatch is scheme-aware, not only kind-aware: scheme-tagged addresses
    /// can ride on endpoint records with a different transport kind.
    #[serde(default)]
    pub address_schemes: Vec<String>,
    /// Whether the module can establish outbound authenticated links.
    pub dialer: bool,
    /// Whether the module runs an inbound listener/proxy when started.
    pub listener: bool,
    /// Whether the module tolerates repeated `stop()`/`start()` cycles.
    pub supports_runtime_toggle: bool,
}

/// Protocol-specific role classification for an endpoint owned by a network
/// module. The network crate needs this generic answer to avoid routing main
/// daemon traffic to a shadow/supervision endpoint, but the protocol-specific
/// parsing and stable-id rules stay inside the module.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum NetworkModuleEndpointRole {
    Unknown,
    MainDaemon,
    ShadowDaemon,
}

/// Host-provided context a network module needs to start its listener/proxy:
/// where cluster state lives on disk and how to reach the local daemon.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NetworkModuleContext {
    /// Cluster data root (`ClusterPaths::root`).
    pub cluster_root: PathBuf,
    /// Local node identity the module listens/advertises as.
    pub node_id: NodeId,
    /// Cluster id; `None` when the node has not joined a cluster yet.
    pub cluster_id: Option<ClusterId>,
    /// Local daemon control address inbound frames are forwarded to.
    pub daemon_forward_address: String,
}

/// A runtime-registered network transport implementation.
///
/// Implementations must be safe to call from any thread. The FFI adapter
/// bridges its blocking opaque-stream callbacks through bounded async pipes
/// (see `network_module_abi::FfiNetworkModule`).
pub trait NetworkModule: Send + Sync {
    /// The module's static descriptor.
    fn descriptor(&self) -> NetworkModuleDescriptor;

    /// Start the module's listener/proxy for `ctx`. Idempotent modules may
    /// treat a second `start` as a restart.
    fn start(&self, ctx: &NetworkModuleContext) -> Result<(), NetworkError>;

    /// Stop the module's listener/proxy. The module object stays alive and
    /// may be started again (runtime toggle).
    fn stop(&self) -> Result<(), NetworkError>;

    /// Establishes one authenticated adjacent-peer link.
    ///
    /// RPC/session framing and route retries live above this transport seam.
    /// Implementations authenticate the endpoint's transport principal and
    /// expose independently cancellable byte streams plus a TLS/exporter
    /// binding without interpreting application bytes.
    fn connect_link(
        &self,
        endpoint: &ClusterEndpointRecord,
        _health: NetworkHealthPolicy,
    ) -> NetworkFuture<AuthenticatedLink> {
        let module_id = self.descriptor().id;
        let endpoint = endpoint.address.clone();
        Box::pin(async move {
            Err(NetworkError::Io {
                operation: "connect network module authenticated link".to_string(),
                detail: format!(
                    "network module `{module_id}` does not support full-duplex streams for `{endpoint}`"
                ),
            })
        })
    }

    /// Endpoint records the module currently advertises (empty when the
    /// module is not listening).
    fn local_endpoints(&self) -> Result<Vec<ClusterEndpointRecord>, NetworkError>;

    /// Protocol-specific endpoint role. Default is unknown so older/simple
    /// modules do not need to implement classification.
    fn endpoint_role(&self, _endpoint: &ClusterEndpointRecord) -> NetworkModuleEndpointRole {
        NetworkModuleEndpointRole::Unknown
    }

    /// Stable transport principal authenticated when connecting to
    /// `endpoint` (for example the Iroh endpoint public key).
    ///
    /// This is deliberately transport-specific and separate from `NodeId`.
    /// The daemon compares it with inbound authenticated metadata and only
    /// then maps the endpoint back to the current cluster directory.
    fn endpoint_transport_identity(&self, _endpoint: &ClusterEndpointRecord) -> Option<String> {
        None
    }

    /// Validates the module-owned proof carried beside an authenticated
    /// transport principal and channel binding. Generic routing never parses
    /// protocol-specific proof bytes; an absent verifier fails closed.
    fn verify_authenticated_transport_metadata(
        &self,
        _metadata: &AuthenticatedTransportMetadata,
    ) -> bool {
        false
    }
}

/// Direct-link provider backed by the enabled runtime module registry.
///
/// Transport selection and address parsing remain module-owned. This adapter
/// only exposes the common authenticated-link contract to node routing.
pub struct RegisteredNetworkModuleLinkProvider {
    health: NetworkHealthPolicy,
}

impl RegisteredNetworkModuleLinkProvider {
    pub fn new(health: NetworkHealthPolicy) -> Self {
        Self { health }
    }
}

impl LinkProvider for RegisteredNetworkModuleLinkProvider {
    fn connect<'a>(
        &'a self,
        endpoint: &'a ClusterEndpointRecord,
    ) -> DirectLinkFuture<'a, AuthenticatedLink> {
        let module =
            network_module_registry().and_then(|registry| registry.module_for_endpoint(endpoint));
        let endpoint = endpoint.clone();
        let health = self.health;
        Box::pin(async move {
            let module = module.ok_or_else(|| NetworkError::Io {
                operation: "connect authenticated network-module link".to_string(),
                detail: format!(
                    "no enabled network module serves transport {:?}",
                    endpoint.transport
                ),
            })?;
            let link = module.connect_link(&endpoint, health).await?;
            if link.peer_node_id() != &endpoint.node_id {
                return Err(NetworkError::AuthDenied);
            }
            Ok(link)
        })
    }

    fn accept(&self) -> DirectLinkFuture<'_, AuthenticatedLink> {
        Box::pin(async {
            Err(NetworkError::Io {
                operation: "accept authenticated network-module link".to_string(),
                detail: "native module listeners deliver accepted streams through the daemon composition boundary"
                    .to_string(),
            })
        })
    }
}

/// One registry entry: the module, its cached descriptor, the context it was
/// registered with (used to `start()` on runtime enable), the enabled flag,
/// and whether `start()` has actually succeeded (`started`).
struct NetworkModuleEntry {
    module: Arc<dyn NetworkModule>,
    descriptor: NetworkModuleDescriptor,
    context: Option<NetworkModuleContext>,
    enabled: bool,
    started: bool,
}

/// Descriptor + runtime state, as reported by [`NetworkModuleRegistry::snapshot`]
/// and the `/network/modules` route.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NetworkModuleStatus {
    pub descriptor: NetworkModuleDescriptor,
    pub enabled: bool,
    /// Whether the module's listener/proxy is running (`start()` succeeded and
    /// no `stop()` followed). An enabled module registered before the node had
    /// a cluster identity stays `started: false` until a context arrives.
    #[serde(default)]
    pub started: bool,
}

/// Response payload of [`NETWORK_MODULES_LIST_ROUTE`].
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NetworkModuleListResponse {
    pub modules: Vec<NetworkModuleStatus>,
}

/// Request payload of [`NETWORK_MODULES_SET_ENABLED_ROUTE`].
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NetworkModuleSetEnabledRequest {
    /// Network module id ([`NetworkModuleDescriptor::id`]).
    pub id: String,
    pub enabled: bool,
}

/// Response payload of [`NETWORK_MODULES_SET_ENABLED_ROUTE`].
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NetworkModuleSetEnabledResponse {
    pub module: NetworkModuleStatus,
}

/// Registry of runtime network modules keyed by module id.
///
/// Reads (dispatch, snapshots) are lock-free `arc-swap` loads; mutations
/// (register, enable/disable) serialize on an internal lock. Disabling a
/// module calls [`NetworkModule::stop`] but NEVER drops the module object:
/// for dylib-backed modules the registry's `Arc` keeps the library's vtable
/// pointers alive for the process lifetime.
#[derive(Default)]
pub struct NetworkModuleRegistry {
    entries: ArcSwap<BTreeMap<String, Arc<NetworkModuleEntry>>>,
    mutate: Mutex<()>,
}

impl NetworkModuleRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers (or replaces) `module` under its descriptor id.
    ///
    /// `context` is remembered so a later `set_enabled(id, true)` can start
    /// the module; registration itself never calls `start()` — the caller
    /// decides when an enabled module actually starts and reports the outcome
    /// through `started`.
    pub fn register(
        &self,
        module: Arc<dyn NetworkModule>,
        context: Option<NetworkModuleContext>,
        enabled: bool,
        started: bool,
    ) -> NetworkModuleDescriptor {
        let descriptor = module.descriptor();
        let entry = Arc::new(NetworkModuleEntry {
            module,
            descriptor: descriptor.clone(),
            context,
            enabled,
            started,
        });
        let _guard = self
            .mutate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut entries = BTreeMap::clone(&self.entries.load());
        entries.insert(descriptor.id.clone(), entry);
        self.entries.store(Arc::new(entries));
        descriptor
    }

    /// Enables or disables the module with `id`.
    ///
    /// Enabling starts the module with its registered context first (when it
    /// has one); the flag only flips when `start()` succeeds. Disabling flips
    /// the flag first so dispatch stops selecting the module, then calls
    /// `stop()` (skipped when the module never started); the module object is
    /// kept registered either way.
    pub fn set_enabled(
        &self,
        id: &str,
        enabled: bool,
    ) -> Result<NetworkModuleStatus, NetworkError> {
        let _guard = self
            .mutate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let entries = self.entries.load();
        let entry = entries
            .get(id)
            .ok_or_else(|| NetworkError::InvalidClusterCommand {
                detail: format!("unknown network module `{id}`"),
            })?
            .clone();
        if entry.enabled == enabled {
            return Ok(NetworkModuleStatus {
                descriptor: entry.descriptor.clone(),
                enabled,
                started: entry.started,
            });
        }
        let publish = |started: bool| {
            let mut updated = BTreeMap::clone(&entries);
            updated.insert(
                id.to_string(),
                Arc::new(NetworkModuleEntry {
                    module: entry.module.clone(),
                    descriptor: entry.descriptor.clone(),
                    context: entry.context.clone(),
                    enabled,
                    started,
                }),
            );
            self.entries.store(Arc::new(updated));
        };
        let started = if enabled {
            match &entry.context {
                Some(context) => {
                    entry.module.start(context)?;
                    true
                }
                // No context yet (pre-cluster registration): enabled for
                // dial dispatch, listener starts once a context is provided
                // (see `provide_context_and_start_enabled`).
                None => false,
            }
        } else {
            false
        };
        if enabled {
            publish(started);
        } else {
            // Publish the disabled state BEFORE stopping so dispatch stops
            // selecting the module while stop() (which may block on listener
            // shutdown) runs; a stop error is surfaced after the flip.
            publish(false);
            if entry.started {
                entry.module.stop()?;
            }
        }
        Ok(NetworkModuleStatus {
            descriptor: entry.descriptor.clone(),
            enabled,
            started,
        })
    }

    /// Replaces the stored context on the entry with `id` without starting or
    /// stopping anything; a later `set_enabled(id, true)` starts with it.
    /// Returns `false` when no module is registered under `id`.
    pub fn provide_context(&self, id: &str, context: &NetworkModuleContext) -> bool {
        let _guard = self
            .mutate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let entries = self.entries.load();
        let Some(entry) = entries.get(id) else {
            return false;
        };
        let mut updated = BTreeMap::clone(&entries);
        updated.insert(
            id.to_string(),
            Arc::new(NetworkModuleEntry {
                module: entry.module.clone(),
                descriptor: entry.descriptor.clone(),
                context: Some(context.clone()),
                enabled: entry.enabled,
                started: entry.started,
            }),
        );
        self.entries.store(Arc::new(updated));
        true
    }

    /// Stores `context` on every entry and starts each enabled module whose
    /// listener is not yet running. Called when the node gains (or changes)
    /// its cluster identity after modules were registered pre-cluster.
    ///
    /// Idempotent: already-started modules only have their stored context
    /// refreshed (no restart). Returns `(module id, start result)` for each
    /// start that was attempted.
    pub fn provide_context_and_start_enabled(
        &self,
        context: &NetworkModuleContext,
    ) -> Vec<(String, Result<(), NetworkError>)> {
        let _guard = self
            .mutate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let entries = self.entries.load();
        let mut updated = BTreeMap::clone(&entries);
        let mut attempts = Vec::new();
        for (id, entry) in entries.iter() {
            let should_start = entry.enabled && !entry.started;
            let started = if should_start {
                let result = entry.module.start(context);
                let started = result.is_ok();
                attempts.push((id.clone(), result));
                started
            } else {
                entry.started
            };
            updated.insert(
                id.clone(),
                Arc::new(NetworkModuleEntry {
                    module: entry.module.clone(),
                    descriptor: entry.descriptor.clone(),
                    context: Some(context.clone()),
                    enabled: entry.enabled,
                    started,
                }),
            );
        }
        self.entries.store(Arc::new(updated));
        attempts
    }

    /// First enabled dialer module claiming `kind` in its transports.
    pub fn module_for_transport(
        &self,
        kind: &ClusterTransportKind,
    ) -> Option<Arc<dyn NetworkModule>> {
        self.entries.load().values().find_map(|entry| {
            (entry.enabled && entry.descriptor.dialer && entry.descriptor.transports.contains(kind))
                .then(|| entry.module.clone())
        })
    }

    /// First enabled dialer module claiming `scheme` (ASCII case-insensitive)
    /// in its address schemes.
    pub fn module_for_address_scheme(&self, scheme: &str) -> Option<Arc<dyn NetworkModule>> {
        self.entries.load().values().find_map(|entry| {
            (entry.enabled
                && entry.descriptor.dialer
                && entry
                    .descriptor
                    .address_schemes
                    .iter()
                    .any(|candidate| candidate.eq_ignore_ascii_case(scheme)))
            .then(|| entry.module.clone())
        })
    }

    /// Enabled dialer module claiming this endpoint by transport or address
    /// scheme. Scheme dispatch remains useful for module-defined carriers
    /// represented by a generic transport kind.
    pub fn module_for_endpoint(
        &self,
        endpoint: &ClusterEndpointRecord,
    ) -> Option<Arc<dyn NetworkModule>> {
        self.module_for_transport(&endpoint.transport).or_else(|| {
            endpoint_address_scheme(&endpoint.address)
                .and_then(|scheme| self.module_for_address_scheme(scheme))
        })
    }

    /// Role classification for `endpoint`, when a registered module claims
    /// either its transport kind or its address scheme.
    pub fn endpoint_role(&self, endpoint: &ClusterEndpointRecord) -> NetworkModuleEndpointRole {
        self.entries
            .load()
            .values()
            .find_map(|entry| {
                let claims_transport = entry.descriptor.transports.contains(&endpoint.transport);
                let claims_scheme =
                    endpoint_address_scheme(&endpoint.address).is_some_and(|scheme| {
                        entry
                            .descriptor
                            .address_schemes
                            .iter()
                            .any(|candidate| candidate.eq_ignore_ascii_case(scheme))
                    });
                if !claims_transport && !claims_scheme {
                    return None;
                }
                match entry.module.endpoint_role(endpoint) {
                    NetworkModuleEndpointRole::Unknown => None,
                    role => Some(role),
                }
            })
            .unwrap_or(NetworkModuleEndpointRole::Unknown)
    }

    /// Returns the transport principal expected for a claimed endpoint.
    pub fn endpoint_transport_identity(&self, endpoint: &ClusterEndpointRecord) -> Option<String> {
        self.entries.load().values().find_map(|entry| {
            if !entry.enabled || !entry.started {
                return None;
            }
            let claims_transport = entry.descriptor.transports.contains(&endpoint.transport);
            let claims_scheme = endpoint_address_scheme(&endpoint.address).is_some_and(|scheme| {
                entry
                    .descriptor
                    .address_schemes
                    .iter()
                    .any(|candidate| candidate.eq_ignore_ascii_case(scheme))
            });
            (claims_transport || claims_scheme)
                .then(|| entry.module.endpoint_transport_identity(endpoint))
                .flatten()
        })
    }

    /// The module registered under `id`, regardless of enabled state (for
    /// diagnostics; dispatch must use the transport/scheme lookups).
    pub fn module(&self, id: &str) -> Option<Arc<dyn NetworkModule>> {
        self.entries
            .load()
            .get(id)
            .map(|entry| entry.module.clone())
    }

    /// Descriptor + enabled flag for every registered module, ordered by id.
    pub fn snapshot(&self) -> Vec<NetworkModuleStatus> {
        self.entries
            .load()
            .values()
            .map(|entry| NetworkModuleStatus {
                descriptor: entry.descriptor.clone(),
                enabled: entry.enabled,
                started: entry.started,
            })
            .collect()
    }

    /// Transport kinds that currently have an enabled dialer module. Cheap
    /// snapshot for the endpoint selector's dialability filter.
    pub fn enabled_dialer_transports(&self) -> Vec<ClusterTransportKind> {
        let mut transports = Vec::new();
        for entry in self.entries.load().values() {
            if !(entry.enabled && entry.descriptor.dialer) {
                continue;
            }
            for transport in &entry.descriptor.transports {
                if !transports.contains(transport) {
                    transports.push(transport.clone());
                }
            }
        }
        transports
    }

    /// Endpoint records currently advertised by enabled listener modules.
    pub fn local_endpoints(&self) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
        let mut endpoints = Vec::new();
        for entry in self.entries.load().values() {
            if !(entry.enabled && entry.started && entry.descriptor.listener) {
                continue;
            }
            endpoints.extend(entry.module.local_endpoints()?);
        }
        Ok(endpoints)
    }
}

static NETWORK_MODULE_REGISTRY: LazyLock<ArcSwap<Option<Arc<NetworkModuleRegistry>>>> =
    LazyLock::new(|| ArcSwap::from_pointee(None));

/// Installs the process-wide network-module registry. Routing consults it
/// before the built-in transport paths; without a registered registry only
/// built-in transports dial (same provider pattern as
/// [`crate::set_cluster_directory_provider`]).
pub fn set_network_module_registry(registry: Arc<NetworkModuleRegistry>) {
    NETWORK_MODULE_REGISTRY.store(Arc::new(Some(registry)));
}

/// The process-wide network-module registry, when one has been installed.
pub fn network_module_registry() -> Option<Arc<NetworkModuleRegistry>> {
    NETWORK_MODULE_REGISTRY.load().as_ref().clone()
}

/// Checks transport-authenticated ingress metadata and its module-owned proof
/// against one advertised endpoint without teaching generic routing how to
/// parse module-owned addresses or credentials.
pub fn authenticated_transport_metadata_matches_endpoint(
    metadata: &AuthenticatedTransportMetadata,
    endpoint: &ClusterEndpointRecord,
) -> bool {
    if metadata.transport != endpoint.transport {
        return false;
    }
    network_module_registry()
        .and_then(|registry| registry.module_for_endpoint(endpoint))
        .is_some_and(|module| {
            module
                .endpoint_transport_identity(endpoint)
                .is_some_and(|identity| {
                    identity == metadata.peer_transport_identity
                        && module.verify_authenticated_transport_metadata(metadata)
                })
        })
}

/// Returns the scheme of an address like `scheme://rest`. Scheme validation is
/// deliberately small and URL-parser-free because daemon endpoint addresses are
/// transport strings, not necessarily full URLs.
pub(crate) fn endpoint_address_scheme(address: &str) -> Option<&str> {
    let address = address.trim_start();
    let (scheme, _) = address.split_once("://")?;
    let scheme = scheme.trim();
    if scheme.is_empty()
        || !scheme
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'-' | b'.'))
    {
        return None;
    }
    Some(scheme)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct TestModule {
        descriptor: NetworkModuleDescriptor,
        starts: AtomicUsize,
        stops: AtomicUsize,
        last_context: Mutex<Option<NetworkModuleContext>>,
    }

    impl TestModule {
        fn new(id: &str, transports: Vec<ClusterTransportKind>, schemes: Vec<&str>) -> Self {
            Self {
                descriptor: NetworkModuleDescriptor {
                    id: id.to_string(),
                    transports,
                    address_schemes: schemes.into_iter().map(str::to_string).collect(),
                    dialer: true,
                    listener: true,
                    supports_runtime_toggle: true,
                },
                starts: AtomicUsize::new(0),
                stops: AtomicUsize::new(0),
                last_context: Mutex::new(None),
            }
        }
    }

    impl NetworkModule for TestModule {
        fn descriptor(&self) -> NetworkModuleDescriptor {
            self.descriptor.clone()
        }

        fn start(&self, ctx: &NetworkModuleContext) -> Result<(), NetworkError> {
            self.starts.fetch_add(1, Ordering::SeqCst);
            *self.last_context.lock().expect("context lock") = Some(ctx.clone());
            Ok(())
        }

        fn stop(&self) -> Result<(), NetworkError> {
            self.stops.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn local_endpoints(&self) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
            Ok(Vec::new())
        }

        fn endpoint_role(&self, endpoint: &ClusterEndpointRecord) -> NetworkModuleEndpointRole {
            if endpoint.address.contains("shadow") {
                NetworkModuleEndpointRole::ShadowDaemon
            } else {
                NetworkModuleEndpointRole::MainDaemon
            }
        }

        fn endpoint_transport_identity(&self, endpoint: &ClusterEndpointRecord) -> Option<String> {
            endpoint
                .address
                .split_once("://")
                .map(|(_, identity)| identity.to_string())
        }

        fn verify_authenticated_transport_metadata(
            &self,
            metadata: &AuthenticatedTransportMetadata,
        ) -> bool {
            metadata.transport_proof == b"test-transport-proof"
        }
    }

    fn test_context() -> NetworkModuleContext {
        NetworkModuleContext {
            cluster_root: PathBuf::from("cluster"),
            node_id: NodeId::from_legacy_named("node_network_module_registry_test")
                .expect("node id"),
            cluster_id: None,
            daemon_forward_address: "127.0.0.1:21353".to_string(),
        }
    }

    #[test]
    fn registry_dispatches_by_transport_and_scheme() {
        let registry = NetworkModuleRegistry::new();
        let mesh = Arc::new(TestModule::new(
            "mesh",
            vec![ClusterTransportKind::WebRtc],
            vec!["mesh"],
        ));
        let serial = Arc::new(TestModule::new(
            "serial",
            vec![ClusterTransportKind::LanQuic],
            vec!["serial"],
        ));
        registry.register(mesh.clone(), None, true, false);
        registry.register(serial.clone(), None, true, false);

        let by_transport = registry
            .module_for_transport(&ClusterTransportKind::WebRtc)
            .expect("module by transport");
        assert_eq!(by_transport.descriptor().id, "mesh");

        let by_scheme = registry
            .module_for_address_scheme("MESH")
            .expect("module by scheme, case-insensitive");
        assert_eq!(by_scheme.descriptor().id, "mesh");

        assert!(
            registry
                .module_for_transport(&ClusterTransportKind::HttpsRelay)
                .is_none(),
            "unclaimed transports must not dispatch"
        );
        assert!(registry.module_for_address_scheme("lora").is_none());

        let transports = registry.enabled_dialer_transports();
        assert!(transports.contains(&ClusterTransportKind::WebRtc));
        assert!(transports.contains(&ClusterTransportKind::LanQuic));
    }

    #[test]
    fn set_enabled_stops_and_restarts_without_dropping_the_module() {
        let registry = NetworkModuleRegistry::new();
        let module = Arc::new(TestModule::new(
            "mesh",
            vec![ClusterTransportKind::WebRtc],
            vec!["mesh"],
        ));
        registry.register(module.clone(), Some(test_context()), true, true);

        let disabled = registry.set_enabled("mesh", false).expect("disable");
        assert!(!disabled.enabled);
        assert!(!disabled.started);
        assert_eq!(module.stops.load(Ordering::SeqCst), 1);
        assert!(
            registry
                .module_for_transport(&ClusterTransportKind::WebRtc)
                .is_none(),
            "disabled modules must not dispatch"
        );
        assert!(
            registry.module("mesh").is_some(),
            "disable must keep the module registered (never dropped)"
        );
        assert_eq!(
            registry.enabled_dialer_transports(),
            Vec::<ClusterTransportKind>::new()
        );

        // Idempotent disable performs no extra stop.
        registry.set_enabled("mesh", false).expect("disable again");
        assert_eq!(module.stops.load(Ordering::SeqCst), 1);

        let enabled = registry.set_enabled("mesh", true).expect("enable");
        assert!(enabled.enabled);
        assert!(enabled.started);
        assert_eq!(module.starts.load(Ordering::SeqCst), 1);
        assert_eq!(
            module
                .last_context
                .lock()
                .expect("context lock")
                .as_ref()
                .map(|ctx| ctx.daemon_forward_address.clone()),
            Some("127.0.0.1:21353".to_string()),
            "enable must start the module with its registered context"
        );
        assert!(
            registry
                .module_for_transport(&ClusterTransportKind::WebRtc)
                .is_some()
        );

        assert!(
            registry.set_enabled("missing", true).is_err(),
            "unknown module ids are an error"
        );
    }

    #[test]
    fn snapshot_lists_descriptors_and_enabled_flags() {
        let registry = NetworkModuleRegistry::new();
        registry.register(
            Arc::new(TestModule::new(
                "mesh",
                vec![ClusterTransportKind::WebRtc],
                vec!["mesh"],
            )),
            None,
            true,
            false,
        );
        registry.register(
            Arc::new(TestModule::new(
                "lora",
                vec![ClusterTransportKind::HttpsRelay],
                vec!["lora"],
            )),
            None,
            false,
            false,
        );

        let snapshot = registry.snapshot();
        assert_eq!(snapshot.len(), 2);
        let mesh = snapshot
            .iter()
            .find(|status| status.descriptor.id == "mesh")
            .expect("module status");
        assert!(mesh.enabled);
        assert!(mesh.descriptor.dialer);
        let lora = snapshot
            .iter()
            .find(|status| status.descriptor.id == "lora")
            .expect("lora status");
        assert!(!lora.enabled);
    }

    #[test]
    fn registry_classifies_module_endpoint_roles() {
        let registry = NetworkModuleRegistry::new();
        registry.register(
            Arc::new(TestModule::new(
                "mesh",
                vec![ClusterTransportKind::WebRtc],
                vec!["mesh"],
            )),
            None,
            true,
            false,
        );
        let node_id = NodeId::from_legacy_named("node_network_module_role_test").expect("node id");
        let main = ClusterEndpointRecord {
            endpoint_id: sinomo_base::Id::generate(),
            node_id,
            transport: ClusterTransportKind::WebRtc,
            address: "mesh://main".to_string(),
            source: sinomo_base::ClusterEndpointSource::Rendezvous,
            privacy: sinomo_base::ClusterEndpointPrivacy::Relayed,
            priority: 0,
            last_observed_latency_ms: None,
        };
        let shadow = ClusterEndpointRecord {
            address: "mesh://shadow".to_string(),
            ..main.clone()
        };
        assert_eq!(
            registry.endpoint_role(&main),
            NetworkModuleEndpointRole::MainDaemon
        );
        assert_eq!(
            registry.endpoint_role(&shadow),
            NetworkModuleEndpointRole::ShadowDaemon
        );
    }

    #[test]
    fn registry_keeps_transport_identity_parsing_inside_the_claiming_module() {
        let registry = NetworkModuleRegistry::new();
        registry.register(
            Arc::new(TestModule::new(
                "mesh",
                vec![ClusterTransportKind::WebRtc],
                vec!["mesh"],
            )),
            Some(test_context()),
            true,
            true,
        );
        let endpoint = ClusterEndpointRecord {
            endpoint_id: sinomo_base::Id::generate(),
            node_id: NodeId::from_legacy_named("node_network_module_identity_test")
                .expect("node id"),
            transport: ClusterTransportKind::WebRtc,
            address: "mesh://authenticated-principal".to_string(),
            source: sinomo_base::ClusterEndpointSource::Rendezvous,
            privacy: sinomo_base::ClusterEndpointPrivacy::Relayed,
            priority: 0,
            last_observed_latency_ms: None,
        };

        assert_eq!(
            registry.endpoint_transport_identity(&endpoint).as_deref(),
            Some("authenticated-principal")
        );
    }
}
