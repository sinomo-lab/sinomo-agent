//! Host-agnostic control-plane services for native and dynamically loaded modules.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sinomo_core::network::{NetworkModuleStatus, ObjectRpcRequest};
use sinomo_core::service::{
    Service, ServiceContext, ServiceDisposition, ServiceError, ServiceFuture,
    ServiceRegistryBuilder, ServiceRoute,
};

use crate::{
    MODULE_NATIVE_RUST_LIST_ROUTE, MODULE_NATIVE_RUST_RPC_CATEGORY,
    MODULE_NATIVE_RUST_SET_ENABLED_ROUTE, ModuleId, NativeRustModuleListRequest,
    NativeRustModuleListResponse, NativeRustModuleSetEnabledRequest,
    NativeRustModuleSetEnabledResponse, NativeRustModuleStatus,
    read_native_rust_module_runtime_state, set_native_rust_module_enabled,
};

pub const MODULE_NATIVE_RUST_RPC_ROUTES: &[&str] = &[
    MODULE_NATIVE_RUST_LIST_ROUTE,
    MODULE_NATIVE_RUST_SET_ENABLED_ROUTE,
];

pub const NETWORK_MODULES_LIST_ROUTE: &str = "/network/modules";
pub const NETWORK_MODULES_SET_ENABLED_ROUTE: &str = "/network/modules/set-enabled";
pub const NETWORK_MODULES_RPC_CATEGORY: &str = "network.modules";
pub const NETWORK_MODULES_ADMIN_CAPABILITY: &str = "network.modules.admin";
pub const NETWORK_MODULES_RPC_ROUTES: &[&str] = &[
    NETWORK_MODULES_LIST_ROUTE,
    NETWORK_MODULES_SET_ENABLED_ROUTE,
];

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NetworkModuleListResponse {
    pub modules: Vec<NetworkModuleStatus>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NetworkModuleSetEnabledRequest {
    pub id: String,
    pub enabled: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NetworkModuleSetEnabledResponse {
    pub module: NetworkModuleStatus,
}

/// One native Rust module whose persisted enablement is exposed by the module
/// control service. The host supplies only the process-observed `active` bit;
/// persistence and restart-required semantics remain owned by `sinomo-module`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeRustModuleServiceTarget {
    pub legacy_id: String,
    pub active: bool,
    pub default_enabled: bool,
    pub detail: String,
}

impl NativeRustModuleServiceTarget {
    pub fn new(
        legacy_id: impl Into<String>,
        active: bool,
        default_enabled: bool,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            legacy_id: legacy_id.into(),
            active,
            default_enabled,
            detail: detail.into(),
        }
    }
}

#[derive(Clone)]
pub struct HostedNativeRustModulesService {
    data_dir_root: PathBuf,
    targets: Arc<[NativeRustModuleServiceTarget]>,
}

impl HostedNativeRustModulesService {
    pub fn new(
        data_dir_root: PathBuf,
        targets: impl IntoIterator<Item = NativeRustModuleServiceTarget>,
    ) -> Self {
        Self {
            data_dir_root,
            targets: targets.into_iter().collect::<Vec<_>>().into(),
        }
    }
}

impl Service for HostedNativeRustModulesService {
    fn name(&self) -> &'static str {
        "native-rust-modules"
    }

    fn routes(&self) -> &'static [&'static str] {
        MODULE_NATIVE_RUST_RPC_ROUTES
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        self.routes()
            .iter()
            .copied()
            .map(native_rust_module_service_route)
            .collect()
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let data_dir_root = self.data_dir_root.clone();
        let targets = Arc::clone(&self.targets);
        Box::pin(async move {
            match request.route.as_str() {
                MODULE_NATIVE_RUST_LIST_ROUTE => {
                    let _: NativeRustModuleListRequest = ctx.decode_request_body(&request).await?;
                    let response =
                        run_blocking_module_service("native Rust module list", move || {
                            native_rust_module_list_response(&data_dir_root, &targets)
                        })
                        .await?;
                    ctx.respond_value(MODULE_NATIVE_RUST_LIST_ROUTE, response)?;
                }
                MODULE_NATIVE_RUST_SET_ENABLED_ROUTE => {
                    let payload: NativeRustModuleSetEnabledRequest =
                        ctx.decode_request_body(&request).await?;
                    let response =
                        run_blocking_module_service("native Rust module enablement", move || {
                            native_rust_module_set_enabled(&data_dir_root, &targets, payload)
                        })
                        .await?;
                    ctx.respond_value(MODULE_NATIVE_RUST_SET_ENABLED_ROUTE, response)?;
                }
                other => return Err(ServiceError::UnknownRoute(other.to_string())),
            }
            Ok(ServiceDisposition::Continue)
        })
    }
}

pub fn register_native_rust_modules_service(
    builder: ServiceRegistryBuilder,
    data_dir_root: PathBuf,
    targets: impl IntoIterator<Item = NativeRustModuleServiceTarget>,
) -> ServiceRegistryBuilder {
    builder.register(HostedNativeRustModulesService::new(data_dir_root, targets))
}

fn native_rust_module_service_route(route: &'static str) -> ServiceRoute {
    let capability = if route == MODULE_NATIVE_RUST_SET_ENABLED_ROUTE {
        sinomo_core::node::MODULE_NATIVE_RUST_ADMIN_CAPABILITY
    } else {
        sinomo_core::node::MODULE_NATIVE_RUST_READ_CAPABILITY
    };
    let route = ServiceRoute::from_route_metadata(
        route,
        MODULE_NATIVE_RUST_RPC_CATEGORY,
        "cluster-member",
        [capability],
    );
    if route.route == MODULE_NATIVE_RUST_SET_ENABLED_ROUTE {
        route.require_local_permission("remote-management")
    } else {
        route
    }
}

fn native_rust_module_list_response(
    data_dir_root: &Path,
    targets: &[NativeRustModuleServiceTarget],
) -> Result<NativeRustModuleListResponse, String> {
    let modules = targets
        .iter()
        .map(|target| native_rust_module_status(data_dir_root, target))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(NativeRustModuleListResponse { modules })
}

fn native_rust_module_set_enabled(
    data_dir_root: &Path,
    targets: &[NativeRustModuleServiceTarget],
    payload: NativeRustModuleSetEnabledRequest,
) -> Result<NativeRustModuleSetEnabledResponse, String> {
    let requested_id = ModuleId::new(payload.module_id.trim().to_string())
        .or_else(|_| ModuleId::from_legacy_named(payload.module_id.trim().to_string()))
        .map_err(|error| format!("invalid module id: {error}"))?;
    let target = targets
        .iter()
        .find(|target| {
            ModuleId::from_legacy_named(target.legacy_id.as_str())
                .is_ok_and(|module_id| module_id == requested_id)
        })
        .ok_or_else(|| {
            format!(
                "module `{}` does not support native Rust enable/disable",
                payload.module_id
            )
        })?;
    let reason = if payload.reason.trim().is_empty() {
        "operator changed native Rust module state from the app".to_string()
    } else {
        payload.reason
    };
    set_native_rust_module_enabled(
        data_dir_root,
        &requested_id,
        payload.enabled,
        "daemon-module-service",
        reason,
    )
    .map_err(|error| error.to_string())?;
    Ok(NativeRustModuleSetEnabledResponse {
        module: native_rust_module_status(data_dir_root, target)?,
    })
}

fn native_rust_module_status(
    data_dir_root: &Path,
    target: &NativeRustModuleServiceTarget,
) -> Result<NativeRustModuleStatus, String> {
    let module_id = ModuleId::from_legacy_named(target.legacy_id.as_str())
        .map_err(|error| format!("invalid native Rust module target: {error}"))?;
    let state = read_native_rust_module_runtime_state(data_dir_root, &module_id)
        .map_err(|error| error.to_string())?;
    let configured_enabled = state
        .as_ref()
        .map(|state| state.enabled)
        .unwrap_or(target.default_enabled);
    let restart_required = configured_enabled != target.active;
    let state_detail = if restart_required {
        if configured_enabled {
            "configured enabled; restart the daemon to start it"
        } else {
            "configured disabled; restart the daemon to stop it"
        }
    } else if target.active {
        "active in this daemon"
    } else {
        "inactive in this daemon"
    };
    Ok(NativeRustModuleStatus {
        module_id: target.legacy_id.clone(),
        configured_enabled,
        active: target.active,
        explicit: state.is_some(),
        restart_required,
        detail: format!("{} · {state_detail}", target.detail),
    })
}

/// Process-specific access used by the host-agnostic network-module service.
/// The daemon may keep a process-wide registry and cluster-context recovery;
/// route decoding, authorization metadata, and response contracts stay here.
pub trait NetworkModulesServiceHost: Send + Sync {
    fn list_modules(&self) -> Result<Vec<NetworkModuleStatus>, String>;

    fn set_module_enabled(&self, id: &str, enabled: bool) -> Result<NetworkModuleStatus, String>;
}

#[derive(Clone)]
pub struct HostedNetworkModulesService {
    host: Arc<dyn NetworkModulesServiceHost>,
}

impl HostedNetworkModulesService {
    pub fn new(host: Arc<dyn NetworkModulesServiceHost>) -> Self {
        Self { host }
    }
}

impl Service for HostedNetworkModulesService {
    fn name(&self) -> &'static str {
        "network-modules"
    }

    fn routes(&self) -> &'static [&'static str] {
        NETWORK_MODULES_RPC_ROUTES
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        self.routes()
            .iter()
            .copied()
            .map(network_module_service_route)
            .collect()
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let host = Arc::clone(&self.host);
        Box::pin(async move {
            match request.route.as_str() {
                NETWORK_MODULES_LIST_ROUTE => {
                    let _: () = ctx.decode_request_body(&request).await?;
                    let response = run_blocking_module_service("network module list", move || {
                        host.list_modules()
                            .map(|modules| NetworkModuleListResponse { modules })
                    })
                    .await?;
                    ctx.respond_value(NETWORK_MODULES_LIST_ROUTE, response)?;
                }
                NETWORK_MODULES_SET_ENABLED_ROUTE => {
                    let payload: NetworkModuleSetEnabledRequest =
                        ctx.decode_request_body(&request).await?;
                    let response =
                        run_blocking_module_service("network module enablement", move || {
                            host.set_module_enabled(&payload.id, payload.enabled)
                                .map(|module| NetworkModuleSetEnabledResponse { module })
                        })
                        .await?;
                    ctx.respond_value(NETWORK_MODULES_SET_ENABLED_ROUTE, response)?;
                }
                other => return Err(ServiceError::UnknownRoute(other.to_string())),
            }
            Ok(ServiceDisposition::Continue)
        })
    }
}

pub fn register_network_modules_service(
    builder: ServiceRegistryBuilder,
    host: Arc<dyn NetworkModulesServiceHost>,
) -> ServiceRegistryBuilder {
    builder.register(HostedNetworkModulesService::new(host))
}

fn network_module_service_route(route: &'static str) -> ServiceRoute {
    ServiceRoute::from_route_metadata(
        route,
        NETWORK_MODULES_RPC_CATEGORY,
        "admin",
        [NETWORK_MODULES_ADMIN_CAPABILITY],
    )
}

async fn run_blocking_module_service<T, F>(
    label: &'static str,
    operation: F,
) -> Result<T, ServiceError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, String> + Send + 'static,
{
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|error| ServiceError::Service(format!("{label} worker failed: {error}")))?
        .map_err(ServiceError::Service)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_rust_module_enablement_reports_restart_required() {
        let root = sinomo_testdir::TestDir::new("sinomo-module-native-rust-service");
        let targets = [NativeRustModuleServiceTarget::new(
            "sinomo-dfs",
            true,
            true,
            "distributed filesystem provider module",
        )];
        let response = native_rust_module_set_enabled(
            &root,
            &targets,
            NativeRustModuleSetEnabledRequest {
                module_id: "sinomo-dfs".to_string(),
                enabled: false,
                reason: "test disabled DFS".to_string(),
            },
        )
        .expect("persist native Rust module state");

        assert!(!response.module.configured_enabled);
        assert!(response.module.active);
        assert!(response.module.explicit);
        assert!(response.module.restart_required);
    }

    #[test]
    fn module_control_routes_keep_their_authority_metadata() {
        let native = native_rust_module_service_route(MODULE_NATIVE_RUST_SET_ENABLED_ROUTE);
        assert_eq!(native.auth_scope.as_deref(), Some("cluster-member"));
        assert_eq!(
            native.required_local_permissions,
            vec!["remote-management".to_string()]
        );

        let network = network_module_service_route(NETWORK_MODULES_SET_ENABLED_ROUTE);
        assert_eq!(network.auth_scope.as_deref(), Some("admin"));
        assert_eq!(
            network.required_capabilities,
            vec![NETWORK_MODULES_ADMIN_CAPABILITY.to_string()]
        );
    }
}
