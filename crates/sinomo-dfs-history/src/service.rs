//! Module-owned filesystem feature routes and lifecycle.
use crate::*;
use sinomo_core::local::SinomoDataDir;
use sinomo_core::network::ObjectRpcRequest;
use sinomo_core::service::{
    Service, ServiceContext, ServiceDisposition, ServiceError, ServiceFuture,
    ServiceRegistryBuilder, ServiceRoute,
};
use sinomo_dfs::{
    DfsBrowserBackend, DfsBrowserMutationAttempt, DfsBrowserServiceHost,
    authorize_dfs_core_component_access, dfs_browser_component_host,
    dfs_core_component_requirement, normalize_sifs_browser_path, run_blocking_browser_task,
    run_configured_browser_task, run_served_browser_route,
};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
const DAEMON_RPC_CATEGORY: &str = "daemon";
fn dfs_browser_service_error(error: impl std::fmt::Display) -> ServiceError {
    ServiceError::Service(error.to_string())
}
struct FeatureBackend {
    core: Arc<DfsBrowserBackend>,
    host: Arc<dyn DfsBrowserServiceHost>,
}
impl FeatureBackend {
    fn for_data_dir(
        data_dir: Arc<SinomoDataDir>,
        runtime: &tokio::runtime::Handle,
        _host: Arc<dyn DfsBrowserServiceHost>,
    ) -> Result<Self, ServiceError> {
        let host = dfs_browser_component_host(&data_dir)?;
        let core = DfsBrowserBackend::for_component_data_dir(Arc::clone(&data_dir), runtime)?;
        Ok(Self { core, host })
    }
    fn event_origin_node_id(&self) -> Result<sinomo_core::Id, ServiceError> {
        self.core.event_origin_node_id()
    }
}

impl FeatureBackend {
    fn list_file_history(
        &self,
        request: DfsFileHistoryListRequest,
    ) -> Result<DfsFileHistoryListResponse, ServiceError> {
        self.core.with_read_service(|service| {
            service
                .history()
                .list_file_history(request)
                .map_err(dfs_browser_service_error)
        })
    }
    fn read_file_history(
        &self,
        request: DfsFileHistoryReadRequest,
    ) -> Result<DfsFileHistoryReadOutcome, ServiceError> {
        self.core.with_read_service(|service| {
            service
                .history()
                .read_file_history(request)
                .map_err(dfs_browser_service_error)
        })
    }
    fn try_restore_file_history(
        &self,
        mut request: DfsFileHistoryRestoreRequest,
    ) -> Result<
        DfsBrowserMutationAttempt<DfsFileHistoryRestoreRequest, DfsFileHistoryRestoreResponse>,
        ServiceError,
    > {
        request.destination_path = normalize_sifs_browser_path(&request.destination_path)?;
        let path = request.destination_path.clone();
        let result = self.core.try_with_configured_mutation_service(
            &[],
            request,
            |service, configuration, request| {
                let options = self.core.write_options_for_path_with_configuration(
                    service,
                    &path,
                    configuration,
                )?;
                service
                    .history()
                    .restore_file_history(request, options)
                    .map_err(dfs_browser_service_error)
            },
        );
        if !matches!(
            &result,
            Ok(DfsBrowserMutationAttempt::ConfigurationPending(_))
        ) {
            self.host.notify_maintenance();
        }
        result
    }
    fn file_history_policy(
        &self,
        request: DfsFileHistoryPolicyRequest,
    ) -> Result<DfsFileHistoryPolicy, ServiceError> {
        self.core.with_read_service(|service| {
            service
                .history()
                .file_history_policy(&request.file_system_id)
                .map_err(dfs_browser_service_error)
        })
    }
    fn update_file_history_policy(
        &self,
        request: DfsFileHistoryPolicyUpdateRequest,
    ) -> Result<DfsFileHistoryPolicy, ServiceError> {
        let result = self.core.with_mutation_service(|service| {
            service
                .history()
                .update_file_history_policy(request)
                .map_err(dfs_browser_service_error)
        });
        self.host.notify_maintenance();
        result
    }
    fn file_history_pin(
        &self,
        request: DfsFileHistoryPinRequest,
    ) -> Result<DfsFileHistoryPinResponse, ServiceError> {
        let result = self.core.with_mutation_service(|service| {
            service
                .history()
                .file_history_pin(request)
                .map_err(dfs_browser_service_error)
        });
        self.host.notify_maintenance();
        result
    }
}

pub const FEATURE_ROUTES: &[&str] = &[
    DAEMON_DFS_BROWSER_FILE_HISTORY_PIN_ROUTE,
    DAEMON_DFS_BROWSER_FILE_HISTORY_LIST_ROUTE,
    DAEMON_DFS_BROWSER_FILE_HISTORY_READ_ROUTE,
    DAEMON_DFS_BROWSER_FILE_HISTORY_RESTORE_ROUTE,
    DAEMON_DFS_BROWSER_FILE_HISTORY_POLICY_ROUTE,
    DAEMON_DFS_BROWSER_FILE_HISTORY_POLICY_UPDATE_ROUTE,
];

struct HostedFeatureService {
    runtime: tokio::runtime::Handle,
    host: Arc<dyn DfsBrowserServiceHost>,
    core_client: sinomo_module::ComponentServiceClient,
}
impl Service for HostedFeatureService {
    fn name(&self) -> &'static str {
        "dfs-history"
    }
    fn routes(&self) -> &'static [&'static str] {
        FEATURE_ROUTES
    }
    fn route_categories(&self) -> Vec<ServiceRoute> {
        FEATURE_ROUTES.iter().copied().map(feature_route).collect()
    }
    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let runtime = self.runtime.clone();
        let backend_host = Arc::clone(&self.host);
        let effect_host = Arc::clone(&self.host);
        let core_client = self.core_client.clone();
        Box::pin(async move {
            authorize_dfs_core_component_access(&core_client).await?;
            let _owner = backend_host
                .operation_guard()?
                .ok_or_else(|| dfs_browser_service_error("DFS component owner is unavailable"))?;
            let route = request.route.clone();
            let request_traceparent = request.traceparent().map(ToString::to_string);
            let data_dir = Arc::clone(&ctx.data_dir);
            match route.as_str() {
                DAEMON_DFS_BROWSER_FILE_HISTORY_LIST_ROUTE => {
                    let payload: DfsFileHistoryListRequest =
                        ctx.decode_request_body(&request).await?;
                    let result = run_served_browser_route(
                        &request,
                        run_blocking_browser_task(
                            Arc::clone(&data_dir),
                            route.clone(),
                            "DFS history list",
                            move || {
                                FeatureBackend::for_data_dir(
                                    data_dir,
                                    &runtime,
                                    Arc::clone(&backend_host),
                                )?
                                .list_file_history(payload)
                            },
                        ),
                    )
                    .await?;
                    ctx.respond_value(DAEMON_DFS_BROWSER_FILE_HISTORY_LIST_ROUTE, result)?;
                }
                DAEMON_DFS_BROWSER_FILE_HISTORY_READ_ROUTE => {
                    let payload: DfsFileHistoryReadRequest =
                        ctx.decode_request_body(&request).await?;
                    let result = run_served_browser_route(
                        &request,
                        run_blocking_browser_task(
                            Arc::clone(&data_dir),
                            route.clone(),
                            "DFS history read",
                            move || {
                                FeatureBackend::for_data_dir(
                                    data_dir,
                                    &runtime,
                                    Arc::clone(&backend_host),
                                )?
                                .read_file_history(payload)
                            },
                        ),
                    )
                    .await?;
                    ctx.respond_value(DAEMON_DFS_BROWSER_FILE_HISTORY_READ_ROUTE, result)?;
                }
                DAEMON_DFS_BROWSER_FILE_HISTORY_RESTORE_ROUTE => {
                    let payload: DfsFileHistoryRestoreRequest =
                        ctx.decode_request_body(&request).await?;
                    let backend_data_dir = Arc::clone(&data_dir);
                    let backend_runtime = runtime.clone();
                    let attempt_host = Arc::clone(&backend_host);
                    let result = run_served_browser_route(
                        &request,
                        run_configured_browser_task(
                            Arc::clone(&data_dir),
                            route.clone(),
                            "DFS history restore",
                            runtime.clone(),
                            Arc::clone(&backend_host),
                            payload,
                            move |payload| {
                                let backend = FeatureBackend::for_data_dir(
                                    Arc::clone(&backend_data_dir),
                                    &backend_runtime,
                                    Arc::clone(&attempt_host),
                                )?;
                                match backend.try_restore_file_history(payload)? {
                                    DfsBrowserMutationAttempt::Complete(response) => {
                                        Ok(DfsBrowserMutationAttempt::Complete((
                                            response,
                                            backend.event_origin_node_id()?,
                                        )))
                                    }
                                    DfsBrowserMutationAttempt::ConfigurationPending(original) => {
                                        Ok(DfsBrowserMutationAttempt::ConfigurationPending(
                                            original,
                                        ))
                                    }
                                }
                            },
                        ),
                    )
                    .await?;
                    effect_host.schedule_metadata_publish(
                        data_dir.as_ref(),
                        &runtime,
                        request_traceparent.as_deref(),
                    );
                    effect_host.publish_namespace_change(
                        result.1,
                        "restore-file-history",
                        vec![result.0.path.clone()],
                    );
                    ctx.respond_value(DAEMON_DFS_BROWSER_FILE_HISTORY_RESTORE_ROUTE, result.0)?;
                }
                DAEMON_DFS_BROWSER_FILE_HISTORY_POLICY_ROUTE => {
                    let payload: DfsFileHistoryPolicyRequest =
                        ctx.decode_request_body(&request).await?;
                    let result = run_served_browser_route(
                        &request,
                        run_blocking_browser_task(
                            Arc::clone(&data_dir),
                            route.clone(),
                            "DFS history policy",
                            move || {
                                FeatureBackend::for_data_dir(
                                    data_dir,
                                    &runtime,
                                    Arc::clone(&backend_host),
                                )?
                                .file_history_policy(payload)
                            },
                        ),
                    )
                    .await?;
                    ctx.respond_value(DAEMON_DFS_BROWSER_FILE_HISTORY_POLICY_ROUTE, result)?;
                }
                DAEMON_DFS_BROWSER_FILE_HISTORY_POLICY_UPDATE_ROUTE => {
                    let payload: DfsFileHistoryPolicyUpdateRequest =
                        ctx.decode_request_body(&request).await?;
                    let result = run_served_browser_route(
                        &request,
                        run_blocking_browser_task(
                            Arc::clone(&data_dir),
                            route.clone(),
                            "DFS history policy update",
                            move || {
                                FeatureBackend::for_data_dir(
                                    data_dir,
                                    &runtime,
                                    Arc::clone(&backend_host),
                                )?
                                .update_file_history_policy(payload)
                            },
                        ),
                    )
                    .await?;
                    ctx.respond_value(DAEMON_DFS_BROWSER_FILE_HISTORY_POLICY_UPDATE_ROUTE, result)?;
                }
                DAEMON_DFS_BROWSER_FILE_HISTORY_PIN_ROUTE => {
                    let payload: crate::DfsFileHistoryPinRequest =
                        ctx.decode_request_body(&request).await?;
                    let backend_data_dir = Arc::clone(&data_dir);
                    let backend_runtime = runtime.clone();
                    let result = run_served_browser_route(
                        &request,
                        run_blocking_browser_task(
                            Arc::clone(&data_dir),
                            route.clone(),
                            "DFS file history pin",
                            move || {
                                FeatureBackend::for_data_dir(
                                    backend_data_dir,
                                    &backend_runtime,
                                    Arc::clone(&backend_host),
                                )?
                                .file_history_pin(payload)
                            },
                        ),
                    )
                    .await?;
                    effect_host.schedule_metadata_publish(
                        data_dir.as_ref(),
                        &runtime,
                        request_traceparent.as_deref(),
                    );
                    ctx.respond_value(DAEMON_DFS_BROWSER_FILE_HISTORY_PIN_ROUTE, result)?;
                }
                other => return Err(ServiceError::UnknownRoute(other.to_owned())),
            }
            Ok(ServiceDisposition::Continue)
        })
    }
}
fn feature_route(route: &'static str) -> ServiceRoute {
    let path_access = !matches!(
        route,
        DAEMON_DFS_BROWSER_FILE_HISTORY_POLICY_ROUTE
            | DAEMON_DFS_BROWSER_FILE_HISTORY_POLICY_UPDATE_ROUTE
    );
    let descriptor = ServiceRoute::new(route, DAEMON_RPC_CATEGORY)
        .with_auth_scope(if path_access {
            "local-or-cluster-member"
        } else {
            "local-command"
        })
        .require_capability("daemon.dfs.browser")
        .with_concurrency_limit(4)
        .with_timeout(Duration::from_secs(60));
    if path_access {
        descriptor.require_capability(sinomo_dfs::SIFS_PATH_ACCESS_CAPABILITY)
    } else {
        descriptor
    }
}
pub fn module_id() -> sinomo_module::ModuleId {
    sinomo_module::ModuleId::from_legacy_named("module.dfs.history")
        .expect("valid filesystem feature module id")
}
pub fn module_enabled(root: impl AsRef<Path>) -> Result<bool, sinomo_dfs::DfsError> {
    sinomo_module::native_rust_module_enabled(root.as_ref(), &module_id(), true).map_err(|error| {
        sinomo_dfs::DfsError::Metadata {
            message: error.to_string(),
        }
    })
}
pub fn module_registration() -> sinomo_module::ModuleRegistration {
    let mut registration = sinomo_module::ModuleRegistration::native_rust(
        module_id(),
        env!("CARGO_PKG_VERSION"),
        "sinomo-dfs-history",
        env!("CARGO_PKG_NAME"),
        "sinomo.dfs.history",
        sinomo_core::TrustRing::R1TrustedRuntime,
        "sinomo-dfs-history",
    )
    .with_display_name("File History");
    let mut surfaces =
        sinomo_module::ModuleSurfaces::default().with_storage_owner("sinomo-dfs-history");
    for route in FEATURE_ROUTES.iter().copied() {
        let descriptor = feature_route(route);
        let auth = if descriptor.auth_scope.as_deref() == Some("component") {
            sinomo_module::ModuleRouteAuth::Component
        } else if descriptor.auth_scope.as_deref() == Some("local-command") {
            sinomo_module::ModuleRouteAuth::LocalCommand
        } else {
            sinomo_module::ModuleRouteAuth::LocalOrClusterMember
        };
        let surface = sinomo_module::ModuleObjectRpcRoute::new(
            route,
            DAEMON_RPC_CATEGORY,
            if descriptor.auth_scope.as_deref() == Some("component") {
                "dfs.attachments.manage"
            } else {
                "daemon.dfs.browser"
            },
            auth,
        )
        .with_concurrency_limit(if descriptor.auth_scope.as_deref() == Some("component") {
            16
        } else {
            4
        })
        .with_timeout_ms(60_000);
        surfaces = surfaces.with_object_rpc_route(surface);
    }
    let capabilities = sinomo_core::ModuleCapabilities {
        imports: [
            "dfs.semantic.access",
            "storage.local.read",
            "storage.local.write",
            "network.object_rpc",
            "cluster.kv.read",
        ]
        .into_iter()
        .map(|name| sinomo_core::CapabilityName::new(name).expect("valid feature import"))
        .collect(),
        exports: ["daemon.dfs.browser", "sifs.path.access"]
            .into_iter()
            .map(|name| sinomo_core::CapabilityName::new(name).expect("valid feature export"))
            .collect(),
    };
    surfaces.capabilities = capabilities.clone();
    registration.manifest.capabilities = capabilities;
    registration.manifest.limits = sinomo_core::ModuleResourceLimits {
        network: sinomo_core::NetworkAccess::Allowlisted,
        filesystem: sinomo_core::FileSystemAccess::Scoped,
        ..Default::default()
    };
    registration.surfaces = surfaces;
    let mut browser_offer = sinomo_module::ServiceOffer::new(
        "dfs-history-browser",
        "sinomo.dfs.history.browser",
        "1.0.0",
    )
    .cluster_visible()
    .with_provider_capability("daemon.dfs.browser")
    .with_provider_capability(sinomo_dfs::SIFS_PATH_ACCESS_CAPABILITY);
    for route in FEATURE_ROUTES.iter().copied() {
        if feature_route(route).auth_scope.as_deref() != Some("component") {
            browser_offer = browser_offer.with_route(route);
        }
    }
    let descriptor =
        sinomo_module::ComponentDescriptor::new(module_id().as_str(), env!("CARGO_PKG_VERSION"))
            .with_display_name("File History")
            .with_offer(browser_offer)
            .require(dfs_core_component_requirement());
    registration.component = descriptor;
    registration
}
pub fn component_factory()
-> Result<sinomo_module::StaticComponentFactory, sinomo_module::ComponentError> {
    let descriptor = sinomo_module::component_descriptor_from_registration(&module_registration())?;
    let caller = descriptor.clone();
    sinomo_module::StaticComponentFactory::new(descriptor, move |context, scope| {
        let runtime = context.runtime().cloned().ok_or_else(|| {
            sinomo_module::ComponentError::component(
                &caller.component_id,
                "filesystem feature requires the core runtime",
            )
        })?;
        let host = dfs_browser_component_host(context.data_dir()).map_err(|error| {
            sinomo_module::ComponentError::component(&caller.component_id, error)
        })?;
        let core_client = context.service_client(&caller)?;
        let service = HostedFeatureService {
            runtime,
            host,
            core_client,
        };
        scope.set_services(ServiceRegistryBuilder::default().register(service).build());
        Ok(())
    })
}
pub fn register_storage_paths(
    layout: &mut sinomo_core::storage::StorageLayout,
) -> Result<(), sinomo_core::storage::StorageContractError> {
    let _ = layout; // Retained revisions and pins use the core metadata authority.
    Ok(())
}

#[cfg(test)]
#[path = "service/configuration_tests.rs"]
mod configuration_tests;
