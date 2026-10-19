//! The DFS control-plane service (host-agnostic).
//!
//! Serves the DFS object-RPC routes against a [`ServiceContext`]. Everything is derived
//! from the request's [`ServiceContext::data_dir`] using public `sinomo-dfs`/
//! `sinomo-network` APIs, so DFS runs with no daemon and no live cluster (limited to
//! whatever is materialized locally on disk). Route names are owned by `sinomo-dfs`;
//! `sinomo-network` only provides the object-RPC framing. Cluster-replica routes bind the
//! authenticated service signer to the claimed metadata source and establish an authoritative
//! cluster-KV read before validating the exact shard role and epoch. They fail closed without that
//! cluster context.
//!
//! Covers the host-agnostic DFS control-plane routes. Smoke and typed message
//! data-plane routes bind to a [`DfsDataPlaneHost`], so `sinomo-dfs` still owns
//! the route contract while an embedding process supplies cluster transport.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::direct_read::handle_dfs_direct_read_request_with_initialized_content_store;
use crate::{
    DFS_CONTROL_PLANE_ROUTES, DFS_DATA_PLANE_SERVICE_ROUTES, DFS_DIRECT_READ_ROUTE,
    DFS_INSPECTION_ROUTE, DFS_METADATA_JOURNAL_BATCH_ROUTE, DFS_METADATA_SNAPSHOT_INSTALL_ROUTE,
    DFS_SMOKE_ROUTE, DFS_STATUS_ROUTE, DaemonDfsInspectionRequest, DaemonDfsInspectionResponse,
    DfsFileSystemId, DfsMessageCommand, DfsMessageResponse, DfsMetadataJournalBatchCommand,
    DfsMetadataJournalBatchResponse, DfsMetadataReplicaShardContext, DfsMetadataShardConfig,
    DfsMetadataShardId, DfsMetadataShardMembershipHandoff, DfsMetadataSnapshotInstallCommand,
    DfsMetadataSnapshotInstallResponse, DfsOperatorStatus, DfsOutboundAttachment, DfsSmokeReport,
    LocalContentStore, dfs_module_surfaces, dfs_route_is_message_data_plane,
    inspect_cluster_aware_dfs, inspect_cluster_aware_dfs_status, metadata_shard_key,
    metadata_shard_membership_handoff_key,
};
use sinomo_core::local::SinomoDataDir;
#[cfg(test)]
use sinomo_core::network::ObjectRpcResponse;
use sinomo_core::network::{ClusterPaths, ObjectRpcIncomingBody, ObjectRpcRequest};
use sinomo_core::node::NodeId;

use sinomo_core::service::{
    ClusterAccess, Service, ServiceBodyMode, ServiceContext, ServiceDisposition, ServiceError,
    ServiceFuture, ServiceRegistryBuilder, ServiceRoute,
};

const DFS_DEFAULT_ROUTE_CONCURRENCY_LIMIT: usize = 8;
const DFS_DATA_PLANE_ROUTE_CONCURRENCY_LIMIT: usize = 64;
const DFS_METRIC_EXPORT_ROUTE: &str = "/dfs/metrics/export";

struct DfsRepairRetryService {
    paths: crate::runtime_host::DfsRuntimePaths,
}

impl Service for DfsRepairRetryService {
    fn name(&self) -> &'static str {
        "dfs-repair-retry"
    }
    fn routes(&self) -> &'static [&'static str] {
        &[sinomo_api::dfs::DFS_REPAIR_RETRY_ROUTE]
    }
    fn route_categories(&self) -> Vec<ServiceRoute> {
        vec![
            ServiceRoute::new(
                sinomo_api::dfs::DFS_REPAIR_RETRY_ROUTE,
                crate::DFS_RPC_CATEGORY,
            )
            .with_auth_scope("admin")
            .require_capability("dfs.repair.retry")
            .with_concurrency_limit(1)
            .with_timeout(Duration::from_secs(30)),
        ]
    }
    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        Box::pin(async move {
            let request: sinomo_api::dfs::DfsRepairRetryRequest =
                ctx.decode_request_body_bounded(&request, 32 * 1024).await?;
            let generation =
                crate::runtime_host::request_daemon_dfs_maintenance_reconciliation_with_namespace_jobs(&self.paths, request.compact_namespace_jobs.clone())
                    .map_err(|error| ServiceError::Service(error.to_string()))?;
            ctx.respond_value(
                sinomo_api::dfs::DFS_REPAIR_RETRY_ROUTE,
                sinomo_api::dfs::DfsRepairRetryResponse {
                    request_id: request.request_id,
                    requested_generation: generation,
                    compact_namespace_jobs: request.compact_namespace_jobs,
                },
            )?;
            Ok(ServiceDisposition::Continue)
        })
    }
}

struct DfsMetricExporterService;

struct DfsMountedMutationHandoffService {
    paths: crate::runtime_host::DfsRuntimePaths,
    runtime: tokio::runtime::Handle,
}

impl Service for DfsMountedMutationHandoffService {
    fn name(&self) -> &'static str {
        "dfs-mounted-mutation-handoff"
    }

    fn routes(&self) -> &'static [&'static str] {
        &[crate::DFS_MOUNTED_MUTATION_HANDOFF_V1_ROUTE]
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        vec![
            ServiceRoute::new(
                crate::DFS_MOUNTED_MUTATION_HANDOFF_V1_ROUTE,
                crate::DFS_MOUNTED_MUTATION_RPC_CATEGORY,
            )
            .with_auth_scope("local-command")
            .require_capability("dfs.mounted-mutation.handoff")
            .with_timeout(Duration::from_secs(30)),
        ]
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let paths = self.paths.clone();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            let handoff: crate::MountedSifsMutationHandoffRequest = ctx
                .decode_request_body_bounded(
                    &request,
                    crate::DFS_MOUNTED_MUTATION_HANDOFF_MAX_PAYLOAD_BYTES,
                )
                .await?;
            handoff.validate().map_err(ServiceError::Service)?;
            let traceparent = request.traceparent().map(ToString::to_string);
            let response = tokio::task::spawn_blocking(move || {
                crate::runtime_host::handle_dfs_mounted_mutation_handoff(
                    &paths.data_dir()?,
                    &runtime,
                    traceparent,
                    handoff,
                )
            })
            .await
            .map_err(|error| {
                ServiceError::Service(format!("DFS mounted-mutation handoff task failed: {error}"))
            })?
            .map_err(|error| ServiceError::Service(error.to_string()))?;
            ctx.respond_value(crate::DFS_MOUNTED_MUTATION_HANDOFF_V1_ROUTE, response)?;
            Ok(ServiceDisposition::Continue)
        })
    }
}

struct DfsSourceAdoptionService {
    actor: Arc<crate::source_adoption::DfsSourceAdoptionActor>,
}
impl Service for DfsSourceAdoptionService {
    fn name(&self) -> &'static str {
        "dfs-source-adoption"
    }
    fn routes(&self) -> &'static [&'static str] {
        &[crate::source_adoption::SOURCE_ADOPTION_ROUTE]
    }
    fn route_categories(&self) -> Vec<ServiceRoute> {
        vec![dfs_service_route(
            crate::source_adoption::SOURCE_ADOPTION_ROUTE,
        )]
    }
    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let actor = Arc::clone(&self.actor);
        Box::pin(async move {
            let route = crate::source_adoption::SOURCE_ADOPTION_ROUTE;
            let target = authenticated_dfs_cluster_source_node_id(&ctx, route)?;
            let request: crate::source_adoption::DfsSourceAdoptionRequest = ctx
                .decode_request_body_bounded(&request, 4 * 1024 * 1024)
                .await?;
            // This actor owns source custody, not a metadata effect. Claim,
            // validation and release must remain reachable while the normal
            // target/source metadata continuation is parked.
            let response = tokio::task::spawn_blocking(move || actor.handle_rpc(&target, request))
                .await
                .map_err(|error| {
                    ServiceError::Service(format!("source custody task failed: {error}"))
                })?
                .map_err(service_error)?;
            ctx.respond_value(route, response)?;
            Ok(ServiceDisposition::Continue)
        })
    }
}

impl Service for DfsMetricExporterService {
    fn name(&self) -> &'static str {
        "dfs-metric-exporter"
    }

    fn routes(&self) -> &'static [&'static str] {
        &[DFS_METRIC_EXPORT_ROUTE]
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        vec![ServiceRoute::from_route_metadata(
            DFS_METRIC_EXPORT_ROUTE,
            sinomo_api::observe::OBSERVE_METRIC_EXPORTER_CATEGORY,
            "component",
            [sinomo_api::observe::OBSERVE_METRIC_EXPORTER_OPERATION],
        )]
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        Box::pin(async move {
            let export: sinomo_api::observe::ObserveMetricExportRequest =
                ctx.decode_request_body(&request).await?;
            let metrics = if crate::performance_metrics::dfs_status_metric_prefix_matches(
                export.name_prefix.as_deref(),
            ) {
                // Observation must not cold-open or scan the metadata store
                // while a resident generation is unavailable or busy. Surface
                // that omission through Observe's existing partial response.
                crate::runtime_host::resident_dfs_observe_metrics(ctx.data_dir.as_ref())
                    .map_err(ServiceError::Service)?
            } else {
                Vec::new()
            };
            let limit = if export.limit == 0 {
                sinomo_api::observe::OBSERVE_METRIC_EXPORTER_MAX_RECORDS
            } else {
                export
                    .limit
                    .min(sinomo_api::observe::OBSERVE_METRIC_EXPORTER_MAX_RECORDS)
            };
            let metrics = metrics
                .into_iter()
                .filter(|metric| {
                    export
                        .name_prefix
                        .as_ref()
                        .is_none_or(|prefix| metric.name.starts_with(prefix))
                })
                .take(limit)
                .collect();
            ctx.respond_value(
                request.route,
                sinomo_api::observe::ObserveMetricExportResponse {
                    descriptors: Vec::new(),
                    metrics,
                },
            )?;
            Ok(ServiceDisposition::Continue)
        })
    }
}

fn dfs_service_perf_trace(args: std::fmt::Arguments<'_>) {
    tracing::trace!(target: "sinomo.dfs.perf", detail = %args, "dfs service perf");
}

/// DFS control-plane service. Host-agnostic: it derives everything from the request's
/// [`ServiceContext::data_dir`], so it runs with or without a daemon/cluster.
#[derive(Default)]
pub struct DfsService {
    host: Option<Arc<dyn DfsDataPlaneHost>>,
}

impl DfsService {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_data_plane_host(host: Arc<dyn DfsDataPlaneHost>) -> Self {
        Self { host: Some(host) }
    }
}

pub fn register_services(builder: ServiceRegistryBuilder) -> ServiceRegistryBuilder {
    builder.register(DfsService::new())
}

#[cfg(test)]
pub(crate) fn register_data_plane_services(
    builder: ServiceRegistryBuilder,
    host: Arc<dyn DfsDataPlaneHost>,
) -> ServiceRegistryBuilder {
    builder.register(DfsDataPlaneService::new(host))
}

/// Register the complete DFS protocol around one process-owned host.
///
/// Sharing the host lets control-plane status reuse the daemon's resident
/// metadata generation instead of opening a parallel inspection backend.
pub(crate) fn register_services_with_data_plane_host(
    builder: ServiceRegistryBuilder,
    host: Arc<dyn DfsDataPlaneHost>,
) -> ServiceRegistryBuilder {
    builder
        .register(DfsService::with_data_plane_host(Arc::clone(&host)))
        .register(DfsDataPlaneService::new(host))
}

/// Materialize the complete built-in DFS component from generic host
/// mechanisms exposed by [`sinomo_module::ComponentContext`].
///
/// The embedding daemon does not construct DFS-specific service adapters. The
/// component owns its data lock, resident runtime host, data plane, browser
/// control surface, events, and lifecycle resources.
pub fn component_factory()
-> Result<sinomo_module::StaticComponentFactory, sinomo_module::ComponentError> {
    component_factory_with_preparation(None)
}

pub fn component_factory_with_storage_preparation(
    provider: crate::DfsStoragePreparationProviderBuilder,
) -> Result<sinomo_module::StaticComponentFactory, sinomo_module::ComponentError> {
    component_factory_with_preparation(Some(provider))
}

fn component_factory_with_preparation(
    preparation: Option<crate::DfsStoragePreparationProviderBuilder>,
) -> Result<sinomo_module::StaticComponentFactory, sinomo_module::ComponentError> {
    let descriptor =
        sinomo_module::component_descriptor_from_registration(&crate::dfs_module_registration())?
            .with_offer(sifs_path_access_service_offer())
            .with_offer(
                sinomo_module::ServiceOffer::new(
                    "dfs-semantic-filesystem",
                    crate::DFS_CORE_COMPONENT_INTERFACE,
                    crate::DFS_CORE_COMPONENT_INTERFACE_VERSION,
                )
                .with_operation(
                    sinomo_module::ServiceOperationOffer::new(
                        crate::DFS_CORE_COMPONENT_ACCESS_OPERATION,
                    )
                    .at_route(crate::DFS_CORE_COMPONENT_ACCESS_ROUTE)
                    .require_caller_capability(crate::DFS_CORE_COMPONENT_ACCESS_OPERATION),
                ),
            )
            .with_offer(dfs_metric_exporter_offer())
            .publish_event(crate::runtime_host::dfs_namespace_event_publication())
            .publish_event(crate::runtime_host::dfs_placement_event_publication())
            .subscribe_event(crate::runtime_host::host_cluster_control_subscription())
            .subscribe_event(crate::runtime_host::host_connectivity_subscription());
    let caller = descriptor.clone();
    Ok(
        sinomo_module::StaticComponentFactory::new(descriptor, move |context, scope| {
            let mut builder = ServiceRegistryBuilder::default();
            let active_runtime = context
                .runtime()
                .cloned()
                .zip(context.cluster_access())
                .zip(context.object_rpc_transport());
            if context.host_role() != sinomo_module::ComponentHostRole::Shadow
                && let Some(((runtime, cluster), object_rpc)) = active_runtime
            {
                let data_dir = context.data_dir().clone();
                let paths = crate::runtime_host::DfsRuntimePaths::for_host_role(
                    &data_dir,
                    context.host_role(),
                );
                let data_owner = match crate::source_adoption::retained_owner_for_mount(context)? {
                    Some(actual_retained_owner) => actual_retained_owner,
                    None => Arc::new(crate::runtime_host::DfsDataOwnerLock::acquire(&paths)
                        .map_err(|error| sinomo_module::ComponentError::Component {
                            component_id: caller.component_id.clone(), detail: error.to_string(),
                        })?),
                };
                #[cfg(any(test, feature = "test-support"))]
                let native_trace_mount = crate::native_shard_trace::register_mount(
                    data_dir.root().to_owned(), scope.shutdown_receiver(), &data_owner,
                    &ClusterPaths::new(&data_dir),
                );
                crate::runtime_host::register_daemon_dfs_data_owner_lock(&paths, &data_owner)
                    .map_err(|error| sinomo_module::ComponentError::Component {
                        component_id: caller.component_id.clone(),
                        detail: error.to_string(),
                    })?;
                let storage_preparation = preparation.as_ref().map(|provider| {
                    let lifecycle = scope.shutdown_receiver();
                    let provider = provider(context, lifecycle.clone())?;
                    let owner = Arc::clone(&data_owner);
                    let owned: crate::DfsStoragePreparationProvider = Arc::new(move |root, shard, budget| {
                        let _actual_owner = Arc::clone(&owner);
                        provider(root, shard, budget)
                    });
                    crate::register_dfs_storage_preparation_provider(data_dir.root().to_owned(), owned, Some(lifecycle))
                        .map_err(|error| sinomo_module::ComponentError::Component { component_id: caller.component_id.clone(), detail: error.to_string() })
                }).transpose()?;
                let events = context.event_client(&caller)?;
                let host_event_receivers = vec![
                    events
                        .subscribe(&crate::runtime_host::host_cluster_control_subscription())
                        .map_err(|error| sinomo_module::ComponentError::Component {
                            component_id: caller.component_id.clone(),
                            detail: error.to_string(),
                        })?,
                    events
                        .subscribe(&crate::runtime_host::host_connectivity_subscription())
                        .map_err(|error| sinomo_module::ComponentError::Component {
                            component_id: caller.component_id.clone(),
                            detail: error.to_string(),
                        })?,
                ];
                let host = Arc::new(crate::runtime_host::DfsRuntimeHost::new(
                    cluster,
                    object_rpc,
                    context.activity_observer(),
                ).with_event_client(events.clone()));
                crate::runtime_host::register_dfs_runtime_host(&data_dir, &host).map_err(
                    |error| sinomo_module::ComponentError::Component {
                        component_id: caller.component_id.clone(),
                        detail: error.to_string(),
                    },
                )?;
                let source_adoption = crate::source_adoption::DfsSourceAdoptionActor::from_component(
                    context,
                    Arc::clone(&data_owner),
                    scope.shutdown_receiver(),
                )?;
                builder = builder.register(DfsSourceAdoptionService {
                    actor: Arc::clone(&source_adoption),
                });
                // Recover exact retained captures before strict startup opens
                // can wait for their already-owned metadata writers.
                let source_recovery = Arc::clone(&source_adoption);
                scope.on_host_prepare(move || {
                    let actor = Arc::clone(&source_recovery);
                    async move {
                        actor.wake_reconciliation();
                        Ok(())
                    }
                });
                crate::runtime_host::mount_dfs_runtime_tasks(
                    context,
                    scope,
                    paths.clone(),
                    runtime.clone(),
                    host_event_receivers,
                )?;
                let data_plane: Arc<dyn DfsDataPlaneHost> = Arc::new(
                    crate::runtime_host::DaemonDfsDataPlaneHost::new_on_runtime_with_event_client(
                        paths.clone(),
                        &runtime,
                        events.clone(),
                    ),
                );
                builder = register_services_with_data_plane_host(builder, data_plane);
                // Bind browser readiness to the existing component-owned
                // authority once during lifecycle construction. Hot writes
                // borrow this owner without reopening bootstrap state files.
                let cluster_paths = ClusterPaths::new(&data_dir);
                let metadata_authority = match sinomo_core::cluster::read_cluster_state_without_kv_cached_shared(&cluster_paths) {
                    Ok(state) => {
                        #[cfg(any(test, feature = "test-support"))]
                        native_trace_mount.observe_mounted_identity(&state);
                        Some(crate::runtime_host::daemon_dfs_metadata_authority_generation_for_identity(
                        &cluster_paths, &state.cluster_id, &state.local_node_id, Some(&runtime),
                    ).map_err(|error| sinomo_module::ComponentError::Component {
                        component_id: caller.component_id.clone(), detail: error.to_string(),
                    })?) },
                    Err(sinomo_core::network::NetworkError::ClusterOrphan) => None,
                    Err(error) => return Err(sinomo_module::ComponentError::Component {
                        component_id: caller.component_id.clone(), detail: error.to_string(),
                    }),
                };

                let browser: Arc<dyn crate::DfsBrowserServiceHost> =
                    Arc::new(crate::runtime_host::RuntimeDfsBrowserServiceHost::new(
                        paths,
                        runtime,
                        events,
                        Arc::clone(&host),
                        Arc::downgrade(&data_owner),
                        metadata_authority,
                    ));
                crate::browser_service::register_dfs_browser_component_host(&data_dir, &browser)
                    .map_err(|error| sinomo_module::ComponentError::component(&caller.component_id, error))?;
                builder = crate::register_dfs_browser_services(
                    builder,
                    context.runtime().cloned(),
                    browser,
                );
                builder = builder.register(DfsRepairRetryService {
                    paths: crate::runtime_host::DfsRuntimePaths::for_host_role(
                        &data_dir,
                        context.host_role(),
                    ),
                });
                builder = builder.register(DfsMountedMutationHandoffService {
                    paths: crate::runtime_host::DfsRuntimePaths::for_host_role(
                        &data_dir,
                        context.host_role(),
                    ),
                    runtime: context
                        .runtime()
                        .cloned()
                        .expect("active DFS runtime was already resolved"),
                });
                let drain_host = Arc::clone(&host);
                let drain_source = Arc::clone(&source_adoption);
                scope.on_task_drain("DFS runtime children and source custody", move |deadline| {
                    let host = Arc::clone(&drain_host);
                    let source = Arc::clone(&drain_source);
                    async move {
                        host.shutdown_and_wait(deadline).await?;
                        source.closed_custody_disposition().map(|disposition| {
                            if disposition == crate::source_adoption::DfsSourceAdoptionCloseDisposition::Retained {
                                tracing::debug!(target: "sinomo.dfs.source_custody",
                                    "closed DFS source effects remain in exact retained native custody");
                            }
                        }).map_err(|error| error.to_string())
                    }
                });
                let data_dir_for_drop = data_dir.clone();
                #[cfg(any(test, feature = "test-support"))]
                let owner_paths_for_drop = crate::runtime_host::DfsRuntimePaths::for_host_role(
                    &data_dir_for_drop,
                    context.host_role(),
                );
                scope.defer(move || {
                    #[cfg(any(test, feature = "test-support"))]
                    crate::runtime_host::trace_dfs_data_owner_references(
                        &owner_paths_for_drop, "factory-disposer-started",
                    );
                    match source_adoption.closed_custody_disposition() {
                        Ok(crate::source_adoption::DfsSourceAdoptionCloseDisposition::Drained) => {}
                        Ok(crate::source_adoption::DfsSourceAdoptionCloseDisposition::Retained) => {
                            // Only the verified registry retains the actual FD.
                            // Normal remount must still prove fresh effect authority.
                            tracing::debug!(target: "sinomo.dfs.source_custody",
                                "disposed DFS host while exact source custody remains retained");
                        }
                        Err(error) => {
                            tracing::error!(%error, "DFS source custody could not close safely");
                        }
                    }
                    host.shutdown();
                    crate::browser_service::release_browser_backends(&data_dir_for_drop);
                    crate::runtime_host::unregister_dfs_runtime_host(&data_dir_for_drop);
                    drop(host);
                    drop(storage_preparation);
                    #[cfg(any(test, feature = "test-support"))]
                    drop(native_trace_mount);
                    drop(data_owner);
                    #[cfg(any(test, feature = "test-support"))]
                    crate::runtime_host::trace_dfs_data_owner_references(
                        &owner_paths_for_drop, "factory-explicit-owner-dropped",
                    );
                });
            } else {
                builder = register_services(builder);
            }
            builder = builder.register(DfsMetricExporterService);
            scope.set_services(builder.build());
            Ok(())
        })?
        .with_config_definitions(register_component_config_definitions)
        .with_storage_paths(crate::register_storage_paths),
    )
}

fn dfs_metric_exporter_offer() -> sinomo_module::ServiceOffer {
    sinomo_module::ServiceOffer::new(
        "dfs-metrics",
        sinomo_api::observe::OBSERVE_METRIC_EXPORTER_INTERFACE_ID,
        sinomo_api::observe::OBSERVE_METRIC_EXPORTER_INTERFACE_VERSION,
    )
    .with_operation(
        sinomo_module::ServiceOperationOffer::new(
            sinomo_api::observe::OBSERVE_METRIC_EXPORTER_OPERATION,
        )
        .at_route(DFS_METRIC_EXPORT_ROUTE)
        .require_caller_capability(sinomo_api::observe::OBSERVE_METRIC_EXPORTER_OPERATION),
    )
}

/// Declare the DFS-owned browser provider capability so service-catalog
/// discovery selects only nodes with an active path-access component.
fn sifs_path_access_service_offer() -> sinomo_module::ServiceOffer {
    let mut offer = sinomo_module::ServiceOffer::new(
        crate::SIFS_PATH_ACCESS_CAPABILITY,
        "sinomo.sifs.path-access",
        crate::DFS_PROTOCOL,
    )
    .cluster_visible()
    .with_provider_capability(crate::SIFS_PATH_ACCESS_CAPABILITY);
    for route in crate::DFS_CORE_BROWSER_ROUTES
        .iter()
        .copied()
        .filter(|route| crate::browser_service::daemon_dfs_browser_route_is_path_access(route))
    {
        offer = offer.with_route(route);
    }
    offer
}

fn register_component_config_definitions(
    builder: sinomo_config::ConfigRegistryBuilder,
) -> Result<sinomo_config::ConfigRegistryBuilder, String> {
    crate::dfs_module_registration()
        .surfaces
        .register_config_definitions(builder)
        .map_err(|error| error.to_string())
}

fn dfs_service_route(route: &'static str) -> ServiceRoute {
    let descriptor = dfs_module_surfaces()
        .object_rpc_route_descriptor(route)
        .expect("DFS module surfaces should be valid")
        .unwrap_or_else(|| panic!("unknown DFS service route {route}"));
    let mut route = ServiceRoute::from_route_metadata(
        descriptor.route,
        descriptor.category,
        descriptor.auth_scope,
        descriptor
            .required_capabilities
            .into_iter()
            .map(|capability| capability.to_string()),
    )
    .with_concurrency_limit(dfs_service_route_concurrency_limit(route));
    if let Some(timeout) = dfs_service_route_timeout(route.route.as_str()) {
        route = route.with_timeout(timeout);
    }
    if dfs_route_accepts_request_body(route.route.as_str()) {
        route = route.with_request_body_mode(ServiceBodyMode::Optional);
    }
    if dfs_route_returns_response_body(route.route.as_str()) {
        route = route.with_response_body_mode(ServiceBodyMode::Optional);
    }
    if matches!(
        route.route.as_str(),
        DFS_METADATA_JOURNAL_BATCH_ROUTE | DFS_METADATA_SNAPSHOT_INSTALL_ROUTE
    ) {
        route = route.with_incremental_request_value();
    }
    route
}

fn dfs_route_accepts_request_body(route: &str) -> bool {
    matches!(
        route,
        DFS_METADATA_SNAPSHOT_INSTALL_ROUTE
            | crate::DFS_DATA_WRITE_ROUTE
            | crate::DFS_DATA_DISTRIBUTED_WRITE_ROUTE
            | crate::DFS_DATA_NAMESPACE_ROUTE
            | crate::DFS_DATA_STAGE_BLOB_ROUTE
            | crate::DFS_DATA_STAGE_PATCH_ROUTE
            | crate::DFS_DATA_REPAIR_REPLICA_ROUTE
            | crate::DFS_DATA_FETCH_BLOB_RESPONSE_ROUTE
            | crate::DFS_DATA_METADATA_SNAPSHOT_INSTALL_ROUTE
    )
}

fn dfs_route_returns_response_body(route: &str) -> bool {
    matches!(
        route,
        DFS_DIRECT_READ_ROUTE | crate::DFS_DATA_READ_ROUTE | crate::DFS_DATA_FETCH_BLOB_ROUTE
    )
}

fn dfs_service_route_concurrency_limit(route: &str) -> usize {
    if dfs_route_is_message_data_plane(route) || route == DFS_DIRECT_READ_ROUTE {
        DFS_DATA_PLANE_ROUTE_CONCURRENCY_LIMIT
    } else {
        DFS_DEFAULT_ROUTE_CONCURRENCY_LIMIT
    }
}

fn dfs_service_route_timeout(route: &str) -> Option<Duration> {
    if route == DFS_STATUS_ROUTE {
        Some(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
    } else {
        None
    }
}

fn dfs_service_routes(routes: &[&'static str]) -> Vec<ServiceRoute> {
    routes.iter().copied().map(dfs_service_route).collect()
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsDataPlaneHostError {
    detail: String,
}

impl DfsDataPlaneHostError {
    pub fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }
}

impl std::fmt::Display for DfsDataPlaneHostError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl std::error::Error for DfsDataPlaneHostError {}

pub trait DfsDataPlaneHost: Send + Sync {
    fn status<'a>(&'a self) -> DfsDataPlaneHostFuture<'a, DfsOperatorStatus>;

    /// Return the process-owned content store when the embedding host already
    /// retains one for this data root. `None` keeps the host-agnostic service's
    /// bounded local fallback.
    fn shared_content_store(
        &self,
        data_dir: &SinomoDataDir,
    ) -> Result<Option<Arc<LocalContentStore>>, DfsDataPlaneHostError>;

    fn run_smoke<'a>(&'a self) -> DfsDataPlaneHostFuture<'a, DfsSmokeReport>;

    /// Inspect or mutate operator-visible metadata through the same resident
    /// backend generation and admission fences as ordinary daemon traffic.
    fn inspect<'a>(
        &'a self,
        data_dir: Arc<SinomoDataDir>,
        request: DaemonDfsInspectionRequest,
    ) -> DfsDataPlaneHostFuture<'a, DaemonDfsInspectionResponse>;

    /// Return a metadata-replica authority context from the host's retained,
    /// coherent control-plane generation. `None` means this host does not own
    /// such a projection; the service may then use a configured `ClusterAccess`
    /// current-term exact-set read, but must not promote a resident local KV
    /// projection into authority. `Some(future)` is authoritative even when the
    /// future returns `Ok(None)`, which fails closed without issuing a second
    /// authority read.
    fn retained_metadata_replica_shard_context<'a>(
        &'a self,
        _data_dir: Arc<SinomoDataDir>,
        _file_system_id: DfsFileSystemId,
        _shard_id: DfsMetadataShardId,
    ) -> Option<DfsDataPlaneHostFuture<'a, Option<DfsMetadataReplicaShardContext>>> {
        None
    }

    /// Persist an already-authorized replica journal command through the
    /// process-resident exact shard backend. The service boundary supplies the
    /// committed authority context so the host must not repeat a remote KV
    /// refresh while holding replica ingress.
    fn apply_metadata_journal_batch<'a>(
        &'a self,
        data_dir: Arc<SinomoDataDir>,
        authenticated_source_node_id: NodeId,
        command: DfsMetadataJournalBatchCommand,
        context: Option<DfsMetadataReplicaShardContext>,
    ) -> DfsDataPlaneHostFuture<'a, DfsMetadataJournalBatchResponse>;

    /// Install an already-authorized replica snapshot through the same
    /// resident exact shard generation used by ordinary journal ingress.
    fn install_metadata_snapshot<'a>(
        &'a self,
        data_dir: Arc<SinomoDataDir>,
        authenticated_source_node_id: NodeId,
        command: DfsMetadataSnapshotInstallCommand,
        context: Option<DfsMetadataReplicaShardContext>,
        body: ObjectRpcIncomingBody,
    ) -> DfsDataPlaneHostFuture<'a, DfsMetadataSnapshotInstallResponse>;

    /// Process one authenticated routed DFS message through the host's
    /// canonical authority and resident-backend path. Implementations must
    /// fail closed on stale topology, shard epoch, handoff, freeze, and inner
    /// source claims; the service adapter validates the outer signer and
    /// response envelope but cannot validate host-owned authority state.
    /// `encoded_value_bytes` is captured from that envelope before decoding;
    /// it bounds response custody and is never metadata authority evidence.
    fn handle_message<'a>(
        &'a self,
        command: DfsMessageCommand,
        attachment: Option<DfsInboundAttachment>,
        traceparent: Option<String>,
        encoded_value_bytes: Option<u64>,
    ) -> DfsDataPlaneHostFuture<'a, DfsDataPlaneResponse>;
}

/// An ownership-preserving DFS request attachment.
///
/// Whole-object data-plane handlers consume these chunks directly into the
/// content store. Other attachment kinds retain their specialized decoding
/// paths at the service boundary.
#[derive(Debug)]
pub struct DfsInboundAttachment {
    pub descriptor: crate::DfsRpcAttachmentDescriptor,
    pub body: ObjectRpcIncomingBody,
}

#[derive(Debug)]
pub struct DfsDataPlaneResponse {
    pub response: DfsMessageResponse,
    pub attachment: Option<DfsOutboundAttachment>,
}

impl DfsDataPlaneResponse {
    pub fn value(response: DfsMessageResponse) -> Self {
        Self {
            response,
            attachment: None,
        }
    }
}

pub type DfsDataPlaneHostFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, DfsDataPlaneHostError>> + Send + 'a>>;

#[derive(Clone)]
pub struct DfsDataPlaneService {
    host: Arc<dyn DfsDataPlaneHost>,
    content_stores: Arc<
        std::sync::Mutex<std::collections::BTreeMap<std::path::PathBuf, Arc<LocalContentStore>>>,
    >,
}

impl DfsDataPlaneService {
    pub fn new(host: Arc<dyn DfsDataPlaneHost>) -> Self {
        Self {
            host,
            content_stores: Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::new())),
        }
    }

    fn content_store_for(
        &self,
        data_dir: &SinomoDataDir,
    ) -> Result<Arc<LocalContentStore>, ServiceError> {
        let content_root = data_dir.dfs_dir().join("content");
        let mut stores = self.content_stores.lock().map_err(|_| {
            ServiceError::Service("DFS data-plane content-store cache is poisoned".to_string())
        })?;
        if let Some(store) = stores.get(&content_root) {
            return Ok(Arc::clone(store));
        }
        let store = match self
            .host
            .shared_content_store(data_dir)
            .map_err(service_error)?
        {
            Some(store) => store,
            None => {
                data_dir.ensure_root_layout().map_err(service_error)?;
                Arc::new(LocalContentStore::new(&content_root).map_err(service_error)?)
            }
        };
        stores.insert(content_root, Arc::clone(&store));
        Ok(store)
    }

    #[cfg(test)]
    fn cached_content_store_count(&self) -> usize {
        self.content_stores
            .lock()
            .expect("DFS data-plane content-store cache")
            .len()
    }
}

impl Service for DfsService {
    fn name(&self) -> &'static str {
        "dfs"
    }

    fn routes(&self) -> &'static [&'static str] {
        DFS_CONTROL_PLANE_ROUTES
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        dfs_service_routes(DFS_CONTROL_PLANE_ROUTES)
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let host = self.host.clone();
        Box::pin(async move {
            let mut operation_metrics = crate::performance_metrics::DfsOperationMetricGuard::start(
                request.route.as_str(),
                "control",
            );
            match request.route.as_str() {
                DFS_STATUS_ROUTE => {
                    let started_at = Instant::now();
                    let _: () = ctx.decode_request_body(&request).await?;
                    let status_started_at = Instant::now();
                    let status = if let Some(host) = host {
                        host.status().await.map_err(service_error)?
                    } else {
                        let data_dir = Arc::clone(&ctx.data_dir);
                        tokio::task::spawn_blocking(move || dfs_status(&data_dir))
                            .await
                            .map_err(|source| {
                                ServiceError::Service(format!("DFS status task failed: {source}"))
                            })??
                    };
                    let status_ms = status_started_at.elapsed().as_millis();
                    ctx.respond_value(DFS_STATUS_ROUTE, status)?;
                    dfs_service_perf_trace(format_args!(
                        "service route={} status_ms={} total_ms={}",
                        DFS_STATUS_ROUTE,
                        status_ms,
                        started_at.elapsed().as_millis()
                    ));
                }
                DFS_METADATA_JOURNAL_BATCH_ROUTE => {
                    let command: DfsMetadataJournalBatchCommand =
                        ctx.decode_request_body(&request).await?;
                    let authenticated_source_node_id = authenticated_dfs_cluster_source_node_id(
                        &ctx,
                        DFS_METADATA_JOURNAL_BATCH_ROUTE,
                    )?;
                    if command.source_node_id.as_ref() != Some(&authenticated_source_node_id) {
                        let response = DfsMetadataJournalBatchResponse {
                            request_id: command.request_id.clone(),
                            shard_id: command.batch.shard_id.clone(),
                            next_after_sequence: command.batch.next_after_sequence,
                            applied_entries: 0,
                            verified_prefix_only: false,
                            verified_authority_probe_id: None,
                            result: crate::DfsMetadataJournalBatchResult::Rejected {
                                reason: format!(
                                    "authenticated metadata journal caller {authenticated_source_node_id} does not match claimed source {}",
                                    command
                                        .source_node_id
                                        .as_ref()
                                        .map(ToString::to_string)
                                        .unwrap_or_else(|| "<missing>".to_string())
                                ),
                                code: crate::DfsMetadataRejectionCode::Other,
                            },
                        };
                        ctx.respond_value(DFS_METADATA_JOURNAL_BATCH_ROUTE, response)?;
                        operation_metrics.succeed();
                        return Ok(ServiceDisposition::Continue);
                    }
                    #[cfg(any(test, feature = "test-support"))]
                    crate::test_runtime::pause_decoded_write_commit_publication(&command).await?;
                    let may_have_deferred_materialization =
                        command.durable_replication.is_some() || command.authority_probe.is_some();
                    let materialize_shard_id = command.batch.shard_id.clone();
                    let context = authoritative_metadata_replica_shard_context(
                        host.as_deref(),
                        ctx.cluster.clone(),
                        Arc::clone(&ctx.data_dir),
                        command.file_system_id.clone(),
                        command.batch.shard_id.clone(),
                        authenticated_source_node_id.clone(),
                        command.authority_epoch,
                        command
                            .prefix_probe
                            .as_ref()
                            .and_then(|probe| probe.primary_handoff_id.clone()),
                    )
                    .await?;
                    if let Some(host) = host {
                        let response = host
                            .apply_metadata_journal_batch(
                                Arc::clone(&ctx.data_dir),
                                authenticated_source_node_id,
                                command,
                                context,
                            )
                            .await
                            .map_err(service_error)?;
                        ctx.respond_value(DFS_METADATA_JOURNAL_BATCH_ROUTE, response)?;
                        operation_metrics.succeed();
                        return Ok(ServiceDisposition::Continue);
                    }
                    let data_dir = Arc::clone(&ctx.data_dir);
                    let response = tokio::task::spawn_blocking(move || {
                        crate::metadata_replication::process_cluster_dfs_metadata_journal_batch_with_context(
                            &data_dir,
                            &authenticated_source_node_id,
                            command,
                            context,
                        )
                    })
                    .await
                    .map_err(|source| {
                        ServiceError::Service(format!("DFS metadata journal task failed: {source}"))
                    })?
                    .map_err(service_error)?;
                    // Exact frames become durable before acknowledgement-floor
                    // persistence and response delivery. Schedule the bounded
                    // rebuild even if the later acknowledgement-floor write
                    // rejects the request; pre-ingest rejections are a no-op,
                    // while post-ingest rejections still own a durable tail.
                    if may_have_deferred_materialization {
                        let data_dir = Arc::clone(&ctx.data_dir);
                        crate::metadata_replication::schedule_cluster_dfs_metadata_replica_materialization_on_runtime(
                            tokio::runtime::Handle::current(),
                            (*data_dir).clone(),
                            materialize_shard_id,
                        );
                    }
                    ctx.respond_value(DFS_METADATA_JOURNAL_BATCH_ROUTE, response)?;
                }
                DFS_METADATA_SNAPSHOT_INSTALL_ROUTE => {
                    let mut command: DfsMetadataSnapshotInstallCommand =
                        ctx.decode_request_body(&request).await?;
                    let authenticated_source_node_id = authenticated_dfs_cluster_source_node_id(
                        &ctx,
                        DFS_METADATA_SNAPSHOT_INSTALL_ROUTE,
                    )?;
                    if command.source_node_id != authenticated_source_node_id {
                        let response = DfsMetadataSnapshotInstallResponse {
                            request_id: command.request_id.clone(),
                            shard_id: command.shard_id.clone(),
                            next_after_sequence: command.next_after_sequence,
                            result: crate::DfsMetadataSnapshotInstallResult::Rejected {
                                reason: format!(
                                    "authenticated metadata snapshot caller {authenticated_source_node_id} does not match claimed source {}",
                                    command.source_node_id
                                ),
                                code: crate::DfsMetadataRejectionCode::Other,
                            },
                        };
                        ctx.respond_value(DFS_METADATA_SNAPSHOT_INSTALL_ROUTE, response)?;
                        operation_metrics.succeed();
                        return Ok(ServiceDisposition::Continue);
                    }
                    let context = authoritative_metadata_replica_shard_context(
                        host.as_deref(),
                        ctx.cluster.clone(),
                        Arc::clone(&ctx.data_dir),
                        command.file_system_id.clone(),
                        command.shard_id.clone(),
                        authenticated_source_node_id.clone(),
                        command.authority_epoch,
                        command.primary_handoff_id.clone(),
                    )
                    .await?;
                    if !ctx.has_request_attachment() {
                        return Err(ServiceError::Service(
                            "snapshot install requires a native image attachment".into(),
                        ));
                    }
                    let body = ctx.take_request_body();
                    if let Some(host) = host {
                        let response = host
                            .install_metadata_snapshot(
                                Arc::clone(&ctx.data_dir),
                                authenticated_source_node_id,
                                command,
                                context,
                                body,
                            )
                            .await
                            .map_err(service_error)?;
                        ctx.respond_value(DFS_METADATA_SNAPSHOT_INSTALL_ROUTE, response)?;
                        operation_metrics.succeed();
                        return Ok(ServiceDisposition::Continue);
                    }
                    return Err(ServiceError::Service(
                        "streamed snapshot install requires its normal owned data-plane host"
                            .into(),
                    ));
                }
                DFS_INSPECTION_ROUTE => {
                    let inspection: DaemonDfsInspectionRequest =
                        ctx.decode_request_body(&request).await?;
                    let response = match host {
                        Some(host) => host
                            .inspect(Arc::clone(&ctx.data_dir), inspection)
                            .await
                            .map_err(service_error)?,
                        None => {
                            let data_dir = Arc::clone(&ctx.data_dir);
                            tokio::task::spawn_blocking(move || {
                                dfs_inspection(&data_dir, inspection)
                            })
                            .await
                            .map_err(|source| {
                                ServiceError::Service(format!(
                                    "DFS inspection task failed: {source}"
                                ))
                            })??
                        }
                    };
                    ctx.respond_value(DFS_INSPECTION_ROUTE, response)?;
                }
                other => return Err(ServiceError::UnknownRoute(other.to_string())),
            }
            operation_metrics.succeed();
            Ok(ServiceDisposition::Continue)
        })
    }
}

impl Service for DfsDataPlaneService {
    fn name(&self) -> &'static str {
        "dfs-data-plane"
    }

    fn routes(&self) -> &'static [&'static str] {
        DFS_DATA_PLANE_SERVICE_ROUTES
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        dfs_service_routes(DFS_DATA_PLANE_SERVICE_ROUTES)
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let service = self.clone();
        Box::pin(async move {
            let mut operation_metrics = crate::performance_metrics::DfsOperationMetricGuard::start(
                request.route.as_str(),
                "data",
            );
            match request.route.as_str() {
                DFS_SMOKE_ROUTE => {
                    let _: () = ctx.decode_request_body(&request).await?;
                    let report = service.host.run_smoke().await.map_err(service_error)?;
                    ctx.respond_value(DFS_SMOKE_ROUTE, report)?;
                }
                route if dfs_route_is_message_data_plane(route) => {
                    let authenticated_source_node_id =
                        authenticated_dfs_cluster_source_node_id(&ctx, route)?;
                    let traceparent = request.traceparent().map(ToString::to_string);
                    let encoded_value_bytes = ctx.request_value_encoded_bytes(&request);
                    let (command, attachment) = if !ctx.has_request_attachment() {
                        (
                            ctx.decode_request_body::<DfsMessageCommand>(&request)
                                .await?,
                            None,
                        )
                    } else {
                        let head: crate::body_rpc::DfsAttachmentMessageCommand =
                            ctx.decode_request_body(&request).await?;
                        let request_attachment = ctx.take_request_body();
                        crate::body_rpc::validate_incoming_attachment_length(
                            &head.attachment,
                            &request_attachment,
                            "DFS request",
                        )?;
                        if matches!(
                            &head.command.routed_message.message,
                            crate::DfsNetworkMessage::StageBlob(_)
                                | crate::DfsNetworkMessage::MetadataSnapshotInstall(_)
                                | crate::DfsNetworkMessage::StageRetainedBlob(_)
                                | crate::DfsNetworkMessage::RepairRetainedBlob(_)
                                | crate::DfsNetworkMessage::DistributedWriteRequest(_)
                                | crate::DfsNetworkMessage::ExclusiveCreateRequest(_)
                                | crate::DfsNetworkMessage::RetainedWriteRequest(_)
                        ) {
                            let attachment = DfsInboundAttachment {
                                descriptor: head.attachment,
                                body: request_attachment,
                            };
                            (head.command, Some(attachment))
                        } else {
                            let max_bytes = usize::try_from(head.attachment.total_size_bytes)
                                .map_err(|_| {
                                ServiceError::Service(format!(
                                    "DFS request attachment for {route} does not fit this platform address space"
                                ))
                            })?;
                            let bytes = request_attachment.collect_to_vec(max_bytes).await?;
                            (
                                crate::body_rpc::attach_dfs_message_request_attachment(
                                    head, bytes,
                                )?,
                                None,
                            )
                        }
                    };
                    if command.routed_message.source_node_id != authenticated_source_node_id {
                        return Err(ServiceError::Service(format!(
                            "authenticated DFS message caller {authenticated_source_node_id} does not match claimed source {} for route {route}",
                            command.routed_message.source_node_id
                        )));
                    }
                    #[cfg(any(test, feature = "test-support"))]
                    crate::test_runtime::observe_decoded_namespace_call(&command);
                    let message_response = service
                        .host
                        .handle_message(command, attachment, traceparent, encoded_value_bytes)
                        .await
                        .map_err(service_error)?;
                    if let Some(attachment) = message_response.attachment {
                        let response = crate::body_rpc::encode_dfs_attachment_message_rpc_response(
                            route,
                            message_response.response,
                            attachment.descriptor,
                        )?;
                        ctx.respond_with_body(response, attachment.source)?;
                        operation_metrics.succeed();
                        return Ok(ServiceDisposition::Continue);
                    }
                    ctx.respond_value(route, message_response.response)?;
                }
                DFS_DIRECT_READ_ROUTE => {
                    let content = service.content_store_for(&ctx.data_dir)?;
                    let request = ctx
                        .decode_request_body::<crate::DfsDirectReadRequest>(&request)
                        .await?;
                    let response = handle_dfs_direct_read_request_with_initialized_content_store(
                        &content, request,
                    )
                    .map_err(service_error)?;
                    ctx.respond_with_body(response.response, response.attachment)?;
                    operation_metrics.succeed();
                    return Ok(ServiceDisposition::Continue);
                }
                other => return Err(ServiceError::UnknownRoute(other.to_string())),
            }
            operation_metrics.succeed();
            Ok(ServiceDisposition::Continue)
        })
    }
}

fn service_error(error: impl std::fmt::Display) -> ServiceError {
    ServiceError::Service(error.to_string())
}

/// Build the DFS operator status, then trim shard detail to the local node's role.
fn dfs_status(data_dir: &SinomoDataDir) -> Result<DfsOperatorStatus, ServiceError> {
    let started_at = Instant::now();
    data_dir.ensure_root_layout().map_err(service_error)?;
    dfs_service_perf_trace(format_args!(
        "status ensure_layout_ms={}",
        started_at.elapsed().as_millis()
    ));
    let status_started_at = Instant::now();
    let cluster_paths = ClusterPaths::new(data_dir);
    let status =
        inspect_cluster_aware_dfs_status(data_dir, &cluster_paths).map_err(service_error)?;
    dfs_service_perf_trace(format_args!(
        "status cluster_aware_ms={} total_ms={}",
        status_started_at.elapsed().as_millis(),
        started_at.elapsed().as_millis()
    ));
    Ok(status)
}

/// Serve a DFS operator inspection request against the local metadata backend.
fn dfs_inspection(
    data_dir: &SinomoDataDir,
    request: DaemonDfsInspectionRequest,
) -> Result<DaemonDfsInspectionResponse, ServiceError> {
    data_dir.ensure_root_layout().map_err(service_error)?;
    if matches!(&request, DaemonDfsInspectionRequest::Status) {
        return Ok(DaemonDfsInspectionResponse::Status {
            status: dfs_status(data_dir)?,
        });
    }
    let cluster_paths = ClusterPaths::new(data_dir);
    inspect_cluster_aware_dfs(data_dir, &cluster_paths, request).map_err(service_error)
}

async fn authoritative_metadata_replica_shard_context(
    host: Option<&dyn DfsDataPlaneHost>,
    _cluster: Option<Arc<dyn ClusterAccess>>,
    data_dir: Arc<SinomoDataDir>,
    file_system_id: Option<DfsFileSystemId>,
    shard_id: DfsMetadataShardId,
    _source_node_id: NodeId,
    _authority_epoch: Option<u64>,
    primary_handoff_id: Option<String>,
) -> Result<Option<DfsMetadataReplicaShardContext>, ServiceError> {
    let started_at = Instant::now();
    let Some(file_system_id) = file_system_id else {
        return Ok(None);
    };
    if let Some(retained) = host.and_then(|host| {
        host.retained_metadata_replica_shard_context(
            Arc::clone(&data_dir),
            file_system_id.clone(),
            shard_id.clone(),
        )
    }) {
        let context = retained.await.map_err(service_error)?;
        dfs_service_perf_trace(format_args!(
            "metadata-replica-authority-context shard={} mode=host-retained handoff={} total_ms={}",
            shard_id.0,
            primary_handoff_id.is_some(),
            started_at.elapsed().as_millis(),
        ));
        return Ok(context);
    }

    // A host without the existing retained authority contract is unavailable.
    // Replica ingress never starts a second control-plane authority protocol.
    dfs_service_perf_trace(format_args!(
        "metadata-replica-authority-context shard={} mode=authority-unavailable handoff={} total_ms={}",
        shard_id.0,
        primary_handoff_id.is_some(),
        started_at.elapsed().as_millis(),
    ));
    Ok(None)
}

fn authenticated_dfs_cluster_source_node_id(
    ctx: &ServiceContext,
    route: &str,
) -> Result<NodeId, ServiceError> {
    let Some((node_id, _)) = ctx.signer.cluster_identity() else {
        return Err(ServiceError::Service(format!(
            "DFS cluster route {route} requires an authenticated cluster signer identity"
        )));
    };
    node_id.parse::<NodeId>().map_err(|source| {
        ServiceError::Service(format!(
            "DFS cluster route {route} has invalid authenticated signer node id {node_id}: {source}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DFS_OBJECT_RPC_ROUTES, DFS_RPC_CATEGORY, DfsMetadataJournalFrameBatch};
    use crate::{
        DaemonDfsInspectionRequest, DaemonDfsInspectionResponse, DfsFileSystem, DfsMetadataBackend,
        DfsMetadataJournalBatch, DfsMetadataJournalBatchResponse,
        DfsMetadataJournalReplicationReport, DfsMetadataShardAssignment, DfsMetadataShardId,
        DfsMetadataSnapshotInstallCommand, DfsMetadataSnapshotInstallResponse,
        DfsMetadataSnapshotInstallResult, DfsObjectId, DfsObjectKind, DfsObjectMetadata,
        DfsObjectVersionRecord, DfsReplicaReceiptRecord, DfsReplicationPolicy, DfsSmokeAttempt,
        DfsVersion, DfsWriteIntentRecord, DfsWriteIntentStatus, LocalSobjMetadataBackend,
        default_dfs_metadata_shards_root, metadata_shard_key, sifs_file_system_id,
    };
    use sinomo_core::DfsObjectPolicy;
    use sinomo_core::cluster::ClusterKvPathsExt;
    use sinomo_core::kv::{
        ClusterKvTxnCommitResult, KvAuthoritativeRead, KvAuthorityBasis, KvAuthorityProof, KvEntry,
        KvLocalRead, KvRevision, KvTxnRequest, KvVersion,
    };
    use sinomo_core::node::NodeId;
    use sinomo_core::service::{ResponseSink, ServiceRegistry, ServiceSigner};
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use time::OffsetDateTime;

    struct UnavailableComponentObjectRpc;

    impl sinomo_module::ComponentObjectRpcTransport for UnavailableComponentObjectRpc {
        fn call(
            &self,
            _target_node_id: &NodeId,
            _call: sinomo_core::network::ObjectRpcCall,
            _options: sinomo_core::network::ClusterObjectRpcOptions,
            _observe_connectivity: bool,
        ) -> Result<sinomo_core::network::ObjectRpcResult, sinomo_core::network::NetworkError>
        {
            Err(sinomo_core::network::NetworkError::delivery_failed(
                "test component object RPC",
                sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent,
                "unavailable",
            ))
        }

        fn call_async<'a>(
            &'a self,
            target_node_id: &'a NodeId,
            call: sinomo_core::network::ObjectRpcCall,
            options: sinomo_core::network::ClusterObjectRpcOptions,
            observe_connectivity: bool,
        ) -> Pin<
            Box<
                dyn Future<
                        Output = Result<
                            sinomo_core::network::ObjectRpcResult,
                            sinomo_core::network::NetworkError,
                        >,
                    > + Send
                    + 'a,
            >,
        > {
            Box::pin(async move { self.call(target_node_id, call, options, observe_connectivity) })
        }
    }

    #[derive(Clone, Default)]
    struct VecSink {
        frames: Arc<Mutex<Vec<sinomo_core::network::ObjectRpcOutgoingResponse>>>,
    }

    impl ResponseSink for VecSink {
        fn send(
            self: Box<Self>,
            response: sinomo_core::network::ObjectRpcOutgoingResponse,
        ) -> Result<(), ServiceError> {
            self.frames.lock().expect("sink lock").push(response);
            Ok(())
        }
    }

    fn native_surface_fixture(
        name: &str,
    ) -> (
        crate::test_support::DfsTestRuntime,
        NodeId,
        Arc<SinomoDataDir>,
        Arc<crate::runtime_host::DaemonDfsDataPlaneHost>,
    ) {
        let node = NodeId::from_legacy_named(name).expect("native surface node");
        let mut runtime = crate::test_support::DfsTestRuntime::new(name, node.clone())
            .expect("shared native runtime");
        runtime.start_nodes().expect("normal embedded lifecycle");
        let data_dir = Arc::new(runtime.data_dir(&node).clone());
        let host = Arc::new(crate::runtime_host::DaemonDfsDataPlaneHost::new_on_runtime(
            crate::runtime_host::DfsRuntimePaths::for_host_role(
                &data_dir,
                sinomo_module::ComponentHostRole::Embedded,
            ),
            &runtime.runtime_handle(),
        ));
        (runtime, node, data_dir, host)
    }

    fn temp_data_dir() -> (sinomo_testdir::TestDir, Arc<SinomoDataDir>) {
        let root = sinomo_testdir::TestDir::new("sinomo-service-dfs");
        let data_dir = Arc::new(SinomoDataDir::new(&root));
        (root, data_dir)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn metric_export_never_cold_opens_metadata_when_resident_status_is_unavailable() {
        let (mut runtime, service, _) = crate::test_support::current_native_metadata_service(
            "native-metric-cold-owner",
            1,
            "/services",
        )
        .unwrap();
        let node = service.node_id().clone();
        let data_dir = Arc::new(runtime.data_dir(&node).clone());
        drop(service);
        runtime
            .stop_node(&node)
            .expect("actual owner stopped before cold metric request");
        for prefix in [None, Some("sinomo.dfs.background")] {
            crate::metadata_backend::reset_metadata_backend_open_calls();
            let request = ObjectRpcRequest::encode_body(
                DFS_METRIC_EXPORT_ROUTE,
                sinomo_api::observe::OBSERVE_METRIC_EXPORTER_CATEGORY,
                &sinomo_api::observe::ObserveMetricExportRequest {
                    name_prefix: prefix.map(str::to_string),
                    limit: 0,
                },
            )
            .unwrap();
            let context = ServiceContext::new(
                Arc::clone(&data_dir),
                None,
                ServiceSigner::Local,
                sinomo_core::network::ObjectRpcIncomingBody::empty(),
                Box::new(VecSink::default()),
            );
            let result = DfsMetricExporterService.handle(context, request).await;
            assert_eq!(
                result.is_ok(),
                prefix.is_some(),
                "unavailable resident status stays unavailable"
            );
            assert_eq!(crate::metadata_backend::metadata_backend_open_calls(), 0);
        }
        runtime.close().unwrap();
    }

    #[test]
    fn component_factory_owns_complete_dfs_runtime_surface() {
        use sinomo_module::ComponentFactory as _;
        let (mut runtime, node, data_dir, _host) =
            native_surface_fixture("native-component-surface");
        let registry = runtime
            .service_registry(&node)
            .expect("normal mounted registry");
        let factory = component_factory().expect("DFS component descriptor");
        let content_root = data_dir.dfs_dir().join("content");
        assert!(content_root.join("inline-bindings/current.sobj").is_file());
        let content = crate::LocalContentStore::open_for_inspection(&content_root);
        assert_eq!(
            content
                .stats()
                .expect("actual active projection")
                .manifest_count,
            0
        );
        drop(content);
        let browser_route = registry
            .route(crate::DAEMON_DFS_BROWSER_STATUS_ROUTE)
            .expect("normal mounted browser route");
        let discovery = browser_route
            .discovery
            .expect("normal component discovery identity");
        assert_eq!(discovery.service_id, crate::SIFS_PATH_ACCESS_CAPABILITY);
        assert_eq!(discovery.interface_id, "sinomo.sifs.path-access");
        assert_eq!(
            discovery.provider_capabilities,
            [crate::SIFS_PATH_ACCESS_CAPABILITY]
        );
        for route in [
            crate::DFS_STATUS_ROUTE,
            crate::DFS_DATA_READ_ROUTE,
            crate::DAEMON_DFS_BROWSER_STATUS_ROUTE,
            crate::DFS_CORE_COMPONENT_ACCESS_ROUTE,
            crate::DFS_MOUNTED_MUTATION_HANDOFF_V1_ROUTE,
            DFS_METRIC_EXPORT_ROUTE,
        ] {
            assert!(registry.owns(route), "missing actual core route {route}");
        }
        for route in [
            crate::DFS_ATTACHMENT_ROUTE,
            crate::DAEMON_DFS_BROWSER_FILE_HISTORY_PIN_ROUTE,
            crate::DAEMON_DFS_BROWSER_FILE_HISTORY_LIST_ROUTE,
            crate::DAEMON_DFS_BROWSER_FILE_HISTORY_READ_ROUTE,
            crate::DAEMON_DFS_BROWSER_FILE_HISTORY_RESTORE_ROUTE,
            crate::DAEMON_DFS_BROWSER_FILE_HISTORY_POLICY_ROUTE,
            crate::DAEMON_DFS_BROWSER_FILE_HISTORY_POLICY_UPDATE_ROUTE,
        ] {
            assert!(
                !registry.owns(route),
                "feature route remains core-owned {route}"
            );
        }
        assert_eq!(factory.descriptor().event_publications.len(), 2);
        assert_eq!(factory.descriptor().event_subscriptions.len(), 2);
        drop(registry);
        runtime
            .stop_node(&node)
            .expect("normal component lifecycle stopped");
        let owner = crate::runtime_host::DfsDataOwnerLock::acquire(
            &crate::runtime_host::DfsRuntimePaths::new(&data_dir),
        )
        .expect("same retained volume OS lease released");
        drop(owner);
        runtime.close().expect("drain shared runtime");
    }

    #[test]
    fn component_factory_does_not_activate_content_without_active_owner_context() {
        use sinomo_module::ComponentFactory as _;
        for host_role in [
            sinomo_module::ComponentHostRole::Main,
            sinomo_module::ComponentHostRole::Embedded,
            sinomo_module::ComponentHostRole::Shadow,
        ] {
            let (_root, data_dir) = temp_data_dir();
            let context = sinomo_module::ComponentContext::new(Arc::clone(&data_dir))
                .with_host_role(host_role);
            let factory = component_factory().unwrap();
            assert!(!data_dir.dfs_dir().join("content/inline-bindings").exists());
            let scope = factory.mount(&context).unwrap();
            assert!(!data_dir.dfs_dir().join("content/inline-bindings").exists());
            drop(scope);
        }
    }

    #[tokio::test]
    async fn metadata_replica_authority_without_host_or_cluster_rejects_stale_local_raft() {
        let (_root, data_dir) = temp_data_dir();
        let cluster_paths = ClusterPaths::new(data_dir.as_ref());
        cluster_paths.ensure().expect("cluster paths");
        let local_node_id =
            NodeId::from_legacy_named("node_dfs_stale_authority_local").expect("local node");
        let retired_primary_node_id =
            NodeId::from_legacy_named("node_dfs_stale_authority_primary").expect("retired primary");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            sinomo_core::node::ClusterId::from_legacy_named("cluster_dfs_stale_authority")
                .expect("cluster"),
            "DFS stale local metadata authority",
            local_node_id.clone(),
            "local",
            "pk-local",
            OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state)
            .expect("write cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
            .expect("write local cluster KV state");

        let file_system_id = sifs_file_system_id();
        let shard_id = DfsMetadataShardId::new("root");
        let stale_shard = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: shard_id.0.clone(),
            primary_node_id: retired_primary_node_id.clone(),
            replica_node_ids: vec![local_node_id.clone()],
            epoch: 7,
            assignment: DfsMetadataShardAssignment::Root,

            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        sinomo_core::cluster::append_cluster_raft_command(
            &cluster_paths,
            &local_node_id,
            sinomo_core::kv::KvCommand::Op(
                sinomo_core::kv::KvOp::put_serde(
                    metadata_shard_key(&file_system_id, &shard_id.0),
                    &stale_shard,
                    None,
                )
                .expect("stale shard operation"),
            ),
        )
        .expect("persist stale local Raft shard");

        let context = authoritative_metadata_replica_shard_context(
            None,
            None,
            data_dir,
            Some(file_system_id),
            shard_id,
            retired_primary_node_id,
            Some(stale_shard.epoch),
            None,
        )
        .await
        .expect("fail-closed authority result");
        assert!(
            context.is_none(),
            "a hostless service must not promote a resident stale Raft assignment into ingress authority"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn metadata_replica_authority_exact_set_linearizes_handoff_mutation() {
        let nodes = crate::test_support::DfsTestRuntime::ordered_node_ids("native-exact-cut", 3);
        let mut runtime =
            crate::test_support::DfsTestRuntime::new("native-exact-cut", nodes[0].clone()).unwrap();
        for node in &nodes[1..] {
            runtime.define_node(node.clone()).unwrap();
        }
        runtime.start_nodes().expect("normal native startup");
        let data_dir = Arc::new(runtime.data_dir(&nodes[1]).clone());
        let host = crate::runtime_host::registered_dfs_runtime_host(&data_dir)
            .unwrap()
            .expect("actual registered host");
        let old = runtime.root_shard().expect("actual initial assignment");
        let cluster = Arc::clone(&host.cluster);
        let component = runtime.component_context(&nodes[1]).unwrap();
        let data_plane = crate::runtime_host::DaemonDfsDataPlaneHost::new_on_runtime(
            crate::runtime_host::DfsRuntimePaths::new(&data_dir),
            component.runtime().unwrap(),
        );
        let old_context = authoritative_metadata_replica_shard_context(
            Some(&data_plane),
            Some(Arc::clone(&cluster)),
            Arc::clone(&data_dir),
            Some(old.file_system_id.clone()),
            DfsMetadataShardId::new(&old.shard_id),
            old.primary_node_id.clone(),
            Some(old.epoch),
            None,
        )
        .await
        .unwrap()
        .expect("actual pre-handoff cut");
        assert_eq!(old_context.shard, old);
        assert!(old_context.pending_membership_handoff.is_none());
        let installed = runtime
            .reassign_metadata_shard(
                &old.file_system_id,
                &old.shard_id,
                nodes[1].clone(),
                vec![nodes[0].clone(), nodes[2].clone()],
            )
            .expect("normal freeze/joint acknowledgement/activation");
        let new_context = authoritative_metadata_replica_shard_context(
            Some(&data_plane),
            Some(cluster),
            data_dir,
            Some(old.file_system_id.clone()),
            DfsMetadataShardId::new(&old.shard_id),
            old.primary_node_id.clone(),
            Some(old.epoch),
            None,
        )
        .await
        .unwrap()
        .expect("actual successor cut");
        assert_eq!(new_context.shard, installed);
        assert_eq!(installed.epoch, old.epoch + 1);
        assert!(new_context.pending_membership_handoff.is_none());
        assert_ne!(new_context.shard, old_context.shard);
        drop(data_plane);
        drop(component);
        drop(host);
        runtime.close().expect("drain shared native owners");
    }

    async fn dispatch_local<B: serde::Serialize>(
        registry: &ServiceRegistry,
        data_dir: Arc<SinomoDataDir>,
        route: &str,
        body: &B,
    ) -> (ServiceDisposition, ObjectRpcResponse) {
        dispatch_with_signer(registry, data_dir, route, body, ServiceSigner::Local)
            .await
            .expect("dispatch")
    }

    async fn dispatch_with_signer<B: serde::Serialize>(
        registry: &ServiceRegistry,
        data_dir: Arc<SinomoDataDir>,
        route: &str,
        body: &B,
        signer: ServiceSigner,
    ) -> Result<(ServiceDisposition, ObjectRpcResponse), ServiceError> {
        let sink = VecSink::default();
        let frames = Arc::clone(&sink.frames);
        let ctx = ServiceContext::new(
            data_dir,
            None,
            signer,
            sinomo_core::network::ObjectRpcIncomingBody::empty(),
            Box::new(sink),
        );
        let request = ObjectRpcRequest::encode_body(route, DFS_RPC_CATEGORY, body)
            .expect("encode")
            .with_traceparent("00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01");
        let disposition = registry.dispatch(ctx, request).await?;
        let outgoing = frames
            .lock()
            .expect("frames")
            .pop()
            .expect("a response frame");
        let mut response = outgoing.response;
        if response.body.is_empty() {
            // Unit-test compatibility boundary: production transports consume
            // the owned value source directly. These service tests materialize
            // it only so their existing typed assertions can use ObjectRpcResponse.
            response.body = outgoing.body.collect_to_vec(usize::MAX).await?;
        }
        Ok((disposition, response))
    }

    async fn dispatch_body_local(
        registry: &ServiceRegistry,
        data_dir: Arc<SinomoDataDir>,
        request: ObjectRpcRequest,
    ) -> sinomo_core::network::ObjectRpcOutgoingResponse {
        let sink = VecSink::default();
        let frames = Arc::clone(&sink.frames);
        let ctx = ServiceContext::new(
            data_dir,
            None,
            ServiceSigner::Local,
            sinomo_core::network::ObjectRpcIncomingBody::empty(),
            Box::new(sink),
        );
        registry
            .dispatch(ctx, request)
            .await
            .expect("body dispatch");
        frames.lock().expect("frames").pop().expect("response")
    }

    #[test]
    fn dfs_service_route_metadata_matches_module_surfaces() {
        let (runtime, _node, _data_dir, host) = native_surface_fixture("native-route-metadata");
        let registry =
            register_services_with_data_plane_host(ServiceRegistry::builder(), host).build();
        let service_routes = registry
            .route_categories()
            .into_iter()
            .map(|route| (route.route.clone(), route))
            .collect::<std::collections::BTreeMap<_, _>>();
        let module_routes = dfs_module_surfaces()
            .object_rpc_route_descriptors()
            .expect("DFS module surfaces")
            .into_iter()
            .map(|descriptor| (descriptor.route.clone(), descriptor))
            .collect::<std::collections::BTreeMap<_, _>>();

        for route in DFS_OBJECT_RPC_ROUTES {
            let service_route = service_routes
                .get(*route)
                .unwrap_or_else(|| panic!("service registry should own DFS route {route}"));
            let module_route = module_routes
                .get(*route)
                .unwrap_or_else(|| panic!("module surfaces should declare DFS route {route}"));
            assert_eq!(service_route.category, module_route.category);
            assert_eq!(
                service_route.auth_scope.as_deref(),
                Some(module_route.auth_scope.as_str())
            );
            assert_eq!(
                service_route.required_capabilities,
                module_route
                    .required_capabilities
                    .iter()
                    .map(|capability| capability.as_str().to_string())
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                service_route.concurrency_limit,
                Some(dfs_service_route_concurrency_limit(route))
            );
            assert_eq!(
                service_route.timeout_ms,
                dfs_service_route_timeout(route)
                    .map(|timeout| timeout.as_millis().min(u128::from(u64::MAX)) as u64)
            );
        }

        let snapshot_install = service_routes
            .get(crate::DFS_DATA_METADATA_SNAPSHOT_INSTALL_ROUTE)
            .expect("metadata snapshot install route");
        assert_eq!(
            snapshot_install.request_body_mode,
            ServiceBodyMode::Optional,
            "compacted metadata checkpoints must use the streamed RPC body"
        );

        let namespace = service_routes
            .get(crate::DFS_DATA_NAMESPACE_ROUTE)
            .expect("namespace route");
        assert_eq!(
            namespace.request_body_mode,
            ServiceBodyMode::Optional,
            "namespace commands use the canonical value path; explicit DFS attachments remain optional"
        );

        let fetch_blob_response = service_routes
            .get(crate::DFS_DATA_FETCH_BLOB_RESPONSE_ROUTE)
            .expect("fetch blob response route");
        assert_eq!(
            fetch_blob_response.request_body_mode,
            ServiceBodyMode::Optional,
            "a fetch response carries the requested blob as a streamed request attachment"
        );
        runtime.close().expect("drain shared native owner");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dfs_direct_read_service_reuses_initialized_content_store() {
        let (runtime, _node, data_dir, host) = native_surface_fixture("native-direct-read-cache");
        data_dir.ensure_root_layout().expect("root layout");
        let object_id = DfsObjectId::generate();
        let version = DfsVersion::generate();
        let bytes = (0..(2 * 1024 * 1024 + 16 * 1024))
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let content =
            LocalContentStore::new(data_dir.dfs_dir().join("content")).expect("content store");
        let manifest = content
            .put_object_version(object_id.as_str(), version.as_str(), &bytes)
            .expect("put object version");
        let direct_read = crate::DfsDirectReadRequest {
            request_id: "service-direct-read-cache".to_string(),
            object_id,
            version,
            content_hash: manifest.content_hash,
            content_size_bytes: bytes.len() as u64,
            range_offset: None,
            range_max_bytes: None,
        };
        let service = DfsDataPlaneService::new(host);
        let cache_probe = service.clone();
        let registry = ServiceRegistry::builder().register(service).build();

        let first_request =
            crate::dfs_direct_read_rpc_request(&direct_read).expect("first request");
        let first = dispatch_body_local(&registry, data_dir.clone(), first_request).await;
        let first_bytes = first
            .body
            .collect_to_vec(bytes.len())
            .await
            .expect("read first body");
        assert_eq!(first_bytes, bytes);
        assert_eq!(cache_probe.cached_content_store_count(), 1);
        let first_cached = cache_probe
            .content_store_for(&data_dir)
            .expect("first cached content store");

        let second_request =
            crate::dfs_direct_read_rpc_request(&direct_read).expect("second request");
        let second = dispatch_body_local(&registry, data_dir.clone(), second_request).await;
        let second_bytes = second
            .body
            .collect_to_vec(bytes.len())
            .await
            .expect("read second body");
        assert_eq!(second_bytes, bytes);
        assert_eq!(cache_probe.cached_content_store_count(), 1);
        let second_cached = cache_probe
            .content_store_for(&data_dir)
            .expect("second cached content store");
        assert!(Arc::ptr_eq(&first_cached, &second_cached));
        runtime.close().expect("drain shared native owner");
    }

    #[test]
    fn hosted_data_plane_caches_the_host_content_store_reference() {
        let (mut runtime, node, data_dir, host) =
            native_surface_fixture("native-host-content-cache");
        let service = DfsDataPlaneService::new(host);
        let selected = service
            .content_store_for(&data_dir)
            .expect("actual hosted content store");
        assert_eq!(service.cached_content_store_count(), 1);
        runtime.stop_node(&node).expect("stop actual mounted host");
        let cached = service
            .content_store_for(&data_dir)
            .expect("cached hosted content store");
        assert!(Arc::ptr_eq(&cached, &selected));
        drop(service);
        runtime.close().expect("drain native owner");
    }

    #[tokio::test]
    async fn dfs_status_serves_in_local_mode_without_a_cluster() {
        let registry = register_services(ServiceRegistry::builder()).build();
        let (disposition, response) = {
            let (_root, data_dir) = temp_data_dir();
            dispatch_local(&registry, data_dir, DFS_STATUS_ROUTE, &()).await
        };
        assert_eq!(disposition, ServiceDisposition::Continue);
        let _status: DfsOperatorStatus = response.decode_body().expect("decode status");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn hosted_dfs_status_reuses_the_process_data_plane_host() {
        let (mut runtime, node, data_dir, host) = native_surface_fixture("native-host-status");
        let registry =
            register_services_with_data_plane_host(ServiceRegistry::builder(), host).build();
        let deadline = tokio::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        let status = loop {
            let (disposition, response) = tokio::time::timeout_at(
                deadline,
                dispatch_local(&registry, data_dir.clone(), DFS_STATUS_ROUTE, &()),
            )
            .await
            .expect("hosted status completes within its original request window");
            assert_eq!(disposition, ServiceDisposition::Continue);
            let status: DfsOperatorStatus =
                response.decode_body().expect("decode actual hosted status");
            let updating = !status.incomplete_reasons.is_empty()
                && status.incomplete_reasons.iter().all(|reason|
                    reason == "network error: DFS runtime network error at DFS message cache root identity: bounded DFS status unavailable while the resident generation cache is being updated"
                    || reason == "network error: bounded DFS status unavailable while the resident generation cache is being updated");
            if updating {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "hosted status remained incomplete: {status:#?}"
                );
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                continue;
            }
            break status;
        };
        assert!(
            status.blocking_incomplete_reasons().is_empty(),
            "actual hosted status={status:#?}"
        );
        assert!(status.file_system_count > 0);
        assert!(status.object_count > 0, "actual hosted status={status:#?}");
        runtime.stop_node(&node).expect("stop actual data owner");
        let unavailable = dispatch_with_signer(
            &registry,
            data_dir,
            DFS_STATUS_ROUTE,
            &(),
            ServiceSigner::Local,
        )
        .await;
        assert!(
            unavailable.is_err(),
            "closed owner cannot supply a synthetic healthy status"
        );
        drop(registry);
        runtime.close().expect("drain native owner");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dfs_status_route_scopes_operational_counts_to_primary_shards() {
        let nodes =
            crate::test_runtime::DfsTestRuntime::ordered_node_ids("native-status-primary-scope", 3);
        let mut runtime = crate::test_runtime::DfsTestRuntime::new(
            "native-status-primary-scope",
            nodes[0].clone(),
        )
        .unwrap();
        for node in &nodes[1..] {
            runtime.define_node(node.clone()).unwrap();
        }
        runtime.start_nodes().unwrap();
        let users = runtime
            .metadata_shards()
            .unwrap()
            .into_iter()
            .find(|config| config.shard_id == "users")
            .unwrap();
        let target = nodes
            .iter()
            .find(|node| **node != users.primary_node_id)
            .unwrap()
            .clone();
        let mut placement =
            crate::DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 2).unwrap();
        placement.required_node_ids = vec![users.primary_node_id.clone(), target.clone()];
        placement.forbidden_node_ids = nodes
            .iter()
            .filter(|node| **node != users.primary_node_id && **node != target)
            .cloned()
            .collect();
        let accepted = runtime
            .apply_placement_policy(
                &users.primary_node_id,
                "/users/status-scope-file",
                crate::DfsPlacementScope::SelfOnly,
                placement,
            )
            .unwrap();
        let control = runtime.shared_control_plane().unwrap();
        let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        loop {
            let job = control
                .with_client(|kv| {
                    crate::DfsControlPlaneStore::new(kv.as_mut())
                        .placement_reconciliation(&accepted.file_system_id, &accepted.job_id)
                })
                .unwrap()
                .unwrap();
            assert_eq!(job.accepted_config, accepted.accepted_config);
            assert!(job.last_error.is_none());
            if job.state == crate::DfsPlacementReconciliationState::Completed {
                break;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(
                !remaining.is_zero(),
                "empty status fixture policy must finish"
            );
            tokio::task::block_in_place(|| {
                runtime.maintenance_tick(&users.primary_node_id, remaining)
            })
            .unwrap()
            .outcome
            .unwrap();
        }
        let status = |runtime: &crate::test_runtime::DfsTestRuntime, target: &NodeId| {
            // Exact diagnostic summaries validate the bounded route's claims
            // without asking that route to scan or hide declared omissions.
            let observer = runtime.client(target).unwrap();
            let backend = observer.metadata_backend().unwrap();
            let inventory = backend.metadata_summary().unwrap();
            let owned = backend.metadata_summary_for_local_primary_shards().unwrap();
            drop(observer);
            // As with the exact metadata inventory above, establish the real
            // durable queue view outside the bounded status route. An idle
            // replica need not have run repair maintenance before this query.
            crate::LocalDfsRepairQueueStore::new(crate::default_dfs_repair_attempt_snapshot_path(
                runtime.data_dir(target),
            ))
            .view()
            .unwrap();
            let request =
                ObjectRpcRequest::encode_body(DFS_STATUS_ROUTE, crate::DFS_RPC_CATEGORY, &())
                    .unwrap();
            let bounded = runtime
                .rpc_request(
                    &users.primary_node_id,
                    target,
                    request,
                    Duration::from_secs(5),
                )
                .unwrap()
                .decode_body::<DfsOperatorStatus>()
                .unwrap();
            let configurations = runtime.metadata_shards().unwrap();
            assert_eq!(bounded.file_system_count, 1);
            assert_eq!(bounded.metadata_shards.len(), configurations.len());
            for config in &configurations {
                let actual = bounded
                    .metadata_shards
                    .iter()
                    .find(|shard| shard.shard_id.0 == config.shard_id)
                    .unwrap();
                assert_eq!(
                    actual.primary_node_id.as_ref(),
                    Some(&config.primary_node_id)
                );
                assert_eq!(actual.epoch, Some(config.epoch));
            }
            if !bounded.incomplete_reasons.is_empty() {
                // Nonblocking status can observe a real cache transition. Its
                // explicit unavailable result is not a zero inventory/work claim.
                assert!(bounded.incomplete_reasons.iter().all(|reason| reason.contains(
                    "bounded DFS status unavailable while the resident generation cache is being updated"
                )), "unexpected unavailable status: {bounded:?}");
                return (inventory, owned);
            }
            if bounded.bounded_omissions.is_empty() {
                assert_eq!(bounded.object_count, inventory.object_count);
                assert_eq!(bounded.pending_repair_count, owned.pending_repair_count);
                assert_eq!(bounded.pending_write_count, owned.pending_write_count);
                assert_eq!(
                    bounded.pending_namespace_job_count,
                    owned.pending_namespace_job_count
                );
                assert_eq!(
                    bounded.failed_namespace_job_count,
                    owned.failed_namespace_job_count
                );
            } else {
                // Retained routing history requires an exact visibility scan.
                // The bounded route must disclose that omission, never present
                // its zero placeholders as a complete operational measurement.
                assert!(bounded.bounded_omissions.iter().all(|omission| matches!(omission,
                    crate::DfsOperatorStatusOmission::HistoricalRoutingTopology { generation_count, .. }
                    if *generation_count > 0)), "unexpected bounded omission: {bounded:?}");
                for metric in [
                    crate::DfsOperatorStatusMetric::ObjectCount,
                    crate::DfsOperatorStatusMetric::PendingRepairCount,
                ] {
                    assert!(
                        bounded
                            .bounded_omissions
                            .iter()
                            .any(|omission| omission.omitted_metrics().contains(&metric))
                    );
                }
            }
            (inventory, owned)
        };
        let (before_inventory, before) = status(&runtime, &target);
        let (_, primary_before) = status(&runtime, &users.primary_node_id);
        for route in [
            crate::DFS_DATA_STAGE_BLOB_ROUTE,
            crate::DFS_DATA_REPAIR_REPLICA_ROUTE,
        ] {
            runtime.install_fault(crate::test_runtime::FaultRule {
                source: None,
                target: Some(target.clone()),
                route: Some(route.into()),
                call_id: None,
                origin: None,
                remaining_hits: 32,
                action: crate::test_runtime::FaultAction::BudgetExhausted,
            });
        }
        let mut writer = runtime.client(&users.primary_node_id).unwrap();
        let (resolved, _, write_policy) = writer
            .managed_replication_policy_for_path(
                "/users/status-scope-file",
                DfsObjectPolicy::ReplicatedEventual,
                1,
            )
            .unwrap();
        assert_eq!(
            resolved
                .policy
                .required_node_ids
                .iter()
                .cloned()
                .collect::<std::collections::BTreeSet<_>>(),
            [users.primary_node_id.clone(), target.clone()]
                .into_iter()
                .collect()
        );
        assert_eq!(
            write_policy.required_node_ids,
            resolved.policy.required_node_ids
        );
        assert_eq!(
            write_policy.forbidden_node_ids,
            resolved.policy.forbidden_node_ids
        );
        let object = writer
            .posix_write_file(
                "/users/status-scope-file",
                b"native foreign-authority data",
                crate::DfsWriteOptions {
                    policy: write_policy,
                    replica_candidates: vec![users.primary_node_id.clone(), target.clone()],
                    wait_for_commit: true,
                    foreground_replica_repair: false,
                },
            )
            .unwrap();
        // The real write commits before its asynchronous metadata publisher
        // necessarily reaches every member. Observe the physical inventory
        // only after a genuine metadata barrier; content repairs stay faulted.
        let mut author = runtime
            .metadata_primary_service("/users/status-scope-file")
            .unwrap();
        let sync_object = author
            .metadata_backend()
            .unwrap()
            .get_object(&object.object_id)
            .unwrap()
            .unwrap();
        let sync_target = author
            .capture_metadata_quorum_sync_target(&sync_object)
            .unwrap();
        let certificate = author
            .sync_metadata_shard_to_quorum(
                &sync_target,
                sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            )
            .unwrap();
        assert_eq!(
            certificate
                .acknowledged_node_ids
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>(),
            std::iter::once(users.primary_node_id.clone())
                .chain(users.replica_node_ids.iter().cloned())
                .collect(),
            "the complete actual metadata membership must contain this committed inventory cut"
        );
        drop(author);
        let mut receiver = runtime.client(&target).unwrap();
        assert_eq!(
            receiver
                .posix_stat("/users/status-scope-file")
                .unwrap()
                .object_id,
            object.object_id
        );
        let (after_inventory, after) = status(&runtime, &target);
        let (_, primary_after) = status(&runtime, &users.primary_node_id);
        assert_eq!(
            after_inventory.object_count,
            before_inventory.object_count + 1,
            "inventory includes the actual replicated file"
        );
        let actual_owner = runtime.metadata_primary_service("/users").unwrap();
        let intents = actual_owner
            .metadata_backend()
            .unwrap()
            .intents_for_object(&object.object_id)
            .unwrap();
        let receipts = actual_owner
            .metadata_backend()
            .unwrap()
            .receipts_for_version(&object.object_id, &object.version)
            .unwrap();
        let queue =
            crate::LocalDfsRepairQueueStore::new(crate::default_dfs_repair_attempt_snapshot_path(
                runtime.data_dir(&users.primary_node_id),
            ))
            .view()
            .unwrap();
        assert_eq!(
            primary_after.pending_repair_count,
            primary_before.pending_repair_count + 1,
            "the real unfinished repair belongs to the foreign primary; intents={intents:?}; receipts={receipts:?}; queue={:?}",
            queue.records
        );
        drop(actual_owner);
        assert_eq!(after.pending_repair_count, before.pending_repair_count);
        assert_eq!(after.pending_write_count, before.pending_write_count);
        assert_eq!(
            after.pending_namespace_job_count,
            before.pending_namespace_job_count
        );
        assert_eq!(
            after.failed_namespace_job_count,
            before.failed_namespace_job_count
        );
        drop((receiver, writer));
        runtime.close().unwrap();
    }

    #[tokio::test]
    async fn dfs_metadata_journal_batch_requires_authenticated_cluster_signer() {
        let registry = register_services(ServiceRegistry::builder()).build();
        let command = DfsMetadataJournalBatchCommand {
            request_id: "req-journal-1".to_string(),
            source_node_id: None,
            file_system_id: None,
            authority_epoch: None,
            verify_prefix_only: false,
            prefix_probe: None,
            authority_probe: None,
            durable_replication: None,
            batch: DfsMetadataJournalFrameBatch {
                shard_id: DfsMetadataShardId::new("shard-local"),
                journal_frames: Vec::new(),
                next_after_sequence: 0,
            },
        };
        let (_test_root, test_data_dir) = temp_data_dir();
        let error = dispatch_with_signer(
            &registry,
            test_data_dir.clone(),
            DFS_METADATA_JOURNAL_BATCH_ROUTE,
            &command,
            ServiceSigner::Local,
        )
        .await
        .expect_err("bare local signer must not enter a replica ingest route");
        assert!(
            error
                .to_string()
                .contains("requires an authenticated cluster signer identity")
        );
    }

    #[tokio::test]
    async fn dfs_metadata_snapshot_install_requires_authenticated_cluster_signer() {
        let registry = register_services(ServiceRegistry::builder()).build();
        let (_root, data_dir) = temp_data_dir();
        let snapshot = LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new("snapshot-source"),
            data_dir.root().join("snapshot-source"),
        )
        .expect("source metadata")
        .export_snapshot_image()
        .expect("native snapshot image");
        let command = DfsMetadataSnapshotInstallCommand {
            request_id: "req-snapshot-1".to_string(),
            source_node_id: sinomo_core::node::NodeId::from_legacy_named("node_snapshot_source")
                .expect("source node"),
            file_system_id: None,
            authority_epoch: None,
            primary_handoff_id: None,
            shard_id: DfsMetadataShardId::new("shard-snapshot-local"),
            next_after_sequence: 7,
            snapshot: snapshot.descriptor().clone(),
            image: Some(snapshot),
            reason: "service test".to_string(),
        };

        let error = dispatch_with_signer(
            &registry,
            data_dir,
            DFS_METADATA_SNAPSHOT_INSTALL_ROUTE,
            &command,
            ServiceSigner::Local,
        )
        .await
        .expect_err("bare local signer must not enter a replica ingest route");
        assert!(
            error
                .to_string()
                .contains("requires an authenticated cluster signer identity")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn native_replica_routes_ingest_exact_frames_and_reject_changed_source_and_epoch() {
        let (runtime, mut source, config) = crate::test_support::current_native_metadata_service(
            "native-host-replica-routes",
            2,
            "/",
        )
        .unwrap();
        let primary = config.primary_node_id.clone();
        let receiver = config.replica_node_ids[0].clone();
        source
            .posix_write_file(
                "/native-host-replica-data",
                b"actual native durable content",
                crate::DfsWriteOptions::replicated_eventual(vec![primary.clone()]),
            )
            .unwrap();
        // Journal bytes and the expected snapshot must come from the same
        // current owning Core, not the requester's older browser image.
        drop(source);
        let source = runtime.metadata_primary_service("/").unwrap();
        let shard = DfsMetadataShardId::new(config.shard_id.clone());
        let backend = source.metadata_backend().unwrap();
        let selected = backend
            .journal_batch_for_shard_since_limited(&shard, 0, 128, 16 * 1024 * 1024)
            .unwrap()
            .unwrap();
        let frames = backend.journal_frames_for_batch(&selected).unwrap();
        assert!(!frames.is_empty());
        let snapshot = backend
            .export_shard_table_snapshot(&shard)
            .unwrap()
            .unwrap();
        let command = DfsMetadataJournalBatchCommand {
            request_id: "native-exact-host-journal".into(),
            source_node_id: Some(primary.clone()),
            file_system_id: Some(config.file_system_id.clone()),
            authority_epoch: Some(config.epoch),
            verify_prefix_only: false,
            prefix_probe: None,
            authority_probe: None,
            durable_replication: Some(crate::DfsMetadataDurableReplication {
                confirmation_id: "native-exact-host-journal".into(),
                background_publication: false,
            }),
            batch: DfsMetadataJournalFrameBatch {
                shard_id: shard.clone(),
                next_after_sequence: selected.next_after_sequence,
                journal_frames: frames,
            },
        };
        let request = ObjectRpcRequest::encode_body(
            DFS_METADATA_JOURNAL_BATCH_ROUTE,
            DFS_RPC_CATEGORY,
            &command,
        )
        .unwrap();
        let response: DfsMetadataJournalBatchResponse = runtime
            .rpc_request(&primary, &receiver, request, Duration::from_secs(5))
            .unwrap()
            .decode_body()
            .unwrap();
        assert!(matches!(
            response.result,
            crate::DfsMetadataJournalBatchResult::Applied
        ));
        assert_eq!(response.next_after_sequence, selected.next_after_sequence);
        let mut changed_source = command.clone();
        changed_source.request_id = "native-changed-source".into();
        changed_source.source_node_id = Some(NodeId::generate());
        let request = ObjectRpcRequest::encode_body(
            DFS_METADATA_JOURNAL_BATCH_ROUTE,
            DFS_RPC_CATEGORY,
            &changed_source,
        )
        .unwrap();
        let response: DfsMetadataJournalBatchResponse = runtime
            .rpc_request(&primary, &receiver, request, Duration::from_secs(5))
            .unwrap()
            .decode_body()
            .unwrap();
        assert!(matches!(
            response.result,
            crate::DfsMetadataJournalBatchResult::Rejected { .. }
        ));
        let mut changed_epoch = command;
        changed_epoch.request_id = "native-changed-epoch".into();
        changed_epoch.authority_epoch = Some(config.epoch + 1);
        let request = ObjectRpcRequest::encode_body(
            DFS_METADATA_JOURNAL_BATCH_ROUTE,
            DFS_RPC_CATEGORY,
            &changed_epoch,
        )
        .unwrap();
        let response: DfsMetadataJournalBatchResponse = runtime
            .rpc_request(&primary, &receiver, request, Duration::from_secs(5))
            .unwrap()
            .decode_body()
            .unwrap();
        assert!(matches!(
            response.result,
            crate::DfsMetadataJournalBatchResult::Rejected { .. }
        ));
        let receiver_view = runtime.client(&receiver).unwrap();
        assert_eq!(
            receiver_view
                .metadata_backend()
                .unwrap()
                .metadata_prefix_proof(&shard)
                .unwrap(),
            Some(snapshot.prefix_proof)
        );
        drop((receiver_view, source));
        runtime
            .close()
            .expect("all native receivers and owners stopped");
    }

    #[tokio::test]
    async fn dfs_replica_routes_bind_authenticated_caller_to_claimed_source() {
        let registry = register_services(ServiceRegistry::builder()).build();
        let (_root, data_dir) = temp_data_dir();
        let claimed = NodeId::from_legacy_named("node_dfs_replica_claimed").expect("claimed");
        let authenticated =
            NodeId::from_legacy_named("node_dfs_replica_authenticated").expect("authenticated");
        let signer = ServiceSigner::Remote {
            node_id: authenticated.to_string(),
            public_key: "pk-dfs-replica-authenticated".to_string(),
        };
        let journal = DfsMetadataJournalBatchCommand {
            request_id: "req-journal-caller-mismatch".to_string(),
            source_node_id: Some(claimed.clone()),
            file_system_id: None,
            authority_epoch: None,
            verify_prefix_only: false,
            prefix_probe: None,
            authority_probe: None,
            durable_replication: None,
            batch: DfsMetadataJournalFrameBatch {
                shard_id: DfsMetadataShardId::new("root"),
                journal_frames: Vec::new(),
                next_after_sequence: 0,
            },
        };
        let (disposition, response) = dispatch_with_signer(
            &registry,
            Arc::clone(&data_dir),
            DFS_METADATA_JOURNAL_BATCH_ROUTE,
            &journal,
            signer.clone(),
        )
        .await
        .expect("journal mismatch response");
        assert_eq!(disposition, ServiceDisposition::Continue);
        let response: DfsMetadataJournalBatchResponse = response.decode_body().expect("decode");
        assert!(matches!(
            response.result,
            crate::DfsMetadataJournalBatchResult::Rejected { ref reason , ..}
                if reason.contains("authenticated metadata journal caller")
                    && reason.contains(claimed.as_str())
        ));

        let snapshot_backend = LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new("snapshot-source"),
            data_dir.root().join("snapshot-source-mismatch"),
        )
        .expect("source metadata");
        let snapshot = DfsMetadataSnapshotInstallCommand {
            request_id: "req-snapshot-caller-mismatch".to_string(),
            source_node_id: claimed.clone(),
            file_system_id: None,
            authority_epoch: None,
            primary_handoff_id: None,
            shard_id: DfsMetadataShardId::new("root"),
            next_after_sequence: 0,
            snapshot: snapshot_backend
                .export_snapshot_image()
                .unwrap()
                .descriptor()
                .clone(),
            image: Some(snapshot_backend.export_snapshot_image().unwrap()),
            reason: "service caller identity test".to_string(),
        };
        let (disposition, response) = dispatch_with_signer(
            &registry,
            data_dir,
            DFS_METADATA_SNAPSHOT_INSTALL_ROUTE,
            &snapshot,
            signer,
        )
        .await
        .expect("snapshot mismatch response");
        assert_eq!(disposition, ServiceDisposition::Continue);
        let response: DfsMetadataSnapshotInstallResponse =
            response.decode_body().expect("decode snapshot response");
        assert!(matches!(
            response.result,
            DfsMetadataSnapshotInstallResult::Rejected { ref reason , ..}
                if reason.contains("authenticated metadata snapshot caller")
                    && reason.contains(claimed.as_str())
        ));
    }

    #[tokio::test]
    async fn repair_retry_requires_admin_metadata_and_only_schedules_owner_work() {
        let (_root, data_dir) = temp_data_dir();
        let paths = crate::runtime_host::DfsRuntimePaths::new(data_dir.as_ref());
        let service = DfsRepairRetryService {
            paths: paths.clone(),
        };
        let route = service.route_categories().remove(0);
        assert_eq!(route.auth_scope.as_deref(), Some("admin"));
        assert_eq!(route.required_capabilities, vec!["dfs.repair.retry"]);
        let declared = dfs_module_surfaces()
            .object_rpc_route_descriptors()
            .unwrap();
        let declared = declared
            .iter()
            .find(|route| route.route == sinomo_api::dfs::DFS_REPAIR_RETRY_ROUTE)
            .unwrap();
        assert_eq!(declared.auth_scope, "admin");
        // A host without a resident DFS owner, including shadow, cannot expose
        // this operation through the ordinary inspection service.
        assert!(
            !register_services(ServiceRegistry::builder())
                .build()
                .route_categories()
                .iter()
                .any(|route| route.route == sinomo_api::dfs::DFS_REPAIR_RETRY_ROUTE)
        );
        assert!(
            crate::runtime_host::daemon_dfs_maintenance_reconciliation_generation(&paths).is_none()
        );
        let registry = ServiceRegistry::builder().register(service).build();
        let request = sinomo_api::dfs::DfsRepairRetryRequest {
            request_id: sinomo_core::Id::generate(),
            compact_namespace_jobs: Vec::new(),
        };
        let (_, response) = dispatch_local(
            &registry,
            data_dir,
            sinomo_api::dfs::DFS_REPAIR_RETRY_ROUTE,
            &request,
        )
        .await;
        let response: sinomo_api::dfs::DfsRepairRetryResponse = response.decode_body().unwrap();
        assert_eq!(response.request_id, request.request_id);
        assert_eq!(
            crate::runtime_host::daemon_dfs_maintenance_reconciliation_generation(&paths),
            Some(response.requested_generation)
        );
    }

    #[tokio::test]
    async fn dfs_inspection_status_serves_in_local_mode() {
        let registry = register_services(ServiceRegistry::builder()).build();
        let (_test_root, test_data_dir) = temp_data_dir();
        let (disposition, response) = dispatch_local(
            &registry,
            test_data_dir.clone(),
            DFS_INSPECTION_ROUTE,
            &DaemonDfsInspectionRequest::Status,
        )
        .await;
        assert_eq!(disposition, ServiceDisposition::Continue);
        let result: DaemonDfsInspectionResponse = response.decode_body().expect("decode");
        assert!(matches!(result, DaemonDfsInspectionResponse::Status { .. }));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn hosted_dfs_inspection_reuses_the_process_host() {
        let (mut runtime, node, data_dir, host) = native_surface_fixture("native-host-inspection");
        let registry =
            register_services_with_data_plane_host(ServiceRegistry::builder(), host).build();
        let request = DaemonDfsInspectionRequest::Status;
        let (disposition, response) =
            dispatch_local(&registry, data_dir.clone(), DFS_INSPECTION_ROUTE, &request).await;
        assert_eq!(disposition, ServiceDisposition::Continue);
        let result: DaemonDfsInspectionResponse =
            response.decode_body().expect("decode actual inspection");
        let DaemonDfsInspectionResponse::Status { status } = result else {
            panic!("expected hosted status");
        };
        assert!(status.file_system_count > 0);
        assert!(status.object_count > 0);
        runtime.stop_node(&node).expect("stop actual owner");
        let unavailable = dispatch_with_signer(
            &registry,
            data_dir,
            DFS_INSPECTION_ROUTE,
            &request,
            ServiceSigner::Local,
        )
        .await;
        assert!(
            unavailable.is_err(),
            "inspection cannot reopen a closed process owner"
        );
        drop(registry);
        runtime.close().expect("drain native owner");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dfs_data_plane_routes_delegate_to_host() {
        let (runtime, local, data_dir, host) = native_surface_fixture("native-data-plane-dispatch");
        let registry = register_data_plane_services(ServiceRegistry::builder(), host).build();
        assert!(registry.owns(DFS_SMOKE_ROUTE));
        assert!(registry.owns(crate::DFS_DATA_READ_ROUTE));
        assert!(registry.owns(DFS_DIRECT_READ_ROUTE));
        assert!(!registry.owns("/dfs/message"));
        assert!(!registry.owns("/dfs/protocol/frame"));
        let (disposition, smoke_response) =
            dispatch_local(&registry, data_dir.clone(), DFS_SMOKE_ROUTE, &()).await;
        assert_eq!(disposition, ServiceDisposition::Continue);
        let smoke: DfsSmokeReport = smoke_response.decode_body().expect("decode native smoke");
        assert!(
            smoke
                .attempts
                .iter()
                .any(|attempt| attempt.committed && attempt.path == smoke.path)
        );
        assert!(smoke.committed_replica_node_ids.contains(&local));
        let mut client = runtime.client(&local).expect("actual native smoke reader");
        let actual = client
            .posix_stat(&smoke.path)
            .expect("actual committed smoke path");
        assert_eq!(actual.object_id, smoke.object_id);
        assert_eq!(actual.version, smoke.version);
        let bytes = client
            .posix_read_file(&smoke.path)
            .expect("actual committed smoke bytes");
        assert!(!bytes.is_empty());
        assert_eq!(Some(bytes.len() as u64), smoke.size_bytes);
        assert_eq!(Some(crate::content_hash(&bytes)), smoke.content_hash);
        drop(client);
        let object_id = DfsObjectId::generate();
        let command = DfsMessageCommand {
            request_id: "message-route-1".into(),
            routed_message: crate::DfsRoutedMessage {
                source_node_id: local.clone(),
                target_node_id: local.clone(),
                frame_id: 2,
                message_id: 2,
                message: crate::DfsNetworkMessage::ReadRequest(crate::DfsReadRequest {
                    request_id: "message-read-1".into(),
                    object_id: object_id.clone(),
                    version: None,
                    content_hash: None,
                    content_size_bytes: None,
                    range_offset: None,
                    range_max_bytes: None,
                }),
            },
        };
        let (disposition, response) = dispatch_with_signer(
            &registry,
            data_dir,
            crate::DFS_DATA_READ_ROUTE,
            &command,
            ServiceSigner::LocalNode {
                node_id: local.to_string(),
                public_key: format!("pk-{local}"),
            },
        )
        .await
        .expect("actual native read dispatch");
        assert_eq!(disposition, ServiceDisposition::Continue);
        let reply: crate::DfsMessageResponse = response.decode_body().expect("decode native reply");
        assert_eq!(reply.request_id, command.request_id);
        assert_eq!(reply.outbound_messages.len(), 1);
        let frame = &reply.outbound_messages[0];
        assert_eq!(frame.source_node_id, local);
        assert_eq!(frame.target_node_id, local);
        let crate::DfsNetworkMessage::ReadResponse(read) = &frame.message else {
            panic!("expected real read response");
        };
        assert_eq!(read.object_id, object_id);
        assert_eq!(read.request_id, "message-read-1");
        assert_eq!(read.result, crate::DfsReadResult::NotFound);
        drop(registry);
        runtime.close().expect("drain native owner");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dfs_data_plane_route_binds_authenticated_caller_to_routed_source() {
        let (runtime, authenticated, data_dir, host) =
            native_surface_fixture("native-data-plane-source-binding");
        let mut bootstrap = runtime.client(&authenticated).unwrap();
        bootstrap.posix_sync_directory("/").unwrap();
        drop(bootstrap);
        let snapshot = || {
            let observer = runtime.client(&authenticated).unwrap();
            let backend = observer.metadata_backend().unwrap();
            let summary = backend.metadata_summary().unwrap();
            let prefixes = backend
                .metadata_shard_statuses()
                .unwrap()
                .into_iter()
                .map(|shard| (shard.shard_id, shard.journal_last_sequence))
                .collect::<Vec<_>>();
            (summary, prefixes)
        };
        let (before, before_prefixes) = snapshot();
        let registry =
            register_data_plane_services(ServiceRegistry::builder(), host.clone()).build();
        let command = DfsMessageCommand {
            request_id: "message-route-source-mismatch".into(),
            routed_message: crate::DfsRoutedMessage {
                source_node_id: NodeId::generate(),
                target_node_id: authenticated.clone(),
                frame_id: 3,
                message_id: 3,
                message: crate::DfsNetworkMessage::ReadRequest(crate::DfsReadRequest {
                    request_id: "message-read-source-mismatch".into(),
                    object_id: DfsObjectId::generate(),
                    version: None,
                    content_hash: None,
                    content_size_bytes: None,
                    range_offset: None,
                    range_max_bytes: None,
                }),
            },
        };
        let error = dispatch_with_signer(
            &registry,
            Arc::clone(&data_dir),
            crate::DFS_DATA_READ_ROUTE,
            &command,
            ServiceSigner::LocalNode {
                node_id: authenticated.to_string(),
                public_key: format!("pk-{authenticated}"),
            },
        )
        .await
        .expect_err("mismatched routed source must be rejected before real host dispatch");
        assert!(error.to_string().contains("does not match claimed source"));
        let (after, after_prefixes) = snapshot();
        assert_eq!(
            after_prefixes, before_prefixes,
            "rejected identity cannot append on any shard"
        );
        assert_eq!(before.object_count, after.object_count);
        assert_eq!(before.version_count, after.version_count);
        assert_eq!(before.pending_write_count, after.pending_write_count);
        drop(registry);
        drop(host);
        runtime.close().expect("drain native owner");
    }
}
