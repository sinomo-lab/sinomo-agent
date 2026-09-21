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
                ctx.decode_request_body_bounded(&request, 1024).await?;
            let generation =
                crate::runtime_host::request_daemon_dfs_maintenance_reconciliation(&self.paths)
                    .map_err(|error| ServiceError::Service(error.to_string()))?;
            ctx.respond_value(
                sinomo_api::dfs::DFS_REPAIR_RETRY_ROUTE,
                sinomo_api::dfs::DfsRepairRetryResponse {
                    request_id: request.request_id,
                    requested_generation: generation,
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
            let metrics = crate::runtime_host::resident_dfs_observe_metrics(ctx.data_dir.as_ref())
                .or_else(|_| {
                    let cluster_paths = ClusterPaths::new(ctx.data_dir.as_ref());
                    crate::inspect_cluster_aware_dfs_status(ctx.data_dir.as_ref(), &cluster_paths)
                        .map(crate::dfs_status_observe_metrics)
                        .map_err(|error| error.to_string())
                })
                .map_err(ServiceError::Service)?;
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
    let descriptor =
        sinomo_module::component_descriptor_from_registration(&crate::dfs_module_registration())?
            .with_offer(sifs_path_access_service_offer())
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
                let data_owner = Arc::new(
                    crate::runtime_host::DfsDataOwnerLock::acquire(&paths).map_err(|error| {
                        sinomo_module::ComponentError::Component {
                            component_id: caller.component_id.clone(),
                            detail: error.to_string(),
                        }
                    })?,
                );
                crate::runtime_host::register_daemon_dfs_data_owner_lock(&paths, &data_owner)
                    .map_err(|error| sinomo_module::ComponentError::Component {
                        component_id: caller.component_id.clone(),
                        detail: error.to_string(),
                    })?;
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
                ));
                crate::runtime_host::register_dfs_runtime_host(&data_dir, &host).map_err(
                    |error| sinomo_module::ComponentError::Component {
                        component_id: caller.component_id.clone(),
                        detail: error.to_string(),
                    },
                )?;
                crate::runtime_host::replay_daemon_dfs_mounted_mutation_committed_work_on_runtime(
                    &paths, &runtime,
                )
                .map_err(|error| sinomo_module::ComponentError::Component {
                    component_id: caller.component_id.clone(),
                    detail: error.to_string(),
                })?;
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
                let browser: Arc<dyn crate::DfsBrowserServiceHost> =
                    Arc::new(crate::runtime_host::RuntimeDfsBrowserServiceHost::new(
                        paths,
                        runtime,
                        events,
                        Arc::clone(&host),
                    ));
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
                let data_dir_for_drop = data_dir.clone();
                scope.defer(move || {
                    host.shutdown();
                    crate::runtime_host::unregister_dfs_runtime_host(&data_dir_for_drop);
                    drop(host);
                    drop(data_owner);
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
    for route in crate::DAEMON_DFS_BROWSER_ROUTES
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
        crate::DFS_DATA_WRITE_ROUTE
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
    ) -> DfsDataPlaneHostFuture<'a, DfsMetadataSnapshotInstallResponse>;

    /// Process one authenticated routed DFS message through the host's
    /// canonical authority and resident-backend path. Implementations must
    /// fail closed on stale topology, shard epoch, handoff, freeze, and inner
    /// source claims; the service adapter validates the outer signer and
    /// response envelope but cannot validate host-owned authority state.
    fn handle_message<'a>(
        &'a self,
        command: DfsMessageCommand,
        attachment: Option<DfsInboundAttachment>,
        traceparent: Option<String>,
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
                                code: None,
                            },
                        };
                        ctx.respond_value(DFS_METADATA_JOURNAL_BATCH_ROUTE, response)?;
                        operation_metrics.succeed();
                        return Ok(ServiceDisposition::Continue);
                    }
                    let may_have_deferred_materialization = command.durable_replication.is_some()
                        || command
                            .authority_probe
                            .as_ref()
                            .is_some_and(|probe| probe.durable_journal_ack);
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
                    let command: DfsMetadataSnapshotInstallCommand =
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
                                code: None,
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
                    if let Some(host) = host {
                        let response = host
                            .install_metadata_snapshot(
                                Arc::clone(&ctx.data_dir),
                                authenticated_source_node_id,
                                command,
                                context,
                            )
                            .await
                            .map_err(service_error)?;
                        ctx.respond_value(DFS_METADATA_SNAPSHOT_INSTALL_ROUTE, response)?;
                        operation_metrics.succeed();
                        return Ok(ServiceDisposition::Continue);
                    }
                    let data_dir = Arc::clone(&ctx.data_dir);
                    let response = tokio::task::spawn_blocking(move || {
                        crate::metadata_replication::process_cluster_dfs_metadata_snapshot_install_with_context(
                            &data_dir,
                            &authenticated_source_node_id,
                            command,
                            context,
                        )
                    })
                    .await
                    .map_err(|source| {
                        ServiceError::Service(format!(
                            "DFS metadata snapshot task failed: {source}"
                        ))
                    })?
                    .map_err(service_error)?;
                    ctx.respond_value(DFS_METADATA_SNAPSHOT_INSTALL_ROUTE, response)?;
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
                    let message_response = service
                        .host
                        .handle_message(command, attachment, traceparent)
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
    cluster: Option<Arc<dyn ClusterAccess>>,
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

    let Some(cluster) = cluster else {
        // A resident local Raft projection can be internally coherent yet
        // belong to a retired primary generation. Without a retained host
        // proof or current-term ClusterAccess proof there is no authority to
        // admit metadata replica writes, so fail closed immediately.
        dfs_service_perf_trace(format_args!(
            "metadata-replica-authority-context shard={} mode=authority-unavailable handoff={} total_ms={}",
            shard_id.0,
            primary_handoff_id.is_some(),
            started_at.elapsed().as_millis(),
        ));
        return Ok(None);
    };
    let shard_key = metadata_shard_key(&file_system_id, &shard_id.0);
    let handoff_key = metadata_shard_membership_handoff_key(&file_system_id, &shard_id.0);
    let authority_keys = vec![shard_key, handoff_key];
    let authority_read_started_at = Instant::now();
    let authoritative = cluster
        .kv_get_authoritative_exact_set_async(authority_keys.clone())
        .await?;
    let authority_read_ms = authority_read_started_at.elapsed().as_millis();
    let authority_term = authoritative.authority.term;
    let authority_revision = authoritative.authority.revision;
    // Besides proving one current-term authority point for both records, this
    // rejects wrong-order values and entries whose revisions exceed that proof.
    sinomo_core::kv::validate_cluster_kv_authoritative_entries_read(
        &authority_keys,
        &authoritative,
    )?;
    let mut values = authoritative.value.into_iter();
    let shard_entry = values.next().expect("validated exact-key-set length");
    let handoff_entry = values.next().expect("validated exact-key-set length");
    let shard = shard_entry
        .map(|entry| entry.decode::<DfsMetadataShardConfig>())
        .transpose()?;
    let Some(shard) = shard else {
        dfs_service_perf_trace(format_args!(
            "metadata-replica-authority-context shard={} mode=authoritative-missing local_read_ms={} authority_read_ms={} authority_term={} authority_revision={} total_ms={}",
            shard_id.0,
            0,
            authority_read_ms,
            authority_term,
            authority_revision.0,
            started_at.elapsed().as_millis(),
        ));
        return Ok(None);
    };
    let pending_membership_handoff = handoff_entry
        .map(|entry| entry.decode::<DfsMetadataShardMembershipHandoff>())
        .transpose()?
        .filter(|handoff| {
            handoff.retiring_shard == shard
                && handoff.desired_shard.file_system_id == shard.file_system_id
                && handoff.desired_shard.shard_id == shard.shard_id
                && handoff.desired_shard.epoch == shard.epoch.saturating_add(1)
        });
    let state =
        sinomo_core::cluster::read_cluster_state_without_kv(&ClusterPaths::new(data_dir.as_ref()))?;
    let context = DfsMetadataReplicaShardContext {
        local_node_id: state.local_node_id,
        shard,
        pending_membership_handoff,
    };
    dfs_service_perf_trace(format_args!(
        "metadata-replica-authority-context shard={} mode=authoritative local_read_ms={} authority_read_ms={} authority_term={} authority_revision={} handoff={} total_ms={}",
        shard_id.0,
        0,
        authority_read_ms,
        authority_term,
        authority_revision.0,
        primary_handoff_id.is_some(),
        started_at.elapsed().as_millis(),
    ));
    Ok(Some(context))
}

#[cfg(test)]
fn metadata_replica_context_can_skip_authoritative_read(
    context: Option<&DfsMetadataReplicaShardContext>,
    file_system_id: &DfsFileSystemId,
    shard_id: &DfsMetadataShardId,
    source_node_id: &NodeId,
    authority_epoch: Option<u64>,
    primary_handoff_id: Option<&str>,
) -> bool {
    primary_handoff_id.is_none()
        && context.is_some_and(|context| {
            metadata_replica_context_matches_authority(
                context,
                file_system_id,
                shard_id,
                source_node_id,
                authority_epoch,
                None,
            ) && metadata_replica_context_has_local_ingest_membership(context)
        })
}

#[cfg(test)]
fn metadata_replica_context_has_local_ingest_membership(
    context: &DfsMetadataReplicaShardContext,
) -> bool {
    context
        .shard
        .replica_node_ids
        .contains(&context.local_node_id)
        || context
            .pending_membership_handoff
            .as_ref()
            .is_some_and(|handoff| {
                handoff.retiring_shard == context.shard
                    && handoff.desired_shard.file_system_id == context.shard.file_system_id
                    && handoff.desired_shard.shard_id == context.shard.shard_id
                    && handoff.desired_shard.epoch == context.shard.epoch.saturating_add(1)
                    && std::iter::once(&handoff.desired_shard.primary_node_id)
                        .chain(handoff.desired_shard.replica_node_ids.iter())
                        .any(|node_id| node_id == &context.local_node_id)
            })
}

#[cfg(test)]
fn metadata_replica_context_matches_authority(
    context: &DfsMetadataReplicaShardContext,
    file_system_id: &DfsFileSystemId,
    shard_id: &DfsMetadataShardId,
    source_node_id: &NodeId,
    authority_epoch: Option<u64>,
    primary_handoff_id: Option<&str>,
) -> bool {
    context.shard.file_system_id == *file_system_id
        && context.shard.shard_id == shard_id.0
        && context.shard.primary_node_id == *source_node_id
        && authority_epoch == Some(context.shard.epoch)
        && primary_handoff_id.is_none_or(|handoff_id| {
            context
                .pending_membership_handoff
                .as_ref()
                .is_some_and(|handoff| handoff.handoff_id == handoff_id)
        })
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
    use crate::{DFS_OBJECT_RPC_ROUTES, DFS_RPC_CATEGORY};
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

    #[derive(Default)]
    struct StubDfsDataPlaneHost {
        status_calls: Mutex<usize>,
        smoke_calls: Mutex<usize>,
        inspection_calls: Mutex<usize>,
        journal_request_ids: Mutex<Vec<String>>,
        journal_contexts: Mutex<Vec<DfsMetadataReplicaShardContext>>,
        snapshot_request_ids: Mutex<Vec<String>>,
        snapshot_contexts: Mutex<Vec<DfsMetadataReplicaShardContext>>,
        message_request_ids: Mutex<Vec<String>>,
        message_traceparents: Mutex<Vec<Option<String>>>,
        shared_content_store: Mutex<Option<Arc<LocalContentStore>>>,
        retained_metadata_replica_context: Mutex<Option<DfsMetadataReplicaShardContext>>,
    }

    impl DfsDataPlaneHost for StubDfsDataPlaneHost {
        fn status<'a>(&'a self) -> DfsDataPlaneHostFuture<'a, DfsOperatorStatus> {
            Box::pin(async move {
                *self.status_calls.lock().expect("status calls") += 1;
                Ok(DfsOperatorStatus {
                    storage_migrations: Vec::new(),
                    incomplete_reasons: Vec::new(),
                    bounded_omissions: Vec::new(),
                    file_system_count: 0,
                    object_count: 0,
                    version_count: 0,
                    receipt_count: 0,
                    intent_count: 0,
                    pending_write_count: 0,
                    open_conflict_count: 0,
                    pending_repair_count: 0,
                    repair_attempt_count: 0,
                    pending_namespace_job_count: 0,
                    failed_namespace_job_count: 0,
                    cross_shard_namespace_lock_count: Some(0),
                    metadata_shards: Vec::new(),
                    repair_attempts: Vec::new(),
                })
            })
        }

        fn shared_content_store(
            &self,
            _data_dir: &SinomoDataDir,
        ) -> Result<Option<Arc<LocalContentStore>>, DfsDataPlaneHostError> {
            Ok(self
                .shared_content_store
                .lock()
                .expect("shared content store")
                .clone())
        }

        fn retained_metadata_replica_shard_context<'a>(
            &'a self,
            _data_dir: Arc<SinomoDataDir>,
            _file_system_id: DfsFileSystemId,
            _shard_id: DfsMetadataShardId,
        ) -> Option<DfsDataPlaneHostFuture<'a, Option<DfsMetadataReplicaShardContext>>> {
            let context = self
                .retained_metadata_replica_context
                .lock()
                .expect("retained metadata replica context")
                .clone();
            Some(Box::pin(async move { Ok(context) }))
        }

        fn run_smoke<'a>(&'a self) -> DfsDataPlaneHostFuture<'a, DfsSmokeReport> {
            Box::pin(async move {
                *self.smoke_calls.lock().expect("smoke calls") += 1;
                let local = sinomo_core::node::NodeId::from_legacy_named(
                    "node_dfs_service_data_plane_local",
                )
                .expect("local node");
                Ok(DfsSmokeReport {
                    path: "/services/dfs-smoke/service-route.txt".to_string(),
                    file_system_id: sifs_file_system_id(),
                    object_id: DfsObjectId::from_legacy_named("obj_dfs_service_data_plane")
                        .expect("object"),
                    version: DfsVersion::from_legacy_named("version_dfs_service_data_plane")
                        .expect("version"),
                    content_hash: Some("hash".to_string()),
                    size_bytes: Some(4),
                    requested_replica_node_ids: vec![local.clone()],
                    committed_replica_node_ids: vec![local.clone()],
                    pending_repair_count: 0,
                    metadata_replication: DfsMetadataJournalReplicationReport {
                        outcomes: Vec::new(),
                    },
                    attempts: vec![DfsSmokeAttempt {
                        replica_node_ids: vec![local],
                        path: "/services/dfs-smoke/service-route.txt".to_string(),
                        committed: true,
                        detail: "ok".to_string(),
                    }],
                })
            })
        }

        fn inspect<'a>(
            &'a self,
            data_dir: Arc<SinomoDataDir>,
            request: DaemonDfsInspectionRequest,
        ) -> DfsDataPlaneHostFuture<'a, DaemonDfsInspectionResponse> {
            Box::pin(async move {
                *self.inspection_calls.lock().expect("inspection calls") += 1;
                dfs_inspection(data_dir.as_ref(), request)
                    .map_err(|error| DfsDataPlaneHostError::new(error.to_string()))
            })
        }

        fn apply_metadata_journal_batch<'a>(
            &'a self,
            _data_dir: Arc<SinomoDataDir>,
            authenticated_source_node_id: NodeId,
            command: DfsMetadataJournalBatchCommand,
            context: Option<DfsMetadataReplicaShardContext>,
        ) -> DfsDataPlaneHostFuture<'a, DfsMetadataJournalBatchResponse> {
            Box::pin(async move {
                let valid_context = context.as_ref().is_some_and(|context| {
                    command.source_node_id.as_ref() == Some(&authenticated_source_node_id)
                        && command.file_system_id.as_ref() == Some(&context.shard.file_system_id)
                        && command.batch.shard_id.0 == context.shard.shard_id
                        && command.authority_epoch == Some(context.shard.epoch)
                        && context.shard.primary_node_id == authenticated_source_node_id
                        && metadata_replica_context_has_local_ingest_membership(context)
                });
                if !valid_context {
                    return Ok(DfsMetadataJournalBatchResponse {
                        request_id: command.request_id,
                        shard_id: command.batch.shard_id,
                        next_after_sequence: command.batch.next_after_sequence,
                        applied_entries: 0,
                        verified_prefix_only: false,
                        verified_authority_probe_id: None,
                        result: crate::DfsMetadataJournalBatchResult::Rejected {
                            reason: "missing or stale hosted replica context".to_string(),
                            code: None,
                        },
                    });
                }
                self.journal_request_ids
                    .lock()
                    .expect("journal request ids")
                    .push(command.request_id.clone());
                self.journal_contexts
                    .lock()
                    .expect("journal contexts")
                    .push(context.expect("validated journal context"));
                Ok(DfsMetadataJournalBatchResponse {
                    request_id: command.request_id,
                    shard_id: command.batch.shard_id,
                    next_after_sequence: command.batch.next_after_sequence,
                    applied_entries: command.batch.entries.len(),
                    verified_prefix_only: command.verify_prefix_only,
                    verified_authority_probe_id: None,
                    result: crate::DfsMetadataJournalBatchResult::Applied,
                })
            })
        }

        fn install_metadata_snapshot<'a>(
            &'a self,
            _data_dir: Arc<SinomoDataDir>,
            authenticated_source_node_id: NodeId,
            command: DfsMetadataSnapshotInstallCommand,
            context: Option<DfsMetadataReplicaShardContext>,
        ) -> DfsDataPlaneHostFuture<'a, DfsMetadataSnapshotInstallResponse> {
            Box::pin(async move {
                let valid_context = context.as_ref().is_some_and(|context| {
                    command.source_node_id == authenticated_source_node_id
                        && command.file_system_id.as_ref() == Some(&context.shard.file_system_id)
                        && command.shard_id.0 == context.shard.shard_id
                        && command.authority_epoch == Some(context.shard.epoch)
                        && context.shard.primary_node_id == authenticated_source_node_id
                        && metadata_replica_context_has_local_ingest_membership(context)
                });
                if !valid_context {
                    return Ok(DfsMetadataSnapshotInstallResponse {
                        request_id: command.request_id,
                        shard_id: command.shard_id,
                        next_after_sequence: command.next_after_sequence,
                        result: DfsMetadataSnapshotInstallResult::Rejected {
                            reason: "missing or stale hosted replica context".to_string(),
                            code: None,
                        },
                    });
                }
                self.snapshot_request_ids
                    .lock()
                    .expect("snapshot request ids")
                    .push(command.request_id.clone());
                self.snapshot_contexts
                    .lock()
                    .expect("snapshot contexts")
                    .push(context.expect("validated snapshot context"));
                Ok(DfsMetadataSnapshotInstallResponse {
                    request_id: command.request_id,
                    shard_id: command.shard_id,
                    next_after_sequence: command.next_after_sequence,
                    result: DfsMetadataSnapshotInstallResult::Installed {
                        archived_existing: false,
                    },
                })
            })
        }

        fn handle_message<'a>(
            &'a self,
            command: DfsMessageCommand,
            _attachment: Option<DfsInboundAttachment>,
            traceparent: Option<String>,
        ) -> DfsDataPlaneHostFuture<'a, DfsDataPlaneResponse> {
            Box::pin(async move {
                self.message_request_ids
                    .lock()
                    .expect("message request ids")
                    .push(command.request_id.clone());
                self.message_traceparents
                    .lock()
                    .expect("message traceparents")
                    .push(traceparent);
                Ok(DfsDataPlaneResponse::value(DfsMessageResponse {
                    request_id: command.request_id,
                    outbound_messages: Vec::new(),
                }))
            })
        }
    }

    fn temp_data_dir() -> (sinomo_testdir::TestDir, Arc<SinomoDataDir>) {
        let root = sinomo_testdir::TestDir::new("sinomo-service-dfs");
        let data_dir = Arc::new(SinomoDataDir::new(&root));
        (root, data_dir)
    }

    #[test]
    fn component_factory_owns_complete_dfs_runtime_surface() {
        use sinomo_module::ComponentFactory as _;

        let (_root, data_dir) = temp_data_dir();
        data_dir.ensure_root_layout().expect("data layout");
        let cluster_paths = ClusterPaths::new(data_dir.as_ref());
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            sinomo_core::node::ClusterId::from_legacy_named("cluster_dfs_component_factory")
                .expect("cluster id"),
            "DFS component factory",
            NodeId::from_legacy_named("node_dfs_component_factory").expect("node id"),
            "DFS component node",
            "dfs-component-public-key",
            OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");
        let cluster: Arc<dyn ClusterAccess> = Arc::new(ExactSetMetadataAuthority::new(
            KvRevision::default(),
            BTreeMap::new(),
            None,
        ));
        let context = sinomo_module::ComponentContext::new(Arc::clone(&data_dir))
            .with_runtime(runtime.handle().clone())
            .with_cluster_access(cluster)
            .with_object_rpc_transport(Arc::new(UnavailableComponentObjectRpc))
            .with_event_bus(sinomo_module::ComponentEventBus::default());
        let factory = component_factory().expect("DFS component factory");
        let scope = factory.mount(&context).expect("mount DFS component");

        let browser_route = scope
            .services()
            .route(crate::DAEMON_DFS_BROWSER_STATUS_ROUTE)
            .expect("DFS browser status route");
        let browser_route = sinomo_module::service_route_with_component_identity(
            browser_route,
            factory.descriptor(),
        );
        let discovery = browser_route
            .discovery
            .expect("path-access discovery identity");
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
            crate::DFS_MOUNTED_MUTATION_HANDOFF_V1_ROUTE,
            DFS_METRIC_EXPORT_ROUTE,
        ] {
            assert!(
                scope.services().owns(route),
                "missing DFS-owned route {route}"
            );
        }
        assert_eq!(factory.descriptor().event_publications.len(), 2);
        assert_eq!(factory.descriptor().event_subscriptions.len(), 2);
        drop(scope);
        runtime.shutdown_timeout(Duration::from_secs(1));

        let paths = crate::runtime_host::DfsRuntimePaths::new(data_dir.as_ref());
        crate::runtime_host::DfsDataOwnerLock::acquire(&paths)
            .expect("component drop releases DFS data ownership");
    }

    struct ExactSetMetadataAuthorityState {
        revision: KvRevision,
        entries: BTreeMap<String, KvEntry>,
        pending_handoff_mutation: Option<(KvRevision, KvEntry)>,
    }

    struct ExactSetMetadataAuthority {
        state: Mutex<ExactSetMetadataAuthorityState>,
        authoritative_point_calls: AtomicUsize,
        authoritative_exact_set_calls: AtomicUsize,
        requested_exact_sets: Mutex<Vec<Vec<String>>>,
        returned_proof_revisions: Mutex<Vec<KvRevision>>,
    }

    impl ExactSetMetadataAuthority {
        fn new(
            revision: KvRevision,
            entries: BTreeMap<String, KvEntry>,
            pending_handoff_mutation: Option<(KvRevision, KvEntry)>,
        ) -> Self {
            Self {
                state: Mutex::new(ExactSetMetadataAuthorityState {
                    revision,
                    entries,
                    pending_handoff_mutation,
                }),
                authoritative_point_calls: AtomicUsize::new(0),
                authoritative_exact_set_calls: AtomicUsize::new(0),
                requested_exact_sets: Mutex::new(Vec::new()),
                returned_proof_revisions: Mutex::new(Vec::new()),
            }
        }
    }

    impl ClusterAccess for ExactSetMetadataAuthority {
        fn kv_get_local(&self, _key: &str) -> Result<KvLocalRead<Option<KvEntry>>, ServiceError> {
            Err(ServiceError::Service(
                "unexpected local KV point read in exact-set metadata authority test".to_string(),
            ))
        }

        fn kv_range_prefix_local(
            &self,
            _prefix: &str,
        ) -> Result<KvLocalRead<Vec<KvEntry>>, ServiceError> {
            Err(ServiceError::Service(
                "unexpected local KV prefix read in exact-set metadata authority test".to_string(),
            ))
        }

        fn kv_get_authoritative_async<'a>(
            &'a self,
            _key: &'a str,
        ) -> Pin<
            Box<
                dyn Future<Output = Result<KvAuthoritativeRead<Option<KvEntry>>, ServiceError>>
                    + Send
                    + 'a,
            >,
        > {
            self.authoritative_point_calls
                .fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                Err(ServiceError::Service(
                    "metadata replica authority must not use sequential point reads".to_string(),
                ))
            })
        }

        fn kv_get_authoritative_exact_set_async<'a>(
            &'a self,
            keys: Vec<String>,
        ) -> Pin<
            Box<
                dyn Future<Output = Result<KvAuthoritativeRead<Vec<Option<KvEntry>>>, ServiceError>>
                    + Send
                    + 'a,
            >,
        > {
            self.authoritative_exact_set_calls
                .fetch_add(1, Ordering::SeqCst);
            self.requested_exact_sets
                .lock()
                .expect("requested exact sets")
                .push(keys.clone());
            Box::pin(async move {
                let read = {
                    let mut state = self.state.lock().expect("exact-set authority state");
                    let revision = state.revision;
                    let value = keys
                        .iter()
                        .map(|key| state.entries.get(key).cloned())
                        .collect();
                    // Linearize the exact set first, then deterministically
                    // change the live handoff before returning. A sequential
                    // shard/handoff read would be able to straddle this hook;
                    // one exact-set proof cannot.
                    if let Some((next_revision, entry)) = state.pending_handoff_mutation.take() {
                        state.revision = next_revision;
                        state.entries.insert(entry.key.clone(), entry);
                    }
                    KvAuthoritativeRead {
                        value,
                        authority: KvAuthorityProof {
                            term: 9,
                            commit_index: revision.0,
                            revision,
                            basis: KvAuthorityBasis::CurrentTermQuorum {
                                voter_acks: 2,
                                required_quorum: 2,
                            },
                        },
                    }
                };
                self.returned_proof_revisions
                    .lock()
                    .expect("returned proof revisions")
                    .push(read.authority.revision);
                Ok(read)
            })
        }

        fn kv_commit_async<'a>(
            &'a self,
            _request: KvTxnRequest,
        ) -> Pin<Box<dyn Future<Output = Result<ClusterKvTxnCommitResult, ServiceError>> + Send + 'a>>
        {
            Box::pin(async move {
                Err(ServiceError::Service(
                    "unexpected KV commit in exact-set metadata authority test".to_string(),
                ))
            })
        }
    }

    fn exact_set_metadata_entry<T: serde::Serialize>(
        key: String,
        value: &T,
        revision: KvRevision,
    ) -> KvEntry {
        KvEntry {
            key,
            value: sinomo_core::kv::encode_kv_value(value).expect("encode exact-set value"),
            create_revision: revision,
            modify_revision: revision,
            version: KvVersion(1),
            lease_id: None,
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
            namespace_prepare_codec: Default::default(),
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

    #[tokio::test]
    async fn metadata_replica_authority_exact_set_linearizes_handoff_mutation() {
        let (_root, data_dir) = temp_data_dir();
        let cluster_paths = ClusterPaths::new(data_dir.as_ref());
        let local_node_id =
            NodeId::from_legacy_named("node_dfs_exact_set_local").expect("local node");
        let primary_node_id =
            NodeId::from_legacy_named("node_dfs_exact_set_primary").expect("primary node");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            sinomo_core::node::ClusterId::from_legacy_named("cluster_dfs_exact_set")
                .expect("cluster"),
            "DFS exact-set metadata authority",
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
        let retiring_shard = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: shard_id.0.clone(),
            primary_node_id: primary_node_id.clone(),
            replica_node_ids: vec![local_node_id.clone()],
            epoch: 41,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let desired_shard = DfsMetadataShardConfig {
            primary_node_id: local_node_id.clone(),
            replica_node_ids: vec![primary_node_id.clone()],
            epoch: retiring_shard.epoch + 1,
            ..retiring_shard.clone()
        };
        let old_handoff = DfsMetadataShardMembershipHandoff {
            handoff_id: "handoff-exact-set-old".to_string(),
            retiring_shard: retiring_shard.clone(),
            desired_shard: desired_shard.clone(),
            required_prepared_sequence: 0,
            required_head_watermark: None,
            retiring_primary_fence: None,
        };
        let new_handoff = DfsMetadataShardMembershipHandoff {
            handoff_id: "handoff-exact-set-new".to_string(),
            ..old_handoff.clone()
        };
        let shard_key = metadata_shard_key(&file_system_id, &shard_id.0);
        let handoff_key = metadata_shard_membership_handoff_key(&file_system_id, &shard_id.0);
        let old_revision = KvRevision(17);
        let new_revision = KvRevision(18);
        let mut entries = BTreeMap::new();
        entries.insert(
            shard_key.clone(),
            exact_set_metadata_entry(shard_key.clone(), &retiring_shard, old_revision),
        );
        entries.insert(
            handoff_key.clone(),
            exact_set_metadata_entry(handoff_key.clone(), &old_handoff, old_revision),
        );
        let authority = Arc::new(ExactSetMetadataAuthority::new(
            old_revision,
            entries,
            Some((
                new_revision,
                exact_set_metadata_entry(handoff_key.clone(), &new_handoff, new_revision),
            )),
        ));
        let cluster: Arc<dyn ClusterAccess> = authority.clone();

        let old_context = authoritative_metadata_replica_shard_context(
            None,
            Some(Arc::clone(&cluster)),
            Arc::clone(&data_dir),
            Some(file_system_id.clone()),
            shard_id.clone(),
            primary_node_id.clone(),
            Some(retiring_shard.epoch),
            Some(old_handoff.handoff_id.clone()),
        )
        .await
        .expect("old exact-set authority context")
        .expect("old context");
        assert_eq!(old_context.shard, retiring_shard);
        assert_eq!(
            old_context.pending_membership_handoff,
            Some(old_handoff.clone()),
            "the first proof must contain the pre-mutation pair, even though the handoff changed before the RPC returned"
        );
        assert!(metadata_replica_context_matches_authority(
            &old_context,
            &file_system_id,
            &shard_id,
            &primary_node_id,
            Some(retiring_shard.epoch),
            Some(&old_handoff.handoff_id),
        ));
        assert_eq!(
            authority
                .authoritative_exact_set_calls
                .load(Ordering::SeqCst),
            1,
            "one replica validation must establish exactly one authority proof"
        );
        assert_eq!(
            authority.authoritative_point_calls.load(Ordering::SeqCst),
            0,
            "replica validation must not issue independently linearized point reads"
        );
        assert_eq!(
            authority
                .requested_exact_sets
                .lock()
                .expect("requested exact sets")
                .as_slice(),
            &[vec![shard_key, handoff_key]],
        );
        assert_eq!(
            authority
                .returned_proof_revisions
                .lock()
                .expect("proof revisions")
                .as_slice(),
            &[old_revision],
        );

        let new_context = authoritative_metadata_replica_shard_context(
            None,
            Some(cluster),
            data_dir,
            Some(file_system_id.clone()),
            shard_id.clone(),
            primary_node_id.clone(),
            Some(retiring_shard.epoch),
            Some(old_handoff.handoff_id.clone()),
        )
        .await
        .expect("new exact-set authority context")
        .expect("new context");
        assert_eq!(
            new_context.pending_membership_handoff,
            Some(new_handoff),
            "a later proof observes the complete post-mutation revision"
        );
        assert!(!metadata_replica_context_matches_authority(
            &new_context,
            &file_system_id,
            &shard_id,
            &primary_node_id,
            Some(retiring_shard.epoch),
            Some(&old_handoff.handoff_id),
        ));
        assert_eq!(
            authority
                .returned_proof_revisions
                .lock()
                .expect("proof revisions")
                .as_slice(),
            &[old_revision, new_revision],
            "no context may combine values from the two revisions"
        );
    }

    #[test]
    fn metadata_replica_local_context_fast_path_requires_exact_authority() {
        let file_system_id =
            DfsFileSystemId::from_legacy_named("filesystem_dfs_service_replica_fast_path")
                .expect("filesystem id");
        let shard_id = DfsMetadataShardId::new("root");
        let primary_node_id =
            NodeId::from_legacy_named("node_dfs_service_replica_fast_path_primary")
                .expect("primary node");
        let local_node_id = NodeId::from_legacy_named("node_dfs_service_replica_fast_path_local")
            .expect("local node");
        let context = DfsMetadataReplicaShardContext {
            local_node_id: local_node_id.clone(),
            shard: DfsMetadataShardConfig {
                file_system_id: file_system_id.clone(),
                shard_id: shard_id.0.clone(),
                primary_node_id: primary_node_id.clone(),
                replica_node_ids: vec![local_node_id],
                epoch: 64,
                assignment: DfsMetadataShardAssignment::Root,
                namespace_prepare_codec: Default::default(),
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            },
            pending_membership_handoff: None,
        };

        assert!(metadata_replica_context_matches_authority(
            &context,
            &file_system_id,
            &shard_id,
            &primary_node_id,
            Some(64),
            None,
        ));
        assert!(metadata_replica_context_can_skip_authoritative_read(
            Some(&context),
            &file_system_id,
            &shard_id,
            &primary_node_id,
            Some(64),
            None,
        ));
        assert!(!metadata_replica_context_matches_authority(
            &context,
            &file_system_id,
            &shard_id,
            &primary_node_id,
            Some(63),
            None,
        ));
        assert!(!metadata_replica_context_matches_authority(
            &context,
            &file_system_id,
            &shard_id,
            &NodeId::from_legacy_named("node_dfs_service_replica_fast_path_stale")
                .expect("stale node"),
            Some(64),
            None,
        ));
        assert!(!metadata_replica_context_matches_authority(
            &context,
            &file_system_id,
            &shard_id,
            &primary_node_id,
            Some(64),
            Some("missing-primary-handoff"),
        ));

        let mut stale_handoff_context = context.clone();
        stale_handoff_context.pending_membership_handoff =
            Some(DfsMetadataShardMembershipHandoff {
                handoff_id: "stale-primary-handoff".to_string(),
                retiring_shard: context.shard.clone(),
                desired_shard: DfsMetadataShardConfig {
                    primary_node_id: context.local_node_id.clone(),
                    replica_node_ids: vec![primary_node_id.clone()],
                    epoch: context.shard.epoch + 1,
                    ..context.shard.clone()
                },
                required_prepared_sequence: 0,
                required_head_watermark: None,
                retiring_primary_fence: None,
            });
        assert!(metadata_replica_context_matches_authority(
            &stale_handoff_context,
            &file_system_id,
            &shard_id,
            &primary_node_id,
            Some(64),
            Some("stale-primary-handoff"),
        ));
        assert!(!metadata_replica_context_can_skip_authoritative_read(
            Some(&stale_handoff_context),
            &file_system_id,
            &shard_id,
            &primary_node_id,
            Some(64),
            Some("stale-primary-handoff"),
        ));

        let mut desired_only_context = stale_handoff_context;
        desired_only_context.shard.replica_node_ids.clear();
        desired_only_context
            .pending_membership_handoff
            .as_mut()
            .expect("pending handoff")
            .retiring_shard
            .replica_node_ids
            .clear();
        assert!(metadata_replica_context_has_local_ingest_membership(
            &desired_only_context
        ));
        assert!(metadata_replica_context_can_skip_authoritative_read(
            Some(&desired_only_context),
            &file_system_id,
            &shard_id,
            &primary_node_id,
            Some(64),
            None,
        ));
        desired_only_context.pending_membership_handoff = None;
        assert!(!metadata_replica_context_can_skip_authoritative_read(
            Some(&desired_only_context),
            &file_system_id,
            &shard_id,
            &primary_node_id,
            Some(64),
            None,
        ));
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
        let registry = register_services_with_data_plane_host(
            ServiceRegistry::builder(),
            Arc::new(StubDfsDataPlaneHost::default()),
        )
        .build();
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
    }

    #[tokio::test]
    async fn dfs_direct_read_service_reuses_initialized_content_store() {
        let (_root, data_dir) = temp_data_dir();
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
        let service = DfsDataPlaneService::new(Arc::new(StubDfsDataPlaneHost::default()));
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
    }

    #[test]
    fn hosted_data_plane_caches_the_host_content_store_reference() {
        let (_root, data_dir) = temp_data_dir();
        data_dir.ensure_root_layout().expect("root layout");
        let shared = Arc::new(
            LocalContentStore::new(data_dir.dfs_dir().join("content")).expect("content store"),
        );
        let host = Arc::new(StubDfsDataPlaneHost::default());
        *host
            .shared_content_store
            .lock()
            .expect("shared content store") = Some(Arc::clone(&shared));
        let service = DfsDataPlaneService::new(host.clone());

        let selected = service
            .content_store_for(&data_dir)
            .expect("hosted content store");
        assert!(Arc::ptr_eq(&selected, &shared));
        assert_eq!(service.cached_content_store_count(), 1);

        *host
            .shared_content_store
            .lock()
            .expect("shared content store") = None;
        let cached = service
            .content_store_for(&data_dir)
            .expect("cached hosted content store");
        assert!(Arc::ptr_eq(&cached, &shared));
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

    #[tokio::test]
    async fn hosted_dfs_status_reuses_the_process_data_plane_host() {
        let host = Arc::new(StubDfsDataPlaneHost::default());
        let registry =
            register_services_with_data_plane_host(ServiceRegistry::builder(), host.clone())
                .build();
        let (_root, data_dir) = temp_data_dir();
        let (disposition, response) =
            dispatch_local(&registry, data_dir, DFS_STATUS_ROUTE, &()).await;
        assert_eq!(disposition, ServiceDisposition::Continue);
        let status: DfsOperatorStatus = response.decode_body().expect("decode hosted status");
        assert_eq!(status.object_count, 0);
        assert_eq!(*host.status_calls.lock().expect("status calls"), 1);
    }

    #[tokio::test]
    async fn dfs_status_route_scopes_operational_counts_to_primary_shards() {
        let registry = register_services(ServiceRegistry::builder()).build();
        let (_root, data_dir) = temp_data_dir();
        let cluster_paths = ClusterPaths::new(data_dir.as_ref());
        let now = OffsetDateTime::now_utc();
        let local = NodeId::from_legacy_named("node_dfs_status_scope_local").expect("local");
        let primary = NodeId::from_legacy_named("node_dfs_status_scope_primary").expect("primary");
        let missing = NodeId::from_legacy_named("node_dfs_status_scope_missing").expect("missing");
        let cluster_id =
            sinomo_core::node::ClusterId::from_legacy_named("cluster_dfs_status_scope")
                .expect("cluster");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id,
            "dfs status scope",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
            .expect("cluster kv state");

        let file_system = DfsFileSystem::sifs(local.clone());
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: primary,
            replica_node_ids: vec![local.clone()],
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let mut cluster_kv = sinomo_core::kv::InProcessKvStore::restore(
            sinomo_core::kv::read_cluster_kv_cached_snapshot(&cluster_paths.kv_paths())
                .expect("cluster kv snapshot"),
        )
        .expect("cluster kv store");
        cluster_kv
            .put_serde(
                metadata_shard_key(&file_system.file_system_id, "root"),
                &shard,
                None,
            )
            .expect("shard op");
        let snapshot = cluster_kv.snapshot().expect("cluster kv snapshot");
        sinomo_core::kv::write_cluster_kv_snapshot(&cluster_paths.kv_paths(), &snapshot)
            .expect("write shard config");
        cluster_paths
            .kv_paths()
            .raft_store()
            .append_command_with_response(
                &local,
                sinomo_core::kv::KvCommand::Op(
                    sinomo_core::kv::KvOp::put_serde(
                        metadata_shard_key(&file_system.file_system_id, "root"),
                        &shard,
                        None,
                    )
                    .expect("canonical shard op"),
                ),
            )
            .expect("write canonical shard config");

        let object_id =
            DfsObjectId::from_legacy_named("obj_dfs_status_scope_repair").expect("object");
        let version = DfsVersion::from_legacy_named("version_dfs_status_scope").expect("version");
        let content_hash = "blake3:status-scope".to_string();
        let mut backend = LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new("root"),
            default_dfs_metadata_shards_root(data_dir.as_ref()).join("root"),
        )
        .expect("metadata backend");
        backend.put_file_system(&file_system).expect("filesystem");
        let object = DfsObjectMetadata {
            object_id: object_id.clone(),
            file_system_id: file_system.file_system_id.clone(),
            path: "/services/cluster-tests/status-scope.txt".to_string(),
            kind: DfsObjectKind::File,
            tombstone_provenance: None,
            owner_node_id: local.clone(),
            current_locations: vec![local.clone()],
            content_hash: Some(content_hash.clone()),
            content_size_bytes: Some(12),
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version: version.clone(),
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        };
        backend.put_object(&object).expect("object");
        backend
            .put_version(&DfsObjectVersionRecord {
                content_durability: None,
                history_order: 0,
                retained: None,
                file_system_id: file_system.file_system_id.clone(),
                object_id: object_id.clone(),
                version: version.clone(),
                content_hash: Some(content_hash.clone()),
                size_bytes: Some(12),
                locations: vec![local.clone()],
                created_by_event_id: None,
                created_at: now,
                visible: true,
            })
            .expect("version");
        backend
            .put_intent(&DfsWriteIntentRecord {
                publication: Default::default(),
                intent_id: "intent_dfs_status_scope".to_string(),
                file_system_id: file_system.file_system_id.clone(),
                object_id: object_id.clone(),
                base_version: None,
                target_version: version.clone(),
                coordinator_node_id: local.clone(),
                operation_id: None,
                operation_order_token: None,
                owner_epoch: 1,
                desired_replica_node_ids: vec![local.clone(), missing.clone()],
                policy: DfsReplicationPolicy::new(1, 2).expect("policy"),
                status: DfsWriteIntentStatus::Committed,
                content_hash: Some(content_hash.clone()),
                object_metadata: Some(object),
                touch_parent_on_create: false,
                create_predecessor_object_id: None,
                created_at: now,
                updated_at: Some(now),
            })
            .expect("intent");
        backend
            .put_receipt(&DfsReplicaReceiptRecord {
                receipt: sinomo_core::DfsWriteReceipt {
                    file_system_id: file_system.file_system_id.clone(),
                    object_id: object_id.clone(),
                    committed_version: version.clone(),
                    owner_node_id: local.clone(),
                    event_id: sinomo_core::Id::generate(),
                },
                replica_node_id: local,
                content_hash: Some(content_hash),
                received_at: now,
            })
            .expect("receipt");
        drop(backend);

        let (disposition, response) =
            dispatch_local(&registry, Arc::clone(&data_dir), DFS_STATUS_ROUTE, &()).await;
        assert_eq!(disposition, ServiceDisposition::Continue);
        let status: DfsOperatorStatus = response.decode_body().expect("decode status");
        assert_eq!(status.object_count, 1);
        assert_eq!(status.pending_repair_count, 0);

        let (disposition, response) = dispatch_local(
            &registry,
            data_dir,
            DFS_INSPECTION_ROUTE,
            &DaemonDfsInspectionRequest::Status,
        )
        .await;
        assert_eq!(disposition, ServiceDisposition::Continue);
        let response: DaemonDfsInspectionResponse =
            response.decode_body().expect("decode inspection");
        let DaemonDfsInspectionResponse::Status { status } = response else {
            panic!("expected status response");
        };
        assert_eq!(status.object_count, 1);
        assert_eq!(status.pending_repair_count, 0);
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
            batch: DfsMetadataJournalBatch {
                shard_id: DfsMetadataShardId::new("shard-local"),
                entries: Vec::new(),
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
        .export_table_snapshot();
        let command = DfsMetadataSnapshotInstallCommand {
            request_id: "req-snapshot-1".to_string(),
            source_node_id: sinomo_core::node::NodeId::from_legacy_named("node_snapshot_source")
                .expect("source node"),
            file_system_id: None,
            authority_epoch: None,
            primary_handoff_id: None,
            shard_id: DfsMetadataShardId::new("shard-snapshot-local"),
            next_after_sequence: 7,
            snapshot,
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

    #[tokio::test]
    async fn hosted_replica_routes_use_direct_authorized_host_methods() {
        let host = Arc::new(StubDfsDataPlaneHost::default());
        let registry =
            register_services_with_data_plane_host(ServiceRegistry::builder(), host.clone())
                .build();
        let (_root, data_dir) = temp_data_dir();
        let primary =
            NodeId::from_legacy_named("node_dfs_hosted_replica_primary").expect("primary");
        let local = NodeId::from_legacy_named("node_dfs_hosted_replica_local").expect("local");
        let signer = ServiceSigner::Remote {
            node_id: primary.to_string(),
            public_key: "pk-dfs-hosted-replica-primary".to_string(),
        };
        let cluster_paths = ClusterPaths::new(data_dir.as_ref());
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            sinomo_core::node::ClusterId::from_legacy_named("cluster_dfs_hosted_replica")
                .expect("cluster"),
            "hosted replica service test",
            local.clone(),
            "local",
            "pk-local",
            OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state)
            .expect("write cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
            .expect("cluster kv state");
        let shard = DfsMetadataShardConfig {
            file_system_id: sifs_file_system_id(),
            shard_id: "root".to_string(),
            primary_node_id: primary.clone(),
            replica_node_ids: vec![local.clone()],
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        *host
            .retained_metadata_replica_context
            .lock()
            .expect("retained metadata replica context") = Some(DfsMetadataReplicaShardContext {
            local_node_id: local.clone(),
            shard: shard.clone(),
            pending_membership_handoff: None,
        });
        let mut cluster_kv = sinomo_core::kv::InProcessKvStore::restore(
            sinomo_core::kv::read_cluster_kv_cached_snapshot(&cluster_paths.kv_paths())
                .expect("cluster kv snapshot"),
        )
        .expect("cluster kv store");
        cluster_kv
            .put_serde(
                metadata_shard_key(&shard.file_system_id, &shard.shard_id),
                &shard,
                None,
            )
            .expect("shard op");
        let kv_snapshot = cluster_kv.snapshot().expect("cluster kv snapshot");
        sinomo_core::kv::write_cluster_kv_snapshot(&cluster_paths.kv_paths(), &kv_snapshot)
            .expect("write shard config");
        let journal = DfsMetadataJournalBatchCommand {
            request_id: "hosted-replica-journal".to_string(),
            source_node_id: Some(primary.clone()),
            file_system_id: Some(sifs_file_system_id()),
            authority_epoch: Some(1),
            verify_prefix_only: false,
            prefix_probe: None,
            authority_probe: None,
            durable_replication: None,
            batch: DfsMetadataJournalBatch {
                shard_id: DfsMetadataShardId::new("root"),
                entries: Vec::new(),
                next_after_sequence: 11,
            },
        };
        let (_, response) = dispatch_with_signer(
            &registry,
            Arc::clone(&data_dir),
            DFS_METADATA_JOURNAL_BATCH_ROUTE,
            &journal,
            signer.clone(),
        )
        .await
        .expect("hosted journal route");
        let response: DfsMetadataJournalBatchResponse = response.decode_body().expect("journal");
        assert_eq!(response.request_id, journal.request_id);
        assert!(matches!(
            response.result,
            crate::DfsMetadataJournalBatchResult::Applied
        ));

        let snapshot_backend = LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new("snapshot-source"),
            data_dir.root().join("hosted-snapshot-source"),
        )
        .expect("snapshot source");
        let snapshot = DfsMetadataSnapshotInstallCommand {
            request_id: "hosted-replica-snapshot".to_string(),
            source_node_id: primary.clone(),
            file_system_id: Some(sifs_file_system_id()),
            authority_epoch: Some(1),
            primary_handoff_id: None,
            shard_id: DfsMetadataShardId::new("root"),
            next_after_sequence: 12,
            snapshot: snapshot_backend.export_table_snapshot(),
            reason: "hosted route test".to_string(),
        };
        let (_, response) = dispatch_with_signer(
            &registry,
            Arc::clone(&data_dir),
            DFS_METADATA_SNAPSHOT_INSTALL_ROUTE,
            &snapshot,
            signer.clone(),
        )
        .await
        .expect("hosted snapshot route");
        let response: DfsMetadataSnapshotInstallResponse =
            response.decode_body().expect("snapshot");
        assert_eq!(response.request_id, snapshot.request_id);
        assert!(matches!(
            response.result,
            DfsMetadataSnapshotInstallResult::Installed { .. }
        ));

        let claimed =
            NodeId::from_legacy_named("node_dfs_hosted_replica_claimed").expect("claimed");
        let mut mismatched_journal = journal.clone();
        mismatched_journal.request_id = "hosted-replica-journal-mismatch".to_string();
        mismatched_journal.source_node_id = Some(claimed.clone());
        let (_, response) = dispatch_with_signer(
            &registry,
            Arc::clone(&data_dir),
            DFS_METADATA_JOURNAL_BATCH_ROUTE,
            &mismatched_journal,
            signer.clone(),
        )
        .await
        .expect("hosted journal mismatch response");
        let response: DfsMetadataJournalBatchResponse = response.decode_body().expect("journal");
        assert!(matches!(
            response.result,
            crate::DfsMetadataJournalBatchResult::Rejected { .. }
        ));

        let mut mismatched_snapshot = snapshot.clone();
        mismatched_snapshot.request_id = "hosted-replica-snapshot-mismatch".to_string();
        mismatched_snapshot.source_node_id = claimed;
        let (_, response) = dispatch_with_signer(
            &registry,
            Arc::clone(&data_dir),
            DFS_METADATA_SNAPSHOT_INSTALL_ROUTE,
            &mismatched_snapshot,
            signer,
        )
        .await
        .expect("hosted snapshot mismatch response");
        let response: DfsMetadataSnapshotInstallResponse =
            response.decode_body().expect("snapshot");
        assert!(matches!(
            response.result,
            DfsMetadataSnapshotInstallResult::Rejected { .. }
        ));

        let mut missing_context_journal = journal.clone();
        missing_context_journal.request_id = "hosted-replica-journal-missing-context".to_string();
        missing_context_journal.file_system_id = Some(
            DfsFileSystemId::from_legacy_named("filesystem_dfs_hosted_replica_missing")
                .expect("missing filesystem"),
        );
        let (_, response) = dispatch_with_signer(
            &registry,
            Arc::clone(&data_dir),
            DFS_METADATA_JOURNAL_BATCH_ROUTE,
            &missing_context_journal,
            ServiceSigner::Remote {
                node_id: primary.to_string(),
                public_key: "pk-dfs-hosted-replica-primary".to_string(),
            },
        )
        .await
        .expect("hosted missing context response");
        let response: DfsMetadataJournalBatchResponse = response.decode_body().expect("journal");
        assert!(matches!(
            response.result,
            crate::DfsMetadataJournalBatchResult::Rejected { .. }
        ));
        assert_eq!(
            host.journal_request_ids
                .lock()
                .expect("journal request ids")
                .as_slice(),
            ["hosted-replica-journal"]
        );
        assert_eq!(
            host.snapshot_request_ids
                .lock()
                .expect("snapshot request ids")
                .as_slice(),
            ["hosted-replica-snapshot"]
        );
        let journal_contexts = host.journal_contexts.lock().expect("journal contexts");
        let snapshot_contexts = host.snapshot_contexts.lock().expect("snapshot contexts");
        assert_eq!(
            journal_contexts.as_slice(),
            &[DfsMetadataReplicaShardContext {
                local_node_id: local.clone(),
                shard: shard.clone(),
                pending_membership_handoff: None,
            }]
        );
        assert_eq!(snapshot_contexts.as_slice(), journal_contexts.as_slice());
        assert!(
            host.message_request_ids
                .lock()
                .expect("message request ids")
                .is_empty(),
            "replica control-plane routes must not enter the routed message host"
        );
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
            batch: DfsMetadataJournalBatch {
                shard_id: DfsMetadataShardId::new("root"),
                entries: Vec::new(),
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
            snapshot: snapshot_backend.export_table_snapshot(),
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

    #[tokio::test]
    async fn hosted_dfs_inspection_reuses_the_process_host() {
        let host = Arc::new(StubDfsDataPlaneHost::default());
        let registry =
            register_services_with_data_plane_host(ServiceRegistry::builder(), host.clone())
                .build();
        let (_test_root, test_data_dir) = temp_data_dir();
        let (disposition, response) = dispatch_local(
            &registry,
            test_data_dir,
            DFS_INSPECTION_ROUTE,
            &DaemonDfsInspectionRequest::Status,
        )
        .await;
        assert_eq!(disposition, ServiceDisposition::Continue);
        let result: DaemonDfsInspectionResponse = response.decode_body().expect("decode");
        assert!(matches!(result, DaemonDfsInspectionResponse::Status { .. }));
        assert_eq!(*host.inspection_calls.lock().expect("inspection calls"), 1);
    }

    #[tokio::test]
    async fn dfs_data_plane_routes_delegate_to_host() {
        let host = Arc::new(StubDfsDataPlaneHost::default());
        let registry =
            register_data_plane_services(ServiceRegistry::builder(), host.clone()).build();
        assert!(registry.owns(DFS_SMOKE_ROUTE));
        assert!(!registry.owns("/dfs/message"));
        assert!(registry.owns(crate::DFS_DATA_READ_ROUTE));
        assert!(registry.owns(DFS_DIRECT_READ_ROUTE));

        let (disposition, smoke_response) = {
            let (_root, data_dir) = temp_data_dir();
            dispatch_local(&registry, data_dir, DFS_SMOKE_ROUTE, &()).await
        };
        assert_eq!(disposition, ServiceDisposition::Continue);
        let smoke: DfsSmokeReport = smoke_response.decode_body().expect("decode smoke");
        assert_eq!(smoke.path, "/services/dfs-smoke/service-route.txt");
        assert_eq!(*host.smoke_calls.lock().expect("smoke calls"), 1);

        let local =
            sinomo_core::node::NodeId::from_legacy_named("node_dfs_service_data_plane_frame_local")
                .expect("local node");
        let typed_command = DfsMessageCommand {
            request_id: "message-route-1".to_string(),
            routed_message: crate::DfsRoutedMessage {
                source_node_id: local.clone(),
                target_node_id: local.clone(),
                frame_id: 2,
                message_id: 2,
                message: crate::DfsNetworkMessage::ReadRequest(crate::DfsReadRequest {
                    request_id: "message-read-1".to_string(),
                    object_id: DfsObjectId::generate(),
                    version: None,
                    content_hash: None,
                    content_size_bytes: None,
                    range_offset: None,
                    range_max_bytes: None,
                }),
            },
        };
        let (_test_root, test_data_dir) = temp_data_dir();
        let (disposition, message_response) = dispatch_with_signer(
            &registry,
            test_data_dir.clone(),
            crate::DFS_DATA_READ_ROUTE,
            &typed_command,
            ServiceSigner::LocalNode {
                node_id: local.to_string(),
                public_key: "pk-dfs-service-data-plane-local".to_string(),
            },
        )
        .await
        .expect("authenticated data-plane dispatch");
        assert_eq!(disposition, ServiceDisposition::Continue);
        let message: crate::DfsMessageResponse =
            message_response.decode_body().expect("decode message");
        assert_eq!(message.request_id, "message-route-1");
        assert!(message.outbound_messages.is_empty());

        assert_eq!(
            host.message_request_ids
                .lock()
                .expect("message request ids")
                .as_slice(),
            ["message-route-1"]
        );
        assert_eq!(
            host.message_traceparents
                .lock()
                .expect("message traceparents")
                .as_slice(),
            [Some(
                "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01".to_string()
            )]
        );
        assert!(!registry.owns("/dfs/protocol/frame"));
    }

    #[tokio::test]
    async fn dfs_data_plane_route_binds_authenticated_caller_to_routed_source() {
        let host = Arc::new(StubDfsDataPlaneHost::default());
        let registry =
            register_data_plane_services(ServiceRegistry::builder(), host.clone()).build();
        let claimed = NodeId::from_legacy_named("node_dfs_data_plane_claimed").expect("claimed");
        let authenticated =
            NodeId::from_legacy_named("node_dfs_data_plane_authenticated").expect("authenticated");
        let command = DfsMessageCommand {
            request_id: "message-route-source-mismatch".to_string(),
            routed_message: crate::DfsRoutedMessage {
                source_node_id: claimed.clone(),
                target_node_id: authenticated.clone(),
                frame_id: 3,
                message_id: 3,
                message: crate::DfsNetworkMessage::ReadRequest(crate::DfsReadRequest {
                    request_id: "message-read-source-mismatch".to_string(),
                    object_id: DfsObjectId::generate(),
                    version: None,
                    content_hash: None,
                    content_size_bytes: None,
                    range_offset: None,
                    range_max_bytes: None,
                }),
            },
        };
        let (_root, data_dir) = temp_data_dir();
        let error = dispatch_with_signer(
            &registry,
            data_dir,
            crate::DFS_DATA_READ_ROUTE,
            &command,
            ServiceSigner::Remote {
                node_id: authenticated.to_string(),
                public_key: "pk-dfs-data-plane-authenticated".to_string(),
            },
        )
        .await
        .expect_err("mismatched routed source must be rejected before host dispatch");
        assert!(error.to_string().contains("does not match claimed source"));
        assert!(
            host.message_request_ids
                .lock()
                .expect("message request ids")
                .is_empty(),
            "an impersonated routed source must not reach the DFS message host"
        );
    }
}
