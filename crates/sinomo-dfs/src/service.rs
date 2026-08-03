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

#[allow(unused_imports)]
use sinomo_core::cluster::ClusterKvPathsExt;
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
    DfsMetadataReplicaShardContext, DfsMetadataShardConfig, DfsMetadataShardId,
    DfsMetadataShardMembershipHandoff, DfsMetadataSnapshotInstallCommand, DfsOperatorStatus,
    DfsOutboundAttachment, DfsSmokeReport, LocalContentStore, dfs_module_surfaces,
    dfs_route_is_message_data_plane, inspect_cluster_aware_dfs, inspect_cluster_aware_dfs_status,
    metadata_shard_key, metadata_shard_membership_handoff_key,
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

pub fn register_data_plane_services(
    builder: ServiceRegistryBuilder,
    host: Arc<dyn DfsDataPlaneHost>,
) -> ServiceRegistryBuilder {
    builder.register(DfsDataPlaneService::new(host))
}

/// Register the complete DFS protocol around one process-owned host.
///
/// Sharing the host lets control-plane status reuse the daemon's resident
/// metadata generation instead of opening a parallel inspection backend.
pub fn register_services_with_data_plane_host(
    builder: ServiceRegistryBuilder,
    host: Arc<dyn DfsDataPlaneHost>,
) -> ServiceRegistryBuilder {
    builder
        .register(DfsService::with_data_plane_host(Arc::clone(&host)))
        .register(DfsDataPlaneService::new(host))
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
                    let may_have_deferred_materialization = command.durable_replication.is_some()
                        || command
                            .authority_probe
                            .as_ref()
                            .is_some_and(|probe| probe.durable_journal_ack);
                    let materialize_shard_id = command.batch.shard_id.clone();
                    let authenticated_source_node_id = authenticated_dfs_cluster_source_node_id(
                        &ctx,
                        DFS_METADATA_JOURNAL_BATCH_ROUTE,
                    )?;
                    let context = authoritative_metadata_replica_shard_context(
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
                    // rebuild as soon as that persistence attempt returns;
                    // rejected pre-ingest requests simply make this a no-op.
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
                    let context = authoritative_metadata_replica_shard_context(
                        ctx.cluster.clone(),
                        Arc::clone(&ctx.data_dir),
                        command.file_system_id.clone(),
                        command.shard_id.clone(),
                        authenticated_source_node_id.clone(),
                        command.authority_epoch,
                        command.primary_handoff_id.clone(),
                    )
                    .await?;
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
                    let data_dir = Arc::clone(&ctx.data_dir);
                    let response =
                        tokio::task::spawn_blocking(move || dfs_inspection(&data_dir, inspection))
                            .await
                            .map_err(|source| {
                                ServiceError::Service(format!(
                                    "DFS inspection task failed: {source}"
                                ))
                            })??;
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
                                | crate::DfsNetworkMessage::DistributedWriteRequest(_)
                                | crate::DfsNetworkMessage::ExclusiveCreateRequest(_)
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
    cluster: Option<Arc<dyn ClusterAccess>>,
    data_dir: Arc<SinomoDataDir>,
    file_system_id: Option<DfsFileSystemId>,
    shard_id: DfsMetadataShardId,
    source_node_id: NodeId,
    authority_epoch: Option<u64>,
    primary_handoff_id: Option<String>,
) -> Result<Option<DfsMetadataReplicaShardContext>, ServiceError> {
    let started_at = Instant::now();
    let Some(file_system_id) = file_system_id else {
        return Ok(None);
    };
    let local_read_started_at = Instant::now();
    let local_data_dir = Arc::clone(&data_dir);
    let local_shard_id = shard_id.clone();
    let local_context = tokio::task::spawn_blocking(move || {
        crate::metadata_replication::cluster_dfs_metadata_replica_shard_context(
            &ClusterPaths::new(local_data_dir.as_ref()),
            &local_shard_id,
        )
    })
    .await
    .map_err(|source| {
        ServiceError::Service(format!(
            "DFS metadata replica authority read task failed: {source}"
        ))
    })?
    .map_err(service_error)?;
    let local_read_ms = local_read_started_at.elapsed().as_millis();
    if metadata_replica_context_can_skip_authoritative_read(
        local_context.as_ref(),
        &file_system_id,
        &shard_id,
        &source_node_id,
        authority_epoch,
        primary_handoff_id.as_deref(),
    ) {
        dfs_service_perf_trace(format_args!(
            "metadata-replica-authority-context shard={} mode=local local_read_ms={} total_ms={}",
            shard_id.0,
            local_read_ms,
            started_at.elapsed().as_millis(),
        ));
        return Ok(local_context);
    }
    let Some(cluster) = cluster else {
        // A role-rotation probe can persist a promotion marker and authorize
        // the next primary generation. A locally coherent but retired KV
        // snapshot is therefore not sufficient; fail closed without a
        // current-term authoritative read.
        dfs_service_perf_trace(format_args!(
            "metadata-replica-authority-context shard={} mode=no-cluster local_read_ms={} handoff={} total_ms={}",
            shard_id.0,
            local_read_ms,
            primary_handoff_id.is_some(),
            started_at.elapsed().as_millis(),
        ));
        return if primary_handoff_id.is_some() {
            Ok(None)
        } else {
            Ok(local_context)
        };
    };
    let shard_key = metadata_shard_key(&file_system_id, &shard_id.0);
    let shard_read_started_at = Instant::now();
    let shard = cluster
        .kv_get_authoritative_async(&shard_key)
        .await?
        .value
        .map(|entry| entry.decode::<DfsMetadataShardConfig>())
        .transpose()?;
    let shard_read_ms = shard_read_started_at.elapsed().as_millis();
    let Some(shard) = shard else {
        dfs_service_perf_trace(format_args!(
            "metadata-replica-authority-context shard={} mode=authoritative-missing local_read_ms={} shard_read_ms={} total_ms={}",
            shard_id.0,
            local_read_ms,
            shard_read_ms,
            started_at.elapsed().as_millis(),
        ));
        return Ok(None);
    };
    let handoff_key = metadata_shard_membership_handoff_key(&shard.file_system_id, &shard.shard_id);
    let handoff_read_started_at = Instant::now();
    let pending_membership_handoff = cluster
        .kv_get_authoritative_async(&handoff_key)
        .await?
        .value
        .map(|entry| entry.decode::<DfsMetadataShardMembershipHandoff>())
        .transpose()?
        .filter(|handoff| {
            handoff.retiring_shard == shard
                && handoff.desired_shard.file_system_id == shard.file_system_id
                && handoff.desired_shard.shard_id == shard.shard_id
                && handoff.desired_shard.epoch == shard.epoch.saturating_add(1)
        });
    let handoff_read_ms = handoff_read_started_at.elapsed().as_millis();
    let state =
        sinomo_core::cluster::read_cluster_state_without_kv(&ClusterPaths::new(data_dir.as_ref()))?;
    let context = DfsMetadataReplicaShardContext {
        local_node_id: state.local_node_id,
        shard,
        pending_membership_handoff,
    };
    dfs_service_perf_trace(format_args!(
        "metadata-replica-authority-context shard={} mode=authoritative local_read_ms={} shard_read_ms={} handoff_read_ms={} handoff={} total_ms={}",
        shard_id.0,
        local_read_ms,
        shard_read_ms,
        handoff_read_ms,
        primary_handoff_id.is_some(),
        started_at.elapsed().as_millis(),
    ));
    Ok(Some(context))
}

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
    use sinomo_core::node::NodeId;
    use sinomo_core::service::{ResponseSink, ServiceRegistry, ServiceSigner};
    use std::sync::{Arc, Mutex};
    use time::OffsetDateTime;

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
        message_request_ids: Mutex<Vec<String>>,
        message_traceparents: Mutex<Vec<Option<String>>>,
        shared_content_store: Mutex<Option<Arc<LocalContentStore>>>,
    }

    impl DfsDataPlaneHost for StubDfsDataPlaneHost {
        fn status<'a>(&'a self) -> DfsDataPlaneHostFuture<'a, DfsOperatorStatus> {
            Box::pin(async move {
                *self.status_calls.lock().expect("status calls") += 1;
                Ok(DfsOperatorStatus {
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
                intent_id: "intent_dfs_status_scope".to_string(),
                file_system_id: file_system.file_system_id.clone(),
                object_id: object_id.clone(),
                base_version: None,
                target_version: version.clone(),
                coordinator_node_id: local.clone(),
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
                    file_system_id: file_system.file_system_id.clone().into(),
                    object_id: object_id.clone().into(),
                    committed_version: version.clone().into(),
                    owner_node_id: local.clone().into(),
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
            crate::DfsMetadataJournalBatchResult::Rejected { ref reason }
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
            DfsMetadataSnapshotInstallResult::Rejected { ref reason }
                if reason.contains("authenticated metadata snapshot caller")
                    && reason.contains(claimed.as_str())
        ));
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
