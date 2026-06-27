//! The DFS control-plane service (host-agnostic).
//!
//! Serves the DFS object-RPC routes against a [`ServiceContext`]. Everything is derived
//! from the request's [`ServiceContext::data_dir`] using public `sinomo-dfs`/
//! `sinomo-network` APIs, so DFS runs with no daemon and no live cluster (limited to
//! whatever is materialized locally on disk). Route names are owned by `sinomo-dfs`;
//! `sinomo-network` only provides the object-RPC framing. Cluster-replica routes read the local
//! cluster-KV snapshot to validate shard roles; with no cluster state present they
//! either apply locally (journal batch) or reject gracefully (snapshot install), the
//! same way the daemon's handlers do.
//!
//! Covers the host-agnostic DFS control-plane routes. Smoke and typed message
//! data-plane routes bind to a [`DfsDataPlaneHost`], so `sinomo-dfs` still owns
//! the route contract while an embedding process supplies cluster transport.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use crate::{
    DFS_CONTROL_PLANE_ROUTES, DFS_DATA_PLANE_SERVICE_ROUTES, DFS_DIRECT_READ_ROUTE,
    DFS_EXPOSE_CONFIG_ROUTE, DFS_INSPECTION_ROUTE, DFS_METADATA_JOURNAL_BATCH_ROUTE,
    DFS_METADATA_SNAPSHOT_INSTALL_ROUTE, DFS_SMOKE_ROUTE, DFS_STATUS_ROUTE,
    DaemonDfsInspectionRequest, DaemonDfsInspectionResponse, DfsMessageCommand, DfsMessageResponse,
    DfsMetadataBackend, DfsMetadataJournalBatchCommand, DfsMetadataJournalBatchResponse,
    DfsMetadataShardConfig, DfsMetadataSnapshotInstallCommand, DfsMetadataSnapshotInstallResponse,
    DfsOperatorInspector, DfsOperatorStatus, DfsSmokeReport, decode_dfs_message_rpc_request,
    default_dfs_metadata_root, default_dfs_metadata_shards_root,
    default_dfs_repair_attempt_snapshot_path, dfs_module_surfaces, dfs_route_is_message_data_plane,
    ensure_default_cluster_aware_dfs_expose_config, handle_dfs_direct_read_rpc_request,
    metadata_shards_prefix, open_local_sharded_metadata_backend, open_root_local_metadata_backend,
    read_dfs_repair_attempt_snapshot, sifs_file_system_id,
};
use sinomo_local::SinomoDataDir;
use sinomo_network::{
    ClusterPaths, ObjectRpcRequest, ObjectRpcResponse, read_cluster_state_without_kv,
};

use sinomo_service::{
    Service, ServiceContext, ServiceDisposition, ServiceError, ServiceFuture,
    ServiceRegistryBuilder, ServiceRoute,
};

const DFS_DEFAULT_ROUTE_CONCURRENCY_LIMIT: usize = 8;
const DFS_DATA_PLANE_ROUTE_CONCURRENCY_LIMIT: usize = 64;

fn dfs_service_perf_trace(args: std::fmt::Arguments<'_>) {
    tracing::trace!(target: "sinomo.dfs.perf", detail = %args, "dfs service perf");
}

/// DFS control-plane service. Host-agnostic: it derives everything from the request's
/// [`ServiceContext::data_dir`], so it runs with or without a daemon/cluster.
#[derive(Default)]
pub struct DfsService;

impl DfsService {
    pub fn new() -> Self {
        Self
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

fn dfs_service_route(route: &'static str) -> ServiceRoute {
    let descriptor = dfs_module_surfaces()
        .object_rpc_route_descriptor(route)
        .expect("DFS module surfaces should be valid")
        .unwrap_or_else(|| panic!("unknown DFS service route {route}"));
    ServiceRoute::from_route_metadata(
        descriptor.route,
        descriptor.category,
        descriptor.auth_scope,
        descriptor
            .required_capabilities
            .into_iter()
            .map(|capability| capability.to_string()),
    )
    .with_concurrency_limit(dfs_service_route_concurrency_limit(route))
}

fn dfs_service_route_concurrency_limit(route: &str) -> usize {
    if dfs_route_is_message_data_plane(route) || route == DFS_DIRECT_READ_ROUTE {
        DFS_DATA_PLANE_ROUTE_CONCURRENCY_LIMIT
    } else {
        DFS_DEFAULT_ROUTE_CONCURRENCY_LIMIT
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
    fn run_smoke<'a>(&'a self) -> DfsDataPlaneHostFuture<'a, DfsSmokeReport>;

    fn handle_message<'a>(
        &'a self,
        command: DfsMessageCommand,
    ) -> DfsDataPlaneHostFuture<'a, DfsMessageResponse>;
}

pub type DfsDataPlaneHostFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, DfsDataPlaneHostError>> + Send + 'a>>;

#[derive(Clone)]
pub struct DfsDataPlaneService {
    host: Arc<dyn DfsDataPlaneHost>,
}

impl DfsDataPlaneService {
    pub fn new(host: Arc<dyn DfsDataPlaneHost>) -> Self {
        Self { host }
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
        Box::pin(async move {
            let response = match request.route.as_str() {
                DFS_STATUS_ROUTE => {
                    let started_at = Instant::now();
                    let _: () = request.decode_body()?;
                    let status_started_at = Instant::now();
                    let status = dfs_status(&ctx.data_dir)?;
                    let status_ms = status_started_at.elapsed().as_millis();
                    let response = ObjectRpcResponse::encode_body(DFS_STATUS_ROUTE, &status)?;
                    dfs_service_perf_trace(format_args!(
                        "service route={} status_ms={} total_ms={}",
                        DFS_STATUS_ROUTE,
                        status_ms,
                        started_at.elapsed().as_millis()
                    ));
                    response
                }
                DFS_EXPOSE_CONFIG_ROUTE => {
                    let _: () = request.decode_body()?;
                    ObjectRpcResponse::encode_body(
                        DFS_EXPOSE_CONFIG_ROUTE,
                        &dfs_expose_config(&ctx.data_dir)?,
                    )?
                }
                DFS_METADATA_JOURNAL_BATCH_ROUTE => {
                    let command: DfsMetadataJournalBatchCommand = request.decode_body()?;
                    ObjectRpcResponse::encode_body(
                        DFS_METADATA_JOURNAL_BATCH_ROUTE,
                        &dfs_metadata_journal_batch(&ctx.data_dir, command)?,
                    )?
                }
                DFS_METADATA_SNAPSHOT_INSTALL_ROUTE => {
                    let command: DfsMetadataSnapshotInstallCommand = request.decode_body()?;
                    ObjectRpcResponse::encode_body(
                        DFS_METADATA_SNAPSHOT_INSTALL_ROUTE,
                        &dfs_metadata_snapshot_install(&ctx.data_dir, command)?,
                    )?
                }
                DFS_INSPECTION_ROUTE => {
                    let inspection: DaemonDfsInspectionRequest = request.decode_body()?;
                    ObjectRpcResponse::encode_body(
                        DFS_INSPECTION_ROUTE,
                        &dfs_inspection(&ctx.data_dir, inspection)?,
                    )?
                }
                other => return Err(ServiceError::UnknownRoute(other.to_string())),
            };
            ctx.respond(response)?;
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
        let host = Arc::clone(&self.host);
        Box::pin(async move {
            let response = match request.route.as_str() {
                DFS_SMOKE_ROUTE => {
                    let _: () = request.decode_body()?;
                    let report = host.run_smoke().await.map_err(service_error)?;
                    ObjectRpcResponse::encode_body(DFS_SMOKE_ROUTE, &report)?
                }
                route if dfs_route_is_message_data_plane(route) => {
                    let command = decode_dfs_message_rpc_request(&request)?;
                    let message_response =
                        host.handle_message(command).await.map_err(service_error)?;
                    ObjectRpcResponse::encode_body(route, &message_response)?
                }
                DFS_DIRECT_READ_ROUTE => handle_dfs_direct_read_rpc_request(&ctx.data_dir, request)
                    .map_err(service_error)?,
                other => return Err(ServiceError::UnknownRoute(other.to_string())),
            };
            ctx.respond(response)?;
            Ok(ServiceDisposition::Continue)
        })
    }
}

fn service_error(error: impl std::fmt::Display) -> ServiceError {
    ServiceError::Service(error.to_string())
}

/// Open the DFS metadata backend for `data_dir`: the sharded backend when cluster state
/// is present on disk (reading shard configs from the local cluster-KV snapshot),
/// otherwise the local-owned root backend.
fn open_metadata_backend(
    data_dir: &SinomoDataDir,
) -> Result<Box<dyn DfsMetadataBackend>, ServiceError> {
    let cluster_paths = ClusterPaths::new(data_dir);
    if cluster_paths.state.exists() {
        let file_system_id = sifs_file_system_id();
        let mut shards = sinomo_kv::read_cluster_kv_prefix_entries(
            &cluster_paths.kv_paths(),
            &format!("{}/", metadata_shards_prefix(&file_system_id)),
        )?
        .into_iter()
        .map(|entry| entry.decode::<DfsMetadataShardConfig>())
        .collect::<Result<Vec<_>, _>>()
        .map_err(service_error)?;
        shards.sort_by(|left, right| left.shard_id.cmp(&right.shard_id));
        if !shards.is_empty() {
            let backend = open_local_sharded_metadata_backend(
                default_dfs_metadata_shards_root(data_dir),
                shards,
            )
            .map_err(service_error)?;
            return Ok(Box::new(backend));
        }
    }
    open_root_local_metadata_backend(default_dfs_metadata_root(data_dir))
        .map(|backend| Box::new(backend) as Box<dyn DfsMetadataBackend>)
        .map_err(service_error)
}

/// Build the DFS operator status, then trim shard detail to the local node's role.
fn dfs_status(data_dir: &SinomoDataDir) -> Result<DfsOperatorStatus, ServiceError> {
    let started_at = Instant::now();
    data_dir.ensure_root_layout().map_err(service_error)?;
    dfs_service_perf_trace(format_args!(
        "status ensure_layout_ms={}",
        started_at.elapsed().as_millis()
    ));
    let metadata_started_at = Instant::now();
    let metadata = open_metadata_backend(data_dir)?;
    dfs_service_perf_trace(format_args!(
        "status open_metadata_ms={} total_ms={}",
        metadata_started_at.elapsed().as_millis(),
        started_at.elapsed().as_millis()
    ));
    let inspector_started_at = Instant::now();
    let mut status = DfsOperatorInspector::new(metadata.as_ref())
        .status()
        .map_err(service_error)?;
    attach_dfs_service_maintenance_observations(&mut status, data_dir)?;
    dfs_service_perf_trace(format_args!(
        "status inspector_ms={} total_ms={}",
        inspector_started_at.elapsed().as_millis(),
        started_at.elapsed().as_millis()
    ));
    let cluster_paths = ClusterPaths::new(data_dir);
    let trim_started_at = Instant::now();
    if let Ok(state) = read_cluster_state_without_kv(&cluster_paths) {
        for shard in &mut status.metadata_shards {
            if shard.primary_node_id.as_ref() == Some(&state.local_node_id) {
                shard.replay_cursors.clear();
            } else {
                shard.replica_lag.clear();
            }
        }
    }
    dfs_service_perf_trace(format_args!(
        "status trim_ms={} total_ms={}",
        trim_started_at.elapsed().as_millis(),
        started_at.elapsed().as_millis()
    ));
    Ok(status)
}

fn attach_dfs_service_maintenance_observations(
    status: &mut DfsOperatorStatus,
    data_dir: &SinomoDataDir,
) -> Result<(), ServiceError> {
    status.repair_attempts =
        read_dfs_repair_attempt_snapshot(&default_dfs_repair_attempt_snapshot_path(data_dir))
            .map_err(service_error)?
            .map(|snapshot| snapshot.records)
            .unwrap_or_default();
    Ok(())
}

/// Serve a DFS operator inspection request against the local metadata backend.
fn dfs_inspection(
    data_dir: &SinomoDataDir,
    request: DaemonDfsInspectionRequest,
) -> Result<DaemonDfsInspectionResponse, ServiceError> {
    data_dir.ensure_root_layout().map_err(service_error)?;
    let metadata = open_metadata_backend(data_dir)?;
    let inspector = DfsOperatorInspector::new(metadata.as_ref());
    let response = match request {
        DaemonDfsInspectionRequest::Status => {
            let mut status = inspector.status().map_err(service_error)?;
            attach_dfs_service_maintenance_observations(&mut status, data_dir)?;
            DaemonDfsInspectionResponse::Status { status }
        }
        DaemonDfsInspectionRequest::FileSystem { file_system_id } => {
            DaemonDfsInspectionResponse::FileSystem {
                file_system: inspector
                    .file_system(&file_system_id)
                    .map_err(service_error)?,
            }
        }
        DaemonDfsInspectionRequest::Path {
            file_system_id,
            path,
        } => DaemonDfsInspectionResponse::Path {
            object: inspector
                .object_by_path(&file_system_id, &path)
                .map_err(service_error)?,
        },
        DaemonDfsInspectionRequest::Object { object_id } => DaemonDfsInspectionResponse::Object {
            object: inspector.object(&object_id).map_err(service_error)?,
        },
        DaemonDfsInspectionRequest::Conflicts {
            file_system_id,
            object_id,
            limit,
            cursor,
        } => {
            let page = inspector
                .conflicts_page(
                    file_system_id.as_ref(),
                    object_id.as_ref(),
                    limit,
                    cursor.as_ref(),
                )
                .map_err(service_error)?;
            DaemonDfsInspectionResponse::Conflicts {
                conflicts: page.conflicts,
                next_cursor: page.next_cursor,
            }
        }
        DaemonDfsInspectionRequest::Repair {
            file_system_id,
            object_id,
            limit,
            cursor,
        } => {
            let page = inspector
                .repairs_page(
                    file_system_id.as_ref(),
                    object_id.as_ref(),
                    limit,
                    cursor.as_ref(),
                )
                .map_err(service_error)?;
            DaemonDfsInspectionResponse::Repair {
                repairs: page.repairs,
                next_cursor: page.next_cursor,
            }
        }
    };
    Ok(response)
}

/// Ensure a default DFS expose config exists, then return it as pretty JSON (the wire
/// contract is a JSON string, matching the daemon).
fn dfs_expose_config(data_dir: &SinomoDataDir) -> Result<String, ServiceError> {
    data_dir.ensure_root_layout().map_err(service_error)?;
    let cluster_paths = ClusterPaths::new(data_dir);
    let config = ensure_default_cluster_aware_dfs_expose_config(data_dir, &cluster_paths)
        .map_err(service_error)?;
    serde_json::to_string_pretty(&config).map_err(service_error)
}

fn dfs_metadata_journal_batch(
    data_dir: &SinomoDataDir,
    command: DfsMetadataJournalBatchCommand,
) -> Result<DfsMetadataJournalBatchResponse, ServiceError> {
    let cluster_paths = ClusterPaths::new(data_dir);
    crate::process_cluster_dfs_metadata_journal_batch(data_dir, &cluster_paths, command)
        .map_err(service_error)
}

fn dfs_metadata_snapshot_install(
    data_dir: &SinomoDataDir,
    command: DfsMetadataSnapshotInstallCommand,
) -> Result<DfsMetadataSnapshotInstallResponse, ServiceError> {
    let cluster_paths = ClusterPaths::new(data_dir);
    crate::process_cluster_dfs_metadata_snapshot_install(data_dir, &cluster_paths, command)
        .map_err(service_error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DFS_OBJECT_RPC_ROUTES, DFS_RPC_CATEGORY};
    use crate::{
        DaemonDfsInspectionRequest, DaemonDfsInspectionResponse, DfsExposeConfig,
        DfsMetadataJournalBatch, DfsMetadataJournalReplicationReport, DfsMetadataShardId,
        DfsMetadataSnapshotInstallCommand, DfsMetadataSnapshotInstallResponse,
        DfsMetadataSnapshotInstallResult, DfsObjectId, DfsSmokeAttempt, DfsVersion,
        LocalSobjMetadataBackend, sifs_file_system_id,
    };
    use sinomo_service::{ResponseSink, ServiceRegistry, ServiceSigner};
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct VecSink {
        frames: Arc<Mutex<Vec<ObjectRpcResponse>>>,
    }

    impl ResponseSink for VecSink {
        fn emit(&mut self, response: ObjectRpcResponse) -> Result<(), ServiceError> {
            self.frames.lock().expect("sink lock").push(response);
            Ok(())
        }
    }

    #[derive(Default)]
    struct StubDfsDataPlaneHost {
        smoke_calls: Mutex<usize>,
        message_request_ids: Mutex<Vec<String>>,
    }

    impl DfsDataPlaneHost for StubDfsDataPlaneHost {
        fn run_smoke<'a>(&'a self) -> DfsDataPlaneHostFuture<'a, DfsSmokeReport> {
            Box::pin(async move {
                *self.smoke_calls.lock().expect("smoke calls") += 1;
                let local =
                    sinomo_node::NodeId::from_legacy_named("node_dfs_service_data_plane_local")
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
        ) -> DfsDataPlaneHostFuture<'a, DfsMessageResponse> {
            Box::pin(async move {
                self.message_request_ids
                    .lock()
                    .expect("message request ids")
                    .push(command.request_id.clone());
                Ok(DfsMessageResponse {
                    request_id: command.request_id,
                    outbound_messages: Vec::new(),
                })
            })
        }
    }

    fn temp_data_dir() -> (sinomo_testdir::TestDir, Arc<SinomoDataDir>) {
        let root = sinomo_testdir::TestDir::new("sinomo-service-dfs");
        let data_dir = Arc::new(SinomoDataDir::new(&root));
        (root, data_dir)
    }

    async fn dispatch_local<B: serde::Serialize>(
        registry: &ServiceRegistry,
        data_dir: Arc<SinomoDataDir>,
        route: &str,
        body: &B,
    ) -> (ServiceDisposition, ObjectRpcResponse) {
        let sink = VecSink::default();
        let frames = Arc::clone(&sink.frames);
        let ctx = ServiceContext {
            data_dir,
            cluster: None,
            signer: ServiceSigner::Local,
            sink: Box::new(sink),
        };
        let request = ObjectRpcRequest::encode_body(route, DFS_RPC_CATEGORY, body).expect("encode");
        let disposition = registry.dispatch(ctx, request).await.expect("dispatch");
        let response = frames
            .lock()
            .expect("frames")
            .first()
            .cloned()
            .expect("a response frame");
        (disposition, response)
    }

    #[test]
    fn dfs_service_route_metadata_matches_module_surfaces() {
        let registry = register_data_plane_services(
            register_services(ServiceRegistry::builder()),
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
        }
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
    async fn dfs_expose_config_serves_default_in_local_mode() {
        let registry = register_services(ServiceRegistry::builder()).build();
        let (disposition, response) = {
            let (_root, data_dir) = temp_data_dir();
            dispatch_local(&registry, data_dir, DFS_EXPOSE_CONFIG_ROUTE, &()).await
        };
        assert_eq!(disposition, ServiceDisposition::Continue);
        let json: String = response.decode_body().expect("decode json string");
        // The wire body is pretty-JSON that parses back into a DfsExposeConfig.
        let _config: DfsExposeConfig =
            serde_json::from_str(&json).expect("valid expose config json");
    }

    #[tokio::test]
    async fn dfs_metadata_journal_batch_applies_locally_without_a_cluster() {
        let registry = register_services(ServiceRegistry::builder()).build();
        let command = DfsMetadataJournalBatchCommand {
            request_id: "req-journal-1".to_string(),
            source_node_id: None,
            batch: DfsMetadataJournalBatch {
                shard_id: DfsMetadataShardId::new("shard-local"),
                entries: Vec::new(),
                next_after_sequence: 0,
            },
        };
        let (_test_root, test_data_dir) = temp_data_dir();
        let (disposition, response) = dispatch_local(
            &registry,
            test_data_dir.clone(),
            DFS_METADATA_JOURNAL_BATCH_ROUTE,
            &command,
        )
        .await;
        assert_eq!(disposition, ServiceDisposition::Continue);
        // No cluster state on disk ⇒ source validation is skipped and the (empty) batch
        // is applied to a fresh local backend; the route returns a well-formed response.
        let result: DfsMetadataJournalBatchResponse = response.decode_body().expect("decode");
        assert_eq!(result.request_id, "req-journal-1");
        assert_eq!(result.shard_id.0, "shard-local");
    }

    #[tokio::test]
    async fn dfs_metadata_snapshot_install_rejects_without_local_shard_config() {
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
            source_node_id: sinomo_node::NodeId::from_legacy_named("node_snapshot_source")
                .expect("source node"),
            shard_id: DfsMetadataShardId::new("shard-snapshot-local"),
            next_after_sequence: 7,
            snapshot,
            reason: "service test".to_string(),
        };

        let (disposition, response) = dispatch_local(
            &registry,
            data_dir,
            DFS_METADATA_SNAPSHOT_INSTALL_ROUTE,
            &command,
        )
        .await;

        assert_eq!(disposition, ServiceDisposition::Continue);
        let result: DfsMetadataSnapshotInstallResponse =
            response.decode_body().expect("decode snapshot response");
        assert_eq!(result.request_id, "req-snapshot-1");
        assert_eq!(result.shard_id.0, "shard-snapshot-local");
        assert_eq!(result.next_after_sequence, 7);
        assert!(matches!(
            result.result,
            DfsMetadataSnapshotInstallResult::Rejected { ref reason }
                if reason.contains("no local shard config")
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
            sinomo_node::NodeId::from_legacy_named("node_dfs_service_data_plane_frame_local")
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
                    start_chunk_index: 0,
                    max_chunks: 0,
                }),
            },
        };
        let (_test_root, test_data_dir) = temp_data_dir();
        let (disposition, message_response) = dispatch_local(
            &registry,
            test_data_dir.clone(),
            crate::DFS_DATA_READ_ROUTE,
            &typed_command,
        )
        .await;
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
        assert!(!registry.owns("/dfs/protocol/frame"));
    }
}
