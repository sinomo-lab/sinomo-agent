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
//! Covers the host-agnostic DFS control-plane routes. Smoke and protocol-frame data
//! plane routes still require daemon-owned cluster access and remain registered by the
//! daemon data-plane adapter.

use crate::{
    DFS_CONTROL_PLANE_ROUTES, DFS_EXPOSE_CONFIG_ROUTE, DFS_INSPECTION_ROUTE,
    DFS_METADATA_JOURNAL_BATCH_ROUTE, DFS_METADATA_SNAPSHOT_INSTALL_ROUTE, DFS_STATUS_ROUTE,
    DaemonDfsInspectionRequest, DaemonDfsInspectionResponse, DfsMetadataBackend,
    DfsMetadataJournalBatchCommand, DfsMetadataJournalBatchResponse, DfsMetadataShardConfig,
    DfsMetadataSnapshotInstallCommand, DfsMetadataSnapshotInstallResponse, DfsOperatorInspector,
    DfsOperatorStatus, default_dfs_metadata_root, default_dfs_metadata_shards_root,
    ensure_default_cluster_aware_dfs_expose_config, metadata_shards_prefix,
    open_local_sharded_metadata_backend, open_root_local_metadata_backend, sifs_file_system_id,
};
use sinomo_local::SinomoDataDir;
use sinomo_network::{
    ClusterPaths, ObjectRpcRequest, ObjectRpcResponse, read_cluster_state_without_kv,
};

use sinomo_service::{
    Service, ServiceContext, ServiceDisposition, ServiceError, ServiceFuture,
    ServiceRegistryBuilder,
};

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

impl Service for DfsService {
    fn name(&self) -> &'static str {
        "dfs"
    }

    fn routes(&self) -> &'static [&'static str] {
        DFS_CONTROL_PLANE_ROUTES
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        Box::pin(async move {
            let response = match request.route.as_str() {
                DFS_STATUS_ROUTE => {
                    let _: () = request.decode_body()?;
                    ObjectRpcResponse::encode_body(DFS_STATUS_ROUTE, &dfs_status(&ctx.data_dir)?)?
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
    data_dir.ensure_root_layout().map_err(service_error)?;
    let metadata = open_metadata_backend(data_dir)?;
    let mut status = DfsOperatorInspector::new(metadata.as_ref())
        .status()
        .map_err(service_error)?;
    let cluster_paths = ClusterPaths::new(data_dir);
    if let Ok(state) = read_cluster_state_without_kv(&cluster_paths) {
        for shard in &mut status.metadata_shards {
            if shard.primary_node_id.as_ref() == Some(&state.local_node_id) {
                shard.replay_cursors.clear();
            } else {
                shard.replica_lag.clear();
            }
        }
    }
    Ok(status)
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
        DaemonDfsInspectionRequest::Status => DaemonDfsInspectionResponse::Status {
            status: inspector.status().map_err(service_error)?,
        },
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
    use crate::DFS_RPC_CATEGORY;
    use crate::{
        DaemonDfsInspectionRequest, DaemonDfsInspectionResponse, DfsExposeConfig,
        DfsMetadataJournalBatch, DfsMetadataShardId, DfsMetadataSnapshotInstallCommand,
        DfsMetadataSnapshotInstallResponse, DfsMetadataSnapshotInstallResult,
        LocalSobjMetadataBackend,
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

    fn temp_data_dir() -> Arc<SinomoDataDir> {
        let dir = std::env::temp_dir().join(format!(
            "sinomo-service-dfs-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        Arc::new(SinomoDataDir::new(dir))
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

    #[tokio::test]
    async fn dfs_status_serves_in_local_mode_without_a_cluster() {
        let registry = register_services(ServiceRegistry::builder()).build();
        let (disposition, response) =
            dispatch_local(&registry, temp_data_dir(), DFS_STATUS_ROUTE, &()).await;
        assert_eq!(disposition, ServiceDisposition::Continue);
        let _status: DfsOperatorStatus = response.decode_body().expect("decode status");
    }

    #[tokio::test]
    async fn dfs_expose_config_serves_default_in_local_mode() {
        let registry = register_services(ServiceRegistry::builder()).build();
        let (disposition, response) =
            dispatch_local(&registry, temp_data_dir(), DFS_EXPOSE_CONFIG_ROUTE, &()).await;
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
        let (disposition, response) = dispatch_local(
            &registry,
            temp_data_dir(),
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
        let data_dir = temp_data_dir();
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
        let (disposition, response) = dispatch_local(
            &registry,
            temp_data_dir(),
            DFS_INSPECTION_ROUTE,
            &DaemonDfsInspectionRequest::Status,
        )
        .await;
        assert_eq!(disposition, ServiceDisposition::Continue);
        let result: DaemonDfsInspectionResponse = response.decode_body().expect("decode");
        assert!(matches!(result, DaemonDfsInspectionResponse::Status { .. }));
    }
}
