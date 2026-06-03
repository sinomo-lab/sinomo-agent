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
    DaemonDfsInspectionRequest, DaemonDfsInspectionResponse, DfsExposeStore, DfsMetadataBackend,
    DfsMetadataJournalBatchCommand, DfsMetadataJournalBatchResponse, DfsMetadataJournalBatchResult,
    DfsMetadataShardConfig, DfsMetadataSnapshotInstallCommand, DfsMetadataSnapshotInstallResponse,
    DfsMetadataSnapshotInstallResult, DfsOperatorInspector, DfsOperatorStatus,
    LocalSobjMetadataBackend, default_dfs_metadata_root, default_dfs_metadata_shards_root,
    fallback_dfs_node_id, metadata_shards_prefix, open_local_sharded_metadata_backend,
    open_root_local_metadata_backend, sifs_file_system_id,
};
use sinomo_local::SinomoDataDir;
use sinomo_network::{
    ClusterPaths, ObjectRpcRequest, ObjectRpcResponse, read_cluster_kv_prefix_entries,
    read_cluster_kv_prefix_values, read_cluster_state_without_kv,
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

/// Sanitize a string for use as a single filesystem path segment. Must match the
/// daemon's helper so on-disk metadata lands in the same place.
fn path_safe_segment(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect()
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
        let mut shards = read_cluster_kv_prefix_entries(
            &cluster_paths,
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
    let node_id = if cluster_paths.state.exists() {
        read_cluster_state_without_kv(&cluster_paths)
            .map(|state| state.local_node_id)
            .unwrap_or_else(|_| fallback_dfs_node_id())
    } else {
        fallback_dfs_node_id()
    };
    let config_path = data_dir.dfs_expose_config_file();
    let store = DfsExposeStore::new(&config_path);
    store
        .ensure_default(&data_dir.root(), node_id)
        .map_err(service_error)?;
    let config = store.load().map_err(service_error)?;
    serde_json::to_string_pretty(&config).map_err(service_error)
}

/// Reject a journal batch with `reason`, mirroring the daemon's rejection response.
fn journal_batch_rejection(
    command: &DfsMetadataJournalBatchCommand,
    reason: String,
) -> DfsMetadataJournalBatchResponse {
    DfsMetadataJournalBatchResponse {
        request_id: command.request_id.clone(),
        shard_id: command.batch.shard_id.clone(),
        next_after_sequence: command.batch.next_after_sequence,
        applied_entries: 0,
        result: DfsMetadataJournalBatchResult::Rejected { reason },
    }
}

/// Validate that a journal batch may be applied here (correct shard primary as source,
/// local node is a configured replica). `Ok(Some(_))` is an early rejection; `Ok(None)`
/// means proceed. With no cluster state on disk, batches apply locally.
fn validate_journal_batch_source(
    data_dir: &SinomoDataDir,
    command: &DfsMetadataJournalBatchCommand,
) -> Result<Option<DfsMetadataJournalBatchResponse>, ServiceError> {
    let cluster_paths = ClusterPaths::new(data_dir);
    if !cluster_paths.state.exists() {
        return Ok(None);
    }
    let state = read_cluster_state_without_kv(&cluster_paths)?;
    let shard = read_cluster_kv_prefix_values::<DfsMetadataShardConfig>(
        &cluster_paths,
        &format!("{}/", metadata_shards_prefix(&sifs_file_system_id())),
    )?
    .into_iter()
    .find(|shard| shard.shard_id == command.batch.shard_id.0.as_str());
    let Some(shard) = shard else {
        return Ok(Some(journal_batch_rejection(
            command,
            "metadata journal batch has no local shard config".to_string(),
        )));
    };
    let Some(source_node_id) = command.source_node_id.as_ref() else {
        return Ok(Some(journal_batch_rejection(
            command,
            "metadata journal batch is missing source node id".to_string(),
        )));
    };
    if source_node_id != &shard.primary_node_id {
        return Ok(Some(journal_batch_rejection(
            command,
            format!(
                "metadata journal source {source_node_id} is not shard {} primary {}",
                shard.shard_id, shard.primary_node_id
            ),
        )));
    }
    if shard.primary_node_id == state.local_node_id {
        return Ok(Some(journal_batch_rejection(
            command,
            "metadata primary must not apply a replica journal batch".to_string(),
        )));
    }
    if !shard.replica_node_ids.contains(&state.local_node_id) {
        return Ok(Some(journal_batch_rejection(
            command,
            format!(
                "local node {} is not configured as a replica for metadata shard {}",
                state.local_node_id, shard.shard_id
            ),
        )));
    }
    Ok(None)
}

fn dfs_metadata_journal_batch(
    data_dir: &SinomoDataDir,
    command: DfsMetadataJournalBatchCommand,
) -> Result<DfsMetadataJournalBatchResponse, ServiceError> {
    data_dir.ensure_root_layout().map_err(service_error)?;
    if let Some(rejection) = validate_journal_batch_source(data_dir, &command)? {
        return Ok(rejection);
    }
    let mut backend = LocalSobjMetadataBackend::new(
        command.batch.shard_id.clone(),
        data_dir
            .dfs_dir()
            .join("metadata")
            .join(path_safe_segment(&command.batch.shard_id.0)),
    )
    .map_err(service_error)?;
    let shard_id = command.batch.shard_id.clone();
    let next_after_sequence = command.batch.next_after_sequence;
    match backend.apply_journal_batch(&command.batch) {
        Ok(applied_entries) => Ok(DfsMetadataJournalBatchResponse {
            request_id: command.request_id,
            shard_id,
            next_after_sequence,
            applied_entries,
            result: DfsMetadataJournalBatchResult::Applied,
        }),
        Err(error) => Ok(DfsMetadataJournalBatchResponse {
            request_id: command.request_id,
            shard_id,
            next_after_sequence,
            applied_entries: 0,
            result: DfsMetadataJournalBatchResult::Rejected {
                reason: error.to_string(),
            },
        }),
    }
}

fn dfs_metadata_snapshot_install(
    data_dir: &SinomoDataDir,
    command: DfsMetadataSnapshotInstallCommand,
) -> Result<DfsMetadataSnapshotInstallResponse, ServiceError> {
    data_dir.ensure_root_layout().map_err(service_error)?;
    let cluster_paths = ClusterPaths::new(data_dir);
    let state = read_cluster_state_without_kv(&cluster_paths)?;
    let shard = read_cluster_kv_prefix_values::<DfsMetadataShardConfig>(
        &cluster_paths,
        &format!("{}/", metadata_shards_prefix(&sifs_file_system_id())),
    )?
    .into_iter()
    .find(|shard| shard.shard_id == command.shard_id.0.as_str());

    let reject = |reason: String| DfsMetadataSnapshotInstallResponse {
        request_id: command.request_id.clone(),
        shard_id: command.shard_id.clone(),
        next_after_sequence: command.next_after_sequence,
        result: DfsMetadataSnapshotInstallResult::Rejected { reason },
    };

    let Some(shard) = shard else {
        return Ok(reject(
            "metadata snapshot install has no local shard config".to_string(),
        ));
    };
    if shard.primary_node_id != command.source_node_id {
        return Ok(reject(format!(
            "metadata snapshot source {} is not shard {} primary {}",
            command.source_node_id, shard.shard_id, shard.primary_node_id
        )));
    }
    if shard.primary_node_id == state.local_node_id {
        return Ok(reject(
            "metadata primary must not install a replica snapshot".to_string(),
        ));
    }
    if !shard.replica_node_ids.contains(&state.local_node_id) {
        return Ok(reject(format!(
            "local node {} is not configured as a replica for metadata shard {}",
            state.local_node_id, shard.shard_id
        )));
    }

    let mut backend = LocalSobjMetadataBackend::new(
        command.shard_id.clone(),
        data_dir
            .dfs_dir()
            .join("metadata")
            .join(path_safe_segment(&command.shard_id.0)),
    )
    .map_err(service_error)?;
    match backend.replace_with_replicated_snapshot(
        command.shard_id.clone(),
        command.next_after_sequence,
        command.snapshot,
    ) {
        Ok(archived_existing) => Ok(DfsMetadataSnapshotInstallResponse {
            request_id: command.request_id,
            shard_id: command.shard_id,
            next_after_sequence: command.next_after_sequence,
            result: DfsMetadataSnapshotInstallResult::Installed { archived_existing },
        }),
        Err(error) => Ok(reject(error.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DFS_RPC_CATEGORY;
    use crate::{
        DaemonDfsInspectionRequest, DaemonDfsInspectionResponse, DfsExposeConfig,
        DfsMetadataJournalBatch, DfsMetadataShardId,
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
