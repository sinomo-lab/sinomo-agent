use crate::{
    DaemonError, DaemonPaths, DaemonRunOptions, cluster_has_local_quorum,
    put_cluster_kv_control_record, send_dfs_metadata_journal_batch_to_endpoint,
};
use serde::{Deserialize, Serialize};
use sinomo_core::DfsObjectPolicy;
use sinomo_dfs::{
    DfsExposeStore, DfsFileSystemConfig, DfsMetadataBackend, DfsMetadataJournalBatchCommand,
    DfsMetadataJournalBatchResponse, DfsMetadataJournalBatchResult, DfsMetadataShardAssignment,
    DfsMetadataShardConfig, DfsMetadataShardId, DfsNodeAdvertisement, DfsObjectVersionRecord,
    LocalJsonMetadataBackend, content_service_key, distributed_sifs,
    distributed_sifs_bootstrap_objects, fallback_dfs_node_id, file_system_config_key,
    metadata_service_key, metadata_shard_key,
};
use sinomo_local::SinomoDataDir;
use sinomo_network::ClusterPaths;
use std::fs;
use std::time::Duration;
use time::OffsetDateTime;

const DFS_METADATA_REPLICATION_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DaemonDfsMetadataReplicationReport {
    pub outcomes: Vec<DaemonDfsMetadataReplicationOutcome>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DaemonDfsMetadataReplicationOutcome {
    Applied {
        target_node_id: sinomo_node::NodeId,
        shard_id: DfsMetadataShardId,
        entries: usize,
        next_after_sequence: u64,
    },
    NoChanges {
        target_node_id: sinomo_node::NodeId,
        shard_id: DfsMetadataShardId,
        after_sequence: u64,
    },
    MissingEndpoint {
        target_node_id: sinomo_node::NodeId,
        shard_id: DfsMetadataShardId,
        after_sequence: u64,
    },
    Rejected {
        target_node_id: sinomo_node::NodeId,
        shard_id: DfsMetadataShardId,
        reason: String,
    },
    Failed {
        target_node_id: sinomo_node::NodeId,
        shard_id: DfsMetadataShardId,
        reason: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct DaemonDfsMetadataReplicationCursor {
    target_node_id: sinomo_node::NodeId,
    shard_id: DfsMetadataShardId,
    next_after_sequence: u64,
}

pub fn ensure_default_local_dfs_expose(paths: &DaemonPaths) -> Result<(), DaemonError> {
    let data_dir = daemon_data_dir(paths)?;
    let data_dir = SinomoDataDir::new(data_dir);
    data_dir.ensure_root_layout()?;
    let cluster_paths = ClusterPaths::new(&data_dir);
    let node_id = if cluster_paths.state.exists() {
        sinomo_network::read_cluster_state_without_kv(&cluster_paths)
            .map(|state| state.local_node_id)
            .unwrap_or_else(|_| fallback_dfs_node_id())
    } else {
        fallback_dfs_node_id()
    };
    DfsExposeStore::new(data_dir.dfs_expose_config_file())
        .ensure_default(&data_dir.root(), node_id)?;
    Ok(())
}

pub fn handle_daemon_dfs_expose_config(paths: &DaemonPaths) -> Result<String, DaemonError> {
    ensure_default_local_dfs_expose(paths)?;
    let data_dir = daemon_data_dir(paths)?;
    let data_dir = SinomoDataDir::new(data_dir);
    let config_path = data_dir.dfs_expose_config_file();
    let config = DfsExposeStore::new(&config_path).load()?;
    serde_json::to_string_pretty(&config).map_err(|source| DaemonError::Json {
        path: config_path.display().to_string(),
        detail: source.to_string(),
    })
}

pub fn handle_daemon_dfs_metadata_journal_batch(
    paths: &DaemonPaths,
    command: DfsMetadataJournalBatchCommand,
) -> Result<DfsMetadataJournalBatchResponse, DaemonError> {
    let data_dir = daemon_data_dir(paths)?;
    let data_dir = SinomoDataDir::new(data_dir);
    data_dir.ensure_root_layout()?;
    let mut backend = LocalJsonMetadataBackend::new(
        command.batch.shard_id.clone(),
        data_dir
            .dfs_dir()
            .join("metadata")
            .join(command.batch.shard_id.0.clone()),
    )?;
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

pub fn ensure_default_distributed_dfs(
    paths: &DaemonPaths,
    options: &DaemonRunOptions,
    publish_cluster_control: bool,
) -> Result<(), DaemonError> {
    let data_dir = daemon_data_dir(paths)?;
    let data_dir = SinomoDataDir::new(data_dir);
    data_dir.ensure_root_layout()?;
    let cluster_paths = ClusterPaths::new(&data_dir);
    if !cluster_paths.state.exists() {
        return Ok(());
    }
    let state = sinomo_network::read_cluster_state_without_kv(&cluster_paths)?;
    let file_system = distributed_sifs(state.local_node_id.clone());
    let mut backend = LocalJsonMetadataBackend::new(
        DfsMetadataShardId::new("root"),
        data_dir.dfs_dir().join("metadata").join("root"),
    )?;
    if backend.get_file_system(&file_system.file_system_id)? != Some(file_system.clone()) {
        backend.put_file_system(&file_system)?;
    }
    for object in distributed_sifs_bootstrap_objects(state.local_node_id.clone()) {
        if backend
            .get_object_by_path(&object.file_system_id, object.path.trim_matches('/'))?
            .is_none()
        {
            backend.put_object(&object)?;
            backend.put_version(&DfsObjectVersionRecord {
                file_system_id: object.file_system_id.clone(),
                object_id: object.object_id.clone(),
                version: object.version.clone(),
                content_hash: object.content_hash.clone(),
                size_bytes: object.content_size_bytes,
                locations: object.current_locations.clone(),
                created_by_event_id: object.created_by_event_id.clone(),
                created_at: OffsetDateTime::now_utc(),
                visible: true,
            })?;
        }
    }
    if !publish_cluster_control || !cluster_has_local_quorum(&cluster_paths, &state) {
        return Ok(());
    }

    put_cluster_kv_control_record(
        &cluster_paths,
        &state.local_node_id,
        file_system_config_key(&file_system.file_system_id),
        &DfsFileSystemConfig::from(&file_system),
    )?;
    put_cluster_kv_control_record(
        &cluster_paths,
        &state.local_node_id,
        metadata_shard_key(&file_system.file_system_id, "root"),
        &DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: state.local_node_id.clone(),
            replica_node_ids: state
                .members
                .iter()
                .filter(|member| member.node_id != state.local_node_id)
                .take(2)
                .map(|member| member.node_id.clone())
                .collect(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        },
    )?;
    let endpoint = format!("loopback://{}:{}", options.host, options.port);
    put_cluster_kv_control_record(
        &cluster_paths,
        &state.local_node_id,
        metadata_service_key(&state.local_node_id),
        &DfsNodeAdvertisement::metadata(state.local_node_id.clone(), endpoint.clone()),
    )?;
    put_cluster_kv_control_record(
        &cluster_paths,
        &state.local_node_id,
        content_service_key(&state.local_node_id),
        &DfsNodeAdvertisement::content(state.local_node_id.clone(), endpoint),
    )?;
    Ok(())
}

pub fn publish_default_dfs_metadata_journal(
    paths: &DaemonPaths,
) -> Result<DaemonDfsMetadataReplicationReport, DaemonError> {
    let data_dir = daemon_data_dir(paths)?;
    let data_dir = SinomoDataDir::new(data_dir);
    let cluster_paths = ClusterPaths::new(&data_dir);
    if !cluster_paths.state.exists() {
        return Ok(DaemonDfsMetadataReplicationReport {
            outcomes: Vec::new(),
        });
    }
    let state = sinomo_network::read_cluster_state_without_kv(&cluster_paths)?;
    let file_system = distributed_sifs(state.local_node_id.clone());
    let store = sinomo_network::read_cluster_kv_store(&cluster_paths)?;
    let Some(entry) = store.get(&metadata_shard_key(&file_system.file_system_id, "root")) else {
        return Ok(DaemonDfsMetadataReplicationReport {
            outcomes: Vec::new(),
        });
    };
    let shard: DfsMetadataShardConfig = entry.decode()?;
    if shard.primary_node_id != state.local_node_id {
        return Ok(DaemonDfsMetadataReplicationReport {
            outcomes: Vec::new(),
        });
    }

    let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
    let backend = LocalJsonMetadataBackend::new(
        shard_id.clone(),
        data_dir.dfs_dir().join("metadata").join(&shard.shard_id),
    )?;
    let endpoints = sinomo_network::cluster_sync_targets_with_connectivity(&cluster_paths, &state);
    let mut outcomes = Vec::new();
    for target_node_id in shard
        .replica_node_ids
        .into_iter()
        .filter(|target_node_id| target_node_id != &state.local_node_id)
    {
        let cursor_path =
            dfs_metadata_replication_cursor_path(&data_dir, &target_node_id, &shard_id);
        let after_sequence = read_dfs_metadata_replication_cursor(&cursor_path)?
            .map(|cursor| cursor.next_after_sequence)
            .unwrap_or(0);
        let entries =
            backend.hydrate_journal_entry_payloads(backend.journal_since(after_sequence)?)?;
        let Some(last_entry) = entries.last() else {
            outcomes.push(DaemonDfsMetadataReplicationOutcome::NoChanges {
                target_node_id,
                shard_id: shard_id.clone(),
                after_sequence,
            });
            continue;
        };
        let target_endpoints = endpoints
            .iter()
            .filter(|endpoint| endpoint.node_id == target_node_id)
            .collect::<Vec<_>>();
        if target_endpoints.is_empty() {
            outcomes.push(DaemonDfsMetadataReplicationOutcome::MissingEndpoint {
                target_node_id,
                shard_id: shard_id.clone(),
                after_sequence,
            });
            continue;
        }
        let next_after_sequence = last_entry.sequence;
        let entry_count = entries.len();
        let command = DfsMetadataJournalBatchCommand {
            request_id: format!(
                "dfs_meta_{}_{}_{}",
                state.local_node_id, target_node_id, next_after_sequence
            ),
            batch: sinomo_dfs::DfsMetadataJournalBatch {
                shard_id: shard_id.clone(),
                entries,
                next_after_sequence,
            },
        };
        let mut failures = Vec::new();
        let mut delivered = false;
        for endpoint in target_endpoints {
            match send_dfs_metadata_journal_batch_to_endpoint(
                endpoint,
                &command,
                DFS_METADATA_REPLICATION_TIMEOUT,
            ) {
                Ok(response) => match response.result {
                    DfsMetadataJournalBatchResult::Applied => {
                        write_dfs_metadata_replication_cursor(
                            &cursor_path,
                            &DaemonDfsMetadataReplicationCursor {
                                target_node_id: target_node_id.clone(),
                                shard_id: shard_id.clone(),
                                next_after_sequence,
                            },
                        )?;
                        outcomes.push(DaemonDfsMetadataReplicationOutcome::Applied {
                            target_node_id: target_node_id.clone(),
                            shard_id: shard_id.clone(),
                            entries: entry_count,
                            next_after_sequence,
                        });
                        delivered = true;
                        break;
                    }
                    DfsMetadataJournalBatchResult::Rejected { reason } => {
                        outcomes.push(DaemonDfsMetadataReplicationOutcome::Rejected {
                            target_node_id: target_node_id.clone(),
                            shard_id: shard_id.clone(),
                            reason,
                        });
                        delivered = true;
                        break;
                    }
                },
                Err(error) => {
                    failures.push(format!("{} failed: {error}", endpoint.address));
                }
            }
        }
        if !delivered {
            outcomes.push(DaemonDfsMetadataReplicationOutcome::Failed {
                target_node_id,
                shard_id: shard_id.clone(),
                reason: failures.join("; "),
            });
        }
    }
    Ok(DaemonDfsMetadataReplicationReport { outcomes })
}

fn daemon_data_dir(paths: &DaemonPaths) -> Result<&std::path::Path, DaemonError> {
    paths.root.parent().ok_or_else(|| DaemonError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon root has no data directory parent".to_string(),
    })
}

fn dfs_metadata_replication_cursor_path(
    data_dir: &SinomoDataDir,
    target_node_id: &sinomo_node::NodeId,
    shard_id: &DfsMetadataShardId,
) -> std::path::PathBuf {
    data_dir
        .dfs_dir()
        .join("metadata-replication-cursors")
        .join(path_safe_segment(&target_node_id.to_string()))
        .join(format!("{}.json", path_safe_segment(&shard_id.0)))
}

fn read_dfs_metadata_replication_cursor(
    path: &std::path::Path,
) -> Result<Option<DaemonDfsMetadataReplicationCursor>, DaemonError> {
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|source| DaemonError::Json {
                path: path.display().to_string(),
                detail: source.to_string(),
            }),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(DaemonError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        }),
    }
}

fn write_dfs_metadata_replication_cursor(
    path: &std::path::Path,
    cursor: &DaemonDfsMetadataReplicationCursor,
) -> Result<(), DaemonError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| DaemonError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let bytes = serde_json::to_vec_pretty(cursor).map_err(|source| DaemonError::Json {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    fs::write(path, bytes).map_err(|source| DaemonError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_dfs::DfsMetadataJournalBatchResult;
    use sinomo_node::NodeId;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn daemon_dfs_metadata_journal_batch_applies_to_local_backend() {
        let root = temp_root("daemon_dfs_metadata_journal_batch");
        let paths = DaemonPaths::with_root(root.join("data").join("daemon"));
        let source_root = root.join("source-meta");
        let node_id = NodeId::from_legacy_named("node_daemon_dfs_metadata").expect("node");
        let file_system = distributed_sifs(node_id.clone());
        let object = distributed_sifs_bootstrap_objects(node_id)
            .into_iter()
            .next()
            .expect("bootstrap object");
        let mut source =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("root"), source_root)
                .expect("source backend");
        source.put_file_system(&file_system).expect("filesystem");
        source.put_object(&object).expect("object");
        source
            .put_version(&DfsObjectVersionRecord {
                file_system_id: object.file_system_id.clone(),
                object_id: object.object_id.clone(),
                version: object.version.clone(),
                content_hash: object.content_hash.clone(),
                size_bytes: object.content_size_bytes,
                locations: object.current_locations.clone(),
                created_by_event_id: object.created_by_event_id.clone(),
                created_at: OffsetDateTime::now_utc(),
                visible: true,
            })
            .expect("version");
        let mut subscription = source.subscribe(0);
        let batch = source
            .poll_subscription(&mut subscription)
            .expect("source batch");

        let response = handle_daemon_dfs_metadata_journal_batch(
            &paths,
            DfsMetadataJournalBatchCommand {
                request_id: "dfs_meta_req_1".to_string(),
                batch,
            },
        )
        .expect("apply batch");
        assert_eq!(response.result, DfsMetadataJournalBatchResult::Applied);
        assert_eq!(response.applied_entries, 3);

        let target = LocalJsonMetadataBackend::new(
            DfsMetadataShardId::new("root"),
            root.join("data").join("dfs").join("metadata").join("root"),
        )
        .expect("target backend");
        assert_eq!(
            target.get_object(&object.object_id).expect("target object"),
            Some(object)
        );

        cleanup(&root);
    }

    fn temp_root(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("sinomo-daemon-dfs-{name}-{nanos}"))
    }

    fn cleanup(path: &std::path::Path) {
        let _ = fs::remove_dir_all(path);
    }
}
