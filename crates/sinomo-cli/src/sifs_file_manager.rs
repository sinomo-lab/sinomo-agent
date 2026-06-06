use crate::file_manager::{
    FileManagerBackend, FileManagerEntry, FileManagerEntryKind, FileManagerMetadata,
    FileManagerRead, validated_relative_child_path,
};
use sinomo_runtime::daemon::{DaemonDfsEndpointTransport, DaemonPaths};
use sinomo_runtime::dfs::{
    DfsMetadataShardConfig, DfsObjectKind, DfsWriteOptions, LocalContentStore, NetworkedDfsService,
    default_dfs_metadata_root, default_dfs_metadata_shards_root, fallback_dfs_node_id,
    metadata_shards_prefix, open_local_sharded_metadata_backend, open_root_local_metadata_backend,
    sifs_file_system_id,
};
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::network::{self, ClusterPaths};
use std::sync::Mutex;

#[derive(Debug)]
pub(crate) struct SifsFileManagerBackend {
    data_dir: SinomoDataDir,
    service: Mutex<NetworkedDfsService<DaemonDfsEndpointTransport>>,
}

impl SifsFileManagerBackend {
    pub(crate) fn open(data_dir: &SinomoDataDir) -> Result<Self, String> {
        let service = open_sifs_service(data_dir)?;
        Ok(Self {
            data_dir: data_dir.clone(),
            service: Mutex::new(service),
        })
    }

    fn lock_service(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, NetworkedDfsService<DaemonDfsEndpointTransport>>, String>
    {
        self.service
            .lock()
            .map_err(|_| "SIFS service lock is poisoned".to_string())
    }

    fn publish_metadata_after_mutation(&self) {
        let _ = sinomo_runtime::daemon::publish_default_dfs_metadata_journal(&DaemonPaths::new(
            &self.data_dir,
        ));
    }

    /// The node id objects written here are placed on (the local SIFS node).
    pub(crate) fn node_label(&self) -> String {
        self.lock_service()
            .map(|service| service.node_id().to_string())
            .unwrap_or_default()
    }
}

impl FileManagerBackend for SifsFileManagerBackend {
    fn source_label(&self) -> &str {
        "SIFS"
    }

    fn normalize_path(&self, path: &str) -> Result<String, String> {
        normalize_sifs_path(path)
    }

    fn parent_path(&self, path: &str) -> Option<String> {
        let path = normalize_sifs_path(path).ok()?;
        if path == "/" {
            return None;
        }
        let (parent, _) = path.rsplit_once('/')?;
        Some(if parent.is_empty() {
            "/".to_string()
        } else {
            parent.to_string()
        })
    }

    fn child_path(&self, current_dir: &str, name: &str) -> Result<String, String> {
        let child = validated_relative_child_path(name)?;
        let mut current = normalize_sifs_path(current_dir)?;
        for component in child.components() {
            let std::path::Component::Normal(name) = component else {
                continue;
            };
            let name = name.to_string_lossy();
            if current == "/" {
                current.push_str(&name);
            } else {
                current.push('/');
                current.push_str(&name);
            }
        }
        normalize_sifs_path(&current)
    }

    fn display_name(&self, path: &str) -> String {
        let path = normalize_sifs_path(path).unwrap_or_else(|_| path.to_string());
        if path == "/" {
            return "/".to_string();
        }
        path.rsplit('/')
            .next()
            .filter(|name| !name.is_empty())
            .unwrap_or(path.as_str())
            .to_string()
    }

    fn stat(&self, path: &str) -> Result<FileManagerMetadata, String> {
        let path = normalize_sifs_path(path)?;
        let mut service = self.lock_service()?;
        let stat = service
            .posix_stat_distributed(&path)
            .map_err(|error| format!("cannot inspect {path}: {error}"))?;
        Ok(FileManagerMetadata {
            kind: sifs_entry_kind(&stat.kind),
            size: stat.size_bytes,
        })
    }

    fn list_dir(&self, path: &str) -> Result<Vec<FileManagerEntry>, String> {
        let path = normalize_sifs_path(path)?;
        let mut service = self.lock_service()?;
        let entries = service
            .posix_list_dir_distributed(&path)
            .map_err(|error| format!("cannot list {path}: {error}"))?;
        let mut results = Vec::new();
        for entry in entries {
            let stat = service.posix_stat_distributed(&entry.path).ok();
            let kind = stat
                .as_ref()
                .map(|stat| sifs_entry_kind(&stat.kind))
                .unwrap_or_else(|| sifs_entry_kind(&entry.kind));
            let size = stat.and_then(|stat| stat.size_bytes);
            results.push(FileManagerEntry {
                name: entry.name,
                path: entry.path,
                kind,
                size,
            });
        }
        Ok(results)
    }

    fn read_file_prefix(&self, path: &str, max_bytes: u64) -> Result<FileManagerRead, String> {
        let path = normalize_sifs_path(path)?;
        let mut service = self.lock_service()?;
        let stat = service
            .posix_stat_distributed(&path)
            .map_err(|error| format!("cannot inspect {path}: {error}"))?;
        let truncated = stat
            .size_bytes
            .map(|size| size > max_bytes)
            .unwrap_or(false);
        let bytes = if truncated {
            service
                .posix_read_file_range(&path, 0, max_bytes as usize)
                .map_err(|error| format!("cannot read {path}: {error}"))?
        } else {
            service
                .posix_read_file_distributed(&path)
                .map_err(|error| format!("cannot read {path}: {error}"))?
        };
        Ok(FileManagerRead { bytes, truncated })
    }

    fn directory_entry_count(&self, path: &str) -> Result<usize, String> {
        let path = normalize_sifs_path(path)?;
        let mut service = self.lock_service()?;
        service
            .posix_list_dir_distributed(&path)
            .map(|entries| entries.len())
            .map_err(|error| error.to_string())
    }

    fn create_file(&self, path: &str) -> Result<(), String> {
        let path = normalize_sifs_path(path)?;
        let mut service = self.lock_service()?;
        let options = sifs_write_options(&service);
        service
            .posix_write_file(&path, &[], options)
            .map(|_| ())
            .map_err(|error| format!("cannot create SIFS file {path}: {error}"))?;
        drop(service);
        self.publish_metadata_after_mutation();
        Ok(())
    }

    fn create_dir(&self, path: &str) -> Result<(), String> {
        let path = normalize_sifs_path(path)?;
        let mut service = self.lock_service()?;
        service
            .posix_mkdir(&path)
            .map(|_| ())
            .map_err(|error| format!("cannot create SIFS directory {path}: {error}"))?;
        drop(service);
        self.publish_metadata_after_mutation();
        Ok(())
    }

    fn rename(&self, from: &str, to: &str) -> Result<(), String> {
        let from = normalize_sifs_path(from)?;
        let to = normalize_sifs_path(to)?;
        let mut service = self.lock_service()?;
        service
            .posix_rename(&from, &to)
            .map(|_| ())
            .map_err(|error| format!("cannot rename SIFS item {from} to {to}: {error}"))?;
        drop(service);
        self.publish_metadata_after_mutation();
        Ok(())
    }

    fn delete_file(&self, path: &str) -> Result<(), String> {
        let path = normalize_sifs_path(path)?;
        let mut service = self.lock_service()?;
        service
            .posix_unlink(&path)
            .map(|_| ())
            .map_err(|error| format!("cannot delete SIFS file {path}: {error}"))?;
        drop(service);
        self.publish_metadata_after_mutation();
        Ok(())
    }

    fn delete_dir(&self, path: &str) -> Result<(), String> {
        let path = normalize_sifs_path(path)?;
        let mut service = self.lock_service()?;
        service
            .posix_rmdir(&path)
            .map(|_| ())
            .map_err(|error| {
                format!(
                    "cannot delete SIFS directory {path}: {error}; only empty directories can be deleted"
                )
            })?;
        drop(service);
        self.publish_metadata_after_mutation();
        Ok(())
    }
}

fn open_sifs_service(
    data_dir: &SinomoDataDir,
) -> Result<NetworkedDfsService<DaemonDfsEndpointTransport>, String> {
    data_dir
        .ensure_root_layout()
        .map_err(|error| format!("cannot prepare Sinomo data directory: {error}"))?;
    let cluster_paths = ClusterPaths::new(data_dir);
    let (local_node_id, endpoints) = if cluster_paths.state.exists() {
        let state = network::read_cluster_state_without_kv(&cluster_paths)
            .map_err(|error| format!("cannot read local cluster state: {error}"))?;
        let endpoints = network::cluster_dfs_protocol_targets(&cluster_paths, &state);
        (state.local_node_id, endpoints)
    } else {
        (fallback_dfs_node_id(), Vec::new())
    };
    let content = LocalContentStore::new(data_dir.dfs_dir().join("content"))
        .map_err(|error| format!("cannot open DFS content store: {error}"))?;
    let service = NetworkedDfsService::new(
        DaemonDfsEndpointTransport::new(local_node_id, endpoints),
        content,
    );
    let service =
        if let Some(metadata) = open_sifs_sharded_metadata_backend(data_dir, &cluster_paths)? {
            service
                .with_metadata_backend(metadata)
                .map_err(|error| format!("cannot open sharded SIFS metadata: {error}"))?
        } else {
            let metadata = open_root_local_metadata_backend(default_dfs_metadata_root(data_dir))
                .map_err(|error| format!("cannot open local SIFS metadata: {error}"))?;
            service
                .with_metadata_backend(metadata)
                .map_err(|error| format!("cannot open local SIFS metadata: {error}"))?
        };
    let mut service = service;
    service
        .bootstrap_distributed_sifs()
        .map_err(|error| format!("cannot bootstrap SIFS namespace: {error}"))?;
    Ok(service)
}

fn open_sifs_sharded_metadata_backend(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
) -> Result<Option<sinomo_runtime::dfs::LocalShardedMetadataBackend>, String> {
    if !cluster_paths.state.exists() {
        return Ok(None);
    }
    let file_system_id = sifs_file_system_id();
    let entries = network::read_cluster_kv_prefix_entries(
        cluster_paths,
        &format!("{}/", metadata_shards_prefix(&file_system_id)),
    )
    .map_err(|error| format!("cannot read DFS metadata shard config: {error}"))?;
    let mut shards = Vec::new();
    for entry in entries {
        shards.push(
            entry
                .decode::<DfsMetadataShardConfig>()
                .map_err(|error| format!("cannot decode DFS metadata shard config: {error}"))?,
        );
    }
    shards.sort_by(|left, right| left.shard_id.cmp(&right.shard_id));
    if shards.is_empty() {
        return Ok(None);
    }
    open_local_sharded_metadata_backend(default_dfs_metadata_shards_root(data_dir), shards)
        .map(Some)
        .map_err(|error| format!("cannot open DFS metadata shards: {error}"))
}

fn sifs_write_options(
    service: &NetworkedDfsService<DaemonDfsEndpointTransport>,
) -> DfsWriteOptions {
    DfsWriteOptions::replicated_eventual(vec![service.node_id().clone()])
}

fn sifs_entry_kind(kind: &DfsObjectKind) -> FileManagerEntryKind {
    match kind {
        DfsObjectKind::Directory => FileManagerEntryKind::Directory,
        DfsObjectKind::File | DfsObjectKind::Artifact => FileManagerEntryKind::File,
        DfsObjectKind::Symlink => FileManagerEntryKind::Symlink,
        DfsObjectKind::Hardlink | DfsObjectKind::Mount | DfsObjectKind::Tombstone => {
            FileManagerEntryKind::Other
        }
    }
}

fn normalize_sifs_path(path: &str) -> Result<String, String> {
    let path = path.trim();
    let path = if path.is_empty() {
        "/".to_string()
    } else if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    };
    if path.bytes().any(|byte| byte == 0) {
        return Err(format!("invalid SIFS path: {path}"));
    }
    let mut parts = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            value => parts.push(value),
        }
    }
    if parts.is_empty() {
        Ok("/".to_string())
    } else {
        Ok(format!("/{}", parts.join("/")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_manager::FileManagerState;
    use std::sync::Arc;

    #[test]
    fn normalize_sifs_paths() {
        assert_eq!(normalize_sifs_path("").expect("root"), "/");
        assert_eq!(
            normalize_sifs_path("/services/../users").expect("path"),
            "/users"
        );
        assert_eq!(
            normalize_sifs_path("sinomo/nodes").expect("relative"),
            "/sinomo/nodes"
        );
    }

    #[test]
    fn sifs_backend_manages_local_sifs_without_cluster() {
        let root = temp_sifs_data_dir("local");
        let data_dir = SinomoDataDir::new(&root);
        let backend = SifsFileManagerBackend::open(&data_dir).expect("backend");
        let mut manager = FileManagerState::open(Arc::new(backend), "/services").expect("manager");

        manager.create_dir("tui-test").expect("create dir");
        assert!(manager.entries.iter().any(|entry| entry.name == "tui-test"));
        manager.open_selected().expect("open dir");
        manager.create_file("alpha.txt").expect("create file");
        manager.rename_selected("beta.txt").expect("rename file");
        assert!(manager.entries.iter().any(|entry| entry.name == "beta.txt"));
        manager.delete_selected("DELETE").expect("delete file");
        manager.go_parent().expect("parent");
        manager.delete_selected("DELETE").expect("delete dir");

        let _ = std::fs::remove_dir_all(root);
    }

    fn temp_sifs_data_dir(name: &str) -> std::path::PathBuf {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "sinomo-tui-sifs-file-manager-{name}-{}-{unique}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        root
    }
}
