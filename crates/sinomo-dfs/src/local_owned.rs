//! Local-owned DFS directory exposure.
//!
//! This module maps configured host directories into DFS virtual paths. The
//! owner node remains the only write authority; remote nodes can route requests
//! to the owner through the normal DFS control plane.

use crate::{SIFS_FILE_SYSTEM_ID, namespace::sifs_node_files_path};
use serde::{Deserialize, Serialize};
use sinomo_core::{DfsFileSystemId, NodeId};
use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};
use thiserror::Error;
use time::OffsetDateTime;

#[derive(Debug, Error)]
pub enum DfsExposeStoreError {
    #[error("io error at {path}: {source}")]
    Io { path: PathBuf, source: io::Error },
    #[error("config serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("invalid DFS exposure config: {0}")]
    InvalidConfig(String),
}

#[derive(Debug, Error)]
pub enum LocalOwnedDfsError {
    #[error("io error at {path}: {source}")]
    Io { path: PathBuf, source: io::Error },
    #[error("unknown DFS exposed path: {path}")]
    UnknownVirtualPath { path: String },
    #[error("DFS exposed path escapes its configured root: {path}")]
    PathEscapesRoot { path: String },
    #[error("DFS exposed path is read-only: {path}")]
    ReadOnly { path: String },
    #[error("refusing to follow symlink at {path}")]
    SymlinkDenied { path: PathBuf },
    #[error("invalid DFS exposed path: {0}")]
    InvalidVirtualPath(String),
    #[error("invalid DFS exposure config: {0}")]
    InvalidConfig(String),
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsExposeConfig {
    #[serde(default)]
    pub entries: Vec<DfsExposeEntry>,
}

impl DfsExposeConfig {
    pub fn empty() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub fn default_for_node(data_dir: &Path, node_id: NodeId) -> Self {
        let dfs_path = sifs_node_files_path(&node_id);
        Self {
            entries: vec![DfsExposeEntry {
                id: format!("local-files-{node_id}"),
                file_system_id: sifs_id(),
                dfs_path,
                source_path: data_dir.join("files"),
                owner_node_id: node_id,
                writable: true,
                follow_symlinks: false,
                created_at: OffsetDateTime::now_utc(),
            }],
        }
    }

    pub fn ensure_default_for_node(&mut self, data_dir: &Path, node_id: NodeId) -> bool {
        let default_path = sifs_node_files_path(&node_id);
        if self
            .entries
            .iter()
            .any(|entry| entry.file_system_id == sifs_id() && entry.dfs_path == default_path)
        {
            return false;
        }
        self.entries
            .extend(Self::default_for_node(data_dir, node_id).entries);
        true
    }

    pub fn validate(&self) -> Result<(), DfsExposeStoreError> {
        for entry in &self.entries {
            entry
                .validate()
                .map_err(DfsExposeStoreError::InvalidConfig)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsExposeEntry {
    pub id: String,
    pub file_system_id: DfsFileSystemId,
    pub dfs_path: String,
    pub source_path: PathBuf,
    pub owner_node_id: NodeId,
    #[serde(default = "default_true")]
    pub writable: bool,
    #[serde(default)]
    pub follow_symlinks: bool,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsExposeAddRequest {
    pub id: Option<String>,
    pub file_system_id: DfsFileSystemId,
    pub dfs_path: String,
    pub source_path: PathBuf,
    pub owner_node_id: NodeId,
    pub writable: bool,
    pub follow_symlinks: bool,
    pub created_at: OffsetDateTime,
}

impl DfsExposeEntry {
    pub fn validate(&self) -> Result<(), String> {
        if self.id.trim().is_empty() {
            return Err("exposure id cannot be empty".to_string());
        }
        validate_virtual_root(&self.dfs_path)?;
        if self.source_path.as_os_str().is_empty() {
            return Err("source path cannot be empty".to_string());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsExposeFileInfo {
    pub dfs_path: String,
    pub source_path: PathBuf,
    pub is_dir: bool,
    pub is_file: bool,
    pub len: u64,
    pub readonly: bool,
}

#[derive(Clone, Debug)]
pub struct DfsExposeStore {
    path: PathBuf,
}

impl DfsExposeStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn load(&self) -> Result<DfsExposeConfig, DfsExposeStoreError> {
        if !self.path.exists() {
            return Ok(DfsExposeConfig::empty());
        }
        let bytes = fs::read(&self.path).map_err(|source| DfsExposeStoreError::Io {
            path: self.path.clone(),
            source,
        })?;
        let config: DfsExposeConfig = serde_json::from_slice(&bytes)?;
        config.validate()?;
        Ok(config)
    }

    pub fn save(&self, config: &DfsExposeConfig) -> Result<(), DfsExposeStoreError> {
        config.validate()?;
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(|source| DfsExposeStoreError::Io {
                path: parent.to_owned(),
                source,
            })?;
        }
        for entry in &config.entries {
            fs::create_dir_all(&entry.source_path).map_err(|source| DfsExposeStoreError::Io {
                path: entry.source_path.clone(),
                source,
            })?;
        }
        let bytes = serde_json::to_vec_pretty(config)?;
        let tmp_path = self.path.with_extension("tmp");
        fs::write(&tmp_path, bytes).map_err(|source| DfsExposeStoreError::Io {
            path: tmp_path.clone(),
            source,
        })?;
        fs::rename(&tmp_path, &self.path).map_err(|source| DfsExposeStoreError::Io {
            path: self.path.clone(),
            source,
        })
    }

    pub fn ensure_default(
        &self,
        data_dir: &Path,
        node_id: NodeId,
    ) -> Result<DfsExposeConfig, DfsExposeStoreError> {
        let mut config = self.load()?;
        if config.ensure_default_for_node(data_dir, node_id) {
            self.save(&config)?;
        }
        Ok(config)
    }

    pub fn add_entry(
        &self,
        request: DfsExposeAddRequest,
    ) -> Result<DfsExposeConfig, DfsExposeStoreError> {
        let mut config = self.load()?;
        let id = request.id.unwrap_or_else(|| {
            format!(
                "expose-{}-{}",
                request.owner_node_id,
                request.dfs_path.trim_matches('/').replace('/', "-")
            )
        });
        config.entries.retain(|entry| entry.id != id);
        config.entries.push(DfsExposeEntry {
            id,
            file_system_id: request.file_system_id,
            dfs_path: request.dfs_path,
            source_path: request.source_path,
            owner_node_id: request.owner_node_id,
            writable: request.writable,
            follow_symlinks: request.follow_symlinks,
            created_at: request.created_at,
        });
        self.save(&config)?;
        Ok(config)
    }

    pub fn remove_entry(&self, id: &str) -> Result<(DfsExposeConfig, bool), DfsExposeStoreError> {
        let mut config = self.load()?;
        let before = config.entries.len();
        config.entries.retain(|entry| entry.id != id);
        let removed = before != config.entries.len();
        self.save(&config)?;
        Ok((config, removed))
    }
}

#[derive(Clone, Debug)]
pub struct DfsExposedDirectory {
    config: DfsExposeConfig,
}

impl DfsExposedDirectory {
    pub fn new(config: DfsExposeConfig) -> Result<Self, LocalOwnedDfsError> {
        config
            .entries
            .iter()
            .try_for_each(|entry| entry.validate().map_err(LocalOwnedDfsError::InvalidConfig))?;
        Ok(Self { config })
    }

    pub fn config(&self) -> &DfsExposeConfig {
        &self.config
    }

    pub fn list(&self, dfs_path: &str) -> Result<Vec<DfsExposeFileInfo>, LocalOwnedDfsError> {
        let resolved = self.resolve(dfs_path)?;
        reject_denied_symlink_path(&resolved)?;
        let metadata = fs::symlink_metadata(&resolved.source_path).map_err(|source| {
            LocalOwnedDfsError::Io {
                path: resolved.source_path.clone(),
                source,
            }
        })?;
        if !metadata.is_dir() {
            return Ok(vec![info_for_path(
                dfs_path.to_string(),
                resolved.source_path,
                metadata,
            )]);
        }

        let mut entries = Vec::new();
        let reader =
            fs::read_dir(&resolved.source_path).map_err(|source| LocalOwnedDfsError::Io {
                path: resolved.source_path.clone(),
                source,
            })?;
        for entry in reader {
            let entry = entry.map_err(|source| LocalOwnedDfsError::Io {
                path: resolved.source_path.clone(),
                source,
            })?;
            let source_path = entry.path();
            if !resolved.entry.follow_symlinks && is_symlink(&source_path)? {
                continue;
            }
            let metadata =
                fs::symlink_metadata(&source_path).map_err(|source| LocalOwnedDfsError::Io {
                    path: source_path.clone(),
                    source,
                })?;
            let name = entry.file_name().to_string_lossy().to_string();
            let child_dfs_path = join_virtual_path(&resolved.dfs_path, &name);
            entries.push(info_for_path(child_dfs_path, source_path, metadata));
        }
        entries.sort_by(|left, right| left.dfs_path.cmp(&right.dfs_path));
        Ok(entries)
    }

    pub fn read(&self, dfs_path: &str) -> Result<Vec<u8>, LocalOwnedDfsError> {
        let resolved = self.resolve(dfs_path)?;
        reject_denied_symlink_path(&resolved)?;
        fs::read(&resolved.source_path).map_err(|source| LocalOwnedDfsError::Io {
            path: resolved.source_path,
            source,
        })
    }

    pub fn write(&self, dfs_path: &str, bytes: &[u8]) -> Result<(), LocalOwnedDfsError> {
        let resolved = self.resolve(dfs_path)?;
        if !resolved.entry.writable {
            return Err(LocalOwnedDfsError::ReadOnly {
                path: dfs_path.to_string(),
            });
        }
        if !resolved.entry.follow_symlinks && path_contains_symlink(&resolved)? {
            return Err(LocalOwnedDfsError::SymlinkDenied {
                path: resolved.source_path,
            });
        }
        if let Some(parent) = resolved.source_path.parent() {
            fs::create_dir_all(parent).map_err(|source| LocalOwnedDfsError::Io {
                path: parent.to_owned(),
                source,
            })?;
        }
        let tmp_path = resolved.source_path.with_extension("sinomo-dfs-tmp");
        fs::write(&tmp_path, bytes).map_err(|source| LocalOwnedDfsError::Io {
            path: tmp_path.clone(),
            source,
        })?;
        fs::rename(&tmp_path, &resolved.source_path).map_err(|source| LocalOwnedDfsError::Io {
            path: resolved.source_path,
            source,
        })
    }

    fn resolve(&self, dfs_path: &str) -> Result<ResolvedExposePath<'_>, LocalOwnedDfsError> {
        let normalized = normalize_virtual_path(dfs_path)?;
        if normalized == "/" {
            return Err(LocalOwnedDfsError::InvalidVirtualPath(
                "DFS exposure path cannot be /".to_string(),
            ));
        }
        let Some(entry) = self
            .config
            .entries
            .iter()
            .filter(|entry| {
                normalized == entry.dfs_path || has_virtual_prefix(&normalized, &entry.dfs_path)
            })
            .max_by_key(|entry| entry.dfs_path.len())
        else {
            return Err(LocalOwnedDfsError::UnknownVirtualPath { path: normalized });
        };
        let relative = normalized
            .strip_prefix(&entry.dfs_path)
            .unwrap_or_default()
            .trim_start_matches('/');
        let mut source_path = entry.source_path.clone();
        for part in relative.split('/').filter(|part| !part.is_empty()) {
            source_path.push(part);
        }
        Ok(ResolvedExposePath {
            entry,
            dfs_path: normalized,
            source_path,
        })
    }
}

#[derive(Clone, Debug)]
struct ResolvedExposePath<'a> {
    entry: &'a DfsExposeEntry,
    dfs_path: String,
    source_path: PathBuf,
}

fn sifs_id() -> DfsFileSystemId {
    DfsFileSystemId::from_legacy_named(SIFS_FILE_SYSTEM_ID).expect("valid sifs id")
}

fn default_true() -> bool {
    true
}

fn validate_virtual_root(path: &str) -> Result<(), String> {
    let normalized = normalize_virtual_path(path).map_err(|error| error.to_string())?;
    if normalized == "/" {
        return Err("DFS exposure path cannot be /".to_string());
    }
    Ok(())
}

fn normalize_virtual_path(path: &str) -> Result<String, LocalOwnedDfsError> {
    if !path.starts_with('/') {
        return Err(LocalOwnedDfsError::InvalidVirtualPath(
            "path must start with /".to_string(),
        ));
    }
    let mut parts = Vec::new();
    for component in Path::new(path).components() {
        match component {
            Component::RootDir => {}
            Component::Normal(value) => {
                let part = value.to_string_lossy();
                if part.is_empty() {
                    continue;
                }
                parts.push(part.to_string());
            }
            Component::CurDir => {}
            Component::ParentDir => {
                return Err(LocalOwnedDfsError::PathEscapesRoot {
                    path: path.to_string(),
                });
            }
            Component::Prefix(_) => {
                return Err(LocalOwnedDfsError::InvalidVirtualPath(
                    "windows prefixes are not valid DFS paths".to_string(),
                ));
            }
        }
    }
    Ok(format!("/{}", parts.join("/")))
}

fn has_virtual_prefix(path: &str, prefix: &str) -> bool {
    path.strip_prefix(prefix)
        .is_some_and(|suffix| suffix.starts_with('/'))
}

fn join_virtual_path(parent: &str, child: &str) -> String {
    if parent == "/" {
        format!("/{child}")
    } else {
        format!("{}/{child}", parent.trim_end_matches('/'))
    }
}

fn is_symlink(path: &Path) -> Result<bool, LocalOwnedDfsError> {
    fs::symlink_metadata(path)
        .map(|metadata| metadata.file_type().is_symlink())
        .map_err(|source| LocalOwnedDfsError::Io {
            path: path.to_owned(),
            source,
        })
}

fn reject_denied_symlink_path(resolved: &ResolvedExposePath<'_>) -> Result<(), LocalOwnedDfsError> {
    if !resolved.entry.follow_symlinks && path_contains_symlink(resolved)? {
        return Err(LocalOwnedDfsError::SymlinkDenied {
            path: resolved.source_path.clone(),
        });
    }
    Ok(())
}

fn path_contains_symlink(resolved: &ResolvedExposePath<'_>) -> Result<bool, LocalOwnedDfsError> {
    let root = &resolved.entry.source_path;
    if root.exists() && is_symlink(root)? {
        return Ok(true);
    }
    let relative = resolved.source_path.strip_prefix(root).map_err(|_| {
        LocalOwnedDfsError::PathEscapesRoot {
            path: resolved.dfs_path.clone(),
        }
    })?;
    let mut current = root.clone();
    for component in relative.components() {
        let Component::Normal(part) = component else {
            return Ok(true);
        };
        current.push(part);
        if current.exists() && is_symlink(&current)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn info_for_path(
    dfs_path: String,
    source_path: PathBuf,
    metadata: fs::Metadata,
) -> DfsExposeFileInfo {
    DfsExposeFileInfo {
        dfs_path,
        source_path,
        is_dir: metadata.is_dir(),
        is_file: metadata.is_file(),
        len: metadata.len(),
        readonly: metadata.permissions().readonly(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn default_config_exposes_data_files_under_node_sifs_path() {
        let node_id = NodeId::from_legacy_named("node_alpha").expect("node id");
        let config = DfsExposeConfig::default_for_node(Path::new("/tmp/sinomo"), node_id);

        assert_eq!(config.entries.len(), 1);
        assert_eq!(
            config.entries[0].file_system_id,
            crate::sifs_file_system_id()
        );
        assert_eq!(
            config.entries[0].dfs_path,
            format!("/sinomo/nodes/{}/files", config.entries[0].owner_node_id)
        );
        assert_eq!(
            config.entries[0].source_path,
            PathBuf::from("/tmp/sinomo/files")
        );
    }

    #[test]
    fn exposed_directory_rejects_parent_escape() {
        let service = DfsExposedDirectory::new(test_config()).expect("service");

        let error = service
            .read("/sinomo/nodes/node_alpha/files/../secret")
            .unwrap_err();

        assert!(matches!(error, LocalOwnedDfsError::PathEscapesRoot { .. }));
    }

    #[test]
    fn exposed_directory_reads_and_writes_files_atomically() {
        let root = temp_root("read_write");
        fs::create_dir_all(root.join("files")).expect("create root");
        let service = DfsExposedDirectory::new(config_for_root(&root)).expect("service");

        service
            .write("/sinomo/nodes/node_alpha/files/a/b.txt", b"hello")
            .expect("write");
        let bytes = service
            .read("/sinomo/nodes/node_alpha/files/a/b.txt")
            .expect("read");

        assert_eq!(bytes, b"hello");
    }

    #[test]
    fn expose_store_adds_replaces_and_removes_entries() {
        let root = temp_root("store_mutations");
        let store = DfsExposeStore::new(root.join("expose.json"));
        let node_id = NodeId::from_legacy_named("node_alpha").expect("node id");

        let config = store
            .add_entry(DfsExposeAddRequest {
                id: Some("project".to_string()),
                file_system_id: sifs_id(),
                dfs_path: "/sinomo/nodes/node_alpha/files/project".to_string(),
                source_path: root.join("project"),
                owner_node_id: node_id.clone(),
                writable: true,
                follow_symlinks: false,
                created_at: OffsetDateTime::UNIX_EPOCH,
            })
            .expect("add");
        assert_eq!(config.entries.len(), 1);

        let config = store
            .add_entry(DfsExposeAddRequest {
                id: Some("project".to_string()),
                file_system_id: sifs_id(),
                dfs_path: "/sinomo/nodes/node_alpha/files/project2".to_string(),
                source_path: root.join("project2"),
                owner_node_id: node_id,
                writable: false,
                follow_symlinks: true,
                created_at: OffsetDateTime::UNIX_EPOCH,
            })
            .expect("replace");
        assert_eq!(config.entries.len(), 1);
        assert_eq!(
            config.entries[0].dfs_path,
            "/sinomo/nodes/node_alpha/files/project2"
        );

        let (config, removed) = store.remove_entry("project").expect("remove");
        assert!(removed);
        assert!(config.entries.is_empty());
        let _ = fs::remove_dir_all(root);
    }

    fn test_config() -> DfsExposeConfig {
        config_for_root(&temp_root("escape"))
    }

    fn config_for_root(root: &Path) -> DfsExposeConfig {
        DfsExposeConfig {
            entries: vec![DfsExposeEntry {
                id: "local-files-node_alpha".to_string(),
                file_system_id: sifs_id(),
                dfs_path: "/sinomo/nodes/node_alpha/files".to_string(),
                source_path: root.join("files"),
                owner_node_id: NodeId::from_legacy_named("node_alpha").expect("node id"),
                writable: true,
                follow_symlinks: false,
                created_at: OffsetDateTime::now_utc(),
            }],
        }
    }

    fn temp_root(name: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        std::env::temp_dir().join(format!("sinomo-dfs-{name}-{stamp}"))
    }
}
