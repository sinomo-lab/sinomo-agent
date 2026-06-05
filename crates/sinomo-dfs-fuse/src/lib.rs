//! Optional FUSE mount adapter for SIFS.
//!
//! The crate is intentionally cheap by default. Real FUSE support is compiled
//! only on Unix targets with the `fuse` feature.

use std::path::PathBuf;
use std::time::Duration;
use thiserror::Error;

#[cfg(all(unix, feature = "fuse"))]
mod fuse;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsFuseMountOptions {
    pub data_dir: Option<PathBuf>,
    pub mount_point: PathBuf,
    pub root_path: String,
    pub read_only: bool,
    pub foreground: bool,
    pub allow_other: bool,
    pub mount_name: String,
    pub attr_ttl: Duration,
    pub entry_ttl: Duration,
}

impl DfsFuseMountOptions {
    pub fn new(mount_point: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: None,
            mount_point: mount_point.into(),
            root_path: "/".to_string(),
            read_only: false,
            foreground: true,
            allow_other: false,
            mount_name: "sinomo-sifs".to_string(),
            attr_ttl: Duration::from_secs(1),
            entry_ttl: Duration::from_secs(1),
        }
    }

    pub fn with_root_path(mut self, root_path: impl Into<String>) -> Self {
        self.root_path = root_path.into();
        self
    }

    pub fn with_data_dir(mut self, data_dir: impl Into<PathBuf>) -> Self {
        self.data_dir = Some(data_dir.into());
        self
    }
}

#[derive(Debug, Error)]
pub enum DfsFuseError {
    #[error(
        "SIFS FUSE support is not available on this platform or in this build; use a Unix build with FUSE enabled"
    )]
    Unsupported,
    #[error("invalid SIFS path `{path}`: {detail}")]
    InvalidPath { path: String, detail: String },
    #[error("local data directory error: {0}")]
    LocalData(String),
    #[error("DFS error: {0}")]
    Dfs(String),
    #[error("network error: {0}")]
    Network(String),
    #[error("daemon error: {0}")]
    Daemon(String),
    #[error("cluster KV error: {0}")]
    Kv(String),
    #[error("FUSE mount error at {mount_point}: {detail}")]
    Mount { mount_point: String, detail: String },
}

pub type Result<T> = std::result::Result<T, DfsFuseError>;

pub fn mount_sifs(options: DfsFuseMountOptions) -> Result<()> {
    #[cfg(all(unix, feature = "fuse"))]
    {
        fuse::mount_sifs(options)
    }
    #[cfg(not(all(unix, feature = "fuse")))]
    {
        let _ = options;
        Err(DfsFuseError::Unsupported)
    }
}

#[cfg(any(test, all(unix, feature = "fuse")))]
fn normalize_sifs_root(path: &str) -> Result<String> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return Err(DfsFuseError::InvalidPath {
            path: path.to_string(),
            detail: "path cannot be empty".to_string(),
        });
    }
    if !trimmed.starts_with('/') {
        return Err(DfsFuseError::InvalidPath {
            path: path.to_string(),
            detail: "path must be absolute".to_string(),
        });
    }
    let mut parts = Vec::new();
    for component in trimmed.split('/') {
        match component {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            part => parts.push(part),
        }
    }
    if parts.is_empty() {
        Ok("/".to_string())
    } else {
        Ok(format!("/{}", parts.join("/")))
    }
}

#[cfg(any(test, all(unix, feature = "fuse")))]
fn join_sifs_child(parent: &str, name: &std::ffi::OsStr) -> Result<String> {
    let Some(name) = name.to_str() else {
        return Err(DfsFuseError::InvalidPath {
            path: parent.to_string(),
            detail: "path segment is not UTF-8".to_string(),
        });
    };
    if name.is_empty() || name == "." || name == ".." || name.contains('/') {
        return Err(DfsFuseError::InvalidPath {
            path: format!("{parent}/{name}"),
            detail: "invalid path segment".to_string(),
        });
    }
    if parent == "/" {
        Ok(format!("/{name}"))
    } else {
        Ok(format!("{}/{name}", parent.trim_end_matches('/')))
    }
}

#[cfg(any(test, all(unix, feature = "fuse")))]
fn parent_sifs_path(path: &str) -> String {
    let normalized = normalize_sifs_root(path).unwrap_or_else(|_| "/".to_string());
    if normalized == "/" {
        return "/".to_string();
    }
    normalized
        .rsplit_once('/')
        .and_then(|(parent, _)| (!parent.is_empty()).then_some(parent.to_string()))
        .unwrap_or_else(|| "/".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsStr;

    #[test]
    fn normalizes_absolute_sifs_roots() {
        assert_eq!(normalize_sifs_root("/").expect("root"), "/");
        assert_eq!(
            normalize_sifs_root("/sinomo//nodes/./node/../node/files/").expect("normalized"),
            "/sinomo/nodes/node/files"
        );
    }

    #[test]
    fn joins_child_segments_without_escaping_parent() {
        assert_eq!(
            join_sifs_child("/", OsStr::new("users")).expect("child"),
            "/users"
        );
        assert_eq!(
            join_sifs_child("/sinomo", OsStr::new("nodes")).expect("child"),
            "/sinomo/nodes"
        );
        assert!(join_sifs_child("/sinomo", OsStr::new("../x")).is_err());
    }

    #[test]
    fn computes_parent_paths() {
        assert_eq!(parent_sifs_path("/"), "/");
        assert_eq!(parent_sifs_path("/sinomo"), "/");
        assert_eq!(parent_sifs_path("/sinomo/nodes"), "/sinomo");
    }
}
