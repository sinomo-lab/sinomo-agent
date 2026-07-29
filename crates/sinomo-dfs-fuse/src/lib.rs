//! Optional FUSE mount adapter for SIFS.
//!
//! The crate is intentionally cheap by default. Real FUSE support is compiled
//! only on Unix targets with the `fuse` feature.

use std::fmt;
use std::path::{Path, PathBuf};
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
    #[error("FUSE mount error at {mount_point}: {detail}")]
    Mount { mount_point: String, detail: String },
}

pub type Result<T> = std::result::Result<T, DfsFuseError>;

/// Owns a nonblocking SIFS FUSE mount.
///
/// The resolved mount root remains valid for as long as this handle is kept
/// alive. Dropping the handle closes the background FUSE session and unmounts
/// the filesystem. Owner-only mounts intentionally avoid FUSE `auto_unmount`,
/// because fuser implements that option by implicitly requesting
/// `allow_other`; the owning session still guarantees orderly unmounting.
#[must_use = "dropping the SIFS mount handle immediately unmounts the filesystem"]
pub struct DfsFuseMountHandle {
    mount_root: PathBuf,
    #[cfg(all(unix, feature = "fuse"))]
    session: Option<fuser::BackgroundSession>,
}

impl DfsFuseMountHandle {
    /// Absolute, canonical host path at which the selected SIFS root is
    /// mounted.
    pub fn mount_root(&self) -> &Path {
        &self.mount_root
    }

    /// Whether the background FUSE worker has not exited yet.
    pub fn is_active(&self) -> bool {
        #[cfg(all(unix, feature = "fuse"))]
        {
            self.session
                .as_ref()
                .is_some_and(|session| !session.guard.is_finished())
        }
        #[cfg(not(all(unix, feature = "fuse")))]
        {
            false
        }
    }

    #[cfg(all(unix, feature = "fuse"))]
    fn new(mount_root: PathBuf, session: fuser::BackgroundSession) -> Self {
        Self {
            mount_root,
            session: Some(session),
        }
    }
}

impl fmt::Debug for DfsFuseMountHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DfsFuseMountHandle")
            .field("mount_root", &self.mount_root)
            .field("active", &self.is_active())
            .finish_non_exhaustive()
    }
}

impl Drop for DfsFuseMountHandle {
    fn drop(&mut self) {
        #[cfg(all(unix, feature = "fuse"))]
        {
            // fuser's BackgroundSession owns the mount object. Dropping it
            // first initiates unmount while the rest of this handle is still
            // intact.
            drop(self.session.take());
        }
    }
}

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

/// Mount SIFS on a background worker and return an owning session handle.
///
/// Unlike [`mount_sifs`], this returns after the kernel mount is established.
/// Missing mount-point directories are created, and the returned handle
/// exposes the canonical mount root suitable for use as a host-side sandbox
/// filesystem boundary. Keep the handle alive for as long as paths below that
/// root may be used. This lifecycle guarantee requires an executable
/// `fusermount3` or `fusermount` helper.
pub fn mount_sifs_background(options: DfsFuseMountOptions) -> Result<DfsFuseMountHandle> {
    #[cfg(all(unix, feature = "fuse"))]
    {
        fuse::mount_sifs_background(options)
    }
    #[cfg(not(all(unix, feature = "fuse")))]
    {
        let _ = options;
        Err(DfsFuseError::Unsupported)
    }
}

#[cfg(any(test, all(unix, feature = "fuse")))]
fn prepare_background_mount_point(path: &Path) -> Result<PathBuf> {
    if path.as_os_str().is_empty() {
        return Err(DfsFuseError::Mount {
            mount_point: path.display().to_string(),
            detail: "mount point cannot be empty".to_string(),
        });
    }
    std::fs::create_dir_all(path).map_err(|source| DfsFuseError::Mount {
        mount_point: path.display().to_string(),
        detail: format!("failed to create mount point: {source}"),
    })?;
    let metadata = std::fs::metadata(path).map_err(|source| DfsFuseError::Mount {
        mount_point: path.display().to_string(),
        detail: format!("failed to inspect mount point: {source}"),
    })?;
    if !metadata.is_dir() {
        return Err(DfsFuseError::Mount {
            mount_point: path.display().to_string(),
            detail: "mount point is not a directory".to_string(),
        });
    }
    std::fs::canonicalize(path).map_err(|source| DfsFuseError::Mount {
        mount_point: path.display().to_string(),
        detail: format!("failed to resolve mount point: {source}"),
    })
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

    #[test]
    fn background_mount_point_is_created_and_resolved() {
        let root = sinomo_testdir::TestDir::new("sinomo-fuse-background-mount-point");
        let mount_point = root.join("nested").join("sifs");

        let resolved = prepare_background_mount_point(&mount_point).expect("mount point");

        assert!(mount_point.is_dir());
        assert!(resolved.is_absolute());
        assert_eq!(resolved, std::fs::canonicalize(&mount_point).unwrap());
    }

    #[test]
    fn background_mount_point_rejects_files_and_empty_paths() {
        let root = sinomo_testdir::TestDir::new("sinomo-fuse-background-mount-file");
        let file = root.join("not-a-directory");
        std::fs::write(&file, b"fixture").expect("mount point fixture");

        assert!(prepare_background_mount_point(&file).is_err());
        assert!(prepare_background_mount_point(Path::new("")).is_err());
    }

    #[cfg(all(unix, feature = "fuse"))]
    #[test]
    #[ignore = "requires an executable FUSE helper and a usable /dev/fuse"]
    fn dead_fuse_worker_is_unmounted_and_removed() {
        let root = sinomo_testdir::TestDir::new("sinomo-fuse-dead-worker-cleanup");
        let mount_point = root.join("mount");
        let data_dir = root.join("data");
        let mut handle =
            mount_sifs_background(DfsFuseMountOptions::new(&mount_point).with_data_dir(data_dir))
                .expect("background SIFS mount");
        assert!(handle.is_active(), "fixture must start a live FUSE worker");

        // Model the worker exiting before its owning application handle. The
        // BackgroundSession drop is the same lifecycle edge reached when the
        // worker terminates or the owner is torn down.
        drop(handle.session.take());
        assert!(!handle.is_active());
        drop(handle);

        std::fs::remove_dir(&mount_point)
            .expect("an exited worker must leave an unmounted, removable mount point");
        assert!(!mount_point.exists());
    }
}
