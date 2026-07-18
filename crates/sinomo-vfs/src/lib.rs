//! Root-scoped virtual filesystem facade.
//!
//! The VFS layer gives high-level crates a storage-oriented API without
//! binding them to the current backing store. Handles are always scoped to a
//! root path, so callers can operate with local names while the backend remains
//! local, remote, service-backed, or an in-memory test store.

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sinomo_core::object::{LengthMode, serde_ext};
use std::collections::BTreeMap;
use std::fs;
use std::io::{self, Read, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use thiserror::Error;

pub type Result<T> = std::result::Result<T, VfsError>;

#[derive(Debug, Error)]
pub enum VfsError {
    #[error("invalid VFS path {path}: {detail}")]
    InvalidPath { path: String, detail: String },
    #[error("VFS path {path} is outside scoped root {root}")]
    OutsideScope { root: String, path: String },
    #[error("invalid VFS directory cursor: {detail}")]
    InvalidCursor { detail: String },
    #[error("VFS path not found: {path}")]
    NotFound { path: String },
    #[error("VFS path already exists: {path}")]
    AlreadyExists { path: String },
    #[error("VFS path is not a directory: {path}")]
    NotDirectory { path: String },
    #[error("VFS path is a directory: {path}")]
    IsDirectory { path: String },
    #[error("VFS directory is not empty: {path}")]
    DirectoryNotEmpty { path: String },
    #[error("VFS write conflicted at {path}: {detail}")]
    Conflict { path: String, detail: String },
    #[error("VFS operation is unsupported by {backend}: {detail}")]
    Unsupported { backend: String, detail: String },
    #[error("VFS I/O failed at {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: io::Error,
    },
    #[error("VFS JSON codec failed at {path}: {detail}")]
    Json { path: String, detail: String },
    #[error("VFS SOBJ codec failed at {path}: {detail}")]
    Sobj { path: String, detail: String },
    #[error("VFS backend {backend} failed at {path}: {detail}")]
    Backend {
        backend: String,
        path: String,
        detail: String,
    },
}

impl VfsError {
    pub fn path(&self) -> Option<&str> {
        match self {
            Self::InvalidPath { path, .. }
            | Self::OutsideScope { path, .. }
            | Self::NotFound { path }
            | Self::AlreadyExists { path }
            | Self::NotDirectory { path }
            | Self::IsDirectory { path }
            | Self::DirectoryNotEmpty { path }
            | Self::Conflict { path, .. }
            | Self::Io { path, .. }
            | Self::Json { path, .. }
            | Self::Sobj { path, .. }
            | Self::Backend { path, .. } => Some(path),
            Self::InvalidCursor { .. } | Self::Unsupported { .. } => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VfsObjectKind {
    File,
    Directory,
    Symlink,
    Hardlink,
    Artifact,
    Mount,
    Tombstone,
    Other,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VfsMetadata {
    pub path: String,
    pub kind: VfsObjectKind,
    /// Stable backend object identity when the filesystem exposes one.
    pub object_id: Option<String>,
    pub size_bytes: Option<u64>,
    pub content_hash: Option<String>,
    pub version: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VfsDirEntry {
    pub name: String,
    pub path: String,
    pub kind: VfsObjectKind,
    pub size_bytes: Option<u64>,
}

/// Opaque continuation for one backend and one resolved directory.
///
/// Callers may persist the encoded form, but only [`VfsHandle::list_page`]
/// validates and unwraps it. A cursor cannot be reused for another backend or
/// directory, including a path outside a scoped handle's root.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct VfsDirectoryCursor {
    schema_version: u8,
    backend: String,
    directory: String,
    opaque: Vec<u8>,
}

impl VfsDirectoryCursor {
    const SCHEMA_VERSION: u8 = 1;
    const ENCODED_PREFIX: &'static str = "vfs-dir-v1.";
    const MAX_ENCODED_BYTES: usize = 128 * 1024;
    const MAX_BACKEND_BYTES: usize = 16 * 1024;

    pub fn to_opaque_string(&self) -> Result<String> {
        let bytes = serde_json::to_vec(self).map_err(|source| VfsError::InvalidCursor {
            detail: format!("cannot encode cursor: {source}"),
        })?;
        Ok(format!(
            "{}{}",
            Self::ENCODED_PREFIX,
            URL_SAFE_NO_PAD.encode(bytes)
        ))
    }

    pub fn from_opaque_string(encoded: &str) -> Result<Self> {
        if encoded.len() > Self::MAX_ENCODED_BYTES {
            return Err(VfsError::InvalidCursor {
                detail: "encoded cursor exceeds the size limit".to_string(),
            });
        }
        let payload =
            encoded
                .strip_prefix(Self::ENCODED_PREFIX)
                .ok_or_else(|| VfsError::InvalidCursor {
                    detail: "cursor has an unsupported encoding".to_string(),
                })?;
        let bytes = URL_SAFE_NO_PAD
            .decode(payload)
            .map_err(|source| VfsError::InvalidCursor {
                detail: format!("cursor is not valid base64url: {source}"),
            })?;
        let cursor: Self =
            serde_json::from_slice(&bytes).map_err(|source| VfsError::InvalidCursor {
                detail: format!("cursor payload is invalid: {source}"),
            })?;
        if cursor.schema_version != Self::SCHEMA_VERSION {
            return Err(VfsError::InvalidCursor {
                detail: format!("cursor schema {} is unsupported", cursor.schema_version),
            });
        }
        Ok(cursor)
    }

    fn new(backend: &str, directory: &str, opaque: Vec<u8>) -> Self {
        Self {
            schema_version: Self::SCHEMA_VERSION,
            backend: backend.to_string(),
            directory: directory.to_string(),
            opaque,
        }
    }

    fn backend_token<'a>(&'a self, backend: &str, directory: &str) -> Result<&'a [u8]> {
        if self.schema_version != Self::SCHEMA_VERSION {
            return Err(VfsError::InvalidCursor {
                detail: format!("cursor schema {} is unsupported", self.schema_version),
            });
        }
        if self.opaque.len() > Self::MAX_BACKEND_BYTES {
            return Err(VfsError::InvalidCursor {
                detail: "backend cursor exceeds the size limit".to_string(),
            });
        }
        if self.backend != backend {
            return Err(VfsError::InvalidCursor {
                detail: format!("cursor belongs to backend {}, not {backend}", self.backend),
            });
        }
        if self.directory != directory {
            return Err(VfsError::InvalidCursor {
                detail: format!(
                    "cursor belongs to directory {}, not {directory}",
                    self.directory
                ),
            });
        }
        Ok(&self.opaque)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VfsDirectoryPage {
    /// Exact directory generation that produced this page when the backend
    /// exposes one. Guarded no-link listing requires this receipt whenever a
    /// directory exists.
    pub directory_metadata: Option<VfsMetadata>,
    pub entries: Vec<VfsDirEntry>,
    pub next_cursor: Option<VfsDirectoryCursor>,
}

/// Backend-facing page with a cursor payload that the VFS facade treats as
/// uninterpreted bytes.
#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VfsBackendDirectoryPage {
    pub directory_metadata: Option<VfsMetadata>,
    pub entries: Vec<VfsDirEntry>,
    pub next_cursor: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VfsFileRead {
    pub bytes: Vec<u8>,
    pub truncated: bool,
}

/// Bounded bytes plus the exact backend identity/version that was opened for
/// that read. `truncated` means content remains after the returned range.
/// Security-sensitive callers use this receipt instead of issuing a second
/// stat that could observe a replacement object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VfsOpenedFileRead {
    pub metadata: VfsMetadata,
    pub bytes: Vec<u8>,
    pub truncated: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VfsWriteDurability {
    Local,
    ReplicatedEventual,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VfsConsistencyTarget {
    BestEffort,
    ReadYourWrites,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VfsCreateMode {
    CreateOrReplace,
    CreateNew,
    ReplaceExisting,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VfsConflictBehavior {
    Fail,
    Overwrite,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VfsWriteOptions {
    pub durability: VfsWriteDurability,
    pub consistency: VfsConsistencyTarget,
    pub create_mode: VfsCreateMode,
    pub conflict_behavior: VfsConflictBehavior,
}

impl Default for VfsWriteOptions {
    fn default() -> Self {
        Self {
            durability: VfsWriteDurability::ReplicatedEventual,
            consistency: VfsConsistencyTarget::ReadYourWrites,
            create_mode: VfsCreateMode::CreateOrReplace,
            conflict_behavior: VfsConflictBehavior::Fail,
        }
    }
}

impl VfsWriteOptions {
    pub fn local_replace() -> Self {
        Self {
            durability: VfsWriteDurability::Local,
            ..Self::default()
        }
    }

    pub fn service_artifact() -> Self {
        Self {
            durability: VfsWriteDurability::ReplicatedEventual,
            consistency: VfsConsistencyTarget::BestEffort,
            create_mode: VfsCreateMode::CreateOrReplace,
            conflict_behavior: VfsConflictBehavior::Overwrite,
        }
    }

    pub fn create_new(mut self) -> Self {
        self.create_mode = VfsCreateMode::CreateNew;
        self
    }

    pub fn replace_existing(mut self) -> Self {
        self.create_mode = VfsCreateMode::ReplaceExisting;
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VfsAppendOptions {
    pub durability: VfsWriteDurability,
    pub consistency: VfsConsistencyTarget,
    pub create: bool,
    pub conflict_behavior: VfsConflictBehavior,
}

impl Default for VfsAppendOptions {
    fn default() -> Self {
        Self {
            durability: VfsWriteDurability::ReplicatedEventual,
            consistency: VfsConsistencyTarget::ReadYourWrites,
            create: true,
            conflict_behavior: VfsConflictBehavior::Fail,
        }
    }
}

impl VfsAppendOptions {
    pub fn local() -> Self {
        Self {
            durability: VfsWriteDurability::Local,
            ..Self::default()
        }
    }

    pub fn service_artifact() -> Self {
        Self {
            durability: VfsWriteDurability::ReplicatedEventual,
            consistency: VfsConsistencyTarget::BestEffort,
            create: true,
            conflict_behavior: VfsConflictBehavior::Overwrite,
        }
    }

    pub fn without_create(mut self) -> Self {
        self.create = false;
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VfsRemoveOptions {
    pub directory: bool,
    pub missing_ok: bool,
}

impl Default for VfsRemoveOptions {
    fn default() -> Self {
        Self {
            directory: false,
            missing_ok: false,
        }
    }
}

impl VfsRemoveOptions {
    pub fn file() -> Self {
        Self::default()
    }

    pub fn directory() -> Self {
        Self {
            directory: true,
            missing_ok: false,
        }
    }

    pub fn missing_ok(mut self) -> Self {
        self.missing_ok = true;
        self
    }
}

pub trait VfsBackend: Send + Sync + std::fmt::Debug {
    fn backend_name(&self) -> &'static str;
    fn stat(&self, path: &str) -> Result<VfsMetadata>;
    /// Return metadata for the namespace entry itself without following a
    /// final symbolic or backend-native hard link.
    ///
    /// Backends that do not distinguish link records may use the ordinary
    /// stat projection. Capability-scoped callers must use a backend that
    /// implements true no-follow semantics before treating a path as trusted.
    fn lstat(&self, path: &str) -> Result<VfsMetadata> {
        self.stat(path)
    }
    /// Return exact metadata after an authoritative component walk that
    /// rejects every link record from `root` through `path`.
    fn stat_beneath_no_links(&self, root: &str, path: &str) -> Result<VfsMetadata> {
        Err(VfsError::Unsupported {
            backend: self.backend_name().to_string(),
            detail: format!(
                "no-link metadata lookup beneath scoped root {root} is unavailable for {path}"
            ),
        })
    }
    fn list(&self, path: &str) -> Result<Vec<VfsDirEntry>>;
    fn list_page(
        &self,
        path: &str,
        after: Option<&[u8]>,
        max_entries: usize,
    ) -> Result<VfsBackendDirectoryPage> {
        let mut entries = self.list(path)?;
        entries.sort_by(|left, right| left.path.cmp(&right.path));
        let start = after
            .map(|cursor| entries.partition_point(|entry| entry.path.as_bytes() <= cursor))
            .unwrap_or(0);
        let end = start.saturating_add(max_entries).min(entries.len());
        let has_more = end < entries.len();
        let entries = entries[start..end].to_vec();
        let next_cursor = has_more
            .then(|| entries.last().map(|entry| entry.path.as_bytes().to_vec()))
            .flatten();
        Ok(VfsBackendDirectoryPage {
            directory_metadata: None,
            entries,
            next_cursor,
        })
    }
    /// List one bounded directory page beneath `root` after an authoritative
    /// component walk that rejects every link record. The backend cursor must
    /// remain bound to the exact opened directory generation.
    fn list_page_beneath_no_links(
        &self,
        root: &str,
        path: &str,
        _after: Option<&[u8]>,
        _max_entries: usize,
    ) -> Result<VfsBackendDirectoryPage> {
        Err(VfsError::Unsupported {
            backend: self.backend_name().to_string(),
            detail: format!(
                "bounded no-link listing beneath scoped root {root} is unavailable for {path}"
            ),
        })
    }
    fn read(&self, path: &str) -> Result<Vec<u8>>;
    fn read_prefix(&self, path: &str, max_bytes: u64) -> Result<VfsFileRead>;
    /// Read through an authoritative component walk that rejects every link
    /// record from `root` through `path` and never resolves the path again
    /// after opening the exact file identity.
    fn read_prefix_beneath_no_links(
        &self,
        root: &str,
        path: &str,
        _max_bytes: u64,
    ) -> Result<VfsOpenedFileRead> {
        Err(VfsError::Unsupported {
            backend: self.backend_name().to_string(),
            detail: format!(
                "bounded no-link reads beneath scoped root {root} are unavailable for {path}"
            ),
        })
    }
    /// Read one bounded range through an authoritative component walk that
    /// rejects every link record from `root` through `path`. The returned
    /// metadata and bytes must describe the same exact opened object version.
    /// An offset beyond end-of-file must fail instead of returning an empty
    /// range that callers could mistake for EOF.
    fn read_range_beneath_no_links(
        &self,
        root: &str,
        path: &str,
        _offset: u64,
        _max_bytes: u64,
    ) -> Result<VfsOpenedFileRead> {
        Err(VfsError::Unsupported {
            backend: self.backend_name().to_string(),
            detail: format!(
                "bounded no-link range reads beneath scoped root {root} are unavailable for {path}"
            ),
        })
    }
    fn write(&self, path: &str, bytes: &[u8], options: &VfsWriteOptions) -> Result<()>;
    /// Atomically create a new file and return the exact committed identity
    /// and version. Backends must not emulate the receipt with a later stat.
    fn create_new_versioned(
        &self,
        path: &str,
        _bytes: &[u8],
        _options: &VfsWriteOptions,
    ) -> Result<VfsMetadata> {
        Err(VfsError::Unsupported {
            backend: self.backend_name().to_string(),
            detail: format!("atomic versioned creation is unavailable for {path}"),
        })
    }
    /// Atomically create a new file beneath `root` without following any link
    /// component. Backends must not implement this as lstat-then-write.
    fn create_new_versioned_beneath_no_links(
        &self,
        root: &str,
        path: &str,
        _bytes: &[u8],
        _options: &VfsWriteOptions,
    ) -> Result<VfsMetadata> {
        Err(VfsError::Unsupported {
            backend: self.backend_name().to_string(),
            detail: format!(
                "atomic no-link creation beneath scoped root {root} is unavailable for {path}"
            ),
        })
    }
    /// Replace an existing file only if its opaque backend version still
    /// matches `expected_version`.
    ///
    /// Backends without an atomic compare-and-replace primitive must leave
    /// this unsupported; a stat-then-write emulation would lose updates.
    fn replace_if_version(
        &self,
        path: &str,
        expected_object_id: &str,
        expected_version: &str,
        _bytes: &[u8],
        _options: &VfsWriteOptions,
    ) -> Result<VfsMetadata> {
        Err(VfsError::Unsupported {
            backend: self.backend_name().to_string(),
            detail: format!(
                "atomic versioned replacement is unavailable for {path} object {expected_object_id} at version {expected_version}"
            ),
        })
    }
    /// Atomically compare-and-replace an exact file beneath `root` without
    /// following any link component. Backends must bind both the object
    /// identity and version after the no-link open.
    fn replace_if_version_beneath_no_links(
        &self,
        root: &str,
        path: &str,
        expected_object_id: &str,
        expected_version: &str,
        _bytes: &[u8],
        _options: &VfsWriteOptions,
    ) -> Result<VfsMetadata> {
        Err(VfsError::Unsupported {
            backend: self.backend_name().to_string(),
            detail: format!(
                "atomic no-link replacement beneath scoped root {root} is unavailable for {path} object {expected_object_id} at version {expected_version}"
            ),
        })
    }
    fn append(&self, path: &str, bytes: &[u8], options: &VfsAppendOptions) -> Result<()>;
    /// Ensure a directory chain exists beneath `root` without following any
    /// link component. Each missing component must be created relative to an
    /// exact opened parent rather than by re-resolving the full path.
    fn create_dir_all_beneath_no_links(&self, root: &str, path: &str) -> Result<()> {
        Err(VfsError::Unsupported {
            backend: self.backend_name().to_string(),
            detail: format!(
                "no-link directory creation beneath scoped root {root} is unavailable for {path}"
            ),
        })
    }
    fn create_dir_all(&self, path: &str) -> Result<()>;
    fn rename(&self, from: &str, to: &str) -> Result<()>;
    fn remove(&self, path: &str, options: &VfsRemoveOptions) -> Result<()>;
}

#[derive(Clone, Debug)]
pub struct VfsHandle {
    backend: Arc<dyn VfsBackend>,
    root: String,
}

impl VfsHandle {
    pub fn new(backend: Arc<dyn VfsBackend>, root: impl AsRef<str>) -> Result<Self> {
        let root = normalize_absolute_path(root.as_ref())?;
        Ok(Self { backend, root })
    }

    pub fn local(root: impl Into<PathBuf>) -> Result<Self> {
        Self::new(Arc::new(LocalVfsBackend::new(root.into())), "/")
    }

    pub fn memory() -> Self {
        Self {
            backend: Arc::new(MemoryVfsBackend::new()),
            root: "/".to_string(),
        }
    }

    pub fn scoped(&self, root: impl AsRef<str>) -> Result<Self> {
        let root = self.resolve(root.as_ref())?;
        Ok(Self {
            backend: Arc::clone(&self.backend),
            root,
        })
    }

    pub fn root(&self) -> &str {
        &self.root
    }

    pub fn backend_name(&self) -> &'static str {
        self.backend.backend_name()
    }

    pub fn stat(&self, path: impl AsRef<str>) -> Result<VfsMetadata> {
        let path = self.resolve(path.as_ref())?;
        self.backend.stat(&path)
    }

    /// Inspect the final namespace entry without following link records.
    pub fn lstat(&self, path: impl AsRef<str>) -> Result<VfsMetadata> {
        let path = self.resolve(path.as_ref())?;
        self.backend.lstat(&path)
    }

    /// Inspect an exact path within this handle's root without following any
    /// symbolic, hardlink, or backend-native link component.
    pub fn stat_beneath_no_links(&self, path: impl AsRef<str>) -> Result<VfsMetadata> {
        let path = self.resolve(path.as_ref())?;
        self.backend.stat_beneath_no_links(&self.root, &path)
    }

    pub fn list(&self, path: impl AsRef<str>) -> Result<Vec<VfsDirEntry>> {
        let path = self.resolve(path.as_ref())?;
        self.backend.list(&path)
    }

    pub fn list_page(
        &self,
        path: impl AsRef<str>,
        after: Option<&VfsDirectoryCursor>,
        max_entries: usize,
    ) -> Result<VfsDirectoryPage> {
        if max_entries == 0 {
            return Err(VfsError::InvalidCursor {
                detail: "directory page size must be greater than zero".to_string(),
            });
        }
        let path = self.resolve(path.as_ref())?;
        let backend_name = self.backend.backend_name();
        let backend_cursor = after
            .map(|cursor| cursor.backend_token(backend_name, &path))
            .transpose()?;
        let page = self.backend.list_page(&path, backend_cursor, max_entries)?;
        if page.entries.len() > max_entries {
            return Err(VfsError::Backend {
                backend: backend_name.to_string(),
                path: path.clone(),
                detail: format!(
                    "backend returned {} entries for a page limited to {max_entries}",
                    page.entries.len()
                ),
            });
        }
        if page
            .next_cursor
            .as_ref()
            .is_some_and(|cursor| cursor.len() > VfsDirectoryCursor::MAX_BACKEND_BYTES)
        {
            return Err(VfsError::Backend {
                backend: backend_name.to_string(),
                path: path.clone(),
                detail: "backend returned an oversized directory cursor".to_string(),
            });
        }
        for entry in &page.entries {
            let normalized =
                normalize_absolute_path(&entry.path).map_err(|source| VfsError::Backend {
                    backend: backend_name.to_string(),
                    path: path.clone(),
                    detail: format!("backend returned an invalid entry path: {source}"),
                })?;
            if normalized != entry.path
                || parent_vfs_path(&normalized).as_deref() != Some(path.as_str())
            {
                return Err(VfsError::Backend {
                    backend: backend_name.to_string(),
                    path: path.clone(),
                    detail: format!(
                        "backend returned entry {} outside the listed directory",
                        entry.path
                    ),
                });
            }
        }
        if let (Some(previous), Some(next)) = (backend_cursor, page.next_cursor.as_deref())
            && previous == next
        {
            return Err(VfsError::Backend {
                backend: backend_name.to_string(),
                path: path.clone(),
                detail: "backend returned a non-advancing directory cursor".to_string(),
            });
        }
        Ok(VfsDirectoryPage {
            directory_metadata: page.directory_metadata,
            entries: page.entries,
            next_cursor: page
                .next_cursor
                .map(|opaque| VfsDirectoryCursor::new(backend_name, &path, opaque)),
        })
    }

    /// List a bounded page while treating this handle's root as a no-link
    /// capability boundary. Returned entries and cursors receive the same
    /// path and progress validation as ordinary VFS pages.
    pub fn list_page_beneath_no_links(
        &self,
        path: impl AsRef<str>,
        after: Option<&VfsDirectoryCursor>,
        max_entries: usize,
    ) -> Result<VfsDirectoryPage> {
        if max_entries == 0 {
            return Err(VfsError::InvalidCursor {
                detail: "directory page size must be greater than zero".to_string(),
            });
        }
        let path = self.resolve(path.as_ref())?;
        let backend_name = self.backend.backend_name();
        let backend_cursor = after
            .map(|cursor| cursor.backend_token(backend_name, &path))
            .transpose()?;
        let page = self.backend.list_page_beneath_no_links(
            &self.root,
            &path,
            backend_cursor,
            max_entries,
        )?;
        let directory_metadata =
            page.directory_metadata
                .as_ref()
                .ok_or_else(|| VfsError::Backend {
                    backend: backend_name.to_string(),
                    path: path.clone(),
                    detail: "no-link directory page omitted its exact directory receipt"
                        .to_string(),
                })?;
        if directory_metadata.path != path
            || directory_metadata.kind != VfsObjectKind::Directory
            || directory_metadata.object_id.is_none()
            || directory_metadata.version.is_none()
        {
            return Err(VfsError::Backend {
                backend: backend_name.to_string(),
                path: path.clone(),
                detail: "no-link directory page returned an invalid directory receipt".to_string(),
            });
        }
        if page.entries.len() > max_entries {
            return Err(VfsError::Backend {
                backend: backend_name.to_string(),
                path: path.clone(),
                detail: format!(
                    "backend returned {} entries for a page limited to {max_entries}",
                    page.entries.len()
                ),
            });
        }
        if page
            .next_cursor
            .as_ref()
            .is_some_and(|cursor| cursor.len() > VfsDirectoryCursor::MAX_BACKEND_BYTES)
        {
            return Err(VfsError::Backend {
                backend: backend_name.to_string(),
                path: path.clone(),
                detail: "backend returned an oversized directory cursor".to_string(),
            });
        }
        for entry in &page.entries {
            let normalized =
                normalize_absolute_path(&entry.path).map_err(|source| VfsError::Backend {
                    backend: backend_name.to_string(),
                    path: path.clone(),
                    detail: format!("backend returned an invalid entry path: {source}"),
                })?;
            if normalized != entry.path
                || parent_vfs_path(&normalized).as_deref() != Some(path.as_str())
            {
                return Err(VfsError::Backend {
                    backend: backend_name.to_string(),
                    path: path.clone(),
                    detail: format!(
                        "backend returned entry {} outside the listed directory",
                        entry.path
                    ),
                });
            }
        }
        if let (Some(previous), Some(next)) = (backend_cursor, page.next_cursor.as_deref())
            && previous == next
        {
            return Err(VfsError::Backend {
                backend: backend_name.to_string(),
                path: path.clone(),
                detail: "backend returned a non-advancing directory cursor".to_string(),
            });
        }
        Ok(VfsDirectoryPage {
            directory_metadata: page.directory_metadata,
            entries: page.entries,
            next_cursor: page
                .next_cursor
                .map(|opaque| VfsDirectoryCursor::new(backend_name, &path, opaque)),
        })
    }

    pub fn list_page_beneath_no_links_if_exists(
        &self,
        path: impl AsRef<str>,
        after: Option<&VfsDirectoryCursor>,
        max_entries: usize,
    ) -> Result<VfsDirectoryPage> {
        match self.list_page_beneath_no_links(path, after, max_entries) {
            Ok(page) => Ok(page),
            Err(VfsError::NotFound { .. }) => Ok(VfsDirectoryPage {
                directory_metadata: None,
                entries: Vec::new(),
                next_cursor: None,
            }),
            Err(source) => Err(source),
        }
    }

    pub fn list_page_if_exists(
        &self,
        path: impl AsRef<str>,
        after: Option<&VfsDirectoryCursor>,
        max_entries: usize,
    ) -> Result<VfsDirectoryPage> {
        match self.list_page(path, after, max_entries) {
            Ok(page) => Ok(page),
            Err(VfsError::NotFound { .. }) => Ok(VfsDirectoryPage {
                directory_metadata: None,
                entries: Vec::new(),
                next_cursor: None,
            }),
            Err(source) => Err(source),
        }
    }

    pub fn list_if_exists(&self, path: impl AsRef<str>) -> Result<Vec<VfsDirEntry>> {
        let path = self.resolve(path.as_ref())?;
        match self.backend.list(&path) {
            Ok(entries) => Ok(entries),
            Err(VfsError::NotFound { .. }) => Ok(Vec::new()),
            Err(source) => Err(source),
        }
    }

    pub fn read(&self, path: impl AsRef<str>) -> Result<Vec<u8>> {
        let path = self.resolve(path.as_ref())?;
        self.backend.read(&path)
    }

    pub fn read_optional(&self, path: impl AsRef<str>) -> Result<Option<Vec<u8>>> {
        let path = self.resolve(path.as_ref())?;
        match self.backend.read(&path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(VfsError::NotFound { .. }) => Ok(None),
            Err(source) => Err(source),
        }
    }

    pub fn read_prefix(&self, path: impl AsRef<str>, max_bytes: u64) -> Result<VfsFileRead> {
        let path = self.resolve(path.as_ref())?;
        self.backend.read_prefix(&path, max_bytes)
    }

    /// Read a bounded prefix while treating this handle's scoped root as a
    /// no-link capability boundary.
    pub fn read_prefix_beneath_no_links(
        &self,
        path: impl AsRef<str>,
        max_bytes: u64,
    ) -> Result<VfsOpenedFileRead> {
        let path = self.resolve(path.as_ref())?;
        self.backend
            .read_prefix_beneath_no_links(&self.root, &path, max_bytes)
    }

    /// Read a bounded byte range while treating this handle's scoped root as a
    /// no-link capability boundary. `truncated` is true exactly when the opened
    /// file has bytes after this range.
    pub fn read_range_beneath_no_links(
        &self,
        path: impl AsRef<str>,
        offset: u64,
        max_bytes: u64,
    ) -> Result<VfsOpenedFileRead> {
        let path = self.resolve(path.as_ref())?;
        self.backend
            .read_range_beneath_no_links(&self.root, &path, offset, max_bytes)
    }

    pub fn write(&self, path: impl AsRef<str>, bytes: impl AsRef<[u8]>) -> Result<()> {
        self.write_with_options(path, bytes, VfsWriteOptions::default())
    }

    pub fn write_with_options(
        &self,
        path: impl AsRef<str>,
        bytes: impl AsRef<[u8]>,
        options: VfsWriteOptions,
    ) -> Result<()> {
        let path = self.resolve(path.as_ref())?;
        self.backend.write(&path, bytes.as_ref(), &options)
    }

    pub fn replace_if_version(
        &self,
        path: impl AsRef<str>,
        expected_object_id: impl AsRef<str>,
        expected_version: impl AsRef<str>,
        bytes: impl AsRef<[u8]>,
        options: VfsWriteOptions,
    ) -> Result<VfsMetadata> {
        let path = self.resolve(path.as_ref())?;
        self.backend.replace_if_version(
            &path,
            expected_object_id.as_ref(),
            expected_version.as_ref(),
            bytes.as_ref(),
            &options,
        )
    }

    /// Compare-and-replace within this handle's root without following any
    /// symbolic, hardlink, or backend-native link component.
    pub fn replace_if_version_beneath_no_links(
        &self,
        path: impl AsRef<str>,
        expected_object_id: impl AsRef<str>,
        expected_version: impl AsRef<str>,
        bytes: impl AsRef<[u8]>,
        options: VfsWriteOptions,
    ) -> Result<VfsMetadata> {
        let path = self.resolve(path.as_ref())?;
        self.backend.replace_if_version_beneath_no_links(
            &self.root,
            &path,
            expected_object_id.as_ref(),
            expected_version.as_ref(),
            bytes.as_ref(),
            &options,
        )
    }

    pub fn create_new_versioned(
        &self,
        path: impl AsRef<str>,
        bytes: impl AsRef<[u8]>,
        options: VfsWriteOptions,
    ) -> Result<VfsMetadata> {
        let path = self.resolve(path.as_ref())?;
        self.backend
            .create_new_versioned(&path, bytes.as_ref(), &options)
    }

    /// Exclusively create within this handle's root without following any
    /// symbolic, hardlink, or backend-native link component.
    pub fn create_new_versioned_beneath_no_links(
        &self,
        path: impl AsRef<str>,
        bytes: impl AsRef<[u8]>,
        options: VfsWriteOptions,
    ) -> Result<VfsMetadata> {
        let path = self.resolve(path.as_ref())?;
        self.backend.create_new_versioned_beneath_no_links(
            &self.root,
            &path,
            bytes.as_ref(),
            &options,
        )
    }

    pub fn append(&self, path: impl AsRef<str>, bytes: impl AsRef<[u8]>) -> Result<()> {
        self.append_with_options(path, bytes, VfsAppendOptions::default())
    }

    pub fn append_with_options(
        &self,
        path: impl AsRef<str>,
        bytes: impl AsRef<[u8]>,
        options: VfsAppendOptions,
    ) -> Result<()> {
        let path = self.resolve(path.as_ref())?;
        self.backend.append(&path, bytes.as_ref(), &options)
    }

    pub fn create_dir_all(&self, path: impl AsRef<str>) -> Result<()> {
        let path = self.resolve(path.as_ref())?;
        self.backend.create_dir_all(&path)
    }

    /// Ensure a directory chain exists while treating this handle's root as a
    /// no-link capability boundary.
    pub fn create_dir_all_beneath_no_links(&self, path: impl AsRef<str>) -> Result<()> {
        let path = self.resolve(path.as_ref())?;
        self.backend
            .create_dir_all_beneath_no_links(&self.root, &path)
    }

    pub fn rename(&self, from: impl AsRef<str>, to: impl AsRef<str>) -> Result<()> {
        let from = self.resolve(from.as_ref())?;
        let to = self.resolve(to.as_ref())?;
        self.backend.rename(&from, &to)
    }

    pub fn remove(&self, path: impl AsRef<str>) -> Result<()> {
        self.remove_with_options(path, VfsRemoveOptions::default())
    }

    pub fn remove_with_options(
        &self,
        path: impl AsRef<str>,
        options: VfsRemoveOptions,
    ) -> Result<()> {
        let path = self.resolve(path.as_ref())?;
        self.backend.remove(&path, &options)
    }

    fn resolve(&self, input: &str) -> Result<String> {
        let path = if input.is_empty() || input == "." {
            self.root.clone()
        } else if input.starts_with('/') {
            normalize_absolute_path(input)?
        } else {
            normalize_absolute_path(&join_vfs_path(&self.root, input))?
        };
        if path == self.root || path.starts_with(&format!("{}/", self.root.trim_end_matches('/'))) {
            Ok(path)
        } else {
            Err(VfsError::OutsideScope {
                root: self.root.clone(),
                path,
            })
        }
    }
}

#[derive(Debug)]
pub struct LocalVfsBackend {
    root: PathBuf,
}

impl LocalVfsBackend {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    fn host_path(&self, path: &str) -> PathBuf {
        let relative = path.trim_start_matches('/');
        if relative.is_empty() {
            self.root.clone()
        } else {
            self.root.join(relative)
        }
    }

    fn map_io(&self, path: &str, source: io::Error) -> VfsError {
        match source.kind() {
            io::ErrorKind::NotFound => VfsError::NotFound {
                path: path.to_string(),
            },
            io::ErrorKind::AlreadyExists => VfsError::AlreadyExists {
                path: path.to_string(),
            },
            _ => VfsError::Io {
                path: path.to_string(),
                source,
            },
        }
    }
}

impl VfsBackend for LocalVfsBackend {
    fn backend_name(&self) -> &'static str {
        "local"
    }

    fn stat(&self, path: &str) -> Result<VfsMetadata> {
        let metadata = fs::symlink_metadata(self.host_path(path))
            .map_err(|source| self.map_io(path, source))?;
        Ok(VfsMetadata {
            path: path.to_string(),
            kind: local_file_kind(&metadata),
            object_id: None,
            size_bytes: metadata.is_file().then_some(metadata.len()),
            content_hash: None,
            version: None,
        })
    }

    fn list(&self, path: &str) -> Result<Vec<VfsDirEntry>> {
        let mut entries = Vec::new();
        for entry in
            fs::read_dir(self.host_path(path)).map_err(|source| self.map_io(path, source))?
        {
            let entry = entry.map_err(|source| self.map_io(path, source))?;
            let name = entry.file_name().to_string_lossy().into_owned();
            let child_path = join_vfs_path(path, &name);
            let metadata = fs::symlink_metadata(entry.path())
                .map_err(|source| self.map_io(&child_path, source))?;
            entries.push(VfsDirEntry {
                name,
                path: child_path,
                kind: local_file_kind(&metadata),
                size_bytes: metadata.is_file().then_some(metadata.len()),
            });
        }
        entries.sort_by(|left, right| left.path.cmp(&right.path));
        Ok(entries)
    }

    fn read(&self, path: &str) -> Result<Vec<u8>> {
        fs::read(self.host_path(path)).map_err(|source| self.map_io(path, source))
    }

    fn read_prefix(&self, path: &str, max_bytes: u64) -> Result<VfsFileRead> {
        let host_path = self.host_path(path);
        let metadata = fs::metadata(&host_path).map_err(|source| self.map_io(path, source))?;
        let mut file = fs::File::open(host_path).map_err(|source| self.map_io(path, source))?;
        let mut bytes = Vec::new();
        Read::by_ref(&mut file)
            .take(max_bytes)
            .read_to_end(&mut bytes)
            .map_err(|source| self.map_io(path, source))?;
        Ok(VfsFileRead {
            bytes,
            truncated: metadata.len() > max_bytes,
        })
    }

    fn write(&self, path: &str, bytes: &[u8], options: &VfsWriteOptions) -> Result<()> {
        enforce_local_create_mode(path, &self.host_path(path), options)?;
        fs::write(self.host_path(path), bytes).map_err(|source| self.map_io(path, source))
    }

    fn append(&self, path: &str, bytes: &[u8], options: &VfsAppendOptions) -> Result<()> {
        let mut open_options = fs::OpenOptions::new();
        open_options.append(true).create(options.create);
        let mut file = open_options
            .open(self.host_path(path))
            .map_err(|source| self.map_io(path, source))?;
        file.write_all(bytes)
            .map_err(|source| self.map_io(path, source))?;
        if options.consistency == VfsConsistencyTarget::ReadYourWrites {
            file.sync_all()
                .map_err(|source| self.map_io(path, source))?;
        }
        Ok(())
    }

    fn create_dir_all(&self, path: &str) -> Result<()> {
        fs::create_dir_all(self.host_path(path)).map_err(|source| self.map_io(path, source))
    }

    fn rename(&self, from: &str, to: &str) -> Result<()> {
        fs::rename(self.host_path(from), self.host_path(to))
            .map_err(|source| self.map_io(from, source))
    }

    fn remove(&self, path: &str, options: &VfsRemoveOptions) -> Result<()> {
        let result = if options.directory {
            fs::remove_dir(self.host_path(path))
        } else {
            fs::remove_file(self.host_path(path))
        };
        match result {
            Ok(()) => Ok(()),
            Err(source) if options.missing_ok && source.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(self.map_io(path, source)),
        }
    }
}

#[derive(Clone, Debug)]
pub struct MemoryVfsBackend {
    inner: Arc<Mutex<BTreeMap<String, MemoryNode>>>,
}

#[derive(Clone, Debug)]
enum MemoryNode {
    File(Vec<u8>),
    Directory,
}

impl MemoryVfsBackend {
    pub fn new() -> Self {
        let mut nodes = BTreeMap::new();
        nodes.insert("/".to_string(), MemoryNode::Directory);
        Self {
            inner: Arc::new(Mutex::new(nodes)),
        }
    }

    fn lock(&self, path: &str) -> Result<std::sync::MutexGuard<'_, BTreeMap<String, MemoryNode>>> {
        self.inner.lock().map_err(|_| VfsError::Backend {
            backend: self.backend_name().to_string(),
            path: path.to_string(),
            detail: "memory VFS backend lock is poisoned".to_string(),
        })
    }
}

impl Default for MemoryVfsBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl VfsBackend for MemoryVfsBackend {
    fn backend_name(&self) -> &'static str {
        "memory"
    }

    fn stat(&self, path: &str) -> Result<VfsMetadata> {
        let nodes = self.lock(path)?;
        let node = nodes.get(path).ok_or_else(|| VfsError::NotFound {
            path: path.to_string(),
        })?;
        Ok(memory_metadata(path, node))
    }

    fn list(&self, path: &str) -> Result<Vec<VfsDirEntry>> {
        let nodes = self.lock(path)?;
        match nodes.get(path) {
            Some(MemoryNode::Directory) => {}
            Some(MemoryNode::File(_)) => {
                return Err(VfsError::NotDirectory {
                    path: path.to_string(),
                });
            }
            None => {
                return Err(VfsError::NotFound {
                    path: path.to_string(),
                });
            }
        }
        let mut entries = Vec::new();
        for (candidate, node) in nodes.iter() {
            if candidate == path {
                continue;
            }
            if parent_vfs_path(candidate).as_deref() == Some(path) {
                entries.push(VfsDirEntry {
                    name: file_name(candidate).unwrap_or_default().to_string(),
                    path: candidate.clone(),
                    kind: memory_kind(node),
                    size_bytes: match node {
                        MemoryNode::File(bytes) => Some(bytes.len() as u64),
                        MemoryNode::Directory => None,
                    },
                });
            }
        }
        Ok(entries)
    }

    fn read(&self, path: &str) -> Result<Vec<u8>> {
        let nodes = self.lock(path)?;
        match nodes.get(path) {
            Some(MemoryNode::File(bytes)) => Ok(bytes.clone()),
            Some(MemoryNode::Directory) => Err(VfsError::IsDirectory {
                path: path.to_string(),
            }),
            None => Err(VfsError::NotFound {
                path: path.to_string(),
            }),
        }
    }

    fn read_prefix(&self, path: &str, max_bytes: u64) -> Result<VfsFileRead> {
        let nodes = self.lock(path)?;
        match nodes.get(path) {
            Some(MemoryNode::File(bytes)) => {
                let len = bytes.len().min(max_bytes as usize);
                Ok(VfsFileRead {
                    bytes: bytes[..len].to_vec(),
                    truncated: bytes.len() as u64 > max_bytes,
                })
            }
            Some(MemoryNode::Directory) => Err(VfsError::IsDirectory {
                path: path.to_string(),
            }),
            None => Err(VfsError::NotFound {
                path: path.to_string(),
            }),
        }
    }

    fn write(&self, path: &str, bytes: &[u8], options: &VfsWriteOptions) -> Result<()> {
        let mut nodes = self.lock(path)?;
        enforce_memory_create_mode(&nodes, path, options)?;
        let parent = parent_vfs_path(path).ok_or_else(|| VfsError::InvalidPath {
            path: path.to_string(),
            detail: "cannot write root as a file".to_string(),
        })?;
        match nodes.get(&parent) {
            Some(MemoryNode::Directory) => {}
            Some(MemoryNode::File(_)) => {
                return Err(VfsError::NotDirectory { path: parent });
            }
            None => {
                return Err(VfsError::NotFound { path: parent });
            }
        }
        nodes.insert(path.to_string(), MemoryNode::File(bytes.to_vec()));
        Ok(())
    }

    fn append(&self, path: &str, bytes: &[u8], options: &VfsAppendOptions) -> Result<()> {
        let mut nodes = self.lock(path)?;
        match nodes.get_mut(path) {
            Some(MemoryNode::File(existing)) => {
                existing.extend_from_slice(bytes);
                return Ok(());
            }
            Some(MemoryNode::Directory) => {
                return Err(VfsError::IsDirectory {
                    path: path.to_string(),
                });
            }
            None if !options.create => {
                return Err(VfsError::NotFound {
                    path: path.to_string(),
                });
            }
            None => {}
        }
        let parent = parent_vfs_path(path).ok_or_else(|| VfsError::InvalidPath {
            path: path.to_string(),
            detail: "cannot append to root as a file".to_string(),
        })?;
        match nodes.get(&parent) {
            Some(MemoryNode::Directory) => {}
            Some(MemoryNode::File(_)) => {
                return Err(VfsError::NotDirectory { path: parent });
            }
            None => {
                return Err(VfsError::NotFound { path: parent });
            }
        }
        nodes.insert(path.to_string(), MemoryNode::File(bytes.to_vec()));
        Ok(())
    }

    fn create_dir_all(&self, path: &str) -> Result<()> {
        let mut nodes = self.lock(path)?;
        let mut current = "/".to_string();
        for segment in path
            .trim_start_matches('/')
            .split('/')
            .filter(|s| !s.is_empty())
        {
            current = join_vfs_path(&current, segment);
            match nodes.get(&current) {
                Some(MemoryNode::Directory) => {}
                Some(MemoryNode::File(_)) => {
                    return Err(VfsError::NotDirectory {
                        path: current.clone(),
                    });
                }
                None => {
                    nodes.insert(current.clone(), MemoryNode::Directory);
                }
            }
        }
        Ok(())
    }

    fn rename(&self, from: &str, to: &str) -> Result<()> {
        let mut nodes = self.lock(from)?;
        if !nodes.contains_key(from) {
            return Err(VfsError::NotFound {
                path: from.to_string(),
            });
        }
        let parent = parent_vfs_path(to).ok_or_else(|| VfsError::InvalidPath {
            path: to.to_string(),
            detail: "cannot rename onto root".to_string(),
        })?;
        match nodes.get(&parent) {
            Some(MemoryNode::Directory) => {}
            Some(MemoryNode::File(_)) => {
                return Err(VfsError::NotDirectory { path: parent });
            }
            None => return Err(VfsError::NotFound { path: parent }),
        }
        let moving = nodes
            .iter()
            .filter(|(path, _)| *path == from || path.starts_with(&format!("{from}/")))
            .map(|(path, node)| (path.clone(), node.clone()))
            .collect::<Vec<_>>();
        if moving.is_empty() {
            return Err(VfsError::NotFound {
                path: from.to_string(),
            });
        }
        for (path, _) in &moving {
            nodes.remove(path);
        }
        for (path, node) in moving {
            let suffix = path.strip_prefix(from).unwrap_or_default();
            nodes.insert(format!("{to}{suffix}"), node);
        }
        Ok(())
    }

    fn remove(&self, path: &str, options: &VfsRemoveOptions) -> Result<()> {
        let mut nodes = self.lock(path)?;
        match nodes.get(path) {
            Some(MemoryNode::Directory) if !options.directory => {
                return Err(VfsError::IsDirectory {
                    path: path.to_string(),
                });
            }
            Some(MemoryNode::File(_)) if options.directory => {
                return Err(VfsError::NotDirectory {
                    path: path.to_string(),
                });
            }
            Some(MemoryNode::Directory) => {
                let child_prefix = format!("{}/", path.trim_end_matches('/'));
                if nodes
                    .keys()
                    .any(|candidate| candidate.starts_with(&child_prefix))
                {
                    return Err(VfsError::DirectoryNotEmpty {
                        path: path.to_string(),
                    });
                }
            }
            Some(MemoryNode::File(_)) => {}
            None if options.missing_ok => return Ok(()),
            None => {
                return Err(VfsError::NotFound {
                    path: path.to_string(),
                });
            }
        }
        nodes.remove(path);
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct JsonFileStore<T> {
    handle: VfsHandle,
    path: String,
    pretty: bool,
    _marker: PhantomData<T>,
}

impl<T> JsonFileStore<T> {
    pub fn new(handle: VfsHandle, path: impl Into<String>) -> Self {
        Self {
            handle,
            path: path.into(),
            pretty: true,
            _marker: PhantomData,
        }
    }

    pub fn compact(mut self) -> Self {
        self.pretty = false;
        self
    }

    pub fn path(&self) -> &str {
        &self.path
    }
}

impl<T> JsonFileStore<T>
where
    T: Serialize + DeserializeOwned,
{
    pub fn load_optional(&self) -> Result<Option<T>> {
        let Some(bytes) = self.handle.read_optional(&self.path)? else {
            return Ok(None);
        };
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|source| VfsError::Json {
                path: self.path.clone(),
                detail: source.to_string(),
            })
    }

    pub fn load(&self) -> Result<T> {
        let bytes = self.handle.read(&self.path)?;
        serde_json::from_slice(&bytes).map_err(|source| VfsError::Json {
            path: self.path.clone(),
            detail: source.to_string(),
        })
    }

    pub fn save(&self, value: &T) -> Result<()> {
        self.save_with_options(value, VfsWriteOptions::service_artifact())
    }

    pub fn save_with_options(&self, value: &T, options: VfsWriteOptions) -> Result<()> {
        let bytes = if self.pretty {
            serde_json::to_vec_pretty(value)
        } else {
            serde_json::to_vec(value)
        }
        .map_err(|source| VfsError::Json {
            path: self.path.clone(),
            detail: source.to_string(),
        })?;
        self.handle.write_with_options(&self.path, bytes, options)
    }
}

#[derive(Clone, Debug)]
pub struct SobjFileStore<T> {
    handle: VfsHandle,
    path: String,
    version: u16,
    length_mode: LengthMode,
    _marker: PhantomData<T>,
}

impl<T> SobjFileStore<T> {
    pub fn new(handle: VfsHandle, path: impl Into<String>, version: u16) -> Self {
        Self {
            handle,
            path: path.into(),
            version,
            length_mode: LengthMode::U32,
            _marker: PhantomData,
        }
    }

    pub fn with_length_mode(mut self, length_mode: LengthMode) -> Self {
        self.length_mode = length_mode;
        self
    }

    pub fn path(&self) -> &str {
        &self.path
    }
}

impl<T> SobjFileStore<T>
where
    T: Serialize + DeserializeOwned,
{
    pub fn load_optional(&self) -> Result<Option<T>> {
        let Some(bytes) = self.handle.read_optional(&self.path)? else {
            return Ok(None);
        };
        serde_ext::from_bytes(&bytes)
            .map(Some)
            .map_err(|source| VfsError::Sobj {
                path: self.path.clone(),
                detail: source.to_string(),
            })
    }

    pub fn load(&self) -> Result<T> {
        let bytes = self.handle.read(&self.path)?;
        serde_ext::from_bytes(&bytes).map_err(|source| VfsError::Sobj {
            path: self.path.clone(),
            detail: source.to_string(),
        })
    }

    pub fn save(&self, value: &T) -> Result<()> {
        self.save_with_options(value, VfsWriteOptions::service_artifact())
    }

    pub fn save_with_options(&self, value: &T, options: VfsWriteOptions) -> Result<()> {
        let bytes =
            serde_ext::to_bytes(value, self.version, self.length_mode).map_err(|source| {
                VfsError::Sobj {
                    path: self.path.clone(),
                    detail: source.to_string(),
                }
            })?;
        self.handle.write_with_options(&self.path, bytes, options)
    }
}

#[derive(Clone, Debug)]
pub struct DirectoryObjectStore<T> {
    handle: VfsHandle,
    extension: String,
    codec: DirectoryObjectCodec,
    pretty: bool,
    _marker: PhantomData<T>,
}

#[derive(Clone, Debug)]
enum DirectoryObjectCodec {
    Json,
    Sobj {
        version: u16,
        length_mode: LengthMode,
    },
}

impl<T> DirectoryObjectStore<T> {
    pub fn json(handle: VfsHandle, extension: impl Into<String>) -> Self {
        Self {
            handle,
            extension: normalize_extension(extension.into()),
            codec: DirectoryObjectCodec::Json,
            pretty: true,
            _marker: PhantomData,
        }
    }

    pub fn sobj(handle: VfsHandle, extension: impl Into<String>, version: u16) -> Self {
        Self {
            handle,
            extension: normalize_extension(extension.into()),
            codec: DirectoryObjectCodec::Sobj {
                version,
                length_mode: LengthMode::U32,
            },
            pretty: false,
            _marker: PhantomData,
        }
    }

    pub fn compact_json(mut self) -> Self {
        self.pretty = false;
        self
    }

    pub fn root(&self) -> &str {
        self.handle.root()
    }

    pub fn path_for_id(&self, id: &str) -> Result<String> {
        validate_object_id(id)?;
        Ok(format!("{}.{}", id, self.extension))
    }
}

impl<T> DirectoryObjectStore<T>
where
    T: Serialize + DeserializeOwned,
{
    pub fn load_all(&self) -> Result<Vec<T>> {
        let mut entries = self
            .handle
            .list_if_exists("")?
            .into_iter()
            .filter(|entry| {
                entry.kind == VfsObjectKind::File
                    && entry.name.ends_with(&format!(".{}", self.extension))
            })
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| left.path.cmp(&right.path));
        let mut values = Vec::with_capacity(entries.len());
        for entry in entries {
            let bytes = self.handle.read(&entry.path)?;
            values.push(self.decode(&entry.path, &bytes)?);
        }
        Ok(values)
    }

    pub fn load(&self, id: &str) -> Result<T> {
        let path = self.path_for_id(id)?;
        let bytes = self.handle.read(&path)?;
        self.decode(&path, &bytes)
    }

    pub fn load_optional(&self, id: &str) -> Result<Option<T>> {
        let path = self.path_for_id(id)?;
        let Some(bytes) = self.handle.read_optional(&path)? else {
            return Ok(None);
        };
        self.decode(&path, &bytes).map(Some)
    }

    pub fn save(&self, id: &str, value: &T) -> Result<String> {
        self.save_with_options(id, value, VfsWriteOptions::service_artifact())
    }

    pub fn save_with_options(
        &self,
        id: &str,
        value: &T,
        options: VfsWriteOptions,
    ) -> Result<String> {
        let path = self.path_for_id(id)?;
        self.handle.create_dir_all("")?;
        let bytes = self.encode(&path, value)?;
        self.handle.write_with_options(&path, bytes, options)?;
        Ok(join_vfs_path(self.handle.root(), &path))
    }

    pub fn delete(&self, id: &str) -> Result<bool> {
        let path = self.path_for_id(id)?;
        match self
            .handle
            .remove_with_options(&path, VfsRemoveOptions::file())
        {
            Ok(()) => Ok(true),
            Err(VfsError::NotFound { .. }) => Ok(false),
            Err(source) => Err(source),
        }
    }

    fn encode(&self, path: &str, value: &T) -> Result<Vec<u8>> {
        match self.codec {
            DirectoryObjectCodec::Json => if self.pretty {
                serde_json::to_vec_pretty(value)
            } else {
                serde_json::to_vec(value)
            }
            .map_err(|source| VfsError::Json {
                path: path.to_string(),
                detail: source.to_string(),
            }),
            DirectoryObjectCodec::Sobj {
                version,
                length_mode,
            } => {
                serde_ext::to_bytes(value, version, length_mode).map_err(|source| VfsError::Sobj {
                    path: path.to_string(),
                    detail: source.to_string(),
                })
            }
        }
    }

    fn decode(&self, path: &str, bytes: &[u8]) -> Result<T> {
        match self.codec {
            DirectoryObjectCodec::Json => {
                serde_json::from_slice(bytes).map_err(|source| VfsError::Json {
                    path: path.to_string(),
                    detail: source.to_string(),
                })
            }
            DirectoryObjectCodec::Sobj { .. } => {
                serde_ext::from_bytes(bytes).map_err(|source| VfsError::Sobj {
                    path: path.to_string(),
                    detail: source.to_string(),
                })
            }
        }
    }
}

fn normalize_absolute_path(path: &str) -> Result<String> {
    if !path.starts_with('/') {
        return Err(VfsError::InvalidPath {
            path: path.to_string(),
            detail: "path must be absolute".to_string(),
        });
    }
    let mut segments = Vec::new();
    for segment in path.split('/') {
        match segment {
            "" | "." => {}
            ".." => {
                return Err(VfsError::InvalidPath {
                    path: path.to_string(),
                    detail: "parent traversal is not allowed".to_string(),
                });
            }
            segment => segments.push(segment),
        }
    }
    if segments.is_empty() {
        Ok("/".to_string())
    } else {
        Ok(format!("/{}", segments.join("/")))
    }
}

fn join_vfs_path(root: &str, child: &str) -> String {
    if root == "/" {
        format!("/{}", child.trim_start_matches('/'))
    } else if child.is_empty() {
        root.to_string()
    } else {
        format!(
            "{}/{}",
            root.trim_end_matches('/'),
            child.trim_start_matches('/')
        )
    }
}

fn parent_vfs_path(path: &str) -> Option<String> {
    if path == "/" {
        return None;
    }
    let trimmed = path.trim_end_matches('/');
    let index = trimmed.rfind('/')?;
    if index == 0 {
        Some("/".to_string())
    } else {
        Some(trimmed[..index].to_string())
    }
}

fn file_name(path: &str) -> Option<&str> {
    path.trim_end_matches('/').rsplit('/').next()
}

fn validate_object_id(id: &str) -> Result<()> {
    if id.is_empty() || id.contains('/') || id == "." || id == ".." {
        return Err(VfsError::InvalidPath {
            path: id.to_string(),
            detail: "object id must be a single path segment".to_string(),
        });
    }
    Ok(())
}

fn normalize_extension(extension: String) -> String {
    extension.trim_start_matches('.').to_string()
}

fn local_file_kind(metadata: &fs::Metadata) -> VfsObjectKind {
    if metadata.is_file() {
        VfsObjectKind::File
    } else if metadata.is_dir() {
        VfsObjectKind::Directory
    } else if metadata.file_type().is_symlink() {
        VfsObjectKind::Symlink
    } else {
        VfsObjectKind::Other
    }
}

fn enforce_local_create_mode(
    path: &str,
    host_path: &Path,
    options: &VfsWriteOptions,
) -> Result<()> {
    let exists = host_path.exists();
    enforce_create_mode(path, exists, options)
}

fn enforce_memory_create_mode(
    nodes: &BTreeMap<String, MemoryNode>,
    path: &str,
    options: &VfsWriteOptions,
) -> Result<()> {
    enforce_create_mode(path, nodes.contains_key(path), options)
}

fn enforce_create_mode(path: &str, exists: bool, options: &VfsWriteOptions) -> Result<()> {
    match options.create_mode {
        VfsCreateMode::CreateNew if exists => Err(VfsError::AlreadyExists {
            path: path.to_string(),
        }),
        VfsCreateMode::ReplaceExisting if !exists => Err(VfsError::NotFound {
            path: path.to_string(),
        }),
        _ => Ok(()),
    }
}

fn memory_kind(node: &MemoryNode) -> VfsObjectKind {
    match node {
        MemoryNode::File(_) => VfsObjectKind::File,
        MemoryNode::Directory => VfsObjectKind::Directory,
    }
}

fn memory_metadata(path: &str, node: &MemoryNode) -> VfsMetadata {
    VfsMetadata {
        path: path.to_string(),
        kind: memory_kind(node),
        object_id: None,
        size_bytes: match node {
            MemoryNode::File(bytes) => Some(bytes.len() as u64),
            MemoryNode::Directory => None,
        },
        content_hash: None,
        version: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[test]
    fn scoped_handle_rejects_parent_escape() {
        let handle = VfsHandle::memory()
            .scoped("/services/sinomo/agents")
            .unwrap();
        assert!(matches!(
            handle.write("../outside.txt", b"bad"),
            Err(VfsError::InvalidPath { .. })
        ));
        assert!(matches!(
            handle.write("/services/sinomo/other/file.txt", b"bad"),
            Err(VfsError::OutsideScope { .. })
        ));
    }

    #[test]
    fn no_link_beneath_primitives_fail_closed_on_unsupported_backends() {
        let handle = VfsHandle::memory().scoped("/scope").unwrap();
        assert!(matches!(
            handle.stat_beneath_no_links("note.md"),
            Err(VfsError::Unsupported { .. })
        ));
        assert!(matches!(
            handle.read_prefix_beneath_no_links("note.md", 16),
            Err(VfsError::Unsupported { .. })
        ));
        assert!(matches!(
            handle.read_range_beneath_no_links("note.md", 4, 16),
            Err(VfsError::Unsupported { .. })
        ));
        assert!(matches!(
            handle.list_page_beneath_no_links("", None, 16),
            Err(VfsError::Unsupported { .. })
        ));
        assert!(matches!(
            handle.create_new_versioned_beneath_no_links(
                "note.md",
                b"note",
                VfsWriteOptions::default().create_new(),
            ),
            Err(VfsError::Unsupported { .. })
        ));
        assert!(matches!(
            handle.create_dir_all_beneath_no_links("nested"),
            Err(VfsError::Unsupported { .. })
        ));
        assert!(matches!(
            handle.replace_if_version_beneath_no_links(
                "note.md",
                "object",
                "version",
                b"updated",
                VfsWriteOptions::default().replace_existing(),
            ),
            Err(VfsError::Unsupported { .. })
        ));
        assert!(matches!(
            handle.read_prefix_beneath_no_links("/outside/note.md", 16),
            Err(VfsError::OutsideScope { .. })
        ));
        assert!(matches!(
            handle.read_range_beneath_no_links("/outside/note.md", 0, 16),
            Err(VfsError::OutsideScope { .. })
        ));
        assert!(matches!(
            handle.stat_beneath_no_links("/outside/note.md"),
            Err(VfsError::OutsideScope { .. })
        ));
    }

    #[test]
    fn memory_backend_reads_writes_lists_and_renames() {
        let handle = VfsHandle::memory()
            .scoped("/services/sinomo/agents")
            .unwrap();
        handle.create_dir_all("").unwrap();
        handle.write("alpha.txt", b"abcdef").unwrap();
        assert_eq!(handle.read("alpha.txt").unwrap(), b"abcdef");
        assert_eq!(
            handle.read_prefix("alpha.txt", 3).unwrap(),
            VfsFileRead {
                bytes: b"abc".to_vec(),
                truncated: true
            }
        );
        let entries = handle.list("").unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "alpha.txt");
        handle.rename("alpha.txt", "beta.txt").unwrap();
        assert!(matches!(
            handle.read("alpha.txt"),
            Err(VfsError::NotFound { .. })
        ));
        assert_eq!(handle.read("beta.txt").unwrap(), b"abcdef");
    }

    #[test]
    fn directory_pages_are_ordered_opaque_and_bound_to_the_resolved_directory() {
        let root = VfsHandle::memory();
        root.create_dir_all("/scope/notes").unwrap();
        root.create_dir_all("/scope/other").unwrap();
        for name in ["charlie.md", "alpha.md", "bravo.md"] {
            root.write(format!("/scope/notes/{name}"), name.as_bytes())
                .unwrap();
        }
        let scoped = root.scoped("/scope").unwrap();

        let first = scoped.list_page("notes", None, 2).unwrap();
        assert_eq!(
            first
                .entries
                .iter()
                .map(|entry| entry.name.as_str())
                .collect::<Vec<_>>(),
            vec!["alpha.md", "bravo.md"]
        );
        let encoded = first
            .next_cursor
            .as_ref()
            .expect("continuation")
            .to_opaque_string()
            .expect("encode cursor");
        let restored = VfsDirectoryCursor::from_opaque_string(&encoded).expect("decode cursor");
        let second = scoped.list_page("notes", Some(&restored), 2).unwrap();
        assert_eq!(
            second
                .entries
                .iter()
                .map(|entry| entry.name.as_str())
                .collect::<Vec<_>>(),
            vec!["charlie.md"]
        );
        assert!(second.next_cursor.is_none());

        assert!(matches!(
            scoped.list_page("other", Some(&restored), 2),
            Err(VfsError::InvalidCursor { .. })
        ));
        assert!(matches!(
            scoped.list_page("/outside", Some(&restored), 2),
            Err(VfsError::OutsideScope { .. })
        ));
        assert!(matches!(
            scoped.list_page("notes", None, 0),
            Err(VfsError::InvalidCursor { .. })
        ));
    }

    #[test]
    fn memory_backend_appends_bytes_generically() {
        let handle = VfsHandle::memory().scoped("/logs").unwrap();
        handle.create_dir_all("").unwrap();
        handle.append("events.log", b"one\n").unwrap();
        handle.append("events.log", b"two\n").unwrap();
        assert_eq!(handle.read("events.log").unwrap(), b"one\ntwo\n");

        assert!(matches!(
            handle.append_with_options(
                "missing.log",
                b"nope",
                VfsAppendOptions::local().without_create()
            ),
            Err(VfsError::NotFound { .. })
        ));
    }

    #[test]
    fn write_create_modes_are_enforced() {
        let handle = VfsHandle::memory().scoped("/configs").unwrap();
        handle.create_dir_all("").unwrap();
        handle
            .write_with_options(
                "agent.json",
                b"{}",
                VfsWriteOptions::service_artifact().create_new(),
            )
            .unwrap();
        assert!(matches!(
            handle.write_with_options(
                "agent.json",
                b"{}",
                VfsWriteOptions::service_artifact().create_new()
            ),
            Err(VfsError::AlreadyExists { .. })
        ));
        handle
            .write_with_options(
                "agent.json",
                b"{\"ok\":true}",
                VfsWriteOptions::service_artifact().replace_existing(),
            )
            .unwrap();
    }

    #[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
    struct TestObject {
        id: String,
        value: u32,
    }

    #[test]
    fn directory_object_store_round_trips_json() {
        let handle = VfsHandle::memory()
            .scoped("/services/sinomo/agents")
            .unwrap();
        let store = DirectoryObjectStore::<TestObject>::json(handle, "json");
        store
            .save(
                "agent-one",
                &TestObject {
                    id: "agent-one".to_string(),
                    value: 7,
                },
            )
            .unwrap();
        assert_eq!(
            store.load_all().unwrap(),
            vec![TestObject {
                id: "agent-one".to_string(),
                value: 7
            }]
        );
        assert!(store.delete("agent-one").unwrap());
        assert!(!store.delete("agent-one").unwrap());
        assert!(store.load_all().unwrap().is_empty());
    }

    #[test]
    fn local_backend_maps_absolute_vfs_paths_under_host_root() {
        let test_dir = sinomo_testdir::TestDir::new("sinomo-vfs-local");
        let handle = VfsHandle::local(test_dir.path()).unwrap();
        let scoped = handle.scoped("/services/sinomo/agents").unwrap();
        scoped.create_dir_all("").unwrap();
        scoped.write("agent.json", b"{}").unwrap();
        let host_path = test_dir
            .path()
            .join("services")
            .join("sinomo")
            .join("agents")
            .join("agent.json");
        assert_eq!(fs::read(host_path).unwrap(), b"{}");
    }

    #[test]
    fn local_backend_appends_under_host_root() {
        let test_dir = sinomo_testdir::TestDir::new("sinomo-vfs-local-append");
        let handle = VfsHandle::local(test_dir.path()).unwrap();
        handle.create_dir_all("/events").unwrap();
        handle
            .append_with_options("/events/trace.jsonl", b"first\n", VfsAppendOptions::local())
            .unwrap();
        handle
            .append_with_options(
                "/events/trace.jsonl",
                b"second\n",
                VfsAppendOptions::local(),
            )
            .unwrap();
        assert_eq!(
            fs::read(test_dir.path().join("events").join("trace.jsonl")).unwrap(),
            b"first\nsecond\n"
        );
    }
}
