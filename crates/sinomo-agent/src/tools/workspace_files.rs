//! Workspace file access through the VFS.
//!
//! Every byte an agent tool reads from or writes to a granted workspace goes
//! through this module. Each grant is opened once as a confined VFS handle:
//! host-local grants use [`ConfinedLocalVfsBackend`] (links may be followed
//! only while they stay inside the grant), and grants that live on the cluster
//! SIFS root use the SIFS VFS directly, scoped to the grant's SIFS path, rather
//! than the host FUSE projection. Tool inputs still name files by the host
//! path commands see (or relative to the primary grant); this module maps
//! those names onto `(grant, VFS path)` lexically and never performs ambient
//! filesystem I/O itself.

#![deny(clippy::disallowed_methods, clippy::disallowed_types)]

use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::vfs::{
    ConfinedLocalVfsBackend, VfsCreateMode, VfsDirEntry, VfsError, VfsFileRead, VfsHandle,
    VfsMetadata, VfsObjectKind, VfsRemoveOptions, VfsWriteOptions, mounted_sifs,
};
use std::fmt;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

const READ_ALL_CHUNK_BYTES: u64 = 1024 * 1024;

/// Lazily opened SIFS VFS used for grants that live on the cluster SIFS root.
#[derive(Clone)]
pub struct WorkspaceSifsVfs {
    source: WorkspaceSifsSource,
    opened: Arc<OnceLock<Result<VfsHandle, String>>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum WorkspaceSifsSource {
    Mounted(PathBuf),
    Provided(u64),
}

impl WorkspaceSifsVfs {
    /// Open the node's mounted SIFS service for `data_dir` on first use.
    pub fn mounted(data_dir: &SinomoDataDir) -> Self {
        Self {
            source: WorkspaceSifsSource::Mounted(data_dir.root().to_path_buf()),
            opened: Arc::new(OnceLock::new()),
        }
    }

    /// Use an already-open SIFS handle (rooted at the SIFS root).
    pub fn from_handle(handle: VfsHandle) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        Self {
            source: WorkspaceSifsSource::Provided(NEXT.fetch_add(1, Ordering::Relaxed)),
            opened: Arc::new(OnceLock::from(Ok(handle))),
        }
    }

    fn handle(&self) -> Result<VfsHandle, String> {
        self.opened
            .get_or_init(|| match &self.source {
                WorkspaceSifsSource::Mounted(data_dir) => mounted_sifs(Some(data_dir.clone()))
                    .map_err(|source| format!("open mounted SIFS: {source}")),
                WorkspaceSifsSource::Provided(_) => Err("SIFS handle was not provided".to_string()),
            })
            .clone()
    }
}

impl PartialEq for WorkspaceSifsVfs {
    fn eq(&self, other: &Self) -> bool {
        self.source == other.source
    }
}

impl Eq for WorkspaceSifsVfs {}

impl fmt::Debug for WorkspaceSifsVfs {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkspaceSifsVfs")
            .field("source", &self.source)
            .finish_non_exhaustive()
    }
}

/// How one grant is reached: the host path commands see, and where it lives
/// on SIFS when the grant is under the live SIFS mount.
#[derive(Clone, Debug)]
pub struct WorkspaceGrantSpec {
    pub host_root: PathBuf,
    pub sifs_root: Option<String>,
}

impl WorkspaceGrantSpec {
    pub fn local(host_root: impl Into<PathBuf>) -> Self {
        Self {
            host_root: host_root.into(),
            sifs_root: None,
        }
    }

    /// Classify `host_root` against the live SIFS mount: a root beneath the
    /// mount names the SIFS directory at the same relative path.
    pub fn classify(host_root: impl Into<PathBuf>, sifs_mount_root: Option<&Path>) -> Self {
        let host_root = host_root.into();
        let sifs_root = sifs_mount_root.and_then(|mount| {
            let relative = strip_root_prefix(
                &lexical_components(&host_root)?,
                &lexical_components(mount)?,
            )?;
            Some(format!("/{}", relative.join("/")))
        });
        Self {
            host_root,
            sifs_root,
        }
    }
}

struct OpenGrant {
    /// Name for the grant root in tool inputs, outputs, and command cwd.
    host_root: PathBuf,
    /// Lexical forms an absolute tool path may use to name the root.
    match_roots: Vec<Vec<String>>,
    vfs: VfsHandle,
}

/// A file or directory inside one grant.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct WorkspaceFile {
    grant: usize,
    /// Absolute VFS path within the grant (`/` is the grant root).
    path: String,
}

impl WorkspaceFile {
    pub fn is_primary(&self) -> bool {
        self.grant == 0
    }

    pub fn vfs_path(&self) -> &str {
        &self.path
    }

    pub fn file_name(&self) -> Option<&str> {
        self.path.rsplit('/').next().filter(|name| !name.is_empty())
    }

    pub fn child(&self, name: &str) -> Self {
        Self {
            grant: self.grant,
            path: if self.path == "/" {
                format!("/{name}")
            } else {
                format!("{}/{name}", self.path)
            },
        }
    }

    pub fn parent(&self) -> Option<Self> {
        if self.path == "/" {
            return None;
        }
        let index = self.path.rfind('/')?;
        Some(Self {
            grant: self.grant,
            path: if index == 0 {
                "/".to_string()
            } else {
                self.path[..index].to_string()
            },
        })
    }
}

/// Open workspace grants for one tool call.
pub struct WorkspaceFiles {
    grants: Vec<OpenGrant>,
}

impl WorkspaceFiles {
    /// Open grants in order; the first is the primary grant every relative path
    /// resolves against. The primary grant must open; an additional grant that
    /// cannot be opened is skipped, so it simply cannot be addressed.
    pub fn open(
        grants: &[WorkspaceGrantSpec],
        sifs: Option<&WorkspaceSifsVfs>,
    ) -> Result<Self, String> {
        let mut opened = Vec::new();
        for (index, spec) in grants.iter().enumerate() {
            match open_grant(spec, sifs) {
                Ok(grant) => opened.push(grant),
                Err(detail) if index == 0 => return Err(detail),
                Err(_) => {}
            }
        }
        if opened.is_empty() {
            return Err("no workspace grant is configured".to_string());
        }
        Ok(Self { grants: opened })
    }

    /// Convenience for a single host-local root.
    pub fn open_local(root: &Path) -> Result<Self, String> {
        Self::open(&[WorkspaceGrantSpec::local(root)], None)
    }

    pub fn primary_root(&self) -> &Path {
        &self.grants[0].host_root
    }

    pub fn primary(&self) -> WorkspaceFile {
        WorkspaceFile {
            grant: 0,
            path: "/".to_string(),
        }
    }

    /// Resolve a tool path. Relative paths name the primary grant; absolute
    /// paths name whichever grant root they start with. `.` and `..` are
    /// resolved lexically and may not climb above the grant root.
    pub fn resolve(&self, requested: &str) -> Result<WorkspaceFile, String> {
        let requested = requested.trim();
        if is_absolute_tool_path(requested) {
            let components = sandbox_drive_components(requested)
                .or_else(|| lexical_components(Path::new(requested)))
                .ok_or_else(|| format!("path {requested} is not a valid workspace path"))?;
            let mut best: Option<(usize, usize, Vec<String>)> = None;
            for (grant, open) in self.grants.iter().enumerate() {
                for root in &open.match_roots {
                    if let Some(rest) = strip_root_prefix(&components, root)
                        && best
                            .as_ref()
                            .is_none_or(|(_, depth, _)| root.len() > *depth)
                    {
                        best = Some((grant, root.len(), rest));
                    }
                }
            }
            let (grant, _, rest) = best.ok_or_else(|| {
                format!(
                    "path {requested} is outside granted workspace roots (primary: {})",
                    self.primary_root().display()
                )
            })?;
            return Ok(WorkspaceFile {
                grant,
                path: vfs_path(&rest),
            });
        }
        let segments = relative_segments(requested).ok_or_else(|| {
            format!(
                "path {requested} escapes workspace root {}",
                self.primary_root().display()
            )
        })?;
        Ok(WorkspaceFile {
            grant: 0,
            path: vfs_path(&segments),
        })
    }

    /// Resolve a path that must be relative to the primary grant and contain
    /// only normal components (write tools).
    pub fn resolve_primary_relative(&self, requested: &str) -> Result<WorkspaceFile, String> {
        let requested = requested.trim();
        if requested.is_empty() {
            return Err("path must not be empty".to_string());
        }
        if is_absolute_tool_path(requested) {
            return Err(format!(
                "path `{requested}` must be relative to the workspace root"
            ));
        }
        let mut segments = Vec::new();
        for component in Path::new(requested).components() {
            match component {
                Component::Normal(name) => segments.push(
                    name.to_str()
                        .ok_or_else(|| format!("path `{requested}` is not valid UTF-8"))?
                        .to_string(),
                ),
                Component::CurDir => {}
                _ => {
                    return Err(format!(
                        "path `{requested}` contains unsupported or escaping components"
                    ));
                }
            }
        }
        if segments.is_empty() {
            return Err(format!("path `{requested}` names the workspace root"));
        }
        Ok(WorkspaceFile {
            grant: 0,
            path: vfs_path(&segments),
        })
    }

    /// The host path commands see for `file`. Naming only; never used for I/O.
    pub fn host_path(&self, file: &WorkspaceFile) -> PathBuf {
        let mut path = self.grants[file.grant].host_root.clone();
        for segment in file.path.split('/').filter(|segment| !segment.is_empty()) {
            path.push(segment);
        }
        path
    }

    /// Model-facing name: relative to the primary grant (with `/`
    /// separators), otherwise the grant's host path.
    pub fn display(&self, file: &WorkspaceFile) -> String {
        if file.grant == 0 {
            let relative = file.path.trim_start_matches('/');
            if relative.is_empty() {
                ".".to_string()
            } else {
                relative.to_string()
            }
        } else {
            self.host_path(file).display().to_string()
        }
    }

    fn vfs(&self, file: &WorkspaceFile) -> &VfsHandle {
        &self.grants[file.grant].vfs
    }

    /// Metadata, following a final link that stays inside the grant.
    /// `Ok(None)` means nothing exists at `file`.
    pub fn stat(&self, file: &WorkspaceFile) -> Result<Option<VfsMetadata>, VfsError> {
        match self.vfs(file).stat_confined(relative(&file.path)) {
            Ok(metadata) => Ok(Some(metadata)),
            Err(VfsError::NotFound { .. }) => Ok(None),
            Err(source) => Err(source),
        }
    }

    pub fn is_dir(&self, file: &WorkspaceFile) -> bool {
        matches!(
            self.stat(file),
            Ok(Some(VfsMetadata {
                kind: VfsObjectKind::Directory,
                ..
            }))
        )
    }

    /// Directory entries sorted by name. Entry kinds describe the entry itself.
    pub fn list(&self, dir: &WorkspaceFile) -> Result<Vec<(WorkspaceFile, VfsDirEntry)>, VfsError> {
        let mut entries = self.vfs(dir).list_confined(relative(&dir.path))?;
        entries.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(entries
            .into_iter()
            .map(|entry| (dir.child(&entry.name), entry))
            .collect())
    }

    pub fn read_prefix(
        &self,
        file: &WorkspaceFile,
        max_bytes: u64,
    ) -> Result<VfsFileRead, VfsError> {
        self.vfs(file)
            .read_range_confined(relative(&file.path), 0, max_bytes)
    }

    pub fn read_all(&self, file: &WorkspaceFile) -> Result<Vec<u8>, VfsError> {
        let mut bytes = Vec::new();
        loop {
            let chunk = self.vfs(file).read_range_confined(
                relative(&file.path),
                bytes.len() as u64,
                READ_ALL_CHUNK_BYTES,
            )?;
            bytes.extend_from_slice(&chunk.bytes);
            if !chunk.truncated || chunk.bytes.is_empty() {
                return Ok(bytes);
            }
        }
    }

    pub fn write(
        &self,
        file: &WorkspaceFile,
        bytes: &[u8],
        create_mode: VfsCreateMode,
    ) -> Result<(), VfsError> {
        let mut options = VfsWriteOptions::local_replace();
        options.create_mode = create_mode;
        self.vfs(file)
            .write_confined(relative(&file.path), bytes, options)
    }

    pub fn create_dir_all(&self, dir: &WorkspaceFile) -> Result<(), VfsError> {
        self.vfs(dir).create_dir_all_confined(relative(&dir.path))
    }

    pub fn remove_file(&self, file: &WorkspaceFile) -> Result<(), VfsError> {
        self.vfs(file)
            .remove_confined(relative(&file.path), VfsRemoveOptions::file())
    }

    pub fn rename(&self, from: &WorkspaceFile, to: &WorkspaceFile) -> Result<(), VfsError> {
        if from.grant != to.grant {
            return Err(VfsError::Unsupported {
                backend: "workspace".to_string(),
                detail: "cannot move files between workspace grants".to_string(),
            });
        }
        self.vfs(from)
            .rename_confined(relative(&from.path), relative(&to.path))
    }
}

fn open_grant(
    spec: &WorkspaceGrantSpec,
    sifs: Option<&WorkspaceSifsVfs>,
) -> Result<OpenGrant, String> {
    let mut match_roots = Vec::new();
    if let Some(components) = lexical_components(&spec.host_root) {
        match_roots.push(components);
    }
    let (vfs, host_root) = match (spec.sifs_root.as_deref(), sifs) {
        (Some(sifs_root), Some(sifs)) => {
            let handle = sifs
                .handle()?
                .scoped(sifs_root)
                .map_err(|source| format!("scope SIFS workspace {sifs_root}: {source}"))?;
            (handle, spec.host_root.clone())
        }
        _ => {
            let backend = ConfinedLocalVfsBackend::open(&spec.host_root).map_err(|source| {
                format!(
                    "workspace root {} is not accessible: {source}",
                    spec.host_root.display()
                )
            })?;
            let canonical = backend.canonical_root().to_path_buf();
            if let Some(components) = lexical_components(&canonical)
                && !match_roots.contains(&components)
            {
                match_roots.push(components);
            }
            let handle = VfsHandle::new(Arc::new(backend), "/")
                .map_err(|source| format!("open workspace VFS: {source}"))?;
            (handle, canonical)
        }
    };
    Ok(OpenGrant {
        host_root,
        match_roots,
        vfs,
    })
}

fn relative(path: &str) -> &str {
    let trimmed = path.trim_start_matches('/');
    if trimmed.is_empty() { "." } else { trimmed }
}

fn vfs_path(segments: &[String]) -> String {
    if segments.is_empty() {
        "/".to_string()
    } else {
        format!("/{}", segments.join("/"))
    }
}

fn is_absolute_tool_path(path: &str) -> bool {
    Path::new(path).is_absolute() || path.starts_with('/') || path.starts_with('\\')
}

/// Lexical components of an absolute host path: a prefix token (Windows drive
/// or UNC share, lower-cased) followed by normal names with `.`/`..` applied.
fn lexical_components(path: &Path) -> Option<Vec<String>> {
    let raw = path.to_str()?;
    let unverbatim = match raw.strip_prefix(r"\\?\UNC\") {
        Some(rest) => format!(r"\\{rest}"),
        None => raw.strip_prefix(r"\\?\").unwrap_or(raw).to_string(),
    };
    let path = Path::new(&unverbatim);
    let mut components = Vec::new();
    let mut saw_root = false;
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => {
                components.push(normalize_case(&prefix.as_os_str().to_string_lossy()));
            }
            Component::RootDir => saw_root = true,
            Component::CurDir => {}
            Component::ParentDir => {
                if components.len() > usize::from(cfg!(windows)) {
                    components.pop();
                }
            }
            Component::Normal(name) => components.push(normalize_case(name.to_str()?)),
        }
    }
    saw_root.then_some(components)
}

/// `/D:/x/y`, the sandbox's spelling of a Windows host path.
fn sandbox_drive_components(path: &str) -> Option<Vec<String>> {
    let rest = path.strip_prefix('/')?;
    let bytes = rest.as_bytes();
    if bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':' {
        lexical_components(Path::new(&rest.replace('/', "\\")))
    } else {
        None
    }
}

fn normalize_case(segment: &str) -> String {
    if cfg!(windows) {
        segment.to_lowercase()
    } else {
        segment.to_string()
    }
}

fn strip_root_prefix(path: &[String], root: &[String]) -> Option<Vec<String>> {
    if path.len() < root.len() || path[..root.len()] != *root {
        return None;
    }
    Some(path[root.len()..].to_vec())
}

fn relative_segments(requested: &str) -> Option<Vec<String>> {
    let mut segments: Vec<String> = Vec::new();
    for component in Path::new(requested).components() {
        match component {
            Component::Normal(name) => segments.push(name.to_str()?.to_string()),
            Component::CurDir => {}
            Component::ParentDir => {
                segments.pop()?;
            }
            Component::RootDir | Component::Prefix(_) => return None,
        }
    }
    Some(segments)
}

#[cfg(test)]
#[allow(clippy::disallowed_methods, clippy::disallowed_types)]
mod tests {
    use super::*;

    #[test]
    fn resolves_relative_and_absolute_paths_against_grants() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-workspace-files-resolve");
        std::fs::create_dir_all(root.join("primary/src")).expect("primary");
        std::fs::create_dir_all(root.join("extra")).expect("extra");
        let files = WorkspaceFiles::open(
            &[
                WorkspaceGrantSpec::local(root.join("primary")),
                WorkspaceGrantSpec::local(root.join("extra")),
            ],
            None,
        )
        .expect("open grants");

        let relative = files.resolve("src/../src/lib.rs").expect("relative");
        assert!(relative.is_primary());
        assert_eq!(relative.vfs_path(), "/src/lib.rs");
        assert_eq!(files.display(&relative), "src/lib.rs");
        assert!(files.resolve("../outside").is_err());

        let absolute = root.join("extra").join("notes.md");
        let extra = files
            .resolve(&absolute.display().to_string())
            .expect("absolute in extra grant");
        assert!(!extra.is_primary());
        assert_eq!(extra.vfs_path(), "/notes.md");
        assert!(
            files
                .resolve(&root.join("elsewhere").display().to_string())
                .is_err()
        );
        assert_eq!(files.display(&files.resolve(".").expect("root")), ".");
    }

    #[test]
    fn reads_and_writes_through_the_confined_vfs() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-workspace-files-io");
        let files = WorkspaceFiles::open_local(root.path()).expect("open");
        let file = files.resolve_primary_relative("dir/a.txt").expect("path");
        files
            .create_dir_all(&file.parent().expect("parent"))
            .expect("mkdir");
        files
            .write(&file, b"hello", VfsCreateMode::CreateNew)
            .expect("write");
        assert_eq!(files.read_all(&file).expect("read"), b"hello");
        assert_eq!(
            std::fs::read(root.join("dir").join("a.txt")).expect("host"),
            b"hello"
        );
        assert!(files.resolve_primary_relative("../x").is_err());
        assert!(files.resolve_primary_relative("/abs").is_err());
    }

    #[test]
    fn classifies_grants_under_the_sifs_mount() {
        let spec = WorkspaceGrantSpec::classify(
            Path::new("/mnt/sifs/conversations/c1/workspaces/project"),
            Some(Path::new("/mnt/sifs")),
        );
        assert_eq!(
            spec.sifs_root.as_deref(),
            Some("/conversations/c1/workspaces/project")
        );
        assert_eq!(
            WorkspaceGrantSpec::classify(
                Path::new("/home/me/project"),
                Some(Path::new("/mnt/sifs"))
            )
            .sifs_root,
            None
        );
    }

    #[test]
    fn sifs_grants_read_and_write_through_the_sifs_vfs() {
        let memory = VfsHandle::memory();
        memory
            .create_dir_all("/workspaces/project")
            .expect("workspace dir");
        let sifs = WorkspaceSifsVfs::from_handle(memory.clone());
        let files = WorkspaceFiles::open(
            &[WorkspaceGrantSpec {
                host_root: PathBuf::from("/no/fuse/mount/project"),
                sifs_root: Some("/workspaces/project".to_string()),
            }],
            Some(&sifs),
        )
        .expect("open SIFS grant without a host mount");
        let file = files.resolve_primary_relative("notes.md").expect("path");
        files
            .write(&file, b"on sifs", VfsCreateMode::CreateOrReplace)
            .expect("write");
        assert_eq!(
            memory
                .read("/workspaces/project/notes.md")
                .expect("sifs bytes"),
            b"on sifs"
        );
    }
}
