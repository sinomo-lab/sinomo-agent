//! Canonical SIFS sources with node-local, rebuildable memory indexes.
use super::types::*;
use crate::MemoryError;
use sinomo_runtime::api::{DaemonApiClient, DaemonPaths};
use sinomo_runtime::core::Id;
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::vfs::{
    VfsDirectoryCursor, VfsError, VfsHandle, VfsMetadata, VfsObjectKind,
    discovered_daemon_sifs_in_cluster,
};
use std::path::{Component, Path, PathBuf};

pub(super) const CONNECTOR_ID: &str = "sinomo.memory.sifs/v1";
const MAX_SCAN_PAGE_SIZE: usize = 256;
const MAX_SCAN_DEPTH: usize = 128;

#[derive(Clone, Debug)]
enum Source {
    Daemon(SinomoDataDir),
    AuthorizedVfs {
        filesystem: VfsHandle,
        namespace: String,
    },
}

#[derive(Clone, Debug)]
pub struct SifsSourceConnector {
    source: Source,
}

fn vfs_error(path: &str, error: impl std::fmt::Display) -> MemoryError {
    MemoryError::Vfs {
        path: path.into(),
        detail: error.to_string(),
    }
}

impl SifsSourceConnector {
    /// Construction does no I/O. Local-only memory operations need no daemon.
    pub fn new(data_dir: SinomoDataDir) -> Self {
        Self {
            source: Source::Daemon(data_dir),
        }
    }

    pub fn from_vfs(filesystem: VfsHandle, namespace: impl Into<String>) -> Self {
        Self {
            source: Source::AuthorizedVfs {
                filesystem,
                namespace: namespace.into(),
            },
        }
    }

    fn filesystem(&self) -> Result<(VfsHandle, String), MemoryError> {
        match &self.source {
            Source::AuthorizedVfs {
                filesystem,
                namespace,
            } => {
                if namespace.is_empty() || namespace.len() > 512 || namespace.contains('\0') {
                    return Err(MemoryError::InvalidIndexRoot {
                        path: filesystem.root().into(),
                        detail:
                            "an injected filesystem requires an explicit bounded namespace identity"
                                .into(),
                    });
                }
                Ok((filesystem.clone(), format!("injected:{namespace}")))
            }
            Source::Daemon(data_dir) => {
                let client = DaemonApiClient::connect_for_paths(&DaemonPaths::new(data_dir))
                    .map_err(|error| vfs_error("/", error))?;
                let cluster = client
                    .status()
                    .map_err(|error| vfs_error("/", error))?
                    .cluster
                    .ok_or_else(|| MemoryError::InvalidIndexRoot {
                        path: "/".into(),
                        detail: "SIFS memory sources require an authenticated cluster namespace"
                            .into(),
                    })?;
                let namespace = format!("cluster:{}", cluster.cluster_id);
                let filesystem = discovered_daemon_sifs_in_cluster(
                    data_dir.clone(),
                    client.address(),
                    cluster.cluster_id,
                )
                .map_err(|error| vfs_error("/", error))?;
                Ok((filesystem, namespace))
            }
        }
    }

    pub(super) fn registered_root_identity(
        &self,
        path: &Path,
    ) -> Result<(String, Id), MemoryError> {
        let path = path
            .to_str()
            .filter(|path| path.starts_with('/'))
            .ok_or_else(|| MemoryError::InvalidIndexRoot {
                path: path.display().to_string(),
                detail: "SIFS index root must be an absolute UTF-8 namespace path".into(),
            })?;
        let (filesystem, namespace) = self.filesystem()?;
        let scoped = filesystem
            .scoped(path)
            .map_err(|error| vfs_error(path, error))?;
        let metadata = scoped
            .stat_beneath_no_links("")
            .map_err(|error| vfs_error(path, error))?;
        if metadata.kind != VfsObjectKind::Directory {
            return Err(MemoryError::InvalidIndexRoot {
                path: path.into(),
                detail: "SIFS index root is not a directory".into(),
            });
        }
        let object = stable_object(&metadata)?;
        let path = scoped.root().to_string();
        Ok((path.clone(), root_id(&namespace, &path, object)))
    }

    fn scoped(&self, root: &IndexRoot) -> Result<VfsHandle, MemoryError> {
        if root.connector_id != CONNECTOR_ID {
            return Err(MemoryError::InvalidIndexRoot {
                path: root.config.path.display().to_string(),
                detail: "SIFS connector cannot open another source kind".into(),
            });
        }
        let path = root
            .config
            .path
            .to_str()
            .ok_or_else(|| MemoryError::InvalidIndexRoot {
                path: root.config.path.display().to_string(),
                detail: "SIFS root is not UTF-8".into(),
            })?;
        let (filesystem, namespace) = self.filesystem()?;
        let scoped = filesystem
            .scoped(path)
            .map_err(|error| vfs_error(path, error))?;
        let metadata = scoped
            .stat_beneath_no_links("")
            .map_err(|error| vfs_error(path, error))?;
        if metadata.kind != VfsObjectKind::Directory
            || root_id(&namespace, scoped.root(), stable_object(&metadata)?) != root.root_id
        {
            return Err(MemoryError::SourceChanged { path: path.into() });
        }
        Ok(scoped)
    }
}

fn root_id(namespace: &str, path: &str, object: &str) -> Id {
    Id::derived("memory-sifs-root", format!("{namespace}\0{path}\0{object}"))
}

fn stable_object(metadata: &VfsMetadata) -> Result<&str, MemoryError> {
    metadata
        .object_id
        .as_deref()
        .filter(|id| !id.is_empty())
        .ok_or_else(|| MemoryError::InvalidSourceContent {
            path: metadata.path.clone(),
            detail: "filesystem source has no stable object identity".into(),
        })
}

fn storage_version(metadata: &VfsMetadata) -> Result<&str, MemoryError> {
    metadata
        .version
        .as_deref()
        .filter(|version| !version.is_empty())
        .ok_or_else(|| MemoryError::InvalidSourceContent {
            path: metadata.path.clone(),
            detail: "filesystem source has no exact storage version".into(),
        })
}

fn version_hint(metadata: &VfsMetadata) -> Result<String, MemoryError> {
    Ok(format!(
        "{}\0{}\0{}",
        stable_object(metadata)?,
        storage_version(metadata)?,
        metadata
            .size_bytes
            .ok_or_else(|| MemoryError::InvalidSourceContent {
                path: metadata.path.clone(),
                detail: "filesystem source has no exact byte length".into(),
            })?
    ))
}

fn relative_path(path: &str) -> Result<&str, MemoryError> {
    if Path::new(path)
        .components()
        .all(|component| matches!(component, Component::Normal(_)))
    {
        Ok(path)
    } else {
        Err(MemoryError::InvalidSourceCandidate {
            path: path.into(),
            detail: "source locator must stay beneath its registered root".into(),
        })
    }
}

fn included(root: &IndexRoot, path: &str) -> bool {
    Path::new(path)
        .extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| {
            root.config
                .include_extensions
                .iter()
                .any(|candidate| candidate.eq_ignore_ascii_case(extension))
        })
}

fn candidate(
    root: &IndexRoot,
    relative: &str,
    metadata: &VfsMetadata,
) -> Result<SourceCandidate, MemoryError> {
    let identity = stable_object(metadata)?.to_string();
    let version = version_hint(metadata)?;
    let extension = Path::new(relative)
        .extension()
        .and_then(|extension| extension.to_str())
        .unwrap_or("");
    Ok(SourceCandidate {
        file_ref: MemoryFileRef {
            connector_id: CONNECTOR_ID.into(),
            root_id: root.root_id.clone(),
            object_id: Id::derived("memory-sifs-file", format!("{}\0{identity}", root.root_id)),
            path_hint: relative.into(),
        },
        absolute_path: root.config.path.join(relative),
        platform_identity: identity,
        source_version_hint: version,
        byte_length: metadata.size_bytes.unwrap_or(0),
        media_type: if extension.eq_ignore_ascii_case("md")
            || extension.eq_ignore_ascii_case("markdown")
        {
            "text/markdown"
        } else {
            "text/plain"
        }
        .into(),
    })
}

fn source_error(path: &str, error: VfsError) -> MemoryError {
    match error {
        VfsError::NotFound { .. } | VfsError::Conflict { .. } => {
            MemoryError::SourceChanged { path: path.into() }
        }
        error => vfs_error(path, error),
    }
}

impl MemorySourceConnector for SifsSourceConnector {
    fn scan_page(
        &self,
        root: &IndexRoot,
        cursor: Option<&LocalScanCursor>,
        limit: usize,
    ) -> Result<SourcePage, MemoryError> {
        if limit == 0 || limit > MAX_SCAN_PAGE_SIZE {
            return Err(MemoryError::InvalidMemoryPageLimit {
                surface: "SIFS source scan",
                limit,
                max: MAX_SCAN_PAGE_SIZE,
            });
        }
        let scoped = self.scoped(root)?;
        let binding = format!("sifs-scan-v1:{}", root.root_id);
        let mut frames = match cursor {
            None => vec![LocalScanFrame {
                relative_dir: String::new(),
                after_name: None,
            }],
            Some(cursor) => {
                let Some((header, frames)) = cursor.frames.split_first() else {
                    return Err(MemoryError::InvalidMemoryContinuation {
                        detail: "SIFS scan cursor is empty".into(),
                    });
                };
                if header.relative_dir != binding
                    || header.after_name.is_some()
                    || frames.is_empty()
                {
                    return Err(MemoryError::InvalidMemoryContinuation {
                        detail: "SIFS scan cursor belongs to another source root".into(),
                    });
                }
                frames.to_vec()
            }
        };
        if frames.len() > MAX_SCAN_DEPTH {
            return Err(MemoryError::InvalidMemoryContinuation {
                detail: "SIFS scan cursor exceeds directory depth bound".into(),
            });
        }
        let mut sources = Vec::new();
        for _ in 0..limit {
            let Some(frame) = frames.last().cloned() else {
                break;
            };
            relative_path(&frame.relative_dir)?;
            let after = frame
                .after_name
                .as_deref()
                .map(VfsDirectoryCursor::from_opaque_string)
                .transpose()
                .map_err(|error| vfs_error(&frame.relative_dir, error))?;
            // One entry per step keeps both namespace work and retained cursor
            // state bounded, including when a directory entry descends a level.
            let page = scoped
                .list_page_beneath_no_links(&frame.relative_dir, after.as_ref(), 1)
                .map_err(|error| source_error(&frame.relative_dir, error))?;
            if let Some(next) = page.next_cursor {
                frames.last_mut().expect("current frame").after_name = Some(
                    next.to_opaque_string()
                        .map_err(|error| vfs_error(&frame.relative_dir, error))?,
                );
            } else {
                frames.pop();
            }
            if page.entries.len() > 1 {
                return Err(MemoryError::InvalidSourceContent {
                    path: frame.relative_dir,
                    detail: "filesystem exceeded its directory page bound".into(),
                });
            }
            let Some(entry) = page.entries.into_iter().next() else {
                continue;
            };
            let relative = PathBuf::from(&entry.path)
                .strip_prefix(scoped.root())
                .map_err(|_| MemoryError::InvalidSourceCandidate {
                    path: entry.path.clone(),
                    detail: "directory entry is outside its registered root".into(),
                })?
                .to_str()
                .ok_or_else(|| MemoryError::InvalidSourceCandidate {
                    path: entry.path.clone(),
                    detail: "SIFS entry is not UTF-8".into(),
                })?
                .trim_start_matches('/')
                .to_string();
            relative_path(&relative)?;
            match entry.kind {
                VfsObjectKind::Directory => {
                    if frames.len() >= MAX_SCAN_DEPTH
                        || Path::new(&relative).components().count() >= MAX_SCAN_DEPTH
                    {
                        return Err(MemoryError::InvalidIndexRoot {
                            path: entry.path,
                            detail: "SIFS source exceeds directory depth bound".into(),
                        });
                    }
                    frames.push(LocalScanFrame {
                        relative_dir: relative,
                        after_name: None,
                    });
                }
                VfsObjectKind::File if included(root, &relative) => {
                    let metadata = scoped
                        .stat_beneath_no_links(&relative)
                        .map_err(|error| source_error(&entry.path, error))?;
                    if metadata.kind == VfsObjectKind::File
                        && metadata
                            .size_bytes
                            .is_some_and(|size| size <= root.config.max_source_bytes)
                    {
                        sources.push(candidate(root, &relative, &metadata)?);
                    }
                }
                _ => {} // Links and non-file namespace records never become sources.
            }
        }
        let continuation = if frames.is_empty() {
            None
        } else {
            frames.insert(
                0,
                LocalScanFrame {
                    relative_dir: binding,
                    after_name: None,
                },
            );
            Some(LocalScanCursor { frames })
        };
        Ok(SourcePage {
            sources,
            continuation,
        })
    }

    fn acquire_snapshot(
        &self,
        root: &IndexRoot,
        source: &SourceCandidate,
    ) -> Result<VerifiedSourceSnapshot, MemoryError> {
        let relative = relative_path(&source.file_ref.path_hint)?;
        if source.file_ref.root_id != root.root_id
            || source.file_ref.connector_id != CONNECTOR_ID
            || source.absolute_path != root.config.path.join(relative)
            || !included(root, relative)
        {
            return Err(MemoryError::InvalidSourceCandidate {
                path: source.absolute_path.display().to_string(),
                detail: "SIFS candidate differs from its registered root/locator".into(),
            });
        }
        let scoped = self.scoped(root)?;
        let read = scoped
            .read_prefix_beneath_no_links(
                relative,
                source.byte_length.min(root.config.max_source_bytes),
            )
            .map_err(|error| source_error(relative, error))?;
        if read.metadata.kind != VfsObjectKind::File
            || stable_object(&read.metadata)? != source.platform_identity
            || version_hint(&read.metadata)? != source.source_version_hint
        {
            return Err(MemoryError::SourceChanged {
                path: read.metadata.path,
            });
        }
        if candidate(root, relative, &read.metadata)? != *source {
            return Err(MemoryError::InvalidSourceCandidate {
                path: source.absolute_path.display().to_string(),
                detail: "SIFS candidate identity, length or media type is not canonical for the opened source".into(),
            });
        }
        if read.truncated || read.metadata.size_bytes != Some(read.bytes.len() as u64) {
            return Err(MemoryError::SourceTooLarge {
                path: read.metadata.path,
                bytes: read.metadata.size_bytes.unwrap_or(read.bytes.len() as u64),
                limit: root.config.max_source_bytes,
            });
        }
        let current = scoped
            .stat_beneath_no_links(relative)
            .map_err(|error| source_error(relative, error))?;
        if current.object_id != read.metadata.object_id || current.version != read.metadata.version
        {
            return Err(MemoryError::SourceChanged { path: current.path });
        }
        let content_hash = blake3::hash(&read.bytes).to_hex().to_string();
        if read.metadata.content_hash.as_ref().is_some_and(|expected| {
            expected.strip_prefix("blake3:").unwrap_or(expected) != content_hash
        }) {
            return Err(MemoryError::InvalidSourceContent {
                path: read.metadata.path,
                detail: "filesystem content differs from its exact metadata hash".into(),
            });
        }
        let version_id = Id::derived(
            "memory-sifs-file-version",
            format!(
                "{}\0{}\0{content_hash}",
                source.file_ref.object_id,
                storage_version(&read.metadata)?
            ),
        );
        Ok(VerifiedSourceSnapshot {
            candidate: source.clone(),
            version: MemoryFileVersionRef {
                file_ref: source.file_ref.clone(),
                version_id,
                content_hash,
                byte_length: read.bytes.len() as u64,
            },
            bytes: read.bytes,
        })
    }

    fn descriptor_identity_is_current(
        &self,
        root: &IndexRoot,
        source: &SourceDescriptor,
    ) -> Result<bool, MemoryError> {
        if source.version.file_ref.root_id != root.root_id
            || source.version.file_ref.connector_id != CONNECTOR_ID
        {
            return Ok(false);
        }
        let relative = relative_path(&source.version.file_ref.path_hint)?;
        if !included(root, relative) {
            return Ok(false);
        }
        let scoped = self.scoped(root)?;
        match scoped.stat_beneath_no_links(relative) {
            Ok(metadata) => Ok(metadata.kind == VfsObjectKind::File
                && stable_object(&metadata)? == source.platform_identity
                && metadata
                    .size_bytes
                    .is_some_and(|size| size <= root.config.max_source_bytes)),
            Err(VfsError::NotFound { .. }) => Ok(false),
            Err(error) => Err(vfs_error(relative, error)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoryQuery;
    use crate::indexed::IndexedFileMemoryStore;

    #[test]
    fn unknown_persisted_connector_never_reopens_a_local_path() {
        let directory =
            std::env::temp_dir().join(format!("unknown-memory-connector-{}", Id::generate()));
        let source = VfsHandle::memory();
        source.create_dir_all("/notes").unwrap();
        source.write("/notes/note.md", b"needle").unwrap();
        let store =
            IndexedFileMemoryStore::new(&directory).with_sifs_vfs(source, "fixture-namespace");
        let mut root = store
            .register_sifs_root(LocalIndexRootConfig::new(
                "/notes",
                IndexRootPolicy::cluster_local(),
            ))
            .unwrap();
        store.index_root(&root.root_id).unwrap();
        root.connector_id = "unknown/connector".into();
        store
            .write_sobj_atomic(&store.root_path(&root.root_id), &root)
            .unwrap();
        assert!(matches!(
            store.index_root(&root.root_id),
            Err(MemoryError::InvalidIndexRoot { .. })
        ));
        assert!(matches!(
            store.retrieve(
                &MemoryAccessContext::cluster_local(),
                &MemoryQuery {
                    task_hint: Some("needle".into()),
                    max_bytes: 128 * 1024,
                    ..MemoryQuery::default()
                }
            ),
            Err(MemoryError::InvalidIndexRoot { .. })
        ));
        drop(store);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
