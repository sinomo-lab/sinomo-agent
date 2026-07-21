use super::memory_notes::{
    MAX_MEMORY_NOTE_SEARCH_SNIPPET_BYTES, MemoryNoteBackend, MemoryNoteBackendError,
    MemoryNoteDocument, MemoryNotePath, MemoryNoteReadRequest, MemoryNoteSearchCursor,
    MemoryNoteSearchMatch, MemoryNoteSearchPage, MemoryNoteSearchRequest, MemoryNoteVersion,
    MemoryNoteWritePrecondition, MemoryNoteWriteReceipt, MemoryNoteWriteRequest,
};
use crate::AgentCancellationToken;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::vfs::{
    VfsDirectoryCursor, VfsError, VfsHandle, VfsObjectKind, VfsWriteOptions, mounted_sifs,
};
use std::path::PathBuf;
use std::sync::Mutex;

const MEMORY_NOTES_DIRECTORY: &str = "memory";
const MAX_SEARCHED_FILES: usize = 4_096;
const MAX_SEARCHED_DIRECTORIES: usize = 4_096;
const MAX_SEARCHED_BYTES: usize = 16 * 1024 * 1024;
const MAX_SEARCH_DEPTH: usize = 32;
const MAX_SEARCH_FILE_BYTES: u64 = 1024 * 1024;
const MEMORY_NOTE_LIST_PAGE_SIZE: usize = 128;
const MAX_MEMORY_SEARCH_OVERLAP_BYTES: usize =
    super::memory_notes::MAX_MEMORY_NOTE_QUERY_BYTES * 4 + 16;
const MEMORY_NOTE_SEARCH_CURSOR_PREFIX: &str = "vfs-memory-v3.";
const LEGACY_MEMORY_NOTE_SEARCH_CURSOR_PREFIX: &str = "sifs-memory-v2.";

fn memory_note_root(agent_key: &str) -> Result<String, MemoryNoteBackendError> {
    if agent_key.trim().is_empty() {
        return Err(MemoryNoteBackendError::Invalid {
            detail: "agent key for the memory-note root is empty".to_string(),
        });
    }
    Ok(format!(
        "{}/{MEMORY_NOTES_DIRECTORY}",
        super::local_workspace::agent_home_sifs_path_for_id(agent_key)
    ))
}

#[derive(Debug, Deserialize, Serialize)]
struct SifsMemorySearchCursorState {
    #[serde(rename = "b")]
    binding: String,
    #[serde(rename = "f")]
    frames: Vec<SifsMemorySearchFrame>,
    #[serde(rename = "a", skip_serializing_if = "Option::is_none")]
    active_file: Option<SifsMemorySearchActiveFile>,
}

#[derive(Debug, Deserialize, Serialize)]
struct SifsMemorySearchFrame {
    #[serde(rename = "p")]
    directory: String,
    #[serde(rename = "d")]
    depth: usize,
    #[serde(rename = "c", skip_serializing_if = "Option::is_none")]
    after: Option<String>,
    #[serde(rename = "i")]
    entry_offset: usize,
    #[serde(rename = "o", skip_serializing_if = "Option::is_none")]
    directory_object_id: Option<String>,
    #[serde(rename = "v", skip_serializing_if = "Option::is_none")]
    directory_version: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct SifsMemorySearchActiveFile {
    #[serde(rename = "p")]
    path: String,
    #[serde(rename = "o", skip_serializing_if = "Option::is_none")]
    object_id: Option<String>,
    #[serde(rename = "v", skip_serializing_if = "Option::is_none")]
    version: Option<String>,
    #[serde(rename = "b")]
    next_byte: u64,
    #[serde(rename = "l")]
    next_line: u64,
    #[serde(rename = "m", default)]
    current_line_matched: bool,
    #[serde(rename = "x", default, skip_serializing_if = "String::is_empty")]
    overlap: String,
}

struct CachedMemoryDirectoryPage {
    entries: Vec<sinomo_runtime::vfs::VfsDirEntry>,
    next_cursor: Option<VfsDirectoryCursor>,
}

#[derive(Clone)]
struct MemoryNoteVfs {
    scoped: VfsHandle,
}

/// VFS-backed Markdown notes confined to one agent's canonical home.
///
/// The identity and root are injected by the host. Model input is always a
/// validated relative `.md` path, so it cannot select another agent's home.
/// Any VFS backend used here must implement the guarded no-link and versioned
/// operations; unsupported backends fail closed.
pub struct VfsMemoryNoteBackend {
    sifs_data_dir: Option<PathBuf>,
    root: String,
    vfs: Mutex<Option<MemoryNoteVfs>>,
}

/// Compatibility name for the normal runtime constructor, which lazily opens
/// the agent's roaming SIFS-backed VFS.
pub type SifsMemoryNoteBackend = VfsMemoryNoteBackend;

impl VfsMemoryNoteBackend {
    pub fn new(data_dir: &SinomoDataDir, agent_key: &str) -> Result<Self, MemoryNoteBackendError> {
        let root = memory_note_root(agent_key)?;
        Ok(Self {
            sifs_data_dir: Some(data_dir.root().to_path_buf()),
            root,
            vfs: Mutex::new(None),
        })
    }

    /// Bind memory notes to an already-authorized VFS instead of opening DFS.
    /// This is used by local/in-memory hosts and the memory-tools benchmark.
    pub fn from_vfs(
        filesystem: VfsHandle,
        agent_key: &str,
    ) -> Result<Self, MemoryNoteBackendError> {
        let root = memory_note_root(agent_key)?;
        filesystem
            .create_dir_all_beneath_no_links(&root)
            .map_err(|source| MemoryNoteBackendError::Unavailable {
                detail: format!("cannot create agent memory-note VFS root {root}: {source}"),
            })?;
        let scoped =
            filesystem
                .scoped(&root)
                .map_err(|source| MemoryNoteBackendError::Unavailable {
                    detail: format!("cannot scope agent memory-note VFS root {root}: {source}"),
                })?;
        Ok(Self {
            sifs_data_dir: None,
            root,
            vfs: Mutex::new(Some(MemoryNoteVfs { scoped })),
        })
    }

    pub fn root(&self) -> &str {
        &self.root
    }

    fn vfs_handles(&self) -> Result<MemoryNoteVfs, MemoryNoteBackendError> {
        let mut cached = self
            .vfs
            .lock()
            .map_err(|_| MemoryNoteBackendError::Unavailable {
                detail: "memory-note VFS handle cache is poisoned".to_string(),
            })?;
        if let Some(vfs) = cached.as_ref() {
            return Ok(vfs.clone());
        }
        // Memory notes are the agent's authoritative roaming home, not a
        // best-effort node-local catalog. Use distributed SIFS reads so a
        // newly placed agent cannot silently observe an incomplete mirror.
        let data_dir =
            self.sifs_data_dir
                .as_ref()
                .ok_or_else(|| MemoryNoteBackendError::Unavailable {
                    detail: "injected memory-note VFS handle is unavailable".to_string(),
                })?;
        let filesystem = mounted_sifs(Some(data_dir.clone())).map_err(|source| {
            MemoryNoteBackendError::Unavailable {
                detail: format!(
                    "cannot open agent memory-note SIFS root {}: {source}",
                    self.root
                ),
            }
        })?;
        let scoped = filesystem.scoped(&self.root).map_err(|source| {
            MemoryNoteBackendError::Unavailable {
                detail: format!(
                    "cannot scope agent memory-note SIFS root {}: {source}",
                    self.root
                ),
            }
        })?;
        let vfs = MemoryNoteVfs { scoped };
        *cached = Some(vfs.clone());
        Ok(vfs)
    }

    fn read_search_range(
        &self,
        vfs: &MemoryNoteVfs,
        path: &MemoryNotePath,
        offset: u64,
        max_bytes: u64,
    ) -> Result<sinomo_runtime::vfs::VfsOpenedFileRead, MemoryNoteBackendError> {
        let read = vfs
            .scoped
            .read_range_beneath_no_links(path.as_str(), offset, max_bytes)
            .map_err(|source| map_vfs_error(path, source))?;
        if read.metadata.kind != VfsObjectKind::File {
            return Err(disallowed_namespace_entry(
                path.as_str(),
                &read.metadata.kind,
            ));
        }
        Ok(read)
    }

    fn prepare_note_parent(
        &self,
        vfs: &MemoryNoteVfs,
        path: &MemoryNotePath,
    ) -> Result<(), MemoryNoteBackendError> {
        let relative_parent = path
            .as_str()
            .rsplit_once('/')
            .map(|(parent, _)| parent)
            .unwrap_or("");
        vfs.scoped
            .create_dir_all_beneath_no_links(relative_parent)
            .map_err(|source| map_vfs_error(path, source))
    }

    fn relative_note_path(&self, absolute: &str) -> Option<MemoryNotePath> {
        absolute
            .strip_prefix(self.root.trim_end_matches('/'))
            .and_then(|path| path.strip_prefix('/'))
            .and_then(|path| MemoryNotePath::new(path.to_string()).ok())
    }
}

impl MemoryNoteBackend for VfsMemoryNoteBackend {
    fn search(
        &self,
        request: MemoryNoteSearchRequest,
        cancellation: &AgentCancellationToken,
    ) -> Result<MemoryNoteSearchPage, MemoryNoteBackendError> {
        let vfs = self.vfs_handles()?;
        let query = request.query.to_lowercase();
        let binding = memory_search_cursor_binding(&self.root, &query);
        let mut state = match request.cursor.as_ref() {
            Some(cursor) => decode_memory_search_cursor(cursor, &binding, &self.root)?,
            None => SifsMemorySearchCursorState {
                binding,
                frames: vec![SifsMemorySearchFrame {
                    directory: self.root.clone(),
                    depth: 0,
                    after: None,
                    entry_offset: 0,
                    directory_object_id: None,
                    directory_version: None,
                }],
                active_file: None,
            },
        };
        let mut cached_pages: Vec<Option<CachedMemoryDirectoryPage>> =
            (0..state.frames.len()).map(|_| None).collect();
        let mut matches = Vec::new();
        let mut searched_files = 0_usize;
        let mut searched_directory_pages = 0_usize;
        let mut searched_bytes = 0_usize;
        let mut output_bytes = 0_usize;
        let mut truncated = false;
        let mut stopped = false;

        loop {
            if cancellation.is_cancelled() {
                return Err(MemoryNoteBackendError::Cancelled);
            }
            if searched_files >= MAX_SEARCHED_FILES || searched_bytes >= MAX_SEARCHED_BYTES {
                break;
            }

            if let Some(mut active_file) = state.active_file.take() {
                let path = MemoryNotePath::new(active_file.path.clone()).map_err(|source| {
                    MemoryNoteBackendError::Invalid {
                        detail: format!("memory search cursor has an invalid note path: {source}"),
                    }
                })?;
                searched_files = searched_files.saturating_add(1);
                let remaining = MAX_SEARCHED_BYTES.saturating_sub(searched_bytes);
                if remaining == 0 {
                    state.active_file = Some(active_file);
                    break;
                }
                let max_bytes = remaining.min(MAX_SEARCH_FILE_BYTES as usize) as u64;
                let read = self.read_search_range(&vfs, &path, active_file.next_byte, max_bytes)?;
                let fetched_bytes = read.bytes.len();
                searched_bytes = searched_bytes.saturating_add(fetched_bytes);
                bind_active_file_identity(&mut active_file, &path, &read.metadata)?;
                let version = memory_note_version_from_metadata(&path, &read.metadata)?;
                let backend_truncated = read.truncated;
                let (markdown, decoded_bytes) =
                    decode_markdown_range(&path, read.bytes, backend_truncated)?;
                if decoded_bytes == 0 && backend_truncated {
                    // The bounded fetch ended inside the first UTF-8 scalar.
                    // Preserve the exact offset and retry with a fresh per-call
                    // budget instead of skipping bytes or spinning.
                    state.active_file = Some(active_file);
                    break;
                }
                let more_after_chunk = backend_truncated || decoded_bytes < fetched_bytes;

                if query.is_empty() {
                    let mut found_preview = false;
                    for segment in markdown.split_inclusive('\n') {
                        let segment_bytes = segment.len();
                        let preview = segment.strip_suffix('\n').unwrap_or(segment).trim();
                        if preview.is_empty() {
                            advance_active_file_segment(&mut active_file, segment, segment_bytes);
                            continue;
                        }
                        let (snippet, snippet_truncated) =
                            truncate_utf8(preview, MAX_MEMORY_NOTE_SEARCH_SNIPPET_BYTES);
                        let estimated =
                            path.as_str().len() + snippet.len() + version.as_str().len() + 64;
                        if matches.len() >= request.max_results
                            || output_bytes.saturating_add(estimated) > request.max_output_bytes
                        {
                            state.active_file = Some(active_file.clone());
                            stopped = true;
                            break;
                        }
                        output_bytes = output_bytes.saturating_add(estimated);
                        truncated |=
                            snippet_truncated || (!segment.ends_with('\n') && more_after_chunk);
                        matches.push(MemoryNoteSearchMatch {
                            path: path.clone(),
                            line: None,
                            snippet,
                            version: version.clone(),
                        });
                        found_preview = true;
                        if matches.len() >= request.max_results
                            || output_bytes >= request.max_output_bytes
                        {
                            stopped = true;
                        }
                        break;
                    }
                    if stopped {
                        break;
                    }
                    if found_preview {
                        continue;
                    }
                    if more_after_chunk {
                        state.active_file = Some(active_file);
                    }
                    continue;
                }

                let mut segments = markdown.split_inclusive('\n').peekable();
                while let Some(segment) = segments.next() {
                    let segment_bytes = segment.len();
                    let line = segment.strip_suffix('\n').unwrap_or(segment);
                    let candidate = if active_file.overlap.is_empty() {
                        line.to_string()
                    } else {
                        let mut candidate = String::with_capacity(
                            active_file.overlap.len().saturating_add(line.len()),
                        );
                        candidate.push_str(&active_file.overlap);
                        candidate.push_str(line);
                        candidate
                    };
                    let is_match = !active_file.current_line_matched
                        && candidate.to_lowercase().contains(&query);
                    if is_match {
                        let (snippet, snippet_truncated) = matching_memory_snippet(
                            candidate.trim(),
                            &query,
                            MAX_MEMORY_NOTE_SEARCH_SNIPPET_BYTES,
                        );
                        let estimated =
                            path.as_str().len() + snippet.len() + version.as_str().len() + 64;
                        if matches.len() >= request.max_results
                            || output_bytes.saturating_add(estimated) > request.max_output_bytes
                        {
                            state.active_file = Some(active_file.clone());
                            stopped = true;
                            break;
                        }
                        output_bytes = output_bytes.saturating_add(estimated);
                        truncated |= snippet_truncated;
                        matches.push(MemoryNoteSearchMatch {
                            path: path.clone(),
                            line: Some(active_file.next_line),
                            snippet,
                            version: version.clone(),
                        });
                        active_file.current_line_matched = true;
                    }

                    advance_active_file_segment(&mut active_file, segment, segment_bytes);
                    if !segment.ends_with('\n') {
                        active_file.overlap = if active_file.current_line_matched {
                            String::new()
                        } else {
                            utf8_suffix(
                                &candidate,
                                query.len().saturating_mul(4).saturating_add(16),
                            )
                        };
                    }

                    if is_match
                        && (matches.len() >= request.max_results
                            || output_bytes >= request.max_output_bytes)
                    {
                        if segments.peek().is_some() || more_after_chunk {
                            state.active_file = Some(active_file.clone());
                        }
                        stopped = true;
                        break;
                    }
                }
                if stopped {
                    break;
                }
                if more_after_chunk {
                    state.active_file = Some(active_file);
                }
                continue;
            }

            let Some(frame_index) = state.frames.len().checked_sub(1) else {
                break;
            };
            if cached_pages[frame_index].is_none() {
                if searched_directory_pages >= MAX_SEARCHED_DIRECTORIES {
                    break;
                }
                searched_directory_pages = searched_directory_pages.saturating_add(1);
                let frame = &state.frames[frame_index];
                let after = frame
                    .after
                    .as_deref()
                    .map(VfsDirectoryCursor::from_opaque_string)
                    .transpose()
                    .map_err(|source| MemoryNoteBackendError::Invalid {
                        detail: format!("memory search cursor has an invalid VFS page: {source}"),
                    })?;
                let page = vfs
                    .scoped
                    .list_page_beneath_no_links_if_exists(
                        &frame.directory,
                        after.as_ref(),
                        MEMORY_NOTE_LIST_PAGE_SIZE,
                    )
                    .map_err(|source| MemoryNoteBackendError::Unavailable {
                        detail: format!(
                            "cannot list memory-note directory {}: {source}",
                            frame.directory
                        ),
                    })?;
                bind_directory_generation(
                    &mut state.frames[frame_index],
                    page.directory_metadata.as_ref(),
                )?;
                if state.frames[frame_index].entry_offset > page.entries.len() {
                    return Err(MemoryNoteBackendError::Invalid {
                        detail: "memory search cursor points beyond its directory page".to_string(),
                    });
                }
                cached_pages[frame_index] = Some(CachedMemoryDirectoryPage {
                    entries: page.entries,
                    next_cursor: page.next_cursor,
                });
            }

            let page = cached_pages[frame_index]
                .as_ref()
                .expect("directory page loaded");
            let entry_offset = state.frames[frame_index].entry_offset;
            if entry_offset == page.entries.len() {
                let next_cursor = page
                    .next_cursor
                    .as_ref()
                    .map(VfsDirectoryCursor::to_opaque_string)
                    .transpose()
                    .map_err(|source| MemoryNoteBackendError::Unavailable {
                        detail: format!("cannot preserve memory search position: {source}"),
                    })?;
                if let Some(next_cursor) = next_cursor {
                    state.frames[frame_index].after = Some(next_cursor);
                    state.frames[frame_index].entry_offset = 0;
                    cached_pages[frame_index] = None;
                } else {
                    state.frames.pop();
                    cached_pages.pop();
                }
                continue;
            }

            let entry = page.entries[entry_offset].clone();
            state.frames[frame_index].entry_offset = entry_offset.saturating_add(1);
            // Directory entries are discovery hints only. A concurrent
            // replacement can make this kind stale, so every file body is
            // still opened through the no-link bounded-read primitive before
            // any content or version reaches the model.
            match entry.kind {
                VfsObjectKind::Directory if state.frames[frame_index].depth < MAX_SEARCH_DEPTH => {
                    let depth = state.frames[frame_index].depth.saturating_add(1);
                    state.frames.push(SifsMemorySearchFrame {
                        directory: entry.path,
                        depth,
                        after: None,
                        entry_offset: 0,
                        directory_object_id: None,
                        directory_version: None,
                    });
                    cached_pages.push(None);
                }
                VfsObjectKind::Directory => truncated = true,
                VfsObjectKind::File => {
                    let Some(path) = self.relative_note_path(&entry.path) else {
                        searched_files = searched_files.saturating_add(1);
                        continue;
                    };
                    state.active_file = Some(SifsMemorySearchActiveFile {
                        path: path.as_str().to_string(),
                        object_id: None,
                        version: None,
                        next_byte: 0,
                        next_line: 1,
                        current_line_matched: false,
                        overlap: String::new(),
                    });
                }
                _ => {}
            }
        }
        if state.active_file.is_none() {
            while let Some(frame_index) = state.frames.len().checked_sub(1) {
                let Some(page) = cached_pages[frame_index].as_ref() else {
                    break;
                };
                if state.frames[frame_index].entry_offset != page.entries.len() {
                    break;
                }
                let next_cursor = page
                    .next_cursor
                    .as_ref()
                    .map(VfsDirectoryCursor::to_opaque_string)
                    .transpose()
                    .map_err(|source| MemoryNoteBackendError::Unavailable {
                        detail: format!("cannot preserve memory search position: {source}"),
                    })?;
                if let Some(next_cursor) = next_cursor {
                    state.frames[frame_index].after = Some(next_cursor);
                    state.frames[frame_index].entry_offset = 0;
                    cached_pages[frame_index] = None;
                    break;
                }
                state.frames.pop();
                cached_pages.pop();
            }
        }
        let has_more = state.active_file.is_some() || !state.frames.is_empty();
        let next_cursor = if has_more {
            Some(encode_memory_search_cursor(&state)?)
        } else {
            None
        };
        truncated |= next_cursor.is_some();
        Ok(MemoryNoteSearchPage {
            matches,
            next_cursor,
            truncated,
        })
    }

    fn read(
        &self,
        request: MemoryNoteReadRequest,
        cancellation: &AgentCancellationToken,
    ) -> Result<MemoryNoteDocument, MemoryNoteBackendError> {
        if cancellation.is_cancelled() {
            return Err(MemoryNoteBackendError::Cancelled);
        }
        let vfs = self.vfs_handles()?;
        let read = self.read_search_range(
            &vfs,
            &request.path,
            request.offset,
            request.max_bytes as u64,
        )?;
        let version = memory_note_version_from_metadata(&request.path, &read.metadata)?;
        if request
            .expected_version
            .as_ref()
            .is_some_and(|expected| expected != &version)
        {
            return Err(MemoryNoteBackendError::Conflict {
                path: request.path,
                current_version: Some(version),
            });
        }
        let fetched_bytes = read.bytes.len();
        let backend_truncated = read.truncated;
        let (markdown, decoded_bytes) =
            decode_markdown_range(&request.path, read.bytes, backend_truncated)?;
        if decoded_bytes == 0 && backend_truncated {
            return Err(MemoryNoteBackendError::Invalid {
                detail: "memory_read max_bytes is too small to return the next UTF-8 character"
                    .to_string(),
            });
        }
        Ok(MemoryNoteDocument {
            path: request.path,
            offset: request.offset,
            markdown,
            version,
            truncated: backend_truncated || decoded_bytes < fetched_bytes,
        })
    }

    fn write(
        &self,
        request: MemoryNoteWriteRequest,
        cancellation: &AgentCancellationToken,
    ) -> Result<MemoryNoteWriteReceipt, MemoryNoteBackendError> {
        if cancellation.is_cancelled() {
            return Err(MemoryNoteBackendError::Cancelled);
        }
        let vfs = self.vfs_handles()?;
        self.prepare_note_parent(&vfs, &request.path)?;
        let (metadata, created) = match request.precondition {
            MemoryNoteWritePrecondition::CreateNew => {
                let metadata = vfs
                    .scoped
                    .create_new_versioned_beneath_no_links(
                        request.path.as_str(),
                        request.markdown.as_bytes(),
                        VfsWriteOptions::default().create_new(),
                    )
                    .map_err(|source| map_vfs_error(&request.path, source))?;
                (metadata, true)
            }
            MemoryNoteWritePrecondition::ReplaceIfVersion { expected_version } => {
                let (expected_object_id, expected_storage_version) =
                    parse_memory_note_version(&request.path, &expected_version)?;
                let metadata = vfs
                    .scoped
                    .replace_if_version_beneath_no_links(
                        request.path.as_str(),
                        expected_object_id,
                        expected_storage_version,
                        request.markdown.as_bytes(),
                        VfsWriteOptions::default().replace_existing(),
                    )
                    .map_err(|source| map_vfs_error(&request.path, source))?;
                (metadata, false)
            }
        };
        let version = memory_note_version_from_metadata(&request.path, &metadata)?;
        Ok(MemoryNoteWriteReceipt {
            path: request.path,
            version,
            created,
        })
    }
}

fn bind_directory_generation(
    frame: &mut SifsMemorySearchFrame,
    metadata: Option<&sinomo_runtime::vfs::VfsMetadata>,
) -> Result<(), MemoryNoteBackendError> {
    let Some(metadata) = metadata else {
        if frame.directory_object_id.is_some() || frame.directory_version.is_some() {
            return Err(MemoryNoteBackendError::Invalid {
                detail: "memory search directory disappeared while its cursor was suspended"
                    .to_string(),
            });
        }
        return Ok(());
    };
    let object_id =
        metadata
            .object_id
            .as_deref()
            .ok_or_else(|| MemoryNoteBackendError::Unavailable {
                detail: "memory search directory has no stable storage identity".to_string(),
            })?;
    let version =
        metadata
            .version
            .as_deref()
            .ok_or_else(|| MemoryNoteBackendError::Unavailable {
                detail: "memory search directory has no stable storage version".to_string(),
            })?;
    match (
        frame.directory_object_id.as_deref(),
        frame.directory_version.as_deref(),
    ) {
        (None, None) => {
            frame.directory_object_id = Some(object_id.to_string());
            frame.directory_version = Some(version.to_string());
            Ok(())
        }
        (Some(expected_object_id), Some(expected_version))
            if expected_object_id == object_id && expected_version == version =>
        {
            Ok(())
        }
        (Some(_), Some(_)) => Err(MemoryNoteBackendError::Invalid {
            detail: "memory search directory changed while its cursor was suspended".to_string(),
        }),
        _ => Err(MemoryNoteBackendError::Invalid {
            detail: "memory search cursor has an incomplete directory generation".to_string(),
        }),
    }
}

fn bind_active_file_identity(
    active_file: &mut SifsMemorySearchActiveFile,
    path: &MemoryNotePath,
    metadata: &sinomo_runtime::vfs::VfsMetadata,
) -> Result<(), MemoryNoteBackendError> {
    let object_id =
        metadata
            .object_id
            .as_deref()
            .ok_or_else(|| MemoryNoteBackendError::Unavailable {
                detail: format!("memory note `{path}` has no stable storage identity"),
            })?;
    let version =
        metadata
            .version
            .as_deref()
            .ok_or_else(|| MemoryNoteBackendError::Unavailable {
                detail: format!("memory note `{path}` has no storage version"),
            })?;
    match (
        active_file.object_id.as_deref(),
        active_file.version.as_deref(),
    ) {
        (None, None) => {
            active_file.object_id = Some(object_id.to_string());
            active_file.version = Some(version.to_string());
            Ok(())
        }
        (Some(expected_object_id), Some(expected_version))
            if expected_object_id == object_id && expected_version == version =>
        {
            Ok(())
        }
        (Some(_), Some(_)) => Err(MemoryNoteBackendError::Conflict {
            path: path.clone(),
            current_version: memory_note_version_from_metadata(path, metadata).ok(),
        }),
        _ => Err(MemoryNoteBackendError::Invalid {
            detail: "memory search cursor has an incomplete note identity".to_string(),
        }),
    }
}

fn decode_markdown_range(
    path: &MemoryNotePath,
    bytes: Vec<u8>,
    truncated: bool,
) -> Result<(String, usize), MemoryNoteBackendError> {
    let byte_len = bytes.len();
    match String::from_utf8(bytes) {
        Ok(markdown) => Ok((markdown, byte_len)),
        Err(source) if truncated && source.utf8_error().error_len().is_none() => {
            let valid_up_to = source.utf8_error().valid_up_to();
            let mut bytes = source.into_bytes();
            bytes.truncate(valid_up_to);
            Ok((
                String::from_utf8(bytes).expect("validated UTF-8 range prefix"),
                valid_up_to,
            ))
        }
        Err(source) => Err(MemoryNoteBackendError::Invalid {
            detail: format!("memory note `{path}` is not UTF-8 Markdown: {source}"),
        }),
    }
}

fn advance_active_file_segment(
    active_file: &mut SifsMemorySearchActiveFile,
    segment: &str,
    segment_bytes: usize,
) {
    active_file.next_byte = active_file
        .next_byte
        .saturating_add(u64::try_from(segment_bytes).unwrap_or(u64::MAX));
    if segment.ends_with('\n') {
        active_file.next_line = active_file.next_line.saturating_add(1);
        active_file.current_line_matched = false;
        active_file.overlap.clear();
    }
}

fn utf8_suffix(value: &str, max_bytes: usize) -> String {
    let max_bytes = max_bytes.min(MAX_MEMORY_SEARCH_OVERLAP_BYTES);
    if value.len() <= max_bytes {
        return value.to_string();
    }
    let mut start = value.len().saturating_sub(max_bytes);
    while start < value.len() && !value.is_char_boundary(start) {
        start = start.saturating_add(1);
    }
    value[start..].to_string()
}

fn matching_memory_snippet(value: &str, query: &str, max_bytes: usize) -> (String, bool) {
    if value.len() <= max_bytes {
        return (value.to_string(), false);
    }
    let Some((match_start, match_end)) = case_folded_match_span(value, query) else {
        return truncate_utf8(value, max_bytes);
    };
    let match_bytes = match_end.saturating_sub(match_start);
    if match_bytes >= max_bytes {
        let mut end = match_start.saturating_add(max_bytes).min(value.len());
        while end > match_start && !value.is_char_boundary(end) {
            end = end.saturating_sub(1);
        }
        return (value[match_start..end].to_string(), true);
    }
    let context_before = max_bytes.saturating_sub(match_bytes) / 2;
    let mut start = match_start.saturating_sub(context_before);
    while start < match_start && !value.is_char_boundary(start) {
        start = start.saturating_add(1);
    }
    let mut end = start.saturating_add(max_bytes).min(value.len());
    while end > match_end && !value.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    if end < match_end {
        end = match_end;
        start = end.saturating_sub(max_bytes);
        while start < match_start && !value.is_char_boundary(start) {
            start = start.saturating_add(1);
        }
    }
    (value[start..end].to_string(), true)
}

fn case_folded_match_span(value: &str, query: &str) -> Option<(usize, usize)> {
    let mut folded = String::with_capacity(value.len());
    let mut offsets = Vec::with_capacity(value.chars().count().saturating_add(1));
    for (original_offset, character) in value.char_indices() {
        for lowercase in character.to_lowercase() {
            offsets.push((folded.len(), original_offset));
            folded.push(lowercase);
        }
    }
    offsets.push((folded.len(), value.len()));
    let folded_start = folded.find(query)?;
    let folded_end = folded_start.saturating_add(query.len());
    let original_start = offsets
        .iter()
        .rev()
        .find(|(offset, _)| *offset <= folded_start)
        .map(|(_, original)| *original)?;
    let original_end = offsets
        .iter()
        .find(|(offset, _)| *offset >= folded_end)
        .map(|(_, original)| *original)
        .unwrap_or(value.len());
    Some((original_start, original_end))
}

fn memory_search_cursor_binding(root: &str, query: &str) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(root.as_bytes());
    hasher.update(&[0]);
    hasher.update(query.as_bytes());
    hasher.finalize().to_hex().as_str()[..24].to_string()
}

fn encode_memory_search_cursor(
    state: &SifsMemorySearchCursorState,
) -> Result<MemoryNoteSearchCursor, MemoryNoteBackendError> {
    let payload =
        serde_json::to_vec(state).map_err(|source| MemoryNoteBackendError::Unavailable {
            detail: format!("cannot encode memory search cursor: {source}"),
        })?;
    MemoryNoteSearchCursor::new(format!(
        "{MEMORY_NOTE_SEARCH_CURSOR_PREFIX}{}",
        URL_SAFE_NO_PAD.encode(payload)
    ))
    .map_err(|source| MemoryNoteBackendError::Unavailable {
        detail: format!("memory search cursor is too large to return: {source}"),
    })
}

fn decode_memory_search_cursor(
    cursor: &MemoryNoteSearchCursor,
    expected_binding: &str,
    root: &str,
) -> Result<SifsMemorySearchCursorState, MemoryNoteBackendError> {
    let encoded = cursor
        .as_str()
        .strip_prefix(MEMORY_NOTE_SEARCH_CURSOR_PREFIX)
        .or_else(|| {
            cursor
                .as_str()
                .strip_prefix(LEGACY_MEMORY_NOTE_SEARCH_CURSOR_PREFIX)
        })
        .ok_or_else(|| MemoryNoteBackendError::Invalid {
            detail: "memory search cursor belongs to another backend".to_string(),
        })?;
    let payload =
        URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|source| MemoryNoteBackendError::Invalid {
                detail: format!("memory search cursor is not valid base64url: {source}"),
            })?;
    let state: SifsMemorySearchCursorState =
        serde_json::from_slice(&payload).map_err(|source| MemoryNoteBackendError::Invalid {
            detail: format!("memory search cursor is invalid: {source}"),
        })?;
    if state.binding != expected_binding {
        return Err(MemoryNoteBackendError::Invalid {
            detail: "memory search cursor belongs to another agent or query".to_string(),
        });
    }
    if state.frames.len() > MAX_SEARCH_DEPTH.saturating_add(1) {
        return Err(MemoryNoteBackendError::Invalid {
            detail: "memory search cursor exceeds the traversal depth limit".to_string(),
        });
    }
    for (index, frame) in state.frames.iter().enumerate() {
        if frame.depth != index
            || !(frame.directory == root
                || frame
                    .directory
                    .strip_prefix(root)
                    .is_some_and(|suffix| suffix.starts_with('/')))
            || frame.entry_offset > MEMORY_NOTE_LIST_PAGE_SIZE
            || frame.directory_object_id.is_some() != frame.directory_version.is_some()
        {
            return Err(MemoryNoteBackendError::Invalid {
                detail: "memory search cursor has an invalid directory traversal state".to_string(),
            });
        }
        if let Some(after) = frame.after.as_deref() {
            VfsDirectoryCursor::from_opaque_string(after).map_err(|source| {
                MemoryNoteBackendError::Invalid {
                    detail: format!("memory search cursor contains an invalid VFS page: {source}"),
                }
            })?;
        }
    }
    if let Some(active_file) = state.active_file.as_ref() {
        MemoryNotePath::new(active_file.path.clone()).map_err(|source| {
            MemoryNoteBackendError::Invalid {
                detail: format!("memory search cursor has an invalid active note: {source}"),
            }
        })?;
        if active_file.object_id.is_some() != active_file.version.is_some()
            || active_file.next_line == 0
            || active_file.overlap.len() > MAX_MEMORY_SEARCH_OVERLAP_BYTES
            || (active_file.next_byte > 0 && active_file.object_id.is_none())
        {
            return Err(MemoryNoteBackendError::Invalid {
                detail: "memory search cursor has an invalid active note position".to_string(),
            });
        }
    }
    if state.frames.is_empty() && state.active_file.is_none() {
        return Err(MemoryNoteBackendError::Invalid {
            detail: "memory search cursor is already complete".to_string(),
        });
    }
    Ok(state)
}

fn disallowed_namespace_entry(path: &str, kind: &VfsObjectKind) -> MemoryNoteBackendError {
    MemoryNoteBackendError::Invalid {
        detail: format!(
            "memory-note namespace entry `{path}` is {kind:?}; only real directories and regular Markdown files are allowed"
        ),
    }
}

fn memory_note_version_from_metadata(
    path: &MemoryNotePath,
    metadata: &sinomo_runtime::vfs::VfsMetadata,
) -> Result<MemoryNoteVersion, MemoryNoteBackendError> {
    let object_id =
        metadata
            .object_id
            .as_deref()
            .ok_or_else(|| MemoryNoteBackendError::Unavailable {
                detail: format!("memory note `{path}` has no stable storage identity"),
            })?;
    let version =
        metadata
            .version
            .as_deref()
            .ok_or_else(|| MemoryNoteBackendError::Unavailable {
                detail: format!("memory note `{path}` has no storage version"),
            })?;
    MemoryNoteVersion::new(format!("vfs:v1:{object_id}:{version}")).map_err(|source| {
        MemoryNoteBackendError::Invalid {
            detail: format!("memory note `{path}` has an invalid version token: {source}"),
        }
    })
}

fn parse_memory_note_version<'a>(
    path: &MemoryNotePath,
    token: &'a MemoryNoteVersion,
) -> Result<(&'a str, &'a str), MemoryNoteBackendError> {
    let mut parts = token.as_str().split(':');
    let backend = parts.next();
    let valid_prefix = matches!(backend, Some("vfs" | "sifs")) && parts.next() == Some("v1");
    let object_id = parts.next();
    let version = parts.next();
    if !valid_prefix
        || object_id.is_none_or(str::is_empty)
        || version.is_none_or(str::is_empty)
        || parts.next().is_some()
    {
        return Err(MemoryNoteBackendError::Invalid {
            detail: format!("memory note `{path}` has an invalid VFS version token"),
        });
    }
    Ok((object_id.expect("checked"), version.expect("checked")))
}

fn map_vfs_error(path: &MemoryNotePath, source: VfsError) -> MemoryNoteBackendError {
    match source {
        VfsError::NotFound { .. } => MemoryNoteBackendError::NotFound { path: path.clone() },
        VfsError::AlreadyExists { .. } => {
            MemoryNoteBackendError::AlreadyExists { path: path.clone() }
        }
        VfsError::Conflict { .. } => MemoryNoteBackendError::Conflict {
            path: path.clone(),
            current_version: None,
        },
        VfsError::InvalidPath { detail, .. }
        | VfsError::OutsideScope { path: detail, .. }
        | VfsError::NotDirectory { path: detail }
        | VfsError::IsDirectory { path: detail } => MemoryNoteBackendError::Invalid { detail },
        source => MemoryNoteBackendError::Unavailable {
            detail: source.to_string(),
        },
    }
}

fn truncate_utf8(value: &str, max_bytes: usize) -> (String, bool) {
    if value.len() <= max_bytes {
        return (value.to_string(), false);
    }
    let mut end = max_bytes.min(value.len());
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    (value[..end].to_string(), true)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path(value: &str) -> MemoryNotePath {
        MemoryNotePath::new(value).expect("valid memory-note path")
    }

    #[test]
    fn memory_notes_use_an_injected_memory_vfs_without_dfs() {
        let backend = VfsMemoryNoteBackend::from_vfs(VfsHandle::memory(), "main-agent")
            .expect("in-memory backend");
        let cancellation = AgentCancellationToken::new();
        let note_path = path("benchmarks/history.md");
        let created = backend
            .write(
                MemoryNoteWriteRequest {
                    path: note_path.clone(),
                    markdown: "Remember the in-memory VFS needle.\n".to_string(),
                    precondition: MemoryNoteWritePrecondition::CreateNew,
                },
                &cancellation,
            )
            .expect("write in-memory note");
        assert!(created.version.as_str().starts_with("vfs:v1:"));

        let page = backend
            .search(
                MemoryNoteSearchRequest {
                    query: "VFS needle".to_string(),
                    cursor: None,
                    max_results: 10,
                    max_output_bytes: 64 * 1024,
                },
                &cancellation,
            )
            .expect("search in-memory note");
        assert_eq!(page.matches.len(), 1, "{page:#?}");
        assert_eq!(page.matches[0].path, note_path);

        let read = backend
            .read(
                MemoryNoteReadRequest {
                    path: note_path,
                    offset: 0,
                    max_bytes: 4 * 1024,
                    expected_version: Some(created.version),
                },
                &cancellation,
            )
            .expect("read in-memory note");
        assert!(read.markdown.contains("in-memory VFS needle"));
    }

    #[test]
    fn sifs_memory_notes_are_agent_scoped_searchable_and_versioned() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-memory-notes-sifs");
        let data_dir = SinomoDataDir::new(&root);
        let main = SifsMemoryNoteBackend::new(&data_dir, "main-agent").expect("main backend");
        let background =
            SifsMemoryNoteBackend::new(&data_dir, "background-agent").expect("background backend");
        let cancellation = AgentCancellationToken::new();
        let note_path = path("runtime/lesson.md");

        let created = main
            .write(
                MemoryNoteWriteRequest {
                    path: note_path.clone(),
                    markdown: "# Lesson\nUse the authoritative SIFS path.\n".to_string(),
                    precondition: MemoryNoteWritePrecondition::CreateNew,
                },
                &cancellation,
            )
            .expect("create note");
        assert!(created.created);
        assert!(matches!(
            main.write(
                MemoryNoteWriteRequest {
                    path: note_path.clone(),
                    markdown: "duplicate".to_string(),
                    precondition: MemoryNoteWritePrecondition::CreateNew,
                },
                &cancellation,
            ),
            Err(MemoryNoteBackendError::AlreadyExists { .. })
        ));

        let document = main
            .read(
                MemoryNoteReadRequest {
                    path: note_path.clone(),
                    offset: 0,
                    max_bytes: 1024,
                    expected_version: None,
                },
                &cancellation,
            )
            .expect("read note");
        assert_eq!(document.version, created.version);
        assert!(document.markdown.contains("authoritative SIFS"));
        assert!(matches!(
            background.read(
                MemoryNoteReadRequest {
                    path: note_path.clone(),
                    offset: 0,
                    max_bytes: 1024,
                    expected_version: None,
                },
                &cancellation,
            ),
            Err(MemoryNoteBackendError::NotFound { .. })
        ));

        let page = main
            .search(
                MemoryNoteSearchRequest {
                    query: "authoritative".to_string(),
                    cursor: None,
                    max_results: 10,
                    max_output_bytes: 8 * 1024,
                },
                &cancellation,
            )
            .expect("search notes");
        assert_eq!(page.matches.len(), 1);
        assert_eq!(page.matches[0].path, note_path);
        assert_eq!(page.matches[0].line, Some(2));

        let listing = main
            .search(
                MemoryNoteSearchRequest {
                    query: String::new(),
                    cursor: None,
                    max_results: 10,
                    max_output_bytes: 8 * 1024,
                },
                &cancellation,
            )
            .expect("list notes");
        assert_eq!(listing.matches.len(), 1);
        assert_eq!(listing.matches[0].path, note_path);
        assert_eq!(listing.matches[0].line, None);
        assert_eq!(listing.matches[0].snippet, "# Lesson");

        let replaced = main
            .write(
                MemoryNoteWriteRequest {
                    path: note_path.clone(),
                    markdown: "# Lesson\nUse an optimistic version fence.\n".to_string(),
                    precondition: MemoryNoteWritePrecondition::ReplaceIfVersion {
                        expected_version: created.version.clone(),
                    },
                },
                &cancellation,
            )
            .expect("replace note");
        assert!(!replaced.created);
        assert_ne!(replaced.version, created.version);
        assert!(matches!(
            main.read(
                MemoryNoteReadRequest {
                    path: note_path.clone(),
                    offset: 1,
                    max_bytes: 1024,
                    expected_version: Some(created.version.clone()),
                },
                &cancellation,
            ),
            Err(MemoryNoteBackendError::Conflict { .. })
        ));
        assert!(matches!(
            main.write(
                MemoryNoteWriteRequest {
                    path: note_path,
                    markdown: "stale replacement".to_string(),
                    precondition: MemoryNoteWritePrecondition::ReplaceIfVersion {
                        expected_version: created.version,
                    },
                },
                &cancellation,
            ),
            Err(MemoryNoteBackendError::Conflict { .. })
        ));
    }

    #[test]
    fn sifs_memory_notes_reject_final_and_intermediate_links() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-memory-notes-link-confinement");
        let data_dir = SinomoDataDir::new(&root);
        let backend = SifsMemoryNoteBackend::new(&data_dir, "main-agent").expect("backend");
        let cancellation = AgentCancellationToken::new();

        backend
            .write(
                MemoryNoteWriteRequest {
                    path: path("safe.md"),
                    markdown: "# Safe\ninside the agent home\n".to_string(),
                    precondition: MemoryNoteWritePrecondition::CreateNew,
                },
                &cancellation,
            )
            .expect("bootstrap memory root");

        let mut opened =
            sinomo_runtime::dfs::open_mounted_sifs_service(Some(data_dir.root().to_path_buf()))
                .expect("open DFS service");
        let options = sinomo_runtime::dfs::DfsWriteOptions::replicated_eventual(vec![
            opened.service.node_id().clone(),
        ]);
        opened
            .service
            .posix_mkdir_all("/outside-memory")
            .expect("create external directory");
        opened
            .service
            .posix_write_file(
                "/outside-memory/secret.md",
                b"# Secret\nneedle outside the agent home\n",
                options.clone(),
            )
            .expect("create external note");
        opened
            .service
            .posix_hardlink(
                "/outside-memory/secret.md",
                &format!("{}/hard.md", backend.root()),
            )
            .expect("create hardlink into memory root");
        opened
            .service
            .posix_symlink(
                "/outside-memory/secret.md",
                &format!("{}/symbolic.md", backend.root()),
                options.clone(),
            )
            .expect("create final symlink into memory root");
        opened
            .service
            .posix_symlink(
                "/outside-memory",
                &format!("{}/escape", backend.root()),
                options,
            )
            .expect("create intermediate symlink into memory root");
        drop(opened);

        for linked_path in ["hard.md", "symbolic.md", "escape/secret.md"] {
            let error = backend
                .read(
                    MemoryNoteReadRequest {
                        path: path(linked_path),
                        offset: 0,
                        max_bytes: 1024,
                        expected_version: None,
                    },
                    &cancellation,
                )
                .expect_err("linked note must be rejected");
            assert!(matches!(error, MemoryNoteBackendError::Invalid { .. }));
        }

        let page = backend
            .search(
                MemoryNoteSearchRequest {
                    query: "needle outside".to_string(),
                    cursor: None,
                    max_results: 10,
                    max_output_bytes: 8 * 1024,
                },
                &cancellation,
            )
            .expect("search skips link records");
        assert!(page.matches.is_empty());

        let error = backend
            .write(
                MemoryNoteWriteRequest {
                    path: path("escape/new.md"),
                    markdown: "must stay confined".to_string(),
                    precondition: MemoryNoteWritePrecondition::CreateNew,
                },
                &cancellation,
            )
            .expect_err("intermediate symlink must reject creation");
        assert!(matches!(error, MemoryNoteBackendError::Invalid { .. }));

        let mut opened =
            sinomo_runtime::dfs::open_mounted_sifs_service(Some(data_dir.root().to_path_buf()))
                .expect("reopen DFS service");
        assert!(matches!(
            opened
                .service
                .posix_namespace_lstat_distributed("/outside-memory/new.md"),
            Err(sinomo_runtime::dfs::DfsPosixError::NotFound { .. })
        ));
    }

    #[test]
    fn sifs_memory_notes_reject_substituted_root_parent_and_note_links() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-memory-notes-link-substitution");
        let data_dir = SinomoDataDir::new(&root);
        let root_backend =
            SifsMemoryNoteBackend::new(&data_dir, "root-swap-agent").expect("root backend");
        let parent_backend =
            SifsMemoryNoteBackend::new(&data_dir, "parent-swap-agent").expect("parent backend");
        let note_backend =
            SifsMemoryNoteBackend::new(&data_dir, "note-swap-agent").expect("note backend");
        let cancellation = AgentCancellationToken::new();

        let root_seed = root_backend
            .write(
                MemoryNoteWriteRequest {
                    path: path("seed.md"),
                    markdown: "root seed".to_string(),
                    precondition: MemoryNoteWritePrecondition::CreateNew,
                },
                &cancellation,
            )
            .expect("bootstrap root-swap backend");
        let parent_seed = parent_backend
            .write(
                MemoryNoteWriteRequest {
                    path: path("nested/seed.md"),
                    markdown: "parent seed".to_string(),
                    precondition: MemoryNoteWritePrecondition::CreateNew,
                },
                &cancellation,
            )
            .expect("bootstrap parent-swap backend");
        let note_seed = note_backend
            .write(
                MemoryNoteWriteRequest {
                    path: path("victim.md"),
                    markdown: "inside note".to_string(),
                    precondition: MemoryNoteWritePrecondition::CreateNew,
                },
                &cancellation,
            )
            .expect("bootstrap note-swap backend");
        assert!(root_seed.created && parent_seed.created && note_seed.created);

        let mut opened =
            sinomo_runtime::dfs::open_mounted_sifs_service(Some(data_dir.root().to_path_buf()))
                .expect("open DFS service");
        let options = sinomo_runtime::dfs::DfsWriteOptions::replicated_eventual(vec![
            opened.service.node_id().clone(),
        ]);
        opened
            .service
            .posix_mkdir_all("/outside-memory/root")
            .expect("create outside root target");
        opened
            .service
            .posix_mkdir_all("/outside-memory/parent")
            .expect("create outside parent target");
        opened
            .service
            .posix_mkdir_all("/outside-memory/note")
            .expect("create outside note target");
        opened
            .service
            .posix_write_file(
                "/outside-memory/note/secret.md",
                b"outside secret",
                options.clone(),
            )
            .expect("create outside secret");

        let root_seed_path = format!("{}/seed.md", root_backend.root());
        opened
            .service
            .posix_unlink(&root_seed_path)
            .expect("remove root seed");
        opened
            .service
            .posix_rmdir(root_backend.root())
            .expect("remove memory root before substitution");
        opened
            .service
            .posix_symlink("/outside-memory/root", root_backend.root(), options.clone())
            .expect("substitute memory root link");

        let nested_parent = format!("{}/nested", parent_backend.root());
        opened
            .service
            .posix_unlink(&format!("{nested_parent}/seed.md"))
            .expect("remove nested seed");
        opened
            .service
            .posix_rmdir(&nested_parent)
            .expect("remove nested parent before substitution");
        opened
            .service
            .posix_symlink("/outside-memory/parent", &nested_parent, options.clone())
            .expect("substitute nested parent link");

        let victim = format!("{}/victim.md", note_backend.root());
        opened
            .service
            .posix_unlink(&victim)
            .expect("remove victim before substitution");
        opened
            .service
            .posix_symlink("/outside-memory/note/secret.md", &victim, options)
            .expect("substitute final note link");
        drop(opened);

        assert!(matches!(
            root_backend.write(
                MemoryNoteWriteRequest {
                    path: path("new.md"),
                    markdown: "must not escape through root".to_string(),
                    precondition: MemoryNoteWritePrecondition::CreateNew,
                },
                &cancellation,
            ),
            Err(MemoryNoteBackendError::Invalid { .. })
        ));
        assert!(matches!(
            parent_backend.write(
                MemoryNoteWriteRequest {
                    path: path("nested/new.md"),
                    markdown: "must not escape through parent".to_string(),
                    precondition: MemoryNoteWritePrecondition::CreateNew,
                },
                &cancellation,
            ),
            Err(MemoryNoteBackendError::Invalid { .. })
        ));
        assert!(matches!(
            note_backend.read(
                MemoryNoteReadRequest {
                    path: path("victim.md"),
                    offset: 0,
                    max_bytes: 1024,
                    expected_version: None,
                },
                &cancellation,
            ),
            Err(MemoryNoteBackendError::Invalid { .. })
        ));
        assert!(matches!(
            note_backend.write(
                MemoryNoteWriteRequest {
                    path: path("victim.md"),
                    markdown: "must not replace outside".to_string(),
                    precondition: MemoryNoteWritePrecondition::ReplaceIfVersion {
                        expected_version: note_seed.version,
                    },
                },
                &cancellation,
            ),
            Err(MemoryNoteBackendError::Invalid { .. })
                | Err(MemoryNoteBackendError::Conflict { .. })
        ));

        let mut opened =
            sinomo_runtime::dfs::open_mounted_sifs_service(Some(data_dir.root().to_path_buf()))
                .expect("reopen DFS service");
        for outside in [
            "/outside-memory/root/new.md",
            "/outside-memory/parent/new.md",
        ] {
            assert!(matches!(
                opened.service.posix_namespace_lstat_distributed(outside),
                Err(sinomo_runtime::dfs::DfsPosixError::NotFound { .. })
            ));
        }
        assert_eq!(
            opened
                .service
                .posix_read_file_distributed("/outside-memory/note/secret.md")
                .expect("outside secret unchanged"),
            b"outside secret"
        );
    }

    #[test]
    fn sifs_memory_search_cursor_continues_without_repeating_notes() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-memory-notes-cursor");
        let data_dir = SinomoDataDir::new(&root);
        let backend = SifsMemoryNoteBackend::new(&data_dir, "main-agent").expect("backend");
        let cancellation = AgentCancellationToken::new();
        let query = "needle-private-query";
        for name in ["alpha.md", "bravo.md", "charlie.md"] {
            backend
                .write(
                    MemoryNoteWriteRequest {
                        path: path(name),
                        markdown: format!("# {name}\n{query}\n"),
                        precondition: MemoryNoteWritePrecondition::CreateNew,
                    },
                    &cancellation,
                )
                .expect("create note");
        }

        let mut cursor = None;
        let mut first_cursor = None;
        let mut found = Vec::new();
        loop {
            let page = backend
                .search(
                    MemoryNoteSearchRequest {
                        query: query.to_string(),
                        cursor,
                        max_results: 1,
                        max_output_bytes: 8 * 1024,
                    },
                    &cancellation,
                )
                .expect("search page");
            assert!(page.matches.len() <= 1);
            found.extend(
                page.matches
                    .into_iter()
                    .map(|entry| entry.path.as_str().to_string()),
            );
            if first_cursor.is_none() {
                let next_cursor = page.next_cursor.as_ref().expect("first page cursor");
                assert!(!next_cursor.as_str().contains("main-agent"));
                assert!(!next_cursor.as_str().contains(backend.root()));
                assert!(!next_cursor.as_str().contains(query));
                first_cursor = Some(next_cursor.clone());
            }
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }

        // A DFS page cursor defines a stable traversal continuation, not a
        // lexicographic presentation order. Verify the exhaustive,
        // non-repeating set independently of that backend order.
        found.sort();
        assert_eq!(found, vec!["alpha.md", "bravo.md", "charlie.md"]);
        let error = backend
            .search(
                MemoryNoteSearchRequest {
                    query: "different query".to_string(),
                    cursor: first_cursor,
                    max_results: 1,
                    max_output_bytes: 8 * 1024,
                },
                &cancellation,
            )
            .expect_err("cursor must be query bound");
        assert!(matches!(error, MemoryNoteBackendError::Invalid { .. }));
    }

    #[test]
    fn sifs_memory_search_cursor_rejects_directory_generation_changes() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-memory-directory-generation");
        let data_dir = SinomoDataDir::new(&root);
        let backend = SifsMemoryNoteBackend::new(&data_dir, "main-agent").expect("backend");
        let cancellation = AgentCancellationToken::new();
        for name in ["alpha.md", "bravo.md"] {
            backend
                .write(
                    MemoryNoteWriteRequest {
                        path: path(name),
                        markdown: "needle\n".to_string(),
                        precondition: MemoryNoteWritePrecondition::CreateNew,
                    },
                    &cancellation,
                )
                .expect("create initial note");
        }
        let first = backend
            .search(
                MemoryNoteSearchRequest {
                    query: "needle".to_string(),
                    cursor: None,
                    max_results: 1,
                    max_output_bytes: 8 * 1024,
                },
                &cancellation,
            )
            .expect("first search page");
        let cursor = first.next_cursor.expect("directory continuation");

        backend
            .write(
                MemoryNoteWriteRequest {
                    path: path("000-created-before-current.md"),
                    markdown: "needle\n".to_string(),
                    precondition: MemoryNoteWritePrecondition::CreateNew,
                },
                &cancellation,
            )
            .expect("change directory generation");
        let resumed = backend.search(
            MemoryNoteSearchRequest {
                query: "needle".to_string(),
                cursor: Some(cursor),
                max_results: 10,
                max_output_bytes: 8 * 1024,
            },
            &cancellation,
        );
        assert!(
            matches!(resumed, Err(MemoryNoteBackendError::Invalid { .. })),
            "a stale in-page offset must not be replayed against a new directory generation: {resumed:?}"
        );
    }

    #[test]
    fn sifs_memory_search_cursor_rejects_active_note_replacement() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-memory-active-version");
        let data_dir = SinomoDataDir::new(&root);
        let backend = SifsMemoryNoteBackend::new(&data_dir, "main-agent").expect("backend");
        let cancellation = AgentCancellationToken::new();
        let note_path = path("changing.md");
        let created = backend
            .write(
                MemoryNoteWriteRequest {
                    path: note_path.clone(),
                    markdown: "needle first\nneedle second\n".to_string(),
                    precondition: MemoryNoteWritePrecondition::CreateNew,
                },
                &cancellation,
            )
            .expect("create changing note");
        let first = backend
            .search(
                MemoryNoteSearchRequest {
                    query: "needle".to_string(),
                    cursor: None,
                    max_results: 1,
                    max_output_bytes: 8 * 1024,
                },
                &cancellation,
            )
            .expect("first note page");
        assert_eq!(first.matches[0].line, Some(1));
        let cursor = first.next_cursor.expect("active note continuation");

        backend
            .write(
                MemoryNoteWriteRequest {
                    path: note_path,
                    markdown: "needle replacement\n".to_string(),
                    precondition: MemoryNoteWritePrecondition::ReplaceIfVersion {
                        expected_version: created.version,
                    },
                },
                &cancellation,
            )
            .expect("replace active note");
        let resumed = backend.search(
            MemoryNoteSearchRequest {
                query: "needle".to_string(),
                cursor: Some(cursor),
                max_results: 10,
                max_output_bytes: 8 * 1024,
            },
            &cancellation,
        );
        assert!(
            matches!(
                resumed,
                Err(MemoryNoteBackendError::Conflict { .. })
                    | Err(MemoryNoteBackendError::Invalid { .. })
            ),
            "search must not mix note versions across pages: {resumed:?}"
        );
    }

    #[test]
    fn sifs_memory_search_reaches_matches_after_one_mebibyte() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-memory-large-search");
        let data_dir = SinomoDataDir::new(&root);
        let backend = SifsMemoryNoteBackend::new(&data_dir, "main-agent").expect("backend");
        let cancellation = AgentCancellationToken::new();
        let note_path = path("large.md");
        let mut markdown = "filler line\n".repeat(90_000);
        assert!(markdown.len() > MAX_SEARCH_FILE_BYTES as usize);
        markdown.push_str("needle after the first mebibyte\n");
        backend
            .write(
                MemoryNoteWriteRequest {
                    path: note_path.clone(),
                    markdown,
                    precondition: MemoryNoteWritePrecondition::CreateNew,
                },
                &cancellation,
            )
            .expect("create large note");

        let page = backend
            .search(
                MemoryNoteSearchRequest {
                    query: "needle after".to_string(),
                    cursor: None,
                    max_results: 10,
                    max_output_bytes: 8 * 1024,
                },
                &cancellation,
            )
            .expect("search complete large note");
        assert_eq!(page.matches.len(), 1);
        assert_eq!(page.matches[0].path, note_path);
        assert_eq!(page.matches[0].line, Some(90_001));

        let first_read = backend
            .read(
                MemoryNoteReadRequest {
                    path: note_path.clone(),
                    offset: 0,
                    max_bytes: MAX_SEARCH_FILE_BYTES as usize,
                    expected_version: None,
                },
                &cancellation,
            )
            .expect("read first large-note page");
        assert!(first_read.truncated);
        let second_offset = first_read.markdown.len() as u64;
        let second_read = backend
            .read(
                MemoryNoteReadRequest {
                    path: note_path,
                    offset: second_offset,
                    max_bytes: MAX_SEARCH_FILE_BYTES as usize,
                    expected_version: Some(first_read.version.clone()),
                },
                &cancellation,
            )
            .expect("read after one mebibyte");
        assert_eq!(second_read.offset, second_offset);
        assert_eq!(second_read.version, first_read.version);
        assert!(
            second_read
                .markdown
                .contains("needle after the first mebibyte")
        );
        assert!(!second_read.truncated);

        let boundary_query = "cross-boundary-needle";
        let boundary_path = path("single-line-boundary.md");
        let mut boundary_markdown =
            "x".repeat(MAX_SEARCH_FILE_BYTES as usize - boundary_query.len() / 2);
        boundary_markdown.push_str(boundary_query);
        boundary_markdown.push_str(&"y".repeat(8 * 1024));
        backend
            .write(
                MemoryNoteWriteRequest {
                    path: boundary_path.clone(),
                    markdown: boundary_markdown,
                    precondition: MemoryNoteWritePrecondition::CreateNew,
                },
                &cancellation,
            )
            .expect("create range-boundary note");
        let boundary_page = backend
            .search(
                MemoryNoteSearchRequest {
                    query: boundary_query.to_string(),
                    cursor: None,
                    max_results: 10,
                    max_output_bytes: 16 * 1024,
                },
                &cancellation,
            )
            .expect("search across range boundary");
        assert_eq!(boundary_page.matches.len(), 1);
        assert_eq!(boundary_page.matches[0].path, boundary_path);
        assert_eq!(boundary_page.matches[0].line, Some(1));
        assert!(boundary_page.matches[0].snippet.contains(boundary_query));
    }

    #[test]
    fn sifs_memory_search_cursor_cannot_cross_a_directory_link_swap() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-memory-search-directory-swap");
        let data_dir = SinomoDataDir::new(&root);
        let backend = SifsMemoryNoteBackend::new(&data_dir, "main-agent").expect("backend");
        let cancellation = AgentCancellationToken::new();
        for name in ["nested/alpha.md", "nested/bravo.md"] {
            backend
                .write(
                    MemoryNoteWriteRequest {
                        path: path(name),
                        markdown: "needle inside".to_string(),
                        precondition: MemoryNoteWritePrecondition::CreateNew,
                    },
                    &cancellation,
                )
                .expect("create note");
        }
        let first = backend
            .search(
                MemoryNoteSearchRequest {
                    query: "needle".to_string(),
                    cursor: None,
                    max_results: 1,
                    max_output_bytes: 8 * 1024,
                },
                &cancellation,
            )
            .expect("first search page");
        assert_eq!(first.matches.len(), 1);
        let cursor = first.next_cursor.expect("directory continuation");

        let mut opened =
            sinomo_runtime::dfs::open_mounted_sifs_service(Some(data_dir.root().to_path_buf()))
                .expect("open DFS service");
        let options = sinomo_runtime::dfs::DfsWriteOptions::replicated_eventual(vec![
            opened.service.node_id().clone(),
        ]);
        let nested = format!("{}/nested", backend.root());
        for name in ["alpha.md", "bravo.md"] {
            opened
                .service
                .posix_unlink(&format!("{nested}/{name}"))
                .expect("remove listed note");
        }
        opened
            .service
            .posix_rmdir(&nested)
            .expect("remove listed directory");
        opened
            .service
            .posix_mkdir_all("/outside-search")
            .expect("create outside search dir");
        opened
            .service
            .posix_write_file(
                "/outside-search/leaked-name.md",
                b"needle outside",
                options.clone(),
            )
            .expect("create outside search note");
        opened
            .service
            .posix_symlink("/outside-search", &nested, options)
            .expect("swap traversed directory for link");
        drop(opened);

        let resumed = backend.search(
            MemoryNoteSearchRequest {
                query: "needle".to_string(),
                cursor: Some(cursor),
                max_results: 10,
                max_output_bytes: 8 * 1024,
            },
            &cancellation,
        );
        assert!(
            matches!(
                resumed,
                Err(MemoryNoteBackendError::Invalid { .. })
                    | Err(MemoryNoteBackendError::Unavailable { .. })
                    | Err(MemoryNoteBackendError::NotFound { .. })
            ),
            "search must fail closed after a traversed directory becomes a link: {resumed:?}"
        );
    }

    #[test]
    fn sifs_memory_search_deep_cursor_is_bounded_and_resumable() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-memory-notes-deep-cursor");
        let data_dir = SinomoDataDir::new(&root);
        let backend = SifsMemoryNoteBackend::new(&data_dir, "main-agent").expect("backend");
        let cancellation = AgentCancellationToken::new();
        let directory = (0..MAX_SEARCH_DEPTH)
            .map(|index| format!("segment-{index:02}-abcdefghij"))
            .collect::<Vec<_>>()
            .join("/");
        let note_path = path(&format!("{directory}/deep.md"));
        backend
            .write(
                MemoryNoteWriteRequest {
                    path: note_path.clone(),
                    markdown: "needle first\nneedle second\n".to_string(),
                    precondition: MemoryNoteWritePrecondition::CreateNew,
                },
                &cancellation,
            )
            .expect("create deep note");

        let mut cursor = None;
        let mut lines = Vec::new();
        loop {
            let page = backend
                .search(
                    MemoryNoteSearchRequest {
                        query: "needle".to_string(),
                        cursor,
                        max_results: 1,
                        max_output_bytes: 64 * 1024,
                    },
                    &cancellation,
                )
                .expect("search deep page");
            lines.extend(page.matches.into_iter().filter_map(|entry| entry.line));
            if let Some(next_cursor) = page.next_cursor.as_ref() {
                assert!(
                    next_cursor.as_str().len()
                        <= super::super::memory_notes::MAX_MEMORY_NOTE_SEARCH_CURSOR_BYTES
                );
            }
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }

        assert_eq!(lines, vec![1, 2]);
    }

    #[test]
    fn bounded_read_rejects_a_nonadvancing_utf8_page() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-memory-notes-utf8-prefix");
        let data_dir = SinomoDataDir::new(&root);
        let backend = SifsMemoryNoteBackend::new(&data_dir, "main-agent").expect("memory backend");
        let cancellation = AgentCancellationToken::new();
        let note_path = path("unicode.md");
        backend
            .write(
                MemoryNoteWriteRequest {
                    path: note_path.clone(),
                    markdown: "éx".to_string(),
                    precondition: MemoryNoteWritePrecondition::CreateNew,
                },
                &cancellation,
            )
            .expect("create Unicode note");

        let split = backend
            .read(
                MemoryNoteReadRequest {
                    path: note_path.clone(),
                    offset: 0,
                    max_bytes: 1,
                    expected_version: None,
                },
                &cancellation,
            )
            .expect_err("a split scalar cannot produce a continuation");
        assert!(matches!(split, MemoryNoteBackendError::Invalid { .. }));

        let complete = backend
            .read(
                MemoryNoteReadRequest {
                    path: note_path,
                    offset: 0,
                    max_bytes: 2,
                    expected_version: None,
                },
                &cancellation,
            )
            .expect("read complete character");
        assert_eq!(complete.markdown, "é");
        assert!(complete.truncated);
    }
}
