//! File-associated objects on the existing DFS authority/content path. A header
//! and its payload commit together, so there are no dangling blob references or
//! upload-side GC roots. Attachment revisions use normal DFS retention and GC.

use crate::DfsAttachmentResult as Result;
use crate::*;
use std::io::{BufRead, BufReader, Cursor, Read, Seek, SeekFrom};

fn invalid(detail: impl ToString) -> DfsPosixError {
    DfsPosixError::InvalidPath {
        path: format!("attachment: {}", detail.to_string()),
    }
}

fn conflict(path: &str, detail: &str) -> DfsPosixError {
    DfsPosixError::Conflict {
        path: path.into(),
        reason: detail.into(),
    }
}

fn revision(stat: &DfsStat) -> DfsAttachmentRevision {
    DfsAttachmentRevision {
        object_id: stat.object_id.clone(),
        version: stat.version.clone(),
    }
}

pub(crate) fn descriptor(
    header: DfsAttachmentHeader,
    stat: &DfsStat,
    source: &DfsStat,
) -> DfsAttachmentDescriptor {
    let current = header
        .metadata
        .source_version
        .as_ref()
        .is_none_or(|v| v == &source.version);
    DfsAttachmentDescriptor {
        metadata: header.metadata,
        revision: revision(stat),
        operation_id: header.operation_id,
        payload_bytes: header.payload_bytes,
        current,
    }
}

impl<T: DfsMessageTransport> DfsAttachmentService<'_, T> {
    pub(crate) fn attachment_source(&mut self, source: &DfsAttachmentSource) -> Result<DfsStat> {
        if source.path.len() > 4096 {
            return Err(invalid("source path too long"));
        }
        let stat = self.posix_stat_distributed(&source.path)?;
        if stat.file_system_id != source.file_system_id || stat.object_id != source.object_id {
            return Err(conflict(&source.path, "attachment source identity changed"));
        }
        if stat.kind != DfsObjectKind::File
            || stat
                .path
                .trim_start_matches('/')
                .split('/')
                .any(|part| part == ".attachments")
        {
            return Err(invalid("attachments require an ordinary source file"));
        }
        Ok(stat)
    }

    pub(crate) fn attachment_open(
        &mut self,
        source: &DfsAttachmentSource,
        key: &DfsAttachmentKey,
    ) -> Result<Option<(DfsStat, DfsAttachmentHeader)>> {
        let path = dfs_attachment_path(source, key).map_err(invalid)?;
        let stat = match self.posix_open_beneath_no_links_distributed("/", &path) {
            Ok(stat) => stat,
            Err(DfsPosixError::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(error),
        };
        if stat.kind != DfsObjectKind::File {
            return Err(invalid("attachment is not a regular file"));
        }
        let bytes = self.posix_read_file_range_for_opened_stat(
            &path,
            &stat,
            0,
            DFS_ATTACHMENT_HEADER_BYTES,
        )?;
        let header = DfsAttachmentHeader::decode(&bytes).map_err(invalid)?;
        if header.file_system_id != source.file_system_id
            || header.object_id != source.object_id
            || &header.metadata.key != key
            || stat.size_bytes != Some(DFS_ATTACHMENT_HEADER_BYTES as u64 + header.payload_bytes)
        {
            return Err(invalid(
                "attachment header does not match its identity or length",
            ));
        }
        Ok(Some((stat, header)))
    }

    /// Publish a seekable, bounded upload without materializing its complete
    /// payload. Schema validation and the immutable source fence run here for
    /// both local callers and streamed RPCs.
    pub fn file_attachment_from_reader<R: Read + Seek>(
        &mut self,
        request: DfsAttachmentUploadRequest,
        reader: R,
        options: DfsWriteOptions,
    ) -> Result<DfsAttachmentResponse> {
        self.catalog_attachment_put("local-operator", request, reader, options)
    }

    pub(crate) fn file_attachment_from_reader_as<R: Read + Seek>(
        &mut self,
        actor: &str,
        request: DfsAttachmentUploadRequest,
        reader: R,
        options: DfsWriteOptions,
    ) -> Result<DfsAttachmentResponse> {
        self.catalog_attachment_put(actor, request, reader, options)
    }

    /// Bounded operations over the DFS object index. Listing never enumerates
    /// the source directory or filesystem. Catalog reads are bounded to 1 MiB.
    pub fn file_attachment(
        &mut self,
        request: DfsAttachmentRequest,
        options: Option<DfsWriteOptions>,
    ) -> Result<DfsAttachmentResponse> {
        self.file_attachment_as("local-operator", request, options)
    }

    pub(crate) fn file_attachment_as(
        &mut self,
        actor: &str,
        request: DfsAttachmentRequest,
        options: Option<DfsWriteOptions>,
    ) -> Result<DfsAttachmentResponse> {
        if let DfsAttachmentRequest::Maintain {
            source,
            cursor,
            limit,
        } = &request
        {
            if *limit == 0 || *limit > DFS_ATTACHMENT_MAX_PAGE {
                return Err(invalid("maintenance limit must be 1..64"));
            }
            let owner = dfs_attachment_catalog_directory(source);
            let after = if let Some(cursor) = cursor {
                if cursor.len() > 4096 {
                    return Err(invalid("maintenance cursor too large"));
                }
                let (directory, after): (String, DfsDirectoryPageCursor) =
                    serde_json::from_str(cursor).map_err(invalid)?;
                if directory != owner {
                    return Err(invalid("maintenance cursor belongs to another source"));
                }
                Some(after)
            } else {
                None
            };
            let (next, due, reclaimed) =
                self.maintain_attachment_source(source, after.as_ref(), *limit)?;
            return Ok(DfsAttachmentResponse::Maintenance {
                reclaimed,
                next_cursor: next
                    .map(|cursor| serde_json::to_string(&(owner, cursor)).map_err(invalid))
                    .transpose()?,
                next_due_unix_nanos: due,
            });
        }
        let source_stat = self.attachment_source(request.source())?;
        match request {
            DfsAttachmentRequest::Maintain { .. } => unreachable!(),
            DfsAttachmentRequest::ClaimGeneration { upload } => self.claim_attachment_generation(
                actor,
                upload,
                options
                    .ok_or_else(|| invalid("generation claim requires source placement options"))?,
            ),
            DfsAttachmentRequest::Get { source, key } => {
                let attachment = self
                    .catalog_attachment_get(&source, &source_stat, &[key])?
                    .pop()
                    .unwrap();
                Ok(DfsAttachmentResponse::Descriptor { attachment })
            }
            DfsAttachmentRequest::GetMany { source, keys } => {
                if keys.is_empty() || keys.len() > DFS_ATTACHMENT_MAX_PAGE {
                    return Err(invalid("batch requires 1..64 keys"));
                }
                Ok(DfsAttachmentResponse::Descriptors {
                    attachments: self.catalog_attachment_get(&source, &source_stat, &keys)?,
                })
            }
            DfsAttachmentRequest::Read {
                source,
                key,
                revision: expected,
                offset,
                max_bytes,
            } => {
                if max_bytes == 0 || max_bytes > DFS_ATTACHMENT_MAX_RANGE_BYTES {
                    return Err(invalid("read range must be 1..=1 MiB"));
                }
                if let Some(response) = self.catalog_attachment_read(
                    &source,
                    &source_stat,
                    &key,
                    &expected,
                    offset,
                    max_bytes,
                )? {
                    return Ok(response);
                }
                let (stat, header) = self.attachment_open(&source, &key)?.ok_or_else(|| {
                    DfsPosixError::NotFound {
                        path: source.path.clone(),
                    }
                })?;
                if revision(&stat) != expected {
                    return Err(conflict(&source.path, "attachment revision changed"));
                }
                if header
                    .metadata
                    .source_version
                    .as_ref()
                    .is_some_and(|v| v != &source_stat.version)
                {
                    return Err(conflict(
                        &source.path,
                        "attachment is for an older source version",
                    ));
                }
                if offset > header.payload_bytes {
                    return Err(invalid("read offset exceeds attachment length"));
                }
                let len = max_bytes.min((header.payload_bytes - offset) as usize);
                let bytes = self.posix_read_file_range_for_opened_stat(
                    &stat.path,
                    &stat,
                    DFS_ATTACHMENT_HEADER_BYTES as u64 + offset,
                    len,
                )?;
                if bytes.len() != len {
                    return Err(invalid("short attachment range"));
                }
                Ok(DfsAttachmentResponse::Data {
                    eof: offset + bytes.len() as u64 == header.payload_bytes,
                    bytes,
                })
            }
            DfsAttachmentRequest::List {
                source,
                cursor,
                limit,
            } => {
                if limit == 0 || limit > DFS_ATTACHMENT_MAX_PAGE {
                    return Err(invalid("attachment page limit must be 1..=64"));
                }
                let owner = dfs_attachment_catalog_directory(&source);
                let mut cursor = match cursor {
                    Some(cursor) => {
                        if cursor.len() > 4096 {
                            return Err(invalid("cursor too large"));
                        }
                        let cursor: AttachmentListCursor =
                            serde_json::from_str(&cursor).map_err(invalid)?;
                        if cursor.owner != owner {
                            return Err(invalid("cursor belongs to another source"));
                        }
                        cursor
                    }
                    None => AttachmentListCursor {
                        owner,
                        catalog_after: None,
                        legacy: false,
                        legacy_after: None,
                    },
                };
                let view = self.catalog_attachment_view(&source)?;
                let mut attachments = Vec::new();
                if !cursor.legacy {
                    let (page, after) =
                        view.page(&source_stat, cursor.catalog_after.as_deref(), limit);
                    attachments = page;
                    if after.is_some() {
                        cursor.catalog_after = after;
                        return Ok(DfsAttachmentResponse::Page {
                            attachments,
                            next_cursor: Some(serde_json::to_string(&cursor).map_err(invalid)?),
                        });
                    }
                    cursor.legacy = true;
                }
                let directory = dfs_attachment_directory(&source);
                // Scan one bounded legacy page, hiding slots already migrated or
                // deleted in the catalog. New files never pay per-field reads.
                let remaining = limit.saturating_sub(attachments.len());
                let page = match self.posix_list_dir_beneath_no_links_distributed_page(
                    "/",
                    &directory,
                    cursor.legacy_after.as_ref(),
                    remaining.max(1),
                ) {
                    Ok((_, page)) => page,
                    Err(DfsPosixError::NotFound { .. }) => {
                        return Ok(DfsAttachmentResponse::Page {
                            attachments,
                            next_cursor: None,
                        });
                    }
                    Err(error) => return Err(error),
                };
                if remaining == 0 {
                    return Ok(DfsAttachmentResponse::Page {
                        attachments,
                        next_cursor: if page.entries.is_empty() {
                            None
                        } else {
                            Some(serde_json::to_string(&cursor).map_err(invalid)?)
                        },
                    });
                }
                for entry in page.entries {
                    let Some((kind, name)) = entry
                        .name
                        .strip_suffix(".satt")
                        .and_then(|s| s.split_once('~'))
                    else {
                        return Err(invalid("unexpected legacy attachment entry"));
                    };
                    let key = DfsAttachmentKey {
                        kind: kind.into(),
                        name: name.into(),
                    };
                    if !view.contains(&key) {
                        if let Some((stat, header)) = self.attachment_open(&source, &key)? {
                            attachments.push(descriptor(header, &stat, &source_stat));
                        }
                    }
                }
                let next_cursor = if let Some(after) = page.next_cursor {
                    cursor.legacy_after = Some(after);
                    Some(serde_json::to_string(&cursor).map_err(invalid)?)
                } else {
                    None
                };
                Ok(DfsAttachmentResponse::Page {
                    attachments,
                    next_cursor,
                })
            }

            DfsAttachmentRequest::Put {
                source,
                metadata,
                expected,
                operation_id,
                bytes,
            } => {
                let upload = DfsAttachmentUploadRequest {
                    generation: false,
                    source,
                    metadata,
                    expected,
                    operation_id,
                    payload_bytes: bytes.len() as u64,
                };
                self.file_attachment_from_reader_as(
                    actor,
                    upload,
                    Cursor::new(bytes),
                    options.ok_or_else(|| {
                        invalid("attachment write requires source placement options")
                    })?,
                )
            }

            DfsAttachmentRequest::Delete {
                source,
                key,
                expected,
                operation_id,
            } => {
                let options = match options {
                    Some(options) => options,
                    None => {
                        let (_, candidates, policy) = self.managed_replication_policy_for_path(
                            &source_stat.path,
                            source_stat.policy.clone(),
                            source_stat.current_locations.len().max(1),
                        )?;
                        let replica_candidates = if candidates.is_empty() {
                            vec![self.node_id().clone()]
                        } else {
                            candidates
                                .into_iter()
                                .filter(|c| c.accepts_content)
                                .map(|c| c.node_id)
                                .collect()
                        };
                        DfsWriteOptions {
                            policy,
                            replica_candidates,
                            wait_for_commit: true,
                            foreground_replica_repair: false,
                        }
                    }
                };
                self.catalog_attachment_delete(
                    actor,
                    &source,
                    &key,
                    &expected,
                    &operation_id,
                    options,
                )
            }
        }
    }
}

pub(crate) fn validate_attachment_reader<R: Read + Seek>(
    metadata: &DfsAttachmentMetadata,
    reader: &mut R,
    size: u64,
) -> Result<String> {
    metadata.validate().map_err(invalid)?;
    if size > DFS_ATTACHMENT_MAX_PAYLOAD_BYTES as u64
        || reader.seek(SeekFrom::End(0)).map_err(invalid)? != size
    {
        return Err(invalid(
            "attachment upload length does not match its bounded declaration",
        ));
    }
    reader.rewind().map_err(invalid)?;
    match metadata.key.kind.as_str() {
        DFS_TAGS_KIND => {
            if size > 64 * 1024 {
                return Err(invalid("tags exceed 64 KiB"));
            }
            let mut bytes = Vec::with_capacity(size as usize);
            reader
                .take(size + 1)
                .read_to_end(&mut bytes)
                .map_err(invalid)?;
            validate_dfs_attachment_payload(metadata, &bytes).map_err(invalid)?;
        }
        DFS_TRANSCRIPT_KIND => {
            let mut buffered = BufReader::new(&mut *reader);
            let mut line = Vec::new();
            let mut previous = 0;
            loop {
                line.clear();
                let read = (&mut buffered)
                    .take(128 * 1024 + 2)
                    .read_until(b'\n', &mut line)
                    .map_err(invalid)?;
                if read == 0 {
                    break;
                }
                let bytes = line.strip_suffix(b"\n").unwrap_or(&line);
                if bytes.is_empty() {
                    continue;
                }
                if bytes.len() > 128 * 1024 {
                    return Err(invalid("transcript line exceeds 128 KiB"));
                }
                let segment: DfsTranscriptSegment =
                    serde_json::from_slice(bytes).map_err(invalid)?;
                segment.validate().map_err(invalid)?;
                if segment.start_millis < previous {
                    return Err(invalid("transcript segments must be ordered"));
                }
                previous = segment.start_millis;
            }
        }
        DFS_THUMBNAIL_KIND => {
            let mut signature = [0; 8];
            reader.read_exact(&mut signature).map_err(invalid)?;
            if size > 1024 * 1024 || &signature != b"\x89PNG\r\n\x1a\n" {
                return Err(invalid("thumbnail must be bounded PNG"));
            }
        }
        _ => {}
    }
    reader.rewind().map_err(invalid)?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0u8; 64 * 1024];
    let mut total = 0u64;
    loop {
        let n = reader.read(&mut buffer).map_err(invalid)?;
        if n == 0 {
            break;
        }
        total = total
            .checked_add(n as u64)
            .ok_or_else(|| invalid("attachment length overflow"))?;
        if total > size {
            return Err(invalid("attachment grew during validation"));
        }
        hasher.update(&buffer[..n]);
    }
    if total != size {
        return Err(invalid("attachment changed during validation"));
    }
    reader.rewind().map_err(invalid)?;
    Ok(format!("blake3:{}", hasher.finalize().to_hex()))
}

#[derive(serde::Serialize, serde::Deserialize)]
struct AttachmentListCursor {
    owner: String,
    catalog_after: Option<String>,
    legacy: bool,
    legacy_after: Option<DfsDirectoryPageCursor>,
}
