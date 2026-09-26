//! File-associated objects on the existing DFS authority/content path. A header
//! and its payload commit together, so there are no dangling blob references or
//! upload-side GC roots. Attachment revisions use normal DFS retention and GC.

use crate::posix_model::Result;
use crate::*;
use std::io::{Cursor, Read};

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

fn descriptor(
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

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    fn attachment_source(&mut self, source: &DfsAttachmentSource) -> Result<DfsStat> {
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
                .starts_with(".attachments/")
        {
            return Err(invalid("attachments require an ordinary source file"));
        }
        Ok(stat)
    }

    fn attachment_open(
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

    /// Bounded operations over the DFS object index. Listing never enumerates
    /// the source directory or filesystem, and descriptor reads fetch 4 KiB.
    pub fn file_attachment(
        &mut self,
        request: DfsAttachmentRequest,
        options: Option<DfsWriteOptions>,
    ) -> Result<DfsAttachmentResponse> {
        let source_stat = self.attachment_source(request.source())?;
        match request {
            DfsAttachmentRequest::Get { source, key } => {
                let attachment = self
                    .attachment_open(&source, &key)?
                    .map(|(stat, header)| descriptor(header, &stat, &source_stat));
                Ok(DfsAttachmentResponse::Descriptor { attachment })
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
                let directory = dfs_attachment_directory(&source);
                let after = match cursor {
                    Some(cursor) => {
                        if cursor.len() > 4096 {
                            return Err(invalid("cursor too large"));
                        }
                        let (owner, after): (String, DfsDirectoryPageCursor) =
                            serde_json::from_str(&cursor).map_err(invalid)?;
                        if owner != directory {
                            return Err(invalid("cursor belongs to another source"));
                        }
                        Some(after)
                    }
                    None => None,
                };
                let page = match self.posix_list_dir_beneath_no_links_distributed_page(
                    "/",
                    &directory,
                    after.as_ref(),
                    limit,
                ) {
                    Ok((_, page)) => page,
                    Err(DfsPosixError::NotFound { .. }) => {
                        return Ok(DfsAttachmentResponse::Page {
                            attachments: vec![],
                            next_cursor: None,
                        });
                    }
                    Err(error) => return Err(error),
                };
                let mut attachments = Vec::with_capacity(page.entries.len());
                for entry in page.entries {
                    let Some((kind, name)) = entry
                        .name
                        .strip_suffix(".satt")
                        .and_then(|s| s.split_once('~'))
                    else {
                        return Err(invalid("unexpected attachment entry"));
                    };
                    let key = DfsAttachmentKey {
                        kind: kind.into(),
                        name: name.into(),
                    };
                    if let Some((stat, header)) = self.attachment_open(&source, &key)? {
                        attachments.push(descriptor(header, &stat, &source_stat));
                    }
                }
                let next_cursor = page
                    .next_cursor
                    .map(|c| serde_json::to_string(&(directory, c)).map_err(invalid))
                    .transpose()?;
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
                validate_dfs_attachment_payload(&metadata, &bytes).map_err(invalid)?;
                if metadata
                    .source_version
                    .as_ref()
                    .is_some_and(|v| v != &source_stat.version)
                {
                    return Err(conflict(
                        &source.path,
                        "source version changed before attachment publication",
                    ));
                }
                let path = dfs_attachment_path(&source, &metadata.key).map_err(invalid)?;
                let header = DfsAttachmentHeader {
                    file_system_id: source.file_system_id.clone(),
                    object_id: source.object_id.clone(),
                    metadata,
                    payload_bytes: bytes.len() as u64,
                    operation_id: operation_id.clone(),
                };
                let encoded = header.encode().map_err(invalid)?;
                let options = options
                    .ok_or_else(|| invalid("attachment write requires source placement options"))?;
                // Use the existing streaming commit to avoid copying the payload into
                // another envelope-sized buffer. Stable operation IDs fence retries.
                let reader = Cursor::new(encoded).chain(Cursor::new(bytes));
                let stat = match expected {
                    Some(expected) => self
                        .posix_replace_file_if_version_from_reader_with_operation_id(
                            &path,
                            &expected.object_id,
                            &expected.version,
                            reader,
                            options,
                            &operation_id,
                        )?,
                    None => {
                        let directory = dfs_attachment_directory(&source);
                        self.posix_mkdir_all(&directory)?;
                        self.posix_open_beneath_no_links_distributed("/", &directory)?;
                        self.posix_create_file_from_reader_with_operation_id(
                            &path,
                            reader,
                            options,
                            &operation_id,
                        )?
                    }
                };
                // A source edit may race the independent attachment transaction.
                // Reads always compare the binding; report freshness from a fresh
                // source observation rather than claiming an atomic cross-object edit.
                let mut attachment = descriptor(header, &stat, &source_stat);
                attachment.current = self.attachment_source(&source).ok().is_some_and(|current| {
                    attachment
                        .metadata
                        .source_version
                        .as_ref()
                        .is_none_or(|version| version == &current.version)
                });
                Ok(DfsAttachmentResponse::Stored { attachment })
            }
            DfsAttachmentRequest::Delete {
                source,
                key,
                expected,
                operation_id,
            } => {
                let path = dfs_attachment_path(&source, &key).map_err(invalid)?;
                self.posix_unlink_if_generation(
                    &path,
                    operation_id,
                    DfsNamespaceObjectPrecondition {
                        object_id: expected.object_id,
                        version: expected.version,
                    },
                )?;
                Ok(DfsAttachmentResponse::Deleted)
            }
        }
    }
}
