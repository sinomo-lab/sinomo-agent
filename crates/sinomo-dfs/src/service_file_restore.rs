//! Restore input streaming. Durable execution lives in the namespace job lane.

use crate::*;
use std::io::Read;

pub(crate) enum FileRestoreInput {
    Reader {
        reader: Box<dyn Read>,
        content_hash: String,
        size_bytes: u64,
    },
    Quarantined(ContentStoreQuarantinedBlob),
}

impl FileRestoreInput {
    pub(crate) fn into_quarantined(
        self,
        content: &LocalContentStore,
    ) -> Result<ContentStoreQuarantinedBlob, DfsError> {
        let Self::Reader {
            mut reader,
            content_hash,
            size_bytes,
        } = self
        else {
            let Self::Quarantined(blob) = self else {
                unreachable!()
            };
            return Ok(blob);
        };
        // This is write ingestion with a known source identity, not a scrub
        // added to ordinary reads. Publication cannot precede this check.
        let mut ingest = content.begin_quarantined_blob_ingest(size_bytes, Some(&content_hash))?;
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            let count = reader
                .read(&mut buffer)
                .map_err(|source| DfsError::ContentStore {
                    message: source.to_string(),
                })?;
            if count == 0 {
                break;
            }
            ingest.write_chunk(&buffer[..count])?;
        }
        ingest.finish_quarantined().map_err(Into::into)
    }
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub fn restore_file_history(
        &mut self,
        request: DfsFileHistoryRestoreRequest,
        options: DfsWriteOptions,
    ) -> Result<DfsFileHistoryRestoreResponse, DfsError> {
        self.restore_file_history_durable(request, options)
    }

    pub(crate) fn open_file_restore_reader(
        &mut self,
        descriptor: &DfsFileHistoryDescriptor,
    ) -> Result<FileRestoreInput, DfsError> {
        let version = &descriptor.version;
        let hash = version
            .content_hash
            .as_deref()
            .ok_or_else(|| DfsError::Metadata {
                message: "retained source has no content identity".into(),
            })?;
        let size = version.size_bytes.ok_or_else(|| DfsError::Metadata {
            message: "retained source has no content length".into(),
        })?;
        if let Ok((_, reader)) =
            self.open_content_hash_stream(&version.object_id, &version.version, hash, Some(size))
        {
            return Ok(FileRestoreInput::Reader {
                reader: Box::new(reader),
                content_hash: hash.to_string(),
                size_bytes: size,
            });
        }
        let content = self.content.clone();
        let mut ingest = content.begin_quarantined_blob_ingest(size, Some(hash))?;
        let mut offset = 0;
        while offset < size {
            match self.read_file_history_descriptor(
                descriptor,
                offset,
                crate::file_history_query::DFS_FILE_HISTORY_RANGE_LIMIT,
            )? {
                DfsFileHistoryReadOutcome::Found { data } if !data.bytes.is_empty() => {
                    offset += data.bytes.len() as u64;
                    ingest.write_chunk(&data.bytes)?;
                }
                _ => {
                    return Err(DfsError::Metadata {
                        message:
                            "history-content-unavailable: restore source has no readable replica"
                                .into(),
                    });
                }
            }
        }
        Ok(FileRestoreInput::Quarantined(ingest.finish_quarantined()?))
    }
}
