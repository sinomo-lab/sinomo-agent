//! Verified immutable content input for durable conditional copy jobs.

use crate::*;
use std::io::Read;

/// Copy ingestion always checks exact bytes before destination publication.
pub enum VerifiedContentInput {
    Reader {
        reader: Box<dyn Read>,
        content_hash: String,
        size_bytes: u64,
    },
    Quarantined(ContentStoreQuarantinedBlob),
}

impl VerifiedContentInput {
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

/// Current policy and ownership authorize a protected revision's bytes. Opaque
/// module metadata is never consulted by the content-copy engine.
pub(crate) fn copy_revision_content_object(
    descriptor: &DfsRetainedRevisionDescriptor,
) -> Result<DfsObjectMetadata, DfsError> {
    let version = &descriptor.version;
    if descriptor.current_object.version == version.version
        && descriptor.current_object.kind != DfsObjectKind::Tombstone
    {
        return Ok(descriptor.current_object.clone());
    }
    let grant = version
        .retention
        .values()
        .find_map(|state| state.grant.as_deref())
        .ok_or_else(|| DfsError::Metadata {
            message: "content revision has no live grant".into(),
        })?;
    let mut object = descriptor.current_object.clone();
    object.kind = grant.object.kind.clone();
    object.version = version.version.clone();
    object.content_hash = version.content_hash.clone();
    object.content_size_bytes = version.size_bytes;
    object.current_locations = version.locations.clone();
    Ok(object)
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub(crate) fn open_verified_content_input(
        &mut self,
        identity: &DfsRevisionReference,
        expected_hash: &str,
        expected_size: u64,
    ) -> Result<VerifiedContentInput, DfsError> {
        // Re-resolve authority here. A supplied hash, revision descriptor, or
        // local content cache cannot itself authorize source bytes.
        let descriptor =
            self.resolve_readable_revision(identity)?
                .ok_or_else(|| DfsError::Metadata {
                    message: "content copy source is neither current nor granted".into(),
                })?;
        let object = copy_revision_content_object(&descriptor)?;
        if object.content_hash.as_deref() != Some(expected_hash)
            || object.content_size_bytes != Some(expected_size)
            || !matches!(object.kind, DfsObjectKind::File | DfsObjectKind::Artifact)
        {
            return Err(DfsError::Metadata {
                message: "content copy source identity changed".into(),
            });
        }
        if let Ok((_, reader)) = self.open_content_hash_stream(
            &identity.object_id,
            &identity.version,
            expected_hash,
            Some(expected_size),
        ) {
            return Ok(VerifiedContentInput::Reader {
                reader: Box::new(reader),
                content_hash: expected_hash.into(),
                size_bytes: expected_size,
            });
        }
        let content = self.content.clone();
        let mut ingest =
            content.begin_quarantined_blob_ingest(expected_size, Some(expected_hash))?;
        let mut offset = 0;
        while offset < expected_size {
            match self.read_content_revision_range(&object, offset, 64 * 1024)? {
                NetworkedDfsRangeReadOutcome::Found { data, .. } if !data.bytes.is_empty() => {
                    if data.object_id != identity.object_id
                        || data.version != identity.version
                        || data.content_hash.as_deref() != Some(expected_hash)
                        || data.offset != offset
                        || data.total_size_bytes != expected_size
                        || data.bytes.len() as u64 > expected_size - offset
                    {
                        return Err(DfsError::Metadata {
                            message: "content copy range differs from its authorized source".into(),
                        });
                    }
                    offset += data.bytes.len() as u64;
                    ingest.write_chunk(&data.bytes)?;
                }
                _ => {
                    return Err(DfsError::Metadata {
                        message: "content copy source has no readable replica".into(),
                    });
                }
            }
        }
        Ok(VerifiedContentInput::Quarantined(
            ingest.finish_quarantined()?,
        ))
    }
}
