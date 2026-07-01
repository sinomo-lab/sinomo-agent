//! Service-side object content access helpers.
//!
//! These helpers centralize how the networked service opens current object
//! bytes, validates content hashes and sizes, reads patch ranges, and derives
//! size hints from local manifests. They sit above the content store so write,
//! transfer, repair, and POSIX paths all use the same validation behavior.

use std::io::Read;

use crate::metadata_pages::walk_metadata_pages;
use crate::transfer::PendingNetworkedWrite;
use crate::*;

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub(crate) fn open_pending_write_blob_stream(
        &self,
        pending: &PendingNetworkedWrite,
    ) -> Result<ObjectVersionReader, DfsError> {
        if let Ok((manifest, reader)) = self
            .content
            .open_object_version_stream(pending.object_id.as_str(), pending.version.as_str())
        {
            if manifest.content_hash == pending.content_hash
                && (pending.size_bytes == 0 || manifest.size_bytes == pending.size_bytes)
            {
                return Ok(reader);
            }
            return Err(DfsError::Distributed {
                message: format!(
                    "pending write manifest mismatch for {} {}: expected hash {} size {}, actual hash {} size {}",
                    pending.object_id,
                    pending.version,
                    pending.content_hash,
                    pending.size_bytes,
                    manifest.content_hash,
                    manifest.size_bytes
                ),
            });
        }

        let reader = self.content.open_blob_stream(&pending.content_hash)?;
        if pending.size_bytes != 0 && reader.size_bytes() != pending.size_bytes {
            return Err(DfsError::Distributed {
                message: format!(
                    "pending write blob size mismatch for {} {}: expected {}, actual {}",
                    pending.object_id,
                    pending.version,
                    pending.size_bytes,
                    reader.size_bytes()
                ),
            });
        }
        Ok(ObjectVersionReader::from_blob(reader))
    }

    pub(crate) fn open_content_hash_stream(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        content_hash: &str,
        expected_size_bytes: Option<u64>,
    ) -> Result<(u64, ObjectVersionReader), DfsError> {
        if let Ok((manifest, reader)) = self
            .content
            .open_object_version_stream(object_id.as_str(), version.as_str())
        {
            if manifest.content_hash != content_hash {
                return Err(DfsError::ContentHashMismatch {
                    expected: content_hash.to_string(),
                    actual: manifest.content_hash,
                });
            }
            if let Some(expected_size_bytes) = expected_size_bytes
                && manifest.size_bytes != expected_size_bytes
            {
                return Err(DfsError::Distributed {
                    message: format!(
                        "object version size mismatch for {object_id} {version}: expected {expected_size_bytes}, actual {}",
                        manifest.size_bytes
                    ),
                });
            }
            return Ok((manifest.size_bytes, reader));
        }

        let reader = self.content.open_blob_stream(content_hash)?;
        if let Some(expected_size_bytes) = expected_size_bytes
            && reader.size_bytes() != expected_size_bytes
        {
            return Err(DfsError::Distributed {
                message: format!(
                    "content blob size mismatch for {object_id} {version}: expected {expected_size_bytes}, actual {}",
                    reader.size_bytes()
                ),
            });
        }
        let size_bytes = reader.size_bytes();
        Ok((size_bytes, ObjectVersionReader::from_blob(reader)))
    }

    /// Reads an object version by expected content hash, preferring the
    /// object-version manifest and falling back to the content-addressed blob.
    pub fn read_object_version_content(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        content_hash: &str,
        expected_size_bytes: Option<u64>,
    ) -> Result<Vec<u8>, DfsError> {
        let (size_bytes, mut reader) =
            self.open_content_hash_stream(object_id, version, content_hash, expected_size_bytes)?;
        let mut bytes = Vec::with_capacity(usize::try_from(size_bytes).unwrap_or(0));
        reader
            .read_to_end(&mut bytes)
            .map_err(|source| DfsError::ContentStore {
                message: format!("cannot read DFS object {object_id} version {version}: {source}"),
            })?;
        Ok(bytes)
    }

    pub(crate) fn open_content_hash_range_stream(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        content_hash: &str,
        expected_size_bytes: Option<u64>,
        offset: u64,
        max_bytes: usize,
    ) -> Result<ObjectVersionReader, DfsError> {
        if let Ok((manifest, reader)) = self.content.open_object_version_range_stream(
            object_id.as_str(),
            version.as_str(),
            offset,
            max_bytes,
        ) {
            if manifest.content_hash != content_hash {
                return Err(DfsError::ContentHashMismatch {
                    expected: content_hash.to_string(),
                    actual: manifest.content_hash,
                });
            }
            if let Some(expected_size_bytes) = expected_size_bytes
                && manifest.size_bytes != expected_size_bytes
            {
                return Err(DfsError::Distributed {
                    message: format!(
                        "object version range size mismatch for {object_id} {version}: expected {expected_size_bytes}, actual {}",
                        manifest.size_bytes
                    ),
                });
            }
            return Ok(reader);
        }

        let reader = self
            .content
            .open_blob_range_stream(content_hash, offset, max_bytes)?;
        if let Some(expected_size_bytes) = expected_size_bytes {
            let range_start = reader.offset();
            let range_end = range_start.saturating_add(reader.remaining_bytes());
            if range_start > expected_size_bytes || range_end > expected_size_bytes {
                return Err(DfsError::Distributed {
                    message: format!(
                        "content blob range for {object_id} {version} exceeds expected size {expected_size_bytes}: {range_start}..{range_end}"
                    ),
                });
            }
        }
        Ok(ObjectVersionReader::from_blob_range(reader))
    }

    pub(crate) fn read_patch_blob_range(
        &self,
        patch_hash: &str,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>, DfsError> {
        let len = usize::try_from(len).map_err(|_| DfsError::Distributed {
            message: format!("DFS patch range is too large to fit in memory: {len} bytes"),
        })?;
        let mut reader = self
            .content
            .open_blob_range_stream(patch_hash, offset, len)
            .map_err(|source| DfsError::ContentStore {
                message: format!(
                    "failed to open DFS patch blob {patch_hash} at {offset}+{len}: {source}"
                ),
            })?;
        let mut bytes = vec![0_u8; len];
        reader
            .read_exact(&mut bytes)
            .map_err(|source| DfsError::ContentStore {
                message: format!(
                    "failed to read DFS patch blob {patch_hash} at {offset}+{len}: {source}"
                ),
            })?;
        Ok(bytes)
    }

    pub(crate) fn object_content_size_hint(
        &self,
        object: &DfsObjectMetadata,
    ) -> Result<Option<u64>, DfsError> {
        if !matches!(object.kind, DfsObjectKind::File | DfsObjectKind::Symlink) {
            return Ok(None);
        }
        if let Ok(manifest) = self
            .content
            .read_manifest(object.object_id.as_str(), object.version.as_str())
        {
            return Ok(Some(manifest.size_bytes));
        }
        if object.content_size_bytes.is_some() {
            return Ok(object.content_size_bytes);
        }
        if let Some(backend) = self.metadata_backend.as_ref()
            && let Some(version) = backend.get_version(&object.object_id, &object.version)?
            && version.size_bytes.is_some()
        {
            return Ok(version.size_bytes);
        }
        if let Some(expected_hash) = object.content_hash.as_ref()
            && let Some(manifest) = self
                .content
                .manifests_for_object(object.object_id.as_str())?
                .into_iter()
                .rev()
                .find(|manifest| &manifest.content_hash == expected_hash)
        {
            return Ok(Some(manifest.size_bytes));
        }
        if let Some(backend) = self.metadata_backend.as_ref()
            && let Some(expected_hash) = object.content_hash.as_ref()
        {
            let mut matching_size_bytes = None;
            walk_metadata_pages(|cursor| {
                let page = backend.versions_for_object_page(
                    &object.object_id,
                    cursor,
                    DFS_METADATA_OBJECT_VERSION_PAGE_SIZE,
                )?;
                for version in page.versions {
                    if version.content_hash.as_ref() == Some(expected_hash) {
                        matching_size_bytes = Some(version.size_bytes);
                    }
                }
                Ok(page.next_cursor)
            })?;
            if let Some(size_bytes) = matching_size_bytes {
                return Ok(size_bytes);
            }
        }
        Ok(None)
    }

    pub(crate) fn local_object_version_manifest_matches(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        expected_hash: &str,
        expected_size_bytes: u64,
    ) -> bool {
        if self
            .content
            .read_manifest(object_id.as_str(), version.as_str())
            .is_ok_and(|manifest| {
                manifest.content_hash == expected_hash
                    && manifest.size_bytes == expected_size_bytes
                    && self
                        .content
                        .manifest_content_available(&manifest)
                        .unwrap_or(false)
            })
        {
            return true;
        }
        self.content
            .open_blob_stream(expected_hash)
            .is_ok_and(|reader| reader.size_bytes() == expected_size_bytes)
    }

    pub(crate) fn local_object_version_manifest_size_if_matches(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        expected_hash: &str,
    ) -> Option<u64> {
        if let Some(size_bytes) = self
            .content
            .read_manifest(object_id.as_str(), version.as_str())
            .ok()
            .filter(|manifest| {
                manifest.content_hash == expected_hash
                    && self
                        .content
                        .manifest_content_available(manifest)
                        .unwrap_or(false)
            })
            .map(|manifest| manifest.size_bytes)
        {
            return Some(size_bytes);
        }
        self.content
            .open_blob_stream(expected_hash)
            .ok()
            .map(|reader| reader.size_bytes())
    }
}

pub(crate) fn object_content_identity(
    object: &DfsObjectMetadata,
) -> Option<ContentVersionIdentity> {
    Some(ContentVersionIdentity {
        content_hash: object.content_hash.clone()?,
        size_bytes: object.content_size_bytes?,
    })
}
