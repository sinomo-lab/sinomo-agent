use super::{
    ContentManifest, ContentStoreError, ContentStoreQuotaDelta, ContentVersionIdentity,
    LocalContentStore, Result, create_dir_all, file_content_hash_and_size, hash_hex,
    remove_file_if_exists, temporary_path_for,
};
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

/// Incremental assembler for a chunked DFS object-version transfer.
///
/// Dropping an unfinished assembler removes its temporary staging file. Callers
/// should pass a complete assembler back to `LocalContentStore` to commit the
/// assembled object or patch.
#[derive(Debug)]
pub struct ChunkedBlobAssembler {
    object_id: String,
    version: String,
    expected_hash: String,
    total_size_bytes: u64,
    total_chunks: u32,
    chunk_size_bytes: usize,
    received_chunks: Vec<bool>,
    received_count: u32,
    path: PathBuf,
    file: fs::File,
    store: LocalContentStore,
    cleanup_on_drop: bool,
}

impl ChunkedBlobAssembler {
    pub fn insert_chunk(&mut self, chunk_index: u32, bytes: &[u8]) -> Result<bool> {
        self.validate_chunk(chunk_index, bytes)?;
        let offset = u64::from(chunk_index) * self.chunk_size_bytes as u64;
        if self.received_chunks[chunk_index as usize] {
            let mut existing = vec![0_u8; bytes.len()];
            self.file
                .seek(SeekFrom::Start(offset))
                .and_then(|_| self.file.read_exact(&mut existing))
                .map_err(|source| ContentStoreError::Io {
                    path: self.path.clone(),
                    source,
                })?;
            if existing != bytes {
                return Err(ContentStoreError::ConflictingChunk {
                    object_id: self.object_id.clone(),
                    version: self.version.clone(),
                    chunk_index,
                });
            }
            return Ok(self.is_complete());
        }

        self.file
            .seek(SeekFrom::Start(offset))
            .and_then(|_| self.file.write_all(bytes))
            .map_err(|source| ContentStoreError::Io {
                path: self.path.clone(),
                source,
            })?;
        self.received_chunks[chunk_index as usize] = true;
        self.received_count += 1;
        Ok(self.is_complete())
    }

    pub fn is_complete(&self) -> bool {
        self.received_count == self.total_chunks
    }

    pub fn contiguous_received_chunk_count(&self) -> u32 {
        self.received_chunks
            .iter()
            .take_while(|received| **received)
            .count()
            .try_into()
            .expect("chunk count fits in u32")
    }

    pub fn total_size_bytes(&self) -> u64 {
        self.total_size_bytes
    }

    fn validate_chunk(&self, chunk_index: u32, bytes: &[u8]) -> Result<()> {
        if chunk_index >= self.total_chunks {
            return Err(ContentStoreError::InvalidChunkedContent {
                message: format!(
                    "chunk index out of range for object {}, version {}: {chunk_index}/{}",
                    self.object_id, self.version, self.total_chunks
                ),
            });
        }
        let expected_len = self.expected_chunk_len(chunk_index)?;
        if bytes.len() as u64 != expected_len {
            return Err(ContentStoreError::InvalidChunkedContent {
                message: format!(
                    "chunk length mismatch for object {}, version {}, chunk {chunk_index}: expected {expected_len}, actual {}",
                    self.object_id,
                    self.version,
                    bytes.len()
                ),
            });
        }
        Ok(())
    }

    fn expected_chunk_len(&self, chunk_index: u32) -> Result<u64> {
        let chunk_size = self.chunk_size_bytes as u64;
        let offset = u64::from(chunk_index)
            .checked_mul(chunk_size)
            .ok_or_else(|| ContentStoreError::InvalidChunkedContent {
                message: format!(
                    "chunk offset overflow for object {}, version {}",
                    self.object_id, self.version
                ),
            })?;
        if offset >= self.total_size_bytes {
            return Err(ContentStoreError::InvalidChunkedContent {
                message: format!(
                    "chunk offset exceeds transfer size for object {}, version {}",
                    self.object_id, self.version
                ),
            });
        }
        Ok((self.total_size_bytes - offset).min(chunk_size))
    }
}

impl Drop for ChunkedBlobAssembler {
    fn drop(&mut self) {
        if self.cleanup_on_drop {
            let _ = self.store.remove_tracked_staging_file_if_exists(&self.path);
        }
    }
}

impl LocalContentStore {
    pub fn begin_chunked_object_version(
        &self,
        object_id: &str,
        version: &str,
        expected_hash: &str,
        total_size_bytes: u64,
        total_chunks: u32,
        chunk_size_bytes: usize,
    ) -> Result<ChunkedBlobAssembler> {
        validate_chunked_object_shape(total_size_bytes, total_chunks, chunk_size_bytes)?;
        hash_hex(expected_hash)?;
        let received_len = usize::try_from(total_chunks).map_err(|_| {
            ContentStoreError::InvalidChunkedContent {
                message: format!("chunk count is too large for this platform: {total_chunks}"),
            }
        })?;
        let staging_anchor = self.root.join("staging").join("chunked-object-version");
        let path = temporary_path_for(&staging_anchor);
        self.ensure_quota_delta(ContentStoreQuotaDelta {
            staging_bytes: total_size_bytes,
            ..ContentStoreQuotaDelta::default()
        })?;
        if let Some(parent) = path.parent() {
            create_dir_all(parent.to_owned())?;
        }
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|source| ContentStoreError::Io {
                path: path.clone(),
                source,
            })?;
        file.set_len(total_size_bytes)
            .map_err(|source| ContentStoreError::Io {
                path: path.clone(),
                source,
            })?;
        if let Err(error) = self.track_staging_file_created(&path) {
            remove_file_if_exists(&path).ok();
            return Err(error);
        }
        Ok(ChunkedBlobAssembler {
            object_id: object_id.to_owned(),
            version: version.to_owned(),
            expected_hash: expected_hash.to_owned(),
            total_size_bytes,
            total_chunks,
            chunk_size_bytes,
            received_chunks: vec![false; received_len],
            received_count: 0,
            path,
            file,
            store: self.clone(),
            cleanup_on_drop: true,
        })
    }

    pub fn commit_chunked_object_version(
        &self,
        mut assembler: ChunkedBlobAssembler,
    ) -> Result<ContentManifest> {
        if !assembler.is_complete() {
            return Err(ContentStoreError::IncompleteChunkedContent {
                object_id: assembler.object_id.clone(),
                version: assembler.version.clone(),
            });
        }
        assembler
            .file
            .sync_all()
            .map_err(|source| ContentStoreError::Io {
                path: assembler.path.clone(),
                source,
            })?;
        let (actual_hash, actual_size_bytes) = file_content_hash_and_size(&assembler.path)?;
        if actual_size_bytes != assembler.total_size_bytes {
            return Err(ContentStoreError::InvalidChunkedContent {
                message: format!(
                    "assembled chunked object has wrong size for object {}, version {}: expected {}, actual {actual_size_bytes}",
                    assembler.object_id, assembler.version, assembler.total_size_bytes
                ),
            });
        }
        if actual_hash != assembler.expected_hash {
            return Err(ContentStoreError::ChunkHashMismatch {
                object_id: assembler.object_id.clone(),
                version: assembler.version.clone(),
                expected_hash: assembler.expected_hash.clone(),
                actual_hash,
            });
        }
        let manifest = self.write_object_version_from_verified_file(
            &assembler.object_id,
            &assembler.version,
            &assembler.path,
            actual_hash,
            actual_size_bytes,
        )?;
        self.remove_tracked_staging_file_if_exists(&assembler.path)?;
        assembler.cleanup_on_drop = false;
        Ok(manifest)
    }

    pub fn commit_chunked_object_version_patch(
        &self,
        assembler: ChunkedBlobAssembler,
        base_version: &str,
        offset: u64,
    ) -> Result<ContentManifest> {
        self.commit_chunked_object_version_patch_with_base_identity(
            assembler,
            base_version,
            offset,
            None,
        )
    }

    pub fn commit_chunked_object_version_patch_with_base_identity(
        &self,
        assembler: ChunkedBlobAssembler,
        base_version: &str,
        offset: u64,
        base_identity: Option<&ContentVersionIdentity>,
    ) -> Result<ContentManifest> {
        self.commit_chunked_object_version_patch_impl(
            assembler,
            base_version,
            offset,
            None,
            base_identity,
        )
    }

    pub(crate) fn commit_chunked_object_version_patch_with_trusted_content_hash_and_base_identity(
        &self,
        assembler: ChunkedBlobAssembler,
        base_version: &str,
        offset: u64,
        trusted_content_hash: &str,
        base_identity: Option<&ContentVersionIdentity>,
    ) -> Result<ContentManifest> {
        self.commit_chunked_object_version_patch_impl(
            assembler,
            base_version,
            offset,
            Some(trusted_content_hash),
            base_identity,
        )
    }

    fn commit_chunked_object_version_patch_impl(
        &self,
        mut assembler: ChunkedBlobAssembler,
        base_version: &str,
        offset: u64,
        trusted_content_hash: Option<&str>,
        base_identity: Option<&ContentVersionIdentity>,
    ) -> Result<ContentManifest> {
        if !assembler.is_complete() {
            return Err(ContentStoreError::IncompleteChunkedContent {
                object_id: assembler.object_id.clone(),
                version: assembler.version.clone(),
            });
        }
        assembler
            .file
            .sync_all()
            .map_err(|source| ContentStoreError::Io {
                path: assembler.path.clone(),
                source,
            })?;
        let (actual_hash, actual_size_bytes) = file_content_hash_and_size(&assembler.path)?;
        if actual_size_bytes != assembler.total_size_bytes {
            return Err(ContentStoreError::InvalidChunkedContent {
                message: format!(
                    "assembled chunked patch has wrong size for object {}, version {}: expected {}, actual {actual_size_bytes}",
                    assembler.object_id, assembler.version, assembler.total_size_bytes
                ),
            });
        }
        if actual_hash != assembler.expected_hash {
            return Err(ContentStoreError::ChunkHashMismatch {
                object_id: assembler.object_id.clone(),
                version: assembler.version.clone(),
                expected_hash: assembler.expected_hash.clone(),
                actual_hash,
            });
        }
        let manifest = self
            .put_object_version_patch_from_file_with_trusted_content_hash(
                &assembler.object_id,
                base_version,
                &assembler.version,
                offset,
                &assembler.path,
                trusted_content_hash,
                base_identity,
            )?
            .manifest;
        self.remove_tracked_staging_file_if_exists(&assembler.path)?;
        assembler.cleanup_on_drop = false;
        Ok(manifest)
    }
}

fn validate_chunked_object_shape(
    total_size_bytes: u64,
    total_chunks: u32,
    chunk_size_bytes: usize,
) -> Result<()> {
    if total_chunks == 0 {
        return Err(ContentStoreError::InvalidChunkedContent {
            message: "chunked object transfer has no chunks".to_string(),
        });
    }
    if chunk_size_bytes == 0 {
        return Err(ContentStoreError::InvalidChunkedContent {
            message: "chunked object transfer has zero-sized chunks".to_string(),
        });
    }
    let expected_chunks = total_size_bytes.div_ceil(chunk_size_bytes as u64);
    if expected_chunks == 0 || expected_chunks > u32::MAX as u64 {
        return Err(ContentStoreError::InvalidChunkedContent {
            message: format!(
                "chunked object size cannot be represented in {chunk_size_bytes}-byte chunks"
            ),
        });
    }
    if expected_chunks != u64::from(total_chunks) {
        return Err(ContentStoreError::InvalidChunkedContent {
            message: format!(
                "chunked object chunk count does not match size: expected {expected_chunks}, actual {total_chunks}"
            ),
        });
    }
    Ok(())
}
