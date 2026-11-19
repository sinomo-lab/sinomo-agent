//! Build one immutable manifest from packed, ordered dirty ranges.
use super::*;

impl LocalContentStore {
    pub(crate) fn range_patch_manifest(
        &self,
        object: &str,
        base_version: &str,
        target: &str,
        patch: &crate::DfsDistributedWritePatch,
        bytes: &[u8],
        blob: Option<ContentStoreQuarantinedBlob>,
        replay_only: bool,
    ) -> Result<Option<ContentManifest>> {
        let invalid = |message: &str| ContentStoreError::InvalidContentLayout {
            message: message.into(),
        };
        if !patch.valid_ranges() || patch.ranges.is_empty() {
            return Err(invalid("invalid packed file ranges"));
        }
        let identity = ContentVersionIdentity {
            content_hash: patch.base_content_hash.clone(),
            size_bytes: patch.base_size_bytes,
        };
        self.prepare_patch_units(object, base_version, target, Some(&identity))?;
        let _owner = self.content_mutation_guard()?;
        let existing = match self.read_current_manifest(object, target) {
            Ok(manifest) => Some(manifest),
            Err(error) if content_error_is_not_found(&error) => None,
            Err(error) => return Err(error),
        };
        if replay_only && existing.is_none() {
            return Ok(None);
        }
        let base = self.patch_base_manifest(object, base_version, Some(&identity))?;
        if base.content_hash != identity.content_hash || base.size_bytes != identity.size_bytes {
            return Err(invalid(
                "range patch base manifest differs from the frozen revision",
            ));
        }
        let chunks = self.logical_extents_for_manifest(&base)?;
        let mut chunks = chunks.iter().peekable();
        let mut packed_offset = 0;
        let mut cursor = 0;
        let mut extents = Vec::new();
        for &(offset, len) in &patch.ranges {
            // A base extent crossing a replacement remains available for its suffix.
            while let Some(chunk) = chunks.peek() {
                let end = chunk
                    .logical_offset
                    .checked_add(chunk.size_bytes)
                    .ok_or_else(|| invalid("base extent overflow"))?;
                let start = cursor.max(chunk.logical_offset);
                if start < offset.min(end) {
                    push_extent_coalescing(
                        &mut extents,
                        clip_extent(chunk, start, offset.min(end))?,
                    )?;
                }
                if end > offset {
                    break;
                }
                chunks.next();
            }
            push_extent_coalescing(
                &mut extents,
                ContentExtent {
                    logical_offset: offset,
                    content_hash: patch.patch_hash.clone(),
                    blob_offset: packed_offset,
                    size_bytes: len,
                },
            )?;
            packed_offset += len;
            cursor = offset + len;
        }
        for chunk in chunks {
            let end = chunk
                .logical_offset
                .checked_add(chunk.size_bytes)
                .ok_or_else(|| invalid("base extent overflow"))?;
            let start = cursor.max(chunk.logical_offset);
            if start < end {
                push_extent_coalescing(&mut extents, clip_extent(chunk, start, end)?)?;
            }
        }
        let size = base.size_bytes.max(cursor);
        validate_extents_cover_size(&extents, size, object, target)?;
        if let Some(existing) = existing {
            if existing.extents != extents || existing.size_bytes != size {
                return Err(invalid(
                    "replayed ranges differ from the immutable manifest",
                ));
            }
            return Ok(Some(existing));
        }
        if let Some(blob) = blob {
            if blob.store_root != self.root
                || blob.content_hash != patch.patch_hash
                || blob.size_bytes != packed_offset
            {
                return Err(invalid("range attachment differs from admitted bytes"));
            }
            self.ensure_content_reservation(object, target, &blob.content_hash)?;
            if !self.has_blob(&blob.content_hash)? {
                let path = self.blob_path(&blob.content_hash)?;
                self.ensure_quota_delta(ContentStoreQuotaDelta {
                    blob_bytes: projected_file_growth(&path, blob.size_bytes)?,
                    ..Default::default()
                })?;
                self.publish_prehashed_temporary_blob(&blob.synced_file, &path)?;
            }
        } else {
            if bytes.len() as u64 != packed_offset || content_hash(bytes) != patch.patch_hash {
                return Err(invalid("packed range content mismatch"));
            }
            self.write_bytes_as_blob(object, target, bytes)?;
        }
        for hash in extents
            .iter()
            .map(|e| &e.content_hash)
            .collect::<BTreeSet<_>>()
        {
            self.ensure_content_reservation(object, target, hash)?;
        }
        let mut manifest = ContentManifest {
            object_id: object.into(),
            version: target.into(),
            size_bytes: size,
            content_hash: base.content_hash.clone(),
            extents,
        };
        let tree = self.patch_hash_tree_for_extents(&base, &manifest, &patch.ranges, None)?;
        manifest.content_hash = match &tree {
            Some(tree) => tree.content_hash.clone(),
            None => self.hash_extents(&manifest.extents)?,
        };
        let manifest = self.publish_current_manifest(manifest)?;
        if let Some(tree) = tree {
            self.write_hash_tree_create_new_or_existing(tree)?;
        }
        Ok(Some(manifest))
    }
}
