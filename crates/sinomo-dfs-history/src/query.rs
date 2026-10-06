//! Module-owned history cursors, discovery and exact historical reads.

use crate::*;
use serde::Serialize;

const HISTORY_PAGE_LIMIT: usize = 128;
pub const DFS_FILE_HISTORY_RANGE_LIMIT: u32 = 64 * 1024;

fn error(message: impl Into<String>) -> DfsError {
    DfsError::Metadata {
        message: message.into(),
    }
}

impl<T: DfsMessageTransport> DfsHistoryService<'_, T> {
    pub fn list_file_history(
        &mut self,
        mut request: DfsFileHistoryListRequest,
    ) -> Result<DfsFileHistoryListResponse, DfsError> {
        if request.limit == 0 || request.limit as usize > HISTORY_PAGE_LIMIT {
            return Err(error("history page limit must be in 1..=128"));
        }
        if let Some(path) = request.path.as_mut() {
            *path = sinomo_dfs::normalize_semantic_path(path)
                .map_err(|source| error(source.to_string()))?;
        }
        let shards = self
            .core
            .metadata_shard_authorities(&request.file_system_id)?;
        let topology = history_fingerprint(&shards)?;
        let query =
            history_fingerprint(&(&request.file_system_id, &request.path, &request.object_id))?;
        let mut index = 0;
        let mut after = None;
        if let Some(cursor) = &request.after {
            if cursor.topology != topology || cursor.query != query {
                return Err(error(
                    "history cursor expired or belongs to another query; restart listing",
                ));
            }
            index = shards
                .iter()
                .position(|shard| {
                    shard.shard_id == cursor.shard_id && shard.epoch == cursor.shard_epoch
                })
                .ok_or_else(|| error("history cursor authority changed; restart listing"))?;
            after = match (&cursor.object_id, &cursor.version) {
                (Some(object_id), Some(version)) => Some(DfsObjectVersionScanCursor {
                    object_id: object_id.clone(),
                    version: version.clone(),
                }),
                (None, None) => None,
                _ => return Err(error("history cursor has an incomplete version identity")),
            };
        }
        // A legacy shard cannot contain explicitly captured history. Its codec
        // must be upgraded before the first capture can be admitted.
        while index < shards.len()
            && !shards[index]
                .namespace_prepare_codec
                .supports_file_history()
        {
            index += 1;
            after = None;
        }
        if index == shards.len() {
            return Ok(DfsFileHistoryListResponse {
                entries: Vec::new(),
                next_cursor: None,
            });
        }
        let result = self.core.query_metadata_authority(
            &shards[index],
            DfsMetadataSelection::RetainedRevisions {
                namespace: Some(HISTORY_MODULE_ID.into()),
                path: request.path.clone(),
                object_id: request.object_id.clone(),
                after,
                limit: request.limit as usize,
            },
        )?;
        let DfsMetadataQueryResult::RetainedRevisions { entries, after } = result else {
            return Err(error("history authority returned the wrong query kind"));
        };
        let entries = entries
            .into_iter()
            .map(crate::metadata::entry_from_descriptor)
            .collect::<Result<Vec<_>, _>>()?;
        if entries.len() > request.limit as usize
            || entries.iter().any(|entry| {
                entry.identity.file_system_id != request.file_system_id
                    || request
                        .object_id
                        .as_ref()
                        .is_some_and(|object_id| object_id != &entry.identity.object_id)
            })
        {
            return Err(error("history authority returned an out-of-scope page"));
        }
        let next = if after.is_some() {
            Some((&shards[index], after))
        } else {
            shards.get(index + 1).map(|shard| (shard, None))
        };
        let next_cursor = next.map(|(shard, after)| DfsFileHistoryCursor {
            shard_id: shard.shard_id.clone(),
            shard_epoch: shard.epoch,
            object_id: after.as_ref().map(|cursor| cursor.object_id.clone()),
            version: after.map(|cursor| cursor.version),
            topology,
            query,
        });
        Ok(DfsFileHistoryListResponse {
            entries,
            next_cursor,
        })
    }

    pub(crate) fn resolve_file_history_version(
        &mut self,
        identity: &DfsFileVersionIdentity,
    ) -> Result<Option<DfsRetainedRevisionDescriptor>, DfsError> {
        for shard in self
            .core
            .metadata_shard_authorities(&identity.file_system_id)?
        {
            if !shard.namespace_prepare_codec.supports_file_history() {
                continue;
            }
            let result = self.core.query_metadata_authority(
                &shard,
                DfsMetadataSelection::Version {
                    namespace: Some(HISTORY_MODULE_ID.into()),
                    allow_current: false,
                    identity: identity.clone(),
                },
            )?;
            let DfsMetadataQueryResult::Version { descriptor } = result else {
                return Err(error("history authority returned the wrong query kind"));
            };
            if let Some(descriptor) = descriptor {
                let version = &descriptor.version;
                if version.file_system_id != identity.file_system_id
                    || version.object_id != identity.object_id
                    || version.version != identity.version
                    || descriptor.current_object.file_system_id != identity.file_system_id
                    || descriptor.current_object.object_id != identity.object_id
                    || descriptor.current_object.tombstone_provenance.is_some()
                    || version.retention_grant(HISTORY_MODULE_ID).is_none()
                {
                    return Err(error(
                        "history authority returned a mismatched immutable version",
                    ));
                }
                crate::validation::validate_file_version_record(version)?;
                return Ok(Some(*descriptor));
            }
        }
        Ok(None)
    }

    fn resolve_file_version_current_object(
        &mut self,
        identity: &DfsFileVersionIdentity,
    ) -> Result<Option<DfsObjectMetadata>, DfsError> {
        for shard in self
            .core
            .metadata_shard_authorities(&identity.file_system_id)?
        {
            let result = self.core.query_metadata_authority(
                &shard,
                DfsMetadataSelection::CurrentObject {
                    object_id: identity.object_id.clone(),
                },
            )?;
            let DfsMetadataQueryResult::CurrentObject { object } = result else {
                return Err(error("version authority returned the wrong query kind"));
            };
            if let Some(object) = object {
                if object.file_system_id != identity.file_system_id
                    || object.object_id != identity.object_id
                    || object.tombstone_provenance.is_some()
                {
                    return Err(error("version authority returned a different object"));
                }
                return Ok(Some(*object));
            }
        }
        Ok(None)
    }

    /// Collect an exact current or explicitly retained version. The supplied
    /// hash and length are expectations, never permission to open a blob.
    /// Content is fetched in bounded ranges under one authoritative descriptor.
    pub fn read_file_version_content(
        &mut self,
        identity: &DfsFileVersionIdentity,
        content_hash: &str,
        expected_size_bytes: Option<u64>,
    ) -> Result<Vec<u8>, DfsError> {
        let current = self.resolve_file_version_current_object(identity)?;
        let object = match current {
            Some(object)
                if object.version == identity.version
                    && matches!(
                        object.kind,
                        DfsObjectKind::File | DfsObjectKind::Artifact | DfsObjectKind::Symlink
                    ) =>
            {
                object
            }
            _ => {
                let descriptor = self
                    .resolve_file_history_version(identity)?
                    .ok_or_else(|| {
                        error("requested version is neither current nor explicitly retained")
                    })?;
                file_history_content_object(&descriptor)?
            }
        };
        if object.content_hash.as_deref() != Some(content_hash) {
            return Err(error(
                "requested content hash does not match the authorized version",
            ));
        }
        let size = object
            .content_size_bytes
            .ok_or_else(|| error("version has no content length"))?;
        if expected_size_bytes.is_some_and(|expected| expected != size) {
            return Err(error(
                "requested length does not match the authorized version",
            ));
        }
        let capacity =
            usize::try_from(size).map_err(|_| error("version is too large to collect"))?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(capacity)
            .map_err(|_| error("cannot allocate version content"))?;
        loop {
            let offset = bytes.len() as u64;
            let outcome = self
                .core
                .read_content_revision_range(&object, offset, DFS_FILE_HISTORY_RANGE_LIMIT as usize)
                .map_err(|source| error(source.to_string()))?;
            let NetworkedDfsRangeReadOutcome::Found { data, .. } = outcome else {
                return Err(error(
                    "authorized version has no currently readable replica",
                ));
            };
            if data.object_id != identity.object_id
                || data.version != identity.version
                || data.content_hash != object.content_hash
                || data.total_size_bytes != size
                || data.offset != offset
                || data.bytes.len() as u64
                    != size
                        .saturating_sub(offset)
                        .min(DFS_FILE_HISTORY_RANGE_LIMIT as u64)
            {
                return Err(error(
                    "version range does not match its authoritative descriptor",
                ));
            }
            bytes.extend_from_slice(&data.bytes);
            if bytes.len() == capacity {
                return Ok(bytes);
            }
        }
    }

    pub fn read_file_history(
        &mut self,
        request: DfsFileHistoryReadRequest,
    ) -> Result<DfsFileHistoryReadOutcome, DfsError> {
        if request.max_bytes == 0 || request.max_bytes > DFS_FILE_HISTORY_RANGE_LIMIT {
            return Err(error("history reads require a range of 1..=65536 bytes"));
        }
        let Some(descriptor) = self.resolve_file_history_version(&request.identity)? else {
            return Ok(DfsFileHistoryReadOutcome::NotRetained {
                identity: request.identity,
            });
        };
        self.read_file_history_descriptor(&descriptor, request.offset, request.max_bytes)
    }

    pub(crate) fn read_file_history_descriptor(
        &mut self,
        descriptor: &DfsRetainedRevisionDescriptor,
        offset: u64,
        max_bytes: u32,
    ) -> Result<DfsFileHistoryReadOutcome, DfsError> {
        let version = &descriptor.version;
        let identity = DfsFileVersionIdentity {
            file_system_id: version.file_system_id.clone(),
            object_id: version.object_id.clone(),
            version: version.version.clone(),
        };
        let object = file_history_content_object(descriptor)?;
        let size = version
            .size_bytes
            .ok_or_else(|| error("retained version has no length"))?;
        let outcome = self
            .core
            .read_content_revision_range(&object, offset, max_bytes as usize)
            .map_err(|source| error(source.to_string()))?;
        match outcome {
            NetworkedDfsRangeReadOutcome::Found { data, .. } => {
                let expected = size.saturating_sub(offset).min(max_bytes as u64);
                if data.object_id != identity.object_id
                    || data.version != identity.version
                    || data.content_hash != version.content_hash
                    || data.total_size_bytes != size
                    || data.offset != offset.min(size)
                    || data.bytes.len() as u64 != expected
                {
                    return Err(error(
                        "historical range response does not match its immutable descriptor",
                    ));
                }
                Ok(DfsFileHistoryReadOutcome::Found {
                    data: DfsFileHistoryReadResponse {
                        identity,
                        offset: data.offset,
                        size_bytes: size,
                        bytes: data.bytes,
                    },
                })
            }
            _ => Ok(DfsFileHistoryReadOutcome::ContentUnavailable {
                identity,
                detail: "retained content has no currently readable replica".into(),
            }),
        }
    }
}

fn file_history_content_object(
    descriptor: &DfsRetainedRevisionDescriptor,
) -> Result<DfsObjectMetadata, DfsError> {
    let version = &descriptor.version;
    let mut object = descriptor.current_object.clone();
    // Current ownership and policy authorize content access, including for
    // deleted files. Only the explicitly retained content identity is restored.
    object.kind = version
        .retention_grant(HISTORY_MODULE_ID)
        .ok_or_else(|| error("revision is not retained by History"))?
        .object
        .kind
        .clone();
    object.version = version.version.clone();
    object.content_hash = version.content_hash.clone();
    object.content_size_bytes = version.size_bytes;
    object.current_locations = version.locations.clone();
    Ok(object)
}

fn history_fingerprint(value: &impl Serialize) -> Result<String, DfsError> {
    serde_json::to_vec(value)
        .map(|bytes| blake3::hash(&bytes).to_hex().to_string())
        .map_err(|source| error(source.to_string()))
}
