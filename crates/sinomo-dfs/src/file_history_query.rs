//! Authoritative history discovery and immutable, bounded content reads.

use crate::*;
use serde::{Deserialize, Serialize};
use std::time::Duration;

const HISTORY_PAGE_LIMIT: usize = 128;
pub const DFS_FILE_HISTORY_RANGE_LIMIT: u32 = 64 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsFileHistoryDescriptor {
    pub version: DfsObjectVersionRecord,
    pub current_object: DfsObjectMetadata,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum DfsFileHistorySelection {
    /// Exact object lookup for the legacy version-read surface. This does not
    /// require retention to be enabled on the shard.
    CurrentObject {
        object_id: DfsObjectId,
    },
    VersionPin {
        identity: DfsFileVersionIdentity,
        pin_id: String,
    },
    RestoreJob {
        job_id: String,
    },
    List {
        path: Option<String>,
        object_id: Option<DfsObjectId>,
        after: Option<DfsObjectVersionScanCursor>,
        limit: usize,
    },
    Version {
        identity: DfsFileVersionIdentity,
    },
}

impl DfsFileHistorySelection {
    pub(crate) fn supports_codec(&self, codec: DfsMetadataNamespacePrepareCodec) -> bool {
        matches!(self, Self::CurrentObject { .. }) || codec.supports_file_history()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsFileHistoryShardQuery {
    pub file_system_id: DfsFileSystemId,
    pub shard_id: DfsMetadataShardId,
    pub epoch: u64,
    pub selection: DfsFileHistorySelection,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum DfsFileHistoryShardResult {
    CurrentObject {
        object: Option<Box<DfsObjectMetadata>>,
    },
    VersionPin {
        identity: DfsFileVersionIdentity,
        pin_id: String,
        present: bool,
    },
    RestoreJob {
        job: Option<Box<DfsNamespaceJobRecord>>,
        receipt: Option<Box<DfsNamespaceJobReceipt>>,
    },
    List {
        entries: Vec<DfsFileHistoryEntry>,
        after: Option<DfsObjectVersionScanCursor>,
    },
    Version {
        descriptor: Option<Box<DfsFileHistoryDescriptor>>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsFileHistoryShardResponse {
    pub file_system_id: DfsFileSystemId,
    pub shard_id: DfsMetadataShardId,
    pub epoch: u64,
    pub result: DfsFileHistoryShardResult,
}

fn error(message: impl Into<String>) -> DfsError {
    DfsError::Metadata {
        message: message.into(),
    }
}

pub(crate) fn query_backend_file_history<B: DfsMetadataBackend + ?Sized>(
    backend: &B,
    query: &DfsFileHistoryShardQuery,
) -> Result<DfsFileHistoryShardResult, MetadataBackendError> {
    match &query.selection {
        DfsFileHistorySelection::CurrentObject { object_id } => {
            let mut object = backend.get_object(object_id)?.filter(|object| {
                object.file_system_id == query.file_system_id
                    && object.tombstone_provenance.is_none()
            });
            if let Some(object) = object.as_mut() {
                include_receipt_locations(
                    backend,
                    object_id,
                    &object.version,
                    &object.content_hash,
                    &mut object.current_locations,
                )?;
            }
            Ok(DfsFileHistoryShardResult::CurrentObject {
                object: object.map(Box::new),
            })
        }
        DfsFileHistorySelection::VersionPin { identity, pin_id } => {
            if identity.file_system_id != query.file_system_id
                || pin_id.is_empty()
                || pin_id.len() > 256
            {
                return Err(MetadataBackendError::Serde(
                    "invalid history pin query".into(),
                ));
            }
            let anchored = backend
                .get_object(&identity.object_id)?
                .is_some_and(|object| {
                    object.file_system_id == identity.file_system_id
                        && object.tombstone_provenance.is_none()
                });
            let present = anchored
                && backend
                    .version_pins_for_object(&identity.object_id)?
                    .iter()
                    .any(|pin| {
                        pin.file_system_id == identity.file_system_id
                            && pin.version == identity.version
                            && pin.pin_id == *pin_id
                    });
            Ok(DfsFileHistoryShardResult::VersionPin {
                identity: identity.clone(),
                pin_id: pin_id.clone(),
                present,
            })
        }
        DfsFileHistorySelection::RestoreJob { job_id } => {
            if job_id.is_empty() || job_id.len() > 256 {
                return Err(MetadataBackendError::Serde(
                    "invalid restore job identity".into(),
                ));
            }
            let job = backend.get_namespace_job(job_id)?;
            let receipt = backend.get_namespace_job_receipt(job_id)?;
            if job.as_ref().is_some_and(|job| {
                job.file_system_id != query.file_system_id
                    || job.kind != DfsNamespaceJobKind::FileHistoryRestore
            }) || receipt.as_ref().is_some_and(|receipt| {
                receipt.file_system_id != query.file_system_id
                    || receipt.kind != DfsNamespaceJobKind::FileHistoryRestore
            }) {
                return Err(MetadataBackendError::Serde(
                    "restore job does not belong to the requested filesystem".into(),
                ));
            }
            Ok(DfsFileHistoryShardResult::RestoreJob {
                job: job.map(Box::new),
                receipt: receipt.map(Box::new),
            })
        }
        DfsFileHistorySelection::List {
            path,
            object_id,
            after,
            limit,
        } => {
            if *limit == 0 || *limit > HISTORY_PAGE_LIMIT {
                return Err(MetadataBackendError::Serde(
                    "history page limit must be in 1..=128".into(),
                ));
            }
            let (entries, after) = backend.retained_file_versions_page(
                &query.file_system_id,
                object_id.as_ref(),
                path.as_deref(),
                after.as_ref(),
                *limit,
            )?;
            Ok(DfsFileHistoryShardResult::List { entries, after })
        }
        DfsFileHistorySelection::Version { identity } => {
            if identity.file_system_id != query.file_system_id {
                return Err(MetadataBackendError::Serde(
                    "history lookup has a foreign filesystem identity".into(),
                ));
            }
            let Some(mut version) = backend.get_version(&identity.object_id, &identity.version)?
            else {
                return Ok(DfsFileHistoryShardResult::Version { descriptor: None });
            };
            let Some(retained) = &version.retained else {
                return Ok(DfsFileHistoryShardResult::Version { descriptor: None });
            };
            crate::file_history::validate_file_version_record(&version)?;
            let pinned = backend
                .version_pins_for_object(&identity.object_id)?
                .iter()
                .any(|pin| pin.version == identity.version);
            if version.file_system_id != identity.file_system_id
                || (!pinned && retained.policy_capture.is_none())
            {
                return Ok(DfsFileHistoryShardResult::Version { descriptor: None });
            }
            let Some(current_object) = backend.get_object(&identity.object_id)?.filter(|object| {
                object.file_system_id == identity.file_system_id
                    && object.tombstone_provenance.is_none()
            }) else {
                return Ok(DfsFileHistoryShardResult::Version { descriptor: None });
            };
            include_receipt_locations(
                backend,
                &identity.object_id,
                &identity.version,
                &version.content_hash,
                &mut version.locations,
            )?;
            Ok(DfsFileHistoryShardResult::Version {
                descriptor: Some(Box::new(DfsFileHistoryDescriptor {
                    version,
                    current_object,
                })),
            })
        }
    }
}

fn include_receipt_locations<B: DfsMetadataBackend + ?Sized>(
    backend: &B,
    object_id: &DfsObjectId,
    version: &DfsVersion,
    content_hash: &Option<String>,
    locations: &mut Vec<NodeId>,
) -> Result<(), MetadataBackendError> {
    let mut cursor = None;
    loop {
        let page = backend.receipts_for_version_page(object_id, version, cursor.as_ref(), 128)?;
        for receipt in page.receipts {
            if &receipt.content_hash == content_hash
                && !locations.contains(&receipt.replica_node_id)
            {
                locations.push(receipt.replica_node_id);
            }
        }
        cursor = page.next_cursor;
        if cursor.is_none() {
            return Ok(());
        }
    }
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub(crate) fn file_history_authorities(
        &mut self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Vec<DfsMetadataShardConfig>, DfsError> {
        if self.control_plane.is_none() {
            return Ok(vec![DfsMetadataShardConfig {
                file_system_id: file_system_id.clone(),
                shard_id: String::new(),
                primary_node_id: self.node_id.clone(),
                replica_node_ids: Vec::new(),
                epoch: 0,
                assignment: DfsMetadataShardAssignment::Root,
                namespace_prepare_codec: DfsMetadataNamespacePrepareCodec::PackedV5,
                default_policy: sinomo_core::DfsObjectPolicy::LocalOwned,
            }]);
        }
        self.refresh_control_plane_latest()?;
        let mut shards = self
            .metadata_shard_config_snapshot(file_system_id)?
            .unwrap_or_default();
        if shards.is_empty() {
            return Err(error(
                "history lookup has no configured metadata authorities",
            ));
        }
        shards.sort_by(|left, right| left.shard_id.cmp(&right.shard_id));
        Ok(shards)
    }

    pub(crate) fn serve_file_history_query(
        &mut self,
        query: &DfsFileHistoryShardQuery,
    ) -> Result<DfsFileHistoryShardResponse, DfsError> {
        if self.control_plane.is_some() {
            let context = self.namespace_execution.context().ok_or_else(|| {
                error("history query requires an authenticated authority context")
            })?;
            if context.file_system_id != query.file_system_id
                || !context.shards.iter().any(|shard| {
                    shard.shard_id == query.shard_id.0
                        && shard.epoch == query.epoch
                        && shard.primary_node_id == self.node_id
                        && query
                            .selection
                            .supports_codec(shard.namespace_prepare_codec)
                })
            {
                return Err(error(
                    "history query does not match the current shard authority",
                ));
            }
        }
        let backend = self
            .metadata_backend
            .as_mut()
            .ok_or_else(|| error("history query requires durable metadata"))?;
        backend.begin_consistent_read_snapshot()?;
        let result = backend.query_file_history_on_shard(query);
        backend.end_consistent_read_snapshot();
        let result = result?;
        match &result {
            DfsFileHistoryShardResult::CurrentObject {
                object: Some(object),
            } => {
                self.ensure_object_not_in_cross_shard_flight(object)?;
            }
            DfsFileHistoryShardResult::Version {
                descriptor: Some(descriptor),
            } => self.ensure_object_not_in_cross_shard_flight(&descriptor.current_object)?,
            DfsFileHistoryShardResult::List { entries, .. } => {
                for entry in entries {
                    self.ensure_path_not_in_cross_shard_flight(
                        &entry.identity.file_system_id,
                        &entry.current_path,
                    )?;
                }
            }
            _ => {}
        }
        Ok(DfsFileHistoryShardResponse {
            file_system_id: query.file_system_id.clone(),
            shard_id: query.shard_id.clone(),
            epoch: query.epoch,
            result,
        })
    }

    pub(crate) fn query_file_history_authority(
        &mut self,
        shard: &DfsMetadataShardConfig,
        selection: DfsFileHistorySelection,
    ) -> Result<DfsFileHistoryShardResult, DfsError> {
        let query = DfsFileHistoryShardQuery {
            file_system_id: shard.file_system_id.clone(),
            shard_id: DfsMetadataShardId::new(shard.shard_id.clone()),
            epoch: shard.epoch,
            selection,
        };
        let response = if self.control_plane.is_none() {
            self.serve_file_history_query(&query)?
        } else {
            let outcome = self
                .route_namespace_operation_to_metadata_primary_with_timeout_and_options(
                    query.file_system_id.clone(),
                    shard.primary_node_id.clone(),
                    DfsNamespaceOperation::FileHistoryQuery {
                        query: query.clone(),
                    },
                    Duration::from_secs(30),
                    false,
                    false,
                )?;
            match (outcome.result, outcome.posix_coordination_result) {
                (
                    DfsNamespaceResult::Applied,
                    Some(DfsPosixCoordinationResult::FileHistory(response)),
                ) => response,
                (DfsNamespaceResult::Rejected { reason, .. }, _) => return Err(error(reason)),
                _ => return Err(error("history authority omitted its typed query response")),
            }
        };
        if response.file_system_id != query.file_system_id
            || response.shard_id != query.shard_id
            || response.epoch != query.epoch
        {
            return Err(error(
                "history response does not match the requested authority",
            ));
        }
        Ok(response.result)
    }

    pub fn list_file_history(
        &mut self,
        mut request: DfsFileHistoryListRequest,
    ) -> Result<DfsFileHistoryListResponse, DfsError> {
        if request.limit == 0 || request.limit as usize > HISTORY_PAGE_LIMIT {
            return Err(error("history page limit must be in 1..=128"));
        }
        if let Some(path) = request.path.as_mut() {
            *path = crate::posix_path::normalize_posix_path(path)
                .map_err(|source| error(source.to_string()))?;
        }
        let shards = self.file_history_authorities(&request.file_system_id)?;
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
        let result = self.query_file_history_authority(
            &shards[index],
            DfsFileHistorySelection::List {
                path: request.path.clone(),
                object_id: request.object_id.clone(),
                after,
                limit: request.limit as usize,
            },
        )?;
        let DfsFileHistoryShardResult::List { entries, after } = result else {
            return Err(error("history authority returned the wrong query kind"));
        };
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
    ) -> Result<Option<DfsFileHistoryDescriptor>, DfsError> {
        for shard in self.file_history_authorities(&identity.file_system_id)? {
            if !shard.namespace_prepare_codec.supports_file_history() {
                continue;
            }
            let result = self.query_file_history_authority(
                &shard,
                DfsFileHistorySelection::Version {
                    identity: identity.clone(),
                },
            )?;
            let DfsFileHistoryShardResult::Version { descriptor } = result else {
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
                    || version.retained.is_none()
                {
                    return Err(error(
                        "history authority returned a mismatched immutable version",
                    ));
                }
                crate::file_history::validate_file_version_record(version)?;
                return Ok(Some(*descriptor));
            }
        }
        Ok(None)
    }

    fn resolve_file_version_current_object(
        &mut self,
        identity: &DfsFileVersionIdentity,
    ) -> Result<Option<DfsObjectMetadata>, DfsError> {
        for shard in self.file_history_authorities(&identity.file_system_id)? {
            let result = self.query_file_history_authority(
                &shard,
                DfsFileHistorySelection::CurrentObject {
                    object_id: identity.object_id.clone(),
                },
            )?;
            let DfsFileHistoryShardResult::CurrentObject { object } = result else {
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
                .read_immutable_object_metadata_range_or_fetch_remote_without_cache(
                    &object,
                    offset,
                    DFS_FILE_HISTORY_RANGE_LIMIT as usize,
                )
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
        descriptor: &DfsFileHistoryDescriptor,
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
            .read_immutable_object_metadata_range_or_fetch_remote_without_cache(
                &object,
                offset,
                max_bytes as usize,
            )
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
    descriptor: &DfsFileHistoryDescriptor,
) -> Result<DfsObjectMetadata, DfsError> {
    let version = &descriptor.version;
    let mut object = descriptor.current_object.clone();
    // Current ownership and policy authorize content access, including for
    // deleted files. Only the explicitly retained content identity is restored.
    object.kind = version
        .retained
        .as_ref()
        .ok_or_else(|| error("version is not explicitly retained"))?
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
