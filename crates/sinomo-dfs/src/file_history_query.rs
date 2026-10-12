//! Authoritative history discovery and immutable, bounded content reads.

use crate::*;
use serde::{Deserialize, Serialize};
use std::time::Duration;

const HISTORY_PAGE_LIMIT: usize = 128;
pub const DFS_FILE_HISTORY_RANGE_LIMIT: u32 = 64 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum DfsMetadataSelection {
    /// One current owner's durable direct-child page and exact local cursors.
    DirectoryPage {
        path: String,
        after: Option<DfsDirectoryPageCursor>,
        limit: usize,
    },
    /// Exact object lookup for the current version-read surface. This does not
    /// require retention to be enabled on the shard.
    CurrentObject {
        object_id: DfsObjectId,
    },
    /// Canonical identity and live content-reference evidence in one snapshot.
    ObjectLifecycle {
        object_id: DfsObjectId,
    },
    VersionPin {
        identity: DfsFileVersionIdentity,
        pin_id: String,
    },
    NamespaceJob {
        job_id: String,
    },
    RetainedRevisions {
        namespace: Option<String>,
        path: Option<String>,
        object_id: Option<DfsObjectId>,
        after: Option<DfsObjectVersionScanCursor>,
        limit: usize,
    },
    Version {
        namespace: Option<String>,
        allow_current: bool,
        identity: DfsRevisionReference,
    },
    /// Metadata-only inspection of one anchored revision. Returning this row
    /// grants no permission to read its immutable content.
    RevisionMetadata {
        identity: DfsRevisionReference,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataShardQuery {
    pub file_system_id: DfsFileSystemId,
    pub shard_id: DfsMetadataShardId,
    pub epoch: u64,
    pub selection: DfsMetadataSelection,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum DfsMetadataQueryResult {
    DirectoryPage {
        entries: Vec<(DfsObjectMetadata, DfsDirectoryPageCursor)>,
        after: Option<DfsDirectoryPageCursor>,
    },
    CurrentObject {
        object: Option<Box<DfsObjectMetadata>>,
    },
    ObjectLifecycle {
        object: Option<Box<DfsObjectMetadata>>,
        has_content_references: bool,
        references_complete: bool,
    },
    VersionPin {
        identity: DfsFileVersionIdentity,
        pin_id: String,
        present: bool,
    },
    NamespaceJob {
        job: Option<Box<DfsNamespaceJobRecord>>,
        receipt: Option<Box<DfsNamespaceJobReceipt>>,
    },
    RetainedRevisions {
        entries: Vec<DfsRetainedRevisionDescriptor>,
        after: Option<DfsObjectVersionScanCursor>,
    },
    Version {
        descriptor: Option<Box<DfsRetainedRevisionDescriptor>>,
    },
    RevisionMetadata {
        descriptor: Option<Box<DfsRevisionMetadataDescriptor>>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataShardResponse {
    pub file_system_id: DfsFileSystemId,
    pub shard_id: DfsMetadataShardId,
    pub epoch: u64,
    pub result: DfsMetadataQueryResult,
}

fn error(message: impl Into<String>) -> DfsError {
    DfsError::Metadata {
        message: message.into(),
    }
}

pub(crate) fn query_backend_metadata<B: DfsMetadataBackend + ?Sized>(
    backend: &B,
    query: &DfsMetadataShardQuery,
) -> Result<DfsMetadataQueryResult, MetadataBackendError> {
    match &query.selection {
        DfsMetadataSelection::DirectoryPage { path, after, limit } => {
            let path = crate::posix_path::normalize_posix_path(path)
                .map_err(|error| MetadataBackendError::Serde(error.to_string()))?;
            if after
                .as_ref()
                .is_some_and(|cursor| cursor.shard_id.is_some())
            {
                return Err(MetadataBackendError::Serde(
                    "owner directory query requires its exact local cursor".into(),
                ));
            }
            let (entries, after) = backend.child_objects_for_directory_page_entries(
                &query.file_system_id,
                &path,
                after.as_ref(),
                *limit,
            )?;
            Ok(DfsMetadataQueryResult::DirectoryPage { entries, after })
        }
        DfsMetadataSelection::ObjectLifecycle { object_id } => {
            let object = backend.get_object(object_id)?.filter(|object| {
                object.file_system_id == query.file_system_id
                    && object.tombstone_provenance.is_none()
            });
            let pins = backend.version_pins_for_object(object_id)?;
            let mut after = None;
            let mut has_content_references = pins.iter().any(|pin| {
                pin.file_system_id == query.file_system_id && pin.object_id == *object_id
            });
            let mut references_complete = false;
            // One consistent authority snapshot, bounded object-local work.
            // Exhausting the budget is incomplete evidence, never a GC permit.
            if !has_content_references {
                for _ in 0..16 {
                    let (retained, next) = backend.retained_revision_descriptors_page(
                        None,
                        &query.file_system_id,
                        Some(object_id),
                        None,
                        after.as_ref(),
                        128,
                    )?;
                    if !retained.is_empty() {
                        has_content_references = true;
                        references_complete = true;
                        break;
                    }
                    after = next;
                    if after.is_none() {
                        references_complete = true;
                        break;
                    }
                }
            } else {
                references_complete = true;
            }
            if !has_content_references && references_complete {
                let (prepared_reference, prepared_complete) = backend
                    .prepared_content_reference_protection(&query.file_system_id, object_id, 128)?;
                has_content_references = prepared_reference;
                references_complete = prepared_complete;
            }
            Ok(DfsMetadataQueryResult::ObjectLifecycle {
                object: object.map(Box::new),
                has_content_references,
                references_complete,
            })
        }
        DfsMetadataSelection::CurrentObject { object_id } => {
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
            Ok(DfsMetadataQueryResult::CurrentObject {
                object: object.map(Box::new),
            })
        }
        DfsMetadataSelection::VersionPin { identity, pin_id } => {
            if identity.file_system_id != query.file_system_id
                || pin_id.is_empty()
                // Queries use the qualified storage key, which also includes
                // the namespace. Match the persisted reference-key bound.
                || pin_id.len() > 1024
                || pin_id.contains('\0')
            {
                return Err(MetadataBackendError::Serde(
                    "invalid content reference query".into(),
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
                            && content_reference_record_id(pin) == *pin_id
                    });
            Ok(DfsMetadataQueryResult::VersionPin {
                identity: identity.clone(),
                pin_id: pin_id.clone(),
                present,
            })
        }
        DfsMetadataSelection::NamespaceJob { job_id } => {
            if job_id.is_empty() || job_id.len() > 256 {
                return Err(MetadataBackendError::Serde(
                    "invalid restore job identity".into(),
                ));
            }
            let job = backend.get_namespace_job(job_id)?;
            let receipt = backend.get_namespace_job_receipt(job_id)?;
            if job
                .as_ref()
                .is_some_and(|job| job.file_system_id != query.file_system_id)
                || receipt
                    .as_ref()
                    .is_some_and(|receipt| receipt.file_system_id != query.file_system_id)
            {
                return Err(MetadataBackendError::Serde(
                    "restore job does not belong to the requested filesystem".into(),
                ));
            }
            Ok(DfsMetadataQueryResult::NamespaceJob {
                job: job.map(Box::new),
                receipt: receipt.map(Box::new),
            })
        }
        DfsMetadataSelection::RetainedRevisions {
            namespace,
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
            let (entries, after) = backend.retained_revision_descriptors_page(
                namespace.as_deref(),
                &query.file_system_id,
                object_id.as_ref(),
                path.as_deref(),
                after.as_ref(),
                *limit,
            )?;
            Ok(DfsMetadataQueryResult::RetainedRevisions { entries, after })
        }
        DfsMetadataSelection::Version {
            namespace,
            allow_current,
            identity,
        } => {
            if identity.file_system_id != query.file_system_id {
                return Err(MetadataBackendError::Serde(
                    "foreign revision filesystem".into(),
                ));
            }
            let Some(mut version) = backend.get_version(&identity.object_id, &identity.version)?
            else {
                return Ok(DfsMetadataQueryResult::Version { descriptor: None });
            };
            let Some(current_object) = backend.get_object(&identity.object_id)?.filter(|object| {
                object.file_system_id == identity.file_system_id
                    && object.tombstone_provenance.is_none()
            }) else {
                return Ok(DfsMetadataQueryResult::Version { descriptor: None });
            };
            crate::file_history::validate_file_version_record(&version)?;
            let references = backend
                .version_pins_for_object(&identity.object_id)?
                .into_iter()
                .filter(|pin| pin.version == identity.version)
                .collect::<Vec<_>>();
            let pinned = references.iter().any(|pin| {
                namespace
                    .as_deref()
                    .is_none_or(|owner| pin.namespace == owner)
            });
            let current = *allow_current
                && current_object.version == identity.version
                && current_object.kind != DfsObjectKind::Tombstone;
            if version.file_system_id != identity.file_system_id
                || (!current
                    && !crate::file_history::retained_namespace_is_live(
                        &version,
                        namespace.as_deref(),
                        pinned,
                        crate::dfs_now_unix_nanos(),
                        || {
                            crate::file_history::retention_orders_from_backend(
                                backend,
                                &identity.object_id,
                            )
                        },
                    )?)
            {
                return Ok(DfsMetadataQueryResult::Version { descriptor: None });
            }
            include_receipt_locations(
                backend,
                &identity.object_id,
                &identity.version,
                &version.content_hash,
                &mut version.locations,
            )?;
            Ok(DfsMetadataQueryResult::Version {
                descriptor: Some(Box::new(DfsRetainedRevisionDescriptor {
                    version,
                    current_object,
                    references,
                })),
            })
        }
        DfsMetadataSelection::RevisionMetadata { identity } => {
            if identity.file_system_id != query.file_system_id {
                return Err(MetadataBackendError::Serde(
                    "foreign revision metadata filesystem".into(),
                ));
            }
            let Some(version) = backend.get_version(&identity.object_id, &identity.version)? else {
                return Ok(DfsMetadataQueryResult::RevisionMetadata { descriptor: None });
            };
            let Some(current_object) = backend.get_object(&identity.object_id)?.filter(|object| {
                object.file_system_id == identity.file_system_id
                    && object.object_id == identity.object_id
                    && object.tombstone_provenance.is_none()
            }) else {
                return Ok(DfsMetadataQueryResult::RevisionMetadata { descriptor: None });
            };
            if version.file_system_id != identity.file_system_id
                || version.object_id != identity.object_id
                || version.version != identity.version
            {
                return Err(MetadataBackendError::Serde(
                    "foreign revision metadata identity".into(),
                ));
            }
            crate::file_history::validate_file_version_record(&version)?;
            Ok(DfsMetadataQueryResult::RevisionMetadata {
                descriptor: Some(Box::new(DfsRevisionMetadataDescriptor {
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
    pub fn metadata_shard_authorities(
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

    pub(crate) fn serve_metadata_query(
        &mut self,
        query: &DfsMetadataShardQuery,
    ) -> Result<DfsMetadataShardResponse, DfsError> {
        if self.control_plane.is_some() {
            let context = self.namespace_execution.context().ok_or_else(|| {
                error("history query requires an authenticated authority context")
            })?;
            if context.file_system_id != query.file_system_id
                || !context.shards.iter().any(|shard| {
                    shard.shard_id == query.shard_id.0
                        && shard.epoch == query.epoch
                        && shard.primary_node_id == self.node_id
                })
            {
                return Err(error(
                    "history query does not match the current shard authority",
                ));
            }
        }
        if let DfsMetadataSelection::DirectoryPage { path, .. } = &query.selection {
            self.ensure_path_not_in_cross_shard_flight(&query.file_system_id, path)?;
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
            DfsMetadataQueryResult::DirectoryPage { entries, .. } => {
                for (object, _) in entries {
                    self.ensure_object_not_in_cross_shard_flight(object)?;
                }
            }
            DfsMetadataQueryResult::CurrentObject {
                object: Some(object),
            }
            | DfsMetadataQueryResult::ObjectLifecycle {
                object: Some(object),
                ..
            } => {
                self.ensure_object_not_in_cross_shard_flight(object)?;
            }
            DfsMetadataQueryResult::Version {
                descriptor: Some(descriptor),
            } => self.ensure_object_not_in_cross_shard_flight(&descriptor.current_object)?,
            DfsMetadataQueryResult::RevisionMetadata {
                descriptor: Some(descriptor),
            } => self.ensure_object_not_in_cross_shard_flight(&descriptor.current_object)?,
            DfsMetadataQueryResult::RetainedRevisions { entries, .. } => {
                for entry in entries {
                    self.ensure_path_not_in_cross_shard_flight(
                        &entry.current_object.file_system_id,
                        &entry.current_object.path,
                    )?;
                }
            }
            _ => {}
        }
        Ok(DfsMetadataShardResponse {
            file_system_id: query.file_system_id.clone(),
            shard_id: query.shard_id.clone(),
            epoch: query.epoch,
            result,
        })
    }

    pub(crate) fn query_metadata_authority_impl(
        &mut self,
        shard: &DfsMetadataShardConfig,
        selection: DfsMetadataSelection,
    ) -> Result<DfsMetadataQueryResult, DfsError> {
        let query = DfsMetadataShardQuery {
            file_system_id: shard.file_system_id.clone(),
            shard_id: DfsMetadataShardId::new(shard.shard_id.clone()),
            epoch: shard.epoch,
            selection,
        };
        let response = if self.control_plane.is_none() {
            self.serve_metadata_query(&query)?
        } else {
            let outcome = self.route_metadata_query(shard, &query)?;
            match (outcome.result, outcome.posix_coordination_result) {
                (
                    DfsNamespaceResult::Applied,
                    Some(DfsPosixCoordinationResult::MetadataQuery(response)),
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

    /// The read uses the existing namespace command, authenticated sender and
    /// exact response fence. Its wait must not enter the mutation dispatcher.
    fn route_metadata_query(
        &mut self,
        expected: &DfsMetadataShardConfig,
        query: &DfsMetadataShardQuery,
    ) -> Result<PendingRoutedNamespaceResult, DfsError> {
        self.check_cancelled("metadata query")?;
        let snapshot = self
            .metadata_shard_config_snapshot(&query.file_system_id)?
            .unwrap_or_default();
        let current = snapshot
            .iter()
            .find(|shard| {
                shard.file_system_id == query.file_system_id
                    && shard.shard_id == query.shard_id.0
                    && shard.epoch == query.epoch
            })
            .ok_or_else(|| error("history query names a stale or foreign shard authority"))?;
        if current != expected {
            return Err(error("history query authority changed before dispatch"));
        }
        let request_id = self.next_request_id();
        let command = DfsNamespaceCommand {
            request_id: request_id.clone(),
            file_system_id: query.file_system_id.clone(),
            requesting_node_id: self.node_id.clone(),
            include_metadata_journal_batches: false,
            metadata_journal_after_sequences: Vec::new(),
            authority_shards: vec![current.clone()],
            operation: DfsNamespaceOperation::MetadataQuery {
                query: query.clone(),
            },
        };
        // An actual local primary reads its resident generation. Mounted clients
        // continue to delegate to that primary through their ordinary transport.
        if current.primary_node_id == self.node_id
            && !self.transport.delegates_local_namespace_authority()
        {
            let _planning = self.namespace_execution.begin_independent_planning();
            let previous =
                self.replace_namespace_operation_context(query.file_system_id.clone(), snapshot);
            let result = self.serve_metadata_query(query);
            self.restore_namespace_operation_context(previous);
            let response = result?;
            self.validate_routed_namespace_response_authority(
                &expected.primary_node_id,
                Some(expected),
                &command.authority_shards,
            )?;
            return Ok(PendingRoutedNamespaceResult {
                result: DfsNamespaceResult::Applied,
                posix_coordination_result: Some(DfsPosixCoordinationResult::MetadataQuery(
                    response,
                )),
                synced_path_object: None,
                synced_path_objects: Vec::new(),
                metadata_journal_has_more: false,
            });
        }
        let requested = std::time::Instant::now() + Duration::from_secs(30);
        let previous_deadline = self.transport.set_operation_deadline(Some(requested));
        let deadline = previous_deadline.map_or(requested, |old| old.min(requested));
        self.transport.set_operation_deadline(Some(deadline));
        self.routed_namespace_settle_depth = self.routed_namespace_settle_depth.saturating_add(1);
        self.pending_routed_namespace_ops.insert(
            request_id.clone(),
            crate::service_state::PendingRoutedNamespaceOp {
                primary_node_id: expected.primary_node_id.clone(),
                dispatch_authority_shard: Some(expected.clone()),
                authority_shards: command.authority_shards.clone(),
                replay_journal_batches: false,
                apply_synced_path_object: false,
                result: None,
            },
        );
        let result = (|| {
            if !self.send_dfs_message_if_routable(
                &expected.primary_node_id,
                DfsNetworkMessage::NamespaceRequest(command.clone()),
            )? {
                return Err(error("metadata query primary is not routable"));
            }
            let response = wait_for_metadata_query_response(
                &mut self.transport,
                self.cancellation.as_ref(),
                &self.node_id,
                &expected.primary_node_id,
                &request_id,
                deadline,
            )?;
            match &response.result {
                DfsNamespaceResult::Applied => self.validate_routed_namespace_response_authority(
                    &expected.primary_node_id,
                    Some(expected),
                    &command.authority_shards,
                )?,
                DfsNamespaceResult::Rejected {
                    current_primary_node_id: Some(redirect),
                    ..
                } => self.validate_routed_namespace_authority_redirect(
                    &expected.primary_node_id,
                    Some(expected),
                    &command.authority_shards,
                    redirect,
                )?,
                DfsNamespaceResult::Rejected { .. } => {}
            }
            Ok(PendingRoutedNamespaceResult {
                result: response.result,
                posix_coordination_result: response.posix_coordination_result,
                synced_path_object: None,
                synced_path_objects: Vec::new(),
                metadata_journal_has_more: false,
            })
        })();
        self.pending_routed_namespace_ops.remove(&request_id);
        self.routed_namespace_settle_depth = self.routed_namespace_settle_depth.saturating_sub(1);
        self.transport.set_operation_deadline(previous_deadline);
        result
    }

    pub(crate) fn resolve_file_version_current_object(
        &mut self,
        identity: &DfsRevisionReference,
    ) -> Result<Option<DfsObjectMetadata>, DfsError> {
        self.current_object_from_authority(&identity.file_system_id, &identity.object_id)
    }

    pub(crate) fn current_object_from_authority(
        &mut self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
    ) -> Result<Option<DfsObjectMetadata>, DfsError> {
        for shard in self.metadata_shard_authorities(file_system_id)? {
            let result = self.query_metadata_authority_impl(
                &shard,
                DfsMetadataSelection::CurrentObject {
                    object_id: object_id.clone(),
                },
            )?;
            let DfsMetadataQueryResult::CurrentObject { object } = result else {
                return Err(error("version authority returned the wrong query kind"));
            };
            if let Some(object) = object {
                if object.file_system_id != *file_system_id
                    || object.object_id != *object_id
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
        identity: &DfsRevisionReference,
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
                let descriptor = self.resolve_retained_revision(identity)?.ok_or_else(|| {
                    error("requested version is neither current nor explicitly retained")
                })?;
                retained_revision_content_object(&descriptor)?
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
}

pub(crate) fn retained_revision_content_object(
    descriptor: &DfsRetainedRevisionDescriptor,
) -> Result<DfsObjectMetadata, DfsError> {
    let version = &descriptor.version;
    let mut object = descriptor.current_object.clone();
    if object.version != version.version || object.kind == DfsObjectKind::Tombstone {
        object.kind = version
            .retention
            .values()
            .find_map(|state| state.grant.as_ref())
            .ok_or_else(|| error("revision has no retention grant"))?
            .object
            .kind
            .clone();
    }
    object.version = version.version.clone();
    object.content_hash = version.content_hash.clone();
    object.content_size_bytes = version.size_bytes;
    object.current_locations = version.locations.clone();
    Ok(object)
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    /// Inspect one exact revision's declarative metadata at its authority.
    /// This projection is not a content-read or retention authorization.
    /// sinomo:dfs-operation
    pub fn revision_metadata(
        &mut self,
        identity: &DfsRevisionReference,
    ) -> Result<Option<DfsRevisionMetadataDescriptor>, DfsError> {
        for shard in self.metadata_shard_authorities(&identity.file_system_id)? {
            match self.query_metadata_authority(
                &shard,
                DfsMetadataSelection::RevisionMetadata {
                    identity: identity.clone(),
                },
            )? {
                DfsMetadataQueryResult::RevisionMetadata {
                    descriptor: Some(descriptor),
                } => {
                    if descriptor.version.file_system_id != identity.file_system_id
                        || descriptor.version.object_id != identity.object_id
                        || descriptor.version.version != identity.version
                        || descriptor.current_object.file_system_id != identity.file_system_id
                        || descriptor.current_object.object_id != identity.object_id
                        || descriptor.current_object.tombstone_provenance.is_some()
                    {
                        return Err(error(
                            "revision metadata authority returned foreign evidence",
                        ));
                    }
                    return Ok(Some(*descriptor));
                }
                DfsMetadataQueryResult::RevisionMetadata { descriptor: None } => {}
                _ => {
                    return Err(error(
                        "revision metadata authority returned wrong query kind",
                    ));
                }
            }
        }
        Ok(None)
    }

    pub fn resolve_retained_revision(
        &mut self,
        identity: &DfsRevisionReference,
    ) -> Result<Option<DfsRetainedRevisionDescriptor>, DfsError> {
        self.resolve_revision_descriptor(identity, false)
    }

    pub fn resolve_readable_revision(
        &mut self,
        identity: &DfsRevisionReference,
    ) -> Result<Option<DfsRetainedRevisionDescriptor>, DfsError> {
        self.resolve_revision_descriptor(identity, true)
    }

    fn resolve_revision_descriptor(
        &mut self,
        identity: &DfsRevisionReference,
        allow_current: bool,
    ) -> Result<Option<DfsRetainedRevisionDescriptor>, DfsError> {
        for shard in self.metadata_shard_authorities(&identity.file_system_id)? {
            match self.query_metadata_authority(
                &shard,
                DfsMetadataSelection::Version {
                    namespace: None,
                    allow_current,
                    identity: identity.clone(),
                },
            )? {
                DfsMetadataQueryResult::Version {
                    descriptor: Some(descriptor),
                } => {
                    if descriptor.version.file_system_id != identity.file_system_id
                        || descriptor.version.object_id != identity.object_id
                        || descriptor.version.version != identity.version
                        || descriptor.current_object.object_id != identity.object_id
                        || descriptor.current_object.file_system_id != identity.file_system_id
                        || descriptor.current_object.tombstone_provenance.is_some()
                    {
                        return Err(error("revision authority returned foreign evidence"));
                    }
                    return Ok(Some(*descriptor));
                }
                DfsMetadataQueryResult::Version { descriptor: None } => {}
                _ => return Err(error("revision authority returned wrong query kind")),
            }
        }
        Ok(None)
    }
}

/// Transport-only wait: no backend, service dispatcher, prepare or repair graph.
fn wait_for_metadata_query_response(
    transport: &mut dyn DfsMessageTransport,
    cancellation: Option<&DfsCancellationFlag>,
    local: &sinomo_core::node::NodeId,
    primary: &sinomo_core::node::NodeId,
    request_id: &str,
    deadline: std::time::Instant,
) -> Result<DfsNamespaceResponse, DfsError> {
    loop {
        if cancellation.is_some_and(DfsCancellationFlag::is_cancelled) {
            return Err(DfsError::Cancelled {
                operation: "metadata query",
            });
        }
        if let Some(message) = transport
            .take_namespace_response(request_id)
            .map_err(DfsError::from_network)?
        {
            if message.target_node_id != *local || message.source_node_id != *primary {
                continue; // Same existing rule: a foreign/late frame grants no outcome.
            }
            let DfsNetworkMessage::NamespaceResponse(response) = message.message else {
                return Err(error("metadata query inbox returned a non-response"));
            };
            if response.request_id != request_id {
                return Err(error("metadata query response identity changed"));
            }
            return Ok(response);
        }
        let remaining = deadline
            .checked_duration_since(std::time::Instant::now())
            .filter(|remaining| !remaining.is_zero())
            .ok_or_else(|| error("metadata query timed out waiting for its authority"))?;
        // Unrelated queued traffic retains custody and cannot cause a busy loop.
        std::thread::sleep(remaining.min(Duration::from_millis(10)));
    }
}

#[cfg(test)]
mod metadata_query_graph_tests {
    use super::*;

    #[test]
    fn current_native_metadata_query_is_read_only_and_rejects_stale_scope() {
        let node =
            crate::test_runtime::DfsTestRuntime::ordered_node_ids("metadata-query-read-only", 1)
                .remove(0);
        let mut runtime =
            crate::test_runtime::DfsTestRuntime::new("metadata-query-read-only", node.clone())
                .unwrap();
        runtime.start_nodes().unwrap();
        let mut service = runtime.client(&node).unwrap();
        service
            .posix_write_file(
                "/services/query-proof",
                b"current bytes",
                crate::DfsWriteOptions::replicated_eventual(vec![node]),
            )
            .unwrap();
        let object = service
            .file_history_capture_object("/services/query-proof")
            .unwrap();
        let authorities = service
            .metadata_shard_authorities(&object.file_system_id)
            .unwrap();
        let before = service
            .metadata_backend()
            .unwrap()
            .metadata_shard_statuses()
            .unwrap();
        let mut found = false;
        for shard in &authorities {
            let result = service
                .query_metadata_authority_impl(
                    shard,
                    DfsMetadataSelection::CurrentObject {
                        object_id: object.object_id.clone(),
                    },
                )
                .unwrap();
            if let DfsMetadataQueryResult::CurrentObject { object: Some(row) } = result {
                assert_eq!(*row, object);
                found = true;
            }
        }
        assert!(found);
        let after = service
            .metadata_backend()
            .unwrap()
            .metadata_shard_statuses()
            .unwrap();
        assert_eq!(
            before, after,
            "query must not prepare, commit or repair metadata"
        );
        let mut stale = authorities[0].clone();
        stale.epoch += 1;
        assert!(
            service
                .query_metadata_authority_impl(
                    &stale,
                    DfsMetadataSelection::CurrentObject {
                        object_id: object.object_id.clone()
                    }
                )
                .is_err()
        );
        assert!(service.pending_routed_namespace_ops.is_empty());
        assert_eq!(service.routed_namespace_settle_depth, 0);
        drop(service);
        runtime.close().unwrap();
    }
}
