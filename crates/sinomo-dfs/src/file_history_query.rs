//! Authoritative history discovery and immutable, bounded content reads.

use crate::*;
use serde::{Deserialize, Serialize};
use std::time::Duration;

pub(crate) const HISTORY_PAGE_LIMIT: usize = 128;
pub const DFS_FILE_HISTORY_RANGE_LIMIT: u32 = 64 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum DfsMetadataSelection {
    StatAtPathV1 {
        request: Box<DfsStatPathRequest>,
    },
    /// Resolve an inode identity without opening unrelated physical scopes.
    StatObjectV1 {
        object_id: DfsObjectId,
    },
    /// Explicit capability/version: older peers reject this selection. It may
    /// never fall back to caller-local derived stat or journal ingestion.
    StatProjectionV1 {
        request: Box<DfsStatProjectionRequest>,
    },
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
    /// Read the physical source of an existing rename job. Only the current
    /// authenticated job coordinator may use its own reservation for this read.
    /// Unknown variants are rejected by older peers; no public-read fallback.
    CurrentObjectForNamespaceJob {
        job: Box<DfsNamespaceJobRecord>,
        source_path: String,
        object_id: DfsObjectId,
    },
    DirectoryPageForNamespaceJob {
        job: Box<DfsNamespaceJobRecord>,
        path: String,
        after: Option<DfsDirectoryPageCursor>,
        limit: usize,
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
    /// One shard's current objects at or under `path`, in path order, served
    /// by that shard's primary. Placement reconciliation pages every shard
    /// this way. Older peers reject this selection.
    SubtreeObjectPageV1 {
        path: String,
        after: Option<DfsObjectPageCursor>,
        limit: usize,
    },
}

/// Largest subtree object page one authority returns per query.
pub(crate) const DFS_SUBTREE_OBJECT_PAGE_LIMIT: usize = 512;

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
    StatAtPathV1 {
        view: Box<DfsStatPathView>,
    },
    /// `next_cursor` is local to the queried shard.
    SubtreeObjectPageV1 {
        objects: Vec<DfsObjectMetadata>,
        next_cursor: Option<DfsObjectPageCursor>,
    },
    StatObjectV1 {
        object: Option<Box<DfsObjectMetadata>>,
        prefix: DfsMetadataPrefixProof,
    },
    StatProjectionV1 {
        page: Box<DfsStatProjectionPage>,
    },
    /// The requested cut is no longer current. This is a typed refusal, not
    /// an empty page or permission to publish a partially collected stat.
    StatProjectionChangedV1 {
        request: Box<DfsStatProjectionRequest>,
        current_prefix: DfsMetadataPrefixProof,
    },
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
        DfsMetadataSelection::StatAtPathV1 { .. } => Err(MetadataBackendError::Serde(
            "stat path requires its authenticated owning service and complete catalog".into(),
        )),
        DfsMetadataSelection::StatObjectV1 { object_id } => {
            let prefix = backend
                .metadata_prefix_proof(&query.shard_id)?
                .filter(DfsMetadataPrefixProof::is_well_formed)
                .ok_or_else(|| {
                    MetadataBackendError::Serde("stat object has no exact prefix".into())
                })?;
            let object = backend.get_object(object_id)?.filter(|row| {
                row.file_system_id == query.file_system_id && row.tombstone_provenance.is_none()
            });
            Ok(DfsMetadataQueryResult::StatObjectV1 {
                object: object.map(Box::new),
                prefix,
            })
        }
        DfsMetadataSelection::StatProjectionV1 { request } => {
            crate::stat_projection::query_stat_projection(backend, query, request)
        }
        DfsMetadataSelection::DirectoryPage { path, after, limit }
        | DfsMetadataSelection::DirectoryPageForNamespaceJob {
            path, after, limit, ..
        } => {
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
        DfsMetadataSelection::SubtreeObjectPageV1 { path, after, limit } => {
            let path = crate::posix_path::normalize_posix_path(path)
                .map_err(|error| MetadataBackendError::Serde(error.to_string()))?;
            let page = backend.objects_for_subtree_page(
                &query.file_system_id,
                &path,
                after.as_ref(),
                (*limit).min(DFS_SUBTREE_OBJECT_PAGE_LIMIT),
            )?;
            Ok(DfsMetadataQueryResult::SubtreeObjectPageV1 {
                objects: page.objects,
                next_cursor: page.next_cursor,
            })
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
        DfsMetadataSelection::CurrentObjectForNamespaceJob {
            object_id,
            source_path,
            ..
        } => {
            // Source capture compares the canonical physical row, including
            // its original locations and tombstone provenance. A public read's
            // derived receipt locations are not that immutable source body.
            let object = backend.get_object(object_id)?.filter(|object| {
                object.file_system_id == query.file_system_id && object.path == *source_path
            });
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
        let requester = self.node_id.clone();
        self.serve_metadata_query_for_requester(query, &requester)
    }

    pub(crate) fn serve_metadata_query_for_requester(
        &mut self,
        query: &DfsMetadataShardQuery,
        requester: &NodeId,
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
        if let DfsMetadataSelection::StatProjectionV1 { request } = &query.selection {
            // This physical scope may own only aliases/children of a canonical
            // object in another shard. A lagging replica guard for that foreign
            // path is not this authority's read reservation. The canonical
            // authority checks its path, and the unchanged result checks below
            // still fence every actual object/alias returned by this scope.
            let owns_object_path = match self.namespace_execution.context() {
                Some(context) => {
                    crate::select_metadata_shard_config_for_path_strict_default_sifs_split(
                        &query.file_system_id,
                        &context.shards,
                        &request.object_path,
                    )?
                    .ok_or_else(|| {
                        error("stat projection has no current canonical path authority")
                    })?
                    .shard_id
                        == query.shard_id.0
                }
                // Without a certified routing context, retain the original
                // conservative guard; do not infer a foreign-scope exemption.
                None => true,
            };
            if owns_object_path {
                self.ensure_path_not_in_cross_shard_flight(
                    &query.file_system_id,
                    &request.object_path,
                )?;
            }
        }
        let (complete_stat_scope, stat_catalog) =
            if let DfsMetadataSelection::StatAtPathV1 { request } = &query.selection {
                self.ensure_path_not_in_cross_shard_flight(&query.file_system_id, &request.path)?;
                let context = self
                    .namespace_execution
                    .context()
                    .ok_or_else(|| error("stat path requires current authority context"))?;
                let mut current = context.shards.clone();
                current.sort_by(|a, b| a.shard_id.cmp(&b.shard_id));
                if request.read_id.is_empty()
                    || request.catalog_digest
                        != crate::stat_projection::stat_catalog_digest(&current)?
                    || crate::posix_path::normalize_posix_path(&request.path)
                        .ok()
                        .as_ref()
                        != Some(&request.path)
                    || !crate::select_metadata_shard_config_for_path_strict_default_sifs_split(
                        &query.file_system_id,
                        &current,
                        &request.path,
                    )?
                    .is_some_and(|s| s.shard_id == query.shard_id.0 && s.epoch == query.epoch)
                {
                    return Err(error("stat path names a stale catalog or invalid request"));
                }
                let complete = current.iter().all(|s| {
                    s.primary_node_id == self.node_id
                        && self.metadata_backend.as_ref().is_some_and(|b| {
                            b.has_physical_read_scope(&DfsMetadataShardId::new(s.shard_id.clone()))
                        })
                });
                (complete, current)
            } else {
                (false, Vec::new())
            };
        let coordinated_job = match &query.selection {
            DfsMetadataSelection::CurrentObjectForNamespaceJob {
                job,
                source_path,
                object_id,
            } => {
                let selected = job.pending_frames.iter().any(|frame| match frame {
                    DfsNamespaceJobFrame::CrossShardAdopt {
                        moved_object,
                        old_path,
                        ..
                    } => moved_object.object_id == *object_id && old_path == source_path,
                    DfsNamespaceJobFrame::CrossShardDirScan {
                        source_path: parent,
                        ..
                    } => crate::posix_path::parent_posix_path(source_path).as_ref() == Some(parent),
                    _ => false,
                });
                if !selected {
                    return Err(error(
                        "source query is outside its recorded namespace job frame",
                    ));
                }
                Some((job.as_ref(), source_path.as_str()))
            }
            DfsMetadataSelection::DirectoryPageForNamespaceJob {
                job, path, after, ..
            } => {
                if !job.pending_frames.iter().any(|frame| {
                    matches!(frame,
                    DfsNamespaceJobFrame::CrossShardDirScan { source_path, cursor, .. }
                    if source_path == path && cursor == after)
                }) {
                    return Err(error(
                        "source directory query differs from its recorded job cursor",
                    ));
                }
                Some((job.as_ref(), path.as_str()))
            }
            _ => None,
        };
        let coordinated_scope = if let Some((job, source_path)) = coordinated_job {
            let context = self.namespace_execution.context().ok_or_else(|| {
                error("namespace job source read has no current authority context")
            })?;
            if job.job_id.is_empty()
                || job.file_system_id != query.file_system_id
                || job.status != DfsNamespaceJobStatus::Running
                || !matches!(
                    job.kind,
                    DfsNamespaceJobKind::RenameObjectAcrossShardPrimaries
                        | DfsNamespaceJobKind::RenameDirectoryAcrossShardPrimaries
                )
                || job.root_path == "/"
                || crate::posix_path::normalize_posix_path(source_path)
                    .ok()
                    .as_deref()
                    != Some(source_path)
                || !crate::posix_path::posix_path_is_at_or_under_prefix(source_path, &job.root_path)
                || crate::posix_path::normalize_posix_path(&job.root_path)
                    .ok()
                    .as_ref()
                    != Some(&job.root_path)
            {
                return Err(error(
                    "source read has no valid original Running rename job",
                ));
            }
            let source = crate::select_metadata_shard_config_for_path_strict_default_sifs_split(
                &query.file_system_id,
                &context.shards,
                source_path,
            )?
            .ok_or_else(|| error("namespace job source path has no current authority"))?;
            if source.shard_id != query.shard_id.0
                || source.epoch != query.epoch
                || source.primary_node_id != self.node_id
            {
                return Err(error(
                    "source read path does not match the current queried shard",
                ));
            }
            let coordinator =
                crate::select_metadata_shard_config_for_path_strict_default_sifs_split(
                    &job.file_system_id,
                    &context.shards,
                    &job.root_path,
                )?
                .ok_or_else(|| {
                    error("namespace job source read has no current coordinator shard")
                })?;
            if coordinator.primary_node_id != *requester {
                return Err(error(
                    "namespace job source read requires its authenticated current coordinator",
                ));
            }
            Some((
                job.job_id.as_str(),
                DfsMetadataShardId::new(coordinator.shard_id),
                job.root_path.as_str(),
            ))
        } else {
            None
        };
        let query_deadline = self.transport.operation_deadline();
        let query_cancellation = self.cancellation.clone();
        #[cfg(test)]
        let diagnostic_root = self
            .native_publication_owner_for_observation()
            .map(|(root, _)| root);
        let ensure_read = |file_system_id: &DfsFileSystemId, path: &str| {
            if query_cancellation
                .as_ref()
                .is_some_and(DfsCancellationFlag::is_cancelled)
            {
                return Err(DfsError::Cancelled {
                    operation: "metadata query",
                });
            }
            if query_deadline.is_some_and(|d| std::time::Instant::now() >= d) {
                return Err(error("metadata query reached its existing deadline"));
            }
            let Some(path_key) = crate::posix_path::normalized_posix_path_key(path) else {
                return Ok(());
            };
            let exemption = coordinated_scope
                .as_ref()
                .map(|(id, shard, root)| (*id, shard, *root));
            if self.cross_shard_namespace_locks.blocks_except_verified_job(
                file_system_id,
                &path_key,
                exemption,
            ) {
                #[cfg(test)]
                if let Some(root) = &diagnostic_root {
                    crate::native_boundary_diagnostics::record(
                        &root,
                        "metadata-query-path-blocked",
                        || {
                            serde_json::json!({
                                "node": self.node_id, "file_system_id": file_system_id, "path": path,
                                "locks": format!("{:?}", self.cross_shard_namespace_locks),
                                "coordinated_exemption": format!("{exemption:?}"),
                            })
                        },
                        true,
                    );
                }
                return Err(DfsError::Busy {
                    path: path.to_owned(),
                });
            }
            Ok(())
        };
        if let DfsMetadataSelection::DirectoryPageForNamespaceJob { path, .. } = &query.selection {
            ensure_read(&query.file_system_id, path)?;
        }
        let backend = self
            .metadata_backend
            .as_mut()
            .ok_or_else(|| error("history query requires durable metadata"))?;
        let exact_stat_scope =
            matches!(
                query.selection,
                DfsMetadataSelection::StatProjectionV1 { .. }
                    | DfsMetadataSelection::StatObjectV1 { .. }
            ) || matches!(query.selection, DfsMetadataSelection::StatAtPathV1 { .. })
                && !complete_stat_scope;
        let stat_scope_ids = stat_catalog
            .iter()
            .map(|s| DfsMetadataShardId::new(s.shard_id.clone()))
            .collect::<Vec<_>>();
        let result = if let DfsMetadataSelection::StatAtPathV1 { request } = &query.selection {
            let mut remaining = HISTORY_PAGE_LIMIT;
            let mut accepted = None;
            for _ in 0..crate::posix::POSIX_ROUTED_SYNC_RETRY_ROUNDS {
                ensure_read(&query.file_system_id, &request.path)?;
                if exact_stat_scope {
                    backend.begin_stat_read_snapshot_on_shard(&query.shard_id)?;
                } else {
                    backend.begin_stat_read_snapshot_on_shards(&stat_scope_ids)?;
                }
                let view = crate::stat_projection::query_stat_path(
                    backend.as_ref(),
                    query,
                    request,
                    complete_stat_scope,
                    &stat_catalog,
                    &ensure_read,
                    &mut remaining,
                );
                if exact_stat_scope {
                    backend.end_stat_read_snapshot_on_shard(&query.shard_id);
                } else {
                    backend.end_stat_read_snapshot_on_shards(&stat_scope_ids);
                }
                let view = view?;
                if view.complete {
                    // Reopen after releasing the captured snapshots. Equal
                    // exact prefixes prove overlapping stable read intervals;
                    // an unchanged borrowed snapshot is not this validation.
                    backend.begin_stat_read_snapshot_on_shards(&stat_scope_ids)?;
                    let validation = (|| {
                        for cut in &view.prefixes {
                            ensure_read(&query.file_system_id, &request.path)
                                .map_err(|e| MetadataBackendError::Serde(e.to_string()))?;
                            if backend.metadata_prefix_proof(&cut.shard_id)?.as_ref()
                                != Some(&cut.prefix)
                            {
                                return Ok(false);
                            }
                        }
                        Ok::<_, MetadataBackendError>(true)
                    })();
                    backend.end_stat_read_snapshot_on_shards(&stat_scope_ids);
                    if !validation? {
                        continue;
                    }
                }
                // Detect catalog additions as well as changes to an existing
                // member. The native Main reuses its current retained catalog
                // certificate; exact-key checks alone cannot prove no new shard.
                let mut current_catalog = DfsControlPlaneStore::new(
                    self.control_plane
                        .as_mut()
                        .ok_or_else(|| error("stat path lost its configuration owner"))?
                        .as_mut(),
                )
                .list_metadata_shards(&query.file_system_id)?;
                current_catalog.sort_by(|a, b| a.shard_id.cmp(&b.shard_id));
                if crate::stat_projection::stat_catalog_digest(&current_catalog)?
                    != request.catalog_digest
                {
                    return Err(error("stat path authority catalog changed during capture"));
                }
                accepted = Some(DfsMetadataQueryResult::StatAtPathV1 {
                    view: Box::new(view),
                });
                break;
            }
            accepted.ok_or_else(|| {
                MetadataBackendError::Serde(
                    "stat path could not validate a stable view within the existing read rounds"
                        .into(),
                )
            })
        } else {
            if exact_stat_scope {
                backend.begin_stat_read_snapshot_on_shard(&query.shard_id)?;
            } else {
                backend.begin_consistent_read_snapshot()?;
            }
            let result = backend.query_file_history_on_shard(query);
            if exact_stat_scope {
                backend.end_stat_read_snapshot_on_shard(&query.shard_id);
            } else {
                backend.end_consistent_read_snapshot();
            }
            result
        };
        let result = result?;
        match &result {
            DfsMetadataQueryResult::StatAtPathV1 { view } => {
                if let Some(object) = view.object.as_deref() {
                    ensure_read(&object.file_system_id, &object.path)?;
                }
            }
            DfsMetadataQueryResult::StatProjectionV1 { page } => {
                for object in page.entries.iter().chain(page.object.as_deref()) {
                    ensure_read(&object.file_system_id, &object.path)?;
                }
            }
            DfsMetadataQueryResult::DirectoryPage { entries, .. } => {
                for (object, _) in entries {
                    ensure_read(&object.file_system_id, &object.path)?;
                }
            }
            DfsMetadataQueryResult::CurrentObject {
                object: Some(object),
            }
            | DfsMetadataQueryResult::StatObjectV1 {
                object: Some(object),
                ..
            }
            | DfsMetadataQueryResult::ObjectLifecycle {
                object: Some(object),
                ..
            } => {
                ensure_read(&object.file_system_id, &object.path)?;
            }
            DfsMetadataQueryResult::Version {
                descriptor: Some(descriptor),
            } => ensure_read(
                &descriptor.current_object.file_system_id,
                &descriptor.current_object.path,
            )?,
            DfsMetadataQueryResult::RevisionMetadata {
                descriptor: Some(descriptor),
            } => ensure_read(
                &descriptor.current_object.file_system_id,
                &descriptor.current_object.path,
            )?,
            DfsMetadataQueryResult::RetainedRevisions { entries, .. } => {
                for entry in entries {
                    ensure_read(
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
        self.query_metadata_authority_from_snapshot(shard, selection, None)
    }

    pub(crate) fn query_metadata_authority_from_snapshot(
        &mut self,
        shard: &DfsMetadataShardConfig,
        selection: DfsMetadataSelection,
        snapshot: Option<&[DfsMetadataShardConfig]>,
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
            let outcome = self.route_metadata_query(shard, &query, snapshot)?;
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
        if let DfsMetadataQueryResult::StatProjectionChangedV1 {
            request: refused,
            current_prefix,
        } = &response.result
        {
            let DfsMetadataSelection::StatProjectionV1 { request } = &query.selection else {
                return Err(error("unexpected stat generation refusal"));
            };
            if refused != request
                || !current_prefix.is_well_formed()
                || request.expected_prefix.as_ref().is_none_or(|expected| {
                    !expected.is_well_formed()
                        || current_prefix.journal_sequence <= expected.journal_sequence
                })
            {
                return Err(error("invalid or non-forward stat generation refusal"));
            }
            return Err(DfsError::MetadataRejection {
                code: DfsMetadataRejectionCode::PrefixMismatch,
                message: "authoritative stat generation advanced during collection".into(),
            });
        }
        Ok(response.result)
    }

    /// The read uses the existing namespace command, authenticated sender and
    /// exact response fence. Its wait must not enter the mutation dispatcher.
    fn route_metadata_query(
        &mut self,
        expected: &DfsMetadataShardConfig,
        query: &DfsMetadataShardQuery,
        snapshot: Option<&[DfsMetadataShardConfig]>,
    ) -> Result<PendingRoutedNamespaceResult, DfsError> {
        self.check_cancelled("metadata query")?;
        let snapshot = match snapshot {
            Some(snapshot) => snapshot.to_vec(),
            None => self
                .metadata_shard_config_snapshot(&query.file_system_id)?
                .unwrap_or_default(),
        };
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
        // Declare the complete co-owned read footprint so the existing scope
        // classifier borrows Main's Global backend rather than a single shard.
        // Cross-owner projections remain exact-shard reads and page explicitly.
        let authority_shards =
            if matches!(query.selection, DfsMetadataSelection::StatAtPathV1 { .. })
                && snapshot
                    .iter()
                    .all(|s| s.primary_node_id == current.primary_node_id)
            {
                snapshot.clone()
            } else {
                vec![current.clone()]
            };
        let request_id = self.next_request_id();
        let command = DfsNamespaceCommand {
            request_id: request_id.clone(),
            file_system_id: query.file_system_id.clone(),
            requesting_node_id: self.node_id.clone(),
            include_metadata_journal_batches: false,
            metadata_journal_after_sequences: Vec::new(),
            authority_shards,
            operation: DfsNamespaceOperation::MetadataQuery {
                query: query.clone(),
            },
        };
        // An actual local primary reads its resident generation. Mounted clients
        // continue to delegate to that primary through their ordinary transport.
        if current.primary_node_id == self.node_id
            && !self.transport.delegates_local_namespace_authority()
            && self
                .metadata_backend
                .as_ref()
                .is_some_and(|backend| backend.has_physical_read_scope(&query.shard_id))
            && !matches!(&query.selection, DfsMetadataSelection::StatAtPathV1 { .. }
                if command.authority_shards.iter().any(|s| !self.metadata_backend.as_ref().is_some_and(|b|
                    b.has_physical_read_scope(&DfsMetadataShardId::new(s.shard_id.clone())))))
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
            if expected.primary_node_id == self.node_id {
                let message = self.next_dfs_routed_message(
                    &expected.primary_node_id,
                    DfsNetworkMessage::NamespaceRequest(command.clone()),
                );
                self.transport
                    .send_local_metadata_query_to_main(message)
                    .map_err(DfsError::from_network)?;
            } else if !self.send_dfs_message_if_routable(
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
        let (object, size) =
            self.authorize_version_content(identity, content_hash, expected_size_bytes)?;
        let capacity =
            usize::try_from(size).map_err(|_| error("version is too large to collect"))?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(capacity)
            .map_err(|_| error("cannot allocate version content"))?;
        loop {
            let offset = bytes.len() as u64;
            let page = self.read_authorized_range(
                &object,
                size,
                offset,
                DFS_FILE_HISTORY_RANGE_LIMIT as usize,
            )?;
            bytes.extend_from_slice(&page);
            if bytes.len() == capacity {
                return Ok(bytes);
            }
        }
    }

    pub fn read_sifs_version_range(
        &mut self,
        identity: &DfsRevisionReference,
        hash: &str,
        size: u64,
        offset: u64,
        length: u32,
    ) -> Result<Vec<u8>, DfsError> {
        if length == 0 || length > crate::DFS_BROWSER_RANGE_LIMIT || offset > size {
            return Err(error("invalid immutable content range"));
        }
        let (object, size) = self.authorize_version_content(identity, hash, Some(size))?;
        self.read_authorized_range(&object, size, offset, length as usize)
    }

    fn authorize_version_content(
        &mut self,
        identity: &DfsRevisionReference,
        content_hash: &str,
        expected_size_bytes: Option<u64>,
    ) -> Result<(DfsObjectMetadata, u64), DfsError> {
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
        if object.object_id != identity.object_id
            || object.version != identity.version
            || object.file_system_id != identity.file_system_id
        {
            return Err(error(
                "resolved descriptor does not match the requested version",
            ));
        }
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
        Ok((object, size))
    }

    fn read_authorized_range(
        &mut self,
        object: &DfsObjectMetadata,
        size: u64,
        offset: u64,
        length: usize,
    ) -> Result<Vec<u8>, DfsError> {
        let outcome = self
            .read_immutable_object_metadata_range_or_fetch_remote_without_cache(
                object, offset, length,
            )
            .map_err(|source| error(source.to_string()))?;
        let NetworkedDfsRangeReadOutcome::Found { data, .. } = outcome else {
            return Err(error(
                "authorized version has no currently readable replica",
            ));
        };
        if data.object_id != object.object_id
            || data.version != object.version
            || data.content_hash != object.content_hash
            || data.total_size_bytes != size
            || data.offset != offset
            || data.bytes.len() as u64 != size.saturating_sub(offset).min(length as u64)
        {
            return Err(error(
                "version range does not match its authoritative descriptor",
            ));
        }
        Ok(data.bytes)
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
