//! Authoritative history discovery and immutable, bounded content reads.

use crate::*;
use serde::{Deserialize, Serialize};

pub(crate) const HISTORY_PAGE_LIMIT: usize = 128;
pub const DFS_FILE_HISTORY_RANGE_LIMIT: u32 = 64 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum DfsMetadataSelection {
    /// One current path owner's metadata, including its maintained namespace count.
    /// No alias enumeration, cross-shard snapshot, or generation revalidation.
    StatAtPathV2 { path: String },
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
    /// The queried shard's contributions to `target_shard`'s maintained
    /// namespace counts under the serving primary's current catalog. Only the
    /// shard's current primary answers. A continuation names the prefix of
    /// its first page; older peers reject this selection. Each page also
    /// answers the request's `question` about what the shard holds.
    CountContributionsV1 {
        target_shard: String,
        #[serde(default)]
        expected_prefix: Option<DfsMetadataPrefixProof>,
        #[serde(default)]
        after: Option<DfsCountContributionCursor>,
        #[serde(default)]
        question: DfsCountContributionQuestion,
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
    StatAtPathV2 {
        #[serde(deserialize_with = "crate::protocol::required_optional")]
        object: Option<Box<DfsObjectMetadata>>,
    },
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
    CountContributionsV1 {
        page: DfsCountContributionPage,
    },
    /// The continuation's expected prefix is no longer the shard's current
    /// prefix. A typed refusal, not an empty page; the read restarts.
    CountContributionsChangedV1 {
        prefix: DfsMetadataPrefixProof,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataShardResponse {
    pub file_system_id: DfsFileSystemId,
    pub shard_id: DfsMetadataShardId,
    pub epoch: u64,
    pub result: DfsMetadataQueryResult,
}

/// The shards one fan-out over every shard authority found closed.
///
/// A shard whose storage is closed refuses a query addressed to it, locally or
/// from a remote node. In a fan-out that refusal means that authority cannot
/// answer now, never that it holds nothing: the fan-out notes the shard and
/// asks the others. An answer that decides the result on its own stands, such
/// as the live record of the object in an open shard: a closed shard does not
/// change while closed, and a move into a closed target waits until it is
/// Active again, so the closed shard holds no record newer than its closure.
/// Without a deciding answer the fan-out is undecided, and returns the
/// retryable `Busy` rather than absence. Every fan-out over the authorities
/// queries through [`NetworkedDfsService::query_metadata_authority_in_fan_out`].
///
/// A shard whose primary the query did not reach is noted the same way and
/// asked no further while the others are asked: an unrelated object does not
/// depend on an unavailable primary. Its answer is needed only when no other
/// shard decided; [`NetworkedDfsService::fan_out_undecided`] then returns the
/// query's error, or, in an attribute read, waits for that shard's record.
#[derive(Debug, Default)]
pub(crate) struct DfsAuthorityFanOut {
    closed: Vec<DfsMetadataShardConfig>,
    unanswered: Vec<(DfsMetadataShardConfig, DfsError)>,
}

impl DfsAuthorityFanOut {
    /// The shards that could not answer because their storage is closed.
    pub(crate) fn closed(&self) -> &[DfsMetadataShardConfig] {
        &self.closed
    }

    /// Whether `shard` was asked and its primary was not reached.
    pub(crate) fn left_unanswered(&self, shard: &DfsMetadataShardConfig) -> bool {
        self.unanswered.iter().any(|(asked, _)| asked == shard)
    }

    /// Start the next question of a fan-out that asks several in turn: the
    /// shards left unanswered so far concern the questions already decided.
    pub(crate) fn next_question(&mut self) {
        self.unanswered.clear();
    }

    /// The fan-out found no deciding answer: `none` when every authority
    /// answered, otherwise `Busy` naming `subject` and the closed shards.
    pub(crate) fn undecided<R>(&self, subject: impl std::fmt::Display, none: R) -> Result<R, DfsError> {
        if self.closed.is_empty() {
            return Ok(none);
        }
        Err(self.refusal(subject))
    }

    /// The retryable refusal of an undecided fan-out, naming `subject` and the
    /// closed shards.
    pub(crate) fn refusal(&self, subject: impl std::fmt::Display) -> DfsError {
        let shards = self
            .closed
            .iter()
            .map(|shard| shard.shard_id.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        DfsError::Busy {
            path: format!("{subject} (metadata shard {shards} storage is closed)"),
        }
    }
}

/// What one metadata query came back with.
pub(crate) enum DfsMetadataQueryAnswer {
    Answered(DfsMetadataQueryResult),
    /// The query was not delivered, or its primary refused it for authority;
    /// the error is what the query ended with.
    Unanswered(DfsError),
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
        DfsMetadataSelection::StatAtPathV2 { path } => {
            let normalized = crate::posix_path::normalize_posix_path(path)
                .map_err(|error| MetadataBackendError::Serde(error.to_string()))?;
            if normalized != *path {
                return Err(MetadataBackendError::Serde("stat path is not canonical".into()));
            }
            let mut object = backend.get_object_by_path_on_shard(
                &query.shard_id, &query.file_system_id, path,
            )?.filter(|row| row.file_system_id == query.file_system_id
                && row.tombstone_provenance.is_none() && row.kind != DfsObjectKind::Tombstone);
            crate::stat_projection::enrich_stat_object(backend, &mut object)?;
            Ok(DfsMetadataQueryResult::StatAtPathV2 { object: object.map(Box::new) })
        }
        DfsMetadataSelection::StatAtPathV1 { .. } => Err(MetadataBackendError::Serde(
            "stat path requires its authenticated owning service and complete catalog".into(),
        )),
        DfsMetadataSelection::CountContributionsV1 { .. } => Err(MetadataBackendError::Serde(
            "count contributions require the authenticated primary and its current catalog".into(),
        )),
        DfsMetadataSelection::StatObjectV1 { object_id } => {
            let prefix = backend
                .metadata_prefix_proof(&query.shard_id)?
                .filter(DfsMetadataPrefixProof::is_well_formed)
                .ok_or_else(|| {
                    MetadataBackendError::Serde("stat object has no exact prefix".into())
                })?;
            let mut object = backend.get_object(object_id)?.filter(|row| {
                row.file_system_id == query.file_system_id && row.tombstone_provenance.is_none()
            });
            crate::stat_projection::enrich_stat_object(backend, &mut object)?;
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
            // An absent identity is not proof that its old path is vacant.
            // Read both from this same physical snapshot, including when the
            // original job is corroborating an already-retired source.
            if backend
                .get_object_by_path_on_shard(&query.shard_id, &query.file_system_id, source_path)?
                .is_some_and(|occupant| {
                    occupant.kind != DfsObjectKind::Tombstone
                        && occupant.tombstone_provenance.is_none()
                        && occupant.object_id != *object_id
                })
            {
                return Err(MetadataBackendError::Serde(
                    "namespace job source path names another live object".into(),
                ));
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
        // Native admission (or the actual local owner) already captured the
        // operation's complete routing projection. Reuse that view to exclude
        // ancestor-shard copies rather than fetching the catalog again per read.
        // The normal response boundary separately revalidates shard authority.
        let inode_catalog = if matches!(query.selection,
            DfsMetadataSelection::StatAtPathV2 { .. } | DfsMetadataSelection::StatObjectV1 { .. }) {
            Some(self.namespace_execution.context()
                .filter(|context| context.file_system_id == query.file_system_id)
                .ok_or_else(|| error("stat requires its admitted routing projection"))?
                .shards.clone())
        } else { None };
        // Contributions attribute every path with the catalog this primary
        // admitted the query under; the epoch/primary check above binds it.
        let contribution_catalog = if matches!(
            query.selection,
            DfsMetadataSelection::CountContributionsV1 { .. }
        ) {
            Some(
                self.namespace_execution
                    .context()
                    .filter(|context| context.file_system_id == query.file_system_id)
                    .ok_or_else(|| {
                        error("count contributions require the admitted authority catalog")
                    })?
                    .shards
                    .clone(),
            )
        } else {
            None
        };
        // Path-resolution stats follow the lookup rule: refused only at or
        // under a moving path, not for a directory that contains one.
        if let DfsMetadataSelection::StatAtPathV2 { path } = &query.selection {
            self.ensure_lookup_path_not_in_cross_shard_flight(&query.file_system_id, path)?;
            if !crate::select_metadata_shard_config_for_path_strict_default_sifs_split(
                &query.file_system_id,
                inode_catalog.as_deref().expect("stat catalog"),
                path,
            )?
            .is_some_and(|shard| shard.shard_id == query.shard_id.0 && shard.epoch == query.epoch)
            {
                return Err(error("stat path does not belong to the queried authority"));
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
                self.ensure_lookup_path_not_in_cross_shard_flight(
                    &query.file_system_id,
                    &request.path,
                )?;
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
                    }
                    | DfsNamespaceJobFrame::CrossShardComplete {
                        moved_object,
                        old_path,
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
                Some((job.as_ref(), source_path.as_str(), false))
            }
            DfsMetadataSelection::DirectoryPageForNamespaceJob {
                job, path, after, ..
            } => {
                // The job's recorded scan cursor binds the query. A cursor
                // without a shard is the directory owner's own page: only the
                // owner answers it, after exactly that cursor. A shard-tagged
                // cursor, or none, spans every shard that owns a child of the
                // directory, in shard order: the queried shard's part is the
                // cursor without its shard when it names this shard, the
                // shard's start when it names an earlier shard or there is no
                // cursor, and nothing when it names a later shard, whose page
                // this shard already finished. `after` must equal that part;
                // any other query is refused.
                let queried = &query.shard_id;
                let part = job.pending_frames.iter().find_map(|frame| {
                    let DfsNamespaceJobFrame::CrossShardDirScan {
                        source_path,
                        cursor,
                        ..
                    } = frame
                    else {
                        return None;
                    };
                    if source_path != path {
                        return None;
                    }
                    let owner_page = cursor.as_ref().is_some_and(|c| c.shard_id.is_none());
                    let accepted = match cursor.as_ref().and_then(|c| c.shard_id.as_ref()) {
                        None => cursor == after,
                        Some(shard) if shard == queried => {
                            after.as_ref()
                                == cursor
                                    .as_ref()
                                    .map(crate::metadata_keys::local_directory_page_cursor)
                                    .as_ref()
                        }
                        Some(shard) => shard < queried && after.is_none(),
                    };
                    accepted.then_some(owner_page)
                });
                let Some(owner_page) = part else {
                    return Err(error(
                        "source directory query differs from its recorded job cursor",
                    ));
                };
                Some((job.as_ref(), path.as_str(), owner_page))
            }
            _ => None,
        };
        let coordinated_scope = if let Some((job, source_path, owner_page)) = coordinated_job {
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
            // A directory page is read from each shard that owns a child of
            // the directory, an owner's own page and any other job read from
            // the source path's owner.
            let source = if !owner_page
                && matches!(
                    query.selection,
                    DfsMetadataSelection::DirectoryPageForNamespaceJob { .. }
                ) {
                crate::posix::directory_child_authority_shards(source_path, &context.shards)
                    .map_err(|failure| error(failure.to_string()))?
                    .into_iter()
                    .find(|shard| shard.shard_id == query.shard_id.0)
                    .cloned()
                    .ok_or_else(|| {
                        error("source directory has no child owned by the current queried shard")
                    })?
            } else {
                crate::select_metadata_shard_config_for_path_strict_default_sifs_split(
                    &query.file_system_id,
                    &context.shards,
                    source_path,
                )?
                .ok_or_else(|| error("namespace job source path has no current authority"))?
            };
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
        // A path stat resolves its path component by component; each check
        // follows the lookup rule. Every other read keeps the two-way rule.
        let path_stat = matches!(query.selection, DfsMetadataSelection::StatAtPathV1 { .. });
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
            let blocked = if path_stat {
                self.cross_shard_namespace_locks
                    .blocks_lookup_except_verified_job(file_system_id, &path_key, exemption)
            } else {
                self.cross_shard_namespace_locks.blocks_except_verified_job(
                    file_system_id,
                    &path_key,
                    exemption,
                )
            };
            if blocked {
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
        match &query.selection {
            DfsMetadataSelection::DirectoryPageForNamespaceJob { path, .. }
            | DfsMetadataSelection::CurrentObjectForNamespaceJob {
                source_path: path, ..
            } => ensure_read(&query.file_system_id, path)?,
            _ => {}
        }
        let backend = self
            .metadata_backend
            .as_mut()
            .ok_or_else(|| error("history query requires durable metadata"))?;
        let exact_stat_scope =
            matches!(
                query.selection,
                DfsMetadataSelection::StatAtPathV2 { .. }
                    | DfsMetadataSelection::StatProjectionV1 { .. }
                    | DfsMetadataSelection::StatObjectV1 { .. }
                    // Pins the shard's tables together with their prefix.
                    | DfsMetadataSelection::CountContributionsV1 { .. }
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
            let result = match contribution_catalog.as_deref() {
                Some(catalog) => {
                    // This node is the shard's primary, which runs its jobs.
                    let node = self.node_id.clone();
                    backend.count_contributions_on_shard(query, catalog, &|job| {
                        crate::maintenance::storage_waits::job_storage_waits(&node, job)
                    })
                }
                None => backend.query_file_history_on_shard(query),
            };
            if exact_stat_scope {
                backend.end_stat_read_snapshot_on_shard(&query.shard_id);
            } else {
                backend.end_consistent_read_snapshot();
            }
            result
        };
        let mut result = result?;
        if let DfsMetadataQueryResult::StatObjectV1 { object, .. } = &mut result {
            if let Some(row) = object.as_ref()
                && !crate::select_metadata_shard_config_for_path_strict_default_sifs_split(
                    &query.file_system_id, inode_catalog.as_deref().expect("stat catalog"), &row.path,
                )?.is_some_and(|owner| owner.shard_id == query.shard_id.0
                    && owner.epoch == query.epoch && owner.primary_node_id == self.node_id) {
                *object = None;
            }
        }
        if let DfsMetadataSelection::StatAtPathV2 { path } = &query.selection {
            ensure_read(&query.file_system_id, path)?;
        }
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
            DfsMetadataQueryResult::StatAtPathV2 { object: Some(object) }
            | DfsMetadataQueryResult::CurrentObject {
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
            // Not fenced by in-flight cross-shard paths: the page reports the
            // Running jobs that hold them, and its consumer accounts for them.
            DfsMetadataQueryResult::CountContributionsV1 { .. }
            | DfsMetadataQueryResult::CountContributionsChangedV1 { .. } => {}
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

    /// One query of a fan-out over every shard authority (see
    /// [`DfsAuthorityFanOut`]): `None` when `shard` cannot answer now because
    /// its storage is closed or its primary was not reached, which `fan_out`
    /// notes. The query is sent once; the fan-out asks the other shards.
    pub(crate) fn query_metadata_authority_in_fan_out(
        &mut self,
        fan_out: &mut DfsAuthorityFanOut,
        shard: &DfsMetadataShardConfig,
        selection: DfsMetadataSelection,
        snapshot: Option<&[DfsMetadataShardConfig]>,
    ) -> Result<Option<DfsMetadataQueryResult>, DfsError> {
        match self.query_metadata_authority_routed(shard, selection, snapshot, false) {
            Err(error) | Ok(DfsMetadataQueryAnswer::Unanswered(error))
                if crate::error::dfs_error_is_closed_shard_storage(&error) =>
            {
                if !fan_out.closed.contains(shard) {
                    fan_out.closed.push(shard.clone());
                }
                Ok(None)
            }
            Ok(DfsMetadataQueryAnswer::Unanswered(error)) => {
                if !fan_out.left_unanswered(shard) {
                    fan_out.unanswered.push((shard.clone(), error));
                }
                Ok(None)
            }
            Ok(DfsMetadataQueryAnswer::Answered(result)) => Ok(Some(result)),
            Err(error) => Err(error),
        }
    }

    /// A fan-out found no deciding answer (see [`DfsAuthorityFanOut`]). A
    /// shard it left unanswered could hold the answer, so its answer is
    /// needed: the error its query ended with, or, in an attribute read, the
    /// wait for that shard's record. Otherwise `none` when every authority
    /// answered, or the retryable `Busy` naming the closed shards.
    pub(crate) fn fan_out_undecided<R>(
        &mut self,
        fan_out: &DfsAuthorityFanOut,
        subject: impl std::fmt::Display,
        none: R,
    ) -> Result<R, DfsError> {
        if let Some(needed) = self.fan_out_needed_answer(fan_out) {
            return Err(needed);
        }
        fan_out.undecided(subject, none)
    }

    /// What a fan-out without a deciding answer ends with while a shard it
    /// left unanswered could hold the answer; `None` when it left none.
    ///
    /// An attribute read waits for that shard's record, as its own query
    /// would: a newer record ends the read with
    /// [`DfsError::ShardRecordChanged`], and the re-send time with
    /// [`DfsError::ShardAnswerDue`], so the read asks every shard again. A
    /// mounted call hands the wait back and a background pass parks it.
    /// Any other fan-out sent its queries once and returns the error.
    pub(crate) fn fan_out_needed_answer(
        &mut self,
        fan_out: &DfsAuthorityFanOut,
    ) -> Option<DfsError> {
        let (shard, unanswered) = fan_out.unanswered.first()?;
        if !self.attribute_read_replans {
            return Some(unanswered.clone());
        }
        let mut resend = self
            .attribute_read_resend
            .take()
            .unwrap_or_else(crate::service_record_follow::DfsRecordFollowResend::new);
        let followed = self.follow_undelivered_read(shard, &mut resend);
        self.attribute_read_resend = Some(resend);
        Some(match followed {
            Err(error) => error,
            Ok(crate::service_record_follow::DfsRecordFollowed::ResendDue(record))
                if record.shard_id == shard.shard_id
                    && record.epoch == shard.epoch
                    && record.primary_node_id == shard.primary_node_id =>
            {
                DfsError::ShardAnswerDue {
                    file_system_id: shard.file_system_id.clone(),
                    shard_id: shard.shard_id.clone(),
                    epoch: shard.epoch,
                }
            }
            Ok(
                crate::service_record_follow::DfsRecordFollowed::Newer(_)
                | crate::service_record_follow::DfsRecordFollowed::ResendDue(_),
            ) => DfsError::ShardRecordChanged {
                file_system_id: shard.file_system_id.clone(),
                shard_id: shard.shard_id.clone(),
                epoch: shard.epoch,
            },
            Ok(
                crate::service_record_follow::DfsRecordFollowed::Woken
                | crate::service_record_follow::DfsRecordFollowed::Ended,
            ) => unanswered.clone(),
        })
    }

    pub(crate) fn query_metadata_authority_from_snapshot(
        &mut self,
        shard: &DfsMetadataShardConfig,
        selection: DfsMetadataSelection,
        snapshot: Option<&[DfsMetadataShardConfig]>,
    ) -> Result<DfsMetadataQueryResult, DfsError> {
        match self.query_metadata_authority_routed(shard, selection, snapshot, true)? {
            DfsMetadataQueryAnswer::Answered(result) => Ok(result),
            DfsMetadataQueryAnswer::Unanswered(error) => Err(error),
        }
    }

    /// Ask `shard`'s authority one metadata query. With `follow`, a query
    /// an attribute read makes follows its record (see
    /// [`Self::route_metadata_query`]); without it the query is sent once.
    fn query_metadata_authority_routed(
        &mut self,
        shard: &DfsMetadataShardConfig,
        selection: DfsMetadataSelection,
        snapshot: Option<&[DfsMetadataShardConfig]>,
        follow: bool,
    ) -> Result<DfsMetadataQueryAnswer, DfsError> {
        let query = DfsMetadataShardQuery {
            file_system_id: shard.file_system_id.clone(),
            shard_id: DfsMetadataShardId::new(shard.shard_id.clone()),
            epoch: shard.epoch,
            selection,
        };
        let response = if self.control_plane.is_none() {
            self.serve_metadata_query(&query)?
        } else {
            let (outcome, unanswered) = self.route_metadata_query(shard, &query, snapshot, follow);
            let response = outcome.and_then(|outcome| {
                match (outcome.result, outcome.posix_coordination_result) {
                    (
                        DfsNamespaceResult::Applied,
                        Some(DfsPosixCoordinationResult::MetadataQuery(response)),
                    ) => Ok(response),
                    (
                        DfsNamespaceResult::Rejected {
                            posix_error:
                                Some(DfsNamespacePosixError::ShardStorageClosed { cause, detail }),
                            ..
                        },
                        _,
                    ) => Err(DfsError::ShardStorageClosed { cause, detail }),
                    (DfsNamespaceResult::Rejected { reason, .. }, _) => Err(error(reason)),
                    _ => Err(error("history authority omitted its typed query response")),
                }
            });
            match response {
                Ok(response) => response,
                Err(failed) if unanswered => {
                    return Ok(DfsMetadataQueryAnswer::Unanswered(failed));
                }
                Err(failed) => return Err(failed),
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
        match (&query.selection, &response.result) {
            (
                DfsMetadataSelection::CountContributionsV1 {
                    expected_prefix,
                    question,
                    ..
                },
                DfsMetadataQueryResult::CountContributionsV1 { page },
            ) => {
                if page.source_shard != query.shard_id.0
                    || !page.held.answers(question)
                    || !page.prefix.is_well_formed()
                    || expected_prefix
                        .as_ref()
                        .is_some_and(|expected| expected != &page.prefix)
                    || page.complete != page.after.is_none()
                    || page.entry_count()
                        > crate::count_contributions::DFS_COUNT_CONTRIBUTION_PAGE_LIMIT
                {
                    return Err(error("count contribution page does not match its query"));
                }
            }
            // The refusal is returned typed: the caller restarts its read at
            // the new prefix rather than treating this as a transport error.
            (
                DfsMetadataSelection::CountContributionsV1 {
                    expected_prefix: Some(expected),
                    ..
                },
                DfsMetadataQueryResult::CountContributionsChangedV1 { prefix },
            ) => {
                if !prefix.is_well_formed()
                    || !expected.is_well_formed()
                    || prefix.journal_sequence <= expected.journal_sequence
                {
                    return Err(error("invalid or non-forward count contribution refusal"));
                }
            }
            (DfsMetadataSelection::CountContributionsV1 { .. }, _)
            | (
                _,
                DfsMetadataQueryResult::CountContributionsV1 { .. }
                | DfsMetadataQueryResult::CountContributionsChangedV1 { .. },
            ) => return Err(error("count contribution authority returned the wrong query kind")),
            _ => {}
        }
        Ok(DfsMetadataQueryAnswer::Answered(response.result))
    }

    /// The read uses the existing namespace command, authenticated sender and
    /// exact response fence. Its wait must not enter the mutation dispatcher.
    /// Ask `expected`'s primary one metadata query. A query names one exact
    /// record. One an attribute read makes follows that record like any
    /// other read when it could not be delivered or its primary refused it
    /// for authority: it is sent again on the re-send timer while the record
    /// stands, and a newer record ends it with
    /// [`DfsError::ShardRecordChanged`], so the attribute read plans again at
    /// the new record. A query another operation planned is sent once.
    /// Without `follow` (a query of a fan-out) the query is sent once too.
    ///
    /// Also returns whether the query was left unanswered: it was not
    /// delivered, or its primary refused it for authority to a requester
    /// that follows record changes.
    fn route_metadata_query(
        &mut self,
        expected: &DfsMetadataShardConfig,
        query: &DfsMetadataShardQuery,
        snapshot: Option<&[DfsMetadataShardConfig]>,
        follow: bool,
    ) -> (Result<PendingRoutedNamespaceResult, DfsError>, bool) {
        let mut expected = expected.clone();
        let mut snapshot = snapshot.map(<[DfsMetadataShardConfig]>::to_vec);
        let mut resend = crate::service_record_follow::DfsRecordFollowResend::new();
        loop {
            let (result, undelivered) =
                self.route_metadata_query_once(&expected, query, snapshot.as_deref());
            let refused_for_authority = matches!(
                &result,
                Ok(routed) if matches!(
                    routed.result,
                    DfsNamespaceResult::Rejected { current_primary_node_id: Some(_), .. }
                )
            );
            let unanswered =
                undelivered || (refused_for_authority && self.follows_record_changes());
            // Only an attribute read plans again at a newer record; a query
            // another operation planned is sent once, as that operation's
            // plan stands.
            if !follow || !self.attribute_read_replans || !unanswered {
                if undelivered {
                    self.request_primary_verification(&expected);
                }
                return (result, unanswered);
            }
            let followed = match self.follow_undelivered_read(&expected, &mut resend) {
                Ok(followed) => followed,
                Err(error) => return (Err(error), false),
            };
            match followed {
                crate::service_record_follow::DfsRecordFollowed::ResendDue(record)
                    if record.shard_id == expected.shard_id
                        && record.epoch == expected.epoch
                        && record.primary_node_id == expected.primary_node_id =>
                {
                    // The same record, with the replica list the projection
                    // has now.
                    expected = record;
                    snapshot = None;
                }
                crate::service_record_follow::DfsRecordFollowed::Newer(_)
                | crate::service_record_follow::DfsRecordFollowed::ResendDue(_) => {
                    return (
                        Err(DfsError::ShardRecordChanged {
                            file_system_id: expected.file_system_id.clone(),
                            shard_id: expected.shard_id.clone(),
                            epoch: expected.epoch,
                        }),
                        false,
                    );
                }
                crate::service_record_follow::DfsRecordFollowed::Woken
                | crate::service_record_follow::DfsRecordFollowed::Ended => {
                    return (result, unanswered);
                }
            }
        }
    }

    /// One attempt at a metadata query, and whether it went undelivered.
    fn route_metadata_query_once(
        &mut self,
        expected: &DfsMetadataShardConfig,
        query: &DfsMetadataShardQuery,
        snapshot: Option<&[DfsMetadataShardConfig]>,
    ) -> (Result<PendingRoutedNamespaceResult, DfsError>, bool) {
        let mut undelivered = false;
        let result = self.route_metadata_query_attempt(expected, query, snapshot, &mut undelivered);
        (result, undelivered)
    }

    fn route_metadata_query_attempt(
        &mut self,
        expected: &DfsMetadataShardConfig,
        query: &DfsMetadataShardQuery,
        snapshot: Option<&[DfsMetadataShardConfig]>,
        undelivered: &mut bool,
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
        // Work that holds admission reads another node's shard with its
        // lanes and admission released; the re-run takes the answer the
        // yield brought.
        let lanes_released_read = (expected.primary_node_id != self.node_id
            && self.routed_read_waits_with_lanes_released(&expected.primary_node_id))
        .then(|| {
            self.routed_read_key(
                &expected.primary_node_id,
                &query.file_system_id,
                &command.operation,
            )
        })
        .transpose()?;
        let lanes_released = lanes_released_read.is_some();
        // A query adds no deadline of its own: its caller's budget, its
        // requester's departure, or the network's verdict on its primary
        // ends it.
        let previous_deadline = self.transport.operation_deadline();
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
        let mut unroutable = false;
        let result = (|| {
            let response = if let Some(key) = lanes_released_read {
                match self.answered_remote_reads.get(&key).cloned() {
                    Some(Ok(answer)) if answer.target == expected.primary_node_id => {
                        answer.response
                    }
                    Some(Ok(_)) => {
                        return Err(error("metadata query answer came from another primary"));
                    }
                    Some(Err(failed)) => return Err(failed),
                    None => {
                        let pending = self
                            .pending_routed_namespace_ops
                            .remove(&request_id)
                            .ok_or_else(|| error("metadata query lost its response fence"))?;
                        return Err(self.yield_routed_read(
                            key,
                            command.clone(),
                            pending,
                            previous_deadline,
                        ));
                    }
                }
            } else {
                if expected.primary_node_id == self.node_id {
                    self.check_mounted_call_running()?;
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
                    unroutable = true;
                    return Err(error("metadata query primary is not routable"));
                }
                let effective_deadline = self.transport.operation_deadline();
                wait_for_metadata_query_response(
                    &mut self.transport,
                    self.cancellation.as_ref(),
                    &self.node_id,
                    &expected.primary_node_id,
                    &request_id,
                    effective_deadline,
                )?
            };
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
        // A query handed to a lanes-released read is answered by that read;
        // only one this attempt sent itself can have gone undelivered.
        *undelivered = !lanes_released
            && matches!(&result, Err(failed) if unroutable
                || matches!(failed, DfsError::Network { .. } | DfsError::NetworkDelivery { .. }));
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
        self.current_object_in_fan_out(
            &mut DfsAuthorityFanOut::default(),
            file_system_id,
            object_id,
        )
    }

    /// Ask every shard authority for the current record of `object_id`,
    /// past the shards `fan_out` already left unanswered.
    pub(crate) fn current_object_in_fan_out(
        &mut self,
        fan_out: &mut DfsAuthorityFanOut,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
    ) -> Result<Option<DfsObjectMetadata>, DfsError> {
        for shard in self.metadata_shard_authorities(file_system_id)? {
            if fan_out.left_unanswered(&shard) {
                continue;
            }
            let Some(result) = self.query_metadata_authority_in_fan_out(
                fan_out,
                &shard,
                DfsMetadataSelection::CurrentObject {
                    object_id: object_id.clone(),
                },
                None,
            )?
            else {
                continue;
            };
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
        self.fan_out_undecided(fan_out, format!("object {object_id}"), None)
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
        let mut fan_out = DfsAuthorityFanOut::default();
        for shard in self.metadata_shard_authorities(&identity.file_system_id)? {
            let Some(result) = self.query_metadata_authority_in_fan_out(
                &mut fan_out,
                &shard,
                DfsMetadataSelection::RevisionMetadata {
                    identity: identity.clone(),
                },
                None,
            )?
            else {
                continue;
            };
            match result {
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
        self.fan_out_undecided(&fan_out, format!("object {}", identity.object_id), None)
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
        let mut fan_out = DfsAuthorityFanOut::default();
        for shard in self.metadata_shard_authorities(&identity.file_system_id)? {
            let Some(result) = self.query_metadata_authority_in_fan_out(
                &mut fan_out,
                &shard,
                DfsMetadataSelection::Version {
                    namespace: None,
                    allow_current,
                    identity: identity.clone(),
                },
                None,
            )?
            else {
                continue;
            };
            match result {
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
        self.fan_out_undecided(&fan_out, format!("object {}", identity.object_id), None)
    }
}

/// Transport-only wait: no backend, service dispatcher, prepare or repair graph.
///
/// The answer's arrival, the caller's cancellation or the caller's deadline
/// ends the wait. Unrelated queued traffic keeps its custody: the wait takes
/// only this request's answer and wakes on arrivals, never on what is already
/// queued, so it cannot spin. Without an arrival it looks again at the
/// network's liveness interval; that look ends nothing.
fn wait_for_metadata_query_response(
    transport: &mut dyn DfsMessageTransport,
    cancellation: Option<&DfsCancellationFlag>,
    local: &sinomo_core::node::NodeId,
    primary: &sinomo_core::node::NodeId,
    request_id: &str,
    deadline: Option<std::time::Instant>,
) -> Result<DfsNamespaceResponse, DfsError> {
    let arrived = std::sync::Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let signal = std::sync::Arc::clone(&arrived);
    let waker: crate::service_state::DfsCancellationWaker = std::sync::Arc::new(move || {
        *signal
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        signal.1.notify_all();
    });
    let _cancellation_wake = cancellation.map(|cancellation| cancellation.register_waker(&waker));
    transport.set_incoming_arrival_waker(Some(std::sync::Arc::clone(&waker)));
    let answer = (|| loop {
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
        let now = std::time::Instant::now();
        if deadline.is_some_and(|deadline| now >= deadline) {
            return Err(error("metadata query timed out waiting for its authority"));
        }
        let wait = deadline.map_or(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT, |deadline| {
            deadline
                .saturating_duration_since(now)
                .min(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
        });
        let flag = arrived
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (mut flag, _) = arrived
            .1
            .wait_timeout_while(flag, wait, |arrived| !*arrived)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *flag = false;
    })();
    transport.set_incoming_arrival_waker(None);
    answer
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
        // The client keeps no store; the node's durable metadata must stay
        // unchanged by the query.
        let node = service.node_id().clone();
        let before = runtime
            .node_metadata(&node)
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
        let after = runtime
            .node_metadata(&node)
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
