//! Durable semantic projection of one metadata shard's namespace journal.
//!
//! The feed exposes visible namespace effects and committed revision-metadata
//! changes. Internal prepares, receipts, repair records, and write intents remain journal
//! implementation details. A cursor is fenced to one filesystem, shard, and
//! authority epoch; callers must rescan their subtree instead of carrying a
//! cursor across an authority change or a missing journal prefix.

use serde::{Deserialize, Serialize};
use sinomo_core::events::EventId;
use time::OffsetDateTime;

use crate::control_plane::{DfsMetadataShardAssignment, DfsMetadataShardConfig};
use crate::metadata_api::DfsMetadataBackend;
use crate::metadata_error::Result;
use crate::metadata_records::{
    DfsMetadataJournalBatch, DfsMetadataJournalEntry, DfsMetadataMutation, DfsMetadataShardId,
    DfsMetadataSubscription, DfsNamespaceTransaction,
};
use crate::model::{DfsObjectKind, DfsObjectMetadata, DfsTombstoneProvenance};
use crate::namespace::{
    namespace_transaction_id_for_path, namespace_transaction_id_for_versioned_path,
};
use crate::posix_path::posix_path_is_at_or_under_prefix;
use crate::{DfsError, DfsFileSystemId, DfsNodeId, DfsObjectId, DfsVersion};

/// Restart-safe position in one authoritative metadata-shard journal.
///
/// `after_sequence` names the last completely projected journal entry. It is
/// advanced even when an entry has no visible namespace effect, so retries do
/// not re-expose internal mutations. A cursor is invalid after either its
/// shard assignment or authority epoch changes.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsNamespaceChangeCursor {
    pub file_system_id: DfsFileSystemId,
    pub shard_id: DfsMetadataShardId,
    pub authority_epoch: u64,
    pub after_sequence: u64,
}

impl DfsNamespaceChangeCursor {
    /// Create a cursor at an already snapshotted journal boundary.
    pub fn from_shard_config(shard: &DfsMetadataShardConfig, after_sequence: u64) -> Self {
        Self {
            file_system_id: shard.file_system_id.clone(),
            shard_id: DfsMetadataShardId::new(shard.shard_id.clone()),
            authority_epoch: shard.epoch,
            after_sequence,
        }
    }
}

/// Stable evidence tying one semantic event to its durable journal position.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsNamespaceChangeProvenance {
    /// Deterministic identity derived from shard, sequence, and event ordinal.
    pub event_id: String,
    pub shard_id: DfsMetadataShardId,
    pub authority_epoch: u64,
    pub journal_sequence: u64,
    pub journal_ordinal: u64,
    #[serde(with = "time::serde::rfc3339")]
    pub journal_created_at: OffsetDateTime,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transaction_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requester_node_id: Option<DfsNodeId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by_event_id: Option<EventId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_modified_by_event_id: Option<EventId>,
}

/// Why a consumer must rebuild its subtree snapshot before polling again.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsNamespaceRescanReason {
    FileSystemChanged,
    ShardChanged,
    AuthorityEpochChanged,
    JournalCompactionGap,
    JournalRewound,
    PayloadUnavailable,
    CrossShardRenameCorrelationUnavailable,
    NonAtomicRenameProjectionUnavailable,
}

/// Public namespace effects projected from durable metadata mutations.
///
/// `RescanRequired` is terminal for its batch: the returned cursor is left at
/// its input position and no earlier event from that batch is returned.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum DfsNamespaceChangeEvent {
    Upsert {
        path: String,
        object_id: DfsObjectId,
        version: DfsVersion,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content_hash: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content_size_bytes: Option<u64>,
        kind: DfsObjectKind,
        provenance: DfsNamespaceChangeProvenance,
    },
    Remove {
        path: String,
        object_id: DfsObjectId,
        version: DfsVersion,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content_hash: Option<String>,
        provenance: DfsNamespaceChangeProvenance,
    },
    Rename {
        from_path: String,
        to_path: String,
        object_id: DfsObjectId,
        version: DfsVersion,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content_hash: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content_size_bytes: Option<u64>,
        kind: DfsObjectKind,
        provenance: DfsNamespaceChangeProvenance,
    },
    /// Shard-scoped identity notification. A consumer resolves current scope
    /// and authority before inspecting metadata or bytes; this event grants neither.
    RevisionMetadataChanged {
        object_id: DfsObjectId,
        version: DfsVersion,
        provenance: DfsNamespaceChangeProvenance,
    },
    RescanRequired {
        reason: DfsNamespaceRescanReason,
        detail: String,
    },
}

/// One ordered shard-local namespace change batch.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsNamespaceChangeBatch {
    /// Last complete journal boundary safe to persist for the next poll.
    pub next_cursor: DfsNamespaceChangeCursor,
    pub events: Vec<DfsNamespaceChangeEvent>,
    /// Authority journal head observed by the poller.
    pub journal_head_sequence: u64,
    /// Whether another immediate poll may return a later retained entry.
    pub has_more: bool,
}

impl DfsNamespaceChangeBatch {
    pub fn rescan_required(&self) -> bool {
        matches!(
            self.events.as_slice(),
            [DfsNamespaceChangeEvent::RescanRequired { .. }]
        )
    }

    pub(crate) fn require_rescan(
        cursor: &DfsNamespaceChangeCursor,
        journal_head_sequence: u64,
        reason: DfsNamespaceRescanReason,
        detail: impl Into<String>,
    ) -> Self {
        rescan_batch(cursor, journal_head_sequence, reason, detail.into())
    }
}

/// Resolve every current metadata shard whose assignment can contain an
/// object in `root_path`'s subtree.
///
/// The result is deliberately conservative. A root assignment and every
/// non-empty name-hash assignment may contain descendants at arbitrary paths;
/// path-prefix assignments intersect when either prefix contains the other.
/// Results are shard-local and sorted by shard id, never a cross-shard order.
pub fn dfs_namespace_change_shards_for_subtree(
    file_system_id: &DfsFileSystemId,
    root_path: &str,
    shards: &[DfsMetadataShardConfig],
) -> std::result::Result<Vec<DfsMetadataShardConfig>, DfsError> {
    let root_path = crate::posix_path::normalize_posix_path(root_path)
        .map_err(dfs_error_from_namespace_path)?;
    let mut selected = std::collections::BTreeMap::new();
    for shard in shards {
        if &shard.file_system_id != file_system_id {
            continue;
        }
        let intersects = match &shard.assignment {
            DfsMetadataShardAssignment::Root => true,
            DfsMetadataShardAssignment::PathPrefix { prefix } => {
                let prefix = crate::posix_path::normalize_posix_path(prefix)
                    .map_err(dfs_error_from_namespace_path)?;
                posix_path_is_at_or_under_prefix(&root_path, &prefix)
                    || posix_path_is_at_or_under_prefix(&prefix, &root_path)
            }
            // Equal endpoints denote the full hash ring. Every other range is
            // non-empty and a descendant name can hash into it.
            DfsMetadataShardAssignment::NameHashRange { .. } => true,
        };
        if !intersects {
            continue;
        }
        match selected.entry(shard.shard_id.clone()) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(shard.clone());
            }
            std::collections::btree_map::Entry::Occupied(entry) if entry.get() == shard => {}
            std::collections::btree_map::Entry::Occupied(entry) => {
                return Err(DfsError::Distributed {
                    message: format!(
                        "metadata shard {} has conflicting current configurations while resolving namespace change subtree {root_path}: {:?} and {:?}",
                        shard.shard_id,
                        entry.get(),
                        shard
                    ),
                });
            }
        }
    }
    Ok(selected.into_values().collect())
}

fn dfs_error_from_namespace_path(error: crate::DfsPosixError) -> DfsError {
    match error {
        crate::DfsPosixError::Dfs(error) => error,
        other => DfsError::Distributed {
            message: other.to_string(),
        },
    }
}

#[derive(Clone, Debug, Default)]
struct RequestProvenance {
    operation_id: Option<String>,
    requester_node_id: Option<DfsNodeId>,
    request_id: Option<String>,
}

struct NamespaceChangeProjector<'a> {
    backend: Option<&'a dyn DfsMetadataBackend>,
    cursor: &'a DfsNamespaceChangeCursor,
    root_path: &'a str,
    batch: &'a DfsMetadataJournalBatch,
    prepare_lookback: std::cell::RefCell<Option<Vec<DfsMetadataJournalEntry>>>,
}

enum ProjectedEntry {
    Events(Vec<DfsNamespaceChangeEvent>),
    Rescan {
        reason: DfsNamespaceRescanReason,
        detail: String,
    },
}

/// Project a payload-complete journal batch without consulting mutable
/// metadata tables.
///
/// This is suitable for routed callers that already obtained exact journal
/// bodies. If a compact entry references a body outside the batch, projection
/// fails closed with `RescanRequired`; [`poll_dfs_namespace_changes`] can
/// resolve those bodies from the shard-local durable backend.
pub fn project_dfs_namespace_change_batch(
    cursor: &DfsNamespaceChangeCursor,
    shard: &DfsMetadataShardConfig,
    root_path: &str,
    batch: &DfsMetadataJournalBatch,
) -> DfsNamespaceChangeBatch {
    project_namespace_change_batch(
        cursor,
        shard,
        root_path,
        batch,
        None,
        batch.next_after_sequence,
    )
    .unwrap_or_else(|error| {
        rescan_batch(
            cursor,
            batch.next_after_sequence,
            DfsNamespaceRescanReason::PayloadUnavailable,
            format!("could not project namespace journal payload: {error}"),
        )
    })
}

/// Poll and semantically project one local metadata shard.
///
/// The caller supplies the currently authoritative shard configuration. This
/// bounded seam validates the persisted cursor against the supplied current
/// authority configuration before reading exactly the cursor's shard. Network
/// routing and primary-role validation belong above this R1 metadata boundary.
pub fn poll_dfs_namespace_changes(
    backend: &dyn DfsMetadataBackend,
    cursor: &DfsNamespaceChangeCursor,
    shard: &DfsMetadataShardConfig,
    root_path: &str,
) -> Result<DfsNamespaceChangeBatch> {
    if let Some(batch) = cursor_fence_rescan(cursor, shard, 0) {
        return Ok(batch);
    }

    // The prefix proof and subscription are both exact-shard trait methods.
    // This is important for mounted sharded backends: asking for global shard
    // statuses would cold-open every configured metadata shard merely to poll
    // one cursor.
    let Some(prefix_proof) = backend.metadata_prefix_proof(&cursor.shard_id)? else {
        return Ok(rescan_batch(
            cursor,
            cursor.after_sequence,
            DfsNamespaceRescanReason::PayloadUnavailable,
            format!(
                "namespace change-feed shard {} has no exact durable prefix proof",
                cursor.shard_id.0
            ),
        ));
    };
    let observed_head = prefix_proof.journal_sequence;
    if observed_head < cursor.after_sequence {
        return Ok(rescan_batch(
            cursor,
            observed_head,
            DfsNamespaceRescanReason::JournalRewound,
            format!(
                "resident journal head {observed_head} is behind cursor {}",
                cursor.after_sequence
            ),
        ));
    }
    let mut subscription =
        DfsMetadataSubscription::new(cursor.shard_id.clone(), cursor.after_sequence);
    let batch = backend.poll_subscription(&mut subscription)?;
    project_namespace_change_batch(
        cursor,
        shard,
        root_path,
        &batch,
        Some(backend),
        observed_head,
    )
}

fn project_namespace_change_batch(
    cursor: &DfsNamespaceChangeCursor,
    shard: &DfsMetadataShardConfig,
    root_path: &str,
    batch: &DfsMetadataJournalBatch,
    backend: Option<&dyn DfsMetadataBackend>,
    journal_head_sequence: u64,
) -> Result<DfsNamespaceChangeBatch> {
    if let Some(batch) = cursor_fence_rescan(cursor, shard, journal_head_sequence) {
        return Ok(batch);
    }
    if batch.shard_id != cursor.shard_id {
        return Ok(rescan_batch(
            cursor,
            journal_head_sequence,
            DfsNamespaceRescanReason::ShardChanged,
            format!(
                "journal batch shard {} does not match cursor shard {}",
                batch.shard_id.0, cursor.shard_id.0
            ),
        ));
    }
    if batch.next_after_sequence < cursor.after_sequence {
        return Ok(rescan_batch(
            cursor,
            journal_head_sequence,
            DfsNamespaceRescanReason::JournalRewound,
            format!(
                "journal batch boundary {} is behind cursor {}",
                batch.next_after_sequence, cursor.after_sequence
            ),
        ));
    }

    let root_path = crate::metadata_keys::normalize_posix_path(root_path);
    let projector = NamespaceChangeProjector {
        backend,
        cursor,
        root_path: &root_path,
        batch,
        prepare_lookback: Default::default(),
    };
    let mut events = Vec::new();
    let mut expected_sequence = cursor.after_sequence.saturating_add(1);
    let mut previous_entry: Option<&DfsMetadataJournalEntry> = None;

    for entry in &batch.entries {
        if entry.shard_id != cursor.shard_id {
            return Ok(rescan_batch(
                cursor,
                journal_head_sequence,
                DfsNamespaceRescanReason::ShardChanged,
                format!(
                    "journal entry {} belongs to shard {}, expected {}",
                    entry.sequence, entry.shard_id.0, cursor.shard_id.0
                ),
            ));
        }

        if let Some(previous) = previous_entry {
            if entry.sequence < previous.sequence {
                return Ok(rescan_batch(
                    cursor,
                    journal_head_sequence,
                    DfsNamespaceRescanReason::JournalRewound,
                    format!(
                        "journal sequence moved backward from {} to {}",
                        previous.sequence, entry.sequence
                    ),
                ));
            }
            if entry.sequence == previous.sequence {
                if entry != previous {
                    return Ok(rescan_batch(
                        cursor,
                        journal_head_sequence,
                        DfsNamespaceRescanReason::JournalCompactionGap,
                        format!(
                            "journal sequence {} appeared with conflicting payloads",
                            entry.sequence
                        ),
                    ));
                }
                continue;
            }
        }
        previous_entry = Some(entry);

        if entry.sequence <= cursor.after_sequence {
            continue;
        }
        if entry.sequence != expected_sequence {
            return Ok(rescan_batch(
                cursor,
                journal_head_sequence,
                DfsNamespaceRescanReason::JournalCompactionGap,
                format!(
                    "journal sequence {} followed cursor {}, expected {}",
                    entry.sequence, cursor.after_sequence, expected_sequence
                ),
            ));
        }

        match projector.project_entry(entry)? {
            ProjectedEntry::Events(mut projected) => events.append(&mut projected),
            ProjectedEntry::Rescan { reason, detail } => {
                return Ok(rescan_batch(cursor, journal_head_sequence, reason, detail));
            }
        }
        expected_sequence = expected_sequence.saturating_add(1);
    }

    let processed_through = expected_sequence.saturating_sub(1);
    if batch.next_after_sequence != processed_through {
        let (reason, relation) = if batch.next_after_sequence < processed_through {
            (DfsNamespaceRescanReason::JournalRewound, "behind")
        } else {
            (DfsNamespaceRescanReason::JournalCompactionGap, "ahead of")
        };
        return Ok(rescan_batch(
            cursor,
            journal_head_sequence,
            reason,
            format!(
                "journal batch boundary {} is {relation} projected boundary {processed_through}",
                batch.next_after_sequence
            ),
        ));
    }
    if processed_through < journal_head_sequence && batch.entries.is_empty() {
        return Ok(rescan_batch(
            cursor,
            journal_head_sequence,
            DfsNamespaceRescanReason::JournalCompactionGap,
            format!(
                "journal head {} is ahead of cursor {} but no retained entry was returned",
                journal_head_sequence, cursor.after_sequence
            ),
        ));
    }

    let mut next_cursor = cursor.clone();
    next_cursor.after_sequence = processed_through;
    Ok(DfsNamespaceChangeBatch {
        has_more: processed_through < journal_head_sequence,
        next_cursor,
        events,
        journal_head_sequence,
    })
}

impl NamespaceChangeProjector<'_> {
    fn project_entry(&self, entry: &DfsMetadataJournalEntry) -> Result<ProjectedEntry> {
        let mutation_file_system_id = entry.mutation.file_system_id();
        if mutation_file_system_id != &self.cursor.file_system_id {
            return Ok(ProjectedEntry::Events(Vec::new()));
        }

        match &entry.mutation {
            DfsMetadataMutation::PrepareOperationV7 { .. } => {
                Ok(ProjectedEntry::Events(Vec::new()))
            }
            DfsMetadataMutation::CommitPreparedOperationV7 { digest, .. } => {
                let Some(payload) = self.resolve_prepared_operation(digest, entry.sequence)? else {
                    return Ok(payload_rescan(entry, "committed V7 body is unavailable"));
                };
                if digest.shard_id != self.cursor.shard_id
                    || digest.file_system_id != self.cursor.file_system_id
                    || digest.prepare_sequence >= entry.sequence
                    || digest.validate_payload(&payload).is_err()
                {
                    return Ok(payload_rescan(
                        entry,
                        "V7 commit identity differs from its actual prepared body",
                    ));
                }
                let request = if payload.completed_request.is_none()
                    && payload.completed_request_completion.is_none()
                {
                    payload
                        .terminal_intent
                        .as_ref()
                        .and_then(|intent| intent.operation_id.clone())
                        .map(|operation_id| RequestProvenance {
                            operation_id: Some(operation_id),
                            ..Default::default()
                        })
                        .unwrap_or_else(|| request_provenance(&payload))
                } else {
                    request_provenance(&payload)
                };
                self.project_payload(entry, &payload, request)
            }
        }
    }

    fn resolve_prepared_operation(
        &self,
        digest: &crate::DfsPreparedOperationDigest,
        before_sequence: u64,
    ) -> Result<Option<std::sync::Arc<crate::DfsPreparedOperationPayload>>> {
        let find = |entries: &[DfsMetadataJournalEntry]| {
            entries.iter().find_map(|entry| {
                if entry.shard_id != digest.shard_id
                    || entry.sequence != digest.prepare_sequence
                    || entry.sequence >= before_sequence
                {
                    return None;
                }
                match &entry.mutation {
                    DfsMetadataMutation::PrepareOperationV7 { record, .. }
                        if &record.digest == digest =>
                    {
                        Some(std::sync::Arc::clone(&record.payload))
                    }
                    _ => None,
                }
            })
        };
        if let Some(payload) = find(&self.batch.entries) {
            return Ok(Some(payload));
        }
        let Some(backend) = self.backend else {
            return Ok(None);
        };
        if backend
            .prepared_operation_digest(&digest.operation_id)?
            .as_ref()
            == Some(digest)
            && let Some(payload) = backend.prepared_operation_payload(&digest.operation_id)?
        {
            return Ok(Some(payload));
        }
        if self.prepare_lookback.borrow().is_none() {
            let prior = backend.journal_batch_for_shard_since_limited(
                &self.cursor.shard_id,
                self.cursor.after_sequence.saturating_sub(128),
                128,
                8 * 1024 * 1024,
            )?;
            *self.prepare_lookback.borrow_mut() =
                Some(prior.map(|batch| batch.entries).unwrap_or_default());
        }
        Ok(find(
            self.prepare_lookback
                .borrow()
                .as_ref()
                .expect("initialized lookback"),
        ))
    }

    fn project_payload(
        &self,
        entry: &DfsMetadataJournalEntry,
        payload: &crate::DfsPreparedOperationPayload,
        request: RequestProvenance,
    ) -> Result<ProjectedEntry> {
        use crate::DfsPreparedMetadataEffect as Effect;
        let mut transaction = payload.transaction.clone();
        let mut revisions = std::collections::BTreeSet::new();
        // The committed body is atomic. Project each affected path's final
        // row, retaining the native transaction order needed for rename pairing.
        for effect in &payload.metadata_effects {
            match effect {
                Effect::PutObject { object }
                | Effect::FinalizeCrossShardSourceEviction {
                    tombstone: object, ..
                } => {
                    if let Some(row) = transaction
                        .objects
                        .iter_mut()
                        .find(|row| row.object_id == object.object_id)
                    {
                        *row = object.clone();
                    } else {
                        transaction.objects.push(object.clone());
                    }
                }
                Effect::PutVersion { version } => {
                    if let Some(row) = transaction.versions.iter_mut().find(|row| {
                        row.object_id == version.object_id && row.version == version.version
                    }) {
                        *row = version.clone();
                    } else {
                        transaction.versions.push(version.clone());
                    }
                    revisions.insert((version.object_id.clone(), version.version.clone()));
                }
                Effect::ReconcileCrossShardPinTransfer { versions, .. } => {
                    for version in versions {
                        revisions.insert((version.object_id.clone(), version.version.clone()));
                    }
                }
                Effect::PutVersionPin { pin } => {
                    revisions.insert((pin.object_id.clone(), pin.version.clone()));
                }
                Effect::RemoveVersion { object_id, version }
                | Effect::RemoveVersionPin {
                    object_id, version, ..
                } => {
                    revisions.insert((object_id.clone(), version.clone()));
                }
                // These are hidden custody, scheduling or bookkeeping rows;
                // they do not publish an alternative visible namespace state.
                Effect::PutFileSystem { .. }
                | Effect::RemoveCrossShardPinTransfer { .. }
                | Effect::PutReceipt { .. }
                | Effect::RemoveReceipt { .. }
                | Effect::WithdrawReceipt { .. }
                | Effect::PutIntent { .. }
                | Effect::RemoveRetainedRepairOwner { .. }
                | Effect::PutIntentReceipt { .. }
                | Effect::RemoveIntentReceipt { .. }
                | Effect::MergeIntentReceiptRetention { .. }
                | Effect::PutAdvisoryLockState { .. }
                | Effect::PutNamespaceTransactionReceipt { .. }
                | Effect::RemoveNamespaceTransactionReceipt { .. }
                | Effect::PutNamespaceJob { .. }
                | Effect::PutNamespaceJobReceipt { .. }
                | Effect::RemoveNamespaceJobReceipt { .. }
                | Effect::PutNamespaceRequest { .. }
                | Effect::PutNamespaceRequestReceipt { .. }
                | Effect::RemoveNamespaceRequestReceipt { .. }
                | Effect::MergeContentReferenceOutcomeRetention { .. }
                | Effect::RemovePreparedOperation { .. }
                | Effect::ResolvePreparedOperation { .. } => {}
            }
        }
        let mut events = match self.project_transaction(entry, &transaction, request.clone())? {
            ProjectedEntry::Events(events) => events,
            rescan @ ProjectedEntry::Rescan { .. } => return Ok(rescan),
        };
        let mut ordinal = transaction.objects.len() as u64;
        for event in &events {
            let provenance = match event {
                DfsNamespaceChangeEvent::Upsert { provenance, .. }
                | DfsNamespaceChangeEvent::Remove { provenance, .. }
                | DfsNamespaceChangeEvent::Rename { provenance, .. }
                | DfsNamespaceChangeEvent::RevisionMetadataChanged { provenance, .. } => provenance,
                DfsNamespaceChangeEvent::RescanRequired { .. } => continue,
            };
            ordinal = ordinal.max(provenance.journal_ordinal.saturating_add(1));
        }
        for (object_id, version) in revisions {
            if events.iter().any(|event| matches!(event,
                DfsNamespaceChangeEvent::RevisionMetadataChanged { object_id: id, version: revision, .. }
                if id == &object_id && revision == &version)) {
                continue;
            }
            events.push(DfsNamespaceChangeEvent::RevisionMetadataChanged {
                object_id,
                version,
                provenance: DfsNamespaceChangeProvenance {
                    event_id: format!(
                        "dfs-namespace-change:{}:{}:{}:{}:{ordinal}",
                        self.cursor.file_system_id,
                        entry.shard_id.0,
                        self.cursor.authority_epoch,
                        entry.sequence
                    ),
                    shard_id: entry.shard_id.clone(),
                    authority_epoch: self.cursor.authority_epoch,
                    journal_sequence: entry.sequence,
                    journal_ordinal: ordinal,
                    journal_created_at: entry.created_at,
                    transaction_id: Some(transaction.transaction_id.clone()),
                    operation_id: request.operation_id.clone(),
                    requester_node_id: request.requester_node_id.clone(),
                    request_id: request.request_id.clone(),
                    created_by_event_id: None,
                    last_modified_by_event_id: None,
                },
            });
            ordinal = ordinal.saturating_add(1);
        }
        Ok(ProjectedEntry::Events(events))
    }

    fn project_transaction(
        &self,
        entry: &DfsMetadataJournalEntry,
        transaction: &DfsNamespaceTransaction,
        request: RequestProvenance,
    ) -> Result<ProjectedEntry> {
        if transaction
            .transaction_id
            .starts_with("namespace_rename-child-batch_")
        {
            return Ok(ProjectedEntry::Rescan {
                reason: DfsNamespaceRescanReason::NonAtomicRenameProjectionUnavailable,
                detail: format!(
                    "directory rename transaction {} does not retain each child's prior path",
                    transaction.transaction_id
                ),
            });
        }

        if let Some((old_index, moved_index)) = same_shard_rename_indices(transaction) {
            let old_path = &transaction.objects[old_index].path;
            let moved = &transaction.objects[moved_index];
            let mut events = Vec::new();
            if path_is_relevant(old_path, self.root_path)
                || path_is_relevant(&moved.path, self.root_path)
            {
                events.push(DfsNamespaceChangeEvent::Rename {
                    from_path: old_path.clone(),
                    to_path: moved.path.clone(),
                    object_id: moved.object_id.clone(),
                    version: moved.version.clone(),
                    content_hash: moved.content_hash.clone(),
                    content_size_bytes: moved.content_size_bytes,
                    kind: moved.kind.clone(),
                    provenance: event_provenance(
                        self.cursor,
                        entry,
                        moved_index as u64,
                        Some(transaction.transaction_id.clone()),
                        &request,
                        moved,
                    ),
                });
            }
            for (index, object) in transaction.objects.iter().enumerate() {
                if index == old_index || index == moved_index {
                    continue;
                }
                // Replacing an existing destination is part of the atomic
                // rename and must not leak as a separate remove after the
                // moved object's canonical rename event.
                if object.kind == DfsObjectKind::Tombstone && object.path == moved.path {
                    continue;
                }
                match self.project_object(
                    entry,
                    index as u64,
                    object,
                    Some(transaction.transaction_id.clone()),
                    request.clone(),
                )? {
                    ProjectedEntry::Events(mut projected) => events.append(&mut projected),
                    rescan @ ProjectedEntry::Rescan { .. } => return Ok(rescan),
                }
            }
            events.extend(self.project_revision_metadata_changes(entry, transaction, &request));
            return Ok(ProjectedEntry::Events(events));
        }

        if transaction.transaction_id.starts_with("namespace_rename_") {
            return Ok(ProjectedEntry::Rescan {
                reason: DfsNamespaceRescanReason::NonAtomicRenameProjectionUnavailable,
                detail: format!(
                    "rename transaction {} does not contain a canonical old/new path pair",
                    transaction.transaction_id
                ),
            });
        }

        let mut events = Vec::new();
        for (index, object) in transaction.objects.iter().enumerate() {
            match self.project_object(
                entry,
                index as u64,
                object,
                Some(transaction.transaction_id.clone()),
                request.clone(),
            )? {
                ProjectedEntry::Events(mut projected) => events.append(&mut projected),
                rescan @ ProjectedEntry::Rescan { .. } => return Ok(rescan),
            }
        }
        events.extend(self.project_revision_metadata_changes(entry, transaction, &request));
        Ok(ProjectedEntry::Events(events))
    }

    fn project_revision_metadata_changes(
        &self,
        entry: &DfsMetadataJournalEntry,
        transaction: &DfsNamespaceTransaction,
        request: &RequestProvenance,
    ) -> Vec<DfsNamespaceChangeEvent> {
        // Appending after namespace ordinals keeps existing path event IDs
        // stable. Sorting and deduplicating identities makes a pin plus a row
        // update one notification, including the final reference release.
        let mut revisions = std::collections::BTreeMap::new();
        for version in &transaction.versions {
            let previous_data_changed =
                transaction
                    .extension_expectations
                    .iter()
                    .any(|expectation| {
                        expectation.object_id == version.object_id
                            && expectation.version == version.version
                            && expectation.expected.as_ref().is_some_and(|previous| {
                                (!previous.retention.is_empty()
                                    || !previous.module_metadata.is_empty())
                                    && (previous.retention != version.retention
                                        || previous.module_metadata != version.module_metadata)
                            })
                    });
            let semantic_metadata = !version.retention.is_empty()
                || !version.module_metadata.is_empty()
                || previous_data_changed
                || transaction.contribution_digest.is_some();
            if version.file_system_id == self.cursor.file_system_id && semantic_metadata {
                revisions.insert(
                    (version.object_id.clone(), version.version.clone()),
                    version.created_by_event_id.clone(),
                );
            }
        }
        if let Some(references) = &transaction.file_version_pins {
            for reference in references.add.iter().chain(&references.remove) {
                if reference.file_system_id == self.cursor.file_system_id {
                    revisions
                        .entry((reference.object_id.clone(), reference.version.clone()))
                        .or_insert(None);
                }
            }
        }
        revisions
            .into_iter()
            .enumerate()
            .map(|(index, ((object_id, version), created_by_event_id))| {
                let ordinal = transaction.objects.len() as u64 + index as u64;
                DfsNamespaceChangeEvent::RevisionMetadataChanged {
                    object_id,
                    version,
                    provenance: DfsNamespaceChangeProvenance {
                        event_id: format!(
                            "dfs-namespace-change:{}:{}:{}:{}:{ordinal}",
                            self.cursor.file_system_id,
                            entry.shard_id.0,
                            self.cursor.authority_epoch,
                            entry.sequence
                        ),
                        shard_id: entry.shard_id.clone(),
                        authority_epoch: self.cursor.authority_epoch,
                        journal_sequence: entry.sequence,
                        journal_ordinal: ordinal,
                        journal_created_at: entry.created_at,
                        transaction_id: Some(transaction.transaction_id.clone()),
                        operation_id: request.operation_id.clone(),
                        requester_node_id: request.requester_node_id.clone(),
                        request_id: request.request_id.clone(),
                        created_by_event_id,
                        last_modified_by_event_id: None,
                    },
                }
            })
            .collect()
    }

    fn project_object(
        &self,
        entry: &DfsMetadataJournalEntry,
        ordinal: u64,
        object: &DfsObjectMetadata,
        transaction_id: Option<String>,
        request: RequestProvenance,
    ) -> Result<ProjectedEntry> {
        if object.file_system_id != self.cursor.file_system_id
            || !path_is_relevant(&object.path, self.root_path)
        {
            return Ok(ProjectedEntry::Events(Vec::new()));
        }
        if matches!(
            object.tombstone_provenance,
            Some(DfsTombstoneProvenance::SourceEviction { .. })
        ) {
            return Ok(ProjectedEntry::Rescan {
                reason: DfsNamespaceRescanReason::CrossShardRenameCorrelationUnavailable,
                detail: format!(
                    "source-shard eviction for object {} at {} has no durable target-path correlation",
                    object.object_id, object.path
                ),
            });
        }

        let provenance = event_provenance(
            self.cursor,
            entry,
            ordinal,
            transaction_id,
            &request,
            object,
        );
        let event = if object.kind == DfsObjectKind::Tombstone {
            DfsNamespaceChangeEvent::Remove {
                path: object.path.clone(),
                object_id: object.object_id.clone(),
                version: object.version.clone(),
                content_hash: object.content_hash.clone(),
                provenance,
            }
        } else {
            DfsNamespaceChangeEvent::Upsert {
                path: object.path.clone(),
                object_id: object.object_id.clone(),
                version: object.version.clone(),
                content_hash: object.content_hash.clone(),
                content_size_bytes: object.content_size_bytes,
                kind: object.kind.clone(),
                provenance,
            }
        };
        Ok(ProjectedEntry::Events(vec![event]))
    }
}

fn same_shard_rename_indices(transaction: &DfsNamespaceTransaction) -> Option<(usize, usize)> {
    let old = transaction.objects.first()?;
    if old.kind != DfsObjectKind::Tombstone {
        return None;
    }
    let mut candidates = transaction
        .objects
        .iter()
        .enumerate()
        .skip(1)
        .filter(|(_, object)| {
            object.kind != DfsObjectKind::Tombstone
                && (transaction.transaction_id
                    == namespace_transaction_id_for_versioned_path(
                        "rename",
                        &object.object_id,
                        &object.version,
                        &object.path,
                    )
                    || transaction.transaction_id
                        == namespace_transaction_id_for_path(
                            "rename",
                            &object.object_id,
                            &object.path,
                        ))
        });
    let (moved_index, moved) = candidates.next()?;
    if candidates.next().is_some() || old.path == moved.path {
        return None;
    }
    Some((0, moved_index))
}

fn request_provenance(prepare: &crate::DfsPreparedOperationPayload) -> RequestProvenance {
    if let Some(request) = prepare.completed_request.as_ref() {
        return RequestProvenance {
            // `request_id` is the caller-owned stable operation identity.
            // `operation_id` is an internal requester-qualified prepare key
            // and must never leak into suppression-facing feed provenance.
            operation_id: Some(request.request_id.clone()),
            requester_node_id: Some(request.requester_node_id.clone()),
            request_id: Some(request.request_id.clone()),
        };
    }
    if let Some(completion) = prepare.completed_request_completion.as_ref() {
        return RequestProvenance {
            operation_id: Some(completion.request_id.clone()),
            requester_node_id: Some(completion.requester_node_id.clone()),
            request_id: Some(completion.request_id.clone()),
        };
    }
    RequestProvenance {
        operation_id: Some(
            prepare
                .journal_operation_id
                .as_ref()
                .unwrap_or(&prepare.operation_id)
                .clone(),
        ),
        ..RequestProvenance::default()
    }
}

fn event_provenance(
    cursor: &DfsNamespaceChangeCursor,
    entry: &DfsMetadataJournalEntry,
    ordinal: u64,
    transaction_id: Option<String>,
    request: &RequestProvenance,
    object: &DfsObjectMetadata,
) -> DfsNamespaceChangeProvenance {
    DfsNamespaceChangeProvenance {
        event_id: format!(
            "dfs-namespace-change:{}:{}:{}:{}:{ordinal}",
            cursor.file_system_id, entry.shard_id.0, cursor.authority_epoch, entry.sequence
        ),
        shard_id: entry.shard_id.clone(),
        authority_epoch: cursor.authority_epoch,
        journal_sequence: entry.sequence,
        journal_ordinal: ordinal,
        journal_created_at: entry.created_at,
        transaction_id,
        operation_id: request.operation_id.clone(),
        requester_node_id: request.requester_node_id.clone(),
        request_id: request.request_id.clone(),
        created_by_event_id: object.created_by_event_id.clone(),
        last_modified_by_event_id: object.last_modified_by_event_id.clone(),
    }
}

fn path_is_relevant(path: &str, root_path: &str) -> bool {
    posix_path_is_at_or_under_prefix(path, root_path)
}

fn payload_rescan(entry: &DfsMetadataJournalEntry, detail: impl Into<String>) -> ProjectedEntry {
    ProjectedEntry::Rescan {
        reason: DfsNamespaceRescanReason::PayloadUnavailable,
        detail: format!(
            "journal entry {} on shard {}: {}",
            entry.sequence,
            entry.shard_id.0,
            detail.into()
        ),
    }
}

fn cursor_fence_rescan(
    cursor: &DfsNamespaceChangeCursor,
    shard: &DfsMetadataShardConfig,
    journal_head_sequence: u64,
) -> Option<DfsNamespaceChangeBatch> {
    if cursor.file_system_id != shard.file_system_id {
        return Some(rescan_batch(
            cursor,
            journal_head_sequence,
            DfsNamespaceRescanReason::FileSystemChanged,
            format!(
                "cursor filesystem {} does not match configured filesystem {}",
                cursor.file_system_id, shard.file_system_id
            ),
        ));
    }
    if cursor.shard_id.0 != shard.shard_id {
        return Some(rescan_batch(
            cursor,
            journal_head_sequence,
            DfsNamespaceRescanReason::ShardChanged,
            format!(
                "cursor shard {} does not match configured shard {}",
                cursor.shard_id.0, shard.shard_id
            ),
        ));
    }
    if cursor.authority_epoch != shard.epoch {
        return Some(rescan_batch(
            cursor,
            journal_head_sequence,
            DfsNamespaceRescanReason::AuthorityEpochChanged,
            format!(
                "cursor authority epoch {} does not match configured epoch {}",
                cursor.authority_epoch, shard.epoch
            ),
        ));
    }
    None
}

fn rescan_batch(
    cursor: &DfsNamespaceChangeCursor,
    journal_head_sequence: u64,
    reason: DfsNamespaceRescanReason,
    detail: String,
) -> DfsNamespaceChangeBatch {
    DfsNamespaceChangeBatch {
        next_cursor: cursor.clone(),
        events: vec![DfsNamespaceChangeEvent::RescanRequired { reason, detail }],
        journal_head_sequence,
        has_more: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::DfsObjectPolicy;
    use sinomo_core::node::NodeId;
    use sinomo_core::object::{LengthMode, serde_ext as sinomo_object_serde};

    use crate::control_plane::DfsMetadataShardAssignment;

    fn id(value: &str) -> sinomo_core::Id {
        sinomo_core::Id::from_legacy_named(value).expect("test id")
    }

    fn node(value: &str) -> NodeId {
        NodeId::from_legacy_named(value).expect("test node id")
    }

    fn shard_config(shard_id: &str, epoch: u64) -> DfsMetadataShardConfig {
        DfsMetadataShardConfig {
            file_system_id: id("namespace-feed-filesystem"),
            shard_id: shard_id.to_string(),
            primary_node_id: node("namespace-feed-primary"),
            replica_node_ids: Vec::new(),
            epoch,
            assignment: DfsMetadataShardAssignment::Root,

            default_policy: DfsObjectPolicy::ReplicatedEventual,
        }
    }

    fn object(object_id: &str, path: &str, kind: DfsObjectKind) -> DfsObjectMetadata {
        DfsObjectMetadata {
            object_id: id(object_id),
            file_system_id: id("namespace-feed-filesystem"),
            path: path.to_string(),
            kind,
            tombstone_provenance: None,
            owner_node_id: node("namespace-feed-owner"),
            current_locations: vec![node("namespace-feed-owner")],
            content_hash: Some(format!("blake3:{object_id}")),
            content_size_bytes: Some(12),
            link_target_object_id: None,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            accessed_at_unix_nanos: 1,
            modified_at_unix_nanos: 2,
            created_at_unix_nanos: 3,
            changed_at_unix_nanos: 4,
            version: id(&format!("{object_id}-version")),
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        }
    }

    fn current_object_entries(
        shard_id: &str,
        ordinal: u64,
        object: DfsObjectMetadata,
    ) -> Vec<DfsMetadataJournalEntry> {
        let operation_id = format!("feed-object-{ordinal}");
        let payload = crate::DfsPreparedOperationPayload::for_metadata_effects(
            crate::DfsPreparedOperationScope {
                file_system_id: object.file_system_id.clone(),
                shard_id: DfsMetadataShardId::new(shard_id),
                owner_epoch: 7,
                operation_id: operation_id.clone(),
            },
            Vec::new(),
            Vec::new(),
            vec![crate::DfsPreparedMetadataEffect::PutObject { object }],
            OffsetDateTime::UNIX_EPOCH,
        )
        .unwrap();
        let prepare_sequence = ordinal * 2 - 1;
        let digest =
            crate::DfsPreparedOperationDigest::at_sequence(&payload, prepare_sequence).unwrap();
        let record = crate::DfsPreparedOperationRecord {
            digest: digest.clone(),
            payload: std::sync::Arc::new(payload),
            status: crate::DfsPreparedOperationStatus::Prepared,
            committed_entry: None,
        };
        vec![
            DfsMetadataJournalEntry {
                shard_id: digest.shard_id.clone(),
                sequence: prepare_sequence,
                created_at: OffsetDateTime::UNIX_EPOCH,
                mutation: DfsMetadataMutation::PrepareOperationV7 {
                    file_system_id: digest.file_system_id.clone(),
                    operation_id: operation_id.clone(),
                    record: Box::new(record),
                },
            },
            DfsMetadataJournalEntry {
                shard_id: digest.shard_id.clone(),
                sequence: ordinal * 2,
                created_at: OffsetDateTime::UNIX_EPOCH,
                mutation: DfsMetadataMutation::CommitPreparedOperationV7 {
                    file_system_id: digest.file_system_id.clone(),
                    operation_id,
                    digest,
                },
            },
        ]
    }

    fn batch(
        shard_id: &str,
        entries: Vec<Vec<DfsMetadataJournalEntry>>,
        through_ordinal: u64,
    ) -> DfsMetadataJournalBatch {
        DfsMetadataJournalBatch {
            shard_id: DfsMetadataShardId::new(shard_id),
            entries: entries.into_iter().flatten().collect(),
            next_after_sequence: through_ordinal * 2,
        }
    }

    fn rescan_reason(batch: &DfsNamespaceChangeBatch) -> &DfsNamespaceRescanReason {
        match batch.events.as_slice() {
            [DfsNamespaceChangeEvent::RescanRequired { reason, .. }] => reason,
            events => panic!("expected one rescan event, got {events:?}"),
        }
    }

    #[test]
    fn namespace_change_cursor_restart_replays_no_events_and_keeps_canonical_order() {
        let shard = shard_config("root", 7);
        let cursor = DfsNamespaceChangeCursor::from_shard_config(&shard, 0);
        let first_journal_batch = batch(
            "root",
            vec![
                current_object_entries("root", 1, object("alpha", "/docs/a", DfsObjectKind::File)),
                current_object_entries("root", 2, object("beta", "/docs/b", DfsObjectKind::File)),
            ],
            2,
        );

        let first =
            project_dfs_namespace_change_batch(&cursor, &shard, "/docs", &first_journal_batch);
        assert!(!first.rescan_required());
        assert_eq!(first.next_cursor.after_sequence, 4);
        assert_eq!(first.events.len(), 2);
        let event_ids = first
            .events
            .iter()
            .map(|event| match event {
                DfsNamespaceChangeEvent::Upsert { provenance, .. } => provenance.event_id.clone(),
                event => panic!("expected upsert, got {event:?}"),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            event_ids,
            vec![
                format!("dfs-namespace-change:{}:root:7:2:0", cursor.file_system_id),
                format!("dfs-namespace-change:{}:root:7:4:0", cursor.file_system_id)
            ]
        );

        let cursor_bytes = sinomo_object_serde::to_bytes(&first.next_cursor, 1, LengthMode::U32)
            .expect("encode persisted cursor");
        let restarted: DfsNamespaceChangeCursor =
            sinomo_object_serde::from_bytes(&cursor_bytes).expect("decode persisted cursor");
        assert_eq!(restarted, first.next_cursor);

        let duplicate =
            project_dfs_namespace_change_batch(&restarted, &shard, "/docs", &first_journal_batch);
        assert!(!duplicate.rescan_required());
        assert!(duplicate.events.is_empty());
        assert_eq!(duplicate.next_cursor, restarted);

        let next = project_dfs_namespace_change_batch(
            &restarted,
            &shard,
            "/docs",
            &batch(
                "root",
                vec![current_object_entries(
                    "root",
                    3,
                    object("gamma", "/docs/c", DfsObjectKind::File),
                )],
                3,
            ),
        );
        assert_eq!(next.events.len(), 1);
        assert_eq!(next.next_cursor.after_sequence, 6);
    }

    #[test]
    fn namespace_change_cursor_gap_requires_rescan_without_advancing() {
        let shard = shard_config("root", 7);
        let cursor = DfsNamespaceChangeCursor::from_shard_config(&shard, 0);
        let projected = project_dfs_namespace_change_batch(
            &cursor,
            &shard,
            "/docs",
            &batch(
                "root",
                vec![current_object_entries(
                    "root",
                    2,
                    object("gap-object", "/docs/gap", DfsObjectKind::File),
                )],
                2,
            ),
        );

        assert_eq!(
            rescan_reason(&projected),
            &DfsNamespaceRescanReason::JournalCompactionGap
        );
        assert_eq!(projected.next_cursor, cursor);
        assert_eq!(projected.events.len(), 1);
    }

    #[test]
    fn namespace_change_cursor_epoch_or_shard_change_requires_rescan() {
        let shard = shard_config("root", 7);
        let cursor = DfsNamespaceChangeCursor::from_shard_config(&shard, 8);

        let mut newer_epoch = shard.clone();
        newer_epoch.epoch = 8;
        let epoch = project_dfs_namespace_change_batch(
            &cursor,
            &newer_epoch,
            "/",
            &batch("root", Vec::new(), 4),
        );
        assert_eq!(
            rescan_reason(&epoch),
            &DfsNamespaceRescanReason::AuthorityEpochChanged
        );
        assert_eq!(epoch.next_cursor, cursor);

        let other_shard = shard_config("other", 7);
        let changed = project_dfs_namespace_change_batch(
            &cursor,
            &other_shard,
            "/",
            &batch("other", Vec::new(), 4),
        );
        assert_eq!(
            rescan_reason(&changed),
            &DfsNamespaceRescanReason::ShardChanged
        );
        assert_eq!(changed.next_cursor, cursor);
    }

    #[test]
    fn cross_shard_source_eviction_never_projects_a_false_remove() {
        let shard = shard_config("source", 7);
        let cursor = DfsNamespaceChangeCursor::from_shard_config(&shard, 0);
        let mut eviction = object(
            "cross-shard-object",
            "/docs/source-path",
            DfsObjectKind::Tombstone,
        );
        eviction.content_hash = None;
        eviction.content_size_bytes = None;
        eviction.tombstone_provenance = Some(DfsTombstoneProvenance::SourceEviction {
            source_shard_id: DfsMetadataShardId::new("source"),
            target_shard_id: DfsMetadataShardId::new("target"),
            pin_transfer_id: Some("pin-transfer".to_string()),
        });

        let projected = project_dfs_namespace_change_batch(
            &cursor,
            &shard,
            "/docs",
            &batch(
                "source",
                vec![current_object_entries("source", 1, eviction)],
                1,
            ),
        );

        assert_eq!(
            rescan_reason(&projected),
            &DfsNamespaceRescanReason::CrossShardRenameCorrelationUnavailable
        );
        assert_eq!(projected.next_cursor, cursor);
        assert!(
            !projected
                .events
                .iter()
                .any(|event| matches!(event, DfsNamespaceChangeEvent::Remove { .. }))
        );
    }

    #[test]
    fn subtree_shard_resolution_is_conservative_sorted_and_prefix_scoped() {
        let mut root = shard_config("root", 7);
        root.assignment = DfsMetadataShardAssignment::Root;
        let mut services = shard_config("services", 7);
        services.assignment = DfsMetadataShardAssignment::PathPrefix {
            prefix: "/services".to_string(),
        };
        let mut users = shard_config("users", 7);
        users.assignment = DfsMetadataShardAssignment::PathPrefix {
            prefix: "/users".to_string(),
        };
        let mut hash = shard_config("hash", 7);
        hash.assignment = DfsMetadataShardAssignment::NameHashRange {
            start_inclusive: 0,
            end_exclusive: u64::MAX,
        };
        let selected = dfs_namespace_change_shards_for_subtree(
            &root.file_system_id,
            "/services/projects",
            &[users, services, root.clone(), hash],
        )
        .expect("resolve subtree shards");
        assert_eq!(
            selected
                .iter()
                .map(|shard| shard.shard_id.as_str())
                .collect::<Vec<_>>(),
            vec!["hash", "root", "services"]
        );

        let mut conflicting = root.clone();
        conflicting.epoch += 1;
        let file_system_id = root.file_system_id.clone();
        assert!(
            dfs_namespace_change_shards_for_subtree(&file_system_id, "/", &[root, conflicting])
                .is_err(),
            "one shard id must never resolve to two authority configurations"
        );
    }

    #[test]
    fn namespace_change_poll_wire_keeps_typed_per_shard_cursor() {
        let shard = shard_config("services", 7);
        let cursor = DfsNamespaceChangeCursor::from_shard_config(&shard, 41);
        let operation = crate::DfsNamespaceOperation::PollNamespaceChanges {
            request: crate::DfsNamespaceChangePollRequest {
                root_path: "/".to_string(),
                cursor: cursor.clone(),
            },
        };
        assert_eq!(
            operation.routing_path(),
            None,
            "a subtree root must never masquerade as one cursor shard's dispatch path"
        );
        let encoded = sinomo_object_serde::to_bytes(&operation, 1, LengthMode::U32)
            .expect("encode poll operation");
        let decoded: crate::DfsNamespaceOperation =
            sinomo_object_serde::from_bytes(&encoded).expect("decode poll operation");
        assert_eq!(decoded, operation);

        let response = crate::DfsPosixCoordinationResult::NamespaceChanges(
            crate::DfsNamespaceChangePollResponse {
                batch: DfsNamespaceChangeBatch {
                    next_cursor: cursor,
                    events: Vec::new(),
                    journal_head_sequence: 41,
                    has_more: false,
                },
            },
        );
        let encoded = sinomo_object_serde::to_bytes(&response, 1, LengthMode::U32)
            .expect("encode poll response");
        let decoded: crate::DfsPosixCoordinationResult =
            sinomo_object_serde::from_bytes(&encoded).expect("decode poll response");
        assert_eq!(decoded, response);
    }
}
