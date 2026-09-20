//! File-history capture planning over the existing version and pin tables.
//!
//! These decisions run inside the same metadata transaction as publication,
//! including deterministic replay. No clock, disk access, background task, or
//! alternate namespace is involved in deciding whether to capture old content.

use crate::metadata_records::{DfsMetadataTableOp, DfsObjectVersionRecord};
use crate::metadata_tables::DfsMetadataTables;
use crate::*;
use time::OffsetDateTime;

pub(crate) fn file_restore_job_id(requester: &NodeId, operation_id: &str) -> String {
    let key = format!("{requester}\0{operation_id}");
    format!("file-restore:{}", blake3::hash(key.as_bytes()).to_hex())
}

pub(crate) fn validate_file_restore_job(
    job: &DfsNamespaceJobRecord,
) -> Result<(), MetadataBackendError> {
    let frames = job
        .pending_frames
        .iter()
        .filter(|frame| matches!(frame, DfsNamespaceJobFrame::FileHistoryRestore { .. }))
        .count();
    if job.kind != DfsNamespaceJobKind::FileHistoryRestore {
        return if frames == 0 && !job.job_id.starts_with("file-restore:") {
            Ok(())
        } else {
            Err(MetadataBackendError::Serde(
                "restore state belongs only to a restore job".into(),
            ))
        };
    }
    let invalid = || MetadataBackendError::Serde("invalid durable file restore job state".into());
    let [DfsNamespaceJobFrame::FileHistoryRestore { state }] = job.pending_frames.as_slice() else {
        return Err(invalid());
    };
    crate::service_routed_write::validate_write_operation_id(&state.request.operation_id)
        .map_err(|source| MetadataBackendError::Serde(source.to_string()))?;
    let expected_object = state
        .request
        .expected_destination
        .as_ref()
        .map(|expected| expected.object_id.clone())
        .unwrap_or_else(|| {
            crate::intent::stable_create_object_id(
                &state.requester_node_id,
                &job.root_path,
                &state.write_operation_id,
            )
        });
    if job.file_system_id != sifs_file_system_id()
        || state.planned.file_system_id != job.file_system_id
        || job.job_id != file_restore_job_id(&state.requester_node_id, &state.request.operation_id)
        || state.write_operation_id != format!("restore-write:{}", job.job_id)
        || state.planned.object_id != expected_object
        || state.planned.version
            != crate::intent::stable_write_target_version(
                &state.requester_node_id,
                &expected_object,
                &state.write_operation_id,
            )
        || job
            .origin
            .as_ref()
            .is_none_or(|origin| origin.requester_node_id != state.requester_node_id)
        || state
            .request
            .expected_destination
            .as_ref()
            .is_some_and(|expected| expected.file_system_id != job.file_system_id)
        || job.root_path != crate::metadata_keys::normalize_posix_path(&job.root_path)
        || state.content_hash.is_some() != state.content_size_bytes.is_some()
        || state.source_pin.is_some() != state.content_hash.is_some()
        || (state.source_pin_applied && state.source_pin.is_none())
    {
        return Err(invalid());
    }
    if let Some(pin) = &state.source_pin
        && (pin.identity != state.request.source
            || pin.operation_id != format!("restore-pin:{}", job.job_id)
            || pin.operation
                != (DfsFileHistoryPinOperation::Pin {
                    pin_id: format!("restore:{}", job.job_id),
                }))
    {
        return Err(invalid());
    }
    if let Some(DfsFileRestoreJobResult::Applied { response }) = &state.result
        && (!state.source_pin_applied
            || response.restored_from != state.request.source
            || response.committed != state.planned
            || response.path != job.root_path)
    {
        return Err(invalid());
    }
    match (
        &job.status,
        &state.result,
        state.cleanup_complete,
        job.completed_at,
    ) {
        (DfsNamespaceJobStatus::Running, _, false, None) => {}
        (
            DfsNamespaceJobStatus::Completed,
            Some(DfsFileRestoreJobResult::Applied { .. }),
            true,
            Some(_),
        ) => {}
        (
            DfsNamespaceJobStatus::Failed,
            Some(DfsFileRestoreJobResult::Rejected { .. }),
            true,
            Some(_),
        ) => {}
        _ => return Err(invalid()),
    }
    state
        .options
        .policy
        .validate()
        .map_err(|source| MetadataBackendError::Serde(source.to_string()))?;
    Ok(())
}

fn validate_file_restore_job_transition(
    previous: &DfsNamespaceJobRecord,
    next: &DfsNamespaceJobRecord,
) -> Result<(), MetadataBackendError> {
    if previous.kind != DfsNamespaceJobKind::FileHistoryRestore
        && next.kind != DfsNamespaceJobKind::FileHistoryRestore
    {
        return Ok(());
    }
    validate_file_restore_job(previous)?;
    validate_file_restore_job(next)?;
    fn state(job: &DfsNamespaceJobRecord) -> Option<&DfsFileRestoreJobState> {
        match job.pending_frames.first() {
            Some(DfsNamespaceJobFrame::FileHistoryRestore { state }) => Some(state),
            _ => None,
        }
    }
    let old = state(previous)
        .ok_or_else(|| MetadataBackendError::Serde("restore job kind cannot change".into()))?;
    let new = state(next)
        .ok_or_else(|| MetadataBackendError::Serde("restore job kind cannot change".into()))?;
    if previous.job_id != next.job_id
        || previous.file_system_id != next.file_system_id
        || previous.origin != next.origin
        || previous.root_path != next.root_path
        || previous.created_at != next.created_at
        || old.request != new.request
        || old.requester_node_id != new.requester_node_id
        || old.options != new.options
        || old.write_operation_id != new.write_operation_id
        || old.planned != new.planned
        || old
            .source_pin
            .as_ref()
            .is_some_and(|pin| new.source_pin.as_ref() != Some(pin))
        || old
            .content_hash
            .as_ref()
            .is_some_and(|hash| new.content_hash.as_ref() != Some(hash))
        || old
            .content_size_bytes
            .is_some_and(|size| new.content_size_bytes != Some(size))
        || (old.source_pin_applied && !new.source_pin_applied)
        || old
            .result
            .as_ref()
            .is_some_and(|result| new.result.as_ref() != Some(result))
        || (old.cleanup_complete && !new.cleanup_complete)
        || next.processed_object_count < previous.processed_object_count
    {
        return Err(MetadataBackendError::Serde(
            "restore job cannot change identity or regress durable progress".into(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_file_restore_job_receipt(
    receipt: &DfsNamespaceJobReceipt,
) -> Result<(), MetadataBackendError> {
    let invalid = || MetadataBackendError::Serde("invalid durable file restore receipt".into());
    if receipt.kind != DfsNamespaceJobKind::FileHistoryRestore {
        return if receipt.file_restore.is_none() && !receipt.job_id.starts_with("file-restore:") {
            Ok(())
        } else {
            Err(invalid())
        };
    }
    let outcome = receipt.file_restore.as_ref().ok_or_else(invalid)?;
    let write_operation_id = format!("restore-write:{}", receipt.job_id);
    let object_id = outcome
        .request
        .expected_destination
        .as_ref()
        .map(|expected| expected.object_id.clone())
        .unwrap_or_else(|| {
            crate::intent::stable_create_object_id(
                &outcome.requester_node_id,
                &receipt.root_path,
                &write_operation_id,
            )
        });
    let version = crate::intent::stable_write_target_version(
        &outcome.requester_node_id,
        &object_id,
        &write_operation_id,
    );
    if receipt.file_system_id != sifs_file_system_id()
        || receipt.job_id
            != file_restore_job_id(&outcome.requester_node_id, &outcome.request.operation_id)
        || receipt
            .origin
            .as_ref()
            .is_none_or(|origin| origin.requester_node_id != outcome.requester_node_id)
    {
        return Err(invalid());
    }
    match (&receipt.status, &outcome.result) {
        (DfsNamespaceJobStatus::Completed, DfsFileRestoreJobResult::Applied { response })
            if response.restored_from == outcome.request.source
                && response.committed.file_system_id == receipt.file_system_id
                && response.committed.object_id == object_id
                && response.committed.version == version
                && response.path == receipt.root_path =>
        {
            Ok(())
        }
        (DfsNamespaceJobStatus::Failed, DfsFileRestoreJobResult::Rejected { .. }) => Ok(()),
        _ => Err(invalid()),
    }
}

fn validate_file_restore_mutation(
    tables: &DfsMetadataTables,
    mutation: &DfsMetadataMutation,
) -> Result<(), MetadataBackendError> {
    match mutation {
        DfsMetadataMutation::PutNamespaceJob { job: Some(job), .. } => {
            validate_file_restore_job(job)?;
            if let Some(previous) = tables.namespace_jobs.get(&job.job_id) {
                validate_file_restore_job_transition(previous, job)?;
            }
            if job.kind == DfsNamespaceJobKind::FileHistoryRestore
                && tables
                    .namespace_job_receipts
                    .get(&job.job_id)
                    .is_some_and(|receipt| !receipt.matches_terminal_job(job))
            {
                return Err(MetadataBackendError::Serde(
                    "restore job conflicts with its completed receipt".into(),
                ));
            }
            Ok(())
        }
        DfsMetadataMutation::PutNamespaceJobReceipt { receipt, .. } => {
            validate_file_restore_job_receipt(receipt)
        }
        _ => Ok(()),
    }
}

/// New retained bytes carry the same durable intent and W evidence as a
/// canonical write, but the prepared transaction cannot publish any objects.
pub(crate) fn validate_retained_content_prepare(
    tables: &DfsMetadataTables,
    prepare: &DfsNamespacePrepareRecord,
) -> Result<bool, MetadataBackendError> {
    let tx = &prepare.transaction;
    let Some(intent_id) = tx.transaction_id.strip_prefix("file-history-content:") else {
        if tx.objects.is_empty()
            && tx.versions.iter().any(|version| {
                version.retained.is_some()
                    && !tables
                        .versions
                        .contains_key(&(version.object_id.clone(), version.version.clone()))
            })
        {
            return Err(MetadataBackendError::Serde(
                "new retained content requires an explicit durable write intent".into(),
            ));
        }
        return Ok(false);
    };
    let invalid = || {
        MetadataBackendError::Serde(
            "retained content prepare does not match its durable intent, pin, or W receipts".into(),
        )
    };
    let [version] = tx.versions.as_slice() else {
        return Err(invalid());
    };
    let pins = tx.file_version_pins.as_ref().ok_or_else(invalid)?;
    let [pin] = pins.add.as_slice() else {
        return Err(invalid());
    };
    if !tx.objects.is_empty()
        || !tx.advisory_lock_states.is_empty()
        || !pins.remove.is_empty()
        || version.visible
        || pin.object_id != version.object_id
        || pin.version != version.version
        || pin.file_system_id != version.file_system_id
        || prepare.operation_id != format!("namespace:{}", tx.transaction_id)
    {
        return Err(invalid());
    }
    validate_file_version_record(version)?;
    let captured = version.retained.as_ref().ok_or_else(invalid)?;
    let already_published = tables.namespace_transactions.get(&tx.transaction_id) == Some(tx)
        || tables
            .namespace_transaction_receipts
            .get(&tx.transaction_id)
            .map(|receipt| receipt.matches_transaction(tx))
            .transpose()
            .map_err(|source| MetadataBackendError::Serde(source.to_string()))?
            .unwrap_or(false);
    if already_published {
        return Ok(true);
    }
    let key = (version.object_id.clone(), intent_id.to_string());
    let compacted;
    let intent = if let Some(intent) = tables.intents.get(&key) {
        intent
    } else {
        compacted = tables
            .intent_receipts
            .get(&key)
            .ok_or_else(invalid)?
            .to_intent_record();
        &compacted
    };
    let DfsWritePublication::Retained { capture } = &intent.publication else {
        return Err(invalid());
    };
    validate_replication_plan(&intent.desired_replica_node_ids, &intent.policy)
        .map_err(|source| MetadataBackendError::Serde(source.to_string()))?;
    if intent.file_system_id != tx.file_system_id
        || intent.target_version != version.version
        || intent.content_hash != version.content_hash
        || capture.source != captured.source
        || capture.pin_id != pin.pin_id
        || !matches!(
            intent.status,
            DfsWriteIntentStatus::Staging
                | DfsWriteIntentStatus::ReceiptsPending
                | DfsWriteIntentStatus::Committed
        )
        || intent
            .object_metadata
            .as_ref()
            .and_then(|object| object.content_size_bytes)
            .is_some_and(|size| version.size_bytes != Some(size))
    {
        return Err(invalid());
    }
    let mut nodes = std::collections::BTreeSet::new();
    for receipt in &prepare.content_receipts {
        if receipt.receipt.object_id != version.object_id
            || receipt.receipt.committed_version != version.version
            || receipt.content_hash != version.content_hash
            || !intent
                .desired_replica_node_ids
                .contains(&receipt.replica_node_id)
            || !nodes.insert(&receipt.replica_node_id)
            || !tables
                .receipts
                .get(&(
                    version.object_id.clone(),
                    version.version.clone(),
                    receipt.replica_node_id.clone(),
                ))
                .is_some_and(|stored| {
                    stored.receipt.file_system_id == tx.file_system_id
                        && stored.content_hash == version.content_hash
                })
        {
            return Err(invalid());
        }
    }
    if nodes.len() < intent.policy.min_commit_replicas {
        return Err(invalid());
    }
    Ok(true)
}

/// The effect of a durable operation cannot change on retry, replay, or
/// compaction. In particular a failed current write cannot become a capture.
pub(crate) fn validate_intent_publication_mutation(
    tables: &DfsMetadataTables,
    mutation: &DfsMetadataMutation,
) -> Result<(), MetadataBackendError> {
    validate_file_restore_mutation(tables, mutation)?;
    if let DfsMetadataMutation::RemoveRetainedRepairOwner {
        file_system_id,
        object_id,
        intent_id,
    } = mutation
    {
        if tables
            .intents
            .get(&(object_id.clone(), intent_id.clone()))
            .is_some_and(|record| {
                !record.publication.is_retained_repair() || &record.file_system_id != file_system_id
            })
        {
            return Err(MetadataBackendError::Serde(
                "repair-owner collection cannot remove a user write".into(),
            ));
        }
        return Ok(());
    }
    let receipt_intent;
    let intent = match mutation {
        DfsMetadataMutation::PutIntent {
            intent: Some(intent),
            ..
        } => intent,
        DfsMetadataMutation::PutIntentReceipt { receipt, .. } => {
            if receipt.publication.is_retained_repair() {
                return Err(MetadataBackendError::Serde(
                    "a repair owner is not a user retry receipt".into(),
                ));
            }
            receipt_intent = receipt.to_intent_record();
            &receipt_intent
        }
        _ => return Ok(()),
    };
    validate_intent_publication(intent)?;
    let key = (intent.object_id.clone(), intent.intent_id.clone());
    let existing_publication = tables
        .intents
        .get(&key)
        .map(|record| &record.publication)
        .or_else(|| {
            tables
                .intent_receipts
                .get(&key)
                .map(|record| &record.publication)
        });
    if existing_publication.is_some_and(|publication| publication != &intent.publication) {
        return Err(MetadataBackendError::Serde(
            "write intent publication purpose cannot change".into(),
        ));
    }
    if intent.publication.is_retained_repair()
        && tables
            .intents
            .get(&key)
            .is_some_and(|existing| !retained_repair_plan_matches(existing, intent))
    {
        return Err(MetadataBackendError::Serde(
            "retained repair durability plan cannot change".into(),
        ));
    }
    if !intent.publication.is_current()
        && tables.intents.get(&key).is_some_and(|existing| {
            existing.file_system_id != intent.file_system_id
                || existing.target_version != intent.target_version
                || existing.content_hash != intent.content_hash
                || existing.base_version != intent.base_version
        })
    {
        return Err(MetadataBackendError::Serde(
            "retained content intent identity cannot change".into(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_intent_publication(
    intent: &DfsWriteIntentRecord,
) -> Result<(), MetadataBackendError> {
    intent
        .publication
        .validate()
        .map_err(MetadataBackendError::Serde)?;
    if intent.publication.is_retained_repair()
        && (intent.status != DfsWriteIntentStatus::Committed
            || intent.operation_id.is_some()
            || intent.operation_order_token.is_some()
            || intent.base_version.is_some())
    {
        return Err(MetadataBackendError::Serde(
            "retained repair ownership is committed maintenance work, not a write request".into(),
        ));
    }
    if !intent.publication.is_current()
        && (intent.touch_parent_on_create || intent.create_predecessor_object_id.is_some())
    {
        return Err(MetadataBackendError::Serde(
            "retained content intent cannot create a namespace object".into(),
        ));
    }
    Ok(())
}

/// Validate sparse intent transitions before any part of a replay batch is
/// appended. Materialization validation alone runs after journal durability.
pub(crate) fn validate_intent_publication_batch<'a>(
    tables: &DfsMetadataTables,
    mutations: impl IntoIterator<Item = &'a DfsMetadataMutation>,
) -> Result<(), MetadataBackendError> {
    use std::borrow::Cow;
    let mut jobs = std::collections::BTreeMap::<&str, &DfsNamespaceJobRecord>::new();
    let mut seen =
        std::collections::BTreeMap::<(DfsObjectId, String), Cow<'a, DfsWriteIntentRecord>>::new();
    for mutation in mutations {
        validate_intent_publication_mutation(tables, mutation)?;
        if let DfsMetadataMutation::RemoveRetainedRepairOwner {
            file_system_id,
            object_id,
            intent_id,
        } = mutation
        {
            let key = (object_id.clone(), intent_id.clone());
            if seen.get(&key).is_some_and(|record| {
                !record.publication.is_retained_repair() || &record.file_system_id != file_system_id
            }) {
                return Err(MetadataBackendError::Serde(
                    "repair-owner collection cannot remove a user write".into(),
                ));
            }
            continue;
        }
        if let DfsMetadataMutation::PutNamespaceJob { job: Some(job), .. } = mutation
            && let Some(previous) = jobs.insert(&job.job_id, job)
        {
            validate_file_restore_job_transition(previous, job)?;
        }
        let intent = match mutation {
            DfsMetadataMutation::PutIntent {
                intent: Some(intent),
                ..
            } => Cow::Borrowed(intent),
            DfsMetadataMutation::PutIntentReceipt { receipt, .. } => {
                Cow::Owned(receipt.to_intent_record())
            }
            _ => continue,
        };
        let key = (intent.object_id.clone(), intent.intent_id.clone());
        if seen.get(&key).is_some_and(|previous| {
            previous.publication != intent.publication
                || (intent.publication.is_retained_repair()
                    && !retained_repair_plan_matches(previous, &intent))
                || (!intent.publication.is_current()
                    && (previous.file_system_id != intent.file_system_id
                        || previous.target_version != intent.target_version
                        || previous.content_hash != intent.content_hash
                        || previous.base_version != intent.base_version))
        }) {
            return Err(MetadataBackendError::Serde(
                "write intent changes its immutable publication identity within a journal batch"
                    .into(),
            ));
        }
        seen.insert(key, intent);
    }
    Ok(())
}

/// Capture outgoing bytes only under an explicit rule, when their authoritative
/// namespace publication actually changes content or deletes/replaces the file.
fn object_history_order(tables: &DfsMetadataTables, object: &DfsObjectMetadata) -> u64 {
    tables
        .versions
        .get(&(object.object_id.clone(), object.version.clone()))
        .map(|version| version.history_order)
        .unwrap_or(0)
}

fn family_history_order(tables: &DfsMetadataTables, object_id: &DfsObjectId) -> u64 {
    // A newly adopted object receives its version family before its new head.
    // Ordinary updates use the exact current row above, never a history scan.
    use std::ops::Bound::{Included, Unbounded};
    tables
        .versions
        .range((
            Included((object_id.clone(), crate::metadata_keys::id_lower_bound())),
            Unbounded,
        ))
        .take_while(|((record_object_id, _), _)| record_object_id == object_id)
        .map(|(_, version)| version.history_order)
        .max()
        .unwrap_or(0)
}

fn version_for_object(
    tables: &DfsMetadataTables,
    object: &DfsObjectMetadata,
) -> DfsObjectVersionRecord {
    tables
        .versions
        .get(&(object.object_id.clone(), object.version.clone()))
        .cloned()
        .unwrap_or_else(|| DfsObjectVersionRecord {
            content_durability: None,
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            version: object.version.clone(),
            content_hash: object.content_hash.clone(),
            size_bytes: object.content_size_bytes,
            locations: object.current_locations.clone(),
            created_by_event_id: object.last_modified_by_event_id.clone(),
            created_at: OffsetDateTime::from_unix_timestamp_nanos(
                object.changed_at_unix_nanos.into(),
            )
            .expect("u64 nanos is supported"),
            visible: true,
            history_order: 0,
            retained: None,
        })
}

pub(crate) fn history_order_from_backend<B: DfsMetadataBackend + ?Sized>(
    backend: &B,
    object_id: &DfsObjectId,
) -> Result<u64, MetadataBackendError> {
    let mut high = 0;
    crate::metadata_pages::walk_metadata_pages(|cursor| {
        let page = backend.versions_for_object_page(object_id, cursor, 128)?;
        for version in page.versions {
            high = high.max(version.history_order);
        }
        Ok(page.next_cursor)
    })?;
    Ok(high)
}

/// Logical retention is independent of when physical garbage collection runs.
/// Pins and age grants avoid a count-order lookup altogether.
pub(crate) fn retained_version_is_live(
    version: &DfsObjectVersionRecord,
    pinned: bool,
    now: u64,
    high_water: impl FnOnce() -> Result<u64, MetadataBackendError>,
) -> Result<bool, MetadataBackendError> {
    let Some(retained) = &version.retained else {
        return Ok(false);
    };
    if pinned {
        return Ok(true);
    }
    let Some(capture) = &retained.policy_capture else {
        return Ok(false);
    };
    if capture
        .retention
        .retains(capture.captured_at_unix_nanos, now, usize::MAX)
    {
        return Ok(true);
    }
    if capture.retention.keep_last == 0 {
        return Ok(false);
    }
    let newer = usize::try_from(high_water()?.saturating_sub(capture.order)).unwrap_or(usize::MAX);
    Ok(capture
        .retention
        .retains(capture.captured_at_unix_nanos, now, newer))
}

fn capture_retention(
    tables: &DfsMetadataTables,
    previous: &DfsObjectMetadata,
    next: &DfsObjectMetadata,
) -> DfsFileHistoryRetention {
    capture_retention_for_file_system(
        tables.file_systems.get(&previous.file_system_id),
        previous,
        next,
    )
}

pub(crate) fn capture_retention_for_file_system(
    file_system: Option<&DfsFileSystem>,
    previous: &DfsObjectMetadata,
    next: &DfsObjectMetadata,
) -> DfsFileHistoryRetention {
    if next.tombstone_provenance.is_some()
        || previous.file_system_id != next.file_system_id
        || !matches!(
            previous.kind,
            DfsObjectKind::File | DfsObjectKind::Artifact | DfsObjectKind::Symlink
        )
        || previous.content_hash.is_none()
        || previous.content_size_bytes.is_none()
        || (previous.object_id == next.object_id
            && (previous.version == next.version
                || (next.kind != DfsObjectKind::Tombstone
                    && previous.content_hash == next.content_hash)))
    {
        return DfsFileHistoryRetention::default();
    }
    file_system
        .map(|fs| {
            fs.file_history
                .retention_for_path(&crate::metadata_keys::normalize_posix_path(&previous.path))
        })
        .unwrap_or_default()
}

fn previous_objects<'a>(
    tables: &'a DfsMetadataTables,
    next: &DfsObjectMetadata,
    previous: Option<&'a DfsObjectMetadata>,
) -> impl Iterator<Item = &'a DfsObjectMetadata> {
    let own = previous.or_else(|| tables.objects.get(&next.object_id));
    let replaced = tables
        .paths
        .get(&(
            next.file_system_id.clone(),
            crate::metadata_keys::normalize_path(&next.path),
        ))
        .filter(|path| path.object_id != next.object_id)
        .and_then(|path| tables.objects.get(&path.object_id));
    own.into_iter().chain(replaced)
}

/// Only authoring calls this discovery path. Replay consumes the explicit
/// enrichment journal entry and never infers a new plan from an old command.
pub(crate) fn legacy_capture_candidates<'a>(
    tables: &DfsMetadataTables,
    objects: impl IntoIterator<Item = &'a DfsObjectMetadata>,
) -> Vec<DfsObjectVersionRecord> {
    let mut candidates = std::collections::BTreeMap::new();
    for next in objects {
        if tables
            .file_systems
            .get(&next.file_system_id)
            .is_none_or(|fs| fs.file_history.is_empty())
        {
            continue;
        }
        for previous in previous_objects(tables, next, None) {
            if capture_retention(tables, previous, next).is_disabled() {
                continue;
            }
            let Some(version) = tables
                .versions
                .get(&(previous.object_id.clone(), previous.version.clone()))
                .filter(|version| version.content_durability.is_none())
            else {
                continue;
            };
            candidates.insert(
                (version.object_id.clone(), version.version.clone()),
                version.clone(),
            );
        }
    }
    candidates.into_values().collect()
}

pub(crate) fn validate_file_history_transition(
    tables: &DfsMetadataTables,
    next: &DfsObjectMetadata,
) -> crate::metadata_error::Result<()> {
    for previous in previous_objects(tables, next, None) {
        if !capture_retention(tables, previous, next).is_disabled() {
            validate_file_version_record(&version_for_object(tables, previous))?;
        }
        if !capture_retention(tables, previous, next).is_disabled()
            && object_history_order(tables, previous) == u64::MAX
        {
            return Err(crate::MetadataBackendError::Serde(
                "file history ordering counter is exhausted".into(),
            ));
        }
    }
    Ok(())
}

pub(crate) fn policy_capture_table_ops(
    tables: &DfsMetadataTables,
    next: &DfsObjectMetadata,
    previous: Option<&DfsObjectMetadata>,
) -> Vec<DfsMetadataTableOp> {
    if next.tombstone_provenance.is_some() {
        return Vec::new();
    }
    let mut next_order = tables
        .versions
        .get(&(next.object_id.clone(), next.version.clone()))
        .map(|record| record.history_order)
        .unwrap_or(0);
    if previous
        .or_else(|| tables.objects.get(&next.object_id))
        .is_none_or(|object| object.tombstone_provenance.is_some())
    {
        next_order = next_order.max(family_history_order(tables, &next.object_id));
    }
    let mut ops = Vec::new();
    for previous in previous_objects(tables, next, previous) {
        let old_order = object_history_order(tables, previous);
        if previous.object_id == next.object_id {
            next_order = next_order.max(old_order);
        }
        let retention = capture_retention(tables, previous, next);
        if retention.is_disabled() {
            continue;
        }
        let mut version = version_for_object(tables, previous);
        if version
            .retained
            .as_ref()
            .is_some_and(|capture| capture.policy_capture.is_some())
        {
            continue;
        }
        // Admission rejects exhaustion before journal append. Saturation also
        // keeps legacy diagnostic planners total for malformed fixture state.
        let order = old_order.saturating_add(1);
        let captured_at = next.changed_at_unix_nanos.max(next.modified_at_unix_nanos);
        let retained = version.retained.get_or_insert_with(|| {
            Box::new(DfsRetainedFileVersion {
                object: Box::new(DfsObjectMetadata {
                    path: crate::metadata_keys::normalize_posix_path(&previous.path),
                    ..previous.clone()
                }),
                captured_at_unix_nanos: captured_at,
                source: DfsFileVersionSource::RetentionRule,
                policy_capture: None,
            })
        });
        retained.policy_capture = Some(DfsFileVersionPolicyCapture {
            order,
            captured_at_unix_nanos: captured_at,
            retention,
        });
        version.history_order = order;
        ops.push(DfsMetadataTableOp::PutVersion { version });
        if previous.object_id == next.object_id {
            next_order = next_order.max(order);
        }
    }
    let existing = tables
        .versions
        .get(&(next.object_id.clone(), next.version.clone()));
    let inherited_durability = previous
        .or_else(|| tables.objects.get(&next.object_id))
        .filter(|old| {
            old.object_id == next.object_id
                && old.file_system_id == next.file_system_id
                && old.content_hash.is_some()
                && old.content_hash == next.content_hash
                && old.content_size_bytes == next.content_size_bytes
                && old.last_modified_by_event_id == next.last_modified_by_event_id
        })
        .and_then(|old| {
            tables
                .versions
                .get(&(old.object_id.clone(), old.version.clone()))
        })
        .and_then(|old| old.content_durability.clone());
    if (next_order > 0 && existing.is_none_or(|record| record.history_order < next_order))
        || (inherited_durability.is_some()
            && existing.is_none_or(|record| record.content_durability.is_none()))
    {
        let mut version = version_for_object(tables, next);
        version.history_order = next_order;
        if version.content_durability.is_none() {
            version.content_durability = inherited_durability;
        }
        ops.push(DfsMetadataTableOp::PutVersion { version });
    }
    ops
}

pub(crate) fn validate_file_version_record(
    version: &DfsObjectVersionRecord,
) -> crate::metadata_error::Result<()> {
    if let Some(plan) = &version.content_durability {
        validate_replication_plan(&plan.desired_replica_node_ids, &plan.policy)
            .map_err(|error| MetadataBackendError::Serde(error.to_string()))?;
        if version.content_hash.is_none() || version.size_bytes.is_none() {
            return Err(MetadataBackendError::Serde(
                "content durability requires immutable content identity".into(),
            ));
        }
    }
    if let Some(retained) = &version.retained {
        retained
            .validate_for_version(version)
            .map_err(crate::MetadataBackendError::Serde)?;
    }
    Ok(())
}

pub(crate) fn retained_repair_intent_id(version: &DfsObjectVersionRecord) -> String {
    let key = format!(
        "{}:{}:{}",
        version.file_system_id, version.object_id, version.version
    );
    format!("history-repair:{}", blake3::hash(key.as_bytes()).to_hex())
}

/// Materialize a durability owner, not a replay of the original user's write.
/// Its logical lifetime starts with the capture, making repeated construction
/// from the same immutable evidence deterministic.
pub(crate) fn retained_repair_intent(
    version: &DfsObjectVersionRecord,
    source: Option<&DfsWriteIntentRecord>,
) -> Result<DfsWriteIntentRecord, MetadataBackendError> {
    let captured = version.retained.as_ref().ok_or_else(|| {
        MetadataBackendError::Serde("repair ownership requires explicit retained provenance".into())
    })?;
    if version.content_hash.is_none()
        || source.is_some_and(|source| {
            source.status != DfsWriteIntentStatus::Committed
                || version.file_system_id != source.file_system_id
                || version.object_id != source.object_id
                || version.content_hash != source.content_hash
                || source.publication.is_retained_repair()
        })
    {
        return Err(MetadataBackendError::Serde(
            "repair ownership has unrelated content evidence".into(),
        ));
    }
    let plan = version
        .content_durability
        .clone()
        .or_else(|| source.map(crate::content_durability::from_record))
        .ok_or_else(|| {
            MetadataBackendError::Serde("retained version has no recorded durability plan".into())
        })?;
    validate_replication_plan(&plan.desired_replica_node_ids, &plan.policy)
        .map_err(|error| MetadataBackendError::Serde(error.to_string()))?;
    let created_at =
        OffsetDateTime::from_unix_timestamp_nanos(captured.captured_at_unix_nanos.into())
            .map_err(|error| MetadataBackendError::Serde(error.to_string()))?;
    Ok(DfsWriteIntentRecord {
        publication: DfsWritePublication::RetainedRepair,
        intent_id: retained_repair_intent_id(version),
        file_system_id: version.file_system_id.clone(),
        object_id: version.object_id.clone(),
        base_version: None,
        target_version: version.version.clone(),
        coordinator_node_id: plan.coordinator_node_id.clone(),
        operation_id: None,
        operation_order_token: None,
        owner_epoch: plan.owner_epoch,
        desired_replica_node_ids: plan.desired_replica_node_ids.clone(),
        policy: plan.policy.clone(),
        status: DfsWriteIntentStatus::Committed,
        content_hash: version.content_hash.clone(),
        object_metadata: None,
        touch_parent_on_create: false,
        create_predecessor_object_id: None,
        created_at,
        updated_at: Some(created_at),
    })
}

pub(crate) fn retained_repair_plan_matches(
    left: &DfsWriteIntentRecord,
    right: &DfsWriteIntentRecord,
) -> bool {
    left.publication.is_retained_repair()
        && right.publication.is_retained_repair()
        && left.file_system_id == right.file_system_id
        && left.object_id == right.object_id
        && left.target_version == right.target_version
        && left.content_hash == right.content_hash
        && left.policy == right.policy
        && left.desired_replica_node_ids == right.desired_replica_node_ids
}

/// Stamp only newly derived grants, before their table operations are applied.
/// Imported versions keep the immutable time supplied by their source authority.
pub(crate) fn stamp_policy_captures(
    tables: &DfsMetadataTables,
    ops: &mut Vec<DfsMetadataTableOp>,
    committed_at: OffsetDateTime,
) {
    let committed_at = u64::try_from(committed_at.unix_timestamp_nanos()).unwrap_or(0);
    for op in ops.iter_mut() {
        let DfsMetadataTableOp::PutVersion { version } = op else {
            continue;
        };
        if tables
            .versions
            .get(&(version.object_id.clone(), version.version.clone()))
            .and_then(|version| version.retained.as_ref())
            .and_then(|capture| capture.policy_capture.as_ref())
            .is_some()
        {
            continue;
        }
        let Some(capture) = version.retained.as_mut() else {
            continue;
        };
        let Some(policy) = capture.policy_capture.as_mut() else {
            continue;
        };
        policy.captured_at_unix_nanos = committed_at;
        if matches!(capture.source, DfsFileVersionSource::RetentionRule) {
            capture.captured_at_unix_nanos = committed_at;
        }
    }
    let owners = ops
        .iter()
        .filter_map(|op| match op {
            DfsMetadataTableOp::PutVersion { version } => {
                capture_repair_owner_table_op(tables, version)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    ops.extend(owners);
}

/// A newly captured generation owns its durable repair plan in the same
/// metadata commit. A still-present data intent retains ownership instead;
/// compact retry receipts never become executors again.
pub(crate) fn capture_repair_owner_table_op(
    tables: &DfsMetadataTables,
    version: &DfsObjectVersionRecord,
) -> Option<DfsMetadataTableOp> {
    if version.retained.is_none() || version.content_durability.is_none() {
        return None;
    }
    let plan = version
        .content_durability
        .as_ref()
        .expect("checked durability");
    let owner_key = (
        version.object_id.clone(),
        retained_repair_intent_id(version),
    );
    if tables.intents.contains_key(&owner_key) || tables.intent_receipts.contains_key(&owner_key) {
        // Never replace an existing durable identity from a derived table op.
        // Ordinary intent admission handles any inconsistent record explicitly.
        return None;
    }
    use std::ops::Bound::{Included, Unbounded};
    let existing_owner = tables
        .intent_targets
        .range((
            Included((
                version.object_id.clone(),
                version.version.clone(),
                String::new(),
            )),
            Unbounded,
        ))
        .take_while(|((object, target, _), _)| {
            object == &version.object_id && target == &version.version
        })
        .filter_map(|(_, key)| tables.intents.get(key))
        .any(|intent| {
            intent.file_system_id == version.file_system_id
                // Placement work expires with its policy job. It cannot own a
                // retained grant, and an unrelated W/N cannot discharge it.
                && !intent.intent_id.starts_with("placement:")
                && intent.policy == plan.policy
                && intent.desired_replica_node_ids == plan.desired_replica_node_ids
                && (crate::intent::write_intent_status_is_active(&intent.status)
                    || intent.status == DfsWriteIntentStatus::Committed)
        });
    if existing_owner {
        return None;
    }
    // Admission validates new captures. Malformed legacy rows cannot create
    // executable maintenance work while a metadata image is being recovered.
    retained_repair_intent(version, None)
        .ok()
        .map(|intent| DfsMetadataTableOp::PutIntent { intent })
}

pub(crate) fn validate_retained_version_update(
    existing: Option<&DfsObjectVersionRecord>,
    incoming: &DfsObjectVersionRecord,
) -> crate::metadata_error::Result<()> {
    validate_file_version_record(incoming)?;
    if let (Some(old), Some(plan)) = (
        existing.and_then(|record| record.content_durability.as_ref()),
        incoming.content_durability.as_ref(),
    ) && old != plan
    {
        return Err(MetadataBackendError::Serde(
            "recorded content durability is immutable".into(),
        ));
    }
    let Some(existing) = existing.filter(|record| record.retained.is_some()) else {
        return Ok(());
    };
    if existing.file_system_id != incoming.file_system_id
        || existing.content_hash != incoming.content_hash
        || existing.size_bytes != incoming.size_bytes
    {
        return Err(crate::MetadataBackendError::Serde(
            "an explicitly retained version cannot change content identity".into(),
        ));
    }
    if let Some(incoming_capture) = &incoming.retained {
        let existing_capture = existing.retained.as_ref().expect("retained version");
        if incoming_capture.object != existing_capture.object
            || incoming_capture.source != existing_capture.source
            || incoming_capture.captured_at_unix_nanos != existing_capture.captured_at_unix_nanos
            || (existing_capture.policy_capture.is_some()
                && existing_capture.policy_capture != incoming_capture.policy_capture)
        {
            return Err(crate::MetadataBackendError::Serde(
                "retained file version provenance is immutable".into(),
            ));
        }
    }
    Ok(())
}

pub(crate) fn validate_transaction_file_pins(
    tables: &DfsMetadataTables,
    transaction: &DfsNamespaceTransaction,
) -> crate::metadata_error::Result<()> {
    let Some(pins) = &transaction.file_version_pins else {
        return Ok(());
    };
    pins.validate_scope(&transaction.file_system_id)
        .map_err(crate::MetadataBackendError::Serde)?;
    let error = |message: &str| crate::MetadataBackendError::Serde(message.into());
    let mut keys = std::collections::BTreeSet::new();
    let mut added_per_object = std::collections::BTreeMap::<DfsObjectId, usize>::new();
    for pin in pins.add.iter().chain(&pins.remove) {
        if !transaction.objects.iter().any(|object| {
            object.object_id == pin.object_id && object.file_system_id == pin.file_system_id
        }) && !transaction.versions.iter().any(|version| {
            version.object_id == pin.object_id && version.file_system_id == pin.file_system_id
        }) {
            return Err(error(
                "history pin mutation requires an object authority precondition",
            ));
        }
        if pin.file_system_id != transaction.file_system_id
            || pin.pin_id.trim().is_empty()
            || pin.pin_id.len() > 256
            || !keys.insert((
                pin.object_id.clone(),
                pin.version.clone(),
                pin.pin_id.clone(),
            ))
        {
            return Err(error(
                "invalid, cross-filesystem, or duplicate history pin mutation",
            ));
        }
        if tables
            .cross_shard_pin_transfer_fence(&pin.object_id, &pin.version, &pin.pin_id)
            .is_some()
        {
            return Err(error("history pin is fenced by a cross-shard transfer"));
        }
        if let Some(existing) = tables.version_pins.get(&(
            pin.object_id.clone(),
            pin.version.clone(),
            pin.pin_id.clone(),
        )) && existing != pin
        {
            return Err(error(
                "history pin identity was reused with a different payload",
            ));
        }
    }
    for pin in &pins.add {
        let record = transaction
            .versions
            .iter()
            .find(|v| v.object_id == pin.object_id && v.version == pin.version)
            .or_else(|| {
                tables
                    .versions
                    .get(&(pin.object_id.clone(), pin.version.clone()))
            })
            .ok_or_else(|| error("history pin references a missing content version"))?;
        validate_file_version_record(record)?;
        if tables
            .versions
            .get(&(record.object_id.clone(), record.version.clone()))
            .is_some_and(|existing| existing.retained.is_none())
            && record.retained.as_ref().is_some_and(|capture| {
                matches!(capture.source, DfsFileVersionSource::Checkpoint { .. })
            })
            && !tables
                .objects
                .get(&record.object_id)
                .is_some_and(|current| {
                    current.version == record.version && current.kind != DfsObjectKind::Tombstone
                })
        {
            return Err(error(
                "file changed before its explicit checkpoint was captured",
            ));
        }
        if record.retained.is_none() {
            return Err(error("history pin requires an explicitly captured version"));
        }
        if !tables.version_pins.contains_key(&(
            pin.object_id.clone(),
            pin.version.clone(),
            pin.pin_id.clone(),
        )) {
            *added_per_object.entry(pin.object_id.clone()).or_default() += 1;
        }
    }
    for (object_id, added) in added_per_object {
        let existing = tables
            .version_pins
            .keys()
            .filter(|(id, _, _)| id == &object_id)
            .count();
        let removed = pins
            .remove
            .iter()
            .filter(|pin| {
                pin.object_id == object_id
                    && tables.version_pins.contains_key(&(
                        pin.object_id.clone(),
                        pin.version.clone(),
                        pin.pin_id.clone(),
                    ))
            })
            .count();
        if existing.saturating_sub(removed).saturating_add(added)
            > crate::metadata_limits::MAX_DURABLE_VERSION_PINS_PER_OBJECT
        {
            return Err(error("file has too many independently named history pins"));
        }
    }
    Ok(())
}

pub(crate) fn transaction_file_pin_ops(
    transaction: &DfsNamespaceTransaction,
) -> Vec<DfsMetadataTableOp> {
    let Some(pins) = &transaction.file_version_pins else {
        return Vec::new();
    };
    pins.remove
        .iter()
        .map(|pin| DfsMetadataTableOp::RemoveVersionPin {
            object_id: pin.object_id.clone(),
            version: pin.version.clone(),
            pin_id: pin.pin_id.clone(),
        })
        .chain(
            pins.add
                .iter()
                .map(|pin| DfsMetadataTableOp::PutVersionPin { pin: pin.clone() }),
        )
        .collect()
}

/// Background selection, shared by history listings and authoritative GC.
/// Independent pin roots are added by the caller.
pub(crate) fn policy_retained_versions(
    tables: &DfsMetadataTables,
    now: u64,
) -> std::collections::BTreeSet<(DfsObjectId, DfsVersion)> {
    let mut high_water = std::collections::BTreeMap::<DfsObjectId, u64>::new();
    for version in tables.versions.values() {
        let high = high_water.entry(version.object_id.clone()).or_default();
        *high = (*high).max(version.history_order);
    }
    let mut protected = std::collections::BTreeSet::new();
    for version in tables.versions.values() {
        if tables
            .objects
            .get(&version.object_id)
            .is_some_and(|object| object.tombstone_provenance.is_some())
        {
            continue;
        }
        let Some(capture) = version
            .retained
            .as_ref()
            .and_then(|capture| capture.policy_capture.as_ref())
        else {
            continue;
        };
        let high = high_water
            .get(&version.object_id)
            .copied()
            .unwrap_or(capture.order);
        let newer = usize::try_from(high.saturating_sub(capture.order)).unwrap_or(usize::MAX);
        if capture
            .retention
            .retains(capture.captured_at_unix_nanos, now, newer)
        {
            protected.insert((version.object_id.clone(), version.version.clone()));
        }
    }
    protected
}
