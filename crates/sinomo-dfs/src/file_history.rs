//! File-history capture planning over the existing version and pin tables.
//!
//! These decisions run inside the same metadata transaction as publication,
//! including deterministic replay. No clock, disk access, background task, or
//! alternate namespace is involved in deciding whether to capture old content.

use crate::metadata_records::{DfsMetadataTableOp, DfsObjectVersionRecord};
use crate::metadata_tables::DfsMetadataTables;
use crate::*;
use time::OffsetDateTime;

pub(crate) use crate::service_restore_jobs::{
    validate_content_copy_job as validate_file_restore_job,
    validate_content_copy_job_receipt as validate_file_restore_job_receipt,
};

fn validate_file_restore_mutation(
    tables: &DfsMetadataTables,
    mutation: &DfsMetadataMutation,
) -> Result<(), MetadataBackendError> {
    match mutation {
        DfsMetadataMutation::PutNamespaceJob { job: Some(job), .. } => {
            crate::service_restore_jobs::validate_content_copy_job(job)?;
            if let Some(previous) = tables.namespace_jobs.get(&job.job_id) {
                crate::service_restore_jobs::validate_content_copy_job_transition(previous, job)?;
            }
            if job.kind == DfsNamespaceJobKind::ContentCopy
                && tables
                    .namespace_job_receipts
                    .get(&job.job_id)
                    .is_some_and(|receipt| !receipt.matches_terminal_job(job))
            {
                return Err(MetadataBackendError::Serde(
                    "content copy job conflicts with its terminal receipt".into(),
                ));
            }
            Ok(())
        }
        DfsMetadataMutation::PutNamespaceJobReceipt { receipt, .. } => {
            crate::service_restore_jobs::validate_content_copy_job_receipt(receipt)
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
                version.has_retention_grant()
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
        || version.retention_grant(&capture.namespace).is_none()
        || version.module_metadata.get(&capture.namespace) != capture.metadata.as_ref()
        || crate::lifecycle::content_reference_record_id(pin)
            != crate::lifecycle::content_reference_storage_id(
                &capture.namespace,
                &capture.reference_id,
            )
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
            crate::service_restore_jobs::validate_content_copy_job_transition(previous, job)?;
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
fn object_retention_orders(
    tables: &DfsMetadataTables,
    object: &DfsObjectMetadata,
) -> std::collections::BTreeMap<String, u64> {
    tables
        .versions
        .get(&(object.object_id.clone(), object.version.clone()))
        .map(|version| {
            version
                .retention
                .iter()
                .map(|(namespace, state)| (namespace.clone(), state.order))
                .collect()
        })
        .unwrap_or_default()
}

fn family_retention_orders(
    tables: &DfsMetadataTables,
    object_id: &DfsObjectId,
) -> std::collections::BTreeMap<String, u64> {
    use std::ops::Bound::{Included, Unbounded};
    let mut orders = std::collections::BTreeMap::new();
    for (_, version) in tables
        .versions
        .range((
            Included((object_id.clone(), crate::metadata_keys::id_lower_bound())),
            Unbounded,
        ))
        .take_while(|((id, _), _)| id == object_id)
    {
        for (namespace, state) in &version.retention {
            let order = orders.entry(namespace.clone()).or_insert(0);
            *order = (*order).max(state.order);
        }
    }
    orders
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
            metadata_format: crate::DfsRevisionMetadataFormat::NamespacedV6,
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
            retention: Default::default(),
            module_metadata: Default::default(),
        })
}

pub(crate) fn retention_orders_from_backend<B: DfsMetadataBackend + ?Sized>(
    backend: &B,
    object_id: &DfsObjectId,
) -> Result<std::collections::BTreeMap<String, u64>, MetadataBackendError> {
    let current = backend.get_object(object_id)?.ok_or_else(|| {
        MetadataBackendError::Serde("canonical retention order owner is unavailable".into())
    })?;
    let version = backend
        .get_version(object_id, &current.version)?
        .ok_or_else(|| {
            MetadataBackendError::Serde("canonical retention order witness is unavailable".into())
        })?;
    Ok(version
        .retention
        .into_iter()
        .map(|(namespace, state)| (namespace, state.order))
        .collect())
}

/// Grant liveness is independent of provider availability and physical GC.
pub(crate) fn retained_namespace_is_live(
    version: &DfsObjectVersionRecord,
    namespace: Option<&str>,
    pinned: bool,
    now: u64,
    high_water: impl FnOnce() -> Result<std::collections::BTreeMap<String, u64>, MetadataBackendError>,
) -> Result<bool, MetadataBackendError> {
    let grants = version
        .retention
        .iter()
        .filter(|(owner, _)| namespace.is_none_or(|namespace| namespace == owner.as_str()))
        .filter_map(|(owner, state)| state.grant.as_ref().map(|grant| (owner, grant)));
    let mut count_grants = Vec::new();
    let mut found = false;
    for (owner, grant) in grants {
        found = true;
        if pinned {
            return Ok(true);
        }
        let Some(minimum) = &grant.minimum else {
            continue;
        };
        if minimum
            .minimum
            .retains(minimum.granted_at_unix_nanos, now, usize::MAX)
        {
            return Ok(true);
        }
        if minimum.minimum.keep_last != 0 {
            count_grants.push((owner, minimum));
        }
    }
    if !found || count_grants.is_empty() {
        return Ok(false);
    }
    let orders = high_water()?;
    let mut incomplete = false;
    for (owner, grant) in count_grants {
        let Some(high) = orders
            .get(owner)
            .copied()
            .filter(|high| *high >= grant.order)
        else {
            incomplete = true;
            continue;
        };
        if grant.minimum.retains(
            grant.granted_at_unix_nanos,
            now,
            usize::try_from(high.saturating_sub(grant.order)).unwrap_or(usize::MAX),
        ) {
            return Ok(true);
        }
    }
    if incomplete {
        return Err(MetadataBackendError::Serde(
            "canonical namespace retention order witness is unavailable".into(),
        ));
    }
    Ok(false)
}

pub(crate) fn retained_version_is_live(
    version: &DfsObjectVersionRecord,
    pinned: bool,
    now: u64,
    high_water: impl FnOnce() -> Result<std::collections::BTreeMap<String, u64>, MetadataBackendError>,
) -> Result<bool, MetadataBackendError> {
    retained_namespace_is_live(version, None, pinned, now, high_water)
}

pub(crate) fn publication_adds_retention_grant(
    tables: &DfsMetadataTables,
    next: &DfsObjectMetadata,
) -> bool {
    previous_objects(tables, next, None)
        .any(|previous| !capture_retention(tables, previous, next).is_empty())
}

fn capture_retention(
    tables: &DfsMetadataTables,
    previous: &DfsObjectMetadata,
    next: &DfsObjectMetadata,
) -> std::collections::BTreeMap<String, DfsRetentionMinimum> {
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
) -> std::collections::BTreeMap<String, DfsRetentionMinimum> {
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
        return Default::default();
    }
    file_system
        .map(|fs| {
            fs.retention_policies
                .iter()
                .filter_map(|(namespace, policy)| {
                    let minimum = policy.minimum_for_path(
                        &crate::metadata_keys::normalize_posix_path(&previous.path),
                    );
                    (!minimum.is_disabled()).then(|| (namespace.clone(), minimum))
                })
                .collect()
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

/// Legacy bytes acquire a verified durability plan before accepting a grant.
pub(crate) fn legacy_capture_candidates<'a>(
    tables: &DfsMetadataTables,
    objects: impl IntoIterator<Item = &'a DfsObjectMetadata>,
) -> Vec<DfsObjectVersionRecord> {
    let mut candidates = std::collections::BTreeMap::new();
    for next in objects {
        for previous in previous_objects(tables, next, None) {
            if capture_retention(tables, previous, next).is_empty() {
                continue;
            }
            if let Some(version) = tables
                .versions
                .get(&(previous.object_id.clone(), previous.version.clone()))
                .filter(|version| version.content_durability.is_none())
            {
                candidates.insert(
                    (version.object_id.clone(), version.version.clone()),
                    version.clone(),
                );
            }
        }
    }
    candidates.into_values().collect()
}

pub(crate) fn validate_file_history_transition(
    tables: &DfsMetadataTables,
    next: &DfsObjectMetadata,
) -> crate::metadata_error::Result<()> {
    for previous in previous_objects(tables, next, None) {
        let contributions = capture_retention(tables, previous, next);
        if contributions.is_empty() {
            continue;
        }
        validate_file_version_record(&version_for_object(tables, previous))?;
        let orders = object_retention_orders(tables, previous);
        if contributions
            .keys()
            .any(|namespace| orders.get(namespace) == Some(&u64::MAX))
        {
            return Err(MetadataBackendError::Serde(
                "retention order stream is exhausted".into(),
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
    let mut next_orders = object_retention_orders(tables, next);
    if previous
        .or_else(|| tables.objects.get(&next.object_id))
        .is_none_or(|object| object.tombstone_provenance.is_some())
    {
        for (namespace, order) in family_retention_orders(tables, &next.object_id) {
            let high = next_orders.entry(namespace).or_insert(0);
            *high = (*high).max(order);
        }
    }
    let mut ops = Vec::new();
    for previous in previous_objects(tables, next, previous) {
        let old_orders = object_retention_orders(tables, previous);
        if previous.object_id == next.object_id {
            for (namespace, order) in &old_orders {
                let high = next_orders.entry(namespace.clone()).or_insert(0);
                *high = (*high).max(*order);
            }
        }
        let contributions = capture_retention(tables, previous, next);
        if contributions.is_empty() {
            continue;
        }
        let mut version = version_for_object(tables, previous);
        let mut changed = false;
        for (namespace, minimum) in contributions {
            let state = version.retention.entry(namespace.clone()).or_default();
            if state
                .grant
                .as_ref()
                .is_some_and(|grant| grant.minimum.is_some())
            {
                continue;
            }
            let order = old_orders
                .get(&namespace)
                .copied()
                .unwrap_or(0)
                .saturating_add(1);
            let granted_at = next.changed_at_unix_nanos.max(next.modified_at_unix_nanos);
            let grant = state.grant.get_or_insert_with(|| {
                Box::new(DfsRevisionRetentionGrant {
                    object: Box::new(DfsObjectMetadata {
                        path: crate::metadata_keys::normalize_posix_path(&previous.path),
                        ..previous.clone()
                    }),
                    granted_at_unix_nanos: granted_at,
                    origin: DfsRetentionGrantOrigin::Rule,
                    minimum: None,
                })
            });
            grant.minimum = Some(DfsOrderedRetentionGrant {
                order,
                granted_at_unix_nanos: granted_at,
                minimum,
            });
            state.order = order;
            changed = true;
            if previous.object_id == next.object_id {
                let high = next_orders.entry(namespace).or_insert(0);
                *high = (*high).max(order);
            }
        }
        if changed {
            version.metadata_format = DfsRevisionMetadataFormat::NamespacedV6;
            ops.push(DfsMetadataTableOp::PutVersion { version });
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
    if next_orders.iter().any(|(namespace, order)| {
        *order > existing.map_or(0, |record| record.retention_order(namespace))
    }) || (inherited_durability.is_some()
        && existing.is_none_or(|record| record.content_durability.is_none()))
    {
        let mut version = version_for_object(tables, next);
        version.metadata_format = DfsRevisionMetadataFormat::NamespacedV6;
        for (namespace, order) in next_orders {
            let state = version.retention.entry(namespace).or_default();
            state.order = state.order.max(order);
        }
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
    version
        .validate_extension_metadata()
        .map_err(MetadataBackendError::Serde)?;
    if let Some(plan) = &version.content_durability {
        validate_replication_plan(&plan.desired_replica_node_ids, &plan.policy)
            .map_err(|error| MetadataBackendError::Serde(error.to_string()))?;
        if version.content_hash.is_none() || version.size_bytes.is_none() {
            return Err(MetadataBackendError::Serde(
                "content durability requires immutable content identity".into(),
            ));
        }
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
    let captured = version
        .retention
        .values()
        .find_map(|state| state.grant.as_ref())
        .ok_or_else(|| {
            MetadataBackendError::Serde(
                "repair ownership requires explicit retained provenance".into(),
            )
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
        OffsetDateTime::from_unix_timestamp_nanos(captured.granted_at_unix_nanos.into())
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
        for (namespace, state) in &mut version.retention {
            if tables
                .versions
                .get(&(version.object_id.clone(), version.version.clone()))
                .and_then(|version| version.retention_grant(namespace))
                .and_then(|grant| grant.minimum.as_ref())
                .is_some()
            {
                continue;
            }
            let Some(grant) = state.grant.as_mut() else {
                continue;
            };
            let Some(minimum) = grant.minimum.as_mut() else {
                continue;
            };
            minimum.granted_at_unix_nanos = committed_at;
            if grant.origin == DfsRetentionGrantOrigin::Rule {
                grant.granted_at_unix_nanos = committed_at;
            }
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
    if !version.has_retention_grant() || version.content_durability.is_none() {
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
    if let (Some(old), Some(new)) = (
        existing.and_then(|record| record.content_durability.as_ref()),
        incoming.content_durability.as_ref(),
    ) && old != new
    {
        return Err(MetadataBackendError::Serde(
            "recorded content durability is immutable".into(),
        ));
    }
    let Some(existing) = existing.filter(|record| record.has_retention_grant()) else {
        return Ok(());
    };
    if existing.file_system_id != incoming.file_system_id
        || existing.content_hash != incoming.content_hash
        || existing.size_bytes != incoming.size_bytes
    {
        return Err(MetadataBackendError::Serde(
            "a referenced revision cannot change content identity".into(),
        ));
    }
    for (namespace, state) in &existing.retention {
        let Some(old) = &state.grant else { continue };
        let Some(new) = incoming.retention_grant(namespace) else {
            continue;
        };
        if old.object != new.object
            || old.origin != new.origin
            || old.granted_at_unix_nanos != new.granted_at_unix_nanos
            || (old.minimum.is_some() && old.minimum != new.minimum)
        {
            return Err(MetadataBackendError::Serde(
                "accepted retention grant cannot change identity or minimum".into(),
            ));
        }
    }
    Ok(())
}

fn validate_content_reference_source_liveness(
    tables: &DfsMetadataTables,
    transaction: &DfsNamespaceTransaction,
    pin: &crate::DfsMetadataVersionPinRecord,
    decision_time: u64,
) -> crate::metadata_error::Result<()> {
    let source = tables.objects.get(&pin.object_id).or_else(|| {
        transaction
            .objects
            .iter()
            .find(|object| object.object_id == pin.object_id)
    });
    if source.is_some_and(|object| {
        object.file_system_id == pin.file_system_id
            && object.kind != DfsObjectKind::Tombstone
            && object.tombstone_provenance.is_none()
    }) {
        return Ok(());
    }
    let prior = tables
        .versions
        .get(&(pin.object_id.clone(), pin.version.clone()));
    let prior_reference = tables
        .version_pins
        .range((
            std::ops::Bound::Included((pin.object_id.clone(), pin.version.clone(), String::new())),
            std::ops::Bound::Unbounded,
        ))
        .take_while(|((object_id, version, _), _)| {
            object_id == &pin.object_id && version == &pin.version
        })
        .next()
        .is_some();
    if prior.is_none_or(|prior| !prior.has_retention_grant())
        || !retained_version_is_live(
            prior.expect("prior grant"),
            prior_reference,
            decision_time,
            || Ok(family_retention_orders(tables, &pin.object_id)),
        )?
    {
        return Err(MetadataBackendError::Serde(
            "deleted content source has no live retention grant".into(),
        ));
    }
    Ok(())
}

/// Only fresh authoritative admission uses the current writer clock. Accepted
/// prepared commits and replicated replay validate the persisted decision time.
pub(crate) fn validate_fresh_content_reference_liveness(
    tables: &DfsMetadataTables,
    transaction: &DfsNamespaceTransaction,
    now: u64,
) -> crate::metadata_error::Result<()> {
    if let Some(pins) = &transaction.file_version_pins {
        for pin in &pins.add {
            if pin.namespace.is_some()
                && !tables.version_pins.contains_key(&(
                    pin.object_id.clone(),
                    pin.version.clone(),
                    pin.pin_id.clone(),
                ))
            {
                validate_content_reference_source_liveness(tables, transaction, pin, now)?;
            }
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
    let recorded = tables
        .namespace_transactions
        .get(&transaction.transaction_id)
        .is_some_and(|existing| existing == transaction)
        || tables
            .namespace_transaction_receipts
            .get(&transaction.transaction_id)
            .map(|receipt| {
                receipt
                    .matches_transaction(transaction)
                    .map_err(|error| MetadataBackendError::Serde(error.to_string()))
            })
            .transpose()?
            .unwrap_or(false);
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
            || pin.pin_id.len() > 1024
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
            .is_some_and(|existing| !existing.has_retention_grant())
            && record.retention.values().any(|state| {
                state
                    .grant
                    .as_ref()
                    .is_some_and(|grant| grant.origin == DfsRetentionGrantOrigin::ExplicitOperation)
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
        if !record.has_retention_grant() {
            return Err(error("history pin requires an explicitly captured version"));
        }
        let new_reference = !tables.version_pins.contains_key(&(
            pin.object_id.clone(),
            pin.version.clone(),
            pin.pin_id.clone(),
        ));
        if pin.namespace.is_some() && new_reference && !recorded {
            // A live object may repin a structurally captured alternative
            // revision. A deleted source needs a genuine prior content root;
            // the proposed reference or grant cannot authorize itself.
            let decision_time = u64::try_from(pin.created_at.unix_timestamp_nanos())
                .map_err(|_| error("content reference decision time is out of range"))?;
            validate_content_reference_source_liveness(tables, transaction, pin, decision_time)?;
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
    let mut high_water =
        std::collections::BTreeMap::<DfsObjectId, std::collections::BTreeMap<String, u64>>::new();
    for version in tables.versions.values() {
        let orders = high_water.entry(version.object_id.clone()).or_default();
        for (namespace, state) in &version.retention {
            let order = orders.entry(namespace.clone()).or_insert(0);
            *order = (*order).max(state.order);
        }
    }
    tables
        .versions
        .values()
        .filter(|version| {
            tables
                .objects
                .get(&version.object_id)
                .is_none_or(|object| object.tombstone_provenance.is_none())
                && retained_version_is_live(version, false, now, || {
                    Ok(high_water
                        .get(&version.object_id)
                        .cloned()
                        .unwrap_or_default())
                })
                .unwrap_or(false)
        })
        .map(|version| (version.object_id.clone(), version.version.clone()))
        .collect()
}
