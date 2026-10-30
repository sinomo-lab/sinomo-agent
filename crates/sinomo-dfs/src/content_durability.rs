//! Data-write durability survives metadata-only generations and retry GC.

use crate::{
    DfsContentDurability, DfsMetadataBackend, DfsObjectKind, DfsObjectMetadata,
    DfsObjectVersionRecord, DfsReplicaReceiptRecord, DfsReplicationPolicy, DfsWriteIntentRecord,
    DfsWriteIntentStatus, DistributedWriteIntent, MetadataBackendError,
};

fn current_content_version_from_backend(
    backend: &dyn DfsMetadataBackend,
    object: &DfsObjectMetadata,
) -> Result<Option<DfsObjectVersionRecord>, MetadataBackendError> {
    let current = backend
        .get_object(&object.object_id)?
        .ok_or_else(|| current_durability_error("current object is unavailable"))?;
    if current.file_system_id != object.file_system_id
        || current.object_id != object.object_id
        || current.version != object.version
        || current.kind != object.kind
        || current.path != object.path
        || current.policy != object.policy
        || current.owner_node_id != object.owner_node_id
        || current.tombstone_provenance != object.tombstone_provenance
        || current.content_hash != object.content_hash
        || current.content_size_bytes != object.content_size_bytes
        || current.last_modified_by_event_id != object.last_modified_by_event_id
    {
        return Err(current_durability_error("current object identity changed"));
    }
    if !matches!(
        object.kind,
        DfsObjectKind::File | DfsObjectKind::Symlink | DfsObjectKind::Artifact
    ) || object.content_hash.is_none()
    {
        return Ok(None);
    }
    if object.tombstone_provenance.is_some() {
        return Err(current_durability_error(
            "current content object is tombstoned",
        ));
    }
    let version = backend
        .get_version(&object.object_id, &object.version)?
        .ok_or_else(|| current_durability_error("current content version is unavailable"))?;
    validate_current_content_version(object, &version)?;
    Ok(Some(version))
}

/// Read the current content contract without turning retry history into repair
/// ownership. Managed placement supersedes the original write's replica plan;
/// the original durability record and data-write generation remain unchanged.
pub(crate) fn current_content_durability_from_backend(
    backend: &dyn DfsMetadataBackend,
    object: &DfsObjectMetadata,
) -> Result<Option<DfsContentDurability>, MetadataBackendError> {
    let Some(version) = current_content_version_from_backend(backend, object)? else {
        return Ok(None);
    };
    let mut managed = None;
    crate::metadata_pages::walk_metadata_pages(|cursor| {
        let page = backend.intents_for_object_page(
            &object.object_id,
            cursor,
            crate::metadata_limits::OBJECT_LOCAL_METADATA_PAGE_SIZE,
        )?;
        for intent in page.intents {
            if !intent.publication.is_current()
                || intent.status != DfsWriteIntentStatus::Committed
                || !intent.intent_id.starts_with("placement:")
                || backend
                    .get_intent_receipt(&object.object_id, &intent.intent_id)?
                    .is_some()
            {
                continue;
            }
            select_current_managed_owner(&mut managed, &intent, object, &version)?;
        }
        Ok(page.next_cursor)
    })?;
    recorded_current_durability(&version, managed).map(Some)
}

/// Observe the exact full owner named by an already selected placement job.
/// This DATA lookup supplies no job/permit authority and never revives a receipt.
pub(crate) fn current_placement_durability_for_intent_from_backend(
    backend: &dyn DfsMetadataBackend,
    object: &DfsObjectMetadata,
    intent_id: &str,
) -> Result<Option<DfsContentDurability>, MetadataBackendError> {
    let Some(version) = current_content_version_from_backend(backend, object)? else {
        return Ok(None);
    };
    let Some(intent) = backend.get_intent(&object.object_id, intent_id)? else {
        return Ok(None);
    };
    if backend
        .get_intent_receipt(&object.object_id, intent_id)?
        .is_some()
    {
        return Ok(None);
    }
    if intent.intent_id != intent_id || !current_managed_intent_matches(&intent, object, &version)?
    {
        return Err(current_durability_error(
            "exact placement owner does not bind current content",
        ));
    }
    recorded_current_durability(
        &version,
        Some(managed_content_durability(&intent, &version)?),
    )
    .map(Some)
}

fn recorded_current_durability(
    version: &DfsObjectVersionRecord,
    managed: Option<DfsContentDurability>,
) -> Result<DfsContentDurability, MetadataBackendError> {
    let durability = managed
        .or_else(|| version.content_durability.as_deref().cloned())
        .ok_or_else(|| current_durability_error("written content has no recorded durability"))?;
    crate::distributed::validate_replication_plan(
        &durability.desired_replica_node_ids,
        &durability.policy,
    )
    .map_err(|error| {
        current_durability_error(&format!("invalid recorded replica plan: {error}"))
    })?;
    Ok(durability)
}

fn select_current_managed_owner(
    selected: &mut Option<DfsContentDurability>,
    intent: &DfsWriteIntentRecord,
    object: &DfsObjectMetadata,
    version: &DfsObjectVersionRecord,
) -> Result<(), MetadataBackendError> {
    if !current_managed_intent_matches(intent, object, version)? {
        return Ok(());
    }
    if selected.is_some() {
        return Err(current_durability_error(
            "multiple current placement owners",
        ));
    }
    *selected = Some(managed_content_durability(intent, version)?);
    Ok(())
}

fn managed_content_durability(
    intent: &DfsWriteIntentRecord,
    version: &DfsObjectVersionRecord,
) -> Result<DfsContentDurability, MetadataBackendError> {
    let original = version.content_durability.as_ref().ok_or_else(|| {
        current_durability_error("managed content has no recorded data-write generation")
    })?;
    Ok(DfsContentDurability {
        source_version: original.source_version.clone(),
        coordinator_node_id: intent.coordinator_node_id.clone(),
        owner_epoch: intent.owner_epoch,
        desired_replica_node_ids: intent.desired_replica_node_ids.clone(),
        policy: intent.policy.clone(),
    })
}

fn current_durability_error(detail: &str) -> MetadataBackendError {
    MetadataBackendError::Serde(format!(
        "current content durability is unavailable: {detail}"
    ))
}

fn validate_current_content_version(
    object: &DfsObjectMetadata,
    version: &DfsObjectVersionRecord,
) -> Result<(), MetadataBackendError> {
    if !version.visible
        || version.file_system_id != object.file_system_id
        || version.object_id != object.object_id
        || version.version != object.version
        || version.content_hash != object.content_hash
        || version.size_bytes.is_none()
        || version.size_bytes != object.content_size_bytes
        || version.created_by_event_id != object.last_modified_by_event_id
    {
        return Err(current_durability_error(
            "current version does not bind the object's bytes",
        ));
    }
    Ok(())
}

fn current_managed_intent_matches(
    intent: &DfsWriteIntentRecord,
    object: &DfsObjectMetadata,
    current: &DfsObjectVersionRecord,
) -> Result<bool, MetadataBackendError> {
    if intent.file_system_id != object.file_system_id
        || intent.object_id != object.object_id
        || !crate::placement::current_placement_repair_owner_matches_identity(intent)
    {
        return Err(current_durability_error(
            "placement owner identity is invalid",
        ));
    }
    if intent.content_hash != object.content_hash {
        return if intent.target_version == object.version {
            Err(current_durability_error(
                "current placement owner names different bytes",
            ))
        } else {
            Ok(false)
        };
    }
    if let Some(captured) = &intent.object_metadata
        && (captured.file_system_id != intent.file_system_id
            || captured.object_id != intent.object_id
            || captured.version != intent.target_version
            || captured.content_hash != intent.content_hash
            || captured.content_size_bytes != object.content_size_bytes
            || captured.kind != object.kind
            || captured.tombstone_provenance.is_some())
    {
        return Err(current_durability_error(
            "placement owner captured a different object",
        ));
    }
    if intent.target_version == current.version
        || current
            .content_durability
            .as_ref()
            .is_some_and(|plan| plan.source_version == intent.target_version)
    {
        return Ok(true);
    }
    // Metadata-only successors can outlive the source version row. Use the
    // full owner's existing captured object to prove the same content lineage.
    let captured = intent.object_metadata.as_ref().ok_or_else(|| {
        current_durability_error("placement owner has no current content lineage")
    })?;
    let source =
        crate::metadata_records::object_version_record(captured, captured.content_size_bytes);
    Ok(same_content_lineage(current, &source))
}

pub(crate) fn from_intent(intent: &DistributedWriteIntent) -> Box<DfsContentDurability> {
    Box::new(DfsContentDurability {
        source_version: intent.target_version.clone(),
        coordinator_node_id: intent.requesting_node_id.clone(),
        owner_epoch: intent.owner_epoch,
        desired_replica_node_ids: intent.desired_replica_nodes.clone(),
        policy: intent.policy.clone(),
    })
}

/// Match an actual receipt to the complete target byte identity. The receipt's
/// owner is its metadata publisher; the recorded plan's coordinator is the
/// original requester. Neither node identity substitutes for this DATA join.
/// Callers retain their existing committed-plan and publication provenance checks.
pub(crate) fn receipt_matches_target_version(
    receipt: &DfsReplicaReceiptRecord,
    target: &DfsObjectVersionRecord,
    replica_node_id: &sinomo_core::node::NodeId,
) -> bool {
    target.size_bytes.is_some()
        && target.content_hash.as_ref().is_some_and(|hash| {
            receipt.replica_node_id == *replica_node_id
                && receipt.receipt.file_system_id == target.file_system_id
                && receipt.receipt.object_id == target.object_id
                && receipt.receipt.committed_version == target.version
                && receipt.content_hash.as_ref() == Some(hash)
        })
}

/// The local-eventual batch contract is exactly one durable caller copy.
/// Its receipt does not carry a distributed write-intent authority epoch.
pub(crate) fn from_local_receipt(receipt: &DfsReplicaReceiptRecord) -> Box<DfsContentDurability> {
    Box::new(DfsContentDurability {
        source_version: receipt.receipt.committed_version.clone(),
        coordinator_node_id: receipt.replica_node_id.clone(),
        owner_epoch: 0,
        desired_replica_node_ids: vec![receipt.replica_node_id.clone()],
        policy: DfsReplicationPolicy::new(1, 1).expect("single-replica batch contract"),
    })
}

/// A shared nonempty modification event proves a
/// metadata-only lineage; equal bytes from unrelated writes do not.
pub(crate) fn same_content_lineage(
    target: &DfsObjectVersionRecord,
    source: &DfsObjectVersionRecord,
) -> bool {
    target.file_system_id == source.file_system_id
        && target.object_id == source.object_id
        && target.content_hash.is_some()
        && target.content_hash == source.content_hash
        && target.size_bytes.is_some()
        && target.size_bytes == source.size_bytes
        && (target.version == source.version
            || (target.created_by_event_id.is_some()
                && target.created_by_event_id == source.created_by_event_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    // DATA-only selector inputs; these rows never authorize a publication.
    fn current_fixture() -> (
        DfsObjectMetadata,
        DfsObjectVersionRecord,
        DfsWriteIntentRecord,
    ) {
        let node =
            sinomo_core::node::NodeId::from_legacy_named("current-durability-owner").unwrap();
        let replica =
            sinomo_core::node::NodeId::from_legacy_named("current-durability-replica").unwrap();
        let mut object = crate::posix_object::new_posix_metadata_object(
            &node,
            "/users/current-durability",
            DfsObjectKind::File,
        );
        object.policy = sinomo_core::DfsObjectPolicy::ReplicatedEventual;
        object.content_hash = Some("blake3:current-durability".into());
        object.content_size_bytes = Some(7);
        object.last_modified_by_event_id = Some(sinomo_core::events::EventId::generate());
        let mut version = crate::metadata_records::object_version_record(&object, Some(7));
        version.content_durability = Some(Box::new(DfsContentDurability {
            source_version: object.version.clone(),
            coordinator_node_id: node.clone(),
            owner_epoch: 7,
            desired_replica_node_ids: vec![node.clone(), replica],
            policy: DfsReplicationPolicy::new(2, 2).unwrap(),
        }));
        let intent = DistributedWriteIntent {
            publication: Default::default(),
            intent_id: format!("placement:current:{}:{}", object.object_id, object.version),
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            base_version: Some(object.version.clone()),
            target_version: object.version.clone(),
            requesting_node_id: node.clone(),
            operation_id: None,
            operation_order_token: None,
            owner_epoch: 7,
            desired_replica_nodes: vec![node],
            policy: DfsReplicationPolicy::new(1, 1).unwrap(),
            touch_parent_on_create: false,
            create_predecessor_object_id: None,
            state: crate::DfsWriteIntentState::Committed,
        };
        let intent = crate::intent::write_intent_record(
            &intent,
            object.content_hash.as_deref(),
            Some(object.clone()),
        );
        (object, version, intent)
    }

    #[test]
    fn current_managed_owner_precedes_but_does_not_rewrite_original_durability() {
        let (object, version, intent) = current_fixture();
        let original = version.content_durability.clone().unwrap();
        let mut selected = None;
        select_current_managed_owner(&mut selected, &intent, &object, &version).unwrap();
        let actual = recorded_current_durability(&version, selected).unwrap();
        assert_eq!(
            actual.desired_replica_node_ids,
            intent.desired_replica_node_ids
        );
        assert_eq!(actual.policy, intent.policy);
        assert_eq!(actual.source_version, intent.target_version);
        assert_eq!(version.content_durability, Some(original));
    }

    #[test]
    fn current_durability_missing_invalid_and_ambiguous_inputs_fail_closed() {
        let (object, mut version, intent) = current_fixture();
        let original = version.content_durability.take();
        assert!(recorded_current_durability(&version, None).is_err());
        assert!(managed_content_durability(&intent, &version).is_err());
        version.content_durability = original;
        let mut selected = None;
        select_current_managed_owner(&mut selected, &intent, &object, &version).unwrap();
        let mut conflicting = intent.clone();
        conflicting.intent_id = format!("placement:other:{}:{}", object.object_id, object.version);
        assert!(
            select_current_managed_owner(&mut selected, &conflicting, &object, &version).is_err()
        );
        let mut invalid = selected.unwrap();
        invalid.desired_replica_node_ids.clear();
        assert!(recorded_current_durability(&version, Some(invalid)).is_err());
    }

    #[test]
    fn current_version_and_managed_owner_reject_foreign_or_malformed_identity() {
        let (object, version, intent) = current_fixture();
        let mut versions = Vec::new();
        let mut wrong = version.clone();
        wrong.file_system_id =
            crate::DfsFileSystemId::from_legacy_named("foreign-durability-fs").unwrap();
        versions.push(wrong);
        let mut wrong = version.clone();
        wrong.object_id = crate::DfsObjectId::generate();
        versions.push(wrong);
        let mut wrong = version.clone();
        wrong.version = crate::DfsVersion::generate();
        versions.push(wrong);
        let mut wrong = version.clone();
        wrong.content_hash = Some("blake3:foreign".into());
        versions.push(wrong);
        let mut wrong = version.clone();
        wrong.size_bytes = Some(8);
        versions.push(wrong);
        let mut wrong = version.clone();
        wrong.visible = false;
        versions.push(wrong);
        for wrong in versions {
            assert!(validate_current_content_version(&object, &wrong).is_err());
        }
        let mut wrong = intent.clone();
        wrong.file_system_id =
            crate::DfsFileSystemId::from_legacy_named("foreign-owner-fs").unwrap();
        assert!(current_managed_intent_matches(&wrong, &object, &version).is_err());
        let mut wrong = intent.clone();
        wrong.object_id = crate::DfsObjectId::generate();
        assert!(current_managed_intent_matches(&wrong, &object, &version).is_err());
        let mut wrong = intent.clone();
        wrong.content_hash = Some("blake3:foreign".into());
        assert!(current_managed_intent_matches(&wrong, &object, &version).is_err());
        let mut wrong = intent.clone();
        wrong.object_metadata.as_mut().unwrap().content_size_bytes = Some(8);
        assert!(current_managed_intent_matches(&wrong, &object, &version).is_err());
        let mut wrong = intent.clone();
        wrong.object_metadata.as_mut().unwrap().kind = DfsObjectKind::Directory;
        assert!(current_managed_intent_matches(&wrong, &object, &version).is_err());
    }

    #[test]
    fn metadata_only_current_durability_does_not_require_the_source_version_row() {
        let (mut object, mut version, mut intent) = current_fixture();
        let original = version.content_durability.clone().unwrap();
        object.version = crate::DfsVersion::generate();
        version.version = object.version.clone();
        validate_current_content_version(&object, &version).unwrap();
        intent.object_metadata = None;
        assert!(current_managed_intent_matches(&intent, &object, &version).unwrap());
        assert_eq!(
            recorded_current_durability(&version, None).unwrap(),
            *original
        );

        // Placement can itself have been captured after a metadata-only edit.
        let mut captured = object.clone();
        captured.version = crate::DfsVersion::generate();
        intent.target_version = captured.version.clone();
        intent.base_version = Some(captured.version.clone());
        intent.intent_id = format!(
            "placement:metadata:{}:{}",
            object.object_id, captured.version
        );
        intent.object_metadata = Some(captured);
        assert!(current_managed_intent_matches(&intent, &object, &version).unwrap());
        assert_eq!(
            managed_content_durability(&intent, &version)
                .unwrap()
                .source_version,
            original.source_version,
            "a placement metadata generation is not the original data-write generation"
        );
        intent
            .object_metadata
            .as_mut()
            .unwrap()
            .last_modified_by_event_id = Some(sinomo_core::events::EventId::generate());
        assert!(!current_managed_intent_matches(&intent, &object, &version).unwrap());
        intent.object_metadata = None;
        assert!(current_managed_intent_matches(&intent, &object, &version).is_err());
    }
}
