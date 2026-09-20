//! Data-write durability survives metadata-only generations and retry GC.

use crate::{
    DfsContentDurability, DfsMetadataBackend, DfsObjectVersionRecord, DfsReplicaReceiptRecord,
    DfsReplicationPolicy, DfsWriteIntentRecord, DfsWriteIntentStatus, DistributedWriteIntent,
    MetadataBackendError,
};

pub(crate) fn from_intent(intent: &DistributedWriteIntent) -> Box<DfsContentDurability> {
    Box::new(DfsContentDurability {
        current_policy_granted_at_unix_nanos: None,
        source_version: intent.target_version.clone(),
        coordinator_node_id: intent.requesting_node_id.clone(),
        owner_epoch: intent.owner_epoch,
        desired_replica_node_ids: intent.desired_replica_nodes.clone(),
        policy: intent.policy.clone(),
    })
}

pub(crate) fn from_record(intent: &DfsWriteIntentRecord) -> Box<DfsContentDurability> {
    Box::new(DfsContentDurability {
        current_policy_granted_at_unix_nanos: None,
        source_version: intent.target_version.clone(),
        coordinator_node_id: intent.coordinator_node_id.clone(),
        owner_epoch: intent.owner_epoch,
        desired_replica_node_ids: intent.desired_replica_node_ids.clone(),
        policy: intent.policy.clone(),
    })
}

/// The local-eventual batch contract is exactly one durable caller copy.
/// Its receipt does not carry a distributed write-intent authority epoch.
pub(crate) fn from_local_receipt(receipt: &DfsReplicaReceiptRecord) -> Box<DfsContentDurability> {
    Box::new(DfsContentDurability {
        current_policy_granted_at_unix_nanos: None,
        source_version: receipt.receipt.committed_version.clone(),
        coordinator_node_id: receipt.replica_node_id.clone(),
        owner_epoch: 0,
        desired_replica_node_ids: vec![receipt.replica_node_id.clone()],
        policy: DfsReplicationPolicy::new(1, 1).expect("single-replica batch contract"),
    })
}

/// Resolve legacy metadata without deriving a replication requirement from the
/// number of surviving copies. A shared nonempty modification event proves a
/// metadata-only lineage; equal bytes from unrelated writes do not.
struct LegacyPlanRecovery<'a> {
    target: &'a DfsObjectVersionRecord,
    plan: Option<Box<DfsContentDurability>>,
    maintenance_plan: Option<Box<DfsContentDurability>>,
}

impl<'a> LegacyPlanRecovery<'a> {
    fn new(target: &'a DfsObjectVersionRecord) -> Self {
        Self {
            target,
            plan: target.content_durability.clone(),
            maintenance_plan: None,
        }
    }

    fn same_lineage(&self, source: &DfsObjectVersionRecord) -> bool {
        self.target.file_system_id == source.file_system_id
            && self.target.object_id == source.object_id
            && self.target.content_hash.is_some()
            && self.target.content_hash == source.content_hash
            && self.target.size_bytes.is_some()
            && self.target.size_bytes == source.size_bytes
            && (self.target.version == source.version
                || (self.target.created_by_event_id.is_some()
                    && self.target.created_by_event_id == source.created_by_event_id))
    }

    fn consider_version(
        &mut self,
        source: &DfsObjectVersionRecord,
    ) -> Result<(), MetadataBackendError> {
        if self.same_lineage(source)
            && let Some(plan) = &source.content_durability
        {
            merge_plan(&mut self.plan, plan.clone())?;
        }
        Ok(())
    }

    fn consider_intent(
        &mut self,
        intent: &DfsWriteIntentRecord,
        source: Option<&DfsObjectVersionRecord>,
    ) -> Result<(), MetadataBackendError> {
        if intent.status != DfsWriteIntentStatus::Committed
            || intent.file_system_id != self.target.file_system_id
            || intent.object_id != self.target.object_id
            || intent.content_hash.is_none()
            || intent.content_hash != self.target.content_hash
            || intent.intent_id.starts_with("placement:")
        {
            return Ok(());
        }
        if intent.publication.is_retained_repair() {
            if intent.target_version == self.target.version {
                merge_plan(&mut self.maintenance_plan, from_record(intent))?;
            }
            return Ok(());
        }
        let snapshot = intent
            .object_metadata
            .as_ref()
            .filter(|object| object.version == intent.target_version)
            .map(|object| crate::object_version_record(object, object.content_size_bytes));
        let source = source.or(snapshot.as_ref());
        if intent.target_version == self.target.version
            || source.is_some_and(|source| {
                source.version == intent.target_version && self.same_lineage(source)
            })
        {
            merge_plan(&mut self.plan, from_record(intent))?;
        }
        Ok(())
    }

    fn finish(self) -> Result<Option<Box<DfsContentDurability>>, MetadataBackendError> {
        if let (Some(plan), Some(owner)) = (&self.plan, &self.maintenance_plan)
            && (plan.policy != owner.policy
                || plan.desired_replica_node_ids != owner.desired_replica_node_ids)
        {
            return Err(MetadataBackendError::Serde(
                "legacy content has conflicting recorded durability plans".into(),
            ));
        }
        Ok(self.plan.or(self.maintenance_plan))
    }
}

fn merge_plan(
    recovered: &mut Option<Box<DfsContentDurability>>,
    mut candidate: Box<DfsContentDurability>,
) -> Result<(), MetadataBackendError> {
    crate::validate_replication_plan(&candidate.desired_replica_node_ids, &candidate.policy)
        .map_err(|error| MetadataBackendError::Serde(error.to_string()))?;
    candidate.desired_replica_node_ids.sort();
    if recovered
        .as_ref()
        .is_some_and(|previous| previous != &candidate)
    {
        return Err(MetadataBackendError::Serde(
            "legacy content has conflicting recorded durability plans".into(),
        ));
    }
    *recovered = Some(candidate);
    Ok(())
}

pub(crate) fn recover_from_backend(
    target: &DfsObjectVersionRecord,
    backend: &dyn DfsMetadataBackend,
) -> Result<Option<Box<DfsContentDurability>>, MetadataBackendError> {
    let mut recovery = LegacyPlanRecovery::new(target);
    crate::metadata_pages::walk_metadata_pages(|cursor| {
        let page = backend.versions_for_object_page(&target.object_id, cursor, 128)?;
        for version in page.versions {
            recovery.consider_version(&version)?;
        }
        Ok(page.next_cursor)
    })?;
    crate::metadata_pages::walk_metadata_pages(|cursor| {
        // This object-local view includes compact retry receipts as evidence.
        let page = backend.intents_for_object_page(&target.object_id, cursor, 128)?;
        for intent in page.intents {
            let source = backend.get_version(&target.object_id, &intent.target_version)?;
            recovery.consider_intent(&intent, source.as_ref())?;
        }
        Ok(page.next_cursor)
    })?;
    recovery.finish()
}
