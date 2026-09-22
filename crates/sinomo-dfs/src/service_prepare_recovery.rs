//! Recovery of hidden metadata prepares after shard-authority movement.
//!
//! Visibility still goes through the normal canonical authorization path.
//! This module only adopts durable, non-visible work after proving that the
//! local node owns the exact same shard at a newer (or equal) certified epoch.

use std::collections::{BTreeMap, BTreeSet};

use time::OffsetDateTime;

use crate::*;

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub(crate) fn has_recoverable_standalone_namespace_prepare_on_shard(
        &self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<bool, DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(false);
        };
        let write_prepare_ids = backend
            .all_intents()?
            .into_iter()
            .map(|intent| format!("namespace:create:{}", intent.intent_id))
            .collect::<BTreeSet<_>>();
        Ok(backend
            .list_prepared_namespace_prepares()?
            .iter()
            .any(|prepare| {
                prepare.shard_id == *shard_id && !write_prepare_ids.contains(&prepare.operation_id)
            }))
    }

    pub(crate) fn certified_local_recovery_shard(
        &mut self,
        object: &DfsObjectMetadata,
        durable_shard_id: &DfsMetadataShardId,
        durable_owner_epoch: u64,
    ) -> Result<Option<DfsMetadataShardConfig>, DfsError> {
        if self.control_plane.is_none() {
            return Ok(None);
        }
        let certified_epoch = match self.local_metadata_owner_epoch_for_object(object) {
            Ok(epoch) => epoch,
            Err(DfsError::StaleOwnerEpoch { .. }) => return Ok(None),
            Err(error) => return Err(error),
        };
        let Some(shard) = self.select_metadata_shard_for_object(object)? else {
            return Ok(None);
        };
        if shard.primary_node_id != self.node_id
            || shard.shard_id != durable_shard_id.0
            || shard.epoch != certified_epoch
            || shard.epoch < durable_owner_epoch
        {
            return Ok(None);
        }
        Ok(Some(shard))
    }

    fn namespace_prepare_authority_object(
        &self,
        prepare: &DfsNamespacePrepareRecord,
    ) -> Result<Option<DfsObjectMetadata>, DfsError> {
        if let Some(object) = prepare.transaction.objects.first() {
            return Ok(Some(object.clone()));
        }
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(None);
        };
        for expected in &prepare.expected_object_versions {
            if let Some(object) = backend.get_object(&expected.object_id)? {
                return Ok(Some(object));
            }
        }
        Ok(None)
    }

    /// Complete standalone hidden namespace prepares before object hydration.
    /// Create prepares coupled to write intents are deliberately left for the
    /// write-intent recovery path so receipt threshold evidence remains part
    /// of the visibility decision.
    pub(crate) fn recover_durable_namespace_prepares(&mut self) -> Result<(), DfsError> {
        let completed = self.recover_durable_namespace_prepares_until_cancelled(&|| false)?;
        debug_assert!(
            completed,
            "non-cancellable DFS namespace-prepare recovery was cancelled"
        );
        Ok(())
    }

    pub(crate) fn recover_durable_namespace_prepares_until_cancelled(
        &mut self,
        should_cancel: &(dyn Fn() -> bool + Sync),
    ) -> Result<bool, DfsError> {
        if self.control_plane.is_none() {
            return Ok(true);
        }
        if should_cancel() {
            return Ok(false);
        }
        let backend = self
            .metadata_backend
            .as_ref()
            .expect("metadata backend exists during hydration");
        let mut write_prepare_ids = BTreeSet::new();
        let mut aborted_write_prepare_ids = BTreeSet::new();
        let mut intent_cursor = None;
        loop {
            if should_cancel() {
                return Ok(false);
            }
            let page = backend
                .write_intents_page(intent_cursor.as_ref(), DFS_METADATA_OBJECT_INTENT_PAGE_SIZE)?;
            for intent in page.intents {
                let prepare_id = format!("namespace:create:{}", intent.intent_id);
                if intent.status == DfsWriteIntentStatus::Aborted {
                    aborted_write_prepare_ids.insert(prepare_id.clone());
                }
                write_prepare_ids.insert(prepare_id);
            }
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            intent_cursor = Some(next_cursor);
        }
        if should_cancel() {
            return Ok(false);
        }
        let prepared_namespace_prepares = backend.list_prepared_namespace_prepares()?;
        let mut aborted_write_prepares = prepared_namespace_prepares
            .iter()
            .filter(|prepare| aborted_write_prepare_ids.contains(&prepare.operation_id))
            .cloned()
            .collect::<Vec<_>>();
        aborted_write_prepares.sort_by(|left, right| left.operation_id.cmp(&right.operation_id));
        for prepare in aborted_write_prepares {
            if should_cancel() {
                return Ok(false);
            }
            let Some(authority_object) = self.namespace_prepare_authority_object(&prepare)? else {
                continue;
            };
            if self
                .certified_local_recovery_shard(
                    &authority_object,
                    &prepare.shard_id,
                    prepare.owner_epoch,
                )?
                .is_none()
            {
                continue;
            }
            self.metadata_backend
                .as_mut()
                .expect("metadata backend exists during hydration")
                .abort_prepared_namespace_transaction(
                    &prepare.operation_id,
                    &prepare.operation_digest,
                    prepare.owner_epoch,
                )?;
        }
        let mut prepares = prepared_namespace_prepares
            .into_iter()
            .filter(|prepare| !write_prepare_ids.contains(&prepare.operation_id))
            .collect::<Vec<_>>();
        prepares.sort_by(|left, right| left.operation_id.cmp(&right.operation_id));

        for prepare in prepares {
            if should_cancel() {
                return Ok(false);
            }
            self.recover_one_durable_namespace_prepare(prepare)?;
        }
        Ok(!should_cancel())
    }

    /// Finish request-owned prepares before replaying their namespace command.
    /// The durable prepare already contains the exact transaction and completed
    /// request record. Replanning a mkdir or rename can regenerate timestamps
    /// or versions and must not be mistaken for an idempotent retry.
    pub(crate) fn recover_durable_namespace_prepares_for_requests(
        &mut self,
        request_keys: &BTreeSet<(NodeId, String)>,
        max_prepares: usize,
    ) -> Result<usize, DfsError> {
        if request_keys.is_empty() || max_prepares == 0 || self.control_plane.is_none() {
            return Ok(0);
        }
        let backend = self
            .metadata_backend
            .as_ref()
            .expect("metadata backend exists during request recovery");
        let mut prepares = Vec::new();
        for (requester_node_id, request_id) in request_keys {
            let Some(prepare) = backend
                .get_prepared_namespace_prepare_for_request(requester_node_id, request_id)?
            else {
                continue;
            };
            prepares.push(prepare);
        }
        prepares.sort_by(|left, right| left.operation_id.cmp(&right.operation_id));

        let mut recovered = 0usize;
        for prepare in prepares.into_iter().take(max_prepares) {
            if self.recover_one_durable_namespace_prepare(prepare)? {
                recovered = recovered.saturating_add(1);
            }
        }
        Ok(recovered)
    }

    fn recover_one_durable_namespace_prepare(
        &mut self,
        prepare: DfsNamespacePrepareRecord,
    ) -> Result<bool, DfsError> {
        let Some(authority_object) = self.namespace_prepare_authority_object(&prepare)? else {
            return Ok(false);
        };
        let Some(shard) = self.certified_local_recovery_shard(
            &authority_object,
            &prepare.shard_id,
            prepare.owner_epoch,
        )?
        else {
            return Ok(false);
        };
        let (prepared, prepare_entry) = if shard.epoch == prepare.owner_epoch {
            let entry = self
                .metadata_backend
                .as_mut()
                .expect("metadata backend exists during hydration")
                .prepare_namespace_transaction(&prepare)?;
            (prepare, entry)
        } else {
            let mut rebound = prepare.clone();
            rebound.owner_epoch = shard.epoch;
            rebound.status = DfsNamespacePrepareStatus::Prepared;
            rebound.operation_digest =
                crate::service_replica::metadata_namespace_prepare_payload_digest(&rebound)?;
            let entry = self
                .metadata_backend
                .as_mut()
                .expect("metadata backend exists during hydration")
                .rebind_prepared_namespace_transaction(
                    &prepare.operation_id,
                    &prepare.operation_digest,
                    prepare.owner_epoch,
                    &rebound,
                )?;
            (rebound, entry)
        };
        if prepare_entry.shard_id != prepared.shard_id {
            return Err(DfsError::Metadata {
                message: format!(
                    "recovered namespace prepare {} was journaled on shard {}, expected {}",
                    prepared.operation_id, prepare_entry.shard_id.0, prepared.shard_id.0
                ),
            });
        }
        let prepare_sequence = prepare_entry.sequence;
        let prepared_batch = DfsMetadataJournalBatch {
            shard_id: prepared.shard_id.clone(),
            entries: vec![prepare_entry],
            next_after_sequence: prepare_sequence,
        };
        let operation = crate::service_replica::metadata_authority_operation_for_namespace_prepare(
            &prepared,
            prepare_sequence,
        )?;
        let authorization =
            self.authorize_metadata_shard_operation(&shard, &operation, &prepared_batch)?;
        let publication_guard =
            self.validate_canonical_authorization(&authorization, &operation)?;
        self.metadata_backend
            .as_mut()
            .expect("metadata backend exists during hydration")
            .commit_prepared_namespace_transaction_on_shard(
                &prepared.shard_id,
                &prepared.operation_id,
                &prepared.operation_digest,
                prepared.owner_epoch,
            )?;
        drop(publication_guard);
        Ok(true)
    }

    /// Map each durable write intent to the exact local metadata shard that
    /// contains it. A combined snapshot loses this provenance, so recovery
    /// intentionally uses shard-scoped snapshots. Historical copies can
    /// legitimately survive a shard move; duplicate keys are therefore
    /// omitted from this adoption map so recovery fails closed instead of
    /// assigning ambiguous work to whichever shard happened to scan last.
    #[cfg(test)]
    pub(crate) fn durable_write_intent_shards(
        &self,
    ) -> Result<BTreeMap<(DfsObjectId, String), DfsMetadataShardId>, DfsError> {
        let intent_shards = self
            .durable_write_intent_shards_until_cancelled(&|| false)?
            .expect("non-cancellable DFS write-intent shard scan was cancelled");
        Ok(intent_shards)
    }

    pub(crate) fn durable_write_intent_shards_until_cancelled(
        &self,
        should_cancel: &(dyn Fn() -> bool + Sync),
    ) -> Result<Option<BTreeMap<(DfsObjectId, String), DfsMetadataShardId>>, DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(Some(BTreeMap::new()));
        };
        if should_cancel() {
            return Ok(None);
        };
        let mut intent_shards = BTreeMap::new();
        let mut ambiguous_intents = BTreeSet::new();
        for status in backend.metadata_shard_statuses()? {
            if should_cancel() {
                return Ok(None);
            }
            let mut cursor = None;
            loop {
                if should_cancel() {
                    return Ok(None);
                }
                let Some(page) = backend.write_intents_on_shard_page(
                    &status.shard_id,
                    cursor.as_ref(),
                    DFS_METADATA_OBJECT_INTENT_PAGE_SIZE,
                )?
                else {
                    break;
                };
                for intent in page.intents {
                    if should_cancel() {
                        return Ok(None);
                    }
                    let key = (intent.object_id, intent.intent_id);
                    if ambiguous_intents.contains(&key) {
                        continue;
                    }
                    if intent_shards
                        .insert(key.clone(), status.shard_id.clone())
                        .is_some()
                    {
                        intent_shards.remove(&key);
                        ambiguous_intents.insert(key);
                    }
                }
                let Some(next_cursor) = page.next_cursor else {
                    break;
                };
                cursor = Some(next_cursor);
            }
        }
        Ok(Some(intent_shards))
    }

    /// Adopt an exact non-terminal write intent on the certified current
    /// primary. The durable operation payload and requester stay unchanged;
    /// only the shard-owner epoch is rebound. Any associated hidden create
    /// prepare is rebound first and remains invisible until the normal write
    /// commit path reauthorizes the exact new-epoch operation.
    pub(crate) fn adopt_prepared_write_intent_for_recovery(
        &mut self,
        intent: &mut DfsWriteIntentRecord,
        durable_shard_id: Option<&DfsMetadataShardId>,
    ) -> Result<bool, DfsError> {
        if !matches!(
            intent.status,
            DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
        ) {
            return Ok(false);
        }
        let Some(durable_shard_id) = durable_shard_id else {
            return Ok(false);
        };
        let Some(authority_object) = self
            .metadata
            .object(&intent.object_id)
            .cloned()
            .or_else(|| intent.object_metadata.clone())
        else {
            return Ok(false);
        };
        let Some(shard) = self.certified_local_recovery_shard(
            &authority_object,
            durable_shard_id,
            intent.owner_epoch,
        )?
        else {
            return Ok(false);
        };

        let create_prepare_id = format!("namespace:create:{}", intent.intent_id);
        if let Some(prepare) = self
            .metadata_backend
            .as_ref()
            .expect("metadata backend exists during hydration")
            .get_namespace_prepare(&create_prepare_id)?
        {
            if prepare.status != DfsNamespacePrepareStatus::Prepared
                || prepare.shard_id != *durable_shard_id
                || prepare.owner_epoch > shard.epoch
            {
                return Ok(false);
            }
            if prepare.owner_epoch < shard.epoch {
                let mut rebound = prepare.clone();
                rebound.owner_epoch = shard.epoch;
                rebound.status = DfsNamespacePrepareStatus::Prepared;
                rebound.operation_digest =
                    crate::service_replica::metadata_namespace_prepare_payload_digest(&rebound)?;
                self.metadata_backend
                    .as_mut()
                    .expect("metadata backend exists during hydration")
                    .rebind_prepared_namespace_transaction(
                        &prepare.operation_id,
                        &prepare.operation_digest,
                        prepare.owner_epoch,
                        &rebound,
                    )?;
            }
        }

        if intent.owner_epoch < shard.epoch {
            let expected = intent.clone();
            intent.owner_epoch = shard.epoch;
            intent.updated_at = Some(OffsetDateTime::now_utc());
            self.metadata_backend
                .as_mut()
                .expect("metadata backend exists during hydration")
                .rebind_prepared_write_intent(Some(&authority_object), &expected, intent)?;
        }
        Ok(intent.owner_epoch == shard.epoch)
    }

    /// Finalize a recovered create through the same batched canonical path
    /// that installed its hidden namespace prepare. The generic historical
    /// recovery path predates durable namespace prepares and would publish
    /// only object/version metadata, leaving the exact prepared parent touch
    /// stranded.
    pub(crate) fn apply_recovered_commit_ready_intent(
        &mut self,
        intent_id: &str,
    ) -> Result<(), DfsError> {
        let Some(intent) = self.coordinator.intent(intent_id).cloned() else {
            return Err(DfsError::Distributed {
                message: format!("unknown recovered intent: {intent_id}"),
            });
        };
        let has_durable_create_prepare = if intent.touch_parent_on_create {
            let operation_id = format!("namespace:create:{intent_id}");
            self.metadata_backend
                .as_ref()
                .map(|backend| backend.get_namespace_prepare(&operation_id))
                .transpose()?
                .flatten()
                .is_some()
        } else {
            false
        };
        if !has_durable_create_prepare {
            return self.apply_committed_intent(intent_id);
        }
        let pending = self
            .writes
            .pending_writes
            .get(intent_id)
            .cloned()
            .ok_or_else(|| DfsError::Distributed {
                message: format!("missing pending recovered create intent: {intent_id}"),
            })?;
        let base_object = self.current_read_object_metadata(&intent.object_id)?;
        self.commit_ready_intent_batched(
            &intent,
            &base_object,
            &pending.content_hash,
            pending.size_bytes,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::events::EventId;
    use sinomo_core::kv::InProcessKvStore;
    use sinomo_core::network::InProcessNetwork;
    use sinomo_core::{DfsObjectPolicy, DfsWriteReceipt};
    use std::time::Duration;

    fn node(value: &str) -> NodeId {
        NodeId::from_legacy_named(value).expect("node id")
    }

    fn version(value: &str) -> DfsVersion {
        DfsVersion::from_legacy_named(value).expect("version")
    }

    fn recovery_file_system(owner: &NodeId) -> DfsFileSystem {
        DfsFileSystem::sifs(owner.clone())
    }

    fn recovery_object(owner: &NodeId, version: DfsVersion) -> DfsObjectMetadata {
        DfsObjectMetadata {
            object_id: DfsObjectId::from_legacy_named("object_prepare_recovery").expect("object"),
            file_system_id: sifs_file_system_id(),
            path: "/prepare-recovery.txt".to_string(),
            kind: DfsObjectKind::File,
            tombstone_provenance: None,
            owner_node_id: owner.clone(),
            current_locations: vec![owner.clone()],
            content_hash: Some("blake3:old".to_string()),
            content_size_bytes: Some(3),
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 1,
            modified_at_unix_nanos: 1,
            created_at_unix_nanos: 1,
            changed_at_unix_nanos: 1,
            version,
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        }
    }

    fn recovery_shard(primary: &NodeId, epoch: u64) -> DfsMetadataShardConfig {
        DfsMetadataShardConfig {
            file_system_id: sifs_file_system_id(),
            shard_id: "root".to_string(),
            primary_node_id: primary.clone(),
            replica_node_ids: Vec::new(),
            epoch,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        }
    }

    fn recovery_control_plane(
        file_system: &DfsFileSystem,
        shard: &DfsMetadataShardConfig,
    ) -> InProcessKvStore {
        let mut kv = InProcessKvStore::new();
        let mut control = DfsControlPlaneStore::new(&mut kv);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(file_system))
            .expect("filesystem config");
        control.put_metadata_shard(shard).expect("metadata shard");
        kv
    }

    fn namespace_prepare_for_recovery(
        current: &DfsObjectMetadata,
        committed: DfsObjectMetadata,
        owner_epoch: u64,
    ) -> DfsNamespacePrepareRecord {
        let mut prepare = DfsNamespacePrepareRecord {
            operation_id: "namespace:prepare-recovery".to_string(),
            file_system_id: current.file_system_id.clone(),
            shard_id: DfsMetadataShardId::new("root"),
            owner_epoch,
            operation_digest: String::new(),
            expected_object_versions: vec![DfsNamespaceObjectVersionExpectation {
                object_id: current.object_id.clone(),
                expected_version: Some(current.version.clone()),
            }],
            expected_path_occupants: vec![DfsNamespacePathOccupantExpectation {
                path: current.path.clone(),
                expected_object_id: Some(current.object_id.clone()),
            }],
            content_receipts: Vec::new(),
            transaction: DfsNamespaceTransaction {
                file_version_pins: None,
                transaction_id: "transaction:prepare-recovery".to_string(),
                file_system_id: current.file_system_id.clone(),
                objects: vec![committed],
                versions: Vec::new(),
                advisory_lock_states: Vec::new(),
                created_at: OffsetDateTime::UNIX_EPOCH,
            },
            completed_request: None,
            completed_request_completion: None,
            status: DfsNamespacePrepareStatus::Prepared,
        };
        prepare.operation_digest =
            crate::service_replica::metadata_namespace_prepare_payload_digest(&prepare)
                .expect("prepare digest");
        prepare
    }

    #[test]
    fn hydration_rebinds_and_reauthorizes_namespace_prepare_on_current_primary() {
        let root = sinomo_testdir::TestDir::new("dfs-namespace-prepare-primary-recovery");
        let old_primary = node("node_prepare_recovery_old");
        let new_primary = node("node_prepare_recovery_new");
        let file_system = recovery_file_system(&old_primary);
        let current = recovery_object(&old_primary, version("version_prepare_recovery_1"));
        let mut committed = current.clone();
        committed.version = version("version_prepare_recovery_2");
        committed.changed_at_unix_nanos += 1;
        let operation = DfsNamespaceOperation::SetMode {
            path: current.path.clone(),
            mode: committed.mode,
        };
        let now = OffsetDateTime::UNIX_EPOCH;
        let pending_request = DfsNamespaceRequestRecord {
            file_system_id: current.file_system_id.clone(),
            requester_node_id: old_primary.clone(),
            request_id: "request-prepare-recovery".to_string(),
            operation_id: "req-prepare-recovery".to_string(),
            authority_shard_id: DfsMetadataShardId::new("root"),
            operation,
            command_digest: "blake3:prepare-recovery".to_string(),
            status: DfsNamespaceRequestStatus::Pending,
            result: None,
            posix_coordination_result: None,
            recovery_attempts: 0,
            last_recovery_error: None,
            created_at: now,
            updated_at: now,
        };
        let mut completed_request = pending_request.clone();
        completed_request.status = DfsNamespaceRequestStatus::Completed;
        completed_request.result = Some(DfsNamespaceResult::Applied);
        let mut prepare = namespace_prepare_for_recovery(&current, committed.clone(), 1);
        prepare.operation_id = format!(
            "namespace-request:{}:{}",
            completed_request.operation_id, prepare.transaction.transaction_id
        );
        prepare.completed_request = Some(completed_request.clone());
        prepare.operation_digest =
            crate::service_replica::metadata_namespace_prepare_payload_digest(&prepare)
                .expect("request-owned prepare digest");
        let metadata_root = root.join("metadata");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &metadata_root)
                .expect("backend");
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&current).expect("current object");
        backend
            .put_namespace_request_on_shard(&DfsMetadataShardId::new("root"), &pending_request)
            .expect("pending request");
        backend
            .prepare_namespace_transaction(&prepare)
            .expect("durable prepare");

        let shard = recovery_shard(&new_primary, 2);
        let control_plane = recovery_control_plane(&file_system, &shard);
        let network = InProcessNetwork::new();
        crate::metadata_backend::reset_metadata_export_table_snapshot_calls();
        let recovered = NetworkedDfsService::new(
            network.add_node(new_primary),
            LocalContentStore::new(root.join("content")).expect("content"),
        )
        .with_control_plane(control_plane)
        .with_metadata_backend(
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &metadata_root)
                .expect("reopen backend"),
        )
        .expect("current primary recovers prepare");
        assert_eq!(
            crate::metadata_backend::metadata_export_table_snapshot_calls(),
            0,
            "ordinary hydration must not export and fingerprint a full replica snapshot"
        );

        assert_eq!(
            recovered
                .metadata_backend()
                .expect("backend")
                .get_object(&current.object_id)
                .expect("object"),
            Some(committed)
        );
        assert!(
            recovered
                .metadata_backend()
                .expect("backend")
                .get_namespace_prepare(&prepare.operation_id)
                .expect("prepare")
                .is_none(),
            "recovery must compact the committed prepare body"
        );
        assert!(
            recovered
                .metadata_backend()
                .expect("backend")
                .journal_since(0)
                .expect("journal")
                .iter()
                .any(|entry| matches!(
                    &entry.mutation,
                    DfsMetadataMutation::CommitNamespacePrepare {
                        operation_id,
                        owner_epoch: 2,
                        ..
                    } if operation_id == &prepare.operation_id
                )),
            "the authoritative commit must retain the rebound owner epoch"
        );
        assert_eq!(
            recovered
                .metadata_backend()
                .expect("backend")
                .get_namespace_request(&old_primary, &pending_request.request_id)
                .expect("request")
                .expect("durable request"),
            completed_request
        );
    }

    #[test]
    fn pending_request_recovery_commits_its_existing_hidden_prepare() {
        let root = sinomo_testdir::TestDir::new("dfs-request-owned-prepare-recovery");
        let primary = node("node_request_owned_prepare_primary");
        let requester = node("node_request_owned_prepare_requester");
        let file_system = recovery_file_system(&primary);
        let current = recovery_object(&primary, version("version_request_owned_prepare_1"));
        let mut committed = current.clone();
        committed.version = version("version_request_owned_prepare_2");
        committed.mode = 0o600;
        committed.changed_at_unix_nanos += 1;
        let operation = DfsNamespaceOperation::SetMode {
            path: current.path.clone(),
            mode: committed.mode,
        };
        let now = OffsetDateTime::from_unix_timestamp(10).expect("request timestamp");
        let pending_request = DfsNamespaceRequestRecord {
            file_system_id: current.file_system_id.clone(),
            requester_node_id: requester.clone(),
            request_id: "request-owned-prepare".to_string(),
            operation_id: "req-request-owned-prepare".to_string(),
            authority_shard_id: DfsMetadataShardId::new("root"),
            operation,
            command_digest: "blake3:request-owned-prepare".to_string(),
            status: DfsNamespaceRequestStatus::Pending,
            result: None,
            posix_coordination_result: None,
            recovery_attempts: 0,
            last_recovery_error: None,
            created_at: now,
            updated_at: now,
        };
        let mut completed_request = pending_request.clone();
        completed_request.status = DfsNamespaceRequestStatus::Completed;
        completed_request.result = Some(DfsNamespaceResult::Applied);
        let mut prepare = namespace_prepare_for_recovery(&current, committed.clone(), 1);
        prepare.operation_id = format!(
            "namespace-request:{}:{}",
            pending_request.operation_id, prepare.transaction.transaction_id
        );
        prepare.completed_request = Some(completed_request.clone());
        prepare.operation_digest =
            crate::service_replica::metadata_namespace_prepare_payload_digest(&prepare)
                .expect("request-owned prepare digest");

        let metadata_root = root.join("metadata");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &metadata_root)
                .expect("backend");
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&current).expect("current object");
        let shard = recovery_shard(&primary, 1);
        let control_plane = recovery_control_plane(&file_system, &shard);
        let network = InProcessNetwork::new();
        let mut service = NetworkedDfsService::new(
            network.add_node(primary),
            LocalContentStore::new(root.join("content")).expect("content"),
        )
        .with_control_plane(control_plane)
        .with_metadata_backend(backend)
        .expect("attach metadata backend before request is admitted");

        let backend = service
            .metadata_backend
            .as_mut()
            .expect("attached metadata backend");
        for index in 0..5 {
            let mut foreign_request = pending_request.clone();
            foreign_request.requester_node_id =
                node(&format!("node_request_owned_prepare_foreign_{index}"));
            foreign_request.request_id = format!("request-owned-prepare-foreign-{index}");
            foreign_request.operation_id = format!("req-request-owned-prepare-foreign-{index}");
            foreign_request.authority_shard_id = DfsMetadataShardId::new("foreign");
            foreign_request.command_digest =
                format!("blake3:request-owned-prepare-foreign-{index}");
            let timestamp =
                OffsetDateTime::from_unix_timestamp(index).expect("foreign request timestamp");
            foreign_request.created_at = timestamp;
            foreign_request.updated_at = timestamp;
            backend
                .put_namespace_request_on_shard(&DfsMetadataShardId::new("root"), &foreign_request)
                .expect("foreign replica request");
        }
        backend
            .prepare_namespace_transaction(&prepare)
            .expect("fused durable admission and hidden prepare");

        let bounded = service
            .recover_pending_namespace_requests_bounded(1)
            .expect("bounded foreign request scan");
        assert_eq!(bounded.scanned, 0);
        assert_eq!(bounded.resolved, 0);
        assert_ne!(
            bounded.remaining, 0,
            "an unscanned continuation must keep recovery scheduled"
        );
        assert!(
            service.pending_namespace_request_recovery_cursor.is_some(),
            "the next pass must resume after the bounded four-page scan"
        );
        assert_eq!(
            service
                .metadata_backend()
                .expect("backend")
                .get_namespace_request(&requester, &pending_request.request_id)
                .expect("pending request after bounded scan")
                .expect("durable pending request")
                .status,
            DfsNamespaceRequestStatus::Pending
        );

        let report = service
            .recover_pending_namespace_requests_bounded(1)
            .expect("recover exact request-owned prepare");
        assert_eq!(report.scanned, 1);
        assert_eq!(report.resolved, 1);
        assert_ne!(
            report.remaining, 0,
            "the conservative continuation also accounts for older foreign replica requests"
        );
        assert!(report.failures.is_empty());
        assert_eq!(
            service
                .metadata_backend()
                .expect("backend")
                .get_object(&current.object_id)
                .expect("object"),
            Some(committed),
            "recovery must materialize the persisted transaction, not replan it"
        );
        assert!(
            service
                .metadata_backend()
                .expect("backend")
                .get_namespace_prepare(&prepare.operation_id)
                .expect("prepare")
                .is_none(),
            "request recovery must compact the committed prepare body"
        );
        assert!(
            service
                .metadata_backend()
                .expect("backend")
                .get_namespace_transaction(&prepare.transaction.transaction_id)
                .expect("transaction")
                .is_none(),
            "request recovery must compact the committed transaction body"
        );
        assert!(
            service
                .metadata_backend()
                .expect("backend")
                .journal_since(0)
                .expect("journal")
                .iter()
                .any(|entry| matches!(
                    &entry.mutation,
                    DfsMetadataMutation::CommitNamespacePrepare {
                        operation_id,
                        owner_epoch,
                        ..
                    } if operation_id == &prepare.operation_id
                        && *owner_epoch == prepare.owner_epoch
                )),
            "request recovery must retain the authoritative fenced commit"
        );
        assert_eq!(
            service
                .metadata_backend()
                .expect("backend")
                .get_namespace_request(&requester, &pending_request.request_id)
                .expect("request")
                .expect("durable request"),
            completed_request
        );
    }

    #[test]
    fn unfenced_handoff_yields_to_existing_request_owned_prepare() {
        let root = sinomo_testdir::TestDir::new("dfs-handoff-request-prepare-drain");
        let primary = node("node_handoff_prepare_primary");
        let requester = node("node_handoff_prepare_requester");
        let replica = node("node_handoff_prepare_replica");
        let file_system = recovery_file_system(&primary);
        let current = recovery_object(&primary, version("version_handoff_prepare_1"));
        let mut committed = current.clone();
        committed.version = version("version_handoff_prepare_2");
        committed.mode = 0o600;
        let now = OffsetDateTime::UNIX_EPOCH;
        let pending_request = DfsNamespaceRequestRecord {
            file_system_id: current.file_system_id.clone(),
            requester_node_id: requester.clone(),
            request_id: "request-handoff-prepare".to_string(),
            operation_id: "req-handoff-prepare".to_string(),
            authority_shard_id: DfsMetadataShardId::new("root"),
            operation: DfsNamespaceOperation::SetMode {
                path: current.path.clone(),
                mode: committed.mode,
            },
            command_digest: "blake3:handoff-prepare".to_string(),
            status: DfsNamespaceRequestStatus::Pending,
            result: None,
            posix_coordination_result: None,
            recovery_attempts: 0,
            last_recovery_error: None,
            created_at: now,
            updated_at: now,
        };
        let mut completed_request = pending_request.clone();
        completed_request.status = DfsNamespaceRequestStatus::Completed;
        completed_request.result = Some(DfsNamespaceResult::Applied);
        let mut prepare = namespace_prepare_for_recovery(&current, committed.clone(), 1);
        prepare.operation_id = format!(
            "namespace-request:{}:{}",
            pending_request.operation_id, prepare.transaction.transaction_id
        );
        prepare.completed_request = Some(completed_request.clone());
        prepare.operation_digest =
            crate::service_replica::metadata_namespace_prepare_payload_digest(&prepare)
                .expect("request-owned prepare digest");

        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root.join("metadata"))
                .expect("backend");
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&current).expect("current object");
        let shard = recovery_shard(&primary, 1);
        let control_plane = recovery_control_plane(&file_system, &shard);
        let network = InProcessNetwork::new();
        let mut service = NetworkedDfsService::new(
            network.add_node(primary),
            LocalContentStore::new(root.join("content")).expect("content"),
        )
        .with_control_plane(control_plane)
        .with_metadata_backend(backend)
        .expect("service");
        let backend = service.metadata_backend.as_mut().expect("metadata backend");
        backend
            .put_namespace_request_on_shard(&DfsMetadataShardId::new("root"), &pending_request)
            .expect("pending request");
        backend
            .prepare_namespace_transaction(&prepare)
            .expect("durable prepare");

        let desired = DfsMetadataShardConfig {
            replica_node_ids: vec![replica],
            epoch: shard.epoch + 1,
            ..shard.clone()
        };
        let handoff = DfsControlPlaneStore::new(
            service
                .control_plane
                .as_mut()
                .expect("control plane")
                .as_mut(),
        )
        .begin_metadata_shard_membership_handoff(&shard, &desired)
        .expect("begin handoff")
        .expect("pending handoff");

        assert_eq!(
            service
                .reconcile_pending_metadata_shard_membership_handoffs(
                    std::slice::from_ref(&shard),
                    Duration::from_millis(100),
                )
                .expect("cancel handoff and drain prepare"),
            0
        );
        let store = DfsControlPlaneStore::new(
            service
                .control_plane
                .as_mut()
                .expect("control plane")
                .as_mut(),
        );
        assert!(
            store
                .get_metadata_shard_membership_handoff(&shard.file_system_id, &shard.shard_id)
                .expect("handoff state")
                .is_none(),
            "the exact unfenced handoff must be withdrawn before recovery"
        );
        assert!(handoff.retiring_primary_fence.is_none());
        assert_eq!(
            service
                .metadata_backend()
                .expect("backend")
                .get_object(&current.object_id)
                .expect("object"),
            Some(committed)
        );
        assert_eq!(
            service
                .metadata_backend()
                .expect("backend")
                .get_namespace_request(&requester, &pending_request.request_id)
                .expect("request")
                .expect("durable request"),
            completed_request
        );
    }

    #[test]
    fn hydration_leaves_prepared_namespace_untouched_on_non_primary() {
        let root = sinomo_testdir::TestDir::new("dfs-namespace-prepare-non-primary-recovery");
        let old_primary = node("node_prepare_non_primary_old");
        let current_primary = node("node_prepare_non_primary_current");
        let observer = node("node_prepare_non_primary_observer");
        let file_system = recovery_file_system(&old_primary);
        let current = recovery_object(&old_primary, version("version_prepare_non_primary_1"));
        let mut committed = current.clone();
        committed.version = version("version_prepare_non_primary_2");
        let prepare = namespace_prepare_for_recovery(&current, committed, 1);
        let metadata_root = root.join("metadata");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &metadata_root)
                .expect("backend");
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&current).expect("current object");
        backend
            .prepare_namespace_transaction(&prepare)
            .expect("durable prepare");

        let shard = recovery_shard(&current_primary, 2);
        let control_plane = recovery_control_plane(&file_system, &shard);
        let network = InProcessNetwork::new();
        let recovered = NetworkedDfsService::new(
            network.add_node(observer),
            LocalContentStore::new(root.join("content")).expect("content"),
        )
        .with_control_plane(control_plane)
        .with_metadata_backend(
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &metadata_root)
                .expect("reopen backend"),
        )
        .expect("non-primary hydration leaves prepare for authority");

        let durable_prepare = recovered
            .metadata_backend()
            .expect("backend")
            .get_namespace_prepare(&prepare.operation_id)
            .expect("prepare")
            .expect("durable prepare");
        assert_eq!(durable_prepare.status, DfsNamespacePrepareStatus::Prepared);
        assert_eq!(durable_prepare.owner_epoch, prepare.owner_epoch);
        assert_eq!(durable_prepare.operation_digest, prepare.operation_digest);
        assert_eq!(durable_prepare.transaction, prepare.transaction);
        assert_eq!(
            recovered
                .metadata_backend()
                .expect("backend")
                .get_object(&current.object_id)
                .expect("object"),
            Some(current),
            "a non-primary must not publish or abort the prepared operation"
        );
    }

    #[test]
    fn hydration_aborts_create_prepare_left_by_aborted_write_intent() {
        let root = sinomo_testdir::TestDir::new("dfs-aborted-create-prepare-recovery");
        let old_primary = node("node_aborted_create_old");
        let new_primary = node("node_aborted_create_new");
        let file_system = recovery_file_system(&old_primary);
        let current = recovery_object(&old_primary, version("version_aborted_create_1"));
        let mut hidden = current.clone();
        hidden.version = version("version_aborted_create_2");
        hidden.changed_at_unix_nanos += 1;
        let intent_id = "intent-aborted-create-recovery";
        let intent = DfsWriteIntentRecord {
            publication: Default::default(),
            intent_id: intent_id.to_string(),
            file_system_id: current.file_system_id.clone(),
            object_id: current.object_id.clone(),
            base_version: Some(current.version.clone()),
            target_version: hidden.version.clone(),
            coordinator_node_id: old_primary.clone(),
            operation_id: None,
            operation_order_token: None,
            owner_epoch: 1,
            desired_replica_node_ids: vec![old_primary.clone()],
            policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
            status: DfsWriteIntentStatus::Aborted,
            content_hash: Some("blake3:aborted-create".to_string()),
            object_metadata: Some(current.clone()),
            touch_parent_on_create: true,
            create_predecessor_object_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: Some(OffsetDateTime::UNIX_EPOCH),
        };
        let mut prepare = namespace_prepare_for_recovery(&current, hidden, 1);
        prepare.operation_id = format!("namespace:create:{intent_id}");
        prepare.operation_digest =
            crate::service_replica::metadata_namespace_prepare_payload_digest(&prepare)
                .expect("prepare digest");

        let metadata_root = root.join("metadata");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &metadata_root)
                .expect("backend");
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&current).expect("current object");
        backend
            .put_intent_for_object(Some(&current), &intent)
            .expect("aborted intent");
        backend
            .prepare_namespace_transaction(&prepare)
            .expect("durable create prepare");

        let shard = recovery_shard(&new_primary, 2);
        let control_plane = recovery_control_plane(&file_system, &shard);
        let network = InProcessNetwork::new();
        let recovered = NetworkedDfsService::new(
            network.add_node(new_primary),
            LocalContentStore::new(root.join("content")).expect("content"),
        )
        .with_control_plane(control_plane)
        .with_metadata_backend(
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &metadata_root)
                .expect("reopen backend"),
        )
        .expect("current primary reconciles aborted create");

        assert_eq!(
            recovered
                .metadata_backend()
                .expect("backend")
                .get_namespace_prepare(&prepare.operation_id)
                .expect("prepare")
                .expect("durable prepare")
                .status,
            DfsNamespacePrepareStatus::Aborted
        );
        assert_eq!(
            recovered
                .metadata_backend()
                .expect("backend")
                .get_object(&current.object_id)
                .expect("object"),
            Some(current),
            "recovery must release the hidden prepare without publishing it"
        );
    }

    #[test]
    fn hydration_rebinds_prepared_write_and_reauthorizes_before_visibility() {
        let root = sinomo_testdir::TestDir::new("dfs-write-intent-primary-recovery");
        let old_primary = node("node_write_recovery_old");
        let new_primary = node("node_write_recovery_new");
        let file_system = recovery_file_system(&old_primary);
        let current = recovery_object(&old_primary, version("version_write_recovery_1"));
        let target_version = version("version_write_recovery_2");
        let bytes = b"rebound write intent content";
        let content = LocalContentStore::new(root.join("content")).expect("content");
        let manifest = content
            .put_object_version(current.object_id.as_str(), target_version.as_str(), bytes)
            .expect("target content");
        let metadata_root = root.join("metadata");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &metadata_root)
                .expect("backend");
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&current).expect("current object");
        let intent = DfsWriteIntentRecord {
            publication: Default::default(),
            intent_id: "intent-write-recovery".to_string(),
            file_system_id: current.file_system_id.clone(),
            object_id: current.object_id.clone(),
            base_version: Some(current.version.clone()),
            target_version: target_version.clone(),
            coordinator_node_id: old_primary.clone(),
            operation_id: None,
            operation_order_token: None,
            owner_epoch: 1,
            desired_replica_node_ids: vec![new_primary.clone()],
            policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
            status: DfsWriteIntentStatus::ReceiptsPending,
            content_hash: Some(manifest.content_hash.clone()),
            object_metadata: Some(current.clone()),
            touch_parent_on_create: false,
            create_predecessor_object_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: Some(OffsetDateTime::UNIX_EPOCH),
        };
        backend.put_intent(&intent).expect("intent");
        backend
            .put_receipt(&DfsReplicaReceiptRecord {
                receipt: DfsWriteReceipt {
                    file_system_id: current.file_system_id.clone(),
                    object_id: current.object_id.clone(),
                    committed_version: target_version.clone(),
                    owner_node_id: old_primary,
                    event_id: EventId::generate(),
                },
                replica_node_id: new_primary.clone(),
                content_hash: Some(manifest.content_hash.clone()),
                received_at: OffsetDateTime::UNIX_EPOCH,
            })
            .expect("receipt");

        let shard = recovery_shard(&new_primary, 2);
        let control_plane = recovery_control_plane(&file_system, &shard);
        let network = InProcessNetwork::new();
        let recovered = NetworkedDfsService::new(network.add_node(new_primary), content)
            .with_control_plane(control_plane)
            .with_metadata_backend(
                LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &metadata_root)
                    .expect("reopen backend"),
            )
            .expect("current primary recovers write intent");

        let visible = recovered
            .metadata_backend()
            .expect("backend")
            .get_object(&current.object_id)
            .expect("object")
            .expect("visible object");
        assert_eq!(visible.version, target_version);
        assert_eq!(visible.content_hash, Some(manifest.content_hash));
        let durable_intent = recovered
            .metadata_backend()
            .expect("backend")
            .get_intent(&intent.object_id, &intent.intent_id)
            .expect("intent")
            .expect("durable intent");
        assert_eq!(durable_intent.owner_epoch, 2);
        assert_eq!(
            durable_intent.coordinator_node_id,
            intent.coordinator_node_id
        );
        assert_eq!(durable_intent.status, DfsWriteIntentStatus::Committed);
    }

    #[test]
    fn hydration_reuses_exact_durable_create_prepare_after_epoch_move() {
        let root = sinomo_testdir::TestDir::new("dfs-create-prepare-epoch-recovery");
        let old_primary = node("node_create_recovery_old");
        let new_primary = node("node_create_recovery_new");
        let file_system = recovery_file_system(&old_primary);
        let mut child = recovery_object(&old_primary, version("version_create_recovery_base"));
        child.path = "/directory/child.txt".to_string();
        let target_version = version("version_create_recovery_target");
        let bytes = b"exact recovered create bytes";
        let content = LocalContentStore::new(root.join("content")).expect("content");
        let manifest = content
            .put_object_version(child.object_id.as_str(), target_version.as_str(), bytes)
            .expect("target content");

        let mut parent = recovery_object(&old_primary, version("version_parent_before"));
        parent.object_id =
            DfsObjectId::from_legacy_named("object_create_recovery_parent").expect("parent id");
        parent.path = "/directory".to_string();
        parent.kind = DfsObjectKind::Directory;
        parent.content_hash = None;
        parent.content_size_bytes = None;
        let mut committed_child = child.clone();
        committed_child.version = target_version.clone();
        committed_child.content_hash = Some(manifest.content_hash.clone());
        committed_child.content_size_bytes = Some(manifest.size_bytes);
        committed_child.current_locations = vec![new_primary.clone()];
        committed_child.modified_at_unix_nanos = 700;
        committed_child.changed_at_unix_nanos = 701;
        let mut touched_parent = parent.clone();
        touched_parent.version = version("version_parent_exact_touch");
        touched_parent.modified_at_unix_nanos = 777;
        touched_parent.changed_at_unix_nanos = 778;

        let intent_id = "intent-create-recovery".to_string();
        let intent = DfsWriteIntentRecord {
            publication: Default::default(),
            intent_id: intent_id.clone(),
            file_system_id: child.file_system_id.clone(),
            object_id: child.object_id.clone(),
            base_version: Some(child.version.clone()),
            target_version: target_version.clone(),
            coordinator_node_id: old_primary.clone(),
            operation_id: None,
            operation_order_token: None,
            owner_epoch: 1,
            desired_replica_node_ids: vec![new_primary.clone()],
            policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
            status: DfsWriteIntentStatus::ReceiptsPending,
            content_hash: Some(manifest.content_hash.clone()),
            object_metadata: Some(child.clone()),
            touch_parent_on_create: true,
            create_predecessor_object_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: Some(OffsetDateTime::UNIX_EPOCH),
        };
        let mut prepare = DfsNamespacePrepareRecord {
            operation_id: format!("namespace:create:{intent_id}"),
            file_system_id: child.file_system_id.clone(),
            shard_id: DfsMetadataShardId::new("root"),
            owner_epoch: 1,
            operation_digest: String::new(),
            expected_object_versions: vec![
                DfsNamespaceObjectVersionExpectation {
                    object_id: child.object_id.clone(),
                    expected_version: None,
                },
                DfsNamespaceObjectVersionExpectation {
                    object_id: parent.object_id.clone(),
                    expected_version: Some(parent.version.clone()),
                },
            ],
            expected_path_occupants: vec![
                DfsNamespacePathOccupantExpectation {
                    path: child.path.clone(),
                    expected_object_id: None,
                },
                DfsNamespacePathOccupantExpectation {
                    path: parent.path.clone(),
                    expected_object_id: Some(parent.object_id.clone()),
                },
            ],
            content_receipts: Vec::new(),
            transaction: DfsNamespaceTransaction {
                file_version_pins: None,
                transaction_id: namespace_transaction_id_for_object(
                    "create-visible-batched",
                    &child.object_id,
                    &target_version,
                ),
                file_system_id: child.file_system_id.clone(),
                objects: vec![committed_child.clone(), touched_parent.clone()],
                versions: vec![
                    object_version_record(&committed_child, Some(manifest.size_bytes)),
                    object_version_record(&touched_parent, touched_parent.content_size_bytes),
                ],
                advisory_lock_states: Vec::new(),
                created_at: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(123),
            },
            completed_request: None,
            completed_request_completion: None,
            status: DfsNamespacePrepareStatus::Prepared,
        };
        prepare.operation_digest =
            crate::service_replica::metadata_namespace_prepare_payload_digest(&prepare)
                .expect("prepare digest");

        let metadata_root = root.join("metadata");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &metadata_root)
                .expect("backend");
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&parent).expect("parent");
        backend
            .put_intent_for_object(Some(&child), &intent)
            .expect("intent");
        backend
            .put_receipt(&DfsReplicaReceiptRecord {
                receipt: DfsWriteReceipt {
                    file_system_id: child.file_system_id.clone(),
                    object_id: child.object_id.clone(),
                    committed_version: target_version.clone(),
                    owner_node_id: old_primary,
                    event_id: EventId::generate(),
                },
                replica_node_id: new_primary.clone(),
                content_hash: Some(manifest.content_hash.clone()),
                received_at: OffsetDateTime::UNIX_EPOCH,
            })
            .expect("receipt");
        backend
            .prepare_namespace_transaction(&prepare)
            .expect("durable create prepare");
        let exact_transaction = backend
            .get_namespace_prepare(&prepare.operation_id)
            .expect("prepare lookup")
            .expect("prepare")
            .transaction;

        let shard = recovery_shard(&new_primary, 2);
        let control_plane = recovery_control_plane(&file_system, &shard);
        let network = InProcessNetwork::new();
        let recovered = NetworkedDfsService::new(network.add_node(new_primary), content)
            .with_control_plane(control_plane)
            .with_metadata_backend(
                LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &metadata_root)
                    .expect("reopen backend"),
            )
            .expect("recover prepared create on new epoch primary");

        assert!(
            recovered
                .metadata_backend()
                .expect("backend")
                .get_namespace_prepare(&prepare.operation_id)
                .expect("prepare")
                .is_none(),
            "epoch recovery must compact the committed prepare body"
        );
        assert!(
            recovered
                .metadata_backend()
                .expect("backend")
                .get_namespace_transaction(&exact_transaction.transaction_id)
                .expect("transaction")
                .is_none(),
            "epoch recovery must compact the committed transaction body"
        );
        assert!(
            recovered
                .metadata_backend()
                .expect("backend")
                .journal_since(0)
                .expect("journal")
                .iter()
                .any(|entry| matches!(
                    &entry.mutation,
                    DfsMetadataMutation::CommitNamespacePrepare {
                        operation_id,
                        owner_epoch: 2,
                        ..
                    } if operation_id == &prepare.operation_id
                )),
            "epoch recovery must retain the rebound authoritative commit"
        );
        assert_eq!(
            recovered
                .metadata_backend()
                .expect("backend")
                .get_object(&child.object_id)
                .expect("child"),
            exact_transaction
                .objects
                .iter()
                .find(|object| object.object_id == child.object_id)
                .cloned()
        );
        assert_eq!(
            recovered
                .metadata_backend()
                .expect("backend")
                .get_object(&parent.object_id)
                .expect("parent"),
            exact_transaction
                .objects
                .iter()
                .find(|object| object.object_id == parent.object_id)
                .cloned(),
            "recovery must retain the original parent version and timestamps"
        );
    }
}
