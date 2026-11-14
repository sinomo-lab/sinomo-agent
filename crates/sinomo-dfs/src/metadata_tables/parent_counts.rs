//! Derived active-parent index and write admission for existing namespace jobs.
use super::*;
use crate::{
    DfsParentCountAction, DfsParentCountState, DfsPreparedMetadataEffect,
    DfsPreparedOperationPayload,
};

impl DfsMetadataTables {
    pub(crate) fn parent_count_reservation_ids(
        &self,
        fs: &DfsFileSystemId,
        object: &DfsObjectId,
    ) -> impl Iterator<Item = &String> {
        self.parent_count_reservations
            .get(&(fs.clone(), object.clone()))
            .into_iter()
            .flat_map(|ids| ids.iter())
    }

    pub(super) fn remove_parent_count_index(&mut self, job_id: &str) {
        let Some((participant, state)) = self
            .namespace_jobs
            .get(job_id)
            .and_then(|job| job.parent_count_participant())
        else {
            return;
        };
        if *state != DfsParentCountState::Reserved {
            return;
        }
        let key = (
            participant.expected.file_system_id.clone(),
            participant.expected.object_id.clone(),
        );
        if let Some(ids) = self.parent_count_reservations.get_mut(&key) {
            ids.remove(job_id);
            if ids.is_empty() {
                self.parent_count_reservations.remove(&key);
            }
        }
    }

    pub(super) fn insert_parent_count_index(&mut self, job_id: &str) {
        let Some(job) = self.namespace_jobs.get(job_id) else {
            return;
        };
        let Some((participant, state)) = job.parent_count_participant() else {
            return;
        };
        if job.status == DfsNamespaceJobStatus::Running && *state == DfsParentCountState::Reserved {
            self.parent_count_reservations
                .entry((
                    participant.expected.file_system_id.clone(),
                    participant.expected.object_id.clone(),
                ))
                .or_default()
                .insert(job_id.to_owned());
        }
    }

    pub(super) fn rebuild_parent_count_index(&mut self) {
        self.parent_count_reservations.clear();
        let ids = self.namespace_jobs.keys().cloned().collect::<Vec<_>>();
        for id in ids {
            self.insert_parent_count_index(&id);
        }
    }

    /// Called at both Prepare and Commit under the original metadata writer.
    /// A prepare predating a reservation cannot publish through it later.
    pub(crate) fn validate_parent_count_writes(
        &self,
        payload: &DfsPreparedOperationPayload,
    ) -> Result<()> {
        let fail = || {
            MetadataBackendError::Serde(
                "parent row is reserved by an exact namespace participant".into(),
            )
        };
        let parent_chain = self.parent_supersession_chain(payload)?;
        for effect in &payload.metadata_effects {
            let DfsPreparedMetadataEffect::PutNamespaceJob { job } = effect else {
                continue;
            };
            self.validate_parent_coordinator_transition(job)?;
            if job.kind != crate::DfsNamespaceJobKind::ParentCountParticipant {
                continue;
            }
            let (participant, state) = job.parent_count_participant().ok_or_else(fail)?;
            participant.validate()?;
            let mut own_jobs = 0;
            for effect in &payload.metadata_effects {
                match effect {
                    DfsPreparedMetadataEffect::PutNamespaceJob { job: candidate }
                        if candidate.job_id == participant.participant_id =>
                    {
                        own_jobs += 1
                    }
                    DfsPreparedMetadataEffect::ResolvePreparedOperation { digest, status }
                        if *state == DfsParentCountState::Aborted
                            && *status == crate::DfsPreparedOperationStatus::Aborted
                            && digest.operation_id
                                == participant.operation_id_at_epoch(
                                    &DfsParentCountAction::Reserve,
                                    digest.owner_epoch,
                                ) =>
                    {
                        let reserve = self
                            .prepared_operations_v7
                            .get(&digest.operation_id)
                            .ok_or_else(fail)?;
                        reserve.validate()?;
                        if reserve.digest != *digest
                            || participant.validate_phase_payload(&reserve.payload)?
                                != DfsParentCountState::Reserved
                        {
                            return Err(fail());
                        }
                        self.parent_supersession_chain(&reserve.payload)?;
                    }
                    DfsPreparedMetadataEffect::ResolvePreparedOperation {
                        digest,
                        status: crate::DfsPreparedOperationStatus::Superseded,
                    } if parent_chain
                        .as_ref()
                        .is_some_and(|chain| chain.contains(&digest.operation_id)) => {}
                    _ => return Err(fail()),
                }
            }
            if own_jobs != 1
                || !payload.transaction.advisory_lock_states.is_empty()
                || !payload.content_receipts.is_empty()
                || payload.prepared_intent.is_some()
                || payload.terminal_intent.is_some()
            {
                return Err(fail());
            }
            if !participant.matches_job(job, state)
                || payload.file_system_id != participant.expected.file_system_id
                || payload.shard_id.0 != participant.parent_scope.shard_id
            {
                return Err(fail());
            }
            let action = match state {
                DfsParentCountState::Reserved => DfsParentCountAction::Reserve,
                DfsParentCountState::Applied => DfsParentCountAction::Apply,
                DfsParentCountState::Aborted => DfsParentCountAction::Abort,
            };
            if payload.operation_id
                != participant.operation_id_at_epoch(&action, payload.owner_epoch)
            {
                return Err(fail());
            }
            if let Some(existing) = self.namespace_jobs.get(&job.job_id) {
                let (original, old_state) = existing.parent_count_participant().ok_or_else(fail)?;
                if original != participant
                    || (*old_state != DfsParentCountState::Reserved && old_state != state)
                {
                    return Err(fail());
                }
            } else if let Some(receipt) = self.namespace_job_receipts.get(&job.job_id) {
                if !receipt.parent_count.as_ref().is_some_and(|old| {
                    old.participant_id == participant.participant_id && old.state == *state
                }) {
                    return Err(fail());
                }
            } else if *state == DfsParentCountState::Applied {
                return Err(fail());
            }
            if *state == DfsParentCountState::Reserved {
                if self
                    .parent_count_reservation_ids(
                        &participant.expected.file_system_id,
                        &participant.expected.object_id,
                    )
                    .any(|id| id != &participant.participant_id)
                    || self.objects.get(&participant.expected.object_id)
                        != Some(&participant.expected)
                {
                    return Err(fail());
                }
            }
            let versions = if *state == DfsParentCountState::Applied {
                vec![participant.successor_version()?]
            } else {
                Vec::new()
            };
            if payload.transaction.versions != versions {
                return Err(fail());
            }
            if *state == DfsParentCountState::Applied {
                if payload.transaction.objects.as_slice() != [participant.successor.clone()]
                    || !payload.expected_object_versions.iter().any(|predicate| {
                        predicate.object_id == participant.expected.object_id
                            && predicate.expected_version.as_ref()
                                == Some(&participant.expected.version)
                    })
                {
                    return Err(fail());
                }
            } else if !payload.transaction.objects.is_empty() {
                return Err(fail());
            }
        }
        let touched =
            payload
                .transaction
                .objects
                .iter()
                .chain(
                    payload
                        .metadata_effects
                        .iter()
                        .filter_map(|effect| match effect {
                            DfsPreparedMetadataEffect::PutObject { object } => Some(object),
                            DfsPreparedMetadataEffect::FinalizeCrossShardSourceEviction {
                                tombstone,
                                ..
                            } => Some(tombstone),
                            _ => None,
                        }),
                );
        for object in touched {
            for job_id in
                self.parent_count_reservation_ids(&object.file_system_id, &object.object_id)
            {
                let job = self.namespace_jobs.get(job_id).ok_or_else(fail)?;
                let (participant, state) = job.parent_count_participant().ok_or_else(fail)?;
                participant.validate()?;
                if *state != DfsParentCountState::Reserved
                    || payload.operation_id != participant.operation_id_at_epoch(&DfsParentCountAction::Apply, payload.owner_epoch)
                    || object != &participant.successor
                    || !payload.metadata_effects.iter().any(|effect| matches!(effect,
                        DfsPreparedMetadataEffect::PutNamespaceJob { job } if participant.matches_job(job, &DfsParentCountState::Applied)))
                { return Err(fail()); }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::*;

    fn participant() -> DfsParentCountParticipant {
        let node = NodeId::from_legacy_named("parent_index_owner").unwrap();
        let mut parent =
            crate::posix_object::new_posix_metadata_object(&node, "/", DfsObjectKind::Directory);
        parent.namespace_link_count = Some(4);
        let source = crate::posix_object::new_posix_metadata_object(
            &node,
            "/users",
            DfsObjectKind::Directory,
        );
        let mut successor = parent.clone();
        successor.version = crate::namespace::next_version(&parent.version);
        successor.namespace_link_count = Some(3);
        let config = DfsMetadataShardConfig {
            file_system_id: parent.file_system_id.clone(),
            shard_id: "root".into(),
            primary_node_id: node,
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
        };
        let mut result = DfsParentCountParticipant {
            participant_id: String::new(),
            binding: DfsParentCountBinding {
                job_id: "parent-index-original-job".into(),
                file_system_id: parent.file_system_id.clone(),
                root_path: "/users".into(),
                target_path: "/services/moved".into(),
                coordinator_scope: config.clone(),
                source,
                expected_destination: DfsNamespaceDestinationPrecondition::Absent,
                origin: None,
                created_at: time::OffsetDateTime::now_utc(),
            },
            parent_scope: config,
            expected: parent,
            successor,
            delta: -1,
        };
        result.participant_id = result.identity().unwrap();
        result.validate().unwrap();
        result
    }

    fn install(tables: &mut DfsMetadataTables, participant: &DfsParentCountParticipant) {
        tables
            .apply_checked(DfsMetadataTableOp::PutObject {
                object: participant.expected.clone(),
            })
            .unwrap();
        tables
            .apply_checked(DfsMetadataTableOp::PutNamespaceJob {
                job: participant.job(DfsParentCountState::Reserved),
            })
            .unwrap();
    }

    fn assert_index(
        tables: &DfsMetadataTables,
        participant: &DfsParentCountParticipant,
        expected: usize,
    ) {
        let indexed = tables
            .parent_count_reservation_ids(
                &participant.expected.file_system_id,
                &participant.expected.object_id,
            )
            .cloned()
            .collect::<BTreeSet<_>>();
        let actual = tables
            .namespace_jobs
            .values()
            .filter(|job| job.status == DfsNamespaceJobStatus::Running)
            .filter_map(|job| {
                job.parent_count_participant()
                    .filter(|(p, state)| {
                        p.expected.object_id == participant.expected.object_id
                            && **state == DfsParentCountState::Reserved
                    })
                    .map(|_| job.job_id.clone())
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(indexed, actual);
        assert_eq!(indexed.len(), expected);
    }

    #[test]
    fn parent_reservation_index_follows_replay_abort_and_compact_receipt() {
        let participant = participant();
        let mut tables = DfsMetadataTables::default();
        install(&mut tables, &participant);
        assert_index(&tables, &participant, 1);
        let mut replay = DfsMetadataTables::default();
        for object in tables.objects.values() {
            replay
                .apply_checked(DfsMetadataTableOp::PutObject {
                    object: object.clone(),
                })
                .unwrap();
        }
        for job in tables.namespace_jobs.values() {
            replay
                .apply_checked(DfsMetadataTableOp::PutNamespaceJob { job: job.clone() })
                .unwrap();
        }
        assert_index(&replay, &participant, 1);
        let aborted = participant.job(DfsParentCountState::Aborted);
        replay
            .apply_checked(DfsMetadataTableOp::PutNamespaceJob {
                job: aborted.clone(),
            })
            .unwrap();
        assert_index(&replay, &participant, 0);
        assert_eq!(
            replay.objects.get(&participant.expected.object_id),
            Some(&participant.expected)
        );
        let receipt =
            DfsNamespaceJobReceipt::from_job(&aborted, time::OffsetDateTime::now_utc()).unwrap();
        assert_eq!(
            receipt.parent_count.as_ref().unwrap().state,
            DfsParentCountState::Aborted
        );
        replay
            .apply_checked(DfsMetadataTableOp::PutNamespaceJobReceipt { receipt })
            .unwrap();
        assert_index(&replay, &participant, 0);
    }

    #[test]
    fn parent_apply_materialization_rollback_restores_row_and_active_index() {
        let participant = participant();
        let mut tables = DfsMetadataTables::default();
        install(&mut tables, &participant);
        let mut rollback = DfsMetadataTableRollback::default();
        tables
            .apply_checked_with_rollback(
                DfsMetadataTableOp::PutObject {
                    object: participant.successor.clone(),
                },
                &mut rollback,
            )
            .unwrap();
        tables
            .apply_checked_with_rollback(
                DfsMetadataTableOp::PutNamespaceJob {
                    job: participant.job(DfsParentCountState::Applied),
                },
                &mut rollback,
            )
            .unwrap();
        let version = participant.successor_version().unwrap();
        tables
            .apply_checked_with_rollback(
                DfsMetadataTableOp::PutVersion {
                    version: version.clone(),
                },
                &mut rollback,
            )
            .unwrap();
        assert_index(&tables, &participant, 0);
        let mut invalid = participant.job(DfsParentCountState::Applied);
        invalid.root_path = "/wrong-parent".into();
        assert!(
            tables
                .apply_checked_with_rollback(
                    DfsMetadataTableOp::PutNamespaceJob { job: invalid },
                    &mut rollback
                )
                .is_err()
        );
        rollback.rollback(&mut tables);
        assert!(
            !tables
                .versions
                .contains_key(&(version.object_id, version.version))
        );
        assert_index(&tables, &participant, 1);
        assert_eq!(
            tables.objects.get(&participant.expected.object_id),
            Some(&participant.expected)
        );
    }
}

impl DfsMetadataTables {
    fn validate_parent_coordinator_transition(&self, job: &DfsNamespaceJobRecord) -> Result<()> {
        use crate::DfsParentCountPhase as Phase;
        if job.kind != crate::DfsNamespaceJobKind::RenameDirectoryAcrossShardPrimaries {
            return Ok(());
        }
        let plan_of = |job: &DfsNamespaceJobRecord| {
            job.pending_frames.iter().find_map(|frame| match frame {
                crate::DfsNamespaceJobFrame::CrossShardDirFinish {
                    count_plan: Some(counts),
                    ..
                } => counts.parent_participants.clone(),
                _ => None,
            })
        };
        let new = plan_of(job);
        let old_job = self.namespace_jobs.get(&job.job_id);
        if old_job == Some(job) {
            return Ok(());
        }
        let old = old_job.and_then(plan_of);
        let fail = || {
            MetadataBackendError::Serde(
                "original parent coordinator changed its durable issuance/abort decision".into(),
            )
        };
        if new.is_none() && old.is_none() {
            return Ok(());
        }
        if let Some(plan) = &new {
            let mut running = job.clone();
            running.status = DfsNamespaceJobStatus::Running;
            plan.validate_for_job(&running)?;
        }
        match (old, new) {
            (None, Some(new)) if old_job.is_none() => {
                if new.phase != Phase::Preparing
                    || new.confirmed != 0
                    || job.processed_object_count != 0
                    || job.status != DfsNamespaceJobStatus::Running
                {
                    return Err(fail());
                }
            }
            (Some(old), Some(new)) => {
                let original = old_job.expect("old plan owns actual row");
                if original.status != DfsNamespaceJobStatus::Running
                    || old.participants != new.participants
                    || original.file_system_id != job.file_system_id
                    || original.root_path != job.root_path
                    || original.target_path != job.target_path
                    || original.origin != job.origin
                    || original.created_at != job.created_at
                    || job.processed_object_count < original.processed_object_count
                {
                    return Err(fail());
                }
                if job.status == DfsNamespaceJobStatus::Failed {
                    if old.phase != Phase::Aborting
                        || old.confirmed != old.participants.len()
                        || new != old
                        || job.processed_object_count != 0
                    {
                        return Err(fail());
                    }
                    return Ok(());
                }
                if job.status != DfsNamespaceJobStatus::Running {
                    return Err(fail());
                }
                let allowed = match (&old.phase, &new.phase) {
                    (Phase::Preparing, Phase::Preparing)
                    | (Phase::Settling, Phase::Settling)
                    | (Phase::Aborting, Phase::Aborting) => {
                        new.confirmed == old.confirmed || new.confirmed == old.confirmed + 1
                    }
                    (Phase::Preparing, Phase::Issued) => {
                        old.confirmed == old.participants.len() && new.confirmed == 0
                    }
                    (Phase::Preparing, Phase::Aborting) => {
                        job.processed_object_count == 0 && new.confirmed == 0
                    }
                    (Phase::Issued, Phase::Issued) => new.confirmed == old.confirmed,
                    (Phase::Issued, Phase::Settling) => new.confirmed == 0,
                    _ => false,
                };
                if !allowed {
                    return Err(fail());
                }
            }
            (Some(old), None) => {
                let original = old_job.expect("old plan owns actual row");
                if original.status != DfsNamespaceJobStatus::Running
                    || old.phase != Phase::Settling
                    || old.confirmed != old.participants.len()
                    || job.status != DfsNamespaceJobStatus::Completed
                    || !job.pending_frames.is_empty()
                    || job.file_system_id != original.file_system_id
                    || job.root_path != original.root_path
                    || job.target_path != original.target_path
                    || job.origin != original.origin
                    || job.created_at != original.created_at
                {
                    return Err(fail());
                }
            }
            _ => return Err(fail()),
        }
        Ok(())
    }
}

impl DfsMetadataTables {
    /// Validate only the exact native-attempt ancestry of a parent phase.
    /// Every direct edge preserves the full predecessor body, including older
    /// edges. This does not authorize publication or qualify a retiring epoch.
    pub(crate) fn parent_supersession_chain(
        &self,
        payload: &DfsPreparedOperationPayload,
    ) -> Result<Option<BTreeSet<String>>> {
        use crate::DfsPreparedOperationStatus as Status;
        let fail = || {
            MetadataBackendError::Serde(
                "parent supersession changes or loses its exact predecessor chain".into(),
            )
        };
        let parents = payload
            .metadata_effects
            .iter()
            .filter_map(|effect| match effect {
                DfsPreparedMetadataEffect::PutNamespaceJob { job } => {
                    job.parent_count_participant()
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        let [(participant, state)] = parents.as_slice() else {
            if parents.is_empty() {
                return Ok(None);
            }
            return Err(fail());
        };
        participant.validate()?;
        let action = match state {
            DfsParentCountState::Reserved => DfsParentCountAction::Reserve,
            DfsParentCountState::Applied => DfsParentCountAction::Apply,
            DfsParentCountState::Aborted => DfsParentCountAction::Abort,
        };
        let superseded = |body: &DfsPreparedOperationPayload| {
            body.metadata_effects
                .iter()
                .filter_map(|effect| match effect {
                    DfsPreparedMetadataEffect::ResolvePreparedOperation {
                        digest,
                        status: Status::Superseded,
                    } => Some(digest.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>()
        };
        let declared = superseded(payload);
        let all_ids = declared
            .iter()
            .map(|digest| digest.operation_id.clone())
            .collect::<BTreeSet<_>>();
        if all_ids.len() != declared.len() {
            return Err(fail());
        }
        let mut visited = BTreeSet::new();
        let mut current = payload;
        loop {
            if participant.validate_phase_payload(current)? != **state {
                return Err(fail());
            }
            if current.file_system_id != participant.expected.file_system_id
                || current.shard_id.0 != participant.parent_scope.shard_id
                || current.operation_id
                    != participant.operation_id_at_epoch(&action, current.owner_epoch)
            {
                return Err(fail());
            }
            let edges = superseded(current);
            let Some(direct) = edges.iter().max_by_key(|digest| digest.owner_epoch) else {
                break;
            };
            if direct.owner_epoch >= current.owner_epoch
                || !visited.insert(direct.operation_id.clone())
            {
                return Err(fail());
            }
            let old = self
                .prepared_operations_v7
                .get(&direct.operation_id)
                .ok_or_else(fail)?;
            old.validate()?;
            if old.digest != *direct
                || old.status != Status::Prepared
                || old.file_system_id != current.file_system_id
                || old.shard_id != current.shard_id
                || old.operation_id != participant.operation_id_at_epoch(&action, old.owner_epoch)
            {
                return Err(fail());
            }
            if participant.validate_phase_payload(&old.payload)? != **state {
                return Err(fail());
            }
            let mut unchanged = current.clone();
            unchanged.operation_id = old.operation_id.clone();
            unchanged.owner_epoch = old.owner_epoch;
            let before = unchanged.metadata_effects.len();
            unchanged.metadata_effects.retain(|effect| !matches!(effect,
                DfsPreparedMetadataEffect::ResolvePreparedOperation { digest, status: Status::Superseded } if digest == direct));
            if unchanged.metadata_effects.len() + 1 != before || unchanged != *old.payload {
                return Err(fail());
            }
            current = &old.payload;
        }
        if visited != all_ids {
            return Err(fail());
        }
        Ok(Some(visited))
    }
}
