//! Parent WRITE participants driven by the existing directory namespace job.
use super::*;
use crate::{
    DfsParentCountAction as Action, DfsParentCountOutcome as Outcome,
    DfsParentCountParticipant as Participant, DfsParentCountPhase as Phase,
    DfsParentCountPlan as Plan, DfsParentCountRejection as Rejection, DfsParentCountState as State,
};

fn invalid(message: &str) -> DfsPosixError {
    DfsPosixError::Dfs(DfsError::Metadata {
        message: message.into(),
    })
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    fn sync_parent_count_participant(&mut self, participant: &Participant) -> Result<()> {
        let target = self.capture_metadata_quorum_sync_target_for_namespace_scope(
            &participant.expected.file_system_id,
            &participant.expected.path,
            &participant.participant_id,
        )?;
        // A write-set shard certifies against its write set, as a directory
        // sync does: a configured member outside it, such as a lost primary,
        // is not waited for.
        match self.sync_metadata_shard_to_write_set(&target, None)? {
            Some(crate::service_sync::DfsWriteSetSync::Certified(_)) => Ok(()),
            Some(crate::service_sync::DfsWriteSetSync::BelowFloor { write_set, floor }) => {
                Err(DfsPosixError::Dfs(DfsError::Distributed {
                    message: format!(
                        "parent participant's write set of {write_set} is below its floor {floor}"
                    ),
                }))
            }
            None => {
                self.sync_metadata_shard_to_quorum(&target, None)?;
                Ok(())
            }
        }
    }

    pub(crate) fn execute_parent_count_participant(
        &mut self,
        supplied_job: &DfsNamespaceJobRecord,
        participant: &Participant,
        action: &Action,
    ) -> Result<Outcome> {
        participant.validate()?;
        let supplied_plan = supplied_job
            .directory_move_counts()?
            .and_then(|counts| counts.parent_participants.as_ref())
            .ok_or_else(|| {
                invalid("parent command omitted its original participant declaration")
            })?;
        supplied_plan.validate_for_job(supplied_job)?;
        if !supplied_plan.participants.contains(participant) {
            return Err(invalid("parent is not declared by original namespace job"));
        }
        let current_parent_scope = self
            .namespace_shards_for_exact_paths(
                &participant.expected.file_system_id,
                [participant.expected.path.as_str()],
            )?
            .into_iter()
            .next()
            .ok_or_else(|| invalid("parent has no current authority"))?;
        if current_parent_scope.primary_node_id != self.node_id
            || current_parent_scope.shard_id != participant.parent_scope.shard_id
            || current_parent_scope.epoch < participant.parent_scope.epoch
        {
            return Err(invalid(
                "parent participant reached an obsolete or different physical owner",
            ));
        }
        let scope = self.metadata_effect_scope_for_object(
            &participant.expected,
            participant.operation_id_at_epoch(action, current_parent_scope.epoch),
        )?;
        let _execution = self.acquire_prepared_execution(&scope)?;
        let backend = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| invalid("parent participant has no native backend"))?;
        let existing = backend.get_namespace_job(&participant.participant_id)?;
        let existing_state = if let Some(job) = &existing {
            let (original, state) = job
                .parent_count_participant()
                .ok_or_else(|| invalid("parent participant identity belongs to another job"))?;
            if original != participant || !participant.matches_job(job, state) {
                return Err(invalid(
                    "parent participant changes its original immutable successor",
                ));
            }
            Some(state.clone())
        } else if let Some(receipt) =
            backend.get_namespace_job_receipt(&participant.participant_id)?
        {
            let terminal = receipt
                .parent_count
                .ok_or_else(|| invalid("parent receipt omitted its exact terminal decision"))?;
            if terminal.participant_id != participant.participant_id {
                return Err(invalid("foreign parent terminal receipt"));
            }
            Some(terminal.state)
        } else {
            None
        };
        let terminal = match &existing_state {
            Some(State::Applied) if *action == Action::Abort => {
                return Err(invalid("issued parent successor cannot be aborted"));
            }
            Some(State::Applied) => Some(Outcome::Applied),
            Some(State::Aborted) => Some(Outcome::Aborted),
            Some(State::Reserved) if *action == Action::Reserve => Some(Outcome::Reserved),
            _ => None,
        };
        if let Some(outcome) = terminal {
            self.sync_parent_count_participant(participant)?;
            return Ok(outcome);
        }
        if let Some(outcome) = self.observe_committed_parent_phase(participant)? {
            if *action == Action::Abort && outcome == Outcome::Applied {
                return Err(invalid(
                    "committed original parent successor cannot be aborted",
                ));
            }
            self.sync_parent_count_participant(participant)?;
            return Ok(outcome);
        }
        // Verify the current durable coordinator state through the existing
        // exact job read. Remote phases release their coordinator admission;
        // a co-located owner reads its held original backend without self RPC.
        let coordinator_scope = self
            .namespace_shards_for_exact_paths(
                &participant.binding.file_system_id,
                [participant.binding.root_path.as_str()],
            )?
            .into_iter()
            .next()
            .ok_or_else(|| invalid("parent coordinator has no current authority"))?;
        if coordinator_scope.shard_id != participant.binding.coordinator_scope.shard_id
            || coordinator_scope.epoch < participant.binding.coordinator_scope.epoch
        {
            return Err(invalid("parent coordinator physical identity changed"));
        }
        let current_job = if coordinator_scope.primary_node_id == self.node_id {
            self.namespace_job(&participant.binding.job_id)?
        } else {
            match self.query_metadata_authority_impl(
                &coordinator_scope,
                crate::DfsMetadataSelection::NamespaceJob {
                    job_id: participant.binding.job_id.clone(),
                },
            )? {
                crate::DfsMetadataQueryResult::NamespaceJob { job, .. } => job.map(|job| *job),
                _ => {
                    return Err(invalid(
                        "parent coordinator returned another metadata selection",
                    ));
                }
            }
        }
        .ok_or_else(|| invalid("parent command has no actual Running coordinator job"))?;
        let plan = current_job
            .directory_move_counts()?
            .and_then(|counts| counts.parent_participants.as_ref())
            .ok_or_else(|| invalid("actual coordinator omitted parent participants"))?;
        plan.validate_for_job(&current_job)?;
        if !plan.participants.contains(participant) {
            return Err(invalid("actual coordinator does not declare this parent"));
        }
        let state = match action {
            // The shard's current primary reserves at any epoch at or after
            // the declared one; the parent's version decides below.
            Action::Reserve if plan.phase == Phase::Preparing => {
                let backend = self.metadata_backend.as_ref().expect("checked backend");
                if backend
                    .get_object_on_shard(&scope.shard_id, &participant.expected.object_id)?
                    .as_ref()
                    != Some(&participant.expected)
                {
                    return Ok(Outcome::Rejected {
                        reason: Rejection::ParentChanged,
                    });
                }
                if backend
                    .parent_count_reservations(
                        &scope.shard_id,
                        &scope.file_system_id,
                        &participant.expected.object_id,
                    )?
                    .iter()
                    .any(|job| job.job_id != participant.participant_id)
                {
                    return Ok(Outcome::Rejected {
                        reason: Rejection::Contended,
                    });
                }
                State::Reserved
            }
            Action::Apply
                if plan.phase == Phase::Settling && existing_state == Some(State::Reserved) =>
            {
                State::Applied
            }
            Action::Abort if plan.phase == Phase::Aborting => State::Aborted,
            _ => {
                return Err(invalid(
                    "parent phase is not authorized by the original durable issuance decision",
                ));
            }
        };
        let payload = if let Some(retained) =
            self.parent_phase_attempt(participant, &current_parent_scope, action)?
        {
            retained
        } else {
            let mut terminal_job = participant.job(state.clone());
            terminal_job.updated_at = time::OffsetDateTime::now_utc().max(terminal_job.created_at);
            terminal_job.completed_at =
                (state != State::Reserved).then_some(terminal_job.updated_at);
            let mut payload = crate::DfsPreparedOperationPayload::for_metadata_effects(
                scope.clone(),
                if state == State::Aborted {
                    Vec::new()
                } else {
                    vec![DfsNamespaceObjectVersionExpectation {
                        object_id: participant.expected.object_id.clone(),
                        expected_version: Some(participant.expected.version.clone()),
                    }]
                },
                if state == State::Aborted {
                    Vec::new()
                } else {
                    vec![DfsNamespacePathOccupantExpectation {
                        path: participant.expected.path.clone(),
                        expected_object_id: Some(participant.expected.object_id.clone()),
                    }]
                },
                vec![crate::DfsPreparedMetadataEffect::PutNamespaceJob { job: terminal_job }],
                participant.binding.created_at,
            )?;
            if state == State::Applied {
                payload
                    .transaction
                    .objects
                    .push(participant.successor.clone());
                payload
                    .transaction
                    .versions
                    .push(participant.successor_version()?);
            }
            if state == State::Aborted {
                let mut after = None;
                loop {
                    let (records, next) =
                        self.parent_phase_family_page(participant, after.as_deref())?;
                    for reserve in records {
                        let original_state = phase_state_for_record(participant, &reserve)?;
                        if reserve.status != crate::DfsPreparedOperationStatus::Prepared {
                            continue;
                        }
                        if reserve.operation_id == scope.operation_id
                            && original_state == State::Aborted
                        {
                            continue;
                        }

                        if original_state != State::Reserved {
                            return Err(invalid(
                                "abort encountered a possibly issued non-reservation phase",
                            ));
                        }
                        // The plan is aborting: it retires every reservation
                        // it prepared, at any epoch.
                        payload.metadata_effects.push(
                            crate::DfsPreparedMetadataEffect::ResolvePreparedOperation {
                                digest: reserve.digest,
                                status: crate::DfsPreparedOperationStatus::Aborted,
                            },
                        );
                    }
                    let Some(next) = next else {
                        break;
                    };
                    after = Some(next);
                }
            }
            payload
        };
        if state == State::Reserved && self.metadata_backend.as_ref().expect("checked backend")
            .prepared_operation_reservations(&payload)?.iter().any(|record| {
                record.operation_id != payload.operation_id && !payload.metadata_effects.iter().any(|effect| matches!(effect,
                    crate::DfsPreparedMetadataEffect::ResolvePreparedOperation { digest, .. } if *digest == record.digest))
            }) {
            return Ok(Outcome::Rejected { reason: Rejection::Contended });
        }
        self.publish_metadata_effect_payload(payload)?;
        self.sync_parent_count_participant(participant)?;
        Ok(match state {
            State::Reserved => Outcome::Reserved,
            State::Applied => Outcome::Applied,
            State::Aborted => Outcome::Aborted,
        })
    }
}

fn replace_plan(job: &mut DfsNamespaceJobRecord, plan: Plan) -> Result<()> {
    let frame = job
        .pending_frames
        .iter_mut()
        .find_map(|frame| match frame {
            DfsNamespaceJobFrame::CrossShardDirFinish {
                count_plan: Some(counts),
                ..
            } => Some(counts),
            _ => None,
        })
        .ok_or_else(|| invalid("original directory job lost its participant plan"))?;
    frame.parent_participants = Some(plan);
    Ok(())
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub(super) fn plan_foreign_parent_counts(
        &mut self,
        job_id: &str,
        from: &str,
        to: &str,
        source: &DfsObjectMetadata,
        counts: &crate::DfsDirectoryMoveCounts,
        now: time::OffsetDateTime,
    ) -> Result<Option<Plan>> {
        let coordinator_scope = self
            .namespace_shards_for_exact_paths(&source.file_system_id, [from])?
            .into_iter()
            .next()
            .ok_or_else(|| invalid("directory coordinator has no actual assignment"))?;
        let binding = crate::DfsParentCountBinding {
            job_id: job_id.into(),
            file_system_id: source.file_system_id.clone(),
            root_path: from.into(),
            target_path: to.into(),
            coordinator_scope,
            source: source.clone(),
            expected_destination: counts.expected_destination.clone(),
            origin: self
                .namespace_execution
                .request()
                .map(DfsNamespaceJobOrigin::from_request),
            created_at: now,
        };
        let mut participants = Vec::new();
        for (child, delta) in [
            (from, counts.source_parent_delta),
            (to, counts.target_parent_delta),
        ] {
            if delta == 0 {
                continue;
            }
            let Some(parent_path) = crate::posix_path::parent_posix_path(child) else {
                continue;
            };
            let child_scope = self
                .namespace_shards_for_exact_paths(&source.file_system_id, [child])?
                .into_iter()
                .next()
                .ok_or_else(|| invalid("directory effect has no physical assignment"))?;
            let parent_scope = self
                .namespace_shards_for_exact_paths(&source.file_system_id, [parent_path.as_str()])?
                .into_iter()
                .next()
                .ok_or_else(|| invalid("directory parent has no physical assignment"))?;
            let parent = self
                .lookup_sifs_path_direct_distributed_synced(&parent_path)?
                .ok_or_else(|| DfsPosixError::MissingParent {
                    path: parent_path.clone(),
                })?;
            let count = super::super::metadata::updated_namespace_link_count(&parent, delta)?;
            if parent.kind != DfsObjectKind::Directory {
                return Err(DfsPosixError::NotDirectory { path: parent_path });
            }
            if child_scope.shard_id == parent_scope.shard_id {
                continue;
            }
            let mut successor = parent.clone();
            successor.namespace_link_count = Some(count);
            successor.version = next_version(&parent.version);
            let timestamp = now_unix_nanos()
                .max(parent.modified_at_unix_nanos.saturating_add(1))
                .max(parent.changed_at_unix_nanos.saturating_add(1));
            successor.modified_at_unix_nanos = timestamp;
            successor.changed_at_unix_nanos = timestamp;
            let mut participant = Participant {
                participant_id: String::new(),
                binding: binding.clone(),
                parent_scope,
                expected: parent,
                successor,
                delta,
            };
            participant.participant_id = participant.identity()?;
            participant.validate()?;
            participants.push(participant);
        }
        participants.sort_by(|a, b| {
            (&a.parent_scope.shard_id, &a.expected.object_id)
                .cmp(&(&b.parent_scope.shard_id, &b.expected.object_id))
        });
        if participants.is_empty() {
            Ok(None)
        } else {
            Ok(Some(Plan {
                participants,
                phase: Phase::Preparing,
                confirmed: 0,
                rejection: None,
            }))
        }
    }

    pub(super) fn begin_parent_count_abort(
        &mut self,
        job: &mut DfsNamespaceJobRecord,
        reason: Rejection,
    ) -> Result<()> {
        let original = job
            .directory_move_counts()?
            .and_then(|counts| counts.parent_participants.clone())
            .ok_or_else(|| invalid("abort has no original declared participants"))?;
        let scope = self.metadata_effect_scope_for_object(
            &original.participants[0].binding.source,
            format!("parent-abort-decision:{}", job.job_id),
        )?;
        let _execution = self.acquire_prepared_execution(&scope)?;
        let mut current = self
            .namespace_job(&job.job_id)?
            .ok_or_else(|| invalid("abort lost its original coordinator"))?;
        let mut plan = current
            .directory_move_counts()?
            .and_then(|counts| counts.parent_participants.clone())
            .ok_or_else(|| invalid("abort lost its durable declaration"))?;
        plan.validate_for_job(&current)?;
        if plan.participants != original.participants {
            return Err(invalid("abort changes original participants"));
        }
        if plan.phase == Phase::Aborting {
            *job = current;
            return Ok(());
        }
        let rejected_root = reason == Rejection::RootAdoptionRejected
            && plan.phase == Phase::Issued && current.initial_directory_root_adoption().is_some();
        if !rejected_root && (plan.phase != Phase::Preparing || current.processed_object_count != 0) {
            return Err(invalid(
                "namespace issuance makes parent recovery forward-only",
            ));
        }
        plan.phase = Phase::Aborting;
        plan.confirmed = 0;
        plan.rejection = Some(reason);
        replace_plan(&mut current, plan)?;
        self.persist_namespace_job(&current)?;
        *job = current;
        Ok(())
    }

    pub(super) fn advance_parent_count_plan(
        &mut self,
        job: &mut DfsNamespaceJobRecord,
    ) -> Result<bool> {
        if job.kind != DfsNamespaceJobKind::RenameDirectoryAcrossShardPrimaries {
            return Ok(true);
        }
        let Some(mut plan) = job
            .directory_move_counts()?
            .and_then(|counts| counts.parent_participants.clone())
        else {
            return Ok(true);
        };
        loop {
            plan.validate_for_job(job)?;
            if plan.phase == Phase::Issued {
                if !matches!(
                    job.pending_frames.as_slice(),
                    [DfsNamespaceJobFrame::CrossShardDirFinish { .. }]
                ) {
                    return Ok(true);
                }
                plan.phase = Phase::Settling;
                plan.confirmed = 0;
                replace_plan(job, plan.clone())?;
                self.persist_namespace_job(job)?;
                #[cfg(test)]
                run_parent_count_test_hook(
                    &self.node_id,
                    &job.root_path,
                    ParentCountTestPoint::Settling,
                )?;
            }
            if plan.confirmed == plan.participants.len() {
                match plan.phase {
                    Phase::Preparing => {
                        // This durable issuance decision precedes every child
                        // namespace effect; errors thereafter are forward-only.
                        plan.phase = Phase::Issued;
                        plan.confirmed = 0;
                        replace_plan(job, plan)?;
                        self.persist_namespace_job(job)?;
                        #[cfg(test)]
                        run_parent_count_test_hook(
                            &self.node_id,
                            &job.root_path,
                            ParentCountTestPoint::Issued,
                        )?;
                        return Ok(true);
                    }
                    Phase::Settling => return Ok(true),
                    Phase::Aborting => {
                        *job = self.fail_namespace_job(
                            job.clone(),
                            format!(
                                "parent reservation withdrawn before namespace effects: {:?}",
                                plan.rejection
                            ),
                        )?;
                        return Ok(false);
                    }
                    Phase::Issued => unreachable!(),
                }
            }
            let participant = plan.participants[plan.confirmed].clone();
            let action = match plan.phase {
                Phase::Preparing => Action::Reserve,
                Phase::Settling => Action::Apply,
                Phase::Aborting => Action::Abort,
                Phase::Issued => unreachable!(),
            };
            #[cfg(test)]
            if action == Action::Reserve && plan.confirmed == 0 {
                if let Err(error) = run_parent_count_test_hook(
                    &self.node_id,
                    &job.root_path,
                    ParentCountTestPoint::Declared,
                ) {
                    self.begin_parent_count_abort(job, Rejection::ReservationUnavailable)?;
                    return Err(error);
                }
            }
            let current = self
                .namespace_shards_for_exact_paths(
                    &participant.expected.file_system_id,
                    [participant.expected.path.as_str()],
                )?
                .into_iter()
                .next()
                .ok_or_else(|| invalid("declared parent has no current assignment"))?;
            if current.shard_id != participant.parent_scope.shard_id {
                return Err(invalid("declared parent moved to another physical shard"));
            }
            // A participant on a parent shard whose storage is closed waits
            // there before any effect (see `closed_storage_parks`).
            let next_frame = job.pending_frames.last().cloned();
            if let Some(frame) = &next_frame
                && !self.is_namespace_job_coordinator_shard(job, &current)
                && self.shard_storage_record_is_closed(&current)?
            {
                return Err(self.park_namespace_step_on_shard_storage(
                    job,
                    frame,
                    vec![current],
                    participant.expected.path.clone(),
                ));
            }
            let command = DfsNamespaceOperation::ParentCountParticipant {
                coordinator_job: Box::new(job.clone()),
                participant: Box::new(participant.clone()),
                action: action.clone(),
            };
            let response = if current.primary_node_id == self.node_id {
                self.execute_posix_coordination_operation_local(&command)
            } else {
                self.route_cross_shard_job_phase_with_result(
                    Some(job),
                    &participant.operation_id(&action),
                    current.primary_node_id.clone(),
                    command,
                )
            };
            let response = match response {
                Ok(response) => response,
                Err(error) => {
                    // A parent shard whose storage is closed took no effect:
                    // the phase waits there.
                    if let Some(frame) = &next_frame
                        && super::closed_storage_parks::posix_error_is_closed_shard_storage(&error)
                    {
                        return Err(self.park_namespace_step_on_shard_storage(
                            job,
                            frame,
                            vec![current],
                            participant.expected.path.clone(),
                        ));
                    }
                    // Preparing is durable proof that no child effect was
                    // issued. Unknown reservation delivery is safe to abort
                    // only through terminal withdrawals of every declaration.
                    if action == Action::Reserve && self.remote_namespace_phase.is_none() {
                        self.begin_parent_count_abort(job, Rejection::ReservationUnavailable)?;
                    }
                    return Err(error);
                }
            };
            let outcome = match response {
                Some(DfsPosixCoordinationResult::ParentCountParticipant {
                    participant_id,
                    outcome,
                }) if participant_id == participant.participant_id => outcome,
                _ => {
                    return Err(invalid(
                        "parent authority omitted its exact participant acknowledgement",
                    ));
                }
            };
            match (&action, outcome) {
                (Action::Reserve, Outcome::Reserved)
                | (Action::Apply, Outcome::Applied)
                | (Action::Abort, Outcome::Aborted) => {
                    #[cfg(test)]
                    if let Err(error) = run_parent_count_test_hook(
                        &self.node_id,
                        &job.root_path,
                        match action {
                            Action::Reserve => ParentCountTestPoint::Reserved,
                            Action::Apply => ParentCountTestPoint::Applied,
                            Action::Abort => ParentCountTestPoint::Aborted,
                        },
                    ) {
                        if action == Action::Reserve {
                            self.begin_parent_count_abort(job, Rejection::ReservationUnavailable)?;
                        }
                        return Err(error);
                    }
                    plan.confirmed += 1;
                }
                (Action::Reserve, Outcome::Rejected { reason }) => {
                    self.begin_parent_count_abort(job, reason)?;
                    plan = job
                        .directory_move_counts()?
                        .and_then(|counts| counts.parent_participants.clone())
                        .expect("persisted abort plan");
                    continue;
                }
                _ => {
                    return Err(invalid(
                        "parent authority returned an incompatible terminal decision",
                    ));
                }
            }
            replace_plan(job, plan.clone())?;
            self.persist_namespace_job(job)?;
        }
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) enum ParentCountTestPoint {
    Declared,
    Reserved,
    Issued,
    Settling,
    Applied,
    Aborted,
}
#[cfg(test)]
type ParentCountHook = Box<dyn FnOnce() -> Result<()> + Send>;
#[cfg(test)]
static PARENT_COUNT_HOOKS: std::sync::Mutex<
    std::collections::BTreeMap<(NodeId, String, ParentCountTestPoint), ParentCountHook>,
> = std::sync::Mutex::new(std::collections::BTreeMap::new());
#[cfg(test)]
pub(crate) struct ParentCountTestHookGuard((NodeId, String, ParentCountTestPoint));
#[cfg(test)]
impl Drop for ParentCountTestHookGuard {
    fn drop(&mut self) {
        PARENT_COUNT_HOOKS
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&self.0);
    }
}
#[cfg(test)]
pub(crate) fn install_parent_count_test_hook(
    node: &NodeId,
    path: &str,
    point: ParentCountTestPoint,
    hook: impl FnOnce() -> Result<()> + Send + 'static,
) -> ParentCountTestHookGuard {
    let key = (node.clone(), path.to_owned(), point);
    assert!(
        PARENT_COUNT_HOOKS
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(key.clone(), Box::new(hook))
            .is_none(),
        "duplicate parent-count test hook"
    );
    ParentCountTestHookGuard(key)
}
#[cfg(test)]
fn run_parent_count_test_hook(
    node: &NodeId,
    path: &str,
    point: ParentCountTestPoint,
) -> Result<()> {
    let hook = PARENT_COUNT_HOOKS
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .remove(&(node.clone(), path.to_owned(), point));
    if let Some(hook) = hook {
        hook()?;
    }
    Ok(())
}

fn phase_state_for_record(
    participant: &Participant,
    record: &crate::DfsPreparedOperationRecord,
) -> Result<State> {
    record.validate()?;
    Ok(participant.validate_phase_payload(&record.payload)?)
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    fn parent_phase_family_page(
        &self,
        participant: &Participant,
        after: Option<&str>,
    ) -> Result<(Vec<crate::DfsPreparedOperationRecord>, Option<String>)> {
        self.check_cancelled("parent participant recovery")?;
        if self
            .transport
            .operation_deadline()
            .is_some_and(|deadline| std::time::Instant::now() >= deadline)
        {
            return Err(invalid(
                "parent participant recovery exceeded its original deadline",
            ));
        }
        let backend = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| invalid("parent recovery has no native backend"))?;
        Ok(backend.prepared_operation_family_page(
            &DfsMetadataShardId::new(&participant.parent_scope.shard_id),
            &format!("{}:", participant.participant_id),
            after,
            16,
        )?)
    }

    fn observe_committed_parent_phase(
        &mut self,
        participant: &Participant,
    ) -> Result<Option<Outcome>> {
        let mut after = None;
        let mut terminal = None;
        loop {
            let (records, next) = self.parent_phase_family_page(participant, after.as_deref())?;
            for record in records {
                let state = phase_state_for_record(participant, &record)?;
                if record.status != crate::DfsPreparedOperationStatus::Committed
                    || state == State::Reserved
                {
                    continue;
                }
                let backend = self.metadata_backend.as_ref().expect("checked backend");
                let prefix = backend
                    .metadata_prefix_proof(&record.shard_id)?
                    .ok_or_else(|| invalid("parent terminal has no exact current prefix"))?;
                let Some(Some(qualified)) = backend.prepared_operation_record_at_prefix(
                    &record.shard_id,
                    &prefix,
                    &record.operation_id,
                )?
                else {
                    return Err(invalid(
                        "parent terminal cannot be corroborated at its current canonical cut",
                    ));
                };
                if qualified.digest != record.digest
                    || qualified.status != crate::DfsPreparedOperationStatus::Committed
                    || qualified.committed_entry.is_none()
                {
                    return Err(invalid("parent terminal changed at the canonical cut"));
                }
                if terminal.as_ref().is_some_and(|prior| *prior != state) {
                    return Err(invalid(
                        "parent phase history contains conflicting terminal decisions",
                    ));
                }
                terminal = Some(state);
            }
            let Some(next) = next else {
                break;
            };
            after = Some(next);
        }
        Ok(terminal.map(|state| match state {
            State::Applied => Outcome::Applied,
            State::Aborted => Outcome::Aborted,
            State::Reserved => unreachable!(),
        }))
    }

    fn parent_phase_attempt(
        &mut self,
        participant: &Participant,
        active: &crate::DfsMetadataShardConfig,
        requested: &Action,
    ) -> Result<Option<crate::DfsPreparedOperationPayload>> {
        let desired = match requested {
            Action::Reserve => State::Reserved,
            Action::Apply => State::Applied,
            Action::Abort => State::Aborted,
        };
        let current_id = participant.operation_id_at_epoch(requested, active.epoch);
        if let Some(current) = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| invalid("parent recovery has no native backend"))?
            .prepared_operation_record(&current_id)?
        {
            if phase_state_for_record(participant, &current)? != desired
                || matches!(
                    current.status,
                    crate::DfsPreparedOperationStatus::Aborted
                        | crate::DfsPreparedOperationStatus::Superseded
                )
            {
                return Err(invalid(
                    "current parent attempt is not its original live phase",
                ));
            }
            // Its entire body was already sealed, including timestamps and
            // inherited resolution edges. Recovery never reconstructs it.
            return Ok(Some(current.payload.as_ref().clone()));
        }
        let mut retiring = Vec::new();
        let mut after = None;
        loop {
            let (records, next) = self.parent_phase_family_page(participant, after.as_deref())?;
            for record in records {
                let state = phase_state_for_record(participant, &record)?;
                if record.status != crate::DfsPreparedOperationStatus::Prepared {
                    continue;
                }
                if *requested == Action::Abort && state == State::Reserved {
                    continue;
                }
                if state != desired {
                    return Err(invalid("parent replacement cannot retire another phase"));
                }
                // The shard's current primary re-issues what an earlier
                // epoch's primary prepared: the replacement only commits the
                // coordinator's own prepared phase.
                if record.owner_epoch >= active.epoch {
                    return Err(invalid("parent phase attempt is not from an earlier epoch"));
                }
                retiring.push(record);
            }
            let Some(next) = next else {
                break;
            };
            after = Some(next);
        }
        let Some(latest) = retiring.iter().max_by_key(|record| record.owner_epoch) else {
            return Ok(None);
        };
        let mut replacement = latest.payload.as_ref().clone();
        replacement.operation_id = current_id;
        replacement.owner_epoch = active.epoch;
        replacement.metadata_effects.push(
            crate::DfsPreparedMetadataEffect::ResolvePreparedOperation {
                digest: latest.digest.clone(),
                status: crate::DfsPreparedOperationStatus::Superseded,
            },
        );
        // Missing/forked history is not repaired by inventing another edge.
        // The selected predecessor must already carry every older dependency.
        if retiring.iter().any(|record| !replacement.metadata_effects.iter().any(|effect| matches!(effect,
            crate::DfsPreparedMetadataEffect::ResolvePreparedOperation { digest, status: crate::DfsPreparedOperationStatus::Superseded }
                if *digest == record.digest))) {
            return Err(invalid("parent replacement predecessor omits an original dependency"));
        }
        Ok(Some(replacement))
    }

    /// The participant phase whose prepare recovery re-issues: an abort
    /// retires the reservations it was prepared over, and an apply follows
    /// its committed reservation, so the latest prepared phase decides.
    fn latest_prepared_parent_phase(&self, participant: &Participant) -> Result<Option<Action>> {
        let mut latest = None;
        let mut after = None;
        loop {
            let (records, next) = self.parent_phase_family_page(participant, after.as_deref())?;
            for record in records {
                if record.status != crate::DfsPreparedOperationStatus::Prepared {
                    continue;
                }
                let action = match phase_state_for_record(participant, &record)? {
                    State::Reserved => Action::Reserve,
                    State::Applied => Action::Apply,
                    State::Aborted => Action::Abort,
                };
                let rank = |action: &Action| match action {
                    Action::Reserve => 0,
                    Action::Apply => 1,
                    Action::Abort => 2,
                };
                if latest
                    .as_ref()
                    .is_none_or(|latest| rank(&action) > rank(latest))
                {
                    latest = Some(action);
                }
            }
            let Some(next) = next else {
                break;
            };
            after = Some(next);
        }
        Ok(latest)
    }

    /// Recover a participant's phase that an earlier epoch's primary
    /// prepared: the shard's current primary re-issues the participant's
    /// latest prepared phase under the participant's own id at its epoch,
    /// superseding the earlier attempts, and commits it. A phase prepared at
    /// this epoch is the coordinator's to send again. Returns whether it
    /// re-issued.
    pub(crate) fn recover_parent_count_participant_prepare(
        &mut self,
        record: &crate::DfsPreparedOperationRecord,
        participant: &Participant,
    ) -> std::result::Result<bool, DfsError> {
        let backend = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| DfsError::Metadata {
                message: "parent recovery has no native backend".into(),
            })?;
        // An earlier body of this recovery pass may have resolved it.
        if backend
            .prepared_operation_record(&record.digest.operation_id)?
            .is_none_or(|current| current.status != crate::DfsPreparedOperationStatus::Prepared)
        {
            return Ok(false);
        }
        let Some(context) = self.retained_metadata_replica_context(
            &record.digest.file_system_id,
            &record.digest.shard_id,
        )?
        else {
            return Ok(false);
        };
        let shard = context.shard;
        if shard.primary_node_id != self.node_id || shard.epoch <= record.digest.owner_epoch {
            return Ok(false);
        }
        let reissue = (|| -> Result<Option<crate::DfsPreparedOperationPayload>> {
            let Some(action) = self.latest_prepared_parent_phase(participant)? else {
                return Ok(None);
            };
            self.parent_phase_attempt(participant, &shard, &action)
        })()
        .map_err(crate::service_routed_namespace::dfs_error_from_posix_namespace_sync)?;
        let Some(payload) = reissue else {
            return Ok(false);
        };
        let committed = self.publish_prepared_operation(payload)?;
        self.finish_recovered_prepared_publication(&committed.payload, committed.published)?;
        Ok(true)
    }
}

#[cfg(test)]
static PARENT_APPLY_COMMIT_HOLDS: std::sync::Mutex<
    std::collections::BTreeSet<(std::path::PathBuf, Option<u64>)>,
> = std::sync::Mutex::new(std::collections::BTreeSet::new());
#[cfg(test)]
pub(crate) struct ParentApplyCommitHold((std::path::PathBuf, Option<u64>));
#[cfg(test)]
impl Drop for ParentApplyCommitHold {
    fn drop(&mut self) {
        PARENT_APPLY_COMMIT_HOLDS
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&self.0);
    }
}
#[cfg(test)]
pub(crate) fn hold_parent_apply_commit(
    root: &std::path::Path,
    epoch: Option<u64>,
) -> ParentApplyCommitHold {
    let key = (root.to_owned(), epoch);
    assert!(
        PARENT_APPLY_COMMIT_HOLDS
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(key.clone())
    );
    ParentApplyCommitHold(key)
}
#[cfg(test)]
pub(crate) fn parent_apply_commit_is_held(
    root: &std::path::Path,
    record: &crate::DfsPreparedOperationRecord,
) -> bool {
    let held = {
        let holds = PARENT_APPLY_COMMIT_HOLDS
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        holds.contains(&(root.to_owned(), Some(record.owner_epoch)))
            || holds.contains(&(root.to_owned(), None))
    };
    held && record.metadata_effects.iter().any(|effect| matches!(effect,
            crate::DfsPreparedMetadataEffect::PutNamespaceJob { job }
                if job.parent_count_participant().is_some_and(|(_, state)| *state == State::Applied)))
}
