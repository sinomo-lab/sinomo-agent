//! Exact parent-write participants of an existing directory namespace job.
use super::*;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsParentCountBinding {
    pub job_id: String,
    pub file_system_id: DfsFileSystemId,
    pub root_path: String,
    pub target_path: String,
    pub coordinator_scope: crate::DfsMetadataShardConfig,
    pub source: DfsObjectMetadata,
    pub expected_destination: crate::DfsNamespaceDestinationPrecondition,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<DfsNamespaceJobOrigin>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsParentCountParticipant {
    pub participant_id: String,
    pub binding: DfsParentCountBinding,
    pub parent_scope: crate::DfsMetadataShardConfig,
    pub expected: DfsObjectMetadata,
    pub successor: DfsObjectMetadata,
    pub delta: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsParentCountPhase {
    Preparing,
    Issued,
    Settling,
    Aborting,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsParentCountPlan {
    pub participants: Vec<DfsParentCountParticipant>,
    pub phase: DfsParentCountPhase,
    pub confirmed: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rejection: Option<DfsParentCountRejection>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsParentCountAction {
    Reserve,
    Apply,
    Abort,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsParentCountState {
    Reserved,
    Applied,
    Aborted,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsParentCountRejection {
    Contended,
    ParentChanged,
    AssignmentChanged,
    ReservationUnavailable,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "state", rename_all = "kebab-case")]
pub enum DfsParentCountOutcome {
    Reserved,
    Applied,
    Aborted,
    Rejected { reason: DfsParentCountRejection },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsParentCountReceipt {
    pub participant_id: String,
    pub state: DfsParentCountState,
}

impl DfsParentCountParticipant {
    pub(crate) fn identity(&self) -> std::result::Result<String, crate::MetadataBackendError> {
        let bytes = serde_json::to_vec(&(
            &self.binding,
            &self.parent_scope,
            &self.expected,
            &self.successor,
            self.delta,
        ))
        .map_err(|error| crate::MetadataBackendError::Serde(error.to_string()))?;
        Ok(format!(
            "namespace-parent-count:{}",
            blake3::hash(&bytes).to_hex()
        ))
    }

    pub(crate) fn validate(&self) -> std::result::Result<(), crate::MetadataBackendError> {
        let fail = || {
            crate::MetadataBackendError::Serde("invalid original parent-count declaration".into())
        };
        let b = &self.binding;
        if self.participant_id != self.identity()?
            || b.job_id.is_empty()
            || b.file_system_id != self.expected.file_system_id
            || b.coordinator_scope.file_system_id != b.file_system_id
            || self.parent_scope.file_system_id != b.file_system_id
            || self.expected.object_id != self.successor.object_id
            || self.expected.file_system_id != self.successor.file_system_id
            || self.expected.path != self.successor.path
            || self.expected.kind != DfsObjectKind::Directory
            || self.successor.kind != DfsObjectKind::Directory
            || self.expected.version == self.successor.version
            || !matches!(self.delta, -1 | 1)
            || self
                .expected
                .namespace_link_count
                .and_then(|count| count.checked_add_signed(self.delta))
                != self.successor.namespace_link_count
            || self
                .successor
                .namespace_link_count
                .is_none_or(|count| count < 2)
            || self
                .expected
                .namespace_link_count
                .is_none_or(|count| count < 2)
        {
            return Err(fail());
        }
        let parent = &self.expected.path;
        if crate::posix_path::parent_posix_path(&b.root_path).as_ref() != Some(parent)
            && crate::posix_path::parent_posix_path(&b.target_path).as_ref() != Some(parent)
        {
            return Err(fail());
        }
        let mut allowed = self.expected.clone();
        allowed.namespace_link_count = self.successor.namespace_link_count;
        allowed.version = self.successor.version.clone();
        allowed.modified_at_unix_nanos = self.successor.modified_at_unix_nanos;
        allowed.changed_at_unix_nanos = self.successor.changed_at_unix_nanos;
        if allowed != self.successor {
            return Err(fail());
        }
        Ok(())
    }

    /// The normal durable version row uses the already sealed generation
    /// timestamp, never the clock of a retry or a successor authority.
    pub(crate) fn successor_version(
        &self,
    ) -> std::result::Result<DfsObjectVersionRecord, crate::MetadataBackendError> {
        let mut version = crate::object_version_record(&self.successor, None);
        version.created_at = OffsetDateTime::from_unix_timestamp_nanos(i128::from(
            self.successor.changed_at_unix_nanos,
        ))
        .map_err(|error| crate::MetadataBackendError::Serde(error.to_string()))?;
        Ok(version)
    }

    pub(crate) fn operation_id(&self, action: &DfsParentCountAction) -> String {
        self.operation_id_at_epoch(action, self.parent_scope.epoch)
    }

    pub(crate) fn operation_id_at_epoch(
        &self,
        action: &DfsParentCountAction,
        epoch: u64,
    ) -> String {
        let phase = match action {
            DfsParentCountAction::Reserve => "reserve",
            DfsParentCountAction::Apply => "apply",
            DfsParentCountAction::Abort => "abort",
        };
        format!("{}:{phase}:epoch:{epoch}", self.participant_id)
    }

    pub(crate) fn matches_job(
        &self,
        job: &DfsNamespaceJobRecord,
        state: &DfsParentCountState,
    ) -> bool {
        let mut expected = self.job(state.clone());
        expected.updated_at = job.updated_at;
        expected.completed_at = job.completed_at;
        expected == *job
            && job.updated_at >= job.created_at
            && match state {
                DfsParentCountState::Reserved => job.completed_at.is_none(),
                _ => job.completed_at.is_some_and(|time| time >= job.created_at),
            }
    }

    pub(crate) fn job(&self, state: DfsParentCountState) -> DfsNamespaceJobRecord {
        DfsNamespaceJobRecord {
            job_id: self.participant_id.clone(),
            file_system_id: self.expected.file_system_id.clone(),
            origin: self.binding.origin.clone(),
            kind: DfsNamespaceJobKind::ParentCountParticipant,
            status: if state == DfsParentCountState::Reserved {
                DfsNamespaceJobStatus::Running
            } else {
                DfsNamespaceJobStatus::Completed
            },
            root_path: self.expected.path.clone(),
            target_path: None,
            pending_frames: vec![DfsNamespaceJobFrame::ParentCountParticipant {
                participant: Box::new(self.clone()),
                state: state.clone(),
            }],
            processed_object_count: 0,
            created_at: self.binding.created_at,
            updated_at: self.binding.created_at,
            completed_at: (state != DfsParentCountState::Reserved)
                .then_some(self.binding.created_at),
            failure_reason: None,
        }
    }
}

impl DfsNamespaceJobRecord {
    pub(crate) fn parent_count_participant(
        &self,
    ) -> Option<(&DfsParentCountParticipant, &DfsParentCountState)> {
        if self.kind != DfsNamespaceJobKind::ParentCountParticipant {
            return None;
        }
        match self.pending_frames.as_slice() {
            [DfsNamespaceJobFrame::ParentCountParticipant { participant, state }] => {
                Some((participant, state))
            }
            _ => None,
        }
    }
}

impl DfsParentCountPlan {
    pub(crate) fn validate_for_job(
        &self,
        job: &DfsNamespaceJobRecord,
    ) -> std::result::Result<(), crate::MetadataBackendError> {
        let fail = || {
            crate::MetadataBackendError::Serde(
                "parent participants differ from their original namespace job".into(),
            )
        };
        if job.kind != DfsNamespaceJobKind::RenameDirectoryAcrossShardPrimaries
            || job.status != DfsNamespaceJobStatus::Running
            || self.participants.is_empty()
            || self.participants.len() > 2
            || self.confirmed > self.participants.len()
        {
            return Err(fail());
        }
        if (self.phase == DfsParentCountPhase::Aborting) != self.rejection.is_some() {
            return Err(fail());
        }
        let counts = job.directory_move_counts()?.ok_or_else(fail)?;
        if counts.parent_participants.as_ref() != Some(self) {
            return Err(fail());
        }
        let mut previous = None;
        for participant in &self.participants {
            participant.validate()?;
            let b = &participant.binding;
            if b.job_id != job.job_id
                || b.file_system_id != job.file_system_id
                || b.root_path != job.root_path
                || Some(&b.target_path) != job.target_path.as_ref()
                || b.origin != job.origin
                || b.created_at != job.created_at
                || b.expected_destination != counts.expected_destination
                || b.source.path != b.root_path
                || b.source.kind != DfsObjectKind::Directory
                || b.source.file_system_id != b.file_system_id
            {
                return Err(fail());
            }
            let key = (
                participant.parent_scope.shard_id.as_str(),
                participant.expected.object_id.as_str(),
            );
            if previous.is_some_and(|old| old >= key) {
                return Err(fail());
            }
            previous = Some(key);
            let parent_path = &participant.expected.path;
            let delta = if crate::posix_path::parent_posix_path(&job.root_path).as_ref()
                == Some(parent_path)
            {
                counts.source_parent_delta
            } else {
                0
            } + if job
                .target_path
                .as_ref()
                .and_then(|path| crate::posix_path::parent_posix_path(path))
                .as_ref()
                == Some(parent_path)
            {
                counts.target_parent_delta
            } else {
                0
            };
            if delta != participant.delta {
                return Err(fail());
            }
        }
        if self.phase == DfsParentCountPhase::Aborting && job.processed_object_count != 0 {
            return Err(fail());
        }
        if self.phase == DfsParentCountPhase::Settling
            && !matches!(
                job.pending_frames.as_slice(),
                [DfsNamespaceJobFrame::CrossShardDirFinish { .. }]
            )
        {
            return Err(fail());
        }
        Ok(())
    }
}

impl DfsDirectoryMoveCounts {
    pub(crate) fn parent_is_participant(&self, child: &str) -> bool {
        let parent = crate::posix_path::parent_posix_path(child);
        self.parent_participants.as_ref().is_some_and(|plan| {
            plan.participants
                .iter()
                .any(|p| Some(&p.expected.path) == parent.as_ref())
        })
    }
    pub(crate) fn source_effect_delta(&self, child: &str) -> i64 {
        if self.parent_is_participant(child) {
            0
        } else {
            self.source_parent_delta
        }
    }
    pub(crate) fn target_effect_delta(&self, child: &str) -> i64 {
        if self.parent_is_participant(child) {
            0
        } else {
            self.target_parent_delta
        }
    }
}

impl DfsParentCountParticipant {
    pub(crate) fn validate_phase_payload(
        &self,
        record: &crate::DfsPreparedOperationPayload,
    ) -> std::result::Result<DfsParentCountState, crate::MetadataBackendError> {
        let fail = |message: &str| crate::MetadataBackendError::Serde(message.into());
        self.validate()?;

        let mut state = None;
        for effect in &record.metadata_effects {
            match effect {
                crate::DfsPreparedMetadataEffect::PutNamespaceJob { job } => {
                    let (original, observed) = job
                        .parent_count_participant()
                        .ok_or_else(|| fail("parent phase history contains another job kind"))?;
                    if original != self || !self.matches_job(job, observed) || state.is_some() {
                        return Err(fail(
                            "parent phase history changes its original declaration",
                        ));
                    }
                    state = Some(observed.clone());
                }
                crate::DfsPreparedMetadataEffect::ResolvePreparedOperation { digest, status }
                    if *status == crate::DfsPreparedOperationStatus::Aborted
                        && digest.operation_id
                            == self.operation_id_at_epoch(
                                &DfsParentCountAction::Reserve,
                                digest.owner_epoch,
                            )
                        && digest.file_system_id == self.expected.file_system_id
                        && digest.shard_id.0 == self.parent_scope.shard_id => {}
                crate::DfsPreparedMetadataEffect::ResolvePreparedOperation {
                    status: crate::DfsPreparedOperationStatus::Superseded,
                    ..
                } => {}
                _ => {
                    return Err(fail("parent phase history contains an undeclared effect"));
                }
            }
        }
        let state = state.ok_or_else(|| fail("parent phase history omitted its decision"))?;
        let action = match state {
            DfsParentCountState::Reserved => DfsParentCountAction::Reserve,
            DfsParentCountState::Applied => DfsParentCountAction::Apply,
            DfsParentCountState::Aborted => DfsParentCountAction::Abort,
        };
        if record.operation_id != self.operation_id_at_epoch(&action, record.owner_epoch)
            || record.file_system_id != self.expected.file_system_id
            || record.shard_id.0 != self.parent_scope.shard_id
            || !record.transaction.advisory_lock_states.is_empty()
            || !record.content_receipts.is_empty()
            || record.prepared_intent.is_some()
            || record.terminal_intent.is_some()
            || record.owner_epoch < self.parent_scope.epoch
        {
            return Err(fail(
                "parent phase history changes its scope or operation identity",
            ));
        }
        for effect in &record.metadata_effects {
            if matches!(
                effect,
                crate::DfsPreparedMetadataEffect::ResolvePreparedOperation {
                    status: crate::DfsPreparedOperationStatus::Aborted,
                    ..
                }
            ) && state != DfsParentCountState::Aborted
            {
                return Err(fail("only parent abort may retire a reserve preparation"));
            }
            if let crate::DfsPreparedMetadataEffect::ResolvePreparedOperation {
                digest,
                status: crate::DfsPreparedOperationStatus::Superseded,
            } = effect
            {
                if digest.operation_id != self.operation_id_at_epoch(&action, digest.owner_epoch)
                    || digest.owner_epoch >= record.owner_epoch
                    || digest.file_system_id != record.file_system_id
                    || digest.shard_id != record.shard_id
                {
                    return Err(fail(
                        "parent supersession crosses its original participant phase",
                    ));
                }
            }
        }
        let versions = if state == DfsParentCountState::Applied {
            vec![self.successor_version()?]
        } else {
            Vec::new()
        };
        if record.transaction.versions != versions {
            return Err(fail(
                "parent phase changes its sealed directory version record",
            ));
        }
        if state == DfsParentCountState::Applied {
            if record.transaction.objects.as_slice() != [self.successor.clone()] {
                return Err(fail("parent phase history changes its sealed successor"));
            }
        } else if !record.transaction.objects.is_empty() {
            return Err(fail("non-applying parent phase changes namespace rows"));
        }
        let expected_objects = if state == DfsParentCountState::Aborted {
            Vec::new()
        } else {
            vec![crate::DfsNamespaceObjectVersionExpectation {
                object_id: self.expected.object_id.clone(),
                expected_version: Some(self.expected.version.clone()),
            }]
        };
        let expected_paths = if state == DfsParentCountState::Aborted {
            Vec::new()
        } else {
            vec![crate::DfsNamespacePathOccupantExpectation {
                path: crate::metadata_keys::normalize_path(&self.expected.path),
                expected_object_id: Some(self.expected.object_id.clone()),
            }]
        };
        if record.expected_object_versions != expected_objects
            || record.expected_path_occupants != expected_paths
        {
            return Err(fail("parent phase history changes its exact preconditions"));
        }
        Ok(state)
    }
}
