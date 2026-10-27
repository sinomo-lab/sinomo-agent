//! Dormant selected-private DATA. Supplied frames are not transport evidence.
use super::*;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RepairCompletionCandidateProvenance {
    FixtureUnverifiedTransportMaterial,
}

/// Correlation inputs only; none of these caller-supplied fields is authority.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RepairCompletionCandidateInput {
    pub(crate) queue_incarnation: String,
    pub(crate) original_command_digest: String,
    pub(crate) attempt_sequence: u64,
    pub(crate) attempted_carrier: crate::DfsRoutedMessage,
    pub(crate) returned_frame: crate::DfsRoutedMessage,
    pub(crate) provenance: RepairCompletionCandidateProvenance,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(super) struct RepairCompletionCandidateKey {
    subject: crate::content_store::RepairPayloadSubject,
    attempt_sequence: u64,
}

/// A retained DATA reference, never an observation/application/closure permit.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg(any(test, feature = "test-support"))]
pub(crate) struct RepairCompletionCandidateRef {
    queue_incarnation: String,
    subject: crate::content_store::RepairPayloadSubject,
    attempt_sequence: u64,
    retained_at_sequence: u64,
    material_digest: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RetainedRepairCompletionCandidate {
    pub(crate) input: RepairCompletionCandidateInput,
    retained_at_sequence: u64,
}

impl RepairCompletionCandidateInput {
    fn key(&self) -> Result<RepairCompletionCandidateKey, DfsError> {
        Ok(RepairCompletionCandidateKey {
            subject: repair_subject(&self.queue_incarnation, &self.attempted_carrier)?,
            attempt_sequence: self.attempt_sequence,
        })
    }

    #[cfg(any(test, feature = "test-support"))]
    fn material_digest(&self) -> Result<String, DfsError> {
        #[derive(Serialize)]
        struct DigestInput<'a> {
            domain: &'static str,
            material: &'a RepairCompletionCandidateInput,
        }
        let bytes = sinomo_object_serde::to_bytes(
            &DigestInput {
                domain: "sinomo.dfs.fixture-unverified-completion-candidate.v1",
                material: self,
            },
            1,
            LengthMode::U32,
        )
        .map_err(|error| custody_error(&format!("encode completion candidate: {error}")))?;
        Ok(blake3::hash(&bytes).to_hex().to_string())
    }
}

impl RetainedRepairCompletionCandidate {
    // Shared local prefix-conflict checking only; this exposes no native
    // provenance and never promotes the candidate into another record family.
    pub(super) fn retained_sequence(&self) -> u64 {
        self.retained_at_sequence
    }

    #[cfg(any(test, feature = "test-support"))]
    fn reference(&self) -> Result<RepairCompletionCandidateRef, DfsError> {
        let key = self.input.key()?;
        Ok(RepairCompletionCandidateRef {
            queue_incarnation: self.input.queue_incarnation.clone(),
            subject: key.subject,
            attempt_sequence: key.attempt_sequence,
            retained_at_sequence: self.retained_at_sequence,
            material_digest: self.input.material_digest()?,
        })
    }
}

pub(super) fn validate_candidate_frame(
    operations: &[DfsRepairQueueOperation],
) -> Result<(), DfsError> {
    if operations.iter().any(|operation| {
        matches!(operation, DfsRepairQueueOperation::RepairCustody { change }
            if matches!(change.as_ref(), RepairCustodyChange::RetainCompletionCandidate { .. }))
    }) && operations.len() != 1
    {
        return Err(custody_error(
            "completion candidate requires its own DATA-only frame",
        ));
    }
    Ok(())
}

impl RepairCustodyIndex {
    fn validate_completion_candidate_input(
        &self,
        input: &RepairCompletionCandidateInput,
    ) -> Result<RepairCompletionCandidateKey, DfsError> {
        if self.incarnation.is_empty() || input.queue_incarnation != self.incarnation {
            return Err(custody_error(
                "completion candidate has another queue incarnation",
            ));
        }
        let key = input.key()?;
        let birth = self.births.get(&key.subject).ok_or_else(|| {
            custody_error("completion candidate has no exact native original birth")
        })?;
        birth.validate(&self.incarnation)?;
        if input.original_command_digest != birth.root.original_command_digest
            || original_repair_digest(&input.attempted_carrier, &birth.attachment)?
                != birth.root.original_command_digest
            || !birth.attempts.iter().any(|attempt| {
                attempt.sequence != 0
                    && attempt.sequence == input.attempt_sequence
                    && attempt.frame_id == input.attempted_carrier.frame_id
                    && attempt.message_id == input.attempted_carrier.message_id
            })
        {
            return Err(custody_error(
                "completion candidate differs from its original digest or persisted attempt",
            ));
        }
        let crate::DfsNetworkMessage::RepairReplicaResponse(response) =
            &input.returned_frame.message
        else {
            return Err(custody_error(
                "completion candidate has another response family",
            ));
        };
        if input.returned_frame.source_node_id != birth.original.target_node_id
            || input.returned_frame.target_node_id != birth.original.source_node_id
        {
            return Err(custody_error(
                "completion candidate response frame scope differs",
            ));
        }
        birth.validate_response(response)?;
        Ok(key)
    }

    pub(super) fn validate_new_completion_candidate(
        &self,
        input: &RepairCompletionCandidateInput,
    ) -> Result<(), DfsError> {
        let key = self.validate_completion_candidate_input(input)?;
        if self.retained_completion_candidates.contains_key(&key) {
            return Err(custody_error(
                "completion candidate attempt already has immutable retained material",
            ));
        }
        Ok(())
    }

    pub(super) fn restore_completion_candidate(
        &mut self,
        candidate: RetainedRepairCompletionCandidate,
        snapshot_floor: u64,
    ) -> Result<(), DfsError> {
        let key = self.validate_completion_candidate_input(&candidate.input)?;
        let sequence = candidate.retained_at_sequence;
        if sequence <= candidate.input.attempt_sequence
            || sequence > snapshot_floor
            || self.retained_completion_candidates.contains_key(&key)
            || self
                .retained_completion_candidates
                .values()
                .any(|old| old.retained_at_sequence == sequence)
            || self.history_sequences.contains_key(&sequence)
            || self
                .imported_executor_associations
                .values()
                .any(|association| association.associated_at_sequence == sequence)
            || self.births.values().any(|birth| {
                birth.closed_at_sequence == Some(sequence)
                    || birth
                        .attempts
                        .iter()
                        .any(|attempt| attempt.sequence == sequence)
            })
        {
            return Err(custody_error(
                "completion candidate has a conflicting snapshot prefix",
            ));
        }
        self.retained_completion_candidates.insert(key, candidate);
        Ok(())
    }

    pub(super) fn apply_completion_candidate(
        &mut self,
        input: &RepairCompletionCandidateInput,
        sequence: u64,
    ) -> Result<(), DfsError> {
        self.restore_completion_candidate(
            RetainedRepairCompletionCandidate {
                input: input.clone(),
                retained_at_sequence: sequence,
            },
            sequence,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixture {
        queue: LocalDfsRepairQueueStore,
        identity: SelectedOriginalFixtureIdentity,
        content: crate::LocalContentStore,
        root: crate::content_store::RepairPayloadRoot,
        input: RepairCompletionCandidateInput,
    }

    fn fixture(root: &Path, with_attempt: bool) -> Fixture {
        fs::create_dir_all(root).unwrap();
        let (queue, content, original, descriptor, payload, identity) =
            super::super::tests::selected_original_fixture(&root.join("queue"));
        queue
            .enroll_original_repair(&original, &descriptor, &payload)
            .unwrap();
        let mut carrier = original.clone();
        carrier.frame_id += 100;
        carrier.message_id += 100;
        if with_attempt {
            queue.record_original_repair_attempt(&carrier).unwrap();
        }
        let birth = queue.original_repair(&original).unwrap().unwrap();
        let crate::DfsNetworkMessage::RepairReplica(command) = &original.message else {
            panic!("actual original fixture");
        };
        let returned_frame = crate::DfsRoutedMessage {
            source_node_id: original.target_node_id.clone(),
            target_node_id: original.source_node_id.clone(),
            frame_id: 901,
            message_id: 902,
            message: crate::DfsNetworkMessage::RepairReplicaResponse(
                crate::DfsRepairReplicaResponse {
                    request_id: command.request_id.clone(),
                    intent_id: command.intent_id.clone(),
                    file_system_id: command.file_system_id.clone(),
                    object_id: command.object_id.clone(),
                    version: command.version.clone(),
                    replica_node_id: original.target_node_id.clone(),
                    result: crate::DfsBlobTransferResult::Committed {
                        content_hash: command.content_hash.clone(),
                    },
                },
            ),
        };
        let input = RepairCompletionCandidateInput {
            queue_incarnation: birth.root.subject.queue_incarnation.clone(),
            original_command_digest: birth.root.original_command_digest.clone(),
            attempt_sequence: birth.attempts.last().map_or(1, |attempt| attempt.sequence),
            attempted_carrier: carrier,
            returned_frame,
            provenance: RepairCompletionCandidateProvenance::FixtureUnverifiedTransportMaterial,
        };
        Fixture {
            queue,
            identity,
            content,
            root: payload.root().clone(),
            input,
        }
    }

    fn files(identity: &SelectedOriginalFixtureIdentity) -> Vec<Option<Vec<u8>>> {
        [
            identity.snapshot_path().to_path_buf(),
            repair_queue_active_log_path(identity.snapshot_path()),
        ]
        .iter()
        .map(|path| match fs::read(path) {
            Ok(bytes) => Some(bytes),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => panic!("fixture file {}: {error}", path.display()),
        })
        .collect()
    }

    fn non_candidate_state(queue: &LocalDfsRepairQueueStore) -> String {
        let state = queue.inner.lock().unwrap();
        format!(
            "native={:?}; derived={:?}; scheduling={:?}",
            (
                &state.records,
                &state.custody.births,
                &state.custody.slots,
                &state.custody.closed_requests,
                &state.custody.observed_pending,
            ),
            (
                &state.custody.imported_original_data,
                &state.custody.imported_executor_associations,
                &state.custody.imported_history_queries,
                &state.custody.history_sequences,
                &state.custody.associated_slots,
            ),
            (
                state.revision,
                &state.maintenance_cursor,
                state.maintenance_restart_pending,
                state.maintenance_revisit_pending,
                &state.custody_recovery_cursor,
            )
        )
    }

    fn response(
        input: &mut RepairCompletionCandidateInput,
    ) -> &mut crate::DfsRepairReplicaResponse {
        let crate::DfsNetworkMessage::RepairReplicaResponse(response) =
            &mut input.returned_frame.message
        else {
            panic!("actual candidate response family");
        };
        response
    }

    #[test]
    fn completion_candidate_is_unverified_data_and_exact_duplicate_has_no_append() {
        let root = sinomo_testdir::TestDir::new("completion-candidate-data");
        let f = fixture(root.path(), true);
        let before = non_candidate_state(&f.queue);
        let head = f.queue.view().unwrap().head_sequence;
        let retained = f
            .queue
            .retain_repair_completion_candidate(f.input.clone())
            .unwrap();
        assert_eq!(retained.retained_at_sequence, head + 1);
        let data = f
            .queue
            .retained_repair_completion_candidate(&retained)
            .unwrap();
        assert_eq!(data.input, f.input);
        assert_eq!(
            data.input.provenance,
            RepairCompletionCandidateProvenance::FixtureUnverifiedTransportMaterial
        );
        assert_eq!(non_candidate_state(&f.queue), before);
        f.content.inspect_repair_payload_root(&f.root).unwrap();
        let bytes = files(&f.identity);
        assert_eq!(
            f.queue
                .retain_repair_completion_candidate(f.input.clone())
                .unwrap(),
            retained
        );
        assert_eq!(f.queue.view().unwrap().head_sequence, head + 1);
        assert_eq!(files(&f.identity), bytes);
        assert_eq!(non_candidate_state(&f.queue), before);
    }

    #[test]
    fn completion_candidate_cold_replay_and_compaction_preserve_data() {
        let root = sinomo_testdir::TestDir::new("completion-candidate-reopen");
        let f = fixture(root.path(), true);
        let reference = f
            .queue
            .retain_repair_completion_candidate(f.input.clone())
            .unwrap();
        let data = f
            .queue
            .retained_repair_completion_candidate(&reference)
            .unwrap();
        let expected_birth = f
            .queue
            .original_repair(&f.input.attempted_carrier)
            .unwrap()
            .unwrap();
        let expected_rows = f.queue.view().unwrap().records;
        drop(f.queue);
        LocalDfsRepairQueueStore::evict_for_test(f.identity.snapshot_path());
        let queue =
            LocalDfsRepairQueueStore::reopen_selected_original_fixture(&f.identity).unwrap();
        assert_eq!(
            queue
                .retained_repair_completion_candidate(&reference)
                .unwrap(),
            data
        );
        // Process-local revision is not a durable queue field; compare actual
        // birth/rows explicitly across a fresh resident generation.
        assert_eq!(
            queue.original_repair(&f.input.attempted_carrier).unwrap(),
            Some(expected_birth)
        );
        assert_eq!(queue.view().unwrap().records, expected_rows);
        let before = files(&f.identity);
        assert_eq!(
            queue
                .retain_repair_completion_candidate(f.input.clone())
                .unwrap(),
            reference
        );
        assert_eq!(files(&f.identity), before);
        queue
            .compact_if_needed(DfsRepairQueueCompactionOptions::force())
            .unwrap();
        let rows = queue.view().unwrap().records;
        let birth = queue
            .original_repair(&f.input.attempted_carrier)
            .unwrap()
            .unwrap();
        drop(queue);
        LocalDfsRepairQueueStore::evict_for_test(f.identity.snapshot_path());
        let queue =
            LocalDfsRepairQueueStore::reopen_selected_original_fixture(&f.identity).unwrap();
        assert_eq!(
            queue
                .retained_repair_completion_candidate(&reference)
                .unwrap(),
            data
        );
        assert_eq!(queue.view().unwrap().records, rows);
        assert_eq!(
            queue.original_repair(&f.input.attempted_carrier).unwrap(),
            Some(birth)
        );
        assert_eq!(
            queue.retain_repair_completion_candidate(f.input).unwrap(),
            reference
        );
        f.content.inspect_repair_payload_root(&f.root).unwrap();
    }

    #[test]
    fn completion_candidate_conflicts_and_wrong_material_preserve_files_and_indexes() {
        let root = sinomo_testdir::TestDir::new("completion-candidate-conflict");
        let f = fixture(root.path(), true);
        f.queue
            .retain_repair_completion_candidate(f.input.clone())
            .unwrap();
        let mut variants = Vec::new();
        let mut changed = f.input.clone();
        changed.returned_frame.frame_id += 1;
        variants.push(changed);
        let mut changed = f.input.clone();
        response(&mut changed).result = crate::DfsBlobTransferResult::Rejected {
            reason: "different nonempty result".into(),
        };
        variants.push(changed);
        let mut changed = f.input.clone();
        response(&mut changed).request_id.push_str("-changed");
        variants.push(changed);
        let mut changed = f.input.clone();
        response(&mut changed).intent_id.push_str("-changed");
        variants.push(changed);
        let mut changed = f.input.clone();
        response(&mut changed).object_id =
            crate::DfsObjectId::from_legacy_named("another-object").unwrap();
        variants.push(changed);
        let mut changed = f.input.clone();
        response(&mut changed).version =
            crate::DfsVersion::from_legacy_named("another-version").unwrap();
        variants.push(changed);
        let mut changed = f.input.clone();
        response(&mut changed).file_system_id =
            crate::DfsFileSystemId::from_legacy_named("another-filesystem").unwrap();
        variants.push(changed);
        let mut changed = f.input.clone();
        response(&mut changed).replica_node_id =
            NodeId::from_legacy_named("another-replica").unwrap();
        variants.push(changed);
        let mut changed = f.input.clone();
        response(&mut changed).result = crate::DfsBlobTransferResult::Committed {
            content_hash: "blake3:changed".into(),
        };
        variants.push(changed);
        let mut changed = f.input.clone();
        changed.returned_frame.target_node_id =
            NodeId::from_legacy_named("another-target").unwrap();
        variants.push(changed);
        let mut changed = f.input.clone();
        changed.returned_frame.source_node_id =
            NodeId::from_legacy_named("another-source").unwrap();
        variants.push(changed);
        let mut changed = f.input.clone();
        changed.returned_frame.message = f.input.attempted_carrier.message.clone();
        variants.push(changed);
        let before = files(&f.identity);
        let state = non_candidate_state(&f.queue);
        let head = f.queue.view().unwrap().head_sequence;
        for changed in variants {
            assert!(f.queue.retain_repair_completion_candidate(changed).is_err());
            assert_eq!(f.queue.view().unwrap().head_sequence, head);
            assert_eq!(files(&f.identity), before);
            assert_eq!(non_candidate_state(&f.queue), state);
        }
    }

    #[test]
    fn completion_candidate_requires_native_birth_attempt_and_exact_reference() {
        let root = sinomo_testdir::TestDir::new("completion-candidate-required-origin");
        let missing = fixture(&root.join("missing-attempt"), false);
        let before = files(&missing.identity);
        assert!(
            missing
                .queue
                .retain_repair_completion_candidate(missing.input.clone())
                .is_err()
        );
        assert_eq!(files(&missing.identity), before);
        let f = fixture(&root.join("attempt"), true);
        let mut invalid = Vec::new();
        let mut changed = f.input.clone();
        changed.queue_incarnation.push_str("-foreign");
        invalid.push(changed);
        let mut changed = f.input.clone();
        changed.original_command_digest.push_str("changed");
        invalid.push(changed);
        let mut changed = f.input.clone();
        changed.attempt_sequence += 100;
        invalid.push(changed);
        let mut changed = f.input.clone();
        changed.attempted_carrier.frame_id += 1;
        invalid.push(changed);
        let mut changed = f.input.clone();
        changed.attempted_carrier.source_node_id =
            NodeId::from_legacy_named("foreign-producer").unwrap();
        invalid.push(changed);
        let mut changed = f.input.clone();
        let crate::DfsNetworkMessage::RepairReplica(command) =
            &mut changed.attempted_carrier.message
        else {
            panic!("fixture");
        };
        command.request_id.push_str("-absent");
        invalid.push(changed);
        let before = files(&f.identity);
        for changed in invalid {
            assert!(f.queue.retain_repair_completion_candidate(changed).is_err());
            assert_eq!(files(&f.identity), before);
        }
        let reference = f
            .queue
            .retain_repair_completion_candidate(f.input.clone())
            .unwrap();
        let mut changed = reference.clone();
        changed.material_digest.push_str("wrong");
        assert!(
            f.queue
                .retained_repair_completion_candidate(&changed)
                .is_err()
        );
        let mut changed = reference;
        changed.queue_incarnation.push_str("wrong");
        assert!(
            f.queue
                .retained_repair_completion_candidate(&changed)
                .is_err()
        );
    }

    #[cfg(unix)]
    #[test]
    fn completion_candidate_rejects_foreign_queue_and_physical_replacement() {
        let root = sinomo_testdir::TestDir::new("completion-candidate-physical");
        let f = fixture(&root.join("first"), true);
        let other = fixture(&root.join("other"), true);
        let before = files(&other.identity);
        assert!(
            other
                .queue
                .retain_repair_completion_candidate(f.input.clone())
                .is_err()
        );
        assert_eq!(files(&other.identity), before);
        let reference = f
            .queue
            .retain_repair_completion_candidate(f.input.clone())
            .unwrap();
        let retained = root.join("retained-queue");
        fs::rename(f.identity.root(), &retained).unwrap();
        fs::create_dir(f.identity.root()).unwrap();
        // Copy the ACTUAL valid marker/log/snapshot bytes. The negative must
        // reach the pinned directory identity fence, not a missing marker.
        for entry in fs::read_dir(&retained).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_file() {
                fs::copy(entry.path(), f.identity.root().join(entry.file_name())).unwrap();
            }
        }
        let copied = files(&f.identity);
        let error = f
            .queue
            .retain_repair_completion_candidate(f.input)
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("selected original fixture was physically replaced")
        );
        let error = f
            .queue
            .retained_repair_completion_candidate(&reference)
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("selected original fixture was physically replaced")
        );
        assert_eq!(files(&f.identity), copied);
        fs::remove_dir_all(f.identity.root()).unwrap();
        fs::rename(&retained, f.identity.root()).unwrap();
        assert!(
            f.queue
                .retained_repair_completion_candidate(&reference)
                .is_ok()
        );
    }

    #[test]
    fn completion_candidate_default_api_and_actual_log_snapshot_grammar_reject() {
        let root = sinomo_testdir::TestDir::new("completion-candidate-default");
        let f = fixture(root.path(), true);
        let ordinary_path = root.join("ordinary.sobj");
        let ordinary = LocalDfsRepairQueueStore::new(&ordinary_path);
        assert!(
            ordinary
                .retain_repair_completion_candidate(f.input.clone())
                .is_err()
        );
        assert!(!ordinary_path.exists());
        assert!(!repair_queue_active_log_path(&ordinary_path).exists());
        let operation = DfsRepairQueueOperation::RepairCustody {
            change: Box::new(RepairCustodyChange::RetainCompletionCandidate {
                input: Box::new(f.input.clone()),
            }),
        };
        assert!(ordinary.append_operations(vec![operation.clone()]).is_err());
        assert!(f.queue.append_operations(vec![operation]).is_err());
        f.queue.retain_repair_completion_candidate(f.input).unwrap();
        let source_log =
            fs::read(repair_queue_active_log_path(f.identity.snapshot_path())).unwrap();
        let ordinary_log = repair_queue_active_log_path(&ordinary_path);
        fs::write(&ordinary_log, &source_log).unwrap();
        for _ in 0..2 {
            let error = ordinary.view().unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("default queue replay rejects selected original DATA")
            );
        }
        assert_eq!(fs::read(&ordinary_log).unwrap(), source_log);
        f.queue
            .compact_if_needed(DfsRepairQueueCompactionOptions::force())
            .unwrap();
        let snapshot = fs::read(f.identity.snapshot_path()).unwrap();
        fs::write(&ordinary_path, &snapshot).unwrap();
        fs::remove_file(&ordinary_log).unwrap();
        LocalDfsRepairQueueStore::evict_for_test(&ordinary_path);
        let ordinary = LocalDfsRepairQueueStore::new(&ordinary_path);
        let error = ordinary.view().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("default queue reader rejects selected original DATA")
        );
        assert_eq!(fs::read(&ordinary_path).unwrap(), snapshot);
        assert!(!ordinary_log.exists());
    }

    #[test]
    fn completion_candidate_snapshot_prefix_and_mixed_frame_reject_before_publish() {
        let root = sinomo_testdir::TestDir::new("completion-candidate-prefix");
        let f = fixture(root.path(), true);
        // A real second attempt makes a different known physical sequence.
        // The first recorded attempt remains valid historical candidate DATA.
        let mut second = f.input.attempted_carrier.clone();
        second.frame_id += 1;
        f.queue.record_original_repair_attempt(&second).unwrap();
        let second_sequence = f
            .queue
            .original_repair(&second)
            .unwrap()
            .unwrap()
            .attempts
            .last()
            .unwrap()
            .sequence;
        f.queue
            .retain_repair_completion_candidate(f.input.clone())
            .unwrap();
        let log_path = repair_queue_active_log_path(f.identity.snapshot_path());
        let original_log = fs::read(&log_path).unwrap();
        let mut reader = File::open(&log_path).unwrap();
        let mut last = None;
        while let Some(frame) = durable_file::read_length_prefixed_record(
            &mut reader,
            &log_path,
            durable_file::DurableRecordReadOptions::new(durable_file::DurableLengthEndian::Little)
                .max_record_bytes(DFS_REPAIR_QUEUE_LOG_MAX_RECORD_BYTES),
        )
        .unwrap()
        {
            last = Some(frame);
        }
        let last = last.unwrap();
        let mut batch: DfsRepairQueueLogBatch =
            sinomo_object_serde::from_bytes(&last.bytes).unwrap();
        batch.operations.push(batch.operations[0].clone());
        let bytes = encode_sobj(&batch).unwrap();
        let mut mixed = original_log[..last.record_start as usize].to_vec();
        mixed.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        mixed.extend_from_slice(&bytes);
        drop(f.queue);
        LocalDfsRepairQueueStore::evict_for_test(f.identity.snapshot_path());
        write_file_atomic(&log_path, &mixed).unwrap();
        let error =
            LocalDfsRepairQueueStore::reopen_selected_original_fixture(&f.identity).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("completion candidate requires its own DATA-only frame")
        );
        assert_eq!(fs::read(&log_path).unwrap(), mixed);
        LocalDfsRepairQueueStore::evict_for_test(f.identity.snapshot_path());
        write_file_atomic(&log_path, &original_log).unwrap();
        let queue =
            LocalDfsRepairQueueStore::reopen_selected_original_fixture(&f.identity).unwrap();
        queue
            .compact_if_needed(DfsRepairQueueCompactionOptions::force())
            .unwrap();
        let snapshot = read_repair_queue_snapshot(f.identity.snapshot_path()).unwrap();
        let good_bytes = fs::read(f.identity.snapshot_path()).unwrap();
        drop(queue);
        let mut broken = Vec::new();
        let mut changed = snapshot.clone();
        changed
            .custody
            .as_mut()
            .unwrap()
            .retained_completion_candidates[0]
            .retained_at_sequence = f.input.attempt_sequence;
        broken.push(changed);
        let mut changed = snapshot.clone();
        changed
            .custody
            .as_mut()
            .unwrap()
            .retained_completion_candidates[0]
            .retained_at_sequence = second_sequence;
        broken.push(changed);
        let mut changed = snapshot.clone();
        changed
            .custody
            .as_mut()
            .unwrap()
            .retained_completion_candidates[0]
            .input
            .attempt_sequence += 999;
        broken.push(changed);
        let mut changed = snapshot.clone();
        let duplicate = changed
            .custody
            .as_ref()
            .unwrap()
            .retained_completion_candidates[0]
            .clone();
        changed
            .custody
            .as_mut()
            .unwrap()
            .retained_completion_candidates
            .push(duplicate);
        broken.push(changed);
        for changed in broken {
            LocalDfsRepairQueueStore::evict_for_test(f.identity.snapshot_path());
            let bytes = encode_sobj(&changed).unwrap();
            write_file_atomic(f.identity.snapshot_path(), &bytes).unwrap();
            let error = LocalDfsRepairQueueStore::reopen_selected_original_fixture(&f.identity)
                .unwrap_err();
            assert!(error.to_string().contains("completion candidate"));
            assert_eq!(fs::read(f.identity.snapshot_path()).unwrap(), bytes);
        }
        LocalDfsRepairQueueStore::evict_for_test(f.identity.snapshot_path());
        write_file_atomic(f.identity.snapshot_path(), &good_bytes).unwrap();
        assert!(LocalDfsRepairQueueStore::reopen_selected_original_fixture(&f.identity).is_ok());
    }

    #[test]
    fn completion_candidate_namespace_failure_withholds_success_then_reuses_record() {
        struct ClearFailure;
        impl Drop for ClearFailure {
            fn drop(&mut self) {
                REPAIR_QUEUE_FAIL_SELECTED_NAMESPACE.set(false);
            }
        }
        let root = sinomo_testdir::TestDir::new("completion-candidate-sync");
        let f = fixture(root.path(), true);
        let head = f.queue.view().unwrap().head_sequence;
        let _clear = ClearFailure;
        REPAIR_QUEUE_FAIL_SELECTED_NAMESPACE.set(true);
        let error = f
            .queue
            .retain_repair_completion_candidate(f.input.clone())
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("injected selected namespace barrier failure")
        );
        // Append may already be durable. This is an uncertain local completion,
        // not a pre-append rejection or an OS-crash proof.
        assert_eq!(f.queue.view().unwrap().head_sequence, head + 1);
        let data_ref = f
            .queue
            .inner
            .lock()
            .unwrap()
            .custody
            .retained_completion_candidates
            .values()
            .next()
            .unwrap()
            .reference()
            .unwrap();
        let bytes = files(&f.identity);
        assert_eq!(
            f.queue.retain_repair_completion_candidate(f.input).unwrap(),
            data_ref
        );
        assert_eq!(f.queue.view().unwrap().head_sequence, head + 1);
        assert_eq!(files(&f.identity), bytes);
    }
}

// Runtime admission is intentionally absent. Only an explicit selected fixture
// can retain this unverified DATA until an actual owned-return caller exists.
#[cfg(any(test, feature = "test-support"))]
impl LocalDfsRepairQueueStore {
    pub(crate) fn retain_repair_completion_candidate(
        &self,
        input: RepairCompletionCandidateInput,
    ) -> Result<RepairCompletionCandidateRef, DfsError> {
        let identity = self.selected_fixture.as_ref().ok_or_else(|| {
            custody_error("completion candidate requires its selected private queue")
        })?;
        self.validate_selected_original_fixture(identity)?;
        let mut state = self.lock_state()?;
        state.with_writer_lock(|state| {
            state.refresh_from_disk()?;
            let key = state.custody.validate_completion_candidate_input(&input)?;
            if let Some(existing) = state.custody.retained_completion_candidates.get(&key) {
                if existing.input != input {
                    return Err(custody_error(
                        "completion candidate changed same-attempt material",
                    ));
                }
            } else {
                state.append_operations_held(vec![DfsRepairQueueOperation::RepairCustody {
                    change: Box::new(RepairCustodyChange::RetainCompletionCandidate {
                        input: Box::new(input),
                    }),
                }])?;
            }
            // A readable duplicate is not a renewed durable DATA acknowledgement.
            state.sync_selected_import(identity.root())?;
            state.custody.retained_completion_candidates[&key].reference()
        })
    }

    pub(crate) fn retained_repair_completion_candidate(
        &self,
        reference: &RepairCompletionCandidateRef,
    ) -> Result<RetainedRepairCompletionCandidate, DfsError> {
        let identity = self.selected_fixture.as_ref().ok_or_else(|| {
            custody_error("completion candidate requires its selected private queue")
        })?;
        self.validate_selected_original_fixture(identity)?;
        let mut state = self.lock_state()?;
        state.refresh_from_disk()?;
        if reference.queue_incarnation != state.custody.incarnation {
            return Err(custody_error(
                "completion candidate reference has another queue incarnation",
            ));
        }
        let key = RepairCompletionCandidateKey {
            subject: reference.subject.clone(),
            attempt_sequence: reference.attempt_sequence,
        };
        let candidate = state
            .custody
            .retained_completion_candidates
            .get(&key)
            .ok_or_else(|| custody_error("completion candidate reference is unavailable"))?;
        if candidate.reference()? != *reference {
            return Err(custody_error(
                "completion candidate reference changed retained DATA",
            ));
        }
        Ok(candidate.clone())
    }
}
