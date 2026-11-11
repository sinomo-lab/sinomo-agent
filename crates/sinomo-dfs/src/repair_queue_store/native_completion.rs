//! Durable original-owner custody of an actual native-owned return.
//! Replay exposes historical DATA only; it never reconstructs authentication,
//! delivery/application, a receiver Terminal, Close, or a root-release permit.
use super::*;

/// Persisted DATA schema. Its private fields are constructed at intake only
/// from the opaque actual native completion, never a fixture candidate.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeOwnedRepairCompletionData {
    capture_schema: u32,
    material: crate::DfsOriginalRepairMaterial,
    payload_root: crate::content_store::RepairPayloadRoot,
    carrier: crate::DfsRoutedMessage,
    attempt_sequence: u64,
    rpc_request_id: String,
    returned: crate::DfsRoutedMessage,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(super) struct NativeCompletionKey {
    subject: crate::content_store::RepairPayloadSubject,
    attempt_sequence: u64,
}

/// Local durable DATA reference, not a transport or semantic completion permit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NativeOwnedRepairCompletionRef {
    key: NativeCompletionKey,
    retained_at_sequence: u64,
    data_digest: String,
}

/// This success disposition is deliberately deferred before semantic handling.
#[derive(Debug)]
pub(crate) struct DeferredNativeRepairCompletion {
    reference: NativeOwnedRepairCompletionRef,
}

impl DeferredNativeRepairCompletion {
    pub(crate) fn reference(&self) -> &NativeOwnedRepairCompletionRef {
        &self.reference
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RetainedNativeOwnedRepairCompletion {
    data: NativeOwnedRepairCompletionData,
    retained_at_sequence: u64,
}

impl NativeOwnedRepairCompletionData {
    fn key(&self) -> NativeCompletionKey {
        NativeCompletionKey {
            subject: self.material.subject.clone(),
            attempt_sequence: self.attempt_sequence,
        }
    }

    fn digest(&self) -> Result<String, DfsError> {
        #[derive(Serialize)]
        struct DigestInput<'a> {
            domain: &'static str,
            data: &'a NativeOwnedRepairCompletionData,
        }
        Ok(blake3::hash(&encode_sobj(&DigestInput {
            domain: "sinomo.dfs.native-owned-original-return.v1",
            data: self,
        })?)
        .to_hex()
        .to_string())
    }
}

impl RetainedNativeOwnedRepairCompletion {
    fn reference(&self) -> Result<NativeOwnedRepairCompletionRef, DfsError> {
        Ok(NativeOwnedRepairCompletionRef {
            key: self.data.key(),
            retained_at_sequence: self.retained_at_sequence,
            data_digest: self.data.digest()?,
        })
    }

    pub(crate) fn carrier(&self) -> &crate::DfsRoutedMessage {
        &self.data.carrier
    }
    pub(crate) fn material(&self) -> &crate::DfsOriginalRepairMaterial {
        &self.data.material
    }
    pub(crate) fn payload_root(&self) -> &crate::content_store::RepairPayloadRoot {
        &self.data.payload_root
    }
    pub(crate) fn attempt_sequence(&self) -> u64 {
        self.data.attempt_sequence
    }
    pub(crate) fn rpc_request_id(&self) -> &str {
        &self.data.rpc_request_id
    }
    pub(crate) fn returned_frame(&self) -> &crate::DfsRoutedMessage {
        &self.data.returned
    }
    pub(crate) fn retained_at_sequence(&self) -> u64 {
        self.retained_at_sequence
    }
}

/// Forensic test DATA only. It cannot open a store, certify durability, admit a
/// reader/owner or reconstruct the opaque native transport capability.
#[cfg(test)]
#[derive(Debug)]
pub(crate) struct NativeCompletionProcessDataInspection {
    retained: RetainedNativeOwnedRepairCompletion,
    original_birth: RepairOriginalBirth,
    snapshot_floor: u64,
    log_head: u64,
    log_file_bytes: u64,
    log_valid_bytes: u64,
}

#[cfg(test)]
impl NativeCompletionProcessDataInspection {
    pub(crate) fn retained(&self) -> &RetainedNativeOwnedRepairCompletion {
        &self.retained
    }
    pub(crate) fn original_birth(&self) -> &RepairOriginalBirth {
        &self.original_birth
    }
    pub(crate) fn snapshot_floor(&self) -> u64 {
        self.snapshot_floor
    }
    pub(crate) fn log_head(&self) -> u64 {
        self.log_head
    }
    pub(crate) fn log_file_bytes(&self) -> u64 {
        self.log_file_bytes
    }
    pub(crate) fn log_valid_bytes(&self) -> u64 {
        self.log_valid_bytes
    }
}

#[cfg(test)]
const NATIVE_COMPLETION_PROCESS_INSPECTION_MAX_BYTES: u64 = 64 * 1024 * 1024;

#[cfg(test)]
fn same_process_inspection_directory(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    if !left.is_dir()
        || !right.is_dir()
        || left.file_type().is_symlink()
        || right.file_type().is_symlink()
    {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        (left.dev(), left.ino()) == (right.dev(), right.ino())
    }
    #[cfg(not(unix))]
    {
        left.created().ok().is_some() && left.created().ok() == right.created().ok()
    }
}

#[cfg(test)]
impl RetainedNativeOwnedRepairCompletion {
    /// Inspect exactly one expected positive DATA record without any owner or
    /// recovery path. The parent MUST pin the physical root while its genuine
    /// child is alive, kill/reap/drain it and capture raw files before this
    /// call, and establish no active writer for this private fixture/capture.
    /// The size bound applies to that stable small input; racing replacements
    /// or changes fail the before/after physical and consumed-byte checks.
    pub(crate) fn inspect_expected_process_data_for_test(
        snapshot_path: &Path,
        expected: &Self,
    ) -> Result<NativeCompletionProcessDataInspection, DfsError> {
        let invalid = |detail: &str| {
            custody_error(&format!(
                "native completion process DATA inspection {detail}"
            ))
        };
        let observation_error = |(status, detail): (&str, String)| {
            invalid(&format!("unavailable ({status}): {detail}"))
        };
        let directory = snapshot_path
            .parent()
            .ok_or_else(|| invalid("has no private parent directory"))?;
        let directory_before =
            fs::symlink_metadata(directory).map_err(|error| fingerprint_error(directory, error))?;
        if !same_process_inspection_directory(&directory_before, &directory_before) {
            return Err(invalid("requires a regular physical parent directory"));
        }
        let log_path = repair_queue_active_log_path(snapshot_path);
        let paths = [snapshot_path, log_path.as_path()];
        let before = paths
            .iter()
            .map(|path| diagnostic_queue_metadata(path).map_err(&observation_error))
            .collect::<Result<Vec<_>, _>>()?;
        let bytes = before.iter().try_fold(0_u64, |sum, metadata| {
            sum.checked_add(metadata.as_ref().map_or(0, fs::Metadata::len))
                .ok_or_else(|| invalid("file size overflow"))
        })?;
        // Pure decoding reads each file; incomplete framing can reread its
        // unacknowledged suffix. Reserve two full passes plus one exact-hash
        // pass and growth probes, without changing the existing codecs.
        if bytes
            .checked_mul(3)
            .and_then(|bytes| bytes.checked_add(4))
            .is_none_or(|cost| cost > NATIVE_COMPLETION_PROCESS_INSPECTION_MAX_BYTES)
        {
            return Err(invalid("exceeds the fixed forensic byte budget"));
        }

        let (snapshot, consumed_snapshot) =
            read_repair_queue_snapshot_with_fingerprint(snapshot_path)?;
        let snapshot_floor = snapshot.applied_through_sequence;
        let mut records = records_by_key(snapshot.records)?;
        // `true` selects only the strict private DATA grammar in temporary
        // maps. No store handle, production/default reader or native owner is
        // created or admitted, and no authentication capability is returned.
        let mut custody =
            RepairCustodyIndex::from_snapshot(snapshot.custody, snapshot_floor, true)?;
        let replay =
            replay_repair_queue_log(&log_path, snapshot_floor, &mut records, &mut custody, true)?;
        let mut remaining =
            NATIVE_COMPLETION_PROCESS_INSPECTION_MAX_BYTES - bytes.saturating_mul(2);
        let mut hash_bytes_read = 0;
        let current_snapshot = diagnostic_queue_fingerprint(
            snapshot_path,
            &before[0],
            &mut remaining,
            &mut hash_bytes_read,
        )
        .map_err(&observation_error)?;
        let current_log = diagnostic_queue_fingerprint(
            &log_path,
            &before[1],
            &mut remaining,
            &mut hash_bytes_read,
        )
        .map_err(&observation_error)?;
        if consumed_snapshot != current_snapshot || replay.fingerprint != current_log {
            return Err(invalid(
                "consumed files differ from the unchanged physical cut",
            ));
        }
        for (path, before) in paths.into_iter().zip(&before) {
            let after = diagnostic_queue_metadata(path).map_err(&observation_error)?;
            if !same_diagnostic_queue_file(before, &after) {
                return Err(invalid("queue file identity changed during decoding"));
            }
        }
        let directory_after =
            fs::symlink_metadata(directory).map_err(|error| fingerprint_error(directory, error))?;
        if !same_process_inspection_directory(&directory_before, &directory_after) {
            return Err(invalid("physical parent directory changed during decoding"));
        }

        let expected_reference = expected.reference()?;
        let retained = custody
            .retained_native_completions
            .get(&expected_reference.key)
            .filter(|retained| *retained == expected)
            .ok_or_else(|| invalid("did not prove the exact expected positive DATA record"))?;
        if retained.reference()? != expected_reference {
            return Err(invalid("expected DATA reference differs"));
        }
        let original_birth = custody
            .births
            .get(&expected_reference.key.subject)
            .ok_or_else(|| invalid("did not prove the expected DATA's complete native birth"))?;
        Ok(NativeCompletionProcessDataInspection {
            retained: retained.clone(),
            original_birth: original_birth.clone(),
            snapshot_floor,
            log_head: replay.head_sequence,
            log_file_bytes: replay.file_bytes,
            log_valid_bytes: replay.valid_bytes,
        })
    }
}

pub(super) fn validate_native_completion_frame(
    operations: &[DfsRepairQueueOperation],
) -> Result<(), DfsError> {
    if operations.iter().any(|operation| {
        matches!(operation, DfsRepairQueueOperation::RepairCustody { change }
            if matches!(change.as_ref(), RepairCustodyChange::RetainNativeOwnedCompletion { .. }))
    }) && operations.len() != 1
    {
        return Err(custody_error(
            "native completion custody requires its own DATA-only frame",
        ));
    }
    Ok(())
}

impl RepairCustodyIndex {
    pub(super) fn native_completion_pending(
        &self,
        subject: &crate::content_store::RepairPayloadSubject,
    ) -> bool {
        let first = NativeCompletionKey {
            subject: subject.clone(),
            attempt_sequence: 0,
        };
        self.retained_native_completions
            .range(first..)
            .next()
            .is_some_and(|(key, _)| &key.subject == subject)
    }

    fn validate_native_completion(
        &self,
        data: &NativeOwnedRepairCompletionData,
    ) -> Result<NativeCompletionKey, DfsError> {
        data.material.validate()?;
        let key = data.key();
        let birth = self.births.get(&key.subject).ok_or_else(|| {
            custody_error("native completion custody has no exact native original birth")
        })?;
        birth.validate(&self.incarnation)?;
        if data.capture_schema != 1
            || key.subject.queue_incarnation != self.incarnation
            || birth.original_wire_material()? != data.material
            || birth.root != data.payload_root
            || !data.material.matches_carrier(&data.carrier)
            || data.rpc_request_id != format!("dfs_message_{}", data.carrier.frame_id)
        {
            return Err(custody_error(
                "native completion custody changed its original, physical body or RPC identity",
            ));
        }
        let mut attempts = birth.attempts.iter().filter(|attempt| {
            attempt.frame_id == data.carrier.frame_id
                && attempt.message_id == data.carrier.message_id
        });
        let attempt = attempts.next().ok_or_else(|| {
            custody_error("native completion custody has no actual persisted attempt")
        })?;
        if attempts.next().is_some()
            || attempt.sequence == 0
            || attempt.sequence != data.attempt_sequence
        {
            return Err(custody_error(
                "native completion custody has a changed or ambiguous persisted attempt",
            ));
        }
        let crate::DfsNetworkMessage::RepairReplicaResponse(response) = &data.returned.message
        else {
            return Err(custody_error(
                "native completion custody has another response family",
            ));
        };
        if data.returned.source_node_id != birth.original.target_node_id
            || data.returned.target_node_id != birth.original.source_node_id
        {
            return Err(custody_error(
                "native completion custody changed its returned route",
            ));
        }
        birth.validate_response(response)?;
        Ok(key)
    }

    pub(super) fn validate_new_native_completion(
        &self,
        data: &NativeOwnedRepairCompletionData,
    ) -> Result<NativeCompletionKey, DfsError> {
        let key = self.validate_native_completion(data)?;
        if let Some(existing) = self.retained_native_completions.get(&key) {
            if existing.data != *data {
                return Err(custody_error(
                    "native completion custody changed same-attempt returned material",
                ));
            }
        } else if self.births[&key.subject].closed_at_sequence.is_some() {
            return Err(custody_error(
                "new native completion custody cannot acknowledge an already closed birth",
            ));
        }
        Ok(key)
    }

    pub(super) fn restore_native_completion(
        &mut self,
        completion: RetainedNativeOwnedRepairCompletion,
        floor: u64,
    ) -> Result<(), DfsError> {
        let key = self.validate_native_completion(&completion.data)?;
        let sequence = completion.retained_at_sequence;
        if sequence <= key.attempt_sequence
            || sequence > floor
            || self.retained_native_completions.contains_key(&key)
            || self.history_sequences.contains_key(&sequence)
            || self
                .retained_completion_candidates
                .values()
                .any(|candidate| candidate.retained_sequence() == sequence)
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
                "native completion custody has a conflicting snapshot prefix",
            ));
        }
        self.history_sequences.insert(sequence, None);
        self.retained_native_completions.insert(key, completion);
        Ok(())
    }

    pub(super) fn apply_native_completion(
        &mut self,
        data: &NativeOwnedRepairCompletionData,
        sequence: u64,
    ) -> Result<(), DfsError> {
        self.restore_native_completion(
            RetainedNativeOwnedRepairCompletion {
                data: data.clone(),
                retained_at_sequence: sequence,
            },
            sequence,
        )
    }
}

#[cfg(any(test, feature = "test-support"))]
impl LocalDfsRepairQueueStore {
    /// A scoped retry hold for an already retained native-return fact. This is
    /// not a receipt, current-holder/Terminal fact, or a legacy-origin refusal.
    pub(crate) fn native_original_completion_pending(
        &self,
        carrier: &crate::DfsRoutedMessage,
    ) -> Result<bool, DfsError> {
        let mut state = self.lock_state()?;
        state.refresh_from_disk()?;
        let subject = repair_subject(&state.custody.incarnation, carrier)?;
        Ok(state.custody.native_completion_pending(&subject))
    }

    /// Retain only the opaque actual native return, before any serving-effect
    /// handler. The caller keeps the capability on error. Physical validation
    /// runs outside queue locks, including after the durable namespace barrier.
    pub(crate) fn retain_native_owned_repair_completion(
        &self,
        completion: &crate::runtime_host::OwnedOriginalRepairCompletion,
    ) -> Result<DeferredNativeRepairCompletion, DfsError> {
        completion.revalidate_custody_owner(self)?;
        let identity = self.selected_fixture.as_ref().ok_or_else(|| {
            custody_error("native completion custody requires its selected physical queue")
        })?;
        let data = NativeOwnedRepairCompletionData {
            capture_schema: 1,
            material: completion.material().clone(),
            payload_root: completion.payload_root().clone(),
            carrier: completion.carrier().clone(),
            attempt_sequence: completion.attempt_sequence(),
            rpc_request_id: completion.rpc_request_id().to_owned(),
            returned: completion.returned_frame().clone(),
        };
        let reference = {
            let mut state = self.lock_state()?;
            state.with_writer_lock(|state| {
                state.refresh_from_disk()?;
                let key = state.custody.validate_new_native_completion(&data)?;
                if !state.custody.retained_native_completions.contains_key(&key) {
                    state.append_operations_held(vec![DfsRepairQueueOperation::RepairCustody {
                        change: Box::new(RepairCustodyChange::RetainNativeOwnedCompletion {
                            data: Box::new(data),
                        }),
                    }])?;
                }
                // A readable duplicate is DATA, not a renewed custody ACK.
                // Reuse the existing writer's exact files+namespace barrier.
                state.sync_selected_import(identity.root())?;
                state.custody.retained_native_completions[&key].reference()
            })?
        };
        completion.revalidate_custody_owner(self)?;
        Ok(DeferredNativeRepairCompletion { reference })
    }

    /// Historical DATA only. This cannot reconstruct an owned native return or
    /// authorize delivery, receipt publication, local application or release.
    pub(crate) fn retained_native_owned_repair_completion(
        &self,
        reference: &NativeOwnedRepairCompletionRef,
    ) -> Result<RetainedNativeOwnedRepairCompletion, DfsError> {
        let identity = self.selected_fixture.as_ref().ok_or_else(|| {
            custody_error("native completion custody requires its selected physical queue")
        })?;
        self.validate_selected_original_fixture(identity)?;
        let mut state = self.lock_state()?;
        state.refresh_from_disk()?;
        let retained = state
            .custody
            .retained_native_completions
            .get(&reference.key)
            .ok_or_else(|| custody_error("native completion custody reference is unavailable"))?;
        if retained.reference()? != *reference {
            return Err(custody_error("native completion custody reference changed"));
        }
        Ok(retained.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // These fixtures exercise the persisted codec/structural validators only.
    // The supplied response is synthetic DATA. They never construct an opaque
    // OwnedOriginalRepairCompletion, call native intake, or prove transport
    // authentication. Actual opaque-capability intake is a separate native test.
    struct CodecFixture {
        queue: LocalDfsRepairQueueStore,
        identity: SelectedOriginalFixtureIdentity,
        _content: crate::LocalContentStore,
        data: NativeOwnedRepairCompletionData,
    }

    fn fixture(root: &Path) -> CodecFixture {
        let (queue, content, original, attachment, payload, identity) =
            super::super::tests::selected_original_fixture(&root.join("queue"));
        queue
            .enroll_original_repair(&original, &attachment, &payload)
            .unwrap();
        let mut carrier = original;
        carrier.frame_id += 100;
        carrier.message_id += 100;
        queue.record_original_repair_attempt(&carrier).unwrap();
        let birth = queue.original_repair(&carrier).unwrap().unwrap();
        let crate::DfsNetworkMessage::RepairReplica(command) = &carrier.message else {
            panic!("actual file-backed Repair birth");
        };
        let data = NativeOwnedRepairCompletionData {
            capture_schema: 1,
            material: birth.original_wire_material().unwrap(),
            payload_root: birth.root,
            attempt_sequence: birth.attempts[0].sequence,
            rpc_request_id: format!("dfs_message_{}", carrier.frame_id),
            returned: crate::DfsRoutedMessage {
                source_node_id: carrier.target_node_id.clone(),
                target_node_id: carrier.source_node_id.clone(),
                frame_id: 901,
                message_id: 902,
                message: crate::DfsNetworkMessage::RepairReplicaResponse(
                    crate::DfsRepairReplicaResponse {
                        request_id: command.request_id.clone(),
                        intent_id: command.intent_id.clone(),
                        file_system_id: command.file_system_id.clone(),
                        object_id: command.object_id.clone(),
                        version: command.version.clone(),
                        replica_node_id: carrier.target_node_id.clone(),
                        result: crate::DfsBlobTransferResult::Committed {
                            content_hash: command.content_hash.clone(),
                        },
                        receipt_generation: command.receipt_generation,
                    },
                ),
            },
            carrier,
        };
        CodecFixture {
            queue,
            identity,
            _content: content,
            data,
        }
    }

    fn non_effect_state(queue: &LocalDfsRepairQueueStore) -> String {
        let state = queue.inner.lock().unwrap();
        format!(
            "{:?}",
            (
                (
                    &state.records,
                    &state.custody.births,
                    &state.custody.slots,
                    &state.custody.closed_requests,
                    &state.custody.observed_pending
                ),
                (
                    &state.custody.imported_original_data,
                    &state.custody.imported_executor_associations,
                    &state.custody.imported_history_queries,
                    &state.custody.associated_slots,
                    &state.custody.retained_completion_candidates
                ),
                (
                    state.revision,
                    &state.maintenance_cursor,
                    state.maintenance_restart_pending,
                    state.maintenance_revisit_pending,
                    &state.custody_recovery_cursor
                ),
            )
        )
    }

    fn rejects<T>(result: Result<T, DfsError>, expected: &str) {
        match result {
            Err(error) => assert!(error.to_string().contains(expected), "{error}"),
            Ok(_) => panic!("expected {expected}"),
        }
    }

    // Test-only codec setup through the actual existing writer. This does not
    // call the opaque-capability intake or issue its durable custody disposition.
    fn append_codec_fixture(f: &CodecFixture) -> NativeOwnedRepairCompletionRef {
        let mut state = f.queue.lock_state().unwrap();
        state
            .with_writer_lock(|state| {
                state.append_operations_held(vec![DfsRepairQueueOperation::RepairCustody {
                    change: Box::new(RepairCustodyChange::RetainNativeOwnedCompletion {
                        data: Box::new(f.data.clone()),
                    }),
                }])?;
                state.custody.retained_native_completions[&f.data.key()].reference()
            })
            .unwrap()
    }

    #[test]
    fn native_completion_codec_checks_exact_birth_attempt_and_return_without_mutation() {
        let root = sinomo_testdir::TestDir::new("native-completion-codec-correlation");
        let f = fixture(root.path());
        let before = super::super::tests::diagnostic_fixture_state(&f.queue);
        let mut variants = Vec::new();
        let mut changed = f.data.clone();
        changed.capture_schema = 0;
        variants.push(changed);
        let mut changed = f.data.clone();
        changed.rpc_request_id.push('x');
        variants.push(changed);
        let mut changed = f.data.clone();
        changed.payload_root.store_incarnation.push('x');
        variants.push(changed);
        let mut changed = f.data.clone();
        changed.attempt_sequence += 1;
        variants.push(changed);
        let mut changed = f.data.clone();
        changed.returned.source_node_id = changed.carrier.source_node_id.clone();
        variants.push(changed);
        let mut changed = f.data.clone();
        if let crate::DfsNetworkMessage::RepairReplicaResponse(response) =
            &mut changed.returned.message
        {
            response.request_id.push('x');
        }
        variants.push(changed);
        {
            let state = f.queue.lock_state().unwrap();
            assert_eq!(
                state
                    .custody
                    .validate_new_native_completion(&f.data)
                    .unwrap(),
                f.data.key()
            );
            for changed in variants {
                assert!(
                    state
                        .custody
                        .validate_new_native_completion(&changed)
                        .is_err()
                );
            }
        }
        assert_eq!(
            super::super::tests::diagnostic_fixture_state(&f.queue),
            before
        );
    }

    #[test]
    fn native_completion_codec_preserves_data_through_actual_log_compaction_and_cold_replay() {
        let root = sinomo_testdir::TestDir::new("native-completion-codec-replay");
        let mut f = fixture(root.path());
        let before = non_effect_state(&f.queue);
        let reference = append_codec_fixture(&f);
        let retained = f
            .queue
            .retained_native_owned_repair_completion(&reference)
            .unwrap();
        assert_eq!(retained.data, f.data);
        assert_eq!(non_effect_state(&f.queue), before);
        LocalDfsRepairQueueStore::evict_for_test(f.identity.snapshot_path());
        f.queue = LocalDfsRepairQueueStore::reopen_selected_original_fixture(&f.identity).unwrap();
        assert_eq!(
            f.queue
                .retained_native_owned_repair_completion(&reference)
                .unwrap(),
            retained
        );
        assert!(
            f.queue
                .original_repair(&f.data.carrier)
                .unwrap()
                .unwrap()
                .observed
                .is_empty()
        );
        f.queue
            .compact_if_needed(DfsRepairQueueCompactionOptions::force())
            .unwrap();
        LocalDfsRepairQueueStore::evict_for_test(f.identity.snapshot_path());
        f.queue = LocalDfsRepairQueueStore::reopen_selected_original_fixture(&f.identity).unwrap();
        assert_eq!(
            f.queue
                .retained_native_owned_repair_completion(&reference)
                .unwrap(),
            retained
        );
        let birth = f.queue.original_repair(&f.data.carrier).unwrap().unwrap();
        assert!(birth.closed_at_sequence.is_none() && birth.observed.is_empty());
        assert!(!birth.attempts[0].delivery_observed);
        assert!(
            f.queue
                .lock_state()
                .unwrap()
                .custody
                .retained_completion_candidates
                .is_empty()
        );
        assert!(
            f.queue
                .native_original_completion_pending(&f.data.carrier)
                .unwrap()
        );
        let before = super::super::tests::diagnostic_fixture_state(&f.queue);
        let mut retry = f.data.carrier.clone();
        retry.frame_id += 1;
        retry.message_id += 1;
        rejects(
            f.queue.record_original_repair_attempt(&retry),
            "awaits native completion custody settlement",
        );
        rejects(
            f.queue.validate_original_repair_dispatch(&f.data.carrier),
            "awaits native completion custody settlement",
        );
        let crate::DfsNetworkMessage::RepairReplicaResponse(response) = &f.data.returned.message
        else {
            panic!("structural Repair response fixture");
        };
        rejects(
            f.queue
                .observe_original_repair_result(&f.data.carrier.source_node_id, response),
            "awaits native completion custody settlement",
        );
        // The shared writer/replay validator also refuses a raw ordinary
        // Observe transition before it can change any map or frame.
        let change = RepairCustodyChange::Observe {
            subject: f.data.material.subject.clone(),
            response: response.clone(),
        };
        {
            let mut state = f.queue.lock_state().unwrap();
            rejects(
                state.append_operations(vec![DfsRepairQueueOperation::RepairCustody {
                    change: Box::new(change.clone()),
                }]),
                "awaits native completion custody settlement",
            );
            let mut replay_index = state.custody.clone();
            let before_replay = format!("{replay_index:?}");
            rejects(
                replay_index.apply_change(&change, state.head_sequence + 1),
                "awaits native completion custody settlement",
            );
            assert_eq!(format!("{replay_index:?}"), before_replay);
        }
        assert_eq!(
            super::super::tests::diagnostic_fixture_state(&f.queue),
            before
        );
        let mut unrelated = f.data.carrier.clone();
        unrelated.source_node_id = NodeId::from_legacy_named("unrelated-native-origin").unwrap();
        assert!(
            !f.queue
                .native_original_completion_pending(&unrelated)
                .unwrap()
        );
        // The same legacy writer contract remains available without a native
        // fact. This synthetic local response is not native auth evidence.
        let other_root = sinomo_testdir::TestDir::new("native-completion-observe-unheld");
        let other = fixture(other_root.path());
        let crate::DfsNetworkMessage::RepairReplicaResponse(other_response) =
            &other.data.returned.message
        else {
            panic!("unheld Repair response fixture");
        };
        assert!(
            other
                .queue
                .observe_original_repair_result(&other.data.carrier.source_node_id, other_response)
                .unwrap()
                .is_some()
        );
        assert_eq!(
            other
                .queue
                .original_repair(&other.data.carrier)
                .unwrap()
                .unwrap()
                .observed,
            vec![other_response.clone()]
        );
    }

    #[test]
    fn native_completion_codec_same_attempt_conflict_and_new_after_close_reject() {
        let root = sinomo_testdir::TestDir::new("native-completion-codec-conflict");
        let f = fixture(root.path());
        append_codec_fixture(&f);
        let before = super::super::tests::diagnostic_fixture_state(&f.queue);
        let mut changed = f.data.clone();
        changed.returned.frame_id += 1;
        {
            let state = f.queue.lock_state().unwrap();
            assert!(
                state
                    .custody
                    .validate_new_native_completion(&f.data)
                    .is_ok()
            );
            rejects(
                state.custody.validate_new_native_completion(&changed),
                "changed same-attempt returned material",
            );
        }
        assert_eq!(
            super::super::tests::diagnostic_fixture_state(&f.queue),
            before
        );

        // A structural clone demonstrates replay of historical DATA is distinct
        // from admitting a NEW record after durable closure. No close capability
        // or native capture is manufactured by this codec-only branch control.
        let mut index = f.queue.lock_state().unwrap().custody.clone();
        index.retained_native_completions.clear();
        let birth = index.births.get_mut(&f.data.material.subject).unwrap();
        let crate::DfsNetworkMessage::RepairReplicaResponse(response) = &f.data.returned.message
        else {
            unreachable!()
        };
        birth.observed.push(response.clone());
        birth.closed_at_sequence = Some(f.data.attempt_sequence + 2);
        rejects(
            index.validate_new_native_completion(&f.data),
            "already closed birth",
        );
    }

    #[test]
    fn native_completion_codec_default_and_mixed_frame_snapshot_prefix_reject() {
        let root = sinomo_testdir::TestDir::new("native-completion-codec-admission");
        let f = fixture(root.path());
        let reference = append_codec_fixture(&f);
        let log = repair_queue_active_log_path(f.identity.snapshot_path());
        let actual_log = fs::read(&log).unwrap();
        let ordinary_path = root.join("ordinary.sobj");
        let ordinary_log = repair_queue_active_log_path(&ordinary_path);
        write_file_atomic(&ordinary_log, &actual_log).unwrap();
        let ordinary = LocalDfsRepairQueueStore::new(&ordinary_path);
        rejects(
            ordinary.view(),
            "default queue replay rejects selected original DATA",
        );
        assert_eq!(fs::read(&ordinary_log).unwrap(), actual_log);
        assert!(!ordinary_path.exists());

        f.queue
            .compact_if_needed(DfsRepairQueueCompactionOptions::force())
            .unwrap();
        let snapshot = read_repair_queue_snapshot(f.identity.snapshot_path()).unwrap();
        let bytes = fs::read(f.identity.snapshot_path()).unwrap();
        let snapshot_path = root.join("ordinary-snapshot.sobj");
        write_file_atomic(&snapshot_path, &bytes).unwrap();
        let ordinary = LocalDfsRepairQueueStore::new(&snapshot_path);
        rejects(
            ordinary.view(),
            "default queue reader rejects selected original DATA",
        );
        assert_eq!(fs::read(&snapshot_path).unwrap(), bytes);
        let custody = snapshot.custody.clone().unwrap();
        assert!(custody.retained_completion_candidates.is_empty());
        for fault in [
            "attempt-sequence",
            "future-prefix",
            "duplicate",
            "changed-rpc",
        ] {
            let mut changed = custody.clone();
            match fault {
                "attempt-sequence" => {
                    changed.retained_native_completions[0].retained_at_sequence =
                        f.data.attempt_sequence
                }
                "future-prefix" => {
                    changed.retained_native_completions[0].retained_at_sequence =
                        snapshot.applied_through_sequence + 1
                }
                "duplicate" => changed
                    .retained_native_completions
                    .push(changed.retained_native_completions[0].clone()),
                "changed-rpc" => changed.retained_native_completions[0]
                    .data
                    .rpc_request_id
                    .push('x'),
                _ => unreachable!(),
            }
            assert!(
                RepairCustodyIndex::from_snapshot(
                    Some(changed),
                    snapshot.applied_through_sequence,
                    true
                )
                .is_err(),
                "{fault}"
            );
        }

        // A separate actual R35 candidate remains explicitly unverified. Its
        // retained frame and the native DATA frame must not share a prefix
        // sequence in either direction; neither family upgrades the other.
        f.queue
            .retain_repair_completion_candidate(RepairCompletionCandidateInput {
                queue_incarnation: f.data.material.subject.queue_incarnation.clone(),
                original_command_digest: f.data.material.command_digest.clone(),
                attempt_sequence: f.data.attempt_sequence,
                attempted_carrier: f.data.carrier.clone(),
                returned_frame: f.data.returned.clone(),
                provenance: RepairCompletionCandidateProvenance::FixtureUnverifiedTransportMaterial,
            })
            .unwrap();
        f.queue
            .compact_if_needed(DfsRepairQueueCompactionOptions::force())
            .unwrap();
        let both = read_repair_queue_snapshot(f.identity.snapshot_path()).unwrap();
        let both_custody = both.custody.unwrap();
        assert_eq!(
            both_custody.retained_completion_candidates[0]
                .input
                .provenance,
            RepairCompletionCandidateProvenance::FixtureUnverifiedTransportMaterial
        );
        let candidate_sequence = both_custody.retained_completion_candidates[0].retained_sequence();
        let native_sequence = both_custody.retained_native_completions[0].retained_at_sequence;
        let mut changed = both_custody.clone();
        changed.retained_native_completions[0].retained_at_sequence = candidate_sequence;
        assert!(
            RepairCustodyIndex::from_snapshot(Some(changed), both.applied_through_sequence, true)
                .is_err()
        );
        // The candidate's sequence is private to its record. Mutate the actual
        // named codec representation rather than exposing a raw intake API.
        let mut candidate_bytes: serde_json::Value = serde_json::to_value(&both_custody).unwrap();
        candidate_bytes["retained_completion_candidates"][0]["retained_at_sequence"] =
            native_sequence.into();
        let changed: RepairCustodySnapshot = serde_json::from_value(candidate_bytes).unwrap();
        assert!(
            RepairCustodyIndex::from_snapshot(Some(changed), both.applied_through_sequence, true)
                .is_err()
        );

        // A complete, correctly framed two-operation native record must fail
        // before publishing any replayed resident maps or mutating these files.
        let mut reader = fs::File::open(&ordinary_log).unwrap();
        let mut last = None;
        while let Some(frame) = durable_file::read_length_prefixed_record(
            &mut reader,
            &ordinary_log,
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
        let bad = encode_sobj(&batch).unwrap();
        let mut mixed = actual_log[..last.record_start as usize].to_vec();
        mixed.extend_from_slice(&(bad.len() as u32).to_le_bytes());
        mixed.extend_from_slice(&bad);
        let mixed_path = root.join("mixed.slog");
        write_file_atomic(&mixed_path, &mixed).unwrap();
        let mut records = BTreeMap::new();
        let mut index = RepairCustodyIndex::default();
        rejects(
            replay_repair_queue_log(&mixed_path, 0, &mut records, &mut index, true),
            "native completion custody requires its own DATA-only frame",
        );
        assert!(index.retained_native_completions.is_empty());
        assert!(
            index
                .births
                .values()
                .all(|birth| birth.observed.is_empty() && birth.closed_at_sequence.is_none())
        );
        assert_eq!(fs::read(&mixed_path).unwrap(), mixed);
        assert_eq!(
            f.queue
                .retained_native_owned_repair_completion(&reference)
                .unwrap()
                .data,
            f.data
        );
    }

    #[test]
    fn native_completion_forensic_expected_data_inspection_never_recovers_or_mutates() {
        use std::io::Write as _;
        let root = sinomo_testdir::TestDir::new("native-completion-forensic-data");
        let f = fixture(root.path());
        let reference = append_codec_fixture(&f);
        f.queue
            .compact_if_needed(DfsRepairQueueCompactionOptions::force())
            .unwrap();
        let expected = f
            .queue
            .retained_native_owned_repair_completion(&reference)
            .unwrap();
        let expected_birth = f.queue.original_repair(&f.data.carrier).unwrap().unwrap();
        let before = super::super::tests::diagnostic_fixture_state(&f.queue);
        let actual = RetainedNativeOwnedRepairCompletion::inspect_expected_process_data_for_test(
            f.identity.snapshot_path(),
            &expected,
        )
        .unwrap();
        assert_eq!(actual.retained(), &expected);
        assert_eq!(actual.original_birth(), &expected_birth);
        assert!(actual.snapshot_floor() >= expected.retained_at_sequence());
        assert_eq!(actual.log_head(), actual.snapshot_floor());
        assert_eq!((actual.log_file_bytes(), actual.log_valid_bytes()), (0, 0));
        assert_eq!(
            super::super::tests::diagnostic_fixture_state(&f.queue),
            before
        );

        // A real incomplete suffix is reported, never recovered/truncated.
        // This local codec fault is not the separate parent-SIGKILL evidence.
        let log = repair_queue_active_log_path(f.identity.snapshot_path());
        let mut file = fs::OpenOptions::new().append(true).open(&log).unwrap();
        file.write_all(&[5, 0]).unwrap();
        file.sync_all().unwrap();
        drop(file);
        let before = super::super::tests::diagnostic_fixture_state(&f.queue);
        let actual = RetainedNativeOwnedRepairCompletion::inspect_expected_process_data_for_test(
            f.identity.snapshot_path(),
            &expected,
        )
        .unwrap();
        assert_eq!(actual.retained(), &expected);
        assert_eq!(actual.original_birth(), &expected_birth);
        assert_eq!((actual.log_file_bytes(), actual.log_valid_bytes()), (2, 0));
        let mut wrong_expected = expected.clone();
        wrong_expected.data.returned.frame_id += 1;
        rejects(
            RetainedNativeOwnedRepairCompletion::inspect_expected_process_data_for_test(
                f.identity.snapshot_path(),
                &wrong_expected,
            ),
            "did not prove the exact expected positive DATA record",
        );
        assert_eq!(
            super::super::tests::diagnostic_fixture_state(&f.queue),
            before
        );

        // Sparse length exceeds the bound before any decode or full-file read.
        let file = fs::OpenOptions::new().write(true).open(&log).unwrap();
        file.set_len(NATIVE_COMPLETION_PROCESS_INSPECTION_MAX_BYTES)
            .unwrap();
        drop(file);
        let before = fs::symlink_metadata(&log).unwrap();
        rejects(
            RetainedNativeOwnedRepairCompletion::inspect_expected_process_data_for_test(
                f.identity.snapshot_path(),
                &expected,
            ),
            "fixed forensic byte budget",
        );
        assert!(same_file_observation(
            &before,
            &fs::symlink_metadata(&log).unwrap()
        ));

        // A complete malformed frame remains unchanged; the helper never
        // invokes the writer's resync/truncation or any resident reload path.
        let corrupt = [1_u8, 0, 0, 0, 0xff];
        write_file_atomic(&log, &corrupt).unwrap();
        let before = super::super::tests::diagnostic_fixture_state(&f.queue);
        rejects(
            RetainedNativeOwnedRepairCompletion::inspect_expected_process_data_for_test(
                f.identity.snapshot_path(),
                &expected,
            ),
            "could not decode DFS repair queue log",
        );
        assert_eq!(
            super::super::tests::diagnostic_fixture_state(&f.queue),
            before
        );

        #[cfg(unix)]
        {
            let retained_log = root.join("retained-log");
            fs::rename(&log, &retained_log).unwrap();
            std::os::unix::fs::symlink(&retained_log, &log).unwrap();
            let before = super::super::tests::diagnostic_fixture_state(&f.queue);
            rejects(
                RetainedNativeOwnedRepairCompletion::inspect_expected_process_data_for_test(
                    f.identity.snapshot_path(),
                    &expected,
                ),
                "NonRegular",
            );
            assert_eq!(
                super::super::tests::diagnostic_fixture_state(&f.queue),
                before
            );
        }
    }
}
