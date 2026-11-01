//! Read-only binding to one actual persisted native original/attempt.
//!
//! This is queue DATA, not authentication, renewed durability, delivery,
//! execution, application, or a content/root-release capability.
use super::*;

/// Only the queue owner can construct this witness. It is deliberately neither
/// serialized nor reconstructed from a completion candidate or a caller's
/// attempt counter. The native transport must independently establish its
/// actual DATA/content owner and authenticated one-call return.
#[derive(Debug)]
pub(crate) struct RepairOriginalAttemptWitness {
    owner: Arc<()>,
    physical: RepairQueuePhysicalOwner,
    carrier: crate::DfsRoutedMessage,
    material: crate::DfsOriginalRepairMaterial,
    payload_root: crate::content_store::RepairPayloadRoot,
    attempt_sequence: u64,
}

/// Existing physical witness: selected DATA and actual ordinary native owners
/// remain distinct. Neither variant is serialized or a serving permission.
#[derive(Clone, Debug)]
pub(super) enum RepairQueuePhysicalOwner {
    Selected(SelectedOriginalFixtureIdentity),
    Default {
        owner: std::sync::Weak<crate::runtime_host::DfsDataOwnerLock>,
        snapshot_path: PathBuf,
        lock_identity: (u64, u64),
    },
}
impl RepairQueuePhysicalOwner {
    pub(super) fn for_default(
        owner: &Arc<crate::runtime_host::DfsDataOwnerLock>,
    ) -> Result<Self, DfsError> {
        let snapshot_path = owner
            .checked_default_repair_queue_path()
            .map_err(|error| custody_error(&error.to_string()))?;
        let lock_identity = owner
            .storage_owner_file_identity()
            .map_err(|error| custody_error(&error.to_string()))?;
        Ok(Self::Default {
            owner: Arc::downgrade(owner),
            snapshot_path,
            lock_identity,
        })
    }
    pub(super) fn check(&self) -> Result<(), DfsError> {
        match self {
            Self::Selected(identity) => identity.check(),
            Self::Default {
                owner,
                snapshot_path,
                lock_identity,
            } => {
                let actual = owner
                    .upgrade()
                    .ok_or_else(|| custody_error("ordinary Repair witness native owner retired"))?;
                if actual
                    .checked_default_repair_queue_path()
                    .map_err(|error| custody_error(&error.to_string()))?
                    != *snapshot_path
                    || actual
                        .storage_owner_file_identity()
                        .map_err(|error| custody_error(&error.to_string()))?
                        != *lock_identity
                {
                    return Err(custody_error(
                        "ordinary Repair witness changed registered/native physical owner",
                    ));
                }
                Ok(())
            }
        }
    }
    pub(super) fn matches_state(&self, state: &DfsRepairQueueState) -> bool {
        match self {
            Self::Selected(expected) => state.selected_fixture.as_ref().is_some_and(|actual| {
                actual.root == expected.root
                    && actual.snapshot_path == expected.snapshot_path
                    && actual.marker == expected.marker
                    && state.snapshot_path == expected.snapshot_path
            }),
            Self::Default { snapshot_path, .. } => {
                state.selected_fixture.is_none()
                    && state.snapshot_path == *snapshot_path
                    && !state.custody.incarnation.is_empty()
            }
        }
    }
}

impl RepairOriginalAttemptWitness {
    pub(crate) fn carrier(&self) -> &crate::DfsRoutedMessage {
        &self.carrier
    }

    pub(crate) fn material(&self) -> &crate::DfsOriginalRepairMaterial {
        &self.material
    }

    pub(crate) fn payload_root(&self) -> &crate::content_store::RepairPayloadRoot {
        &self.payload_root
    }

    pub(crate) fn attempt_sequence(&self) -> u64 {
        self.attempt_sequence
    }

    pub(crate) fn queue_incarnation(&self) -> &str {
        &self.material.subject.queue_incarnation
    }
}

/// Do not refresh, replay, truncate, synchronize or change resident state here.
/// The producer's existing writer/replay must already have supplied a certain
/// prefix. Directory identity and exact current files are checked again on use.
pub(super) fn checked_loaded_prefix(
    state: &DfsRepairQueueState) -> Result<(), DfsError> {
    if !state.loaded || state.selected_fixture_initializing {
        return Err(custody_error(
            "original attempt witness requires an already loaded resident prefix",
        ));
    }
    if state.resync_on_load || state.writer_held {
        return Err(custody_error(
            "original attempt witness cannot use an uncertain or active writer prefix",
        ));
    }
    if state.active_log_bytes != state.valid_active_log_bytes {
        return Err(custody_error(
            "original attempt witness cannot certify an incomplete log suffix",
        ));
    }
    let expected = state.fingerprint.as_ref().ok_or_else(|| {
        custody_error("original attempt witness has no resident file fingerprint")
    })?;
    let paths = [&state.snapshot_path, &state.active_log_path];
    let before = paths
        .iter()
        .map(|path| checked_queue_file(path))
        .collect::<Result<Vec<_>, _>>()?;
    let actual = state.disk_fingerprint()?;
    for (path, before) in paths.into_iter().zip(before) {
        let after = checked_queue_file(path)?;
        let unchanged = match (before, after) {
            (None, None) => true,
            (Some(before), Some(after)) => same_file_observation(&before, &after),
            _ => false,
        };
        if !unchanged {
            return Err(custody_error(
                "original attempt witness queue files changed during observation",
            ));
        }
    }
    if &actual != expected {
        return Err(custody_error(
            "original attempt witness queue files differ from the resident prefix",
        ));
    }
    Ok(())
}
fn checked_resident_prefix(
    state: &DfsRepairQueueState,
) -> Result<RepairQueuePhysicalOwner, DfsError> {
    let identity = state.selected_fixture.as_ref().ok_or_else(|| {
        custody_error("original attempt witness requires its explicit physical queue owner")
    })?;
    identity.check()?;
    checked_loaded_prefix(state)?;
    identity.check()?;
    if state.custody.incarnation != identity.marker.queue_incarnation {
        return Err(custody_error(
            "original attempt witness queue incarnation differs from its physical owner",
        ));
    }
    Ok(RepairQueuePhysicalOwner::Selected(identity.clone()))
}

fn checked_queue_file(path: &Path) -> Result<Option<fs::Metadata>, DfsError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
            Ok(Some(metadata))
        }
        Ok(_) => Err(custody_error(
            "original attempt witness requires regular non-symlink queue files",
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(fingerprint_error(path, error)),
    }
}

fn checked_attempt<'a>(
    state: &'a DfsRepairQueueState,
    carrier: &crate::DfsRoutedMessage,
    material: &crate::DfsOriginalRepairMaterial,
) -> Result<(&'a RepairOriginalBirth, u64), DfsError> {
    material.validate()?;
    if !material.matches_carrier(carrier) {
        return Err(custody_error(
            "original attempt witness carrier differs from immutable original material",
        ));
    }
    let subject = repair_subject(&state.custody.incarnation, carrier)?;
    let birth = state.custody.births.get(&subject).ok_or_else(|| {
        custody_error("original attempt witness has no exact persisted native birth")
    })?;
    birth.validate(&state.custody.incarnation)?;
    if birth.original_wire_material()? != *material {
        return Err(custody_error(
            "original attempt witness material differs from the persisted native birth",
        ));
    }
    let mut matching = birth.attempts.iter().filter(|attempt| {
        attempt.frame_id == carrier.frame_id && attempt.message_id == carrier.message_id
    });
    let attempt = matching.next().ok_or_else(|| {
        custody_error("original attempt witness has no matching persisted carrier attempt")
    })?;
    if matching.next().is_some() {
        return Err(custody_error(
            "original attempt witness has ambiguous persisted carrier attempts",
        ));
    }
    if attempt.sequence == 0 || attempt.sequence > state.head_sequence {
        return Err(custody_error(
            "original attempt witness sequence is outside the resident durable prefix",
        ));
    }
    Ok((birth, attempt.sequence))
}

impl LocalDfsRepairQueueStore {
    /// Identity only, before a custody writer may recover/recertify its SAME
    /// physical queue. This does not validate a prefix, renew the witness,
    /// reconstruct transport provenance, load, synchronize or change state.
    pub(crate) fn check_original_repair_attempt_physical_owner(
        &self,
        witness: &RepairOriginalAttemptWitness,
    ) -> Result<(), DfsError> {
        witness.physical.check()?;
        let state = self.lock_state()?;
        if !Arc::ptr_eq(&state.owner, &witness.owner) || !witness.physical.matches_state(&state) {
            return Err(custody_error(
                "original attempt witness belongs to another native/resident physical owner",
            ));
        }
        Ok(())
    }

    /// Capture an exact historical queue Attempt without changing any state.
    /// The native caller must supply this queue from its actual retained DATA
    /// owner; selected representation alone supplies no authority.
    pub(crate) fn capture_original_repair_attempt_witness(
        &self,
        carrier: &crate::DfsRoutedMessage,
        material: &crate::DfsOriginalRepairMaterial,
    ) -> Result<RepairOriginalAttemptWitness, DfsError> {
        let state = self.lock_state()?;
        let physical = checked_resident_prefix(&state)?;
        let (birth, attempt_sequence) = checked_attempt(&state, carrier, material)?;
        Ok(RepairOriginalAttemptWitness {
            owner: Arc::clone(&state.owner),
            physical,
            carrier: carrier.clone(),
            material: material.clone(),
            payload_root: birth.root.clone(),
            attempt_sequence,
        })
    }

    /// Default production capture is available only with the normal producer's
    /// current native admission and the SAME registered physical DATA owner.
    pub(crate) fn capture_default_original_repair_attempt_witness(
        &self,
        owner: &Arc<crate::runtime_host::DfsDataOwnerLock>,
        admission: &crate::publication_guard::NativeRepairProducerAdmission,
        carrier: &crate::DfsRoutedMessage,
        material: &crate::DfsOriginalRepairMaterial,
    ) -> Result<RepairOriginalAttemptWitness, DfsError> {
        admission.check_default_queue_owner(owner, material)?;
        let physical = RepairQueuePhysicalOwner::for_default(owner)?;
        let state = self.lock_state()?;
        if !physical.matches_state(&state) {
            return Err(custody_error(
                "ordinary Repair witness selected a different native queue",
            ));
        }
        checked_loaded_prefix(&state)?;
        let (birth, attempt_sequence) = checked_attempt(&state, carrier, material)?;
        Ok(RepairOriginalAttemptWitness {
            owner: Arc::clone(&state.owner),
            physical,
            carrier: carrier.clone(),
            material: material.clone(),
            payload_root: birth.root.clone(),
            attempt_sequence,
        })
    }

    /// Revalidate against the actual current native-owner queue before/after
    /// the RPC. No mutex survives this return. Unrelated progress and delivery
    /// updates do not erase this uniquely identified historical Attempt.
    pub(crate) fn revalidate_original_repair_attempt_witness(
        &self,
        witness: &RepairOriginalAttemptWitness,
    ) -> Result<(), DfsError> {
        witness.physical.check()?;
        let state = self.lock_state()?;
        if !Arc::ptr_eq(&state.owner, &witness.owner) || !witness.physical.matches_state(&state) {
            return Err(custody_error(
                "original attempt witness belongs to another native/resident physical owner",
            ));
        }
        checked_loaded_prefix(&state)?;
        let (birth, sequence) = checked_attempt(&state, &witness.carrier, &witness.material)?;
        if sequence != witness.attempt_sequence || birth.root != witness.payload_root {
            return Err(custody_error(
                "original attempt witness changed its persisted attempt or payload root",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixture {
        queue: LocalDfsRepairQueueStore,
        identity: SelectedOriginalFixtureIdentity,
        _content: crate::LocalContentStore,
        carrier: crate::DfsRoutedMessage,
        material: crate::DfsOriginalRepairMaterial,
    }

    fn fixture(root: &Path, with_attempt: bool) -> Fixture {
        let (queue, content, original, descriptor, payload, identity) =
            super::super::tests::selected_original_fixture(&root.join("queue"));
        queue
            .enroll_original_repair(&original, &descriptor, &payload)
            .unwrap();
        let material = queue
            .original_repair(&original)
            .unwrap()
            .unwrap()
            .original_wire_material()
            .unwrap();
        let mut carrier = original;
        carrier.frame_id += 100;
        carrier.message_id += 100;
        if with_attempt {
            queue.record_original_repair_attempt(&carrier).unwrap();
        }
        Fixture {
            queue,
            identity,
            _content: content,
            carrier,
            material,
        }
    }

    fn unchanged_state(store: &LocalDfsRepairQueueStore) -> (String, serde_json::Value) {
        super::super::tests::diagnostic_fixture_state(store)
    }

    fn rejects<T>(result: Result<T, DfsError>, expected: &str) {
        match result {
            Err(error) => assert!(
                error.to_string().contains(expected),
                "expected {expected:?}, got {error}"
            ),
            Ok(_) => panic!("expected rejection {expected:?}"),
        }
    }

    #[test]
    fn original_attempt_witness_captures_actual_sequence_without_mutation() {
        let root = sinomo_testdir::TestDir::new("original-attempt-witness-exact");
        let f = fixture(root.path(), true);
        let birth = f.queue.original_repair(&f.carrier).unwrap().unwrap();
        let before = unchanged_state(&f.queue);
        let witness = f
            .queue
            .capture_original_repair_attempt_witness(&f.carrier, &f.material)
            .unwrap();
        assert_eq!(witness.carrier(), &f.carrier);
        assert_eq!(witness.material(), &f.material);
        assert_eq!(witness.payload_root(), &birth.root);
        assert_eq!(witness.attempt_sequence(), birth.attempts[0].sequence);
        assert_eq!(
            witness.queue_incarnation(),
            birth.root.subject.queue_incarnation
        );
        assert_ne!(f.carrier.frame_id, birth.original.frame_id);
        assert_ne!(f.carrier.message_id, birth.original.message_id);
        f.queue
            .revalidate_original_repair_attempt_witness(&witness)
            .unwrap();
        assert_eq!(unchanged_state(&f.queue), before);
    }

    #[test]
    fn original_attempt_witness_refuses_missing_changed_and_cross_origin_material() {
        let root = sinomo_testdir::TestDir::new("original-attempt-witness-correlation");
        let f = fixture(root.path(), false);
        let before = unchanged_state(&f.queue);
        rejects(
            f.queue
                .capture_original_repair_attempt_witness(&f.carrier, &f.material),
            "no matching persisted carrier attempt",
        );
        assert_eq!(unchanged_state(&f.queue), before);
        f.queue.record_original_repair_attempt(&f.carrier).unwrap();
        let before = unchanged_state(&f.queue);
        let mut cross_origin = f.carrier.clone();
        cross_origin.source_node_id = NodeId::from_legacy_named("another-origin").unwrap();
        rejects(
            f.queue
                .capture_original_repair_attempt_witness(&cross_origin, &f.material),
            "carrier differs from immutable original material",
        );
        let mut wrong_attempt = f.carrier.clone();
        wrong_attempt.frame_id += 1;
        rejects(
            f.queue
                .capture_original_repair_attempt_witness(&wrong_attempt, &f.material),
            "no matching persisted carrier attempt",
        );
        // Envelope IDs are excluded from the semantic digest, but the full
        // immutable original head must still equal the actual retained birth.
        let mut changed_origin_head = f.material.clone();
        changed_origin_head.original.frame_id += 1;
        rejects(
            f.queue
                .capture_original_repair_attempt_witness(&f.carrier, &changed_origin_head),
            "material differs from the persisted native birth",
        );
        assert_eq!(unchanged_state(&f.queue), before);
        let (empty, _) =
            LocalDfsRepairQueueStore::new_selected_original_fixture(root.join("empty")).unwrap();
        let before = unchanged_state(&empty);
        rejects(
            empty.capture_original_repair_attempt_witness(&f.carrier, &f.material),
            "no exact persisted native birth",
        );
        assert_eq!(unchanged_state(&empty), before);
    }

    #[test]
    fn original_attempt_witness_refuses_duplicate_actual_carrier_sequences() {
        let root = sinomo_testdir::TestDir::new("original-attempt-witness-ambiguous");
        let f = fixture(root.path(), true);
        let witness = f
            .queue
            .capture_original_repair_attempt_witness(&f.carrier, &f.material)
            .unwrap();
        f.queue.record_original_repair_attempt(&f.carrier).unwrap();
        let birth = f.queue.original_repair(&f.carrier).unwrap().unwrap();
        assert_eq!(birth.attempts.len(), 2);
        assert_ne!(birth.attempts[0].sequence, birth.attempts[1].sequence);
        let before = unchanged_state(&f.queue);
        rejects(
            f.queue
                .capture_original_repair_attempt_witness(&f.carrier, &f.material),
            "ambiguous persisted carrier attempts",
        );
        rejects(
            f.queue.revalidate_original_repair_attempt_witness(&witness),
            "ambiguous persisted carrier attempts",
        );
        assert_eq!(unchanged_state(&f.queue), before);
    }

    #[test]
    fn original_attempt_witness_preserves_distinct_older_attempt_after_progress() {
        let root = sinomo_testdir::TestDir::new("original-attempt-witness-progress");
        let f = fixture(root.path(), true);
        let witness = f
            .queue
            .capture_original_repair_attempt_witness(&f.carrier, &f.material)
            .unwrap();
        let mut next = f.carrier.clone();
        next.frame_id += 1;
        next.message_id += 1;
        f.queue.record_original_repair_attempt(&next).unwrap();
        f.queue
            .record_original_repair_delivery(
                &f.carrier,
                Some(sinomo_core::network::NetworkDeliveryState::PossiblyApplied),
            )
            .unwrap();
        let mut unrelated = f.queue.view().unwrap().records[0].clone();
        unrelated.intent_id.push_str("-unrelated");
        f.queue
            .append_operations(vec![DfsRepairQueueOperation::EnqueueIfMissing {
                record: unrelated,
            }])
            .unwrap();
        f.queue
            .compact_if_needed(DfsRepairQueueCompactionOptions::force())
            .unwrap();
        let before = unchanged_state(&f.queue);
        f.queue
            .revalidate_original_repair_attempt_witness(&witness)
            .unwrap();
        let same = f
            .queue
            .capture_original_repair_attempt_witness(&f.carrier, &f.material)
            .unwrap();
        assert_eq!(same.attempt_sequence(), witness.attempt_sequence());
        let newer = f
            .queue
            .capture_original_repair_attempt_witness(&next, &f.material)
            .unwrap();
        assert!(newer.attempt_sequence() > same.attempt_sequence());
        assert_eq!(unchanged_state(&f.queue), before);
    }

    #[test]
    fn original_attempt_witness_refuses_default_foreign_and_replaced_physical_owner() {
        let root = sinomo_testdir::TestDir::new("original-attempt-witness-physical");
        let f = fixture(root.path(), true);
        let witness = f
            .queue
            .capture_original_repair_attempt_witness(&f.carrier, &f.material)
            .unwrap();
        let ordinary = LocalDfsRepairQueueStore::new(f.identity.snapshot_path());
        let before = unchanged_state(&f.queue);
        rejects(
            ordinary.revalidate_original_repair_attempt_witness(&witness),
            "different selected original reader",
        );
        assert_eq!(unchanged_state(&f.queue), before);
        let plain = LocalDfsRepairQueueStore::new(root.join("plain.sobj"));
        let before = unchanged_state(&plain);
        rejects(
            plain.capture_original_repair_attempt_witness(&f.carrier, &f.material),
            "requires its explicit physical queue owner",
        );
        assert_eq!(unchanged_state(&plain), before);
        let foreign_root = sinomo_testdir::TestDir::new("original-attempt-witness-foreign");
        let foreign = fixture(foreign_root.path(), true);
        let before = unchanged_state(&foreign.queue);
        rejects(
            foreign
                .queue
                .revalidate_original_repair_attempt_witness(&witness),
            "another resident queue owner",
        );
        assert_eq!(unchanged_state(&foreign.queue), before);

        // Copy the actual valid queue files, not an empty missing-marker root.
        // Byte equality does not preserve the pinned directory identity.
        let retained = root.join("retained-old-queue");
        fs::rename(f.identity.root(), &retained).unwrap();
        fs::create_dir(f.identity.root()).unwrap();
        for entry in fs::read_dir(&retained).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_file() {
                fs::copy(entry.path(), f.identity.root().join(entry.file_name())).unwrap();
            }
        }
        let before = unchanged_state(&f.queue);
        rejects(
            f.queue.revalidate_original_repair_attempt_witness(&witness),
            "physically replaced",
        );
        rejects(
            f.queue
                .capture_original_repair_attempt_witness(&f.carrier, &f.material),
            "physically replaced",
        );
        assert_eq!(unchanged_state(&f.queue), before);
    }

    #[test]
    fn original_attempt_witness_refuses_cold_uncertain_and_disk_drift_without_recovery() {
        use std::io::Write as _;
        let root = sinomo_testdir::TestDir::new("original-attempt-witness-nonrecovering");
        let f = fixture(root.path(), true);
        let witness = f
            .queue
            .capture_original_repair_attempt_witness(&f.carrier, &f.material)
            .unwrap();
        // Resident branch fault controls, not an OS-crash/restart claim.
        f.queue.inner.lock().unwrap().loaded = false;
        let before = unchanged_state(&f.queue);
        rejects(
            f.queue
                .capture_original_repair_attempt_witness(&f.carrier, &f.material),
            "already loaded resident prefix",
        );
        assert_eq!(unchanged_state(&f.queue), before);
        f.queue.inner.lock().unwrap().loaded = true;
        f.queue.inner.lock().unwrap().resync_on_load = true;
        REPAIR_QUEUE_FAIL_RESYNC.set(true);
        let before = unchanged_state(&f.queue);
        rejects(
            f.queue.revalidate_original_repair_attempt_witness(&witness),
            "uncertain or active writer prefix",
        );
        assert!(REPAIR_QUEUE_FAIL_RESYNC.replace(false));
        assert_eq!(unchanged_state(&f.queue), before);
        f.queue.inner.lock().unwrap().resync_on_load = false;

        let log = repair_queue_active_log_path(f.identity.snapshot_path());
        #[cfg(unix)]
        {
            let retained_log = root.join("actual-retained-log");
            fs::rename(&log, &retained_log).unwrap();
            std::os::unix::fs::symlink(&retained_log, &log).unwrap();
            let before = unchanged_state(&f.queue);
            rejects(
                f.queue.revalidate_original_repair_attempt_witness(&witness),
                "regular non-symlink queue files",
            );
            assert_eq!(unchanged_state(&f.queue), before);
            fs::remove_file(&log).unwrap();
            fs::rename(retained_log, &log).unwrap();
        }
        let mut file = fs::OpenOptions::new().append(true).open(&log).unwrap();
        file.write_all(b"actual incomplete suffix").unwrap();
        file.sync_all().unwrap();
        drop(file);
        let before = unchanged_state(&f.queue);
        rejects(
            f.queue.revalidate_original_repair_attempt_witness(&witness),
            "queue files differ from the resident prefix",
        );
        assert_eq!(unchanged_state(&f.queue), before);
    }
}
