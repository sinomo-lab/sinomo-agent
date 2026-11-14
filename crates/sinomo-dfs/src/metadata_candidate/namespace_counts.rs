use super::*;
use crate::*;
use std::collections::BTreeSet;
use std::sync::Arc;

fn invalid(message: &str) -> MetadataBackendError {
    MetadataBackendError::Serde(message.into())
}

struct EncodedBatchBudget(usize);
impl std::io::Write for EncodedBatchBudget {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.0 {
            return Err(std::io::Error::other(
                "count initialization encoded batch exceeds 8MiB",
            ));
        }
        self.0 -= bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl DfsMetadataCandidateIdentity {
    /// Immutable DATA provenance used by each count conversion batch. It is not
    /// a completion receipt or a grant to open the candidate as ordinary DATA.
    pub fn namespace_count_predecessor_binding(&self) -> Result<String> {
        Ok(format!(
            "{}{}",
            crate::namespace_counts::COUNT_PREDECESSOR_BINDING_PREFIX,
            blake3::hash(
                &serde_json::to_vec(self)
                    .map_err(|error| MetadataBackendError::Serde(error.to_string()))?
            )
            .to_hex()
        ))
    }
}

impl DfsMetadataCandidateWriter {
    pub fn namespace_count_cohort_operation_id(
        initialization_id: &str,
        shard: &DfsMetadataShardId,
        predecessor: &DfsMetadataCandidateIdentity,
    ) -> Result<String> {
        if initialization_id.is_empty() {
            return Err(invalid("count cohort initialization identity is empty"));
        }
        let binding = predecessor.namespace_count_predecessor_binding()?;
        let hash = binding
            .strip_prefix(crate::namespace_counts::COUNT_PREDECESSOR_BINDING_PREFIX)
            .expect("canonical binding prefix");
        Ok(format!(
            "namespace-count-cohort:{initialization_id}:{hash}:{}",
            shard.0
        ))
    }

    /// Append one bounded, deterministic initialization to hidden candidate DATA.
    /// The caller's complete-cohort seal owns census correctness and activation;
    /// this writer proves only exact predecessor rows and durable replay identity.
    pub fn initialize_namespace_counts(
        &self,
        expected_identity: &DfsMetadataCandidateIdentity,
        scope: DfsPreparedOperationScope,
        expected_rows: &[DfsObjectMetadata],
        counted_rows: &[DfsObjectMetadata],
        at: time::OffsetDateTime,
    ) -> Result<DfsMetadataCandidateIdentity> {
        if expected_rows.len() != counted_rows.len() || expected_rows.len() > 256 {
            return Err(invalid("count initialization exceeds its exact row batch"));
        }
        serde_json::to_writer(
            EncodedBatchBudget(8 * 1024 * 1024),
            &(expected_rows, counted_rows),
        )
        .map_err(|error| MetadataBackendError::Serde(error.to_string()))?;
        let actual = inspect_metadata_candidate(&self.root)?;
        if expected_rows.is_empty() {
            return if actual == *expected_identity {
                Ok(actual)
            } else {
                Err(invalid(
                    "empty count initialization changed candidate identity",
                ))
            };
        }
        let mut ids = BTreeSet::new();
        for (before, after) in expected_rows.iter().zip(counted_rows) {
            let mut only_count = before.clone();
            only_count.namespace_link_count = after.namespace_link_count;
            let valid_count = match after.kind {
                DfsObjectKind::Hardlink => false,
                DfsObjectKind::Tombstone => {
                    after.tombstone_provenance.is_none() && after.namespace_link_count == Some(0)
                }
                DfsObjectKind::Directory => after.namespace_link_count.is_some_and(|n| n >= 2),
                _ => after.namespace_link_count.is_some_and(|n| n > 0),
            };
            if before.file_system_id != scope.file_system_id
                || before.namespace_link_count.is_some()
                || only_count != *after
                || !valid_count
                || !ids.insert(before.object_id.clone())
            {
                return Err(invalid(
                    "count initialization differs from its exact unknown predecessor",
                ));
            }
        }
        let transaction = DfsNamespaceTransaction {
            transaction_id: scope.operation_id.clone(),
            file_system_id: scope.file_system_id.clone(),
            objects: counted_rows.to_vec(),
            versions: Vec::new(),
            advisory_lock_states: Vec::new(),
            content_reference_identity: None,
            contribution_digest: None,
            extension_expectations: Vec::new(),
            file_version_pins: None,
            created_at: at,
        };
        let payload = DfsPreparedOperationPayload::for_namespace_transaction(
            scope,
            expected_rows
                .iter()
                .map(|row| DfsNamespaceObjectVersionExpectation {
                    object_id: row.object_id.clone(),
                    expected_version: Some(row.version.clone()),
                })
                .collect(),
            expected_rows
                .iter()
                .map(|row| DfsNamespacePathOccupantExpectation {
                    path: crate::metadata_keys::normalize_path(&row.path),
                    expected_object_id: Some(row.object_id.clone()),
                })
                .collect(),
            transaction,
            Vec::new(),
            None,
            None,
            Vec::new(),
            None,
            None,
        )?;
        self.append_count_candidate(
            expected_identity,
            actual,
            payload,
            expected_rows,
            counted_rows,
            at,
        )
    }

    /// Retain the cohort obligation even when its census changes no rows. This
    /// hidden DATA record never grants activation or ordinary storage readiness.
    pub fn record_namespace_count_cohort(
        &self,
        expected_identity: &DfsMetadataCandidateIdentity,
        scope: DfsPreparedOperationScope,
        at: time::OffsetDateTime,
    ) -> Result<DfsMetadataCandidateIdentity> {
        let (_, predecessor_hash) = crate::namespace_counts::cohort_binding_components(
            &scope.operation_id,
            &scope.shard_id,
        )
        .ok_or_else(|| {
            invalid("count cohort binding has no canonical scoped operation identity")
        })?;
        if expected_identity.namespace_count_predecessor_binding()?
            != format!(
                "{}{predecessor_hash}",
                crate::namespace_counts::COUNT_PREDECESSOR_BINDING_PREFIX
            )
        {
            return Err(invalid(
                "count cohort binding names another predecessor identity",
            ));
        }
        let actual = inspect_metadata_candidate(&self.root)?;
        let payload = DfsPreparedOperationPayload::for_metadata_effects(
            scope,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            at,
        )?;
        self.append_count_candidate(expected_identity, actual, payload, &[], &[], at)
    }

    fn append_count_candidate(
        &self,
        expected_identity: &DfsMetadataCandidateIdentity,
        actual: DfsMetadataCandidateIdentity,
        mut payload: DfsPreparedOperationPayload,
        expected_rows: &[DfsObjectMetadata],
        counted_rows: &[DfsObjectMetadata],
        at: time::OffsetDateTime,
    ) -> Result<DfsMetadataCandidateIdentity> {
        payload.journal_operation_id =
            Some(expected_identity.namespace_count_predecessor_binding()?);
        payload.expected_journal_sequence = Some(expected_identity.prefix_proof.journal_sequence);
        payload.validate()?;
        let sequence = expected_identity
            .prefix_proof
            .journal_sequence
            .checked_add(1)
            .ok_or_else(|| invalid("count initialization sequence overflow"))?;
        let commit_sequence = sequence
            .checked_add(1)
            .ok_or_else(|| invalid("count initialization sequence overflow"))?;
        let mut record = DfsPreparedOperationRecord {
            digest: DfsPreparedOperationDigest::at_sequence(&payload, sequence)?,
            payload: Arc::new(payload),
            status: DfsPreparedOperationStatus::Prepared,
            committed_entry: None,
        };
        let snapshot = read_metadata_candidate(&self.root)?;
        let mut state = DfsMetadataCandidateState::new(*snapshot.tables)?;
        if actual != *expected_identity {
            let Some(existing) = state
                .tables
                .prepared_operations_v7
                .get(&record.digest.operation_id)
            else {
                return Err(invalid(
                    "count initialization candidate predecessor changed",
                ));
            };
            if existing.digest != record.digest
                || existing.payload != record.payload
                || existing.status != DfsPreparedOperationStatus::Committed
                || existing
                    .committed_entry
                    .as_ref()
                    .is_none_or(|entry| entry.sequence != commit_sequence)
                || actual.prefix_proof.journal_sequence < commit_sequence
                || counted_rows
                    .iter()
                    .any(|row| state.tables.objects.get(&row.object_id) != Some(row))
            {
                return Err(invalid(
                    "count initialization replay differs from its committed candidate",
                ));
            }
            return Ok(actual);
        }
        if state
            .tables
            .prepared_operations_v7
            .values()
            .any(|p| p.status == DfsPreparedOperationStatus::Prepared)
            || state
                .tables
                .namespace_jobs
                .values()
                .any(|job| job.status == DfsNamespaceJobStatus::Running)
            || state
                .tables
                .namespace_requests
                .values()
                .any(|request| request.status == DfsNamespaceRequestStatus::Pending)
            || expected_rows
                .iter()
                .any(|row| state.tables.objects.get(&row.object_id) != Some(row))
        {
            return Err(invalid(
                "count initialization requires exact settled predecessor DATA",
            ));
        }
        if state
            .tables
            .prepared_operations_v7
            .contains_key(&record.digest.operation_id)
            || state
                .tables
                .prepared_operation_evictions
                .contains_key(&record.digest.operation_id)
        {
            return Err(invalid(
                "count initialization operation identity was already used",
            ));
        }
        if let Some(next) = commit_sequence.checked_add(1)
            && crate::metadata_journal::read_segmented_journal_frame_at_sequence(&self.root, next)?
                .is_some()
        {
            return Err(invalid(
                "count initialization candidate has a later journal owner",
            ));
        }
        let prepare = DfsMetadataJournalEntry {
            shard_id: record.digest.shard_id.clone(),
            sequence,
            created_at: at,
            mutation: DfsMetadataMutation::PrepareOperationV7 {
                file_system_id: record.digest.file_system_id.clone(),
                operation_id: record.digest.operation_id.clone(),
                record: Box::new(record.clone()),
            },
        };
        let after_prepare =
            self.append_count_frame_or_resume(&prepare, &expected_identity.prefix_proof)?;
        let commit = DfsMetadataJournalEntry {
            shard_id: record.digest.shard_id.clone(),
            sequence: commit_sequence,
            created_at: at,
            mutation: DfsMetadataMutation::CommitPreparedOperationV7 {
                file_system_id: record.digest.file_system_id.clone(),
                operation_id: record.digest.operation_id.clone(),
                digest: record.digest.clone(),
            },
        };
        let proof = self.append_count_frame_or_resume(&commit, &after_prepare)?;
        record.status = DfsPreparedOperationStatus::Committed;
        record.committed_entry = Some(Box::new(
            DfsPreparedOperationCommitReference::from_journal_entry(&commit)?,
        ));
        state.apply_namespace(&record.transaction)?;
        state.restore_prepared(record)?;
        self.write(state.snapshot(), proof)
    }

    fn append_count_frame_or_resume(
        &self,
        entry: &DfsMetadataJournalEntry,
        previous: &DfsMetadataPrefixProof,
    ) -> Result<DfsMetadataPrefixProof> {
        if let Some(frame) = crate::metadata_journal::read_segmented_journal_frame_at_sequence(
            &self.root,
            entry.sequence,
        )? {
            if frame.entry() != entry {
                return Err(invalid(
                    "count initialization journal slot belongs to another exact body",
                ));
            }
            return crate::metadata_journal::metadata_prefix_proof_after_exact_frame(
                previous,
                entry,
                &entry.shard_id,
                frame.exact_bytes(),
            );
        }
        self.append(entry, previous)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source(
        root: &Path,
    ) -> (
        DfsMetadataCandidateIdentity,
        DfsObjectMetadata,
        DfsPreparedOperationRecord,
    ) {
        let node = sinomo_core::node::NodeId::generate();
        let mut object = crate::posix_object::new_posix_metadata_object(
            &node,
            "/legacy-file",
            DfsObjectKind::File,
        );
        object.namespace_link_count = None;
        let mut second = object.clone();
        second.object_id = crate::DfsObjectId::generate();
        second.path = "/legacy-second".into();
        let mut state = DfsMetadataCandidateState::new(Default::default()).unwrap();
        let effects = vec![
            DfsPreparedMetadataEffect::PutFileSystem {
                file_system: DfsFileSystem::sifs(node),
            },
            DfsPreparedMetadataEffect::PutObject {
                object: object.clone(),
            },
            DfsPreparedMetadataEffect::PutObject { object: second },
        ];
        for effect in &effects {
            state.apply(effect.clone()).unwrap();
        }
        let writer = DfsMetadataCandidateWriter::open(root).unwrap();
        let initial = DfsMetadataPrefixProof {
            version: 2,
            journal_sequence: 0,
            digest: format!("dfs-metadata-prefix-v2:blake3:{}", "0".repeat(64)),
        };
        let payload = DfsPreparedOperationPayload::for_metadata_effects(
            DfsPreparedOperationScope {
                file_system_id: object.file_system_id.clone(),
                shard_id: DfsMetadataShardId::new("root"),
                owner_epoch: 1,
                operation_id: "historical-body".into(),
            },
            effects
                .iter()
                .filter_map(|effect| match effect {
                    DfsPreparedMetadataEffect::PutObject { object } => {
                        Some(DfsNamespaceObjectVersionExpectation {
                            object_id: object.object_id.clone(),
                            expected_version: None,
                        })
                    }
                    _ => None,
                })
                .collect(),
            effects
                .iter()
                .filter_map(|effect| match effect {
                    DfsPreparedMetadataEffect::PutObject { object } => {
                        Some(DfsNamespacePathOccupantExpectation {
                            path: crate::metadata_keys::normalize_path(&object.path),
                            expected_object_id: None,
                        })
                    }
                    _ => None,
                })
                .collect(),
            effects,
            time::OffsetDateTime::UNIX_EPOCH,
        )
        .unwrap();
        let (prepared, prefix) = writer.prepare(payload, &initial).unwrap();
        let (committed, prefix) = writer
            .commit(prepared, &prefix, time::OffsetDateTime::UNIX_EPOCH)
            .unwrap();
        state.restore_prepared(committed.clone()).unwrap();
        (
            writer.write(state.snapshot(), prefix).unwrap(),
            object,
            committed,
        )
    }

    fn scope(object: &DfsObjectMetadata) -> DfsPreparedOperationScope {
        DfsPreparedOperationScope {
            file_system_id: object.file_system_id.clone(),
            shard_id: DfsMetadataShardId::new("root"),
            owner_epoch: 1,
            operation_id: "count-cohort-root-batch0".into(),
        }
    }

    #[test]
    fn count_candidate_initialization_preserves_history_and_resumes_committed_frames() {
        let root = sinomo_testdir::TestDir::new("count-candidate-original-body");
        let (initial, original, historical) = source(root.path());
        let snapshot_path = metadata_table_snapshot_path(root.path());
        let original_bytes = fs::read(&snapshot_path).unwrap();
        let mut counted = original.clone();
        counted.namespace_link_count = Some(1);
        let writer = DfsMetadataCandidateWriter::open(root.path()).unwrap();
        let result = writer
            .initialize_namespace_counts(
                &initial,
                scope(&original),
                &[original.clone()],
                &[counted.clone()],
                time::OffsetDateTime::UNIX_EPOCH,
            )
            .unwrap();
        assert_eq!(
            result.prefix_proof.journal_sequence,
            initial.prefix_proof.journal_sequence + 2
        );
        let after = read_metadata_candidate(root.path()).unwrap();
        assert_eq!(
            after
                .tables
                .prepared_operations_v7
                .iter()
                .find(|record| record.digest == historical.digest),
            Some(&historical)
        );
        assert_eq!(
            after
                .tables
                .objects
                .iter()
                .find(|row| row.object_id == original.object_id),
            Some(&counted)
        );
        assert_eq!(
            writer
                .initialize_namespace_counts(
                    &initial,
                    scope(&original),
                    &[original.clone()],
                    &[counted.clone()],
                    time::OffsetDateTime::UNIX_EPOCH
                )
                .unwrap(),
            result
        );
        let mut wrong = counted.clone();
        wrong.namespace_link_count = Some(2);
        assert!(
            writer
                .initialize_namespace_counts(
                    &initial,
                    scope(&original),
                    &[original.clone()],
                    &[wrong],
                    time::OffsetDateTime::UNIX_EPOCH
                )
                .is_err()
        );
        // Crash after the exact Commit append, before atomic snapshot replacement.
        // Keep the real journal bytes and restore only this hidden candidate's
        // original image; resumption must consume the original two slots.
        crate::metadata_io::write_file_atomic(&snapshot_path, &original_bytes).unwrap();
        let journal = root
            .path()
            .join("journal/segments/00000000000000000001.slog");
        let journal_before = fs::read(&journal).unwrap();
        assert_eq!(
            writer
                .initialize_namespace_counts(
                    &initial,
                    scope(&original),
                    &[original.clone()],
                    &[counted.clone()],
                    time::OffsetDateTime::UNIX_EPOCH
                )
                .unwrap(),
            result
        );
        assert_eq!(fs::read(&journal).unwrap(), journal_before);
        let second = read_metadata_candidate(root.path())
            .unwrap()
            .tables
            .objects
            .into_iter()
            .find(|object| object.path == "/legacy-second")
            .unwrap();
        let mut second_counted = second.clone();
        second_counted.namespace_link_count = Some(1);
        let mut second_scope = scope(&second);
        second_scope.operation_id = "count-cohort-root-batch1".into();
        let final_identity = writer
            .initialize_namespace_counts(
                &result,
                second_scope,
                &[second],
                &[second_counted],
                time::OffsetDateTime::UNIX_EPOCH,
            )
            .unwrap();
        let final_journal = fs::read(&journal).unwrap();
        assert_eq!(
            writer
                .initialize_namespace_counts(
                    &initial,
                    scope(&original),
                    &[original.clone()],
                    &[counted.clone()],
                    time::OffsetDateTime::UNIX_EPOCH
                )
                .unwrap(),
            final_identity
        );
        assert_eq!(
            fs::read(journal).unwrap(),
            final_journal,
            "earlier batch verification cannot append after later batches"
        );
        let mut wrong_predecessor = initial.clone();
        wrong_predecessor.file_fingerprint.push('0');
        assert!(
            writer
                .initialize_namespace_counts(
                    &wrong_predecessor,
                    scope(&original),
                    &[original.clone()],
                    &[counted.clone()],
                    time::OffsetDateTime::UNIX_EPOCH
                )
                .is_err()
        );
        drop(writer);
        // Recover the count from durable original Prepare/Commit frames alone.
        fs::remove_file(snapshot_path).unwrap();
        let backend =
            crate::LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root.path())
                .unwrap();
        assert_eq!(
            backend.get_object(&original.object_id).unwrap(),
            Some(counted)
        );
        assert_eq!(
            backend
                .prepared_operation_record(&historical.operation_id)
                .unwrap(),
            Some(historical)
        );
    }

    #[test]
    fn count_candidate_rejects_changed_predecessor_before_journal_append() {
        let root = sinomo_testdir::TestDir::new("count-candidate-predecessor");
        let (initial, original, _) = source(root.path());
        let writer = DfsMetadataCandidateWriter::open(root.path()).unwrap();
        let mut changed = original.clone();
        changed.mode ^= 1;
        let mut counted = changed.clone();
        counted.namespace_link_count = Some(1);
        assert!(
            writer
                .initialize_namespace_counts(
                    &initial,
                    scope(&original),
                    &[changed],
                    &[counted],
                    time::OffsetDateTime::UNIX_EPOCH
                )
                .is_err()
        );
        assert_eq!(inspect_metadata_candidate(root.path()).unwrap(), initial);
        assert!(
            crate::metadata_journal::read_segmented_journal_frame_at_sequence(
                root.path(),
                initial.prefix_proof.journal_sequence + 1
            )
            .unwrap()
            .is_none()
        );
        assert_eq!(
            writer
                .initialize_namespace_counts(
                    &initial,
                    scope(&original),
                    &[],
                    &[],
                    time::OffsetDateTime::UNIX_EPOCH
                )
                .unwrap(),
            initial
        );
    }
    #[test]
    fn count_candidate_cohort_obligation_survives_marker_loss_and_compaction() {
        let root = sinomo_testdir::TestDir::new("count-candidate-cohort-obligation");
        let (initial, original, _) = source(root.path());
        let writer = DfsMetadataCandidateWriter::open(root.path()).unwrap();
        let rows = read_metadata_candidate(root.path()).unwrap().tables.objects;
        let counted = rows
            .iter()
            .cloned()
            .map(|mut row| {
                row.namespace_link_count = Some(1);
                row
            })
            .collect::<Vec<_>>();
        let counted_identity = writer
            .initialize_namespace_counts(
                &initial,
                scope(&original),
                &rows,
                &counted,
                time::OffsetDateTime::UNIX_EPOCH,
            )
            .unwrap();
        let mut binding = scope(&original);
        binding.operation_id = DfsMetadataCandidateWriter::namespace_count_cohort_operation_id(
            "original-complete-cut",
            &binding.shard_id,
            &counted_identity,
        )
        .unwrap();
        let bound = writer
            .record_namespace_count_cohort(
                &counted_identity,
                binding.clone(),
                time::OffsetDateTime::UNIX_EPOCH,
            )
            .unwrap();
        assert_eq!(
            bound.prefix_proof.journal_sequence,
            counted_identity.prefix_proof.journal_sequence + 2
        );
        assert_eq!(
            writer
                .record_namespace_count_cohort(
                    &counted_identity,
                    binding.clone(),
                    time::OffsetDateTime::UNIX_EPOCH
                )
                .unwrap(),
            bound
        );
        assert_eq!(
            crate::namespace_count_cohort_obligations(
                &read_metadata_candidate(root.path()).unwrap(),
                &binding.shard_id
            )
            .unwrap(),
            std::collections::BTreeSet::from([(
                original.file_system_id.clone(),
                "original-complete-cut".to_string()
            )])
        );
        assert!(
            crate::namespace_count_cohort_obligations(
                &read_metadata_candidate(root.path()).unwrap(),
                &DfsMetadataShardId::new("foreign-scope")
            )
            .unwrap()
            .is_empty()
        );
        let valid = read_metadata_candidate(root.path()).unwrap();
        let mut wrong_scope = valid.clone();
        wrong_scope
            .tables
            .prepared_operations_v7
            .iter_mut()
            .find(|row| row.operation_id == binding.operation_id)
            .unwrap()
            .digest
            .shard_id = DfsMetadataShardId::new("forged-scope");
        assert!(
            crate::namespace_count_cohort_obligations(&wrong_scope, &binding.shard_id).is_err(),
            "misbound claimed record is validated before physical-scope filtering"
        );
        let mut malformed = valid.clone();
        let malformed_record = malformed
            .tables
            .prepared_operations_v7
            .iter_mut()
            .find(|row| row.operation_id == binding.operation_id)
            .unwrap();
        std::sync::Arc::make_mut(&mut malformed_record.payload).expected_journal_sequence = None;
        assert!(crate::namespace_count_cohort_obligations(&malformed, &binding.shard_id).is_err());
        for status in [
            DfsPreparedOperationStatus::Aborted,
            DfsPreparedOperationStatus::Superseded,
        ] {
            let mut terminal_data = valid.clone();
            let terminal = terminal_data
                .tables
                .prepared_operations_v7
                .iter_mut()
                .find(|row| row.operation_id == binding.operation_id)
                .unwrap();
            terminal.status = status;
            terminal.committed_entry = None;
            terminal.validate().unwrap();
            assert!(
                crate::namespace_count_cohort_obligations(&terminal_data, &binding.shard_id)
                    .unwrap()
                    .is_empty()
            );
        }
        assert!(!root.path().join("metadata-import-v7.json").exists());
        assert!(
            matches!(
                LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root.path()),
                Err(MetadataBackendError::StoragePreparationPending { .. })
            ),
            "a missing convenience marker cannot release the durable cohort obligation"
        );
        let snapshot = read_metadata_candidate(root.path()).unwrap();
        let mut state = DfsMetadataCandidateState::new(*snapshot.tables).unwrap();
        let record = state
            .tables
            .prepared_operations_v7
            .get(&binding.operation_id)
            .unwrap()
            .clone();
        state
            .apply(DfsPreparedMetadataEffect::RemovePreparedOperation {
                digest: record.digest.clone(),
            })
            .unwrap();
        let eviction = state
            .tables
            .prepared_operation_evictions
            .get(&binding.operation_id)
            .unwrap();
        assert_eq!(
            eviction.committed_entry, record.committed_entry,
            "ordinary compaction preserves the exact commit timestamp and sequence"
        );
        assert_eq!(eviction.digest, record.digest);
        writer.write(state.snapshot(), bound.prefix_proof).unwrap();
        assert!(
            matches!(
                LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root.path()),
                Err(MetadataBackendError::StoragePreparationPending { .. })
            ),
            "the validated compact digest retains the same refusal obligation"
        );
        inspect_metadata_candidate(root.path()).unwrap();
    }
    #[test]
    fn count_candidate_ordinary_prefix_collision_is_not_a_binding() {
        let root = sinomo_testdir::TestDir::new("count-candidate-prefix-collision");
        let (initial, original, _) = source(root.path());
        let writer = DfsMetadataCandidateWriter::open(root.path()).unwrap();
        let snapshot = read_metadata_candidate(root.path()).unwrap();
        let filesystem = snapshot.tables.file_systems[0].clone();
        let mut ordinary_scope = scope(&original);
        ordinary_scope.operation_id = "namespace-count-cohort:ordinary:component:root".into();
        let mut payload = DfsPreparedOperationPayload::for_metadata_effects(
            ordinary_scope,
            Vec::new(),
            Vec::new(),
            vec![DfsPreparedMetadataEffect::PutFileSystem {
                file_system: filesystem,
            }],
            time::OffsetDateTime::UNIX_EPOCH,
        )
        .unwrap();
        payload.journal_operation_id = Some(initial.namespace_count_predecessor_binding().unwrap());
        payload.expected_journal_sequence = Some(initial.prefix_proof.journal_sequence);
        let (prepare, prefix) = writer.prepare(payload, &initial.prefix_proof).unwrap();
        let (record, prefix) = writer
            .commit(prepare, &prefix, time::OffsetDateTime::UNIX_EPOCH)
            .unwrap();
        let mut state = DfsMetadataCandidateState::new(*snapshot.tables).unwrap();
        state.restore_prepared(record.clone()).unwrap();
        writer.write(state.snapshot(), prefix.clone()).unwrap();
        assert!(
            crate::namespace_count_cohort_obligations(
                &read_metadata_candidate(root.path()).unwrap(),
                &record.shard_id
            )
            .unwrap()
            .is_empty()
        );
        // Model the compact representation only; this is a refusal classifier
        // control, not a fabricated native GC or activation acknowledgement.
        state
            .tables
            .prepared_operations_v7
            .remove(&record.operation_id);
        state.tables.prepared_operation_evictions.insert(
            record.operation_id.clone(),
            DfsPreparedOperationEviction {
                digest: record.digest.clone(),
                status: record.status,
                committed_entry: record.committed_entry.clone(),
            },
        );
        writer.write(state.snapshot(), prefix).unwrap();
        assert!(
            crate::namespace_count_cohort_obligations(
                &read_metadata_candidate(root.path()).unwrap(),
                &record.shard_id
            )
            .unwrap()
            .is_empty()
        );
    }
    #[test]
    fn native_candidate_replay_preserves_prepare_receipts_and_exact_commit_body() {
        let (runtime, mut service, config) = crate::test_support::current_native_metadata_service(
            "native-count-original-journal-replay",
            1,
            "/services",
        )
        .unwrap();
        let shard = DfsMetadataShardId::new(&config.shard_id);
        let before = runtime
            .node_metadata(&config.primary_node_id)
            .unwrap()
            .export_shard_table_snapshot(&shard)
            .unwrap()
            .unwrap();
        let file = service
            .posix_write_file(
                "/services/original-replay",
                b"native source bytes",
                crate::DfsWriteOptions::replicated_eventual(vec![config.primary_node_id.clone()]),
            )
            .unwrap();
        let source = runtime.node_metadata(&config.primary_node_id).unwrap();
        let after = source.export_shard_table_snapshot(&shard).unwrap().unwrap();
        let suffix = source
            .journal_batch_for_shard_since_limited(
                &shard,
                before.materialized_through_sequence,
                128,
                8 * 1024 * 1024,
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            suffix.next_after_sequence,
            after.materialized_through_sequence
        );
        let mut projected = DfsMetadataCandidateState::new(*before.tables).unwrap();
        let mut observed_prepare_receipt = false;
        let mut future_publications = std::collections::BTreeSet::new();
        let mut observed_future_commit = false;
        for entry in &suffix.entries {
            let previous_object = projected.object(&file.object_id);
            let stages_future = matches!(&entry.mutation, DfsMetadataMutation::PrepareOperationV7 { record, .. }
                if record.transaction.versions.iter().any(|version| version.object_id == file.object_id && version.version == file.version)
                    || record.metadata_effects.iter().any(|effect| matches!(effect, DfsPreparedMetadataEffect::PutVersion { version }
                        if version.object_id == file.object_id && version.version == file.version)));
            if stages_future {
                assert!(projected.version(&file.object_id, &file.version).is_none());
            }
            projected.replay_native_journal_entry(entry).unwrap();
            if stages_future {
                assert_eq!(
                    projected.object(&file.object_id),
                    previous_object,
                    "an original Prepare is not a visible future object head"
                );
                assert!(
                    projected.version(&file.object_id, &file.version).is_none(),
                    "an uncommitted future version remains outside the visible source view"
                );
                let DfsMetadataMutation::PrepareOperationV7 { record, .. } = &entry.mutation else {
                    unreachable!()
                };
                future_publications.insert(record.operation_id.clone());
            }
            if let DfsMetadataMutation::CommitPreparedOperationV7 { operation_id, .. } =
                &entry.mutation
                && future_publications.contains(operation_id)
            {
                observed_future_commit = true;
                assert!(projected.version(&file.object_id, &file.version).is_some());
                assert_eq!(
                    projected.object(&file.object_id).unwrap().version,
                    file.version
                );
            }
            if let DfsMetadataMutation::PrepareOperationV7 { record, .. } = &entry.mutation {
                for receipt in &record.content_receipts {
                    if receipt.receipt.object_id == file.object_id {
                        observed_prepare_receipt = true;
                        assert!(
                            projected
                                .receipts_for_version(&file.object_id, &file.version)
                                .contains(receipt)
                        );
                    }
                }
            }
        }
        assert!(
            observed_future_commit && !future_publications.is_empty(),
            "actual native source contains the future version Prepare and its matching Commit"
        );
        assert!(
            observed_prepare_receipt,
            "the genuine original Prepare carries its durable content receipt"
        );
        assert_eq!(
            projected.snapshot(),
            *after.tables,
            "the same native projector preserves original payloads, digests and terminal references"
        );
        drop(source);
        drop(service);
        runtime.close().unwrap();
    }
}
