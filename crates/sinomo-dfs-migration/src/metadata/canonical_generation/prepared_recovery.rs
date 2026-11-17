//! Exact current reservations derived from the original fenced replay. The
//! base capture stays immutable, including its unresolved-operation references.
use super::*;
use sinomo_dfs::{
    DfsObjectKind, DfsPreparedMetadataEffect, DfsPreparedOperationRecord,
    DfsPreparedOperationStatus,
};

const CHECKPOINT: &str = "captured-v7-recovery.json";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(super) struct PreparedRecovery {
    version: u16,
    records: Vec<DfsPreparedOperationRecord>,
    pub(super) binding: String,
}

fn work_matches(work: &OriginalWorkRef, record: &DfsPreparedOperationRecord) -> bool {
    work.family == "prepared-operation-v7"
        && work.original_id == record.digest.operation_id
        && work.original_digest.as_ref() == Some(&record.digest.payload_digest)
}

/// Only count-neutral object metadata has a current generic recovery owner.
/// Namespace/parent participation, content custody and outcome publication
/// require their own original family decisions and remain migration blockers.
fn supported(unit: &CapturedMetadataUnit, record: &DfsPreparedOperationRecord) -> bool {
    let payload = &record.payload;
    let tx = &payload.transaction;
    if record.status != DfsPreparedOperationStatus::Prepared
        || record.committed_entry.is_some()
        || record.digest.owner_epoch == 0
        || record.digest.prepare_sequence > unit.source.source_journal_sequence
        || record.digest.shard_id != unit.source.physical_shard
        || !unit
            .source
            .file_system_ids
            .contains(&record.digest.file_system_id)
        || !unit.source.projection_assignments.iter().any(|assignment| {
            assignment.file_system_id == record.digest.file_system_id
                && assignment.shard_id == record.digest.shard_id.0
                && record.digest.owner_epoch <= assignment.epoch
        })
        || payload.expected_journal_sequence.is_some()
        || !tx.objects.is_empty()
        || !tx.versions.is_empty()
        || tx.file_version_pins.is_some()
        || !tx.advisory_lock_states.is_empty()
        || tx.content_reference_identity.is_some()
        || tx.contribution_digest.is_some()
        || !tx.extension_expectations.is_empty()
        || payload.prepared_intent.is_some()
        || payload.terminal_intent.is_some()
        || payload.completed_request.is_some()
        || payload.completed_request_completion.is_some()
        || !payload.content_receipts.is_empty()
        || !payload.required_replica_node_ids.is_empty()
        || payload.metadata_effects.is_empty()
    {
        return false;
    }
    let rows = &unit.projected_rows;
    if payload.expected_object_versions.iter().any(|expected| {
        rows.objects
            .iter()
            .find(|object| object.object_id == expected.object_id)
            .map(|object| &object.version)
            != expected.expected_version.as_ref()
    }) || payload.expected_path_occupants.iter().any(|expected| {
        rows.paths
            .iter()
            .find(|path| {
                path.file_system_id == payload.file_system_id
                    && path.path.trim_matches('/') == expected.path.trim_matches('/')
                    && path.kind != DfsObjectKind::Tombstone
            })
            .map(|path| &path.object_id)
            != expected.expected_object_id.as_ref()
    }) {
        return false;
    }
    payload.metadata_effects.iter().all(|effect| {
        let DfsPreparedMetadataEffect::PutObject { object } = effect else {
            return false;
        };
        rows.objects.iter().any(|before| {
            before.object_id == object.object_id
                && before.file_system_id == payload.file_system_id
                && object.file_system_id == before.file_system_id
                && object.path == before.path
                && object.kind == before.kind
                && object.version == before.version
                && object.link_target_object_id == before.link_target_object_id
                && object.content_hash == before.content_hash
                && object.content_size_bytes == before.content_size_bytes
                && object.owner_node_id == before.owner_node_id
                && object.current_locations == before.current_locations
                && object.policy == before.policy
                && object.tombstone_provenance.is_none()
                && before.tombstone_provenance.is_none()
                && !matches!(
                    object.kind,
                    DfsObjectKind::Hardlink | DfsObjectKind::Tombstone
                )
                && object.namespace_link_count.is_some()
                && object.namespace_link_count == before.namespace_link_count
        })
    })
}

impl PreparedRecovery {
    pub(super) fn expected_binding(&self, unit: &CapturedMetadataUnit) -> Result<String> {
        fingerprint(&(
            "exact-v7-recovery-v1",
            &unit.capture_fingerprint,
            &self.records,
        ))
    }
    pub(super) fn validate(&self, unit: &CapturedMetadataUnit) -> Result<()> {
        if self.version != 1
            || self.records.is_empty()
            || self.binding != self.expected_binding(unit)?
        {
            return Err(error(
                "V7 recovery evidence differs from its original captured source",
            ));
        }
        let mut ids = std::collections::BTreeSet::new();
        for record in &self.records {
            record.validate()?;
            if !ids.insert(&record.digest.operation_id)
                || !supported(unit, record)
                || !unit
                    .unresolved
                    .iter()
                    .any(|work| work_matches(work, record))
                || unit
                    .proven_v7_terminals
                    .iter()
                    .any(|terminal| terminal.digest.operation_id == record.digest.operation_id)
                || unit
                    .projected_rows
                    .prepared_operations_v7
                    .iter()
                    .any(|original| {
                        original.digest.operation_id == record.digest.operation_id
                            && original != record
                    })
                || !unit.original_outcomes.iter().any(|original| {
                    original.family == "prepared-operation-v7"
                        && original.original_status == "prepared"
                        && original.original_id == record.digest.operation_id
                        && original.original_digest.as_ref() == Some(&record.digest.payload_digest)
                }) && !unit.projected_rows.prepared_operations_v7.contains(record)
            {
                return Err(error(
                    "V7 recovery evidence changes an original reservation or requires unsupported family recovery",
                ));
            }
        }
        Ok(())
    }
}

impl CapturedMetadataUnit {
    pub(crate) fn recovery_preparations(&self) -> &[DfsPreparedOperationRecord] {
        self.recovery
            .as_ref()
            .map_or(&[], |recovery| recovery.records.as_slice())
    }
    pub(crate) fn has_unresolved_work(&self) -> bool {
        self.unresolved.iter().any(|work| {
            !self
                .recovery_preparations()
                .iter()
                .any(|record| work_matches(work, record))
        })
    }
    /// [`Self::has_unresolved_work`] without the captured rows' own
    /// non-Completed namespace jobs, Pending requests and staging intents:
    /// those are recorded rows a reader can report as they stand. What
    /// remains has no known effect on the rows yet.
    pub(crate) fn has_unresolved_work_beside_recorded_rows(&self) -> bool {
        let rows = &self.projected_rows;
        self.unresolved.iter().any(|work| {
            let recorded = match work.family.as_str() {
                "namespace_jobs" => rows.namespace_jobs.iter().any(|job| {
                    job.job_id == work.original_id
                        && job.status != sinomo_dfs::DfsNamespaceJobStatus::Completed
                }),
                "namespace_requests" => rows.namespace_requests.iter().any(|request| {
                    request.request_id == work.original_id
                        && request.status == sinomo_dfs::DfsNamespaceRequestStatus::Pending
                }),
                "intents" => rows.intents.iter().any(|intent| {
                    intent.intent_id == work.original_id
                        && matches!(
                            intent.status,
                            sinomo_dfs::DfsWriteIntentStatus::Staging
                                | sinomo_dfs::DfsWriteIntentStatus::ReceiptsPending
                        )
                }),
                _ => false,
            };
            !recorded
                && !self
                    .recovery_preparations()
                    .iter()
                    .any(|record| work_matches(work, record))
        })
    }
    /// [`Self::has_unresolved_work`] without the captured rows' own
    /// non-Completed namespace jobs: a converting shard settles each of
    /// those from its rows, keeps it for another shard's job, or stops.
    pub(crate) fn has_unresolved_work_beside_recorded_jobs(&self) -> bool {
        let rows = &self.projected_rows;
        self.unresolved.iter().any(|work| {
            let recorded = work.family == "namespace_jobs"
                && rows.namespace_jobs.iter().any(|job| {
                    job.job_id == work.original_id
                        && job.status != sinomo_dfs::DfsNamespaceJobStatus::Completed
                });
            !recorded
                && !self
                    .recovery_preparations()
                    .iter()
                    .any(|record| work_matches(work, record))
        })
    }
    pub(crate) fn validate_recovery_counts(&self, counted: &[DfsObjectMetadata]) -> Result<()> {
        for record in self.recovery_preparations() {
            for effect in &record.metadata_effects {
                let DfsPreparedMetadataEffect::PutObject { object } = effect else {
                    return Err(error("unsupported inherited V7 effect"));
                };
                if !counted.iter().any(|current| {
                    current.object_id == object.object_id
                        && current.file_system_id == object.file_system_id
                        && current.namespace_link_count == object.namespace_link_count
                }) {
                    return Err(error(
                        "original V7 recovery would change the qualified namespace count",
                    ));
                }
            }
        }
        Ok(())
    }
    /// Loads only bound DATA; the actual source owner replays and compares it
    /// before enrollment. Missing evidence leaves the original work unresolved.
    pub(crate) fn load_recovery_evidence(&mut self, capture_path: &Path) -> Result<()> {
        if self.recovery.is_some() {
            return Err(error(
                "base capture contains unverified attached recovery evidence",
            ));
        }
        let path = capture_path
            .parent()
            .ok_or_else(|| error("capture has no owner directory"))?
            .join(CHECKPOINT);
        match sinomo_core::storage::durable_file::read_object_file_bytes(&path, 64 * 1024 * 1024) {
            Ok(bytes) => {
                self.recovery = Some(serde_json::from_slice(&bytes).map_err(error)?);
                self.validate()
            }
            Err(cause) if cause.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(cause) => Err(error(cause)),
        }
    }
    pub(super) fn bind_replayed_recovery(
        &mut self,
        operations: &[Value],
        stage: &Path,
    ) -> Result<()> {
        let path = stage.join(CHECKPOINT);
        if !self
            .unresolved
            .iter()
            .any(|work| work.family == "prepared-operation-v7")
        {
            if path.exists() {
                return Err(error("recovery evidence has no original V7 reservation"));
            }
            return Ok(());
        }
        let mut records = BTreeMap::new();
        let mut retain = |record: DfsPreparedOperationRecord| -> Result<()> {
            record.validate()?;
            if supported(self, &record) {
                if records
                    .get(&record.digest.operation_id)
                    .is_some_and(|original| original != &record)
                {
                    return Err(error(
                        "original V7 replay has conflicting reservation bodies",
                    ));
                }
                records.insert(record.digest.operation_id.clone(), record);
            }
            Ok(())
        };
        for operation in operations {
            if operation["source_protocol_family"] != "prepared-operation-v7"
                || operation["status"] != "prepared"
                || operation["source_v7_payload_exact"] != true
            {
                continue;
            }
            retain(DfsPreparedOperationRecord {
                digest: serde_json::from_value(operation["source_v7_exact_digest"].clone())
                    .map_err(error)?,
                payload: Arc::new(
                    serde_json::from_value(operation["payload"].clone()).map_err(error)?,
                ),
                status: DfsPreparedOperationStatus::Prepared,
                committed_entry: serde_json::from_value(
                    operation["source_v7_commit_reference"].clone(),
                )
                .map_err(error)?,
            })?;
        }
        for record in &self.projected_rows.prepared_operations_v7 {
            if record.status == DfsPreparedOperationStatus::Prepared {
                retain(record.clone())?;
            }
        }
        if records.is_empty() {
            if path.exists() {
                return Err(error(
                    "retained V7 recovery evidence lost its exact replayed reservations",
                ));
            }
            return Ok(());
        }
        let mut recovery = PreparedRecovery {
            version: 1,
            records: records.into_values().collect(),
            binding: String::new(),
        };
        recovery.binding = recovery.expected_binding(self)?;
        recovery.validate(self)?;
        write_count_cohort_checkpoint(&path, &recovery)?;
        self.recovery = Some(recovery);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::{DfsObjectPolicy, node::NodeId};
    use sinomo_dfs::{
        DfsObjectId, DfsPreparedOperationDigest, DfsPreparedOperationPayload,
        DfsPreparedOperationScope, DfsVersion,
    };

    // Comparison DATA only. Actual native source, activation and publication
    // are covered by the provider lifecycle interruption/restart controls.
    fn model(
        root: &Path,
        change: impl FnOnce(&mut DfsPreparedOperationPayload),
    ) -> (CapturedMetadataUnit, Value, DfsPreparedOperationRecord) {
        let node = NodeId::from_legacy_named("recovery-data-owner").unwrap();
        let fs = sinomo_dfs::sifs_file_system_id();
        let object = DfsObjectMetadata {
            object_id: DfsObjectId::from_legacy_named("recovery-data-object").unwrap(),
            file_system_id: fs.clone(),
            path: "/object".into(),
            kind: DfsObjectKind::File,
            namespace_link_count: Some(1),
            tombstone_provenance: None,
            owner_node_id: node.clone(),
            current_locations: Vec::new(),
            content_hash: None,
            content_size_bytes: None,
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version: DfsVersion::from_legacy_named("recovery-data-version").unwrap(),
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        };
        let mut changed = object.clone();
        changed.accessed_at_unix_nanos = 1;
        let mut payload = DfsPreparedOperationPayload::for_metadata_effects(
            DfsPreparedOperationScope {
                file_system_id: fs.clone(),
                shard_id: DfsMetadataShardId::new("root"),
                owner_epoch: 1,
                operation_id: "original-v7-metadata".into(),
            },
            Vec::new(),
            Vec::new(),
            vec![DfsPreparedMetadataEffect::PutObject { object: changed }],
            time::OffsetDateTime::UNIX_EPOCH,
        )
        .unwrap();
        change(&mut payload);
        let record = DfsPreparedOperationRecord {
            digest: DfsPreparedOperationDigest::at_sequence(&payload, 7).unwrap(),
            payload: Arc::new(payload),
            status: DfsPreparedOperationStatus::Prepared,
            committed_entry: None,
        };
        let operation = serde_json::json!({
            "payload": record.payload, "status": "prepared", "source_protocol_family": "prepared-operation-v7",
            "source_operation_digest": record.digest.payload_digest, "source_v7_exact_digest": record.digest,
            "source_v7_payload_exact": true, "source_v7_commit_reference": null,
        });
        let rows = DfsMetadataTableSnapshotTables {
            objects: vec![object],
            ..Default::default()
        };
        let (outcomes, terminals, unresolved) =
            collect_original_outcomes(&rows, &[operation.clone()], Vec::new()).unwrap();
        let source = CapturedMetadataSource {
            retained_placement_reservations: Vec::new(),
            actual_source_root: root.join("source"),
            source_identity: "original-data".into(),
            source_format: Some(12),
            source_snapshot_fingerprint: "original-snapshot".into(),
            exact_prefix: None,
            source_journal_sequence: 7,
            source_state_fingerprint: "original-state".into(),
            file_system_ids: vec![fs.clone()],
            physical_shard: DfsMetadataShardId::new("root"),
            projection_assignments: vec![DfsMetadataShardConfig {
                file_system_id: fs,
                shard_id: "root".into(),
                primary_node_id: node,
                replica_node_ids: Vec::new(),
                epoch: 1,
                assignment: sinomo_dfs::DfsMetadataShardAssignment::Root,
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            }],
            original_handoff: None,
            evidence_root: root.to_owned(),
            planned_target_root: root.join("candidate"),
            event_ledger_cut: Default::default(),
        };
        (
            CapturedMetadataUnit::bind_projection(source, rows, outcomes, terminals, unresolved)
                .unwrap(),
            operation,
            record,
        )
    }

    #[test]
    fn exact_reservation_enriches_wire_without_rewriting_the_old_capture() {
        let dir = sinomo_testdir::TestDir::new("v7-recovery-evidence");
        let (mut unit, operation, record) = model(dir.path(), |_| {});
        let base = dir.join("captured-unit.json");
        write_count_cohort_checkpoint(&base, &unit).unwrap();
        let bytes = fs::read(&base).unwrap();
        let original_fingerprint = unit.projection_fingerprint().to_owned();
        assert!(unit.has_unresolved_work());
        unit.bind_replayed_recovery(&[operation.clone()], dir.path())
            .unwrap();
        assert!(!unit.has_unresolved_work());
        assert_eq!(unit.recovery_preparations(), std::slice::from_ref(&record));
        assert_ne!(unit.projection_fingerprint(), original_fingerprint);
        assert_eq!(fs::read(&base).unwrap(), bytes);
        let recovery = fs::read(dir.join(CHECKPOINT)).unwrap();
        let mut old: CapturedMetadataUnit = serde_json::from_slice(&bytes).unwrap();
        assert!(
            old.has_unresolved_work(),
            "a reader without the exception still blocks"
        );
        old.load_recovery_evidence(&base).unwrap();
        assert_eq!(old.projection_fingerprint(), unit.projection_fingerprint());
        let wire = encode_captured_unit(&unit, 64 * 1024).unwrap();
        let decoded = decode_captured_unit(&wire, 64 * 1024, 32).unwrap();
        assert_eq!(
            decoded.recovery_preparations(),
            std::slice::from_ref(&record)
        );
        assert!(decode_captured_unit(&wire, wire.len() - 1, 32).is_err());
        assert!(
            decode_captured_unit(&wire, 64 * 1024, 3).is_err(),
            "recovery records count toward the bound"
        );
        fs::rename(dir.join(CHECKPOINT), dir.join("saved-recovery.json")).unwrap();
        let mut resumed: CapturedMetadataUnit = serde_json::from_slice(&bytes).unwrap();
        resumed.load_recovery_evidence(&base).unwrap();
        assert!(resumed.has_unresolved_work());
        resumed
            .bind_replayed_recovery(&[operation], dir.path())
            .unwrap();
        assert_eq!(fs::read(dir.join(CHECKPOINT)).unwrap(), recovery);
        assert_eq!(fs::read(base).unwrap(), bytes);
    }

    #[test]
    fn changed_or_foreign_recovery_evidence_cannot_replace_the_original() {
        let dir = sinomo_testdir::TestDir::new("v7-recovery-tamper");
        let (mut unit, operation, _) = model(dir.path(), |_| {});
        unit.bind_replayed_recovery(&[operation.clone()], dir.path())
            .unwrap();
        let mut changed = unit.clone();
        Arc::make_mut(&mut changed.recovery.as_mut().unwrap().records[0].payload)
            .operation_id
            .push_str("-other");
        assert!(changed.validate().is_err());
        let record = &mut changed.recovery.as_mut().unwrap().records[0];
        record.digest = DfsPreparedOperationDigest::at_sequence(
            &record.payload,
            record.digest.prepare_sequence,
        )
        .unwrap();
        let binding = changed
            .recovery
            .as_ref()
            .unwrap()
            .expected_binding(&changed)
            .unwrap();
        changed.recovery.as_mut().unwrap().binding = binding;
        assert!(
            changed.validate().is_err(),
            "rehashing cannot change original capture references"
        );
        let mut foreign = unit.clone();
        foreign.source.physical_shard = DfsMetadataShardId::new("users");
        assert!(foreign.validate().is_err());
        let original = fs::read(dir.join(CHECKPOINT)).unwrap();
        fs::write(dir.join("original-recovery-evidence.json"), &original).unwrap();
        let mut wire: Value = serde_json::from_slice(&original).unwrap();
        wire["records"][0]["digest"]["prepare_sequence"] = serde_json::json!(8);
        fs::write(dir.join(CHECKPOINT), serde_json::to_vec(&wire).unwrap()).unwrap();
        let mut plain = unit.clone();
        plain.recovery = None;
        assert!(
            plain
                .load_recovery_evidence(&dir.join("captured-unit.json"))
                .is_err()
        );
        // Unsigned DATA can be internally consistent after a changed slot is
        // rehashed. Only the original fenced replay may qualify that slot.
        let mut changed_slot = unit.clone();
        changed_slot.recovery.as_mut().unwrap().records[0]
            .digest
            .prepare_sequence = 6;
        let binding = changed_slot
            .recovery
            .as_ref()
            .unwrap()
            .expected_binding(&changed_slot)
            .unwrap();
        changed_slot.recovery.as_mut().unwrap().binding = binding;
        changed_slot.validate().unwrap();
        let changed_bytes = serde_json::to_vec(changed_slot.recovery.as_ref().unwrap()).unwrap();
        fs::write(dir.join(CHECKPOINT), &changed_bytes).unwrap();
        let mut replayed = unit.clone();
        replayed.recovery = None;
        assert!(
            replayed
                .bind_replayed_recovery(&[operation], dir.path())
                .is_err()
        );
        assert_eq!(
            fs::read(dir.join(CHECKPOINT)).unwrap(),
            changed_bytes,
            "failed replay cannot rewrite an incompatible checkpoint"
        );
        fs::write(dir.join(CHECKPOINT), original).unwrap();
    }

    #[test]
    fn namespace_counts_parent_work_and_journal_cuts_remain_blocked() {
        for change in 0..7 {
            let dir = sinomo_testdir::TestDir::new("v7-unsupported-recovery");
            let (mut unit, operation, _) = model(dir.path(), |payload| {
                let DfsPreparedMetadataEffect::PutObject { object } =
                    &mut payload.metadata_effects[0]
                else {
                    unreachable!()
                };
                match change {
                    0 => object.namespace_link_count = Some(2),
                    1 => object.path = "/new-parent/object".into(),
                    2 => payload.transaction.objects.push(object.clone()),
                    3 => payload.expected_journal_sequence = Some(6),
                    4 => payload.owner_epoch = 2,
                    5 => object
                        .current_locations
                        .push(NodeId::from_legacy_named("unproven-content-holder").unwrap()),
                    _ => payload
                        .metadata_effects
                        .push(DfsPreparedMetadataEffect::PutFileSystem {
                            file_system: sinomo_dfs::DfsFileSystem::sifs(
                                NodeId::from_legacy_named("foreign-family").unwrap(),
                            ),
                        }),
                }
            });
            unit.bind_replayed_recovery(&[operation], dir.path())
                .unwrap();
            assert!(unit.has_unresolved_work(), "unsupported change {change}");
            assert!(unit.recovery_preparations().is_empty());
            assert!(!dir.join(CHECKPOINT).exists());
        }
        let dir = sinomo_testdir::TestDir::new("v7-recovery-census-count");
        let (mut unit, operation, _) = model(dir.path(), |_| {});
        unit.bind_replayed_recovery(&[operation], dir.path())
            .unwrap();
        let mut counted = unit.projected_rows.objects.clone();
        counted[0].namespace_link_count = Some(2);
        assert!(unit.validate_recovery_counts(&counted).is_err());
    }

    #[test]
    fn a_normalized_old_body_never_becomes_a_current_v7_reservation() {
        let dir = sinomo_testdir::TestDir::new("v7-recovery-original-only");
        let (mut unit, mut operation, _) = model(dir.path(), |_| {});
        operation["source_v7_payload_exact"] = Value::Bool(false);
        unit.bind_replayed_recovery(&[operation], dir.path())
            .unwrap();
        assert!(unit.has_unresolved_work());
        assert!(unit.recovery_preparations().is_empty());
    }
}
