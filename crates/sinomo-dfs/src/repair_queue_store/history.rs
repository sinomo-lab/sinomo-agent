//! Selected non-executing history observations in the existing queue log.
use super::*;
use crate::{
    DfsImportedRepairHistoryRef, DfsOriginalRepairHistoryCarrier, DfsOriginalRepairHistoryQueryRef,
    DfsOriginalRepairHistoryRequest, DfsOriginalRepairHistoryResponse,
    DfsOriginalRepairHistoryResult, DfsRoutedMessage,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ImportedRepairHistoryQuery {
    request: DfsOriginalRepairHistoryRequest,
    delivery: Vec<(u64, Option<sinomo_core::network::NetworkDeliveryState>)>,
    observed: Option<(u64, DfsRoutedMessage)>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ImportedRepairHistorySnapshot {
    import_ref: DfsImportedRepairHistoryRef,
    assignment: crate::DfsMetadataShardConfig,
    carrier: DfsOriginalRepairHistoryCarrier,
    query_ref: DfsOriginalRepairHistoryQueryRef,
    delivery: Vec<(u64, Option<sinomo_core::network::NetworkDeliveryState>)>,
    observed: Option<(u64, DfsRoutedMessage)>,
}

impl ImportedRepairHistoryQuery {
    fn retained_sequences(&self) -> Vec<u64> {
        std::iter::once(self.request.query_ref.attempt_sequence)
            .chain(self.delivery.iter().map(|(sequence, _)| *sequence))
            .chain(self.observed.iter().map(|(sequence, _)| *sequence))
            .collect()
    }
    pub(super) fn snapshot(&self) -> ImportedRepairHistorySnapshot {
        ImportedRepairHistorySnapshot {
            import_ref: self.request.import_ref.clone(),
            assignment: self.request.current_assignment.clone(),
            carrier: self.request.carrier.clone(),
            query_ref: self.request.query_ref.clone(),
            delivery: self.delivery.clone(),
            observed: self.observed.clone(),
        }
    }
    pub(crate) fn request(&self) -> &DfsOriginalRepairHistoryRequest {
        &self.request
    }
    pub(crate) fn response(&self) -> Option<&DfsOriginalRepairHistoryResponse> {
        self.observed
            .as_ref()
            .and_then(|(_, routed)| match &routed.message {
                crate::DfsNetworkMessage::OriginalRepairHistoryResponse(response) => Some(response),
                _ => None,
            })
    }
    #[cfg(test)]
    pub(crate) fn returned_for_test(&self) -> Option<&DfsRoutedMessage> {
        self.observed.as_ref().map(|(_, returned)| returned)
    }
    pub(crate) fn terminal(&self) -> Option<&DfsOriginalRepairHistoryResponse> {
        self.observed
            .as_ref()
            .and_then(|(_, routed)| match &routed.message {
                crate::DfsNetworkMessage::OriginalRepairHistoryResponse(response)
                    if matches!(
                        &response.result,
                        DfsOriginalRepairHistoryResult::HistoricalTerminal { .. }
                    ) =>
                {
                    Some(response)
                }
                _ => None,
            })
    }
}

impl ImportedRepairOriginalData {
    pub(crate) fn history_ref(&self) -> Result<DfsImportedRepairHistoryRef, DfsError> {
        self.material.validate()?;
        Ok(DfsImportedRepairHistoryRef {
            queue_incarnation: self.installer_queue_incarnation.clone(),
            imported_at_sequence: self.installed_at_sequence,
            captured_birth_digest: self.material.birth.captured_data_digest()?,
            original_command_digest: self.material.birth.root.original_command_digest.clone(),
            cut: self.material.cut.clone(),
        })
    }
}

impl RepairCustodyIndex {
    fn history_import(
        &self,
        reference: &DfsImportedRepairHistoryRef,
    ) -> Result<&ImportedRepairOriginalData, DfsError> {
        let selected = reference
            .cut
            .selected_original_data
            .as_ref()
            .ok_or_else(|| custody_error("history query has no selected original"))?;
        let imported = self
            .imported_original_data
            .get(&(selected.producer.to_string(), selected.request_id.clone()))
            .ok_or_else(|| custody_error("history query has no installed original DATA"))?;
        if imported.history_ref()? != *reference {
            return Err(custody_error(
                "history query changed its exact import reference",
            ));
        }
        Ok(imported)
    }

    fn history_request(
        &self,
        reference: &DfsImportedRepairHistoryRef,
        assignment: &crate::DfsMetadataShardConfig,
        carrier: &DfsOriginalRepairHistoryCarrier,
        sequence: u64,
    ) -> Result<DfsOriginalRepairHistoryRequest, DfsError> {
        let imported = self.history_import(reference)?;
        let request = DfsOriginalRepairHistoryRequest {
            version: 1,
            query_ref: DfsOriginalRepairHistoryQueryRef {
                queue_incarnation: self.incarnation.clone(),
                attempt_sequence: sequence,
            },
            import_ref: reference.clone(),
            original_material: imported.material.birth.original_wire_material()?,
            current_assignment: assignment.clone(),
            carrier: carrier.clone(),
        };
        request.validate()?;
        Ok(request)
    }

    fn validate_history_return(
        query: &ImportedRepairHistoryQuery,
        returned: &DfsRoutedMessage,
    ) -> Result<(), DfsError> {
        let request = &query.request;
        let crate::DfsNetworkMessage::OriginalRepairHistoryResponse(response) = &returned.message
        else {
            return Err(custody_error(
                "history observation cannot erase its query purpose",
            ));
        };
        if returned.source_node_id != request.carrier.target
            || returned.target_node_id != request.carrier.source
        {
            return Err(custody_error(
                "history observation has another authenticated carrier",
            ));
        }
        response.validate_for_request(request)?;
        if let Some(prior) = query.terminal() {
            if prior != response {
                return Err(custody_error(
                    "history observation cannot replace an exact saved Terminal",
                ));
            }
        }
        Ok(())
    }

    pub(super) fn validate_history_change(
        &self,
        change: &RepairCustodyChange,
    ) -> Result<(), DfsError> {
        match change {
            RepairCustodyChange::BeginImportedHistoryQuery {
                import_ref,
                assignment,
                carrier,
            } => {
                let imported = self.history_import(import_ref)?;
                import_ref.validate_original_carrier(
                    &imported.material.birth.original_wire_material()?,
                    assignment,
                    carrier,
                )?;
                if let Some(prior) = self
                    .imported_history_slots
                    .get(&imported.material.wire_key())
                {
                    let prior = &self.imported_history_queries[prior].request;
                    if &prior.import_ref != import_ref
                        || &prior.current_assignment != assignment
                        || &prior.carrier != carrier
                    {
                        return Err(custody_error(
                            "history query slot cannot change its retained attempt",
                        ));
                    }
                }
            }
            RepairCustodyChange::ObserveImportedHistoryQueryDelivery {
                query_ref, carrier, ..
            } => {
                let query = self
                    .imported_history_queries
                    .get(query_ref)
                    .ok_or_else(|| custody_error("query delivery has no synced exact attempt"))?;
                if query.request.carrier != *carrier {
                    return Err(custody_error("query delivery changed its carrier"));
                }
            }
            RepairCustodyChange::ObserveImportedHistoryQueryResult {
                query_ref,
                returned,
            } => {
                let query = self
                    .imported_history_queries
                    .get(query_ref)
                    .ok_or_else(|| custody_error("query result has no synced exact attempt"))?;
                Self::validate_history_return(query, returned)?;
            }
            _ => return Err(custody_error("not a history transition")),
        }
        Ok(())
    }

    pub(super) fn validate_history_sequence(
        &self,
        change: &RepairCustodyChange,
        sequence: u64,
    ) -> Result<(), DfsError> {
        if self.history_sequences.contains_key(&sequence) {
            return Err(custody_error(
                "query phase reuses a retained import/query frame",
            ));
        }
        if let RepairCustodyChange::ObserveImportedHistoryQueryDelivery { query_ref, .. }
        | RepairCustodyChange::ObserveImportedHistoryQueryResult { query_ref, .. } = change
        {
            let query = self
                .imported_history_queries
                .get(query_ref)
                .ok_or_else(|| custody_error("query phase has no attempt"))?;
            if sequence <= query.request.query_ref.attempt_sequence {
                return Err(custody_error(
                    "query observation must follow its actual attempt frame",
                ));
            }
        }
        if let RepairCustodyChange::BeginImportedHistoryQuery {
            import_ref,
            assignment,
            carrier,
        } = change
        {
            self.history_request(import_ref, assignment, carrier, sequence)?;
        }
        Ok(())
    }

    pub(super) fn apply_history_change(
        &mut self,
        change: &RepairCustodyChange,
        sequence: u64,
    ) -> Result<(), DfsError> {
        self.validate_history_sequence(change, sequence)?;
        match change {
            RepairCustodyChange::BeginImportedHistoryQuery {
                import_ref,
                assignment,
                carrier,
            } => {
                let key = self.history_import(import_ref)?.material.wire_key();
                if !self.imported_history_slots.contains_key(&key) {
                    let request =
                        self.history_request(import_ref, assignment, carrier, sequence)?;
                    if self
                        .imported_history_queries
                        .contains_key(&request.query_ref)
                    {
                        return Err(custody_error(
                            "history query append reference already belongs to another origin",
                        ));
                    }
                    self.imported_history_slots
                        .insert(key, request.query_ref.clone());
                    self.history_sequences
                        .insert(sequence, Some(request.query_ref.clone()));
                    self.imported_history_queries.insert(
                        request.query_ref.clone(),
                        ImportedRepairHistoryQuery {
                            request,
                            delivery: Vec::new(),
                            observed: None,
                        },
                    );
                }
            }
            RepairCustodyChange::ObserveImportedHistoryQueryDelivery {
                query_ref,
                delivery,
                ..
            } => {
                let query = self
                    .imported_history_queries
                    .get_mut(query_ref)
                    .ok_or_else(|| custody_error("missing query attempt"))?;
                if !query
                    .delivery
                    .iter()
                    .any(|(_, previous)| previous == delivery)
                {
                    query.delivery.push((sequence, *delivery));
                    self.history_sequences
                        .insert(sequence, Some(query_ref.clone()));
                }
            }
            RepairCustodyChange::ObserveImportedHistoryQueryResult {
                query_ref,
                returned,
            } => {
                let query = self
                    .imported_history_queries
                    .get_mut(query_ref)
                    .ok_or_else(|| custody_error("missing query attempt"))?;
                let equal = query
                    .observed
                    .as_ref()
                    .is_some_and(|(_, prior)| prior.message == returned.message);
                if !equal {
                    let previous = query.observed.replace((sequence, returned.clone()));
                    // Only the currently retained observation is represented
                    // in snapshots. The ordinary physical head still prevents
                    // replaying/reusing superseded Pending frames.
                    if let Some((previous_sequence, _)) = previous {
                        self.history_sequences.remove(&previous_sequence);
                    }
                    self.history_sequences
                        .insert(sequence, Some(query_ref.clone()));
                }
            }
            _ => return Err(custody_error("not a history transition")),
        }
        Ok(())
    }

    pub(super) fn restore_history_query(
        &mut self,
        snapshot: ImportedRepairHistorySnapshot,
        floor: u64,
    ) -> Result<(), DfsError> {
        let request = self.history_request(
            &snapshot.import_ref,
            &snapshot.assignment,
            &snapshot.carrier,
            snapshot.query_ref.attempt_sequence,
        )?;
        if request.query_ref != snapshot.query_ref {
            return Err(custody_error(
                "history snapshot changed its local queue incarnation",
            ));
        }
        let query = ImportedRepairHistoryQuery {
            request,
            delivery: snapshot.delivery,
            observed: snapshot.observed,
        };
        let request = &query.request;
        let imported = self.history_import(&request.import_ref)?;
        let key = imported.material.wire_key();
        if request
            != &self.history_request(
                &request.import_ref,
                &request.current_assignment,
                &request.carrier,
                request.query_ref.attempt_sequence,
            )?
            || request.query_ref.attempt_sequence > floor
            || query
                .delivery
                .iter()
                .any(|(seq, _)| *seq <= request.query_ref.attempt_sequence || *seq > floor)
            || query.delivery.windows(2).any(|pair| pair[0].0 >= pair[1].0)
            || query
                .observed
                .as_ref()
                .is_some_and(|(seq, _)| *seq <= request.query_ref.attempt_sequence || *seq > floor)
            || self.imported_history_slots.contains_key(&key)
            || self
                .imported_history_queries
                .contains_key(&request.query_ref)
        {
            return Err(custody_error(
                "history query snapshot changed its exact physical prefix",
            ));
        }
        if let Some((_, returned)) = &query.observed {
            Self::validate_history_return(&query, returned)?;
        }
        let claims = query.retained_sequences();
        let mut unique = std::collections::BTreeSet::new();
        if claims.iter().any(|sequence| {
            !unique.insert(*sequence) || self.history_sequences.contains_key(sequence)
        }) {
            return Err(custody_error(
                "history snapshot shares a retained import/query phase frame",
            ));
        }
        for sequence in claims {
            self.history_sequences
                .insert(sequence, Some(request.query_ref.clone()));
        }
        self.imported_history_slots
            .insert(key, request.query_ref.clone());
        self.imported_history_queries
            .insert(request.query_ref.clone(), query);
        Ok(())
    }
}

/// The existing writer permits one custody transition per frame. Preserve V1
/// replay behavior, but never reinterpret a query-bearing multi-transition
/// frame as multiple phases at the same physical sequence.
pub(super) fn validate_history_frame(
    operations: &[DfsRepairQueueOperation],
) -> Result<(), DfsError> {
    let mut count = 0;
    let mut history = false;
    for operation in operations {
        if let DfsRepairQueueOperation::RepairCustody { change } = operation {
            count += 1;
            history |= matches!(
                change.as_ref(),
                RepairCustodyChange::BeginImportedHistoryQuery { .. }
                    | RepairCustodyChange::ObserveImportedHistoryQueryDelivery { .. }
                    | RepairCustodyChange::ObserveImportedHistoryQueryResult { .. }
            );
        }
    }
    if history && count > 1 {
        return Err(custody_error(
            "query-bearing frame requires one indivisible custody transition",
        ));
    }
    Ok(())
}

impl LocalDfsRepairQueueStore {
    #[cfg(test)]
    pub(crate) fn imported_history_query_for_test(
        &self,
        producer: &NodeId,
        request_id: &str,
    ) -> Result<Option<ImportedRepairHistoryQuery>, DfsError> {
        let mut state = self.lock_state()?;
        state.refresh_from_disk()?;
        Ok(state
            .custody
            .imported_history_slots
            .get(&(producer.to_string(), request_id.to_owned()))
            .and_then(|reference| state.custody.imported_history_queries.get(reference))
            .cloned())
    }
    /// Existing single-item writer, never a retry queue or executor enrollment.
    pub(crate) fn begin_imported_history_query(
        &self,
        imported: &ImportedRepairOriginalData,
        assignment: &crate::DfsMetadataShardConfig,
        deadline: std::time::Instant,
        make_carrier: impl FnOnce() -> Result<DfsOriginalRepairHistoryCarrier, DfsError>,
    ) -> Result<ImportedRepairHistoryQuery, DfsError> {
        let identity = self
            .selected_fixture
            .as_ref()
            .ok_or_else(|| custody_error("history query requires its selected physical queue"))?;
        self.validate_selected_original_fixture(identity)?;
        let mut state = self.lock_state()?;
        state.with_writer_lock(|state| {
            state.refresh_from_disk()?;
            history_deadline(deadline)?;
            if state
                .custody
                .imported_original_data
                .get(&imported.material.wire_key())
                != Some(imported)
            {
                return Err(custody_error("history original changed before its attempt"));
            }
            if let Some(reference) = state
                .custody
                .imported_history_slots
                .get(&imported.material.wire_key())
                .cloned()
            {
                let prior = state.custody.imported_history_queries[&reference].clone();
                if prior.request.import_ref != imported.history_ref()?
                    || prior.request.current_assignment != *assignment
                {
                    return Err(custody_error(
                        "history query replay changed its import/current assignment",
                    ));
                }
                history_deadline(deadline)?;
                state.sync_selected_import(identity.root())?;
                history_deadline(deadline)?;
                return Ok(prior);
            }
            state.append_operations_held(vec![DfsRepairQueueOperation::RepairCustody {
                change: Box::new(RepairCustodyChange::BeginImportedHistoryQuery {
                    import_ref: imported.history_ref()?,
                    assignment: assignment.clone(),
                    carrier: make_carrier()?,
                }),
            }])?;
            history_deadline(deadline)?;
            state.sync_selected_import(identity.root())?;
            history_deadline(deadline)?;
            let reference = &state.custody.imported_history_slots[&imported.material.wire_key()];
            Ok(state.custody.imported_history_queries[reference].clone())
        })
    }

    pub(crate) fn observe_imported_history_delivery(
        &self,
        query: &ImportedRepairHistoryQuery,
        delivery: Option<sinomo_core::network::NetworkDeliveryState>,
        deadline: std::time::Instant,
    ) -> Result<(), DfsError> {
        self.append_history_observation(
            query,
            deadline,
            RepairCustodyChange::ObserveImportedHistoryQueryDelivery {
                query_ref: query.request.query_ref.clone(),
                carrier: query.request.carrier.clone(),
                delivery,
            },
        )
        .map(|_| ())
    }

    pub(crate) fn observe_imported_history_result(
        &self,
        query: &ImportedRepairHistoryQuery,
        returned: DfsRoutedMessage,
        deadline: std::time::Instant,
    ) -> Result<ImportedRepairHistoryQuery, DfsError> {
        self.append_history_observation(
            query,
            deadline,
            RepairCustodyChange::ObserveImportedHistoryQueryResult {
                query_ref: query.request.query_ref.clone(),
                returned,
            },
        )
    }

    fn append_history_observation(
        &self,
        query: &ImportedRepairHistoryQuery,
        deadline: std::time::Instant,
        change: RepairCustodyChange,
    ) -> Result<ImportedRepairHistoryQuery, DfsError> {
        let identity = self
            .selected_fixture
            .as_ref()
            .ok_or_else(|| custody_error("history observation requires selected queue"))?;
        self.validate_selected_original_fixture(identity)?;
        let mut state = self.lock_state()?;
        state.with_writer_lock(|state| {
            state.refresh_from_disk()?;
            history_deadline(deadline)?;
            let reference = &query.request.query_ref;
            let current = state
                .custody
                .imported_history_queries
                .get(reference)
                .ok_or_else(|| custody_error("history attempt disappeared"))?;
            if current.request != query.request {
                return Err(custody_error("history attempt changed before observation"));
            }
            state.custody.validate_history_change(&change)?;
            let duplicate = match &change {
                RepairCustodyChange::ObserveImportedHistoryQueryDelivery { delivery, .. } => {
                    current
                        .delivery
                        .iter()
                        .any(|(_, previous)| previous == delivery)
                }
                RepairCustodyChange::ObserveImportedHistoryQueryResult { returned, .. } => current
                    .observed
                    .as_ref()
                    .is_some_and(|(_, prior)| prior.message == returned.message),
                _ => false,
            };
            if !duplicate {
                state.append_operations_held(vec![DfsRepairQueueOperation::RepairCustody {
                    change: Box::new(change),
                }])?;
            }
            history_deadline(deadline)?;
            state.sync_selected_import(identity.root())?;
            history_deadline(deadline)?;
            Ok(state.custody.imported_history_queries[reference].clone())
        })
    }
}

fn history_deadline(deadline: std::time::Instant) -> Result<(), DfsError> {
    if std::time::Instant::now() >= deadline {
        return Err(custody_error(
            "history query deadline expired before queue acknowledgement",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // File-backed DATA only: real source custody/export and destination
    // writers, without asserting native metadata authority or receiver result.
    fn install_origin(
        root: &Path,
        queue: &LocalDfsRepairQueueStore,
        identity: &SelectedOriginalFixtureIdentity,
        suffix: &str,
    ) -> ImportedRepairOriginalData {
        let (source, content, mut original, attachment, _prior_payload, source_identity) =
            crate::repair_queue_store::tests::selected_original_fixture(
                &root.join(format!("source-{suffix}")),
            );
        let crate::DfsNetworkMessage::RepairReplica(command) = &mut original.message else {
            unreachable!()
        };
        command.request_id = format!("history-original-{suffix}");
        command.intent_id = format!("history-intent-{suffix}");
        let command = command.clone();
        let retained = content
            .capture_repair_payload(
                repair_subject(&source.custody_incarnation().unwrap(), &original).unwrap(),
                original_repair_digest(&original, &attachment).unwrap(),
                command.object_id.as_str(),
                command.version.as_str(),
                &command.content_hash,
                attachment.total_size_bytes,
            )
            .unwrap();
        source
            .enroll_original_repair(&original, &attachment, &retained)
            .unwrap();
        source.record_original_repair_attempt(&original).unwrap();
        let retiring = command.authority_shard.clone().unwrap();
        let assignment = crate::DfsMetadataShardConfig {
            primary_node_id: NodeId::from_legacy_named("history-installer").unwrap(),
            replica_node_ids: vec![
                original.source_node_id.clone(),
                original.target_node_id.clone(),
            ],
            epoch: retiring.epoch + 1,
            ..retiring.clone()
        };
        let handoff_id = format!("history-data-handoff-{suffix}");
        let handoff = crate::DfsMetadataShardMembershipHandoff {
            cause: crate::DfsMetadataShardHandoffCause::Membership,
            handoff_id: handoff_id.clone(),
            desired_shard: assignment,
            required_prepared_sequence: 17,
            required_head_watermark: None,
            retiring_primary_fence: Some(crate::DfsMetadataShardMembershipHandoffFence {
                handoff_id,
                primary_node_id: retiring.primary_node_id.clone(),
                journal_sequence: 17,
                state_fingerprint: "history-local-data-only".into(),
            }),
            retiring_shard: retiring,
        };
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let export = crate::repair_handoff::RepairHandoffExport::capture_selected_original(
            &source_identity,
            &handoff,
            &source,
            &command.request_id,
            |_| Ok(true),
            deadline,
        )
        .unwrap();
        for ordinal in 0..export.cut.page_count {
            crate::repair_handoff::receive_selected_original_data(
                identity,
                &export.page(ordinal).unwrap(),
                deadline,
            )
            .unwrap();
        }
        assert!(
            crate::repair_handoff::install_selected_original_data(
                identity,
                &export.cut,
                queue,
                deadline
            )
            .unwrap()
        );
        queue
            .installed_original_handoff_data(&original.source_node_id, &command.request_id)
            .unwrap()
            .unwrap()
    }

    fn begin(
        queue: &LocalDfsRepairQueueStore,
        imported: &ImportedRepairOriginalData,
    ) -> ImportedRepairHistoryQuery {
        let assignment = &imported.material.cut.handoff.desired_shard;
        queue
            .begin_imported_history_query(
                imported,
                assignment,
                std::time::Instant::now() + std::time::Duration::from_secs(5),
                || {
                    Ok(DfsOriginalRepairHistoryCarrier {
                        source: assignment.primary_node_id.clone(),
                        target: imported.material.birth.original.target_node_id.clone(),
                        frame_id: 31,
                        message_id: 32,
                    })
                },
            )
            .unwrap()
    }

    fn pending_return(request: &DfsOriginalRepairHistoryRequest) -> DfsRoutedMessage {
        DfsRoutedMessage {
            source_node_id: request.carrier.target.clone(),
            target_node_id: request.carrier.source.clone(),
            frame_id: 41,
            message_id: 42,
            message: crate::DfsNetworkMessage::OriginalRepairHistoryResponse(
                crate::DfsOriginalRepairHistoryResponse {
                    version: 1,
                    query_ref: request.query_ref.clone(),
                    import_ref: request.import_ref.clone(),
                    original_command_digest: request.original_material.command_digest.clone(),
                    query_carrier: request.carrier.clone(),
                    result: crate::DfsOriginalRepairHistoryResult::Pending {
                        reason: crate::DfsOriginalRepairHistoryPending::MissingTerminal,
                    },
                },
            ),
        }
    }

    #[test]
    fn history_snapshot_rejects_duplicate_query_refs_before_index_mutation() {
        let root = sinomo_testdir::TestDir::new("history-snapshot-ref-collision");
        let (queue, identity) =
            LocalDfsRepairQueueStore::new_selected_original_fixture(root.join("destination"))
                .unwrap();
        let a = install_origin(root.path(), &queue, &identity, "a");
        let b = install_origin(root.path(), &queue, &identity, "b");
        let first = begin(&queue, &a);
        let second = begin(&queue, &b);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        queue
            .observe_imported_history_delivery(&first, None, deadline)
            .unwrap();
        let first = queue
            .observe_imported_history_result(&first, pending_return(first.request()), deadline)
            .unwrap();
        queue
            .observe_imported_history_delivery(&second, None, deadline)
            .unwrap();
        let second = queue
            .observe_imported_history_result(&second, pending_return(second.request()), deadline)
            .unwrap();
        assert_ne!(first.request().query_ref, second.request().query_ref);
        assert_ne!(a.material.wire_key(), b.material.wire_key());
        queue
            .compact_if_needed(crate::DfsRepairQueueCompactionOptions::force())
            .unwrap();
        let persisted = read_repair_queue_snapshot(identity.snapshot_path()).unwrap();
        let floor = persisted.applied_through_sequence;
        let mut snapshot = persisted.custody.unwrap();
        let second_snapshot = snapshot.imported_history_queries.pop().unwrap();
        let first_snapshot = snapshot.imported_history_queries[0].clone();
        let mut index =
            RepairCustodyIndex::from_snapshot(Some(snapshot.clone()), floor, true).unwrap();
        let before = index.snapshot();
        let before_sequences = index.history_sequences.clone();
        let before_slots = index.imported_history_slots.clone();
        let mut collision = second_snapshot.clone();
        collision.query_ref = first_snapshot.query_ref.clone();
        assert!(
            collision.query_ref.attempt_sequence > collision.import_ref.imported_at_sequence,
            "the collision must survive the independent import-prefix check"
        );
        assert!(index.restore_history_query(collision, floor).is_err());
        assert_eq!(
            index.snapshot(),
            before,
            "neither query index can change on a duplicate physical append reference"
        );
        assert_eq!(index.history_sequences, before_sequences);
        assert_eq!(index.imported_history_slots, before_slots);
        assert!(
            index
                .apply_change(
                    &RepairCustodyChange::BeginImportedHistoryQuery {
                        import_ref: second.request().import_ref.clone(),
                        assignment: second.request().current_assignment.clone(),
                        carrier: second.request().carrier.clone(),
                    },
                    first.request().query_ref.attempt_sequence
                )
                .is_err()
        );
        assert_eq!(
            index.snapshot(),
            before,
            "the same collision also rejects before either live-append index mutation"
        );
        assert_eq!(index.history_sequences, before_sequences);
        assert_eq!(index.imported_history_slots, before_slots);
        for sequence in [second_snapshot.import_ref.imported_at_sequence, floor + 1] {
            let mut invalid = second_snapshot.clone();
            invalid.query_ref.attempt_sequence = sequence;
            assert!(index.restore_history_query(invalid, floor).is_err());
            assert_eq!(index.snapshot(), before);
            assert_eq!(index.history_sequences, before_sequences);
            assert_eq!(index.imported_history_slots, before_slots);
        }
        // One frame cannot own both the retained delivery and the result, or
        // a phase of another origin. These values all otherwise fit the floor.
        for fault in [
            "same-query-result-delivery",
            "cross-query-delivery",
            "cross-query-result",
            "cross-query-attempt",
        ] {
            let mut invalid = second_snapshot.clone();
            match fault {
                "same-query-result-delivery" => {
                    invalid.observed.as_mut().unwrap().0 = invalid.delivery[0].0
                }
                "cross-query-delivery" => invalid.delivery[0].0 = first_snapshot.delivery[0].0,
                "cross-query-result" => {
                    invalid.observed.as_mut().unwrap().0 =
                        first_snapshot.observed.as_ref().unwrap().0
                }
                "cross-query-attempt" => {
                    invalid.query_ref.attempt_sequence = first_snapshot.observed.as_ref().unwrap().0
                }
                _ => unreachable!(),
            }
            if let Some((_, returned)) = invalid.observed.as_mut() {
                let crate::DfsNetworkMessage::OriginalRepairHistoryResponse(response) =
                    &mut returned.message
                else {
                    unreachable!()
                };
                response.query_ref = invalid.query_ref.clone();
            }
            let error = index.restore_history_query(invalid, floor).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("shares a retained import/query phase frame"),
                "{fault}: {error}"
            );
            assert_eq!(index.snapshot(), before, "{fault}");
            assert_eq!(index.history_sequences, before_sequences, "{fault}");
            assert_eq!(index.imported_history_slots, before_slots, "{fault}");
        }
        let mut imports_only = snapshot.clone();
        imports_only.imported_history_queries.clear();
        let mut imports_index =
            RepairCustodyIndex::from_snapshot(Some(imports_only), floor, true).unwrap();
        let imports_before = imports_index.snapshot();
        let imports_sequences = imports_index.history_sequences.clone();
        let mut at_other_import = first_snapshot.clone();
        at_other_import.query_ref.attempt_sequence = b.installed_at_sequence;
        if let Some((_, returned)) = at_other_import.observed.as_mut() {
            let crate::DfsNetworkMessage::OriginalRepairHistoryResponse(response) =
                &mut returned.message
            else {
                unreachable!()
            };
            response.query_ref = at_other_import.query_ref.clone();
        }
        assert!(at_other_import.query_ref.attempt_sequence > a.installed_at_sequence);
        let error = imports_index
            .restore_history_query(at_other_import, floor)
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("shares a retained import/query phase frame"),
            "a query cannot impersonate another origin's import frame: {error}"
        );
        assert_eq!(imports_index.snapshot(), imports_before);
        assert_eq!(imports_index.history_sequences, imports_sequences);
        assert!(imports_index.imported_history_slots.is_empty());

        index
            .restore_history_query(second_snapshot.clone(), floor)
            .unwrap();
        let both = index.snapshot();
        let both_sequences = index.history_sequences.clone();
        for sequence in [
            1,
            second.request().query_ref.attempt_sequence,
            second_snapshot.delivery[0].0,
            first_snapshot.observed.as_ref().unwrap().0,
        ] {
            for change in [
                RepairCustodyChange::ObserveImportedHistoryQueryDelivery {
                    query_ref: second.request().query_ref.clone(),
                    carrier: second.request().carrier.clone(),
                    delivery: None,
                },
                RepairCustodyChange::ObserveImportedHistoryQueryResult {
                    query_ref: second.request().query_ref.clone(),
                    returned: pending_return(second.request()),
                },
            ] {
                assert!(index.apply_change(&change, sequence).is_err());
                assert_eq!(index.snapshot(), both);
                assert_eq!(index.history_sequences, both_sequences);
            }
        }
        assert_eq!(index.imported_history_queries.len(), 2);
        assert_eq!(index.imported_history_slots.len(), 2);
        assert_eq!(
            read_repair_queue_snapshot(identity.snapshot_path())
                .unwrap()
                .applied_through_sequence,
            floor
        );
        // A newer Pending disposition replaces the retained observation, not
        // the physical queue head. Compaction reconstructs only retained claims;
        // future frame allocation/replay still starts strictly above its floor.
        let old_sequence = first_snapshot.observed.as_ref().unwrap().0;
        let mut newer = pending_return(first.request());
        let crate::DfsNetworkMessage::OriginalRepairHistoryResponse(response) = &mut newer.message
        else {
            unreachable!()
        };
        response.result = crate::DfsOriginalRepairHistoryResult::Pending {
            reason: crate::DfsOriginalRepairHistoryPending::UnsupportedHistory,
        };
        let newer = queue
            .observe_imported_history_result(
                &first,
                newer,
                std::time::Instant::now() + std::time::Duration::from_secs(5),
            )
            .unwrap();
        let newer_sequence = newer.observed.as_ref().unwrap().0;
        assert!(newer_sequence > floor);
        queue
            .compact_if_needed(crate::DfsRepairQueueCompactionOptions::force())
            .unwrap();
        queue.reload_from_disk_on_next_access();
        let head = queue.view().unwrap().head_sequence;
        assert_eq!(head, newer_sequence);
        let state = queue.lock_state().unwrap();
        assert!(!state.custody.history_sequences.contains_key(&old_sequence));
        assert_eq!(
            state.custody.history_sequences.get(&newer_sequence),
            Some(&Some(first.request().query_ref.clone()))
        );
    }

    #[test]
    fn history_private_codecs_default_readers_reject_new_frames_and_snapshot_fields() {
        let root = sinomo_testdir::TestDir::new("history-default-codec-rejection");
        let (queue, identity) =
            LocalDfsRepairQueueStore::new_selected_original_fixture(root.join("destination"))
                .unwrap();
        let imported = install_origin(root.path(), &queue, &identity, "codec");
        let query = begin(&queue, &imported);
        queue
            .compact_if_needed(crate::DfsRepairQueueCompactionOptions::force())
            .unwrap();
        let mut persisted = read_repair_queue_snapshot(identity.snapshot_path()).unwrap();
        // Isolate the NEW field's default-reader guard from the preexisting
        // imported-original and association guards. Selected replay separately
        // requires the referenced import; default must reject before that join.
        persisted
            .custody
            .as_mut()
            .unwrap()
            .imported_original_data
            .clear();
        let path = root.join("ordinary-snapshot.sobj");
        let bytes = encode_sobj(&persisted).unwrap();
        write_file_atomic(&path, &bytes).unwrap();
        let error = LocalDfsRepairQueueStore::new(&path).view().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("default queue reader rejects selected original DATA")
        );
        assert_eq!(fs::read(&path).unwrap(), bytes);
        assert!(!repair_queue_active_log_path(&path).exists());

        let request = query.request();
        let response = crate::DfsOriginalRepairHistoryResponse {
            version: 1,
            query_ref: request.query_ref.clone(),
            import_ref: request.import_ref.clone(),
            original_command_digest: request.original_material.command_digest.clone(),
            query_carrier: request.carrier.clone(),
            result: crate::DfsOriginalRepairHistoryResult::Pending {
                reason: crate::DfsOriginalRepairHistoryPending::MissingTerminal,
            },
        };
        let returned = DfsRoutedMessage {
            source_node_id: request.carrier.target.clone(),
            target_node_id: request.carrier.source.clone(),
            frame_id: 41,
            message_id: 42,
            message: crate::DfsNetworkMessage::OriginalRepairHistoryResponse(response),
        };
        let changes = [
            RepairCustodyChange::BeginImportedHistoryQuery {
                import_ref: request.import_ref.clone(),
                assignment: request.current_assignment.clone(),
                carrier: request.carrier.clone(),
            },
            RepairCustodyChange::ObserveImportedHistoryQueryDelivery {
                query_ref: request.query_ref.clone(),
                carrier: request.carrier.clone(),
                delivery: None,
            },
            RepairCustodyChange::ObserveImportedHistoryQueryResult {
                query_ref: request.query_ref.clone(),
                returned,
            },
        ];
        for (number, change) in changes.into_iter().enumerate() {
            let path = root.join(format!("ordinary-log-{number}.sobj"));
            let ordinary = LocalDfsRepairQueueStore::new(&path);
            ordinary.custody_incarnation().unwrap();
            let before = ordinary.view().unwrap();
            let log = repair_queue_active_log_path(&path);
            let original_bytes = fs::read(&log).unwrap();
            {
                let mut state = ordinary.lock_state().unwrap();
                let error = state
                    .with_writer_lock(|state| {
                        state.append_operations_held(vec![DfsRepairQueueOperation::RepairCustody {
                            change: Box::new(change.clone()),
                        }])
                    })
                    .unwrap_err();
                assert!(
                    error
                        .to_string()
                        .contains("selected original DATA requires its admitted physical fixture")
                );
            }
            assert_eq!(ordinary.view().unwrap(), before);
            assert_eq!(fs::read(&log).unwrap(), original_bytes);
            // Negative valid-frame input exercises the ACTUAL replay codec and
            // new-variant guard, not a mirrored old serializer.
            let batch = DfsRepairQueueLogBatch {
                sequence: before.head_sequence + 1,
                operations: vec![DfsRepairQueueOperation::RepairCustody {
                    change: Box::new(change),
                }],
            };
            let bytes = sinomo_object_serde::to_bytes(&batch, 1, LengthMode::U32).unwrap();
            let mut append = durable_file::open_file_for_append(&log).unwrap();
            durable_file::append_length_prefixed_records(
                &mut append.file,
                &log,
                [bytes.as_slice()],
                durable_file::DurableRecordAppendOptions::new(
                    durable_file::DurableLengthEndian::Little,
                )
                .sync(durable_file::DurableFileSync::All),
            )
            .unwrap();
            drop(append);
            let fault_bytes = fs::read(&log).unwrap();
            ordinary.reload_from_disk_on_next_access();
            let error = ordinary.view().unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("default queue replay rejects selected original DATA")
            );
            assert_eq!(fs::read(&log).unwrap(), fault_bytes);
        }

        // Actual malformed selected frames: each has a valid scheduling Set
        // before two custody changes. Replay must reject the entire frame
        // before either the row projection or any custody index is changed.
        let (selected, selected_identity) =
            LocalDfsRepairQueueStore::new_selected_original_fixture(root.join("multi-destination"))
                .unwrap();
        let imported = install_origin(root.path(), &selected, &selected_identity, "multi-frame");
        selected
            .compact_if_needed(crate::DfsRepairQueueCompactionOptions::force())
            .unwrap();
        let (floor, prefix_records, prefix_custody) = {
            let mut state = selected.lock_state().unwrap();
            state.ensure_loaded().unwrap();
            (
                state.head_sequence,
                state.records.clone(),
                state.custody.clone(),
            )
        };
        let sequence = floor + 1;
        let carrier = DfsOriginalRepairHistoryCarrier {
            source: imported
                .material
                .cut
                .handoff
                .desired_shard
                .primary_node_id
                .clone(),
            target: imported.material.birth.original.target_node_id.clone(),
            frame_id: 51,
            message_id: 52,
        };
        // This is negative frame DATA, never a usable pre-append attempt.
        let negative_request = prefix_custody
            .history_request(
                &imported.history_ref().unwrap(),
                &imported.material.cut.handoff.desired_shard,
                &carrier,
                sequence,
            )
            .unwrap();
        let begin = RepairCustodyChange::BeginImportedHistoryQuery {
            import_ref: imported.history_ref().unwrap(),
            assignment: imported.material.cut.handoff.desired_shard.clone(),
            carrier: carrier.clone(),
        };
        let delivery = RepairCustodyChange::ObserveImportedHistoryQueryDelivery {
            query_ref: negative_request.query_ref.clone(),
            carrier,
            delivery: None,
        };
        let result = RepairCustodyChange::ObserveImportedHistoryQueryResult {
            query_ref: negative_request.query_ref.clone(),
            returned: pending_return(&negative_request),
        };
        let mut changed_row = prefix_records
            .values()
            .next()
            .expect("actual imported scheduling row")
            .clone();
        changed_row.last_error = Some("invalid multi-custody frame cannot publish this Set".into());
        for (number, changes) in [
            vec![begin.clone(), delivery],
            vec![begin.clone(), result],
            vec![
                RepairCustodyChange::Initialize {
                    incarnation: prefix_custody.incarnation.clone(),
                },
                begin,
            ],
        ]
        .into_iter()
        .enumerate()
        {
            let log = if number == 0 {
                repair_queue_active_log_path(selected_identity.snapshot_path())
            } else {
                root.join(format!("selected-negative-frame-{number}.slog"))
            };
            let operations = std::iter::once(DfsRepairQueueOperation::Set {
                record: changed_row.clone(),
            })
            .chain(
                changes
                    .into_iter()
                    .map(|change| DfsRepairQueueOperation::RepairCustody {
                        change: Box::new(change),
                    }),
            )
            .collect();
            let batch = DfsRepairQueueLogBatch {
                sequence,
                operations,
            };
            let bytes = sinomo_object_serde::to_bytes(&batch, 1, LengthMode::U32).unwrap();
            let mut append = durable_file::open_file_for_append(&log).unwrap();
            durable_file::append_length_prefixed_records(
                &mut append.file,
                &log,
                [bytes.as_slice()],
                durable_file::DurableRecordAppendOptions::new(
                    durable_file::DurableLengthEndian::Little,
                )
                .sync(durable_file::DurableFileSync::All),
            )
            .unwrap();
            drop(append);
            let physical = fs::read(&log).unwrap();
            let mut records = prefix_records.clone();
            let mut custody = prefix_custody.clone();
            let error =
                replay_repair_queue_log(&log, floor, &mut records, &mut custody, true).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("query-bearing frame requires one indivisible custody transition"),
                "{error}"
            );
            assert_eq!(records, prefix_records);
            assert_eq!(custody.snapshot(), prefix_custody.snapshot());
            assert_eq!(
                custody.imported_history_slots,
                prefix_custody.imported_history_slots
            );
            assert_eq!(custody.history_sequences, prefix_custody.history_sequences);
            assert_eq!(fs::read(&log).unwrap(), physical);
            if number == 0 {
                selected.reload_from_disk_on_next_access();
                let error = selected.view().unwrap_err();
                assert!(
                    error.to_string().contains(
                        "query-bearing frame requires one indivisible custody transition"
                    )
                );
                assert_eq!(
                    fs::read(&log).unwrap(),
                    physical,
                    "actual selected owner load must neither apply nor repair this semantic-invalid frame"
                );
            }
        }
    }
}
