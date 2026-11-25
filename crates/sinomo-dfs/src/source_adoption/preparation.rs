//! Source-primary capture after the ordinary coordinator lane has ended.
//! Plans and tokens identify work; only native captures retain writer custody.
use super::*;
use crate::DfsMetadataBackend;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct SourcePreparationPlan {
    pub(crate) request_id: String,
    pub(crate) job: crate::DfsNamespaceJobRecord,
    pub(crate) job_config: DfsMetadataShardConfig,
    pub(crate) source_config: DfsMetadataShardConfig,
    pub(crate) target_config: DfsMetadataShardConfig,
    pub(crate) destination: crate::DfsObjectMetadata,
    pub(crate) source_path: String,
    pub(crate) expected_source: Option<crate::DfsObjectMetadata>,
    pub(crate) transfer_id: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct SourcePreparationToken {
    pub(crate) request_id: String,
    pub(crate) capture_id: String,
    pub(crate) plan_digest: String,
    pub(crate) source_node_id: NodeId,
    pub(crate) prefix: crate::DfsMetadataPrefixProof,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PreparedSourceAdoption {
    pub(crate) offer: DfsSourceAdoptionOffer,
    pub(crate) family: DfsCrossShardSourceFamily,
}
pub(super) struct PendingSourcePreparation {
    pub(super) plan: SourcePreparationPlan,
    token: SourcePreparationToken,
    state: Mutex<SourcePreparationState>,
}
enum SourcePreparationState {
    Unbound(Arc<crate::DfsSourceAdoptionSourceCapture>),
    Bound(PreparedSourceAdoption),
    // Volatile ownership of terminal publication after an exact unclaimed
    // cancellation. It grants no effect authority and serializes only rebind.
    Terminalizing(Arc<CancelledSourcePreparation>),
    Revoked {
        // DATA from an actual guarded unclaimed-offer retirement. No writer,
        // target admission or committed outcome is retained here.
        cancelled: Option<Arc<CancelledSourcePreparation>>,
    },
}
struct CancelledSourcePreparation {
    plan: SourcePreparationPlan,
    token: SourcePreparationToken,
    prepared: PreparedSourceAdoption,
    physical_scope: crate::publication_guard::DfsPublicationOwnerScope,
    authority_generation: u64,
    storage_generation: u64,
}

struct CancelledTerminalization {
    pending: Arc<PendingSourcePreparation>,
    cancelled: Arc<CancelledSourcePreparation>,
}
impl Drop for CancelledTerminalization {
    fn drop(&mut self) {
        let mut state = self
            .pending
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if matches!(&*state, SourcePreparationState::Terminalizing(actual)
            if Arc::ptr_eq(actual, &self.cancelled))
        {
            *state = SourcePreparationState::Revoked {
                cancelled: Some(self.cancelled.clone()),
            };
        }
    }
}
fn terminalization_blocks_job(
    rows: &BTreeMap<String, Arc<PendingSourcePreparation>>,
    job: &crate::DfsNamespaceJobRecord,
) -> Result<bool> {
    for pending in rows.values().filter(|pending| {
        pending.plan.job.file_system_id == job.file_system_id
            && pending.plan.job.job_id == job.job_id
    }) {
        if matches!(
            &*pending
                .state
                .lock()
                .map_err(|_| error("source preparation state poisoned"))?,
            SourcePreparationState::Terminalizing(_)
        ) {
            return Ok(true);
        }
    }
    Ok(false)
}
fn terminalization_busy() -> MetadataBackendError {
    MetadataBackendError::StatusUnavailable {
        reason: "source preparation is settling its exact rejected adoption".into(),
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "state", content = "body", rename_all = "kebab-case")]
pub(crate) enum SourcePreparationRevocation {
    Revoked(SourcePreparationToken),
    Bound(PreparedSourceAdoption),
}
fn validate_cancelled_preparation_plan(
    cancelled: &CancelledSourcePreparation,
    current: &SourcePreparationPlan,
) -> Result<()> {
    // Comparison DATA only, never an input replacement or rebase. Preserve
    // every original frame, cursor, object precondition, operation and owner;
    // only the coordinator's error text and update timestamp may differ.
    let mut compared = current.clone();
    compared.job.updated_at = cancelled.plan.job.updated_at;
    compared.job.failure_reason = cancelled.plan.job.failure_reason.clone();
    if compared != cancelled.plan {
        tracing::warn!(target: "sinomo.dfs.source_adoption",
            request = %current.request_id,
            job = %current.job.job_id,
            original_token = ?cancelled.token,
            pending_frames_equal = current.job.pending_frames == cancelled.plan.job.pending_frames,
            processed_count_equal = current.job.processed_object_count == cancelled.plan.job.processed_object_count,
            original_job = ?cancelled.plan.job,
            current_job = ?current.job,
            "cancelled source preparation rejected changed original job/body");
        return Err(error(
            "cancelled source retry changes original operation/body",
        ));
    }
    Ok(())
}

fn validate_cancelled_source_rows(
    cancelled: &CancelledSourcePreparation,
    current: &DfsCrossShardSourceFamily,
) -> Result<()> {
    // The fresh native writer determines its own actual cut; the old cancelled
    // clock stays historical DATA. All original source rows must still match.
    let rows = |family: &DfsCrossShardSourceFamily| -> Result<serde_json::Value> {
        let mut data = serde_json::to_value(family).map_err(error)?;
        data.as_object_mut()
            .ok_or_else(|| error("cancelled source family is not a typed object"))?
            .remove("source_journal_sequence")
            .ok_or_else(|| error("cancelled source family has no actual source clock"))?;
        Ok(data)
    };
    if rows(&cancelled.prepared.family)? != rows(current)? {
        return Err(error(
            "fresh source capture changes cancelled original source rows",
        ));
    }
    Ok(())
}

impl PendingSourcePreparation {
    pub(super) fn holds_unbound(&self) -> Result<bool> {
        Ok(matches!(
            *self
                .state
                .lock()
                .map_err(|_| error("source preparation state poisoned"))?,
            SourcePreparationState::Unbound(_)
        ))
    }
}
impl DfsSourceAdoptionActor {
    fn rebind_capture_backend(
        &self,
        backend: &mut crate::LocalSobjMetadataBackend,
        expected: &DfsMetadataShardConfig,
    ) -> Result<()> {
        let shard_id = crate::DfsMetadataShardId::new(&expected.shard_id);
        if !backend.metadata_shard_instance_writes_frozen(&shard_id)? {
            return Ok(());
        }
        // A newly opened capture owns no successor binding. Reuse the normal
        // completed-handoff proof and exact-prefix check against the runtime's
        // coherent configuration; an activated marker alone grants nothing.
        let projection = self
            .authority
            .write_configuration_projection()
            .map_err(error)?
            .ok_or_else(|| error("source capture configuration unavailable"))?;
        let entries = projection
            .entries(crate::DFS_METADATA_AUTHORITY_PREFIX)
            .ok_or_else(|| error("source capture configuration scope unavailable"))?;
        let active_key = crate::metadata_shard_key(&expected.file_system_id, &expected.shard_id);
        let active: Option<DfsMetadataShardConfig> = entries
            .iter()
            .find(|entry| entry.key == active_key)
            .map(|entry| entry.decode())
            .transpose()
            .map_err(error)?;
        if active.as_ref() != Some(expected) || expected.primary_node_id != self.node {
            return Err(error("source capture cannot bind a different active shard"));
        }
        let pending_key = crate::metadata_shard_membership_handoff_key(
            &expected.file_system_id,
            &expected.shard_id,
        );
        let proof_key = crate::metadata_shard_membership_handoff_proof_key(
            &expected.file_system_id,
            &expected.shard_id,
        );
        let pending = entries
            .iter()
            .find(|entry| entry.key == pending_key)
            .map(|entry| entry.decode())
            .transpose()
            .map_err(error)?;
        let proof = entries
            .iter()
            .find(|entry| entry.key == proof_key)
            .map(|entry| entry.decode())
            .transpose()
            .map_err(error)?;
        if !crate::service_sync::rebind_completed_metadata_shard_membership_handoff(
            backend,
            &self.node,
            expected,
            pending.as_ref(),
            proof.as_ref(),
        )
        .map_err(error)?
        {
            return Err(error("source capture has no completed exact handoff proof"));
        }
        Ok(())
    }

    pub(super) fn revoke_cancelled_bound_preparation(
        &self,
        offer: &DfsSourceAdoptionOffer,
        physical_scope: crate::publication_guard::DfsPublicationOwnerScope,
        authority_generation: u64,
        storage_generation: u64,
    ) -> Result<()> {
        for actor in std::iter::once(self).chain(self.predecessors.iter().map(Arc::as_ref)) {
            let preparations = actor
                .source_preparations
                .lock()
                .map_err(|_| error("cancelled source preparations poisoned"))?;
            for pending in preparations.values() {
                let mut state = pending
                    .state
                    .lock()
                    .map_err(|_| error("cancelled source preparation state poisoned"))?;
                let SourcePreparationState::Bound(prepared) = &*state else {
                    continue;
                };
                if prepared.offer != *offer {
                    continue;
                }
                let cancelled = Arc::new(CancelledSourcePreparation {
                    plan: pending.plan.clone(),
                    token: pending.token.clone(),
                    prepared: prepared.clone(),
                    physical_scope: physical_scope.clone(),
                    authority_generation,
                    storage_generation,
                });
                *state = SourcePreparationState::Revoked {
                    cancelled: Some(cancelled),
                };
            }
        }
        Ok(())
    }
    /// Existing volatile cancellation DATA can serialize a terminal attempt;
    /// only the normal current native kernel may publish the Failed job.
    pub(crate) fn finish_locally_cancelled_adoption(
        &self,
        request_id: &str,
        job: &crate::DfsNamespaceJobRecord,
        destination: &crate::DfsObjectMetadata,
        source_path: &str,
        physical_scope: &crate::publication_guard::DfsPublicationOwnerScope,
        execution: &crate::publication_guard::DfsPublicationExecutionOwners,
        finish: impl FnOnce() -> crate::posix_model::Result<()>,
    ) -> crate::posix_model::Result<bool> {
        let initial_file = job.kind == crate::DfsNamespaceJobKind::RenameObjectAcrossShardPrimaries
            && job.processed_object_count == 0 && destination.kind == crate::DfsObjectKind::File;
        let initial_directory = job.initial_directory_root_adoption() == Some(destination);
        if (!initial_file && !initial_directory)
            || job.status != DfsNamespaceJobStatus::Running || job.root_path != source_path
        {
            return Ok(false);
        }
        let preparations = self
            .source_preparations
            .lock()
            .map_err(|_| error("source preparations poisoned"))?;
        let Some(pending) = preparations.get(request_id).cloned() else {
            return Ok(false);
        };
        let mut state = pending
            .state
            .lock()
            .map_err(|_| error("source preparation state poisoned"))?;
        let SourcePreparationState::Revoked {
            cancelled: Some(cancelled),
        } = &*state
        else {
            return Ok(false);
        };
        let cancelled = Arc::clone(cancelled);
        for other in preparations.values().filter(|other| {
            !Arc::ptr_eq(other, &pending)
                && other.plan.job.file_system_id == job.file_system_id
                && other.plan.job.job_id == job.job_id
        }) {
            if !matches!(
                &*other
                    .state
                    .lock()
                    .map_err(|_| error("source preparation state poisoned"))?,
                SourcePreparationState::Revoked { .. }
            ) {
                return Ok(false);
            }
        }
        for previous in &self.predecessors {
            let rows = previous
                .source_preparations
                .lock()
                .map_err(|_| error("retained source preparations poisoned"))?;
            for other in rows.values().filter(|other| {
                other.plan.job.file_system_id == job.file_system_id
                    && other.plan.job.job_id == job.job_id
            }) {
                if !matches!(
                    &*other
                        .state
                        .lock()
                        .map_err(|_| error("retained source state poisoned"))?,
                    SourcePreparationState::Revoked { .. }
                ) {
                    return Ok(false);
                }
            }
        }
        let offer = &cancelled.prepared.offer;
        let plan = &cancelled.plan;
        let mut current = plan.clone();
        current.job = job.clone();
        current.destination = destination.clone();
        current.source_path = source_path.into();
        current.request_id = request_id.into();
        validate_cancelled_preparation_plan(&cancelled, &current)?;
        if offer.source_node_id != self.node
            || offer.job_offer.coordinator_node_id != self.node
            || plan.source_config != plan.job_config
            || plan.source_config.primary_node_id != self.node
            || plan.destination != *destination
            || plan.source_path != source_path
            || cancelled.prepared.family.source_object.kind != destination.kind
            || &cancelled.physical_scope != physical_scope
            || self.authority.storage_generation() != cancelled.storage_generation
            || self.authority.generation() != cancelled.authority_generation
            || execution.data_root() != self.data_root
            || !execution.admits(
                physical_scope,
                &job.file_system_id,
                physical_scope.shard_id(),
            )
        {
            return Ok(false);
        }
        self.validate_preparation_plan(&self.node, &current)?;
        let owner = crate::runtime_host::publication_runtime_owner(physical_scope.root())
            .map_err(error)?
            .ok_or_else(|| error("cancelled source has no current native data owner"))?;
        if !Arc::ptr_eq(&owner, &self._owner) {
            return Ok(false);
        }
        let retired = self
            .retired_jobs
            .lock()
            .map_err(|_| error("original job retirement poisoned"))?;
        if !retired
            .get(&offer.job_offer.session_id)
            .is_some_and(|(actual, digest)| actual == offer && digest.is_none())
        {
            return Ok(false);
        }
        drop(retired);
        *state = SourcePreparationState::Terminalizing(cancelled.clone());
        let reservation = CancelledTerminalization {
            pending: pending.clone(),
            cancelled,
        };
        drop(state);
        drop(preparations);
        // An in-flight actual capture can now finish its existing relock and
        // release its writer. No mutex is held while native publication waits.
        let result = finish();
        drop(reservation);
        result.map(|()| true)
    }

    pub(super) fn revoke_unbound_preparation(
        &self,
        caller: &NodeId,
        token: &SourcePreparationToken,
    ) -> Result<SourcePreparationRevocation> {
        let pending = self
            .source_preparations
            .lock()
            .map_err(|_| error("source preparations poisoned"))?
            .get(&token.request_id)
            .cloned()
            .or_else(|| {
                self.predecessors.iter().find_map(|old| {
                    old.source_preparations
                        .lock()
                        .ok()
                        .and_then(|rows| rows.get(&token.request_id).cloned())
                })
            })
            .ok_or_else(|| error("actual source preparation unavailable"))?;
        if pending.token != *token
            || caller != &pending.plan.job_config.primary_node_id
            || token.source_node_id != self.node
        {
            return Err(error(
                "unbound revocation changes exact captured owner/token identity",
            ));
        }
        let mut state = pending
            .state
            .lock()
            .map_err(|_| error("source preparation state poisoned"))?;
        match &*state {
            SourcePreparationState::Bound(prepared) => {
                return Ok(SourcePreparationRevocation::Bound(prepared.clone()));
            }
            SourcePreparationState::Revoked { .. } => {
                return Ok(SourcePreparationRevocation::Revoked(token.clone()));
            }
            SourcePreparationState::Terminalizing(_) => return Err(terminalization_busy()),
            SourcePreparationState::Unbound(_) => {}
        }
        // The same lock serializes revocation against all offer construction.
        // This drops only the unbound actual writer, never a target witness.
        *state = SourcePreparationState::Revoked { cancelled: None };
        RETAINED
            .get_or_init(Default::default)
            .lock()
            .map_err(|_| error("source custody registry poisoned"))?
            .remove(&(
                self.data_root.clone(),
                format!("preparation:{}", token.request_id),
            ));
        Ok(SourcePreparationRevocation::Revoked(token.clone()))
    }
    pub(crate) fn register(
        self: &Arc<Self>,
        capture: DfsSourceAdoptionCapture,
        plan: SourcePreparationPlan,
    ) -> Result<DfsSourceAdoptionOffer> {
        self.validate_preparation_plan(&self.node, &plan)?;
        if capture.job() != &plan.job
            || capture.family().source_object.object_id != plan.destination.object_id
            || capture.family().source_object.path != plan.source_path
            || plan
                .expected_source
                .as_ref()
                .is_some_and(|expected| expected != &capture.family().source_object)
        {
            return Err(error(
                "local source capture changed its full preparation plan",
            ));
        }
        let token = SourcePreparationToken {
            request_id: plan.request_id.clone(),
            capture_id: sinomo_core::Id::generate().to_string(),
            plan_digest: blake3::hash(&serde_json::to_vec(&plan).map_err(error)?)
                .to_hex()
                .to_string(),
            source_node_id: self.node.clone(),
            prefix: capture.prefix().clone(),
        };
        let mut preparations = self
            .source_preparations
            .lock()
            .map_err(|_| error("source preparation poisoned"))?;
        if terminalization_blocks_job(&preparations, &plan.job)? {
            return Err(terminalization_busy());
        }
        for previous in &self.predecessors {
            let rows = previous
                .source_preparations
                .lock()
                .map_err(|_| error("retained source preparations poisoned"))?;
            if terminalization_blocks_job(&rows, &plan.job)? {
                return Err(terminalization_busy());
            }
        }
        if let Some(existing) = preparations.get(&plan.request_id) {
            let state = existing
                .state
                .lock()
                .map_err(|_| error("source preparation state poisoned"))?;
            match &*state {
                SourcePreparationState::Terminalizing(_) => return Err(terminalization_busy()),
                SourcePreparationState::Revoked {
                    cancelled: Some(cancelled),
                } => {
                    validate_cancelled_preparation_plan(cancelled, &plan)?;
                    validate_cancelled_source_rows(cancelled, capture.family())?;
                }
                SourcePreparationState::Revoked { cancelled: None } => {
                    if existing.plan != plan {
                        #[cfg(test)]
                        tracing::warn!(target: "sinomo.dfs.source_adoption",
                            original_plan = ?existing.plan,
                            current_plan = ?plan,
                            "local unbound-revoked source guard rejected different complete plan");
                        return Err(error(
                            "local source retry changed its complete preparation plan",
                        ));
                    }
                }
                SourcePreparationState::Bound(prepared) => {
                    if existing.plan != plan {
                        return Err(error(
                            "local source retry changed its complete preparation plan",
                        ));
                    }
                    return Ok(prepared.offer.clone());
                }
                SourcePreparationState::Unbound(_) => {
                    if existing.plan != plan {
                        return Err(error(
                            "local source retry changed its complete preparation plan",
                        ));
                    }
                    return Err(error(
                        "local source phase already owns an unbound preparation",
                    ));
                }
            }
        }
        let (source_capture, job_capture) = capture.into_parts();
        let family = source_capture.family().clone();
        let job_offer = self.register_original_job(
            job_capture,
            plan.job_config.clone(),
            plan.source_config.clone(),
            plan.target_config.clone(),
            plan.destination.clone(),
            plan.source_path.clone(),
        )?;
        let offer = self.register_source_capture(
            Arc::new(source_capture),
            plan.source_config.clone(),
            plan.target_config.clone(),
            plan.transfer_id.clone(),
            plan.destination.clone(),
            job_offer,
        )?;
        // The real source/job sessions own both original writers. This is the
        // SAME small Bound receipt used by normal remote preparation retries.
        preparations.insert(
            plan.request_id.clone(),
            Arc::new(PendingSourcePreparation {
                plan,
                token,
                state: Mutex::new(SourcePreparationState::Bound(PreparedSourceAdoption {
                    offer: offer.clone(),
                    family,
                })),
            }),
        );
        Ok(offer)
    }

    pub(crate) fn inspect_source_preparation(
        &self,
        caller: &NodeId,
        plan: &SourcePreparationPlan,
    ) -> Result<Option<PreparedSourceAdoption>> {
        self.validate_preparation_plan(caller, plan)?;
        let pending = self
            .source_preparations
            .lock()
            .map_err(|_| error("source preparations poisoned"))?
            .get(&plan.request_id)
            .cloned()
            .or_else(|| {
                self.predecessors.iter().find_map(|old| {
                    old.source_preparations
                        .lock()
                        .ok()
                        .and_then(|rows| rows.get(&plan.request_id).cloned())
                })
            });
        let Some(pending) = pending else {
            return Ok(None);
        };
        let prepared = match &*pending
            .state
            .lock()
            .map_err(|_| error("source preparation state poisoned"))?
        {
            SourcePreparationState::Bound(prepared) => prepared.clone(),
            SourcePreparationState::Revoked {
                cancelled: Some(cancelled),
            } => {
                validate_cancelled_preparation_plan(cancelled, plan)?;
                return Ok(None);
            }
            _ => return Ok(None),
        };
        if pending.plan == *plan {
            return Ok(Some(prepared));
        }
        // A later phase can already see the destination and omit the removed
        // source entry. Carry only this actor's original expectation, proved by
        // its actual source capture. This retry still passes every identity check
        // and the native terminal-outcome proof below; it cannot reenter the
        // original exact-plan fast path or authorize an unknown bound target.
        let mut retry_plan = plan.clone();
        if retry_plan.expected_source.is_none()
            && pending.plan.expected_source.as_ref() == Some(&prepared.family.source_object)
        {
            retry_plan.expected_source = pending.plan.expected_source.clone();
        }
        let plan = &retry_plan;
        // A later current job phase may retry an already committed adoption.
        // Historical work is data only; verify the exact native terminal body
        // before returning it without reacquiring an original job writer.
        if pending.plan.job.job_id != plan.job.job_id
            || pending.plan.job.file_system_id != plan.job.file_system_id
            || pending.plan.source_config != plan.source_config
            || pending.plan.target_config != plan.target_config
            || pending.plan.job_config != plan.job_config
            || pending.plan.destination != plan.destination
            || pending.plan.source_path != plan.source_path
            || pending.plan.expected_source != plan.expected_source
            || pending.plan.transfer_id != plan.transfer_id
        {
            // Bounded DATA from the same compared plans; this grants no custody.
            let identity = |object: &crate::DfsObjectMetadata| {
                (
                    object.file_system_id.clone(),
                    object.object_id.clone(),
                    object.version.clone(),
                    object.owner_node_id.clone(),
                    object.path.chars().take(256).collect::<String>(),
                    object
                        .content_hash
                        .as_ref()
                        .map(|hash| hash.chars().take(96).collect::<String>()),
                    object.content_size_bytes,
                )
            };
            tracing::warn!(target: "sinomo.dfs.source_adoption",
                job_id_equal = pending.plan.job.job_id == plan.job.job_id,
                filesystem_equal = pending.plan.job.file_system_id == plan.job.file_system_id,
                source_config_equal = pending.plan.source_config == plan.source_config,
                target_config_equal = pending.plan.target_config == plan.target_config,
                job_config_equal = pending.plan.job_config == plan.job_config,
                destination_equal = pending.plan.destination == plan.destination,
                source_path_equal = pending.plan.source_path == plan.source_path,
                expected_source_equal = pending.plan.expected_source == plan.expected_source,
                transfer_id_equal = pending.plan.transfer_id == plan.transfer_id,
                old_expected_matches_captured = pending.plan.expected_source.as_ref()
                    .is_some_and(|expected| expected == &prepared.family.source_object),
                destination_locations_equal = pending.plan.destination.current_locations == plan.destination.current_locations,
                old_destination = ?identity(&pending.plan.destination),
                current_destination = ?identity(&plan.destination),
                old_expected_source = ?pending.plan.expected_source.as_ref().map(&identity),
                current_expected_source = ?plan.expected_source.as_ref().map(&identity),
                "source preparation rejected changed stable identity"
            );
            return Err(error(
                "source preparation retry changed its stable adoption identity",
            ));
        }
        let completed = self
            .completed_sources
            .lock()
            .map_err(|_| error("completed sources poisoned"))?
            .get(&prepared.offer.session_id)
            .cloned()
            .or_else(|| {
                self.predecessors.iter().find_map(|old| {
                    old.completed_sources
                        .lock()
                        .ok()
                        .and_then(|rows| rows.get(&prepared.offer.session_id).cloned())
                })
            })
            .ok_or_else(|| error("changed current job has no retained terminal adoption"))?;
        let reply = self.call(
            &plan.target_config.primary_node_id,
            &DfsSourceAdoptionRequest::InspectOutcome {
                offer: prepared.offer.clone(),
                target_scope: completed.scope.clone(),
                payload_digest: completed.hash.clone(),
                expected_digest: Some(completed.digest.clone()),
            },
        )?;
        match reply.outcome {
            Some(DfsSourceAdoptionOutcome::Committed { digest, commit })
                if commits_adoption(&completed.digest, &digest) =>
            {
                commit.validate(&digest)?
            }
            _ => {
                return Err(error(
                    "changed current job has no exact native terminal adoption proof",
                ));
            }
        }
        Ok(Some(prepared))
    }
    pub(super) fn complete_remote_source(&self, offer: &DfsSourceAdoptionOffer) -> Result<()> {
        if offer.source_node_id == self.node {
            return self.complete_source_received(&self.node, offer);
        }
        match self.call_rpc(
            &offer.source_node_id,
            &DfsSourceAdoptionRequest::CompleteSource {
                offer: offer.clone(),
            },
        )? {
            DfsSourceAdoptionRpcResponse::SourceCompleted { offer: actual } if actual == *offer => {
                Ok(())
            }
            _ => Err(error(
                "source completion response changed the exact native offer",
            )),
        }
    }
    pub(super) fn complete_source_received(
        &self,
        caller: &NodeId,
        offer: &DfsSourceAdoptionOffer,
    ) -> Result<()> {
        if caller != &offer.job_offer.coordinator_node_id {
            return Err(error(
                "source completion is not from the original coordinator",
            ));
        }
        self.require_current(offer)?;
        let local_completed = self
            .completed_sources
            .lock()
            .map_err(|_| error("completed sources poisoned"))?
            .get(&offer.session_id)
            .cloned();
        let completed = match local_completed {
            Some(completed) => completed,
            None => self
                .predecessors
                .iter()
                .find_map(|old| {
                    old.completed_sources
                        .lock()
                        .ok()
                        .and_then(|completed| completed.get(&offer.session_id).cloned())
                })
                .ok_or_else(|| error("source has no exact released target effect"))?,
        };
        if completed.offer != *offer {
            return Err(error("source completion changes its exact retained effect"));
        }
        let reply = self.call(
            &offer.target_config.primary_node_id,
            &DfsSourceAdoptionRequest::InspectOutcome {
                offer: offer.clone(),
                target_scope: completed.scope.clone(),
                payload_digest: completed.hash.clone(),
                expected_digest: Some(completed.digest.clone()),
            },
        )?;
        match reply.outcome {
            Some(DfsSourceAdoptionOutcome::Committed { digest, commit })
                if commits_adoption(&completed.digest, &digest) =>
            {
                commit.validate(&digest)?
            }
            ref outcome => {
                tracing::warn!(
                    session_id = %offer.session_id,
                    job_id = %offer.job_id,
                    source_node_id = %offer.source_node_id,
                    target_node_id = %offer.target_config.primary_node_id,
                    offer_equal = (reply.offer == *offer),
                    target_scope_equal = (reply.target_scope == completed.scope),
                    payload_digest_equal = (reply.payload_digest == completed.hash),
                    expected_target_scope = ?completed.scope,
                    actual_target_scope = ?reply.target_scope,
                    expected_digest = ?completed.digest,
                    actual_outcome = ?outcome,
                    "DFS source completion rejected the actual native terminal target proof"
                );
                return Err(error(
                    "source completion has no exact current native terminal target proof",
                ));
            }
        }
        let data_dir = sinomo_core::local::SinomoDataDir::new(&self.data_root);
        let mut service = crate::runtime_host::open_dfs_runtime_service_on_shard(
            &data_dir,
            Some(&self.runtime),
            &offer.source_config,
        )
        .map_err(error)?;
        // Capture retirement's source family under the same mutation admission
        // as its guarded publication. A queued admission may span unrelated
        // journal progress; the captured sequence must remain an exact predicate.
        let retirement_scope = crate::DfsPreparedOperationScope {
            file_system_id: offer.source_config.file_system_id.clone(),
            shard_id: crate::DfsMetadataShardId::new(offer.source_config.shard_id.clone()),
            owner_epoch: offer.source_config.epoch,
            operation_id: format!("source-retirement:{}", offer.session_id),
        };
        let _retirement = service
            .acquire_prepared_execution(&retirement_scope)
            .map_err(error)?;
        // This service opened the shard on its own. A handoff the assignment
        // already completed leaves its instance frozen until it is rebound, as
        // every other mutation entry does before it takes the writer.
        service
            .rebind_completed_local_metadata_shard_membership_handoff_if_frozen(
                &offer.source_config,
            )
            .map_err(error)?;
        let family = service
            .metadata_backend_mut()
            .ok_or_else(|| error("source completion has no native backend"))?
            .capture_cross_shard_source_family(
                &offer.object_id,
                &completed.family.source_object.path,
            )?;
        if matches!(
            &family.source_object.tombstone_provenance,
            Some(crate::DfsTombstoneProvenance::SourceEviction { .. })
        ) {
            return Ok(());
        }
        if family.source_object != completed.family.source_object
            || family.version_pins != completed.family.version_pins
        {
            return Err(error(
                "original source head/pins changed before guarded retirement",
            ));
        }
        let transfer = (!family.version_pins.is_empty() || !family.pin_transfers.is_empty())
            .then_some(offer.transfer_id.as_str());
        service
            .publish_cross_shard_source_eviction(
                &offer.destination,
                &family.source_object.path,
                &family,
                transfer,
                if family.source_object.kind == crate::DfsObjectKind::Directory
                    && completed.original_job.kind == crate::DfsNamespaceJobKind::RenameDirectoryAcrossShardPrimaries
                    && crate::namespace::metadata_path(&completed.original_job.root_path)
                        == crate::namespace::metadata_path(&family.source_object.path) {
                    completed.original_job.directory_move_counts()?.map_or(0, |plan| plan.source_effect_delta(&family.source_object.path))
                } else { 0 },
            )
            .map_err(error)?;
        Ok(())
    }
    fn validate_preparation_plan(
        &self,
        caller: &NodeId,
        plan: &SourcePreparationPlan,
    ) -> Result<()> {
        if *self.shutdown.borrow()
            || self.shutdown.has_changed().is_err()
            || caller != &plan.job_config.primary_node_id
            || self.node != plan.source_config.primary_node_id
            || plan.job.status != DfsNamespaceJobStatus::Running
            || plan.job.file_system_id != plan.source_config.file_system_id
            || plan.job.file_system_id != plan.target_config.file_system_id
            || plan.job.file_system_id != plan.job_config.file_system_id
            || !job_custody::job_binds_destination(&plan.job, &plan.destination, &plan.source_path)
            || plan.request_id.is_empty()
            || plan.transfer_id.is_empty()
        {
            #[cfg(test)]
            eprintln!(
                "SOURCE_PLAN_REJECT root={:?} closed={} sender_closed={} caller={} coordinator={} actor={} source={} status={:?} fs_source={} fs_target={} fs_job={} binds={} request_empty={} transfer_empty={} job={:?} destination={:?} path={}",
                self.data_root,
                *self.shutdown.borrow(),
                self.shutdown.has_changed().is_err(),
                caller,
                plan.job_config.primary_node_id,
                self.node,
                plan.source_config.primary_node_id,
                plan.job.status,
                plan.job.file_system_id == plan.source_config.file_system_id,
                plan.job.file_system_id == plan.target_config.file_system_id,
                plan.job.file_system_id == plan.job_config.file_system_id,
                job_custody::job_binds_destination(&plan.job, &plan.destination, &plan.source_path),
                plan.request_id.is_empty(),
                plan.transfer_id.is_empty(),
                plan.job,
                plan.destination,
                plan.source_path,
            );
            return Err(error(
                "source preparation lacks its actual source/coordinator plan",
            ));
        }
        let configs = self
            .authority
            .configuration_snapshot()
            .metadata_shards(&plan.job.file_system_id)
            .map_err(error)?
            .ok_or_else(|| error("source preparation configuration unavailable"))?;
        if !configs.contains(&plan.job_config)
            || !configs.contains(&plan.source_config)
            || !configs.contains(&plan.target_config)
        {
            return Err(error("source preparation current assignment changed"));
        }
        Ok(())
    }
    pub(super) fn acquire_remote_source(
        self: &Arc<Self>,
        caller: &NodeId,
        plan: SourcePreparationPlan,
    ) -> Result<SourcePreparationToken> {
        self.validate_preparation_plan(caller, &plan)?;
        let mut cancelled = None;
        let mut pending = self
            .source_preparations
            .lock()
            .map_err(|_| error("source preparation poisoned"))?;
        if terminalization_blocks_job(&pending, &plan.job)? {
            return Err(terminalization_busy());
        }
        for previous in &self.predecessors {
            let rows = previous
                .source_preparations
                .lock()
                .map_err(|_| error("retained source preparations poisoned"))?;
            if terminalization_blocks_job(&rows, &plan.job)? {
                return Err(terminalization_busy());
            }
        }
        if !pending.contains_key(&plan.request_id) {
            for previous in &self.predecessors {
                let mut old = previous
                    .source_preparations
                    .lock()
                    .map_err(|_| error("retained source preparations poisoned"))?;
                if let Some(existing) = old.get(&plan.request_id).cloned() {
                    let state = existing
                        .state
                        .lock()
                        .map_err(|_| error("retained source state poisoned"))?;
                    if matches!(&*state, SourcePreparationState::Terminalizing(_)) {
                        return Err(terminalization_busy());
                    }
                    if let SourcePreparationState::Revoked { cancelled: old } = &*state {
                        if let Some(old) = old {
                            validate_cancelled_preparation_plan(old, &plan)?;
                            cancelled = Some(Arc::clone(old));
                        }
                        continue;
                    }
                    drop(state);
                    if existing.plan != plan {
                        return Err(error(
                            "retained source preparation changed its complete plan",
                        ));
                    }
                    self.retain_current_session(format!("preparation:{}", plan.request_id))?;
                    old.remove(&plan.request_id);
                    pending.insert(plan.request_id.clone(), existing);
                    break;
                }
            }
        }
        if let Some(existing) = pending.get(&plan.request_id).cloned() {
            let state = existing
                .state
                .lock()
                .map_err(|_| error("source preparation state poisoned"))?;
            if matches!(&*state, SourcePreparationState::Terminalizing(_)) {
                return Err(terminalization_busy());
            }
            if !matches!(*state, SourcePreparationState::Revoked { .. }) {
                if existing.plan != plan {
                    return Err(error("source preparation retry changed its complete plan"));
                }
                return Ok(existing.token.clone());
            }
            if let SourcePreparationState::Revoked { cancelled: old } = &*state {
                if let Some(old) = old {
                    validate_cancelled_preparation_plan(old, &plan)?;
                    cancelled = Some(Arc::clone(old));
                }
            }
            drop(state);
            // Retain the cancelled DATA until a fresh real capture succeeds.
        }
        drop(pending);
        let root = crate::default_dfs_metadata_shards_root(
            &sinomo_core::local::SinomoDataDir::new(&self.data_root),
        )
        .join(crate::metadata_keys::key_segment(
            &plan.source_config.shard_id,
        ));
        let mut backend = crate::LocalSobjMetadataBackend::new(
            crate::DfsMetadataShardId::new(&plan.source_config.shard_id),
            root,
        )?;
        self.rebind_capture_backend(&mut backend, &plan.source_config)?;
        let capture = backend
            .capture_source_adoption_source(&plan.destination.object_id, &plan.source_path)?;
        if plan
            .expected_source
            .as_ref()
            .is_some_and(|expected| expected != &capture.family().source_object)
        {
            return Err(error("actual source head changed before remote capture"));
        }
        self.validate_preparation_plan(caller, &plan)?;
        if let Some(cancelled) = &cancelled {
            validate_cancelled_source_rows(cancelled, capture.family())?;
        }
        let token = SourcePreparationToken {
            request_id: plan.request_id.clone(),
            capture_id: sinomo_core::Id::generate().to_string(),
            plan_digest: blake3::hash(&serde_json::to_vec(&plan).map_err(error)?)
                .to_hex()
                .to_string(),
            source_node_id: self.node.clone(),
            prefix: capture.prefix().clone(),
        };
        #[cfg(any(test, feature = "test-support"))]
        crate::source_adoption_test::capture_preparation_and_pause(
            &self.data_root,
            crate::source_adoption_test::DfsSourcePausePoint::BeforeSourceRegistration,
            &plan,
            &token,
            None,
            &self.runtime,
        );
        let mut pending = self
            .source_preparations
            .lock()
            .map_err(|_| error("source preparation poisoned"))?;
        if terminalization_blocks_job(&pending, &plan.job)? {
            return Err(terminalization_busy());
        }
        for previous in &self.predecessors {
            let rows = previous
                .source_preparations
                .lock()
                .map_err(|_| error("retained source preparations poisoned"))?;
            if terminalization_blocks_job(&rows, &plan.job)? {
                return Err(terminalization_busy());
            }
        }
        if let Some(existing) = pending.get(&plan.request_id) {
            let state = existing
                .state
                .lock()
                .map_err(|_| error("source preparation state poisoned"))?;
            if matches!(&*state, SourcePreparationState::Terminalizing(_)) {
                return Err(terminalization_busy());
            }
            if !matches!(*state, SourcePreparationState::Revoked { .. }) {
                if existing.plan != plan {
                    return Err(error(
                        "source preparation changed while its actual writer queued",
                    ));
                }
                return Ok(existing.token.clone());
            }
        }
        pending.insert(
            plan.request_id.clone(),
            Arc::new(PendingSourcePreparation {
                plan,
                token: token.clone(),
                state: Mutex::new(SourcePreparationState::Unbound(Arc::new(capture))),
            }),
        );
        RETAINED
            .get_or_init(Default::default)
            .lock()
            .map_err(|_| error("source custody registry poisoned"))?
            .insert(
                (
                    self.data_root.clone(),
                    format!("preparation:{}", token.request_id),
                ),
                self.clone(),
            );
        Ok(token)
    }
    pub(super) fn bind_remote_source(
        self: &Arc<Self>,
        caller: &NodeId,
        token: SourcePreparationToken,
        job_offer: DfsSourceAdoptionJobOffer,
    ) -> Result<PreparedSourceAdoption> {
        let pending = self
            .source_preparations
            .lock()
            .map_err(|_| error("source preparation poisoned"))?
            .get(&token.request_id)
            .cloned()
            .ok_or_else(|| error("actual source preparation unavailable"))?;
        self.validate_preparation_plan(caller, &pending.plan)?;
        if pending.token != token
            || job_offer.job_id != pending.plan.job.job_id
            || job_offer.job_config != pending.plan.job_config
            || job_offer.coordinator_node_id != *caller
        {
            return Err(error(
                "source binding changed its exact native preparation/job identity",
            ));
        }
        let receipt = if caller == &self.node {
            self.inspect_original_job(&self.node, &job_offer)?
        } else {
            match self.call_rpc(
                caller,
                &DfsSourceAdoptionRequest::InspectOriginalJob {
                    offer: job_offer.clone(),
                },
            )? {
                DfsSourceAdoptionRpcResponse::OriginalJob(receipt) => receipt,
                _ => {
                    return Err(error(
                        "source binding received another original-job response kind",
                    ));
                }
            }
        };
        if receipt.offer != job_offer || receipt.job != pending.plan.job {
            return Err(error(
                "source binding lacks the byte-identical actual held Running job",
            ));
        }
        let mut state = pending
            .state
            .lock()
            .map_err(|_| error("source preparation state poisoned"))?;
        let capture = match &*state {
            SourcePreparationState::Bound(existing) => {
                if existing.offer.job_offer != job_offer {
                    return Err(error("source binding retry changed coordinator custody"));
                }
                return Ok(existing.clone());
            }
            SourcePreparationState::Terminalizing(_) => return Err(terminalization_busy()),
            SourcePreparationState::Revoked { .. } => {
                return Err(error(
                    "source preparation was atomically revoked before binding",
                ));
            }
            SourcePreparationState::Unbound(capture) => capture.clone(),
        };
        let family = capture.family().clone();
        let offer = self.register_source_capture(
            capture,
            pending.plan.source_config.clone(),
            pending.plan.target_config.clone(),
            pending.plan.transfer_id.clone(),
            pending.plan.destination.clone(),
            job_offer,
        )?;
        let prepared = PreparedSourceAdoption { offer, family };
        *state = SourcePreparationState::Bound(prepared.clone());
        // The retained SourceSession now owns the same native writer. Keep the
        // small exact retry receipt without a second writer or owner lease.
        RETAINED
            .get_or_init(Default::default)
            .lock()
            .map_err(|_| error("source custody registry poisoned"))?
            .remove(&(
                self.data_root.clone(),
                format!("preparation:{}", token.request_id),
            ));
        Ok(prepared)
    }

    pub(crate) fn prepare_cross_node_source(
        self: &Arc<Self>,
        plan: SourcePreparationPlan,
        deadline: Option<std::time::Instant>,
    ) -> Result<PreparedSourceAdoption> {
        if plan.job_config.primary_node_id != self.node {
            return Err(error("source phase is not at its actual coordinator"));
        }
        match self.call_rpc_before(
            &plan.source_config.primary_node_id,
            &DfsSourceAdoptionRequest::InspectSourcePreparation { plan: plan.clone() },
            deadline,
        )? {
            DfsSourceAdoptionRpcResponse::SourcePreparationInspected {
                prepared: Some(prepared),
            } => return Ok(prepared),
            DfsSourceAdoptionRpcResponse::SourcePreparationInspected { prepared: None } => {}
            _ => {
                return Err(error(
                    "source preparation inspection returned another native response kind",
                ));
            }
        }
        let acquire_source = || match self.call_rpc_before(
            &plan.source_config.primary_node_id,
            &DfsSourceAdoptionRequest::AcquireSource { plan: plan.clone() },
            deadline,
        )? {
            DfsSourceAdoptionRpcResponse::SourcePrepared(token) => Ok(token),
            _ => Err(error(
                "source capture returned another native response kind",
            )),
        };
        // Node identities order inter-node acquisition. Same-node acquisition
        // uses the native backend's canonical physical-root ordering instead.
        let first_source = plan.source_config.primary_node_id < self.node;
        let early_source = if first_source {
            Some(acquire_source()?)
        } else {
            None
        };
        #[cfg(any(test, feature = "test-support"))]
        if let Some(token) = early_source.as_ref() {
            crate::source_adoption_test::capture_preparation_and_pause(
                &self.data_root,
                crate::source_adoption_test::DfsSourcePausePoint::SourceAcquired,
                &plan,
                token,
                None,
                &self.runtime,
            );
        }
        let capture_job = || -> Result<DfsSourceAdoptionJobOffer> {
            let existing_job = self
                .job_sessions
                .lock()
                .map_err(|_| error("original jobs poisoned"))?
                .values()
                .find(|session| {
                    session.capture.job() == &plan.job
                        && session.offer.job_config == plan.job_config
                        && session.source == plan.source_config
                        && session.target == plan.target_config
                        && session.destination == plan.destination
                        && session.source_path == plan.source_path
                })
                .map(|session| session.offer.clone());
            if let Some(offer) = existing_job {
                self.inspect_original_job(&plan.source_config.primary_node_id, &offer)?;
                Ok(offer)
            } else {
                let root = crate::default_dfs_metadata_shards_root(
                    &sinomo_core::local::SinomoDataDir::new(&self.data_root),
                )
                .join(crate::metadata_keys::key_segment(&plan.job_config.shard_id));
                let mut backend = crate::LocalSobjMetadataBackend::new(
                    crate::DfsMetadataShardId::new(&plan.job_config.shard_id),
                    root,
                )?;
                self.rebind_capture_backend(&mut backend, &plan.job_config)?;
                let capture = backend.capture_source_adoption_job(&plan.job.job_id)?;
                if capture.job() != &plan.job {
                    return Err(error(
                        "coordinator Running job changed while its real writer queued",
                    ));
                }
                self.register_original_job(
                    capture,
                    plan.job_config.clone(),
                    plan.source_config.clone(),
                    plan.target_config.clone(),
                    plan.destination.clone(),
                    plan.source_path.clone(),
                )
            }
        };
        let job_offer = match capture_job() {
            Ok(offer) => offer,
            Err(capture_error) => {
                let Some(token) = early_source.as_ref() else {
                    return Err(capture_error);
                };
                // No target was dispatched by this phase. Binding and this
                // exact-token revoke share one source-side state lock. A
                // concurrent completed bind remains resumable and protected.
                return match self.call_rpc_before(
                    &plan.source_config.primary_node_id,
                    &DfsSourceAdoptionRequest::RevokeSourcePreparation {
                        token: token.clone(),
                    },
                    deadline,
                )? {
                    DfsSourceAdoptionRpcResponse::SourcePreparationRevoked(
                        SourcePreparationRevocation::Revoked(actual),
                    ) if actual == *token => Err(capture_error),
                    DfsSourceAdoptionRpcResponse::SourcePreparationRevoked(
                        SourcePreparationRevocation::Bound(prepared),
                    ) if prepared.offer.destination == plan.destination
                        && prepared.offer.source_config == plan.source_config
                        && prepared.offer.target_config == plan.target_config
                        && prepared.offer.transfer_id == plan.transfer_id
                        && prepared.offer.job_offer.job_config == plan.job_config
                        && prepared.offer.job_offer.coordinator_node_id == self.node
                        && prepared.offer.source_node_id == token.source_node_id
                        && prepared.offer.object_id == plan.destination.object_id
                        && prepared.offer.job_id == plan.job.job_id
                        && prepared.family.source_journal_sequence
                            == token.prefix.journal_sequence =>
                    {
                        Ok(prepared)
                    }
                    _ => Err(error(
                        "source revocation changed the exact capture identity",
                    )),
                };
            }
        };
        let token = match early_source {
            Some(token) => token,
            None => acquire_source()?,
        };
        #[cfg(any(test, feature = "test-support"))]
        crate::source_adoption_test::capture_preparation_and_pause(
            &self.data_root,
            crate::source_adoption_test::DfsSourcePausePoint::BeforeBind,
            &plan,
            &token,
            Some(&job_offer),
            &self.runtime,
        );
        let binding = self.call_rpc_before(
            &plan.source_config.primary_node_id,
            &DfsSourceAdoptionRequest::BindSource {
                token: token.clone(),
                job_offer: job_offer.clone(),
            },
            deadline,
        );
        let reply = match binding {
            Ok(reply) => reply,
            Err(binding_error) => {
                // No target dispatch follows an unavailable Bind result. The
                // source's atomic token state determines whether real custody
                // remains bound, is known unbound, or must stay uncertain.
                return match self.call_rpc_before(
                    &plan.source_config.primary_node_id,
                    &DfsSourceAdoptionRequest::RevokeSourcePreparation {
                        token: token.clone(),
                    },
                    deadline,
                )? {
                    DfsSourceAdoptionRpcResponse::SourcePreparationRevoked(
                        SourcePreparationRevocation::Revoked(actual),
                    ) if actual == token => {
                        self.release_unbound_original_job(&job_offer, &plan)?;
                        Err(binding_error)
                    }
                    DfsSourceAdoptionRpcResponse::SourcePreparationRevoked(
                        SourcePreparationRevocation::Bound(prepared),
                    ) if prepared.offer.job_offer == job_offer
                        && prepared.offer.source_node_id == token.source_node_id
                        && prepared.offer.source_config == plan.source_config
                        && prepared.offer.target_config == plan.target_config
                        && prepared.offer.destination == plan.destination
                        && prepared.offer.transfer_id == plan.transfer_id
                        && prepared.family.source_journal_sequence
                            == token.prefix.journal_sequence =>
                    {
                        Ok(prepared)
                    }
                    _ => Err(error(
                        "bind recovery changed exact native source/job custody",
                    )),
                };
            }
        };
        match reply {
            DfsSourceAdoptionRpcResponse::SourceBound(prepared) => {
                if prepared.offer.destination != plan.destination
                    || prepared.offer.source_config != plan.source_config
                    || prepared.offer.target_config != plan.target_config
                    || prepared.offer.transfer_id != plan.transfer_id
                {
                    return Err(error(
                        "source binding response changed the exact phase plan",
                    ));
                }
                Ok(prepared)
            }
            _ => Err(error(
                "source binding returned another native response kind",
            )),
        }
    }
}

#[cfg(test)]
mod genuine_source_first_tests {
    use super::*;
    use std::time::Duration;

    fn source_first_local_owned_options(
        native: &crate::test_support::DfsTestRuntime,
        nested: &DfsMetadataShardConfig,
        directory: &crate::DfsStat,
    ) -> crate::DfsWriteOptions {
        let inspector = native.metadata_primary_service(&directory.path).unwrap();
        let object = inspector
            .metadata_backend()
            .expect("actual owning shard backend")
            .get_object(&directory.object_id)
            .unwrap()
            .expect("the actual declared genesis directory");
        assert_eq!(object.file_system_id, nested.file_system_id);
        assert_eq!(object.path, directory.path);
        assert_eq!(object.kind, crate::DfsObjectKind::Directory);
        assert_eq!(object.policy, sinomo_core::DfsObjectPolicy::LocalOwned);
        assert_eq!(object.owner_node_id, nested.primary_node_id);
        assert_eq!(object.current_locations, vec![object.owner_node_id.clone()]);
        assert_eq!(directory.policy, object.policy);
        assert_eq!(directory.current_locations, object.current_locations);
        // This is the stored content owner, not the later Root coordinator.
        // Normal LocalOwned validation and durable commit remain enabled.
        crate::DfsWriteOptions {
            policy: crate::DfsReplicationPolicy::new(1, 1).unwrap(),
            replica_candidates: vec![object.owner_node_id],
            wait_for_commit: true,
            foreground_replica_repair: true,
        }
    }

    // Keep this fixture's source-before-job order explicit. Native default
    // reconciliation preserves the activated assignment, so constructor node
    // order alone does not select the actual Root coordinator.
    fn source_first_coordinator_and_target(
        native: &crate::test_support::DfsTestRuntime,
        nested: &DfsMetadataShardConfig,
        nodes: &[NodeId],
    ) -> (NodeId, NodeId) {
        let configs = native.metadata_shards().unwrap();
        let root = configs
            .iter()
            .find(|cfg| cfg.shard_id == "root")
            .unwrap()
            .clone();
        let activated = if nested.primary_node_id < root.primary_node_id {
            root
        } else {
            let primary = nodes
                .iter()
                .find(|node| **node > nested.primary_node_id)
                .expect("a real later node for the source-first coordinator")
                .clone();
            let replicas = std::iter::once(root.primary_node_id.clone())
                .chain(root.replica_node_ids.iter().cloned())
                .filter(|node| *node != primary)
                .collect();
            native
                .reassign_metadata_shard(&root.file_system_id, &root.shard_id, primary, replicas)
                .unwrap()
        };
        let configs = native.metadata_shards().unwrap();
        assert_eq!(
            configs.iter().find(|cfg| cfg.shard_id == "root"),
            Some(&activated),
            "the genuine primary handoff must have activated before the operation"
        );
        assert_eq!(
            configs.iter().find(|cfg| cfg.shard_id == nested.shard_id),
            Some(nested),
            "the original nested source assignment stays exact"
        );
        assert!(nested.primary_node_id < activated.primary_node_id);
        let mut progress = native.client(&activated.primary_node_id).unwrap();
        progress.posix_mkdir("/source-capture-progress").unwrap();
        drop(progress);
        let target = configs
            .iter()
            .find(|cfg| cfg.shard_id == "users")
            .unwrap()
            .primary_node_id
            .clone();
        (activated.primary_node_id, target)
    }

    #[test]
    fn actual_source_first_bound_offer_survives_exact_revocation() {
        let name = "native-source-first-bind-revoke";
        let nodes = crate::test_support::DfsTestRuntime::ordered_node_ids(name, 3);
        let mut native = crate::test_support::DfsTestRuntime::new(name, nodes[0].clone()).unwrap();
        for node in &nodes[1..] {
            native.define_node(node.clone()).unwrap();
        }
        native.start_nodes().unwrap();
        // The normal native genesis already defines this real node's parent
        // and files directory. Declare the empty physical source shard over
        // that exact genesis root; never create a directory/parent across two
        // physical writers or copy a pre-existing file into the new shard.
        let from_path = crate::sifs_node_path(&nodes[0]);
        let source_path = crate::sifs_node_files_path(&nodes[0]);
        let nested = native
            .bootstrap_empty_metadata_prefix("native-deep-source", &source_path)
            .unwrap();
        let mut seed = native.client(&nodes[0]).unwrap();
        let from = from_path.as_str();
        let to = "/users/source-first";
        let source_directory = seed.posix_stat_distributed(&source_path).unwrap();
        assert_eq!(source_directory.kind, crate::DfsObjectKind::Directory);
        assert_eq!(source_directory.path, source_path);
        let original = seed
            .posix_write_file(
                &format!("{from}/files/child.bin"),
                b"actual source-first bytes",
                source_first_local_owned_options(&native, &nested, &source_directory),
            )
            .unwrap();
        assert_eq!(original.policy, sinomo_core::DfsObjectPolicy::LocalOwned);
        assert_eq!(
            original.current_locations,
            source_directory.current_locations
        );
        assert_eq!(
            seed.posix_read_file_distributed(&format!("{from}/files/child.bin"))
                .unwrap(),
            b"actual source-first bytes"
        );
        assert_eq!(
            seed.posix_stat_distributed(&source_path).unwrap().object_id,
            source_directory.object_id
        );
        drop(seed);
        let (coordinator, target) = source_first_coordinator_and_target(&native, &nested, &nodes);
        let _diagnostic = crate::native_boundary_diagnostics::capture(
            native
                .data_dir(&coordinator)
                .root()
                .parent()
                .unwrap()
                .to_path_buf(),
        );
        assert!(nested.primary_node_id < coordinator);
        let mut requester = native.client(&nodes[0]).unwrap();
        let source_pause = native
            .pause_source_preparation(
                &coordinator,
                crate::source_adoption_test::DfsSourcePausePoint::SourceAcquired,
            )
            .unwrap();
        let bind_pause = native
            .pause_source_preparation(
                &coordinator,
                crate::source_adoption_test::DfsSourcePausePoint::BeforeBind,
            )
            .unwrap();
        std::thread::scope(|scope| {
            // A step that fails while the rename is paused releases it.
            let _released = (source_pause.release_on_drop(), bind_pause.release_on_drop());
            let foreground = scope.spawn(move || {
                let result = requester.posix_rename(from, to);
                if result.is_err() {
                    eprintln!("SOURCE_FIRST_OPERATION_RESULT from={from} result={result:?}");
                }
                result
            });
            let captured = source_pause
                .wait_preparation(Duration::from_secs(5))
                .unwrap();
            assert_eq!(captured.plan.source_config, nested);
            assert_eq!(captured.plan.job_config.primary_node_id, coordinator);
            assert_eq!(
                captured.plan.destination.object_id,
                source_directory.object_id
            );
            assert!(
                captured.job_offer.is_none(),
                "source was acquired before actual job writer"
            );
            source_pause.release();
            let held = bind_pause.wait_preparation(Duration::from_secs(5)).unwrap();
            assert_eq!(held.token, captured.token);
            let job_offer = held
                .job_offer
                .expect("actual stored job captured before bind");
            let call = |request: &DfsSourceAdoptionRequest| {
                let rpc = sinomo_core::network::ObjectRpcRequest::encode_body(
                    SOURCE_ADOPTION_ROUTE,
                    crate::DFS_RPC_CATEGORY,
                    request,
                )
                .unwrap();
                native
                    .rpc_request(
                        &coordinator,
                        &nested.primary_node_id,
                        rpc,
                        Duration::from_secs(5),
                    )
                    .unwrap()
                    .decode_body::<DfsSourceAdoptionRpcResponse>()
                    .unwrap()
            };
            let bound = call(&DfsSourceAdoptionRequest::BindSource {
                token: held.token.clone(),
                job_offer,
            });
            let DfsSourceAdoptionRpcResponse::SourceBound(prepared) = bound else {
                panic!("genuine actor binding");
            };
            assert_eq!(
                prepared.family.source_object.object_id,
                source_directory.object_id
            );
            assert_eq!(prepared.offer.target_config.primary_node_id, target);
            let actor = actor_for_root(&native.data_dir(&coordinator).root()).unwrap();
            let origin = held.plan.job.origin.as_ref().expect("original request");
            let mut command = crate::DfsNamespaceCommand {
                storage_revision: None,
                request_id: origin.request_id.clone(),
                file_system_id: held.plan.job.file_system_id.clone(),
                requesting_node_id: origin.requester_node_id.clone(),
                include_metadata_journal_batches: false,
                metadata_journal_after_sequences: Vec::new(),
                authority_shards: Vec::new(),
                operation: crate::DfsNamespaceOperation::Rename {
                    from: from.into(), to: to.into(),
                },
            };
            // The job custody validates the resumed request; it holds row
            // fences, not a shard writer to lend.
            assert!(actor.ingress_job_writer_custody(
                &prepared.offer, &command, &held.plan.job_config,
            ).unwrap().is_none());
            command.request_id.push_str("-unrelated");
            assert!(actor.ingress_job_writer_custody(
                &prepared.offer, &command, &held.plan.job_config,
            ).is_err());
            command.request_id = origin.request_id.clone();
            let mut wrong_epoch = held.plan.job_config.clone();
            wrong_epoch.epoch += 1;
            assert!(actor.ingress_job_writer_custody(
                &prepared.offer, &command, &wrong_epoch,
            ).is_err());
            let revoked =
                call(&DfsSourceAdoptionRequest::RevokeSourcePreparation { token: held.token });
            assert!(
                matches!(revoked, DfsSourceAdoptionRpcResponse::SourcePreparationRevoked(SourcePreparationRevocation::Bound(actual)) if actual.offer == prepared.offer)
            );
            bind_pause.release();
            foreground.join().unwrap().unwrap();
        });
        drop(source_pause);
        drop(bind_pause);
        let mut reader = native.client(&nodes[2]).unwrap();
        assert_eq!(
            reader
                .posix_stat_distributed(&format!("{to}/files/child.bin"))
                .unwrap()
                .object_id,
            original.object_id
        );
        assert_eq!(
            reader
                .posix_read_file_distributed(&format!("{to}/files/child.bin"))
                .unwrap(),
            b"actual source-first bytes"
        );
        drop(reader);
        let observations = native.rpc_observations();
        let source_calls = observations
            .iter()
            .filter(|event| {
                event.origin == crate::test_runtime::RpcOrigin::RegisteredHost
                    && event.phase == crate::test_runtime::RpcPhase::BeforeDispatch
                    && (event.route == SOURCE_ADOPTION_ROUTE
                        || event.route == "/dfs/data/namespace")
            })
            .collect::<Vec<_>>();
        assert!(
            source_calls
                .iter()
                .any(|event| event.route == SOURCE_ADOPTION_ROUTE)
        );
        assert!(
            source_calls
                .iter()
                .all(|event| event.options.destination_process_affinity()
                    == sinomo_core::network::DestinationProcessAffinity::MainDaemon),
            "source custody and target publication must reach Main when a node also has a shadow"
        );
        assert_eq!(native.observation_dropped_count(), 0);
        native.close().unwrap();
    }
    #[test]
    fn actual_source_first_revoked_bind_releases_job_writer_and_retries() {
        let name = "native-source-first-revoke-wins";
        let nodes = crate::test_support::DfsTestRuntime::ordered_node_ids(name, 3);
        let mut native = crate::test_support::DfsTestRuntime::new(name, nodes[0].clone()).unwrap();
        for node in &nodes[1..] {
            native.define_node(node.clone()).unwrap();
        }
        native.start_nodes().unwrap();
        // The normal native genesis already defines this real node's parent
        // and files directory. Declare the empty physical source shard over
        // that exact genesis root; never create a directory/parent across two
        // physical writers or copy a pre-existing file into the new shard.
        let from_path = crate::sifs_node_path(&nodes[0]);
        let source_path = crate::sifs_node_files_path(&nodes[0]);
        let nested = native
            .bootstrap_empty_metadata_prefix("native-deep-source", &source_path)
            .unwrap();
        let mut seed = native.client(&nodes[0]).unwrap();
        let from = from_path.as_str();
        let to = "/users/source-first";
        let source_directory = seed.posix_stat_distributed(&source_path).unwrap();
        assert_eq!(source_directory.kind, crate::DfsObjectKind::Directory);
        assert_eq!(source_directory.path, source_path);
        let original = seed
            .posix_write_file(
                &format!("{from}/files/child.bin"),
                b"actual source-first bytes",
                source_first_local_owned_options(&native, &nested, &source_directory),
            )
            .unwrap();
        assert_eq!(original.policy, sinomo_core::DfsObjectPolicy::LocalOwned);
        assert_eq!(
            original.current_locations,
            source_directory.current_locations
        );
        assert_eq!(
            seed.posix_read_file_distributed(&format!("{from}/files/child.bin"))
                .unwrap(),
            b"actual source-first bytes"
        );
        assert_eq!(
            seed.posix_stat_distributed(&source_path).unwrap().object_id,
            source_directory.object_id
        );
        drop(seed);
        let (coordinator, target) = source_first_coordinator_and_target(&native, &nested, &nodes);
        assert!(nested.primary_node_id < coordinator);
        // Construct observer clients before pinning the coordinator admission.
        // Their normal startup/open path is outside the read-only RPC boundary.
        let mut coordinator_reader = native.client(&coordinator).unwrap();
        let outsider = nodes
            .iter()
            .find(|node| **node != coordinator && **node != nested.primary_node_id)
            .unwrap();
        let mut unrelated = native.client(outsider).unwrap();
        let mut requester = native.single_attempt_client(&nodes[0]).unwrap();
        let source_pause = native
            .pause_source_preparation(
                &coordinator,
                crate::source_adoption_test::DfsSourcePausePoint::SourceAcquired,
            )
            .unwrap();
        let bind_pause = native
            .pause_source_preparation(
                &coordinator,
                crate::source_adoption_test::DfsSourcePausePoint::BeforeBind,
            )
            .unwrap();
        let operation = sinomo_core::Id::generate().to_string();
        let (captured_job, captured_source, read_scope) = std::thread::scope(|scope| {
            // A step that fails while the rename is paused releases it.
            let _released = (source_pause.release_on_drop(), bind_pause.release_on_drop());
            let op = operation.clone();
            let foreground = scope.spawn(move || {
                let result = requester.posix_rename_with_operation_id(from, to, &op);
                if result.is_err() {
                    eprintln!(
                        "SOURCE_FIRST_OPERATION_RESULT from={from} op={op} result={result:?}"
                    );
                }
                result
            });
            let captured = source_pause
                .wait_preparation(Duration::from_secs(5))
                .unwrap();
            assert_eq!(captured.plan.source_config, nested);
            assert_eq!(captured.plan.job_config.primary_node_id, coordinator);
            assert_eq!(
                captured.plan.destination.object_id,
                source_directory.object_id
            );
            assert!(
                captured.job_offer.is_none(),
                "source was acquired before actual job writer"
            );
            source_pause.release();
            let held = bind_pause.wait_preparation(Duration::from_secs(5)).unwrap();
            assert_eq!(held.token, captured.token);
            let job_offer = held
                .job_offer
                .expect("actual stored job captured before bind");
            let call = |request: &DfsSourceAdoptionRequest| {
                let rpc = sinomo_core::network::ObjectRpcRequest::encode_body(
                    SOURCE_ADOPTION_ROUTE,
                    crate::DFS_RPC_CATEGORY,
                    request,
                )
                .unwrap();
                native
                    .rpc_request(
                        &coordinator,
                        &nested.primary_node_id,
                        rpc,
                        Duration::from_secs(5),
                    )
                    .unwrap()
                    .decode_body::<DfsSourceAdoptionRpcResponse>()
                    .unwrap()
            };
            let revoked = call(&DfsSourceAdoptionRequest::RevokeSourcePreparation {
                token: held.token.clone(),
            });
            assert!(
                matches!(revoked, DfsSourceAdoptionRpcResponse::SourcePreparationRevoked(SourcePreparationRevocation::Revoked(actual)) if actual == held.token)
            );
            // Pin the ordinary coordinator admission before releasing its
            // rejected source phase. Read-only peer queries remain available;
            // a separate maintenance pass cannot finish the job during these
            // input/authorization observations.
            let scheduler =
                crate::runtime_host::daemon_dfs_mutation_scheduler(native.data_dir(&coordinator))
                    .unwrap();
            let admission = native.runtime_handle().block_on(async {
                tokio::time::timeout(
                    Duration::from_secs(5),
                    scheduler.admit(&crate::DfsMessageExecutionScope::GlobalExclusive),
                )
                .await
                .map_err(|_| "native coordinator admission deadline".to_string())?
                .map_err(|error| error.to_string())
            });
            let read_scope = match admission {
                Ok(scope) => scope,
                Err(error) => {
                    bind_pause.release();
                    let outcome = foreground.join();
                    panic!("native coordinator scope: {error}; drained={outcome:?}");
                }
            };
            bind_pause.release();
            assert!(foreground.join().unwrap().is_err());
            let coordinator_actor = actor_for_root(&native.data_dir(&coordinator).root()).unwrap();
            assert!(
                coordinator_actor
                    .original_job_session(&job_offer, false)
                    .is_err(),
                "known revoked bind must release exact coordinator job writer"
            );
            (
                captured.plan.job,
                captured.plan.expected_source.unwrap(),
                read_scope,
            )
        });
        drop(source_pause);
        drop(bind_pause);
        // Rebuild only restrictive guard DATA from this real replica's
        // committed Running job, through the production projection helpers.
        let root_shard = crate::DfsMetadataShardId::new("root");
        let root_config = native
            .metadata_shards()
            .unwrap()
            .into_iter()
            .find(|config| config.shard_id == root_shard.0)
            .expect("root metadata assignment");
        let source_inspector = native
            .local_shard_copy_service(&nested.primary_node_id, &root_config)
            .unwrap();
        let guards = source_inspector
            .replica_namespace_guard_projection(&root_shard)
            .unwrap();
        assert!(
            guards
                .iter()
                .any(|(id, fs, paths)| id == &captured_job.job_id
                    && fs == &captured_job.file_system_id
                    && paths.contains(&captured_job.root_path))
        );
        drop(source_inspector);
        let cache_slot = crate::runtime_host::daemon_dfs_message_service_cache()
            .root(&native.data_dir(&nested.primary_node_id).root())
            .unwrap();
        let registry = cache_slot
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .cross_shard_namespace_locks
            .clone();
        registry.replace_shard_jobs(&root_shard, guards);
        assert!(registry.blocks(&nested.file_system_id, &source_path));
        eprintln!(
            "NATIVE_CURRENT_SOURCE_JOB_GUARD job={} registry={registry:?}",
            captured_job.job_id
        );
        let source_read = crate::DfsMetadataSelection::DirectoryPageForNamespaceJob {
            job: Box::new(captured_job.clone()),
            path: source_path.clone(),
            after: None,
            limit: 64,
        };

        let public = coordinator_reader.query_metadata_authority_impl(
            &nested,
            crate::DfsMetadataSelection::DirectoryPage {
                path: source_path.clone(),
                after: None,
                limit: 64,
            },
        );
        assert!(
            public
                .unwrap_err()
                .to_string()
                .contains("moving between metadata shards")
        );

        let denied = unrelated.query_metadata_authority_impl(&nested, source_read.clone());
        assert!(
            denied
                .unwrap_err()
                .to_string()
                .contains("authenticated current coordinator")
        );
        let mut wrong_job = captured_job.clone();
        wrong_job.job_id.push_str("-other-job-negative-control");
        let denied = coordinator_reader.query_metadata_authority_impl(
            &nested,
            crate::DfsMetadataSelection::DirectoryPageForNamespaceJob {
                job: Box::new(wrong_job),
                path: source_path.clone(),
                after: None,
                limit: 64,
            },
        );
        assert!(
            denied
                .unwrap_err()
                .to_string()
                .contains("moving between metadata shards")
        );
        let denied = coordinator_reader.query_metadata_authority_impl(
            &nested,
            crate::DfsMetadataSelection::DirectoryPageForNamespaceJob {
                job: Box::new(captured_job.clone()),
                path: "/outside-recorded-frame".into(),
                after: None,
                limit: 64,
            },
        );
        assert!(
            denied
                .unwrap_err()
                .to_string()
                .contains("recorded job cursor")
        );
        let mut outside_job = captured_job.clone();
        outside_job
            .pending_frames
            .push(crate::DfsNamespaceJobFrame::CrossShardDirScan {
                source_path: "/outside-recorded-frame".into(),
                target_path: "/users/outside".into(),
                cursor: None,
            });
        let denied = coordinator_reader.query_metadata_authority_impl(
            &nested,
            crate::DfsMetadataSelection::DirectoryPageForNamespaceJob {
                job: Box::new(outside_job),
                path: "/outside-recorded-frame".into(),
                after: None,
                limit: 64,
            },
        );
        assert!(
            denied
                .unwrap_err()
                .to_string()
                .contains("valid original Running rename job")
        );
        // A directory page is read from every shard that owns a child of the
        // directory. The nested shard's root is a child of the job root, so
        // the nested shard answers that page; a directory none of whose
        // children it owns is refused.
        let mut child_owner = captured_job.clone();
        child_owner
            .pending_frames
            .push(crate::DfsNamespaceJobFrame::CrossShardDirScan {
                source_path: captured_job.root_path.clone(),
                target_path: "/users/source-first".into(),
                cursor: None,
            });
        let answered = coordinator_reader.query_metadata_authority_impl(
            &nested,
            crate::DfsMetadataSelection::DirectoryPageForNamespaceJob {
                job: Box::new(child_owner.clone()),
                path: captured_job.root_path.clone(),
                after: None,
                limit: 64,
            },
        );
        assert!(matches!(
            &answered,
            Ok(crate::DfsMetadataQueryResult::DirectoryPage { entries, .. })
                if entries.iter().any(|(object, _)| object.path == source_path)
        ));
        let elsewhere = format!("{}/elsewhere", captured_job.root_path);
        let mut wrong_source_shard = captured_job.clone();
        wrong_source_shard
            .pending_frames
            .push(crate::DfsNamespaceJobFrame::CrossShardDirScan {
                source_path: elsewhere.clone(),
                target_path: "/users/source-first/elsewhere".into(),
                cursor: None,
            });
        let denied = coordinator_reader.query_metadata_authority_impl(
            &nested,
            crate::DfsMetadataSelection::DirectoryPageForNamespaceJob {
                job: Box::new(wrong_source_shard),
                path: elsewhere,
                after: None,
                limit: 64,
            },
        );
        assert!(
            denied
                .unwrap_err()
                .to_string()
                .contains("current queried shard")
        );
        // Each shard's part of the recorded cursor: a shard before a tagged
        // cursor's shard has finished its page and is refused; the cursor's
        // own shard is queried after the cursor without its shard; a cursor
        // without a shard is the directory owner's page alone.
        let root_shard = crate::DfsMetadataShardId::new("root");
        assert!(crate::DfsMetadataShardId::new(nested.shard_id.clone()) < root_shard);
        let Ok(crate::DfsMetadataQueryResult::DirectoryPage { entries, .. }) = answered else {
            unreachable!("matched above");
        };
        let (_, nested_cursor) = entries.last().unwrap().clone();
        let nested_shard = crate::DfsMetadataShardId::new(nested.shard_id.clone());
        let tagged = |shard| {
            crate::metadata_keys::directory_page_cursor_with_shard(nested_cursor.clone(), shard)
        };
        for (recorded, after, refusal) in [
            (tagged(&root_shard), None, Some("recorded job cursor")),
            (tagged(&nested_shard), None, Some("recorded job cursor")),
            (tagged(&nested_shard), Some(nested_cursor.clone()), None),
            (
                nested_cursor.clone(),
                Some(nested_cursor.clone()),
                Some("current queried shard"),
            ),
        ] {
            let mut resumed = captured_job.clone();
            resumed
                .pending_frames
                .push(crate::DfsNamespaceJobFrame::CrossShardDirScan {
                    source_path: captured_job.root_path.clone(),
                    target_path: "/users/source-first".into(),
                    cursor: Some(recorded),
                });
            let result = coordinator_reader.query_metadata_authority_impl(
                &nested,
                crate::DfsMetadataSelection::DirectoryPageForNamespaceJob {
                    job: Box::new(resumed),
                    path: captured_job.root_path.clone(),
                    after,
                    limit: 64,
                },
            );
            match refusal {
                None => assert!(result.is_ok(), "{result:?}"),
                Some(refusal) => assert!(
                    result
                        .as_ref()
                        .is_err_and(|error| error.to_string().contains(refusal)),
                    "{result:?}"
                ),
            }
        }
        // A separate restrictive guard can never be borrowed as this job's
        // authority. This negative-only fault is removed by its exact key.
        registry.upsert_job(
            Some(root_shard.clone()),
            "negative-other-job".into(),
            nested.file_system_id.clone(),
            source_path.clone(),
        );
        let denied = coordinator_reader.query_metadata_authority_impl(&nested, source_read.clone());
        let absent_read = crate::DfsMetadataSelection::CurrentObjectForNamespaceJob {
            job: Box::new(captured_job.clone()),
            source_path: format!("{source_path}/absent-source-read"),
            object_id: sinomo_core::Id::generate(),
        };
        let absent_denied =
            coordinator_reader.query_metadata_authority_impl(&nested, absent_read.clone());
        registry.remove_job("negative-other-job");
        assert!(
            denied
                .unwrap_err()
                .to_string()
                .contains("moving between metadata shards")
        );
        assert!(
            absent_denied
                .unwrap_err()
                .to_string()
                .contains("moving between metadata shards"),
            "an absent source still respects another job's reservation"
        );
        assert!(matches!(
            coordinator_reader
                .query_metadata_authority_impl(&nested, absent_read)
                .unwrap(),
            crate::DfsMetadataQueryResult::CurrentObject { object: None }
        ));
        let page = coordinator_reader
            .query_metadata_authority_impl(&nested, source_read)
            .unwrap();
        let crate::DfsMetadataQueryResult::DirectoryPage { entries, after } = page else {
            panic!("actual source directory page")
        };
        assert!(after.is_none());
        assert_eq!(
            entries
                .iter()
                .map(|(object, _)| &object.object_id)
                .collect::<Vec<_>>(),
            vec![&original.object_id]
        );
        let current = coordinator_reader
            .query_metadata_authority_impl(
                &nested,
                crate::DfsMetadataSelection::CurrentObjectForNamespaceJob {
                    job: Box::new(captured_job.clone()),
                    source_path: source_path.clone(),
                    object_id: captured_source.object_id.clone(),
                },
            )
            .unwrap();
        let crate::DfsMetadataQueryResult::CurrentObject {
            object: Some(actual),
        } = current
        else {
            panic!("actual physical source")
        };
        assert_eq!(
            *actual, captured_source,
            "source preparation must retain the exact physical source body"
        );
        assert!(
            registry.blocks(&nested.file_system_id, &source_path),
            "coordinator reads cannot clear any reservation"
        );
        drop((coordinator_reader, unrelated, read_scope));
        let mut reader = native.client(&nodes[0]).unwrap();
        reader
            .posix_mkdir("/source-capture-progress/after")
            .unwrap();
        reader
            .posix_rename_with_operation_id(from, to, &operation)
            .unwrap();
        assert_eq!(
            reader
                .posix_stat_distributed(&format!("{to}/files/child.bin"))
                .unwrap()
                .object_id,
            original.object_id
        );
        assert_eq!(
            reader
                .posix_read_file_distributed(&format!("{to}/files/child.bin"))
                .unwrap(),
            b"actual source-first bytes"
        );
        drop(reader);
        drop((registry, cache_slot));
        assert_eq!(native.observation_dropped_count(), 0);
        native.close().unwrap();
    }
    #[test]
    fn actual_source_first_bind_revoke_race_preserves_winner_custody() {
        let name = "native-source-first-bind-revoke-race";
        let nodes = crate::test_support::DfsTestRuntime::ordered_node_ids(name, 3);
        let mut native = crate::test_support::DfsTestRuntime::new(name, nodes[0].clone()).unwrap();
        for node in &nodes[1..] {
            native.define_node(node.clone()).unwrap();
        }
        native.start_nodes().unwrap();
        // The normal native genesis already defines this real node's parent
        // and files directory. Declare the empty physical source shard over
        // that exact genesis root; never create a directory/parent across two
        // physical writers or copy a pre-existing file into the new shard.
        let from_path = crate::sifs_node_path(&nodes[0]);
        let source_path = crate::sifs_node_files_path(&nodes[0]);
        let nested = native
            .bootstrap_empty_metadata_prefix("native-deep-source", &source_path)
            .unwrap();
        let mut seed = native.client(&nodes[0]).unwrap();
        let from = from_path.as_str();
        let to = "/users/source-first";
        let source_directory = seed.posix_stat_distributed(&source_path).unwrap();
        assert_eq!(source_directory.kind, crate::DfsObjectKind::Directory);
        assert_eq!(source_directory.path, source_path);
        let original = seed
            .posix_write_file(
                &format!("{from}/files/child.bin"),
                b"actual source-first bytes",
                source_first_local_owned_options(&native, &nested, &source_directory),
            )
            .unwrap();
        assert_eq!(original.policy, sinomo_core::DfsObjectPolicy::LocalOwned);
        assert_eq!(
            original.current_locations,
            source_directory.current_locations
        );
        assert_eq!(
            seed.posix_read_file_distributed(&format!("{from}/files/child.bin"))
                .unwrap(),
            b"actual source-first bytes"
        );
        assert_eq!(
            seed.posix_stat_distributed(&source_path).unwrap().object_id,
            source_directory.object_id
        );
        drop(seed);
        let (coordinator, target) = source_first_coordinator_and_target(&native, &nested, &nodes);
        assert!(nested.primary_node_id < coordinator);
        let mut requester = native.single_attempt_client(&nodes[0]).unwrap();
        let source_pause = native
            .pause_source_preparation(
                &coordinator,
                crate::source_adoption_test::DfsSourcePausePoint::SourceAcquired,
            )
            .unwrap();
        let bind_pause = native
            .pause_source_preparation(
                &coordinator,
                crate::source_adoption_test::DfsSourcePausePoint::BeforeBind,
            )
            .unwrap();
        let operation = sinomo_core::Id::generate().to_string();
        let mut revoked_first = false;
        std::thread::scope(|scope| {
            // A step that fails while the rename is paused releases it.
            let _released = (source_pause.release_on_drop(), bind_pause.release_on_drop());
            let op = operation.clone();
            let foreground = scope.spawn(move || {
                let result = requester.posix_rename_with_operation_id(from, to, &op);
                if result.is_err() {
                    eprintln!(
                        "SOURCE_FIRST_OPERATION_RESULT from={from} op={op} result={result:?}"
                    );
                }
                result
            });
            let captured = source_pause
                .wait_preparation(Duration::from_secs(5))
                .unwrap();
            assert_eq!(captured.plan.source_config, nested);
            assert_eq!(captured.plan.job_config.primary_node_id, coordinator);
            assert_eq!(
                captured.plan.destination.object_id,
                source_directory.object_id
            );
            assert!(
                captured.job_offer.is_none(),
                "source was acquired before actual job writer"
            );
            source_pause.release();
            let held = bind_pause.wait_preparation(Duration::from_secs(5)).unwrap();
            assert_eq!(held.token, captured.token);
            let job_offer = held
                .job_offer
                .expect("actual stored job captured before bind");
            let bind_request = DfsSourceAdoptionRequest::BindSource {
                token: held.token.clone(),
                job_offer: job_offer.clone(),
            };
            let revoke_request = DfsSourceAdoptionRequest::RevokeSourcePreparation {
                token: held.token.clone(),
            };
            let coordinator_actor = actor_for_root(&native.data_dir(&coordinator).root()).unwrap();
            let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
            let (bound, revoked) = std::thread::scope(|race| {
                let first = barrier.clone();
                let second = barrier.clone();
                let binding_actor = coordinator_actor.clone();
                let revoking_actor = coordinator_actor.clone();
                let first_source = nested.primary_node_id.clone();
                let second_source = first_source.clone();
                let binding = race.spawn(move || {
                    first.wait();
                    binding_actor.call_rpc(&first_source, &bind_request)
                });
                let revoking = race.spawn(move || {
                    second.wait();
                    revoking_actor.call_rpc(&second_source, &revoke_request)
                });
                barrier.wait();
                (binding.join().unwrap(), revoking.join().unwrap())
            });
            let revoked = revoked.unwrap();
            match revoked {
                DfsSourceAdoptionRpcResponse::SourcePreparationRevoked(
                    SourcePreparationRevocation::Bound(actual),
                ) => {
                    let bound = bound.unwrap();
                    assert!(
                        matches!(bound, DfsSourceAdoptionRpcResponse::SourceBound(prepared) if prepared.offer == actual.offer)
                    );
                    revoked_first = false;
                }
                DfsSourceAdoptionRpcResponse::SourcePreparationRevoked(
                    SourcePreparationRevocation::Revoked(actual),
                ) => {
                    assert_eq!(actual, held.token);
                    assert!(bound.is_err());
                    revoked_first = true;
                }
                _ => panic!("actual atomic bind/revoke state"),
            }
            bind_pause.release();
            let foreground = foreground.join().unwrap();
            if !revoked_first {
                foreground.unwrap();
                return;
            }
            assert!(foreground.is_err());
            let coordinator_actor = actor_for_root(&native.data_dir(&coordinator).root()).unwrap();
            assert!(
                coordinator_actor
                    .original_job_session(&job_offer, false)
                    .is_err(),
                "known revoked bind must release exact coordinator job writer"
            );
        });
        drop(source_pause);
        drop(bind_pause);
        let mut reader = native.client(&nodes[0]).unwrap();
        if revoked_first {
            reader
                .posix_mkdir("/source-capture-progress/after")
                .unwrap();
            reader
                .posix_rename_with_operation_id(from, to, &operation)
                .unwrap();
        }
        assert_eq!(
            reader
                .posix_stat_distributed(&format!("{to}/files/child.bin"))
                .unwrap()
                .object_id,
            original.object_id
        );
        assert_eq!(
            reader
                .posix_read_file_distributed(&format!("{to}/files/child.bin"))
                .unwrap(),
            b"actual source-first bytes"
        );
        drop(reader);
        assert_eq!(native.observation_dropped_count(), 0);
        native.close().unwrap();
    }
}

#[cfg(test)]
mod actual_unbound_capture_tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn cancelled_actual_offer_allows_exact_unbound_capture_revocation() {
        let name = "native-cancelled-offer-unbound-revoke";
        let node = crate::test_support::DfsTestRuntime::ordered_node_ids(name, 1).remove(0);
        let mut native = crate::test_support::DfsTestRuntime::new(name, node.clone()).unwrap();
        native.start_nodes().unwrap();
        let mut requester = native.client(&node).unwrap();
        let from = "/services/native-unbound-source";
        let to = "/users/native-unbound-target";
        requester
            .posix_write_file(
                from,
                b"real captured bytes",
                crate::DfsWriteOptions::replicated_eventual(vec![node.clone()]),
            )
            .unwrap();
        let pause = native.pause_remote_namespace_phase(&node).unwrap();
        let mut source_client = native.client(&node).unwrap();
        let original_origin = std::thread::scope(|scope| {
            // A step that fails while the rename is paused releases it, so
            // the rename's thread ends and the failure is reported.
            let _released = pause.release_on_drop();
            let original = scope.spawn(move || requester.posix_rename(from, to));
            let captured = pause.wait_captured(Duration::from_secs(5)).unwrap();
            let crate::DfsNetworkMessage::NamespaceRequest(command) =
                &captured.routed_message.message
            else {
                panic!("actual source phase");
            };
            let crate::DfsNamespaceOperation::AdoptObjectAtPath {
                source_offer,
                source_path,
                ..
            } = &command.operation
            else {
                panic!("actual produced offer");
            };
            let actor = actor_for_root(&native.data_dir(&node).root()).unwrap();
            // Both rows come from the already-held actual native captures. No
            // serialized job/family creates permission in this test.
            let job = actor
                .inspect_original_job(&node, &source_offer.job_offer)
                .unwrap()
                .job;
            let expected = actor
                .source_session(source_offer, false)
                .unwrap()
                .capture
                .family()
                .source_object
                .clone();
            let call = |request: &DfsSourceAdoptionRequest| {
                let rpc = sinomo_core::network::ObjectRpcRequest::encode_body(
                    SOURCE_ADOPTION_ROUTE,
                    crate::DFS_RPC_CATEGORY,
                    request,
                )
                .unwrap();
                native.rpc_request(&node, &node, rpc, Duration::from_secs(5))
            };
            let original_pending = actor
                .source_preparations
                .lock()
                .unwrap()
                .values()
                .find(|pending| {
                    matches!(
                        &*pending.state.lock().unwrap(),
                        SourcePreparationState::Bound(prepared) if prepared.offer == *source_offer
                    )
                })
                .cloned()
                .expect("same actual native Bound source preparation");
            let original_plan = original_pending.plan.clone();
            let original_token = original_pending.token.clone();
            let mut unknown = source_offer.clone();
            unknown.session_id = sinomo_core::Id::generate().to_string();
            let no_effect = call(&DfsSourceAdoptionRequest::CancelUnclaimed {
                offer: unknown.clone(),
            })
            .unwrap()
            .decode_body::<DfsSourceAdoptionRpcResponse>()
            .unwrap();
            assert!(
                matches!(no_effect, DfsSourceAdoptionRpcResponse::SourceCancelled { offer } if offer == unknown)
            );
            assert!(
                matches!(&*original_pending.state.lock().unwrap(),
                SourcePreparationState::Bound(prepared) if prepared.offer == *source_offer),
                "idempotent unknown cancellation is no proof against this real Bound offer"
            );
            let cancelled = call(&DfsSourceAdoptionRequest::CancelUnclaimed {
                offer: source_offer.clone(),
            })
            .unwrap()
            .decode_body::<DfsSourceAdoptionRpcResponse>()
            .unwrap();
            assert!(
                matches!(cancelled, DfsSourceAdoptionRpcResponse::SourceCancelled { offer } if offer == *source_offer)
            );
            let cancelled_data = match &*original_pending.state.lock().unwrap() {
                SourcePreparationState::Revoked {
                    cancelled: Some(data),
                } => Arc::clone(data),
                _ => panic!("actual exact unclaimed retirement revokes its Bound memo"),
            };
            assert_eq!(cancelled_data.plan, original_plan);
            assert_eq!(cancelled_data.token, original_token);
            assert_eq!(cancelled_data.prepared.offer, *source_offer);
            assert_eq!(cancelled_data.plan.job, job);
            assert_eq!(cancelled_data.prepared.family.source_object, expected);
            let inspected = call(&DfsSourceAdoptionRequest::InspectSourcePreparation {
                plan: original_plan.clone(),
            })
            .unwrap()
            .decode_body::<DfsSourceAdoptionRpcResponse>()
            .unwrap();
            assert!(
                matches!(
                    inspected,
                    DfsSourceAdoptionRpcResponse::SourcePreparationInspected { prepared: None }
                ),
                "cancelled DATA is not a live offer or a committed adoption"
            );
            assert!(!original_plan.job.pending_frames.is_empty());
            let mut changed = original_plan.clone();
            changed.job.pending_frames.clear();
            assert!(
                matches!(validate_cancelled_preparation_plan(&cancelled_data, &changed),
                Err(MetadataBackendError::Serde(message)) if message == "source adoption custody: cancelled source retry changes original operation/body"),
                "changed original semantic frames fail the SAME production comparison"
            );
            let mut changed = original_plan.clone();
            changed.expected_source.as_mut().unwrap().version = crate::DfsVersion::generate();
            assert!(
                matches!(validate_cancelled_preparation_plan(&cancelled_data, &changed),
                Err(MetadataBackendError::Serde(message)) if message == "source adoption custody: cancelled source retry changes original operation/body"),
                "changed original source precondition cannot recapture under the old phase ID"
            );
            let mut changed_family = cancelled_data.prepared.family.clone();
            changed_family.source_object.version = crate::DfsVersion::generate();
            assert!(
                matches!(validate_cancelled_source_rows(&cancelled_data, &changed_family),
                Err(MetadataBackendError::Serde(message)) if message == "source adoption custody: fresh source capture changes cancelled original source rows")
            );
            // A genuine fresh native capture already owns its writer before
            // this reservation. Its existing relock must release that capture,
            // rather than rebind or wait on a terminalizer needing the writer.
            let registration = native
                .pause_source_preparation(
                    &node,
                    crate::source_adoption_test::DfsSourcePausePoint::BeforeSourceRegistration,
                )
                .unwrap();
            let mut current = native.metadata_primary_service(from).unwrap();
            let scope = crate::DfsPreparedOperationScope {
                file_system_id: original_plan.job.file_system_id.clone(),
                shard_id: crate::DfsMetadataShardId::new(&original_plan.job_config.shard_id),
                owner_epoch: original_plan.job_config.epoch,
                operation_id: "test.cancelled-terminalization-owned-scope".into(),
            };
            let ownership = current.acquire_prepared_execution(&scope).unwrap();
            let execution = current
                .publication_execution
                .current()
                .expect("actual native Core provider admission");
            let physical = current
                .metadata_backend()
                .unwrap()
                .publication_owner_scope(&scope.file_system_id, &scope.shard_id)
                .unwrap();
            let mut progressed = original_plan.job.clone();
            progressed.processed_object_count = 1;
            assert!(
                !actor
                    .finish_locally_cancelled_adoption(
                        &original_plan.request_id,
                        &progressed,
                        &original_plan.destination,
                        &original_plan.source_path,
                        &physical,
                        &execution,
                        || panic!("an earlier-effect job cannot enter terminal publication")
                    )
                    .unwrap()
            );
            assert!(
                !actor
                    .finish_locally_cancelled_adoption(
                        "missing-native-cancellation",
                        &original_plan.job,
                        &original_plan.destination,
                        &original_plan.source_path,
                        &physical,
                        &execution,
                        || panic!("absence cannot enter terminal publication")
                    )
                    .unwrap()
            );
            let mut directory = original_plan.destination.clone();
            directory.kind = crate::DfsObjectKind::Directory;
            assert!(
                !actor
                    .finish_locally_cancelled_adoption(
                        &original_plan.request_id,
                        &original_plan.job,
                        &directory,
                        &original_plan.source_path,
                        &physical,
                        &execution,
                        || panic!(
                            "a directory generation cannot enter single-file terminal publication"
                        )
                    )
                    .unwrap()
            );
            std::thread::scope(|race| {
                let actor = Arc::clone(&actor);
                let source = node.clone();
                let actual_plan = original_plan.clone();
                let mut capture =
                    Some(race.spawn(move || actor.acquire_remote_source(&source, actual_plan)));
                let held = registration.wait_preparation(Duration::from_secs(5));
                let result = if held.is_ok() {
                    actor_for_root(&native.data_dir(&node).root()).unwrap().finish_locally_cancelled_adoption(
                        &original_plan.request_id, &original_plan.job, &original_plan.destination,
                        &original_plan.source_path, &physical, &execution,
                        || {
                            registration.release();
                            let denied = capture.take().unwrap().join().unwrap();
                            assert!(matches!(denied, Err(MetadataBackendError::StatusUnavailable { reason })
                                if reason == "source preparation is settling its exact rejected adoption"));
                            let writable = current.metadata_backend.as_mut().unwrap()
                                .capture_source_adoption_source(&original_plan.destination.object_id,
                                    &original_plan.source_path).unwrap();
                            assert_eq!(writable.family().source_object, expected);
                            drop(writable);
                            Err(crate::DfsPosixError::Conflict {
                                path: from.into(), reason: "native failed-terminal-publication control".into(),
                            })
                        },
                    )
                } else {
                    Err(crate::DfsPosixError::Dfs(crate::DfsError::Metadata {
                        message: format!("native post-capture boundary unavailable: {held:?}"),
                    }))
                };
                registration.release();
                let uncaptured_outcome = capture.map(|capture| format!("{:?}", capture.join()));
                if held.is_err() {
                    std::fs::write(
                        native.data_dir(&node).root().join("boundary-diagnostic-cancelled-offer-registration.json"),
                        serde_json::to_vec_pretty(&serde_json::json!({
                            "plan": original_plan, "original_token": original_token,
                            "wait_result": format!("{held:?}"),
                            "actual_capture_thread_result_after_release": uncaptured_outcome,
                            "qualification": "Actual capture result retained after the unchanged wait failed and the existing registration gate was released; not evidence that the checkpoint passed."
                        })).unwrap(),
                    ).unwrap();
                    eprintln!("actual source registration wait={held:?}; capture after release={uncaptured_outcome:?}");
                }
                let held = held.expect("actual fresh writer reached post-capture registration");
                assert_eq!(held.plan, original_plan);
                assert_ne!(held.token.capture_id, original_token.capture_id);
                assert!(
                    matches!(result, Err(crate::DfsPosixError::Conflict { reason, .. })
                    if reason == "native failed-terminal-publication control")
                );
            });
            assert!(matches!(&*original_pending.state.lock().unwrap(),
                SourcePreparationState::Revoked { cancelled: Some(actual) }
                    if Arc::ptr_eq(actual, &cancelled_data)));
            assert_eq!(
                current
                    .metadata_backend()
                    .unwrap()
                    .get_namespace_job(&job.job_id)
                    .unwrap(),
                Some(job.clone())
            );
            let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                actor.finish_locally_cancelled_adoption(
                    &original_plan.request_id,
                    &original_plan.job,
                    &original_plan.destination,
                    &original_plan.source_path,
                    &physical,
                    &execution,
                    || panic!("native terminal reservation unwind control"),
                )
            }));
            assert!(unwind.is_err());
            assert!(matches!(&*original_pending.state.lock().unwrap(),
                SourcePreparationState::Revoked { cancelled: Some(actual) }
                    if Arc::ptr_eq(actual, &cancelled_data)));
            drop(execution);
            drop(ownership);
            drop(current);
            drop(registration);
            // This is an independent unbound-capture control, as in the
            // original native test. Mint its own ID once before its first RPC;
            // never replace the cancelled original move's same-phase memo.
            let mut plan = original_plan.clone();
            plan.request_id = sinomo_core::Id::generate().to_string();
            let control_request_id = plan.request_id.clone();
            let prepared = call(&DfsSourceAdoptionRequest::AcquireSource { plan })
                .unwrap()
                .decode_body::<DfsSourceAdoptionRpcResponse>()
                .unwrap();
            let DfsSourceAdoptionRpcResponse::SourcePrepared(token) = prepared else {
                panic!("actual native writer capture");
            };
            assert_eq!(token.request_id, control_request_id);
            assert_ne!(token.request_id, original_token.request_id);
            assert_ne!(
                token.capture_id, original_token.capture_id,
                "fresh actual writer custody does not reuse the retired capture"
            );
            let mut changed = token.clone();
            changed.capture_id = sinomo_core::Id::generate().to_string();
            let rejected =
                call(&DfsSourceAdoptionRequest::RevokeSourcePreparation { token: changed });
            assert!(match rejected {
                Err(_) => true,
                Ok(response) => response
                    .decode_body::<DfsSourceAdoptionRpcResponse>()
                    .is_err(),
            });
            assert!(
                actor
                    .source_preparations
                    .lock()
                    .unwrap()
                    .get(&token.request_id)
                    .unwrap()
                    .holds_unbound()
                    .unwrap()
            );
            for _ in 0..2 {
                let revoked = call(&DfsSourceAdoptionRequest::RevokeSourcePreparation {
                    token: token.clone(),
                })
                .unwrap()
                .decode_body::<DfsSourceAdoptionRpcResponse>()
                .unwrap();
                assert!(
                    matches!(revoked, DfsSourceAdoptionRpcResponse::SourcePreparationRevoked(SourcePreparationRevocation::Revoked(actual)) if actual == token)
                );
            }
            assert!(
                !actor
                    .source_preparations
                    .lock()
                    .unwrap()
                    .get(&token.request_id)
                    .unwrap()
                    .holds_unbound()
                    .unwrap()
            );
            // The original move still owns its live path lock while paused.
            // Revoking this separate unbound capture cannot release that lock.
            assert!(matches!(
                source_client.posix_set_mode(from, 0o600),
                Err(crate::DfsPosixError::Dfs(crate::DfsError::Busy { path }))
                    if path == from
            ));
            pause.release();
            assert!(original.join().unwrap().is_err());
            let preparations = actor.source_preparations.lock().unwrap();
            let revoked = preparations.get(&token.request_id).unwrap();
            assert_eq!(revoked.token, token);
            assert!(!revoked.holds_unbound().unwrap());
            assert!(matches!(
                *revoked.state.lock().unwrap(),
                SourcePreparationState::Revoked { .. }
            ));
            let original = preparations
                .get(&original_plan.request_id)
                .expect("the original phase remains independently recorded");
            assert!(Arc::ptr_eq(original, &original_pending));
            assert_eq!(original.plan, original_plan);
            assert_eq!(original.token, original_token);
            match &*original.state.lock().unwrap() {
                SourcePreparationState::Revoked {
                    cancelled: Some(actual),
                } => {
                    assert_eq!(actual.plan, cancelled_data.plan);
                    assert_eq!(actual.token, cancelled_data.token);
                    assert_eq!(
                        serde_json::to_value(&actual.prepared).unwrap(),
                        serde_json::to_value(&cancelled_data.prepared).unwrap()
                    );
                }
                _ => panic!("the independent control must not replace original cancellation DATA"),
            }
            original_plan
                .job
                .origin
                .clone()
                .expect("actual original Main request identity")
        });
        drop(pause);
        drop(source_client);
        // The cancelled offer proves no target effect. Normal Main recovery
        // captures fresh custody and completes the original still-running job.
        let mut finish = native.client(&node).unwrap();
        assert_eq!(original_origin.requester_node_id, node);
        let moved = finish
            .posix_rename_with_operation_id(from, to, &original_origin.operation_id)
            .unwrap();
        assert_eq!(
            finish.posix_read_file_distributed(to).unwrap(),
            b"real captured bytes"
        );
        drop(finish);
        let actor = actor_for_root(&native.data_dir(&node).root()).unwrap();
        let (completed_pending, completed_prepared) = actor
            .source_preparations
            .lock()
            .unwrap()
            .values()
            .find_map(|pending| match &*pending.state.lock().unwrap() {
                SourcePreparationState::Bound(prepared)
                    if prepared.offer.object_id == moved.object_id =>
                {
                    Some((Arc::clone(pending), prepared.clone()))
                }
                _ => None,
            })
            .expect("actual successful native adoption Bound DATA");
        assert!(
            actor
                .completed_sources
                .lock()
                .unwrap()
                .contains_key(&completed_prepared.offer.session_id)
        );
        assert!(
            !actor
                .sessions
                .lock()
                .unwrap()
                .contains_key(&completed_prepared.offer.session_id)
        );
        let cancelled = actor
            .call_rpc(
                &node,
                &DfsSourceAdoptionRequest::CancelUnclaimed {
                    offer: completed_prepared.offer.clone(),
                },
            )
            .unwrap();
        assert!(
            matches!(cancelled, DfsSourceAdoptionRpcResponse::SourceCancelled { offer }
            if offer == completed_prepared.offer)
        );
        match &*completed_pending.state.lock().unwrap() {
            SourcePreparationState::Bound(actual) => assert_eq!(
                serde_json::to_value(actual).unwrap(),
                serde_json::to_value(&completed_prepared).unwrap()
            ),
            _ => {
                panic!("missing already-completed source session is no guarded cancellation proof")
            }
        }
        drop(completed_pending);
        drop(actor);
        assert_eq!(native.observation_dropped_count(), 0);
        native.close().unwrap();
    }
}
