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
    Revoked,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "state", content = "body", rename_all = "kebab-case")]
pub(crate) enum SourcePreparationRevocation {
    Revoked(SourcePreparationToken),
    Bound(PreparedSourceAdoption),
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
            SourcePreparationState::Revoked => {
                return Ok(SourcePreparationRevocation::Revoked(token.clone()));
            }
            SourcePreparationState::Unbound(_) => {}
        }
        // The same lock serializes revocation against all offer construction.
        // This drops only the unbound actual writer, never a target witness.
        *state = SourcePreparationState::Revoked;
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
        if let Some(existing) = preparations.get(&plan.request_id) {
            if existing.plan != plan {
                return Err(error(
                    "local source retry changed its complete preparation plan",
                ));
            }
            match &*existing
                .state
                .lock()
                .map_err(|_| error("source preparation state poisoned"))?
            {
                SourcePreparationState::Bound(prepared) => return Ok(prepared.offer.clone()),
                SourcePreparationState::Unbound(_) => {
                    return Err(error(
                        "local source phase already owns an unbound preparation",
                    ));
                }
                SourcePreparationState::Revoked => {}
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
            _ => return Ok(None),
        };
        if pending.plan == *plan {
            return Ok(Some(prepared));
        }
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
                if digest == completed.digest =>
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
                if digest == completed.digest =>
            {
                commit.validate(&digest)?
            }
            _ => {
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
            return Err(error(
                "source preparation lacks its actual source/coordinator plan",
            ));
        }
        let configs = self
            .authority
            .cached_metadata_shards(
                &plan.job.file_system_id,
                self.authority.projection_generation(),
            )
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
        let mut pending = self
            .source_preparations
            .lock()
            .map_err(|_| error("source preparation poisoned"))?;
        if !pending.contains_key(&plan.request_id) {
            for previous in &self.predecessors {
                let mut old = previous
                    .source_preparations
                    .lock()
                    .map_err(|_| error("retained source preparations poisoned"))?;
                if let Some(existing) = old.get(&plan.request_id).cloned() {
                    if matches!(
                        *existing
                            .state
                            .lock()
                            .map_err(|_| error("retained source state poisoned"))?,
                        SourcePreparationState::Revoked
                    ) {
                        continue;
                    }
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
            if !matches!(*state, SourcePreparationState::Revoked) {
                if existing.plan != plan {
                    return Err(error("source preparation retry changed its complete plan"));
                }
                return Ok(existing.token.clone());
            }
            drop(state);
            pending.remove(&plan.request_id);
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
        let token = SourcePreparationToken {
            request_id: plan.request_id.clone(),
            capture_id: sinomo_core::Id::generate().to_string(),
            plan_digest: blake3::hash(&serde_json::to_vec(&plan).map_err(error)?)
                .to_hex()
                .to_string(),
            source_node_id: self.node.clone(),
            prefix: capture.prefix().clone(),
        };
        let mut pending = self
            .source_preparations
            .lock()
            .map_err(|_| error("source preparation poisoned"))?;
        if let Some(existing) = pending.get(&plan.request_id) {
            let state = existing
                .state
                .lock()
                .map_err(|_| error("source preparation state poisoned"))?;
            if !matches!(*state, SourcePreparationState::Revoked) {
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
            SourcePreparationState::Revoked => {
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
        deadline: std::time::Instant,
    ) -> Result<PreparedSourceAdoption> {
        if plan.job_config.primary_node_id != self.node {
            return Err(error("source phase is not at its actual coordinator"));
        }
        match self.call_rpc_before(
            &plan.source_config.primary_node_id,
            &DfsSourceAdoptionRequest::InspectSourcePreparation { plan: plan.clone() },
            Some(deadline),
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
            Some(deadline),
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
                    Some(deadline),
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
            Some(deadline),
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
                    Some(deadline),
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

    #[test]
    fn actual_source_first_bound_offer_survives_exact_revocation() {
        let name = "native-source-first-bind-revoke";
        let nodes = crate::test_support::DfsTestRuntime::ordered_node_ids(name, 3);
        let mut native = crate::test_support::DfsTestRuntime::new(name, nodes[0].clone()).unwrap();
        for node in &nodes[1..] {
            native.define_node(node.clone()).unwrap();
        }
        native.start_nodes().unwrap();
        let nested = native
            .bootstrap_empty_metadata_prefix("native-deep-source", "/services/source-first/deep")
            .unwrap();
        let configs = native.metadata_shards().unwrap();
        let coordinator = configs
            .iter()
            .find(|cfg| cfg.shard_id == "services")
            .unwrap()
            .primary_node_id
            .clone();
        let target = configs
            .iter()
            .find(|cfg| cfg.shard_id == "users")
            .unwrap()
            .primary_node_id
            .clone();
        assert!(nested.primary_node_id < coordinator);
        let mut requester = native.client(&nodes[0]).unwrap();
        let from = "/services/source-first";
        let to = "/users/source-first";
        requester.posix_mkdir_all(&format!("{from}/deep")).unwrap();
        let original = requester
            .posix_write_file(
                &format!("{from}/deep/child.bin"),
                b"actual source-first bytes",
                crate::DfsWriteOptions::replicated_eventual(nodes.clone()),
            )
            .unwrap();
        let source_directory = requester
            .posix_stat_distributed(&format!("{from}/deep"))
            .unwrap();
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
            let foreground = scope.spawn(move || requester.posix_rename(from, to));
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
                .posix_stat_distributed(&format!("{to}/deep/child.bin"))
                .unwrap()
                .object_id,
            original.object_id
        );
        assert_eq!(
            reader
                .posix_read_file_distributed(&format!("{to}/deep/child.bin"))
                .unwrap(),
            b"actual source-first bytes"
        );
        drop(reader);
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
        let nested = native
            .bootstrap_empty_metadata_prefix("native-deep-source", "/services/source-first/deep")
            .unwrap();
        let configs = native.metadata_shards().unwrap();
        let coordinator = configs
            .iter()
            .find(|cfg| cfg.shard_id == "services")
            .unwrap()
            .primary_node_id
            .clone();
        let target = configs
            .iter()
            .find(|cfg| cfg.shard_id == "users")
            .unwrap()
            .primary_node_id
            .clone();
        assert!(nested.primary_node_id < coordinator);
        let mut requester = native.client(&nodes[0]).unwrap();
        let from = "/services/source-first";
        let to = "/users/source-first";
        requester.posix_mkdir_all(&format!("{from}/deep")).unwrap();
        let original = requester
            .posix_write_file(
                &format!("{from}/deep/child.bin"),
                b"actual source-first bytes",
                crate::DfsWriteOptions::replicated_eventual(nodes.clone()),
            )
            .unwrap();
        let source_directory = requester
            .posix_stat_distributed(&format!("{from}/deep"))
            .unwrap();
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
        std::thread::scope(|scope| {
            let op = operation.clone();
            let foreground =
                scope.spawn(move || requester.posix_rename_with_operation_id(from, to, &op));
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
            bind_pause.release();
            assert!(foreground.join().unwrap().is_err());
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
        reader
            .posix_mkdir_all("/services/unbound-job-writer-progress")
            .unwrap();
        reader
            .posix_rename_with_operation_id(from, to, &operation)
            .unwrap();
        assert_eq!(
            reader
                .posix_stat_distributed(&format!("{to}/deep/child.bin"))
                .unwrap()
                .object_id,
            original.object_id
        );
        assert_eq!(
            reader
                .posix_read_file_distributed(&format!("{to}/deep/child.bin"))
                .unwrap(),
            b"actual source-first bytes"
        );
        drop(reader);
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
        let nested = native
            .bootstrap_empty_metadata_prefix("native-deep-source", "/services/source-first/deep")
            .unwrap();
        let configs = native.metadata_shards().unwrap();
        let coordinator = configs
            .iter()
            .find(|cfg| cfg.shard_id == "services")
            .unwrap()
            .primary_node_id
            .clone();
        let target = configs
            .iter()
            .find(|cfg| cfg.shard_id == "users")
            .unwrap()
            .primary_node_id
            .clone();
        assert!(nested.primary_node_id < coordinator);
        let mut requester = native.client(&nodes[0]).unwrap();
        let from = "/services/source-first";
        let to = "/users/source-first";
        requester.posix_mkdir_all(&format!("{from}/deep")).unwrap();
        let original = requester
            .posix_write_file(
                &format!("{from}/deep/child.bin"),
                b"actual source-first bytes",
                crate::DfsWriteOptions::replicated_eventual(nodes.clone()),
            )
            .unwrap();
        let source_directory = requester
            .posix_stat_distributed(&format!("{from}/deep"))
            .unwrap();
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
            let op = operation.clone();
            let foreground =
                scope.spawn(move || requester.posix_rename_with_operation_id(from, to, &op));
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
                .posix_mkdir_all("/services/unbound-job-writer-progress")
                .unwrap();
            reader
                .posix_rename_with_operation_id(from, to, &operation)
                .unwrap();
        }
        assert_eq!(
            reader
                .posix_stat_distributed(&format!("{to}/deep/child.bin"))
                .unwrap()
                .object_id,
            original.object_id
        );
        assert_eq!(
            reader
                .posix_read_file_distributed(&format!("{to}/deep/child.bin"))
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
        std::thread::scope(|scope| {
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
            let cancelled = call(&DfsSourceAdoptionRequest::CancelUnclaimed {
                offer: source_offer.clone(),
            })
            .unwrap()
            .decode_body::<DfsSourceAdoptionRpcResponse>()
            .unwrap();
            assert!(
                matches!(cancelled, DfsSourceAdoptionRpcResponse::SourceCancelled { offer } if offer == *source_offer)
            );
            let plan = SourcePreparationPlan {
                request_id: sinomo_core::Id::generate().to_string(),
                job,
                job_config: source_offer.job_offer.job_config.clone(),
                source_config: source_offer.source_config.clone(),
                target_config: source_offer.target_config.clone(),
                destination: source_offer.destination.clone(),
                source_path: source_path.clone(),
                expected_source: Some(expected),
                transfer_id: source_offer.transfer_id.clone(),
            };
            let prepared = call(&DfsSourceAdoptionRequest::AcquireSource { plan })
                .unwrap()
                .decode_body::<DfsSourceAdoptionRpcResponse>()
                .unwrap();
            let DfsSourceAdoptionRpcResponse::SourcePrepared(token) = prepared else {
                panic!("actual native writer capture");
            };
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
                SourcePreparationState::Revoked
            ));
        });
        drop(pause);
        drop(source_client);
        assert_eq!(native.observation_dropped_count(), 0);
        native.close().unwrap();
    }
}
