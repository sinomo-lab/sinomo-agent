//! Actual original coordinator custody. Offers identify a retained writer;
//! their wire values cannot create that writer or authorize publication.
use super::*;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsSourceAdoptionJobOffer {
    pub coordinator_node_id: NodeId,
    pub session_id: String,
    pub job_config: DfsMetadataShardConfig,
    pub job_id: String,
    pub job_digest: String,
    pub prefix: crate::DfsMetadataPrefixProof,
}

pub(super) struct OriginalJobSession {
    pub(super) capture: Arc<crate::DfsSourceAdoptionJobCapture>,
    pub(super) offer: DfsSourceAdoptionJobOffer,
    pub(super) generation: u64,
    pub(super) source: DfsMetadataShardConfig,
    pub(super) target: DfsMetadataShardConfig,
    pub(super) destination: crate::DfsObjectMetadata,
    pub(super) source_path: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct OriginalJobReceipt {
    pub(super) offer: DfsSourceAdoptionJobOffer,
    pub(super) job: crate::DfsNamespaceJobRecord,
}

fn job_digest(job: &crate::DfsNamespaceJobRecord) -> Result<String> {
    Ok(blake3::hash(&serde_json::to_vec(job).map_err(error)?)
        .to_hex()
        .to_string())
}

impl DfsSourceAdoptionActor {
    pub(super) fn release_original_job(
        &self,
        offer: &DfsSourceAdoptionOffer,
        digest: Option<&DfsPreparedOperationDigest>,
    ) -> Result<()> {
        if offer.job_offer.coordinator_node_id == self.node {
            self.release_original_job_received(&offer.source_node_id, offer, digest)
        } else {
            match self.call_rpc(
                &offer.job_offer.coordinator_node_id,
                &DfsSourceAdoptionRequest::ReleaseOriginalJob {
                    offer: offer.clone(),
                    digest: digest.cloned(),
                },
            )? {
                DfsSourceAdoptionRpcResponse::OriginalJobReleased { offer: actual }
                    if actual == offer.job_offer =>
                {
                    Ok(())
                }
                _ => Err(error(
                    "original job release changed its exact native session",
                )),
            }
        }
    }
    pub(super) fn release_original_job_received(
        &self,
        caller: &NodeId,
        offer: &DfsSourceAdoptionOffer,
        digest: Option<&DfsPreparedOperationDigest>,
    ) -> Result<()> {
        if caller != &offer.source_node_id || caller != &offer.source_config.primary_node_id {
            return Err(error(
                "original job release is not from its bound actual source actor",
            ));
        }
        if !self
            .job_sessions
            .lock()
            .map_err(|_| error("original jobs poisoned"))?
            .contains_key(&offer.job_offer.session_id)
            && !self
                .retired_jobs
                .lock()
                .map_err(|_| error("original job retirement poisoned"))?
                .contains_key(&offer.job_offer.session_id)
        {
            for previous in &self.predecessors {
                let held = previous
                    .job_sessions
                    .lock()
                    .map_err(|_| error("retained jobs poisoned"))?
                    .get(&offer.job_offer.session_id)
                    .is_some_and(|session| session.offer == offer.job_offer);
                let retired = previous
                    .retired_jobs
                    .lock()
                    .map_err(|_| error("retained job retirement poisoned"))?
                    .get(&offer.job_offer.session_id)
                    .is_some_and(|(old, _)| old == offer);
                if held || retired {
                    return previous.release_original_job_received(caller, offer, digest);
                }
            }
        }
        if let Some(digest) = digest {
            if digest.file_system_id != offer.target_config.file_system_id
                || digest.shard_id.0 != offer.target_config.shard_id
                || digest.owner_epoch != offer.target_config.epoch
            {
                return Err(error(
                    "original job release changes exact target effect scope",
                ));
            }
        }
        let mut retired = self
            .retired_jobs
            .lock()
            .map_err(|_| error("original job retirement poisoned"))?;
        if let Some((old_offer, old_digest)) = retired.get(&offer.job_offer.session_id) {
            if old_offer != offer || old_digest.as_ref() != digest {
                return Err(error(
                    "original job release retry changed its exact terminal identity",
                ));
            }
            return Ok(());
        }
        self.release_local_original_job(offer)?;
        retired.insert(
            offer.job_offer.session_id.clone(),
            (offer.clone(), digest.cloned()),
        );
        Ok(())
    }
    pub(super) fn release_local_original_job(&self, offer: &DfsSourceAdoptionOffer) -> Result<()> {
        if offer.job_offer.coordinator_node_id != self.node {
            return Err(error(
                "original job release belongs to another actual coordinator",
            ));
        }
        let mut sessions = self
            .job_sessions
            .lock()
            .map_err(|_| error("original job sessions poisoned"))?;
        let session = sessions
            .get(&offer.job_offer.session_id)
            .ok_or_else(|| error("original job custody no longer exists"))?;
        if session.offer != offer.job_offer
            || session.source != offer.source_config
            || session.target != offer.target_config
            || session.destination != offer.destination
        {
            return Err(error(
                "original job release changes its exact captured source plan",
            ));
        }
        sessions.remove(&offer.job_offer.session_id);
        RETAINED
            .get_or_init(Default::default)
            .lock()
            .map_err(|_| error("source custody registry poisoned"))?
            .remove(&(
                self.data_root.clone(),
                format!("job:{}", offer.job_offer.session_id),
            ));
        Ok(())
    }
    /// Called only after the actual source atomically reports this exact
    /// preparation token Revoked. It retires no issued target witness.
    pub(super) fn release_unbound_original_job(
        &self,
        offer: &DfsSourceAdoptionJobOffer,
        plan: &super::preparation::SourcePreparationPlan,
    ) -> Result<()> {
        if self
            .sessions
            .lock()
            .map_err(|_| error("source sessions poisoned"))?
            .values()
            .any(|source| source.offer.job_offer == *offer)
        {
            return Err(error(
                "another actual bound source still retains original job custody",
            ));
        }
        let mut sessions = self
            .job_sessions
            .lock()
            .map_err(|_| error("original jobs poisoned"))?;
        let session = sessions
            .get(&offer.session_id)
            .ok_or_else(|| error("unbound original job custody unavailable"))?;
        if offer.coordinator_node_id != self.node
            || session.offer != *offer
            || session.capture.job() != &plan.job
            || session.source != plan.source_config
            || session.target != plan.target_config
            || session.destination != plan.destination
            || session.source_path != plan.source_path
            || session.offer.job_config != plan.job_config
        {
            return Err(error(
                "unbound job release changed its complete actual capture/plan",
            ));
        }
        sessions.remove(&offer.session_id);
        RETAINED
            .get_or_init(Default::default)
            .lock()
            .map_err(|_| error("source custody registry poisoned"))?
            .remove(&(self.data_root.clone(), format!("job:{}", offer.session_id)));
        Ok(())
    }

    /// Called after the normal coordinator releases global/service ownership.
    /// The opaque native capture owns the exact current physical job writer.
    pub(super) fn register_original_job(
        self: &Arc<Self>,
        capture: crate::DfsSourceAdoptionJobCapture,
        job_config: DfsMetadataShardConfig,
        source: DfsMetadataShardConfig,
        target: DfsMetadataShardConfig,
        destination: crate::DfsObjectMetadata,
        source_path: String,
    ) -> Result<DfsSourceAdoptionJobOffer> {
        if *self.shutdown.borrow()
            || self.shutdown.has_changed().is_err()
            || job_config.primary_node_id != self.node
            || capture.scope().file_system_id() != &job_config.file_system_id
            || capture.scope().shard_id().0 != job_config.shard_id
            || !capture.scope().root().starts_with(&self.data_root)
            || capture.job().status != DfsNamespaceJobStatus::Running
        {
            return Err(error(
                "original job capture has no actual current coordinator",
            ));
        }
        let configs = self
            .authority
            .configuration_snapshot()
            .metadata_shards(&job_config.file_system_id)
            .map_err(error)?
            .ok_or_else(|| error("original coordinator configuration unavailable"))?;
        if !configs.contains(&job_config)
            || !configs.contains(&source)
            || !configs.contains(&target)
        {
            return Err(error(
                "original coordinator/source/target configuration changed",
            ));
        }
        let generation = self
            .authority
            .shard_projection_state(capture.scope().file_system_id(), capture.scope().shard_id());
        if !generation.1 || !job_binds_destination(capture.job(), &destination, &source_path) {
            return Err(error(
                "actual running job does not bind this source/destination",
            ));
        }
        let offer = DfsSourceAdoptionJobOffer {
            coordinator_node_id: self.node.clone(),
            session_id: sinomo_core::Id::generate().to_string(),
            job_config,
            job_id: capture.job().job_id.clone(),
            job_digest: job_digest(capture.job())?,
            prefix: capture.prefix().clone(),
        };
        let session = Arc::new(OriginalJobSession {
            capture: Arc::new(capture),
            offer: offer.clone(),
            generation: generation.0,
            source,
            target,
            destination,
            source_path,
        });
        self.job_sessions
            .lock()
            .map_err(|_| error("original job sessions poisoned"))?
            .insert(offer.session_id.clone(), session);
        RETAINED
            .get_or_init(Default::default)
            .lock()
            .map_err(|_| error("source custody registry poisoned"))?
            .insert(
                (self.data_root.clone(), format!("job:{}", offer.session_id)),
                Arc::clone(self),
            );
        Ok(offer)
    }

    /// Borrow the original local job writer for this exact resumed namespace
    /// request's negative genesis read. Wire offers alone supply no custody.
    pub(crate) fn ingress_job_writer_custody(
        &self,
        offer: &DfsSourceAdoptionOffer,
        command: &crate::DfsNamespaceCommand,
        current: &DfsMetadataShardConfig,
    ) -> Result<Option<crate::metadata_backend::DfsSourceAdoptionWriterCustody>> {
        if offer.job_offer.job_config.file_system_id != current.file_system_id
            || offer.job_offer.job_config.shard_id != current.shard_id
        {
            return Ok(None);
        }
        if offer.job_offer.job_config != *current {
            return Err(error("genesis read changed the original job's current assignment"));
        }
        self.inspect_original_job(&offer.source_node_id, &offer.job_offer)?;
        let session = self.original_job_session(&offer.job_offer, true)?;
        let job = session.capture.job();
        if command.file_system_id != job.file_system_id
            || !job.origin.as_ref().is_some_and(|origin|
                origin.request_id == command.request_id
                    && origin.requester_node_id == command.requesting_node_id)
            || session.source != offer.source_config
            || session.target != offer.target_config
            || session.destination != offer.destination
        {
            return Err(error("genesis read changed the original namespace job/request custody"));
        }
        Ok(Some(session.capture.writer_custody()))
    }

    pub(super) fn inspect_original_job(
        &self,
        source_node: &NodeId,
        offer: &DfsSourceAdoptionJobOffer,
    ) -> Result<OriginalJobReceipt> {
        let session = self.original_job_session(offer, true)?;
        if session.offer != *offer
            || (source_node != &session.source.primary_node_id
                && source_node != &session.target.primary_node_id)
            || offer.coordinator_node_id != self.node
            || offer.job_config.primary_node_id != self.node
            || *self.shutdown.borrow()
            || self.shutdown.has_changed().is_err()
        {
            return Err(error(
                "original job inspection changed actual actor/session identity",
            ));
        }
        // One publication supplies the shard fence and its routing rows.
        let snapshot = self.authority.configuration_snapshot();
        let state = (
            snapshot.generation(),
            snapshot.contains_shard_key(
                &offer.job_config.file_system_id,
                &crate::DfsMetadataShardId::new(&offer.job_config.shard_id),
            ),
        );
        let configs = snapshot
            .metadata_shards(&offer.job_config.file_system_id)
            .map_err(error)?
            .ok_or_else(|| error("original job configuration unavailable"))?;
        if !state.1
            || state.0 != session.generation
            || !configs.contains(&offer.job_config)
            || !configs.contains(&session.source)
            || !configs.contains(&session.target)
            || session.capture.prefix() != &offer.prefix
            || job_digest(session.capture.job())? != offer.job_digest
            || !job_binds_destination(
                session.capture.job(),
                &session.destination,
                &session.source_path,
            )
        {
            return Err(error(
                "original job writer/configuration/complete body changed",
            ));
        }
        Ok(OriginalJobReceipt {
            offer: offer.clone(),
            job: session.capture.job().clone(),
        })
    }
}

pub(super) fn job_binds_destination(
    job: &crate::DfsNamespaceJobRecord,
    destination: &crate::DfsObjectMetadata,
    source_path: &str,
) -> bool {
    job.file_system_id == destination.file_system_id
        && job.pending_frames.iter().any(|frame| match frame {
            crate::DfsNamespaceJobFrame::CrossShardAdopt {
                moved_object,
                old_path,
                ..
            }
            | crate::DfsNamespaceJobFrame::CrossShardComplete {
                moved_object,
                old_path,
            } => moved_object == destination && old_path == source_path,
            _ => false,
        })
}
