//! Current namespace job delivery after exact native source retirement.
//! Blueprints are data; receivers replay their real native receipts and obtain
//! fresh current effect authority before the ordinary DFS job publication.
use super::*;
use sinomo_dfs::{DfsNamespaceJobRecord, DfsPreparedOperationDigest};

pub(super) type JobOutcome = Arc<Mutex<Option<DfsPreparedOperationDigest>>>;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RetirementBlueprint {
    pub(super) plan: MetadataImportPlan,
    pub(super) activation_request: KvTxnRequest,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct JobDataRequest {
    pub(super) request_id: String,
    pub(super) source: MetadataSourceCut,
    pub(super) retirements: Vec<RetirementBlueprint>,
    pub(super) coordinator: DfsMetadataShardConfig,
    pub(super) job: DfsNamespaceJobRecord,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct JobDataInstalled {
    pub(super) request_id: String,
    pub(super) coordinator: DfsMetadataShardConfig,
    pub(super) job_id: String,
    pub(super) digest: DfsPreparedOperationDigest,
}

fn pending(source: &MetadataSourceCut) -> MetadataBackendError {
    MetadataBackendError::StoragePreparationPending {
        root: source.source_root.clone(),
        processed_records: 0,
    }
}

impl NativeImportAuthority {
    pub(super) fn resume_current_imported_jobs(
        &self,
        source: &MetadataSourceCut,
        certificate: &ImportCertificate,
        installed_root: &Path,
        jobs: &[DfsNamespaceJobRecord],
    ) -> Result<()> {
        let ImportCertificate::Native(certificate) = certificate else {
            return Err(error(
                "native namespace work received a local-owner certificate",
            ));
        };
        if jobs != crate::metadata::imported_namespace_jobs(source)? {
            return Err(error(
                "namespace work differs from complete captured current job DATA",
            ));
        }
        if jobs.is_empty() {
            return Ok(());
        }
        Self::require_committed(&certificate.stage_commit)?;
        Self::require_committed(&certificate.activation_commit)?;
        let proof = &certificate.joint_handoff_proof;
        if proof.cause != Self::import_cause(source)? {
            return Err(error(
                "namespace work does not belong to this native source cut",
            ));
        }
        let blueprints = self.retirement_blueprints(source)?;
        let binding = digest(source)?;
        if !blueprints.iter().any(|blueprint| {
            digest(&blueprint.plan.source_cut).is_ok_and(|captured| captured == binding)
                && blueprint.plan.stage_request == certificate.stage_commit.request
                && blueprint.activation_request == certificate.activation_commit.request
        }) {
            return Err(error(
                "namespace work changed its sealed actual import receipts",
            ));
        }
        self.verify_retirement_blueprints(source, &blueprints)?;
        let current_image = sinomo_dfs::read_metadata_candidate(installed_root)?;
        if current_image.semantic_state_fingerprint != certificate.current_image_fingerprint
            || current_image.prefix_proof != certificate.target_exact_prefix
        {
            return Err(error(
                "namespace work differs from its actual installed image",
            ));
        }
        let data_dir = self.component_context.data_dir().clone();
        let mut unfinished = false;
        for job in jobs {
            if current_image
                .tables
                .namespace_jobs
                .iter()
                .any(|stored| stored == job)
            {
                continue;
            }
            let binding = digest(&("current-imported-job-delivery-v7", source, job))?;
            let outcome = {
                let mut outcomes = self.namespace_work.lock().map_err(error)?;
                Arc::clone(
                    outcomes
                        .entry(binding.clone())
                        .or_insert_with(|| Arc::new(Mutex::new(None))),
                )
            };
            if outcome.lock().map_err(error)?.is_some() {
                continue;
            }
            unfinished = true;
            let authority = self.clone();
            let source = source.clone();
            let job = job.clone();
            let blueprints = blueprints.clone();
            let action_binding = binding.clone();
            sinomo_dfs::runtime_host::queue_namespace_reconciliation_work(
                &data_dir,
                Arc::clone(&self.owner),
                binding,
                true,
                Arc::new(move || {
                    if outcome
                        .lock()
                        .map_err(|e| sinomo_dfs::runtime_host::DfsRuntimeError::Parse {
                            detail: e.to_string(),
                        })?
                        .is_some()
                    {
                        return Ok(true);
                    }
                    let completed = authority
                        .deliver_current_job(&source, &blueprints, &job, &action_binding)
                        .map_err(|e| sinomo_dfs::runtime_host::DfsRuntimeError::Parse {
                            detail: e.to_string(),
                        })?;
                    *outcome.lock().map_err(|e| {
                        sinomo_dfs::runtime_host::DfsRuntimeError::Parse {
                            detail: e.to_string(),
                        }
                    })? = Some(completed);
                    Ok(true)
                }),
            )
            .map_err(error)?;
        }
        if unfinished {
            return Err(pending(source));
        }
        for job in jobs {
            let binding = digest(&("current-imported-job-delivery-v7", source, job))?;
            self.namespace_work.lock().map_err(error)?.remove(&binding);
        }
        sinomo_dfs::runtime_host::wake_namespace_reconciliation(&data_dir, &self.owner)
            .map_err(error)
    }

    fn deliver_current_job(
        &self,
        source: &MetadataSourceCut,
        retirements: &[RetirementBlueprint],
        job: &DfsNamespaceJobRecord,
        request_id: &str,
    ) -> Result<DfsPreparedOperationDigest> {
        let assignments = self.assignments(&[job.file_system_id.clone()])?;
        let coordinator = sinomo_dfs::select_metadata_shard_config_for_path(
            &job.file_system_id,
            &assignments,
            &job.root_path,
        )
        .map_err(error)?
        .ok_or_else(|| error("current job has no actual source coordinator"))?;
        let request = JobDataRequest {
            request_id: request_id.to_owned(),
            source: source.clone(),
            retirements: retirements.to_vec(),
            coordinator: coordinator.clone(),
            job: job.clone(),
        };
        let local = self.local_node()?;
        let installed = if coordinator.primary_node_id == local {
            self.receive_current_job(&local, request)?
        } else {
            let rpc = sinomo_core::network::ObjectRpcRequest::encode_body(
                staged_service::NAMESPACE_JOB_ROUTE,
                "dfs-migration",
                &request,
            )
            .map_err(error)?;
            let mut response = block_native(
                &self.runtime,
                self.transport
                    .as_ref()
                    .ok_or_else(|| error("namespace delivery has no native transport"))?
                    .call_async(
                        &coordinator.primary_node_id,
                        sinomo_core::network::ObjectRpcCall::value(rpc),
                        source_effect_rpc_options(),
                        true,
                    ),
            )?;
            block_native(&self.runtime, response.materialize_value_body(64 * 1024))?;
            if response.response.route != staged_service::NAMESPACE_JOB_ROUTE {
                return Err(error("namespace delivery returned another route"));
            }
            response
                .response
                .decode_body::<JobDataInstalled>()
                .map_err(error)?
        };
        let job_binding = blake3::hash(&serde_json::to_vec(job).map_err(error)?)
            .to_hex()
            .to_string();
        if installed.request_id != request_id
            || installed.coordinator != coordinator
            || installed.job_id != job.job_id
            || installed.digest.file_system_id != job.file_system_id
            || installed.digest.shard_id.0 != coordinator.shard_id
            || installed.digest.operation_id != format!("namespace-job-data-v7:{job_binding}")
            || installed.digest.prepare_sequence == 0
            || installed.digest.payload_digest.is_empty()
        {
            return Err(error(
                "namespace delivery changed its actual correlated committed identity",
            ));
        }
        Ok(installed.digest)
    }

    pub(super) fn receive_current_job(
        &self,
        caller: &sinomo_core::node::NodeId,
        request: JobDataRequest,
    ) -> Result<JobDataInstalled> {
        if !crate::metadata::imported_namespace_jobs(&request.source)?.contains(&request.job) {
            return Err(error(
                "namespace delivery job is absent from the immutable import cut",
            ));
        }
        let source_binding = digest(&request.source)?;
        let original = request
            .retirements
            .iter()
            .find(|blueprint| {
                digest(&blueprint.plan.source_cut).is_ok_and(|binding| binding == source_binding)
                    && blueprint.plan.source_cut.scope_shard == request.source.scope_shard
            })
            .ok_or_else(|| error("namespace delivery has no exact native source blueprint"))?;
        let original_scope = &original.plan.expected_handoff.desired_shard;
        let assignments = self.assignments(&[request.job.file_system_id.clone()])?;
        let actual = sinomo_dfs::select_metadata_shard_config_for_path(
            &request.job.file_system_id,
            &assignments,
            &request.job.root_path,
        )
        .map_err(error)?
        .ok_or_else(|| error("namespace delivery source path has no current assignment"))?;
        let keys = vec![
            sinomo_dfs::metadata_shard_key(
                &original_scope.file_system_id,
                &original_scope.shard_id,
            ),
            sinomo_dfs::metadata_shard_key(&actual.file_system_id, &actual.shard_id),
            sinomo_dfs::metadata_shard_membership_handoff_key(
                &actual.file_system_id,
                &actual.shard_id,
            ),
        ];
        let (entries, _) = self.read_exact(&keys)?;
        let caller_scope: DfsMetadataShardConfig = entries[0]
            .entry
            .as_ref()
            .ok_or_else(|| error("namespace caller has no current source scope"))?
            .decode()
            .map_err(error)?;
        let effect_scope: DfsMetadataShardConfig = entries[1]
            .entry
            .as_ref()
            .ok_or_else(|| error("namespace effect has no current assignment"))?
            .decode()
            .map_err(error)?;
        if caller_scope.file_system_id != original_scope.file_system_id
            || caller_scope.shard_id != original_scope.shard_id
            || caller_scope.epoch < original_scope.epoch
            || &caller_scope.primary_node_id != caller
            || effect_scope != actual
            || actual != request.coordinator
            || actual.primary_node_id != self.local_node()?
            || entries[2].entry.is_some()
        {
            return Err(error(
                "namespace work lacks fresh exact caller/effect authority",
            ));
        }
        self.verify_retirement_blueprints(&request.source, &request.retirements)?;
        let data_dir = self.component_context.data_dir().clone();
        let mut service = sinomo_dfs::runtime_host::open_dfs_runtime_service_on_shard(
            &data_dir,
            Some(&self.runtime),
            &actual,
        )
        .map_err(error)?;
        let committed = service
            .publish_namespace_job_data(&request.job)
            .map_err(error)?;
        drop(service);
        sinomo_dfs::runtime_host::wake_namespace_reconciliation(&data_dir, &self.owner)
            .map_err(error)?;
        Ok(JobDataInstalled {
            request_id: request.request_id,
            coordinator: actual,
            job_id: request.job.job_id,
            digest: committed,
        })
    }
}

impl NativeImportAuthority {
    /// Queue after the certified installer has removed its intent and released
    /// its RootSlot. Historic topology is DATA, never a current writer permit.
    pub(super) fn queue_native_original_topology(
        &self,
        source: &MetadataSourceCut,
        certificate: &ImportCertificate,
        installed_root: &Path,
    ) -> Result<()> {
        if source
            .pending_operations
            .iter()
            .any(|row| row.get("prior_native_topology").is_some())
        {
            let ImportCertificate::Native(current) = certificate else {
                return Err(error(
                    "historic native topology has no current native certificate",
                ));
            };
            self.queue_prior_native_topology(
                source,
                &current.stage_commit,
                &current.activation_commit,
                &current.joint_handoff_proof,
                &current.current_image_fingerprint,
                &current.target_exact_prefix,
                installed_root,
                true,
            )?;
        }
        let Some(original) = source.original_handoff.as_ref() else {
            return Ok(());
        };
        let ImportCertificate::Native(certificate) = certificate else {
            return Err(error(
                "original native topology has no native import certificate",
            ));
        };
        Self::require_committed(&self.commit(certificate.stage_commit.request.clone())?)?;
        Self::require_committed(&self.commit(certificate.activation_commit.request.clone())?)?;
        let proof = &certificate.joint_handoff_proof;
        if proof.cause != Self::import_cause(source)?
            || proof.desired_shard.file_system_id != original.desired_shard.file_system_id
            || proof.desired_shard.shard_id != original.desired_shard.shard_id
        {
            return Err(error(
                "original topology DATA changed its exact activated source scope",
            ));
        }
        self.queue_original_topology_work(
            source,
            installed_root,
            proof.desired_shard.clone(),
            certificate.stage_commit.request.clone(),
            certificate.activation_commit.request.clone(),
        )
    }

    /// Rebind historic desired DATA only across the proven storage-schema
    /// successor. The old request is replayed, never used as a9 writer permit.
    fn queue_prior_native_topology(
        &self,
        source: &MetadataSourceCut,
        stage_commit: &DfsNativeTxnCommit,
        activation_commit: &DfsNativeTxnCommit,
        current_proof: &sinomo_dfs::DfsMetadataShardMembershipHandoffProof,
        current_image_fingerprint: &str,
        target_exact_prefix: &sinomo_dfs::DfsMetadataPrefixProof,
        installed_root: &Path,
        exact_installed_image: bool,
    ) -> Result<()> {
        for row in &source.pending_operations {
            let Some(data) = row.get("prior_native_topology") else {
                continue;
            };
            let bytes = |name: &str, hash: &str| -> Result<Vec<u8>> {
                let bytes: Vec<u8> = serde_json::from_value(data[name].clone()).map_err(error)?;
                if data[hash].as_str() != Some(blake3::hash(&bytes).to_hex().as_str()) {
                    return Err(error(
                        "historic topology DATA changes its captured original bytes",
                    ));
                }
                Ok(bytes)
            };
            let marker: serde_json::Value =
                serde_json::from_slice(&bytes("marker_bytes", "marker_digest")?).map_err(error)?;
            let prior: MetadataImportPlan =
                serde_json::from_slice(&bytes("plan_bytes", "plan_digest")?).map_err(error)?;
            Self::validate_plan(&prior)?;
            let old_cut: MetadataSourceCut =
                serde_json::from_value(marker["source_cut"].clone()).map_err(error)?;
            if marker["format_version"].as_u64() != Some(7)
                || digest(&old_cut)? != digest(&prior.source_cut)?
                || prior.source_cut.scope_shard != source.scope_shard
            {
                return Err(error(
                    "historic topology DATA differs from its original native plan",
                ));
            }
            let Some(original) = old_cut.original_handoff.as_ref() else {
                return Err(error(
                    "historic topology DATA lost its original desired configuration",
                ));
            };
            if source.source_format_version != Some(8)
                || current_proof.retiring_shard.file_system_id
                    != original.desired_shard.file_system_id
                || current_proof.retiring_shard.shard_id != original.desired_shard.shard_id
                || current_proof.cause != Self::import_cause(source)?
                || current_proof.retiring_shard.epoch >= current_proof.desired_shard.epoch
            {
                return Err(error(
                    "historic topology DATA has no exact native8-to-current import lineage",
                ));
            }
            if current_proof.state_fingerprint != current_image_fingerprint
                || current_proof.journal_sequence != target_exact_prefix.journal_sequence
                || !source
                    .projection_assignments
                    .iter()
                    .any(|cfg| cfg == &current_proof.retiring_shard)
                || Self::activation_proof(&activation_commit.request)?.as_ref()
                    != Some(current_proof)
            {
                return Err(error(
                    "historic topology callback changes its actual current activation binding",
                ));
            }
            Self::require_committed(&self.commit(stage_commit.request.clone())?)?;
            Self::require_committed(&self.commit(activation_commit.request.clone())?)?;
            if !crate::metadata::current_root(installed_root)? {
                return Err(error(
                    "historic topology callback has no current storage generation",
                ));
            }
            if exact_installed_image {
                let actual_image = sinomo_dfs::inspect_metadata_candidate(installed_root)?;
                if actual_image.state_fingerprint != current_image_fingerprint
                    || &actual_image.prefix_proof != target_exact_prefix
                {
                    return Err(error(
                        "historic topology callback does not own the actual current image",
                    ));
                }
            }
            // Source input membership must be exactly the previous native
            // import's activated configuration. Epoch alone grants nothing.
            let prior_request = match prior.activation_request.clone() {
                Some(request) => request,
                None => {
                    let marker: serde_json::Value = serde_json::from_slice(
                        &std::fs::read(installed_root.join("metadata-import-v7.json"))
                            .map_err(error)?,
                    )
                    .map_err(error)?;
                    let retained =
                        PathBuf::from(marker["retained_source_root"].as_str().ok_or_else(
                            || error("current import lost its actual prior-native source"),
                        )?);
                    self.validate_source_root(&retained)?;
                    crate::metadata::validate_retained_source_identity(
                        &retained,
                        &source.source_identity,
                        &source.source_snapshot_fingerprint,
                    )?;
                    self.find_native_request(&retained, &prior.expected_handoff, true)?
                }
            };
            let prior_proof = Self::activation_proof(&prior_request)?.ok_or_else(|| {
                error("historic topology DATA lacks its actual original joint proof")
            })?;
            self.validate_activation_request(&prior_request, &prior, &prior_proof)?;
            Self::require_committed(&self.commit(prior.stage_request.clone())?)?;
            Self::require_committed(&self.commit(prior_request)?)?;
            if !topology_matches(&current_proof.retiring_shard, &prior_proof.desired_shard) {
                // A genuinely different topology superseded the historic DATA.
                // A schema-only successor does not satisfy this condition.
                continue;
            }
            if current_proof.retiring_shard != prior_proof.desired_shard
                && !topology_matches(&current_proof.retiring_shard, &original.desired_shard)
            {
                return Err(error(
                    "historic topology DATA crossed an unproved native ownership change",
                ));
            }
            let mut topology_source = source.clone();
            // This clone is callback DATA only. It never re-enters plan(), the
            // old-reservation replacement CAS, or current source certification.
            topology_source.original_handoff = Some(original.clone());
            self.queue_original_topology_work(
                &topology_source,
                installed_root,
                current_proof.desired_shard.clone(),
                stage_commit.request.clone(),
                activation_commit.request.clone(),
            )?;
        }
        Ok(())
    }

    pub(super) fn queue_published_original_topology(
        &self,
        source: &MetadataSourceCut,
        installed_root: &Path,
    ) -> Result<()> {
        if source
            .pending_operations
            .iter()
            .any(|row| row.get("prior_native_topology").is_some())
        {
            let plan: MetadataImportPlan = serde_json::from_slice(
                &std::fs::read(installed_root.join("native-import-plan.json")).map_err(error)?,
            )
            .map_err(error)?;
            Self::validate_plan(&plan)?;
            if digest(&plan.source_cut)? != digest(source)? {
                return Err(error(
                    "published historic topology differs from its sealed current source cut",
                ));
            }
            let request = self.find_native_request(installed_root, &plan.expected_handoff, true)?;
            let proof = Self::activation_proof(&request)?.ok_or_else(|| {
                error("published historic topology has no actual current activation")
            })?;
            self.validate_activation_request(&request, &plan, &proof)?;
            let stage_commit = self.commit(plan.stage_request.clone())?;
            let activation_commit = self.commit(request)?;
            Self::require_committed(&stage_commit)?;
            Self::require_committed(&activation_commit)?;
            let source_retirements = self.verify_original_source_retirements(
                source,
                &plan,
                &stage_commit,
                &activation_commit,
                &proof,
            )?;
            let data_reservation_retirements = self.verify_data_reservation_retirements(
                source,
                &plan,
                &stage_commit,
                &activation_commit,
                &proof,
            )?;
            // The same verifiers consumed these actual receipts. No synthetic
            // certificate/proposal identity is introduced for a callback.
            let _ = (source_retirements, data_reservation_retirements);
            self.queue_prior_native_topology(
                source,
                &stage_commit,
                &activation_commit,
                &proof,
                &plan.current_image_fingerprint,
                &plan.target_exact_prefix,
                installed_root,
                false,
            )?;
        }
        if source.original_handoff.is_none() {
            return Ok(());
        }
        let path = installed_root.join("native-import-plan.json");
        let plan: MetadataImportPlan =
            serde_json::from_slice(&std::fs::read(&path).map_err(error)?).map_err(error)?;
        Self::validate_plan(&plan)?;
        if digest(&plan.source_cut)? != digest(source)?
            || plan.expected_handoff.cause != Self::import_cause(source)?
        {
            return Err(error(
                "published original topology marker differs from actual native plan",
            ));
        }
        let activation_request =
            self.find_native_request(installed_root, &plan.expected_handoff, true)?;
        let proof = Self::activation_proof(&activation_request)?
            .ok_or_else(|| error("published topology has no actual activation blueprint"))?;
        if proof.handoff_id != plan.expected_handoff.handoff_id
            || proof.cause != plan.expected_handoff.cause
            || proof.retiring_shard != plan.expected_handoff.retiring_shard
            || proof.desired_shard != plan.expected_handoff.desired_shard
        {
            return Err(error(
                "published topology native activation changed its exact source/image scope",
            ));
        }
        Self::require_committed(&self.commit(plan.stage_request.clone())?)?;
        Self::require_committed(&self.commit(activation_request.clone())?)?;
        self.queue_original_topology_work(
            source,
            installed_root,
            proof.desired_shard,
            plan.stage_request,
            activation_request,
        )
    }

    fn queue_original_topology_work(
        &self,
        source: &MetadataSourceCut,
        installed_root: &Path,
        current: sinomo_dfs::DfsMetadataShardConfig,
        stage_request: KvTxnRequest,
        activation_request: KvTxnRequest,
    ) -> Result<()> {
        let original = source
            .original_handoff
            .as_ref()
            .ok_or_else(|| error("topology work lost original DATA"))?;
        let mut desired = original.desired_shard.clone();
        desired.epoch = current
            .epoch
            .checked_add(1)
            .ok_or_else(|| error("desired topology epoch exhausted"))?;
        if topology_matches(&current, &desired) {
            return Ok(());
        }
        let root = installed_root.canonicalize().map_err(error)?;
        self.validate_source_root(&root)?;
        let binding = digest(&(
            "imported-original-topology-v7",
            &source.original_handoff,
            &current,
            &desired,
        ))?;
        let data_dir = self.component_context.data_dir().clone();
        let authority = self.clone();
        sinomo_dfs::runtime_host::queue_namespace_reconciliation_work(
            &data_dir,
            Arc::clone(&self.owner),
            binding,
            true,
            Arc::new(move || {
                let run = || -> Result<bool> {
                    // A retained callback does not inherit a new component's
                    // permission. Reopen reconstructs/requeues with fresh DI.
                    let Some(watch) =
                        sinomo_dfs::dfs_storage_preparation_lifecycle(&authority.data_root)
                    else {
                        return Ok(false);
                    };
                    if !watch.same_channel(&authority.lifecycle)
                        || *authority.lifecycle.borrow()
                        || authority.lifecycle.has_changed().is_err()
                        || *watch.borrow()
                        || watch.has_changed().is_err()
                    {
                        return Ok(false);
                    }
                    let owner = sinomo_dfs::runtime_host::retain_component_data_owner(
                        &authority.component_context,
                    )
                    .map_err(error)?;
                    if !Arc::ptr_eq(&owner, &authority.owner) {
                        return Ok(false);
                    }
                    Self::require_committed(&authority.commit(stage_request.clone())?)?;
                    Self::require_committed(&authority.commit(activation_request.clone())?)?;
                    let mut control = authority.control_client(&root)?;
                    let mut store = sinomo_dfs::DfsControlPlaneStore::new(&mut control);
                    let actual = store
                        .get_metadata_shard(&current.file_system_id, &current.shard_id)
                        .map_err(error)?
                        .ok_or_else(|| error("current original-topology scope disappeared"))?;
                    if topology_matches(&actual, &desired) {
                        return Ok(true);
                    }
                    // Native epochs are monotone. A later unrelated choice
                    // supersedes this DATA; this callback cannot overwrite it.
                    if actual != current {
                        return Ok(true);
                    }
                    if authority.local_node()? != actual.primary_node_id {
                        return Ok(true);
                    }
                    let configs = store
                        .list_metadata_shards(&actual.file_system_id)
                        .map_err(error)?;
                    let pending = store
                        .begin_metadata_shard_membership_handoff(&actual, &desired)
                        .map_err(error)?;
                    let Some(pending) = pending else {
                        // A concurrently staged different handoff is an explicit
                        // newer control-plane choice, not permission to replace.
                        return Ok(true);
                    };
                    if pending.retiring_shard != actual
                        || pending.desired_shard != desired
                        || pending.cause != sinomo_dfs::DfsMetadataShardHandoffCause::Membership
                    {
                        return Err(error(
                            "ordinary topology stage changed its exact current identity",
                        ));
                    }
                    let mut service = sinomo_dfs::runtime_host::open_dfs_runtime_service_on_shard(
                        &data_dir_for(&authority),
                        Some(&authority.runtime),
                        &actual,
                    )
                    .map_err(error)?;
                    service
                        .reconcile_pending_metadata_shard_membership_handoffs(
                            &configs,
                            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
                        )
                        .map_err(error)?;
                    let installed = sinomo_dfs::DfsControlPlaneStore::new(&mut control)
                        .get_metadata_shard(&actual.file_system_id, &actual.shard_id)
                        .map_err(error)?;
                    Ok(installed
                        .as_ref()
                        .is_some_and(|cfg| topology_matches(cfg, &desired)))
                };
                run().map_err(|e| sinomo_dfs::runtime_host::DfsRuntimeError::Parse {
                    detail: e.to_string(),
                })
            }),
        )
        .map_err(error)
    }
}
fn data_dir_for(authority: &NativeImportAuthority) -> sinomo_core::local::SinomoDataDir {
    authority.component_context.data_dir().clone()
}
fn topology_matches(
    actual: &sinomo_dfs::DfsMetadataShardConfig,
    desired: &sinomo_dfs::DfsMetadataShardConfig,
) -> bool {
    let mut same_epoch = desired.clone();
    same_epoch.epoch = actual.epoch;
    actual == &same_epoch
}
