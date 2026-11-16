//! Count cohorts reuse the actual native stage/retirement owners. A peer's
//! recorded blueprint is DATA; current source fences and native receipts are
//! independently reobserved before activation or ordinary storage readiness.
use super::*;
use crate::namespace_count_cohort::NamespaceCountCohortSeal;
use sinomo_dfs::{DfsMetadataShardId, DfsMetadataShardMembershipHandoffProof};

pub(super) const PROBE_ROUTE: &str = "/dfs-migration/namespace-count-cohort-probe-v1";
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Probe {
    pub seal: NamespaceCountCohortSeal,
    pub shard: DfsMetadataShardId,
    pub activated_only: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "kebab-case")]
pub(super) enum ProbeResult {
    Pending,
    Staged {
        #[serde(with = "super::plan_wire")]
        plan: MetadataImportPlan,
        proof: DfsMetadataShardMembershipHandoffProof,
    },
    Activated {
        retirement: super::namespace_work::RetirementBlueprint,
    },
}
impl NativeImportAuthority {
    pub(super) fn require_count_capabilities(
        &self,
        catalog: &[DfsMetadataShardConfig],
    ) -> Result<()> {
        if self.count_capabilities_ready(catalog)? { return Ok(()); }
        Err(MetadataBackendError::StoragePreparationPending {
            root: self.metadata_root.clone(), processed_records: 0,
        })
    }

    /// Binary advertisements and effective permission can arrive after the
    /// source owner mounts. Their absence parks work; it never grants permission
    /// or turns mutable configuration into corrupt retained source DATA.
    pub(super) fn count_capabilities_ready(
        &self,
        catalog: &[DfsMetadataShardConfig],
    ) -> Result<bool> {
        let nodes = catalog
            .iter()
            .flat_map(|s| {
                std::iter::once(s.primary_node_id.clone()).chain(s.replica_node_ids.clone())
            })
            .collect::<BTreeSet<_>>();
        let keys = nodes
            .iter()
            .flat_map(|node| {
                [
                    sinomo_core::node::cluster_member_key(node),
                    sinomo_core::node::cluster_capability_policy_key(node),
                ]
            })
            .collect::<Vec<_>>();
        let (entries, _) = self.read_exact(&keys)?;
        self.require_current_assignment_members(catalog, &entries)?;
        for node in nodes {
            let member: sinomo_core::ClusterMemberRecord = entries
                .iter()
                .find(|row| row.key == sinomo_core::node::cluster_member_key(&node))
                .and_then(|row| row.entry.as_ref())
                .ok_or_else(|| error("count cohort member is absent"))?
                .decode()
                .map_err(error)?;
            let policy = entries
                .iter()
                .find(|row| row.key == sinomo_core::node::cluster_capability_policy_key(&node))
                .and_then(|row| row.entry.as_ref())
                .map(|row| row.decode::<sinomo_core::node::ClusterCapabilityPolicyRecord>())
                .transpose()
                .map_err(error)?;
            let capability = sinomo_core::node::DFS_METADATA_STORAGE_GENERATION_V1_CAPABILITY;
            if !member
                .capabilities
                .iter()
                .any(|value| value.as_str() == capability)
                || !policy
                    .as_ref()
                    .map_or(member.capabilities.as_slice(), |policy| {
                        policy.capabilities.as_slice()
                    })
                    .iter()
                    .any(|value| value.as_str() == capability)
            {
                return Ok(false);
            }
        }
        Ok(true)
    }
    fn count_plan_location(
        &self,
        seal: &NamespaceCountCohortSeal,
        shard: &DfsMetadataShardId,
    ) -> Result<Option<(PathBuf, Arc<MetadataImportPlan>)>> {
        seal.member(shard)?;
        let destination = self.metadata_root.join(&shard.0);
        let staged = crate::metadata::namespace_count_plan_root(&destination, shard)?;
        for root in [destination, staged] {
            let path = root.join("native-import-plan.json");
            if let Some(plan) = self.retained_plan_file.read_if_present(&path)? {
                if plan.source_cut.namespace_count_cohort.as_deref() == Some(seal) {
                    return Ok(Some((root, plan)));
                }
            }
        }
        // An activated receiver may not have physically exchanged its image yet.
        let member = seal.member(shard)?;
        let key = sinomo_dfs::metadata_shard_membership_handoff_proof_key(
            &member.assignment.file_system_id,
            &shard.0,
        );
        let (rows, _) = self.read_exact(&[key])?;
        if let Some(entry) = rows[0].entry.as_ref() {
            let proof: DfsMetadataShardMembershipHandoffProof = entry.decode().map_err(error)?;
            let root = self.receiver_root_for(
                &member.assignment.file_system_id,
                &shard.0,
                &proof.handoff_id,
            )?;
            if let Some(plan) = self.retained_plan_file.read_if_present(&root.join("native-import-plan.json"))? {
                if plan.source_cut.namespace_count_cohort.as_deref() == Some(seal) {
                    return Ok(Some((root, plan)));
                }
            }
        }
        Ok(None)
    }
    pub(super) fn receive_count_probe(
        &self,
        source: sinomo_core::node::NodeId,
        request: Probe,
    ) -> Result<ProbeResult> {
        request.seal.validate()?;
        if !self.count_capabilities_ready(&request.seal.catalog)? {
            return Ok(ProbeResult::Pending);
        }
        let member = request.seal.member(&request.shard)?;
        if member.assignment.primary_node_id != self.local_node()?
            || !request
                .seal
                .catalog
                .iter()
                .any(|s| s.primary_node_id == source)
        {
            return Err(error(
                "count cohort probe has a foreign native source owner",
            ));
        }
        let Some((root, plan)) = self.count_plan_location(&request.seal, &request.shard)? else {
            return Ok(ProbeResult::Pending);
        };
        let observed = (|| {
            Self::validate_plan(&plan)?;
            if let Some(proof) = self.check_handoff_state(&plan)? {
                let activation_request =
                    self.find_native_request(&root, &plan.expected_handoff, true)?;
                if Self::activation_proof(&activation_request)?.as_ref() != Some(&proof) {
                    return Err(error(
                        "count cohort activation blueprint differs from actual native proof",
                    ));
                }
                let retirement = super::namespace_work::RetirementBlueprint {
                    plan: (*plan).clone(),
                    activation_request,
                };
                self.verify_retirement_blueprints(&plan.source_cut, std::slice::from_ref(&retirement))?;
                self.forget_count_stage(&plan);
                return Ok(ProbeResult::Activated { retirement });
            }
            if request.activated_only {
                return Ok(ProbeResult::Pending);
            }
            // A probe observes completed source work; it cannot stage every
            // participant inside this network read's caller budget. The same
            // background owner requalifies after restart or a missing observation.
            let Some(proof) = self.observed_count_stage(&plan, &root)? else {
                return Ok(ProbeResult::Pending);
            };
            Ok(ProbeResult::Staged {
                plan: (*plan).clone(),
                proof,
            })
        })();
        match observed {
            Err(MetadataBackendError::StoragePreparationPending { .. }) => Ok(ProbeResult::Pending),
            Err(MetadataBackendError::Io { path, source })
                if path.starts_with(&root) && source.kind() == std::io::ErrorKind::NotFound => {
                    // The probe owns no image or publication. A relocating
                    // candidate must be found and freshly verified next turn.
                    Ok(ProbeResult::Pending)
                }
            result => result,
        }
    }
    fn count_probe(
        &self,
        seal: &NamespaceCountCohortSeal,
        shard: &DfsMetadataShardId,
        activated_only: bool,
    ) -> Result<ProbeResult> {
        let target = &seal.member(shard)?.assignment.primary_node_id;
        let request = Probe {
            seal: seal.clone(),
            shard: shard.clone(),
            activated_only,
        };
        if *target == self.local_node()? {
            return self.receive_count_probe(self.local_node()?, request);
        }
        let transport = self
            .transport
            .as_ref()
            .ok_or_else(|| error("count cohort requires the actual native peer transport"))?;
        let call = sinomo_core::network::ObjectRpcCall::encode_borrowed_value(
            PROBE_ROUTE,
            "dfs-migration",
            &request,
        )
        .map_err(error)?;
        let result = self.read_peer(transport.call_async(target, call, source_effect_rpc_options(), true))?;
        let response = self.read_peer(result.into_materialized_value_response(4 * 1024 * 1024))?;
        response.decode_body().map_err(error)
    }
    pub(super) fn require_count_cohort_staged(&self, plan: &MetadataImportPlan) -> Result<()> {
        let Some(seal) = plan.source_cut.namespace_count_cohort.as_deref() else {
            return Ok(());
        };
        seal.validate_bound_cut(&plan.source_cut)?;
        self.require_count_capabilities(&seal.catalog)?;
        for member in &seal.members {
            let shard = &member.original_source.physical_shard;
            match self.count_probe(seal, shard, false)? {
                ProbeResult::Pending => {
                    return Err(MetadataBackendError::StoragePreparationPending {
                        root: self.metadata_root.join(&plan.source_cut.scope_shard.0),
                        processed_records: 0,
                    });
                }
                ProbeResult::Activated { retirement } => {
                    if retirement.plan.source_cut.namespace_count_cohort.as_deref() != Some(seal)
                        || retirement.plan.expected_handoff.retiring_shard != member.assignment
                    {
                        return Err(error("count cohort activation belongs to another set"));
                    }
                    self.verify_retirement_blueprints(
                        &retirement.plan.source_cut,
                        std::slice::from_ref(&retirement),
                    )?;
                }
                ProbeResult::Staged { plan: other, proof } => {
                    Self::validate_plan(&other)?;
                    Self::validate_joint_acknowledgements(&proof)?;
                    if other.source_cut.namespace_count_cohort.as_deref() != Some(seal)
                        || proof.retiring_shard != member.assignment
                        || proof.desired_shard != other.expected_handoff.desired_shard
                        || proof.cause != Self::import_cause(&other.source_cut)?
                        || proof.journal_sequence != member.initialized_prefix.journal_sequence
                        || proof.state_fingerprint != member.initialized_state_fingerprint
                    {
                        return Err(error(
                            "count cohort stage proof changes its exact source/candidate",
                        ));
                    }
                    if self.check_handoff_state(&other)?.is_none() {
                        let key = sinomo_dfs::metadata_shard_membership_handoff_key(
                            &member.assignment.file_system_id,
                            &shard.0,
                        );
                        let (rows, _) = self.read_exact(&[key])?;
                        let Some(entry) = rows[0].entry.as_ref() else {
                            if self.check_handoff_state(&other)?.is_some() { continue; }
                            return Err(error("count cohort source fence vanished without its exact activation"));
                        };
                        let pending: sinomo_dfs::DfsMetadataShardMembershipHandoff = entry.decode().map_err(error)?;
                        if pending.retiring_primary_fence.as_ref().is_none_or(|fence| {
                            fence.handoff_id != proof.handoff_id
                                || fence.primary_node_id != member.assignment.primary_node_id
                                || fence.journal_sequence
                                    != other.source_cut.source_journal_sequence
                                || fence.state_fingerprint
                                    != other.source_cut.source_state_fingerprint
                        }) {
                            return Err(error(
                                "count cohort has no actual complete-set retiring fence",
                            ));
                        }
                        // Every activation advances the SAME filesystem catalog
                        // entry. Preserve immutable native request bodies by
                        // waiting for earlier catalog members before choosing
                        // this activation's predecessor, rather than racing
                        // sibling compare-and-swap requests against that entry.
                        if shard.0 < plan.source_cut.scope_shard.0 {
                            #[cfg(test)]
                            super::owner::filesystem::observe_cohort_activation_wait(self, plan)?;
                            return Err(MetadataBackendError::StoragePreparationPending {
                                root: self.metadata_root.join(&plan.source_cut.scope_shard.0),
                                processed_records: 0,
                            });
                        }
                    }
                }
            }
        }
        #[cfg(test)]
        super::owner::filesystem::observe_cohort_qualified(self, plan)?;
        Ok(())
    }
    pub(super) fn published_count_cohort(&self, seal: &NamespaceCountCohortSeal) -> Result<bool> {
        seal.validate()?;
        let fs = &seal.catalog[0].file_system_id;
        let (rows, _) = match self.read_exact_evaluated(&[sinomo_dfs::file_system_config_key(fs)]) {
            Ok(read) => read,
            Err(NativeEvaluationError::Native(failure)) => {
                tracing::warn!(error=%failure, "filesystem activation awaits native authority");
                return Err(self.native_read_error(failure));
            }
            Err(NativeEvaluationError::Domain(failure)) => return Err(failure),
        };
        let Some(entry) = rows[0].entry.as_ref() else {
            return Ok(false);
        };
        let config: sinomo_dfs::DfsFileSystemConfig = entry.decode().map_err(error)?;
        active_matches(seal, &config)
    }
    /// Release only this exact complete cohort. The caller is the single
    /// filesystem job owner and supplies its already-owned checkpoint directory.
    /// A false result preserves all fences and original requests for resumption.
    pub(super) fn release_count_cohort(
        &self,
        seal: &NamespaceCountCohortSeal,
        checkpoint_root: &Path,
    ) -> Result<bool> {
        seal.validate()?;
        if self.published_count_cohort(seal)? {
            return Ok(true);
        }
        let coordinator = seal
            .catalog
            .iter()
            .map(|row| &row.primary_node_id)
            .min()
            .ok_or_else(|| error("storage release has no original coordinator"))?;
        if coordinator != &self.local_node()? {
            return Err(error(
                "only the original filesystem coordinator can release its cohort",
            ));
        }
        let root = checkpoint_root.canonicalize().map_err(error)?;
        if !root.starts_with(&self.data_root) {
            return Err(error(
                "storage release checkpoint is outside its actual data owner",
            ));
        }
        if !self.count_capabilities_ready(&seal.catalog)? { return Ok(false); }
        let mut proofs = BTreeMap::new();
        for member in &seal.members {
            let shard = &member.original_source.physical_shard;
            let name = format!("filesystem-retirement-{}.json", shard.0);
            let path = root.join(&name);
            let retirement: super::namespace_work::RetirementBlueprint = if path.exists() {
                let bytes = sinomo_core::storage::durable_file::read_object_file_bytes(
                    &path,
                    64 * 1024 * 1024,
                )
                .map_err(error)?;
                serde_json::from_slice(&bytes).map_err(error)?
            } else {
                let ProbeResult::Activated { retirement } = self.count_probe(seal, shard, true)?
                else {
                    return Ok(false);
                };
                retirement
            };
            if retirement.plan.source_cut.namespace_count_cohort.as_deref() != Some(seal)
                || retirement.plan.expected_handoff.retiring_shard != member.assignment
            {
                return Err(error(
                    "filesystem release changes an original retirement scope",
                ));
            }
            self.verify_retirement_blueprints(
                &retirement.plan.source_cut,
                std::slice::from_ref(&retirement),
            )?;
            let proof = Self::activation_proof(&retirement.activation_request)?
                .ok_or_else(|| error("filesystem release lacks actual activation proof"))?;
            validate_release_proof(seal, member, &proof)?;
            Self::save_blueprint(&root, &name, &retirement)?;
            proofs.insert(shard.clone(), proof);
        }
        let fs = &seal.catalog[0].file_system_id;
        let catalog_prefix = format!("{}/", sinomo_dfs::metadata_shards_prefix(fs));
        let range = block_native(&self.runtime, async {
            Ok::<_, MetadataBackendError>(
                self.cluster()?
                    .kv_range_prefix_complete_authoritative_async(catalog_prefix.clone())
                    .await,
            )
        })?;
        let range = match range {
            Ok(range) => range,
            Err(failure) => {
                tracing::warn!(error=%failure, "filesystem release awaits native catalog authority");
                return Err(self.native_read_error(failure));
            }
        };
        sinomo_core::kv::validate_cluster_kv_authoritative_complete_range_prefix_read(
            &catalog_prefix,
            &range,
        )
        .map_err(error)?;
        current_authority(&range.authority)?;
        let expected = seal
            .catalog
            .iter()
            .map(|original| {
                let mut desired = original.clone();
                desired.epoch = desired
                    .epoch
                    .checked_add(1)
                    .ok_or_else(|| error("storage release epoch exhausted"))?;
                Ok((
                    sinomo_dfs::metadata_shard_key(fs, &desired.shard_id),
                    desired,
                ))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        if range.value.len() != expected.len() {
            return Err(error(
                "filesystem release catalog has a missing or additional shard",
            ));
        }
        for entry in &range.value {
            let actual: DfsMetadataShardConfig = entry.decode().map_err(error)?;
            if expected.get(&entry.key) != Some(&actual) {
                return Err(error(
                    "filesystem release catalog differs from the exact activated cohort",
                ));
            }
        }
        let fs_key = sinomo_dfs::file_system_config_key(fs);
        let mut keys = vec![fs_key.clone()];
        keys.extend(expected.keys().cloned());
        keys.extend(seal.members.iter().map(|member| {
            sinomo_dfs::metadata_shard_membership_handoff_proof_key(fs, &member.assignment.shard_id)
        }));
        let read = self.read_exact_evaluated(&keys);
        let (entries, _) = match read {
            Ok(read) => read,
            Err(NativeEvaluationError::Native(failure)) => {
                tracing::warn!(error=%failure, "filesystem release awaits its exact native cut");
                return Err(self.native_read_error(failure));
            }
            Err(NativeEvaluationError::Domain(failure)) => return Err(failure),
        };
        let entry = |key: &str| -> Result<&KvEntry> {
            let actual = entries
                .iter()
                .find(|row| row.key == key)
                .and_then(|row| row.entry.as_ref())
                .ok_or_else(|| error("filesystem release exact-set entry disappeared"))?;
            if actual.key != key {
                return Err(error("filesystem release exact-set entry changed key"));
            }
            Ok(actual)
        };
        let fs_entry = entry(&fs_key)?;
        let current: sinomo_dfs::DfsFileSystemConfig = fs_entry.decode().map_err(error)?;
        if active_matches(seal, &current)? {
            return Ok(true);
        }
        validate_pending_release(seal, &current)?;
        let mut fixed = Vec::new();
        for (key, desired) in &expected {
            let actual = entry(key)?;
            if actual.decode::<DfsMetadataShardConfig>().map_err(error)? != *desired {
                return Err(error(
                    "filesystem release shard changed after catalog capture",
                ));
            }
            fixed.push(KvCompare::ValueEquals {
                key: key.clone(),
                value: actual.value.clone(),
            });
        }
        for member in &seal.members {
            let key = sinomo_dfs::metadata_shard_membership_handoff_proof_key(
                fs,
                &member.assignment.shard_id,
            );
            let actual = entry(&key)?;
            let proof: DfsMetadataShardMembershipHandoffProof = actual.decode().map_err(error)?;
            if proofs.get(&member.original_source.physical_shard) != Some(&proof) {
                return Err(error(
                    "filesystem release changed an actual original activation proof",
                ));
            }
            fixed.push(KvCompare::ValueEquals {
                key,
                value: actual.value.clone(),
            });
        }
        let attempt_path = root.join("filesystem-release-attempt.json");
        let request = if attempt_path.exists() {
            let bytes = sinomo_core::storage::durable_file::read_object_file_bytes(
                &attempt_path,
                64 * 1024 * 1024,
            )
            .map_err(error)?;
            let saved: ReleaseAttempt = serde_json::from_slice(&bytes).map_err(error)?;
            if saved.cohort_id != seal.cohort_id
                || saved.checksum != digest(&(&saved.cohort_id, &saved.request))?
            {
                return Err(error(
                    "filesystem release request checkpoint changed its original body",
                ));
            }
            let before = saved
                .request
                .txn
                .compares
                .iter()
                .find_map(|compare| match compare {
                    KvCompare::ValueEquals { key, value } if key == &fs_key => Some(value.clone()),
                    _ => None,
                })
                .ok_or_else(|| {
                    error("filesystem release request omitted its original filesystem compare")
                })?;
            let mut original_entry = fs_entry.clone();
            original_entry.value = before;
            let expected_request = release_request(seal, &original_entry, &fixed)?;
            if serde_json::to_vec(&saved.request).map_err(error)?
                != serde_json::to_vec(&expected_request).map_err(error)?
            {
                return Err(error(
                    "filesystem release request differs from its exact native source/candidate cut",
                ));
            }
            saved.request
        } else {
            let request = release_request(seal, fs_entry, &fixed)?;
            Self::save_blueprint(
                &root,
                &format!("filesystem-release-{}.json", request.request_id),
                &request,
            )?;
            let attempt = ReleaseAttempt {
                cohort_id: seal.cohort_id.clone(),
                checksum: digest(&(&seal.cohort_id, &request))?,
                request: request.clone(),
            };
            crate::metadata::write_count_cohort_checkpoint(&attempt_path, &attempt)?;
            request
        };
        let commit = match self.commit_evaluated(request) {
            Ok(commit) => commit,
            Err(NativeEvaluationError::Native(failure)) => {
                tracing::warn!(error=%failure, "filesystem release retains its uncertain original native request");
                return Err(self.native_read_error(failure));
            }
            Err(NativeEvaluationError::Domain(failure)) => return Err(failure),
        };
        if commit
            .result
            .txn_response
            .as_ref()
            .is_some_and(|response| response.succeeded)
        {
            Self::require_committed(&commit)?;
            return self.published_count_cohort(seal);
        }
        match commit.result.authority.basis {
            KvAuthorityBasis::CurrentTermQuorum { .. } => {
                current_authority(&commit.result.authority)?
            }
            KvAuthorityBasis::ExactCommittedRequest if commit.result.authority.term != 0 => {}
            _ => {
                return Err(error(
                    "failed filesystem release has no actual terminal native outcome",
                ));
            }
        }
        // Only a certified failure permits another CAS against changed ordinary
        // config fields. The immutable old request remains available for audit.
        sinomo_core::storage::durable_file::remove_file_if_exists_and_sync_parent(&attempt_path)
            .map_err(error)?;
        Ok(false)
    }
    pub(super) fn count_cohort_ready(&self, source: &MetadataSourceCut) -> Result<bool> {
        let Some(seal) = source.namespace_count_cohort.as_deref() else {
            return Ok(true);
        };
        seal.validate_bound_cut(source)?;
        self.published_count_cohort(seal)
    }
}


#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReleaseAttempt {
    cohort_id: String,
    request: KvTxnRequest,
    checksum: String,
}

fn release_generation(seal: &NamespaceCountCohortSeal) -> Result<u64> {
    seal.source_catalog_generation
        .checked_add(u64::try_from(seal.members.len()).map_err(error)?)
        .ok_or_else(|| error("storage transition catalog generation exhausted"))
}
fn activation_digest(seal: &NamespaceCountCohortSeal) -> Result<String> {
    Ok(format!(
        "blake3:{}",
        digest(&(
            "filesystem-storage-activation-v1",
            &seal.cohort_id,
            &seal.target_generation,
            seal.target_format,
            release_generation(seal)?
        ))?
    ))
}
fn active_matches(
    seal: &NamespaceCountCohortSeal,
    config: &sinomo_dfs::DfsFileSystemConfig,
) -> Result<bool> {
    if config.file_system_id != seal.catalog[0].file_system_id {
        return Err(error("storage activation changed filesystem"));
    }
    config
        .metadata_storage
        .as_ref()
        .map(|state| state.validate_at_catalog_generation(config.catalog_generation))
        .transpose()
        .map_err(error)?;
    Ok(
        matches!(&config.metadata_storage, Some(sinomo_dfs::DfsMetadataStorageState::Active {
        generation, target_format, catalog_generation, activation_digest: actual,
    }) if generation == &seal.target_generation && *target_format == seal.target_format
        && *catalog_generation == release_generation(seal)? && *actual == activation_digest(seal)?),
    )
}
fn validate_pending_release(
    seal: &NamespaceCountCohortSeal,
    config: &sinomo_dfs::DfsFileSystemConfig,
) -> Result<()> {
    if config.file_system_id != seal.catalog[0].file_system_id
        || config.catalog_generation != release_generation(seal)?
    {
        return Err(error(
            "filesystem release catalog generation includes missing, foreign, or repeated changes",
        ));
    }
    config
        .metadata_storage
        .as_ref()
        .ok_or_else(|| error("filesystem release lost Pending state"))?
        .validate_at_catalog_generation(config.catalog_generation)
        .map_err(error)?;
    if !matches!(&config.metadata_storage, Some(sinomo_dfs::DfsMetadataStorageState::PendingTransition {
        transition_id, source_catalog_generation, target_generation, target_format, expected_catalog_digest, ..
    }) if transition_id == &seal.transition_id && *source_catalog_generation == seal.source_catalog_generation
        && target_generation == &seal.target_generation && *target_format == seal.target_format
        && expected_catalog_digest == &seal.catalog_digest)
    {
        return Err(error(
            "filesystem release changed its original Pending identity",
        ));
    }
    Ok(())
}
fn release_request(
    seal: &NamespaceCountCohortSeal,
    fs: &KvEntry,
    fixed: &[KvCompare],
) -> Result<KvTxnRequest> {
    let mut config: sinomo_dfs::DfsFileSystemConfig = fs.decode().map_err(error)?;
    validate_pending_release(seal, &config)?;
    if fs.key != sinomo_dfs::file_system_config_key(&config.file_system_id) {
        return Err(error("release filesystem key changed"));
    }
    config.metadata_storage = Some(sinomo_dfs::DfsMetadataStorageState::Active {
        generation: seal.target_generation.clone(),
        target_format: seal.target_format,
        catalog_generation: release_generation(seal)?,
        activation_digest: activation_digest(seal)?,
    });
    let mut compares = vec![KvCompare::ValueEquals {
        key: fs.key.clone(),
        value: fs.value.clone(),
    }];
    compares.extend_from_slice(fixed);
    let txn = KvTxn {
        compares,
        success: vec![KvOp::put_serde(fs.key.clone(), &config, fs.lease_id).map_err(error)?],
        failure: Vec::new(),
    };
    Ok(KvTxnRequest::new(
        KvRequestId::derived(
            "dfs-filesystem-storage-release-v1",
            digest(&txn)?.as_bytes(),
        ),
        txn,
    ))
}
fn validate_release_proof(
    seal: &NamespaceCountCohortSeal,
    member: &crate::namespace_count_cohort::NamespaceCountCohortMember,
    proof: &DfsMetadataShardMembershipHandoffProof,
) -> Result<()> {
    NativeImportAuthority::validate_joint_acknowledgements(proof)?;
    let mut desired = member.assignment.clone();
    desired.epoch = desired
        .epoch
        .checked_add(1)
        .ok_or_else(|| error("storage release epoch exhausted"))?;
    if proof.retiring_shard != member.assignment
        || proof.desired_shard != desired
        || proof.journal_sequence != member.initialized_prefix.journal_sequence
        || proof.state_fingerprint != member.initialized_state_fingerprint
        || !matches!(&proof.cause, sinomo_dfs::DfsMetadataShardHandoffCause::StorageImport { cut }
            if cut.storage_transition_id.as_ref() == Some(&seal.transition_id)
                && cut.target_prefix == member.initialized_prefix
                && cut.converted_image_fingerprint == member.initialized_state_fingerprint)
    {
        return Err(error(
            "filesystem release proof changes its exact original storage transition",
        ));
    }
    Ok(())
}
