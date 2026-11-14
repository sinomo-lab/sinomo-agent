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
            let capability = sinomo_core::node::DFS_METADATA_NAMESPACE_COUNTS_V1_CAPABILITY;
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
                return Err(error(
                    "count cohort requires actual count-aware binaries and permission on every participant",
                ));
            }
        }
        Ok(())
    }
    fn count_plan_location(
        &self,
        seal: &NamespaceCountCohortSeal,
        shard: &DfsMetadataShardId,
    ) -> Result<Option<PathBuf>> {
        seal.member(shard)?;
        let destination = self.metadata_root.join(&shard.0);
        let staged = crate::metadata::namespace_count_plan_root(&destination, shard)?;
        for root in [destination, staged] {
            let path = root.join("native-import-plan.json");
            if path.is_file() {
                let plan = self.retained_plan_file.read(&path)?;
                if plan.source_cut.namespace_count_cohort.as_deref() == Some(seal) {
                    return Ok(Some(root));
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
            if root.join("native-import-plan.json").is_file() {
                let plan = self
                    .retained_plan_file
                    .read(&root.join("native-import-plan.json"))?;
                if plan.source_cut.namespace_count_cohort.as_deref() == Some(seal) {
                    return Ok(Some(root));
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
        self.require_count_capabilities(&request.seal.catalog)?;
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
        let Some(root) = self.count_plan_location(&request.seal, &request.shard)? else {
            return Ok(ProbeResult::Pending);
        };
        let plan = self
            .retained_plan_file
            .read(&root.join("native-import-plan.json"))?;
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
            return Ok(ProbeResult::Activated { retirement });
        }
        if request.activated_only {
            return Ok(ProbeResult::Pending);
        }
        let (_, proof) = self.qualify_handoff(&plan, &root, &root)?;
        Ok(ProbeResult::Staged {
            plan: (*plan).clone(),
            proof,
        })
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
        let result = block_native(
            &self.runtime,
            transport.call_async(target, call, source_effect_rpc_options(), true),
        )?;
        let response = block_native(
            &self.runtime,
            result.into_materialized_value_response(4 * 1024 * 1024),
        )?;
        response.decode_body().map_err(error)
    }
    pub(super) fn require_count_cohort_staged(&self, plan: &MetadataImportPlan) -> Result<()> {
        let Some(seal) = plan.source_cut.namespace_count_cohort.as_deref() else {
            return Ok(());
        };
        seal.validate_bound_cut(&plan.source_cut)?;
        self.require_count_capabilities(&seal.catalog)?;
        for member in &seal.members {
            let shard = &member.original_cut.scope_shard;
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
                        let pending: sinomo_dfs::DfsMetadataShardMembershipHandoff = rows[0]
                            .entry
                            .as_ref()
                            .ok_or_else(|| error("count cohort source fence vanished"))?
                            .decode()
                            .map_err(error)?;
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
                    }
                }
            }
        }
        Ok(())
    }
    /// A decoded binding is an obligation, not release authority. The existing
    /// native KV compares cannot exclude a newly inserted catalog member, so
    /// complete-set conversion release is unavailable. No persisted activation
    /// flag can substitute for that missing complete-catalog publication proof.
    pub(super) fn count_initializations_ready(
        &self,
        _shard: &DfsMetadataShardId,
        obligations: &BTreeSet<(sinomo_dfs::DfsFileSystemId, String)>,
    ) -> Result<bool> {
        Ok(obligations.is_empty())
    }
    #[cfg(test)]
    fn published_count_cohort(&self, seal: &NamespaceCountCohortSeal) -> Result<bool> {
        let fs = &seal.catalog[0].file_system_id;
        let (rows, _) = self.read_exact(&[sinomo_dfs::file_system_config_key(fs)])?;
        let Some(entry) = rows[0].entry.as_ref() else {
            return Ok(false);
        };
        let config: sinomo_dfs::DfsFileSystemConfig = entry.decode().map_err(error)?;
        if config.file_system_id != *fs {
            return Err(error("count activation configuration changed filesystem"));
        }
        let Some(activation) = config.namespace_count_activation else {
            return Ok(false);
        };
        activation.validate().map_err(error)?;
        if activation.initialization_id != seal.initialization_id
            || activation.cohort_id != seal.cohort_id
        {
            return Err(error(
                "filesystem already activated a different count cohort",
            ));
        }
        Ok(true)
    }
    pub(super) fn count_cohort_ready(&self, source: &MetadataSourceCut) -> Result<bool> {
        let Some(seal) = source.namespace_count_cohort.as_deref() else {
            return Ok(true);
        };
        seal.validate_bound_cut(source)?;
        // Native stage/fence/activation recovery remains available, but final
        // ordinary release is deliberately unresolved. Exact comparisons of
        // all observed shard rows cannot prove absence of a concurrent new
        // shard. Until the owning complete-catalog guard is approved and
        // implemented, neither local installation nor a compact FS activation
        // record can grant Ready. There is no release CAS on this path.
        Ok(false)
    }
}

#[cfg(test)]
mod native_tests;
#[cfg(test)]
mod remote_tests;
