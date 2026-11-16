//! Retained request DATA for a configured receiver. Neither this response nor
//! its local copy establishes an outcome or installation authority.
use super::*;

pub(super) const ROUTE: &str = "/dfs-migration/activated-request-data-v7";
const MAX_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Request {
    file_system_id: sinomo_dfs::DfsFileSystemId,
    shard_id: sinomo_dfs::DfsMetadataShardId,
    handoff_id: String,
    stage_request_id: KvRequestId,
    plan_digest: String,
}

impl Request {
    pub(super) fn for_plan(
        plan: &MetadataImportPlan,
        proof: &sinomo_dfs::DfsMetadataShardMembershipHandoffProof,
    ) -> Result<Self> {
        Ok(Self {
            file_system_id: proof.desired_shard.file_system_id.clone(),
            shard_id: sinomo_dfs::DfsMetadataShardId::new(&proof.desired_shard.shard_id),
            handoff_id: proof.handoff_id.clone(),
            stage_request_id: plan.stage_request.request_id.clone(),
            plan_digest: digest(plan)?,
        })
    }
}

impl NativeImportAuthority {
    pub(super) fn activated_request_data(
        &self,
        caller: sinomo_core::node::NodeId,
        request: Request,
    ) -> Result<Option<KvTxnRequest>> {
        owner::validate_metadata_scope(&request.shard_id)?;
        let keys = [
            sinomo_dfs::metadata_shard_key(&request.file_system_id, &request.shard_id.0),
            sinomo_dfs::metadata_shard_membership_handoff_proof_key(
                &request.file_system_id,
                &request.shard_id.0,
            ),
        ];
        let (entries, _) = self.read_exact(&keys)?;
        let current: DfsMetadataShardConfig = entries[0]
            .entry
            .as_ref()
            .ok_or_else(|| error("activation DATA has no current shard"))?
            .decode()
            .map_err(error)?;
        let proof: sinomo_dfs::DfsMetadataShardMembershipHandoffProof = entries[1]
            .entry
            .as_ref()
            .ok_or_else(|| error("activation DATA has no actual native proof"))?
            .decode()
            .map_err(error)?;
        Self::validate_joint_acknowledgements(&proof)?;
        if !matches!(
            proof.cause,
            sinomo_dfs::DfsMetadataShardHandoffCause::StorageImport { .. }
        ) || proof.handoff_id != request.handoff_id
            || !sinomo_dfs::metadata_shard_storage_scope_matches(&proof.desired_shard, &current)
            || current.file_system_id != request.file_system_id
            || current.shard_id != request.shard_id.0
            || (caller != current.primary_node_id && !current.replica_node_ids.contains(&caller))
            || !proof.acknowledged_node_ids.contains(&self.local_node()?)
        {
            return Err(error(
                "activation DATA caller, holder or current native scope differs",
            ));
        }
        let roots = [
            self.metadata_root.join(&request.shard_id.0),
            self.receiver_root_for(
                &request.file_system_id,
                &request.shard_id.0,
                &request.handoff_id,
            )?,
        ];
        for root in roots {
            let path = root.join("native-import-plan.json");
            if !path.try_exists().map_err(error)? {
                continue;
            }
            self.validate_source_root(&root)?;
            let plan = self.retained_plan_file.read(&path)?;
            if plan.expected_handoff.handoff_id != request.handoff_id {
                continue;
            }
            if plan.stage_request.request_id != request.stage_request_id
                || digest(plan.as_ref())? != request.plan_digest
                || self.check_handoff_state(&plan)? != Some(proof.clone())
            {
                return Err(error(
                    "activation DATA differs from its exact original plan",
                ));
            }
            let path = root.join("native-activation-request.json");
            if !path.try_exists().map_err(error)? {
                continue;
            }
            let bytes =
                sinomo_core::storage::durable_file::read_object_file_bytes(&path, MAX_BYTES as u64)
                    .map_err(error)?;
            let original: KvTxnRequest = serde_json::from_slice(&bytes).map_err(error)?;
            self.validate_activation_request(&original, &plan, &proof)?;
            if self.check_handoff_state(&plan)? != Some(proof.clone()) {
                return Err(error("activation DATA native scope changed during lookup"));
            }
            return Ok(Some(original));
        }
        Ok(None)
    }

    pub(super) fn original_receiver_activation_request(
        &self,
        root: &Path,
        plan: &MetadataImportPlan,
        proof: &sinomo_dfs::DfsMetadataShardMembershipHandoffProof,
    ) -> Result<KvTxnRequest> {
        if root
            .join("native-activation-request.json")
            .try_exists()
            .map_err(error)?
        {
            return self.find_native_request(root, &plan.expected_handoff, true);
        }
        let request = Request::for_plan(plan, proof)?;
        let transport = self
            .transport
            .as_ref()
            .ok_or_else(|| error("activation DATA needs actual native transport"))?;
        let local = self.local_node()?;
        for peer in &proof.acknowledged_node_ids {
            let result = if peer == &local {
                self.activated_request_data(local.clone(), request.clone())
            } else {
                (|| {
                    let call = sinomo_core::network::ObjectRpcCall::encode_borrowed_value(
                        ROUTE,
                        "dfs-migration",
                        &request,
                    )
                    .map_err(error)?;
                    let response = self.read_peer(transport.call_async(
                        peer,
                        call,
                        source_effect_rpc_options(),
                        true,
                    ))?;
                    self.read_peer(response.into_materialized_value_response(MAX_BYTES))?
                        .decode_body()
                        .map_err(error)
                })()
            };
            match result {
                Ok(Some(original)) => {
                    self.validate_activation_request(&original, plan, proof)?;
                    return Ok(original);
                }
                Ok(None) | Err(MetadataBackendError::StoragePreparationPending { .. }) => {}
                Err(failure) => return Err(failure),
            }
        }
        Err(MetadataBackendError::StoragePreparationPending {
            root: root.to_owned(),
            processed_records: 0,
        })
    }
}
