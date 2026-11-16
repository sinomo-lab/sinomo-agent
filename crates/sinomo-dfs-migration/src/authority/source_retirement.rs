//! Actual original-scope retirement. A converted target or later target epoch
//! cannot retire an operation that belonged to another original authority.
use super::*;

#[derive(Clone, Debug, Serialize)]
pub(crate) struct NativeSourceRetirementCertificate {
    pub(crate) file_system_id: sinomo_dfs::DfsFileSystemId,
    pub(crate) shard_id: sinomo_dfs::DfsMetadataShardId,
    pub(crate) owner_epoch: u64,
    pub(crate) operation_id: String,
    pub(crate) source_operation_digest: String,
    pub(crate) original_source_cut_digest: String,
    pub(crate) stage_commit: DfsNativeTxnCommit,
    pub(crate) activation_commit: DfsNativeTxnCommit,
    pub(crate) joint_handoff_proof: sinomo_dfs::DfsMetadataShardMembershipHandoffProof,
}

/// Retirement of exact archived reservation DATA, never fresh author permission.
/// No Deserialize or public constructor can mint this certificate.
#[derive(Clone, Debug, Serialize)]
pub(crate) struct NativeDataReservationRetirementCertificate {
    pub(crate) reservation: crate::metadata::MetadataDataReservationRef,
    pub(crate) original_source_cut_digest: String,
    pub(crate) stage_commit: DfsNativeTxnCommit,
    pub(crate) activation_commit: DfsNativeTxnCommit,
    pub(crate) joint_handoff_proof: sinomo_dfs::DfsMetadataShardMembershipHandoffProof,
}

#[derive(Deserialize)]
struct OriginalScope {
    file_system_id: sinomo_dfs::DfsFileSystemId,
    shard_id: sinomo_dfs::DfsMetadataShardId,
    owner_epoch: u64,
}

fn original_operation(
    row: &serde_json::Value,
) -> Result<Option<(OriginalScope, String, String, serde_json::Value)>> {
    let needs_retirement = row["disposition"].as_str()
        == Some("requires-original-source-retirement")
        || row["retired_reservation"].as_bool() == Some(true);
    if !needs_retirement {
        return Ok(None);
    }
    let payload = row
        .get("source_payload")
        .or_else(|| row.get("payload"))
        .ok_or_else(|| error("original retirement has no complete source payload"))?;
    let scope = row.get("source_scope").cloned().unwrap_or_else(|| {
        serde_json::json!({
            "file_system_id": payload["file_system_id"],
            "shard_id": payload["shard_id"],
            "owner_epoch": payload["owner_epoch"],
        })
    });
    let scope: OriginalScope = serde_json::from_value(scope).map_err(error)?;
    let operation_id = payload["operation_id"]
        .as_str()
        .ok_or_else(|| error("original retirement lacks operation identity"))?
        .to_owned();
    let operation_digest = row["source_operation_digest"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| error("original retirement lacks the captured operation digest"))?
        .to_owned();
    Ok(Some((
        scope,
        operation_id,
        operation_digest,
        payload.clone(),
    )))
}

impl NativeImportAuthority {
    pub(super) fn retirement_blueprints(
        &self,
        source: &MetadataSourceCut,
    ) -> Result<Vec<super::namespace_work::RetirementBlueprint>> {
        let mut scopes = BTreeSet::new();
        for fs in &source.file_system_ids {
            if source
                .projection_assignments
                .iter()
                .any(|cfg| cfg.file_system_id == *fs && cfg.shard_id == source.scope_shard.0)
            {
                scopes.insert((fs.clone(), source.scope_shard.clone()));
            }
        }
        for row in &source.pending_operations {
            if let Some((scope, _, _, _)) = original_operation(row)? {
                scopes.insert((scope.file_system_id, scope.shard_id));
            }
        }
        for reservation in crate::metadata::data_reservation_refs(source)? {
            scopes.insert((reservation.file_system_id, reservation.original_shard_id));
        }
        scopes
            .into_iter()
            .map(|(fs, shard)| {
                let (plan, stage, activation, _) =
                    self.recover_scope_retirement(&fs, &shard)?.ok_or_else(|| {
                        error("namespace work has no actual original activation blueprint")
                    })?;
                if stage.request != plan.stage_request {
                    return Err(error(
                        "namespace work stage differs from its actual import plan",
                    ));
                }
                Ok(super::namespace_work::RetirementBlueprint {
                    plan,
                    activation_request: activation.request,
                })
            })
            .collect()
    }

    pub(super) fn verify_retirement_blueprints(
        &self,
        source: &MetadataSourceCut,
        blueprints: &[super::namespace_work::RetirementBlueprint],
    ) -> Result<()> {
        let mut verified = BTreeMap::new();
        for blueprint in blueprints {
            Self::validate_plan(&blueprint.plan)?;
            let proof = Self::activation_proof(&blueprint.activation_request)?
                .ok_or_else(|| error("namespace work blueprint lacks its actual joint proof"))?;
            self.validate_activation_request(
                &blueprint.activation_request,
                &blueprint.plan,
                &proof,
            )?;
            self.original_scope_still_retired(&proof)?;
            let stage = self.observe_original_import_commit(
                &blueprint.plan,
                blueprint.plan.stage_request.clone(),
            )?;
            let activation = self.observe_original_import_commit(
                &blueprint.plan,
                blueprint.activation_request.clone(),
            )?;
            Self::require_committed(&stage)?;
            Self::require_committed(&activation)?;
            if !stage.result.replayed_committed_request
                || !activation.result.replayed_committed_request
            {
                return Err(error(
                    "namespace work requires already committed original receipts",
                ));
            }
            self.original_scope_still_retired(&proof)?;
            let scope = (
                proof.retiring_shard.file_system_id.clone(),
                sinomo_dfs::DfsMetadataShardId::new(&proof.retiring_shard.shard_id),
            );
            if verified.insert(scope, (&blueprint.plan, proof)).is_some() {
                return Err(error("namespace work repeats an original authority scope"));
            }
        }
        for row in &source.pending_operations {
            if let Some((scope, id, digest, payload)) = original_operation(row)? {
                let (plan, proof) = verified
                    .get(&(scope.file_system_id.clone(), scope.shard_id.clone()))
                    .ok_or_else(|| error("namespace work omits an original prepared scope"))?;
                Self::validate_original_retirement(&scope, &id, &digest, &payload, plan, proof)?;
            }
        }
        for reservation in crate::metadata::data_reservation_refs(source)? {
            let captured = source
                .data_reservation_capsules()
                .find(|row| {
                    row["capsule_digest"].as_str() == Some(reservation.capsule_digest.as_str())
                        && row["disposition"].as_str()
                            == Some("data-after-original-reservation-retirement")
                })
                .ok_or_else(|| error("namespace work lost its complete source capsule"))?;
            let (plan, proof) = verified
                .get(&(
                    reservation.file_system_id.clone(),
                    reservation.original_shard_id.clone(),
                ))
                .ok_or_else(|| error("namespace work omits an original data scope"))?;
            Self::validate_data_reservation_retirement(&reservation, captured, plan, proof)?;
        }
        Ok(())
    }
    pub(super) fn pending_original_sources(
        &self,
        source: &MetadataSourceCut,
    ) -> Result<Vec<(sinomo_dfs::DfsFileSystemId, sinomo_dfs::DfsMetadataShardId)>> {
        let mut pending = BTreeSet::new();
        for row in &source.pending_operations {
            let Some((scope, operation_id, operation_digest, payload)) = original_operation(row)?
            else {
                continue;
            };
            if scope.shard_id == source.scope_shard
                && source.projection_assignments.iter().any(|cfg| {
                    cfg.file_system_id == scope.file_system_id
                        && cfg.shard_id == source.scope_shard.0
                })
            {
                continue;
            }
            let Some((plan, _, _, proof)) = self.recover_original_retirement(&scope)? else {
                pending.insert((scope.file_system_id, scope.shard_id));
                continue;
            };
            Self::validate_original_retirement(
                &scope,
                &operation_id,
                &operation_digest,
                &payload,
                &plan,
                &proof,
            )?;
        }
        for reservation in crate::metadata::data_reservation_refs(source)? {
            if reservation.original_shard_id == source.scope_shard
                && source.projection_assignments.iter().any(|cfg| {
                    cfg.file_system_id == reservation.file_system_id
                        && cfg.shard_id == source.scope_shard.0
                })
            {
                continue;
            }
            let Some((plan, _, _, proof)) = self.recover_scope_retirement(
                &reservation.file_system_id,
                &reservation.original_shard_id,
            )?
            else {
                pending.insert((reservation.file_system_id, reservation.original_shard_id));
                continue;
            };
            let captured = source
                .data_reservation_capsules()
                .find(|row| {
                    row["capsule_digest"].as_str() == Some(reservation.capsule_digest.as_str())
                        && row["disposition"].as_str()
                            == Some("data-after-original-reservation-retirement")
                })
                .ok_or_else(|| error("data reservation lacks its exact captured capsule"))?;
            Self::validate_data_reservation_retirement(&reservation, captured, &plan, &proof)?;
        }
        Ok(pending.into_iter().collect())
    }

    pub(super) fn verify_original_source_retirements(
        &self,
        source: &MetadataSourceCut,
        target_plan: &MetadataImportPlan,
        target_stage: &DfsNativeTxnCommit,
        target_activation: &DfsNativeTxnCommit,
        target_proof: &sinomo_dfs::DfsMetadataShardMembershipHandoffProof,
    ) -> Result<Vec<NativeSourceRetirementCertificate>> {
        let mut certificates = Vec::new();
        for row in &source.pending_operations {
            let Some((scope, operation_id, operation_digest, payload)) = original_operation(row)?
            else {
                continue;
            };
            let same_scope = target_proof.retiring_shard.file_system_id == scope.file_system_id
                && target_proof.retiring_shard.shard_id == scope.shard_id.0;
            let (plan, stage_commit, activation_commit, proof) = if same_scope {
                (
                    target_plan.clone(),
                    target_stage.clone(),
                    target_activation.clone(),
                    target_proof.clone(),
                )
            } else {
                self.recover_original_retirement(&scope)?.ok_or_else(|| {
                    error("original source scope must complete its own native import first")
                })?
            };
            Self::validate_original_retirement(
                &scope,
                &operation_id,
                &operation_digest,
                &payload,
                &plan,
                &proof,
            )?;
            Self::require_committed(&stage_commit)?;
            Self::require_committed(&activation_commit)?;
            self.original_scope_still_retired(&proof)?;
            certificates.push(NativeSourceRetirementCertificate {
                file_system_id: scope.file_system_id,
                shard_id: scope.shard_id,
                owner_epoch: scope.owner_epoch,
                operation_id,
                source_operation_digest: operation_digest,
                original_source_cut_digest: digest(&plan.source_cut)?,
                stage_commit,
                activation_commit,
                joint_handoff_proof: proof,
            });
        }
        Ok(certificates)
    }

    pub(super) fn verify_data_reservation_retirements(
        &self,
        source: &MetadataSourceCut,
        target_plan: &MetadataImportPlan,
        target_stage: &DfsNativeTxnCommit,
        target_activation: &DfsNativeTxnCommit,
        target_proof: &sinomo_dfs::DfsMetadataShardMembershipHandoffProof,
    ) -> Result<Vec<NativeDataReservationRetirementCertificate>> {
        let mut certificates = Vec::new();
        for reservation in crate::metadata::data_reservation_refs(source)? {
            let captured = source
                .data_reservation_capsules()
                .find(|row| {
                    row["capsule_digest"].as_str() == Some(reservation.capsule_digest.as_str())
                        && row["disposition"].as_str()
                            == Some("data-after-original-reservation-retirement")
                })
                .ok_or_else(|| error("data reservation has no actual source capsule"))?;
            let same_scope = target_proof.retiring_shard.file_system_id
                == reservation.file_system_id
                && target_proof.retiring_shard.shard_id == reservation.original_shard_id.0;
            let (plan, stage_commit, activation_commit, proof) = if same_scope {
                (
                    target_plan.clone(),
                    target_stage.clone(),
                    target_activation.clone(),
                    target_proof.clone(),
                )
            } else {
                self.recover_scope_retirement(&reservation.file_system_id, &reservation.original_shard_id)?
                    .ok_or_else(|| error("original data reservation scope must complete its actual native import first"))?
            };
            Self::validate_data_reservation_retirement(&reservation, captured, &plan, &proof)?;
            Self::require_committed(&stage_commit)?;
            Self::require_committed(&activation_commit)?;
            Self::validate_joint_acknowledgements(&proof)?;
            self.original_scope_still_retired(&proof)?;
            certificates.push(NativeDataReservationRetirementCertificate {
                reservation,
                original_source_cut_digest: digest(&plan.source_cut)?,
                stage_commit,
                activation_commit,
                joint_handoff_proof: proof,
            });
        }
        Ok(certificates)
    }

    pub(super) fn validate_data_reservation_retirement(
        reservation: &crate::metadata::MetadataDataReservationRef,
        captured: &serde_json::Value,
        plan: &MetadataImportPlan,
        proof: &sinomo_dfs::DfsMetadataShardMembershipHandoffProof,
    ) -> Result<()> {
        Self::validate_plan(&plan)?;
        if proof.retiring_shard.file_system_id != reservation.file_system_id
            || proof.retiring_shard.shard_id != reservation.original_shard_id.0
            || proof.retiring_shard != plan.expected_handoff.retiring_shard
            || proof.desired_shard != plan.expected_handoff.desired_shard
            || proof.desired_shard.epoch <= proof.retiring_shard.epoch
            || proof.handoff_id != plan.expected_handoff.handoff_id
            || proof.cause != Self::import_cause(&plan.source_cut)?
        {
            return Err(error(
                "native import does not retire the exact original data reservation scope",
            ));
        }
        let original_refs = crate::metadata::data_reservation_refs(&plan.source_cut)?;
        if !original_refs.contains(reservation) {
            return Err(error(
                "original native source cut does not bind this exact data reservation reference",
            ));
        }
        let original_capsules = plan
            .source_cut
            .data_reservation_capsules()
            .filter(|row| {
                row["capsule_digest"].as_str() == Some(reservation.capsule_digest.as_str())
                    && row["disposition"].as_str()
                        == Some("data-after-original-reservation-retirement")
            })
            .collect::<Vec<_>>();
        if original_capsules.len() != 1
            || original_capsules[0]["source_row"] != captured["source_row"]
            || original_capsules[0]["original_source_scope"] != captured["original_source_scope"]
        {
            return Err(error(
                "original source cut changes or duplicates the complete data capsule",
            ));
        }
        Self::validate_joint_acknowledgements(proof)
    }

    fn validate_original_retirement(
        scope: &OriginalScope,
        operation_id: &str,
        operation_digest: &str,
        payload: &serde_json::Value,
        plan: &MetadataImportPlan,
        proof: &sinomo_dfs::DfsMetadataShardMembershipHandoffProof,
    ) -> Result<()> {
        Self::validate_plan(plan)?;
        if proof.retiring_shard.file_system_id != scope.file_system_id
            || proof.retiring_shard.shard_id != scope.shard_id.0
            || proof.retiring_shard.epoch < scope.owner_epoch
            || proof.desired_shard.epoch <= scope.owner_epoch
            || proof.retiring_shard != plan.expected_handoff.retiring_shard
            || proof.desired_shard != plan.expected_handoff.desired_shard
            || proof.cause != Self::import_cause(&plan.source_cut)?
            || proof.handoff_id != plan.expected_handoff.handoff_id
        {
            return Err(error(
                "target handoff does not retire the exact original source scope",
            ));
        }
        let mut found = false;
        for source_row in &plan.source_cut.pending_operations {
            let Some((original, id, digest, body)) = original_operation(source_row)? else {
                continue;
            };
            if id == operation_id {
                if original.file_system_id != scope.file_system_id
                    || original.shard_id != scope.shard_id
                    || original.owner_epoch != scope.owner_epoch
                    || digest != operation_digest
                    || body != *payload
                    || found
                {
                    return Err(error(
                        "original retirement changes or repeats the immutable operation",
                    ));
                }
                found = true;
            }
        }
        if !found {
            return Err(error(
                "original source handoff does not contain this captured pending operation",
            ));
        }
        Self::validate_joint_acknowledgements(proof)
    }

    fn recover_original_retirement(
        &self,
        scope: &OriginalScope,
    ) -> Result<
        Option<(
            MetadataImportPlan,
            DfsNativeTxnCommit,
            DfsNativeTxnCommit,
            sinomo_dfs::DfsMetadataShardMembershipHandoffProof,
        )>,
    > {
        self.recover_scope_retirement(&scope.file_system_id, &scope.shard_id)
    }

    pub(super) fn recover_scope_retirement(
        &self,
        file_system_id: &sinomo_dfs::DfsFileSystemId,
        shard_id: &sinomo_dfs::DfsMetadataShardId,
    ) -> Result<
        Option<(
            MetadataImportPlan,
            DfsNativeTxnCommit,
            DfsNativeTxnCommit,
            sinomo_dfs::DfsMetadataShardMembershipHandoffProof,
        )>,
    > {
        self.evaluate_scope_retirement(file_system_id, shard_id)
            .map_err(|failure| self.native_observation_error(failure))
    }

    pub(super) fn evaluate_scope_retirement(
        &self,
        file_system_id: &sinomo_dfs::DfsFileSystemId,
        shard_id: &sinomo_dfs::DfsMetadataShardId,
    ) -> std::result::Result<
        Option<(
            MetadataImportPlan,
            DfsNativeTxnCommit,
            DfsNativeTxnCommit,
            sinomo_dfs::DfsMetadataShardMembershipHandoffProof,
        )>,
        NativeEvaluationError,
    > {
        let key =
            sinomo_dfs::metadata_shard_membership_handoff_proof_key(file_system_id, &shard_id.0);
        let (entries, _) = self.read_exact(&[key])?;
        let latest: Option<sinomo_dfs::DfsMetadataShardMembershipHandoffProof> = entries[0]
            .entry
            .as_ref()
            .map(|entry| entry.decode().map_err(error))
            .transpose()?;
        let mut locations = vec![self.metadata_root.join(&shard_id.0)];
        if let Some(proof) = latest {
            locations.push(self.receiver_root_for(
                file_system_id,
                &shard_id.0,
                &proof.handoff_id,
            )?);
        }
        for location in locations {
            let plan_path = location.join("native-import-plan.json");
            if !plan_path.is_file() {
                continue;
            }
            let plan: MetadataImportPlan =
                serde_json::from_slice(&std::fs::read(&plan_path).map_err(error)?)
                    .map_err(error)?;
            Self::validate_plan(&plan)?;
            if plan.expected_handoff.retiring_shard.file_system_id != *file_system_id
                || plan.expected_handoff.retiring_shard.shard_id != shard_id.0
            {
                continue;
            }
            let activation = self.find_native_request(&location, &plan.expected_handoff, true)?;
            let actual = Self::activation_proof(&activation)?
                .ok_or_else(|| error("original activation request has no exact joint proof"))?;
            self.validate_activation_request(&activation, &plan, &actual)?;
            let stage_commit =
                self.evaluate_original_import_commit(&plan, plan.stage_request.clone())?;
            let activation_commit = self.evaluate_original_import_commit(&plan, activation)?;
            Self::require_committed(&stage_commit)?;
            Self::require_committed(&activation_commit)?;
            self.original_scope_still_retired(&actual)?;
            return Ok(Some((plan, stage_commit, activation_commit, actual)));
        }
        Ok(None)
    }
    pub(super) fn original_scope_still_retired(
        &self,
        proof: &sinomo_dfs::DfsMetadataShardMembershipHandoffProof,
    ) -> Result<()> {
        let key = sinomo_dfs::metadata_shard_key(
            &proof.desired_shard.file_system_id,
            &proof.desired_shard.shard_id,
        );
        let (entries, _) = self.read_exact(&[key])?;
        let current: DfsMetadataShardConfig = entries[0]
            .entry
            .as_ref()
            .ok_or_else(|| error("original retired scope has no current native assignment"))?
            .decode()
            .map_err(error)?;
        if current.file_system_id != proof.desired_shard.file_system_id
            || current.shard_id != proof.desired_shard.shard_id
            || current.epoch < proof.desired_shard.epoch
        {
            return Err(error(
                "original source retirement is not preserved by current native scope",
            ));
        }
        // Historical exact receipts only retire archived old operations;
        // this evidence cannot authorize a fresh publication or target image.
        Ok(())
    }
}
