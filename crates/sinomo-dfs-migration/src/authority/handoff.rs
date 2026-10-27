use super::*;
use sinomo_dfs::{
    DfsMetadataShardHandoffCause, DfsMetadataShardMembershipHandoff,
    DfsMetadataShardMembershipHandoffFence, DfsMetadataShardMembershipHandoffProof,
    DfsMetadataStorageImportCut,
};

impl NativeImportAuthority {
    pub(super) fn local_node(&self) -> Result<sinomo_core::node::NodeId> {
        Ok(sinomo_core::cluster::read_cluster_state_without_kv(
            &sinomo_core::network::ClusterPaths::new(&sinomo_core::local::SinomoDataDir::new(
                &self.data_root,
            )),
        )
        .map_err(error)?
        .local_node_id)
    }
    pub(super) fn import_cause(source: &MetadataSourceCut) -> Result<DfsMetadataShardHandoffCause> {
        Ok(DfsMetadataShardHandoffCause::StorageImport {
            cut: DfsMetadataStorageImportCut {
                source_cut_digest: digest(&(
                    source.source_identity.clone(),
                    source.source_snapshot_fingerprint.clone(),
                    source.source_exact_prefix.clone(),
                    source.scope_shard.clone(),
                    source.projection_assignments.clone(),
                    source.converted_image_fingerprint.clone(),
                    source.original_handoff.clone(),
                ))?,
                source_journal_sequence: source.source_journal_sequence,
                source_state_fingerprint: source.source_state_fingerprint.clone(),
                target_prefix: source.target_exact_prefix.clone(),
                converted_image_fingerprint: source.current_image_fingerprint.clone(),
                pending_mapping_digest: digest(&(
                    &source.pending_operations,
                    &source.original_handoff,
                ))?,
            },
        })
    }
    pub(super) fn stage_handoff(
        &self,
        source: &MetadataSourceCut,
        captured: Vec<MetadataCapturedKvEntry>,
        authority: KvAuthorityProof,
    ) -> Result<MetadataImportPlan> {
        let targets = source
            .projection_assignments
            .iter()
            .filter(|cfg| cfg.shard_id == source.scope_shard.0)
            .collect::<Vec<_>>();
        let [retiring] = targets.as_slice() else {
            return Err(error(
                "current physical metadata root requires one unique filesystem/shard assignment; all source data remains retained",
            ));
        };
        let floor_key = sinomo_dfs::metadata_shard_operation_floor_key(
            &retiring.file_system_id,
            &retiring.shard_id,
        );
        if let Some(entry) = captured
            .iter()
            .find(|item| item.key == floor_key)
            .and_then(|item| item.entry.as_ref())
        {
            let floor: serde_json::Value = entry.decode().map_err(error)?;
            let sequence = floor
                .get("prepared_sequence")
                .and_then(serde_json::Value::as_u64)
                .ok_or_else(|| error("native source floor has no exact sequence"))?;
            if source.source_journal_sequence < sequence {
                return Err(error(
                    "actual source cut is behind its committed native floor; mapped target frames cannot repair source freshness",
                ));
            }
        }
        let watermark_key = sinomo_dfs::metadata_shard_head_watermark_key(
            &retiring.file_system_id,
            &retiring.shard_id,
        );
        if let Some(entry) = captured
            .iter()
            .find(|item| item.key == watermark_key)
            .and_then(|item| item.entry.as_ref())
        {
            let watermark: sinomo_dfs::DfsMetadataShardHeadWatermark =
                entry.decode().map_err(error)?;
            if source.source_journal_sequence < watermark.journal_sequence
                || (source.source_journal_sequence == watermark.journal_sequence
                    && source.source_state_fingerprint != watermark.state_fingerprint)
            {
                return Err(error(
                    "actual fenced source differs from its committed native head watermark",
                ));
            }
        }
        if retiring.primary_node_id != self.local_node()? {
            return Err(error(
                "only the actual retiring primary may stage its retained source cut; other nodes receive its authenticated current image",
            ));
        }
        let mut desired = (*retiring).clone();
        desired.epoch = retiring
            .epoch
            .checked_add(1)
            .ok_or_else(|| error("import epoch exhausted"))?;
        let cause = Self::import_cause(source)?;
        let mut client = self.control_client(&source.staged_root)?;
        if let Some(original) = &source.original_handoff {
            if original.retiring_shard != **retiring && original.desired_shard != **retiring {
                return Err(error(
                    "original pending handoff differs from the captured retiring scope",
                ));
            }
            let value =
                crate::metadata::source_document::decode_original_document(&original.entry.value)?;
            let required = value
                .get("required_prepared_sequence")
                .and_then(serde_json::Value::as_u64)
                .ok_or_else(|| error("original pending handoff has no recorded source floor"))?;
            if source.source_journal_sequence < required {
                return Err(error(
                    "actual source cut is behind the original pending handoff floor",
                ));
            }
            client.original_handoff = Some(original.entry.clone());
        }
        let pending = DfsControlPlaneStore::new(&mut client)
            .begin_metadata_shard_handoff_with_cause(retiring, &desired, cause)
            .map_err(error)?
            .ok_or_else(|| error("native assignment changed before staging the import cut"))?;
        let stage_commit = client.last_commit.clone();
        let stage_request = if let Some(commit) = stage_commit {
            commit.request
        } else {
            self.find_native_request(&source.staged_root, &pending, false)?
        };
        let mut desired_assignments = source.projection_assignments.clone();
        for cfg in &mut desired_assignments {
            if cfg.file_system_id == desired.file_system_id && cfg.shard_id == desired.shard_id {
                *cfg = desired.clone();
            }
        }
        Ok(MetadataImportPlan {
            facts: MetadataImportPlanFacts {
                expected_assignments: source.projection_assignments.clone(),
                desired_assignments,
                target_exact_prefix: source.target_exact_prefix.clone(),
                current_image_fingerprint: source.current_image_fingerprint.clone(),
                pending_operation_mapping: source.pending_operations.clone(),
                combined_layout_source_fences: Vec::new(),
                source_cut: source.clone(),
            },
            captured_entries: captured,
            captured_authority: authority,
            stage_request,
            expected_handoff: pending,
            activation_request: None,
        })
    }
    pub(super) fn stage_record(
        request: &KvTxnRequest,
    ) -> Result<Option<DfsMetadataShardMembershipHandoff>> {
        let mut found = None;
        for operation in &request.txn.success {
            if let KvOp::Put { key, value, .. } = operation {
                if let Ok(record) =
                    sinomo_core::kv::decode_kv_value::<DfsMetadataShardMembershipHandoff>(value)
                {
                    if *key
                        == sinomo_dfs::metadata_shard_membership_handoff_key(
                            &record.retiring_shard.file_system_id,
                            &record.retiring_shard.shard_id,
                        )
                        && matches!(
                            record.cause,
                            DfsMetadataShardHandoffCause::StorageImport { .. }
                        )
                        && record.retiring_primary_fence.is_none()
                    {
                        if found.replace(record).is_some() {
                            return Err(error(
                                "one native request contains competing import stages",
                            ));
                        }
                    }
                }
            }
        }
        Ok(found)
    }
    pub(super) fn activation_proof(
        request: &KvTxnRequest,
    ) -> Result<Option<DfsMetadataShardMembershipHandoffProof>> {
        let mut found = None;
        for operation in &request.txn.success {
            if let KvOp::Put { key, value, .. } = operation {
                if let Ok(proof) = sinomo_core::kv::decode_kv_value::<
                    DfsMetadataShardMembershipHandoffProof,
                >(value)
                {
                    if *key
                        == sinomo_dfs::metadata_shard_membership_handoff_proof_key(
                            &proof.retiring_shard.file_system_id,
                            &proof.retiring_shard.shard_id,
                        )
                        && matches!(
                            proof.cause,
                            DfsMetadataShardHandoffCause::StorageImport { .. }
                        )
                    {
                        if found.replace(proof).is_some() {
                            return Err(error(
                                "one native request contains competing import activations",
                            ));
                        }
                    }
                }
            }
        }
        Ok(found)
    }
    pub(super) fn validate_plan(plan: &MetadataImportPlan) -> Result<()> {
        let source = &plan.source_cut;
        let h = &plan.expected_handoff;
        let staged = Self::stage_record(&plan.stage_request)?
            .ok_or_else(|| error("import plan has no original stage effect"))?;
        let mut desired = h.retiring_shard.clone();
        desired.epoch = desired
            .epoch
            .checked_add(1)
            .ok_or_else(|| error("import epoch exhausted"))?;
        let stage_key = sinomo_dfs::metadata_shard_membership_handoff_key(
            &h.retiring_shard.file_system_id,
            &h.retiring_shard.shard_id,
        );
        let expected_stage = vec![
            KvOp::put_serde(
                stage_key.clone(),
                h,
                source
                    .original_handoff
                    .as_ref()
                    .and_then(|original| original.entry.lease_id),
            )
            .map_err(error)?,
        ];
        let has_stage_guard =
            if let Some(original) = &source.original_handoff {
                original.entry.key == stage_key
                && (original.retiring_shard == h.retiring_shard
                    || original.desired_shard == h.retiring_shard)
                && [
                    KvCompare::ValueEquals {
                        key: stage_key.clone(),
                        value: original.entry.value.clone(),
                    },
                    KvCompare::ModifyRevisionEquals {
                        key: stage_key.clone(),
                        revision: original.entry.modify_revision,
                    },
                    KvCompare::LeaseEquals {
                        key: stage_key.clone(),
                        lease_id: original.entry.lease_id,
                    },
                ]
                .iter()
                .all(|required| plan.stage_request.txn.compares.contains(required))
                && !plan.stage_request.txn.compares.iter().any(
                    |compare| matches!(compare, KvCompare::Missing { key } if *key == stage_key),
                )
            } else {
                plan.stage_request.txn.compares.iter().any(
                    |compare| matches!(compare, KvCompare::Missing { key } if *key == stage_key),
                )
            };
        if plan.stage_request.txn.success != expected_stage
            || !plan.stage_request.txn.failure.is_empty()
            || !has_stage_guard
            || h != &staged
            || h.cause != Self::import_cause(source)?
            || h.desired_shard != desired
            || plan.expected_assignments != source.projection_assignments
            || plan.target_exact_prefix != source.target_exact_prefix
            || plan.current_image_fingerprint != source.current_image_fingerprint
            || plan.pending_operation_mapping != source.pending_operations
            || h.retiring_shard.shard_id != source.scope_shard.0
            || !source
                .file_system_ids
                .contains(&h.retiring_shard.file_system_id)
        {
            return Err(error(
                "native import blueprint differs from its complete source/candidate scope",
            ));
        }
        let mut expected = plan.expected_assignments.clone();
        let target = expected
            .iter_mut()
            .find(|cfg| **cfg == h.retiring_shard)
            .ok_or_else(|| error("retiring assignment is absent from the source cut"))?;
        *target = h.desired_shard.clone();
        if plan.desired_assignments != expected {
            return Err(error(
                "import blueprint changes assignments outside its exact physical scope",
            ));
        }
        Ok(())
    }
    pub(super) fn find_native_request(
        &self,
        root: &Path,
        handoff: &DfsMetadataShardMembershipHandoff,
        activation: bool,
    ) -> Result<KvTxnRequest> {
        if activation {
            let path = root.join("native-activation-request.json");
            if path.exists() {
                let request: KvTxnRequest =
                    serde_json::from_slice(&std::fs::read(path).map_err(error)?).map_err(error)?;
                if Self::activation_proof(&request)?.is_some_and(|proof| {
                    proof.handoff_id == handoff.handoff_id
                        && proof.cause == handoff.cause
                        && proof.retiring_shard == handoff.retiring_shard
                        && proof.desired_shard == handoff.desired_shard
                }) {
                    return Ok(request);
                }
                return Err(error(
                    "immutable activation request belongs to another native handoff",
                ));
            }
        }
        let mut found = None;
        for entry in std::fs::read_dir(root).map_err(error)? {
            let entry = entry.map_err(error)?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if !name.starts_with("native-request-") || !name.ends_with(".json") {
                continue;
            }
            let request: KvTxnRequest =
                serde_json::from_slice(&std::fs::read(entry.path()).map_err(error)?)
                    .map_err(error)?;
            let matches = if activation {
                Self::activation_proof(&request)?.is_some_and(|proof| {
                    proof.handoff_id == handoff.handoff_id
                        && proof.cause == handoff.cause
                        && proof.retiring_shard == handoff.retiring_shard
                        && proof.desired_shard == handoff.desired_shard
                })
            } else {
                Self::stage_record(&request)?.as_ref() == Some(handoff)
            };
            if matches {
                if found.replace(request).is_some() {
                    return Err(error("competing native import request blueprints"));
                }
            }
        }
        found.ok_or_else(|| error("exact original native import request blueprint is unavailable"))
    }
    /// A saved-only primary retries the ordinary guarded first CAS. A stage
    /// already present/activated is observed, never resubmitted as a new effect.
    fn recover_stage_commit(&self, plan: &MetadataImportPlan) -> Result<DfsNativeTxnCommit> {
        self.retained_plan_file.validate(plan)?;
        let h = &plan.expected_handoff;
        let keys = vec![
            sinomo_dfs::metadata_shard_key(
                &h.retiring_shard.file_system_id,
                &h.retiring_shard.shard_id,
            ),
            sinomo_dfs::metadata_shard_membership_handoff_key(
                &h.retiring_shard.file_system_id,
                &h.retiring_shard.shard_id,
            ),
            sinomo_dfs::metadata_shard_membership_handoff_proof_key(
                &h.retiring_shard.file_system_id,
                &h.retiring_shard.shard_id,
            ),
        ];
        let (state, _) = self.read_exact(&keys)?;
        let assignment: DfsMetadataShardConfig = state[0]
            .entry
            .as_ref()
            .ok_or_else(|| error("saved import assignment vanished"))?
            .decode()
            .map_err(error)?;
        let prior_proof = state[2]
            .entry
            .as_ref()
            .map(|entry| entry.decode::<DfsMetadataShardMembershipHandoffProof>())
            .transpose()
            .map_err(error)?;
        let saved_only = assignment == h.retiring_shard
            && state[1].entry.is_none()
            && prior_proof
                .as_ref()
                .is_none_or(|proof| proof.handoff_id != h.handoff_id);
        if saved_only && self.local_node()? != h.retiring_shard.primary_node_id {
            return Err(error(
                "saved-only stage requires its actual retiring primary",
            ));
        }
        let before = if saved_only {
            None
        } else {
            Some(self.check_handoff_state(plan)?)
        };
        let mut source_fence = None;
        if self.local_node()? == plan.expected_handoff.retiring_shard.primary_node_id {
            let source = &plan.source_cut;
            self.validate_source_root(&source.source_root)?;
            self.validate_source_root(&source.staged_root)?;
            source_fence = Some(crate::metadata::retain_source_fence(&source.source_root)?);
            crate::metadata::validate_retained_source_identity(
                &source.source_root,
                &source.source_identity,
                &source.source_snapshot_fingerprint,
            )?;
            let input_path = source.staged_root.join("native-import-input.json");
            let (input_source, input_entries, input_authority): (
                MetadataSourceCut,
                Vec<MetadataCapturedKvEntry>,
                KvAuthorityProof,
            ) = serde_json::from_slice(&std::fs::read(input_path).map_err(error)?)
                .map_err(error)?;
            if !plan_file::same_source_cut(&input_source, source)
                || serde_json::to_value(&input_entries).map_err(error)?
                    != serde_json::to_value(&plan.captured_entries).map_err(error)?
                || input_authority != plan.captured_authority
            {
                return Err(error(
                    "original stage replay changed its captured source/input authority",
                ));
            }
            let original_path = source.staged_root.join(format!(
                "native-request-{}.json",
                plan.stage_request.request_id,
            ));
            let original: KvTxnRequest =
                serde_json::from_slice(&std::fs::read(original_path).map_err(error)?)
                    .map_err(error)?;
            if original != plan.stage_request
                || KvRequestId::derived(
                    "dfs-storage-import-control-v7",
                    digest(&original.txn)?.as_bytes(),
                ) != original.request_id
            {
                return Err(error(
                    "original stage replay changed its immutable request/producer identity",
                ));
            }
        }
        if saved_only {
            // Saving the plan/request precedes the ordinary first atomic CAS.
            // No receipt is inferred from files or absent domain keys.
            let outcome = block_native(
                &self.runtime,
                self.cluster()?
                    .kv_request_outcome_authoritative_async(plan.stage_request.request_id.clone()),
            )?;
            sinomo_core::kv::validate_cluster_kv_request_outcome_read(&outcome).map_err(error)?;
            current_authority(&outcome.authority)?;
            if !matches!(
                outcome.outcome,
                sinomo_core::kv::KvRequestOutcome::Unknown { .. }
            ) {
                return Err(error(
                    "saved-only stage has a prior or unavailable native outcome",
                ));
            }
            let mut permission_keys = BTreeSet::new();
            for node in std::iter::once(&h.retiring_shard.primary_node_id)
                .chain(&h.retiring_shard.replica_node_ids)
            {
                permission_keys.insert(sinomo_core::node::cluster_member_key(node));
                permission_keys.insert(sinomo_core::node::cluster_capability_policy_key(node));
            }
            let (permissions, _) =
                self.read_exact(&permission_keys.into_iter().collect::<Vec<_>>())?;
            self.require_current_assignment_members(
                std::slice::from_ref(&h.retiring_shard),
                &permissions,
            )?;
            let (fresh, _) = self.read_exact(&keys)?;
            if serde_json::to_value(&fresh).map_err(error)?
                != serde_json::to_value(&state).map_err(error)?
            {
                return Err(error("saved-only stage moved before its first atomic CAS"));
            }
            // The exact existing request/CAS and normal host authorization are
            // retained. A concurrent stage cannot bypass its Missing compare.
            let stage = self.commit(plan.stage_request.clone())?;
            Self::require_committed(&stage)?;
            self.check_handoff_state(plan)?;
            return Ok(stage);
        }
        let stage = self.observe_original_import_commit(plan, plan.stage_request.clone())?;
        Self::require_committed(&stage)?;
        if Some(self.check_handoff_state(plan)?) != before {
            return Err(error(
                "native handoff moved while its original stage receipt was observed",
            ));
        }
        drop(source_fence);
        Ok(stage)
    }
    /// Observe the exact existing physical transaction under this domain plan.
    /// Captured authorization bytes do not identify a historical caller.
    pub(super) fn observe_original_import_commit(
        &self,
        plan: &MetadataImportPlan,
        request: KvTxnRequest,
    ) -> Result<DfsNativeTxnCommit> {
        self.evaluate_original_import_commit(plan, request)
            .map_err(NativeEvaluationError::into_metadata)
    }
    pub(super) fn evaluate_original_import_commit(
        &self,
        plan: &MetadataImportPlan,
        request: KvTxnRequest,
    ) -> std::result::Result<DfsNativeTxnCommit, NativeEvaluationError> {
        self.retained_plan_file.validate(plan)?;
        self.evaluate_original_native_commit(
            request,
            &plan.expected_handoff.retiring_shard.primary_node_id,
            &plan.captured_entries,
        )
    }

    /// Domain callers bind immutable custody and current scope separately.
    /// This observes a native receipt and never delegates a first effect.
    pub(super) fn observe_original_native_commit(
        &self,
        request: KvTxnRequest,
        owner: &sinomo_core::node::NodeId,
        captured_issuer_rows: &[MetadataCapturedKvEntry],
    ) -> Result<DfsNativeTxnCommit> {
        self.evaluate_original_native_commit(request, owner, captured_issuer_rows)
            .map_err(NativeEvaluationError::into_metadata)
    }
    pub(super) fn evaluate_original_native_commit(
        &self,
        request: KvTxnRequest,
        owner: &sinomo_core::node::NodeId,
        captured_issuer_rows: &[MetadataCapturedKvEntry],
    ) -> std::result::Result<DfsNativeTxnCommit, NativeEvaluationError> {
        // Preserve only raw read availability until the ordinary wrapper ends.
        // Both wrappers execute this same observation; neither submits a command.
        let read = block_native(&self.runtime, async {
            Ok::<_, MetadataBackendError>(
                self.cluster()?
                    .kv_request_outcome_authoritative_async(request.request_id.clone())
                    .await,
            )
        })?
        .map_err(NativeEvaluationError::Native)?;
        sinomo_core::kv::validate_cluster_kv_request_outcome_read(&read).map_err(error)?;
        current_authority(&read.authority)?;
        let sinomo_core::kv::KvRequestOutcome::Committed(record) = read.outcome else {
            return Err(
                error("original import request has no actual committed native receipt").into(),
            );
        };
        let submitted = sinomo_core::kv::kv_command_digest(&KvCommand::Txn(request.txn.clone()))
            .map_err(error)?;
        if record.request_id != request.request_id
            || record.term == 0
            || record.log_index == 0
            || record
                .txn_response
                .as_ref()
                .is_none_or(|response| !response.succeeded || response.revision != record.revision)
        {
            return Err(error(
                "original import native receipt does not bind its successful request",
            )
            .into());
        }
        Self::validate_original_command_preimage(
            &request,
            owner,
            captured_issuer_rows,
            &record.command_digest,
        )?;
        let result = sinomo_core::kv::ClusterKvTxnCompactCommitResult {
            request_id: record.request_id,
            submitted_command_digest: Some(submitted),
            command_digest: record.command_digest,
            txn_response: record.txn_response,
            authority: KvAuthorityProof {
                term: record.term,
                commit_index: record.log_index,
                revision: record.revision,
                basis: KvAuthorityBasis::ExactCommittedRequest,
            },
            replayed_committed_request: true,
        };
        let commit = DfsNativeTxnCommit { request, result };
        Self::require_committed(&commit)?;
        Ok(commit)
    }

    /// Match only the unchanged local body or released Remote ingress's exact
    /// four authorization compares. Captured rows are original byte DATA;
    /// current domain authority is checked separately by every caller.
    pub(super) fn validate_original_command_preimage(
        request: &KvTxnRequest,
        permission_subject: &sinomo_core::node::NodeId,
        captured_rows: &[MetadataCapturedKvEntry],
        physical_digest: &sinomo_core::kv::KvCommandDigest,
    ) -> Result<()> {
        let bare = sinomo_core::kv::kv_command_digest(&KvCommand::Txn(request.txn.clone()))
            .map_err(error)?;
        if &bare == physical_digest {
            return Ok(());
        }
        let captured = |key: &str| -> Result<sinomo_core::kv::KvEntry> {
            let mut rows = captured_rows.iter().filter(|row| row.key == key);
            let row = rows
                .next()
                .ok_or_else(|| error("original receipt lacks captured permission preimage"))?;
            let entry = row
                .entry
                .clone()
                .ok_or_else(|| error("original permission preimage was absent"))?;
            if rows.next().is_some() || entry.key != key {
                return Err(error(
                    "original permission preimage changes or repeats its key",
                ));
            }
            Ok(entry)
        };
        let member = captured(&sinomo_core::node::cluster_member_key(permission_subject))?;
        let policy = captured(&sinomo_core::node::cluster_capability_policy_key(
            permission_subject,
        ))?;
        let original: sinomo_core::cluster::ClusterMemberRecord = member.decode().map_err(error)?;
        if &original.node_id != permission_subject {
            return Err(error("original permission preimage has a foreign subject"));
        }
        let signer = sinomo_core::service::ServiceSigner::Remote {
            node_id: permission_subject.to_string(),
            public_key: original.node_public_key,
        };
        let physical = sinomo_core::cluster::fence_remote_cluster_kv_txn_request_with_entries(
            Some(&member),
            Some(&policy),
            &signer,
            request.clone(),
        )
        .map_err(error)?;
        if sinomo_core::kv::kv_command_digest(&KvCommand::Txn(physical.txn)).map_err(error)?
            != *physical_digest
        {
            return Err(error(
                "original receipt has no exact physical command preimage",
            ));
        }
        Ok(())
    }

    pub(super) fn recover_plan(
        &self,
        source: &MetadataSourceCut,
    ) -> Result<Option<MetadataImportPlan>> {
        let path = source.staged_root.join("native-import-plan.json");
        let plan = if path.exists() {
            self.retained_plan_file.read(&path)?.as_ref().clone()
        } else {
            let input = source.staged_root.join("native-import-input.json");
            if !input.exists() {
                return Ok(None);
            }
            let (captured_source, captured, authority): (
                MetadataSourceCut,
                Vec<MetadataCapturedKvEntry>,
                KvAuthorityProof,
            ) = serde_json::from_slice(&std::fs::read(input).map_err(error)?).map_err(error)?;
            if !plan_file::same_source_cut(&captured_source, source) {
                return Err(error("pre-stage source cut changed after restart"));
            }
            let mut recovered = None;
            for entry in std::fs::read_dir(&source.staged_root).map_err(error)? {
                let entry = entry.map_err(error)?;
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if !name.starts_with("native-request-") || !name.ends_with(".json") {
                    continue;
                }
                let request: KvTxnRequest =
                    serde_json::from_slice(&std::fs::read(entry.path()).map_err(error)?)
                        .map_err(error)?;
                if let Some(h) = Self::stage_record(&request)? {
                    if h.cause != Self::import_cause(source)? {
                        continue;
                    }
                    if recovered.is_some() {
                        return Err(error(
                            "multiple original import stage requests survived restart",
                        ));
                    }
                    let mut desired_assignments = source.projection_assignments.clone();
                    for cfg in &mut desired_assignments {
                        if *cfg == h.retiring_shard {
                            *cfg = h.desired_shard.clone();
                        }
                    }
                    recovered = Some(MetadataImportPlan {
                        facts: MetadataImportPlanFacts {
                            expected_assignments: source.projection_assignments.clone(),
                            desired_assignments,
                            target_exact_prefix: source.target_exact_prefix.clone(),
                            current_image_fingerprint: source.current_image_fingerprint.clone(),
                            pending_operation_mapping: source.pending_operations.clone(),
                            combined_layout_source_fences: Vec::new(),
                            source_cut: source.clone(),
                        },
                        captured_entries: captured.clone(),
                        captured_authority: authority,
                        stage_request: request,
                        expected_handoff: h,
                        activation_request: None,
                    });
                }
            }
            let Some(plan) = recovered else {
                return Ok(None);
            };
            Self::save_blueprint(&source.staged_root, "native-import-plan.json", &plan)?;
            plan
        };
        self.retained_plan_file.validate(&plan)?;
        if !plan_file::same_source_cut(&plan.source_cut, source) {
            return Err(error(
                "immutable native import blueprint changed after restart",
            ));
        }
        let stage = self.recover_stage_commit(&plan)?;
        Self::require_committed(&stage)?;
        self.check_handoff_state(&plan)?;
        Ok(Some(plan))
    }
    pub(super) fn check_handoff_state(
        &self,
        plan: &MetadataImportPlan,
    ) -> Result<Option<DfsMetadataShardMembershipHandoffProof>> {
        self.retained_plan_file.validate(plan)?;
        let h = &plan.expected_handoff;
        let keys = vec![
            sinomo_dfs::metadata_shard_key(
                &h.retiring_shard.file_system_id,
                &h.retiring_shard.shard_id,
            ),
            sinomo_dfs::metadata_shard_membership_handoff_key(
                &h.retiring_shard.file_system_id,
                &h.retiring_shard.shard_id,
            ),
            sinomo_dfs::metadata_shard_membership_handoff_proof_key(
                &h.retiring_shard.file_system_id,
                &h.retiring_shard.shard_id,
            ),
        ];
        let (entries, _) = self.read_exact(&keys)?;
        let current: DfsMetadataShardConfig = entries[0]
            .entry
            .as_ref()
            .ok_or_else(|| error("import assignment vanished"))?
            .decode()
            .map_err(error)?;
        if current == h.retiring_shard {
            let pending: DfsMetadataShardMembershipHandoff = entries[1]
                .entry
                .as_ref()
                .ok_or_else(|| error("staged native handoff vanished before activation"))?
                .decode()
                .map_err(error)?;
            if pending.handoff_id != h.handoff_id
                || pending.cause != h.cause
                || pending.retiring_shard != h.retiring_shard
                || pending.desired_shard != h.desired_shard
            {
                return Err(error("native import was superseded by another handoff"));
            }
            return Ok(None);
        }
        if current != h.desired_shard {
            return Err(error(
                "native assignment moved beyond the recorded import; normal authoritative catch-up is required",
            ));
        }
        let proof: DfsMetadataShardMembershipHandoffProof = entries[2]
            .entry
            .as_ref()
            .ok_or_else(|| error("activated import has no exact native joint proof"))?
            .decode()
            .map_err(error)?;
        if proof.handoff_id != h.handoff_id
            || proof.cause != h.cause
            || proof.retiring_shard != h.retiring_shard
            || proof.desired_shard != h.desired_shard
            || proof.journal_sequence != plan.target_exact_prefix.journal_sequence
            || proof.state_fingerprint != plan.current_image_fingerprint
            || entries[1].entry.is_some()
        {
            return Err(error(
                "prior native membership proof cannot authorize this candidate",
            ));
        }
        Self::validate_joint_acknowledgements(&proof)?;
        Ok(Some(proof))
    }
    pub(super) fn validate_joint_acknowledgements(
        proof: &DfsMetadataShardMembershipHandoffProof,
    ) -> Result<()> {
        let acknowledgements = proof.acknowledged_node_ids.iter().collect::<BTreeSet<_>>();
        let configured = std::iter::once(&proof.retiring_shard.primary_node_id)
            .chain(&proof.retiring_shard.replica_node_ids)
            .chain(std::iter::once(&proof.desired_shard.primary_node_id))
            .chain(&proof.desired_shard.replica_node_ids)
            .collect::<BTreeSet<_>>();
        if acknowledgements.len() != proof.acknowledged_node_ids.len()
            || !acknowledgements.is_subset(&configured)
        {
            return Err(error(
                "joint import proof contains duplicate or foreign acknowledgements",
            ));
        }
        for cfg in [&proof.retiring_shard, &proof.desired_shard] {
            let nodes = std::iter::once(&cfg.primary_node_id)
                .chain(&cfg.replica_node_ids)
                .collect::<BTreeSet<_>>();
            if !acknowledgements.contains(&cfg.primary_node_id)
                || nodes.intersection(&acknowledgements).count() < nodes.len() / 2 + 1
            {
                return Err(error(
                    "actual import proof lacks a primary or assignment majority",
                ));
            }
        }
        Ok(())
    }
    pub(super) fn validate_activation_request(
        &self,
        request: &KvTxnRequest,
        plan: &MetadataImportPlan,
        proof: &DfsMetadataShardMembershipHandoffProof,
    ) -> Result<()> {
        self.retained_plan_file.validate(plan)?;
        Self::validate_joint_acknowledgements(proof)?;
        let h = &plan.expected_handoff;
        if proof.handoff_id != h.handoff_id
            || proof.cause != h.cause
            || proof.retiring_shard != h.retiring_shard
            || proof.desired_shard != h.desired_shard
            || proof.journal_sequence != plan.target_exact_prefix.journal_sequence
            || proof.state_fingerprint != plan.current_image_fingerprint
            || request.txn.success.len() != 4
            || !request.txn.failure.is_empty()
        {
            return Err(error(
                "activation request is not the exact bounded joint import transition",
            ));
        }
        let cfg_key = sinomo_dfs::metadata_shard_key(
            &h.retiring_shard.file_system_id,
            &h.retiring_shard.shard_id,
        );
        let pending_key = sinomo_dfs::metadata_shard_membership_handoff_key(
            &h.retiring_shard.file_system_id,
            &h.retiring_shard.shard_id,
        );
        let proof_key = sinomo_dfs::metadata_shard_membership_handoff_proof_key(
            &h.retiring_shard.file_system_id,
            &h.retiring_shard.shard_id,
        );
        let watermark_key = sinomo_dfs::metadata_shard_head_watermark_key(
            &h.retiring_shard.file_system_id,
            &h.retiring_shard.shard_id,
        );
        let desired_nodes = std::iter::once(&h.desired_shard.primary_node_id)
            .chain(&h.desired_shard.replica_node_ids)
            .collect::<BTreeSet<_>>();
        let watermark = sinomo_dfs::DfsMetadataShardHeadWatermark {
            file_system_id: h.desired_shard.file_system_id.clone(),
            shard_id: h.desired_shard.shard_id.clone(),
            primary_node_id: h.desired_shard.primary_node_id.clone(),
            epoch: h.desired_shard.epoch,
            journal_sequence: proof.journal_sequence,
            state_fingerprint: proof.state_fingerprint.clone(),
            acknowledged_node_ids: proof
                .acknowledged_node_ids
                .iter()
                .filter(|node| desired_nodes.contains(node))
                .cloned()
                .collect(),
        };
        let expected = vec![
            KvOp::put_serde(cfg_key.clone(), &h.desired_shard, None).map_err(error)?,
            KvOp::put_serde(watermark_key, &watermark, None).map_err(error)?,
            KvOp::put_serde(proof_key, proof, None).map_err(error)?,
            KvOp::Delete {
                key: pending_key.clone(),
            },
        ];
        if request.txn.success != expected || !request.txn.compares.iter().any(|compare| matches!(compare, KvCompare::ValueEquals { key, value } if *key == cfg_key && sinomo_core::kv::decode_kv_value::<DfsMetadataShardConfig>(value).is_ok_and(|cfg| cfg == h.retiring_shard))) {
            return Err(error("activation blueprint changes native effects or omits the retiring config CAS"));
        }
        let pending = request
            .txn
            .compares
            .iter()
            .find_map(|compare| match compare {
                KvCompare::ValueEquals { key, value } if *key == pending_key => {
                    sinomo_core::kv::decode_kv_value::<DfsMetadataShardMembershipHandoff>(value)
                        .ok()
                }
                _ => None,
            })
            .ok_or_else(|| error("activation request omits the exact fenced pending CAS"))?;
        if pending.handoff_id != h.handoff_id
            || pending.cause != h.cause
            || pending.retiring_shard != h.retiring_shard
            || pending.desired_shard != h.desired_shard
            || pending.required_prepared_sequence != h.required_prepared_sequence
            || pending.required_head_watermark != h.required_head_watermark
            || !pending
                .retiring_primary_fence
                .as_ref()
                .is_some_and(|fence| {
                    fence.handoff_id == h.handoff_id
                        && fence.primary_node_id == h.retiring_shard.primary_node_id
                        && fence.journal_sequence == plan.source_cut.source_journal_sequence
                        && fence.state_fingerprint == plan.source_cut.source_state_fingerprint
                })
        {
            return Err(error("activation request has no actual source-cut fence"));
        }
        Ok(())
    }
    pub(super) fn distribute_activation_blueprint(
        &self,
        root: &Path,
        request: &KvTxnRequest,
    ) -> Result<()> {
        let Some(proof) = Self::activation_proof(request)? else {
            return Ok(());
        };
        let plan = self
            .retained_plan_file
            .read(&root.join("native-import-plan.json"))?;
        self.validate_activation_request(request, &plan, &proof)?;
        Self::save_blueprint(root, "native-activation-request.json", request)?;
        let envelope = staged_service::ActivationBlueprint {
            plan: plan.as_ref().clone(),
            request: request.clone(),
        };
        for node in &proof.acknowledged_node_ids {
            let response = if *node == self.local_node()? {
                self.receive_activation_blueprint(self.local_node()?, envelope.clone())?
            } else {
                let transport = self.transport.as_ref().ok_or_else(|| {
                    error("activation blueprint requires the actual native object-RPC transport")
                })?;
                let call = sinomo_core::network::ObjectRpcCall::encode_borrowed_value(
                    staged_service::BLUEPRINT_ROUTE,
                    "dfs-migration",
                    &envelope,
                )
                .map_err(error)?;
                let result = block_native(
                    &self.runtime,
                    transport.call_async(node, call, source_effect_rpc_options(), true),
                )?;
                let response = block_native(
                    &self.runtime,
                    result.into_materialized_value_response(64 * 1024),
                )?;
                response
                    .decode_body::<staged_service::BlueprintStored>()
                    .map_err(error)?
            };
            if response.request_id != request.request_id || response.handoff_id != proof.handoff_id
            {
                return Err(error(
                    "peer did not durably retain the exact activation request identity",
                ));
            }
        }
        Ok(())
    }
    pub(super) fn activate_handoff(
        &self,
        plan: &MetadataImportPlan,
        resolved_image_root: &Path,
        provenance_plan_root: &Path,
    ) -> Result<(
        DfsNativeTxnCommit,
        DfsNativeTxnCommit,
        DfsMetadataShardMembershipHandoffProof,
    )> {
        if let Some(proof) = self.check_handoff_state(plan)? {
            let stage = self.observe_original_import_commit(plan, plan.stage_request.clone())?;
            Self::require_committed(&stage)?;
            let request = match &plan.activation_request {
                Some(request) => request.clone(),
                None => {
                    self.find_native_request(provenance_plan_root, &plan.expected_handoff, true)?
                }
            };
            let request_proof = Self::activation_proof(&request)?
                .ok_or_else(|| error("retained activation request omits its joint proof"))?;
            self.validate_activation_request(&request, plan, &request_proof)?;
            if request_proof != proof {
                return Err(error(
                    "retained activation request differs from native proof",
                ));
            }
            let activation = self.observe_original_import_commit(plan, request)?;
            Self::require_committed(&activation)?;
            if self.check_handoff_state(plan)? != Some(proof.clone()) {
                return Err(error(
                    "native activation moved while original receipts were observed",
                ));
            }
            return Ok((stage, activation, proof));
        }
        if self.local_node()? != plan.expected_handoff.retiring_shard.primary_node_id {
            return Err(error(
                "only the actual retiring primary can advance an unfinished native import",
            ));
        }
        let stage = self.recover_stage_commit(plan)?;
        Self::require_committed(&stage)?;
        if provenance_plan_root
            .join("native-activation-request.json")
            .exists()
        {
            let request =
                self.find_native_request(provenance_plan_root, &plan.expected_handoff, true)?;
            let proof = Self::activation_proof(&request)?
                .ok_or_else(|| error("saved activation request has no exact proof"))?;
            self.validate_activation_request(&request, plan, &proof)?;
            // The original request may not have crossed the native commit
            // boundary. Re-establish its exact blueprint custody before replay;
            // never choose a new ACK set or request ID for this import.
            let activation = self.blueprint_commit(provenance_plan_root, request)?;
            Self::require_committed(&activation)?;
            let actual = self.check_handoff_state(plan)?.ok_or_else(|| {
                error("original activation request has no retained native transition")
            })?;
            if actual != proof {
                return Err(error(
                    "original activation outcome differs from its retained proof",
                ));
            }
            return Ok((stage, activation, actual));
        }
        let h = &plan.expected_handoff;
        let mut client = self.control_client(provenance_plan_root)?;
        let fence = DfsMetadataShardMembershipHandoffFence {
            handoff_id: h.handoff_id.clone(),
            primary_node_id: self.local_node()?,
            journal_sequence: plan.source_cut.source_journal_sequence,
            state_fingerprint: plan.source_cut.source_state_fingerprint.clone(),
        };
        let pending = DfsControlPlaneStore::new(&mut client)
            .acknowledge_metadata_shard_membership_handoff_fence(h, &fence)
            .map_err(error)?
            .ok_or_else(|| error("native source fence changed before import confirmation"))?;
        let image = sinomo_dfs::read_metadata_candidate_image(resolved_image_root)?;
        if image.prefix_proof != plan.target_exact_prefix
            || image.semantic_state_fingerprint != plan.current_image_fingerprint
        {
            return Err(error(
                "hidden candidate changed after its immutable native plan",
            ));
        }
        let configured = std::iter::once(pending.retiring_shard.primary_node_id.clone())
            .chain(pending.retiring_shard.replica_node_ids.clone())
            .chain(std::iter::once(
                pending.desired_shard.primary_node_id.clone(),
            ))
            .chain(pending.desired_shard.replica_node_ids.clone())
            .collect::<BTreeSet<_>>();
        let local = self.local_node()?;
        let mut acknowledged = BTreeSet::new();
        let mut failures = Vec::new();
        for node in configured {
            let command = sinomo_dfs::DfsMetadataSnapshotInstallCommand {
                request_id: sinomo_core::Id::generate().to_string(),
                source_node_id: local.clone(),
                file_system_id: Some(h.retiring_shard.file_system_id.clone()),
                authority_epoch: Some(h.retiring_shard.epoch),
                primary_handoff_id: Some(h.handoff_id.clone()),
                shard_id: plan.source_cut.scope_shard.clone(),
                next_after_sequence: plan.target_exact_prefix.journal_sequence,
                snapshot: image.descriptor().clone(),
                image: Some(image.clone()),
                reason: "current storage import".into(),
            };
            let result = self.confirm_stage_node(&node, command, plan);
            #[cfg(test)]
            sinomo_dfs::test_support::record_metadata_operation_boundary(
                &self.data_root,
                "migration-stage-participant-result",
                || {
                    serde_json::json!({
                        "node":node,"local":local,"handoff_id":h.handoff_id,
                        "retiring":h.retiring_shard,"desired":h.desired_shard,
                        "source_prefix":plan.source_cut.source_exact_prefix,
                        "target_prefix":plan.target_exact_prefix,
                        "result":format!("{result:?}"),
                    })
                },
            );
            match result {
                Ok(()) => {
                    acknowledged.insert(node);
                }
                Err(error) => failures.push(format!("{node}: {error}")),
            }
        }
        let proof = DfsMetadataShardMembershipHandoffProof {
            cause: h.cause.clone(),
            handoff_id: h.handoff_id.clone(),
            retiring_shard: h.retiring_shard.clone(),
            desired_shard: h.desired_shard.clone(),
            journal_sequence: plan.target_exact_prefix.journal_sequence,
            state_fingerprint: plan.current_image_fingerprint.clone(),
            acknowledged_node_ids: acknowledged.into_iter().collect(),
        };
        Self::validate_joint_acknowledgements(&proof).map_err(|reason| {
            error(format!("{reason}; stage failures: {}", failures.join("; ")))
        })?;
        client.last_commit = None;
        if !DfsControlPlaneStore::new(&mut client)
            .complete_metadata_shard_membership_handoff(&h.retiring_shard, &h.desired_shard, &proof)
            .map_err(error)?
        {
            return Err(error("native joint import activation did not commit"));
        }
        let activation = client
            .last_commit
            .clone()
            .ok_or_else(|| error("joint activation returned no actual native commit receipt"))?;
        Self::require_committed(&activation)?;
        let actual = self
            .check_handoff_state(plan)?
            .ok_or_else(|| error("joint activation is not retained in actual native state"))?;
        Ok((stage, activation, actual))
    }
    fn stage_rpc_value<T: serde::de::DeserializeOwned>(
        &self,
        node: &sinomo_core::node::NodeId,
        route: &str,
        body: &impl Serialize,
    ) -> Result<T> {
        let transport = self
            .transport
            .as_ref()
            .ok_or_else(|| error("native component object-RPC transport is unavailable"))?;
        let call = sinomo_core::network::ObjectRpcCall::encode_borrowed_value(
            route,
            "dfs-migration",
            body,
        )
        .map_err(error)?;
        let result = block_native(
            &self.runtime,
            transport.call_async(node, call, source_effect_rpc_options(), true),
        )?;
        let response = block_native(
            &self.runtime,
            result.into_materialized_value_response(64 * 1024),
        )?;
        response.decode_body().map_err(error)
    }
    pub(super) fn confirm_stage_node(
        &self,
        node: &sinomo_core::node::NodeId,
        command: sinomo_dfs::DfsMetadataSnapshotInstallCommand,
        plan: &MetadataImportPlan,
    ) -> Result<()> {
        let fs = command.file_system_id.clone();
        let epoch = command.authority_epoch;
        let shard = command.shard_id.clone();
        let sequence = command.next_after_sequence;
        let id = command.primary_handoff_id.clone();
        let fingerprint = command.snapshot.semantic_state_fingerprint.clone();
        let request_id = command.request_id.clone();
        // Retries of one immutable plan must resume the same peer's retained
        // ancestry exchange. This identity correlates DATA; authenticated source,
        // native handoff, exact floors and chunks still supply every admission.
        let ancestry_request_id = digest(&(plan, node))?;
        let challenge = staged_service::SourceFloorChallenge {
            request_id: ancestry_request_id.clone(),
            plan: plan.clone(),
        };
        let floors = if *node == self.local_node()? {
            self.receive_source_floor_challenge(self.local_node()?, challenge)?
        } else {
            self.stage_rpc_value(node, staged_service::SOURCE_FLOORS_ROUTE, &challenge)?
        };
        if floors.request_id != ancestry_request_id
            || floors.handoff_id != plan.expected_handoff.handoff_id
        {
            return Err(error(
                "source floor reply differs from the actual nonce/handoff",
            ));
        }
        let retained = floors
            .floors
            .into_iter()
            .map(Into::into)
            .collect::<Vec<crate::metadata::ack_floor::LegacyAcknowledgement>>();
        let mut reader = crate::metadata::source_ancestry::source_ancestry_reader(
            &plan.source_cut,
            &plan.source_cut.source_root,
            &retained,
            &floors.checkpoints,
        )?;
        let mut ordinal = 0u64;
        while let Some(chunk) =
            reader.next_chunk(crate::metadata::source_ancestry::MAX_SOURCE_ANCESTRY_CHUNK_BYTES)?
        {
            let transfer = staged_service::SourceChunkTransfer {
                request_id: ancestry_request_id.clone(),
                handoff_id: plan.expected_handoff.handoff_id.clone(),
                ordinal,
                chunk,
            };
            let stored = if *node == self.local_node()? {
                self.receive_source_ancestry_chunk(self.local_node()?, transfer)?
            } else {
                self.stage_rpc_value(node, staged_service::SOURCE_CHUNK_ROUTE, &transfer)?
            };
            if stored.request_id != ancestry_request_id || stored.ordinal != ordinal {
                return Err(error(
                    "source ancestry peer did not durably retain the exact chunk",
                ));
            }
            ordinal = ordinal
                .checked_add(1)
                .ok_or_else(|| error("source ancestry ordinal exhausted"))?;
        }
        let envelope = staged_service::StageSnapshot {
            command,
            plan: plan.clone(),
            ancestry_request_id,
        };
        let response = if *node == self.local_node()? {
            self.receive_snapshot(self.local_node()?, envelope)?
        } else {
            let transport = self
                .transport
                .as_ref()
                .ok_or_else(|| error("native component object-RPC transport is unavailable"))?;
            let image = envelope
                .command
                .image
                .as_ref()
                .ok_or_else(|| error("native import snapshot has no actual image resource"))?;
            if image.descriptor() != &envelope.command.snapshot {
                return Err(error("native import image differs from its exact command"));
            }
            let head = sinomo_core::network::ObjectRpcRequest::encode_body(
                staged_service::SNAPSHOT_ROUTE,
                "dfs-migration",
                &envelope,
            )
            .map_err(error)?;
            let call = sinomo_core::network::ObjectRpcCall::new(
                head,
                image.attachment_source().map_err(error)?,
            );
            let result = block_native(
                &self.runtime,
                transport.call_async(node, call, source_effect_rpc_options(), true),
            )?;
            let response = block_native(
                &self.runtime,
                result.into_materialized_value_response(64 * 1024),
            )?;
            sinomo_dfs::decode_dfs_metadata_snapshot_install_rpc_response(&response)
                .map_err(error)?
        };
        if response.request_id != request_id
            || response.shard_id != shard
            || response.next_after_sequence != sequence
            || !matches!(
                response.result,
                sinomo_dfs::DfsMetadataSnapshotInstallResult::Installed { .. }
            )
        {
            return Err(error(
                "snapshot response is not the exact submitted stage effect",
            ));
        }
        let probe = sinomo_dfs::DfsMetadataPrefixProbe {
            barrier_id: sinomo_core::Id::generate().to_string(),
            file_system_id: fs,
            authority_epoch: epoch,
            shard_id: shard.clone(),
            journal_sequence: sequence,
            state_fingerprint: fingerprint,
            primary_handoff_id: id.clone(),
            repair_custody: None,
        };
        let barrier = probe.barrier_id.clone();
        let response = if *node == self.local_node()? {
            self.receive_prefix(self.local_node()?, probe)?
        } else {
            let transport = self
                .transport
                .as_ref()
                .ok_or_else(|| error("native object-RPC transport is unavailable"))?;
            let call = sinomo_core::network::ObjectRpcCall::encode_borrowed_value(
                staged_service::PREFIX_ROUTE,
                "dfs-migration",
                &probe,
            )
            .map_err(error)?;
            let result = block_native(
                &self.runtime,
                transport.call_async(node, call, source_effect_rpc_options(), true),
            )?;
            let response = block_native(
                &self.runtime,
                result.into_materialized_value_response(64 * 1024),
            )?;
            sinomo_dfs::decode_dfs_metadata_journal_batch_rpc_response(&response).map_err(error)?
        };
        if response.request_id != barrier
            || response.shard_id != shard
            || response.next_after_sequence != sequence
            || !response.verified_prefix_only
            || response.verified_authority_probe_id != id
            || response.result != sinomo_dfs::DfsMetadataJournalBatchResult::Applied
        {
            return Err(error(
                "stage peer did not verify the actual current prefix and nonce",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // These are untrusted blueprint rows, not native receipts or network ACKs.
    // The parser and set checks cannot construct an import certificate.
    fn untrusted_handoff() -> DfsMetadataShardMembershipHandoff {
        let node = |name| sinomo_core::node::NodeId::from_legacy_named(name).unwrap();
        let retiring = DfsMetadataShardConfig {
            file_system_id: sinomo_dfs::DfsFileSystemId::from_legacy_named("blueprint-scope")
                .unwrap(),
            shard_id: "root".into(),
            primary_node_id: node("blueprint-primary"),
            replica_node_ids: vec![node("blueprint-replica-a"), node("blueprint-replica-b")],
            epoch: 1,
            assignment: sinomo_dfs::DfsMetadataShardAssignment::Root,
            default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
        };
        let mut desired = retiring.clone();
        desired.epoch = 2;
        DfsMetadataShardMembershipHandoff {
            cause: DfsMetadataShardHandoffCause::StorageImport {
                cut: DfsMetadataStorageImportCut {
                    source_cut_digest: "untrusted-cut".into(),
                    source_journal_sequence: 1,
                    source_state_fingerprint: "untrusted-source".into(),
                    target_prefix: sinomo_dfs::DfsMetadataPrefixProof {
                        version: 2,
                        journal_sequence: 1,
                        digest: "untrusted-prefix".into(),
                    },
                    converted_image_fingerprint: "untrusted-image".into(),
                    pending_mapping_digest: "untrusted-mapping".into(),
                },
            },
            handoff_id: "blueprint-only".into(),
            retiring_shard: retiring,
            desired_shard: desired,
            required_prepared_sequence: 1,
            required_head_watermark: None,
            retiring_primary_fence: None,
        }
    }
    fn request(success: Vec<KvOp>) -> KvTxnRequest {
        KvTxnRequest::new(
            KvRequestId::generate(),
            KvTxn {
                compares: Vec::new(),
                success,
                failure: Vec::new(),
            },
        )
    }
    #[test]
    fn stage_blueprint_rejects_foreign_key_and_competing_stage_rows() {
        let h = untrusted_handoff();
        let key = sinomo_dfs::metadata_shard_membership_handoff_key(
            &h.retiring_shard.file_system_id,
            &h.retiring_shard.shard_id,
        );
        let row = KvOp::put_serde(key, &h, None).unwrap();
        assert!(
            NativeImportAuthority::stage_record(&request(vec![
                KvOp::put_serde("foreign", &h, None).unwrap()
            ]))
            .unwrap()
            .is_none()
        );
        assert!(NativeImportAuthority::stage_record(&request(vec![row.clone(), row])).is_err());
    }
    #[test]
    fn joint_blueprint_rejects_missing_primary_majority_duplicates_and_foreign_nodes() {
        let h = untrusted_handoff();
        let mut proof = DfsMetadataShardMembershipHandoffProof {
            cause: h.cause,
            handoff_id: h.handoff_id,
            retiring_shard: h.retiring_shard.clone(),
            desired_shard: h.desired_shard,
            journal_sequence: 1,
            state_fingerprint: "untrusted-image".into(),
            acknowledged_node_ids: h.retiring_shard.replica_node_ids.clone(),
        };
        assert!(NativeImportAuthority::validate_joint_acknowledgements(&proof).is_err());
        proof.acknowledged_node_ids = vec![h.retiring_shard.primary_node_id.clone()];
        assert!(NativeImportAuthority::validate_joint_acknowledgements(&proof).is_err());
        proof.acknowledged_node_ids = vec![
            h.retiring_shard.primary_node_id.clone(),
            h.retiring_shard.primary_node_id.clone(),
            h.retiring_shard.replica_node_ids[0].clone(),
        ];
        assert!(NativeImportAuthority::validate_joint_acknowledgements(&proof).is_err());
        proof.acknowledged_node_ids = vec![
            h.retiring_shard.primary_node_id,
            h.retiring_shard.replica_node_ids[0].clone(),
            sinomo_core::node::NodeId::from_legacy_named("foreign-blueprint-node").unwrap(),
        ];
        assert!(NativeImportAuthority::validate_joint_acknowledgements(&proof).is_err());
    }
    #[test]
    fn activation_blueprint_does_not_recognize_proof_under_foreign_key() {
        let h = untrusted_handoff();
        let proof = DfsMetadataShardMembershipHandoffProof {
            cause: h.cause,
            handoff_id: h.handoff_id,
            retiring_shard: h.retiring_shard,
            desired_shard: h.desired_shard,
            journal_sequence: 1,
            state_fingerprint: "untrusted-image".into(),
            acknowledged_node_ids: Vec::new(),
        };
        assert!(
            NativeImportAuthority::activation_proof(&request(vec![
                KvOp::put_serde("foreign", &proof, None).unwrap()
            ]))
            .unwrap()
            .is_none()
        );
    }
}
