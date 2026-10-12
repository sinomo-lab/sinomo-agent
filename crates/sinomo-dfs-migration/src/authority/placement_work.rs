//! Archived original placement DATA restored by the existing current producer.
//! Native requests and actual source-scope imports establish the retirement;
//! the ordinary maintenance callback only plans NEW current work.
use super::namespace_work::RetirementBlueprint;
use super::placement_job_source::OriginalPlacementJob;
use super::*;
mod custody;
pub(super) use custody::{
    PlacementCustodyEnvelope, PlacementCustodyPhase, PlacementCustodyRequest,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct OriginalPlacementGoal {
    job: OriginalPlacementJob,
    assignments: Vec<DfsMetadataShardConfig>,
    #[serde(default)]
    related_entries: Vec<MetadataCapturedKvEntry>,
}
#[derive(Clone, Debug)]
pub(super) struct OriginalPlacementSource {
    pub(super) assignment: DfsMetadataShardConfig,
    pub(super) capsules: Vec<serde_json::Value>,
}

/// Per-component continuation DATA. A restart starts at zero and revalidates
/// already-saved native request receipts within the SAME bounded pass.
#[derive(Default)]
pub(super) struct PlacementRetirementCursor {
    goal_set_digest: String,
    next: usize,
}

impl NativeImportAuthority {
    fn placement_archive(&self) -> PathBuf {
        self.receiver_root.join("configuration-v7")
    }
    /// One already-supported native prefix captures the compact source records.
    /// Archive writes are bounded. The saved assignment cut never advances when
    /// a sibling source scope completes its actual import.
    pub(super) fn capture_original_placement_goals(
        &self,
        max_records: usize,
    ) -> Result<Option<Vec<OriginalPlacementGoal>>> {
        let root = self.placement_archive();
        std::fs::create_dir_all(&root).map_err(error)?;
        let observed = block_native(
            &self.runtime,
            self.cluster()?
                .kv_range_prefix_complete_authoritative_async("/dfs/filesystems/".into()),
        )?;
        current_authority(&observed.authority)?;
        let mut remaining = max_records;
        for entry in &observed.value {
            if !entry.key.contains("/placement-reconciliations/")
                && !entry.key.contains("/placement-reconciliations-v1/")
            {
                continue;
            }
            // Exact malformed source bytes remain archived before rejection.
            let job = match OriginalPlacementJob::requires_conversion(entry).and_then(|needed| {
                if needed {
                    OriginalPlacementJob::decode(entry).map(Some)
                } else {
                    Ok(None)
                }
            }) {
                Ok(Some(job)) => job,
                Ok(None) => continue,
                Err(failed) => {
                    let binding = digest(entry)?;
                    Self::save_blueprint(&root, &format!("original-{binding}.json"), entry)?;
                    return Err(failed);
                }
            };
            let path = root.join(format!("placement-goal-{}.json", job.source_digest()?));
            if path.exists() {
                let saved: OriginalPlacementGoal =
                    serde_json::from_slice(&std::fs::read(&path).map_err(error)?).map_err(error)?;
                if saved.job.entry != *entry {
                    return Err(error(
                        "archived placement goal changed its exact native row",
                    ));
                }
                continue;
            }
            if remaining == 0 {
                return Ok(None);
            }
            let prefix = format!(
                "{}/",
                sinomo_dfs::metadata_shards_prefix(&job.file_system_id)
            );
            let mut assignments = observed
                .value
                .iter()
                .filter(|row| row.key.starts_with(&prefix))
                .map(|row| row.decode::<DfsMetadataShardConfig>().map_err(error))
                .collect::<Result<Vec<_>>>()?;
            assignments.sort_by(|a, b| a.shard_id.cmp(&b.shard_id));
            if !job.terminal && assignments.is_empty() {
                return Err(error(
                    "original placement work has no actual captured source scopes",
                ));
            }
            if assignments
                .windows(2)
                .any(|pair| pair[0].shard_id == pair[1].shard_id)
                || assignments
                    .iter()
                    .any(|cfg| cfg.file_system_id != job.file_system_id)
            {
                return Err(error("placement source routing is duplicate or foreign"));
            }
            let related_entries = job
                .related_keys()
                .into_iter()
                .map(|key| MetadataCapturedKvEntry {
                    entry: observed.value.iter().find(|row| row.key == key).cloned(),
                    key,
                })
                .collect();
            let goal = OriginalPlacementGoal {
                job,
                assignments,
                related_entries,
            };
            Self::save_blueprint(&root, path.file_name().unwrap().to_str().unwrap(), &goal)?;
            remaining -= 1;
        }
        // The archive remains the restart source after exact old-key deletion.
        let mut goals = Vec::new();
        for entry in std::fs::read_dir(&root).map_err(error)? {
            let path = entry.map_err(error)?.path();
            if path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("placement-goal-") && n.ends_with(".json"))
            {
                let goal: OriginalPlacementGoal =
                    serde_json::from_slice(&std::fs::read(&path).map_err(error)?).map_err(error)?;
                let decoded = OriginalPlacementJob::decode(&goal.job.entry)?;
                if serde_json::to_value(&decoded).map_err(error)?
                    != serde_json::to_value(&goal.job).map_err(error)?
                {
                    return Err(error(
                        "archived placement goal differs from original source bytes",
                    ));
                }
                goals.push(goal);
            }
        }
        goals.sort_by(|a, b| a.job.entry.key.cmp(&b.job.entry.key));
        Ok(Some(goals))
    }

    fn verify_placement_blueprints(
        &self,
        goal: &OriginalPlacementGoal,
        blueprints: &[RetirementBlueprint],
    ) -> Result<()> {
        if goal.job.terminal {
            if !blueprints.is_empty() {
                return Err(error("terminal placement DATA has no active reservation"));
            }
            return Ok(());
        }
        if blueprints.len() != goal.assignments.len() {
            return Err(error(
                "placement retirement omits or repeats an original scope",
            ));
        }
        let required_scopes = goal
            .assignments
            .iter()
            .map(|cfg| (&cfg.file_system_id, &cfg.shard_id))
            .collect::<BTreeSet<_>>();
        for blueprint in blueprints {
            let native_scopes = blueprint
                .plan
                .source_cut
                .projection_assignments
                .iter()
                .filter(|cfg| cfg.file_system_id == goal.job.file_system_id)
                .map(|cfg| (&cfg.file_system_id, &cfg.shard_id))
                .collect::<BTreeSet<_>>();
            if native_scopes != required_scopes {
                return Err(error(
                    "placement retirement omitted actual original filesystem routing",
                ));
            }
        }
        for assignment in &goal.assignments {
            let capsule = goal.job.capsule_for_scope(assignment)?;
            let reservation = OriginalPlacementJob::verify_capsule(&capsule)?;
            let matching = blueprints
                .iter()
                .filter(|blueprint| {
                    blueprint
                        .plan
                        .expected_handoff
                        .retiring_shard
                        .file_system_id
                        == assignment.file_system_id
                        && blueprint.plan.expected_handoff.retiring_shard.shard_id
                            == assignment.shard_id
                })
                .collect::<Vec<_>>();
            if matching.len() != 1 {
                return Err(error(
                    "placement retirement has no unique actual source scope",
                ));
            }
            let blueprint = matching[0];
            let proof = Self::activation_proof(&blueprint.activation_request)?
                .ok_or_else(|| error("placement retirement lacks actual activation proof"))?;
            self.validate_activation_request(
                &blueprint.activation_request,
                &blueprint.plan,
                &proof,
            )?;
            Self::validate_data_reservation_retirement(
                &reservation,
                &capsule,
                &blueprint.plan,
                &proof,
            )?;
            self.original_scope_still_retired(&proof)?;
            let stage = self.commit(blueprint.plan.stage_request.clone())?;
            let activation = self.commit(blueprint.activation_request.clone())?;
            Self::require_committed(&stage)?;
            Self::require_committed(&activation)?;
            if !stage.result.replayed_committed_request
                || !activation.result.replayed_committed_request
            {
                return Err(error(
                    "placement retirement requires previously committed original requests",
                ));
            }
        }
        Ok(())
    }

    /// Returns only genuinely missing scopes, preferring this node's actual
    /// primary scopes so peers cannot wait on each other's unstarted imports.
    pub(super) fn required_original_placement_sources(
        &self,
        goals: &[OriginalPlacementGoal],
    ) -> Result<Vec<OriginalPlacementSource>> {
        let mut pending =
            BTreeMap::<(sinomo_dfs::DfsFileSystemId, String), OriginalPlacementSource>::new();
        let local = self.local_node()?;
        for goal in goals.iter().filter(|goal| !goal.job.terminal) {
            for assignment in &goal.assignments {
                let capsule = goal.job.capsule_for_scope(assignment)?;
                let reservation = OriginalPlacementJob::verify_capsule(&capsule)?;
                let complete = match self.recover_scope_retirement(
                    &assignment.file_system_id,
                    &sinomo_dfs::DfsMetadataShardId::new(&assignment.shard_id),
                )? {
                    Some((plan, _, _, proof))
                        if crate::metadata::data_reservation_refs(&plan.source_cut)?
                            .contains(&reservation) =>
                    {
                        Self::validate_data_reservation_retirement(
                            &reservation,
                            &capsule,
                            &plan,
                            &proof,
                        )?;
                        let owns_local_image = [&proof.retiring_shard, &proof.desired_shard]
                            .into_iter()
                            .any(|config| {
                                config.primary_node_id == local
                                    || config.replica_node_ids.contains(&local)
                            });
                        !owns_local_image
                            || crate::metadata::installed_native_data_goal_matches(
                                &self.metadata_root.join(&assignment.shard_id),
                                &plan,
                            )?
                    }
                    _ => false,
                };
                if !complete {
                    pending
                        .entry((
                            assignment.file_system_id.clone(),
                            assignment.shard_id.clone(),
                        ))
                        .or_insert_with(|| OriginalPlacementSource {
                            assignment: assignment.clone(),
                            capsules: Vec::new(),
                        })
                        .capsules
                        .push(capsule);
                }
            }
        }
        let mut result = pending.into_values().collect::<Vec<_>>();
        result.sort_by_key(|source| source.assignment.primary_node_id != local);
        Ok(result)
    }

    /// Exact old-key deletion occurs only after its complete source retirement.
    /// The archived source+native requests retain restart work until the SAME
    /// current planner has durably published the newly identified current job.
    pub(super) fn retire_and_queue_placement_goals(
        &self,
        goals: &[OriginalPlacementGoal],
        callback_budget: usize,
        cursor: &mut PlacementRetirementCursor,
    ) -> Result<bool> {
        if callback_budget == 0 {
            return Err(error("placement callback budget must be positive"));
        }
        let root = self.placement_archive();
        let goal_set_digest =
            digest(&("placement-retirement-goal-set-v1", &self.data_root, goals))?;
        if cursor.goal_set_digest != goal_set_digest {
            cursor.goal_set_digest = goal_set_digest.clone();
            cursor.next = 0;
        }
        let end = cursor.next.saturating_add(callback_budget).min(goals.len());
        for goal in &goals[cursor.next..end] {
            let binding = goal.job.source_digest()?;
            let proof_path = root.join(format!("placement-retirements-{binding}.json"));
            let blueprints: Vec<RetirementBlueprint> = if proof_path.is_file() {
                serde_json::from_slice(&std::fs::read(&proof_path).map_err(error)?)
                    .map_err(error)?
            } else {
                let mut blueprints = Vec::new();
                if !goal.job.terminal {
                    for assignment in &goal.assignments {
                        let (plan, _, activation, _) = self
                            .recover_scope_retirement(
                                &assignment.file_system_id,
                                &sinomo_dfs::DfsMetadataShardId::new(&assignment.shard_id),
                            )?
                            .ok_or_else(|| {
                                error("placement source import has not actually activated")
                            })?;
                        blueprints.push(RetirementBlueprint {
                            plan,
                            activation_request: activation.request,
                        });
                    }
                }
                self.verify_placement_blueprints(goal, &blueprints)?;
                Self::save_blueprint(
                    &root,
                    proof_path.file_name().unwrap().to_str().unwrap(),
                    &blueprints,
                )?;
                blueprints
            };
            self.verify_placement_blueprints(goal, &blueprints)?;
            if !self.retire_original_placement_goal(goal, &blueprints)? {
                return Ok(false);
            }
            // Only this actual committed deletion advances scheduling DATA.
            // Unknown/error retries retain this same goal/native request.
            cursor.next += 1;
        }
        if cursor.next < goals.len() {
            return Ok(false);
        }
        // ONE existing action for the exact captured archive/source goal set.
        // It reads the already-persisted per-goal retirement provenance lazily;
        // there is no duplicate queue or serialized author capability.
        let work = goals
            .iter()
            .filter(|goal| !goal.job.terminal)
            .cloned()
            .collect::<Vec<_>>();
        if work.is_empty() {
            return Ok(true);
        }
        let binding = goal_set_digest;
        let authority = self.clone();
        let data_dir = self.component_context.data_dir().clone();
        let cursor = Arc::new(Mutex::new((0usize, BTreeSet::<usize>::new())));
        sinomo_dfs::runtime_host::queue_namespace_reconciliation_work(
            &data_dir,
            Arc::clone(&self.owner),
            format!("placement-data-{binding}"),
            false, // planner may wait on work the ordinary maintenance pass drains
            Arc::new(move || {
                for _ in 0..callback_budget.min(work.len()) {
                    let index = {
                        let mut state = cursor.lock().map_err(|_| {
                            sinomo_dfs::runtime_host::DfsRuntimeError::Parse {
                                detail: "placement scheduling cursor poisoned".into(),
                            }
                        })?;
                        if state.1.len() == work.len() {
                            return Ok(true);
                        }
                        let mut index = state.0;
                        while state.1.contains(&index) {
                            index = (index + 1) % work.len();
                        }
                        state.0 = (index + 1) % work.len();
                        index
                    };
                    let goal = &work[index];
                    let blueprints: Vec<RetirementBlueprint> = serde_json::from_slice(
                        &std::fs::read(authority.placement_archive().join(format!(
                            "placement-retirements-{}.json",
                            goal.job.source_digest().map_err(|e| {
                                sinomo_dfs::runtime_host::DfsRuntimeError::Parse {
                                    detail: e.to_string(),
                                }
                            })?
                        )))
                        .map_err(|e| {
                            sinomo_dfs::runtime_host::DfsRuntimeError::Parse {
                                detail: e.to_string(),
                            }
                        })?,
                    )
                    .map_err(|e| {
                        sinomo_dfs::runtime_host::DfsRuntimeError::Parse {
                            detail: e.to_string(),
                        }
                    })?;
                    if authority
                        .publish_current_placement_goal(goal, &blueprints)
                        .map_err(|e| sinomo_dfs::runtime_host::DfsRuntimeError::Parse {
                            detail: e.to_string(),
                        })?
                    {
                        cursor
                            .lock()
                            .map_err(|_| sinomo_dfs::runtime_host::DfsRuntimeError::Parse {
                                detail: "placement scheduling cursor poisoned".into(),
                            })?
                            .1
                            .insert(index);
                    }
                }
                let complete = cursor
                    .lock()
                    .map_err(|_| sinomo_dfs::runtime_host::DfsRuntimeError::Parse {
                        detail: "placement scheduling cursor poisoned".into(),
                    })?
                    .1
                    .len()
                    == work.len();
                Ok(complete)
            }),
        )
        .map_err(error)?;
        Ok(true)
    }

    fn retire_original_placement_goal(
        &self,
        goal: &OriginalPlacementGoal,
        blueprints: &[RetirementBlueprint],
    ) -> Result<bool> {
        let root = self.placement_archive();
        let binding = goal.job.source_digest()?;
        if goal.job.source_schema == super::placement_job_source::OriginalPlacementSchema::V0 {
            // Preserve the original v0 fixed-request retirement. These old
            // jobs predate the v1 queue/permit family; current named work only
            // excludes their later fresh planner callback, as before.
            let request = KvTxnRequest::new(
                KvRequestId::derived(
                    "dfs-original-placement-data-retirement-v7",
                    binding.as_bytes(),
                ),
                KvTxn {
                    compares: captured_compares(&[MetadataCapturedKvEntry {
                        key: goal.job.entry.key.clone(),
                        entry: Some(goal.job.entry.clone()),
                    }]),
                    success: vec![KvOp::Delete {
                        key: goal.job.entry.key.clone(),
                    }],
                    failure: Vec::new(),
                },
            );
            let commit = self.blueprint_commit(&root, request)?;
            Self::require_committed(&commit)?;
            return Ok(true);
        }
        // Election must not prevent another upgraded host from recovering
        // its OWN exact prior-process latch on a nondefault source filesystem.
        // The existing normal host capability rejects a current/foreign holder.
        {
            let local = self.local_node()?;
            let mut client = self.control_client(&root)?;
            match DfsControlPlaneStore::new(&mut client)
                .prepare_storage_import_placement_retirement(&goal.job.file_system_id, &local)
            {
                Ok(_) | Err(KvError::Busy { .. }) => {}
                Err(failed) => return Err(error(failed)),
            }
        }
        let Some(coordinator) = self.placement_coordinator(goal)? else {
            return Ok(false);
        };
        if coordinator.node_id != self.local_node()? {
            return self.fetch_placement_custody(
                goal,
                PlacementCustodyPhase::Retirement,
                &coordinator,
            );
        }
        let attempt_path = root.join(format!("placement-deletion-{binding}.json"));
        if let Some(saved) =
            self.saved_placement_custody(goal, PlacementCustodyPhase::Retirement)?
        {
            if self.replay_confirmed_placement_custody(goal, &saved)? {
                return Ok(true);
            }
            if !attempt_path.is_file()
                && serde_json::to_value(&saved.coordinator.guards).map_err(error)?
                    == serde_json::to_value(&coordinator.guards).map_err(error)?
            {
                return Ok(false);
            }
        }
        // Reconcile the exact saved request before reading a new cut.
        let prior = attempt_path.is_file().then_some(&attempt_path);
        if let Some(path) = prior {
            let envelope: PlacementCustodyEnvelope =
                serde_json::from_slice(&std::fs::read(path).map_err(error)?).map_err(error)?;
            if envelope.goal.job.entry != goal.job.entry
                || envelope.phase != PlacementCustodyPhase::Retirement
            {
                return Err(error(
                    "owned retirement attempt changed original subject/phase",
                ));
            }
            if let Some(commit) = self.commit_owned_placement_attempt(&envelope)? {
                if commit
                    .result
                    .txn_response
                    .as_ref()
                    .is_some_and(|result| result.succeeded)
                {
                    Self::require_committed(&commit)?;
                    return Ok(true);
                }
            }
            // A native rejection or a freshly fenced, still-unapplied old cut permits a new attempt.
        }
        let mut client = self.control_client(&root)?;
        let local = self.local_node()?;
        let permit_compare = match DfsControlPlaneStore::new(&mut client)
            .prepare_storage_import_placement_retirement(&goal.job.file_system_id, &local)
        {
            Ok(compare) => compare,
            Err(KvError::Busy { .. }) => return Ok(false),
            Err(failed) => return Err(error(failed)),
        };
        let normal_permit = sinomo_dfs::placement_authority_permit_key(&goal.job.file_system_id);
        let raw_permit = format!(
            "/dfs/filesystems/{}/placement-authority-permit-v1",
            goal.job.raw_file_system_id
        );
        let mut keys = vec![
            goal.job.entry.key.clone(),
            normal_permit.clone(),
            raw_permit.clone(),
        ];
        keys.extend(goal.job.related_keys());
        keys.extend(coordinator.guards.iter().map(|row| row.key.clone()));
        let mut retired = Vec::new();
        for blueprint in blueprints {
            let proof = Self::activation_proof(&blueprint.activation_request)?
                .ok_or_else(|| error("placement retirement has no actual proof"))?;
            keys.push(sinomo_dfs::metadata_shard_key(
                &proof.desired_shard.file_system_id,
                &proof.desired_shard.shard_id,
            ));
            keys.push(sinomo_dfs::metadata_shard_membership_handoff_proof_key(
                &proof.desired_shard.file_system_id,
                &proof.desired_shard.shard_id,
            ));
            retired.push(proof);
        }
        keys.sort();
        keys.dedup();
        let (captured, authority) = self.read_exact(&keys)?;
        // Every shared row is archived exactly, even if unrelated IDs remain.
        for row in &captured {
            if let Some(entry) = &row.entry {
                let archived = digest(entry)?;
                Self::save_blueprint(&root, &format!("original-{archived}.json"), entry)?;
            }
        }

        let lookup = |key: &str| {
            captured
                .iter()
                .find(|row| row.key == key)
                .ok_or_else(|| error("placement exact cut omits a requested key"))
        };
        if coordinator
            .guards
            .iter()
            .any(|guard| match lookup(&guard.key) {
                Ok(row) => row.entry != guard.entry,
                Err(_) => true,
            })
        {
            return Ok(false);
        }
        if lookup(&goal.job.entry.key)?.entry.as_ref() != Some(&goal.job.entry) {
            return Err(error(
                "original placement job changed without this native deletion receipt",
            ));
        }
        if lookup(&normal_permit)?.entry.is_some() || lookup(&raw_permit)?.entry.is_some() {
            return Ok(false); // raced/live holder: preserve all original work
        }
        for proof in retired {
            let cfg_key = sinomo_dfs::metadata_shard_key(
                &proof.desired_shard.file_system_id,
                &proof.desired_shard.shard_id,
            );
            let config: DfsMetadataShardConfig = lookup(&cfg_key)?
                .entry
                .as_ref()
                .ok_or_else(|| error("retired placement scope has no current assignment"))?
                .decode()
                .map_err(error)?;
            let proof_key = sinomo_dfs::metadata_shard_membership_handoff_proof_key(
                &proof.desired_shard.file_system_id,
                &proof.desired_shard.shard_id,
            );
            let current: sinomo_dfs::DfsMetadataShardMembershipHandoffProof = lookup(&proof_key)?
                .entry
                .as_ref()
                .ok_or_else(|| error("retired placement scope has no current proof"))?
                .decode()
                .map_err(error)?;
            if config.file_system_id != proof.desired_shard.file_system_id
                || config.shard_id != proof.desired_shard.shard_id
                || config.epoch < proof.desired_shard.epoch
                || current.desired_shard.file_system_id != config.file_system_id
                || current.desired_shard.shard_id != config.shard_id
                || current.desired_shard.epoch < proof.desired_shard.epoch
                || current.desired_shard.epoch > config.epoch
            {
                return Err(error(
                    "placement source retirement changed at exact deletion cut",
                ));
            }
        }
        let mut success = vec![KvOp::Delete {
            key: goal.job.entry.key.clone(),
        }];
        success.extend(goal.job.related_retirement_ops(&captured)?);
        let mut compares = OriginalPlacementJob::exact_retirement_compares(&captured);
        compares.push(permit_compare);
        let txn = KvTxn {
            compares,
            success,
            failure: Vec::new(),
        };
        let attempt = digest(&(&binding, &txn, &authority))?;
        let request = KvTxnRequest::new(
            KvRequestId::derived(
                "dfs-original-placement-exact-retirement-v7",
                attempt.as_bytes(),
            ),
            txn,
        );
        let envelope = PlacementCustodyEnvelope {
            goal: goal.clone(),
            phase: PlacementCustodyPhase::Retirement,
            coordinator,
            request,
            blueprints: blueprints.to_vec(),
        };
        sinomo_core::storage::durable_file::write_file_atomic(
            &attempt_path,
            &serde_json::to_vec(&envelope).map_err(error)?,
            sinomo_core::storage::durable_file::DurableAtomicFileOptions::new("import", "tmp")
                .sync_all(),
        )
        .map_err(error)?;
        let Some(commit) = self.commit_owned_placement_attempt(&envelope)? else {
            return Ok(false);
        };
        if commit
            .result
            .txn_response
            .as_ref()
            .is_some_and(|result| result.succeeded)
        {
            Self::require_committed(&commit)?;
            return Ok(true);
        }
        Ok(false)
    }

    fn publish_current_placement_goal(
        &self,
        goal: &OriginalPlacementGoal,
        blueprints: &[RetirementBlueprint],
    ) -> Result<bool> {
        if *self.lifecycle.borrow() || self.lifecycle.has_changed().is_err() {
            return Err(error("placement restore owner lifecycle is closed"));
        }
        self.verify_placement_blueprints(goal, blueprints)?;
        let root = self.placement_archive();
        let Some(coordinator) = self.placement_coordinator(goal)? else {
            return Ok(false);
        };
        if let Some(saved) =
            self.saved_placement_custody(goal, PlacementCustodyPhase::Publication)?
        {
            if self.replay_confirmed_placement_custody(goal, &saved)? {
                return Ok(true);
            }
            let local_attempt = root.join(format!(
                "placement-publication-{}.json",
                goal.job.source_digest()?
            ));
            if !local_attempt.is_file()
                && serde_json::to_value(&saved.coordinator.guards).map_err(error)?
                    == serde_json::to_value(&coordinator.guards).map_err(error)?
            {
                return Ok(false);
            }
        }
        if self.fetch_placement_custody(goal, PlacementCustodyPhase::Publication, &coordinator)? {
            return Ok(true);
        }
        if coordinator.node_id != self.local_node()? {
            return Ok(false);
        }
        let request_path = root.join(format!(
            "placement-publication-{}.json",
            goal.job.source_digest()?
        ));
        if request_path.is_file() {
            let envelope: PlacementCustodyEnvelope =
                serde_json::from_slice(&std::fs::read(&request_path).map_err(error)?)
                    .map_err(error)?;
            if envelope.goal.job.entry != goal.job.entry
                || envelope.phase != PlacementCustodyPhase::Publication
            {
                return Err(error(
                    "owned publication attempt changed original subject/phase",
                ));
            }
            let proposed = Self::placement_request_job(&goal.job, &envelope.request)?;
            goal.job.validate_current_goal(&proposed)?;
            if let Some(commit) = self.commit_owned_placement_attempt(&envelope)? {
                if commit
                    .result
                    .txn_response
                    .as_ref()
                    .is_some_and(|response| response.succeeded)
                {
                    Self::require_committed(&commit)?;
                    self.validate_published_placement_goal(goal, &commit.request)?;
                    return Ok(true);
                }
            }
            // A native rejection or a freshly fenced, still-unapplied owner cut
            // permits the current planner to capture a new request.
        }
        let mut client = self.control_client(&root)?;
        let current_snapshot = if goal.job.is_adaptive() {
            let Some(snapshot) = client.storage_import_placement_snapshot() else {
                return Ok(false);
            };
            if snapshot.nodes.is_empty() || time::OffsetDateTime::now_utc() > snapshot.expires_at {
                return Ok(false);
            }
            Some((*snapshot).clone())
        } else {
            None
        };
        let mut store = DfsControlPlaneStore::new(&mut client);
        let config = store
            .get_file_system_config(&goal.job.file_system_id)
            .map_err(error)?
            .ok_or_else(|| error("current placement goal has no current filesystem"))?;
        if let Some(snapshot) = &current_snapshot {
            let Some(root_assignment) = store
                .select_metadata_shard_for_path_strict_default_sifs_split(
                    &goal.job.file_system_id,
                    "/",
                )
                .map_err(error)?
            else {
                return Ok(false);
            };
            if snapshot.primary_node_id != self.local_node()?
                || root_assignment.primary_node_id != snapshot.primary_node_id
            {
                return Ok(false);
            }
        }
        let view = store.cluster_view().map_err(error)?;
        let candidates = view
            .content_nodes
            .into_iter()
            .filter(|node| node.accepts_content)
            .collect::<Vec<_>>();
        if candidates.is_empty() {
            return Ok(false);
        }
        let now =
            u64::try_from(time::OffsetDateTime::now_utc().unix_timestamp_nanos()).map_err(error)?;
        let job = goal
            .job
            .build_current_job(&config, candidates, current_snapshot, now)?;
        let plan =
            sinomo_dfs::DfsPlacementUpdatePlan::new(&config, &config, &job).map_err(error)?;
        let mut read_keys = plan.keys().to_vec();
        read_keys.extend(coordinator.guards.iter().map(|row| row.key.clone()));
        let (all_captured, proof) = self.read_exact(&read_keys)?;
        let captured = all_captured[..plan.keys().len()].to_vec();
        let owner_entries = &all_captured[plan.keys().len()..];
        if owner_entries
            .iter()
            .zip(&coordinator.guards)
            .any(|(actual, expected)| actual.key != expected.key || actual.entry != expected.entry)
        {
            return Ok(false);
        }
        let entries = captured
            .iter()
            .map(|entry| entry.entry.clone())
            .collect::<Vec<_>>();
        let queued = match plan.queued_job_keys(entries.clone()) {
            Ok(Some(queued)) => queued,
            Ok(None) | Err(KvError::Busy { .. }) => return Ok(false),
            Err(failed) => return Err(error(failed)),
        };
        // No previous queued jobs means no additional DATA keys to read.
        // The nonempty plan/coordinator cut and its actual native proof above
        // still fence this publication; empty DATA manufactures no authority.
        let queued_entries = if queued.is_empty() {
            Vec::new()
        } else {
            self.read_exact(&queued)?.0
        };
        let mut txn = match plan.transaction(
            entries,
            queued_entries.iter().map(|row| row.entry.clone()).collect(),
        ) {
            Ok(Some(txn)) => txn,
            Ok(None) | Err(KvError::Busy { .. }) => return Ok(false),
            Err(failed) => return Err(error(failed)),
        };
        txn.compares.push(KvCompare::Missing {
            key: goal.job.entry.key.clone(),
        });
        txn.compares
            .extend(OriginalPlacementJob::exact_retirement_compares(
                owner_entries,
            ));
        // A definitive compare rejection can be retried at a NEW native cut;
        // a previously stored request is always reconciled before planning.
        let attempt = digest(&(
            goal.job.source_digest()?,
            &captured,
            &queued_entries,
            &proof,
            &txn,
        ))?;
        let request = KvTxnRequest::new(
            KvRequestId::derived("dfs-current-placement-goal-v7", attempt.as_bytes()),
            txn,
        );
        let envelope = PlacementCustodyEnvelope {
            goal: goal.clone(),
            phase: PlacementCustodyPhase::Publication,
            coordinator,
            request,
            blueprints: blueprints.to_vec(),
        };
        sinomo_core::storage::durable_file::write_file_atomic(
            &request_path,
            &serde_json::to_vec(&envelope).map_err(error)?,
            sinomo_core::storage::durable_file::DurableAtomicFileOptions::new("import", "tmp")
                .sync_all(),
        )
        .map_err(error)?;
        let Some(commit) = self.commit_owned_placement_attempt(&envelope)? else {
            return Ok(false);
        };
        let published = commit
            .result
            .txn_response
            .as_ref()
            .is_some_and(|result| result.succeeded);
        if published {
            Self::require_committed(&commit)?;
            self.validate_published_placement_goal(goal, &commit.request)?;
        }
        Ok(published)
    }
}

#[cfg(test)]
pub(in crate::authority) mod native_tests;
