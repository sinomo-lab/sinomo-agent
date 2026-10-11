//! Saved request DATA for the SAME placement controller. The native outcome
//! owner must confirm an exact committed command before a peer can replay it.
use super::*;
use sinomo_core::kv::KvRequestOutcome;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum PlacementCustodyPhase {
    Retirement,
    Publication,
}
impl PlacementCustodyPhase {
    fn name(self) -> &'static str {
        match self {
            Self::Retirement => "retirement",
            Self::Publication => "publication",
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PlacementCoordinator {
    pub(crate) node_id: sinomo_core::node::NodeId,
    pub(crate) peers: Vec<sinomo_core::node::NodeId>,
    pub(crate) guards: Vec<MetadataCapturedKvEntry>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PlacementCustodyEnvelope {
    pub(crate) goal: OriginalPlacementGoal,
    pub(crate) phase: PlacementCustodyPhase,
    pub(crate) coordinator: PlacementCoordinator,
    pub(crate) request: KvTxnRequest,
    pub(crate) blueprints: Vec<RetirementBlueprint>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "kebab-case", deny_unknown_fields)]
pub(crate) enum PlacementCustodyRequest {
    Store {
        envelope: PlacementCustodyEnvelope,
    },
    Fetch {
        source_entry: KvEntry,
        phase: PlacementCustodyPhase,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PlacementCustodyResponse {
    pub(crate) stored_request_id: Option<KvRequestId>,
    pub(crate) envelope: Option<PlacementCustodyEnvelope>,
}
impl NativeImportAuthority {
    pub(super) fn placement_coordinator(
        &self,
        goal: &OriginalPlacementGoal,
    ) -> Result<Option<PlacementCoordinator>> {
        if goal.job.terminal {
            // An administrative work owner from ACTUAL native voters, never a
            // fabricated DFS primary or a permission from accepted_config.
            let key = sinomo_core::kv::cluster_kv_raft_config_key();
            let (guards, _) = self.read_exact(&[key])?;
            let entry = guards[0].entry.as_ref().ok_or_else(|| {
                error("terminal placement cleanup has no actual native configuration")
            })?;
            let config: sinomo_core::kv::ClusterKvRaftConfigRecord =
                entry.decode().map_err(error)?;
            let paths = sinomo_core::network::ClusterPaths::new(self.component_context.data_dir());
            let state =
                sinomo_core::cluster::read_cluster_state_without_kv(&paths).map_err(error)?;
            if config.cluster_id != state.cluster_id {
                return Err(error("terminal cleanup configuration is foreign"));
            }
            let peers = config
                .voters
                .into_iter()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            let node_id = peers
                .first()
                .cloned()
                .ok_or_else(|| error("terminal cleanup has no actual configured native voter"))?;
            return Ok(Some(PlacementCoordinator {
                node_id,
                peers,
                guards,
            }));
        }
        let root = self.placement_archive();
        let mut client = self.control_client(&root)?;
        let store = DfsControlPlaneStore::new(&mut client);
        let Some(selected) = store
            .select_metadata_shard_for_path_strict_default_sifs_split(&goal.job.file_system_id, "/")
            .map_err(error)?
        else {
            return Ok(None);
        };
        let key = sinomo_dfs::metadata_shard_key(&selected.file_system_id, &selected.shard_id);
        let (guards, _) = self.read_exact(&[key])?;
        let current: DfsMetadataShardConfig = guards[0]
            .entry
            .as_ref()
            .ok_or_else(|| error("placement coordinator disappeared"))?
            .decode()
            .map_err(error)?;
        if current != selected {
            return Ok(None);
        }
        let peers = std::iter::once(current.primary_node_id.clone())
            .chain(current.replica_node_ids)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        Ok(Some(PlacementCoordinator {
            node_id: current.primary_node_id,
            peers,
            guards,
        }))
    }
    fn custody_path(
        &self,
        goal: &OriginalPlacementGoal,
        phase: PlacementCustodyPhase,
    ) -> Result<PathBuf> {
        Ok(self.placement_archive().join(format!(
            "placement-custody-{}-{}.json",
            goal.job.source_digest()?,
            phase.name()
        )))
    }
    pub(super) fn saved_placement_custody(
        &self,
        goal: &OriginalPlacementGoal,
        phase: PlacementCustodyPhase,
    ) -> Result<Option<PlacementCustodyEnvelope>> {
        let path = self.custody_path(goal, phase)?;
        if !path.is_file() {
            return Ok(None);
        }
        let envelope: PlacementCustodyEnvelope =
            serde_json::from_slice(&std::fs::read(path).map_err(error)?).map_err(error)?;
        if envelope.goal.job.entry != goal.job.entry || envelope.phase != phase {
            return Err(error(
                "saved placement custody changes original subject/phase",
            ));
        }
        self.validate_placement_custody(&envelope)?;
        Ok(Some(envelope))
    }
    fn validate_placement_custody(&self, envelope: &PlacementCustodyEnvelope) -> Result<()> {
        let original = OriginalPlacementJob::decode(&envelope.goal.job.entry)?;
        if serde_json::to_value(&original).map_err(error)?
            != serde_json::to_value(&envelope.goal.job).map_err(error)?
        {
            return Err(error(
                "placement custody changes exact original source DATA",
            ));
        }
        if !envelope.request.txn.failure.is_empty() {
            return Err(error(
                "placement custody request has unexpected failure effects",
            ));
        }
        for compare in OriginalPlacementJob::exact_retirement_compares(&envelope.coordinator.guards)
        {
            if !envelope.request.txn.compares.contains(&compare) {
                return Err(error("placement custody omits actual coordinator fence"));
            }
        }
        match envelope.phase {
            PlacementCustodyPhase::Retirement => {
                if !envelope
                    .request
                    .txn
                    .success
                    .iter()
                    .any(|op| matches!(op, KvOp::Delete { key } if key == &original.entry.key))
                {
                    return Err(error(
                        "placement retirement custody omits exact old-key deletion",
                    ));
                }
                for compare in
                    OriginalPlacementJob::exact_retirement_compares(&[MetadataCapturedKvEntry {
                        key: original.entry.key.clone(),
                        entry: Some(original.entry.clone()),
                    }])
                {
                    if !envelope.request.txn.compares.contains(&compare) {
                        return Err(error(
                            "placement custody changes original bytes/revision/version/lease",
                        ));
                    }
                }
            }
            PlacementCustodyPhase::Publication => {
                if original.terminal {
                    return Err(error("terminal source DATA cannot publish current work"));
                }
                let proposed = Self::placement_request_job(&original, &envelope.request)?;
                original.validate_current_goal(&proposed)?;
            }
        }
        Ok(())
    }
    pub(super) fn placement_request_job(
        original: &OriginalPlacementJob,
        request: &KvTxnRequest,
    ) -> Result<sinomo_dfs::DfsPlacementReconciliationJob> {
        let key = original.current_job_key()?;
        let rows = request
            .txn
            .success
            .iter()
            .filter_map(|op| match op {
                KvOp::Put {
                    key: actual, value, ..
                } if actual == &key => Some(value),
                _ => None,
            })
            .collect::<Vec<_>>();
        if rows.len() != 1 {
            return Err(error(
                "winning placement request omits/repeats its exact current job",
            ));
        }
        sinomo_core::kv::decode_kv_value(rows[0]).map_err(error)
    }
    pub(super) fn placement_outcome(&self, request: &KvTxnRequest) -> Result<Option<bool>> {
        let read = block_native(
            &self.runtime,
            self.cluster()?
                .kv_request_outcome_authoritative_async(request.request_id.clone()),
        )?;
        current_authority(&read.authority)?;
        let record = match read.outcome {
            KvRequestOutcome::Committed(record) => record,
            KvRequestOutcome::RejectedBeforeCommit { .. } => return Ok(Some(false)),
            KvRequestOutcome::Unknown { .. } | KvRequestOutcome::Expired => return Ok(None),
        };
        let exact = sinomo_core::kv::kv_command_digest(&KvCommand::Txn(request.txn.clone()))
            .map_err(error)?;
        if record.request_id != request.request_id
            || record.command_digest != exact
            || record.term == 0
            || record.log_index == 0
            || read.authority.term < record.term
            || read.authority.commit_index < record.log_index
            || read.authority.revision < record.revision
        {
            return Err(error(
                "native placement outcome changes exact request/digest/term/index",
            ));
        }
        Ok(Some(
            record
                .txn_response
                .as_ref()
                .is_some_and(|response| response.succeeded),
        ))
    }
    fn placement_committed(&self, request: &KvTxnRequest) -> Result<bool> {
        self.placement_outcome(request)
            .map(|outcome| outcome == Some(true))
    }
    /// Only the controller's locally owned attempt path calls this method.
    /// Receiving an envelope never enters it: peer DATA uses confirmed replay.
    pub(super) fn commit_owned_placement_attempt(
        &self,
        envelope: &PlacementCustodyEnvelope,
    ) -> Result<Option<DfsNativeTxnCommit>> {
        self.validate_placement_custody(envelope)?;
        match self.placement_outcome(&envelope.request)? {
            Some(false) => return Ok(None),
            Some(true) => {
                let replay = self.commit(envelope.request.clone())?;
                Self::require_committed(&replay)?;
                if !replay.result.replayed_committed_request {
                    return Err(error(
                        "known placement outcome was not an exact committed replay",
                    ));
                }
                return Ok(Some(replay));
            }
            None => {}
        }
        let Some(current) = self.placement_coordinator(&envelope.goal)? else {
            return Ok(None);
        };
        if current.node_id != self.local_node()? {
            return Ok(None);
        }
        let fenced = current.guards.iter().any(|guard| {
            envelope
                .request
                .txn
                .compares
                .iter()
                .any(|compare| match compare {
                    KvCompare::ValueEquals { key, value } if key == &guard.key => guard
                        .entry
                        .as_ref()
                        .is_none_or(|entry| &entry.value != value),
                    KvCompare::ModifyRevisionEquals { key, revision } if key == &guard.key => guard
                        .entry
                        .as_ref()
                        .is_none_or(|entry| entry.modify_revision != *revision),
                    KvCompare::LeaseEquals { key, lease_id } if key == &guard.key => {
                        guard.entry.as_ref().map(|entry| entry.lease_id) != Some(*lease_id)
                    }
                    _ => false,
                })
        });
        if fenced {
            // This is fresh scheduling DATA, not a fabricated rejection receipt.
            // Confirm the old effect is still absent before planning at a new
            // owner cut. A missing old row/present new job never means success.
            let key = match envelope.phase {
                PlacementCustodyPhase::Retirement => envelope.goal.job.entry.key.clone(),
                PlacementCustodyPhase::Publication => envelope.goal.job.current_job_key()?,
            };
            let (rows, _) = self.read_exact(&[key])?;
            let unapplied = match envelope.phase {
                PlacementCustodyPhase::Retirement => {
                    rows[0].entry.as_ref() == Some(&envelope.goal.job.entry)
                }
                PlacementCustodyPhase::Publication => rows[0].entry.is_none(),
            };
            if !unapplied {
                return Err(error(
                    "fenced placement attempt needs its actual winning native receipt",
                ));
            }
            return Ok(None);
        }
        // Saving a request is not proof that the previous cohort fanout ran.
        // Repeat the SAME complete DATA retention before every first effect.
        self.retain_placement_custody(envelope)?;
        self.commit(envelope.request.clone()).map(Some)
    }

    pub(super) fn validate_published_placement_goal(
        &self,
        goal: &OriginalPlacementGoal,
        request: &KvTxnRequest,
    ) -> Result<()> {
        let proposed = Self::placement_request_job(&goal.job, request)?;
        let (rows, _) = self.read_exact(&[goal.job.current_job_key()?])?;
        let current: sinomo_dfs::DfsPlacementReconciliationJob = rows[0]
            .entry
            .as_ref()
            .ok_or_else(|| error("winning current placement job disappeared"))?
            .decode()
            .map_err(error)?;
        goal.job.validate_current_goal(&current)?;
        if !same_immutable_current_goal(&proposed, &current)
            || current.processed_content_bytes < proposed.processed_content_bytes
        {
            return Err(error(
                "present current job changes the winning immutable placement goal",
            ));
        }
        Ok(())
    }
    pub(super) fn replay_confirmed_placement_custody(
        &self,
        goal: &OriginalPlacementGoal,
        envelope: &PlacementCustodyEnvelope,
    ) -> Result<bool> {
        self.validate_placement_custody(envelope)?;
        if envelope.goal.job.entry != goal.job.entry {
            return Err(error("peer custody changes original DATA subject"));
        }
        // Never turn received uncommitted request DATA into its first effect.
        if !self.placement_committed(&envelope.request)? {
            return Ok(false);
        }
        for blueprint in &envelope.blueprints {
            if !self.placement_committed(&blueprint.plan.stage_request)?
                || !self.placement_committed(&blueprint.activation_request)?
            {
                return Ok(false);
            }
        }
        self.verify_placement_blueprints(goal, &envelope.blueprints)?;
        let commit = self.commit(envelope.request.clone())?;
        Self::require_committed(&commit)?;
        if !commit.result.replayed_committed_request {
            return Err(error(
                "peer placement custody was not an actual committed replay",
            ));
        }
        if envelope.phase == PlacementCustodyPhase::Publication {
            self.validate_published_placement_goal(goal, &envelope.request)?;
        }
        Ok(true)
    }
    fn placement_custody_call(
        &self,
        node: &sinomo_core::node::NodeId,
        request: PlacementCustodyRequest,
    ) -> Result<PlacementCustodyResponse> {
        if *node == self.local_node()? {
            return self.receive_placement_custody(self.local_node()?, request);
        }
        let transport = self
            .transport
            .as_ref()
            .ok_or_else(|| error("placement custody has no actual native object-RPC transport"))?;
        let call = sinomo_core::network::ObjectRpcCall::encode_value(
            super::super::staged_service::PLACEMENT_CUSTODY_ROUTE,
            "dfs-migration",
            request,
        )
        .map_err(error)?;
        let result = block_native(
            &self.runtime,
            transport.call_async(node, call, source_effect_rpc_options(), true),
        )?;
        block_native(
            &self.runtime,
            result.decode_body::<PlacementCustodyResponse>(),
        )
    }
    pub(super) fn retain_placement_custody(
        &self,
        envelope: &PlacementCustodyEnvelope,
    ) -> Result<()> {
        self.validate_placement_custody(envelope)?;
        let current = self
            .placement_coordinator(&envelope.goal)?
            .ok_or_else(|| error("placement custody has no current owner"))?;
        if current.node_id != self.local_node()?
            || serde_json::to_value(&current.guards).map_err(error)?
                != serde_json::to_value(&envelope.coordinator.guards).map_err(error)?
        {
            return Err(error(
                "only the current actual placement work owner authors a request",
            ));
        }
        // Custody acknowledgements retain DATA only; native consensus and DFS
        // authority do not count them as publication/quorum evidence.
        for peer in &current.peers {
            let stored = self.placement_custody_call(
                peer,
                PlacementCustodyRequest::Store {
                    envelope: envelope.clone(),
                },
            )?;
            if stored.stored_request_id.as_ref() != Some(&envelope.request.request_id) {
                return Err(error(
                    "placement peer did not retain the exact authored request DATA",
                ));
            }
        }
        Ok(())
    }
    pub(super) fn fetch_placement_custody(
        &self,
        goal: &OriginalPlacementGoal,
        phase: PlacementCustodyPhase,
        coordinator: &PlacementCoordinator,
    ) -> Result<bool> {
        let mut peers = coordinator.peers.clone();
        peers.extend(goal.assignments.iter().flat_map(|config| {
            std::iter::once(config.primary_node_id.clone()).chain(config.replica_node_ids.clone())
        }));
        peers.sort();
        peers.dedup();
        for peer in peers {
            let fetched = match self.placement_custody_call(
                &peer,
                PlacementCustodyRequest::Fetch {
                    source_entry: goal.job.entry.clone(),
                    phase,
                },
            ) {
                Ok(fetched) => fetched,
                Err(_) => continue,
            };
            let Some(envelope) = fetched.envelope else {
                continue;
            };
            if self.replay_confirmed_placement_custody(goal, &envelope)? {
                self.save_placement_custody_data(&envelope)?;
                return Ok(true);
            }
        }
        Ok(false)
    }
    fn save_placement_custody_data(&self, envelope: &PlacementCustodyEnvelope) -> Result<()> {
        let root = self.placement_archive();
        std::fs::create_dir_all(&root).map_err(error)?;
        Self::save_blueprint(
            &root,
            &format!("native-request-{}.json", envelope.request.request_id),
            &envelope.request,
        )?;
        Self::save_blueprint(
            &root,
            &format!(
                "placement-custody-request-{}.json",
                envelope.request.request_id
            ),
            envelope,
        )?;
        let path = self.custody_path(&envelope.goal, envelope.phase)?;
        if path.is_file() {
            let old: PlacementCustodyEnvelope =
                serde_json::from_slice(&std::fs::read(&path).map_err(error)?).map_err(error)?;
            if old.request != envelope.request {
                match self.placement_outcome(&old.request)? {
                    Some(true) => {
                        return Err(error("winning placement custody cannot be replaced"));
                    }
                    Some(false) => {}
                    None if serde_json::to_value(&old.coordinator.guards).map_err(error)?
                        != serde_json::to_value(&envelope.coordinator.guards).map_err(error)? => {}
                    None => {
                        return Err(error(
                            "unresolved placement proposal retains its exact request identity",
                        ));
                    }
                }
            }
        }
        sinomo_core::storage::durable_file::write_file_atomic(
            &path,
            &serde_json::to_vec(envelope).map_err(error)?,
            sinomo_core::storage::durable_file::DurableAtomicFileOptions::new("import", "tmp")
                .sync_all(),
        )
        .map_err(error)
    }
    pub(in crate::authority) fn receive_placement_custody(
        &self,
        source: sinomo_core::node::NodeId,
        request: PlacementCustodyRequest,
    ) -> Result<PlacementCustodyResponse> {
        if *self.lifecycle.borrow() || self.lifecycle.has_changed().is_err() {
            return Err(error("placement custody owner lifecycle is closed"));
        }
        match request {
            PlacementCustodyRequest::Store { envelope } => {
                self.validate_placement_custody(&envelope)?;
                let current = self.placement_coordinator(&envelope.goal)?.ok_or_else(|| {
                    error("received placement custody has no actual current owner")
                })?;
                if source != current.node_id
                    || envelope.coordinator.node_id != current.node_id
                    || envelope.coordinator.peers != current.peers
                    || serde_json::to_value(&current.guards).map_err(error)?
                        != serde_json::to_value(&envelope.coordinator.guards).map_err(error)?
                {
                    return Err(error(
                        "received placement request DATA did not come from actual current work owner",
                    ));
                }
                self.save_placement_custody_data(&envelope)?;
                Ok(PlacementCustodyResponse {
                    stored_request_id: Some(envelope.request.request_id),
                    envelope: None,
                })
            }
            PlacementCustodyRequest::Fetch {
                source_entry,
                phase,
            } => {
                let original = OriginalPlacementJob::decode(&source_entry)?;
                let path = self.placement_archive().join(format!(
                    "placement-custody-{}-{}.json",
                    original.source_digest()?,
                    phase.name()
                ));
                let envelope = if path.is_file() {
                    let envelope: PlacementCustodyEnvelope =
                        serde_json::from_slice(&std::fs::read(path).map_err(error)?)
                            .map_err(error)?;
                    if envelope.goal.job.entry != source_entry || envelope.phase != phase {
                        return Err(error("placement fetch changes exact archived DATA subject"));
                    }
                    self.validate_placement_custody(&envelope)?;
                    Some(envelope)
                } else {
                    None
                };
                // Read-only archive fetch: no received request ever commits here.
                Ok(PlacementCustodyResponse {
                    stored_request_id: None,
                    envelope,
                })
            }
        }
    }
}
fn same_immutable_current_goal(
    a: &sinomo_dfs::DfsPlacementReconciliationJob,
    b: &sinomo_dfs::DfsPlacementReconciliationJob,
) -> bool {
    a.schema_version == b.schema_version
        && a.job_id == b.job_id
        && a.file_system_id == b.file_system_id
        && a.path == b.path
        && a.scope == b.scope
        && a.unavailable_node_ids == b.unavailable_node_ids
        && a.candidate_snapshot == b.candidate_snapshot
        && a.placement_snapshot == b.placement_snapshot
        && a.adaptive == b.adaptive
        && a.adaptive_budget == b.adaptive_budget
        && a.accepted_config == b.accepted_config
        && a.created_at_unix_nanos == b.created_at_unix_nanos
}
