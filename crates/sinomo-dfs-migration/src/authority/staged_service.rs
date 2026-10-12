//! Read-only current-stage receiver. Normal DFS roots stay untouched until
//! actual native joint activation authorizes the converter's final install.
use super::*;
use sinomo_core::network::ObjectRpcRequest;
use sinomo_core::service::{
    Service, ServiceBodyMode, ServiceContext, ServiceDisposition, ServiceFuture,
    ServiceRegistryBuilder, ServiceRoute,
};
use sinomo_dfs::{
    DfsMetadataJournalBatchResponse, DfsMetadataJournalBatchResult, DfsMetadataPrefixProbe,
    DfsMetadataShardHandoffCause, DfsMetadataShardMembershipHandoff,
    DfsMetadataSnapshotInstallCommand, DfsMetadataSnapshotInstallResponse,
    DfsMetadataSnapshotInstallResult,
};

pub(super) const SNAPSHOT_ROUTE: &str = "/dfs-migration/current-stage-snapshot-v7";
pub(super) const PREFIX_ROUTE: &str = "/dfs-migration/current-stage-prefix-v7";
pub(super) const BLUEPRINT_ROUTE: &str = "/dfs-migration/current-stage-activation-blueprint-v7";
pub(super) const SOURCE_FLOORS_ROUTE: &str = "/dfs-migration/source-floor-challenge-v7";
pub(super) const SOURCE_CHUNK_ROUTE: &str = "/dfs-migration/source-ancestry-chunk-v7";
pub(super) const CONTENT_STAGE_ROUTE: &str = "/dfs-migration/retained-content-stage-v7";
pub(super) const CONTENT_VERIFY_ROUTE: &str = "/dfs-migration/retained-content-verify-v7";
pub(super) const PLACEMENT_CUSTODY_ROUTE: &str = "/dfs-migration/placement-request-custody-v7";
pub(super) const NAMESPACE_JOB_ROUTE: &str = "/dfs-migration/current-namespace-job-data-v7";
const ROUTES: &[&str] = &[
    SNAPSHOT_ROUTE,
    PREFIX_ROUTE,
    BLUEPRINT_ROUTE,
    SOURCE_FLOORS_ROUTE,
    SOURCE_CHUNK_ROUTE,
    CONTENT_STAGE_ROUTE,
    CONTENT_VERIFY_ROUTE,
    NAMESPACE_JOB_ROUTE,
    PLACEMENT_CUSTODY_ROUTE,
];

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ContentStageRequest {
    pub(super) command: sinomo_dfs::DfsMessageCommand,
    pub(super) size_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StageSnapshot {
    pub(super) command: DfsMetadataSnapshotInstallCommand,
    #[serde(with = "super::plan_wire")]
    pub(super) plan: MetadataImportPlan,
    pub(super) ancestry_request_id: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ActivationBlueprint {
    #[serde(with = "super::plan_wire")]
    pub(super) plan: MetadataImportPlan,
    pub(super) request: KvTxnRequest,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct BlueprintStored {
    pub(super) request_id: KvRequestId,
    pub(super) handoff_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(super) struct SourceFloor {
    pub(super) shard_id: sinomo_dfs::DfsMetadataShardId,
    pub(super) journal_sequence: u64,
    pub(super) state_fingerprint: String,
    pub(super) durable_prefix_proof: Option<sinomo_dfs::DfsMetadataPrefixProof>,
}
impl From<&crate::metadata::ack_floor::LegacyAcknowledgement> for SourceFloor {
    fn from(value: &crate::metadata::ack_floor::LegacyAcknowledgement) -> Self {
        Self {
            shard_id: value.shard_id.clone(),
            journal_sequence: value.journal_sequence,
            state_fingerprint: value.state_fingerprint.clone(),
            durable_prefix_proof: value.durable_prefix_proof.clone(),
        }
    }
}
impl From<SourceFloor> for crate::metadata::ack_floor::LegacyAcknowledgement {
    fn from(value: SourceFloor) -> Self {
        Self {
            shard_id: value.shard_id,
            journal_sequence: value.journal_sequence,
            state_fingerprint: value.state_fingerprint,
            durable_prefix_proof: value.durable_prefix_proof,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SourceFloorChallenge {
    pub(super) request_id: String,
    #[serde(with = "super::plan_wire")]
    pub(super) plan: MetadataImportPlan,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SourceFloorResponse {
    pub(super) request_id: String,
    pub(super) handoff_id: String,
    pub(super) floors: Vec<SourceFloor>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SourceChunkTransfer {
    pub(super) request_id: String,
    pub(super) handoff_id: String,
    pub(super) ordinal: u64,
    pub(super) chunk: crate::metadata::source_ancestry::SourceAncestryChunk,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SourceChunkStored {
    pub(super) request_id: String,
    pub(super) ordinal: u64,
}

pub(super) struct SourceTransferVerifier {
    verifier: crate::metadata::source_ancestry::SourceAncestryVerifier,
    next_ordinal: u64,
}

pub(super) struct StagedImportService {
    pub(super) authority: Arc<NativeImportAuthority>,
    pub(super) data_owner: Arc<sinomo_dfs::runtime_host::DfsDataOwnerLock>,
    pub(super) component_context: sinomo_module::ComponentContext,
}
impl NativeImportAuthority {
    pub(super) fn receiver_root_for(
        &self,
        fs: &sinomo_dfs::DfsFileSystemId,
        shard: &str,
        handoff_id: &str,
    ) -> Result<PathBuf> {
        Ok(self.receiver_root.join(digest(&(fs, shard, handoff_id))?))
    }
    pub(super) fn staged_receiver_root(
        &self,
        handoff: &DfsMetadataShardMembershipHandoff,
    ) -> Result<PathBuf> {
        self.receiver_root_for(
            &handoff.retiring_shard.file_system_id,
            &handoff.retiring_shard.shard_id,
            &handoff.handoff_id,
        )
    }
    fn receive_context(
        &self,
        fs: &sinomo_dfs::DfsFileSystemId,
        shard: &sinomo_dfs::DfsMetadataShardId,
        epoch: u64,
        handoff_id: &str,
        source: &sinomo_core::node::NodeId,
    ) -> Result<DfsMetadataShardMembershipHandoff> {
        let keys = vec![
            sinomo_dfs::metadata_shard_key(fs, &shard.0),
            sinomo_dfs::metadata_shard_membership_handoff_key(fs, &shard.0),
        ];
        let (entries, _) = self.read_exact(&keys)?;
        let cfg: DfsMetadataShardConfig = entries[0]
            .entry
            .as_ref()
            .ok_or_else(|| error("staged receiver has no actual assignment"))?
            .decode()
            .map_err(error)?;
        let pending: DfsMetadataShardMembershipHandoff = entries[1]
            .entry
            .as_ref()
            .ok_or_else(|| error("staged receiver has no pending native import"))?
            .decode()
            .map_err(error)?;
        let local = sinomo_core::cluster::read_cluster_state_without_kv(
            &sinomo_core::network::ClusterPaths::new(&sinomo_core::local::SinomoDataDir::new(
                &self.data_root,
            )),
        )
        .map_err(error)?
        .local_node_id;
        let admitted = std::iter::once(&pending.retiring_shard.primary_node_id)
            .chain(&pending.retiring_shard.replica_node_ids)
            .chain(std::iter::once(&pending.desired_shard.primary_node_id))
            .chain(&pending.desired_shard.replica_node_ids)
            .any(|node| *node == local);
        if pending.handoff_id != handoff_id
            || cfg != pending.retiring_shard
            || cfg.epoch != epoch
            || cfg.primary_node_id != *source
            || !admitted
            || !matches!(
                pending.cause,
                DfsMetadataShardHandoffCause::StorageImport { .. }
            )
        {
            return Err(error(
                "current-stage request differs from its real native source/scope/handoff",
            ));
        }
        if let DfsMetadataShardHandoffCause::StorageImport { cut } = &pending.cause {
            let fence = pending
                .retiring_primary_fence
                .as_ref()
                .ok_or_else(|| error("current stage cannot acknowledge an unfenced source"))?;
            if fence.primary_node_id != pending.retiring_shard.primary_node_id
                || fence.journal_sequence != cut.source_journal_sequence
                || fence.state_fingerprint != cut.source_state_fingerprint
            {
                return Err(error(
                    "source fence differs from its actual captured import cut",
                ));
            }
        }
        Ok(pending)
    }
    /// Validate the SAME native source/scope/plan before accepting a large
    /// attachment. The receiver repeats these checks after the streaming wait
    /// and after its candidate effect; this head alone supplies no authority.
    fn validate_incoming_snapshot_head(
        &self,
        source: &sinomo_core::node::NodeId,
        envelope: &StageSnapshot,
    ) -> Result<()> {
        Self::validate_plan(&envelope.plan)?;
        let command = &envelope.command;
        if &command.source_node_id != source {
            return Err(error("staged image head differs from authenticated caller"));
        }
        let fs = command
            .file_system_id
            .as_ref()
            .ok_or_else(|| error("current stage has no filesystem stamp"))?;
        let epoch = command
            .authority_epoch
            .ok_or_else(|| error("current stage has no source epoch"))?;
        let handoff = command
            .primary_handoff_id
            .as_deref()
            .ok_or_else(|| error("current stage has no native handoff"))?;
        let pending = self.receive_context(fs, &command.shard_id, epoch, handoff, source)?;
        if pending.handoff_id != envelope.plan.expected_handoff.handoff_id
            || pending.cause != envelope.plan.expected_handoff.cause
            || pending.retiring_shard != envelope.plan.expected_handoff.retiring_shard
            || pending.desired_shard != envelope.plan.expected_handoff.desired_shard
        {
            return Err(error("staged image head differs from actual pending plan"));
        }
        let DfsMetadataShardHandoffCause::StorageImport { cut } = &pending.cause else {
            return Err(error("staged image has no storage import cause"));
        };
        if command.next_after_sequence != cut.target_prefix.journal_sequence
            || command.snapshot.materialized_through_sequence != command.next_after_sequence
            || command.snapshot.prefix_proof != cut.target_prefix
            || command.snapshot.semantic_state_fingerprint != cut.converted_image_fingerprint
        {
            return Err(error("staged image head differs from certified native cut"));
        }
        Ok(())
    }
    pub(super) fn receive_snapshot(
        &self,
        source: sinomo_core::node::NodeId,
        envelope: StageSnapshot,
    ) -> Result<DfsMetadataSnapshotInstallResponse> {
        Self::validate_plan(&envelope.plan)?;
        let command = envelope.command;
        if source != command.source_node_id {
            return Err(error(
                "snapshot payload differs from its authenticated source",
            ));
        }
        let fs = command
            .file_system_id
            .as_ref()
            .ok_or_else(|| error("current stage requires a filesystem stamp"))?;
        let epoch = command
            .authority_epoch
            .ok_or_else(|| error("current stage requires an actual epoch"))?;
        let handoff_id = command
            .primary_handoff_id
            .as_deref()
            .ok_or_else(|| error("current stage requires its exact import handoff identity"))?;
        let pending = self.receive_context(fs, &command.shard_id, epoch, handoff_id, &source)?;
        if pending.handoff_id != envelope.plan.expected_handoff.handoff_id
            || pending.cause != envelope.plan.expected_handoff.cause
            || pending.retiring_shard != envelope.plan.expected_handoff.retiring_shard
            || pending.desired_shard != envelope.plan.expected_handoff.desired_shard
        {
            return Err(error(
                "received source blueprint differs from the actual pending handoff",
            ));
        }
        let _source_fences = self.verify_staged_source_ancestry(
            fs,
            &command.shard_id,
            &envelope.plan,
            &envelope.ancestry_request_id,
        )?;
        let DfsMetadataShardHandoffCause::StorageImport { cut } = &pending.cause else {
            unreachable!()
        };
        if command.next_after_sequence != cut.target_prefix.journal_sequence
            || command.snapshot.prefix_proof != cut.target_prefix
            || command.snapshot.semantic_state_fingerprint != cut.converted_image_fingerprint
        {
            return Err(error(
                "snapshot is not the exact candidate certified by the native import intent",
            ));
        }
        let image = command
            .image
            .as_ref()
            .ok_or_else(|| error("current stage has no delivered native image"))?;
        if image.descriptor() != &command.snapshot {
            return Err(error(
                "current stage DATA resource differs from its exact head",
            ));
        }
        image.validate_file_system(fs)?;
        let root = self.staged_receiver_root(&pending)?;
        if root.join("records/tables/snapshot.sobj").exists() {
            let actual = sinomo_dfs::inspect_metadata_candidate(&root)?;
            if actual.prefix_proof != cut.target_prefix
                || actual.state_fingerprint != cut.converted_image_fingerprint
            {
                return Err(error(
                    "existing immutable receiver candidate belongs to another cut",
                ));
            }
        } else {
            let actual = sinomo_dfs::write_metadata_candidate_image(&root, image, fs)?;
            if actual.prefix_proof != cut.target_prefix
                || actual.state_fingerprint != cut.converted_image_fingerprint
            {
                return Err(error(
                    "actual current candidate checksum differs after durable write",
                ));
            }
        }
        Self::save_blueprint(&root, "native-import-plan.json", &envelope.plan)?;
        Self::save_blueprint(
            &root,
            "source-ancestry-request.json",
            &envelope.ancestry_request_id,
        )?;
        Self::save_blueprint(
            &root,
            &format!(
                "native-request-{}.json",
                envelope.plan.stage_request.request_id
            ),
            &envelope.plan.stage_request,
        )?;
        // Revalidate after the actual file effect; a superseding configuration
        // cannot receive an ACK for an obsolete import identity.
        self.receive_context(fs, &command.shard_id, epoch, handoff_id, &source)?;
        self.source_verifiers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.source_ancestry_root(handoff_id, &envelope.ancestry_request_id)?);
        Ok(DfsMetadataSnapshotInstallResponse {
            request_id: command.request_id,
            shard_id: command.shard_id,
            next_after_sequence: command.next_after_sequence,
            result: DfsMetadataSnapshotInstallResult::Installed {
                archived_existing: false,
            },
        })
    }
    pub(super) fn receive_prefix(
        &self,
        source: sinomo_core::node::NodeId,
        probe: DfsMetadataPrefixProbe,
    ) -> Result<DfsMetadataJournalBatchResponse> {
        let fs = probe
            .file_system_id
            .as_ref()
            .ok_or_else(|| error("prefix request has no filesystem scope"))?;
        let epoch = probe
            .authority_epoch
            .ok_or_else(|| error("prefix request has no epoch"))?;
        let id = probe
            .primary_handoff_id
            .clone()
            .ok_or_else(|| error("prefix request has no exact import identity"))?;
        let pending = self.receive_context(fs, &probe.shard_id, epoch, &id, &source)?;
        let DfsMetadataShardHandoffCause::StorageImport { cut } = &pending.cause else {
            unreachable!()
        };
        let root = self.staged_receiver_root(&pending)?;
        let plan: MetadataImportPlan = serde_json::from_slice(
            &std::fs::read(root.join("native-import-plan.json")).map_err(error)?,
        )
        .map_err(error)?;
        Self::validate_plan(&plan)?;
        if plan.expected_handoff.handoff_id != pending.handoff_id
            || plan.expected_handoff.cause != pending.cause
        {
            return Err(error("prefix probe has no exact retained source blueprint"));
        }
        let ancestry_request_id: String = serde_json::from_slice(
            &std::fs::read(root.join("source-ancestry-request.json")).map_err(error)?,
        )
        .map_err(error)?;
        let _fences =
            self.verify_staged_source_ancestry(fs, &probe.shard_id, &plan, &ancestry_request_id)?;
        let actual = sinomo_dfs::inspect_metadata_candidate(&root)?;
        if actual.prefix_proof != cut.target_prefix
            || actual.state_fingerprint != cut.converted_image_fingerprint
            || probe.journal_sequence != actual.prefix_proof.journal_sequence
            || probe.state_fingerprint != actual.state_fingerprint
        {
            return Err(error(
                "actual staged prefix does not match the submitted current probe",
            ));
        }
        Ok(DfsMetadataJournalBatchResponse {
            request_id: probe.barrier_id,
            shard_id: probe.shard_id,
            next_after_sequence: probe.journal_sequence,
            applied_entries: 0,
            verified_prefix_only: true,
            verified_authority_probe_id: Some(id),
            result: DfsMetadataJournalBatchResult::Applied,
        })
    }
    fn source_ancestry_root(&self, handoff_id: &str, request_id: &str) -> Result<PathBuf> {
        Ok(self
            .receiver_root
            .join("source-ancestry")
            .join(digest(&(handoff_id, request_id))?))
    }
    fn legacy_source_has_shard_clock(
        cut: &MetadataSourceCut,
        shard: &sinomo_dfs::DfsMetadataShardId,
    ) -> Result<bool> {
        if cut.source_format_version != Some(6) || cut.scope_shard != *shard {
            return Ok(false);
        }
        // Compare PRIMARY's captured source and image roles with each other,
        // never with this receiver's physical data directory. The same owned
        // converter constructs the image under its destination parent's hidden
        // conversion root. Separators may differ between PRIMARY and receiver.
        let Some(source) = cut.source_root.to_str().map(|path| path.replace('\\', "/")) else {
            return Ok(false);
        };
        let Some(stage) = cut.staged_root.to_str().map(|path| path.replace('\\', "/")) else {
            return Ok(false);
        };
        let Some((owner, source_shard)) = source.rsplit_once('/') else {
            return Ok(false);
        };
        if source_shard != shard.0 || owner.split('/').any(|part| part == "." || part == "..") {
            return Ok(false);
        }
        let Some(relative) = stage.strip_prefix(&format!("{owner}/.metadata-conversion/")) else {
            return Ok(false);
        };
        let key = blake3::hash(&serde_json::to_vec(&(&cut.source_root, shard)).map_err(error)?)
            .to_hex()
            .to_string();
        let mut capsules = cut
            .pending_operations
            .iter()
            .filter(|row| {
                matches!(
                    row["capsule_kind"].as_str(),
                    Some("placement-job-v0" | "placement-job-v1")
                )
            })
            .cloned()
            .collect::<Vec<_>>();
        // build_native_image adds precisely this non-authoring mapping marker
        // to original goal capsules. Restore only that declared false field in
        // comparison DATA; all original fields and order remain exact. The
        // converter's require_placement_goal_refs checks count and reservation
        // set, so unrelated same-kind pending jobs cannot join this goal.
        for capsule in &mut capsules {
            let Some(fields) = capsule.as_object_mut() else {
                return Ok(false);
            };
            if fields.remove("current_effects_authored") != Some(serde_json::Value::Bool(false)) {
                return Ok(false);
            }
        }
        let parts: Vec<_> = relative.split('/').collect();
        match parts.as_slice() {
            [actual_key, "target-format-9", "current-image"] => {
                Ok(*actual_key == key && capsules.is_empty())
            }
            [
                actual_key,
                "target-format-9",
                "data-goals",
                actual_goal,
                "current-image",
            ] => Ok(*actual_key == key
                && !capsules.is_empty()
                && *actual_goal
                    == crate::metadata::data_goal_digest(&cut.source_root, shard, &capsules)?),
            _ => Ok(false),
        }
    }
    fn current_source_root_for_plan(
        &self,
        root: &Path,
        fs: &sinomo_dfs::DfsFileSystemId,
        shard: &sinomo_dfs::DfsMetadataShardId,
        plan: &MetadataImportPlan,
    ) -> Result<bool> {
        let cut = &plan.source_cut;
        let retiring = &plan.expected_handoff.retiring_shard;
        // Format 6 also has per-shard native streams, but remains legacy
        // conversion DATA. A combined root serving another logical shard does
        // not qualify: its actual source-root role differs from that shard.
        let legacy_shard_clock = Self::legacy_source_has_shard_clock(cut, shard)?;
        if !crate::metadata::native_source_format(cut.source_format_version) && !legacy_shard_clock
        {
            return Ok(false);
        }
        if !matches!(
            plan.expected_handoff.cause,
            DfsMetadataShardHandoffCause::StorageImport { .. }
        ) || &retiring.file_system_id != fs
            || retiring.shard_id != shard.0
            || cut.scope_shard != *shard
            || !cut.file_system_ids.contains(fs)
            || !cut.projection_assignments.iter().any(|cfg| cfg == retiring)
        {
            return Err(error(
                "current source format is not bound to its actual retiring filesystem/shard/config",
            ));
        }
        let expected = self.metadata_root.join(&retiring.shard_id);
        if root.canonicalize().map_err(error)? != expected.canonicalize().map_err(error)? {
            return Ok(false);
        }
        // The source cut's PRIMARY header fact selects semantics, while this
        // actual LOCAL header must independently match that native generation.
        // Physical identity and prefix cuts may differ per member; exact floor
        // ancestry decides.
        let actual_header = if legacy_shard_clock {
            crate::metadata::metadata_root_at_format(root, 6)?
        } else {
            crate::metadata::native_source_root(root, cut.source_format_version)?
        };
        if !actual_header {
            return Err(error(
                "qualified native source lacks its actual local generation header",
            ));
        }
        let prefix = cut
            .source_exact_prefix
            .as_ref()
            .ok_or_else(|| error("current source cut lacks PRIMARY exact prefix"))?;
        if prefix.version != 2 || prefix.journal_sequence != cut.source_journal_sequence {
            return Err(error(
                "current source cut has no exact PRIMARY native prefix",
            ));
        }
        Ok(true)
    }
    fn capture_local_source_floors(
        &self,
        fs: &sinomo_dfs::DfsFileSystemId,
        shard: &sinomo_dfs::DfsMetadataShardId,
        plan: &MetadataImportPlan,
    ) -> Result<(
        Vec<Arc<std::fs::File>>,
        Vec<crate::metadata::ack_floor::LegacyAcknowledgement>,
    )> {
        let mut roots = BTreeSet::from([
            self.metadata_root.join(&shard.0),
            self.metadata_root.join("root"),
        ]);
        roots.retain(|root| root.exists());
        let mut fences = Vec::new();
        let mut floors = Vec::new();
        let requested_root = self.metadata_root.join(&shard.0);
        if requested_root.exists() {
            let fence = crate::metadata::retain_source_fence(&requested_root)?;
            if self.current_source_root_for_plan(&requested_root, fs, shard, plan)? {
                // Independent shard streams have different journal clocks.
                // ROOT fallback floors belong to ROOT's own original cut;
                // combined-layout sources still retain both roots below.
                roots = BTreeSet::from([requested_root]);
            }
            fences.push(fence);
        }
        for root in roots {
            let original_fence = crate::metadata::retain_source_fence(&root)?;
            let current_source = self.current_source_root_for_plan(&root, fs, shard, plan)?;
            if root == self.metadata_root.join(&shard.0)
                && !root.join("metadata-import-v7.json").is_file()
                && crate::metadata::current_root(&root)?
                && !current_source
            {
                return Err(error(
                    "native target without matching current source cut requires normal catch-up",
                ));
            }
            let Some(source) = self.original_source_root(&root, current_source)? else {
                continue;
            };
            let fence = if source == root {
                original_fence
            } else {
                crate::metadata::retain_source_fence(&source)?
            };
            let actual = crate::metadata::ack_floor::acknowledged_source_floors_for_root(
                &source,
                fs,
                current_source
                    .then_some(plan.source_cut.source_format_version)
                    .flatten(),
                shard,
            )?;
            if current_source
                && actual.iter().any(|floor| {
                    floor.shard_id != *shard
                        || floor
                            .durable_prefix_proof
                            .as_ref()
                            .is_some_and(|proof| proof.version != 2)
                })
            {
                return Err(error(
                    "qualified native source has foreign/retired/unanchored ACK clock",
                ));
            }
            // Same existing verifier/ledger extends each actual anchor to the
            // immutable PRIMARY source_exact_prefix; no height shortcut.
            if current_source {
                crate::metadata::source_ancestry::SourceAncestryVerifier::new(
                    &plan.source_cut,
                    &actual,
                )?;
            }
            floors.extend(actual);
            fences.push(fence);
        }
        let mut exact = BTreeMap::new();
        for floor in floors {
            exact.insert(serde_json::to_vec(&floor).map_err(error)?, floor);
        }
        Ok((fences, exact.into_values().collect()))
    }
    fn original_source_root(&self, root: &Path, current_source: bool) -> Result<Option<PathBuf>> {
        if current_source {
            // A qualified native cut uses this live root's exact clocks/data. An
            // older completed import marker is history, not a root redirect.
            return Ok(Some(root.to_owned()));
        }
        let marker = root.join("metadata-import-v7.json");
        if !marker.is_file() {
            // Current native ACK clocks are not original-source clocks.
            return Ok((!crate::metadata::current_root(root)?).then(|| root.to_owned()));
        }
        let marker: serde_json::Value =
            serde_json::from_slice(&std::fs::read(marker).map_err(error)?).map_err(error)?;
        if marker["format_version"].as_u64() != Some(7) {
            return Err(error("retained source marker has an unknown format"));
        }
        let source = marker["retained_source_root"]
            .as_str()
            .ok_or_else(|| error("current import lost its original source location"))?;
        let source = PathBuf::from(source).canonicalize().map_err(error)?;
        if !source.starts_with(&self.data_root) {
            return Err(error("original source alias escapes its actual data owner"));
        }
        let _fence = crate::metadata::retain_source_fence(&source)?;
        crate::metadata::validate_retained_source_identity(
            &source,
            marker["source_identity"]
                .as_str()
                .ok_or_else(|| error("original source marker lacks its LOCAL identity"))?,
            marker["source_snapshot_fingerprint"]
                .as_str()
                .ok_or_else(|| error("original source marker lacks its LOCAL snapshot cut"))?,
        )?;
        Ok(Some(source))
    }
    pub(super) fn receive_source_floor_challenge(
        &self,
        source: sinomo_core::node::NodeId,
        challenge: SourceFloorChallenge,
    ) -> Result<SourceFloorResponse> {
        Self::validate_plan(&challenge.plan)?;
        let h = &challenge.plan.expected_handoff;
        let actual = self.receive_context(
            &h.retiring_shard.file_system_id,
            &challenge.plan.source_cut.scope_shard,
            h.retiring_shard.epoch,
            &h.handoff_id,
            &source,
        )?;
        if actual.cause != h.cause || actual.desired_shard != h.desired_shard {
            return Err(error("source floor challenge was superseded"));
        }
        let (_fences, floors) = self.capture_local_source_floors(
            &h.retiring_shard.file_system_id,
            &challenge.plan.source_cut.scope_shard,
            &challenge.plan,
        )?;
        // This validates the exact actual anchors; no chain, height or state
        // claim from the sender can replace the receiver's retained floors.
        crate::metadata::source_ancestry::SourceAncestryVerifier::new(
            &challenge.plan.source_cut,
            &floors,
        )?;
        let response = SourceFloorResponse {
            request_id: challenge.request_id.clone(),
            handoff_id: h.handoff_id.clone(),
            floors: floors.iter().map(SourceFloor::from).collect(),
        };
        let root = self.source_ancestry_root(&h.handoff_id, &challenge.request_id)?;
        std::fs::create_dir_all(&root).map_err(error)?;
        Self::save_blueprint(&root, "challenge.json", &challenge)?;
        Self::save_blueprint(&root, "floors.json", &response)?;
        Ok(response)
    }
    pub(super) fn receive_source_ancestry_chunk(
        &self,
        source: sinomo_core::node::NodeId,
        transfer: SourceChunkTransfer,
    ) -> Result<SourceChunkStored> {
        if transfer.chunk.bytes.len()
            > crate::metadata::source_ancestry::MAX_SOURCE_ANCESTRY_CHUNK_BYTES
        {
            return Err(error("source ancestry chunk exceeds bounded transfer size"));
        }
        // A request ID alone is not a scope or permission. Resolve the saved
        // challenge and require its exact authenticated current native source.
        let root = self.source_ancestry_root(&transfer.handoff_id, &transfer.request_id)?;
        let challenge: SourceFloorChallenge =
            serde_json::from_slice(&std::fs::read(root.join("challenge.json")).map_err(error)?)
                .map_err(error)?;
        if challenge.request_id != transfer.request_id
            || challenge.plan.expected_handoff.handoff_id != transfer.handoff_id
        {
            return Err(error(
                "source ancestry chunk changes its retained challenge identity",
            ));
        }
        let h = &challenge.plan.expected_handoff;
        self.receive_context(
            &h.retiring_shard.file_system_id,
            &challenge.plan.source_cut.scope_shard,
            h.retiring_shard.epoch,
            &h.handoff_id,
            &source,
        )?;
        let floors: SourceFloorResponse =
            serde_json::from_slice(&std::fs::read(root.join("floors.json")).map_err(error)?)
                .map_err(error)?;
        let progress = {
            let mut registry = self
                .source_verifiers
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if let Some(progress) = registry.get(&root) {
                Arc::clone(progress)
            } else {
                let actual = floors
                    .floors
                    .into_iter()
                    .map(Into::into)
                    .collect::<Vec<_>>();
                let mut verifier = crate::metadata::source_ancestry::SourceAncestryVerifier::new(
                    &challenge.plan.source_cut,
                    &actual,
                )?;
                let next_ordinal = Self::replay_source_chunks(&root, &mut verifier, None)?;
                let progress = Arc::new(Mutex::new(SourceTransferVerifier {
                    verifier,
                    next_ordinal,
                }));
                registry.insert(root.clone(), Arc::clone(&progress));
                progress
            }
        };
        let mut progress = progress.lock().unwrap_or_else(|e| e.into_inner());
        let path = root.join(format!("{:020}.sobj", transfer.ordinal));
        let bytes = sinomo_core::object::serde_ext::to_bytes(
            &transfer.chunk,
            1,
            sinomo_core::object::LengthMode::U32,
        )
        .map_err(error)?;
        if transfer.ordinal < progress.next_ordinal {
            if std::fs::read(&path).map_err(error)? != bytes {
                return Err(error(
                    "source ancestry retry changes an immutable durable chunk",
                ));
            }
        } else {
            if transfer.ordinal != progress.next_ordinal {
                return Err(error("source ancestry chunk cursor has a gap"));
            }
            let effect = (|| -> Result<()> {
                progress.verifier.apply(&transfer.chunk)?;
                if path.exists() {
                    if std::fs::read(&path).map_err(error)? != bytes {
                        return Err(error("source ancestry retry changes an immutable chunk"));
                    }
                } else {
                    sinomo_core::storage::durable_file::write_file_atomic(
                        &path,
                        &bytes,
                        sinomo_core::storage::durable_file::DurableAtomicFileOptions::new(
                            "import", "tmp",
                        )
                        .sync_all(),
                    )
                    .map_err(error)?;
                }
                progress.next_ordinal = progress
                    .next_ordinal
                    .checked_add(1)
                    .ok_or_else(|| error("source ancestry chunk ordinal overflow"))?;
                Ok(())
            })();
            if let Err(reason) = effect {
                // A failed or uncertain write invalidates only the resident
                // cursor; the durable exact ledger is replayed on recovery.
                drop(progress);
                self.source_verifiers
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .remove(&root);
                return Err(reason);
            }
        }
        Ok(SourceChunkStored {
            request_id: transfer.request_id,
            ordinal: transfer.ordinal,
        })
    }
    fn replay_source_chunks(
        root: &Path,
        verifier: &mut crate::metadata::source_ancestry::SourceAncestryVerifier,
        through: Option<u64>,
    ) -> Result<u64> {
        let mut ordinal = 0u64;
        loop {
            if through.is_some_and(|end| ordinal == end) {
                break;
            }
            let path = root.join(format!("{ordinal:020}.sobj"));
            if !path.is_file() {
                if through.is_some() {
                    return Err(error("source ancestry durable chunk cursor has a gap"));
                }
                break;
            }
            let limit = crate::metadata::source_ancestry::MAX_SOURCE_ANCESTRY_CHUNK_BYTES as u64
                + 64 * 1024;
            let file = std::fs::File::open(&path).map_err(error)?;
            if file.metadata().map_err(error)?.len() > limit {
                return Err(error(
                    "durable source ancestry chunk exceeds its bounded encoding",
                ));
            }
            let mut bytes = Vec::new();
            std::io::Read::read_to_end(&mut std::io::Read::take(file, limit + 1), &mut bytes)
                .map_err(error)?;
            if bytes.len() as u64 > limit {
                return Err(error(
                    "durable source ancestry chunk grew beyond its bounded encoding",
                ));
            }
            let chunk = sinomo_core::object::serde_ext::from_bytes(&bytes).map_err(error)?;
            verifier.apply(&chunk)?;
            ordinal = ordinal
                .checked_add(1)
                .ok_or_else(|| error("source ancestry chunk ordinal overflow"))?;
        }
        Ok(ordinal)
    }
    fn verify_staged_source_ancestry(
        &self,
        fs: &sinomo_dfs::DfsFileSystemId,
        shard: &sinomo_dfs::DfsMetadataShardId,
        plan: &MetadataImportPlan,
        request_id: &str,
    ) -> Result<Vec<Arc<std::fs::File>>> {
        let root = self.source_ancestry_root(&plan.expected_handoff.handoff_id, request_id)?;
        let challenge: SourceFloorChallenge =
            serde_json::from_slice(&std::fs::read(root.join("challenge.json")).map_err(error)?)
                .map_err(error)?;
        if digest(&challenge.plan)? != digest(plan)? {
            return Err(error("source ancestry belongs to another immutable plan"));
        }
        let (fences, floors) = self.capture_local_source_floors(fs, shard, plan)?;
        let response: SourceFloorResponse =
            serde_json::from_slice(&std::fs::read(root.join("floors.json")).map_err(error)?)
                .map_err(error)?;
        if response.floors != floors.iter().map(SourceFloor::from).collect::<Vec<_>>() {
            return Err(error(
                "actual source ACK floors changed after the ancestry challenge",
            ));
        }
        let mut verifier = crate::metadata::source_ancestry::SourceAncestryVerifier::new(
            &plan.source_cut,
            &floors,
        )?;
        Self::replay_source_chunks(&root, &mut verifier, None)?;
        verifier.finish()?;
        Ok(fences)
    }
    pub(super) fn verify_recovered_source_ancestry(
        &self,
        plan: &MetadataImportPlan,
        provenance_plan_root: &Path,
        actual_source_root: &Path,
    ) -> Result<Vec<Arc<std::fs::File>>> {
        let request_id: String = serde_json::from_slice(
            &std::fs::read(provenance_plan_root.join("source-ancestry-request.json"))
                .map_err(error)?,
        )
        .map_err(error)?;
        let root = self.source_ancestry_root(&plan.expected_handoff.handoff_id, &request_id)?;
        let challenge: SourceFloorChallenge =
            serde_json::from_slice(&std::fs::read(root.join("challenge.json")).map_err(error)?)
                .map_err(error)?;
        if digest(&challenge.plan)? != digest(plan)? {
            return Err(error(
                "recovered ancestry changes the immutable native plan",
            ));
        }
        let source_fence = crate::metadata::retain_source_fence(actual_source_root)?;
        let actual = crate::metadata::ack_floor::acknowledged_source_floors_for_root(
            actual_source_root,
            &plan.expected_handoff.retiring_shard.file_system_id,
            plan.source_cut.source_format_version,
            &plan.source_cut.scope_shard,
        )?;
        if crate::metadata::native_source_format(plan.source_cut.source_format_version) {
            if !crate::metadata::native_source_root(
                actual_source_root,
                plan.source_cut.source_format_version,
            )? || actual.iter().any(|floor| {
                floor.shard_id != plan.source_cut.scope_shard
                    || floor
                        .durable_prefix_proof
                        .as_ref()
                        .is_some_and(|proof| proof.version != 2)
            }) {
                return Err(error(
                    "recovered native source has foreign/retired/unanchored ACK clocks",
                ));
            }
        }
        let response: SourceFloorResponse =
            serde_json::from_slice(&std::fs::read(root.join("floors.json")).map_err(error)?)
                .map_err(error)?;
        let challenged = response
            .floors
            .into_iter()
            .map(Into::into)
            .collect::<Vec<crate::metadata::ack_floor::LegacyAcknowledgement>>();
        if actual.iter().any(|floor| !challenged.contains(floor)) {
            return Err(error(
                "recovered original source has an unverified ACK floor",
            ));
        }
        let mut verifier = crate::metadata::source_ancestry::SourceAncestryVerifier::new(
            &plan.source_cut,
            &challenged,
        )?;
        Self::replay_source_chunks(&root, &mut verifier, None)?;
        verifier.finish()?;
        Ok(vec![source_fence])
    }
    pub(super) fn validate_local_source_floors(
        &self,
        fs: &sinomo_dfs::DfsFileSystemId,
        shard: &sinomo_dfs::DfsMetadataShardId,
        source: &MetadataSourceCut,
    ) -> Result<Vec<Arc<std::fs::File>>> {
        // Certificate paths use the already persisted exact source challenge;
        // they never grant an ACK from a larger source/target clock.
        let keys = vec![sinomo_dfs::metadata_shard_membership_handoff_proof_key(
            fs, &shard.0,
        )];
        let (entries, _) = self.read_exact(&keys)?;
        let proof: sinomo_dfs::DfsMetadataShardMembershipHandoffProof = entries[0]
            .entry
            .as_ref()
            .ok_or_else(|| error("source ancestry has no retained native import proof"))?
            .decode()
            .map_err(error)?;
        let root = self.receiver_root_for(fs, &shard.0, &proof.handoff_id)?;
        let plan: MetadataImportPlan = serde_json::from_slice(
            &std::fs::read(root.join("native-import-plan.json")).map_err(error)?,
        )
        .map_err(error)?;
        if digest(&plan.source_cut)? != digest(source)? {
            return Err(error("source floor verification changed its original cut"));
        }
        let request_id: String = serde_json::from_slice(
            &std::fs::read(root.join("source-ancestry-request.json")).map_err(error)?,
        )
        .map_err(error)?;
        self.verify_staged_source_ancestry(fs, shard, &plan, &request_id)
    }
    pub(super) fn receive_activation_blueprint(
        &self,
        source: sinomo_core::node::NodeId,
        envelope: ActivationBlueprint,
    ) -> Result<BlueprintStored> {
        Self::validate_plan(&envelope.plan)?;
        let h = &envelope.plan.expected_handoff;
        let proof = Self::activation_proof(&envelope.request)?
            .ok_or_else(|| error("activation blueprint has no exact joint proof effect"))?;
        self.validate_activation_request(&envelope.request, &envelope.plan, &proof)?;
        let pending = self.receive_context(
            &h.retiring_shard.file_system_id,
            &envelope.plan.source_cut.scope_shard,
            h.retiring_shard.epoch,
            &h.handoff_id,
            &source,
        )?;
        if pending.cause != h.cause || pending.desired_shard != h.desired_shard {
            return Err(error("activation blueprint was superseded"));
        }
        let root = self.staged_receiver_root(&pending)?;
        let ancestry_request_id: String = serde_json::from_slice(
            &std::fs::read(root.join("source-ancestry-request.json")).map_err(error)?,
        )
        .map_err(error)?;
        let _fences = self.verify_staged_source_ancestry(
            &h.retiring_shard.file_system_id,
            &envelope.plan.source_cut.scope_shard,
            &envelope.plan,
            &ancestry_request_id,
        )?;
        let retained: MetadataImportPlan = serde_json::from_slice(
            &std::fs::read(root.join("native-import-plan.json")).map_err(error)?,
        )
        .map_err(error)?;
        if digest(&retained)? != digest(&envelope.plan)? {
            return Err(error(
                "activation request changes the acknowledged source blueprint",
            ));
        }
        let actual = sinomo_dfs::inspect_metadata_candidate(&root)?;
        if actual.prefix_proof != envelope.plan.target_exact_prefix
            || actual.state_fingerprint != envelope.plan.current_image_fingerprint
        {
            return Err(error(
                "activation request has no exact durable staged candidate",
            ));
        }
        Self::save_blueprint(&root, "native-activation-request.json", &envelope.request)?;
        Self::save_blueprint(
            &root,
            &format!("native-request-{}.json", envelope.request.request_id),
            &envelope.request,
        )?;
        Ok(BlueprintStored {
            request_id: envelope.request.request_id,
            handoff_id: h.handoff_id.clone(),
        })
    }
}
impl Service for StagedImportService {
    fn name(&self) -> &'static str {
        "dfs-migration"
    }
    fn routes(&self) -> &'static [&'static str] {
        ROUTES
    }
    fn route_categories(&self) -> Vec<ServiceRoute> {
        ROUTES
            .iter()
            .map(|route| {
                let content_stage = *route == CONTENT_STAGE_ROUTE;
                let snapshot_stage = *route == SNAPSHOT_ROUTE;
                let content_verify = *route == CONTENT_VERIFY_ROUTE;
                let route = ServiceRoute::new(*route, "dfs-migration")
                    .with_auth_scope("cluster-member")
                    .require_capability(if content_stage || content_verify {
                        "dfs.content.replicate"
                    } else {
                        "dfs.metadata.replicate"
                    });
                if content_stage || snapshot_stage {
                    route.with_request_body_mode(ServiceBodyMode::Optional)
                } else if content_verify {
                    route.with_response_body_mode(ServiceBodyMode::Optional)
                } else {
                    route
                }
            })
            .collect()
    }
    fn handle(&self, mut context: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        Box::pin(async move {
            let _owner = Arc::clone(&self.data_owner);
            let signer = context.signer.cluster_identity().ok_or_else(|| {
                ServiceError::Service(
                    "staged metadata requires an authenticated cluster signer".into(),
                )
            })?;
            let source = signer
                .0
                .parse::<sinomo_core::node::NodeId>()
                .map_err(|e| ServiceError::Service(e.to_string()))?;
            match request.route.as_str() {
                PLACEMENT_CUSTODY_ROUTE => {
                    let data = context
                        .decode_request_body::<super::placement_work::PlacementCustodyRequest>(
                            &request,
                        )
                        .await?;
                    let response = tokio::task::block_in_place(|| {
                        self.authority.receive_placement_custody(source, data)
                    })
                    .map_err(|error| ServiceError::Service(error.to_string()))?;
                    context.respond_value(PLACEMENT_CUSTODY_ROUTE, response)?;
                }
                NAMESPACE_JOB_ROUTE => {
                    let work = context
                        .decode_request_body::<super::namespace_work::JobDataRequest>(&request)
                        .await?;
                    let installed = tokio::task::block_in_place(|| {
                        self.authority.receive_current_job(&source, work)
                    })
                    .map_err(|error| ServiceError::Service(error.to_string()))?;
                    context.respond_value(NAMESPACE_JOB_ROUTE, installed)?;
                }
                CONTENT_VERIFY_ROUTE => {
                    let wire = context
                        .decode_request_body::<super::content_work::ContentVerificationWire>(
                            &request,
                        )
                        .await?;
                    let verification = match self
                        .authority
                        .resolve_content_verification(&source, wire)
                        .map_err(|e| ServiceError::Service(e.to_string()))?
                    {
                        super::content_work::ResolvedVerification::Request(request) => request,
                        super::content_work::ResolvedVerification::ContextMissing(response) => {
                            context.respond_value(CONTENT_VERIFY_ROUTE, response)?;
                            return Ok(ServiceDisposition::Continue);
                        }
                    };
                    tokio::task::block_in_place(|| {
                        self.authority
                            .authorize_content_verification(&source, &verification)
                    })
                    .map_err(|e| ServiceError::Service(e.to_string()))?;
                    let receiver = sinomo_dfs::runtime_host::DfsStorageImportContentReceiver::for_authenticated_request(
                        &self.component_context, &context,
                    ).map_err(|e| ServiceError::Service(e.to_string()))?;
                    tokio::task::block_in_place(|| {
                        super::content_work::verify_local_retained_content(
                            receiver.content_store(),
                            &verification.version,
                            &verification.locator,
                        )
                    })
                    .map_err(|e| ServiceError::Service(e.to_string()))?;
                    // Revalidate the exact current native caller after the
                    // potentially large immutable byte verification.
                    tokio::task::block_in_place(|| {
                        self.authority
                            .authorize_content_verification(&source, &verification)
                    })
                    .map_err(|e| ServiceError::Service(e.to_string()))?;
                    let plan_context = self
                        .authority
                        .acknowledge_content_verification_plan(&source, &verification)
                        .map_err(|e| ServiceError::Service(e.to_string()))?;
                    let verified = super::content_work::ContentVerified {
                        request_id: verification.request_id,
                        holder_node_id: self
                            .authority
                            .local_node()
                            .map_err(|e| ServiceError::Service(e.to_string()))?,
                        file_system_id: verification.version.file_system_id.clone(),
                        object_id: verification.version.object_id.clone(),
                        version: verification.version.version.clone(),
                        content_hash: verification.version.content_hash.clone().ok_or_else(
                            || ServiceError::Service("verified version lacks hash".into()),
                        )?,
                        size_bytes: verification.version.size_bytes.ok_or_else(|| {
                            ServiceError::Service("verified version lacks length".into())
                        })?,
                    };
                    let verified = super::content_work::ContentVerificationResult::Verified {
                        verification: verified,
                        plan_context,
                    };
                    if verification.export_bytes {
                        let (_, reader) = receiver
                            .content_store()
                            .open_object_version_stream(
                                verification.version.object_id.as_str(),
                                verification.version.version.as_str(),
                            )
                            .map_err(|e| ServiceError::Service(e.to_string()))?;
                        let length = verification.version.size_bytes.ok_or_else(|| {
                            ServiceError::Service("verified version lacks length".into())
                        })?;
                        let response = sinomo_core::network::ObjectRpcResponse::encode_body(
                            CONTENT_VERIFY_ROUTE,
                            &verified,
                        )?;
                        context.respond_with_body(
                            response,
                            sinomo_core::network::ObjectRpcBodySource::from_reader(
                                super::content_work::OwnedContentReader {
                                    reader: Box::new(reader),
                                    owner: receiver,
                                },
                                Some(length),
                            ),
                        )?;
                    } else {
                        context.respond_value(CONTENT_VERIFY_ROUTE, verified)?;
                    }
                }
                CONTENT_STAGE_ROUTE => {
                    let stage = context
                        .decode_request_body::<ContentStageRequest>(&request)
                        .await?;
                    let mut receiver = sinomo_dfs::runtime_host::DfsStorageImportContentReceiver::for_authenticated_request(
                        &self.component_context,
                        &context,
                    )
                    .map_err(|e| ServiceError::Service(e.to_string()))?;
                    let sinomo_dfs::DfsNetworkMessage::StageRetainedBlob(command) =
                        &stage.command.routed_message.message
                    else {
                        return Err(ServiceError::Service(
                            "import content route accepts only retained stages".into(),
                        ));
                    };
                    let mut input = receiver
                        .content_store()
                        .begin_quarantined_blob_ingest(
                            stage.size_bytes,
                            Some(&command.content_hash),
                        )
                        .map_err(|e| ServiceError::Service(e.to_string()))?;
                    let mut body = context.take_request_body();
                    while let Some(chunk) = body
                        .next_chunk()
                        .await
                        .map_err(|e| ServiceError::Service(e.to_string()))?
                    {
                        // The current owned ingest checks exact length/hash at
                        // EOF. No metadata row or healthy ACK precedes it.
                        super::block_native(&self.authority.runtime, async {
                            input.write_chunk(chunk.as_ref())
                        })
                        .map_err(|e| ServiceError::Service(e.to_string()))?;
                    }
                    let blob = super::block_native(&self.authority.runtime, async {
                        input.finish_quarantined()
                    })
                    .map_err(|e| ServiceError::Service(e.to_string()))?;
                    let result = super::block_native(&self.authority.runtime, async {
                        receiver.process(stage.command, Some(blob))
                    })
                    .map_err(|e| ServiceError::Service(e.to_string()))?;
                    context.respond_value(CONTENT_STAGE_ROUTE, result.response)?;
                }
                SNAPSHOT_ROUTE => {
                    let mut command = context
                        .decode_request_body::<StageSnapshot>(&request)
                        .await?;
                    tokio::task::block_in_place(|| {
                        self.authority
                            .validate_incoming_snapshot_head(&source, &command)
                    })
                    .map_err(|e| ServiceError::Service(e.to_string()))?;
                    if !context.has_request_attachment() {
                        return Err(ServiceError::Service(
                            "current stage requires its native record image attachment".into(),
                        ));
                    }
                    command.command.image = Some(
                        sinomo_dfs::receive_metadata_candidate_image(
                            self.authority.data_root.clone(),
                            command.command.snapshot.clone(),
                            context.take_request_body(),
                            Arc::clone(&_owner),
                        )
                        .await
                        .map_err(|e| ServiceError::Service(e.to_string()))?,
                    );
                    // Blocking storage effects stay inside this poll and retain
                    // their real owner even when the external waiter times out.
                    let result = tokio::task::block_in_place(|| {
                        self.authority.receive_snapshot(source, command)
                    })
                    .map_err(|e| ServiceError::Service(e.to_string()))?;
                    context.respond_value(SNAPSHOT_ROUTE, result)?;
                }
                PREFIX_ROUTE => {
                    let probe = context
                        .decode_request_body::<DfsMetadataPrefixProbe>(&request)
                        .await?;
                    let result = tokio::task::block_in_place(|| {
                        self.authority.receive_prefix(source, probe)
                    })
                    .map_err(|e| ServiceError::Service(e.to_string()))?;
                    context.respond_value(PREFIX_ROUTE, result)?;
                }
                BLUEPRINT_ROUTE => {
                    let envelope = context
                        .decode_request_body::<ActivationBlueprint>(&request)
                        .await?;
                    let result = tokio::task::block_in_place(|| {
                        self.authority
                            .receive_activation_blueprint(source, envelope)
                    })
                    .map_err(|e| ServiceError::Service(e.to_string()))?;
                    context.respond_value(BLUEPRINT_ROUTE, result)?;
                }
                SOURCE_FLOORS_ROUTE => {
                    let challenge = context
                        .decode_request_body::<SourceFloorChallenge>(&request)
                        .await?;
                    let result = tokio::task::block_in_place(|| {
                        self.authority
                            .receive_source_floor_challenge(source, challenge)
                    })
                    .map_err(|e| ServiceError::Service(e.to_string()))?;
                    context.respond_value(SOURCE_FLOORS_ROUTE, result)?;
                }
                SOURCE_CHUNK_ROUTE => {
                    let transfer = context
                        .decode_request_body::<SourceChunkTransfer>(&request)
                        .await?;
                    let result = tokio::task::block_in_place(|| {
                        self.authority
                            .receive_source_ancestry_chunk(source, transfer)
                    })
                    .map_err(|e| ServiceError::Service(e.to_string()))?;
                    context.respond_value(SOURCE_CHUNK_ROUTE, result)?;
                }
                _ => return Err(ServiceError::UnknownRoute(request.route)),
            }
            Ok(ServiceDisposition::Continue)
        })
    }
}

pub(super) fn factory()
-> std::result::Result<sinomo_module::StaticComponentFactory, sinomo_module::ComponentError> {
    sinomo_module::StaticComponentFactory::new(
        sinomo_module::ComponentDescriptor::new("dfs-migration", env!("CARGO_PKG_VERSION"))
            .require(sinomo_dfs::dfs_core_component_requirement()),
        move |context, scope| {
            if !super::source_effect_host(context) {
                return Ok(());
            }
            if context.cluster_access().is_none()
                && sinomo_dfs::runtime_host::registered_component_cluster(context)
                    .map_err(|e| sinomo_module::ComponentError::component("dfs-migration", e))?
                    .is_none()
            {
                // An explicitly local source provider owns its local effects;
                // there is no authenticated native peer endpoint to mount.
                return Ok(());
            }
            let owner = sinomo_dfs::runtime_host::retain_component_data_owner(context)
                .map_err(|e| sinomo_module::ComponentError::component("dfs-migration", e))?;
            let data_root = context.data_dir_root().canonicalize().map_err(|error| {
                sinomo_module::ComponentError::component("dfs-migration", error)
            })?;
            let lifecycle =
                sinomo_dfs::dfs_storage_preparation_lifecycle(&data_root).ok_or_else(|| {
                    sinomo_module::ComponentError::component(
                        "dfs-migration",
                        "staged service has no actual current provider lifecycle",
                    )
                })?;
            let runtime = super::shared_runtime(context, lifecycle)?;
            scope.set_services(
                ServiceRegistryBuilder::default()
                    .register(StagedImportService {
                        authority: Arc::clone(&runtime.authority),
                        data_owner: owner,
                        component_context: context.clone(),
                    })
                    .build(),
            );
            Ok(())
        },
    )
}

#[cfg(test)]
mod ancestry_ledger_tests {
    use super::*;

    fn empty_cut(root: &Path) -> MetadataSourceCut {
        // No ACKs, cfg, native receipt or permission is created by this
        // filesystem-boundary fixture. The verifier tests only durable framing.
        MetadataSourceCut {
            source_root: root.to_owned(),
            source_identity: "ledger-fixture".into(),
            scope_shard: sinomo_dfs::DfsMetadataShardId::new("root"),
            file_system_ids: Vec::new(),
            source_snapshot_fingerprint: "snapshot-fixture".into(),
            source_format_version: None,
            source_exact_prefix: None,
            converted_image_fingerprint: "projection-fixture".into(),
            current_image_fingerprint: "image-fixture".into(),
            target_exact_prefix: sinomo_dfs::DfsMetadataPrefixProof {
                version: 2,
                journal_sequence: 0,
                digest: "target-not-used-by-source-verifier".into(),
            },
            source_journal_sequence: 0,
            source_state_fingerprint: "source-fixture".into(),
            projection_assignments: Vec::new(),
            staged_root: root.join("current-image"),
            pending_operations: Vec::new(),
            original_handoff: None,
        }
    }
    #[test]
    fn source_ancestry_durable_cursor_rejects_missing_chunk() {
        let root = sinomo_testdir::TestDir::new("source-ancestry-cursor-gap");
        let mut verifier = crate::metadata::source_ancestry::SourceAncestryVerifier::new(
            &empty_cut(root.path()),
            &[],
        )
        .unwrap();
        assert!(
            NativeImportAuthority::replay_source_chunks(root.path(), &mut verifier, Some(1))
                .is_err()
        );
    }
    #[test]
    fn source_ancestry_durable_cursor_rejects_oversized_encoding_before_read() {
        let root = sinomo_testdir::TestDir::new("source-ancestry-cursor-oversized");
        let file = std::fs::File::create(root.join("00000000000000000000.sobj")).unwrap();
        file.set_len(
            crate::metadata::source_ancestry::MAX_SOURCE_ANCESTRY_CHUNK_BYTES as u64
                + 64 * 1024
                + 1,
        )
        .unwrap();
        let mut verifier = crate::metadata::source_ancestry::SourceAncestryVerifier::new(
            &empty_cut(root.path()),
            &[],
        )
        .unwrap();
        assert!(
            NativeImportAuthority::replay_source_chunks(root.path(), &mut verifier, None).is_err()
        );
    }
    #[test]
    fn source_ancestry_durable_cursor_rejects_truncated_serialized_chunk() {
        let root = sinomo_testdir::TestDir::new("source-ancestry-cursor-truncated");
        std::fs::write(root.join("00000000000000000000.sobj"), [0xff, 0x03, 0x01]).unwrap();
        let mut verifier = crate::metadata::source_ancestry::SourceAncestryVerifier::new(
            &empty_cut(root.path()),
            &[],
        )
        .unwrap();
        assert!(
            NativeImportAuthority::replay_source_chunks(root.path(), &mut verifier, None).is_err()
        );
    }
    #[test]
    fn legacy_shard_stream_clock_keeps_root_fallback_for_combined_sources() {
        let root = sinomo_testdir::TestDir::new("source-clock-layout");
        let primary_metadata = root.path().join("primary-owner/dfs/metadata");
        let mut cut = empty_cut(&primary_metadata.join("users"));
        let key = blake3::hash(
            &serde_json::to_vec(&(
                &cut.source_root,
                &sinomo_dfs::DfsMetadataShardId::new("users"),
            ))
            .unwrap(),
        )
        .to_hex()
        .to_string();
        cut.staged_root = primary_metadata
            .join(".metadata-conversion")
            .join(&key)
            .join("target-format-9/current-image");
        cut.scope_shard = sinomo_dfs::DfsMetadataShardId::new("users");
        cut.source_format_version = Some(6);
        assert!(
            NativeImportAuthority::legacy_source_has_shard_clock(&cut, &cut.scope_shard).unwrap()
        );
        // Hash-only DATA controls do not create a placement reservation,
        // RPC, receipt or authority. The common pipeline validates those bodies.
        cut.pending_operations =
            vec![serde_json::json!({"capsule_kind":"placement-job-v1","opaque-DATA":"unchanged"})];
        let goal = crate::metadata::data_goal_digest(
            &cut.source_root,
            &cut.scope_shard,
            &cut.pending_operations,
        )
        .unwrap();
        cut.pending_operations[0]["current_effects_authored"] = serde_json::json!(false);
        cut.staged_root = primary_metadata
            .join(".metadata-conversion")
            .join(&key)
            .join("target-format-9/data-goals")
            .join(&goal)
            .join("current-image");
        assert!(
            NativeImportAuthority::legacy_source_has_shard_clock(&cut, &cut.scope_shard).unwrap()
        );
        let valid = cut.clone();
        for change in 0..8 {
            let mut changed = valid.clone();
            match change {
                0 => changed.pending_operations[0]["opaque-DATA"] = serde_json::json!("changed"),
                1 => {
                    changed.staged_root = primary_metadata
                        .join(".metadata-conversion")
                        .join("0".repeat(64))
                        .join("target-format-9/data-goals")
                        .join(&goal)
                        .join("current-image")
                }
                2 => {
                    changed.staged_root = primary_metadata
                        .join(".metadata-conversion")
                        .join(&key)
                        .join("target-format-9/data-goals")
                        .join("0".repeat(64))
                        .join("current-image")
                }
                3 => {
                    changed.staged_root = primary_metadata
                        .join(".metadata-conversion")
                        .join(&key)
                        .join("target-format-9/data-goals")
                        .join(&goal)
                        .join("../current-image")
                }
                4 => {
                    changed.staged_root = primary_metadata
                        .join(".metadata-conversion")
                        .join(&key)
                        .join("target-format-9/data-goals")
                        .join(&goal)
                        .join(".metadata-conversion/current-image")
                }
                5 => {
                    changed.pending_operations[0]["current_effects_authored"] =
                        serde_json::json!(true)
                }
                6 => {
                    changed.pending_operations[0]["current_effects_authored"] =
                        serde_json::Value::Null
                }
                _ => {
                    changed.pending_operations[0]
                        .as_object_mut()
                        .unwrap()
                        .remove("current_effects_authored");
                }
            }
            assert!(
                !NativeImportAuthority::legacy_source_has_shard_clock(
                    &changed,
                    &changed.scope_shard
                )
                .unwrap()
            );
        }
        cut.pending_operations.clear();
        cut.staged_root = primary_metadata
            .join(".metadata-conversion")
            .join(&key)
            .join("target-format-9/current-image");
        // A combined ROOT source for USERS keeps ROOT floors and source fences.
        cut.source_root = primary_metadata.join("root");
        assert!(
            !NativeImportAuthority::legacy_source_has_shard_clock(&cut, &cut.scope_shard).unwrap()
        );
        cut.scope_shard = sinomo_dfs::DfsMetadataShardId::new("root");
        let key = blake3::hash(&serde_json::to_vec(&(&cut.source_root, &cut.scope_shard)).unwrap())
            .to_hex()
            .to_string();
        cut.staged_root = primary_metadata
            .join(".metadata-conversion")
            .join(key)
            .join("target-format-9/current-image");
        assert!(
            NativeImportAuthority::legacy_source_has_shard_clock(&cut, &cut.scope_shard).unwrap()
        );
        for format in [None, Some(5), Some(7)] {
            cut.source_format_version = format;
            assert!(
                !NativeImportAuthority::legacy_source_has_shard_clock(&cut, &cut.scope_shard)
                    .unwrap()
            );
        }
        cut.source_format_version = Some(6);
        cut.source_root = root.path().join("foreign-primary-owner/root");
        assert!(
            !NativeImportAuthority::legacy_source_has_shard_clock(&cut, &cut.scope_shard).unwrap()
        );
        cut.source_root = std::path::PathBuf::from(r"C:\Primary\dfs\metadata\users");
        cut.scope_shard = sinomo_dfs::DfsMetadataShardId::new("users");
        let key = blake3::hash(&serde_json::to_vec(&(&cut.source_root, &cut.scope_shard)).unwrap())
            .to_hex()
            .to_string();
        cut.staged_root = std::path::PathBuf::from(format!(
            r"C:\Primary\dfs\metadata\.metadata-conversion\{key}\target-format-9\current-image",
        ));
        assert!(
            NativeImportAuthority::legacy_source_has_shard_clock(&cut, &cut.scope_shard).unwrap()
        );
        // LOCAL ownership/header inspection is independent of PRIMARY's path.
        let local = root.path().join("different-local-owner/dfs/metadata/users");
        let path = local.join("records/tables/snapshot.sobj");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let proof = sinomo_dfs::DfsMetadataPrefixProof {
            version: 2,
            journal_sequence: 8356,
            digest: format!("dfs-metadata-prefix-v2:blake3:{}", "a".repeat(64)),
        };
        let bytes = sinomo_core::object::serde_ext::to_bytes(
            &serde_json::json!({"format_version":6,"materialized_through_sequence":8356,
                "prefix_proof":proof,"semantic_state_fingerprint":"actual-header-DATA"}),
            1,
            sinomo_core::object::LengthMode::U32,
        )
        .unwrap();
        let mut writer = sinomo_core::storage::migration::stream::MigrationStreamWriter::resume(
            &path,
            &bytes,
            &Default::default(),
        )
        .unwrap();
        writer.finish().unwrap();
        assert!(crate::metadata::metadata_root_at_format(&local, 6).unwrap());
        assert!(!crate::metadata::metadata_root_at_format(&local, 8).unwrap());
        std::fs::write(&path, b"not-a-valid-stream-header").unwrap();
        assert!(!crate::metadata::metadata_root_at_format(&local, 6).unwrap());
    }
}
