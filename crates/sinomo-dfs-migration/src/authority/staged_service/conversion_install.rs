//! Install of a per-shard conversion's sealed image on the shard's members.
//!
//! The attempt's converter, the shard's primary or the member converting a
//! closed shard in its lost primary's place, sends each member of the install
//! set three requests on `dfs-migration` object RPC:
//!
//! 1. **Floors.** The member reports the acknowledgement floors its own copy
//!    of the shard retains, each checked to be anchored. The request carries
//!    the primary's captured source facts; the member binds them to the
//!    attempt through the source-cut digest in the shard's storage record.
//! 2. **Ancestry.** The primary streams its retained source journal frames
//!    from the lowest floor to its captured head. The member verifies and
//!    keeps each chunk, so a retry resumes from its durable cursor.
//! 3. **Stage.** The image follows as an attachment of the existing snapshot
//!    install command. The member verifies the complete ancestry, stages the
//!    image under `<receiver root>/shard-conversions/<fs>/<shard>/<attempt>/`,
//!    checks the staged candidate against the seal (prefix proof, state and
//!    file fingerprints), records what it staged, registers the attempt's
//!    `MetadataGeneration` unit, and replies with the seal it staged.
//!
//! Before each effect and again before it replies, the member reads the
//! shard's serving, write-set and storage records in one exact-set read and
//! requires the storage record Converting for exactly this attempt and seal,
//! the attempt at the shard's current epoch, the sender the attempt's
//! converter, and itself in the install set. A primary's own attempt comes
//! from the shard's primary and installs under the shard's rule; a takeover
//! attempt comes from its converter, names the shard's primary as lost, and
//! installs under the rule of the configuration its activation makes current.
//! There is no handoff and no fence: the shard is closed on every node.
//!
//! **Floors** are a migration-only obligation: an image may replace a
//! member's copy only if every prefix that copy acknowledged is an ancestor
//! of the primary's source cut. The exchange above reconciles them, keyed by
//! the attempt. [`member_floors`] reads and anchors them: a native copy's
//! floors carry prefix proofs, and a legacy copy's height-only rows are
//! anchored by a local checkpoint capture of the copy, which the floors reply
//! names so the primary streams from it. The member records the capture with
//! the challenge and requires the same floors and captures at every later
//! effect.
//!
//! A member that cannot be reached, is busy, or refuses is reported to the
//! driver as unavailable; the driver parks on its reachability.
mod member_floors;

use super::*;
use crate::authority::owner::conversion_receiver::{
    StagedConversion, conversion_stage_root, enqueue_received_conversion,
};
use crate::authority::owner::shard_conversion::InstallReply;
use crate::metadata::ack_floor::LegacyAcknowledgement;
use crate::metadata::canonical_generation::CapturedMetadataSource;
use crate::metadata::source_ancestry::{
    MAX_SOURCE_ANCESTRY_CHUNK_BYTES, SourceAncestryChunk, SourceCheckpointAnchor,
};
use member_floors::{CHECKPOINTS_RECORD, FloorRefusal, MemberFloors, recorded_checkpoint_captures};
use sinomo_core::node::NodeId;
use sinomo_dfs::{
    DfsFileSystemId, DfsMetadataGenerationSeal, DfsMetadataShardConversion,
    DfsMetadataShardConversionInstallRule, DfsMetadataShardId, DfsMetadataShardStorage,
};

pub(in crate::authority) const FLOORS_ROUTE: &str = "/dfs-migration/shard-conversion-floors-v1";
pub(in crate::authority) const CHUNK_ROUTE: &str = "/dfs-migration/shard-conversion-ancestry-v1";
pub(in crate::authority) const STAGE_ROUTE: &str = "/dfs-migration/shard-conversion-stage-v1";
pub(in crate::authority) const MAX_FLOORS_REQUEST_BYTES: usize = 16 * 1024 * 1024;
const MAX_REPLY_BYTES: usize = 1024 * 1024;

/// The attempt and image a request is for, as the primary reads its record.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(in crate::authority) struct StagedAttempt {
    pub file_system_id: DfsFileSystemId,
    pub shard_id: String,
    pub attempt_id: String,
    pub epoch: u64,
    pub seal: DfsMetadataGenerationSeal,
}

/// The primary's captured source facts, which the attempt's source-cut
/// digest binds.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(in crate::authority) struct ConversionSourceCut {
    pub source: CapturedMetadataSource,
    pub projection_fingerprint: String,
}

/// The digest an attempt records of its captured source and projection.
pub(in crate::authority) fn source_cut_digest(
    source: &CapturedMetadataSource,
    projection_fingerprint: &str,
) -> Result<String> {
    digest(&("dfs-shard-conversion-source-cut-v1", source, projection_fingerprint))
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(in crate::authority) struct FloorsRequest {
    pub attempt: StagedAttempt,
    pub source: ConversionSourceCut,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(in crate::authority) struct FloorsResponse {
    pub attempt_id: String,
    pub floors: Vec<SourceFloor>,
    /// The local checkpoint anchors of the member's height-only floors.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub checkpoints: Vec<SourceCheckpointAnchor>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::authority) struct ChunkTransfer {
    pub attempt: StagedAttempt,
    pub ordinal: u64,
    pub chunk: SourceAncestryChunk,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(in crate::authority) struct ChunkStored {
    pub attempt_id: String,
    pub ordinal: u64,
}

/// The head of the stage request; the image is its attachment.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::authority) struct StageHead {
    pub attempt: StagedAttempt,
    pub command: DfsMetadataSnapshotInstallCommand,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(in crate::authority) struct StageReply {
    pub request_id: String,
    pub attempt_id: String,
    pub result: StageResult,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "state", rename_all = "kebab-case", deny_unknown_fields)]
pub(in crate::authority) enum StageResult {
    /// The member durably holds a candidate equal to `seal`.
    Staged { seal: DfsMetadataGenerationSeal },
    /// Another writer holds the member's candidate; nothing was staged.
    Busy { reason: String },
}

/// What a member admits a stage request under.
#[derive(Debug)]
pub(in crate::authority) struct Admitted {
    pub attempt: DfsMetadataShardConversion,
}

fn refused(reason: &str) -> MetadataBackendError {
    error(format!("shard conversion stage refused: {reason}"))
}

/// Admit a stage request from `source` on member `local`, given the shard's
/// serving, write-set and storage entries from one exact-set read.
pub(in crate::authority) fn admit_conversion_stage(
    local: &NodeId,
    source: &NodeId,
    request: &StagedAttempt,
    shard: Option<&KvEntry>,
    write_set: Option<&KvEntry>,
    storage: Option<&KvEntry>,
) -> Result<Admitted> {
    let shard: DfsMetadataShardConfig = shard
        .ok_or_else(|| refused("the shard has no serving record"))?
        .decode()
        .map_err(error)?;
    if shard.file_system_id != request.file_system_id || shard.shard_id != request.shard_id {
        return Err(refused("the serving record names another shard"));
    }
    let record: DfsMetadataShardStorage = storage
        .ok_or_else(|| refused("the shard has no storage record"))?
        .decode()
        .map_err(error)?;
    record.validate_against(&shard)?;
    let attempt = record
        .converting_attempt()
        .ok_or_else(|| refused("the shard's storage is not Converting"))?;
    if attempt.attempt_id != request.attempt_id {
        return Err(refused("the storage record names another attempt"));
    }
    if attempt.sealed().is_none_or(|(seal, _)| *seal != request.seal) {
        return Err(refused("the attempt is not sealed with this image"));
    }
    if attempt.epoch != request.epoch || attempt.epoch != shard.epoch {
        return Err(refused("the attempt does not run at the shard's current epoch"));
    }
    let rule = match &attempt.takeover {
        Some(takeover) => {
            if takeover.previous_primary != shard.primary_node_id || *source != attempt.primary_node_id {
                return Err(refused("the sender is not the converter of a takeover of the shard's primary"));
            }
            DfsMetadataShardConversionInstallRule::for_takeover(&shard, write_set, &attempt.primary_node_id)
        }
        None => {
            if *source != shard.primary_node_id || attempt.primary_node_id != shard.primary_node_id {
                return Err(refused("the sender is not the shard's primary and the attempt's converter"));
            }
            DfsMetadataShardConversionInstallRule::for_shard(&shard, write_set)
        }
    }
    .map_err(error)?;
    if local == source || !rule.install_set().contains(local) {
        return Err(refused("this node is not a member the attempt installs on"));
    }
    Ok(Admitted { attempt: attempt.clone() })
}

/// The primary's source cut of `attempt`, as the ancestry exchange binds it.
fn ancestry_cut(attempt: &DfsMetadataShardConversion, cut: &ConversionSourceCut) -> Result<MetadataSourceCut> {
    let source = &cut.source;
    if source_cut_digest(source, &cut.projection_fingerprint)? != attempt.source.source_cut_digest
        || source.source_identity != attempt.source.source_identity
        || source.source_format != attempt.source.source_format
        || source.source_journal_sequence != attempt.source.source_journal_sequence
        || source.source_state_fingerprint != attempt.source.source_state_fingerprint
    {
        return Err(error("the captured source differs from the one the attempt records"));
    }
    let (seal, _) = attempt
        .sealed()
        .ok_or_else(|| error("only a sealed attempt is installed"))?;
    if source.planned_target_root.parent() != Some(source.evidence_root.as_path()) {
        return Err(error("the captured source has no retained evidence stage"));
    }
    Ok(MetadataSourceCut {
        retained_placement_reservations: source.retained_placement_reservations.clone(),
        namespace_count_cohort: None,
        source_root: source.actual_source_root.clone(),
        source_identity: source.source_identity.clone(),
        scope_shard: source.physical_shard.clone(),
        file_system_ids: source.file_system_ids.clone(),
        source_snapshot_fingerprint: source.source_snapshot_fingerprint.clone(),
        source_format_version: source.source_format,
        source_exact_prefix: source.exact_prefix.clone(),
        converted_image_fingerprint: cut.projection_fingerprint.clone(),
        current_image_fingerprint: seal.state_fingerprint.clone(),
        target_exact_prefix: seal.prefix_proof.clone(),
        source_journal_sequence: source.source_journal_sequence,
        source_state_fingerprint: source.source_state_fingerprint.clone(),
        projection_assignments: source.projection_assignments.clone(),
        original_handoff: source.original_handoff.clone(),
        staged_root: source.planned_target_root.clone(),
        pending_operations: Vec::new(),
    })
}

/// The stage command must carry exactly the attempt's image.
fn check_stage_command(admitted: &Admitted, head: &StageHead) -> Result<()> {
    let command = &head.command;
    let request = &head.attempt;
    let seal = &request.seal;
    let generation = sinomo_dfs::metadata_shard_converted_generation(
        &request.file_system_id,
        &request.shard_id,
        &admitted.attempt.attempt_id,
    );
    if command.source_node_id != admitted.attempt.primary_node_id
        || command.file_system_id.as_ref() != Some(&request.file_system_id)
        || command.authority_epoch != Some(admitted.attempt.epoch)
        || command.primary_handoff_id.is_some()
        || command.shard_id.0 != request.shard_id
        || command.next_after_sequence != seal.prefix_proof.journal_sequence
        || command.snapshot.materialized_through_sequence != command.next_after_sequence
        || command.snapshot.prefix_proof != seal.prefix_proof
        || command.snapshot.semantic_state_fingerprint != seal.state_fingerprint
        || command.snapshot.generation != generation
    {
        return Err(refused("the stage command does not carry the attempt's sealed image"));
    }
    Ok(())
}

impl NativeImportAuthority {
    /// sinomo:kv-consistency: A member admits a conversion stage only under one exact-set cut of its shard's serving, write-set and storage records.
    fn conversion_receive_context(&self, source: &NodeId, request: &StagedAttempt) -> Result<Admitted> {
        let fs = &request.file_system_id;
        let keys = [
            sinomo_dfs::metadata_shard_key(fs, &request.shard_id),
            sinomo_dfs::metadata_shard_write_set_key(fs, &request.shard_id),
            sinomo_dfs::metadata_shard_storage_key(fs, &request.shard_id),
        ];
        let (entries, _) = self.read_exact(&keys)?;
        admit_conversion_stage(
            &self.local_node()?,
            source,
            request,
            entries[0].entry.as_ref(),
            entries[1].entry.as_ref(),
            entries[2].entry.as_ref(),
        )
    }

    fn conversion_stage(&self, request: &StagedAttempt) -> Result<PathBuf> {
        conversion_stage_root(
            &self.receiver_root,
            &request.file_system_id,
            &request.shard_id,
            &request.attempt_id,
        )
    }

    /// The floors this member's copies of the shard retain, each anchored for
    /// `cut`, under the source fences of those copies.
    fn conversion_local_floors(
        &self,
        fs: &DfsFileSystemId,
        shard: &DfsMetadataShardId,
        cut: &MetadataSourceCut,
    ) -> Result<MemberFloors> {
        member_floors::member_floors(&self.metadata_root, fs, shard, cut)
    }

    /// The floors recorded at the challenge, which must still be the
    /// member's own with the same checkpoint captures, and the attempt's
    /// source cut.
    fn recorded_conversion_floors(
        &self,
        ancestry: &Path,
        admitted: &Admitted,
        request: &StagedAttempt,
    ) -> Result<(MemberFloors, MetadataSourceCut)> {
        let challenge: FloorsRequest =
            serde_json::from_slice(&std::fs::read(ancestry.join("challenge.json")).map_err(error)?)
                .map_err(error)?;
        if challenge.attempt != *request {
            return Err(error("a conversion ancestry transfer changes its recorded challenge"));
        }
        let cut = ancestry_cut(&admitted.attempt, &challenge.source)?;
        let recorded: FloorsResponse =
            serde_json::from_slice(&std::fs::read(ancestry.join("floors.json")).map_err(error)?)
                .map_err(error)?;
        let floors = self.conversion_local_floors(
            &request.file_system_id,
            &DfsMetadataShardId::new(&request.shard_id),
            &cut,
        )?;
        if recorded.floors != floors.floors.iter().map(SourceFloor::from).collect::<Vec<_>>()
            || recorded.checkpoints != floors.anchors()
            || recorded_checkpoint_captures(ancestry)? != floors.checkpoints
        {
            return Err(error("this member's floors changed after its conversion challenge"));
        }
        Ok((floors, cut))
    }

    /// The record of what this member stages for `request`, and the
    /// attempt's `MetadataGeneration` unit, which owns the stage from this
    /// member's first effect on it.
    fn register_conversion_stage(
        &self,
        context: &sinomo_module::ComponentContext,
        source: &NodeId,
        request: &StagedAttempt,
    ) -> Result<(PathBuf, StagedConversion)> {
        let stage = self.conversion_stage(request)?;
        let record = StagedConversion::new(
            source.clone(),
            &request.file_system_id,
            &request.shard_id,
            &request.attempt_id,
            request.seal.clone(),
        );
        enqueue_received_conversion(context, &record, &stage)?;
        Ok((stage, record))
    }

    pub(in crate::authority) fn receive_conversion_floors(
        &self,
        context: &sinomo_module::ComponentContext,
        source: NodeId,
        request: FloorsRequest,
    ) -> Result<FloorsResponse> {
        let admitted = self.conversion_receive_context(&source, &request.attempt)?;
        let cut = ancestry_cut(&admitted.attempt, &request.source)?;
        let floors = self.conversion_local_floors(
            &request.attempt.file_system_id,
            &DfsMetadataShardId::new(&request.attempt.shard_id),
            &cut,
        )?;
        let response = FloorsResponse {
            attempt_id: request.attempt.attempt_id.clone(),
            floors: floors.floors.iter().map(SourceFloor::from).collect(),
            checkpoints: floors.anchors(),
        };
        let (stage, _) = self.register_conversion_stage(context, &source, &request.attempt)?;
        let ancestry = stage.join("ancestry");
        if ancestry.join("floors.json").is_file() {
            // A retry keeps the recorded challenge, floors, captures and chunks.
            let recorded: FloorsRequest =
                serde_json::from_slice(&std::fs::read(ancestry.join("challenge.json")).map_err(error)?)
                    .map_err(error)?;
            let retained: FloorsResponse =
                serde_json::from_slice(&std::fs::read(ancestry.join("floors.json")).map_err(error)?)
                    .map_err(error)?;
            if recorded != request
                || retained != response
                || recorded_checkpoint_captures(&ancestry)? != floors.checkpoints
            {
                return Err(error("a conversion challenge retry changes its challenge or this member's floors"));
            }
            return Ok(retained);
        }
        std::fs::create_dir_all(&ancestry).map_err(error)?;
        Self::save_blueprint(&ancestry, "challenge.json", &request)?;
        if !floors.checkpoints.is_empty() {
            Self::save_blueprint(&ancestry, CHECKPOINTS_RECORD, &floors.checkpoints)?;
        }
        Self::save_blueprint(&ancestry, "floors.json", &response)?;
        Ok(response)
    }

    pub(in crate::authority) fn receive_conversion_chunk(
        &self,
        source: NodeId,
        transfer: ChunkTransfer,
    ) -> Result<ChunkStored> {
        if transfer.chunk.bytes.len() > MAX_SOURCE_ANCESTRY_CHUNK_BYTES {
            return Err(error("source ancestry chunk exceeds bounded transfer size"));
        }
        let admitted = self.conversion_receive_context(&source, &transfer.attempt)?;
        let ancestry = self.conversion_stage(&transfer.attempt)?.join("ancestry");
        let progress = {
            let mut registry = self.source_verifiers.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(progress) = registry.get(&ancestry) {
                Arc::clone(progress)
            } else {
                let (floors, cut) = self.recorded_conversion_floors(&ancestry, &admitted, &transfer.attempt)?;
                let mut verifier = floors.verifier(&cut)?;
                let next_ordinal = Self::replay_source_chunks(&ancestry, &mut verifier, None)?;
                let progress = Arc::new(Mutex::new(SourceTransferVerifier { verifier, next_ordinal }));
                registry.insert(ancestry.clone(), Arc::clone(&progress));
                progress
            }
        };
        let mut progress = progress.lock().unwrap_or_else(|e| e.into_inner());
        let path = ancestry.join(format!("{:020}.sobj", transfer.ordinal));
        let bytes = sinomo_core::object::serde_ext::to_bytes(
            &transfer.chunk,
            1,
            sinomo_core::object::LengthMode::U32,
        )
        .map_err(error)?;
        if transfer.ordinal < progress.next_ordinal {
            if std::fs::read(&path).map_err(error)? != bytes {
                return Err(error("source ancestry retry changes an immutable durable chunk"));
            }
        } else {
            if transfer.ordinal != progress.next_ordinal {
                return Err(error("source ancestry chunk cursor has a gap"));
            }
            let effect = (|| -> Result<()> {
                progress.verifier.apply(&transfer.chunk).map_err(FloorRefusal::not_ancestor)?;
                sinomo_core::storage::durable_file::write_file_atomic(
                    &path,
                    &bytes,
                    sinomo_core::storage::durable_file::DurableAtomicFileOptions::new("import", "tmp")
                        .sync_all(),
                )
                .map_err(error)?;
                progress.next_ordinal = progress
                    .next_ordinal
                    .checked_add(1)
                    .ok_or_else(|| error("source ancestry chunk ordinal overflow"))?;
                Ok(())
            })();
            if let Err(reason) = effect {
                // Only the resident cursor is dropped; the durable chunks are
                // replayed on the next transfer.
                drop(progress);
                self.source_verifiers
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .remove(&ancestry);
                return Err(reason);
            }
        }
        Ok(ChunkStored {
            attempt_id: transfer.attempt.attempt_id,
            ordinal: transfer.ordinal,
        })
    }

    /// Checked before the attachment is read.
    pub(in crate::authority) fn validate_conversion_stage_head(
        &self,
        source: &NodeId,
        head: &StageHead,
    ) -> Result<()> {
        let admitted = self.conversion_receive_context(source, &head.attempt)?;
        check_stage_command(&admitted, head)
    }

    pub(in crate::authority) fn receive_conversion_stage(
        &self,
        context: &sinomo_module::ComponentContext,
        source: NodeId,
        head: StageHead,
        image: sinomo_dfs::DfsMetadataSnapshotImage,
    ) -> Result<StageReply> {
        let admitted = self.conversion_receive_context(&source, &head.attempt)?;
        check_stage_command(&admitted, &head)?;
        let request = &head.attempt;
        let fs = &request.file_system_id;
        if image.descriptor() != &head.command.snapshot {
            return Err(error("the delivered image differs from its stage command"));
        }
        image.validate_file_system(fs)?;
        let (stage, record) = self.register_conversion_stage(context, &source, request)?;
        let ancestry = stage.join("ancestry");
        let (floors, cut) = self.recorded_conversion_floors(&ancestry, &admitted, request)?;
        let mut verifier = floors.verifier(&cut)?;
        Self::replay_source_chunks(&ancestry, &mut verifier, None)?;
        verifier.finish().map_err(FloorRefusal::not_ancestor)?;
        let reply = |result| StageReply {
            request_id: head.command.request_id.clone(),
            attempt_id: request.attempt_id.clone(),
            result,
        };
        let staged = (|| {
            let inspection = match sinomo_dfs::write_metadata_candidate_image_inspected(
                &stage.join("image"),
                &image,
                fs,
            ) {
                Ok(inspection) => inspection,
                Err(failure) if candidate_publication_busy(&failure) => {
                    return Ok(None);
                }
                Err(failure) => return Err(failure),
            };
            let identity = inspection.identity();
            let staged = DfsMetadataGenerationSeal {
                prefix_proof: identity.prefix_proof.clone(),
                state_fingerprint: identity.state_fingerprint.clone(),
                file_fingerprint: identity.file_fingerprint.clone(),
            };
            if staged != request.seal {
                return Err(error("the staged candidate differs from the attempt's seal"));
            }
            Self::save_blueprint(&stage, StagedConversion::record_name(), &record)?;
            Ok(Some(staged))
        })();
        // The unit settles whatever this request left, under the record it
        // reads after this request's effects.
        enqueue_received_conversion(context, &record, &stage)?;
        let Some(staged) = staged? else {
            return Ok(reply(StageResult::Busy {
                reason: "another writer holds this member's staged candidate".into(),
            }));
        };
        // The effect is acknowledged only while its attempt still runs.
        self.conversion_receive_context(&source, request)?;
        self.source_verifiers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&ancestry);
        Ok(reply(StageResult::Staged { seal: staged }))
    }

    fn conversion_rpc<T: serde::de::DeserializeOwned>(
        &self,
        node: &NodeId,
        route: &str,
        body: &impl Serialize,
    ) -> Result<T> {
        let transport = self
            .transport
            .as_ref()
            .ok_or_else(|| error("native component object-RPC transport is unavailable"))?;
        let call = sinomo_core::network::ObjectRpcCall::encode_borrowed_value(route, "dfs-migration", body)
            .map_err(error)?;
        let result = self.read_peer(transport.call_async(node, call, source_effect_rpc_options(), true))?;
        let response = self.read_peer(result.into_materialized_value_response(MAX_REPLY_BYTES))?;
        response.decode_body().map_err(error)
    }

    fn conversion_stage_rpc(
        &self,
        node: &NodeId,
        head: &StageHead,
        image: &sinomo_dfs::DfsMetadataSnapshotImage,
    ) -> Result<StageReply> {
        let transport = self
            .transport
            .as_ref()
            .ok_or_else(|| error("native component object-RPC transport is unavailable"))?;
        let request =
            sinomo_core::network::ObjectRpcRequest::encode_body(STAGE_ROUTE, "dfs-migration", head)
                .map_err(error)?;
        let call = sinomo_core::network::ObjectRpcCall::new(request, image.attachment_source().map_err(error)?);
        let result = self.read_peer(transport.call_async(node, call, source_effect_rpc_options(), true))?;
        let response = self.read_peer(result.into_materialized_value_response(MAX_REPLY_BYTES))?;
        response.decode_body().map_err(error)
    }

    /// Stage `attempt`'s sealed image at `image_root` on member `node`.
    /// `Installed` means the member acknowledged staging a candidate equal
    /// to the seal. A member that cannot be reached, is busy or refuses is
    /// `Unavailable`; an error means this node's own image or source cannot
    /// be read.
    pub(in crate::authority) fn install_conversion_image(
        &self,
        node: &NodeId,
        shard: &DfsMetadataShardConfig,
        attempt: &DfsMetadataShardConversion,
        image_root: &Path,
        source: &ConversionSourceCut,
    ) -> Result<InstallReply> {
        let (seal, _) = attempt
            .sealed()
            .ok_or_else(|| error("only a sealed attempt is installed"))?;
        let request = StagedAttempt {
            file_system_id: shard.file_system_id.clone(),
            shard_id: shard.shard_id.clone(),
            attempt_id: attempt.attempt_id.clone(),
            epoch: attempt.epoch,
            seal: seal.clone(),
        };
        let cut = ancestry_cut(attempt, source)?;
        let image = sinomo_dfs::read_metadata_candidate_image(image_root)?;
        let descriptor = image.descriptor().clone();
        if descriptor.prefix_proof != seal.prefix_proof
            || descriptor.semantic_state_fingerprint != seal.state_fingerprint
        {
            return Err(error("the attempt's image differs from its seal"));
        }
        let unavailable = |reason: String| Ok(InstallReply::Unavailable { reason });
        let floors: FloorsResponse = match self.conversion_rpc(
            node,
            FLOORS_ROUTE,
            &FloorsRequest { attempt: request.clone(), source: source.clone() },
        ) {
            Ok(floors) => floors,
            Err(failure) => return unavailable(failure.to_string()),
        };
        if floors.attempt_id != attempt.attempt_id {
            return unavailable("the member answered the floor challenge of another attempt".into());
        }
        let checkpoints = floors.checkpoints;
        let floors = floors.floors.into_iter().map(Into::into).collect::<Vec<LegacyAcknowledgement>>();
        // A member whose floors the source cannot extend is not installed.
        // A combined source copy can have moved beside the image `root`'s
        // own conversion installed since the capture recorded it.
        let actual = crate::metadata::combined_root::current_location(&cut.source_root, &cut.scope_shard)?;
        crate::metadata::rebind_relocated_source_fence(&cut.source_root, &actual)?;
        let mut reader = match crate::metadata::source_ancestry::source_ancestry_reader(
            &cut,
            &actual,
            &floors,
            &checkpoints,
        ) {
            Ok(reader) => reader,
            Err(failure) => return unavailable(failure.to_string()),
        };
        let mut ordinal = 0u64;
        loop {
            let chunk = match reader.next_chunk(MAX_SOURCE_ANCESTRY_CHUNK_BYTES) {
                Ok(Some(chunk)) => chunk,
                Ok(None) => break,
                Err(failure) => return unavailable(failure.to_string()),
            };
            let stored: ChunkStored = match self.conversion_rpc(
                node,
                CHUNK_ROUTE,
                &ChunkTransfer { attempt: request.clone(), ordinal, chunk },
            ) {
                Ok(stored) => stored,
                Err(failure) => return unavailable(failure.to_string()),
            };
            if stored != (ChunkStored { attempt_id: attempt.attempt_id.clone(), ordinal }) {
                return unavailable("the member did not retain the exact ancestry chunk".into());
            }
            ordinal = ordinal
                .checked_add(1)
                .ok_or_else(|| error("source ancestry ordinal exhausted"))?;
        }
        let command = DfsMetadataSnapshotInstallCommand {
            request_id: digest(&("dfs-shard-conversion-stage-v1", &attempt.attempt_id, node))?,
            source_node_id: self.local_node()?,
            file_system_id: Some(shard.file_system_id.clone()),
            authority_epoch: Some(attempt.epoch),
            primary_handoff_id: None,
            shard_id: DfsMetadataShardId::new(&shard.shard_id),
            next_after_sequence: seal.prefix_proof.journal_sequence,
            snapshot: descriptor,
            image: None,
            reason: "per-shard conversion install".into(),
        };
        let request_id = command.request_id.clone();
        let reply = match self.conversion_stage_rpc(node, &StageHead { attempt: request, command }, &image) {
            Ok(reply) => reply,
            Err(failure) => return unavailable(failure.to_string()),
        };
        if reply.request_id != request_id || reply.attempt_id != attempt.attempt_id {
            return unavailable("the member answered another stage request".into());
        }
        match reply.result {
            StageResult::Staged { seal: staged } if staged == *seal => Ok(InstallReply::Installed),
            StageResult::Staged { .. } => unavailable("the member staged another image".into()),
            StageResult::Busy { reason } => unavailable(reason),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(name: &str) -> NodeId {
        NodeId::from_legacy_named(name).unwrap()
    }

    fn shard(epoch: u64, primary: &str) -> DfsMetadataShardConfig {
        DfsMetadataShardConfig {
            file_system_id: sinomo_dfs::sifs_file_system_id(),
            shard_id: "users".into(),
            primary_node_id: node(primary),
            replica_node_ids: [ "stage-primary", "stage-member", "stage-other"]
                .into_iter()
                .filter(|name| *name != primary)
                .map(node)
                .collect(),
            epoch,
            assignment: sinomo_dfs::DfsMetadataShardAssignment::PathPrefix { prefix: "/users".into() },
            default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
        }
    }

    fn seal(digit: &str) -> DfsMetadataGenerationSeal {
        DfsMetadataGenerationSeal {
            prefix_proof: sinomo_dfs::DfsMetadataPrefixProof {
                version: 2,
                journal_sequence: 7,
                digest: format!("dfs-metadata-prefix-v2:blake3:{}", digit.repeat(64)),
            },
            state_fingerprint: format!("state-{digit}"),
            file_fingerprint: format!("file-{digit}"),
        }
    }

    fn attempt(shard: &DfsMetadataShardConfig, sequence: u64, seal: DfsMetadataGenerationSeal) -> DfsMetadataShardConversion {
        let source = sinomo_dfs::DfsMetadataConversionSource {
            source_identity: "stage-source".into(),
            source_format: Some(10),
            source_journal_sequence: sequence,
            source_state_fingerprint: "stage-state".into(),
            source_cut_digest: "a".repeat(64),
            pending_mapping_digest: "b".repeat(64),
        };
        DfsMetadataShardConversion {
            attempt_id: sinomo_dfs::metadata_shard_conversion_attempt_id(
                &shard.file_system_id,
                &shard.shard_id,
                &shard.assignment,
                shard.epoch,
                &shard.primary_node_id,
                &source,
                None,
            )
            .unwrap(),
            epoch: shard.epoch,
            primary_node_id: shard.primary_node_id.clone(),
            takeover: None,
            source,
            seal: Some(seal),
            contributions_digest: Some("c".repeat(64)),
        }
    }

    struct Keys {
        store: sinomo_core::kv::InProcessKvStore,
        shard_key: String,
        write_set_key: String,
        storage_key: String,
    }

    impl Keys {
        fn new(shard: &DfsMetadataShardConfig, storage: &DfsMetadataShardStorage) -> Self {
            let mut keys = Self {
                store: sinomo_core::kv::InProcessKvStore::new(),
                shard_key: sinomo_dfs::metadata_shard_key(&shard.file_system_id, &shard.shard_id),
                write_set_key: sinomo_dfs::metadata_shard_write_set_key(&shard.file_system_id, &shard.shard_id),
                storage_key: sinomo_dfs::metadata_shard_storage_key(&shard.file_system_id, &shard.shard_id),
            };
            keys.put_shard(shard);
            keys.put_storage(storage);
            keys
        }
        fn put_shard(&mut self, shard: &DfsMetadataShardConfig) {
            let value = sinomo_core::kv::encode_kv_value(shard).unwrap();
            self.store.put(self.shard_key.clone(), value, None).unwrap();
        }
        fn put_storage(&mut self, storage: &DfsMetadataShardStorage) {
            let value = sinomo_core::kv::encode_kv_value(storage).unwrap();
            self.store.put(self.storage_key.clone(), value, None).unwrap();
        }
        fn admit(&self, local: &str, source: &str, request: &StagedAttempt) -> Result<Admitted> {
            admit_conversion_stage(
                &node(local),
                &node(source),
                request,
                self.store.get(&self.shard_key),
                self.store.get(&self.write_set_key),
                self.store.get(&self.storage_key),
            )
        }
    }

    fn request(attempt: &DfsMetadataShardConversion) -> StagedAttempt {
        StagedAttempt {
            file_system_id: sinomo_dfs::sifs_file_system_id(),
            shard_id: "users".into(),
            attempt_id: attempt.attempt_id.clone(),
            epoch: attempt.epoch,
            seal: attempt.seal.clone().unwrap(),
        }
    }

    fn refusal(result: Result<Admitted>) -> String {
        result.expect_err("the stage must be refused").to_string()
    }

    #[test]
    fn a_member_refuses_a_stage_for_another_attempt_epoch_or_seal() {
        let current = shard(3, "stage-primary");
        let ours = attempt(&current, 9, seal("1"));
        let mut keys = Keys::new(&current, &DfsMetadataShardStorage::converting(&current, ours.clone()));
        let admitted = keys.admit("stage-member", "stage-primary", &request(&ours)).unwrap();
        assert_eq!(admitted.attempt, ours);

        // Another attempt of the same primary than the one recorded.
        let other = attempt(&current, 10, seal("1"));
        assert!(refusal(keys.admit("stage-member", "stage-primary", &request(&other))).contains("another attempt"));
        // The recorded attempt with another image.
        let mut other_seal = request(&ours);
        other_seal.seal = seal("2");
        assert!(refusal(keys.admit("stage-member", "stage-primary", &other_seal)).contains("not sealed with this image"));
        // A request for an earlier epoch than the attempt's.
        let mut earlier = request(&ours);
        earlier.epoch -= 1;
        assert!(refusal(keys.admit("stage-member", "stage-primary", &earlier)).contains("current epoch"));
        // Only the shard's primary sends its image; and never to a non-member.
        assert!(refusal(keys.admit("stage-member", "stage-other", &request(&ours))).contains("not the shard's primary"));
        assert!(refusal(keys.admit("stage-outsider", "stage-primary", &request(&ours))).contains("not a member"));
        // An unsealed record admits no image.
        let mut unsealed = ours.clone();
        unsealed.seal = None;
        unsealed.contributions_digest = None;
        keys.put_storage(&DfsMetadataShardStorage::converting(&current, unsealed));
        assert!(refusal(keys.admit("stage-member", "stage-primary", &request(&ours))).contains("not sealed with this image"));

        // The epoch moved under the attempt: the old primary's request is
        // refused even though the record still names its attempt.
        keys.put_storage(&DfsMetadataShardStorage::converting(&current, ours.clone()));
        let successor = shard(4, "stage-member");
        keys.put_shard(&successor);
        assert!(refusal(keys.admit("stage-other", "stage-primary", &request(&ours))).contains("current epoch"));
        // So is the new primary's, until its own attempt is recorded.
        let replacement = attempt(&successor, 9, seal("3"));
        assert!(refusal(keys.admit("stage-other", "stage-member", &request(&replacement))).contains("another attempt"));
        keys.put_storage(&DfsMetadataShardStorage::converting(&successor, replacement.clone()));
        keys.admit("stage-other", "stage-member", &request(&replacement)).unwrap();
        assert!(refusal(keys.admit("stage-other", "stage-primary", &request(&ours))).contains("another attempt"));
    }
}
