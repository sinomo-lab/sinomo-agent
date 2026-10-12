//! Representation-only configuration preparation before current DFS admission.
//! The existing factory retains the actual owner; native reads/CAS supply the
//! control-plane authority. This phase grants no DFS publication permission.
use super::*;
use crate::metadata::source_document::{
    SourceDocumentContext, SourcePayloadWire, decode_original_document,
};
use sinomo_dfs::{DfsFileSystemConfig, DfsStoragePreparation};

const PREFIX: &str = "/dfs/filesystems/";

#[derive(Clone, Copy)]
enum RecordKind {
    FileSystem,
    Assignment,
    Handoff,
    HandoffProof,
    PlacementPermit,
}

fn record_kind(key: &str) -> Option<RecordKind> {
    let relative = key.strip_prefix(PREFIX)?;
    let parts = relative.split('/').collect::<Vec<_>>();
    match parts.as_slice() {
        [fs, "config"] if !fs.is_empty() => Some(RecordKind::FileSystem),
        [fs, "placement-authority-permit-v1"] if !fs.is_empty() => {
            Some(RecordKind::PlacementPermit)
        }
        [fs, "metadata-shards", shard] if !fs.is_empty() && !shard.is_empty() => {
            Some(RecordKind::Assignment)
        }
        [fs, "metadata-membership-handoffs", shard] if !fs.is_empty() && !shard.is_empty() => {
            Some(RecordKind::Handoff)
        }
        [fs, "metadata-membership-handoff-proofs", shard]
            if !fs.is_empty() && !shard.is_empty() =>
        {
            Some(RecordKind::HandoffProof)
        }
        _ => None,
    }
}

fn pointer_segment(value: &str) -> String {
    value.replace('~', "~0").replace('/', "~1")
}

fn module_payload_paths(value: &serde_json::Value) -> Vec<String> {
    value
        .get("module_metadata")
        .and_then(serde_json::Value::as_object)
        .into_iter()
        .flat_map(|map| map.keys())
        .map(|namespace| format!("/module_metadata/{}/payload", pointer_segment(namespace)))
        .collect()
}
fn normalized_file_system(
    entry: &KvEntry,
    current_defaults: Option<&[sinomo_dfs::DfsSubtreePolicy]>,
) -> Result<DfsFileSystemConfig> {
    let mut value = decode_original_document(&entry.value)?;
    let context = SourceDocumentContext::new(&entry.value, "")?;
    let declared = module_payload_paths(&value);
    crate::legacy_wire::normalize_original_module_payloads(&mut value, &context, &declared)?;
    if let Some(rules) = value
        .get_mut("subtree_policies")
        .and_then(serde_json::Value::as_array_mut)
    {
        for rule in rules {
            crate::legacy_wire::source_identity_fields(rule)?;
            if let Some(default) = current_defaults.and_then(|defaults| {
                defaults.iter().find(|default| {
                    rule.get("path_prefix").and_then(serde_json::Value::as_str)
                        == Some(default.path_prefix.as_str())
                })
            }) {
                for (field, count) in [
                    ("desired_replicas", default.desired_replicas),
                    ("min_commit_replicas", default.min_commit_replicas),
                ] {
                    if rule.get(field).is_none_or(serde_json::Value::is_null) {
                        rule[field] = serde_json::json!(count);
                    }
                }
            }
        }
    }
    let count = current_defaults.and_then(|defaults| {
        defaults
            .iter()
            .find(|default| default.policy == sinomo_core::DfsObjectPolicy::ReplicatedEventual)
            .map(|default| default.desired_replicas)
    });
    // These are NEW current bootstrap defaults from actual native membership;
    // no historical grant/receipt W is inferred or altered here.
    let value =
        crate::legacy_wire::convert_file_system_wire_with_current_import_defaults(value, count)
            .map_err(error)?;
    let config: DfsFileSystemConfig = serde_json::from_value(value).map_err(error)?;
    if sinomo_dfs::file_system_config_key(&config.file_system_id) != entry.key {
        return Err(error(
            "source filesystem config differs from its actual native key",
        ));
    }
    Ok(config)
}

fn normalized_assignment(entry: &KvEntry) -> Result<DfsMetadataShardConfig> {
    normalized_assignment_value(decode_original_document(&entry.value)?, &entry.key)
}
pub(super) fn normalized_assignment_value(
    mut value: serde_json::Value,
    expected_key: &str,
) -> Result<DfsMetadataShardConfig> {
    let fields = value
        .as_object_mut()
        .ok_or_else(|| error("source assignment is not a record"))?;
    if let Some(codec) = fields.remove("namespace_prepare_codec")
        && !matches!(
            codec.as_str(),
            Some("legacy" | "packed-v1" | "packed-v2" | "packed-v3" | "packed-v6" | "prepared-v7")
        )
    {
        return Err(error(
            "unknown original assignment codec requires explicit conversion",
        ));
    }
    crate::legacy_wire::source_identity_fields(&mut value)?;
    let config: DfsMetadataShardConfig = serde_json::from_value(value).map_err(error)?;
    if sinomo_dfs::metadata_shard_key(&config.file_system_id, &config.shard_id) != expected_key {
        return Err(error(
            "source assignment differs from its actual native key",
        ));
    }
    Ok(config)
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ReleasedHandoffProof {
    #[serde(default)]
    cause: Option<serde_json::Value>,
    handoff_id: String,
    retiring_shard: serde_json::Value,
    desired_shard: serde_json::Value,
    journal_sequence: u64,
    state_fingerprint: String,
    #[serde(default)]
    acknowledged_node_ids: Vec<String>,
}
enum LegacyProofDisposition {
    Settled(Vec<MetadataCapturedKvEntry>),
    Pending,
}
/// Classifies historical DATA only. No current proof/cause/ACK is synthesized.
fn settled_legacy_proof(
    entry: &KvEntry,
    scope: &BTreeMap<String, KvEntry>,
) -> Result<LegacyProofDisposition> {
    let proof: ReleasedHandoffProof =
        serde_json::from_value(decode_original_document(&entry.value)?).map_err(error)?;
    let codec_bearing = [&proof.retiring_shard, &proof.desired_shard]
        .iter()
        .any(|cfg| cfg.get("namespace_prepare_codec").is_some());
    if !codec_bearing && proof.cause.is_some() {
        return Err(error(
            "malformed current proof cannot be retired as historical data",
        ));
    }
    if let Some(cause) = &proof.cause {
        if serde_json::from_value::<sinomo_dfs::DfsMetadataShardHandoffCause>(cause.clone())
            .map_err(error)?
            != sinomo_dfs::DfsMetadataShardHandoffCause::Membership
        {
            return Err(error(
                "native storage-import proof requires its existing source installation reconciliation",
            ));
        }
    }
    if proof.handoff_id.is_empty()
        || proof.state_fingerprint.is_empty()
        || proof.acknowledged_node_ids.iter().any(String::is_empty)
    {
        return Err(error(
            "historical handoff proof has incomplete recorded data",
        ));
    }
    let fs = crate::legacy_wire::source_file_system_id(
        proof
            .desired_shard
            .get("file_system_id")
            .cloned()
            .ok_or_else(|| error("historical desired scope lacks filesystem"))?,
    )?;
    let shard = proof
        .desired_shard
        .get("shard_id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| error("historical desired scope lacks shard"))?;
    let key = sinomo_dfs::metadata_shard_key(&fs, shard);
    let retiring = normalized_assignment_value(proof.retiring_shard, &key)?;
    let desired = normalized_assignment_value(proof.desired_shard, &key)?;
    if sinomo_dfs::metadata_shard_membership_handoff_proof_key(&fs, &desired.shard_id) != entry.key
        || desired.epoch <= retiring.epoch
    {
        return Err(error(
            "historical proof differs from encoded scope or epoch transition",
        ));
    }
    let pending_key = sinomo_dfs::metadata_shard_membership_handoff_key(&fs, &desired.shard_id);
    if scope.contains_key(&pending_key) {
        return Ok(LegacyProofDisposition::Pending);
    }
    let active = scope
        .get(&key)
        .ok_or_else(|| error("settled historical proof has no actual current assignment"))?;
    if normalized_assignment(active)? != desired {
        return Err(error(
            "historical proof desired configuration differs from actual current assignment; native reconciliation required",
        ));
    }
    // Sequence/ACK data are archived only, never accepted as publication evidence.
    let _source_sequence = proof.journal_sequence;
    Ok(LegacyProofDisposition::Settled(vec![
        MetadataCapturedKvEntry {
            key: key.clone(),
            entry: Some(active.clone()),
        },
        MetadataCapturedKvEntry {
            key: pending_key,
            entry: None,
        },
    ]))
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct CompletePlacementPermitSource {
    job_id: String,
    holder_id: String,
    holder_cluster_id: String,
    holder_node_id: sinomo_core::node::NodeId,
    holder_session_id: String,
}
fn validate_complete_placement_permit_source(entry: &KvEntry) -> Result<()> {
    let rejected = || MetadataBackendError::Rejected {
        code: sinomo_dfs::DfsMetadataRejectionCode::Other,
        message: format!(
            "placement permit {} is malformed: no production acquisition writes missing holder cluster/node/session identity; original bytes are retained",
            entry.key
        ),
    };
    let raw = decode_original_document(&entry.value).map_err(|_| rejected())?;
    let data: CompletePlacementPermitSource =
        serde_json::from_value(raw).map_err(|_| rejected())?;
    if [
        &data.job_id,
        &data.holder_id,
        &data.holder_cluster_id,
        &data.holder_session_id,
    ]
    .into_iter()
    .any(|value| value.is_empty() || value.len() > 512 || value.chars().any(char::is_control))
    {
        return Err(rejected());
    }
    let _actual_named_owner = data.holder_node_id;
    Ok(())
}

fn current_record(entry: &KvEntry, kind: RecordKind) -> Result<bool> {
    match kind {
        RecordKind::PlacementPermit => {
            validate_complete_placement_permit_source(entry)?;
            Ok(true)
        }
        RecordKind::FileSystem => {
            let value = decode_original_document(&entry.value)?;
            let context = SourceDocumentContext::new(&entry.value, "")?;
            for path in module_payload_paths(&value) {
                if matches!(context.raw_payload_wire(&path)?, SourcePayloadWire::Json) {
                    return Ok(false);
                }
            }
            match entry.decode::<DfsFileSystemConfig>() {
                Ok(config) => {
                    if sinomo_dfs::file_system_config_key(&config.file_system_id) != entry.key {
                        return Err(error(
                            "current filesystem config differs from its native key",
                        ));
                    }
                    Ok(true)
                }
                Err(_) => Ok(false),
            }
        }
        RecordKind::Assignment => match entry.decode::<DfsMetadataShardConfig>() {
            Ok(config) => {
                if sinomo_dfs::metadata_shard_key(&config.file_system_id, &config.shard_id)
                    != entry.key
                {
                    return Err(error("current assignment differs from its native key"));
                }
                Ok(true)
            }
            Err(_) => Ok(false),
        },
        RecordKind::Handoff => {
            match entry.decode::<sinomo_dfs::DfsMetadataShardMembershipHandoff>() {
                Ok(handoff) => {
                    if handoff.retiring_shard.file_system_id != handoff.desired_shard.file_system_id
                        || handoff.retiring_shard.shard_id != handoff.desired_shard.shard_id
                        || sinomo_dfs::metadata_shard_membership_handoff_key(
                            &handoff.retiring_shard.file_system_id,
                            &handoff.retiring_shard.shard_id,
                        ) != entry.key
                    {
                        return Err(error(
                            "current handoff differs from its actual native scope key",
                        ));
                    }
                    Ok(true)
                }
                Err(_) => Ok(false),
            }
        }
        RecordKind::HandoffProof => {
            match entry.decode::<sinomo_dfs::DfsMetadataShardMembershipHandoffProof>() {
                Ok(proof) => {
                    if proof.retiring_shard.file_system_id != proof.desired_shard.file_system_id
                        || proof.retiring_shard.shard_id != proof.desired_shard.shard_id
                        || sinomo_dfs::metadata_shard_membership_handoff_proof_key(
                            &proof.retiring_shard.file_system_id,
                            &proof.retiring_shard.shard_id,
                        ) != entry.key
                    {
                        return Err(error(
                            "current handoff proof differs from its actual native scope key",
                        ));
                    }
                    Ok(true)
                }
                Err(_) => Ok(false),
            }
        }
    }
}

fn original_reservation(
    entry: &KvEntry,
) -> Result<crate::metadata::OriginalMetadataHandoffReservation> {
    let value = decode_original_document(&entry.value)?;
    let retiring_raw = value
        .get("retiring_shard")
        .cloned()
        .ok_or_else(|| error("original pending handoff has no retiring assignment"))?;
    let desired_raw = value
        .get("desired_shard")
        .cloned()
        .ok_or_else(|| error("original pending handoff has no desired assignment"))?;
    let fs = crate::legacy_wire::source_file_system_id(
        retiring_raw
            .get("file_system_id")
            .cloned()
            .ok_or_else(|| error("original pending handoff has no filesystem identity"))?,
    )?;
    let shard = retiring_raw
        .get("shard_id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| error("original pending handoff has no shard identity"))?
        .to_owned();
    let expected_key = sinomo_dfs::metadata_shard_key(&fs, &shard);
    let retiring_shard = normalized_assignment_value(retiring_raw, &expected_key)?;
    let desired_shard = normalized_assignment_value(desired_raw, &expected_key)?;
    if entry.key != sinomo_dfs::metadata_shard_membership_handoff_key(&fs, &shard)
        || desired_shard.epoch
            != retiring_shard
                .epoch
                .checked_add(1)
                .ok_or_else(|| error("original handoff epoch exhausted"))?
        || desired_shard.assignment != retiring_shard.assignment
        || desired_shard.default_policy != retiring_shard.default_policy
    {
        return Err(error(
            "original pending handoff changes its exact scope/assignment or epoch",
        ));
    }
    Ok(crate::metadata::OriginalMetadataHandoffReservation {
        entry: entry.clone(),
        retiring_shard,
        desired_shard,
    })
}

fn proof_paired_with_original_pending(
    entry: &KvEntry,
    scope: &BTreeMap<String, KvEntry>,
) -> Result<bool> {
    let raw = decode_original_document(&entry.value)?;
    let proof: ReleasedHandoffProof = serde_json::from_value(raw).map_err(error)?;
    let fs = crate::legacy_wire::source_file_system_id(
        proof
            .retiring_shard
            .get("file_system_id")
            .cloned()
            .ok_or_else(|| error("original proof has no filesystem"))?,
    )?;
    let shard = proof
        .retiring_shard
        .get("shard_id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| error("original proof has no shard"))?;
    let pending_key = sinomo_dfs::metadata_shard_membership_handoff_key(&fs, shard);
    let Some(pending) = scope.get(&pending_key) else {
        return Ok(false);
    };
    if current_record(pending, RecordKind::Handoff)? {
        return Ok(false);
    }
    let original = original_reservation(pending)?;
    let pending_raw = decode_original_document(&pending.value)?;
    let cfg_key = sinomo_dfs::metadata_shard_key(&fs, shard);
    let same_handoff = pending_raw
        .get("handoff_id")
        .and_then(serde_json::Value::as_str)
        == Some(proof.handoff_id.as_str());
    let proof_retiring = normalized_assignment_value(proof.retiring_shard, &cfg_key)?;
    let proof_desired = normalized_assignment_value(proof.desired_shard, &cfg_key)?;
    Ok((same_handoff
        && proof_retiring == original.retiring_shard
        && proof_desired == original.desired_shard)
        || (!same_handoff && proof_desired == original.retiring_shard))
}

impl NativeImportAuthority {
    pub(super) fn capture_original_handoff(
        &self,
        shard: &sinomo_dfs::DfsMetadataShardId,
        file_system_ids: &[sinomo_dfs::DfsFileSystemId],
    ) -> Result<Option<crate::metadata::OriginalMetadataHandoffReservation>> {
        let keys = file_system_ids
            .iter()
            .map(|fs| sinomo_dfs::metadata_shard_membership_handoff_key(fs, &shard.0))
            .collect::<Vec<_>>();
        // An empty physical DATA scope names no original handoff records.
        // Other import scope/authority validation remains required.
        if keys.is_empty() {
            return Ok(None);
        }
        let (rows, _) = self.read_exact(&keys)?;
        let mut original = None;
        for row in rows {
            let Some(entry) = row.entry else {
                continue;
            };
            if current_record(&entry, RecordKind::Handoff)? {
                continue;
            }
            let captured = original_reservation(&entry)?;
            if captured.retiring_shard.shard_id != shard.0
                || !file_system_ids.contains(&captured.retiring_shard.file_system_id)
            {
                return Err(error(
                    "original reservation differs from the requested physical source",
                ));
            }
            if original.replace(captured).is_some() {
                return Err(error("physical import has competing original reservations"));
            }
        }
        Ok(original)
    }
    pub(crate) fn required_original_metadata_sources(
        &self,
    ) -> Result<Vec<crate::metadata::OriginalMetadataHandoffReservation>> {
        let read = block_native(
            &self.runtime,
            self.cluster()?
                .kv_range_prefix_complete_authoritative_async(PREFIX.into()),
        )?;
        current_authority(&read.authority)?;
        let mut required = Vec::new();
        for entry in read.value {
            if !matches!(record_kind(&entry.key), Some(RecordKind::Handoff)) {
                continue;
            }
            if !current_record(&entry, RecordKind::Handoff)? {
                required.push(original_reservation(&entry)?);
                continue;
            }
            let handoff: sinomo_dfs::DfsMetadataShardMembershipHandoff =
                entry.decode().map_err(error)?;
            if !matches!(
                handoff.cause,
                sinomo_dfs::DfsMetadataShardHandoffCause::StorageImport { .. }
            ) {
                continue;
            }
            let fs = &handoff.retiring_shard.file_system_id;
            let shard = sinomo_dfs::DfsMetadataShardId::new(&handoff.retiring_shard.shard_id);
            let destination = sinomo_dfs::local_dfs_metadata_shard_path(
                &sinomo_core::local::SinomoDataDir::new(&self.data_root),
                &shard,
            );
            let retained = crate::metadata::retained_native_import_plan(
                &destination,
                fs,
                &shard,
                &handoff.handoff_id,
            )?;
            let plan = match retained {
                Some((plan, _actual_plan_root)) => plan,
                None => {
                    let received = self.receiver_root_for(fs, &shard.0, &handoff.handoff_id)?;
                    let path = received.join("native-import-plan.json");
                    if !path.exists() {
                        return Err(MetadataBackendError::StoragePreparationPending {
                            root: destination.clone(),
                            processed_records: 0,
                        });
                    }
                    serde_json::from_slice(&std::fs::read(path).map_err(error)?).map_err(error)?
                }
            };
            Self::validate_plan(&plan)?;
            let source_fence = sinomo_dfs::DfsMetadataShardMembershipHandoffFence {
                handoff_id: plan.expected_handoff.handoff_id.clone(),
                primary_node_id: plan.expected_handoff.retiring_shard.primary_node_id.clone(),
                journal_sequence: plan.source_cut.source_journal_sequence,
                state_fingerprint: plan.source_cut.source_state_fingerprint.clone(),
            };
            if !Self::content_handoff_matches_plan(&handoff, &plan.expected_handoff, &source_fence)
            {
                return Err(error(
                    "retained native plan differs from live exact StorageImport",
                ));
            }
            let replay = self.commit(plan.stage_request.clone())?;
            Self::require_committed(&replay)?;
            if let Some(original) = &plan.source_cut.original_handoff {
                required.push(original.clone());
            }
        }
        required.sort_by(|left, right| left.entry.key.cmp(&right.entry.key));
        Ok(required)
    }
    /// sinomo:kv-configuration: Normalize raw schema and current bootstrap
    /// policy inputs from one owned native authority cut before DFS readiness.
    pub(super) fn prepare_configuration_records(
        &self,
        max_records: usize,
    ) -> Result<DfsStoragePreparation> {
        if max_records == 0 {
            return Ok(DfsStoragePreparation::Pending {
                processed_records: 0,
            });
        }
        let archive = self.receiver_root.join("configuration-v7");
        std::fs::create_dir_all(&archive).map_err(error)?;
        // The existing native prefix API inventories compact control-plane rows.
        // This budget bounds conversion/CAS work, not that API's prefix response.
        let client = self.control_client(&archive)?;
        // One actual native ReadIndex binds configuration and member inputs.
        // This does not wait for this factory's later v7 service advertisement.
        let native = block_native(
            &self.runtime,
            self.cluster()?
                .kv_range_prefix_complete_authoritative_async("/".into()),
        )?;
        current_authority(&native.authority)?;
        // Fresh roots and already-current control data need no policy defaults
        // and must not wait for the later ordinary filesystem genesis.
        let mut all_current = true;
        for entry in native
            .value
            .iter()
            .filter(|entry| entry.key.starts_with(PREFIX))
        {
            if let Some(kind) = record_kind(&entry.key) {
                match current_record(entry, kind) {
                    Ok(true) => {}
                    Ok(false) => all_current = false,
                    Err(failed) => {
                        let binding = digest(entry)?;
                        Self::save_blueprint(&archive, &format!("original-{binding}.json"), entry)?;
                        return Err(failed);
                    }
                }
            }
        }
        if all_current {
            return Ok(DfsStoragePreparation::Ready);
        }
        let raw_scope = native
            .value
            .iter()
            .map(|entry| (entry.key.clone(), entry.clone()))
            .collect::<BTreeMap<_, _>>();
        let mut state = sinomo_core::cluster::read_cluster_state_without_kv(
            &sinomo_core::network::ClusterPaths::new(&sinomo_core::local::SinomoDataDir::new(
                &self.data_root,
            )),
        )
        .map_err(error)?;
        let member_rows = native
            .value
            .iter()
            .filter(|row| row.key.starts_with("/cluster/members/"))
            .cloned()
            .collect::<Vec<_>>();
        let mut members = Vec::with_capacity(member_rows.len());
        for row in &member_rows {
            let member: sinomo_core::ClusterMemberRecord = row.decode().map_err(error)?;
            if member.cluster_id != state.cluster_id
                || sinomo_core::node::cluster_member_key(&member.node_id) != row.key
            {
                return Err(error(
                    "native bootstrap policy member differs from actual cluster/key",
                ));
            }
            members.push(member);
        }
        if !members
            .iter()
            .any(|member| member.node_id == state.local_node_id && member.state.is_authorized())
        {
            return Err(error(
                "current bootstrap policy has no actual authorized local identity",
            ));
        }
        state.members = members;
        let defaults = sinomo_dfs::default_cluster_sifs_subtree_policies(&state);
        let policy_binding = digest(&(
            &member_rows,
            &native.authority,
            &state.local_node_id,
            &defaults,
        ))?;
        let mut entries = native
            .value
            .into_iter()
            .filter(|entry| entry.key.starts_with(PREFIX))
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| left.key.cmp(&right.key));
        if entries.windows(2).any(|pair| pair[0].key == pair[1].key) {
            return Err(error("native configuration prefix contains duplicate keys"));
        }
        let mut conversions = Vec::<(KvEntry, Option<Vec<u8>>)>::new();
        let mut extra_guards = BTreeMap::<String, MetadataCapturedKvEntry>::new();
        let mut has_more = false;
        for entry in entries {
            let Some(kind) = record_kind(&entry.key) else {
                continue;
            };
            if current_record(&entry, kind)? {
                continue;
            }
            if conversions.len() == max_records {
                has_more = true;
                break;
            }
            // Preserve the original native row/bytes/revisions/lease before
            // any source-only adaptation. It never serves as author permission.
            let binding = digest(&entry)?;
            Self::save_blueprint(&archive, &format!("original-{binding}.json"), &entry)?;
            let value = match kind {
                RecordKind::PlacementPermit => {
                    validate_complete_placement_permit_source(&entry)?;
                    continue;
                }
                RecordKind::FileSystem => {
                    Self::save_blueprint(
                        &archive,
                        &format!("current-policy-{binding}-{policy_binding}.json"),
                        &(
                            &member_rows,
                            &native.authority,
                            &state.local_node_id,
                            &defaults,
                        ),
                    )?;
                    sinomo_core::kv::encode_kv_value(&normalized_file_system(
                        &entry,
                        Some(&defaults),
                    )?)
                }
                .map(Some)
                .map_err(error)?,
                RecordKind::Assignment => Some(
                    sinomo_core::kv::encode_kv_value(&normalized_assignment(&entry)?)
                        .map_err(error)?,
                ),
                RecordKind::HandoffProof => {
                    if proof_paired_with_original_pending(&entry, &raw_scope)? {
                        continue;
                    }
                    match settled_legacy_proof(&entry, &raw_scope)? {
                        LegacyProofDisposition::Pending => {
                            return Ok(DfsStoragePreparation::Pending {
                                processed_records: 0,
                            });
                        }
                        LegacyProofDisposition::Settled(guards) => {
                            for guard in guards {
                                extra_guards.insert(guard.key.clone(), guard);
                            }
                            None
                        }
                    }
                }
                RecordKind::Handoff => {
                    original_reservation(&entry)?;
                    continue;
                }
            };
            conversions.push((entry, value));
        }
        if conversions.is_empty() {
            return Ok(DfsStoragePreparation::Ready);
        }
        let mut keys = conversions
            .iter()
            .map(|(entry, _)| entry.key.clone())
            .collect::<Vec<_>>();
        keys.extend(member_rows.iter().map(|row| row.key.clone()));
        keys.extend(extra_guards.keys().cloned());
        keys.sort();
        keys.dedup();
        let (captured, exact_authority) = self.read_exact(&keys)?;
        if conversions
            .iter()
            .map(|(row, _)| row)
            .chain(member_rows.iter())
            .any(|expected| {
                captured
                    .iter()
                    .find(|row| row.key == expected.key)
                    .and_then(|row| row.entry.as_ref())
                    != Some(expected)
            })
        {
            return Ok(DfsStoragePreparation::Pending {
                processed_records: 0,
            });
        }
        if extra_guards.values().any(|expected| {
            captured
                .iter()
                .find(|row| row.key == expected.key)
                .is_none_or(|actual| actual.entry != expected.entry)
        }) {
            return Ok(DfsStoragePreparation::Pending {
                processed_records: 0,
            });
        }
        for guard in extra_guards
            .values()
            .filter_map(|guard| guard.entry.as_ref())
        {
            let binding = digest(guard)?;
            Self::save_blueprint(&archive, &format!("original-{binding}.json"), guard)?;
        }
        let submitted = KvTxn {
            compares: captured_compares(&captured),
            success: conversions
                .iter()
                .map(|(entry, value)| match value {
                    Some(value) => KvOp::Put {
                        key: entry.key.clone(),
                        value: value.clone(),
                        lease_id: entry.lease_id,
                    },
                    None => KvOp::Delete {
                        key: entry.key.clone(),
                    },
                })
                .collect(),
            failure: Vec::new(),
        };
        let binding = digest(&(&submitted, &exact_authority))?;
        let request = KvTxnRequest::new(
            KvRequestId::derived("dfs-configuration-shape-import-v7", binding.as_bytes()),
            submitted,
        );
        let committed = self.blueprint_commit(&archive, request)?;
        if !committed
            .result
            .txn_response
            .as_ref()
            .is_some_and(|response| response.succeeded)
        {
            return Ok(DfsStoragePreparation::Pending {
                processed_records: 0,
            });
        }
        Self::require_committed(&committed)?;
        let (observed, _) = self.read_exact(&keys)?;
        if conversions.iter().any(|(old, value)| {
            let entry = observed
                .iter()
                .find(|row| row.key == old.key)
                .and_then(|row| row.entry.as_ref());
            match (value, entry) {
                (Some(value), Some(entry)) => {
                    entry.value != *value || entry.lease_id != old.lease_id
                }
                (None, None) => false,
                _ => true,
            }
        }) {
            return Err(error(
                "converted native configuration changed before admission",
            ));
        }
        let post = client.range_prefix(PREFIX).map_err(error)?;
        let post_scope = post
            .iter()
            .map(|entry| (entry.key.clone(), entry.clone()))
            .collect::<BTreeMap<_, _>>();
        for entry in post {
            if let Some(kind) = record_kind(&entry.key)
                && !current_record(&entry, kind)?
            {
                if matches!(kind, RecordKind::Handoff) {
                    original_reservation(&entry)?;
                    let binding = digest(&entry)?;
                    Self::save_blueprint(&archive, &format!("original-{binding}.json"), &entry)?;
                    continue;
                }
                if matches!(kind, RecordKind::HandoffProof)
                    && proof_paired_with_original_pending(&entry, &post_scope)?
                {
                    let binding = digest(&entry)?;
                    Self::save_blueprint(&archive, &format!("original-{binding}.json"), &entry)?;
                    continue;
                }
                has_more = true;
                break;
            }
        }
        Ok(if has_more {
            DfsStoragePreparation::Pending {
                processed_records: conversions.len(),
            }
        } else {
            DfsStoragePreparation::Ready
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;
    use sinomo_core::kv::{KvVersion, LeaseId};

    fn row(key: String, value: Vec<u8>) -> KvEntry {
        KvEntry {
            key,
            value,
            create_revision: KvRevision(7),
            modify_revision: KvRevision(9),
            version: KvVersion(2),
            lease_id: Some(LeaseId(5)),
        }
    }

    fn config() -> DfsFileSystemConfig {
        DfsFileSystemConfig::from(&sinomo_dfs::DfsFileSystem::sifs(
            sinomo_core::node::NodeId::generate(),
        ))
    }

    #[test]
    fn old_configuration_preserves_explicit_policy_and_actual_opaque_array_type() {
        let current = config();
        let mut source = serde_json::to_value(&current).unwrap();
        source["metadata_format"] = serde_json::json!("namespaced-v6");
        source["module_metadata"] = serde_json::json!({"owner.opaque": {
            "schema_version":1,"payload":[110,117,108,108]
        }});
        let original = row(
            sinomo_dfs::file_system_config_key(&current.file_system_id),
            sinomo_core::kv::encode_kv_value(&source).unwrap(),
        );
        let captured = original.clone();
        let normalized = normalized_file_system(&original, None).unwrap();
        assert_eq!(normalized.subtree_policies, current.subtree_policies);
        assert_eq!(normalized.owner_node_id, current.owner_node_id);
        assert_eq!(
            normalized.module_metadata["owner.opaque"].payload,
            serde_json::json!([110, 117, 108, 108]),
            "actual JSON array cannot become the JSON null encoded by the same bytes"
        );
        assert_eq!(
            original, captured,
            "source bytes/revision/lease remain exact"
        );
        source.as_object_mut().unwrap().remove("metadata_format");
        let no_format_hint = row(
            original.key.clone(),
            sinomo_core::kv::encode_kv_value(&source).unwrap(),
        );
        assert!(
            !current_record(&no_format_hint, RecordKind::FileSystem).unwrap(),
            "a JSON numeric list is not current SOBJ Bytes even if typed decoding accepts its shape"
        );
        assert_eq!(
            normalized_file_system(&no_format_hint, None)
                .unwrap()
                .module_metadata["owner.opaque"]
                .payload,
            serde_json::json!([110, 117, 108, 108])
        );
    }

    #[test]
    fn old_configuration_uses_actual_byte_leaf_and_never_guesses_missing_policy_counts() {
        #[derive(Serialize)]
        struct Module<'a> {
            schema_version: u32,
            #[serde(with = "serde_bytes")]
            payload: &'a [u8],
        }
        #[derive(Serialize)]
        struct Source<'a> {
            file_system_id: &'a sinomo_dfs::DfsFileSystemId,
            display_name: &'a str,
            owner_node_id: &'a sinomo_core::node::NodeId,
            subtree_policies: &'a [sinomo_dfs::DfsSubtreePolicy],
            retention_policies: &'a BTreeMap<String, sinomo_dfs::DfsRetentionPolicy>,
            module_metadata: BTreeMap<&'a str, Module<'a>>,
            metadata_format: &'a str,
        }
        let current = config();
        let source = Source {
            file_system_id: &current.file_system_id,
            display_name: &current.display_name,
            owner_node_id: &current.owner_node_id,
            subtree_policies: &current.subtree_policies,
            retention_policies: &current.retention_policies,
            module_metadata: BTreeMap::from([(
                "vendor.opaque",
                Module {
                    schema_version: 1,
                    payload: b"null",
                },
            )]),
            metadata_format: "namespaced-v6",
        };
        let encoded = row(
            sinomo_dfs::file_system_config_key(&current.file_system_id),
            sinomo_core::kv::encode_kv_value(&source).unwrap(),
        );
        assert_eq!(
            normalized_file_system(&encoded, None)
                .unwrap()
                .module_metadata["vendor.opaque"]
                .payload,
            serde_json::Value::Null
        );
        let mut missing = serde_json::to_value(&current).unwrap();
        missing["subtree_policies"] = serde_json::json!([{
            "path_prefix":"/","policy":"replicated-eventual","scope":"subtree"
        }]);
        let encoded = row(
            sinomo_dfs::file_system_config_key(&current.file_system_id),
            sinomo_core::kv::encode_kv_value(&missing).unwrap(),
        );
        assert!(normalized_file_system(&encoded, None).is_err());
    }

    #[test]
    fn explicitly_supplied_new_bootstrap_defaults_preserve_all_recorded_counts() {
        let current = config();
        let mut source = serde_json::to_value(&current).unwrap();
        source["subtree_policies"] = serde_json::json!([
            {"path_prefix":"/users","policy":"replicated-eventual"},
            {"path_prefix":"/services","policy":"replicated-eventual","scope":"self-only","min_commit_replicas":2,"desired_replicas":3,"min_distinct_zones":2}
        ]);
        let original = row(
            sinomo_dfs::file_system_config_key(&current.file_system_id),
            sinomo_core::kv::encode_kv_value(&source).unwrap(),
        );
        let defaults = vec![
            sinomo_dfs::DfsSubtreePolicy::new(
                "/users",
                sinomo_core::DfsObjectPolicy::ReplicatedEventual,
                1,
                2,
            ),
            sinomo_dfs::DfsSubtreePolicy::new(
                "/services",
                sinomo_core::DfsObjectPolicy::ReplicatedEventual,
                1,
                2,
            ),
        ];
        let normalized = normalized_file_system(&original, Some(&defaults)).unwrap();
        assert_eq!(
            (
                normalized.subtree_policies[0].min_commit_replicas,
                normalized.subtree_policies[0].desired_replicas
            ),
            (1, 2)
        );
        assert_eq!(
            (
                normalized.subtree_policies[1].min_commit_replicas,
                normalized.subtree_policies[1].desired_replicas
            ),
            (2, 3)
        );
        assert_eq!(
            normalized.subtree_policies[1].scope,
            sinomo_dfs::DfsPlacementScope::SelfOnly
        );
        assert_eq!(normalized.subtree_policies[1].min_distinct_zones, 2);
        assert_eq!(decode_original_document(&original.value).unwrap(), source);
    }

    #[test]
    fn retired_codec_conversion_preserves_exact_assignment_and_rejects_other_scope() {
        let current = DfsMetadataShardConfig {
            file_system_id: config().file_system_id,
            shard_id: "root".into(),
            primary_node_id: sinomo_core::node::NodeId::generate(),
            replica_node_ids: vec![sinomo_core::node::NodeId::generate()],
            epoch: 17,
            assignment: sinomo_dfs::DfsMetadataShardAssignment::Root,
            default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
        };
        let mut source = serde_json::to_value(&current).unwrap();
        source["namespace_prepare_codec"] = serde_json::json!("packed-v6");
        let original = row(
            sinomo_dfs::metadata_shard_key(&current.file_system_id, &current.shard_id),
            sinomo_core::kv::encode_kv_value(&source).unwrap(),
        );
        assert_eq!(normalized_assignment(&original).unwrap(), current);
        let mut wrong_key = original.clone();
        wrong_key.key = sinomo_dfs::metadata_shard_key(&current.file_system_id, "another");
        assert!(normalized_assignment(&wrong_key).is_err());
        source["namespace_prepare_codec"] = serde_json::json!("unknown-codec");
        let invalid = row(
            original.key.clone(),
            sinomo_core::kv::encode_kv_value(&source).unwrap(),
        );
        assert!(normalized_assignment(&invalid).is_err());
    }
}

#[cfg(test)]
mod settled_proof_tests {
    use super::*;
    use sinomo_core::kv::KvVersion;
    // Exact released source SOBJ values retained by the read-only source-copy
    // manifest. These are schema DATA fixtures, never native authority/ACKs.
    const REAL_SETTLED_ROWS: &[(&str, &str, &str)] = &[
        (
            "/dfs/filesystems/3m3x1ehfwt2cca4jkz8r1jwq9g/metadata-membership-handoff-proofs/root",
            concat!(
                "534f424a01000400890400000000000021060000000a00000068616e646f66665f6964241a00000030316d3437396b33",
                "70763570347368346a7a66787771656b6a610e0000007265746972696e675f736861726421080000000e00000066696c",
                "655f73797374656d5f6964241a000000336d337831656866777432636361346a6b7a3872316a77713967080000007368",
                "6172645f69642404000000726f6f740f0000007072696d6172795f6e6f64655f6964241a00000030316d3437396a3078",
                "303631706b6171797a3568743066676e6d100000007265706c6963615f6e6f64655f6964732224010000001a00000030",
                "316d3437396a33686e6b6364637238747a67327a34736d37630500000065706f63680802000000000000000a00000061",
                "737369676e6d656e742404000000726f6f74170000006e616d6573706163655f707265706172655f636f646563240900",
                "00007061636b65642d76360e00000064656661756c745f706f6c69637924130000007265706c6963617465642d657665",
                "6e7475616c0d000000646573697265645f736861726421080000000e00000066696c655f73797374656d5f6964241a00",
                "0000336d337831656866777432636361346a6b7a3872316a777139670800000073686172645f69642404000000726f6f",
                "740f0000007072696d6172795f6e6f64655f6964241a00000030316d3437396a3078303631706b6171797a3568743066",
                "676e6d100000007265706c6963615f6e6f64655f6964732224050000001a00000030316d3437396a33686e6b63646372",
                "38747a67327a34736d37631a00000030316d3437396a336a6a703877686467313878366731316b31721a00000030316d",
                "3437396a346b7a37676174316e377834706839333377321a00000030316d3437396a366d34637435676d7470356b7631",
                "6a796631671a00000030316d3437396a366d6b353767343865723930337273376865610500000065706f636808030000",
                "00000000000a00000061737369676e6d656e742404000000726f6f74170000006e616d6573706163655f707265706172",
                "655f636f64656324090000007061636b65642d76360e00000064656661756c745f706f6c69637924130000007265706c",
                "6963617465642d6576656e7475616c100000006a6f75726e616c5f73657175656e6365080c0000000000000011000000",
                "73746174655f66696e6765727072696e74245e0000006466732d6d657461646174612d7072656669782d76323a626c61",
                "6b65333a3763363162336133613535616330336430303332363931316666393135306332636332373437633330643939",
                "38663233366132326536393961396439343531621500000061636b6e6f776c65646765645f6e6f64655f696473222405",
                "0000001a00000030316d3437396a3078303631706b6171797a3568743066676e6d1a00000030316d3437396a33686e6b",
                "6364637238747a67327a34736d37631a00000030316d3437396a336a6a703877686467313878366731316b31721a0000",
                "0030316d3437396a346b7a37676174316e377834706839333377321a00000030316d3437396a366d34637435676d7470",
                "356b76316a79663167"
            ),
            concat!(
                "534f424a01000400b90100000000000021080000000e00000066696c655f73797374656d5f6964241a000000336d3378",
                "31656866777432636361346a6b7a3872316a777139670800000073686172645f69642404000000726f6f740f00000070",
                "72696d6172795f6e6f64655f6964241a00000030316d3437396a3078303631706b6171797a3568743066676e6d100000",
                "007265706c6963615f6e6f64655f6964732224050000001a00000030316d3437396a33686e6b6364637238747a67327a",
                "34736d37631a00000030316d3437396a336a6a703877686467313878366731316b31721a00000030316d3437396a346b",
                "7a37676174316e377834706839333377321a00000030316d3437396a366d34637435676d7470356b76316a796631671a",
                "00000030316d3437396a366d6b353767343865723930337273376865610500000065706f63680803000000000000000a",
                "00000061737369676e6d656e742404000000726f6f74170000006e616d6573706163655f707265706172655f636f6465",
                "6324090000007061636b65642d76360e00000064656661756c745f706f6c69637924130000007265706c696361746564",
                "2d6576656e7475616c"
            ),
        ),
        (
            "/dfs/filesystems/3m3x1ehfwt2cca4jkz8r1jwq9g/metadata-membership-handoff-proofs/services",
            concat!(
                "534f424a01000400770500000000000021060000000a00000068616e646f66665f6964241a00000030316d3437396d33",
                "627a6d33787a383938356b326e326a7a64650e0000007265746972696e675f736861726421080000000e00000066696c",
                "655f73797374656d5f6964241a000000336d337831656866777432636361346a6b7a3872316a77713967080000007368",
                "6172645f6964240800000073657276696365730f0000007072696d6172795f6e6f64655f6964241a00000030316d3437",
                "396a3078303631706b6171797a3568743066676e6d100000007265706c6963615f6e6f64655f6964732224050000001a",
                "00000030316d3437396a33686e6b6364637238747a67327a34736d37631a00000030316d3437396a336a6a7038776864",
                "67313878366731316b31721a00000030316d3437396a346b7a37676174316e377834706839333377321a00000030316d",
                "3437396a366d34637435676d7470356b76316a796631671a00000030316d3437396a366d6b3537673438657239303372",
                "73376865610500000065706f63680802000000000000000a00000061737369676e6d656e7421010000000b0000007061",
                "74682d70726566697821010000000600000070726566697824090000002f7365727669636573170000006e616d657370",
                "6163655f707265706172655f636f64656324090000007061636b65642d76360e00000064656661756c745f706f6c6963",
                "7924130000007265706c6963617465642d6576656e7475616c0d000000646573697265645f736861726421080000000e",
                "00000066696c655f73797374656d5f6964241a000000336d337831656866777432636361346a6b7a3872316a77713967",
                "0800000073686172645f6964240800000073657276696365730f0000007072696d6172795f6e6f64655f6964241a0000",
                "0030316d3437396a33686e6b6364637238747a67327a34736d3763100000007265706c6963615f6e6f64655f69647322",
                "24050000001a00000030316d3437396a336a6a703877686467313878366731316b31721a00000030316d3437396a346b",
                "7a37676174316e377834706839333377321a00000030316d3437396a366d34637435676d7470356b76316a796631671a",
                "00000030316d3437396a366d6b353767343865723930337273376865611a00000030316d3437396a3078303631706b61",
                "71797a3568743066676e6d0500000065706f63680803000000000000000a00000061737369676e6d656e742101000000",
                "0b000000706174682d70726566697821010000000600000070726566697824090000002f736572766963657317000000",
                "6e616d6573706163655f707265706172655f636f64656324090000007061636b65642d76360e00000064656661756c74",
                "5f706f6c69637924130000007265706c6963617465642d6576656e7475616c100000006a6f75726e616c5f7365717565",
                "6e63650804000000000000001100000073746174655f66696e6765727072696e74245e0000006466732d6d6574616461",
                "74612d7072656669782d76323a626c616b65333a32376463373035323463363265316539646365356135366165633131",
                "3031626237343536353366616135326336636266333139386232396665396438323264621500000061636b6e6f776c65",
                "646765645f6e6f64655f6964732224060000001a00000030316d3437396a3078303631706b6171797a3568743066676e",
                "6d1a00000030316d3437396a33686e6b6364637238747a67327a34736d37631a00000030316d3437396a336a6a703877",
                "686467313878366731316b31721a00000030316d3437396a346b7a37676174316e377834706839333377321a00000030",
                "316d3437396a366d34637435676d7470356b76316a796631671a00000030316d3437396a366d6b353767343865723930",
                "33727337686561"
            ),
            concat!(
                "534f424a01000400e50100000000000021080000000e00000066696c655f73797374656d5f6964241a000000336d3378",
                "31656866777432636361346a6b7a3872316a777139670800000073686172645f6964240800000073657276696365730f",
                "0000007072696d6172795f6e6f64655f6964241a00000030316d3437396a33686e6b6364637238747a67327a34736d37",
                "63100000007265706c6963615f6e6f64655f6964732224050000001a00000030316d3437396a336a6a70387768646731",
                "3878366731316b31721a00000030316d3437396a346b7a37676174316e377834706839333377321a00000030316d3437",
                "396a366d34637435676d7470356b76316a796631671a00000030316d3437396a366d6b35376734386572393033727337",
                "6865611a00000030316d3437396a3078303631706b6171797a3568743066676e6d0500000065706f6368080300000000",
                "0000000a00000061737369676e6d656e7421010000000b000000706174682d7072656669782101000000060000007072",
                "6566697824090000002f7365727669636573170000006e616d6573706163655f707265706172655f636f646563240900",
                "00007061636b65642d76360e00000064656661756c745f706f6c69637924130000007265706c6963617465642d657665",
                "6e7475616c"
            ),
        ),
        (
            "/dfs/filesystems/3m3x1ehfwt2cca4jkz8r1jwq9g/metadata-membership-handoff-proofs/users",
            concat!(
                "534f424a010004006b0500000000000021060000000a00000068616e646f66665f6964241a00000030316d3437396d37",
                "7677316177333574307332306e687368707a0e0000007265746972696e675f736861726421080000000e00000066696c",
                "655f73797374656d5f6964241a000000336d337831656866777432636361346a6b7a3872316a77713967080000007368",
                "6172645f6964240500000075736572730f0000007072696d6172795f6e6f64655f6964241a00000030316d3437396a30",
                "78303631706b6171797a3568743066676e6d100000007265706c6963615f6e6f64655f6964732224050000001a000000",
                "30316d3437396a33686e6b6364637238747a67327a34736d37631a00000030316d3437396a336a6a7038776864673138",
                "78366731316b31721a00000030316d3437396a346b7a37676174316e377834706839333377321a00000030316d343739",
                "6a366d34637435676d7470356b76316a796631671a00000030316d3437396a366d6b3537673438657239303372733768",
                "65610500000065706f63680802000000000000000a00000061737369676e6d656e7421010000000b000000706174682d",
                "70726566697821010000000600000070726566697824060000002f7573657273170000006e616d6573706163655f7072",
                "65706172655f636f64656324090000007061636b65642d76360e00000064656661756c745f706f6c6963792413000000",
                "7265706c6963617465642d6576656e7475616c0d000000646573697265645f736861726421080000000e00000066696c",
                "655f73797374656d5f6964241a000000336d337831656866777432636361346a6b7a3872316a77713967080000007368",
                "6172645f6964240500000075736572730f0000007072696d6172795f6e6f64655f6964241a00000030316d3437396a33",
                "6a6a703877686467313878366731316b3172100000007265706c6963615f6e6f64655f6964732224050000001a000000",
                "30316d3437396a346b7a37676174316e377834706839333377321a00000030316d3437396a366d34637435676d747035",
                "6b76316a796631671a00000030316d3437396a366d6b353767343865723930337273376865611a00000030316d343739",
                "6a3078303631706b6171797a3568743066676e6d1a00000030316d3437396a33686e6b6364637238747a67327a34736d",
                "37630500000065706f63680803000000000000000a00000061737369676e6d656e7421010000000b000000706174682d",
                "70726566697821010000000600000070726566697824060000002f7573657273170000006e616d6573706163655f7072",
                "65706172655f636f64656324090000007061636b65642d76360e00000064656661756c745f706f6c6963792413000000",
                "7265706c6963617465642d6576656e7475616c100000006a6f75726e616c5f73657175656e6365080400000000000000",
                "1100000073746174655f66696e6765727072696e74245e0000006466732d6d657461646174612d7072656669782d7632",
                "3a626c616b65333a39323130303533306366303762303433303765643832313939396165663436303263393538346261",
                "6461616631633462376136313164383336333732353461301500000061636b6e6f776c65646765645f6e6f64655f6964",
                "732224060000001a00000030316d3437396a3078303631706b6171797a3568743066676e6d1a00000030316d3437396a",
                "33686e6b6364637238747a67327a34736d37631a00000030316d3437396a336a6a703877686467313878366731316b31",
                "721a00000030316d3437396a346b7a37676174316e377834706839333377321a00000030316d3437396a366d34637435",
                "676d7470356b76316a796631671a00000030316d3437396a366d6b35376734386572393033727337686561"
            ),
            concat!(
                "534f424a01000400df0100000000000021080000000e00000066696c655f73797374656d5f6964241a000000336d3378",
                "31656866777432636361346a6b7a3872316a777139670800000073686172645f6964240500000075736572730f000000",
                "7072696d6172795f6e6f64655f6964241a00000030316d3437396a336a6a703877686467313878366731316b31721000",
                "00007265706c6963615f6e6f64655f6964732224050000001a00000030316d3437396a346b7a37676174316e37783470",
                "6839333377321a00000030316d3437396a366d34637435676d7470356b76316a796631671a00000030316d3437396a36",
                "6d6b353767343865723930337273376865611a00000030316d3437396a3078303631706b6171797a3568743066676e6d",
                "1a00000030316d3437396a33686e6b6364637238747a67327a34736d37630500000065706f6368080300000000000000",
                "0a00000061737369676e6d656e7421010000000b000000706174682d7072656669782101000000060000007072656669",
                "7824060000002f7573657273170000006e616d6573706163655f707265706172655f636f64656324090000007061636b",
                "65642d76360e00000064656661756c745f706f6c69637924130000007265706c6963617465642d6576656e7475616c"
            ),
        ),
        (
            "/dfs/filesystems/3m3x1ehfwt2cca4jkz8r1jwq9g/metadata-membership-handoff-proofs/root",
            concat!(
                "534f424a010004006b0400000000000021060000000a00000068616e646f66665f6964241a00000030316d3432647076",
                "66656e7979637172653973717935316e6d6e0e0000007265746972696e675f736861726421080000000e00000066696c",
                "655f73797374656d5f6964241a000000336d337831656866777432636361346a6b7a3872316a77713967080000007368",
                "6172645f69642404000000726f6f740f0000007072696d6172795f6e6f64655f6964241a00000030316d3432646e7377",
                "6d333932656a6a6d793331336633327938100000007265706c6963615f6e6f64655f6964732224010000001a00000030",
                "316d3432646e7739776e317678786336366a7671383667767a0500000065706f63680802000000000000000a00000061",
                "737369676e6d656e742404000000726f6f74170000006e616d6573706163655f707265706172655f636f646563240900",
                "00007061636b65642d76360e00000064656661756c745f706f6c69637924130000007265706c6963617465642d657665",
                "6e7475616c0d000000646573697265645f736861726421080000000e00000066696c655f73797374656d5f6964241a00",
                "0000336d337831656866777432636361346a6b7a3872316a777139670800000073686172645f69642404000000726f6f",
                "740f0000007072696d6172795f6e6f64655f6964241a00000030316d3432646e73776d333932656a6a6d793331336633",
                "327938100000007265706c6963615f6e6f64655f6964732224050000001a00000030316d3432646e7739767a39703164",
                "3031716b6e336732376e301a00000030316d3432646e7739776e317678786336366a7671383667767a1a00000030316d",
                "3432646e7865373673703737326d646b35746e317631611a00000030316d3432646e7a356d6377317a77383864683036",
                "766e3638721a00000030316d3432646e7a367a3036306370626b30676b7a74726a66760500000065706f636808030000",
                "00000000000a00000061737369676e6d656e742404000000726f6f74170000006e616d6573706163655f707265706172",
                "655f636f64656324090000007061636b65642d76360e00000064656661756c745f706f6c69637924130000007265706c",
                "6963617465642d6576656e7475616c100000006a6f75726e616c5f73657175656e636508150000000000000011000000",
                "73746174655f66696e6765727072696e74245e0000006466732d6d657461646174612d7072656669782d76323a626c61",
                "6b65333a6139303139343961366263376136663163376233633064643066316130623538666664363337326232633832",
                "37376365633934626632336434353938653066371500000061636b6e6f776c65646765645f6e6f64655f696473222404",
                "0000001a00000030316d3432646e73776d333932656a6a6d7933313366333279381a00000030316d3432646e7739767a",
                "397031643031716b6e336732376e301a00000030316d3432646e7739776e317678786336366a7671383667767a1a0000",
                "0030316d3432646e7865373673703737326d646b35746e31763161"
            ),
            concat!(
                "534f424a01000400b90100000000000021080000000e00000066696c655f73797374656d5f6964241a000000336d3378",
                "31656866777432636361346a6b7a3872316a777139670800000073686172645f69642404000000726f6f740f00000070",
                "72696d6172795f6e6f64655f6964241a00000030316d3432646e73776d333932656a6a6d793331336633327938100000",
                "007265706c6963615f6e6f64655f6964732224050000001a00000030316d3432646e7739767a397031643031716b6e33",
                "6732376e301a00000030316d3432646e7739776e317678786336366a7671383667767a1a00000030316d3432646e7865",
                "373673703737326d646b35746e317631611a00000030316d3432646e7a356d6377317a77383864683036766e3638721a",
                "00000030316d3432646e7a367a3036306370626b30676b7a74726a66760500000065706f63680803000000000000000a",
                "00000061737369676e6d656e742404000000726f6f74170000006e616d6573706163655f707265706172655f636f6465",
                "6324090000007061636b65642d76360e00000064656661756c745f706f6c69637924130000007265706c696361746564",
                "2d6576656e7475616c"
            ),
        ),
        (
            "/dfs/filesystems/3m3x1ehfwt2cca4jkz8r1jwq9g/metadata-membership-handoff-proofs/services",
            concat!(
                "534f424a01000400770500000000000021060000000a00000068616e646f66665f6964241a00000030316d3432647176",
                "3638727a36386e70627176646567646b74630e0000007265746972696e675f736861726421080000000e00000066696c",
                "655f73797374656d5f6964241a000000336d337831656866777432636361346a6b7a3872316a77713967080000007368",
                "6172645f6964240800000073657276696365730f0000007072696d6172795f6e6f64655f6964241a00000030316d3432",
                "646e73776d333932656a6a6d793331336633327938100000007265706c6963615f6e6f64655f6964732224050000001a",
                "00000030316d3432646e7739767a397031643031716b6e336732376e301a00000030316d3432646e7739776e31767878",
                "6336366a7671383667767a1a00000030316d3432646e7865373673703737326d646b35746e317631611a00000030316d",
                "3432646e7a356d6377317a77383864683036766e3638721a00000030316d3432646e7a367a3036306370626b30676b7a",
                "74726a66760500000065706f63680802000000000000000a00000061737369676e6d656e7421010000000b0000007061",
                "74682d70726566697821010000000600000070726566697824090000002f7365727669636573170000006e616d657370",
                "6163655f707265706172655f636f64656324090000007061636b65642d76360e00000064656661756c745f706f6c6963",
                "7924130000007265706c6963617465642d6576656e7475616c0d000000646573697265645f736861726421080000000e",
                "00000066696c655f73797374656d5f6964241a000000336d337831656866777432636361346a6b7a3872316a77713967",
                "0800000073686172645f6964240800000073657276696365730f0000007072696d6172795f6e6f64655f6964241a0000",
                "0030316d3432646e7739767a397031643031716b6e336732376e30100000007265706c6963615f6e6f64655f69647322",
                "24050000001a00000030316d3432646e7739776e317678786336366a7671383667767a1a00000030316d3432646e7865",
                "373673703737326d646b35746e317631611a00000030316d3432646e7a356d6377317a77383864683036766e3638721a",
                "00000030316d3432646e7a367a3036306370626b30676b7a74726a66761a00000030316d3432646e73776d333932656a",
                "6a6d7933313366333279380500000065706f63680803000000000000000a00000061737369676e6d656e742101000000",
                "0b000000706174682d70726566697821010000000600000070726566697824090000002f736572766963657317000000",
                "6e616d6573706163655f707265706172655f636f64656324090000007061636b65642d76360e00000064656661756c74",
                "5f706f6c69637924130000007265706c6963617465642d6576656e7475616c100000006a6f75726e616c5f7365717565",
                "6e63650805000000000000001100000073746174655f66696e6765727072696e74245e0000006466732d6d6574616461",
                "74612d7072656669782d76323a626c616b65333a66623561366332376337386566393436383965383264313166353937",
                "3535353531316130393961376632356137306332666633323963353832356165633530371500000061636b6e6f776c65",
                "646765645f6e6f64655f6964732224060000001a00000030316d3432646e73776d333932656a6a6d7933313366333279",
                "381a00000030316d3432646e7739767a397031643031716b6e336732376e301a00000030316d3432646e7739776e3176",
                "78786336366a7671383667767a1a00000030316d3432646e7865373673703737326d646b35746e317631611a00000030",
                "316d3432646e7a356d6377317a77383864683036766e3638721a00000030316d3432646e7a367a3036306370626b3067",
                "6b7a74726a6676"
            ),
            concat!(
                "534f424a01000400e50100000000000021080000000e00000066696c655f73797374656d5f6964241a000000336d3378",
                "31656866777432636361346a6b7a3872316a777139670800000073686172645f6964240800000073657276696365730f",
                "0000007072696d6172795f6e6f64655f6964241a00000030316d3432646e7739767a397031643031716b6e336732376e",
                "30100000007265706c6963615f6e6f64655f6964732224050000001a00000030316d3432646e7739776e317678786336",
                "366a7671383667767a1a00000030316d3432646e7865373673703737326d646b35746e317631611a00000030316d3432",
                "646e7a356d6377317a77383864683036766e3638721a00000030316d3432646e7a367a3036306370626b30676b7a7472",
                "6a66761a00000030316d3432646e73776d333932656a6a6d7933313366333279380500000065706f6368080300000000",
                "0000000a00000061737369676e6d656e7421010000000b000000706174682d7072656669782101000000060000007072",
                "6566697824090000002f7365727669636573170000006e616d6573706163655f707265706172655f636f646563240900",
                "00007061636b65642d76360e00000064656661756c745f706f6c69637924130000007265706c6963617465642d657665",
                "6e7475616c"
            ),
        ),
        (
            "/dfs/filesystems/3m3x1ehfwt2cca4jkz8r1jwq9g/metadata-membership-handoff-proofs/users",
            concat!(
                "534f424a010004006b0500000000000021060000000a00000068616e646f66665f6964241a00000030316d343264717a",
                "353667336b663435363038397372616671370e0000007265746972696e675f736861726421080000000e00000066696c",
                "655f73797374656d5f6964241a000000336d337831656866777432636361346a6b7a3872316a77713967080000007368",
                "6172645f6964240500000075736572730f0000007072696d6172795f6e6f64655f6964241a00000030316d3432646e73",
                "776d333932656a6a6d793331336633327938100000007265706c6963615f6e6f64655f6964732224050000001a000000",
                "30316d3432646e7739767a397031643031716b6e336732376e301a00000030316d3432646e7739776e31767878633636",
                "6a7671383667767a1a00000030316d3432646e7865373673703737326d646b35746e317631611a00000030316d343264",
                "6e7a356d6377317a77383864683036766e3638721a00000030316d3432646e7a367a3036306370626b30676b7a74726a",
                "66760500000065706f63680802000000000000000a00000061737369676e6d656e7421010000000b000000706174682d",
                "70726566697821010000000600000070726566697824060000002f7573657273170000006e616d6573706163655f7072",
                "65706172655f636f64656324090000007061636b65642d76360e00000064656661756c745f706f6c6963792413000000",
                "7265706c6963617465642d6576656e7475616c0d000000646573697265645f736861726421080000000e00000066696c",
                "655f73797374656d5f6964241a000000336d337831656866777432636361346a6b7a3872316a77713967080000007368",
                "6172645f6964240500000075736572730f0000007072696d6172795f6e6f64655f6964241a00000030316d3432646e77",
                "39776e317678786336366a7671383667767a100000007265706c6963615f6e6f64655f6964732224050000001a000000",
                "30316d3432646e7865373673703737326d646b35746e317631611a00000030316d3432646e7a356d6377317a77383864",
                "683036766e3638721a00000030316d3432646e7a367a3036306370626b30676b7a74726a66761a00000030316d343264",
                "6e73776d333932656a6a6d7933313366333279381a00000030316d3432646e7739767a397031643031716b6e33673237",
                "6e300500000065706f63680803000000000000000a00000061737369676e6d656e7421010000000b000000706174682d",
                "70726566697821010000000600000070726566697824060000002f7573657273170000006e616d6573706163655f7072",
                "65706172655f636f64656324090000007061636b65642d76360e00000064656661756c745f706f6c6963792413000000",
                "7265706c6963617465642d6576656e7475616c100000006a6f75726e616c5f73657175656e6365080500000000000000",
                "1100000073746174655f66696e6765727072696e74245e0000006466732d6d657461646174612d7072656669782d7632",
                "3a626c616b65333a65613863663137663336303962663861643737336264383333373535303361373437386532393139",
                "3165346438633536313635623237363535636166326364301500000061636b6e6f776c65646765645f6e6f64655f6964",
                "732224060000001a00000030316d3432646e73776d333932656a6a6d7933313366333279381a00000030316d3432646e",
                "7739767a397031643031716b6e336732376e301a00000030316d3432646e7739776e317678786336366a767138366776",
                "7a1a00000030316d3432646e7865373673703737326d646b35746e317631611a00000030316d3432646e7a356d637731",
                "7a77383864683036766e3638721a00000030316d3432646e7a367a3036306370626b30676b7a74726a6676"
            ),
            concat!(
                "534f424a01000400df0100000000000021080000000e00000066696c655f73797374656d5f6964241a000000336d3378",
                "31656866777432636361346a6b7a3872316a777139670800000073686172645f6964240500000075736572730f000000",
                "7072696d6172795f6e6f64655f6964241a00000030316d3432646e7739776e317678786336366a7671383667767a1000",
                "00007265706c6963615f6e6f64655f6964732224050000001a00000030316d3432646e7865373673703737326d646b35",
                "746e317631611a00000030316d3432646e7a356d6377317a77383864683036766e3638721a00000030316d3432646e7a",
                "367a3036306370626b30676b7a74726a66761a00000030316d3432646e73776d333932656a6a6d793331336633327938",
                "1a00000030316d3432646e7739767a397031643031716b6e336732376e300500000065706f6368080300000000000000",
                "0a00000061737369676e6d656e7421010000000b000000706174682d7072656669782101000000060000007072656669",
                "7824060000002f7573657273170000006e616d6573706163655f707265706172655f636f64656324090000007061636b",
                "65642d76360e00000064656661756c745f706f6c69637924130000007265706c6963617465642d6576656e7475616c"
            ),
        ),
    ];
    fn original(hex: &str) -> Vec<u8> {
        hex.as_bytes()
            .chunks_exact(2)
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect()
    }
    fn row(key: String, value: Vec<u8>) -> KvEntry {
        KvEntry {
            key,
            value,
            create_revision: KvRevision(7),
            modify_revision: KvRevision(9),
            version: KvVersion(2),
            lease_id: None,
        }
    }
    fn fixture(index: usize) -> (KvEntry, BTreeMap<String, KvEntry>, String) {
        let (proof_key, proof_hex, cfg_hex) = REAL_SETTLED_ROWS[index];
        let raw = decode_original_document(&original(cfg_hex)).unwrap();
        let fs = crate::legacy_wire::source_file_system_id(raw["file_system_id"].clone()).unwrap();
        let shard = raw["shard_id"].as_str().unwrap();
        let key = sinomo_dfs::metadata_shard_key(&fs, shard);
        let pending = sinomo_dfs::metadata_shard_membership_handoff_key(&fs, shard);
        let scope = BTreeMap::from([(key.clone(), row(key, original(cfg_hex)))]);
        (row(proof_key.into(), original(proof_hex)), scope, pending)
    }
    #[test]
    fn all_six_actual_settled_old_proofs_plan_only_archival_deletion() {
        for index in 0..REAL_SETTLED_ROWS.len() {
            let (proof, scope, pending) = fixture(index);
            let original = proof.clone();
            let LegacyProofDisposition::Settled(guards) =
                settled_legacy_proof(&proof, &scope).unwrap()
            else {
                panic!("actual settled source requires no pending retirement")
            };
            assert_eq!(guards.len(), 2);
            assert!(
                guards
                    .iter()
                    .any(|guard| guard.key == pending && guard.entry.is_none())
            );
            assert!(
                guards
                    .iter()
                    .any(|guard| scope.get(&guard.key) == guard.entry.as_ref())
            );
            assert_eq!(
                proof, original,
                "schema planning preserves exact source proof bytes"
            );
            assert!(!current_record(&proof, RecordKind::HandoffProof).unwrap());
            // No filesystem table/root/backend is needed to recognize settled
            // historical audit data beside an already-current local root.
        }
    }
    #[test]
    fn pending_scope_and_changed_current_assignment_never_plan_proof_deletion() {
        let (proof, mut scope, pending) = fixture(0);
        let opaque_pending = row(
            pending.clone(),
            sinomo_core::kv::encode_kv_value(
                &serde_json::json!({"preserve":"exact pending source"}),
            )
            .unwrap(),
        );
        scope.insert(pending.clone(), opaque_pending.clone());
        assert!(matches!(
            settled_legacy_proof(&proof, &scope).unwrap(),
            LegacyProofDisposition::Pending
        ));
        assert_eq!(scope.remove(&pending), Some(opaque_pending));
        let current = scope.values_mut().next().unwrap();
        let mut raw = decode_original_document(&current.value).unwrap();
        raw["epoch"] = serde_json::json!(99);
        current.value = sinomo_core::kv::encode_kv_value(&raw).unwrap();
        assert!(settled_legacy_proof(&proof, &scope).is_err());
        let mut wrong_scope = proof.clone();
        wrong_scope.key.push_str("-wrong");
        assert!(settled_legacy_proof(&wrong_scope, &scope).is_err());
    }
}
