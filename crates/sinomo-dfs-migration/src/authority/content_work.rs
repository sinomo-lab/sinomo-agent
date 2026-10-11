//! Actual content work for retained imports. New grants follow verified bytes
//! and correlated normal receiver results; historical policy is never invented.
use super::*;
use crate::metadata::{MetadataSourceContentLocator, MetadataSourceContentWorkScope};
use std::io::Read;

#[derive(Clone, Debug, Deserialize)]
#[serde(try_from = "ContentVerificationWire")]
pub(super) struct ContentVerificationRequest {
    pub(super) request_id: String,
    pub(super) source: MetadataSourceContentWorkScope,
    pub(super) object: sinomo_dfs::DfsObjectMetadata,
    pub(super) version: sinomo_dfs::DfsObjectVersionRecord,
    pub(super) locator: MetadataSourceContentLocator,
    pub(super) authority: DfsMetadataShardConfig,
    pub(super) import_plan: Option<Arc<verification_context::ValidatedPlanData>>,
    plan_acknowledgement: Option<verification_context::PlanAcknowledgement>,
    pub(super) export_bytes: bool,
}

// One current wire for this route. All original typed request DATA is restored
// before any binding, plan or authority validation. The shared plan's duplicate
// mapping is structural equality on the wire, never an omitted proof/default.
pub(super) mod verification_context;
const CONTENT_VERIFICATION_WIRE_VERSION: u16 = 2;
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ContentVerificationWire {
    wire_version: u16,
    request_id: String,
    source: MetadataSourceContentWorkScope,
    object: sinomo_dfs::DfsObjectMetadata,
    version: sinomo_dfs::DfsObjectVersionRecord,
    locator: MetadataSourceContentLocator,
    authority: DfsMetadataShardConfig,
    #[serde(deserialize_with = "required_verification_plan")]
    import_plan: Option<ContentVerificationPlanEnvelope>,
    export_bytes: bool,
}
#[derive(Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum ContentVerificationPlanEnvelope {
    Definition {
        plan_digest: String,
        plan: super::plan_wire::NativePlanWire,
    },
    Reference {
        plan_digest: String,
        receiver_context: String,
    },
}
fn required_verification_plan<'de, D: serde::Deserializer<'de>>(
    decoder: D,
) -> std::result::Result<Option<ContentVerificationPlanEnvelope>, D::Error> {
    Option::deserialize(decoder)
}
impl TryFrom<ContentVerificationWire> for ContentVerificationRequest {
    type Error = MetadataBackendError;
    fn try_from(wire: ContentVerificationWire) -> Result<Self> {
        if wire.wire_version != CONTENT_VERIFICATION_WIRE_VERSION {
            return Err(error("unsupported retained verification wire version"));
        }
        let import_plan = match wire.import_plan {
            Some(ContentVerificationPlanEnvelope::Definition { plan_digest, plan }) => {
                let data = verification_context::ValidatedPlanData::new(plan.restore()?)?;
                if data.digest != plan_digest {
                    return Err(error(
                        "retained verification plan definition changes its exact digest",
                    ));
                }
                Some(data)
            }
            Some(ContentVerificationPlanEnvelope::Reference { .. }) => {
                return Err(error(
                    "plan reference requires the actual receiver data-owner context",
                ));
            }
            None => None,
        };
        Ok(Self {
            request_id: wire.request_id,
            source: wire.source,
            object: wire.object,
            version: wire.version,
            locator: wire.locator,
            authority: wire.authority,
            import_plan,
            plan_acknowledgement: None,
            export_bytes: wire.export_bytes,
        })
    }
}
#[derive(Serialize)]
#[serde(tag = "kind")]
enum ContentVerificationPlanRef<'a> {
    Definition {
        plan_digest: &'a str,
        plan: super::plan_wire::NativePlanRef<'a>,
    },
    Reference {
        plan_digest: &'a str,
        receiver_context: &'a str,
    },
}
impl Serialize for ContentVerificationRequest {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::{Error as _, SerializeMap};
        if self.import_plan.as_ref().is_some_and(|plan| {
            plan.plan.pending_operation_mapping != plan.plan.source_cut.pending_operations
        }) {
            return Err(S::Error::custom(
                "retained verification plan has unequal pending mapping DATA",
            ));
        }
        let mut fields = serializer.serialize_map(Some(9))?;
        fields.serialize_entry("wire_version", &CONTENT_VERIFICATION_WIRE_VERSION)?;
        fields.serialize_entry("request_id", &self.request_id)?;
        fields.serialize_entry("source", &self.source)?;
        fields.serialize_entry("object", &self.object)?;
        fields.serialize_entry("version", &self.version)?;
        fields.serialize_entry("locator", &self.locator)?;
        fields.serialize_entry("authority", &self.authority)?;
        fields.serialize_entry(
            "import_plan",
            &self
                .import_plan
                .as_ref()
                .map(|data| match &self.plan_acknowledgement {
                    Some(ack) => ContentVerificationPlanRef::Reference {
                        plan_digest: &data.digest,
                        receiver_context: &ack.receiver_context,
                    },
                    None => ContentVerificationPlanRef::Definition {
                        plan_digest: &data.digest,
                        plan: super::plan_wire::NativePlanRef(&data.plan),
                    },
                }),
        )?;
        fields.serialize_entry("export_bytes", &self.export_bytes)?;
        fields.end()
    }
}

impl ContentVerificationRequest {
    fn binding(&self) -> Result<String> {
        digest(&(
            "retained-content-verification-v7-wire2",
            &self.source,
            &self.object,
            &self.version,
            &self.locator,
            &self.authority,
            self.import_plan.as_ref().map(|data| data.digest.as_str()),
            self.export_bytes,
        ))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ContentVerified {
    pub(super) request_id: String,
    pub(super) holder_node_id: sinomo_core::node::NodeId,
    pub(super) file_system_id: sinomo_dfs::DfsFileSystemId,
    pub(super) object_id: sinomo_dfs::DfsObjectId,
    pub(super) version: sinomo_dfs::DfsVersion,
    pub(super) content_hash: String,
    pub(super) size_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "result", deny_unknown_fields)]
pub(super) enum ContentVerificationResult {
    Verified {
        verification: ContentVerified,
        plan_context: Option<verification_context::PlanAcknowledgement>,
    },
    ContextMissing {
        request_id: String,
        holder_node_id: sinomo_core::node::NodeId,
        file_system_id: sinomo_dfs::DfsFileSystemId,
        scope_shard: sinomo_dfs::DfsMetadataShardId,
        requested: verification_context::PlanAcknowledgement,
    },
}
pub(super) enum ResolvedVerification {
    Request(ContentVerificationRequest),
    ContextMissing(ContentVerificationResult),
}

pub(super) struct OwnedContentReader {
    pub(super) reader: Box<dyn Read + Send>,
    pub(super) owner: sinomo_dfs::runtime_host::DfsStorageImportContentReceiver,
}
impl Read for OwnedContentReader {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        // The native receiver retains the actual data owner until the network
        // body reader is closed, including cancellation during a streamed read.
        let _ = &self.owner;
        self.reader.read(output)
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct MetadataImportedRetainedContent {
    pub(crate) durability: sinomo_dfs::DfsContentDurability,
    pub(crate) receipts: Vec<sinomo_dfs::DfsReplicaReceiptRecord>,
}

pub(super) fn verify_local_retained_content(
    content: &sinomo_dfs::LocalContentStore,
    version: &sinomo_dfs::DfsObjectVersionRecord,
    locator: &MetadataSourceContentLocator,
) -> Result<()> {
    let source_object = sinomo_core::Id::new(locator.object_id.clone())
        .or_else(|_| sinomo_core::Id::from_legacy_named(locator.object_id.clone()))
        .map_err(error)?;
    let source_version = sinomo_core::Id::new(locator.version.clone())
        .or_else(|_| sinomo_core::Id::from_legacy_named(locator.version.clone()))
        .map_err(error)?;
    if source_object != version.object_id || source_version != version.version {
        return Err(error(
            "actual source content locator maps to another canonical version",
        ));
    }
    let hash = version
        .content_hash
        .as_deref()
        .ok_or_else(|| error("retained content work lacks its exact source hash"))?;
    let size = version
        .size_bytes
        .ok_or_else(|| error("retained content work lacks its exact source length"))?;
    let content_version = locator
        .content_version
        .as_deref()
        .unwrap_or(&locator.version);
    if let (Some(raw_source), Some(durability)) =
        (&locator.content_version, &version.content_durability)
    {
        let actual_source = sinomo_core::Id::new(raw_source.clone())
            .or_else(|_| sinomo_core::Id::from_legacy_named(raw_source.clone()))
            .map_err(error)?;
        if actual_source != durability.source_version {
            return Err(error(
                "content locator differs from the actual recorded source generation",
            ));
        }
    }
    // Adapters obtain this locator from the independently re-read original row;
    // the metadata successor and its byte generation remain separate identities.
    let raw_exists = match content.read_manifest(&locator.object_id, content_version) {
        Ok(_) => true,
        Err(sinomo_dfs::ContentStoreError::Io { source, .. })
            if source.kind() == std::io::ErrorKind::NotFound =>
        {
            false
        }
        Err(error_value) => return Err(error(error_value)),
    };
    let (object_key, version_key) = if raw_exists {
        (locator.object_id.as_str(), content_version)
    } else {
        // An earlier exact native import may already have established this
        // current binding. It still has to pass the complete byte verification.
        (version.object_id.as_str(), version.version.as_str())
    };
    let (manifest, mut reader) = content
        .open_object_version_stream(object_key, version_key)
        .map_err(error)?;
    if manifest.content_hash != hash || manifest.size_bytes != size {
        return Err(error("retained manifest differs from the captured version"));
    }
    let mut ingest = content
        .begin_quarantined_blob_ingest(size, Some(hash))
        .map_err(error)?;
    let mut buffer = [0; 64 * 1024];
    loop {
        let count = reader.read(&mut buffer).map_err(error)?;
        if count == 0 {
            break;
        }
        ingest.write_chunk(&buffer[..count]).map_err(error)?;
    }
    let verified = ingest.finish_quarantined().map_err(error)?;
    content
        .bind_quarantined_blob(
            version.object_id.as_str(),
            version.version.as_str(),
            verified,
        )
        .map_err(error)?;
    Ok(())
}

impl NativeImportAuthority {
    fn matches_original_pending_byte_scope(
        source: &MetadataSourceContentWorkScope,
        current: &DfsMetadataShardConfig,
        pending: &sinomo_core::kv::KvEntry,
    ) -> Result<bool> {
        let Some(original) = source.original_handoff.as_ref() else {
            return Ok(false);
        };
        let key = sinomo_dfs::metadata_shard_membership_handoff_key(
            &current.file_system_id,
            &current.shard_id,
        );
        if original.entry.key != key
            || original.retiring_shard != *current
            || original.desired_shard.file_system_id != current.file_system_id
            || original.desired_shard.shard_id != current.shard_id
            || !source.file_system_ids.contains(&current.file_system_id)
        {
            return Err(error(
                "original pending byte scope differs from its actual retiring assignment",
            ));
        }
        // Equality includes exact original bytes, modify revision and lease.
        // This is a DATA staging fence, never an old acknowledgement or author
        // capability. Current config/primary and source row checks remain live.
        Ok(original.entry == *pending)
    }

    pub(super) fn content_work_authority(
        &self,
        source: &MetadataSourceContentWorkScope,
        file_system_id: &sinomo_dfs::DfsFileSystemId,
    ) -> Result<(
        DfsMetadataShardConfig,
        Option<Arc<verification_context::ValidatedPlanData>>,
    )> {
        let keys = [
            sinomo_dfs::metadata_shard_key(file_system_id, &source.scope_shard.0),
            sinomo_dfs::metadata_shard_membership_handoff_key(
                file_system_id,
                &source.scope_shard.0,
            ),
        ];
        let (entries, _) = self.read_exact(&keys)?;
        let cfg: DfsMetadataShardConfig = entries[0]
            .entry
            .as_ref()
            .ok_or_else(|| error("retained bytework has no current native assignment"))?
            .decode()
            .map_err(error)?;
        if cfg.primary_node_id != self.local_node()?
            || !source.projection_assignments.contains(&cfg)
        {
            return Err(error(
                "retained bytework caller is not the current exact scope primary",
            ));
        }
        let Some(handoff) = entries[1].entry.as_ref() else {
            if source.original_handoff.is_some() {
                return Err(error(
                    "original pending reservation disappeared before byte verification",
                ));
            }
            return Ok((cfg, None));
        };
        if Self::matches_original_pending_byte_scope(source, &cfg, handoff)? {
            return Ok((cfg, None));
        }
        let handoff: sinomo_dfs::DfsMetadataShardMembershipHandoff =
            handoff.decode().map_err(error)?;
        let stage = crate::metadata::owned_metadata_source_stage(source)?;
        let path = stage.join("native-import-plan.json");
        let writer = crate::metadata::retain_source_fence(&source.source_root)?;
        let stamp = verification_context::plan_stamp(&path)?;
        let retained = match stamp.as_ref() {
            Some(stamp) => self
                .content_plan_contexts
                .lock()
                .map_err(|_| error("retained verification plan DATA cache is poisoned"))?
                .source(&path, stamp, &writer),
            None => None,
        };
        let data = match retained {
            Some(data) => data,
            None => {
                let plan: MetadataImportPlan =
                    serde_json::from_slice(&std::fs::read(&path).map_err(error)?).map_err(error)?;
                let data = verification_context::ValidatedPlanData::new(plan)?;
                if let Some(stamp) = stamp {
                    if verification_context::plan_stamp(&path)?.as_ref() != Some(&stamp) {
                        return Err(error("owned immutable plan changed during its exact read"));
                    }
                    self.content_plan_contexts
                        .lock()
                        .map_err(|_| error("retained verification plan DATA cache is poisoned"))?
                        .retain_source(path, stamp, &writer, Arc::clone(&data));
                }
                data
            }
        };
        let plan = &data.plan;
        let source_fence = sinomo_dfs::DfsMetadataShardMembershipHandoffFence {
            handoff_id: plan.expected_handoff.handoff_id.clone(),
            primary_node_id: plan.expected_handoff.retiring_shard.primary_node_id.clone(),
            journal_sequence: plan.source_cut.source_journal_sequence,
            state_fingerprint: plan.source_cut.source_state_fingerprint.clone(),
        };
        if !Self::content_handoff_matches_plan(&handoff, &plan.expected_handoff, &source_fence)
            || !Self::content_plan_matches_source(&plan, source)
        {
            return Err(error(
                "retained confirmation is not bound to this exact native import",
            ));
        }
        Ok((cfg, Some(data)))
    }

    /// Compare actual DATA without discarding an acknowledged source fence.
    /// Only the exact native plan's unfenced-to-fenced progress is permitted.
    pub(super) fn content_handoff_matches_plan(
        handoff: &sinomo_dfs::DfsMetadataShardMembershipHandoff,
        expected: &sinomo_dfs::DfsMetadataShardMembershipHandoff,
        source_fence: &sinomo_dfs::DfsMetadataShardMembershipHandoffFence,
    ) -> bool {
        if expected.retiring_primary_fence.is_some()
            && handoff.retiring_primary_fence != expected.retiring_primary_fence
        {
            return false;
        }
        if handoff
            .retiring_primary_fence
            .as_ref()
            .is_some_and(|actual| actual != source_fence)
        {
            return false;
        }
        // Whole-record equality preserves every other field, including the
        // required floors. This temporary comparison never changes stored DATA.
        let mut immutable = handoff.clone();
        immutable.retiring_primary_fence = expected.retiring_primary_fence.clone();
        immutable == *expected
    }

    fn content_plan_matches_source(
        plan: &MetadataImportPlan,
        source: &MetadataSourceContentWorkScope,
    ) -> bool {
        let cut = &plan.source_cut;
        cut.source_identity == source.source_identity
            && cut.source_snapshot_fingerprint == source.source_snapshot_fingerprint
            && cut.source_journal_sequence == source.source_journal_sequence
            && cut.scope_shard == source.scope_shard
            && cut.file_system_ids == source.file_system_ids
            && cut.projection_assignments == source.projection_assignments
            && cut.original_handoff == source.original_handoff
    }

    pub(super) fn authorize_content_verification(
        &self,
        caller: &sinomo_core::node::NodeId,
        request: &ContentVerificationRequest,
    ) -> Result<()> {
        if request.request_id != request.binding()?
            || request.object.file_system_id != request.version.file_system_id
            || request.object.object_id != request.version.object_id
            || !request
                .source
                .file_system_ids
                .contains(&request.version.file_system_id)
            || request.authority.file_system_id != request.version.file_system_id
            || request.authority.shard_id != request.source.scope_shard.0
            || !request
                .source
                .projection_assignments
                .contains(&request.authority)
        {
            return Err(error(
                "retained verification request changed its original member/scope binding",
            ));
        }
        let keys = [
            sinomo_dfs::metadata_shard_key(
                &request.version.file_system_id,
                &request.source.scope_shard.0,
            ),
            sinomo_dfs::metadata_shard_membership_handoff_key(
                &request.version.file_system_id,
                &request.source.scope_shard.0,
            ),
        ];
        let (entries, _) = self.read_exact(&keys)?;
        let current: DfsMetadataShardConfig = entries[0]
            .entry
            .as_ref()
            .ok_or_else(|| error("retained verification has no current native scope"))?
            .decode()
            .map_err(error)?;
        if current != request.authority || &current.primary_node_id != caller {
            return Err(error(
                "retired or foreign caller cannot verify source content",
            ));
        }
        match (&entries[1].entry, &request.import_plan) {
            (None, None) if request.source.original_handoff.is_none() => {}
            (Some(entry), None)
                if Self::matches_original_pending_byte_scope(&request.source, &current, entry)? => {
            }
            (Some(entry), Some(plan)) => {
                let plan = &plan.plan;
                let handoff: sinomo_dfs::DfsMetadataShardMembershipHandoff =
                    entry.decode().map_err(error)?;
                let source_fence = sinomo_dfs::DfsMetadataShardMembershipHandoffFence {
                    handoff_id: plan.expected_handoff.handoff_id.clone(),
                    primary_node_id: plan.expected_handoff.retiring_shard.primary_node_id.clone(),
                    journal_sequence: plan.source_cut.source_journal_sequence,
                    state_fingerprint: plan.source_cut.source_state_fingerprint.clone(),
                };
                if !Self::content_handoff_matches_plan(
                    &handoff,
                    &plan.expected_handoff,
                    &source_fence,
                ) || !Self::content_plan_matches_source(plan, &request.source)
                {
                    return Err(error(
                        "retained verification crosses an unrelated pending handoff",
                    ));
                }
            }
            _ => {
                return Err(error(
                    "retained verification has no exact current handoff context",
                ));
            }
        }
        if request.object.policy == sinomo_core::DfsObjectPolicy::LocalOwned
            && (request.export_bytes || request.object.owner_node_id != self.local_node()?)
        {
            return Err(error(
                "local-owned import can only confirm bytes at its actual owner",
            ));
        }
        Ok(())
    }

    pub(super) fn resolve_content_verification(
        &self,
        caller: &sinomo_core::node::NodeId,
        mut wire: ContentVerificationWire,
    ) -> Result<ResolvedVerification> {
        if wire.wire_version != CONTENT_VERIFICATION_WIRE_VERSION {
            return Err(error("unsupported retained verification wire version"));
        }
        if *self.lifecycle.borrow() || self.lifecycle.has_changed().is_err() {
            return Err(error(
                "retained verification data-owner lifecycle is closed",
            ));
        }
        if let Some(ContentVerificationPlanEnvelope::Reference {
            plan_digest,
            receiver_context,
        }) = &wire.import_plan
        {
            let requested = verification_context::PlanAcknowledgement {
                plan_digest: plan_digest.clone(),
                receiver_context: receiver_context.clone(),
            };
            let scope = verification_context::wire_scope(caller, &wire);
            let data = self
                .content_plan_contexts
                .lock()
                .map_err(|_| error("retained verification plan DATA cache is poisoned"))?
                .receiver(&scope, &requested);
            let Some(data) = data else {
                // The routed request arrived, but no authority/content/byte
                // verification has begun. This is a typed no-effect DATA miss,
                // never NetworkDeliveryState::DefinitelyNotSent or a receipt.
                return Ok(ResolvedVerification::ContextMissing(
                    ContentVerificationResult::ContextMissing {
                        request_id: wire.request_id,
                        holder_node_id: self.local_node()?,
                        file_system_id: wire.version.file_system_id,
                        scope_shard: wire.source.scope_shard,
                        requested,
                    },
                ));
            };
            wire.import_plan = None;
            let mut request = ContentVerificationRequest::try_from(wire)?;
            request.import_plan = Some(data);
            request.plan_acknowledgement = Some(requested);
            if request.request_id != request.binding()? {
                return Err(error(
                    "retained plan reference changes its exact request binding",
                ));
            }
            return Ok(ResolvedVerification::Request(request));
        }
        Ok(ResolvedVerification::Request(
            ContentVerificationRequest::try_from(wire)?,
        ))
    }

    pub(super) fn acknowledge_content_verification_plan(
        &self,
        caller: &sinomo_core::node::NodeId,
        request: &ContentVerificationRequest,
    ) -> Result<Option<verification_context::PlanAcknowledgement>> {
        if *self.lifecycle.borrow() || self.lifecycle.has_changed().is_err() {
            return Err(error(
                "retained verification data-owner lifecycle is closed",
            ));
        }
        match request.import_plan.as_ref() {
            Some(data) => Ok(Some(
                self.content_plan_contexts
                    .lock()
                    .map_err(|_| error("retained verification plan DATA cache is poisoned"))?
                    .publish(
                        verification_context::scope(caller, request),
                        Arc::clone(data),
                    ),
            )),
            None => Ok(None),
        }
    }

    fn content_verification_request(
        &self,
        source: &MetadataSourceContentWorkScope,
        object: &sinomo_dfs::DfsObjectMetadata,
        version: &sinomo_dfs::DfsObjectVersionRecord,
        locator: &MetadataSourceContentLocator,
        export_bytes: bool,
    ) -> Result<ContentVerificationRequest> {
        let evidence = crate::metadata::lookup_owned_metadata_source_version(
            source,
            &version.file_system_id,
            &version.object_id,
            &version.version,
        )?
        .ok_or_else(|| error("retained bytework has no independently verified original member"))?;
        if evidence.object != *object
            || evidence.locator != *locator
            || evidence.version.content_hash != version.content_hash
            || evidence.version.size_bytes != version.size_bytes
        {
            return Err(error(
                "retained bytework differs from its original row witness",
            ));
        }
        let (authority, import_plan) =
            self.content_work_authority(source, &version.file_system_id)?;
        let mut request = ContentVerificationRequest {
            request_id: String::new(),
            source: source.clone(),
            object: object.clone(),
            version: version.clone(),
            locator: locator.clone(),
            authority,
            import_plan,
            plan_acknowledgement: None,
            export_bytes,
        };
        request.request_id = request.binding()?;
        Ok(request)
    }

    fn confirm_or_read_content(
        &self,
        holder: &sinomo_core::node::NodeId,
        request: &ContentVerificationRequest,
        content: &sinomo_dfs::LocalContentStore,
    ) -> Result<()> {
        let options = source_effect_rpc_options();
        let started = std::time::Instant::now();
        let scope = verification_context::scope(holder, request);
        let mut sent = request.clone();
        sent.plan_acknowledgement = match request.import_plan.as_ref() {
            Some(data) => self
                .content_plan_contexts
                .lock()
                .map_err(|_| error("retained verification plan DATA cache is poisoned"))?
                .acknowledged(&scope, &data.digest),
            None => None,
        };
        let mut result = self.call_content_verification(holder, &sent, options)?;
        let mut outcome: ContentVerificationResult =
            result.response.decode_body().map_err(error)?;
        if let ContentVerificationResult::ContextMissing {
            request_id,
            holder_node_id,
            file_system_id,
            scope_shard,
            requested,
        } = &outcome
        {
            if result.response.route != staged_service::CONTENT_VERIFY_ROUTE
                || request_id != &request.request_id
                || holder_node_id != holder
                || file_system_id != &request.version.file_system_id
                || scope_shard != &request.source.scope_shard
                || sent.plan_acknowledgement.as_ref() != Some(requested)
                || !result.body.is_empty()
            {
                return Err(error(
                    "retained plan DATA miss changes its exact no-effect request/context",
                ));
            }
            self.content_plan_contexts
                .lock()
                .map_err(|_| error("retained verification plan DATA cache is poisoned"))?
                .forget(&scope, requested);
            sent.plan_acknowledgement = None;
            // Exactly one full definition after a genuine typed no-effect miss.
            // Preserve the original whole-call budget, including None; transport
            // health/defaults and the actual per-version checks are unchanged.
            let remaining_options = match options.overall_timeout() {
                Some(budget) => {
                    let remaining = budget.checked_sub(started.elapsed())
                        .filter(|remaining| !remaining.is_zero())
                        .ok_or_else(|| error("retained verification original deadline elapsed before plan definition"))?;
                    options.with_overall_timeout(remaining)
                }
                None => options,
            };
            result = self.call_content_verification(holder, &sent, remaining_options)?;
            outcome = result.response.decode_body().map_err(error)?;
        }
        let ContentVerificationResult::Verified {
            verification: verified,
            plan_context,
        } = outcome
        else {
            return Err(error(
                "full retained verification definition did not produce an actual verified result",
            ));
        };
        match (&request.import_plan, &plan_context) {
            (Some(data), Some(ack))
                if ack.plan_digest == data.digest && !ack.receiver_context.is_empty() => {}
            (None, None) => {}
            _ => {
                return Err(error(
                    "actual verified result changes its acknowledged immutable plan context",
                ));
            }
        }
        let version = &request.version;
        if result.response.route != staged_service::CONTENT_VERIFY_ROUTE
            || verified.request_id != request.request_id
            || &verified.holder_node_id != holder
            || verified.file_system_id != version.file_system_id
            || verified.object_id != version.object_id
            || verified.version != version.version
            || Some(&verified.content_hash) != version.content_hash.as_ref()
            || Some(verified.size_bytes) != version.size_bytes
        {
            return Err(error(
                "retained content response changes its exact confirmed tuple",
            ));
        }
        if request.export_bytes {
            let mut ingest = content
                .begin_quarantined_blob_ingest(verified.size_bytes, Some(&verified.content_hash))
                .map_err(error)?;
            while let Some(chunk) = block_native(&self.runtime, result.body.next_chunk())? {
                ingest.write_chunk(chunk.as_ref()).map_err(error)?;
            }
            let blob = ingest.finish_quarantined().map_err(error)?;
            content
                .bind_quarantined_blob(version.object_id.as_str(), version.version.as_str(), blob)
                .map_err(error)?;
        } else if !result.body.is_empty() {
            return Err(error(
                "local-owned confirmation unexpectedly exported bytes",
            ));
        }
        if let Some(ack) = plan_context {
            self.content_plan_contexts
                .lock()
                .map_err(|_| error("retained verification plan DATA cache is poisoned"))?
                .acknowledge(scope, ack);
        }
        Ok(())
    }
    fn call_content_verification(
        &self,
        holder: &sinomo_core::node::NodeId,
        request: &ContentVerificationRequest,
        options: sinomo_core::network::ClusterObjectRpcOptions,
    ) -> Result<sinomo_core::network::ObjectRpcResult> {
        let rpc = sinomo_core::network::ObjectRpcRequest::encode_body(
            staged_service::CONTENT_VERIFY_ROUTE,
            "dfs-migration",
            request,
        )
        .map_err(error)?;
        let transport = self
            .transport
            .as_ref()
            .ok_or_else(|| error("retained bytework has no native transport"))?;
        let mut result = block_native(
            &self.runtime,
            transport.call_async(
                holder,
                sinomo_core::network::ObjectRpcCall::value(rpc),
                options,
                true,
            ),
        )?;
        block_native(&self.runtime, result.materialize_value_body(64 * 1024))?;
        Ok(result)
    }
    pub(super) fn establish_retained_content(
        &self,
        source: &MetadataSourceContentWorkScope,
        object: &sinomo_dfs::DfsObjectMetadata,
        version: &sinomo_dfs::DfsObjectVersionRecord,
        locator: &MetadataSourceContentLocator,
        policy: sinomo_dfs::DfsReplicationPolicy,
        targets: Vec<sinomo_core::node::NodeId>,
    ) -> Result<MetadataImportedRetainedContent> {
        self.validate_source_root(&source.source_root)?;
        let _source_fence = crate::metadata::retain_source_fence(&source.source_root)?;
        crate::metadata::validate_retained_source_identity(
            &source.source_root,
            &source.source_identity,
            &source.source_snapshot_fingerprint,
        )?;
        if object.file_system_id != version.file_system_id
            || object.object_id != version.object_id
            || !source.file_system_ids.contains(&version.file_system_id)
            || !matches!(
                object.kind,
                sinomo_dfs::DfsObjectKind::File
                    | sinomo_dfs::DfsObjectKind::Artifact
                    | sinomo_dfs::DfsObjectKind::Symlink
                    | sinomo_dfs::DfsObjectKind::Tombstone
            )
        {
            return Err(error(
                "retained content work changes its actual source identity",
            ));
        }
        sinomo_dfs::validate_replication_plan(&targets, &policy).map_err(error)?;
        let confirmation =
            self.content_verification_request(source, object, version, locator, false)?;
        let cfg = confirmation.authority.clone();
        let local = self.local_node()?;
        if object.policy == sinomo_core::DfsObjectPolicy::LocalOwned
            && (targets != vec![object.owner_node_id.clone()]
                || policy.min_commit_replicas != 1
                || policy.desired_replicas != 1)
        {
            return Err(error(
                "local-owned retained content requires its actual owner and W1/N1",
            ));
        }
        let content_root = self
            .metadata_root
            .parent()
            .ok_or_else(|| error("retained content source lacks its data-owned DFS root"))?
            .join("content");
        let content = sinomo_dfs::LocalContentStore::new(content_root).map_err(error)?;
        if object.policy != sinomo_core::DfsObjectPolicy::LocalOwned
            || object.owner_node_id == local
        {
            if let Err(local_error) = verify_local_retained_content(&content, version, locator) {
                if object.policy == sinomo_core::DfsObjectPolicy::LocalOwned {
                    return Err(local_error);
                }
                let read =
                    self.content_verification_request(source, object, version, locator, true)?;
                let holders = version
                    .locations
                    .iter()
                    .chain(object.current_locations.iter())
                    .chain(std::iter::once(&object.owner_node_id))
                    .chain(targets.iter())
                    .filter(|node| *node != &local)
                    .cloned()
                    .collect::<BTreeSet<_>>();
                let mut recovered = false;
                let mut cause = local_error;
                for holder in holders {
                    match self.confirm_or_read_content(&holder, &read, &content) {
                        Ok(()) => {
                            recovered = true;
                            break;
                        }
                        Err(error_value) => cause = error_value,
                    }
                }
                if !recovered {
                    return Err(cause);
                }
            }
        }
        let hash = version
            .content_hash
            .as_deref()
            .ok_or_else(|| error("retained work has no actual source hash"))?;
        let size = version
            .size_bytes
            .ok_or_else(|| error("retained work has no actual source length"))?;
        let mut receipts = Vec::new();
        let receipt_targets = std::iter::once(&local)
            .filter(|node| targets.contains(node))
            .chain(targets.iter().filter(|node| *node != &local));
        for target in receipt_targets {
            if receipts.len() >= policy.min_commit_replicas {
                break;
            }
            if *target != local {
                let confirmed = self.confirm_or_read_content(target, &confirmation, &content);
                if confirmed.is_ok() {
                    receipts.push(sinomo_dfs::DfsReplicaReceiptRecord {
                        receipt: sinomo_core::DfsWriteReceipt {
                            file_system_id: version.file_system_id.clone(),
                            object_id: version.object_id.clone(),
                            committed_version: version.version.clone(),
                            owner_node_id: local.clone(),
                            event_id: sinomo_core::events::EventId::generate(),
                        },
                        replica_node_id: target.clone(),
                        content_hash: Some(hash.to_owned()),
                        received_at: time::OffsetDateTime::now_utc(),
                    });
                    continue;
                }
                if object.policy == sinomo_core::DfsObjectPolicy::LocalOwned
                    || confirmation.import_plan.is_some()
                {
                    return Err(confirmed.expect_err("confirmation failed"));
                }
                let request_id = digest(&(
                    "retained-content-import-v1",
                    &source.source_identity,
                    &version.file_system_id,
                    &version.object_id,
                    &version.version,
                    hash,
                    &policy,
                    &targets,
                    target,
                ))?;
                let intent_id = format!("storage-import-content:{request_id}");
                let clock = blake3::hash(request_id.as_bytes());
                let frame_id = u64::from_le_bytes(
                    clock.as_bytes()[..8]
                        .try_into()
                        .expect("BLAKE3 identity prefix"),
                );
                let command = sinomo_dfs::DfsMessageCommand {
                    request_id: request_id.clone(),
                    routed_message: sinomo_dfs::DfsRoutedMessage {
                        source_node_id: local.clone(),
                        target_node_id: target.clone(),
                        frame_id,
                        message_id: frame_id,
                        message: sinomo_dfs::DfsNetworkMessage::StageRetainedBlob(
                            sinomo_dfs::DfsStageBlobCommand {
                                request_id: request_id.clone(),
                                intent_id: intent_id.clone(),
                                file_system_id: version.file_system_id.clone(),
                                object_id: version.object_id.clone(),
                                expected_base_version: None,
                                version: version.version.clone(),
                                content_hash: hash.to_owned(),
                                bytes: Vec::new(),
                                object_metadata: Some(object.clone()),
                                authority_shard: Some(cfg.clone()),
                            },
                        ),
                    },
                };
                let request = sinomo_core::network::ObjectRpcRequest::encode_body(
                    staged_service::CONTENT_STAGE_ROUTE,
                    "dfs-migration",
                    &staged_service::ContentStageRequest {
                        command,
                        size_bytes: size,
                    },
                )
                .map_err(error)?;
                let (_, reader) = content
                    .open_object_version_stream(
                        version.object_id.as_str(),
                        version.version.as_str(),
                    )
                    .map_err(error)?;
                let body =
                    sinomo_core::network::ObjectRpcBodySource::from_reader(reader, Some(size));
                let call = sinomo_core::network::ObjectRpcCall::new(request, body);
                let transport = self
                    .transport
                    .as_ref()
                    .ok_or_else(|| error("retained import has no actual native transport"))?;
                let result = block_native(
                    &self.runtime,
                    transport.call_async(target, call, source_effect_rpc_options(), true),
                )?;
                let response = block_native(
                    &self.runtime,
                    result.into_materialized_value_response(64 * 1024),
                )?;
                let response: sinomo_dfs::DfsMessageResponse =
                    response.decode_body().map_err(error)?;
                if response.request_id != request_id {
                    return Err(error("retained-content reply changes request identity"));
                }
                let mut confirmed = false;
                for message in response.outbound_messages {
                    if let sinomo_dfs::DfsNetworkMessage::StageBlobResponse(reply) = message.message
                        && message.source_node_id == *target
                        && message.target_node_id == local
                        && reply.request_id == request_id
                        && reply.intent_id == intent_id
                        && reply.file_system_id == version.file_system_id
                        && reply.object_id == version.object_id
                        && reply.version == version.version
                        && reply.replica_node_id == *target
                        && matches!(reply.result, sinomo_dfs::DfsBlobTransferResult::Durable { content_hash } if content_hash == hash)
                    {
                        if confirmed {
                            return Err(error("retained-content reply repeats a durable receipt"));
                        }
                        confirmed = true;
                    }
                }
                if !confirmed {
                    return Err(error(
                        "retained-content import lacks the actual correlated durable receiver result",
                    ));
                }
            }
            receipts.push(sinomo_dfs::DfsReplicaReceiptRecord {
                receipt: sinomo_core::DfsWriteReceipt {
                    file_system_id: version.file_system_id.clone(),
                    object_id: version.object_id.clone(),
                    committed_version: version.version.clone(),
                    owner_node_id: local.clone(),
                    event_id: sinomo_core::events::EventId::generate(),
                },
                replica_node_id: target.clone(),
                content_hash: Some(hash.to_owned()),
                received_at: time::OffsetDateTime::now_utc(),
            });
        }
        if receipts.len() < policy.min_commit_replicas {
            return Err(error(
                "retained-content import did not establish its actual W",
            ));
        }
        let (confirmed_authority, confirmed_plan) =
            self.content_work_authority(source, &version.file_system_id)?;
        if confirmed_authority != cfg
            || confirmed_plan.as_ref().map(|data| &data.digest)
                != confirmation.import_plan.as_ref().map(|data| &data.digest)
        {
            return Err(error(
                "retained bytework authority changed before its actual receipts completed",
            ));
        }
        Ok(MetadataImportedRetainedContent {
            durability: sinomo_dfs::DfsContentDurability {
                source_version: version.version.clone(),
                coordinator_node_id: local,
                owner_epoch: cfg.epoch,
                desired_replica_node_ids: targets,
                policy,
            },
            receipts,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::node::NodeId;

    fn captured_version(
        manifest: &sinomo_dfs::ContentManifest,
        locator: &MetadataSourceContentLocator,
    ) -> sinomo_dfs::DfsObjectVersionRecord {
        sinomo_dfs::DfsObjectVersionRecord {
            file_system_id: sinomo_dfs::DfsFileSystemId::generate(),
            object_id: sinomo_dfs::DfsObjectId::from_legacy_named(&locator.object_id).unwrap(),
            version: sinomo_dfs::DfsVersion::from_legacy_named(&locator.version).unwrap(),
            content_hash: Some(manifest.content_hash.clone()),
            size_bytes: Some(manifest.size_bytes),
            locations: Vec::new(),
            created_by_event_id: None,
            created_at: time::OffsetDateTime::now_utc(),
            visible: true,
            retention: BTreeMap::new(),
            module_metadata: BTreeMap::new(),
            content_durability: None,
        }
    }

    // Ordinary typed codec DATA only. No native request/authority is executed.
    fn verification_codec_request(plan: Option<MetadataImportPlan>) -> ContentVerificationRequest {
        let fs = plan
            .as_ref()
            .map(|p| p.expected_handoff.retiring_shard.file_system_id.clone())
            .unwrap_or_else(sinomo_dfs::DfsFileSystemId::generate);
        let owner = plan
            .as_ref()
            .map(|p| p.expected_handoff.retiring_shard.primary_node_id.clone())
            .unwrap_or_else(sinomo_core::node::NodeId::generate);
        let object_id = sinomo_dfs::DfsObjectId::generate();
        let version_id = sinomo_dfs::DfsVersion::generate();
        let object: sinomo_dfs::DfsObjectMetadata = serde_json::from_value(serde_json::json!({
            "object_id":object_id,"file_system_id":fs,"path":"/codec-DATA",
            "kind":sinomo_dfs::DfsObjectKind::File,"tombstone_provenance":null,"owner_node_id":owner,
            "current_locations":[],"content_hash":null,"content_size_bytes":null,
            "link_target_object_id":null,"mode":420,"uid":0,"gid":0,
            "accessed_at_unix_nanos":0,"modified_at_unix_nanos":0,
            "created_at_unix_nanos":0,"changed_at_unix_nanos":0,"version":version_id,
            "policy":sinomo_core::DfsObjectPolicy::ReplicatedEventual,"created_by_event_id":null,"last_modified_by_event_id":null
        })).unwrap();
        let version = sinomo_dfs::DfsObjectVersionRecord {
            file_system_id: fs.clone(),
            object_id: object_id.clone(),
            version: version_id.clone(),
            content_hash: None,
            size_bytes: None,
            locations: Vec::new(),
            created_by_event_id: None,
            created_at: time::OffsetDateTime::UNIX_EPOCH,
            visible: true,
            retention: BTreeMap::new(),
            module_metadata: BTreeMap::new(),
            content_durability: None,
        };
        let authority = plan
            .as_ref()
            .map(|p| p.expected_handoff.retiring_shard.clone())
            .unwrap_or(DfsMetadataShardConfig {
                file_system_id: fs.clone(),
                shard_id: "root".into(),
                primary_node_id: owner,
                replica_node_ids: Vec::new(),
                epoch: 1,
                assignment: sinomo_dfs::DfsMetadataShardAssignment::Root,
                default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
            });
        let source = if let Some(p) = &plan {
            let c = &p.source_cut;
            MetadataSourceContentWorkScope {
                source_root: c.source_root.clone(),
                source_identity: c.source_identity.clone(),
                source_snapshot_fingerprint: c.source_snapshot_fingerprint.clone(),
                source_journal_sequence: c.source_journal_sequence,
                scope_shard: c.scope_shard.clone(),
                file_system_ids: c.file_system_ids.clone(),
                projection_assignments: c.projection_assignments.clone(),
                original_handoff: c.original_handoff.clone(),
            }
        } else {
            MetadataSourceContentWorkScope {
                source_root: PathBuf::from("/saved-codec-DATA"),
                source_identity: "codec-only".into(),
                source_snapshot_fingerprint: "codec-only".into(),
                source_journal_sequence: 0,
                scope_shard: sinomo_dfs::DfsMetadataShardId::new("root"),
                file_system_ids: vec![fs],
                projection_assignments: vec![authority.clone()],
                original_handoff: None,
            }
        };
        let mut request = ContentVerificationRequest {
            request_id: String::new(),
            source,
            object,
            version,
            locator: MetadataSourceContentLocator {
                object_id: object_id.to_string(),
                version: version_id.to_string(),
                content_version: None,
            },
            authority,
            import_plan: plan
                .map(|plan| verification_context::ValidatedPlanData::new(plan).unwrap()),
            plan_acknowledgement: None,
            export_bytes: false,
        };
        request.request_id = request.binding().unwrap();
        request
    }
    fn verification_original_data(request: &ContentVerificationRequest) -> serde_json::Value {
        serde_json::to_value((
            &request.request_id,
            &request.source,
            &request.object,
            &request.version,
            &request.locator,
            &request.authority,
            request.import_plan.as_ref().map(|data| &data.plan),
            request.export_bytes,
        ))
        .unwrap()
    }
    #[test]
    fn current_verification_wire_requires_version_and_all_nullable_fields() {
        let request = verification_codec_request(None);
        let wire = serde_json::to_value(&request).unwrap();
        let bytes = sinomo_core::object::serde_ext::to_bytes(
            &request,
            1,
            sinomo_core::object::LengthMode::U32,
        )
        .unwrap();
        let decoded: ContentVerificationRequest =
            sinomo_core::object::serde_ext::from_bytes(&bytes).unwrap();
        assert_eq!(
            verification_original_data(&request),
            verification_original_data(&decoded)
        );
        assert_eq!(request.request_id, decoded.binding().unwrap());
        for field in [
            "wire_version",
            "request_id",
            "source",
            "object",
            "version",
            "locator",
            "authority",
            "import_plan",
            "export_bytes",
        ] {
            let mut missing = wire.clone();
            missing.as_object_mut().unwrap().remove(field);
            assert!(
                serde_json::from_value::<ContentVerificationRequest>(missing).is_err(),
                "missing {field}"
            );
        }
        let mut old = wire.clone();
        old.as_object_mut().unwrap().remove("wire_version");
        assert!(serde_json::from_value::<ContentVerificationRequest>(old).is_err());
        let mut wrong = wire.clone();
        wrong["wire_version"] = serde_json::json!(1);
        assert!(serde_json::from_value::<ContentVerificationRequest>(wrong).is_err());
        let mut extra = wire;
        extra["unrelated-format"] = serde_json::json!(true);
        assert!(serde_json::from_value::<ContentVerificationRequest>(extra).is_err());
    }
    #[test]
    #[ignore = "requires the exact saved actual r439 native plan DATA; no authority/RPC"]
    fn actual_retained_verification_wire_restores_full_plan_and_rejects_changed_data() {
        let input = std::env::var("SINOMO_MIGRATION_CONTENT_VERIFICATION_PLAN")
            .expect("operator must supply actual saved plan");
        let plan: MetadataImportPlan =
            serde_json::from_slice(&std::fs::read(input).unwrap()).unwrap();
        assert_eq!(
            plan.pending_operation_mapping,
            plan.source_cut.pending_operations
        );
        let request = verification_codec_request(Some(plan));
        let original = verification_original_data(&request);
        let wire = serde_json::to_value(&request).unwrap();
        assert!(
            wire["import_plan"]["plan"]
                .get("pending_operation_mapping")
                .is_none()
        );
        let bytes = sinomo_core::object::serde_ext::to_bytes(
            &request,
            1,
            sinomo_core::object::LengthMode::U32,
        )
        .unwrap();
        let decoded: ContentVerificationRequest =
            sinomo_core::object::serde_ext::from_bytes(&bytes).unwrap();
        assert_eq!(verification_original_data(&decoded), original);
        assert_eq!(decoded.request_id, decoded.binding().unwrap());
        assert_eq!(serde_json::to_value(&decoded).unwrap(), wire);
        assert_eq!(
            sinomo_core::object::serde_ext::to_bytes(
                &decoded,
                1,
                sinomo_core::object::LengthMode::U32
            )
            .unwrap(),
            bytes
        );
        let mut unequal = request.clone();
        Arc::make_mut(unequal.import_plan.as_mut().unwrap())
            .plan
            .facts
            .pending_operation_mapping
            .push(serde_json::json!({"different":true}));
        assert!(
            sinomo_core::object::serde_ext::to_bytes(
                &unequal,
                1,
                sinomo_core::object::LengthMode::U32
            )
            .is_err()
        );
        for field in [
            "mapping_uses_source_pending",
            "expected_assignments",
            "desired_assignments",
            "target_exact_prefix",
            "current_image_fingerprint",
            "combined_layout_source_fences",
            "source_cut",
            "captured_entries",
            "captured_authority",
            "stage_request",
            "expected_handoff",
            "activation_request",
        ] {
            let mut missing = wire.clone();
            missing["import_plan"]["plan"]
                .as_object_mut()
                .unwrap()
                .remove(field);
            assert!(
                serde_json::from_value::<ContentVerificationRequest>(missing).is_err(),
                "missing plan {field}"
            );
        }
        let mut extra_mapping = wire.clone();
        extra_mapping["import_plan"]["plan"]["pending_operation_mapping"] = serde_json::json!([]);
        assert!(serde_json::from_value::<ContentVerificationRequest>(extra_mapping).is_err());
        for field in [
            "request_id",
            "source",
            "object",
            "version",
            "locator",
            "authority",
            "export_bytes",
        ] {
            let mut changed = wire.clone();
            match field {
                "request_id" => changed[field] = serde_json::json!("changed-binding"),
                "source" => changed[field]["source_identity"] = serde_json::json!("changed-scope"),
                "object" => changed[field]["path"] = serde_json::json!("/changed-path"),
                "version" => changed[field]["visible"] = serde_json::json!(false),
                "locator" => changed[field]["object_id"] = serde_json::json!("changed-locator"),
                "authority" => changed[field]["epoch"] = serde_json::json!(99),
                _ => changed[field] = serde_json::json!(true),
            }
            let changed: ContentVerificationRequest = serde_json::from_value(changed).unwrap();
            assert_ne!(
                changed.request_id,
                changed.binding().unwrap(),
                "changed original field {field}"
            );
        }
        for alias in [
            serde_json::json!(false),
            serde_json::json!(null),
            serde_json::json!("true"),
        ] {
            let mut changed = wire.clone();
            changed["import_plan"]["plan"]["mapping_uses_source_pending"] = alias;
            assert!(serde_json::from_value::<ContentVerificationRequest>(changed).is_err());
        }
        for field in [
            "expected_assignments",
            "desired_assignments",
            "target_exact_prefix",
            "current_image_fingerprint",
            "combined_layout_source_fences",
            "source_cut",
            "captured_entries",
            "captured_authority",
            "stage_request",
            "expected_handoff",
            "activation_request",
        ] {
            let mut changed = wire.clone();
            let value = &mut changed["import_plan"]["plan"][field];
            match field {
                "expected_assignments" | "desired_assignments" => {
                    value[0]["epoch"] = serde_json::json!(99)
                }
                "target_exact_prefix" => value["digest"] = serde_json::json!("changed-prefix"),
                "current_image_fingerprint" => *value = serde_json::json!("changed-image"),
                "combined_layout_source_fences" => *value = serde_json::json!([{"changed":true}]),
                "source_cut" => value["source_identity"] = serde_json::json!("changed-source"),
                "captured_entries" => value[0]["key"] = serde_json::json!("/changed-key"),
                "captured_authority" => value["term"] = serde_json::json!(99),
                "stage_request" => {
                    value["request_id"] = serde_json::to_value(KvRequestId::derived(
                        "changed-codec-request",
                        b"codec-DATA",
                    ))
                    .unwrap()
                }
                "expected_handoff" => value["required_prepared_sequence"] = serde_json::json!(0),
                "activation_request" => {
                    *value = wire["import_plan"]["plan"]["stage_request"].clone()
                }
                _ => unreachable!(),
            }
            assert!(
                serde_json::from_value::<ContentVerificationRequest>(changed).is_err(),
                "changed full plan field {field}"
            );
        }
        let mut changed = wire.clone();
        changed["import_plan"]["plan"]["source_cut"]["pending_operations"][0]["changed-row-DATA"] =
            serde_json::json!(true);
        assert!(serde_json::from_value::<ContentVerificationRequest>(changed).is_err());
        // Compare ordinary original full request DATA, not a shortened request.
        #[derive(Serialize)]
        struct OriginalWire<'a> {
            request_id: &'a str,
            source: &'a MetadataSourceContentWorkScope,
            object: &'a sinomo_dfs::DfsObjectMetadata,
            version: &'a sinomo_dfs::DfsObjectVersionRecord,
            locator: &'a MetadataSourceContentLocator,
            authority: &'a DfsMetadataShardConfig,
            import_plan: Option<&'a MetadataImportPlan>,
            export_bytes: bool,
        }
        let full_request_wire = OriginalWire {
            request_id: &request.request_id,
            source: &request.source,
            object: &request.object,
            version: &request.version,
            locator: &request.locator,
            authority: &request.authority,
            import_plan: request.import_plan.as_ref().map(|data| &data.plan),
            export_bytes: request.export_bytes,
        };
        let original_bytes = sinomo_core::object::serde_ext::to_bytes(
            &full_request_wire,
            1,
            sinomo_core::object::LengthMode::U32,
        )
        .unwrap();
        assert!(bytes.len() < original_bytes.len());
        println!(
            "actual-plan verification codec original_sobj={} current_sobj={} full_rows={} binding={}",
            original_bytes.len(),
            bytes.len(),
            request
                .import_plan
                .as_ref()
                .unwrap()
                .plan
                .source_cut
                .pending_operations
                .len(),
            request.request_id
        );
    }

    #[test]
    fn native_verification_plan_context_uses_normal_route_and_expires_on_receiver_restart() {
        use crate::authority::placement_work::native_tests::{
            canonical_migration_runtime, native_migration_cluster,
        };
        use crate::metadata::{MetadataImportAuthority, MetadataSourceCut};
        let (mut native, nodes) = native_migration_cluster("migration-wire2-owner-context", 2);
        let primary = nodes[0].clone();
        let peer = nodes[1].clone();
        let context = native.component_context(&primary).unwrap();
        let runtime = canonical_migration_runtime(&context);
        let fs = sinomo_dfs::sifs_file_system_id();
        let payload = b"actual Main immutable bytes for plan context";
        let options = sinomo_dfs::DfsWriteOptions {
            policy: sinomo_dfs::DfsReplicationPolicy::new(2, 2).unwrap(),
            replica_candidates: nodes.clone(),
            wait_for_commit: true,
            foreground_replica_repair: true,
        };
        let written = native
            .client(&primary)
            .unwrap()
            .posix_create_file("/verification-plan-context", payload, options)
            .unwrap();
        native
            .client(&primary)
            .unwrap()
            .posix_sync_file(
                "/verification-plan-context",
                sinomo_dfs::DfsPosixSyncKind::File,
            )
            .unwrap();
        let shard = sinomo_dfs::DfsMetadataShardId::new("root");
        let backend =
            sinomo_dfs::open_local_metadata_shard_backend(context.data_dir(), shard.clone())
                .unwrap();
        let image = backend.export_snapshot_image().unwrap();
        let snapshot = backend.export_table_snapshot();
        drop(backend);
        assert!(
            snapshot
                .tables
                .prepared_operations_v7
                .iter()
                .all(|row| row.status != sinomo_dfs::DfsPreparedOperationStatus::Prepared),
            "the actual source fixture must not omit hidden pending operations"
        );
        assert!(
            snapshot.tables.namespace_jobs.is_empty(),
            "the actual source fixture must not omit native job DATA"
        );
        let object = snapshot
            .tables
            .objects
            .iter()
            .find(|row| row.object_id == written.object_id)
            .unwrap()
            .clone();
        let version = snapshot
            .tables
            .versions
            .iter()
            .find(|row| row.object_id == object.object_id && row.version == object.version)
            .unwrap()
            .clone();
        assert_eq!(version.size_bytes, Some(payload.len() as u64));
        let stage = context
            .data_dir_root()
            .join("verification-plan-context-source");
        let source_root = stage.join("source");
        let current_image = stage.join("current-image");
        let source_image =
            sinomo_dfs::write_metadata_candidate_image(&source_root, &image, &fs).unwrap();
        let target =
            sinomo_dfs::write_metadata_candidate_image(&current_image, &image, &fs).unwrap();
        let _source_writer = crate::metadata::retain_source_fence(&source_root).unwrap();
        let (source_identity, source_snapshot_fingerprint) =
            crate::metadata::read_retained_source_identity(&source_root, None).unwrap();
        let assignments = runtime.authority.assignments(&[fs.clone()]).unwrap();
        let active = assignments
            .iter()
            .find(|cfg| cfg.shard_id == shard.0)
            .unwrap()
            .clone();
        assert_eq!(active.primary_node_id, primary);
        let pending_key = sinomo_dfs::metadata_shard_membership_handoff_key(&fs, &shard.0);
        assert!(
            runtime.authority.read_exact(&[pending_key]).unwrap().0[0]
                .entry
                .is_none()
        );
        let cut = MetadataSourceCut {
            source_root: source_root.clone(),
            source_identity,
            scope_shard: shard.clone(),
            file_system_ids: vec![fs.clone()],
            source_snapshot_fingerprint,
            source_format_version: Some(snapshot.format_version),
            source_exact_prefix: Some(source_image.prefix_proof.clone()),
            converted_image_fingerprint: target.state_fingerprint.clone(),
            current_image_fingerprint: target.state_fingerprint.clone(),
            target_exact_prefix: target.prefix_proof.clone(),
            source_journal_sequence: snapshot.materialized_through_sequence,
            source_state_fingerprint: source_image.prefix_proof.digest.clone(),
            projection_assignments: assignments.clone(),
            staged_root: current_image,
            pending_operations: Vec::new(),
            original_handoff: None,
        };
        let crate::metadata::MetadataImportPlan::Native(plan) =
            runtime.authority.plan(&cut).unwrap()
        else {
            panic!("actual native plan required");
        };
        let staged = runtime
            .authority
            .commit(plan.stage_request.clone())
            .unwrap();
        NativeImportAuthority::require_committed(&staged).unwrap();
        // A receiver restart must find the SAME actual import in its ordinary
        // staged receiver, with fresh floor ancestry and exact image receipts.
        let source_fence = sinomo_dfs::DfsMetadataShardMembershipHandoffFence {
            handoff_id: plan.expected_handoff.handoff_id.clone(),
            primary_node_id: primary.clone(),
            journal_sequence: cut.source_journal_sequence,
            state_fingerprint: cut.source_state_fingerprint.clone(),
        };
        let mut control = runtime.authority.control_client(&cut.staged_root).unwrap();
        let pending = DfsControlPlaneStore::new(&mut control)
            .acknowledge_metadata_shard_membership_handoff_fence(
                &plan.expected_handoff,
                &source_fence,
            )
            .unwrap()
            .expect("actual same source fence must commit");
        assert!(NativeImportAuthority::content_handoff_matches_plan(
            &pending,
            &plan.expected_handoff,
            &source_fence,
        ));
        for node in &nodes {
            runtime
                .authority
                .confirm_stage_node(
                    node,
                    sinomo_dfs::DfsMetadataSnapshotInstallCommand {
                        request_id: sinomo_core::Id::generate().to_string(),
                        source_node_id: primary.clone(),
                        file_system_id: Some(fs.clone()),
                        authority_epoch: Some(active.epoch),
                        primary_handoff_id: Some(pending.handoff_id.clone()),
                        shard_id: shard.clone(),
                        next_after_sequence: target.prefix_proof.journal_sequence,
                        snapshot: image.descriptor().clone(),
                        image: Some(image.clone()),
                        reason: "current storage import".into(),
                    },
                    &plan,
                )
                .unwrap();
        }
        let mut request = ContentVerificationRequest {
            request_id: String::new(),
            source: MetadataSourceContentWorkScope {
                source_root,
                source_identity: cut.source_identity.clone(),
                source_snapshot_fingerprint: cut.source_snapshot_fingerprint.clone(),
                source_journal_sequence: cut.source_journal_sequence,
                file_system_ids: cut.file_system_ids.clone(),
                scope_shard: shard,
                projection_assignments: assignments,
                original_handoff: None,
            },
            object: object.clone(),
            version: version.clone(),
            locator: MetadataSourceContentLocator {
                object_id: object.object_id.to_string(),
                version: version.version.to_string(),
                content_version: None,
            },
            authority: active,
            import_plan: Some(verification_context::ValidatedPlanData::new(plan).unwrap()),
            plan_acknowledgement: None,
            export_bytes: false,
        };
        request.request_id = request.binding().unwrap();
        let content =
            sinomo_dfs::LocalContentStore::new(context.data_dir().dfs_dir().join("content"))
                .unwrap();
        // Full normal verification, then the SAME product helper emits a scoped
        // reference. Both use the ordinary authenticated Main verifier route.
        runtime
            .authority
            .confirm_or_read_content(&peer, &request, &content)
            .unwrap();
        let scope = verification_context::scope(&peer, &request);
        let first = runtime
            .authority
            .content_plan_contexts
            .lock()
            .unwrap()
            .acknowledged(&scope, &request.import_plan.as_ref().unwrap().digest)
            .unwrap();
        runtime
            .authority
            .confirm_or_read_content(&peer, &request, &content)
            .unwrap();
        let mut reference = request.clone();
        reference.plan_acknowledgement = Some(first.clone());
        let rpc = sinomo_core::network::ObjectRpcRequest::encode_body(
            staged_service::CONTENT_VERIFY_ROUTE,
            "dfs-migration",
            &reference,
        )
        .unwrap();
        let reply: ContentVerificationResult = native
            .rpc_request(
                &primary,
                &peer,
                rpc,
                sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            )
            .unwrap()
            .decode_body()
            .unwrap();
        let ContentVerificationResult::Verified {
            verification,
            plan_context,
        } = reply
        else {
            panic!("actual scoped verification required");
        };
        assert_eq!(plan_context.as_ref(), Some(&first));
        assert_eq!(verification.object_id, version.object_id);
        assert_eq!(verification.version, version.version);
        assert_eq!(
            verification.content_hash,
            version.content_hash.clone().unwrap()
        );
        assert_eq!(verification.size_bytes, payload.len() as u64);
        // A cached plan is DATA only: another authenticated caller or shard
        // cannot borrow it, and a changed hash still reaches the real byte check.
        let rpc = sinomo_core::network::ObjectRpcRequest::encode_body(
            staged_service::CONTENT_VERIFY_ROUTE,
            "dfs-migration",
            &reference,
        )
        .unwrap();
        let foreign: ContentVerificationResult = native
            .rpc_request(
                &peer,
                &peer,
                rpc,
                sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            )
            .unwrap()
            .decode_body()
            .unwrap();
        assert!(matches!(
            foreign,
            ContentVerificationResult::ContextMissing { .. }
        ));
        let mut foreign_scope = reference.clone();
        foreign_scope.source.scope_shard = sinomo_dfs::DfsMetadataShardId::new("services");
        foreign_scope.request_id = foreign_scope.binding().unwrap();
        let rpc = sinomo_core::network::ObjectRpcRequest::encode_body(
            staged_service::CONTENT_VERIFY_ROUTE,
            "dfs-migration",
            &foreign_scope,
        )
        .unwrap();
        let foreign: ContentVerificationResult = native
            .rpc_request(
                &primary,
                &peer,
                rpc,
                sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            )
            .unwrap()
            .decode_body()
            .unwrap();
        assert!(matches!(
            foreign,
            ContentVerificationResult::ContextMissing { .. }
        ));
        let mut changed_hash = reference.clone();
        changed_hash.version.content_hash = Some(format!(
            "blake3:{}",
            blake3::hash(b"different actual bytes").to_hex()
        ));
        changed_hash.request_id = changed_hash.binding().unwrap();
        let rpc = sinomo_core::network::ObjectRpcRequest::encode_body(
            staged_service::CONTENT_VERIFY_ROUTE,
            "dfs-migration",
            &changed_hash,
        )
        .unwrap();
        assert!(
            native
                .rpc_request(
                    &primary,
                    &peer,
                    rpc,
                    sinomo_core::network::DEFAULT_NETWORK_TIMEOUT
                )
                .is_err(),
            "a plan context must not substitute for actual current byte verification"
        );
        // No peer context/runtime/data-owner borrow is retained by this fixture.
        native.stop_node(&peer).unwrap();
        native.reopen_node(peer.clone()).unwrap();
        let rpc = sinomo_core::network::ObjectRpcRequest::encode_body(
            staged_service::CONTENT_VERIFY_ROUTE,
            "dfs-migration",
            &reference,
        )
        .unwrap();
        let reply: ContentVerificationResult = native
            .rpc_request(
                &primary,
                &peer,
                rpc,
                sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            )
            .unwrap()
            .decode_body()
            .unwrap();
        assert!(
            matches!(reply, ContentVerificationResult::ContextMissing { requested, .. } if requested == first)
        );
        // The SAME normal helper resolves only that genuine no-effect miss with
        // one full definition. It must produce actual fresh content evidence.
        runtime
            .authority
            .confirm_or_read_content(&peer, &request, &content)
            .unwrap();
        let second = runtime
            .authority
            .content_plan_contexts
            .lock()
            .unwrap()
            .acknowledged(&scope, &request.import_plan.as_ref().unwrap().digest)
            .unwrap();
        assert_ne!(second.receiver_context, first.receiver_context);
        assert_eq!(second.plan_digest, first.plan_digest);
        let peer_content =
            sinomo_dfs::LocalContentStore::new(native.data_dir(&peer).dfs_dir().join("content"))
                .unwrap();
        let (_, mut reader) = peer_content
            .open_object_version_stream(version.object_id.as_str(), version.version.as_str())
            .unwrap();
        let mut actual = Vec::new();
        reader.read_to_end(&mut actual).unwrap();
        assert_eq!(actual, payload);
        drop(reader);
        drop(peer_content);
        drop(content);
        drop(request);
        drop(reference);
        drop(image);
        drop(_source_writer);
        drop(runtime);
        drop(context);
        native.close().unwrap();
    }

    #[test]
    fn verified_original_named_binding_reopens_under_canonical_identity() {
        let root = sinomo_testdir::TestDir::new("import-content-canonical-binding");
        let content = sinomo_dfs::LocalContentStore::new(root.path()).unwrap();
        let locator = MetadataSourceContentLocator {
            object_id: "original-object".into(),
            version: "original-version".into(),
            content_version: None,
        };
        let bytes = vec![0x36; 128 * 1024];
        let manifest = content
            .put_object_version(&locator.object_id, &locator.version, &bytes)
            .unwrap();
        let version = captured_version(&manifest, &locator);
        verify_local_retained_content(&content, &version, &locator).unwrap();
        assert_eq!(
            content
                .read_object_version(version.object_id.as_str(), version.version.as_str())
                .unwrap()
                .1,
            bytes,
        );
        assert_eq!(
            content
                .read_object_version(&locator.object_id, &locator.version)
                .unwrap()
                .1,
            bytes,
        );
    }

    #[test]
    fn wrong_original_locator_or_hash_never_binds_canonical_manifest() {
        let root = sinomo_testdir::TestDir::new("import-content-reject-binding");
        let content = sinomo_dfs::LocalContentStore::new(root.path()).unwrap();
        let locator = MetadataSourceContentLocator {
            object_id: "original-object".into(),
            version: "original-version".into(),
            content_version: None,
        };
        let manifest = content
            .put_object_version(&locator.object_id, &locator.version, b"actual bytes")
            .unwrap();
        let mut version = captured_version(&manifest, &locator);
        let foreign = MetadataSourceContentLocator {
            object_id: "foreign-object".into(),
            version: locator.version.clone(),
            content_version: None,
        };
        assert!(verify_local_retained_content(&content, &version, &foreign).is_err());
        version.content_hash = Some(format!("blake3:{}", blake3::hash(b"other bytes").to_hex()));
        assert!(verify_local_retained_content(&content, &version, &locator).is_err());
        assert!(
            content
                .read_manifest(version.object_id.as_str(), version.version.as_str())
                .is_err()
        );
        assert_eq!(
            content
                .read_object_version(&locator.object_id, &locator.version)
                .unwrap()
                .1,
            b"actual bytes"
        );
    }

    #[test]
    fn original_pending_byte_fence_requires_exact_row_revision_lease_and_scope() {
        use sinomo_core::kv::{KvEntry, KvRevision, KvVersion, LeaseId};
        let current = DfsMetadataShardConfig {
            file_system_id: sinomo_dfs::DfsFileSystemId::generate(),
            shard_id: "root".into(),
            primary_node_id: NodeId::generate(),
            replica_node_ids: vec![NodeId::generate()],
            epoch: 7,
            assignment: sinomo_dfs::DfsMetadataShardAssignment::Root,
            default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
        };
        let entry = KvEntry {
            key: sinomo_dfs::metadata_shard_membership_handoff_key(
                &current.file_system_id,
                &current.shard_id,
            ),
            value: b"original reservation data, not an acknowledgement".to_vec(),
            create_revision: KvRevision(3),
            modify_revision: KvRevision(5),
            version: KvVersion(2),
            lease_id: Some(LeaseId(9)),
        };
        let mut desired = current.clone();
        desired.epoch += 1;
        let mut scope = MetadataSourceContentWorkScope {
            source_root: PathBuf::from("owned-source"),
            source_identity: "actual-source-cut".into(),
            source_snapshot_fingerprint: "actual-source-snapshot".into(),
            source_journal_sequence: 17,
            file_system_ids: vec![current.file_system_id.clone()],
            scope_shard: sinomo_dfs::DfsMetadataShardId(current.shard_id.clone()),
            projection_assignments: vec![current.clone()],
            original_handoff: Some(crate::metadata::OriginalMetadataHandoffReservation {
                entry: entry.clone(),
                retiring_shard: current.clone(),
                desired_shard: desired,
            }),
        };
        assert!(
            NativeImportAuthority::matches_original_pending_byte_scope(&scope, &current, &entry)
                .unwrap()
        );
        for changed in [
            KvEntry {
                value: b"different reservation".to_vec(),
                ..entry.clone()
            },
            KvEntry {
                modify_revision: KvRevision(6),
                ..entry.clone()
            },
            KvEntry {
                lease_id: None,
                ..entry.clone()
            },
        ] {
            assert!(
                !NativeImportAuthority::matches_original_pending_byte_scope(
                    &scope, &current, &changed
                )
                .unwrap()
            );
        }
        let mut changed_primary = current.clone();
        changed_primary.primary_node_id = NodeId::generate();
        assert!(
            NativeImportAuthority::matches_original_pending_byte_scope(
                &scope,
                &changed_primary,
                &entry
            )
            .is_err()
        );
        scope
            .original_handoff
            .as_mut()
            .unwrap()
            .desired_shard
            .shard_id = "foreign".into();
        assert!(
            NativeImportAuthority::matches_original_pending_byte_scope(&scope, &current, &entry)
                .is_err()
        );
        scope.original_handoff = None;
        assert!(
            !NativeImportAuthority::matches_original_pending_byte_scope(&scope, &current, &entry)
                .unwrap()
        );
    }
    #[test]
    fn actual_retained_import_plan_allows_only_exact_source_fence_progress() {
        // Exact node1 format9 stage DATA copied at r425; no RPC or author
        // capability is synthesized by these pure relation controls.
        let expected: sinomo_dfs::DfsMetadataShardMembershipHandoff =
            serde_json::from_str(r#"{"cause":{"kind":"storage-import","cut":{"source_cut_digest":"c9765e85273b92379c71cf3b363d8e54f1c6baf601c7063c6d28fe9b4d7d985b","source_journal_sequence":8776,"source_state_fingerprint":"dfs-metadata-prefix-v2:blake3:901ca765adabaad74092279654a738d752914332735111ee5e155c210b35b541","target_prefix":{"version":2,"journal_sequence":11334,"digest":"dfs-metadata-prefix-v2:blake3:a31ef0248a0ace959e7d499463967525ddda7ab2ba2b1eb9f3fc3c99f76ea91b"},"converted_image_fingerprint":"blake3:a0beabfaa3c3765f2091a45dcaac2f4a92dc9028ac3e62415fc2aa60e63efe37","pending_mapping_digest":"0f3bcf1c17745618b2b5dd6f8499601d25749cd34dbcb7c78ec06e8fe0cd1ba0"}},"handoff_id":"38k1rc5h8mstd1dzv2tsd4r8pq","retiring_shard":{"file_system_id":"3m3x1ehfwt2cca4jkz8r1jwq9g","shard_id":"root","primary_node_id":"01m479j0x061pkaqyz5ht0fgnm","replica_node_ids":["01m479j3hnkcdcr8tzg2z4sm7c","01m479j3jjp8whdg18x6g11k1r","01m479j4kz7gat1n7x4ph933w2","01m479j6m4ct5gmtp5kv1jyf1g","01m479j6mk57g48er903rs7hea"],"epoch":3,"assignment":"root","default_policy":"replicated-eventual"},"desired_shard":{"file_system_id":"3m3x1ehfwt2cca4jkz8r1jwq9g","shard_id":"root","primary_node_id":"01m479j0x061pkaqyz5ht0fgnm","replica_node_ids":["01m479j3hnkcdcr8tzg2z4sm7c","01m479j3jjp8whdg18x6g11k1r","01m479j4kz7gat1n7x4ph933w2","01m479j6m4ct5gmtp5kv1jyf1g","01m479j6mk57g48er903rs7hea"],"epoch":4,"assignment":"root","default_policy":"replicated-eventual"},"required_prepared_sequence":8687,"required_head_watermark":{"file_system_id":"3m3x1ehfwt2cca4jkz8r1jwq9g","shard_id":"root","primary_node_id":"01m479j0x061pkaqyz5ht0fgnm","epoch":3,"journal_sequence":12,"state_fingerprint":"dfs-metadata-prefix-v2:blake3:7c61b3a3a55ac03d00326911ff9150c2cc2747c30d998f236a22e699a9d9451b","acknowledged_node_ids":["01m479j0x061pkaqyz5ht0fgnm","01m479j3hnkcdcr8tzg2z4sm7c","01m479j3jjp8whdg18x6g11k1r","01m479j4kz7gat1n7x4ph933w2","01m479j6m4ct5gmtp5kv1jyf1g"]}}"#).unwrap();
        let fence: sinomo_dfs::DfsMetadataShardMembershipHandoffFence =
            serde_json::from_str(r#"{"handoff_id":"38k1rc5h8mstd1dzv2tsd4r8pq","primary_node_id":"01m479j0x061pkaqyz5ht0fgnm","journal_sequence":8776,"state_fingerprint":"dfs-metadata-prefix-v2:blake3:901ca765adabaad74092279654a738d752914332735111ee5e155c210b35b541"}"#).unwrap();
        let matches =
            |actual: &sinomo_dfs::DfsMetadataShardMembershipHandoff,
             saved: &sinomo_dfs::DfsMetadataShardMembershipHandoff| {
                NativeImportAuthority::content_handoff_matches_plan(actual, saved, &fence)
            };
        assert!(matches(&expected, &expected));
        let mut acknowledged = expected.clone();
        acknowledged.retiring_primary_fence = Some(fence.clone());
        assert!(matches(&acknowledged, &expected));
        assert!(matches(&acknowledged, &acknowledged));
        assert!(
            !matches(&expected, &acknowledged),
            "an acknowledged fence cannot regress"
        );

        for field in [
            "handoff_id",
            "cause",
            "retiring_shard",
            "desired_shard",
            "required_prepared_sequence",
            "required_head_watermark",
        ] {
            let mut changed = serde_json::to_value(&acknowledged).unwrap();
            match field {
                "handoff_id" => {
                    changed[field] = serde_json::to_value(sinomo_core::Id::generate()).unwrap()
                }
                "cause" => {
                    changed[field]["cut"]["source_cut_digest"] = serde_json::json!("changed-source")
                }
                "retiring_shard" | "desired_shard" => {
                    changed[field]["epoch"] = serde_json::json!(99)
                }
                "required_prepared_sequence" => changed[field] = serde_json::json!(1),
                "required_head_watermark" => {
                    changed[field]["journal_sequence"] = serde_json::json!(1)
                }
                _ => unreachable!(),
            }
            let changed = serde_json::from_value(changed).unwrap();
            assert!(
                !matches(&changed, &expected),
                "changed immutable field {field}"
            );
        }
        for field in [
            "handoff_id",
            "primary_node_id",
            "journal_sequence",
            "state_fingerprint",
        ] {
            let mut changed = serde_json::to_value(&acknowledged).unwrap();
            let value = &mut changed["retiring_primary_fence"];
            match field {
                "handoff_id" | "primary_node_id" => {
                    value[field] = serde_json::to_value(sinomo_core::Id::generate()).unwrap()
                }
                "journal_sequence" => value[field] = serde_json::json!(11334),
                "state_fingerprint" => value[field] = serde_json::json!("changed-prefix"),
                _ => unreachable!(),
            }
            let changed = serde_json::from_value(changed).unwrap();
            assert!(
                !matches(&changed, &expected),
                "changed source fence {field}"
            );
        }
    }
}
