//! The real [`ContributionSource`]: one source shard S answers what it
//! contributes to a converting target shard T, read where S's state lives.
//!
//! - **Open S** (its storage record is Active, or it has no record and its
//!   primary serves a current-format copy under the filesystem-level rule):
//!   S's current primary answers `count-contributions-v1` from its current
//!   tables, in pages bound to one prefix. Each Running job the pages report
//!   is read back from the same primary (`NamespaceJob`) so its frames decide
//!   whether it touches T and what phase it has reached.
//! - **Old or closed S** (its record is Converting, or it has no record and
//!   its copies hold an older image): the node holding the copy S converts
//!   from answers on the migration route `/dfs-migration/count-contributions-v1`
//!   from the capture S's own conversion recorded of that copy
//!   ([`super::contribution_extraction`]). That node is S's primary, or the
//!   converting member of a converting takeover at S's current epoch.
//!
//! A source with no record is asked on the migration route first; its node
//! refuses with `NotOld` when its copy is current, and the current tables
//! answer instead. A source that cannot answer now (its node is unreachable,
//! busy, not yet captured, or refuses for a stale catalog or primary) is
//! `Waiting` on S's configured members and its shard, write-set and storage
//! keys. Nothing here retries on a timer: the driver parks on those
//! dependencies, and an unanswered source is never reported as empty.
//!
//! **Touching T.** A non-Completed job, Pending request or staging intent of
//! S touches T when it names a path T owns under the catalog, an object at
//! such a path, a target fence on an object at such a path, or a parent
//! participant (or coordinator scope) that is T. Work that does not touch T
//! is not reported: settlement is per shard pair.
//!
//! **Phase.** A job's phase is the type of the frame it runs next (the last
//! pending frame), `no-pending-frame` when it has none, or `failed`. A job
//! holding a counted parent plan appends `;parent-counts=<plan phase>` and,
//! for each participant the plan declares, `;participant=<participant id>`,
//! so the participant's shard can find the plan phase of the move it holds a
//! participant for; while the job's frames still hold the move of its root
//! out of or into that participant's directory it also appends
//! `;participant-effect-pending=<participant id>`. A parent participant
//! frame appends `;state=<participant state>`. A Running
//! job of an open source whose primary recorded that its next step waits for
//! T's storage appends `;parked-on-target`. A Pending request has phase
//! `pending`; an intent has its status.
//!
//! **Basis.** Every answer names what it was read from: S's serving epoch,
//! a digest of S's storage record (or its absence), the catalog ownership it
//! is attributed with, and either the prefix proof of S's current tables or
//! the captured source identity and fingerprints of S's old image.
//!
//! **Question.** The target's question to S is asked after the contribution
//! pages, in bounded parts bound to the same prefix (current tables) or the
//! same basis (old image), and S's answer is checked to cover every asked
//! entry. A hardlink job's phase names the target fence it installs or
//! commits as `;fence=<fence job id>`, so the fence's shard can find the job
//! that holds its fence.
#![cfg_attr(
    not(test),
    allow(dead_code, reason = "consumed by the per-shard conversion driver")
)]
use super::contribution_extraction as extraction;
use super::contribution_source::{
    ContributionSource, SourceContributionAnswer, SourceContributionRead, SourceHoldings,
    SourceJobTouchingTarget, SourceQuestion,
};
use super::*;
use crate::namespace_counts::{
    NamespaceCountAliasContribution, NamespaceCountChildDirectoryContribution,
    ShardCountContributions,
};
use sinomo_core::node::NodeId;
use sinomo_dfs::{
    DfsFileSystemId, DfsMetadataPrefixProof, DfsMetadataQueryResult, DfsMetadataSelection,
    DfsMetadataShardId, DfsMetadataShardStorage, DfsNamespaceJobFrame, DfsNamespaceJobRecord,
    DfsNamespaceJobStatus, DfsNamespaceRequestStatus, DfsObjectMetadata, DfsWriteIntentStatus,
};
use std::future::Future;

#[cfg(test)]
mod native_tests;

/// Reads sources through this node's migration owner and DFS host.
#[derive(Clone)]
pub(crate) struct NativeContributionSource {
    authority: Arc<NativeImportAuthority>,
}

impl NativeContributionSource {
    pub(super) fn new(authority: Arc<NativeImportAuthority>) -> Self {
        Self { authority }
    }
}

impl NativeContributionSource {
    fn answer(
        &self,
        target: &DfsMetadataShardConfig,
        source: &DfsMetadataShardConfig,
        question: SourceQuestion,
    ) -> impl Future<Output = Result<SourceContributionRead>> + Send + use<> {
        let authority = Arc::clone(&self.authority);
        let (target, source) = (target.clone(), source.clone());
        async move {
            // Every read below blocks on KV and peer calls, as the owner's
            // other steps do; keep them off the async executor.
            tokio::task::spawn_blocking(move || {
                authority.read_source_contributions(&target, &source, &question)
            })
            .await
            .map_err(error)?
        }
    }
}

impl ContributionSource for NativeContributionSource {
    fn read(
        &self,
        target: &DfsMetadataShardConfig,
        source: &DfsMetadataShardConfig,
    ) -> impl Future<Output = Result<SourceContributionRead>> + Send {
        self.answer(target, source, SourceQuestion::default())
    }

    fn read_asking(
        &self,
        target: &DfsMetadataShardConfig,
        source: &DfsMetadataShardConfig,
        question: &SourceQuestion,
    ) -> impl Future<Output = Result<SourceContributionRead>> + Send {
        self.answer(target, source, question.clone())
    }
}

/// Bytes of one question part on the wire, besides its entries' text.
const QUESTION_ENTRY_OVERHEAD: usize = 8;
/// Largest encoded size of one question part, well inside the request
/// limits of both routes.
const QUESTION_PART_BYTES: usize = 32 * 1024;

/// Split `question` into parts that each fit one request, in question order.
pub(super) fn question_parts(question: &SourceQuestion) -> Vec<SourceQuestion> {
    let mut parts = Vec::new();
    let mut part = SourceQuestion::default();
    let mut bytes = 0usize;
    let mut push = |part: &mut SourceQuestion, bytes: &mut usize, len: usize| {
        let size = len + QUESTION_ENTRY_OVERHEAD;
        if !part.is_empty()
            && (part.len() >= sinomo_dfs::DFS_COUNT_CONTRIBUTION_QUESTION_LIMIT
                || *bytes + size > QUESTION_PART_BYTES)
        {
            parts.push(std::mem::take(part));
            *bytes = 0;
        }
        *bytes += size;
    };
    for object in &question.objects {
        push(&mut part, &mut bytes, object.as_str().len());
        part.objects.push(object.clone());
    }
    for directory in &question.directories {
        push(&mut part, &mut bytes, directory.len());
        part.directories.push(directory.clone());
    }
    for job in &question.jobs {
        push(&mut part, &mut bytes, job.len());
        part.jobs.push(job.clone());
    }
    if !part.is_empty() {
        parts.push(part);
    }
    parts
}

/// Add one part's answer to the answer of the whole question.
pub(super) fn add_holdings(held: &mut SourceHoldings, part: SourceHoldings) {
    held.answered += part.answered;
    held.objects.extend(part.objects);
    held.directories.extend(part.directories);
    held.jobs.extend(part.jobs);
}

/// Digest of the path ownership a catalog assigns: every shard's identity
/// and assignment. Serving fields (epoch, primary, replicas) are not part of
/// it, as in the census's ownership comparison.
pub(super) fn catalog_ownership_digest(catalog: &[DfsMetadataShardConfig]) -> Result<String> {
    let mut ownership = catalog
        .iter()
        .map(|shard| (&shard.file_system_id, &shard.shard_id, &shard.assignment))
        .collect::<Vec<_>>();
    ownership.sort_by(|left, right| (left.0, left.1).cmp(&(right.0, right.1)));
    if ownership
        .windows(2)
        .any(|pair| (pair[0].0, pair[0].1) == (pair[1].0, pair[1].1))
    {
        return Err(error("contribution catalog repeats a shard"));
    }
    digest(&("dfs-count-contribution-catalog-v1", ownership))
}

/// Digest of a storage record entry as stored, or of its absence.
pub(super) fn storage_record_digest(entry: Option<&KvEntry>) -> String {
    match entry {
        Some(entry) => format!("blake3:{}", blake3::hash(&entry.value).to_hex()),
        None => "absent".into(),
    }
}

/// The basis of one answer, rendered canonically so two answers read from
/// the same state are equal and answers read from different states differ.
#[derive(Serialize)]
#[serde(tag = "read", rename_all = "kebab-case")]
pub(super) enum AnswerBasis<'a> {
    CurrentTables {
        epoch: u64,
        storage: &'a str,
        catalog: &'a str,
        prefix: &'a DfsMetadataPrefixProof,
    },
    OldImage {
        epoch: u64,
        storage: &'a str,
        catalog: &'a str,
        source_identity: &'a str,
        source_snapshot_fingerprint: &'a str,
        source_journal_sequence: u64,
        source_state_fingerprint: &'a str,
        capture: &'a str,
    },
}
impl AnswerBasis<'_> {
    pub(super) fn render(&self) -> Result<String> {
        serde_json::to_string(self).map_err(error)
    }
}

fn parent_path(path: &str) -> Option<&str> {
    if path == "/" {
        return None;
    }
    let index = path.rfind('/')?;
    Some(if index == 0 { "/" } else { &path[..index] })
}

/// Decides, under one catalog, whether recorded work touches one target.
pub(super) struct TargetScope<'a> {
    pub file_system_id: &'a DfsFileSystemId,
    pub catalog: &'a [DfsMetadataShardConfig],
    pub target: &'a str,
}
impl TargetScope<'_> {
    /// A path the catalog cannot place is treated as the target's: an
    /// unreadable record is never reported as settled.
    fn owns(&self, path: &str) -> bool {
        sinomo_dfs::select_metadata_shard_config_for_path(self.file_system_id, self.catalog, path)
            .map_or(true, |owner| {
                owner.is_none_or(|owner| owner.shard_id == self.target)
            })
    }
    fn owns_object(&self, object: &DfsObjectMetadata) -> bool {
        self.owns(&object.path)
    }
    fn is_target(&self, shard: &DfsMetadataShardConfig) -> bool {
        shard.file_system_id == *self.file_system_id && shard.shard_id == self.target
    }

    /// Whether a recorded job of the source touches the target.
    pub(super) fn job_touches(&self, job: &DfsNamespaceJobRecord) -> bool {
        use DfsNamespaceJobFrame as Frame;
        if self.owns(&job.root_path)
            || job
                .target_path
                .as_deref()
                .is_some_and(|path| self.owns(path))
        {
            return true;
        }
        job.pending_frames.iter().any(|frame| match frame {
            Frame::ContentCopy { .. }
            | Frame::RenameDirectoryStart { .. }
            | Frame::CrossShardHardlinkComplete { .. } => false,
            Frame::ScanDirectory { path, .. } | Frame::CompleteDirectory { path } => {
                self.owns(path)
            }
            Frame::RenameDirectoryScan {
                source_path,
                target_path,
                ..
            }
            | Frame::RenameDirectoryComplete {
                source_path,
                target_path,
                ..
            }
            | Frame::CrossShardDirScan {
                source_path,
                target_path,
                ..
            } => self.owns(source_path) || self.owns(target_path),
            Frame::CrossShardAdopt {
                moved_object,
                old_path,
                ..
            }
            | Frame::CrossShardComplete {
                moved_object,
                old_path,
            } => self.owns_object(moved_object) || self.owns(old_path),
            Frame::CrossShardHardlinkAdopt { alias, .. } => self.owns_object(alias),
            Frame::CrossShardHardlinkTargetFenceInstall { fence, source_path } => {
                self.owns(source_path)
                    || self.owns(&fence.root_path)
                    || fence
                        .target_path
                        .as_deref()
                        .is_some_and(|path| self.owns(path))
                    || self.owns(&fence.source_path)
                    || self.owns_object(&fence.committed_target)
            }
            Frame::CrossShardHardlinkAliasCommit {
                transaction,
                shard_id,
                expected_path_occupants,
                ..
            } => {
                shard_id.0 == self.target
                    || transaction
                        .objects
                        .iter()
                        .any(|object| self.owns_object(object))
                    || expected_path_occupants
                        .iter()
                        .any(|occupant| self.owns(&occupant.path))
            }
            Frame::CrossShardHardlinkTargetFenceCommit {
                source_path,
                committed_target,
                ..
            }
            | Frame::HardlinkTargetFenceReservation {
                source_path,
                committed_target,
                ..
            } => self.owns(source_path) || self.owns_object(committed_target),
            Frame::CrossShardDirStart {
                source_root,
                target_root,
                ..
            } => self.owns(source_root) || self.owns(target_root),
            Frame::CrossShardDirFinish {
                source_root,
                count_plan,
            } => {
                self.owns(source_root)
                    || count_plan
                        .as_ref()
                        .and_then(|plan| plan.parent_participants.as_ref())
                        .is_some_and(|plan| {
                            plan.participants.iter().any(|participant| {
                                self.is_target(&participant.parent_scope)
                                    || self.owns_object(&participant.expected)
                            })
                        })
            }
            Frame::ParentCountParticipant { participant, .. } => {
                self.is_target(&participant.parent_scope)
                    || self.is_target(&participant.binding.coordinator_scope)
                    || self.owns_object(&participant.expected)
            }
            Frame::DirectoryMoveDestinationReservation {
                source_root,
                target_root,
                ..
            } => self.owns(source_root) || self.owns(target_root),
            Frame::CrossShardDirAbort { .. } => false,
        })
    }

    /// The recorded job as reported to the target, when it touches it.
    pub(super) fn job_touching(
        &self,
        job: &DfsNamespaceJobRecord,
    ) -> Result<Option<SourceJobTouchingTarget>> {
        if job.status == DfsNamespaceJobStatus::Completed || !self.job_touches(job) {
            return Ok(None);
        }
        Ok(Some(SourceJobTouchingTarget {
            job_id: job.job_id.clone(),
            family: wire_tag(&job.kind)?,
            phase: recorded_phase(job)?,
        }))
    }

    /// The old image's non-Completed namespace jobs, Pending requests and
    /// staging intents that touch the target, in a canonical order.
    pub(super) fn unsettled_work(
        &self,
        tables: &sinomo_dfs::DfsMetadataTableSnapshotTables,
    ) -> Result<Vec<SourceJobTouchingTarget>> {
        let mut work = Vec::new();
        for job in &tables.namespace_jobs {
            work.extend(self.job_touching(job)?);
        }
        for request in &tables.namespace_requests {
            if request.status != DfsNamespaceRequestStatus::Pending
                || !request
                    .operation
                    .involved_paths()
                    .into_iter()
                    .any(|path| self.owns(path))
            {
                continue;
            }
            let operation = serde_json::to_value(&request.operation).map_err(error)?;
            let kind = operation
                .as_object()
                .and_then(|fields| fields.keys().next())
                .cloned()
                .or_else(|| operation.as_str().map(str::to_owned))
                .unwrap_or_else(|| "operation".into());
            work.push(SourceJobTouchingTarget {
                job_id: format!("{}:{}", request.requester_node_id, request.request_id),
                family: format!("namespace-request:{kind}"),
                phase: "pending".into(),
            });
        }
        for intent in &tables.intents {
            if !matches!(
                intent.status,
                DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
            ) {
                continue;
            }
            let touches = intent.object_metadata.as_ref().is_some_and(|object| {
                self.owns_object(object)
                    || (intent.touch_parent_on_create
                        && parent_path(&object.path).is_some_and(|parent| self.owns(parent)))
            });
            if touches {
                work.push(SourceJobTouchingTarget {
                    job_id: intent.intent_id.clone(),
                    family: "write-intent".into(),
                    phase: wire_tag(&intent.status)?,
                });
            }
        }
        work.sort();
        work.dedup();
        Ok(work)
    }
}

/// The serde tag of a kebab-case unit or tagged enum value.
fn wire_tag(value: &impl Serialize) -> Result<String> {
    let value = serde_json::to_value(value).map_err(error)?;
    value
        .as_str()
        .map(str::to_owned)
        .or_else(|| {
            value
                .get("type")
                .and_then(|tag| tag.as_str())
                .map(str::to_owned)
        })
        .or_else(|| {
            value
                .as_object()
                .and_then(|fields| fields.keys().next().cloned())
        })
        .ok_or_else(|| error("recorded work has no wire tag"))
}

/// The coordinator's recorded phase, in the format the module documents.
pub(super) fn recorded_phase(job: &DfsNamespaceJobRecord) -> Result<String> {
    let mut phase = match job.status {
        DfsNamespaceJobStatus::Completed => "completed".to_owned(),
        DfsNamespaceJobStatus::Failed => "failed".to_owned(),
        DfsNamespaceJobStatus::Running => match job.pending_frames.last() {
            Some(frame) => wire_tag(frame)?,
            None => "no-pending-frame".to_owned(),
        },
    };
    for frame in &job.pending_frames {
        match frame {
            DfsNamespaceJobFrame::CrossShardDirFinish {
                count_plan: Some(plan),
                ..
            } => {
                if let Some(parents) = &plan.parent_participants {
                    phase.push_str(&format!(";parent-counts={}", wire_tag(&parents.phase)?));
                    for participant in &parents.participants {
                        phase.push_str(&participant_phase(job, participant));
                    }
                }
            }
            DfsNamespaceJobFrame::ParentCountParticipant { state, .. } => {
                phase.push_str(&format!(";state={}", wire_tag(state)?));
            }
            _ => {}
        }
    }
    let mut fences = BTreeSet::new();
    for frame in &job.pending_frames {
        let fence = match frame {
            DfsNamespaceJobFrame::CrossShardHardlinkTargetFenceCommit { fence_job_id, .. } => {
                fence_job_id
            }
            DfsNamespaceJobFrame::CrossShardHardlinkTargetFenceInstall { fence, .. } => {
                &fence.job_id
            }
            _ => continue,
        };
        if fences.insert(fence) {
            phase.push_str(&format!(";fence={fence}"));
        }
    }
    Ok(phase)
}

/// The phase parts naming one parent participant of a directory move's
/// plan: `;participant=<id>`, and `;participant-effect-pending=<id>` while
/// the job's frames still hold the move of its root out of or into the
/// participant's directory (the root's start, or the root's own source
/// completion for the source parent, or adoption for the target parent).
fn participant_phase(
    job: &DfsNamespaceJobRecord,
    participant: &sinomo_dfs::DfsParentCountParticipant,
) -> String {
    let id = &participant.participant_id;
    let parent = participant.expected.path.as_str();
    let source_side = parent_path(&job.root_path) == Some(parent);
    let target_side = job.target_path.as_deref().and_then(parent_path) == Some(parent);
    let pending = job.pending_frames.iter().any(|frame| match frame {
        DfsNamespaceJobFrame::CrossShardDirStart { .. } => true,
        DfsNamespaceJobFrame::CrossShardComplete { old_path, .. } => {
            source_side && *old_path == job.root_path
        }
        DfsNamespaceJobFrame::CrossShardAdopt { moved_object, .. } => {
            target_side && job.target_path.as_ref() == Some(&moved_object.path)
        }
        _ => false,
    });
    let mut parts = format!(";participant={id}");
    if pending {
        parts.push_str(&format!(";participant-effect-pending={id}"));
    }
    parts
}

/// What one source has, as read from KV.
struct SourceState {
    shard: DfsMetadataShardConfig,
    catalog: Vec<DfsMetadataShardConfig>,
    catalog_digest: String,
    storage_entry: Option<KvEntry>,
    storage: Option<DfsMetadataShardStorage>,
}

/// How one attempt to read a source ended short of an answer.
enum Unanswered {
    /// Park on the source's dependencies, plus these keys.
    Waiting(BTreeSet<String>),
    /// The source's copy is current: read its tables.
    NotOld,
    /// The source's node answered with an error that waiting does not fix.
    Failed(MetadataBackendError),
}

impl NativeImportAuthority {
    fn waiting_for(
        &self,
        source: &DfsMetadataShardConfig,
        current: Option<&DfsMetadataShardConfig>,
        extra: BTreeSet<String>,
    ) -> SourceContributionRead {
        let mut nodes = BTreeSet::new();
        for shard in std::iter::once(source).chain(current) {
            nodes.insert(shard.primary_node_id.clone());
            nodes.extend(shard.replica_node_ids.iter().cloned());
        }
        let fs = &source.file_system_id;
        let mut keys = extra;
        keys.extend([
            sinomo_dfs::metadata_shard_key(fs, &source.shard_id),
            sinomo_dfs::metadata_shard_write_set_key(fs, &source.shard_id),
            sinomo_dfs::metadata_shard_storage_key(fs, &source.shard_id),
        ]);
        SourceContributionRead::Waiting { nodes, keys }
    }

    pub(super) fn read_source_contributions(
        &self,
        target: &DfsMetadataShardConfig,
        source: &DfsMetadataShardConfig,
        question: &SourceQuestion,
    ) -> Result<SourceContributionRead> {
        if target.file_system_id != source.file_system_id || target.shard_id == source.shard_id {
            return Err(error(
                "contributions need two distinct shards of one filesystem",
            ));
        }
        let state = match self.source_state(target, source) {
            Ok(state) => state,
            Err(MetadataBackendError::StoragePreparationPending { .. }) => {
                return Ok(self.waiting_for(source, None, BTreeSet::new()));
            }
            Err(failure) => return Err(failure),
        };
        let old = match &state.storage {
            Some(record) if record.is_active() => None,
            Some(record) => Some(record.converting_attempt().and_then(|attempt| {
                (attempt.takeover.is_some() && attempt.epoch == state.shard.epoch)
                    .then(|| attempt.primary_node_id.clone())
            })),
            None => Some(None),
        };
        let attempt = match old {
            Some(converter) => {
                let node = converter.unwrap_or_else(|| state.shard.primary_node_id.clone());
                self.read_old_image(&state, target, &node, question)
            }
            None => Err(Unanswered::NotOld),
        };
        let attempt = match attempt {
            Err(Unanswered::NotOld) => self.read_current_tables(&state, target, question),
            other => other,
        };
        match attempt {
            Ok(answer) => Ok(SourceContributionRead::Answered(answer)),
            Err(Unanswered::Waiting(keys)) => {
                Ok(self.waiting_for(source, Some(&state.shard), keys))
            }
            Err(Unanswered::Failed(failure)) => Err(failure),
            Err(Unanswered::NotOld) => Err(error(
                "source refused both its old image and its current tables",
            )),
        }
    }

    fn source_state(
        &self,
        target: &DfsMetadataShardConfig,
        source: &DfsMetadataShardConfig,
    ) -> Result<SourceState> {
        let fs = &source.file_system_id;
        let catalog = self.assignments(std::slice::from_ref(fs))?;
        let shard = catalog
            .iter()
            .find(|shard| shard.file_system_id == *fs && shard.shard_id == source.shard_id)
            .cloned()
            .ok_or_else(|| error("contribution source is not a shard of the current catalog"))?;
        if !catalog
            .iter()
            .any(|shard| shard.file_system_id == *fs && shard.shard_id == target.shard_id)
        {
            return Err(error(
                "contribution target is not a shard of the current catalog",
            ));
        }
        let (entries, _) =
            self.read_exact(&[sinomo_dfs::metadata_shard_storage_key(fs, &source.shard_id)])?;
        let storage_entry = entries.into_iter().next().and_then(|row| row.entry);
        let storage = storage_entry
            .as_ref()
            .map(|entry| entry.decode::<DfsMetadataShardStorage>().map_err(error))
            .transpose()?;
        if let Some(record) = &storage {
            record.validate_against(&shard)?;
        }
        Ok(SourceState {
            catalog_digest: catalog_ownership_digest(&catalog)?,
            shard,
            catalog,
            storage_entry,
            storage,
        })
    }

    fn extraction_call(
        &self,
        node: &NodeId,
        request: &extraction::Request,
    ) -> Result<extraction::Response> {
        let local = self.local_node()?;
        if *node == local {
            return self.receive_contribution_extraction(&local, request.clone());
        }
        let transport = self
            .transport
            .as_ref()
            .ok_or_else(|| error("contribution extraction requires the native peer transport"))?;
        let call = sinomo_core::network::ObjectRpcCall::encode_borrowed_value(
            extraction::ROUTE,
            "dfs-migration",
            request,
        )
        .map_err(error)?;
        let result =
            self.read_peer(transport.call_async(node, call, source_effect_rpc_options(), true))?;
        let response = self
            .read_peer(result.into_materialized_value_response(extraction::MAX_RESPONSE_BYTES))?;
        response.decode_body().map_err(error)
    }

    /// Page the old image's answer from `node`. A continuation whose basis
    /// moved restarts the read once from its first page.
    fn read_old_image(
        &self,
        state: &SourceState,
        target: &DfsMetadataShardConfig,
        node: &NodeId,
        question: &SourceQuestion,
    ) -> std::result::Result<SourceContributionAnswer, Unanswered> {
        let fs = &state.shard.file_system_id;
        let mut restarted = false;
        let parts = question_parts(question);
        'read: loop {
            let mut contributions = ShardCountContributions {
                source_shard: DfsMetadataShardId::new(&state.shard.shard_id),
                target_shard: DfsMetadataShardId::new(&target.shard_id),
                aliases: Vec::new(),
                child_directories: Vec::new(),
            };
            let mut jobs = Vec::new();
            let mut continuation = None::<extraction::Continuation>;
            let mut held = SourceHoldings::default();
            let mut read = 0u64;
            // The question's parts follow the last page, as continuations
            // past every entry of the same basis.
            let mut asking = None::<(String, usize)>;
            loop {
                let part = asking.as_ref().map(|(_, index)| parts[*index].clone());
                let request = extraction::Request {
                    file_system_id: fs.clone(),
                    source_shard: state.shard.shard_id.clone(),
                    target_shard: target.shard_id.clone(),
                    source_epoch: state.shard.epoch,
                    catalog_digest: state.catalog_digest.clone(),
                    continuation: match &asking {
                        Some((basis, _)) => Some(extraction::Continuation {
                            basis: basis.clone(),
                            after: read,
                        }),
                        None => continuation.clone(),
                    },
                    question: part.clone().unwrap_or_default(),
                };
                let response = match self.extraction_call(node, &request) {
                    Ok(response) => response,
                    // Unreachable, unknown or saturated: the transport's
                    // retryable failures (see `read_peer`).
                    Err(MetadataBackendError::StoragePreparationPending { .. }) => {
                        tracing::debug!(source=%state.shard.shard_id, node=%node,
                            "contribution source awaits its old-image node");
                        return Err(Unanswered::Waiting(BTreeSet::new()));
                    }
                    Err(failure) => return Err(Unanswered::Failed(failure)),
                };
                match response {
                    extraction::Response::Page {
                        basis,
                        aliases,
                        child_directories,
                        running_jobs_touching_target,
                        after,
                        complete,
                        held: part_held,
                    } => {
                        if let Some((asked_basis, index)) = &mut asking {
                            let part = part.expect("an asked part");
                            if *asked_basis != basis
                                || !complete
                                || !aliases.is_empty()
                                || !child_directories.is_empty()
                                || !running_jobs_touching_target.is_empty()
                                || !part_held.answers(&part)
                            {
                                return Err(Unanswered::Failed(error(
                                    "old-image question answer does not continue its read",
                                )));
                            }
                            add_holdings(&mut held, part_held);
                            *index += 1;
                            if *index == parts.len() {
                                let mut answer = finish_answer(contributions, jobs, basis);
                                answer.held = held;
                                return Ok(answer);
                            }
                            continue;
                        }
                        if continuation
                            .as_ref()
                            .is_some_and(|previous| previous.basis != basis)
                            || complete != after.is_none()
                        {
                            return Err(Unanswered::Failed(error(
                                "contribution page does not continue its read",
                            )));
                        }
                        read += u64::try_from(
                            aliases.len()
                                + child_directories.len()
                                + running_jobs_touching_target.len(),
                        )
                        .map_err(|failure| Unanswered::Failed(error(failure)))?;
                        contributions.aliases.extend(aliases);
                        contributions.child_directories.extend(child_directories);
                        jobs.extend(running_jobs_touching_target);
                        match after {
                            Some(after) => {
                                continuation = Some(extraction::Continuation { basis, after });
                            }
                            None if parts.is_empty() => {
                                return Ok(finish_answer(contributions, jobs, basis));
                            }
                            None => asking = Some((basis, 0)),
                        }
                    }
                    extraction::Response::Changed { .. } if !restarted => {
                        restarted = true;
                        continue 'read;
                    }
                    extraction::Response::Changed { .. }
                    | extraction::Response::SourcePending { .. } => {
                        return Err(Unanswered::Waiting(BTreeSet::new()));
                    }
                    extraction::Response::Refused { refusal } => {
                        return Err(match refusal {
                            extraction::Refusal::NotOld => Unanswered::NotOld,
                            extraction::Refusal::NotPrimary { .. } => {
                                Unanswered::Waiting(BTreeSet::new())
                            }
                            extraction::Refusal::CatalogMismatch => Unanswered::Waiting(
                                state
                                    .catalog
                                    .iter()
                                    .map(|shard| {
                                        sinomo_dfs::metadata_shard_key(
                                            &shard.file_system_id,
                                            &shard.shard_id,
                                        )
                                    })
                                    .collect(),
                            ),
                        });
                    }
                }
            }
        }
    }

    /// Page `count-contributions-v1` from S's current primary, then read back
    /// each Running job it reports. A prefix that moves between pages
    /// restarts the read once.
    fn read_current_tables(
        &self,
        state: &SourceState,
        target: &DfsMetadataShardConfig,
        question: &SourceQuestion,
    ) -> std::result::Result<SourceContributionAnswer, Unanswered> {
        let unavailable = |failure: &dyn std::fmt::Display| {
            tracing::debug!(source=%state.shard.shard_id, error=%failure,
                "contribution source awaits its current primary");
            Unanswered::Waiting(BTreeSet::new())
        };
        let data_dir = self.component_context.data_dir().clone();
        let mut service =
            sinomo_dfs::runtime_host::open_dfs_runtime_service(&data_dir, Some(&self.runtime))
                .map_err(|failure| unavailable(&failure))?;
        let fs = &state.shard.file_system_id;
        let scope = TargetScope {
            file_system_id: fs,
            catalog: &state.catalog,
            target: &target.shard_id,
        };
        let mut restarted = false;
        'read: loop {
            let mut contributions = ShardCountContributions {
                source_shard: DfsMetadataShardId::new(&state.shard.shard_id),
                target_shard: DfsMetadataShardId::new(&target.shard_id),
                aliases: Vec::new(),
                child_directories: Vec::new(),
            };
            let mut running = Vec::new();
            let mut prefix = None::<DfsMetadataPrefixProof>;
            let mut after = None;
            loop {
                let result = service
                    .query_metadata_authority(
                        &state.shard,
                        DfsMetadataSelection::CountContributionsV1 {
                            target_shard: target.shard_id.clone(),
                            expected_prefix: prefix.clone(),
                            after: after.clone(),
                            question: SourceQuestion::default(),
                        },
                    )
                    .map_err(|failure| unavailable(&failure))?;
                let page = match result {
                    DfsMetadataQueryResult::CountContributionsV1 { page } => page,
                    DfsMetadataQueryResult::CountContributionsChangedV1 { .. } if !restarted => {
                        restarted = true;
                        continue 'read;
                    }
                    other => {
                        return Err(unavailable(&format!(
                            "unexpected contribution result {other:?}"
                        )));
                    }
                };
                contributions
                    .aliases
                    .extend(page.aliases.into_iter().map(|alias| {
                        NamespaceCountAliasContribution {
                            file_system_id: fs.clone(),
                            alias_object_id: alias.alias_object_id,
                            alias_path: alias.alias_path,
                            target_object_id: alias.target_object_id,
                        }
                    }));
                contributions
                    .child_directories
                    .extend(page.child_directories.into_iter().map(|child| {
                        NamespaceCountChildDirectoryContribution {
                            file_system_id: fs.clone(),
                            parent_path: child.parent_path,
                            child_object_id: child.child_object_id,
                            child_path: child.child_path,
                        }
                    }));
                running.extend(page.running_jobs.into_iter().map(|job| {
                    let parked = job
                        .waiting_for_storage_of
                        .iter()
                        .any(|shard| shard.0 == target.shard_id);
                    (job.job_id, parked)
                }));
                prefix = Some(page.prefix);
                if page.complete {
                    break;
                }
                after = page.after;
            }
            let prefix = prefix.expect("a complete read has a page");
            // The question, in parts bound to the pages' prefix.
            let mut held = SourceHoldings::default();
            for part in question_parts(question) {
                let result = service
                    .query_metadata_authority(
                        &state.shard,
                        DfsMetadataSelection::CountContributionsV1 {
                            target_shard: target.shard_id.clone(),
                            expected_prefix: Some(prefix.clone()),
                            after: Some(sinomo_dfs::DfsCountContributionCursor::Complete),
                            question: part.clone(),
                        },
                    )
                    .map_err(|failure| unavailable(&failure))?;
                match result {
                    DfsMetadataQueryResult::CountContributionsV1 { page }
                        if page.held.answers(&part)
                            && page.complete
                            && page.aliases.is_empty()
                            && page.child_directories.is_empty()
                            && page.running_jobs.is_empty() =>
                    {
                        add_holdings(&mut held, page.held);
                    }
                    DfsMetadataQueryResult::CountContributionsChangedV1 { .. } if !restarted => {
                        restarted = true;
                        continue 'read;
                    }
                    other => {
                        return Err(unavailable(&format!(
                            "unexpected contribution question result {other:?}"
                        )));
                    }
                }
            }
            let mut jobs = Vec::new();
            for (job_id, parked) in running {
                let result = service
                    .query_metadata_authority(
                        &state.shard,
                        DfsMetadataSelection::NamespaceJob {
                            job_id: job_id.clone(),
                        },
                    )
                    .map_err(|failure| unavailable(&failure))?;
                let DfsMetadataQueryResult::NamespaceJob { job, .. } = result else {
                    return Err(unavailable(&"namespace job query returned another result"));
                };
                match job {
                    Some(job) if job.status == DfsNamespaceJobStatus::Running => {
                        jobs.extend(
                            scope
                                .job_touching(&job)
                                .map_err(|failure| unavailable(&failure))?
                                .map(|touching| parked_on_target_hook(touching, parked)),
                        );
                    }
                    // The job moved on after the page was read: the source
                    // changed, and this read cannot be the stable one.
                    _ => jobs.push(SourceJobTouchingTarget {
                        job_id,
                        family: "namespace-job".into(),
                        phase: "changed-during-read".into(),
                    }),
                }
            }
            let storage = storage_record_digest(state.storage_entry.as_ref());
            let basis = AnswerBasis::CurrentTables {
                epoch: state.shard.epoch,
                storage: &storage,
                catalog: &state.catalog_digest,
                prefix: &prefix,
            }
            .render()
            .map_err(|failure| unavailable(&failure))?;
            let mut answer = finish_answer(contributions, jobs, basis);
            answer.held = held;
            return Ok(answer);
        }
    }
}

/// Mark a job its page reported as waiting on the target's storage. The
/// read-back job is from the same primary; a job that moved on between the
/// two reads changed the source's prefix, so the next round differs.
fn parked_on_target_hook(mut job: SourceJobTouchingTarget, parked: bool) -> SourceJobTouchingTarget {
    if parked {
        job.phase.push(';');
        job.phase
            .push_str(super::contribution_source::PARKED_ON_TARGET_PHASE);
    }
    job
}

/// Canonical order, so equal reads give equal answers.
pub(super) fn finish_answer(
    mut contributions: ShardCountContributions,
    mut jobs: Vec<SourceJobTouchingTarget>,
    basis: String,
) -> SourceContributionAnswer {
    contributions.aliases.sort_by(|left, right| {
        left.alias_object_id
            .cmp(&right.alias_object_id)
            .then(left.alias_path.cmp(&right.alias_path))
    });
    contributions.child_directories.sort_by(|left, right| {
        left.child_object_id
            .cmp(&right.child_object_id)
            .then(left.child_path.cmp(&right.child_path))
    });
    jobs.sort();
    jobs.dedup();
    SourceContributionAnswer {
        contributions,
        running_jobs_touching_target: jobs,
        basis,
        held: SourceHoldings::default(),
    }
}

#[cfg(test)]
mod touch_tests {
    use super::*;
    use sinomo_dfs::{
        DfsMetadataShardAssignment, DfsNamespaceJobKind, DfsObjectId, DfsObjectKind, DfsVersion,
    };

    fn catalog() -> Vec<DfsMetadataShardConfig> {
        ["root", "services", "users"]
            .into_iter()
            .map(|shard| DfsMetadataShardConfig {
                file_system_id: sinomo_dfs::sifs_file_system_id(),
                shard_id: shard.into(),
                primary_node_id: NodeId::from_legacy_named(format!("touch-{shard}")).unwrap(),
                replica_node_ids: Vec::new(),
                epoch: 1,
                assignment: if shard == "root" {
                    DfsMetadataShardAssignment::Root
                } else {
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: format!("/{shard}"),
                    }
                },
                default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
            })
            .collect()
    }
    fn object(name: &str, path: &str, kind: DfsObjectKind) -> DfsObjectMetadata {
        DfsObjectMetadata {
            object_id: DfsObjectId::from_legacy_named(name).unwrap(),
            file_system_id: sinomo_dfs::sifs_file_system_id(),
            path: path.into(),
            namespace_link_count: kind.initial_namespace_link_count(),
            kind,
            tombstone_provenance: None,
            owner_node_id: NodeId::from_legacy_named("touch-owner").unwrap(),
            current_locations: Vec::new(),
            content_hash: None,
            content_size_bytes: None,
            link_target_object_id: None,
            mode: 0o755,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version: DfsVersion::from_legacy_named(format!("{name}-version")).unwrap(),
            policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        }
    }
    fn job(
        id: &str,
        kind: DfsNamespaceJobKind,
        root: &str,
        target: Option<&str>,
        frames: Vec<DfsNamespaceJobFrame>,
    ) -> DfsNamespaceJobRecord {
        DfsNamespaceJobRecord {
            job_id: id.into(),
            file_system_id: sinomo_dfs::sifs_file_system_id(),
            origin: None,
            kind,
            status: DfsNamespaceJobStatus::Running,
            root_path: root.into(),
            target_path: target.map(Into::into),
            pending_frames: frames,
            processed_object_count: 0,
            created_at: time::OffsetDateTime::UNIX_EPOCH,
            updated_at: time::OffsetDateTime::UNIX_EPOCH,
            completed_at: None,
            failure_reason: None,
        }
    }
    fn touched(job: &DfsNamespaceJobRecord) -> BTreeSet<&'static str> {
        let catalog = catalog();
        let fs = sinomo_dfs::sifs_file_system_id();
        ["root", "services", "users"]
            .into_iter()
            .filter(|target| {
                TargetScope {
                    file_system_id: &fs,
                    catalog: &catalog,
                    target,
                }
                .job_touches(job)
            })
            .collect()
    }

    #[test]
    fn a_job_its_page_reports_waiting_on_the_target_is_parked_on_it() {
        let job = SourceJobTouchingTarget {
            job_id: "move".into(),
            family: "rename-directory-across-shard-primaries".into(),
            phase: "cross-shard-dir-scan;parent-counts=issued".into(),
        };
        assert!(!job.parked_on_target());
        assert_eq!(parked_on_target_hook(job.clone(), false), job);
        let parked = parked_on_target_hook(job.clone(), true);
        assert_eq!(
            parked.phase,
            "cross-shard-dir-scan;parent-counts=issued;parked-on-target"
        );
        assert!(parked.parked_on_target());
        // Only the exact phase part marks the park.
        let other = SourceJobTouchingTarget {
            phase: "cross-shard-dir-scan;state=parked-on-target-shard".into(),
            ..job
        };
        assert!(!other.parked_on_target());
    }

    /// A directory move names each participant of its plan, and marks one
    /// whose child effect its frames still hold: the root's start, the
    /// root's source completion for the source parent, or the root's
    /// adoption for the target parent. A child's frames mark nothing.
    #[test]
    fn a_directory_moves_phase_names_its_participants_and_their_pending_effects() {
        let catalog = catalog();
        let participant = |root: &str, target: &str| {
            let mut parent = object("phase-root", "/", DfsObjectKind::Directory);
            parent.namespace_link_count = Some(4);
            sinomo_dfs::DfsParentCountParticipant {
                participant_id: format!("participant-of-{root}"),
                binding: sinomo_dfs::DfsParentCountBinding {
                    job_id: "move".into(),
                    file_system_id: sinomo_dfs::sifs_file_system_id(),
                    root_path: root.into(),
                    target_path: target.into(),
                    coordinator_scope: catalog[2].clone(),
                    source: object("phase-dir", root, DfsObjectKind::Directory),
                    expected_destination: sinomo_dfs::DfsNamespaceDestinationPrecondition::Absent,
                    origin: None,
                    created_at: time::OffsetDateTime::UNIX_EPOCH,
                },
                parent_scope: catalog[0].clone(),
                expected: parent.clone(),
                successor: parent,
                delta: -1,
            }
        };
        let moved = |path: &str| object("phase-dir", path, DfsObjectKind::Directory);
        let complete = |old: &str| DfsNamespaceJobFrame::CrossShardComplete {
            moved_object: moved("/unused"),
            old_path: old.into(),
        };
        let adopt = |path: &str, old: &str| DfsNamespaceJobFrame::CrossShardAdopt {
            moved_object: moved(path),
            old_path: old.into(),
            promotion_alias: None,
            destination_primary_node_id: NodeId::from_legacy_named("phase-destination").unwrap(),
        };
        let phase = |root: &str, target: &str, frames: Vec<DfsNamespaceJobFrame>| {
            let participant = participant(root, target);
            let id = participant.participant_id.clone();
            let mut pending = vec![DfsNamespaceJobFrame::CrossShardDirFinish {
                source_root: root.into(),
                count_plan: Some(sinomo_dfs::DfsDirectoryMoveCounts {
                    parent_participants: Some(sinomo_dfs::DfsParentCountPlan {
                        participants: vec![participant],
                        phase: sinomo_dfs::DfsParentCountPhase::Issued,
                        confirmed: 0,
                        rejection: None,
                    }),
                    expected_destination: sinomo_dfs::DfsNamespaceDestinationPrecondition::Absent,
                    source_parent_delta: -1,
                    target_parent_delta: 1,
                }),
            }];
            pending.extend(frames);
            let job = job(
                "move",
                DfsNamespaceJobKind::RenameDirectoryAcrossShardPrimaries,
                root,
                Some(target),
                pending,
            );
            let phase = recorded_phase(&job).unwrap();
            let named = phase.split(';').any(|part| part == format!("participant={id}"));
            let marked = phase
                .split(';')
                .any(|part| part == format!("participant-effect-pending={id}"));
            assert!(named, "{phase}");
            marked
        };
        let scan = |source: &str, target: &str| DfsNamespaceJobFrame::CrossShardDirScan {
            source_path: source.into(),
            target_path: target.into(),
            cursor: None,
        };
        // The source parent "/" of a moved shard root.
        let (root, target) = ("/users", "/services/moved");
        let start = DfsNamespaceJobFrame::CrossShardDirStart {
            source_object_id: moved(root).object_id,
            source_root: root.into(),
            target_root: target.into(),
        };
        assert!(phase(root, target, vec![scan(root, target), start]));
        assert!(phase(root, target, vec![scan(root, target), complete(root), adopt(target, root)]));
        assert!(phase(root, target, vec![scan(root, target), complete(root)]));
        assert!(!phase(root, target, vec![scan(root, target)]));
        assert!(!phase(
            root,
            target,
            vec![
                scan(root, target),
                complete("/users/child"),
                adopt("/services/moved/child", "/users/child")
            ]
        ));
        assert!(!phase(root, target, Vec::new()), "only the finish is left");
        // The target parent "/" of a move that makes a new shard root.
        let (root, target) = ("/services/dir", "/users");
        assert!(phase(root, target, vec![scan(root, target), complete(root), adopt(target, root)]));
        assert!(!phase(root, target, vec![scan(root, target), complete(root)]));
        assert!(!phase(root, target, vec![scan(root, target)]));
    }

    #[test]
    fn work_touches_a_target_through_its_paths_fences_and_parent_participants() {
        let rename = job(
            "rename",
            DfsNamespaceJobKind::RenameObjectAcrossShardPrimaries,
            "/services/file",
            Some("/moved"),
            vec![],
        );
        assert_eq!(touched(&rename), BTreeSet::from(["services", "root"]));

        // The coordinator's alias paths are its own; the fence it commits is
        // held on the canonical object's shard.
        let mut target = object("touch-target", "/users/doc", DfsObjectKind::File);
        target.namespace_link_count = Some(2);
        let fence = job(
            "fence",
            DfsNamespaceJobKind::HardlinkAliasMutationAcrossShardPrimaries,
            "/services/alias",
            Some("/services/renamed"),
            vec![DfsNamespaceJobFrame::CrossShardHardlinkTargetFenceCommit {
                fence_job_id: "fence-target".into(),
                source_path: "/services/alias".into(),
                expected_target: sinomo_dfs::DfsNamespaceObjectPrecondition {
                    object_id: target.object_id.clone(),
                    version: target.version.clone(),
                },
                committed_target: target.clone(),
            }],
        );
        assert_eq!(touched(&fence), BTreeSet::from(["services", "users"]));
        assert_eq!(
            recorded_phase(&fence).unwrap(),
            "cross-shard-hardlink-target-fence-commit;fence=fence-target",
            "the phase names the fence the job commits"
        );

        // A directory move under services holds a parent participant on root.
        let mut parent = object("touch-root", "/", DfsObjectKind::Directory);
        parent.namespace_link_count = Some(4);
        let mut successor = parent.clone();
        successor.namespace_link_count = Some(5);
        successor.version = DfsVersion::from_legacy_named("touch-root-next").unwrap();
        let catalog = catalog();
        let participant = sinomo_dfs::DfsParentCountParticipant {
            participant_id: "participant".into(),
            binding: sinomo_dfs::DfsParentCountBinding {
                job_id: "dir".into(),
                file_system_id: sinomo_dfs::sifs_file_system_id(),
                root_path: "/services/dir".into(),
                target_path: "/services/other/dir".into(),
                coordinator_scope: catalog[1].clone(),
                source: object("touch-dir", "/services/dir", DfsObjectKind::Directory),
                expected_destination: sinomo_dfs::DfsNamespaceDestinationPrecondition::Absent,
                origin: None,
                created_at: time::OffsetDateTime::UNIX_EPOCH,
            },
            parent_scope: catalog[0].clone(),
            expected: parent,
            successor,
            delta: 1,
        };
        let plan = |participants| sinomo_dfs::DfsDirectoryMoveCounts {
            parent_participants: Some(sinomo_dfs::DfsParentCountPlan {
                participants,
                phase: sinomo_dfs::DfsParentCountPhase::Issued,
                confirmed: 0,
                rejection: None,
            }),
            expected_destination: sinomo_dfs::DfsNamespaceDestinationPrecondition::Absent,
            source_parent_delta: 0,
            target_parent_delta: 0,
        };
        let frames = |participants| {
            vec![
                DfsNamespaceJobFrame::CrossShardDirFinish {
                    source_root: "/services/dir".into(),
                    count_plan: Some(plan(participants)),
                },
                DfsNamespaceJobFrame::CrossShardDirScan {
                    source_path: "/services/dir".into(),
                    target_path: "/services/other/dir".into(),
                    cursor: None,
                },
            ]
        };
        let directory = job(
            "dir",
            DfsNamespaceJobKind::RenameDirectoryAcrossShardPrimaries,
            "/services/dir",
            Some("/services/other/dir"),
            frames(vec![participant]),
        );
        assert_eq!(touched(&directory), BTreeSet::from(["services", "root"]));
        assert_eq!(
            recorded_phase(&directory).unwrap(),
            "cross-shard-dir-scan;parent-counts=issued;participant=participant",
            "the phase names the participants of the job's plan"
        );
        let local = job(
            "local",
            DfsNamespaceJobKind::RenameDirectoryAcrossShardPrimaries,
            "/services/dir",
            Some("/services/other/dir"),
            frames(Vec::new()),
        );
        assert_eq!(
            touched(&local),
            BTreeSet::from(["services"]),
            "work that names no other shard is not reported"
        );

        // Unsettled work of an old image: only what touches the target.
        let mut tables = sinomo_dfs::DfsMetadataTableSnapshotTables::default();
        let mut completed = rename.clone();
        completed.job_id = "completed".into();
        completed.status = DfsNamespaceJobStatus::Completed;
        let mut failed = local.clone();
        failed.job_id = "failed".into();
        failed.status = DfsNamespaceJobStatus::Failed;
        failed.target_path = Some("/users/dir".into());
        tables.namespace_jobs = vec![rename, completed, failed, fence, directory, local];
        tables
            .namespace_requests
            .push(sinomo_dfs::DfsNamespaceRequestRecord {
                file_system_id: sinomo_dfs::sifs_file_system_id(),
                requester_node_id: NodeId::from_legacy_named("touch-requester").unwrap(),
                request_id: "mkdir".into(),
                operation_id: "mkdir".into(),
                authority_shard_id: DfsMetadataShardId::new("users"),
                operation: sinomo_dfs::DfsNamespaceOperation::Mkdir {
                    path: "/users/new".into(),
                },
                command_digest: "digest".into(),
                status: DfsNamespaceRequestStatus::Pending,
                result: None,
                posix_coordination_result: None,
                recovery_attempts: 0,
                last_recovery_error: None,
                created_at: time::OffsetDateTime::UNIX_EPOCH,
                updated_at: time::OffsetDateTime::UNIX_EPOCH,
            });
        let fs = sinomo_dfs::sifs_file_system_id();
        let reported = |target: &str| {
            TargetScope {
                file_system_id: &fs,
                catalog: &catalog,
                target,
            }
            .unsettled_work(&tables)
            .unwrap()
            .into_iter()
            .map(|work| (work.job_id, work.phase))
            .collect::<BTreeSet<_>>()
        };
        assert_eq!(
            reported("root"),
            BTreeSet::from([
                (
                    "dir".into(),
                    "cross-shard-dir-scan;parent-counts=issued;participant=participant".into()
                ),
                ("rename".into(), "no-pending-frame".into()),
            ])
        );
        let requester = NodeId::from_legacy_named("touch-requester").unwrap();
        assert_eq!(
            reported("users"),
            BTreeSet::from([
                ("failed".into(), "failed;parent-counts=issued".into()),
                (
                    "fence".into(),
                    "cross-shard-hardlink-target-fence-commit;fence=fence-target".into()
                ),
                (format!("{requester}:mkdir"), "pending".into()),
            ])
        );
    }
}
