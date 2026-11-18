//! Per-pair checks and settlements of one shard's conversion.
//!
//! What a converting shard T holds can name what other shards hold:
//!
//! - an alias whose target is none of T's live canonical rows;
//! - a non-directory row whose parent path another shard owns (possible only
//!   under hash routing);
//! - a target fence T holds for another shard's hardlink job;
//! - a hardlink job of T whose only step left commits a fence another shard
//!   holds;
//! - a parent-count participant T holds, Reserved, for another shard's
//!   directory move;
//! - a legacy cross-shard hardlink creation of T whose alias another shard
//!   owns.
//!
//! T asks every other shard about these in its contribution rounds
//! ([`RoundQuestions`]) and decides them on the accepted round
//! ([`settle_pairs`]), so each decision reads one stable state of every shard.
//! A shard that cannot answer keeps the round from completing, so nothing is
//! decided against it:
//!
//! - a target no shard holds, T included, is a dangling alias: T's conversion
//!   stops for operator repair, and no other shard's does;
//! - a parent its owner does not hold as a live directory stops T;
//! - a fence T holds for another shard's job settles from the phase the job's
//!   coordinator recorded: a job whose next step is the fence commit has made
//!   its alias change durable, so T applies the committed target and records
//!   the fence Completed, as its commit would; any other phase, or no
//!   reported job, keeps the fence, and T's counts are those from before it;
//! - a fence-commit-only job settles forward once its fence's shard no longer
//!   holds the fence. While that shard is closed and holds it, the job stays
//!   Running in T's image: that shard's conversion applies the fence from
//!   this job's phase, and the job's commit, which runs once both are open,
//!   finds the fence committed ([`closed`]). T waits only for that shard to
//!   record its conversion. When that shard is current and holds the fence,
//!   releasing it needs a fence-release step that does not exist, so T
//!   stops;
//! - a parent-count participant T holds settles from the plan phase its
//!   coordinating move recorded (DESIGN-BD C.4): a plan Issued or Settling
//!   has made the move's effect on the parent forward-only, so T applies the
//!   participant's successor and records the participant Applied, and the
//!   coordinator's later apply finds it recorded. A plan Issued whose frames
//!   still hold the move of its root out of or into that parent has not yet
//!   made the effect T's other count inputs show, so T keeps the
//!   participant, as for a plan Preparing or Aborting or a coordinator that
//!   reports no phase: T keeps the count before the delta and the
//!   reservation, and the coordinator applies or aborts it after T opens;
//! - a legacy creation of T settles from the live aliases its alias shard
//!   reports ([`closed`]).
//!
//! A job another closed shard's own conversion settles from its rows does
//! not hold T's rounds ([`closed`]).
//!
//! T's own hardlink work is settled from T's rows before the rounds
//! ([`OwnPairWork`]): a fence-commit-only job whose canonical target is
//! T's settles forward with the commit applied in the image, and a fence whose
//! coordinating job is T's settles with that job: applied when the job
//! settles Completed, dropped when it is aborted. The outer namespace request
//! of every job T settles takes the matching terminal result.
use super::*;
use crate::authority::contribution_source::SourceJobTouchingTarget;
use crate::authority::contribution_source::{SourceContributionAnswer, SourceQuestion};
use crate::namespace_counts::contributions::{ForeignAliasTarget, ForeignParent};
use sinomo_dfs::{
    DfsMetadataTableSnapshotTables, DfsNamespaceRequestRecord, DfsNamespaceRequestStatus,
    DfsNamespaceResult, DfsObjectMetadata,
};
use time::OffsetDateTime;

pub(super) mod closed;

/// The phase tag of a hardlink job whose next step is its fence commit.
const FENCE_COMMIT_PHASE: &str = "cross-shard-hardlink-target-fence-commit";
/// The phase part naming a parent participant of a directory move's plan.
const PARTICIPANT_PHASE: &str = "participant=";
/// The phase part marking a participant whose child effect the move's
/// frames still hold.
const PARTICIPANT_EFFECT_PENDING_PHASE: &str = "participant-effect-pending=";
/// The phase part carrying a directory move's parent-count plan phase.
const PARENT_COUNTS_PHASE: &str = "parent-counts=";

fn owner_of(
    catalog: &[DfsMetadataShardConfig],
    shard: &DfsMetadataShardConfig,
    path: &str,
) -> Result<String> {
    sinomo_dfs::select_metadata_shard_config_for_path(&shard.file_system_id, catalog, path)
        .map_err(error)?
        .map(|owner| owner.shard_id)
        .ok_or_else(|| error(format!("no shard owns {path}")))
}

/// The fence commit a hardlink job holds as its only step.
fn only_fence_commit(
    job: &DfsNamespaceJobRecord,
) -> Option<(
    &str,
    &sinomo_dfs::DfsNamespaceObjectPrecondition,
    &DfsObjectMetadata,
)> {
    match job.pending_frames.as_slice() {
        [
            DfsNamespaceJobFrame::CrossShardHardlinkTargetFenceCommit {
                fence_job_id,
                expected_target,
                committed_target,
                ..
            },
        ] if job.status == DfsNamespaceJobStatus::Running
            && matches!(
                job.kind,
                DfsNamespaceJobKind::HardlinkAcrossShardPrimaries
                    | DfsNamespaceJobKind::HardlinkAliasMutationAcrossShardPrimaries
            ) =>
        {
            Some((fence_job_id, expected_target, committed_target))
        }
        _ => None,
    }
}

/// The reservation of a target fence the shard keeps for a hardlink job.
fn fence_reservation(
    job: &DfsNamespaceJobRecord,
) -> Option<(
    &str,
    &sinomo_dfs::DfsNamespaceObjectPrecondition,
    &DfsObjectMetadata,
)> {
    if !crate::namespace_counts::contributions::is_kept_target_fence(job) {
        return None;
    }
    match job.pending_frames.as_slice() {
        [
            DfsNamespaceJobFrame::HardlinkTargetFenceReservation {
                source_path,
                expected_target,
                committed_target,
            },
        ] if job.root_path == committed_target.path => {
            Some((source_path, expected_target, committed_target))
        }
        _ => None,
    }
}

/// The parent-count participant `job` holds Reserved on the shard.
fn held_participant(job: &DfsNamespaceJobRecord) -> Option<&sinomo_dfs::DfsParentCountParticipant> {
    if !crate::namespace_counts::contributions::is_kept_parent_participant(job) {
        return None;
    }
    match job.pending_frames.as_slice() {
        [DfsNamespaceJobFrame::ParentCountParticipant { participant, .. }]
            if job.job_id == participant.participant_id
                && job.root_path == participant.expected.path =>
        {
            Some(participant.as_ref())
        }
        _ => None,
    }
}

/// Whether `job` installs or commits the target fence `fence_job_id`.
fn names_fence(job: &DfsNamespaceJobRecord, fence_job_id: &str) -> bool {
    job.pending_frames.iter().any(|frame| match frame {
        DfsNamespaceJobFrame::CrossShardHardlinkTargetFenceCommit {
            fence_job_id: named,
            ..
        } => named == fence_job_id,
        DfsNamespaceJobFrame::CrossShardHardlinkTargetFenceInstall { fence, .. } => {
            fence.job_id == fence_job_id
        }
        _ => false,
    })
}

fn completed(job: &DfsNamespaceJobRecord, at: OffsetDateTime) -> DfsNamespaceJobRecord {
    let mut job = job.clone();
    job.pending_frames.clear();
    job.status = DfsNamespaceJobStatus::Completed;
    job.updated_at = at;
    job.completed_at = Some(at);
    job
}

/// The live row at `committed.path` must still be the generation the fence
/// expected, or already the committed one.
fn check_fence_target(
    rows: &DfsMetadataTableSnapshotTables,
    job_id: &str,
    expected: &sinomo_dfs::DfsNamespaceObjectPrecondition,
    committed: &DfsObjectMetadata,
) -> Result<()> {
    let current = rows.objects.iter().find(|object| {
        object.file_system_id == committed.file_system_id
            && object.path == committed.path
            && object.kind != sinomo_dfs::DfsObjectKind::Tombstone
            && object.tombstone_provenance.is_none()
    });
    match current {
        Some(current) if current == committed => Ok(()),
        Some(current)
            if current.object_id == expected.object_id && current.version == expected.version =>
        {
            Ok(())
        }
        _ => Err(ConversionStop::LegacyHardlinkUnsettled {
            job_id: job_id.to_owned(),
            reason: "the canonical target changed while its fence was held".into(),
        }
        .into()),
    }
}

/// The outer namespace request of a job this conversion settles, with the
/// matching terminal result: Applied for a job settled Completed, Rejected for
/// an aborted one. A request already terminal, or a job without one, is left.
fn settled_request(
    rows: &DfsMetadataTableSnapshotTables,
    job: &DfsNamespaceJobRecord,
    completed: bool,
    at: OffsetDateTime,
) -> Option<DfsNamespaceRequestRecord> {
    let origin = job.origin.as_ref()?;
    let request = rows.namespace_requests.iter().find(|request| {
        request.file_system_id == job.file_system_id
            && request.requester_node_id == origin.requester_node_id
            && request.request_id == origin.request_id
            && request.operation_id == origin.operation_id
            && request.status == DfsNamespaceRequestStatus::Pending
    })?;
    let mut request = request.clone();
    request.status = DfsNamespaceRequestStatus::Completed;
    request.result = Some(if completed {
        DfsNamespaceResult::Applied
    } else {
        DfsNamespaceResult::Rejected {
            reason: format!(
                "cross-shard hardlink job {} was aborted while its shard converted",
                job.job_id
            ),
            posix_error: None,
            current_primary_node_id: None,
        }
    });
    request.posix_coordination_result = None;
    request.last_recovery_error = None;
    request.updated_at = at;
    Some(request)
}

/// `rows` with its namespace jobs replaced and the settled requests,
/// committed targets and their version rows written through the ordinary
/// row projection.
fn settled_rows(
    rows: &DfsMetadataTableSnapshotTables,
    mut namespace_jobs: Vec<DfsNamespaceJobRecord>,
    requests: &[DfsNamespaceRequestRecord],
    objects: &[DfsObjectMetadata],
    versions: &[sinomo_dfs::DfsObjectVersionRecord],
) -> Result<DfsMetadataTableSnapshotTables> {
    namespace_jobs.sort_by(|left, right| left.job_id.cmp(&right.job_id));
    let mut rows = rows.clone();
    rows.namespace_jobs = namespace_jobs;
    if requests.is_empty() && objects.is_empty() && versions.is_empty() {
        return Ok(rows);
    }
    let mut state = sinomo_dfs::DfsMetadataCandidateState::new(rows)?;
    for object in objects {
        state.apply(sinomo_dfs::DfsPreparedMetadataEffect::PutObject {
            object: object.clone(),
        })?;
    }
    for version in versions {
        state.apply(sinomo_dfs::DfsPreparedMetadataEffect::PutVersion {
            version: version.clone(),
        })?;
    }
    for request in requests {
        state.apply(sinomo_dfs::DfsPreparedMetadataEffect::PutNamespaceRequest {
            request: Box::new(request.clone()),
        })?;
    }
    Ok(state.snapshot())
}

/// The capture with the shard's own settlement applied.
pub(super) fn settled_unit(
    captured: &CapturedMetadataUnit,
    settled: &SettledJobs,
) -> Result<CapturedMetadataUnit> {
    let rows = settled_rows(
        &captured.projected_rows,
        settled.namespace_jobs.clone(),
        &settled.requests,
        &settled.objects,
        &[],
    )?;
    captured.with_settled_rows(rows)
}

/// The shard's rows this module settles: its target fences, its hardlink
/// jobs whose only step left is the fence commit, and the parent-count
/// participants it holds.
pub(super) struct OwnPairWork<'a> {
    catalog: &'a [DfsMetadataShardConfig],
    shard: &'a DfsMetadataShardConfig,
    rows: &'a DfsMetadataTableSnapshotTables,
    fences: Vec<&'a DfsNamespaceJobRecord>,
    commits: Vec<&'a DfsNamespaceJobRecord>,
    participants: Vec<&'a DfsNamespaceJobRecord>,
    /// Legacy creations whose alias another shard owns.
    creations: Vec<&'a DfsNamespaceJobRecord>,
}

impl<'a> OwnPairWork<'a> {
    pub(super) fn new(
        catalog: &'a [DfsMetadataShardConfig],
        shard: &'a DfsMetadataShardConfig,
        rows: &'a DfsMetadataTableSnapshotTables,
    ) -> Self {
        Self {
            catalog,
            shard,
            rows,
            fences: Vec::new(),
            commits: Vec::new(),
            participants: Vec::new(),
            creations: Vec::new(),
        }
    }

    /// Take a non-Completed job this module settles.
    pub(super) fn claim(&mut self, job: &'a DfsNamespaceJobRecord) -> Result<bool> {
        if let Some((_, _, committed)) = fence_reservation(job) {
            if owner_of(self.catalog, self.shard, &committed.path)? != self.shard.shard_id {
                return Ok(false);
            }
            self.fences.push(job);
            return Ok(true);
        }
        if only_fence_commit(job).is_some() {
            self.commits.push(job);
            return Ok(true);
        }
        if let Some(participant) = held_participant(job) {
            if owner_of(self.catalog, self.shard, &participant.expected.path)? != self.shard.shard_id {
                return Ok(false);
            }
            participant.validate_declaration()?;
            self.participants.push(job);
            return Ok(true);
        }
        if closed::remote_legacy_creation_shard(self.catalog, self.shard, job).is_some() {
            self.creations.push(job);
            return Ok(true);
        }
        Ok(false)
    }

    /// Finish the shard's own settlement. `namespace_jobs` and `settled` are
    /// the jobs the shard keeps and the job ids settled so far, with their
    /// outcome (`completed` or `aborted`).
    pub(super) fn finish(
        self,
        mut namespace_jobs: Vec<DfsNamespaceJobRecord>,
        mut settled: Vec<(String, String)>,
        at: OffsetDateTime,
    ) -> Result<SettledJobs> {
        let mut objects = BTreeMap::<sinomo_dfs::DfsObjectId, DfsObjectMetadata>::new();
        let mut pending_pairs = Vec::new();
        for job in &self.commits {
            let (_, expected, committed) = only_fence_commit(job).expect("claimed");
            if owner_of(self.catalog, self.shard, &committed.path)? == self.shard.shard_id {
                // The alias change is durable and the fence and canonical
                // target are this shard's: the commit's effect goes into the
                // image and the job is Completed.
                check_fence_target(self.rows, &job.job_id, expected, committed)?;
                objects.insert(committed.object_id.clone(), committed.clone());
                namespace_jobs.push(completed(job, at));
                settled.push((job.job_id.clone(), "completed".into()));
            } else {
                namespace_jobs.push((*job).clone());
                pending_pairs.push(job.job_id.clone());
            }
        }
        for fence in &self.fences {
            let (_, expected, committed) = fence_reservation(fence).expect("claimed");
            let coordinator = self
                .rows
                .namespace_jobs
                .iter()
                .find(|job| job.job_id != fence.job_id && names_fence(job, &fence.job_id));
            let Some(coordinator) = coordinator else {
                namespace_jobs.push((*fence).clone());
                pending_pairs.push(fence.job_id.clone());
                continue;
            };
            let phase = match settled
                .iter()
                .find(|(job_id, _)| *job_id == coordinator.job_id)
            {
                Some((_, outcome)) if outcome == "completed" => CoordinatorPhase::SettledCompleted,
                Some(_) => CoordinatorPhase::SettledAborted,
                None => {
                    return Err(ConversionStop::UnsettledNamespaceJob {
                        job_id: fence.job_id.clone(),
                        kind: format!("{:?}", fence.kind),
                    }
                    .into());
                }
            };
            match settle_target_fence(phase) {
                FenceOutcome::Apply => {
                    check_fence_target(self.rows, &fence.job_id, expected, committed)?;
                    objects.insert(committed.object_id.clone(), committed.clone());
                }
                FenceOutcome::Drop => {}
                FenceOutcome::Keep => namespace_jobs.push((*fence).clone()),
            }
        }
        // A participant's coordinator is another shard's move; its phase
        // comes from the rounds.
        // A remote legacy creation settles from its alias shard's rows.
        for job in self.participants.iter().chain(&self.creations) {
            namespace_jobs.push((*job).clone());
            pending_pairs.push(job.job_id.clone());
        }
        let mut requests = Vec::new();
        for (job_id, outcome) in &settled {
            let job = self
                .rows
                .namespace_jobs
                .iter()
                .find(|job| job.job_id == *job_id)
                .ok_or_else(|| error("a settled job is not in the shard's rows"))?;
            requests.extend(settled_request(self.rows, job, outcome == "completed", at));
        }
        namespace_jobs.sort_by(|left, right| left.job_id.cmp(&right.job_id));
        Ok(SettledJobs {
            namespace_jobs,
            settled,
            requests,
            objects: objects.into_values().collect(),
            pending_pairs,
            settled_at: Some(at),
        })
    }
}

/// A target fence the shard holds for another shard's hardlink job.
#[derive(Clone, Debug)]
struct RemoteFence {
    fence_job_id: String,
    expected: sinomo_dfs::DfsNamespaceObjectPrecondition,
    committed: DfsObjectMetadata,
}

/// A hardlink job of the shard whose only step left commits a fence another
/// shard holds.
#[derive(Clone, Debug)]
struct RemoteFenceCommit {
    job_id: String,
    fence_job_id: String,
    fence_shard: String,
}

/// What one attempt's rounds ask every other shard, and what each answer
/// decides.
pub(super) struct RoundQuestions {
    per_source: BTreeMap<String, SourceQuestion>,
    aliases: Vec<ForeignAliasTarget>,
    parents: Vec<ForeignParent>,
    fences: Vec<RemoteFence>,
    commits: Vec<RemoteFenceCommit>,
    /// Parent-count participants the shard holds for other shards' moves.
    participants: Vec<sinomo_dfs::DfsParentCountParticipant>,
    creations: Vec<closed::RemoteLegacyCreation>,
}

impl RoundQuestions {
    /// Whether this shard settles `job`'s effect on it per pair: the job's
    /// recorded phase names a fence this shard holds for it, so the fence's
    /// outcome is decided from that phase ([`settle_target_fence`]) and the
    /// job does not hold the rounds. So is a job its closed source's own
    /// conversion settles ([`closed`]).
    pub(super) fn settles_per_pair(&self, job: &SourceJobTouchingTarget) -> bool {
        closed::marked_settled_by_own_conversion(job)
            || self.fences.iter().any(|fence| {
                let marker = format!("fence={}", fence.fence_job_id);
                job.phase.split(';').skip(1).any(|part| part == marker)
            })
    }

    /// The questions from the shard's settled capture: every other shard is
    /// asked for each alias target the shard does not hold; the owner of a
    /// foreign parent for that parent; and the shard holding a fence for the
    /// fence.
    pub(super) fn new(
        catalog: &[DfsMetadataShardConfig],
        shard: &DfsMetadataShardConfig,
        unit: &CapturedMetadataUnit,
        settled: &SettledJobs,
    ) -> Result<Self> {
        let (aliases, parents) = crate::namespace_counts::contributions::foreign_references(
            unit,
            catalog,
            COUNT_LIMITS,
        )?;
        let targets = aliases
            .iter()
            .map(|alias| alias.target_object_id.clone())
            .collect::<BTreeSet<_>>();
        let mut per_source = catalog
            .iter()
            .filter(|source| source.shard_id != shard.shard_id)
            .map(|source| {
                (
                    source.shard_id.clone(),
                    SourceQuestion {
                        objects: targets.iter().cloned().collect(),
                        ..Default::default()
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        fn asked<'q>(
            per_source: &'q mut BTreeMap<String, SourceQuestion>,
            source: &str,
        ) -> Result<&'q mut SourceQuestion> {
            per_source.get_mut(source).ok_or_else(|| {
                error(format!(
                    "shard {source} is not another shard of the catalog"
                ))
            })
        }
        for parent in &parents {
            asked(&mut per_source, &parent.parent_shard)?
                .directories
                .push(parent.parent_path.clone());
        }
        let (mut fences, mut commits, mut participants) = (Vec::new(), Vec::new(), Vec::new());
        let mut creations = Vec::new();
        for job_id in &settled.pending_pairs {
            let job = settled
                .namespace_jobs
                .iter()
                .find(|job| job.job_id == *job_id)
                .ok_or_else(|| {
                    error("a job kept for per-pair settlement is not in the settled rows")
                })?;
            if let Some(participant) = held_participant(job) {
                // The coordinator reports the move it holds this participant
                // for with every round, as a job touching this shard.
                participants.push(participant.clone());
            } else if let Some((_, expected, committed)) = fence_reservation(job) {
                fences.push(RemoteFence {
                    fence_job_id: job.job_id.clone(),
                    expected: expected.clone(),
                    committed: committed.clone(),
                });
            } else if let Some((fence_job_id, _, committed)) = only_fence_commit(job) {
                let fence_shard = owner_of(catalog, shard, &committed.path)?;
                asked(&mut per_source, &fence_shard)?
                    .jobs
                    .push(fence_job_id.to_owned());
                commits.push(RemoteFenceCommit {
                    job_id: job.job_id.clone(),
                    fence_job_id: fence_job_id.to_owned(),
                    fence_shard,
                });
            } else if let Some(alias_shard) = closed::remote_legacy_creation_shard(catalog, shard, job) {
                creations.push(closed::RemoteLegacyCreation { job: job.clone(), alias_shard });
            } else {
                return Err(error(
                    "a job kept for per-pair settlement is neither a fence, a fence commit, a parent participant nor a legacy creation",
                ));
            }
        }
        for question in per_source.values_mut() {
            question.directories.sort();
            question.directories.dedup();
            question.jobs.sort();
            question.jobs.dedup();
        }
        Ok(Self {
            per_source,
            aliases,
            parents,
            fences,
            commits,
            participants,
            creations,
        })
    }

    /// What the rounds ask `source`.
    pub(super) fn for_source(&self, source: &str) -> SourceQuestion {
        self.per_source.get(source).cloned().unwrap_or_default()
    }
}

/// How a fence the shard holds for another shard's job is settled.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum FenceOutcome {
    /// Apply the committed target and drop the fence.
    Apply,
    /// Drop the fence without applying it.
    Drop,
    /// Keep the fence; the shard's counts are those from before it.
    Keep,
}

/// The phase a fence's coordinating job has recorded, as the fence's shard
/// sees it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CoordinatorPhase<'a> {
    /// The job is Running with this recorded phase.
    Running(&'a str),
    /// This conversion settled the job forward.
    SettledCompleted,
    /// This conversion settled the job by aborting it.
    SettledAborted,
    /// No job naming the fence was found.
    Unknown,
}

/// Settle a target fence from its coordinator's recorded phase: past the
/// alias change (its next step is the fence commit) or settled Completed
/// applies the committed target; settled as aborted drops the fence
/// unapplied; any other phase keeps it.
pub(super) fn settle_target_fence(phase: CoordinatorPhase<'_>) -> FenceOutcome {
    match phase {
        CoordinatorPhase::Running(phase) if phase.split(';').next() == Some(FENCE_COMMIT_PHASE) => {
            FenceOutcome::Apply
        }
        CoordinatorPhase::SettledCompleted => FenceOutcome::Apply,
        CoordinatorPhase::SettledAborted => FenceOutcome::Drop,
        CoordinatorPhase::Running(_) | CoordinatorPhase::Unknown => FenceOutcome::Keep,
    }
}

/// The phase of the reported job that installs or commits `fence_job_id`.
fn reported_phase<'a>(answers: &'a ContributionRound, fence_job_id: &str) -> CoordinatorPhase<'a> {
    let marker = format!("fence={fence_job_id}");
    answers
        .values()
        .flat_map(|answer| &answer.running_jobs_touching_target)
        .find(|job| job.phase.split(';').skip(1).any(|part| part == marker))
        .map_or(CoordinatorPhase::Unknown, |job| {
            CoordinatorPhase::Running(&job.phase)
        })
}

/// How a parent-count participant the shard holds is settled.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ParticipantOutcome {
    /// Apply the participant's successor and record it Applied.
    Apply,
    /// Keep the participant Reserved; the shard's count is the one from
    /// before its delta.
    Keep,
}

/// What the rounds report of the move a participant belongs to.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum ParticipantCoordinator {
    /// The move reports its plan phase, and whether its frames still hold
    /// the move of its root out of or into the participant's directory.
    Reported {
        plan: Option<sinomo_dfs::DfsParentCountPhase>,
        effect_pending: bool,
    },
    /// No reported job names the participant.
    Unknown,
}

/// Settle a held participant from its coordinator's recorded plan phase
/// (DESIGN-BD C.4): Issued or Settling applies the successor, unless an
/// Issued move's frames still hold the effect on the participant's
/// directory; Preparing, Aborting and an unreported phase keep it.
pub(super) fn settle_parent_participant(coordinator: ParticipantCoordinator) -> ParticipantOutcome {
    use sinomo_dfs::DfsParentCountPhase as Phase;
    match coordinator {
        ParticipantCoordinator::Reported {
            plan: Some(Phase::Settling),
            ..
        }
        | ParticipantCoordinator::Reported {
            plan: Some(Phase::Issued),
            effect_pending: false,
        } => ParticipantOutcome::Apply,
        ParticipantCoordinator::Reported { .. } | ParticipantCoordinator::Unknown => {
            ParticipantOutcome::Keep
        }
    }
}

/// The reported phase of the move whose plan declares `participant`: a job
/// of the participant's coordinating move whose phase names it.
fn reported_participant_coordinator(
    answers: &ContributionRound,
    participant: &sinomo_dfs::DfsParentCountParticipant,
) -> Result<ParticipantCoordinator> {
    let id = &participant.participant_id;
    let named = format!("{PARTICIPANT_PHASE}{id}");
    let Some(job) = answers
        .values()
        .flat_map(|answer| &answer.running_jobs_touching_target)
        .find(|job| {
            job.job_id == participant.binding.job_id
                && job.phase.split(';').skip(1).any(|part| part == named)
        })
    else {
        return Ok(ParticipantCoordinator::Unknown);
    };
    let parts = job.phase.split(';').skip(1).collect::<Vec<_>>();
    let plan = parts
        .iter()
        .find_map(|part| part.strip_prefix(PARENT_COUNTS_PHASE))
        .map(|tag| serde_json::from_value(serde_json::Value::String(tag.to_owned())))
        .transpose()
        .map_err(error)?;
    let pending = format!("{PARTICIPANT_EFFECT_PENDING_PHASE}{id}");
    Ok(ParticipantCoordinator::Reported {
        plan,
        effect_pending: parts.iter().any(|part| *part == pending),
    })
}

/// The live parent row of `participant` must still be the generation it
/// expected.
fn check_participant_parent(
    rows: &DfsMetadataTableSnapshotTables,
    participant: &sinomo_dfs::DfsParentCountParticipant,
) -> Result<()> {
    let expected = &participant.expected;
    let current = rows.objects.iter().find(|object| {
        object.file_system_id == expected.file_system_id && object.object_id == expected.object_id
    });
    if current != Some(expected) {
        return Err(error(format!(
            "the parent of participant {} changed while it was reserved",
            participant.participant_id
        )));
    }
    Ok(())
}

/// Whether an answer was read from the source's current tables.
fn from_current_tables(answer: &SourceContributionAnswer) -> bool {
    serde_json::from_str::<serde_json::Value>(&answer.basis).is_ok_and(|basis| {
        basis.get("read").and_then(serde_json::Value::as_str) == Some("current-tables")
    })
}

/// The per-pair settlement one accepted round decides.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct PairSettlement {
    fences: BTreeMap<String, FenceOutcome>,
    /// Fence-commit-only jobs settled forward.
    completed: BTreeSet<String>,
    /// Held parent participants, by participant id.
    participants: BTreeMap<String, ParticipantOutcome>,
    /// Remote legacy creations, by job id.
    creations: BTreeMap<String, closed::CreationOutcome>,
}

/// What an accepted round decides for the shard's pairs.
#[derive(Debug)]
pub(super) enum PairDecision {
    Settled(PairSettlement),
    /// The round is complete but a pair waits; the evidence says on what.
    Wait(String),
}

/// The per-pair checks and settlements of one complete, stable round: the
/// hook the contribution rounds call before they accept. A failed check is a
/// typed stop of this shard only.
pub(super) fn settle_pairs(
    questions: &RoundQuestions,
    round: &ContributionRound,
) -> Result<PairDecision> {
    for alias in &questions.aliases {
        if !round
            .values()
            .any(|answer| answer.held.objects.contains(&alias.target_object_id))
        {
            return Err(ConversionStop::DanglingAlias {
                alias_path: alias.alias_path.clone(),
                alias_object_id: alias.alias_object_id.to_string(),
                target_object_id: alias.target_object_id.to_string(),
            }
            .into());
        }
    }
    for parent in &questions.parents {
        let held = round
            .get(&parent.parent_shard)
            .is_some_and(|answer| answer.held.directories.contains(&parent.parent_path));
        if !held {
            return Err(ConversionStop::ParentDirectoryAbsent {
                path: parent.path.clone(),
                parent_path: parent.parent_path.clone(),
                parent_shard: parent.parent_shard.clone(),
            }
            .into());
        }
    }
    let mut settlement = PairSettlement::default();
    for fence in &questions.fences {
        let outcome = settle_target_fence(reported_phase(round, &fence.fence_job_id));
        settlement
            .fences
            .insert(fence.fence_job_id.clone(), outcome);
    }
    for participant in &questions.participants {
        let outcome = settle_parent_participant(reported_participant_coordinator(round, participant)?);
        settlement
            .participants
            .insert(participant.participant_id.clone(), outcome);
    }
    for commit in &questions.commits {
        let answer = round.get(&commit.fence_shard).ok_or_else(|| {
            error(format!(
                "the round has no answer from fence shard {}",
                commit.fence_shard
            ))
        })?;
        if !answer.held.jobs.contains(&commit.fence_job_id) {
            settlement.completed.insert(commit.job_id.clone());
        } else if from_current_tables(answer) {
            return Err(ConversionStop::TargetFenceNeedsRelease {
                job_id: commit.job_id.clone(),
                fence_job_id: commit.fence_job_id.clone(),
                fence_shard: commit.fence_shard.clone(),
            }
            .into());
        } else if !closed::old_image_records_conversion(answer) {
            return Ok(PairDecision::Wait(format!(
                "fence {} of job {} is held by shard {}, which has not recorded its conversion",
                commit.fence_job_id, commit.job_id, commit.fence_shard
            )));
        }
        // Otherwise the job stays Running in the image ([`closed`]).
    }
    for creation in &questions.creations {
        let answer = round.get(&creation.alias_shard).ok_or_else(|| {
            error(format!("the round has no answer from alias shard {}", creation.alias_shard))
        })?;
        let outcome = closed::settle_remote_legacy_creation(creation, answer)?;
        settlement.creations.insert(creation.job.job_id.clone(), outcome);
    }
    Ok(PairDecision::Settled(settlement))
}

/// The capture with the shard's own settlement and the per-pair settlement
/// of the accepted round applied.
pub(super) fn with_pair_settlement(
    captured: &CapturedMetadataUnit,
    settled: &SettledJobs,
    questions: &RoundQuestions,
    accepted: &ContributionRound,
) -> Result<CapturedMetadataUnit> {
    let PairDecision::Settled(settlement) = settle_pairs(questions, accepted)? else {
        return Err(error(
            "an accepted contribution round still waits on a pair",
        ));
    };
    let at = settled.settled_at.unwrap_or_else(OffsetDateTime::now_utc);
    let rows = settled_rows(
        &captured.projected_rows,
        settled.namespace_jobs.clone(),
        &settled.requests,
        &settled.objects,
        &[],
    )?;
    let mut jobs = Vec::with_capacity(rows.namespace_jobs.len());
    let (mut requests, mut objects, mut versions) = (Vec::new(), Vec::new(), Vec::new());
    for job in &rows.namespace_jobs {
        if let Some(outcome) = settlement.participants.get(&job.job_id) {
            let participant = questions
                .participants
                .iter()
                .find(|participant| participant.participant_id == job.job_id)
                .expect("a held participant");
            match outcome {
                ParticipantOutcome::Apply => {
                    check_participant_parent(&rows, participant)?;
                    objects.push(participant.successor.clone());
                    versions.push(participant.applied_version()?);
                    jobs.push(participant.recorded_job(sinomo_dfs::DfsParentCountState::Applied, at));
                }
                ParticipantOutcome::Keep => jobs.push(job.clone()),
            }
            continue;
        }
        if let Some(outcome) = settlement.fences.get(&job.job_id) {
            let fence = questions
                .fences
                .iter()
                .find(|fence| fence.fence_job_id == job.job_id)
                .expect("asked fence");
            match outcome {
                FenceOutcome::Apply => {
                    check_fence_target(&rows, &job.job_id, &fence.expected, &fence.committed)?;
                    objects.push(fence.committed.clone());
                    jobs.push(closed::committed_fence(job, at));
                }
                FenceOutcome::Drop => {}
                FenceOutcome::Keep => jobs.push(job.clone()),
            }
            continue;
        }
        if settlement.completed.contains(&job.job_id) {
            requests.extend(settled_request(&rows, job, true, at));
            jobs.push(completed(job, at));
            continue;
        }
        if let Some(outcome) = settlement.creations.get(&job.job_id) {
            let completes = *outcome == closed::CreationOutcome::Completed;
            requests.extend(settled_request(&rows, job, completes, at));
            if completes {
                jobs.push(completed(job, at));
            }
            continue;
        }
        jobs.push(job.clone());
    }
    captured.with_settled_rows(settled_rows(&rows, jobs, &requests, &objects, &versions)?)
}

#[cfg(test)]
mod tests;
