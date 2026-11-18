//! Work of a closed shard that its own conversion settles, so that another
//! closed shard does not wait for that conversion because of it.
//!
//! A closed shard S runs no step of its jobs until it activates, and a step
//! that would change what S contributes to another closed shard T resolves
//! T's objects first, so it waits for T. Some of S's jobs that touch T are
//! settled by S's own conversion, from S's rows or per pair, and that
//! settlement changes none of T's count inputs:
//!
//! - a hardlink job whose only step left is its target fence commit: its
//!   alias change is durable in S's rows, and a fence T holds for it settles
//!   from its recorded phase ([`super::settle_target_fence`]);
//! - a target fence S keeps on its own canonical target for another shard's
//!   hardlink job: its outcome changes only that target;
//! - a legacy cross-shard hardlink creation (decision Q-C2): its settlement
//!   changes only S's job and request rows, and whether its alias was
//!   adopted is the alias shard's own rows.
//!
//! S's old-image answer marks each such job `;settled-by-own-conversion`
//! ([`mark_settled_by_own_conversion`]), and T's rounds settle it per pair
//! instead of waiting for S to convert. Its Pending outer request takes its
//! result from the job's settlement and is not reported beside it. Without
//! the mark, two closed shards that each hold such a job toward the other
//! would each wait for the other's conversion.
//!
//! A legacy creation whose alias another shard A owns is settled from A's
//! live aliases in S's accepted round ([`settle_remote_legacy_creation`]):
//! Completed when the adopted alias is live at its path, Aborted when A
//! holds no live alias of the adopted identity. A's live aliases naming S's
//! target cannot change while S is closed, since every change resolves the
//! target, and A's conversion carries them unchanged. So S reaches the same
//! decision from A's old image as from A's current tables, whichever of the
//! two converts first, and A never waits for S.
//!
//! A fence-commit-only job of S whose fence closed T holds stays Running in
//! S's image, and S does not wait for T's conversion. T applies the fence
//! from the job's phase, which it reads from S's old image or, once S is
//! open, from S's current tables, and records the fence Completed, as the
//! fence's commit would ([`committed_fence`]). Once S is open the job's
//! commit step parks on T's closed storage and is woken by T's activation;
//! it then finds the fence committed. S waits only until T has recorded its
//! conversion, since the park reads that record
//! ([`old_image_records_conversion`]). Two closed shards whose fence commits
//! cross therefore both convert, in either order.
use super::*;
use sinomo_dfs::DfsObjectKind;

/// The phase part an old image appends to a job its own conversion settles.
const SETTLED_BY_OWN_CONVERSION_PHASE: &str = "settled-by-own-conversion";
/// The family prefix of a reported Pending namespace request.
const REQUEST_FAMILY: &str = "namespace-request:";
/// The family of a reported staging write intent.
const INTENT_FAMILY: &str = "write-intent";

/// The shard the catalog gives `path`, if any.
fn owner(
    catalog: &[DfsMetadataShardConfig],
    job: &DfsNamespaceJobRecord,
    path: &str,
) -> Option<String> {
    sinomo_dfs::select_metadata_shard_config_for_path(&job.file_system_id, catalog, path)
        .ok()
        .flatten()
        .map(|owner| owner.shard_id)
}

/// The alias path of a Running legacy cross-shard hardlink creation.
fn legacy_creation_alias_path(job: &DfsNamespaceJobRecord) -> Option<&str> {
    let legacy = job.kind == DfsNamespaceJobKind::HardlinkAcrossShardPrimaries
        && job.status == DfsNamespaceJobStatus::Running
        && matches!(
            job.pending_frames.first(),
            Some(DfsNamespaceJobFrame::CrossShardHardlinkComplete { .. })
        );
    if legacy {
        job.target_path.as_deref()
    } else {
        None
    }
}

/// The shard other than `shard` that owns the alias path of `job`, when
/// `job` is a legacy cross-shard hardlink creation.
pub(super) fn remote_legacy_creation_shard(
    catalog: &[DfsMetadataShardConfig],
    shard: &DfsMetadataShardConfig,
    job: &DfsNamespaceJobRecord,
) -> Option<String> {
    let alias_path = legacy_creation_alias_path(job)?;
    owner(catalog, job, alias_path).filter(|owner| *owner != shard.shard_id)
}

/// Whether the conversion of `shard` settles `job` itself, from its rows or
/// per pair, leaving every other shard's count inputs as they are.
fn settled_by_own_conversion(
    catalog: &[DfsMetadataShardConfig],
    shard: &DfsMetadataShardConfig,
    job: &DfsNamespaceJobRecord,
) -> bool {
    if let Some((_, _, committed)) = fence_reservation(job) {
        return owner(catalog, job, &committed.path).as_deref() == Some(shard.shard_id.as_str());
    }
    only_fence_commit(job).is_some()
        || legacy_creation_alias_path(job).is_some_and(|path| owner(catalog, job, path).is_some())
}

/// `work`, an old image's reported work toward one target, with each job its
/// own conversion settles marked and the Pending outer requests of those jobs
/// left to them. `tables` are the old image's rows and `source` its shard.
pub(in crate::authority) fn mark_settled_by_own_conversion(
    tables: &DfsMetadataTableSnapshotTables,
    catalog: &[DfsMetadataShardConfig],
    source: &str,
    work: Vec<SourceJobTouchingTarget>,
) -> Result<Vec<SourceJobTouchingTarget>> {
    let shard = catalog
        .iter()
        .find(|shard| shard.shard_id == source)
        .ok_or_else(|| error("contribution source left the catalog"))?;
    let mut settled = BTreeSet::new();
    let mut requests = BTreeSet::new();
    for job in &tables.namespace_jobs {
        if !settled_by_own_conversion(catalog, shard, job) {
            continue;
        }
        settled.insert(job.job_id.as_str());
        let Some(origin) = &job.origin else { continue };
        // Reported as `TargetScope::unsettled_work` names a Pending request.
        requests.extend(
            tables
                .namespace_requests
                .iter()
                .filter(|request| {
                    request.status == DfsNamespaceRequestStatus::Pending
                        && request.file_system_id == job.file_system_id
                        && request.requester_node_id == origin.requester_node_id
                        && request.request_id == origin.request_id
                        && request.operation_id == origin.operation_id
                })
                .map(|request| format!("{}:{}", request.requester_node_id, request.request_id)),
        );
    }
    Ok(work
        .into_iter()
        .filter_map(|mut work| {
            if work.family.starts_with(REQUEST_FAMILY) {
                return (!requests.contains(&work.job_id)).then_some(work);
            }
            if work.family != INTENT_FAMILY && settled.contains(work.job_id.as_str()) {
                work.phase.push(';');
                work.phase.push_str(SETTLED_BY_OWN_CONVERSION_PHASE);
            }
            Some(work)
        })
        .collect())
}

/// Whether a source reported `job` as one its own conversion settles.
pub(super) fn marked_settled_by_own_conversion(job: &SourceJobTouchingTarget) -> bool {
    job.phase
        .split(';')
        .skip(1)
        .any(|part| part == SETTLED_BY_OWN_CONVERSION_PHASE)
}

/// Whether `answer` was read from the old image of a shard with a storage
/// record: a closed shard whose conversion has begun. A fence-commit step
/// parks on such a shard until it is Active; a shard without a record writes
/// it once its conversion has captured its copy.
pub(super) fn old_image_records_conversion(answer: &SourceContributionAnswer) -> bool {
    let absent = crate::authority::contribution_sources::storage_record_digest(None);
    serde_json::from_str::<serde_json::Value>(&answer.basis).is_ok_and(|basis| {
        basis.get("read").and_then(serde_json::Value::as_str) == Some("old-image")
            && basis
                .get("storage")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|storage| storage != absent)
    })
}

/// A target fence the shard applied for another shard's job, recorded as the
/// fence's own commit records it: Completed, its reservation kept. The job's
/// commit step, which can run after the shard activates, then finds the
/// fence committed, whatever the target has become since.
pub(super) fn committed_fence(
    job: &DfsNamespaceJobRecord,
    at: OffsetDateTime,
) -> DfsNamespaceJobRecord {
    let mut job = job.clone();
    job.status = DfsNamespaceJobStatus::Completed;
    job.processed_object_count = 1;
    job.updated_at = at;
    job.completed_at = Some(at);
    job
}

/// A legacy cross-shard hardlink creation the shard coordinates whose alias
/// path another shard owns.
#[derive(Clone, Debug)]
pub(super) struct RemoteLegacyCreation {
    pub(super) job: DfsNamespaceJobRecord,
    pub(super) alias_shard: String,
}

/// How a remote legacy creation settles.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CreationOutcome {
    /// The alias is live: the job is Completed and its request Applied.
    Completed,
    /// The alias was never adopted: the job is dropped and its request
    /// Rejected.
    Aborted,
}

/// Settle a remote legacy creation from the live aliases its alias shard
/// reported in the accepted round, by the rule
/// [`crate::legacy_wire::hardlink_settlement::settle_legacy_hardlink_job`]
/// applies to the alias shard's rows: Completed when the adopted alias is
/// live at its path naming the job's target (or, once the adopt frame was
/// popped, when such an alias is live there), Aborted when no live alias
/// carries the adopted identity. Anything else fails closed.
pub(super) fn settle_remote_legacy_creation(
    creation: &RemoteLegacyCreation,
    alias_answer: &SourceContributionAnswer,
) -> Result<CreationOutcome> {
    let job = &creation.job;
    let unsettled = |reason: &str| -> MetadataBackendError {
        ConversionStop::LegacyHardlinkUnsettled {
            job_id: job.job_id.clone(),
            reason: reason.into(),
        }
        .into()
    };
    let alias_path = job
        .target_path
        .as_deref()
        .ok_or_else(|| unsettled("the job has no alias path"))?;
    let (target, adopt) = match job.pending_frames.as_slice() {
        [DfsNamespaceJobFrame::CrossShardHardlinkComplete { target_object_id }] => {
            (target_object_id, None)
        }
        [
            DfsNamespaceJobFrame::CrossShardHardlinkComplete { target_object_id },
            DfsNamespaceJobFrame::CrossShardHardlinkAdopt {
                alias,
                target_object_id: adopted,
                ..
            },
        ] if adopted == target_object_id => (target_object_id, Some(alias)),
        _ => return Err(unsettled("the job has no settleable creation frame stack")),
    };
    let mut live = alias_answer
        .contributions
        .aliases
        .iter()
        .filter(|alias| alias.file_system_id == job.file_system_id);
    let Some(alias) = adopt else {
        // The adopt frame was popped only after the alias shard accepted it.
        return if live
            .any(|alias| alias.alias_path == alias_path && alias.target_object_id == *target)
        {
            Ok(CreationOutcome::Completed)
        } else {
            Err(unsettled(
                "the job recorded its adoption but its alias shard holds no live alias at its path",
            ))
        };
    };
    if alias.kind != DfsObjectKind::Hardlink
        || alias.path != alias_path
        || alias.file_system_id != job.file_system_id
        || alias.link_target_object_id.as_ref() != Some(target)
    {
        return Err(unsettled("the job's adopt frame differs from the job"));
    }
    let carriers = live
        .filter(|live| live.alias_object_id == alias.object_id)
        .collect::<Vec<_>>();
    match carriers.as_slice() {
        [] => Ok(CreationOutcome::Aborted),
        [carrier] if carrier.alias_path == alias_path && carrier.target_object_id == *target => {
            Ok(CreationOutcome::Completed)
        }
        _ => Err(unsettled(
            "its alias shard holds the adopted alias identity elsewhere",
        )),
    }
}

#[cfg(test)]
mod tests;
