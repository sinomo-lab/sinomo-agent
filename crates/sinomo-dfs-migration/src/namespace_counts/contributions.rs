//! Per-shard namespace counts from cross-shard contributions.
//!
//! A shard T is converted on its own. Its counts are its own owned rows plus
//! what every other shard S contributes to them: live aliases S owns that
//! name T's canonical objects, and live directories S owns whose parent path
//! T owns. This is conversion DATA under the census rules, not authority;
//! the caller keeps each source settled and fenced while its contribution is
//! in use.
//!
//! Attribution rules:
//! - Rows are attributed to the current catalog owner of their path, as in
//!   the census. Ownership is compared by assignment only, so a takeover that
//!   changes a shard's primary or epoch does not invalidate a contribution.
//! - S reports every live alias it owns, whatever the target: S cannot tell
//!   which shard holds a target object id. T counts an alias exactly when its
//!   target id is one of T's live canonical rows; an alias naming any other id
//!   is left to the shard that holds it. So, unlike the complete census, one
//!   shard cannot detect an alias whose target no shard holds.
//! - A contributed child directory must name a parent path T owns, and that
//!   parent must be a live directory in T, or the plan fails closed.
//! - A directory that a source's Running cross-shard directory move has not
//!   placed yet counts toward the parent at the path the move places it, when
//!   T holds that parent: the directory the move adopted there carries a
//!   count that includes it ([`moving`]).
//! - Aliases are deduplicated by alias object id and child directories by
//!   child object id, across T's own rows and every contribution: an entry
//!   that moved between shards can appear in two of them and counts once. The
//!   same identity with a different target or parent fails closed.
//! - A non-directory row whose parent path another shard owns (possible only
//!   under hash routing) is not a count input. Its parent's existence is
//!   checked by the converting shard, which asks the parent's owner whether
//!   the parent is a live directory there ([`foreign_references`]).
//! - Likewise an alias whose target is none of the shard's own rows is
//!   checked by asking every other shard whether it holds the target as a
//!   live canonical row; one that no shard holds is dangling.
#![cfg_attr(not(test), allow(dead_code, reason = "consumed by the per-shard conversion driver"))]
use super::*;
use sinomo_dfs::DfsMetadataShardAssignment;
use std::collections::btree_map::Entry;

mod moving;
pub(crate) use moving::is_kept_destination_reservation;
pub use moving::NamespaceCountMovingDirectoryContribution;

/// One live alias its source shard owns. Its target may be owned by any shard.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct NamespaceCountAliasContribution {
    pub file_system_id: DfsFileSystemId,
    pub alias_object_id: DfsObjectId,
    pub alias_path: String,
    pub target_object_id: DfsObjectId,
}

/// One live directory its source shard owns whose parent path the target
/// shard owns.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct NamespaceCountChildDirectoryContribution {
    pub file_system_id: DfsFileSystemId,
    pub parent_path: String,
    pub child_object_id: DfsObjectId,
    pub child_path: String,
}

/// What one settled source shard contributes to one target shard's counts.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ShardCountContributions {
    pub source_shard: DfsMetadataShardId,
    pub target_shard: DfsMetadataShardId,
    pub aliases: Vec<NamespaceCountAliasContribution>,
    pub child_directories: Vec<NamespaceCountChildDirectoryContribution>,
    /// Directories the source's Running cross-shard directory moves still
    /// have to place under a parent the target owns ([`moving`]).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub moving_directories: Vec<NamespaceCountMovingDirectoryContribution>,
}

fn same_ownership(
    left: &[DfsMetadataShardConfig],
    right: &[DfsMetadataShardConfig],
) -> Result<bool> {
    let ownership = |catalog| -> Result<BTreeMap<ScopeKey, DfsMetadataShardAssignment>> {
        Ok(catalog_by_scope(catalog)?
            .into_iter()
            .map(|(scope, shard)| (scope, shard.assignment))
            .collect())
    };
    Ok(ownership(left)? == ownership(right)?)
}

/// How an extraction treats the source's own unsettled namespace and write
/// work: non-Completed jobs, Pending requests and staging intents.
#[derive(Clone, Copy, Eq, PartialEq)]
enum SourceWork {
    /// Refused, as the census refuses it.
    Settled,
    /// Refused, except the rows the shard keeps for another shard's job
    /// through its conversion: target fences ([`is_kept_target_fence`]) and
    /// Reserved parent-count participants ([`is_kept_parent_participant`]);
    /// destination reservations ([`is_kept_destination_reservation`]); and its
    /// own hardlink jobs whose only step left commits a fence another shard
    /// holds ([`is_kept_fence_commit`]), with their Pending outer requests.
    SettledKeepingPairRows,
    /// Left to the caller, which reports the work that touches the target.
    /// Other original work without a proven outcome is still refused: its
    /// effects on the rows are not known.
    Reported,
}

/// Admit one captured source against the catalog the counts attribute with.
fn admit_shard_source(
    captured: &CapturedMetadataUnit,
    catalog: &[DfsMetadataShardConfig],
    work: SourceWork,
) -> Result<()> {
    let scopes = catalog_by_scope(catalog)?;
    match work {
        SourceWork::Settled => admit_settled_count_source(captured)?,
        SourceWork::SettledKeepingPairRows => {
            admit_count_source_identity(captured, captured.has_unresolved_work())?;
            if has_unrecovered_preparation(captured)
                || has_unsettled_namespace_work_beside(&captured.projected_rows, |job| {
                    is_kept_target_fence(job)
                        || is_kept_parent_participant(job)
                        || is_kept_destination_reservation(job)
                        || is_kept_fence_commit(job)
                })
            {
                return Err(invalid(
                    "namespace-count maintenance requires settled original namespace and write operations",
                ));
            }
        }
        SourceWork::Reported => {
            admit_count_source_identity(captured, captured.has_unresolved_work_beside_recorded_rows())?;
            if has_unrecovered_preparation(captured) {
                return Err(invalid(
                    "namespace-count source has a preparation its recovery does not replay",
                ));
            }
        }
    }
    if !same_ownership(&captured.source.projection_assignments, catalog)? {
        return Err(invalid(
            "namespace-count source catalog assigns paths differently from the count catalog",
        ));
    }
    source_scopes(&captured.source, &scopes)?;
    Ok(())
}

/// Extract S's contributions to `target_shard` from S's captured image, under
/// the census rules for a single source. A source with unsettled work is
/// refused exactly as the census refuses it.
pub(crate) fn contributions_from_captured(
    captured: &CapturedMetadataUnit,
    catalog: &[DfsMetadataShardConfig],
    target_shard: &str,
    limits: NamespaceCountMaintenanceLimits,
) -> Result<ShardCountContributions> {
    extract_contributions(captured, catalog, target_shard, limits, SourceWork::Settled)
}

/// [`contributions_from_captured`] for a source that may still hold
/// non-Completed namespace jobs, Pending requests or staging intents. The
/// rows are read under the same census rules; the caller reports the
/// unsettled work that touches the target beside them, so a reader never
/// accepts these contributions while that work is in flight.
pub(crate) fn contributions_beside_unsettled_work(
    captured: &CapturedMetadataUnit,
    catalog: &[DfsMetadataShardConfig],
    target_shard: &str,
    limits: NamespaceCountMaintenanceLimits,
) -> Result<ShardCountContributions> {
    extract_contributions(captured, catalog, target_shard, limits, SourceWork::Reported)
}

fn extract_contributions(
    captured: &CapturedMetadataUnit,
    catalog: &[DfsMetadataShardConfig],
    target_shard: &str,
    limits: NamespaceCountMaintenanceLimits,
    work: SourceWork,
) -> Result<ShardCountContributions> {
    preflight_count_input(std::iter::once(captured), 0, limits)?;
    let target_shard = DfsMetadataShardId::new(target_shard);
    let source_shard = &captured.source.physical_shard;
    if source_shard == &target_shard || catalog.iter().all(|shard| shard.shard_id != target_shard.0) {
        return Err(invalid(
            "namespace-count contribution target must be another catalog shard",
        ));
    }
    admit_shard_source(captured, catalog, work)?;
    // Uniqueness is checked as in the census; the rows themselves stay borrowed.
    let mut owned = CountedRows::default();
    let mut aliases = Vec::new();
    let mut child_directories = Vec::new();
    for object in owned_live_rows(captured, Some(catalog), limits)? {
        owned.insert(source_shard, object)?;
        match object.kind {
            DfsObjectKind::Hardlink => aliases.push(NamespaceCountAliasContribution {
                file_system_id: object.file_system_id.clone(),
                alias_object_id: object.object_id.clone(),
                alias_path: object.path.clone(),
                target_object_id: object
                    .link_target_object_id
                    .clone()
                    .ok_or_else(|| invalid("namespace alias has no canonical target"))?,
            }),
            DfsObjectKind::Directory => {
                if let Some(parent_path) = parent(&object.path)
                    && current_owner(&object.file_system_id, catalog, parent_path)?.shard_id
                        == target_shard.0
                {
                    child_directories.push(NamespaceCountChildDirectoryContribution {
                        file_system_id: object.file_system_id.clone(),
                        parent_path: parent_path.to_owned(),
                        child_object_id: object.object_id.clone(),
                        child_path: object.path.clone(),
                    });
                }
            }
            _ => {}
        }
    }
    Ok(ShardCountContributions {
        source_shard: source_shard.clone(),
        target_shard,
        aliases,
        child_directories,
        moving_directories: Vec::new(),
    })
}

/// Counts for the one shard `target` captures: its own rows give base counts
/// and local increments; `contributions`, one per other shard, give the
/// foreign increments. Existing initialized counts are verified and only
/// absent ones filled, as in the census.
pub(crate) fn plan_shard_namespace_count_maintenance(
    catalog: &[DfsMetadataShardConfig],
    target: &NamespaceCountMaintenanceSource,
    contributions: &[ShardCountContributions],
    limits: NamespaceCountMaintenanceLimits,
) -> Result<NamespaceCountMaintenancePlan> {
    let mut contributed = 0usize;
    for contribution in contributions {
        contributed = contributed
            .checked_add(contribution.aliases.len())
            .and_then(|count| count.checked_add(contribution.child_directories.len()))
            .ok_or_else(|| invalid("namespace census record count overflow"))?;
    }
    preflight_count_input(std::iter::once(target.captured.as_ref()), contributed, limits)?;
    admit_shard_source(&target.captured, catalog, SourceWork::SettledKeepingPairRows)?;
    let shard = &target.captured.source.physical_shard;
    let mut owned = CountedRows::default();
    for object in owned_live_rows(&target.captured, Some(catalog), limits)? {
        owned.insert(shard, object)?;
    }
    let mut counts = local_counts(&owned, CountScope::Shard { catalog, shard }, limits)?;
    let moves = moving::KeptMoves::new(&target.captured.projected_rows.namespace_jobs);
    add_contributed_counts(catalog, shard, &owned, &moves, contributions, &mut counts, limits)?;
    let updates = verified_count_updates(counts, owned.rows, limits)?;
    finish_count_plan(catalog.to_vec(), std::slice::from_ref(target), updates, limits)
}

/// Per-shard activation check of a built image of `shard`: every live row
/// the shard owns under `catalog` carries exactly the count its own rows and
/// `contributions` give, and none is absent. This checks one shard against
/// the contributions its conversion sealed; no other shard's rows are read.
pub(crate) fn validate_shard_image_counts(
    catalog: &[DfsMetadataShardConfig],
    shard: &DfsMetadataShardId,
    file_system_ids: &[DfsFileSystemId],
    rows: &sinomo_dfs::DfsMetadataTableSnapshotTables,
    contributions: &[ShardCountContributions],
    limits: NamespaceCountMaintenanceLimits,
) -> Result<()> {
    let mut owned = CountedRows::default();
    for object in owned_live_rows_in(shard, file_system_ids, rows, Some(catalog), limits)? {
        owned.insert(shard, object)?;
    }
    let mut counts = local_counts(&owned, CountScope::Shard { catalog, shard }, limits)?;
    let moves = moving::KeptMoves::new(&rows.namespace_jobs);
    add_contributed_counts(catalog, shard, &owned, &moves, contributions, &mut counts, limits)?;
    if !verified_count_updates(counts, owned.rows, limits)?.is_empty() {
        return Err(invalid("built image leaves a namespace count uninitialized"));
    }
    Ok(())
}

/// A Running target fence another shard's hardlink job installed on this
/// shard and that the shard keeps through its conversion: its count change
/// waits for that job's fence commit, so the shard's rows carry the counts
/// from before the fence.
pub(crate) fn is_kept_target_fence(job: &sinomo_dfs::DfsNamespaceJobRecord) -> bool {
    job.kind == sinomo_dfs::DfsNamespaceJobKind::HardlinkTargetFenceAcrossShardPrimary
        && job.status == DfsNamespaceJobStatus::Running
        && matches!(
            job.pending_frames.as_slice(),
            [sinomo_dfs::DfsNamespaceJobFrame::HardlinkTargetFenceReservation { .. }]
        )
}

/// A Running hardlink job of the shard whose only step left commits a target
/// fence another shard holds, kept through the shard's conversion while that
/// shard is closed: its alias change is durable in the shard's rows, so none
/// of its counts wait for it, and its commit runs once both shards are open.
pub(crate) fn is_kept_fence_commit(job: &sinomo_dfs::DfsNamespaceJobRecord) -> bool {
    matches!(
        job.kind,
        sinomo_dfs::DfsNamespaceJobKind::HardlinkAcrossShardPrimaries
            | sinomo_dfs::DfsNamespaceJobKind::HardlinkAliasMutationAcrossShardPrimaries
    ) && job.status == DfsNamespaceJobStatus::Running
        && matches!(
            job.pending_frames.as_slice(),
            [sinomo_dfs::DfsNamespaceJobFrame::CrossShardHardlinkTargetFenceCommit { .. }]
        )
}

/// A Running parent-count participant the shard holds Reserved for another
/// shard's directory move and keeps through its conversion: the parent's
/// count change waits for that move's apply, so the shard's rows carry the
/// count from before the participant's delta.
pub(crate) fn is_kept_parent_participant(job: &sinomo_dfs::DfsNamespaceJobRecord) -> bool {
    job.kind == sinomo_dfs::DfsNamespaceJobKind::ParentCountParticipant
        && job.status == DfsNamespaceJobStatus::Running
        && matches!(
            job.pending_frames.as_slice(),
            [sinomo_dfs::DfsNamespaceJobFrame::ParentCountParticipant {
                state: sinomo_dfs::DfsParentCountState::Reserved,
                ..
            }]
        )
}

/// A live alias of the shard whose target is none of the shard's own live
/// canonical rows.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ForeignAliasTarget {
    pub alias_path: String,
    pub alias_object_id: DfsObjectId,
    pub target_object_id: DfsObjectId,
}

/// A live non-directory row of the shard whose parent path another shard owns.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ForeignParent {
    pub path: String,
    pub parent_path: String,
    pub parent_shard: String,
}

/// What the shard `captured` holds refers to in other shards, under the
/// census rules: aliases whose target it does not hold, and non-directory
/// rows whose parent path another shard owns. Each is in path order.
pub(crate) fn foreign_references(
    captured: &CapturedMetadataUnit,
    catalog: &[DfsMetadataShardConfig],
    limits: NamespaceCountMaintenanceLimits,
) -> Result<(Vec<ForeignAliasTarget>, Vec<ForeignParent>)> {
    let shard = &captured.source.physical_shard;
    let mut owned = CountedRows::default();
    for object in owned_live_rows(captured, Some(catalog), limits)? {
        owned.insert(shard, object)?;
    }
    let (mut aliases, mut parents) = (Vec::new(), Vec::new());
    for ((file_system_id, _), key) in &owned.paths {
        check_deadline(limits)?;
        let object = &owned.rows[key].1;
        if object.kind == DfsObjectKind::Hardlink {
            let target = object
                .link_target_object_id
                .clone()
                .ok_or_else(|| invalid("namespace alias has no canonical target"))?;
            if !owned.rows.contains_key(&(file_system_id.clone(), target.clone())) {
                aliases.push(ForeignAliasTarget {
                    alias_path: object.path.clone(),
                    alias_object_id: object.object_id.clone(),
                    target_object_id: target,
                });
            }
        }
        if object.kind != DfsObjectKind::Directory
            && let Some(parent_path) = parent(&object.path)
        {
            let owner = current_owner(file_system_id, catalog, parent_path)?;
            if owner.shard_id != shard.0 {
                parents.push(ForeignParent {
                    path: object.path.clone(),
                    parent_path: parent_path.to_owned(),
                    parent_shard: owner.shard_id,
                });
            }
        }
    }
    Ok((aliases, parents))
}

/// What one captured shard holds under a catalog: its live canonical
/// (non-alias) rows and live directories, by the census rules, and its
/// Running namespace jobs.
#[derive(Default)]
pub(crate) struct ShardHoldings {
    objects: BTreeSet<ObjectKey>,
    directories: BTreeSet<(DfsFileSystemId, String)>,
    jobs: BTreeSet<(DfsFileSystemId, String)>,
}

impl ShardHoldings {
    /// The entries of `question` this shard holds in `file_system_id`.
    pub(crate) fn answer(
        &self,
        file_system_id: &DfsFileSystemId,
        question: &sinomo_dfs::DfsCountContributionQuestion,
    ) -> Result<sinomo_dfs::DfsCountContributionHoldings> {
        if question.directories.iter().any(|path| !canonical_path(path)) {
            return Err(invalid("namespace holdings question names a noncanonical path"));
        }
        let fs = || file_system_id.clone();
        Ok(sinomo_dfs::DfsCountContributionHoldings {
            answered: u64::try_from(question.len()).map_err(|error| invalid(error.to_string()))?,
            objects: question
                .objects
                .iter()
                .filter(|id| self.objects.contains(&(fs(), (*id).clone())))
                .cloned()
                .collect(),
            directories: question
                .directories
                .iter()
                .filter(|path| self.directories.contains(&(fs(), (*path).clone())))
                .cloned()
                .collect(),
            jobs: question
                .jobs
                .iter()
                .filter(|id| self.jobs.contains(&(fs(), (*id).clone())))
                .cloned()
                .collect(),
        })
    }
}

/// [`ShardHoldings`] of the captured shard under `catalog`.
pub(crate) fn shard_holdings(
    captured: &CapturedMetadataUnit,
    catalog: &[DfsMetadataShardConfig],
    limits: NamespaceCountMaintenanceLimits,
) -> Result<ShardHoldings> {
    let mut holdings = ShardHoldings::default();
    for object in owned_live_rows(captured, Some(catalog), limits)? {
        let key = (object.file_system_id.clone(), object.object_id.clone());
        match object.kind {
            DfsObjectKind::Hardlink => {}
            DfsObjectKind::Directory => {
                holdings.directories.insert((object.file_system_id.clone(), object.path.clone()));
                holdings.objects.insert(key);
            }
            _ => {
                holdings.objects.insert(key);
            }
        }
    }
    for job in &captured.projected_rows.namespace_jobs {
        if job.status == DfsNamespaceJobStatus::Running {
            holdings.jobs.insert((job.file_system_id.clone(), job.job_id.clone()));
        }
    }
    Ok(holdings)
}

fn add_contributed_counts(
    catalog: &[DfsMetadataShardConfig],
    shard: &DfsMetadataShardId,
    owned: &CountedRows,
    moves: &moving::KeptMoves,
    contributions: &[ShardCountContributions],
    counts: &mut BTreeMap<ObjectKey, u64>,
    limits: NamespaceCountMaintenanceLimits,
) -> Result<()> {
    // T's own aliases and directories take part in the identity dedupe.
    let mut aliases = BTreeMap::<ObjectKey, DfsObjectId>::new();
    let mut children = BTreeMap::<ObjectKey, String>::new();
    for (key, (_, object)) in &owned.rows {
        match (&object.kind, &object.link_target_object_id, parent(&object.path)) {
            (DfsObjectKind::Hardlink, Some(target), _) => {
                aliases.insert(key.clone(), target.clone());
            }
            (DfsObjectKind::Directory, _, Some(parent_path)) => {
                children.insert(key.clone(), parent_path.to_owned());
            }
            _ => {}
        }
    }
    let mut sources = BTreeSet::new();
    for contribution in contributions {
        if &contribution.target_shard != shard
            || &contribution.source_shard == shard
            || !sources.insert(&contribution.source_shard)
        {
            return Err(invalid(
                "namespace-count contributions must come once from each other shard to this target",
            ));
        }
        let source_owns = |file_system_id: &DfsFileSystemId, path: &str| -> Result<()> {
            if !canonical_path(path)
                || current_owner(file_system_id, catalog, path)?.shard_id
                    != contribution.source_shard.0
            {
                return Err(invalid(
                    "namespace-count contribution names a path its source shard does not own",
                ));
            }
            Ok(())
        };
        for alias in &contribution.aliases {
            check_deadline(limits)?;
            source_owns(&alias.file_system_id, &alias.alias_path)?;
            let key = (alias.file_system_id.clone(), alias.alias_object_id.clone());
            match aliases.entry(key.clone()) {
                Entry::Occupied(seen) => {
                    if seen.get() != &alias.target_object_id {
                        return Err(invalid("namespace alias identity names two canonical targets"));
                    }
                    continue; // the same alias seen in two shards counts once
                }
                Entry::Vacant(entry) => {
                    entry.insert(alias.target_object_id.clone());
                }
            }
            if owned.rows.contains_key(&key) || children.contains_key(&key) {
                return Err(invalid("namespace alias identity is another object"));
            }
            let target_key = (alias.file_system_id.clone(), alias.target_object_id.clone());
            let Some((_, target)) = owned.rows.get(&target_key) else {
                continue; // another shard's canonical object
            };
            if matches!(
                target.kind,
                DfsObjectKind::Directory | DfsObjectKind::Hardlink | DfsObjectKind::Tombstone
            ) {
                return Err(invalid(
                    "namespace alias names a noncanonical or directory target",
                ));
            }
            increment_count(counts, &target_key)?;
        }
        for child in &contribution.child_directories {
            check_deadline(limits)?;
            source_owns(&child.file_system_id, &child.child_path)?;
            if parent(&child.child_path) != Some(child.parent_path.as_str())
                || current_owner(&child.file_system_id, catalog, &child.parent_path)?.shard_id
                    != shard.0
            {
                return Err(invalid(
                    "namespace-count child directory names a parent outside the target shard",
                ));
            }
            let key = (child.file_system_id.clone(), child.child_object_id.clone());
            match children.entry(key.clone()) {
                Entry::Occupied(seen) => {
                    if seen.get() != &child.parent_path {
                        return Err(invalid("namespace directory identity has two parents"));
                    }
                    continue; // the same directory seen in two shards counts once
                }
                Entry::Vacant(entry) => {
                    entry.insert(child.parent_path.clone());
                }
            }
            if owned.rows.contains_key(&key) || aliases.contains_key(&key) {
                return Err(invalid("namespace directory identity is another object"));
            }
            let parent_key = owned
                .paths
                .get(&(child.file_system_id.clone(), child.parent_path.clone()))
                .ok_or_else(|| invalid("live namespace object parent is absent from its owner"))?;
            if owned.rows[parent_key].1.kind != DfsObjectKind::Directory {
                return Err(invalid("live namespace object parent is not a directory"));
            }
            increment_count(counts, parent_key)?;
        }
        for placing in &contribution.moving_directories {
            check_deadline(limits)?;
            source_owns(&placing.file_system_id, &placing.child_path)?;
            let Some(placed) = moves.placement(placing)? else {
                continue; // the move has placed nothing in this shard yet
            };
            if current_owner(&placing.file_system_id, catalog, placed.parent_path)?.shard_id
                != shard.0
            {
                return Err(invalid(
                    "moving directory names a destination parent outside the target shard",
                ));
            }
            let key = (placing.file_system_id.clone(), placing.child_object_id.clone());
            match children.entry(key.clone()) {
                Entry::Occupied(seen) => {
                    if seen.get() != placed.parent_path {
                        return Err(invalid("namespace directory identity has two parents"));
                    }
                    continue; // the same directory already adopted here counts once
                }
                Entry::Vacant(entry) => {
                    entry.insert(placed.parent_path.to_owned());
                }
            }
            if owned.rows.contains_key(&key) || aliases.contains_key(&key) {
                return Err(invalid("namespace directory identity is another object"));
            }
            let Some(parent_key) = owned
                .paths
                .get(&(placing.file_system_id.clone(), placed.parent_path.to_owned()))
            else {
                if placed.move_root {
                    return Err(invalid(
                        "a kept directory move's root is absent from the shard",
                    ));
                }
                continue; // its parent is not adopted either and counts it in the source
            };
            if owned.rows[parent_key].1.kind != DfsObjectKind::Directory {
                return Err(invalid("live namespace object parent is not a directory"));
            }
            increment_count(counts, parent_key)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::tests::{fixture, limits, object, reseal};
    use super::*;

    fn contributions_to(
        catalog: &[DfsMetadataShardConfig],
        sources: &[NamespaceCountMaintenanceSource],
        target: &str,
    ) -> Result<Vec<ShardCountContributions>> {
        sources
            .iter()
            .filter(|source| source.captured.source.physical_shard.0 != target)
            .map(|source| contributions_from_captured(&source.captured, catalog, target, limits()))
            .collect()
    }
    fn shard_counts(
        catalog: &[DfsMetadataShardConfig],
        sources: &[NamespaceCountMaintenanceSource],
        target: &str,
    ) -> Result<BTreeMap<String, u64>> {
        let source = sources
            .iter()
            .find(|source| source.captured.source.physical_shard.0 == target)
            .unwrap();
        let plan = plan_shard_namespace_count_maintenance(
            catalog,
            source,
            &contributions_to(catalog, sources, target)?,
            limits(),
        )?;
        assert_eq!(plan.sources, vec![source.captured.source.clone()]);
        Ok(plan
            .updates
            .into_iter()
            .map(|update| {
                assert_eq!(update.shard_id.0, target);
                (update.original.path, update.namespace_link_count)
            })
            .collect())
    }
    fn add_object(source: &mut NamespaceCountMaintenanceSource, object: DfsObjectMetadata) {
        let captured = Arc::make_mut(&mut source.captured);
        let mut state = sinomo_dfs::DfsMetadataCandidateState::new(captured.projected_rows.clone()).unwrap();
        state.apply(sinomo_dfs::DfsPreparedMetadataEffect::PutObject { object }).unwrap();
        captured.projected_rows = state.snapshot();
        reseal(source);
    }

    /// The per-shard activation check accepts an image carrying exactly the
    /// counts of its own rows and sealed contributions, and refuses one whose
    /// counts are absent or that a missing contribution would change.
    #[test]
    fn a_built_image_is_checked_against_its_own_rows_and_sealed_contributions() {
        let (_root, catalog, sources) = fixture();
        for shard in ["root", "services", "users"] {
            let source = sources
                .iter()
                .find(|source| source.captured.source.physical_shard.0 == shard)
                .unwrap();
            let contributions = contributions_to(&catalog, &sources, shard).unwrap();
            let plan = plan_shard_namespace_count_maintenance(&catalog, source, &contributions, limits()).unwrap();
            let mut image = source.captured.projected_rows.clone();
            image.objects = plan.counted_objects(source, limits()).unwrap();
            let id = DfsMetadataShardId::new(shard);
            let file_systems = &source.captured.source.file_system_ids;
            validate_shard_image_counts(&catalog, &id, file_systems, &image, &contributions, limits()).unwrap();
            assert!(
                validate_shard_image_counts(
                    &catalog, &id, file_systems, &source.captured.projected_rows, &contributions, limits(),
                )
                .is_err(),
                "an image without its counts is refused"
            );
        }
        // The root shard counts the other shards' roots as its children: an
        // image built from those contributions disagrees without them.
        let root = sources.iter().find(|source| source.captured.source.physical_shard.0 == "root").unwrap();
        let contributions = contributions_to(&catalog, &sources, "root").unwrap();
        assert!(contributions.iter().any(|contribution| !contribution.child_directories.is_empty()));
        let plan = plan_shard_namespace_count_maintenance(&catalog, root, &contributions, limits()).unwrap();
        let mut image = root.captured.projected_rows.clone();
        image.objects = plan.counted_objects(root, limits()).unwrap();
        let id = DfsMetadataShardId::new("root");
        let partial = contributions
            .iter()
            .map(|contribution| ShardCountContributions { child_directories: Vec::new(), ..contribution.clone() })
            .collect::<Vec<_>>();
        assert!(validate_shard_image_counts(
            &catalog, &id, &root.captured.source.file_system_ids, &image, &partial, limits(),
        )
        .is_err());
    }

    #[test]
    fn per_shard_counts_equal_the_complete_census_on_the_same_cut() {
        let (_root, catalog, sources) = fixture();
        let census = plan_namespace_count_maintenance(&catalog, &sources, limits()).unwrap();
        let mut expected = BTreeMap::new();
        for update in census.updates {
            expected.insert((update.shard_id.0.clone(), update.original.object_id.clone()), update);
        }
        let mut actual = BTreeMap::new();
        for shard in ["root", "services", "users"] {
            let source = sources.iter().find(|source| source.captured.source.physical_shard.0 == shard).unwrap();
            let plan = plan_shard_namespace_count_maintenance(
                &catalog, source, &contributions_to(&catalog, &sources, shard).unwrap(), limits(),
            ).unwrap();
            for update in plan.updates {
                assert!(actual.insert((update.shard_id.0.clone(), update.original.object_id.clone()), update).is_none());
            }
        }
        assert_eq!(actual, expected);
        let counts = actual.values().map(|update| (update.original.path.as_str(), update.namespace_link_count))
            .collect::<BTreeMap<_, _>>();
        // The root counts the two foreign shard roots; the file counts the
        // alias the users shard owns.
        assert_eq!(counts["/"], 4);
        assert_eq!(counts["/services/file"], 2);
    }

    #[test]
    fn extraction_applies_the_census_rules_for_one_source() {
        let (_root, catalog, sources) = fixture();
        let users = contributions_from_captured(&sources[2].captured, &catalog, "services", limits()).unwrap();
        assert_eq!(users.source_shard.0, "users");
        assert_eq!(users.target_shard.0, "services");
        assert_eq!(users.aliases.len(), 1);
        assert_eq!(users.aliases[0].alias_path, "/users/alias");
        assert!(users.child_directories.is_empty(), "/users's parent belongs to root");
        let to_root = contributions_from_captured(&sources[2].captured, &catalog, "root", limits()).unwrap();
        assert_eq!(to_root.aliases, users.aliases, "every live alias, whatever its target");
        assert_eq!(to_root.child_directories.len(), 1);
        assert_eq!(to_root.child_directories[0].parent_path, "/");
        assert_eq!(to_root.child_directories[0].child_path, "/users");
        // The root shard's stale physical copy of /services/file is not a row
        // the root owns, so it contributes nothing.
        let root = contributions_from_captured(&sources[0].captured, &catalog, "services", limits()).unwrap();
        assert!(root.aliases.is_empty() && root.child_directories.is_empty());
        for target in ["users", "absent"] {
            assert!(contributions_from_captured(&sources[2].captured, &catalog, target, limits()).is_err());
        }
    }

    #[test]
    fn unsettled_contributor_or_target_is_refused_like_the_census() {
        let job = sinomo_dfs::DfsNamespaceJobRecord {
            job_id: "unfinished-contribution".into(),
            file_system_id: sinomo_dfs::sifs_file_system_id(),
            origin: None,
            kind: sinomo_dfs::DfsNamespaceJobKind::RenameObjectAcrossShardPrimaries,
            status: DfsNamespaceJobStatus::Running,
            root_path: "/users/text".into(),
            target_path: Some("/services/text".into()),
            pending_frames: Vec::new(),
            processed_object_count: 0,
            created_at: time::OffsetDateTime::UNIX_EPOCH,
            updated_at: time::OffsetDateTime::UNIX_EPOCH,
            completed_at: None,
            failure_reason: None,
        };
        let (_root, catalog, mut sources) = fixture();
        Arc::make_mut(&mut sources[2].captured).projected_rows.namespace_jobs.push(job);
        reseal(&mut sources[2]);
        assert!(contributions_from_captured(&sources[2].captured, &catalog, "services", limits()).is_err());
        let users = sources.remove(2);
        let error = plan_shard_namespace_count_maintenance(&catalog, &users, &[], limits()).err().unwrap();
        assert!(error.to_string().contains("settled original namespace"), "{error}");
    }

    #[test]
    fn an_alias_seen_in_two_shards_counts_once_and_conflicting_identities_fail_closed() {
        let (_root, catalog, mut sources) = fixture();
        let alias = sources[2].captured.projected_rows.objects.iter()
            .find(|object| object.kind == DfsObjectKind::Hardlink).unwrap().clone();
        // A cross-shard alias rename adopts at the destination before the
        // source evicts, so both shards can report the same alias.
        let mut moved = alias.clone();
        moved.path = "/moved-alias".into();
        add_object(&mut sources[0], moved.clone());
        assert_eq!(shard_counts(&catalog, &sources, "services").unwrap()["/services/file"], 2);

        let (_root, catalog, mut sources) = fixture();
        let mut conflicting = moved;
        conflicting.link_target_object_id = Some(DfsObjectId::from_legacy_named("count-text").unwrap());
        add_object(&mut sources[0], conflicting);
        let error = shard_counts(&catalog, &sources, "services").err().unwrap();
        assert!(error.to_string().contains("two canonical targets"), "{error}");
    }

    #[test]
    fn an_alias_naming_another_shards_object_is_left_to_its_owner() {
        let (_root, catalog, mut sources) = fixture();
        let mut dangling = object("count-dangling", "/users/dangling", DfsObjectKind::Hardlink);
        dangling.link_target_object_id = Some(DfsObjectId::from_legacy_named("nowhere").unwrap());
        add_object(&mut sources[2], dangling);
        for shard in ["root", "services", "users"] {
            shard_counts(&catalog, &sources, shard).unwrap();
        }
        assert!(plan_namespace_count_maintenance(&catalog, &sources, limits()).is_err(),
            "only the complete census can see that no shard holds the target");
    }

    #[test]
    fn a_contributed_child_needs_a_live_directory_parent_in_the_target() {
        let (root, catalog, mut sources) = fixture();
        let file = sources[1].captured.projected_rows.objects.iter()
            .find(|object| object.path == "/services/file").unwrap().clone();
        sources[0] = super::super::tests::source(&root.join("parentless-root"), &catalog, "root", vec![file]);
        let error = shard_counts(&catalog, &sources, "root").err().unwrap();
        assert!(error.to_string().contains("parent is absent"), "{error}");
    }

    #[test]
    fn contributions_must_be_attributed_to_their_real_source_and_target() {
        let (_root, catalog, sources) = fixture();
        let target = &sources[1];
        let honest = contributions_to(&catalog, &sources, "services").unwrap();
        plan_shard_namespace_count_maintenance(&catalog, target, &honest, limits()).unwrap();
        let mut repeated = honest.clone();
        repeated.push(honest[0].clone());
        let mut retargeted = honest.clone();
        retargeted[0].target_shard = DfsMetadataShardId::new("users");
        let mut own = honest.clone();
        own[0].source_shard = DfsMetadataShardId::new("services");
        // The users shard's alias path is not the root shard's to report.
        let mut misattributed = honest.clone();
        misattributed.retain(|contribution| contribution.source_shard.0 == "users");
        misattributed[0].source_shard = DfsMetadataShardId::new("root");
        let mut foreign_parent = honest;
        assert_eq!(foreign_parent[0].source_shard.0, "root");
        foreign_parent[0].child_directories.push(NamespaceCountChildDirectoryContribution {
            file_system_id: sinomo_dfs::sifs_file_system_id(),
            parent_path: "/".into(),
            child_object_id: DfsObjectId::from_legacy_named("count-root-child").unwrap(),
            child_path: "/root-child".into(),
        });
        for (bad, reason) in [
            (repeated, "once from each other shard"),
            (retargeted, "once from each other shard"),
            (own, "once from each other shard"),
            (misattributed, "source shard does not own"),
            (foreign_parent, "parent outside the target shard"),
        ] {
            let error = plan_shard_namespace_count_maintenance(&catalog, target, &bad, limits()).err().unwrap();
            assert!(error.to_string().contains(reason), "{reason}: {error}");
        }
    }

    #[test]
    fn existing_counts_are_verified_and_ownership_not_epoch_binds_the_catalog() {
        let (_root, catalog, mut sources) = fixture();
        let file = Arc::make_mut(&mut sources[1].captured).projected_rows.objects.iter_mut()
            .find(|row| row.path == "/services/file").unwrap();
        file.namespace_link_count = Some(2);
        reseal(&mut sources[1]);
        let counts = shard_counts(&catalog, &sources, "services").unwrap();
        assert!(!counts.contains_key("/services/file"), "a verified count is not an update");
        assert_eq!(counts["/services"], 3);
        Arc::make_mut(&mut sources[1].captured).projected_rows.objects.iter_mut()
            .find(|row| row.path == "/services/file").unwrap().namespace_link_count = Some(1);
        reseal(&mut sources[1]);
        assert!(shard_counts(&catalog, &sources, "services").is_err());

        let (_root, _, sources) = fixture();
        let mut moved_primary = catalog.clone();
        moved_primary[1].epoch += 1;
        moved_primary[1].primary_node_id = sinomo_core::node::NodeId::from_legacy_named("count-taken-over").unwrap();
        assert_eq!(shard_counts(&moved_primary, &sources, "services").unwrap()["/services/file"], 2);
        let mut reassigned = catalog;
        reassigned[1].assignment = DfsMetadataShardAssignment::PathPrefix { prefix: "/services/sub".into() };
        assert!(shard_counts(&reassigned, &sources, "services").is_err());
    }

    #[test]
    fn contributions_count_toward_the_object_budget() {
        let (_root, catalog, sources) = fixture();
        let contributions = contributions_to(&catalog, &sources, "root").unwrap();
        let objects = sources[0].captured.projected_rows.objects.len();
        let bounded = NamespaceCountMaintenanceLimits { max_objects: objects, ..limits() };
        assert!(plan_shard_namespace_count_maintenance(&catalog, &sources[0], &[], bounded).is_ok());
        let error = plan_shard_namespace_count_maintenance(&catalog, &sources[0], &contributions, bounded)
            .err().unwrap();
        assert!(error.to_string().contains("object budget exhausted"), "{error}");
    }
}
