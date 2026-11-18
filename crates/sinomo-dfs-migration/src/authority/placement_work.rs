//! Original placement goals, archived from their legacy placement rows. A
//! shard's per-shard conversion binds its share of them into its capture.
use super::placement_job_source::OriginalPlacementJob;
use super::*;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(super) struct OriginalPlacementGoal {
    job: OriginalPlacementJob,
    assignments: Vec<DfsMetadataShardConfig>,
}
impl OriginalPlacementGoal {
    #[cfg(test)]
    pub(super) fn for_test(job: OriginalPlacementJob, assignments: Vec<DfsMetadataShardConfig>) -> Self {
        Self { job, assignments }
    }
    pub(super) fn file_system_id(&self) -> &sinomo_dfs::DfsFileSystemId { &self.job.file_system_id }
    pub(super) fn is_terminal(&self) -> bool { self.job.terminal }
    pub(super) fn evidence(&self, shard: &sinomo_dfs::DfsMetadataShardId) -> Result<crate::metadata::RetainedPlacementReservation> {
        let assignment = self.assignments.iter().find(|row| row.shard_id == shard.0)
            .ok_or_else(|| error("placement evidence omits an original assignment"))?;
        crate::metadata::RetainedPlacementReservation::from_capsule(self.job.capsule_for_scope(assignment)?)
    }
}
/// One shard's share of the original placement goals of its filesystem: the
/// evidence of every non-terminal goal whose captured assignments name the
/// shard, in reference order. A goal captured before the shard existed has no
/// share in it.
pub(super) fn shard_placement_share(
    goals: &[OriginalPlacementGoal],
    file_system_id: &sinomo_dfs::DfsFileSystemId,
    shard: &sinomo_dfs::DfsMetadataShardId,
) -> Result<Vec<crate::metadata::RetainedPlacementReservation>> {
    let mut evidence = Vec::new();
    for goal in goals {
        if goal.is_terminal()
            || goal.file_system_id() != file_system_id
            || !goal.assignments.iter().any(|assignment| assignment.shard_id == shard.0)
        {
            continue;
        }
        let item = goal.evidence(shard)?;
        evidence.push((item.reference()?, item));
    }
    evidence.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(evidence.into_iter().map(|(_, item)| item).collect())
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
        let observed = self.read_native(
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
            let goal = OriginalPlacementGoal { job, assignments };
            Self::save_blueprint(&root, path.file_name().unwrap().to_str().unwrap(), &goal)?;
            remaining -= 1;
        }
        // The archive is the restart source of the captured goals.
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
}
